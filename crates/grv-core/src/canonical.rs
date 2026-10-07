//! Bounded external sorting and a versioned deterministic Parquet profile.
use crate::{Error, Result, contract};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array,
    Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray,
};
use arrow_schema::{DataType, Schema, TimeUnit};
use grv_adapter_api::TableContract;
use grv_types::Digest;
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, Encoding},
    file::properties::{EnabledStatistics, WriterProperties, WriterVersion},
};
use sha2::{Digest as _, Sha256};
use std::{
    cmp::Ordering,
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

/// Changing any output-affecting constant or encoding requires a new profile.
pub const CANONICAL_WRITER: &str =
    "grv/parquet-58.3.0/v1/plain-uncompressed/f4096-r4096-b16777216-p1048576";
const FILE_ROWS: usize = 4096;
const CHUNK_ROWS: usize = 1024;
const CHUNK_BYTES: usize = 4 * 1024 * 1024;
const MAX_ROW_BYTES: usize = 8 * 1024 * 1024;
const WRITER_MEMORY: usize = 64 * 1024 * 1024;
const READER_BUFFER: usize = 8192;

#[derive(Debug, Clone)]
pub struct StagedFile {
    pub name: String,
    pub path: PathBuf,
    pub size: u64,
    pub sha256: Digest,
    pub rows: u64,
}
/// Keeps the staging directory alive until the consumer durably adopts it.
pub struct StagedGroup {
    pub files: Vec<StagedFile>,
    pub row_count: u64,
    pub writer: &'static str,
    _directory: tempfile::TempDir,
}
impl StagedGroup {
    pub(crate) fn adopted(
        files: Vec<StagedFile>,
        row_count: u64,
        directory: tempfile::TempDir,
    ) -> Self {
        Self {
            files,
            row_count,
            writer: CANONICAL_WRITER,
            _directory: directory,
        }
    }
}

#[derive(Debug, Clone)]
enum Cell {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Decimal(i128),
    Bytes(Vec<u8>),
}
impl Cell {
    fn compare(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Null, Self::Null) => Ordering::Equal,
            (Self::Null, _) => Ordering::Less,
            (_, Self::Null) => Ordering::Greater,
            (Self::Bool(a), Self::Bool(b)) => a.cmp(b),
            (Self::Int(a), Self::Int(b)) => a.cmp(b),
            (Self::Float(a), Self::Float(b)) => a.total_cmp(b),
            (Self::Decimal(a), Self::Decimal(b)) => a.cmp(b),
            (Self::Bytes(a), Self::Bytes(b)) => a.cmp(b),
            _ => unreachable!("rows share a validated schema"),
        }
    }
}
type Row = Vec<Cell>;
fn compare_rows(a: &Row, b: &Row) -> Ordering {
    a.iter()
        .zip(b)
        .map(|(a, b)| a.compare(b))
        .find(|o| *o != Ordering::Equal)
        .unwrap_or(Ordering::Equal)
}
fn row_memory(row: &Row) -> usize {
    std::mem::size_of::<Row>()
        + row.capacity() * std::mem::size_of::<Cell>()
        + row
            .iter()
            .map(|cell| match cell {
                Cell::Bytes(bytes) => bytes.capacity(),
                _ => 0,
            })
            .sum::<usize>()
}

pub struct Sorter {
    contract: TableContract,
    schema: Arc<Schema>,
    directory: tempfile::TempDir,
    rows: Vec<Row>,
    memory: usize,
    budget: usize,
    max_row_memory: usize,
    spills: Vec<PathBuf>,
    next_spill: usize,
    count: u64,
}
impl Sorter {
    pub fn new(contract: TableContract, staging_parent: &Path) -> Result<Self> {
        Self::with_memory(contract, staging_parent, 128 * 1024 * 1024)
    }
    /// Memory controls sorting only, and therefore cannot change output bytes.
    pub fn with_memory(
        contract: TableContract,
        staging_parent: &Path,
        budget: usize,
    ) -> Result<Self> {
        if budget < 1024 * 1024 {
            return Err(Error("sort memory must be at least 1MiB".into()));
        }
        let schema = contract::arrow_schema(&contract)?;
        let directory = tempfile::Builder::new()
            .prefix("grv-canonical-")
            .tempdir_in(staging_parent)?;
        Ok(Self {
            contract,
            schema,
            directory,
            rows: Vec::new(),
            memory: 0,
            budget,
            max_row_memory: 1,
            spills: Vec::new(),
            next_spill: 0,
            count: 0,
        })
    }
    pub fn append(&mut self, batch: &RecordBatch) -> Result<()> {
        contract::validate_batch(&self.contract, batch)?;
        for index in 0..batch.num_rows() {
            let size = row_size(batch, index)?;
            if size > MAX_ROW_BYTES || size + READER_BUFFER > self.budget / 4 {
                return Err(Error("row exceeds bounded sort workspace".into()));
            }
            // Spill before allocating the new owned row. The input batch is
            // separately owned by the receive credit, never retained here.
            if !self.rows.is_empty()
                && self.memory + size + std::mem::size_of::<Row>() > self.budget / 2
            {
                self.spill()?;
            }
            let row = extract_row(batch, index)?;
            let size = row_memory(&row);
            self.max_row_memory = self.max_row_memory.max(size);
            self.memory += size;
            self.rows.push(row);
            self.count = self
                .count
                .checked_add(1)
                .ok_or_else(|| Error("row count exhausted".into()))?;
        }
        Ok(())
    }
    fn next_path(&mut self) -> PathBuf {
        let path = self
            .directory
            .path()
            .join(format!("sort-{}.bin", self.next_spill));
        self.next_spill += 1;
        path
    }
    fn spill(&mut self) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        self.rows.sort_unstable_by(compare_rows);
        let path = self.next_path();
        let mut output = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)?,
        );
        for row in &self.rows {
            write_row(&mut output, row)?;
        }
        output.flush()?;
        output.get_ref().sync_all()?;
        self.rows = Vec::new();
        self.memory = 0;
        self.spills.push(path);
        Ok(())
    }
    pub fn finish(mut self) -> Result<StagedGroup> {
        self.spill()?;
        // Every reader retains one row and a small buffer. Reserve space for
        // the replacement head and the output writer independently.
        let fan_in = (self.budget / (self.max_row_memory + READER_BUFFER) / 2).clamp(2, 32);
        while self.spills.len() > fan_in {
            let paths = std::mem::take(&mut self.spills);
            for group in paths.chunks(fan_in) {
                let path = self.next_path();
                let mut output = BufWriter::new(
                    OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&path)?,
                );
                merge(group, &self.schema, |row| write_row(&mut output, &row))?;
                output.flush()?;
                output.get_ref().sync_all()?;
                for input in group {
                    fs::remove_file(input)?;
                }
                self.spills.push(path);
            }
        }
        let mut sink = ParquetSink::new(self.directory.path(), self.schema.clone())?;
        merge(&self.spills, &self.schema, |row| sink.append(row))?;
        let files = sink.finish()?;
        for path in &self.spills {
            fs::remove_file(path)?;
        }
        File::open(self.directory.path())?.sync_all()?;
        Ok(StagedGroup {
            files,
            row_count: self.count,
            writer: CANONICAL_WRITER,
            _directory: self.directory,
        })
    }
}

fn row_size(batch: &RecordBatch, index: usize) -> Result<usize> {
    let mut size = std::mem::size_of::<Row>() + batch.num_columns() * std::mem::size_of::<Cell>();
    for array in batch.columns() {
        if array.is_null(index) {
            continue;
        }
        let bytes = match array.data_type() {
            DataType::Utf8 => downcast::<StringArray>(array)?.value(index).len(),
            DataType::Binary => downcast::<BinaryArray>(array)?.value(index).len(),
            _ => 0,
        };
        size = size
            .checked_add(bytes)
            .ok_or_else(|| Error("row size overflow".into()))?;
    }
    Ok(size)
}
fn downcast<T: Array + 'static>(array: &ArrayRef) -> Result<&T> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| Error("array implementation differs from schema".into()))
}
fn extract_row(batch: &RecordBatch, index: usize) -> Result<Row> {
    batch
        .columns()
        .iter()
        .map(|array| {
            if array.is_null(index) {
                return Ok(Cell::Null);
            }
            Ok(match array.data_type() {
                DataType::Boolean => Cell::Bool(downcast::<BooleanArray>(array)?.value(index)),
                DataType::Int64 => Cell::Int(downcast::<Int64Array>(array)?.value(index)),
                DataType::Date32 => {
                    Cell::Int(i64::from(downcast::<Date32Array>(array)?.value(index)))
                }
                DataType::Float64 => Cell::Float(downcast::<Float64Array>(array)?.value(index)),
                DataType::Decimal128(precision, _) => {
                    let value = downcast::<Decimal128Array>(array)?.value(index);
                    if value.unsigned_abs() >= 10u128.pow(u32::from(*precision)) {
                        return Err(Error("decimal value exceeds precision".into()));
                    }
                    Cell::Decimal(value)
                }
                DataType::Utf8 => Cell::Bytes(
                    downcast::<StringArray>(array)?
                        .value(index)
                        .as_bytes()
                        .to_vec(),
                ),
                DataType::Binary => {
                    Cell::Bytes(downcast::<BinaryArray>(array)?.value(index).to_vec())
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    Cell::Int(downcast::<TimestampMillisecondArray>(array)?.value(index))
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    Cell::Int(downcast::<TimestampMicrosecondArray>(array)?.value(index))
                }
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    Cell::Int(downcast::<TimestampNanosecondArray>(array)?.value(index))
                }
                _ => return Err(Error("unsupported canonical type".into())),
            })
        })
        .collect()
}
fn write_row(output: &mut impl Write, row: &Row) -> Result<()> {
    for cell in row {
        if let Cell::Null = cell {
            output.write_all(&[0])?;
            continue;
        }
        output.write_all(&[1])?;
        match cell {
            Cell::Bool(value) => output.write_all(&[u8::from(*value)])?,
            Cell::Int(value) => output.write_all(&value.to_le_bytes())?,
            Cell::Float(value) => output.write_all(&value.to_bits().to_le_bytes())?,
            Cell::Decimal(value) => output.write_all(&value.to_le_bytes())?,
            Cell::Bytes(value) => {
                output.write_all(&(value.len() as u64).to_le_bytes())?;
                output.write_all(value)?;
            }
            Cell::Null => unreachable!(),
        }
    }
    Ok(())
}
struct RowReader {
    input: BufReader<File>,
    schema: Arc<Schema>,
}
impl RowReader {
    fn open(path: &Path, schema: Arc<Schema>) -> Result<Self> {
        Ok(Self {
            input: BufReader::with_capacity(READER_BUFFER, File::open(path)?),
            schema,
        })
    }
    fn next(&mut self) -> Result<Option<Row>> {
        let mut row = Vec::with_capacity(self.schema.fields().len());
        for (index, field) in self.schema.fields().iter().enumerate() {
            let mut present = [0];
            match self.input.read(&mut present)? {
                0 if index == 0 => return Ok(None),
                0 => return Err(Error("truncated sort record".into())),
                _ => {}
            }
            if present[0] == 0 {
                row.push(Cell::Null);
                continue;
            }
            if present[0] != 1 {
                return Err(Error("invalid sort record".into()));
            }
            fn number<const N: usize>(input: &mut impl Read) -> Result<[u8; N]> {
                let mut value = [0; N];
                input.read_exact(&mut value)?;
                Ok(value)
            }
            let cell = match field.data_type() {
                DataType::Boolean => Cell::Bool(match number::<1>(&mut self.input)?[0] {
                    0 => false,
                    1 => true,
                    _ => return Err(Error("invalid sort boolean".into())),
                }),
                DataType::Int64 | DataType::Date32 | DataType::Timestamp(_, _) => {
                    Cell::Int(i64::from_le_bytes(number(&mut self.input)?))
                }
                DataType::Float64 => {
                    Cell::Float(f64::from_bits(u64::from_le_bytes(number(&mut self.input)?)))
                }
                DataType::Decimal128(_, _) => {
                    Cell::Decimal(i128::from_le_bytes(number(&mut self.input)?))
                }
                DataType::Utf8 | DataType::Binary => {
                    let size = u64::from_le_bytes(number(&mut self.input)?);
                    if size > MAX_ROW_BYTES as u64 {
                        return Err(Error("sort record size exceeded".into()));
                    }
                    let mut bytes = vec![0; size as usize];
                    self.input.read_exact(&mut bytes)?;
                    if field.data_type() == &DataType::Utf8 && std::str::from_utf8(&bytes).is_err()
                    {
                        return Err(Error("corrupt sort UTF-8".into()));
                    }
                    Cell::Bytes(bytes)
                }
                _ => return Err(Error("unsupported sort record type".into())),
            };
            row.push(cell);
            if row_memory(&row) > MAX_ROW_BYTES {
                return Err(Error("sort record size exceeded".into()));
            }
        }
        Ok(Some(row))
    }
}
fn merge(
    paths: &[PathBuf],
    schema: &Arc<Schema>,
    mut consume: impl FnMut(Row) -> Result<()>,
) -> Result<()> {
    let mut readers = paths
        .iter()
        .map(|path| RowReader::open(path, schema.clone()))
        .collect::<Result<Vec<_>>>()?;
    let mut heads = readers
        .iter_mut()
        .map(RowReader::next)
        .collect::<Result<Vec<_>>>()?;
    loop {
        let Some(index) = heads
            .iter()
            .enumerate()
            .filter_map(|(i, row)| row.as_ref().map(|row| (i, row)))
            .min_by(|(_, a), (_, b)| compare_rows(a, b))
            .map(|(i, _)| i)
        else {
            return Ok(());
        };
        consume(heads[index].take().unwrap())?;
        heads[index] = readers[index].next()?;
    }
}

struct ParquetSink {
    directory: PathBuf,
    schema: Arc<Schema>,
    writer: Option<ArrowWriter<File>>,
    chunk: Vec<Row>,
    chunk_bytes: usize,
    rows: usize,
    files: Vec<StagedFile>,
}
impl ParquetSink {
    fn new(directory: &Path, schema: Arc<Schema>) -> Result<Self> {
        let mut out = Self {
            directory: directory.into(),
            schema,
            writer: None,
            chunk: Vec::new(),
            chunk_bytes: 0,
            rows: 0,
            files: Vec::new(),
        };
        out.open()?; // Even zero rows produce the required data.parquet.
        Ok(out)
    }
    fn name(&self) -> String {
        if self.files.is_empty() {
            "data.parquet".into()
        } else {
            format!("data-{}.parquet", self.files.len())
        }
    }
    fn open(&mut self) -> Result<()> {
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(self.directory.join(self.name()))?;
        let properties = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_created_by(CANONICAL_WRITER.into())
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_encoding(Encoding::PLAIN)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_offset_index_disabled(true)
            .set_data_page_size_limit(1024 * 1024)
            .set_data_page_row_count_limit(1024)
            .set_write_batch_size(1024)
            .set_max_row_group_row_count(Some(FILE_ROWS))
            .set_max_row_group_bytes(Some(16 * 1024 * 1024))
            .build();
        self.writer = Some(
            ArrowWriter::try_new(file, self.schema.clone(), Some(properties))
                .map_err(|e| Error(e.to_string()))?,
        );
        Ok(())
    }
    fn append(&mut self, row: Row) -> Result<()> {
        if self.rows == FILE_ROWS {
            self.close_file()?;
            self.open()?;
        }
        let size = row_memory(&row);
        if !self.chunk.is_empty()
            && (self.chunk.len() == CHUNK_ROWS || self.chunk_bytes + size > CHUNK_BYTES)
        {
            self.flush_chunk()?;
        }
        self.chunk_bytes += size;
        self.chunk.push(row);
        self.rows += 1;
        Ok(())
    }
    fn flush_chunk(&mut self) -> Result<()> {
        if self.chunk.is_empty() {
            return Ok(());
        }
        let batch = rows_batch(&self.schema, &self.chunk)?;
        let writer = self.writer.as_mut().unwrap();
        writer.write(&batch).map_err(|e| Error(e.to_string()))?;
        // Statistics, dictionaries and indexes are disabled. This conservative
        // metadata allowance includes column paths and encoding descriptors.
        let column_bytes: usize = self
            .schema
            .fields()
            .iter()
            .map(|field| field.name().len() * 4 + 2048)
            .sum();
        let metadata_bytes = (writer.flushed_row_groups().len() + 1).saturating_mul(column_bytes);
        if writer.memory_size().saturating_add(metadata_bytes) > WRITER_MEMORY {
            return Err(Error("canonical writer memory budget exceeded".into()));
        }
        self.chunk.clear();
        self.chunk_bytes = 0;
        Ok(())
    }
    fn close_file(&mut self) -> Result<()> {
        self.flush_chunk()?;
        self.writer
            .take()
            .unwrap()
            .close()
            .map_err(|e| Error(e.to_string()))?;
        let name = self.name();
        let path = self.directory.join(&name);
        let mut file = File::open(&path)?;
        file.sync_all()?;
        let size = file.metadata()?.len();
        let mut hash = Sha256::new();
        let mut buffer = [0; 64 * 1024];
        loop {
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            hash.update(&buffer[..n]);
        }
        self.files.push(StagedFile {
            name,
            path,
            size,
            sha256: Digest::new(format!("{:x}", hash.finalize())).unwrap(),
            rows: self.rows as u64,
        });
        self.rows = 0;
        Ok(())
    }
    fn finish(mut self) -> Result<Vec<StagedFile>> {
        self.close_file()?;
        Ok(self.files)
    }
}
fn rows_batch(schema: &Arc<Schema>, rows: &[Row]) -> Result<RecordBatch> {
    let arrays: Result<Vec<ArrayRef>> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            macro_rules! primitive {
                ($array:ty, $variant:ident, $cast:ty) => {
                    Arc::new(<$array>::from(
                        rows.iter()
                            .map(|row| match row[index] {
                                Cell::Null => None,
                                Cell::$variant(value) => Some(value as $cast),
                                _ => unreachable!(),
                            })
                            .collect::<Vec<_>>(),
                    )) as ArrayRef
                };
            }
            let array = match field.data_type() {
                DataType::Boolean => primitive!(BooleanArray, Bool, bool),
                DataType::Int64 => primitive!(Int64Array, Int, i64),
                DataType::Date32 => primitive!(Date32Array, Int, i32),
                DataType::Float64 => primitive!(Float64Array, Float, f64),
                DataType::Decimal128(precision, scale) => {
                    let array = Decimal128Array::from(
                        rows.iter()
                            .map(|row| match row[index] {
                                Cell::Null => None,
                                Cell::Decimal(value) => Some(value),
                                _ => unreachable!(),
                            })
                            .collect::<Vec<_>>(),
                    )
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|e| Error(e.to_string()))?;
                    Arc::new(array) as ArrayRef
                }
                DataType::Utf8 => Arc::new(StringArray::from(
                    rows.iter()
                        .map(|row| match &row[index] {
                            Cell::Null => None,
                            Cell::Bytes(value) => Some(std::str::from_utf8(value).unwrap()),
                            _ => unreachable!(),
                        })
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
                DataType::Binary => Arc::new(BinaryArray::from(
                    rows.iter()
                        .map(|row| match &row[index] {
                            Cell::Null => None,
                            Cell::Bytes(value) => Some(value.as_slice()),
                            _ => unreachable!(),
                        })
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
                DataType::Timestamp(unit, timezone) => {
                    let values = rows
                        .iter()
                        .map(|row| match row[index] {
                            Cell::Null => None,
                            Cell::Int(value) => Some(value),
                            _ => unreachable!(),
                        })
                        .collect::<Vec<_>>();
                    match unit {
                        TimeUnit::Millisecond => Arc::new(
                            TimestampMillisecondArray::from(values)
                                .with_timezone_opt(timezone.clone()),
                        ) as ArrayRef,
                        TimeUnit::Microsecond => Arc::new(
                            TimestampMicrosecondArray::from(values)
                                .with_timezone_opt(timezone.clone()),
                        ) as ArrayRef,
                        TimeUnit::Nanosecond => Arc::new(
                            TimestampNanosecondArray::from(values)
                                .with_timezone_opt(timezone.clone()),
                        ) as ArrayRef,
                        _ => return Err(Error("unsupported timestamp unit".into())),
                    }
                }
                _ => return Err(Error("unsupported canonical type".into())),
            };
            Ok(array)
        })
        .collect();
    RecordBatch::try_new(schema.clone(), arrays?).map_err(|e| Error(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use grv_adapter_api::Column;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use serde_json::json;
    fn contract(columns: &[(&str, serde_json::Value)]) -> TableContract {
        TableContract {
            columns: columns
                .iter()
                .map(|(name, logical_type)| Column {
                    name: (*name).into(),
                    logical_type: logical_type.clone(),
                })
                .collect(),
            partition_keys: vec![],
            extensions: json!({}),
            column_ext: json!({}),
        }
    }
    fn read(group: &StagedGroup) -> Vec<RecordBatch> {
        group
            .files
            .iter()
            .flat_map(|file| {
                ParquetRecordBatchReaderBuilder::try_new(File::open(&file.path).unwrap())
                    .unwrap()
                    .with_batch_size(8192)
                    .build()
                    .unwrap()
                    .map(|batch| batch.unwrap())
                    .collect::<Vec<_>>()
            })
            .collect()
    }
    #[test]
    fn parquet_bytes_are_identical_across_batch_spill_and_file_boundaries() {
        let parent = tempfile::tempdir().unwrap();
        let contract = contract(&[("id", json!("int64")), ("text", json!("string"))]);
        let schema = contract::arrow_schema(&contract).unwrap();
        let values: Vec<i64> = (0..12_000).rev().map(|i| i % 4097).collect();
        let strings: Vec<String> = values
            .iter()
            .map(|v| format!("{v:04}-{}", "abcdef".repeat(32)))
            .collect();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(values)),
                Arc::new(StringArray::from(strings)),
            ],
        )
        .unwrap();
        let mut a = Sorter::with_memory(contract.clone(), parent.path(), 1024 * 1024).unwrap();
        for begin in (0..batch.num_rows()).step_by(17) {
            a.append(&batch.slice(begin, 17.min(batch.num_rows() - begin)))
                .unwrap();
        }
        assert!(!a.spills.is_empty());
        let a = a.finish().unwrap();
        let mut b = Sorter::new(contract, parent.path()).unwrap();
        b.append(&batch).unwrap();
        let b = b.finish().unwrap();
        assert_eq!(a.row_count, 12_000);
        assert_eq!(a.files.len(), 3);
        assert_eq!(
            a.files
                .iter()
                .map(|f| (&f.name, f.size, &f.sha256, f.rows))
                .collect::<Vec<_>>(),
            b.files
                .iter()
                .map(|f| (&f.name, f.size, &f.sha256, f.rows))
                .collect::<Vec<_>>()
        );
        let mut previous = i64::MIN;
        let mut count = 0;
        for batch in read(&a) {
            let ids = downcast::<Int64Array>(batch.column(0)).unwrap();
            for value in ids.values() {
                assert!(*value >= previous);
                previous = *value;
                count += 1;
            }
        }
        assert_eq!(count, 12_000);
    }
    #[test]
    fn total_order_preserves_null_signed_zero_nan_bits_and_duplicate_rows() {
        let parent = tempfile::tempdir().unwrap();
        let contract = contract(&[("value", json!("float64"))]);
        let values = vec![
            None,
            Some(0.0),
            Some(-0.0),
            Some(f64::from_bits(0xfff8_0000_0000_0001)),
            Some(f64::from_bits(0x7ff8_0000_0000_0002)),
            Some(f64::NEG_INFINITY),
            Some(f64::INFINITY),
            Some(-0.0),
        ];
        let mut expected = values.clone();
        expected.sort_by(|a, b| match (a, b) {
            (None, None) => Ordering::Equal,
            (None, _) => Ordering::Less,
            (_, None) => Ordering::Greater,
            (Some(a), Some(b)) => a.total_cmp(b),
        });
        let batch = RecordBatch::try_new(
            contract::arrow_schema(&contract).unwrap(),
            vec![Arc::new(Float64Array::from(values))],
        )
        .unwrap();
        let mut sorter = Sorter::new(contract, parent.path()).unwrap();
        sorter.append(&batch).unwrap();
        let group = sorter.finish().unwrap();
        let batches = read(&group);
        let output = downcast::<Float64Array>(batches[0].column(0)).unwrap();
        assert_eq!(
            output
                .iter()
                .map(|v| v.map(f64::to_bits))
                .collect::<Vec<_>>(),
            expected
                .iter()
                .map(|v| v.map(f64::to_bits))
                .collect::<Vec<_>>()
        );
    }
    #[test]
    fn decimal_timestamp_binary_and_empty_schema_round_trip_exactly() {
        let parent = tempfile::tempdir().unwrap();
        let contract = contract(&[
            ("amount", json!({"decimal":{"precision":38,"scale":6}})),
            ("instant", json!({"timestamp":{"unit":"us","utc":true}})),
            ("bytes", json!("binary")),
        ]);
        let extreme = 99_999_999_999_999_999_999_999_999_999_999_999_999i128;
        let batch = RecordBatch::try_new(
            contract::arrow_schema(&contract).unwrap(),
            vec![
                Arc::new(
                    Decimal128Array::from(vec![Some(extreme), Some(-extreme), None])
                        .with_precision_and_scale(38, 6)
                        .unwrap(),
                ),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![Some(i64::MAX), Some(i64::MIN), None])
                        .with_timezone("UTC"),
                ),
                Arc::new(BinaryArray::from(vec![
                    Some(&[0, 255][..]),
                    Some(&[255, 0][..]),
                    None,
                ])),
            ],
        )
        .unwrap();
        let mut sorter = Sorter::new(contract.clone(), parent.path()).unwrap();
        sorter.append(&batch).unwrap();
        let group = sorter.finish().unwrap();
        let result = read(&group);
        assert_eq!(
            downcast::<Decimal128Array>(result[0].column(0))
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![None, Some(-extreme), Some(extreme)]
        );
        assert_eq!(
            downcast::<TimestampMicrosecondArray>(result[0].column(1))
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![None, Some(i64::MIN), Some(i64::MAX)]
        );
        let empty = Sorter::new(contract, parent.path())
            .unwrap()
            .finish()
            .unwrap();
        assert_eq!(empty.files.len(), 1);
        assert_eq!(empty.row_count, 0);
        assert!(empty.files[0].size > 0);
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(File::open(&empty.files[0].path).unwrap())
                .unwrap();
        assert_eq!(reader.schema().fields(), batch.schema().fields());
    }
    #[test]
    fn oversized_row_and_contract_mismatch_fail_before_adoption() {
        let parent = tempfile::tempdir().unwrap();
        let contract = contract(&[("text", json!("string"))]);
        let mut sorter = Sorter::with_memory(contract.clone(), parent.path(), 1024 * 1024).unwrap();
        let data = "x".repeat(1024 * 1024);
        let batch = RecordBatch::try_new(
            contract::arrow_schema(&contract).unwrap(),
            vec![Arc::new(StringArray::from(vec![data]))],
        )
        .unwrap();
        assert!(sorter.append(&batch).is_err());
        assert_eq!(sorter.count, 0);
        assert!(sorter.rows.is_empty());
        let wrong = RecordBatch::try_new(
            Arc::new(Schema::new(vec![arrow_schema::Field::new(
                "text",
                DataType::Int64,
                true,
            )])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        assert!(sorter.append(&wrong).is_err());
    }
    #[test]
    fn multi_pass_merge_has_bounded_fan_in_and_keeps_duplicates() {
        let parent = tempfile::tempdir().unwrap();
        let contract = contract(&[("id", json!("int64"))]);
        let mut sorter = Sorter::with_memory(contract, parent.path(), 1024 * 1024).unwrap();
        for index in (0..65).rev() {
            let path = sorter.next_path();
            let mut file = File::create(&path).unwrap();
            write_row(&mut file, &vec![Cell::Int(index % 7)]).unwrap();
            sorter.spills.push(path);
            sorter.count += 1;
        }
        let group = sorter.finish().unwrap();
        assert_eq!(group.row_count, 65);
        let result = read(&group);
        let values = downcast::<Int64Array>(result[0].column(0)).unwrap();
        assert_eq!(values.len(), 65);
        assert!(values.values().windows(2).all(|w| w[0] <= w[1]));
    }
}
