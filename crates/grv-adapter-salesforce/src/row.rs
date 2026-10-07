//! Complete client primitive decoding and Arrow representation. Source metadata
//! must separately prove compatible field semantics; no decimal uses f64.
use crate::{
    Result,
    config::field_path,
    conversion::{self, TimestampUnit},
    integrity,
};
use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
    RecordBatch, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use base64::{Engine, engine::general_purpose::STANDARD};
use grv_adapter_api::{ExtractTable, TableContract};
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};

pub fn selectors(table: &ExtractTable) -> Result<Vec<String>> {
    let mappings = table
        .columns
        .as_array()
        .ok_or_else(|| integrity("column mappings must be expanded"))?;
    table
        .contract
        .columns
        .iter()
        .map(|column| {
            let mapping = mappings
                .iter()
                .find(|mapping| mapping["name"] == column.name)
                .ok_or_else(|| integrity("projected column mapping is missing"))?;
            let source = mapping
                .get("source")
                .and_then(Value::as_str)
                .unwrap_or(&column.name);
            if !field_path(source) {
                return Err(integrity("invalid projected field selector"));
            }
            Ok(source.into())
        })
        .collect()
}

pub fn field<'a>(row: &'a Value, source: &str) -> Result<&'a Value> {
    if let Some(flat) = lookup(row, source)? {
        return Ok(flat);
    }
    let mut value = row;
    for part in source.split('.') {
        if value.is_null() {
            return Ok(value);
        }
        value = lookup(value, part)?
            .ok_or_else(|| integrity("source row lacks a declared projected field"))?;
    }
    Ok(value)
}

fn lookup<'a>(value: &'a Value, name: &str) -> Result<Option<&'a Value>> {
    let Some(object) = value.as_object() else {
        return Ok(None);
    };
    let mut matches = object
        .iter()
        .filter(|(key, _)| key.eq_ignore_ascii_case(name));
    let value = matches.next().map(|(_, value)| value);
    if matches.next().is_some() {
        return Err(integrity(
            "source row has ambiguous case-insensitive field keys",
        ));
    }
    Ok(value)
}

fn number_text(value: &Value) -> Result<String> {
    match value {
        Value::String(text) => Ok(text.clone()),
        Value::Number(number) => Ok(number.to_string()),
        _ => Err(integrity(
            "numeric source is not an exact number or text token",
        )),
    }
}
pub fn int64(value: &Value) -> Result<Option<i64>> {
    if value.is_null() {
        return Ok(None);
    }
    let coefficient = conversion::decimal128(&number_text(value)?, 19, 0)?;
    i64::try_from(coefficient)
        .map(Some)
        .map_err(|_| integrity("int64 source overflows contract"))
}

fn numeric_identity(text: &str) -> Result<(bool, String, i64)> {
    let negative = text.starts_with('-');
    let unsigned = text.trim_start_matches(['-', '+']);
    let (mantissa, exponent) = unsigned.split_once(['e', 'E']).unwrap_or((unsigned, "0"));
    let exponent: i64 = exponent
        .parse()
        .map_err(|_| integrity("invalid float exponent"))?;
    let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if integer.is_empty()
        || !integer
            .bytes()
            .chain(fraction.bytes())
            .all(|c| c.is_ascii_digit())
    {
        return Err(integrity("invalid float token"));
    }
    let mut digits = format!("{integer}{fraction}")
        .trim_start_matches('0')
        .to_owned();
    if digits.is_empty() {
        return Ok((false, "0".into(), 0));
    }
    let mut exponent = exponent
        .checked_sub(fraction.len() as i64)
        .ok_or_else(|| integrity("float exponent overflows"))?;
    while digits.ends_with('0') {
        digits.pop();
        exponent = exponent
            .checked_add(1)
            .ok_or_else(|| integrity("float exponent overflows"))?;
    }
    Ok((negative, digits, exponent))
}

/// Finite floating point text must round-trip to the same decimal token value.
/// This rejects hidden rounding of long tokens or integers; source metadata
/// decides whether the field itself has binary64 or decimal semantics.
pub fn float64(value: &Value) -> Result<Option<f64>> {
    if value.is_null() {
        return Ok(None);
    }
    let text = number_text(value)?;
    let binary = text
        .parse::<f64>()
        .ok()
        .filter(|n| n.is_finite())
        .ok_or_else(|| integrity("invalid finite float64 token"))?;
    let mut buffer = ryu_js::Buffer::new();
    if numeric_identity(&text)? != numeric_identity(buffer.format_finite(binary))? {
        return Err(integrity("float64 parsing would round the source token"));
    }
    Ok(Some(if binary == 0.0 { 0.0 } else { binary }))
}

pub fn date32(text: &str) -> Result<i32> {
    if text.len() != 10 || !text.is_ascii() || &text[4..5] != "-" || &text[7..8] != "-" {
        return Err(integrity("invalid date source"));
    }
    let date = chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .map_err(|_| integrity("invalid date source"))?;
    i32::try_from(
        date.signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days(),
    )
    .map_err(|_| integrity("date32 overflow"))
}

pub fn timestamp(text: &str, unit: TimestampUnit, utc: bool) -> Result<i64> {
    if utc {
        return conversion::timestamp_utc(text, unit);
    }
    if !text.is_ascii() {
        return Err(integrity("invalid local timestamp source"));
    }
    if let Some((_, tail)) = text.split_once('.') {
        let supported = match unit {
            TimestampUnit::Millisecond => 3,
            TimestampUnit::Microsecond => 6,
            TimestampUnit::Nanosecond => 9,
        };
        if tail.len() > supported && tail[supported..].bytes().any(|c| c != b'0') {
            return Err(integrity("local timestamp precision would be truncated"));
        }
    }
    let naive = chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f")
        .map_err(|_| integrity("invalid local timestamp source"))?;
    let value = naive.and_utc();
    let nanos = value.timestamp_subsec_nanos();
    if nanos >= 1_000_000_000 {
        return Err(integrity("leap second source is not exactly representable"));
    }
    let divisor = match unit {
        TimestampUnit::Millisecond => 1_000_000,
        TimestampUnit::Microsecond => 1_000,
        TimestampUnit::Nanosecond => 1,
    };
    if nanos % divisor != 0 {
        return Err(integrity("local timestamp precision would be truncated"));
    }
    value
        .timestamp()
        .checked_mul(1_000_000_000 / i64::from(divisor))
        .and_then(|v| v.checked_add(i64::from(nanos / divisor)))
        .ok_or_else(|| integrity("timestamp overflows declared unit"))
}

fn datatype(value: &Value) -> DataType {
    match value.as_str() {
        Some("boolean") => DataType::Boolean,
        Some("int64") => DataType::Int64,
        Some("float64") => DataType::Float64,
        Some("string") => DataType::Utf8,
        Some("binary") => DataType::Binary,
        Some("date") => DataType::Date32,
        _ if value.get("decimal").is_some() => DataType::Decimal128(
            value["decimal"]["precision"].as_u64().unwrap() as u8,
            value["decimal"]["scale"].as_u64().unwrap() as i8,
        ),
        _ => DataType::Timestamp(
            match value["timestamp"]["unit"].as_str().unwrap() {
                "ms" => TimeUnit::Millisecond,
                "us" => TimeUnit::Microsecond,
                "ns" => TimeUnit::Nanosecond,
                _ => unreachable!(),
            },
            value["timestamp"]["utc"]
                .as_bool()
                .unwrap()
                .then(|| Arc::from("UTC")),
        ),
    }
}
fn schema(contract: &TableContract) -> Result<Arc<Schema>> {
    contract
        .validate()
        .map_err(|_| integrity("invalid extraction contract"))?;
    let fields: Result<Vec<_>> = contract
        .columns
        .iter()
        .map(|column| {
            let mut metadata = HashMap::new();
            if let Some(extension) = contract.column_ext.get(&column.name) {
                metadata.insert(
                    "grv.column.ext".into(),
                    String::from_utf8(
                        grv_types::canonical_json(extension)
                            .map_err(|_| integrity("invalid column extensions"))?,
                    )
                    .unwrap(),
                );
            }
            Ok(
                Field::new(&column.name, datatype(&column.logical_type), true)
                    .with_metadata(metadata),
            )
        })
        .collect();
    let metadata = HashMap::from([(
        "grv.table.extensions".into(),
        String::from_utf8(
            grv_types::canonical_json(&contract.extensions)
                .map_err(|_| integrity("invalid table extensions"))?,
        )
        .unwrap(),
    )]);
    Ok(Arc::new(Schema::new_with_metadata(fields?, metadata)))
}

pub fn decode_rows(
    contract: &TableContract,
    sources: &[String],
    rows: &[Value],
) -> Result<RecordBatch> {
    let schema = schema(contract)?;
    if sources.len() != contract.columns.len() || sources.iter().any(|source| !field_path(source)) {
        return Err(integrity("source projection differs from contract"));
    }
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for (column, source) in contract.columns.iter().zip(sources) {
        let values: Result<Vec<_>> = rows.iter().map(|row| field(row, source)).collect();
        let values = values?;
        macro_rules! typed {
            ($array:ident, $decode:expr) => {{
                let decoded: Result<Vec<_>> = values
                    .iter()
                    .map(|value| {
                        if value.is_null() {
                            Ok(None)
                        } else {
                            ($decode)(value).map(Some)
                        }
                    })
                    .collect();
                Arc::new($array::from(decoded?)) as ArrayRef
            }};
        }
        let logical = &column.logical_type;
        let array = match logical.as_str() {
            Some("boolean") => typed!(BooleanArray, |v: &Value| v
                .as_bool()
                .ok_or_else(|| integrity("boolean source is not a boolean"))),
            Some("int64") => Arc::new(Int64Array::from(
                values
                    .iter()
                    .map(|v| int64(v))
                    .collect::<Result<Vec<_>>>()?,
            )) as ArrayRef,
            Some("float64") => Arc::new(Float64Array::from(
                values
                    .iter()
                    .map(|v| float64(v))
                    .collect::<Result<Vec<_>>>()?,
            )) as ArrayRef,
            Some("string") => typed!(StringArray, |v: &Value| v
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| integrity("string source is not text"))),
            Some("binary") => {
                let binary: Result<Vec<_>> =
                    values
                        .iter()
                        .map(|value| {
                            if value.is_null() {
                                Ok(None)
                            } else {
                                STANDARD
                                    .decode(value.as_str().ok_or_else(|| {
                                        integrity("binary source is not base64 text")
                                    })?)
                                    .map(Some)
                                    .map_err(|_| integrity("invalid exact base64 binary source"))
                            }
                        })
                        .collect();
                let binary = binary?;
                Arc::new(BinaryArray::from_iter(binary.iter().map(|v| v.as_deref()))) as ArrayRef
            }
            Some("date") => typed!(Date32Array, |v: &Value| date32(
                v.as_str()
                    .ok_or_else(|| integrity("date source is not text"))?
            )),
            _ if logical.get("decimal").is_some() => {
                let precision = logical["decimal"]["precision"].as_u64().unwrap() as u8;
                let scale = logical["decimal"]["scale"].as_u64().unwrap() as u8;
                let coefficients: Result<Vec<_>> = values
                    .iter()
                    .map(|v| conversion::json_decimal128(v, precision, scale))
                    .collect();
                Arc::new(
                    Decimal128Array::from(coefficients?)
                        .with_precision_and_scale(precision, scale as i8)
                        .map_err(|_| integrity("decimal contract failed"))?,
                ) as ArrayRef
            }
            _ => {
                let utc = logical["timestamp"]["utc"].as_bool().unwrap();
                let unit = match logical["timestamp"]["unit"].as_str().unwrap() {
                    "ms" => TimestampUnit::Millisecond,
                    "us" => TimestampUnit::Microsecond,
                    "ns" => TimestampUnit::Nanosecond,
                    _ => unreachable!(),
                };
                let ticks: Result<Vec<_>> = values
                    .iter()
                    .map(|v| {
                        if v.is_null() {
                            Ok(None)
                        } else {
                            timestamp(
                                v.as_str()
                                    .ok_or_else(|| integrity("timestamp source is not text"))?,
                                unit,
                                utc,
                            )
                            .map(Some)
                        }
                    })
                    .collect();
                let ticks = ticks?;
                macro_rules! timestamps {
                    ($array:ident) => {{
                        let array = $array::from(ticks);
                        Arc::new(if utc {
                            array.with_timezone("UTC")
                        } else {
                            array
                        }) as ArrayRef
                    }};
                }
                match unit {
                    TimestampUnit::Millisecond => timestamps!(TimestampMillisecondArray),
                    TimestampUnit::Microsecond => timestamps!(TimestampMicrosecondArray),
                    TimestampUnit::Nanosecond => timestamps!(TimestampNanosecondArray),
                }
            }
        };
        arrays.push(array);
    }
    RecordBatch::try_new(schema, arrays)
        .map_err(|_| integrity("decoded rows differ from declared contract"))
}

pub fn source_text_bytes(value: &Value) -> usize {
    match value {
        Value::String(text) => text.len(),
        Value::Number(number) => number.as_str().len(),
        _ => 0,
    }
}

/// Budget before converting. This covers growing Arrow buffers, temporary
/// option/reference vectors, encoded IPC bodies, output growth and schema JSON.
pub fn scratch_upper_bound(
    contract: &TableContract,
    sources: &[String],
    rows: &[Value],
) -> Result<usize> {
    let metadata = crate::source_json::heap_upper_bound(&contract.extensions)?
        .checked_add(crate::source_json::heap_upper_bound(&contract.column_ext)?)
        .and_then(|size| size.checked_mul(4))
        .ok_or_else(|| integrity("scratch accounting overflow"))?;
    let base = contract
        .columns
        .len()
        .checked_mul(2048)
        .and_then(|size| size.checked_add(metadata))
        .and_then(|size| size.checked_add(256 * 1024))
        .ok_or_else(|| integrity("scratch accounting overflow"))?;
    rows.iter().try_fold(base, |sum, row| {
        sources.iter().try_fold(sum, |sum, source| {
            let text = source_text_bytes(field(row, source)?);
            sum.checked_add(
                text.checked_mul(6)
                    .and_then(|size| size.checked_add(64))
                    .ok_or_else(|| integrity("scratch accounting overflow"))?,
            )
            .ok_or_else(|| integrity("scratch accounting overflow"))
        })
    })
}

struct BoundedOutput {
    bytes: Vec<u8>,
    limit: usize,
}
impl std::io::Write for BoundedOutput {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let needed = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .filter(|size| *size <= self.limit)
            .ok_or_else(|| std::io::Error::other("bounded IPC output exhausted"))?;
        if self.bytes.capacity() < needed {
            let target = self
                .bytes
                .capacity()
                .saturating_mul(2)
                .max(needed)
                .min(self.limit);
            self.bytes.reserve_exact(target - self.bytes.len());
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub fn ipc(batch: &RecordBatch, limit: usize) -> Result<Vec<u8>> {
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(
        BoundedOutput {
            bytes: Vec::new(),
            limit: limit.saturating_add(8),
        },
        batch.schema().as_ref(),
    )
    .map_err(|_| integrity("IPC schema encoding failed"))?;
    writer
        .write(batch)
        .map_err(|_| integrity("IPC rows encoding failed"))?;
    let mut bytes = writer
        .into_inner()
        .map_err(|_| integrity("IPC finalization failed"))?
        .bytes;
    if bytes.ends_with(&[255, 255, 255, 255, 0, 0, 0, 0]) {
        bytes.truncate(bytes.len() - 8);
    } else {
        return Err(integrity("unexpected IPC termination"));
    }
    if bytes.len() > limit {
        return Err(integrity("IPC batch exceeds negotiated resource budget"));
    }
    Ok(bytes)
}
