//! Exact native-window conversion to the protocol's schema + one Arrow batch.
use crate::{
    conversion::{Conversion, Scalar},
    native::NativeWindow,
};
use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
    RecordBatch, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray,
};
use arrow_schema::{DataType, Schema, TimeUnit};
use std::{
    io::{self, Write},
    sync::Arc,
};

fn schema_text_bytes(schema: &Schema) -> io::Result<usize> {
    schema
        .metadata()
        .iter()
        .map(|(key, value)| key.len() + value.len())
        .chain(schema.fields().iter().flat_map(|field| {
            std::iter::once(field.name().len()).chain(
                field
                    .metadata()
                    .iter()
                    .map(|(key, value)| key.len() + value.len()),
            )
        }))
        .try_fold(0usize, |sum, size| {
            sum.checked_add(size)
                .ok_or_else(|| io::Error::other("schema metadata byte count overflow"))
        })
}
/// Conservative admission before native transfer allocation. Account for
/// variable-array offsets and f32 widening without limiting every row to half
/// the offered batch size. The native window contains at most2,048rows.
pub fn native_allowance(
    schema: &Schema,
    types: &[crate::conversion::SourceType],
    batch: usize,
    scratch: usize,
) -> io::Result<usize> {
    use crate::conversion::SourceType;
    if types.len() != schema.fields().len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "native allowance schema mismatch",
        ));
    }
    let difference = types
        .iter()
        .map(|kind| match kind {
            SourceType::Utf8 | SourceType::Binary => 16usize,
            SourceType::Float32 => 4,
            _ => 0,
        })
        .sum::<usize>();
    let text = schema_text_bytes(schema)?;
    let per_window = difference
        .checked_mul(2048)
        .ok_or_else(|| io::Error::other("window overhead overflow"))?;
    let metadata = schema.fields().len() * 2048
        + 4096
        + 2048 * 64
        + text
            .checked_mul(4)
            .ok_or_else(|| io::Error::other("metadata overhead overflow"))?;
    let scratch_fixed = batch
        .checked_add(metadata)
        .and_then(|value| value.checked_add(per_window * 4))
        .ok_or_else(|| io::Error::other("scratch admission overflow"))?;
    let batch_fixed = schema.fields().len() * 4096 + 4096 + per_window + text * 4;
    Ok(batch
        .saturating_sub(batch_fixed)
        .min(scratch.saturating_sub(scratch_fixed) / 4))
}

pub fn encode(
    window: &NativeWindow,
    schema: Arc<Schema>,
    batch_allowance: usize,
    scratch_allowance: usize,
) -> io::Result<Vec<u8>> {
    if schema.fields().len() != window.source_types().len()
        || batch_allowance == 0
        || batch_allowance > crate::MAX_BATCH_BYTES
        || scratch_allowance > crate::SCRATCH_BUDGET_BYTES
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid Arrow acquisition schema/budgets",
        ));
    }
    let conversions = window
        .source_types()
        .iter()
        .zip(schema.fields())
        .map(|(source, field)| {
            Conversion::prepare(source.clone(), field.data_type()).map_err(io::Error::other)
        })
        .collect::<io::Result<Vec<_>>>()?;
    // Native wire values are at least as wide as their Arrow data buffers except
    // decimal/timestamp widening. Four copies plus bounded IPC/schema metadata
    // cover temporary primitive vectors, finished arrays, IPC generator data
    // and the outbound writer. Admission happens before allocating any arrays.
    let mut array_bytes = 0usize;
    window.visit(|_, column, value| {
        let converted = conversions[column]
            .convert(value)
            .map_err(io::Error::other)?;
        if converted == Scalar::Null && !schema.field(column).is_nullable() {
            return Err(io::Error::other("NULL violates output contract"));
        }
        let width = match schema.field(column).data_type() {
            DataType::Boolean => 2,
            DataType::Int64 | DataType::Float64 | DataType::Timestamp(..) => 9,
            DataType::Date32 => 5,
            DataType::Decimal128(..) => 17,
            DataType::Utf8 | DataType::Binary => {
                25 + match converted {
                    Scalar::Utf8(bytes) | Scalar::Binary(bytes) => bytes.len(),
                    _ => 0,
                }
            }
            _ => return Err(io::Error::other("unsupported output Arrow type")),
        };
        array_bytes = array_bytes
            .checked_add(width)
            .ok_or_else(|| io::Error::other("conversion byte count overflow"))?;
        Ok(())
    })?;
    let text_bytes = schema_text_bytes(&schema)?;
    let metadata_bytes =
        schema.fields().len() * 2048 + 4096 + window.row_count() as usize * 64 + text_bytes * 4;
    if array_bytes
        .checked_mul(4)
        .and_then(|count| count.checked_add(batch_allowance + metadata_bytes))
        .is_none_or(|count| count > scratch_allowance)
    {
        return Err(io::Error::other(
            "Arrow conversion exceeds negotiated scratch budget",
        ));
    }
    let mut arrays = Vec::with_capacity(schema.fields().len());
    for (column, field) in schema.fields().iter().enumerate() {
        let mut scalars = Vec::with_capacity(window.row_count() as usize);
        window.visit(|_, index, value| {
            if column == index {
                scalars.push(
                    conversions[column]
                        .convert(value)
                        .map_err(io::Error::other)?,
                );
            }
            Ok(())
        })?;
        macro_rules! primitive {
            ($variant:ident,$array:ty) => {{
                let values = scalars
                    .iter()
                    .map(|value| match value {
                        Scalar::Null => Ok(None),
                        Scalar::$variant(number) => Ok(Some(*number)),
                        _ => Err(io::Error::other("converted scalar kind mismatch")),
                    })
                    .collect::<io::Result<Vec<_>>>()?;
                Arc::new(<$array>::from(values)) as ArrayRef
            }};
        }
        let array: ArrayRef = match field.data_type() {
            DataType::Boolean => primitive!(Boolean, BooleanArray),
            DataType::Int64 => primitive!(Integer, Int64Array),
            DataType::Float64 => primitive!(Float64, Float64Array),
            DataType::Date32 => primitive!(Date32, Date32Array),
            DataType::Decimal128(precision, scale) => {
                let values = scalars
                    .iter()
                    .map(|value| match value {
                        Scalar::Null => Ok(None),
                        Scalar::Decimal128(number) => Ok(Some(*number)),
                        _ => Err(io::Error::other("decimal scalar kind mismatch")),
                    })
                    .collect::<io::Result<Vec<_>>>()?;
                Arc::new(
                    Decimal128Array::from(values)
                        .with_precision_and_scale(*precision, *scale)
                        .map_err(io::Error::other)?,
                )
            }
            DataType::Timestamp(unit, zone) => {
                let values = scalars
                    .iter()
                    .map(|value| match value {
                        Scalar::Null => Ok(None),
                        Scalar::Timestamp(number) => Ok(Some(*number)),
                        _ => Err(io::Error::other("timestamp scalar kind mismatch")),
                    })
                    .collect::<io::Result<Vec<_>>>()?;
                match unit {
                    TimeUnit::Millisecond => Arc::new(
                        TimestampMillisecondArray::from(values).with_timezone_opt(zone.clone()),
                    ),
                    TimeUnit::Microsecond => Arc::new(
                        TimestampMicrosecondArray::from(values).with_timezone_opt(zone.clone()),
                    ),
                    TimeUnit::Nanosecond => Arc::new(
                        TimestampNanosecondArray::from(values).with_timezone_opt(zone.clone()),
                    ),
                    _ => return Err(io::Error::other("unsupported timestamp output unit")),
                }
            }
            DataType::Utf8 => {
                let values = scalars
                    .iter()
                    .map(|value| match value {
                        Scalar::Null => Ok(None),
                        Scalar::Utf8(bytes) => std::str::from_utf8(bytes)
                            .map(Some)
                            .map_err(io::Error::other),
                        _ => Err(io::Error::other("string scalar kind mismatch")),
                    })
                    .collect::<io::Result<Vec<_>>>()?;
                Arc::new(StringArray::from(values))
            }
            DataType::Binary => {
                let values = scalars
                    .iter()
                    .map(|value| match value {
                        Scalar::Null => Ok(None),
                        Scalar::Binary(bytes) => Ok(Some(*bytes)),
                        _ => Err(io::Error::other("binary scalar kind mismatch")),
                    })
                    .collect::<io::Result<Vec<_>>>()?;
                Arc::new(BinaryArray::from(values))
            }
            _ => return Err(io::Error::other("unsupported Arrow output type")),
        };
        arrays.push(array);
    }
    let batch = RecordBatch::try_new(schema.clone(), arrays).map_err(io::Error::other)?;
    let mut output = BoundedOutput {
        bytes: Vec::with_capacity(batch_allowance),
        limit: batch_allowance,
    };
    {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut output, &schema)
            .map_err(io::Error::other)?;
        writer.write(&batch).map_err(io::Error::other)?;
        // No finish(): v1 carries schema+exactly one record batch with no EOS.
    }
    Ok(output.bytes)
}
struct BoundedOutput {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for BoundedOutput {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit - self.bytes.len() {
            return Err(io::Error::other("Arrow batch exceeds reserved credit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
