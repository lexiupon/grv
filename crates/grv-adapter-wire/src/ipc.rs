//! Validate the v1 two-message IPC envelope before decoding untrusted arrays.
use crate::{ProtocolError, Result, fail};
use arrow_array::RecordBatch;
use arrow_ipc::{Endianness, MessageHeader, MetadataVersion};

pub fn decode(payload: &[u8], expected_rows: u64) -> Result<RecordBatch> {
    if !payload.len().is_multiple_of(8) {
        return fail("unaligned IPC payload");
    }
    let mut offset = 0usize;
    for index in 0..2 {
        let header = payload
            .get(
                offset
                    ..offset
                        .checked_add(8)
                        .ok_or_else(|| bad("IPC offset overflow"))?,
            )
            .ok_or_else(|| bad("truncated IPC header"))?;
        if header[..4] != [255; 4] {
            return fail("legacy IPC marker");
        }
        let metadata = i32::from_le_bytes(header[4..8].try_into().unwrap());
        if metadata <= 0 || metadata % 8 != 0 {
            return fail("invalid IPC metadata length");
        }
        offset += 8;
        let end = offset
            .checked_add(metadata as usize)
            .ok_or_else(|| bad("IPC length overflow"))?;
        let bytes = payload
            .get(offset..end)
            .ok_or_else(|| bad("truncated IPC metadata"))?;
        let message =
            arrow_ipc::root_as_message(bytes).map_err(|_| bad("invalid FlatBuffers metadata"))?;
        if message.version() != MetadataVersion::V5
            || message.bodyLength() < 0
            || message.bodyLength() % 8 != 0
        {
            return fail("invalid IPC version/body length");
        }
        if index == 0 {
            if message.header_type() != MessageHeader::Schema || message.bodyLength() != 0 {
                return fail("IPC schema must be first with no body");
            }
            let schema = message
                .header_as_schema()
                .ok_or_else(|| bad("invalid IPC schema"))?;
            if schema.endianness() != Endianness::Little {
                return fail("big endian IPC schema");
            }
            if let Some(fs) = schema.fields() {
                let mut names = std::collections::BTreeSet::new();
                for field in fs {
                    validate_field(field)?;
                    if !names.insert(field.name().unwrap()) {
                        return fail("duplicate IPC column name");
                    }
                }
            }
        } else {
            if message.header_type() != MessageHeader::RecordBatch {
                return fail("IPC must contain exactly one record batch");
            }
            let batch = message
                .header_as_record_batch()
                .ok_or_else(|| bad("invalid record batch metadata"))?;
            if batch.compression().is_some()
                || batch.length() < 0
                || batch.length() as u64 != expected_rows
            {
                return fail("compressed IPC or incorrect row count");
            }
            if let Some(buffers) = batch.buffers() {
                for buffer in buffers {
                    let off = buffer.offset();
                    let len = buffer.length();
                    if off < 0
                        || len < 0
                        || off % 8 != 0
                        || off
                            .checked_add(len)
                            .is_none_or(|end| end > message.bodyLength())
                    {
                        return fail("IPC buffer outside body");
                    }
                }
            }
        }
        offset = end
            .checked_add(message.bodyLength() as usize)
            .ok_or_else(|| bad("IPC body overflow"))?;
        if offset > payload.len() {
            return fail("truncated IPC body");
        }
    }
    if offset != payload.len() {
        return fail("IPC trailing bytes, EOS or extra message");
    }
    let mut reader = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(payload), None)
        .map_err(|_| bad("IPC schema decoding failed"))?;
    let batch = reader
        .next()
        .ok_or_else(|| bad("missing IPC record batch"))?
        .map_err(|_| bad("IPC array validation failed"))?;
    if batch.num_rows() as u64 != expected_rows {
        return fail("decoded row count differs");
    }
    for array in batch.columns() {
        array
            .to_data()
            .validate_full()
            .map_err(|_| bad("invalid IPC array offsets/UTF-8"))?;
    }
    Ok(batch)
}

/// Arrow's conversion helpers panic on some semantically invalid enum values
/// even after FlatBuffers structural verification. Check the supported v1
/// primitives and their union bodies before calling those helpers.
pub fn validate_field(field: arrow_ipc::Field<'_>) -> Result<()> {
    if field.dictionary().is_some()
        || field
            .children()
            .is_some_and(|children| !children.is_empty())
        || field
            .name()
            .is_none_or(|name| name.is_empty() || name.contains('\0'))
    {
        return fail("invalid or unsupported IPC field");
    }
    let valid = match field.type_type() {
        arrow_ipc::Type::Bool => field.type_as_bool().is_some(),
        arrow_ipc::Type::Int => field
            .type_as_int()
            .is_some_and(|value| value.bitWidth() == 64 && value.is_signed()),
        arrow_ipc::Type::FloatingPoint => field
            .type_as_floating_point()
            .is_some_and(|value| value.precision() == arrow_ipc::Precision::DOUBLE),
        arrow_ipc::Type::Utf8 => field.type_as_utf_8().is_some(),
        arrow_ipc::Type::Binary => field.type_as_binary().is_some(),
        arrow_ipc::Type::Date => field
            .type_as_date()
            .is_some_and(|value| value.unit() == arrow_ipc::DateUnit::DAY),
        arrow_ipc::Type::Decimal => field.type_as_decimal().is_some_and(|value| {
            value.bitWidth() == 128
                && (1..=38).contains(&value.precision())
                && (0..=value.precision()).contains(&value.scale())
        }),
        arrow_ipc::Type::Timestamp => field.type_as_timestamp().is_some_and(|value| {
            let utc = match value.timezone() {
                None => false,
                Some("UTC") => true,
                _ => return false,
            };
            [
                arrow_ipc::TimeUnit::MILLISECOND,
                arrow_ipc::TimeUnit::MICROSECOND,
                arrow_ipc::TimeUnit::NANOSECOND,
            ]
            .contains(&value.unit())
                && !(utc && value.unit() == arrow_ipc::TimeUnit::NANOSECOND)
        }),
        _ => false,
    };
    if !valid {
        return fail("invalid or unsupported IPC logical type");
    }
    let mut keys = std::collections::BTreeSet::new();
    if let Some(metadata) = field.custom_metadata() {
        for entry in metadata {
            if entry.key().is_none()
                || entry.value().is_none()
                || !keys.insert(entry.key().unwrap())
            {
                return fail("invalid or duplicate IPC field metadata");
            }
        }
    }
    Ok(())
}
fn bad(message: &str) -> ProtocolError {
    ProtocolError::Invalid(message.into())
}
