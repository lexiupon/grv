//! Exact lexical decoding; never route Salesforce numbers through binary f64.
use crate::{Result, integrity};
use chrono::DateTime;

/// Return the signed unscaled decimal128 coefficient. Scientific notation is
/// accepted losslessly; discarded fractional digits must all be zero.
pub fn decimal128(text: &str, precision: u8, scale: u8) -> Result<i128> {
    if !(1..=38).contains(&precision) || scale > precision {
        return Err(integrity("invalid decimal contract"));
    }
    let (negative, unsigned) = if let Some(value) = text.strip_prefix('-') {
        (true, value)
    } else if let Some(value) = text.strip_prefix('+') {
        (false, value)
    } else {
        (false, text)
    };
    let mut exponent_parts = unsigned.split(['e', 'E']);
    let mantissa = exponent_parts.next().unwrap_or("");
    let exponent: i64 = exponent_parts
        .next()
        .map(|v| v.parse())
        .transpose()
        .map_err(|_| integrity("invalid decimal exponent"))?
        .unwrap_or(0);
    if exponent_parts.next().is_some() {
        return Err(integrity("invalid decimal text"));
    }
    let mut parts = mantissa.split('.');
    let integer = parts.next().unwrap_or("");
    let fractional = parts.next().unwrap_or("");
    if parts.next().is_some()
        || integer.is_empty()
        || !integer
            .bytes()
            .chain(fractional.bytes())
            .all(|c| c.is_ascii_digit())
    {
        return Err(integrity("invalid decimal text"));
    }
    let mut digits = format!("{integer}{fractional}");
    let shift = exponent
        .checked_add(i64::from(scale))
        .and_then(|v| v.checked_sub(fractional.len() as i64))
        .ok_or_else(|| integrity("decimal exponent overflow"))?;
    if shift < 0 {
        let removed = shift.unsigned_abs();
        if removed >= digits.len() as u64 {
            if digits.bytes().any(|c| c != b'0') {
                return Err(integrity("decimal conversion would round"));
            }
            digits.clear();
            digits.push('0');
        } else {
            let retained = digits.len() - removed as usize;
            if digits[retained..].bytes().any(|c| c != b'0') {
                return Err(integrity("decimal conversion would round"));
            }
            digits.truncate(retained);
        }
    }
    let nonzero = digits.trim_start_matches('0');
    if nonzero.is_empty() {
        return Ok(0);
    }
    let appended = if shift > 0 { shift as u64 } else { 0 };
    if nonzero.len() as u64 + appended > u64::from(precision) {
        return Err(integrity("decimal precision overflow"));
    }
    let mut coefficient: i128 = nonzero
        .parse()
        .map_err(|_| integrity("decimal128 overflow"))?;
    for _ in 0..appended {
        coefficient *= 10;
    }
    Ok(if negative { -coefficient } else { coefficient })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampUnit {
    Millisecond,
    Microsecond,
    Nanosecond,
}

/// UTC-aware timestamps require an explicit offset. Unit downscaling may remove
/// only zero fractional digits, and leap seconds are rejected rather than
/// collapsed into an adjacent second. UTC nanoseconds are outside client v1.
pub fn timestamp_utc(text: &str, unit: TimestampUnit) -> Result<i64> {
    if unit == TimestampUnit::Nanosecond {
        return Err(integrity("UTC nanosecond timestamps are outside client v1"));
    }
    if !text.is_ascii() {
        return Err(integrity("invalid offset timestamp"));
    }
    // Chrono accepts excess fractional digits, so validate precision before it
    // can discard any. Zero suffixes are an exact representation.
    if let Some((_, tail)) = text.split_once('.') {
        let fraction: String = tail.chars().take_while(char::is_ascii_digit).collect();
        let supported = match unit {
            TimestampUnit::Millisecond => 3,
            TimestampUnit::Microsecond => 6,
            TimestampUnit::Nanosecond => unreachable!(),
        };
        if fraction.len() > supported && fraction[supported..].bytes().any(|c| c != b'0') {
            return Err(integrity("timestamp conversion would truncate precision"));
        }
    }
    // Salesforce commonly spells offsets +0000; RFC3339 spells +00:00.
    let normalized = if text.len() >= 5 {
        let offset = &text[text.len() - 5..];
        if matches!(offset.as_bytes()[0], b'+' | b'-')
            && offset[1..].bytes().all(|c| c.is_ascii_digit())
        {
            format!(
                "{}{}:{}",
                &text[..text.len() - 5],
                &offset[..3],
                &offset[3..]
            )
        } else {
            text.into()
        }
    } else {
        text.into()
    };
    let value = DateTime::parse_from_rfc3339(&normalized)
        .map_err(|_| integrity("invalid offset timestamp"))?;
    let nanos = value.timestamp_subsec_nanos();
    if nanos >= 1_000_000_000 {
        return Err(integrity(
            "leap second timestamp is not exactly representable",
        ));
    }
    let divisor = match unit {
        TimestampUnit::Millisecond => 1_000_000,
        TimestampUnit::Microsecond => 1_000,
        TimestampUnit::Nanosecond => unreachable!(),
    };
    if nanos % divisor != 0 {
        return Err(integrity("timestamp conversion would truncate precision"));
    }
    let ticks = 1_000_000_000 / i64::from(divisor);
    value
        .timestamp()
        .checked_mul(ticks)
        .and_then(|v| v.checked_add(i64::from(nanos / divisor)))
        .ok_or_else(|| integrity("timestamp overflow"))
}

/// `arbitrary_precision` serde_json preserves a REST JSON number's lexical
/// value. Strings and numeric tokens share the exact decimal decoder.
pub fn json_decimal128(
    value: &serde_json::Value,
    precision: u8,
    scale: u8,
) -> Result<Option<i128>> {
    match value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(text) => decimal128(text, precision, scale).map(Some),
        serde_json::Value::Number(number) => {
            decimal128(&number.to_string(), precision, scale).map(Some)
        }
        _ => Err(integrity(
            "decimal source must be a number, string, or null",
        )),
    }
}
