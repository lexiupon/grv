//! One-tree source JSON decoder. A conservative allocation preflight accounts
//! retained input, Vec growth, number/string storage and BTree node overhead
//! before serde allocates the decoded tree. No nested RawValue copies occur.
use crate::{Result, integrity};
use serde::{
    Deserialize, Deserializer,
    de::{self, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Map, Value};
use std::fmt;
const NODE: usize = 2 * std::mem::size_of::<Value>();
const MAP_ENTRY: usize = 1024;
const RESERVED: &str = "$serde_json::private::";

fn preflight(bytes: &[u8], retained: usize, limit: usize) -> Result<()> {
    let mut charged = retained
        .checked_add(256 * 1024)
        .ok_or_else(|| integrity("source JSON allocation overflow"))?;
    let mut at = 0;
    let mut depth = 0usize;
    while at < bytes.len() {
        let start = at;
        let charge = match bytes[at] {
            b' ' | b'\n' | b'\r' | b'\t' | b',' | b':' => {
                at += 1;
                0
            }
            b'{' | b'[' => {
                depth += 1;
                if depth > 128 {
                    return Err(integrity("source JSON nesting exceeds budget"));
                }
                at += 1;
                NODE
            }
            b'}' | b']' => {
                depth = depth.saturating_sub(1);
                at += 1;
                0
            }
            b'"' => {
                at += 1;
                let mut escaped = false;
                while at < bytes.len() {
                    match bytes[at] {
                        b'\\' => {
                            escaped = true;
                            at = at.saturating_add(2)
                        }
                        b'"' => {
                            at += 1;
                            break;
                        }
                        _ => at += 1,
                    }
                }
                let mut next = at;
                while bytes.get(next).is_some_and(u8::is_ascii_whitespace) {
                    next += 1;
                }
                let key = bytes.get(next) == Some(&b':');
                let size = at
                    .saturating_sub(start)
                    .checked_mul(if escaped { 3 } else { 1 })
                    .ok_or_else(|| integrity("source JSON allocation overflow"))?;
                if key {
                    // Bound the temporary key decode before inspecting escaped
                    // reserved names. Syntax is checked by the actual decoder.
                    if charged
                        .checked_add(size + MAP_ENTRY)
                        .is_none_or(|sum| sum > limit)
                    {
                        return Err(integrity("source JSON exceeds aggregate source budget"));
                    }
                    let name: String = serde_json::from_slice(
                        bytes
                            .get(start..at)
                            .ok_or_else(|| integrity("invalid source JSON string"))?,
                    )
                    .map_err(|_| integrity("invalid source JSON key"))?;
                    if name.starts_with(RESERVED) {
                        return Err(integrity("reserved source JSON key"));
                    }
                    size + MAP_ENTRY
                } else {
                    size + NODE
                }
            }
            _ => {
                while bytes.get(at).is_some_and(|byte| {
                    !byte.is_ascii_whitespace() && !matches!(byte, b',' | b']' | b'}' | b':')
                }) {
                    at += 1;
                }
                // arbitrary_precision numbers may transiently own two strings.
                at.saturating_sub(start)
                    .checked_mul(2)
                    .and_then(|size| size.checked_add(NODE))
                    .ok_or_else(|| integrity("source JSON allocation overflow"))?
            }
        };
        charged = charged
            .checked_add(charge)
            .ok_or_else(|| integrity("source JSON allocation overflow"))?;
        if charged > limit {
            return Err(integrity("source JSON exceeds aggregate source budget"));
        }
    }
    Ok(())
}
struct Strict(Value);
impl<'de> Deserialize<'de> for Strict {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct JsonVisitor;
        impl<'de> Visitor<'de> for JsonVisitor {
            type Value = Strict;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("bounded source JSON")
            }
            fn visit_unit<E: de::Error>(self) -> std::result::Result<Strict, E> {
                Ok(Strict(Value::Null))
            }
            fn visit_bool<E: de::Error>(self, value: bool) -> std::result::Result<Strict, E> {
                Ok(Strict(Value::Bool(value)))
            }
            fn visit_i64<E: de::Error>(self, value: i64) -> std::result::Result<Strict, E> {
                Ok(Strict(Value::Number(value.into())))
            }
            fn visit_u64<E: de::Error>(self, value: u64) -> std::result::Result<Strict, E> {
                Ok(Strict(Value::Number(value.into())))
            }
            fn visit_f64<E: de::Error>(self, value: f64) -> std::result::Result<Strict, E> {
                serde_json::Number::from_f64(value)
                    .map(|number| Strict(Value::Number(number)))
                    .ok_or_else(|| E::custom("nonfinite source number"))
            }
            fn visit_str<E: de::Error>(self, value: &str) -> std::result::Result<Strict, E> {
                Ok(Strict(Value::String(value.into())))
            }
            fn visit_string<E: de::Error>(self, value: String) -> std::result::Result<Strict, E> {
                Ok(Strict(Value::String(value)))
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<Strict, A::Error> {
                let mut values = Vec::new();
                while let Some(Strict(value)) = seq.next_element()? {
                    values.push(value);
                }
                Ok(Strict(Value::Array(values)))
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<Strict, A::Error> {
                let mut values = Map::new();
                while let Some(key) = map.next_key::<String>()? {
                    if key == "$serde_json::private::Number" {
                        // This synthetic map is generated only for a numeric
                        // token; raw authored reserved keys were rejected first.
                        if !values.is_empty() {
                            return Err(de::Error::custom("reserved source number"));
                        }
                        let text = map.next_value::<String>()?;
                        let number: serde_json::Number = text.parse().map_err(de::Error::custom)?;
                        if !number.as_f64().is_some_and(f64::is_finite)
                            || map.next_key::<String>()?.is_some()
                        {
                            return Err(de::Error::custom("invalid source number"));
                        }
                        return Ok(Strict(Value::Number(number)));
                    }
                    if key.starts_with(RESERVED) || values.contains_key(&key) {
                        return Err(de::Error::custom("duplicate or reserved source key"));
                    }
                    let Strict(value) = map.next_value()?;
                    values.insert(key, value);
                }
                Ok(Strict(Value::Object(values)))
            }
        }
        deserializer.deserialize_any(JsonVisitor)
    }
}

pub fn parse(bytes: &[u8], retained: usize, limit: usize) -> Result<Value> {
    preflight(bytes, retained.max(bytes.len()), limit)?;
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let Strict(value) =
        Strict::deserialize(&mut deserializer).map_err(|_| integrity("invalid source JSON"))?;
    deserializer
        .end()
        .map_err(|_| integrity("trailing source JSON"))?;
    Ok(value)
}

/// Conservative decoded heap accounting for already-owned mock/metadata data.
/// Allocator arenas/RSS are outside the transfer-buffer contract.
pub fn heap_upper_bound(value: &Value) -> Result<usize> {
    fn add(a: usize, b: usize) -> Result<usize> {
        a.checked_add(b)
            .ok_or_else(|| integrity("source heap accounting overflow"))
    }
    Ok(match value {
        Value::String(text) => text.capacity(),
        Value::Number(number) => number.as_str().len().saturating_mul(2),
        Value::Array(values) => values.iter().try_fold(
            values
                .capacity()
                .saturating_mul(std::mem::size_of::<Value>()),
            |sum, value| add(sum, heap_upper_bound(value)?),
        )?,
        Value::Object(values) => values.iter().try_fold(
            values.len().saturating_mul(MAP_ENTRY),
            |sum, (key, value)| add(add(sum, key.capacity())?, heap_upper_bound(value)?),
        )?,
        _ => 0,
    })
}
