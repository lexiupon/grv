//! Preserve lexical numbers without allowing duplicate keys or serde sentinels.
//! Borrow raw slices so nested containers never copy entire subdocuments.
use serde::{
    Deserialize, Deserializer,
    de::{self, MapAccess, Visitor},
};
use serde_json::{Map, Value, value::RawValue};
use std::fmt;
struct RawObject<'a>(Vec<(String, &'a RawValue)>);
impl<'de> Deserialize<'de> for RawObject<'de> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = RawObject<'de>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("JSON object")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<RawObject<'de>, A::Error> {
                let mut out = Vec::new();
                let mut keys = std::collections::BTreeSet::new();
                while let Some(key) = a.next_key::<String>()? {
                    if !keys.insert(key.clone()) {
                        return Err(de::Error::custom("duplicate key"));
                    }
                    if key.starts_with("$serde_json::private::") {
                        return Err(de::Error::custom("reserved parser key"));
                    }
                    out.push((key, a.next_value::<&'de RawValue>()?));
                }
                Ok(RawObject(out))
            }
        }
        d.deserialize_map(V)
    }
}
pub fn parse(bytes: &[u8]) -> Result<Value, serde_json::Error> {
    let raw: &RawValue = serde_json::from_slice(bytes)?;
    fn decode(raw: &RawValue, depth: usize) -> Result<Value, serde_json::Error> {
        if depth > 128 {
            return Err(de::Error::custom("JSON nesting limit"));
        }
        match raw.get().as_bytes()[0] {
            b'{' => {
                let pairs: RawObject = serde_json::from_str(raw.get())?;
                let mut map = Map::new();
                for (key, value) in pairs.0 {
                    map.insert(key, decode(value, depth + 1)?);
                }
                Ok(Value::Object(map))
            }
            b'[' => {
                let values: Vec<&RawValue> = serde_json::from_str(raw.get())?;
                Ok(Value::Array(
                    values
                        .iter()
                        .map(|v| decode(v, depth + 1))
                        .collect::<Result<_, _>>()?,
                ))
            }
            b'-' | b'0'..=b'9' => {
                let number: serde_json::Number = serde_json::from_str(raw.get())?;
                if !number.as_f64().is_some_and(f64::is_finite) {
                    return Err(de::Error::custom("nonfinite number"));
                }
                Ok(Value::Number(number))
            }
            _ => serde_json::from_str(raw.get()),
        }
    }
    decode(raw, 0)
}
#[cfg(test)]
mod tests {
    #[test]
    fn rejects_duplicate_nested_keys_and_trailing_values() {
        for input in [
            br#"{"x":{"a":1,"a":2}}"#.as_slice(),
            b"{}{}",
            b"\xef\xbb\xbf{}",
            b"\xff",
            b"NaN",
            b"1e999",
            br#"{"$serde_json::private::Number":"1"}"#,
        ] {
            assert!(super::parse(input).is_err());
        }
    }
    #[test]
    fn numbers_keep_numeric_semantics_with_unified_features() {
        let value =
            super::parse(br#"{"float":1.25,"negative":-0.5,"large":9007199254740993}"#).unwrap();
        assert_eq!(value["float"].as_f64(), Some(1.25));
        assert_eq!(value["negative"].as_f64(), Some(-0.5));
        assert_eq!(value["large"].as_u64(), Some(9007199254740993));
    }
}
