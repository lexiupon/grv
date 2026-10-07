//! RFC8785 serializer, independent of serde_json's arbitrary_precision feature.
use serde::{
    Serialize, Serializer,
    ser::{
        self, SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
        SerializeTupleStruct, SerializeTupleVariant,
    },
};
use serde_json::Value;

fn invalid(message: &str) -> serde_json::Error {
    <serde_json::Error as ser::Error>::custom(message)
}

pub(super) fn to_vec<T: Serialize>(value: &T) -> Result<Vec<u8>, serde_json::Error> {
    // serde_json::to_value ordinarily turns NaN/infinity into null. Recursively
    // guard floats before that lossy conversion, preserving scalar wrappers.
    let value = serde_json::to_value(Finite(value))?;
    let mut output = Vec::new();
    write_value(&value, &mut output)?;
    Ok(output)
}

fn write_value(value: &Value, output: &mut Vec<u8>) -> Result<(), serde_json::Error> {
    match value {
        Value::Null => output.extend_from_slice(b"null"),
        Value::Bool(true) => output.extend_from_slice(b"true"),
        Value::Bool(false) => output.extend_from_slice(b"false"),
        Value::String(text) => serde_json::to_writer(output, text)?,
        Value::Number(number) => {
            let text = number.to_string();
            let binary = number
                .as_f64()
                .filter(|n| n.is_finite())
                .ok_or_else(|| invalid("JCS numbers must be finite IEEE754 binary64 values"))?;
            // Unlike already-rounded f64 inputs, exact JSON integer inputs must
            // not silently lose information when adapted to I-JSON binary64.
            if !text.contains(['.', 'e', 'E']) {
                let unsigned = text.trim_start_matches('-').trim_start_matches('0');
                let normalized = if unsigned.is_empty() {
                    "0".into()
                } else if text.starts_with('-') {
                    format!("-{unsigned}")
                } else {
                    unsigned.into()
                };
                if exact_integer(binary).as_deref() != Some(&normalized) {
                    return Err(invalid(
                        "JCS integer is not exactly representable in IEEE754 binary64; encode it as a string",
                    ));
                }
            }
            let mut formatter = ryu_js::Buffer::new();
            output.extend_from_slice(formatter.format_finite(binary).as_bytes());
        }
        Value::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                write_value(value, output)?;
            }
            output.push(b']');
        }
        Value::Object(values) => {
            let mut entries: Vec<_> = values.iter().collect();
            entries.sort_by(|(left, _), (right, _)| left.encode_utf16().cmp(right.encode_utf16()));
            output.push(b'{');
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                serde_json::to_writer(&mut *output, key)?;
                output.push(b':');
                write_value(value, output)?;
            }
            output.push(b'}');
        }
    }
    Ok(())
}

/// Produce the mathematical integer represented by binary64, rather than its
/// shortest decimal rendering (which may round last decimal digits). At most
/// 309 digits are needed, so no unbounded integer dependency is required.
fn exact_integer(number: f64) -> Option<String> {
    if number == 0.0 {
        return Some("0".into());
    }
    let bits = number.to_bits();
    let exponent_bits = ((bits >> 52) & 0x7ff) as i32;
    if exponent_bits == 0 || exponent_bits == 0x7ff {
        return None;
    }
    let mantissa = (bits & ((1u64 << 52) - 1)) | (1u64 << 52);
    let exponent = exponent_bits - 1023 - 52;
    let mut digits = if exponent < 0 {
        let shift = exponent.unsigned_abs();
        if shift >= 64 || mantissa & ((1u64 << shift) - 1) != 0 {
            return None;
        }
        (mantissa >> shift).to_string().into_bytes()
    } else {
        let mut digits = mantissa.to_string().into_bytes();
        for _ in 0..exponent {
            let mut carry = 0;
            for digit in digits.iter_mut().rev() {
                let doubled = (*digit - b'0') * 2 + carry;
                *digit = b'0' + doubled % 10;
                carry = doubled / 10;
            }
            if carry != 0 {
                digits.insert(0, b'0' + carry);
            }
        }
        digits
    };
    if number.is_sign_negative() {
        digits.insert(0, b'-');
    }
    String::from_utf8(digits).ok()
}

struct Finite<'a, T: ?Sized>(&'a T);
impl<T: Serialize + ?Sized> Serialize for Finite<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(Guard(serializer))
    }
}

struct Guard<S>(S);
struct Compound<S>(S);
macro_rules! primitive {
    ($($method:ident($value:ident: $kind:ty)),* $(,)?) => {
        $(fn $method(self, $value: $kind) -> Result<Self::Ok, Self::Error> { self.0.$method($value) })*
    };
}
impl<S: Serializer> Serializer for Guard<S> {
    type Ok = S::Ok;
    type Error = S::Error;
    type SerializeSeq = Compound<S::SerializeSeq>;
    type SerializeTuple = Compound<S::SerializeTuple>;
    type SerializeTupleStruct = Compound<S::SerializeTupleStruct>;
    type SerializeTupleVariant = Compound<S::SerializeTupleVariant>;
    type SerializeMap = Compound<S::SerializeMap>;
    type SerializeStruct = Compound<S::SerializeStruct>;
    type SerializeStructVariant = Compound<S::SerializeStructVariant>;
    primitive!(
        serialize_bool(value: bool),
        serialize_i8(value: i8),
        serialize_i16(value: i16),
        serialize_i32(value: i32),
        serialize_i64(value: i64),
        serialize_i128(value: i128),
        serialize_u8(value: u8),
        serialize_u16(value: u16),
        serialize_u32(value: u32),
        serialize_u64(value: u64),
        serialize_u128(value: u128),
        serialize_char(value: char),
        serialize_str(value: &str),
        serialize_bytes(value: &[u8])
    );
    fn serialize_f32(self, value: f32) -> Result<Self::Ok, Self::Error> {
        if !value.is_finite() {
            return Err(ser::Error::custom("JCS numbers cannot be NaN or infinity"));
        }
        self.0.serialize_f32(value)
    }
    fn serialize_f64(self, value: f64) -> Result<Self::Ok, Self::Error> {
        if !value.is_finite() {
            return Err(ser::Error::custom("JCS numbers cannot be NaN or infinity"));
        }
        self.0.serialize_f64(value)
    }
    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_none()
    }
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_some(&Finite(value))
    }
    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_unit()
    }
    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_unit_struct(name)
    }
    fn serialize_unit_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_unit_variant(name, index, variant)
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_newtype_struct(name, &Finite(value))
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        self.0
            .serialize_newtype_variant(name, index, variant, &Finite(value))
    }
    fn serialize_seq(self, size: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        self.0.serialize_seq(size).map(Compound)
    }
    fn serialize_tuple(self, size: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.0.serialize_tuple(size).map(Compound)
    }
    fn serialize_tuple_struct(
        self,
        name: &'static str,
        size: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.0.serialize_tuple_struct(name, size).map(Compound)
    }
    fn serialize_tuple_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        size: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        self.0
            .serialize_tuple_variant(name, index, variant, size)
            .map(Compound)
    }
    fn serialize_map(self, size: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        self.0.serialize_map(size).map(Compound)
    }
    fn serialize_struct(
        self,
        name: &'static str,
        size: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        self.0.serialize_struct(name, size).map(Compound)
    }
    fn serialize_struct_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        size: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        self.0
            .serialize_struct_variant(name, index, variant, size)
            .map(Compound)
    }
    fn is_human_readable(&self) -> bool {
        self.0.is_human_readable()
    }
}
macro_rules! compound {
    ($trait:ident, $method:ident $(, $key:ident: $kind:ty)?) => {
        impl<S: $trait> $trait for Compound<S> {
            type Ok = S::Ok; type Error = S::Error;
            fn $method<T: Serialize + ?Sized>(&mut self, $($key: $kind,)? value: &T) -> Result<(), Self::Error> {
                self.0.$method($($key,)? &Finite(value))
            }
            fn end(self) -> Result<Self::Ok, Self::Error> { self.0.end() }
        }
    };
}
compound!(SerializeSeq, serialize_element);
compound!(SerializeTuple, serialize_element);
compound!(SerializeTupleStruct, serialize_field);
compound!(SerializeTupleVariant, serialize_field);
compound!(SerializeStruct, serialize_field, key: &'static str);
compound!(SerializeStructVariant, serialize_field, key: &'static str);
impl<S: SerializeMap> SerializeMap for Compound<S> {
    type Ok = S::Ok;
    type Error = S::Error;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Self::Error> {
        self.0.serialize_key(&Finite(key))
    }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.0.serialize_value(&Finite(value))
    }
    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn canonical(value: &impl Serialize) -> String {
        String::from_utf8(to_vec(value).unwrap()).unwrap()
    }

    #[test]
    fn rfc8785_number_samples_and_ecmascript_exponent_thresholds() {
        let vectors = [
            (0x0000000000000000, "0"),
            (0x8000000000000000, "0"),
            (0x0000000000000001, "5e-324"),
            (0x8000000000000001, "-5e-324"),
            (0x7fefffffffffffff, "1.7976931348623157e+308"),
            (0xffefffffffffffff, "-1.7976931348623157e+308"),
            (0x4340000000000000, "9007199254740992"),
            (0xc340000000000000, "-9007199254740992"),
            (0x4430000000000000, "295147905179352830000"),
            (0x44b52d02c7e14af5, "9.999999999999997e+22"),
            (0x44b52d02c7e14af6, "1e+23"),
            (0x44b52d02c7e14af7, "1.0000000000000001e+23"),
            (0x444b1ae4d6e2ef4e, "999999999999999700000"),
            (0x444b1ae4d6e2ef4f, "999999999999999900000"),
            (0x444b1ae4d6e2ef50, "1e+21"),
            (0x3eb0c6f7a0b5ed8c, "9.999999999999997e-7"),
            (0x3eb0c6f7a0b5ed8d, "0.000001"),
        ];
        for (bits, expected) in vectors {
            assert_eq!(canonical(&f64::from_bits(bits)), expected);
        }
    }

    #[test]
    fn arbitrary_precision_numbers_do_not_panic_and_integer_precision_is_explicit() {
        let value: Value = serde_json::from_str(
            "[333333333.33333329,1E30,4.50,2e-3,0.000000000000000000000000001,-0.0]",
        )
        .unwrap();
        assert_eq!(
            canonical(&value),
            "[333333333.3333333,1e+30,4.5,0.002,1e-27,0]"
        );
        assert_eq!(canonical(&9007199254740992u64), "9007199254740992");
        assert_eq!(
            canonical(&295147905179352825856u128),
            "295147905179352830000"
        );
        assert!(to_vec(&9007199254740993u64).is_err());
        assert!(to_vec(&u64::MAX).is_err());
        assert!(to_vec(&i64::MAX).is_err());
        let huge: Value = serde_json::from_str("1e999").unwrap();
        assert!(to_vec(&huge).is_err());
        assert_eq!(
            canonical(&crate::U64::new(i64::MAX as u64).unwrap()),
            "\"9223372036854775807\""
        );
    }

    #[test]
    fn rfc8785_property_names_sort_raw_utf16_recursively_without_normalization() {
        let value: Value = serde_json::from_str(r#"{"\u20ac":"Euro Sign","\r":"Carriage Return","\ufb33":"Hebrew Letter Dalet With Dagesh","1":"One","\ud83d\ude00":"Emoji: Grinning Face","\u0080":"Control","\u00f6":"Latin Small Letter O With Diaeresis"}"#).unwrap();
        assert_eq!(
            canonical(&value),
            "{\"\\r\":\"Carriage Return\",\"1\":\"One\",\"\u{80}\":\"Control\",\"ö\":\"Latin Small Letter O With Diaeresis\",\"€\":\"Euro Sign\",\"😀\":\"Emoji: Grinning Face\",\"דּ\":\"Hebrew Letter Dalet With Dagesh\"}"
        );
        let nested = serde_json::json!([{"\u{e000}":1,"😀":2}, "e\u{301}", "é", "\u{f}\n\t\"\\/"]);
        assert_eq!(
            canonical(&nested),
            "[{\"😀\":2,\"\u{e000}\":1},\"e\u{301}\",\"é\",\"\\u000f\\n\\t\\\"\\\\/\"]"
        );
    }

    #[test]
    fn nonfinite_floats_fail_in_nested_structures_instead_of_becoming_null() {
        #[derive(Serialize)]
        struct Nested {
            values: Vec<Option<f64>>,
        }
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(to_vec(&value).is_err());
            assert!(
                to_vec(&Nested {
                    values: vec![Some(value)]
                })
                .is_err()
            );
        }
        assert!(to_vec(&f32::INFINITY).is_err());
        assert_eq!(
            canonical(&Nested {
                values: vec![None, Some(0.5)]
            }),
            "{\"values\":[null,0.5]}"
        );
    }
}
