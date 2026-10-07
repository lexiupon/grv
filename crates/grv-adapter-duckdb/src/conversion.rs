//! Exact source widening. Variable values stay borrowed until an IPC byte
//! allowance has been reserved, so validation cannot create an uncredited copy.
use arrow_schema::{DataType, TimeUnit};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}
impl TickUnit {
    fn power(self) -> u32 {
        match self {
            Self::Second => 0,
            Self::Millisecond => 3,
            Self::Microsecond => 6,
            Self::Nanosecond => 9,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceType {
    Boolean,
    SignedInteger { bits: u8 },
    Float32,
    Float64,
    Utf8,
    Binary,
    Date32,
    Decimal128 { precision: u8, scale: u8 },
    Timestamp { unit: TickUnit, utc: bool },
    Unsupported(String),
}
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Scalar<'a> {
    Null,
    Boolean(bool),
    Integer(i64),
    Float32(f32),
    Float64(f64),
    Utf8(&'a [u8]),
    Binary(&'a [u8]),
    Date32(i32),
    Decimal128(i128),
    Timestamp(i64),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversionError(pub String);
impl std::fmt::Display for ConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for ConversionError {}
type Result<T> = std::result::Result<T, ConversionError>;
fn fail<T>(message: &str) -> Result<T> {
    Err(ConversionError(message.into()))
}

#[derive(Debug, Clone)]
pub struct Conversion {
    source: SourceType,
    decimal_scale_factor: i128,
    timestamp_factor: i64,
}
impl Conversion {
    /// Validate the schema even if every value is null. Nulls do not authorize
    /// narrowing, unsigned-to-signed conversions or timezone changes.
    pub fn prepare(source: SourceType, target: &DataType) -> Result<Self> {
        let mut result = Self {
            source: source.clone(),
            decimal_scale_factor: 1,
            timestamp_factor: 1,
        };
        match (&source, target) {
            (SourceType::Boolean, DataType::Boolean)
            | (SourceType::Float64, DataType::Float64)
            | (SourceType::Float32, DataType::Float64)
            | (SourceType::Utf8, DataType::Utf8)
            | (SourceType::Binary, DataType::Binary)
            | (SourceType::Date32, DataType::Date32) => {}
            (
                SourceType::SignedInteger {
                    bits: 8 | 16 | 32 | 64,
                },
                DataType::Int64,
            ) => {}
            (
                SourceType::Decimal128 { precision, scale },
                DataType::Decimal128(output_precision, output_scale),
            ) => {
                if !(1..=38).contains(precision)
                    || scale > precision
                    || !(1..=38).contains(output_precision)
                    || *output_scale < 0
                    || *output_scale as u8 > *output_precision
                    || (*output_scale as u8) < *scale
                    || output_precision - (*output_scale as u8) < precision - scale
                {
                    return fail("decimal output does not exactly widen source precision/scale");
                }
                result.decimal_scale_factor = 10_i128.pow(*output_scale as u32 - *scale as u32);
            }
            (SourceType::Timestamp { unit, utc }, DataType::Timestamp(output_unit, timezone)) => {
                let output_utc = match timezone.as_deref() {
                    None => false,
                    Some("UTC") => true,
                    _ => return fail("noncanonical output timestamp timezone"),
                };
                if output_utc != *utc || (output_utc && *output_unit == TimeUnit::Nanosecond) {
                    return fail("timestamp UTC flag/unit is unsupported or changed");
                }
                let output = match output_unit {
                    TimeUnit::Millisecond => TickUnit::Millisecond,
                    TimeUnit::Microsecond => TickUnit::Microsecond,
                    TimeUnit::Nanosecond => TickUnit::Nanosecond,
                    _ => return fail("unsupported client v1 output timestamp unit"),
                };
                if output.power() < unit.power() {
                    return fail("timestamp output unit narrows source");
                }
                result.timestamp_factor = 10_i64.pow(output.power() - unit.power());
            }
            _ => return fail("source type cannot exactly widen to the client v1 output type"),
        }
        Ok(result)
    }

    pub fn convert<'a>(&self, value: Scalar<'a>) -> Result<Scalar<'a>> {
        if value == Scalar::Null {
            return Ok(Scalar::Null);
        }
        match (&self.source, value) {
            (SourceType::Boolean, Scalar::Boolean(_))
            | (SourceType::Float64, Scalar::Float64(_))
            | (SourceType::Binary, Scalar::Binary(_)) => Ok(value),
            (SourceType::SignedInteger { bits }, Scalar::Integer(number)) => {
                if *bits < 64 && (number < -(1_i64 << (bits - 1)) || number >= 1_i64 << (bits - 1))
                {
                    return fail("integer value contradicts native source width");
                }
                Ok(value)
            }
            (SourceType::Float32, Scalar::Float32(number)) => {
                Ok(Scalar::Float64(f64::from(number)))
            }
            (SourceType::Utf8, Scalar::Utf8(bytes)) => {
                std::str::from_utf8(bytes)
                    .map_err(|_| ConversionError("invalid UTF-8 source value".into()))?;
                Ok(value)
            }
            (SourceType::Date32, Scalar::Date32(days)) => {
                if days == -i32::MAX || days == i32::MAX {
                    return fail("DuckDB infinite date has no exact GRV date representation");
                }
                Ok(value)
            }
            (SourceType::Decimal128 { precision, .. }, Scalar::Decimal128(number)) => {
                if number.unsigned_abs() >= 10_u128.pow(*precision as u32) {
                    return fail("decimal value exceeds native source precision");
                }
                number
                    .checked_mul(self.decimal_scale_factor)
                    .map(Scalar::Decimal128)
                    .ok_or_else(|| ConversionError("decimal widening overflow".into()))
            }
            (SourceType::Timestamp { .. }, Scalar::Timestamp(ticks)) => {
                if ticks == -i64::MAX || ticks == i64::MAX {
                    return fail(
                        "DuckDB infinite timestamp has no exact GRV timestamp representation",
                    );
                }
                ticks
                    .checked_mul(self.timestamp_factor)
                    .map(Scalar::Timestamp)
                    .ok_or_else(|| ConversionError("timestamp widening overflow".into()))
            }
            _ => fail("source value physical kind contradicts source schema"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decimals_widen_without_float_and_preserve_precision_boundaries() {
        let convert = Conversion::prepare(
            SourceType::Decimal128 {
                precision: 28,
                scale: 2,
            },
            &DataType::Decimal128(38, 12),
        )
        .unwrap();
        let positive = 10_i128.pow(28) - 1;
        assert_eq!(
            convert.convert(Scalar::Decimal128(positive)).unwrap(),
            Scalar::Decimal128(positive * 10_i128.pow(10))
        );
        assert_eq!(
            convert.convert(Scalar::Decimal128(-positive)).unwrap(),
            Scalar::Decimal128(-positive * 10_i128.pow(10))
        );
        assert!(
            convert
                .convert(Scalar::Decimal128(10_i128.pow(28)))
                .is_err()
        );
        assert!(
            Conversion::prepare(
                SourceType::Decimal128 {
                    precision: 28,
                    scale: 2
                },
                &DataType::Decimal128(28, 3)
            )
            .is_err()
        );
        assert!(
            Conversion::prepare(
                SourceType::Decimal128 {
                    precision: 28,
                    scale: 2
                },
                &DataType::Decimal128(28, 1)
            )
            .is_err()
        );
    }
    #[test]
    fn timestamps_require_exact_unit_and_utc_and_checked_multiplication() {
        let convert = Conversion::prepare(
            SourceType::Timestamp {
                unit: TickUnit::Microsecond,
                utc: false,
            },
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .unwrap();
        assert_eq!(
            convert.convert(Scalar::Timestamp(-123)).unwrap(),
            Scalar::Timestamp(-123000)
        );
        assert!(
            convert
                .convert(Scalar::Timestamp(i64::MAX / 1000 + 1))
                .is_err()
        );
        assert!(
            Conversion::prepare(
                SourceType::Timestamp {
                    unit: TickUnit::Microsecond,
                    utc: true
                },
                &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
            )
            .is_err()
        );
        assert!(
            Conversion::prepare(
                SourceType::Timestamp {
                    unit: TickUnit::Microsecond,
                    utc: true
                },
                &DataType::Timestamp(TimeUnit::Microsecond, None)
            )
            .is_err()
        );
        assert!(
            Conversion::prepare(
                SourceType::Timestamp {
                    unit: TickUnit::Microsecond,
                    utc: false
                },
                &DataType::Timestamp(TimeUnit::Millisecond, None)
            )
            .is_err()
        );
    }
    #[test]
    fn integer_and_float_widening_preserves_values_and_negative_zero() {
        let signed =
            Conversion::prepare(SourceType::SignedInteger { bits: 32 }, &DataType::Int64).unwrap();
        assert_eq!(
            signed.convert(Scalar::Integer(i32::MIN as i64)).unwrap(),
            Scalar::Integer(i32::MIN as i64)
        );
        assert!(
            signed
                .convert(Scalar::Integer(i32::MAX as i64 + 1))
                .is_err()
        );
        let float = Conversion::prepare(SourceType::Float32, &DataType::Float64).unwrap();
        let Scalar::Float64(zero) = float.convert(Scalar::Float32(-0.0)).unwrap() else {
            panic!()
        };
        assert_eq!(zero.to_bits(), (-0.0_f64).to_bits());
        assert!(
            Conversion::prepare(SourceType::Unsupported("UBIGINT".into()), &DataType::Int64)
                .is_err()
        );
    }
    #[test]
    fn variable_values_remain_borrowed_and_utf8_is_checked() {
        let bytes = b"source text";
        let conversion = Conversion::prepare(SourceType::Utf8, &DataType::Utf8).unwrap();
        let Scalar::Utf8(converted) = conversion.convert(Scalar::Utf8(bytes)).unwrap() else {
            panic!()
        };
        assert_eq!(converted.as_ptr(), bytes.as_ptr());
        assert!(conversion.convert(Scalar::Utf8(&[0xff])).is_err());
        assert!(Conversion::prepare(SourceType::Utf8, &DataType::Binary).is_err());
        assert_eq!(conversion.convert(Scalar::Null).unwrap(), Scalar::Null);
    }
    #[test]
    fn native_positive_and_negative_infinity_are_not_finite_values() {
        let date = Conversion::prepare(SourceType::Date32, &DataType::Date32).unwrap();
        assert!(date.convert(Scalar::Date32(i32::MAX)).is_err());
        assert!(date.convert(Scalar::Date32(-i32::MAX)).is_err());
        let timestamp = Conversion::prepare(
            SourceType::Timestamp {
                unit: TickUnit::Microsecond,
                utc: false,
            },
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .unwrap();
        assert!(timestamp.convert(Scalar::Timestamp(i64::MAX)).is_err());
        assert!(timestamp.convert(Scalar::Timestamp(-i64::MAX)).is_err());
    }
}
