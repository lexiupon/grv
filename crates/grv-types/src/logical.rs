//! Logical contracts shared by the host, adapters and canonical core.
use crate::{Name, ValidationError};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub logical_type: Value,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableContract {
    pub columns: Vec<Column>,
    pub partition_keys: Vec<Name>,
    pub extensions: Value,
    pub column_ext: Value,
}
/// Parse client v1 authoring spellings without changing the authored document.
/// Wire contracts and persisted file schemas use the returned logical value.
pub fn authoring_type(value: &str) -> Result<Value, ValidationError> {
    let simple = match value {
        "bool" => Some("boolean"),
        "int64" => Some("int64"),
        "double" => Some("float64"),
        "utf8" => Some("string"),
        "binary" => Some("binary"),
        "date32" => Some("date"),
        _ => None,
    };
    if let Some(value) = simple {
        return Ok(json!(value));
    }
    let logical = if let Some(inner) = value
        .strip_prefix("decimal128(")
        .and_then(|v| v.strip_suffix(')'))
    {
        let (precision, scale) = inner
            .split_once(',')
            .ok_or_else(|| ValidationError("invalid decimal type".into()))?;
        let precision: u8 = precision
            .parse()
            .map_err(|_| ValidationError("invalid decimal precision".into()))?;
        let scale: u8 = scale
            .parse()
            .map_err(|_| ValidationError("invalid decimal scale".into()))?;
        if value != format!("decimal128({precision},{scale})") {
            return Err(ValidationError("noncanonical decimal type".into()));
        }
        json!({"decimal":{"precision":precision,"scale":scale}})
    } else if let Some(inner) = value
        .strip_prefix("timestamp(")
        .and_then(|v| v.strip_suffix(')'))
    {
        let (unit, utc) = inner
            .strip_suffix(",UTC")
            .map_or((inner, false), |unit| (unit, true));
        json!({"timestamp":{"unit":unit,"utc":utc}})
    } else {
        return Err(ValidationError(format!("unsupported client type {value}")));
    };
    validate_logical_type(&logical)?;
    Ok(logical)
}

/// Client v1's supported subset of GRV storage logical types.
pub fn validate_logical_type(value: &Value) -> Result<(), ValidationError> {
    let valid = match value {
        Value::String(s) => {
            ["boolean", "int64", "float64", "string", "binary", "date"].contains(&s.as_str())
        }
        Value::Object(o) if o.len() == 1 => {
            if let Some(v) = o.get("decimal").and_then(Value::as_object) {
                v.len() == 2
                    && v.get("precision").and_then(Value::as_u64).is_some_and(|p| {
                        (1..=38).contains(&p)
                            && v.get("scale")
                                .and_then(Value::as_u64)
                                .is_some_and(|s| s <= p)
                    })
            } else if let Some(v) = o.get("timestamp").and_then(Value::as_object) {
                v.len() == 2
                    && v.get("utc").and_then(Value::as_bool).is_some_and(|utc| {
                        v.get("unit").and_then(Value::as_str).is_some_and(|unit| {
                            ["ms", "us", "ns"].contains(&unit) && !(utc && unit == "ns")
                        })
                    })
            } else {
                false
            }
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(ValidationError("unsupported client v1 logical type".into()))
    }
}
impl TableContract {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.columns.is_empty() || !self.extensions.is_object() || !self.column_ext.is_object() {
            return Err(ValidationError("invalid table contract".into()));
        }
        for (i, column) in self.columns.iter().enumerate() {
            if column.name.is_empty()
                || column.name.contains('\0')
                || self.columns[..i].iter().any(|c| c.name == column.name)
            {
                return Err(ValidationError("invalid or duplicate output column".into()));
            }
            validate_logical_type(&column.logical_type)?;
        }
        if self.partition_keys.iter().enumerate().any(|(i, k)| {
            self.partition_keys[..i].contains(k) || matches!(k.as_str(), "version" | "revision")
        }) {
            return Err(ValidationError(
                "duplicate or reserved partition key".into(),
            ));
        }
        if self
            .column_ext
            .as_object()
            .unwrap()
            .iter()
            .any(|(name, ext)| {
                !self.columns.iter().any(|column| &column.name == name) || !ext.is_object()
            })
        {
            return Err(ValidationError(
                "column extension does not name a column or is not a property map".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod authoring_tests {
    use super::*;
    #[test]
    fn client_authoring_aliases_have_one_exact_logical_projection() {
        for (authoring, logical) in [
            ("bool", "boolean"),
            ("double", "float64"),
            ("utf8", "string"),
            ("date32", "date"),
            ("int64", "int64"),
            ("binary", "binary"),
        ] {
            assert_eq!(authoring_type(authoring).unwrap(), json!(logical));
        }
        assert_eq!(
            authoring_type("decimal128(38,6)").unwrap(),
            json!({"decimal":{"precision":38,"scale":6}})
        );
        assert_eq!(
            authoring_type("timestamp(us,UTC)").unwrap(),
            json!({"timestamp":{"unit":"us","utc":true}})
        );
        for value in [
            "decimal128(1,2)",
            "decimal128(038,6)",
            "timestamp(ns,UTC)",
            "int32",
            "double ",
        ] {
            assert!(authoring_type(value).is_err(), "{value}");
        }
    }
}
