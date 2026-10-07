//! One mapping from client authoring spellings through GRV to Arrow.
use crate::{Error, Result};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use grv_adapter_api::TableContract;
use serde_json::Value;
#[cfg(test)]
use serde_json::json;
use std::{collections::HashMap, sync::Arc};

pub fn authoring_type(value: &str) -> Result<Value> {
    grv_types::authoring_type(value).map_err(|e| Error(e.to_string()))
}

pub fn arrow_type(logical: &Value) -> Result<DataType> {
    grv_adapter_api::validate_logical_type(logical).map_err(|e| Error(e.to_string()))?;
    Ok(match logical.as_str() {
        Some("boolean") => DataType::Boolean,
        Some("int64") => DataType::Int64,
        Some("float64") => DataType::Float64,
        Some("string") => DataType::Utf8,
        Some("binary") => DataType::Binary,
        Some("date") => DataType::Date32,
        _ if logical.get("decimal").is_some() => DataType::Decimal128(
            logical["decimal"]["precision"].as_u64().unwrap() as u8,
            logical["decimal"]["scale"].as_u64().unwrap() as i8,
        ),
        _ => {
            let unit = match logical["timestamp"]["unit"].as_str().unwrap() {
                "ms" => TimeUnit::Millisecond,
                "us" => TimeUnit::Microsecond,
                "ns" => TimeUnit::Nanosecond,
                _ => unreachable!(),
            };
            let timezone = logical["timestamp"]["utc"]
                .as_bool()
                .unwrap()
                .then(|| Arc::from("UTC"));
            DataType::Timestamp(unit, timezone)
        }
    })
}

pub fn arrow_schema(contract: &TableContract) -> Result<Arc<Schema>> {
    contract.validate().map_err(|e| Error(e.to_string()))?;
    let fields: Result<Vec<Field>> = contract
        .columns
        .iter()
        .map(|column| {
            let mut metadata = HashMap::new();
            if let Some(ext) = contract.column_ext.get(&column.name) {
                metadata.insert(
                    "grv.column.ext".into(),
                    String::from_utf8(
                        grv_types::canonical_json(ext).map_err(|e| Error(e.to_string()))?,
                    )
                    .unwrap(),
                );
            }
            Ok(
                Field::new(&column.name, arrow_type(&column.logical_type)?, true)
                    .with_metadata(metadata),
            )
        })
        .collect();
    let mut metadata = HashMap::new();
    metadata.insert(
        "grv.table.extensions".into(),
        String::from_utf8(
            grv_types::canonical_json(&contract.extensions).map_err(|e| Error(e.to_string()))?,
        )
        .unwrap(),
    );
    Ok(Arc::new(Schema::new_with_metadata(fields?, metadata)))
}

/// Incidental IPC metadata and nullability are not logical schema identity.
pub fn validate_batch(contract: &TableContract, batch: &arrow_array::RecordBatch) -> Result<()> {
    let expected = arrow_schema(contract)?;
    if expected.fields().len() != batch.num_columns() {
        return Err(Error("batch column count differs from contract".into()));
    }
    let actual_schema = batch.schema();
    for (index, field) in expected.fields().iter().enumerate() {
        let actual = &actual_schema.fields()[index];
        if field.name() != actual.name() || field.data_type() != actual.data_type() {
            return Err(Error(format!(
                "batch column {} differs from contract",
                field.name()
            )));
        }
        // Extensions are semantic, unlike incidental Arrow metadata.
        if actual.metadata().get("grv.column.ext") != field.metadata().get("grv.column.ext") {
            return Err(Error(format!(
                "column {} extension differs from contract",
                field.name()
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn authoring_types_have_one_exact_mapping() {
        assert_eq!(
            authoring_type("decimal128(38,6)").unwrap(),
            json!({"decimal":{"precision":38,"scale":6}})
        );
        assert_eq!(
            arrow_type(&authoring_type("timestamp(us,UTC)").unwrap()).unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
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
