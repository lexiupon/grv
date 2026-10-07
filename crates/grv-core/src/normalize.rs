//! Core-owned partition derivation and checks over declared output names.
use crate::{
    contract, declaration,
    store::{Result, public_error},
};
use arrow_array::{
    Array, ArrayRef, Date32Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use chrono::{Datelike, NaiveDate};
use grv_storage::model::{Partition, TableLayout};
use grv_types::{ErrorCode, Name, TableContract};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeSet, sync::Arc};

fn invalid(message: impl Into<String>) -> grv_types::PublicError {
    public_error(ErrorCode::InvalidDeclaration, message)
}
fn failed(message: impl Into<String>) -> grv_types::PublicError {
    public_error(ErrorCode::EngineFailure, message)
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CalendarFormat {
    Year,
    Month,
    Day,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Derivation {
    pub column: String,
    pub from: String,
    pub format: CalendarFormat,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TablePlan {
    pub table: Name,
    pub contract: TableContract,
    pub derivations: Vec<Derivation>,
    pub not_null: Vec<String>,
}
impl TablePlan {
    pub fn from_declaration(declaration: &Value, table: &Value) -> Result<Self> {
        let name = Name::new(
            table["name"]
                .as_str()
                .ok_or_else(|| invalid("table name required"))?,
        )
        .map_err(|e| invalid(e.to_string()))?;
        let contract =
            declaration::table_contract(table, true).map_err(|e| invalid(e.to_string()))?;
        let mut derivations = vec![];
        for column in table["columns"]
            .as_array()
            .ok_or_else(|| invalid("columns required"))?
        {
            if let Some(derive) = column.get("derive") {
                derivations.push(Derivation {
                    column: column["name"]
                        .as_str()
                        .ok_or_else(|| invalid("derived column required"))?
                        .into(),
                    from: derive["from"]
                        .as_str()
                        .ok_or_else(|| invalid("derived input required"))?
                        .into(),
                    format: match derive["format"].as_str() {
                        Some("year") => CalendarFormat::Year,
                        Some("month") => CalendarFormat::Month,
                        Some("day") => CalendarFormat::Day,
                        _ => return Err(invalid("invalid calendar format")),
                    },
                });
            }
        }
        let mut not_null = BTreeSet::new();
        for check in declaration
            .get("checks")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if check["table"] == name.as_str() {
                for field in check["not_null"]
                    .as_array()
                    .ok_or_else(|| invalid("not_null array required"))?
                {
                    not_null.insert(
                        field
                            .as_str()
                            .ok_or_else(|| invalid("check output name required"))?
                            .to_owned(),
                    );
                }
            }
        }
        let plan = Self {
            table: name,
            contract,
            derivations,
            not_null: not_null.into_iter().collect(),
        };
        plan.validate()?;
        Ok(plan)
    }
    pub fn validate(&self) -> Result<()> {
        self.contract
            .validate()
            .map_err(|e| invalid(e.to_string()))?;
        let schema = contract::arrow_schema(&self.contract).map_err(|e| invalid(e.to_string()))?;
        let mut derived = BTreeSet::new();
        for derivation in &self.derivations {
            if !derived.insert(&derivation.column) {
                return Err(invalid("duplicate partition derivation"));
            }
            let output = schema
                .index_of(&derivation.column)
                .map_err(|_| invalid("derived output missing"))?;
            let input = schema
                .index_of(&derivation.from)
                .map_err(|_| invalid("derived input missing"))?;
            if schema.field(output).data_type() != &DataType::Utf8
                || !self
                    .contract
                    .partition_keys
                    .iter()
                    .any(|key| derivation.column == format!("_{key}_"))
                || !matches!(
                    schema.field(input).data_type(),
                    DataType::Date32 | DataType::Timestamp(..)
                )
            {
                return Err(invalid(
                    "derive requires a date/timestamp input and string partition output",
                ));
            }
        }
        for field in &self.not_null {
            schema
                .index_of(field)
                .map_err(|_| invalid("check output missing"))?;
        }
        Ok(())
    }
    pub fn input_contract(&self) -> Result<TableContract> {
        self.validate()?;
        let derived: BTreeSet<_> = self.derivations.iter().map(|d| d.column.as_str()).collect();
        let mut input = self.contract.clone();
        input
            .columns
            .retain(|column| !derived.contains(column.name.as_str()));
        input
            .partition_keys
            .retain(|key| !derived.contains(format!("_{key}_").as_str()));
        input
            .column_ext
            .as_object_mut()
            .unwrap()
            .retain(|column, _| !derived.contains(column.as_str()));
        input.validate().map_err(|e| invalid(e.to_string()))?;
        Ok(input)
    }
    pub fn layout(&self) -> TableLayout {
        TableLayout {
            table: self.table.clone(),
            partition_keys: self.contract.partition_keys.clone(),
            extensions: self
                .contract
                .extensions
                .as_object()
                .filter(|e| !e.is_empty())
                .map(|e| e.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
        }
    }
    /// Chunking before this call keeps derived strings inside the core scratch
    /// budget. The mapping itself never casts adapter-provided values.
    pub fn normalize(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let input = self.input_contract()?;
        contract::validate_batch(&input, batch)
            .map_err(|e| public_error(ErrorCode::ProtocolFailure, e.to_string()))?;
        let input_schema = batch.schema();
        let output_schema =
            contract::arrow_schema(&self.contract).map_err(|e| invalid(e.to_string()))?;
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.contract.columns.len());
        for column in &self.contract.columns {
            if let Some(derive) = self.derivations.iter().find(|d| d.column == column.name) {
                let array = batch.column(
                    input_schema
                        .index_of(&derive.from)
                        .map_err(|_| invalid("derived input absent"))?,
                );
                let mut values = Vec::with_capacity(batch.num_rows());
                for row in 0..batch.num_rows() {
                    if array.is_null(row) {
                        return Err(failed(format!("null calendar input {}", derive.from)));
                    }
                    let date = calendar_date(array, row)?;
                    if !(1..=9999).contains(&date.year()) {
                        return Err(failed("calendar input is outside years 0001–9999"));
                    }
                    values.push(match derive.format {
                        CalendarFormat::Year => format!("{:04}", date.year()),
                        CalendarFormat::Month => format!("{:04}-{:02}", date.year(), date.month()),
                        CalendarFormat::Day => {
                            format!("{:04}-{:02}-{:02}", date.year(), date.month(), date.day())
                        }
                    });
                }
                columns.push(Arc::new(StringArray::from(values)));
            } else {
                columns.push(
                    batch
                        .column(
                            input_schema
                                .index_of(&column.name)
                                .map_err(|_| invalid("output absent"))?,
                        )
                        .clone(),
                );
            }
        }
        let normalized =
            RecordBatch::try_new(output_schema, columns).map_err(|e| failed(e.to_string()))?;
        self.check(&normalized)?;
        Ok(normalized)
    }
    pub fn check(&self, batch: &RecordBatch) -> Result<()> {
        contract::validate_batch(&self.contract, batch).map_err(|e| failed(e.to_string()))?;
        for name in &self.not_null {
            let index = batch
                .schema()
                .index_of(name)
                .map_err(|_| invalid("check output missing"))?;
            if batch.column(index).null_count() != 0 {
                return Err(failed(format!("not_null failed for {}.{name}", self.table)));
            }
        }
        for row in 0..batch.num_rows() {
            self.partition(batch, row)?;
        }
        Ok(())
    }
    pub fn partition(&self, batch: &RecordBatch, row: usize) -> Result<Partition> {
        if row >= batch.num_rows() {
            return Err(failed("partition row outside batch"));
        }
        let schema = batch.schema();
        let mut partition = Partition::new();
        for key in &self.contract.partition_keys {
            let column = format!("_{key}_");
            let index = schema
                .index_of(&column)
                .map_err(|_| failed("partition output missing"))?;
            let array = batch
                .column(index)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| failed("partition output must be utf8"))?;
            if array.is_null(row) {
                return Err(failed(format!("null partition output {column}")));
            }
            partition.insert(
                key.clone(),
                Name::new(array.value(row)).map_err(|e| failed(e.to_string()))?,
            );
        }
        self.layout()
            .partition_path(&partition)
            .map_err(|e| failed(e.to_string()))?;
        Ok(partition)
    }
}
fn calendar_date(array: &ArrayRef, row: usize) -> Result<NaiveDate> {
    let days = match array.data_type() {
        DataType::Date32 => array
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap()
            .value(row) as i64,
        DataType::Timestamp(unit, _) => {
            let (value, per_day) = match unit {
                TimeUnit::Millisecond => (
                    array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap()
                        .value(row),
                    86_400_000,
                ),
                TimeUnit::Microsecond => (
                    array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap()
                        .value(row),
                    86_400_000_000,
                ),
                TimeUnit::Nanosecond => (
                    array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap()
                        .value(row),
                    86_400_000_000_000,
                ),
                _ => return Err(failed("unsupported timestamp calendar unit")),
            };
            value.div_euclid(per_day)
        }
        _ => return Err(failed("calendar input must be date32 or timestamp")),
    };
    NaiveDate::from_ymd_opt(1970, 1, 1)
        .unwrap()
        .checked_add_signed(
            chrono::Duration::try_days(days)
                .ok_or_else(|| failed("calendar input outside supported range"))?,
        )
        .ok_or_else(|| failed("calendar input outside supported range"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn plan(ty: &str) -> TablePlan {
        let table = json!({"name":"events","partition_keys":["day"],"columns":[
            {"name":"renamed","type":ty},{"name":"_day_","type":"utf8","derive":{"from":"renamed","format":"day"}}]});
        TablePlan::from_declaration(
            &json!({"checks":[{"table":"events","not_null":["renamed","_day_"]}]}),
            &table,
        )
        .unwrap()
    }
    #[test]
    fn dates_and_wall_clock_timestamps_derive_in_output_order_including_before_epoch() {
        for ty in [
            "date32",
            "timestamp(ms)",
            "timestamp(us,UTC)",
            "timestamp(ns)",
        ] {
            let plan = plan(ty);
            let input = plan.input_contract().unwrap();
            let array: ArrayRef = match ty {
                "date32" => Arc::new(Date32Array::from(vec![-1, 0])),
                "timestamp(ms)" => Arc::new(TimestampMillisecondArray::from(vec![-1, 0])),
                "timestamp(us,UTC)" => {
                    Arc::new(TimestampMicrosecondArray::from(vec![-1, 0]).with_timezone("UTC"))
                }
                _ => Arc::new(TimestampNanosecondArray::from(vec![-1, 0])),
            };
            let batch =
                RecordBatch::try_new(contract::arrow_schema(&input).unwrap(), vec![array]).unwrap();
            let result = plan.normalize(&batch).unwrap();
            let values = result
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(values.value(0), "1969-12-31");
            assert_eq!(values.value(1), "1970-01-01");
            assert_eq!(
                plan.partition(&result, 0).unwrap()[&Name::new("day").unwrap()].as_str(),
                "1969-12-31"
            );
        }
    }
    #[test]
    fn nulls_out_of_range_dates_and_noncanonical_partition_values_fail() {
        let plan = plan("date32");
        let schema = contract::arrow_schema(&plan.input_contract().unwrap()).unwrap();
        for values in [vec![None], vec![Some(i32::MAX)], vec![Some(-719_528)]] {
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Date32Array::from(values))])
                    .unwrap();
            assert_eq!(
                plan.normalize(&batch).unwrap_err().code,
                ErrorCode::EngineFailure
            );
        }
        let table = json!({"name":"events","partition_keys":["day"],"columns":[{"name":"_day_","type":"utf8"}]});
        let plan = TablePlan::from_declaration(&json!({}), &table).unwrap();
        for value in ["a/b", "UPPER", "", "leading space"] {
            let batch = RecordBatch::try_new(
                contract::arrow_schema(&plan.input_contract().unwrap()).unwrap(),
                vec![Arc::new(StringArray::from(vec![value]))],
            )
            .unwrap();
            assert_eq!(
                plan.normalize(&batch).unwrap_err().code,
                ErrorCode::EngineFailure
            );
        }
    }
    #[test]
    fn declared_checks_ignore_arrow_nullability_and_use_renamed_outputs() {
        let table =
            json!({"name":"events","columns":[{"name":"renamed","type":"int64","source":"raw"}]});
        let plan = TablePlan::from_declaration(
            &json!({"checks":[{"table":"events","not_null":["renamed"]}]}),
            &table,
        )
        .unwrap();
        let schema = contract::arrow_schema(&plan.contract).unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::Int64Array::from(vec![Some(1), None]))],
        )
        .unwrap();
        assert_eq!(
            plan.normalize(&batch).unwrap_err().code,
            ErrorCode::EngineFailure
        );
        assert!(plan.normalize(&RecordBatch::new_empty(schema)).is_ok());
    }
}
