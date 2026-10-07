//! Adapter-local pull plans. Preparation consumes verified file metadata; no
//! GRV backend or core implementation is imported, and preparation opens no file.
use grv_types::{AdapterIdentity, Digest, Name, RequestedRevision, TableContract, U64, Uuid};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeSet, io, path::PathBuf};

/// DuckDB destinations use their registered grammar, independently of GRV names.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct RelationName(String);
impl RelationName {
    pub fn new(value: impl Into<String>) -> io::Result<Self> {
        let value = value.into();
        if value.is_empty()
            || !value.bytes().enumerate().all(|(index, byte)| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || byte == b'_'
                    || (index > 0 && byte == b'-')
            })
        {
            return Err(invalid("invalid DuckDB relation name"));
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for RelationName {
    type Error = io::Error;
    fn try_from(value: String) -> io::Result<Self> {
        Self::new(value)
    }
}
impl From<RelationName> for String {
    fn from(value: RelationName) -> Self {
        value.0
    }
}
pub(crate) fn reserved_namespace(value: &str) -> bool {
    ["_grv", "grv_source", "grv_target", "grv_input", "grv_self"].contains(&value)
        || value.starts_with("_grv_session_")
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceBinding {
    pub canonical_root: String,
    pub workspace_id: Uuid,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    Replace,
    Append,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopeMode {
    Complete,
    Selected,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransformMode {
    Identity,
    Sql,
}
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MaterializationMode {
    #[default]
    Local,
    S3View,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PullIdentity {
    pub binding: WorkspaceBinding,
    pub attempt_id: Uuid,
    pub request_sha256: Digest,
    pub adapter_identity: AdapterIdentity,
    pub dataset: Name,
    pub target_schema: RelationName,
    pub write_mode: WriteMode,
    pub scope_mode: ScopeMode,
    pub transform_mode: TransformMode,
    pub requested_revision: RequestedRevision,
}
impl PullIdentity {
    pub fn is_identity_materialization(&self) -> bool {
        self.write_mode == WriteMode::Replace
            && self.scope_mode == ScopeMode::Complete
            && self.transform_mode == TransformMode::Identity
    }
    pub fn validate(&self) -> io::Result<()> {
        self.adapter_identity.validate().map_err(io::Error::other)?;
        if self.binding.canonical_root.is_empty()
            || reserved_namespace(self.target_schema.as_str())
            || self.adapter_identity.name.as_str() != "duckdb"
        {
            return Err(invalid(
                "invalid pull root, adapter or reserved destination namespace",
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceFile {
    /// Parent-owned immutable private local file, already verified by the core.
    /// Empty for S3 sources, whose location is in `remote`.
    pub path: PathBuf,
    pub bytes: U64,
    pub sha256: Digest,
    pub contract: TableContract,
    /// Complete exact partition tuple for this file; this is never Hive inference.
    pub partition: Value,
    /// Exact parent-verified S3 facts. A remote source has no local path; its
    /// duplicated schema/size/hash/partition facts must match this closed record.
    #[serde(default)]
    pub remote: Option<grv_adapter_api::VerifiedFile>,
}
impl SourceFile {
    fn location(&self) -> io::Result<String> {
        if let Some(remote) = &self.remote {
            crate::s3_config::sql_filename(&remote.location)
        } else {
            self.path
                .to_str()
                .map(str::to_owned)
                .ok_or_else(|| invalid("non-UTF-8 source file"))
        }
    }
    fn validate_access(&self, table: &Name, mode: MaterializationMode) -> io::Result<()> {
        match (&self.remote, mode) {
            (None, MaterializationMode::Local)
                if self.path.is_absolute()
                    && self.path.to_str().is_some()
                    && !self.path.as_os_str().as_encoded_bytes().contains(&0) =>
            {
                Ok(())
            }
            (Some(remote), MaterializationMode::S3View) => {
                remote.validate().map_err(io::Error::other)?;
                if remote.access != grv_adapter_api::FileAccess::S3View
                    || !self.path.as_os_str().is_empty()
                    || remote.table != *table
                    || remote.size != self.bytes
                    || remote.sha256 != self.sha256
                    || remote.schema.columns != self.contract.columns
                    || remote.partition != self.partition
                {
                    return Err(invalid(
                        "S3 source differs from its exact verified file evidence",
                    ));
                }
                Ok(())
            }
            _ => Err(invalid(
                "source file access differs from materialization mode",
            )),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PullTable {
    pub name: Name,
    pub target_table: RelationName,
    pub source_contract: TableContract,
    pub output_contract: TableContract,
    pub files: Vec<SourceFile>,
    /// Expanded UTF-8 SQL. Native preparation must parse and guard this query.
    pub sql: Option<String>,
    pub not_null: Vec<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PullPlan {
    pub identity: PullIdentity,
    pub committed_revision: U64,
    pub generation_id: Uuid,
    pub request_record: Option<grv_adapter_api::RequestRecord>,
    pub selected_partitions: Option<Value>,
    pub tables: Vec<PullTable>,
    #[serde(default)]
    pub materialization_mode: MaterializationMode,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PullReceipt {
    pub identity: PullIdentity,
    pub committed_revision: U64,
    pub generation_id: Uuid,
    pub table_counts: Value,
    pub table_targets: Value,
    pub source_partitions: Value,
    pub resolved_source_contracts: Value,
    pub output_contracts: Value,
    pub pulled_at: grv_types::Timestamp,
    pub request_record: Option<grv_adapter_api::RequestRecord>,
    #[serde(default)]
    pub materialization_mode: MaterializationMode,
}
#[derive(Debug, Clone, PartialEq)]
pub enum PullResolution {
    /// Ordinary engine, no managed metadata. First successful transaction binds it.
    Uninitialized,
    /// Fenced owner and complete trustworthy receipt store prove no commit.
    NotCommitted,
    Committed(Box<PullReceipt>),
}
#[derive(Debug, Clone)]
pub struct PullPreview {
    pub prior_revision: Option<U64>,
    pub prior_generation_id: Option<Uuid>,
    pub obsolete_targets: Vec<String>,
}
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
pub fn quote_identifier(text: &str) -> String {
    format!("\"{}\"", text.replace('"', "\"\""))
}
pub fn quote_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}
pub fn native_type(value: &Value) -> io::Result<String> {
    grv_types::validate_logical_type(value).map_err(io::Error::other)?;
    Ok(match value.as_str() {
        Some("boolean") => "BOOLEAN".into(),
        Some("int64") => "BIGINT".into(),
        Some("float64") => "DOUBLE".into(),
        Some("string") => "VARCHAR".into(),
        Some("binary") => "BLOB".into(),
        Some("date") => "DATE".into(),
        _ if value.get("decimal").is_some() => format!(
            "DECIMAL({},{})",
            value["decimal"]["precision"], value["decimal"]["scale"]
        ),
        _ if value["timestamp"]["utc"] == true => "TIMESTAMPTZ".into(),
        _ => match value["timestamp"]["unit"].as_str().unwrap() {
            "ms" => "TIMESTAMP_MS",
            "us" => "TIMESTAMP",
            "ns" => "TIMESTAMP_NS",
            _ => unreachable!(),
        }
        .into(),
    })
}
pub fn table_definition(contract: &TableContract) -> io::Result<String> {
    contract.validate().map_err(io::Error::other)?;
    contract
        .columns
        .iter()
        .map(|column| {
            Ok(format!(
                "{} {}",
                quote_identifier(&column.name),
                native_type(&column.logical_type)?
            ))
        })
        .collect::<io::Result<Vec<_>>>()
        .map(|columns| columns.join(","))
}
/// An omitted or selected-empty view retains its exact ordered logical schema.
pub fn empty_projection(contract: &TableContract) -> io::Result<String> {
    contract.validate().map_err(io::Error::other)?;
    let columns = contract
        .columns
        .iter()
        .map(|column| {
            Ok(format!(
                "CAST(NULL AS {}) AS {}",
                native_type(&column.logical_type)?,
                quote_identifier(&column.name)
            ))
        })
        .collect::<io::Result<Vec<_>>>()?;
    Ok(format!("SELECT {} WHERE false", columns.join(",")))
}
pub fn s3_view_projection(table: &PullTable) -> io::Result<String> {
    if table.files.is_empty() {
        return empty_projection(&table.source_contract);
    }
    table
        .files
        .iter()
        .map(|file| {
            file.validate_access(&table.name, MaterializationMode::S3View)?;
            file_projection(file, &table.source_contract)
        })
        .collect::<io::Result<Vec<_>>>()
        .map(|queries| queries.join(" UNION ALL "))
}
#[cfg(feature = "native")]
pub(crate) fn exact_value_assertion(
    contract: &TableContract,
    relation: &str,
    normalized_ms: bool,
) -> Option<String> {
    let tests = contract
        .columns
        .iter()
        .flat_map(|column| {
            let name = quote_identifier(&column.name);
            let mut tests = Vec::new();
            if column.logical_type == "date" || column.logical_type.get("timestamp").is_some() {
                let native = native_type(&column.logical_type).expect("validated output contract");
                tests.push(format!(
                    "{name}=CAST('infinity' AS {native}) OR {name}=CAST('-infinity' AS {native})"
                ));
            }
            if column.logical_type["timestamp"]["unit"] == "ms"
                && (normalized_ms || column.logical_type["timestamp"]["utc"] == true)
            {
                tests.push(format!("epoch_us({name}) % 1000 <> 0"));
            }
            tests
        })
        .collect::<Vec<_>>();
    (!tests.is_empty()).then(||format!("SELECT CASE WHEN count(*)=0 THEN 0 ELSE error('pull output value is not exactly representable in its declared contract') END FROM {relation} WHERE {}",tests.join(" OR ")))
}
impl PullPlan {
    /// Pure preparation. In particular neither receipts nor source bytes are read.
    pub fn validate(&self) -> io::Result<()> {
        self.identity.validate()?;
        if self.materialization_mode == MaterializationMode::S3View
            && (!self.identity.is_identity_materialization()
                || !self.identity.binding.canonical_root.starts_with("s3://"))
        {
            return Err(invalid(
                "S3 views require complete identity replacement against an S3 root",
            ));
        }
        if let Some(request) = &self.request_record {
            request.validate().map_err(io::Error::other)?;
            let declared_mode =
                match request.effective_declaration["options"]["materialization"].as_str() {
                    None | Some("local") => MaterializationMode::Local,
                    Some("s3-view") => MaterializationMode::S3View,
                    _ => return Err(invalid("invalid fixed materialization mode")),
                };
            if request.attempt_id != self.identity.attempt_id
                || request.root != self.identity.binding.canonical_root
                || request.workspace_id != self.identity.binding.workspace_id
                || request.request_sha256 != self.identity.request_sha256
                || request.adapter_identity != self.identity.adapter_identity
                || request.dataset != self.identity.dataset
                || request.requested_revision != self.identity.requested_revision
                || declared_mode != self.materialization_mode
            {
                return Err(invalid(
                    "fixed wire request differs from native pull identity",
                ));
            }
        }
        if self.tables.is_empty() {
            return Err(invalid("empty pull selection"));
        }
        let mut names = BTreeSet::new();
        let mut targets = BTreeSet::new();
        let mut locations = BTreeSet::new();
        let mut version_schemas = std::collections::BTreeMap::new();
        let mut has_sql = false;
        for table in &self.tables {
            if !names.insert(&table.name) || !targets.insert(&table.target_table) {
                return Err(invalid("duplicate source or destination mapping"));
            }
            table.source_contract.validate().map_err(io::Error::other)?;
            table.output_contract.validate().map_err(io::Error::other)?;
            has_sql |= table.sql.is_some();
            if table
                .sql
                .as_ref()
                .is_some_and(|sql| sql.is_empty() || sql.contains('\0'))
            {
                return Err(invalid("empty or NUL SQL"));
            }
            if table.sql.is_none() && table.source_contract != table.output_contract {
                return Err(invalid(
                    "identity import must preserve its exact source contract",
                ));
            }
            if table.not_null.iter().any(|name| {
                !table
                    .output_contract
                    .columns
                    .iter()
                    .any(|column| &column.name == name)
            }) {
                return Err(invalid("not-null check must name an output column"));
            }
            for file in &table.files {
                file.validate_access(&table.name, self.materialization_mode)?;
                if !locations.insert(file.location()?) {
                    return Err(invalid("duplicate source file location"));
                }
                if let Some(remote) = &file.remote
                    && !remote
                        .location
                        .strip_prefix(&self.identity.binding.canonical_root)
                        .is_some_and(|suffix| suffix.starts_with('/'))
                {
                    return Err(invalid("S3 source is outside the fixed canonical root"));
                }
                if let Some(remote) = &file.remote {
                    let key = (
                        &remote.table,
                        remote.version,
                        grv_types::canonical_json(&remote.partition).map_err(io::Error::other)?,
                    );
                    if version_schemas
                        .insert(key, &remote.schema)
                        .is_some_and(|prior| prior != &remote.schema)
                    {
                        return Err(invalid(
                            "files in one source version have different exact schemas",
                        ));
                    }
                }
                file.contract.validate().map_err(io::Error::other)?;
                if file.contract.partition_keys.iter().any(|key| {
                    !file.contract.columns.iter().any(|column| {
                        column.name == format!("_{key}_") && column.logical_type == "string"
                    })
                }) {
                    return Err(invalid(
                        "partition column must be present with exact string type",
                    ));
                }
                if file.contract.partition_keys != table.source_contract.partition_keys
                    || file.contract.extensions != table.source_contract.extensions
                    || file.contract.columns.len() > table.source_contract.columns.len()
                    || file.contract.columns
                        != table.source_contract.columns[..file.contract.columns.len()]
                    || file.contract.columns.iter().any(|column| {
                        file.contract.column_ext.get(&column.name)
                            != table.source_contract.column_ext.get(&column.name)
                    })
                {
                    return Err(invalid(
                        "source file schema is not an exact selected prefix",
                    ));
                }
                let tuple = file
                    .partition
                    .as_object()
                    .ok_or_else(|| invalid("partition tuple must be an object"))?;
                if tuple.len() != file.contract.partition_keys.len()
                    || file.contract.partition_keys.iter().any(|key| {
                        tuple
                            .get(key.as_str())
                            .and_then(Value::as_str)
                            .is_none_or(|value| Name::new(value).is_err())
                    })
                {
                    return Err(invalid("file partition tuple differs from selected layout"));
                }
            }
        }
        if has_sql != (self.identity.transform_mode == TransformMode::Sql) {
            return Err(invalid(
                "whole invocation SQL classification differs from mappings",
            ));
        }
        Ok(())
    }
}
/// Explicit file projection for one verified file. A file's shorter historical
/// schema is null padded; its path never supplies inferred Hive columns.
pub fn file_projection(file: &SourceFile, contract: &TableContract) -> io::Result<String> {
    let expressions = contract
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let value = if index < file.contract.columns.len() {
                quote_identifier(&column.name)
            } else {
                "NULL".into()
            };
            Ok(format!(
                "CAST({value} AS {}) AS {}",
                native_type(&column.logical_type)?,
                quote_identifier(&column.name)
            ))
        })
        .collect::<io::Result<Vec<_>>>()?;
    Ok(format!(
        "SELECT {} FROM read_parquet({},hive_partitioning=false,union_by_name=false)",
        expressions.join(","),
        quote_literal(&file.location()?)
    ))
}
/// Check physical storage-v2 partition columns before adopting source rows.
pub fn partition_assertion(file: &SourceFile) -> io::Result<Option<String>> {
    let tests = file
        .contract
        .partition_keys
        .iter()
        .map(|key| {
            let value = file
                .partition
                .get(key.as_str())
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("missing partition value"))?;
            Name::new(value).map_err(io::Error::other)?;
            let column = quote_identifier(&format!("_{key}_"));
            Ok(format!(
                "{column} IS NULL OR {column} <> {}",
                quote_literal(value)
            ))
        })
        .collect::<io::Result<Vec<_>>>()?;
    if tests.is_empty() {
        return Ok(None);
    }
    Ok(Some(format!(
        "SELECT count(*)::BIGINT FROM read_parquet({},hive_partitioning=false,union_by_name=false) WHERE {}",
        quote_literal(&file.location()?),
        tests.join(" OR ")
    )))
}
impl PullReceipt {
    pub fn matches(&self, identity: &PullIdentity) -> bool {
        self.identity == *identity
    }
    pub fn wire_receipt(&self) -> io::Result<grv_adapter_api::Receipt> {
        use grv_adapter_api::{NamedContract, Receipt, TableCount};
        let request = self
            .request_record
            .clone()
            .ok_or_else(|| io::Error::other("receipt lacks fixed wire request evidence"))?;
        let row_counts = self
            .table_counts
            .as_object()
            .ok_or_else(|| io::Error::other("invalid receipt counts"))?
            .iter()
            .map(|(table, rows)| {
                Ok(TableCount {
                    table: Name::new(table).map_err(io::Error::other)?,
                    rows: serde_json::from_value(rows.clone()).map_err(io::Error::other)?,
                })
            })
            .collect::<io::Result<Vec<_>>>()?;
        let contracts = |value: &Value| -> io::Result<Vec<NamedContract>> {
            value
                .as_array()
                .ok_or_else(|| io::Error::other("invalid receipt contracts"))?
                .iter()
                .map(|pair| {
                    Ok(NamedContract {
                        table: serde_json::from_value(pair[0].clone()).map_err(io::Error::other)?,
                        contract: serde_json::from_value(pair[1].clone())
                            .map_err(io::Error::other)?,
                    })
                })
                .collect()
        };
        let receipt = Receipt {
            request,
            committed_revision: self.committed_revision,
            generation_id: self.generation_id.clone(),
            pulled_at: self.pulled_at.clone(),
            row_counts,
            source_contracts: contracts(&self.resolved_source_contracts)?,
            output_contracts: contracts(&self.output_contracts)?,
            adapter_result: serde_json::json!({"target_schema":self.identity.target_schema,"write_mode":self.identity.write_mode,"transform_mode":self.identity.transform_mode,"scope_mode":self.identity.scope_mode,"materialization_mode":self.materialization_mode,"source_partitions":self.source_partitions}),
        };
        receipt.validate().map_err(io::Error::other)?;
        Ok(receipt)
    }
}

#[cfg(feature = "native")]
mod destination {
    use super::*;
    use crate::native::NativeEngine;
    use crate::worker::Engine;
    use std::path::Path;
    #[derive(Debug)]
    pub enum PullError {
        Engine(io::Error),
        OutcomeUnknown(String),
        RequestMismatch,
        StateConflict(String),
    }
    impl From<io::Error> for PullError {
        fn from(error: io::Error) -> Self {
            Self::Engine(error)
        }
    }
    pub struct PullStore {
        engine: NativeEngine,
        initialized_observed: bool,
        source_scratch_bytes: usize,
        offered_source_bytes: u64,
        offered_scratch_bytes: u64,
        #[cfg(test)]
        commit_crash: Option<bool>,
        stopping: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    }
    #[cfg(test)]
    pub(super) fn erase_receipt_store_for_test(path: &Path) {
        NativeEngine::open(path)
            .unwrap()
            .metadata_query("DROP TABLE _grv.pull_attempts")
            .unwrap();
    }
    #[cfg(test)]
    pub(super) fn crash_at_commit_for_test(store: &mut PullStore, after: bool) {
        store.commit_crash = Some(after);
    }
    #[cfg(test)]
    pub(super) fn erase_all_metadata_for_test(path: &Path) {
        NativeEngine::open(path)
            .unwrap()
            .metadata_query("DROP SCHEMA _grv CASCADE")
            .unwrap();
    }
    #[cfg(test)]
    pub(super) fn materializations_for_test(store: &mut PullStore) -> Vec<Vec<Option<String>>> {
        store.engine.metadata_query("SELECT table_name,target_table,grv_revision,generation_id FROM _grv.pull_meta ORDER BY table_name").unwrap()
    }
    #[cfg(test)]
    pub(super) fn app_attempts_for_test(store: &mut PullStore) -> usize {
        store
            .engine
            .metadata_query("SELECT count(*)::BIGINT FROM _grv.import_attempt_details")
            .unwrap()[0][0]
            .as_ref()
            .unwrap()
            .parse()
            .unwrap()
    }
    #[cfg(test)]
    pub(super) fn scratch_bytes_for_test(store: &PullStore) -> usize {
        store.source_scratch_bytes
    }
    #[cfg(test)]
    pub(super) fn assert_no_destination_for_test(path: &Path) {
        let mut engine = NativeEngine::open(path).unwrap();
        assert!(!engine.relation_exists("app", "rows").unwrap());
        assert!(!engine.relation_exists("app", "second").unwrap());
        assert!(!engine.managed_initialized().unwrap());
    }
    #[cfg(test)]
    pub(super) fn inspect_target_for_test(store: &mut PullStore, name: &str) -> Vec<String> {
        store.engine.authorize_relation("app", name).unwrap();
        store
            .engine
            .metadata_query(&format!(
                "SELECT id FROM app.{} ORDER BY id",
                quote_identifier(name)
            ))
            .unwrap()
            .into_iter()
            .map(|row| row[0].clone().unwrap())
            .collect()
    }
    fn canonical<T: Serialize>(value: &T) -> io::Result<String> {
        String::from_utf8(grv_types::canonical_json(value).map_err(io::Error::other)?)
            .map_err(io::Error::other)
    }
    #[cfg(test)]
    pub(super) fn local_views_for_test(store: &mut PullStore) {
        store.engine.authorize_relation("app", "rows").unwrap();
        for sql in [
            "CREATE MACRO plus_ten(value) AS value+10",
            "CREATE VIEW app.local_view AS SELECT * FROM app.rows",
            "CREATE VIEW app.nested_view AS SELECT * FROM app.local_view",
        ] {
            if sql.contains("nested_view") {
                store
                    .engine
                    .authorize_relation("app", "local_view")
                    .unwrap();
            }
            store.engine.metadata_query(sql).unwrap();
        }
    }
    #[cfg(test)]
    pub(super) fn erase_mutable_for_test(store: &mut PullStore) {
        for table in ["pull_meta", "pull_checkpoint"] {
            store
                .engine
                .metadata_query(&format!("DROP TABLE _grv.{table}"))
                .unwrap();
        }
    }
    #[cfg(test)]
    pub(super) fn corrupt_prior_contract_for_test(store: &mut PullStore, receipt: &PullReceipt) {
        let mut receipt = receipt.clone();
        receipt.resolved_source_contracts[0][1]["columns"][0]["type"] = serde_json::json!("string");
        store
            .engine
            .metadata_query(&format!(
                "UPDATE _grv.pull_checkpoint SET receipt={}",
                quote_literal(&serde_json::to_string(&receipt).unwrap())
            ))
            .unwrap();
    }
    #[cfg(test)]
    pub(super) fn tracking_contract_for_test(
        store: &mut PullStore,
        target: &str,
        contract: &TableContract,
    ) {
        store
            .engine
            .metadata_query(&format!(
                "UPDATE _grv.pull_meta SET contract={} WHERE target_table={}",
                quote_literal(&canonical(contract).unwrap()),
                quote_literal(target)
            ))
            .unwrap();
    }
    #[cfg(test)]
    pub(super) fn typed_values_for_test(store: &mut PullStore) {
        store.engine.authorize_relation("app", "rows").unwrap();
        let values=store.engine.metadata_query("SELECT b=true, id=-7, f=1.25, s='🍕', hex(blob)='00FF', date=DATE '1970-01-02', decimal=1234567890123456789012345678.12::DECIMAL(30,2), ms=TIMESTAMP_MS '1970-01-01 00:00:00.001', us=TIMESTAMP '1970-01-01 00:00:00.000001', ns=TIMESTAMP_NS '1970-01-01 00:00:00.000000001', utc_ms=TIMESTAMPTZ '1970-01-01 00:00:00.001+00', utc_us=TIMESTAMPTZ '1970-01-01 00:00:00.000001+00' FROM app.rows").unwrap();
        assert_eq!(values, vec![vec![Some("true".into()); 12]]);
    }
    fn physical_schema(
        described: &[Vec<Option<String>>],
        contract: &TableContract,
    ) -> Result<(), PullError> {
        let mut contract = contract.clone();
        for column in &mut contract.columns {
            if column.logical_type["timestamp"]["unit"] == "ms"
                && column.logical_type["timestamp"]["utc"] == false
            {
                column.logical_type["timestamp"]["unit"] = Value::String("us".into());
            }
        }
        exact_schema(described, &contract).map_err(|_| {
            PullError::StateConflict(
                "physical source file schema differs from verified logical metadata".into(),
            )
        })
    }
    fn exact_schema(
        described: &[Vec<Option<String>>],
        contract: &TableContract,
    ) -> Result<(), PullError> {
        if described.len() != contract.columns.len() {
            return Err(PullError::StateConflict(
                "destination differs from exact ordered output contract".into(),
            ));
        }
        for (row, column) in described.iter().zip(&contract.columns) {
            let mut datatype = native_type(&column.logical_type)?;
            if datatype == "TIMESTAMPTZ" {
                datatype = "TIMESTAMP WITH TIME ZONE".into();
            }
            if row.len() < 2
                || row[0].as_deref() != Some(&column.name)
                || row[1].as_deref() != Some(&datatype)
            {
                return Err(PullError::StateConflict(
                    "destination differs from exact ordered output contract".into(),
                ));
            }
        }
        Ok(())
    }
    impl PullStore {
        fn admit_s3_resources(&self) -> Result<(), PullError> {
            if self.offered_source_bytes < 2 * 1024 * 1024
                || self.offered_scratch_bytes < 16 * 1024 * 1024
            {
                return Err(invalid(
                    "S3 reader requires at least 2 MiB source and 16 MiB scratch resources",
                )
                .into());
            }
            Ok(())
        }
        pub(super) fn target_is_view(
            &mut self,
            schema: &str,
            table: &str,
        ) -> Result<bool, PullError> {
            Ok(self.engine.is_view(schema, table)?)
        }
        fn drop_target(&mut self, schema: &str, table: &str) -> Result<(), PullError> {
            if self.engine.relation_exists(schema, table)? {
                let kind = if self.target_is_view(schema, table)? {
                    "VIEW"
                } else {
                    "TABLE"
                };
                self.engine.metadata_query(&format!(
                    "DROP {kind} {}.{}",
                    quote_identifier(schema),
                    quote_identifier(table)
                ))?;
            }
            Ok(())
        }
        /// The worker calls this only after source-free immutable receipt lookup.
        pub fn configure_s3_reader(
            &mut self,
            reader: &crate::s3_config::S3Reader,
        ) -> Result<(), PullError> {
            self.admit_s3_resources()?;
            self.engine.configure_s3_reader(reader)?;
            Ok(())
        }
        pub fn open(path: &Path) -> io::Result<Self> {
            let engine = NativeEngine::open(path)?;
            let initialized_observed = engine.managed_evidence_recorded()?;
            Ok(Self {
                engine,
                initialized_observed,
                source_scratch_bytes: 65536,
                offered_source_bytes: 32 * 1024 * 1024,
                offered_scratch_bytes: 32 * 1024 * 1024,
                #[cfg(test)]
                commit_crash: None,
                stopping: None,
            })
        }
        pub(crate) fn initialize_metadata(
            engine: &mut NativeEngine,
            binding: &WorkspaceBinding,
        ) -> io::Result<()> {
            for command in [
                "CREATE SCHEMA _grv",
                "CREATE TABLE _grv.metadata_header(version BIGINT NOT NULL,ownership_version BIGINT NOT NULL)",
                "INSERT INTO _grv.metadata_header VALUES(1,1)",
                "CREATE TABLE _grv.workspace_binding(identity VARCHAR NOT NULL)",
                "CREATE TABLE _grv.pull_attempts(attempt_id VARCHAR PRIMARY KEY,request_sha256 VARCHAR NOT NULL,adapter_identity VARCHAR NOT NULL,receipt VARCHAR NOT NULL)",
                "CREATE TABLE _grv.relation_ownership(schema_name VARCHAR NOT NULL,table_name VARCHAR NOT NULL,kind VARCHAR NOT NULL,owner_scope VARCHAR NOT NULL,PRIMARY KEY(schema_name,table_name))",
                "CREATE TABLE _grv.pull_checkpoint(owner_scope VARCHAR PRIMARY KEY,receipt VARCHAR NOT NULL)",
                "CREATE TABLE _grv.pull_meta(owner_scope VARCHAR NOT NULL,table_name VARCHAR NOT NULL,target_table VARCHAR NOT NULL,grv_revision VARCHAR NOT NULL,generation_id VARCHAR NOT NULL,contract VARCHAR NOT NULL,PRIMARY KEY(owner_scope,table_name))",
                "CREATE TABLE _grv.replacement_scopes(owner_scope VARCHAR PRIMARY KEY,role VARCHAR NOT NULL)",
                "CREATE TABLE _grv.import_bindings(binding_id VARCHAR PRIMARY KEY,target_schema VARCHAR NOT NULL,target_table VARCHAR NOT NULL,contract VARCHAR NOT NULL,evidence VARCHAR NOT NULL)",
                "CREATE TABLE _grv.import_attempt_details(attempt_id VARCHAR PRIMARY KEY,owner_scope VARCHAR NOT NULL,receipt VARCHAR NOT NULL,is_current BOOLEAN NOT NULL)",
            ] {
                engine.metadata_query(command)?;
            }
            engine.metadata_query(&format!(
                "INSERT INTO _grv.workspace_binding VALUES({})",
                quote_literal(&canonical(binding)?)
            ))?;
            Ok(())
        }
        pub(crate) fn into_engine(self) -> NativeEngine {
            self.engine
        }
        pub(crate) fn interrupt_handle(&self) -> crate::native::NativeInterrupt {
            self.engine.interrupt_handle()
        }
        pub(crate) fn set_stopping(
            &mut self,
            stopping: std::sync::Arc<std::sync::atomic::AtomicBool>,
        ) {
            self.stopping = Some(stopping);
        }
        pub(crate) fn configure_resources(
            &mut self,
            resources: &grv_adapter_api::Resources,
        ) -> io::Result<()> {
            resources.validate().map_err(io::Error::other)?;
            self.offered_source_bytes = resources.max_source_unit_bytes.get();
            self.offered_scratch_bytes = resources.max_scratch_bytes.get();
            self.source_scratch_bytes = resources
                .max_source_unit_bytes
                .get()
                .min(
                    resources
                        .max_scratch_bytes
                        .get()
                        .saturating_sub(std::mem::size_of::<sha2::Sha256>() as u64),
                )
                .min(65536) as usize;
            Ok(())
        }
        fn check_stopping(&self) -> io::Result<()> {
            if self
                .stopping
                .as_ref()
                .is_some_and(|stop| stop.load(std::sync::atomic::Ordering::Acquire))
            {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "pull work stopped",
                ));
            }
            Ok(())
        }
        /// Recorded managed-workspace identity forbids treating lost metadata
        /// as a new uninitialized engine. Use this for bound retry invocations.
        pub fn open_bound(path: &Path) -> io::Result<Self> {
            let mut store = Self::open(path)?;
            store.initialized_observed = true;
            Ok(store)
        }
        /// Complete local or S3-view identity replacement; the general path also
        /// supports transactional SQL, selected partitions and append.
        pub fn apply_identity(&mut self, plan: &PullPlan) -> Result<Box<PullReceipt>, PullError> {
            plan.validate()?;
            if !plan.identity.is_identity_materialization() {
                return Err(PullError::StateConflict(
                    "identity apply requires complete identity replacement".into(),
                ));
            }
            self.apply(plan)
        }
        /// Transactional SQL, partition-selected replacement and ordinary-table
        /// append share the same staging, ownership and immutable receipt path.
        pub fn apply(&mut self, plan: &PullPlan) -> Result<Box<PullReceipt>, PullError> {
            use std::os::fd::AsRawFd;
            plan.validate()?;
            let initialize = match self.resolve(&plan.identity)? {
                PullResolution::Committed(receipt) => return Ok(receipt),
                PullResolution::Uninitialized => true,
                PullResolution::NotCommitted => false,
            };
            if plan.materialization_mode == MaterializationMode::S3View
                && plan.tables.iter().any(|table| !table.files.is_empty())
            {
                self.admit_s3_resources()?;
            }
            if self.source_scratch_bytes == 0
                && plan.tables.iter().any(|table| !table.files.is_empty())
            {
                return Err(invalid(
                    "negotiated scratch cannot admit hash state and one source byte",
                )
                .into());
            }
            self.engine.metadata_query("BEGIN TRANSACTION")?;
            let mut committing = false;
            let result = (|| {
                if initialize {
                    Self::initialize_metadata(&mut self.engine, &plan.identity.binding)?;
                }
                if !initialize && plan.identity.is_identity_materialization() {
                    // Checkpoints and per-table records are rebuildable derived
                    // state. Receipt and ownership authority was checked first.
                    self.engine.metadata_query("CREATE TABLE IF NOT EXISTS _grv.pull_checkpoint(owner_scope VARCHAR PRIMARY KEY,receipt VARCHAR NOT NULL)")?;
                    self.engine.metadata_query("CREATE TABLE IF NOT EXISTS _grv.pull_meta(owner_scope VARCHAR NOT NULL,table_name VARCHAR NOT NULL,target_table VARCHAR NOT NULL,grv_revision VARCHAR NOT NULL,generation_id VARCHAR NOT NULL,contract VARCHAR NOT NULL,PRIMARY KEY(owner_scope,table_name))")?;
                }
                let scope=grv_types::sha256(&grv_types::canonical_json(&serde_json::json!({"binding":plan.identity.binding,"dataset":plan.identity.dataset,"adapter":"duckdb","target_schema":plan.identity.target_schema})).map_err(io::Error::other)?);
                let namespace = plan.identity.target_schema.as_str();
                let ns = quote_identifier(namespace);
                let replacing = plan.identity.write_mode == WriteMode::Replace;
                let role = if plan.identity.is_identity_materialization() {
                    "identity_materialization"
                } else {
                    "replacement_import"
                };
                if replacing {
                    let prior = self.engine.metadata_query(&format!(
                        "SELECT role FROM _grv.replacement_scopes WHERE owner_scope={} LIMIT 2",
                        quote_literal(scope.as_str())
                    ))?;
                    if prior.is_empty() {
                        let existing=self.engine.metadata_query(&format!("SELECT count(*)::BIGINT FROM _grv.relation_ownership WHERE owner_scope={}",quote_literal(scope.as_str())))?;
                        if existing != vec![vec![Some("0".into())]] {
                            return Err(PullError::OutcomeUnknown(
                                "owned replacement scope lost its fixed role evidence".into(),
                            ));
                        }
                    }
                    if !prior.is_empty() && prior != vec![vec![Some(role.into())]] {
                        return Err(PullError::StateConflict("replacement scope cannot change between identity and application roles".into()));
                    }
                }
                // Validate ownership BEFORE touching source files or destination rows.
                for table in &plan.tables {
                    let owned=self.engine.metadata_query(&format!("SELECT kind,owner_scope FROM _grv.relation_ownership WHERE schema_name={} AND table_name={} LIMIT 2",quote_literal(namespace),quote_literal(table.target_table.as_str())))?;
                    let expected_owner = if replacing {
                        vec![vec![Some(role.into()), Some(scope.as_str().into())]]
                    } else {
                        vec![vec![Some("append_import".into()), Some("shared".into())]]
                    };
                    if !owned.is_empty() && owned != expected_owner {
                        return Err(PullError::StateConflict(
                            "destination is owned by another scope or import role".into(),
                        ));
                    }
                    let exists = self
                        .engine
                        .relation_exists(namespace, table.target_table.as_str())?;
                    if replacing && owned.is_empty() && exists {
                        return Err(PullError::StateConflict(
                            "replacement cannot adopt an unowned application table".into(),
                        ));
                    }
                    self.engine
                        .authorize_relation(namespace, table.target_table.as_str())?;
                    if !replacing && exists {
                        let described = self.engine.metadata_query(&format!(
                            "DESCRIBE SELECT * FROM {ns}.{}",
                            quote_identifier(table.target_table.as_str())
                        ))?;
                        exact_schema(&described, &table.output_contract)?;
                    }
                    if !replacing {
                        let expected = canonical(&table.output_contract)?;
                        let previous=self.engine.metadata_query(&format!("SELECT count(*)::BIGINT FROM _grv.import_bindings WHERE target_schema={} AND target_table={} AND contract<>{}",quote_literal(namespace),quote_literal(table.target_table.as_str()),quote_literal(&expected)))?;
                        if previous != vec![vec![Some("0".into())]] {
                            return Err(PullError::StateConflict(
                                "append bindings require the same exact output contract".into(),
                            ));
                        }
                    }
                }
                // Reserved schemas are created without IF NOT EXISTS. Existing
                // operator schemas cause rollback instead of replacement.
                if self.engine.schema_exists("grv_source")?
                    || self.engine.schema_exists("grv_target")?
                {
                    return Err(PullError::StateConflict(
                        "reserved source or target schema already exists".into(),
                    ));
                }
                self.engine.metadata_query("CREATE SCHEMA grv_source")?;
                self.engine.metadata_query("CREATE SCHEMA grv_target")?;
                let mut counts = serde_json::Map::new();
                for table in &plan.tables {
                    let source = format!("grv_source.{}", quote_identifier(table.name.as_str()));
                    self.engine.metadata_query(&format!(
                        "CREATE TABLE {source}({})",
                        table_definition(&table.source_contract)?
                    ))?;
                    self.engine
                        .authorize_relation("grv_source", table.name.as_str())?;
                    let mut remote_rows = 0u64;
                    for file in &table.files {
                        self.check_stopping()?;
                        if let Some(remote) = &file.remote {
                            let parquet = format!(
                                "read_parquet({},hive_partitioning=false,union_by_name=false)",
                                quote_literal(&crate::s3_config::sql_filename(&remote.location)?)
                            );
                            let described = self.engine.verified_s3_metadata(
                                remote,
                                &format!("DESCRIBE SELECT * FROM {parquet}"),
                            )?;
                            physical_schema(&described, &file.contract)?;
                            if let Some(check) =
                                exact_value_assertion(&file.contract, &parquet, true)
                            {
                                self.engine.verified_s3_command(remote, &check)?;
                            }
                            if let Some(check) = partition_assertion(file)? {
                                self.engine.verified_s3_command(remote, &format!("SELECT CASE WHEN ({check})=0 THEN 0 ELSE error('file partition columns differ from verified selection') END"))?;
                            }
                            let projection = file_projection(file, &table.source_contract)?;
                            let rows = self.engine.verified_s3_metadata(
                                remote,
                                &format!("SELECT count(*)::BIGINT FROM ({projection})"),
                            )?;
                            let count = rows
                                .first()
                                .and_then(|row| row.first())
                                .and_then(Option::as_deref)
                                .and_then(|s| s.parse::<u64>().ok())
                                .filter(|_| rows.len() == 1 && rows[0].len() == 1)
                                .ok_or_else(|| {
                                    PullError::StateConflict("invalid S3 file row count".into())
                                })?;
                            remote_rows = remote_rows
                                .checked_add(count)
                                .ok_or_else(|| invalid("S3 table row count overflow"))?;
                            continue;
                        }
                        let mut verified = VerifiedSource::open(
                            file,
                            self.stopping.clone(),
                            self.source_scratch_bytes,
                        )?;
                        let mut descriptor = file.clone();
                        descriptor.path = format!("/dev/fd/{}", verified.file.as_raw_fd()).into();
                        let described=self.engine.verified_file_metadata(verified.file.as_raw_fd(),&format!("DESCRIBE SELECT * FROM read_parquet({},hive_partitioning=false,union_by_name=false)",quote_literal(descriptor.path.to_str().unwrap())))?;
                        physical_schema(&described, &file.contract)?;
                        if let Some(check) = exact_value_assertion(
                            &file.contract,
                            &format!(
                                "read_parquet({},hive_partitioning=false,union_by_name=false)",
                                quote_literal(descriptor.path.to_str().unwrap())
                            ),
                            true,
                        ) {
                            self.engine
                                .verified_file_command(verified.file.as_raw_fd(), &check)?;
                        }
                        if let Some(check) = partition_assertion(&descriptor)? {
                            self.engine.verified_file_command(verified.file.as_raw_fd(),&format!("SELECT CASE WHEN ({check})=0 THEN 0 ELSE error('file partition columns differ from verified selection') END"))?;
                        }
                        self.engine.verified_file_command(
                            verified.file.as_raw_fd(),
                            &format!(
                                "INSERT INTO {source} {}",
                                file_projection(&descriptor, &table.source_contract)?
                            ),
                        )?;
                        verified.recheck(file)?;
                    }
                    if plan.materialization_mode == MaterializationMode::S3View {
                        counts.insert(
                            table.name.as_str().into(),
                            Value::String(remote_rows.to_string()),
                        );
                    }
                    let target = format!("{ns}.{}", quote_identifier(table.target_table.as_str()));
                    let binding = format!(
                        "grv_target.{}",
                        quote_identifier(table.target_table.as_str())
                    );
                    if !plan.identity.is_identity_materialization()
                        && self
                            .engine
                            .relation_exists(namespace, table.target_table.as_str())?
                    {
                        self.engine.metadata_query(&format!(
                            "CREATE TABLE {binding} AS SELECT * FROM {target}"
                        ))?;
                    } else {
                        self.engine.metadata_query(&format!(
                            "CREATE TABLE {binding}({})",
                            table_definition(&table.output_contract)?
                        ))?;
                    }
                    self.engine
                        .authorize_relation("grv_target", table.target_table.as_str())?;
                }
                // No target is mutated until every selection has been fully
                // evaluated against the same source/local/pre-write snapshot.
                for (index, table) in plan.tables.iter().enumerate() {
                    self.check_stopping()?;
                    if plan.materialization_mode == MaterializationMode::S3View {
                        continue;
                    }
                    let selected = table.sql.clone().unwrap_or_else(|| {
                        format!(
                            "SELECT * FROM grv_source.{}",
                            quote_identifier(table.name.as_str())
                        )
                    });
                    let expected = table
                        .output_contract
                        .columns
                        .iter()
                        .map(|column| {
                            Ok(format!(
                                "CAST(NULL AS {}) AS {}",
                                native_type(&column.logical_type)?,
                                quote_identifier(&column.name)
                            ))
                        })
                        .collect::<io::Result<Vec<_>>>()?
                        .join(",");
                    let stage = format!("_grv_pull_output_{index}");
                    self.engine.stage_pull_query(
                        &selected,
                        &stage,
                        &format!("SELECT {expected}"),
                    )?;
                    if let Some(check) = exact_value_assertion(
                        &table.output_contract,
                        &format!("temp.main.{}", quote_identifier(&stage)),
                        false,
                    ) {
                        self.engine.private_query(&check)?;
                    }
                    let count = self.engine.private_query(&format!(
                        "SELECT count(*)::BIGINT FROM temp.main.{}",
                        quote_identifier(&stage)
                    ))?;
                    counts.insert(
                        table.name.as_str().into(),
                        Value::String(count[0][0].clone().unwrap()),
                    );
                }
                if replacing {
                    let mut after = String::new();
                    loop {
                        let obsolete = self.engine.metadata_query(&format!(
                    "SELECT table_name FROM _grv.relation_ownership WHERE owner_scope={} AND table_name>{} ORDER BY table_name LIMIT 32",
                    quote_literal(scope.as_str()),quote_literal(&after)
                ))?;
                        if obsolete.is_empty() {
                            break;
                        }
                        for row in obsolete {
                            let name = row[0].as_deref().ok_or_else(|| {
                                PullError::OutcomeUnknown("null ownership target".into())
                            })?;
                            after = name.into();
                            if !plan
                                .tables
                                .iter()
                                .any(|table| table.target_table.as_str() == name)
                            {
                                self.engine.authorize_relation(namespace, name)?;
                                if plan.materialization_mode == MaterializationMode::S3View
                                    || self.target_is_view(namespace, name)?
                                {
                                    let prior = self.engine.metadata_query(&format!("SELECT contract FROM _grv.pull_meta WHERE owner_scope={} AND target_table={} LIMIT 2",quote_literal(scope.as_str()), quote_literal(name)))?;
                                    let contract: TableContract = serde_json::from_str(
                                        prior
                                            .first()
                                            .and_then(|row| row.first())
                                            .and_then(Option::as_deref)
                                            .filter(|_| prior.len() == 1 && prior[0].len() == 1)
                                            .ok_or_else(|| {
                                                PullError::OutcomeUnknown(
                                                    "omitted target lacks exact contract evidence"
                                                        .into(),
                                                )
                                            })?,
                                    )
                                    .map_err(|_| {
                                        PullError::OutcomeUnknown(
                                            "invalid omitted target contract".into(),
                                        )
                                    })?;
                                    contract.validate().map_err(io::Error::other)?;
                                    // Inspect catalog types without binding a stale
                                    // remote view: tracking rows cannot redefine
                                    // the schema of an omitted target.
                                    let actual = self.engine.relation_schema(namespace, name)?;
                                    exact_schema(&actual, &contract)?;
                                    self.drop_target(namespace, name)?;
                                    self.engine.metadata_query(&format!(
                                        "CREATE VIEW {ns}.{} AS {}",
                                        quote_identifier(name),
                                        empty_projection(&contract)?
                                    ))?;
                                } else {
                                    self.engine.metadata_query(&format!(
                                        "DELETE FROM {ns}.{}",
                                        quote_identifier(name)
                                    ))?;
                                }
                            }
                        }
                    }
                }
                self.engine
                    .metadata_query(&format!("CREATE SCHEMA IF NOT EXISTS {ns}"))?;
                if plan.identity.is_identity_materialization() {
                    // Omitted materializations were emptied above and advance
                    // with this generation. A target remapped to another logical
                    // table must not leave a stale provenance alias behind.
                    for table in &plan.tables {
                        self.engine.metadata_query(&format!("DELETE FROM _grv.pull_meta WHERE owner_scope={} AND target_table={} AND table_name<>{}",quote_literal(scope.as_str()),quote_literal(table.target_table.as_str()),quote_literal(table.name.as_str())))?;
                    }
                    self.engine.metadata_query(&format!("UPDATE _grv.pull_meta SET grv_revision={},generation_id={} WHERE owner_scope={}",quote_literal(&plan.committed_revision.to_string()),quote_literal(plan.generation_id.as_str()),quote_literal(scope.as_str())))?;
                }
                for (index, table) in plan.tables.iter().enumerate() {
                    self.check_stopping()?;
                    let target = format!("{ns}.{}", quote_identifier(table.target_table.as_str()));
                    let stage = format!(
                        "temp.main.{}",
                        quote_identifier(&format!("_grv_pull_output_{index}"))
                    );
                    if replacing {
                        if plan.materialization_mode == MaterializationMode::S3View {
                            self.drop_target(namespace, table.target_table.as_str())?;
                            let files = table
                                .files
                                .iter()
                                .map(|file| file.remote.clone().expect("validated S3 source"))
                                .collect::<Vec<_>>();
                            let sql =
                                format!("CREATE VIEW {target} AS {}", s3_view_projection(table)?);
                            if files.is_empty() {
                                self.engine.metadata_query(&sql)?;
                            } else {
                                self.engine.verified_s3_files_command(&files, &sql)?;
                            }
                            for column in &table.not_null {
                                let check = format!(
                                    "SELECT CASE WHEN count(*)=0 THEN 0 ELSE error('destination not-null check failed') END FROM {target} WHERE {} IS NULL",
                                    quote_identifier(column)
                                );
                                if files.is_empty() {
                                    self.engine.metadata_query(&check)?;
                                } else {
                                    self.engine.verified_s3_files_command(&files, &check)?;
                                }
                            }
                        } else {
                            if self.target_is_view(namespace, table.target_table.as_str())? {
                                self.drop_target(namespace, table.target_table.as_str())?;
                            }
                            self.engine.private_query(&format!(
                                "CREATE OR REPLACE TABLE {target} AS SELECT * FROM {stage}"
                            ))?;
                        }
                    } else {
                        if !self
                            .engine
                            .relation_exists(namespace, table.target_table.as_str())?
                        {
                            self.engine.metadata_query(&format!(
                                "CREATE TABLE {target}({})",
                                table_definition(&table.output_contract)?
                            ))?;
                        }
                        self.engine.private_query(&format!(
                            "INSERT INTO {target} SELECT * FROM {stage}"
                        ))?;
                    }
                    for column in table
                        .not_null
                        .iter()
                        .filter(|_| plan.materialization_mode == MaterializationMode::Local)
                    {
                        self.engine.metadata_query(&format!("SELECT CASE WHEN count(*)=0 THEN 0 ELSE error('destination not-null check failed') END FROM {target} WHERE {} IS NULL",quote_identifier(column)))?;
                    }
                    if replacing {
                        self.engine.metadata_query(&format!("INSERT INTO _grv.relation_ownership VALUES({},{},{},{}) ON CONFLICT(schema_name,table_name) DO UPDATE SET kind=excluded.kind,owner_scope=excluded.owner_scope",quote_literal(namespace),quote_literal(table.target_table.as_str()),quote_literal(role),quote_literal(scope.as_str())))?;
                    } else {
                        self.engine.metadata_query(&format!("INSERT INTO _grv.relation_ownership VALUES({},{},'append_import','shared') ON CONFLICT(schema_name,table_name) DO NOTHING",quote_literal(namespace),quote_literal(table.target_table.as_str())))?;
                        let binding=grv_types::sha256(&grv_types::canonical_json(&serde_json::json!({"scope":scope,"source_table":table.name,"target_table":table.target_table})).map_err(io::Error::other)?);
                        let evidence = canonical(
                            &serde_json::json!({"identity":plan.identity,"source_table":table.name,"target_table":table.target_table,"output_contract":table.output_contract,"source_contract":table.source_contract}),
                        )?;
                        self.engine.metadata_query(&format!("INSERT INTO _grv.import_bindings VALUES({},{},{},{},{}) ON CONFLICT(binding_id) DO UPDATE SET contract=excluded.contract,evidence=excluded.evidence",quote_literal(binding.as_str()),quote_literal(namespace),quote_literal(table.target_table.as_str()),quote_literal(&canonical(&table.output_contract)?),quote_literal(&evidence)))?;
                    }
                    if plan.identity.is_identity_materialization() {
                        self.engine.metadata_query(&format!("INSERT INTO _grv.pull_meta VALUES({},{},{},{},{},{}) ON CONFLICT(owner_scope,table_name) DO UPDATE SET target_table=excluded.target_table,grv_revision=excluded.grv_revision,generation_id=excluded.generation_id,contract=excluded.contract",quote_literal(scope.as_str()),quote_literal(table.name.as_str()),quote_literal(table.target_table.as_str()),quote_literal(&plan.committed_revision.to_string()),quote_literal(plan.generation_id.as_str()),quote_literal(&canonical(&table.output_contract)?)))?;
                    }
                }
                let receipt = PullReceipt {
                    identity: plan.identity.clone(),
                    materialization_mode: plan.materialization_mode,
                    request_record: plan.request_record.clone(),
                    committed_revision: plan.committed_revision,
                    generation_id: plan.generation_id.clone(),
                    table_counts: Value::Object(counts),
                    table_targets: serde_json::json!(
                        plan.tables
                            .iter()
                            .map(|table| (table.name.as_str(), table.target_table.as_str()))
                            .collect::<std::collections::BTreeMap<_, _>>()
                    ),
                    source_partitions: plan.selected_partitions.clone().unwrap_or_else(|| {
                        let mut records=Vec::new();
                        for table in &plan.tables {for file in &table.files {
                            let record=serde_json::json!({"table":table.name,"partition":file.partition});
                            if !records.contains(&record) {records.push(record);}
                        }}
                        Value::Array(records)
                    }),
                    resolved_source_contracts: serde_json::json!(
                        plan.tables
                            .iter()
                            .map(|table| (&table.name, &table.source_contract))
                            .collect::<Vec<_>>()
                    ),
                    output_contracts: serde_json::json!(
                        plan.tables
                            .iter()
                            .map(|table| (&table.name, &table.output_contract))
                            .collect::<Vec<_>>()
                    ),
                    pulled_at: grv_types::Timestamp::new(
                        chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
                    )
                    .unwrap(),
                };
                let serialized = canonical(&receipt)?;
                // No accepted outcome may outgrow its source-free evidence
                // reader. Leave framing and the other immutable row fields room.
                if serialized.len() > crate::native::METADATA_BYTES - 4096 {
                    return Err(PullError::StateConflict(
                        "receipt exceeds bounded durable evidence allowance".into(),
                    ));
                }
                if replacing {
                    self.engine.metadata_query(&format!("INSERT INTO _grv.replacement_scopes VALUES({},{}) ON CONFLICT(owner_scope) DO UPDATE SET role=excluded.role",quote_literal(scope.as_str()),quote_literal(role)))?;
                }
                if plan.identity.is_identity_materialization() {
                    self.engine.metadata_query(&format!("INSERT INTO _grv.pull_checkpoint VALUES({},{}) ON CONFLICT(owner_scope) DO UPDATE SET receipt=excluded.receipt",quote_literal(scope.as_str()),quote_literal(&serialized)))?;
                } else {
                    self.engine.metadata_query(&format!("UPDATE _grv.import_attempt_details SET is_current=false WHERE owner_scope={}",quote_literal(scope.as_str())))?;
                    self.engine.metadata_query(&format!(
                        "INSERT INTO _grv.import_attempt_details VALUES({},{},{},true)",
                        quote_literal(plan.identity.attempt_id.as_str()),
                        quote_literal(scope.as_str()),
                        quote_literal(&serialized)
                    ))?;
                }
                self.engine.metadata_query(&format!(
                    "INSERT INTO _grv.pull_attempts VALUES({},{},{},{})",
                    quote_literal(plan.identity.attempt_id.as_str()),
                    quote_literal(plan.identity.request_sha256.as_str()),
                    quote_literal(&canonical(&plan.identity.adapter_identity)?),
                    quote_literal(&serialized)
                ))?;
                // Explicit drops discard staged LocalStorage appends; in pinned
                // DuckDB, schema CASCADE alone leaves stale append undo records.
                for (index, table) in plan.tables.iter().enumerate() {
                    self.engine.metadata_query(&format!(
                        "DROP TABLE grv_source.{}",
                        quote_identifier(table.name.as_str())
                    ))?;
                    self.engine.metadata_query(&format!(
                        "DROP TABLE grv_target.{}",
                        quote_identifier(table.target_table.as_str())
                    ))?;
                    if plan.materialization_mode == MaterializationMode::Local {
                        self.engine.private_query(&format!(
                            "DROP TABLE temp.main.{}",
                            quote_identifier(&format!("_grv_pull_output_{index}"))
                        ))?;
                    }
                }
                self.engine.metadata_query("DROP SCHEMA grv_source")?;
                self.engine.metadata_query("DROP SCHEMA grv_target")?;
                self.check_stopping()?;
                // Persist distrust of total metadata loss before an external
                // commit can become ambiguous. Only a known rollback clears it.
                self.engine.record_managed_evidence(true)?;
                committing = true;
                self.initialized_observed = true;
                #[cfg(test)]
                if self.commit_crash == Some(false) {
                    std::process::exit(86);
                }
                #[cfg(test)]
                crate::commit_boundary_tests::pause_at_commit(false, &receipt, &self.stopping);
                self.engine.metadata_query("COMMIT").map_err(|error| {
                    PullError::OutcomeUnknown(format!(
                        "destination commit requires fenced receipt resolution: {error}"
                    ))
                })?;
                #[cfg(test)]
                if self.commit_crash == Some(true) {
                    std::process::exit(86);
                }
                #[cfg(test)]
                crate::commit_boundary_tests::pause_at_commit(true, &receipt, &self.stopping);
                Ok(Box::new(receipt))
            })();
            if result.is_err() {
                let rolled_back = self.engine.metadata_query("ROLLBACK").is_ok();
                // A failed COMMIT is ambiguous even if ROLLBACK appears to
                // succeed: preserve the marker until fenced receipt lookup.
                if initialize && !committing && rolled_back {
                    self.engine.record_managed_evidence(false)?;
                    self.initialized_observed = false;
                }
            }
            result
        }
        /// Read-only binding observation; no initial ownership metadata is written.
        pub fn binding(&mut self, root: &str) -> Result<Option<WorkspaceBinding>, PullError> {
            if !self.engine.managed_initialized()? {
                if self.initialized_observed {
                    return Err(PullError::OutcomeUnknown(
                        "initialized workspace lost its metadata".into(),
                    ));
                }
                return Ok(None);
            }
            self.initialized_observed = true;
            let metadata = |error: io::Error| {
                PullError::OutcomeUnknown(format!("untrustworthy workspace binding: {error}"))
            };
            let described = self
                .engine
                .metadata_query(
                    "DESCRIBE SELECT version,ownership_version FROM _grv.metadata_header",
                )
                .map_err(metadata)?;
            if described.len() != 2
                || described
                    .iter()
                    .any(|row| row.get(1).and_then(Option::as_deref) != Some("BIGINT"))
            {
                return Err(PullError::OutcomeUnknown(
                    "invalid metadata header physical schema".into(),
                ));
            }
            let header = self
                .engine
                .metadata_query(
                    "SELECT version,ownership_version FROM _grv.metadata_header LIMIT 2",
                )
                .map_err(metadata)?;
            if header != vec![vec![Some("1".into()), Some("1".into())]] {
                return Err(PullError::OutcomeUnknown(
                    "unsupported metadata header".into(),
                ));
            }
            let rows = self
                .engine
                .metadata_query("SELECT identity FROM _grv.workspace_binding LIMIT 2")
                .map_err(metadata)?;
            if rows.len() != 1 || rows[0].len() != 1 {
                return Err(PullError::OutcomeUnknown(
                    "incomplete workspace binding".into(),
                ));
            }
            let binding: WorkspaceBinding = serde_json::from_str(
                rows[0][0]
                    .as_deref()
                    .ok_or_else(|| PullError::OutcomeUnknown("null workspace binding".into()))?,
            )
            .map_err(|error| PullError::OutcomeUnknown(error.to_string()))?;
            if binding.canonical_root != root {
                return Err(PullError::StateConflict(
                    "workspace belongs to another root".into(),
                ));
            }
            Ok(Some(binding))
        }
        /// Phase-one discovery requires only the attempt and canonical root.
        /// It reads immutable receipts without source or authentication access.
        pub fn lookup(&mut self, attempt: &Uuid, root: &str) -> Result<PullResolution, PullError> {
            let Some(binding) = self.binding(root)? else {
                return Ok(PullResolution::Uninitialized);
            };
            let described=self.engine.metadata_query("DESCRIBE SELECT attempt_id,request_sha256,adapter_identity,receipt FROM _grv.pull_attempts").map_err(|error|PullError::OutcomeUnknown(format!("untrustworthy attempt history: {error}")))?;
            if described.len() != 4
                || described
                    .iter()
                    .any(|row| row.get(1).and_then(Option::as_deref) != Some("VARCHAR"))
            {
                return Err(PullError::OutcomeUnknown(
                    "invalid receipt store physical schema".into(),
                ));
            }
            let rows = self
                .engine
                .metadata_query(&format!(
                    "SELECT receipt FROM _grv.pull_attempts WHERE attempt_id={} LIMIT 2",
                    quote_literal(attempt.as_str())
                ))
                .map_err(|error| {
                    PullError::OutcomeUnknown(format!("untrustworthy attempt history: {error}"))
                })?;
            if rows.is_empty() {
                self.engine.metadata_query("SELECT schema_name,table_name,kind,owner_scope FROM _grv.relation_ownership LIMIT 0").map_err(|error|PullError::OutcomeUnknown(format!("untrustworthy ownership: {error}")))?;
                return Ok(PullResolution::NotCommitted);
            }
            if rows.len() != 1 || rows[0].len() != 1 {
                return Err(PullError::OutcomeUnknown(
                    "duplicate/malformed receipt".into(),
                ));
            }
            let receipt: PullReceipt = serde_json::from_str(
                rows[0][0]
                    .as_deref()
                    .ok_or_else(|| PullError::OutcomeUnknown("null receipt".into()))?,
            )
            .map_err(|error| PullError::OutcomeUnknown(error.to_string()))?;
            if receipt.identity.attempt_id != *attempt || receipt.identity.binding != binding {
                return Err(PullError::OutcomeUnknown(
                    "receipt belongs to another workspace or attempt".into(),
                ));
            }
            self.resolve(&receipt.identity)
        }
        /// Native workspace ownership fences previous writers before this lookup.
        /// No source file or GRV backend is consulted, including committed replay.
        pub fn preview(
            &mut self,
            identity: &PullIdentity,
            targets: &[RelationName],
        ) -> Result<PullPreview, PullError> {
            match self.resolve(identity)? {
                PullResolution::Committed(_) => {
                    return Err(PullError::StateConflict(
                        "attempt already committed; resolve its receipt before preparation".into(),
                    ));
                }
                PullResolution::Uninitialized => {
                    return Ok(PullPreview {
                        prior_revision: None,
                        prior_generation_id: None,
                        obsolete_targets: vec![],
                    });
                }
                PullResolution::NotCommitted => {}
            }
            let scope=grv_types::sha256(&grv_types::canonical_json(&serde_json::json!({"binding":identity.binding,"dataset":identity.dataset,"adapter":"duckdb","target_schema":identity.target_schema})).map_err(io::Error::other)?);
            let role = if identity.is_identity_materialization() {
                "identity_materialization"
            } else {
                "replacement_import"
            };
            if identity.write_mode == WriteMode::Replace {
                let previous = self.engine.metadata_query(&format!(
                    "SELECT role FROM _grv.replacement_scopes WHERE owner_scope={} LIMIT 2",
                    quote_literal(scope.as_str())
                ))?;
                if !previous.is_empty() && previous != vec![vec![Some(role.into())]] {
                    return Err(PullError::StateConflict(
                        "replacement scope cannot change role".into(),
                    ));
                }
            }
            for target in targets {
                let records=self.engine.metadata_query(&format!("SELECT kind,owner_scope FROM _grv.relation_ownership WHERE schema_name={} AND table_name={} LIMIT 2",quote_literal(identity.target_schema.as_str()),quote_literal(target.as_str())))?;
                let expected = if identity.write_mode == WriteMode::Replace {
                    vec![vec![Some(role.into()), Some(scope.as_str().into())]]
                } else {
                    vec![vec![Some("append_import".into()), Some("shared".into())]]
                };
                if !records.is_empty() && records != expected {
                    return Err(PullError::StateConflict(
                        "destination belongs to another role or scope".into(),
                    ));
                }
                if records.is_empty()
                    && identity.write_mode == WriteMode::Replace
                    && self
                        .engine
                        .relation_exists(identity.target_schema.as_str(), target.as_str())?
                {
                    return Err(PullError::StateConflict(
                        "replacement cannot adopt an application table".into(),
                    ));
                }
            }
            let mut preview = PullPreview {
                prior_revision: None,
                prior_generation_id: None,
                obsolete_targets: vec![],
            };
            if identity.is_identity_materialization()
                && self.engine.relation_exists("_grv", "pull_checkpoint")?
            {
                let rows = self.engine.metadata_query(&format!(
                    "SELECT receipt FROM _grv.pull_checkpoint WHERE owner_scope={} LIMIT 2",
                    quote_literal(scope.as_str())
                ))?;
                if rows.len() == 1
                    && let Some(receipt) = rows[0][0]
                        .as_deref()
                        .and_then(|value| serde_json::from_str::<PullReceipt>(value).ok())
                {
                    preview.prior_revision = Some(receipt.committed_revision);
                    preview.prior_generation_id = Some(receipt.generation_id);
                }
            }
            if identity.write_mode == WriteMode::Replace {
                let mut after = String::new();
                loop {
                    let rows=self.engine.metadata_query(&format!("SELECT table_name FROM _grv.relation_ownership WHERE owner_scope={} AND table_name>{} ORDER BY table_name LIMIT 32",quote_literal(scope.as_str()),quote_literal(&after)))?;
                    if rows.is_empty() {
                        break;
                    }
                    for row in rows {
                        let name = row[0].clone().ok_or_else(|| {
                            PullError::OutcomeUnknown("null ownership target".into())
                        })?;
                        after = name.clone();
                        if !targets.iter().any(|target| target.as_str() == name) {
                            preview.obsolete_targets.push(name);
                        }
                    }
                }
            }
            Ok(preview)
        }
        /// Prior contracts are acquisition-free evidence, available only after
        /// a fixed request has proved absent under the workspace owner lock.
        /// The current scope pointer must equal its immutable commit receipt;
        /// wall-clock timestamps never determine the most recent commit.
        pub fn prior_source_contracts(
            &mut self,
            identity: &PullIdentity,
            tables: &[Name],
        ) -> Result<Vec<grv_adapter_api::NamedContract>, PullError> {
            match self.resolve(identity)? {
                PullResolution::Uninitialized => return Ok(vec![]),
                PullResolution::Committed(_) => {
                    return Err(PullError::StateConflict(
                        "prior contracts require an uncommitted fixed request".into(),
                    ));
                }
                PullResolution::NotCommitted => {}
            }
            let metadata = if identity.is_identity_materialization() {
                "pull_checkpoint"
            } else {
                "import_attempt_details"
            };
            if !self.engine.relation_exists("_grv", metadata)? {
                return Ok(vec![]);
            }
            let described = self.engine.metadata_query(&format!(
                "DESCRIBE SELECT owner_scope,receipt FROM _grv.{metadata}"
            ))?;
            if described.len() != 2
                || described
                    .iter()
                    .any(|row| row.get(1).and_then(Option::as_deref) != Some("VARCHAR"))
            {
                return Err(PullError::OutcomeUnknown(
                    "invalid prior contract store".into(),
                ));
            }
            let scope=grv_types::sha256(&grv_types::canonical_json(&serde_json::json!({"binding":identity.binding,"dataset":identity.dataset,"adapter":"duckdb","target_schema":identity.target_schema})).map_err(io::Error::other)?);
            let rows = self.engine.metadata_query(&format!(
                "SELECT receipt FROM _grv.{metadata} WHERE owner_scope={}{} LIMIT 2",
                quote_literal(scope.as_str()),
                if metadata == "import_attempt_details" {
                    " AND is_current=true"
                } else {
                    ""
                }
            ))?;
            if rows.is_empty() {
                return Ok(vec![]);
            }
            if rows.len() != 1 || rows[0].len() != 1 {
                return Err(PullError::OutcomeUnknown(
                    "ambiguous prior contract receipt".into(),
                ));
            }
            let receipt: PullReceipt =
                serde_json::from_str(rows[0][0].as_deref().ok_or_else(|| {
                    PullError::OutcomeUnknown("null prior contract receipt".into())
                })?)
                .map_err(|error| PullError::OutcomeUnknown(error.to_string()))?;
            if receipt.identity.binding != identity.binding
                || receipt.identity.dataset != identity.dataset
                || receipt.identity.target_schema != identity.target_schema
                || receipt.identity.is_identity_materialization()
                    != identity.is_identity_materialization()
            {
                return Err(PullError::OutcomeUnknown(
                    "prior contract scope mismatch".into(),
                ));
            }
            if !matches!(self.resolve(&receipt.identity)?, PullResolution::Committed(original) if *original == receipt)
            {
                return Err(PullError::OutcomeUnknown(
                    "prior contract receipt lacks immutable commit evidence".into(),
                ));
            }
            let pairs: Vec<(Name, TableContract)> =
                serde_json::from_value(receipt.resolved_source_contracts)
                    .map_err(|error| PullError::OutcomeUnknown(error.to_string()))?;
            let mut seen = std::collections::BTreeSet::new();
            let mut contracts = Vec::new();
            for (table, contract) in pairs {
                if !seen.insert(table.clone()) {
                    return Err(PullError::OutcomeUnknown(
                        "duplicate prior source contract".into(),
                    ));
                }
                contract
                    .validate()
                    .map_err(|error| PullError::OutcomeUnknown(error.to_string()))?;
                if tables.contains(&table) {
                    contracts.push(grv_adapter_api::NamedContract { table, contract });
                }
            }
            Ok(contracts)
        }
        pub fn resolve(&mut self, identity: &PullIdentity) -> Result<PullResolution, PullError> {
            identity.validate()?;
            if !self.engine.managed_initialized()? {
                if self.initialized_observed {
                    return Err(PullError::OutcomeUnknown(
                        "previously initialized workspace lost its consumer metadata".into(),
                    ));
                }
                return Ok(PullResolution::Uninitialized);
            }
            self.initialized_observed = true;
            let metadata = |error: io::Error| {
                PullError::OutcomeUnknown(format!(
                    "incomplete or untrustworthy consumer metadata: {error}"
                ))
            };
            for (query, expected) in [
                (
                    "DESCRIBE SELECT version,ownership_version FROM _grv.metadata_header",
                    vec![("version", "BIGINT"), ("ownership_version", "BIGINT")],
                ),
                (
                    "DESCRIBE SELECT identity FROM _grv.workspace_binding",
                    vec![("identity", "VARCHAR")],
                ),
                (
                    "DESCRIBE SELECT attempt_id,request_sha256,adapter_identity,receipt FROM _grv.pull_attempts",
                    vec![
                        ("attempt_id", "VARCHAR"),
                        ("request_sha256", "VARCHAR"),
                        ("adapter_identity", "VARCHAR"),
                        ("receipt", "VARCHAR"),
                    ],
                ),
            ] {
                let described = self.engine.metadata_query(query).map_err(metadata)?;
                if described.len() != expected.len()
                    || described
                        .iter()
                        .zip(expected)
                        .any(|(row, (name, datatype))| {
                            row.len() < 2
                                || row[0].as_deref() != Some(name)
                                || row[1].as_deref() != Some(datatype)
                        })
                {
                    return Err(PullError::OutcomeUnknown(
                        "invalid consumer metadata physical schema".into(),
                    ));
                }
            }
            let header = self
                .engine
                .metadata_query(
                    "SELECT version,ownership_version FROM _grv.metadata_header LIMIT 2",
                )
                .map_err(metadata)?;
            if header != vec![vec![Some("1".into()), Some("1".into())]] {
                return Err(PullError::OutcomeUnknown(
                    "unsupported metadata header".into(),
                ));
            }
            let rows = self
                .engine
                .metadata_query("SELECT identity FROM _grv.workspace_binding LIMIT 2")
                .map_err(metadata)?;
            if rows.len() != 1 || rows[0].len() != 1 {
                return Err(PullError::OutcomeUnknown(
                    "incomplete workspace binding".into(),
                ));
            }
            let binding: WorkspaceBinding = serde_json::from_str(
                rows[0][0]
                    .as_deref()
                    .ok_or_else(|| PullError::OutcomeUnknown("null workspace binding".into()))?,
            )
            .map_err(|error| PullError::OutcomeUnknown(error.to_string()))?;
            if binding != identity.binding {
                return Err(PullError::StateConflict(
                    "workspace belongs to a different root or workspace identity".into(),
                ));
            }
            // Required even for an absent attempt. A missing store cannot prove rollback.
            let sql = format!(
                "SELECT request_sha256,adapter_identity,receipt FROM _grv.pull_attempts WHERE attempt_id={} LIMIT 2",
                quote_literal(identity.attempt_id.as_str())
            );
            let rows = self.engine.metadata_query(&sql).map_err(metadata)?;
            if rows.is_empty() {
                // Ownership is required to trust absence and any future refresh.
                // A committed receipt can still replay independently of mutable
                // checkpoints, but lost ownership never authorizes new writes.
                self.engine.metadata_query("SELECT schema_name,table_name,kind,owner_scope FROM _grv.relation_ownership LIMIT 0").map_err(metadata)?;
                return Ok(PullResolution::NotCommitted);
            }
            if rows.len() != 1 || rows[0].len() != 3 {
                return Err(PullError::OutcomeUnknown(
                    "duplicate or malformed attempt receipt".into(),
                ));
            }
            let row = &rows[0];
            let adapter: String = String::from_utf8(
                grv_types::canonical_json(&identity.adapter_identity).map_err(io::Error::other)?,
            )
            .map_err(io::Error::other)?;
            if row[0].as_deref() != Some(identity.request_sha256.as_str())
                || row[1].as_deref() != Some(&adapter)
            {
                return Err(PullError::RequestMismatch);
            }
            let receipt: PullReceipt = serde_json::from_str(
                row[2]
                    .as_deref()
                    .ok_or_else(|| PullError::OutcomeUnknown("null attempt receipt".into()))?,
            )
            .map_err(|error| PullError::OutcomeUnknown(error.to_string()))?;
            if !receipt.matches(identity) {
                return Err(PullError::RequestMismatch);
            }
            Ok(PullResolution::Committed(Box::new(receipt)))
        }
    }
}
#[cfg(feature = "native")]
pub use destination::{PullError, PullStore};

#[cfg(feature = "native")]
pub(crate) struct VerifiedSource {
    pub(crate) file: std::fs::File,
    fingerprint: (u64, u64, u64, i64, i64),
    stopping: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    scratch_bytes: usize,
}
#[cfg(feature = "native")]
impl VerifiedSource {
    pub(crate) fn open(
        source: &SourceFile,
        stopping: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
        scratch_bytes: usize,
    ) -> io::Result<Self> {
        use std::os::unix::fs::OpenOptionsExt;
        if source.remote.is_some() {
            return Err(invalid(
                "a remote verified source cannot be opened as a local file",
            ));
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&source.path)?;
        let mut verified = Self {
            file,
            fingerprint: (0, 0, 0, 0, 0),
            stopping,
            scratch_bytes,
        };
        verified.fingerprint = verified.verify(source)?;
        Ok(verified)
    }
    fn verify(&mut self, source: &SourceFile) -> io::Result<(u64, u64, u64, i64, i64)> {
        use sha2::{Digest as _, Sha256};
        use std::{
            io::{Read, Seek},
            os::unix::fs::MetadataExt,
        };
        let metadata = self.file.metadata()?;
        if !metadata.is_file() || metadata.len() != source.bytes.get() {
            return Err(io::Error::other(
                "verified source size differs from file metadata",
            ));
        }
        self.file.rewind()?;
        let mut hash = Sha256::new();
        if self.scratch_bytes == 0 || self.scratch_bytes > 65536 {
            return Err(invalid("invalid source verification scratch budget"));
        }
        let mut buffer = vec![0u8; self.scratch_bytes];
        loop {
            if self
                .stopping
                .as_ref()
                .is_some_and(|stop| stop.load(std::sync::atomic::Ordering::Acquire))
            {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "source verification stopped",
                ));
            }
            let bytes = self.file.read(&mut buffer)?;
            if bytes == 0 {
                break;
            }
            hash.update(&buffer[..bytes]);
        }
        if format!("{:x}", hash.finalize()) != source.sha256.as_str() {
            return Err(io::Error::other(
                "verified source hash differs from file metadata",
            ));
        }
        self.file.rewind()?;
        Ok((
            metadata.dev(),
            metadata.ino(),
            metadata.len(),
            metadata.mtime(),
            metadata.mtime_nsec(),
        ))
    }
    pub(crate) fn recheck(&mut self, source: &SourceFile) -> io::Result<()> {
        if self.verify(source)? != self.fingerprint {
            return Err(io::Error::other("verified source changed during import"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn identity() -> PullIdentity {
        PullIdentity{binding:WorkspaceBinding{canonical_root:"file:///private/grv".into(),workspace_id:Uuid::v4()},attempt_id:Uuid::v4(),request_sha256:Digest::new("a".repeat(64)).unwrap(),adapter_identity:serde_json::from_value(json!({"name":"duckdb","package_version":"0.1.0","interface_version":1,"binding_schema_version":1})).unwrap(),dataset:Name::new("data").unwrap(),target_schema:RelationName::new("app").unwrap(),write_mode:WriteMode::Replace,scope_mode:ScopeMode::Complete,transform_mode:TransformMode::Identity,requested_revision:RequestedRevision::Latest(grv_types::LatestRevision::Latest)}
    }
    fn contract() -> TableContract {
        TableContract {
            columns: vec![grv_types::Column {
                name: "id".into(),
                logical_type: json!("int64"),
            }],
            partition_keys: vec![],
            extensions: json!({}),
            column_ext: json!({}),
        }
    }
    fn plan(identity: PullIdentity) -> PullPlan {
        let contract = contract();
        PullPlan {
            identity,
            committed_revision: U64::new(1).unwrap(),
            generation_id: Uuid::v4(),
            request_record: None,
            selected_partitions: None,
            materialization_mode: MaterializationMode::Local,
            tables: vec![PullTable {
                name: Name::new("rows").unwrap(),
                target_table: RelationName::new("rows").unwrap(),
                source_contract: contract.clone(),
                output_contract: contract,
                files: vec![],
                sql: None,
                not_null: vec![],
            }],
        }
    }
    fn remote_file(contract: TableContract, location: &str) -> SourceFile {
        let remote = grv_adapter_api::VerifiedFile {
            table: Name::new("rows").unwrap(),
            partition: json!({}),
            version: U64::new(1).unwrap(),
            schema: grv_adapter_api::FileSchema {
                columns: contract.columns.clone(),
            },
            access: grv_adapter_api::FileAccess::S3View,
            location: location.into(),
            size: U64::new(1024).unwrap(),
            sha256: Digest::new("b".repeat(64)).unwrap(),
            validator: "exact-etag".into(),
        };
        SourceFile {
            path: PathBuf::new(),
            bytes: remote.size,
            sha256: remote.sha256.clone(),
            partition: remote.partition.clone(),
            contract,
            remote: Some(remote),
        }
    }
    fn remote_plan() -> PullPlan {
        let mut plan = plan(identity());
        plan.identity.binding.canonical_root = "s3://test-bucket/grv-dev".into();
        plan.materialization_mode = MaterializationMode::S3View;
        plan.tables[0].files.push(remote_file(
            contract(),
            "s3://test-bucket/grv-dev/data/version=1/part.parquet",
        ));
        plan
    }
    #[test]
    fn s3_pure_preparation_requires_exact_closed_evidence_without_opening_sources() {
        let plan = remote_plan();
        plan.validate().unwrap();
        let mut encoded = serde_json::to_value(&plan.tables[0].files[0]).unwrap();
        encoded["remote"]["secret_access_key"] = json!("credential-canary");
        assert!(serde_json::from_value::<SourceFile>(encoded).is_err());
        for mutation in 0..6 {
            let mut changed = plan.clone();
            let file = &mut changed.tables[0].files[0];
            match mutation {
                0 => file.path = "/missing/local-copy.parquet".into(),
                1 => file.bytes = U64::new(1025).unwrap(),
                2 => file.remote.as_mut().unwrap().validator.clear(),
                3 => {
                    file.remote.as_mut().unwrap().location =
                        "s3://test-bucket/grv-dev-other/file.parquet".into()
                }
                4 => {
                    file.remote.as_mut().unwrap().location =
                        "s3://test-bucket/grv-dev/*.parquet".into()
                }
                _ => file.remote.as_mut().unwrap().schema.columns[0].logical_type = json!("string"),
            }
            assert!(changed.validate().is_err(), "mutation {mutation}");
        }
        let mut duplicate = plan;
        let repeated = duplicate.tables[0].files[0].clone();
        duplicate.tables[0].files.push(repeated);
        assert!(duplicate.validate().is_err());
    }
    #[test]
    fn s3_pure_preparation_refuses_application_modes_and_non_s3_roots() {
        for mutation in 0..4 {
            let mut plan = remote_plan();
            match mutation {
                0 => plan.identity.write_mode = WriteMode::Append,
                1 => plan.identity.scope_mode = ScopeMode::Selected,
                2 => {
                    plan.identity.transform_mode = TransformMode::Sql;
                    plan.tables[0].sql = Some("SELECT id FROM grv_source.rows".into());
                }
                _ => plan.identity.binding.canonical_root = "gs://test-bucket/grv-dev".into(),
            }
            assert!(plan.validate().is_err(), "mutation {mutation}");
        }
    }
    #[test]
    fn s3_fixed_view_projects_each_historical_file_and_typed_empty_contract() {
        let mut plan = remote_plan();
        let table = &mut plan.tables[0];
        table.source_contract.columns.push(grv_types::Column {
            name: "later".into(),
            logical_type: json!("string"),
        });
        table.output_contract = table.source_contract.clone();
        table.files.push(remote_file(
            table.source_contract.clone(),
            "s3://test-bucket/grv-dev/data/version=2/part.parquet",
        ));
        table.files[1].remote.as_mut().unwrap().version = U64::new(2).unwrap();
        plan.validate().unwrap();
        let sql = s3_view_projection(&plan.tables[0]).unwrap();
        assert_eq!(sql.matches("hive_partitioning=false").count(), 2);
        assert_eq!(sql.matches("union_by_name=false").count(), 2);
        assert!(sql.contains("CAST(NULL AS VARCHAR) AS \"later\""));
        assert!(sql.contains("CAST(\"later\" AS VARCHAR) AS \"later\""));
        assert!(sql.contains(" UNION ALL "));
        assert!(!sql.contains("SELECT *"));
        let mut inconsistent = plan.clone();
        inconsistent.tables[0].files[1]
            .remote
            .as_mut()
            .unwrap()
            .version = U64::new(1).unwrap();
        assert!(inconsistent.validate().is_err());
        plan.tables[0].files.clear();
        assert_eq!(
            s3_view_projection(&plan.tables[0]).unwrap(),
            "SELECT CAST(NULL AS BIGINT) AS \"id\",CAST(NULL AS VARCHAR) AS \"later\" WHERE false"
        );
    }
    #[cfg(feature = "native")]
    #[test]
    fn native_s3_empty_refresh_replaces_old_rows_and_omitted_targets_atomically_and_replays() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty-s3.duckdb");
        let mut initial = plan(identity());
        initial.identity.binding.canonical_root = "s3://test-bucket/grv-dev".into();
        initial.tables[0].files = vec![source(dir.path(), "old.parquet", vec![Some(1), Some(2)])];
        let mut extra = initial.tables[0].clone();
        extra.name = Name::new("old").unwrap();
        extra.target_table = RelationName::new("old").unwrap();
        extra.files = vec![source(dir.path(), "omitted.parquet", vec![Some(3)])];
        initial.tables.push(extra);
        let mut store = PullStore::open(&path).unwrap();
        store.apply_identity(&initial).unwrap();
        let mut empty = initial.clone();
        empty.identity.attempt_id = Uuid::v4();
        empty.identity.request_sha256 = Digest::new("c".repeat(64)).unwrap();
        empty.committed_revision = U64::new(2).unwrap();
        empty.generation_id = Uuid::v4();
        empty.materialization_mode = MaterializationMode::S3View;
        empty.tables.truncate(1);
        empty.tables[0].files.clear();
        let mut corrupt = contract();
        corrupt.columns[0].logical_type = json!("string");
        destination::tracking_contract_for_test(&mut store, "old", &corrupt);
        assert!(store.apply_identity(&empty).is_err());
        assert!(matches!(
            store.resolve(&empty.identity).unwrap(),
            PullResolution::NotCommitted
        ));
        assert_eq!(
            destination::inspect_target_for_test(&mut store, "rows"),
            vec!["1", "2"]
        );
        assert_eq!(
            destination::inspect_target_for_test(&mut store, "old"),
            vec!["3"]
        );
        destination::tracking_contract_for_test(&mut store, "old", &contract());
        let receipt = store.apply_identity(&empty).unwrap();
        assert_eq!(receipt.materialization_mode, MaterializationMode::S3View);
        assert_eq!(receipt.table_counts, json!({"rows":"0"}));
        assert!(destination::inspect_target_for_test(&mut store, "rows").is_empty());
        assert!(destination::inspect_target_for_test(&mut store, "old").is_empty());
        assert!(store.target_is_view("app", "rows").unwrap());
        assert!(store.target_is_view("app", "old").unwrap());
        for row in destination::materializations_for_test(&mut store) {
            assert_eq!(row[2].as_deref(), Some("2"));
            assert_eq!(row[3].as_deref(), Some(empty.generation_id.as_str()));
        }
        drop(store);
        let mut reopened = PullStore::open_bound(&path).unwrap();
        reopened
            .configure_resources(&grv_adapter_api::Resources {
                max_source_unit_bytes: U64::new(1).unwrap(),
                max_scratch_bytes: U64::new(1).unwrap(),
                ..Default::default()
            })
            .unwrap();
        // A committed attempt never opens newly supplied files or authenticates.
        empty.tables[0].files = remote_plan().tables[0].files.clone();
        assert_eq!(reopened.apply_identity(&empty).unwrap(), receipt);
    }
    #[cfg(feature = "native")]
    #[test]
    fn native_s3_missing_reader_rolls_back_initial_binding_and_all_targets() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("no-reader.duckdb");
        let plan = remote_plan();
        let mut store = PullStore::open(&path).unwrap();
        assert!(store.apply_identity(&plan).is_err());
        assert!(matches!(
            store.resolve(&plan.identity).unwrap(),
            PullResolution::Uninitialized
        ));
        drop(store);
        destination::assert_no_destination_for_test(&path);
    }
    #[cfg(feature = "native")]
    #[test]
    fn native_s3_resource_admission_precedes_extension_staging_and_authentication() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = PullStore::open(&dir.path().join("resource-s3.duckdb")).unwrap();
        let reader = crate::s3_config::S3Reader {
            scope: "s3://test-bucket/grv-dev".into(),
            profile: "must-never-authenticate".into(),
            region: "eu-west-1".into(),
        };
        for (source, scratch) in [
            (2 * 1024 * 1024 - 1, 32 * 1024 * 1024),
            (32 * 1024 * 1024, 16 * 1024 * 1024 - 1),
        ] {
            store
                .configure_resources(&grv_adapter_api::Resources {
                    max_source_unit_bytes: U64::new(source).unwrap(),
                    max_scratch_bytes: U64::new(scratch).unwrap(),
                    ..grv_adapter_api::Resources::default()
                })
                .unwrap();
            let error = store.configure_s3_reader(&reader).unwrap_err();
            assert!(
                format!("{error:?}").contains("requires at least 2 MiB source and 16 MiB scratch")
            );
        }
    }
    #[test]
    fn pure_file_projection_preserves_prefix_and_never_infers_hive_values() {
        let mut selected = contract();
        selected.columns.push(grv_types::Column {
            name: "label\"x".into(),
            logical_type: json!("string"),
        });
        let file = SourceFile {
            path: "/private/file'quoted.parquet".into(),
            bytes: U64::new(0).unwrap(),
            sha256: Digest::new("a".repeat(64)).unwrap(),
            contract: contract(),
            partition: json!({}),
            remote: None,
        };
        let query = file_projection(&file, &selected).unwrap();
        assert!(query.contains("CAST(NULL AS VARCHAR) AS \"label\"\"x\""));
        assert!(query.contains("file''quoted.parquet"));
        assert!(query.contains("hive_partitioning=false"));
        assert!(!query.contains("SELECT *"));
    }
    #[test]
    fn pure_preparation_rejects_schema_changes_partition_inference_and_destination_aliases() {
        let mut plan = plan(identity());
        let mut file = SourceFile {
            path: "/missing/data.parquet".into(),
            bytes: U64::new(0).unwrap(),
            sha256: Digest::new("a".repeat(64)).unwrap(),
            contract: contract(),
            partition: json!({}),
            remote: None,
        };
        plan.tables[0].files.push(file.clone());
        plan.validate().unwrap();
        file.contract.columns[0].logical_type = json!("string");
        plan.tables[0].files[0] = file.clone();
        assert!(plan.validate().is_err());
        plan.tables[0].files[0].contract = contract();
        plan.tables[0].files[0].partition = json!({"year":"2026"});
        assert!(plan.validate().is_err());
        plan.tables[0].files.clear();
        plan.tables.push(plan.tables[0].clone());
        assert!(plan.validate().is_err());
    }
    #[cfg(feature = "native")]
    fn source(directory: &std::path::Path, name: &str, values: Vec<Option<i64>>) -> SourceFile {
        use arrow_array::{Int64Array, RecordBatch};
        use parquet::arrow::ArrowWriter;
        let path = directory.join(name);
        let batch = RecordBatch::try_new(
            std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "id",
                arrow_schema::DataType::Int64,
                true,
            )])),
            vec![std::sync::Arc::new(Int64Array::from(values))],
        )
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), batch.schema(), None)
                .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let bytes = std::fs::read(&path).unwrap();
        SourceFile {
            path,
            bytes: U64::new(bytes.len() as u64).unwrap(),
            sha256: grv_types::sha256(&bytes),
            contract: contract(),
            partition: json!({}),
            remote: None,
        }
    }
    #[cfg(feature = "native")]
    #[test]
    fn native_identity_transaction_replays_without_files_and_refuses_lost_receipts() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("pull.duckdb");
        let identity = identity();
        let mut plan = plan(identity.clone());
        let file = source(directory.path(), "rows.parquet", vec![Some(1), Some(2)]);
        plan.tables[0].files.push(file.clone());
        let mut store = PullStore::open(&database).unwrap();
        assert_eq!(
            store.resolve(&identity).unwrap(),
            PullResolution::Uninitialized
        );
        let receipt = store.apply_identity(&plan).unwrap();
        assert_eq!(receipt.table_counts, json!({"rows":"2"}));
        std::fs::remove_file(file.path).unwrap();
        let mut newer = plan.clone();
        newer.identity.attempt_id = Uuid::v4();
        newer.committed_revision = U64::new(2).unwrap();
        newer.generation_id = Uuid::v4();
        newer.tables[0].files = vec![source(
            directory.path(),
            "newer.parquet",
            vec![Some(10), Some(20), Some(30)],
        )];
        let new_receipt = store.apply_identity(&newer).unwrap();
        assert_eq!(new_receipt.table_counts, json!({"rows":"3"}));
        assert_eq!(store.apply_identity(&plan).unwrap(), receipt);
        assert_eq!(
            destination::inspect_target_for_test(&mut store, "rows"),
            vec!["10", "20", "30"]
        );
        let mut mismatch = identity.clone();
        mismatch.request_sha256 = Digest::new("b".repeat(64)).unwrap();
        assert!(matches!(
            store.resolve(&mismatch),
            Err(PullError::RequestMismatch)
        ));
        drop(store);
        // Use the trusted fixture mutator through an isolated test-only hook.
        destination::erase_receipt_store_for_test(&database);
        let mut store = PullStore::open(&database).unwrap();
        assert!(matches!(
            store.resolve(&identity),
            Err(PullError::OutcomeUnknown(_))
        ));
    }
    #[cfg(feature = "native")]
    #[test]
    fn native_prior_contracts_require_matching_scope_and_immutable_receipt_without_source_access() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("prior.duckdb");
        let mut original = plan(identity());
        original.tables[0].files = vec![source(directory.path(), "prior.parquet", vec![Some(1)])];
        let mut store = PullStore::open(&database).unwrap();
        let mut next = original.identity.clone();
        next.attempt_id = Uuid::v4();
        let rows = Name::new("rows").unwrap();
        assert!(
            store
                .prior_source_contracts(&next, std::slice::from_ref(&rows))
                .unwrap()
                .is_empty()
        );
        let receipt = store.apply_identity(&original).unwrap();
        std::fs::remove_file(&original.tables[0].files[0].path).unwrap();
        let evidence = store
            .prior_source_contracts(&next, std::slice::from_ref(&rows))
            .unwrap();
        assert_eq!(
            evidence,
            vec![grv_adapter_api::NamedContract {
                table: rows.clone(),
                contract: contract()
            }]
        );
        assert!(
            store
                .prior_source_contracts(&next, &[Name::new("unrelated").unwrap()])
                .unwrap()
                .is_empty()
        );
        let mut another = next.clone();
        another.dataset = Name::new("other").unwrap();
        assert!(
            store
                .prior_source_contracts(&another, std::slice::from_ref(&rows))
                .unwrap()
                .is_empty()
        );
        another = next.clone();
        another.target_schema = RelationName::new("other").unwrap();
        assert!(
            store
                .prior_source_contracts(&another, std::slice::from_ref(&rows))
                .unwrap()
                .is_empty()
        );
        another = next.clone();
        another.binding.workspace_id = Uuid::v4();
        assert!(matches!(
            store.prior_source_contracts(&another, std::slice::from_ref(&rows)),
            Err(PullError::StateConflict(_))
        ));
        assert!(matches!(
            store.prior_source_contracts(&original.identity, std::slice::from_ref(&rows)),
            Err(PullError::StateConflict(_))
        ));
        destination::corrupt_prior_contract_for_test(&mut store, &receipt);
        assert!(matches!(
            store.prior_source_contracts(&next, std::slice::from_ref(&rows)),
            Err(PullError::OutcomeUnknown(_))
        ));
        destination::erase_mutable_for_test(&mut store);
        assert!(
            store
                .prior_source_contracts(&next, std::slice::from_ref(&rows))
                .unwrap()
                .is_empty()
        );
        assert_eq!(store.apply_identity(&original).unwrap(), receipt);
    }
    #[cfg(feature = "native")]
    #[test]
    fn native_initial_rollback_reuses_fixed_workspace_and_lost_all_metadata_refuses() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("initial.duckdb");
        let mut original = plan(identity());
        original.tables[0].files = vec![source(directory.path(), "null.parquet", vec![None])];
        original.tables[0].not_null = vec!["id".into()];
        let mut store = PullStore::open(&database).unwrap();
        assert!(store.apply(&original).is_err());
        assert!(
            store
                .binding(&original.identity.binding.canonical_root)
                .unwrap()
                .is_none()
        );
        drop(store);
        let locator = crate::binding::locate_pull(json!({"database":database})).unwrap();
        let mut runtime = crate::pull_runtime::Runtime::default();
        let bound = runtime
            .bind(
                locator,
                Some(original.identity.binding.canonical_root.clone()),
                None,
                Some(original.identity.binding.workspace_id.clone()),
                &grv_adapter_api::Resources::default(),
            )
            .unwrap();
        assert_eq!(bound.binding, grv_adapter_api::BindingState::Uninitialized);
        assert_eq!(bound.workspace_id, None);
        runtime.stop().unwrap();
        // Reopen the native owner exactly as a fresh retry process would.
        let mut store = PullStore::open(&database).unwrap();
        assert_eq!(
            store.resolve(&original.identity).unwrap(),
            PullResolution::Uninitialized
        );
        original.tables[0].files = vec![source(directory.path(), "valid.parquet", vec![Some(1)])];
        let receipt = store.apply(&original).unwrap();
        assert_eq!(
            receipt.identity.binding.workspace_id,
            original.identity.binding.workspace_id
        );
        drop(store);
        destination::erase_all_metadata_for_test(&database);
        let mut store = PullStore::open(&database).unwrap();
        assert!(matches!(
            store.binding(&original.identity.binding.canonical_root),
            Err(PullError::OutcomeUnknown(_))
        ));
        assert!(matches!(
            store.resolve(&original.identity),
            Err(PullError::OutcomeUnknown(_))
        ));
    }
    #[cfg(feature = "native")]
    #[test]
    fn native_source_verification_reserves_hash_state_and_obeys_one_byte_source_budget() {
        let directory = tempfile::tempdir().unwrap();
        let mut original = plan(identity());
        original.tables[0].files = vec![source(directory.path(), "tiny.parquet", vec![Some(1)])];
        let mut store = PullStore::open(&directory.path().join("tiny.duckdb")).unwrap();
        let resources = grv_adapter_api::Resources {
            max_source_unit_bytes: U64::new(1).unwrap(),
            max_scratch_bytes: U64::new(1).unwrap(),
            ..Default::default()
        };
        store.configure_resources(&resources).unwrap();
        assert!(
            matches!(store.apply(&original), Err(PullError::Engine(error)) if error.kind() == io::ErrorKind::InvalidInput)
        );
        assert_eq!(
            store.resolve(&original.identity).unwrap(),
            PullResolution::Uninitialized
        );
        let resources = grv_adapter_api::Resources {
            max_scratch_bytes: U64::new(std::mem::size_of::<sha2::Sha256>() as u64 + 1).unwrap(),
            ..resources
        };
        store.configure_resources(&resources).unwrap();
        assert_eq!(destination::scratch_bytes_for_test(&store), 1);
        let receipt = store.apply(&original).unwrap();
        assert_eq!(receipt.table_counts, json!({"rows":"1"}));
        std::fs::remove_file(&original.tables[0].files[0].path).unwrap();
        store
            .configure_resources(&grv_adapter_api::Resources {
                max_scratch_bytes: U64::new(1).unwrap(),
                ..resources
            })
            .unwrap();
        assert_eq!(store.apply(&original).unwrap(), receipt);
    }
    #[cfg(feature = "native")]
    #[test]
    fn native_oversized_receipt_cannot_commit_an_unreplayable_outcome() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("bounded.duckdb");
        let mut original = plan(identity());
        original.tables[0].source_contract.extensions =
            json!({"large":"x".repeat(crate::native::METADATA_BYTES)});
        original.tables[0].output_contract = original.tables[0].source_contract.clone();
        let mut store = PullStore::open(&database).unwrap();
        assert!(
            matches!(store.apply(&original), Err(PullError::StateConflict(message)) if message.contains("evidence allowance"))
        );
        assert_eq!(
            store.resolve(&original.identity).unwrap(),
            PullResolution::Uninitialized
        );
        drop(store);
        destination::assert_no_destination_for_test(&database);
    }
    #[cfg(feature = "native")]
    #[test]
    fn native_materialization_provenance_advances_omitted_tables_and_preserves_app_attempts() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("provenance.duckdb");
        let mut original = plan(identity());
        let mut omitted = original.tables[0].clone();
        omitted.name = Name::new("second").unwrap();
        omitted.target_table = RelationName::new("second").unwrap();
        original.tables.push(omitted);
        let mut store = PullStore::open(&database).unwrap();
        store.apply(&original).unwrap();
        let mut next = original.clone();
        next.identity.attempt_id = Uuid::v4();
        next.committed_revision = U64::new(2).unwrap();
        next.generation_id = Uuid::v4();
        next.tables.pop();
        next.tables[0].target_table = RelationName::new("renamed").unwrap();
        store.apply(&next).unwrap();
        assert_eq!(
            destination::materializations_for_test(&mut store),
            vec![
                vec![
                    Some("rows".into()),
                    Some("renamed".into()),
                    Some("2".into()),
                    Some(next.generation_id.as_str().into())
                ],
                vec![
                    Some("second".into()),
                    Some("second".into()),
                    Some("2".into()),
                    Some(next.generation_id.as_str().into())
                ],
            ]
        );
        drop(store);
        let database = directory.path().join("app.duckdb");
        let mut app = original;
        app.tables.pop();
        app.identity.write_mode = WriteMode::Append;
        let mut store = PullStore::open(&database).unwrap();
        store.apply(&app).unwrap();
        let mut next = app.clone();
        next.identity.attempt_id = Uuid::v4();
        store.apply(&next).unwrap();
        assert_eq!(destination::app_attempts_for_test(&mut store), 2);
        let mut pending = next.identity.clone();
        pending.attempt_id = Uuid::v4();
        assert_eq!(
            store
                .prior_source_contracts(&pending, &[Name::new("rows").unwrap()])
                .unwrap()[0]
                .contract,
            contract()
        );
    }
    #[cfg(feature = "native")]
    #[test]
    fn native_pull_commit_crash_probe() {
        let Ok(path) = std::env::var("GRV_TEST_COMMIT_PLAN") else {
            return;
        };
        let plan: PullPlan = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let database = std::env::var("GRV_TEST_COMMIT_DATABASE").unwrap();
        let after = std::env::var("GRV_TEST_COMMIT_AFTER").unwrap() == "true";
        let mut store = PullStore::open(std::path::Path::new(&database)).unwrap();
        destination::crash_at_commit_for_test(&mut store, after);
        store.apply(&plan).unwrap();
        panic!("commit crash hook did not execute");
    }
    #[cfg(feature = "native")]
    #[test]
    fn native_precommit_and_postcommit_process_exit_resolve_without_reapplying() {
        for materialization_mode in [MaterializationMode::Local, MaterializationMode::S3View] {
            for after in [false, true] {
                let directory = tempfile::tempdir().unwrap();
                let database = directory.path().join("crash.duckdb");
                let mut baseline = plan(identity());
                if materialization_mode == MaterializationMode::S3View {
                    baseline.identity.binding.canonical_root = "s3://test-bucket/grv-dev".into();
                }
                baseline.tables[0].files =
                    vec![source(directory.path(), "old.parquet", vec![Some(1)])];
                PullStore::open(&database)
                    .unwrap()
                    .apply(&baseline)
                    .unwrap();
                let mut next = baseline.clone();
                next.identity.attempt_id = Uuid::v4();
                next.committed_revision = U64::new(2).unwrap();
                next.generation_id = Uuid::v4();
                next.tables[0].files = vec![source(
                    directory.path(),
                    "new.parquet",
                    vec![Some(7), Some(8)],
                )];
                next.materialization_mode = materialization_mode;
                if materialization_mode == MaterializationMode::S3View {
                    next.tables[0].files.clear();
                }
                let path = directory.path().join("plan.json");
                std::fs::write(&path, serde_json::to_vec(&next).unwrap()).unwrap();
                let result = std::process::Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "pull::tests::native_pull_commit_crash_probe",
                        "--nocapture",
                    ])
                    .env("GRV_TEST_COMMIT_PLAN", path)
                    .env("GRV_TEST_COMMIT_DATABASE", &database)
                    .env("GRV_TEST_COMMIT_AFTER", after.to_string())
                    .output()
                    .unwrap();
                assert_eq!(
                    result.status.code(),
                    Some(86),
                    "{}",
                    String::from_utf8_lossy(&result.stderr)
                );
                if materialization_mode == MaterializationMode::Local {
                    std::fs::remove_file(&next.tables[0].files[0].path).unwrap();
                }
                let mut store = PullStore::open(&database).unwrap();
                match store.resolve(&next.identity).unwrap() {
                    PullResolution::Committed(receipt) if after => {
                        assert_eq!(receipt.committed_revision.get(), 2);
                        assert_eq!(store.apply(&next).unwrap(), receipt);
                        assert_eq!(
                            destination::inspect_target_for_test(&mut store, "rows"),
                            if materialization_mode == MaterializationMode::Local {
                                vec!["7", "8"]
                            } else {
                                vec![]
                            }
                        );
                        // Advance the mutable checkpoint after recovering the
                        // lost commit result, then replay the original attempt
                        // with its source already deleted. Receipt is immutable
                        // and must neither reapply nor rewind destination rows.
                        let mut later = next.clone();
                        later.identity.attempt_id = Uuid::v4();
                        later.committed_revision = U64::new(3).unwrap();
                        later.generation_id = Uuid::v4();
                        if materialization_mode == MaterializationMode::Local {
                            later.tables[0].files =
                                vec![source(directory.path(), "later.parquet", vec![Some(11)])];
                        }
                        let newer = store.apply(&later).unwrap();
                        assert_eq!(newer.committed_revision.get(), 3);
                        assert_eq!(store.apply(&next).unwrap(), receipt);
                        assert_eq!(
                            destination::inspect_target_for_test(&mut store, "rows"),
                            if materialization_mode == MaterializationMode::Local {
                                vec!["11"]
                            } else {
                                vec![]
                            }
                        );
                    }
                    PullResolution::NotCommitted if !after => {
                        assert_eq!(
                            destination::inspect_target_for_test(&mut store, "rows"),
                            vec!["1"]
                        );
                    }
                    unexpected => panic!("unexpected crash outcome: {unexpected:?}"),
                }
            }
        }
    }
    #[cfg(feature = "native")]
    #[test]
    fn native_second_target_failure_rolls_back_rows_binding_and_receipt() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("rollback.duckdb");
        let identity = identity();
        let mut plan = plan(identity.clone());
        plan.tables[0]
            .files
            .push(source(directory.path(), "first.parquet", vec![Some(1)]));
        let mut second = plan.tables[0].clone();
        second.name = Name::new("second").unwrap();
        second.target_table = RelationName::new("second").unwrap();
        second.files = vec![source(directory.path(), "second.parquet", vec![None])];
        second.not_null = vec!["id".into()];
        plan.tables.push(second);
        let mut store = PullStore::open(&database).unwrap();
        let failure = store.apply_identity(&plan).unwrap_err();
        assert!(
            format!("{failure:?}").contains("destination not-null check failed"),
            "{failure:?}"
        );
        assert_eq!(
            store.resolve(&identity).unwrap(),
            PullResolution::Uninitialized
        );
        drop(store);
        destination::assert_no_destination_for_test(&database);
    }
    #[test]
    fn destination_names_follow_registered_duckdb_grammar() {
        assert!(RelationName::new("_ordinary").is_ok());
        assert!(RelationName::new("a".repeat(300)).is_ok());
        for value in ["", "Upper", "-first", "a.b", "a b"] {
            assert!(RelationName::new(value).is_err());
        }
        let mut request = identity();
        request.target_schema = RelationName::new("_grv_session_123abc").unwrap();
        assert!(request.validate().is_err());
        request.target_schema = RelationName::new("_ordinary").unwrap();
        request.validate().unwrap();
    }
    #[cfg(feature = "native")]
    #[test]
    fn native_sql_append_stages_all_queries_against_prewrite_snapshot_and_replays() {
        let directory = tempfile::tempdir().unwrap();
        let mut initial = plan(identity());
        initial.identity.write_mode = WriteMode::Append;
        initial.tables[0].files = vec![source(directory.path(), "one.parquet", vec![Some(1)])];
        let mut second = initial.tables[0].clone();
        second.name = Name::new("second").unwrap();
        second.target_table = RelationName::new("second").unwrap();
        second.files = vec![source(directory.path(), "two.parquet", vec![Some(2)])];
        initial.tables.push(second);
        let mut store = PullStore::open(&directory.path().join("append.duckdb")).unwrap();
        store.apply(&initial).unwrap();
        let mut selected = initial.clone();
        selected.identity.attempt_id = Uuid::v4();
        selected.identity.transform_mode = TransformMode::Sql;
        selected.tables[0].sql=Some("WITH selected AS (SELECT * FROM grv_source.rows) SELECT id+coalesce((SELECT sum(id) FROM grv_target.second),0)::BIGINT AS id FROM selected".into());
        selected.tables[1].sql=Some("SELECT id+coalesce((SELECT sum(id) FROM app.rows),0)::BIGINT AS id FROM grv_source.second".into());
        let receipt = store.apply(&selected).unwrap();
        assert_eq!(receipt.table_counts, json!({"rows":"1","second":"1"}));
        assert_eq!(
            destination::inspect_target_for_test(&mut store, "rows"),
            vec!["1", "3"]
        );
        assert_eq!(
            destination::inspect_target_for_test(&mut store, "second"),
            vec!["2", "3"]
        );
        for table in &selected.tables {
            std::fs::remove_file(&table.files[0].path).unwrap();
        }
        assert_eq!(store.apply(&selected).unwrap(), receipt);
        assert_eq!(
            destination::inspect_target_for_test(&mut store, "rows"),
            vec!["1", "3"]
        );
    }
    #[cfg(feature = "native")]
    #[test]
    fn native_local_nested_views_macros_and_guarded_sql_boundaries() {
        let directory = tempfile::tempdir().unwrap();
        let mut initial = plan(identity());
        initial.identity.write_mode = WriteMode::Append;
        initial.tables[0].files = vec![source(directory.path(), "row.parquet", vec![Some(1)])];
        let mut store = PullStore::open(&directory.path().join("locals.duckdb")).unwrap();
        store.apply(&initial).unwrap();
        destination::local_views_for_test(&mut store);
        let mut selected = initial.clone();
        selected.identity.attempt_id = Uuid::v4();
        selected.identity.transform_mode = TransformMode::Sql;
        selected.tables[0].sql =
            Some("SELECT main.plus_ten(id)::BIGINT AS id FROM app.nested_view".into());
        store.apply(&selected).unwrap();
        assert_eq!(
            destination::inspect_target_for_test(&mut store, "rows"),
            vec!["1", "11"]
        );
        for query in [
            "SELECT 1::BIGINT AS id FROM _grv.workspace_binding",
            "SELECT 1::BIGINT AS id FROM duckdb_secrets()",
            "SELECT 1::BIGINT AS id FROM read_parquet('/missing')",
            "SELECT nextval('side_effect')::BIGINT AS id",
            "SELECT 1::BIGINT AS id; SELECT 2::BIGINT AS id",
            "DELETE FROM app.rows",
            "SELECT 1::INTEGER AS id",
            "SELECT 1::BIGINT AS renamed",
            "SELECT 1::BIGINT AS id FROM temp.main._grv_pull_output_0",
        ] {
            selected.identity.attempt_id = Uuid::v4();
            selected.tables[0].sql = Some(query.into());
            assert!(
                store.apply(&selected).is_err(),
                "unexpected permitted SQL: {query}"
            );
            assert_eq!(
                store.resolve(&selected.identity).unwrap(),
                PullResolution::NotCommitted
            );
            assert_eq!(
                destination::inspect_target_for_test(&mut store, "rows"),
                vec!["1", "11"]
            );
        }
    }
    #[cfg(feature = "native")]
    #[test]
    fn native_append_refuses_replacement_ownership_and_role_switch() {
        let directory = tempfile::tempdir().unwrap();
        let mut initial = plan(identity());
        let mut store = PullStore::open(&directory.path().join("roles.duckdb")).unwrap();
        store.apply_identity(&initial).unwrap();
        initial.identity.attempt_id = Uuid::v4();
        initial.identity.write_mode = WriteMode::Append;
        assert!(matches!(
            store.apply(&initial),
            Err(PullError::StateConflict(_))
        ));
        initial.identity.attempt_id = Uuid::v4();
        initial.identity.write_mode = WriteMode::Replace;
        initial.identity.scope_mode = ScopeMode::Selected;
        assert!(matches!(
            store.apply(&initial),
            Err(PullError::StateConflict(_))
        ));
    }
    #[cfg(feature = "native")]
    fn partition_source(directory: &std::path::Path, filename: &str, actual: &str) -> SourceFile {
        use arrow_array::{Int64Array, RecordBatch, StringArray};
        use parquet::arrow::ArrowWriter;
        let path = directory.join(filename);
        let schema = std::sync::Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, true),
            arrow_schema::Field::new("_year_", arrow_schema::DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                std::sync::Arc::new(Int64Array::from(vec![5])),
                std::sync::Arc::new(StringArray::from(vec![actual])),
            ],
        )
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let mut contract = contract();
        contract.columns.push(grv_types::Column {
            name: "_year_".into(),
            logical_type: json!("string"),
        });
        contract.partition_keys = vec![Name::new("year").unwrap()];
        SourceFile {
            path,
            bytes: U64::new(bytes.len() as u64).unwrap(),
            sha256: grv_types::sha256(&bytes),
            contract,
            partition: json!({"year":"2026"}),
            remote: None,
        }
    }
    #[cfg(feature = "native")]
    #[test]
    fn native_selected_partition_uses_stored_values_and_refuses_metadata_mismatch() {
        let directory = tempfile::tempdir().unwrap();
        let mut selected = plan(identity());
        selected.identity.scope_mode = ScopeMode::Selected;
        let file = partition_source(directory.path(), "part.parquet", "2026");
        selected.tables[0].source_contract = file.contract.clone();
        selected.tables[0].output_contract = file.contract.clone();
        selected.tables[0].files = vec![file];
        let mut store = PullStore::open(&directory.path().join("partitions.duckdb")).unwrap();
        let receipt = store.apply(&selected).unwrap();
        assert_eq!(receipt.table_counts, json!({"rows":"1"}));
        assert_eq!(
            destination::inspect_target_for_test(&mut store, "rows"),
            vec!["5"]
        );
        selected.identity.attempt_id = Uuid::v4();
        selected.tables[0].files = vec![partition_source(
            directory.path(),
            "mismatch.parquet",
            "2027",
        )];
        let failure = store.apply(&selected).unwrap_err();
        assert!(
            format!("{failure:?}").contains("partition columns differ"),
            "{failure:?}"
        );
        assert_eq!(
            destination::inspect_target_for_test(&mut store, "rows"),
            vec!["5"]
        );
        assert_eq!(
            store.resolve(&selected.identity).unwrap(),
            PullResolution::NotCommitted
        );
    }
    #[cfg(feature = "native")]
    #[test]
    fn native_append_second_target_failure_rolls_back_existing_rows_and_receipt() {
        let directory = tempfile::tempdir().unwrap();
        let mut initial = plan(identity());
        initial.identity.write_mode = WriteMode::Append;
        initial.tables[0].files = vec![source(directory.path(), "one.parquet", vec![Some(1)])];
        let mut second = initial.tables[0].clone();
        second.name = Name::new("second").unwrap();
        second.target_table = RelationName::new("second").unwrap();
        second.files = vec![source(directory.path(), "two.parquet", vec![Some(2)])];
        initial.tables.push(second);
        let mut store = PullStore::open(&directory.path().join("append-rollback.duckdb")).unwrap();
        store.apply(&initial).unwrap();
        initial.identity.attempt_id = Uuid::v4();
        initial.tables[1].files = vec![source(directory.path(), "null.parquet", vec![None])];
        initial.tables[1].not_null = vec!["id".into()];
        let failure = store.apply(&initial).unwrap_err();
        assert!(
            format!("{failure:?}").contains("destination not-null check failed"),
            "{failure:?}"
        );
        assert_eq!(
            destination::inspect_target_for_test(&mut store, "rows"),
            vec!["1"]
        );
        assert_eq!(
            destination::inspect_target_for_test(&mut store, "second"),
            vec!["2"]
        );
        assert_eq!(
            store.resolve(&initial.identity).unwrap(),
            PullResolution::NotCommitted
        );
    }
    #[cfg(feature = "native")]
    #[test]
    fn native_pull_owner_cancellation_joins_and_fences_before_outcome_resolution() {
        use crate::{lock::WorkspaceLock, pull_worker::PullWorker};
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("cancel-pull.duckdb");
        let mut plan = plan(identity());
        plan.identity.transform_mode = TransformMode::Sql;
        plan.tables[0].sql =
            Some("SELECT sum(range)::BIGINT AS id FROM range(100000000000)".into());
        let mut owner = PullWorker::open(&database, false).unwrap();
        assert!(WorkspaceLock::acquire(&database).is_err());
        let stop = Arc::new(AtomicBool::new(false));
        let trigger = stop.clone();
        let cancel = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            trigger.store(true, Ordering::Release);
        });
        let started = std::time::Instant::now();
        assert!(owner.apply(plan.clone(), stop.as_ref()).is_err());
        cancel.join().unwrap();
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
        assert!(WorkspaceLock::acquire(&database).is_ok());
        let mut store = PullStore::open(&database).unwrap();
        assert_eq!(
            store.resolve(&plan.identity).unwrap(),
            PullResolution::Uninitialized
        );
    }
    #[cfg(feature = "native")]
    #[test]
    fn native_full_rebuild_recovers_mutable_metadata_and_empties_obsolete_mapping() {
        let directory = tempfile::tempdir().unwrap();
        let mut selected = plan(identity());
        selected.tables[0].files = vec![source(directory.path(), "one.parquet", vec![Some(1)])];
        let mut store = PullStore::open(&directory.path().join("rebuild.duckdb")).unwrap();
        let original = store.apply_identity(&selected).unwrap();
        destination::erase_mutable_for_test(&mut store);
        assert_eq!(store.apply_identity(&selected).unwrap(), original);
        selected.identity.attempt_id = Uuid::v4();
        selected.tables[0].target_table = RelationName::new("_ordinary").unwrap();
        selected.tables[0].files = vec![source(directory.path(), "new.parquet", vec![Some(2)])];
        store.apply_identity(&selected).unwrap();
        assert!(destination::inspect_target_for_test(&mut store, "rows").is_empty());
        assert_eq!(
            destination::inspect_target_for_test(&mut store, "_ordinary"),
            vec!["2"]
        );
    }
    #[cfg(feature = "native")]
    #[test]
    fn native_sql_rejects_submillisecond_utc_values_and_infinite_dates() {
        let directory = tempfile::tempdir().unwrap();
        let mut selected = plan(identity());
        selected.identity.transform_mode = TransformMode::Sql;
        let mut store = PullStore::open(&directory.path().join("exact-pull.duckdb")).unwrap();
        for (datatype, query) in [
            (
                json!({"timestamp":{"unit":"ms","utc":true}}),
                "SELECT TIMESTAMPTZ '1970-01-01 00:00:00.000001+00' AS id",
            ),
            (json!("date"), "SELECT DATE 'infinity' AS id"),
        ] {
            selected.identity.attempt_id = Uuid::v4();
            selected.tables[0].sql = Some(query.into());
            selected.tables[0].output_contract.columns[0].logical_type = datatype;
            let failure = store.apply(&selected).unwrap_err();
            assert!(
                format!("{failure:?}").contains("not exactly representable"),
                "{failure:?}"
            );
            assert_eq!(
                store.resolve(&selected.identity).unwrap(),
                PullResolution::Uninitialized
            );
        }
        selected.identity.attempt_id = Uuid::v4();
        selected.tables[0].sql =
            Some("SELECT TIMESTAMPTZ '1970-01-01 00:00:00.001+00' AS id".into());
        selected.tables[0].output_contract.columns[0].logical_type =
            json!({"timestamp":{"unit":"ms","utc":true}});
        store.apply(&selected).unwrap();
    }
    #[cfg(feature = "native")]
    #[test]
    fn native_append_bindings_share_exact_table_and_remain_managed_sources() {
        use crate::{
            journal::{AcquisitionIntent, AcquisitionJournal},
            native::{NativeEngine, SourceSelection},
        };
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("shared-append.duckdb");
        let mut selected = plan(identity());
        selected.identity.write_mode = WriteMode::Append;
        selected.tables[0].files = vec![source(directory.path(), "one.parquet", vec![Some(1)])];
        let mut store = PullStore::open(&database).unwrap();
        store.apply(&selected).unwrap();
        selected.identity.attempt_id = Uuid::v4();
        selected.identity.dataset = Name::new("another").unwrap();
        store.apply(&selected).unwrap();
        assert_eq!(
            destination::inspect_target_for_test(&mut store, "rows"),
            vec!["1", "1"]
        );
        selected.identity.attempt_id = Uuid::v4();
        selected.tables[0].source_contract.columns[0].logical_type = json!("string");
        selected.tables[0].output_contract = selected.tables[0].source_contract.clone();
        selected.tables[0].files.clear();
        assert!(matches!(
            store.apply(&selected),
            Err(PullError::StateConflict(_))
        ));
        drop(store);
        let private = tempfile::tempdir().unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(private.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let journal = AcquisitionJournal::start(
            private.path(),
            AcquisitionIntent {
                attempt_id: Uuid::v4(),
                request_sha256: Digest::new("a".repeat(64)).unwrap(),
                adapter_identity: selected.identity.adapter_identity,
                connection_identity: "duckdb:test".into(),
                snapshot_id: Uuid::v4(),
                capture_start: grv_types::Timestamp::new("2026-10-06T00:00:00Z").unwrap(),
                reopenable: false,
            },
        )
        .unwrap();
        let mut source = NativeEngine::open_readonly(&database).unwrap();
        let failure = source
            .acquire_sources(
                &[SourceSelection {
                    schema: "app".into(),
                    table: "rows".into(),
                    columns: vec!["id".into()],
                    filter: None,
                }],
                &journal,
            )
            .unwrap_err();
        assert!(failure.to_string().contains("managed imports"), "{failure}");
    }
    #[cfg(feature = "native")]
    #[test]
    fn native_identity_preserves_every_v1_parquet_mapping_exactly() {
        use arrow_array::{
            ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array,
            Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
            TimestampMillisecondArray, TimestampNanosecondArray,
        };
        use arrow_schema::{DataType, Field, Schema, TimeUnit};
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;
        let fields = vec![
            Field::new("b", DataType::Boolean, true),
            Field::new("id", DataType::Int64, true),
            Field::new("f", DataType::Float64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("blob", DataType::Binary, true),
            Field::new("date", DataType::Date32, true),
            Field::new("decimal", DataType::Decimal128(30, 2), true),
            Field::new("ms", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("us", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("ns", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new(
                "utc_ms",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                true,
            ),
            Field::new(
                "utc_us",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ];
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(Int64Array::from(vec![-7])),
            Arc::new(Float64Array::from(vec![1.25])),
            Arc::new(StringArray::from(vec!["🍕"])),
            Arc::new(BinaryArray::from(vec![&[0u8, 255][..]])),
            Arc::new(Date32Array::from(vec![1])),
            Arc::new(
                Decimal128Array::from(vec![123456789012345678901234567812i128])
                    .with_precision_and_scale(30, 2)
                    .unwrap(),
            ),
            Arc::new(TimestampMillisecondArray::from(vec![1])),
            Arc::new(TimestampMicrosecondArray::from(vec![1])),
            Arc::new(TimestampNanosecondArray::from(vec![1])),
            Arc::new(TimestampMillisecondArray::from(vec![1]).with_timezone("UTC")),
            Arc::new(TimestampMicrosecondArray::from(vec![1]).with_timezone("UTC")),
        ];
        let logical = vec![
            json!("boolean"),
            json!("int64"),
            json!("float64"),
            json!("string"),
            json!("binary"),
            json!("date"),
            json!({"decimal":{"precision":30,"scale":2}}),
            json!({"timestamp":{"unit":"ms","utc":false}}),
            json!({"timestamp":{"unit":"us","utc":false}}),
            json!({"timestamp":{"unit":"ns","utc":false}}),
            json!({"timestamp":{"unit":"ms","utc":true}}),
            json!({"timestamp":{"unit":"us","utc":true}}),
        ];
        let contract = TableContract {
            columns: fields
                .iter()
                .zip(logical)
                .map(|(field, logical_type)| grv_types::Column {
                    name: field.name().clone(),
                    logical_type,
                })
                .collect(),
            partition_keys: vec![],
            extensions: json!({}),
            column_ext: json!({}),
        };
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("all-types.parquet");
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let mut plan = plan(identity());
        plan.tables[0].source_contract = contract.clone();
        plan.tables[0].output_contract = contract.clone();
        plan.tables[0].files = vec![SourceFile {
            path,
            bytes: U64::new(bytes.len() as u64).unwrap(),
            sha256: grv_types::sha256(&bytes),
            contract,
            partition: json!({}),
            remote: None,
        }];
        let mut store = PullStore::open(&directory.path().join("all-types.duckdb")).unwrap();
        let receipt = store.apply_identity(&plan).unwrap();
        assert_eq!(receipt.table_counts, json!({"rows":"1"}));
        destination::typed_values_for_test(&mut store);
    }
}
