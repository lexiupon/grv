//! Closed pull lifecycle records. Verified file metadata conveys reader facts;
//! no storage ownership token or backend implementation crosses this boundary.
use super::*;
use grv_types::{PullRequestIdentity, RequestedRevision, pull_request_digest};
use std::collections::BTreeSet;

fn invalid(message: &str) -> ValidationError {
    ValidationError(message.into())
}
fn positive(value: U64) -> Result<(), ValidationError> {
    if value.get() == 0 {
        Err(invalid("file versions must be positive"))
    } else {
        Ok(())
    }
}
fn partition(value: &Value, contract: &TableContract) -> Result<(), ValidationError> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid("partition must be an object"))?;
    if object.len() != contract.partition_keys.len()
        || contract.partition_keys.iter().any(|key| {
            object
                .get(key.as_str())
                .and_then(Value::as_str)
                .is_none_or(|value| Name::new(value).is_err())
        })
    {
        return Err(invalid("partition differs from its table layout"));
    }
    Ok(())
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileSchema {
    pub columns: Vec<Column>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FileAccess {
    Local,
    S3View,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifiedFile {
    pub table: Name,
    pub partition: Value,
    pub version: U64,
    pub schema: FileSchema,
    pub access: FileAccess,
    pub location: String,
    pub size: U64,
    pub sha256: Digest,
    pub validator: String,
}
impl VerifiedFile {
    pub fn validate(&self) -> Result<(), ValidationError> {
        positive(self.version)?;
        if self.size.get() == 0 || self.validator.is_empty() || self.location.contains('\0') {
            return Err(invalid("invalid verified file metadata"));
        }
        match self.access {
            FileAccess::Local if !std::path::Path::new(&self.location).is_absolute() => {
                return Err(invalid("local file location must be absolute"));
            }
            FileAccess::S3View => {
                let uri = self
                    .location
                    .strip_prefix("s3://")
                    .ok_or_else(|| invalid("S3 view requires an S3 URI"))?;
                let (bucket, key) = uri
                    .split_once('/')
                    .ok_or_else(|| invalid("S3 view requires an exact data file"))?;
                if !(3..=63).contains(&bucket.len())
                    || bucket.starts_with(['-', '.'])
                    || bucket.ends_with(['-', '.'])
                    || !bucket.bytes().all(|b| {
                        b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'-' | b'.')
                    })
                    || key.is_empty()
                    || self.location.contains(['?', '#'])
                    || !self.location.is_ascii()
                    || self
                        .location
                        .bytes()
                        .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
                {
                    return Err(invalid("S3 view location must be credential-free"));
                }
                if self.location.contains(['*', '[', ']', '\\'])
                    || key
                        .split('/')
                        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
                {
                    return Err(invalid(
                        "S3 view location must identify one canonical file, without globs",
                    ));
                }
                // Core locators encode each path component separately. An
                // encoded delimiter or traversal component is not an alias for
                // another object. Decode once, preserving literal percent keys.
                let mut decoded = Vec::with_capacity(key.len());
                let mut bytes = key.bytes();
                while let Some(byte) = bytes.next() {
                    if byte == b'%' {
                        let high = bytes.next().and_then(|b| (b as char).to_digit(16));
                        let low = bytes.next().and_then(|b| (b as char).to_digit(16));
                        let byte = high
                            .zip(low)
                            .map(|(h, l)| (h * 16 + l) as u8)
                            .ok_or_else(|| invalid("invalid S3 path encoding"))?;
                        if byte == b'/' {
                            return Err(invalid("encoded S3 path delimiter"));
                        }
                        decoded.push(byte);
                    } else {
                        decoded.push(byte);
                    }
                }
                let decoded =
                    std::str::from_utf8(&decoded).map_err(|_| invalid("invalid S3 path UTF-8"))?;
                if decoded.contains(['\0', '\\'])
                    || decoded.split('/').any(|s| s == "." || s == "..")
                {
                    return Err(invalid("encoded S3 path traversal"));
                }
            }
            _ => {}
        }
        TableContract {
            columns: self.schema.columns.clone(),
            partition_keys: vec![],
            extensions: serde_json::json!({}),
            column_ext: serde_json::json!({}),
        }
        .validate()
    }
}

#[cfg(test)]
mod file_tests {
    use super::*;
    #[test]
    fn s3_verified_files_require_exact_credential_free_locations() {
        let mut file = VerifiedFile {
            table: Name::new("rows").unwrap(),
            partition: serde_json::json!({}),
            version: U64::new(1).unwrap(),
            schema: FileSchema {
                columns: vec![Column {
                    name: "id".into(),
                    logical_type: serde_json::json!("int64"),
                }],
            },
            access: FileAccess::S3View,
            location: "s3://bucket/prefix/datasets/data/rows/version=1/data.parquet".into(),
            size: U64::new(1024).unwrap(),
            sha256: Digest::new("a".repeat(64)).unwrap(),
            validator: "etag".into(),
        };
        file.validate().unwrap();
        for valid in [
            "s3://bucket/prefix%20with%20space/data.parquet",
            "s3://bucket/literal%252F/data.parquet",
            "s3://bucket/owner@example/data.parquet",
            "s3://bucket/emoji%F0%9F%8D%95/data.parquet",
        ] {
            file.location = valid.into();
            file.validate().unwrap();
        }
        for invalid in [
            "s3://bucket/rows/*.parquet",
            "s3://bucket/rows/part-[01].parquet",
            "s3://bucket/rows/../data.parquet",
            "s3://bucket//data.parquet",
            "s3://bucket/rows/",
            "s3://bucket/./data.parquet",
            "s3://bucket/rows\\data.parquet",
            "s3://user:credential@bucket/data.parquet",
            "s3://bucket/data.parquet?X-Amz-Signature=credential",
            "s3://bucket/data.parquet#fragment",
            "s3://bucket/prefix/%2E%2E/data.parquet",
            "s3://bucket/prefix%2Fforeign/data.parquet",
            "s3://bucket/prefix%00/data.parquet",
            "s3://bucket/prefix%5Cdata.parquet",
            "s3://bucket/prefix%FF/data.parquet",
            "s3://bucket/prefix%/data.parquet",
            "s3://bucket/raw space/data.parquet",
            "s3://bucket/raw\nnewline/data.parquet",
            "s3://bucket/raw🍕/data.parquet",
        ] {
            file.location = invalid.into();
            assert!(file.validate().is_err(), "{invalid}");
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestRecord {
    pub attempt_id: Uuid,
    pub root: String,
    pub dataset: Name,
    pub workspace_id: Uuid,
    pub adapter_identity: AdapterIdentity,
    pub connection_identity: String,
    pub effective_declaration: Value,
    /// Parent-normalized input to pure validation. Together with the effective
    /// declaration this identifies which missing members were defaulted.
    pub validation_input: Value,
    pub declaration_sha256: Digest,
    pub request_sha256: Digest,
    pub registry: Registry,
    pub requested_revision: RequestedRevision,
}
impl RequestRecord {
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.adapter_identity.validate()?;
        self.registry.validate()?;
        if self.root.is_empty()
            || self.connection_identity.is_empty()
            || !self.effective_declaration.is_object()
            || !self.validation_input.is_object()
            || ["kind", "dataset", "adapter"]
                .iter()
                .any(|key| self.validation_input.get(*key) != self.effective_declaration.get(*key))
            || self
                .effective_declaration
                .get("kind")
                .and_then(Value::as_str)
                != Some("pull")
            || self
                .effective_declaration
                .get("dataset")
                .and_then(Value::as_str)
                != Some(self.dataset.as_str())
            || self
                .effective_declaration
                .get("adapter")
                .and_then(Value::as_str)
                != Some(self.adapter_identity.name.as_str())
        {
            return Err(invalid("invalid fixed pull request"));
        }
        let digest = pull_request_digest(&PullRequestIdentity {
            root: self.root.clone(),
            workspace_id: self.workspace_id.clone(),
            declaration_sha256: self.declaration_sha256.clone(),
            requested_revision: self.requested_revision.clone(),
        })
        .map_err(|_| invalid("pull identity is not canonicalizable"))?;
        if digest != self.request_sha256 {
            return Err(invalid(
                "pull request digest differs from its fixed identity",
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableCount {
    pub table: Name,
    pub rows: U64,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamedContract {
    pub table: Name,
    pub contract: TableContract,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub request: RequestRecord,
    pub committed_revision: U64,
    pub generation_id: Uuid,
    pub pulled_at: Timestamp,
    pub row_counts: Vec<TableCount>,
    pub source_contracts: Vec<NamedContract>,
    pub output_contracts: Vec<NamedContract>,
    pub adapter_result: Value,
}
impl Receipt {
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.request.validate()?;
        let mut tables = BTreeSet::new();
        if self.row_counts.iter().any(|r| !tables.insert(&r.table)) {
            return Err(invalid("duplicate receipt table"));
        }
        for contracts in [&self.source_contracts, &self.output_contracts] {
            let mut names = BTreeSet::new();
            for record in contracts {
                record.contract.validate()?;
                if !names.insert(&record.table) {
                    return Err(invalid("duplicate receipt contract"));
                }
            }
            if names != tables {
                return Err(invalid("receipt contract coverage differs"));
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolvePhase {
    Lookup,
    Compare,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvePullRequest {
    pub phase: ResolvePhase,
    pub attempt_id: Uuid,
    pub root: String,
    #[serde(deserialize_with = "required_option")]
    pub request: Option<RequestRecord>,
}
impl ResolvePullRequest {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.root.is_empty() || (self.phase == ResolvePhase::Lookup) != self.request.is_none() {
            return Err(invalid("invalid pull resolution phase"));
        }
        if let Some(request) = &self.request {
            request.validate()?;
            if request.attempt_id != self.attempt_id || request.root != self.root {
                return Err(invalid("resolution request identity differs"));
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolutionState {
    Committed,
    NotCommitted,
    Busy,
    Unknown,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PullResolution {
    pub state: ResolutionState,
    #[serde(deserialize_with = "required_option")]
    pub request: Option<RequestRecord>,
    #[serde(deserialize_with = "required_option")]
    pub receipt: Option<Receipt>,
    #[serde(deserialize_with = "required_option")]
    pub recovery: Option<Value>,
    pub prior_source_contracts: Vec<NamedContract>,
}
impl PullResolution {
    pub fn validate(&self) -> Result<(), ValidationError> {
        let mut names = BTreeSet::new();
        for prior in &self.prior_source_contracts {
            prior.contract.validate()?;
            if !names.insert(&prior.table) {
                return Err(invalid("duplicate prior source contract"));
            }
        }
        if self.state != ResolutionState::NotCommitted && !self.prior_source_contracts.is_empty() {
            return Err(invalid(
                "prior source evidence requires not_committed resolution",
            ));
        }
        if let Some(request) = &self.request {
            request.validate()?;
        }
        if (self.state == ResolutionState::Committed) != self.receipt.is_some() {
            return Err(invalid("committed resolution requires exactly one receipt"));
        }
        if let Some(receipt) = &self.receipt {
            receipt.validate()?;
            if self.request.as_ref() != Some(&receipt.request) {
                return Err(invalid("resolution and receipt requests differ"));
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PullTable {
    pub name: Name,
    pub target: Value,
    #[serde(deserialize_with = "required_option")]
    pub select: Option<Value>,
    pub partitions: Vec<Value>,
    pub source_contract: TableContract,
    pub output_contract: TableContract,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Refresh {
    Changed,
    Full,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparePullRequest {
    pub request: RequestRecord,
    pub resolved_revision: U64,
    pub tables: Vec<PullTable>,
    pub files: Vec<VerifiedFile>,
    #[serde(deserialize_with = "required_option")]
    pub recovery: Option<Value>,
}
fn plan(
    request: &RequestRecord,
    revision: U64,
    tables: &[PullTable],
    files: &[VerifiedFile],
) -> Result<(), ValidationError> {
    request.validate()?;
    if let RequestedRevision::Revision(requested) = request.requested_revision
        && requested != revision
    {
        return Err(invalid("resolved revision differs from fixed selector"));
    }
    let mut names = BTreeSet::new();
    for table in tables {
        if !names.insert(&table.name) {
            return Err(invalid("duplicate pull table"));
        }
        table.source_contract.validate()?;
        table.output_contract.validate()?;
        for (i, tuple) in table.partitions.iter().enumerate() {
            partition(tuple, &table.source_contract)?;
            if table.partitions[..i].contains(tuple) {
                return Err(invalid("duplicate selected partition"));
            }
        }
    }
    let mut locations = BTreeSet::new();
    for file in files {
        file.validate()?;
        if !locations.insert(&file.location) {
            return Err(invalid("duplicate verified file location"));
        }
        let table = tables
            .iter()
            .find(|table| table.name == file.table)
            .ok_or_else(|| invalid("file has no selected table"))?;
        partition(&file.partition, &table.source_contract)?;
        if !table.partitions.contains(&file.partition)
            || file.schema.columns.len() > table.source_contract.columns.len()
            || table.source_contract.columns[..file.schema.columns.len()] != file.schema.columns
        {
            return Err(invalid(
                "file schema or partition differs from verified selection",
            ));
        }
    }
    Ok(())
}
impl PreparePullRequest {
    pub fn validate(&self) -> Result<(), ValidationError> {
        plan(
            &self.request,
            self.resolved_revision,
            &self.tables,
            &self.files,
        )
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PullPlan {
    pub plan_id: Uuid,
    pub request: RequestRecord,
    pub resolved_revision: U64,
    pub tables: Vec<PullTable>,
    pub files: Vec<VerifiedFile>,
    pub refresh: Refresh,
    pub recovery_contract: PullRecovery,
    pub adapter_details: Value,
}
impl PullPlan {
    pub fn validate(&self) -> Result<(), ValidationError> {
        plan(
            &self.request,
            self.resolved_revision,
            &self.tables,
            &self.files,
        )
    }
}

impl PullResolution {
    pub fn validate_for(&self, query: &ResolvePullRequest) -> Result<(), ValidationError> {
        self.validate()?;
        query.validate()?;
        if query.phase != ResolvePhase::Compare && !self.prior_source_contracts.is_empty() {
            return Err(invalid(
                "prior source evidence requires fixed-request comparison",
            ));
        }
        if let Some(fixed) = &query.request {
            for prior in &self.prior_source_contracts {
                if !fixed
                    .effective_declaration
                    .get("tables")
                    .and_then(Value::as_array)
                    .is_some_and(|tables| {
                        tables.iter().any(|table| {
                            table.get("name").and_then(Value::as_str) == Some(prior.table.as_str())
                        })
                    })
                {
                    return Err(invalid(
                        "prior contract is outside the fixed declared scope",
                    ));
                }
            }
        }
        if let Some(recorded) = &self.request
            && (recorded.attempt_id != query.attempt_id
                || recorded.root != query.root
                || query
                    .request
                    .as_ref()
                    .is_some_and(|fixed| fixed != recorded))
        {
            return Err(invalid("resolution changed the fixed request"));
        }
        Ok(())
    }
}
impl Receipt {
    pub fn validate_for(&self, plan: &PullPlan) -> Result<(), ValidationError> {
        self.validate()?;
        plan.validate()?;
        if self.request != plan.request
            || self.committed_revision != plan.resolved_revision
            || self.source_contracts.len() != plan.tables.len()
            || self.output_contracts.len() != plan.tables.len()
            || plan.tables.iter().any(|table| {
                self.source_contracts
                    .iter()
                    .find(|record| record.table == table.name)
                    .map(|record| &record.contract)
                    != Some(&table.source_contract)
                    || self
                        .output_contracts
                        .iter()
                        .find(|record| record.table == table.name)
                        .map(|record| &record.contract)
                        != Some(&table.output_contract)
            })
        {
            return Err(invalid("receipt differs from the applied verified plan"));
        }
        Ok(())
    }
}
