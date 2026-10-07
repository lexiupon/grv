//! Exact v2 persisted schemas. Storage counters are numeric JSON int64 values,
//! distinct from the decimal-string counters of the process/public contracts.
use crate::{Error, ErrorKind, Result, Validator};
use grv_types::{Digest, Name, RunId, Timestamp, Uuid};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct Counter(u64);
impl Counter {
    pub fn new(v: u64) -> Result<Self> {
        if v <= i64::MAX as u64 {
            Ok(Self(v))
        } else {
            Err(invalid("counter exceeds int64"))
        }
    }
    pub fn get(self) -> u64 {
        self.0
    }
    pub fn next(self) -> Result<Self> {
        self.0
            .checked_add(1)
            .ok_or_else(|| invalid("counter exhausted"))
            .and_then(Self::new)
    }
}
impl<'de> Deserialize<'de> for Counter {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        Self::new(u64::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}
impl fmt::Display for Counter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl From<u32> for Counter {
    fn from(v: u32) -> Self {
        Self(v as u64)
    }
}
macro_rules! token {
    ($name:ident) => {
        #[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);
        impl $name {
            pub fn generate() -> Self {
                Self(Uuid::v4())
            }
        }
        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(concat!(stringify!($name), "([redacted])"))
            }
        }
    };
}
token!(OwnerToken);
token!(ClaimToken);
token!(LeaseToken);
impl ClaimToken {
    /// A token's only public textual projection is its prescribed allocation
    /// object key; adapters never import this storage crate.
    pub fn allocation_key(&self, dataset: &Name, run: &RunId) -> Result<crate::ObjectKey> {
        crate::ObjectKey::new(format!(
            "datasets/{dataset}/.runs/{run}.allocations/{}.json",
            self.0
        ))
    }
}
pub type MutationId = Uuid;
pub type Partition = BTreeMap<Name, Name>;
pub trait Validate {
    fn validate(&self) -> Result<()>;
}
fn invalid(message: &str) -> Error {
    Error::new(ErrorKind::InvalidRecord, message)
}
fn require(condition: bool, message: &str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(invalid(message))
    }
}
fn nonempty(s: &str) -> Result<()> {
    require(!s.is_empty(), "required string is empty")
}
fn positive(v: Counter) -> Result<()> {
    require(v.get() > 0, "counter must be positive")
}
fn partition(p: &Partition) -> Result<()> {
    require(
        !p.keys()
            .any(|k| matches!(k.as_str(), "version" | "revision")),
        "reserved partition key",
    )
}
fn ordered(start: &Timestamp, end: &Timestamp) -> Result<()> {
    require(
        chrono::DateTime::parse_from_rfc3339(start.as_str()).unwrap()
            <= chrono::DateTime::parse_from_rfc3339(end.as_str()).unwrap(),
        "timestamps are reversed",
    )
}
fn unique<T: Ord>(values: impl IntoIterator<Item = T>) -> bool {
    let mut set = BTreeSet::new();
    values.into_iter().all(|v| set.insert(v))
}
fn required_option<'de, D, T>(d: D) -> std::result::Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(d)
}
fn optional_non_null<'de, D, T>(d: D) -> std::result::Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(d).map(Some)
}
pub fn encode_record<T: Validate + Serialize>(record: &T) -> Result<Vec<u8>> {
    record.validate()?;
    serde_json::to_vec(record).map_err(|_| invalid("record serialization failed"))
}
pub fn decode_record<T: Validate + for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T> {
    // Reject duplicate keys recursively before typed deserialization. Storage
    // records retain integer precision and do not pass through JCS/IEEE754.
    let value = crate::json::parse(bytes)
        .map_err(|e| Error::new(ErrorKind::InvalidRecord, e.to_string()))?;
    let record: T = serde_json::from_value(value)
        .map_err(|e| Error::new(ErrorKind::InvalidRecord, e.to_string()))?;
    record.validate()?;
    Ok(record)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreParameters {
    pub format: String,
    pub format_version: u32,
    pub mutation_id: MutationId,
    pub max_clock_skew_seconds: Counter,
    pub max_lease_ttl_seconds: Counter,
    pub pending_grace_seconds: Counter,
}
impl Default for StoreParameters {
    fn default() -> Self {
        Self {
            format: "grv".into(),
            format_version: 2,
            mutation_id: Uuid::v4(),
            max_clock_skew_seconds: 30.into(),
            max_lease_ttl_seconds: 900.into(),
            pending_grace_seconds: 604800.into(),
        }
    }
}
impl Validate for StoreParameters {
    fn validate(&self) -> Result<()> {
        require(
            self.format == "grv" && self.format_version == 2,
            "unsupported store format",
        )?;
        positive(self.max_lease_ttl_seconds)
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableLayout {
    pub table: Name,
    pub partition_keys: Vec<Name>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub extensions: Option<BTreeMap<String, Value>>,
}
impl Validate for TableLayout {
    fn validate(&self) -> Result<()> {
        require(
            unique(self.partition_keys.iter())
                && !self
                    .partition_keys
                    .iter()
                    .any(|k| matches!(k.as_str(), "version" | "revision")),
            "invalid partition keys",
        )?;
        if let Some(ext) = &self.extensions {
            for key in ext.keys() {
                nonempty(key)?;
            }
        }
        Ok(())
    }
}
impl TableLayout {
    pub fn partition_path(&self, p: &Partition) -> Result<String> {
        self.validate()?;
        partition(p)?;
        require(
            p.len() == self.partition_keys.len()
                && self.partition_keys.iter().all(|k| p.contains_key(k)),
            "partition does not match layout",
        )?;
        Ok(self
            .partition_keys
            .iter()
            .map(|k| format!("{k}={}", p[k]))
            .collect::<Vec<_>>()
            .join("/"))
    }
    pub fn parse_partition(&self, path: &str) -> Result<Partition> {
        let mut values = BTreeMap::new();
        if !path.is_empty() {
            for segment in path.split('/') {
                let (k, v) = segment
                    .split_once('=')
                    .ok_or_else(|| invalid("noncanonical partition path"))?;
                let key = Name::new(k).map_err(|_| invalid("invalid partition key"))?;
                let value = Name::new(v).map_err(|_| invalid("invalid partition value"))?;
                require(
                    values.insert(key, value).is_none(),
                    "duplicate partition key",
                )?;
            }
        }
        require(
            self.partition_path(&values)? == path,
            "partition path order differs from layout",
        )?;
        Ok(values)
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub logical_type: Value,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub ext: Option<BTreeMap<String, Value>>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaBaseline {
    pub table: Name,
    pub mutation_id: MutationId,
    pub columns: Vec<StorageColumn>,
}
impl Validate for SchemaBaseline {
    fn validate(&self) -> Result<()> {
        require(
            unique(self.columns.iter().map(|c| &c.name)),
            "duplicate schema column",
        )?;
        for c in &self.columns {
            logical_type(&c.logical_type)?;
        }
        Ok(())
    }
}
fn logical_type(value: &Value) -> Result<()> {
    if let Some(s) = value.as_str() {
        return require(
            [
                "boolean", "int8", "int16", "int32", "int64", "uint8", "uint16", "uint32",
                "uint64", "float32", "float64", "string", "json", "binary", "uuid", "date",
            ]
            .contains(&s),
            "unknown GRV logical type",
        );
    }
    let o = value
        .as_object()
        .ok_or_else(|| invalid("invalid logical type"))?;
    require(
        o.len() == 1,
        "logical type must contain exactly one constructor",
    )?;
    let (name, args) = o.iter().next().unwrap();
    let a = args
        .as_object()
        .ok_or_else(|| invalid("invalid logical type constructor"))?;
    match name.as_str() {
        "fixed_binary" => require(
            a.len() == 1
                && a.get("length")
                    .and_then(Value::as_u64)
                    .is_some_and(|v| v > 0 && v <= i32::MAX as u64),
            "invalid fixed binary",
        ),
        "decimal" => require(
            a.len() == 2
                && a.get("precision").and_then(Value::as_u64).is_some_and(|p| {
                    p > 0
                        && p <= i32::MAX as u64
                        && a.get("scale")
                            .and_then(Value::as_u64)
                            .is_some_and(|s| s <= p)
                }),
            "invalid decimal",
        ),
        "time" | "timestamp" => require(
            a.len() == 2
                && a.get("unit")
                    .and_then(Value::as_str)
                    .is_some_and(|u| ["ms", "us", "ns"].contains(&u))
                && a.get("utc").and_then(Value::as_bool).is_some(),
            "invalid temporal type",
        ),
        "list" => {
            require(a.len() == 1, "invalid list type")?;
            logical_type(
                a.get("element")
                    .ok_or_else(|| invalid("missing list element"))?,
            )
        }
        "map" => {
            require(a.len() == 2, "invalid map type")?;
            logical_type(a.get("key").ok_or_else(|| invalid("missing map key"))?)?;
            logical_type(a.get("value").ok_or_else(|| invalid("missing map value"))?)
        }
        "struct" => {
            require(a.len() == 1, "invalid struct type")?;
            let fields = a
                .get("fields")
                .and_then(Value::as_array)
                .ok_or_else(|| invalid("missing struct fields"))?;
            let mut names = BTreeSet::new();
            for field in fields {
                let f = field
                    .as_object()
                    .ok_or_else(|| invalid("invalid struct field"))?;
                require(f.len() == 2, "invalid struct field members")?;
                let name = f
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| invalid("missing struct field name"))?;
                require(names.insert(name), "duplicate struct field")?;
                logical_type(
                    f.get("type")
                        .ok_or_else(|| invalid("missing struct field type"))?,
                )?;
            }
            Ok(())
        }
        _ => Err(invalid("unknown logical constructor")),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimOutcome {
    Finalized,
    Abandoned,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LastRelease {
    pub token: ClaimToken,
    pub version: Counter,
    pub outcome: ClaimOutcome,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimRecord {
    pub holder: RunId,
    pub token: ClaimToken,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub version: Option<Counter>,
    pub high_water: Counter,
    pub mutation_id: MutationId,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub claimed_at: Option<Timestamp>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub expires_at: Option<Timestamp>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub released_at: Option<Timestamp>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub outcome: Option<ClaimOutcome>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub last_release: Option<LastRelease>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimPhase {
    Acquired,
    Allocated,
    Released,
}
impl ClaimRecord {
    pub fn phase(&self) -> Result<ClaimPhase> {
        self.validate()?;
        Ok(if self.released_at.is_some() {
            ClaimPhase::Released
        } else if self.version.is_some() {
            ClaimPhase::Allocated
        } else {
            ClaimPhase::Acquired
        })
    }
}
impl Validate for ClaimRecord {
    fn validate(&self) -> Result<()> {
        if let Some(v) = self.version {
            positive(v)?;
            require(
                v == self.high_water,
                "allocated claim does not equal its reservation high water",
            )?;
        }
        if let Some(v) = &self.last_release {
            positive(v.version)?;
            require(
                v.version <= self.high_water,
                "last release exceeds high water",
            )?;
            require(
                v.token != self.token,
                "last release reuses the current claim token",
            )?;
            if let Some(current) = self.version {
                require(
                    v.version < current,
                    "last release is not an earlier reservation",
                )?;
            }
        }
        if self.released_at.is_some() {
            require(
                self.outcome.is_some() && self.claimed_at.is_none() && self.expires_at.is_none(),
                "invalid released claim",
            )?;
            if self.outcome == Some(ClaimOutcome::Finalized) {
                require(self.version.is_some(), "unallocated claim cannot finalize")?;
            }
        } else {
            require(
                self.outcome.is_none() && self.claimed_at.is_some() && self.expires_at.is_some(),
                "invalid live claim",
            )?;
            ordered(
                self.claimed_at.as_ref().unwrap(),
                self.expires_at.as_ref().unwrap(),
            )?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunInput {
    pub dataset: Name,
    pub revision: Counter,
    pub retention_id: Uuid,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunEntry {
    pub table: Name,
    pub partition: Partition,
    pub version: Counter,
    pub claim_token: ClaimToken,
}
fn inputs(values: &[RunInput]) -> Result<()> {
    require(
        unique(values.iter().map(|i| (&i.dataset, i.revision))),
        "duplicate run input",
    )?;
    require(
        unique(values.iter().map(|i| &i.retention_id)),
        "reused hold identity",
    )?;
    for i in values {
        positive(i.revision)?;
    }
    Ok(())
}
fn entries(values: &[RunEntry]) -> Result<()> {
    require(
        unique(values.iter().map(|e| (&e.table, &e.partition, e.version))),
        "duplicate run entry",
    )?;
    require(
        unique(values.iter().map(|entry| &entry.claim_token.0)),
        "claim token reused across run entries",
    )?;
    for e in values {
        positive(e.version)?;
        partition(&e.partition)?;
    }
    Ok(())
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunPhase {
    Open,
    Recovering,
    Sealed,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunControl {
    pub run_id: RunId,
    pub created_at: Timestamp,
    pub base_revision: Counter,
    pub inputs: Vec<RunInput>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub metadata: Option<BTreeMap<String, Value>>,
    pub phase: RunPhase,
    pub owner_token: OwnerToken,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub expires_at: Option<Timestamp>,
    pub mutation_id: MutationId,
    pub holds_confirmed: bool,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub sealed_at: Option<Timestamp>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub entries: Option<Vec<RunEntry>>,
}
impl Validate for RunControl {
    fn validate(&self) -> Result<()> {
        inputs(&self.inputs)?;
        if self.phase == RunPhase::Sealed {
            require(
                self.expires_at.is_none() && self.sealed_at.is_some() && self.entries.is_some(),
                "invalid sealed control",
            )?;
            ordered(&self.created_at, self.sealed_at.as_ref().unwrap())?;
            entries(self.entries.as_ref().unwrap())?;
            require(
                self.holds_confirmed || self.entries.as_ref().unwrap().is_empty(),
                "unconfirmed holds cannot seal entries",
            )?;
        } else {
            require(
                self.expires_at.is_some() && self.sealed_at.is_none() && self.entries.is_none(),
                "invalid live run control",
            )?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealedRun {
    pub run_id: RunId,
    pub created_at: Timestamp,
    pub base_revision: Counter,
    pub inputs: Vec<RunInput>,
    pub holds_confirmed: bool,
    pub sealed_at: Timestamp,
    pub entries: Vec<RunEntry>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub metadata: Option<BTreeMap<String, Value>>,
}
impl Validate for SealedRun {
    fn validate(&self) -> Result<()> {
        inputs(&self.inputs)?;
        entries(&self.entries)?;
        ordered(&self.created_at, &self.sealed_at)?;
        require(
            self.holds_confirmed || self.entries.is_empty(),
            "unconfirmed holds cannot seal entries",
        )
    }
}
impl RunControl {
    pub fn sealed_run(&self) -> Result<SealedRun> {
        self.validate()?;
        require(self.phase == RunPhase::Sealed, "run is not sealed")?;
        Ok(SealedRun {
            run_id: self.run_id.clone(),
            created_at: self.created_at.clone(),
            base_revision: self.base_revision,
            inputs: self.inputs.clone(),
            holds_confirmed: self.holds_confirmed,
            sealed_at: self.sealed_at.clone().unwrap(),
            entries: self.entries.clone().unwrap(),
            metadata: self.metadata.clone(),
        })
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AllocationState {
    Allocated,
    Finalized,
    Abandoned,
    Unproven,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllocationRecord {
    pub run_id: RunId,
    pub table: Name,
    pub partition: Partition,
    pub version: Counter,
    pub claim_token: ClaimToken,
    pub state: AllocationState,
    pub mutation_id: MutationId,
}
impl Validate for AllocationRecord {
    fn validate(&self) -> Result<()> {
        positive(self.version)?;
        partition(&self.partition)
    }
}
impl AllocationRecord {
    /// A matching current release or the carried prior release is the only
    /// claim-based proof of a terminal allocation outcome. A live claim or
    /// manifest alone supplies no finalization authority.
    pub fn release_proof(&self, claim: &ClaimRecord) -> Result<Option<ClaimOutcome>> {
        self.validate()?;
        claim.validate()?;
        if claim.token == self.claim_token
            && claim.version == Some(self.version)
            && claim.holder == self.run_id
            && claim.released_at.is_some()
        {
            return Ok(claim.outcome);
        }
        Ok(claim
            .last_release
            .as_ref()
            .filter(|last| last.token == self.claim_token && last.version == self.version)
            .map(|last| last.outcome))
    }
}
impl SealedRun {
    pub fn validate_control(&self, control: &RunControl) -> Result<()> {
        self.validate()?;
        require(
            *self == control.sealed_run()?,
            "immutable run differs from sealed control",
        )
    }
    /// Checks persisted ownership/provenance fences; data verification, active
    /// holds, pruning and schema prefix checks remain consumer responsibilities.
    pub fn validate_manifest(&self, manifest: &VersionManifest) -> Result<()> {
        self.validate()?;
        manifest.validate()?;
        require(
            self.run_id == manifest.run_id && self.holds_confirmed,
            "manifest has no confirmed sealed run",
        )?;
        require(
            self.entries.iter().any(|entry| {
                entry.table == manifest.table
                    && entry.partition == manifest.partition
                    && entry.version == manifest.version
                    && entry.claim_token == manifest.claim_token
            }),
            "manifest is absent from authoritative sealed entries",
        )?;
        for source in manifest.derived_from.iter().flatten() {
            require(
                self.inputs.iter().any(|input| {
                    input.dataset == source.dataset
                        && input.revision == source.revision
                        && input.retention_id == source.retention_id
                }),
                "manifest provenance is not a run input",
            )?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataFile {
    pub name: String,
    pub sha256: Digest,
    pub size: Counter,
    pub validator: Validator,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceReference {
    pub dataset: Name,
    pub revision: Counter,
    pub retention_id: Uuid,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub table: Option<Name>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub partition: Option<Partition>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionManifest {
    pub table: Name,
    pub partition: Partition,
    pub version: Counter,
    pub run_id: RunId,
    pub created_at: Timestamp,
    pub claim_token: ClaimToken,
    pub data_files: Vec<DataFile>,
    pub row_count: Counter,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub derived_from: Option<Vec<SourceReference>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub metadata: Option<BTreeMap<String, Value>>,
}
impl Validate for VersionManifest {
    fn validate(&self) -> Result<()> {
        positive(self.version)?;
        partition(&self.partition)?;
        require(
            !self.data_files.is_empty(),
            "manifest requires data.parquet",
        )?;
        for (i, f) in self.data_files.iter().enumerate() {
            require(
                f.name
                    == if i == 0 {
                        "data.parquet".into()
                    } else {
                        format!("data-{i}.parquet")
                    },
                "data files must use contiguous numeric order",
            )?;
            positive(f.size)?;
        }
        if let Some(refs) = &self.derived_from {
            require(!refs.is_empty(), "empty derivation must be absent")?;
            require(
                unique(refs.iter().map(|r| {
                    (
                        &r.dataset,
                        r.revision,
                        &r.retention_id,
                        &r.table,
                        &r.partition,
                    )
                })),
                "duplicate source reference",
            )?;
            for r in refs {
                positive(r.revision)?;
                require(
                    r.partition.is_none() || r.table.is_some(),
                    "source partition needs table",
                )?;
                if let Some(p) = &r.partition {
                    partition(p)?;
                }
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lease {
    pub holder: String,
    pub token: LeaseToken,
    pub claimed_at: Timestamp,
    pub expires_at: Timestamp,
}
impl Validate for Lease {
    fn validate(&self) -> Result<()> {
        nonempty(&self.holder)?;
        ordered(&self.claimed_at, &self.expires_at)
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Latest {
    pub revision: Counter,
    pub high_water: Counter,
    pub mutation_id: MutationId,
    #[serde(deserialize_with = "required_option")]
    pub lease: Option<Lease>,
    #[serde(deserialize_with = "required_option")]
    pub pending: Option<String>,
}
impl Latest {
    pub fn empty() -> Self {
        Self {
            revision: 0.into(),
            high_water: 0.into(),
            mutation_id: Uuid::v4(),
            lease: None,
            pending: None,
        }
    }
}
impl Validate for Latest {
    fn validate(&self) -> Result<()> {
        require(
            self.revision <= self.high_water,
            "LATEST revision exceeds high water",
        )?;
        if let Some(l) = &self.lease {
            l.validate()?;
        }
        if let Some(p) = &self.pending {
            let id = p
                .strip_prefix(".states/operations/")
                .and_then(|v| v.strip_suffix(".json"))
                .ok_or_else(|| invalid("invalid pending operation path"))?;
            RunId::new(id).map_err(|_| invalid("pending operation id is invalid"))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Omission {
    pub table: Name,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub partition: Option<Partition>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Selection {
    pub table: Name,
    pub partition: Partition,
    pub version: Counter,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ChangeSet {
    #[serde(default)]
    pub runs: Vec<RunId>,
    #[serde(default)]
    pub omissions: Vec<Omission>,
    #[serde(default)]
    pub selections: Vec<Selection>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub expected_revision: Option<Counter>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub reason: Option<String>,
}
impl Validate for ChangeSet {
    fn validate(&self) -> Result<()> {
        require(unique(self.runs.iter()), "duplicate run")?;
        require(
            unique(self.omissions.iter().map(|o| (&o.table, &o.partition))),
            "duplicate omission",
        )?;
        require(
            unique(self.selections.iter().map(|o| (&o.table, &o.partition))),
            "duplicate selection",
        )?;
        for o in &self.omissions {
            if let Some(p) = &o.partition {
                partition(p)?;
            }
        }
        for s in &self.selections {
            partition(&s.partition)?;
            positive(s.version)?;
        }
        for o in &self.omissions {
            require(
                !self.selections.iter().any(|s| {
                    s.table == o.table
                        && (o.partition.is_none() || o.partition.as_ref() == Some(&s.partition))
                }),
                "selection overlaps omission",
            )?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishPayload {
    pub revision: Counter,
    pub previous_revision: Counter,
    pub change_set: ChangeSet,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HoldRelease {
    pub consumer_dataset: Name,
    pub revision: Counter,
    pub retention_id: Uuid,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseHoldPayload {
    pub releases: Vec<HoldRelease>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionTarget {
    pub table: Name,
    pub partition: Partition,
    pub version: Counter,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrunePayload {
    pub targets: Vec<VersionTarget>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PinScope {
    Revision(RevisionScope),
    Version(VersionScope),
    Partition(PartitionScope),
    Table(TableScope),
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RevisionScope {
    pub revision: Counter,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionScope {
    pub table: Name,
    pub partition: Partition,
    pub version: Counter,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionScope {
    pub table: Name,
    pub partition: Partition,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableScope {
    pub table: Name,
}
impl Validate for PinScope {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Revision(v) => positive(v.revision),
            Self::Version(v) => {
                partition(&v.partition)?;
                positive(v.version)
            }
            Self::Partition(v) => partition(&v.partition),
            Self::Table(_) => Ok(()),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinPayload {
    pub pin_id: Uuid,
    pub scope: PinScope,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub reason: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetirePayload {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub reason: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
pub enum OperationPayload {
    Publish(PublishPayload),
    ReleaseHold(ReleaseHoldPayload),
    Pin(PinPayload),
    Unpin(PinPayload),
    Retire(RetirePayload),
    PruneIntent(PrunePayload),
    Prune(PrunePayload),
}
#[derive(Debug, Clone, PartialEq)]
pub struct OperationRecord {
    pub operation_id: RunId,
    pub dataset: Name,
    pub created_at: Timestamp,
    pub created_by: String,
    pub body: OperationPayload,
}
// Flatten is intentionally confined to this conversion helper: serde's
// deny_unknown_fields cannot reliably close a flatten-tagged outer record.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawOperation {
    operation_id: RunId,
    dataset: Name,
    kind: String,
    created_at: Timestamp,
    created_by: String,
    payload: Value,
}
impl Serialize for OperationRecord {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        let body = serde_json::to_value(&self.body).map_err(serde::ser::Error::custom)?;
        RawOperation {
            operation_id: self.operation_id.clone(),
            dataset: self.dataset.clone(),
            kind: body["kind"].as_str().unwrap().into(),
            created_at: self.created_at.clone(),
            created_by: self.created_by.clone(),
            payload: body["payload"].clone(),
        }
        .serialize(s)
    }
}
impl<'de> Deserialize<'de> for OperationRecord {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let raw = RawOperation::deserialize(d)?;
        let body =
            serde_json::from_value(serde_json::json!({"kind":raw.kind,"payload":raw.payload}))
                .map_err(serde::de::Error::custom)?;
        Ok(Self {
            operation_id: raw.operation_id,
            dataset: raw.dataset,
            created_at: raw.created_at,
            created_by: raw.created_by,
            body,
        })
    }
}
impl Validate for OperationRecord {
    fn validate(&self) -> Result<()> {
        nonempty(&self.created_by)?;
        match &self.body {
            OperationPayload::Publish(p) => {
                positive(p.revision)?;
                require(
                    p.revision > p.previous_revision,
                    "publication revision must advance",
                )?;
                p.change_set.validate()
            }
            OperationPayload::ReleaseHold(p) => {
                require(
                    unique(
                        p.releases
                            .iter()
                            .map(|r| (&r.consumer_dataset, r.revision, &r.retention_id)),
                    ),
                    "duplicate hold release",
                )?;
                for r in &p.releases {
                    positive(r.revision)?;
                }
                Ok(())
            }
            OperationPayload::Pin(p) | OperationPayload::Unpin(p) => p.scope.validate(),
            OperationPayload::Retire(_) => Ok(()),
            OperationPayload::PruneIntent(p) | OperationPayload::Prune(p) => {
                require(
                    unique(
                        p.targets
                            .iter()
                            .map(|t| (&t.table, &t.partition, t.version)),
                    ),
                    "duplicate prune target",
                )?;
                for t in &p.targets {
                    partition(&t.partition)?;
                    positive(t.version)?;
                }
                Ok(())
            }
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HoldRecord {
    pub retention_id: Uuid,
    pub dataset: Name,
    pub revision: Counter,
    pub target_dataset: Name,
    pub target_run_id: RunId,
    pub created_at: Timestamp,
}
impl Validate for HoldRecord {
    fn validate(&self) -> Result<()> {
        positive(self.revision)?;
        require(
            self.dataset != self.target_dataset,
            "dataset cannot hold itself",
        )
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HoldReleaseMarker {
    pub retention_id: Uuid,
    pub operation_id: RunId,
    pub released_at: Timestamp,
}
impl Validate for HoldReleaseMarker {
    fn validate(&self) -> Result<()> {
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupersessionReceipt {
    pub operation_id: RunId,
    pub successor: Counter,
    pub observed_at: Timestamp,
}
impl Validate for SupersessionReceipt {
    fn validate(&self) -> Result<()> {
        positive(self.successor)
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrunedMarker {
    pub operation_id: RunId,
    pub pruned_by: String,
    pub pruned_at: Timestamp,
    pub table: Name,
    pub partition: Partition,
    pub version: Counter,
}
impl Validate for PrunedMarker {
    fn validate(&self) -> Result<()> {
        nonempty(&self.pruned_by)?;
        positive(self.version)?;
        partition(&self.partition)
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetiredMarker {
    pub operation_id: RunId,
    pub retired_at: Timestamp,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub reason: Option<String>,
}
impl Validate for RetiredMarker {
    fn validate(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinRecord {
    pub pin_id: Uuid,
    pub operation_id: RunId,
    pub scope: PinScope,
    pub created_at: Timestamp,
    pub created_by: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_non_null"
    )]
    pub reason: Option<String>,
}
impl Validate for PinRecord {
    fn validate(&self) -> Result<()> {
        nonempty(&self.created_by)?;
        self.scope.validate()
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinReleaseMarker {
    pub pin_id: Uuid,
    pub operation_id: RunId,
    pub released_at: Timestamp,
}
impl Validate for PinReleaseMarker {
    fn validate(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn storage_int64_json_is_exact_and_exhaustion_refuses() {
        let value = Counter::new(i64::MAX as u64).unwrap();
        let bytes = serde_json::to_vec(&value).unwrap();
        assert_eq!(bytes, b"9223372036854775807");
        assert_eq!(serde_json::from_slice::<Counter>(&bytes).unwrap(), value);
        assert!(value.next().is_err());
        assert!(serde_json::from_str::<Counter>("9223372036854775808").is_err());
        assert!(serde_json::from_str::<Counter>("\"1\"").is_err());
    }
    #[test]
    fn closed_records_reject_duplicate_keys_and_missing_required_nulls() {
        assert!(decode_record::<Latest>(br#"{"revision":0,"high_water":0,"mutation_id":"359c6d0f-a9c1-4ae6-b804-0742a5e2b9de","lease":null}"#).is_err());
        assert!(decode_record::<StoreParameters>(br#"{"format":"grv","format":"other","format_version":2,"mutation_id":"359c6d0f-a9c1-4ae6-b804-0742a5e2b9de","max_clock_skew_seconds":30,"max_lease_ttl_seconds":900,"pending_grace_seconds":604800}"#).is_err());
        assert!(
            decode_record::<TableLayout>(
                br#"{"table":"table","partition_keys":[],"extensions":null}"#
            )
            .is_err()
        );
        assert!(decode_record::<Latest>(&encode_record(&Latest::empty()).unwrap()).is_ok());
    }
    #[test]
    fn partition_representations_match_declared_order() {
        let layout = TableLayout {
            table: Name::new("table").unwrap(),
            partition_keys: vec![Name::new("year").unwrap(), Name::new("region").unwrap()],
            extensions: None,
        };
        let p = layout.parse_partition("year=2025/region=eu").unwrap();
        assert_eq!(layout.partition_path(&p).unwrap(), "year=2025/region=eu");
        assert!(layout.parse_partition("region=eu/year=2025").is_err());
        assert!(layout.parse_partition("year=2025").is_err());
    }
    #[test]
    fn ownership_tokens_are_redacted_and_not_wire_scalar_types() {
        let t = OwnerToken::generate();
        assert_eq!(format!("{t:?}"), "OwnerToken([redacted])");
        assert!(serde_json::to_string(&t).unwrap().len() > 30);
    }
    #[test]
    fn retirement_marker_matches_the_v2_record_without_an_invented_actor_field() {
        let bytes = br#"{"operation_id":"01M3KQA080R6Y8C2D9F0G1H2J3","retired_at":"2026-10-06T00:00:00Z","reason":"obsolete"}"#;
        let marker: RetiredMarker = decode_record(bytes).unwrap();
        let value: Value = serde_json::from_slice(&encode_record(&marker).unwrap()).unwrap();
        assert_eq!(value.as_object().unwrap().len(), 3);
        let mut forbidden = value;
        forbidden["retired_by"] = Value::String("engine".into());
        assert!(decode_record::<RetiredMarker>(&serde_json::to_vec(&forbidden).unwrap()).is_err());
    }
    #[test]
    fn release_proof_requires_token_version_and_finalization_evidence() {
        let run = RunId::new("01M3KQA080R6Y8C2D9F0G1H2J3").unwrap();
        let token = ClaimToken::generate();
        let record = AllocationRecord {
            run_id: run.clone(),
            table: Name::new("table").unwrap(),
            partition: Partition::new(),
            version: Counter::from(1),
            claim_token: token.clone(),
            state: AllocationState::Allocated,
            mutation_id: Uuid::v4(),
        };
        let mut claim = ClaimRecord {
            holder: run,
            token,
            version: Some(Counter::from(1)),
            high_water: Counter::from(1),
            mutation_id: Uuid::v4(),
            claimed_at: Some(Timestamp::new("2026-10-06T00:00:00Z").unwrap()),
            expires_at: Some(Timestamp::new("2026-10-06T00:01:00Z").unwrap()),
            released_at: None,
            outcome: None,
            last_release: None,
        };
        assert_eq!(record.release_proof(&claim).unwrap(), None);
        claim.claimed_at = None;
        claim.expires_at = None;
        claim.released_at = Some(Timestamp::new("2026-10-06T00:00:30Z").unwrap());
        claim.outcome = Some(ClaimOutcome::Finalized);
        assert_eq!(
            record.release_proof(&claim).unwrap(),
            Some(ClaimOutcome::Finalized)
        );
        claim.last_release = Some(LastRelease {
            token: claim.token.clone(),
            version: Counter::from(1),
            outcome: ClaimOutcome::Finalized,
        });
        claim.token = ClaimToken::generate();
        claim.version = None;
        claim.released_at = None;
        claim.outcome = None;
        claim.claimed_at = Some(Timestamp::new("2026-10-06T00:00:30Z").unwrap());
        claim.expires_at = Some(Timestamp::new("2026-10-06T00:01:30Z").unwrap());
        assert_eq!(
            record.release_proof(&claim).unwrap(),
            Some(ClaimOutcome::Finalized)
        );
        claim.last_release.as_mut().unwrap().version = Counter::from(2);
        assert!(record.release_proof(&claim).is_err());
    }
}
