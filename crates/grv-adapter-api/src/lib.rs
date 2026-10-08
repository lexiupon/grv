//! Closed process adapter v1 records. No core/storage authority belongs here.
pub use grv_types::{
    AdapterIdentity, Column, Digest, ErrorCode, Handle, Name, ObjectIdentity, Req, RunId, SafeInt,
    TableContract, Timestamp, U64, Uuid, validate_logical_type,
};
use grv_types::{SourceConsistency, ValidationError};
use serde::{Deserialize, Deserializer, Serialize};

fn required_option<'de, D, T>(d: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(d)
}
use serde_json::Value;
mod pull;
pub use pull::*;
mod build;
pub use build::*;

/// Shared confirmed GRV outcome; its wire shape is identical for builds/hooks.
pub type Outcome = BuildOutcome;

/// Fixed private attempt identity for an idempotent post-publication hook.
/// This grants no authority to acquire source data or publish again.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AfterPublishRequest {
    pub attempt_id: Uuid,
    pub declaration_sha256: Digest,
    pub outcome: Outcome,
}
impl AfterPublishRequest {
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.outcome.validate()
    }
}

/// Read-only engine observation. A declaration scopes existing local facts;
/// it never authorizes source execution, repair, authentication, or renewal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectConnectionRequest {
    #[serde(deserialize_with = "required_option")]
    pub root: Option<String>,
    #[serde(deserialize_with = "required_option")]
    pub declaration: Option<Value>,
}
impl InspectConnectionRequest {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.root.as_ref().is_some_and(|root| root.is_empty())
            || self
                .declaration
                .as_ref()
                .is_some_and(|value| !value.is_object())
        {
            return Err(ValidationError(
                "invalid connection inspection scope".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Doc<T> {
    Inline(Box<InlineDocument<T>>),
    Reference(DocumentReference),
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InlineDocument<T> {
    pub inline: T,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DocumentReference {
    pub document_id: Uuid,
}
impl<T> Doc<T> {
    pub fn inline(value: T) -> Self {
        Self::Inline(Box::new(InlineDocument { inline: value }))
    }
    pub fn reference(document_id: Uuid) -> Self {
        Self::Reference(DocumentReference { document_id })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resources {
    pub slots: Req,
    pub max_batch_bytes: U64,
    pub max_source_unit_bytes: U64,
    pub max_scratch_bytes: U64,
}
impl Default for Resources {
    fn default() -> Self {
        Self {
            slots: Req::new(4).unwrap(),
            max_batch_bytes: U64::new(8 * 1024 * 1024).unwrap(),
            max_source_unit_bytes: U64::new(32 * 1024 * 1024).unwrap(),
            max_scratch_bytes: U64::new(32 * 1024 * 1024).unwrap(),
        }
    }
}
impl Resources {
    pub fn validate(&self) -> Result<(), ValidationError> {
        let batch = self.max_batch_bytes.get();
        if self.slots.get() > 64
            || !(262144..=67108864).contains(&batch)
            || !batch.is_multiple_of(8)
            || self.max_source_unit_bytes.get() == 0
            || self.max_source_unit_bytes.get() > 67108864
            || self.max_scratch_bytes.get() == 0
            || self.max_scratch_bytes.get() > 67108864
        {
            Err(ValidationError("invalid resource budgets".into()))
        } else {
            Ok(())
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireConsistency {
    Snapshot,
    CaptureWindow,
    None,
}
impl From<WireConsistency> for SourceConsistency {
    fn from(value: WireConsistency) -> Self {
        match value {
            WireConsistency::Snapshot => Self::TransactionSnapshot,
            WireConsistency::CaptureWindow => Self::CaptureWindow,
            WireConsistency::None => Self::AdapterDefined,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    Replace,
    Append,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Materialization {
    #[serde(rename = "local")]
    Local,
    #[serde(rename = "s3-view")]
    S3View,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PullRecovery {
    Transactional,
    Journaled,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Capabilities {
    pub push: bool,
    pub pull: bool,
    pub managed_build: bool,
    pub external_build: bool,
    pub resumable_extract: bool,
    pub inspect_connection: bool,
    pub after_publish: bool,
    pub source_consistency: WireConsistency,
    pub pull_write_modes: Vec<WriteMode>,
    pub pull_materializations: Vec<Materialization>,
    #[serde(deserialize_with = "required_option")]
    pub pull_recovery: Option<PullRecovery>,
    pub data_plane: Vec<String>,
}
impl Default for Capabilities {
    fn default() -> Self {
        Self {
            push: false,
            pull: false,
            managed_build: false,
            external_build: false,
            resumable_extract: false,
            inspect_connection: false,
            after_publish: false,
            source_consistency: WireConsistency::None,
            pull_write_modes: vec![],
            pull_materializations: vec![],
            pull_recovery: None,
            data_plane: vec!["stream".into()],
        }
    }
}
impl Capabilities {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.data_plane != ["stream"]
            || ((self.managed_build || self.external_build) && !self.push)
            || (self.pull
                && (self.pull_recovery.is_none()
                    || self.pull_write_modes.is_empty()
                    || self.pull_materializations.is_empty()))
            || (!self.pull
                && (!self.pull_write_modes.is_empty()
                    || !self.pull_materializations.is_empty()
                    || self.pull_recovery.is_some()))
            || self
                .pull_write_modes
                .iter()
                .enumerate()
                .any(|(i, x)| self.pull_write_modes[..i].contains(x))
            || self
                .pull_materializations
                .iter()
                .enumerate()
                .any(|(i, x)| self.pull_materializations[..i].contains(x))
        {
            Err(ValidationError("inconsistent capabilities".into()))
        } else {
            Ok(())
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Extract,
    ManagedBuild,
    ExternalBuild,
    Pull,
    Inspect,
    Command,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Registry {
    pub schema_bundle: Value,
    pub points: Vec<PointDescriptor>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PointDescriptor {
    pub point: String,
    pub mode: Mode,
    pub schema_pointer: String,
    pub default_value: Value,
    pub has_default: bool,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandDescriptor {
    pub name: Name,
    pub requires_connection: bool,
    pub requires_authentication: bool,
    pub args_schema_pointer: String,
    pub result_schema_pointer: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub name: Name,
    pub version: String,
    pub interface_versions: Vec<Req>,
    pub binding_schema_version: Req,
    pub entrypoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entrypoint_sha256: Option<Digest>,
}
impl Manifest {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.version.is_empty()
            || self.entrypoint.is_empty()
            || self.interface_versions.is_empty()
            || self
                .interface_versions
                .iter()
                .enumerate()
                .any(|(i, x)| self.interface_versions[..i].contains(x))
        {
            Err(ValidationError("invalid manifest".into()))
        } else {
            Ok(())
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoreIdentity {
    pub name: String,
    pub version: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandCall {
    pub command_id: Uuid,
    pub name: Name,
    pub args: Value,
    #[serde(deserialize_with = "required_option")]
    pub connection: Option<Value>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionLocator {
    pub canonical_connection: Value,
    #[serde(deserialize_with = "required_option")]
    pub identity: Option<String>,
    #[serde(deserialize_with = "required_option")]
    pub engine_path: Option<String>,
    #[serde(deserialize_with = "required_option")]
    pub session_lock_path: Option<String>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BindingState {
    Bound,
    Uninitialized,
    NotApplicable,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    pub attempt_id: Uuid,
    pub adapter_identity: AdapterIdentity,
    pub connection_identity: String,
    pub tables: Vec<CheckpointTable>,
    pub job: Value,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointTable {
    pub table: Name,
    pub snapshot_id: String,
    pub reopenable: bool,
    pub source_identity: Value,
    pub capture_start: Timestamp,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureWindow {
    pub start: Timestamp,
    pub end: Timestamp,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceCompletion {
    pub job: Value,
    pub capture_window: CaptureWindow,
    pub adapter_result: Value,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectionPolicy {
    Changed,
    All,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtractSelection {
    pub policy: SelectionPolicy,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtractTable {
    pub name: Name,
    pub source: Value,
    pub columns: Value,
    pub contract: TableContract,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtractRequest {
    pub attempt_id: Uuid,
    pub stream_id: Uuid,
    pub root: String,
    pub dataset: Name,
    pub run_id: RunId,
    pub declaration_sha256: Digest,
    pub adapter_identity: AdapterIdentity,
    pub connection_identity: String,
    pub selection: ExtractSelection,
    pub options: Value,
    pub tables: Vec<ExtractTable>,
    #[serde(deserialize_with = "required_option")]
    pub resume: Option<Checkpoint>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelState {
    Stopped,
    Completed,
}

/// First-wave implemented lifecycle only. Unknown variants are rejected.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "msg", rename_all = "snake_case", deny_unknown_fields)]
pub enum Frame {
    Hello {
        interface_versions: Vec<Req>,
        core: CoreIdentity,
        attempt: Uuid,
        resources: Resources,
    },
    Identified {
        name: Name,
        package_version: String,
        interface_versions: Vec<Req>,
        binding_schema_version: Req,
        capabilities: Capabilities,
        registry: Registry,
        commands: Vec<CommandDescriptor>,
    },
    Ready {
        interface_version: Req,
        binding_schema_version: Req,
        resources: Resources,
    },
    DocumentBegin {
        document_id: Uuid,
        size: U64,
        sha256: Digest,
    },
    DocumentChunk {
        document_id: Uuid,
        index: SafeInt,
        data: String,
    },
    DocumentEnd {
        document_id: Uuid,
    },
    DocumentAck {
        document_id: Uuid,
    },
    ValidateBinding {
        req: Req,
        declaration: Doc<Value>,
        mode: Mode,
        schema_version: Req,
    },
    ValidateResult {
        req: Req,
        effective_declaration: Doc<Value>,
    },
    LocateConnection {
        req: Req,
        connection: Doc<Value>,
        mode: Mode,
        #[serde(deserialize_with = "required_option")]
        run_id: Option<RunId>,
    },
    ConnectionLocated {
        req: Req,
        connection: Doc<ConnectionLocator>,
    },
    BindConnection {
        req: Req,
        locator: Doc<ConnectionLocator>,
        #[serde(deserialize_with = "required_option")]
        root: Option<String>,
        #[serde(deserialize_with = "required_option")]
        expected_identity: Option<String>,
        #[serde(deserialize_with = "required_option")]
        expected_workspace_id: Option<Uuid>,
        mode: Mode,
    },
    BindResult {
        req: Req,
        handle: Handle,
        #[serde(deserialize_with = "required_option")]
        identity: Option<String>,
        #[serde(deserialize_with = "required_option")]
        workspace_id: Option<Uuid>,
        binding: BindingState,
        details: Doc<Value>,
    },
    Authenticate {
        req: Req,
        handle: Handle,
        #[serde(deserialize_with = "required_option")]
        expected_identity: Option<String>,
    },
    AuthenticateResult {
        req: Req,
        identity: String,
    },
    AfterPublish {
        req: Req,
        handle: Handle,
        attempt_id: Uuid,
        declaration_sha256: Digest,
        outcome: Outcome,
    },
    AfterPublishResult {
        req: Req,
        acknowledged: bool,
    },
    InspectConnection {
        req: Req,
        handle: Handle,
        #[serde(deserialize_with = "required_option")]
        root: Option<String>,
        #[serde(deserialize_with = "required_option")]
        declaration: Option<Doc<Value>>,
    },
    InspectResult {
        req: Req,
        details: Doc<Value>,
    },
    PrepareCommand {
        req: Req,
        name: Name,
        argv: Doc<Vec<String>>,
    },
    CommandPrepared {
        req: Req,
        call: Doc<CommandCall>,
    },
    Command {
        req: Req,
        command_id: Uuid,
        #[serde(deserialize_with = "required_option")]
        handle: Option<Handle>,
    },
    CommandResult {
        req: Req,
        details: Doc<Value>,
    },
    Extract {
        req: Req,
        handle: Handle,
        payload: Doc<ExtractRequest>,
    },
    ExtractStarted {
        req: Req,
    },
    Checkpoint {
        req: Req,
        checkpoint_id: Uuid,
        payload: Doc<Checkpoint>,
    },
    CheckpointAck {
        req: Req,
        checkpoint_id: Uuid,
    },
    Batch {
        req: Req,
        table: Name,
        seq: SafeInt,
        slot: SafeInt,
        size: U64,
        rows: U64,
    },
    BatchAck {
        req: Req,
        table: Name,
        seq: SafeInt,
        slot: SafeInt,
    },
    TableComplete {
        req: Req,
        table: Name,
        row_count: U64,
        source_identity: Doc<Value>,
        capture: CaptureWindow,
    },
    SourceComplete {
        req: Req,
        completion: Doc<SourceCompletion>,
    },
    ResolvePull {
        req: Req,
        handle: Handle,
        payload: Doc<ResolvePullRequest>,
    },
    ResolveResult {
        req: Req,
        resolution: Doc<PullResolution>,
    },
    PreparePull {
        req: Req,
        handle: Handle,
        payload: Doc<PreparePullRequest>,
    },
    PrepareResult {
        req: Req,
        plan: Doc<PullPlan>,
    },
    ApplyPull {
        req: Req,
        handle: Handle,
        plan: Doc<PullPlan>,
    },
    ApplyResult {
        req: Req,
        receipt: Doc<Receipt>,
    },
    DiscoverBuild {
        req: Req,
        handle: Handle,
        payload: Doc<DiscoverBuildRequest>,
    },
    BuildDiscovered {
        req: Req,
        discovery: Doc<BuildDiscovery>,
    },
    PrepareBuild {
        req: Req,
        handle: Handle,
        payload: Doc<PrepareBuildRequest>,
    },
    BuildPrepared {
        req: Req,
        session: Doc<BuildSession>,
    },
    ExecuteBuild {
        req: Req,
        handle: Handle,
        payload: Doc<ExecuteBuildRequest>,
    },
    BuildFinished {
        req: Req,
        result: Doc<BuildExecutionResult>,
    },
    AcceptBuildCompletion {
        req: Req,
        handle: Handle,
        session_id: Uuid,
        completion: Doc<BuildCompletion>,
    },
    CompletionAccepted {
        req: Req,
        session_id: Uuid,
        completion_sha256: Digest,
    },
    ExportBuild {
        req: Req,
        handle: Handle,
        session_id: Uuid,
        completion_sha256: Digest,
        stream_id: Uuid,
    },
    BuildTableComplete {
        req: Req,
        table: Name,
        row_count: U64,
    },
    ExportComplete {
        req: Req,
        session_id: Uuid,
        completion_sha256: Digest,
        adapter_result: Doc<Value>,
    },
    OpenBuild {
        req: Req,
        handle: Handle,
        payload: Doc<BuildIdentity>,
    },
    BuildOpened {
        req: Req,
        record: Doc<BuildRecord>,
    },
    InspectBuild {
        req: Req,
        handle: Handle,
        payload: Doc<BuildIdentity>,
    },
    BuildInspected {
        req: Req,
        record: Doc<BuildRecord>,
    },
    AbortBuild {
        req: Req,
        handle: Handle,
        session_id: Uuid,
    },
    BuildAborted {
        req: Req,
        session_id: Uuid,
        writers_stopped: bool,
    },
    RecordBuildOutcome {
        req: Req,
        handle: Handle,
        session_id: Uuid,
        outcome: BuildOutcome,
    },
    BuildOutcomeRecorded {
        req: Req,
        session_id: Uuid,
    },
    CleanupBuild {
        req: Req,
        handle: Handle,
        session_id: Uuid,
    },
    BuildCleaned {
        req: Req,
        session_id: Uuid,
    },
    Cancel {
        req: Req,
    },
    CancelAck {
        req: Req,
        state: CancelState,
    },
    Close {
        req: Req,
    },
    CloseResult {
        req: Req,
    },
    Error {
        #[serde(deserialize_with = "required_option")]
        req: Option<Req>,
        code: ErrorCode,
        message: String,
        retryable: bool,
        #[serde(deserialize_with = "required_option")]
        object: Option<ObjectIdentity>,
    },
}
impl Frame {
    /// Semantic invariants that do not depend on a live channel or served schema.
    pub fn validate(&self) -> Result<(), ValidationError> {
        fn versions(v: &[Req]) -> Result<(), ValidationError> {
            if v.is_empty() || v.iter().enumerate().any(|(i, x)| v[..i].contains(x)) {
                Err(ValidationError("invalid interface version set".into()))
            } else {
                Ok(())
            }
        }
        fn capture(v: &CaptureWindow) -> Result<(), ValidationError> {
            if chrono_time(&v.start) > chrono_time(&v.end) {
                Err(ValidationError("capture window is reversed".into()))
            } else {
                Ok(())
            }
        }
        fn chrono_time(v: &Timestamp) -> chrono::DateTime<chrono::FixedOffset> {
            chrono::DateTime::parse_from_rfc3339(v.as_str()).expect("validated timestamp")
        }
        match self {
            Self::Hello {
                interface_versions,
                core,
                resources,
                ..
            } => {
                versions(interface_versions)?;
                resources.validate()?;
                if core.name.is_empty() || core.version.is_empty() {
                    return Err(ValidationError("empty core identity".into()));
                }
            }
            Self::Identified {
                package_version,
                interface_versions,
                capabilities,
                registry,
                commands,
                ..
            } => {
                versions(interface_versions)?;
                capabilities.validate()?;
                registry.validate()?;
                if package_version.is_empty() {
                    return Err(ValidationError("empty adapter package version".into()));
                }
                for (i, command) in commands.iter().enumerate() {
                    if command.requires_authentication && !command.requires_connection
                        || commands[..i].iter().any(|old| old.name == command.name)
                        || registry
                            .schema_bundle
                            .pointer(&command.args_schema_pointer)
                            .is_none()
                        || registry
                            .schema_bundle
                            .pointer(&command.result_schema_pointer)
                            .is_none()
                    {
                        return Err(ValidationError("invalid command registration".into()));
                    }
                }
            }
            Self::Ready { resources, .. } => resources.validate()?,
            Self::BindResult {
                binding,
                workspace_id,
                identity,
                ..
            } => {
                if (*binding == BindingState::Bound) != workspace_id.is_some()
                    || identity.as_ref().is_some_and(|id| id.is_empty())
                {
                    return Err(ValidationError(
                        "binding state contradicts its established workspace identity".into(),
                    ));
                }
            }
            Self::DocumentBegin { size, .. }
                if size.get() == 0 || size.get() > 64 * 1024 * 1024 =>
            {
                return Err(ValidationError("invalid document size".into()));
            }
            Self::Checkpoint {
                payload: Doc::Inline(payload),
                ..
            } => payload.inline.validate()?,
            Self::Extract {
                payload: Doc::Inline(payload),
                ..
            } => {
                let payload = &payload.inline;
                payload.adapter_identity.validate()?;
                if payload.root.is_empty()
                    || payload.connection_identity.is_empty()
                    || !payload.options.is_object()
                    || payload.tables.is_empty()
                    || payload
                        .tables
                        .iter()
                        .enumerate()
                        .any(|(i, t)| payload.tables[..i].iter().any(|old| old.name == t.name))
                {
                    return Err(ValidationError("invalid extraction request".into()));
                }
                for table in &payload.tables {
                    table.contract.validate()?;
                }
                if let Some(checkpoint) = &payload.resume {
                    checkpoint.validate()?;
                }
            }
            Self::Batch { size, rows, .. }
                if size.get() == 0
                    || size.get() % 8 != 0
                    || rows.get() == 0
                    || rows.get() > i32::MAX as u64 =>
            {
                return Err(ValidationError("invalid batch dimensions".into()));
            }
            Self::TableComplete { capture: v, .. } => capture(v)?,
            Self::SourceComplete {
                completion: Doc::Inline(v),
                ..
            } => capture(&v.inline.capture_window)?,
            Self::ResolvePull {
                payload: Doc::Inline(payload),
                ..
            } => payload.inline.validate()?,
            Self::ResolveResult {
                resolution: Doc::Inline(payload),
                ..
            } => payload.inline.validate()?,
            Self::PreparePull {
                payload: Doc::Inline(payload),
                ..
            } => payload.inline.validate()?,
            Self::PrepareResult {
                plan: Doc::Inline(payload),
                ..
            }
            | Self::ApplyPull {
                plan: Doc::Inline(payload),
                ..
            } => payload.inline.validate()?,
            Self::ApplyResult {
                receipt: Doc::Inline(payload),
                ..
            } => payload.inline.validate()?,
            Self::DiscoverBuild {
                payload: Doc::Inline(p),
                ..
            } => p.inline.validate()?,
            Self::BuildDiscovered {
                discovery: Doc::Inline(p),
                ..
            } => p.inline.validate()?,
            Self::PrepareBuild {
                payload: Doc::Inline(p),
                ..
            } => p.inline.validate()?,
            Self::BuildPrepared {
                session: Doc::Inline(p),
                ..
            } => p.inline.validate()?,
            Self::ExecuteBuild {
                payload: Doc::Inline(p),
                ..
            } => p.inline.validate()?,
            Self::BuildFinished {
                result: Doc::Inline(p),
                ..
            } => p.inline.validate()?,
            Self::AcceptBuildCompletion {
                completion: Doc::Inline(p),
                ..
            } => p.inline.validate()?,
            Self::OpenBuild {
                payload: Doc::Inline(p),
                ..
            }
            | Self::InspectBuild {
                payload: Doc::Inline(p),
                ..
            } => p.inline.validate()?,
            Self::BuildOpened {
                record: Doc::Inline(p),
                ..
            }
            | Self::BuildInspected {
                record: Doc::Inline(p),
                ..
            } => p.inline.validate()?,
            Self::RecordBuildOutcome { outcome, .. } | Self::AfterPublish { outcome, .. } => {
                outcome.validate()?
            }
            Self::AfterPublishResult { acknowledged, .. } if !acknowledged => {
                return Err(ValidationError(
                    "after-publish acknowledgement must be true".into(),
                ));
            }
            Self::InspectConnection {
                root, declaration, ..
            } => {
                InspectConnectionRequest {
                    root: root.clone(),
                    declaration: match declaration {
                        Some(Doc::Inline(value)) => Some(value.inline.clone()),
                        _ => None,
                    },
                }
                .validate()?;
            }
            Self::BuildAborted {
                writers_stopped: false,
                ..
            } => {
                return Err(ValidationError(
                    "build abort requires stopped writers".into(),
                ));
            }
            Self::Error {
                message, object, ..
            } => {
                if message.is_empty() {
                    return Err(ValidationError("empty protocol error message".into()));
                }
                if let Some(object) = object {
                    object.validate()?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    pub fn req(&self) -> Option<Req> {
        match self {
            Self::ValidateBinding { req, .. }
            | Self::ValidateResult { req, .. }
            | Self::LocateConnection { req, .. }
            | Self::ConnectionLocated { req, .. }
            | Self::BindConnection { req, .. }
            | Self::BindResult { req, .. }
            | Self::Authenticate { req, .. }
            | Self::AuthenticateResult { req, .. }
            | Self::AfterPublish { req, .. }
            | Self::AfterPublishResult { req, .. }
            | Self::InspectConnection { req, .. }
            | Self::InspectResult { req, .. }
            | Self::PrepareCommand { req, .. }
            | Self::CommandPrepared { req, .. }
            | Self::Command { req, .. }
            | Self::CommandResult { req, .. }
            | Self::Extract { req, .. }
            | Self::ExtractStarted { req }
            | Self::Checkpoint { req, .. }
            | Self::CheckpointAck { req, .. }
            | Self::Batch { req, .. }
            | Self::BatchAck { req, .. }
            | Self::TableComplete { req, .. }
            | Self::SourceComplete { req, .. }
            | Self::ResolvePull { req, .. }
            | Self::ResolveResult { req, .. }
            | Self::PreparePull { req, .. }
            | Self::PrepareResult { req, .. }
            | Self::ApplyPull { req, .. }
            | Self::ApplyResult { req, .. }
            | Self::DiscoverBuild { req, .. }
            | Self::BuildDiscovered { req, .. }
            | Self::PrepareBuild { req, .. }
            | Self::BuildPrepared { req, .. }
            | Self::ExecuteBuild { req, .. }
            | Self::BuildFinished { req, .. }
            | Self::AcceptBuildCompletion { req, .. }
            | Self::CompletionAccepted { req, .. }
            | Self::ExportBuild { req, .. }
            | Self::BuildTableComplete { req, .. }
            | Self::ExportComplete { req, .. }
            | Self::OpenBuild { req, .. }
            | Self::BuildOpened { req, .. }
            | Self::InspectBuild { req, .. }
            | Self::BuildInspected { req, .. }
            | Self::AbortBuild { req, .. }
            | Self::BuildAborted { req, .. }
            | Self::RecordBuildOutcome { req, .. }
            | Self::BuildOutcomeRecorded { req, .. }
            | Self::CleanupBuild { req, .. }
            | Self::BuildCleaned { req, .. }
            | Self::Cancel { req }
            | Self::CancelAck { req, .. }
            | Self::Close { req }
            | Self::CloseResult { req } => Some(*req),
            Self::Error { req, .. } => *req,
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterListEntry {
    pub name: Name,
    pub package_version: String,
    pub interface_versions: Vec<Req>,
    pub binding_schema_version: Req,
    pub manifest: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterListResult {
    pub search_roots: Vec<String>,
    pub adapters: Vec<AdapterListEntry>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterInstallResult {
    pub adapter: AdapterListEntry,
    pub replaced: bool,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicCapabilities {
    pub push: bool,
    pub pull: bool,
    pub source_consistency: SourceConsistency,
    pub resumable_extract: bool,
    pub pull_write_modes: Vec<WriteMode>,
    pub commands: Vec<Name>,
    pub managed_build: bool,
    pub external_build: bool,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterDescriptor {
    pub name: Name,
    pub package_version: String,
    pub interface_version: Req,
    pub capabilities: PublicCapabilities,
    pub binding_schema_version: Req,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterCapabilitiesResult {
    pub adapter: AdapterDescriptor,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterCommandResult {
    pub adapter: Name,
    pub package_version: String,
    pub adapter_command: Name,
    pub details: Value,
}

impl Registry {
    /// Check registration structure without fetching external schemas.
    pub fn validate(&self) -> Result<(), ValidationError> {
        fn local_refs(value: &Value) -> bool {
            match value {
                Value::Object(o) => {
                    !o.contains_key("$dynamicRef")
                        && !o.contains_key("$recursiveRef")
                        && o.get("$ref").is_none_or(|v| {
                            v.as_str().is_some_and(|s| s == "#" || s.starts_with("#/"))
                        })
                        && o.values().all(local_refs)
                }
                Value::Array(a) => a.iter().all(local_refs),
                _ => true,
            }
        }
        if !self.schema_bundle.is_object() || !local_refs(&self.schema_bundle) {
            return Err(ValidationError(
                "registry must be an offline schema bundle".into(),
            ));
        }
        for (i, point) in self.points.iter().enumerate() {
            if ![
                "connection",
                "options",
                "table_source",
                "target",
                "table_target",
                "table_select",
                "column_source",
                "build_input",
                "connection_details",
                "pull_plan",
                "pull_result",
                "push_result",
                "inspection_result",
                "session_details",
                "source_identity",
                "source_job",
            ]
            .contains(&point.point.as_str())
                || (!point.has_default && !point.default_value.is_null())
                || self.points[..i]
                    .iter()
                    .any(|p| p.point == point.point && p.mode == point.mode)
                || self.schema_bundle.pointer(&point.schema_pointer).is_none()
            {
                return Err(ValidationError("invalid registry point descriptor".into()));
            }
        }
        Ok(())
    }
}

impl Checkpoint {
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.adapter_identity.validate()?;
        if self.connection_identity.is_empty()
            || self.tables.is_empty()
            || self.tables.iter().enumerate().any(|(i, t)| {
                t.snapshot_id.is_empty() || self.tables[..i].iter().any(|old| old.table == t.table)
            })
        {
            Err(ValidationError("invalid checkpoint evidence".into()))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn inspection_scope_is_closed_and_nullable_members_are_required() {
        for invalid in [
            serde_json::json!({"root":null}),
            serde_json::json!({"root":null,"declaration":{},"other":0}),
        ] {
            assert!(serde_json::from_value::<InspectConnectionRequest>(invalid).is_err());
        }
        for declaration in [
            serde_json::json!([]),
            serde_json::json!(1),
            serde_json::json!("secret"),
        ] {
            assert!(
                Frame::InspectConnection {
                    req: Req::new(1).unwrap(),
                    handle: Handle::new("h").unwrap(),
                    root: None,
                    declaration: Some(Doc::inline(declaration))
                }
                .validate()
                .is_err()
            );
        }
        InspectConnectionRequest {
            root: None,
            declaration: None,
        }
        .validate()
        .unwrap();
    }
    #[test]
    fn uninitialized_binding_never_claims_a_reserved_workspace_is_established() {
        let workspace = Uuid::v4();
        for (binding, workspace_id, valid) in [
            (BindingState::Bound, Some(workspace.clone()), true),
            (BindingState::Bound, None, false),
            (BindingState::Uninitialized, None, true),
            (BindingState::Uninitialized, Some(workspace.clone()), false),
            (BindingState::NotApplicable, None, true),
            (BindingState::NotApplicable, Some(workspace), false),
        ] {
            let frame = Frame::BindResult {
                req: Req::new(1).unwrap(),
                handle: Handle::new("bound").unwrap(),
                identity: Some("connection".into()),
                workspace_id,
                binding,
                details: Doc::inline(serde_json::json!({})),
            };
            assert_eq!(frame.validate().is_ok(), valid);
        }
    }
    #[test]
    fn documents_and_frames_are_closed() {
        assert!(
            serde_json::from_str::<Doc<Value>>(
                r#"{"inline":null,"document_id":"359c6d0f-a9c1-4ae6-b804-0742a5e2b9de"}"#
            )
            .is_err()
        );
        assert!(serde_json::from_str::<Frame>(r#"{"msg":"close","req":1,"other":true}"#).is_err());
        assert!(serde_json::from_str::<Frame>(r#"{"msg":"close","req":0}"#).is_err());
        assert!(serde_json::from_str::<Frame>(r#"{"msg":"error","code":"ADAPTER_FAILURE","message":"failed","retryable":false,"object":null}"#).is_err());
        assert!(
            serde_json::from_str::<CommandCall>(
                r#"{"command_id":"359c6d0f-a9c1-4ae6-b804-0742a5e2b9de","name":"echo","args":{}}"#
            )
            .is_err()
        );
    }
    #[test]
    fn consistency_projection_is_explicit() {
        assert_eq!(
            SourceConsistency::from(WireConsistency::None),
            SourceConsistency::AdapterDefined
        );
        assert_eq!(
            serde_json::to_value(SourceConsistency::from(WireConsistency::Snapshot)).unwrap(),
            "transaction-snapshot"
        );
    }
    #[test]
    fn offered_resources_validate() {
        Resources::default().validate().unwrap();
        let r = Resources {
            slots: Req::new(65).unwrap(),
            ..Resources::default()
        };
        assert!(r.validate().is_err());
    }
    #[test]
    fn exact_logical_types_reject_lossy_and_unknown_shapes() {
        for v in [
            serde_json::json!("int32"),
            serde_json::json!({"decimal":{"precision":39,"scale":0}}),
            serde_json::json!({"timestamp":{"unit":"ns","utc":true}}),
            serde_json::json!({"timestamp":{"unit":"us","utc":true,"other":0}}),
        ] {
            assert!(validate_logical_type(&v).is_err());
        }
        validate_logical_type(&serde_json::json!({"decimal":{"precision":38,"scale":38}})).unwrap();
    }
}
