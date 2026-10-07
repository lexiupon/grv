//! Closed build records. These carry fixed identities and verified file facts,
//! never GRV ownership tokens or storage implementations.
use super::*;
use std::collections::BTreeSet;

fn invalid(message: &str) -> ValidationError {
    ValidationError(message.into())
}
fn unique<'a>(names: impl IntoIterator<Item = &'a Name>) -> Result<(), ValidationError> {
    let mut seen = BTreeSet::new();
    if names.into_iter().any(|name| !seen.insert(name)) {
        return Err(invalid("duplicate build name"));
    }
    Ok(())
}
fn selected(outputs: &[OutputBinding], names: &[Name]) -> Result<(), ValidationError> {
    unique(names)?;
    if names
        .iter()
        .any(|name| !outputs.iter().any(|o| &o.table == name))
    {
        return Err(invalid("selected output has no binding"));
    }
    Ok(())
}
fn engine_table(name: &str) -> bool {
    let parts: Vec<_> = name.split('.').collect();
    parts.len() == 2
        && parts.iter().all(|part| {
            !part.is_empty()
                && part.bytes().enumerate().all(|(i, b)| {
                    b.is_ascii_lowercase()
                        || b.is_ascii_digit()
                        || b == b'_'
                        || (i > 0 && b == b'-')
                })
        })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuildExecution {
    Managed,
    External,
}
impl BuildExecution {
    pub fn mode(self) -> Mode {
        match self {
            Self::Managed => Mode::ManagedBuild,
            Self::External => Mode::ExternalBuild,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildIdentity {
    pub attempt_id: Uuid,
    pub root: String,
    pub dataset: Name,
    pub run_id: RunId,
    pub workspace_id: Uuid,
    pub declaration_sha256: Digest,
    pub adapter_identity: AdapterIdentity,
    pub connection_identity: String,
}
impl BuildIdentity {
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.adapter_identity.validate()?;
        if self.root.is_empty() || self.connection_identity.is_empty() {
            return Err(invalid("invalid build identity"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputBinding {
    pub alias: Name,
    pub relation: Value,
    pub dataset: Name,
    pub table: Name,
    pub revision: U64,
    pub generation_id: Uuid,
    pub contract: TableContract,
    pub materialization: Materialization,
}
impl InputBinding {
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.contract.validate()?;
        if self.revision.get() == 0 {
            return Err(invalid("build input revision must be published"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputBinding {
    pub table: Name,
    pub source: Value,
    pub columns: Value,
    pub engine_table: String,
    pub contract: TableContract,
}
impl OutputBinding {
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.contract.validate()?;
        build_columns(&self.columns, &self.contract)?;
        if !engine_table(&self.engine_table) {
            return Err(invalid("invalid build engine mapping"));
        }
        Ok(())
    }
}
/// The adapter receives only mapped columns, in the core input-contract order.
/// Derived partition columns are computed by the parent after capture.
fn build_columns(columns: &Value, contract: &TableContract) -> Result<(), ValidationError> {
    let columns = columns
        .as_array()
        .ok_or_else(|| invalid("build columns must be an array"))?;
    if columns.len() != contract.columns.len() {
        return Err(invalid("build mappings differ from logical input contract"));
    }
    for (column, field) in columns.iter().zip(&contract.columns) {
        let column = column
            .as_object()
            .ok_or_else(|| invalid("build column must be an object"))?;
        if column
            .keys()
            .any(|key| !["name", "type", "source"].contains(&key.as_str()))
            || column.get("name").and_then(Value::as_str) != Some(field.name.as_str())
            || column
                .get("type")
                .and_then(|value| {
                    if grv_types::validate_logical_type(value).is_ok() {
                        Some(value.clone())
                    } else {
                        value
                            .as_str()
                            .and_then(|value| grv_types::authoring_type(value).ok())
                    }
                })
                .as_ref()
                != Some(&field.logical_type)
        {
            return Err(invalid("build mappings differ from logical input contract"));
        }
    }
    Ok(())
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildInput {
    pub alias: Name,
    pub relation: Value,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildOutput {
    pub table: Name,
    pub source: Value,
    pub columns: Value,
    pub contract: TableContract,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoverBuildRequest {
    pub identity: BuildIdentity,
    pub options: Value,
    pub execution: BuildExecution,
    pub inputs: Vec<BuildInput>,
    pub outputs: Vec<BuildOutput>,
    pub selected_outputs: Vec<Name>,
    pub self_input: bool,
}
impl DiscoverBuildRequest {
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.identity.validate()?;
        if !self.options.is_object() {
            return Err(invalid("build options must be an object"));
        }
        unique(self.inputs.iter().map(|i| &i.alias))?;
        unique(self.outputs.iter().map(|o| &o.table))?;
        unique(&self.selected_outputs)?;
        for output in &self.outputs {
            output.contract.validate()?;
            build_columns(&output.columns, &output.contract)?;
        }
        if self
            .selected_outputs
            .iter()
            .any(|n| !self.outputs.iter().any(|o| &o.table == n))
        {
            return Err(invalid("unknown selected build output"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildDiscovery {
    pub discovery_id: Uuid,
    pub identity: BuildIdentity,
    pub inputs: Vec<InputBinding>,
    pub outputs: Vec<OutputBinding>,
}
impl BuildDiscovery {
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.identity.validate()?;
        unique(self.inputs.iter().map(|i| &i.alias))?;
        unique(self.outputs.iter().map(|o| &o.table))?;
        let mut mappings = BTreeSet::new();
        for input in &self.inputs {
            input.validate()?;
            if input.dataset == self.identity.dataset {
                return Err(invalid("external build input cannot be target dataset"));
            }
        }
        for output in &self.outputs {
            output.validate()?;
            if !mappings.insert(&output.engine_table) {
                return Err(invalid("duplicate physical output mapping"));
            }
        }
        Ok(())
    }
    pub fn validate_for(&self, request: &DiscoverBuildRequest) -> Result<(), ValidationError> {
        self.validate()?;
        request.validate()?;
        if self.identity != request.identity
            || self.inputs.len() != request.inputs.len()
            || self.outputs.len() != request.outputs.len()
            || request.inputs.iter().any(|i| {
                !self
                    .inputs
                    .iter()
                    .any(|b| b.alias == i.alias && b.relation == i.relation)
            })
            || request.outputs.iter().any(|o| {
                !self.outputs.iter().any(|b| {
                    b.table == o.table
                        && b.source == o.source
                        && b.columns == o.columns
                        && b.contract == o.contract
                })
            })
        {
            return Err(invalid("build discovery changed fixed request"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildInputFiles {
    pub alias: Name,
    pub files: Vec<VerifiedFile>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrepareBuildRequest {
    pub discovery: BuildDiscovery,
    pub base_revision: U64,
    pub self_input: bool,
    pub base_contracts: Vec<NamedContract>,
    pub base_files: Vec<VerifiedFile>,
    pub input_files: Vec<BuildInputFiles>,
    pub holds_confirmed: bool,
}
fn files(files: &[VerifiedFile], contract: &TableContract) -> Result<(), ValidationError> {
    let mut locations = BTreeSet::new();
    let mut table = None;
    for file in files {
        file.validate()?;
        if !locations.insert(&file.location) || table.is_some_and(|t| t != &file.table) {
            return Err(invalid("duplicate or mixed build input files"));
        }
        table = Some(&file.table);
        let partition = file
            .partition
            .as_object()
            .ok_or_else(|| invalid("build file partition must be an object"))?;
        if partition.len() != contract.partition_keys.len()
            || contract.partition_keys.iter().any(|key| {
                partition
                    .get(key.as_str())
                    .and_then(Value::as_str)
                    .is_none_or(|s| Name::new(s).is_err())
            })
            || file.schema.columns.len() > contract.columns.len()
            || contract.columns[..file.schema.columns.len()] != file.schema.columns
        {
            return Err(invalid(
                "build file schema or partition differs from fixed contract",
            ));
        }
    }
    Ok(())
}
impl PrepareBuildRequest {
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.discovery.validate()?;
        unique(self.input_files.iter().map(|i| &i.alias))?;
        unique(self.base_contracts.iter().map(|c| &c.table))?;
        if !self.holds_confirmed
            || (!self.self_input
                && (!self.base_files.is_empty() || !self.base_contracts.is_empty()))
            || (self.base_revision.get() == 0
                && (!self.base_files.is_empty() || !self.base_contracts.is_empty()))
            || self.input_files.len() != self.discovery.inputs.len()
        {
            return Err(invalid("build preparation requires confirmed exact inputs"));
        }
        for group in &self.input_files {
            let binding = self
                .discovery
                .inputs
                .iter()
                .find(|i| i.alias == group.alias)
                .ok_or_else(|| invalid("unknown build file alias"))?;
            if group.files.iter().any(|f| f.table != binding.table) {
                return Err(invalid("build files differ from discovered source table"));
            }
            files(&group.files, &binding.contract)?;
        }
        for base in &self.base_contracts {
            base.contract.validate()?;
            let members: Vec<_> = self
                .base_files
                .iter()
                .filter(|f| f.table == base.table)
                .cloned()
                .collect();
            files(&members, &base.contract)?;
        }
        if self
            .base_files
            .iter()
            .any(|f| !self.base_contracts.iter().any(|c| c.table == f.table))
        {
            return Err(invalid("base file has no fixed base contract"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildSession {
    pub session_id: Uuid,
    pub options: Value,
    pub identity: BuildIdentity,
    pub execution: BuildExecution,
    pub base_revision: U64,
    pub base_contracts: Vec<NamedContract>,
    pub inputs: Vec<InputBinding>,
    pub outputs: Vec<OutputBinding>,
    pub selected_outputs: Vec<Name>,
    pub self_input: bool,
    pub adapter_details: Value,
}
impl BuildSession {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if !self.options.is_object() {
            return Err(invalid("build options must be an object"));
        }
        BuildDiscovery {
            discovery_id: self.session_id.clone(),
            identity: self.identity.clone(),
            inputs: self.inputs.clone(),
            outputs: self.outputs.clone(),
        }
        .validate()?;
        unique(self.base_contracts.iter().map(|c| &c.table))?;
        for base in &self.base_contracts {
            base.contract.validate()?;
        }
        if (!self.self_input || self.base_revision.get() == 0) && !self.base_contracts.is_empty() {
            return Err(invalid(
                "base contracts require self-input on a published base",
            ));
        }
        selected(&self.outputs, &self.selected_outputs)
    }
    pub fn validate_for(
        &self,
        request: &DiscoverBuildRequest,
        preparation: &PrepareBuildRequest,
    ) -> Result<(), ValidationError> {
        self.validate()?;
        preparation.validate()?;
        preparation.discovery.validate_for(request)?;
        if self.identity != request.identity
            || self.options != request.options
            || self.execution != request.execution
            || self.selected_outputs != request.selected_outputs
            || self.self_input != request.self_input
            || preparation.self_input != request.self_input
            || self.base_revision != preparation.base_revision
            || self.base_contracts != preparation.base_contracts
            || self.inputs != preparation.discovery.inputs
            || self.outputs != preparation.discovery.outputs
        {
            return Err(invalid("prepared session changed fixed build facts"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildQuery {
    pub table: Name,
    pub sql: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecuteBuildRequest {
    pub session: BuildSession,
    pub queries: Vec<BuildQuery>,
}
impl ExecuteBuildRequest {
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.session.validate()?;
        unique(self.queries.iter().map(|q| &q.table))?;
        if self.session.execution != BuildExecution::Managed
            || self.queries.len() != self.session.selected_outputs.len()
            || self.queries.iter().any(|q| {
                q.sql.is_empty()
                    || !self.session.selected_outputs.contains(&q.table)
                    || self
                        .session
                        .outputs
                        .iter()
                        .find(|o| o.table == q.table)
                        .and_then(|o| o.source.get("sql"))
                        .and_then(Value::as_str)
                        != Some(q.sql.as_str())
            })
        {
            return Err(invalid("managed queries differ from prepared selected SQL"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CompletionKind {
    Engine,
    Direct,
    OmissionOnly,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionStatus {
    Succeeded,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletedOutput {
    pub table: Name,
    pub engine_table: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildCompletion {
    pub result_version: Req,
    pub run_id: RunId,
    pub workspace_id: Uuid,
    pub declaration_sha256: Digest,
    pub kind: CompletionKind,
    pub invocation_id: String,
    pub status: CompletionStatus,
    pub writers_stopped: bool,
    pub completed_at: Timestamp,
    pub completed_outputs: Vec<CompletedOutput>,
}
impl BuildCompletion {
    pub fn validate(&self) -> Result<(), ValidationError> {
        unique(self.completed_outputs.iter().map(|o| &o.table))?;
        if self.result_version.get() != 1
            || !self.writers_stopped
            || self.invocation_id.is_empty()
            || self
                .completed_outputs
                .iter()
                .any(|o| !engine_table(&o.engine_table))
            || (self.kind == CompletionKind::OmissionOnly && !self.completed_outputs.is_empty())
        {
            return Err(invalid("invalid stopped-writer build completion"));
        }
        Ok(())
    }
    pub fn validate_for(&self, session: &BuildSession) -> Result<(), ValidationError> {
        self.validate()?;
        session.validate()?;
        if self.run_id != session.identity.run_id
            || self.workspace_id != session.identity.workspace_id
            || self.declaration_sha256 != session.identity.declaration_sha256
            || self.completed_outputs.len() != session.selected_outputs.len()
            || self.completed_outputs.iter().any(|c| {
                !session.selected_outputs.contains(&c.table)
                    || !session
                        .outputs
                        .iter()
                        .any(|o| o.table == c.table && o.engine_table == c.engine_table)
            })
        {
            return Err(invalid(
                "completion differs from fixed session identity or mappings",
            ));
        }
        Ok(())
    }
    pub fn digest(&self) -> Result<Digest, ValidationError> {
        self.validate()?;
        grv_types::canonical_json(self)
            .map(|b| grv_types::sha256(&b))
            .map_err(|_| invalid("completion is not canonicalizable"))
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuildExecutionStatus {
    Succeeded,
    Failed,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildExecutionResult {
    pub status: BuildExecutionStatus,
    #[serde(deserialize_with = "required_option")]
    pub completion: Option<BuildCompletion>,
    pub row_counts: Vec<TableCount>,
}
fn counts(
    counts: &[TableCount],
    session: &BuildSession,
    complete: bool,
) -> Result<(), ValidationError> {
    unique(counts.iter().map(|c| &c.table))?;
    if (complete && counts.len() != session.selected_outputs.len())
        || counts
            .iter()
            .any(|c| !session.selected_outputs.contains(&c.table))
    {
        return Err(invalid("build counts differ from selected outputs"));
    }
    Ok(())
}
impl BuildExecutionResult {
    pub fn validate(&self) -> Result<(), ValidationError> {
        unique(self.row_counts.iter().map(|c| &c.table))?;
        if (self.status == BuildExecutionStatus::Succeeded) != self.completion.is_some() {
            return Err(invalid(
                "build success requires completion and failure forbids it",
            ));
        }
        if let Some(completion) = &self.completion {
            completion.validate()?;
        }
        Ok(())
    }
    pub fn validate_for(&self, session: &BuildSession) -> Result<(), ValidationError> {
        self.validate()?;
        session.validate()?;
        counts(
            &self.row_counts,
            session,
            self.status == BuildExecutionStatus::Succeeded,
        )?;
        if let Some(completion) = &self.completion {
            completion.validate_for(session)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OutcomeKind {
    Published,
    NoOp,
    Aborted,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildOutcome {
    pub kind: OutcomeKind,
    #[serde(deserialize_with = "required_option")]
    pub revision: Option<U64>,
    #[serde(deserialize_with = "required_option")]
    pub operation_id: Option<RunId>,
}
impl BuildOutcome {
    pub fn validate(&self) -> Result<(), ValidationError> {
        let valid = match self.kind {
            OutcomeKind::Published => {
                self.revision.is_some_and(|r| r.get() > 0) && self.operation_id.is_some()
            }
            OutcomeKind::NoOp => self.revision.is_some() && self.operation_id.is_none(),
            OutcomeKind::Aborted => self.revision.is_none() && self.operation_id.is_none(),
        };
        if !valid {
            return Err(invalid("invalid build outcome identifiers"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuildState {
    Prepared,
    Executing,
    Completed,
    Aborted,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildRecord {
    pub session: BuildSession,
    pub state: BuildState,
    #[serde(deserialize_with = "required_option")]
    pub completion: Option<BuildCompletion>,
    #[serde(deserialize_with = "required_option")]
    pub completion_sha256: Option<Digest>,
    #[serde(deserialize_with = "required_option")]
    pub candidate: Option<BuildCompletion>,
    pub row_counts: Vec<TableCount>,
    #[serde(deserialize_with = "required_option")]
    pub outcome: Option<BuildOutcome>,
}
impl BuildRecord {
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.session.validate()?;
        counts(&self.row_counts, &self.session, self.candidate.is_some())?;
        if self.completion.is_some() != self.completion_sha256.is_some()
            || ((self.candidate.is_some() || self.completion.is_some())
                && !matches!(self.state, BuildState::Completed | BuildState::Aborted))
            || (self.candidate.is_some() && self.session.execution != BuildExecution::Managed)
        {
            return Err(invalid(
                "build record state contradicts completion evidence",
            ));
        }
        for completion in [&self.candidate, &self.completion].into_iter().flatten() {
            completion.validate_for(&self.session)?;
        }
        if let Some(completion) = &self.completion
            && (Some(completion.digest()?) != self.completion_sha256
                || self.candidate.as_ref().is_some_and(|c| c != completion))
        {
            return Err(invalid(
                "build record completion digest or candidate differs",
            ));
        }
        if let Some(outcome) = &self.outcome {
            outcome.validate()?;
            if match outcome.kind {
                OutcomeKind::Published | OutcomeKind::NoOp => {
                    self.state != BuildState::Completed || self.completion.is_none()
                }
                OutcomeKind::Aborted => self.state != BuildState::Aborted,
            } {
                return Err(invalid(
                    "terminal build outcome contradicts accepted completion or abort state",
                ));
            }
        }
        Ok(())
    }
}
