//! Native build consumer state. Discovery keeps one preparation transaction;
//! invocation evidence and completion remain independent of GRV publication.
use crate::{
    native::NativeEngine,
    pull::{
        self, PullError, PullReceipt, PullStore, SourceFile, VerifiedSource, WorkspaceBinding,
        quote_identifier as qi, quote_literal as ql,
    },
    worker::Engine,
};
use grv_adapter_api::*;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

pub type Result<T> = std::result::Result<T, BuildError>;
#[derive(Debug)]
pub enum BuildError {
    Engine(io::Error),
    RequestMismatch,
    Incomplete(String),
    Unknown(String),
    Conflict(String),
}
impl From<io::Error> for BuildError {
    fn from(error: io::Error) -> Self {
        Self::Engine(error)
    }
}
impl From<PullError> for BuildError {
    fn from(error: PullError) -> Self {
        match error {
            PullError::Engine(e) => Self::Engine(e),
            PullError::RequestMismatch => Self::RequestMismatch,
            PullError::StateConflict(e) => Self::Conflict(e),
            PullError::OutcomeUnknown(e) => Self::Unknown(e),
        }
    }
}
fn invalid(message: &str) -> BuildError {
    BuildError::Conflict(message.into())
}
fn canonical<T: Serialize>(value: &T) -> io::Result<String> {
    String::from_utf8(grv_types::canonical_json(value).map_err(io::Error::other)?)
        .map_err(io::Error::other)
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Reservation {
    request: DiscoverBuildRequest,
    discovery: BuildDiscovery,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Invocation {
    id: Uuid,
    queries_sha256: Digest,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Stored {
    record: BuildRecord,
    invocation: Option<Invocation>,
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum CrashPoint {
    BeforeOutputCommit,
    AfterOutputCommit,
    AfterCandidate,
    AfterAcceptance,
}
pub struct BuildStore {
    engine: NativeEngine,
    path: PathBuf,
    root: String,
    binding: Option<WorkspaceBinding>,
    expected_workspace: Option<Uuid>,
    reservation: Option<Reservation>,
    scratch: usize,
    s3_resources_admitted: bool,
    stopping: Arc<AtomicBool>,
    broken: bool,
    export: Option<(Vec<crate::native::AcquiredSchema>, Vec<OutputBinding>)>,
    #[cfg(test)]
    crash: Option<CrashPoint>,
    #[cfg(test)]
    preparation_fault: Option<bool>,
}
impl BuildStore {
    pub fn configure_s3_reader(&mut self, reader: &crate::s3_config::S3Reader) -> Result<()> {
        if !self.s3_resources_admitted {
            return Err(invalid(
                "negotiated resources cannot admit bounded S3 reader state",
            ));
        }
        self.engine.configure_s3_reader(reader)?;
        Ok(())
    }

    pub fn open(
        path: &Path,
        root: String,
        workspace: Option<Uuid>,
        resources: &Resources,
    ) -> Result<Self> {
        resources.validate().map_err(io::Error::other)?;
        let canonical_path = crate::lock::canonical_engine_path(path)?;
        let mut store = PullStore::open(&canonical_path)?;
        let binding = store.binding(&root)?;
        if binding.as_ref().is_some_and(|binding| {
            workspace
                .as_ref()
                .is_some_and(|id| id != &binding.workspace_id)
        }) {
            return Err(BuildError::RequestMismatch);
        }
        // Validate complete immutable receipt authority before any discovery.
        store.lookup(&Uuid::v4(), &root)?;
        let mut engine = store.into_engine();
        if engine.build_evidence_recorded()?
            && (!engine.base_table("_grv", "build_sessions")?
                || engine
                    .metadata_query("SELECT attempt_id FROM _grv.build_sessions LIMIT 1")?
                    .is_empty())
        {
            return Err(BuildError::Unknown(
                "workspace lost durable build history".into(),
            ));
        }
        Ok(Self {
            engine,
            path: canonical_path,
            root,
            binding,
            expected_workspace: workspace,
            reservation: None,
            s3_resources_admitted: resources.max_source_unit_bytes.get() >= 2 * 1024 * 1024
                && resources.max_scratch_bytes.get() >= 16 * 1024 * 1024,
            scratch: resources
                .max_source_unit_bytes
                .get()
                .min(
                    resources
                        .max_scratch_bytes
                        .get()
                        .saturating_sub(std::mem::size_of::<sha2::Sha256>() as u64),
                )
                .min(65536) as usize,
            stopping: Arc::new(AtomicBool::new(false)),
            broken: false,
            export: None,
            #[cfg(test)]
            crash: None,
            #[cfg(test)]
            preparation_fault: None,
        })
    }
    #[cfg(test)]
    fn crash_at(&self, point: CrashPoint) {
        if self.crash == Some(point) {
            std::process::exit(86);
        }
    }
    pub fn workspace_id(&self) -> Option<Uuid> {
        self.binding.as_ref().map(|b| b.workspace_id.clone())
    }
    pub(crate) fn interrupt_handle(&self) -> crate::native::NativeInterrupt {
        self.engine.interrupt_handle()
    }
    pub(crate) fn set_stopping(&mut self, stop: Arc<AtomicBool>) {
        self.stopping = stop;
    }
    fn check(&self) -> Result<()> {
        if self.broken {
            return Err(BuildError::Unknown(
                "native connection reopening failed; reopen the fenced session in a fresh process"
                    .into(),
            ));
        }
        if self.stopping.load(Ordering::Acquire) {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "build work stopped").into());
        }
        Ok(())
    }
    fn fixed(&self, identity: &BuildIdentity) -> Result<()> {
        identity.validate().map_err(io::Error::other)?;
        let expected = self
            .binding
            .as_ref()
            .map(|b| &b.workspace_id)
            .or(self.expected_workspace.as_ref());
        if identity.root != self.root
            || identity.connection_identity != format!("duckdb:{}", self.path.to_string_lossy())
            || expected.is_some_and(|id| id != &identity.workspace_id)
            || identity.adapter_identity.name.as_str() != "duckdb"
            || identity.adapter_identity.package_version != env!("CARGO_PKG_VERSION")
            || identity.adapter_identity.interface_version.get() != 1
            || identity.adapter_identity.binding_schema_version.get() != 1
        {
            return Err(BuildError::RequestMismatch);
        }
        Ok(())
    }
    fn journal_path(&self, attempt: &Uuid) -> PathBuf {
        let mut path = self.path.as_os_str().to_os_string();
        path.push(".grv-builds");
        PathBuf::from(path).join(format!("{}.json", attempt.as_str()))
    }
    fn reserve(&self, reservation: &Reservation) -> Result<()> {
        let path = self.journal_path(&reservation.request.identity.attempt_id);
        let directory = path.parent().unwrap();
        match fs::create_dir(directory) {
            Ok(()) => {
                fs::set_permissions(
                    directory,
                    std::os::unix::fs::PermissionsExt::from_mode(0o700),
                )?;
                File::open(directory.parent().unwrap())?.sync_all()?;
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        crate::lock::check_ancestors(&path)?;
        let metadata = fs::symlink_metadata(directory)?;
        if !metadata.is_dir()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.mode() & 0o077 != 0
        {
            return Err(invalid("build journal directory is not protected"));
        }
        let mut file=OpenOptions::new().write(true).create_new(true).mode(0o600).custom_flags(libc::O_NOFOLLOW|libc::O_CLOEXEC).open(&path).map_err(|error|if error.kind()==io::ErrorKind::AlreadyExists{BuildError::Incomplete("recorded discovery cannot be repeated; recover its original session or abandon the attempt".into())}else{error.into()})?;
        file.write_all(canonical(reservation)?.as_bytes())?;
        file.sync_all()?;
        File::open(directory)?.sync_all()?;
        Ok(())
    }
    fn query(&mut self, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
        self.check()?;
        Ok(self.engine.metadata_query(sql)?)
    }
    fn text(row: &[Option<String>], index: usize) -> Result<&str> {
        row.get(index)
            .and_then(Option::as_deref)
            .ok_or_else(|| BuildError::Unknown("incomplete build metadata".into()))
    }
    fn read_stored(&mut self, identity: &BuildIdentity) -> Result<Stored> {
        self.check()?;
        self.fixed(identity)?;
        if self.binding.is_none() {
            return Err(BuildError::Unknown(
                "build session has no established workspace binding".into(),
            ));
        }
        let rows = self
            .query(&format!(
                "SELECT record,session_id FROM _grv.build_sessions WHERE attempt_id={} LIMIT 2",
                ql(identity.attempt_id.as_str())
            ))
            .map_err(|e| {
                BuildError::Unknown(format!("build session history unavailable: {e:?}"))
            })?;
        if rows.len() != 1 {
            return Err(BuildError::Unknown(
                "original build session is absent or ambiguous".into(),
            ));
        }
        let stored: Stored = serde_json::from_str(Self::text(&rows[0], 0)?)
            .map_err(|e| BuildError::Unknown(format!("invalid build record: {e}")))?;
        stored
            .record
            .validate()
            .map_err(|e| BuildError::Unknown(e.to_string()))?;
        if Self::text(&rows[0], 1)? != stored.record.session.session_id.as_str() {
            return Err(BuildError::Unknown(
                "build history session identity differs from immutable record".into(),
            ));
        }
        if stored.record.session.identity != *identity {
            return Err(BuildError::RequestMismatch);
        }
        if stored.record.session.adapter_details != self.session_details(&stored.record.session) {
            return Err(BuildError::Unknown(
                "fixed physical input mappings differ from session metadata".into(),
            ));
        }
        Ok(stored)
    }
    fn write_stored(&mut self, stored: &Stored, insert: bool) -> Result<()> {
        stored.record.validate().map_err(io::Error::other)?;
        let data = canonical(stored)?;
        if data.len() > crate::native::METADATA_BYTES - 4096 {
            return Err(invalid(
                "build session exceeds independently bounded metadata record allowance",
            ));
        }
        let session = &stored.record.session;
        let sql = if insert {
            format!(
                "INSERT INTO _grv.build_sessions VALUES({},{},{})",
                ql(session.identity.attempt_id.as_str()),
                ql(session.session_id.as_str()),
                ql(&data)
            )
        } else {
            format!(
                "UPDATE _grv.build_sessions SET record={} WHERE attempt_id={} AND session_id={}",
                ql(&data),
                ql(session.identity.attempt_id.as_str()),
                ql(session.session_id.as_str())
            )
        };
        self.query(&sql)?;
        Ok(())
    }
    fn namespace(session: &BuildSession) -> String {
        format!(
            "_grv_session_{}",
            session.session_id.as_str().replace('-', "")
        )
    }
    fn private_input(session: &BuildSession, index: usize) -> String {
        format!(
            "{}.{}",
            qi(&Self::namespace(session)),
            qi(&format!("input_{index}"))
        )
    }
    fn session_details(&self, session: &BuildSession) -> serde_json::Value {
        serde_json::json!({
            "database":self.path,
            "input_mappings":session.inputs.iter().enumerate().map(|(index,input)|serde_json::json!({
                "alias":input.alias,
                "engine_table":format!("{}.input_{index}",Self::namespace(session)),
            })).collect::<Vec<_>>(),
            "self_mappings":session.base_contracts.iter().enumerate().map(|(index,base)|serde_json::json!({
                "table":base.table,
                "engine_table":format!("{}.self_{index}",Self::namespace(session)),
            })).collect::<Vec<_>>(),
        })
    }
    fn private_self(session: &BuildSession, index: usize) -> String {
        format!(
            "{}.{}",
            qi(&Self::namespace(session)),
            qi(&format!("self_{index}"))
        )
    }
    fn output(binding: &OutputBinding) -> String {
        let (schema, table) = binding.engine_table.split_once('.').unwrap();
        format!("{}.{}", qi(schema), qi(table))
    }
    fn authorize_private(&mut self, relation: &str) -> Result<()> {
        let (schema, table) = relation
            .split_once('.')
            .ok_or_else(|| invalid("invalid private build relation"))?;
        self.engine
            .authorize_relation(schema.trim_matches('"'), table.trim_matches('"'))?;
        Ok(())
    }
    pub fn discover(&mut self, request: DiscoverBuildRequest) -> Result<BuildDiscovery> {
        self.check()?;
        request.validate().map_err(io::Error::other)?;
        self.fixed(&request.identity)?;
        if request.options != serde_json::json!({}) {
            return Err(invalid(
                "DuckDB build options differ from pure validated options",
            ));
        }
        if self.reservation.is_some() || self.journal_path(&request.identity.attempt_id).exists() {
            return Err(BuildError::Incomplete(
                "discovery already recorded; never select fresh inputs under an existing attempt"
                    .into(),
            ));
        }
        self.query("BEGIN TRANSACTION")?;
        let result = (|| {
            let mut inputs = Vec::new();
            for input in &request.inputs {
                let relation = input
                    .relation
                    .as_str()
                    .ok_or_else(|| invalid("DuckDB build input must be a qualified relation"))?;
                let (schema, table) = relation
                    .split_once('.')
                    .ok_or_else(|| invalid("invalid build input relation"))?;
                for name in [schema, table] {
                    pull::RelationName::new(name)?;
                }
                if pull::reserved_namespace(schema) {
                    return Err(invalid("reserved build input namespace"));
                }
                let physical_view = self.engine.is_view(schema, table)?;
                if !physical_view && !self.engine.base_table(schema, table)? {
                    return Err(invalid(
                        "build input must be an existing native identity relation",
                    ));
                }
                let owner=self.query(&format!("SELECT kind,owner_scope FROM _grv.relation_ownership WHERE schema_name={} AND table_name={} LIMIT 2",ql(schema),ql(table)))?;
                if owner.len() != 1 || Self::text(&owner[0], 0)? != "identity_materialization" {
                    return Err(invalid(
                        "build input is not a complete identity materialization",
                    ));
                }
                let scope = Self::text(&owner[0], 1)?;
                let facts=self.query(&format!("SELECT table_name,grv_revision,generation_id,contract FROM _grv.pull_meta WHERE owner_scope={} AND target_table={} LIMIT 2",ql(scope),ql(table)))?;
                let checkpoint = self.query(&format!(
                    "SELECT receipt FROM _grv.pull_checkpoint WHERE owner_scope={} LIMIT 2",
                    ql(scope)
                ))?;
                if facts.len() != 1 || checkpoint.len() != 1 {
                    return Err(BuildError::Unknown(
                        "build input lost materialization facts".into(),
                    ));
                }
                let receipt: PullReceipt = serde_json::from_str(Self::text(&checkpoint[0], 0)?)
                    .map_err(io::Error::other)?;
                let committed = self.query(&format!(
                    "SELECT receipt FROM _grv.pull_attempts WHERE attempt_id={} LIMIT 2",
                    ql(receipt.identity.attempt_id.as_str())
                ))?;
                if committed != checkpoint
                    || !receipt.identity.is_identity_materialization()
                    || receipt.identity.binding.canonical_root != self.root
                    || Some(&receipt.identity.binding) != self.binding.as_ref()
                    || receipt.identity.target_schema.as_str() != schema
                    || receipt.committed_revision.get() == 0
                {
                    return Err(BuildError::Unknown(
                        "materialization lacks matching immutable identity receipt".into(),
                    ));
                }
                let expected_scope=grv_types::sha256(&grv_types::canonical_json(&serde_json::json!({"binding":receipt.identity.binding,"dataset":receipt.identity.dataset,"adapter":"duckdb","target_schema":receipt.identity.target_schema})).map_err(io::Error::other)?);
                if expected_scope.as_str() != scope {
                    return Err(BuildError::Unknown(
                        "materialization scope differs from original receipt".into(),
                    ));
                }
                let source_table =
                    Name::new(Self::text(&facts[0], 0)?).map_err(io::Error::other)?;
                let contract: TableContract =
                    serde_json::from_str(Self::text(&facts[0], 3)?).map_err(io::Error::other)?;
                let wire = receipt.wire_receipt()?;
                if Self::text(&facts[0], 1)? != receipt.committed_revision.to_string()
                    || Self::text(&facts[0], 2)? != receipt.generation_id.as_str()
                    || !wire
                        .source_contracts
                        .iter()
                        .any(|c| c.table == source_table && c.contract == contract)
                    || !wire
                        .output_contracts
                        .iter()
                        .any(|c| c.table == source_table && c.contract == contract)
                {
                    return Err(BuildError::Unknown(
                        "materialization facts disagree with immutable receipt".into(),
                    ));
                }
                if physical_view
                    != (receipt.materialization_mode == pull::MaterializationMode::S3View)
                {
                    return Err(BuildError::Unknown(
                        "materialization kind differs from immutable receipt".into(),
                    ));
                }
                let actual = self.engine.relation_schema(schema, table)?;
                let expected = contract
                    .columns
                    .iter()
                    .map(|column| {
                        let native = pull::native_type(&column.logical_type)?;
                        Ok(vec![
                            Some(column.name.clone()),
                            Some(if native == "TIMESTAMPTZ" {
                                "TIMESTAMP WITH TIME ZONE".into()
                            } else {
                                native
                            }),
                        ])
                    })
                    .collect::<io::Result<Vec<_>>>()?;
                if actual != expected {
                    return Err(BuildError::Unknown(
                        "materialization schema differs from immutable source contract".into(),
                    ));
                }
                inputs.push(InputBinding {
                    alias: input.alias.clone(),
                    relation: input.relation.clone(),
                    dataset: receipt.identity.dataset.clone(),
                    table: source_table,
                    revision: receipt.committed_revision,
                    generation_id: receipt.generation_id,
                    contract,
                    materialization: if physical_view {
                        Materialization::S3View
                    } else {
                        Materialization::Local
                    },
                });
            }
            let session_id = Uuid::v4();
            let namespace = format!("_grv_session_{}", session_id.as_str().replace('-', ""));
            let outputs = request
                .outputs
                .iter()
                .enumerate()
                .map(|(index, output)| {
                    let table = match request.execution {
                        BuildExecution::Managed => format!("output_{index}"),
                        BuildExecution::External => output
                            .source
                            .get("table")
                            .and_then(serde_json::Value::as_str)
                            .ok_or_else(|| {
                                invalid("external build source requires a table mapping")
                            })?
                            .to_owned(),
                    };
                    pull::RelationName::new(&table)?;
                    Ok(OutputBinding {
                        table: output.table.clone(),
                        source: output.source.clone(),
                        columns: output.columns.clone(),
                        engine_table: format!("{namespace}.{table}"),
                        contract: output.contract.clone(),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let discovery = BuildDiscovery {
                discovery_id: session_id,
                identity: request.identity.clone(),
                inputs,
                outputs,
            };
            discovery.validate_for(&request).map_err(io::Error::other)?;
            let reservation = Reservation {
                request,
                discovery: discovery.clone(),
            };
            self.reserve(&reservation)?;
            self.reservation = Some(reservation);
            Ok(discovery)
        })();
        if result.is_err() {
            let _ = self.engine.metadata_query("ROLLBACK");
        }
        result
    }
    fn import_s3_view(
        &mut self,
        relation: &str,
        contract: &TableContract,
        files: &[VerifiedFile],
    ) -> Result<()> {
        self.authorize_private(relation)?;
        if files.is_empty() {
            let projection = contract
                .columns
                .iter()
                .map(|column| {
                    Ok(format!(
                        "CAST(NULL AS {}) AS {}",
                        pull::native_type(&column.logical_type)?,
                        qi(&column.name)
                    ))
                })
                .collect::<io::Result<Vec<_>>>()?
                .join(",");
            self.query(&format!(
                "CREATE VIEW {relation} AS SELECT {projection} WHERE FALSE"
            ))?;
            return Ok(());
        }
        let mut projected = Vec::new();
        let mut unique = std::collections::BTreeSet::new();
        for file in files {
            self.check()?;
            if file.access != FileAccess::S3View || !unique.insert(&file.location) {
                return Err(invalid(
                    "external S3 inputs require unique exact verified S3 metadata",
                ));
            }
            let prefix = TableContract {
                columns: file.schema.columns.clone(),
                partition_keys: contract.partition_keys.clone(),
                extensions: contract.extensions.clone(),
                column_ext: serde_json::Value::Object(
                    file.schema
                        .columns
                        .iter()
                        .filter_map(|column| {
                            contract
                                .column_ext
                                .get(&column.name)
                                .map(|value| (column.name.clone(), value.clone()))
                        })
                        .collect(),
                ),
            };
            let source = SourceFile {
                path: PathBuf::new(),
                remote: Some(file.clone()),
                bytes: file.size,
                sha256: file.sha256.clone(),
                contract: prefix,
                partition: file.partition.clone(),
            };
            file.validate().map_err(io::Error::other)?;
            source.contract.validate().map_err(io::Error::other)?;
            if !file.location.starts_with(&format!("{}/", self.root))
                || source.contract.columns.len() > contract.columns.len()
                || source.contract.columns != contract.columns[..source.contract.columns.len()]
            {
                return Err(invalid(
                    "build source file is outside the fixed root or exact logical prefix",
                ));
            }
            let mut fixed = source.clone();
            fixed.path = file.location.clone().into();
            let parquet = format!(
                "read_parquet({},hive_partitioning=false,union_by_name=false)",
                ql(&crate::s3_config::sql_filename(&file.location)?)
            );
            let described = self
                .engine
                .verified_s3_metadata(file, &format!("DESCRIBE SELECT * FROM {parquet}"))?;
            if described.len() != file.schema.columns.len()
                || described
                    .iter()
                    .zip(&file.schema.columns)
                    .any(|(row, column)| {
                        let native = pull::native_type(&column.logical_type).unwrap();
                        let native = if native == "TIMESTAMP_MS" {
                            "TIMESTAMP"
                        } else if native == "TIMESTAMPTZ" {
                            "TIMESTAMP WITH TIME ZONE"
                        } else {
                            &native
                        };
                        row.first().and_then(Option::as_deref) != Some(&column.name)
                            || row.get(1).and_then(Option::as_deref) != Some(native)
                    })
            {
                return Err(invalid(
                    "physical S3 build input differs from verified logical schema",
                ));
            }
            if let Some(check) = pull::exact_value_assertion(&source.contract, &parquet, true) {
                self.engine.verified_s3_command(file, &check)?;
            }
            if let Some(check) = pull::partition_assertion(&fixed)? {
                self.engine.verified_s3_command(file,&format!("SELECT CASE WHEN ({check})=0 THEN 0 ELSE error('build file partition differs from verified metadata') END"))?;
            }
            projected.push(pull::file_projection(&fixed, contract)?);
        }
        self.engine.verified_s3_files_command(
            files,
            &format!(
                "CREATE VIEW {relation} AS {}",
                projected.join(" UNION ALL ")
            ),
        )?;
        Ok(())
    }
    fn prepared_source_query(
        &mut self,
        files: &[VerifiedFile],
        query: &str,
    ) -> Result<Vec<Vec<Option<String>>>> {
        self.check()?;
        if files.iter().any(|file| file.access == FileAccess::S3View) {
            Ok(self.engine.s3_query(files, query)?)
        } else {
            self.query(query)
        }
    }
    fn import(
        &mut self,
        relation: &str,
        contract: &TableContract,
        files: &[VerifiedFile],
        remote_views: bool,
    ) -> Result<()> {
        if remote_views {
            return self.import_s3_view(relation, contract, files);
        }
        self.query(&format!(
            "CREATE TABLE {relation}({})",
            pull::table_definition(contract)?
        ))?;
        self.authorize_private(relation)?;
        if !files.is_empty() && self.scratch == 0 {
            return Err(invalid("negotiated scratch cannot admit source hash state"));
        }
        for file in files {
            self.check()?;
            if file.access != FileAccess::Local {
                return Err(invalid(
                    "native build inputs require parent-verified local staging",
                ));
            }
            let prefix = TableContract {
                columns: file.schema.columns.clone(),
                partition_keys: contract.partition_keys.clone(),
                extensions: contract.extensions.clone(),
                column_ext: serde_json::Value::Object(
                    file.schema
                        .columns
                        .iter()
                        .filter_map(|column| {
                            contract
                                .column_ext
                                .get(&column.name)
                                .map(|value| (column.name.clone(), value.clone()))
                        })
                        .collect(),
                ),
            };
            let source = SourceFile {
                remote: None,
                path: file.location.clone().into(),
                bytes: file.size,
                sha256: file.sha256.clone(),
                contract: prefix,
                partition: file.partition.clone(),
            };
            let mut pinned =
                VerifiedSource::open(&source, Some(self.stopping.clone()), self.scratch)?;
            let descriptor = pinned.file.as_raw_fd();
            let mut fixed = source.clone();
            fixed.path = format!("/dev/fd/{descriptor}").into();
            let parquet = format!(
                "read_parquet({},hive_partitioning=false,union_by_name=false)",
                ql(fixed.path.to_str().unwrap())
            );
            let described = self
                .engine
                .verified_file_metadata(descriptor, &format!("DESCRIBE SELECT * FROM {parquet}"))?;
            if described.len() != file.schema.columns.len()
                || described
                    .iter()
                    .zip(&file.schema.columns)
                    .any(|(row, col)| {
                        let expected = pull::native_type(&col.logical_type).unwrap();
                        let expected = if expected == "TIMESTAMP_MS" {
                            "TIMESTAMP"
                        } else if expected == "TIMESTAMPTZ" {
                            "TIMESTAMP WITH TIME ZONE"
                        } else {
                            &expected
                        };
                        row.first().and_then(Option::as_deref) != Some(&col.name)
                            || row.get(1).and_then(Option::as_deref) != Some(expected)
                    })
            {
                return Err(invalid(
                    "physical build input file differs from verified logical schema",
                ));
            }
            if let Some(check) = pull::exact_value_assertion(&source.contract, &parquet, true) {
                self.engine.verified_file_command(descriptor, &check)?;
            }
            if let Some(check) = pull::partition_assertion(&fixed)? {
                self.engine.verified_file_command(descriptor,&format!("SELECT CASE WHEN ({check})=0 THEN 0 ELSE error('build file partition differs from verified metadata') END"))?;
            }
            self.engine.verified_file_command(
                descriptor,
                &format!(
                    "INSERT INTO {relation} {}",
                    pull::file_projection(&fixed, contract)?
                ),
            )?;
            pinned.recheck(&source)?;
        }
        Ok(())
    }
    fn mapped_columns(output: &OutputBinding) -> io::Result<Vec<String>> {
        crate::binding::source_columns(&ExtractTable {
            name: output.table.clone(),
            source: output.source.clone(),
            columns: output.columns.clone(),
            contract: output.contract.clone(),
        })
    }
    fn physical_contract(output: &OutputBinding) -> io::Result<TableContract> {
        use crate::conversion::{Conversion, SourceType, TickUnit};
        use arrow_schema::{DataType, TimeUnit};
        let mapped = Self::mapped_columns(output)?;
        let schema = crate::binding::output_schema(&output.contract)?;
        let source = |ty: &DataType| -> io::Result<SourceType> {
            Ok(match ty {
                DataType::Boolean => SourceType::Boolean,
                DataType::Int64 => SourceType::SignedInteger { bits: 64 },
                DataType::Float64 => SourceType::Float64,
                DataType::Utf8 => SourceType::Utf8,
                DataType::Binary => SourceType::Binary,
                DataType::Date32 => SourceType::Date32,
                DataType::Decimal128(precision, scale) => SourceType::Decimal128 {
                    precision: *precision,
                    scale: *scale as u8,
                },
                DataType::Timestamp(unit, zone) => SourceType::Timestamp {
                    unit: if zone.is_some() {
                        TickUnit::Microsecond
                    } else {
                        match unit {
                            TimeUnit::Second => TickUnit::Second,
                            TimeUnit::Millisecond => TickUnit::Millisecond,
                            TimeUnit::Microsecond => TickUnit::Microsecond,
                            TimeUnit::Nanosecond => TickUnit::Nanosecond,
                        }
                    },
                    utc: zone.is_some(),
                },
                _ => return Err(io::Error::other("unsupported client output type")),
            })
        };
        let mut physical = output.contract.clone();
        physical.columns.clear();
        physical.partition_keys.clear();
        physical.column_ext = serde_json::json!({});
        let mut seen = std::collections::BTreeSet::new();
        for (index, name) in mapped.iter().enumerate() {
            if !seen.insert(name.to_ascii_lowercase()) {
                continue;
            }
            let members: Vec<_> = mapped
                .iter()
                .enumerate()
                .filter(|(_, n)| n.eq_ignore_ascii_case(name))
                .map(|(i, _)| i)
                .collect();
            let candidate = members
                .iter()
                .find(|i| {
                    source(schema.field(**i).data_type()).is_ok_and(|source| {
                        members.iter().all(|other| {
                            Conversion::prepare(source.clone(), schema.field(*other).data_type())
                                .is_ok()
                        })
                    })
                })
                .ok_or_else(|| {
                    io::Error::other(
                        "one physical field cannot losslessly represent its requested mappings",
                    )
                })?;
            let mut column = output.contract.columns[*candidate].clone();
            column.name = name.clone();
            physical.columns.push(column);
            if let Some(extension) = output
                .contract
                .column_ext
                .get(&output.contract.columns[index].name)
            {
                physical.column_ext[name] = extension.clone();
            }
        }
        Ok(physical)
    }
    fn import_output(
        &mut self,
        session: &BuildSession,
        output: &OutputBinding,
        index: usize,
        files: &[VerifiedFile],
    ) -> Result<()> {
        if session.execution == BuildExecution::Managed {
            return self.import(&Self::output(output), &output.contract, files, false);
        }
        let physical = Self::physical_contract(output)?;
        self.import(&Self::output(output), &physical, &[], false)?;
        if files.is_empty() {
            return Ok(());
        }
        let seed = format!(
            "{}.{}",
            qi(&Self::namespace(session)),
            qi(&format!("seed_{index}"))
        );
        self.import(
            &seed,
            &output.contract,
            files,
            self.root.starts_with("s3://"),
        )?;
        self.authorize_private(&seed)?;
        let mappings = Self::mapped_columns(output)?;
        let mut projection = Vec::new();
        for column in &physical.columns {
            let members: Vec<_> = mappings
                .iter()
                .enumerate()
                .filter(|(_, n)| n.eq_ignore_ascii_case(&column.name))
                .map(|(i, _)| i)
                .collect();
            let first = qi(&output.contract.columns[members[0]].name);
            for member in members.iter().skip(1) {
                let other = qi(&output.contract.columns[*member].name);
                self.prepared_source_query(files,&format!("SELECT CASE WHEN count(*)=0 THEN 0 ELSE error('base fields disagree for a repeated physical mapping') END FROM {seed} WHERE {first} IS DISTINCT FROM {other}"))?;
            }
            projection.push(format!(
                "CAST({first} AS {}) AS {}",
                pull::native_type(&column.logical_type)?,
                qi(&column.name)
            ));
        }
        let relation = Self::output(output);
        self.authorize_private(&relation)?;
        self.prepared_source_query(
            files,
            &format!(
                "INSERT INTO {relation} SELECT {} FROM {seed}",
                projection.join(",")
            ),
        )?;
        self.query(&format!(
            "DROP {} {seed}",
            if self.root.starts_with("s3://") {
                "VIEW"
            } else {
                "TABLE"
            }
        ))?;
        Ok(())
    }
    pub fn prepare(&mut self, preparation: PrepareBuildRequest) -> Result<BuildSession> {
        self.check()?;
        preparation.validate().map_err(io::Error::other)?;
        let reservation = self
            .reservation
            .clone()
            .ok_or_else(|| BuildError::Incomplete("no live fixed discovery transaction".into()))?;
        if preparation.discovery != reservation.discovery
            || preparation.self_input != reservation.request.self_input
        {
            return Err(BuildError::RequestMismatch);
        }
        let previous_evidence = self.engine.managed_evidence_state()?;
        let mut committing = false;
        let initialize = self.binding.is_none();
        let result = (|| {
            let mut session = BuildSession {
                session_id: reservation.discovery.discovery_id.clone(),
                identity: reservation.request.identity.clone(),
                options: reservation.request.options.clone(),
                execution: reservation.request.execution,
                base_revision: preparation.base_revision,
                base_contracts: preparation.base_contracts.clone(),
                inputs: reservation.discovery.inputs.clone(),
                outputs: reservation.discovery.outputs.clone(),
                selected_outputs: reservation.request.selected_outputs.clone(),
                self_input: preparation.self_input,
                adapter_details: serde_json::json!({"database":self.path}),
            };
            session.adapter_details = self.session_details(&session);
            session
                .validate_for(&reservation.request, &preparation)
                .map_err(io::Error::other)?;
            if initialize {
                PullStore::initialize_metadata(
                    &mut self.engine,
                    &WorkspaceBinding {
                        canonical_root: self.root.clone(),
                        workspace_id: session.identity.workspace_id.clone(),
                    },
                )?;
            }
            self.query("CREATE TABLE IF NOT EXISTS _grv.build_sessions(attempt_id VARCHAR PRIMARY KEY,session_id VARCHAR UNIQUE NOT NULL,record VARCHAR NOT NULL)")?;
            let namespace = Self::namespace(&session);
            self.query(&format!("CREATE SCHEMA {}", qi(&namespace)))?;
            for (index, input) in session.inputs.iter().enumerate() {
                let group = preparation
                    .input_files
                    .iter()
                    .find(|f| f.alias == input.alias)
                    .unwrap();
                self.import(
                    &Self::private_input(&session, index),
                    &input.contract,
                    &group.files,
                    session.execution == BuildExecution::External && self.root.starts_with("s3://"),
                )?;
            }
            for (index, base) in session.base_contracts.iter().enumerate() {
                let files: Vec<_> = preparation
                    .base_files
                    .iter()
                    .filter(|f| f.table == base.table)
                    .cloned()
                    .collect();
                self.import(
                    &Self::private_self(&session, index),
                    &base.contract,
                    &files,
                    session.execution == BuildExecution::External && self.root.starts_with("s3://"),
                )?;
            }
            for (index, output) in session.outputs.iter().enumerate() {
                let seeded = session.execution == BuildExecution::External && session.self_input;
                let files: Vec<_> = if seeded {
                    preparation
                        .base_files
                        .iter()
                        .filter(|f| f.table == output.table)
                        .cloned()
                        .collect()
                } else {
                    vec![]
                };
                self.import_output(&session, output, index, &files)?;
                self.query(&format!(
                    "INSERT INTO _grv.relation_ownership VALUES({},{},'build_output',{})",
                    ql(&namespace),
                    ql(output.engine_table.split_once('.').unwrap().1),
                    ql(session.session_id.as_str())
                ))?;
            }
            let stored = Stored {
                record: BuildRecord {
                    session: session.clone(),
                    state: BuildState::Prepared,
                    completion: None,
                    completion_sha256: None,
                    candidate: None,
                    row_counts: vec![],
                    outcome: None,
                },
                invocation: None,
            };
            self.write_stored(&stored, true)?;
            self.check()?;
            self.engine.record_build_evidence()?;
            #[cfg(test)]
            if self.preparation_fault == Some(false) {
                return Err(BuildError::Incomplete(
                    "injected proved pre-commit rollback".into(),
                ));
            }
            committing = true;
            #[cfg(test)]
            if self.preparation_fault == Some(true) {
                return Err(BuildError::Unknown(
                    "injected ambiguous preparation commit".into(),
                ));
            }
            self.query("COMMIT").map_err(|e| {
                BuildError::Unknown(format!(
                    "prepared commit requires exact session recovery: {e:?}"
                ))
            })?;
            self.binding = Some(WorkspaceBinding {
                canonical_root: self.root.clone(),
                workspace_id: session.identity.workspace_id.clone(),
            });
            self.reservation = None;
            Ok(session)
        })();
        if result.is_err() {
            let rolled_back = self.engine.metadata_query("ROLLBACK").is_ok();
            if !committing && rolled_back {
                self.engine.restore_managed_evidence(previous_evidence)?;
            }
        }
        result
    }
    pub fn open_session(&mut self, identity: &BuildIdentity) -> Result<BuildRecord> {
        Ok(self.read_stored(identity)?.record)
    }
    pub(crate) fn begin_external(
        &mut self,
        session: &BuildSession,
        invocation_id: &Uuid,
        command_sha256: Digest,
    ) -> Result<Arc<crate::lock::WorkspaceLock>> {
        let mut stored = self.read_stored(&session.identity)?;
        if stored.record.session != *session || session.execution != BuildExecution::External {
            return Err(BuildError::RequestMismatch);
        }
        if stored.record.state != BuildState::Prepared
            || stored.invocation.is_some()
            || stored.record.outcome.is_some()
        {
            return Err(BuildError::Incomplete(
                "external invocation has already started; recover its completion or abandon it"
                    .into(),
            ));
        }
        stored.record.state = BuildState::Executing;
        stored.invocation = Some(Invocation {
            id: invocation_id.clone(),
            queries_sha256: command_sha256,
        });
        // Durable before spawn: failed or ambiguous creation must not execute again.
        self.write_stored(&stored, false)?;
        Ok(self.engine.workspace_reference())
    }
    pub(crate) fn validate_external_finish(
        &mut self,
        session: &BuildSession,
        invocation_id: &Uuid,
        outputs: &[CompletedOutput],
    ) -> Result<Arc<crate::lock::WorkspaceLock>> {
        let stored = self.read_stored(&session.identity)?;
        if stored.record.session != *session
            || stored.record.state != BuildState::Executing
            || stored.record.outcome.is_some()
            || stored.invocation.as_ref().map(|i| &i.id) != Some(invocation_id)
        {
            return Err(BuildError::RequestMismatch);
        }
        for output in outputs {
            let (schema, table) = output
                .engine_table
                .split_once('.')
                .ok_or_else(|| invalid("invalid external output mapping"))?;
            if !self.engine.base_table(schema, table)? {
                return Err(BuildError::Unknown(
                    "external output is absent or no longer a native base table".into(),
                ));
            }
        }
        Ok(self.engine.workspace_reference())
    }
    pub fn execute(&mut self, request: ExecuteBuildRequest) -> Result<BuildExecutionResult> {
        self.check()?;
        request.validate().map_err(io::Error::other)?;
        let mut stored = self.read_stored(&request.session.identity)?;
        if stored.record.session != request.session {
            return Err(BuildError::RequestMismatch);
        }
        if stored.record.state != BuildState::Prepared
            || stored.invocation.is_some()
            || stored.record.outcome.is_some()
        {
            return Err(BuildError::Incomplete(
                "invocation has already started; recover its original completion or abandon it"
                    .into(),
            ));
        }
        let invocation = Invocation {
            id: Uuid::v4(),
            queries_sha256: grv_types::sha256(
                &grv_types::canonical_json(&request.queries).map_err(io::Error::other)?,
            ),
        };
        stored.record.state = BuildState::Executing;
        stored.invocation = Some(invocation.clone());
        self.write_stored(&stored, false)?;
        self.query("BEGIN TRANSACTION")?;
        let executed = (|| {
            for ns in ["grv_input", "grv_self"] {
                if self.engine.schema_exists(ns)? {
                    return Err(invalid(
                        "reserved immutable build binding schema already exists",
                    ));
                }
            }
            self.query("CREATE SCHEMA grv_input")?;
            self.query("CREATE SCHEMA grv_self")?;
            for (index, input) in request.session.inputs.iter().enumerate() {
                let relation = Self::private_input(&request.session, index);
                self.authorize_private(&relation)?;
                self.query(&format!(
                    "CREATE TABLE grv_input.{} AS SELECT * FROM {relation}",
                    qi(input.alias.as_str())
                ))?;
            }
            for (index, base) in request.session.base_contracts.iter().enumerate() {
                let relation = Self::private_self(&request.session, index);
                self.authorize_private(&relation)?;
                self.query(&format!(
                    "CREATE TABLE grv_self.{} AS SELECT * FROM {relation}",
                    qi(base.table.as_str())
                ))?;
            }
            let mut counts = Vec::new();
            for (index, query) in request.queries.iter().enumerate() {
                self.check()?;
                let output = request
                    .session
                    .outputs
                    .iter()
                    .find(|o| o.table == query.table)
                    .unwrap();
                let expected = format!(
                    "SELECT {}",
                    output
                        .contract
                        .columns
                        .iter()
                        .map(|c| Ok(format!(
                            "CAST(NULL AS {}) AS {}",
                            pull::native_type(&c.logical_type)?,
                            qi(&c.name)
                        )))
                        .collect::<io::Result<Vec<_>>>()?
                        .join(",")
                );
                let stage = format!("_grv_build_output_{index}");
                let mappings = Self::mapped_columns(output)?;
                let projection = mappings
                    .iter()
                    .zip(&output.contract.columns)
                    .map(|(source, column)| format!("{} AS {}", qi(source), qi(&column.name)))
                    .collect::<Vec<_>>()
                    .join(",");
                let native = self.engine.stage_build_query(
                    &query.sql,
                    &projection,
                    &stage,
                    &expected,
                    output.contract.columns.len(),
                )?;
                let schema = crate::binding::output_schema(&output.contract)?;
                for (source, field) in native.iter().zip(schema.fields()) {
                    crate::conversion::Conversion::prepare(source.clone(), field.data_type())
                        .map_err(io::Error::other)?;
                }
                if let Some(check) = pull::exact_value_assertion(
                    &output.contract,
                    &format!("temp.main.{}", qi(&stage)),
                    false,
                ) {
                    self.engine.private_query(&check)?;
                }
                let count = self.engine.private_query(&format!(
                    "SELECT count(*)::BIGINT FROM temp.main.{}",
                    qi(&stage)
                ))?;
                let rows = Self::text(&count[0], 0)?
                    .parse::<U64>()
                    .map_err(io::Error::other)?;
                counts.push(TableCount {
                    table: query.table.clone(),
                    rows,
                });
            }
            for (index, query) in request.queries.iter().enumerate() {
                let output = request
                    .session
                    .outputs
                    .iter()
                    .find(|o| o.table == query.table)
                    .unwrap();
                let relation = Self::output(output);
                self.authorize_private(&relation)?;
                self.query(&format!("DELETE FROM {relation}"))?;
                self.engine.private_query(&format!(
                    "INSERT INTO {relation} SELECT * FROM temp.main.{}",
                    qi(&format!("_grv_build_output_{index}"))
                ))?;
                self.engine.private_query(&format!(
                    "DROP TABLE temp.main.{}",
                    qi(&format!("_grv_build_output_{index}"))
                ))?;
            }
            for input in &request.session.inputs {
                self.query(&format!(
                    "DROP TABLE grv_input.{}",
                    qi(input.alias.as_str())
                ))?;
            }
            for base in &request.session.base_contracts {
                self.query(&format!("DROP TABLE grv_self.{}", qi(base.table.as_str())))?;
            }
            self.query("DROP SCHEMA grv_input")?;
            self.query("DROP SCHEMA grv_self")?;
            self.check()?;
            #[cfg(test)]
            self.crash_at(CrashPoint::BeforeOutputCommit);
            self.query("COMMIT").map_err(|e| {
                BuildError::Unknown(format!(
                    "output commit requires fenced incomplete-session recovery: {e:?}"
                ))
            })?;
            #[cfg(test)]
            self.crash_at(CrashPoint::AfterOutputCommit);
            Ok(counts)
        })();
        if executed.is_err() {
            let _ = self.engine.metadata_query("ROLLBACK");
        }
        // This closes every invocation connection even for failed/ambiguous work.
        if let Err(error) = self.engine.close_invocation() {
            self.broken = true;
            return Err(BuildError::Unknown(format!(
                "stopped invocation reopening failed: {error}"
            )));
        }
        let counts = match executed {
            Ok(counts) => counts,
            Err(BuildError::Unknown(error)) => return Err(BuildError::Unknown(error)),
            Err(_) => {
                return Ok(BuildExecutionResult {
                    status: BuildExecutionStatus::Failed,
                    completion: None,
                    row_counts: vec![],
                });
            }
        };
        let completion = BuildCompletion {
            result_version: Req::new(1).unwrap(),
            run_id: request.session.identity.run_id.clone(),
            workspace_id: request.session.identity.workspace_id.clone(),
            declaration_sha256: request.session.identity.declaration_sha256.clone(),
            kind: if request.session.selected_outputs.is_empty() {
                CompletionKind::OmissionOnly
            } else {
                CompletionKind::Engine
            },
            invocation_id: invocation.id.as_str().into(),
            status: CompletionStatus::Succeeded,
            writers_stopped: true,
            completed_at: Timestamp::new(
                chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
            )
            .map_err(io::Error::other)?,
            completed_outputs: request
                .session
                .outputs
                .iter()
                .filter(|o| request.session.selected_outputs.contains(&o.table))
                .map(|o| CompletedOutput {
                    table: o.table.clone(),
                    engine_table: o.engine_table.clone(),
                })
                .collect(),
        };
        stored.record.state = BuildState::Completed;
        stored.record.candidate = Some(completion.clone());
        stored.record.row_counts = counts.clone();
        self.write_stored(&stored, false)?;
        #[cfg(test)]
        self.crash_at(CrashPoint::AfterCandidate);
        Ok(BuildExecutionResult {
            status: BuildExecutionStatus::Succeeded,
            completion: Some(completion),
            row_counts: counts,
        })
    }
    pub fn accept(
        &mut self,
        identity: &BuildIdentity,
        completion: BuildCompletion,
    ) -> Result<Digest> {
        let mut stored = self.read_stored(identity)?;
        completion
            .validate_for(&stored.record.session)
            .map_err(io::Error::other)?;
        if stored.record.state == BuildState::Aborted || stored.record.outcome.is_some() {
            return Err(BuildError::Incomplete(
                "terminal build cannot accept completion".into(),
            ));
        }
        if stored.record.session.execution == BuildExecution::Managed
            && stored.record.candidate.as_ref() != Some(&completion)
        {
            return Err(BuildError::RequestMismatch);
        }
        if stored.record.session.execution == BuildExecution::External
            && stored
                .invocation
                .as_ref()
                .is_some_and(|invocation| invocation.id.as_str() != completion.invocation_id)
        {
            return Err(BuildError::RequestMismatch);
        }
        if let Some(original) = &stored.record.completion {
            if original != &completion {
                return Err(BuildError::RequestMismatch);
            }
            return Ok(stored.record.completion_sha256.unwrap());
        }
        let digest = completion.digest().map_err(io::Error::other)?;
        stored.record.state = BuildState::Completed;
        stored.record.completion = Some(completion);
        stored.record.completion_sha256 = Some(digest.clone());
        self.write_stored(&stored, false)?;
        #[cfg(test)]
        self.crash_at(CrashPoint::AfterAcceptance);
        Ok(digest)
    }
    pub fn start_export(
        &mut self,
        identity: &BuildIdentity,
        digest: &Digest,
    ) -> Result<Vec<TableCount>> {
        let stored = self.read_stored(identity)?;
        if stored.record.state != BuildState::Completed
            || stored.record.outcome.is_some()
            || stored.record.completion_sha256.as_ref() != Some(digest)
        {
            return Err(BuildError::Incomplete(
                "export requires immutable accepted completion without a terminal outcome".into(),
            ));
        }
        let outputs: Vec<_> = stored
            .record
            .session
            .outputs
            .iter()
            .filter(|o| stored.record.session.selected_outputs.contains(&o.table))
            .cloned()
            .collect();
        // Reset any previous read snapshot without releasing workspace ownership.
        if let Err(error) = self.engine.close_invocation() {
            self.broken = true;
            return Err(BuildError::Unknown(format!(
                "export snapshot reopen failed: {error}"
            )));
        }
        for output in &outputs {
            let (schema, table) = output.engine_table.split_once('.').unwrap();
            if !self.engine.base_table(schema, table)? {
                return Err(BuildError::Unknown(
                    "accepted build output is not its fixed native base table".into(),
                ));
            }
        }
        let acquired = self.engine.acquire_build_outputs(
            &outputs,
            stored.record.session.execution == BuildExecution::External,
        )?;
        for (member, output) in acquired.iter().zip(&outputs) {
            let schema = crate::binding::output_schema(&output.contract)?;
            for (source, field) in member.types.iter().zip(schema.fields()) {
                crate::conversion::Conversion::prepare(source.clone(), field.data_type())
                    .map_err(io::Error::other)?;
            }
            if stored
                .record
                .row_counts
                .iter()
                .find(|c| c.table == output.table)
                .is_some_and(|c| c.rows.get() != member.rows)
            {
                return Err(BuildError::Incomplete(
                    "stable output count differs from successful invocation".into(),
                ));
            }
        }
        let counts = outputs
            .iter()
            .zip(&acquired)
            .map(|(output, member)| {
                Ok(TableCount {
                    table: output.table.clone(),
                    rows: U64::new(member.rows).map_err(io::Error::other)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        self.export = Some((acquired, outputs));
        Ok(counts)
    }
    pub fn fetch_export(
        &mut self,
        table: usize,
        resources: &Resources,
    ) -> Result<Option<(Vec<u8>, U64)>> {
        self.check()?;
        let (acquired, outputs) = self
            .export
            .as_ref()
            .ok_or_else(|| BuildError::Incomplete("no accepted export snapshot".into()))?;
        let member = acquired
            .get(table)
            .ok_or_else(|| invalid("unknown export table"))?;
        if member.rows == 0 {
            return Ok(None);
        }
        let schema = crate::binding::output_schema(&outputs[table].contract)?;
        let batch = (resources.max_batch_bytes.get() as usize).min(crate::MAX_BATCH_BYTES);
        let scratch = (resources.max_scratch_bytes.get() as usize).min(crate::SCRATCH_BUDGET_BYTES);
        let source =
            (resources.max_source_unit_bytes.get() as usize).min(crate::SOURCE_BUDGET_BYTES);
        let allowance = crate::ipc::native_allowance(&schema, &member.types, batch, scratch)
            .map_err(BuildError::Engine)?;
        self.engine
            .fetch_acquired(member, allowance, source)?
            .map(|window| {
                let bytes = crate::ipc::encode(&window, schema, batch, scratch)?;
                Ok((
                    bytes,
                    U64::new(window.row_count()).map_err(io::Error::other)?,
                ))
            })
            .transpose()
    }
    pub fn finish_export(&mut self) -> Result<()> {
        self.export = None;
        if let Err(error) = self.engine.close_invocation() {
            self.broken = true;
            return Err(BuildError::Unknown(format!("export close failed: {error}")));
        }
        Ok(())
    }
    pub fn record_outcome(
        &mut self,
        identity: &BuildIdentity,
        outcome: BuildOutcome,
    ) -> Result<()> {
        outcome.validate().map_err(io::Error::other)?;
        let mut stored = self.read_stored(identity)?;
        if stored
            .record
            .outcome
            .as_ref()
            .is_some_and(|old| old != &outcome)
        {
            return Err(BuildError::RequestMismatch);
        }
        stored.record.outcome = Some(outcome);
        self.write_stored(&stored, false)
    }
    pub fn abort(&mut self, identity: &BuildIdentity) -> Result<()> {
        let mut stored = self.read_stored(identity)?;
        if stored
            .record
            .outcome
            .as_ref()
            .is_some_and(|o| o.kind != OutcomeKind::Aborted)
        {
            return Err(BuildError::Conflict(
                "abort cannot undo known publication".into(),
            ));
        }
        stored.record.state = BuildState::Aborted;
        self.write_stored(&stored, false)
    }
    fn drop_private_relation(&mut self, relation: &str) -> Result<()> {
        let (schema, table) = relation
            .split_once('.')
            .ok_or_else(|| invalid("invalid private relation mapping"))?;
        let kind = if self
            .engine
            .is_view(schema.trim_matches('"'), table.trim_matches('"'))?
        {
            "VIEW"
        } else {
            "TABLE"
        };
        self.query(&format!("DROP {kind} IF EXISTS {relation}"))?;
        Ok(())
    }
    pub fn cleanup(&mut self, identity: &BuildIdentity) -> Result<()> {
        let stored = self.read_stored(identity)?;
        if stored.record.outcome.is_none() {
            return Err(invalid("cleanup requires confirmed terminal outcome"));
        }
        self.query("BEGIN TRANSACTION")?;
        let result = (|| {
            let session = &stored.record.session;
            for (index, _) in session.inputs.iter().enumerate() {
                self.drop_private_relation(&Self::private_input(session, index))?;
            }
            for (index, _) in session.base_contracts.iter().enumerate() {
                self.drop_private_relation(&Self::private_self(session, index))?;
            }
            for output in &session.outputs {
                self.drop_private_relation(&Self::output(output))?;
            }
            self.query(&format!(
                "DROP SCHEMA IF EXISTS {}",
                qi(&Self::namespace(session))
            ))?;
            self.query("COMMIT")?;
            Ok(())
        })();
        if result.is_err() {
            let _ = self.engine.metadata_query("ROLLBACK");
        }
        result
    }
}
impl Drop for BuildStore {
    fn drop(&mut self) {
        if self.reservation.is_some() && !self.broken {
            let _ = self.engine.metadata_query("ROLLBACK");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn name(s: &str) -> Name {
        Name::new(s).unwrap()
    }
    fn fixed(path: &Path) -> DiscoverBuildRequest {
        DiscoverBuildRequest {
            identity: BuildIdentity {
                attempt_id: Uuid::v4(),
                root: "file:///private/grv-build-crash".into(),
                dataset: name("product"),
                run_id: RunId::new("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
                workspace_id: Uuid::v4(),
                declaration_sha256: grv_types::sha256(b"crash build"),
                adapter_identity: AdapterIdentity {
                    name: name("duckdb"),
                    package_version: env!("CARGO_PKG_VERSION").into(),
                    interface_version: Req::new(1).unwrap(),
                    binding_schema_version: Req::new(1).unwrap(),
                },
                connection_identity: format!("duckdb:{}", path.display()),
            },
            options: json!({}),
            execution: BuildExecution::Managed,
            inputs: vec![],
            outputs: vec![BuildOutput {
                table: name("rows"),
                source: json!({"sql":"SELECT 7::BIGINT AS id"}),
                columns: json!([{"name":"id","type":"int64"}]),
                contract: TableContract {
                    columns: vec![Column {
                        name: "id".into(),
                        logical_type: json!("int64"),
                    }],
                    partition_keys: vec![],
                    extensions: json!({}),
                    column_ext: json!({}),
                },
            }],
            selected_outputs: vec![name("rows")],
            self_input: false,
        }
    }
    fn request(session: BuildSession) -> ExecuteBuildRequest {
        ExecuteBuildRequest {
            queries: vec![BuildQuery {
                table: name("rows"),
                sql: "SELECT 7::BIGINT AS id".into(),
            }],
            session,
        }
    }
    fn preparation(store: &mut BuildStore, fixed: DiscoverBuildRequest) -> PrepareBuildRequest {
        PrepareBuildRequest {
            discovery: store.discover(fixed).unwrap(),
            base_revision: U64::new(0).unwrap(),
            self_input: false,
            base_contracts: vec![],
            base_files: vec![],
            input_files: vec![],
            holds_confirmed: true,
        }
    }
    #[test]
    fn empty_external_s3_self_inputs_are_private_views_and_cleanup_needs_no_reader() {
        let directory = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR")).unwrap();
        let path = directory.path().join("external.duckdb");
        let mut fixed = fixed(&path);
        fixed.identity.root = "s3://test-bucket/empty-build".into();
        fixed.execution = BuildExecution::External;
        fixed.self_input = true;
        fixed.outputs[0].source = json!({"table":"rows"});
        let mut store = BuildStore::open(
            &path,
            fixed.identity.root.clone(),
            Some(fixed.identity.workspace_id.clone()),
            &Resources::default(),
        )
        .unwrap();
        let preparation = PrepareBuildRequest {
            discovery: store.discover(fixed.clone()).unwrap(),
            base_revision: U64::new(1).unwrap(),
            self_input: true,
            base_contracts: vec![NamedContract {
                table: name("rows"),
                contract: fixed.outputs[0].contract.clone(),
            }],
            base_files: vec![],
            input_files: vec![],
            holds_confirmed: true,
        };
        let session = store.prepare(preparation).unwrap();
        let namespace = BuildStore::namespace(&session);
        assert!(store.engine.is_view(&namespace, "self_0").unwrap());
        assert!(store.engine.base_table(&namespace, "rows").unwrap());
        store.abort(&session.identity).unwrap();
        store
            .record_outcome(
                &session.identity,
                BuildOutcome {
                    kind: OutcomeKind::Aborted,
                    revision: None,
                    operation_id: None,
                },
            )
            .unwrap();
        store.cleanup(&session.identity).unwrap();
        assert!(!store.engine.schema_exists(&namespace).unwrap());
        let recorded = store.open_session(&session.identity).unwrap();
        assert_eq!(recorded.state, BuildState::Aborted);
        assert!(recorded.outcome.is_some());
    }
    #[test]
    fn build_history_witness_refuses_deleted_or_emptied_session_store() {
        use std::os::unix::fs::PermissionsExt;
        fn after_close<T>(mut reopen: impl FnMut() -> Result<T>) -> Result<T> {
            // Parallel crash-probe tests can fork while this owner is closing.
            // Their CLOEXEC copy retains the description only until exec. The
            // production API correctly reports Busy; this history test waits
            // briefly for that unrelated child instead of asserting on Busy.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
            loop {
                match reopen() {
                    Err(BuildError::Engine(error))
                        if error.kind() == io::ErrorKind::WouldBlock
                            && std::time::Instant::now() < deadline =>
                    {
                        std::thread::sleep(std::time::Duration::from_millis(2));
                    }
                    outcome => return outcome,
                }
            }
        }
        for damage in [
            "DROP TABLE _grv.build_sessions",
            "DELETE FROM _grv.build_sessions",
        ] {
            let dir = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR")).unwrap();
            fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
            let path = dir.path().join("build.duckdb");
            let fixed = fixed(&path);
            let mut store = BuildStore::open(
                &path,
                fixed.identity.root.clone(),
                Some(fixed.identity.workspace_id.clone()),
                &Resources::default(),
            )
            .unwrap();
            let prep = preparation(&mut store, fixed.clone());
            store.prepare(prep).unwrap();
            assert!(store.engine.build_evidence_recorded().unwrap());
            // Later pulls cannot downgrade the build-history witness.
            store.engine.record_managed_evidence(true).unwrap();
            assert!(store.engine.build_evidence_recorded().unwrap());
            drop(store);
            {
                let mut engine =
                    after_close(|| NativeEngine::open(&path).map_err(BuildError::from)).unwrap();
                engine.metadata_query(damage).unwrap();
            }
            let before = fs::read(&path).unwrap();
            assert!(matches!(
                after_close(|| BuildStore::open(
                    &path,
                    fixed.identity.root.clone(),
                    Some(fixed.identity.workspace_id.clone()),
                    &Resources::default()
                )),
                Err(BuildError::Unknown(_))
            ));
            assert_eq!(fs::read(&path).unwrap(), before);
        }
    }
    #[test]
    fn preparation_marker_restores_exact_previous_state_only_for_proved_rollback() {
        use crate::lock::ManagedEvidence;
        use std::os::unix::fs::PermissionsExt;
        for previous in [
            ManagedEvidence::Absent,
            ManagedEvidence::Managed,
            ManagedEvidence::BuildHistory,
        ] {
            for ambiguous in [false, true] {
                let dir = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR")).unwrap();
                fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
                let path = dir.path().join("build.duckdb");
                let mut fixed = fixed(&path);
                let mut store = BuildStore::open(
                    &path,
                    fixed.identity.root.clone(),
                    Some(fixed.identity.workspace_id.clone()),
                    &Resources::default(),
                )
                .unwrap();
                if previous != ManagedEvidence::Absent {
                    let prep = preparation(&mut store, fixed.clone());
                    store.prepare(prep).unwrap();
                    store.engine.restore_managed_evidence(previous).unwrap();
                    fixed.identity.attempt_id = Uuid::v4();
                    fixed.identity.run_id = RunId::new("01ARZ3NDEKTSV4RRFFQ69G5FAX").unwrap();
                }
                let prep = preparation(&mut store, fixed);
                store.preparation_fault = Some(ambiguous);
                let result = store.prepare(prep);
                assert!(if ambiguous {
                    matches!(result, Err(BuildError::Unknown(_)))
                } else {
                    matches!(result, Err(BuildError::Incomplete(_)))
                });
                assert_eq!(
                    store.engine.managed_evidence_state().unwrap(),
                    if ambiguous {
                        ManagedEvidence::BuildHistory
                    } else {
                        previous
                    }
                );
            }
        }
    }
    #[test]
    fn native_build_crash_probe() {
        let Ok(path) = std::env::var("GRV_TEST_BUILD_EXECUTION") else {
            return;
        };
        let execution: ExecuteBuildRequest =
            serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        let path = PathBuf::from(std::env::var("GRV_TEST_BUILD_DATABASE").unwrap());
        let point = match std::env::var("GRV_TEST_BUILD_POINT").unwrap().as_str() {
            "before-output" => CrashPoint::BeforeOutputCommit,
            "after-output" => CrashPoint::AfterOutputCommit,
            "after-candidate" => CrashPoint::AfterCandidate,
            "after-acceptance" => CrashPoint::AfterAcceptance,
            _ => panic!("point"),
        };
        let identity = execution.session.identity.clone();
        let mut store = BuildStore::open(
            &path,
            identity.root.clone(),
            Some(identity.workspace_id.clone()),
            &Resources::default(),
        )
        .unwrap();
        store.crash = Some(point);
        let result = store.execute(execution).unwrap();
        if point == CrashPoint::AfterAcceptance {
            store.accept(&identity, result.completion.unwrap()).unwrap();
        }
        panic!("crash hook did not execute");
    }
    #[test]
    fn native_crashes_preserve_only_closed_durable_candidates_and_accepted_completions() {
        use std::os::unix::fs::PermissionsExt;
        for point in [
            "before-output",
            "after-output",
            "after-candidate",
            "after-acceptance",
        ] {
            let dir = tempfile::Builder::new()
                .prefix(".native-build-crash-")
                .tempdir_in(env!("CARGO_MANIFEST_DIR"))
                .unwrap();
            fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
            let path = dir.path().join("build.duckdb");
            let fixed = fixed(&path);
            let identity = fixed.identity.clone();
            let mut store = BuildStore::open(
                &path,
                identity.root.clone(),
                Some(identity.workspace_id.clone()),
                &Resources::default(),
            )
            .unwrap();
            let discovery = store.discover(fixed).unwrap();
            let session = store
                .prepare(PrepareBuildRequest {
                    discovery,
                    base_revision: U64::new(0).unwrap(),
                    self_input: false,
                    base_contracts: vec![],
                    base_files: vec![],
                    input_files: vec![],
                    holds_confirmed: true,
                })
                .unwrap();
            drop(store);
            let execution = request(session);
            let execution_path = dir.path().join("execution.json");
            fs::write(&execution_path, serde_json::to_vec(&execution).unwrap()).unwrap();
            let result = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "build::tests::native_build_crash_probe",
                    "--nocapture",
                ])
                .env("GRV_TEST_BUILD_EXECUTION", &execution_path)
                .env("GRV_TEST_BUILD_DATABASE", &path)
                .env("GRV_TEST_BUILD_POINT", point)
                .output()
                .unwrap();
            assert_eq!(
                result.status.code(),
                Some(86),
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
            let mut store = BuildStore::open(
                &path,
                identity.root.clone(),
                Some(identity.workspace_id.clone()),
                &Resources::default(),
            )
            .unwrap();
            let record = store.open_session(&identity).unwrap();
            assert!(store.execute(execution).is_err());
            if point == "before-output" || point == "after-output" {
                assert_eq!(record.state, BuildState::Executing);
                assert!(record.candidate.is_none());
                assert!(record.completion.is_none());
                assert!(
                    store
                        .start_export(&identity, &grv_types::sha256(b"unknown"))
                        .is_err()
                );
            } else {
                assert_eq!(record.state, BuildState::Completed);
                let candidate = record.candidate.unwrap();
                assert!(candidate.writers_stopped);
                if point == "after-candidate" {
                    assert!(record.completion.is_none());
                } else {
                    assert_eq!(record.completion, Some(candidate.clone()));
                }
                let digest = store.accept(&identity, candidate.clone()).unwrap();
                assert_eq!(digest, candidate.digest().unwrap());
                assert_eq!(
                    store.start_export(&identity, &digest).unwrap()[0]
                        .rows
                        .get(),
                    1
                );
                assert!(
                    store
                        .fetch_export(0, &Resources::default())
                        .unwrap()
                        .is_some()
                );
            }
        }
    }
}
