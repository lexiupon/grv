//! One native owner reads durable engine metadata in a read-only transaction.
//! Declared SQL, source rows, credentials and GRV authority never enter this path.
use crate::{
    binding,
    extraction::error,
    native::{NativeEngine, NativeInterrupt},
    pull::{PullReceipt, WorkspaceBinding},
    worker::{Engine, Interrupt},
};
use grv_adapter_api::{
    BindingState, ConnectionLocator, Handle, InspectConnectionRequest, Resources,
};
use grv_adapter_sdk::{BoundConnection, StopToken};
use grv_types::{ErrorCode, Name, Uuid};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    path::{Path, PathBuf},
    sync::mpsc,
    thread,
    time::Duration,
};
type Result<T> = grv_adapter_sdk::Result<T>;
#[allow(clippy::result_large_err)]
fn failure(e: io::Error) -> grv_types::PublicError {
    error(
        if e.kind() == io::ErrorKind::WouldBlock {
            ErrorCode::EngineBusy
        } else {
            ErrorCode::OutcomeUnknown
        },
        format!("connection inspection unavailable: {e}"),
    )
}
#[allow(clippy::result_large_err)]
fn unknown(message: &str) -> grv_types::PublicError {
    error(ErrorCode::OutcomeUnknown, message)
}
fn decode<T: DeserializeOwned>(text: &str) -> Result<T> {
    let value = grv_adapter_wire::json::parse(text.as_bytes())
        .map_err(|_| unknown("invalid durable engine JSON"))?;
    serde_json::from_value(value).map_err(|_| unknown("invalid closed engine metadata record"))
}
fn text(row: &[Option<String>], index: usize) -> Result<&str> {
    row.get(index)
        .and_then(Option::as_deref)
        .ok_or_else(|| unknown("incomplete engine metadata"))
}
fn query(engine: &mut NativeEngine, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
    engine.metadata_query(sql).map_err(failure)
}
fn require_table(engine: &mut NativeEngine, name: &str) -> Result<()> {
    if !engine.base_table("_grv", name).map_err(failure)? {
        return Err(unknown(
            "engine metadata must be an existing native base table",
        ));
    }
    Ok(())
}
fn binding_at(engine: &mut NativeEngine, root: Option<&str>) -> Result<Option<WorkspaceBinding>> {
    if !engine.managed_initialized().map_err(failure)? {
        if engine.managed_evidence_recorded().map_err(failure)? {
            return Err(unknown("managed workspace lost its engine metadata"));
        }
        return Ok(None);
    }
    for table in ["metadata_header", "workspace_binding"] {
        require_table(engine, table)?;
    }
    for (sql, expected) in [
        (
            "DESCRIBE SELECT version,ownership_version FROM _grv.metadata_header",
            vec![("version", "BIGINT"), ("ownership_version", "BIGINT")],
        ),
        (
            "DESCRIBE SELECT identity FROM _grv.workspace_binding",
            vec![("identity", "VARCHAR")],
        ),
    ] {
        let rows = query(engine, sql)?;
        if rows.len() != expected.len()
            || rows.iter().zip(expected).any(|(row, (name, datatype))| {
                row.first().and_then(Option::as_deref) != Some(name)
                    || row.get(1).and_then(Option::as_deref) != Some(datatype)
            })
        {
            return Err(unknown("unrecognized binding metadata physical schema"));
        }
    }
    if query(
        engine,
        "SELECT version,ownership_version FROM _grv.metadata_header LIMIT 2",
    )? != vec![vec![Some("1".into()), Some("1".into())]]
    {
        return Err(unknown("unrecognized engine metadata header"));
    }
    let rows = query(
        engine,
        "SELECT identity FROM _grv.workspace_binding LIMIT 2",
    )?;
    if rows.len() != 1 {
        return Err(unknown("ambiguous workspace binding"));
    }
    let binding: WorkspaceBinding = decode(text(&rows[0], 0)?)?;
    if binding.canonical_root.is_empty() {
        return Err(unknown("empty workspace root identity"));
    }
    if root.is_some_and(|root| binding.canonical_root != root) {
        return Err(error(
            ErrorCode::StateConflict,
            "workspace belongs to another GRV root",
        ));
    }
    Ok(Some(binding))
}
fn scope(receipt: &PullReceipt) -> Result<String> {
    Ok(grv_types::sha256(&grv_types::canonical_json(&json!({"binding":receipt.identity.binding,"dataset":receipt.identity.dataset,"adapter":"duckdb","target_schema":receipt.identity.target_schema})).map_err(|_|unknown("invalid scope identity"))?).to_string())
}
fn checked_receipt(
    engine: &mut NativeEngine,
    bytes: &str,
    binding: &WorkspaceBinding,
) -> Result<PullReceipt> {
    let receipt: PullReceipt = decode(bytes)?;
    receipt.identity.validate().map_err(failure)?;
    if &receipt.identity.binding != binding {
        return Err(unknown("checkpoint receipt belongs to another workspace"));
    }
    receipt.wire_receipt().map_err(failure)?;
    let rows = query(
        engine,
        &format!(
            "SELECT receipt FROM _grv.pull_attempts WHERE attempt_id={} LIMIT 2",
            crate::pull::quote_literal(receipt.identity.attempt_id.as_str())
        ),
    )?;
    if rows.len() != 1 || decode::<PullReceipt>(text(&rows[0], 0)?)? != receipt {
        return Err(unknown(
            "checkpoint lost its matching immutable attempt receipt",
        ));
    }
    Ok(receipt)
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredSession {
    record: grv_adapter_api::BuildRecord,
    #[serde(rename = "invocation")]
    _invocation: Option<StoredInvocation>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredInvocation {
    #[serde(rename = "id")]
    _id: Uuid,
    #[serde(rename = "queries_sha256")]
    _queries_sha256: grv_types::Digest,
}

fn observe(
    engine: &mut NativeEngine,
    path: &Path,
    binding: Option<&WorkspaceBinding>,
    request: &InspectConnectionRequest,
    resources: &Resources,
) -> Result<Value> {
    request
        .validate()
        .map_err(|_| error(ErrorCode::InvalidArgument, "invalid inspection request"))?;
    let dataset = request
        .declaration
        .as_ref()
        .map(|declaration| {
            declaration
                .get("dataset")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    error(
                        ErrorCode::InvalidDeclaration,
                        "inspection declaration requires dataset",
                    )
                })
                .and_then(|name| {
                    Name::new(name).map_err(|_| {
                        error(ErrorCode::InvalidDeclaration, "invalid inspection dataset")
                    })
                })
        })
        .transpose()?;
    if let Some(declaration) = &request.declaration {
        let canonical = binding::locate(declaration["connection"].clone()).map_err(failure)?;
        if canonical.engine_path.as_deref() != path.to_str() {
            return Err(error(
                ErrorCode::RequestMismatch,
                "inspection declaration names another engine",
            ));
        }
    }
    if let Some(binding) = binding
        && request
            .root
            .as_ref()
            .is_some_and(|root| root != &binding.canonical_root)
    {
        return Err(error(
            ErrorCode::StateConflict,
            "inspection root differs from established binding",
        ));
    }
    let mut result = json!({"path":path,"workspace_id":binding.map(|b|&b.workspace_id),"binding":if binding.is_some(){"bound"}else{"uninitialized"},"materializations":[],"imports":[],"sessions":[]});
    let target_schema = request
        .declaration
        .as_ref()
        .and_then(|declaration| declaration.get("target"))
        .and_then(|target| target.get("schema"))
        .and_then(Value::as_str);
    let Some(binding) = binding else {
        return Ok(result);
    };
    for table in [
        "pull_checkpoint",
        "pull_meta",
        "pull_attempts",
        "relation_ownership",
        "import_attempt_details",
    ] {
        require_table(engine, table)?;
    }
    // The reply and one bounded native result occupy separate negotiated
    // metadata windows. Refuse oversized observations rather than truncate.
    let allowance = resources
        .max_scratch_bytes
        .get()
        .min(resources.max_source_unit_bytes.get()) as usize;
    let mut summaries = 0usize;
    for (table, key, column, category) in [
        (
            "pull_checkpoint",
            "owner_scope",
            "receipt",
            "materializations",
        ),
        ("import_attempt_details", "attempt_id", "receipt", "imports"),
        ("build_sessions", "attempt_id", "record", "sessions"),
    ] {
        if table == "build_sessions" {
            let recorded = engine.build_evidence_recorded().map_err(failure)?;
            if !engine.relation_exists("_grv", table).map_err(failure)? {
                if recorded {
                    return Err(unknown("workspace lost durable build history"));
                }
                continue;
            }
            require_table(engine, table)?;
            if recorded
                && query(engine, "SELECT attempt_id FROM _grv.build_sessions LIMIT 1")?.is_empty()
            {
                return Err(unknown("workspace lost durable build history"));
            }
        }
        require_table(engine, table)?;
        let mut after = String::new();
        loop {
            let rows = query(
                engine,
                &format!(
                    "SELECT {key},{column} FROM _grv.{table} WHERE {key}>{} ORDER BY {key} LIMIT 1",
                    crate::pull::quote_literal(&after)
                ),
            )?;
            if rows.is_empty() {
                break;
            }
            after = text(&rows[0], 0)?.into();
            let summary = if category == "sessions" {
                let stored: StoredSession = decode(text(&rows[0], 1)?)?;
                let record = stored.record;
                record
                    .validate()
                    .map_err(|_| unknown("invalid durable build session"))?;
                let session = &record.session;
                if session.identity.root != binding.canonical_root
                    || session.identity.workspace_id != binding.workspace_id
                    || session.identity.attempt_id.as_str() != after
                {
                    return Err(unknown(
                        "build session identity differs from its workspace or key",
                    ));
                }
                if dataset
                    .as_ref()
                    .is_some_and(|dataset| dataset != &session.identity.dataset)
                {
                    continue;
                }
                let inputs: BTreeSet<_> = session
                    .inputs
                    .iter()
                    .map(|i| (&i.dataset, i.revision.to_string()))
                    .collect();
                json!({"dataset":session.identity.dataset,"run_id":session.identity.run_id,"attempt_id":session.identity.attempt_id,"session_id":session.session_id,"workspace_id":session.identity.workspace_id,"base_revision":session.base_revision,"input_revisions":inputs.into_iter().map(|(dataset,revision)|json!({"dataset":dataset,"revision":revision})).collect::<Vec<_>>(),"state":record.state,"completion_sha256":record.completion_sha256,"outcome":record.outcome})
            } else {
                let receipt = checked_receipt(engine, text(&rows[0], 1)?, binding)?;
                if dataset
                    .as_ref()
                    .is_some_and(|dataset| dataset != &receipt.identity.dataset)
                {
                    continue;
                }
                if target_schema
                    .is_some_and(|schema| schema != receipt.identity.target_schema.as_str())
                {
                    continue;
                }
                if category == "materializations" {
                    if !receipt.identity.is_identity_materialization() || after != scope(&receipt)?
                    {
                        return Err(unknown(
                            "identity checkpoint has inconsistent role or scope",
                        ));
                    }
                    let mut mappings = Vec::new();
                    let targets = receipt
                        .table_targets
                        .as_object()
                        .ok_or_else(|| unknown("invalid materialization mappings"))?;
                    let rows = query(
                        engine,
                        &format!(
                            "SELECT table_name,target_table,grv_revision,generation_id,contract FROM _grv.pull_meta WHERE owner_scope={} ORDER BY table_name",
                            crate::pull::quote_literal(&after)
                        ),
                    )?;
                    if rows.len() != targets.len() {
                        return Err(unknown(
                            "materialization mapping coverage differs from checkpoint",
                        ));
                    }
                    let contracts = receipt
                        .output_contracts
                        .as_array()
                        .ok_or_else(|| unknown("invalid checkpoint contracts"))?;
                    for row in rows {
                        let name = text(&row, 0)?;
                        let target = text(&row, 1)?;
                        if targets.get(name).and_then(Value::as_str) != Some(target)
                            || text(&row, 2)? != receipt.committed_revision.to_string()
                            || text(&row, 3)? != receipt.generation_id.as_str()
                            || !contracts.iter().any(|c| {
                                c[0] == name
                                    && decode::<grv_types::TableContract>(
                                        text(&row, 4).unwrap_or(""),
                                    )
                                    .is_ok_and(|contract| {
                                        serde_json::to_value(contract)
                                            .is_ok_and(|value| value == c[1])
                                    })
                            })
                        {
                            return Err(unknown(
                                "materialization mappings differ from immutable checkpoint",
                            ));
                        }
                        let owned = query(
                            engine,
                            &format!(
                                "SELECT kind,owner_scope FROM _grv.relation_ownership WHERE schema_name={} AND table_name={} LIMIT 2",
                                crate::pull::quote_literal(receipt.identity.target_schema.as_str()),
                                crate::pull::quote_literal(target)
                            ),
                        )?;
                        if owned
                            != vec![vec![
                                Some("identity_materialization".into()),
                                Some(after.clone()),
                            ]]
                            || !engine
                                .relation_exists(receipt.identity.target_schema.as_str(), target)
                                .map_err(failure)?
                        {
                            return Err(unknown(
                                "materialization lost matching relation ownership",
                            ));
                        }
                        let schema = receipt.identity.target_schema.as_str();
                        let view = engine.is_view(schema, target).map_err(failure)?;
                        if view
                            != (receipt.materialization_mode
                                == crate::pull::MaterializationMode::S3View)
                            || (!view && !engine.base_table(schema, target).map_err(failure)?)
                        {
                            return Err(unknown(
                                "materialization kind differs from immutable receipt",
                            ));
                        }
                        let contract: grv_types::TableContract = decode(text(&row, 4)?)?;
                        let expected = contract
                            .columns
                            .iter()
                            .map(|column| {
                                let native = crate::pull::native_type(&column.logical_type)
                                    .map_err(failure)?;
                                Ok(vec![
                                    Some(column.name.clone()),
                                    Some(if native == "TIMESTAMPTZ" {
                                        "TIMESTAMP WITH TIME ZONE".into()
                                    } else {
                                        native
                                    }),
                                ])
                            })
                            .collect::<Result<Vec<_>>>()?;
                        if engine.relation_schema(schema, target).map_err(failure)? != expected {
                            return Err(unknown(
                                "materialization schema differs from immutable receipt",
                            ));
                        }
                        mappings.push(json!({"grv_table":name,"target_table":format!("{}.{}",receipt.identity.target_schema.as_str(),target)}));
                    }
                    json!({"dataset":receipt.identity.dataset,"target_schema":receipt.identity.target_schema,"revision_mode":if matches!(receipt.identity.requested_revision,grv_types::RequestedRevision::Latest(_)){"latest"}else{"fixed"},"materialization_mode":receipt.materialization_mode,"committed_revision":receipt.committed_revision,"generation_id":receipt.generation_id,"mappings":mappings,"adapter":"duckdb","write_mode":"replace","transform_mode":"identity"})
                } else {
                    if after != receipt.identity.attempt_id.as_str()
                        || receipt.identity.is_identity_materialization()
                    {
                        return Err(unknown("import attempt role or identity differs"));
                    }
                    json!({"dataset":receipt.identity.dataset,"target_schema":receipt.identity.target_schema,"write_mode":receipt.identity.write_mode,"transform_mode":receipt.identity.transform_mode,"attempt_id":receipt.identity.attempt_id,"source_revision":receipt.committed_revision,"declaration_sha256":receipt.request_record.as_ref().unwrap().declaration_sha256,"pulled_at":receipt.pulled_at,"scope_mode":receipt.identity.scope_mode})
                }
            };
            summaries += 1;
            result[category].as_array_mut().unwrap().push(summary);
            if summaries > 4096
                || grv_types::canonical_json(&result)
                    .map_err(|_| unknown("invalid inspection summary"))?
                    .len()
                    > allowance.saturating_sub(crate::native::METADATA_BYTES) / 4
            {
                return Err(error(
                    ErrorCode::UnsupportedCapability,
                    "inspection metadata exceeds negotiated bounded workspace",
                ));
            }
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    fn fixture() -> tempfile::TempDir {
        let root = tempfile::tempdir_in(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
                .parent()
                .unwrap(),
        )
        .unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        root
    }
    #[test]
    fn inspection_refuses_small_metadata_budgets_before_acquiring_engine() {
        let root = fixture();
        let path = root.path().join("db.duckdb");
        drop(NativeEngine::open(&path).unwrap());
        let before = std::fs::read(&path).unwrap();
        let locator = binding::locate(json!({"database":path})).unwrap();
        // The existing owner proves the budget failure precedes engine access.
        let owner = NativeEngine::open_readonly(&path).unwrap();
        for source in [true, false] {
            let mut resources = Resources::default();
            if source {
                resources.max_source_unit_bytes = grv_types::U64::new(1).unwrap();
            } else {
                resources.max_scratch_bytes = grv_types::U64::new(1).unwrap();
            }
            let mut runtime = Runtime::default();
            assert_eq!(
                runtime
                    .bind(
                        locator.clone(),
                        None,
                        locator.identity.clone(),
                        None,
                        &resources
                    )
                    .unwrap_err()
                    .code,
                ErrorCode::UnsupportedCapability
            );
        }
        drop(owner);
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }
    #[test]
    fn readonly_inspection_never_initializes_or_executes_declared_sql_and_drop_releases_owner() {
        let root = fixture();
        let path = root.path().join("db.duckdb");
        drop(NativeEngine::open(&path).unwrap());
        let before = std::fs::read(&path).unwrap();
        let mut worker = Worker::open(
            path.clone(),
            Some("file:///root".into()),
            Resources::default(),
        )
        .unwrap();
        assert!(worker.binding.is_none());
        assert_eq!(
            NativeEngine::open_readonly(&path).err().unwrap().kind(),
            io::ErrorKind::WouldBlock
        );
        worker.sender.as_ref().unwrap().send(InspectConnectionRequest{root:Some("file:///root".into()),declaration:Some(json!({"dataset":"data","connection":{"database":path},"tables":[{"select":{"sql":"COPY (SELECT 1) TO '/never/execute'"}}]}))}).unwrap();
        let details = worker.receiver.recv().unwrap().unwrap();
        assert_eq!(details["binding"], "uninitialized");
        for field in ["materializations", "imports", "sessions"] {
            assert_eq!(details[field], json!([]));
        }
        worker.stop().unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), before);
        let mut engine = NativeEngine::open_readonly(&path).unwrap();
        assert!(!engine.managed_initialized().unwrap());
    }
    #[test]
    fn inspection_refuses_lost_build_history_without_mutation() {
        for damage in [
            "DROP TABLE _grv.build_sessions",
            "DELETE FROM _grv.build_sessions",
        ] {
            let root = fixture();
            let path = root.path().join("db.duckdb");
            let binding = WorkspaceBinding {
                canonical_root: "file:///root".into(),
                workspace_id: Uuid::v4(),
            };
            {
                let mut engine = NativeEngine::open(&path).unwrap();
                crate::pull::PullStore::initialize_metadata(&mut engine, &binding).unwrap();
                engine.metadata_query("CREATE TABLE _grv.build_sessions(attempt_id VARCHAR,session_id VARCHAR,record VARCHAR)").unwrap();
                engine.record_build_evidence().unwrap();
                engine.metadata_query(damage).unwrap();
            }
            let before = std::fs::read(&path).unwrap();
            let mut worker = Worker::open(
                path.clone(),
                Some("file:///root".into()),
                Resources::default(),
            )
            .unwrap();
            worker
                .sender
                .as_ref()
                .unwrap()
                .send(InspectConnectionRequest {
                    root: Some("file:///root".into()),
                    declaration: None,
                })
                .unwrap();
            assert_eq!(
                worker.receiver.recv().unwrap().unwrap_err().code,
                ErrorCode::OutcomeUnknown
            );
            worker.stop().unwrap();
            assert_eq!(std::fs::read(&path).unwrap(), before);
        }
    }
    #[test]
    fn inspection_refuses_foreign_root_lost_marker_and_metadata_views_without_repair() {
        let root = fixture();
        let path = root.path().join("db.duckdb");
        let binding = WorkspaceBinding {
            canonical_root: "file:///root".into(),
            workspace_id: Uuid::v4(),
        };
        {
            let mut engine = NativeEngine::open(&path).unwrap();
            crate::pull::PullStore::initialize_metadata(&mut engine, &binding).unwrap();
        }
        let before = std::fs::read(&path).unwrap();
        assert_eq!(
            Worker::open(
                path.clone(),
                Some("file:///foreign".into()),
                Resources::default()
            )
            .err()
            .unwrap()
            .code,
            ErrorCode::StateConflict
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
        {
            let mut engine = NativeEngine::open(&path).unwrap();
            engine
                .metadata_query("DROP TABLE _grv.workspace_binding")
                .unwrap();
            engine.metadata_query("CREATE VIEW _grv.workspace_binding AS SELECT 'not-a-binding'::VARCHAR AS identity").unwrap();
        }
        assert_eq!(
            Worker::open(
                path.clone(),
                Some("file:///root".into()),
                Resources::default()
            )
            .err()
            .unwrap()
            .code,
            ErrorCode::OutcomeUnknown
        );
        {
            let mut engine = NativeEngine::open(&path).unwrap();
            engine.metadata_query("DROP SCHEMA _grv CASCADE").unwrap();
            engine.record_managed_evidence(true).unwrap();
        }
        assert_eq!(
            Worker::open(path, Some("file:///root".into()), Resources::default())
                .err()
                .unwrap()
                .code,
            ErrorCode::OutcomeUnknown
        );
    }
}

struct Worker {
    sender: Option<mpsc::SyncSender<InspectConnectionRequest>>,
    receiver: mpsc::Receiver<Result<Value>>,
    owner: Option<thread::JoinHandle<()>>,
    interrupt: NativeInterrupt,
    binding: Option<WorkspaceBinding>,
}
impl Worker {
    fn open(path: PathBuf, root: Option<String>, resources: Resources) -> Result<Self> {
        let (sender, commands) = mpsc::sync_channel(1);
        let (reply, receiver) = mpsc::sync_channel(1);
        let (ready, initialized) = mpsc::sync_channel(1);
        let owner = thread::spawn(move || {
            let opened = (|| {
                let mut engine = NativeEngine::open_readonly(&path).map_err(failure)?;
                query(&mut engine, "BEGIN TRANSACTION")?;
                let binding = binding_at(&mut engine, root.as_deref())?;
                Ok::<_, grv_types::PublicError>((engine, binding))
            })();
            let (mut engine, binding) = match opened {
                Ok(value) => value,
                Err(e) => {
                    let _ = ready.send(Err(e));
                    return;
                }
            };
            if ready
                .send(Ok((engine.interrupt_handle(), binding.clone())))
                .is_err()
            {
                return;
            }
            while let Ok(request) = commands.recv() {
                let result = observe(&mut engine, &path, binding.as_ref(), &request, &resources);
                if reply.send(result).is_err() {
                    return;
                }
            }
            let _ = query(&mut engine, "ROLLBACK");
        });
        match initialized.recv() {
            Ok(Ok((interrupt, binding))) => Ok(Self {
                sender: Some(sender),
                receiver,
                owner: Some(owner),
                interrupt,
                binding,
            }),
            Ok(Err(error)) => {
                let _ = owner.join();
                Err(error)
            }
            Err(_) => {
                let _ = owner.join();
                Err(unknown("inspection owner stopped during initialization"))
            }
        }
    }
    fn inspect(&mut self, request: InspectConnectionRequest, stop: &StopToken) -> Result<Value> {
        stop.check()?;
        self.sender
            .as_ref()
            .ok_or_else(|| unknown("inspection owner stopped"))?
            .send(request)
            .map_err(|_| unknown("inspection owner stopped"))?;
        loop {
            match self.receiver.recv_timeout(Duration::from_millis(20)) {
                Ok(result) => return result,
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(unknown("inspection owner stopped"));
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if stop.is_cancelled() {
                        self.stop().map_err(failure)?;
                        return stop.check().and(Err(unknown("inspection stopped")));
                    }
                }
            }
        }
    }
    fn stop(&mut self) -> io::Result<()> {
        self.interrupt.interrupt();
        self.sender.take();
        self.owner.take().map_or(Ok(()), |owner| {
            owner
                .join()
                .map_err(|_| io::Error::other("inspection owner panicked"))
        })
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}
#[derive(Default)]
pub struct Runtime {
    connections: BTreeMap<Handle, Worker>,
    next: u64,
}
#[allow(clippy::result_large_err)]
impl Runtime {
    pub fn bind(
        &mut self,
        locator: ConnectionLocator,
        root: Option<String>,
        expected: Option<String>,
        workspace: Option<Uuid>,
        resources: &Resources,
    ) -> Result<BoundConnection> {
        // Native metadata documents have a fixed 2-MiB window. Reserve copies,
        // strict JSON decoding and the result separately before opening/reading.
        if resources.max_source_unit_bytes.get() < crate::native::METADATA_BYTES as u64
            || resources.max_scratch_bytes.get() < (crate::native::METADATA_BYTES * 8) as u64
        {
            return Err(error(
                ErrorCode::UnsupportedCapability,
                "inspection needs a 2-MiB metadata unit and 16-MiB scratch allowance",
            ));
        }
        let canonical = binding::locate(locator.canonical_connection.clone()).map_err(failure)?;
        if locator != canonical
            || expected
                .as_ref()
                .is_some_and(|id| Some(id) != locator.identity.as_ref())
        {
            return Err(error(
                ErrorCode::RequestMismatch,
                "inspection connection locator changed",
            ));
        }
        let path = PathBuf::from(locator.engine_path.as_ref().unwrap());
        if let Some(root) = &root {
            crate::extraction::outside_root(&path, root).map_err(failure)?;
        }
        let worker = Worker::open(path, root, resources.clone())?;
        let workspace_id = worker.binding.as_ref().map(|b| b.workspace_id.clone());
        if workspace
            .as_ref()
            .is_some_and(|id| Some(id) != workspace_id.as_ref())
        {
            return Err(error(
                ErrorCode::RequestMismatch,
                "inspection workspace identity changed",
            ));
        }
        let state = if workspace_id.is_some() {
            BindingState::Bound
        } else {
            BindingState::Uninitialized
        };
        self.next += 1;
        let handle = Handle::new(format!("duckdb-inspect-{}", self.next))
            .map_err(|_| unknown("invalid inspection handle"))?;
        self.connections.insert(handle.clone(), worker);
        Ok(BoundConnection {
            handle,
            identity: locator.identity,
            workspace_id: workspace_id.clone(),
            binding: state,
            details: json!({"database":locator.canonical_connection["database"],"workspace_id":workspace_id,"binding":state}),
        })
    }
    pub fn inspect(
        &mut self,
        handle: Handle,
        request: InspectConnectionRequest,
        stop: &StopToken,
    ) -> Result<Value> {
        self.connections
            .get_mut(&handle)
            .ok_or_else(|| error(ErrorCode::ProtocolFailure, "unknown inspection handle"))?
            .inspect(request, stop)
    }
    pub fn stop(&mut self) -> Result<()> {
        let connections = std::mem::take(&mut self.connections);
        for (_, mut worker) in connections {
            worker.stop().map_err(failure)?;
        }
        Ok(())
    }
}
