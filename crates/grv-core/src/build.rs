//! Parent-owned build provenance and preparation. Discovery retains the engine
//! transaction; GRV inputs are fixed, held and verified before materialization.
use crate::{
    clock::Clock,
    holds::Holds,
    ownership::{Ownership, RunOwner},
    revision, source,
    store::{Result, Store, backend_error, public_error},
};
use grv_adapter_api::{
    BuildDiscovery, BuildExecution, BuildInputFiles, BuildSession, DiscoverBuildRequest,
    FileAccess, Handle, NamedContract, PrepareBuildRequest,
};
use grv_adapter_host::process::Session;
use grv_storage::{Backend, ObjectKey, model::*};
use grv_types::{ErrorCode, Name, RequestedRevision, U64, Uuid};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::Path,
};

const RECORD_LIMIT: usize = 64 * 1024 * 1024;
const GRAPH_DATASETS: usize = 65_536;
const GRAPH_EDGES: usize = 1_048_576;

fn invalid(message: impl Into<String>) -> grv_types::PublicError {
    public_error(ErrorCode::InvalidDeclaration, message)
}
fn integrity(message: &str) -> grv_types::PublicError {
    public_error(ErrorCode::IntegrityFailure, message)
}

/// Walk only observable current states. Historical input revision numbers do
/// not change which upstream current state is visited. This never creates a run,
/// lease, hold or engine resource, and errors cannot turn into missing edges.
pub fn check_acyclic<B: Backend>(
    store: &Store<B>,
    target: &Name,
    inputs: &[Name],
    scratch: &Path,
) -> Result<()> {
    let mut parents = BTreeMap::<Name, Option<Name>>::new();
    let mut queue = VecDeque::new();
    for input in inputs {
        if input == target {
            return Err(invalid(format!(
                "build dependency cycle: {target} -> {target}; use self_input"
            )));
        }
        if !parents.contains_key(input) {
            parents.insert(input.clone(), None);
            queue.push_back(input.clone());
        }
    }
    if parents.len() > GRAPH_DATASETS {
        return Err(invalid("build dependency graph exceeds dataset budget"));
    }
    let mut edges = 0usize;
    while let Some(dataset) = queue.pop_front() {
        let Some((latest, _)) = revision::read_latest(store, &dataset)? else {
            continue;
        };
        if latest.revision.get() == 0 {
            continue;
        }
        let current = revision::read(store, &dataset, latest.revision, scratch)?;
        let runs: BTreeSet<_> = current.state.values().map(|entry| &entry.run_id).collect();
        for run_id in runs {
            let key = ObjectKey::new(format!("datasets/{dataset}/.runs/{run_id}.json"))
                .map_err(backend_error)?;
            let bytes = store
                .backend
                .read_bytes(&key, RECORD_LIMIT)
                .map_err(backend_error)?
                .0;
            let run: SealedRun = decode_record(&bytes).map_err(backend_error)?;
            if &run.run_id != run_id {
                return Err(integrity(
                    "current state's immutable run differs from its path",
                ));
            }
            for input in run.inputs {
                edges += 1;
                if edges > GRAPH_EDGES {
                    return Err(invalid("build dependency graph exceeds edge budget"));
                }
                if &input.dataset == target {
                    let mut path = vec![target.clone(), dataset.clone()];
                    let mut cursor = &dataset;
                    while let Some(Some(parent)) = parents.get(cursor) {
                        path.push(parent.clone());
                        cursor = parent;
                    }
                    path.push(target.clone());
                    path.reverse();
                    return Err(invalid(format!(
                        "build dependency cycle: {}",
                        path.iter()
                            .map(Name::as_str)
                            .collect::<Vec<_>>()
                            .join(" -> ")
                    )));
                }
                if !parents.contains_key(&input.dataset) {
                    if parents.len() == GRAPH_DATASETS {
                        return Err(invalid("build dependency graph exceeds dataset budget"));
                    }
                    parents.insert(input.dataset.clone(), Some(dataset.clone()));
                    queue.push_back(input.dataset);
                }
            }
        }
    }
    Ok(())
}

/// Private durable identity. Persist this whole record before commit_run or
/// acquiring any hold; it contains GRV authority and never crosses the wire.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildIntent {
    pub request: DiscoverBuildRequest,
    pub discovery: BuildDiscovery,
    pub run: RunOwner,
    pub holds: Vec<HoldRecord>,
    pub initial_latest: Option<Latest>,
}
impl BuildIntent {
    pub fn validate(&self) -> Result<()> {
        self.discovery
            .validate_for(&self.request)
            .map_err(|e| invalid(e.to_string()))?;
        self.run.control().validate().map_err(backend_error)?;
        if let Some(initial) = &self.initial_latest {
            initial.validate().map_err(backend_error)?;
            if self.run.control().base_revision.get() != 0
                || initial.revision.get() != 0
                || initial.high_water.get() != 0
                || initial.lease.is_some()
                || initial.pending.is_some()
            {
                return Err(integrity("build initial coordination is not empty"));
            }
        }
        if self.run.dataset() != &self.request.identity.dataset
            || self.run.control().run_id != self.request.identity.run_id
            || self.run.control().phase != RunPhase::Open
            || self.run.control().holds_confirmed
        {
            return Err(integrity("build intent changed prepared run identity"));
        }
        let bindings: BTreeSet<_> = self
            .discovery
            .inputs
            .iter()
            .map(|i| (&i.dataset, i.revision.get()))
            .collect();
        let actual: BTreeSet<_> = self
            .run
            .control()
            .inputs
            .iter()
            .map(|i| (&i.dataset, i.revision.get()))
            .collect();
        if actual != bindings || self.holds.len() != actual.len() {
            return Err(integrity("build intent changed whole-revision input scope"));
        }
        let mut ids = BTreeSet::new();
        for hold in &self.holds {
            hold.validate().map_err(backend_error)?;
            if !ids.insert(&hold.retention_id)
                || hold.target_dataset != *self.run.dataset()
                || hold.target_run_id != self.run.control().run_id
                || hold.created_at != self.run.control().created_at
                || !self.run.control().inputs.iter().any(|i| {
                    i.dataset == hold.dataset
                        && i.revision == hold.revision
                        && i.retention_id == hold.retention_id
                })
            {
                return Err(integrity("build intent hold differs from fixed run input"));
            }
        }
        Ok(())
    }
}

/// Reads provenance and fixes the target base, then prepares identities purely.
/// Caller journals the result before creating the run and its dependency holds.
pub fn prepare_intent<B: Backend>(
    store: &Store<B>,
    clock: &dyn Clock,
    ttl: u64,
    request: DiscoverBuildRequest,
    discovery: BuildDiscovery,
    scratch: &Path,
) -> Result<BuildIntent> {
    discovery
        .validate_for(&request)
        .map_err(|e| invalid(e.to_string()))?;
    check_acyclic(
        store,
        &request.identity.dataset,
        &discovery
            .inputs
            .iter()
            .map(|i| i.dataset.clone())
            .collect::<Vec<_>>(),
        scratch,
    )?;
    let latest = revision::read_latest(store, &request.identity.dataset)?;
    let initial_latest = latest.is_none().then(Latest::empty);
    let base = latest.map_or(Counter::from(0), |(latest, _)| latest.revision);
    let dependencies: BTreeSet<_> = discovery
        .inputs
        .iter()
        .map(|i| (i.dataset.clone(), i.revision.get()))
        .collect();
    let inputs = dependencies
        .into_iter()
        .map(|(dataset, revision)| {
            Ok(RunInput {
                dataset,
                revision: Counter::new(revision).map_err(backend_error)?,
                retention_id: Uuid::v4(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let owner = Ownership::new(store, clock, ttl)?.prepare_run(
        request.identity.dataset.clone(),
        request.identity.run_id.clone(),
        base,
        inputs,
        Some(BTreeMap::from([(
            "grv_cli".into(),
            serde_json::json!({
                "canonical_writer": crate::canonical::CANONICAL_WRITER,
                "adapter": request.identity.adapter_identity,
                "attempt_id": request.identity.attempt_id,
                "declaration_sha256": request.identity.declaration_sha256,
                "mode": "build",
            }),
        )])),
    )?;
    let holds = Holds::new(store, clock, scratch).prepare(owner.dataset(), owner.control())?;
    let intent = BuildIntent {
        request,
        discovery,
        run: owner,
        holds,
        initial_latest,
    };
    intent.validate()?;
    Ok(intent)
}

/// Holds exact local file lifetimes through adapter preparation/materialization.
/// On any adapter error, stop/close its reader before dropping this object.
pub struct VerifiedBuildInputs {
    pub preparation: PrepareBuildRequest,
    _external: Vec<source::VerifiedSelection>,
    _base: Option<source::VerifiedSelection>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum PreparationProgress {
    Prepared,
    RunCreated {
        owner: RunOwner,
    },
    HoldsConfirmed {
        owner: RunOwner,
    },
    FilesVerified {
        owner: RunOwner,
        preparation: Box<PrepareBuildRequest>,
    },
    SessionPrepared {
        owner: RunOwner,
        session: Box<BuildSession>,
    },
}

/// The process drops first, stopping its readers before verified files vanish.
/// Subsequent execution/export must keep the run continuously renewed.
pub struct PreparedBuild {
    process: Session,
    pub owner: RunOwner,
    pub session: BuildSession,
    _verified: VerifiedBuildInputs,
}
impl PreparedBuild {
    pub fn process_mut(&mut self) -> &mut Session {
        &mut self.process
    }
}

/// Keep one managed invocation or acceptance call fenced while blocked on the
/// adapter. A renewal failure closes the protocol socket and stops supervision.
/// Callers persist execute-once intent before dispatch and results on receipt.
pub fn supervise<B: Backend, T>(
    store: &Store<B>,
    clock: &dyn Clock,
    ttl: u64,
    owner: &mut RunOwner,
    process: &mut Session,
    operation: impl FnOnce(&mut Session) -> Result<T>,
) -> Result<T> {
    let ownership = Ownership::new(store, clock, ttl)?;
    ownership.require_open_transfer(owner)?;
    ownership.renew_run(owner)?;
    let shutdown = process
        .channel
        .codec
        .io_mut()
        .shutdown_handle()
        .map_err(|e| public_error(ErrorCode::BackendFailure, e.to_string()))?;
    let (result, renewed) = crate::renewal::during(
        owner.clone(),
        ownership.renewal_interval(),
        |owner| {
            let result = ownership.renew_run(owner);
            if result.is_err() {
                let _ = shutdown.shutdown(std::net::Shutdown::Both);
            }
            result
        },
        |_| operation(process),
    )?;
    *owner = renewed;
    Ok(result)
}
struct ReaderLifetime {
    process: Option<Session>,
    verified: Option<VerifiedBuildInputs>,
}
impl Drop for ReaderLifetime {
    fn drop(&mut self) {
        // Session::drop terminates and reaps the whole process group, including
        // an ignored close/cancel. Only then can staging files be removed.
        drop(self.process.take());
        drop(self.verified.take());
    }
}

/// One new retained-discovery lifecycle. Recovery of an existing engine session
/// uses its stored build record instead of calling this function again.
pub struct PreparationContext<'a, B: Backend> {
    pub store: &'a Store<B>,
    pub clock: &'a dyn Clock,
    pub ttl: u64,
    pub scratch: &'a Path,
}
pub fn prepare_session<B: Backend>(
    process: Session,
    handle: Handle,
    context: PreparationContext<'_, B>,
    intent: BuildIntent,
    mut persist: impl FnMut(&BuildIntent, &PreparationProgress) -> Result<()>,
) -> Result<PreparedBuild> {
    let PreparationContext {
        store,
        clock,
        ttl,
        scratch,
    } = context;
    let mut reader = ReaderLifetime {
        process: Some(process),
        verified: None,
    };
    intent.validate()?;
    let ownership = Ownership::new(store, clock, ttl)?;
    persist(&intent, &PreparationProgress::Prepared)?;
    if let Some(initial) = &intent.initial_latest {
        crate::publication::Publisher::new(store, clock, ttl)?
            .initialize_dataset(intent.run.dataset(), initial)?;
    }
    ownership.commit_run(&intent.run)?;
    let mut owner = intent.run.clone();
    persist(
        &intent,
        &PreparationProgress::RunCreated {
            owner: owner.clone(),
        },
    )?;
    let holds = Holds::new(store, clock, scratch);
    let mut proofs = Vec::new();
    let (_, renewed) = crate::renewal::during(
        owner,
        ownership.renewal_interval(),
        |owner| ownership.renew_run(owner),
        |watch| {
            for hold in &intent.holds {
                watch.check()?;
                proofs.push(holds.acquire(hold)?);
            }
            watch.check()
        },
    )?;
    owner = renewed;
    ownership.confirm_dependency_holds(&mut owner, scratch, &proofs)?;
    persist(
        &intent,
        &PreparationProgress::HoldsConfirmed {
            owner: owner.clone(),
        },
    )?;
    let authority = owner.clone();
    let shutdown = reader
        .process
        .as_mut()
        .unwrap()
        .channel
        .codec
        .io_mut()
        .shutdown_handle()
        .map_err(|e| public_error(ErrorCode::BackendFailure, e.to_string()))?;
    let (session, owner) = crate::renewal::during(
        owner,
        ownership.renewal_interval(),
        |owner| {
            let result = ownership.renew_run(owner);
            if result.is_err() {
                let _ = shutdown.shutdown(std::net::Shutdown::Both);
            }
            result
        },
        |watch| {
            watch.check()?;
            reader.verified = Some(verify_inputs(
                store, clock, ttl, &intent, &authority, scratch,
            )?);
            let preparation = reader.verified.as_ref().unwrap().preparation.clone();
            watch.check()?;
            persist(
                &intent,
                &PreparationProgress::FilesVerified {
                    owner: authority.clone(),
                    preparation: Box::new(preparation.clone()),
                },
            )?;
            let session = reader
                .process
                .as_mut()
                .unwrap()
                .prepare_build(handle, preparation)
                .map_err(|error| error.public())?;
            watch.check()?;
            Ok(session)
        },
    )?;
    persist(
        &intent,
        &PreparationProgress::SessionPrepared {
            owner: owner.clone(),
            session: Box::new(session.clone()),
        },
    )?;
    Ok(PreparedBuild {
        process: reader.process.take().unwrap(),
        owner,
        session,
        _verified: reader.verified.take().unwrap(),
    })
}

/// The caller continuously renews the run while this potentially blocking
/// verification executes. Only a live, confirmed exact run can authorize it.
pub fn verify_inputs<B: Backend>(
    store: &Store<B>,
    clock: &dyn Clock,
    ttl: u64,
    intent: &BuildIntent,
    owner: &RunOwner,
    scratch: &Path,
) -> Result<VerifiedBuildInputs> {
    intent.validate()?;
    if owner.dataset() != intent.run.dataset()
        || owner.control().run_id != intent.run.control().run_id
        || owner.control().owner_token != intent.run.control().owner_token
        || owner.control().base_revision != intent.run.control().base_revision
        || owner.control().inputs != intent.run.control().inputs
    {
        return Err(integrity("build verification changed fixed run authority"));
    }
    Ownership::new(store, clock, ttl)?.require_open_transfer(owner)?;
    for hold in &intent.holds {
        if !Holds::new(store, clock, scratch).active(hold)? {
            return Err(integrity("build input hold is missing or released"));
        }
    }
    // External S3 preparations retain exact remote views under these holds,
    // independently of the mutable input's current materialization mode.
    let access = if intent.request.execution == BuildExecution::External
        && store
            .backend
            .s3_data_uri(&ObjectKey::new("grv.json").map_err(backend_error)?)
            .map_err(backend_error)?
            .is_some()
    {
        FileAccess::S3View
    } else {
        FileAccess::Local
    };
    let mut external = Vec::new();
    let mut input_files = Vec::new();
    for input in &intent.discovery.inputs {
        let selected = source::verify_with_access(
            store,
            &input.dataset,
            &RequestedRevision::Revision(input.revision),
            &[source::TableSelection {
                table: input.table.clone(),
                partitions: None,
                expect_columns: Some(input.contract.columns.clone()),
                expect_partition_keys: Some(input.contract.partition_keys.clone()),
                prior_source_contract: None,
            }],
            scratch,
            access,
        )
        .map_err(|error| {
            if error.code == ErrorCode::InvalidDeclaration {
                public_error(
                    ErrorCode::ProtocolFailure,
                    "recorded build input contract differs from verified GRV files",
                )
            } else {
                error
            }
        })?;
        if selected.tables.len() != 1 || selected.tables[0].contract != input.contract {
            return Err(public_error(
                ErrorCode::ProtocolFailure,
                "verified build input differs from discovered materialization contract",
            ));
        }
        input_files.push(BuildInputFiles {
            alias: input.alias.clone(),
            files: selected.files.clone(),
        });
        external.push(selected);
    }
    let base_number = owner.control().base_revision;
    let mut base = None;
    let mut base_contracts = Vec::new();
    let mut base_files = Vec::new();
    if intent.request.self_input && base_number.get() != 0 {
        let state = revision::read(store, owner.dataset(), base_number, scratch)?.state;
        let names: BTreeSet<_> = state.values().map(|e| e.table.clone()).collect();
        let selections: Vec<_> = names
            .into_iter()
            .map(|table| source::TableSelection {
                table,
                partitions: None,
                expect_columns: None,
                expect_partition_keys: None,
                prior_source_contract: None,
            })
            .collect();
        let selected = source::verify_with_access(
            store,
            owner.dataset(),
            &RequestedRevision::Revision(U64::new(base_number.get()).unwrap()),
            &selections,
            scratch,
            access,
        )?;
        base_contracts = selected
            .tables
            .iter()
            .map(|t| NamedContract {
                table: t.table.clone(),
                contract: t.contract.clone(),
            })
            .collect();
        base_files = selected.files.clone();
        base = Some(selected);
    }
    let preparation = PrepareBuildRequest {
        discovery: intent.discovery.clone(),
        base_revision: U64::new(base_number.get()).unwrap(),
        self_input: intent.request.self_input,
        base_contracts,
        base_files,
        input_files,
        holds_confirmed: true,
    };
    preparation
        .validate()
        .map_err(|e| integrity(&e.to_string()))?;
    Ok(VerifiedBuildInputs {
        preparation,
        _external: external,
        _base: base,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{canonical::Sorter, clock::SystemClock, store::InitOptions};
    use grv_adapter_api::*;
    use grv_storage::LocalBackend;
    use grv_types::{Req, RunId};
    use serde_json::json;
    use std::{fs::File, sync::Arc};

    fn name(s: &str) -> Name {
        Name::new(s).unwrap()
    }
    fn run(n: u32) -> RunId {
        RunId::new(format!("01ARZ3NDEKTSV4RRFFQ{n:07}")).unwrap()
    }
    struct Fixture {
        root: tempfile::TempDir,
        store: Store<LocalBackend>,
        clock: SystemClock,
    }
    impl Fixture {
        fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let store = Store::initialize(
                LocalBackend::open(root.path()).unwrap(),
                InitOptions::default(),
            )
            .unwrap()
            .0;
            Self {
                root,
                store,
                clock: SystemClock::default(),
            }
        }
        fn graph(&self, dataset: &str, dependencies: &[&str], revision: u32) {
            let dataset = name(dataset);
            let layout_key =
                ObjectKey::new(format!("datasets/{dataset}/rows/.layout.json")).unwrap();
            if self.store.backend.head(&layout_key).is_err() {
                self.store
                    .backend
                    .create_bytes(
                        &layout_key,
                        &encode_record(&TableLayout {
                            table: name("rows"),
                            partition_keys: vec![],
                            extensions: None,
                        })
                        .unwrap(),
                    )
                    .unwrap();
            }
            let run_id = run(revision);
            let timestamp = self.clock.now();
            let sealed = SealedRun {
                run_id: run_id.clone(),
                created_at: timestamp.clone(),
                sealed_at: timestamp.clone(),
                base_revision: Counter::from(0),
                holds_confirmed: true,
                inputs: dependencies
                    .iter()
                    .map(|d| RunInput {
                        dataset: name(d),
                        revision: Counter::from(1),
                        retention_id: Uuid::v4(),
                    })
                    .collect(),
                entries: vec![RunEntry {
                    table: name("rows"),
                    partition: Partition::new(),
                    version: Counter::from(revision),
                    claim_token: ClaimToken::generate(),
                }],
                metadata: None,
            };
            self.store
                .backend
                .create_bytes(
                    &ObjectKey::new(format!("datasets/{dataset}/.runs/{run_id}.json")).unwrap(),
                    &encode_record(&sealed).unwrap(),
                )
                .unwrap();
            self.publish_state(&dataset, &run_id, revision, timestamp);
        }
        fn publish_state(
            &self,
            dataset: &Name,
            run_id: &RunId,
            revision: u32,
            timestamp: grv_types::Timestamp,
        ) {
            let record = revision::Revision {
                revision: Counter::from(revision),
                previous_revision: Counter::from(0),
                operation_id: run(900 + revision),
                created_at: timestamp,
                state: BTreeMap::from([(
                    (name("rows"), String::new()),
                    revision::Entry {
                        table: name("rows"),
                        partition: String::new(),
                        version: Counter::from(revision),
                        run_id: run_id.clone(),
                    },
                )]),
            };
            let file = revision::encode(&record, self.root.path()).unwrap();
            self.store
                .backend
                .conditional_create(
                    &revision::revision_key(dataset, Counter::from(revision)),
                    &mut File::open(file.path()).unwrap(),
                )
                .unwrap();
            let latest = Latest {
                revision: Counter::from(revision),
                high_water: Counter::from(revision),
                ..Latest::empty()
            };
            let path = revision::latest_key(dataset);
            if let Ok(metadata) = self.store.backend.head(&path) {
                self.store
                    .backend
                    .put_bytes(&path, &metadata.validator, &encode_record(&latest).unwrap())
                    .unwrap();
            } else {
                self.store
                    .backend
                    .create_bytes(&path, &encode_record(&latest).unwrap())
                    .unwrap();
            }
        }
        fn source(&self) {
            let ownership = Ownership::new(&self.store, &self.clock, 60).unwrap();
            let mut owner = ownership
                .prepare_run(name("upstream"), run(50), Counter::from(0), vec![], None)
                .unwrap();
            ownership.commit_run(&owner).unwrap();
            ownership.confirm_holds(&mut owner).unwrap();
            let contract = contract();
            let mut sorter = Sorter::new(contract.clone(), self.root.path()).unwrap();
            sorter
                .append(
                    &arrow_array::RecordBatch::try_new(
                        crate::contract::arrow_schema(&contract).unwrap(),
                        vec![Arc::new(arrow_array::Int64Array::from(vec![3, 1, 2]))],
                    )
                    .unwrap(),
                )
                .unwrap();
            let staged = sorter.finish().unwrap();
            let intent = ownership
                .prepare_reservation(
                    &owner,
                    TableLayout {
                        table: name("rows"),
                        partition_keys: vec![],
                        extensions: None,
                    },
                    Partition::new(),
                    &contract,
                )
                .unwrap();
            let mut reservation = ownership
                .reserve_authorized(
                    &owner,
                    &intent,
                    &mut crate::ownership::ReservationProgress::Prepared,
                    |_| Ok(()),
                )
                .unwrap();
            ownership
                .write_group(&owner, &mut reservation, &contract, &staged, None)
                .unwrap();
            ownership
                .release(&mut reservation, ClaimOutcome::Finalized)
                .unwrap();
            ownership.seal(&mut owner).unwrap();
            self.publish_state(&name("upstream"), &run(50), 1, self.clock.now());
        }
    }
    fn contract() -> TableContract {
        TableContract {
            columns: vec![Column {
                name: "id".into(),
                logical_type: json!("int64"),
            }],
            partition_keys: vec![],
            extensions: json!({}),
            column_ext: json!({}),
        }
    }
    fn discovery(
        f: &Fixture,
        aliases: &[&str],
        self_input: bool,
    ) -> (DiscoverBuildRequest, BuildDiscovery) {
        let identity = BuildIdentity {
            attempt_id: Uuid::v4(),
            root: f.root.path().to_str().unwrap().into(),
            dataset: name("derived"),
            run_id: run(100),
            workspace_id: Uuid::v4(),
            declaration_sha256: grv_types::sha256(b"fixed"),
            adapter_identity: AdapterIdentity {
                name: name("fixture"),
                package_version: "1".into(),
                interface_version: Req::new(1).unwrap(),
                binding_schema_version: Req::new(1).unwrap(),
            },
            connection_identity: "/engine".into(),
        };
        let inputs: Vec<_> = aliases
            .iter()
            .map(|alias| InputBinding {
                alias: name(alias),
                relation: json!({"table":"raw.rows"}),
                dataset: name("upstream"),
                table: name("rows"),
                revision: U64::new(1).unwrap(),
                generation_id: Uuid::v4(),
                contract: contract(),
                materialization: Materialization::Local,
            })
            .collect();
        let request = DiscoverBuildRequest {
            identity: identity.clone(),
            execution: BuildExecution::Managed,
            options: json!({}),
            inputs: inputs
                .iter()
                .map(|i| BuildInput {
                    alias: i.alias.clone(),
                    relation: i.relation.clone(),
                })
                .collect(),
            outputs: vec![],
            selected_outputs: vec![],
            self_input,
        };
        (
            request,
            BuildDiscovery {
                discovery_id: Uuid::v4(),
                identity,
                inputs,
                outputs: vec![],
            },
        )
    }
    #[test]
    fn dependency_cycle_reports_shortest_observable_path_before_any_run_or_hold() {
        let f = Fixture::new();
        f.graph("upstream", &["bridge"], 1);
        f.graph("bridge", &["derived"], 1);
        let error = check_acyclic(
            &f.store,
            &name("derived"),
            &[name("upstream")],
            f.root.path(),
        )
        .unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidDeclaration);
        assert!(
            error
                .message
                .contains("derived -> upstream -> bridge -> derived")
        );
        assert!(!f.root.path().join("datasets/derived").exists());
        assert_eq!(
            check_acyclic(
                &f.store,
                &name("derived"),
                &[name("derived")],
                f.root.path()
            )
            .unwrap_err()
            .code,
            ErrorCode::InvalidDeclaration
        );
    }
    #[test]
    fn dependency_walk_uses_current_states_and_visits_shared_upstream_once() {
        let f = Fixture::new();
        f.graph("upstream", &["derived"], 1);
        f.graph("upstream", &["leaf"], 2);
        f.graph("other", &["upstream"], 1);
        check_acyclic(
            &f.store,
            &name("derived"),
            &[name("upstream"), name("other"), name("upstream")],
            f.root.path(),
        )
        .unwrap();
        // A pre-existing upstream cycle without the target is bounded by visited datasets.
        f.graph("leaf", &["upstream"], 1);
        check_acyclic(
            &f.store,
            &name("derived"),
            &[name("upstream")],
            f.root.path(),
        )
        .unwrap();
    }
    #[test]
    fn missing_or_wrong_immutable_run_is_an_error_not_a_missing_graph_edge() {
        let f = Fixture::new();
        f.graph("upstream", &[], 1);
        let path = ObjectKey::new(format!("datasets/upstream/.runs/{}.json", run(1))).unwrap();
        let (bytes, _) = f.store.backend.read_bytes(&path, RECORD_LIMIT).unwrap();
        f.store.backend.delete(&path).unwrap();
        assert_eq!(
            check_acyclic(
                &f.store,
                &name("derived"),
                &[name("upstream")],
                f.root.path()
            )
            .unwrap_err()
            .code,
            ErrorCode::NotFound
        );
        let mut record: SealedRun = decode_record(&bytes).unwrap();
        record.run_id = run(2);
        f.store
            .backend
            .create_bytes(&path, &encode_record(&record).unwrap())
            .unwrap();
        assert_eq!(
            check_acyclic(
                &f.store,
                &name("derived"),
                &[name("upstream")],
                f.root.path()
            )
            .unwrap_err()
            .code,
            ErrorCode::IntegrityFailure
        );
    }
    #[test]
    fn pure_build_intent_deduplicates_whole_revision_holds_and_verifies_exact_alias_files() {
        let f = Fixture::new();
        f.source();
        let (request, discovery) = discovery(&f, &["first", "second"], true);
        let intent =
            prepare_intent(&f.store, &f.clock, 60, request, discovery, f.root.path()).unwrap();
        assert_eq!(intent.holds.len(), 1);
        assert_eq!(intent.run.control().inputs.len(), 1);
        assert!(!f.root.path().join("datasets/derived").exists());
        let mut changed = intent.clone();
        changed.holds[0].retention_id = Uuid::v4();
        assert_eq!(
            changed.validate().unwrap_err().code,
            ErrorCode::IntegrityFailure
        );
        let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
        ownership.commit_run(&intent.run).unwrap();
        let mut owner = intent.run.clone();
        assert!(verify_inputs(&f.store, &f.clock, 60, &intent, &owner, f.root.path()).is_err());
        let holds = Holds::new(&f.store, &f.clock, f.root.path());
        let proofs = intent
            .holds
            .iter()
            .map(|h| holds.acquire(h).unwrap())
            .collect::<Vec<_>>();
        ownership
            .confirm_dependency_holds(&mut owner, f.root.path(), &proofs)
            .unwrap();
        let verified =
            verify_inputs(&f.store, &f.clock, 60, &intent, &owner, f.root.path()).unwrap();
        assert_eq!(verified.preparation.input_files.len(), 2);
        assert!(
            verified
                .preparation
                .input_files
                .iter()
                .all(|i| i.files.len() == 1 && i.files[0].table == name("rows"))
        );
        assert!(verified.preparation.base_files.is_empty());
        assert!(verified.preparation.base_contracts.is_empty());
        let paths: Vec<_> = verified
            .preparation
            .input_files
            .iter()
            .flat_map(|i| &i.files)
            .map(|f| f.location.clone())
            .collect();
        assert!(paths.iter().all(|p| Path::new(p).is_file()));
        drop(verified);
        assert!(paths.iter().all(|p| !Path::new(p).exists()));
    }

    struct S3Coordinates<'a>(&'a grv_storage::LocalBackend);
    impl Backend for S3Coordinates<'_> {
        fn s3_data_uri(&self, key: &ObjectKey) -> grv_storage::Result<Option<String>> {
            Ok(Some(format!("s3://test-bucket/held/{}", key.as_str())))
        }
        fn get(
            &self,
            key: &ObjectKey,
            sink: &mut dyn std::io::Write,
        ) -> grv_storage::Result<grv_storage::ObjectMeta> {
            self.0.get(key, sink)
        }
        fn head(&self, key: &ObjectKey) -> grv_storage::Result<grv_storage::ObjectMeta> {
            self.0.head(key)
        }
        fn list(
            &self,
            prefix: &grv_storage::ObjectPrefix,
            mode: grv_storage::ListMode,
        ) -> grv_storage::Result<Vec<grv_storage::ListEntry>> {
            self.0.list(prefix, mode)
        }
        fn delete(&self, _: &ObjectKey) -> grv_storage::Result<()> {
            panic!("verification mutated source")
        }
        fn conditional_create(
            &self,
            _: &ObjectKey,
            _: &mut dyn std::io::Read,
        ) -> grv_storage::Result<grv_storage::Validator> {
            panic!("verification mutated source")
        }
        fn conditional_put(
            &self,
            _: &ObjectKey,
            _: &grv_storage::Validator,
            _: &mut dyn std::io::Read,
        ) -> grv_storage::Result<grv_storage::Validator> {
            panic!("verification mutated source")
        }
    }
    #[test]
    fn external_s3_build_uses_verified_held_uris_even_for_locally_materialized_inputs() {
        let f = Fixture::new();
        f.source();
        let (mut request, discovery) = discovery(&f, &["input"], false);
        assert_eq!(discovery.inputs[0].materialization, Materialization::Local);
        request.execution = BuildExecution::External;
        let intent =
            prepare_intent(&f.store, &f.clock, 60, request, discovery, f.root.path()).unwrap();
        let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
        ownership.commit_run(&intent.run).unwrap();
        let mut owner = intent.run.clone();
        let proofs = intent
            .holds
            .iter()
            .map(|hold| {
                Holds::new(&f.store, &f.clock, f.root.path())
                    .acquire(hold)
                    .unwrap()
            })
            .collect::<Vec<_>>();
        ownership
            .confirm_dependency_holds(&mut owner, f.root.path(), &proofs)
            .unwrap();
        let store = Store::open(S3Coordinates(&f.store.backend)).unwrap();
        let external = verify_inputs(&store, &f.clock, 60, &intent, &owner, f.root.path()).unwrap();
        let files = &external.preparation.input_files[0].files;
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].access, FileAccess::S3View);
        assert!(
            files[0]
                .location
                .starts_with("s3://test-bucket/held/datasets/upstream/rows/")
        );
        assert_eq!(
            external.preparation.discovery.inputs[0].materialization,
            Materialization::Local
        );
        // Managed execution deliberately reconstructs private local files using
        // the same confirmed holds rather than trusting mutable tracking tables.
        let mut managed = intent.clone();
        managed.request.execution = BuildExecution::Managed;
        let local = verify_inputs(&store, &f.clock, 60, &managed, &owner, f.root.path()).unwrap();
        assert_eq!(
            local.preparation.input_files[0].files[0].access,
            FileAccess::Local
        );
        assert!(Path::new(&local.preparation.input_files[0].files[0].location).is_file());
        let mut inconsistent = managed;
        inconsistent.discovery.inputs[0].contract.columns[0].logical_type = json!("boolean");
        let error = match verify_inputs(&store, &f.clock, 60, &inconsistent, &owner, f.root.path())
        {
            Err(error) => error,
            Ok(_) => panic!("inconsistent consumer contract was accepted"),
        };
        assert_eq!(error.code, ErrorCode::ProtocolFailure);
    }
}
