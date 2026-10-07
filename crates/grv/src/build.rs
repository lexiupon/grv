//! Managed build attempts preserve fixed provenance and accepted exports outside
//! GRV. Complete captures and terminal publications never reopen an engine.
use super::{Failure, Success, backend::Root, transfer::Options};
use grv_adapter_api::*;
use grv_adapter_host::{
    discovery,
    process::{Deadlines, Session},
    registry::CheckedRegistry,
    session_lock::SessionMutationLock,
};
use grv_core::{
    build::{self, BuildIntent, PreparationContext, PreparationProgress, PreparedBuild},
    build_export::{
        self, AcceptedBuild, CompletionProgress, ExportJob, ExportProgress, ExportReceipt,
    },
    build_publication::{self, Finalizer, Selection},
    clock::{Clock, SystemClock, new_run_id},
    declaration,
    journal::{Envelope, Evidence, Journal},
    normalize::TablePlan,
    ownership::{Ownership, RunOwner},
    push::PushOutcome,
    store::{Result, Store, public_error},
};
use grv_types::{DeclarationIdentity, PublicError};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    io::Cursor,
    path::{Path, PathBuf},
};

fn invalid(message: &str) -> PublicError {
    public_error(ErrorCode::InvalidDeclaration, message)
}
fn incomplete(message: &str) -> PublicError {
    public_error(ErrorCode::BuildIncomplete, message)
}
fn mismatch() -> PublicError {
    public_error(
        ErrorCode::RequestMismatch,
        "build attempt belongs to a different fixed request",
    )
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixed {
    authoring_sha256: Digest,
    authored: Value,
    declaration_path: PathBuf,
    engine_path: Option<PathBuf>,
    root: String,
    effective: Value,
    descriptor: AdapterDescriptor,
    after_publish: bool,
    registry: Registry,
    canonical_connection: Value,
    request: DiscoverBuildRequest,
    plans: Vec<TablePlan>,
    selection: Selection,
}
#[derive(Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct Progress {
    discovery_started: bool,
    intent: Option<BuildIntent>,
    preparation: Option<PreparationProgress>,
    owner: Option<RunOwner>,
    session: Option<BuildSession>,
    execution_started: bool,
    candidate: Option<BuildCompletion>,
    accepted: Option<AcceptedBuild>,
    failure: Option<PublicError>,
    aborted: Option<Value>,
    abort_progress: Option<grv_core::build_abort::Progress>,
    export: Option<ExportProgress>,
    publication: Option<build_publication::Progress>,
    #[serde(deserialize_with = "super::after_publish::required_state")]
    after_publish: Option<super::after_publish::State>,
}
type Record = Envelope<Fixed, ExportReceipt, Progress, Value>;
fn save(
    journal: &Journal,
    record: &mut Record,
    change: impl FnOnce(&mut Evidence<Fixed, ExportReceipt, Progress, Value>),
) -> Result<()> {
    let mut next = record.evidence.clone();
    change(&mut next);
    *record = journal.compare_and_swap(record.generation, next)?;
    Ok(())
}
enum Active {
    Prepared(Box<PreparedBuild>),
    Open(Box<Session>),
}
impl Active {
    fn process(&mut self) -> &mut Session {
        match self {
            Self::Prepared(p) => p.process_mut(),
            Self::Open(p) => p,
        }
    }
}
fn ttl<B: grv_storage::Backend>(store: &Store<B>) -> u64 {
    900.min(store.parameters.max_lease_ttl_seconds.get())
}

enum Purpose {
    Managed,
    Prepare(PathBuf),
    Finalize(Option<PathBuf>),
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Context {
    mode: String,
    context_version: u32,
    directory: PathBuf,
    fixed: Fixed,
    intent: BuildIntent,
    session: BuildSession,
}

pub(super) fn run(
    options: Options,
    root: Root,
    canonical: &str,
    authored: Value,
) -> std::result::Result<Success, Failure> {
    run_internal(options, root, canonical, authored, Purpose::Managed)
}
fn run_internal(
    options: Options,
    root: Root,
    canonical: &str,
    authored: Value,
    purpose: Purpose,
) -> std::result::Result<Success, Failure> {
    let mode = declaration::mode(&authored).map_err(|_| invalid("invalid build mode"))?;
    if !matches!(mode, Mode::ManagedBuild | Mode::ExternalBuild)
        || (matches!(purpose, Purpose::Managed) && mode != Mode::ManagedBuild)
        || (!matches!(purpose, Purpose::Managed) && mode != Mode::ExternalBuild)
    {
        return Err(invalid("session preparation requires an external build declaration").into());
    }
    let authoring_sha256 = grv_types::sha256(
        &grv_types::canonical_json(&authored)
            .map_err(|_| invalid("build declaration is not JCS interoperable"))?,
    );
    let directory = options.state.join("push").join(options.attempt.as_str());
    let existing = directory
        .try_exists()
        .map_err(|_| public_error(ErrorCode::BackendFailure, "consumer state unavailable"))?;
    let journal;
    let mut record: Record;
    let mut active = None;
    let mut handle = None;
    let mut mutation = None;
    let clock = SystemClock::default();
    if existing {
        journal = Journal::open(&directory, root.exclusions())?;
        record = journal.read()?;
        if record.evidence.intent.authoring_sha256 != authoring_sha256
            || record.evidence.intent.root != canonical
            || record.evidence.intent.request.identity.attempt_id != options.attempt
        {
            return Err(mismatch().into());
        }
        if record.evidence.progress.aborted.is_some() {
            return Err(Failure {
                error: incomplete("build session was aborted"),
                result: None,
                root: Some(canonical.into()),
            });
        }
        if let Some(mut result) = record.evidence.terminal.clone() {
            if let Purpose::Prepare(path) = &purpose {
                return expose_context(&journal, &record, path, canonical);
            }
            result["replayed"] = json!(true);
            return acknowledge(&journal, &mut record, result, canonical);
        }
        if let Some(outcome) = record
            .evidence
            .progress
            .publication
            .as_ref()
            .and_then(|p| p.publication.as_ref())
            .and_then(|p| p.terminal.clone())
        {
            let success = finish(&journal, &mut record, outcome, canonical, true)?;
            return acknowledge(&journal, &mut record, success.result, canonical);
        }
        if let Some(error) = &record.evidence.progress.failure {
            return Err(error.clone().into());
        }
    } else {
        let name = Name::new(
            authored["adapter"]
                .as_str()
                .ok_or_else(|| invalid("build adapter required"))?,
        )
        .map_err(|e| invalid(&e.to_string()))?;
        let installed = discovery::discover(&discovery::search_roots(None)?)?;
        let installation = installed
            .iter()
            .find(|i| i.manifest.name == name)
            .ok_or_else(|| {
                public_error(
                    ErrorCode::NotFound,
                    "build adapter installation unavailable",
                )
            })?;
        let decl_path = std::fs::canonicalize(&options.decl)
            .map_err(|_| invalid("build declaration path unavailable"))?;
        let mut process =
            Session::spawn_at(installation, Deadlines::default(), decl_path.parent())?;
        if (mode == Mode::ManagedBuild && !process.capabilities.managed_build)
            || (mode == Mode::ExternalBuild && !process.capabilities.external_build)
        {
            return Err(public_error(
                ErrorCode::UnsupportedCapability,
                "adapter does not advertise a complete managed build lifecycle",
            )
            .into());
        }
        let original_authoring = authored.clone();
        let mut original = authored;
        declaration::apply_point_defaults(&mut original, &process.registry)
            .map_err(|_| invalid("build defaults failed validation"))?;
        declaration::validate_points(&original, &process.registry)
            .map_err(|_| invalid("build configuration failed validation"))?;
        let effective = process.validate_binding(original.clone(), mode)?;
        declaration::validate_effective(&original, &effective, &process.registry).map_err(
            |_| {
                public_error(
                    ErrorCode::ProtocolFailure,
                    "adapter changed explicit build authoring",
                )
            },
        )?;
        let plans: Vec<_> = effective["tables"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| TablePlan::from_declaration(&effective, t))
            .collect::<Result<_>>()?;
        let selection: Selection = serde_json::from_value(
            effective
                .get("selection")
                .cloned()
                .unwrap_or_else(|| json!({})),
        )
        .map_err(|_| invalid("invalid build selection"))?;
        let preflight = Store::open(root.open(false)?)?;
        let dataset = Name::new(effective["dataset"].as_str().unwrap()).unwrap();
        let base = grv_core::revision::read_latest(&preflight, &dataset)?
            .map(|(l, _)| l.revision)
            .unwrap_or_else(|| grv_storage::model::Counter::new(0).unwrap());
        let preflight_scratch = tempfile::Builder::new()
            .prefix("grv-build-preflight-")
            .tempdir()
            .map_err(|_| {
                public_error(ErrorCode::BackendFailure, "preflight scratch unavailable")
            })?;
        build_publication::validate_selection(
            &preflight,
            &dataset,
            base,
            &plans,
            &selection,
            preflight_scratch.path(),
        )?;
        let run_id = new_run_id(&clock.now())?;
        let locator = process.locate_connection(
            effective["connection"].clone(),
            mode,
            Some(run_id.clone()),
        )?;
        lock_session(&locator, &run_id, &mut mutation)?;
        let engine_path = locator.engine_path.as_ref().map(PathBuf::from);
        if let Purpose::Prepare(path) = &purpose {
            let mut excluded: Vec<&Path> = root.exclusions().iter().map(PathBuf::as_path).collect();
            if let Some(engine) = &engine_path {
                excluded.push(engine);
            }
            grv_adapter_host::protected_document::canonical_path(path, &excluded)?;
        }
        let coordinates = locator.canonical_connection.clone();
        let expected = locator.identity.clone();
        let bound = process.bind_connection(
            locator,
            Some(canonical.into()),
            expected.clone(),
            None,
            mode,
        )?;
        let lookup_only = bound.workspace_id.is_none();
        let workspace = bound.workspace_id.clone().unwrap_or_else(Uuid::v4);
        let mut bound_handle = bound.handle.clone();
        let identity = process.authenticate(bound.handle.clone(), expected.or(bound.identity))?;
        let adapter_identity = grv_types::AdapterIdentity {
            name: process.descriptor.name.clone(),
            package_version: process.descriptor.package_version.clone(),
            interface_version: process.descriptor.interface_version,
            binding_schema_version: process.descriptor.binding_schema_version,
        };
        let digest = grv_types::declaration_digest(&DeclarationIdentity {
            effective_declaration: effective.clone(),
            adapter_identity: adapter_identity.clone(),
            connection_identity: identity.clone(),
            canonical_connection: coordinates.clone(),
        })
        .map_err(|_| invalid("build identity is not JCS interoperable"))?;
        let selected: Vec<_> = plans
            .iter()
            .filter(|p| {
                !selection
                    .hold
                    .iter()
                    .chain(&selection.drop)
                    .any(|s| s.table == p.table && s.partition.is_none())
            })
            .filter(|p| {
                selection.policy != build_publication::Policy::Explicit
                    || selection.include.iter().any(|s| s.table == p.table)
            })
            .map(|p| p.table.clone())
            .collect();
        let inputs = effective
            .pointer("/build/inputs")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|input| {
                let alias = Name::new(
                    input["as"]
                        .as_str()
                        .ok_or_else(|| invalid("build input alias required"))?,
                )
                .map_err(|e| invalid(&e.to_string()))?;
                Ok(BuildInput {
                    alias,
                    relation: input["table"].clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let outputs = effective["tables"]
            .as_array()
            .unwrap()
            .iter()
            .zip(&plans)
            .map(|(t, p)| {
                Ok(BuildOutput {
                    table: p.table.clone(),
                    source: t["source"].clone(),
                    columns: Value::Array(
                        t["columns"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .filter(|c| c.get("derive").is_none())
                            .cloned()
                            .collect(),
                    ),
                    contract: p.input_contract()?,
                })
            })
            .collect::<Result<_>>()?;
        let request = DiscoverBuildRequest {
            identity: BuildIdentity {
                attempt_id: options.attempt.clone(),
                root: canonical.into(),
                dataset: Name::new(effective["dataset"].as_str().unwrap()).unwrap(),
                run_id,
                workspace_id: workspace,
                declaration_sha256: digest,
                adapter_identity,
                connection_identity: identity,
            },
            options: effective
                .get("options")
                .cloned()
                .unwrap_or_else(|| json!({})),
            execution: if mode == Mode::ManagedBuild {
                BuildExecution::Managed
            } else {
                BuildExecution::External
            },
            inputs,
            outputs,
            selected_outputs: selected,
            self_input: effective
                .pointer("/build/self_input")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        };
        request.validate().map_err(|e| invalid(&e.to_string()))?;
        // Check root availability before persisting an attempt; fixed discovery
        // identity is still persisted before the retained engine transaction.
        let store = Store::open(root.open(false)?)?;
        eprintln!("attempt {}", options.attempt);
        journal = Journal::create(&directory, root.exclusions())?;
        record = journal.create_evidence(Evidence {
            intent: Fixed {
                authoring_sha256,
                authored: original_authoring,
                declaration_path: decl_path,
                engine_path,
                root: canonical.into(),
                effective,
                descriptor: process.descriptor.clone(),
                after_publish: process.capabilities.after_publish,
                registry: process.registry.registry.clone(),
                canonical_connection: coordinates,
                request: request.clone(),
                plans,
                selection,
            },
            capture: None,
            progress: Progress::default(),
            terminal: None,
        })?;
        if lookup_only {
            process.close()?;
            (process, bound_handle) =
                open_fixed(&record.evidence.intent, &options.decl, &mut mutation)?;
        }
        save(&journal, &mut record, |e| {
            e.progress.discovery_started = true
        })?;
        let discovery = process.discover_build(bound_handle.clone(), request.clone())?;
        let intent = build::prepare_intent(
            &store,
            &clock,
            ttl(&store),
            request,
            discovery,
            journal.directory(),
        )?;
        build_publication::validate_selection(
            &store,
            intent.run.dataset(),
            intent.run.control().base_revision,
            &record.evidence.intent.plans,
            &record.evidence.intent.selection,
            journal.directory(),
        )?;
        let prepared = build::prepare_session(
            process,
            bound_handle.clone(),
            PreparationContext {
                store: &store,
                clock: &clock,
                ttl: ttl(&store),
                scratch: journal.directory(),
            },
            intent,
            |intent, progress| {
                save(&journal, &mut record, |e| {
                    e.progress.intent = Some(intent.clone());
                    e.progress.preparation = Some(progress.clone());
                    match progress {
                        PreparationProgress::Prepared => {}
                        PreparationProgress::RunCreated { owner }
                        | PreparationProgress::HoldsConfirmed { owner }
                        | PreparationProgress::FilesVerified { owner, .. } => {
                            e.progress.owner = Some(owner.clone())
                        }
                        PreparationProgress::SessionPrepared { owner, session } => {
                            e.progress.owner = Some(owner.clone());
                            e.progress.session = Some((**session).clone());
                        }
                    }
                })
            },
        )?;
        handle = Some(bound_handle);
        active = Some(Active::Prepared(Box::new(prepared)));
    }
    if let Purpose::Prepare(path) = &purpose {
        let store = Store::open(root.open(false)?)?;
        let mut owner = record
            .evidence
            .progress
            .owner
            .clone()
            .ok_or_else(|| incomplete("prepared run missing"))?;
        Ownership::new(&store, &clock, ttl(&store))?.require_open_transfer(&owner)?;
        if active.is_none() {
            let (mut process, handle) =
                open_fixed(&record.evidence.intent, &options.decl, &mut mutation)?;
            let engine =
                process.open_build(handle, record.evidence.intent.request.identity.clone())?;
            if Some(&engine.session) != record.evidence.progress.session.as_ref() {
                return Err(mismatch().into());
            }
            active = Some(Active::Open(Box::new(process)));
        }
        let result = build::supervise(
            &store,
            &clock,
            ttl(&store),
            &mut owner,
            active.as_mut().unwrap().process(),
            |process| {
                let result =
                    expose_context(&journal, &record, path, canonical).map_err(|f| f.error)?;
                process.close().map_err(|e| e.public())?;
                Ok(result)
            },
        )?;
        save(&journal, &mut record, |e| e.progress.owner = Some(owner))?;
        return Ok(result);
    }
    if let Purpose::Finalize(Some(path)) = &purpose {
        let session = record
            .evidence
            .progress
            .session
            .as_ref()
            .ok_or_else(|| incomplete("prepared session missing"))?;
        let mut excluded: Vec<&Path> = root.exclusions().iter().map(PathBuf::as_path).collect();
        if let Some(engine) = &record.evidence.intent.engine_path {
            excluded.push(engine);
        }
        let path = grv_adapter_host::protected_document::canonical_path(path, &excluded)?;
        let completion: BuildCompletion =
            grv_adapter_host::protected_document::read(&path, 2 * 1024 * 1024)?;
        completion
            .validate_for(session)
            .map_err(|_| incomplete("completion does not attest the prepared outputs"))?;
        if record
            .evidence
            .progress
            .accepted
            .as_ref()
            .is_some_and(|a| a.completion != completion)
            || record
                .evidence
                .progress
                .candidate
                .as_ref()
                .is_some_and(|c| c != &completion)
        {
            return Err(mismatch().into());
        }
        if record.evidence.progress.accepted.is_none() {
            save(&journal, &mut record, |e| {
                e.progress.candidate = Some(completion)
            })?;
        }
    }
    let store = Store::open(root.open(false)?)?;
    let lease_ttl = ttl(&store);
    // An existing export plan never requires today’s adapter installation,
    // credentials, local engine, or retained source input bytes.
    if record.evidence.progress.publication.is_some() {
        return publish(
            &store,
            &clock,
            lease_ttl,
            &journal,
            &mut record,
            canonical,
            existing,
        );
    }
    if record.evidence.capture.is_none() {
        let mut owner = record
            .evidence
            .progress
            .owner
            .clone()
            .ok_or_else(|| incomplete("interrupted build discovery cannot be repeated"))?;
        Ownership::new(&store, &clock, lease_ttl)?.require_open_transfer(&owner)?;
        let selected_plans: Vec<_> = record
            .evidence
            .intent
            .plans
            .iter()
            .filter(|p| {
                record
                    .evidence
                    .intent
                    .request
                    .selected_outputs
                    .contains(&p.table)
            })
            .cloned()
            .collect();
        let stopped = record
            .evidence
            .progress
            .export
            .as_ref()
            .is_some_and(|p| p.stopped && p.adapter_result.is_some());
        if !stopped {
            if active.is_none() {
                let (mut process, bound_handle) =
                    open_fixed(&record.evidence.intent, &options.decl, &mut mutation)?;
                let fixed = record.evidence.intent.clone();
                let reopened = build::supervise(
                    &store,
                    &clock,
                    lease_ttl,
                    &mut owner,
                    &mut process,
                    |process| {
                        process
                            .open_build(bound_handle.clone(), fixed.request.identity.clone())
                            .map_err(|e| e.public())
                    },
                )?;
                let recorded = record.evidence.progress.session.as_ref();
                if recorded.is_some_and(|s| s != &reopened.session) {
                    return Err(mismatch().into());
                }
                if recorded.is_none() {
                    let Some(PreparationProgress::FilesVerified { preparation, .. }) =
                        &record.evidence.progress.preparation
                    else {
                        return Err(incomplete(
                            "interrupted materialization has no fixed preparation evidence",
                        )
                        .into());
                    };
                    reopened
                        .session
                        .validate_for(&fixed.request, preparation)
                        .map_err(|_| mismatch())?;
                    save(&journal, &mut record, |e| {
                        e.progress.session = Some(reopened.session.clone())
                    })?;
                }
                if let Some(recorded) = &record.evidence.progress.accepted {
                    let accepted = AcceptedBuild::from_record(&reopened)?;
                    if accepted.completion_sha256 != recorded.completion_sha256 {
                        return Err(mismatch().into());
                    }
                } else if reopened.completion.is_some() {
                    let accepted = AcceptedBuild::from_record(&reopened)?;
                    save(&journal, &mut record, |e| {
                        e.progress.accepted = Some(accepted)
                    })?;
                } else if let Some(candidate) = reopened.candidate {
                    if record
                        .evidence
                        .progress
                        .candidate
                        .as_ref()
                        .is_some_and(|c| c != &candidate)
                    {
                        return Err(mismatch().into());
                    }
                    save(&journal, &mut record, |e| {
                        e.progress.candidate = Some(candidate)
                    })?;
                } else if mode == Mode::ManagedBuild
                    && (record.evidence.progress.execution_started
                        || reopened.state != BuildState::Prepared)
                {
                    return Err(incomplete("incomplete managed invocation cannot be rerun").into());
                }
                active = Some(Active::Open(Box::new(process)));
                handle = Some(bound_handle);
            }
            let process = active.as_mut().unwrap().process();
            let handle = handle.clone().unwrap();
            if record.evidence.progress.accepted.is_none() {
                let session = record
                    .evidence
                    .progress
                    .session
                    .clone()
                    .ok_or_else(|| incomplete("prepared engine session missing"))?;
                if record.evidence.progress.candidate.is_none() {
                    if mode == Mode::ExternalBuild {
                        return Err(incomplete(
                            "external finalization requires a stopped successful completion record",
                        )
                        .into());
                    }
                    if record.evidence.progress.execution_started {
                        return Err(incomplete("managed execution was already dispatched").into());
                    }
                    let queries = session
                        .selected_outputs
                        .iter()
                        .map(|table| {
                            let output =
                                session.outputs.iter().find(|o| &o.table == table).unwrap();
                            Ok(BuildQuery {
                                table: table.clone(),
                                sql: output.source["sql"]
                                    .as_str()
                                    .ok_or_else(|| invalid("managed SQL was not expanded"))?
                                    .into(),
                            })
                        })
                        .collect::<Result<_>>()?;
                    build::supervise(&store, &clock, lease_ttl, &mut owner, process, |process| {
                        save(&journal, &mut record, |e| {
                            e.progress.execution_started = true
                        })?;
                        let result = process
                            .execute_build(
                                handle.clone(),
                                ExecuteBuildRequest {
                                    session: session.clone(),
                                    queries,
                                },
                            )
                            .map_err(|e| e.public())?;
                        if result.status == BuildExecutionStatus::Succeeded {
                            let candidate = result.completion.ok_or_else(|| {
                                public_error(
                                    ErrorCode::ProtocolFailure,
                                    "successful build omitted completion",
                                )
                            })?;
                            save(&journal, &mut record, |e| {
                                e.progress.candidate = Some(candidate)
                            })
                        } else {
                            let error = public_error(
                                ErrorCode::EngineFailure,
                                "managed invocation failed; a new attempt is required",
                            );
                            save(&journal, &mut record, |e| {
                                e.progress.failure = Some(error.clone())
                            })?;
                            Err(error)
                        }
                    })?;
                }
                let candidate = record.evidence.progress.candidate.clone().unwrap();
                let bytes = grv_types::canonical_json(&candidate)
                    .map_err(|_| invalid("completion encoding failed"))?;
                let key = grv_storage::ObjectKey::new("build/completion.json").unwrap();
                match journal.create_artifact(&key, &mut Cursor::new(&bytes)) {
                    Ok(_) => {}
                    Err(e)
                        if e.code == ErrorCode::StateConflict
                            || e.code == ErrorCode::OutcomeUnknown =>
                    {
                        let (stored, _) = journal
                            .head_artifact(&key)
                            .map(|m| (m.size.get(), m.validator))?;
                        if stored > 64 * 1024 * 1024 {
                            return Err(invalid("completion exceeds metadata budget").into());
                        }
                        let mut old = vec![];
                        journal.get_artifact(&key, &mut old)?;
                        if stored != bytes.len() as u64 || old != bytes {
                            return Err(mismatch().into());
                        }
                    }
                    Err(e) => return Err(e.into()),
                }
                build::supervise(&store, &clock, lease_ttl, &mut owner, process, |process| {
                    build_export::accept(process, handle.clone(), session, candidate, |phase| {
                        save(&journal, &mut record, |e| match phase {
                            CompletionProgress::Candidate { value } => {
                                e.progress.candidate = Some(value.completion.clone())
                            }
                            CompletionProgress::Accepted { value } => {
                                e.progress.accepted = Some((**value).clone())
                            }
                        })
                    })
                    .map(|_| ())
                })?;
            }
            save(&journal, &mut record, |e| {
                e.progress.owner = Some(owner.clone())
            })?;
            let accepted = record.evidence.progress.accepted.clone().unwrap();
            let ownership = Ownership::new(&store, &clock, lease_ttl)?;
            let job = ExportJob {
                ownership: &ownership,
                run: &owner,
                journal: &journal,
                accepted: &accepted,
                plans: &selected_plans,
            };
            // Accepted immutable outputs permit a fresh export stream after a
            // partial transfer. Raw artifacts from older streams remain separate.
            let mut export = ExportProgress::prepared(&accepted, Uuid::v4());
            save(&journal, &mut record, |e| {
                e.progress.export = Some(export.clone())
            })?;
            job.acquire(process, handle, &mut export, |p| {
                save(&journal, &mut record, |e| {
                    e.progress.export = Some(p.clone())
                })
            })?;
        }
        let accepted = record
            .evidence
            .progress
            .accepted
            .clone()
            .ok_or_else(|| incomplete("accepted completion missing"))?;
        let ownership = Ownership::new(&store, &clock, lease_ttl)?;
        let job = ExportJob {
            ownership: &ownership,
            run: &owner,
            journal: &journal,
            accepted: &accepted,
            plans: &selected_plans,
        };
        let receipt = job.canonicalize(record.evidence.progress.export.as_ref().unwrap())?;
        save(&journal, &mut record, |e| e.capture = Some(receipt))?;
    }
    let owner = record.evidence.progress.owner.clone().unwrap();
    let fixed = record.evidence.intent.clone();
    let plan = build_publication::prepare_plan(
        &store,
        &clock,
        lease_ttl,
        &owner,
        record.evidence.capture.as_ref().unwrap(),
        &fixed.plans,
        &fixed.selection,
        &journal,
        journal.directory(),
    )?;
    let progress = build_publication::Progress::planned(owner, plan)?;
    save(&journal, &mut record, |e| {
        e.progress.publication = Some(progress)
    })?;
    // The capture reader is closed before publication and cache outcome work.
    drop(active);
    let result = publish(
        &store,
        &clock,
        lease_ttl,
        &journal,
        &mut record,
        canonical,
        existing,
    )?;
    if !existing || matches!(purpose, Purpose::Finalize(_)) {
        let maintenance = (|| -> Result<()> {
            let (mut process, handle) = open_fixed(&fixed, &options.decl, &mut mutation)?;
            let engine = process
                .open_build(handle.clone(), fixed.request.identity.clone())
                .map_err(|e| e.public())?;
            let terminal = &record.evidence.terminal.as_ref().unwrap()["outcome"];
            let outcome = BuildOutcome {
                kind: if terminal["kind"] == "no-op" {
                    OutcomeKind::NoOp
                } else {
                    OutcomeKind::Published
                },
                revision: Some(
                    U64::new(terminal["revision"].as_str().unwrap().parse().unwrap()).unwrap(),
                ),
                operation_id: serde_json::from_value(terminal["operation_id"].clone())
                    .map_err(|_| mismatch())?,
            };
            process
                .record_build_outcome(handle.clone(), engine.session.session_id.clone(), outcome)
                .map_err(|e| e.public())?;
            process
                .cleanup_build(handle, engine.session.session_id)
                .map_err(|e| e.public())?;
            process.close().map_err(|e| e.public())
        })();
        if let Err(error) = maintenance {
            return Err(Failure {
                error,
                result: Some(result.result),
                root: Some(canonical.into()),
            });
        }
    }
    acknowledge(&journal, &mut record, result.result, canonical)
}

fn lock_session(
    locator: &ConnectionLocator,
    run_id: &RunId,
    mutation: &mut Option<SessionMutationLock>,
) -> Result<()> {
    if let Some(engine) = &locator.engine_path {
        if let Some(lock) = mutation {
            lock.recheck().map_err(|e| e.public())?;
            if lock.run_id() != run_id || lock.engine_path() != Path::new(engine) {
                return Err(mismatch());
            }
        } else {
            *mutation = Some(
                SessionMutationLock::acquire(Path::new(engine), run_id).map_err(|e| e.public())?,
            );
        }
        if let Some(path) = &locator.session_lock_path
            && mutation.as_ref().unwrap().helper_path() != Path::new(path)
        {
            return Err(mismatch());
        }
    } else if locator.session_lock_path.is_some() {
        return Err(public_error(
            ErrorCode::ProtocolFailure,
            "session lock has no canonical engine path",
        ));
    }
    Ok(())
}
fn open_fixed(
    fixed: &Fixed,
    declaration: &Path,
    mutation: &mut Option<SessionMutationLock>,
) -> Result<(Session, Handle)> {
    let roots = discovery::search_roots(None).map_err(|e| e.public())?;
    let installed = discovery::discover(&roots).map_err(|e| e.public())?;
    let installation = installed
        .iter()
        .find(|i| i.manifest.name == fixed.descriptor.name)
        .ok_or_else(|| public_error(ErrorCode::NotFound, "fixed build adapter unavailable"))?;
    let declaration = declaration.to_path_buf();
    let mode = if fixed.request.execution == BuildExecution::Managed {
        Mode::ManagedBuild
    } else {
        Mode::ExternalBuild
    };
    let mut process = Session::spawn_at(installation, Deadlines::default(), declaration.parent())
        .map_err(|e| e.public())?;
    if process.descriptor != fixed.descriptor || process.registry.registry != fixed.registry {
        return Err(mismatch());
    }
    let expected = Some(fixed.request.identity.connection_identity.clone());
    let locator = process
        .locate_connection(
            fixed.canonical_connection.clone(),
            mode,
            Some(fixed.request.identity.run_id.clone()),
        )
        .map_err(|e| e.public())?;
    if locator.canonical_connection != fixed.canonical_connection
        || locator
            .identity
            .as_ref()
            .is_some_and(|i| Some(i) != expected.as_ref())
    {
        return Err(mismatch());
    }
    lock_session(&locator, &fixed.request.identity.run_id, mutation)?;
    let bound = process
        .bind_connection(
            locator,
            Some(fixed.root.clone()),
            expected.clone(),
            Some(fixed.request.identity.workspace_id.clone()),
            mode,
        )
        .map_err(|e| e.public())?;
    let identity = process
        .authenticate(bound.handle.clone(), expected)
        .map_err(|e| e.public())?;
    if identity != fixed.request.identity.connection_identity {
        return Err(mismatch());
    }
    Ok((process, bound.handle))
}
fn publish<B: grv_storage::Backend>(
    store: &Store<B>,
    clock: &dyn Clock,
    ttl: u64,
    journal: &Journal,
    record: &mut Record,
    canonical: &str,
    replayed: bool,
) -> std::result::Result<Success, Failure> {
    let mut progress = record.evidence.progress.publication.clone().unwrap();
    let outcome = Finalizer::new(store, clock, ttl)?.finalize(
        &mut progress,
        journal,
        journal.directory(),
        |p| {
            save(journal, record, |e| {
                e.progress.publication = Some(p.clone())
            })
        },
    )?;
    finish(journal, record, outcome, canonical, replayed)
}
fn finish(
    journal: &Journal,
    record: &mut Record,
    outcome: PushOutcome,
    canonical: &str,
    replayed: bool,
) -> std::result::Result<Success, Failure> {
    let fixed = &record.evidence.intent;
    let receipt =
        record.evidence.capture.as_ref().ok_or_else(|| {
            public_error(ErrorCode::IntegrityFailure, "fixed build capture missing")
        })?;
    CheckedRegistry::recorded(fixed.registry.clone())
        .map_err(|e| e.public())?
        .validate_point(
            if fixed.request.execution == BuildExecution::Managed {
                Mode::ManagedBuild
            } else {
                Mode::ExternalBuild
            },
            "push_result",
            &receipt.adapter_result,
            ErrorCode::ProtocolFailure,
        )
        .map_err(|e| e.public())?;
    let operation = record
        .evidence
        .progress
        .publication
        .as_ref()
        .and_then(|p| p.publication.as_ref())
        .and_then(|p| p.publication.as_ref())
        .filter(|_| !outcome.no_op)
        .map(|p| p.operation.operation_id.clone());
    let identity = &fixed.request.identity;
    let result = json!({"adapter":identity.adapter_identity.name,"mode":"build","attempt_id":identity.attempt_id,"dataset":identity.dataset,"run_id":identity.run_id,
        "completion_sha256":receipt.accepted.completion_sha256,"outcome":{"kind":if outcome.no_op{"no-op"}else{"published"},"revision":outcome.revision.to_string(),"operation_id":operation},"replayed":replayed,"adapter_result":receipt.adapter_result});
    let mut terminal = result.clone();
    terminal["replayed"] = json!(false);
    let hook_required = fixed.after_publish;
    save(journal, record, |e| {
        e.terminal = Some(terminal);
        if hook_required {
            e.progress.after_publish = Some(super::after_publish::State::Pending);
        }
    })
    .map_err(|error| Failure {
        error,
        result: Some(result.clone()),
        root: Some(canonical.into()),
    })?;
    if let Some(error) = outcome.maintenance_error {
        return Err(Failure {
            error,
            result: Some(result),
            root: Some(canonical.into()),
        });
    }
    Ok(Success {
        result,
        root: Some(canonical.into()),
    })
}
fn acknowledge(
    journal: &Journal,
    record: &mut Record,
    result: Value,
    canonical: &str,
) -> std::result::Result<Success, Failure> {
    let mut ack = || -> Result<()> {
        let fixed = &record.evidence.intent;
        super::after_publish::validate_state(
            fixed.after_publish,
            record.evidence.progress.after_publish,
        )?;
        if record.evidence.progress.after_publish == Some(super::after_publish::State::Pending) {
            let identity = &fixed.request.identity;
            super::after_publish::call(
                super::after_publish::Fixed {
                    descriptor: &fixed.descriptor,
                    registry: &fixed.registry,
                    connection: &fixed.effective["connection"],
                    canonical_connection: &fixed.canonical_connection,
                    identity: &identity.connection_identity,
                    root: &fixed.root,
                    workspace: Some(&identity.workspace_id),
                    run: Some(&identity.run_id),
                    mode: fixed.request.execution.mode(),
                    attempt: &identity.attempt_id,
                    declaration_sha256: &identity.declaration_sha256,
                    declaration_path: &fixed.declaration_path,
                },
                &result,
            )?;
            save(journal, record, |e| {
                e.progress.after_publish = Some(super::after_publish::State::Complete)
            })?;
        }
        Ok(())
    };
    ack().map_err(|error| Failure {
        error,
        result: Some(result.clone()),
        root: Some(canonical.into()),
    })?;
    Ok(Success {
        result,
        root: Some(canonical.into()),
    })
}

fn context_from(journal: &Journal, record: &Record) -> Result<Context> {
    Ok(Context {
        mode: "build".into(),
        context_version: 1,
        directory: journal.directory().into(),
        fixed: record.evidence.intent.clone(),
        intent: record
            .evidence
            .progress
            .intent
            .clone()
            .ok_or_else(|| incomplete("fixed preparation missing"))?,
        session: record
            .evidence
            .progress
            .session
            .clone()
            .ok_or_else(|| incomplete("prepared engine session missing"))?,
    })
}
fn read_context(path: &Path) -> Result<(PathBuf, Context, Root)> {
    use grv_adapter_host::protected_document;
    let path = protected_document::canonical_path(path, &[]).map_err(|e| e.public())?;
    let context: Context =
        protected_document::read(&path, 64 * 1024 * 1024).map_err(|e| e.public())?;
    let root = Root::parse(&context.fixed.root)?;
    let mut excluded: Vec<&Path> = root.exclusions().iter().map(PathBuf::as_path).collect();
    if let Some(engine) = &context.fixed.engine_path {
        excluded.push(engine);
    }
    protected_document::canonical_path(&path, &excluded).map_err(|e| e.public())?;
    if context.mode != "build"
        || context.context_version != 1
        || root.canonical != context.fixed.root
        || context.fixed.request.execution != BuildExecution::External
        || context.directory.file_name().and_then(|s| s.to_str())
            != Some(context.fixed.request.identity.attempt_id.as_str())
        || context
            .directory
            .parent()
            .and_then(Path::file_name)
            .and_then(|s| s.to_str())
            != Some("push")
    {
        return Err(mismatch());
    }
    context.intent.validate()?;
    context.session.validate().map_err(|_| mismatch())?;
    if context.intent.request != context.fixed.request
        || context.session.identity != context.fixed.request.identity
        || grv_types::sha256(
            &grv_types::canonical_json(&context.fixed.authored).map_err(|_| mismatch())?,
        ) != context.fixed.authoring_sha256
    {
        return Err(mismatch());
    }
    Ok((path, context, root))
}
fn open_context(context: &Context, root: &Root) -> Result<(Journal, Record)> {
    let mut excluded = root.exclusions().to_vec();
    if let Some(engine) = &context.fixed.engine_path {
        excluded.push(engine.clone());
    }
    let journal = Journal::open(&context.directory, &excluded)?;
    let record: Record = journal.read()?;
    let actual = context_from(&journal, &record)?;
    if grv_types::canonical_json(&actual).map_err(|_| mismatch())?
        != grv_types::canonical_json(context).map_err(|_| mismatch())?
    {
        return Err(mismatch());
    }
    Ok((journal, record))
}
fn context_lock(context: &Context) -> Result<Option<SessionMutationLock>> {
    context
        .fixed
        .engine_path
        .as_ref()
        .map(|path| {
            SessionMutationLock::acquire(path, &context.fixed.request.identity.run_id)
                .map_err(|e| e.public())
        })
        .transpose()
}
fn observed_run(root: &Root, context: &Context) -> Result<grv_storage::model::RunControl> {
    let backend = root.open(false)?;
    let id = &context.fixed.request.identity;
    let key = grv_storage::ObjectKey::new(format!(
        "datasets/{}/.runs/{}.control.json",
        id.dataset, id.run_id
    ))
    .unwrap();
    let bytes = backend
        .read_bytes(&key, 64 * 1024 * 1024)
        .map_err(grv_core::store::backend_error)?
        .0;
    let observed: grv_storage::model::RunControl =
        grv_storage::model::decode_record(&bytes).map_err(grv_core::store::backend_error)?;
    let fixed = context.intent.run.control();
    if observed.run_id != id.run_id
        || observed.base_revision != fixed.base_revision
        || observed.created_at != fixed.created_at
        || observed.inputs != fixed.inputs
        || observed.metadata != fixed.metadata
    {
        return Err(mismatch());
    }
    Ok(observed)
}
fn context_result(
    path: &Path,
    context: &Context,
    record: &Record,
    observed: &grv_storage::model::RunControl,
) -> Result<Value> {
    let session = &context.session;
    let inputs = context.intent.run.control().inputs.iter().map(|i| json!({"dataset":i.dataset,"revision":i.revision.to_string(),"retention_id":i.retention_id})).collect::<Vec<_>>();
    let details = session
        .adapter_details
        .get("input_mappings")
        .and_then(Value::as_array);
    let mappings = session.inputs.iter().map(|input| {
        let physical = details.and_then(|v| v.iter().find(|v| v["alias"] == input.alias.as_str()))
            .and_then(|v| v["engine_table"].as_str()).ok_or_else(|| public_error(ErrorCode::ProtocolFailure,"adapter omitted physical input mapping"))?;
        let hold = context.intent.run.control().inputs.iter().find(|i| i.dataset == input.dataset && i.revision.get() == input.revision.get()).ok_or_else(mismatch)?;
        let source = input.relation.as_str().ok_or_else(mismatch)?;
        Ok(json!({"alias":input.alias,"engine_table":physical,"source_table":source,"dataset":input.dataset,"grv_table":input.table,"revision":input.revision,"generation_id":input.generation_id,"retention_id":hold.retention_id}))
    }).collect::<Result<Vec<_>>>()?;
    let expired = observed.expires_at.as_ref().is_some_and(|t| {
        grv_core::clock::parse(t) <= grv_core::clock::parse(&SystemClock::default().now())
    });
    let outcome = record
        .evidence
        .terminal
        .as_ref()
        .and_then(|v| v.get("outcome"))
        .cloned()
        .or_else(|| {
            record
                .evidence
                .progress
                .aborted
                .as_ref()
                .and_then(|v| v.get("outcome"))
                .cloned()
        });
    let export = record.evidence.progress.publication.as_ref().map(|p| {
        let contributions = p.plan.groups.iter().map(|g| {
            let (action,version,run_id) = match g.action {
                grv_core::push::ExportAction::Write => ("write",None,Some(session.identity.run_id.clone())),
                grv_core::push::ExportAction::Reuse{version} => ("reuse",Some(version.to_string()),None),
            };
            json!({"table":g.plan.table,"partition":g.capture.partition,"action":action,"version":version,"run_id":run_id})
        }).collect::<Vec<_>>();
        json!({"base_revision":p.plan.base_revision.to_string(),"completion_sha256":record.evidence.progress.accepted.as_ref().map(|a|&a.completion_sha256),"contributions":contributions,"omissions":p.plan.omissions,"held_tables":context.fixed.selection.hold.iter().filter(|s|s.partition.is_none()).map(|s|s.table.clone()).collect::<Vec<_>>()})
    });
    let publication = record.evidence.progress.publication.as_ref().and_then(|p|p.publication.as_ref())
        .and_then(|p|p.publication.as_ref()).and_then(|p| {
            if let grv_storage::model::OperationPayload::Publish(payload) = &p.operation.body {
                Some(json!({"operation_id":p.operation.operation_id,"reserved_revision":payload.revision.to_string(),"state":if outcome.is_some(){"committed"}else{"unknown"}}))
            } else {None}
        });
    Ok(
        json!({"context_path":path,"engine_path":context.fixed.engine_path,"state_path":context.directory,"session":{
            "dataset":session.identity.dataset,"run_id":session.identity.run_id,"workspace_id":session.identity.workspace_id,
            "base_revision":session.base_revision,"declaration_sha256":session.identity.declaration_sha256,"input_revisions":inputs,
            "mappings":{"inputs":mappings,"outputs":session.outputs.iter().map(|o|json!({"table":o.table,"engine_table":o.engine_table})).collect::<Vec<_>>()},
            "completion_sha256":record.evidence.progress.accepted.as_ref().map(|a|&a.completion_sha256),"export_plan":export,
            "publication_attempt":publication,"outcome":outcome,"run":{"run_id":observed.run_id,"phase":observed.phase,
            "base_revision":observed.base_revision.to_string(),"expires_at":observed.expires_at,"expired":expired,"holds_confirmed":observed.holds_confirmed},
            "adapter":session.identity.adapter_identity.name,"mode":"build","attempt_id":session.identity.attempt_id,"capture":null,"adapter_context":session.adapter_details
        }}),
    )
}
fn expose_context(
    journal: &Journal,
    record: &Record,
    path: &Path,
    canonical: &str,
) -> std::result::Result<Success, Failure> {
    let context = context_from(journal, record)?;
    let root = Root::parse(canonical)?;
    let mut excluded: Vec<&Path> = root.exclusions().iter().map(PathBuf::as_path).collect();
    if let Some(engine) = &context.fixed.engine_path {
        excluded.push(engine);
    }
    let path = grv_adapter_host::protected_document::canonical_path(path, &excluded)?;
    grv_adapter_host::protected_document::publish(&path, &context, 64 * 1024 * 1024)?;
    let run = observed_run(&root, &context)?;
    Ok(Success {
        result: context_result(&path, &context, record, &run)?,
        root: Some(canonical.into()),
    })
}
fn session_path(args: &[String]) -> Result<PathBuf> {
    if args.len() != 2 || args[0] != "--session" {
        return Err(public_error(
            ErrorCode::InvalidArgument,
            "session command requires --session <context.json>",
        ));
    }
    Ok(PathBuf::from(&args[1]))
}
pub(super) fn session(args: &[String]) -> std::result::Result<Success, Failure> {
    let all_args = args;
    let command = args
        .first()
        .ok_or_else(|| invalid("session command required"))?;
    let args = &args[1..];
    if command == "prepare" {
        let mut transfer = vec![];
        let mut context = None;
        for pair in args.chunks(2) {
            if pair.len() != 2 {
                return Err(invalid("session preparation flags require values").into());
            }
            if pair[0] == "--session" {
                if context.replace(PathBuf::from(&pair[1])).is_some() {
                    return Err(invalid("duplicate session path").into());
                }
            } else {
                transfer.extend_from_slice(pair);
            }
        }
        let path = context.ok_or_else(|| invalid("session preparation requires --session"))?;
        let options = super::transfer::options(&transfer)?;
        let root = Root::parse(&options.root)?;
        let canonical = root.canonical.clone();
        let authored = super::transfer::load_authored(&options, &root)?;
        if declaration::mode(&authored).map_err(|_| invalid("invalid session mode"))?
            == Mode::Extract
        {
            return super::transfer::session(all_args);
        }
        return run_internal(options, root, &canonical, authored, Purpose::Prepare(path));
    }
    let path = session_path(args)?;
    if context_mode(&path)? == "extract" {
        return super::transfer::session(all_args);
    }
    let (path, context, root) = read_context(&path)?;
    let mut mutation = context_lock(&context)?;
    let (journal, mut record) = open_context(&context, &root)?;
    match command.as_str() {
        "show" => {
            let (mut process, handle) = open_fixed(
                &context.fixed,
                &context.fixed.declaration_path,
                &mut mutation,
            )?;
            let engine = process.open_build(handle, context.session.identity.clone())?;
            if engine.session != context.session {
                return Err(mismatch().into());
            }
            process.close()?;
            let run = observed_run(&root, &context)?;
            Ok(Success {
                result: context_result(&path, &context, &record, &run)?,
                root: Some(root.canonical),
            })
        }
        "renew" => {
            if record.evidence.terminal.is_some() || record.evidence.progress.aborted.is_some() {
                return Err(public_error(
                    ErrorCode::StateConflict,
                    "terminal session cannot renew",
                )
                .into());
            }
            let store = Store::open(root.open(false)?)?;
            let clock = SystemClock::default();
            let mut owner = record
                .evidence
                .progress
                .owner
                .clone()
                .ok_or_else(|| incomplete("run ownership missing"))?;
            let ownership = Ownership::new(&store, &clock, ttl(&store))?;
            ownership.require_open_transfer(&owner)?;
            ownership.renew_run(&mut owner)?;
            save(&journal, &mut record, |e| {
                e.progress.owner = Some(owner.clone())
            })?;
            Ok(Success {
                result: json!({"dataset":owner.dataset(),"run_id":owner.control().run_id,"phase":"open","expires_at":owner.control().expires_at,"renewed":true}),
                root: Some(root.canonical),
            })
        }
        "abort" => abort_context(&context, &root, &journal, &mut record, &mut mutation),
        _ => Err(invalid("unsupported session command").into()),
    }
}
pub(super) fn finalize_context(args: &[String]) -> std::result::Result<Success, Failure> {
    let mut path = None;
    let mut completion = None;
    let mut seen = std::collections::BTreeSet::new();
    for pair in args.chunks(2) {
        if pair.len() != 2 || !seen.insert(&pair[0]) {
            return Err(invalid("session finalization flags must be unique").into());
        }
        match pair[0].as_str() {
            "--session" => path = Some(PathBuf::from(&pair[1])),
            "--build-result" => completion = Some(PathBuf::from(&pair[1])),
            _ => return Err(invalid("session finalization forbids declaration overrides").into()),
        }
    }
    let path = path.ok_or_else(|| invalid("session path required"))?;
    if context_mode(&path)? == "extract" {
        return super::transfer::finalize_context(args);
    }
    let (_, context, root) = read_context(&path)?;
    // Validate the exact attempt index before any source or terminal lookup.
    let (journal, _record) = open_context(&context, &root)?;
    drop(journal);
    let options = Options {
        root: root.canonical.clone(),
        decl: context.fixed.declaration_path.clone(),
        state: context
            .directory
            .parent()
            .and_then(Path::parent)
            .ok_or_else(mismatch)?
            .into(),
        attempt: context.session.identity.attempt_id.clone(),
    };
    let canonical = root.canonical.clone();
    run_internal(
        options,
        root,
        &canonical,
        context.fixed.authored,
        Purpose::Finalize(completion),
    )
}
fn abort_summary(context: &Context, record: &Record, outcome: &Value) -> Value {
    let sealed = record
        .evidence
        .progress
        .publication
        .as_ref()
        .and_then(|p| p.publication.as_ref())
        .and_then(|p| p.sealed.as_ref());
    let entries = sealed.map(|run|run.entries.iter().map(|entry|json!({"table":entry.table,"partition":entry.partition,"version":entry.version.to_string(),"run_id":run.run_id})).collect::<Vec<_>>()).unwrap_or_default();
    json!({"dataset":context.session.identity.dataset,"run_id":context.session.identity.run_id,"outcome":outcome,"finalized_entries":entries})
}
fn abort_context(
    context: &Context,
    root: &Root,
    journal: &Journal,
    record: &mut Record,
    mutation: &mut Option<SessionMutationLock>,
) -> std::result::Result<Success, Failure> {
    if let Some(result) = &record.evidence.progress.aborted {
        return Ok(Success {
            result: result.clone(),
            root: Some(root.canonical.clone()),
        });
    }
    if let Some(result) = &record.evidence.terminal {
        return Ok(Success {
            result: abort_summary(context, record, &result["outcome"]),
            root: Some(root.canonical.clone()),
        });
    }
    let store = Store::open(root.open(false)?)?;
    let clock = SystemClock::default();
    if record.evidence.progress.abort_progress.is_none() {
        let owner = record
            .evidence
            .progress
            .owner
            .clone()
            .ok_or_else(|| incomplete("run ownership missing"))?;
        let progress = grv_core::build_abort::Progress::prepare(
            owner,
            record.evidence.progress.publication.clone(),
        )?;
        save(journal, record, |e| {
            e.progress.abort_progress = Some(progress)
        })?;
    }
    let mut progress = record.evidence.progress.abort_progress.clone().unwrap();
    let mut active = None;
    let outcome = grv_core::build_abort::Aborter::new(&store, &clock, ttl(&store))?.abort(
        &mut progress,
        journal.directory(),
        |progress| {
            save(journal, record, |e| {
                e.progress.owner = Some(progress.owner.clone());
                e.progress.publication = progress.publication.clone();
                e.progress.abort_progress = Some(progress.clone());
            })
        },
        || {
            if active.is_none() {
                let (mut process, handle) =
                    open_fixed(&context.fixed, &context.fixed.declaration_path, mutation)?;
                let engine = process
                    .open_build(handle.clone(), context.session.identity.clone())
                    .map_err(|e| e.public())?;
                if engine.session != context.session {
                    return Err(mismatch());
                }
                process
                    .abort_build(handle.clone(), context.session.session_id.clone())
                    .map_err(|e| e.public())?;
                active = Some((process, handle));
            }
            if let Some(lock) = mutation {
                lock.recheck().map_err(|e| e.public())?;
            }
            Ok(())
        },
    )?;
    let sealed = match outcome {
        grv_core::build_abort::Outcome::Published { outcome } => {
            let result = finish(journal, record, outcome, &root.canonical, true);
            return match result {
                Ok(success) => Ok(Success {
                    result: abort_summary(context, record, &success.result["outcome"]),
                    root: success.root,
                }),
                Err(mut failure) => {
                    failure.result = failure
                        .result
                        .as_ref()
                        .map(|r| abort_summary(context, record, &r["outcome"]));
                    Err(failure)
                }
            };
        }
        grv_core::build_abort::Outcome::Aborted { sealed } => sealed,
    };
    let entries=sealed.entries.iter().map(|entry|json!({"table":entry.table,"partition":entry.partition,"version":entry.version.to_string(),"run_id":sealed.run_id})).collect::<Vec<_>>();
    let result = json!({"dataset":context.session.identity.dataset,"run_id":context.session.identity.run_id,"outcome":{"kind":"aborted","revision":null,"operation_id":null},"finalized_entries":entries});
    save(journal, record, |e| {
        e.progress.aborted = Some(result.clone())
    })?;
    let maintenance = (|| -> Result<()> {
        if let Some((mut process, handle)) = active {
            process
                .record_build_outcome(
                    handle.clone(),
                    context.session.session_id.clone(),
                    BuildOutcome {
                        kind: OutcomeKind::Aborted,
                        revision: None,
                        operation_id: None,
                    },
                )
                .map_err(|e| e.public())?;
            process
                .cleanup_build(handle, context.session.session_id.clone())
                .map_err(|e| e.public())?;
            process.close().map_err(|e| e.public())?;
        }
        Ok(())
    })();
    if let Err(error) = maintenance {
        return Err(Failure {
            error,
            result: Some(result),
            root: Some(root.canonical.clone()),
        });
    }
    Ok(Success {
        result,
        root: Some(root.canonical.clone()),
    })
}

/// Called only while the trusted local coordinator retains this journal lock.
/// Source work must already have reached a clean stopped export acknowledgement.
pub(super) fn stopped_export_proof(
    journal: &Journal,
    dataset: &Name,
    control: &grv_storage::model::RunControl,
    canonical: &str,
) -> Result<Option<Digest>> {
    let record: Record = journal.read()?;
    let Some(intent) = record.evidence.progress.intent.as_ref() else {
        return Ok(None);
    };
    let fixed = intent.run.control();
    intent.validate()?;
    if record.evidence.intent.root != canonical
        || intent.run.dataset() != dataset
        || fixed.run_id != control.run_id
        || fixed.owner_token != control.owner_token
        || fixed.created_at != control.created_at
        || fixed.base_revision != control.base_revision
        || fixed.inputs != control.inputs
        || fixed.metadata != control.metadata
    {
        return Ok(None);
    }
    let Some(accepted) = record.evidence.progress.accepted.as_ref() else {
        return Ok(None);
    };
    accepted.validate()?;
    if accepted.session.identity != intent.request.identity
        || Some(&accepted.session) != record.evidence.progress.session.as_ref()
        || accepted.session.inputs != intent.discovery.inputs
        || accepted.session.outputs != intent.discovery.outputs
    {
        return Ok(None);
    }
    let Some(export) = record.evidence.progress.export.as_ref() else {
        return Ok(None);
    };
    export.validate(accepted)?;
    if !export.started
        || !export.stopped
        || export.adapter_result.is_none()
        || export.session_id != accepted.session.session_id
        || export.completion_sha256 != accepted.completion_sha256
        || export.tables.len() != accepted.session.selected_outputs.len()
        || export
            .tables
            .iter()
            .any(|t| t.completion != Some(t.row_count))
    {
        return Ok(None);
    }
    let proof = grv_types::canonical_json(&serde_json::json!({
        "kind":"local-stopped-build-export","root":canonical,"dataset":dataset,
        "run_id":control.run_id,"owner_token":control.owner_token,"mutation_id":control.mutation_id,
        "attempt_id":record.evidence.intent.request.identity.attempt_id,
        "completion_sha256":accepted.completion_sha256,"export":export,
    }))
    .map_err(|_| mismatch())?;
    Ok(Some(grv_types::sha256(&proof)))
}

fn context_mode(path: &Path) -> Result<String> {
    let value: Value = grv_adapter_host::protected_document::read(path, 64 * 1024 * 1024)
        .map_err(|e| e.public())?;
    match value["mode"].as_str() {
        Some(mode @ ("build" | "extract")) => Ok(mode.into()),
        _ => Err(mismatch()),
    }
}
