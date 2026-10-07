//! Extraction CLI orchestration. Terminal evidence is read before discovery,
//! authentication, source access or backend availability checks.
use super::{Failure, Success};
use grv_adapter_api::{
    AdapterDescriptor, ExtractRequest, ExtractSelection, ExtractTable, Mode, Registry,
    SelectionPolicy,
};
use grv_adapter_host::{
    discovery,
    process::{Deadlines, Session},
    registry::CheckedRegistry,
};
use grv_core::{
    canonical::CANONICAL_WRITER,
    capture::{AcquisitionProgress, CaptureJob, CaptureReceipt},
    clock::{Clock, SystemClock, new_run_id},
    declaration,
    journal::{Envelope, Evidence, Journal},
    normalize::TablePlan,
    ownership::{AbortClaim, AbortClaimProgress, Ownership, RunOwner},
    publication::Publisher,
    push::{ExportPlan, GroupProgress, PushFinalizer, PushOutcome, PushPolicy, PushProgress},
    revision,
    store::{Result, Store, public_error},
};
use grv_storage::{Backend, model::Counter};
use grv_types::{AdapterIdentity, DeclarationIdentity, Digest, ErrorCode, Name, Uuid};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    time::Duration,
};
const TTL: u64 = 900;
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixed {
    authoring_sha256: Digest,
    authored: Value,
    declaration_path: PathBuf,
    registry: Registry,
    root: String,
    effective_declaration: Value,
    descriptor: AdapterDescriptor,
    after_publish: bool,
    canonical_connection: Value,
    request: ExtractRequest,
    run: RunOwner,
    plans: Vec<TablePlan>,
    drops: Vec<Name>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Progress {
    acquisition: AcquisitionProgress,
    #[serde(deserialize_with = "required_option")]
    push: Option<PushProgress>,
    owner: Option<RunOwner>,
    aborted: Option<Value>,
    abort_claims: Vec<(AbortClaim, AbortClaimProgress)>,
    initial_latest: Option<grv_storage::model::Latest>,
    #[serde(deserialize_with = "required_option")]
    after_publish: Option<super::after_publish::State>,
}
fn required_option<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> std::result::Result<Option<T>, D::Error> {
    Option::deserialize(d)
}
type Record = Envelope<Fixed, CaptureReceipt, Progress, Value>;
fn invalid(message: &str) -> grv_types::PublicError {
    public_error(ErrorCode::InvalidArgument, message)
}
fn save(
    journal: &Journal,
    record: &mut Record,
    next: Evidence<Fixed, CaptureReceipt, Progress, Value>,
) -> Result<()> {
    *record = journal.compare_and_swap(record.generation, next)?;
    Ok(())
}
pub(super) struct Options {
    pub(super) root: String,
    pub(super) decl: PathBuf,
    pub(super) state: PathBuf,
    pub(super) attempt: Uuid,
}
pub(super) fn options(args: &[String]) -> Result<Options> {
    let mut root = None;
    let mut decl = None;
    let mut state = None;
    let mut attempt = None;
    let mut seen = BTreeSet::new();
    for pair in args.chunks(2) {
        if pair.len() != 2 || !seen.insert(pair[0].as_str()) {
            return Err(invalid("push flags require unique names and values"));
        }
        match pair[0].as_str() {
            "--grv" => root = Some(pair[1].clone()),
            "--decl" => decl = Some(PathBuf::from(&pair[1])),
            "--state" => state = Some(PathBuf::from(&pair[1])),
            "--attempt" => {
                attempt = Some(
                    Uuid::new(&pair[1]).map_err(|_| invalid("attempt must be a canonical UUID"))?,
                )
            }
            _ => return Err(invalid("unsupported push flag")),
        }
    }
    let root = root.ok_or_else(|| invalid("push requires --grv <root>"))?;
    let decl = decl.unwrap_or_default();
    let state = state.unwrap_or_else(|| {
        if let Some(path) = std::env::var_os("XDG_STATE_HOME") {
            PathBuf::from(path).join("grv")
        } else if cfg!(target_os = "macos") {
            PathBuf::from(std::env::var_os("HOME").unwrap_or_default())
                .join("Library/Application Support/grv/state")
        } else {
            PathBuf::from(std::env::var_os("HOME").unwrap_or_default()).join(".local/state/grv")
        }
    });
    Ok(Options {
        root,
        decl,
        state,
        attempt: attempt.unwrap_or_else(Uuid::v4),
    })
}
pub(super) fn push(args: &[String]) -> std::result::Result<Success, Failure> {
    if args.chunks(2).any(|pair| pair[0] == "--session") {
        return super::build::finalize_context(args);
    }
    let options = options(args)?;
    let root = super::backend::Root::parse(&options.root)?;
    let canonical = root.canonical.clone();
    run(options, root, &canonical).map_err(|mut failure| {
        failure.root = Some(canonical);
        failure
    })
}
fn run(
    options: Options,
    root: super::backend::Root,
    canonical: &str,
) -> std::result::Result<Success, Failure> {
    let authored = load_authored(&options, &root)?;
    run_internal(options, root, canonical, authored, None)
}
pub(super) fn load_authored(options: &Options, root: &super::backend::Root) -> Result<Value> {
    // Exact private state precedes any declaration or source access. The
    // recorded authoring also routes managed-build terminal retries offline.
    let directory = options.state.join("push").join(options.attempt.as_str());
    let authored = if directory
        .try_exists()
        .map_err(|_| invalid("consumer state unavailable"))?
    {
        let journal = Journal::open(&directory, root.exclusions())?;
        let record: Envelope<Value, Value, Value, Value> = journal.read()?;
        if record.evidence.intent["root"] != root.canonical {
            return Err(mismatch());
        }
        if !options.decl.as_os_str().is_empty() && options.decl.exists() {
            declaration::load(&options.decl)
                .map_err(|_| invalid("declaration validation failed"))?
        } else {
            record
                .evidence
                .intent
                .get("authored")
                .cloned()
                .ok_or_else(mismatch)?
        }
    } else {
        if options.decl.as_os_str().is_empty() {
            return Err(invalid("new push requires --decl <yaml>"));
        }
        declaration::load(&options.decl).map_err(|_| {
            public_error(
                ErrorCode::InvalidDeclaration,
                "declaration validation failed",
            )
        })?
    };
    Ok(authored)
}
fn run_internal(
    options: Options,
    root: super::backend::Root,
    canonical: &str,
    authored: Value,
    prepare: Option<PathBuf>,
) -> std::result::Result<Success, Failure> {
    if authored["kind"] == "push"
        && declaration::mode(&authored).map_err(|_| invalid("invalid transfer mode"))?
            == Mode::ManagedBuild
    {
        return super::build::run(options, root, canonical, authored);
    }
    if authored["kind"] != "push"
        || declaration::mode(&authored).map_err(|_| invalid("invalid transfer mode"))?
            != Mode::Extract
    {
        return Err(public_error(
            ErrorCode::UnsupportedCapability,
            "build orchestration is not ready",
        )
        .into());
    }
    let authoring_sha256 =
        grv_types::sha256(&grv_types::canonical_json(&authored).map_err(|_| {
            public_error(
                ErrorCode::InvalidDeclaration,
                "declaration is not JCS interoperable",
            )
        })?);
    let directory = options.state.join("push").join(options.attempt.as_str());
    let existing = directory
        .try_exists()
        .map_err(|_| public_error(ErrorCode::BackendFailure, "consumer state unavailable"))?;
    let journal;
    let clock = SystemClock::default();
    let mut record: Record;
    let mut source = None;
    if existing {
        journal = Journal::open(&directory, root.exclusions())?;
        record = journal.read()?;
        if record.evidence.intent.authoring_sha256 != authoring_sha256
            || record.evidence.intent.root != canonical
            || record.evidence.intent.request.attempt_id != options.attempt
        {
            return Err(public_error(
                ErrorCode::RequestMismatch,
                "attempt belongs to a different fixed request",
            )
            .into());
        }
        if record.evidence.progress.aborted.is_some() && prepare.is_none() {
            return Err(public_error(
                ErrorCode::StateConflict,
                "aborted extraction cannot finalize",
            )
            .into());
        }
        if prepare.is_none() {
            if let Some(mut result) = record.evidence.terminal.clone() {
                validate_result(&record.evidence.intent, &result["adapter_result"])?;
                result["replayed"] = json!(true);
                return acknowledge(&journal, &mut record, result, canonical);
            }
            if let Some(outcome) = record
                .evidence
                .progress
                .push
                .as_ref()
                .and_then(|push| push.terminal.clone())
            {
                return finish_result(&journal, &mut record, outcome, canonical, true);
            }
        }
        if prepare.is_none()
            && record.evidence.progress.push.is_none()
            && record.evidence.capture.is_none()
            && record.evidence.progress.acquisition.started
            && !record.evidence.progress.acquisition.stopped
        {
            return Err(public_error(
                ErrorCode::ExtractionIncomplete,
                "incomplete nonresumable acquisition cannot be restarted",
            )
            .into());
        }
    } else {
        let name = Name::new(
            authored["adapter"]
                .as_str()
                .ok_or_else(|| invalid("adapter required"))?,
        )
        .map_err(|_| invalid("invalid adapter name"))?;
        let installations = discovery::discover(&discovery::search_roots(None)?)?;
        let installation = installations
            .iter()
            .find(|i| i.manifest.name == name)
            .ok_or_else(|| public_error(ErrorCode::NotFound, "adapter installation not found"))?;
        let declaration_path = std::fs::canonicalize(&options.decl)
            .map_err(|_| invalid("declaration path unavailable"))?;
        let mut session = Session::spawn_at(
            installation,
            Deadlines::default(),
            declaration_path.parent(),
        )?;
        if !session.descriptor.capabilities.push {
            return Err(public_error(
                ErrorCode::UnsupportedCapability,
                "adapter does not advertise a complete extraction lifecycle",
            )
            .into());
        }
        let mut original = authored.clone();
        declaration::apply_point_defaults(&mut original, &session.registry).map_err(|_| {
            public_error(
                ErrorCode::InvalidDeclaration,
                "adapter defaults are invalid",
            )
        })?;
        declaration::validate_points(&original, &session.registry).map_err(|_| {
            public_error(
                ErrorCode::InvalidDeclaration,
                "adapter configuration validation failed",
            )
        })?;
        let effective = session.validate_binding(original.clone(), Mode::Extract)?;
        declaration::validate_effective(&original, &effective, &session.registry).map_err(
            |_| {
                public_error(
                    ErrorCode::ProtocolFailure,
                    "adapter changed explicit declaration values",
                )
            },
        )?;
        let locator =
            session.locate_connection(effective["connection"].clone(), Mode::Extract, None)?;
        let coordinates = locator.canonical_connection.clone();
        let expected = locator.identity.clone();
        let bound = session.bind_connection(
            locator,
            Some(canonical.into()),
            expected.clone(),
            None,
            Mode::Extract,
        )?;
        let identity = session.authenticate(bound.handle.clone(), expected.or(bound.identity))?;
        let adapter_identity = AdapterIdentity {
            name: session.descriptor.name.clone(),
            package_version: session.descriptor.package_version.clone(),
            interface_version: session.descriptor.interface_version,
            binding_schema_version: session.descriptor.binding_schema_version,
        };
        let digest = grv_types::declaration_digest(&DeclarationIdentity {
            effective_declaration: effective.clone(),
            adapter_identity: adapter_identity.clone(),
            connection_identity: identity.clone(),
            canonical_connection: coordinates.clone(),
        })
        .map_err(|_| {
            public_error(
                ErrorCode::InvalidDeclaration,
                "fixed declaration is not JCS interoperable",
            )
        })?;
        let plans: Vec<_> = effective["tables"]
            .as_array()
            .unwrap()
            .iter()
            .map(|table| TablePlan::from_declaration(&effective, table))
            .collect::<Result<_>>()?;
        let drops: Vec<_> = effective
            .pointer("/selection/drop")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|drop| Name::new(drop["table"].as_str().unwrap()).unwrap())
            .collect();
        let store = Store::open(root.open(false)?)?;
        let base = revision::read_latest(
            &store,
            &Name::new(effective["dataset"].as_str().unwrap()).unwrap(),
        )?
        .map_or(Counter::from(0), |x| x.0.revision);
        let dataset = Name::new(effective["dataset"].as_str().unwrap()).unwrap();
        let validation_scratch = tempfile::tempdir().map_err(|_| {
            public_error(ErrorCode::BackendFailure, "validation scratch unavailable")
        })?;
        grv_core::push::validate_snapshot_membership(
            &store,
            &dataset,
            base,
            &plans.iter().map(|p| p.table.clone()).collect::<Vec<_>>(),
            &drops,
            validation_scratch.path(),
        )?;
        let ownership = Ownership::new(&store, &clock, lease_ttl(&store))?;
        let run_id = new_run_id(&clock.now())?;
        let request = ExtractRequest {
            attempt_id: options.attempt.clone(),
            stream_id: Uuid::v4(),
            root: canonical.into(),
            dataset: dataset.clone(),
            run_id: run_id.clone(),
            declaration_sha256: digest,
            adapter_identity: adapter_identity.clone(),
            connection_identity: identity,
            selection: ExtractSelection {
                policy: if effective
                    .pointer("/selection/policy")
                    .and_then(Value::as_str)
                    == Some("all")
                {
                    SelectionPolicy::All
                } else {
                    SelectionPolicy::Changed
                },
            },
            options: effective
                .get("options")
                .cloned()
                .unwrap_or_else(|| json!({})),
            tables: effective["tables"]
                .as_array()
                .unwrap()
                .iter()
                .zip(&plans)
                .map(|(table, plan)| ExtractTable {
                    name: plan.table.clone(),
                    source: table["source"].clone(),
                    columns: Value::Array(
                        table["columns"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .filter(|c| c.get("derive").is_none())
                            .cloned()
                            .collect(),
                    ),
                    contract: plan.input_contract().unwrap(),
                })
                .collect(),
            resume: None,
        };
        let metadata = std::collections::BTreeMap::from([(
            "grv_cli".into(),
            json!({"canonical_writer":CANONICAL_WRITER,"adapter":adapter_identity,"attempt_id":options.attempt}),
        )]);
        let mut run = ownership.prepare_run(dataset, run_id, base, vec![], Some(metadata))?;
        // Validation has no external writes. Create retry state only once the
        // complete fixed intent is ready to persist before run acquisition.
        eprintln!("attempt {}", options.attempt);
        journal = Journal::create(&directory, root.exclusions())?;
        record = journal.create_evidence(Evidence {
            intent: Fixed {
                authoring_sha256,
                authored: authored.clone(),
                declaration_path: declaration_path.clone(),
                registry: session.registry.registry.clone(),
                root: canonical.into(),
                effective_declaration: effective,
                descriptor: session.descriptor.clone(),
                after_publish: session.capabilities.after_publish,
                canonical_connection: coordinates,
                request: request.clone(),
                run: run.clone(),
                plans,
                drops,
            },
            capture: None,
            progress: Progress {
                acquisition: AcquisitionProgress::prepared(&request),
                push: None,
                owner: None,
                aborted: None,
                abort_claims: vec![],
                initial_latest: Some(grv_storage::model::Latest::empty()),
                after_publish: None,
            },
            terminal: None,
        })?;
        Publisher::new(&store, &clock, lease_ttl(&store))?.initialize_dataset(
            &request.dataset,
            record.evidence.progress.initial_latest.as_ref().unwrap(),
        )?;
        ownership.commit_run(&run)?;
        ownership.confirm_holds(&mut run)?;
        let mut next = record.evidence.clone();
        next.progress.owner = Some(run);
        save(&journal, &mut record, next)?;
        source = Some((session, bound.handle));
    }
    // Full accepted captures and pending publications do not open a source.
    let store = Store::open(root.open(false)?)?;
    let ttl = lease_ttl(&store);
    let ownership = Ownership::new(&store, &clock, ttl)?;
    if !record.evidence.progress.acquisition.started
        && record.evidence.progress.push.is_none()
        && record.evidence.terminal.is_none()
        && record.evidence.progress.aborted.is_none()
        && let Some(initial) = &record.evidence.progress.initial_latest
    {
        Publisher::new(&store, &clock, ttl)?
            .initialize_dataset(&record.evidence.intent.request.dataset, initial)?;
    }
    if let Some(path) = prepare {
        if !record.evidence.progress.acquisition.started
            && record.evidence.progress.aborted.is_none()
            && record.evidence.terminal.is_none()
        {
            let owner = ownership.resume_prepared_run(&record.evidence.intent.run)?;
            let mut next = record.evidence.clone();
            next.progress.owner = Some(owner);
            save(&journal, &mut record, next)?;
        }
        if let Some((mut session, _)) = source {
            session.close()?;
        }
        return expose_context(&journal, &record, &path, &root);
    }
    if existing
        && record.evidence.progress.push.is_none()
        && record.evidence.capture.is_none()
        && !record.evidence.progress.acquisition.started
    {
        let fixed = &record.evidence.intent;
        record
            .evidence
            .progress
            .acquisition
            .require_unstarted(&fixed.request)?;
        let owner = ownership.resume_prepared_run(&fixed.run)?;
        let (mut session, locator) = open_fixed(fixed, &fixed.declaration_path)?;
        let expected = Some(fixed.request.connection_identity.clone());
        let bound = session.bind_connection(
            locator,
            Some(canonical.into()),
            expected.clone(),
            None,
            Mode::Extract,
        )?;
        let identity = session.authenticate(bound.handle.clone(), expected)?;
        if identity != fixed.request.connection_identity {
            return Err(public_error(
                ErrorCode::RequestMismatch,
                "prepared attempt authenticated a different source",
            )
            .into());
        }
        source = Some((session, bound.handle));
        let mut next = record.evidence.clone();
        next.progress.owner = Some(owner);
        save(&journal, &mut record, next)?;
    }

    if record.evidence.progress.push.is_none() {
        let fixed = record.evidence.intent.clone();

        let interval = Duration::from_millis(
            ((ttl - store.parameters.max_clock_skew_seconds.get()) * 1000 / 3).max(1),
        );
        let job = CaptureJob {
            ownership: &ownership,
            run: &fixed.run,
            journal: &journal,
            request: &fixed.request,
            plans: &fixed.plans,
            renewal_interval: interval,
        };
        if let Some((mut session, handle)) = source {
            let mut acquisition = record.evidence.progress.acquisition.clone();
            job.acquire(&mut session, handle, &mut acquisition, |progress| {
                let mut next = record.evidence.clone();
                next.progress.acquisition = progress.clone();
                save(&journal, &mut record, next)
            })?;
        }
        if record.evidence.capture.is_none() {
            let receipt = job.canonicalize(
                &record.evidence.progress.acquisition,
                fixed.descriptor.capabilities.source_consistency,
            )?;
            let mut next = record.evidence.clone();
            next.capture = Some(receipt);
            save(&journal, &mut record, next)?;
        }
        let receipt = record.evidence.capture.as_ref().unwrap();
        validate_result(&fixed, &receipt.completion.adapter_result)?;
        let policy = if fixed.request.selection.policy == SelectionPolicy::All {
            PushPolicy::All
        } else {
            PushPolicy::Changed
        };
        let plan = ExportPlan::prepare(
            &store,
            &clock,
            ttl,
            &fixed.run,
            receipt,
            &fixed.request,
            &journal,
            policy,
            &fixed.drops,
            journal.directory(),
        )?;
        let progress = PushProgress::planned(fixed.run, plan)?;
        let mut next = record.evidence.clone();
        next.progress.push = Some(progress);
        save(&journal, &mut record, next)?;
    }
    let mut progress = record.evidence.progress.push.clone().unwrap();
    let finalizer = PushFinalizer::new(&store, &clock, ttl)?;
    let outcome = finalizer.finalize_checked(
        &mut progress,
        &journal,
        journal.directory(),
        |progress| {
            let mut next = record.evidence.clone();
            next.progress.push = Some(progress.clone());
            save(&journal, &mut record, next)
        },
        || Ok(()),
    )?;
    finish_result(&journal, &mut record, outcome, canonical, existing)
}
fn finish_result(
    journal: &Journal,
    record: &mut Record,
    outcome: PushOutcome,
    canonical: &str,
    replayed: bool,
) -> std::result::Result<Success, Failure> {
    let fixed = &record.evidence.intent;
    let progress = record.evidence.progress.push.as_ref().ok_or_else(|| {
        public_error(
            ErrorCode::IntegrityFailure,
            "fixed publication progress missing",
        )
    })?;
    let operation = progress
        .publication
        .as_ref()
        .filter(|_| !outcome.no_op)
        .map(|i| i.operation.operation_id.clone());
    let receipt = record.evidence.capture.as_ref().ok_or_else(|| {
        public_error(
            ErrorCode::IntegrityFailure,
            "accepted capture receipt missing",
        )
    })?;
    validate_result(fixed, &receipt.completion.adapter_result)?;
    let result = json!({"adapter":fixed.request.adapter_identity.name,"mode":"extract","attempt_id":fixed.request.attempt_id,"dataset":fixed.request.dataset,"run_id":fixed.request.run_id,
        "completion_sha256":receipt.digest()?,"outcome":{"kind":if outcome.no_op{"no-op"}else{"published"},"revision":outcome.revision.to_string(),"operation_id":operation},
        "replayed":replayed,"adapter_result":receipt.completion.adapter_result});
    let mut next = record.evidence.clone();
    let mut terminal = result.clone();
    terminal["replayed"] = json!(false);
    next.terminal = Some(terminal);
    if fixed.after_publish {
        next.progress.after_publish = Some(super::after_publish::State::Pending);
    }
    save(journal, record, next).map_err(|error| Failure {
        error,
        result: Some(result.clone()),
        root: Some(canonical.into()),
    })?;
    let success = acknowledge(journal, record, result, canonical)?;
    if let Some(error) = outcome.maintenance_error {
        return Err(Failure {
            error,
            result: Some(success.result),
            root: Some(canonical.into()),
        });
    }
    Ok(success)
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
            super::after_publish::call(
                super::after_publish::Fixed {
                    descriptor: &fixed.descriptor,
                    registry: &fixed.registry,
                    connection: &fixed.effective_declaration["connection"],
                    canonical_connection: &fixed.canonical_connection,
                    identity: &fixed.request.connection_identity,
                    root: &fixed.root,
                    workspace: None,
                    run: None,
                    mode: Mode::Extract,
                    attempt: &fixed.request.attempt_id,
                    declaration_sha256: &fixed.request.declaration_sha256,
                    declaration_path: &fixed.declaration_path,
                },
                &result,
            )?;
            let mut next = record.evidence.clone();
            next.progress.after_publish = Some(super::after_publish::State::Complete);
            save(journal, record, next)?;
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
fn lease_ttl<B: Backend>(store: &Store<B>) -> u64 {
    TTL.min(store.parameters.max_lease_ttl_seconds.get())
}
fn open_fixed(
    fixed: &Fixed,
    declaration: &Path,
) -> Result<(Session, grv_adapter_api::ConnectionLocator)> {
    let check = || -> grv_adapter_host::Result<(Session, grv_adapter_api::ConnectionLocator)> {
        let installed = discovery::discover(&discovery::search_roots(None)?)?;
        let installation = installed
            .iter()
            .find(|i| i.manifest.name == fixed.descriptor.name)
            .ok_or_else(|| {
                grv_adapter_host::Error::new(
                    ErrorCode::NotFound,
                    "fixed adapter installation unavailable",
                )
            })?;
        if installation.manifest.version != fixed.descriptor.package_version
            || installation.manifest.binding_schema_version
                != fixed.descriptor.binding_schema_version
            || !installation
                .manifest
                .interface_versions
                .contains(&fixed.descriptor.interface_version)
        {
            return Err(grv_adapter_host::Error::new(
                ErrorCode::RequestMismatch,
                "unfinished attempt adapter identity changed",
            ));
        }
        // This path fixes the source working directory; the self-contained
        // context does not depend on the original declaration file surviving.
        let declaration = declaration.to_path_buf();
        let mut session =
            Session::spawn_at(installation, Deadlines::default(), declaration.parent())?;
        if session.descriptor != fixed.descriptor || session.registry.registry != fixed.registry {
            return Err(grv_adapter_host::Error::new(
                ErrorCode::RequestMismatch,
                "unfinished attempt adapter descriptor changed",
            ));
        }
        let locator = session.locate_connection(
            fixed.effective_declaration["connection"].clone(),
            Mode::Extract,
            None,
        )?;
        if locator.canonical_connection != fixed.canonical_connection
            || locator
                .identity
                .as_ref()
                .is_some_and(|identity| identity != &fixed.request.connection_identity)
        {
            return Err(grv_adapter_host::Error::new(
                ErrorCode::RequestMismatch,
                "unfinished attempt connection coordinates changed",
            ));
        }
        Ok((session, locator))
    };
    check().map_err(|e| e.public())
}
fn mismatch() -> grv_types::PublicError {
    public_error(
        ErrorCode::RequestMismatch,
        "extraction context differs from fixed attempt",
    )
}
fn validate_result(fixed: &Fixed, value: &Value) -> Result<()> {
    CheckedRegistry::recorded(fixed.registry.clone())
        .map_err(|e| e.public())?
        .validate_point(
            Mode::Extract,
            "push_result",
            value,
            ErrorCode::ProtocolFailure,
        )
        .map_err(|e| e.public())
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Context {
    mode: String,
    context_version: u32,
    directory: PathBuf,
    fixed: Fixed,
}
fn context_from(journal: &Journal, record: &Record) -> Context {
    Context {
        mode: "extract".into(),
        context_version: 1,
        directory: journal.directory().into(),
        fixed: record.evidence.intent.clone(),
    }
}
fn read_context(path: &Path) -> Result<(PathBuf, Context, super::backend::Root)> {
    use grv_adapter_host::protected_document;
    let path = protected_document::canonical_path(path, &[]).map_err(|e| e.public())?;
    let context: Context =
        protected_document::read(&path, 64 * 1024 * 1024).map_err(|e| e.public())?;
    let root = super::backend::Root::parse(&context.fixed.root)?;
    let excluded = root
        .exclusions()
        .iter()
        .map(PathBuf::as_path)
        .collect::<Vec<_>>();
    protected_document::canonical_path(&path, &excluded).map_err(|e| e.public())?;
    if context.mode != "extract"
        || context.context_version != 1
        || root.canonical != context.fixed.root
        || context.directory.file_name().and_then(|s| s.to_str())
            != Some(context.fixed.request.attempt_id.as_str())
        || context
            .directory
            .parent()
            .and_then(Path::file_name)
            .and_then(|s| s.to_str())
            != Some("push")
        || context.fixed.run.dataset() != &context.fixed.request.dataset
        || context.fixed.run.control().run_id != context.fixed.request.run_id
        || !context.fixed.run.control().inputs.is_empty()
        || grv_types::sha256(
            &grv_types::canonical_json(&context.fixed.authored).map_err(|_| mismatch())?,
        ) != context.fixed.authoring_sha256
    {
        return Err(mismatch());
    }
    CheckedRegistry::recorded(context.fixed.registry.clone()).map_err(|e| e.public())?;
    Ok((path, context, root))
}
fn open_context(context: &Context, root: &super::backend::Root) -> Result<(Journal, Record)> {
    let journal = Journal::open(&context.directory, root.exclusions())?;
    let record: Record = journal.read()?;
    if grv_types::canonical_json(&context_from(&journal, &record)).map_err(|_| mismatch())?
        != grv_types::canonical_json(context).map_err(|_| mismatch())?
    {
        return Err(mismatch());
    }
    Ok((journal, record))
}
fn observed_run(
    root: &super::backend::Root,
    fixed: &Fixed,
) -> Result<grv_storage::model::RunControl> {
    let backend = root.open(false)?;
    let key = grv_storage::ObjectKey::new(format!(
        "datasets/{}/.runs/{}.control.json",
        fixed.request.dataset, fixed.request.run_id
    ))
    .unwrap();
    let (bytes, _) = backend
        .read_bytes(&key, 64 * 1024 * 1024)
        .map_err(grv_core::store::backend_error)?;
    let observed: grv_storage::model::RunControl =
        grv_storage::model::decode_record(&bytes).map_err(grv_core::store::backend_error)?;
    let original = fixed.run.control();
    if observed.run_id != original.run_id
        || observed.created_at != original.created_at
        || observed.base_revision != original.base_revision
        || observed.inputs != original.inputs
        || observed.metadata != original.metadata
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
    let fixed = &context.fixed;
    let completion = record
        .evidence
        .capture
        .as_ref()
        .map(CaptureReceipt::digest)
        .transpose()?;
    let outcome = record
        .evidence
        .terminal
        .as_ref()
        .or(record.evidence.progress.aborted.as_ref())
        .and_then(|v| v.get("outcome"))
        .cloned();
    let export = record.evidence.progress.push.as_ref().map(|push| {
        let mut contributions = push.plan.groups.iter().map(|g| {
            let (action, version, run) = match g.action {
                grv_core::push::ExportAction::Write => ("write", None, Some(fixed.request.run_id.clone())),
                grv_core::push::ExportAction::Reuse{version} => ("reuse", Some(version.to_string()), None),
            };
            json!({"table":g.plan.table,"partition":g.capture.partition,"action":action,"version":version,"run_id":run})
        }).collect::<Vec<_>>();
        if let Some(receipt) = &record.evidence.capture {
            contributions.extend(receipt.tables.iter().filter(|t|t.groups.is_empty()).map(|t|json!({"table":t.plan.table,"partition":{},"action":"empty","version":null,"run_id":null})));
        }
        json!({"base_revision":push.plan.base_revision.to_string(),"completion_sha256":completion,"contributions":contributions,"omissions":push.plan.omissions,"held_tables":[]})
    });
    let publication = record.evidence.progress.push.as_ref().and_then(|p|p.publication.as_ref()).and_then(|p| {
        if let grv_storage::model::OperationPayload::Publish(payload)=&p.operation.body {
            Some(json!({"operation_id":p.operation.operation_id,"reserved_revision":payload.revision.to_string(),"state":if outcome.is_some(){"committed"}else{"unknown"}}))
        } else {None}
    });
    let capture = record.evidence.capture.as_ref().map(|receipt| -> Result<Value> {
        let tables=receipt.tables.iter().map(|t| {
            let digest=grv_types::sha256(&grv_types::canonical_json(t).map_err(|_| mismatch())?);
            let jobs=t.completion.source_identity.get("job_ids").and_then(Value::as_array).cloned().unwrap_or_default();
            Ok(json!({"table":t.plan.table,"row_count":t.row_count,"started_at":t.completion.capture.start,"completed_at":t.completion.capture.end,"capture_sha256":digest,"job_ids":jobs}))
        }).collect::<Result<Vec<_>>>()?;
        Ok(json!({"source_identity":receipt.connection_identity,"source_consistency":receipt.source_consistency,"tables":tables}))
    }).transpose()?;
    let expired = observed.expires_at.as_ref().is_some_and(|t| {
        grv_core::clock::parse(t) <= grv_core::clock::parse(&SystemClock::default().now())
    });
    Ok(
        json!({"context_path":path,"engine_path":null,"state_path":context.directory,"session":{
            "dataset":fixed.request.dataset,"run_id":fixed.request.run_id,"workspace_id":null,"base_revision":fixed.run.control().base_revision.to_string(),
            "declaration_sha256":fixed.request.declaration_sha256,"input_revisions":[],"mappings":null,"completion_sha256":completion,"export_plan":export,"publication_attempt":publication,"outcome":outcome,
            "run":{"run_id":observed.run_id,"phase":observed.phase,"base_revision":observed.base_revision.to_string(),"expires_at":observed.expires_at,"expired":expired,"holds_confirmed":observed.holds_confirmed},
            "adapter":fixed.request.adapter_identity.name,"mode":"extract","attempt_id":fixed.request.attempt_id,"capture":capture,"adapter_context":{}
        }}),
    )
}
fn expose_context(
    journal: &Journal,
    record: &Record,
    path: &Path,
    root: &super::backend::Root,
) -> std::result::Result<Success, Failure> {
    let context = context_from(journal, record);
    let excluded = root
        .exclusions()
        .iter()
        .map(PathBuf::as_path)
        .collect::<Vec<_>>();
    let path = grv_adapter_host::protected_document::canonical_path(path, &excluded)?;
    grv_adapter_host::protected_document::publish(&path, &context, 64 * 1024 * 1024)?;
    let observed = observed_run(root, &context.fixed)?;
    Ok(Success {
        result: context_result(&path, &context, record, &observed)?,
        root: Some(root.canonical.clone()),
    })
}
pub(super) fn session(args: &[String]) -> std::result::Result<Success, Failure> {
    let command = args
        .first()
        .ok_or_else(|| invalid("session command required"))?;
    if command == "prepare" {
        let mut transfer = vec![];
        let mut path = None;
        for pair in args[1..].chunks(2) {
            if pair.len() != 2 {
                return Err(invalid("session flags require values").into());
            }
            if pair[0] == "--session" {
                if path.replace(PathBuf::from(&pair[1])).is_some() {
                    return Err(invalid("duplicate session path").into());
                }
            } else {
                transfer.extend_from_slice(pair);
            }
        }
        let options = options(&transfer)?;
        let path = path.ok_or_else(|| invalid("session preparation requires --session"))?;
        let root = super::backend::Root::parse(&options.root)?;
        let canonical = root.canonical.clone();
        let authored = load_authored(&options, &root)?;
        return run_internal(options, root, &canonical, authored, Some(path));
    }
    if args.len() != 3 || args[1] != "--session" {
        return Err(invalid("session command requires --session <context.json>").into());
    }
    let (path, context, root) = read_context(Path::new(&args[2]))?;
    let (journal, mut record) = open_context(&context, &root)?;
    match command.as_str() {
        "show" => {
            let observed = observed_run(&root, &context.fixed)?;
            Ok(Success {
                result: context_result(&path, &context, &record, &observed)?,
                root: Some(root.canonical),
            })
        }
        "renew" => {
            if record.evidence.terminal.is_some() || record.evidence.progress.aborted.is_some() {
                return Err(public_error(
                    ErrorCode::StateConflict,
                    "terminal extraction cannot renew",
                )
                .into());
            }
            let store = Store::open(root.open(false)?)?;
            let clock = SystemClock::default();
            let ownership = Ownership::new(&store, &clock, lease_ttl(&store))?;
            let mut owner = record
                .evidence
                .progress
                .push
                .as_ref()
                .map(|p| p.owner.clone())
                .or_else(|| record.evidence.progress.owner.clone())
                .unwrap_or_else(|| context.fixed.run.clone());
            ownership.require_open_transfer(&owner)?;
            ownership.renew_run(&mut owner)?;
            let mut next = record.evidence.clone();
            next.progress.owner = Some(owner.clone());
            if let Some(push) = &mut next.progress.push {
                push.owner = owner.clone();
            }
            save(&journal, &mut record, next)?;
            Ok(Success {
                result: json!({"dataset":owner.dataset(),"run_id":owner.control().run_id,"phase":"open","expires_at":owner.control().expires_at,"renewed":true}),
                root: Some(root.canonical),
            })
        }
        "abort" => abort_context(&context, &root, &journal, &mut record),
        _ => Err(invalid("unsupported session command").into()),
    }
}
pub(super) fn finalize_context(args: &[String]) -> std::result::Result<Success, Failure> {
    if args.len() != 2 || args[0] != "--session" {
        return Err(invalid("extraction finalization requires only --session").into());
    }
    let (_, context, root) = read_context(Path::new(&args[1]))?;
    let (journal, _) = open_context(&context, &root)?;
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
        attempt: context.fixed.request.attempt_id.clone(),
    };
    let canonical = root.canonical.clone();
    run_internal(options, root, &canonical, context.fixed.authored, None)
}
fn abort_projection(record: &Record, outcome: &Value) -> Value {
    let fixed = &record.evidence.intent;
    let entries=record.evidence.progress.push.as_ref().and_then(|p|p.sealed.as_ref()).map(|s|s.entries.iter().map(|e|json!({"table":e.table,"partition":e.partition,"version":e.version.to_string(),"run_id":s.run_id})).collect::<Vec<_>>()).unwrap_or_default();
    json!({"dataset":fixed.request.dataset,"run_id":fixed.request.run_id,"outcome":outcome,"finalized_entries":entries})
}
fn abort_context(
    context: &Context,
    root: &super::backend::Root,
    journal: &Journal,
    record: &mut Record,
) -> std::result::Result<Success, Failure> {
    if let Some(result) = &record.evidence.progress.aborted {
        return Ok(Success {
            result: result.clone(),
            root: Some(root.canonical.clone()),
        });
    }
    if let Some(result) = &record.evidence.terminal {
        return Ok(Success {
            result: abort_projection(record, &result["outcome"]),
            root: Some(root.canonical.clone()),
        });
    }
    let acquisition = &record.evidence.progress.acquisition;
    if acquisition.started && !acquisition.stopped {
        return Err(public_error(
            ErrorCode::OutcomeUnknown,
            "source stopped-writer evidence is required before abort",
        )
        .into());
    }
    let store = Store::open(root.open(false)?)?;
    let clock = SystemClock::default();
    let ttl = lease_ttl(&store);
    if record
        .evidence
        .progress
        .push
        .as_ref()
        .is_some_and(|p| p.publication.is_some() || p.terminal.is_some())
    {
        let mut push = record.evidence.progress.push.clone().unwrap();
        let stopped = std::cell::Cell::new(false);
        let resolved = PushFinalizer::new(&store, &clock, ttl)?.finalize_checked(
            &mut push,
            journal,
            journal.directory(),
            |p| {
                let mut next = record.evidence.clone();
                next.progress.push = Some(p.clone());
                save(journal, record, next)
            },
            || {
                stopped.set(true);
                Err(public_error(
                    ErrorCode::StateConflict,
                    "abort stops unpublished output",
                ))
            },
        );
        match resolved {
            Ok(outcome) => {
                let success = finish_result(journal, record, outcome, &root.canonical, true)?;
                return Ok(Success {
                    result: abort_projection(record, &success.result["outcome"]),
                    root: success.root,
                });
            }
            Err(_) if stopped.get() => {}
            Err(error) => return Err(error.into()),
        }
    }
    let ownership = Ownership::new(&store, &clock, ttl)?;
    let mut owner = record
        .evidence
        .progress
        .push
        .as_ref()
        .map(|p| p.owner.clone())
        .or_else(|| record.evidence.progress.owner.clone())
        .unwrap_or_else(|| context.fixed.run.clone());
    ownership.require_stopped_epoch(&owner)?;
    // Persist a closed list of exact existing reservation authorities. Cleanup
    // cannot reserve a pending group or manufacture a missing allocation.
    if record.evidence.progress.abort_claims.is_empty() {
        let mut claims = vec![];
        if let Some(push) = &record.evidence.progress.push {
            for (group, phase) in push.plan.groups.iter().zip(&push.groups) {
                let claim = match phase {
                    GroupProgress::Reserving { intent, progress } => {
                        Some(AbortClaim::from_reserving(&owner, intent, progress)?)
                    }
                    GroupProgress::Reserved { reservation }
                    | GroupProgress::Written { reservation, .. } => Some(
                        AbortClaim::from_reservation(&owner, reservation, &group.plan.contract)?,
                    ),
                    GroupProgress::Pending | GroupProgress::Finalized { .. } => None,
                };
                if let Some(claim) = claim {
                    claims.push((claim, AbortClaimProgress::Prepared));
                }
            }
        }
        let mut next = record.evidence.clone();
        next.progress.abort_claims = claims;
        save(journal, record, next)?;
    }
    owner = cleanup_claims(
        &ownership,
        owner,
        journal,
        record,
        Duration::from_millis(
            ((ttl - store.parameters.max_clock_skew_seconds.get()) * 1000 / 3).max(1),
        ),
    )?;
    let mut next = record.evidence.clone();
    next.progress.owner = Some(owner.clone());
    save(journal, record, next)?;
    let sealed = ownership.seal_renewed(&mut owner)?;
    let entries=sealed.entries.iter().map(|e|json!({"table":e.table,"partition":e.partition,"version":e.version.to_string(),"run_id":sealed.run_id})).collect::<Vec<_>>();
    let result = json!({"dataset":owner.dataset(),"run_id":owner.control().run_id,"outcome":{"kind":"aborted","revision":null,"operation_id":null},"finalized_entries":entries});
    let mut next = record.evidence.clone();
    next.progress.owner = Some(owner);
    next.progress.aborted = Some(result.clone());
    save(journal, record, next)?;
    Ok(Success {
        result,
        root: Some(root.canonical.clone()),
    })
}

/// Read-only stopped evidence for a trusted attester retaining this exact
/// Journal's lock. A timeout, directory listing or incomplete source cannot
/// produce authority. This deliberately verifies the accepted file hashes.
pub(super) fn stopped_capture_proof(
    journal: &Journal,
    dataset: &Name,
    control: &grv_storage::model::RunControl,
    canonical: &str,
) -> Result<Option<Digest>> {
    let record: Record = journal.read()?;
    let fixed = &record.evidence.intent;
    let original = fixed.run.control();
    if fixed.root != canonical
        || fixed.request.dataset != *dataset
        || original.run_id != control.run_id
        || original.owner_token != control.owner_token
        || original.created_at != control.created_at
        || original.base_revision != control.base_revision
        || original.inputs != control.inputs
        || original.metadata != control.metadata
    {
        return Err(mismatch());
    }
    if !record.evidence.progress.acquisition.stopped {
        return Ok(None);
    }
    let Some(receipt) = &record.evidence.capture else {
        return Ok(None);
    };
    receipt.verify(journal, &fixed.request)?;
    Ok(Some(grv_types::sha256(
        &grv_types::canonical_json(&(fixed.request.clone(), receipt)).map_err(|_| mismatch())?,
    )))
}

/// One scoped renewer covers all stopped-claim IO. It joins before the final
/// owner update/seal, and every conditional cleanup transition remains durable.
fn cleanup_claims<B: Backend>(
    ownership: &Ownership<'_, B>,
    owner: RunOwner,
    journal: &Journal,
    record: &mut Record,
    interval: Duration,
) -> Result<RunOwner> {
    use std::sync::{Mutex, mpsc};
    if record
        .evidence
        .progress
        .abort_claims
        .iter()
        .all(|(_, p)| matches!(p, AbortClaimProgress::Done { .. }))
    {
        return Ok(owner);
    }
    let error = Mutex::new(None::<grv_types::PublicError>);
    let check = || match error.lock().unwrap().as_ref() {
        Some(e) => Err(e.clone()),
        None => Ok(()),
    };
    let (tx, rx) = mpsc::channel::<()>();
    struct Stop(mpsc::Sender<()>);
    impl Drop for Stop {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }
    std::thread::scope(|scope| {
        let check_error = &error;
        let mut renewed = owner.clone();
        let worker = scope.spawn(move || -> Result<RunOwner> {
            while matches!(
                rx.recv_timeout(interval),
                Err(mpsc::RecvTimeoutError::Timeout)
            ) {
                if let Err(e) = ownership.renew_run(&mut renewed) {
                    *check_error.lock().unwrap() = Some(e.clone());
                    return Err(e);
                }
            }
            Ok(renewed)
        });
        let stop = Stop(tx);
        let result = (|| -> Result<()> {
            for index in 0..record.evidence.progress.abort_claims.len() {
                let (claim, mut progress) = record.evidence.progress.abort_claims[index].clone();
                ownership.abort_claim_authorized(
                    &owner,
                    &claim,
                    &mut progress,
                    |p| {
                        check()?;
                        let mut next = record.evidence.clone();
                        next.progress.abort_claims[index].1 = p.clone();
                        save(journal, record, next)
                    },
                    check,
                )?;
            }
            check()
        })();
        drop(stop);
        let renewed = worker.join().map_err(|_| {
            public_error(ErrorCode::BackendFailure, "abort renewal worker failed")
        })??;
        result?;
        Ok(renewed)
    })
}
