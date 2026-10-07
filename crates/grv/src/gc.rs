//! Read-only GC observations and journaled, lease-fenced application.
use super::{Failure, Success, backend::Root};
use grv_core::{
    admin::{AdminProgress, PruneProgress},
    clock::SystemClock,
    gc::{CandidateState, Gc, GcOutcome, GcPreview, GcProgress},
    journal::{Envelope, Evidence, Journal},
    publication::{LeaseIntent, LeaseProgress, Publisher},
    store::{Result, Store, public_error},
};
use grv_storage::model::HoldRecord;
use grv_types::{ErrorCode, Name, RunId, Uuid};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, path::PathBuf};

struct Options {
    dataset: Name,
    root: String,
    apply: bool,
    state: PathBuf,
}
fn invalid(message: &str) -> grv_types::PublicError {
    public_error(ErrorCode::InvalidArgument, message)
}
fn options(args: &[String]) -> Result<Options> {
    let dataset = Name::new(
        args.first()
            .ok_or_else(|| invalid("gc requires a dataset"))?,
    )
    .map_err(|_| invalid("invalid dataset"))?;
    let (mut root, mut mode, mut state) = (None, None, None);
    let mut seen = BTreeSet::new();
    let mut index = 1;
    while index < args.len() {
        let flag = args[index].as_str();
        if !seen.insert(flag) {
            return Err(invalid("duplicate gc flag"));
        }
        match flag {
            "--dry-run" | "--apply" => {
                if mode.replace(flag == "--apply").is_some() {
                    return Err(invalid("gc modes are mutually exclusive"));
                }
                index += 1;
            }
            "--grv" | "--state" => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| invalid("gc flag value required"))?;
                if flag == "--grv" {
                    root = Some(value.clone())
                } else {
                    state = Some(PathBuf::from(value))
                }
                index += 2;
            }
            _ => return Err(invalid("unknown gc flag")),
        }
    }
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
        dataset,
        root: root.ok_or_else(|| invalid("gc requires --grv <root>"))?,
        apply: mode.unwrap_or(false),
        state,
    })
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixed {
    root: String,
    lease: LeaseIntent,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Progress {
    lease: LeaseProgress,
    gc: GcProgress,
    completed: BTreeSet<RunId>,
}
type Record = Envelope<Fixed, Value, Progress, GcOutcome>;
fn completed(progress: &GcProgress) -> Vec<RunId> {
    let admin = match progress {
        GcProgress::Release { progress, .. } => Some(progress.as_ref()),
        GcProgress::Prune { progress, .. } => match progress.as_ref() {
            PruneProgress::Intent { progress, .. } | PruneProgress::Decision { progress, .. } => {
                Some(progress.as_ref())
            }
            _ => None,
        },
        _ => None,
    };
    match admin {
        Some(AdminProgress::Complete { outcome }) if outcome.committed => {
            vec![outcome.operation_id.clone()]
        }
        _ => vec![],
    }
}
fn save(journal: &Journal, record: &mut Record, progress: Progress) -> Result<()> {
    let mut next = record.evidence.clone();
    next.progress = progress;
    *record = journal.compare_and_swap(record.generation, next)?;
    Ok(())
}
fn hold_identity(hold: &HoldRecord) -> Value {
    json!({"dataset":hold.dataset,"revision":hold.revision.get().to_string(),"path":grv_core::holds::hold_key(hold)})
}
fn report(
    preview: &GcPreview,
    apply: bool,
    outcome: Option<&GcOutcome>,
    ids: &BTreeSet<RunId>,
) -> Value {
    let candidates = outcome.map_or(preview.candidates.as_slice(), |o| o.candidates.as_slice());
    let mut known_bytes = 0u64;
    let mut unknown_sizes = 0usize;
    let versions: Vec<_> = candidates
        .iter()
        .map(|candidate| {
            if let Some(bytes) = candidate.bytes {
                known_bytes += bytes.get();
            } else {
                unknown_sizes += 1;
            }
            let state = if let Some(outcome) = outcome.filter(|o| {
                o.prune.pruned.contains(&candidate.target)
                    || matches!(candidate.state, CandidateState::AlreadyPruned)
            }) {
                if outcome.prune.cleanup_complete.contains(&candidate.target) {
                    "deleted"
                } else {
                    "cleanup-pending"
                }
            } else {
                match candidate.state {
                    CandidateState::Eligible => "eligible",
                    CandidateState::Protected => "protected",
                    CandidateState::NeedsHoldRelease => "needs-hold-release",
                    CandidateState::NeedsRecovery => "needs-recovery",
                    CandidateState::AlreadyPruned => {
                        if candidate.bytes.is_some_and(|b| b.get() == 0) {
                            "deleted"
                        } else {
                            "cleanup-pending"
                        }
                    }
                }
            };
            json!({"table":candidate.target.table,"partition":candidate.target.partition,
            "version":candidate.target.version.get().to_string(),"state":state,
            "reasons":candidate.reasons,"bytes":candidate.bytes.map(|b|b.get().to_string())})
        })
        .collect();
    let mut waiting: Vec<Value> = preview
        .needs_recovery
        .iter()
        .map(|run| json!({"dataset":preview.dataset,"run_id":run}))
        .collect();
    waiting.extend(
        preview
            .needs_consumer_recovery
            .iter()
            .map(|path| json!({"dataset":preview.dataset,"path":path})),
    );
    if let Some(pending) = &preview.pending {
        waiting.push(json!({"dataset":preview.dataset,"path":pending}));
    }
    let released: Vec<_> = outcome
        .into_iter()
        .flat_map(|o| &o.released_holds)
        .map(|path| json!({"dataset":preview.dataset,"path":path}))
        .collect();
    let mut operation_ids = ids.clone();
    if let Some(outcome) = outcome {
        operation_ids.extend(outcome.completed_operation_ids.iter().cloned());
    }
    json!({"dataset":preview.dataset,"mode":if apply {"apply"} else {"dry-run"},"versions":versions,
        "releasable_holds":preview.holds.iter().filter(|h|h.releasable).map(|h|hold_identity(&h.hold)).collect::<Vec<_>>(),
        "released_holds":released,"completed_operation_ids":operation_ids,"known_bytes":known_bytes.to_string(),
        "unknown_size_versions":unknown_sizes,"waiting":waiting})
}
pub(super) fn gc(args: &[String]) -> std::result::Result<Success, Failure> {
    let options = options(args)?;
    let root = Root::parse(&options.root)?;
    let canonical = root.canonical.clone();
    run(options, root).map_err(|mut error| {
        error.root = Some(canonical);
        error
    })
}
fn run(options: Options, root: Root) -> std::result::Result<Success, Failure> {
    let store = Store::open(root.open(false)?)?;
    let clock = SystemClock::default();
    let ttl = 900.min(store.parameters.max_lease_ttl_seconds.get());
    let gc = Gc::new(&store, &clock, ttl)?;
    if !options.apply {
        // No consumer journal, dataset lease, or repair is opened for preview.
        let scratch = tempfile::tempdir()
            .map_err(|_| public_error(ErrorCode::BackendFailure, "GC scratch unavailable"))?;
        let preview = gc.preview(&options.dataset, scratch.path())?;
        return Ok(Success {
            result: report(&preview, false, None, &BTreeSet::new()),
            root: Some(root.canonical),
        });
    }
    let publisher = Publisher::new(&store, &clock, ttl)?;
    let lease = publisher.prepare_lease(
        options.dataset.clone(),
        format!("grv-gc:{}", std::process::id()),
    )?;
    let journal = Journal::create(
        options.state.join("gc").join(Uuid::v4().as_str()),
        root.exclusions(),
    )?;
    let mut record: Record = journal.create_evidence(Evidence {
        intent: Fixed {
            root: root.canonical.clone(),
            lease: lease.clone(),
        },
        capture: None,
        progress: Progress {
            lease: LeaseProgress::Prepared,
            gc: GcProgress::Prepared,
            completed: BTreeSet::new(),
        },
        terminal: None,
    })?;
    let initial = gc.preview(&options.dataset, journal.directory())?;
    let mut lease_progress = record.evidence.progress.lease.clone();
    let mut owner = publisher.acquire_authorized(&lease, &mut lease_progress, |lease| {
        let mut progress = record.evidence.progress.clone();
        progress.lease = lease.clone();
        save(&journal, &mut record, progress)
    })?;
    let mut progress = record.evidence.progress.gc.clone();
    let attester = super::writer_attester::LocalWriterAttester::new(&options.state, &root);
    let outcome = gc.apply(
        &mut owner,
        &mut progress,
        journal.directory(),
        Some(&attester),
        |gc| {
            let mut progress = record.evidence.progress.clone();
            progress.gc = gc.clone();
            progress.completed.extend(completed(gc));
            save(&journal, &mut record, progress)
        },
    );
    let observation = gc.preview(&options.dataset, journal.directory());
    let release = publisher.release(&mut owner).err();
    let mut maintenance = release;
    if let Ok(outcome) = &outcome {
        let mut next = record.evidence.clone();
        next.terminal = Some(outcome.clone());
        maintenance = outcome
            .maintenance_error
            .clone()
            .or(journal.compare_and_swap(record.generation, next).err())
            .or(maintenance);
    }
    let preview = match observation {
        Ok(preview) => preview,
        Err(error) => {
            maintenance = Some(error);
            initial
        }
    };
    let result = report(
        &preview,
        true,
        outcome.as_ref().ok(),
        &record.evidence.progress.completed,
    );
    if let Some(error) = outcome.err().or(maintenance) {
        return Err(Failure {
            error,
            result: Some(result),
            root: Some(root.canonical),
        });
    }
    Ok(Success {
        result,
        root: Some(root.canonical),
    })
}
