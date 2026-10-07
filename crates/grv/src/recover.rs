//! Durable, conservative recovery. The default frontend has no trusted external
//! writer attester, so an allocated/acquired claim is reported waiting.
use super::{Failure, Success, backend::Root};
use grv_core::{
    clock::SystemClock,
    journal::{Envelope, Evidence, Journal},
    recovery::{Recovery, RecoveryOutcome, RecoveryProgress},
    store::{Result, Store, public_error},
};
use grv_types::{ErrorCode, Name, RunId, Uuid};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, path::PathBuf};
fn invalid(message: &str) -> grv_types::PublicError {
    public_error(ErrorCode::InvalidArgument, message)
}
struct Options {
    dataset: Name,
    root: String,
    target: Option<RunId>,
    dry: bool,
    state: PathBuf,
}
fn options(args: &[String]) -> Result<Options> {
    let dataset = Name::new(
        args.first()
            .ok_or_else(|| invalid("recover requires a dataset"))?,
    )
    .map_err(|_| invalid("invalid dataset"))?;
    let mut root = None;
    let mut target = None;
    let mut dry = false;
    let mut state = None;
    let mut seen = BTreeSet::new();
    let mut index = 1;
    while index < args.len() {
        let flag = &args[index];
        if !seen.insert(flag.as_str()) {
            return Err(invalid("duplicate recovery flag"));
        }
        if flag == "--dry-run" {
            dry = true;
            index += 1;
            continue;
        }
        let value = args
            .get(index + 1)
            .filter(|value| !value.starts_with("--"))
            .ok_or_else(|| invalid("recovery flag value required"))?;
        match flag.as_str() {
            "--grv" => root = Some(value.clone()),
            "--run" => target = Some(RunId::new(value).map_err(|_| invalid("invalid run ID"))?),
            "--state" => state = Some(PathBuf::from(value)),
            _ => return Err(invalid("unknown recovery flag")),
        }
        index += 2;
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
        root: root.ok_or_else(|| invalid("recover requires --grv <root>"))?,
        target,
        dry,
        state,
    })
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixed {
    root: String,
    dataset: Name,
    target_run_id: Option<RunId>,
}
type Record = Envelope<Fixed, Value, RecoveryProgress, RecoveryOutcome>;
fn report(outcome: &RecoveryOutcome, dry: bool) -> Value {
    let mut completed_operation_ids = outcome.completed_operation_ids.clone();
    completed_operation_ids.sort();
    json!({"dataset":outcome.dataset,"dry_run":dry,"target_run_id":outcome.target_run_id,"runs":outcome.runs,"completed_operation_ids":completed_operation_ids,"pending":outcome.pending,"waiting":outcome.waiting})
}
pub(super) fn recover(args: &[String]) -> std::result::Result<Success, Failure> {
    let options = options(args)?;
    let root = Root::parse(&options.root)?;
    let canonical = root.canonical.clone();
    execute(options, root).map_err(|mut error| {
        error.root = Some(canonical);
        error
    })
}
fn execute(options: Options, root: Root) -> std::result::Result<Success, Failure> {
    let store = Store::open(root.open(false)?)?;
    let clock = SystemClock::default();
    let recovery = Recovery::new(
        &store,
        &clock,
        900.min(store.parameters.max_lease_ttl_seconds.get()),
    )?;
    let preview = recovery.preview(&options.dataset, options.target.as_ref())?;
    if options.dry {
        return Ok(Success {
            result: report(&preview.outcome, true),
            root: Some(root.canonical),
        });
    }
    let id = Uuid::v4();
    let journal = Journal::create(
        options.state.join("recover").join(id.as_str()),
        root.exclusions(),
    )?;
    let mut record: Record = journal.create_evidence(Evidence {
        intent: Fixed {
            root: root.canonical.clone(),
            dataset: options.dataset.clone(),
            target_run_id: options.target.clone(),
        },
        capture: None,
        progress: RecoveryProgress::Prepared,
        terminal: None,
    })?;
    let attester = super::writer_attester::LocalWriterAttester::new(&options.state, &root);
    let mut progress = record.evidence.progress.clone();
    let outcome = recovery.apply(
        &options.dataset,
        options.target.as_ref(),
        &mut progress,
        journal.directory(),
        Some(&attester),
        |progress| {
            let mut evidence = record.evidence.clone();
            evidence.progress = progress.clone();
            record = journal.compare_and_swap(record.generation, evidence)?;
            Ok(())
        },
    );
    let outcome = match outcome {
        Ok(outcome) => outcome,
        Err(error) => {
            return Err(Failure {
                error,
                result: Some(report(
                    record
                        .evidence
                        .progress
                        .outcome()
                        .unwrap_or(&preview.outcome),
                    false,
                )),
                root: None,
            });
        }
    };
    let result = report(&outcome, false);
    if let Some(error) = outcome.error.clone() {
        return Err(Failure {
            error,
            result: Some(result),
            root: None,
        });
    }
    let mut evidence = record.evidence.clone();
    evidence.terminal = Some(outcome);
    journal
        .compare_and_swap(record.generation, evidence)
        .map_err(|error| Failure {
            error,
            result: Some(result.clone()),
            root: None,
        })?;
    Ok(Success {
        result,
        root: Some(root.canonical),
    })
}
