//! Revision pin mutations use the dataset lease and durable operation evidence.
use super::{Failure, Success, backend::Root};
use grv_core::{
    admin::{Admin, AdminIntent, AdminOutcome, AdminProgress},
    clock::SystemClock,
    journal::{Envelope, Evidence, Journal},
    publication::{LeaseIntent, LeaseProgress, Publisher},
    store::{Result, Store, public_error},
};
use grv_types::{ErrorCode, Name, U64, Uuid};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, path::PathBuf};
fn invalid(message: &str) -> grv_types::PublicError {
    public_error(ErrorCode::InvalidArgument, message)
}
struct Options {
    dataset: Name,
    root: String,
    revision: U64,
    pin: Uuid,
    reason: Option<String>,
    state: PathBuf,
}
fn options(args: &[String], unpin: bool) -> Result<Options> {
    let dataset = Name::new(
        args.first()
            .ok_or_else(|| invalid("pin requires a dataset"))?,
    )
    .map_err(|_| invalid("invalid dataset"))?;
    let (mut root, mut revision, mut pin, mut reason, mut state) = (None, None, None, None, None);
    let mut seen = BTreeSet::new();
    for pair in args[1..].chunks(2) {
        if pair.len() != 2 || !seen.insert(pair[0].as_str()) {
            return Err(invalid("pin flags require unique names and values"));
        }
        match pair[0].as_str() {
            "--grv" => root = Some(pair[1].clone()),
            "--revision" => {
                revision = Some(
                    pair[1]
                        .parse::<U64>()
                        .map_err(|_| invalid("revision must be a canonical positive integer"))?,
                )
            }
            "--pin" => {
                pin =
                    Some(Uuid::new(&pair[1]).map_err(|_| invalid("pin must be a canonical UUID"))?)
            }
            "--reason" if !unpin && !pair[1].is_empty() => reason = Some(pair[1].clone()),
            "--state" => state = Some(PathBuf::from(&pair[1])),
            _ => return Err(invalid("unsupported pin flag")),
        }
    }
    let revision = revision
        .filter(|revision| revision.get() != 0)
        .ok_or_else(|| invalid("pin requires a positive --revision"))?;
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
        root: root.ok_or_else(|| invalid("pin requires --grv <root>"))?,
        revision,
        pin: if unpin {
            pin.ok_or_else(|| invalid("unpin requires an explicit --pin"))?
        } else {
            pin.unwrap_or_else(Uuid::v4)
        },
        reason: if unpin {
            None
        } else {
            Some(reason.ok_or_else(|| invalid("pin requires a nonempty --reason"))?)
        },
        state,
    })
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixed {
    root: String,
    operation: AdminIntent,
    lease: LeaseIntent,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Progress {
    lease: LeaseProgress,
    operation: AdminProgress,
}
type Record = Envelope<Fixed, Value, Progress, AdminOutcome>;
pub(super) fn pin(args: &[String]) -> std::result::Result<Success, Failure> {
    mutate(args, false)
}
pub(super) fn unpin(args: &[String]) -> std::result::Result<Success, Failure> {
    mutate(args, true)
}
fn mutate(args: &[String], unpin: bool) -> std::result::Result<Success, Failure> {
    let options = options(args, unpin)?;
    let selected = Root::parse(&options.root)?;
    let canonical = selected.canonical.clone();
    run(options, selected, unpin).map_err(|mut error| {
        error.root = Some(canonical);
        error
    })
}
fn save(journal: &Journal, record: &mut Record, progress: Progress) -> Result<()> {
    let mut next = record.evidence.clone();
    next.progress = progress;
    *record = journal.compare_and_swap(record.generation, next)?;
    Ok(())
}
fn run(options: Options, root: Root, unpin: bool) -> std::result::Result<Success, Failure> {
    let store = Store::open(root.open(false)?)?;
    let clock = SystemClock::default();
    let ttl = 900.min(store.parameters.max_lease_ttl_seconds.get());
    let publisher = Publisher::new(&store, &clock, ttl)?;
    let admin = Admin::new(&store, &clock, ttl)?;
    eprintln!("pin {}", options.pin);
    let revision =
        grv_storage::model::Counter::new(options.revision.get()).expect("validated int64 revision");
    let operation = if unpin {
        admin.prepare_revision_unpin(options.dataset.clone(), revision, options.pin)?
    } else {
        admin.prepare_revision_pin(
            options.dataset.clone(),
            revision,
            options.pin,
            options.reason.unwrap(),
        )?
    };
    let lease = publisher.prepare_lease(
        options.dataset.clone(),
        format!("grv-pin:{}", std::process::id()),
    )?;
    let directory = options
        .state
        .join("admin")
        .join(operation.operation().operation_id.as_str());
    let journal = Journal::create(&directory, root.exclusions())?;
    let mut record: Record = journal.create_evidence(Evidence {
        intent: Fixed {
            root: root.canonical.clone(),
            operation: operation.clone(),
            lease: lease.clone(),
        },
        capture: None,
        progress: Progress {
            lease: LeaseProgress::Prepared,
            operation: AdminProgress::Prepared,
        },
        terminal: None,
    })?;
    let mut lease_progress = record.evidence.progress.lease.clone();
    let mut owner = publisher.acquire_authorized(&lease, &mut lease_progress, |lease| {
        let mut progress = record.evidence.progress.clone();
        progress.lease = lease.clone();
        save(&journal, &mut record, progress)
    })?;
    let mut operation_progress = record.evidence.progress.operation.clone();
    let outcome = admin.execute(
        &mut owner,
        &operation,
        &mut operation_progress,
        journal.directory(),
        |_, operation| {
            let mut progress = record.evidence.progress.clone();
            progress.operation = operation.clone();
            save(&journal, &mut record, progress)
        },
    );
    let protections = if unpin && outcome.is_ok() {
        admin
            .remaining_revision_protections(&options.dataset, revision, journal.directory())
            .map(Some)
    } else {
        Ok(None)
    };
    let release = publisher.release(&mut owner).err();
    let outcome = outcome?;
    let pin = outcome.pin.as_ref().ok_or_else(|| {
        public_error(
            ErrorCode::IntegrityFailure,
            "completed pin operation has no scoped record",
        )
    })?;
    let mut result = json!({"pin":{"dataset":options.dataset,"revision":options.revision,"pin_id":pin.pin_id,"reason":pin.reason,"active":outcome.release.is_none(),"operation_id":pin.operation_id,"created_at":pin.created_at,"created_by":pin.created_by,"released_at":outcome.release.as_ref().map(|r|&r.released_at),"release_operation_id":outcome.release.as_ref().map(|r|&r.operation_id)},"no_op":outcome.noop});
    let protection_error = match protections {
        Ok(Some(protections)) => {
            result["remaining_protections"] = serde_json::to_value(protections).unwrap();
            None
        }
        Ok(None) => None,
        Err(error) => Some(error),
    };
    let mut next = record.evidence.clone();
    next.terminal = Some(outcome.clone());
    let persistence = journal.compare_and_swap(record.generation, next).err();
    if let Some(error) = outcome
        .maintenance_error
        .or(persistence)
        .or(protection_error)
        .or(release)
    {
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
