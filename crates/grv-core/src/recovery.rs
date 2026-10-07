//! Recovery replays existing pending authority and seals stopped expired runs.
//! It creates no publication, selection, retention or physical cleanup decision.
use crate::{
    clock::Clock,
    gc::{Gc, GcRun, StoppedWriterEvidence, WriterAttester},
    ownership::{Ownership, RecoveryIntent, RunOwner},
    publication::{LeaseIntent, LeaseOwner, LeaseProgress, Publisher},
    retention, revision,
    store::{Result, Store, backend_error, public_error},
};
use grv_storage::{Backend, ObjectKey, model::*};
use grv_types::{ErrorCode, Name, PublicError, RunId};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::path::Path;
fn protocol(message: &str) -> PublicError {
    public_error(ErrorCode::ProtocolFailure, message)
}
fn busy(message: &str) -> PublicError {
    public_error(ErrorCode::StateConflict, message)
}
fn object(dataset: &Name, suffix: &str) -> ObjectKey {
    ObjectKey::new(format!("datasets/{dataset}/{suffix}")).expect("canonical recovery path")
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryPending {
    pub operation_id: RunId,
    pub kind: String,
    pub path: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryRun {
    pub run_id: RunId,
    pub state: String,
    pub details: serde_json::Map<String, Value>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryOutcome {
    pub dataset: Name,
    pub target_run_id: Option<RunId>,
    pub runs: Vec<RecoveryRun>,
    pub completed_operation_ids: Vec<RunId>,
    pub pending: Option<RecoveryPending>,
    pub waiting: Vec<Value>,
    pub error: Option<PublicError>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryPreview {
    pub outcome: RecoveryOutcome,
    pub discovered: Vec<GcRun>,
    pub dataset_lease_busy: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum RecoveryProgress {
    Prepared,
    Acquire {
        intent: LeaseIntent,
        progress: Box<LeaseProgress>,
        outcome: Box<RecoveryOutcome>,
    },
    Replay {
        owner: Box<LeaseOwner>,
        observed: Latest,
        operation: Box<OperationRecord>,
        outcome: Box<RecoveryOutcome>,
    },
    Release {
        owner: Box<LeaseOwner>,
        outcome: Box<RecoveryOutcome>,
    },
    Work {
        remaining: Vec<RunId>,
        outcome: Box<RecoveryOutcome>,
    },
    Takeover {
        intent: Box<RecoveryIntent>,
        stopped: Option<StoppedWriterEvidence>,
        remaining: Vec<RunId>,
        outcome: Box<RecoveryOutcome>,
    },
    Seal {
        owner: Box<RunOwner>,
        remaining: Vec<RunId>,
        outcome: Box<RecoveryOutcome>,
    },
    Complete {
        outcome: Box<RecoveryOutcome>,
    },
}
impl RecoveryProgress {
    pub fn outcome(&self) -> Option<&RecoveryOutcome> {
        match self {
            Self::Prepared => None,
            Self::Acquire { outcome, .. }
            | Self::Replay { outcome, .. }
            | Self::Release { outcome, .. }
            | Self::Work { outcome, .. }
            | Self::Takeover { outcome, .. }
            | Self::Seal { outcome, .. }
            | Self::Complete { outcome } => Some(outcome),
        }
    }
}
pub struct Recovery<'a, B: Backend> {
    store: &'a Store<B>,
    clock: &'a dyn Clock,
    publisher: Publisher<'a, B>,
    ownership: Ownership<'a, B>,
    gc: Gc<'a, B>,
}
impl<'a, B: Backend> Recovery<'a, B> {
    pub fn new(store: &'a Store<B>, clock: &'a dyn Clock, ttl: u64) -> Result<Self> {
        Ok(Self {
            store,
            clock,
            publisher: Publisher::new(store, clock, ttl)?,
            ownership: Ownership::new(store, clock, ttl)?,
            gc: Gc::new(store, clock, ttl)?,
        })
    }
    fn read<T: Validate + DeserializeOwned>(&self, path: &ObjectKey) -> Result<T> {
        let (bytes, _) = self
            .store
            .backend
            .read_bytes(path, 64 * 1024 * 1024)
            .map_err(backend_error)?;
        decode_record(&bytes).map_err(|_| protocol("malformed required recovery record"))
    }
    fn latest(&self, dataset: &Name) -> Result<Latest> {
        self.read(&revision::latest_key(dataset)).map_err(|error| {
            if error.code == ErrorCode::NotFound {
                public_error(
                    ErrorCode::NotFound,
                    "dataset coordination LATEST is missing",
                )
            } else {
                error
            }
        })
    }
    fn pending(
        &self,
        dataset: &Name,
        latest: &Latest,
    ) -> Result<Option<(RecoveryPending, OperationRecord)>> {
        latest
            .pending
            .as_ref()
            .map(|path| {
                let record: OperationRecord = self.read(&object(dataset, path))?;
                if record.dataset != *dataset
                    || path != &format!(".states/operations/{}.json", record.operation_id)
                    || matches!(record.body, OperationPayload::Publish(_))
                {
                    return Err(protocol("pending operation identity or kind is invalid"));
                }
                let body = json!(record.body);
                Ok((
                    RecoveryPending {
                        operation_id: record.operation_id.clone(),
                        kind: body["kind"].as_str().unwrap().into(),
                        path: path.clone(),
                    },
                    record,
                ))
            })
            .transpose()
    }
    fn observed_runs(&self, dataset: &Name, target: Option<&RunId>) -> Result<Vec<GcRun>> {
        let runs = self
            .gc
            .recovery_runs(dataset, target)
            .map_err(|mut error| {
                if error.code == ErrorCode::IntegrityFailure {
                    error.code = ErrorCode::ProtocolFailure;
                }
                error
            })?;
        if target.is_some() && runs.is_empty() {
            return Err(public_error(
                ErrorCode::NotFound,
                "requested run control is missing",
            ));
        }
        Ok(runs)
    }
    pub fn preview(&self, dataset: &Name, target: Option<&RunId>) -> Result<RecoveryPreview> {
        // Runs may precede the first publication and its initial LATEST. A
        // targeted run never needs unrelated dataset coordination. Missing
        // LATEST with revision files is still a protocol error.
        let latest = if target.is_none() {
            revision::read_latest(self.store, dataset)
                .map_err(|mut error| {
                    if error.code == ErrorCode::IntegrityFailure {
                        error.code = ErrorCode::ProtocolFailure;
                    }
                    error
                })?
                .map(|value| value.0)
        } else {
            None
        };
        let pending = latest
            .as_ref()
            .map(|latest| self.pending(dataset, latest))
            .transpose()?
            .flatten()
            .map(|(pending, _)| pending);
        let dataset_lease_busy = pending.is_some()
            && latest
                .as_ref()
                .and_then(|latest| latest.lease.as_ref())
                .is_some_and(|lease| {
                    crate::clock::parse(&lease.expires_at) > crate::clock::parse(&self.clock.now())
                });
        let discovered = self.observed_runs(dataset, target)?;
        let mut outcome = RecoveryOutcome {
            dataset: dataset.clone(),
            target_run_id: target.cloned(),
            runs: vec![],
            completed_operation_ids: vec![],
            pending,
            waiting: vec![],
            error: None,
        };
        for run in &discovered {
            let state = if run.phase == RunPhase::Sealed && !run.sealed_file_missing {
                "complete"
            } else if run.phase == RunPhase::Sealed || run.expired && run.safe_to_recover {
                "eligible"
            } else if !run.expired {
                if target.is_some() {
                    "waiting"
                } else {
                    "skipped-live"
                }
            } else {
                "waiting"
            };
            let details = json!({"observed_phase":run.phase,"observed_expired":run.expired,"sealed_file_missing":run.sealed_file_missing,"requires_stopped_writer":!run.safe_to_recover,"blocking_claims":run.blocking_claims,"unresolved_claim_paths":run.unresolved_claims.iter().map(ObjectKey::as_str).collect::<Vec<_>>()});
            outcome.runs.push(RecoveryRun {
                run_id: run.run_id.clone(),
                state: state.into(),
                details: details.as_object().unwrap().clone(),
            });
            if state == "waiting" {
                outcome
                    .waiting
                    .push(json!({"dataset":dataset,"run_id":run.run_id}));
            }
        }
        if dataset_lease_busy {
            outcome.waiting.push(json!({"dataset":dataset,"operation_id":outcome.pending.as_ref().unwrap().operation_id}));
        }
        Ok(RecoveryPreview {
            outcome,
            discovered,
            dataset_lease_busy,
        })
    }
    fn work(&self, outcome: RecoveryOutcome) -> Result<RecoveryProgress> {
        let observed = self.observed_runs(&outcome.dataset, outcome.target_run_id.as_ref())?;
        let remaining = observed
            .into_iter()
            .filter(|run| {
                run.sealed_file_missing
                    || run.phase != RunPhase::Sealed
                        && (run.expired || outcome.target_run_id.is_some())
            })
            .map(|run| run.run_id)
            .collect();
        Ok(RecoveryProgress::Work {
            remaining,
            outcome: Box::new(outcome),
        })
    }
    fn set_run(outcome: &mut RecoveryOutcome, id: &RunId, state: &str) {
        if let Some(run) = outcome.runs.iter_mut().find(|run| run.run_id == *id) {
            run.state = state.into();
        }
        outcome
            .waiting
            .retain(|identity| identity["run_id"] != json!(id));
    }
    fn save(
        progress: &mut RecoveryProgress,
        next: RecoveryProgress,
        persist: &mut impl FnMut(&RecoveryProgress) -> Result<()>,
    ) -> Result<()> {
        persist(&next)?;
        *progress = next;
        Ok(())
    }
    /// Owns scoped run renewals. The caller durably saves every full progress
    /// record before its conditional effects; do not wrap another renewer.
    pub fn apply(
        &self,
        dataset: &Name,
        target: Option<&RunId>,
        progress: &mut RecoveryProgress,
        scratch: &Path,
        attester: Option<&dyn WriterAttester>,
        mut persist: impl FnMut(&RecoveryProgress) -> Result<()>,
    ) -> Result<RecoveryOutcome> {
        if progress.outcome().is_some_and(|outcome| {
            outcome.dataset != *dataset || outcome.target_run_id.as_ref() != target
        }) {
            return Err(protocol("recovery journal belongs to another scope"));
        }
        if let RecoveryProgress::Complete { outcome } = progress {
            return Ok((**outcome).clone());
        }
        let initial = self.preview(dataset, target)?.outcome;
        let result = self.apply_inner(dataset, target, progress, scratch, attester, &mut persist);
        match result {
            Ok(outcome) => Ok(outcome),
            Err(error) => {
                let mut outcome = progress.outcome().cloned().unwrap_or(initial);
                if error.code == ErrorCode::StateConflict {
                    let run = match progress {
                        RecoveryProgress::Seal { owner, .. } => {
                            Some(owner.control().run_id.clone())
                        }
                        RecoveryProgress::Takeover { intent, .. } => {
                            Some(intent.control().run_id.clone())
                        }
                        _ => None,
                    };
                    if let Some(id) = run {
                        Self::set_run(&mut outcome, &id, "waiting");
                        outcome.waiting.push(json!({"dataset":dataset,"run_id":id}));
                    }
                }
                outcome.error = Some(error);
                Ok(outcome)
            }
        }
    }
    fn apply_inner(
        &self,
        dataset: &Name,
        target: Option<&RunId>,
        progress: &mut RecoveryProgress,
        _scratch: &Path,
        attester: Option<&dyn WriterAttester>,
        persist: &mut impl FnMut(&RecoveryProgress) -> Result<()>,
    ) -> Result<RecoveryOutcome> {
        if matches!(progress, RecoveryProgress::Prepared) {
            let initial = self.preview(dataset, target)?.outcome;
            let next = if target.is_none() && initial.pending.is_some() {
                RecoveryProgress::Acquire {
                    intent: self.publisher.prepare_lease(
                        dataset.clone(),
                        format!("grv-recover:{}", std::process::id()),
                    )?,
                    progress: Box::new(LeaseProgress::Prepared),
                    outcome: Box::new(initial),
                }
            } else {
                self.work(initial)?
            };
            Self::save(progress, next, persist)?;
        }
        loop {
            match progress.clone() {
                RecoveryProgress::Prepared => unreachable!(),
                RecoveryProgress::Acquire {
                    intent,
                    progress: inner,
                    outcome,
                } => {
                    if intent.dataset != *dataset || target.is_some() {
                        return Err(protocol(
                            "dataset acquisition belongs to another recovery scope",
                        ));
                    }
                    let mut inner = *inner;
                    let owner =
                        self.publisher
                            .acquire_authorized(&intent, &mut inner, |inner| {
                                Self::save(
                                    progress,
                                    RecoveryProgress::Acquire {
                                        intent: intent.clone(),
                                        progress: Box::new(inner.clone()),
                                        outcome: outcome.clone(),
                                    },
                                    persist,
                                )
                            })?;
                    let observed = self.publisher.owned(&owner)?.0;
                    if let Some((pending, operation)) = self.pending(dataset, &observed)? {
                        let mut outcome = *outcome;
                        outcome.pending = Some(pending);
                        Self::save(
                            progress,
                            RecoveryProgress::Replay {
                                owner: Box::new(owner),
                                observed,
                                operation: Box::new(operation),
                                outcome: Box::new(outcome),
                            },
                            persist,
                        )?;
                    } else {
                        Self::save(
                            progress,
                            RecoveryProgress::Release {
                                owner: Box::new(owner),
                                outcome,
                            },
                            persist,
                        )?;
                    }
                }
                RecoveryProgress::Replay {
                    mut owner,
                    observed,
                    operation,
                    mut outcome,
                } => {
                    if owner.intent.dataset != *dataset || target.is_some() {
                        return Err(protocol("pending replay belongs to another recovery scope"));
                    }
                    // This immutable proof was durably observed and journaled before
                    // any effects. Replaying it creates no new retention decision.
                    let proof = retention::DurableOperationProof::from_pending(
                        dataset, &observed, &operation,
                    )?;
                    let current = self.latest(dataset)?;
                    if current.pending.as_deref()
                        == Some(
                            format!(".states/operations/{}.json", operation.operation_id).as_str(),
                        )
                    {
                        self.publisher.finish_pending(&mut owner)?;
                    } else {
                        retention::replay_proven(self.store, &proof, self.clock)?;
                    }
                    if !outcome
                        .completed_operation_ids
                        .contains(&operation.operation_id)
                    {
                        outcome.completed_operation_ids.push(operation.operation_id);
                    }
                    outcome.pending = None;
                    outcome
                        .waiting
                        .retain(|identity| identity["operation_id"].is_null());
                    Self::save(
                        progress,
                        RecoveryProgress::Release { owner, outcome },
                        persist,
                    )?;
                }
                RecoveryProgress::Release { mut owner, outcome } => {
                    if owner.intent.dataset != *dataset || target.is_some() {
                        return Err(protocol("lease release belongs to another recovery scope"));
                    }
                    let current = self.latest(dataset)?;
                    if current
                        .lease
                        .as_ref()
                        .is_some_and(|lease| lease.token == owner.intent.token)
                    {
                        self.publisher.release(&mut owner)?;
                    }
                    if let Some((pending, _)) = self.pending(dataset, &current)? {
                        let mut outcome = *outcome;
                        outcome.pending = Some(pending.clone());
                        outcome
                            .waiting
                            .retain(|identity| identity["operation_id"].is_null());
                        outcome
                            .waiting
                            .push(json!({"dataset":dataset,"operation_id":pending.operation_id}));
                        let intent = self.publisher.prepare_lease(
                            dataset.clone(),
                            format!("grv-recover:{}", std::process::id()),
                        )?;
                        Self::save(
                            progress,
                            RecoveryProgress::Acquire {
                                intent,
                                progress: Box::new(LeaseProgress::Prepared),
                                outcome: Box::new(outcome),
                            },
                            persist,
                        )?;
                    } else {
                        Self::save(progress, self.work(*outcome)?, persist)?;
                    }
                }
                RecoveryProgress::Work {
                    mut remaining,
                    mut outcome,
                } => {
                    if target.is_some_and(|id| remaining.iter().any(|other| other != id)) {
                        return Err(protocol("targeted recovery queue contains another run"));
                    }
                    let Some(id) = remaining.first().cloned() else {
                        if !outcome.waiting.is_empty() {
                            let retry = outcome
                                .waiting
                                .iter()
                                .filter_map(|identity| identity["run_id"].as_str())
                                .map(|id| RunId::new(id).expect("validated waiting run"))
                                .collect();
                            Self::save(
                                progress,
                                RecoveryProgress::Work {
                                    remaining: retry,
                                    outcome,
                                },
                                persist,
                            )?;
                            return Err(busy("recovery has waiting run work"));
                        }
                        outcome.error = None;
                        Self::save(
                            progress,
                            RecoveryProgress::Complete {
                                outcome: outcome.clone(),
                            },
                            persist,
                        )?;
                        return Ok(*outcome);
                    };
                    let observed = self.observed_runs(dataset, Some(&id))?.remove(0);
                    if observed.phase == RunPhase::Sealed && !observed.sealed_file_missing {
                        remaining.remove(0);
                        Self::set_run(&mut outcome, &id, "complete");
                        Self::save(
                            progress,
                            RecoveryProgress::Work { remaining, outcome },
                            persist,
                        )?;
                        continue;
                    }
                    if observed.phase != RunPhase::Sealed && !observed.expired {
                        Self::set_run(
                            &mut outcome,
                            &id,
                            if target.is_some() {
                                "waiting"
                            } else {
                                "skipped-live"
                            },
                        );
                        if target.is_some() {
                            outcome.waiting.push(json!({"dataset":dataset,"run_id":id}));
                            remaining.remove(0);
                            Self::save(
                                progress,
                                RecoveryProgress::Work { remaining, outcome },
                                persist,
                            )?;
                            continue;
                        }
                        remaining.remove(0);
                        Self::save(
                            progress,
                            RecoveryProgress::Work { remaining, outcome },
                            persist,
                        )?;
                        continue;
                    }
                    let control: RunControl =
                        self.read(&object(dataset, &format!(".runs/{id}.control.json")))?;
                    let stopped = if observed.safe_to_recover || control.phase == RunPhase::Sealed {
                        None
                    } else {
                        let evidence = attester
                            .map(|attester| attester.stopped(dataset, &control))
                            .transpose()?
                            .flatten();
                        if evidence
                            .as_ref()
                            .is_some_and(|proof| !proof.matches(dataset, &control))
                        {
                            return Err(protocol(
                                "stopped-writer evidence belongs to another run epoch",
                            ));
                        }
                        if evidence.is_none() {
                            Self::set_run(&mut outcome, &id, "waiting");
                            outcome.waiting.push(json!({"dataset":dataset,"run_id":id}));
                            remaining.remove(0);
                            Self::save(
                                progress,
                                RecoveryProgress::Work { remaining, outcome },
                                persist,
                            )?;
                            continue;
                        }
                        evidence
                    };
                    let intent = self.ownership.prepare_run_recovery(dataset.clone(), id)?;
                    if intent.previous() != &control {
                        return Err(busy("run epoch changed before recovery intent"));
                    }
                    remaining.remove(0);
                    Self::set_run(&mut outcome, &control.run_id, "recovering");
                    Self::save(
                        progress,
                        RecoveryProgress::Takeover {
                            intent: Box::new(intent),
                            stopped,
                            remaining,
                            outcome,
                        },
                        persist,
                    )?;
                }
                RecoveryProgress::Takeover {
                    intent,
                    stopped,
                    remaining,
                    outcome,
                } => {
                    if intent.dataset() != dataset
                        || target.is_some_and(|id| id != &intent.control().run_id)
                    {
                        return Err(protocol(
                            "takeover intent belongs to another recovery scope",
                        ));
                    }
                    if stopped
                        .as_ref()
                        .is_some_and(|proof| !proof.matches(dataset, intent.previous()))
                    {
                        return Err(protocol(
                            "journaled stopped-writer evidence differs from recovery epoch",
                        ));
                    }
                    let owner = self.ownership.recover_authorized(&intent)?;
                    Self::save(
                        progress,
                        RecoveryProgress::Seal {
                            owner: Box::new(owner),
                            remaining,
                            outcome,
                        },
                        persist,
                    )?;
                }
                RecoveryProgress::Seal {
                    mut owner,
                    remaining,
                    mut outcome,
                } => {
                    if owner.dataset() != dataset
                        || target.is_some_and(|id| id != &owner.control().run_id)
                    {
                        return Err(protocol(
                            "sealing authority belongs to another recovery scope",
                        ));
                    }
                    self.ownership.seal_renewed(&mut owner)?;
                    Self::set_run(&mut outcome, &owner.control().run_id, "sealed");
                    Self::save(
                        progress,
                        RecoveryProgress::Work { remaining, outcome },
                        persist,
                    )?;
                }
                RecoveryProgress::Complete { outcome } => return Ok(*outcome),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        clock::{new_run_id, parse, timestamp},
        ownership::ReservationProgress,
        store::InitOptions,
    };
    use grv_adapter_api::{Column, TableContract};
    use grv_storage::{FaultInjector, FaultPoint, LocalBackend};
    use grv_types::{Digest, Timestamp};
    use std::{
        sync::{
            Arc,
            atomic::{AtomicU64, AtomicUsize, Ordering},
        },
        time::Duration,
    };
    struct TestClock(AtomicU64);
    impl TestClock {
        fn advance(&self, seconds: u64) {
            self.0.fetch_add(seconds, Ordering::SeqCst);
        }
    }
    impl Clock for TestClock {
        fn now(&self) -> Timestamp {
            timestamp(
                parse(&Timestamp::new("2020-01-01T00:00:00Z").unwrap())
                    + chrono::Duration::seconds(self.0.load(Ordering::SeqCst) as i64),
            )
        }
        fn elapsed(&self) -> Duration {
            Duration::from_secs(self.0.load(Ordering::SeqCst))
        }
    }
    #[derive(Default)]
    struct Fault {
        remaining: AtomicUsize,
        after: AtomicUsize,
    }
    impl Fault {
        fn arm(&self, n: usize, after: bool) {
            self.after.store(usize::from(after), Ordering::SeqCst);
            self.remaining.store(n, Ordering::SeqCst);
        }
    }
    impl FaultInjector for Fault {
        fn check(&self, point: FaultPoint) -> std::io::Result<()> {
            if point
                != if self.after.load(Ordering::SeqCst) == 1 {
                    FaultPoint::AfterInstall
                } else {
                    FaultPoint::BeforeInstall
                }
            {
                return Ok(());
            }
            let count = self.remaining.load(Ordering::SeqCst);
            if count > 0 && self.remaining.fetch_sub(1, Ordering::SeqCst) == 1 {
                return Err(std::io::Error::other("injected recovery write crash"));
            }
            Ok(())
        }
    }
    struct Fixture {
        root: tempfile::TempDir,
        store: Store<LocalBackend>,
        clock: TestClock,
        fault: Arc<Fault>,
    }
    impl Fixture {
        fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let fault = Arc::new(Fault::default());
            let (store, _) = Store::initialize(
                LocalBackend::create(root.path())
                    .unwrap()
                    .with_faults(fault.clone()),
                InitOptions::default(),
            )
            .unwrap();
            Self {
                root,
                store,
                clock: TestClock(AtomicU64::new(0)),
                fault,
            }
        }
        fn owner(&self) -> RunOwner {
            let own = Ownership::new(&self.store, &self.clock, 60).unwrap();
            let mut owner = own
                .prepare_run(
                    name("data"),
                    new_run_id(&self.clock.now()).unwrap(),
                    0.into(),
                    vec![],
                    None,
                )
                .unwrap();
            own.commit_run(&owner).unwrap();
            own.confirm_holds(&mut owner).unwrap();
            owner
        }
        fn recovery(&self) -> Recovery<'_, LocalBackend> {
            Recovery::new(&self.store, &self.clock, 60).unwrap()
        }
        fn allocate(&self, owner: &RunOwner) {
            let own = Ownership::new(&self.store, &self.clock, 60).unwrap();
            let contract = TableContract {
                columns: vec![Column {
                    name: "id".into(),
                    logical_type: json!("int64"),
                }],
                partition_keys: vec![],
                extensions: json!({}),
                column_ext: json!({}),
            };
            let intent = own
                .prepare_reservation(
                    owner,
                    TableLayout {
                        table: name("events"),
                        partition_keys: vec![],
                        extensions: None,
                    },
                    Partition::new(),
                    &contract,
                )
                .unwrap();
            own.reserve_authorized(owner, &intent, &mut ReservationProgress::Prepared, |_| {
                Ok(())
            })
            .unwrap();
        }
        fn control(&self, id: &RunId) -> RunControl {
            self.recovery()
                .read(&object(&name("data"), &format!(".runs/{id}.control.json")))
                .unwrap()
        }
    }
    fn name(value: &str) -> Name {
        Name::new(value).unwrap()
    }
    struct Attest(bool);
    impl WriterAttester for Attest {
        fn stopped(
            &self,
            dataset: &Name,
            control: &RunControl,
        ) -> Result<Option<StoppedWriterEvidence>> {
            let mut control = control.clone();
            if !self.0 {
                control.owner_token = OwnerToken::generate();
            }
            Ok(Some(StoppedWriterEvidence::verified(
                dataset.clone(),
                &control,
                Digest::new("0".repeat(64)).unwrap(),
            )))
        }
    }
    #[test]
    fn every_durable_recovery_progress_boundary_replays_exact_owned_seal() {
        for crash in 1..=5 {
            let f = Fixture::new();
            let owner = f.owner();
            f.clock.advance(61);
            let r = f.recovery();
            let mut progress = RecoveryProgress::Prepared;
            let mut durable = None;
            let mut count = 0;
            let result = r
                .apply(
                    &name("data"),
                    Some(&owner.control().run_id),
                    &mut progress,
                    f.root.path(),
                    None,
                    |next| {
                        count += 1;
                        durable = Some(serde_json::to_vec(next).unwrap());
                        if count == crash {
                            return Err(public_error(
                                ErrorCode::BackendFailure,
                                "injected journal-boundary crash",
                            ));
                        }
                        Ok(())
                    },
                )
                .unwrap();
            assert!(result.error.is_some(), "boundary {crash}");
            let mut progress: RecoveryProgress = serde_json::from_slice(&durable.unwrap()).unwrap();
            let outcome = r
                .apply(
                    &name("data"),
                    Some(&owner.control().run_id),
                    &mut progress,
                    f.root.path(),
                    None,
                    |_| Ok(()),
                )
                .unwrap();
            assert!(
                outcome.error.is_none(),
                "boundary {crash}: {:?}",
                outcome.error
            );
            let sealed = object(
                &name("data"),
                &format!(".runs/{}.json", owner.control().run_id),
            );
            let bytes = f.store.backend.read_bytes(&sealed, 1024 * 1024).unwrap().0;
            let replay = r
                .apply(
                    &name("data"),
                    Some(&owner.control().run_id),
                    &mut progress,
                    f.root.path(),
                    None,
                    |_| panic!("terminal recovery must not persist"),
                )
                .unwrap();
            assert!(replay.error.is_none());
            assert_eq!(
                bytes,
                f.store.backend.read_bytes(&sealed, 1024 * 1024).unwrap().0
            );
        }
    }
    #[test]
    fn sealed_control_without_payload_is_materialized_after_crash_and_lost_response_is_proven() {
        for after in [false, true] {
            let f = Fixture::new();
            let owner = f.owner();
            f.clock.advance(61);
            f.fault.arm(3, after);
            let r = f.recovery();
            let mut progress = RecoveryProgress::Prepared;
            let first = r
                .apply(
                    &name("data"),
                    Some(&owner.control().run_id),
                    &mut progress,
                    f.root.path(),
                    None,
                    |_| Ok(()),
                )
                .unwrap();
            assert_eq!(first.error.is_none(), after);
            assert_eq!(f.control(&owner.control().run_id).phase, RunPhase::Sealed);
            let outcome = r
                .apply(
                    &name("data"),
                    Some(&owner.control().run_id),
                    &mut progress,
                    f.root.path(),
                    None,
                    |_| Ok(()),
                )
                .unwrap();
            assert!(outcome.error.is_none());
            let sealed: SealedRun = r
                .read(&object(
                    &name("data"),
                    &format!(".runs/{}.json", owner.control().run_id),
                ))
                .unwrap();
            sealed
                .validate_control(&f.control(&owner.control().run_id))
                .unwrap();
        }
    }
    #[test]
    fn live_claim_waits_even_with_stopped_writer_and_exact_epoch_seals_after_claim_expiry() {
        let f = Fixture::new();
        let owner = f.owner();
        f.clock.advance(31);
        f.allocate(&owner);
        f.clock.advance(30);
        let r = f.recovery();
        let original = f.control(&owner.control().run_id);
        let mut progress = RecoveryProgress::Prepared;
        let without = r
            .apply(
                &name("data"),
                Some(&owner.control().run_id),
                &mut progress,
                f.root.path(),
                None,
                |_| Ok(()),
            )
            .unwrap();
        assert_eq!(without.error.unwrap().code, ErrorCode::StateConflict);
        assert_eq!(f.control(&owner.control().run_id), original);
        let wrong = r
            .apply(
                &name("data"),
                Some(&owner.control().run_id),
                &mut progress,
                f.root.path(),
                Some(&Attest(false)),
                |_| Ok(()),
            )
            .unwrap();
        assert_eq!(wrong.error.unwrap().code, ErrorCode::ProtocolFailure);
        assert_eq!(f.control(&owner.control().run_id), original);
        let blocked = r
            .apply(
                &name("data"),
                Some(&owner.control().run_id),
                &mut progress,
                f.root.path(),
                Some(&Attest(true)),
                |_| Ok(()),
            )
            .unwrap();
        assert_eq!(blocked.error.unwrap().code, ErrorCode::StateConflict);
        assert_eq!(blocked.waiting.len(), 1);
        assert_eq!(
            f.control(&owner.control().run_id).phase,
            RunPhase::Recovering
        );
        f.clock.advance(31);
        let completed = r
            .apply(
                &name("data"),
                Some(&owner.control().run_id),
                &mut progress,
                f.root.path(),
                None,
                |_| Ok(()),
            )
            .unwrap();
        assert!(completed.error.is_none());
        assert!(
            f.control(&owner.control().run_id)
                .entries
                .unwrap()
                .is_empty()
        );
    }
    #[test]
    fn missing_claim_becomes_unproven_without_salvaging_an_allocated_version() {
        let f = Fixture::new();
        let owner = f.owner();
        f.allocate(&owner);
        f.store
            .backend
            .delete(&object(&name("data"), "events/.claim"))
            .unwrap();
        f.clock.advance(61);
        let r = f.recovery();
        let mut progress = RecoveryProgress::Prepared;
        let completed = r
            .apply(
                &name("data"),
                Some(&owner.control().run_id),
                &mut progress,
                f.root.path(),
                None,
                |_| Ok(()),
            )
            .unwrap();
        assert!(completed.error.is_none());
        assert!(
            f.control(&owner.control().run_id)
                .entries
                .unwrap()
                .is_empty()
        );
        let files = f
            .store
            .backend
            .list(
                &grv_storage::ObjectPrefix::new(format!(
                    "datasets/data/.runs/{}.allocations/",
                    owner.control().run_id
                ))
                .unwrap(),
                grv_storage::ListMode::Children,
            )
            .unwrap();
        let grv_storage::ListEntry::Object(path) = &files[0] else {
            panic!()
        };
        let record: AllocationRecord = r.read(path).unwrap();
        assert_eq!(record.state, AllocationState::Unproven);
    }
    #[test]
    fn targeted_scope_leaves_pending_and_unrelated_corrupt_controls_untouched() {
        let f = Fixture::new();
        let owner = f.owner();
        f.clock.advance(61);
        let latest = revision::latest_key(&name("data"));
        f.store
            .backend
            .create_bytes(&latest, &encode_record(&Latest::empty()).unwrap())
            .unwrap();
        let (bytes, meta) = f.store.backend.read_bytes(&latest, 1024 * 1024).unwrap();
        let mut record: Latest = decode_record(&bytes).unwrap();
        let operation = new_run_id(&f.clock.now()).unwrap();
        record.pending = Some(format!(".states/operations/{operation}.json"));
        let bytes = encode_record(&record).unwrap();
        f.store
            .backend
            .put_bytes(&latest, &meta.validator, &bytes)
            .unwrap();
        f.store
            .backend
            .create_bytes(
                &object(
                    &name("data"),
                    &format!(".states/operations/{operation}.json"),
                ),
                b"invalid pending JSON",
            )
            .unwrap();
        f.store
            .backend
            .create_bytes(
                &object(&name("data"), ".runs/garbage.control.json"),
                b"invalid unrelated control",
            )
            .unwrap();
        let r = f.recovery();
        assert!(r.preview(&name("data"), None).is_err());
        let mut progress = RecoveryProgress::Prepared;
        let result = r
            .apply(
                &name("data"),
                Some(&owner.control().run_id),
                &mut progress,
                f.root.path(),
                None,
                |_| Ok(()),
            )
            .unwrap();
        assert!(result.error.is_none());
        assert!(result.completed_operation_ids.is_empty());
        assert_eq!(
            f.store.backend.read_bytes(&latest, 1024 * 1024).unwrap().0,
            bytes
        );
    }
    #[test]
    fn pending_completion_survives_lost_clear_journal_and_foreign_lease_without_overwriting_new_work()
     {
        for new_pending in [false, true] {
            let f = Fixture::new();
            let _owner = f.owner();
            f.clock.advance(61);
            let layout = TableLayout {
                table: name("events"),
                partition_keys: vec![],
                extensions: None,
            };
            f.store
                .backend
                .create_bytes(
                    &object(&name("data"), "events/.layout.json"),
                    &encode_record(&layout).unwrap(),
                )
                .unwrap();
            let operation = OperationRecord {
                operation_id: new_run_id(&f.clock.now()).unwrap(),
                dataset: name("data"),
                created_at: f.clock.now(),
                created_by: "recovery-test".into(),
                body: OperationPayload::Pin(PinPayload {
                    pin_id: grv_types::Uuid::v4(),
                    scope: PinScope::Table(TableScope {
                        table: name("events"),
                    }),
                    reason: Some("committed prior intent".into()),
                }),
            };
            f.store
                .backend
                .create_bytes(
                    &object(
                        &name("data"),
                        &format!(".states/operations/{}.json", operation.operation_id),
                    ),
                    &encode_record(&operation).unwrap(),
                )
                .unwrap();
            let mut latest = Latest::empty();
            latest.pending = Some(format!(
                ".states/operations/{}.json",
                operation.operation_id
            ));
            f.store
                .backend
                .create_bytes(
                    &revision::latest_key(&name("data")),
                    &encode_record(&latest).unwrap(),
                )
                .unwrap();
            let r = f.recovery();
            let mut progress = RecoveryProgress::Prepared;
            let first = r
                .apply(
                    &name("data"),
                    None,
                    &mut progress,
                    f.root.path(),
                    None,
                    |next| {
                        if matches!(next, RecoveryProgress::Release { .. }) {
                            Err(public_error(
                                ErrorCode::BackendFailure,
                                "crash after committed clear before journal",
                            ))
                        } else {
                            Ok(())
                        }
                    },
                )
                .unwrap();
            assert!(first.error.is_some());
            assert!(matches!(progress, RecoveryProgress::Replay { .. }));
            f.clock.advance(61);
            let publisher = Publisher::new(&f.store, &f.clock, 60).unwrap();
            let intent = publisher
                .prepare_lease(name("data"), "foreign live owner".into())
                .unwrap();
            let foreign = publisher
                .acquire_authorized(&intent, &mut LeaseProgress::Prepared, |_| Ok(()))
                .unwrap();
            if new_pending {
                let next = OperationRecord {
                    operation_id: new_run_id(&f.clock.now()).unwrap(),
                    dataset: name("data"),
                    created_at: f.clock.now(),
                    created_by: "foreign-owner".into(),
                    body: OperationPayload::Pin(PinPayload {
                        pin_id: grv_types::Uuid::v4(),
                        scope: PinScope::Table(TableScope {
                            table: name("events"),
                        }),
                        reason: Some("new authorized decision".into()),
                    }),
                };
                f.store
                    .backend
                    .create_bytes(
                        &object(
                            &name("data"),
                            &format!(".states/operations/{}.json", next.operation_id),
                        ),
                        &encode_record(&next).unwrap(),
                    )
                    .unwrap();
                let path = revision::latest_key(&name("data"));
                let (bytes, meta) = f.store.backend.read_bytes(&path, 1024 * 1024).unwrap();
                let mut latest: Latest = decode_record(&bytes).unwrap();
                latest.pending = Some(format!(".states/operations/{}.json", next.operation_id));
                f.store
                    .backend
                    .put_bytes(&path, &meta.validator, &encode_record(&latest).unwrap())
                    .unwrap();
            }
            let before = f
                .store
                .backend
                .read_bytes(&revision::latest_key(&name("data")), 1024 * 1024)
                .unwrap()
                .0;
            let replay = r
                .apply(
                    &name("data"),
                    None,
                    &mut progress,
                    f.root.path(),
                    None,
                    |_| Ok(()),
                )
                .unwrap();
            assert_eq!(replay.error.is_some(), new_pending);
            assert_eq!(replay.completed_operation_ids, vec![operation.operation_id]);
            assert_eq!(
                f.store
                    .backend
                    .read_bytes(&revision::latest_key(&name("data")), 1024 * 1024)
                    .unwrap()
                    .0,
                before
            );
            assert_eq!(
                r.latest(&name("data")).unwrap().lease.unwrap().token,
                foreign.intent.token
            );
            if new_pending {
                assert!(replay.pending.is_some());
                assert!(!replay.waiting.is_empty());
            }
        }
    }
}
