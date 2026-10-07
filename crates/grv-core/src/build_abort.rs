//! Stop an owned build without initiating publication, acquisition or allocation.
//! Caller journals progress and retains stopped workspace authority until seal.
use crate::{
    build_publication::{self, DerivedGroup},
    clock::Clock,
    ownership::{AbortClaim, AbortClaimProgress, Ownership, RunOwner},
    publication::{LeaseAbortProgress, LeaseProgress, Publisher},
    push::{GroupProgress, LeaseAttempt, PushOutcome},
    store::{Result, Store, public_error},
};
use grv_storage::{Backend, model::SealedRun};
use grv_types::ErrorCode;
use serde::{Deserialize, Serialize};
use std::{cell::RefCell, collections::BTreeSet, path::Path};
fn integrity(message: &str) -> grv_types::PublicError {
    public_error(ErrorCode::IntegrityFailure, message)
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum Outcome {
    Published { outcome: PushOutcome },
    Aborted { sealed: SealedRun },
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cleanup {
    claim: AbortClaim,
    progress: AbortClaimProgress,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Progress {
    pub owner: RunOwner,
    pub publication: Option<build_publication::Progress>,
    cleanup: Vec<Cleanup>,
    lease: LeaseAbortProgress,
    sealing: bool,
    pub terminal: Option<Outcome>,
}
fn claims(
    owner: &RunOwner,
    publication: Option<&build_publication::Progress>,
) -> Result<Vec<AbortClaim>> {
    let Some(publication) = publication else {
        return Ok(vec![]);
    };
    if publication.plan.digest()? != publication.plan_digest
        || publication.plan.groups.len() != publication.groups.len()
        || publication.owner.dataset() != owner.dataset()
        || publication.plan.dataset != *owner.dataset()
        || publication.owner.control().run_id != owner.control().run_id
        || publication.plan.run_id != owner.control().run_id
        || publication.plan.base_revision != owner.control().base_revision
        || publication.owner.control().owner_token != owner.control().owner_token
        || publication.owner.control().inputs != owner.control().inputs
    {
        return Err(integrity(
            "abort publication differs from fixed owner and plan",
        ));
    }
    let mut claims = vec![];
    for group in &publication.groups {
        match group {
            DerivedGroup::Pending | DerivedGroup::Finalized { .. } => {}
            DerivedGroup::Reserving { intent, progress } => {
                claims.push(AbortClaim::from_derived_reserving(owner, intent, progress)?)
            }
            DerivedGroup::Reserved { reservation, .. }
            | DerivedGroup::Written { reservation, .. } => {
                claims.push(AbortClaim::from_derived(owner, reservation)?)
            }
        }
    }
    if let Some(push) = &publication.publication {
        if push.plan.digest()? != push.plan_digest
            || push.plan_digest != publication.plan_digest
            || push.groups.len() != push.plan.groups.len()
            || push.owner.dataset() != owner.dataset()
            || push.owner.control().run_id != owner.control().run_id
            || push.owner.control().owner_token != owner.control().owner_token
        {
            return Err(integrity(
                "abort inner publication differs from fixed build",
            ));
        }
        for (group, plan) in push.groups.iter().zip(&push.plan.groups) {
            match group {
                GroupProgress::Pending | GroupProgress::Finalized { .. } => {}
                GroupProgress::Reserving { intent, progress } => {
                    claims.push(AbortClaim::from_reserving(owner, intent, progress)?)
                }
                GroupProgress::Reserved { reservation }
                | GroupProgress::Written { reservation, .. } => claims.push(
                    AbortClaim::from_reservation(owner, reservation, &plan.plan.contract)?,
                ),
            }
        }
    }
    let mut tokens = BTreeSet::new();
    for claim in &claims {
        let token = grv_types::canonical_json(claim.token())
            .map_err(|_| integrity("invalid abort claim identity"))?;
        if !tokens.insert(token) {
            return Err(integrity("abort plan repeats an acquisition"));
        }
    }
    Ok(claims)
}
impl Progress {
    /// Pure fixed preparation. Persist before calling Aborter::abort.
    pub fn prepare(
        owner: RunOwner,
        publication: Option<build_publication::Progress>,
    ) -> Result<Self> {
        let cleanup = claims(&owner, publication.as_ref())?
            .into_iter()
            .map(|claim| Cleanup {
                claim,
                progress: AbortClaimProgress::Prepared,
            })
            .collect();
        Ok(Self {
            owner,
            publication,
            cleanup,
            lease: LeaseAbortProgress::Prepared,
            sealing: false,
            terminal: None,
        })
    }
    fn validate(&self) -> Result<()> {
        let expected = claims(&self.owner, self.publication.as_ref())?;
        let actual: Vec<_> = self.cleanup.iter().map(|item| &item.claim).collect();
        if grv_types::canonical_json(&expected).map_err(|_| integrity("invalid abort plan"))?
            != grv_types::canonical_json(&actual).map_err(|_| integrity("invalid abort plan"))?
        {
            return Err(integrity("fixed abort claims changed"));
        }
        if self.sealing
            && (self
                .cleanup
                .iter()
                .any(|cleanup| !matches!(cleanup.progress, AbortClaimProgress::Done { .. }))
                || (self
                    .publication
                    .as_ref()
                    .and_then(|p| p.publication.as_ref())
                    .and_then(|p| p.lease.as_ref())
                    .is_some()
                    && !matches!(self.lease, LeaseAbortProgress::Done)))
        {
            return Err(integrity(
                "sealing intent precedes completed stopped cleanup",
            ));
        }
        Ok(())
    }
}
fn save(
    progress: &mut Progress,
    next: Progress,
    persist: &mut impl FnMut(&Progress) -> Result<()>,
) -> Result<()> {
    persist(&next)?;
    *progress = next;
    Ok(())
}
pub struct Aborter<'a, B: Backend> {
    ownership: Ownership<'a, B>,
    publisher: Publisher<'a, B>,
}
impl<'a, B: Backend> Aborter<'a, B> {
    pub fn new(store: &'a Store<B>, clock: &'a dyn Clock, ttl: u64) -> Result<Self> {
        Ok(Self {
            ownership: Ownership::new(store, clock, ttl)?,
            publisher: Publisher::new(store, clock, ttl)?,
        })
    }
    fn resolve(
        &self,
        progress: &mut Progress,
        scratch: &Path,
        persist: &mut impl FnMut(&Progress) -> Result<()>,
    ) -> Result<Option<PushOutcome>> {
        let Some(push) = progress
            .publication
            .as_ref()
            .and_then(|p| p.publication.as_ref())
        else {
            return Ok(None);
        };
        if let Some(outcome) = &push.terminal {
            return Ok(Some(outcome.clone()));
        }
        let Some(intent) = push.publication.clone() else {
            return Ok(None);
        };
        if push.resolution.is_none() {
            let mut next = progress.clone();
            next.publication
                .as_mut()
                .unwrap()
                .publication
                .as_mut()
                .unwrap()
                .resolution = Some(LeaseAttempt {
                intent: self.publisher.prepare_lease(
                    intent.dataset.clone(),
                    intent.operation.operation_id.to_string(),
                )?,
                progress: LeaseProgress::Prepared,
            });
            save(progress, next, persist)?;
        }
        let mut attempt = progress
            .publication
            .as_ref()
            .unwrap()
            .publication
            .as_ref()
            .unwrap()
            .resolution
            .clone()
            .unwrap();
        let shared = RefCell::new(&mut *progress);
        let writer = RefCell::new(&mut *persist);
        let resolved = self.publisher.resolve_authorized(
            &intent,
            &attempt.intent,
            &mut attempt.progress,
            |phase| {
                let mut progress = shared.borrow_mut();
                let mut next = progress.clone();
                next.publication
                    .as_mut()
                    .unwrap()
                    .publication
                    .as_mut()
                    .unwrap()
                    .resolution
                    .as_mut()
                    .unwrap()
                    .progress = phase.clone();
                save(&mut progress, next, &mut **writer.borrow_mut())
            },
            scratch,
            |revision| {
                let mut progress = shared.borrow_mut();
                let mut next = progress.clone();
                let push = next
                    .publication
                    .as_mut()
                    .unwrap()
                    .publication
                    .as_mut()
                    .unwrap();
                if let Some(revision) = revision {
                    push.terminal = Some(PushOutcome {
                        revision,
                        no_op: false,
                        maintenance_error: None,
                    });
                } else {
                    push.uncommitted.push(intent.clone());
                    push.publication = None;
                    push.resolution = None;
                    push.lease = None;
                }
                save(&mut progress, next, &mut **writer.borrow_mut())
            },
        )?;
        if resolved.revision.is_some() {
            let mut result = progress
                .publication
                .as_ref()
                .unwrap()
                .publication
                .as_ref()
                .unwrap()
                .terminal
                .clone()
                .unwrap();
            result.maintenance_error = resolved.maintenance_error;
            return Ok(Some(result));
        }
        if let Some(error) = resolved.maintenance_error {
            return Err(error);
        }
        Ok(None)
    }
    /// Existing publication proof is resolved before checking stopped engine
    /// ownership. This method never invokes a finalizer or a new publication CAS.
    pub fn abort(
        &self,
        progress: &mut Progress,
        scratch: &Path,
        mut persist: impl FnMut(&Progress) -> Result<()>,
        mut check_stopped: impl FnMut() -> Result<()>,
    ) -> Result<Outcome> {
        if let Some(terminal) = &progress.terminal {
            return Ok(terminal.clone());
        }
        progress.validate()?;
        if let Some(outcome) = self.resolve(progress, scratch, &mut persist)? {
            let terminal = Outcome::Published { outcome };
            let mut next = progress.clone();
            next.terminal = Some(terminal.clone());
            save(progress, next, &mut persist)?;
            return Ok(terminal);
        }
        if progress.sealing {
            let mut owner = progress.owner.clone();
            if let Some(sealed) = self.ownership.adopt_stopped_seal(&mut owner)? {
                let terminal = Outcome::Aborted { sealed };
                let mut next = progress.clone();
                next.owner = owner;
                next.terminal = Some(terminal.clone());
                save(progress, next, &mut persist)?;
                return Ok(terminal);
            }
        }
        check_stopped()?;
        self.ownership.require_stopped_epoch(&progress.owner)?;
        if let Some(lease) = progress
            .publication
            .as_ref()
            .and_then(|p| p.publication.as_ref())
            .and_then(|p| p.lease.clone())
        {
            let mut phase = progress.lease.clone();
            self.publisher.abandon_lease_authorized(
                &lease.intent,
                &lease.progress,
                &mut phase,
                |phase| {
                    let mut next = progress.clone();
                    next.lease = phase.clone();
                    save(progress, next, &mut persist)
                },
            )?;
        }
        for index in 0..progress.cleanup.len() {
            let claim = progress.cleanup[index].claim.clone();
            let mut phase = progress.cleanup[index].progress.clone();
            self.ownership.abort_claim_authorized(
                &progress.owner.clone(),
                &claim,
                &mut phase,
                |phase| {
                    let mut next = progress.clone();
                    next.cleanup[index].progress = phase.clone();
                    save(progress, next, &mut persist)
                },
                &mut check_stopped,
            )?;
        }
        if !progress.sealing {
            let mut next = progress.clone();
            next.sealing = true;
            save(progress, next, &mut persist)?;
        }
        check_stopped()?;
        self.ownership.require_stopped_epoch(&progress.owner)?;
        let mut owner = progress.owner.clone();
        let sealed = self.ownership.seal_renewed(&mut owner)?;
        let terminal = Outcome::Aborted { sealed };
        let mut next = progress.clone();
        next.owner = owner;
        next.terminal = Some(terminal.clone());
        save(progress, next, &mut persist)?;
        Ok(terminal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        canonical::Sorter,
        capture::{adopt_group, stage_group},
        clock::{Clock, parse, timestamp},
        journal::Journal,
        normalize::TablePlan,
        ownership::{Reservation, ReservationProgress},
        push::{ExportAction, ExportGroup, ExportPlan, PushFinalizer, PushPolicy, PushProgress},
        store::InitOptions,
    };
    use grv_adapter_api::{Column, TableContract};
    use grv_storage::{
        ErrorKind, ListEntry, ListMode, LocalBackend, ObjectKey, ObjectMeta, ObjectPrefix,
        Validator, WriteEffect, model::*,
    };
    use grv_types::{Name, RunId};
    use serde_json::json;
    use std::{
        io::{Read, Write},
        os::unix::fs::PermissionsExt,
        sync::{
            Arc,
            atomic::{AtomicU64, AtomicUsize, Ordering},
        },
        time::Duration,
    };
    struct TestClock(AtomicU64);
    impl Clock for TestClock {
        fn now(&self) -> grv_types::Timestamp {
            timestamp(
                parse(&grv_types::Timestamp::new("2026-10-06T00:00:00Z").unwrap())
                    + chrono::Duration::seconds(self.0.load(Ordering::SeqCst) as i64),
            )
        }
        fn elapsed(&self) -> Duration {
            Duration::from_secs(self.0.load(Ordering::SeqCst))
        }
    }
    struct FaultBackend {
        inner: LocalBackend,
        mode: AtomicUsize,
        commits: AtomicUsize,
    }
    impl Backend for FaultBackend {
        fn get(&self, k: &ObjectKey, s: &mut dyn Write) -> grv_storage::Result<ObjectMeta> {
            self.inner.get(k, s)
        }
        fn head(&self, k: &ObjectKey) -> grv_storage::Result<ObjectMeta> {
            self.inner.head(k)
        }
        fn list(&self, k: &ObjectPrefix, m: ListMode) -> grv_storage::Result<Vec<ListEntry>> {
            self.inner.list(k, m)
        }
        fn delete(&self, k: &ObjectKey) -> grv_storage::Result<()> {
            self.inner.delete(k)
        }
        fn conditional_create(
            &self,
            k: &ObjectKey,
            s: &mut dyn Read,
        ) -> grv_storage::Result<Validator> {
            self.inner.conditional_create(k, s)
        }
        fn conditional_put(
            &self,
            k: &ObjectKey,
            v: &Validator,
            s: &mut dyn Read,
        ) -> grv_storage::Result<Validator> {
            let mut bytes = vec![];
            s.read_to_end(&mut bytes).unwrap();
            let commit = k.as_str().ends_with("/LATEST")
                && decode_record::<Latest>(&bytes)
                    .is_ok_and(|x| x.revision.get() > 0 && x.lease.is_none());
            if commit {
                let old: Latest = decode_record(&self.inner.read_bytes(k, 65536)?.0)?;
                let new: Latest = decode_record(&bytes)?;
                if new.revision > old.revision {
                    self.commits.fetch_add(1, Ordering::SeqCst);
                    let mode = self.mode.swap(0, Ordering::SeqCst);
                    if mode != 0 {
                        if mode == 2 {
                            self.inner.put_bytes(k, v, &bytes)?;
                        }
                        let mut e =
                            grv_storage::Error::new(ErrorKind::Io, "injected lost commit response");
                        e.effect = WriteEffect::MaybeApplied;
                        return Err(e);
                    }
                }
            }
            self.inner.put_bytes(k, v, &bytes)
        }
    }
    struct Fixture {
        root: tempfile::TempDir,
        store: Store<FaultBackend>,
        clock: TestClock,
        journal: Journal,
        owner: RunOwner,
        plan: ExportPlan,
    }
    fn fixture() -> Fixture {
        let root = tempfile::Builder::new()
            .prefix(".abort-")
            .tempdir_in(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .parent()
                    .unwrap()
                    .parent()
                    .unwrap(),
            )
            .unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let path = root.path().join("grv");
        std::fs::create_dir(&path).unwrap();
        let store = Store::initialize(
            FaultBackend {
                inner: LocalBackend::open(&path).unwrap(),
                mode: AtomicUsize::new(0),
                commits: AtomicUsize::new(0),
            },
            InitOptions::default(),
        )
        .unwrap()
        .0;
        let clock = TestClock(AtomicU64::new(0));
        let ownership = Ownership::new(&store, &clock, 60).unwrap();
        let mut owner = ownership
            .prepare_run(
                Name::new("product").unwrap(),
                RunId::new("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
                Counter::from(0),
                vec![],
                None,
            )
            .unwrap();
        ownership.commit_run(&owner).unwrap();
        ownership.confirm_holds(&mut owner).unwrap();
        let journal = Journal::create(root.path().join("state"), &[path]).unwrap();
        let table = TablePlan {
            table: Name::new("rows").unwrap(),
            contract: TableContract {
                columns: vec![Column {
                    name: "id".into(),
                    logical_type: json!("int64"),
                }],
                partition_keys: vec![],
                extensions: json!({}),
                column_ext: json!({}),
            },
            derivations: vec![],
            not_null: vec![],
        };
        let mut sorter = Sorter::new(table.contract.clone(), root.path()).unwrap();
        let batch = arrow_array::RecordBatch::try_new(
            crate::contract::arrow_schema(&table.contract).unwrap(),
            vec![Arc::new(arrow_array::Int64Array::from(vec![2, 1]))],
        )
        .unwrap();
        sorter.append(&batch).unwrap();
        let capture =
            adopt_group(&journal, &table, Partition::new(), sorter.finish().unwrap()).unwrap();
        let plan = ExportPlan {
            dataset: owner.dataset().clone(),
            run_id: owner.control().run_id.clone(),
            base_revision: Counter::from(0),
            capture_digest: grv_types::sha256(b"capture"),
            declaration_digest: grv_types::sha256(b"declaration"),
            policy: PushPolicy::All,
            groups: vec![ExportGroup {
                plan: table,
                capture,
                action: ExportAction::Write,
            }],
            omissions: vec![],
        };
        Fixture {
            root,
            store,
            clock,
            journal,
            owner,
            plan,
        }
    }
    fn source(f: &Fixture, push: PushProgress) -> build_publication::Progress {
        let mut progress =
            build_publication::Progress::planned(f.owner.clone(), f.plan.clone()).unwrap();
        progress.publication = Some(push);
        progress
    }
    fn claim(f: &Fixture) -> ClaimRecord {
        decode_record(
            &f.store
                .backend
                .read_bytes(
                    &ObjectKey::new("datasets/product/rows/.claim").unwrap(),
                    65536,
                )
                .unwrap()
                .0,
        )
        .unwrap()
    }
    fn allocations(f: &Fixture) -> usize {
        f.store
            .backend
            .list(
                &ObjectPrefix::new(format!(
                    "datasets/product/.runs/{}.allocations/",
                    f.owner.control().run_id
                ))
                .unwrap(),
                ListMode::Children,
            )
            .unwrap()
            .len()
    }
    fn reserved(f: &Fixture) -> Reservation {
        let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
        let group = &f.plan.groups[0];
        let intent = ownership
            .prepare_reservation(
                &f.owner,
                group.plan.layout(),
                group.capture.partition.clone(),
                &group.plan.contract,
            )
            .unwrap();
        ownership
            .reserve_authorized(
                &f.owner,
                &intent,
                &mut ReservationProgress::Prepared,
                |_| Ok(()),
            )
            .unwrap()
    }
    #[test]
    fn abort_partial_claim_phases_never_allocate_and_replay_journal_failures() {
        for stop in 0..4 {
            let f = fixture();
            let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
            let group = &f.plan.groups[0];
            let intent = ownership
                .prepare_reservation(
                    &f.owner,
                    group.plan.layout(),
                    group.capture.partition.clone(),
                    &group.plan.contract,
                )
                .unwrap();
            let mut recorded = ReservationProgress::Prepared;
            let mut phase = ReservationProgress::Prepared;
            assert!(
                ownership
                    .reserve_authorized(&f.owner, &intent, &mut phase, |next| {
                        let stopped = matches!(
                            (stop, next),
                            (0, ReservationProgress::Acquired { .. })
                                | (1, ReservationProgress::Allocate { .. })
                                | (2, ReservationProgress::Allocated { .. })
                                | (3, ReservationProgress::Complete { .. })
                        );
                        if stopped {
                            return Err(public_error(
                                ErrorCode::BackendFailure,
                                "injected journal failure",
                            ));
                        }
                        recorded = next.clone();
                        Ok(())
                    })
                    .is_err()
            );
            let before = allocations(&f);
            let mut push = PushProgress::planned(f.owner.clone(), f.plan.clone()).unwrap();
            push.groups[0] = GroupProgress::Reserving {
                intent,
                progress: recorded,
            };
            let mut abort = Progress::prepare(f.owner.clone(), Some(source(&f, push))).unwrap();
            let worker = Aborter::new(&f.store, &f.clock, 60).unwrap();
            let mut saved = abort.clone();
            let mut reject = true;
            assert!(
                worker
                    .abort(
                        &mut abort,
                        f.root.path(),
                        |next| {
                            if reject
                                && matches!(
                                    next.cleanup[0].progress,
                                    AbortClaimProgress::Release { .. }
                                )
                            {
                                reject = false;
                                return Err(public_error(
                                    ErrorCode::BackendFailure,
                                    "journal must precede release",
                                ));
                            }
                            saved = next.clone();
                            Ok(())
                        },
                        || Ok(())
                    )
                    .is_err()
            );
            assert!(claim(&f).released_at.is_none());
            abort = saved.clone();
            let outcome = worker
                .abort(
                    &mut abort,
                    f.root.path(),
                    |next| {
                        saved = next.clone();
                        Ok(())
                    },
                    || Ok(()),
                )
                .unwrap();
            let Outcome::Aborted { sealed } = outcome else {
                panic!("expected abort")
            };
            assert!(sealed.entries.is_empty());
            assert_eq!(claim(&f).outcome, Some(ClaimOutcome::Abandoned));
            assert_eq!(allocations(&f), before);
            assert!(matches!(
                worker
                    .abort(
                        &mut abort,
                        f.root.path(),
                        |_| panic!("terminal rewrite"),
                        || panic!("terminal engine access")
                    )
                    .unwrap(),
                Outcome::Aborted { .. }
            ));
        }
    }
    #[test]
    fn lost_release_ack_replays_fixed_mutation_before_sealing() {
        let f = fixture();
        let mut push = PushProgress::planned(f.owner.clone(), f.plan.clone()).unwrap();
        push.groups[0] = GroupProgress::Reserved {
            reservation: reserved(&f),
        };
        let mut progress = Progress::prepare(f.owner.clone(), Some(source(&f, push))).unwrap();
        let mut durable = progress.clone();
        let worker = Aborter::new(&f.store, &f.clock, 60).unwrap();
        assert!(
            worker
                .abort(
                    &mut progress,
                    f.root.path(),
                    |next| {
                        if matches!(
                            next.cleanup[0].progress,
                            AbortClaimProgress::Released { .. }
                        ) {
                            return Err(public_error(
                                ErrorCode::BackendFailure,
                                "lost release acknowledgement",
                            ));
                        }
                        durable = next.clone();
                        Ok(())
                    },
                    || Ok(())
                )
                .is_err()
        );
        assert!(matches!(
            durable.cleanup[0].progress,
            AbortClaimProgress::Release { .. }
        ));
        let released = claim(&f);
        assert_eq!(released.outcome, Some(ClaimOutcome::Abandoned));
        progress = durable;
        assert!(matches!(
            worker
                .abort(&mut progress, f.root.path(), |_| Ok(()), || Ok(()))
                .unwrap(),
            Outcome::Aborted { .. }
        ));
        assert_eq!(
            claim(&f),
            released,
            "release replay must retain its original mutation identity"
        );
        assert_eq!(allocations(&f), 1);
    }
    #[test]
    fn lost_seal_terminal_ack_adopts_exact_seal_without_engine_access() {
        let f = fixture();
        let mut progress = Progress::prepare(f.owner.clone(), None).unwrap();
        let mut durable = progress.clone();
        let worker = Aborter::new(&f.store, &f.clock, 60).unwrap();
        assert!(
            worker
                .abort(
                    &mut progress,
                    f.root.path(),
                    |next| {
                        if next.terminal.is_some() {
                            return Err(public_error(
                                ErrorCode::BackendFailure,
                                "lost sealed terminal acknowledgement",
                            ));
                        }
                        durable = next.clone();
                        Ok(())
                    },
                    || Ok(())
                )
                .is_err()
        );
        assert!(durable.sealing && durable.terminal.is_none());
        let key = ObjectKey::new(format!(
            "datasets/product/.runs/{}.json",
            f.owner.control().run_id
        ))
        .unwrap();
        let before = f.store.backend.read_bytes(&key, 65536).unwrap().0;
        // A sealed control record also authorizes the normal immutable-file
        // materialization if that separate write was interrupted or lost.
        f.store.backend.delete(&key).unwrap();
        f.clock.0.store(100, Ordering::SeqCst);
        progress = durable;
        let Outcome::Aborted { sealed } = worker
            .abort(
                &mut progress,
                f.root.path(),
                |_| Ok(()),
                || panic!("post-seal adoption must not reopen engine"),
            )
            .unwrap()
        else {
            panic!("expected adopted aborted outcome")
        };
        assert!(sealed.entries.is_empty());
        assert_eq!(f.store.backend.read_bytes(&key, 65536).unwrap().0, before);
        assert_eq!(progress.owner.control().phase, RunPhase::Sealed);
    }
    #[test]
    fn sealing_intent_cannot_skip_stopped_claim_cleanup() {
        let f = fixture();
        let mut push = PushProgress::planned(f.owner.clone(), f.plan.clone()).unwrap();
        push.groups[0] = GroupProgress::Reserved {
            reservation: reserved(&f),
        };
        let mut progress = Progress::prepare(f.owner.clone(), Some(source(&f, push))).unwrap();
        progress.sealing = true;
        assert_eq!(
            Aborter::new(&f.store, &f.clock, 60)
                .unwrap()
                .abort(
                    &mut progress,
                    f.root.path(),
                    |_| panic!("invalid progress persisted"),
                    || panic!("invalid progress engine access")
                )
                .err()
                .unwrap()
                .code,
            ErrorCode::IntegrityFailure
        );
        assert!(claim(&f).released_at.is_none());
    }
    #[test]
    fn finalized_carried_release_proof_preserves_entry_and_new_holder() {
        let f = fixture();
        let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
        let mut reservation = reserved(&f);
        let stale = reservation.clone();
        let group = &f.plan.groups[0];
        let staged = stage_group(&f.journal, &group.plan, &group.capture).unwrap();
        ownership
            .write_group(
                &f.owner,
                &mut reservation,
                &group.plan.contract,
                &staged,
                None,
            )
            .unwrap();
        ownership
            .release(&mut reservation, ClaimOutcome::Finalized)
            .unwrap();
        let mut other = ownership
            .prepare_run(
                Name::new("product").unwrap(),
                RunId::new("01ARZ3NDEKTSV4RRFFQ69G5FAW").unwrap(),
                Counter::from(0),
                vec![],
                None,
            )
            .unwrap();
        ownership.commit_run(&other).unwrap();
        ownership.confirm_holds(&mut other).unwrap();
        let intent = ownership
            .prepare_reservation(
                &other,
                group.plan.layout(),
                Partition::new(),
                &group.plan.contract,
            )
            .unwrap();
        let newer = ownership
            .reserve_authorized(&other, &intent, &mut ReservationProgress::Prepared, |_| {
                Ok(())
            })
            .unwrap();
        let before = claim(&f);
        assert_eq!(before.token, newer.allocation().claim_token);
        let mut push = PushProgress::planned(f.owner.clone(), f.plan.clone()).unwrap();
        push.groups[0] = GroupProgress::Reserved { reservation: stale };
        let mut abort = Progress::prepare(f.owner.clone(), Some(source(&f, push))).unwrap();
        let Outcome::Aborted { sealed } = Aborter::new(&f.store, &f.clock, 60)
            .unwrap()
            .abort(&mut abort, f.root.path(), |_| Ok(()), || Ok(()))
            .unwrap()
        else {
            panic!("abort")
        };
        assert_eq!(sealed.entries.len(), 1);
        assert_eq!(sealed.entries[0].version, Counter::from(1));
        assert_eq!(claim(&f), before);
    }
    #[test]
    fn committed_manifest_with_lost_or_corrupt_data_never_becomes_abandoned() {
        for corrupt in [false, true] {
            let f = fixture();
            let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
            let mut reservation = reserved(&f);
            let group = &f.plan.groups[0];
            let staged = stage_group(&f.journal, &group.plan, &group.capture).unwrap();
            let manifest = ownership
                .write_group(
                    &f.owner,
                    &mut reservation,
                    &group.plan.contract,
                    &staged,
                    None,
                )
                .unwrap();
            let data = ObjectKey::new(format!(
                "datasets/product/rows/version=1/{}",
                manifest.data_files[0].name
            ))
            .unwrap();
            if corrupt {
                let meta = f.store.backend.head(&data).unwrap();
                f.store
                    .backend
                    .put_bytes(&data, &meta.validator, b"corrupt committed parquet")
                    .unwrap();
            } else {
                f.store.backend.delete(&data).unwrap();
            }
            let mut push = PushProgress::planned(f.owner.clone(), f.plan.clone()).unwrap();
            push.groups[0] = GroupProgress::Reserved { reservation };
            let mut abort = Progress::prepare(f.owner.clone(), Some(source(&f, push))).unwrap();
            assert_eq!(
                Aborter::new(&f.store, &f.clock, 60)
                    .unwrap()
                    .abort(&mut abort, f.root.path(), |_| Ok(()), || Ok(()))
                    .err()
                    .unwrap()
                    .code,
                ErrorCode::IntegrityFailure
            );
            assert!(claim(&f).released_at.is_none());
            assert!(abort.terminal.is_none());
        }
    }
    #[test]
    fn abort_resolves_publication_without_new_commit_or_engine_for_known_result() {
        for mode in [0, 1, 2] {
            let f = fixture();
            f.store.backend.mode.store(mode, Ordering::SeqCst);
            let mut push = PushProgress::planned(f.owner.clone(), f.plan.clone()).unwrap();
            let result = PushFinalizer::new(&f.store, &f.clock, 60)
                .unwrap()
                .finalize(&mut push, &f.journal, f.root.path(), |_| Ok(()));
            if mode == 1 {
                assert!(result.is_err());
            } else {
                assert!(result.is_ok());
            }
            // Simulate a lost consumer terminal write after backend commit.
            push.terminal = None;
            let before = f.store.backend.commits.load(Ordering::SeqCst);
            let mut abort = Progress::prepare(f.owner.clone(), Some(source(&f, push))).unwrap();
            let outcome = Aborter::new(&f.store, &f.clock, 60)
                .unwrap()
                .abort(
                    &mut abort,
                    f.root.path(),
                    |_| Ok(()),
                    || {
                        assert_eq!(mode, 1, "known commit must resolve before engine access");
                        Ok(())
                    },
                )
                .unwrap();
            assert_eq!(f.store.backend.commits.load(Ordering::SeqCst), before);
            match outcome {
                Outcome::Published { outcome } => {
                    assert_ne!(mode, 1);
                    assert_eq!(outcome.revision, Counter::from(1));
                }
                Outcome::Aborted { sealed } => {
                    assert_eq!(mode, 1);
                    assert_eq!(sealed.entries.len(), 1);
                }
            }
        }
    }
    #[test]
    fn changed_owner_creation_or_metadata_cannot_release_claim() {
        for metadata in [false, true] {
            let f = fixture();
            let mut push = PushProgress::planned(f.owner.clone(), f.plan.clone()).unwrap();
            push.groups[0] = GroupProgress::Reserved {
                reservation: reserved(&f),
            };
            let mut abort = Progress::prepare(f.owner.clone(), Some(source(&f, push))).unwrap();
            let key = ObjectKey::new(format!(
                "datasets/product/.runs/{}.control.json",
                f.owner.control().run_id
            ))
            .unwrap();
            let (bytes, meta) = f.store.backend.read_bytes(&key, 65536).unwrap();
            let mut changed: RunControl = decode_record(&bytes).unwrap();
            if metadata {
                changed.metadata = Some(std::collections::BTreeMap::from([(
                    "grv_cli".into(),
                    json!({"attempt_id":"different"}),
                )]));
            } else {
                changed.created_at = grv_types::Timestamp::new("2026-10-05T00:00:00Z").unwrap();
            }
            changed.mutation_id = grv_types::Uuid::v4();
            f.store
                .backend
                .put_bytes(&key, &meta.validator, &encode_record(&changed).unwrap())
                .unwrap();
            assert_eq!(
                Aborter::new(&f.store, &f.clock, 60)
                    .unwrap()
                    .abort(&mut abort, f.root.path(), |_| Ok(()), || Ok(()))
                    .err()
                    .unwrap()
                    .code,
                ErrorCode::OwnershipLost
            );
            assert!(claim(&f).released_at.is_none());
            assert!(abort.terminal.is_none());
        }
    }
    #[test]
    fn recovered_owner_epoch_cannot_release_or_seal() {
        let f = fixture();
        let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
        let reservation = reserved(&f);
        let mut push = PushProgress::planned(f.owner.clone(), f.plan.clone()).unwrap();
        push.groups[0] = GroupProgress::Reserved { reservation };
        let mut abort = Progress::prepare(f.owner.clone(), Some(source(&f, push))).unwrap();
        f.clock.0.store(100, Ordering::SeqCst);
        let recovery = ownership
            .prepare_run_recovery(f.owner.dataset().clone(), f.owner.control().run_id.clone())
            .unwrap();
        ownership.recover_authorized(&recovery).unwrap();
        assert_eq!(
            Aborter::new(&f.store, &f.clock, 60)
                .unwrap()
                .abort(&mut abort, f.root.path(), |_| Ok(()), || Ok(()))
                .err()
                .unwrap()
                .code,
            ErrorCode::OwnershipLost
        );
        assert!(claim(&f).released_at.is_none());
    }
}
