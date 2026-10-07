//! Replay of irrevocably committed retention operations. Description objects
//! alone are never authorization, and replay never mutates a lease or deletes
//! version data. Only the dataset lease holder may clear LATEST.pending.
use crate::{
    clock::Clock,
    store::{Result, Store, backend_error, public_error},
};
use grv_storage::{Backend, ErrorKind, ObjectKey, Validator, WriteEffect, model::*};
use grv_types::{ErrorCode, Name};
use serde::{Serialize, de::DeserializeOwned};
const RECORD_LIMIT: usize = 64 * 1024 * 1024;
fn invalid(message: &str) -> grv_types::PublicError {
    public_error(ErrorCode::IntegrityFailure, message)
}
fn key(dataset: &Name, relative: &str) -> ObjectKey {
    ObjectKey::new(format!("datasets/{dataset}/{relative}")).expect("validated retention path")
}
fn read<B: Backend, T: DeserializeOwned + Validate>(
    store: &Store<B>,
    path: &ObjectKey,
) -> Result<(T, Validator)> {
    let (bytes, meta) = store
        .backend
        .read_bytes(path, RECORD_LIMIT)
        .map_err(backend_error)?;
    Ok((
        decode_record(&bytes).map_err(backend_error)?,
        meta.validator,
    ))
}
/// An in-memory capability from a durable observation of LATEST.pending and
/// its immutable description. It remains valid after a helper loses its lease.
#[derive(Clone)]
pub struct DurableOperationProof {
    dataset: Name,
    operation: OperationRecord,
}
impl DurableOperationProof {
    /// Core callers must have durably read both records. The public observer
    /// below performs those reads; publishers may use their existing owned read.
    pub(crate) fn from_pending(
        dataset: &Name,
        latest: &Latest,
        operation: &OperationRecord,
    ) -> Result<Self> {
        latest.validate().map_err(backend_error)?;
        operation.validate().map_err(backend_error)?;
        let path = format!(".states/operations/{}.json", operation.operation_id);
        if operation.dataset != *dataset || latest.pending.as_deref() != Some(path.as_str()) {
            return Err(invalid(
                "operation is not the observed committed pending decision",
            ));
        }
        if matches!(operation.body, OperationPayload::Publish(_)) {
            return Err(invalid(
                "publication audit descriptions cannot authorize pending retention effects",
            ));
        }
        Ok(Self {
            dataset: dataset.clone(),
            operation: operation.clone(),
        })
    }
    pub fn operation(&self) -> &OperationRecord {
        &self.operation
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplaySummary {
    pub durable_markers: Vec<ObjectKey>,
}
pub fn observe_pending<B: Backend>(
    store: &Store<B>,
    dataset: &Name,
    operation: &OperationRecord,
) -> Result<DurableOperationProof> {
    let (latest, _): (Latest, _) = read(store, &key(dataset, ".states/LATEST"))?;
    let proof = DurableOperationProof::from_pending(dataset, &latest, operation)?;
    let (recorded, _): (OperationRecord, _) = read(
        store,
        &key(
            dataset,
            &format!(".states/operations/{}.json", operation.operation_id),
        ),
    )?;
    if recorded != *operation {
        return Err(invalid(
            "pending operation differs from its immutable description",
        ));
    }
    Ok(proof)
}
pub fn replay_committed<B: Backend>(
    store: &Store<B>,
    dataset: &Name,
    operation: &OperationRecord,
    clock: &dyn Clock,
) -> Result<ReplaySummary> {
    let proof = observe_pending(store, dataset, operation)?;
    replay_proven(store, &proof, clock)
}
/// This accepts only the capability produced by a committed pending observation;
/// an orphan description cannot construct it. No current lease is required.
pub fn replay_proven<B: Backend>(
    store: &Store<B>,
    proof: &DurableOperationProof,
    clock: &dyn Clock,
) -> Result<ReplaySummary> {
    let operation = &proof.operation;
    let dataset = &proof.dataset;
    let mut markers = vec![];
    match &operation.body {
        OperationPayload::Publish(_) => {
            return Err(invalid(
                "publication descriptions are not pending retention decisions",
            ));
        }
        OperationPayload::PruneIntent(_) => {}
        OperationPayload::Pin(payload) => {
            let path = pin_path(store, dataset, &payload.scope, &payload.pin_id, false)?;
            let marker = PinRecord {
                pin_id: payload.pin_id.clone(),
                operation_id: operation.operation_id.clone(),
                scope: payload.scope.clone(),
                created_at: clock.now(),
                created_by: operation.created_by.clone(),
                reason: payload.reason.clone(),
            };
            materialize(store, &path, &marker, |existing: &PinRecord| {
                existing.pin_id == marker.pin_id && existing.scope == marker.scope
            })?;
            markers.push(path);
        }
        OperationPayload::Unpin(payload) => {
            let path = pin_path(store, dataset, &payload.scope, &payload.pin_id, true)?;
            let marker = PinReleaseMarker {
                pin_id: payload.pin_id.clone(),
                operation_id: operation.operation_id.clone(),
                released_at: clock.now(),
            };
            materialize(store, &path, &marker, |existing: &PinReleaseMarker| {
                existing.pin_id == marker.pin_id
            })?;
            markers.push(path);
        }
        OperationPayload::ReleaseHold(payload) => {
            for release in &payload.releases {
                let path = key(
                    dataset,
                    &format!(
                        ".states/released-holds/{}/revision={}/{}.json",
                        release.consumer_dataset, release.revision, release.retention_id
                    ),
                );
                let marker = HoldReleaseMarker {
                    retention_id: release.retention_id.clone(),
                    operation_id: operation.operation_id.clone(),
                    released_at: clock.now(),
                };
                materialize(store, &path, &marker, |existing: &HoldReleaseMarker| {
                    existing.retention_id == marker.retention_id
                })?;
                markers.push(path);
            }
        }
        OperationPayload::Retire(payload) => {
            let path = key(dataset, ".retired");
            let marker = RetiredMarker {
                operation_id: operation.operation_id.clone(),
                retired_at: clock.now(),
                reason: payload.reason.clone(),
            };
            materialize_presence(store, &path, &marker)?;
            markers.push(path);
        }
        OperationPayload::Prune(payload) => {
            for target in &payload.targets {
                let path = key(
                    dataset,
                    &format!(
                        "{}/version={}/.pruned",
                        partition_path(store, dataset, &target.table, &target.partition)?,
                        target.version
                    ),
                );
                let marker = PrunedMarker {
                    operation_id: operation.operation_id.clone(),
                    pruned_by: operation.created_by.clone(),
                    pruned_at: clock.now(),
                    table: target.table.clone(),
                    partition: target.partition.clone(),
                    version: target.version,
                };
                materialize_presence(store, &path, &marker)?;
                markers.push(path);
            }
        }
    }
    Ok(ReplaySummary {
        durable_markers: markers,
    })
}
fn partition_path<B: Backend>(
    store: &Store<B>,
    dataset: &Name,
    table: &Name,
    partition: &Partition,
) -> Result<String> {
    let (layout, _): (TableLayout, _) =
        read(store, &key(dataset, &format!("{table}/.layout.json")))?;
    if layout.table != *table {
        return Err(invalid("retention table layout differs from its path"));
    }
    let partition = layout.partition_path(partition).map_err(backend_error)?;
    Ok(if partition.is_empty() {
        table.to_string()
    } else {
        format!("{table}/{partition}")
    })
}
pub fn pin_path<B: Backend>(
    store: &Store<B>,
    dataset: &Name,
    scope: &PinScope,
    pin_id: &grv_types::Uuid,
    released: bool,
) -> Result<ObjectKey> {
    scope.validate().map_err(backend_error)?;
    let base = match scope {
        PinScope::Revision(scope) => format!(".states/revisions/revision={}", scope.revision),
        PinScope::Table(scope) => scope.table.to_string(),
        PinScope::Partition(scope) => {
            partition_path(store, dataset, &scope.table, &scope.partition)?
        }
        PinScope::Version(scope) => format!(
            "{}/version={}",
            partition_path(store, dataset, &scope.table, &scope.partition)?,
            scope.version
        ),
    };
    Ok(key(
        dataset,
        &format!(
            "{base}/.pins/{pin_id}{}.json",
            if released { ".released" } else { "" }
        ),
    ))
}
fn materialize<B: Backend, T: Serialize + DeserializeOwned + Validate>(
    store: &Store<B>,
    path: &ObjectKey,
    marker: &T,
    equivalent: impl Fn(&T) -> bool,
) -> Result<()> {
    let bytes = encode_record(marker).map_err(backend_error)?;
    match store.backend.create_bytes(path, &bytes) {
        Ok(_) => Ok(()),
        Err(error)
            if error.kind == ErrorKind::PreconditionFailed
                || error.effect == WriteEffect::MaybeApplied =>
        {
            let (existing, _): (T, _) = read(store, path)?;
            if equivalent(&existing) {
                Ok(())
            } else {
                Err(invalid(
                    "existing retention marker records a different fact",
                ))
            }
        }
        Err(error) => Err(backend_error(error)),
    }
}
fn materialize_presence<B: Backend, T: Serialize + Validate>(
    store: &Store<B>,
    path: &ObjectKey,
    marker: &T,
) -> Result<()> {
    let bytes = encode_record(marker).map_err(backend_error)?;
    match store.backend.create_bytes(path, &bytes) {
        Ok(_) => Ok(()),
        Err(error)
            if error.kind == ErrorKind::PreconditionFailed
                || error.effect == WriteEffect::MaybeApplied =>
        {
            // §8 explicitly accepts every existing pruning/retirement tombstone.
            // Presence is established by durable head, never by a listing.
            store.backend.head(path).map_err(backend_error)?;
            Ok(())
        }
        Err(error) => Err(backend_error(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grv_storage::{FaultInjector, FaultPoint, LocalBackend};
    use grv_types::{RunId, Timestamp, Uuid};
    use std::{
        collections::BTreeMap,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };
    struct FixedClock;
    impl Clock for FixedClock {
        fn now(&self) -> Timestamp {
            Timestamp::new("2026-10-06T00:00:00Z").unwrap()
        }
        fn elapsed(&self) -> Duration {
            Duration::ZERO
        }
    }
    fn name(v: &str) -> Name {
        Name::new(v).unwrap()
    }
    fn id(n: u32) -> RunId {
        RunId::new(format!("01M3KQA080R6Y8C2D9F0G{n:05}")).unwrap()
    }
    fn make_operation(body: OperationPayload) -> OperationRecord {
        OperationRecord {
            operation_id: id(1),
            dataset: name("data"),
            created_at: Timestamp::new("2026-10-05T00:00:00Z").unwrap(),
            created_by: "retention-test/1".into(),
            body,
        }
    }
    fn make_store(path: &std::path::Path) -> Store<LocalBackend> {
        Store {
            backend: LocalBackend::open(path).unwrap(),
            parameters: StoreParameters::default(),
        }
    }
    fn layout(store: &Store<impl Backend>) {
        let layout = TableLayout {
            table: name("events"),
            partition_keys: vec![name("year"), name("region")],
            extensions: None,
        };
        store
            .backend
            .create_bytes(
                &key(&name("data"), "events/.layout.json"),
                &encode_record(&layout).unwrap(),
            )
            .unwrap();
    }
    fn commit(store: &Store<impl Backend>, operation: &OperationRecord) {
        store
            .backend
            .create_bytes(
                &key(
                    &operation.dataset,
                    &format!(".states/operations/{}.json", operation.operation_id),
                ),
                &encode_record(operation).unwrap(),
            )
            .unwrap();
        let mut latest = Latest::empty();
        latest.pending = Some(format!(
            ".states/operations/{}.json",
            operation.operation_id
        ));
        latest.lease = Some(Lease {
            holder: "test".into(),
            token: LeaseToken::generate(),
            claimed_at: FixedClock.now(),
            expires_at: Timestamp::new("2026-10-06T00:01:00Z").unwrap(),
        });
        store
            .backend
            .create_bytes(
                &key(&operation.dataset, ".states/LATEST"),
                &encode_record(&latest).unwrap(),
            )
            .unwrap();
    }
    fn partition() -> Partition {
        BTreeMap::from([(name("region"), name("eu")), (name("year"), name("2025"))])
    }
    #[test]
    fn all_committed_retention_kinds_replay_exact_paths_without_lease_or_data_mutation() {
        let pin = Uuid::v4();
        let hold = Uuid::v4();
        let cases = vec![
            OperationPayload::Pin(PinPayload {
                pin_id: pin.clone(),
                scope: PinScope::Partition(PartitionScope {
                    table: name("events"),
                    partition: partition(),
                }),
                reason: Some("protect".into()),
            }),
            OperationPayload::Unpin(PinPayload {
                pin_id: pin.clone(),
                scope: PinScope::Revision(RevisionScope {
                    revision: Counter::from(1),
                }),
                reason: None,
            }),
            OperationPayload::ReleaseHold(ReleaseHoldPayload {
                releases: vec![HoldRelease {
                    consumer_dataset: name("consumer"),
                    revision: Counter::from(2),
                    retention_id: hold.clone(),
                }],
            }),
            OperationPayload::Retire(RetirePayload {
                reason: Some("obsolete".into()),
            }),
            OperationPayload::Prune(PrunePayload {
                targets: vec![VersionTarget {
                    table: name("events"),
                    partition: partition(),
                    version: Counter::from(3),
                }],
            }),
        ];
        for body in cases {
            let root = tempfile::tempdir().unwrap();
            let store = make_store(root.path());
            layout(&store);
            let operation = make_operation(body);
            commit(&store, &operation);
            let latest_before = store
                .backend
                .read_bytes(&key(&name("data"), ".states/LATEST"), RECORD_LIMIT)
                .unwrap();
            let expected = match &operation.body {
                OperationPayload::Pin(_) => format!("events/year=2025/region=eu/.pins/{pin}.json"),
                OperationPayload::Unpin(_) => {
                    format!(".states/revisions/revision=1/.pins/{pin}.released.json")
                }
                OperationPayload::ReleaseHold(_) => {
                    format!(".states/released-holds/consumer/revision=2/{hold}.json")
                }
                OperationPayload::Retire(_) => ".retired".into(),
                _ => "events/year=2025/region=eu/version=3/.pruned".into(),
            };
            let replay = replay_committed(&store, &name("data"), &operation, &FixedClock).unwrap();
            assert_eq!(replay.durable_markers, vec![key(&name("data"), &expected)]);
            let first = store
                .backend
                .read_bytes(&replay.durable_markers[0], RECORD_LIMIT)
                .unwrap();
            assert!(
                std::str::from_utf8(&first.0)
                    .unwrap()
                    .contains(FixedClock.now().as_str())
            );
            assert!(
                !std::str::from_utf8(&first.0)
                    .unwrap()
                    .contains(operation.created_at.as_str())
            );
            replay_committed(&store, &name("data"), &operation, &FixedClock).unwrap();
            assert_eq!(
                first,
                store
                    .backend
                    .read_bytes(&replay.durable_markers[0], RECORD_LIMIT)
                    .unwrap()
            );
            assert_eq!(
                latest_before,
                store
                    .backend
                    .read_bytes(&key(&name("data"), ".states/LATEST"), RECORD_LIMIT)
                    .unwrap()
            );
        }
    }
    #[test]
    fn orphan_descriptions_modified_payloads_and_publish_pending_authorize_no_markers() {
        let root = tempfile::tempdir().unwrap();
        let store = make_store(root.path());
        let pin = Uuid::v4();
        let mut operation = make_operation(OperationPayload::Pin(PinPayload {
            pin_id: pin.clone(),
            scope: PinScope::Table(TableScope {
                table: name("events"),
            }),
            reason: None,
        }));
        store
            .backend
            .create_bytes(
                &key(
                    &name("data"),
                    &format!(".states/operations/{}.json", operation.operation_id),
                ),
                &encode_record(&operation).unwrap(),
            )
            .unwrap();
        let latest = Latest::empty();
        let path = key(&name("data"), ".states/LATEST");
        let validator = store
            .backend
            .create_bytes(&path, &encode_record(&latest).unwrap())
            .unwrap();
        assert_eq!(
            replay_committed(&store, &name("data"), &operation, &FixedClock)
                .unwrap_err()
                .code,
            ErrorCode::IntegrityFailure
        );
        assert!(!root.path().join("datasets/data/events").exists());
        let mut pending = latest;
        pending.pending = Some(format!(
            ".states/operations/{}.json",
            operation.operation_id
        ));
        pending.mutation_id = Uuid::v4();
        store
            .backend
            .put_bytes(&path, &validator, &encode_record(&pending).unwrap())
            .unwrap();
        if let OperationPayload::Pin(p) = &mut operation.body {
            p.scope = PinScope::Table(TableScope {
                table: name("other"),
            });
        }
        assert_eq!(
            replay_committed(&store, &name("data"), &operation, &FixedClock)
                .unwrap_err()
                .code,
            ErrorCode::IntegrityFailure
        );
        assert!(!root.path().join("datasets/data/other").exists());
        let root = tempfile::tempdir().unwrap();
        let store = make_store(root.path());
        let operation = make_operation(OperationPayload::Publish(PublishPayload {
            revision: Counter::from(1),
            previous_revision: Counter::from(0),
            change_set: ChangeSet::default(),
        }));
        commit(&store, &operation);
        assert_eq!(
            replay_committed(&store, &name("data"), &operation, &FixedClock)
                .unwrap_err()
                .code,
            ErrorCode::IntegrityFailure
        );
    }
    #[test]
    fn existing_pin_and_release_facts_ignore_operation_timestamps_actor_and_reason() {
        for release in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let store = make_store(root.path());
            let pin_id = Uuid::v4();
            let payload = PinPayload {
                pin_id: pin_id.clone(),
                scope: PinScope::Table(TableScope {
                    table: name("events"),
                }),
                reason: Some("new reason".into()),
            };
            let operation = make_operation(if release {
                OperationPayload::Unpin(payload.clone())
            } else {
                OperationPayload::Pin(payload.clone())
            });
            commit(&store, &operation);
            let path = pin_path(&store, &name("data"), &payload.scope, &pin_id, release).unwrap();
            let bytes = if release {
                encode_record(&PinReleaseMarker {
                    pin_id: pin_id.clone(),
                    operation_id: id(9),
                    released_at: Timestamp::new("2020-01-01T00:00:00Z").unwrap(),
                })
                .unwrap()
            } else {
                encode_record(&PinRecord {
                    pin_id: pin_id.clone(),
                    operation_id: id(9),
                    scope: payload.scope,
                    created_at: Timestamp::new("2020-01-01T00:00:00Z").unwrap(),
                    created_by: "another-engine".into(),
                    reason: Some("old reason".into()),
                })
                .unwrap()
            };
            store.backend.create_bytes(&path, &bytes).unwrap();
            replay_committed(&store, &name("data"), &operation, &FixedClock).unwrap();
            assert_eq!(
                store.backend.read_bytes(&path, RECORD_LIMIT).unwrap().0,
                bytes
            );
        }
    }
    #[test]
    fn mismatched_pin_fact_is_rejected_but_any_existing_retirement_or_pruning_is_durable() {
        let root = tempfile::tempdir().unwrap();
        let store = make_store(root.path());
        let pin_id = Uuid::v4();
        let payload = PinPayload {
            pin_id: pin_id.clone(),
            scope: PinScope::Table(TableScope {
                table: name("events"),
            }),
            reason: None,
        };
        let operation = make_operation(OperationPayload::Pin(payload.clone()));
        commit(&store, &operation);
        let path = pin_path(&store, &name("data"), &payload.scope, &pin_id, false).unwrap();
        let wrong = PinRecord {
            pin_id: Uuid::v4(),
            operation_id: id(9),
            scope: payload.scope,
            created_at: FixedClock.now(),
            created_by: "test".into(),
            reason: None,
        };
        store
            .backend
            .create_bytes(&path, &encode_record(&wrong).unwrap())
            .unwrap();
        assert_eq!(
            replay_committed(&store, &name("data"), &operation, &FixedClock)
                .unwrap_err()
                .code,
            ErrorCode::IntegrityFailure
        );
        for retire in [true, false] {
            let root = tempfile::tempdir().unwrap();
            let store = make_store(root.path());
            layout(&store);
            let body = if retire {
                OperationPayload::Retire(RetirePayload { reason: None })
            } else {
                OperationPayload::Prune(PrunePayload {
                    targets: vec![VersionTarget {
                        table: name("events"),
                        partition: partition(),
                        version: Counter::from(1),
                    }],
                })
            };
            let operation = make_operation(body);
            commit(&store, &operation);
            let path = key(
                &name("data"),
                if retire {
                    ".retired"
                } else {
                    "events/year=2025/region=eu/version=1/.pruned"
                },
            );
            store
                .backend
                .create_bytes(&path, b"permanent older tombstone")
                .unwrap();
            replay_committed(&store, &name("data"), &operation, &FixedClock).unwrap();
            assert_eq!(
                store.backend.read_bytes(&path, RECORD_LIMIT).unwrap().0,
                b"permanent older tombstone"
            );
        }
    }
    struct FailOnce {
        point: FaultPoint,
        used: AtomicBool,
    }
    impl FaultInjector for FailOnce {
        fn check(&self, point: FaultPoint) -> std::io::Result<()> {
            if point == self.point && !self.used.swap(true, Ordering::SeqCst) {
                Err(std::io::Error::other("injected marker durability failure"))
            } else {
                Ok(())
            }
        }
    }
    #[test]
    fn lost_marker_ack_is_adopted_only_by_durable_read_and_failed_barrier_stays_pending() {
        for point in [FaultPoint::AfterInstall, FaultPoint::BeforeReadSync] {
            let root = tempfile::tempdir().unwrap();
            let healthy = make_store(root.path());
            let pin_id = Uuid::v4();
            let payload = PinPayload {
                pin_id: pin_id.clone(),
                scope: PinScope::Table(TableScope {
                    table: name("events"),
                }),
                reason: None,
            };
            let operation = make_operation(OperationPayload::Pin(payload.clone()));
            commit(&healthy, &operation);
            let proof = observe_pending(&healthy, &name("data"), &operation).unwrap();
            let marker_path =
                pin_path(&healthy, &name("data"), &payload.scope, &pin_id, false).unwrap();
            if point == FaultPoint::BeforeReadSync {
                let marker = PinRecord {
                    pin_id,
                    operation_id: id(1),
                    scope: payload.scope,
                    created_at: FixedClock.now(),
                    created_by: "test".into(),
                    reason: None,
                };
                healthy
                    .backend
                    .create_bytes(&marker_path, &encode_record(&marker).unwrap())
                    .unwrap();
            }
            let faulty = Store {
                backend: healthy.backend.with_faults(Arc::new(FailOnce {
                    point,
                    used: AtomicBool::new(false),
                })),
                parameters: healthy.parameters,
            };
            let result = replay_proven(&faulty, &proof, &FixedClock);
            if point == FaultPoint::AfterInstall {
                assert!(result.is_ok());
            } else {
                assert_eq!(result.unwrap_err().code, ErrorCode::BackendFailure);
            }
            let (latest, _): (Latest, _) =
                read(&faulty, &key(&name("data"), ".states/LATEST")).unwrap();
            assert!(latest.pending.is_some());
        }
    }
    #[test]
    fn committed_proof_survives_lost_lease_and_prune_intent_never_authorizes_effects() {
        let root = tempfile::tempdir().unwrap();
        let store = make_store(root.path());
        let payload = PinPayload {
            pin_id: Uuid::v4(),
            scope: PinScope::Table(TableScope {
                table: name("events"),
            }),
            reason: None,
        };
        let operation = make_operation(OperationPayload::Pin(payload));
        commit(&store, &operation);
        let proof = observe_pending(&store, &name("data"), &operation).unwrap();
        let path = key(&name("data"), ".states/LATEST");
        let (mut latest, validator): (Latest, _) = read(&store, &path).unwrap();
        latest.pending = None;
        latest.lease = None;
        latest.mutation_id = Uuid::v4();
        store
            .backend
            .put_bytes(&path, &validator, &encode_record(&latest).unwrap())
            .unwrap();
        assert_eq!(
            replay_proven(&store, &proof, &FixedClock)
                .unwrap()
                .durable_markers
                .len(),
            1
        );
        let (current, _): (Latest, _) = read(&store, &path).unwrap();
        assert_eq!(current, latest);
        let root = tempfile::tempdir().unwrap();
        let store = make_store(root.path());
        let operation = make_operation(OperationPayload::PruneIntent(PrunePayload {
            targets: vec![VersionTarget {
                table: name("events"),
                partition: partition(),
                version: Counter::from(9),
            }],
        }));
        commit(&store, &operation);
        assert!(
            replay_committed(&store, &name("data"), &operation, &FixedClock)
                .unwrap()
                .durable_markers
                .is_empty()
        );
        assert!(!root.path().join("datasets/data/events").exists());
    }
}
