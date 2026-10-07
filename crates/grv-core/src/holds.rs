//! Immutable dependency holds. A hold has no expiry and consumers cannot release
//! it: only a committed source-GC operation creates its release marker.
use crate::{
    clock::{Clock, parse},
    ownership::Ownership,
    revision,
    store::{Result, Store, backend_error, public_error},
};
use grv_storage::{
    Backend, ErrorKind, ListEntry, ListMode, ObjectKey, ObjectPrefix, Validator, WriteEffect,
    model::*,
};
use grv_types::{ErrorCode, Name, Uuid};
use serde::de::DeserializeOwned;
use std::{collections::BTreeSet, path::Path};

const RECORD_LIMIT: usize = 64 * 1024 * 1024;
const METADATA_LIMIT: usize = 128 * 1024 * 1024;
fn integrity(message: &str) -> grv_types::PublicError {
    public_error(ErrorCode::IntegrityFailure, message)
}
fn conflict(message: &str) -> grv_types::PublicError {
    public_error(ErrorCode::StateConflict, message)
}
fn key(dataset: &Name, relative: &str) -> ObjectKey {
    ObjectKey::new(format!("datasets/{dataset}/{relative}")).expect("validated hold path")
}
pub fn hold_key(record: &HoldRecord) -> ObjectKey {
    key(
        &record.dataset,
        &format!(
            ".holds/{}/revision={}/{}.json",
            record.target_dataset, record.revision, record.retention_id
        ),
    )
}
pub fn release_key(record: &HoldRecord) -> ObjectKey {
    key(
        &record.dataset,
        &format!(
            ".states/released-holds/{}/revision={}/{}.json",
            record.target_dataset, record.revision, record.retention_id
        ),
    )
}

/// In-memory evidence of the required create-before-prune-check ordering.
/// Construction is private; deserializing an adapter or journal record cannot
/// manufacture confirmation authority.
pub struct ConfirmedHold<'a> {
    record: HoldRecord,
    backend: &'a dyn Backend,
}
impl Clone for ConfirmedHold<'_> {
    fn clone(&self) -> Self {
        Self {
            record: self.record.clone(),
            backend: self.backend,
        }
    }
}
impl std::fmt::Debug for ConfirmedHold<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConfirmedHold")
            .field("record", &self.record)
            .finish_non_exhaustive()
    }
}
impl ConfirmedHold<'_> {
    pub fn record(&self) -> &HoldRecord {
        &self.record
    }
}

pub struct Holds<'a, B: Backend> {
    store: &'a Store<B>,
    clock: &'a dyn Clock,
    scratch: &'a Path,
}
impl<'a, B: Backend> Holds<'a, B> {
    pub fn new(store: &'a Store<B>, clock: &'a dyn Clock, scratch: &'a Path) -> Self {
        Self {
            store,
            clock,
            scratch,
        }
    }
    fn read<T: DeserializeOwned + Validate>(&self, path: &ObjectKey) -> Result<(T, Validator)> {
        let (bytes, metadata) = self
            .store
            .backend
            .read_bytes(path, RECORD_LIMIT)
            .map_err(backend_error)?;
        Ok((
            decode_record(&bytes).map_err(backend_error)?,
            metadata.validator,
        ))
    }
    fn optional<T: DeserializeOwned + Validate>(&self, path: &ObjectKey) -> Result<Option<T>> {
        match self.read(path) {
            Ok((record, _)) => Ok(Some(record)),
            Err(error) if error.code == ErrorCode::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }
    fn parent(&self, record: &HoldRecord) -> Result<(RunControl, Validator)> {
        let found: (RunControl, _) = self.read(&key(
            &record.target_dataset,
            &format!(".runs/{}.control.json", record.target_run_id),
        ))?;
        if found.0.run_id != record.target_run_id
            || !found.0.inputs.iter().any(|input| {
                input.dataset == record.dataset
                    && input.revision == record.revision
                    && input.retention_id == record.retention_id
            })
        {
            return Err(integrity(
                "hold is absent from its target run's fixed inputs",
            ));
        }
        Ok(found)
    }
    /// Pure intents are stable across retries. Persist the prepared run and these
    /// records before committing the run control, then acquire its holds.
    pub fn prepare(&self, consumer: &Name, run: &RunControl) -> Result<Vec<HoldRecord>> {
        run.validate().map_err(backend_error)?;
        if run.phase != RunPhase::Open || run.holds_confirmed {
            return Err(conflict("only an unconfirmed open run may prepare holds"));
        }
        run.inputs
            .iter()
            .map(|input| {
                let record = HoldRecord {
                    retention_id: input.retention_id.clone(),
                    dataset: input.dataset.clone(),
                    revision: input.revision,
                    target_dataset: consumer.clone(),
                    target_run_id: run.run_id.clone(),
                    created_at: run.created_at.clone(),
                };
                record.validate().map_err(backend_error)?;
                Ok(record)
            })
            .collect()
    }
    pub fn active(&self, record: &HoldRecord) -> Result<bool> {
        record.validate().map_err(backend_error)?;
        let Some(found): Option<HoldRecord> = self.optional(&hold_key(record))? else {
            return Ok(false);
        };
        if found != *record {
            return Err(integrity(
                "hold identity was reused or its body differs from its path",
            ));
        }
        if let Some(marker) = self.optional::<HoldReleaseMarker>(&release_key(record))? {
            if marker.retention_id != record.retention_id {
                return Err(integrity("hold release marker differs from its path"));
            }
            return Ok(false);
        }
        Ok(true)
    }
    /// Publisher fence for derived versions, using the authoritative sealed run
    /// already read by the caller. Source selectors must resolve against the
    /// cited committed revision, and each exact hold must remain active.
    pub fn check_references(
        &self,
        consumer: &Name,
        run: &SealedRun,
        manifest: &VersionManifest,
    ) -> Result<()> {
        run.validate_manifest(manifest).map_err(backend_error)?;
        self.check_sources(
            consumer,
            &run.run_id,
            manifest.derived_from.as_deref().unwrap_or_default(),
        )
    }
    /// Read-only writer fence; storage allows a valid subset of the fixed run's
    /// inputs. Client build preparation separately requires whole-input citations.
    pub fn check_open_references(
        &self,
        consumer: &Name,
        run: &RunControl,
        sources: &[SourceReference],
    ) -> Result<()> {
        run.validate().map_err(backend_error)?;
        if run.phase != RunPhase::Open
            || !run.holds_confirmed
            || run
                .expires_at
                .as_ref()
                .is_none_or(|expiry| parse(expiry) <= parse(&self.clock.now()))
        {
            return Err(conflict(
                "expired, unconfirmed, or closed run cannot write derived output",
            ));
        }
        if sources.is_empty() {
            return Err(integrity(
                "derived output requires explicit source citations",
            ));
        }
        let mut seen = BTreeSet::new();
        for source in sources {
            if source.dataset == *consumer
                || source.revision.get() == 0
                || source.partition.is_some() && source.table.is_none()
                || !run.inputs.iter().any(|input| {
                    input.dataset == source.dataset
                        && input.revision == source.revision
                        && input.retention_id == source.retention_id
                })
                || !seen.insert((
                    source.dataset.clone(),
                    source.revision,
                    source.retention_id.clone(),
                    source.table.clone(),
                    source.partition.clone(),
                ))
            {
                return Err(integrity(
                    "derived citation is duplicate, invalid, or absent from fixed run inputs",
                ));
            }
        }
        self.check_sources(consumer, &run.run_id, sources)
    }
    fn check_sources(
        &self,
        consumer: &Name,
        run_id: &grv_types::RunId,
        sources: &[SourceReference],
    ) -> Result<()> {
        for source in sources {
            let path = key(
                &source.dataset,
                &format!(
                    ".holds/{consumer}/revision={}/{}.json",
                    source.revision, source.retention_id
                ),
            );
            let (hold, _): (HoldRecord, _) = self.read(&path)?;
            if hold_key(&hold) != path || hold.target_run_id != *run_id || !self.active(&hold)? {
                return Err(integrity(
                    "version provenance has no active matching run hold",
                ));
            }
            if !revision::committed(self.store, &source.dataset, source.revision, self.scratch)? {
                return Err(integrity(
                    "version provenance references an uncommitted revision",
                ));
            }
            if let Some(table) = &source.table {
                let state =
                    revision::read(self.store, &source.dataset, source.revision, self.scratch)?;
                if !state.state.values().any(|entry| entry.table == *table) {
                    return Err(integrity("version provenance table does not resolve"));
                }
                if let Some(partition) = &source.partition {
                    let layout = self.layout(&source.dataset, table)?;
                    let encoded = layout.partition_path(partition).map_err(backend_error)?;
                    if !state.state.contains_key(&(table.clone(), encoded)) {
                        return Err(integrity("version provenance partition does not resolve"));
                    }
                }
            }
        }
        Ok(())
    }
    pub fn acquire(&self, record: &HoldRecord) -> Result<ConfirmedHold<'a>> {
        record.validate().map_err(backend_error)?;
        let (run, _) = self.parent(record)?;
        if run.phase != RunPhase::Open
            || run
                .expires_at
                .as_ref()
                .is_none_or(|expiry| parse(expiry) <= parse(&self.clock.now()))
        {
            return Err(conflict(
                "expired or sealed run cannot acquire dependency holds",
            ));
        }
        let bytes = encode_record(record).map_err(backend_error)?;
        match self.store.backend.create_bytes(&hold_key(record), &bytes) {
            Ok(_) => {}
            Err(error)
                if error.kind == ErrorKind::PreconditionFailed
                    || error.effect == WriteEffect::MaybeApplied =>
            {
                // Missing/unreadable evidence is not proof that an ambiguous
                // create failed, and must never authorize a changed identity.
                match self.read::<HoldRecord>(&hold_key(record)) {
                    Ok((found, _)) if found == *record => {}
                    Ok(_) => return Err(integrity("immutable hold identity was reused")),
                    Err(_) if error.effect == WriteEffect::MaybeApplied => {
                        return Err(public_error(
                            ErrorCode::OutcomeUnknown,
                            "hold creation has no durable outcome proof",
                        ));
                    }
                    Err(error) => return Err(error),
                }
            }
            Err(error) => return Err(backend_error(error)),
        }
        let (latest, _) = revision::read_latest(self.store, &record.dataset)?
            .ok_or_else(|| conflict("hold source has no committed revision"))?;
        let source = revision::read(self.store, &record.dataset, record.revision, self.scratch)?;
        if let Some(pending) = &latest.pending {
            let (operation, _): (OperationRecord, _) = self.read(&key(&record.dataset, pending))?;
            if operation.dataset != record.dataset
                || pending != &format!(".states/operations/{}.json", operation.operation_id)
            {
                return Err(integrity(
                    "pending retention operation differs from its path",
                ));
            }
            if let OperationPayload::PruneIntent(payload) | OperationPayload::Prune(payload) =
                &operation.body
            {
                let mut touches = false;
                for target in &payload.targets {
                    touches |= self.references(&record.dataset, &source, target)?;
                }
                if touches {
                    // A stale lease does not authorize ignoring this decision.
                    return Err(conflict("pending pruning touches the held revision"));
                }
            }
        }
        if !revision::committed(self.store, &record.dataset, record.revision, self.scratch)? {
            return Err(conflict("hold source revision is not committed"));
        }
        // Always verify every entry. This deliberately uses the conservative
        // path instead of relying on current-revision/revision-pin shortcuts.
        let ownership = Ownership::new(
            self.store,
            self.clock,
            self.store.parameters.max_lease_ttl_seconds.get(),
        )?;
        for entry in source.state.values() {
            let layout = self.layout(&record.dataset, &entry.table)?;
            let partition = layout
                .parse_partition(&entry.partition)
                .map_err(backend_error)?;
            let manifest =
                ownership.verify_version(&record.dataset, &layout, &partition, entry.version)?;
            if manifest.run_id != entry.run_id {
                return Err(integrity("held revision run differs from verified version"));
            }
            let (control, _): (RunControl, _) = self.read(&key(
                &record.dataset,
                &format!(".runs/{}.control.json", entry.run_id),
            ))?;
            let (sealed, _): (SealedRun, _) = self.read(&key(
                &record.dataset,
                &format!(".runs/{}.json", entry.run_id),
            ))?;
            sealed.validate_control(&control).map_err(backend_error)?;
            sealed.validate_manifest(&manifest).map_err(backend_error)?;
        }
        if !self.active(record)? {
            return Err(conflict("hold was released"));
        }
        Ok(ConfirmedHold {
            record: record.clone(),
            backend: &self.store.backend,
        })
    }
    fn layout(&self, dataset: &Name, table: &Name) -> Result<TableLayout> {
        let (layout, _): (TableLayout, _) =
            self.read(&key(dataset, &format!("{table}/.layout.json")))?;
        if layout.table != *table {
            return Err(integrity("hold table layout differs from its path"));
        }
        Ok(layout)
    }
    fn references(
        &self,
        dataset: &Name,
        source: &revision::Revision,
        target: &VersionTarget,
    ) -> Result<bool> {
        let layout = self.layout(dataset, &target.table)?;
        let partition = layout
            .partition_path(&target.partition)
            .map_err(backend_error)?;
        Ok(source
            .state
            .get(&(target.table.clone(), partition))
            .is_some_and(|entry| entry.version == target.version))
    }
    /// CAS confirmation fences recovery/renewal. Callers replace their private
    /// RunOwner control with this exact returned record before allocating.
    pub fn confirm_run(
        &self,
        consumer: &Name,
        expected: &RunControl,
        holds: &[ConfirmedHold<'_>],
    ) -> Result<RunControl> {
        let (mut current, validator): (RunControl, _) = self.read(&key(
            consumer,
            &format!(".runs/{}.control.json", expected.run_id),
        ))?;
        if current.run_id != expected.run_id
            || current.owner_token != expected.owner_token
            || current.inputs != expected.inputs
            || current.created_at != expected.created_at
            || current.base_revision != expected.base_revision
            || current.metadata != expected.metadata
            || current.phase != RunPhase::Open
        {
            return Err(conflict("run was fenced before hold confirmation"));
        }
        if current
            .expires_at
            .as_ref()
            .is_none_or(|expiry| parse(expiry) <= parse(&self.clock.now()))
        {
            return Err(conflict("expired run cannot confirm dependency holds"));
        }
        if holds.len() != current.inputs.len() {
            return Err(integrity(
                "hold confirmations do not cover exactly the run inputs",
            ));
        }
        let mut seen = BTreeSet::new();
        for hold in holds {
            if !std::ptr::addr_eq(hold.backend, &self.store.backend as &dyn Backend)
                || hold.record.target_dataset != *consumer
                || hold.record.target_run_id != current.run_id
                || !current.inputs.iter().any(|input| {
                    input.dataset == hold.record.dataset
                        && input.revision == hold.record.revision
                        && input.retention_id == hold.record.retention_id
                })
                || !seen.insert(hold.record.retention_id.clone())
                || !self.active(&hold.record)?
            {
                return Err(integrity(
                    "hold confirmation is inactive, duplicate, or belongs to another run",
                ));
            }
        }
        if current.holds_confirmed {
            return Ok(current);
        }
        current.holds_confirmed = true;
        current.mutation_id = Uuid::v4();
        let path = key(consumer, &format!(".runs/{}.control.json", current.run_id));
        let bytes = encode_record(&current).map_err(backend_error)?;
        match self.store.backend.put_bytes(&path, &validator, &bytes) {
            Ok(_) => Ok(current),
            Err(error) if error.effect == WriteEffect::MaybeApplied => {
                match self.read::<RunControl>(&path) {
                    Ok((found, _)) if found == current => Ok(found),
                    _ => Err(public_error(
                        ErrorCode::OutcomeUnknown,
                        "hold confirmation may have been overwritten",
                    )),
                }
            }
            Err(error) => Err(backend_error(error)),
        }
    }
    /// Irreversible release eligibility only. Actual release remains a source
    /// dataset lease operation committed through LATEST.pending.
    pub fn releasable(&self, record: &HoldRecord) -> Result<bool> {
        if !self.active(record)? {
            return Ok(false);
        }
        let path = key(
            &record.target_dataset,
            &format!(".runs/{}.control.json", record.target_run_id),
        );
        let Some(control): Option<RunControl> = self.optional(&path)? else {
            return Ok(true);
        };
        if control.run_id != record.target_run_id {
            return Err(integrity("hold target run differs from path"));
        }
        if control.phase != RunPhase::Sealed {
            return Ok(false);
        }
        if !control.holds_confirmed {
            return Ok(true);
        }
        let sealed = control.sealed_run().map_err(backend_error)?;
        for entry in &sealed.entries {
            let layout = self.layout(&record.target_dataset, &entry.table)?;
            let partition = layout
                .partition_path(&entry.partition)
                .map_err(backend_error)?;
            let folder = if partition.is_empty() {
                format!("{}/version={}", entry.table, entry.version)
            } else {
                format!("{}/{partition}/version={}", entry.table, entry.version)
            };
            if let Some(marker) = self.optional::<PrunedMarker>(&key(
                &record.target_dataset,
                &format!("{folder}/.pruned"),
            ))? {
                if marker.table != entry.table
                    || marker.partition != entry.partition
                    || marker.version != entry.version
                {
                    return Err(integrity("pruning tombstone differs from target run entry"));
                }
                continue;
            }
            let (manifest, _): (VersionManifest, _) = self.read(&key(
                &record.target_dataset,
                &format!("{folder}/manifest.json"),
            ))?;
            sealed.validate_manifest(&manifest).map_err(backend_error)?;
            if manifest.table != entry.table
                || manifest.partition != entry.partition
                || manifest.version != entry.version
                || manifest.claim_token != entry.claim_token
            {
                return Err(integrity("hold target manifest differs from sealed entry"));
            }
            if manifest.derived_from.iter().flatten().any(|source| {
                source.dataset == record.dataset
                    && source.revision == record.revision
                    && source.retention_id == record.retention_id
            }) {
                return Ok(false);
            }
        }
        Ok(true)
    }
    /// GC must call this after committing prune_intent, then remove these
    /// targets before its final CAS decision. Unconfirmed holds protect too.
    pub fn protected_targets(
        &self,
        source: &Name,
        candidates: &[VersionTarget],
    ) -> Result<Vec<VersionTarget>> {
        let prefix = ObjectPrefix::new(format!("datasets/{source}/.holds/")).unwrap();
        let mut protected = BTreeSet::new();
        let mut budget = 0usize;
        for item in self
            .store
            .backend
            .list(&prefix, ListMode::Recursive)
            .map_err(backend_error)?
        {
            let ListEntry::Object(path) = item else {
                return Err(integrity("recursive hold listing returned a prefix"));
            };
            budget = budget.saturating_add(path.as_str().len() + 512);
            if budget > METADATA_LIMIT {
                return Err(public_error(
                    ErrorCode::UnsupportedCapability,
                    "hold listing exceeds128MiB metadata budget",
                ));
            }
            let (hold, _): (HoldRecord, _) = self.read(&path)?;
            if hold.dataset != *source || hold_key(&hold) != path {
                return Err(integrity("hold body does not match its source path"));
            }
            if !self.active(&hold)? {
                continue;
            }
            if !revision::committed(self.store, source, hold.revision, self.scratch)? {
                return Err(integrity("active hold references an uncommitted revision"));
            }
            let state = revision::read(self.store, source, hold.revision, self.scratch)?;
            for (index, target) in candidates.iter().enumerate() {
                if self.references(source, &state, target)? {
                    protected.insert(index);
                }
            }
        }
        Ok(protected
            .into_iter()
            .map(|index| candidates[index].clone())
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        canonical::Sorter,
        clock::{expires, timestamp},
        store::InitOptions,
    };
    use grv_adapter_api::{Column, TableContract};
    use grv_storage::{FaultInjector, FaultPoint, LocalBackend};
    use grv_types::{RunId, Timestamp};
    use std::{
        collections::BTreeMap,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
        time::Duration,
    };
    struct TestClock(AtomicU64);
    impl Clock for TestClock {
        fn now(&self) -> Timestamp {
            timestamp(
                parse(&Timestamp::new("2026-10-06T00:00:00Z").unwrap())
                    + chrono::Duration::seconds(self.0.load(Ordering::SeqCst) as i64),
            )
        }
        fn elapsed(&self) -> Duration {
            Duration::from_secs(self.0.load(Ordering::SeqCst))
        }
    }
    struct LostAck(AtomicBool);
    impl FaultInjector for LostAck {
        fn check(&self, point: FaultPoint) -> std::io::Result<()> {
            if point == FaultPoint::AfterInstall && self.0.swap(false, Ordering::SeqCst) {
                Err(std::io::Error::other("lost durable write acknowledgement"))
            } else {
                Ok(())
            }
        }
    }
    fn name(value: &str) -> Name {
        Name::new(value).unwrap()
    }
    fn run(value: u32) -> RunId {
        RunId::new(format!("01M3KQA080R6Y8C2D9F0G{value:05}")).unwrap()
    }
    struct Fixture {
        root: tempfile::TempDir,
        store: Store<LocalBackend>,
        clock: TestClock,
        fault: Arc<LostAck>,
    }
    impl Fixture {
        fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let fault = Arc::new(LostAck(AtomicBool::new(false)));
            let store = Store::initialize(
                LocalBackend::open(root.path())
                    .unwrap()
                    .with_faults(fault.clone()),
                InitOptions::default(),
            )
            .unwrap()
            .0;
            let result = Self {
                root,
                store,
                clock: TestClock(AtomicU64::new(0)),
                fault,
            };
            let ownership = Ownership::new(&result.store, &result.clock, 60).unwrap();
            let mut owner = ownership
                .prepare_run(name("source"), run(1), Counter::from(0), vec![], None)
                .unwrap();
            ownership.commit_run(&owner).unwrap();
            ownership.confirm_holds(&mut owner).unwrap();
            let contract = TableContract {
                columns: vec![Column {
                    name: "id".into(),
                    logical_type: serde_json::json!("int64"),
                }],
                partition_keys: vec![],
                extensions: serde_json::json!({}),
                column_ext: serde_json::json!({}),
            };
            let mut sorter = Sorter::new(contract.clone(), result.root.path()).unwrap();
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
            let layout = TableLayout {
                table: name("events"),
                partition_keys: vec![],
                extensions: None,
            };
            let intent = ownership
                .prepare_reservation(&owner, layout, Partition::new(), &contract)
                .unwrap();
            let mut progress = crate::ownership::ReservationProgress::Prepared;
            let mut reservation = ownership
                .reserve_authorized(&owner, &intent, &mut progress, |_| Ok(()))
                .unwrap();
            ownership
                .write_group(&owner, &mut reservation, &contract, &staged, None)
                .unwrap();
            ownership
                .release(&mut reservation, ClaimOutcome::Finalized)
                .unwrap();
            ownership.seal(&mut owner).unwrap();
            let revision = revision::Revision {
                revision: Counter::from(1),
                previous_revision: Counter::from(0),
                operation_id: run(2),
                created_at: result.clock.now(),
                state: BTreeMap::from([(
                    (name("events"), String::new()),
                    revision::Entry {
                        table: name("events"),
                        partition: String::new(),
                        version: Counter::from(1),
                        run_id: run(1),
                    },
                )]),
            };
            let file = revision::encode(&revision, result.root.path()).unwrap();
            result
                .store
                .backend
                .conditional_create(
                    &revision::revision_key(&name("source"), Counter::from(1)),
                    &mut std::fs::File::open(file.path()).unwrap(),
                )
                .unwrap();
            let mut latest = Latest::empty();
            latest.revision = Counter::from(1);
            latest.high_water = Counter::from(1);
            result
                .store
                .backend
                .create_bytes(
                    &revision::latest_key(&name("source")),
                    &encode_record(&latest).unwrap(),
                )
                .unwrap();
            result
        }
        fn holds(&self) -> Holds<'_, LocalBackend> {
            Holds::new(&self.store, &self.clock, self.root.path())
        }
        fn consumer_owner(&self, number: u32) -> crate::ownership::RunOwner {
            let ownership = Ownership::new(&self.store, &self.clock, 60).unwrap();
            let owner = ownership
                .prepare_run(
                    name("consumer"),
                    run(number),
                    Counter::from(0),
                    vec![RunInput {
                        dataset: name("source"),
                        revision: Counter::from(1),
                        retention_id: Uuid::v4(),
                    }],
                    None,
                )
                .unwrap();
            ownership.commit_run(&owner).unwrap();
            owner
        }
        fn consumer(&self, number: u32) -> (RunControl, HoldRecord) {
            let owner = self.consumer_owner(number);
            let control = owner.control().clone();
            let hold = self
                .holds()
                .prepare(owner.dataset(), &control)
                .unwrap()
                .remove(0);
            (control, hold)
        }
        fn target(&self) -> VersionTarget {
            VersionTarget {
                table: name("events"),
                partition: Partition::new(),
                version: Counter::from(1),
            }
        }
        fn confirmed_consumer(&self, number: u32) -> (crate::ownership::RunOwner, HoldRecord) {
            let mut owner = self.consumer_owner(number);
            let record = self
                .holds()
                .prepare(owner.dataset(), owner.control())
                .unwrap()
                .remove(0);
            let proof = self.holds().acquire(&record).unwrap();
            Ownership::new(&self.store, &self.clock, 60)
                .unwrap()
                .confirm_dependency_holds(&mut owner, self.root.path(), &[proof])
                .unwrap();
            (owner, record)
        }
        fn output(&self) -> (TableContract, TableLayout, crate::canonical::StagedGroup) {
            let contract = TableContract {
                columns: vec![Column {
                    name: "id".into(),
                    logical_type: serde_json::json!("int64"),
                }],
                partition_keys: vec![],
                extensions: serde_json::json!({}),
                column_ext: serde_json::json!({}),
            };
            let mut sorter = Sorter::new(contract.clone(), self.root.path()).unwrap();
            sorter
                .append(
                    &arrow_array::RecordBatch::try_new(
                        crate::contract::arrow_schema(&contract).unwrap(),
                        vec![Arc::new(arrow_array::Int64Array::from(vec![6]))],
                    )
                    .unwrap(),
                )
                .unwrap();
            (
                contract,
                TableLayout {
                    table: name("events"),
                    partition_keys: vec![],
                    extensions: None,
                },
                sorter.finish().unwrap(),
            )
        }
        fn derived(
            &self,
            owner: &crate::ownership::RunOwner,
        ) -> (
            crate::ownership::DerivedReservation,
            crate::canonical::StagedGroup,
        ) {
            let ownership = Ownership::new(&self.store, &self.clock, 60).unwrap();
            let (contract, layout, staged) = self.output();
            let intent = ownership
                .prepare_build_reservation(
                    owner,
                    layout,
                    Partition::new(),
                    &contract,
                    self.root.path(),
                )
                .unwrap();
            let derived = ownership
                .reserve_derived_authorized(
                    owner,
                    &intent,
                    &mut crate::ownership::ReservationProgress::Prepared,
                    self.root.path(),
                    |_, _| Ok(()),
                )
                .unwrap();
            (derived, staged)
        }
        fn publish(&self, dataset: Name, changes: ChangeSet) -> Counter {
            let publisher =
                crate::publication::Publisher::new(&self.store, &self.clock, 60).unwrap();
            let intent = publisher.prepare_lease(dataset, "test".into()).unwrap();
            let mut lease = publisher
                .acquire_authorized(
                    &intent,
                    &mut crate::publication::LeaseProgress::Prepared,
                    |_| Ok(()),
                )
                .unwrap();
            let candidate = publisher
                .prepare(&mut lease, changes, self.root.path())
                .unwrap();
            publisher.commit(candidate, |_| Ok(())).unwrap().revision
        }
        fn intent(&self) {
            // Supersede the held revision; a conforming GC never proposes the
            // current non-retired revision's versions.
            let revision = revision::Revision {
                revision: Counter::from(2),
                previous_revision: Counter::from(1),
                operation_id: run(89),
                created_at: self.clock.now(),
                state: BTreeMap::new(),
            };
            let file = revision::encode(&revision, self.root.path()).unwrap();
            self.store
                .backend
                .conditional_create(
                    &revision::revision_key(&name("source"), Counter::from(2)),
                    &mut std::fs::File::open(file.path()).unwrap(),
                )
                .unwrap();
            let operation = OperationRecord {
                operation_id: run(90),
                dataset: name("source"),
                created_at: self.clock.now(),
                created_by: "test".into(),
                body: OperationPayload::PruneIntent(PrunePayload {
                    targets: vec![self.target()],
                }),
            };
            self.store
                .backend
                .create_bytes(
                    &key(
                        &name("source"),
                        ".states/operations/01M3KQA080R6Y8C2D9F0G00090.json",
                    ),
                    &encode_record(&operation).unwrap(),
                )
                .unwrap();
            let (mut latest, validator): (Latest, _) = self
                .holds()
                .read(&revision::latest_key(&name("source")))
                .unwrap();
            latest.revision = Counter::from(2);
            latest.high_water = Counter::from(2);
            latest.pending = Some(format!(
                ".states/operations/{}.json",
                operation.operation_id
            ));
            latest.lease = Some(Lease {
                holder: "gc".into(),
                token: LeaseToken::generate(),
                claimed_at: self.clock.now(),
                expires_at: expires(&self.clock.now(), 60, &self.store.parameters).unwrap(),
            });
            self.store
                .backend
                .put_bytes(
                    &revision::latest_key(&name("source")),
                    &validator,
                    &encode_record(&latest).unwrap(),
                )
                .unwrap();
        }
    }
    #[test]
    fn durable_hold_creation_and_confirmation_adopt_lost_ack_without_reusing_identity() {
        let f = Fixture::new();
        let (control, record) = f.consumer(3);
        f.fault.0.store(true, Ordering::SeqCst);
        let confirmed = f.holds().acquire(&record).unwrap();
        f.fault.0.store(true, Ordering::SeqCst);
        let result = f
            .holds()
            .confirm_run(
                &name("consumer"),
                &control,
                std::slice::from_ref(&confirmed),
            )
            .unwrap();
        assert!(result.holds_confirmed);
        assert!(
            f.holds()
                .confirm_run(&name("consumer"), &control, &[confirmed])
                .unwrap()
                .holds_confirmed
        );
        let mut changed = record.clone();
        changed.created_at = Timestamp::new("2026-10-06T00:00:01Z").unwrap();
        assert_eq!(
            f.holds().acquire(&changed).unwrap_err().code,
            ErrorCode::IntegrityFailure
        );
    }
    #[test]
    fn earlier_prune_intent_blocks_confirmation_even_after_gc_lease_expires() {
        let f = Fixture::new();
        let (_, record) = f.consumer(3);
        f.intent();
        assert_eq!(
            f.holds().acquire(&record).unwrap_err().code,
            ErrorCode::StateConflict
        );
        f.clock.0.store(61, Ordering::SeqCst);
        let (_, newer) = f.consumer(4);
        assert_eq!(
            f.holds().acquire(&newer).unwrap_err().code,
            ErrorCode::StateConflict
        );
        assert!(f.holds().active(&record).unwrap());
    }
    #[test]
    fn later_prune_intent_relisting_observes_confirmed_and_unconfirmed_holds() {
        let f = Fixture::new();
        let (control, record) = f.consumer(3);
        let confirmed = f.holds().acquire(&record).unwrap();
        f.holds()
            .confirm_run(&name("consumer"), &control, &[confirmed])
            .unwrap();
        f.intent();
        assert_eq!(
            f.holds()
                .protected_targets(&name("source"), &[f.target()])
                .unwrap()
                .len(),
            1
        );
        let (_, unconfirmed) = f.consumer(4);
        f.store
            .backend
            .create_bytes(
                &hold_key(&unconfirmed),
                &encode_record(&unconfirmed).unwrap(),
            )
            .unwrap();
        // Unconfirmed records still protect even if the other record is released.
        f.store
            .backend
            .create_bytes(
                &release_key(&record),
                &encode_record(&HoldReleaseMarker {
                    retention_id: record.retention_id,
                    operation_id: run(91),
                    released_at: f.clock.now(),
                })
                .unwrap(),
            )
            .unwrap();
        assert_eq!(
            f.holds()
                .protected_targets(&name("source"), &[f.target()])
                .unwrap()
                .len(),
            1
        );
    }
    #[test]
    fn expired_target_run_keeps_hold_until_irreversible_seal() {
        let f = Fixture::new();
        let (mut control, record) = f.consumer(3);
        let confirmed = f.holds().acquire(&record).unwrap();
        control = f
            .holds()
            .confirm_run(&name("consumer"), &control, &[confirmed])
            .unwrap();
        f.clock.0.store(1000, Ordering::SeqCst);
        assert!(!f.holds().releasable(&record).unwrap());
        let path = key(
            &name("consumer"),
            &format!(".runs/{}.control.json", control.run_id),
        );
        let (_, validator): (RunControl, _) = f.holds().read(&path).unwrap();
        control.phase = RunPhase::Sealed;
        control.expires_at = None;
        control.sealed_at = Some(f.clock.now());
        control.entries = Some(vec![]);
        control.mutation_id = Uuid::v4();
        f.store
            .backend
            .put_bytes(&path, &validator, &encode_record(&control).unwrap())
            .unwrap();
        assert!(f.holds().releasable(&record).unwrap());
        assert!(
            f.holds().active(&record).unwrap(),
            "eligibility does not itself release a hold"
        );
    }
    #[test]
    fn recovery_or_expiry_fences_confirmation_and_tokens_cannot_be_forged() {
        let f = Fixture::new();
        let (control, record) = f.consumer(3);
        let confirmed = f.holds().acquire(&record).unwrap();
        assert_eq!(
            f.holds()
                .confirm_run(&name("consumer"), &control, &[])
                .unwrap_err()
                .code,
            ErrorCode::IntegrityFailure
        );
        f.clock.0.store(60, Ordering::SeqCst);
        assert_eq!(
            f.holds()
                .confirm_run(&name("consumer"), &control, &[confirmed])
                .unwrap_err()
                .code,
            ErrorCode::StateConflict
        );
    }
    #[test]
    fn corrupt_committed_versions_never_confirm_a_hold() {
        let f = Fixture::new();
        let (_, record) = f.consumer(3);
        let path = key(&name("source"), "events/version=1/data.parquet");
        let validator = f.store.backend.head(&path).unwrap().validator;
        f.store
            .backend
            .put_bytes(&path, &validator, b"corrupt")
            .unwrap();
        assert_eq!(
            f.holds().acquire(&record).unwrap_err().code,
            ErrorCode::IntegrityFailure
        );
    }
    #[test]
    fn pruned_committed_versions_reject_holds_even_while_data_remains_present() {
        let f = Fixture::new();
        let (_, record) = f.consumer(3);
        f.store
            .backend
            .create_bytes(
                &key(&name("source"), "events/version=1/.pruned"),
                &encode_record(&PrunedMarker {
                    operation_id: run(98),
                    pruned_by: "test".into(),
                    pruned_at: f.clock.now(),
                    table: name("events"),
                    partition: Partition::new(),
                    version: Counter::from(1),
                })
                .unwrap(),
            )
            .unwrap();
        assert_eq!(
            f.holds().acquire(&record).unwrap_err().code,
            ErrorCode::IntegrityFailure
        );
    }
    #[test]
    fn malformed_hold_paths_abort_gc_protection_scan() {
        let f = Fixture::new();
        let (_, record) = f.consumer(3);
        f.store
            .backend
            .create_bytes(
                &key(&name("source"), ".holds/other/revision=1/wrong.json"),
                &encode_record(&record).unwrap(),
            )
            .unwrap();
        assert_eq!(
            f.holds()
                .protected_targets(&name("source"), &[f.target()])
                .unwrap_err()
                .code,
            ErrorCode::IntegrityFailure
        );
    }
    #[test]
    fn identical_hold_ids_in_different_consumer_folders_have_independent_release_markers() {
        let f = Fixture::new();
        let (control, record) = f.consumer(3);
        f.holds().acquire(&record).unwrap();
        f.store
            .backend
            .create_bytes(
                &key(
                    &name("other_consumer"),
                    &format!(".runs/{}.control.json", control.run_id),
                ),
                &encode_record(&control).unwrap(),
            )
            .unwrap();
        let other = f
            .holds()
            .prepare(&name("other_consumer"), &control)
            .unwrap()
            .remove(0);
        assert_eq!(record.retention_id, other.retention_id);
        f.holds().acquire(&other).unwrap();
        f.store
            .backend
            .create_bytes(
                &release_key(&record),
                &encode_record(&HoldReleaseMarker {
                    retention_id: record.retention_id.clone(),
                    operation_id: run(99),
                    released_at: f.clock.now(),
                })
                .unwrap(),
            )
            .unwrap();
        assert!(!f.holds().active(&record).unwrap());
        assert!(f.holds().active(&other).unwrap());
        assert_eq!(
            f.holds()
                .protected_targets(&name("source"), &[f.target()])
                .unwrap()
                .len(),
            1
        );
    }
    #[test]
    fn ownership_confirmation_rejects_missing_and_other_backend_proofs_before_allocations() {
        let f = Fixture::new();
        let mut owner = f.consumer_owner(3);
        let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
        let hold = f
            .holds()
            .prepare(owner.dataset(), owner.control())
            .unwrap()
            .remove(0);
        let proof = f.holds().acquire(&hold).unwrap();
        assert_eq!(
            ownership
                .confirm_dependency_holds(&mut owner, f.root.path(), &[])
                .unwrap_err()
                .code,
            ErrorCode::IntegrityFailure
        );
        assert!(!owner.control().holds_confirmed);
        let other = Fixture::new();
        let mut copied_owner = owner.clone();
        let other_ownership = Ownership::new(&other.store, &other.clock, 60).unwrap();
        other_ownership.commit_run(&copied_owner).unwrap();
        other
            .store
            .backend
            .create_bytes(&hold_key(&hold), &encode_record(&hold).unwrap())
            .unwrap();
        assert_eq!(
            other_ownership
                .confirm_dependency_holds(
                    &mut copied_owner,
                    other.root.path(),
                    std::slice::from_ref(&proof)
                )
                .unwrap_err()
                .code,
            ErrorCode::IntegrityFailure
        );
        assert!(!copied_owner.control().holds_confirmed);
        ownership
            .confirm_dependency_holds(&mut owner, f.root.path(), &[proof])
            .unwrap();
        assert!(owner.control().holds_confirmed);
        f.clock.0.store(30, Ordering::SeqCst);
        ownership.renew_run(&mut owner).unwrap();
        assert!(f.holds().active(&hold).unwrap());
        let mut recovered = {
            f.clock.0.store(91, Ordering::SeqCst);
            ownership
                .recover_run(owner.dataset().clone(), owner.control().run_id.clone())
                .unwrap()
        };
        assert!(
            f.holds().active(&hold).unwrap(),
            "recovery preserves immutable holds"
        );
        let sealed = ownership.seal(&mut recovered).unwrap();
        assert!(sealed.holds_confirmed && sealed.entries.is_empty());
        assert!(f.holds().releasable(&hold).unwrap());
    }
    #[test]
    fn unconfirmed_derived_run_recovery_seals_empty_and_makes_partial_holds_releasable() {
        let f = Fixture::new();
        let owner = f.consumer_owner(3);
        let hold = f
            .holds()
            .prepare(owner.dataset(), owner.control())
            .unwrap()
            .remove(0);
        f.holds().acquire(&hold).unwrap();
        let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
        assert_eq!(
            ownership.resume_prepared_run(&owner).unwrap_err().code,
            ErrorCode::UnsupportedCapability
        );
        f.clock.0.store(61, Ordering::SeqCst);
        let mut recovered = ownership
            .recover_run(owner.dataset().clone(), owner.control().run_id.clone())
            .unwrap();
        assert!(f.holds().active(&hold).unwrap());
        let sealed = ownership.seal(&mut recovered).unwrap();
        assert!(!sealed.holds_confirmed && sealed.entries.is_empty());
        assert_eq!(sealed.inputs, owner.control().inputs);
        assert!(f.holds().releasable(&hold).unwrap());
    }
    #[test]
    fn confirmed_derived_run_cannot_use_direct_writer_with_omitted_citations() {
        let f = Fixture::new();
        let mut owner = f.consumer_owner(3);
        let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
        let hold = f
            .holds()
            .prepare(owner.dataset(), owner.control())
            .unwrap()
            .remove(0);
        let proof = f.holds().acquire(&hold).unwrap();
        ownership
            .confirm_dependency_holds(&mut owner, f.root.path(), &[proof])
            .unwrap();
        let contract = TableContract {
            columns: vec![Column {
                name: "id".into(),
                logical_type: serde_json::json!("int64"),
            }],
            partition_keys: vec![],
            extensions: serde_json::json!({}),
            column_ext: serde_json::json!({}),
        };
        let layout = TableLayout {
            table: name("events"),
            partition_keys: vec![],
            extensions: None,
        };
        assert_eq!(
            ownership
                .prepare_reservation(&owner, layout.clone(), Partition::new(), &contract)
                .unwrap_err()
                .code,
            ErrorCode::UnsupportedCapability
        );
        let intent = ownership
            .prepare_build_reservation(&owner, layout, Partition::new(), &contract, f.root.path())
            .unwrap();
        let mut progress = crate::ownership::ReservationProgress::Prepared;
        let mut derived = ownership
            .reserve_derived_authorized(
                &owner,
                &intent,
                &mut progress,
                f.root.path(),
                |_, _| Ok(()),
            )
            .unwrap();
        let staged = Sorter::new(contract.clone(), f.root.path())
            .unwrap()
            .finish()
            .unwrap();
        assert_eq!(
            ownership
                .write_group(&owner, derived.reservation_mut(), &contract, &staged, None)
                .unwrap_err()
                .code,
            ErrorCode::UnsupportedCapability
        );
        assert!(
            !f.root
                .path()
                .join("datasets/consumer/events/version=1/manifest.json")
                .exists()
        );
        assert!(
            !f.root
                .path()
                .join("datasets/consumer/events/version=1/data.parquet")
                .exists()
        );
    }
    #[test]
    fn canonical_derived_version_publishes_and_keeps_historic_input_after_latest_changes() {
        let f = Fixture::new();
        let (mut owner, hold) = f.confirmed_consumer(3);
        let (mut derived, staged) = f.derived(&owner);
        let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
        let manifest = ownership
            .write_derived_group(
                &mut owner,
                &mut derived,
                &staged,
                None,
                f.root.path(),
                &mut crate::ownership::DerivedWriteProgress::Prepared,
                |_| Ok(()),
            )
            .unwrap();
        assert_eq!(manifest.row_count.get(), 1);
        assert_eq!(manifest.derived_from.as_deref(), Some(derived.sources()));
        ownership
            .release(derived.reservation_mut(), ClaimOutcome::Finalized)
            .unwrap();
        let sealed = ownership.seal(&mut owner).unwrap();
        f.holds()
            .check_references(owner.dataset(), &sealed, &manifest)
            .unwrap();
        assert_eq!(
            f.publish(
                name("source"),
                ChangeSet {
                    omissions: vec![Omission {
                        table: name("events"),
                        partition: None
                    }],
                    ..Default::default()
                }
            )
            .get(),
            2
        );
        assert_eq!(
            f.publish(
                name("consumer"),
                ChangeSet {
                    runs: vec![owner.control().run_id.clone()],
                    ..Default::default()
                }
            )
            .get(),
            1
        );
        let revision =
            revision::read(&f.store, &name("consumer"), Counter::from(1), f.root.path()).unwrap();
        assert_eq!(revision.state.len(), 1);
        assert!(f.holds().active(&hold).unwrap());
        assert!(!f.holds().releasable(&hold).unwrap());
        assert_eq!(
            f.holds()
                .protected_targets(&name("source"), &[f.target()])
                .unwrap()
                .len(),
            1
        );
        assert_eq!(manifest.derived_from.unwrap()[0].revision.get(), 1);
    }
    #[test]
    fn invalid_or_inactive_citations_fail_before_claim_layout_schema_or_output_effects() {
        let f = Fixture::new();
        let (owner, hold) = f.confirmed_consumer(3);
        let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
        let (contract, layout, _) = f.output();
        let source = SourceReference {
            dataset: hold.dataset.clone(),
            revision: hold.revision,
            retention_id: hold.retention_id.clone(),
            table: None,
            partition: None,
        };
        let mut invalid = source.clone();
        invalid.retention_id = Uuid::v4();
        assert_eq!(
            ownership
                .prepare_derived_reservation(
                    &owner,
                    layout.clone(),
                    Partition::new(),
                    &contract,
                    vec![invalid],
                    f.root.path()
                )
                .unwrap_err()
                .code,
            ErrorCode::IntegrityFailure
        );
        let mut invalid = source.clone();
        invalid.table = Some(name("missing"));
        assert_eq!(
            ownership
                .prepare_derived_reservation(
                    &owner,
                    layout.clone(),
                    Partition::new(),
                    &contract,
                    vec![invalid],
                    f.root.path()
                )
                .unwrap_err()
                .code,
            ErrorCode::IntegrityFailure
        );
        let intent = ownership
            .prepare_derived_reservation(
                &owner,
                layout,
                Partition::new(),
                &contract,
                vec![source],
                f.root.path(),
            )
            .unwrap();
        f.store
            .backend
            .create_bytes(
                &release_key(&hold),
                &encode_record(&HoldReleaseMarker {
                    retention_id: hold.retention_id.clone(),
                    operation_id: run(99),
                    released_at: f.clock.now(),
                })
                .unwrap(),
            )
            .unwrap();
        assert_eq!(
            ownership
                .reserve_derived_authorized(
                    &owner,
                    &intent,
                    &mut crate::ownership::ReservationProgress::Prepared,
                    f.root.path(),
                    |_, _| Ok(())
                )
                .unwrap_err()
                .code,
            ErrorCode::IntegrityFailure
        );
        assert!(!f.root.path().join("datasets/consumer/events").exists());
    }
    #[test]
    fn expired_or_unconfirmed_derived_run_cannot_allocate_or_commit_output() {
        let f = Fixture::new();
        let owner = f.consumer_owner(3);
        let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
        let (contract, layout, _) = f.output();
        assert_eq!(
            ownership
                .prepare_build_reservation(
                    &owner,
                    layout,
                    Partition::new(),
                    &contract,
                    f.root.path()
                )
                .unwrap_err()
                .code,
            ErrorCode::StateConflict
        );
        let (mut owner, _) = f.confirmed_consumer(4);
        let (mut derived, staged) = f.derived(&owner);
        f.clock.0.store(60, Ordering::SeqCst);
        assert_eq!(
            ownership
                .write_derived_group(
                    &mut owner,
                    &mut derived,
                    &staged,
                    None,
                    f.root.path(),
                    &mut crate::ownership::DerivedWriteProgress::Prepared,
                    |_| Ok(())
                )
                .unwrap_err()
                .code,
            ErrorCode::StateConflict
        );
        assert!(
            !f.root
                .path()
                .join("datasets/consumer/events/version=1")
                .exists()
        );
    }
    #[test]
    fn missing_or_mismatched_hold_records_refuse_derived_allocation() {
        for missing in [true, false] {
            let f = Fixture::new();
            let (owner, mut hold) = f.confirmed_consumer(3);
            let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
            let (contract, layout, _) = f.output();
            let intent = ownership
                .prepare_build_reservation(
                    &owner,
                    layout,
                    Partition::new(),
                    &contract,
                    f.root.path(),
                )
                .unwrap();
            let path = hold_key(&hold);
            if missing {
                f.store.backend.delete(&path).unwrap();
            } else {
                let validator = f.store.backend.head(&path).unwrap().validator;
                hold.target_run_id = run(99);
                f.store
                    .backend
                    .put_bytes(&path, &validator, &encode_record(&hold).unwrap())
                    .unwrap();
            }
            let failed = ownership.reserve_derived_authorized(
                &owner,
                &intent,
                &mut crate::ownership::ReservationProgress::Prepared,
                f.root.path(),
                |_, _| Ok(()),
            );
            assert_eq!(
                failed.unwrap_err().code,
                if missing {
                    ErrorCode::NotFound
                } else {
                    ErrorCode::IntegrityFailure
                }
            );
            assert!(!f.root.path().join("datasets/consumer/events").exists());
        }
    }
    #[test]
    fn hold_release_after_data_write_refuses_manifest_commit_and_publication() {
        let f = Fixture::new();
        let (mut owner, hold) = f.confirmed_consumer(3);
        let (mut derived, staged) = f.derived(&owner);
        let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
        let mut progress = crate::ownership::DerivedWriteProgress::Prepared;
        let failure = ownership.write_derived_group(
            &mut owner,
            &mut derived,
            &staged,
            None,
            f.root.path(),
            &mut progress,
            |_| {
                // Adversarial metadata fault at the last durable preparation boundary.
                f.store
                    .backend
                    .create_bytes(
                        &release_key(&hold),
                        &encode_record(&HoldReleaseMarker {
                            retention_id: hold.retention_id.clone(),
                            operation_id: run(99),
                            released_at: f.clock.now(),
                        })
                        .unwrap(),
                    )
                    .unwrap();
                Ok(())
            },
        );
        assert_eq!(failure.unwrap_err().code, ErrorCode::IntegrityFailure);
        assert!(
            f.root
                .path()
                .join("datasets/consumer/events/version=1/data.parquet")
                .exists()
        );
        assert!(
            !f.root
                .path()
                .join("datasets/consumer/events/version=1/manifest.json")
                .exists()
        );
    }
    #[test]
    fn publisher_rejects_missing_mismatched_or_released_cited_hold() {
        for fault in ["missing", "mismatch", "released"] {
            let f = Fixture::new();
            let (mut owner, mut hold) = f.confirmed_consumer(3);
            let (mut derived, staged) = f.derived(&owner);
            let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
            ownership
                .write_derived_group(
                    &mut owner,
                    &mut derived,
                    &staged,
                    None,
                    f.root.path(),
                    &mut crate::ownership::DerivedWriteProgress::Prepared,
                    |_| Ok(()),
                )
                .unwrap();
            ownership
                .release(derived.reservation_mut(), ClaimOutcome::Finalized)
                .unwrap();
            ownership.seal(&mut owner).unwrap();
            let path = hold_key(&hold);
            match fault {
                "missing" => f.store.backend.delete(&path).unwrap(),
                "mismatch" => {
                    let validator = f.store.backend.head(&path).unwrap().validator;
                    hold.target_run_id = run(99);
                    f.store
                        .backend
                        .put_bytes(&path, &validator, &encode_record(&hold).unwrap())
                        .unwrap();
                }
                _ => {
                    f.store
                        .backend
                        .create_bytes(
                            &release_key(&hold),
                            &encode_record(&HoldReleaseMarker {
                                retention_id: hold.retention_id.clone(),
                                operation_id: run(99),
                                released_at: f.clock.now(),
                            })
                            .unwrap(),
                        )
                        .unwrap();
                }
            }
            let publisher = crate::publication::Publisher::new(&f.store, &f.clock, 60).unwrap();
            let intent = publisher
                .prepare_lease(name("consumer"), "test".into())
                .unwrap();
            let mut lease = publisher
                .acquire_authorized(
                    &intent,
                    &mut crate::publication::LeaseProgress::Prepared,
                    |_| Ok(()),
                )
                .unwrap();
            let rejected = publisher
                .prepare(
                    &mut lease,
                    ChangeSet {
                        runs: vec![owner.control().run_id.clone()],
                        ..Default::default()
                    },
                    f.root.path(),
                )
                .err()
                .unwrap();
            assert_eq!(
                rejected.code,
                if fault == "missing" {
                    ErrorCode::NotFound
                } else {
                    ErrorCode::IntegrityFailure
                }
            );
            assert_eq!(
                revision::read_latest(&f.store, &name("consumer"))
                    .unwrap()
                    .unwrap()
                    .0
                    .revision
                    .get(),
                0
            );
            publisher.release(&mut lease).unwrap();
        }
    }
    #[test]
    fn storage_allows_valid_citation_subset_while_v1_build_helper_cites_all_inputs() {
        let f = Fixture::new();
        f.publish(
            name("source"),
            ChangeSet {
                omissions: vec![Omission {
                    table: name("events"),
                    partition: None,
                }],
                ..Default::default()
            },
        );
        let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
        let mut owner = ownership
            .prepare_run(
                name("consumer"),
                run(3),
                Counter::from(0),
                vec![
                    RunInput {
                        dataset: name("source"),
                        revision: Counter::from(1),
                        retention_id: Uuid::v4(),
                    },
                    RunInput {
                        dataset: name("source"),
                        revision: Counter::from(2),
                        retention_id: Uuid::v4(),
                    },
                ],
                None,
            )
            .unwrap();
        ownership.commit_run(&owner).unwrap();
        let holds = f.holds().prepare(owner.dataset(), owner.control()).unwrap();
        let proofs: Vec<_> = holds
            .iter()
            .map(|hold| f.holds().acquire(hold).unwrap())
            .collect();
        ownership
            .confirm_dependency_holds(&mut owner, f.root.path(), &proofs)
            .unwrap();
        let (contract, layout, staged) = f.output();
        let build = ownership
            .prepare_build_reservation(
                &owner,
                layout.clone(),
                Partition::new(),
                &contract,
                f.root.path(),
            )
            .unwrap();
        assert_eq!(build.sources().len(), 2);
        assert!(
            build
                .sources()
                .iter()
                .all(|source| source.table.is_none() && source.partition.is_none())
        );
        let sources = vec![SourceReference {
            dataset: name("source"),
            revision: holds[0].revision,
            retention_id: holds[0].retention_id.clone(),
            table: Some(name("events")),
            partition: Some(Partition::new()),
        }];
        let subset = ownership
            .prepare_derived_reservation(
                &owner,
                layout,
                Partition::new(),
                &contract,
                sources,
                f.root.path(),
            )
            .unwrap();
        let mut derived = ownership
            .reserve_derived_authorized(
                &owner,
                &subset,
                &mut crate::ownership::ReservationProgress::Prepared,
                f.root.path(),
                |_, _| Ok(()),
            )
            .unwrap();
        let manifest = ownership
            .write_derived_group(
                &mut owner,
                &mut derived,
                &staged,
                None,
                f.root.path(),
                &mut crate::ownership::DerivedWriteProgress::Prepared,
                |_| Ok(()),
            )
            .unwrap();
        assert_eq!(manifest.derived_from.unwrap().len(), 1);
        ownership
            .release(derived.reservation_mut(), ClaimOutcome::Finalized)
            .unwrap();
        ownership.seal(&mut owner).unwrap();
        assert!(!f.holds().releasable(&holds[0]).unwrap());
        assert!(f.holds().releasable(&holds[1]).unwrap());
        assert_eq!(
            f.publish(
                name("consumer"),
                ChangeSet {
                    runs: vec![owner.control().run_id.clone()],
                    ..Default::default()
                }
            )
            .get(),
            1
        );
    }
    #[test]
    fn lost_derived_manifest_ack_adopts_exact_durable_citations() {
        let f = Fixture::new();
        let (mut owner, _) = f.confirmed_consumer(3);
        let (mut derived, staged) = f.derived(&owner);
        let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
        let mut progress = crate::ownership::DerivedWriteProgress::Prepared;
        let manifest = ownership
            .write_derived_group(
                &mut owner,
                &mut derived,
                &staged,
                None,
                f.root.path(),
                &mut progress,
                |_| {
                    f.fault.0.store(true, Ordering::SeqCst);
                    Ok(())
                },
            )
            .unwrap();
        let (durable, _): (VersionManifest, _) = f
            .holds()
            .read(&key(&name("consumer"), "events/version=1/manifest.json"))
            .unwrap();
        assert_eq!(durable, manifest);
        assert_eq!(manifest.derived_from.as_deref(), Some(derived.sources()));
        match progress {
            crate::ownership::DerivedWriteProgress::Commit { manifest: recorded } => {
                assert_eq!(*recorded, manifest)
            }
            _ => panic!("durable commit intent required"),
        }
    }
    #[test]
    fn derived_manifest_journal_replays_exact_time_and_citations_after_commit_boundary_crash() {
        use std::io::Write;
        let f = Fixture::new();
        let (mut owner, _) = f.confirmed_consumer(3);
        let (mut derived, staged) = f.derived(&owner);
        let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
        let journal = tempfile::tempdir().unwrap();
        let path = journal.path().join("writer.json");
        let mut progress = crate::ownership::DerivedWriteProgress::Prepared;
        let interrupted = ownership.write_derived_group(
            &mut owner,
            &mut derived,
            &staged,
            None,
            f.root.path(),
            &mut progress,
            |next| {
                let mut file = std::fs::File::create(&path).unwrap();
                file.write_all(&serde_json::to_vec(next).unwrap()).unwrap();
                file.sync_all().unwrap();
                Err(public_error(
                    ErrorCode::BackendFailure,
                    "simulated process death after durable manifest intent",
                ))
            },
        );
        assert_eq!(interrupted.unwrap_err().code, ErrorCode::BackendFailure);
        assert!(
            !f.root
                .path()
                .join("datasets/consumer/events/version=1/manifest.json")
                .exists()
        );
        let mut progress = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        f.clock.0.store(1, Ordering::SeqCst);
        let manifest = ownership
            .write_derived_group(
                &mut owner,
                &mut derived,
                &staged,
                None,
                f.root.path(),
                &mut progress,
                |_| panic!("fixed manifest intent must not be replaced"),
            )
            .unwrap();
        assert_eq!(
            manifest.created_at.as_str(),
            "2026-10-06T00:00:00.000000000Z"
        );
        let manifest_bytes = f
            .store
            .backend
            .read_bytes(
                &key(&name("consumer"), "events/version=1/manifest.json"),
                RECORD_LIMIT,
            )
            .unwrap()
            .0;
        f.clock.0.store(2, Ordering::SeqCst);
        let replay = ownership
            .write_derived_group(
                &mut owner,
                &mut derived,
                &staged,
                None,
                f.root.path(),
                &mut progress,
                |_| panic!("replay must not create another intent"),
            )
            .unwrap();
        assert_eq!(manifest, replay);
        assert_eq!(
            manifest_bytes,
            f.store
                .backend
                .read_bytes(
                    &key(&name("consumer"), "events/version=1/manifest.json"),
                    RECORD_LIMIT
                )
                .unwrap()
                .0
        );
        if let crate::ownership::DerivedWriteProgress::Commit { manifest } = &mut progress {
            manifest.derived_from.as_mut().unwrap()[0].table = Some(name("events"));
        }
        assert_eq!(
            ownership
                .write_derived_group(
                    &mut owner,
                    &mut derived,
                    &staged,
                    None,
                    f.root.path(),
                    &mut progress,
                    |_| Ok(())
                )
                .unwrap_err()
                .code,
            ErrorCode::IntegrityFailure
        );
    }
    #[test]
    fn sealed_cited_entry_keeps_hold_until_tombstone_and_missing_manifest_is_not_release_proof() {
        let f = Fixture::new();
        let (control, record) = f.consumer(3);
        let confirmed = f.holds().acquire(&record).unwrap();
        let mut control = f
            .holds()
            .confirm_run(&name("consumer"), &control, &[confirmed])
            .unwrap();
        let (mut manifest, _): (VersionManifest, _) = f
            .holds()
            .read(&key(&name("source"), "events/version=1/manifest.json"))
            .unwrap();
        let layout = f.holds().layout(&name("source"), &name("events")).unwrap();
        f.store
            .backend
            .create_bytes(
                &key(&name("consumer"), "events/.layout.json"),
                &encode_record(&layout).unwrap(),
            )
            .unwrap();
        manifest.run_id = control.run_id.clone();
        manifest.derived_from = Some(vec![SourceReference {
            dataset: record.dataset.clone(),
            revision: record.revision,
            retention_id: record.retention_id.clone(),
            table: None,
            partition: None,
        }]);
        let manifest_key = key(&name("consumer"), "events/version=1/manifest.json");
        f.store
            .backend
            .create_bytes(&manifest_key, &encode_record(&manifest).unwrap())
            .unwrap();
        let path = key(
            &name("consumer"),
            &format!(".runs/{}.control.json", control.run_id),
        );
        let (_, validator): (RunControl, _) = f.holds().read(&path).unwrap();
        control.phase = RunPhase::Sealed;
        control.expires_at = None;
        control.sealed_at = Some(f.clock.now());
        control.entries = Some(vec![RunEntry {
            table: manifest.table.clone(),
            partition: manifest.partition.clone(),
            version: manifest.version,
            claim_token: manifest.claim_token.clone(),
        }]);
        control.mutation_id = Uuid::v4();
        f.store
            .backend
            .put_bytes(&path, &validator, &encode_record(&control).unwrap())
            .unwrap();
        assert!(!f.holds().releasable(&record).unwrap());
        let sealed = control.sealed_run().unwrap();
        f.holds()
            .check_references(&name("consumer"), &sealed, &manifest)
            .unwrap();
        let mut invalid = manifest.clone();
        invalid.derived_from.as_mut().unwrap()[0].table = Some(name("missing"));
        assert_eq!(
            f.holds()
                .check_references(&name("consumer"), &sealed, &invalid)
                .unwrap_err()
                .code,
            ErrorCode::IntegrityFailure
        );
        f.store.backend.delete(&manifest_key).unwrap();
        assert_eq!(
            f.holds().releasable(&record).unwrap_err().code,
            ErrorCode::NotFound
        );
        f.store
            .backend
            .create_bytes(
                &key(&name("consumer"), "events/version=1/.pruned"),
                &encode_record(&PrunedMarker {
                    operation_id: run(95),
                    pruned_by: "test".into(),
                    pruned_at: f.clock.now(),
                    table: manifest.table,
                    partition: manifest.partition,
                    version: manifest.version,
                })
                .unwrap(),
            )
            .unwrap();
        assert!(f.holds().releasable(&record).unwrap());
        f.store
            .backend
            .create_bytes(
                &release_key(&record),
                &encode_record(&HoldReleaseMarker {
                    retention_id: record.retention_id.clone(),
                    operation_id: run(96),
                    released_at: f.clock.now(),
                })
                .unwrap(),
            )
            .unwrap();
        assert_eq!(
            f.holds()
                .check_references(&name("consumer"), &sealed, &invalid)
                .unwrap_err()
                .code,
            ErrorCode::IntegrityFailure
        );
    }
}
