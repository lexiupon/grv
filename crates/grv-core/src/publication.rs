//! Storage-v2 lease-fenced publication. Candidates are prepared before the
//! consumer journals their operation identity and authorizes the commit CAS.
use crate::{
    clock::{Clock, ExpiryObservation, expires, new_run_id},
    ownership::Ownership,
    revision::{self, Entry, Revision, State},
    store::{Result, Store, backend_error, public_error},
};
use grv_storage::{
    Backend, ErrorKind, ListEntry, ListMode, ObjectKey, Validator, WriteEffect, model::*,
};
use grv_types::{ErrorCode, Name, RunId, Uuid};
use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{Read, Write},
    path::Path,
    sync::Mutex,
};
const RECORD_LIMIT: usize = 64 * 1024 * 1024;
mod abort;
pub use abort::LeaseAbortProgress;
fn error(code: ErrorCode, message: &str) -> grv_types::PublicError {
    public_error(code, message)
}
fn object(dataset: &Name, suffix: &str) -> ObjectKey {
    ObjectKey::new(format!("datasets/{dataset}/{suffix}")).unwrap()
}
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaseIntent {
    pub(crate) dataset: Name,
    pub(crate) holder: String,
    pub(crate) token: LeaseToken,
    reserve_revision: bool,
}
/// Consumer-only evidence; serialize into protected state, never public output.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaseOwner {
    pub(crate) intent: LeaseIntent,
    pub(crate) latest: Latest,
    pub(crate) validator: Validator,
    reserved_revision: Option<Counter>,
}
/// Persist each phase before the corresponding CAS. A fixed acquisition token
/// is never retried against a different precondition after a lost response.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum LeaseProgress {
    Prepared,
    Compare { latest: Latest, expected: Validator },
    Owned { owner: LeaseOwner },
}
impl LeaseOwner {
    pub fn predecessor(&self) -> Counter {
        self.latest.revision
    }
}
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationIntent {
    pub dataset: Name,
    pub operation: OperationRecord,
    lease: LeaseOwner,
}
/// A failed maintenance write cannot turn an established commit into failure.
pub struct Published {
    pub revision: Counter,
    pub maintenance_error: Option<grv_types::PublicError>,
}
pub struct Resolved {
    pub revision: Option<Counter>,
    pub maintenance_error: Option<grv_types::PublicError>,
}
pub struct Candidate {
    intent: PublicationIntent,
    revision_file: tempfile::NamedTempFile,
    owner: LeaseOwner,
    no_op: bool,
}
impl Candidate {
    pub fn intent(&self) -> &PublicationIntent {
        &self.intent
    }
    pub fn is_noop(&self) -> bool {
        self.no_op
    }
}
pub struct Publisher<'a, B: Backend> {
    store: &'a Store<B>,
    clock: &'a dyn Clock,
    ttl: u64,
    observations: Mutex<BTreeMap<String, ExpiryObservation>>,
}
impl<'a, B: Backend> Publisher<'a, B> {
    pub fn new(store: &'a Store<B>, clock: &'a dyn Clock, ttl: u64) -> Result<Self> {
        expires(&clock.now(), ttl, &store.parameters)?;
        Ok(Self {
            store,
            clock,
            ttl,
            observations: Mutex::new(BTreeMap::new()),
        })
    }
    /// The caller persists this exact empty proposal before invoking creation.
    /// Existing coordination is adopted without changing its state or lease.
    pub fn initialize_dataset(&self, dataset: &Name, proposed: &Latest) -> Result<()> {
        proposed.validate().map_err(backend_error)?;
        if proposed.revision.get() != 0
            || proposed.high_water.get() != 0
            || proposed.lease.is_some()
            || proposed.pending.is_some()
        {
            return Err(error(
                ErrorCode::InvalidArgument,
                "initial dataset proposal must be empty",
            ));
        }
        if revision::read_latest(self.store, dataset)?.is_some() {
            return Ok(());
        }
        match self.store.backend.create_bytes(
            &revision::latest_key(dataset),
            &encode_record(proposed).map_err(backend_error)?,
        ) {
            Ok(_) => Ok(()),
            Err(e)
                if e.kind == ErrorKind::PreconditionFailed
                    || e.effect == WriteEffect::MaybeApplied =>
            {
                if revision::read_latest(self.store, dataset)?.is_some() {
                    Ok(())
                } else {
                    Err(error(
                        ErrorCode::OutcomeUnknown,
                        "initial dataset creation has no authoritative outcome",
                    ))
                }
            }
            Err(e) => Err(backend_error(e)),
        }
    }
    pub fn prepare_lease(&self, dataset: Name, holder: String) -> Result<LeaseIntent> {
        if holder.is_empty() {
            return Err(error(ErrorCode::InvalidArgument, "lease holder required"));
        }
        Ok(LeaseIntent {
            dataset,
            holder,
            token: LeaseToken::generate(),
            reserve_revision: false,
        })
    }
    pub(crate) fn renewal_interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(
            self.ttl - self.store.parameters.max_clock_skew_seconds.get(),
        ) / 3
    }
    /// Reserve the publication number in the acquisition CAS. Short publishes
    /// then mutate LATEST only for acquisition/reservation and final commit.
    pub fn prepare_publication_lease(&self, dataset: Name, holder: String) -> Result<LeaseIntent> {
        let mut intent = self.prepare_lease(dataset, holder)?;
        intent.reserve_revision = true;
        Ok(intent)
    }
    fn next_revision(&self, dataset: &Name, latest: &Latest) -> Result<Counter> {
        let mut highest = latest.high_water.max(latest.revision);
        let prefix = revision::revision_prefix(dataset);
        let stem = format!("{}revision=", prefix.as_str());
        for entry in self
            .store
            .backend
            .list(&prefix, ListMode::Children)
            .map_err(backend_error)?
        {
            if let ListEntry::Prefix(prefix) = entry
                && let Some(text) = prefix
                    .as_str()
                    .strip_prefix(&stem)
                    .and_then(|s| s.strip_suffix('/'))
            {
                let n = text
                    .parse::<u64>()
                    .ok()
                    .filter(|n| *n > 0 && n.to_string() == text)
                    .ok_or_else(|| {
                        error(
                            ErrorCode::IntegrityFailure,
                            "invalid revision directory number",
                        )
                    })?;
                highest = highest.max(Counter::new(n).map_err(backend_error)?);
            }
        }
        highest.next().map_err(backend_error)
    }
    pub(crate) fn read<T: DeserializeOwned + Validate>(
        &self,
        key: &ObjectKey,
    ) -> Result<(T, Validator)> {
        let (bytes, meta) = self
            .store
            .backend
            .read_bytes(key, RECORD_LIMIT)
            .map_err(backend_error)?;
        Ok((
            decode_record(&bytes).map_err(backend_error)?,
            meta.validator,
        ))
    }
    pub(crate) fn create<T: Serialize + DeserializeOwned + Validate>(
        &self,
        key: &ObjectKey,
        record: &T,
    ) -> Result<Validator> {
        let bytes = encode_record(record).map_err(backend_error)?;
        match self.store.backend.create_bytes(key, &bytes) {
            Ok(validator) => Ok(validator),
            Err(e)
                if e.kind == ErrorKind::PreconditionFailed
                    || e.effect == WriteEffect::MaybeApplied =>
            {
                let (found, validator): (T, _) = self.read(key)?;
                if serde_json::to_value(&found).unwrap() == serde_json::to_value(record).unwrap() {
                    Ok(validator)
                } else {
                    Err(error(
                        ErrorCode::IntegrityFailure,
                        "immutable publication object has different content",
                    ))
                }
            }
            Err(e) => Err(backend_error(e)),
        }
    }
    pub(crate) fn put(
        &self,
        dataset: &Name,
        validator: &Validator,
        record: &Latest,
    ) -> Result<Validator> {
        let key = revision::latest_key(dataset);
        match self.store.backend.put_bytes(
            &key,
            validator,
            &encode_record(record).map_err(backend_error)?,
        ) {
            Ok(validator) => Ok(validator),
            Err(e) if e.effect == WriteEffect::MaybeApplied => {
                let (found, validator): (Latest, _) = self.read(&key)?;
                if found == *record {
                    Ok(validator)
                } else {
                    Err(error(
                        ErrorCode::OutcomeUnknown,
                        "LATEST transition may have succeeded and been superseded",
                    ))
                }
            }
            Err(e) if e.kind == ErrorKind::PreconditionFailed => Err(error(
                ErrorCode::OwnershipLost,
                "dataset lease CAS was fenced",
            )),
            Err(e) => Err(backend_error(e)),
        }
    }
    /// The consumer must persist the intent before calling, and fsync every
    /// progress callback. Replays use only the recorded mutation/precondition.
    pub fn acquire_authorized(
        &self,
        intent: &LeaseIntent,
        progress: &mut LeaseProgress,
        mut persist: impl FnMut(&LeaseProgress) -> Result<()>,
    ) -> Result<LeaseOwner> {
        self.acquire_inner(intent, progress, &mut persist, None)
    }
    fn acquire_inner(
        &self,
        intent: &LeaseIntent,
        progress: &mut LeaseProgress,
        persist: &mut impl FnMut(&LeaseProgress) -> Result<()>,
        previous: Option<&LeaseOwner>,
    ) -> Result<LeaseOwner> {
        let key = revision::latest_key(&intent.dataset);
        if matches!(progress, LeaseProgress::Prepared) {
            let prior = match revision::read_latest(self.store, &intent.dataset)? {
                Some(value) => value,
                None => {
                    let initial = Latest::empty();
                    match self
                        .store
                        .backend
                        .create_bytes(&key, &encode_record(&initial).map_err(backend_error)?)
                    {
                        Ok(validator) => (initial, validator),
                        Err(e)
                            if e.kind == ErrorKind::PreconditionFailed
                                || e.effect == WriteEffect::MaybeApplied =>
                        {
                            self.read(&key)?
                        }
                        Err(e) => return Err(backend_error(e)),
                    }
                }
            };
            let (mut latest, expected) = prior;
            if let Some(lease) = &latest.lease {
                // Resolution may fence the caller's previous acquisition
                // immediately. Everybody else must observe expiry first.
                let previous_owned = previous.is_some_and(|owner| {
                    owner.intent.dataset == intent.dataset
                        && owner.intent.token == lease.token
                        && owner.intent.holder == lease.holder
                });
                if lease.token == intent.token {
                    return Err(error(
                        ErrorCode::OutcomeUnknown,
                        "acquisition token already exists without journaled CAS evidence",
                    ));
                }
                if !previous_owned
                    && !self
                        .observations
                        .lock()
                        .unwrap()
                        .entry(key.as_str().into())
                        .or_default()
                        .expired(
                            &expected,
                            &lease.expires_at,
                            self.clock,
                            &self.store.parameters,
                        )
                {
                    return Err(error(
                        ErrorCode::StateConflict,
                        "dataset lease has a live holder",
                    ));
                }
            }
            let now = self.clock.now();
            if intent.reserve_revision {
                latest.high_water = self.next_revision(&intent.dataset, &latest)?;
            }
            latest.lease = Some(Lease {
                holder: intent.holder.clone(),
                token: intent.token.clone(),
                claimed_at: now.clone(),
                expires_at: expires(&now, self.ttl, &self.store.parameters)?,
            });
            latest.mutation_id = Uuid::v4();
            let next = LeaseProgress::Compare { latest, expected };
            persist(&next)?;
            *progress = next;
        }
        if let LeaseProgress::Compare { latest, expected } = progress {
            latest.validate().map_err(backend_error)?;
            if !latest
                .lease
                .as_ref()
                .is_some_and(|lease| lease.token == intent.token && lease.holder == intent.holder)
            {
                return Err(error(
                    ErrorCode::IntegrityFailure,
                    "lease journal differs from intent",
                ));
            }
            let (found, current): (Latest, _) = self.read(&key)?;
            let validator = if found.mutation_id == latest.mutation_id && found == *latest {
                current
            } else if current == *expected {
                self.put(&intent.dataset, expected, latest)?
            } else {
                return Err(error(
                    ErrorCode::OutcomeUnknown,
                    "recorded lease acquisition was overwritten; token cannot be reused",
                ));
            };
            let next = LeaseProgress::Owned {
                owner: LeaseOwner {
                    intent: intent.clone(),
                    latest: latest.clone(),
                    validator,
                    reserved_revision: intent.reserve_revision.then_some(latest.high_water),
                },
            };
            persist(&next)?;
            *progress = next;
        }
        let LeaseProgress::Owned { owner } = progress else {
            unreachable!()
        };
        if owner.intent.dataset != intent.dataset
            || owner.intent.token != intent.token
            || owner.intent.holder != intent.holder
            || owner.intent.reserve_revision != intent.reserve_revision
        {
            return Err(error(
                ErrorCode::IntegrityFailure,
                "owned lease differs from intent",
            ));
        }
        let (latest, validator) = self.owned(owner)?;
        Ok(LeaseOwner {
            intent: intent.clone(),
            latest,
            validator,
            reserved_revision: owner.reserved_revision,
        })
    }
    #[cfg(test)]
    fn acquire(&self, intent: LeaseIntent) -> Result<LeaseOwner> {
        self.acquire_authorized(&intent, &mut LeaseProgress::Prepared, |_| Ok(()))
    }
    pub(crate) fn owned(&self, owner: &LeaseOwner) -> Result<(Latest, Validator)> {
        let (latest, validator): (Latest, _) =
            self.read(&revision::latest_key(&owner.intent.dataset))?;
        if !latest.lease.as_ref().is_some_and(|lease| {
            lease.token == owner.intent.token && lease.holder == owner.intent.holder
        }) {
            return Err(error(
                ErrorCode::OwnershipLost,
                "dataset lease owner changed",
            ));
        }
        Ok((latest, validator))
    }
    pub fn renew(&self, owner: &mut LeaseOwner) -> Result<()> {
        let (mut latest, validator) = self.owned(owner)?;
        latest.lease.as_mut().unwrap().expires_at =
            expires(&self.clock.now(), self.ttl, &self.store.parameters)?;
        latest.mutation_id = Uuid::v4();
        owner.validator = self.put(&owner.intent.dataset, &validator, &latest)?;
        owner.latest = latest;
        Ok(())
    }
    pub fn release(&self, owner: &mut LeaseOwner) -> Result<()> {
        let (mut latest, validator) = self.owned(owner)?;
        latest.lease = None;
        latest.mutation_id = Uuid::v4();
        owner.validator = self.put(&owner.intent.dataset, &validator, &latest)?;
        owner.latest = latest;
        Ok(())
    }
    pub(crate) fn finish_pending(&self, owner: &mut LeaseOwner) -> Result<()> {
        let (mut latest, validator) = self.owned(owner)?;
        if let Some(path) = latest.pending.clone() {
            let (operation, _): (OperationRecord, _) =
                self.read(&object(&owner.intent.dataset, &path))?;
            if operation.dataset != owner.intent.dataset
                || path != format!(".states/operations/{}.json", operation.operation_id)
            {
                return Err(error(
                    ErrorCode::IntegrityFailure,
                    "pending operation identity differs from path",
                ));
            }
            let proof = crate::retention::DurableOperationProof::from_pending(
                &owner.intent.dataset,
                &latest,
                &operation,
            )?;
            let (_, renewed) = crate::renewal::during(
                owner.clone(),
                self.renewal_interval(),
                |owner| self.renew(owner),
                |watch| {
                    watch.check()?;
                    crate::retention::replay_proven(self.store, &proof, self.clock)
                },
            )?;
            *owner = renewed;
            let current = self.owned(owner)?;
            latest = current.0;
            let validator = current.1;
            if latest.pending.as_ref() != Some(&path) {
                return Err(error(
                    ErrorCode::OwnershipLost,
                    "pending operation changed during replay",
                ));
            }
            latest.pending = None;
            latest.mutation_id = Uuid::v4();
            owner.validator = self.put(&owner.intent.dataset, &validator, &latest)?;
            owner.latest = latest;
        } else {
            owner.latest = latest;
            owner.validator = validator;
        }
        Ok(())
    }
    fn layout(&self, dataset: &Name, table: &Name) -> Result<TableLayout> {
        let (layout, _): (TableLayout, _) =
            self.read(&object(dataset, &format!("{table}/.layout.json")))?;
        if layout.table != *table {
            return Err(error(
                ErrorCode::IntegrityFailure,
                "layout differs from table path",
            ));
        }
        Ok(layout)
    }
    pub(crate) fn publishable(&self, dataset: &Name, entry: &Entry, parent: &Path) -> Result<()> {
        let layout = self.layout(dataset, &entry.table)?;
        let partition = layout
            .parse_partition(&entry.partition)
            .map_err(backend_error)?;
        let manifest = Ownership::new(self.store, self.clock, self.ttl)?.verify_version(
            dataset,
            &layout,
            &partition,
            entry.version,
        )?;
        if manifest.run_id != entry.run_id {
            return Err(error(
                ErrorCode::IntegrityFailure,
                "revision run identity differs from version manifest",
            ));
        }
        let (control, _): (RunControl, _) = self.read(&object(
            dataset,
            &format!(".runs/{}.control.json", entry.run_id),
        ))?;
        let (run, _): (SealedRun, _) =
            self.read(&object(dataset, &format!(".runs/{}.json", entry.run_id)))?;
        run.validate_control(&control).map_err(backend_error)?;
        run.validate_manifest(&manifest).map_err(backend_error)?;
        crate::holds::Holds::new(self.store, self.clock, parent)
            .check_references(dataset, &run, &manifest)?;
        Ok(())
    }
    pub fn prepare(
        &self,
        owner: &mut LeaseOwner,
        change_set: ChangeSet,
        parent: &Path,
    ) -> Result<Candidate> {
        change_set.validate().map_err(backend_error)?;
        self.finish_pending(owner)?;
        match self
            .store
            .backend
            .head(&object(&owner.intent.dataset, ".retired"))
        {
            Ok(_) => return Err(error(ErrorCode::Unavailable, "dataset is retired")),
            Err(e) if e.kind == ErrorKind::NotFound => {}
            Err(e) => return Err(backend_error(e)),
        }
        let predecessor = owner.latest.revision;
        if change_set
            .expected_revision
            .is_some_and(|expected| expected != predecessor)
        {
            return Err(error(
                ErrorCode::StateConflict,
                "expected predecessor revision changed",
            ));
        }
        let number = if let Some(number) = owner.reserved_revision {
            if owner.latest.high_water != number || number <= predecessor {
                return Err(error(
                    ErrorCode::OwnershipLost,
                    "reserved publication number changed",
                ));
            }
            number
        } else {
            let mut latest = owner.latest.clone();
            let number = self.next_revision(&owner.intent.dataset, &latest)?;
            latest.high_water = number;
            latest.mutation_id = Uuid::v4();
            owner.validator = self.put(&owner.intent.dataset, &owner.validator, &latest)?;
            owner.latest = latest;
            number
        };
        let (mut candidate, renewed) = crate::renewal::during(
            owner.clone(),
            self.renewal_interval(),
            |owner| self.renew(owner),
            |watch| {
                let before = if predecessor.get() == 0 {
                    State::new()
                } else {
                    revision::read(self.store, &owner.intent.dataset, predecessor, parent)?.state
                };
                let mut after = before.clone();
                let mut actions = BTreeSet::new();
                for run_id in &change_set.runs {
                    watch.check()?;
                    let (run, _): (SealedRun, _) = self.read(&object(
                        &owner.intent.dataset,
                        &format!(".runs/{run_id}.json"),
                    ))?;
                    let (control, _): (RunControl, _) = self.read(&object(
                        &owner.intent.dataset,
                        &format!(".runs/{run_id}.control.json"),
                    ))?;
                    run.validate_control(&control).map_err(backend_error)?;
                    if run.run_id != *run_id {
                        return Err(error(
                            ErrorCode::IntegrityFailure,
                            "sealed run differs from path",
                        ));
                    }
                    let mut selected: BTreeMap<(Name, String), Entry> = BTreeMap::new();
                    for entry in &run.entries {
                        let layout = self.layout(&owner.intent.dataset, &entry.table)?;
                        let partition = layout
                            .partition_path(&entry.partition)
                            .map_err(backend_error)?;
                        let pair = (entry.table.clone(), partition.clone());
                        if selected
                            .get(&pair)
                            .is_none_or(|prior| entry.version > prior.version)
                        {
                            selected.insert(
                                pair,
                                Entry {
                                    table: entry.table.clone(),
                                    partition,
                                    version: entry.version,
                                    run_id: run_id.clone(),
                                },
                            );
                        }
                    }
                    let pairs = selected.keys().cloned().collect();
                    revision::check_run_conflicts(
                        self.store,
                        &owner.intent.dataset,
                        run.base_revision,
                        predecessor,
                        &pairs,
                        parent,
                    )?;
                    for (pair, entry) in selected {
                        if !actions.insert(pair.clone()) {
                            return Err(error(
                                ErrorCode::InvalidArgument,
                                "change set changes a pair more than once",
                            ));
                        }
                        after.insert(pair, entry);
                    }
                }
                for omission in &change_set.omissions {
                    let selected: Vec<_> = if let Some(partition) = &omission.partition {
                        let layout = self.layout(&owner.intent.dataset, &omission.table)?;
                        vec![(
                            omission.table.clone(),
                            layout.partition_path(partition).map_err(backend_error)?,
                        )]
                    } else {
                        after
                            .keys()
                            .filter(|(table, _)| table == &omission.table)
                            .cloned()
                            .collect()
                    };
                    for pair in selected {
                        if !actions.insert(pair.clone()) {
                            return Err(error(
                                ErrorCode::InvalidArgument,
                                "omission overlaps another change",
                            ));
                        }
                        after.remove(&pair);
                    }
                }
                for selection in &change_set.selections {
                    let layout = self.layout(&owner.intent.dataset, &selection.table)?;
                    let partition = layout
                        .partition_path(&selection.partition)
                        .map_err(backend_error)?;
                    let folder = if partition.is_empty() {
                        selection.table.to_string()
                    } else {
                        format!("{}/{partition}", selection.table)
                    };
                    let (manifest, _): (VersionManifest, _) = self.read(&object(
                        &owner.intent.dataset,
                        &format!("{folder}/version={}/manifest.json", selection.version),
                    ))?;
                    let pair = (selection.table.clone(), partition.clone());
                    if !actions.insert(pair.clone()) {
                        return Err(error(
                            ErrorCode::InvalidArgument,
                            "selection overlaps another change",
                        ));
                    }
                    after.insert(
                        pair,
                        Entry {
                            table: selection.table.clone(),
                            partition,
                            version: selection.version,
                            run_id: manifest.run_id,
                        },
                    );
                }
                for (pair, entry) in &after {
                    watch.check()?;
                    if before.get(pair).is_none_or(|prior| prior != entry) {
                        self.publishable(&owner.intent.dataset, entry, parent)?;
                    }
                }
                let now = self.clock.now();
                let operation_id = new_run_id(&now)?;
                let operation = OperationRecord {
                    operation_id: operation_id.clone(),
                    dataset: owner.intent.dataset.clone(),
                    created_at: now.clone(),
                    created_by: format!("grv/{}", env!("CARGO_PKG_VERSION")),
                    body: OperationPayload::Publish(PublishPayload {
                        revision: number,
                        previous_revision: predecessor,
                        change_set,
                    }),
                };
                let no_op = before == after;
                let revision = Revision {
                    revision: number,
                    previous_revision: predecessor,
                    operation_id,
                    created_at: now,
                    state: after,
                };
                let revision_file = revision::encode(&revision, parent)?;
                Ok(Candidate {
                    intent: PublicationIntent {
                        dataset: owner.intent.dataset.clone(),
                        operation,
                        lease: owner.clone(),
                    },
                    revision_file,
                    owner: owner.clone(),
                    no_op,
                })
            },
        )?;
        *owner = renewed;
        candidate.owner = owner.clone();
        candidate.intent.lease = owner.clone();
        Ok(candidate)
    }
    /// A no-op is established under the lease after complete conflict checks.
    /// Lease cleanup failure is maintenance; it cannot erase this known result.
    pub fn finish_noop(&self, candidate: Candidate) -> Result<Published> {
        self.finish_noop_authorized(candidate, |_| Ok(()))
    }
    pub fn finish_noop_authorized(
        &self,
        candidate: Candidate,
        persist_outcome: impl FnOnce(Counter) -> Result<()>,
    ) -> Result<Published> {
        if !candidate.no_op {
            return Err(error(
                ErrorCode::InvalidArgument,
                "candidate changes the complete state",
            ));
        }
        let (latest, _) = self.owned(&candidate.owner)?;
        if latest.revision != candidate.owner.latest.revision {
            return Err(error(ErrorCode::StateConflict, "no-op predecessor changed"));
        }
        persist_outcome(latest.revision)?;
        let mut owner = candidate.owner;
        Ok(Published {
            revision: latest.revision,
            maintenance_error: self.release(&mut owner).err(),
        })
    }
    /// The callback must fsync the consumer's operation identity before any
    /// description/revision creation or commit attempt. It may reject safely.
    pub fn commit(
        &self,
        candidate: Candidate,
        persist_intent: impl FnOnce(&PublicationIntent) -> Result<()>,
    ) -> Result<Published> {
        let OperationPayload::Publish(payload) = &candidate.intent.operation.body else {
            unreachable!()
        };
        let (_, renewed) = crate::renewal::during(
            candidate.owner.clone(),
            self.renewal_interval(),
            |owner| self.renew(owner),
            |watch| {
                persist_intent(&candidate.intent)?;
                watch.check()?;
                self.create(
                    &object(
                        &candidate.intent.dataset,
                        &format!(
                            ".states/operations/{}.json",
                            candidate.intent.operation.operation_id
                        ),
                    ),
                    &candidate.intent.operation,
                )?;
                let key = revision::revision_key(&candidate.intent.dataset, payload.revision);
                watch.check()?;
                let mut source = File::open(candidate.revision_file.path())
                    .map_err(|e| public_error(ErrorCode::BackendFailure, e.to_string()))?;
                match self
                    .store
                    .backend
                    .conditional_create(&key, &mut watch.reader(&mut source))
                {
                    Ok(_) => {}
                    Err(e) if e.effect == WriteEffect::MaybeApplied => {
                        let mut expected = File::open(candidate.revision_file.path())
                            .map_err(|e| public_error(ErrorCode::BackendFailure, e.to_string()))?;
                        let mut hash = Sha256::new();
                        let mut buffer = [0; 64 * 1024];
                        let mut size = 0u64;
                        loop {
                            let n = expected.read(&mut buffer).map_err(|e| {
                                public_error(ErrorCode::BackendFailure, e.to_string())
                            })?;
                            if n == 0 {
                                break;
                            }
                            size += n as u64;
                            hash.update(&buffer[..n]);
                        }
                        struct HashSink(Sha256);
                        impl Write for HashSink {
                            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                                self.0.update(bytes);
                                Ok(bytes.len())
                            }
                            fn flush(&mut self) -> std::io::Result<()> {
                                Ok(())
                            }
                        }
                        let mut found = HashSink(Sha256::new());
                        let meta = self
                            .store
                            .backend
                            .get(&key, &mut found)
                            .map_err(backend_error)?;
                        if meta.size.get() != size || found.0.finalize() != hash.finalize() {
                            return Err(error(
                                ErrorCode::IntegrityFailure,
                                "ambiguous revision create has different bytes",
                            ));
                        }
                    }
                    Err(e) => return Err(backend_error(e)),
                }
                Ok(())
            },
        )?;
        if renewed.latest.revision != candidate.owner.latest.revision
            || renewed.latest.high_water != candidate.owner.latest.high_water
            || renewed.latest.pending != candidate.owner.latest.pending
        {
            return Err(error(
                ErrorCode::OwnershipLost,
                "publication reservation changed during renewal",
            ));
        }
        let mut committed = renewed.latest.clone();
        committed.revision = payload.revision;
        committed.lease = None;
        committed.mutation_id = Uuid::v4();
        self.put(&candidate.intent.dataset, &renewed.validator, &committed)?;
        let maintenance_error = if payload.previous_revision.get() != 0 {
            self.supersession(
                &candidate.intent.dataset,
                payload.previous_revision,
                payload.revision,
                &candidate.intent.operation.operation_id,
            )
            .err()
        } else {
            None
        };
        Ok(Published {
            revision: payload.revision,
            maintenance_error,
        })
    }

    pub(crate) fn supersession(
        &self,
        dataset: &Name,
        previous: Counter,
        successor: Counter,
        operation: &RunId,
    ) -> Result<()> {
        let key = object(
            dataset,
            &format!(".states/revisions/revision={previous}/.superseded.json"),
        );
        let record = SupersessionReceipt {
            operation_id: operation.clone(),
            successor,
            observed_at: self.clock.now(),
        };
        match self
            .store
            .backend
            .create_bytes(&key, &encode_record(&record).map_err(backend_error)?)
        {
            Ok(_) => Ok(()),
            Err(e)
                if e.kind == ErrorKind::PreconditionFailed
                    || e.effect == WriteEffect::MaybeApplied =>
            {
                let (found, _): (SupersessionReceipt, _) = self.read(&key)?;
                if found.successor == successor {
                    Ok(())
                } else {
                    Err(error(
                        ErrorCode::IntegrityFailure,
                        "supersession successor differs",
                    ))
                }
            }
            Err(e) => Err(backend_error(e)),
        }
    }
    /// The fresh intent and each progress transition must be durable before
    /// acquisition. A successful CAS fences delayed commits before chain proof.
    pub fn resolve(
        &self,
        intent: &PublicationIntent,
        fresh: &LeaseIntent,
        progress: &mut LeaseProgress,
        persist: impl FnMut(&LeaseProgress) -> Result<()>,
        parent: &Path,
    ) -> Result<Option<Counter>> {
        let resolved =
            self.resolve_authorized(intent, fresh, progress, persist, parent, |_| Ok(()))?;
        if let Some(e) = resolved.maintenance_error {
            return Err(e);
        }
        Ok(resolved.revision)
    }
    #[allow(clippy::too_many_arguments)] // Separate acquisition and outcome durability boundaries.
    pub fn resolve_authorized(
        &self,
        intent: &PublicationIntent,
        fresh: &LeaseIntent,
        progress: &mut LeaseProgress,
        mut persist: impl FnMut(&LeaseProgress) -> Result<()>,
        parent: &Path,
        persist_outcome: impl FnOnce(Option<Counter>) -> Result<()>,
    ) -> Result<Resolved> {
        intent.operation.validate().map_err(backend_error)?;
        if fresh.dataset != intent.dataset
            || intent.operation.dataset != intent.dataset
            || intent.lease.intent.dataset != intent.dataset
        {
            return Err(error(
                ErrorCode::InvalidArgument,
                "resolution evidence belongs to another dataset",
            ));
        }
        let OperationPayload::Publish(payload) = &intent.operation.body else {
            return Err(error(
                ErrorCode::InvalidArgument,
                "publication intent required",
            ));
        };
        let mut owner = self.acquire_inner(fresh, progress, &mut persist, Some(&intent.lease))?;
        self.finish_pending(&mut owner)?;
        let ((revision, maintenance_error), mut owner) = crate::renewal::during(
            owner,
            self.renewal_interval(),
            |owner| self.renew(owner),
            |watch| {
                watch.check()?;
                let committed =
                    revision::committed(self.store, &intent.dataset, payload.revision, parent)?;
                let mut maintenance_error = None;
                if committed {
                    let revision =
                        revision::read(self.store, &intent.dataset, payload.revision, parent)?;
                    if revision.operation_id != intent.operation.operation_id
                        || revision.previous_revision != payload.previous_revision
                    {
                        return Err(error(
                            ErrorCode::IntegrityFailure,
                            "committed publication differs from recorded intent",
                        ));
                    }
                    if payload.previous_revision.get() != 0 {
                        maintenance_error = self
                            .supersession(
                                &intent.dataset,
                                payload.previous_revision,
                                payload.revision,
                                &intent.operation.operation_id,
                            )
                            .err();
                    }
                }
                let revision = committed.then_some(payload.revision);
                persist_outcome(revision)?;
                Ok((revision, maintenance_error))
            },
        )?;
        let cleanup_error = self.release(&mut owner).err();
        Ok(Resolved {
            revision,
            maintenance_error: maintenance_error.or(cleanup_error),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        canonical::Sorter,
        clock::{parse, timestamp},
        store::InitOptions,
    };
    use grv_adapter_api::{Column, TableContract};
    use grv_storage::{LocalBackend, ObjectMeta, ObjectPrefix};
    use std::{
        io::Read,
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
    fn name(text: &str) -> Name {
        Name::new(text).unwrap()
    }
    fn run_id(n: u32) -> RunId {
        RunId::new(format!("01M3KQA080R6Y8C2D9F0G{n:05}")).unwrap()
    }
    // One-shot faults target the final LATEST CAS or the supersession receipt.
    struct FaultBackend {
        inner: LocalBackend,
        fault: AtomicUsize,
        latest_puts: AtomicUsize,
    }
    impl FaultBackend {
        fn arm(&self, mode: usize) {
            self.fault.store(mode, Ordering::SeqCst);
        }
        fn failure(effect: WriteEffect) -> grv_storage::Error {
            let mut e = grv_storage::Error::new(ErrorKind::Io, "injected publication fault");
            e.effect = effect;
            e
        }
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
            if k.as_str().ends_with("/LATEST") {
                let mode = self.fault.load(Ordering::SeqCst);
                if (6..=8).contains(&mode)
                    && self
                        .fault
                        .compare_exchange(mode, 0, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                {
                    if mode == 6 {
                        self.inner.conditional_create(k, s)?;
                    }
                    return Err(if mode == 8 {
                        grv_storage::Error::new(
                            ErrorKind::PreconditionFailed,
                            "injected creation race",
                        )
                    } else {
                        Self::failure(WriteEffect::MaybeApplied)
                    });
                }
            }
            if k.as_str().ends_with(".superseded.json")
                && self
                    .fault
                    .compare_exchange(4, 0, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                return Err(Self::failure(WriteEffect::NoEffect));
            }
            let result = self.inner.conditional_create(k, s)?;
            if k.as_str().ends_with("data.parquet")
                && k.as_str().contains(".states/revisions/")
                && self
                    .fault
                    .compare_exchange(5, 0, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                return Err(Self::failure(WriteEffect::MaybeApplied));
            }
            Ok(result)
        }
        fn conditional_put(
            &self,
            k: &ObjectKey,
            v: &Validator,
            s: &mut dyn Read,
        ) -> grv_storage::Result<Validator> {
            if k.as_str().ends_with("/LATEST") {
                self.latest_puts.fetch_add(1, Ordering::SeqCst);
            }
            let mut bytes = vec![];
            s.read_to_end(&mut bytes).unwrap();
            let commit = k.as_str().ends_with("LATEST")
                && decode_record::<Latest>(&bytes)
                    .is_ok_and(|x| x.revision.get() > 0 && x.lease.is_none());
            let mode = if commit {
                loop {
                    let value = self.fault.load(Ordering::SeqCst);
                    if !(1..=3).contains(&value) {
                        break 0;
                    }
                    if self
                        .fault
                        .compare_exchange(value, 0, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        break value;
                    }
                }
            } else {
                0
            };
            if mode == 1 {
                return Err(Self::failure(WriteEffect::MaybeApplied));
            }
            let result = self.inner.put_bytes(k, v, &bytes)?;
            if mode == 2 || mode == 3 {
                if mode == 3 {
                    let mut latest: Latest = decode_record(&bytes).unwrap();
                    latest.mutation_id = Uuid::v4();
                    self.inner
                        .put_bytes(k, &result, &encode_record(&latest).unwrap())?;
                }
                return Err(Self::failure(WriteEffect::MaybeApplied));
            }
            Ok(result)
        }
    }
    fn store(root: &Path) -> Store<FaultBackend> {
        Store::initialize(
            FaultBackend {
                inner: LocalBackend::open(root).unwrap(),
                fault: AtomicUsize::new(0),
                latest_puts: AtomicUsize::new(0),
            },
            InitOptions::default(),
        )
        .unwrap()
        .0
    }
    #[test]
    fn dataset_initialization_adopts_only_observed_creation_and_preserves_coordination() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let clock = TestClock(AtomicU64::new(0));
        let publisher = Publisher::new(&store, &clock, 900).unwrap();
        let dataset = name("initial");
        let proposal = Latest::empty();
        for mode in [7, 8] {
            store.backend.arm(mode);
            assert_eq!(
                publisher
                    .initialize_dataset(&dataset, &proposal)
                    .unwrap_err()
                    .code,
                ErrorCode::OutcomeUnknown
            );
            assert!(revision::read_latest(&store, &dataset).unwrap().is_none());
        }
        store.backend.arm(6);
        publisher.initialize_dataset(&dataset, &proposal).unwrap();
        let (observed, _) = revision::read_latest(&store, &dataset).unwrap().unwrap();
        assert_eq!(observed, proposal);
        let _owner = publisher
            .acquire(
                publisher
                    .prepare_lease(dataset.clone(), "concurrent".into())
                    .unwrap(),
            )
            .unwrap();
        let before = store
            .backend
            .read_bytes(&revision::latest_key(&dataset), RECORD_LIMIT)
            .unwrap()
            .0;
        publisher
            .initialize_dataset(&dataset, &Latest::empty())
            .unwrap();
        assert_eq!(
            before,
            store
                .backend
                .read_bytes(&revision::latest_key(&dataset), RECORD_LIMIT)
                .unwrap()
                .0
        );
        let mut invalid = Latest::empty();
        invalid.high_water = 1.into();
        assert_eq!(
            publisher
                .initialize_dataset(&name("invalid"), &invalid)
                .unwrap_err()
                .code,
            ErrorCode::InvalidArgument
        );
        assert!(
            revision::read_latest(&store, &name("invalid"))
                .unwrap()
                .is_none()
        );
    }
    #[test]
    fn dataset_initialization_refuses_lost_latest_with_revision_evidence() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let clock = TestClock(AtomicU64::new(0));
        let publisher = Publisher::new(&store, &clock, 900).unwrap();
        let dataset = name("lost");
        store
            .backend
            .create_bytes(
                &revision::revision_key(&dataset, 1.into()),
                b"committed evidence",
            )
            .unwrap();
        assert_eq!(
            publisher
                .initialize_dataset(&dataset, &Latest::empty())
                .unwrap_err()
                .code,
            ErrorCode::ProtocolFailure
        );
        assert_eq!(
            store
                .backend
                .head(&revision::latest_key(&dataset))
                .unwrap_err()
                .kind,
            ErrorKind::NotFound
        );
    }
    #[test]
    fn blocking_intent_fsync_renews_lease_and_commit_uses_only_its_returned_validator() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let clock = TestClock(AtomicU64::new(0));
        let publisher = Publisher::new(&store, &clock, 31).unwrap();
        let run = sealed(&publisher, 1, 0, root.path());
        let mut owner = publisher
            .acquire(
                publisher
                    .prepare_publication_lease(name("data"), "long".into())
                    .unwrap(),
            )
            .unwrap();
        let candidate = publisher
            .prepare(
                &mut owner,
                ChangeSet {
                    runs: vec![run],
                    ..Default::default()
                },
                root.path(),
            )
            .unwrap();
        let before = store.backend.latest_puts.load(Ordering::SeqCst);
        let published = publisher
            .commit(candidate, |_| {
                clock.0.store(31, Ordering::SeqCst);
                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                while store.backend.latest_puts.load(Ordering::SeqCst) == before {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "renewal did not run during blocked intent persistence"
                    );
                    std::thread::yield_now();
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(published.revision.get(), 1);
        assert!(store.backend.latest_puts.load(Ordering::SeqCst) >= 3);
    }
    #[test]
    fn short_publication_combines_acquisition_and_reservation_into_exactly_two_latest_cas() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let clock = TestClock(AtomicU64::new(0));
        let publisher = Publisher::new(&store, &clock, 60).unwrap();
        let run = sealed(&publisher, 1, 0, root.path());
        let intent = publisher
            .prepare_publication_lease(name("data"), "short".into())
            .unwrap();
        let mut owner = publisher
            .acquire_authorized(&intent, &mut LeaseProgress::Prepared, |_| Ok(()))
            .unwrap();
        assert_eq!(store.backend.latest_puts.load(Ordering::SeqCst), 1);
        let candidate = publisher
            .prepare(
                &mut owner,
                ChangeSet {
                    runs: vec![run],
                    ..Default::default()
                },
                root.path(),
            )
            .unwrap();
        assert!(!candidate.is_noop());
        assert_eq!(store.backend.latest_puts.load(Ordering::SeqCst), 1);
        assert_eq!(
            publisher
                .commit(candidate, |_| Ok(()))
                .unwrap()
                .revision
                .get(),
            1
        );
        assert_eq!(store.backend.latest_puts.load(Ordering::SeqCst), 2);
    }
    fn candidate<'a>(publisher: &Publisher<'a, FaultBackend>, parent: &Path) -> Candidate {
        let mut lease = publisher
            .acquire(
                publisher
                    .prepare_lease(name("data"), "test".into())
                    .unwrap(),
            )
            .unwrap();
        publisher
            .prepare(&mut lease, ChangeSet::default(), parent)
            .unwrap()
    }
    fn resolved(
        publisher: &Publisher<'_, FaultBackend>,
        intent: &PublicationIntent,
        parent: &Path,
    ) -> Option<Counter> {
        publisher
            .resolve(
                intent,
                &publisher
                    .prepare_lease(name("data"), "resolver".into())
                    .unwrap(),
                &mut LeaseProgress::Prepared,
                |_| Ok(()),
                parent,
            )
            .unwrap()
    }
    fn sealed(publisher: &Publisher<'_, FaultBackend>, n: u32, base: u64, parent: &Path) -> RunId {
        let ownership = Ownership::new(publisher.store, publisher.clock, 60).unwrap();
        let id = run_id(n);
        let mut owner = ownership
            .prepare_run(
                name("data"),
                id.clone(),
                Counter::new(base).unwrap(),
                vec![],
                None,
            )
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
        let mut sorter = Sorter::new(contract.clone(), parent).unwrap();
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
                    table: name("events"),
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
        id
    }
    #[test]
    fn publication_commits_empty_and_canonical_runs_then_explicit_omission() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let clock = TestClock(AtomicU64::new(0));
        let p = Publisher::new(&store, &clock, 60).unwrap();
        assert_eq!(
            p.commit(candidate(&p, root.path()), |_| Ok(()))
                .unwrap()
                .revision
                .get(),
            1
        );
        let id = sealed(&p, 1, 1, root.path());
        let mut lease = p
            .acquire(p.prepare_lease(name("data"), "test".into()).unwrap())
            .unwrap();
        let c = p
            .prepare(
                &mut lease,
                ChangeSet {
                    runs: vec![id],
                    expected_revision: Some(1.into()),
                    ..Default::default()
                },
                root.path(),
            )
            .unwrap();
        let mut persisted = None;
        let outcome = p
            .commit(c, |i| {
                persisted = Some(serde_json::to_vec(i).unwrap());
                Ok(())
            })
            .unwrap();
        assert_eq!(outcome.revision.get(), 2);
        assert!(outcome.maintenance_error.is_none());
        let intent: PublicationIntent = serde_json::from_slice(&persisted.unwrap()).unwrap();
        assert_eq!(resolved(&p, &intent, root.path()).unwrap().get(), 2);
        let revision = revision::read(&store, &name("data"), 2.into(), root.path()).unwrap();
        assert_eq!(revision.state.len(), 1);
        let mut lease = p
            .acquire(p.prepare_lease(name("data"), "omit".into()).unwrap())
            .unwrap();
        let c = p
            .prepare(
                &mut lease,
                ChangeSet {
                    omissions: vec![Omission {
                        table: name("events"),
                        partition: None,
                    }],
                    ..Default::default()
                },
                root.path(),
            )
            .unwrap();
        assert_eq!(p.commit(c, |_| Ok(())).unwrap().revision.get(), 3);
        assert!(
            revision::read(&store, &name("data"), 3.into(), root.path())
                .unwrap()
                .state
                .is_empty()
        );
    }
    #[test]
    fn consumer_intent_must_be_durable_before_description_revision_or_commit() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let clock = TestClock(AtomicU64::new(0));
        let p = Publisher::new(&store, &clock, 60).unwrap();
        let c = candidate(&p, root.path());
        let operation = c.intent.operation.operation_id.clone();
        let e = p
            .commit(c, |_| {
                Err(error(ErrorCode::BackendFailure, "journal unavailable"))
            })
            .err()
            .unwrap();
        assert_eq!(e.code, ErrorCode::BackendFailure);
        assert_eq!(
            store
                .backend
                .head(&object(
                    &name("data"),
                    &format!(".states/operations/{operation}.json")
                ))
                .unwrap_err()
                .kind,
            ErrorKind::NotFound
        );
        assert_eq!(
            store
                .backend
                .head(&revision::revision_key(&name("data"), 1.into()))
                .unwrap_err()
                .kind,
            ErrorKind::NotFound
        );
        assert_eq!(
            revision::read_latest(&store, &name("data"))
                .unwrap()
                .unwrap()
                .0
                .revision
                .get(),
            0
        );
    }
    #[test]
    fn lost_commit_ack_is_adopted_or_resolved_by_chain_and_unapplied_commit_is_fenced() {
        for mode in [1, 2, 3, 5] {
            let root = tempfile::tempdir().unwrap();
            let store = store(root.path());
            let clock = TestClock(AtomicU64::new(0));
            let p = Publisher::new(&store, &clock, 60).unwrap();
            let c = candidate(&p, root.path());
            let intent = c.intent.clone();
            store.backend.arm(mode);
            let result = p.commit(c, |_| Ok(()));
            if mode == 2 || mode == 5 {
                assert_eq!(result.unwrap().revision.get(), 1);
            } else {
                assert_eq!(result.err().unwrap().code, ErrorCode::OutcomeUnknown);
            }
            assert_eq!(
                resolved(&p, &intent, root.path()).map(|x| x.get()),
                if mode == 1 { None } else { Some(1) }
            );
            if mode == 1 {
                assert!(
                    !revision::committed(&store, &name("data"), 1.into(), root.path()).unwrap()
                );
                assert_eq!(
                    p.commit(candidate(&p, root.path()), |_| Ok(()))
                        .unwrap()
                        .revision
                        .get(),
                    2
                );
            }
        }
    }
    #[test]
    fn takeover_fences_old_candidate_and_never_reuses_reserved_number() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let clock = TestClock(AtomicU64::new(0));
        let p = Publisher::new(&store, &clock, 60).unwrap();
        let c = candidate(&p, root.path());
        clock.0.store(100, Ordering::SeqCst);
        let mut successor = p
            .acquire(p.prepare_lease(name("data"), "successor".into()).unwrap())
            .unwrap();
        assert_eq!(
            p.commit(c, |_| Ok(())).err().unwrap().code,
            ErrorCode::OwnershipLost
        );
        assert!(!revision::committed(&store, &name("data"), 1.into(), root.path()).unwrap());
        let c = p
            .prepare(&mut successor, ChangeSet::default(), root.path())
            .unwrap();
        assert_eq!(p.commit(c, |_| Ok(())).unwrap().revision.get(), 2);
    }
    #[test]
    fn known_commit_survives_receipt_failure_and_replay_repairs_receipt() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let clock = TestClock(AtomicU64::new(0));
        let p = Publisher::new(&store, &clock, 60).unwrap();
        p.commit(candidate(&p, root.path()), |_| Ok(())).unwrap();
        let c = candidate(&p, root.path());
        let intent = c.intent.clone();
        store.backend.arm(4);
        let result = p.commit(c, |_| Ok(())).unwrap();
        assert_eq!(result.revision.get(), 2);
        assert!(result.maintenance_error.is_some());
        assert_eq!(resolved(&p, &intent, root.path()).unwrap().get(), 2);
        let (receipt, _): (SupersessionReceipt, _) = p
            .read(&object(
                &name("data"),
                ".states/revisions/revision=1/.superseded.json",
            ))
            .unwrap();
        assert_eq!(receipt.successor.get(), 2);
    }
    #[test]
    fn lease_journal_retries_exact_cas_and_rejects_overwritten_acquisition() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let clock = TestClock(AtomicU64::new(0));
        let p = Publisher::new(&store, &clock, 60).unwrap();
        let intent = p.prepare_lease(name("data"), "owner".into()).unwrap();
        let mut progress = LeaseProgress::Prepared;
        let mut persisted = None;
        assert_eq!(
            p.acquire_authorized(&intent, &mut progress, |next| {
                if matches!(next, LeaseProgress::Owned { .. }) {
                    return Err(error(ErrorCode::BackendFailure, "stop after acquisition"));
                }
                persisted = Some(serde_json::to_vec(next).unwrap());
                Ok(())
            })
            .err()
            .unwrap()
            .code,
            ErrorCode::BackendFailure
        );
        let mut replay: LeaseProgress = serde_json::from_slice(&persisted.unwrap()).unwrap();
        let mut owner = p
            .acquire_authorized(&intent, &mut replay, |_| Ok(()))
            .unwrap();
        p.release(&mut owner).unwrap();
        let successor = p
            .acquire(p.prepare_lease(name("data"), "successor".into()).unwrap())
            .unwrap();
        let before = store
            .backend
            .read_bytes(&revision::latest_key(&name("data")), RECORD_LIMIT)
            .unwrap()
            .0;
        assert_eq!(
            p.acquire_authorized(&intent, &mut progress, |_| Ok(()))
                .err()
                .unwrap()
                .code,
            ErrorCode::OutcomeUnknown
        );
        assert_eq!(
            before,
            store
                .backend
                .read_bytes(&revision::latest_key(&name("data")), RECORD_LIMIT)
                .unwrap()
                .0
        );
        assert_eq!(
            p.owned(&successor).unwrap().0.lease.unwrap().holder,
            "successor"
        );
    }
    #[test]
    fn publication_rejects_conflicting_runs_unsealed_versions_and_corrupt_new_data() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let clock = TestClock(AtomicU64::new(0));
        let p = Publisher::new(&store, &clock, 60).unwrap();
        let first = sealed(&p, 1, 0, root.path());
        let second = sealed(&p, 2, 0, root.path());
        let mut lease = p
            .acquire(p.prepare_lease(name("data"), "first".into()).unwrap())
            .unwrap();
        let c = p
            .prepare(
                &mut lease,
                ChangeSet {
                    runs: vec![first],
                    ..Default::default()
                },
                root.path(),
            )
            .unwrap();
        p.commit(c, |_| Ok(())).unwrap();
        let mut lease = p
            .acquire(p.prepare_lease(name("data"), "second".into()).unwrap())
            .unwrap();
        assert_eq!(
            p.prepare(
                &mut lease,
                ChangeSet {
                    runs: vec![second],
                    ..Default::default()
                },
                root.path()
            )
            .err()
            .unwrap()
            .code,
            ErrorCode::StateConflict
        );
        p.release(&mut lease).unwrap();
        let third = sealed(&p, 3, 1, root.path());
        std::fs::write(
            root.path()
                .join("datasets/data/events/version=3/data.parquet"),
            b"corrupt",
        )
        .unwrap();
        let mut lease = p
            .acquire(p.prepare_lease(name("data"), "third".into()).unwrap())
            .unwrap();
        assert_eq!(
            p.prepare(
                &mut lease,
                ChangeSet {
                    runs: vec![third],
                    ..Default::default()
                },
                root.path()
            )
            .err()
            .unwrap()
            .code,
            ErrorCode::IntegrityFailure
        );
        assert_eq!(
            revision::read_latest(&store, &name("data"))
                .unwrap()
                .unwrap()
                .0
                .revision
                .get(),
            1
        );
    }
}
