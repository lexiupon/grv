//! Storage-v2 run, reservation and immutable-version ownership. Adapters have
//! no access to this module or its durable ownership tokens.
use crate::{
    canonical::StagedGroup,
    clock::{Clock, ExpiryObservation, expires},
    contract,
    store::{Result, Store, backend_error, public_error},
};
use grv_adapter_api::TableContract;
use grv_storage::{
    Backend, ErrorKind, ListEntry, ListMode, ObjectKey, ObjectPrefix, Validator, WriteEffect,
    model::*,
};
use grv_types::{ErrorCode, Name, RunId, Uuid};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{Read, Write},
    sync::Mutex,
};
const RECORD_LIMIT: usize = 64 * 1024 * 1024;
mod abort;
pub use abort::{AbortClaim, AbortClaimProgress};
fn conflict(message: &str) -> grv_types::PublicError {
    public_error(ErrorCode::StateConflict, message)
}
fn integrity(message: &str) -> grv_types::PublicError {
    public_error(ErrorCode::IntegrityFailure, message)
}
fn unsupported(message: &str) -> grv_types::PublicError {
    public_error(ErrorCode::UnsupportedCapability, message)
}
fn key(value: String) -> ObjectKey {
    ObjectKey::new(value).expect("validated layout path")
}
fn base(dataset: &Name) -> String {
    format!("datasets/{dataset}")
}
fn control_key(dataset: &Name, run: &RunId) -> ObjectKey {
    key(format!("{}/.runs/{run}.control.json", base(dataset)))
}
fn run_key(dataset: &Name, run: &RunId) -> ObjectKey {
    key(format!("{}/.runs/{run}.json", base(dataset)))
}
fn table_base(dataset: &Name, layout: &TableLayout, partition: &Partition) -> Result<String> {
    let p = layout.partition_path(partition).map_err(backend_error)?;
    Ok(format!(
        "{}/{}{}",
        base(dataset),
        layout.table,
        if p.is_empty() {
            String::new()
        } else {
            format!("/{p}")
        }
    ))
}
// Metadata is verified separately from Parquet contents: only an absent manifest
// or failed data confirmation can authorize abandoning an unfinished allocation.
struct VersionMetadata {
    folder: String,
    manifest: VersionManifest,
    baseline: SchemaBaseline,
    pruned: bool,
}
/// Persist this evidence in consumer state before starting source/external work.
/// Serialization is for consumer journals, never public or adapter output.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunOwner {
    dataset: Name,
    control: RunControl,
}
impl RunOwner {
    pub fn dataset(&self) -> &Name {
        &self.dataset
    }
    pub fn control(&self) -> &RunControl {
        &self.control
    }
}
/// Fixed recovery CAS, journaled before taking over a run.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryIntent {
    dataset: Name,
    previous: RunControl,
    control: RunControl,
    expected: Validator,
}
impl RecoveryIntent {
    pub fn dataset(&self) -> &Name {
        &self.dataset
    }
    pub fn control(&self) -> &RunControl {
        &self.control
    }
    pub fn previous(&self) -> &RunControl {
        &self.previous
    }
}
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reservation {
    dataset: Name,
    layout: TableLayout,
    claim: ClaimRecord,
    allocation: AllocationRecord,
}
impl Reservation {
    pub fn allocation(&self) -> &AllocationRecord {
        &self.allocation
    }
    pub fn dataset(&self) -> &Name {
        &self.dataset
    }
    pub fn layout(&self) -> &TableLayout {
        &self.layout
    }
}
/// Pure identity for one acquisition. This token is never reused after an
/// acquisition whose outcome cannot be established from persisted evidence.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReservationIntent {
    dataset: Name,
    run_id: RunId,
    owner_token: OwnerToken,
    layout: TableLayout,
    partition: Partition,
    contract: TableContract,
    claim_token: ClaimToken,
}
/// Protected consumer-journal identity. Citations are fixed before the first
/// allocation effect and cannot be supplied anew when writing a replayed claim.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DerivedReservationIntent {
    reservation: ReservationIntent,
    sources: Vec<SourceReference>,
}
impl DerivedReservationIntent {
    pub fn sources(&self) -> &[SourceReference] {
        &self.sources
    }
}
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DerivedReservation {
    intent: DerivedReservationIntent,
    reservation: Reservation,
}
impl DerivedReservation {
    pub fn sources(&self) -> &[SourceReference] {
        &self.intent.sources
    }
    pub fn reservation(&self) -> &Reservation {
        &self.reservation
    }
    pub fn reservation_mut(&mut self) -> &mut Reservation {
        &mut self.reservation
    }
}
/// Journal before the immutable manifest create. Freezing at this boundary
/// preserves the actual commit preparation time and byte identity on retry.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum DerivedWriteProgress {
    Prepared,
    Commit { manifest: Box<VersionManifest> },
}
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReservationProgress {
    Prepared,
    Acquire {
        claim: ClaimRecord,
        expected: Option<Validator>,
    },
    Acquired {
        claim: ClaimRecord,
        validator: Validator,
    },
    Allocate {
        claim: ClaimRecord,
        expected: Validator,
    },
    Allocated {
        claim: ClaimRecord,
        validator: Validator,
        allocation: AllocationRecord,
    },
    Complete {
        reservation: Reservation,
    },
}
fn journal(
    progress: &mut ReservationProgress,
    next: ReservationProgress,
    persist: &mut impl FnMut(&ReservationProgress) -> Result<()>,
) -> Result<()> {
    persist(&next)?;
    *progress = next;
    Ok(())
}
/// One coordinator retains excessive-expiry observations across retries.
pub struct Ownership<'a, B: Backend> {
    store: &'a Store<B>,
    clock: &'a dyn Clock,
    ttl: u64,
    observations: Mutex<BTreeMap<String, ExpiryObservation>>,
}
impl<'a, B: Backend> Ownership<'a, B> {
    pub fn new(store: &'a Store<B>, clock: &'a dyn Clock, ttl: u64) -> Result<Self> {
        expires(&clock.now(), ttl, &store.parameters)?;
        Ok(Self {
            store,
            clock,
            ttl,
            observations: Mutex::new(BTreeMap::new()),
        })
    }
    pub(crate) fn renewal_interval(&self) -> std::time::Duration {
        let safe = std::time::Duration::from_secs(
            self.ttl - self.store.parameters.max_clock_skew_seconds.get(),
        ) / 3;
        std::time::Duration::from_millis(u64::try_from(safe.as_millis()).unwrap_or(u64::MAX).max(1))
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
    fn absent(&self, path: &ObjectKey) -> Result<bool> {
        match self.store.backend.head(path) {
            Ok(_) => Ok(false),
            Err(e) if e.kind == ErrorKind::NotFound => Ok(true),
            Err(e) => Err(backend_error(e)),
        }
    }
    fn create<T: Serialize + DeserializeOwned + Validate>(
        &self,
        path: &ObjectKey,
        value: &T,
    ) -> Result<Validator> {
        let bytes = encode_record(value).map_err(backend_error)?;
        match self.store.backend.create_bytes(path, &bytes) {
            Ok(v) => Ok(v),
            Err(e)
                if e.kind == ErrorKind::PreconditionFailed
                    || e.effect == WriteEffect::MaybeApplied =>
            {
                let (found, meta) = self
                    .store
                    .backend
                    .read_bytes(path, RECORD_LIMIT)
                    .map_err(backend_error)?;
                let a: Value = serde_json::from_slice(&bytes).unwrap();
                let b: T = decode_record(&found).map_err(backend_error)?;
                if a == serde_json::to_value(b)
                    .map_err(|_| integrity("record serialization failed"))?
                {
                    Ok(meta.validator)
                } else {
                    Err(integrity(
                        "immutable object already contains different content",
                    ))
                }
            }
            Err(e) => Err(backend_error(e)),
        }
    }
    /// Mutable adoption requires this exact mutation id, not mere content or
    /// a stale validator; overwritten transitions remain outcome-unknown.
    fn put<T: Serialize + DeserializeOwned + Validate>(
        &self,
        path: &ObjectKey,
        expected: &Validator,
        value: &T,
    ) -> Result<Validator> {
        let bytes = encode_record(value).map_err(backend_error)?;
        match self.store.backend.put_bytes(path, expected, &bytes) {
            Ok(v) => Ok(v),
            Err(e) if e.effect == WriteEffect::MaybeApplied => {
                let (found, meta) = self
                    .store
                    .backend
                    .read_bytes(path, RECORD_LIMIT)
                    .map_err(backend_error)?;
                let found: T = decode_record(&found).map_err(backend_error)?;
                let mine = serde_json::to_value(value)
                    .map_err(|_| integrity("record serialization failed"))?;
                let found = serde_json::to_value(found)
                    .map_err(|_| integrity("record serialization failed"))?;
                if mine.get("mutation_id") == found.get("mutation_id") {
                    Ok(meta.validator)
                } else {
                    Err(public_error(
                        ErrorCode::OutcomeUnknown,
                        "mutation may have succeeded before a later transition",
                    ))
                }
            }
            Err(e) => Err(backend_error(e)),
        }
    }
    fn expired(
        &self,
        path: &ObjectKey,
        validator: &Validator,
        expiration: &grv_types::Timestamp,
    ) -> bool {
        self.observations
            .lock()
            .unwrap()
            .entry(path.as_str().to_owned())
            .or_default()
            .expired(validator, expiration, self.clock, &self.store.parameters)
    }
    fn live_run(&self, owner: &RunOwner, phase: RunPhase) -> Result<(RunControl, Validator)> {
        let path = control_key(&owner.dataset, &owner.control.run_id);
        let (control, validator): (RunControl, _) = self.read(&path)?;
        if control.owner_token != owner.control.owner_token || control.phase != phase {
            return Err(conflict(
                "run owner was fenced by a phase or ownership transition",
            ));
        }
        Ok((control, validator))
    }
    /// Storage renewal may be safe after expiry while a token remains owned.
    /// A transfer retry has stricter authority: it must not revive an expired
    /// acquisition or open export lifecycle under the same fixed attempt.
    pub fn require_open_transfer(&self, owner: &RunOwner) -> Result<()> {
        let (control, _) = self.live_run(owner, RunPhase::Open).map_err(|error| {
            if matches!(error.code, ErrorCode::StateConflict | ErrorCode::NotFound) {
                public_error(
                    ErrorCode::ExtractionIncomplete,
                    "transfer run was recovered or fenced",
                )
            } else {
                error
            }
        })?;
        if !control.holds_confirmed
            || control.expires_at.as_ref().is_none_or(|expiry| {
                crate::clock::parse(expiry) <= crate::clock::parse(&self.clock.now())
            })
        {
            return Err(public_error(
                ErrorCode::ExtractionIncomplete,
                "expired or unconfirmed run cannot continue a transfer",
            ));
        }
        Ok(())
    }
    /// Pure preparation: the caller durably journals the returned owner before
    /// calling commit_run or acquiring any storage/source resources.
    pub fn prepare_run(
        &self,
        dataset: Name,
        run_id: RunId,
        base_revision: Counter,
        inputs: Vec<RunInput>,
        metadata: Option<BTreeMap<String, Value>>,
    ) -> Result<RunOwner> {
        if inputs.iter().any(|input| input.dataset == dataset) {
            return Err(public_error(
                ErrorCode::InvalidArgument,
                "a run cannot place a dependency hold on its own dataset",
            ));
        }
        let now = self.clock.now();
        let control = RunControl {
            run_id,
            created_at: now.clone(),
            base_revision,
            inputs,
            metadata,
            phase: RunPhase::Open,
            owner_token: OwnerToken::generate(),
            expires_at: Some(expires(&now, self.ttl, &self.store.parameters)?),
            mutation_id: Uuid::v4(),
            holds_confirmed: false,
            sealed_at: None,
            entries: None,
        };
        control.validate().map_err(backend_error)?;
        Ok(RunOwner { dataset, control })
    }
    pub fn commit_run(&self, owner: &RunOwner) -> Result<()> {
        owner.control.validate().map_err(backend_error)?;
        if owner.control.phase != RunPhase::Open || owner.control.holds_confirmed {
            return Err(conflict(
                "only prepared unconfirmed open runs may be created",
            ));
        }
        if owner
            .control
            .inputs
            .iter()
            .any(|input| input.dataset == owner.dataset)
        {
            return Err(public_error(
                ErrorCode::InvalidArgument,
                "a run cannot hold its own dataset",
            ));
        }
        if !self.absent(&key(format!("{}/.retired", base(&owner.dataset))))? {
            return Err(conflict("dataset is retired"));
        }
        self.create(
            &control_key(&owner.dataset, &owner.control.run_id),
            &owner.control,
        )?;
        Ok(())
    }
    /// Retry only the run-creation/hold-confirmation boundary whose original
    /// owner is already durable. This supplies no authority to restart a source.
    pub fn resume_prepared_run(&self, prepared: &RunOwner) -> Result<RunOwner> {
        prepared.control.validate().map_err(backend_error)?;
        if !prepared.control.inputs.is_empty() {
            return Err(unsupported(
                "dependency preparation retry requires explicit durable hold proofs",
            ));
        }
        let path = control_key(&prepared.dataset, &prepared.control.run_id);
        let (control, _) = match self.read::<RunControl>(&path) {
            Ok(found) => found,
            Err(error) if error.code == ErrorCode::NotFound => {
                if prepared.control.expires_at.as_ref().is_none_or(|expiry| {
                    crate::clock::parse(expiry) <= crate::clock::parse(&self.clock.now())
                }) {
                    return Err(public_error(
                        ErrorCode::ExtractionIncomplete,
                        "expired preparation cannot create a transfer run",
                    ));
                }
                self.commit_run(prepared)?;
                self.read(&path)?
            }
            Err(error) => return Err(error),
        };
        if control.owner_token != prepared.control.owner_token || control.phase != RunPhase::Open {
            return Err(public_error(
                ErrorCode::ExtractionIncomplete,
                "prepared run was recovered or fenced",
            ));
        }
        if control.run_id != prepared.control.run_id
            || control.created_at != prepared.control.created_at
            || control.base_revision != prepared.control.base_revision
            || control.inputs != prepared.control.inputs
            || control.metadata != prepared.control.metadata
        {
            return Err(integrity("prepared run's fixed provenance changed"));
        }
        if control.expires_at.as_ref().is_none_or(|expiry| {
            crate::clock::parse(expiry) <= crate::clock::parse(&self.clock.now())
        }) {
            return Err(public_error(
                ErrorCode::ExtractionIncomplete,
                "expired preparation cannot continue a transfer",
            ));
        }
        let mut owner = RunOwner {
            dataset: prepared.dataset.clone(),
            control,
        };
        self.confirm_holds(&mut owner)?;
        self.require_open_transfer(&owner)?;
        Ok(owner)
    }
    #[cfg(test)]
    fn begin_run(
        &self,
        dataset: Name,
        run_id: RunId,
        base_revision: Counter,
        inputs: Vec<RunInput>,
        metadata: Option<BTreeMap<String, Value>>,
    ) -> Result<RunOwner> {
        let owner = self.prepare_run(dataset, run_id, base_revision, inputs, metadata)?;
        self.commit_run(&owner)?;
        Ok(owner)
    }
    pub fn confirm_holds(&self, owner: &mut RunOwner) -> Result<()> {
        let (mut control, validator) = self.live_run(owner, RunPhase::Open)?;
        if !control.inputs.is_empty() {
            return Err(unsupported(
                "dependency runs require typed hold confirmation proofs",
            ));
        }
        if !control.holds_confirmed {
            control.holds_confirmed = true;
            control.mutation_id = Uuid::v4();
            self.put(
                &control_key(&owner.dataset, &control.run_id),
                &validator,
                &control,
            )?;
        }
        owner.control = control;
        Ok(())
    }
    /// Confirm only proofs produced by this store's immutable hold acquisition.
    /// Replacing the owner with the exact CAS result preserves the ownership
    /// fence; the direct-run convenience method cannot bypass this path.
    pub fn confirm_dependency_holds(
        &self,
        owner: &mut RunOwner,
        scratch: &std::path::Path,
        proofs: &[crate::holds::ConfirmedHold<'_>],
    ) -> Result<()> {
        let control = crate::holds::Holds::new(self.store, self.clock, scratch).confirm_run(
            &owner.dataset,
            &owner.control,
            proofs,
        )?;
        owner.control = control;
        Ok(())
    }
    pub fn renew_run(&self, owner: &mut RunOwner) -> Result<()> {
        let (mut control, validator) = self.live_run(owner, owner.control.phase)?;
        if control.phase == RunPhase::Sealed {
            return Err(conflict("sealed runs cannot renew"));
        }
        control.expires_at = Some(expires(
            &self.clock.now(),
            self.ttl,
            &self.store.parameters,
        )?);
        control.mutation_id = Uuid::v4();
        self.put(
            &control_key(&owner.dataset, &control.run_id),
            &validator,
            &control,
        )?;
        owner.control = control;
        Ok(())
    }
    fn register_layout(&self, dataset: &Name, layout: &TableLayout) -> Result<()> {
        layout.validate().map_err(backend_error)?;
        if layout.extensions.as_ref().is_some_and(|v| !v.is_empty()) {
            return Err(unsupported(
                "table extensions have no supported writer lifecycle",
            ));
        }
        self.create(
            &key(format!("{}/{}/.layout.json", base(dataset), layout.table)),
            layout,
        )?;
        Ok(())
    }
    fn register_schema(
        &self,
        dataset: &Name,
        layout: &TableLayout,
        proposed: &TableContract,
    ) -> Result<()> {
        proposed
            .validate()
            .map_err(|e| public_error(ErrorCode::InvalidArgument, e.to_string()))?;
        if proposed.partition_keys != layout.partition_keys
            || !proposed
                .extensions
                .as_object()
                .is_some_and(|v| v.is_empty())
            || !proposed
                .column_ext
                .as_object()
                .is_some_and(|v| v.is_empty())
        {
            return Err(unsupported(
                "schema partition declaration or extension lifecycle is unsupported",
            ));
        }
        for partition_key in &layout.partition_keys {
            let field = format!("_{partition_key}_");
            if !proposed
                .columns
                .iter()
                .any(|v| v.name == field && v.logical_type == Value::String("string".into()))
            {
                return Err(integrity(
                    "partition duplicate column is missing or not a string",
                ));
            }
        }
        let columns: Vec<_> = proposed
            .columns
            .iter()
            .map(|c| StorageColumn {
                name: c.name.clone(),
                logical_type: c.logical_type.clone(),
                ext: None,
            })
            .collect();
        let path = key(format!("{}/{}/.schema.json", base(dataset), layout.table));
        for _ in 0..64 {
            let existing: std::result::Result<(SchemaBaseline, Validator), _> = self.read(&path);
            let (stored, validator) = match existing {
                Ok(v) => v,
                Err(e) if e.code == ErrorCode::NotFound => {
                    let proposed = SchemaBaseline {
                        table: layout.table.clone(),
                        mutation_id: Uuid::v4(),
                        columns: columns.clone(),
                    };
                    let bytes = encode_record(&proposed).map_err(backend_error)?;
                    match self.store.backend.create_bytes(&path, &bytes) {
                        Ok(_) => return Ok(()),
                        Err(e)
                            if e.kind == ErrorKind::PreconditionFailed
                                || e.effect == WriteEffect::MaybeApplied =>
                        {
                            continue;
                        }
                        Err(e) => return Err(backend_error(e)),
                    }
                }
                Err(e) => return Err(e),
            };
            if stored.table != layout.table
                || stored.columns.len() > columns.len()
                || stored
                    .columns
                    .iter()
                    .zip(&columns)
                    .any(|(a, b)| a.name != b.name || a.logical_type != b.logical_type)
            {
                return Err(public_error(
                    ErrorCode::InvalidDeclaration,
                    "proposed schema does not extend the table baseline",
                ));
            }
            if stored
                .columns
                .iter()
                .any(|c| c.ext.as_ref().is_some_and(|v| !v.is_empty()))
            {
                return Err(unsupported(
                    "baseline column extensions have no supported writer lifecycle",
                ));
            }
            if stored.columns.len() == columns.len() {
                return Ok(());
            }
            let mut extended = stored;
            extended
                .columns
                .extend_from_slice(&columns[extended.columns.len()..]);
            extended.mutation_id = Uuid::v4();
            match self.put(&path, &validator, &extended) {
                Ok(_) => return Ok(()),
                Err(e) if e.code == ErrorCode::StateConflict => continue,
                Err(e) => return Err(e),
            }
        }
        Err(conflict("schema registration contended repeatedly"))
    }
    fn validate_write_contract(
        &self,
        dataset: &Name,
        layout: &TableLayout,
        proposed: &TableContract,
    ) -> Result<()> {
        proposed
            .validate()
            .map_err(|e| public_error(ErrorCode::InvalidArgument, e.to_string()))?;
        if proposed.partition_keys != layout.partition_keys {
            return Err(integrity("schema partition keys differ from layout"));
        }
        for k in &layout.partition_keys {
            if !proposed.columns.iter().any(|column| {
                column.name == format!("_{k}_")
                    && column.logical_type == Value::String("string".into())
            }) {
                return Err(integrity(
                    "partition duplicate column is missing or not a string",
                ));
            }
        }
        let path = key(format!("{}/{}/.schema.json", base(dataset), layout.table));
        match self.read::<SchemaBaseline>(&path) {
            Ok((baseline, _)) => {
                if baseline
                    .columns
                    .iter()
                    .any(|column| column.ext.as_ref().is_some_and(|v| !v.is_empty()))
                {
                    return Err(unsupported(
                        "baseline column extensions have no supported writer lifecycle",
                    ));
                }
                if baseline.table != layout.table
                    || baseline.columns.len() > proposed.columns.len()
                    || baseline
                        .columns
                        .iter()
                        .zip(&proposed.columns)
                        .any(|(a, b)| a.name != b.name || a.logical_type != b.logical_type)
                {
                    return Err(public_error(
                        ErrorCode::InvalidDeclaration,
                        "proposed schema does not extend the table baseline",
                    ));
                }
                Ok(())
            }
            Err(e) if e.code == ErrorCode::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
    pub fn prepare_reservation(
        &self,
        owner: &RunOwner,
        layout: TableLayout,
        partition: Partition,
        contract: &TableContract,
    ) -> Result<ReservationIntent> {
        if !owner.control.inputs.is_empty() {
            return Err(if owner.control.holds_confirmed {
                unsupported("derived runs require a citation-aware reservation")
            } else {
                conflict("run holds are not confirmed")
            });
        }
        self.prepare_reservation_inner(owner, layout, partition, contract)
    }
    fn prepare_reservation_inner(
        &self,
        owner: &RunOwner,
        layout: TableLayout,
        partition: Partition,
        contract: &TableContract,
    ) -> Result<ReservationIntent> {
        layout.validate().map_err(backend_error)?;
        layout.partition_path(&partition).map_err(backend_error)?;
        contract
            .validate()
            .map_err(|e| public_error(ErrorCode::InvalidArgument, e.to_string()))?;
        if layout.extensions.as_ref().is_some_and(|v| !v.is_empty())
            || !contract
                .extensions
                .as_object()
                .is_some_and(|v| v.is_empty())
            || !contract
                .column_ext
                .as_object()
                .is_some_and(|v| v.is_empty())
        {
            return Err(unsupported("extensions have no supported writer lifecycle"));
        }
        if contract.partition_keys != layout.partition_keys {
            return Err(integrity("schema partition keys differ from layout"));
        }
        for k in &layout.partition_keys {
            if !contract.columns.iter().any(|column| {
                column.name == format!("_{k}_")
                    && column.logical_type == Value::String("string".into())
            }) {
                return Err(integrity(
                    "partition duplicate column is missing or not a string",
                ));
            }
        }
        Ok(ReservationIntent {
            dataset: owner.dataset.clone(),
            run_id: owner.control.run_id.clone(),
            owner_token: owner.control.owner_token.clone(),
            layout,
            partition,
            contract: contract.clone(),
            claim_token: ClaimToken::generate(),
        })
    }
    pub fn prepare_derived_reservation(
        &self,
        owner: &RunOwner,
        layout: TableLayout,
        partition: Partition,
        contract: &TableContract,
        sources: Vec<SourceReference>,
        scratch: &std::path::Path,
    ) -> Result<DerivedReservationIntent> {
        self.check_derived_sources(owner, &sources, scratch)?;
        Ok(DerivedReservationIntent {
            reservation: self.prepare_reservation_inner(owner, layout, partition, contract)?,
            sources,
        })
    }
    /// V1 build provenance is deliberately broader than storage provenance:
    /// every external run input is cited using a whole-revision selector.
    pub fn prepare_build_reservation(
        &self,
        owner: &RunOwner,
        layout: TableLayout,
        partition: Partition,
        contract: &TableContract,
        scratch: &std::path::Path,
    ) -> Result<DerivedReservationIntent> {
        let sources = owner
            .control
            .inputs
            .iter()
            .map(|input| SourceReference {
                dataset: input.dataset.clone(),
                revision: input.revision,
                retention_id: input.retention_id.clone(),
                table: None,
                partition: None,
            })
            .collect();
        self.prepare_derived_reservation(owner, layout, partition, contract, sources, scratch)
    }
    fn check_derived_sources(
        &self,
        owner: &RunOwner,
        sources: &[SourceReference],
        scratch: &std::path::Path,
    ) -> Result<()> {
        let (control, _) = self.live_run(owner, RunPhase::Open)?;
        if control.inputs != owner.control.inputs
            || control.run_id != owner.control.run_id
            || control.base_revision != owner.control.base_revision
            || control.created_at != owner.control.created_at
            || control.metadata != owner.control.metadata
        {
            return Err(integrity("derived run fixed provenance changed"));
        }
        crate::holds::Holds::new(self.store, self.clock, scratch).check_open_references(
            &owner.dataset,
            &control,
            sources,
        )?;
        self.require_open_transfer(owner)
    }
    pub fn reserve_derived_authorized(
        &self,
        owner: &RunOwner,
        intent: &DerivedReservationIntent,
        progress: &mut ReservationProgress,
        scratch: &std::path::Path,
        mut persist: impl FnMut(&DerivedReservationIntent, &ReservationProgress) -> Result<()>,
    ) -> Result<DerivedReservation> {
        let reservation = self.reserve_inner(
            owner,
            &intent.reservation,
            progress,
            |progress| persist(intent, progress),
            || self.check_derived_sources(owner, &intent.sources, scratch),
        )?;
        Ok(DerivedReservation {
            intent: intent.clone(),
            reservation,
        })
    }
    /// Every callback must durably persist the whole progress value outside GRV
    /// before returning. A failed callback prevents the next storage effect.
    pub fn reserve_authorized(
        &self,
        owner: &RunOwner,
        intent: &ReservationIntent,
        progress: &mut ReservationProgress,
        mut persist: impl FnMut(&ReservationProgress) -> Result<()>,
    ) -> Result<Reservation> {
        self.reserve_inner(owner, intent, progress, &mut persist, || {
            let (control, _) = self.live_run(owner, RunPhase::Open)?;
            if !control.inputs.is_empty() {
                return Err(if control.holds_confirmed {
                    unsupported("derived runs require a citation-aware reservation")
                } else {
                    conflict("run holds are not confirmed")
                });
            }
            Ok(())
        })
    }
    fn reserve_inner(
        &self,
        owner: &RunOwner,
        intent: &ReservationIntent,
        progress: &mut ReservationProgress,
        mut persist: impl FnMut(&ReservationProgress) -> Result<()>,
        check_sources: impl Fn() -> Result<()>,
    ) -> Result<Reservation> {
        if owner.dataset != intent.dataset
            || owner.control.run_id != intent.run_id
            || owner.control.owner_token != intent.owner_token
        {
            return Err(integrity("reservation intent belongs to another run owner"));
        }
        let folder = table_base(&intent.dataset, &intent.layout, &intent.partition)?;
        let path = key(format!("{folder}/.claim"));
        loop {
            check_sources()?;
            match progress.clone() {
                ReservationProgress::Prepared => {
                    let (run, _) = self.live_run(owner, RunPhase::Open)?;
                    if !run.holds_confirmed {
                        return Err(conflict("run holds are not confirmed"));
                    }
                    self.validate_write_contract(
                        &intent.dataset,
                        &intent.layout,
                        &intent.contract,
                    )?;
                    self.register_layout(&intent.dataset, &intent.layout)?;
                    let now = self.clock.now();
                    let mut claim = ClaimRecord {
                        holder: intent.run_id.clone(),
                        token: intent.claim_token.clone(),
                        version: None,
                        high_water: Counter::from(0),
                        mutation_id: Uuid::v4(),
                        claimed_at: Some(now.clone()),
                        expires_at: Some(expires(&now, self.ttl, &self.store.parameters)?),
                        released_at: None,
                        outcome: None,
                        last_release: None,
                    };
                    let expected = match self.read::<ClaimRecord>(&path) {
                        Err(e) if e.code == ErrorCode::NotFound => None,
                        Err(e) => return Err(e),
                        Ok((prior, validator)) => {
                            if prior.token == intent.claim_token {
                                return Err(integrity(
                                    "prepared progress lost prior acquisition evidence",
                                ));
                            }
                            if prior.released_at.is_none()
                                && !self.expired(
                                    &path,
                                    &validator,
                                    prior.expires_at.as_ref().unwrap(),
                                )
                            {
                                return Err(conflict("partition claim has a live holder"));
                            }
                            self.reconcile_release(intent, &prior)?;
                            claim.high_water = prior.high_water;
                            claim.last_release = prior.last_release.clone();
                            if let (Some(version), Some(outcome)) = (prior.version, prior.outcome) {
                                claim.last_release = Some(LastRelease {
                                    token: prior.token,
                                    version,
                                    outcome,
                                });
                            }
                            Some(validator)
                        }
                    };
                    journal(
                        progress,
                        ReservationProgress::Acquire { claim, expected },
                        &mut persist,
                    )?;
                }
                ReservationProgress::Acquire { claim, expected } => {
                    self.validate_progress_claim(intent, &claim, false)?;
                    self.live_run(owner, RunPhase::Open)?;
                    let validator =
                        self.commit_claim_transition(&path, &claim, expected.as_ref())?;
                    journal(
                        progress,
                        ReservationProgress::Acquired { claim, validator },
                        &mut persist,
                    )?;
                }
                ReservationProgress::Acquired {
                    mut claim,
                    validator,
                } => {
                    self.validate_progress_claim(intent, &claim, false)?;
                    self.live_run(owner, RunPhase::Open)?;
                    let (current, current_validator): (ClaimRecord, _) = self.read(&path)?;
                    if current != claim || current_validator != validator {
                        return Err(public_error(
                            ErrorCode::OutcomeUnknown,
                            "acquired claim changed before reservation; its token is never reused",
                        ));
                    }
                    let children = self
                        .store
                        .backend
                        .list(
                            &ObjectPrefix::new(format!("{folder}/")).unwrap(),
                            ListMode::Children,
                        )
                        .map_err(backend_error)?;
                    let mut highest = claim.high_water;
                    for entry in children {
                        if let ListEntry::Prefix(p) = entry
                            && let Some(n) = p
                                .as_str()
                                .strip_prefix(&format!("{folder}/version="))
                                .and_then(|v| v.strip_suffix('/'))
                        {
                            let number = n
                                .parse::<u64>()
                                .ok()
                                .filter(|v| *v > 0 && n == v.to_string())
                                .ok_or_else(|| integrity("noncanonical version directory"))?;
                            highest = highest.max(Counter::new(number).map_err(backend_error)?);
                        }
                    }
                    let version = highest
                        .next()
                        .map_err(|_| conflict("version number space is exhausted"))?;
                    claim.version = Some(version);
                    claim.high_water = version;
                    claim.expires_at = Some(expires(
                        &self.clock.now(),
                        self.ttl,
                        &self.store.parameters,
                    )?);
                    claim.mutation_id = Uuid::v4();
                    journal(
                        progress,
                        ReservationProgress::Allocate {
                            claim,
                            expected: validator,
                        },
                        &mut persist,
                    )?;
                }
                ReservationProgress::Allocate { claim, expected } => {
                    self.validate_progress_claim(intent, &claim, true)?;
                    self.live_run(owner, RunPhase::Open)?;
                    let validator = self.commit_claim_transition(&path, &claim, Some(&expected))?;
                    let allocation = AllocationRecord {
                        run_id: intent.run_id.clone(),
                        table: intent.layout.table.clone(),
                        partition: intent.partition.clone(),
                        version: claim.version.unwrap(),
                        claim_token: claim.token.clone(),
                        state: AllocationState::Allocated,
                        mutation_id: Uuid::v4(),
                    };
                    journal(
                        progress,
                        ReservationProgress::Allocated {
                            claim,
                            validator,
                            allocation,
                        },
                        &mut persist,
                    )?;
                }
                ReservationProgress::Allocated {
                    claim,
                    validator: _,
                    allocation,
                } => {
                    self.validate_progress_claim(intent, &claim, true)?;
                    self.validate_intent_allocation(intent, &claim, &allocation)?;
                    let allocation_path = allocation
                        .claim_token
                        .allocation_key(&intent.dataset, &intent.run_id)
                        .map_err(backend_error)?;
                    let bytes = encode_record(&allocation).map_err(backend_error)?;
                    let recorded = match self.store.backend.create_bytes(&allocation_path, &bytes) {
                        Ok(_) => allocation,
                        Err(e)
                            if e.kind == ErrorKind::PreconditionFailed
                                || e.effect == WriteEffect::MaybeApplied =>
                        {
                            let (found, _): (AllocationRecord, _) = self.read(&allocation_path)?;
                            self.validate_intent_allocation(intent, &claim, &found)?;
                            found
                        }
                        Err(e) => return Err(backend_error(e)),
                    };
                    let reservation = Reservation {
                        dataset: intent.dataset.clone(),
                        layout: intent.layout.clone(),
                        claim,
                        allocation: recorded,
                    };
                    journal(
                        progress,
                        ReservationProgress::Complete { reservation },
                        &mut persist,
                    )?;
                }
                ReservationProgress::Complete { reservation } => {
                    if reservation.dataset != intent.dataset || reservation.layout != intent.layout
                    {
                        return Err(integrity("completed reservation differs from intent"));
                    }
                    self.validate_intent_allocation(
                        intent,
                        &reservation.claim,
                        &reservation.allocation,
                    )?;
                    return Ok(reservation);
                }
            }
        }
    }
    fn validate_progress_claim(
        &self,
        intent: &ReservationIntent,
        claim: &ClaimRecord,
        allocated: bool,
    ) -> Result<()> {
        claim.validate().map_err(backend_error)?;
        if claim.holder != intent.run_id
            || claim.token != intent.claim_token
            || claim.released_at.is_some()
            || claim.version.is_some() != allocated
        {
            return Err(integrity(
                "journaled claim differs from reservation intent/phase",
            ));
        }
        Ok(())
    }
    fn validate_intent_allocation(
        &self,
        intent: &ReservationIntent,
        claim: &ClaimRecord,
        allocation: &AllocationRecord,
    ) -> Result<()> {
        allocation.validate().map_err(backend_error)?;
        if allocation.run_id != intent.run_id
            || allocation.table != intent.layout.table
            || allocation.partition != intent.partition
            || allocation.claim_token != intent.claim_token
            || Some(allocation.version) != claim.version
        {
            return Err(integrity(
                "allocation identity differs from fixed reservation intent",
            ));
        }
        Ok(())
    }
    /// Exact mutation adoption or an unchanged precondition authorizes replay.
    /// Different content cannot establish that this token was never acquired.
    fn commit_claim_transition(
        &self,
        path: &ObjectKey,
        proposed: &ClaimRecord,
        expected: Option<&Validator>,
    ) -> Result<Validator> {
        let current = self.read::<ClaimRecord>(path);
        match current {
            Ok((claim, validator)) if claim == *proposed => return Ok(validator),
            Ok((_, validator)) if Some(&validator) == expected => {}
            Err(e) if e.code == ErrorCode::NotFound && expected.is_none() => {}
            Err(e) if e.code != ErrorCode::NotFound => return Err(e),
            _ => {
                return Err(public_error(
                    ErrorCode::OutcomeUnknown,
                    "claim transition cannot be proved or safely replayed; fixed token is not reused",
                ));
            }
        }
        let bytes = encode_record(proposed).map_err(backend_error)?;
        let result = match expected {
            Some(expected) => self.store.backend.put_bytes(path, expected, &bytes),
            None => self.store.backend.create_bytes(path, &bytes),
        };
        match result {
            Ok(validator) => Ok(validator),
            Err(error)
                if error.kind == ErrorKind::PreconditionFailed
                    || error.effect == WriteEffect::MaybeApplied =>
            {
                let (found, validator): (ClaimRecord, _) = self.read(path)?;
                if found == *proposed {
                    Ok(validator)
                } else {
                    Err(public_error(
                        ErrorCode::OutcomeUnknown,
                        "claim transition was contested or overwritten; acquisition is not repeated",
                    ))
                }
            }
            Err(error) => Err(backend_error(error)),
        }
    }
    fn reconcile_release(&self, intent: &ReservationIntent, prior: &ClaimRecord) -> Result<()> {
        if let (Some(version), Some(outcome)) = (prior.version, prior.outcome) {
            let allocation_path = prior
                .token
                .allocation_key(&intent.dataset, &prior.holder)
                .map_err(backend_error)?;
            match self.read::<AllocationRecord>(&allocation_path) {
                Ok((allocation, _)) => {
                    if allocation.table != intent.layout.table
                        || allocation.partition != intent.partition
                        || allocation.version != version
                        || allocation.claim_token != prior.token
                        || allocation.run_id != prior.holder
                    {
                        return Err(integrity("released allocation identity differs from claim"));
                    }
                    let mut predecessor = Reservation {
                        dataset: intent.dataset.clone(),
                        layout: intent.layout.clone(),
                        claim: prior.clone(),
                        allocation,
                    };
                    self.resolve_allocation(&mut predecessor, outcome)?;
                }
                Err(e) if e.code == ErrorCode::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
    #[cfg(test)]
    fn reserve(
        &self,
        owner: &RunOwner,
        layout: TableLayout,
        partition: Partition,
        contract: &TableContract,
    ) -> Result<Reservation> {
        let intent = self.prepare_reservation(owner, layout, partition, contract)?;
        self.reserve_authorized(owner, &intent, &mut ReservationProgress::Prepared, |_| {
            Ok(())
        })
    }
    fn claim_path(&self, reservation: &Reservation) -> Result<ObjectKey> {
        Ok(key(format!(
            "{}/.claim",
            table_base(
                &reservation.dataset,
                &reservation.layout,
                &reservation.allocation.partition
            )?
        )))
    }
    fn live_claim(&self, reservation: &Reservation) -> Result<(ClaimRecord, Validator)> {
        let (claim, validator): (ClaimRecord, _) = self.read(&self.claim_path(reservation)?)?;
        if claim.holder != reservation.allocation.run_id
            || claim.token != reservation.allocation.claim_token
            || claim.version != Some(reservation.allocation.version)
            || claim.released_at.is_some()
        {
            return Err(public_error(
                ErrorCode::OwnershipLost,
                "version claim was fenced",
            ));
        }
        Ok((claim, validator))
    }
    pub fn renew_claim(&self, reservation: &mut Reservation) -> Result<()> {
        let (mut claim, validator) = self.live_claim(reservation)?;
        claim.expires_at = Some(expires(
            &self.clock.now(),
            self.ttl,
            &self.store.parameters,
        )?);
        claim.mutation_id = Uuid::v4();
        self.put(&self.claim_path(reservation)?, &validator, &claim)?;
        reservation.claim = claim;
        Ok(())
    }
    pub fn write_group(
        &self,
        owner: &RunOwner,
        reservation: &mut Reservation,
        contract: &TableContract,
        staged: &StagedGroup,
        metadata: Option<BTreeMap<String, Value>>,
    ) -> Result<VersionManifest> {
        self.write_group_inner(
            owner,
            reservation,
            contract,
            staged,
            metadata,
            None,
            None,
            None,
            None,
            |_| Ok(()),
        )
    }
    /// This owns the run's scoped renewer as well as the claim's renewer. Do not
    /// wrap it in another run-renewal worker. The caller must durably persist
    /// every writer progress callback; renewal workers join before owner update.
    #[allow(clippy::too_many_arguments)] // Run ownership and independent durable writer progress.
    pub fn write_derived_group(
        &self,
        owner: &mut RunOwner,
        derived: &mut DerivedReservation,
        staged: &StagedGroup,
        metadata: Option<BTreeMap<String, Value>>,
        scratch: &std::path::Path,
        progress: &mut DerivedWriteProgress,
        mut persist: impl FnMut(&DerivedWriteProgress) -> Result<()>,
    ) -> Result<VersionManifest> {
        self.validate_intent_allocation(
            &derived.intent.reservation,
            &derived.reservation.claim,
            &derived.reservation.allocation,
        )?;
        if derived.reservation.dataset != derived.intent.reservation.dataset
            || derived.reservation.layout != derived.intent.reservation.layout
        {
            return Err(integrity(
                "derived reservation differs from immutable intent",
            ));
        }
        let fixed = match progress {
            DerivedWriteProgress::Prepared => None,
            DerivedWriteProgress::Commit { manifest } => {
                manifest.validate().map_err(backend_error)?;
                if manifest.table != derived.reservation.layout.table
                    || manifest.partition != derived.reservation.allocation.partition
                    || manifest.version != derived.reservation.allocation.version
                    || manifest.run_id != owner.control.run_id
                    || manifest.claim_token != derived.reservation.claim.token
                    || manifest.derived_from.as_deref() != Some(derived.intent.sources.as_slice())
                    || manifest.metadata != metadata
                    || manifest.row_count.get() != staged.row_count
                    || manifest.data_files.len() != staged.files.len()
                    || manifest.data_files.iter().zip(&staged.files).any(|(a, b)| {
                        a.name != b.name || a.sha256 != b.sha256 || a.size.get() != b.size
                    })
                {
                    return Err(integrity(
                        "derived commit progress differs from fixed writer identity",
                    ));
                }
                Some((**manifest).clone())
            }
        };
        self.check_derived_sources(owner, &derived.intent.sources, scratch)?;
        let (manifest, renewed_owner) = crate::renewal::during(
            owner.clone(),
            self.renewal_interval(),
            |owner| self.renew_run(owner),
            |run_watch| {
                self.write_group_inner(
                    owner,
                    &mut derived.reservation,
                    &derived.intent.reservation.contract,
                    staged,
                    metadata,
                    Some(&derived.intent.sources),
                    Some(scratch),
                    fixed.as_ref(),
                    Some(run_watch),
                    |manifest| {
                        if fixed.is_none() {
                            let next = DerivedWriteProgress::Commit {
                                manifest: Box::new(manifest.clone()),
                            };
                            persist(&next)?;
                            *progress = next;
                        }
                        Ok(())
                    },
                )
            },
        )?;
        *owner = renewed_owner;
        Ok(manifest)
    }
    #[allow(clippy::too_many_arguments)]
    fn write_group_inner(
        &self,
        owner: &RunOwner,
        reservation: &mut Reservation,
        contract: &TableContract,
        staged: &StagedGroup,
        metadata: Option<BTreeMap<String, Value>>,
        sources: Option<&[SourceReference]>,
        scratch: Option<&std::path::Path>,
        fixed: Option<&VersionManifest>,
        run_watch: Option<&crate::renewal::Watch>,
        mut persist_manifest: impl FnMut(&VersionManifest) -> Result<()>,
    ) -> Result<VersionManifest> {
        let check_citations = || -> Result<()> {
            if let Some(watch) = run_watch {
                watch.check()?;
            }
            if let Some(sources) = sources {
                self.check_derived_sources(
                    owner,
                    sources,
                    scratch.expect("derived writer scratch"),
                )?;
            }
            Ok(())
        };
        check_citations()?;
        let (control, _) = self.live_run(owner, RunPhase::Open)?;
        if sources.is_none() && !control.inputs.is_empty() {
            return Err(unsupported(
                "derived output requires a citation-aware writer lifecycle",
            ));
        }
        if owner.dataset != reservation.dataset
            || owner.control.run_id != reservation.allocation.run_id
        {
            return Err(integrity("reservation belongs to another run"));
        }
        self.renew_claim(reservation)?;
        let mut collision = false;
        let transfer = crate::renewal::during(
            reservation.clone(),
            self.renewal_interval(),
            |reservation| self.renew_claim(reservation),
            |watch| {
                check_citations()?;
                self.register_schema(&reservation.dataset, &reservation.layout, contract)?;
                let mut total_rows = 0u64;
                for (index, file) in staged.files.iter().enumerate() {
                    watch.check()?;
                    check_citations()?;
                    let expected = if index == 0 {
                        "data.parquet".into()
                    } else {
                        format!("data-{index}.parquet")
                    };
                    if file.name != expected {
                        return Err(integrity("staged data files are not contiguous"));
                    }
                    let (size, digest) = hash_file(&file.path)?;
                    if size != file.size || digest != file.sha256.as_str() {
                        return Err(integrity("staged data metadata differs from actual file"));
                    }
                    let rows = verify_parquet(
                        File::open(&file.path).map_err(io_error)?,
                        contract,
                        &reservation.layout,
                        &reservation.allocation.partition,
                    )?;
                    if rows != file.rows {
                        return Err(integrity("staged row count differs from Parquet"));
                    }
                    total_rows = total_rows
                        .checked_add(rows)
                        .ok_or_else(|| integrity("row count overflow"))?;
                }
                if staged.files.is_empty() || total_rows != staged.row_count {
                    return Err(integrity(
                        "staged group has no file or inconsistent row count",
                    ));
                }
                let folder = format!(
                    "{}/version={}",
                    table_base(
                        &reservation.dataset,
                        &reservation.layout,
                        &reservation.allocation.partition
                    )?,
                    reservation.allocation.version
                );
                let mut data_files = vec![];
                for file in &staged.files {
                    watch.check()?;
                    check_citations()?;
                    let path = key(format!("{folder}/{}", file.name));
                    let mut source = File::open(&file.path).map_err(io_error)?;
                    let mut guarded = watch.reader(&mut source);
                    let result = if let Some(run_watch) = run_watch {
                        self.store
                            .backend
                            .conditional_create(&path, &mut run_watch.reader(&mut guarded))
                    } else {
                        self.store.backend.conditional_create(&path, &mut guarded)
                    };
                    let validator = match result {
                        Ok(v) => v,
                        Err(e)
                            if e.kind == ErrorKind::PreconditionFailed
                                || e.effect == WriteEffect::MaybeApplied =>
                        {
                            let (size, digest, validator) = self.hash_object(&path)?;
                            if size != file.size || digest != file.sha256.as_str() {
                                collision = true;
                                return Err(integrity(
                                    "reserved version contains another writer's data; reservation abandoned",
                                ));
                            }
                            validator
                        }
                        Err(e) => return Err(backend_error(e)),
                    };
                    data_files.push(DataFile {
                        name: file.name.clone(),
                        sha256: file.sha256.clone(),
                        size: Counter::new(file.size).map_err(backend_error)?,
                        validator,
                    });
                }
                Ok((folder, data_files))
            },
        );
        let ((folder, data_files), renewed) = match transfer {
            Ok(result) => result,
            Err(error) => {
                if collision {
                    self.release(reservation, ClaimOutcome::Abandoned)?;
                }
                return Err(error);
            }
        };
        *reservation = renewed;
        check_citations()?;
        // Mandatory renewed claim is the immediate authority for commit-marker
        // creation; a stale source or run owner cannot skip this CAS.
        self.renew_claim(reservation)?;
        let manifest = VersionManifest {
            table: reservation.layout.table.clone(),
            partition: reservation.allocation.partition.clone(),
            version: reservation.allocation.version,
            run_id: reservation.allocation.run_id.clone(),
            created_at: fixed
                .map_or_else(|| self.clock.now(), |manifest| manifest.created_at.clone()),
            claim_token: reservation.allocation.claim_token.clone(),
            data_files,
            row_count: Counter::new(staged.row_count).map_err(backend_error)?,
            derived_from: sources.map(<[SourceReference]>::to_vec),
            metadata,
        };
        if fixed.is_some_and(|fixed| fixed != &manifest) {
            return Err(integrity(
                "derived commit payload changed after durable preparation",
            ));
        }
        persist_manifest(&manifest)?;
        let (_, renewed) = crate::renewal::during(
            reservation.clone(),
            self.renewal_interval(),
            |reservation| self.renew_claim(reservation),
            |watch| {
                watch.check()?;
                check_citations()?;
                self.create(&key(format!("{folder}/manifest.json")), &manifest)?;
                self.verify_version(
                    &reservation.dataset,
                    &reservation.layout,
                    &reservation.allocation.partition,
                    reservation.allocation.version,
                )?;
                Ok(())
            },
        )?;
        *reservation = renewed;
        Ok(manifest)
    }
    fn hash_object(&self, path: &ObjectKey) -> Result<(u64, String, Validator)> {
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
        let mut sink = HashSink(Sha256::new());
        let meta = self
            .store
            .backend
            .get(path, &mut sink)
            .map_err(backend_error)?;
        Ok((
            meta.size.get(),
            format!("{:x}", sink.0.finalize()),
            meta.validator,
        ))
    }
    /// Durable full verification supplies recovery evidence, never release or
    /// publication authority by itself.
    pub fn verify_version(
        &self,
        dataset: &Name,
        layout: &TableLayout,
        partition: &Partition,
        version: Counter,
    ) -> Result<VersionManifest> {
        let metadata = self
            .version_metadata(dataset, layout, partition, version)?
            .ok_or_else(|| public_error(ErrorCode::NotFound, "version manifest is absent"))?;
        if metadata.pruned {
            return Err(integrity("version has a durable pruning tombstone"));
        }
        self.verify_version_data(&metadata, layout, partition)?;
        if self.version_pruned(&metadata.folder, layout, partition, version)? {
            return Err(integrity("version was pruned during verification"));
        }
        Ok(metadata.manifest)
    }
    fn version_pruned(
        &self,
        folder: &str,
        layout: &TableLayout,
        partition: &Partition,
        version: Counter,
    ) -> Result<bool> {
        let marker: PrunedMarker = match self.read(&key(format!("{folder}/.pruned"))) {
            Ok((marker, _)) => marker,
            Err(error) if error.code == ErrorCode::NotFound => return Ok(false),
            Err(error) => return Err(error),
        };
        if marker.table != layout.table
            || marker.partition != *partition
            || marker.version != version
        {
            return Err(integrity("pruning marker does not match its version path"));
        }
        Ok(true)
    }
    fn version_metadata(
        &self,
        dataset: &Name,
        layout: &TableLayout,
        partition: &Partition,
        version: Counter,
    ) -> Result<Option<VersionMetadata>> {
        let (recorded_layout, _): (TableLayout, _) = self.read(&key(format!(
            "{}/{}/.layout.json",
            base(dataset),
            layout.table
        )))?;
        if recorded_layout != *layout {
            return Err(integrity(
                "provided layout differs from the durable table layout",
            ));
        }
        let folder = format!(
            "{}/version={version}",
            table_base(dataset, layout, partition)?
        );
        let pruned = self.version_pruned(&folder, layout, partition, version)?;
        let manifest: VersionManifest = match self.read(&key(format!("{folder}/manifest.json"))) {
            Ok((manifest, _)) => manifest,
            Err(error) if error.code == ErrorCode::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        if manifest.table != layout.table
            || manifest.partition != *partition
            || manifest.version != version
        {
            return Err(integrity("version manifest does not match its path"));
        }
        let (baseline, _): (SchemaBaseline, _) = self.read(&key(format!(
            "{}/{}/.schema.json",
            base(dataset),
            layout.table
        )))?;
        if baseline.table != layout.table {
            return Err(integrity("schema baseline table differs from path"));
        }
        Ok(Some(VersionMetadata {
            folder,
            manifest,
            baseline,
            pruned,
        }))
    }
    fn verify_version_data(
        &self,
        metadata: &VersionMetadata,
        layout: &TableLayout,
        partition: &Partition,
    ) -> Result<()> {
        let VersionMetadata {
            folder,
            manifest,
            baseline,
            ..
        } = metadata;
        let listing = self
            .store
            .backend
            .list(
                &ObjectPrefix::new(format!("{folder}/")).unwrap(),
                ListMode::Children,
            )
            .map_err(backend_error)?;
        let listed: Vec<_> = listing
            .iter()
            .filter_map(|entry| match entry {
                ListEntry::Object(path) => path
                    .as_str()
                    .strip_prefix(&format!("{folder}/"))
                    .filter(|name| name.starts_with("data"))
                    .map(str::to_owned),
                ListEntry::Prefix(prefix)
                    if prefix
                        .as_str()
                        .strip_prefix(&format!("{folder}/"))
                        .is_some_and(|v| v.starts_with("data")) =>
                {
                    Some(prefix.as_str().to_owned())
                }
                _ => None,
            })
            .collect();
        let expected: std::collections::BTreeSet<_> =
            manifest.data_files.iter().map(|f| f.name.clone()).collect();
        if listed
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>()
            != expected
        {
            return Err(integrity("version's data file set differs from manifest"));
        }
        let mut row_count = 0u64;
        let mut first_schema = None;
        for file in &manifest.data_files {
            let path = key(format!("{folder}/{}", file.name));
            let mut temporary = tempfile::tempfile().map_err(io_error)?;
            struct CopyHash<'a> {
                file: &'a mut File,
                sha: Sha256,
            }
            impl Write for CopyHash<'_> {
                fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                    self.file.write_all(bytes)?;
                    self.sha.update(bytes);
                    Ok(bytes.len())
                }
                fn flush(&mut self) -> std::io::Result<()> {
                    self.file.flush()
                }
            }
            let mut sink = CopyHash {
                file: &mut temporary,
                sha: Sha256::new(),
            };
            let current = self
                .store
                .backend
                .get(&path, &mut sink)
                .map_err(backend_error)?;
            let digest = format!("{:x}", sink.sha.finalize());
            if current.size != file.size || digest != file.sha256.as_str() {
                return Err(integrity("data file fails manifest hash/size verification"));
            }
            let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                temporary.try_clone().map_err(io_error)?,
            )
            .map_err(|e| integrity(&e.to_string()))?;
            let actual: Vec<_> = builder
                .schema()
                .fields()
                .iter()
                .map(|field| (field.name().clone(), field.data_type().clone()))
                .collect();
            if first_schema
                .as_ref()
                .is_some_and(|schema| schema != &actual)
            {
                return Err(integrity(
                    "version data files have different logical schemas",
                ));
            }
            first_schema = Some(actual.clone());
            if baseline.columns.len() < actual.len() {
                return Err(integrity("version schema exceeds baseline"));
            }
            let physical_contract = TableContract {
                columns: baseline.columns[..actual.len()]
                    .iter()
                    .map(|c| grv_adapter_api::Column {
                        name: c.name.clone(),
                        logical_type: c.logical_type.clone(),
                    })
                    .collect(),
                partition_keys: layout.partition_keys.clone(),
                extensions: serde_json::json!({}),
                column_ext: serde_json::json!({}),
            };
            row_count = row_count
                .checked_add(verify_parquet(
                    temporary,
                    &physical_contract,
                    layout,
                    partition,
                )?)
                .ok_or_else(|| integrity("row count overflow"))?;
        }
        if row_count != manifest.row_count.get() {
            return Err(integrity("version manifest row count differs from data"));
        }
        Ok(())
    }
    fn resolve_allocation(
        &self,
        reservation: &mut Reservation,
        outcome: ClaimOutcome,
    ) -> Result<()> {
        let path = reservation
            .allocation
            .claim_token
            .allocation_key(&reservation.dataset, &reservation.allocation.run_id)
            .map_err(backend_error)?;
        let target = if outcome == ClaimOutcome::Finalized {
            AllocationState::Finalized
        } else {
            AllocationState::Abandoned
        };
        for _ in 0..64 {
            let (mut record, validator): (AllocationRecord, _) = self.read(&path)?;
            if record.run_id != reservation.allocation.run_id
                || record.table != reservation.allocation.table
                || record.partition != reservation.allocation.partition
                || record.version != reservation.allocation.version
                || record.claim_token != reservation.allocation.claim_token
            {
                return Err(integrity("allocation identity changed"));
            }
            if record.state != AllocationState::Allocated {
                reservation.allocation = record;
                return Ok(());
            }
            record.state = target;
            record.mutation_id = Uuid::v4();
            match self.put(&path, &validator, &record) {
                Ok(_) => {
                    reservation.allocation = record;
                    return Ok(());
                }
                Err(e) if e.code == ErrorCode::StateConflict => continue,
                Err(e) => return Err(e),
            }
        }
        Err(conflict("allocation resolution contended repeatedly"))
    }
    pub fn release(&self, reservation: &mut Reservation, outcome: ClaimOutcome) -> Result<()> {
        if outcome == ClaimOutcome::Finalized {
            let manifest = self.verify_version(
                &reservation.dataset,
                &reservation.layout,
                &reservation.allocation.partition,
                reservation.allocation.version,
            )?;
            if manifest.run_id != reservation.allocation.run_id
                || manifest.claim_token != reservation.allocation.claim_token
            {
                return Err(integrity("manifest differs from the releasing claim"));
            }
        }
        let path = self.claim_path(reservation)?;
        let (mut claim, validator): (ClaimRecord, _) = self.read(&path)?;
        if let Some(proven) = reservation
            .allocation
            .release_proof(&claim)
            .map_err(backend_error)?
        {
            return self.resolve_allocation(reservation, proven);
        }
        if claim.holder != reservation.allocation.run_id
            || claim.token != reservation.allocation.claim_token
            || claim.version != Some(reservation.allocation.version)
            || claim.released_at.is_some()
        {
            return Err(public_error(
                ErrorCode::OwnershipLost,
                "release has no matching live claim or durable outcome proof",
            ));
        }
        claim.claimed_at = None;
        claim.expires_at = None;
        claim.released_at = Some(self.clock.now());
        claim.outcome = Some(outcome);
        claim.mutation_id = Uuid::v4();
        if let Err(error) = self.put(&path, &validator, &claim) {
            let (found, _): (ClaimRecord, _) = self.read(&path)?;
            match reservation
                .allocation
                .release_proof(&found)
                .map_err(backend_error)?
            {
                Some(proven) => return self.resolve_allocation(reservation, proven),
                None => return Err(error),
            }
        }
        reservation.claim = claim;
        self.resolve_allocation(reservation, outcome)
    }
    pub fn prepare_run_recovery(&self, dataset: Name, run_id: RunId) -> Result<RecoveryIntent> {
        let path = control_key(&dataset, &run_id);
        let (previous, expected): (RunControl, _) = self.read(&path)?;
        if previous.run_id != run_id {
            return Err(integrity("run control does not match its path"));
        }
        let mut control = previous.clone();
        if control.phase != RunPhase::Sealed {
            if !self.expired(&path, &expected, control.expires_at.as_ref().unwrap()) {
                return Err(conflict("run lease remains live"));
            }
            control.phase = RunPhase::Recovering;
            control.owner_token = OwnerToken::generate();
            control.expires_at = Some(expires(
                &self.clock.now(),
                self.ttl,
                &self.store.parameters,
            )?);
            control.mutation_id = Uuid::v4();
        }
        Ok(RecoveryIntent {
            dataset,
            previous,
            control,
            expected,
        })
    }
    pub fn recover_authorized(&self, intent: &RecoveryIntent) -> Result<RunOwner> {
        intent.previous.validate().map_err(backend_error)?;
        intent.control.validate().map_err(backend_error)?;
        let mut fixed = intent.previous.clone();
        if fixed.phase != RunPhase::Sealed {
            fixed.phase = RunPhase::Recovering;
            fixed.owner_token = intent.control.owner_token.clone();
            fixed.expires_at = intent.control.expires_at.clone();
            fixed.mutation_id = intent.control.mutation_id.clone();
            if fixed.owner_token == intent.previous.owner_token
                || fixed.mutation_id == intent.previous.mutation_id
            {
                return Err(integrity(
                    "recovery must change owner and mutation identity",
                ));
            }
        }
        if fixed != intent.control {
            return Err(integrity("recovery changed immutable run facts"));
        }
        let path = control_key(&intent.dataset, &intent.control.run_id);
        let (found, validator): (RunControl, _) = self.read(&path)?;
        if found != intent.control {
            if found != intent.previous || validator != intent.expected {
                return Err(public_error(
                    ErrorCode::OwnershipLost,
                    "fixed recovery CAS was fenced",
                ));
            }
            if !self.expired(&path, &validator, found.expires_at.as_ref().unwrap()) {
                return Err(conflict("run lease remains live"));
            }
            if intent.control.expires_at.as_ref().is_some_and(|expiry| {
                crate::clock::parse(&self.clock.now()) >= crate::clock::parse(expiry)
            }) {
                return Err(public_error(
                    ErrorCode::OutcomeUnknown,
                    "fixed recovery authority expired before commit",
                ));
            }
            self.put(&path, &intent.expected, &intent.control)?;
        }
        if intent.control.phase == RunPhase::Sealed {
            self.create(
                &run_key(&intent.dataset, &intent.control.run_id),
                &intent.control.sealed_run().map_err(backend_error)?,
            )?;
        }
        Ok(RunOwner {
            dataset: intent.dataset.clone(),
            control: intent.control.clone(),
        })
    }
    pub fn recover_run(&self, dataset: Name, run_id: RunId) -> Result<RunOwner> {
        let intent = self.prepare_run_recovery(dataset, run_id)?;
        self.recover_authorized(&intent)
    }
    fn recover_allocation(
        &self,
        dataset: &Name,
        record: AllocationRecord,
    ) -> Result<AllocationRecord> {
        if record.state != AllocationState::Allocated {
            return Ok(record);
        }
        let layout_path = key(format!("{}/{}/.layout.json", base(dataset), record.table));
        let (layout, _): (TableLayout, _) = self.read(&layout_path)?;
        if layout.table != record.table {
            return Err(integrity("allocation layout does not match table"));
        }
        let claim_path = key(format!(
            "{}/.claim",
            table_base(dataset, &layout, &record.partition)?
        ));
        let claim_result: std::result::Result<(ClaimRecord, Validator), _> = self.read(&claim_path);
        let (claim, validator) = match claim_result {
            Ok(v) => v,
            Err(e) if e.code == ErrorCode::NotFound => {
                return self.mark_unproven(dataset, record);
            }
            Err(e) => return Err(e),
        };
        let mut reservation = Reservation {
            dataset: dataset.clone(),
            layout,
            claim: claim.clone(),
            allocation: record,
        };
        if let Some(outcome) = reservation
            .allocation
            .release_proof(&claim)
            .map_err(backend_error)?
        {
            self.resolve_allocation(&mut reservation, outcome)?;
            return Ok(reservation.allocation);
        }
        if claim.holder != reservation.allocation.run_id
            || claim.token != reservation.allocation.claim_token
            || claim.version != Some(reservation.allocation.version)
            || claim.released_at.is_some()
        {
            return self.mark_unproven(dataset, reservation.allocation);
        }
        if !self.expired(&claim_path, &validator, claim.expires_at.as_ref().unwrap()) {
            return Err(conflict(
                "in-flight partition claim must finish or expire before sealing",
            ));
        }
        let outcome = match self.version_metadata(
            dataset,
            &reservation.layout,
            &reservation.allocation.partition,
            reservation.allocation.version,
        )? {
            None => ClaimOutcome::Abandoned,
            Some(metadata) => {
                if metadata.manifest.run_id != reservation.allocation.run_id
                    || metadata.manifest.claim_token != reservation.allocation.claim_token
                {
                    return Err(integrity(
                        "version manifest does not match allocation ownership",
                    ));
                }
                let outcome = if metadata.pruned {
                    ClaimOutcome::Abandoned
                } else {
                    // Storage-v2 permits damaged pre-finalization data to be
                    // abandoned. Required coordination/identity reads occur
                    // outside this catch, and backend failures never qualify.
                    match self.verify_version_data(
                        &metadata,
                        &reservation.layout,
                        &reservation.allocation.partition,
                    ) {
                        Ok(()) => ClaimOutcome::Finalized,
                        Err(error)
                            if matches!(
                                error.code,
                                ErrorCode::NotFound | ErrorCode::IntegrityFailure
                            ) =>
                        {
                            ClaimOutcome::Abandoned
                        }
                        Err(error) => return Err(error),
                    }
                };
                if self.version_pruned(
                    &metadata.folder,
                    &reservation.layout,
                    &reservation.allocation.partition,
                    reservation.allocation.version,
                )? {
                    ClaimOutcome::Abandoned
                } else {
                    outcome
                }
            }
        };
        self.release(&mut reservation, outcome)?;
        Ok(reservation.allocation)
    }
    fn mark_unproven(
        &self,
        dataset: &Name,
        mut record: AllocationRecord,
    ) -> Result<AllocationRecord> {
        let path = record
            .claim_token
            .allocation_key(dataset, &record.run_id)
            .map_err(backend_error)?;
        for _ in 0..64 {
            let (current, validator): (AllocationRecord, _) = self.read(&path)?;
            if current != record && current.state != AllocationState::Allocated {
                return Ok(current);
            }
            if current.state != AllocationState::Allocated {
                return Ok(current);
            }
            record = current;
            record.state = AllocationState::Unproven;
            record.mutation_id = Uuid::v4();
            match self.put(&path, &validator, &record) {
                Ok(_) => return Ok(record),
                Err(e) if e.code == ErrorCode::StateConflict => continue,
                Err(e) => return Err(e),
            }
        }
        Err(conflict("allocation resolution contended repeatedly"))
    }
    fn finalized_entries(
        &self,
        owner: &RunOwner,
        watch: Option<&crate::renewal::Watch>,
    ) -> Result<Vec<RunEntry>> {
        self.live_run(owner, owner.control.phase)?;
        let control = &owner.control;
        let prefix = ObjectPrefix::new(format!(
            "{}/.runs/{}.allocations/",
            base(&owner.dataset),
            control.run_id
        ))
        .unwrap();
        let records = self
            .store
            .backend
            .list(&prefix, ListMode::Children)
            .map_err(backend_error)?;
        if records.len() > 131_072 {
            return Err(unsupported("allocation recovery exceeds metadata budget"));
        }
        let mut entries = vec![];
        for entry in records {
            if let Some(watch) = watch {
                watch.check()?;
            }
            self.live_run(owner, owner.control.phase)?;
            let path = match entry {
                ListEntry::Object(path) => path,
                ListEntry::Prefix(_) => {
                    return Err(integrity("allocation directory contains a nested prefix"));
                }
            };
            let (record, _): (AllocationRecord, _) = self.read(&path)?;
            if record.run_id != control.run_id
                || record
                    .claim_token
                    .allocation_key(&owner.dataset, &control.run_id)
                    .map_err(backend_error)?
                    != path
            {
                return Err(integrity(
                    "allocation record does not match path/run identity",
                ));
            }
            let record = self.recover_allocation(&owner.dataset, record)?;
            if record.state == AllocationState::Finalized {
                entries.push(RunEntry {
                    table: record.table,
                    partition: record.partition,
                    version: record.version,
                    claim_token: record.claim_token,
                });
            }
        }
        entries.sort_by(|a, b| {
            (&a.table, &a.partition, a.version).cmp(&(&b.table, &b.partition, b.version))
        });
        if let Some(watch) = watch {
            watch.check()?;
        }
        Ok(entries)
    }
    /// Owns one scoped renewal during allocation reads, then joins it before
    /// the final sealing CAS. This never supplies external build completion
    /// or stopped-writer evidence.
    pub fn seal_renewed(&self, owner: &mut RunOwner) -> Result<SealedRun> {
        let (observed, _): (RunControl, _) =
            self.read(&control_key(&owner.dataset, &owner.control.run_id))?;
        if observed.phase == RunPhase::Sealed {
            return self.commit_seal(owner, vec![]);
        }
        let (entries, renewed) = crate::renewal::during(
            owner.clone(),
            self.renewal_interval(),
            |owner| self.renew_run(owner),
            |watch| self.finalized_entries(owner, Some(watch)),
        )?;
        *owner = renewed;
        self.commit_seal(owner, entries)
    }
    pub fn seal(&self, owner: &mut RunOwner) -> Result<SealedRun> {
        let (observed, _): (RunControl, _) =
            self.read(&control_key(&owner.dataset, &owner.control.run_id))?;
        let entries = if observed.phase == RunPhase::Sealed {
            vec![]
        } else {
            self.finalized_entries(owner, None)?
        };
        self.commit_seal(owner, entries)
    }
    fn commit_seal(&self, owner: &mut RunOwner, mut entries: Vec<RunEntry>) -> Result<SealedRun> {
        let path = control_key(&owner.dataset, &owner.control.run_id);
        let (mut control, validator): (RunControl, _) = self.read(&path)?;
        if control.phase == RunPhase::Sealed {
            let sealed = control.sealed_run().map_err(backend_error)?;
            self.create(&run_key(&owner.dataset, &control.run_id), &sealed)?;
            owner.control = control;
            return Ok(sealed);
        }
        if control.owner_token != owner.control.owner_token || control.phase != owner.control.phase
        {
            return Err(public_error(
                ErrorCode::OwnershipLost,
                "run owner cannot seal after takeover",
            ));
        }
        if !control.holds_confirmed {
            entries.clear();
        }
        control.phase = RunPhase::Sealed;
        control.expires_at = None;
        control.sealed_at = Some(self.clock.now());
        control.entries = Some(entries);
        control.mutation_id = Uuid::v4();
        self.put(&path, &validator, &control)?;
        let sealed = control.sealed_run().map_err(backend_error)?;
        self.create(&run_key(&owner.dataset, &control.run_id), &sealed)?;
        owner.control = control;
        Ok(sealed)
    }
}
fn io_error(e: std::io::Error) -> grv_types::PublicError {
    public_error(ErrorCode::BackendFailure, e.to_string())
}
fn hash_file(path: &std::path::Path) -> Result<(u64, String)> {
    let mut file = File::open(path).map_err(io_error)?;
    let mut sha = Sha256::new();
    let mut size = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buffer).map_err(io_error)?;
        if n == 0 {
            break;
        }
        size = size
            .checked_add(n as u64)
            .ok_or_else(|| integrity("file size overflows"))?;
        sha.update(&buffer[..n]);
    }
    Ok((size, format!("{:x}", sha.finalize())))
}
fn verify_parquet(
    file: File,
    expected: &TableContract,
    layout: &TableLayout,
    partition: &Partition,
) -> Result<u64> {
    let schema = contract::arrow_schema(expected).map_err(|e| integrity(&e.to_string()))?;
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| integrity(&e.to_string()))?;
    if schema.fields().len() != builder.schema().fields().len()
        || schema
            .fields()
            .iter()
            .zip(builder.schema().fields())
            .any(|(a, b)| a.name() != b.name() || a.data_type() != b.data_type())
    {
        return Err(integrity(
            "Parquet logical schema differs from registered schema",
        ));
    }
    let mut rows = 0u64;
    let reader = builder
        .with_batch_size(1024)
        .build()
        .map_err(|e| integrity(&e.to_string()))?;
    for batch in reader {
        let batch = batch.map_err(|e| integrity(&e.to_string()))?;
        for k in &layout.partition_keys {
            use arrow_array::Array;
            let index = batch
                .schema()
                .index_of(&format!("_{k}_"))
                .map_err(|_| integrity("partition duplicate column is absent"))?;
            let strings = batch
                .column(index)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| integrity("partition duplicate column is not string"))?;
            let value = partition
                .get(k)
                .ok_or_else(|| integrity("partition key is absent"))?;
            for row in 0..strings.len() {
                if strings.is_null(row) || strings.value(row) != value.as_str() {
                    return Err(integrity("partition duplicate column does not equal path"));
                }
            }
        }
        rows = rows
            .checked_add(batch.num_rows() as u64)
            .ok_or_else(|| integrity("row count overflow"))?;
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        canonical::Sorter,
        clock::{parse, timestamp},
        store::InitOptions,
    };
    use grv_storage::{FaultInjector, FaultPoint, LocalBackend};
    use std::{
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
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
        fn now(&self) -> grv_types::Timestamp {
            let initial = grv_types::Timestamp::new("2026-10-06T00:00:00Z").unwrap();
            timestamp(
                parse(&initial) + chrono::Duration::seconds(self.0.load(Ordering::SeqCst) as i64),
            )
        }
        fn elapsed(&self) -> Duration {
            Duration::from_secs(self.0.load(Ordering::SeqCst))
        }
    }
    fn name(v: &str) -> Name {
        Name::new(v).unwrap()
    }
    fn run_id(n: u32) -> RunId {
        RunId::new(format!("01M3KQA080R6Y8C2D9F0G{n:05}")).unwrap()
    }
    fn layout() -> TableLayout {
        TableLayout {
            table: name("events"),
            partition_keys: vec![],
            extensions: None,
        }
    }
    fn contract() -> TableContract {
        TableContract {
            columns: vec![grv_adapter_api::Column {
                name: "id".into(),
                logical_type: serde_json::json!("int64"),
            }],
            partition_keys: vec![],
            extensions: serde_json::json!({}),
            column_ext: serde_json::json!({}),
        }
    }
    fn staged(parent: &std::path::Path, contract: &TableContract) -> StagedGroup {
        let mut sorter = Sorter::new(contract.clone(), parent).unwrap();
        let schema = crate::contract::arrow_schema(contract).unwrap();
        let values: Vec<Arc<dyn arrow_array::Array>> = contract
            .columns
            .iter()
            .map(|column| {
                if column.logical_type == serde_json::json!("int64") {
                    Arc::new(arrow_array::Int64Array::from(vec![3, 1, 2]))
                        as Arc<dyn arrow_array::Array>
                } else {
                    Arc::new(arrow_array::StringArray::from(vec!["eu", "eu", "eu"]))
                }
            })
            .collect();
        sorter
            .append(&arrow_array::RecordBatch::try_new(schema, values).unwrap())
            .unwrap();
        sorter.finish().unwrap()
    }
    fn store(root: &std::path::Path) -> Store<LocalBackend> {
        Store::initialize(LocalBackend::open(root).unwrap(), InitOptions::default())
            .unwrap()
            .0
    }
    fn begin<B: Backend>(ownership: &Ownership<'_, B>, n: u32) -> RunOwner {
        let mut run = ownership
            .begin_run(name("data"), run_id(n), Counter::from(0), vec![], None)
            .unwrap();
        ownership.confirm_holds(&mut run).unwrap();
        run
    }
    #[test]
    fn expired_transfer_cannot_revive_an_open_run_but_storage_renewal_remains_token_fenced() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let clock = TestClock(AtomicU64::new(0));
        let ownership = Ownership::new(&store, &clock, 60).unwrap();
        let mut run = begin(&ownership, 1);
        ownership.require_open_transfer(&run).unwrap();
        clock.advance(60);
        assert_eq!(
            ownership.require_open_transfer(&run).unwrap_err().code,
            ErrorCode::ExtractionIncomplete
        );
        // Storage-v2 holders may renew a still-owned token; callers cannot use
        // that permission as an implicit transfer retry before this gate.
        ownership.renew_run(&mut run).unwrap();
        ownership.require_open_transfer(&run).unwrap();
    }
    #[test]
    fn canonical_version_commits_releases_and_seals_with_exact_control_fence() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let clock = TestClock(AtomicU64::new(0));
        let ownership = Ownership::new(&store, &clock, 60).unwrap();
        let mut run = begin(&ownership, 1);
        let contract = contract();
        let staged = staged(root.path(), &contract);
        let mut reservation = ownership
            .reserve(&run, layout(), Partition::new(), &contract)
            .unwrap();
        assert!(
            !root
                .path()
                .join("datasets/data/events/.schema.json")
                .exists(),
            "schema mutation must follow durable allocation"
        );
        let manifest = ownership
            .write_group(&run, &mut reservation, &contract, &staged, None)
            .unwrap();
        assert_eq!(manifest.row_count.get(), 3);
        assert_eq!(manifest.version.get(), 1);
        ownership
            .release(&mut reservation, ClaimOutcome::Finalized)
            .unwrap();
        let sealed = ownership.seal(&mut run).unwrap();
        assert_eq!(sealed.entries.len(), 1);
        sealed.validate_control(run.control()).unwrap();
        sealed.validate_manifest(&manifest).unwrap();
        let replay = ownership.seal(&mut run).unwrap();
        assert_eq!(sealed, replay);
        assert_eq!(reservation.allocation.state, AllocationState::Finalized);
    }
    #[test]
    fn crash_after_reservation_never_reuses_number_even_without_version_directory() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let clock = TestClock(AtomicU64::new(0));
        let ownership = Ownership::new(&store, &clock, 60).unwrap();
        let first = begin(&ownership, 1);
        let first_reservation = ownership
            .reserve(&first, layout(), Partition::new(), &contract())
            .unwrap();
        assert_eq!(first_reservation.allocation.version.get(), 1);
        assert!(!root.path().join("datasets/data/events/version=1").exists());
        clock.advance(61);
        let second = begin(&ownership, 2);
        let second_reservation = ownership
            .reserve(&second, layout(), Partition::new(), &contract())
            .unwrap();
        assert_eq!(second_reservation.allocation.version.get(), 2);
        let mut recovery = ownership.recover_run(name("data"), run_id(1)).unwrap();
        let sealed = ownership.seal(&mut recovery).unwrap();
        assert!(sealed.entries.is_empty());
        let (record, _): (AllocationRecord, _) = ownership
            .read(
                &first_reservation
                    .allocation
                    .claim_token
                    .allocation_key(&name("data"), &run_id(1))
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(record.state, AllocationState::Unproven);
    }
    #[test]
    fn live_claim_blocks_sealing_and_expired_committed_task_can_be_recovered() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let clock = TestClock(AtomicU64::new(0));
        let ownership = Ownership::new(&store, &clock, 60).unwrap();
        let mut run = begin(&ownership, 1);
        let contract = contract();
        let staged = staged(root.path(), &contract);
        let mut reservation = ownership
            .reserve(&run, layout(), Partition::new(), &contract)
            .unwrap();
        let manifest = ownership
            .write_group(&run, &mut reservation, &contract, &staged, None)
            .unwrap();
        assert_eq!(
            ownership.seal(&mut run).unwrap_err().code,
            ErrorCode::StateConflict
        );
        clock.advance(61);
        let mut recovery = ownership.recover_run(name("data"), run_id(1)).unwrap();
        assert_eq!(recovery.control.phase, RunPhase::Recovering);
        assert_eq!(
            ownership.renew_run(&mut run).unwrap_err().code,
            ErrorCode::StateConflict
        );
        let sealed = ownership.seal(&mut recovery).unwrap();
        sealed.validate_manifest(&manifest).unwrap();
        assert_eq!(sealed.entries.len(), 1);
    }
    fn unfinished_version<B: Backend>(
        ownership: &Ownership<'_, B>,
        parent: &std::path::Path,
    ) -> (RunOwner, Reservation, VersionManifest) {
        let run = begin(ownership, 1);
        let contract = contract();
        let staged = staged(parent, &contract);
        let mut reservation = ownership
            .reserve(&run, layout(), Partition::new(), &contract)
            .unwrap();
        let manifest = ownership
            .write_group(&run, &mut reservation, &contract, &staged, None)
            .unwrap();
        (run, reservation, manifest)
    }
    fn assert_recovery_refused_without_claim_or_allocation_changes<B: Backend>(
        ownership: &Ownership<'_, B>,
        reservation: &Reservation,
        code: ErrorCode,
    ) {
        let claim_path = key("datasets/data/events/.claim".into());
        let allocation_path = reservation
            .allocation
            .claim_token
            .allocation_key(&name("data"), &run_id(1))
            .unwrap();
        let before_claim = ownership
            .store
            .backend
            .read_bytes(&claim_path, RECORD_LIMIT)
            .unwrap();
        let before_allocation = ownership
            .store
            .backend
            .read_bytes(&allocation_path, RECORD_LIMIT)
            .unwrap();
        assert_eq!(
            ownership
                .recover_allocation(&name("data"), reservation.allocation.clone())
                .unwrap_err()
                .code,
            code
        );
        assert_eq!(
            ownership
                .store
                .backend
                .read_bytes(&claim_path, RECORD_LIMIT)
                .unwrap(),
            before_claim
        );
        assert_eq!(
            ownership
                .store
                .backend
                .read_bytes(&allocation_path, RECORD_LIMIT)
                .unwrap(),
            before_allocation
        );
    }
    #[test]
    fn expired_allocation_without_manifest_abandons_without_requiring_unwritten_baseline() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let clock = TestClock(AtomicU64::new(0));
        let ownership = Ownership::new(&store, &clock, 60).unwrap();
        let run = begin(&ownership, 1);
        let reservation = ownership
            .reserve(&run, layout(), Partition::new(), &contract())
            .unwrap();
        assert!(
            !root
                .path()
                .join("datasets/data/events/.schema.json")
                .exists()
        );
        clock.advance(61);
        let mut recovered = ownership.recover_run(name("data"), run_id(1)).unwrap();
        assert!(ownership.seal(&mut recovered).unwrap().entries.is_empty());
        let (record, _): (AllocationRecord, _) = ownership
            .read(
                &reservation
                    .allocation
                    .claim_token
                    .allocation_key(&name("data"), &run_id(1))
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(record.state, AllocationState::Abandoned);
    }
    #[test]
    fn expired_manifest_without_finalized_release_abandons_missing_or_corrupt_data() {
        for missing in [true, false] {
            let root = tempfile::tempdir().unwrap();
            let store = store(root.path());
            let clock = TestClock(AtomicU64::new(0));
            let ownership = Ownership::new(&store, &clock, 60).unwrap();
            let (_, reservation, manifest) = unfinished_version(&ownership, root.path());
            let data = root.path().join(format!(
                "datasets/data/events/version=1/{}",
                manifest.data_files[0].name
            ));
            if missing {
                std::fs::remove_file(data).unwrap();
            } else {
                std::fs::write(data, b"corrupt parquet").unwrap();
            }
            clock.advance(61);
            let mut recovered = ownership.recover_run(name("data"), run_id(1)).unwrap();
            assert!(ownership.seal(&mut recovered).unwrap().entries.is_empty());
            let (record, _): (AllocationRecord, _) = ownership
                .read(
                    &reservation
                        .allocation
                        .claim_token
                        .allocation_key(&name("data"), &run_id(1))
                        .unwrap(),
                )
                .unwrap();
            assert_eq!(record.state, AllocationState::Abandoned);
            let (claim, _): (ClaimRecord, _) = ownership
                .read(&key("datasets/data/events/.claim".into()))
                .unwrap();
            assert_eq!(claim.outcome, Some(ClaimOutcome::Abandoned));
            assert!(
                root.path()
                    .join("datasets/data/events/version=1/manifest.json")
                    .exists()
            );
        }
    }
    #[test]
    fn required_coordination_corruption_never_releases_or_resolves_allocation() {
        for (path, mutation, expected) in [
            (".layout.json", "missing", ErrorCode::NotFound),
            (".layout.json", "malformed", ErrorCode::IntegrityFailure),
            (".layout.json", "table", ErrorCode::IntegrityFailure),
            (".schema.json", "missing", ErrorCode::NotFound),
            (".schema.json", "malformed", ErrorCode::IntegrityFailure),
            (".schema.json", "table", ErrorCode::IntegrityFailure),
            (
                "version=1/manifest.json",
                "malformed",
                ErrorCode::IntegrityFailure,
            ),
            (
                "version=1/manifest.json",
                "table",
                ErrorCode::IntegrityFailure,
            ),
            (
                "version=1/manifest.json",
                "version",
                ErrorCode::IntegrityFailure,
            ),
            (
                "version=1/manifest.json",
                "run_id",
                ErrorCode::IntegrityFailure,
            ),
            (
                "version=1/manifest.json",
                "claim_token",
                ErrorCode::IntegrityFailure,
            ),
            (
                "version=1/.pruned",
                "malformed",
                ErrorCode::IntegrityFailure,
            ),
            (
                "version=1/.pruned",
                "wrong_marker",
                ErrorCode::IntegrityFailure,
            ),
        ] {
            let root = tempfile::tempdir().unwrap();
            let store = store(root.path());
            let clock = TestClock(AtomicU64::new(0));
            let ownership = Ownership::new(&store, &clock, 60).unwrap();
            let (_, reservation, _) = unfinished_version(&ownership, root.path());
            let target = root.path().join(format!("datasets/data/events/{path}"));
            match mutation {
                "missing" => std::fs::remove_file(target).unwrap(),
                "malformed" => std::fs::write(target, b"{broken").unwrap(),
                "wrong_marker" => {
                    let marker = PrunedMarker {
                        operation_id: run_id(9),
                        pruned_by: "test".into(),
                        pruned_at: clock.now(),
                        table: name("other"),
                        partition: Partition::new(),
                        version: 1.into(),
                    };
                    std::fs::write(target, encode_record(&marker).unwrap()).unwrap();
                }
                field => {
                    let mut value: Value =
                        serde_json::from_slice(&std::fs::read(&target).unwrap()).unwrap();
                    value[field] = match field {
                        "table" => serde_json::json!("other"),
                        "version" => serde_json::json!(2),
                        "run_id" => serde_json::json!(run_id(9)),
                        "claim_token" => serde_json::json!(ClaimToken::generate()),
                        _ => unreachable!(),
                    };
                    std::fs::write(target, serde_json::to_vec(&value).unwrap()).unwrap();
                }
            }
            clock.advance(61);
            assert_recovery_refused_without_claim_or_allocation_changes(
                &ownership,
                &reservation,
                expected,
            );
        }
    }
    struct FailingDataRead {
        inner: LocalBackend,
        armed: std::sync::atomic::AtomicBool,
    }
    impl Backend for FailingDataRead {
        fn get(
            &self,
            path: &ObjectKey,
            sink: &mut dyn Write,
        ) -> grv_storage::Result<grv_storage::ObjectMeta> {
            if self.armed.load(Ordering::SeqCst) && path.as_str().contains("/version=1/data") {
                return Err(grv_storage::Error::new(
                    ErrorKind::Io,
                    "transient data read failure",
                ));
            }
            self.inner.get(path, sink)
        }
        fn head(&self, path: &ObjectKey) -> grv_storage::Result<grv_storage::ObjectMeta> {
            self.inner.head(path)
        }
        fn list(
            &self,
            prefix: &ObjectPrefix,
            mode: ListMode,
        ) -> grv_storage::Result<Vec<ListEntry>> {
            self.inner.list(prefix, mode)
        }
        fn delete(&self, path: &ObjectKey) -> grv_storage::Result<()> {
            self.inner.delete(path)
        }
        fn conditional_create(
            &self,
            path: &ObjectKey,
            source: &mut dyn Read,
        ) -> grv_storage::Result<Validator> {
            self.inner.conditional_create(path, source)
        }
        fn conditional_put(
            &self,
            path: &ObjectKey,
            expected: &Validator,
            source: &mut dyn Read,
        ) -> grv_storage::Result<Validator> {
            self.inner.conditional_put(path, expected, source)
        }
    }
    #[test]
    fn transient_data_backend_failure_preserves_allocation_for_successful_retry() {
        let root = tempfile::tempdir().unwrap();
        let store = Store {
            backend: FailingDataRead {
                inner: LocalBackend::open(root.path()).unwrap(),
                armed: std::sync::atomic::AtomicBool::new(false),
            },
            parameters: StoreParameters::default(),
        };
        let clock = TestClock(AtomicU64::new(0));
        let ownership = Ownership::new(&store, &clock, 60).unwrap();
        let (_, reservation, _) = unfinished_version(&ownership, root.path());
        clock.advance(61);
        store.backend.armed.store(true, Ordering::SeqCst);
        assert_recovery_refused_without_claim_or_allocation_changes(
            &ownership,
            &reservation,
            ErrorCode::BackendFailure,
        );
        store.backend.armed.store(false, Ordering::SeqCst);
        assert_eq!(
            ownership
                .recover_allocation(&name("data"), reservation.allocation)
                .unwrap()
                .state,
            AllocationState::Finalized
        );
    }
    #[test]
    fn valid_pruning_marker_abandons_only_unfinalized_allocation() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let clock = TestClock(AtomicU64::new(0));
        let ownership = Ownership::new(&store, &clock, 60).unwrap();
        let (_, reservation, _) = unfinished_version(&ownership, root.path());
        let marker = PrunedMarker {
            operation_id: run_id(9),
            pruned_by: "test".into(),
            pruned_at: clock.now(),
            table: name("events"),
            partition: Partition::new(),
            version: 1.into(),
        };
        std::fs::write(
            root.path().join("datasets/data/events/version=1/.pruned"),
            encode_record(&marker).unwrap(),
        )
        .unwrap();
        clock.advance(61);
        assert_eq!(
            ownership
                .recover_allocation(&name("data"), reservation.allocation)
                .unwrap()
                .state,
            AllocationState::Abandoned
        );
    }
    #[test]
    fn terminal_release_evidence_survives_data_damage_and_never_salvages_abandonment() {
        for outcome in [ClaimOutcome::Finalized, ClaimOutcome::Abandoned] {
            for carried in [false, true] {
                let root = tempfile::tempdir().unwrap();
                let store = store(root.path());
                let clock = TestClock(AtomicU64::new(0));
                let ownership = Ownership::new(&store, &clock, 60).unwrap();
                let (_, mut reservation, manifest) = unfinished_version(&ownership, root.path());
                let allocated = reservation.allocation.clone();
                ownership.release(&mut reservation, outcome).unwrap();
                // Emulate a crash after the release became durable but before
                // the allocation CAS. A later acquirer may carry last_release.
                let path = allocated
                    .claim_token
                    .allocation_key(&name("data"), &run_id(1))
                    .unwrap();
                let (_, validator): (AllocationRecord, _) = ownership.read(&path).unwrap();
                ownership.put(&path, &validator, &allocated).unwrap();
                if carried {
                    let successor = begin(&ownership, 2);
                    ownership
                        .reserve(&successor, layout(), Partition::new(), &contract())
                        .unwrap();
                }
                if outcome == ClaimOutcome::Finalized {
                    std::fs::remove_file(root.path().join(format!(
                        "datasets/data/events/version=1/{}",
                        manifest.data_files[0].name
                    )))
                    .unwrap();
                }
                clock.advance(61);
                let mut recovered = ownership.recover_run(name("data"), run_id(1)).unwrap();
                let sealed = ownership.seal(&mut recovered).unwrap();
                assert_eq!(
                    sealed.entries.len(),
                    usize::from(outcome == ClaimOutcome::Finalized)
                );
                let (record, _): (AllocationRecord, _) = ownership.read(&path).unwrap();
                assert_eq!(
                    record.state,
                    if outcome == ClaimOutcome::Finalized {
                        AllocationState::Finalized
                    } else {
                        AllocationState::Abandoned
                    }
                );
                assert_eq!(
                    ownership
                        .recover_allocation(&name("data"), record.clone())
                        .unwrap(),
                    record
                );
            }
        }
    }
    #[test]
    fn stale_claim_cannot_write_or_finalize_under_successor_claim() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let clock = TestClock(AtomicU64::new(0));
        let ownership = Ownership::new(&store, &clock, 60).unwrap();
        let first = begin(&ownership, 1);
        let contract = contract();
        let staged = staged(root.path(), &contract);
        let mut old = ownership
            .reserve(&first, layout(), Partition::new(), &contract)
            .unwrap();
        clock.advance(61);
        let second = begin(&ownership, 2);
        let next = ownership
            .reserve(&second, layout(), Partition::new(), &contract)
            .unwrap();
        assert_eq!(
            ownership
                .write_group(&first, &mut old, &contract, &staged, None)
                .unwrap_err()
                .code,
            ErrorCode::OwnershipLost
        );
        assert!(!root.path().join("datasets/data/events/version=1").exists());
        assert_eq!(next.allocation.version.get(), 2);
    }
    struct LostEveryInstall;
    impl FaultInjector for LostEveryInstall {
        fn check(&self, point: FaultPoint) -> std::io::Result<()> {
            if point == FaultPoint::AfterInstall {
                Err(std::io::Error::other("ack lost after visibility"))
            } else {
                Ok(())
            }
        }
    }
    #[test]
    fn lost_ack_at_each_conditional_transition_is_adopted_by_durable_record_proof() {
        let root = tempfile::tempdir().unwrap();
        let store = Store {
            backend: LocalBackend::open(root.path())
                .unwrap()
                .with_faults(Arc::new(LostEveryInstall)),
            parameters: StoreParameters::default(),
        };
        let clock = TestClock(AtomicU64::new(0));
        let ownership = Ownership::new(&store, &clock, 60).unwrap();
        let mut run = begin(&ownership, 1);
        let contract = contract();
        let staged = staged(root.path(), &contract);
        let mut reservation = ownership
            .reserve(&run, layout(), Partition::new(), &contract)
            .unwrap();
        let manifest = ownership
            .write_group(&run, &mut reservation, &contract, &staged, None)
            .unwrap();
        ownership
            .release(&mut reservation, ClaimOutcome::Finalized)
            .unwrap();
        let sealed = ownership.seal(&mut run).unwrap();
        sealed.validate_manifest(&manifest).unwrap();
    }
    #[test]
    fn data_collision_abandons_reservation_and_never_seals_an_entry() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let clock = TestClock(AtomicU64::new(0));
        let ownership = Ownership::new(&store, &clock, 60).unwrap();
        let mut run = begin(&ownership, 1);
        let contract = contract();
        let staged = staged(root.path(), &contract);
        let mut reservation = ownership
            .reserve(&run, layout(), Partition::new(), &contract)
            .unwrap();
        store
            .backend
            .create_bytes(
                &key("datasets/data/events/version=1/data.parquet".into()),
                b"foreign writer content",
            )
            .unwrap();
        assert_eq!(
            ownership
                .write_group(&run, &mut reservation, &contract, &staged, None)
                .unwrap_err()
                .code,
            ErrorCode::IntegrityFailure
        );
        assert_eq!(reservation.allocation.state, AllocationState::Abandoned);
        assert!(ownership.seal(&mut run).unwrap().entries.is_empty());
    }
    #[test]
    fn extensions_and_unconfirmed_dependency_runs_are_refused_before_claims() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let clock = TestClock(AtomicU64::new(0));
        let ownership = Ownership::new(&store, &clock, 60).unwrap();
        let run = begin(&ownership, 1);
        let mut extended = layout();
        extended.extensions = Some(BTreeMap::from([(
            "encryption".into(),
            serde_json::json!({}),
        )]));
        assert_eq!(
            ownership
                .reserve(&run, extended, Partition::new(), &contract())
                .unwrap_err()
                .code,
            ErrorCode::UnsupportedCapability
        );
        assert!(!root.path().join("datasets/data/events").exists());
        let mut derived = ownership
            .begin_run(
                name("data"),
                run_id(2),
                Counter::from(0),
                vec![RunInput {
                    dataset: name("source"),
                    revision: Counter::from(1),
                    retention_id: Uuid::v4(),
                }],
                None,
            )
            .unwrap();
        assert_eq!(
            ownership.confirm_holds(&mut derived).unwrap_err().code,
            ErrorCode::UnsupportedCapability
        );
        assert_eq!(
            ownership
                .reserve(&derived, layout(), Partition::new(), &contract())
                .unwrap_err()
                .code,
            ErrorCode::StateConflict
        );
        assert!(!root.path().join("datasets/data/events").exists());
        assert!(
            root.path()
                .join(format!("datasets/data/.runs/{}.control.json", run_id(2)))
                .exists()
        );
    }
    #[test]
    fn partition_duplicate_values_must_match_path_before_destination_file_creation() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let clock = TestClock(AtomicU64::new(0));
        let ownership = Ownership::new(&store, &clock, 60).unwrap();
        let run = begin(&ownership, 1);
        let mut contract = contract();
        contract.partition_keys = vec![name("region")];
        contract.columns.push(grv_adapter_api::Column {
            name: "_region_".into(),
            logical_type: serde_json::json!("string"),
        });
        let staged = staged(root.path(), &contract);
        let layout = TableLayout {
            table: name("events"),
            partition_keys: vec![name("region")],
            extensions: None,
        };
        let partition = BTreeMap::from([(name("region"), name("us"))]);
        let mut reservation = ownership
            .reserve(&run, layout, partition, &contract)
            .unwrap();
        assert_eq!(
            ownership
                .write_group(&run, &mut reservation, &contract, &staged, None)
                .unwrap_err()
                .code,
            ErrorCode::IntegrityFailure
        );
        assert!(
            !root
                .path()
                .join("datasets/data/events/region=us/version=1")
                .exists()
        );
    }
    #[test]
    fn irreversible_run_materialization_rejects_mismatched_existing_commit_marker() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let clock = TestClock(AtomicU64::new(0));
        let ownership = Ownership::new(&store, &clock, 60).unwrap();
        let mut run = begin(&ownership, 1);
        let sealed = ownership.seal(&mut run).unwrap();
        let path = root
            .path()
            .join(format!("datasets/data/.runs/{}.json", run.control.run_id));
        let mut forged = sealed;
        forged.base_revision = Counter::from(1);
        std::fs::write(path, encode_record(&forged).unwrap()).unwrap();
        assert_eq!(
            ownership.seal(&mut run).unwrap_err().code,
            ErrorCode::IntegrityFailure
        );
    }
    #[test]
    fn prepared_owner_is_pure_persistable_and_commit_replays_exact_identity() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let clock = TestClock(AtomicU64::new(0));
        let ownership = Ownership::new(&store, &clock, 60).unwrap();
        let owner = ownership
            .prepare_run(name("data"), run_id(1), Counter::from(0), vec![], None)
            .unwrap();
        assert!(!root.path().join("datasets").exists());
        let journal = serde_json::to_vec(&owner).unwrap();
        let restored: RunOwner = serde_json::from_slice(&journal).unwrap();
        ownership.commit_run(&restored).unwrap();
        ownership.commit_run(&restored).unwrap();
        let replacement = ownership
            .prepare_run(name("data"), run_id(1), Counter::from(0), vec![], None)
            .unwrap();
        assert_eq!(
            ownership.commit_run(&replacement).unwrap_err().code,
            ErrorCode::IntegrityFailure
        );
    }
    #[test]
    fn prepared_retry_adopts_hold_confirmation_but_never_revives_expired_or_sealed_runs() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let clock = TestClock(AtomicU64::new(0));
        let ownership = Ownership::new(&store, &clock, 60).unwrap();
        let prepared = ownership
            .prepare_run(name("data"), run_id(1), 0.into(), vec![], None)
            .unwrap();
        let mut owner = ownership.resume_prepared_run(&prepared).unwrap();
        assert!(owner.control.holds_confirmed);
        let adopted = ownership.resume_prepared_run(&prepared).unwrap();
        assert_eq!(adopted.control, owner.control);
        clock.0.store(61, Ordering::SeqCst);
        assert_eq!(
            ownership.resume_prepared_run(&prepared).unwrap_err().code,
            ErrorCode::ExtractionIncomplete
        );
        // Storage renewal remains legal for a still-owned token, but a sealed
        // run is never permission to reopen preparation.
        ownership.renew_run(&mut owner).unwrap();
        ownership.seal(&mut owner).unwrap();
        assert_eq!(
            ownership.resume_prepared_run(&prepared).unwrap_err().code,
            ErrorCode::ExtractionIncomplete
        );
        let absent = ownership
            .prepare_run(name("data"), run_id(2), 0.into(), vec![], None)
            .unwrap();
        clock.0.store(122, Ordering::SeqCst);
        assert_eq!(
            ownership.resume_prepared_run(&absent).unwrap_err().code,
            ErrorCode::ExtractionIncomplete
        );
        assert!(
            !root
                .path()
                .join(format!("datasets/data/.runs/{}/control.json", run_id(2)))
                .exists()
        );
    }
    struct ReleaseOverwritten {
        inner: LocalBackend,
        armed: std::sync::atomic::AtomicBool,
    }
    impl Backend for ReleaseOverwritten {
        fn get(
            &self,
            path: &ObjectKey,
            sink: &mut dyn Write,
        ) -> grv_storage::Result<grv_storage::ObjectMeta> {
            self.inner.get(path, sink)
        }
        fn head(&self, path: &ObjectKey) -> grv_storage::Result<grv_storage::ObjectMeta> {
            self.inner.head(path)
        }
        fn list(
            &self,
            prefix: &ObjectPrefix,
            mode: ListMode,
        ) -> grv_storage::Result<Vec<ListEntry>> {
            self.inner.list(prefix, mode)
        }
        fn delete(&self, path: &ObjectKey) -> grv_storage::Result<()> {
            self.inner.delete(path)
        }
        fn conditional_create(
            &self,
            path: &ObjectKey,
            source: &mut dyn Read,
        ) -> grv_storage::Result<Validator> {
            self.inner.conditional_create(path, source)
        }
        fn conditional_put(
            &self,
            path: &ObjectKey,
            expected: &Validator,
            source: &mut dyn Read,
        ) -> grv_storage::Result<Validator> {
            let mut bytes = vec![];
            source.read_to_end(&mut bytes).unwrap();
            let validator = self.inner.put_bytes(path, expected, &bytes)?;
            if path.as_str().ends_with("/.claim") {
                let mut claim: ClaimRecord = decode_record(&bytes)?;
                if claim.outcome == Some(ClaimOutcome::Finalized)
                    && self.armed.swap(false, Ordering::SeqCst)
                {
                    claim.last_release = Some(LastRelease {
                        token: claim.token.clone(),
                        version: claim.version.unwrap(),
                        outcome: ClaimOutcome::Finalized,
                    });
                    claim.holder = run_id(9);
                    claim.token = ClaimToken::generate();
                    claim.version = None;
                    claim.released_at = None;
                    claim.outcome = None;
                    claim.claimed_at =
                        Some(grv_types::Timestamp::new("2026-10-06T00:00:00Z").unwrap());
                    claim.expires_at =
                        Some(grv_types::Timestamp::new("2026-10-06T00:01:00Z").unwrap());
                    claim.mutation_id = Uuid::v4();
                    self.inner
                        .put_bytes(path, &validator, &encode_record(&claim)?)?;
                    let mut error = grv_storage::Error::new(
                        ErrorKind::Io,
                        "release ACK lost; later claimant acquired",
                    );
                    error.effect = WriteEffect::MaybeApplied;
                    return Err(error);
                }
            }
            Ok(validator)
        }
    }
    #[test]
    fn release_ack_lost_after_takeover_is_proven_by_last_release() {
        let root = tempfile::tempdir().unwrap();
        let store = Store {
            backend: ReleaseOverwritten {
                inner: LocalBackend::open(root.path()).unwrap(),
                armed: std::sync::atomic::AtomicBool::new(true),
            },
            parameters: StoreParameters::default(),
        };
        let clock = TestClock(AtomicU64::new(0));
        let ownership = Ownership::new(&store, &clock, 60).unwrap();
        let mut run = begin(&ownership, 1);
        let contract = contract();
        let staged = staged(root.path(), &contract);
        let mut reservation = ownership
            .reserve(&run, layout(), Partition::new(), &contract)
            .unwrap();
        let manifest = ownership
            .write_group(&run, &mut reservation, &contract, &staged, None)
            .unwrap();
        ownership
            .release(&mut reservation, ClaimOutcome::Finalized)
            .unwrap();
        assert_eq!(reservation.allocation.state, AllocationState::Finalized);
        ownership
            .seal(&mut run)
            .unwrap()
            .validate_manifest(&manifest)
            .unwrap();
    }
    #[test]
    fn released_takeover_reconciles_predecessor_allocation_before_evidence_changes() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let clock = TestClock(AtomicU64::new(0));
        let ownership = Ownership::new(&store, &clock, 60).unwrap();
        let first = begin(&ownership, 1);
        let reservation = ownership
            .reserve(&first, layout(), Partition::new(), &contract())
            .unwrap();
        let path = ownership.claim_path(&reservation).unwrap();
        let (mut claim, validator): (ClaimRecord, _) = ownership.read(&path).unwrap();
        claim.claimed_at = None;
        claim.expires_at = None;
        claim.released_at = Some(clock.now());
        claim.outcome = Some(ClaimOutcome::Abandoned);
        claim.mutation_id = Uuid::v4();
        ownership.put(&path, &validator, &claim).unwrap();
        let second = begin(&ownership, 2);
        let successor = ownership
            .reserve(&second, layout(), Partition::new(), &contract())
            .unwrap();
        assert_eq!(successor.allocation.version.get(), 2);
        let (resolved, _): (AllocationRecord, _) = ownership
            .read(
                &reservation
                    .allocation
                    .claim_token
                    .allocation_key(&name("data"), &run_id(1))
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(resolved.state, AllocationState::Abandoned);
    }
    #[test]
    fn allocation_finalized_after_seal_is_orphaned_and_never_added_to_run() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let clock = TestClock(AtomicU64::new(0));
        let ownership = Ownership::new(&store, &clock, 60).unwrap();
        let mut run = begin(&ownership, 1);
        let sealed = ownership.seal(&mut run).unwrap();
        assert!(sealed.entries.is_empty());
        let late = AllocationRecord {
            run_id: run.control.run_id.clone(),
            table: name("events"),
            partition: Partition::new(),
            version: Counter::from(1),
            claim_token: ClaimToken::generate(),
            state: AllocationState::Finalized,
            mutation_id: Uuid::v4(),
        };
        ownership
            .create(
                &late
                    .claim_token
                    .allocation_key(&name("data"), &run_id(1))
                    .unwrap(),
                &late,
            )
            .unwrap();
        assert!(ownership.seal(&mut run).unwrap().entries.is_empty());
        assert_eq!(
            ownership
                .reserve(&run, layout(), Partition::new(), &contract())
                .unwrap_err()
                .code,
            ErrorCode::StateConflict
        );
    }
    fn progress_name(progress: &ReservationProgress) -> &'static str {
        match progress {
            ReservationProgress::Prepared => "prepared",
            ReservationProgress::Acquire { .. } => "acquire",
            ReservationProgress::Acquired { .. } => "acquired",
            ReservationProgress::Allocate { .. } => "allocate",
            ReservationProgress::Allocated { .. } => "allocated",
            ReservationProgress::Complete { .. } => "complete",
        }
    }
    #[test]
    fn reservation_journal_replays_each_crash_boundary_without_burning_another_number() {
        for crash_phase in ["acquire", "acquired", "allocate", "allocated", "complete"] {
            let root = tempfile::tempdir().unwrap();
            let store = store(root.path());
            let clock = TestClock(AtomicU64::new(0));
            let ownership = Ownership::new(&store, &clock, 60).unwrap();
            let run = begin(&ownership, 1);
            let intent = ownership
                .prepare_reservation(&run, layout(), Partition::new(), &contract())
                .unwrap();
            assert!(!root.path().join("datasets/data/events").exists());
            let mut protected_journal = vec![];
            let mut progress = ReservationProgress::Prepared;
            let error = ownership
                .reserve_authorized(&run, &intent, &mut progress, |next| {
                    protected_journal = serde_json::to_vec(next).unwrap();
                    if progress_name(next) == crash_phase {
                        Err(public_error(
                            ErrorCode::BackendFailure,
                            "consumer crashed immediately after journal fsync",
                        ))
                    } else {
                        Ok(())
                    }
                })
                .unwrap_err();
            assert_eq!(error.code, ErrorCode::BackendFailure);
            if crash_phase == "acquire" {
                assert!(!root.path().join("datasets/data/events/.claim").exists());
            }
            if crash_phase == "allocated" {
                assert!(
                    !root
                        .path()
                        .join(
                            intent
                                .claim_token
                                .allocation_key(&name("data"), &run_id(1))
                                .unwrap()
                                .as_str()
                        )
                        .exists()
                );
            }
            let mut restored: ReservationProgress =
                serde_json::from_slice(&protected_journal).unwrap();
            let reservation = ownership
                .reserve_authorized(&run, &intent, &mut restored, |_| Ok(()))
                .unwrap();
            assert_eq!(reservation.allocation.version.get(), 1);
            let replay = ownership
                .reserve_authorized(&run, &intent, &mut restored, |_| {
                    panic!("completed intent must not mutate journal or storage")
                })
                .unwrap();
            assert_eq!(
                replay.allocation.claim_token,
                reservation.allocation.claim_token
            );
        }
    }
    #[test]
    fn overwritten_acquisition_intent_returns_unknown_and_never_reuses_token() {
        for crash_phase in ["acquire", "acquired", "allocate"] {
            let root = tempfile::tempdir().unwrap();
            let store = store(root.path());
            let clock = TestClock(AtomicU64::new(0));
            let ownership = Ownership::new(&store, &clock, 60).unwrap();
            let first = begin(&ownership, 1);
            let intent = ownership
                .prepare_reservation(&first, layout(), Partition::new(), &contract())
                .unwrap();
            let mut protected_journal = vec![];
            ownership
                .reserve_authorized(
                    &first,
                    &intent,
                    &mut ReservationProgress::Prepared,
                    |next| {
                        protected_journal = serde_json::to_vec(next).unwrap();
                        if progress_name(next) == crash_phase {
                            Err(public_error(ErrorCode::BackendFailure, "crash"))
                        } else {
                            Ok(())
                        }
                    },
                )
                .unwrap_err();
            clock.advance(61);
            let second = begin(&ownership, 2);
            let successor = ownership
                .reserve(&second, layout(), Partition::new(), &contract())
                .unwrap();
            let before = store
                .backend
                .read_bytes(&ownership.claim_path(&successor).unwrap(), RECORD_LIMIT)
                .unwrap();
            let mut restored: ReservationProgress =
                serde_json::from_slice(&protected_journal).unwrap();
            let error = ownership
                .reserve_authorized(&first, &intent, &mut restored, |_| Ok(()))
                .unwrap_err();
            assert_eq!(error.code, ErrorCode::OutcomeUnknown);
            let after = store
                .backend
                .read_bytes(&ownership.claim_path(&successor).unwrap(), RECORD_LIMIT)
                .unwrap();
            assert_eq!(before, after);
            assert!(
                !root
                    .path()
                    .join(
                        intent
                            .claim_token
                            .allocation_key(&name("data"), &run_id(1))
                            .unwrap()
                            .as_str()
                    )
                    .exists()
            );
        }
    }
    #[test]
    fn recorded_number_survives_takeover_before_allocation_record_creation() {
        let root = tempfile::tempdir().unwrap();
        let store = store(root.path());
        let clock = TestClock(AtomicU64::new(0));
        let ownership = Ownership::new(&store, &clock, 60).unwrap();
        let first = begin(&ownership, 1);
        let intent = ownership
            .prepare_reservation(&first, layout(), Partition::new(), &contract())
            .unwrap();
        let mut protected_journal = vec![];
        ownership
            .reserve_authorized(
                &first,
                &intent,
                &mut ReservationProgress::Prepared,
                |next| {
                    protected_journal = serde_json::to_vec(next).unwrap();
                    if progress_name(next) == "allocated" {
                        Err(public_error(
                            ErrorCode::BackendFailure,
                            "crash before allocation object",
                        ))
                    } else {
                        Ok(())
                    }
                },
            )
            .unwrap_err();
        clock.advance(61);
        let second = begin(&ownership, 2);
        let successor = ownership
            .reserve(&second, layout(), Partition::new(), &contract())
            .unwrap();
        assert_eq!(successor.allocation.version.get(), 2);
        let mut restored: ReservationProgress = serde_json::from_slice(&protected_journal).unwrap();
        let mut old = ownership
            .reserve_authorized(&first, &intent, &mut restored, |_| Ok(()))
            .unwrap();
        assert_eq!(old.allocation.version.get(), 1);
        assert_eq!(
            ownership.renew_claim(&mut old).unwrap_err().code,
            ErrorCode::OwnershipLost
        );
        let mut recovery = ownership.recover_run(name("data"), run_id(1)).unwrap();
        assert!(ownership.seal(&mut recovery).unwrap().entries.is_empty());
        let (record, _): (AllocationRecord, _) = ownership
            .read(
                &old.allocation
                    .claim_token
                    .allocation_key(&name("data"), &run_id(1))
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(record.state, AllocationState::Unproven);
    }
}
