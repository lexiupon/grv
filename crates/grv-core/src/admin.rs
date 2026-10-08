//! Lease-fenced retention decisions. An immutable description is never authority:
//! effects require a durable pending observation, and unknown commits are fenced
//! by a fresh lease before marker-based resolution.
use crate::{
    clock::{Clock, new_run_id, parse},
    publication::{LeaseIntent, LeaseOwner, LeaseProgress, Publisher},
    retention, revision,
    store::{Result, Store, backend_error, public_error},
};
use grv_storage::{Backend, ListEntry, ListMode, ObjectKey, ObjectPrefix, Validator, model::*};
use grv_types::{ErrorCode, Name, PublicError, RunId, Uuid};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::path::Path;

mod protection;
pub use protection::{Assessment, Protection, ProtectionKind, ProtectionObject};

fn invalid(message: &str) -> PublicError {
    public_error(ErrorCode::IntegrityFailure, message)
}
fn conflict(message: &str) -> PublicError {
    public_error(ErrorCode::StateConflict, message)
}
fn object(dataset: &Name, suffix: &str) -> ObjectKey {
    ObjectKey::new(format!("datasets/{dataset}/{suffix}")).expect("checked admin path")
}
fn operation_path(operation: &OperationRecord) -> String {
    format!(".states/operations/{}.json", operation.operation_id)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminIntent {
    operation: OperationRecord,
}
impl AdminIntent {
    pub fn operation(&self) -> &OperationRecord {
        &self.operation
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum AdminProgress {
    Prepared,
    Compare {
        latest: Box<Latest>,
        expected: Validator,
    },
    Complete {
        outcome: Box<AdminOutcome>,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminOutcome {
    pub operation_id: RunId,
    pub committed: bool,
    pub noop: bool,
    pub markers: Vec<ObjectKey>,
    pub excluded_pruned: Vec<VersionTarget>,
    pub pin: Option<PinRecord>,
    pub release: Option<PinReleaseMarker>,
    pub maintenance_error: Option<PublicError>,
}
#[derive(Debug)]
pub struct AdminResolution {
    pub committed: bool,
    pub observed: Option<AdminOutcome>,
    pub maintenance_error: Option<PublicError>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PruneRequest {
    pub dataset: Name,
    pub targets: Vec<VersionTarget>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum PruneProgress {
    Prepared,
    Intent {
        intent: Box<AdminIntent>,
        progress: Box<AdminProgress>,
        protected: Vec<VersionTarget>,
        already_pruned: Vec<VersionTarget>,
    },
    Decision {
        proposal: Box<AdminIntent>,
        decision: Box<AdminIntent>,
        progress: Box<AdminProgress>,
        protected: Vec<VersionTarget>,
        already_pruned: Vec<VersionTarget>,
    },
    Complete {
        outcome: Box<PruneOutcome>,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PruneOutcome {
    pub pruned: Vec<VersionTarget>,
    pub protected: Vec<VersionTarget>,
    pub deleted: Vec<ObjectKey>,
    pub cleanup_complete: Vec<VersionTarget>,
    pub completed_operation_ids: Vec<RunId>,
    pub maintenance_error: Option<PublicError>,
}
pub struct Admin<'a, B: Backend> {
    pub(crate) store: &'a Store<B>,
    pub(crate) clock: &'a dyn Clock,
    pub(crate) publisher: Publisher<'a, B>,
}
impl<'a, B: Backend> Admin<'a, B> {
    pub fn new(store: &'a Store<B>, clock: &'a dyn Clock, ttl: u64) -> Result<Self> {
        Ok(Self {
            store,
            clock,
            publisher: Publisher::new(store, clock, ttl)?,
        })
    }
    fn prepare(&self, dataset: Name, body: OperationPayload) -> Result<AdminIntent> {
        let operation = OperationRecord {
            operation_id: new_run_id(&self.clock.now())?,
            dataset,
            created_at: self.clock.now(),
            created_by: format!("grv/{}", env!("CARGO_PKG_VERSION")),
            body,
        };
        operation.validate().map_err(backend_error)?;
        Ok(AdminIntent { operation })
    }
    pub fn prepare_pin(
        &self,
        dataset: Name,
        scope: PinScope,
        pin_id: Uuid,
        reason: Option<String>,
    ) -> Result<AdminIntent> {
        self.prepare(
            dataset,
            OperationPayload::Pin(PinPayload {
                pin_id,
                scope,
                reason,
            }),
        )
    }
    pub fn prepare_revision_pin(
        &self,
        dataset: Name,
        revision: Counter,
        pin_id: Uuid,
        reason: String,
    ) -> Result<AdminIntent> {
        if reason.is_empty() || revision.get() == 0 {
            return Err(public_error(
                ErrorCode::InvalidArgument,
                "revision pin requires a positive revision and nonempty reason",
            ));
        }
        self.prepare_pin(
            dataset,
            PinScope::Revision(RevisionScope { revision }),
            pin_id,
            Some(reason),
        )
    }
    pub fn prepare_unpin(
        &self,
        dataset: Name,
        scope: PinScope,
        pin_id: Uuid,
    ) -> Result<AdminIntent> {
        self.prepare(
            dataset,
            OperationPayload::Unpin(PinPayload {
                pin_id,
                scope,
                reason: None,
            }),
        )
    }
    pub fn prepare_revision_unpin(
        &self,
        dataset: Name,
        revision: Counter,
        pin_id: Uuid,
    ) -> Result<AdminIntent> {
        if revision.get() == 0 {
            return Err(public_error(
                ErrorCode::InvalidArgument,
                "unpin requires a positive revision",
            ));
        }
        self.prepare_unpin(
            dataset,
            PinScope::Revision(RevisionScope { revision }),
            pin_id,
        )
    }
    /// Source GC alone commits irreversible releases, after checking every
    /// exact consumer run for stable absence of unpruned citations.
    pub fn prepare_release_holds(
        &self,
        dataset: Name,
        releases: Vec<HoldRelease>,
    ) -> Result<AdminIntent> {
        if releases.is_empty() || releases.len() > 131_072 {
            return Err(public_error(
                ErrorCode::InvalidArgument,
                "hold release requires bounded nonempty targets",
            ));
        }
        self.prepare(
            dataset,
            OperationPayload::ReleaseHold(ReleaseHoldPayload { releases }),
        )
    }
    fn release_hold_record(&self, dataset: &Name, release: &HoldRelease) -> Result<HoldRecord> {
        let path = object(
            dataset,
            &format!(
                ".holds/{}/revision={}/{}.json",
                release.consumer_dataset, release.revision, release.retention_id
            ),
        );
        let hold: HoldRecord = self.read(&path)?;
        if hold.dataset != *dataset
            || hold.revision != release.revision
            || hold.retention_id != release.retention_id
            || hold.target_dataset != release.consumer_dataset
            || crate::holds::hold_key(&hold) != path
        {
            return Err(invalid(
                "release target differs from exact source hold path",
            ));
        }
        Ok(hold)
    }
    pub(crate) fn read<T: DeserializeOwned + Validate>(&self, path: &ObjectKey) -> Result<T> {
        self.publisher.read(path).map(|(value, _)| value)
    }
    pub(crate) fn optional<T: DeserializeOwned + Validate>(
        &self,
        path: &ObjectKey,
    ) -> Result<Option<T>> {
        match self.read(path) {
            Ok(value) => Ok(Some(value)),
            Err(error) if error.code == ErrorCode::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }
    fn pin_records(
        &self,
        dataset: &Name,
        payload: &PinPayload,
    ) -> Result<(Option<PinRecord>, Option<PinReleaseMarker>)> {
        let pin = self.optional::<PinRecord>(&retention::pin_path(
            self.store,
            dataset,
            &payload.scope,
            &payload.pin_id,
            false,
        )?)?;
        let release = self.optional::<PinReleaseMarker>(&retention::pin_path(
            self.store,
            dataset,
            &payload.scope,
            &payload.pin_id,
            true,
        )?)?;
        if let Some(pin) = &pin {
            if pin.pin_id != payload.pin_id || pin.scope != payload.scope {
                return Err(invalid("pin body differs from addressed scope and ID"));
            }
            let description: OperationRecord = self.read(&object(
                dataset,
                &format!(".states/operations/{}.json", pin.operation_id),
            ))?;
            if description.dataset != *dataset
                || description.operation_id != pin.operation_id
                || !matches!(&description.body,OperationPayload::Pin(original) if original.pin_id==pin.pin_id && original.scope==pin.scope && original.reason==pin.reason)
            {
                return Err(invalid("pin creation audit does not match its operation"));
            }
        }
        if let Some(release) = &release {
            if release.pin_id != payload.pin_id || pin.is_none() {
                return Err(invalid("pin release has no matching scoped pin"));
            }
            let description: OperationRecord = self.read(&object(
                dataset,
                &format!(".states/operations/{}.json", release.operation_id),
            ))?;
            if description.dataset != *dataset
                || description.operation_id != release.operation_id
                || !matches!(&description.body,OperationPayload::Unpin(original) if original.pin_id==payload.pin_id && original.scope==payload.scope)
            {
                return Err(invalid("pin release audit does not match its operation"));
            }
        }
        Ok((pin, release))
    }
    fn excluded(&self, dataset: &Name, scope: &PinScope) -> Result<Vec<VersionTarget>> {
        let (table, partition) = match scope {
            PinScope::Table(v) => (&v.table, None),
            PinScope::Partition(v) => (&v.table, Some(&v.partition)),
            _ => return Ok(vec![]),
        };
        let prefix = ObjectPrefix::new(format!("datasets/{dataset}/{table}/")).unwrap();
        let mut targets = vec![];
        for item in self
            .store
            .backend
            .list(&prefix, ListMode::Recursive)
            .map_err(backend_error)?
        {
            let ListEntry::Object(path) = item else {
                return Err(invalid("recursive pin listing returned a prefix"));
            };
            if !path.as_str().ends_with("/.pruned") {
                continue;
            }
            let marker: PrunedMarker = self.read(&path)?;
            if marker.table != *table {
                return Err(invalid("pin exclusion tombstone differs from table path"));
            }
            let layout: TableLayout =
                self.read(&object(dataset, &format!("{table}/.layout.json")))?;
            let encoded = layout
                .partition_path(&marker.partition)
                .map_err(backend_error)?;
            let folder = if encoded.is_empty() {
                table.to_string()
            } else {
                format!("{table}/{encoded}")
            };
            if path
                != object(
                    dataset,
                    &format!("{folder}/version={}/.pruned", marker.version),
                )
            {
                return Err(invalid("pin exclusion tombstone differs from version path"));
            }
            if partition.is_none_or(|partition| *partition == marker.partition) {
                targets.push(VersionTarget {
                    table: marker.table,
                    partition: marker.partition,
                    version: marker.version,
                });
            }
            if targets.len() > 131_072 {
                return Err(public_error(
                    ErrorCode::UnsupportedCapability,
                    "pin exclusions exceed metadata budget",
                ));
            }
        }
        Ok(targets)
    }
    fn validate(&self, operation: &OperationRecord, parent: &Path) -> Result<Option<AdminOutcome>> {
        if let OperationPayload::ReleaseHold(payload) = &operation.body {
            let holds = crate::holds::Holds::new(self.store, self.clock, parent);
            let mut active = false;
            for release in &payload.releases {
                let hold = self.release_hold_record(&operation.dataset, release)?;
                if holds.active(&hold)? {
                    active = true;
                    if !holds.releasable(&hold)? {
                        return Err(conflict(
                            "source hold still protects an unsealed run or unpruned citation",
                        ));
                    }
                }
            }
            if !active {
                return self.observed(operation, false).map(Some);
            }
            return Ok(None);
        }
        let payload = match &operation.body {
            OperationPayload::Pin(v) | OperationPayload::Unpin(v) => v,
            _ => {
                return Err(public_error(
                    ErrorCode::UnsupportedCapability,
                    "admin operation lifecycle is not implemented for this kind",
                ));
            }
        };
        let (pin, release) = self.pin_records(&operation.dataset, payload)?;
        if matches!(operation.body, OperationPayload::Unpin(_)) {
            if pin.is_none() {
                return Err(public_error(
                    ErrorCode::NotFound,
                    "pin ID does not exist in the addressed scope",
                ));
            }
            if release.is_some() {
                return Ok(Some(self.outcome(
                    operation,
                    false,
                    true,
                    vec![],
                    pin,
                    release,
                )));
            }
            return Ok(None);
        }
        if release.is_some() {
            return Err(conflict("released pin ID cannot be reused in its scope"));
        }
        if let Some(pin) = pin {
            if pin.reason != payload.reason {
                return Err(conflict("active pin reason differs from fixed identity"));
            }
            return Ok(Some(self.outcome(
                operation,
                false,
                true,
                vec![],
                Some(pin),
                None,
            )));
        }
        match &payload.scope {
            PinScope::Revision(scope) => {
                if !revision::committed(self.store, &operation.dataset, scope.revision, parent)? {
                    return Err(conflict("pin revision is not committed"));
                }
                let revision =
                    revision::read(self.store, &operation.dataset, scope.revision, parent)?;
                for entry in revision.state.values() {
                    self.publisher
                        .publishable(&operation.dataset, entry, parent)?;
                }
            }
            PinScope::Version(scope) => {
                let layout: TableLayout = self.read(&object(
                    &operation.dataset,
                    &format!("{}/.layout.json", scope.table),
                ))?;
                let partition = layout
                    .partition_path(&scope.partition)
                    .map_err(backend_error)?;
                let folder = if partition.is_empty() {
                    scope.table.to_string()
                } else {
                    format!("{}/{partition}", scope.table)
                };
                let manifest: VersionManifest = self.read(&object(
                    &operation.dataset,
                    &format!("{folder}/version={}/manifest.json", scope.version),
                ))?;
                self.publisher.publishable(
                    &operation.dataset,
                    &revision::Entry {
                        table: scope.table.clone(),
                        partition,
                        version: scope.version,
                        run_id: manifest.run_id,
                    },
                    parent,
                )?;
            }
            PinScope::Table(_) => {}
            PinScope::Partition(_) => {
                retention::pin_path(
                    self.store,
                    &operation.dataset,
                    &payload.scope,
                    &payload.pin_id,
                    false,
                )?;
            }
        }
        Ok(None)
    }
    fn outcome(
        &self,
        operation: &OperationRecord,
        committed: bool,
        noop: bool,
        markers: Vec<ObjectKey>,
        pin: Option<PinRecord>,
        release: Option<PinReleaseMarker>,
    ) -> AdminOutcome {
        AdminOutcome {
            operation_id: operation.operation_id.clone(),
            committed,
            noop,
            markers,
            excluded_pruned: vec![],
            pin,
            release,
            maintenance_error: None,
        }
    }
    fn observed(&self, operation: &OperationRecord, committed: bool) -> Result<AdminOutcome> {
        if matches!(operation.body, OperationPayload::PruneIntent(_)) {
            return Ok(self.outcome(operation, committed, !committed, vec![], None, None));
        }
        if let OperationPayload::Prune(payload) = &operation.body {
            let mut paths = vec![];
            for target in &payload.targets {
                let path = object(
                    &operation.dataset,
                    &format!("{}/.pruned", self.folder(&operation.dataset, target)?),
                );
                self.store.backend.head(&path).map_err(backend_error)?;
                paths.push(path);
            }
            return Ok(self.outcome(operation, committed, !committed, paths, None, None));
        }
        if let OperationPayload::ReleaseHold(payload) = &operation.body {
            let mut paths = vec![];
            for release in &payload.releases {
                let hold = self.release_hold_record(&operation.dataset, release)?;
                let path = crate::holds::release_key(&hold);
                let marker: HoldReleaseMarker = self.read(&path)?;
                if marker.retention_id != hold.retention_id {
                    return Err(invalid("hold release marker differs from exact target"));
                }
                paths.push(path);
            }
            return Ok(self.outcome(operation, committed, !committed, paths, None, None));
        }
        let payload = match &operation.body {
            OperationPayload::Pin(v) | OperationPayload::Unpin(v) => v,
            _ => return Err(invalid("unsupported admin marker observation")),
        };
        let (pin, release) = self.pin_records(&operation.dataset, payload)?;
        if pin.is_none()
            || matches!(operation.body, OperationPayload::Unpin(_)) && release.is_none()
        {
            return Err(invalid(
                "committed pin operation has no durable effect marker",
            ));
        }
        let markers = vec![retention::pin_path(
            self.store,
            &operation.dataset,
            &payload.scope,
            &payload.pin_id,
            matches!(operation.body, OperationPayload::Unpin(_)),
        )?];
        let mut outcome = self.outcome(operation, committed, !committed, markers, pin, release);
        outcome.excluded_pruned = self.excluded(&operation.dataset, &payload.scope)?;
        Ok(outcome)
    }
    /// Caller journals the complete intent and every progress callback. A failed
    /// callback prevents the next effect. This method owns one scoped lease
    /// renewer; callers must not wrap another renewer around it.
    pub fn execute(
        &self,
        owner: &mut LeaseOwner,
        intent: &AdminIntent,
        progress: &mut AdminProgress,
        parent: &Path,
        mut persist: impl FnMut(&AdminIntent, &AdminProgress) -> Result<()>,
    ) -> Result<AdminOutcome> {
        self.execute_inner(owner, intent, progress, parent, None, false, &mut persist)
    }
    #[allow(clippy::too_many_arguments)]
    fn execute_inner(
        &self,
        owner: &mut LeaseOwner,
        intent: &AdminIntent,
        progress: &mut AdminProgress,
        parent: &Path,
        previous: Option<&OperationRecord>,
        keep_pending: bool,
        mut persist: impl FnMut(&AdminIntent, &AdminProgress) -> Result<()>,
    ) -> Result<AdminOutcome> {
        intent.operation.validate().map_err(backend_error)?;
        let supported = match intent.operation.body {
            OperationPayload::Pin(_)
            | OperationPayload::Unpin(_)
            | OperationPayload::ReleaseHold(_) => true,
            OperationPayload::PruneIntent(_) => keep_pending,
            OperationPayload::Prune(_) => previous.is_some(),
            _ => false,
        };
        if !supported {
            return Err(public_error(
                ErrorCode::UnsupportedCapability,
                "admin operation lifecycle is not implemented for this kind",
            ));
        }
        if owner.intent.dataset != intent.operation.dataset {
            return Err(invalid("admin intent belongs to another dataset lease"));
        }
        if let AdminProgress::Complete { outcome } = progress {
            if outcome.operation_id != intent.operation.operation_id {
                return Err(invalid("terminal admin progress differs from intent"));
            }
            return Ok((**outcome).clone());
        }
        if matches!(progress, AdminProgress::Prepared) {
            persist(intent, progress)?;
            if let Some(previous) = previous {
                let (latest, _) = self.publisher.owned(owner)?;
                if !matches!(previous.body, OperationPayload::PruneIntent(_))
                    || previous.dataset != intent.operation.dataset
                    || latest.pending.as_deref() != Some(operation_path(previous).as_str())
                {
                    return Err(public_error(
                        ErrorCode::OwnershipLost,
                        "prune intent was cleared or fenced before decision",
                    ));
                }
                let recorded: OperationRecord =
                    self.read(&object(&previous.dataset, &operation_path(previous)))?;
                if recorded != *previous {
                    return Err(invalid("prune intent differs from immutable description"));
                }
            } else {
                self.publisher.finish_pending(owner)?;
            }
            let (validation, renewed) = crate::renewal::during(
                owner.clone(),
                self.publisher.renewal_interval(),
                |owner| self.publisher.renew(owner),
                |watch| {
                    watch.check()?;
                    let validation = if matches!(
                        intent.operation.body,
                        OperationPayload::PruneIntent(_) | OperationPayload::Prune(_)
                    ) {
                        None
                    } else {
                        self.validate(&intent.operation, parent)?
                    };
                    if validation.is_none() {
                        self.publisher.create(
                            &object(
                                &intent.operation.dataset,
                                &operation_path(&intent.operation),
                            ),
                            &intent.operation,
                        )?;
                    }
                    Ok(validation)
                },
            )?;
            *owner = renewed;
            if let Some(mut outcome) = validation {
                outcome.excluded_pruned = match &intent.operation.body {
                    OperationPayload::Pin(v) => {
                        self.excluded(&intent.operation.dataset, &v.scope)?
                    }
                    _ => vec![],
                };
                let next = AdminProgress::Complete {
                    outcome: Box::new(outcome.clone()),
                };
                persist(intent, &next)?;
                *progress = next;
                return Ok(outcome);
            }
            let (mut latest, expected) = self.publisher.owned(owner)?;
            if latest.pending.as_deref() != previous.map(operation_path).as_deref() {
                return Err(conflict(
                    "pending retention work must finish before a new decision",
                ));
            }
            latest.pending = Some(operation_path(&intent.operation));
            latest.mutation_id = Uuid::v4();
            let next = AdminProgress::Compare {
                latest: Box::new(latest),
                expected,
            };
            persist(intent, &next)?;
            *progress = next;
        }
        let AdminProgress::Compare { latest, expected } = progress else {
            unreachable!()
        };
        let recorded: OperationRecord = self.read(&object(
            &intent.operation.dataset,
            &operation_path(&intent.operation),
        ))?;
        if recorded != intent.operation {
            return Err(invalid("admin description differs from immutable intent"));
        }
        latest.validate().map_err(backend_error)?;
        if latest.pending.as_deref() != Some(operation_path(&intent.operation).as_str())
            || !latest.lease.as_ref().is_some_and(|lease| {
                lease.token == owner.intent.token && lease.holder == owner.intent.holder
            })
        {
            return Err(invalid(
                "admin CAS progress differs from fixed operation and lease",
            ));
        }
        let (current, validator): (Latest, _) = self
            .publisher
            .read(&revision::latest_key(&intent.operation.dataset))?;
        if current == **latest {
            owner.latest = current;
            owner.validator = validator;
        } else if validator == *expected {
            self.publisher.owned(owner)?;
            if current.pending.as_deref() != previous.map(operation_path).as_deref()
                || latest.revision != current.revision
                || latest.high_water != current.high_water
                || latest.lease != current.lease
                || latest.mutation_id == current.mutation_id
            {
                return Err(invalid(
                    "admin pending CAS changes unrelated LATEST authority",
                ));
            }
            if latest
                .lease
                .as_ref()
                .is_none_or(|lease| parse(&lease.expires_at) <= parse(&self.clock.now()))
            {
                return Err(public_error(
                    ErrorCode::OutcomeUnknown,
                    "expired prepared decision requires fresh lease resolution",
                ));
            }
            match self
                .publisher
                .put(&intent.operation.dataset, expected, latest)
            {
                Ok(validator) => {
                    owner.latest = (**latest).clone();
                    owner.validator = validator;
                }
                Err(error) if error.code == ErrorCode::OutcomeUnknown => {
                    let (found, _): (Latest, _) = self
                        .publisher
                        .read(&revision::latest_key(&intent.operation.dataset))?;
                    if found.pending.as_deref() != Some(operation_path(&intent.operation).as_str())
                    {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        } else if current.pending.as_deref() != Some(operation_path(&intent.operation).as_str()) {
            return Err(public_error(
                ErrorCode::OutcomeUnknown,
                "admin decision may have been superseded; fresh lease resolution required",
            ));
        }
        let proof =
            retention::observe_pending(self.store, &intent.operation.dataset, &intent.operation)?;
        let replay = crate::renewal::during(
            owner.clone(),
            self.publisher.renewal_interval(),
            |owner| self.publisher.renew(owner),
            |watch| {
                watch.check()?;
                retention::replay_proven(self.store, &proof, self.clock)
            },
        );
        let mut replay_error = None;
        match replay {
            Ok((_, renewed)) => *owner = renewed,
            Err(error) => replay_error = Some(error),
        }
        let mut outcome = match self.observed(&intent.operation, true) {
            Ok(outcome) => outcome,
            Err(error) => return Err(replay_error.unwrap_or(error)),
        };
        outcome.maintenance_error = replay_error;
        let next = AdminProgress::Complete {
            outcome: Box::new(outcome.clone()),
        };
        persist(intent, &next)?;
        *progress = next;
        if !keep_pending && let Err(error) = self.publisher.finish_pending(owner) {
            outcome.maintenance_error = Some(error);
        }
        Ok(outcome)
    }
    /// Explicit observed candidates are only hints. Protection is recomputed
    /// under the actual lease, and again after the durable intent exposes them
    /// to concurrent holds. This owns the sole lease renewer for the operation.
    pub fn prune(
        &self,
        owner: &mut LeaseOwner,
        request: &PruneRequest,
        progress: &mut PruneProgress,
        parent: &Path,
        mut persist: impl FnMut(&PruneRequest, &PruneProgress) -> Result<()>,
    ) -> Result<PruneOutcome> {
        if owner.intent.dataset != request.dataset {
            return Err(invalid("prune request belongs to another dataset lease"));
        }
        if let PruneProgress::Complete { outcome } = progress {
            return Ok((**outcome).clone());
        }
        if matches!(progress, PruneProgress::Prepared) {
            persist(request, progress)?;
            self.publisher.finish_pending(owner)?;
            let (assessment, renewed) = crate::renewal::during(
                owner.clone(),
                self.publisher.renewal_interval(),
                |owner| self.publisher.renew(owner),
                |watch| {
                    watch.check()?;
                    self.assess(owner, &request.targets, parent, true)
                },
            )?;
            *owner = renewed;
            if assessment.eligible.is_empty() {
                let mut outcome = PruneOutcome {
                    pruned: assessment.already_pruned.clone(),
                    protected: assessment.protected,
                    deleted: vec![],
                    cleanup_complete: vec![],
                    completed_operation_ids: vec![],
                    maintenance_error: None,
                };
                let deletion = self.delete_tombstoned(&request.dataset, &assessment.already_pruned);
                match deletion {
                    Ok(paths) => {
                        outcome.deleted = paths;
                        outcome.cleanup_complete = outcome.pruned.clone();
                    }
                    Err(error) => outcome.maintenance_error = Some(error),
                };
                let next = PruneProgress::Complete {
                    outcome: Box::new(outcome.clone()),
                };
                persist(request, &next)?;
                *progress = next;
                return Ok(outcome);
            }
            let intent = self.prepare(
                request.dataset.clone(),
                OperationPayload::PruneIntent(PrunePayload {
                    targets: assessment.eligible,
                }),
            )?;
            let next = PruneProgress::Intent {
                intent: Box::new(intent),
                progress: Box::new(AdminProgress::Prepared),
                protected: assessment.protected,
                already_pruned: assessment.already_pruned,
            };
            persist(request, &next)?;
            *progress = next;
        }
        if let PruneProgress::Intent {
            intent,
            progress: inner,
            protected,
            already_pruned,
        } = progress.clone()
        {
            let mut inner = *inner;
            self.execute_inner(
                owner,
                &intent,
                &mut inner,
                parent,
                None,
                true,
                |_, inner| {
                    let next = PruneProgress::Intent {
                        intent: intent.clone(),
                        progress: Box::new(inner.clone()),
                        protected: protected.clone(),
                        already_pruned: already_pruned.clone(),
                    };
                    persist(request, &next)?;
                    *progress = next;
                    Ok(())
                },
            )?;
            let (latest, _) = self.publisher.owned(owner)?;
            if latest.pending.as_deref() != Some(operation_path(&intent.operation).as_str()) {
                return Err(public_error(
                    ErrorCode::OwnershipLost,
                    "prune intent was fenced before hold re-list",
                ));
            }
            let OperationPayload::PruneIntent(proposed) = &intent.operation.body else {
                return Err(invalid("prune journal has no proposal"));
            };
            let (assessment, renewed) = crate::renewal::during(
                owner.clone(),
                self.publisher.renewal_interval(),
                |owner| self.publisher.renew(owner),
                |watch| {
                    watch.check()?;
                    self.assess(owner, &proposed.targets, parent, true)
                },
            )?;
            *owner = renewed;
            let mut protected = protected;
            protected.extend(assessment.protected);
            if assessment.eligible.is_empty() {
                self.publisher.finish_pending(owner)?;
                let mut outcome = PruneOutcome {
                    pruned: already_pruned,
                    protected,
                    deleted: vec![],
                    cleanup_complete: vec![],
                    completed_operation_ids: vec![intent.operation.operation_id.clone()],
                    maintenance_error: None,
                };
                match self.delete_tombstoned(&request.dataset, &outcome.pruned) {
                    Ok(paths) => {
                        outcome.deleted = paths;
                        outcome.cleanup_complete = outcome.pruned.clone();
                    }
                    Err(error) => outcome.maintenance_error = Some(error),
                }
                let next = PruneProgress::Complete {
                    outcome: Box::new(outcome.clone()),
                };
                persist(request, &next)?;
                *progress = next;
                return Ok(outcome);
            }
            let decision = self.prepare(
                request.dataset.clone(),
                OperationPayload::Prune(PrunePayload {
                    targets: assessment.eligible,
                }),
            )?;
            let next = PruneProgress::Decision {
                proposal: intent,
                decision: Box::new(decision),
                progress: Box::new(AdminProgress::Prepared),
                protected,
                already_pruned,
            };
            persist(request, &next)?;
            *progress = next;
        }
        let PruneProgress::Decision {
            proposal,
            decision,
            progress: inner,
            protected,
            already_pruned,
        } = progress.clone()
        else {
            return Err(invalid("prune progress has no committed decision phase"));
        };
        let (OperationPayload::PruneIntent(proposed), OperationPayload::Prune(decided)) =
            (&proposal.operation.body, &decision.operation.body)
        else {
            return Err(invalid("prune progress has inconsistent operation kinds"));
        };
        if proposal.operation.dataset != request.dataset
            || decision.operation.dataset != request.dataset
            || !proposed
                .targets
                .iter()
                .all(|target| request.targets.contains(target))
            || !decided
                .targets
                .iter()
                .all(|target| proposed.targets.contains(target))
        {
            return Err(invalid(
                "prune decision is not a subset of its fixed proposal",
            ));
        }
        let mut inner = *inner;
        let result = self.execute_inner(
            owner,
            &decision,
            &mut inner,
            parent,
            Some(&proposal.operation),
            false,
            |_, inner| {
                let next = PruneProgress::Decision {
                    proposal: proposal.clone(),
                    decision: decision.clone(),
                    progress: Box::new(inner.clone()),
                    protected: protected.clone(),
                    already_pruned: already_pruned.clone(),
                };
                persist(request, &next)?;
                *progress = next;
                Ok(())
            },
        )?;
        let mut pruned = already_pruned;
        pruned.extend(decided.targets.clone());
        let mut outcome = PruneOutcome {
            pruned: pruned.clone(),
            protected,
            deleted: vec![],
            cleanup_complete: vec![],
            completed_operation_ids: vec![
                proposal.operation.operation_id.clone(),
                decision.operation.operation_id.clone(),
            ],
            maintenance_error: result.maintenance_error,
        };
        // A durable inner terminal callback can precede pending clear. Repair
        // that committed operation under the actual lease before cleanup, while
        // retaining marker-proven success if maintenance loses ownership.
        if let Err(error) = self.publisher.finish_pending(owner) {
            outcome.maintenance_error = Some(error);
        }
        // All markers are known durable; clear was attempted before physical
        // deletion. A still-pending decision must be completed by its lease owner.
        let cleared = match revision::read_latest(self.store, &request.dataset) {
            Ok(Some((latest, _))) => {
                latest.pending.as_deref() != Some(operation_path(&decision.operation).as_str())
            }
            Ok(None) => {
                outcome.maintenance_error = Some(invalid("committed prune lost LATEST"));
                false
            }
            Err(error) => {
                outcome.maintenance_error = Some(error);
                false
            }
        };
        if cleared {
            match self.delete_tombstoned(&request.dataset, &pruned) {
                Ok(paths) => {
                    outcome.deleted = paths;
                    outcome.cleanup_complete = pruned.clone();
                }
                Err(error) => outcome.maintenance_error = Some(error),
            }
        }
        let next = PruneProgress::Complete {
            outcome: Box::new(outcome.clone()),
        };
        persist(request, &next)?;
        *progress = next;
        Ok(outcome)
    }
    /// Irreversible tombstone evidence authorizes only these files. No listing,
    /// description, private progress record, or stale lease is delete authority.
    pub fn delete_tombstoned(
        &self,
        dataset: &Name,
        targets: &[VersionTarget],
    ) -> Result<Vec<ObjectKey>> {
        if targets.len() > 131_072 {
            return Err(public_error(
                ErrorCode::UnsupportedCapability,
                "cleanup targets exceed metadata budget",
            ));
        }
        let latest = revision::read_latest(self.store, dataset)?
            .ok_or_else(|| invalid("tombstone cleanup has no LATEST"))?
            .0;
        if latest.pending.is_some() {
            return Err(conflict(
                "pending retention work must complete before physical deletion",
            ));
        }
        // Establish the entire decision's irreversible authority before the
        // first delete, including a partially replayed multi-folder decision.
        let mut folders = vec![];
        for target in targets {
            let folder = self.folder(dataset, target)?;
            self.store
                .backend
                .head(&object(dataset, &format!("{folder}/.pruned")))
                .map_err(backend_error)?;
            folders.push(folder);
        }
        let mut deleted = vec![];
        for folder in folders {
            let stem = format!("datasets/{dataset}/{folder}/");
            let prefix = ObjectPrefix::new(stem.clone()).unwrap();
            let mut files = vec![];
            let mut manifest = None;
            for item in self
                .store
                .backend
                .list(&prefix, ListMode::Children)
                .map_err(backend_error)?
            {
                let ListEntry::Object(path) = item else {
                    continue;
                };
                let name = path
                    .as_str()
                    .strip_prefix(&stem)
                    .ok_or_else(|| invalid("tombstone deletion listing escaped target"))?;
                if name == "manifest.json" {
                    manifest = Some(path);
                } else if name == "data.parquet"
                    || name
                        .strip_prefix("data-")
                        .and_then(|name| name.strip_suffix(".parquet"))
                        .is_some_and(|number| {
                            number
                                .parse::<u64>()
                                .ok()
                                .is_some_and(|value| value > 0 && value.to_string() == number)
                        })
                {
                    files.push(path);
                }
            }
            files.sort();
            for path in files.into_iter().chain(manifest) {
                self.store.backend.delete(&path).map_err(backend_error)?;
                deleted.push(path);
            }
        }
        Ok(deleted)
    }

    /// Reacquisition fences an unknown old CAS. Complete pending work first;
    /// marker operation identity proves the original commit, not a description.
    pub fn resolve(
        &self,
        intent: &AdminIntent,
        fresh: &LeaseIntent,
        progress: &mut LeaseProgress,
        parent: &Path,
        mut persist: impl FnMut(&LeaseProgress) -> Result<()>,
    ) -> Result<AdminResolution> {
        if fresh.dataset != intent.operation.dataset {
            return Err(invalid("resolution lease belongs to another dataset"));
        }
        let mut owner = self
            .publisher
            .acquire_authorized(fresh, progress, &mut persist)?;
        self.publisher.finish_pending(&mut owner)?;
        let mut desired = false;
        let committed = match &intent.operation.body {
            OperationPayload::Pin(payload) | OperationPayload::Unpin(payload) => {
                let (pin, release) = self.pin_records(&intent.operation.dataset, payload)?;
                if matches!(intent.operation.body, OperationPayload::Pin(_)) {
                    desired = pin.as_ref().is_some_and(|p| p.reason == payload.reason)
                        && release.is_none();
                    pin.as_ref()
                        .is_some_and(|p| p.operation_id == intent.operation.operation_id)
                } else {
                    desired = release.is_some();
                    release
                        .as_ref()
                        .is_some_and(|r| r.operation_id == intent.operation.operation_id)
                }
            }
            OperationPayload::ReleaseHold(payload) => {
                desired = true;
                let mut committed = false;
                for release in &payload.releases {
                    let hold = self.release_hold_record(&intent.operation.dataset, release)?;
                    if let Some(marker) =
                        self.optional::<HoldReleaseMarker>(&crate::holds::release_key(&hold))?
                    {
                        if marker.retention_id != hold.retention_id {
                            return Err(invalid("hold release marker differs from exact target"));
                        }
                        committed |= marker.operation_id == intent.operation.operation_id;
                    } else {
                        desired = false;
                    }
                }
                committed
            }
            OperationPayload::Prune(payload) => {
                desired = true;
                let mut committed = false;
                for target in &payload.targets {
                    let path = object(
                        &intent.operation.dataset,
                        &format!(
                            "{}/.pruned",
                            self.folder(&intent.operation.dataset, target)?
                        ),
                    );
                    if let Some(marker) = self.optional::<PrunedMarker>(&path)? {
                        if marker.table != target.table
                            || marker.partition != target.partition
                            || marker.version != target.version
                        {
                            return Err(invalid("prune marker differs from resolution target"));
                        }
                        committed |= marker.operation_id == intent.operation.operation_id;
                    } else {
                        desired = false;
                    }
                }
                committed
            }
            // Harmless intent has no effect marker and no deletion authority.
            OperationPayload::PruneIntent(_) => false,
            _ => return Err(invalid("unsupported admin resolution")),
        };
        let observed = if committed || desired {
            Some(self.observed(&intent.operation, committed)?)
        } else {
            None
        };
        let _ = parent;
        let maintenance_error = self.publisher.release(&mut owner).err();
        Ok(AdminResolution {
            committed,
            observed,
            maintenance_error,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        canonical::Sorter,
        clock::{parse, timestamp},
        ownership::{Ownership, ReservationProgress},
        store::InitOptions,
    };
    use grv_adapter_api::{Column, TableContract};
    use grv_storage::{Error, ErrorKind, LocalBackend, ObjectMeta, WriteEffect};
    use std::{
        io::{Read, Write},
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
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
        writes: AtomicUsize,
        at: AtomicUsize,
        mode: AtomicUsize,
        forbid_data: AtomicBool,
    }
    impl FaultBackend {
        fn arm(&self, at: usize, mode: usize) {
            self.writes.store(0, Ordering::SeqCst);
            self.at.store(at, Ordering::SeqCst);
            self.mode.store(mode, Ordering::SeqCst);
        }
        fn before(&self) -> grv_storage::Result<bool> {
            let n = self.writes.fetch_add(1, Ordering::SeqCst) + 1;
            let fault = n == self.at.load(Ordering::SeqCst);
            if fault && self.mode.load(Ordering::SeqCst) == 1 {
                return Err(Error::new(ErrorKind::Io, "simulated crash before write"));
            }
            Ok(fault)
        }
        fn after(&self, path: &ObjectKey, fault: bool) -> grv_storage::Result<()> {
            if !fault {
                return Ok(());
            }
            let mode = self.mode.load(Ordering::SeqCst);
            if mode == 3 && path.as_str().ends_with("/.states/LATEST") {
                let (bytes, meta) = self.inner.read_bytes(path, 64 * 1024 * 1024)?;
                let mut latest: Latest = decode_record(&bytes)?;
                let pending = latest.pending.as_ref().unwrap();
                let dataset = Name::new(path.as_str().split('/').nth(1).unwrap()).unwrap();
                let (bytes, _) = self
                    .inner
                    .read_bytes(&object(&dataset, pending), 64 * 1024 * 1024)?;
                let operation: OperationRecord = decode_record(&bytes)?;
                if let OperationPayload::Pin(payload) = &operation.body {
                    let marker = PinRecord {
                        pin_id: payload.pin_id.clone(),
                        operation_id: operation.operation_id.clone(),
                        scope: payload.scope.clone(),
                        created_at: operation.created_at.clone(),
                        created_by: operation.created_by.clone(),
                        reason: payload.reason.clone(),
                    };
                    let PinScope::Revision(scope) = &payload.scope else {
                        panic!("test takeover expects revision pin")
                    };
                    let marker_path = object(
                        &dataset,
                        &format!(
                            ".states/revisions/revision={}/.pins/{}.json",
                            scope.revision, payload.pin_id
                        ),
                    );
                    self.inner
                        .create_bytes(&marker_path, &encode_record(&marker)?)?;
                }
                if let OperationPayload::Prune(payload) = &operation.body {
                    for target in &payload.targets {
                        assert!(target.partition.is_empty());
                        let marker = PrunedMarker {
                            operation_id: operation.operation_id.clone(),
                            pruned_by: "fault-helper".into(),
                            table: target.table.clone(),
                            partition: target.partition.clone(),
                            version: target.version,
                            pruned_at: operation.created_at.clone(),
                        };
                        self.inner.create_bytes(
                            &object(
                                &dataset,
                                &format!("{}/version={}/.pruned", target.table, target.version),
                            ),
                            &encode_record(&marker)?,
                        )?;
                    }
                }
                latest.pending = None;
                latest.mutation_id = Uuid::v4();
                latest.lease.as_mut().unwrap().token = LeaseToken::generate();
                self.inner
                    .put_bytes(path, &meta.validator, &encode_record(&latest)?)?;
            }
            let mut error = Error::new(ErrorKind::Io, "lost write acknowledgement");
            error.effect = WriteEffect::MaybeApplied;
            Err(error)
        }
    }
    impl Backend for FaultBackend {
        fn get(&self, key: &ObjectKey, sink: &mut dyn Write) -> grv_storage::Result<ObjectMeta> {
            if self.forbid_data.load(Ordering::SeqCst) && key.as_str().ends_with("data.parquet") {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "source-free outcome replay read data",
                ));
            }
            self.inner.get(key, sink)
        }
        fn head(&self, key: &ObjectKey) -> grv_storage::Result<ObjectMeta> {
            self.inner.head(key)
        }
        fn list(
            &self,
            prefix: &ObjectPrefix,
            mode: ListMode,
        ) -> grv_storage::Result<Vec<ListEntry>> {
            self.inner.list(prefix, mode)
        }
        fn delete(&self, key: &ObjectKey) -> grv_storage::Result<()> {
            self.inner.delete(key)
        }
        fn conditional_create(
            &self,
            key: &ObjectKey,
            source: &mut dyn Read,
        ) -> grv_storage::Result<Validator> {
            let fault = self.before()?;
            let value = self.inner.conditional_create(key, source)?;
            self.after(key, fault)?;
            Ok(value)
        }
        fn conditional_put(
            &self,
            key: &ObjectKey,
            expected: &Validator,
            source: &mut dyn Read,
        ) -> grv_storage::Result<Validator> {
            let fault = self.before()?;
            let value = self.inner.conditional_put(key, expected, source)?;
            self.after(key, fault)?;
            Ok(value)
        }
    }
    fn name(value: &str) -> Name {
        Name::new(value).unwrap()
    }
    fn run(n: u32) -> RunId {
        RunId::new(format!("01M3KQA080R6Y8C2D9F0G{n:05}")).unwrap()
    }
    struct Fixture {
        root: tempfile::TempDir,
        store: Store<FaultBackend>,
        clock: TestClock,
    }
    impl Fixture {
        fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let backend = FaultBackend {
                inner: LocalBackend::open(root.path()).unwrap(),
                writes: AtomicUsize::new(0),
                at: AtomicUsize::new(0),
                mode: AtomicUsize::new(0),
                forbid_data: AtomicBool::new(false),
            };
            let store = Store::initialize(backend, InitOptions::default())
                .unwrap()
                .0;
            let f = Self {
                root,
                store,
                clock: TestClock(AtomicU64::new(0)),
            };
            let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
            let mut owner = ownership
                .prepare_run(name("data"), run(1), Counter::from(0), vec![], None)
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
            let mut sorter = Sorter::new(contract.clone(), f.root.path()).unwrap();
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
                .reserve_authorized(&owner, &intent, &mut ReservationProgress::Prepared, |_| {
                    Ok(())
                })
                .unwrap();
            ownership
                .write_group(&owner, &mut reservation, &contract, &staged, None)
                .unwrap();
            ownership
                .release(&mut reservation, ClaimOutcome::Finalized)
                .unwrap();
            ownership.seal(&mut owner).unwrap();
            let publisher = Publisher::new(&f.store, &f.clock, 60).unwrap();
            let intent = publisher
                .prepare_lease(name("data"), "setup".into())
                .unwrap();
            let mut lease = publisher
                .acquire_authorized(&intent, &mut LeaseProgress::Prepared, |_| Ok(()))
                .unwrap();
            let candidate = publisher
                .prepare(
                    &mut lease,
                    ChangeSet {
                        runs: vec![run(1)],
                        ..Default::default()
                    },
                    f.root.path(),
                )
                .unwrap();
            assert_eq!(
                publisher
                    .commit(candidate, |_| Ok(()))
                    .unwrap()
                    .revision
                    .get(),
                1
            );
            f.store.backend.arm(0, 0);
            f
        }
        fn admin(&self) -> Admin<'_, FaultBackend> {
            Admin::new(&self.store, &self.clock, 60).unwrap()
        }
        fn lease(&self) -> LeaseOwner {
            let p = Publisher::new(&self.store, &self.clock, 60).unwrap();
            let intent = p.prepare_lease(name("data"), "test".into()).unwrap();
            p.acquire_authorized(&intent, &mut LeaseProgress::Prepared, |_| Ok(()))
                .unwrap()
        }
        fn fresh(&self) -> LeaseIntent {
            Publisher::new(&self.store, &self.clock, 60)
                .unwrap()
                .prepare_lease(name("data"), "resolve".into())
                .unwrap()
        }
    }
    #[test]
    fn revision_pin_unpin_are_durable_scoped_idempotent_and_do_not_change_revision_counters() {
        let f = Fixture::new();
        let admin = f.admin();
        let mut lease = f.lease();
        let id = Uuid::v4();
        let pin = admin
            .prepare_revision_pin(name("data"), Counter::from(1), id.clone(), "keep".into())
            .unwrap();
        let first = admin
            .execute(
                &mut lease,
                &pin,
                &mut AdminProgress::Prepared,
                f.root.path(),
                |_, _| Ok(()),
            )
            .unwrap();
        assert!(first.committed && !first.noop);
        assert_eq!(first.pin.as_ref().unwrap().reason.as_deref(), Some("keep"));
        let latest = revision::read_latest(&f.store, &name("data"))
            .unwrap()
            .unwrap()
            .0;
        assert_eq!(latest.revision.get(), 1);
        assert_eq!(latest.high_water.get(), 1);
        assert!(latest.pending.is_none());
        let same = admin
            .prepare_revision_pin(name("data"), Counter::from(1), id.clone(), "keep".into())
            .unwrap();
        assert!(
            admin
                .execute(
                    &mut lease,
                    &same,
                    &mut AdminProgress::Prepared,
                    f.root.path(),
                    |_, _| Ok(())
                )
                .unwrap()
                .noop
        );
        let wrong = admin
            .prepare_revision_pin(
                name("data"),
                Counter::from(1),
                id.clone(),
                "different".into(),
            )
            .unwrap();
        assert_eq!(
            admin
                .execute(
                    &mut lease,
                    &wrong,
                    &mut AdminProgress::Prepared,
                    f.root.path(),
                    |_, _| Ok(())
                )
                .unwrap_err()
                .code,
            ErrorCode::StateConflict
        );
        let unpin = admin
            .prepare_revision_unpin(name("data"), Counter::from(1), id)
            .unwrap();
        assert!(
            admin
                .execute(
                    &mut lease,
                    &unpin,
                    &mut AdminProgress::Prepared,
                    f.root.path(),
                    |_, _| Ok(())
                )
                .unwrap()
                .committed
        );
        assert!(
            admin
                .execute(
                    &mut lease,
                    &unpin,
                    &mut AdminProgress::Prepared,
                    f.root.path(),
                    |_, _| Ok(())
                )
                .unwrap()
                .noop
        );
        assert_eq!(
            admin
                .execute(
                    &mut lease,
                    &same,
                    &mut AdminProgress::Prepared,
                    f.root.path(),
                    |_, _| Ok(())
                )
                .unwrap_err()
                .code,
            ErrorCode::StateConflict
        );
    }
    #[test]
    fn pin_write_crashes_replay_description_cas_marker_and_clear_without_changing_pin_identity() {
        for at in 1..=5 {
            let f = Fixture::new();
            let admin = f.admin();
            let mut lease = f.lease();
            let id = Uuid::v4();
            let intent = admin
                .prepare_revision_pin(name("data"), Counter::from(1), id.clone(), "keep".into())
                .unwrap();
            let mut progress = AdminProgress::Prepared;
            f.store.backend.arm(at, 1);
            let first = admin.execute(&mut lease, &intent, &mut progress, f.root.path(), |_, _| {
                Ok(())
            });
            f.store.backend.arm(0, 0);
            if at <= 3 {
                assert!(first.is_err());
            } else {
                assert!(first.unwrap().committed);
            }
            let result = admin
                .execute(&mut lease, &intent, &mut progress, f.root.path(), |_, _| {
                    Ok(())
                })
                .unwrap();
            assert!(result.committed);
            assert_eq!(result.pin.unwrap().pin_id, id);
        }
    }
    #[test]
    fn lost_conditional_responses_are_adopted_and_known_pin_survives_cleanup_failure() {
        for at in 1..=5 {
            let f = Fixture::new();
            let admin = f.admin();
            let mut lease = f.lease();
            let intent = admin
                .prepare_revision_pin(name("data"), Counter::from(1), Uuid::v4(), "keep".into())
                .unwrap();
            f.store.backend.arm(at, 2);
            let result = admin
                .execute(
                    &mut lease,
                    &intent,
                    &mut AdminProgress::Prepared,
                    f.root.path(),
                    |_, _| Ok(()),
                )
                .unwrap();
            assert!(result.committed);
        }
    }
    #[test]
    fn unknown_committed_pin_is_fenced_then_resolved_from_marker_without_reading_source_data() {
        let f = Fixture::new();
        let admin = f.admin();
        let mut lease = f.lease();
        let intent = admin
            .prepare_revision_pin(name("data"), Counter::from(1), Uuid::v4(), "keep".into())
            .unwrap();
        let mut progress = AdminProgress::Prepared;
        f.store.backend.arm(2, 3);
        assert_eq!(
            admin
                .execute(
                    &mut lease,
                    &intent,
                    &mut progress,
                    f.root.path(),
                    |_, _| Ok(())
                )
                .unwrap_err()
                .code,
            ErrorCode::OutcomeUnknown
        );
        f.store.backend.arm(0, 0);
        f.clock.0.store(61, Ordering::SeqCst);
        f.store.backend.forbid_data.store(true, Ordering::SeqCst);
        let resolved = admin
            .resolve(
                &intent,
                &f.fresh(),
                &mut LeaseProgress::Prepared,
                f.root.path(),
                |_| Ok(()),
            )
            .unwrap();
        assert!(resolved.committed);
        assert!(resolved.observed.unwrap().committed);
    }
    #[test]
    fn unapplied_pin_cas_resolution_fences_old_request_and_reuses_scoped_pin_id() {
        let f = Fixture::new();
        let admin = f.admin();
        let mut lease = f.lease();
        let id = Uuid::v4();
        let intent = admin
            .prepare_revision_pin(name("data"), Counter::from(1), id.clone(), "keep".into())
            .unwrap();
        let mut progress = AdminProgress::Prepared;
        f.store.backend.arm(2, 1);
        assert!(
            admin
                .execute(
                    &mut lease,
                    &intent,
                    &mut progress,
                    f.root.path(),
                    |_, _| Ok(())
                )
                .is_err()
        );
        f.store.backend.arm(0, 0);
        f.clock.0.store(61, Ordering::SeqCst);
        let resolved = admin
            .resolve(
                &intent,
                &f.fresh(),
                &mut LeaseProgress::Prepared,
                f.root.path(),
                |_| Ok(()),
            )
            .unwrap();
        assert!(!resolved.committed);
        assert!(resolved.observed.is_none());
        assert_eq!(
            admin
                .execute(
                    &mut lease,
                    &intent,
                    &mut progress,
                    f.root.path(),
                    |_, _| Ok(())
                )
                .unwrap_err()
                .code,
            ErrorCode::OutcomeUnknown
        );
        let retry = admin
            .prepare_revision_pin(name("data"), Counter::from(1), id.clone(), "keep".into())
            .unwrap();
        let result = admin
            .execute(
                &mut f.lease(),
                &retry,
                &mut AdminProgress::Prepared,
                f.root.path(),
                |_, _| Ok(()),
            )
            .unwrap();
        assert_eq!(result.pin.unwrap().pin_id, id);
    }
    #[test]
    fn missing_scoped_unpin_and_pruned_revision_pin_fail_without_new_operation_effects() {
        let f = Fixture::new();
        let admin = f.admin();
        let mut lease = f.lease();
        let unpin = admin
            .prepare_revision_unpin(name("data"), Counter::from(1), Uuid::v4())
            .unwrap();
        f.store.backend.arm(0, 0);
        assert_eq!(
            admin
                .execute(
                    &mut lease,
                    &unpin,
                    &mut AdminProgress::Prepared,
                    f.root.path(),
                    |_, _| Ok(())
                )
                .unwrap_err()
                .code,
            ErrorCode::NotFound
        );
        assert_eq!(f.store.backend.writes.load(Ordering::SeqCst), 0);
        let marker = PrunedMarker {
            operation_id: run(9),
            pruned_by: "test".into(),
            pruned_at: f.clock.now(),
            table: name("events"),
            partition: Partition::new(),
            version: Counter::from(1),
        };
        f.store
            .backend
            .create_bytes(
                &object(&name("data"), "events/version=1/.pruned"),
                &encode_record(&marker).unwrap(),
            )
            .unwrap();
        let pin = admin
            .prepare_revision_pin(name("data"), Counter::from(1), Uuid::v4(), "keep".into())
            .unwrap();
        f.store.backend.arm(0, 0);
        assert_eq!(
            admin
                .execute(
                    &mut lease,
                    &pin,
                    &mut AdminProgress::Prepared,
                    f.root.path(),
                    |_, _| Ok(())
                )
                .unwrap_err()
                .code,
            ErrorCode::IntegrityFailure
        );
        assert_eq!(f.store.backend.writes.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn journal_callbacks_precede_operation_effects_and_terminal_replay_is_source_free() {
        let f = Fixture::new();
        let admin = f.admin();
        let mut lease = f.lease();
        let intent = admin
            .prepare_revision_pin(name("data"), Counter::from(1), Uuid::v4(), "keep".into())
            .unwrap();
        f.store.backend.arm(0, 0);
        assert!(
            admin
                .execute(
                    &mut lease,
                    &intent,
                    &mut AdminProgress::Prepared,
                    f.root.path(),
                    |_, _| Err(public_error(
                        ErrorCode::BackendFailure,
                        "intent not durable"
                    ))
                )
                .is_err()
        );
        assert_eq!(f.store.backend.writes.load(Ordering::SeqCst), 0);
        let mut saved = None;
        let mut progress = AdminProgress::Prepared;
        let result = admin.execute(
            &mut lease,
            &intent,
            &mut progress,
            f.root.path(),
            |_, next| {
                saved = Some(serde_json::to_vec(next).unwrap());
                if matches!(next, AdminProgress::Complete { .. }) {
                    Err(public_error(
                        ErrorCode::BackendFailure,
                        "crash after outcome fsync",
                    ))
                } else {
                    Ok(())
                }
            },
        );
        assert!(result.is_err());
        let mut durable: AdminProgress = serde_json::from_slice(&saved.unwrap()).unwrap();
        f.store.backend.forbid_data.store(true, Ordering::SeqCst);
        let replay = admin
            .execute(&mut lease, &intent, &mut durable, f.root.path(), |_, _| {
                panic!("terminal replay needs no effects")
            })
            .unwrap();
        assert!(replay.committed);
    }
    impl Fixture {
        fn supersede(&self) {
            let p = Publisher::new(&self.store, &self.clock, 60).unwrap();
            let mut owner = self.lease();
            let candidate = p
                .prepare(
                    &mut owner,
                    ChangeSet {
                        omissions: vec![Omission {
                            table: name("events"),
                            partition: None,
                        }],
                        ..Default::default()
                    },
                    self.root.path(),
                )
                .unwrap();
            p.commit(candidate, |_| Ok(())).unwrap();
            let params: StoreParameters = self
                .admin()
                .read(&ObjectKey::new("grv.json").unwrap())
                .unwrap();
            self.clock.0.store(
                params.pending_grace_seconds.get() + params.max_clock_skew_seconds.get() + 1,
                Ordering::SeqCst,
            );
        }
        fn target(&self) -> VersionTarget {
            VersionTarget {
                table: name("events"),
                partition: Partition::new(),
                version: Counter::from(1),
            }
        }
        fn data_present(&self) -> bool {
            self.store
                .backend
                .head(&object(&name("data"), "events/version=1/data.parquet"))
                .is_ok()
        }
    }
    #[test]
    fn unpin_write_crashes_and_lost_responses_replay_every_phase() {
        for mode in [1, 2] {
            for at in 1..=4 {
                let f = Fixture::new();
                let a = f.admin();
                let mut owner = f.lease();
                let id = Uuid::v4();
                let pin = a
                    .prepare_revision_pin(
                        name("data"),
                        Counter::from(1),
                        id.clone(),
                        "retain".into(),
                    )
                    .unwrap();
                a.execute(
                    &mut owner,
                    &pin,
                    &mut AdminProgress::Prepared,
                    f.root.path(),
                    |_, _| Ok(()),
                )
                .unwrap();
                let unpin = a
                    .prepare_revision_unpin(name("data"), Counter::from(1), id)
                    .unwrap();
                let mut progress = AdminProgress::Prepared;
                f.store.backend.arm(at, mode);
                let result = a.execute(&mut owner, &unpin, &mut progress, f.root.path(), |_, _| {
                    Ok(())
                });
                f.store.backend.arm(0, 0);
                let result = result.unwrap_or_else(|_| {
                    a.execute(&mut owner, &unpin, &mut progress, f.root.path(), |_, _| {
                        Ok(())
                    })
                    .unwrap()
                });
                assert!(result.committed && result.release.is_some());
                assert!(f.data_present());
                a.publisher.finish_pending(&mut owner).unwrap();
            }
        }
    }
    #[test]
    fn remaining_protections_reports_actual_current_pin_grace_and_holds_without_writes() {
        let f = Fixture::new();
        let a = f.admin();
        let mut owner = f.lease();
        let id = Uuid::v4();
        let pin = a
            .prepare_revision_pin(name("data"), Counter::from(1), id.clone(), "retain".into())
            .unwrap();
        a.execute(
            &mut owner,
            &pin,
            &mut AdminProgress::Prepared,
            f.root.path(),
            |_, _| Ok(()),
        )
        .unwrap();
        let before = f.store.backend.writes.load(Ordering::SeqCst);
        let protections = a
            .remaining_revision_protections(&name("data"), Counter::from(1), f.root.path())
            .unwrap();
        assert_eq!(protections.len(), 2);
        let schema: serde_json::Value = serde_json::from_str(include_str!(
            "../../../spec/grv-client-v1-command-output.schema.json"
        ))
        .unwrap();
        let mut projection = serde_json::json!({"$ref":"#/$defs/protection"});
        projection["$defs"] = schema["$defs"].clone();
        let validator = jsonschema::validator_for(&projection).unwrap();
        for protection in &protections {
            let value = serde_json::to_value(protection).unwrap();
            assert!(
                validator.is_valid(&value),
                "{value}: {:?}",
                validator
                    .iter_errors(&value)
                    .map(|error| error.to_string())
                    .collect::<Vec<_>>()
            );
        }

        assert!(
            protections
                .iter()
                .any(|p| matches!(p.kind, ProtectionKind::Current))
        );
        assert!(protections.iter().any(
            |p| matches!(p.kind, ProtectionKind::Pin) && p.object.pin_id.as_ref() == Some(&id)
        ));
        assert_eq!(f.store.backend.writes.load(Ordering::SeqCst), before);
        let unpin = a
            .prepare_revision_unpin(name("data"), Counter::from(1), id)
            .unwrap();
        a.execute(
            &mut owner,
            &unpin,
            &mut AdminProgress::Prepared,
            f.root.path(),
            |_, _| Ok(()),
        )
        .unwrap();
        assert_eq!(
            a.remaining_revision_protections(&name("data"), Counter::from(1), f.root.path())
                .unwrap()
                .len(),
            1
        );
        a.publisher.release(&mut owner).unwrap();
        f.supersede();
        f.clock.0.store(0, Ordering::SeqCst);
        let protections = a
            .remaining_revision_protections(&name("data"), Counter::from(1), f.root.path())
            .unwrap();
        assert!(
            protections
                .iter()
                .any(|p| matches!(p.kind, ProtectionKind::Grace) && p.until.is_some())
        );
        let hold = HoldRecord {
            retention_id: Uuid::v4(),
            dataset: name("data"),
            revision: Counter::from(1),
            target_dataset: name("consumer"),
            target_run_id: run(99),
            created_at: f.clock.now(),
        };
        a.publisher
            .create(&crate::holds::hold_key(&hold), &hold)
            .unwrap();
        let before = f.store.backend.writes.load(Ordering::SeqCst);
        let protections = a
            .remaining_revision_protections(&name("data"), Counter::from(1), f.root.path())
            .unwrap();
        assert!(
            protections
                .iter()
                .any(|p| matches!(p.kind, ProtectionKind::Hold) && p.active && p.releasable)
        );
        assert_eq!(f.store.backend.writes.load(Ordering::SeqCst), before);
        assert_eq!(
            a.remaining_revision_protections(&name("data"), Counter::from(200), f.root.path())
                .unwrap_err()
                .code,
            ErrorCode::NotFound
        );
    }
    #[test]
    fn protected_prune_commits_intent_then_tombstones_clears_then_deletes_exact_files() {
        let f = Fixture::new();
        f.supersede();
        let a = f.admin();
        let mut owner = f.lease();
        let request = PruneRequest {
            dataset: name("data"),
            targets: vec![f.target()],
        };
        let mut phases = vec![];
        let outcome = a
            .prune(
                &mut owner,
                &request,
                &mut PruneProgress::Prepared,
                f.root.path(),
                |_, progress| {
                    if let PruneProgress::Intent { progress, .. } = progress
                        && matches!(**progress, AdminProgress::Complete { .. })
                    {
                        assert!(f.data_present());
                        assert!(
                            f.store
                                .backend
                                .head(&object(&name("data"), "events/version=1/.pruned"))
                                .is_err()
                        );
                        assert!(
                            revision::read_latest(&f.store, &name("data"))
                                .unwrap()
                                .unwrap()
                                .0
                                .pending
                                .is_some()
                        );
                        phases.push("intent");
                    }
                    if let PruneProgress::Decision { progress, .. } = progress
                        && matches!(**progress, AdminProgress::Complete { .. })
                    {
                        assert!(f.data_present());
                        assert!(
                            f.store
                                .backend
                                .head(&object(&name("data"), "events/version=1/.pruned"))
                                .is_ok()
                        );
                        assert!(
                            revision::read_latest(&f.store, &name("data"))
                                .unwrap()
                                .unwrap()
                                .0
                                .pending
                                .is_some()
                        );
                        assert_eq!(
                            a.delete_tombstoned(&name("data"), &[f.target()])
                                .unwrap_err()
                                .code,
                            ErrorCode::StateConflict
                        );
                        phases.push("tombstones");
                    }
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(phases, ["intent", "tombstones"]);
        assert_eq!(outcome.pruned, [f.target()]);
        assert!(!f.data_present());
        assert_eq!(outcome.deleted.len(), 2);
        assert!(
            f.store
                .backend
                .head(&object(&name("data"), "events/version=1/.pruned"))
                .is_ok()
        );
        assert!(
            f.store
                .backend
                .head(&object(&name("data"), "events/.layout.json"))
                .is_ok()
        );
        assert!(
            revision::read_latest(&f.store, &name("data"))
                .unwrap()
                .unwrap()
                .0
                .pending
                .is_none()
        );
        let replay = a
            .prune(
                &mut owner,
                &request,
                &mut PruneProgress::Prepared,
                f.root.path(),
                |_, _| Ok(()),
            )
            .unwrap();
        assert_eq!(replay.pruned, [f.target()]);
        assert!(replay.deleted.is_empty());
    }
    #[test]
    fn current_and_pinned_versions_refuse_prune_without_tombstones() {
        let f = Fixture::new();
        let a = f.admin();
        let mut owner = f.lease();
        let request = PruneRequest {
            dataset: name("data"),
            targets: vec![f.target()],
        };
        let result = a
            .prune(
                &mut owner,
                &request,
                &mut PruneProgress::Prepared,
                f.root.path(),
                |_, _| Ok(()),
            )
            .unwrap();
        assert_eq!(result.protected, [f.target()]);
        assert!(f.data_present());
        let pin = a
            .prepare_revision_pin(name("data"), Counter::from(1), Uuid::v4(), "retain".into())
            .unwrap();
        a.execute(
            &mut owner,
            &pin,
            &mut AdminProgress::Prepared,
            f.root.path(),
            |_, _| Ok(()),
        )
        .unwrap();
        a.publisher.release(&mut owner).unwrap();
        f.supersede();
        let mut owner = f.lease();
        let result = a
            .prune(
                &mut owner,
                &request,
                &mut PruneProgress::Prepared,
                f.root.path(),
                |_, _| Ok(()),
            )
            .unwrap();
        assert_eq!(result.protected, [f.target()]);
        assert!(f.data_present());
    }
    #[test]
    fn hold_arriving_after_durable_prune_intent_is_relisted_and_cannot_confirm() {
        let f = Fixture::new();
        f.supersede();
        let a = f.admin();
        let mut owner = f.lease();
        let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
        let consumer = ownership
            .prepare_run(
                name("consumer"),
                run(99),
                Counter::from(0),
                vec![RunInput {
                    dataset: name("data"),
                    revision: Counter::from(1),
                    retention_id: Uuid::v4(),
                }],
                None,
            )
            .unwrap();
        ownership.commit_run(&consumer).unwrap();
        let holds = crate::holds::Holds::new(&f.store, &f.clock, f.root.path());
        let hold = holds
            .prepare(&name("consumer"), consumer.control())
            .unwrap()
            .pop()
            .unwrap();
        let mut injected = false;
        let result = a.prune(&mut owner, &PruneRequest { dataset: name("data"), targets: vec![f.target()] }, &mut PruneProgress::Prepared, f.root.path(), |_, progress| {
            if matches!(progress, PruneProgress::Intent { progress, .. } if matches!(**progress, AdminProgress::Complete { .. })) && !injected {
                injected = true;
                assert_eq!(holds.acquire(&hold).unwrap_err().code, ErrorCode::StateConflict);
            }
            Ok(())
        }).unwrap();
        assert!(injected);
        assert_eq!(result.protected, [f.target()]);
        assert!(f.data_present());
        assert!(holds.active(&hold).unwrap());
    }
    #[test]
    fn prune_write_crashes_and_lost_responses_replay_all_conditional_boundaries() {
        for mode in [1, 2] {
            for at in 1..=6 {
                let f = Fixture::new();
                f.supersede();
                let a = f.admin();
                let mut owner = f.lease();
                let request = PruneRequest {
                    dataset: name("data"),
                    targets: vec![f.target()],
                };
                let mut progress = PruneProgress::Prepared;
                f.store.backend.arm(at, mode);
                let result = a.prune(
                    &mut owner,
                    &request,
                    &mut progress,
                    f.root.path(),
                    |_, _| Ok(()),
                );
                f.store.backend.arm(0, 0);
                let result = result.unwrap_or_else(|_| {
                    a.prune(
                        &mut owner,
                        &request,
                        &mut progress,
                        f.root.path(),
                        |_, _| Ok(()),
                    )
                    .unwrap()
                });
                assert_eq!(result.pruned, [f.target()], "mode={mode} at={at}");
                a.publisher.finish_pending(&mut owner).unwrap();
                a.delete_tombstoned(&name("data"), &result.pruned).unwrap();
                assert!(!f.data_present());
                assert!(
                    revision::read_latest(&f.store, &name("data"))
                        .unwrap()
                        .unwrap()
                        .0
                        .pending
                        .is_none()
                );
            }
        }
    }
    #[test]
    fn unknown_prune_decision_is_fenced_and_resolved_from_original_tombstone() {
        let f = Fixture::new();
        f.supersede();
        let a = f.admin();
        let mut owner = f.lease();
        let request = PruneRequest {
            dataset: name("data"),
            targets: vec![f.target()],
        };
        let mut progress = PruneProgress::Prepared;
        f.store.backend.arm(4, 3);
        assert_eq!(
            a.prune(
                &mut owner,
                &request,
                &mut progress,
                f.root.path(),
                |_, _| Ok(())
            )
            .unwrap_err()
            .code,
            ErrorCode::OutcomeUnknown
        );
        assert!(f.data_present());
        let PruneProgress::Decision { decision, .. } = progress else {
            panic!("expected frozen decision")
        };
        f.store.backend.arm(0, 0);
        f.clock.0.fetch_add(100, Ordering::SeqCst);
        f.store.backend.forbid_data.store(true, Ordering::SeqCst);
        let resolution = a
            .resolve(
                &decision,
                &f.fresh(),
                &mut LeaseProgress::Prepared,
                f.root.path(),
                |_| Ok(()),
            )
            .unwrap();
        assert!(resolution.committed && resolution.observed.unwrap().committed);
        f.store.backend.forbid_data.store(false, Ordering::SeqCst);
        a.delete_tombstoned(&name("data"), &request.targets)
            .unwrap();
        assert!(!f.data_present());
    }
    #[test]
    fn intent_description_and_private_progress_never_authorize_deletion() {
        let f = Fixture::new();
        f.supersede();
        let a = f.admin();
        let mut owner = f.lease();
        let request = PruneRequest {
            dataset: name("data"),
            targets: vec![f.target()],
        };
        let mut progress = PruneProgress::Prepared;
        assert!(
            a.prune(
                &mut owner,
                &request,
                &mut progress,
                f.root.path(),
                |_, p| {
                    if matches!(p, PruneProgress::Decision { .. }) {
                        Err(public_error(
                            ErrorCode::BackendFailure,
                            "stop before decision description",
                        ))
                    } else {
                        Ok(())
                    }
                }
            )
            .is_err()
        );
        assert!(f.data_present());
        assert_eq!(
            a.delete_tombstoned(&name("data"), &request.targets)
                .unwrap_err()
                .code,
            ErrorCode::StateConflict
        );
        a.publisher.finish_pending(&mut owner).unwrap();
        assert_eq!(
            a.delete_tombstoned(&name("data"), &request.targets)
                .unwrap_err()
                .code,
            ErrorCode::NotFound
        );
        assert!(f.data_present());
    }
    #[test]
    fn hold_release_checks_stable_consumer_evidence_and_replays_every_write() {
        for mode in [1, 2] {
            for at in 1..=4 {
                let f = Fixture::new();
                let a = f.admin();
                let mut owner = f.lease();
                let hold = HoldRecord {
                    retention_id: Uuid::v4(),
                    dataset: name("data"),
                    revision: Counter::from(1),
                    target_dataset: name("consumer"),
                    target_run_id: run(99),
                    created_at: f.clock.now(),
                };
                a.publisher
                    .create(&crate::holds::hold_key(&hold), &hold)
                    .unwrap();
                let releases = vec![HoldRelease {
                    consumer_dataset: hold.target_dataset.clone(),
                    revision: hold.revision,
                    retention_id: hold.retention_id.clone(),
                }];
                let intent = a
                    .prepare_release_holds(name("data"), releases.clone())
                    .unwrap();
                let mut progress = AdminProgress::Prepared;
                f.store.backend.arm(at, mode);
                let result =
                    a.execute(&mut owner, &intent, &mut progress, f.root.path(), |_, _| {
                        Ok(())
                    });
                f.store.backend.arm(0, 0);
                let result = result.unwrap_or_else(|_| {
                    a.execute(&mut owner, &intent, &mut progress, f.root.path(), |_, _| {
                        Ok(())
                    })
                    .unwrap()
                });
                assert!(result.committed);
                assert!(
                    !crate::holds::Holds::new(&f.store, &f.clock, f.root.path())
                        .active(&hold)
                        .unwrap()
                );
                a.publisher.finish_pending(&mut owner).unwrap();
                let retry = a.prepare_release_holds(name("data"), releases).unwrap();
                assert!(
                    a.execute(
                        &mut owner,
                        &retry,
                        &mut AdminProgress::Prepared,
                        f.root.path(),
                        |_, _| Ok(())
                    )
                    .unwrap()
                    .noop
                );
                assert!(f.data_present());
            }
        }
        let f = Fixture::new();
        let a = f.admin();
        let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
        let consumer = ownership
            .prepare_run(
                name("consumer"),
                run(99),
                Counter::from(0),
                vec![RunInput {
                    dataset: name("data"),
                    revision: Counter::from(1),
                    retention_id: Uuid::v4(),
                }],
                None,
            )
            .unwrap();
        ownership.commit_run(&consumer).unwrap();
        let holds = crate::holds::Holds::new(&f.store, &f.clock, f.root.path());
        let hold = holds
            .prepare(&name("consumer"), consumer.control())
            .unwrap()
            .pop()
            .unwrap();
        holds.acquire(&hold).unwrap();
        f.clock.0.store(1000, Ordering::SeqCst);
        let mut owner = f.lease();
        let intent = a
            .prepare_release_holds(
                name("data"),
                vec![HoldRelease {
                    consumer_dataset: hold.target_dataset.clone(),
                    revision: hold.revision,
                    retention_id: hold.retention_id.clone(),
                }],
            )
            .unwrap();
        assert_eq!(
            a.execute(
                &mut owner,
                &intent,
                &mut AdminProgress::Prepared,
                f.root.path(),
                |_, _| Ok(())
            )
            .unwrap_err()
            .code,
            ErrorCode::StateConflict
        );
        assert!(holds.active(&hold).unwrap());
        let mut recovery = ownership.recover_run(name("consumer"), run(99)).unwrap();
        ownership.seal(&mut recovery).unwrap();
        assert!(
            a.execute(
                &mut owner,
                &intent,
                &mut AdminProgress::Prepared,
                f.root.path(),
                |_, _| Ok(())
            )
            .unwrap()
            .committed
        );
    }
    mod gc_tests {
        use super::*;
        use crate::gc::{CandidateState, Gc, GcProgress, StoppedWriterEvidence, WriterAttester};
        #[test]
        fn gc_preview_missing_receipt_is_readonly_protected_and_apply_starts_fresh_grace() {
            let f = Fixture::new();
            f.supersede();
            let receipt = object(
                &name("data"),
                ".states/revisions/revision=1/.superseded.json",
            );
            f.store.backend.inner.delete(&receipt).unwrap();
            let gc = Gc::new(&f.store, &f.clock, 60).unwrap();
            f.store.backend.arm(1, 1);
            let preview = gc.preview(&name("data"), f.root.path()).unwrap();
            assert!(preview.eligible.is_empty());
            assert_eq!(preview.protected, [f.target()]);
            assert!(
                preview.candidates[0]
                    .reasons
                    .iter()
                    .any(|r| r.contains("MissingSupersessionReceipt"))
            );
            assert!(preview.known_bytes.get() > 0);
            assert_eq!(preview.unknown_size_count, 0);
            assert_eq!(f.store.backend.writes.load(Ordering::SeqCst), 0);
            assert!(f.store.backend.head(&receipt).is_err());
            f.store.backend.arm(0, 0);
            let mut owner = f.lease();
            let result = gc
                .apply(
                    &mut owner,
                    &mut GcProgress::Prepared,
                    f.root.path(),
                    None,
                    |_| Ok(()),
                )
                .unwrap();
            assert!(result.prune.pruned.is_empty());
            assert_eq!(result.prune.protected, [f.target()]);
            assert!(f.store.backend.head(&receipt).is_ok());
            assert!(f.data_present());
        }
        #[test]
        fn gc_corrupt_coordination_refuses_before_destructive_effects() {
            use std::collections::BTreeMap;
            fn contents(root: &std::path::Path) -> BTreeMap<String, Vec<u8>> {
                fn walk(
                    root: &std::path::Path,
                    dir: &std::path::Path,
                    out: &mut BTreeMap<String, Vec<u8>>,
                ) {
                    for entry in std::fs::read_dir(dir).unwrap() {
                        let path = entry.unwrap().path();
                        if path.is_dir() {
                            walk(root, &path, out);
                        } else {
                            out.insert(
                                path.strip_prefix(root).unwrap().to_str().unwrap().into(),
                                std::fs::read(path).unwrap(),
                            );
                        }
                    }
                }
                let mut result = BTreeMap::new();
                walk(root, root, &mut result);
                result
            }
            for relative in [
                ".runs/01M3KQA080R6Y8C2D9F0G00001.control.json",
                "events/.claim",
                ".states/revisions/revision=1/.superseded.json",
            ] {
                let f = Fixture::new();
                f.supersede();
                let mut owner = f.lease();
                let key = object(&name("data"), relative);
                // Require a real fixture coordination record, not a random
                // garbage file that production discovery might ignore.
                f.store.backend.inner.head(&key).unwrap();
                let path = f.root.path().join(key.as_str());
                std::fs::write(&path, b"{corrupt-coordination").unwrap();
                let before = contents(f.root.path());
                let gc = Gc::new(&f.store, &f.clock, 60).unwrap();
                let result = gc.apply(
                    &mut owner,
                    &mut GcProgress::Prepared,
                    f.root.path(),
                    None,
                    |_| Ok(()),
                );
                assert!(
                    result.is_err(),
                    "corrupt coordination must refuse: {relative}"
                );
                assert!(f.data_present());
                assert_eq!(
                    contents(f.root.path()),
                    before,
                    "GC changed storage after corrupt coordination: {relative}"
                );
            }
        }
        #[test]
        fn gc_apply_rechecks_new_pins_after_preview_and_only_reclaims_after_release() {
            let f = Fixture::new();
            f.supersede();
            let gc = Gc::new(&f.store, &f.clock, 60).unwrap();
            assert_eq!(
                gc.preview(&name("data"), f.root.path()).unwrap().eligible,
                [f.target()]
            );
            let a = f.admin();
            let mut owner = f.lease();
            let id = Uuid::v4();
            let pin = a
                .prepare_revision_pin(name("data"), Counter::from(1), id.clone(), "retain".into())
                .unwrap();
            a.execute(
                &mut owner,
                &pin,
                &mut AdminProgress::Prepared,
                f.root.path(),
                |_, _| Ok(()),
            )
            .unwrap();
            let result = gc
                .apply(
                    &mut owner,
                    &mut GcProgress::Prepared,
                    f.root.path(),
                    None,
                    |_| Ok(()),
                )
                .unwrap();
            assert!(result.prune.pruned.is_empty());
            assert_eq!(result.prune.protected, [f.target()]);
            assert!(f.data_present());
            let unpin = a
                .prepare_revision_unpin(name("data"), Counter::from(1), id)
                .unwrap();
            a.execute(
                &mut owner,
                &unpin,
                &mut AdminProgress::Prepared,
                f.root.path(),
                |_, _| Ok(()),
            )
            .unwrap();
            let result = gc
                .apply(
                    &mut owner,
                    &mut GcProgress::Prepared,
                    f.root.path(),
                    None,
                    |_| Ok(()),
                )
                .unwrap();
            assert_eq!(result.prune.pruned, [f.target()]);
            assert!(!f.data_present());
        }
        #[test]
        fn gc_releases_only_proven_source_holds_and_terminal_result_replay_is_sourcefree() {
            let f = Fixture::new();
            f.supersede();
            let a = f.admin();
            let hold = HoldRecord {
                retention_id: Uuid::v4(),
                dataset: name("data"),
                revision: Counter::from(1),
                target_dataset: name("consumer"),
                target_run_id: run(99),
                created_at: f.clock.now(),
            };
            a.publisher
                .create(&crate::holds::hold_key(&hold), &hold)
                .unwrap();
            let gc = Gc::new(&f.store, &f.clock, 60).unwrap();
            let before = f.store.backend.writes.load(Ordering::SeqCst);
            let preview = gc.preview(&name("data"), f.root.path()).unwrap();
            assert!(preview.eligible.is_empty());
            assert_eq!(preview.protected, [f.target()]);
            assert!(preview.holds[0].releasable);
            assert!(matches!(
                preview.candidates[0].state,
                CandidateState::NeedsHoldRelease
            ));
            assert_eq!(f.store.backend.writes.load(Ordering::SeqCst), before);
            let mut owner = f.lease();
            let mut progress = GcProgress::Prepared;
            let result = gc
                .apply(&mut owner, &mut progress, f.root.path(), None, |_| Ok(()))
                .unwrap();
            assert_eq!(result.released_holds, [crate::holds::release_key(&hold)]);
            assert_eq!(result.prune.pruned, [f.target()]);
            assert!(!f.data_present());
            assert_eq!(result.prune.cleanup_complete, [f.target()]);
            assert_eq!(result.completed_operation_ids.len(), 3);
            assert!(
                result
                    .candidates
                    .iter()
                    .find(|candidate| candidate.target == f.target())
                    .unwrap()
                    .bytes
                    .unwrap()
                    .get()
                    > 0
            );
            assert!(result.waiting_observation_complete);
            f.store.backend.forbid_data.store(true, Ordering::SeqCst);
            let replay = gc
                .apply(&mut owner, &mut progress, f.root.path(), None, |_| {
                    panic!("terminal replay writes nothing")
                })
                .unwrap();
            assert_eq!(replay.prune.pruned, result.prune.pruned);
        }
        fn pending(f: &Fixture) -> (crate::ownership::RunOwner, VersionTarget) {
            let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
            let mut owner = ownership
                .prepare_run(name("data"), run(99), Counter::from(1), vec![], None)
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
            let reservation = ownership
                .reserve_authorized(&owner, &intent, &mut ReservationProgress::Prepared, |_| {
                    Ok(())
                })
                .unwrap();
            let target = VersionTarget {
                table: name("events"),
                partition: Partition::new(),
                version: reservation.allocation().version,
            };
            f.store
                .backend
                .inner
                .create_bytes(
                    &object(
                        &name("data"),
                        &format!("events/version={}/data.parquet", target.version),
                    ),
                    b"partial output that cannot commit",
                )
                .unwrap();
            f.clock.0.store(1000, Ordering::SeqCst);
            (owner, target)
        }
        #[test]
        fn gc_expired_allocated_claim_without_stopped_writer_evidence_remains_waiting() {
            let f = Fixture::new();
            let (run, target) = pending(&f);
            let gc = Gc::new(&f.store, &f.clock, 60).unwrap();
            let preview = gc.preview(&name("data"), f.root.path()).unwrap();
            assert!(preview.protected.contains(&target));
            assert!(matches!(
                preview
                    .candidates
                    .iter()
                    .find(|candidate| candidate.target == target)
                    .unwrap()
                    .state,
                CandidateState::NeedsRecovery
            ));
            assert!(
                preview
                    .runs
                    .iter()
                    .any(|r| r.run_id == run.control().run_id && r.expired && !r.safe_to_recover)
            );
            let mut owner = f.lease();
            let result = gc
                .apply(
                    &mut owner,
                    &mut GcProgress::Prepared,
                    f.root.path(),
                    None,
                    |_| Ok(()),
                )
                .unwrap();
            assert!(result.prune.protected.contains(&target));
            assert!(
                result
                    .waiting_runs
                    .iter()
                    .any(|r| r.run_id == run.control().run_id)
            );
            assert!(
                f.store
                    .backend
                    .head(&object(
                        &name("data"),
                        &format!("events/version={}/data.parquet", target.version)
                    ))
                    .is_ok()
            );
            let control: RunControl = f
                .admin()
                .read(&object(
                    &name("data"),
                    &format!(".runs/{}.control.json", run.control().run_id),
                ))
                .unwrap();
            assert_eq!(control.phase, RunPhase::Open);
            assert_eq!(control.owner_token, run.control().owner_token);
        }
        struct PreparedNeverStarted;
        impl WriterAttester for PreparedNeverStarted {
            fn stopped(
                &self,
                dataset: &Name,
                control: &RunControl,
            ) -> Result<Option<StoppedWriterEvidence>> {
                // This fixture's pending helper wrote no adapter/session and
                // started no acquisition or producer worker.
                Ok(Some(StoppedWriterEvidence::verified(
                    dataset.clone(),
                    control,
                    grv_types::sha256(b"fixture prepared; no writer ever started"),
                )))
            }
        }
        #[test]
        fn gc_exact_stopped_epoch_recovery_is_journaled_before_cas_then_seals_and_prunes_orphan() {
            let f = Fixture::new();
            let (run, target) = pending(&f);
            let gc = Gc::new(&f.store, &f.clock, 60).unwrap();
            let mut owner = f.lease();
            let mut saved = None;
            let mut progress = GcProgress::Prepared;
            assert!(
                gc.apply(
                    &mut owner,
                    &mut progress,
                    f.root.path(),
                    Some(&PreparedNeverStarted),
                    |next| {
                        saved = Some(serde_json::to_vec(next).unwrap());
                        if matches!(next, GcProgress::RecoverRun { .. }) {
                            Err(public_error(
                                ErrorCode::BackendFailure,
                                "crash after recovery intent fsync",
                            ))
                        } else {
                            Ok(())
                        }
                    }
                )
                .is_err()
            );
            let control: RunControl = f
                .admin()
                .read(&object(
                    &name("data"),
                    &format!(".runs/{}.control.json", run.control().run_id),
                ))
                .unwrap();
            assert_eq!(control.phase, RunPhase::Open);
            assert_eq!(control.owner_token, run.control().owner_token);
            let mut durable: GcProgress = serde_json::from_slice(&saved.unwrap()).unwrap();
            let result = gc
                .apply(&mut owner, &mut durable, f.root.path(), None, |_| Ok(()))
                .unwrap();
            assert_eq!(result.recovered, [run.control().run_id.clone()]);
            assert!(result.prune.pruned.contains(&target));
            let control: RunControl = f
                .admin()
                .read(&object(
                    &name("data"),
                    &format!(".runs/{}.control.json", run.control().run_id),
                ))
                .unwrap();
            assert_eq!(control.phase, RunPhase::Sealed);
            assert!(control.entries.unwrap().is_empty());
            assert!(
                f.store
                    .backend
                    .head(&object(
                        &name("data"),
                        &format!("events/version={}/data.parquet", target.version)
                    ))
                    .is_err()
            );
        }
        #[test]
        fn gc_recovery_does_not_touch_live_runs_and_repairs_missing_sealed_files() {
            let f = Fixture::new();
            let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
            let run = ownership
                .prepare_run(name("data"), super::run(99), Counter::from(1), vec![], None)
                .unwrap();
            ownership.commit_run(&run).unwrap();
            f.store
                .backend
                .inner
                .delete(&object(
                    &name("data"),
                    ".runs/01M3KQA080R6Y8C2D9F0G00001.json",
                ))
                .unwrap();
            let gc = Gc::new(&f.store, &f.clock, 60).unwrap();
            let mut owner = f.lease();
            let result = gc
                .apply(
                    &mut owner,
                    &mut GcProgress::Prepared,
                    f.root.path(),
                    None,
                    |_| Ok(()),
                )
                .unwrap();
            assert_eq!(result.recovered, [super::run(1)]);
            let control: RunControl = f
                .admin()
                .read(&object(
                    &name("data"),
                    &format!(".runs/{}.control.json", run.control().run_id),
                ))
                .unwrap();
            assert_eq!(control.phase, RunPhase::Open);
            assert_eq!(control.owner_token, run.control().owner_token);
            assert!(
                f.store
                    .backend
                    .head(&object(
                        &name("data"),
                        ".runs/01M3KQA080R6Y8C2D9F0G00001.json"
                    ))
                    .is_ok()
            );
        }
        #[test]
        fn gc_safe_expired_run_recovery_resumes_exact_journaled_owner_after_takeover() {
            let f = Fixture::new();
            let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
            let run = ownership
                .prepare_run(name("data"), super::run(99), Counter::from(1), vec![], None)
                .unwrap();
            ownership.commit_run(&run).unwrap();
            f.clock.0.store(1000, Ordering::SeqCst);
            let gc = Gc::new(&f.store, &f.clock, 60).unwrap();
            let mut owner = f.lease();
            let mut progress = GcProgress::Prepared;
            let mut saved = None;
            assert!(
                gc.apply(&mut owner, &mut progress, f.root.path(), None, |next| {
                    saved = Some(serde_json::to_vec(next).unwrap());
                    if matches!(next, GcProgress::SealRun { .. }) {
                        Err(public_error(
                            ErrorCode::BackendFailure,
                            "crash after owner fsync",
                        ))
                    } else {
                        Ok(())
                    }
                })
                .is_err()
            );
            let mut durable: GcProgress = serde_json::from_slice(&saved.unwrap()).unwrap();
            let result = gc
                .apply(&mut owner, &mut durable, f.root.path(), None, |_| Ok(()))
                .unwrap();
            assert_eq!(result.recovered, [super::run(99)]);
            assert!(result.waiting_runs.is_empty());
            assert!(result.prune.pruned.is_empty());
            let control: RunControl = f
                .admin()
                .read(&object(
                    &name("data"),
                    &format!(".runs/{}.control.json", run.control().run_id),
                ))
                .unwrap();
            assert_eq!(control.phase, RunPhase::Sealed);
            assert_ne!(control.owner_token, run.control().owner_token);
        }
        #[test]
        fn gc_reports_external_consumer_recovery_without_taking_over_another_dataset() {
            let f = Fixture::new();
            f.supersede();
            let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
            let consumer = ownership
                .prepare_run(
                    name("consumer"),
                    run(99),
                    Counter::from(0),
                    vec![RunInput {
                        dataset: name("data"),
                        revision: Counter::from(1),
                        retention_id: Uuid::v4(),
                    }],
                    None,
                )
                .unwrap();
            ownership.commit_run(&consumer).unwrap();
            let holds = crate::holds::Holds::new(&f.store, &f.clock, f.root.path());
            let hold = holds
                .prepare(&name("consumer"), consumer.control())
                .unwrap()
                .pop()
                .unwrap();
            holds.acquire(&hold).unwrap();
            f.clock.0.fetch_add(1000, Ordering::SeqCst);
            let gc = Gc::new(&f.store, &f.clock, 60).unwrap();
            let mut owner = f.lease();
            let before: RunControl = f
                .admin()
                .read(&object(
                    &name("consumer"),
                    &format!(".runs/{}.control.json", consumer.control().run_id),
                ))
                .unwrap();
            let preview = gc.preview(&name("data"), f.root.path()).unwrap();
            assert!(preview.holds[0].consumer_needs_recovery);
            assert!(!preview.holds[0].releasable);
            let result = gc
                .apply(
                    &mut owner,
                    &mut GcProgress::Prepared,
                    f.root.path(),
                    None,
                    |_| Ok(()),
                )
                .unwrap();
            assert_eq!(result.waiting_holds.len(), 1);
            assert_eq!(result.prune.protected, [f.target()]);
            assert!(result.released_holds.is_empty());
            let after: RunControl = f
                .admin()
                .read(&object(
                    &name("consumer"),
                    &format!(".runs/{}.control.json", consumer.control().run_id),
                ))
                .unwrap();
            assert_eq!(before, after);
            assert!(f.data_present());
        }
        struct WrongEpoch;
        impl WriterAttester for WrongEpoch {
            fn stopped(
                &self,
                dataset: &Name,
                control: &RunControl,
            ) -> Result<Option<StoppedWriterEvidence>> {
                let mut wrong = control.clone();
                wrong.owner_token = OwnerToken::generate();
                Ok(Some(StoppedWriterEvidence::verified(
                    dataset.clone(),
                    &wrong,
                    grv_types::sha256(b"other worker"),
                )))
            }
        }
        #[test]
        fn gc_refuses_stopped_writer_proof_for_another_run_epoch_before_takeover() {
            let f = Fixture::new();
            let (run, _) = pending(&f);
            let gc = Gc::new(&f.store, &f.clock, 60).unwrap();
            let mut owner = f.lease();
            assert_eq!(
                gc.apply(
                    &mut owner,
                    &mut GcProgress::Prepared,
                    f.root.path(),
                    Some(&WrongEpoch),
                    |_| Ok(())
                )
                .unwrap_err()
                .code,
                ErrorCode::IntegrityFailure
            );
            let after: RunControl = f
                .admin()
                .read(&object(
                    &name("data"),
                    &format!(".runs/{}.control.json", run.control().run_id),
                ))
                .unwrap();
            assert_eq!(&after, run.control());
        }
        #[test]
        fn gc_replays_release_and_prune_journals_at_each_committed_callback_boundary() {
            for phase in 0..4 {
                let f = Fixture::new();
                f.supersede();
                let a = f.admin();
                let hold = HoldRecord {
                    retention_id: Uuid::v4(),
                    dataset: name("data"),
                    revision: Counter::from(1),
                    target_dataset: name("consumer"),
                    target_run_id: run(99),
                    created_at: f.clock.now(),
                };
                a.publisher
                    .create(&crate::holds::hold_key(&hold), &hold)
                    .unwrap();
                let gc = Gc::new(&f.store, &f.clock, 60).unwrap();
                let mut owner = f.lease();
                let mut saved = None;
                assert!(
                    gc.apply(
                        &mut owner,
                        &mut GcProgress::Prepared,
                        f.root.path(),
                        None,
                        |next| {
                            saved = Some(serde_json::to_vec(next).unwrap());
                            let stop = match next {
                                GcProgress::Release { progress, .. } => match phase {
                                    0 => matches!(**progress, AdminProgress::Compare { .. }),
                                    1 => matches!(**progress, AdminProgress::Complete { .. }),
                                    _ => false,
                                },
                                GcProgress::Prune { progress, .. } => match (&**progress, phase) {
                                    (PruneProgress::Intent { progress, .. }, 2)
                                    | (PruneProgress::Decision { progress, .. }, 3) => {
                                        matches!(**progress, AdminProgress::Complete { .. })
                                    }
                                    _ => false,
                                },
                                _ => false,
                            };
                            if stop {
                                Err(public_error(
                                    ErrorCode::BackendFailure,
                                    "simulated crash after journal fsync",
                                ))
                            } else {
                                Ok(())
                            }
                        }
                    )
                    .is_err()
                );
                let mut durable: GcProgress = serde_json::from_slice(&saved.unwrap()).unwrap();
                let result = gc
                    .apply(&mut owner, &mut durable, f.root.path(), None, |_| Ok(()))
                    .unwrap();
                assert_eq!(result.prune.pruned, [f.target()]);
                assert!(!f.data_present(), "phase={phase}");
                assert!(
                    revision::read_latest(&f.store, &name("data"))
                        .unwrap()
                        .unwrap()
                        .0
                        .pending
                        .is_none()
                );
                assert!(
                    f.store
                        .backend
                        .head(&crate::holds::release_key(&hold))
                        .is_ok()
                );
            }
        }
        #[test]
        fn gc_pending_replay_operation_identity_is_durable_before_clear_and_survives_crash() {
            let f = Fixture::new();
            f.supersede();
            let a = f.admin();
            let mut owner = f.lease();
            let pending = a
                .prepare(
                    name("data"),
                    OperationPayload::PruneIntent(PrunePayload {
                        targets: vec![f.target()],
                    }),
                )
                .unwrap();
            a.execute_inner(
                &mut owner,
                &pending,
                &mut AdminProgress::Prepared,
                f.root.path(),
                None,
                true,
                |_, _| Ok(()),
            )
            .unwrap();
            let gc = Gc::new(&f.store, &f.clock, 60).unwrap();
            let mut saved = None;
            assert!(
                gc.apply(
                    &mut owner,
                    &mut GcProgress::Prepared,
                    f.root.path(),
                    None,
                    |next| {
                        saved = Some(serde_json::to_vec(next).unwrap());
                        if matches!(next, GcProgress::Start { .. }) {
                            Err(public_error(
                                ErrorCode::BackendFailure,
                                "crash after pending replay identity fsync",
                            ))
                        } else {
                            Ok(())
                        }
                    }
                )
                .is_err()
            );
            assert!(
                revision::read_latest(&f.store, &name("data"))
                    .unwrap()
                    .unwrap()
                    .0
                    .pending
                    .is_some()
            );
            let mut progress: GcProgress = serde_json::from_slice(&saved.unwrap()).unwrap();
            let result = gc
                .apply(&mut owner, &mut progress, f.root.path(), None, |_| Ok(()))
                .unwrap();
            assert_eq!(result.completed_operation_ids.len(), 3);
            assert_eq!(
                result.completed_operation_ids[0],
                pending.operation.operation_id
            );
            assert!(!f.data_present());
        }
        #[test]
        fn grace_past_supported_timestamp_horizon_is_protected_without_panicking() {
            let f = Fixture::new();
            f.supersede();
            let path = object(
                &name("data"),
                ".states/revisions/revision=1/.superseded.json",
            );
            let (bytes, meta) = f
                .store
                .backend
                .inner
                .read_bytes(&path, 1024 * 1024)
                .unwrap();
            let mut receipt: SupersessionReceipt = decode_record(&bytes).unwrap();
            receipt.observed_at = grv_types::Timestamp::new("9999-12-31T23:59:59Z").unwrap();
            f.store
                .backend
                .inner
                .put_bytes(&path, &meta.validator, &encode_record(&receipt).unwrap())
                .unwrap();
            let protections = f
                .admin()
                .remaining_revision_protections(&name("data"), Counter::from(1), f.root.path())
                .unwrap();
            assert!(
                protections
                    .iter()
                    .any(|p| matches!(p.kind, ProtectionKind::Grace) && p.until.is_none())
            );
        }
    }
}
