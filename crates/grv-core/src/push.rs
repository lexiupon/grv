//! Fixed extraction export plans and source-free finalization. Every callback
//! must durably replace the consumer journal before returning success.
use crate::{
    capture::{CaptureReceipt, CapturedGroup, stage_group},
    clock::Clock,
    journal::Journal,
    normalize::TablePlan,
    ownership::{Ownership, Reservation, ReservationIntent, ReservationProgress, RunOwner},
    publication::{LeaseIntent, LeaseProgress, PublicationIntent, Publisher},
    revision::{self, State},
    store::{Result, Store, backend_error, public_error},
};
use grv_adapter_api::ExtractRequest;
use grv_storage::{Backend, ObjectKey, model::*};
use grv_types::{Digest, ErrorCode, Name, PublicError, RunId};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, path::Path};

fn integrity(message: &str) -> PublicError {
    public_error(ErrorCode::IntegrityFailure, message)
}
fn base_state<B: Backend>(
    store: &Store<B>,
    dataset: &Name,
    base: Counter,
    parent: &Path,
) -> Result<State> {
    if !revision::committed(store, dataset, base, parent)? {
        return Err(public_error(
            ErrorCode::StateConflict,
            "prepared base is not committed",
        ));
    }
    if base.get() == 0 {
        Ok(State::new())
    } else {
        Ok(revision::read(store, dataset, base, parent)?.state)
    }
}
/// Call during session preparation, before creating a run or contacting a source.
pub fn validate_snapshot_membership<B: Backend>(
    store: &Store<B>,
    dataset: &Name,
    base: Counter,
    declared: &[Name],
    drop_tables: &[Name],
    parent: &Path,
) -> Result<()> {
    if declared.is_empty()
        || declared.iter().collect::<BTreeSet<_>>().len() != declared.len()
        || drop_tables.iter().collect::<BTreeSet<_>>().len() != drop_tables.len()
        || drop_tables.iter().any(|table| declared.contains(table))
    {
        return Err(public_error(
            ErrorCode::InvalidArgument,
            "snapshot requires unique nonempty declarations disjoint from whole-table drops",
        ));
    }
    let undeclared: BTreeSet<_> = base_state(store, dataset, base, parent)?
        .keys()
        .filter(|(table, _)| !declared.contains(table) && !drop_tables.contains(table))
        .map(|(table, _)| table.as_str().to_owned())
        .collect();
    if !undeclared.is_empty() {
        return Err(public_error(
            ErrorCode::StateConflict,
            format!(
                "base tables require declarations or whole-table drops: {}",
                undeclared.into_iter().collect::<Vec<_>>().join(", ")
            ),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PushPolicy {
    Changed,
    All,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExportAction {
    Write,
    Reuse { version: Counter },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportGroup {
    pub plan: TablePlan,
    pub capture: CapturedGroup,
    pub action: ExportAction,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportPlan {
    pub dataset: Name,
    pub run_id: RunId,
    pub base_revision: Counter,
    pub capture_digest: Digest,
    pub declaration_digest: Digest,
    pub policy: PushPolicy,
    pub groups: Vec<ExportGroup>,
    pub omissions: Vec<Omission>,
}
/// Equality includes every file, in canonical order. Row totals and the first
/// file alone never establish equality of a split group.
pub fn same_files(captured: &CapturedGroup, manifest: &VersionManifest) -> bool {
    captured.files.len() == manifest.data_files.len()
        && captured
            .files
            .iter()
            .zip(&manifest.data_files)
            .all(|(a, b)| a.name == b.name && a.size.get() == b.size.get() && a.sha256 == b.sha256)
}
impl ExportPlan {
    #[allow(clippy::too_many_arguments)] // Explicit fixed request, storage and journal authority.
    pub fn prepare<B: Backend>(
        store: &Store<B>,
        clock: &dyn Clock,
        ttl: u64,
        owner: &RunOwner,
        receipt: &CaptureReceipt,
        request: &ExtractRequest,
        journal: &Journal,
        policy: PushPolicy,
        drop_tables: &[Name],
        parent: &Path,
    ) -> Result<Self> {
        let ownership = Ownership::new(store, clock, ttl)?;
        ownership.require_open_transfer(owner)?;
        let (plan, _) = crate::renewal::during(
            owner.clone(),
            ownership.renewal_interval(),
            |owner| ownership.renew_run(owner),
            |watch| {
                receipt.verify(journal, request)?;
                watch.check()?;
                if owner.dataset() != &request.dataset
                    || owner.control().run_id != receipt.run_id
                    || !owner.control().inputs.is_empty()
                {
                    return Err(integrity("capture and fixed extraction run differ"));
                }
                let declared: Vec<_> = receipt
                    .tables
                    .iter()
                    .map(|t| t.plan.table.clone())
                    .collect();
                validate_snapshot_membership(
                    store,
                    owner.dataset(),
                    owner.control().base_revision,
                    &declared,
                    drop_tables,
                    parent,
                )?;
                let before = base_state(
                    store,
                    owner.dataset(),
                    owner.control().base_revision,
                    parent,
                )?;
                let mut groups = vec![];
                let mut pairs = BTreeSet::new();
                for table in &receipt.tables {
                    let layout = table.plan.layout();
                    for group in &table.groups {
                        watch.check()?;
                        let pair = (
                            table.plan.table.clone(),
                            layout
                                .partition_path(&group.partition)
                                .map_err(backend_error)?,
                        );
                        if !pairs.insert(pair.clone()) {
                            return Err(integrity("capture repeats a table partition"));
                        }
                        let action = if policy == PushPolicy::Changed {
                            if let Some(entry) = before.get(&pair) {
                                let manifest = ownership.verify_version(
                                    owner.dataset(),
                                    &layout,
                                    &group.partition,
                                    entry.version,
                                )?;
                                if same_files(group, &manifest) {
                                    ExportAction::Reuse {
                                        version: entry.version,
                                    }
                                } else {
                                    ExportAction::Write
                                }
                            } else {
                                ExportAction::Write
                            }
                        } else {
                            ExportAction::Write
                        };
                        groups.push(ExportGroup {
                            plan: table.plan.clone(),
                            capture: group.clone(),
                            action,
                        });
                    }
                }
                groups.sort_by(|a, b| {
                    (&a.plan.table, &a.capture.partition)
                        .cmp(&(&b.plan.table, &b.capture.partition))
                });
                let mut omissions: Vec<_> = drop_tables
                    .iter()
                    .map(|table| Omission {
                        table: table.clone(),
                        partition: None,
                    })
                    .collect();
                for (table, path) in before.keys() {
                    if declared.contains(table) && !pairs.contains(&(table.clone(), path.clone())) {
                        let plan = receipt
                            .tables
                            .iter()
                            .find(|t| &t.plan.table == table)
                            .unwrap();
                        omissions.push(Omission {
                            table: table.clone(),
                            partition: Some(
                                plan.plan
                                    .layout()
                                    .parse_partition(path)
                                    .map_err(backend_error)?,
                            ),
                        });
                    }
                }
                Ok(Self {
                    dataset: owner.dataset().clone(),
                    run_id: receipt.run_id.clone(),
                    base_revision: owner.control().base_revision,
                    capture_digest: receipt.digest()?,
                    declaration_digest: receipt.declaration_sha256.clone(),
                    policy,
                    groups,
                    omissions,
                })
            },
        )?;
        Ok(plan)
    }
    pub fn digest(&self) -> Result<Digest> {
        let value =
            serde_json::to_value(self).map_err(|_| integrity("export plan cannot be encoded"))?;
        grv_types::canonical_json(&value)
            .map(|bytes| grv_types::sha256(&bytes))
            .map_err(|_| integrity("export plan is not canonical JSON"))
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum GroupProgress {
    Pending,
    Reserving {
        intent: ReservationIntent,
        progress: ReservationProgress,
    },
    Reserved {
        reservation: Reservation,
    },
    Written {
        reservation: Reservation,
        manifest: VersionManifest,
    },
    Finalized {
        entry: RunEntry,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaseAttempt {
    pub intent: LeaseIntent,
    pub progress: LeaseProgress,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PushOutcome {
    pub revision: Counter,
    pub no_op: bool,
    pub maintenance_error: Option<PublicError>,
}
/// Consumer-private persisted evidence, including opaque ownership tokens.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PushProgress {
    pub plan: ExportPlan,
    pub plan_digest: Digest,
    pub owner: RunOwner,
    pub groups: Vec<GroupProgress>,
    pub sealed: Option<SealedRun>,
    pub lease: Option<LeaseAttempt>,
    pub publication: Option<PublicationIntent>,
    pub resolution: Option<LeaseAttempt>,
    pub uncommitted: Vec<PublicationIntent>,
    pub terminal: Option<PushOutcome>,
}
impl PushProgress {
    /// Persist this complete value before invoking finalize.
    pub fn planned(owner: RunOwner, plan: ExportPlan) -> Result<Self> {
        let plan_digest = plan.digest()?;
        let groups = plan.groups.iter().map(|_| GroupProgress::Pending).collect();
        Ok(Self {
            owner,
            plan,
            plan_digest,
            groups,
            sealed: None,
            lease: None,
            publication: None,
            resolution: None,
            uncommitted: vec![],
            terminal: None,
        })
    }
}
fn save(
    progress: &mut PushProgress,
    next: PushProgress,
    persist: &mut impl FnMut(&PushProgress) -> Result<()>,
) -> Result<()> {
    persist(&next)?;
    *progress = next;
    Ok(())
}

pub struct PushFinalizer<'a, B: Backend> {
    store: &'a Store<B>,
    ownership: Ownership<'a, B>,
    publisher: Publisher<'a, B>,
}

impl<'a, B: Backend> PushFinalizer<'a, B> {
    pub fn new(store: &'a Store<B>, clock: &'a dyn Clock, ttl: u64) -> Result<Self> {
        Ok(Self {
            store,
            ownership: Ownership::new(store, clock, ttl)?,
            publisher: Publisher::new(store, clock, ttl)?,
        })
    }
    pub fn finalize(
        &self,
        progress: &mut PushProgress,
        journal: &Journal,
        parent: &Path,
        persist: impl FnMut(&PushProgress) -> Result<()>,
    ) -> Result<PushOutcome> {
        self.finalize_checked(progress, journal, parent, persist, || Ok(()))
    }
    /// Resolve established publication evidence before checking the current
    /// installation or offline connection coordinates for unfinished work.
    pub fn finalize_checked(
        &self,
        progress: &mut PushProgress,
        journal: &Journal,
        parent: &Path,
        mut persist: impl FnMut(&PushProgress) -> Result<()>,
        check_unfinished: impl FnOnce() -> Result<()>,
    ) -> Result<PushOutcome> {
        if let Some(outcome) = &progress.terminal {
            return Ok(outcome.clone());
        }
        // Publication proof is independent of retained capture and versions.
        if let Some(intent) = progress.publication.clone() {
            if progress.resolution.is_none() {
                let mut next = progress.clone();
                next.resolution = Some(LeaseAttempt {
                    intent: self.publisher.prepare_lease(
                        intent.dataset.clone(),
                        intent.operation.operation_id.to_string(),
                    )?,
                    progress: LeaseProgress::Prepared,
                });
                save(progress, next, &mut persist)?;
            }
            let mut attempt = progress.resolution.clone().unwrap();
            let shared_progress = std::cell::RefCell::new(&mut *progress);
            let shared_persist = std::cell::RefCell::new(&mut persist);
            let found = self.publisher.resolve_authorized(
                &intent,
                &attempt.intent,
                &mut attempt.progress,
                |phase| {
                    let mut progress = shared_progress.borrow_mut();
                    let mut next = progress.clone();
                    next.resolution.as_mut().unwrap().progress = phase.clone();
                    save(&mut progress, next, &mut **shared_persist.borrow_mut())
                },
                parent,
                |revision| {
                    let mut progress = shared_progress.borrow_mut();
                    let mut next = progress.clone();
                    if let Some(revision) = revision {
                        next.terminal = Some(PushOutcome {
                            revision,
                            no_op: false,
                            maintenance_error: None,
                        });
                    } else {
                        next.uncommitted.push(intent.clone());
                        next.publication = None;
                        next.resolution = None;
                        next.lease = None;
                    }
                    save(&mut progress, next, &mut **shared_persist.borrow_mut())
                },
            )?;
            if found.revision.is_some() {
                let outcome = PushOutcome {
                    maintenance_error: found.maintenance_error,
                    ..progress.terminal.clone().unwrap()
                };
                return Ok(outcome);
            }
            if let Some(error) = found.maintenance_error {
                return Err(error);
            }
        }
        check_unfinished()?;
        if progress.plan.digest()? != progress.plan_digest
            || progress.groups.len() != progress.plan.groups.len()
            || progress.owner.dataset() != &progress.plan.dataset
            || progress.owner.control().run_id != progress.plan.run_id
            || progress.owner.control().base_revision != progress.plan.base_revision
        {
            return Err(integrity("fixed export plan or run identity changed"));
        }
        if progress.sealed.is_none() {
            let key = ObjectKey::new(format!(
                "datasets/{}/.runs/{}.control.json",
                progress.plan.dataset, progress.plan.run_id
            ))
            .unwrap();
            let (bytes, _) = self
                .store
                .backend
                .read_bytes(&key, 64 * 1024 * 1024)
                .map_err(backend_error)?;
            let control: RunControl = decode_record(&bytes).map_err(backend_error)?;
            let recorded = progress.owner.control();
            if control.run_id != recorded.run_id
                || control.created_at != recorded.created_at
                || control.base_revision != recorded.base_revision
                || control.inputs != recorded.inputs
                || control.metadata != recorded.metadata
            {
                return Err(integrity("durable run control changed fixed preparation"));
            }
            if control.phase == RunPhase::Sealed {
                let mut next = progress.clone();
                let sealed = self.ownership.seal(&mut next.owner)?;
                for (index, group) in progress.plan.groups.iter().enumerate() {
                    if matches!(group.action, ExportAction::Reuse { .. }) {
                        continue;
                    }
                    let (token, version) = match &progress.groups[index] {
                        GroupProgress::Finalized { entry } => {
                            (entry.claim_token.clone(), Some(entry.version))
                        }
                        GroupProgress::Reserved { reservation }
                        | GroupProgress::Written { reservation, .. } => (
                            reservation.allocation().claim_token.clone(),
                            Some(reservation.allocation().version),
                        ),
                        GroupProgress::Reserving { progress, .. } => match progress {
                            ReservationProgress::Acquire { claim, .. }
                            | ReservationProgress::Acquired { claim, .. }
                            | ReservationProgress::Allocate { claim, .. } => {
                                (claim.token.clone(), claim.version)
                            }
                            ReservationProgress::Allocated { allocation, .. } => {
                                (allocation.claim_token.clone(), Some(allocation.version))
                            }
                            ReservationProgress::Complete { reservation } => (
                                reservation.allocation().claim_token.clone(),
                                Some(reservation.allocation().version),
                            ),
                            ReservationProgress::Prepared => {
                                return Err(public_error(
                                    ErrorCode::ExtractionIncomplete,
                                    "sealed run has no expected allocation evidence",
                                ));
                            }
                        },
                        GroupProgress::Pending => {
                            return Err(public_error(
                                ErrorCode::ExtractionIncomplete,
                                "sealed run excluded unallocated export work",
                            ));
                        }
                    };
                    let entry = sealed
                        .entries
                        .iter()
                        .find(|entry| {
                            entry.table == group.plan.table
                                && entry.partition == group.capture.partition
                                && entry.claim_token == token
                                && version.is_none_or(|version| entry.version == version)
                        })
                        .ok_or_else(|| {
                            public_error(
                                ErrorCode::ExtractionIncomplete,
                                "recovery excluded an expected output version",
                            )
                        })?;
                    next.groups[index] = GroupProgress::Finalized {
                        entry: entry.clone(),
                    };
                }
                next.sealed = Some(sealed);
                save(progress, next, &mut persist)?;
            } else if control.phase != RunPhase::Open || control.owner_token != recorded.owner_token
            {
                return Err(public_error(
                    ErrorCode::ExtractionIncomplete,
                    "recovered or fenced run cannot continue export",
                ));
            }
        }
        if progress.sealed.is_none() {
            self.ownership.require_open_transfer(&progress.owner)?;
            let (_, mut owner) = crate::renewal::during(
                progress.owner.clone(),
                self.ownership.renewal_interval(),
                |owner| self.ownership.renew_run(owner),
                |watch| -> Result<()> {
                    for index in 0..progress.plan.groups.len() {
                        watch.check()?;
                        let group = progress.plan.groups[index].clone();
                        if matches!(group.action, ExportAction::Reuse { .. }) {
                            continue;
                        }
                        if matches!(progress.groups[index], GroupProgress::Pending) {
                            let intent = self.ownership.prepare_reservation(
                                &progress.owner,
                                group.plan.layout(),
                                group.capture.partition.clone(),
                                &group.plan.contract,
                            )?;
                            let mut next = progress.clone();
                            next.groups[index] = GroupProgress::Reserving {
                                intent,
                                progress: ReservationProgress::Prepared,
                            };
                            save(progress, next, &mut persist)?;
                        }
                        if let GroupProgress::Reserving {
                            intent,
                            progress: mut phase,
                        } = progress.groups[index].clone()
                        {
                            let owner = progress.owner.clone();
                            watch.check()?;
                            let reservation = self.ownership.reserve_authorized(
                                &owner,
                                &intent,
                                &mut phase,
                                |phase| {
                                    let mut next = progress.clone();
                                    next.groups[index] = GroupProgress::Reserving {
                                        intent: intent.clone(),
                                        progress: phase.clone(),
                                    };
                                    save(progress, next, &mut persist)
                                },
                            )?;
                            let mut next = progress.clone();
                            next.groups[index] = GroupProgress::Reserved { reservation };
                            save(progress, next, &mut persist)?;
                        }
                        if let GroupProgress::Reserved { mut reservation } =
                            progress.groups[index].clone()
                        {
                            let staged = stage_group(journal, &group.plan, &group.capture)
                                .map_err(|e| {
                                    if e.code == ErrorCode::NotFound {
                                        public_error(
                                            ErrorCode::ExtractionIncomplete,
                                            "accepted capture artifact is missing",
                                        )
                                    } else {
                                        e
                                    }
                                })?;
                            watch.check()?;
                            let manifest = self.ownership.write_group(
                                &progress.owner,
                                &mut reservation,
                                &group.plan.contract,
                                &staged,
                                None,
                            )?;
                            let mut next = progress.clone();
                            next.groups[index] = GroupProgress::Written {
                                reservation,
                                manifest,
                            };
                            save(progress, next, &mut persist)?;
                        }
                        if let GroupProgress::Written {
                            mut reservation,
                            manifest,
                        } = progress.groups[index].clone()
                        {
                            let actual = self.ownership.verify_version(
                                &progress.plan.dataset,
                                &group.plan.layout(),
                                &group.capture.partition,
                                reservation.allocation().version,
                            )?;
                            if actual != manifest || !same_files(&group.capture, &manifest) {
                                return Err(integrity(
                                    "written version differs from fixed capture plan",
                                ));
                            }
                            watch.check()?;
                            self.ownership
                                .release(&mut reservation, ClaimOutcome::Finalized)?;
                            let allocation = reservation.allocation();
                            if allocation.state != AllocationState::Finalized {
                                return Err(public_error(
                                    ErrorCode::ExtractionIncomplete,
                                    "allocation did not finalize",
                                ));
                            }
                            let entry = RunEntry {
                                table: allocation.table.clone(),
                                partition: allocation.partition.clone(),
                                version: allocation.version,
                                claim_token: allocation.claim_token.clone(),
                            };
                            let mut next = progress.clone();
                            next.groups[index] = GroupProgress::Finalized { entry };
                            save(progress, next, &mut persist)?;
                        }
                    }
                    watch.check()?;
                    Ok(())
                },
            )?;
            let sealed = self.ownership.seal(&mut owner)?;
            let mut next = progress.clone();
            next.owner = owner;
            next.sealed = Some(sealed);
            save(progress, next, &mut persist)?;
        }
        let sealed = progress.sealed.as_ref().unwrap();
        let expected: Vec<_> = progress
            .groups
            .iter()
            .zip(&progress.plan.groups)
            .filter_map(|(phase, group)| {
                if matches!(group.action, ExportAction::Write) {
                    match phase {
                        GroupProgress::Finalized { entry } => Some(entry.clone()),
                        _ => None,
                    }
                } else {
                    None
                }
            })
            .collect();
        if expected.len()
            != progress
                .plan
                .groups
                .iter()
                .filter(|g| matches!(g.action, ExportAction::Write))
                .count()
            || sealed.run_id != progress.plan.run_id
            || sealed.base_revision != progress.plan.base_revision
            || sealed.entries != expected
        {
            return Err(public_error(
                ErrorCode::ExtractionIncomplete,
                "sealed run excludes or changes fixed export work",
            ));
        }
        for (group, phase) in progress.plan.groups.iter().zip(&progress.groups) {
            let version = match (&group.action, phase) {
                (ExportAction::Reuse { version }, GroupProgress::Pending) => *version,
                (ExportAction::Write, GroupProgress::Finalized { entry })
                    if entry.table == group.plan.table
                        && entry.partition == group.capture.partition =>
                {
                    entry.version
                }
                _ => return Err(integrity("group progress differs from fixed export action")),
            };
            let manifest = self.ownership.verify_version(
                &progress.plan.dataset,
                &group.plan.layout(),
                &group.capture.partition,
                version,
            )?;
            if !same_files(&group.capture, &manifest) {
                return Err(integrity(
                    "planned version hashes differ from accepted capture",
                ));
            }
            if let GroupProgress::Finalized { entry } = phase
                && (manifest.run_id != progress.plan.run_id
                    || manifest.claim_token != entry.claim_token)
            {
                return Err(integrity(
                    "finalized version provenance differs from fixed run",
                ));
            }
        }
        if progress.lease.is_none() {
            let mut next = progress.clone();
            next.lease = Some(LeaseAttempt {
                intent: self.publisher.prepare_publication_lease(
                    progress.plan.dataset.clone(),
                    progress.plan.run_id.to_string(),
                )?,
                progress: LeaseProgress::Prepared,
            });
            save(progress, next, &mut persist)?;
        }
        let mut lease = progress.lease.clone().unwrap();
        let mut owner =
            self.publisher
                .acquire_authorized(&lease.intent, &mut lease.progress, |phase| {
                    let mut next = progress.clone();
                    next.lease.as_mut().unwrap().progress = phase.clone();
                    save(progress, next, &mut persist)
                })?;
        let selections = progress
            .plan
            .groups
            .iter()
            .filter_map(|group| match group.action {
                ExportAction::Reuse { version } => Some(Selection {
                    table: group.plan.table.clone(),
                    partition: group.capture.partition.clone(),
                    version,
                }),
                ExportAction::Write => None,
            })
            .collect();
        let change_set = ChangeSet {
            runs: vec![progress.plan.run_id.clone()],
            omissions: progress.plan.omissions.clone(),
            selections,
            expected_revision: Some(progress.plan.base_revision),
            reason: None,
        };
        let candidate = self.publisher.prepare(&mut owner, change_set, parent)?;
        if candidate.is_noop() {
            let outcome = PushOutcome {
                revision: owner.predecessor(),
                no_op: true,
                maintenance_error: None,
            };
            let published = self
                .publisher
                .finish_noop_authorized(candidate, |revision| {
                    let mut next = progress.clone();
                    next.terminal = Some(PushOutcome {
                        revision,
                        ..outcome.clone()
                    });
                    save(progress, next, &mut persist)
                })?;
            return Ok(PushOutcome {
                maintenance_error: published.maintenance_error,
                ..outcome
            });
        }
        let published = self.publisher.commit(candidate, |intent| {
            let mut next = progress.clone();
            next.publication = Some(intent.clone());
            save(progress, next, &mut persist)
        })?;
        let outcome = PushOutcome {
            revision: published.revision,
            no_op: false,
            maintenance_error: published.maintenance_error,
        };
        let mut next = progress.clone();
        next.terminal = Some(outcome.clone());
        save(progress, next, &mut persist)?;
        Ok(outcome)
    }
    pub fn store(&self) -> &Store<B> {
        self.store
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        canonical::{CANONICAL_WRITER, Sorter},
        capture::{CaptureFile, CapturedTable, TableCompletion},
        clock::SystemClock,
        store::InitOptions,
    };
    use arrow_array::{Int64Array, RecordBatch};
    use grv_adapter_api::{
        CaptureWindow, ExtractSelection, ExtractTable, SelectionPolicy, SourceCompletion,
    };
    use grv_storage::{LocalBackend, ObjectKey};
    use grv_types::{AdapterIdentity, Column, Req, SourceConsistency, TableContract, U64, Uuid};
    use serde_json::json;
    use std::{fs::File, os::unix::fs::PermissionsExt, sync::Arc};

    struct Context {
        root: tempfile::TempDir,
        store: Store<LocalBackend>,
        clock: SystemClock,
    }
    fn context() -> Context {
        let root = tempfile::tempdir_in(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
                .parent()
                .unwrap(),
        )
        .unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let target = root.path().join("grv");
        std::fs::create_dir(&target).unwrap();
        let store = Store::initialize(LocalBackend::open(target).unwrap(), InitOptions::default())
            .unwrap()
            .0;
        Context {
            root,
            store,
            clock: SystemClock::default(),
        }
    }
    fn table() -> TablePlan {
        TablePlan {
            table: Name::new("events").unwrap(),
            contract: TableContract {
                columns: vec![Column {
                    name: "value".into(),
                    logical_type: json!("int64"),
                }],
                partition_keys: vec![],
                extensions: json!({}),
                column_ext: json!({}),
            },
            derivations: vec![],
            not_null: vec![],
        }
    }
    fn prepare(
        ctx: &Context,
        n: u32,
        base: Counter,
        policy: PushPolicy,
        values: &[i64],
    ) -> (Journal, PushProgress) {
        prepare_data(ctx, n, base, policy, values, false)
    }
    fn prepare_data(
        ctx: &Context,
        n: u32,
        base: Counter,
        policy: PushPolicy,
        values: &[i64],
        partitioned: bool,
    ) -> (Journal, PushProgress) {
        let journal = Journal::create(
            ctx.root.path().join(format!("state{n}")),
            &[ctx.root.path().join("grv")],
        )
        .unwrap();
        let mut plan = table();
        if partitioned {
            plan.contract.columns.push(Column {
                name: "_region_".into(),
                logical_type: json!("string"),
            });
            plan.contract
                .partition_keys
                .push(Name::new("region").unwrap());
        }
        let partition: Partition = if partitioned {
            [(Name::new("region").unwrap(), Name::new("eu").unwrap())].into()
        } else {
            Partition::new()
        };
        let mut arrays: Vec<Arc<dyn arrow_array::Array>> =
            vec![Arc::new(Int64Array::from(values.to_vec()))];
        if partitioned {
            arrays.push(Arc::new(arrow_array::StringArray::from(vec![
                "eu";
                values.len()
            ])));
        }
        let mut sorter = Sorter::new(plan.contract.clone(), journal.directory()).unwrap();
        let batch = RecordBatch::try_new(
            crate::contract::arrow_schema(&plan.contract).unwrap(),
            arrays,
        )
        .unwrap();
        sorter.append(&batch).unwrap();
        let staged = sorter.finish().unwrap();
        let mut files = vec![];
        for file in &staged.files {
            let suffix = if partitioned { "region=eu/" } else { "" };
            let key = ObjectKey::new(format!("capture/events/{suffix}{}", file.name)).unwrap();
            journal
                .create_artifact(&key, &mut File::open(&file.path).unwrap())
                .unwrap();
            files.push(CaptureFile {
                name: file.name.clone(),
                key,
                size: U64::new(file.size).unwrap(),
                sha256: file.sha256.clone(),
                rows: U64::new(file.rows).unwrap(),
            });
        }
        let run_id = RunId::new(format!("01M3KQA080R6Y8C2D9F0G{n:05}")).unwrap();
        let request = ExtractRequest {
            attempt_id: Uuid::v4(),
            stream_id: Uuid::v4(),
            root: "local:test".into(),
            dataset: Name::new("data").unwrap(),
            run_id,
            declaration_sha256: grv_types::sha256(b"declaration"),
            adapter_identity: AdapterIdentity {
                name: Name::new("fixture").unwrap(),
                package_version: "0.1.0".into(),
                interface_version: Req::new(1).unwrap(),
                binding_schema_version: Req::new(1).unwrap(),
            },
            connection_identity: "fixed-source".into(),
            selection: ExtractSelection {
                policy: SelectionPolicy::All,
            },
            options: json!({}),
            tables: vec![ExtractTable {
                name: plan.table.clone(),
                source: json!({}),
                columns: json!([]),
                contract: plan.contract.clone(),
            }],
            resume: None,
        };
        let window = CaptureWindow {
            start: ctx.clock.now(),
            end: ctx.clock.now(),
        };
        let receipt = CaptureReceipt {
            receipt_version: 1,
            attempt_id: request.attempt_id.clone(),
            run_id: request.run_id.clone(),
            declaration_sha256: request.declaration_sha256.clone(),
            adapter_identity: request.adapter_identity.clone(),
            connection_identity: request.connection_identity.clone(),
            source_consistency: SourceConsistency::AdapterDefined,
            checkpoints: vec![grv_adapter_api::Checkpoint {
                attempt_id: request.attempt_id.clone(),
                adapter_identity: request.adapter_identity.clone(),
                connection_identity: request.connection_identity.clone(),
                tables: vec![grv_adapter_api::CheckpointTable {
                    table: plan.table.clone(),
                    snapshot_id: format!("{n}"),
                    reopenable: false,
                    source_identity: json!({}),
                    capture_start: window.start.clone(),
                }],
                job: json!({}),
            }],
            completion: SourceCompletion {
                job: json!({}),
                capture_window: window.clone(),
                adapter_result: json!({}),
            },
            canonical_writer: CANONICAL_WRITER.into(),
            tables: vec![CapturedTable {
                plan,
                row_count: U64::new(values.len() as u64).unwrap(),
                completion: TableCompletion {
                    row_count: U64::new(values.len() as u64).unwrap(),
                    source_identity: json!({}),
                    capture: window,
                },
                groups: if partitioned && values.is_empty() {
                    vec![]
                } else {
                    vec![CapturedGroup {
                        partition,
                        row_count: U64::new(values.len() as u64).unwrap(),
                        files,
                    }]
                },
            }],
        };
        let ownership = Ownership::new(&ctx.store, &ctx.clock, 60).unwrap();
        validate_snapshot_membership(
            &ctx.store,
            &request.dataset,
            base,
            &[Name::new("events").unwrap()],
            &[],
            ctx.root.path(),
        )
        .unwrap();
        let mut owner = ownership
            .prepare_run(
                request.dataset.clone(),
                request.run_id.clone(),
                base,
                vec![],
                None,
            )
            .unwrap();
        ownership.commit_run(&owner).unwrap();
        ownership.confirm_holds(&mut owner).unwrap();
        let export = ExportPlan::prepare(
            &ctx.store,
            &ctx.clock,
            60,
            &owner,
            &receipt,
            &request,
            &journal,
            policy,
            &[],
            ctx.root.path(),
        )
        .unwrap();
        (journal, PushProgress::planned(owner, export).unwrap())
    }
    fn finish(ctx: &Context, journal: &Journal, progress: &mut PushProgress) -> PushOutcome {
        PushFinalizer::new(&ctx.store, &ctx.clock, 60)
            .unwrap()
            .finalize(progress, journal, ctx.root.path(), |_| Ok(()))
            .unwrap()
    }
    fn crash() -> PublicError {
        public_error(
            ErrorCode::BackendFailure,
            "simulated consumer fsync interruption",
        )
    }

    #[test]
    fn changed_deduplicates_complete_groups_all_allocates_and_empty_table_publishes() {
        let ctx = context();
        let (journal, mut first) = prepare(&ctx, 1, 0.into(), PushPolicy::Changed, &[]);
        let outcome = finish(&ctx, &journal, &mut first);
        assert_eq!(outcome.revision.get(), 1);
        assert!(!outcome.no_op);
        let (journal, mut second) = prepare(&ctx, 2, 1.into(), PushPolicy::Changed, &[]);
        assert!(
            matches!(second.plan.groups[0].action, ExportAction::Reuse { version } if version.get() == 1)
        );
        let outcome = finish(&ctx, &journal, &mut second);
        assert_eq!(outcome.revision.get(), 1);
        assert!(outcome.no_op);
        assert!(second.sealed.unwrap().entries.is_empty());
        let (journal, mut third) = prepare(&ctx, 3, 1.into(), PushPolicy::All, &[]);
        assert!(matches!(third.plan.groups[0].action, ExportAction::Write));
        // The no-op may reserve a high-water number without creating a revision.
        assert_eq!(finish(&ctx, &journal, &mut third).revision.get(), 3);
    }
    #[test]
    fn equality_compares_every_file_name_size_hash_and_count() {
        let ctx = context();
        let (journal, mut progress) = prepare(&ctx, 1, 0.into(), PushPolicy::All, &[3]);
        finish(&ctx, &journal, &mut progress);
        let GroupProgress::Finalized { entry } = &progress.groups[0] else {
            panic!()
        };
        let mut manifest = Ownership::new(&ctx.store, &ctx.clock, 60)
            .unwrap()
            .verify_version(
                &progress.plan.dataset,
                &table().layout(),
                &Partition::new(),
                entry.version,
            )
            .unwrap();
        let mut capture = progress.plan.groups[0].capture.clone();
        let mut next_capture = capture.files[0].clone();
        next_capture.name = "data-1.parquet".into();
        capture.files.push(next_capture);
        let mut next_manifest = manifest.data_files[0].clone();
        next_manifest.name = "data-1.parquet".into();
        manifest.data_files.push(next_manifest);
        assert!(same_files(&capture, &manifest));
        manifest.data_files[1].sha256 = grv_types::sha256(b"second changed");
        assert!(!same_files(&capture, &manifest));
        manifest.data_files[1].sha256 = capture.files[1].sha256.clone();
        manifest.data_files[1].size = manifest.data_files[1].size.next().unwrap();
        assert!(!same_files(&capture, &manifest));
        manifest.data_files[1].size = Counter::new(capture.files[1].size.get()).unwrap();
        manifest.data_files[1].name = "data-2.parquet".into();
        assert!(!same_files(&capture, &manifest));
        manifest.data_files.pop();
        assert!(!same_files(&capture, &manifest));
    }
    #[test]
    fn undeclared_base_membership_rejected_before_new_run_and_missing_drop_is_noop() {
        let ctx = context();
        let (journal, mut progress) = prepare(&ctx, 1, 0.into(), PushPolicy::All, &[1]);
        finish(&ctx, &journal, &mut progress);
        let data = Name::new("data").unwrap();
        let other = Name::new("other").unwrap();
        let error = validate_snapshot_membership(
            &ctx.store,
            &data,
            1.into(),
            std::slice::from_ref(&other),
            &[],
            ctx.root.path(),
        )
        .unwrap_err();
        assert_eq!(error.code, ErrorCode::StateConflict);
        assert!(error.message.contains("events"));
        validate_snapshot_membership(
            &ctx.store,
            &data,
            1.into(),
            &[other],
            &[Name::new("events").unwrap(), Name::new("missing").unwrap()],
            ctx.root.path(),
        )
        .unwrap();
        assert_eq!(
            validate_snapshot_membership(&ctx.store, &data, 1.into(), &[], &[], ctx.root.path())
                .unwrap_err()
                .code,
            ErrorCode::InvalidArgument
        );
    }
    #[test]
    fn written_evidence_replays_release_without_capture_and_sealed_work_survives_pruned_capture() {
        let ctx = context();
        let (journal, mut progress) = prepare(&ctx, 1, 0.into(), PushPolicy::All, &[1]);
        let finalizer = PushFinalizer::new(&ctx.store, &ctx.clock, 60).unwrap();
        let mut written = None;
        assert!(
            finalizer
                .finalize(&mut progress, &journal, ctx.root.path(), |next| {
                    if matches!(next.groups[0], GroupProgress::Written { .. }) {
                        written = Some(next.clone());
                    }
                    if matches!(next.groups[0], GroupProgress::Finalized { .. }) {
                        return Err(crash());
                    }
                    Ok(())
                })
                .is_err()
        );
        progress = written.unwrap();
        std::fs::remove_file(journal.directory().join("capture/events/data.parquet")).unwrap();
        assert_eq!(finish(&ctx, &journal, &mut progress).revision.get(), 1);
        // Terminal evidence is stronger than any subsequent source or data loss.
        progress.plan_digest = grv_types::sha256(b"invalid after terminal");
        assert_eq!(finish(&ctx, &journal, &mut progress).revision.get(), 1);
    }
    #[test]
    fn sealed_replay_does_not_read_capture_and_requires_exact_expected_entries() {
        let ctx = context();
        let (journal, mut progress) = prepare(&ctx, 1, 0.into(), PushPolicy::All, &[2]);
        let finalizer = PushFinalizer::new(&ctx.store, &ctx.clock, 60).unwrap();
        assert!(
            finalizer
                .finalize(&mut progress, &journal, ctx.root.path(), |next| {
                    if next.lease.is_some() {
                        Err(crash())
                    } else {
                        Ok(())
                    }
                })
                .is_err()
        );
        assert!(progress.sealed.is_some());
        std::fs::remove_file(journal.directory().join("capture/events/data.parquet")).unwrap();
        let mut wrong = progress.clone();
        wrong.sealed.as_mut().unwrap().entries.clear();
        assert_eq!(
            finalizer
                .finalize(&mut wrong, &journal, ctx.root.path(), |_| Ok(()))
                .unwrap_err()
                .code,
            ErrorCode::ExtractionIncomplete
        );
        assert_eq!(finish(&ctx, &journal, &mut progress).revision.get(), 1);
    }
    #[test]
    fn recorded_publication_is_fenced_before_retry_and_known_commit_precedes_capture_checks() {
        let ctx = context();
        let (journal, mut progress) = prepare(&ctx, 1, 0.into(), PushPolicy::All, &[8]);
        let finalizer = PushFinalizer::new(&ctx.store, &ctx.clock, 60).unwrap();
        assert!(
            finalizer
                .finalize(&mut progress, &journal, ctx.root.path(), |next| {
                    if next.terminal.is_some() {
                        Err(crash())
                    } else {
                        Ok(())
                    }
                })
                .is_err()
        );
        assert!(progress.publication.is_some());
        assert!(progress.terminal.is_none());
        std::fs::remove_file(journal.directory().join("capture/events/data.parquet")).unwrap();
        progress.plan_digest = grv_types::sha256(b"irrelevant after proven commit");
        assert_eq!(
            finalizer
                .finalize_checked(
                    &mut progress,
                    &journal,
                    ctx.root.path(),
                    |_| Ok(()),
                    || panic!("committed proof must precede offline checks")
                )
                .unwrap()
                .revision
                .get(),
            1
        );
    }
    #[test]
    fn unfinished_identity_gate_runs_before_allocation_and_preserves_the_fixed_plan() {
        let ctx = context();
        let (journal, mut progress) = prepare(&ctx, 1, 0.into(), PushPolicy::All, &[8]);
        let finalizer = PushFinalizer::new(&ctx.store, &ctx.clock, 60).unwrap();
        let before = progress.plan_digest.clone();
        let error = finalizer
            .finalize_checked(
                &mut progress,
                &journal,
                ctx.root.path(),
                |_| panic!("must not mutate progress"),
                || {
                    Err(public_error(
                        ErrorCode::RequestMismatch,
                        "fixed adapter changed",
                    ))
                },
            )
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::RequestMismatch);
        assert_eq!(progress.plan_digest, before);
        assert!(matches!(progress.groups[0], GroupProgress::Pending));
    }
    #[test]
    fn acquisition_and_allocation_effects_require_durable_whole_progress() {
        let ctx = context();
        let (journal, mut progress) = prepare(&ctx, 1, 0.into(), PushPolicy::All, &[8]);
        let finalizer = PushFinalizer::new(&ctx.store, &ctx.clock, 60).unwrap();
        assert!(
            finalizer
                .finalize(&mut progress, &journal, ctx.root.path(), |_| Err(crash()))
                .is_err()
        );
        assert!(matches!(progress.groups[0], GroupProgress::Pending));
        assert!(
            matches!(ctx.store.backend.head(&ObjectKey::new("datasets/data/events/.claim").unwrap()), Err(e) if e.kind == grv_storage::ErrorKind::NotFound)
        );
        assert_eq!(finish(&ctx, &journal, &mut progress).revision.get(), 1);
    }
    #[test]
    fn uncommitted_publication_is_fenced_and_replaced_with_fresh_identity_after_proof() {
        let ctx = context();
        let (journal, mut progress) = prepare(&ctx, 1, 0.into(), PushPolicy::All, &[8]);
        let finalizer = PushFinalizer::new(&ctx.store, &ctx.clock, 60).unwrap();
        let mut fsynced = None;
        assert!(
            finalizer
                .finalize(&mut progress, &journal, ctx.root.path(), |next| {
                    if next.publication.is_some() {
                        fsynced = Some(next.clone());
                        return Err(crash());
                    }
                    Ok(())
                })
                .is_err()
        );
        // Crash after the journal fsync but before the callback acknowledged it.
        progress = fsynced.unwrap();
        let old = progress
            .publication
            .as_ref()
            .unwrap()
            .operation
            .operation_id
            .clone();
        std::fs::remove_file(journal.directory().join("capture/events/data.parquet")).unwrap();
        let outcome = finish(&ctx, &journal, &mut progress);
        assert_eq!(outcome.revision.get(), 2);
        assert_eq!(progress.uncommitted.len(), 1);
        assert_eq!(progress.uncommitted[0].operation.operation_id, old);
        assert_ne!(
            progress
                .publication
                .as_ref()
                .unwrap()
                .operation
                .operation_id,
            old
        );
    }
    #[test]
    fn lost_seal_ack_adopts_exact_durable_run_without_capture_or_new_allocation() {
        let ctx = context();
        let (journal, mut progress) = prepare(&ctx, 1, 0.into(), PushPolicy::All, &[4]);
        let finalizer = PushFinalizer::new(&ctx.store, &ctx.clock, 60).unwrap();
        let mut written = None;
        assert!(
            finalizer
                .finalize(&mut progress, &journal, ctx.root.path(), |next| {
                    if matches!(next.groups[0], GroupProgress::Written { .. }) {
                        written = Some(next.clone());
                    }
                    if next.sealed.is_some() {
                        return Err(crash());
                    }
                    Ok(())
                })
                .is_err()
        );
        assert!(progress.sealed.is_none());
        // Even finalization acknowledgement was lost from the restored journal.
        progress = written.unwrap();
        std::fs::remove_file(journal.directory().join("capture/events/data.parquet")).unwrap();
        assert_eq!(finish(&ctx, &journal, &mut progress).revision.get(), 1);
        assert!(matches!(
            progress.groups[0],
            GroupProgress::Finalized { .. }
        ));
        assert_eq!(progress.sealed.unwrap().entries[0].version.get(), 1);
    }
    #[test]
    fn no_op_outcome_is_durable_before_lease_release_and_failed_fsync_is_retryable() {
        let ctx = context();
        let (journal, mut initial) = prepare(&ctx, 1, 0.into(), PushPolicy::All, &[4]);
        finish(&ctx, &journal, &mut initial);
        let (journal, mut progress) = prepare(&ctx, 2, 1.into(), PushPolicy::Changed, &[4]);
        let finalizer = PushFinalizer::new(&ctx.store, &ctx.clock, 60).unwrap();
        assert!(
            finalizer
                .finalize(&mut progress, &journal, ctx.root.path(), |next| {
                    if next.terminal.is_some() {
                        Err(crash())
                    } else {
                        Ok(())
                    }
                })
                .is_err()
        );
        assert!(progress.terminal.is_none());
        let outcome = finish(&ctx, &journal, &mut progress);
        assert!(outcome.no_op);
        assert_eq!(outcome.revision.get(), 1);
    }
    #[test]
    fn intervening_revision_rejects_changed_noop_without_implicit_rebase() {
        let ctx = context();
        let (journal, mut first) = prepare(&ctx, 1, 0.into(), PushPolicy::All, &[1]);
        finish(&ctx, &journal, &mut first);
        let (stale_journal, mut stale) = prepare(&ctx, 2, 1.into(), PushPolicy::Changed, &[1]);
        let (journal, mut newer) = prepare(&ctx, 3, 1.into(), PushPolicy::All, &[2]);
        finish(&ctx, &journal, &mut newer);
        let error = PushFinalizer::new(&ctx.store, &ctx.clock, 60)
            .unwrap()
            .finalize(&mut stale, &stale_journal, ctx.root.path(), |_| Ok(()))
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::StateConflict);
        assert!(stale.terminal.is_none());
    }
    #[test]
    fn partitioned_zero_rows_omit_base_groups_and_publish_complete_empty_snapshot() {
        let ctx = context();
        let (journal, mut initial) = prepare_data(&ctx, 1, 0.into(), PushPolicy::All, &[4], true);
        assert_eq!(finish(&ctx, &journal, &mut initial).revision.get(), 1);
        let (journal, mut empty) = prepare_data(&ctx, 2, 1.into(), PushPolicy::Changed, &[], true);
        assert!(empty.plan.groups.is_empty());
        assert_eq!(empty.plan.omissions.len(), 1);
        assert!(empty.plan.omissions[0].partition.is_some());
        assert_eq!(finish(&ctx, &journal, &mut empty).revision.get(), 2);
        assert!(
            revision::read(
                &ctx.store,
                &Name::new("data").unwrap(),
                2.into(),
                ctx.root.path()
            )
            .unwrap()
            .state
            .is_empty()
        );
        assert!(empty.sealed.unwrap().entries.is_empty());
    }
}
