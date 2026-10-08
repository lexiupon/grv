//! Dataset-local GC discovery and durable application. Preview is strictly
//! read-only. Applying never treats preview output as deletion authority.
use crate::{
    admin::{
        Admin, AdminIntent, AdminProgress, Assessment, PruneOutcome, PruneProgress, PruneRequest,
    },
    clock::{Clock, ExpiryObservation, parse},
    holds::{Holds, hold_key},
    ownership::{Ownership, RecoveryIntent, RunOwner},
    publication::LeaseOwner,
    revision,
    store::{Result, Store, backend_error, public_error},
};
use grv_storage::{Backend, ListEntry, ListMode, ObjectKey, ObjectPrefix, model::*};
use grv_types::{Digest, ErrorCode, Name, PublicError, RunId};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Mutex,
};

type LiveClaims = BTreeMap<RunId, Vec<(ObjectKey, Option<VersionTarget>)>>;

fn key(dataset: &Name, suffix: &str) -> ObjectKey {
    ObjectKey::new(format!("datasets/{dataset}/{suffix}")).expect("validated GC path")
}
fn integrity(message: &str) -> PublicError {
    public_error(ErrorCode::IntegrityFailure, message)
}
fn bounded(count: usize) -> Result<()> {
    if count > 131_072 {
        Err(public_error(
            ErrorCode::UnsupportedCapability,
            "GC metadata exceeds bounded workspace",
        ))
    } else {
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CandidateState {
    Eligible,
    Protected,
    NeedsHoldRelease,
    NeedsRecovery,
    AlreadyPruned,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcCandidate {
    pub target: VersionTarget,
    pub state: CandidateState,
    pub reasons: Vec<String>,
    pub bytes: Option<Counter>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcRun {
    pub run_id: RunId,
    pub phase: RunPhase,
    pub expired: bool,
    pub safe_to_recover: bool,
    pub blocking_claims: Vec<VersionTarget>,
    pub unresolved_claims: Vec<ObjectKey>,
    pub sealed_file_missing: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcHold {
    pub hold: HoldRecord,
    pub releasable: bool,
    pub consumer_needs_recovery: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcPreview {
    pub dataset: Name,
    pub candidates: Vec<GcCandidate>,
    pub eligible: Vec<VersionTarget>,
    pub protected: Vec<VersionTarget>,
    pub already_pruned: Vec<VersionTarget>,
    pub runs: Vec<GcRun>,
    pub holds: Vec<GcHold>,
    pub needs_recovery: Vec<RunId>,
    pub needs_hold_release: Vec<ObjectKey>,
    pub needs_consumer_recovery: Vec<ObjectKey>,
    pub pending: Option<String>,
    pub known_bytes: Counter,
    pub unknown_size_count: usize,
}
/// Supplied only by a trusted coordinator after it checks its private journal
/// and OS/host stop proof. A timeout, lease takeover, or recovered sealed run is
/// never this evidence. The exact pre-takeover run epoch is checked by GC.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoppedWriterEvidence {
    dataset: Name,
    run_id: RunId,
    owner_token: OwnerToken,
    mutation_id: grv_types::Uuid,
    proof: Digest,
}
impl StoppedWriterEvidence {
    pub fn verified(dataset: Name, control: &RunControl, proof: Digest) -> Self {
        Self {
            dataset,
            run_id: control.run_id.clone(),
            owner_token: control.owner_token.clone(),
            mutation_id: control.mutation_id.clone(),
            proof,
        }
    }
    pub(crate) fn matches(&self, dataset: &Name, control: &RunControl) -> bool {
        self.dataset == *dataset
            && self.run_id == control.run_id
            && self.owner_token == control.owner_token
            && self.mutation_id == control.mutation_id
    }
    pub fn proof(&self) -> &Digest {
        &self.proof
    }
}
pub trait WriterAttester {
    fn stopped(
        &self,
        dataset: &Name,
        control: &RunControl,
    ) -> Result<Option<StoppedWriterEvidence>>;
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum GcProgress {
    Prepared,
    Start {
        completed_operation_ids: Vec<RunId>,
    },
    Recover {
        remaining: Vec<RunId>,
        recovered: Vec<RunId>,
        completed_operation_ids: Vec<RunId>,
    },
    RecoverRun {
        intent: Box<RecoveryIntent>,
        stopped: Option<StoppedWriterEvidence>,
        remaining: Vec<RunId>,
        recovered: Vec<RunId>,
        completed_operation_ids: Vec<RunId>,
    },
    SealRun {
        owner: Box<RunOwner>,
        remaining: Vec<RunId>,
        recovered: Vec<RunId>,
        completed_operation_ids: Vec<RunId>,
    },
    Release {
        intent: Option<Box<AdminIntent>>,
        progress: Box<AdminProgress>,
        recovered: Vec<RunId>,
        completed_operation_ids: Vec<RunId>,
    },
    Prune {
        request: Box<PruneRequest>,
        progress: Box<PruneProgress>,
        recovered: Vec<RunId>,
        completed_operation_ids: Vec<RunId>,
        released_holds: Vec<ObjectKey>,
        candidates: Vec<GcCandidate>,
    },
    Complete {
        outcome: Box<GcOutcome>,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcOutcome {
    pub dataset: Name,
    pub prune: PruneOutcome,
    pub recovered: Vec<RunId>,
    pub completed_operation_ids: Vec<RunId>,
    pub candidates: Vec<GcCandidate>,
    pub released_holds: Vec<ObjectKey>,
    pub waiting_runs: Vec<GcRun>,
    pub waiting_holds: Vec<GcHold>,
    pub maintenance_error: Option<PublicError>,
    pub waiting_observation_complete: bool,
}
pub struct Gc<'a, B: Backend> {
    admin: Admin<'a, B>,
    ownership: Ownership<'a, B>,
    observations: Mutex<BTreeMap<String, ExpiryObservation>>,
}
impl<'a, B: Backend> Gc<'a, B> {
    pub fn new(store: &'a Store<B>, clock: &'a dyn Clock, ttl: u64) -> Result<Self> {
        Ok(Self {
            admin: Admin::new(store, clock, ttl)?,
            ownership: Ownership::new(store, clock, ttl)?,
            observations: Mutex::new(BTreeMap::new()),
        })
    }
    fn targets(&self, dataset: &Name) -> Result<Vec<VersionTarget>> {
        let mut targets = BTreeMap::new();
        let prefix = ObjectPrefix::new(format!("datasets/{dataset}/")).unwrap();
        let mut tables = vec![];
        for item in self
            .admin
            .store
            .backend
            .list(&prefix, ListMode::Children)
            .map_err(backend_error)?
        {
            if let ListEntry::Prefix(path) = item {
                let table = path
                    .as_str()
                    .strip_prefix(prefix.as_str())
                    .ok_or_else(|| integrity("dataset listing escaped its prefix"))?
                    .trim_end_matches('/');
                if table.starts_with('.') {
                    continue;
                }
                tables
                    .push(Name::new(table).map_err(|_| integrity("noncanonical table directory"))?);
            }
        }
        bounded(tables.len())?;
        let mut budget = 0usize;
        for table in tables {
            let layout: TableLayout = self
                .admin
                .read(&key(dataset, &format!("{table}/.layout.json")))?;
            if layout.table != table {
                return Err(integrity("GC layout differs from table directory"));
            }
            let prefix = ObjectPrefix::new(format!("datasets/{dataset}/{table}/")).unwrap();
            for item in self
                .admin
                .store
                .backend
                .list(&prefix, ListMode::Recursive)
                .map_err(backend_error)?
            {
                let ListEntry::Object(path) = item else {
                    return Err(integrity("recursive version discovery returned a prefix"));
                };
                budget = budget.saturating_add(path.as_str().len() + 512);
                if budget > 128 * 1024 * 1024 {
                    return Err(public_error(
                        ErrorCode::UnsupportedCapability,
                        "version discovery exceeds 128MiB metadata budget",
                    ));
                }
                let relative = path
                    .as_str()
                    .strip_prefix(prefix.as_str())
                    .ok_or_else(|| integrity("version listing escaped table"))?;
                let Some((folder, _)) = relative.rsplit_once('/') else {
                    continue;
                };
                let (partition, version) = folder.rsplit_once('/').unwrap_or(("", folder));
                let Some(number) = version.strip_prefix("version=") else {
                    continue;
                };
                let value = number
                    .parse::<u64>()
                    .ok()
                    .filter(|value| *value > 0 && value.to_string() == number)
                    .ok_or_else(|| integrity("noncanonical version directory"))?;
                let version = Counter::new(value).map_err(backend_error)?;
                let partition_values = layout.parse_partition(partition).map_err(backend_error)?;
                targets.insert(
                    (table.clone(), partition.to_owned(), version),
                    VersionTarget {
                        table: table.clone(),
                        partition: partition_values,
                        version,
                    },
                );
                bounded(targets.len())?;
            }
        }
        Ok(targets.into_values().collect())
    }
    fn live_claims(&self, dataset: &Name) -> Result<LiveClaims> {
        let prefix = ObjectPrefix::new(format!("datasets/{dataset}/")).unwrap();
        let mut found = BTreeMap::<RunId, Vec<(ObjectKey, Option<VersionTarget>)>>::new();
        let mut budget = 0usize;
        for item in self
            .admin
            .store
            .backend
            .list(&prefix, ListMode::Recursive)
            .map_err(backend_error)?
        {
            let ListEntry::Object(path) = item else {
                return Err(integrity("recursive claim scan returned prefix"));
            };
            budget = budget.saturating_add(path.as_str().len() + 512);
            if budget > 128 * 1024 * 1024 {
                return Err(public_error(
                    ErrorCode::UnsupportedCapability,
                    "claim scan exceeds metadata budget",
                ));
            }
            let Some(relative) = path.as_str().strip_prefix(prefix.as_str()) else {
                return Err(integrity("claim scan escaped dataset"));
            };
            let Some(folder) = relative.strip_suffix("/.claim") else {
                continue;
            };
            let (table, partition) = folder.split_once('/').unwrap_or((folder, ""));
            if table.starts_with('.') {
                continue;
            }
            let table = Name::new(table).map_err(|_| integrity("noncanonical claim table"))?;
            let layout: TableLayout = self
                .admin
                .read(&key(dataset, &format!("{table}/.layout.json")))?;
            if layout.table != table {
                return Err(integrity("claim table layout differs"));
            }
            let partition = layout.parse_partition(partition).map_err(backend_error)?;
            let claim: ClaimRecord = self.admin.read(&path)?;
            if claim.phase().map_err(backend_error)? == ClaimPhase::Released {
                continue;
            }
            let control: RunControl = self.admin.read(&key(
                dataset,
                &format!(".runs/{}.control.json", claim.holder),
            ))?;
            if control.run_id != claim.holder {
                return Err(integrity("claim holder control differs from path"));
            }
            let target = claim.version.map(|version| VersionTarget {
                table,
                partition,
                version,
            });
            let list = found.entry(claim.holder).or_default();
            list.push((path, target));
            bounded(list.len())?;
        }
        Ok(found)
    }
    fn runs(&self, dataset: &Name) -> Result<Vec<GcRun>> {
        self.recovery_runs(dataset, None)
    }
    /// Read-only run/claim discovery shared with isolated recovery. A targeted
    /// invocation never reads unrelated run controls or allocation records.
    pub(crate) fn recovery_runs(
        &self,
        dataset: &Name,
        target: Option<&RunId>,
    ) -> Result<Vec<GcRun>> {
        let parameters: StoreParameters = self.admin.read(&ObjectKey::new("grv.json").unwrap())?;
        let live_claims = self.live_claims(dataset)?;
        let prefix = ObjectPrefix::new(format!("datasets/{dataset}/.runs/")).unwrap();
        let mut result = vec![];
        for item in self
            .admin
            .store
            .backend
            .list(&prefix, ListMode::Children)
            .map_err(backend_error)?
        {
            let ListEntry::Object(path) = item else {
                continue;
            };
            let Some(id) = path
                .as_str()
                .strip_prefix(prefix.as_str())
                .and_then(|name| name.strip_suffix(".control.json"))
            else {
                continue;
            };
            if target.is_some_and(|target| target.as_str() != id) {
                continue;
            }
            let id = RunId::new(id).map_err(|_| integrity("noncanonical run control filename"))?;
            let (bytes, meta) = self
                .admin
                .store
                .backend
                .read_bytes(&path, 64 * 1024 * 1024)
                .map_err(backend_error)?;
            let control: RunControl = decode_record(&bytes).map_err(backend_error)?;
            if control.run_id != id {
                return Err(integrity("GC run control differs from filename"));
            }
            let expired = control.expires_at.as_ref().is_some_and(|expiry| {
                self.observations
                    .lock()
                    .unwrap()
                    .entry(path.as_str().to_owned())
                    .or_default()
                    .expired(&meta.validator, expiry, self.admin.clock, &parameters)
            });
            let unresolved_claims = live_claims
                .get(&id)
                .map(|claims| {
                    claims
                        .iter()
                        .map(|(path, _)| path.clone())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let mut blocking_claims = live_claims
                .get(&id)
                .map(|claims| {
                    claims
                        .iter()
                        .filter_map(|(_, target)| target.clone())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let allocations =
                ObjectPrefix::new(format!("datasets/{dataset}/.runs/{id}.allocations/")).unwrap();
            for item in self
                .admin
                .store
                .backend
                .list(&allocations, ListMode::Children)
                .map_err(backend_error)?
            {
                let ListEntry::Object(path) = item else {
                    return Err(integrity("allocation scan contains nested prefix"));
                };
                let record: AllocationRecord = self.admin.read(&path)?;
                if record.run_id != id
                    || record
                        .claim_token
                        .allocation_key(dataset, &id)
                        .map_err(backend_error)?
                        != path
                {
                    return Err(integrity("GC allocation differs from run/path"));
                }
                if record.state == AllocationState::Allocated {
                    let layout: TableLayout = self
                        .admin
                        .read(&key(dataset, &format!("{}/.layout.json", record.table)))?;
                    if layout.table != record.table {
                        return Err(integrity("allocation layout identity differs"));
                    }
                    let partition = layout
                        .partition_path(&record.partition)
                        .map_err(backend_error)?;
                    let folder = if partition.is_empty() {
                        record.table.to_string()
                    } else {
                        format!("{}/{partition}", record.table)
                    };
                    if let Some(claim) = self
                        .admin
                        .optional::<ClaimRecord>(&key(dataset, &format!("{folder}/.claim")))?
                        && claim.phase().map_err(backend_error)? == ClaimPhase::Allocated
                        && claim.holder == id
                        && claim.token == record.claim_token
                        && claim.version == Some(record.version)
                    {
                        let target = VersionTarget {
                            table: record.table,
                            partition: record.partition,
                            version: record.version,
                        };
                        if !blocking_claims.contains(&target) {
                            blocking_claims.push(target);
                        }
                    }
                }
                bounded(blocking_claims.len())?;
            }
            let sealed_file_missing = if control.phase == RunPhase::Sealed {
                match self
                    .admin
                    .optional::<SealedRun>(&key(dataset, &format!(".runs/{id}.json")))?
                {
                    Some(sealed) => {
                        sealed.validate_control(&control).map_err(backend_error)?;
                        false
                    }
                    None => true,
                }
            } else {
                false
            };
            result.push(GcRun {
                run_id: id,
                phase: control.phase,
                expired,
                safe_to_recover: blocking_claims.is_empty() && unresolved_claims.is_empty(),
                blocking_claims,
                unresolved_claims,
                sealed_file_missing,
            });
            bounded(result.len())?;
        }
        result.sort_by(|a, b| a.run_id.cmp(&b.run_id));
        Ok(result)
    }
    fn holds(&self, dataset: &Name, scratch: &Path) -> Result<Vec<GcHold>> {
        let holds = Holds::new(self.admin.store, self.admin.clock, scratch);
        let prefix = ObjectPrefix::new(format!("datasets/{dataset}/.holds/")).unwrap();
        let mut result = vec![];
        for item in self
            .admin
            .store
            .backend
            .list(&prefix, ListMode::Recursive)
            .map_err(backend_error)?
        {
            let ListEntry::Object(path) = item else {
                return Err(integrity("recursive hold scan returned prefix"));
            };
            let hold: HoldRecord = self.admin.read(&path)?;
            if hold.dataset != *dataset || hold_key(&hold) != path {
                return Err(integrity("GC hold differs from source path"));
            }
            if !holds.active(&hold)? {
                continue;
            }
            let releasable = holds.releasable(&hold)?;
            let consumer = self.admin.optional::<RunControl>(&key(
                &hold.target_dataset,
                &format!(".runs/{}.control.json", hold.target_run_id),
            ))?;
            if consumer
                .as_ref()
                .is_some_and(|c| c.run_id != hold.target_run_id)
            {
                return Err(integrity("consumer control differs from hold target"));
            }
            let consumer_needs_recovery = !releasable
                && consumer.as_ref().is_some_and(|c| {
                    c.phase != RunPhase::Sealed
                        && c.expires_at
                            .as_ref()
                            .is_some_and(|expiry| parse(&self.admin.clock.now()) >= parse(expiry))
                });
            result.push(GcHold {
                hold,
                releasable,
                consumer_needs_recovery,
            });
            bounded(result.len())?;
        }
        Ok(result)
    }
    fn bytes(&self, dataset: &Name, target: &VersionTarget) -> Result<Option<Counter>> {
        let layout: TableLayout = self
            .admin
            .read(&key(dataset, &format!("{}/.layout.json", target.table)))?;
        let partition = layout
            .partition_path(&target.partition)
            .map_err(backend_error)?;
        let folder = if partition.is_empty() {
            format!("{}/version={}", target.table, target.version)
        } else {
            format!("{}/{partition}/version={}", target.table, target.version)
        };
        let prefix = ObjectPrefix::new(format!("datasets/{dataset}/{folder}/")).unwrap();
        let mut bytes = 0u64;
        for item in self
            .admin
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
                .strip_prefix(prefix.as_str())
                .ok_or_else(|| integrity("byte scan escaped version"))?;
            if name == "manifest.json"
                || name == "data.parquet"
                || name
                    .strip_prefix("data-")
                    .and_then(|n| n.strip_suffix(".parquet"))
                    .is_some_and(|n| {
                        n.parse::<u64>()
                            .ok()
                            .is_some_and(|v| v > 0 && v.to_string() == n)
                    })
            {
                match self.admin.store.backend.head(&path) {
                    Ok(meta) => {
                        let Some(total) = bytes
                            .checked_add(meta.size.get())
                            .filter(|n| *n <= i64::MAX as u64)
                        else {
                            return Ok(None);
                        };
                        bytes = total;
                    }
                    Err(_) => return Ok(None),
                }
            }
        }
        Ok(Some(Counter::new(bytes).map_err(backend_error)?))
    }
    fn observe(&self, dataset: &Name, scratch: &Path, latest: &Latest) -> Result<GcPreview> {
        let targets = self.targets(dataset)?;
        let assessment = self
            .admin
            .assess_observed(dataset, latest, &targets, scratch, false)?;
        let runs = self.runs(dataset)?;
        let holds = self.holds(dataset, scratch)?;
        let mut candidates = vec![];
        let mut known = 0u64;
        let mut unknown = 0;
        for target in targets {
            let (state, reasons) = if assessment.already_pruned.contains(&target) {
                (
                    CandidateState::AlreadyPruned,
                    vec!["durable pruning tombstone; residual data may be swept".into()],
                )
            } else if assessment.protected.contains(&target) {
                let mut reasons = vec![];
                let mut independent = false;
                let mut needs_recovery = false;
                let mut needs_hold_release = false;
                for scope in [
                    PinScope::Table(TableScope {
                        table: target.table.clone(),
                    }),
                    PinScope::Partition(PartitionScope {
                        table: target.table.clone(),
                        partition: target.partition.clone(),
                    }),
                    PinScope::Version(VersionScope {
                        table: target.table.clone(),
                        partition: target.partition.clone(),
                        version: target.version,
                    }),
                ] {
                    for pin in self.admin.observed_pins(dataset, &scope)? {
                        independent = true;
                        reasons.push(format!("active scoped pin {}", pin.pin_id));
                    }
                }
                for hold in &holds {
                    let revision =
                        revision::read(self.admin.store, dataset, hold.hold.revision, scratch)?;
                    let layout: TableLayout = self
                        .admin
                        .read(&key(dataset, &format!("{}/.layout.json", target.table)))?;
                    let partition = layout
                        .partition_path(&target.partition)
                        .map_err(backend_error)?;
                    if revision
                        .state
                        .get(&(target.table.clone(), partition))
                        .is_some_and(|e| e.version == target.version)
                    {
                        if hold.releasable {
                            needs_hold_release = true;
                        } else if hold.consumer_needs_recovery {
                            needs_recovery = true;
                        } else {
                            independent = true;
                        }
                        reasons.push(format!(
                            "active hold {}{}",
                            hold.hold.retention_id,
                            if hold.releasable {
                                "; eligible only after committed hold release"
                            } else if hold.consumer_needs_recovery {
                                "; consumer recovery in another dataset required"
                            } else {
                                ""
                            }
                        ));
                    }
                }
                for run in &runs {
                    if run.blocking_claims.contains(&target) {
                        if run.expired {
                            needs_recovery = true;
                        } else {
                            independent = true;
                        }
                        reasons.push(format!(
                            "allocated claim for run {}; stopped writer not established",
                            run.run_id
                        ));
                    }
                }
                // Exact kept-revision/grace and pending ownership causes are
                // already validated by the core assessment. They are refined
                // below without taking a lease or writing missing receipts.
                independent |=
                    self.revision_reasons(dataset, latest, &target, scratch, &mut reasons)?;
                if reasons.is_empty() {
                    let layout: TableLayout = self
                        .admin
                        .read(&key(dataset, &format!("{}/.layout.json", target.table)))?;
                    let partition = layout
                        .partition_path(&target.partition)
                        .map_err(backend_error)?;
                    let folder = if partition.is_empty() {
                        format!("{}/version={}", target.table, target.version)
                    } else {
                        format!("{}/{partition}/version={}", target.table, target.version)
                    };
                    if let Some(manifest) = self.admin.optional::<VersionManifest>(&key(
                        dataset,
                        &format!("{folder}/manifest.json"),
                    ))? {
                        let run = runs
                            .iter()
                            .find(|run| run.run_id == manifest.run_id)
                            .ok_or_else(|| integrity("pending manifest run control is missing"))?;
                        if run.phase != RunPhase::Sealed && run.expired {
                            needs_recovery = true;
                        } else {
                            independent = true;
                        }
                    } else {
                        independent = true;
                    }
                    reasons.push(
                        "pending version protected by unsealed run or unexpired sealed-run grace"
                            .into(),
                    );
                }
                let state = if !independent && needs_hold_release && !needs_recovery {
                    CandidateState::NeedsHoldRelease
                } else if !independent && needs_recovery && !needs_hold_release {
                    CandidateState::NeedsRecovery
                } else {
                    CandidateState::Protected
                };
                (state, reasons)
            } else if assessment.eligible.contains(&target) {
                (
                    CandidateState::Eligible,
                    vec![
                        "no active pin, kept-revision reference, or pending ownership protection"
                            .into(),
                    ],
                )
            } else {
                continue;
            };
            let bytes = self.bytes(dataset, &target)?;
            if let Some(bytes) = bytes {
                known = known
                    .checked_add(bytes.get())
                    .filter(|v| *v <= i64::MAX as u64)
                    .ok_or_else(|| integrity("GC aggregate byte estimate exceeds counter"))?;
            } else {
                unknown += 1;
            }
            candidates.push(GcCandidate {
                target,
                state,
                reasons,
                bytes,
            });
        }
        Ok(GcPreview {
            dataset: dataset.clone(),
            candidates,
            eligible: assessment.eligible,
            protected: assessment.protected,
            already_pruned: assessment.already_pruned,
            needs_recovery: runs
                .iter()
                .filter(|run| {
                    run.expired && run.phase != RunPhase::Sealed || run.sealed_file_missing
                })
                .map(|run| run.run_id.clone())
                .collect(),
            needs_hold_release: holds
                .iter()
                .filter(|hold| hold.releasable)
                .map(|hold| hold_key(&hold.hold))
                .collect(),
            needs_consumer_recovery: holds
                .iter()
                .filter(|hold| hold.consumer_needs_recovery)
                .map(|hold| hold_key(&hold.hold))
                .collect(),
            runs,
            holds,
            pending: latest.pending.clone(),
            known_bytes: Counter::new(known).map_err(backend_error)?,
            unknown_size_count: unknown,
        })
    }
    fn revision_reasons(
        &self,
        dataset: &Name,
        latest: &Latest,
        target: &VersionTarget,
        scratch: &Path,
        reasons: &mut Vec<String>,
    ) -> Result<bool> {
        let layout: TableLayout = self
            .admin
            .read(&key(dataset, &format!("{}/.layout.json", target.table)))?;
        let partition = layout
            .partition_path(&target.partition)
            .map_err(backend_error)?;
        let mut independent = false;
        let mut revision = latest.revision;
        let mut seen = BTreeSet::new();
        while revision.get() != 0 {
            if !seen.insert(revision) {
                return Err(integrity("cyclic revision chain"));
            }
            bounded(seen.len())?;
            let state = revision::read(self.admin.store, dataset, revision, scratch)?;
            if state
                .state
                .get(&(target.table.clone(), partition.clone()))
                .is_some_and(|e| e.version == target.version)
            {
                for protection in self
                    .admin
                    .remaining_revision_protections(dataset, revision, scratch)?
                {
                    let identity = &protection.object;
                    if identity
                        .table
                        .as_ref()
                        .is_some_and(|table| table != &target.table)
                        || identity
                            .partition
                            .as_ref()
                            .is_some_and(|p| p != &target.partition)
                        || identity
                            .version
                            .is_some_and(|version| version.get() != target.version.get())
                    {
                        continue;
                    }
                    if let Some(protecting) = identity.revision {
                        let protecting = revision::read(
                            self.admin.store,
                            dataset,
                            Counter::new(protecting.get()).map_err(backend_error)?,
                            scratch,
                        )?;
                        let protects_target = protecting
                            .state
                            .get(&(target.table.clone(), partition.clone()))
                            .is_some_and(|entry| entry.version == target.version);
                        if !protects_target {
                            continue;
                        }
                    }
                    independent |= !matches!(protection.kind, crate::admin::ProtectionKind::Hold);
                    // This helper includes reused versions of a revision; retain
                    // the exact protecting object in the human-readable reason.
                    reasons.push(format!(
                        "{:?}: {}",
                        protection.kind,
                        serde_json::to_string(&protection.object).unwrap()
                    ));
                }
                break;
            }
            revision = state.previous_revision;
        }
        Ok(independent)
    }
    pub fn preview(&self, dataset: &Name, scratch: &Path) -> Result<GcPreview> {
        let latest = revision::read_latest(self.admin.store, dataset)?
            .map(|v| v.0)
            .unwrap_or_else(Latest::empty);
        self.observe(dataset, scratch, &latest)
    }
    fn scoped<T>(&self, owner: &mut LeaseOwner, work: impl FnOnce() -> Result<T>) -> Result<T> {
        self.admin.publisher.owned(owner)?;
        let (value, renewed) = crate::renewal::during(
            owner.clone(),
            self.admin.publisher.renewal_interval(),
            |owner| self.admin.publisher.renew(owner),
            |watch| {
                watch.check()?;
                let value = work()?;
                watch.check()?;
                Ok(value)
            },
        )?;
        *owner = renewed;
        Ok(value)
    }
    /// Caller journals the whole progress record before each effect. This method
    /// owns scoped dataset/run renewers; do not wrap another dataset renewer.
    pub fn apply(
        &self,
        owner: &mut LeaseOwner,
        progress: &mut GcProgress,
        scratch: &Path,
        attester: Option<&dyn WriterAttester>,
        mut persist: impl FnMut(&GcProgress) -> Result<()>,
    ) -> Result<GcOutcome> {
        let dataset = owner.intent.dataset.clone();
        if let GcProgress::Complete { outcome } = progress {
            if outcome.dataset != dataset {
                return Err(integrity("GC outcome belongs to another dataset"));
            }
            return Ok((**outcome).clone());
        }
        if matches!(progress, GcProgress::Prepared) {
            persist(progress)?;
            let (before, _) = self.admin.publisher.owned(owner)?;
            let mut completed_operation_ids = vec![];
            if let Some(path) = before.pending {
                let operation: OperationRecord = self.admin.read(&key(&dataset, &path))?;
                if operation.dataset != dataset
                    || path != format!(".states/operations/{}.json", operation.operation_id)
                {
                    return Err(integrity("pending GC operation identity differs from path"));
                }
                completed_operation_ids.push(operation.operation_id);
            }
            let next = GcProgress::Start {
                completed_operation_ids,
            };
            persist(&next)?;
            *progress = next;
        }
        if let GcProgress::Start {
            completed_operation_ids,
        } = progress.clone()
        {
            self.admin.publisher.finish_pending(owner)?;
            let initial = self.scoped(owner, || {
                let targets = self.targets(&dataset)?;
                let latest = revision::read_latest(self.admin.store, &dataset)?
                    .ok_or_else(|| integrity("owned GC lost LATEST"))?
                    .0;
                let _: Assessment = self
                    .admin
                    .assess_observed(&dataset, &latest, &targets, scratch, true)?;
                self.runs(&dataset)
            })?;
            let mut remaining = vec![];
            for run in initial {
                if run.sealed_file_missing || run.expired && run.phase != RunPhase::Sealed {
                    if run.safe_to_recover || run.sealed_file_missing {
                        remaining.push(run.run_id);
                    } else if let Some(attester) = attester {
                        let control: RunControl = self.admin.read(&key(
                            &dataset,
                            &format!(".runs/{}.control.json", run.run_id),
                        ))?;
                        if let Some(proof) = attester.stopped(&dataset, &control)? {
                            if !proof.matches(&dataset, &control) {
                                return Err(integrity(
                                    "stopped-writer proof differs from run epoch",
                                ));
                            }
                            remaining.push(run.run_id);
                        }
                    }
                }
            }
            let next = GcProgress::Recover {
                remaining,
                recovered: vec![],
                completed_operation_ids,
            };
            persist(&next)?;
            *progress = next;
        }
        loop {
            match progress.clone() {
                GcProgress::Recover {
                    mut remaining,
                    recovered,
                    completed_operation_ids,
                } => {
                    if let Some(id) = remaining.first().cloned() {
                        remaining.remove(0);
                        let observed = self
                            .runs(&dataset)?
                            .into_iter()
                            .find(|run| run.run_id == id)
                            .ok_or_else(|| integrity("targeted recovery control disappeared"))?;
                        let control: RunControl = self
                            .admin
                            .read(&key(&dataset, &format!(".runs/{id}.control.json")))?;
                        if control.phase != RunPhase::Sealed && !observed.expired {
                            return Err(public_error(
                                ErrorCode::StateConflict,
                                "targeted GC recovery run remains live",
                            ));
                        }
                        let mut stopped = None;
                        if !observed.safe_to_recover && control.phase != RunPhase::Sealed {
                            let proof = attester
                                .map(|a| a.stopped(&dataset, &control))
                                .transpose()?
                                .flatten();
                            if !proof
                                .as_ref()
                                .is_some_and(|p| p.matches(&dataset, &control))
                            {
                                return Err(public_error(
                                    ErrorCode::StateConflict,
                                    "targeted GC recovery lacks exact stopped-writer evidence",
                                ));
                            }
                            stopped = proof;
                        }
                        let intent = self.ownership.prepare_run_recovery(dataset.clone(), id)?;
                        if intent.previous() != &control {
                            return Err(public_error(
                                ErrorCode::StateConflict,
                                "run epoch changed before recovery journal",
                            ));
                        }
                        let next = GcProgress::RecoverRun {
                            intent: Box::new(intent),
                            stopped,
                            remaining,
                            recovered,
                            completed_operation_ids,
                        };
                        persist(&next)?;
                        *progress = next;
                    } else {
                        let releaseable = self
                            .scoped(owner, || self.holds(&dataset, scratch))?
                            .into_iter()
                            .filter(|hold| hold.releasable)
                            .map(|hold| HoldRelease {
                                consumer_dataset: hold.hold.target_dataset,
                                revision: hold.hold.revision,
                                retention_id: hold.hold.retention_id,
                            })
                            .collect::<Vec<_>>();
                        let intent = if releaseable.is_empty() {
                            None
                        } else {
                            Some(Box::new(
                                self.admin
                                    .prepare_release_holds(dataset.clone(), releaseable)?,
                            ))
                        };
                        let next = GcProgress::Release {
                            intent,
                            progress: Box::new(AdminProgress::Prepared),
                            recovered,
                            completed_operation_ids,
                        };
                        persist(&next)?;
                        *progress = next;
                    }
                }
                GcProgress::RecoverRun {
                    intent,
                    stopped,
                    remaining,
                    recovered,
                    completed_operation_ids,
                } => {
                    if intent.dataset() != &dataset {
                        return Err(integrity("GC recovery intent belongs to another dataset"));
                    }
                    if stopped
                        .as_ref()
                        .is_some_and(|proof| !proof.matches(&dataset, intent.previous()))
                    {
                        return Err(integrity(
                            "journaled stopped-writer evidence differs from recovery epoch",
                        ));
                    }
                    let run = self.scoped(owner, || self.ownership.recover_authorized(&intent))?;
                    let next = GcProgress::SealRun {
                        owner: Box::new(run),
                        remaining,
                        recovered,
                        completed_operation_ids,
                    };
                    persist(&next)?;
                    *progress = next;
                }
                GcProgress::SealRun {
                    owner: mut run,
                    remaining,
                    mut recovered,
                    completed_operation_ids,
                } => {
                    if run.dataset() != &dataset {
                        return Err(integrity("GC sealing owner belongs to another dataset"));
                    }
                    self.scoped(owner, || self.ownership.seal_renewed(&mut run))?;
                    recovered.push(run.control().run_id.clone());
                    let next = GcProgress::Recover {
                        remaining,
                        recovered,
                        completed_operation_ids,
                    };
                    persist(&next)?;
                    *progress = next;
                }
                GcProgress::Release {
                    intent,
                    progress: inner,
                    recovered,
                    mut completed_operation_ids,
                } => {
                    let mut inner = *inner;
                    let released_holds = if let Some(intent) = intent {
                        let outcome = self.admin.execute(
                            owner,
                            &intent,
                            &mut inner,
                            scratch,
                            |_, inner| {
                                let next = GcProgress::Release {
                                    intent: Some(intent.clone()),
                                    progress: Box::new(inner.clone()),
                                    recovered: recovered.clone(),
                                    completed_operation_ids: completed_operation_ids.clone(),
                                };
                                persist(&next)?;
                                *progress = next;
                                Ok(())
                            },
                        )?;
                        if outcome.committed {
                            completed_operation_ids.push(outcome.operation_id.clone());
                        }
                        self.admin.publisher.finish_pending(owner)?;
                        outcome.markers
                    } else {
                        vec![]
                    };
                    let observed = self.scoped(owner, || self.preview(&dataset, scratch))?;
                    let candidates = observed.candidates;
                    let targets = candidates
                        .iter()
                        .map(|candidate| candidate.target.clone())
                        .collect();
                    let next = GcProgress::Prune {
                        request: Box::new(PruneRequest {
                            dataset: dataset.clone(),
                            targets,
                        }),
                        progress: Box::new(PruneProgress::Prepared),
                        recovered,
                        completed_operation_ids,
                        released_holds,
                        candidates,
                    };
                    persist(&next)?;
                    *progress = next;
                }
                GcProgress::Prune {
                    request,
                    progress: inner,
                    recovered,
                    mut completed_operation_ids,
                    released_holds,
                    mut candidates,
                } => {
                    let mut inner = *inner;
                    let prune =
                        self.admin
                            .prune(owner, &request, &mut inner, scratch, |_, inner| {
                                let next = GcProgress::Prune {
                                    request: request.clone(),
                                    progress: Box::new(inner.clone()),
                                    recovered: recovered.clone(),
                                    completed_operation_ids: completed_operation_ids.clone(),
                                    released_holds: released_holds.clone(),
                                    candidates: candidates.clone(),
                                };
                                persist(&next)?;
                                *progress = next;
                                Ok(())
                            })?;
                    completed_operation_ids.extend(prune.completed_operation_ids.clone());
                    let mut unique = BTreeSet::new();
                    completed_operation_ids.retain(|id| unique.insert(id.clone()));
                    for candidate in &mut candidates {
                        if prune.pruned.contains(&candidate.target) {
                            candidate.state = CandidateState::AlreadyPruned;
                            candidate.reasons = vec!["committed pruning tombstone".into()];
                        } else if prune.protected.contains(&candidate.target) {
                            candidate.state = CandidateState::Protected;
                        }
                    }
                    let observed = self.preview(&dataset, scratch);
                    let mut maintenance_error = prune.maintenance_error.clone();
                    let (waiting_runs, waiting_holds, waiting_observation_complete) = match observed
                    {
                        Ok(observed) => {
                            for candidate in &mut candidates {
                                if !prune.pruned.contains(&candidate.target)
                                    && let Some(actual) = observed
                                        .candidates
                                        .iter()
                                        .find(|actual| actual.target == candidate.target)
                                {
                                    candidate.state = actual.state.clone();
                                    candidate.reasons = actual.reasons.clone();
                                }
                            }
                            (
                                observed
                                    .runs
                                    .into_iter()
                                    .filter(|run| run.phase != RunPhase::Sealed && run.expired)
                                    .collect(),
                                observed
                                    .holds
                                    .into_iter()
                                    .filter(|hold| hold.consumer_needs_recovery)
                                    .collect(),
                                true,
                            )
                        }
                        Err(error) => {
                            maintenance_error = Some(error);
                            (vec![], vec![], false)
                        }
                    };
                    let outcome = GcOutcome {
                        dataset: dataset.clone(),
                        maintenance_error,
                        prune,
                        recovered,
                        completed_operation_ids,
                        candidates,
                        released_holds,
                        waiting_runs,
                        waiting_holds,
                        waiting_observation_complete,
                    };
                    let next = GcProgress::Complete {
                        outcome: Box::new(outcome.clone()),
                    };
                    persist(&next)?;
                    *progress = next;
                    return Ok(outcome);
                }
                GcProgress::Complete { outcome } => return Ok(*outcome),
                GcProgress::Prepared | GcProgress::Start { .. } => unreachable!(),
            }
        }
    }
}
