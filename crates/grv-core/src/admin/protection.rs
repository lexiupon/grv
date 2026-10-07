//! Dataset-local protection read under its actual lease. Holds are re-listed
//! separately after prune_intent; this assessment is never deletion authority.
use super::*;
use crate::{clock::parse, holds::Holds};
use chrono::Datelike;
use std::collections::BTreeSet;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Assessment {
    pub eligible: Vec<VersionTarget>,
    pub protected: Vec<VersionTarget>,
    pub already_pruned: Vec<VersionTarget>,
}
impl<'a, B: Backend> Admin<'a, B> {
    pub(super) fn folder(&self, dataset: &Name, target: &VersionTarget) -> Result<String> {
        if target.version.get() == 0 {
            return Err(invalid("version target must be positive"));
        }
        let layout: TableLayout =
            self.read(&object(dataset, &format!("{}/.layout.json", target.table)))?;
        if layout.table != target.table {
            return Err(invalid("prune layout differs from target table"));
        }
        let partition = layout
            .partition_path(&target.partition)
            .map_err(backend_error)?;
        Ok(if partition.is_empty() {
            format!("{}/version={}", target.table, target.version)
        } else {
            format!("{}/{partition}/version={}", target.table, target.version)
        })
    }
    pub(super) fn active_pins(&self, dataset: &Name, scope: &PinScope) -> Result<bool> {
        Ok(!self.observed_pins(dataset, scope)?.is_empty())
    }
    pub(crate) fn observed_pins(&self, dataset: &Name, scope: &PinScope) -> Result<Vec<PinRecord>> {
        let sentinel = Uuid::v4();
        let path = retention::pin_path(self.store, dataset, scope, &sentinel, false)?;
        let stem = path.as_str().rsplit_once('/').unwrap().0;
        let prefix = ObjectPrefix::new(format!("{stem}/")).unwrap();
        let mut found = vec![];
        let mut count = 0usize;
        for item in self
            .store
            .backend
            .list(&prefix, ListMode::Children)
            .map_err(backend_error)?
        {
            let ListEntry::Object(path) = item else {
                return Err(invalid("pin directory contains nested metadata"));
            };
            let filename = path
                .as_str()
                .strip_prefix(&format!("{stem}/"))
                .ok_or_else(|| invalid("pin listing escaped its scope"))?;
            let id = filename
                .strip_suffix(".released.json")
                .or_else(|| filename.strip_suffix(".json"))
                .ok_or_else(|| invalid("nonconforming pin filename"))?;
            let id = Uuid::new(id).map_err(|_| invalid("pin filename is not a canonical UUID"))?;
            let record_path = object(
                dataset,
                &format!(
                    "{}/{}.json",
                    stem.strip_prefix(&format!("datasets/{dataset}/")).unwrap(),
                    id
                ),
            );
            let pin: PinRecord = self.read(&record_path)?;
            if pin.pin_id != id
                || retention::pin_path(self.store, dataset, &pin.scope, &id, false)? != record_path
            {
                return Err(invalid("protection pin body differs from path"));
            }
            // A table pin and empty partition pin share their physical folder.
            let (pin, release) = self.pin_records(
                dataset,
                &PinPayload {
                    pin_id: id,
                    scope: pin.scope,
                    reason: None,
                },
            )?;
            if release.is_none()
                && let Some(pin) = pin
                && !found
                    .iter()
                    .any(|existing: &PinRecord| existing.pin_id == pin.pin_id)
            {
                found.push(pin);
            }
            count += 1;
            if count > 131_072 {
                return Err(public_error(
                    ErrorCode::UnsupportedCapability,
                    "pin scan exceeds metadata budget",
                ));
            }
        }
        Ok(found)
    }
    fn grace_expired(&self, time: &grv_types::Timestamp, parameters: &StoreParameters) -> bool {
        parameters
            .pending_grace_seconds
            .get()
            .checked_add(parameters.max_clock_skew_seconds.get())
            .and_then(|value| i64::try_from(value).ok())
            .and_then(chrono::Duration::try_seconds)
            .and_then(|duration| parse(time).checked_add_signed(duration))
            .is_some_and(|deadline| parse(&self.clock.now()) >= deadline)
    }
    pub(super) fn assess(
        &self,
        owner: &LeaseOwner,
        targets: &[VersionTarget],
        parent: &Path,
        apply: bool,
    ) -> Result<Assessment> {
        let (latest, _) = self.publisher.owned(owner)?;
        self.assess_observed(&owner.intent.dataset, &latest, targets, parent, apply)
    }
    pub(crate) fn assess_observed(
        &self,
        dataset: &Name,
        latest: &Latest,
        targets: &[VersionTarget],
        parent: &Path,
        apply: bool,
    ) -> Result<Assessment> {
        if targets.len() > 131_072 {
            return Err(public_error(
                ErrorCode::UnsupportedCapability,
                "prune candidates exceed metadata budget",
            ));
        }
        let parameters: StoreParameters = self.read(&ObjectKey::new("grv.json").unwrap())?;
        let retired = self
            .optional::<RetiredMarker>(&object(dataset, ".retired"))?
            .is_some();
        let mut pairs = vec![];
        let mut seen = BTreeSet::new();
        let mut protected = vec![false; targets.len()];
        for (index, target) in targets.iter().enumerate() {
            if !seen.insert((
                target.table.clone(),
                target.partition.clone(),
                target.version,
            )) {
                return Err(invalid("duplicate prune candidate"));
            }
            let layout: TableLayout =
                self.read(&object(dataset, &format!("{}/.layout.json", target.table)))?;
            let partition = layout
                .partition_path(&target.partition)
                .map_err(backend_error)?;
            if layout.table != target.table {
                return Err(invalid("protection layout differs from target table"));
            }
            pairs.push((target.table.clone(), partition));
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
                protected[index] |= self.active_pins(dataset, &scope)?;
            }
        }
        let held =
            Holds::new(self.store, self.clock, parent).protected_targets(dataset, targets)?;
        for (index, target) in targets.iter().enumerate() {
            protected[index] |= held.iter().any(|held| held == target);
        }
        let mut current = latest.revision;
        let mut successor: Option<(Counter, RunId)> = None;
        let mut chain = BTreeSet::new();
        let mut max_published = vec![Counter::from(0); targets.len()];
        let mut referenced = vec![false; targets.len()];
        while current.get() != 0 {
            if !chain.insert(current) || chain.len() > 131_072 {
                return Err(invalid(
                    "committed revision chain exceeds bounded protection workspace",
                ));
            }
            let revision = revision::read(self.store, dataset, current, parent)?;
            let mut kept = current == latest.revision && !retired;
            if let Some((next, operation)) = &successor {
                let path = object(
                    dataset,
                    &format!(".states/revisions/revision={current}/.superseded.json"),
                );
                let receipt = if let Some(receipt) = self.optional::<SupersessionReceipt>(&path)? {
                    Some(receipt)
                } else if apply {
                    self.publisher
                        .supersession(dataset, current, *next, operation)?;
                    Some(self.read::<SupersessionReceipt>(&path)?)
                } else {
                    None
                };
                kept |= match receipt {
                    None => true,
                    Some(receipt) => {
                        if receipt.successor != *next {
                            return Err(invalid(
                                "supersession receipt differs from committed successor",
                            ));
                        }
                        !self.grace_expired(&receipt.observed_at, &parameters)
                    }
                };
            }
            kept |= self.active_pins(
                dataset,
                &PinScope::Revision(RevisionScope { revision: current }),
            )?;
            for (index, target) in targets.iter().enumerate() {
                if let Some(entry) = revision.state.get(&pairs[index]) {
                    max_published[index] = max_published[index].max(entry.version);
                    if entry.version == target.version {
                        referenced[index] = true;
                        protected[index] |= kept;
                    }
                }
            }
            successor = Some((current, revision.operation_id));
            current = revision.previous_revision;
        }
        let mut result = Assessment::default();
        for (index, target) in targets.iter().enumerate() {
            let folder = self.folder(dataset, target)?;
            let path = object(dataset, &format!("{folder}/.pruned"));
            match self.store.backend.head(&path) {
                Ok(_) => {
                    result.already_pruned.push(target.clone());
                    continue;
                }
                Err(error) if error.kind == grv_storage::ErrorKind::NotFound => {}
                Err(error) => return Err(backend_error(error)),
            }
            let entries = self
                .store
                .backend
                .list(
                    &ObjectPrefix::new(format!("datasets/{dataset}/{folder}/")).unwrap(),
                    ListMode::Children,
                )
                .map_err(backend_error)?;
            if entries.is_empty() {
                continue;
            } // reservation numbers alone need no prune.
            let manifest = self.optional::<VersionManifest>(&object(
                dataset,
                &format!("{folder}/manifest.json"),
            ))?;
            if let Some(manifest) = manifest {
                if manifest.table != target.table
                    || manifest.partition != target.partition
                    || manifest.version != target.version
                {
                    return Err(invalid("prune manifest differs from observed candidate"));
                }
                if !referenced[index] && target.version > max_published[index] {
                    let control: RunControl = self.read(&object(
                        dataset,
                        &format!(".runs/{}.control.json", manifest.run_id),
                    ))?;
                    if control.run_id != manifest.run_id {
                        return Err(invalid("pending version run differs from its control path"));
                    }
                    if control.phase != RunPhase::Sealed {
                        protected[index] = true;
                    } else if control.entries.as_ref().unwrap().iter().any(|entry| {
                        entry.table == target.table
                            && entry.partition == target.partition
                            && entry.version == target.version
                            && entry.claim_token == manifest.claim_token
                    }) {
                        protected[index] |=
                            !self.grace_expired(control.sealed_at.as_ref().unwrap(), &parameters);
                    }
                }
            } else {
                let table_folder = folder.rsplit_once("/version=").unwrap().0;
                let claim: ClaimRecord =
                    self.read(&object(dataset, &format!("{table_folder}/.claim")))?;
                protected[index] |=
                    claim.released_at.is_none() && claim.version == Some(target.version);
            }
            if protected[index] {
                result.protected.push(target.clone());
            } else {
                result.eligible.push(target.clone());
            }
        }
        Ok(result)
    }
}

/// Public protection evidence. Its object identifies the protecting record;
/// details identify immutable facts, rather than asserting a future GC deadline.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProtectionKind {
    Current,
    Grace,
    MissingSupersessionReceipt,
    Pin,
    Hold,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtectionObject {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataset: Option<Name>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision: Option<grv_types::U64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<Name>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition: Option<Partition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<grv_types::U64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<RunId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<RunId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pin_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Protection {
    pub kind: ProtectionKind,
    pub object: ProtectionObject,
    pub active: bool,
    pub releasable: bool,
    pub until: Option<grv_types::Timestamp>,
    pub details: serde_json::Map<String, serde_json::Value>,
}
impl<'a, B: Backend> Admin<'a, B> {
    /// Read-only observation of records protecting this revision's versions.
    /// Includes other committed revisions that reuse its exact versions. It
    /// acquires no lease, creates no missing receipts, and releases no holds.
    /// Concurrent operations may change protection after this observation.
    pub fn remaining_revision_protections(
        &self,
        dataset: &Name,
        requested: Counter,
        scratch: &Path,
    ) -> Result<Vec<Protection>> {
        if requested.get() == 0 {
            return Err(public_error(
                ErrorCode::InvalidArgument,
                "revision must be positive",
            ));
        }
        let latest = revision::read_latest(self.store, dataset)?
            .ok_or_else(|| public_error(ErrorCode::NotFound, "dataset has no committed revision"))?
            .0;
        if !revision::committed(self.store, dataset, requested, scratch)? {
            return Err(public_error(
                ErrorCode::NotFound,
                "revision is not committed",
            ));
        }
        let requested_state = revision::read(self.store, dataset, requested, scratch)?.state;
        let parameters: StoreParameters = self.read(&ObjectKey::new("grv.json").unwrap())?;
        let retired = self
            .optional::<RetiredMarker>(&object(dataset, ".retired"))?
            .is_some();
        let mut result = vec![];
        let mut related = BTreeSet::new();
        let mut pin_paths = BTreeSet::new();
        let mut current = latest.revision;
        let mut successor: Option<Counter> = None;
        let mut seen = BTreeSet::new();
        while current.get() != 0 {
            if !seen.insert(current) || seen.len() > 131_072 {
                return Err(invalid("protection revision chain exceeds metadata budget"));
            }
            let revision = revision::read(self.store, dataset, current, scratch)?;
            let intersects = current == requested
                || requested_state.iter().any(|(pair, entry)| {
                    revision
                        .state
                        .get(pair)
                        .is_some_and(|other| other.version == entry.version)
                });
            if intersects {
                related.insert(current);
                let identity = ProtectionObject {
                    dataset: Some(dataset.clone()),
                    revision: Some(
                        grv_types::U64::new(current.get()).expect("validated storage counter"),
                    ),
                    ..Default::default()
                };
                if current == latest.revision && !retired {
                    result.push(Protection {
                        kind: ProtectionKind::Current,
                        object: identity.clone(),
                        active: true,
                        releasable: false,
                        until: None,
                        details: serde_json::Map::new(),
                    });
                }
                if let Some(next) = successor {
                    let path = object(
                        dataset,
                        &format!(".states/revisions/revision={current}/.superseded.json"),
                    );
                    match self.optional::<SupersessionReceipt>(&path)? {
                        Some(receipt) => {
                            if receipt.successor != next {
                                return Err(invalid(
                                    "supersession receipt differs from committed chain",
                                ));
                            }
                            if !self.grace_expired(&receipt.observed_at, &parameters) {
                                let until = parameters
                                    .pending_grace_seconds
                                    .get()
                                    .checked_add(parameters.max_clock_skew_seconds.get())
                                    .and_then(|seconds| i64::try_from(seconds).ok())
                                    .and_then(chrono::Duration::try_seconds)
                                    .and_then(|duration| {
                                        parse(&receipt.observed_at).checked_add_signed(duration)
                                    })
                                    .filter(|deadline| (1..=9999).contains(&deadline.year()))
                                    .map(crate::clock::timestamp);
                                result.push(Protection {
                                    kind: ProtectionKind::Grace,
                                    object: ProtectionObject {
                                        path: Some(path.as_str().to_owned()),
                                        ..identity.clone()
                                    },
                                    active: true,
                                    releasable: false,
                                    until,
                                    details: serde_json::Map::new(),
                                });
                            }
                        }
                        None => result.push(Protection {
                            kind: ProtectionKind::MissingSupersessionReceipt,
                            object: ProtectionObject {
                                path: Some(path.as_str().to_owned()),
                                ..identity.clone()
                            },
                            active: true,
                            releasable: false,
                            until: None,
                            details: serde_json::Map::new(),
                        }),
                    }
                }
                self.report_pins(
                    dataset,
                    &PinScope::Revision(RevisionScope { revision: current }),
                    &mut pin_paths,
                    &mut result,
                )?;
            }
            successor = Some(current);
            current = revision.previous_revision;
        }
        for ((table, path), entry) in &requested_state {
            let layout: TableLayout =
                self.read(&object(dataset, &format!("{table}/.layout.json")))?;
            if layout.table != *table {
                return Err(invalid("revision protection layout identity differs"));
            }
            let partition = layout.parse_partition(path).map_err(backend_error)?;
            for scope in [
                PinScope::Table(TableScope {
                    table: table.clone(),
                }),
                PinScope::Partition(PartitionScope {
                    table: table.clone(),
                    partition: partition.clone(),
                }),
                PinScope::Version(VersionScope {
                    table: table.clone(),
                    partition,
                    version: entry.version,
                }),
            ] {
                self.report_pins(dataset, &scope, &mut pin_paths, &mut result)?;
            }
        }
        let holds = Holds::new(self.store, self.clock, scratch);
        let prefix = ObjectPrefix::new(format!("datasets/{dataset}/.holds/")).unwrap();
        let mut budget = 0usize;
        for item in self
            .store
            .backend
            .list(&prefix, ListMode::Recursive)
            .map_err(backend_error)?
        {
            let ListEntry::Object(path) = item else {
                return Err(invalid("recursive hold listing returned a prefix"));
            };
            budget = budget.saturating_add(path.as_str().len() + 512);
            if budget > 128 * 1024 * 1024 {
                return Err(public_error(
                    ErrorCode::UnsupportedCapability,
                    "hold report exceeds metadata budget",
                ));
            }
            let hold: HoldRecord = self.read(&path)?;
            if hold.dataset != *dataset || crate::holds::hold_key(&hold) != path {
                return Err(invalid("hold body differs from source path"));
            }
            if related.contains(&hold.revision) && holds.active(&hold)? {
                let mut details = serde_json::Map::new();
                details.insert(
                    "consumer_dataset".into(),
                    serde_json::json!(hold.target_dataset),
                );
                details.insert("retention_id".into(), serde_json::json!(hold.retention_id));
                result.push(Protection {
                    kind: ProtectionKind::Hold,
                    object: ProtectionObject {
                        dataset: Some(dataset.clone()),
                        revision: Some(
                            grv_types::U64::new(hold.revision.get())
                                .expect("validated storage counter"),
                        ),
                        run_id: Some(hold.target_run_id.clone()),
                        path: Some(path.as_str().to_owned()),
                        ..Default::default()
                    },
                    active: true,
                    releasable: holds.releasable(&hold)?,
                    until: None,
                    details,
                });
            }
        }
        Ok(result)
    }
    fn report_pins(
        &self,
        dataset: &Name,
        scope: &PinScope,
        seen: &mut BTreeSet<ObjectKey>,
        result: &mut Vec<Protection>,
    ) -> Result<()> {
        for pin in self.observed_pins(dataset, scope)? {
            let path = retention::pin_path(self.store, dataset, &pin.scope, &pin.pin_id, false)?;
            if !seen.insert(path.clone()) {
                continue;
            }
            let mut identity = ProtectionObject {
                dataset: Some(dataset.clone()),
                pin_id: Some(pin.pin_id.clone()),
                operation_id: Some(pin.operation_id.clone()),
                path: Some(path.as_str().to_owned()),
                ..Default::default()
            };
            match pin.scope {
                PinScope::Revision(v) => {
                    identity.revision = Some(
                        grv_types::U64::new(v.revision.get()).expect("validated storage counter"),
                    )
                }
                PinScope::Table(v) => identity.table = Some(v.table),
                PinScope::Partition(v) => {
                    identity.table = Some(v.table);
                    identity.partition = Some(v.partition);
                }
                PinScope::Version(v) => {
                    identity.table = Some(v.table);
                    identity.partition = Some(v.partition);
                    identity.version = Some(
                        grv_types::U64::new(v.version.get()).expect("validated storage counter"),
                    );
                }
            }
            let mut details = serde_json::Map::new();
            details.insert("reason".into(), serde_json::json!(pin.reason));
            result.push(Protection {
                kind: ProtectionKind::Pin,
                object: identity,
                active: true,
                releasable: false,
                until: None,
                details,
            });
        }
        Ok(())
    }
}
