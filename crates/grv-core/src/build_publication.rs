//! Fixed build selection, whole-input citations and source-free publication.
//! Consumers durably replace their private journal at every progress callback.
use crate::{
    build_export::ExportReceipt,
    canonical::Sorter,
    capture::{CapturedGroup, stage_group},
    clock::Clock,
    journal::Journal,
    normalize::TablePlan,
    ownership::{
        DerivedReservation, DerivedReservationIntent, DerivedWriteProgress, Ownership,
        ReservationProgress, RunOwner,
    },
    push::{
        ExportAction, ExportGroup, ExportPlan, GroupProgress, PushFinalizer, PushOutcome,
        PushPolicy, PushProgress, same_files,
    },
    revision,
    store::{Result, Store, backend_error, public_error},
};
use grv_storage::{Backend, ObjectKey, model::*};
use grv_types::{Digest, ErrorCode, Name};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

fn invalid(message: &str) -> grv_types::PublicError {
    public_error(ErrorCode::InvalidDeclaration, message)
}
fn integrity(message: &str) -> grv_types::PublicError {
    public_error(ErrorCode::IntegrityFailure, message)
}
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Policy {
    #[default]
    Changed,
    All,
    Explicit,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Selector {
    pub table: Name,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<Partition>,
}
impl Selector {
    fn covers(&self, table: &Name, partition: &Partition) -> bool {
        &self.table == table && self.partition.as_ref().is_none_or(|p| p == partition)
    }
    fn intersects(&self, other: &Self) -> bool {
        self.table == other.table
            && (self.partition.is_none()
                || other.partition.is_none()
                || self.partition == other.partition)
    }
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Selection {
    #[serde(default)]
    pub policy: Policy,
    #[serde(default)]
    pub include: Vec<Selector>,
    #[serde(default)]
    pub drop: Vec<Selector>,
    #[serde(default)]
    pub hold: Vec<Selector>,
    #[serde(default)]
    pub empty: Vec<Selector>,
}
impl Selection {
    /// Validate selector conflicts before staging, claims or allocation. Base
    /// layouts authorize only drops and holds; outputs authorize include/empty.
    fn validate(
        &self,
        outputs: &BTreeMap<Name, TablePlan>,
        layouts: &BTreeMap<Name, TableLayout>,
    ) -> Result<()> {
        if (self.policy == Policy::Explicit) != !self.include.is_empty() {
            return Err(invalid(
                "explicit selection requires include; other policies forbid it",
            ));
        }
        for selectors in [&self.include, &self.drop, &self.hold, &self.empty] {
            for (i, selector) in selectors.iter().enumerate() {
                if selectors[..i]
                    .iter()
                    .any(|other| selector.intersects(other))
                {
                    return Err(invalid("duplicate or intersecting build selectors"));
                }
            }
        }
        for selector in self.include.iter().chain(&self.empty) {
            let output = outputs
                .get(&selector.table)
                .ok_or_else(|| invalid("build selector has no output mapping"))?;
            if let Some(partition) = &selector.partition {
                output
                    .layout()
                    .partition_path(partition)
                    .map_err(backend_error)?;
            }
        }
        for selector in self.drop.iter().chain(&self.hold) {
            let layout = layouts
                .get(&selector.table)
                .ok_or_else(|| invalid("drop or hold table is absent from the prepared base"))?;
            if let Some(partition) = &selector.partition {
                layout.partition_path(partition).map_err(backend_error)?;
            }
        }
        if self.hold.iter().any(|s| s.partition.is_some())
            || self.empty.iter().any(|s| s.partition.is_none())
        {
            return Err(invalid(
                "hold requires whole tables and empty requires exact pairs",
            ));
        }
        for (left, right) in [
            (&self.hold, &self.drop),
            (&self.hold, &self.include),
            (&self.hold, &self.empty),
            (&self.drop, &self.include),
            (&self.drop, &self.empty),
        ] {
            if left.iter().any(|a| right.iter().any(|b| a.intersects(b))) {
                return Err(invalid("build selectors conflict"));
            }
        }
        if self.policy == Policy::Explicit
            && self.empty.iter().any(|s| {
                !self
                    .include
                    .iter()
                    .any(|i| i.covers(&s.table, s.partition.as_ref().unwrap()))
            })
        {
            return Err(invalid("explicit empty pair is outside include"));
        }
        Ok(())
    }
}

fn empty_group(journal: &Journal, plan: &TablePlan, partition: Partition) -> Result<CapturedGroup> {
    let staged = Sorter::new(plan.contract.clone(), journal.directory())
        .and_then(Sorter::finish)
        .map_err(|e| integrity(&e.to_string()))?;
    crate::capture::adopt_group(journal, plan, partition, staged)
}

fn layouts<B: Backend>(
    store: &Store<B>,
    dataset: &Name,
    state: &revision::State,
) -> Result<BTreeMap<Name, TableLayout>> {
    let mut layouts = BTreeMap::new();
    for (table, _) in state.keys() {
        if !layouts.contains_key(table) {
            let key = ObjectKey::new(format!("datasets/{dataset}/{table}/.layout.json"))
                .map_err(backend_error)?;
            let bytes = store
                .backend
                .read_bytes(&key, 64 * 1024 * 1024)
                .map_err(backend_error)?
                .0;
            let layout: TableLayout = decode_record(&bytes).map_err(backend_error)?;
            if &layout.table != table {
                return Err(integrity("base layout table changed"));
            }
            layouts.insert(table.clone(), layout);
        }
    }
    Ok(layouts)
}
/// Validate membership and selector conflicts against the fixed prepared base
/// before opening its run or executing engine queries.
pub fn validate_selection<B: Backend>(
    store: &Store<B>,
    dataset: &Name,
    base: Counter,
    plans: &[TablePlan],
    selection: &Selection,
    scratch: &Path,
) -> Result<()> {
    if !revision::committed(store, dataset, base, scratch)? {
        return Err(public_error(
            ErrorCode::StateConflict,
            "build base is not committed",
        ));
    }
    let state = if base.get() == 0 {
        revision::State::new()
    } else {
        revision::read(store, dataset, base, scratch)?.state
    };
    let mut outputs = BTreeMap::new();
    for plan in plans {
        plan.validate()?;
        if outputs.insert(plan.table.clone(), plan.clone()).is_some() {
            return Err(invalid("duplicate build output mapping"));
        }
    }
    selection.validate(&outputs, &layouts(store, dataset, &state)?)
}

/// Receipt verification and complete selection precede the first allocation.
/// Missing partitions never imply a build omission.
#[allow(clippy::too_many_arguments)]
pub fn prepare_plan<B: Backend>(
    store: &Store<B>,
    clock: &dyn Clock,
    ttl: u64,
    owner: &RunOwner,
    receipt: &ExportReceipt,
    plans: &[TablePlan],
    selection: &Selection,
    journal: &Journal,
    scratch: &Path,
) -> Result<ExportPlan> {
    let ownership = Ownership::new(store, clock, ttl)?;
    ownership.require_open_transfer(owner)?;
    let (plan, _) = crate::renewal::during(
        owner.clone(),
        ownership.renewal_interval(),
        |owner| ownership.renew_run(owner),
        |watch| {
            receipt.verify(journal, &receipt.accepted)?;
            let session = &receipt.accepted.session;
            let identity = &session.identity;
            let run = owner.control();
            let inputs: BTreeSet<_> = session
                .inputs
                .iter()
                .map(|i| (&i.dataset, i.revision.get()))
                .collect();
            let held: BTreeSet<_> = run
                .inputs
                .iter()
                .map(|i| (&i.dataset, i.revision.get()))
                .collect();
            if owner.dataset() != &identity.dataset
                || run.run_id != identity.run_id
                || run.base_revision.get() != session.base_revision.get()
                || inputs != held
                || !run.holds_confirmed
            {
                return Err(integrity(
                    "accepted build differs from fixed run or held inputs",
                ));
            }
            let mut outputs = BTreeMap::new();
            for plan in plans {
                plan.validate()?;
                let binding = session
                    .outputs
                    .iter()
                    .find(|b| b.table == plan.table)
                    .ok_or_else(|| integrity("build plan has no fixed output binding"))?;
                if plan.input_contract()? != binding.contract
                    || outputs.insert(plan.table.clone(), plan.clone()).is_some()
                {
                    return Err(integrity("build output plan or mapping changed"));
                }
            }
            if outputs.len() != session.outputs.len() {
                return Err(integrity("build plan omits output mappings"));
            }
            for table in &receipt.tables {
                if grv_types::canonical_json(&outputs[&table.plan.table])
                    .map_err(|_| integrity("plan encoding failed"))?
                    != grv_types::canonical_json(&table.plan)
                        .map_err(|_| integrity("plan encoding failed"))?
                {
                    return Err(integrity("captured build normalization plan changed"));
                }
            }
            if !revision::committed(store, owner.dataset(), run.base_revision, scratch)? {
                return Err(public_error(
                    ErrorCode::StateConflict,
                    "build base is not committed",
                ));
            }
            let before = if run.base_revision.get() == 0 {
                revision::State::new()
            } else {
                revision::read(store, owner.dataset(), run.base_revision, scratch)?.state
            };
            let layouts = layouts(store, owner.dataset(), &before)?;
            selection.validate(&outputs, &layouts)?;
            let suppressed = |table: &Name| {
                selection
                    .hold
                    .iter()
                    .chain(&selection.drop)
                    .any(|s| &s.table == table && s.partition.is_none())
            };
            let eligible = |table: &Name, partition: &Partition| {
                !suppressed(table)
                    && (selection.policy != Policy::Explicit
                        || selection.include.iter().any(|s| s.covers(table, partition)))
            };
            let mut groups = BTreeMap::new();
            for table in &receipt.tables {
                for group in &table.groups {
                    watch.check()?;
                    if eligible(&table.plan.table, &group.partition) {
                        groups.insert(
                            (table.plan.table.clone(), group.partition.clone()),
                            (table.plan.clone(), group.clone()),
                        );
                    }
                }
            }
            for selector in &selection.empty {
                let partition = selector.partition.clone().unwrap();
                let pair = (selector.table.clone(), partition.clone());
                if let Some((_, group)) = groups.get(&pair) {
                    if group.row_count.get() != 0 {
                        return Err(invalid("explicit empty pair has nonempty output"));
                    }
                } else {
                    let plan = &outputs[&selector.table];
                    // A completed output is still mandatory when producing an
                    // explicit empty partition; failure never supplies emptiness.
                    if !receipt
                        .tables
                        .iter()
                        .any(|t| t.plan.table == selector.table)
                    {
                        return Err(invalid("explicit empty output was not completed"));
                    }
                    groups.insert(pair, (plan.clone(), empty_group(journal, plan, partition)?));
                }
            }
            for selector in &selection.include {
                if !groups.keys().any(|(t, p)| selector.covers(t, p)) {
                    return Err(invalid("requested build output is missing"));
                }
            }
            if groups
                .keys()
                .any(|(t, p)| selection.drop.iter().any(|s| s.covers(t, p)))
            {
                return Err(invalid("build omission overlaps output contribution"));
            }
            let mut export = vec![];
            for ((table, partition), (plan, capture)) in groups {
                watch.check()?;
                let pair = (
                    table,
                    plan.layout()
                        .partition_path(&partition)
                        .map_err(backend_error)?,
                );
                let action = if selection.policy != Policy::All {
                    if let Some(entry) = before.get(&pair) {
                        let manifest = ownership.verify_version(
                            owner.dataset(),
                            &plan.layout(),
                            &partition,
                            entry.version,
                        )?;
                        if same_files(&capture, &manifest) {
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
                export.push(ExportGroup {
                    plan,
                    capture,
                    action,
                });
            }
            Ok(ExportPlan {
                dataset: owner.dataset().clone(),
                run_id: run.run_id.clone(),
                base_revision: run.base_revision,
                capture_digest: receipt.digest()?,
                declaration_digest: identity.declaration_sha256.clone(),
                policy: if selection.policy == Policy::All {
                    PushPolicy::All
                } else {
                    PushPolicy::Changed
                },
                groups: export,
                omissions: selection
                    .drop
                    .iter()
                    .map(|s| Omission {
                        table: s.table.clone(),
                        partition: s.partition.clone(),
                    })
                    .collect(),
            })
        },
    )?;
    Ok(plan)
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum DerivedGroup {
    Pending,
    Reserving {
        intent: Box<DerivedReservationIntent>,
        progress: Box<ReservationProgress>,
    },
    Reserved {
        reservation: Box<DerivedReservation>,
        writer: Box<DerivedWriteProgress>,
    },
    Written {
        reservation: Box<DerivedReservation>,
        manifest: Box<VersionManifest>,
    },
    Finalized {
        entry: RunEntry,
    },
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Progress {
    pub plan: ExportPlan,
    pub plan_digest: Digest,
    pub owner: RunOwner,
    pub groups: Vec<DerivedGroup>,
    pub publication: Option<PushProgress>,
}
impl Progress {
    pub fn planned(owner: RunOwner, plan: ExportPlan) -> Result<Self> {
        let plan_digest = plan.digest()?;
        Ok(Self {
            groups: plan.groups.iter().map(|_| DerivedGroup::Pending).collect(),
            owner,
            plan,
            plan_digest,
            publication: None,
        })
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
pub struct Finalizer<'a, B: Backend> {
    store: &'a Store<B>,
    ownership: Ownership<'a, B>,
    publisher: PushFinalizer<'a, B>,
}
impl<'a, B: Backend> Finalizer<'a, B> {
    pub fn new(store: &'a Store<B>, clock: &'a dyn Clock, ttl: u64) -> Result<Self> {
        Ok(Self {
            store,
            ownership: Ownership::new(store, clock, ttl)?,
            publisher: PushFinalizer::new(store, clock, ttl)?,
        })
    }
    pub fn finalize(
        &self,
        progress: &mut Progress,
        journal: &Journal,
        scratch: &Path,
        mut persist: impl FnMut(&Progress) -> Result<()>,
    ) -> Result<PushOutcome> {
        // A terminal or ambiguous publication is resolved before reading the
        // private export, checking provenance or opening any engine connection.
        if progress.publication.is_some() {
            return self.publish(progress, journal, scratch, &mut persist);
        }
        if progress.plan.digest()? != progress.plan_digest
            || progress.groups.len() != progress.plan.groups.len()
            || progress.owner.dataset() != &progress.plan.dataset
            || progress.owner.control().run_id != progress.plan.run_id
            || progress.owner.control().base_revision != progress.plan.base_revision
        {
            return Err(integrity("fixed build export plan changed"));
        }
        if progress.owner.control().inputs.is_empty() {
            let mut next = progress.clone();
            next.publication = Some(PushProgress::planned(
                next.owner.clone(),
                next.plan.clone(),
            )?);
            save(progress, next, &mut persist)?;
            return self.publish(progress, journal, scratch, &mut persist);
        }
        for index in 0..progress.plan.groups.len() {
            let group = progress.plan.groups[index].clone();
            if matches!(group.action, ExportAction::Reuse { .. }) {
                continue;
            }
            if matches!(progress.groups[index], DerivedGroup::Pending) {
                self.ownership.renew_run(&mut progress.owner)?;
                let (intent, owner) = crate::renewal::during(
                    progress.owner.clone(),
                    self.ownership.renewal_interval(),
                    |owner| self.ownership.renew_run(owner),
                    |_| {
                        self.ownership.prepare_build_reservation(
                            &progress.owner,
                            group.plan.layout(),
                            group.capture.partition.clone(),
                            &group.plan.contract,
                            scratch,
                        )
                    },
                )?;
                let mut next = progress.clone();
                next.owner = owner;
                next.groups[index] = DerivedGroup::Reserving {
                    intent: Box::new(intent),
                    progress: Box::new(ReservationProgress::Prepared),
                };
                save(progress, next, &mut persist)?;
            }
            if let DerivedGroup::Reserving {
                intent,
                progress: mut phase,
            } = progress.groups[index].clone()
            {
                let owner = progress.owner.clone();
                let (reservation, owner) = crate::renewal::during(
                    owner.clone(),
                    self.ownership.renewal_interval(),
                    |owner| self.ownership.renew_run(owner),
                    |_| {
                        self.ownership.reserve_derived_authorized(
                            &owner,
                            &intent,
                            &mut phase,
                            scratch,
                            |intent, phase| {
                                let mut next = progress.clone();
                                next.groups[index] = DerivedGroup::Reserving {
                                    intent: Box::new(intent.clone()),
                                    progress: Box::new(phase.clone()),
                                };
                                save(progress, next, &mut persist)
                            },
                        )
                    },
                )?;
                let mut next = progress.clone();
                next.owner = owner;
                next.groups[index] = DerivedGroup::Reserved {
                    reservation: Box::new(reservation),
                    writer: Box::new(DerivedWriteProgress::Prepared),
                };
                save(progress, next, &mut persist)?;
            }
            if let DerivedGroup::Reserved {
                mut reservation,
                mut writer,
            } = progress.groups[index].clone()
            {
                let mut owner = progress.owner.clone();
                self.ownership.renew_run(&mut owner)?;
                let (staged, renewed) = crate::renewal::during(
                    owner.clone(),
                    self.ownership.renewal_interval(),
                    |owner| self.ownership.renew_run(owner),
                    |_| stage_group(journal, &group.plan, &group.capture),
                )?;
                owner = renewed;
                // This writer owns its sole run renewer and freezes manifest
                // identity before immutable commit; callbacks retain citations.
                let manifest = self.ownership.write_derived_group(
                    &mut owner,
                    &mut reservation,
                    &staged,
                    None,
                    scratch,
                    &mut writer,
                    |writer| {
                        let mut next = progress.clone();
                        if let DerivedGroup::Reserved {
                            writer: current, ..
                        } = &mut next.groups[index]
                        {
                            **current = writer.clone();
                        }
                        save(progress, next, &mut persist)
                    },
                )?;
                let mut next = progress.clone();
                next.owner = owner;
                next.groups[index] = DerivedGroup::Written {
                    reservation,
                    manifest: Box::new(manifest),
                };
                save(progress, next, &mut persist)?;
            }
            if let DerivedGroup::Written {
                mut reservation,
                manifest,
            } = progress.groups[index].clone()
            {
                let owner = progress.owner.clone();
                let (_, owner) = crate::renewal::during(
                    owner.clone(),
                    self.ownership.renewal_interval(),
                    |owner| self.ownership.renew_run(owner),
                    |_| {
                        let actual = self.ownership.verify_version(
                            &progress.plan.dataset,
                            &group.plan.layout(),
                            &group.capture.partition,
                            reservation.reservation().allocation().version,
                        )?;
                        if actual != *manifest
                            || !same_files(&group.capture, &actual)
                            || actual.derived_from.as_deref() != Some(reservation.sources())
                        {
                            return Err(integrity(
                                "derived version differs from fixed export or citations",
                            ));
                        }
                        self.ownership
                            .release(reservation.reservation_mut(), ClaimOutcome::Finalized)
                    },
                )?;
                let allocation = reservation.reservation().allocation();
                if allocation.state != AllocationState::Finalized {
                    return Err(integrity("derived allocation did not finalize"));
                }
                let mut next = progress.clone();
                next.owner = owner;
                next.groups[index] = DerivedGroup::Finalized {
                    entry: RunEntry {
                        table: allocation.table.clone(),
                        partition: allocation.partition.clone(),
                        version: allocation.version,
                        claim_token: allocation.claim_token.clone(),
                    },
                };
                save(progress, next, &mut persist)?;
            }
        }
        let mut next = progress.clone();
        let sealed = self.ownership.seal_renewed(&mut next.owner)?;
        let mut publication = PushProgress::planned(next.owner.clone(), next.plan.clone())?;
        for (index, group) in next.plan.groups.iter().enumerate() {
            if matches!(group.action, ExportAction::Write) {
                let DerivedGroup::Finalized { entry } = &next.groups[index] else {
                    return Err(integrity("derived work is incomplete"));
                };
                publication.groups[index] = GroupProgress::Finalized {
                    entry: entry.clone(),
                };
            }
        }
        if sealed.inputs != next.owner.control().inputs {
            return Err(integrity("sealed build inputs changed"));
        }
        publication.sealed = Some(sealed);
        next.publication = Some(publication);
        save(progress, next, &mut persist)?;
        self.publish(progress, journal, scratch, &mut persist)
    }
    fn publish(
        &self,
        progress: &mut Progress,
        journal: &Journal,
        scratch: &Path,
        persist: &mut impl FnMut(&Progress) -> Result<()>,
    ) -> Result<PushOutcome> {
        let mut publication = progress.publication.clone().unwrap();
        self.publisher
            .finalize(&mut publication, journal, scratch, |publication| {
                let mut next = progress.clone();
                next.publication = Some(publication.clone());
                save(progress, next, persist)
            })
    }
    pub fn store(&self) -> &Store<B> {
        self.store
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        build_export::{AcceptedBuild, ExportJob, tests::Fixture},
        clock::new_run_id,
        holds::Holds,
        journal::{Envelope, Evidence},
    };
    use grv_adapter_api::{InputBinding, Materialization};
    use grv_types::{U64, Uuid};
    use serde_json::{Value, json};

    fn captured(
        f: &mut Fixture,
        accepted: &AcceptedBuild,
        owner: &RunOwner,
        plans: &[TablePlan],
    ) -> ExportReceipt {
        f.journal = Journal::create(
            f.root
                .path()
                .join(format!("capture-{}", accepted.session.identity.attempt_id)),
            &[f.root.path().join("grv")],
        )
        .unwrap();
        let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
        ExportJob {
            ownership: &ownership,
            run: owner,
            journal: &f.journal,
            accepted,
            plans,
        }
        .canonicalize(&f.raw(accepted, plans))
        .unwrap()
    }
    fn next(
        f: &Fixture,
        template: &AcceptedBuild,
        dataset: &str,
        base: u64,
        inputs: Vec<InputBinding>,
    ) -> (AcceptedBuild, RunOwner) {
        let mut accepted = template.clone();
        accepted.session.identity.dataset = Name::new(dataset).unwrap();
        accepted.session.identity.run_id = new_run_id(&f.clock.now()).unwrap();
        accepted.session.identity.attempt_id = Uuid::v4();
        accepted.session.session_id = Uuid::v4();
        accepted.session.base_revision = U64::new(base).unwrap();
        accepted.session.inputs = inputs;
        accepted.completion.run_id = accepted.session.identity.run_id.clone();
        accepted.completion_sha256 = accepted.completion.digest().unwrap();
        accepted.validate().unwrap();
        let unique: BTreeSet<_> = accepted
            .session
            .inputs
            .iter()
            .map(|i| (i.dataset.clone(), i.revision.get()))
            .collect();
        let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
        let mut owner = ownership
            .prepare_run(
                accepted.session.identity.dataset.clone(),
                accepted.session.identity.run_id.clone(),
                Counter::new(base).unwrap(),
                unique
                    .into_iter()
                    .map(|(dataset, revision)| RunInput {
                        dataset,
                        revision: Counter::new(revision).unwrap(),
                        retention_id: Uuid::v4(),
                    })
                    .collect(),
                None,
            )
            .unwrap();
        let holds = Holds::new(&f.store, &f.clock, f.journal.directory());
        let records = holds.prepare(owner.dataset(), owner.control()).unwrap();
        // Fixed identities are journaled before either run or hold creation.
        let evidence: Evidence<Value, Value, Value, Value> = Evidence {
            intent: json!({"owner":owner,"holds":records}),
            capture: None,
            progress: json!({}),
            terminal: None,
        };
        let private = Journal::create(
            f.root
                .path()
                .join(format!("intent-{}", owner.control().run_id)),
            &[f.root.path().join("grv")],
        )
        .unwrap();
        private.create_evidence(evidence).unwrap();
        ownership.commit_run(&owner).unwrap();
        let proofs: Vec<_> = records.iter().map(|r| holds.acquire(r).unwrap()).collect();
        ownership
            .confirm_dependency_holds(&mut owner, f.journal.directory(), &proofs)
            .unwrap();
        (accepted, owner)
    }
    fn plan(
        f: &Fixture,
        owner: &RunOwner,
        receipt: &ExportReceipt,
        plans: &[TablePlan],
        selection: &Selection,
    ) -> Progress {
        Progress::planned(
            owner.clone(),
            prepare_plan(
                &f.store,
                &f.clock,
                60,
                owner,
                receipt,
                plans,
                selection,
                &f.journal,
                f.journal.directory(),
            )
            .unwrap(),
        )
        .unwrap()
    }
    fn finalize(f: &Fixture, progress: &mut Progress) -> PushOutcome {
        let journal = Journal::create(
            f.root
                .path()
                .join(format!("publish-{}", progress.plan.run_id)),
            &[f.root.path().join("grv")],
        )
        .unwrap();
        let mut record: Envelope<Value, Value, Progress, PushOutcome> = journal
            .create_evidence(Evidence {
                intent: json!({"plan_digest":progress.plan_digest}),
                capture: None,
                progress: progress.clone(),
                terminal: None,
            })
            .unwrap();
        Finalizer::new(&f.store, &f.clock, 60)
            .unwrap()
            .finalize(progress, &f.journal, f.journal.directory(), |p| {
                let mut e = record.evidence.clone();
                e.progress = p.clone();
                record = journal.compare_and_swap(record.generation, e)?;
                Ok(())
            })
            .unwrap()
    }
    #[test]
    fn build_selection_orders_groups_preserves_unexported_base_and_commits_omission_only_changes() {
        let mut f = Fixture::new();
        let (accepted, owner, plans) = f.accepted(&["rows", "empty"]);
        let receipt = captured(&mut f, &accepted, &owner, &plans);
        let mut progress = plan(&f, &owner, &receipt, &plans, &Selection::default());
        assert_eq!(progress.plan.groups[0].plan.table.as_str(), "empty");
        assert_eq!(finalize(&f, &mut progress).revision.get(), 1);
        let mut only_rows = accepted.clone();
        only_rows
            .session
            .outputs
            .retain(|o| o.table.as_str() == "rows");
        only_rows
            .session
            .selected_outputs
            .retain(|o| o.as_str() == "rows");
        only_rows
            .completion
            .completed_outputs
            .retain(|o| o.table.as_str() == "rows");
        let (second, owner) = next(&f, &only_rows, "data", 1, vec![]);
        let receipt = captured(&mut f, &second, &owner, &plans[..1]);
        let selection = Selection {
            hold: vec![Selector {
                table: Name::new("empty").unwrap(),
                partition: None,
            }],
            ..Default::default()
        };
        let mut progress = plan(&f, &owner, &receipt, &plans[..1], &selection);
        assert!(progress.plan.omissions.is_empty());
        assert!(matches!(
            progress.plan.groups[0].action,
            ExportAction::Reuse { .. }
        ));
        let no_op = finalize(&f, &mut progress);
        assert!(no_op.no_op);
        assert_eq!(no_op.revision.get(), 1);
        let mut omitted = second;
        omitted.session.outputs.clear();
        omitted.session.selected_outputs.clear();
        omitted.completion.completed_outputs.clear();
        omitted.completion.kind = grv_adapter_api::CompletionKind::OmissionOnly;
        let (omitted, owner) = next(&f, &omitted, "data", 1, vec![]);
        let receipt = captured(&mut f, &omitted, &owner, &[]);
        let selection = Selection {
            drop: vec![Selector {
                table: Name::new("empty").unwrap(),
                partition: None,
            }],
            ..Default::default()
        };
        let mut progress = plan(&f, &owner, &receipt, &[], &selection);
        let outcome = finalize(&f, &mut progress);
        assert!(!outcome.no_op);
        let state = revision::read(
            &f.store,
            &Name::new("data").unwrap(),
            outcome.revision,
            f.journal.directory(),
        )
        .unwrap()
        .state;
        assert_eq!(state.len(), 1);
        assert_eq!(state.keys().next().unwrap().0.as_str(), "rows");
    }
    #[test]
    fn derived_manifest_cites_all_whole_inputs_and_replays_commit_after_journal_failure() {
        let mut f = Fixture::new();
        let (template, owner, plans) = f.accepted(&["rows", "empty"]);
        let receipt = captured(&mut f, &template, &owner, &plans);
        finalize(
            &f,
            &mut plan(&f, &owner, &receipt, &plans, &Selection::default()),
        );
        let (other, other_owner) = next(&f, &template, "other", 0, vec![]);
        let receipt = captured(&mut f, &other, &other_owner, &plans);
        finalize(
            &f,
            &mut plan(&f, &other_owner, &receipt, &plans, &Selection::default()),
        );
        let inputs = [("first", "data"), ("alias", "data"), ("second", "other")]
            .into_iter()
            .map(|(alias, dataset)| InputBinding {
                alias: Name::new(alias).unwrap(),
                relation: json!({"table":alias}),
                dataset: Name::new(dataset).unwrap(),
                table: Name::new("rows").unwrap(),
                revision: U64::new(1).unwrap(),
                generation_id: Uuid::v4(),
                contract: plans[0].input_contract().unwrap(),
                materialization: Materialization::Local,
            })
            .collect();
        let (accepted, owner) = next(&f, &template, "derived", 0, inputs);
        assert_eq!(owner.control().inputs.len(), 2);
        let receipt = captured(&mut f, &accepted, &owner, &plans);
        let mut progress = plan(&f, &owner, &receipt, &plans, &Selection::default());
        let private = Journal::create(
            f.root.path().join("fault-journal"),
            &[f.root.path().join("grv")],
        )
        .unwrap();
        let mut record: Envelope<Value, Value, Progress, PushOutcome> = private
            .create_evidence(Evidence {
                intent: json!({}),
                capture: None,
                progress: progress.clone(),
                terminal: None,
            })
            .unwrap();
        let finalizer = Finalizer::new(&f.store, &f.clock, 60).unwrap();
        let failed = finalizer.finalize(&mut progress, &f.journal, f.journal.directory(), |p| {
            if p.groups
                .iter()
                .any(|g| matches!(g, DerivedGroup::Written { .. }))
            {
                return Err(public_error(
                    ErrorCode::BackendFailure,
                    "injected journal failure after version commit",
                ));
            }
            let mut e = record.evidence.clone();
            e.progress = p.clone();
            record = private.compare_and_swap(record.generation, e)?;
            Ok(())
        });
        assert_eq!(failed.unwrap_err().code, ErrorCode::BackendFailure);
        assert!(
            matches!(record.evidence.progress.groups[0],DerivedGroup::Reserved{ref writer,..}
            if matches!(**writer,DerivedWriteProgress::Commit{..}))
        );
        progress = record.evidence.progress.clone();
        let outcome = finalizer
            .finalize(&mut progress, &f.journal, f.journal.directory(), |p| {
                let mut e = record.evidence.clone();
                e.progress = p.clone();
                record = private.compare_and_swap(record.generation, e)?;
                Ok(())
            })
            .unwrap();
        assert_eq!(outcome.revision.get(), 1);
        let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
        for group in &progress.plan.groups {
            let manifest = ownership
                .verify_version(
                    &Name::new("derived").unwrap(),
                    &group.plan.layout(),
                    &group.capture.partition,
                    Counter::from(1),
                )
                .unwrap();
            let citations = manifest.derived_from.unwrap();
            assert_eq!(citations.len(), 2);
            for input in owner.control().inputs.iter() {
                assert!(citations.iter().any(|c| c.dataset == input.dataset
                    && c.revision == input.revision
                    && c.retention_id == input.retention_id
                    && c.table.is_none()
                    && c.partition.is_none()));
            }
        }
        std::fs::remove_dir_all(f.root.path().join("grv")).unwrap();
        for table in &receipt.tables {
            for group in &table.groups {
                for file in &group.files {
                    std::fs::remove_file(f.journal.directory().join(file.key.as_str())).ok();
                }
            }
        }
        assert_eq!(
            finalizer
                .finalize(
                    &mut progress,
                    &f.journal,
                    f.journal.directory(),
                    |_| panic!("terminal replay mutated journal")
                )
                .unwrap()
                .revision
                .get(),
            1
        );
    }
    #[test]
    fn explicit_empty_partition_is_materialized_and_conflicts_fail_before_allocation() {
        let f = Fixture::new();
        let (mut accepted, owner, mut plans) = f.accepted(&["events"]);
        let plan = &mut plans[0];
        plan.contract.partition_keys = vec![Name::new("region").unwrap()];
        plan.contract.columns.push(grv_types::Column {
            name: "_region_".into(),
            logical_type: json!("string"),
        });
        accepted.session.outputs[0].contract = plan.contract.clone();
        accepted.session.outputs[0].columns = json!([{"name":"id","source":"raw_id","type":"int64"},{"name":"_region_","type":"utf8"}]);
        accepted.validate().unwrap();
        let receipt = ExportReceipt {
            receipt_version: 1,
            accepted: accepted.clone(),
            stream_id: Uuid::v4(),
            canonical_writer: crate::canonical::CANONICAL_WRITER.into(),
            tables: vec![crate::build_export::ExportedTable {
                plan: plan.clone(),
                row_count: U64::new(0).unwrap(),
                groups: vec![],
            }],
            adapter_result: json!({}),
        };
        let partition: Partition =
            [(Name::new("region").unwrap(), Name::new("eu").unwrap())].into();
        let pair = Selector {
            table: plan.table.clone(),
            partition: Some(partition.clone()),
        };
        let selection = Selection {
            policy: Policy::Explicit,
            include: vec![pair.clone()],
            empty: vec![pair.clone()],
            ..Default::default()
        };
        let mut progress = super::tests::plan(&f, &owner, &receipt, &plans, &selection);
        assert_eq!(progress.plan.groups.len(), 1);
        assert_eq!(progress.plan.groups[0].capture.row_count.get(), 0);
        assert_eq!(progress.plan.groups[0].capture.partition, partition);
        assert_eq!(finalize(&f, &mut progress).revision.get(), 1);
        let mut conflict = selection.clone();
        conflict.include.push(Selector {
            table: pair.table.clone(),
            partition: None,
        });
        let (next, owner) = next(&f, &accepted, "different", 0, vec![]);
        let mut receipt = receipt;
        receipt.accepted = next;
        let result = prepare_plan(
            &f.store,
            &f.clock,
            60,
            &owner,
            &receipt,
            &plans,
            &conflict,
            &f.journal,
            f.journal.directory(),
        );
        assert_eq!(result.unwrap_err().code, ErrorCode::InvalidDeclaration);
        assert!(!f.root.path().join("grv/datasets/different/events").exists());
    }
}
