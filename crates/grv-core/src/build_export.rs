//! Accepted build exports have no source acquisition checkpoint or source
//! consistency fields. Credits acknowledge only durable, verified private IPC.
use crate::{
    canonical::CANONICAL_WRITER,
    capture::{self, CapturedGroup, RawBatch},
    contract,
    journal::Journal,
    normalize::TablePlan,
    ownership::{Ownership, RunOwner},
    store::{Result, public_error},
};
use grv_adapter_api::{BuildCompletion, BuildRecord, BuildSession, Doc, Frame, Handle, U64};
use grv_adapter_host::process::Session;
use grv_storage::{Backend, ObjectKey};
use grv_types::{Digest, ErrorCode, Name, Uuid};
use serde::{Deserialize, Serialize};
use serde_json::Value;

fn integrity(message: impl Into<String>) -> grv_types::PublicError {
    public_error(ErrorCode::IntegrityFailure, message)
}
fn incomplete(message: &str) -> grv_types::PublicError {
    public_error(ErrorCode::BuildIncomplete, message)
}
fn encoded<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let bytes = grv_types::canonical_json(value)
        .map_err(|_| integrity("build export evidence is not JCS interoperable"))?;
    if bytes.len() > 64 * 1024 * 1024 {
        return Err(incomplete("build export evidence exceeds metadata budget"));
    }
    Ok(bytes)
}
fn count(n: u64) -> Result<U64> {
    U64::new(n).map_err(|e| integrity(e.to_string()))
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedBuild {
    pub session: BuildSession,
    pub completion: BuildCompletion,
    pub completion_sha256: Digest,
}
impl AcceptedBuild {
    pub fn validate(&self) -> Result<()> {
        self.completion
            .validate_for(&self.session)
            .map_err(|e| integrity(e.to_string()))?;
        if self
            .completion
            .digest()
            .map_err(|e| integrity(e.to_string()))?
            != self.completion_sha256
        {
            return Err(integrity("accepted build completion digest changed"));
        }
        Ok(())
    }
    /// Only a validated immutable engine acceptance record authorizes reopening.
    pub fn from_record(record: &BuildRecord) -> Result<Self> {
        record.validate().map_err(|e| integrity(e.to_string()))?;
        let accepted = Self {
            session: record.session.clone(),
            completion: record
                .completion
                .clone()
                .ok_or_else(|| incomplete("build has no accepted completion"))?,
            completion_sha256: record
                .completion_sha256
                .clone()
                .ok_or_else(|| incomplete("build has no immutable accepted digest"))?,
        };
        accepted.validate()?;
        Ok(accepted)
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum CompletionProgress {
    Candidate { value: Box<AcceptedBuild> },
    Accepted { value: Box<AcceptedBuild> },
}
/// Candidate persistence precedes acceptance; accepted persistence precedes any
/// export frame. A lost acceptance acknowledgement is resolved by open_build.
pub fn accept(
    process: &mut Session,
    handle: Handle,
    session: BuildSession,
    completion: BuildCompletion,
    mut persist: impl FnMut(&CompletionProgress) -> Result<()>,
) -> Result<AcceptedBuild> {
    let digest = completion.digest().map_err(|e| integrity(e.to_string()))?;
    let accepted = AcceptedBuild {
        session,
        completion,
        completion_sha256: digest,
    };
    accepted.validate()?;
    persist(&CompletionProgress::Candidate {
        value: Box::new(accepted.clone()),
    })?;
    let returned = process
        .accept_build_completion(
            handle,
            accepted.session.session_id.clone(),
            accepted.completion.clone(),
        )
        .map_err(|e| e.public())?;
    if returned != accepted.completion_sha256 {
        return Err(integrity("engine accepted a different completion"));
    }
    persist(&CompletionProgress::Accepted {
        value: Box::new(accepted.clone()),
    })?;
    Ok(accepted)
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportTableProgress {
    pub table: Name,
    pub batches: Vec<RawBatch>,
    pub row_count: U64,
    pub completion: Option<U64>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportProgress {
    pub stream_id: Uuid,
    pub session_id: Uuid,
    pub completion_sha256: Digest,
    pub started: bool,
    pub tables: Vec<ExportTableProgress>,
    pub adapter_result: Option<Value>,
    pub stopped: bool,
}
impl ExportProgress {
    pub fn prepared(accepted: &AcceptedBuild, stream_id: Uuid) -> Self {
        Self {
            stream_id,
            session_id: accepted.session.session_id.clone(),
            completion_sha256: accepted.completion_sha256.clone(),
            started: false,
            tables: accepted
                .session
                .selected_outputs
                .iter()
                .map(|table| ExportTableProgress {
                    table: table.clone(),
                    batches: vec![],
                    row_count: U64::new(0).unwrap(),
                    completion: None,
                })
                .collect(),
            adapter_result: None,
            stopped: false,
        }
    }
    pub fn validate(&self, accepted: &AcceptedBuild) -> Result<()> {
        accepted.validate()?;
        if self.session_id != accepted.session.session_id
            || self.completion_sha256 != accepted.completion_sha256
            || self.tables.len() != accepted.session.selected_outputs.len()
            || self
                .tables
                .iter()
                .zip(&accepted.session.selected_outputs)
                .any(|(table, name)| &table.table != name)
            || self.stopped && self.adapter_result.is_none()
        {
            return Err(integrity(
                "build export changed accepted identity or selected scope",
            ));
        }
        for table in &self.tables {
            if table.completion.is_some_and(|n| n != table.row_count) {
                return Err(integrity("build export counts differ from completion"));
            }
            let rows = table
                .batches
                .iter()
                .try_fold(0u64, |rows, batch| rows.checked_add(batch.rows.get()))
                .ok_or_else(|| integrity("build export count overflow"))?;
            if rows != table.row_count.get() {
                return Err(integrity("build export batch count differs"));
            }
        }
        if self.adapter_result.is_some() && self.tables.iter().any(|t| t.completion.is_none()) {
            return Err(integrity(
                "build export ended without explicit table coverage",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportedTable {
    pub plan: TablePlan,
    pub row_count: U64,
    pub groups: Vec<CapturedGroup>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportReceipt {
    pub receipt_version: u8,
    pub accepted: AcceptedBuild,
    pub stream_id: Uuid,
    pub canonical_writer: String,
    pub tables: Vec<ExportedTable>,
    pub adapter_result: Value,
}
impl ExportReceipt {
    pub fn digest(&self) -> Result<Digest> {
        Ok(grv_types::sha256(&encoded(self)?))
    }
    pub fn verify(&self, journal: &Journal, accepted: &AcceptedBuild) -> Result<()> {
        accepted.validate()?;
        if self.receipt_version != 1
            || self.canonical_writer != CANONICAL_WRITER
            || encoded(&self.accepted)? != encoded(accepted)?
            || self.tables.len() != accepted.session.selected_outputs.len()
            || !self.adapter_result.is_object()
        {
            return Err(integrity("accepted build capture changed fixed identity"));
        }
        for (table, name) in self.tables.iter().zip(&accepted.session.selected_outputs) {
            table.plan.validate()?;
            let binding = accepted
                .session
                .outputs
                .iter()
                .find(|o| &o.table == name)
                .ok_or_else(|| integrity("exported output has no binding"))?;
            if &table.plan.table != name || table.plan.input_contract()? != binding.contract {
                return Err(integrity("accepted build output contract changed"));
            }
            let mut rows = 0u64;
            let mut partitions = std::collections::BTreeSet::new();
            for group in &table.groups {
                if !partitions.insert(&group.partition) {
                    return Err(integrity("duplicate accepted build partition"));
                }
                rows = rows
                    .checked_add(capture::stage_group(journal, &table.plan, group)?.row_count)
                    .ok_or_else(|| integrity("accepted build row count overflow"))?;
            }
            if rows != table.row_count.get()
                || table.plan.contract.partition_keys.is_empty() && table.groups.len() != 1
            {
                return Err(integrity(
                    "accepted build rows or empty-table coverage differ",
                ));
            }
        }
        Ok(())
    }
}

pub struct ExportJob<'a, 's, B: Backend> {
    pub ownership: &'a Ownership<'s, B>,
    pub run: &'a RunOwner,
    pub journal: &'a Journal,
    pub accepted: &'a AcceptedBuild,
    pub plans: &'a [TablePlan],
}
impl<B: Backend> ExportJob<'_, '_, B> {
    fn validate(&self, progress: &ExportProgress) -> Result<()> {
        progress.validate(self.accepted)?;
        if self.run.dataset() != &self.accepted.session.identity.dataset
            || self.run.control().run_id != self.accepted.session.identity.run_id
            || self.run.control().base_revision.get() != self.accepted.session.base_revision.get()
            || self.plans.len() != self.accepted.session.selected_outputs.len()
        {
            return Err(integrity("build export changed fixed run"));
        }
        for (plan, table) in self.plans.iter().zip(&progress.tables) {
            plan.validate()?;
            let binding = self
                .accepted
                .session
                .outputs
                .iter()
                .find(|o| o.table == table.table)
                .ok_or_else(|| integrity("build output binding missing"))?;
            if plan.table != table.table || plan.input_contract()? != binding.contract {
                return Err(integrity("build export plan differs from fixed contract"));
            }
        }
        Ok(())
    }
    /// A fresh credited stream of an already accepted immutable completion.
    /// Partial streams never mix artifacts or resume with an old channel ID.
    pub fn acquire(
        &self,
        process: &mut Session,
        handle: Handle,
        progress: &mut ExportProgress,
        mut persist: impl FnMut(&ExportProgress) -> Result<()>,
    ) -> Result<()> {
        self.validate(progress)?;
        if progress.started {
            return Err(incomplete("recorded build stream cannot restart"));
        }
        if encoded(progress)?
            != encoded(&ExportProgress::prepared(
                self.accepted,
                progress.stream_id.clone(),
            ))?
        {
            return Err(integrity("unstarted build stream contains export evidence"));
        }
        self.ownership.require_open_transfer(self.run)?;
        let shutdown = process
            .channel
            .codec
            .io_mut()
            .shutdown_handle()
            .map_err(|e| public_error(ErrorCode::BackendFailure, e.to_string()))?;
        let (_, _) = crate::renewal::during(
            self.run.clone(),
            self.ownership.renewal_interval(),
            |owner| {
                let renewed = self.ownership.renew_run(owner);
                if renewed.is_err() {
                    let _ = shutdown.shutdown(std::net::Shutdown::Both);
                }
                renewed
            },
            |watch| {
                let mut next = progress.clone();
                next.started = true;
                encoded(&next)?;
                persist(&next)?;
                *progress = next;
                let req = process
                    .export_build(
                        handle,
                        self.accepted.session.session_id.clone(),
                        self.accepted.completion_sha256.clone(),
                        progress.stream_id.clone(),
                    )
                    .map_err(|e| e.public())?;
                loop {
                    watch.check()?;
                    let packet = process.receive().map_err(|e| e.public())?;
                    match packet.frame {
                        Frame::Batch {
                            table,
                            seq,
                            slot,
                            rows,
                            ..
                        } => {
                            let plan = self
                                .plans
                                .iter()
                                .find(|p| p.table == table)
                                .ok_or_else(|| integrity("unknown build export table"))?;
                            let batch = grv_adapter_wire::ipc::decode(&packet.payload, rows.get())
                                .map_err(|e| integrity(e.to_string()))?;
                            contract::validate_batch(&plan.input_contract()?, &batch)
                                .map_err(|e| integrity(e.to_string()))?;
                            let mut next = progress.clone();
                            let state = next.tables.iter_mut().find(|t| t.table == table).unwrap();
                            if state.completion.is_some() || seq.get() != state.batches.len() as u64
                            {
                                return Err(integrity(
                                    "build export sequence or completion changed",
                                ));
                            }
                            let key = ObjectKey::new(format!(
                                "build/{}/{table}/batch={}.arrow",
                                progress.stream_id,
                                seq.get()
                            ))
                            .map_err(|_| integrity("invalid build artifact key"))?;
                            capture::immutable(self.journal, &key, &packet.payload)?;
                            state.batches.push(RawBatch {
                                key,
                                seq,
                                rows,
                                size: count(packet.payload.len() as u64)?,
                                sha256: grv_types::sha256(&packet.payload),
                            });
                            state.row_count = count(
                                state
                                    .row_count
                                    .get()
                                    .checked_add(rows.get())
                                    .ok_or_else(|| integrity("build export count overflow"))?,
                            )?;
                            encoded(&next)?;
                            persist(&next)?;
                            *progress = next;
                            watch.check()?;
                            process
                                .send(
                                    &Frame::BatchAck {
                                        req,
                                        table,
                                        slot,
                                        seq,
                                    },
                                    &[],
                                )
                                .map_err(|e| e.public())?;
                        }
                        Frame::BuildTableComplete {
                            table, row_count, ..
                        } => {
                            let mut next = progress.clone();
                            let state = next
                                .tables
                                .iter_mut()
                                .find(|t| t.table == table)
                                .ok_or_else(|| integrity("unknown build table completion"))?;
                            if state.completion.is_some() || state.row_count != row_count {
                                return Err(integrity("build completion count differs"));
                            }
                            state.completion = Some(row_count);
                            encoded(&next)?;
                            persist(&next)?;
                            *progress = next;
                        }
                        Frame::ExportComplete {
                            session_id,
                            completion_sha256,
                            adapter_result: Doc::Inline(result),
                            ..
                        } => {
                            if session_id != progress.session_id
                                || completion_sha256 != progress.completion_sha256
                                || progress.tables.iter().any(|t| t.completion.is_none())
                            {
                                return Err(integrity("build export terminal scope differs"));
                            }
                            let mut next = progress.clone();
                            next.adapter_result = Some(result.inline);
                            next.validate(self.accepted)?;
                            encoded(&next)?;
                            persist(&next)?;
                            *progress = next;
                            process.close().map_err(|e| e.public())?;
                            let mut next = progress.clone();
                            next.stopped = true;
                            encoded(&next)?;
                            persist(&next)?;
                            *progress = next;
                            return watch.check();
                        }
                        _ => {
                            return Err(public_error(
                                ErrorCode::ProtocolFailure,
                                "unexpected build export lifecycle frame",
                            ));
                        }
                    }
                }
            },
        )?;
        Ok(())
    }
    /// Uses only complete stopped private evidence. No adapter is opened here.
    pub fn canonicalize(&self, progress: &ExportProgress) -> Result<ExportReceipt> {
        self.validate(progress)?;
        if !progress.started
            || !progress.stopped
            || progress.adapter_result.is_none()
            || progress.tables.iter().any(|t| t.completion.is_none())
        {
            return Err(incomplete("complete stopped build export required"));
        }
        self.ownership.require_open_transfer(self.run)?;
        let (receipt, _) = crate::renewal::during(
            self.run.clone(),
            self.ownership.renewal_interval(),
            |owner| self.ownership.renew_run(owner),
            |watch| {
                let mut tables = Vec::new();
                for (plan, state) in self.plans.iter().zip(&progress.tables) {
                    let groups = capture::canonicalize_raw_table(
                        self.journal,
                        plan,
                        &state.batches,
                        state.row_count,
                        &format!("build/{}/{}", progress.stream_id, plan.table),
                        || watch.check(),
                    )?;
                    tables.push(ExportedTable {
                        plan: plan.clone(),
                        row_count: state.row_count,
                        groups,
                    });
                }
                let receipt = ExportReceipt {
                    receipt_version: 1,
                    accepted: self.accepted.clone(),
                    stream_id: progress.stream_id.clone(),
                    canonical_writer: CANONICAL_WRITER.into(),
                    tables,
                    adapter_result: progress.adapter_result.clone().unwrap(),
                };
                capture::immutable(
                    self.journal,
                    &ObjectKey::new("build/receipt.json").unwrap(),
                    &encoded(&receipt)?,
                )?;
                receipt.verify(self.journal, self.accepted)?;
                watch.check()?;
                Ok(receipt)
            },
        )?;
        Ok(receipt)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{
        clock::{Clock, SystemClock},
        store::{InitOptions, Store},
    };
    use grv_adapter_api::{
        AdapterIdentity, BuildExecution, BuildIdentity, CompletedOutput, CompletionKind,
        CompletionStatus, OutputBinding,
    };
    use grv_storage::{LocalBackend, model::Counter};
    use grv_types::{Req, RunId};
    use serde_json::json;
    use std::{io::Cursor, os::unix::fs::PermissionsExt, path::Path, sync::Arc};

    pub(crate) struct Fixture {
        pub(crate) root: tempfile::TempDir,
        pub(crate) store: Store<LocalBackend>,
        pub(crate) clock: SystemClock,
        pub(crate) journal: Journal,
    }
    impl Fixture {
        pub(crate) fn new() -> Self {
            let root = tempfile::tempdir_in(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .parent()
                    .unwrap()
                    .parent()
                    .unwrap(),
            )
            .unwrap();
            std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
            let grv = root.path().join("grv");
            std::fs::create_dir(&grv).unwrap();
            let store =
                Store::initialize(LocalBackend::open(&grv).unwrap(), InitOptions::default())
                    .unwrap()
                    .0;
            let journal = Journal::create(root.path().join("state"), &[grv]).unwrap();
            Self {
                root,
                store,
                clock: SystemClock::default(),
                journal,
            }
        }
        pub(crate) fn accepted(&self, names: &[&str]) -> (AcceptedBuild, RunOwner, Vec<TablePlan>) {
            let plans:Vec<_>=names.iter().map(|name| TablePlan::from_declaration(&json!({}),
                &json!({"name":name,"columns":[{"name":"id","source":"raw_id","type":"int64"}]})).unwrap()).collect();
            let identity = BuildIdentity {
                attempt_id: Uuid::v4(),
                root: self.root.path().to_str().unwrap().into(),
                dataset: Name::new("data").unwrap(),
                run_id: RunId::new("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
                workspace_id: Uuid::v4(),
                declaration_sha256: grv_types::sha256(b"fixed"),
                adapter_identity: AdapterIdentity {
                    name: Name::new("fixture").unwrap(),
                    package_version: "1".into(),
                    interface_version: Req::new(1).unwrap(),
                    binding_schema_version: Req::new(1).unwrap(),
                },
                connection_identity: "/engine".into(),
            };
            let outputs:Vec<OutputBinding>=plans.iter().map(|plan|serde_json::from_value(json!({"table":plan.table,
                "source":{"sql":"SELECT raw_id FROM grv_input.one"},"columns":[{"name":"id","source":"raw_id","type":"int64"}],
                "engine_table":format!("private.{}",plan.table),"contract":plan.input_contract().unwrap()})).unwrap()).collect();
            let session = BuildSession {
                session_id: Uuid::v4(),
                identity: identity.clone(),
                execution: BuildExecution::Managed,
                options: json!({}),
                base_revision: U64::new(0).unwrap(),
                base_contracts: vec![],
                inputs: vec![],
                outputs,
                selected_outputs: plans.iter().map(|p| p.table.clone()).collect(),
                self_input: false,
                adapter_details: json!({}),
            };
            let completion = BuildCompletion {
                result_version: Req::new(1).unwrap(),
                run_id: identity.run_id.clone(),
                workspace_id: identity.workspace_id,
                declaration_sha256: identity.declaration_sha256,
                kind: if names.is_empty() {
                    CompletionKind::OmissionOnly
                } else {
                    CompletionKind::Engine
                },
                invocation_id: "closed-invocation".into(),
                status: CompletionStatus::Succeeded,
                writers_stopped: true,
                completed_at: self.clock.now(),
                completed_outputs: session
                    .outputs
                    .iter()
                    .map(|o| CompletedOutput {
                        table: o.table.clone(),
                        engine_table: o.engine_table.clone(),
                    })
                    .collect(),
            };
            let accepted = AcceptedBuild {
                completion_sha256: completion.digest().unwrap(),
                session,
                completion,
            };
            accepted.validate().unwrap();
            let ownership = Ownership::new(&self.store, &self.clock, 60).unwrap();
            let mut owner = ownership
                .prepare_run(
                    identity.dataset,
                    identity.run_id,
                    Counter::from(0),
                    vec![],
                    None,
                )
                .unwrap();
            ownership.commit_run(&owner).unwrap();
            ownership.confirm_holds(&mut owner).unwrap();
            (accepted, owner, plans)
        }
        pub(crate) fn raw(&self, accepted: &AcceptedBuild, plans: &[TablePlan]) -> ExportProgress {
            let mut progress = ExportProgress::prepared(accepted, Uuid::v4());
            progress.started = true;
            for (i, (state, plan)) in progress.tables.iter_mut().zip(plans).enumerate() {
                if i == 0 {
                    let batch = arrow_array::RecordBatch::try_new(
                        contract::arrow_schema(&plan.input_contract().unwrap()).unwrap(),
                        vec![Arc::new(arrow_array::Int64Array::from(vec![3, 1, 1, 2]))],
                    )
                    .unwrap();
                    let mut bytes = vec![];
                    {
                        let mut writer =
                            arrow_ipc::writer::StreamWriter::try_new(&mut bytes, &batch.schema())
                                .unwrap();
                        writer.write(&batch).unwrap();
                        writer.finish().unwrap();
                    }
                    bytes.truncate(bytes.len() - 8);
                    let key = ObjectKey::new(format!(
                        "build/{}/{}/batch=0.arrow",
                        progress.stream_id, state.table
                    ))
                    .unwrap();
                    self.journal
                        .create_artifact(&key, &mut Cursor::new(&bytes))
                        .unwrap();
                    state.batches.push(RawBatch {
                        key,
                        seq: grv_adapter_api::SafeInt::new(0).unwrap(),
                        rows: count(4).unwrap(),
                        size: count(bytes.len() as u64).unwrap(),
                        sha256: grv_types::sha256(&bytes),
                    });
                    state.row_count = count(4).unwrap();
                }
                state.completion = Some(state.row_count);
            }
            progress.adapter_result = Some(json!({}));
            progress.stopped = true;
            progress
        }
    }
    #[test]
    fn accepted_export_canonicalizes_without_adapter_preserving_duplicates_and_explicit_empty_table()
     {
        let f = Fixture::new();
        let (accepted, owner, plans) = f.accepted(&["rows", "empty"]);
        let progress = f.raw(&accepted, &plans);
        let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
        let job = ExportJob {
            ownership: &ownership,
            run: &owner,
            journal: &f.journal,
            accepted: &accepted,
            plans: &plans,
        };
        let receipt = job.canonicalize(&progress).unwrap();
        receipt.verify(&f.journal, &accepted).unwrap();
        assert_eq!(receipt.tables[0].row_count.get(), 4);
        assert_eq!(receipt.tables[1].row_count.get(), 0);
        assert_eq!(receipt.tables[1].groups.len(), 1);
        assert_eq!(receipt.tables[1].groups[0].files.len(), 1);
        assert_eq!(
            receipt.digest().unwrap(),
            job.canonicalize(&progress).unwrap().digest().unwrap()
        );
        let staged =
            capture::stage_group(&f.journal, &plans[0], &receipt.tables[0].groups[0]).unwrap();
        let batches = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(&staged.files[0].path).unwrap(),
        )
        .unwrap()
        .build()
        .unwrap();
        let mut values = vec![];
        for batch in batches {
            let batch = batch.unwrap();
            let ints = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap();
            values.extend(ints.values().iter().copied());
        }
        assert_eq!(values, vec![1, 1, 2, 3]);
    }
    #[test]
    fn partial_unstopped_wrong_digest_counts_and_corrupt_build_artifacts_cannot_be_accepted() {
        let f = Fixture::new();
        let (accepted, owner, plans) = f.accepted(&["rows", "empty"]);
        let progress = f.raw(&accepted, &plans);
        let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
        let job = ExportJob {
            ownership: &ownership,
            run: &owner,
            journal: &f.journal,
            accepted: &accepted,
            plans: &plans,
        };
        let mut partial = progress.clone();
        partial.tables[1].completion = None;
        assert!(job.canonicalize(&partial).is_err());
        let mut unstopped = progress.clone();
        unstopped.stopped = false;
        assert_eq!(
            job.canonicalize(&unstopped).err().unwrap().code,
            ErrorCode::BuildIncomplete
        );
        let mut wrong = progress.clone();
        wrong.completion_sha256 = grv_types::sha256(b"other");
        assert!(job.canonicalize(&wrong).is_err());
        let mut wrong = progress.clone();
        wrong.tables[0].completion = Some(count(5).unwrap());
        assert!(job.canonicalize(&wrong).is_err());
        let mut wrong = progress.clone();
        wrong.tables[0].batches[0].sha256 = grv_types::sha256(b"changed");
        assert!(job.canonicalize(&wrong).is_err());
        let mut wrong = progress.clone();
        wrong.tables[0].batches[0].key =
            ObjectKey::new("build/different/table/batch=0.arrow").unwrap();
        assert!(job.canonicalize(&wrong).is_err());
    }
    #[test]
    fn omission_only_capture_has_no_fake_source_checkpoint_or_empty_table() {
        let f = Fixture::new();
        let (accepted, owner, plans) = f.accepted(&[]);
        let progress = f.raw(&accepted, &plans);
        let ownership = Ownership::new(&f.store, &f.clock, 60).unwrap();
        let job = ExportJob {
            ownership: &ownership,
            run: &owner,
            journal: &f.journal,
            accepted: &accepted,
            plans: &plans,
        };
        let receipt = job.canonicalize(&progress).unwrap();
        assert!(receipt.tables.is_empty());
        let value = serde_json::to_value(&receipt).unwrap();
        assert!(value.get("checkpoints").is_none());
        assert!(value.get("source_consistency").is_none());
        assert!(
            AcceptedBuild::from_record(&BuildRecord {
                session: accepted.session.clone(),
                state: grv_adapter_api::BuildState::Prepared,
                completion: None,
                completion_sha256: None,
                candidate: None,
                row_counts: vec![],
                outcome: None
            })
            .is_err()
        );
    }
}
