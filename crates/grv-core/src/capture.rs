//! Durable, source-independent extraction evidence. Checkpoints and credited
//! batches are acknowledged only after their consumer journal evidence is durable.
use crate::{
    canonical::{CANONICAL_WRITER, Sorter, StagedFile, StagedGroup},
    contract,
    journal::Journal,
    normalize::TablePlan,
    ownership::{Ownership, RunOwner},
    store::{Result, public_error},
};
use arrow_array::RecordBatch;
use grv_adapter_api::{
    Checkpoint, Doc, ExtractRequest, Frame, Handle, SafeInt, SourceCompletion, U64,
};
use grv_adapter_host::process::Session;
use grv_storage::{Backend, ObjectKey, model::Partition};
use grv_types::{AdapterIdentity, Digest, ErrorCode, Name, RunId, SourceConsistency, Uuid};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, OpenOptions},
    io::{Cursor, Read, Write},
    os::unix::fs::OpenOptionsExt,
    sync::{Arc, Mutex, mpsc},
    time::Duration,
};
const BATCH_LIMIT: usize = 8 * 1024 * 1024;
const EVIDENCE_LIMIT: usize = 64 * 1024 * 1024;
struct StopWorker(mpsc::Sender<()>);
impl Drop for StopWorker {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}
fn integrity(message: &str) -> grv_types::PublicError {
    public_error(ErrorCode::IntegrityFailure, message)
}
fn incomplete(message: &str) -> grv_types::PublicError {
    public_error(ErrorCode::ExtractionIncomplete, message)
}
fn local(e: std::io::Error) -> grv_types::PublicError {
    public_error(ErrorCode::BackendFailure, e.to_string())
}
fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let bytes = grv_types::canonical_json(value)
        .map_err(|_| integrity("capture evidence is not canonicalizable"))?;
    if bytes.len() > EVIDENCE_LIMIT {
        return Err(incomplete(
            "capture evidence exceeds supported metadata budget",
        ));
    }
    Ok(bytes)
}
fn count(n: u64) -> Result<U64> {
    U64::new(n).map_err(|e| integrity(&e.to_string()))
}
fn required_option<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> std::result::Result<Option<T>, D::Error> {
    Option::deserialize(d)
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawBatch {
    pub key: ObjectKey,
    pub seq: SafeInt,
    pub rows: U64,
    pub size: U64,
    pub sha256: Digest,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableCompletion {
    pub row_count: U64,
    pub source_identity: Value,
    pub capture: grv_adapter_api::CaptureWindow,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableAcquisition {
    pub table: Name,
    pub batches: Vec<RawBatch>,
    pub row_count: U64,
    #[serde(deserialize_with = "required_option")]
    pub completion: Option<TableCompletion>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcquisitionProgress {
    pub attempt_id: Uuid,
    pub started: bool,
    pub checkpoints: Vec<Checkpoint>,
    pub tables: Vec<TableAcquisition>,
    #[serde(deserialize_with = "required_option")]
    pub completion: Option<SourceCompletion>,
    pub stopped: bool,
}
impl AcquisitionProgress {
    pub fn prepared(request: &ExtractRequest) -> Self {
        Self {
            attempt_id: request.attempt_id.clone(),
            started: false,
            checkpoints: vec![],
            tables: request
                .tables
                .iter()
                .map(|t| TableAcquisition {
                    table: t.name.clone(),
                    batches: vec![],
                    row_count: count(0).unwrap(),
                    completion: None,
                })
                .collect(),
            completion: None,
            stopped: false,
        }
    }
    pub fn require_unstarted(&self, request: &ExtractRequest) -> Result<()> {
        if self.started {
            return Err(incomplete(
                "recorded acquisition cannot be restarted under this attempt",
            ));
        }
        if encode(self)? != encode(&Self::prepared(request))? {
            return Err(integrity("unstarted acquisition contains source evidence"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureFile {
    pub name: String,
    pub key: ObjectKey,
    pub size: U64,
    pub sha256: Digest,
    pub rows: U64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapturedGroup {
    pub partition: Partition,
    pub row_count: U64,
    pub files: Vec<CaptureFile>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapturedTable {
    pub plan: TablePlan,
    pub row_count: U64,
    pub completion: TableCompletion,
    pub groups: Vec<CapturedGroup>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureReceipt {
    pub receipt_version: u8,
    pub attempt_id: Uuid,
    pub run_id: RunId,
    pub declaration_sha256: Digest,
    pub adapter_identity: AdapterIdentity,
    pub connection_identity: String,
    pub source_consistency: SourceConsistency,
    pub checkpoints: Vec<Checkpoint>,
    pub completion: SourceCompletion,
    pub canonical_writer: String,
    pub tables: Vec<CapturedTable>,
}
impl CaptureReceipt {
    pub fn digest(&self) -> Result<Digest> {
        Ok(grv_types::sha256(&encode(self)?))
    }
    pub fn verify(&self, journal: &Journal, request: &ExtractRequest) -> Result<()> {
        if self.receipt_version != 1
            || self.canonical_writer != CANONICAL_WRITER
            || self.attempt_id != request.attempt_id
            || self.run_id != request.run_id
            || self.declaration_sha256 != request.declaration_sha256
            || self.adapter_identity != request.adapter_identity
            || self.connection_identity != request.connection_identity
            || self.tables.len() != request.tables.len()
        {
            return Err(integrity("accepted capture differs from fixed request"));
        }
        let mut checkpoints = BTreeMap::new();
        for checkpoint in &self.checkpoints {
            checkpoint
                .validate()
                .map_err(|_| integrity("invalid accepted acquisition checkpoint"))?;
            if checkpoint.attempt_id != request.attempt_id
                || checkpoint.adapter_identity != request.adapter_identity
                || checkpoint.connection_identity != request.connection_identity
            {
                return Err(integrity(
                    "accepted checkpoint identity differs from request",
                ));
            }
            for table in &checkpoint.tables {
                if !request
                    .tables
                    .iter()
                    .any(|declared| declared.name == table.table)
                    || checkpoints
                        .insert(&table.table, table)
                        .is_some_and(|prior| prior != table)
                {
                    return Err(integrity(
                        "accepted checkpoint table coverage or acquisition changed",
                    ));
                }
            }
        }
        if checkpoints.len() != request.tables.len() {
            return Err(integrity(
                "accepted capture is missing mandatory acquisition evidence",
            ));
        }
        let start = crate::clock::parse(&self.completion.capture_window.start);
        let end = crate::clock::parse(&self.completion.capture_window.end);
        if start > end {
            return Err(integrity("accepted source capture window is reversed"));
        }
        let mut names = BTreeSet::new();
        for table in &self.tables {
            table.plan.validate()?;
            if !names.insert(&table.plan.table) {
                return Err(integrity("duplicate captured table"));
            }
            let declared = request
                .tables
                .iter()
                .find(|t| t.name == table.plan.table)
                .ok_or_else(|| integrity("captured table not declared"))?;
            if table.plan.input_contract()? != declared.contract {
                return Err(integrity("capture contract differs from request"));
            }
            let checkpoint = checkpoints
                .get(&table.plan.table)
                .ok_or_else(|| integrity("captured table lacks checkpoint"))?;
            let table_start = crate::clock::parse(&table.completion.capture.start);
            let table_end = crate::clock::parse(&table.completion.capture.end);
            if table.completion.source_identity != checkpoint.source_identity
                || table_start != crate::clock::parse(&checkpoint.capture_start)
                || table_start > table_end
                || table_start < start
                || table_end > end
            {
                return Err(integrity(
                    "accepted table completion differs from acquisition evidence",
                ));
            }
            let mut rows = 0u64;
            let mut partitions = BTreeSet::new();
            for group in &table.groups {
                if !partitions.insert(&group.partition) {
                    return Err(integrity("duplicate captured partition"));
                }
                let staged = stage_group(journal, &table.plan, group)?;
                rows = rows
                    .checked_add(staged.row_count)
                    .ok_or_else(|| integrity("capture row count overflow"))?;
            }
            if rows != table.row_count.get()
                || rows != table.completion.row_count.get()
                || (table.plan.contract.partition_keys.is_empty() && table.groups.len() != 1)
            {
                return Err(integrity(
                    "capture completion or empty-table coverage differs",
                ));
            }
        }
        Ok(())
    }
}
pub struct CaptureJob<'a, 's, B: Backend> {
    pub ownership: &'a Ownership<'s, B>,
    pub run: &'a RunOwner,
    pub journal: &'a Journal,
    pub request: &'a ExtractRequest,
    pub plans: &'a [TablePlan],
    pub renewal_interval: Duration,
}
impl<B: Backend> CaptureJob<'_, '_, B> {
    fn validate(&self, progress: &AcquisitionProgress) -> Result<()> {
        if self.request.resume.is_some() {
            return Err(public_error(
                ErrorCode::UnsupportedCapability,
                "resumable source acquisition is not implemented",
            ));
        }
        if self.request.run_id != self.run.control().run_id
            || self.request.dataset != *self.run.dataset()
            || progress.attempt_id != self.request.attempt_id
            || self.request.tables.len() != self.plans.len()
            || self.request.tables.len() != progress.tables.len()
        {
            return Err(integrity("capture identity differs from prepared run"));
        }
        let mut names = BTreeSet::new();
        for plan in self.plans {
            plan.validate()?;
            if !names.insert(&plan.table) {
                return Err(integrity("duplicate capture plan"));
            }
            let table = self
                .request
                .tables
                .iter()
                .find(|t| t.name == plan.table)
                .ok_or_else(|| integrity("capture plan table missing"))?;
            if table.contract != plan.input_contract()? {
                return Err(integrity(
                    "adapter input contract differs from normalized plan",
                ));
            }
            if progress
                .tables
                .iter()
                .filter(|t| t.table == plan.table)
                .count()
                != 1
            {
                return Err(integrity("capture progress table coverage differs"));
            }
        }
        for checkpoint in &progress.checkpoints {
            if checkpoint.attempt_id != self.request.attempt_id
                || checkpoint.adapter_identity != self.request.adapter_identity
                || checkpoint.connection_identity != self.request.connection_identity
                || checkpoint
                    .tables
                    .iter()
                    .any(|table| !names.contains(&table.table))
            {
                return Err(integrity("checkpoint differs from the fixed acquisition"));
            }
        }
        if progress.stopped
            && self.plans.iter().any(|plan| {
                !progress.checkpoints.iter().any(|checkpoint| {
                    checkpoint
                        .tables
                        .iter()
                        .any(|table| table.table == plan.table)
                })
            })
        {
            return Err(incomplete(
                "completed acquisition lacks a durable table checkpoint",
            ));
        }
        if self.renewal_interval.is_zero()
            || self.renewal_interval > self.ownership.renewal_interval()
        {
            return Err(public_error(
                ErrorCode::InvalidArgument,
                "renewal interval must preserve the run lease clock-skew margin",
            ));
        }
        Ok(())
    }
    /// Begins at most once. A partial nonresumable acquisition is never restarted.
    /// A stopped complete acquisition can be canonicalized without a process.
    pub fn acquire(
        &self,
        session: &mut Session,
        handle: Handle,
        progress: &mut AcquisitionProgress,
        mut persist: impl FnMut(&AcquisitionProgress) -> Result<()>,
    ) -> Result<()> {
        self.validate(progress)?;
        progress.require_unstarted(self.request)?;
        self.ownership.require_open_transfer(self.run)?;
        let shutdown = session
            .channel
            .codec
            .io_mut()
            .shutdown_handle()
            .map_err(local)?;
        let mut owner = self.run.clone();
        self.ownership.renew_run(&mut owner)?;
        let failure = Arc::new(Mutex::new(None));
        let shared = failure.clone();
        let (stop_tx, stop_rx) = mpsc::channel();
        let result = std::thread::scope(|scope| {
            let worker = scope.spawn(move || {
                let mut owner = self.run.clone();
                while matches!(
                    stop_rx.recv_timeout(self.renewal_interval),
                    Err(mpsc::RecvTimeoutError::Timeout)
                ) {
                    if let Err(e) = self.ownership.renew_run(&mut owner) {
                        *shared.lock().unwrap() = Some(e);
                        let _ = shutdown.shutdown(std::net::Shutdown::Both);
                        break;
                    }
                }
            });
            let stop = StopWorker(stop_tx);
            let result = (|| {
                let mut next = progress.clone();
                next.started = true;
                encode(&next)?;
                persist(&next)?;
                *progress = next;
                let req = session.request_id().map_err(|e| e.public())?;
                session
                    .send(
                        &Frame::Extract {
                            req,
                            handle,
                            payload: Doc::inline(self.request.clone()),
                        },
                        &[],
                    )
                    .map_err(|e| e.public())?;
                loop {
                    if let Some(e) = failure.lock().unwrap().clone() {
                        return Err(e);
                    }
                    let packet = session.receive().map_err(|e| e.public())?;
                    match packet.frame {
                        Frame::ExtractStarted { .. } => {}
                        Frame::Checkpoint {
                            checkpoint_id,
                            payload: Doc::Inline(checkpoint),
                            ..
                        } => {
                            let mut next = progress.clone();
                            next.checkpoints.push(checkpoint.inline);
                            encode(&next)?;
                            persist(&next)?;
                            *progress = next;
                            if let Some(e) = failure.lock().unwrap().clone() {
                                return Err(e);
                            }
                            session
                                .send(&Frame::CheckpointAck { req, checkpoint_id }, &[])
                                .map_err(|e| e.public())?;
                        }
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
                                .ok_or_else(|| integrity("unknown capture table"))?;
                            let batch = grv_adapter_wire::ipc::decode(&packet.payload, rows.get())
                                .map_err(|e| {
                                    public_error(ErrorCode::ProtocolFailure, e.to_string())
                                })?;
                            contract::validate_batch(&plan.input_contract()?, &batch).map_err(
                                |e| public_error(ErrorCode::ProtocolFailure, e.to_string()),
                            )?;
                            let key = ObjectKey::new(format!(
                                "source/{}/{table}/batch={}.arrow",
                                self.request.stream_id,
                                seq.get()
                            ))
                            .map_err(|e| integrity(&e.to_string()))?;
                            let raw = RawBatch {
                                key: key.clone(),
                                seq,
                                rows,
                                size: count(packet.payload.len() as u64)?,
                                sha256: grv_types::sha256(&packet.payload),
                            };
                            immutable(self.journal, &key, &packet.payload)?;
                            let mut next = progress.clone();
                            let state = next.tables.iter_mut().find(|t| t.table == table).unwrap();
                            state.row_count = count(
                                state
                                    .row_count
                                    .get()
                                    .checked_add(rows.get())
                                    .ok_or_else(|| integrity("capture rows overflow"))?,
                            )?;
                            state.batches.push(raw);
                            encode(&next)?;
                            persist(&next)?;
                            *progress = next;
                            drop(batch);
                            drop(packet.payload);
                            if let Some(e) = failure.lock().unwrap().clone() {
                                return Err(e);
                            }
                            session
                                .send(
                                    &Frame::BatchAck {
                                        req,
                                        table,
                                        seq,
                                        slot,
                                    },
                                    &[],
                                )
                                .map_err(|e| e.public())?;
                        }
                        Frame::TableComplete {
                            table,
                            row_count,
                            source_identity: Doc::Inline(source),
                            capture,
                            ..
                        } => {
                            let mut next = progress.clone();
                            let state = next
                                .tables
                                .iter_mut()
                                .find(|t| t.table == table)
                                .ok_or_else(|| integrity("unknown completed table"))?;
                            if state.row_count != row_count || state.completion.is_some() {
                                return Err(incomplete(
                                    "table completion differs from acquired rows",
                                ));
                            }
                            state.completion = Some(TableCompletion {
                                row_count,
                                source_identity: source.inline,
                                capture,
                            });
                            encode(&next)?;
                            persist(&next)?;
                            *progress = next;
                        }
                        Frame::SourceComplete {
                            completion: Doc::Inline(completion),
                            ..
                        } => {
                            if progress.tables.iter().any(|t| t.completion.is_none()) {
                                return Err(incomplete(
                                    "source completed without all table completions",
                                ));
                            }
                            session.close().map_err(|e| e.public())?;
                            let mut next = progress.clone();
                            next.completion = Some(completion.inline);
                            next.stopped = true;
                            encode(&next)?;
                            persist(&next)?;
                            *progress = next;
                            break;
                        }
                        _ => {
                            return Err(public_error(
                                ErrorCode::ProtocolFailure,
                                "unexpected extraction lifecycle frame",
                            ));
                        }
                    }
                }
                Ok(())
            })();
            drop(stop);
            if worker.join().is_err() {
                return Err(public_error(
                    ErrorCode::BackendFailure,
                    "run renewal worker failed",
                ));
            }
            result
        });
        if let Some(e) = failure.lock().unwrap().take() {
            Err(e)
        } else {
            result
        }
    }
    /// Requires explicit stopped-work evidence. Replays this operation only over
    /// recorded batch artifacts, and never starts an adapter or source request.
    pub fn canonicalize(
        &self,
        progress: &AcquisitionProgress,
        consistency: SourceConsistency,
    ) -> Result<CaptureReceipt> {
        self.validate(progress)?;
        self.ownership.require_open_transfer(self.run)?;
        let failure = Arc::new(Mutex::new(None));
        let shared = failure.clone();
        let (stop_tx, stop_rx) = mpsc::channel();
        let result = std::thread::scope(|scope| {
            let worker = scope.spawn(move || {
                let mut owner = self.run.clone();
                while matches!(
                    stop_rx.recv_timeout(self.renewal_interval),
                    Err(mpsc::RecvTimeoutError::Timeout)
                ) {
                    if let Err(e) = self.ownership.renew_run(&mut owner) {
                        *shared.lock().unwrap() = Some(e);
                        break;
                    }
                }
            });
            let stop = StopWorker(stop_tx);
            let result = self.canonicalize_inner(progress, consistency, &failure);
            drop(stop);
            if worker.join().is_err() {
                return Err(public_error(
                    ErrorCode::BackendFailure,
                    "run renewal worker failed",
                ));
            }
            result
        });
        if let Some(e) = failure.lock().unwrap().take() {
            Err(e)
        } else {
            result
        }
    }
    fn canonicalize_inner(
        &self,
        progress: &AcquisitionProgress,
        consistency: SourceConsistency,
        failure: &Mutex<Option<grv_types::PublicError>>,
    ) -> Result<CaptureReceipt> {
        if !progress.started
            || !progress.stopped
            || progress.completion.is_none()
            || progress.tables.iter().any(|t| t.completion.is_none())
        {
            return Err(incomplete("complete stopped acquisition evidence required"));
        }
        let mut tables = vec![];
        for plan in self.plans {
            if let Some(e) = failure.lock().unwrap().clone() {
                return Err(e);
            }
            let state = progress
                .tables
                .iter()
                .find(|t| t.table == plan.table)
                .unwrap();
            if state.row_count != state.completion.as_ref().unwrap().row_count {
                return Err(integrity("raw capture count differs from completion"));
            }
            let groups = canonicalize_raw_table(
                self.journal,
                plan,
                &state.batches,
                state.row_count,
                &format!("source/{}/{}", self.request.stream_id, plan.table),
                || match failure.lock().unwrap().clone() {
                    Some(error) => Err(error),
                    None => Ok(()),
                },
            )?;
            tables.push(CapturedTable {
                plan: plan.clone(),
                row_count: state.row_count,
                completion: state.completion.clone().unwrap(),
                groups,
            });
        }
        let receipt = CaptureReceipt {
            receipt_version: 1,
            attempt_id: self.request.attempt_id.clone(),
            run_id: self.request.run_id.clone(),
            declaration_sha256: self.request.declaration_sha256.clone(),
            adapter_identity: self.request.adapter_identity.clone(),
            connection_identity: self.request.connection_identity.clone(),
            source_consistency: consistency,
            checkpoints: progress.checkpoints.clone(),
            completion: progress.completion.clone().unwrap(),
            canonical_writer: CANONICAL_WRITER.into(),
            tables,
        };
        let bytes = encode(&receipt)?;
        immutable(
            self.journal,
            &ObjectKey::new("capture/receipt.json").unwrap(),
            &bytes,
        )?;
        receipt.verify(self.journal, self.request)?;
        Ok(receipt)
    }
}
/// Shared bounded canonicalization of durably recorded IPC batches. Extraction
/// and accepted build exports supply their own closed lifecycle/count evidence.
pub(crate) fn canonicalize_raw_table(
    journal: &Journal,
    plan: &TablePlan,
    batches: &[RawBatch],
    row_count: U64,
    raw_prefix: &str,
    mut check: impl FnMut() -> Result<()>,
) -> Result<Vec<CapturedGroup>> {
    check()?;
    // A single sorter per table puts partition keys first, permitting a
    // bounded second pass over one partition at a time.
    let mut sort_contract = plan.contract.clone();
    let mut order = Vec::new();
    for key in &plan.contract.partition_keys {
        order.push(
            plan.contract
                .columns
                .iter()
                .position(|c| c.name == format!("_{key}_"))
                .unwrap(),
        );
    }
    order.extend((0..plan.contract.columns.len()).filter(|i| {
        !plan
            .contract
            .partition_keys
            .iter()
            .any(|k| plan.contract.columns[*i].name == format!("_{k}_"))
    }));
    sort_contract.columns = order
        .iter()
        .map(|i| plan.contract.columns[*i].clone())
        .collect();
    let mut sorter = Sorter::new(sort_contract.clone(), journal.directory())
        .map_err(|e| integrity(&e.to_string()))?;
    let mut raw_rows = 0u64;
    for (index, raw) in batches.iter().enumerate() {
        check()?;
        if raw.seq.get() != index as u64
            || raw.key.as_str() != format!("{raw_prefix}/batch={index}.arrow")
        {
            return Err(integrity(
                "raw acquisition sequence differs from fixed stream",
            ));
        }
        let bytes = read_raw(journal, raw)?;
        let batch = grv_adapter_wire::ipc::decode(&bytes, raw.rows.get())
            .map_err(|e| integrity(&e.to_string()))?;
        for offset in (0..batch.num_rows()).step_by(1024) {
            let normalized =
                plan.normalize(&batch.slice(offset, (batch.num_rows() - offset).min(1024)))?;
            let sorted = RecordBatch::try_new(
                contract::arrow_schema(&sort_contract).map_err(|e| integrity(&e.to_string()))?,
                order
                    .iter()
                    .map(|i| normalized.column(*i).clone())
                    .collect(),
            )
            .map_err(|e| integrity(&e.to_string()))?;
            sorter
                .append(&sorted)
                .map_err(|e| integrity(&e.to_string()))?;
        }
        raw_rows = raw_rows
            .checked_add(raw.rows.get())
            .ok_or_else(|| integrity("raw row count overflow"))?;
    }
    if raw_rows != row_count.get() {
        return Err(integrity("raw capture count differs from completion"));
    }
    let sorted = sorter.finish().map_err(|e| integrity(&e.to_string()))?;
    let schema = contract::arrow_schema(&plan.contract).map_err(|e| integrity(&e.to_string()))?;
    let inverse: Vec<_> = plan
        .contract
        .columns
        .iter()
        .map(|column| {
            sort_contract
                .columns
                .iter()
                .position(|c| c.name == column.name)
                .unwrap()
        })
        .collect();
    let mut groups = vec![];
    let mut current: Option<(Partition, Sorter)> = None;
    for file in &sorted.files {
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(File::open(&file.path).map_err(local)?)
                .map_err(|_| integrity("invalid canonical sort file"))?
                .with_batch_size(1024)
                .build()
                .map_err(|_| integrity("invalid canonical sort reader"))?;
        for batch in reader {
            let batch = batch.map_err(|_| integrity("invalid canonical sort batch"))?;
            let normalized = RecordBatch::try_new(
                schema.clone(),
                inverse.iter().map(|i| batch.column(*i).clone()).collect(),
            )
            .map_err(|e| integrity(&e.to_string()))?;
            let mut start = 0usize;
            while start < normalized.num_rows() {
                let partition = plan.partition(&normalized, start)?;
                let mut end = start + 1;
                while end < normalized.num_rows() && plan.partition(&normalized, end)? == partition
                {
                    end += 1;
                }
                if current.as_ref().is_some_and(|(p, _)| p != &partition) {
                    let (p, sorter) = current.take().unwrap();
                    groups.push(adopt_group(
                        journal,
                        plan,
                        p,
                        sorter.finish().map_err(|e| integrity(&e.to_string()))?,
                    )?);
                }
                if current.is_none() {
                    current = Some((
                        partition,
                        Sorter::new(plan.contract.clone(), journal.directory())
                            .map_err(|e| integrity(&e.to_string()))?,
                    ));
                }
                current
                    .as_mut()
                    .unwrap()
                    .1
                    .append(&normalized.slice(start, end - start))
                    .map_err(|e| integrity(&e.to_string()))?;
                start = end;
            }
        }
    }
    if let Some((partition, sorter)) = current {
        groups.push(adopt_group(
            journal,
            plan,
            partition,
            sorter.finish().map_err(|e| integrity(&e.to_string()))?,
        )?);
    } else if plan.contract.partition_keys.is_empty() {
        groups.push(adopt_group(
            journal,
            plan,
            Partition::new(),
            Sorter::new(plan.contract.clone(), journal.directory())
                .map_err(|e| integrity(&e.to_string()))?
                .finish()
                .map_err(|e| integrity(&e.to_string()))?,
        )?);
    }
    Ok(groups)
}

pub(crate) fn immutable(journal: &Journal, key: &ObjectKey, bytes: &[u8]) -> Result<()> {
    match journal.create_artifact(key, &mut Cursor::new(bytes)) {
        Ok(_) => Ok(()),
        Err(e) if matches!(e.code, ErrorCode::StateConflict | ErrorCode::OutcomeUnknown) => {
            let mut sink = HashSink::default();
            let meta = journal.get_artifact(key, &mut sink)?;
            if meta.size.get() == bytes.len() as u64 && sink.finish() == grv_types::sha256(bytes) {
                Ok(())
            } else {
                Err(integrity("immutable capture artifact has different bytes"))
            }
        }
        Err(e) => Err(e),
    }
}
#[derive(Default)]
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
impl HashSink {
    fn finish(self) -> Digest {
        Digest::new(format!("{:x}", self.0.finalize())).unwrap()
    }
}
fn read_raw(journal: &Journal, raw: &RawBatch) -> Result<Vec<u8>> {
    struct Bounded(Vec<u8>);
    impl Write for Bounded {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            if b.len() > BATCH_LIMIT.saturating_sub(self.0.len()) {
                return Err(std::io::Error::other("raw batch exceeds supported credit"));
            }
            self.0.extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut bytes = Bounded(vec![]);
    let meta = journal.get_artifact(&raw.key, &mut bytes)?;
    if meta.size.get() != raw.size.get() || grv_types::sha256(&bytes.0) != raw.sha256 {
        return Err(integrity("raw capture batch hash differs"));
    }
    Ok(bytes.0)
}
pub(crate) fn adopt_group(
    journal: &Journal,
    plan: &TablePlan,
    partition: Partition,
    staged: StagedGroup,
) -> Result<CapturedGroup> {
    let prefix = group_prefix(plan, &partition)?;
    let mut files = vec![];
    for file in &staged.files {
        let key = ObjectKey::new(format!("{prefix}/{}", file.name))
            .map_err(|_| integrity("invalid capture file name"))?;
        match journal.create_artifact(&key, &mut File::open(&file.path).map_err(local)?) {
            Ok(_) => {}
            Err(e) if matches!(e.code, ErrorCode::StateConflict | ErrorCode::OutcomeUnknown) => {
                let mut sink = HashSink::default();
                let meta = journal.get_artifact(&key, &mut sink)?;
                if meta.size.get() != file.size || sink.finish() != file.sha256 {
                    return Err(integrity("canonical capture artifact differs on replay"));
                }
            }
            Err(e) => return Err(e),
        }
        files.push(CaptureFile {
            name: file.name.clone(),
            key,
            size: count(file.size)?,
            sha256: file.sha256.clone(),
            rows: count(file.rows)?,
        });
    }
    Ok(CapturedGroup {
        partition,
        row_count: count(staged.row_count)?,
        files,
    })
}
fn group_prefix(plan: &TablePlan, partition: &Partition) -> Result<String> {
    let partition = plan
        .layout()
        .partition_path(partition)
        .map_err(|e| integrity(&e.to_string()))?;
    Ok(format!(
        "capture/{}{}",
        plan.table,
        if partition.is_empty() {
            String::new()
        } else {
            format!("/{partition}")
        }
    ))
}
/// Reads, hashes and validates accepted files into private temporary staging.
/// Ownership::write_group will independently reverify before GRV effects.
pub fn stage_group(
    journal: &Journal,
    plan: &TablePlan,
    group: &CapturedGroup,
) -> Result<StagedGroup> {
    plan.validate()?;
    let prefix = group_prefix(plan, &group.partition)?;
    if group.files.is_empty() {
        return Err(integrity("capture group requires data.parquet"));
    }
    let directory = tempfile::Builder::new()
        .prefix("grv-capture-verify-")
        .tempdir_in(journal.directory())
        .map_err(local)?;
    let mut files = vec![];
    let mut row_count = 0u64;
    for (index, file) in group.files.iter().enumerate() {
        let expected = if index == 0 {
            "data.parquet".into()
        } else {
            format!("data-{index}.parquet")
        };
        if file.name != expected || file.key.as_str() != format!("{prefix}/{expected}") {
            return Err(integrity(
                "capture file identity differs from canonical group",
            ));
        }
        let path = directory.path().join(&expected);
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .map_err(local)?;
        let meta = journal.get_artifact(&file.key, &mut output)?;
        output.sync_all().map_err(local)?;
        drop(output);
        let mut input = File::open(&path).map_err(local)?;
        let mut hash = Sha256::new();
        let mut buffer = [0; 64 * 1024];
        loop {
            let n = input.read(&mut buffer).map_err(local)?;
            if n == 0 {
                break;
            }
            hash.update(&buffer[..n]);
        }
        if meta.size.get() != file.size.get()
            || format!("{:x}", hash.finalize()) != file.sha256.as_str()
        {
            return Err(integrity("accepted capture file size or hash differs"));
        }
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).map_err(local)?)
            .map_err(|_| integrity("accepted capture parquet invalid"))?;
        contract::validate_batch(
            &plan.contract,
            &RecordBatch::new_empty(builder.schema().clone()),
        )
        .map_err(|_| integrity("accepted capture parquet schema differs"))?;
        let reader = builder
            .with_batch_size(1024)
            .build()
            .map_err(|_| integrity("accepted capture reader invalid"))?;
        let mut rows = 0u64;
        for batch in reader {
            let batch = batch.map_err(|_| integrity("accepted capture batch invalid"))?;
            plan.check(&batch)?;
            for row in 0..batch.num_rows() {
                if plan.partition(&batch, row)? != group.partition {
                    return Err(integrity("capture row differs from partition identity"));
                }
            }
            rows = rows
                .checked_add(batch.num_rows() as u64)
                .ok_or_else(|| integrity("capture row count overflow"))?;
        }
        if rows != file.rows.get() {
            return Err(integrity("capture file rows differ from receipt"));
        }
        row_count = row_count
            .checked_add(rows)
            .ok_or_else(|| integrity("capture group count overflow"))?;
        files.push(StagedFile {
            name: expected,
            path,
            size: file.size.get(),
            sha256: file.sha256.clone(),
            rows,
        });
    }
    if row_count != group.row_count.get() {
        return Err(integrity("capture group count differs from receipt"));
    }
    Ok(StagedGroup::adopted(files, row_count, directory))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        clock::SystemClock,
        store::{InitOptions, Store},
    };
    use grv_adapter_api::{
        CaptureWindow, CheckpointTable, ExtractSelection, ExtractTable, SelectionPolicy,
    };
    use grv_storage::{LocalBackend, model::Counter};
    use grv_types::Timestamp;
    use serde_json::json;
    use std::{os::unix::fs::PermissionsExt, path::Path};
    struct Context {
        root: tempfile::TempDir,
        store: Store<LocalBackend>,
        journal: Journal,
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
        let grv = root.path().join("grv");
        std::fs::create_dir(&grv).unwrap();
        let store = Store::initialize(LocalBackend::open(&grv).unwrap(), InitOptions::default())
            .unwrap()
            .0;
        let journal = Journal::create(root.path().join("state"), &[grv]).unwrap();
        Context {
            root,
            store,
            journal,
            clock: SystemClock::default(),
        }
    }
    fn plan(partitioned: bool) -> TablePlan {
        let mut table = json!({"name":"events","columns":[{"name":"value","type":"int64"}]});
        if partitioned {
            table["columns"].as_array_mut().unwrap().extend([
                json!({"name":"day","type":"date32"}),
                json!({"name":"_day_","type":"utf8","derive":{"from":"day","format":"day"}}),
            ]);
            table["partition_keys"] = json!(["day"]);
        }
        TablePlan::from_declaration(
            &json!({"checks":[{"table":"events","not_null":["value"]}]}),
            &table,
        )
        .unwrap()
    }
    fn request(plan: &TablePlan) -> ExtractRequest {
        ExtractRequest {
            attempt_id: Uuid::v4(),
            stream_id: Uuid::v4(),
            root: "local:test".into(),
            dataset: Name::new("data").unwrap(),
            run_id: RunId::new("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            declaration_sha256: Digest::new("a".repeat(64)).unwrap(),
            adapter_identity: AdapterIdentity {
                name: Name::new("fixture").unwrap(),
                package_version: "0.1.0".into(),
                interface_version: grv_types::Req::new(1).unwrap(),
                binding_schema_version: grv_types::Req::new(1).unwrap(),
            },
            connection_identity: "fixture-source".into(),
            selection: ExtractSelection {
                policy: SelectionPolicy::All,
            },
            options: json!({}),
            tables: vec![ExtractTable {
                name: plan.table.clone(),
                source: json!({}),
                columns: json!([]),
                contract: plan.input_contract().unwrap(),
            }],
            resume: None,
        }
    }
    fn run(ctx: &Context, request: &ExtractRequest) -> RunOwner {
        let ownership = Ownership::new(&ctx.store, &ctx.clock, 60).unwrap();
        let mut run = ownership
            .prepare_run(
                request.dataset.clone(),
                request.run_id.clone(),
                Counter::from(0),
                vec![],
                None,
            )
            .unwrap();
        ownership.commit_run(&run).unwrap();
        ownership.confirm_holds(&mut run).unwrap();
        run
    }
    fn stopped(
        ctx: &Context,
        request: &ExtractRequest,
        batches: &[RecordBatch],
    ) -> AcquisitionProgress {
        let time = Timestamp::new("2026-10-06T00:00:00Z").unwrap();
        let mut progress = AcquisitionProgress::prepared(request);
        progress.started = true;
        progress.stopped = true;
        progress.checkpoints.push(Checkpoint {
            attempt_id: request.attempt_id.clone(),
            adapter_identity: request.adapter_identity.clone(),
            connection_identity: request.connection_identity.clone(),
            tables: vec![CheckpointTable {
                table: request.tables[0].name.clone(),
                snapshot_id: "test-fixed".into(),
                reopenable: false,
                source_identity: json!({"table":"fixed"}),
                capture_start: time.clone(),
            }],
            job: json!({}),
        });
        for (index, batch) in batches.iter().enumerate() {
            let mut bytes = vec![];
            {
                let mut writer =
                    arrow_ipc::writer::StreamWriter::try_new(&mut bytes, &batch.schema()).unwrap();
                writer.write(batch).unwrap();
                writer.finish().unwrap();
            }
            bytes.truncate(bytes.len() - 8);
            let key = ObjectKey::new(format!(
                "source/{}/events/batch={index}.arrow",
                request.stream_id
            ))
            .unwrap();
            immutable(&ctx.journal, &key, &bytes).unwrap();
            progress.tables[0].batches.push(RawBatch {
                key,
                seq: SafeInt::new(index as u64).unwrap(),
                rows: count(batch.num_rows() as u64).unwrap(),
                size: count(bytes.len() as u64).unwrap(),
                sha256: grv_types::sha256(&bytes),
            });
            progress.tables[0].row_count =
                count(progress.tables[0].row_count.get() + batch.num_rows() as u64).unwrap();
        }
        let window = CaptureWindow {
            start: time.clone(),
            end: time,
        };
        progress.tables[0].completion = Some(TableCompletion {
            row_count: progress.tables[0].row_count,
            source_identity: json!({"table":"fixed"}),
            capture: window.clone(),
        });
        progress.completion = Some(SourceCompletion {
            job: json!({}),
            capture_window: window,
            adapter_result: json!({}),
        });
        progress
    }
    #[test]
    fn source_free_canonicalization_groups_derived_partitions_and_retains_duplicates() {
        let ctx = context();
        let plan = plan(true);
        let request = request(&plan);
        let run = run(&ctx, &request);
        let ownership = Ownership::new(&ctx.store, &ctx.clock, 60).unwrap();
        let schema = contract::arrow_schema(&plan.input_contract().unwrap()).unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow_array::Int64Array::from(vec![3, 1, 2, 2])),
                Arc::new(arrow_array::Date32Array::from(vec![1, 0, 1, 1])),
            ],
        )
        .unwrap();
        let progress = stopped(&ctx, &request, &[batch.slice(0, 1), batch.slice(1, 3)]);
        let job = CaptureJob {
            ownership: &ownership,
            run: &run,
            journal: &ctx.journal,
            request: &request,
            plans: std::slice::from_ref(&plan),
            renewal_interval: Duration::from_secs(5),
        };
        let receipt = job
            .canonicalize(&progress, SourceConsistency::TransactionSnapshot)
            .unwrap();
        assert_eq!(receipt.tables[0].groups.len(), 2);
        assert_eq!(receipt.tables[0].groups[1].row_count.get(), 3);
        assert_eq!(
            receipt.tables[0].groups[0].partition[&Name::new("day").unwrap()].as_str(),
            "1970-01-01"
        );
        let replay = job
            .canonicalize(&progress, SourceConsistency::TransactionSnapshot)
            .unwrap();
        assert_eq!(receipt.digest().unwrap(), replay.digest().unwrap());
        let staged = stage_group(&ctx.journal, &plan, &receipt.tables[0].groups[1]).unwrap();
        let values: Vec<i64> =
            ParquetRecordBatchReaderBuilder::try_new(File::open(&staged.files[0].path).unwrap())
                .unwrap()
                .build()
                .unwrap()
                .flat_map(|batch| {
                    batch
                        .unwrap()
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow_array::Int64Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect();
        assert_eq!(values, vec![2, 2, 3]);
        assert!(ctx.root.path().join("state/capture/receipt.json").is_file());
    }
    #[test]
    fn zero_row_completion_creates_only_unpartitioned_empty_version() {
        for partitioned in [false, true] {
            let ctx = context();
            let plan = plan(partitioned);
            let request = request(&plan);
            let run = run(&ctx, &request);
            let ownership = Ownership::new(&ctx.store, &ctx.clock, 60).unwrap();
            let progress = stopped(&ctx, &request, &[]);
            let job = CaptureJob {
                ownership: &ownership,
                run: &run,
                journal: &ctx.journal,
                request: &request,
                plans: std::slice::from_ref(&plan),
                renewal_interval: Duration::from_secs(5),
            };
            let receipt = job
                .canonicalize(&progress, SourceConsistency::AdapterDefined)
                .unwrap();
            assert_eq!(receipt.tables[0].groups.len(), usize::from(!partitioned));
            receipt.verify(&ctx.journal, &request).unwrap();
            if !partitioned {
                assert_eq!(receipt.tables[0].groups[0].files.len(), 1);
                assert_eq!(receipt.tables[0].groups[0].files[0].rows.get(), 0);
            }
        }
    }
    #[test]
    fn missing_checkpoint_partial_acquisition_count_mismatch_and_corrupt_capture_are_refused() {
        let ctx = context();
        let plan = plan(false);
        let request = request(&plan);
        let run = run(&ctx, &request);
        let ownership = Ownership::new(&ctx.store, &ctx.clock, 60).unwrap();
        let batch = RecordBatch::try_new(
            contract::arrow_schema(&plan.input_contract().unwrap()).unwrap(),
            vec![Arc::new(arrow_array::Int64Array::from(vec![1, 2]))],
        )
        .unwrap();
        let progress = stopped(&ctx, &request, &[batch]);
        let job = CaptureJob {
            ownership: &ownership,
            run: &run,
            journal: &ctx.journal,
            request: &request,
            plans: std::slice::from_ref(&plan),
            renewal_interval: Duration::from_secs(5),
        };
        let mut partial = progress.clone();
        partial.stopped = false;
        assert_eq!(
            job.canonicalize(&partial, SourceConsistency::AdapterDefined)
                .unwrap_err()
                .code,
            ErrorCode::ExtractionIncomplete
        );
        let mut missing = progress.clone();
        missing.checkpoints.clear();
        assert_eq!(
            job.canonicalize(&missing, SourceConsistency::AdapterDefined)
                .unwrap_err()
                .code,
            ErrorCode::ExtractionIncomplete
        );
        let mut wrong = progress.clone();
        wrong.tables[0].row_count = count(3).unwrap();
        assert_eq!(
            job.canonicalize(&wrong, SourceConsistency::AdapterDefined)
                .unwrap_err()
                .code,
            ErrorCode::IntegrityFailure
        );
        let receipt = job
            .canonicalize(&progress, SourceConsistency::AdapterDefined)
            .unwrap();
        std::fs::write(
            ctx.journal
                .directory()
                .join(receipt.tables[0].groups[0].files[0].key.as_str()),
            b"corrupt",
        )
        .unwrap();
        assert_eq!(
            receipt.verify(&ctx.journal, &request).unwrap_err().code,
            ErrorCode::IntegrityFailure
        );
    }
}
