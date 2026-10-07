//! Adapter dispatch with one channel writer and a bounded source worker.
//!
//! Implementations must cooperate with cancellation and stop every local worker
//! and descendant in `stop_and_wait`. If they do not, no stopped acknowledgement
//! is emitted and the host's signal ladder remains authoritative.
mod build_state;
use grv_adapter_api::*;
use grv_adapter_wire::{
    Codec, Packet, ProtocolError,
    state::{Role, State},
};
use grv_types::{PublicError, ValidationError};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    os::fd::{AsRawFd, FromRawFd},
    os::unix::net::UnixStream,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

pub type Result<T> = std::result::Result<T, PublicError>;
fn error(code: ErrorCode, message: impl Into<String>) -> PublicError {
    PublicError {
        code,
        message: message.into(),
        retryable: false,
        object: None,
    }
}
fn unsupported() -> PublicError {
    error(
        ErrorCode::UnsupportedCapability,
        "adapter operation is not implemented",
    )
}
fn protocol(message: impl Into<String>) -> PublicError {
    error(ErrorCode::ProtocolFailure, message)
}
impl From<ValidationError> for SdkError {
    fn from(e: ValidationError) -> Self {
        Self(e.to_string())
    }
}
impl From<ProtocolError> for SdkError {
    fn from(e: ProtocolError) -> Self {
        Self(e.to_string())
    }
}
impl From<std::io::Error> for SdkError {
    fn from(e: std::io::Error) -> Self {
        Self(e.to_string())
    }
}
#[derive(Debug)]
pub struct SdkError(pub String);
impl std::fmt::Display for SdkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for SdkError {}

#[derive(Clone, Debug)]
pub struct Registration {
    pub name: Name,
    pub package_version: String,
    pub interface_versions: Vec<Req>,
    pub binding_schema_version: Req,
    pub capabilities: Capabilities,
    pub registry: Registry,
    pub commands: Vec<CommandDescriptor>,
}
impl Registration {
    pub fn validate(&self) -> std::result::Result<(), ValidationError> {
        self.capabilities.validate()?;
        self.registry.validate()?;
        if self.package_version.is_empty()
            || self.interface_versions.is_empty()
            || self
                .interface_versions
                .iter()
                .enumerate()
                .any(|(i, v)| self.interface_versions[..i].contains(v))
        {
            return Err(ValidationError("invalid adapter registration".into()));
        }
        for (i, c) in self.commands.iter().enumerate() {
            if (c.requires_authentication && !c.requires_connection)
                || self.commands[..i].iter().any(|old| old.name == c.name)
                || self
                    .registry
                    .schema_bundle
                    .pointer(&c.args_schema_pointer)
                    .is_none()
                || self
                    .registry
                    .schema_bundle
                    .pointer(&c.result_schema_pointer)
                    .is_none()
            {
                return Err(ValidationError("invalid command descriptor".into()));
            }
        }
        Ok(())
    }
    fn identified(&self) -> Frame {
        Frame::Identified {
            name: self.name.clone(),
            package_version: self.package_version.clone(),
            interface_versions: self.interface_versions.clone(),
            binding_schema_version: self.binding_schema_version,
            capabilities: self.capabilities.clone(),
            registry: self.registry.clone(),
            commands: self.commands.clone(),
        }
    }
}
#[derive(Debug, Clone)]
pub struct PreparedCommand {
    pub args: Value,
    pub connection: Option<Value>,
}
#[derive(Debug, Clone)]
pub struct BoundConnection {
    pub handle: Handle,
    pub identity: Option<String>,
    pub workspace_id: Option<Uuid>,
    pub binding: BindingState,
    pub details: Value,
}

/// A cancellation token wakes all checkpoint and credit waits immediately.
#[derive(Clone)]
pub struct StopToken {
    inner: Arc<Flow>,
}
impl StopToken {
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }
    pub fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            Err(error(ErrorCode::ExtractionIncomplete, "operation stopped"))
        } else {
            Ok(())
        }
    }
    fn cancel(&self) {
        self.inner.cancelled.store(true, Ordering::Release);
        self.inner.wake.notify_all();
    }
}
struct Flow {
    cancelled: AtomicBool,
    state: Mutex<FlowState>,
    wake: Condvar,
}
struct FlowState {
    free: BTreeSet<u64>,
    busy: BTreeMap<u64, Name>,
    checkpoints: BTreeSet<Uuid>,
}
impl Flow {
    fn new(slots: u64) -> Arc<Self> {
        Arc::new(Self {
            cancelled: AtomicBool::new(false),
            state: Mutex::new(FlowState {
                free: (0..slots).collect(),
                busy: BTreeMap::new(),
                checkpoints: BTreeSet::new(),
            }),
            wake: Condvar::new(),
        })
    }
    fn wait_checkpoint(&self, id: &Uuid) -> bool {
        let mut s = self.state.lock().unwrap();
        while !s.checkpoints.contains(id) && !self.cancelled.load(Ordering::Acquire) {
            s = self.wake.wait(s).unwrap();
        }
        !self.cancelled.load(Ordering::Acquire)
    }
    fn wait_credit(&self) -> bool {
        let mut s = self.state.lock().unwrap();
        while s.free.is_empty() && !self.cancelled.load(Ordering::Acquire) {
            s = self.wake.wait(s).unwrap();
        }
        !self.cancelled.load(Ordering::Acquire)
    }
    fn slot(&self, table: &Name) -> Option<u64> {
        let mut s = self.state.lock().unwrap();
        loop {
            if self.cancelled.load(Ordering::Acquire) {
                return None;
            }
            if let Some(slot) = s.free.pop_first() {
                s.busy.insert(slot, table.clone());
                return Some(slot);
            }
            s = self.wake.wait(s).unwrap();
        }
    }
    fn drained(&self, table: Option<&Name>) -> bool {
        let mut s = self.state.lock().unwrap();
        while s
            .busy
            .values()
            .any(|t| table.is_none_or(|table| t == table))
            && !self.cancelled.load(Ordering::Acquire)
        {
            s = self.wake.wait(s).unwrap();
        }
        !self.cancelled.load(Ordering::Acquire)
    }
    fn ack(&self, frame: &Frame) {
        let mut s = self.state.lock().unwrap();
        match frame {
            Frame::CheckpointAck { checkpoint_id, .. } => {
                s.checkpoints.insert(checkpoint_id.clone());
            }
            Frame::BatchAck { slot, .. } => {
                s.busy.remove(&slot.get());
                s.free.insert(slot.get());
            }
            _ => {}
        }
        self.wake.notify_all();
    }
}

/// Each event owns its bytes. The SDK will not call `next` while waiting for
/// a checkpoint ACK or before the prior batch acquires a flow-control credit.
pub trait Extraction: Send {
    fn next(&mut self, stop: &StopToken) -> Result<Option<ExtractionEvent>>;
}
pub enum ExtractionEvent {
    Checkpoint {
        checkpoint_id: Uuid,
        payload: Checkpoint,
    },
    Batch {
        table: Name,
        payload: Vec<u8>,
        rows: U64,
    },
    TableComplete {
        table: Name,
        row_count: U64,
        source_identity: Value,
        capture: CaptureWindow,
    },
    SourceComplete(SourceCompletion),
}

/// Accepted build export has its own events: no acquisition checkpoints or
/// source identity fields are invented for stable engine outputs.
pub trait BuildExport: Send {
    fn next(&mut self, stop: &StopToken) -> Result<Option<BuildExportEvent>>;
}
pub enum BuildExportEvent {
    Batch {
        table: Name,
        payload: Vec<u8>,
        rows: U64,
    },
    TableComplete {
        table: Name,
        row_count: U64,
    },
    Complete(Value),
}

pub trait Adapter: Send + 'static {
    fn registration(&self) -> Registration;
    /// Store negotiated budgets only; no authentication, engine, or source I/O.
    fn configure_resources(&mut self, _resources: &Resources) -> Result<()> {
        Ok(())
    }
    /// Store the current cancellation token only. Helpers started by binding
    /// or authentication must stop before stopped-work acknowledgement.
    fn configure_cancellation(&mut self, _stop: &StopToken) -> Result<()> {
        Ok(())
    }
    /// Pure parsing/default expansion: no filesystem, authentication, or HTTP.
    fn prepare_command(&self, name: &Name, argv: &[String]) -> Result<PreparedCommand>;
    fn execute_command(&mut self, call: &CommandCall, stop: &StopToken) -> Result<Value>;
    fn validate_binding(
        &self,
        _declaration: Value,
        _mode: Mode,
        _schema_version: Req,
    ) -> Result<Value> {
        Err(unsupported())
    }
    fn locate_connection(
        &self,
        _connection: Value,
        _mode: Mode,
        _run_id: Option<RunId>,
    ) -> Result<ConnectionLocator> {
        Err(unsupported())
    }
    fn bind_connection(
        &mut self,
        _locator: ConnectionLocator,
        _root: Option<String>,
        _expected_identity: Option<String>,
        _expected_workspace_id: Option<Uuid>,
        _mode: Mode,
    ) -> Result<BoundConnection> {
        Err(unsupported())
    }
    fn authenticate(
        &mut self,
        _handle: Handle,
        _expected_identity: Option<String>,
    ) -> Result<String> {
        Err(unsupported())
    }
    /// Reopen this fixed attempt's durable private state, without reacquiring
    /// source data. Authenticate lazily only if the hook needs it. The hook
    /// must be idempotent/monotone; aborted outcomes cannot advance a cursor.
    fn after_publish(
        &mut self,
        _handle: Handle,
        _request: AfterPublishRequest,
        _stop: &StopToken,
    ) -> Result<()> {
        Err(unsupported())
    }
    fn inspect_connection(
        &mut self,
        _handle: Handle,
        _request: InspectConnectionRequest,
        _stop: &StopToken,
    ) -> Result<Value> {
        Err(unsupported())
    }
    fn extract(
        &mut self,
        _handle: Handle,
        _request: ExtractRequest,
        _stop: &StopToken,
    ) -> Result<Box<dyn Extraction>> {
        Err(unsupported())
    }
    fn resolve_pull(
        &mut self,
        _handle: Handle,
        _request: ResolvePullRequest,
        _stop: &StopToken,
    ) -> Result<PullResolution> {
        Err(unsupported())
    }
    fn prepare_pull(
        &mut self,
        _handle: Handle,
        _request: PreparePullRequest,
        _stop: &StopToken,
    ) -> Result<PullPlan> {
        Err(unsupported())
    }
    fn apply_pull(
        &mut self,
        _handle: Handle,
        _plan: PullPlan,
        _stop: &StopToken,
    ) -> Result<Receipt> {
        Err(unsupported())
    }
    /// Discovery retains its engine transaction and fixed reservation until
    /// exact held files are prepared or stop_and_wait rolls it back.
    fn discover_build(
        &mut self,
        _handle: Handle,
        _request: DiscoverBuildRequest,
        _stop: &StopToken,
    ) -> Result<BuildDiscovery> {
        Err(unsupported())
    }
    fn prepare_build(
        &mut self,
        _handle: Handle,
        _request: PrepareBuildRequest,
        _stop: &StopToken,
    ) -> Result<BuildSession> {
        Err(unsupported())
    }
    /// Successful completion is durable only after invocation connections and
    /// supervised writers have closed. Never invoke an incomplete session again.
    fn execute_build(
        &mut self,
        _handle: Handle,
        _request: ExecuteBuildRequest,
        _stop: &StopToken,
    ) -> Result<BuildExecutionResult> {
        Err(unsupported())
    }
    fn accept_build_completion(
        &mut self,
        _handle: Handle,
        _session_id: Uuid,
        _completion: BuildCompletion,
        _stop: &StopToken,
    ) -> Result<Digest> {
        Err(unsupported())
    }
    fn export_build(
        &mut self,
        _handle: Handle,
        _session_id: Uuid,
        _completion_sha256: Digest,
        _stream_id: Uuid,
        _stop: &StopToken,
    ) -> Result<Box<dyn BuildExport>> {
        Err(unsupported())
    }
    fn open_build(
        &mut self,
        _handle: Handle,
        _identity: BuildIdentity,
        _stop: &StopToken,
    ) -> Result<BuildRecord> {
        Err(unsupported())
    }
    /// Strictly read-only: inspection grants no execution/export authorization.
    fn inspect_build(
        &mut self,
        _handle: Handle,
        _identity: BuildIdentity,
        _stop: &StopToken,
    ) -> Result<BuildRecord> {
        Err(unsupported())
    }
    /// Return only after stopped-writer evidence is established.
    fn abort_build(&mut self, _handle: Handle, _session_id: Uuid, _stop: &StopToken) -> Result<()> {
        Err(unsupported())
    }
    fn record_build_outcome(
        &mut self,
        _handle: Handle,
        _session_id: Uuid,
        _outcome: BuildOutcome,
        _stop: &StopToken,
    ) -> Result<()> {
        Err(unsupported())
    }
    fn cleanup_build(
        &mut self,
        _handle: Handle,
        _session_id: Uuid,
        _stop: &StopToken,
    ) -> Result<()> {
        Err(unsupported())
    }
    /// Return only after all local work and descendants stop and handles close.
    fn stop_and_wait(&mut self) -> Result<()>;
}

fn inline<T>(doc: Doc<T>) -> Result<T> {
    match doc {
        Doc::Inline(v) => Ok(v.inline),
        Doc::Reference(_) => Err(protocol("unresolved metadata document")),
    }
}
fn error_frame(req: Req, e: PublicError) -> Frame {
    Frame::Error {
        req: Some(req),
        code: e.code,
        message: e.message,
        retryable: e.retryable,
        object: e.object.map(|object| *object),
    }
}
/// One bounded outbound document; controls are pumped by the same main writer.
struct Outbound {
    frame: Option<Frame>,
    payload: Vec<u8>,
    bytes: Option<Vec<u8>>,
    id: Option<Uuid>,
    stage: usize,
    acked: bool,
    req: Req,
    terminal: bool,
    source: bool,
    closed: bool,
}
impl Outbound {
    fn new(frame: Frame, payload: Vec<u8>) -> Result<Self> {
        let req = frame.req().expect("operation output");
        let terminal = !matches!(
            frame,
            Frame::ExtractStarted { .. }
                | Frame::Checkpoint { .. }
                | Frame::Batch { .. }
                | Frame::TableComplete { .. }
                | Frame::BuildTableComplete { .. }
        );
        let source = matches!(frame, Frame::SourceComplete { .. });
        let closed = matches!(frame, Frame::CloseResult { .. });
        let mut bytes = None;
        let mut id = None;
        let mut output = frame;
        if serde_json::to_vec(&output).expect("frame serializes").len() + 1
            > grv_adapter_wire::FRAME_LIMIT
        {
            let mut value = serde_json::to_value(&output).expect("frame serializes");
            let doc = value
                .as_object_mut()
                .unwrap()
                .values_mut()
                .find(|v| {
                    v.as_object()
                        .is_some_and(|o| o.len() == 1 && o.contains_key("inline"))
                })
                .ok_or_else(|| {
                    error(
                        ErrorCode::AdapterFailure,
                        "adapter metadata cannot fit a frame",
                    )
                })?;
            let content = doc.as_object_mut().unwrap().remove("inline").unwrap();
            let canonical = grv_types::canonical_json(&content).map_err(|_| {
                error(
                    ErrorCode::AdapterFailure,
                    "adapter document canonicalization failed",
                )
            })?;
            if canonical.len() > grv_adapter_wire::DOCUMENT_LIMIT {
                return Err(error(
                    ErrorCode::AdapterFailure,
                    "adapter metadata exceeds document budget",
                ));
            }
            let document_id = Uuid::v4();
            *doc = serde_json::json!({"document_id":document_id});
            output = serde_json::from_value(value)
                .map_err(|_| protocol("adapter document replacement invalid"))?;
            bytes = Some(canonical);
            id = Some(document_id);
        }
        Ok(Self {
            frame: Some(output),
            payload,
            bytes,
            id,
            stage: 0,
            acked: false,
            req,
            terminal,
            source,
            closed,
        })
    }
    fn next(&mut self) -> Option<(Frame, Vec<u8>)> {
        use base64::{Engine, engine::general_purpose::STANDARD};
        if let Some(bytes) = &self.bytes {
            let document_id = self.id.as_ref().unwrap().clone();
            let chunks = bytes.len().div_ceil(512 * 1024);
            if self.stage == 0 {
                self.stage += 1;
                return Some((
                    Frame::DocumentBegin {
                        document_id,
                        size: U64::new(bytes.len() as u64).unwrap(),
                        sha256: grv_types::sha256(bytes),
                    },
                    vec![],
                ));
            }
            if self.stage <= chunks {
                let index = self.stage - 1;
                self.stage += 1;
                let start = index * 512 * 1024;
                return Some((
                    Frame::DocumentChunk {
                        document_id,
                        index: SafeInt::new(index as u64).unwrap(),
                        data: STANDARD.encode(&bytes[start..bytes.len().min(start + 512 * 1024)]),
                    },
                    vec![],
                ));
            }
            if self.stage == chunks + 1 {
                self.stage += 1;
                return Some((Frame::DocumentEnd { document_id }, vec![]));
            }
            if !self.acked {
                return None;
            }
        }
        self.frame
            .take()
            .map(|f| (f, std::mem::take(&mut self.payload)))
    }
    fn completed(&self) -> bool {
        self.frame.is_none()
    }
}

enum WorkerReply {
    Event(Frame, Vec<u8>),
    Finished(Box<dyn Adapter>, Result<Option<Frame>>),
    StopFailed(String),
}
struct Active {
    stop: StopToken,
    receiver: mpsc::Receiver<WorkerReply>,
    join: thread::JoinHandle<()>,
    req: Req,
    pull_resolution: Option<ResolvePullRequest>,
    request: Frame,
}

#[allow(clippy::manual_is_multiple_of)] // Preserve Rust 1.85 compatibility.
fn produce(
    adapter: &mut dyn Adapter,
    handle: Handle,
    request: ExtractRequest,
    req: Req,
    stop: &StopToken,
    tx: &mpsc::SyncSender<WorkerReply>,
    max_batch: u64,
) -> Result<Option<Frame>> {
    request
        .adapter_identity
        .validate()
        .map_err(|e| protocol(e.to_string()))?;
    for table in &request.tables {
        table
            .contract
            .validate()
            .map_err(|e| protocol(e.to_string()))?;
    }
    let mut stream = adapter.extract(handle, request, stop)?;
    tx.send(WorkerReply::Event(Frame::ExtractStarted { req }, vec![]))
        .map_err(|_| protocol("parent stopped"))?;
    let mut sequences: BTreeMap<Name, u64> = BTreeMap::new();
    while !stop.is_cancelled() {
        if !stop.inner.wait_credit() {
            return Ok(None);
        }
        let event = stream.next(stop)?.ok_or_else(|| {
            error(
                ErrorCode::ExtractionIncomplete,
                "source ended without completion",
            )
        })?;
        let (frame, payload) = match event {
            ExtractionEvent::Checkpoint {
                checkpoint_id,
                payload,
            } => {
                payload.validate().map_err(|e| protocol(e.to_string()))?;
                tx.send(WorkerReply::Event(
                    Frame::Checkpoint {
                        req,
                        checkpoint_id: checkpoint_id.clone(),
                        payload: Doc::inline(payload),
                    },
                    vec![],
                ))
                .map_err(|_| protocol("parent stopped"))?;
                if !stop.inner.wait_checkpoint(&checkpoint_id) {
                    return Ok(None);
                }
                continue;
            }
            ExtractionEvent::Batch {
                table,
                payload,
                rows,
            } => {
                if payload.is_empty()
                    || payload.len() as u64 > max_batch
                    || payload.len() % 8 != 0
                    || rows.get() == 0
                    || rows.get() > i32::MAX as u64
                {
                    return Err(protocol("invalid producer batch"));
                }
                let Some(slot) = stop.inner.slot(&table) else {
                    return Ok(None);
                };
                let seq = sequences.entry(table.clone()).or_default();
                let current = *seq;
                *seq = seq
                    .checked_add(1)
                    .filter(|s| *s <= grv_types::MAX_SAFE_INTEGER)
                    .ok_or_else(|| protocol("sequence exhausted"))?;
                (
                    Frame::Batch {
                        req,
                        table,
                        seq: SafeInt::new(current).unwrap(),
                        slot: SafeInt::new(slot).unwrap(),
                        size: U64::new(payload.len() as u64).unwrap(),
                        rows,
                    },
                    payload,
                )
            }
            ExtractionEvent::TableComplete {
                table,
                row_count,
                source_identity,
                capture,
            } => {
                if !stop.inner.drained(Some(&table)) {
                    return Ok(None);
                }
                (
                    Frame::TableComplete {
                        req,
                        table,
                        row_count,
                        source_identity: Doc::inline(source_identity),
                        capture,
                    },
                    vec![],
                )
            }
            ExtractionEvent::SourceComplete(completion) => {
                if !stop.inner.drained(None) {
                    return Ok(None);
                }
                return Ok(Some(Frame::SourceComplete {
                    req,
                    completion: Doc::inline(completion),
                }));
            }
        };
        tx.send(WorkerReply::Event(frame, payload))
            .map_err(|_| protocol("parent stopped"))?;
    }
    Ok(None)
}

#[allow(clippy::manual_is_multiple_of)] // Preserve Rust 1.85 compatibility.
fn produce_build(
    mut stream: Box<dyn BuildExport>,
    session_id: Uuid,
    completion_sha256: Digest,
    req: Req,
    stop: &StopToken,
    tx: &mpsc::SyncSender<WorkerReply>,
    max_batch: u64,
) -> Result<Option<Frame>> {
    let mut sequences: BTreeMap<Name, u64> = BTreeMap::new();
    while !stop.is_cancelled() {
        if !stop.inner.wait_credit() {
            return Ok(None);
        }
        let event = stream.next(stop)?.ok_or_else(|| {
            error(
                ErrorCode::BuildIncomplete,
                "build export ended without completion",
            )
        })?;
        let (frame, payload) = match event {
            BuildExportEvent::Batch {
                table,
                payload,
                rows,
            } => {
                if payload.is_empty()
                    || payload.len() as u64 > max_batch
                    || payload.len() % 8 != 0
                    || rows.get() == 0
                    || rows.get() > i32::MAX as u64
                {
                    return Err(protocol("invalid build export batch"));
                }
                let Some(slot) = stop.inner.slot(&table) else {
                    return Ok(None);
                };
                let seq = sequences.entry(table.clone()).or_default();
                let current = *seq;
                *seq = seq
                    .checked_add(1)
                    .filter(|s| *s <= grv_types::MAX_SAFE_INTEGER)
                    .ok_or_else(|| protocol("sequence exhausted"))?;
                (
                    Frame::Batch {
                        req,
                        table,
                        seq: SafeInt::new(current).unwrap(),
                        slot: SafeInt::new(slot).unwrap(),
                        size: U64::new(payload.len() as u64).unwrap(),
                        rows,
                    },
                    payload,
                )
            }
            BuildExportEvent::TableComplete { table, row_count } => {
                if !stop.inner.drained(Some(&table)) {
                    return Ok(None);
                }
                (
                    Frame::BuildTableComplete {
                        req,
                        table,
                        row_count,
                    },
                    vec![],
                )
            }
            BuildExportEvent::Complete(result) => {
                if !stop.inner.drained(None) {
                    return Ok(None);
                }
                return Ok(Some(Frame::ExportComplete {
                    req,
                    session_id,
                    completion_sha256,
                    adapter_result: Doc::inline(result),
                }));
            }
        };
        tx.send(WorkerReply::Event(frame, payload))
            .map_err(|_| protocol("parent stopped"))?;
    }
    Ok(None)
}

fn start(
    mut adapter: Box<dyn Adapter>,
    frame: Frame,
    call: Option<CommandCall>,
    resources: &Resources,
) -> Active {
    let req = frame.req().expect("ordinary request");
    let flow = Flow::new(resources.slots.get());
    let stop = StopToken { inner: flow };
    let worker_stop = stop.clone();
    let max_batch = resources.max_batch_bytes.get();
    let (tx, receiver) = mpsc::sync_channel(1);
    let is_extract = matches!(frame, Frame::Extract { .. } | Frame::ExportBuild { .. });
    let request_frame = frame.clone();
    let pull_resolution = match &frame {
        Frame::ResolvePull {
            payload: Doc::Inline(query),
            ..
        } => Some(query.inline.clone()),
        _ => None,
    };
    let join = thread::spawn(move || {
        let mut result = (|| -> Result<Option<Frame>> {
            adapter.configure_cancellation(&worker_stop)?;
            Ok(Some(match frame {
                Frame::ValidateBinding {
                    declaration,
                    mode,
                    schema_version,
                    ..
                } => Frame::ValidateResult {
                    req,
                    effective_declaration: Doc::inline(adapter.validate_binding(
                        inline(declaration)?,
                        mode,
                        schema_version,
                    )?),
                },
                Frame::LocateConnection {
                    connection,
                    mode,
                    run_id,
                    ..
                } => Frame::ConnectionLocated {
                    req,
                    connection: Doc::inline(adapter.locate_connection(
                        inline(connection)?,
                        mode,
                        run_id,
                    )?),
                },
                Frame::BindConnection {
                    locator,
                    root,
                    expected_identity,
                    expected_workspace_id,
                    mode,
                    ..
                } => {
                    let b = adapter.bind_connection(
                        inline(locator)?,
                        root,
                        expected_identity,
                        expected_workspace_id,
                        mode,
                    )?;
                    Frame::BindResult {
                        req,
                        handle: b.handle,
                        identity: b.identity,
                        workspace_id: b.workspace_id,
                        binding: b.binding,
                        details: Doc::inline(b.details),
                    }
                }
                Frame::Authenticate {
                    handle,
                    expected_identity,
                    ..
                } => Frame::AuthenticateResult {
                    req,
                    identity: adapter.authenticate(handle, expected_identity)?,
                },
                Frame::AfterPublish {
                    handle,
                    attempt_id,
                    declaration_sha256,
                    outcome,
                    ..
                } => {
                    adapter.after_publish(
                        handle,
                        AfterPublishRequest {
                            attempt_id,
                            declaration_sha256,
                            outcome,
                        },
                        &worker_stop,
                    )?;
                    Frame::AfterPublishResult {
                        req,
                        acknowledged: true,
                    }
                }
                Frame::InspectConnection {
                    handle,
                    root,
                    declaration,
                    ..
                } => Frame::InspectResult {
                    req,
                    details: Doc::inline(adapter.inspect_connection(
                        handle,
                        InspectConnectionRequest {
                            root,
                            declaration: declaration.map(inline).transpose()?,
                        },
                        &worker_stop,
                    )?),
                },
                Frame::Command { .. } => Frame::CommandResult {
                    req,
                    details: Doc::inline(adapter.execute_command(
                        &call.ok_or_else(|| protocol("missing prepared command"))?,
                        &worker_stop,
                    )?),
                },
                Frame::Extract {
                    handle, payload, ..
                } => {
                    return produce(
                        adapter.as_mut(),
                        handle,
                        inline(payload)?,
                        req,
                        &worker_stop,
                        &tx,
                        max_batch,
                    );
                }
                Frame::ResolvePull {
                    handle, payload, ..
                } => {
                    let query = inline(payload)?;
                    let resolution = adapter.resolve_pull(handle, query.clone(), &worker_stop)?;
                    resolution
                        .validate_for(&query)
                        .map_err(|e| protocol(e.to_string()))?;
                    Frame::ResolveResult {
                        req,
                        resolution: Doc::inline(resolution),
                    }
                }
                Frame::PreparePull {
                    handle, payload, ..
                } => {
                    let request = inline(payload)?;
                    let plan = adapter.prepare_pull(handle, request.clone(), &worker_stop)?;
                    plan.validate().map_err(|e| protocol(e.to_string()))?;
                    if plan.request != request.request
                        || plan.tables != request.tables
                        || plan.files != request.files
                        || plan.resolved_revision != request.resolved_revision
                    {
                        return Err(protocol(
                            "prepared pull plan changed verified parent inputs",
                        ));
                    }
                    Frame::PrepareResult {
                        req,
                        plan: Doc::inline(plan),
                    }
                }
                Frame::ApplyPull { handle, plan, .. } => {
                    let plan = inline(plan)?;
                    let receipt = adapter.apply_pull(handle, plan.clone(), &worker_stop)?;
                    receipt
                        .validate_for(&plan)
                        .map_err(|e| protocol(e.to_string()))?;
                    Frame::ApplyResult {
                        req,
                        receipt: Doc::inline(receipt),
                    }
                }
                Frame::DiscoverBuild {
                    handle, payload, ..
                } => Frame::BuildDiscovered {
                    req,
                    discovery: Doc::inline(adapter.discover_build(
                        handle,
                        inline(payload)?,
                        &worker_stop,
                    )?),
                },
                Frame::PrepareBuild {
                    handle, payload, ..
                } => Frame::BuildPrepared {
                    req,
                    session: Doc::inline(adapter.prepare_build(
                        handle,
                        inline(payload)?,
                        &worker_stop,
                    )?),
                },
                Frame::ExecuteBuild {
                    handle, payload, ..
                } => Frame::BuildFinished {
                    req,
                    result: Doc::inline(adapter.execute_build(
                        handle,
                        inline(payload)?,
                        &worker_stop,
                    )?),
                },
                Frame::AcceptBuildCompletion {
                    handle,
                    session_id,
                    completion,
                    ..
                } => Frame::CompletionAccepted {
                    req,
                    session_id: session_id.clone(),
                    completion_sha256: adapter.accept_build_completion(
                        handle,
                        session_id,
                        inline(completion)?,
                        &worker_stop,
                    )?,
                },
                Frame::ExportBuild {
                    handle,
                    session_id,
                    completion_sha256,
                    stream_id,
                    ..
                } => {
                    let stream = adapter.export_build(
                        handle,
                        session_id.clone(),
                        completion_sha256.clone(),
                        stream_id,
                        &worker_stop,
                    )?;
                    return produce_build(
                        stream,
                        session_id,
                        completion_sha256,
                        req,
                        &worker_stop,
                        &tx,
                        max_batch,
                    );
                }
                Frame::OpenBuild {
                    handle, payload, ..
                } => Frame::BuildOpened {
                    req,
                    record: Doc::inline(adapter.open_build(
                        handle,
                        inline(payload)?,
                        &worker_stop,
                    )?),
                },
                Frame::InspectBuild {
                    handle, payload, ..
                } => Frame::BuildInspected {
                    req,
                    record: Doc::inline(adapter.inspect_build(
                        handle,
                        inline(payload)?,
                        &worker_stop,
                    )?),
                },
                Frame::AbortBuild {
                    handle, session_id, ..
                } => {
                    adapter.abort_build(handle, session_id.clone(), &worker_stop)?;
                    Frame::BuildAborted {
                        req,
                        session_id,
                        writers_stopped: true,
                    }
                }
                Frame::RecordBuildOutcome {
                    handle,
                    session_id,
                    outcome,
                    ..
                } => {
                    adapter.record_build_outcome(
                        handle,
                        session_id.clone(),
                        outcome,
                        &worker_stop,
                    )?;
                    Frame::BuildOutcomeRecorded { req, session_id }
                }
                Frame::CleanupBuild {
                    handle, session_id, ..
                } => {
                    adapter.cleanup_build(handle, session_id.clone(), &worker_stop)?;
                    Frame::BuildCleaned { req, session_id }
                }
                Frame::Close { .. } => {
                    adapter.stop_and_wait()?;
                    Frame::CloseResult { req }
                }
                _ => return Err(protocol("unexpected SDK request")),
            }))
        })();
        if worker_stop.is_cancelled() {
            if is_extract {
                result = Ok(None);
            }
            if let Err(e) = adapter.stop_and_wait() {
                let _ = tx.send(WorkerReply::StopFailed(e.message));
                return;
            }
        }
        let _ = tx.send(WorkerReply::Finished(adapter, result));
    });
    Active {
        stop,
        receiver,
        join,
        req,
        pull_resolution,
        request: request_frame,
    }
}

/// Serve an already-connected channel. Useful for process conformance tests.
pub fn serve(adapter: impl Adapter, stream: UnixStream) -> std::result::Result<(), SdkError> {
    let registration = adapter.registration();
    registration.validate()?;
    let reader_stream = stream.try_clone()?;
    let shutdown = stream.try_clone()?;
    let (tx, input) = mpsc::sync_channel(4);
    let reader = thread::spawn(move || {
        let mut codec = Codec::new(reader_stream, 67108864);
        loop {
            match codec.read() {
                Ok(p) => {
                    if tx.send(Ok(p)).is_err() {
                        break;
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(e));
                    break;
                }
            }
        }
    });
    let mut writer = Codec::new(stream, 67108864);
    let mut state = State::new(Role::Adapter);
    let send = |state: &mut State,
                writer: &mut Codec<UnixStream>,
                frame: &Frame,
                payload: &[u8]|
     -> std::result::Result<(), SdkError> {
        state.observe(frame, true)?;
        writer.write(frame, payload)?;
        Ok(())
    };
    let mut adapter: Option<Box<dyn Adapter>> = Some(Box::new(adapter));
    let mut active: Option<Active> = None;
    let mut prepared: Option<CommandCall> = None;
    let mut executed = false;
    let mut bound_handle: Option<Handle> = None;
    let mut authenticate_called = false;
    let mut hook_requests: BTreeMap<Uuid, AfterPublishRequest> = BTreeMap::new();
    let mut prepared_pull: Option<PullPlan> = None;
    let mut pull_lookup: Option<(Uuid, String)> = None;
    let mut pull_authorization: Option<(RequestRecord, Option<Value>)> = None;
    let mut pull_applied = false;
    let mut builds = build_state::BuildLifecycle::default();
    let mut resources = Resources::default();
    let mut last_completed: Option<Req> = None;
    let mut pending_cancel: Option<Req> = None;
    let mut last_cancel: Option<(Req, CancelState)> = None;
    let mut outbound: Option<Outbound> = None;
    let mut pending_failure: Option<PublicError> = None;
    let outcome = (|| -> std::result::Result<(), SdkError> {
        let Packet { frame, .. } = input
            .recv()
            .map_err(|_| SdkError("bootstrap EOF".into()))??;
        let Frame::Hello {
            interface_versions,
            resources: offered,
            ..
        } = &frame
        else {
            return Err(SdkError("expected hello".into()));
        };
        offered.validate()?;
        resources = offered.clone();
        state.observe(&frame, false)?;
        send(&mut state, &mut writer, &registration.identified(), &[])?;
        let Packet { frame, .. } = input
            .recv()
            .map_err(|_| SdkError("bootstrap EOF".into()))??;
        let Frame::Ready {
            interface_version,
            binding_schema_version,
            ..
        } = &frame
        else {
            return Err(SdkError("expected ready".into()));
        };
        if !interface_versions.contains(interface_version)
            || !registration.interface_versions.contains(interface_version)
            || *binding_schema_version != registration.binding_schema_version
        {
            return Err(SdkError("invalid negotiation".into()));
        }
        state.observe(&frame, false)?;
        if let Err(e) = adapter.as_mut().unwrap().configure_resources(&resources) {
            send(
                &mut state,
                &mut writer,
                &Frame::Error {
                    req: None,
                    code: e.code,
                    message: e.message,
                    retryable: e.retryable,
                    object: e.object.map(|o| *o),
                },
                &[],
            )?;
            return Err(SdkError("adapter refused negotiated resources".into()));
        }
        loop {
            if let Some(output) = outbound.as_mut() {
                if let Some((f, payload)) = output.next() {
                    send(&mut state, &mut writer, &f, &payload)?;
                }
                if output.completed() {
                    let output = outbound.take().unwrap();
                    if output.terminal {
                        last_completed = Some(output.req);
                        if let Some(req) = pending_cancel.take() {
                            send(
                                &mut state,
                                &mut writer,
                                &Frame::CancelAck {
                                    req,
                                    state: CancelState::Completed,
                                },
                                &[],
                            )?;
                            last_cancel = Some((req, CancelState::Completed));
                        }
                    }
                    if output.closed {
                        return Ok(());
                    }
                }
            }
            if outbound.is_none()
                && let Some(a) = active.as_ref()
            {
                match a.receiver.try_recv() {
                    Ok(WorkerReply::Event(f, payload)) => {
                        if !a.stop.is_cancelled() {
                            match Outbound::new(f, payload) {
                                Ok(output) => outbound = Some(output),
                                Err(e) => {
                                    a.stop.cancel();
                                    pending_failure = Some(e);
                                }
                            }
                        }
                    }
                    Ok(WorkerReply::StopFailed(message)) => {
                        return Err(SdkError(format!(
                            "stopped-work evidence unavailable: {message}"
                        )));
                    }
                    Ok(WorkerReply::Finished(returned, mut result)) => {
                        if let Ok(Some(frame)) = &result
                            && let Err(error) =
                                builds.observe_result(&active.as_ref().unwrap().request, frame)
                        {
                            result = Err(error);
                        }
                        if let Ok(Some(Frame::BindResult { handle, .. })) = &result {
                            bound_handle = Some(handle.clone());
                        }
                        if let Ok(Some(Frame::PrepareResult {
                            plan: Doc::Inline(plan),
                            ..
                        })) = &result
                        {
                            prepared_pull = Some(plan.inline.clone());
                        }
                        let a = active.take().unwrap();
                        if let (
                            Some(query),
                            Ok(Some(Frame::ResolveResult {
                                resolution: Doc::Inline(result),
                                ..
                            })),
                        ) = (&a.pull_resolution, &result)
                        {
                            match query.phase {
                                ResolvePhase::Lookup => {
                                    pull_lookup =
                                        Some((query.attempt_id.clone(), query.root.clone()))
                                }
                                ResolvePhase::Compare => {
                                    pull_authorization =
                                        if result.inline.state == ResolutionState::NotCommitted {
                                            query.request.clone().map(|request| {
                                                (request, result.inline.recovery.clone())
                                            })
                                        } else {
                                            None
                                        };
                                }
                            }
                        }
                        a.join
                            .join()
                            .map_err(|_| SdkError("adapter worker panicked".into()))?;
                        adapter = Some(returned);
                        if let Some(e) = pending_failure.take() {
                            outbound = Some(
                                Outbound::new(error_frame(a.req, e), vec![])
                                    .map_err(|e| SdkError(e.message))?,
                            );
                        } else if let Some(req) = pending_cancel {
                            match result {
                                Ok(Some(f)) => {
                                    outbound = Some(Outbound::new(f, vec![]).unwrap_or_else(|e| {
                                        Outbound::new(error_frame(req, e), vec![]).unwrap()
                                    }))
                                }
                                _ => {
                                    pending_cancel = None;
                                    send(
                                        &mut state,
                                        &mut writer,
                                        &Frame::CancelAck {
                                            req,
                                            state: CancelState::Stopped,
                                        },
                                        &[],
                                    )?;
                                    last_cancel = Some((req, CancelState::Stopped));
                                }
                            }
                        } else {
                            match result {
                                Ok(Some(f)) => {
                                    outbound = Some(Outbound::new(f, vec![]).unwrap_or_else(|e| {
                                        Outbound::new(error_frame(a.req, e), vec![]).unwrap()
                                    }))
                                }
                                Ok(None) => {
                                    return Err(SdkError(
                                        "source stopped without cancellation".into(),
                                    ));
                                }
                                Err(e) => {
                                    outbound = Some(
                                        Outbound::new(error_frame(a.req, e), vec![])
                                            .map_err(|e| SdkError(e.message))?,
                                    )
                                }
                            }
                        }
                    }
                    Err(mpsc::TryRecvError::Disconnected) => {
                        return Err(SdkError("adapter worker ended without evidence".into()));
                    }
                    Err(mpsc::TryRecvError::Empty) => {}
                }
            }
            let packet = match input.recv_timeout(Duration::from_millis(2)) {
                Ok(p) => p?,
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(SdkError("channel EOF".into()));
                }
            };
            let frame = state.resolved_frame(&packet.frame, false)?;
            frame.validate()?;
            state.observe(&packet.frame, false)?;
            let invalid_lifecycle = match &frame {
                Frame::BindConnection { .. } => bound_handle.is_some(),
                Frame::Authenticate { handle, .. } => {
                    authenticate_called || bound_handle.as_ref() != Some(handle)
                }
                Frame::Extract { handle, .. } => bound_handle.as_ref() != Some(handle),
                Frame::AfterPublish { handle, .. } => bound_handle.as_ref() != Some(handle),
                Frame::ResolvePull { handle, .. }
                | Frame::InspectConnection { handle, .. }
                | Frame::PreparePull { handle, .. }
                | Frame::ApplyPull { handle, .. }
                | Frame::DiscoverBuild { handle, .. }
                | Frame::PrepareBuild { handle, .. }
                | Frame::ExecuteBuild { handle, .. }
                | Frame::AcceptBuildCompletion { handle, .. }
                | Frame::ExportBuild { handle, .. }
                | Frame::OpenBuild { handle, .. }
                | Frame::InspectBuild { handle, .. }
                | Frame::AbortBuild { handle, .. }
                | Frame::RecordBuildOutcome { handle, .. }
                | Frame::CleanupBuild { handle, .. } => bound_handle.as_ref() != Some(handle),
                Frame::Command {
                    handle: Some(handle),
                    ..
                } => bound_handle.as_ref() != Some(handle),
                _ => false,
            };
            if invalid_lifecycle {
                send(
                    &mut state,
                    &mut writer,
                    &error_frame(
                        frame.req().unwrap(),
                        protocol("invalid bound-handle lifecycle"),
                    ),
                    &[],
                )?;
                last_completed = frame.req();
                continue;
            }
            if let Frame::AfterPublish {
                attempt_id,
                declaration_sha256,
                outcome,
                ..
            } = &frame
            {
                let request = AfterPublishRequest {
                    attempt_id: attempt_id.clone(),
                    declaration_sha256: declaration_sha256.clone(),
                    outcome: outcome.clone(),
                };
                let failure = if !registration.capabilities.after_publish {
                    Some(unsupported())
                } else if hook_requests
                    .get(attempt_id)
                    .is_some_and(|fixed| fixed != &request)
                {
                    Some(error(
                        ErrorCode::RequestMismatch,
                        "after-publish fixed request differs",
                    ))
                } else if !hook_requests.contains_key(attempt_id) && hook_requests.len() >= 1024 {
                    Some(protocol(
                        "after-publish attempt count exceeds channel bound",
                    ))
                } else {
                    None
                };
                if let Some(failure) = failure {
                    send(
                        &mut state,
                        &mut writer,
                        &error_frame(frame.req().unwrap(), failure),
                        &[],
                    )?;
                    last_completed = frame.req();
                    continue;
                }
                hook_requests.insert(attempt_id.clone(), request);
            }
            if let Err(error) = builds.authorize(&frame) {
                send(
                    &mut state,
                    &mut writer,
                    &error_frame(frame.req().unwrap(), error),
                    &[],
                )?;
                last_completed = frame.req();
                continue;
            }
            if matches!(frame, Frame::Authenticate { .. }) {
                authenticate_called = true;
            }
            if let Frame::ResolvePull {
                payload: Doc::Inline(query),
                ..
            } = &frame
            {
                let invalid = prepared_pull.is_some()
                    || (query.inline.phase == ResolvePhase::Compare
                        && pull_lookup.as_ref()
                            != Some(&(query.inline.attempt_id.clone(), query.inline.root.clone())));
                if invalid {
                    send(
                        &mut state,
                        &mut writer,
                        &error_frame(
                            frame.req().unwrap(),
                            protocol("pull comparison requires lookup before preparation"),
                        ),
                        &[],
                    )?;
                    last_completed = frame.req();
                    continue;
                }
                pull_authorization = None;
            }
            if let Frame::PreparePull {
                payload: Doc::Inline(request),
                ..
            } = &frame
                && (prepared_pull.is_some()
                    || pull_authorization.as_ref()
                        != Some(&(
                            request.inline.request.clone(),
                            request.inline.recovery.clone(),
                        )))
            {
                send(
                    &mut state,
                    &mut writer,
                    &error_frame(
                        frame.req().unwrap(),
                        protocol("pull preparation requires a matching not-committed comparison"),
                    ),
                    &[],
                )?;
                last_completed = frame.req();
                continue;
            }
            if let Frame::ApplyPull {
                plan: Doc::Inline(plan),
                ..
            } = &frame
            {
                if pull_applied || prepared_pull.as_ref() != Some(&plan.inline) {
                    send(
                        &mut state,
                        &mut writer,
                        &error_frame(
                            frame.req().unwrap(),
                            protocol("changed, unknown or reused pull plan"),
                        ),
                        &[],
                    )?;
                    last_completed = frame.req();
                    continue;
                }
                pull_applied = true;
            }
            match frame {
                Frame::DocumentEnd { document_id } => send(
                    &mut state,
                    &mut writer,
                    &Frame::DocumentAck { document_id },
                    &[],
                )?,
                Frame::DocumentAck { document_id } => {
                    if let Some(output) = outbound.as_mut()
                        && output.id.as_ref() == Some(&document_id)
                    {
                        output.acked = true;
                    }
                }
                Frame::DocumentBegin { .. } | Frame::DocumentChunk { .. } => {}
                Frame::CheckpointAck { .. } | Frame::BatchAck { .. } => {
                    if let Some(a) = &active {
                        a.stop.inner.ack(&frame);
                    }
                }
                Frame::Cancel { req } => {
                    if let Some((cached_req, cached_state)) = last_cancel
                        && cached_req == req
                    {
                        send(
                            &mut state,
                            &mut writer,
                            &Frame::CancelAck {
                                req,
                                state: cached_state,
                            },
                            &[],
                        )?;
                        continue;
                    }
                    if let Some(a) = &active {
                        a.stop.cancel();
                        pending_cancel = Some(req);
                        outbound = None;
                    } else if outbound.as_ref().is_some_and(|output| output.req == req) {
                        adapter
                            .as_mut()
                            .unwrap()
                            .stop_and_wait()
                            .map_err(|e| SdkError(e.message))?;
                        if outbound.as_ref().unwrap().source {
                            outbound = None;
                            send(
                                &mut state,
                                &mut writer,
                                &Frame::CancelAck {
                                    req,
                                    state: CancelState::Stopped,
                                },
                                &[],
                            )?;
                            last_cancel = Some((req, CancelState::Stopped));
                        } else {
                            pending_cancel = Some(req);
                        }
                    } else if last_completed == Some(req) {
                        adapter
                            .as_mut()
                            .unwrap()
                            .stop_and_wait()
                            .map_err(|e| SdkError(e.message))?;
                        send(
                            &mut state,
                            &mut writer,
                            &Frame::CancelAck {
                                req,
                                state: CancelState::Completed,
                            },
                            &[],
                        )?;
                        last_cancel = Some((req, CancelState::Completed));
                    }
                }
                Frame::PrepareCommand { req, name, argv } => {
                    let result = (|| -> Result<CommandCall> {
                        if prepared.is_some() {
                            return Err(protocol("a call is already prepared"));
                        }
                        let descriptor = registration
                            .commands
                            .iter()
                            .find(|d| d.name == name)
                            .ok_or_else(unsupported)?;
                        let p = adapter
                            .as_ref()
                            .unwrap()
                            .prepare_command(&name, &inline(argv)?)?;
                        if p.connection.is_some() != descriptor.requires_connection {
                            return Err(protocol("prepared connection disagrees with descriptor"));
                        }
                        Ok(CommandCall {
                            command_id: Uuid::v4(),
                            name,
                            args: p.args,
                            connection: p.connection,
                        })
                    })();
                    match result {
                        Ok(call) => {
                            prepared = Some(call.clone());
                            outbound = Some(
                                Outbound::new(
                                    Frame::CommandPrepared {
                                        req,
                                        call: Doc::inline(call),
                                    },
                                    vec![],
                                )
                                .unwrap_or_else(|e| {
                                    Outbound::new(error_frame(req, e), vec![]).unwrap()
                                }),
                            );
                        }
                        Err(e) => {
                            send(&mut state, &mut writer, &error_frame(req, e), &[])?;
                            last_completed = Some(req);
                        }
                    }
                }
                Frame::Command {
                    req,
                    ref command_id,
                    ref handle,
                } => {
                    let Some(call) = prepared
                        .as_ref()
                        .filter(|c| c.command_id == *command_id && !executed)
                        .cloned()
                    else {
                        send(
                            &mut state,
                            &mut writer,
                            &error_frame(req, protocol("unknown or reused command ID")),
                            &[],
                        )?;
                        last_completed = Some(req);
                        continue;
                    };
                    let descriptor = registration
                        .commands
                        .iter()
                        .find(|d| d.name == call.name)
                        .expect("prepared descriptor");
                    if descriptor.requires_authentication && !authenticate_called {
                        send(
                            &mut state,
                            &mut writer,
                            &error_frame(req, protocol("command requires authentication")),
                            &[],
                        )?;
                        last_completed = Some(req);
                        continue;
                    }
                    if handle.is_some() != call.connection.is_some() {
                        send(
                            &mut state,
                            &mut writer,
                            &error_frame(req, protocol("command handle disagrees with descriptor")),
                            &[],
                        )?;
                        last_completed = Some(req);
                        continue;
                    }
                    executed = true;
                    active = Some(start(
                        adapter.take().unwrap(),
                        frame,
                        Some(call),
                        &resources,
                    ));
                }
                Frame::ValidateBinding { .. }
                | Frame::LocateConnection { .. }
                | Frame::BindConnection { .. }
                | Frame::Authenticate { .. }
                | Frame::AfterPublish { .. }
                | Frame::InspectConnection { .. }
                | Frame::Extract { .. }
                | Frame::ResolvePull { .. }
                | Frame::PreparePull { .. }
                | Frame::ApplyPull { .. }
                | Frame::DiscoverBuild { .. }
                | Frame::PrepareBuild { .. }
                | Frame::ExecuteBuild { .. }
                | Frame::AcceptBuildCompletion { .. }
                | Frame::ExportBuild { .. }
                | Frame::OpenBuild { .. }
                | Frame::InspectBuild { .. }
                | Frame::AbortBuild { .. }
                | Frame::RecordBuildOutcome { .. }
                | Frame::CleanupBuild { .. }
                | Frame::Close { .. } => {
                    active = Some(start(adapter.take().unwrap(), frame, None, &resources))
                }
                _ => return Err(SdkError("unexpected SDK frame".into())),
            }
        }
    })();
    if let Some(a) = active.take() {
        a.stop.cancel();
        while let Ok(reply) = a.receiver.recv() {
            match reply {
                WorkerReply::Finished(mut returned, _) => {
                    let _ = returned.stop_and_wait();
                    break;
                }
                WorkerReply::StopFailed(_) => break,
                WorkerReply::Event(..) => {}
            }
        }
        let _ = a.join.join();
    }
    if let Some(mut adapter) = adapter {
        let _ = adapter.stop_and_wait();
    }
    let _ = shutdown.shutdown(std::net::Shutdown::Both);
    drop(input);
    let _ = reader.join();
    outcome
}

/// fd 3 is reserved by the host and is never inherited by engine children.
pub fn run_fd3(adapter: impl Adapter) -> std::result::Result<(), SdkError> {
    let stream = unsafe { UnixStream::from_raw_fd(3) };
    let fd = stream.as_raw_fd();
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    serve(adapter, stream)
}

/// Tracks direct helper children. Adapters must also supervise any further
/// descendants and remote jobs before their stopped-work acknowledgement.
#[derive(Default)]
pub struct Descendants {
    children: Vec<std::process::Child>,
}
impl Descendants {
    pub fn spawn(&mut self, command: &mut std::process::Command) -> std::io::Result<u32> {
        use std::process::Stdio;
        let child = command.stdin(Stdio::null()).stdout(Stdio::null()).spawn()?;
        let id = child.id();
        self.children.push(child);
        Ok(id)
    }
    pub fn stop_and_wait(&mut self) -> std::io::Result<()> {
        for child in &mut self.children {
            unsafe {
                libc::kill(child.id() as i32, libc::SIGTERM);
            }
        }
        let deadline = std::time::Instant::now() + Duration::from_millis(250);
        for mut child in self.children.drain(..) {
            loop {
                if child.try_wait()?.is_some() {
                    break;
                }
                if std::time::Instant::now() >= deadline {
                    let _ = child.kill();
                    child.wait()?;
                    break;
                }
                thread::sleep(Duration::from_millis(5));
            }
        }
        Ok(())
    }
}
impl Drop for Descendants {
    fn drop(&mut self) {
        let _ = self.stop_and_wait();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grv_adapter_wire::Channel;
    use serde_json::json;
    use std::sync::atomic::AtomicU32;
    struct TestAdapter {
        writes: Arc<AtomicU32>,
        pid: Arc<AtomicU32>,
        stopped: Arc<AtomicBool>,
        descendants: Descendants,
        waiting: bool,
        response_size: usize,
    }
    impl Adapter for TestAdapter {
        fn registration(&self) -> Registration {
            Registration {
                name: Name::new("test").unwrap(),
                package_version: "1".into(),
                interface_versions: vec![Req::new(1).unwrap()],
                binding_schema_version: Req::new(1).unwrap(),
                capabilities: Capabilities {
                    after_publish: true,
                    ..Capabilities::default()
                },
                registry: Registry {
                    schema_bundle: json!({"$schema":"https://json-schema.org/draft/2020-12/schema","$defs":{"args":{"type":"object","properties":{},"additionalProperties":false}}}),
                    points: vec![],
                },
                commands: vec![CommandDescriptor {
                    name: Name::new("test").unwrap(),
                    requires_connection: false,
                    requires_authentication: false,
                    args_schema_pointer: "/$defs/args".into(),
                    result_schema_pointer: "/$defs/args".into(),
                }],
            }
        }
        fn prepare_command(&self, _: &Name, argv: &[String]) -> Result<PreparedCommand> {
            if !argv.is_empty() {
                return Err(protocol("unexpected arguments"));
            }
            Ok(PreparedCommand {
                args: json!({}),
                connection: None,
            })
        }
        fn execute_command(&mut self, _: &CommandCall, stop: &StopToken) -> Result<Value> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            if self.waiting {
                let pid = self
                    .descendants
                    .spawn(std::process::Command::new("/bin/sleep").arg("30"))
                    .map_err(|_| error(ErrorCode::AdapterFailure, "helper spawn failed"))?;
                self.pid.store(pid, Ordering::SeqCst);
                while !stop.is_cancelled() {
                    thread::sleep(Duration::from_millis(2));
                }
                stop.check()?;
            }
            Ok(if self.response_size > 0 {
                json!({"message":"x".repeat(self.response_size)})
            } else {
                json!({})
            })
        }
        fn inspect_connection(
            &mut self,
            _handle: Handle,
            request: InspectConnectionRequest,
            stop: &StopToken,
        ) -> Result<Value> {
            stop.check()?;
            Ok(json!({"root":request.root,"declaration":request.declaration}))
        }
        fn after_publish(
            &mut self,
            _: Handle,
            _: AfterPublishRequest,
            stop: &StopToken,
        ) -> Result<()> {
            self.execute_command(
                &CommandCall {
                    command_id: Uuid::v4(),
                    name: Name::new("test").unwrap(),
                    args: json!({}),
                    connection: None,
                },
                stop,
            )
            .map(|_| ())
        }
        fn bind_connection(
            &mut self,
            _: ConnectionLocator,
            _: Option<String>,
            _: Option<String>,
            _: Option<Uuid>,
            _: Mode,
        ) -> Result<BoundConnection> {
            Ok(BoundConnection {
                handle: Handle::new("test-handle").unwrap(),
                identity: Some("test-source".into()),
                workspace_id: None,
                binding: BindingState::NotApplicable,
                details: json!({}),
            })
        }
        fn resolve_pull(
            &mut self,
            _: Handle,
            query: ResolvePullRequest,
            _: &StopToken,
        ) -> Result<PullResolution> {
            Ok(PullResolution {
                state: ResolutionState::NotCommitted,
                request: query.request,
                receipt: None,
                recovery: None,
                prior_source_contracts: vec![],
            })
        }
        fn prepare_pull(
            &mut self,
            _: Handle,
            request: PreparePullRequest,
            _: &StopToken,
        ) -> Result<PullPlan> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            Ok(PullPlan {
                plan_id: Uuid::v4(),
                request: request.request,
                resolved_revision: request.resolved_revision,
                tables: request.tables,
                files: request.files,
                refresh: Refresh::Full,
                recovery_contract: PullRecovery::Transactional,
                adapter_details: json!({}),
            })
        }
        fn apply_pull(&mut self, _: Handle, plan: PullPlan, _: &StopToken) -> Result<Receipt> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            let source_contracts = plan
                .tables
                .iter()
                .map(|t| NamedContract {
                    table: t.name.clone(),
                    contract: t.source_contract.clone(),
                })
                .collect();
            let output_contracts = plan
                .tables
                .iter()
                .map(|t| NamedContract {
                    table: t.name.clone(),
                    contract: t.output_contract.clone(),
                })
                .collect();
            let row_counts = plan
                .tables
                .iter()
                .map(|t| TableCount {
                    table: t.name.clone(),
                    rows: U64::new(0).unwrap(),
                })
                .collect();
            Ok(Receipt {
                request: plan.request,
                committed_revision: plan.resolved_revision,
                generation_id: Uuid::v4(),
                pulled_at: Timestamp::new("2026-10-06T00:00:00Z").unwrap(),
                row_counts,
                source_contracts,
                output_contracts,
                adapter_result: json!({}),
            })
        }
        // Deliberately unadvertised source seam used only to fault the metadata barrier.
        fn extract(
            &mut self,
            _: Handle,
            request: ExtractRequest,
            _: &StopToken,
        ) -> Result<Box<dyn Extraction>> {
            Ok(Box::new(LargeCheckpoint(Some(request))))
        }
        fn stop_and_wait(&mut self) -> Result<()> {
            self.descendants
                .stop_and_wait()
                .map_err(|_| error(ErrorCode::AdapterFailure, "helper did not stop"))?;
            self.stopped.store(true, Ordering::SeqCst);
            Ok(())
        }
    }
    struct LargeCheckpoint(Option<ExtractRequest>);
    impl Extraction for LargeCheckpoint {
        fn next(&mut self, stop: &StopToken) -> Result<Option<ExtractionEvent>> {
            stop.check()?;
            let Some(request) = self.0.take() else {
                return Ok(None);
            };
            Ok(Some(ExtractionEvent::Checkpoint {
                checkpoint_id: Uuid::v4(),
                payload: Checkpoint {
                    attempt_id: request.attempt_id,
                    adapter_identity: request.adapter_identity,
                    connection_identity: request.connection_identity,
                    tables: request
                        .tables
                        .into_iter()
                        .map(|t| CheckpointTable {
                            table: t.name,
                            snapshot_id: "one-recorded-acquisition".into(),
                            reopenable: false,
                            source_identity: json!({"blob":"x".repeat(2*1024*1024)}),
                            capture_start: Timestamp::new("2026-10-06T00:00:00Z").unwrap(),
                        })
                        .collect(),
                    job: json!({}),
                },
            }))
        }
    }
    fn req(v: u64) -> Req {
        Req::new(v).unwrap()
    }
    fn handshake(channel: &mut Channel<UnixStream>) {
        let resources = Resources::default();
        channel
            .send(
                &Frame::Hello {
                    interface_versions: vec![req(1)],
                    core: CoreIdentity {
                        name: "test".into(),
                        version: "1".into(),
                    },
                    attempt: Uuid::v4(),
                    resources: resources.clone(),
                },
                &[],
            )
            .unwrap();
        assert!(matches!(
            channel.receive().unwrap().frame,
            Frame::Identified { .. }
        ));
        channel
            .send(
                &Frame::Ready {
                    interface_version: req(1),
                    binding_schema_version: req(1),
                    resources,
                },
                &[],
            )
            .unwrap();
    }
    fn prepare(channel: &mut Channel<UnixStream>) -> Uuid {
        channel
            .send(
                &Frame::PrepareCommand {
                    req: req(1),
                    name: Name::new("test").unwrap(),
                    argv: Doc::inline(vec![]),
                },
                &[],
            )
            .unwrap();
        let Frame::CommandPrepared { call, .. } = channel.receive().unwrap().frame else {
            panic!("prepared result")
        };
        inline(call).unwrap().command_id
    }
    type Harness = (
        Channel<UnixStream>,
        thread::JoinHandle<std::result::Result<(), SdkError>>,
        Arc<AtomicU32>,
        Arc<AtomicU32>,
        Arc<AtomicBool>,
    );
    fn setup_with_response(waiting: bool, response_size: usize) -> Harness {
        let (a, b) = UnixStream::pair().unwrap();
        a.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let writes = Arc::new(AtomicU32::new(0));
        let pid = Arc::new(AtomicU32::new(0));
        let stopped = Arc::new(AtomicBool::new(false));
        let adapter = TestAdapter {
            writes: writes.clone(),
            pid: pid.clone(),
            stopped: stopped.clone(),
            descendants: Descendants::default(),
            waiting,
            response_size,
        };
        let server = thread::spawn(move || serve(adapter, b));
        let mut channel = Channel::new(a, Role::Parent, 8 * 1024 * 1024);
        handshake(&mut channel);
        (channel, server, writes, pid, stopped)
    }
    fn setup(waiting: bool) -> Harness {
        setup_with_response(waiting, 0)
    }
    fn bind_hook(channel: &mut Channel<UnixStream>) {
        channel
            .send(
                &Frame::BindConnection {
                    req: req(1),
                    locator: Doc::inline(ConnectionLocator {
                        canonical_connection: json!({}),
                        identity: None,
                        engine_path: None,
                        session_lock_path: None,
                    }),
                    root: None,
                    expected_identity: None,
                    expected_workspace_id: None,
                    mode: Mode::Extract,
                },
                &[],
            )
            .unwrap();
        assert!(matches!(
            channel.receive().unwrap().frame,
            Frame::BindResult { .. }
        ));
    }
    fn hook_frame(id: u64, attempt: Uuid) -> Frame {
        Frame::AfterPublish {
            req: req(id),
            handle: Handle::new("test-handle").unwrap(),
            attempt_id: attempt,
            declaration_sha256: grv_types::sha256(b"fixed"),
            outcome: Outcome {
                kind: OutcomeKind::NoOp,
                revision: Some(U64::new(0).unwrap()),
                operation_id: None,
            },
        }
    }
    #[test]
    fn after_publish_rebind_replay_needs_no_extraction_and_rejects_changed_fixed_hook() {
        let (mut channel, server, writes, _, _) = setup(false);
        bind_hook(&mut channel);
        let attempt = Uuid::v4();
        for id in [2, 3] {
            channel.send(&hook_frame(id, attempt.clone()), &[]).unwrap();
            assert!(matches!(
                channel.receive().unwrap().frame,
                Frame::AfterPublishResult {
                    acknowledged: true,
                    ..
                }
            ));
        }
        let mut changed = hook_frame(4, attempt);
        if let Frame::AfterPublish {
            declaration_sha256, ..
        } = &mut changed
        {
            *declaration_sha256 = grv_types::sha256(b"changed");
        }
        channel.send(&changed, &[]).unwrap();
        assert!(matches!(
            channel.receive().unwrap().frame,
            Frame::Error {
                code: ErrorCode::RequestMismatch,
                ..
            }
        ));
        assert_eq!(writes.load(Ordering::SeqCst), 2);
        channel.send(&Frame::Close { req: req(5) }, &[]).unwrap();
        assert!(matches!(
            channel.receive().unwrap().frame,
            Frame::CloseResult { .. }
        ));
        server.join().unwrap().unwrap();
    }
    #[test]
    fn after_publish_cancel_stops_descendants_before_acknowledgement() {
        let (mut channel, server, writes, pid, stopped) = setup(true);
        bind_hook(&mut channel);
        channel.send(&hook_frame(2, Uuid::v4()), &[]).unwrap();
        while pid.load(Ordering::SeqCst) == 0 {
            thread::sleep(Duration::from_millis(2));
        }
        channel.send(&Frame::Cancel { req: req(2) }, &[]).unwrap();
        assert!(matches!(
            channel.receive().unwrap().frame,
            Frame::CancelAck {
                state: CancelState::Stopped,
                ..
            }
        ));
        assert!(stopped.load(Ordering::SeqCst));
        assert_eq!(writes.load(Ordering::SeqCst), 1);
        assert_eq!(
            unsafe { libc::kill(pid.load(Ordering::SeqCst) as i32, 0) },
            -1
        );
        channel.send(&Frame::Close { req: req(3) }, &[]).unwrap();
        assert!(matches!(
            channel.receive().unwrap().frame,
            Frame::CloseResult { .. }
        ));
        server.join().unwrap().unwrap();
    }
    #[test]
    fn after_publish_eof_stops_descendants_without_acknowledgement() {
        let (mut channel, server, _, pid, stopped) = setup(true);
        bind_hook(&mut channel);
        channel.send(&hook_frame(2, Uuid::v4()), &[]).unwrap();
        while pid.load(Ordering::SeqCst) == 0 {
            thread::sleep(Duration::from_millis(2));
        }
        drop(channel);
        assert!(server.join().unwrap().is_err());
        assert!(stopped.load(Ordering::SeqCst));
        assert_eq!(
            unsafe { libc::kill(pid.load(Ordering::SeqCst) as i32, 0) },
            -1
        );
    }
    #[test]
    fn after_publish_known_acknowledgement_wins_late_cancel() {
        let (mut channel, server, writes, _, _) = setup(false);
        bind_hook(&mut channel);
        channel.send(&hook_frame(2, Uuid::v4()), &[]).unwrap();
        assert!(matches!(
            channel.receive().unwrap().frame,
            Frame::AfterPublishResult {
                acknowledged: true,
                ..
            }
        ));
        channel.send(&Frame::Cancel { req: req(2) }, &[]).unwrap();
        assert!(matches!(
            channel.receive().unwrap().frame,
            Frame::CancelAck {
                state: CancelState::Completed,
                ..
            }
        ));
        assert_eq!(writes.load(Ordering::SeqCst), 1);
        channel.send(&Frame::Close { req: req(3) }, &[]).unwrap();
        assert!(matches!(
            channel.receive().unwrap().frame,
            Frame::CloseResult { .. }
        ));
        server.join().unwrap().unwrap();
    }
    fn wait_for_child(pid: &AtomicU32) -> u32 {
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while pid.load(Ordering::SeqCst) == 0 {
            assert!(std::time::Instant::now() < deadline);
            thread::sleep(Duration::from_millis(2));
        }
        pid.load(Ordering::SeqCst)
    }
    #[test]
    fn command_executes_once_and_clean_close_stops_work() {
        let (mut channel, server, writes, _, stopped) = setup(false);
        let command_id = prepare(&mut channel);
        channel
            .send(
                &Frame::Command {
                    req: req(2),
                    command_id,
                    handle: None,
                },
                &[],
            )
            .unwrap();
        assert!(matches!(
            channel.receive().unwrap().frame,
            Frame::CommandResult { .. }
        ));
        channel.send(&Frame::Close { req: req(3) }, &[]).unwrap();
        assert!(matches!(
            channel.receive().unwrap().frame,
            Frame::CloseResult { .. }
        ));
        server.join().unwrap().unwrap();
        assert_eq!(writes.load(Ordering::SeqCst), 1);
        assert!(stopped.load(Ordering::SeqCst));
    }
    #[test]
    fn prepared_command_cannot_execute_twice() {
        let (mut channel, server, writes, _, _) = setup(false);
        let command_id = prepare(&mut channel);
        channel
            .send(
                &Frame::Command {
                    req: req(2),
                    command_id: command_id.clone(),
                    handle: None,
                },
                &[],
            )
            .unwrap();
        assert!(matches!(
            channel.receive().unwrap().frame,
            Frame::CommandResult { .. }
        ));
        channel
            .send(
                &Frame::Command {
                    req: req(3),
                    command_id,
                    handle: None,
                },
                &[],
            )
            .unwrap();
        assert!(matches!(
            channel.receive().unwrap().frame,
            Frame::Error {
                code: ErrorCode::ProtocolFailure,
                ..
            }
        ));
        channel.send(&Frame::Close { req: req(4) }, &[]).unwrap();
        assert!(matches!(
            channel.receive().unwrap().frame,
            Frame::CloseResult { .. }
        ));
        server.join().unwrap().unwrap();
        assert_eq!(writes.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn cancellation_ack_waits_for_child_exit() {
        let (mut channel, server, _, pid, stopped) = setup(true);
        let command_id = prepare(&mut channel);
        channel
            .send(
                &Frame::Command {
                    req: req(2),
                    command_id,
                    handle: None,
                },
                &[],
            )
            .unwrap();
        let pid = wait_for_child(&pid);
        channel.send(&Frame::Cancel { req: req(2) }, &[]).unwrap();
        assert!(matches!(
            channel.receive().unwrap().frame,
            Frame::CancelAck {
                state: CancelState::Stopped,
                ..
            }
        ));
        assert!(stopped.load(Ordering::SeqCst));
        assert_eq!(unsafe { libc::kill(pid as i32, 0) }, -1);
        channel.send(&Frame::Cancel { req: req(2) }, &[]).unwrap();
        assert!(matches!(
            channel.receive().unwrap().frame,
            Frame::CancelAck {
                state: CancelState::Stopped,
                ..
            }
        ));
        channel.send(&Frame::Close { req: req(3) }, &[]).unwrap();
        assert!(matches!(
            channel.receive().unwrap().frame,
            Frame::CloseResult { .. }
        ));
        server.join().unwrap().unwrap();
    }
    #[test]
    fn eof_waits_for_child_exit() {
        let (mut channel, server, _, pid, stopped) = setup(true);
        let command_id = prepare(&mut channel);
        channel
            .send(
                &Frame::Command {
                    req: req(2),
                    command_id,
                    handle: None,
                },
                &[],
            )
            .unwrap();
        let pid = wait_for_child(&pid);
        drop(channel);
        assert!(server.join().unwrap().is_err());
        assert!(stopped.load(Ordering::SeqCst));
        assert_eq!(unsafe { libc::kill(pid as i32, 0) }, -1);
    }
    #[test]
    fn large_known_command_result_survives_cancel_during_document_upload() {
        let (mut channel, server, _, _, _) = setup_with_response(false, 1024 * 1024 + 128);
        let command_id = prepare(&mut channel);
        channel
            .send(
                &Frame::Command {
                    req: req(2),
                    command_id,
                    handle: None,
                },
                &[],
            )
            .unwrap();
        assert!(matches!(
            channel.receive().unwrap().frame,
            Frame::DocumentBegin { .. }
        ));
        channel.send(&Frame::Cancel { req: req(2) }, &[]).unwrap();
        loop {
            match channel.receive().unwrap().frame {
                Frame::DocumentChunk { .. } => {}
                Frame::DocumentEnd { document_id } => channel
                    .send(&Frame::DocumentAck { document_id }, &[])
                    .unwrap(),
                Frame::CommandResult { details, .. } => {
                    let details = inline(details).unwrap();
                    assert_eq!(
                        details["message"].as_str().unwrap().len(),
                        1024 * 1024 + 128
                    );
                    break;
                }
                other => panic!("unexpected frame {other:?}"),
            }
        }
        assert!(matches!(
            channel.receive().unwrap().frame,
            Frame::CancelAck {
                state: CancelState::Completed,
                ..
            }
        ));
        channel.send(&Frame::Cancel { req: req(2) }, &[]).unwrap();
        assert!(matches!(
            channel.receive().unwrap().frame,
            Frame::CancelAck {
                state: CancelState::Completed,
                ..
            }
        ));
        channel.send(&Frame::Close { req: req(3) }, &[]).unwrap();
        assert!(matches!(
            channel.receive().unwrap().frame,
            Frame::CloseResult { .. }
        ));
        server.join().unwrap().unwrap();
    }
    #[test]
    fn inspection_dispatch_requires_bound_handle_and_does_not_authenticate_or_execute_commands() {
        for bound in [false, true] {
            let (mut channel, server, writes, _, stopped) = setup(false);
            if bound {
                channel
                    .send(
                        &Frame::BindConnection {
                            req: req(1),
                            locator: Doc::inline(ConnectionLocator {
                                canonical_connection: json!({}),
                                identity: Some("test-source".into()),
                                engine_path: None,
                                session_lock_path: None,
                            }),
                            root: None,
                            expected_identity: None,
                            expected_workspace_id: None,
                            mode: Mode::Inspect,
                        },
                        &[],
                    )
                    .unwrap();
                assert!(matches!(
                    channel.receive().unwrap().frame,
                    Frame::BindResult { .. }
                ));
            }
            channel
                .send(
                    &Frame::InspectConnection {
                        req: req(2),
                        handle: Handle::new("test-handle").unwrap(),
                        root: None,
                        declaration: Some(Doc::inline(json!({"dataset":"data"}))),
                    },
                    &[],
                )
                .unwrap();
            match channel.receive().unwrap().frame {
                Frame::InspectResult {
                    details: Doc::Inline(value),
                    ..
                } => {
                    assert!(bound);
                    assert_eq!(
                        value.inline,
                        json!({"root":null,"declaration":{"dataset":"data"}})
                    );
                }
                Frame::Error {
                    code: ErrorCode::ProtocolFailure,
                    ..
                } => assert!(!bound),
                frame => panic!("unexpected inspection response {frame:?}"),
            }
            assert_eq!(writes.load(Ordering::SeqCst), 0);
            channel.send(&Frame::Close { req: req(3) }, &[]).unwrap();
            assert!(matches!(
                channel.receive().unwrap().frame,
                Frame::CloseResult { .. }
            ));
            server.join().unwrap().unwrap();
            assert!(stopped.load(Ordering::SeqCst));
        }
    }
    #[test]
    fn cancellation_during_checkpoint_upload_stops_without_checkpoint_or_rows() {
        let (mut channel, server, _, _, stopped) = setup(false);
        channel
            .send(
                &Frame::BindConnection {
                    req: req(1),
                    locator: Doc::inline(ConnectionLocator {
                        canonical_connection: json!({}),
                        identity: Some("test-source".into()),
                        engine_path: None,
                        session_lock_path: None,
                    }),
                    root: None,
                    expected_identity: None,
                    expected_workspace_id: None,
                    mode: Mode::Extract,
                },
                &[],
            )
            .unwrap();
        assert!(matches!(
            channel.receive().unwrap().frame,
            Frame::BindResult { .. }
        ));
        channel
            .send(
                &Frame::Extract {
                    req: req(2),
                    handle: Handle::new("test-handle").unwrap(),
                    payload: Doc::inline(ExtractRequest {
                        attempt_id: Uuid::v4(),
                        stream_id: Uuid::v4(),
                        root: "/fixture".into(),
                        dataset: Name::new("test").unwrap(),
                        run_id: RunId::new("0".repeat(26)).unwrap(),
                        declaration_sha256: Digest::new("0".repeat(64)).unwrap(),
                        adapter_identity: AdapterIdentity {
                            name: Name::new("test").unwrap(),
                            package_version: "1".into(),
                            interface_version: req(1),
                            binding_schema_version: req(1),
                        },
                        connection_identity: "test-source".into(),
                        selection: ExtractSelection {
                            policy: SelectionPolicy::Changed,
                        },
                        options: json!({}),
                        tables: vec![ExtractTable {
                            name: Name::new("table").unwrap(),
                            source: json!({}),
                            columns: json!({}),
                            contract: TableContract {
                                columns: vec![Column {
                                    name: "id".into(),
                                    logical_type: json!("int64"),
                                }],
                                partition_keys: vec![],
                                extensions: json!({}),
                                column_ext: json!({}),
                            },
                        }],
                        resume: None,
                    }),
                },
                &[],
            )
            .unwrap();
        assert!(matches!(
            channel.receive().unwrap().frame,
            Frame::ExtractStarted { .. }
        ));
        assert!(matches!(
            channel.receive().unwrap().frame,
            Frame::DocumentBegin { .. }
        ));
        channel.send(&Frame::Cancel { req: req(2) }, &[]).unwrap();
        loop {
            match channel.receive().unwrap().frame {
                Frame::DocumentChunk { .. } | Frame::DocumentEnd { .. } => {}
                Frame::CancelAck {
                    state: CancelState::Stopped,
                    ..
                } => break,
                other => panic!("checkpoint or rows escaped cancelled metadata: {other:?}"),
            }
        }
        assert!(stopped.load(Ordering::SeqCst));
        channel.send(&Frame::Close { req: req(3) }, &[]).unwrap();
        assert!(matches!(
            channel.receive().unwrap().frame,
            Frame::CloseResult { .. }
        ));
        server.join().unwrap().unwrap();
    }
    #[test]
    fn malformed_document_hash_utf8_and_duplicate_keys_close_before_ack() {
        use base64::{Engine, engine::general_purpose::STANDARD};
        for (bytes, bad_hash) in [
            (b"{}".to_vec(), true),
            (vec![0xff], false),
            (br#"{"x":1,"x":2}"#.to_vec(), false),
        ] {
            let (mut channel, server, _, _, stopped) = setup(false);
            let id = Uuid::v4();
            let sha256 = if bad_hash {
                Digest::new("0".repeat(64)).unwrap()
            } else {
                grv_types::sha256(&bytes)
            };
            channel
                .codec
                .write(
                    &Frame::DocumentBegin {
                        document_id: id.clone(),
                        size: U64::new(bytes.len() as u64).unwrap(),
                        sha256,
                    },
                    &[],
                )
                .unwrap();
            channel
                .codec
                .write(
                    &Frame::DocumentChunk {
                        document_id: id.clone(),
                        index: SafeInt::new(0).unwrap(),
                        data: STANDARD.encode(bytes),
                    },
                    &[],
                )
                .unwrap();
            channel
                .codec
                .write(&Frame::DocumentEnd { document_id: id }, &[])
                .unwrap();
            assert!(channel.codec.read().is_err());
            assert!(server.join().unwrap().is_err());
            assert!(stopped.load(Ordering::SeqCst));
        }
    }
    #[test]
    fn cancellation_wakes_exhausted_credit_wait() {
        let flow = Flow::new(1);
        let stop = StopToken {
            inner: flow.clone(),
        };
        flow.slot(&Name::new("table").unwrap()).unwrap();
        let joined = thread::spawn(move || flow.wait_credit());
        stop.cancel();
        assert!(!joined.join().unwrap());
    }
    fn pull_request() -> RequestRecord {
        let workspace_id = Uuid::v4();
        let declaration_sha256 = grv_types::sha256(b"authored-declaration");
        let requested_revision = grv_types::RequestedRevision::Revision(U64::new(0).unwrap());
        let request_sha256 = grv_types::pull_request_digest(&grv_types::PullRequestIdentity {
            root: "/fixture".into(),
            workspace_id: workspace_id.clone(),
            declaration_sha256: declaration_sha256.clone(),
            requested_revision: requested_revision.clone(),
        })
        .unwrap();
        RequestRecord {
            attempt_id: Uuid::v4(),
            root: "/fixture".into(),
            dataset: Name::new("test").unwrap(),
            workspace_id,
            adapter_identity: AdapterIdentity {
                name: Name::new("test").unwrap(),
                package_version: "1".into(),
                interface_version: req(1),
                binding_schema_version: req(1),
            },
            connection_identity: "test-source".into(),
            effective_declaration: json!({"kind":"pull","dataset":"test","adapter":"test"}),
            validation_input: json!({"kind":"pull","dataset":"test","adapter":"test"}),
            declaration_sha256,
            request_sha256,
            registry: Registry {
                schema_bundle: json!({"$schema":"https://json-schema.org/draft/2020-12/schema"}),
                points: vec![],
            },
            requested_revision,
        }
    }
    #[test]
    fn pull_requires_lookup_comparison_and_exact_single_use_plan() {
        // Every protocol error ends ordinary work; each adversary gets a fresh channel.
        for scenario in [
            "skip_lookup",
            "skip_compare",
            "change_recovery",
            "change_plan",
            "reuse_plan",
            "valid",
        ] {
            let (mut channel, server, writes, _, _) = setup(false);
            let request = pull_request();
            let handle = Handle::new("test-handle").unwrap();
            let mut id = 1;
            let mut exchange = |frame: Frame| {
                channel.send(&frame, &[]).unwrap();
                channel.receive().unwrap().frame
            };
            assert!(matches!(
                exchange(Frame::BindConnection {
                    req: req(id),
                    locator: Doc::inline(ConnectionLocator {
                        canonical_connection: json!({}),
                        identity: Some("test-source".into()),
                        engine_path: None,
                        session_lock_path: None
                    }),
                    root: Some(request.root.clone()),
                    expected_identity: Some(request.connection_identity.clone()),
                    expected_workspace_id: None,
                    mode: Mode::Pull
                }),
                Frame::BindResult { .. }
            ));
            id += 1;
            let compare = ResolvePullRequest {
                phase: ResolvePhase::Compare,
                attempt_id: request.attempt_id.clone(),
                root: request.root.clone(),
                request: Some(request.clone()),
            };
            let mut rejected = false;
            let mut expected_writes = 0;
            if scenario == "skip_lookup" {
                rejected = matches!(
                    exchange(Frame::ResolvePull {
                        req: req(id),
                        handle: handle.clone(),
                        payload: Doc::inline(compare.clone())
                    }),
                    Frame::Error {
                        code: ErrorCode::ProtocolFailure,
                        ..
                    }
                );
                id += 1;
            } else {
                assert!(matches!(
                    exchange(Frame::ResolvePull {
                        req: req(id),
                        handle: handle.clone(),
                        payload: Doc::inline(ResolvePullRequest {
                            phase: ResolvePhase::Lookup,
                            attempt_id: request.attempt_id.clone(),
                            root: request.root.clone(),
                            request: None
                        })
                    }),
                    Frame::ResolveResult { .. }
                ));
                id += 1;
                if scenario != "skip_compare" {
                    assert!(matches!(
                        exchange(Frame::ResolvePull {
                            req: req(id),
                            handle: handle.clone(),
                            payload: Doc::inline(compare)
                        }),
                        Frame::ResolveResult { .. }
                    ));
                    id += 1;
                }
                let prepare = PreparePullRequest {
                    request: request.clone(),
                    resolved_revision: U64::new(0).unwrap(),
                    tables: vec![],
                    files: vec![],
                    recovery: (scenario == "change_recovery").then(|| json!({"invented":"repair"})),
                };
                let response = exchange(Frame::PreparePull {
                    req: req(id),
                    handle: handle.clone(),
                    payload: Doc::inline(prepare),
                });
                id += 1;
                if ["skip_compare", "change_recovery"].contains(&scenario) {
                    rejected = matches!(
                        response,
                        Frame::Error {
                            code: ErrorCode::ProtocolFailure,
                            ..
                        }
                    );
                } else {
                    let Frame::PrepareResult {
                        plan: Doc::Inline(plan),
                        ..
                    } = response
                    else {
                        panic!("plan missing")
                    };
                    expected_writes = 1;
                    let mut apply = plan.inline.clone();
                    if scenario == "change_plan" {
                        apply.plan_id = Uuid::v4();
                    }
                    let response = exchange(Frame::ApplyPull {
                        req: req(id),
                        handle: handle.clone(),
                        plan: Doc::inline(apply),
                    });
                    id += 1;
                    if scenario == "change_plan" {
                        rejected = matches!(
                            response,
                            Frame::Error {
                                code: ErrorCode::ProtocolFailure,
                                ..
                            }
                        );
                    } else {
                        let Frame::ApplyResult {
                            receipt: Doc::Inline(receipt),
                            ..
                        } = response
                        else {
                            panic!("receipt missing")
                        };
                        assert_eq!(receipt.inline.committed_revision.get(), 0);
                        expected_writes = 2;
                        if scenario == "reuse_plan" {
                            rejected = matches!(
                                exchange(Frame::ApplyPull {
                                    req: req(id),
                                    handle: handle.clone(),
                                    plan: Doc::inline(plan.inline)
                                }),
                                Frame::Error {
                                    code: ErrorCode::ProtocolFailure,
                                    ..
                                }
                            );
                            id += 1;
                        }
                    }
                }
            }
            assert_eq!(rejected, scenario != "valid", "{scenario}");
            assert_eq!(writes.load(Ordering::SeqCst), expected_writes, "{scenario}");
            assert!(matches!(
                exchange(Frame::Close { req: req(id) }),
                Frame::CloseResult { .. }
            ));
            server.join().unwrap().unwrap();
        }
    }
}
