//! Private SDK extraction lifecycle. Capability registration stays gated until
//! whole-source acquisition and recovery conformance pass.
use crate::{
    acquisition::AcquisitionWorker,
    binding,
    journal::{AcquisitionIntent, AcquisitionJournal},
    native::{NativeInterrupt, SourceSelection},
    worker::Interrupt,
};
use grv_adapter_api::{
    BindingState, CaptureWindow, Checkpoint, CheckpointTable, ConnectionLocator, ExtractRequest,
    Handle, Mode, Resources, SourceCompletion,
};
use grv_adapter_sdk::{BoundConnection, Extraction, ExtractionEvent, StopToken};
use grv_types::{ErrorCode, PublicError, Timestamp, U64, Uuid};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::{self, DirBuilder, File},
    io,
    os::unix::fs::{DirBuilderExt, MetadataExt},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

type Result<T> = grv_adapter_sdk::Result<T>;
#[allow(clippy::result_large_err)]
pub fn error(code: ErrorCode, message: impl Into<String>) -> PublicError {
    PublicError {
        code,
        message: message.into(),
        retryable: false,
        object: None,
    }
}
fn failure(error: io::Error) -> PublicError {
    let code = if error.kind() == io::ErrorKind::WouldBlock {
        ErrorCode::EngineBusy
    } else if error.kind() == io::ErrorKind::AlreadyExists {
        ErrorCode::ExtractionIncomplete
    } else if error.kind() == io::ErrorKind::InvalidInput {
        ErrorCode::InvalidDeclaration
    } else {
        ErrorCode::ExtractionIncomplete
    };
    self::error(code, error.to_string())
}
fn now() -> Timestamp {
    Timestamp::new(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)).unwrap()
}
struct BoundSource {
    path: PathBuf,
    identity: String,
    worker: Arc<Mutex<AcquisitionWorker>>,
    interrupt: NativeInterrupt,
}
#[derive(Default)]
pub struct Runtime {
    connections: BTreeMap<Handle, BoundSource>,
    next_handle: u64,
}
#[allow(clippy::result_large_err)]
impl Runtime {
    pub fn bind(
        &mut self,
        locator: ConnectionLocator,
        expected: Option<String>,
        workspace: Option<Uuid>,
        mode: Mode,
        root: Option<String>,
    ) -> Result<BoundConnection> {
        if mode != Mode::Extract || workspace.is_some() {
            return Err(error(
                ErrorCode::UnsupportedCapability,
                "DuckDB binding supports unmanaged extraction only",
            ));
        }
        let relocated = binding::locate(locator.canonical_connection.clone()).map_err(failure)?;
        if relocated != locator
            || expected
                .as_ref()
                .is_some_and(|value| Some(value) != locator.identity.as_ref())
        {
            return Err(error(
                ErrorCode::RequestMismatch,
                "connection locator or identity changed",
            ));
        }
        let path = PathBuf::from(locator.engine_path.as_ref().unwrap());
        if let Some(root) = root {
            outside_root(&path, &root).map_err(failure)?;
        }
        let worker = AcquisitionWorker::open_readonly(&path).map_err(failure)?;
        let interrupt = worker.interrupt_handle();
        self.next_handle += 1;
        let handle = Handle::new(format!("duckdb-{}", self.next_handle))
            .map_err(|e| error(ErrorCode::AdapterFailure, e.to_string()))?;
        let identity = locator.identity.unwrap();
        self.connections.insert(
            handle.clone(),
            BoundSource {
                path,
                identity: identity.clone(),
                worker: Arc::new(Mutex::new(worker)),
                interrupt,
            },
        );
        Ok(BoundConnection {
            handle,
            identity: Some(identity),
            workspace_id: None,
            binding: BindingState::NotApplicable,
            details: json!({"database":locator.canonical_connection["database"],"workspace_id":null,"binding":"not_applicable"}),
        })
    }
    pub fn authenticate(&self, handle: Handle, expected: Option<String>) -> Result<String> {
        let source = self
            .connections
            .get(&handle)
            .ok_or_else(|| error(ErrorCode::ProtocolFailure, "unknown DuckDB handle"))?;
        if expected
            .as_ref()
            .is_some_and(|value| value != &source.identity)
        {
            return Err(error(
                ErrorCode::RequestMismatch,
                "connection identity changed",
            ));
        }
        Ok(source.identity.clone())
    }
    pub fn extract(
        &mut self,
        handle: Handle,
        request: ExtractRequest,
        resources: &Resources,
        stop: &StopToken,
    ) -> Result<Box<dyn Extraction>> {
        stop.check()?;
        binding::extraction_options(&request.options).map_err(failure)?;
        if request.resume.is_some() {
            return Err(error(
                ErrorCode::ExtractionIncomplete,
                "nonresumable DuckDB snapshot cannot be reacquired",
            ));
        }
        let source = self
            .connections
            .get(&handle)
            .ok_or_else(|| error(ErrorCode::ProtocolFailure, "unknown DuckDB handle"))?;
        if request.connection_identity != source.identity
            || request.adapter_identity.name.as_str() != "duckdb"
            || request.adapter_identity.package_version != env!("CARGO_PKG_VERSION")
            || request.tables.is_empty()
        {
            return Err(error(
                ErrorCode::RequestMismatch,
                "extraction request identity/selection mismatch",
            ));
        }
        let sources = request
            .tables
            .iter()
            .map(|table| {
                let (schema, relation, filter) = binding::relation(&table.source)?;
                Ok(SourceSelection {
                    schema,
                    table: relation,
                    columns: binding::source_columns(table)?,
                    filter,
                })
            })
            .collect::<io::Result<Vec<_>>>()
            .map_err(failure)?;
        let schemas = request
            .tables
            .iter()
            .map(|table| binding::output_schema(&table.contract))
            .collect::<io::Result<Vec<_>>>()
            .map_err(failure)?;
        let journal_path =
            journal_directory(&source.path, &request.root, &request.attempt_id).map_err(failure)?;
        let start = now();
        let intent = AcquisitionIntent {
            attempt_id: request.attempt_id.clone(),
            request_sha256: grv_types::sha256(
                &grv_types::canonical_json(&request)
                    .map_err(|e| error(ErrorCode::ProtocolFailure, e.to_string()))?,
            ),
            adapter_identity: request.adapter_identity.clone(),
            connection_identity: request.connection_identity.clone(),
            snapshot_id: Uuid::v4(),
            capture_start: start.clone(),
            reopenable: false,
        };
        let journal = Arc::new(AcquisitionJournal::start(&journal_path, intent).map_err(failure)?);
        let members = source
            .worker
            .lock()
            .unwrap()
            .acquire(sources, schemas, journal.clone(), stop)
            .map_err(failure)?;
        let end = now();
        let identities = request
            .tables
            .iter()
            .map(|table| json!({"relation":table.source["table"],"database":source.path}))
            .collect::<Vec<_>>();
        let checkpoint = Checkpoint {
            attempt_id: request.attempt_id.clone(),
            adapter_identity: request.adapter_identity.clone(),
            connection_identity: request.connection_identity.clone(),
            tables: request
                .tables
                .iter()
                .zip(&identities)
                .map(|(table, identity)| CheckpointTable {
                    table: table.name.clone(),
                    snapshot_id: journal.intent().snapshot_id.as_str().into(),
                    reopenable: false,
                    source_identity: identity.clone(),
                    capture_start: start.clone(),
                })
                .collect(),
            job: json!({}),
        };
        journal.checkpoint_ready(&checkpoint).map_err(failure)?;
        Ok(Box::new(Stream {
            worker: source.worker.clone(),
            resources: resources.clone(),
            request,
            identities,
            counts: members.iter().map(|member| member.rows).collect(),
            emitted: 0,
            table: 0,
            checkpoint: Some(checkpoint),
            start,
            end: Some(end),
            finished: false,
            _journal: journal,
        }))
    }
    pub fn stop(&mut self) -> Result<()> {
        for source in self.connections.values() {
            source.interrupt.interrupt();
        }
        for source in self.connections.values() {
            source.worker.lock().unwrap().stop().map_err(failure)?;
        }
        self.connections.clear();
        Ok(())
    }
}
fn private_directory(path: &Path) -> io::Result<()> {
    match DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => {
            File::open(path.parent().unwrap())?.sync_all()?;
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o077 != 0
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "acquisition directory is not owner-private",
        ));
    }
    Ok(())
}
fn journal_directory(database: &Path, root: &str, attempt: &Uuid) -> io::Result<PathBuf> {
    let mut name = database.as_os_str().to_os_string();
    name.push(".grv-acquisitions");
    let parent = PathBuf::from(name);
    outside_root(&parent, root)?;
    crate::lock::check_ancestors(&parent)?;
    private_directory(&parent)?;
    let path = parent.join(attempt.as_str());
    // Existing directory is acquisition evidence even if intent writing failed.
    DirBuilder::new().mode(0o700).create(&path)?;
    File::open(&parent)?.sync_all()?;
    Ok(path)
}
pub(crate) fn outside_root(path: &Path, root: &str) -> io::Result<()> {
    let local = if let Some(uri) = root.strip_prefix("file://") {
        let uri = uri.strip_prefix("localhost").unwrap_or(uri);
        if !uri.starts_with('/') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "local root URI must have no remote host",
            ));
        }
        let mut bytes = Vec::with_capacity(uri.len());
        let mut input = uri.as_bytes().iter().copied();
        while let Some(byte) = input.next() {
            if byte == b'%' {
                let high = input.next().and_then(|value| (value as char).to_digit(16));
                let low = input.next().and_then(|value| (value as char).to_digit(16));
                bytes.push(match (high, low) {
                    (Some(high), Some(low)) => (high * 16 + low) as u8,
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "invalid local root URI escape",
                        ));
                    }
                });
            } else {
                bytes.push(byte);
            }
        }
        String::from_utf8(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?
    } else {
        root.to_owned()
    };
    if local.contains('\0') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "local root cannot contain NUL",
        ));
    }
    if let Ok(root) = fs::canonicalize(local)
        && path.starts_with(root)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "native engine ownership and acquisition evidence must live outside GRV",
        ));
    }
    Ok(())
}
struct Stream {
    worker: Arc<Mutex<AcquisitionWorker>>,
    resources: Resources,
    request: ExtractRequest,
    identities: Vec<Value>,
    counts: Vec<u64>,
    emitted: u64,
    table: usize,
    checkpoint: Option<Checkpoint>,
    start: Timestamp,
    end: Option<Timestamp>,
    finished: bool,
    _journal: Arc<AcquisitionJournal>,
}
#[allow(clippy::result_large_err)]
impl Extraction for Stream {
    fn next(&mut self, stop: &StopToken) -> Result<Option<ExtractionEvent>> {
        stop.check()?;
        if let Some(payload) = self.checkpoint.take() {
            return Ok(Some(ExtractionEvent::Checkpoint {
                checkpoint_id: Uuid::v4(),
                payload,
            }));
        }
        if self.table < self.request.tables.len() {
            if let Some((payload, rows)) = self
                .worker
                .lock()
                .unwrap()
                .fetch(self.table, &self.resources, stop)
                .map_err(failure)?
            {
                self.emitted += rows;
                return Ok(Some(ExtractionEvent::Batch {
                    table: self.request.tables[self.table].name.clone(),
                    payload,
                    rows: U64::new(rows).unwrap(),
                }));
            }
            if self.emitted != self.counts[self.table] {
                return Err(error(
                    ErrorCode::IntegrityFailure,
                    "native acquisition count differs from emitted rows",
                ));
            }
            let table = self.table;
            self.table += 1;
            self.emitted = 0;
            let end = self.end.get_or_insert_with(now).clone();
            return Ok(Some(ExtractionEvent::TableComplete {
                table: self.request.tables[table].name.clone(),
                row_count: U64::new(self.counts[table])
                    .map_err(|e| error(ErrorCode::ExtractionIncomplete, e.to_string()))?,
                source_identity: self.identities[table].clone(),
                capture: CaptureWindow {
                    start: self.start.clone(),
                    end,
                },
            }));
        }
        if !self.finished {
            self.worker.lock().unwrap().stop().map_err(failure)?;
            self.finished = true;
            return Ok(Some(ExtractionEvent::SourceComplete(SourceCompletion {
                job: json!({}),
                capture_window: CaptureWindow {
                    start: self.start.clone(),
                    end: self.end.get_or_insert_with(now).clone(),
                },
                adapter_result: json!({}),
            })));
        }
        Ok(None)
    }
}
impl Drop for Stream {
    fn drop(&mut self) {
        let _ = self.worker.lock().unwrap().stop();
    }
}
