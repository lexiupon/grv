//! Typed process build hooks. Build capabilities remain disabled until native
//! and parent capture/publication/recovery conformance have all passed.
use crate::{
    binding,
    build::{BuildError, BuildStore},
    build_worker::BuildWorker,
    extraction::error,
    worker::Interrupt,
};
use grv_adapter_api::*;
use grv_adapter_sdk::{BoundConnection, BuildExport, BuildExportEvent, StopToken};
use std::{
    collections::BTreeMap,
    io,
    path::PathBuf,
    sync::{Arc, Mutex},
};
type Result<T> = grv_adapter_sdk::Result<T>;
#[allow(clippy::result_large_err)]
fn failure(failure: BuildError) -> grv_types::PublicError {
    let (code, message) = match failure {
        BuildError::Engine(e) => (
            if e.kind() == io::ErrorKind::WouldBlock {
                ErrorCode::EngineBusy
            } else if e.kind() == io::ErrorKind::InvalidInput {
                ErrorCode::InvalidDeclaration
            } else {
                ErrorCode::AdapterFailure
            },
            e.to_string(),
        ),
        BuildError::RequestMismatch => (
            ErrorCode::RequestMismatch,
            "build identity or completion differs from original recorded session".into(),
        ),
        BuildError::Incomplete(e) => (ErrorCode::BuildIncomplete, e),
        BuildError::Unknown(e) => (ErrorCode::OutcomeUnknown, e),
        BuildError::Conflict(e) => (ErrorCode::StateConflict, e),
    };
    error(code, message)
}
struct Bound {
    identity: String,
    mode: Mode,
    root: String,
    engine: PathBuf,
    worker: Arc<Mutex<BuildWorker>>,
    interrupt: crate::native::NativeInterrupt,
    sessions: BTreeMap<Uuid, BuildIdentity>,
}
#[derive(Default)]
pub struct Runtime {
    connections: BTreeMap<Handle, Bound>,
    next: u64,
}
#[allow(clippy::result_large_err)]
impl Runtime {
    pub fn bind(
        &mut self,
        locator: ConnectionLocator,
        root: Option<String>,
        expected: Option<String>,
        workspace: Option<Uuid>,
        mode: Mode,
        resources: &Resources,
    ) -> Result<BoundConnection> {
        if ![Mode::ManagedBuild, Mode::ExternalBuild].contains(&mode) {
            return Err(error(
                ErrorCode::ProtocolFailure,
                "invalid DuckDB build binding mode",
            ));
        }
        let mut relocated = binding::locate_pull(locator.canonical_connection.clone())
            .map_err(|e| failure(e.into()))?;
        relocated.session_lock_path = locator.session_lock_path.clone();
        if relocated != locator
            || expected
                .as_ref()
                .is_some_and(|id| Some(id) != locator.identity.as_ref())
        {
            return Err(error(
                ErrorCode::RequestMismatch,
                "build connection locator changed",
            ));
        }
        let root = root.ok_or_else(|| {
            error(
                ErrorCode::InvalidDeclaration,
                "build requires a canonical bound root",
            )
        })?;
        let path = PathBuf::from(locator.engine_path.as_ref().unwrap());
        crate::extraction::outside_root(&path, &root).map_err(|e| failure(e.into()))?;
        let worker = BuildWorker::open(&path, root.clone(), workspace, resources.clone())
            .map_err(failure)?;
        let workspace = worker.workspace_id();
        let state = if workspace.is_some() {
            BindingState::Bound
        } else {
            BindingState::Uninitialized
        };
        let interrupt = worker.interrupt_handle();
        let identity = locator.identity.unwrap();
        self.next += 1;
        let handle = Handle::new(format!("duckdb-build-{}", self.next))
            .map_err(|e| error(ErrorCode::AdapterFailure, e.to_string()))?;
        self.connections.insert(
            handle.clone(),
            Bound {
                identity: identity.clone(),
                mode,
                root,
                engine: path,
                worker: Arc::new(Mutex::new(worker)),
                interrupt,
                sessions: BTreeMap::new(),
            },
        );
        Ok(BoundConnection {
            handle,
            identity: Some(identity),
            workspace_id: workspace.clone(),
            binding: state,
            details: serde_json::json!({"database":locator.canonical_connection["database"],"workspace_id":workspace,"binding":state}),
        })
    }
    pub fn contains(&self, handle: &Handle) -> bool {
        self.connections.contains_key(handle)
    }
    fn bound(&self, handle: &Handle) -> Result<&Bound> {
        self.connections
            .get(handle)
            .ok_or_else(|| error(ErrorCode::ProtocolFailure, "unknown DuckDB build handle"))
    }
    fn call<T: Send + 'static>(
        &self,
        handle: &Handle,
        job: impl FnOnce(&mut BuildStore) -> crate::build::Result<T> + Send + 'static,
        stop: &StopToken,
    ) -> Result<T> {
        self.bound(handle)?
            .worker
            .lock()
            .map_err(|_| error(ErrorCode::AdapterFailure, "build owner poisoned"))?
            .call(job, stop)
            .map_err(failure)
    }
    pub fn authenticate(&self, handle: &Handle, expected: Option<String>) -> Result<String> {
        let bound = self.bound(handle)?;
        if expected.as_ref().is_some_and(|id| id != &bound.identity) {
            return Err(error(
                ErrorCode::RequestMismatch,
                "build connection identity changed",
            ));
        }
        Ok(bound.identity.clone())
    }
    pub fn discover(
        &mut self,
        handle: Handle,
        request: DiscoverBuildRequest,
        stop: &StopToken,
    ) -> Result<BuildDiscovery> {
        if self.bound(&handle)?.mode != request.execution.mode() {
            return Err(error(
                ErrorCode::ProtocolFailure,
                "build execution mode differs from connection",
            ));
        }
        self.call(&handle, move |store| store.discover(request), stop)
    }
    pub fn prepare(
        &mut self,
        handle: Handle,
        request: PrepareBuildRequest,
        stop: &StopToken,
    ) -> Result<BuildSession> {
        request.validate().map_err(|_| {
            error(
                ErrorCode::ProtocolFailure,
                "invalid verified build preparation",
            )
        })?;
        let bound = self.bound(&handle)?;
        if request.discovery.identity.root != bound.root
            || request.discovery.identity.connection_identity != bound.identity
        {
            return Err(error(
                ErrorCode::RequestMismatch,
                "build preparation differs from its bound connection",
            ));
        }
        if request
            .base_files
            .iter()
            .chain(request.input_files.iter().flat_map(|input| &input.files))
            .any(|file| file.access == FileAccess::S3View)
        {
            let bound = self.bound(&handle)?;
            if bound.mode != Mode::ExternalBuild {
                return Err(error(
                    ErrorCode::ProtocolFailure,
                    "managed build preparation requires verified local input files",
                ));
            }
            let reader =
                crate::s3_config::load_reader(&bound.engine, &bound.root).map_err(|_| {
                    error(
                        ErrorCode::AdapterFailure,
                        "independent DuckDB S3 reader configuration is unavailable or invalid",
                    )
                })?;
            self.call(
                &handle,
                move |store| store.configure_s3_reader(&reader),
                stop,
            )
            .map_err(|_| {
                error(
                    ErrorCode::AdapterFailure,
                    "independent DuckDB S3 reader authentication failed",
                )
            })?;
        }
        let session = self.call(&handle, move |store| store.prepare(request), stop)?;
        self.connections
            .get_mut(&handle)
            .unwrap()
            .sessions
            .insert(session.session_id.clone(), session.identity.clone());
        Ok(session)
    }
    pub fn execute(
        &mut self,
        handle: Handle,
        request: ExecuteBuildRequest,
        stop: &StopToken,
    ) -> Result<BuildExecutionResult> {
        self.call(&handle, move |store| store.execute(request), stop)
    }
    pub fn open(
        &mut self,
        handle: Handle,
        identity: BuildIdentity,
        stop: &StopToken,
        inspect: bool,
    ) -> Result<BuildRecord> {
        let record = self.call(&handle, move |store| store.open_session(&identity), stop)?;
        if !inspect {
            self.connections.get_mut(&handle).unwrap().sessions.insert(
                record.session.session_id.clone(),
                record.session.identity.clone(),
            );
        }
        Ok(record)
    }
    fn identity(&self, handle: &Handle, session: &Uuid) -> Result<BuildIdentity> {
        self.bound(handle)?
            .sessions
            .get(session)
            .cloned()
            .ok_or_else(|| {
                error(
                    ErrorCode::ProtocolFailure,
                    "session was not prepared or opened under this handle",
                )
            })
    }
    pub fn accept(
        &mut self,
        handle: Handle,
        session: Uuid,
        completion: BuildCompletion,
        stop: &StopToken,
    ) -> Result<Digest> {
        let identity = self.identity(&handle, &session)?;
        self.call(
            &handle,
            move |store| store.accept(&identity, completion),
            stop,
        )
    }
    pub fn export(
        &mut self,
        handle: Handle,
        session: Uuid,
        digest: Digest,
        _stream: Uuid,
        resources: &Resources,
        stop: &StopToken,
    ) -> Result<Box<dyn BuildExport>> {
        let identity = self.identity(&handle, &session)?;
        let counts = self.call(
            &handle,
            move |store| store.start_export(&identity, &digest),
            stop,
        )?;
        Ok(Box::new(Export {
            worker: self.bound(&handle)?.worker.clone(),
            resources: resources.clone(),
            counts,
            index: 0,
            complete: false,
        }))
    }
    pub fn abort(&mut self, handle: Handle, session: Uuid, stop: &StopToken) -> Result<()> {
        let identity = self.identity(&handle, &session)?;
        self.call(&handle, move |store| store.abort(&identity), stop)
    }
    pub fn outcome(
        &mut self,
        handle: Handle,
        session: Uuid,
        outcome: BuildOutcome,
        stop: &StopToken,
    ) -> Result<()> {
        let identity = self.identity(&handle, &session)?;
        self.call(
            &handle,
            move |store| store.record_outcome(&identity, outcome),
            stop,
        )
    }
    pub fn cleanup(&mut self, handle: Handle, session: Uuid, stop: &StopToken) -> Result<()> {
        let identity = self.identity(&handle, &session)?;
        self.call(&handle, move |store| store.cleanup(&identity), stop)
    }
    pub fn stop(&mut self) -> Result<()> {
        for bound in self.connections.values() {
            bound.interrupt.interrupt();
        }
        for bound in self.connections.values() {
            bound
                .worker
                .lock()
                .map_err(|_| error(ErrorCode::AdapterFailure, "build owner poisoned"))?
                .stop()
                .map_err(|e| failure(e.into()))?;
        }
        self.connections.clear();
        Ok(())
    }
}
struct Export {
    worker: Arc<Mutex<BuildWorker>>,
    resources: Resources,
    counts: Vec<TableCount>,
    index: usize,
    complete: bool,
}
#[allow(clippy::result_large_err)]
impl BuildExport for Export {
    fn next(&mut self, stop: &StopToken) -> Result<Option<BuildExportEvent>> {
        if self.complete {
            return Ok(None);
        }
        if self.index == self.counts.len() {
            self.worker
                .lock()
                .map_err(|_| error(ErrorCode::AdapterFailure, "build owner poisoned"))?
                .call(|store| store.finish_export(), stop)
                .map_err(failure)?;
            self.complete = true;
            return Ok(Some(BuildExportEvent::Complete(serde_json::json!({}))));
        }
        let table = self.counts[self.index].table.clone();
        let index = self.index;
        let resources = self.resources.clone();
        let batch = self
            .worker
            .lock()
            .map_err(|_| error(ErrorCode::AdapterFailure, "build owner poisoned"))?
            .call(move |store| store.fetch_export(index, &resources), stop)
            .map_err(failure)?;
        Ok(Some(match batch {
            Some((payload, rows)) => BuildExportEvent::Batch {
                table,
                payload,
                rows,
            },
            None => {
                let row_count = self.counts[self.index].rows;
                self.index += 1;
                BuildExportEvent::TableComplete { table, row_count }
            }
        }))
    }
}
