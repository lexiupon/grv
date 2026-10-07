//! Faults and opt-in after-publish hooks exist only in this harness.
//! Ordinary registration follows the production DuckDbAdapter unchanged.
use grv_adapter_api::{CommandCall, Name, Req, Resources};
use grv_adapter_sdk::{Adapter, PreparedCommand, Registration, StopToken};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    io::Write,
    path::{Path, PathBuf},
};
#[derive(Default)]
struct BuildFixture {
    adapter: grv_adapter_duckdb::process::DuckDbAdapter,
    paths: BTreeMap<grv_adapter_api::Handle, PathBuf>,
    hook_directory: Option<PathBuf>,
    locators: BTreeMap<grv_adapter_api::Handle, grv_adapter_api::ConnectionLocator>,
    prepared: BTreeMap<grv_adapter_api::Uuid, HookPrivate>,
    hooks: BTreeMap<grv_adapter_api::Handle, HookPrivate>,
}
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct HookPrivate {
    identity: grv_adapter_api::BuildIdentity,
    mode: grv_adapter_api::Mode,
    locator: grv_adapter_api::ConnectionLocator,
}
fn hook_path(
    directory: &Path,
    run: &grv_types::RunId,
    suffix: &str,
) -> grv_adapter_sdk::Result<PathBuf> {
    grv_adapter_host::protected_document::canonical_path(
        &directory.join(format!("{run}.{suffix}.json")),
        &[],
    )
    .map_err(|e| e.public())
}
fn private_run(locator: &grv_adapter_api::ConnectionLocator) -> Option<grv_types::RunId> {
    locator
        .session_lock_path
        .as_ref()?
        .rsplit_once(".grv-session-")?
        .1
        .strip_suffix(".lock")
        .and_then(|id| grv_types::RunId::new(id).ok())
}
fn known_private(
    directory: &Path,
    run: &grv_types::RunId,
) -> grv_adapter_sdk::Result<Option<HookPrivate>> {
    let known = hook_path(directory, run, "known")?;
    if !known.exists() {
        return Ok(None);
    }
    let outcome: grv_adapter_api::Outcome =
        grv_adapter_host::protected_document::read(&known, 65536).map_err(|e| e.public())?;
    outcome
        .validate()
        .map_err(|_| fault("invalid fixture known outcome"))?;
    let state: HookPrivate =
        grv_adapter_host::protected_document::read(&hook_path(directory, run, "binding")?, 65536)
            .map_err(|e| e.public())?;
    state
        .identity
        .validate()
        .map_err(|_| fault("invalid fixture private identity"))?;
    if &state.identity.run_id != run {
        return Err(fault("fixture private run differs"));
    }
    Ok(Some(state))
}
fn marker(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    name.into()
}
fn fault(message: &str) -> grv_types::PublicError {
    grv_types::PublicError {
        code: grv_types::ErrorCode::AdapterFailure,
        message: message.into(),
        object: None,
        retryable: false,
    }
}
struct FailAfterBatch {
    inner: Box<dyn grv_adapter_sdk::BuildExport>,
    fail_marker: PathBuf,
    sent_batch: bool,
}
impl grv_adapter_sdk::BuildExport for FailAfterBatch {
    fn next(
        &mut self,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<Option<grv_adapter_sdk::BuildExportEvent>> {
        if self.sent_batch {
            std::fs::remove_file(&self.fail_marker)
                .map_err(|_| fault("could not consume conformance export fault"))?;
            return Err(fault(
                "conformance export interrupted after its first batch",
            ));
        }
        let event = self.inner.next(stop)?;
        self.sent_batch = matches!(event, Some(grv_adapter_sdk::BuildExportEvent::Batch { .. }));
        Ok(event)
    }
}
impl Adapter for BuildFixture {
    fn configure_cancellation(&mut self, token: &StopToken) -> grv_adapter_sdk::Result<()> {
        self.adapter.configure_cancellation(token)
    }
    fn configure_resources(&mut self, resources: &Resources) -> grv_adapter_sdk::Result<()> {
        self.adapter.configure_resources(resources)
    }
    fn registration(&self) -> Registration {
        let mut registration = self.adapter.registration();
        registration.capabilities.after_publish = self.hook_directory.is_some();
        registration
    }
    fn prepare_command(
        &self,
        name: &Name,
        argv: &[String],
    ) -> grv_adapter_sdk::Result<PreparedCommand> {
        self.adapter.prepare_command(name, argv)
    }
    fn execute_command(
        &mut self,
        call: &CommandCall,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<Value> {
        self.adapter.execute_command(call, stop)
    }
    fn validate_binding(
        &self,
        declaration: Value,
        mode: grv_adapter_api::Mode,
        schema_version: Req,
    ) -> grv_adapter_sdk::Result<Value> {
        self.adapter
            .validate_binding(declaration, mode, schema_version)
    }
    fn locate_connection(
        &self,
        connection: Value,
        mode: grv_adapter_api::Mode,
        _run: Option<grv_types::RunId>,
    ) -> grv_adapter_sdk::Result<grv_adapter_api::ConnectionLocator> {
        if let (Some(directory), Some(run)) = (&self.hook_directory, &_run)
            && let Some(state) = known_private(directory, run)?
        {
            if state.mode != mode || state.locator.canonical_connection != connection {
                return Err(fault("fixture hook connection differs"));
            }
            return Ok(state.locator);
        }
        self.adapter.locate_connection(connection, mode, _run)
    }
    fn bind_connection(
        &mut self,
        locator: grv_adapter_api::ConnectionLocator,
        root: Option<String>,
        expected: Option<String>,
        workspace: Option<grv_types::Uuid>,
        mode: grv_adapter_api::Mode,
    ) -> grv_adapter_sdk::Result<grv_adapter_sdk::BoundConnection> {
        if let (Some(directory), Some(run)) = (&self.hook_directory, private_run(&locator))
            && let Some(state) = known_private(directory, &run)?
        {
            if state.locator != locator
                || state.mode != mode
                || root.as_deref() != Some(state.identity.root.as_str())
                || expected.as_deref() != Some(state.identity.connection_identity.as_str())
                || workspace.as_ref() != Some(&state.identity.workspace_id)
            {
                return Err(fault("fixture hook binding differs"));
            }
            let handle = grv_adapter_api::Handle::new("duckdb-private-hook").unwrap();
            let result = grv_adapter_sdk::BoundConnection {
                handle: handle.clone(),
                identity: Some(state.identity.connection_identity.clone()),
                workspace_id: Some(state.identity.workspace_id.clone()),
                binding: grv_adapter_api::BindingState::Bound,
                details: serde_json::json!({"database":state.locator.canonical_connection["database"],"workspace_id":state.identity.workspace_id,"binding":"bound"}),
            };
            self.hooks.insert(handle, state);
            return Ok(result);
        }
        let path = locator.engine_path.as_ref().map(PathBuf::from);
        let fixed_locator = locator.clone();
        let bound = self
            .adapter
            .bind_connection(locator, root, expected, workspace, mode)?;
        if let Some(path) = path {
            self.paths.insert(bound.handle.clone(), path);
        }
        self.locators.insert(bound.handle.clone(), fixed_locator);
        Ok(bound)
    }
    fn authenticate(
        &mut self,
        handle: grv_adapter_api::Handle,
        expected: Option<String>,
    ) -> grv_adapter_sdk::Result<String> {
        if let Some(state) = self.hooks.get(&handle) {
            if expected
                .as_ref()
                .is_some_and(|value| value != &state.identity.connection_identity)
            {
                return Err(fault("fixture hook authentication differs"));
            }
            return Ok(state.identity.connection_identity.clone());
        }
        self.adapter.authenticate(handle, expected)
    }
    fn after_publish(
        &mut self,
        handle: grv_adapter_api::Handle,
        request: grv_adapter_api::AfterPublishRequest,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<()> {
        stop.check()?;
        request
            .validate()
            .map_err(|_| fault("invalid fixture hook request"))?;
        let state = self
            .hooks
            .get(&handle)
            .ok_or_else(|| fault("fixture hook has no private binding"))?;
        let directory = self
            .hook_directory
            .as_ref()
            .ok_or_else(|| fault("fixture hook disabled"))?;
        if request.attempt_id != state.identity.attempt_id
            || request.declaration_sha256 != state.identity.declaration_sha256
        {
            return Err(fault("fixture hook fixed attempt differs"));
        }
        let known: grv_adapter_api::Outcome = grv_adapter_host::protected_document::read(
            &hook_path(directory, &state.identity.run_id, "known")?,
            65536,
        )
        .map_err(|e| e.public())?;
        if known != request.outcome {
            return Err(fault("fixture hook known outcome differs"));
        }
        let publish = |suffix: &str| {
            grv_adapter_host::protected_document::publish(
                &hook_path(directory, &state.identity.run_id, suffix)?,
                &request,
                65536,
            )
            .map_err(|e| e.public())
        };
        publish("pending")?;
        publish(&format!("call-{}", grv_types::Uuid::v4()))?;
        if directory.join("require_auth").exists() {
            publish(&format!("auth-{}", grv_types::Uuid::v4()))?;
        }
        if directory.join("fail").exists() {
            return Err(fault("fixture build after-publish failed"));
        }
        publish("complete")
    }
    fn inspect_connection(
        &mut self,
        handle: grv_adapter_api::Handle,
        request: grv_adapter_api::InspectConnectionRequest,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<Value> {
        self.adapter.inspect_connection(handle, request, stop)
    }
    fn extract(
        &mut self,
        handle: grv_adapter_api::Handle,
        request: grv_adapter_api::ExtractRequest,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<Box<dyn grv_adapter_sdk::Extraction>> {
        self.adapter.extract(handle, request, stop)
    }
    fn resolve_pull(
        &mut self,
        handle: grv_adapter_api::Handle,
        request: grv_adapter_api::ResolvePullRequest,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<grv_adapter_api::PullResolution> {
        self.adapter.resolve_pull(handle, request, stop)
    }
    fn prepare_pull(
        &mut self,
        handle: grv_adapter_api::Handle,
        request: grv_adapter_api::PreparePullRequest,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<grv_adapter_api::PullPlan> {
        self.adapter.prepare_pull(handle, request, stop)
    }
    fn apply_pull(
        &mut self,
        handle: grv_adapter_api::Handle,
        plan: grv_adapter_api::PullPlan,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<grv_adapter_api::Receipt> {
        self.adapter.apply_pull(handle, plan, stop)
    }
    fn discover_build(
        &mut self,
        handle: grv_adapter_api::Handle,
        request: grv_adapter_api::DiscoverBuildRequest,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<grv_adapter_api::BuildDiscovery> {
        let discovery = self.adapter.discover_build(handle.clone(), request, stop)?;
        if let Some(path) = self.paths.get(&handle) {
            let pause = marker(path, ".grv-fixture-pause-discovery");
            if pause.is_file() {
                let ready = marker(path, ".grv-fixture-discovery-ready");
                std::fs::write(&ready, b"ready")
                    .map_err(|_| fault("could not signal conformance discovery"))?;
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
                while pause.exists() {
                    stop.check()?;
                    if std::time::Instant::now() >= deadline {
                        return Err(fault("conformance discovery pause timed out"));
                    }
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
            }
        }
        Ok(discovery)
    }
    fn prepare_build(
        &mut self,
        handle: grv_adapter_api::Handle,
        request: grv_adapter_api::PrepareBuildRequest,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<grv_adapter_api::BuildSession> {
        let session = self.adapter.prepare_build(handle.clone(), request, stop)?;
        if let Some(directory) = &self.hook_directory {
            let locator = self
                .locators
                .get(&handle)
                .ok_or_else(|| fault("fixture build locator missing"))?
                .clone();
            let state = HookPrivate {
                identity: session.identity.clone(),
                mode: session.execution.mode(),
                locator,
            };
            let path = hook_path(directory, &state.identity.run_id, "binding")?;
            grv_adapter_host::protected_document::canonical_path(
                &path,
                &[
                    Path::new(&state.identity.root),
                    Path::new(state.locator.engine_path.as_ref().unwrap()),
                ],
            )
            .map_err(|e| e.public())?;
            grv_adapter_host::protected_document::publish(&path, &state, 65536)
                .map_err(|e| e.public())?;
            self.prepared.insert(session.session_id.clone(), state);
        }
        Ok(session)
    }
    fn execute_build(
        &mut self,
        handle: grv_adapter_api::Handle,
        request: grv_adapter_api::ExecuteBuildRequest,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<grv_adapter_api::BuildExecutionResult> {
        if let Some(path) = self.paths.get(&handle) {
            let count = marker(path, ".grv-fixture-executions");
            if count.is_file() {
                std::fs::OpenOptions::new()
                    .append(true)
                    .open(&count)
                    .and_then(|mut file| file.write_all(b"execute\n"))
                    .map_err(|_| fault("could not record conformance execution"))?;
            }
        }
        self.adapter.execute_build(handle, request, stop)
    }
    fn accept_build_completion(
        &mut self,
        handle: grv_adapter_api::Handle,
        session: grv_adapter_api::Uuid,
        completion: grv_adapter_api::BuildCompletion,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<grv_adapter_api::Digest> {
        self.adapter
            .accept_build_completion(handle, session, completion, stop)
    }
    fn export_build(
        &mut self,
        handle: grv_adapter_api::Handle,
        session: grv_adapter_api::Uuid,
        digest: grv_adapter_api::Digest,
        stream: grv_adapter_api::Uuid,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<Box<dyn grv_adapter_sdk::BuildExport>> {
        let fail_marker = self
            .paths
            .get(&handle)
            .map(|path| marker(path, ".grv-fixture-fail-export-once"));
        let inner = self
            .adapter
            .export_build(handle, session, digest, stream, stop)?;
        if let Some(fail_marker) = fail_marker.filter(|path| path.is_file()) {
            Ok(Box::new(FailAfterBatch {
                inner,
                fail_marker,
                sent_batch: false,
            }))
        } else {
            Ok(inner)
        }
    }
    fn open_build(
        &mut self,
        handle: grv_adapter_api::Handle,
        identity: grv_adapter_api::BuildIdentity,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<grv_adapter_api::BuildRecord> {
        let record = self.adapter.open_build(handle, identity.clone(), stop)?;
        if let Some(directory) = &self.hook_directory {
            let state: HookPrivate = grv_adapter_host::protected_document::read(
                &hook_path(directory, &identity.run_id, "binding")?,
                65536,
            )
            .map_err(|e| e.public())?;
            if state.identity != identity {
                return Err(fault("fixture reopened build identity differs"));
            }
            self.prepared
                .insert(record.session.session_id.clone(), state);
        }
        Ok(record)
    }
    fn inspect_build(
        &mut self,
        handle: grv_adapter_api::Handle,
        identity: grv_adapter_api::BuildIdentity,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<grv_adapter_api::BuildRecord> {
        self.adapter.inspect_build(handle, identity, stop)
    }
    fn abort_build(
        &mut self,
        handle: grv_adapter_api::Handle,
        session: grv_adapter_api::Uuid,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<()> {
        self.adapter.abort_build(handle, session, stop)
    }
    fn record_build_outcome(
        &mut self,
        handle: grv_adapter_api::Handle,
        session: grv_adapter_api::Uuid,
        outcome: grv_adapter_api::BuildOutcome,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<()> {
        self.adapter
            .record_build_outcome(handle, session.clone(), outcome.clone(), stop)?;
        if let Some(directory) = &self.hook_directory {
            let state = self
                .prepared
                .get(&session)
                .ok_or_else(|| fault("fixture prepared hook state missing"))?;
            grv_adapter_host::protected_document::publish(
                &hook_path(directory, &state.identity.run_id, "known")?,
                &outcome,
                65536,
            )
            .map_err(|e| e.public())?;
        }
        Ok(())
    }
    fn cleanup_build(
        &mut self,
        handle: grv_adapter_api::Handle,
        session: grv_adapter_api::Uuid,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<()> {
        self.adapter.cleanup_build(handle, session, stop)
    }
    fn stop_and_wait(&mut self) -> grv_adapter_sdk::Result<()> {
        self.adapter.stop_and_wait()
    }
}
fn main() {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let mut fixture = BuildFixture::default();
    match args.as_slice() {
        [] => {}
        [flag, directory] if flag == "--after-publish-dir" => {
            let path = PathBuf::from(directory);
            match grv_adapter_host::protected_document::canonical_path(
                &path.join("probe.json"),
                &[],
            ) {
                Ok(probe) => fixture.hook_directory = Some(probe.parent().unwrap().into()),
                Err(_) => {
                    eprintln!("unprotected fixture hook directory");
                    std::process::exit(2);
                }
            }
        }
        _ => {
            eprintln!("invalid build fixture arguments");
            std::process::exit(2);
        }
    }
    if let Err(error) = grv_adapter_sdk::run_fd3(fixture) {
        eprintln!("DuckDB build conformance channel failed: {error}");
        std::process::exit(6);
    }
}
