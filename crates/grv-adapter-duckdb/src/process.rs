//! Separate process. Native builds expose guarded extraction, transactional
//! local/S3 pulls, managed/external builds and read-only inspection.
use grv_adapter_api::{Capabilities, CommandCall, Name, Req, Resources};
use grv_adapter_sdk::{Adapter, PreparedCommand, Registration, StopToken};
use grv_types::{ErrorCode, PublicError};
use serde_json::Value;

#[derive(Default)]
pub struct DuckDbAdapter {
    resources: Resources,
    #[cfg(feature = "native")]
    runtime: crate::extraction::Runtime,
    #[cfg(feature = "native")]
    pulls: crate::pull_runtime::Runtime,
    #[cfg(feature = "native")]
    builds: crate::build_runtime::Runtime,
    #[cfg(feature = "native")]
    inspection: crate::inspection::Runtime,
}
impl DuckDbAdapter {
    pub fn resources(&self) -> &Resources {
        &self.resources
    }
}

// The closed SDK contract returns PublicError by value.
#[allow(clippy::result_large_err)]
fn unsupported<T>() -> grv_adapter_sdk::Result<T> {
    Err(PublicError {
        code: ErrorCode::UnsupportedCapability,
        message: "DuckDB requested lifecycle is unavailable or remains capability-gated".into(),
        retryable: false,
        object: None,
    })
}

impl Adapter for DuckDbAdapter {
    fn configure_cancellation(&mut self, token: &StopToken) -> grv_adapter_sdk::Result<()> {
        #[cfg(feature = "native")]
        self.pulls.configure_cancellation(token);
        #[cfg(not(feature = "native"))]
        let _ = token;
        Ok(())
    }
    fn configure_resources(&mut self, resources: &Resources) -> grv_adapter_sdk::Result<()> {
        self.resources = resources.clone();
        Ok(())
    }
    fn registration(&self) -> Registration {
        let capabilities = if cfg!(feature = "native") {
            Capabilities {
                push: true,
                pull: true,
                managed_build: true,
                external_build: true,
                inspect_connection: true,
                source_consistency: grv_adapter_api::WireConsistency::Snapshot,
                pull_write_modes: vec![
                    grv_adapter_api::WriteMode::Replace,
                    grv_adapter_api::WriteMode::Append,
                ],
                pull_materializations: vec![
                    grv_adapter_api::Materialization::Local,
                    grv_adapter_api::Materialization::S3View,
                ],
                pull_recovery: Some(grv_adapter_api::PullRecovery::Transactional),
                ..Capabilities::default()
            }
        } else {
            Capabilities::default()
        };
        Registration {
            name: Name::new("duckdb").expect("valid adapter name"),
            package_version: env!("CARGO_PKG_VERSION").into(),
            interface_versions: vec![Req::new(1).expect("valid version")],
            binding_schema_version: Req::new(1).expect("valid version"),
            capabilities,
            registry: crate::binding::registry(),
            commands: vec![],
        }
    }
    fn prepare_command(&self, _: &Name, _: &[String]) -> grv_adapter_sdk::Result<PreparedCommand> {
        unsupported()
    }
    fn execute_command(
        &mut self,
        _: &CommandCall,
        _: &StopToken,
    ) -> grv_adapter_sdk::Result<Value> {
        unsupported()
    }
    fn validate_binding(
        &self,
        declaration: Value,
        mode: grv_adapter_api::Mode,
        schema_version: Req,
    ) -> grv_adapter_sdk::Result<Value> {
        if ![
            grv_adapter_api::Mode::Extract,
            grv_adapter_api::Mode::Pull,
            grv_adapter_api::Mode::ManagedBuild,
            grv_adapter_api::Mode::ExternalBuild,
            grv_adapter_api::Mode::Inspect,
        ]
        .contains(&mode)
            || schema_version.get() != 1
        {
            return unsupported();
        }
        (if mode == grv_adapter_api::Mode::Inspect {
            crate::binding::database(&declaration["connection"]).map(|_| declaration.clone())
        } else if mode == grv_adapter_api::Mode::Pull {
            crate::binding::validate_pull(declaration)
        } else if [
            grv_adapter_api::Mode::ManagedBuild,
            grv_adapter_api::Mode::ExternalBuild,
        ]
        .contains(&mode)
        {
            crate::binding::validate_build(declaration, mode)
        } else {
            crate::binding::validate(declaration)
        })
        .map_err(|error| PublicError {
            code: ErrorCode::InvalidDeclaration,
            message: error.to_string(),
            retryable: false,
            object: None,
        })
    }
    fn locate_connection(
        &self,
        connection: Value,
        mode: grv_adapter_api::Mode,
        _run: Option<grv_types::RunId>,
    ) -> grv_adapter_sdk::Result<grv_adapter_api::ConnectionLocator> {
        if ![
            grv_adapter_api::Mode::Extract,
            grv_adapter_api::Mode::Pull,
            grv_adapter_api::Mode::ManagedBuild,
            grv_adapter_api::Mode::ExternalBuild,
            grv_adapter_api::Mode::Inspect,
        ]
        .contains(&mode)
        {
            return unsupported();
        }
        (if mode == grv_adapter_api::Mode::Pull {
            crate::binding::locate_pull(connection)
        } else if [
            grv_adapter_api::Mode::ManagedBuild,
            grv_adapter_api::Mode::ExternalBuild,
        ]
        .contains(&mode)
        {
            crate::binding::locate_build(connection, _run)
        } else {
            crate::binding::locate(connection)
        })
        .map_err(|error| PublicError {
            code: ErrorCode::InvalidDeclaration,
            message: error.to_string(),
            retryable: false,
            object: None,
        })
    }
    #[cfg(feature = "native")]
    fn bind_connection(
        &mut self,
        locator: grv_adapter_api::ConnectionLocator,
        root: Option<String>,
        expected: Option<String>,
        workspace: Option<grv_types::Uuid>,
        mode: grv_adapter_api::Mode,
    ) -> grv_adapter_sdk::Result<grv_adapter_sdk::BoundConnection> {
        if mode == grv_adapter_api::Mode::Inspect {
            self.inspection
                .bind(locator, root, expected, workspace, &self.resources)
        } else if mode == grv_adapter_api::Mode::Pull {
            self.pulls
                .bind(locator, root, expected, workspace, &self.resources)
        } else if [
            grv_adapter_api::Mode::ManagedBuild,
            grv_adapter_api::Mode::ExternalBuild,
        ]
        .contains(&mode)
        {
            self.builds
                .bind(locator, root, expected, workspace, mode, &self.resources)
        } else {
            self.runtime.bind(locator, expected, workspace, mode, root)
        }
    }
    #[cfg(feature = "native")]
    fn inspect_connection(
        &mut self,
        handle: grv_adapter_api::Handle,
        request: grv_adapter_api::InspectConnectionRequest,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<Value> {
        self.inspection.inspect(handle, request, stop)
    }
    #[cfg(feature = "native")]
    fn authenticate(
        &mut self,
        handle: grv_adapter_api::Handle,
        expected: Option<String>,
    ) -> grv_adapter_sdk::Result<String> {
        if self.pulls.contains(&handle) {
            self.pulls.authenticate(&handle, expected)
        } else if self.builds.contains(&handle) {
            self.builds.authenticate(&handle, expected)
        } else {
            self.runtime.authenticate(handle, expected)
        }
    }
    #[cfg(feature = "native")]
    fn extract(
        &mut self,
        handle: grv_adapter_api::Handle,
        request: grv_adapter_api::ExtractRequest,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<Box<dyn grv_adapter_sdk::Extraction>> {
        self.runtime.extract(handle, request, &self.resources, stop)
    }
    #[cfg(feature = "native")]
    fn resolve_pull(
        &mut self,
        handle: grv_adapter_api::Handle,
        request: grv_adapter_api::ResolvePullRequest,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<grv_adapter_api::PullResolution> {
        self.pulls.resolve(handle, request, stop)
    }
    #[cfg(feature = "native")]
    fn prepare_pull(
        &mut self,
        handle: grv_adapter_api::Handle,
        request: grv_adapter_api::PreparePullRequest,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<grv_adapter_api::PullPlan> {
        self.pulls.prepare(handle, request, stop)
    }
    #[cfg(feature = "native")]
    fn apply_pull(
        &mut self,
        handle: grv_adapter_api::Handle,
        plan: grv_adapter_api::PullPlan,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<grv_adapter_api::Receipt> {
        self.pulls.apply(handle, plan, stop)
    }
    #[cfg(feature = "native")]
    fn discover_build(
        &mut self,
        handle: grv_adapter_api::Handle,
        request: grv_adapter_api::DiscoverBuildRequest,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<grv_adapter_api::BuildDiscovery> {
        self.builds.discover(handle, request, stop)
    }
    #[cfg(feature = "native")]
    fn prepare_build(
        &mut self,
        handle: grv_adapter_api::Handle,
        request: grv_adapter_api::PrepareBuildRequest,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<grv_adapter_api::BuildSession> {
        self.builds.prepare(handle, request, stop)
    }
    #[cfg(feature = "native")]
    fn execute_build(
        &mut self,
        handle: grv_adapter_api::Handle,
        request: grv_adapter_api::ExecuteBuildRequest,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<grv_adapter_api::BuildExecutionResult> {
        self.builds.execute(handle, request, stop)
    }
    #[cfg(feature = "native")]
    fn accept_build_completion(
        &mut self,
        handle: grv_adapter_api::Handle,
        session: grv_adapter_api::Uuid,
        completion: grv_adapter_api::BuildCompletion,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<grv_adapter_api::Digest> {
        self.builds.accept(handle, session, completion, stop)
    }
    #[cfg(feature = "native")]
    fn export_build(
        &mut self,
        handle: grv_adapter_api::Handle,
        session: grv_adapter_api::Uuid,
        digest: grv_adapter_api::Digest,
        stream: grv_adapter_api::Uuid,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<Box<dyn grv_adapter_sdk::BuildExport>> {
        self.builds
            .export(handle, session, digest, stream, &self.resources, stop)
    }
    #[cfg(feature = "native")]
    fn open_build(
        &mut self,
        handle: grv_adapter_api::Handle,
        identity: grv_adapter_api::BuildIdentity,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<grv_adapter_api::BuildRecord> {
        self.builds.open(handle, identity, stop, false)
    }
    #[cfg(feature = "native")]
    fn inspect_build(
        &mut self,
        handle: grv_adapter_api::Handle,
        identity: grv_adapter_api::BuildIdentity,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<grv_adapter_api::BuildRecord> {
        self.builds.open(handle, identity, stop, true)
    }
    #[cfg(feature = "native")]
    fn abort_build(
        &mut self,
        handle: grv_adapter_api::Handle,
        session: grv_adapter_api::Uuid,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<()> {
        self.builds.abort(handle, session, stop)
    }
    #[cfg(feature = "native")]
    fn record_build_outcome(
        &mut self,
        handle: grv_adapter_api::Handle,
        session: grv_adapter_api::Uuid,
        outcome: grv_adapter_api::BuildOutcome,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<()> {
        self.builds.outcome(handle, session, outcome, stop)
    }
    #[cfg(feature = "native")]
    fn cleanup_build(
        &mut self,
        handle: grv_adapter_api::Handle,
        session: grv_adapter_api::Uuid,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<()> {
        self.builds.cleanup(handle, session, stop)
    }
    fn stop_and_wait(&mut self) -> grv_adapter_sdk::Result<()> {
        #[cfg(feature = "native")]
        {
            let extraction = self.runtime.stop();
            let pulls = self.pulls.stop();
            let builds = self.builds.stop();
            let inspection = self.inspection.stop();
            extraction?;
            pulls?;
            builds?;
            inspection
        }
        #[cfg(not(feature = "native"))]
        {
            Ok(())
        }
    }
}
