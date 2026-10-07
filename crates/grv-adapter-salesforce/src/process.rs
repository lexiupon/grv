//! Process adapter for conformant extraction and private authenticated sessions.
use grv_adapter_api::{
    BindingState, Capabilities, CommandCall, ConnectionLocator, ExtractRequest, Handle, Mode, Name,
    Req, Resources, RunId, Uuid, WireConsistency,
};
use grv_adapter_sdk::{
    Adapter, BoundConnection, Extraction, PreparedCommand, Registration, StopToken,
};
use grv_types::{ErrorCode, PublicError};
use serde_json::{Value, json};

pub struct SalesforceAdapter {
    pub home: std::path::PathBuf,
    pub sf_program: std::ffi::OsString,
    pub curl_program: std::ffi::OsString,
    pub supervisor: Option<std::ffi::OsString>,
    resources: Resources,
    cancellation: crate::runtime::Cancellation,
    bound: Option<(
        Handle,
        crate::auth::BoundConnection,
        crate::config::Connection,
        Option<String>,
    )>,
}
impl Default for SalesforceAdapter {
    fn default() -> Self {
        Self::with_programs(
            std::env::var_os("HOME")
                .map(std::path::PathBuf::from)
                .unwrap_or_default(),
            "sf".into(),
            "curl".into(),
        )
    }
}
impl SalesforceAdapter {
    pub fn with_programs(
        home: std::path::PathBuf,
        sf_program: std::ffi::OsString,
        curl_program: std::ffi::OsString,
    ) -> Self {
        Self {
            home,
            sf_program,
            curl_program,
            supervisor: None,
            resources: Resources::default(),
            cancellation: Default::default(),
            bound: None,
        }
    }
    fn active(
        &mut self,
        handle: &Handle,
    ) -> grv_adapter_sdk::Result<&mut (
        Handle,
        crate::auth::BoundConnection,
        crate::config::Connection,
        Option<String>,
    )> {
        self.bound
            .as_mut()
            .filter(|bound| &bound.0 == handle)
            .ok_or_else(|| PublicError {
                code: ErrorCode::ProtocolFailure,
                message: "unknown Salesforce handle".into(),
                retryable: false,
                object: None,
            })
    }
}

fn unsupported<T>() -> grv_adapter_sdk::Result<T> {
    Err(PublicError {
        code: ErrorCode::UnsupportedCapability,
        message: "Salesforce operation is unavailable in this adapter package".into(),
        retryable: false,
        object: None,
    })
}

impl Adapter for SalesforceAdapter {
    fn configure_resources(&mut self, resources: &Resources) -> grv_adapter_sdk::Result<()> {
        resources.validate().map_err(|_| {
            crate::extraction::public(crate::integrity("invalid Salesforce resources"))
        })?;
        self.resources = resources.clone();
        Ok(())
    }
    fn configure_cancellation(&mut self, token: &StopToken) -> grv_adapter_sdk::Result<()> {
        self.cancellation = crate::runtime::Cancellation::from_sdk(token);
        Ok(())
    }
    fn registration(&self) -> Registration {
        let capabilities = Capabilities {
            push: true,
            source_consistency: WireConsistency::CaptureWindow,
            ..Capabilities::default()
        };
        Registration {
            name: Name::new("salesforce").expect("valid adapter name"),
            package_version: env!("CARGO_PKG_VERSION").into(),
            interface_versions: vec![Req::new(1).expect("valid version")],
            binding_schema_version: Req::new(1).expect("valid version"),
            capabilities,
            registry: crate::config::extraction_registry(),
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
        mode: Mode,
        version: Req,
    ) -> grv_adapter_sdk::Result<Value> {
        if mode != Mode::Extract || version.get() != 1 {
            return unsupported();
        }
        crate::config::validate_binding(&declaration).map_err(crate::extraction::public)
    }
    fn locate_connection(
        &self,
        connection: Value,
        mode: Mode,
        _: Option<RunId>,
    ) -> grv_adapter_sdk::Result<ConnectionLocator> {
        if mode != Mode::Extract {
            return unsupported();
        }
        let connection: crate::config::Connection =
            serde_json::from_value(connection).map_err(|_| {
                crate::extraction::public(crate::invalid("invalid Salesforce connection"))
            })?;
        let locator = crate::auth::locate_connection(
            &connection,
            &mut crate::offline::FileMetadataStore::for_home(self.home.clone()),
        )
        .map_err(crate::extraction::public)?;
        Ok(ConnectionLocator {
            canonical_connection: serde_json::to_value(locator.canonical_connection)
                .expect("serializable connection"),
            identity: locator.identity,
            engine_path: None,
            session_lock_path: None,
        })
    }
    fn bind_connection(
        &mut self,
        locator: ConnectionLocator,
        _: Option<String>,
        expected: Option<String>,
        workspace: Option<Uuid>,
        mode: Mode,
    ) -> grv_adapter_sdk::Result<BoundConnection> {
        if mode != Mode::Extract || workspace.is_some() {
            return unsupported();
        }
        if self.bound.is_some()
            || locator.engine_path.is_some()
            || locator.session_lock_path.is_some()
        {
            return Err(crate::extraction::public(crate::Error::new(
                "PROTOCOL_FAILURE",
                "invalid Salesforce binding lifecycle",
            )));
        }
        let canonical: crate::config::Connection =
            serde_json::from_value(locator.canonical_connection.clone()).map_err(|_| {
                crate::extraction::public(crate::invalid("invalid Salesforce locator"))
            })?;
        let relocated = self.locate_connection(locator.canonical_connection.clone(), mode, None)?;
        if relocated != locator {
            return Err(crate::extraction::public(crate::Error::new(
                "REQUEST_MISMATCH",
                "Salesforce locator changed",
            )));
        }
        let bound = crate::auth::BoundConnection::bind(crate::auth::ConnectionLocator {
            canonical_connection: canonical.clone(),
            identity: locator.identity.clone(),
            engine_path: None,
            session_lock_path: None,
        })
        .map_err(crate::extraction::public)?;
        let handle = Handle::new("salesforce-source").expect("valid handle");
        self.bound = Some((handle.clone(), bound, canonical, expected));
        Ok(BoundConnection {
            handle,
            identity: locator.identity,
            workspace_id: None,
            binding: BindingState::NotApplicable,
            details: json!({}),
        })
    }
    fn authenticate(
        &mut self,
        handle: Handle,
        expected: Option<String>,
    ) -> grv_adapter_sdk::Result<String> {
        let executor = crate::http::CurlHttp {
            program: self.curl_program.clone(),
            supervisor: self.supervisor.clone(),
            cancellation: self.cancellation.clone(),
            timeout: std::time::Duration::from_secs(60),
            resources: self.resources.clone(),
        };
        let mut backend = crate::auth::CliAuthentication {
            program: self.sf_program.clone(),
            supervisor: self.supervisor.clone(),
            verifier: crate::http::SalesforceHttp { executor },
            cancellation: self.cancellation.clone(),
        };
        let bound = self.active(&handle)?;
        if expected
            .as_ref()
            .zip(bound.3.as_ref())
            .is_some_and(|(expected, fixed)| expected != fixed)
        {
            return Err(crate::extraction::public(crate::Error::new(
                "REQUEST_MISMATCH",
                "Salesforce expected identity changed",
            )));
        }
        bound
            .1
            .authenticate(&mut backend, expected.as_deref().or(bound.3.as_deref()))
            .map_err(crate::extraction::public)
    }
    fn extract(
        &mut self,
        handle: Handle,
        request: ExtractRequest,
        stop: &StopToken,
    ) -> grv_adapter_sdk::Result<Box<dyn Extraction>> {
        stop.check()?;
        let bound = self.active(&handle)?;
        let session = bound
            .1
            .shared_session()
            .map_err(crate::extraction::public)?;
        let api_version = bound.2.api_version.clone();
        let state_root = self.home.join(".local/state/grv/adapters/salesforce");
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&state_root)
            .map_err(|_| {
                crate::extraction::public(crate::Error::new(
                    "STATE_CONFLICT",
                    "Salesforce private journal directory cannot be created",
                ))
            })?;
        let journal =
            crate::journal::Journal::open(&state_root).map_err(crate::extraction::public)?;
        let executor = crate::http::CurlHttp {
            program: self.curl_program.clone(),
            supervisor: self.supervisor.clone(),
            cancellation: crate::runtime::Cancellation::from_sdk(stop),
            timeout: std::time::Duration::from_secs(60),
            resources: self.resources.clone(),
        };
        let producer = crate::extraction::RestProducer::new(
            journal,
            request,
            session,
            crate::http::SalesforceHttp { executor },
            api_version,
            &self.resources,
        )
        .map_err(crate::extraction::public)?;
        Ok(Box::new(producer))
    }
    fn stop_and_wait(&mut self) -> grv_adapter_sdk::Result<()> {
        self.cancellation.cancel();
        self.bound = None;
        Ok(())
    }
}
