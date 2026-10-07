use crate::{Error, Result, discovery::Installation, registry::CheckedRegistry};
use grv_adapter_api::*;
use grv_adapter_wire::{Channel, Packet, state::Role};
use grv_types::{ErrorCode, SourceConsistency};
use std::{
    io::{self, Read, Write},
    os::{
        fd::AsRawFd,
        unix::{fs::MetadataExt, net::UnixStream, process::CommandExt},
    },
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

#[derive(Clone, Copy)]
pub struct Deadlines {
    pub bootstrap: Duration,
    pub response: Duration,
    pub grace: Duration,
}
impl Default for Deadlines {
    fn default() -> Self {
        Self {
            bootstrap: Duration::from_secs(30),
            response: Duration::from_secs(300),
            grace: Duration::from_secs(10),
        }
    }
}
pub struct DeadlineSocket {
    socket: UnixStream,
    deadline: Instant,
}
impl DeadlineSocket {
    pub fn new(socket: UnixStream, duration: Duration) -> io::Result<Self> {
        socket.set_nonblocking(true)?;
        Ok(Self {
            socket,
            deadline: Instant::now() + duration,
        })
    }
    pub fn set_deadline(&mut self, duration: Duration) {
        self.deadline = Instant::now() + duration;
    }
    /// Independent shutdown handle for a core ownership-renewal worker. It
    /// never writes frames or shares channel state with that worker.
    pub fn shutdown_handle(&self) -> io::Result<UnixStream> {
        self.socket.try_clone()
    }
    fn remaining(&self) -> io::Result<Duration> {
        let remaining = self
            .deadline
            .checked_duration_since(Instant::now())
            .filter(|d| !d.is_zero())
            .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "adapter deadline exceeded"))?;
        Ok(remaining)
    }
    fn wait_ready(&self, events: libc::c_short) -> io::Result<()> {
        loop {
            let remaining = self.remaining()?;
            let timeout = remaining
                .as_millis()
                .saturating_add(1)
                .min(i32::MAX as u128) as i32;
            let mut poll = libc::pollfd {
                fd: self.socket.as_raw_fd(),
                events,
                revents: 0,
            };
            let result = unsafe { libc::poll(&mut poll, 1, timeout) };
            if result > 0 {
                return Ok(());
            }
            if result < 0 {
                let e = io::Error::last_os_error();
                if e.kind() != io::ErrorKind::Interrupted {
                    return Err(e);
                }
            }
        }
    }
}
impl Read for DeadlineSocket {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        loop {
            self.remaining()?;
            match self.socket.read(b) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => self.wait_ready(libc::POLLIN)?,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                result => return result,
            }
        }
    }
}
impl Write for DeadlineSocket {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        loop {
            self.remaining()?;
            match self.socket.write(b) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.wait_ready(libc::POLLOUT)?
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                result => return result,
            }
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        self.socket.flush()
    }
}

pub struct Session {
    pub channel: Channel<DeadlineSocket>,
    pub registry: CheckedRegistry,
    pub descriptor: AdapterDescriptor,
    pub capabilities: Capabilities,
    child: Child,
    stderr: Option<thread::JoinHandle<()>>,
    stderr_stop: Arc<AtomicBool>,
    pub stderr_truncated: Arc<AtomicBool>,
    deadlines: Deadlines,
    next: u64,
    closed: bool,
    build_discovery: Option<(DiscoverBuildRequest, BuildDiscovery)>,
    build_session: Option<BuildSession>,
    build_completion: Option<Digest>,
}
pub struct CancelOutcome {
    pub state: CancelState,
    pub terminal: Vec<Packet>,
}
pub struct BoundConnection {
    pub handle: Handle,
    pub identity: Option<String>,
    pub workspace_id: Option<Uuid>,
    pub binding: BindingState,
    pub details: serde_json::Value,
}
impl Session {
    pub fn spawn(installation: &Installation, deadlines: Deadlines) -> Result<Self> {
        Self::spawn_at(installation, deadlines, None)
    }
    /// Declaration-relative connection paths resolve in the declaration's
    /// directory without rewriting explicit authoring values.
    pub fn spawn_at(
        installation: &Installation,
        deadlines: Deadlines,
        working_directory: Option<&std::path::Path>,
    ) -> Result<Self> {
        let (executable, verified) = installation.verified_executable()?;
        let (parent, adapter) = UnixStream::pair()?;
        let child_fd = adapter.as_raw_fd();
        let mut command = Command::new(&installation.executable);
        command
            .current_dir(working_directory.unwrap_or(&installation.directory))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .env_clear();
        for key in [
            "PATH",
            "HOME",
            "XDG_CONFIG_HOME",
            "XDG_STATE_HOME",
            "XDG_CACHE_HOME",
            // Nonsecret development artifact coordinates. The adapter verifies
            // pinned hashes/signatures and protected paths before any load.
            "GRV_DUCKDB_EXTENSIONS_DIR",
            "TMPDIR",
            "LANG",
            "LC_ALL",
            "LC_CTYPE",
            "TZ",
        ] {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }
        #[cfg(target_os = "linux")]
        let exe_fd = executable.as_raw_fd();
        #[cfg(target_os = "linux")]
        {
            use std::ffi::CString;
            let argv = vec![
                CString::new(installation.executable.as_os_str().as_encoded_bytes()).map_err(
                    |_| Error::new(ErrorCode::AdapterFailure, "invalid entrypoint path"),
                )?,
            ];
            let env: Vec<CString> = command
                .get_envs()
                .filter_map(|(k, v)| {
                    v.map(|v| {
                        let mut bytes = k.as_encoded_bytes().to_vec();
                        bytes.push(b'=');
                        bytes.extend(v.as_encoded_bytes());
                        CString::new(bytes).unwrap()
                    })
                })
                .collect();
            let mut ap: Vec<usize> = argv.iter().map(|v| v.as_ptr() as usize).collect();
            ap.push(0);
            let mut ep: Vec<usize> = env.iter().map(|v| v.as_ptr() as usize).collect();
            ep.push(0);
            unsafe {
                command.pre_exec(move || {
                    let _keep = (&argv, &env);
                    let exec_copy = libc::fcntl(exe_fd, libc::F_DUPFD_CLOEXEC, 10);
                    if exec_copy < 0 {
                        return Err(io::Error::last_os_error());
                    }
                    if libc::dup2(child_fd, 3) < 0
                        || libc::dup2(exec_copy, 4) < 0
                        || libc::fcntl(3, libc::F_SETFD, 0) < 0
                        || libc::fcntl(4, libc::F_SETFD, libc::FD_CLOEXEC) < 0
                        || libc::setpgid(0, 0) < 0
                    {
                        return Err(io::Error::last_os_error());
                    }
                    close_descriptors(5)?;
                    libc::fexecve(4, ap.as_ptr().cast(), ep.as_ptr().cast());
                    Err(io::Error::last_os_error())
                });
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let path =
                std::ffi::CString::new(installation.executable.as_os_str().as_encoded_bytes())
                    .map_err(|_| {
                        Error::new(ErrorCode::AdapterFailure, "invalid executable path")
                    })?;
            let dev = verified.dev();
            let ino = verified.ino();
            let len = verified.len();
            let mtime = verified.mtime();
            let mtime_nsec = verified.mtime_nsec();
            unsafe {
                command.pre_exec(move || {
                    if libc::dup2(child_fd, 3) < 0
                        || libc::fcntl(3, libc::F_SETFD, 0) < 0
                        || libc::setpgid(0, 0) < 0
                    {
                        return Err(io::Error::last_os_error());
                    }
                    close_descriptors(4)?;
                    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
                    if libc::stat(path.as_ptr(), stat.as_mut_ptr()) != 0 {
                        return Err(io::Error::last_os_error());
                    }
                    let stat = stat.assume_init();
                    if stat.st_dev as u64 != dev
                        || stat.st_ino != ino
                        || stat.st_size as u64 != len
                        || stat.st_mtime != mtime
                        || stat.st_mtime_nsec != mtime_nsec
                    {
                        return Err(io::Error::other("entrypoint changed before execution"));
                    }
                    Ok(())
                });
            }
        }
        let mut child = command.spawn()?;
        drop(adapter);
        drop(executable);
        let stderr_stop = Arc::new(AtomicBool::new(false));
        let stderr_truncated = Arc::new(AtomicBool::new(false));
        let mut stderr = child.stderr.take().unwrap();
        unsafe {
            let flags = libc::fcntl(stderr.as_raw_fd(), libc::F_GETFL);
            libc::fcntl(stderr.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK);
        }
        let stop = stderr_stop.clone();
        let trunc = stderr_truncated.clone();
        let stderr = thread::spawn(move || {
            let mut b = [0u8; 8192];
            let mut total = 0usize;
            while !stop.load(Ordering::Acquire) {
                match stderr.read(&mut b) {
                    Ok(0) => break,
                    Ok(n) => {
                        total = total.saturating_add(n);
                        if total > 1024 * 1024 {
                            trunc.store(true, Ordering::Release);
                        }
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5))
                    }
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(_) => break,
                }
            }
        });
        let resources = Resources::default();
        let mut channel = Channel::new(
            DeadlineSocket::new(parent, deadlines.bootstrap)?,
            Role::Parent,
            resources.max_batch_bytes.get() as usize,
        );
        let hello = Frame::Hello {
            interface_versions: vec![Req::new(1).unwrap()],
            core: CoreIdentity {
                name: "grv".into(),
                version: env!("CARGO_PKG_VERSION").into(),
            },
            attempt: Uuid::v4(),
            resources: resources.clone(),
        };
        let bootstrap = (|| -> Result<(CheckedRegistry, AdapterDescriptor, Capabilities)> {
            channel.send(&hello, &[])?;
            let Frame::Identified {
                name,
                package_version,
                interface_versions,
                binding_schema_version,
                capabilities,
                registry,
                commands,
            } = channel.receive()?.frame
            else {
                return Err(Error::new(
                    ErrorCode::ProtocolFailure,
                    "expected identified",
                ));
            };
            let manifest = &installation.manifest;
            let mut offered = interface_versions.clone();
            offered.sort();
            let mut installed = manifest.interface_versions.clone();
            installed.sort();
            if name != manifest.name
                || package_version != manifest.version
                || offered != installed
                || binding_schema_version != manifest.binding_schema_version
            {
                return Err(Error::new(
                    ErrorCode::AdapterFailure,
                    "manifest and handshake identities differ",
                ));
            }
            if !interface_versions.iter().any(|v| v.get() == 1) {
                return Err(Error::new(
                    ErrorCode::UnsupportedCapability,
                    "no common adapter interface",
                ));
            }
            let public = PublicCapabilities {
                push: capabilities.push,
                pull: capabilities.pull,
                source_consistency: SourceConsistency::from(capabilities.source_consistency),
                resumable_extract: capabilities.resumable_extract,
                pull_write_modes: capabilities.pull_write_modes.clone(),
                commands: commands.iter().map(|c| c.name.clone()).collect(),
                managed_build: capabilities.managed_build,
                external_build: capabilities.external_build,
            };
            let registry = CheckedRegistry::new(registry, commands, &capabilities)?;
            channel.send(
                &Frame::Ready {
                    interface_version: Req::new(1).unwrap(),
                    binding_schema_version,
                    resources,
                },
                &[],
            )?;
            Ok((
                registry,
                AdapterDescriptor {
                    name,
                    package_version,
                    interface_version: Req::new(1).unwrap(),
                    binding_schema_version,
                    capabilities: public,
                },
                capabilities,
            ))
        })();
        let (registry, descriptor, capabilities) = match bootstrap {
            Ok(v) => v,
            Err(e) => {
                terminate(&mut child, deadlines.grace);
                stderr_stop.store(true, Ordering::Release);
                let _ = stderr.join();
                return Err(e);
            }
        };
        Ok(Self {
            channel,
            registry,
            descriptor,
            capabilities,
            child,
            stderr: Some(stderr),
            stderr_stop,
            stderr_truncated,
            deadlines,
            next: 1,
            closed: false,
            build_discovery: None,
            build_session: None,
            build_completion: None,
        })
    }
    pub fn request_id(&mut self) -> Result<Req> {
        let req = Req::new(self.next)
            .map_err(|_| Error::new(ErrorCode::ProtocolFailure, "request IDs exhausted"))?;
        self.next += 1;
        Ok(req)
    }
    pub fn send(&mut self, frame: &Frame, payload: &[u8]) -> Result<()> {
        if let Frame::Extract {
            payload: Doc::Inline(request),
            ..
        } = frame
        {
            self.registry.validate_point(
                Mode::Extract,
                "options",
                &request.inline.options,
                ErrorCode::InvalidDeclaration,
            )?;
            for table in &request.inline.tables {
                self.registry.validate_point(
                    Mode::Extract,
                    "table_source",
                    &table.source,
                    ErrorCode::InvalidDeclaration,
                )?;
                let columns = table.columns.as_array().ok_or_else(|| {
                    Error::new(
                        ErrorCode::InvalidDeclaration,
                        "extraction columns must be an array",
                    )
                })?;
                for column in columns {
                    let selector = column
                        .get("source")
                        .or_else(|| column.get("name"))
                        .unwrap_or(column);
                    self.registry.validate_point(
                        Mode::Extract,
                        "column_source",
                        selector,
                        ErrorCode::InvalidDeclaration,
                    )?;
                }
            }
        }
        self.channel
            .codec
            .io_mut()
            .set_deadline(self.deadlines.response);
        if serde_json::to_vec(frame)
            .map_err(|_| Error::new(ErrorCode::ProtocolFailure, "frame encoding failed"))?
            .len()
            < grv_adapter_wire::FRAME_LIMIT
        {
            return Ok(self.channel.send(frame, payload)?);
        }
        let mut value = grv_adapter_wire::value(frame);
        let obj = value.as_object_mut().unwrap();
        let key = obj
            .iter()
            .find(|(_, v)| v.is_object() && v.get("inline").is_some())
            .map(|(k, _)| k.clone())
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::InvalidArgument,
                    "frame metadata exceeds supported limit",
                )
            })?;
        let bytes = grv_types::canonical_json(&obj[&key]["inline"]).map_err(|_| {
            Error::new(
                ErrorCode::InvalidArgument,
                "metadata is not canonicalizable",
            )
        })?;
        if bytes.len() > grv_adapter_wire::DOCUMENT_LIMIT {
            return Err(Error::new(
                ErrorCode::InvalidDeclaration,
                "expanded metadata exceeds 64MiB",
            ));
        }
        let document_id = Uuid::v4();
        self.channel.send(
            &Frame::DocumentBegin {
                document_id: document_id.clone(),
                size: U64::new(bytes.len() as u64).unwrap(),
                sha256: grv_types::sha256(&bytes),
            },
            &[],
        )?;
        use base64::Engine;
        for (i, chunk) in bytes.chunks(512 * 1024).enumerate() {
            self.channel.send(
                &Frame::DocumentChunk {
                    document_id: document_id.clone(),
                    index: SafeInt::new(i as u64).unwrap(),
                    data: base64::engine::general_purpose::STANDARD.encode(chunk),
                },
                &[],
            )?;
        }
        self.channel.send(
            &Frame::DocumentEnd {
                document_id: document_id.clone(),
            },
            &[],
        )?;
        if !matches!(self.receive()?.frame,Frame::DocumentAck{document_id:id} if id==document_id) {
            return Err(Error::new(
                ErrorCode::ProtocolFailure,
                "metadata acknowledgement missing",
            ));
        }
        obj.insert(key, serde_json::json!({"document_id":document_id}));
        let frame: Frame = serde_json::from_value(value)
            .map_err(|_| Error::new(ErrorCode::ProtocolFailure, "metadata reference invalid"))?;
        Ok(self.channel.send(&frame, payload)?)
    }
    pub fn receive(&mut self) -> Result<Packet> {
        loop {
            let p = self.channel.receive()?;
            if let Frame::DocumentEnd { document_id } = p.frame {
                self.channel
                    .send(&Frame::DocumentAck { document_id }, &[])?;
                continue;
            }
            if matches!(
                p.frame,
                Frame::DocumentBegin { .. } | Frame::DocumentChunk { .. }
            ) {
                continue;
            }
            if let Frame::Error {
                code,
                message,
                object,
                retryable,
                ..
            } = p.frame
            {
                return Err(Error {
                    code,
                    message,
                    object: object.map(Box::new),
                    retryable,
                });
            }
            if let Frame::ExportComplete {
                adapter_result: Doc::Inline(result),
                ..
            } = &p.frame
            {
                let session = self.build_session.as_ref().ok_or_else(|| {
                    Error::new(
                        ErrorCode::ProtocolFailure,
                        "export result has no fixed session",
                    )
                })?;
                self.registry.validate_point(
                    session.execution.mode(),
                    "push_result",
                    &result.inline,
                    ErrorCode::ProtocolFailure,
                )?;
            }
            match &p.frame {
                Frame::Checkpoint {
                    payload: Doc::Inline(checkpoint),
                    ..
                } => {
                    self.registry.validate_point(
                        Mode::Extract,
                        "source_job",
                        &checkpoint.inline.job,
                        ErrorCode::ProtocolFailure,
                    )?;
                    for table in &checkpoint.inline.tables {
                        self.registry.validate_point(
                            Mode::Extract,
                            "source_identity",
                            &table.source_identity,
                            ErrorCode::ProtocolFailure,
                        )?;
                    }
                }
                Frame::TableComplete {
                    source_identity: Doc::Inline(source),
                    ..
                } => {
                    self.registry.validate_point(
                        Mode::Extract,
                        "source_identity",
                        &source.inline,
                        ErrorCode::ProtocolFailure,
                    )?;
                }
                Frame::SourceComplete {
                    completion: Doc::Inline(completion),
                    ..
                } => {
                    self.registry.validate_point(
                        Mode::Extract,
                        "source_job",
                        &completion.inline.job,
                        ErrorCode::ProtocolFailure,
                    )?;
                    self.registry.validate_point(
                        Mode::Extract,
                        "push_result",
                        &completion.inline.adapter_result,
                        ErrorCode::ProtocolFailure,
                    )?;
                }
                _ => {}
            }
            return Ok(p);
        }
    }
    pub fn validate_binding(
        &mut self,
        declaration: serde_json::Value,
        mode: Mode,
    ) -> Result<serde_json::Value> {
        let req = self.request_id()?;
        self.send(
            &Frame::ValidateBinding {
                req,
                declaration: Doc::inline(declaration),
                mode,
                schema_version: self.descriptor.binding_schema_version,
            },
            &[],
        )?;
        match self.receive()?.frame {
            Frame::ValidateResult {
                effective_declaration: Doc::Inline(result),
                ..
            } => Ok(result.inline),
            _ => Err(Error::new(
                ErrorCode::ProtocolFailure,
                "adapter did not return an effective declaration",
            )),
        }
    }
    /// Read-only inspection never authenticates or establishes an engine binding.
    pub fn inspect_connection(
        &mut self,
        handle: Handle,
        request: InspectConnectionRequest,
    ) -> Result<serde_json::Value> {
        if !self.capabilities.inspect_connection {
            return Err(Error::new(
                ErrorCode::UnsupportedCapability,
                "adapter does not support connection inspection",
            ));
        }
        request
            .validate()
            .map_err(|e| Error::new(ErrorCode::InvalidArgument, e.to_string()))?;
        let req = self.request_id()?;
        self.send(
            &Frame::InspectConnection {
                req,
                handle,
                root: request.root,
                declaration: request.declaration.map(Doc::inline),
            },
            &[],
        )?;
        match self.receive()?.frame {
            Frame::InspectResult {
                details: Doc::Inline(result),
                ..
            } => {
                self.registry.validate_point(
                    Mode::Inspect,
                    "inspection_result",
                    &result.inline,
                    ErrorCode::ProtocolFailure,
                )?;
                Ok(result.inline)
            }
            _ => Err(Error::new(
                ErrorCode::ProtocolFailure,
                "adapter did not return connection inspection details",
            )),
        }
    }
    /// Offline lookup only. Authentication is a separate typed lifecycle call.
    pub fn locate_connection(
        &mut self,
        connection: serde_json::Value,
        mode: Mode,
        run_id: Option<RunId>,
    ) -> Result<ConnectionLocator> {
        self.registry.validate_point(
            mode,
            "connection",
            &connection,
            ErrorCode::InvalidDeclaration,
        )?;
        let req = self.request_id()?;
        self.send(
            &Frame::LocateConnection {
                req,
                connection: Doc::inline(connection),
                mode,
                run_id,
            },
            &[],
        )?;
        match self.receive()?.frame {
            Frame::ConnectionLocated {
                connection: Doc::Inline(result),
                ..
            } => {
                self.registry.validate_point(
                    mode,
                    "connection",
                    &result.inline.canonical_connection,
                    ErrorCode::ProtocolFailure,
                )?;
                Ok(result.inline)
            }
            _ => Err(Error::new(
                ErrorCode::ProtocolFailure,
                "adapter did not return a connection locator",
            )),
        }
    }
    pub fn bind_connection(
        &mut self,
        locator: ConnectionLocator,
        root: Option<String>,
        expected_identity: Option<String>,
        expected_workspace_id: Option<Uuid>,
        mode: Mode,
    ) -> Result<BoundConnection> {
        let req = self.request_id()?;
        self.send(
            &Frame::BindConnection {
                req,
                locator: Doc::inline(locator),
                root,
                expected_identity: expected_identity.clone(),
                expected_workspace_id: expected_workspace_id.clone(),
                mode,
            },
            &[],
        )?;
        match self.receive()?.frame {
            Frame::BindResult {
                handle,
                identity,
                workspace_id,
                binding,
                details: Doc::Inline(details),
                ..
            } => {
                if mode == Mode::Pull && identity.is_none() {
                    return Err(Error::new(
                        ErrorCode::ProtocolFailure,
                        "pull binding requires an offline connection identity",
                    ));
                }
                if expected_identity
                    .as_ref()
                    .is_some_and(|id| identity.as_ref().is_some_and(|actual| actual != id))
                    || (binding != BindingState::Uninitialized
                        && expected_workspace_id
                            .as_ref()
                            .is_some_and(|id| workspace_id.as_ref() != Some(id)))
                {
                    return Err(Error::new(
                        ErrorCode::RequestMismatch,
                        "bound connection identity differs from fixed request",
                    ));
                }
                self.registry.validate_point(
                    mode,
                    "connection_details",
                    &details.inline,
                    ErrorCode::ProtocolFailure,
                )?;
                Ok(BoundConnection {
                    handle,
                    identity,
                    workspace_id,
                    binding,
                    details: details.inline,
                })
            }
            _ => Err(Error::new(
                ErrorCode::ProtocolFailure,
                "adapter did not return a bound connection",
            )),
        }
    }
    pub fn authenticate(
        &mut self,
        handle: Handle,
        expected_identity: Option<String>,
    ) -> Result<String> {
        let req = self.request_id()?;
        self.send(
            &Frame::Authenticate {
                req,
                handle,
                expected_identity: expected_identity.clone(),
            },
            &[],
        )?;
        match self.receive()?.frame {
            Frame::AuthenticateResult { identity, .. } => {
                if expected_identity
                    .as_ref()
                    .is_some_and(|expected| expected != &identity)
                {
                    return Err(Error::new(
                        ErrorCode::RequestMismatch,
                        "authenticated connection identity differs from fixed request",
                    ));
                }
                Ok(identity)
            }
            _ => Err(Error::new(
                ErrorCode::ProtocolFailure,
                "adapter did not return an authenticated identity",
            )),
        }
    }
    /// Caller must durably record the fixed pending hook alongside its known
    /// outcome before calling. Success permits durable clearing; errors never
    /// grant publication or source acquisition authority.
    pub fn after_publish(&mut self, handle: Handle, request: AfterPublishRequest) -> Result<()> {
        if !self.capabilities.after_publish {
            return Err(Error::new(
                ErrorCode::UnsupportedCapability,
                "adapter has no after-publish hook",
            ));
        }
        request
            .validate()
            .map_err(|e| Error::new(ErrorCode::InvalidArgument, e.to_string()))?;
        let req = self.request_id()?;
        self.send(
            &Frame::AfterPublish {
                req,
                handle,
                attempt_id: request.attempt_id,
                declaration_sha256: request.declaration_sha256,
                outcome: request.outcome,
            },
            &[],
        )?;
        match self.receive()?.frame {
            Frame::AfterPublishResult {
                acknowledged: true, ..
            } => Ok(()),
            _ => Err(Error::new(
                ErrorCode::ProtocolFailure,
                "adapter did not acknowledge the after-publish hook",
            )),
        }
    }
    pub fn resolve_pull(
        &mut self,
        handle: Handle,
        query: ResolvePullRequest,
    ) -> Result<PullResolution> {
        query
            .validate()
            .map_err(|e| Error::new(ErrorCode::InvalidArgument, e.to_string()))?;
        let req = self.request_id()?;
        self.send(
            &Frame::ResolvePull {
                req,
                handle,
                payload: Doc::inline(query.clone()),
            },
            &[],
        )?;
        let Frame::ResolveResult {
            resolution: Doc::Inline(result),
            ..
        } = self.receive()?.frame
        else {
            return Err(Error::new(
                ErrorCode::ProtocolFailure,
                "expected pull resolution",
            ));
        };
        let resolution = result.inline;
        resolution
            .validate_for(&query)
            .map_err(|e| Error::new(ErrorCode::ProtocolFailure, e.to_string()))?;
        if let Some(recovery) = &resolution.recovery {
            self.registry.validate_point(
                Mode::Pull,
                "pull_plan",
                recovery,
                ErrorCode::ProtocolFailure,
            )?;
        }
        if let Some(receipt) = &resolution.receipt {
            Self::validate_recorded_receipt(receipt)?;
        }
        Ok(resolution)
    }
    fn validate_recorded_receipt(receipt: &Receipt) -> Result<()> {
        CheckedRegistry::recorded(receipt.request.registry.clone())?.validate_point(
            Mode::Pull,
            "pull_result",
            &receipt.adapter_result,
            ErrorCode::ProtocolFailure,
        )
    }
    pub fn prepare_pull(
        &mut self,
        handle: Handle,
        request: PreparePullRequest,
    ) -> Result<PullPlan> {
        request
            .validate()
            .map_err(|e| Error::new(ErrorCode::InvalidArgument, e.to_string()))?;
        for table in &request.tables {
            self.registry.validate_point(
                Mode::Pull,
                "table_target",
                &table.target,
                ErrorCode::InvalidDeclaration,
            )?;
            if let Some(select) = &table.select {
                self.registry.validate_point(
                    Mode::Pull,
                    "table_select",
                    select,
                    ErrorCode::InvalidDeclaration,
                )?;
            }
        }
        let req = self.request_id()?;
        self.send(
            &Frame::PreparePull {
                req,
                handle,
                payload: Doc::inline(request.clone()),
            },
            &[],
        )?;
        let Frame::PrepareResult {
            plan: Doc::Inline(result),
            ..
        } = self.receive()?.frame
        else {
            return Err(Error::new(
                ErrorCode::ProtocolFailure,
                "expected prepared pull plan",
            ));
        };
        let plan = result.inline;
        plan.validate()
            .map_err(|e| Error::new(ErrorCode::ProtocolFailure, e.to_string()))?;
        if plan.request != request.request
            || plan.tables != request.tables
            || plan.files != request.files
            || plan.resolved_revision != request.resolved_revision
            || Some(plan.recovery_contract) != self.capabilities.pull_recovery
        {
            return Err(Error::new(
                ErrorCode::ProtocolFailure,
                "prepared plan changed verified inputs or recovery capability",
            ));
        }
        self.registry.validate_point(
            Mode::Pull,
            "pull_plan",
            &plan.adapter_details,
            ErrorCode::ProtocolFailure,
        )?;
        Ok(plan)
    }
    pub fn apply_pull(&mut self, handle: Handle, plan: PullPlan) -> Result<Receipt> {
        plan.validate()
            .map_err(|e| Error::new(ErrorCode::InvalidArgument, e.to_string()))?;
        let req = self.request_id()?;
        self.send(
            &Frame::ApplyPull {
                req,
                handle,
                plan: Doc::inline(plan.clone()),
            },
            &[],
        )?;
        let Frame::ApplyResult {
            receipt: Doc::Inline(result),
            ..
        } = self.receive()?.frame
        else {
            return Err(Error::new(
                ErrorCode::ProtocolFailure,
                "expected immutable pull receipt",
            ));
        };
        let receipt = result.inline;
        receipt
            .validate_for(&plan)
            .map_err(|e| Error::new(ErrorCode::ProtocolFailure, e.to_string()))?;
        Self::validate_recorded_receipt(&receipt)?;
        Ok(receipt)
    }
    fn build_supported(&self, execution: BuildExecution) -> Result<()> {
        if match execution {
            BuildExecution::Managed => self.capabilities.managed_build,
            BuildExecution::External => self.capabilities.external_build,
        } {
            Ok(())
        } else {
            Err(Error::new(
                ErrorCode::UnsupportedCapability,
                "adapter does not implement this build lifecycle",
            ))
        }
    }
    fn validate_build_session_points(&self, session: &BuildSession) -> Result<()> {
        let mode = session.execution.mode();
        self.registry.validate_point(
            mode,
            "options",
            &session.options,
            ErrorCode::ProtocolFailure,
        )?;
        self.registry.validate_point(
            mode,
            "session_details",
            &session.adapter_details,
            ErrorCode::ProtocolFailure,
        )?;
        for input in &session.inputs {
            self.registry.validate_point(
                mode,
                "build_input",
                &input.relation,
                ErrorCode::ProtocolFailure,
            )?;
        }
        for output in &session.outputs {
            self.registry.validate_point(
                mode,
                "table_source",
                &output.source,
                ErrorCode::ProtocolFailure,
            )?;
            for column in output.columns.as_array().expect("validated build columns") {
                self.registry.validate_point(
                    mode,
                    "column_source",
                    column
                        .get("source")
                        .or_else(|| column.get("name"))
                        .expect("validated build mapping"),
                    ErrorCode::ProtocolFailure,
                )?;
            }
        }
        Ok(())
    }
    fn build_session(&self, id: &Uuid) -> Result<&BuildSession> {
        self.build_session
            .as_ref()
            .filter(|s| &s.session_id == id)
            .ok_or_else(|| Error::new(ErrorCode::InvalidArgument, "unknown build session"))
    }
    pub fn discover_build(
        &mut self,
        handle: Handle,
        request: DiscoverBuildRequest,
    ) -> Result<BuildDiscovery> {
        self.build_supported(request.execution)?;
        request
            .validate()
            .map_err(|e| Error::new(ErrorCode::InvalidDeclaration, e.to_string()))?;
        if self.build_discovery.is_some() || self.build_session.is_some() {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                "build discovery already reserved",
            ));
        }
        self.registry.validate_point(
            request.execution.mode(),
            "options",
            &request.options,
            ErrorCode::InvalidDeclaration,
        )?;
        for input in &request.inputs {
            self.registry.validate_point(
                request.execution.mode(),
                "build_input",
                &input.relation,
                ErrorCode::InvalidDeclaration,
            )?;
        }
        for output in &request.outputs {
            self.registry.validate_point(
                request.execution.mode(),
                "table_source",
                &output.source,
                ErrorCode::InvalidDeclaration,
            )?;
            for column in output.columns.as_array().expect("validated build mappings") {
                self.registry.validate_point(
                    request.execution.mode(),
                    "column_source",
                    column
                        .get("source")
                        .or_else(|| column.get("name"))
                        .expect("validated build name"),
                    ErrorCode::InvalidDeclaration,
                )?;
            }
        }
        let req = self.request_id()?;
        self.send(
            &Frame::DiscoverBuild {
                req,
                handle,
                payload: Doc::inline(request.clone()),
            },
            &[],
        )?;
        let Frame::BuildDiscovered {
            discovery: Doc::Inline(discovery),
            ..
        } = self.receive()?.frame
        else {
            return Err(Error::new(
                ErrorCode::ProtocolFailure,
                "expected fixed build discovery",
            ));
        };
        discovery
            .inline
            .validate_for(&request)
            .map_err(|e| Error::new(ErrorCode::ProtocolFailure, e.to_string()))?;
        self.build_discovery = Some((request, discovery.inline.clone()));
        Ok(discovery.inline)
    }
    pub fn prepare_build(
        &mut self,
        handle: Handle,
        preparation: PrepareBuildRequest,
    ) -> Result<BuildSession> {
        preparation
            .validate()
            .map_err(|e| Error::new(ErrorCode::InvalidArgument, e.to_string()))?;
        let (request, discovery) = self
            .build_discovery
            .as_ref()
            .ok_or_else(|| Error::new(ErrorCode::InvalidArgument, "no retained build discovery"))?;
        if discovery != &preparation.discovery
            || request.self_input != preparation.self_input
            || self.build_session.is_some()
        {
            return Err(Error::new(
                ErrorCode::RequestMismatch,
                "preparation changed retained discovery",
            ));
        }
        let request = request.clone();
        let req = self.request_id()?;
        self.send(
            &Frame::PrepareBuild {
                req,
                handle,
                payload: Doc::inline(preparation.clone()),
            },
            &[],
        )?;
        let Frame::BuildPrepared {
            session: Doc::Inline(session),
            ..
        } = self.receive()?.frame
        else {
            return Err(Error::new(
                ErrorCode::ProtocolFailure,
                "expected durable build preparation",
            ));
        };
        session
            .inline
            .validate_for(&request, &preparation)
            .map_err(|e| Error::new(ErrorCode::ProtocolFailure, e.to_string()))?;
        self.registry.validate_point(
            session.inline.execution.mode(),
            "session_details",
            &session.inline.adapter_details,
            ErrorCode::ProtocolFailure,
        )?;
        self.validate_build_session_points(&session.inline)?;
        self.build_session = Some(session.inline.clone());
        self.build_discovery = None;
        Ok(session.inline)
    }
    pub fn execute_build(
        &mut self,
        handle: Handle,
        request: ExecuteBuildRequest,
    ) -> Result<BuildExecutionResult> {
        self.build_supported(BuildExecution::Managed)?;
        request
            .validate()
            .map_err(|e| Error::new(ErrorCode::InvalidDeclaration, e.to_string()))?;
        if self.build_session.as_ref() != Some(&request.session) {
            return Err(Error::new(
                ErrorCode::RequestMismatch,
                "execution changed prepared session",
            ));
        }
        let req = self.request_id()?;
        self.send(
            &Frame::ExecuteBuild {
                req,
                handle,
                payload: Doc::inline(request.clone()),
            },
            &[],
        )?;
        let Frame::BuildFinished {
            result: Doc::Inline(result),
            ..
        } = self.receive()?.frame
        else {
            return Err(Error::new(
                ErrorCode::ProtocolFailure,
                "expected stopped build execution result",
            ));
        };
        result
            .inline
            .validate_for(&request.session)
            .map_err(|e| Error::new(ErrorCode::ProtocolFailure, e.to_string()))?;
        Ok(result.inline)
    }
    pub fn accept_build_completion(
        &mut self,
        handle: Handle,
        session_id: Uuid,
        completion: BuildCompletion,
    ) -> Result<Digest> {
        completion
            .validate_for(self.build_session(&session_id)?)
            .map_err(|e| Error::new(ErrorCode::RequestMismatch, e.to_string()))?;
        let expected = completion
            .digest()
            .map_err(|e| Error::new(ErrorCode::InvalidArgument, e.to_string()))?;
        let req = self.request_id()?;
        self.send(
            &Frame::AcceptBuildCompletion {
                req,
                handle,
                session_id: session_id.clone(),
                completion: Doc::inline(completion),
            },
            &[],
        )?;
        let Frame::CompletionAccepted {
            session_id: returned,
            completion_sha256,
            ..
        } = self.receive()?.frame
        else {
            return Err(Error::new(
                ErrorCode::ProtocolFailure,
                "expected immutable completion acceptance",
            ));
        };
        if returned != session_id || completion_sha256 != expected {
            return Err(Error::new(
                ErrorCode::ProtocolFailure,
                "accepted completion identity differs",
            ));
        }
        self.build_completion = Some(completion_sha256.clone());
        Ok(completion_sha256)
    }
    /// Starts the outbound build stream. The caller durably stores batches
    /// before acknowledging them and consumes BuildTableComplete/ExportComplete.
    pub fn export_build(
        &mut self,
        handle: Handle,
        session_id: Uuid,
        completion_sha256: Digest,
        stream_id: Uuid,
    ) -> Result<Req> {
        self.build_session(&session_id)?;
        if self.build_completion.as_ref() != Some(&completion_sha256) {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                "export requires accepted completion",
            ));
        }
        let req = self.request_id()?;
        self.send(
            &Frame::ExportBuild {
                req,
                handle,
                session_id,
                completion_sha256,
                stream_id,
            },
            &[],
        )?;
        Ok(req)
    }
    pub fn open_build(&mut self, handle: Handle, identity: BuildIdentity) -> Result<BuildRecord> {
        self.read_build(handle, identity, false)
    }
    pub fn inspect_build(
        &mut self,
        handle: Handle,
        identity: BuildIdentity,
    ) -> Result<BuildRecord> {
        self.read_build(handle, identity, true)
    }
    fn read_build(
        &mut self,
        handle: Handle,
        identity: BuildIdentity,
        inspect: bool,
    ) -> Result<BuildRecord> {
        identity
            .validate()
            .map_err(|e| Error::new(ErrorCode::InvalidArgument, e.to_string()))?;
        if !self.capabilities.managed_build && !self.capabilities.external_build {
            return Err(Error::new(
                ErrorCode::UnsupportedCapability,
                "adapter does not implement builds",
            ));
        }
        let req = self.request_id()?;
        let frame = if inspect {
            Frame::InspectBuild {
                req,
                handle,
                payload: Doc::inline(identity.clone()),
            }
        } else {
            Frame::OpenBuild {
                req,
                handle,
                payload: Doc::inline(identity.clone()),
            }
        };
        self.send(&frame, &[])?;
        let record = match self.receive()?.frame {
            Frame::BuildOpened {
                record: Doc::Inline(record),
                ..
            } if !inspect => record.inline,
            Frame::BuildInspected {
                record: Doc::Inline(record),
                ..
            } if inspect => record.inline,
            _ => {
                return Err(Error::new(
                    ErrorCode::ProtocolFailure,
                    "expected original build record",
                ));
            }
        };
        record
            .validate()
            .map_err(|e| Error::new(ErrorCode::ProtocolFailure, e.to_string()))?;
        self.build_supported(record.session.execution)?;
        self.registry.validate_point(
            record.session.execution.mode(),
            "options",
            &record.session.options,
            ErrorCode::ProtocolFailure,
        )?;
        if record.session.identity != identity
            || self
                .build_session
                .as_ref()
                .is_some_and(|s| s != &record.session)
        {
            return Err(Error::new(
                ErrorCode::RequestMismatch,
                "build record changed original identity",
            ));
        }
        self.registry.validate_point(
            record.session.execution.mode(),
            "session_details",
            &record.session.adapter_details,
            ErrorCode::ProtocolFailure,
        )?;
        self.validate_build_session_points(&record.session)?;
        if !inspect {
            self.build_session = Some(record.session.clone());
            self.build_completion = record.completion_sha256.clone();
        }
        Ok(record)
    }
    pub fn abort_build(&mut self, handle: Handle, session_id: Uuid) -> Result<()> {
        self.build_session(&session_id)?;
        let req = self.request_id()?;
        self.send(
            &Frame::AbortBuild {
                req,
                handle,
                session_id: session_id.clone(),
            },
            &[],
        )?;
        match self.receive()?.frame {
            Frame::BuildAborted {
                session_id: returned,
                writers_stopped: true,
                ..
            } if returned == session_id => Ok(()),
            _ => Err(Error::new(
                ErrorCode::ProtocolFailure,
                "expected stopped build abort",
            )),
        }
    }
    pub fn record_build_outcome(
        &mut self,
        handle: Handle,
        session_id: Uuid,
        outcome: BuildOutcome,
    ) -> Result<()> {
        self.build_session(&session_id)?;
        outcome
            .validate()
            .map_err(|e| Error::new(ErrorCode::InvalidArgument, e.to_string()))?;
        let req = self.request_id()?;
        self.send(
            &Frame::RecordBuildOutcome {
                req,
                handle,
                session_id: session_id.clone(),
                outcome,
            },
            &[],
        )?;
        match self.receive()?.frame {
            Frame::BuildOutcomeRecorded {
                session_id: returned,
                ..
            } if returned == session_id => Ok(()),
            _ => Err(Error::new(
                ErrorCode::ProtocolFailure,
                "expected build outcome acknowledgement",
            )),
        }
    }
    pub fn cleanup_build(&mut self, handle: Handle, session_id: Uuid) -> Result<()> {
        self.build_session(&session_id)?;
        let req = self.request_id()?;
        self.send(
            &Frame::CleanupBuild {
                req,
                handle,
                session_id: session_id.clone(),
            },
            &[],
        )?;
        match self.receive()?.frame {
            Frame::BuildCleaned {
                session_id: returned,
                ..
            } if returned == session_id => Ok(()),
            _ => Err(Error::new(
                ErrorCode::ProtocolFailure,
                "expected build cleanup acknowledgement",
            )),
        }
    }
    pub fn command(&mut self, name: Name, argv: Vec<String>) -> Result<AdapterCommandResult> {
        let descriptor = self
            .registry
            .commands
            .iter()
            .find(|c| c.name == name)
            .cloned()
            .ok_or_else(|| Error::new(ErrorCode::InvalidArgument, "unknown adapter command"))?;
        let req = self.request_id()?;
        self.send(
            &Frame::PrepareCommand {
                req,
                name: name.clone(),
                argv: Doc::inline(argv),
            },
            &[],
        )?;
        let Frame::CommandPrepared {
            call: Doc::Inline(call),
            ..
        } = self.receive()?.frame
        else {
            return Err(Error::new(
                ErrorCode::ProtocolFailure,
                "expected prepared command",
            ));
        };
        let call = call.inline;
        if call.name != name || call.connection.is_some() != descriptor.requires_connection {
            return Err(Error::new(
                ErrorCode::ProtocolFailure,
                "prepared command contradicts descriptor",
            ));
        }
        self.registry.validate(
            &descriptor.args_schema_pointer,
            &call.args,
            ErrorCode::InvalidArgument,
        )?;
        let handle = if let Some(connection) = call.connection {
            self.registry.validate_point(
                Mode::Command,
                "connection",
                &connection,
                ErrorCode::InvalidArgument,
            )?;
            let req = self.request_id()?;
            self.send(
                &Frame::LocateConnection {
                    req,
                    connection: Doc::inline(connection),
                    mode: Mode::Command,
                    run_id: None,
                },
                &[],
            )?;
            let Frame::ConnectionLocated {
                connection: Doc::Inline(locator),
                ..
            } = self.receive()?.frame
            else {
                return Err(Error::new(
                    ErrorCode::ProtocolFailure,
                    "expected connection locator",
                ));
            };
            self.registry.validate_point(
                Mode::Command,
                "connection",
                &locator.inline.canonical_connection,
                ErrorCode::ProtocolFailure,
            )?;
            let req = self.request_id()?;
            self.send(
                &Frame::BindConnection {
                    req,
                    locator: Doc::inline(locator.inline),
                    root: None,
                    expected_identity: None,
                    expected_workspace_id: None,
                    mode: Mode::Command,
                },
                &[],
            )?;
            let Frame::BindResult {
                handle,
                details: Doc::Inline(details),
                ..
            } = self.receive()?.frame
            else {
                return Err(Error::new(
                    ErrorCode::ProtocolFailure,
                    "expected bind result",
                ));
            };
            self.registry.validate_point(
                Mode::Command,
                "connection_details",
                &details.inline,
                ErrorCode::ProtocolFailure,
            )?;
            if descriptor.requires_authentication {
                let req = self.request_id()?;
                self.send(
                    &Frame::Authenticate {
                        req,
                        handle: handle.clone(),
                        expected_identity: None,
                    },
                    &[],
                )?;
                if !matches!(self.receive()?.frame, Frame::AuthenticateResult { .. }) {
                    return Err(Error::new(
                        ErrorCode::ProtocolFailure,
                        "expected authentication result",
                    ));
                }
            }
            Some(handle)
        } else {
            None
        };
        let req = self.request_id()?;
        self.send(
            &Frame::Command {
                req,
                command_id: call.command_id,
                handle,
            },
            &[],
        )?;
        let Frame::CommandResult {
            details: Doc::Inline(details),
            ..
        } = self.receive()?.frame
        else {
            return Err(Error::new(
                ErrorCode::ProtocolFailure,
                "expected command result",
            ));
        };
        self.registry.validate(
            &descriptor.result_schema_pointer,
            &details.inline,
            ErrorCode::ProtocolFailure,
        )?;
        Ok(AdapterCommandResult {
            adapter: self.descriptor.name.clone(),
            package_version: self.descriptor.package_version.clone(),
            adapter_command: name,
            details: details.inline,
        })
    }
    pub fn close(&mut self) -> Result<()> {
        let req = self.request_id()?;
        self.channel
            .codec
            .io_mut()
            .set_deadline(self.deadlines.grace);
        self.channel.send(&Frame::Close { req }, &[])?;
        if !matches!(self.receive()?.frame, Frame::CloseResult { .. }) {
            return Err(Error::new(
                ErrorCode::ProtocolFailure,
                "expected close result",
            ));
        }
        let deadline = Instant::now() + self.deadlines.grace;
        loop {
            if let Some(status) = self.child.try_wait()? {
                let remaining = group_exists(self.child.id());
                if remaining {
                    terminate(&mut self.child, self.deadlines.grace);
                }
                self.closed = true;
                self.finish_stderr();
                return if status.success() && !remaining {
                    Ok(())
                } else {
                    Err(Error::new(
                        ErrorCode::AdapterFailure,
                        "adapter shutdown left abnormal exit or supervised descendants",
                    ))
                };
            }
            if Instant::now() >= deadline {
                terminate(&mut self.child, self.deadlines.grace);
                self.closed = true;
                self.finish_stderr();
                return Err(Error::new(
                    ErrorCode::AdapterFailure,
                    "adapter did not exit after close",
                ));
            }
            thread::sleep(Duration::from_millis(5));
        }
    }
    pub fn cancel(&mut self, req: Req) -> Result<CancelOutcome> {
        self.channel
            .codec
            .io_mut()
            .set_deadline(self.deadlines.grace);
        self.channel.send(&Frame::Cancel { req }, &[])?;
        let mut terminal = Vec::new();
        loop {
            let p = self.receive()?;
            if let Frame::CancelAck { state, .. } = p.frame {
                return Ok(CancelOutcome { state, terminal });
            }
            if matches!(
                p.frame,
                Frame::CommandResult { .. }
                    | Frame::SourceComplete { .. }
                    | Frame::ValidateResult { .. }
                    | Frame::ConnectionLocated { .. }
                    | Frame::BindResult { .. }
                    | Frame::AuthenticateResult { .. }
                    | Frame::AfterPublishResult { .. }
                    | Frame::CommandPrepared { .. }
            ) {
                terminal.push(p);
            }
        }
    }
    fn finish_stderr(&mut self) {
        self.stderr_stop.store(true, Ordering::Release);
        if let Some(t) = self.stderr.take() {
            let _ = t.join();
        }
    }
}
fn group_exists(pid: u32) -> bool {
    unsafe { libc::kill(-(pid as i32), 0) == 0 }
}
impl Drop for Session {
    fn drop(&mut self) {
        if !self.closed {
            terminate(&mut self.child, self.deadlines.grace);
        }
        self.finish_stderr();
    }
}
fn terminate(child: &mut Child, grace: Duration) {
    unsafe {
        libc::kill(-(child.id() as i32), libc::SIGTERM);
    }
    let deadline = Instant::now() + grace;
    while Instant::now() < deadline {
        if child.try_wait().ok().flatten().is_some() {
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }
    // Signal the group even when its direct leader has already exited.
    unsafe {
        libc::kill(-(child.id() as i32), libc::SIGKILL);
    }
    let _ = child.wait();
}
unsafe fn close_descriptors(start: i32) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        // Enumerate this single-threaded post-fork child, not a racy parent
        // snapshot. Walking RLIMIT_NOFILE can mean a million close syscalls.
        let mut entries = std::mem::MaybeUninit::<[libc::proc_fdinfo; 4096]>::uninit();
        let capacity = std::mem::size_of::<[libc::proc_fdinfo; 4096]>();
        let count = unsafe {
            libc::proc_pidinfo(
                libc::getpid(),
                libc::PROC_PIDLISTFDS,
                0,
                entries.as_mut_ptr().cast(),
                capacity as i32,
            )
        };
        if count <= 0 || count as usize >= capacity {
            return Err(io::Error::other(
                "cannot enumerate all inherited descriptors",
            ));
        }
        let entries = unsafe {
            std::slice::from_raw_parts(
                entries.as_ptr().cast::<libc::proc_fdinfo>(),
                count as usize / std::mem::size_of::<libc::proc_fdinfo>(),
            )
        };
        for entry in entries {
            if entry.proc_fd >= start {
                unsafe {
                    libc::close(entry.proc_fd);
                }
            }
        }
        Ok(())
    }
    #[cfg(not(target_os = "macos"))]
    {
        #[cfg(target_os = "linux")]
        {
            if unsafe { libc::syscall(libc::SYS_close_range, start as u32, u32::MAX, 0) } == 0 {
                return Ok(());
            }
        }
        let mut limit = std::mem::MaybeUninit::<libc::rlimit>::uninit();
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limit.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let limit = unsafe { limit.assume_init() }.rlim_cur.min(i32::MAX as _) as i32;
        for fd in start..limit {
            unsafe {
                libc::close(fd);
            }
        }
        Ok(())
    }
}
