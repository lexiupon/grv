#![cfg(feature = "native")]
use grv_adapter_api::*;
use grv_adapter_wire::{Channel, state::Role};
use serde_json::json;
use std::{
    fs, io,
    os::{
        fd::AsRawFd,
        unix::{fs::PermissionsExt, net::UnixStream, process::CommandExt},
    },
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
// Test-only RAII guard. It owns the child, joins it on a bounded deadline and
// only escalates to kill+wait for a PID it still owns; a PID already reaped by
// try_wait is marked and never signalled again.
struct OwnedChild {
    child: Option<Child>,
    reaped: bool,
}
impl OwnedChild {
    fn spawn(command: &mut Command) -> io::Result<Self> {
        Ok(Self {
            child: Some(command.spawn()?),
            reaped: false,
        })
    }
    fn join(&mut self) -> io::Result<()> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let owned = self.child.as_mut().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "child already taken")
            })?;
            // Already reaped here: never signal this PID again.
            if owned.try_wait()?.is_some() {
                self.reaped = true;
                return Ok(());
            }
            if Instant::now() >= deadline {
                // Re-check before escalating so a just-reaped PID is never killed.
                if owned.try_wait()?.is_some() {
                    self.reaped = true;
                    return Ok(());
                }
                owned.kill()?;
                owned.wait()?;
                self.reaped = true;
                // A forced kill is not a qualified closure of the child.
                return Err(io::Error::from(io::ErrorKind::TimedOut));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if self.reaped {
            return;
        }
        if let Some(child) = self.child.as_mut() {
            // Only a live owned child is signalled; an unclassified try_wait error
            // leaves it alone (this guard may leak rather than signal blindly).
            match child.try_wait() {
                Ok(None) => {
                    let _ = child.kill();
                    let _ = child.wait();
                }
                Ok(Some(_)) => self.reaped = true,
                Err(_) => {}
            }
        }
    }
}
struct Peer {
    channel: Channel<UnixStream>,
    child: OwnedChild,
    handle: Handle,
    next: u64,
}
impl Peer {
    fn start(path: &Path, request: &DiscoverBuildRequest) -> Self {
        let (parent, socket) = UnixStream::pair().unwrap();
        parent
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let fd = socket.as_raw_fd();
        let mut command = Command::new(env!("CARGO_BIN_EXE_grv-adapter-duckdb"));
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        unsafe {
            command.pre_exec(move || {
                if libc::dup2(fd, 3) < 0 || libc::fcntl(3, libc::F_SETFD, 0) < 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        // Null stdio: nothing is drained and no child output is secreted.
        command.stdout(Stdio::null()).stderr(Stdio::null());
        let child = OwnedChild::spawn(&mut command).unwrap();
        drop(socket);
        let resources = Resources::default();
        let mut channel = Channel::new(
            parent,
            Role::Parent,
            resources.max_batch_bytes.get() as usize,
        );
        channel
            .send(
                &Frame::Hello {
                    interface_versions: vec![Req::new(1).unwrap()],
                    core: CoreIdentity {
                        name: "grv".into(),
                        version: "0.1.0".into(),
                    },
                    attempt: request.identity.attempt_id.clone(),
                    resources: resources.clone(),
                },
                &[],
            )
            .unwrap();
        let Frame::Identified {
            capabilities,
            binding_schema_version,
            ..
        } = channel.receive().unwrap().frame
        else {
            panic!("identify")
        };
        assert!(capabilities.managed_build && capabilities.external_build);
        channel
            .send(
                &Frame::Ready {
                    interface_version: Req::new(1).unwrap(),
                    binding_schema_version,
                    resources,
                },
                &[],
            )
            .unwrap();
        channel
            .send(
                &Frame::LocateConnection {
                    req: Req::new(1).unwrap(),
                    connection: Doc::inline(json!({"database":path})),
                    mode: request.execution.mode(),
                    run_id: Some(request.identity.run_id.clone()),
                },
                &[],
            )
            .unwrap();
        let Frame::ConnectionLocated {
            connection: Doc::Inline(locator),
            ..
        } = channel.receive().unwrap().frame
        else {
            panic!("locate")
        };
        channel
            .send(
                &Frame::BindConnection {
                    req: Req::new(2).unwrap(),
                    locator: Doc::inline(locator.inline),
                    root: Some(request.identity.root.clone()),
                    expected_identity: Some(request.identity.connection_identity.clone()),
                    expected_workspace_id: Some(request.identity.workspace_id.clone()),
                    mode: request.execution.mode(),
                },
                &[],
            )
            .unwrap();
        let packet = channel.receive().unwrap();
        let Frame::BindResult { handle, .. } = packet.frame else {
            panic!("bind {:?}", packet.frame)
        };
        Self {
            channel,
            child,
            handle,
            next: 3,
        }
    }
    fn req(&mut self) -> Req {
        let req = Req::new(self.next).unwrap();
        self.next += 1;
        req
    }
    fn prepare(&mut self, request: DiscoverBuildRequest) -> BuildSession {
        // Derived before the request moves into the discovery frame. Managed
        // defaults are unchanged: no base contracts, revision zero.
        let self_input = request.self_input;
        let base_contracts = if self_input {
            request
                .outputs
                .iter()
                .map(|output| NamedContract {
                    table: output.table.clone(),
                    contract: output.contract.clone(),
                })
                .collect()
        } else {
            vec![]
        };
        let base_revision = U64::new(if self_input { 1 } else { 0 }).unwrap();
        let req = self.req();
        self.channel
            .send(
                &Frame::DiscoverBuild {
                    req,
                    handle: self.handle.clone(),
                    payload: Doc::inline(request),
                },
                &[],
            )
            .unwrap();
        let packet = self.channel.receive().unwrap();
        let Frame::BuildDiscovered {
            discovery: Doc::Inline(discovery),
            ..
        } = packet.frame
        else {
            panic!("discover {:?}", packet.frame)
        };
        let req = self.req();
        self.channel
            .send(
                &Frame::PrepareBuild {
                    req,
                    handle: self.handle.clone(),
                    payload: Doc::inline(PrepareBuildRequest {
                        discovery: discovery.inline,
                        base_revision,
                        self_input,
                        base_contracts,
                        base_files: vec![],
                        input_files: vec![],
                        holds_confirmed: true,
                    }),
                },
                &[],
            )
            .unwrap();
        let packet = self.channel.receive().unwrap();
        let Frame::BuildPrepared {
            session: Doc::Inline(session),
            ..
        } = packet.frame
        else {
            panic!("prepare {:?}", packet.frame)
        };
        session.inline
    }
}
fn name(s: &str) -> Name {
    Name::new(s).unwrap()
}
fn request(path: &Path, sql: &str) -> DiscoverBuildRequest {
    DiscoverBuildRequest {
        identity: BuildIdentity {
            attempt_id: Uuid::v4(),
            root: "file:///private/grv-build-process".into(),
            dataset: name("product"),
            run_id: RunId::new("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            workspace_id: Uuid::v4(),
            declaration_sha256: grv_types::sha256(b"processbuild"),
            adapter_identity: AdapterIdentity {
                name: name("duckdb"),
                package_version: env!("CARGO_PKG_VERSION").into(),
                interface_version: Req::new(1).unwrap(),
                binding_schema_version: Req::new(1).unwrap(),
            },
            connection_identity: format!("duckdb:{}", path.display()),
        },
        options: json!({}),
        execution: BuildExecution::Managed,
        inputs: vec![],
        outputs: vec![BuildOutput {
            table: name("rows"),
            source: json!({"sql":sql}),
            columns: json!([{"name":"id","type":"int64"}]),
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
        selected_outputs: vec![name("rows")],
        self_input: false,
    }
}
fn directory() -> tempfile::TempDir {
    let dir = tempfile::Builder::new()
        .prefix(".native-build-process-")
        .tempdir_in(env!("CARGO_MANIFEST_DIR"))
        .unwrap();
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
    dir
}
#[test]
fn cancellation_and_eof_interrupt_native_invocation_join_owner_and_refuse_reexecution() {
    for cancel in [true, false] {
        let dir = directory();
        let path = dir.path().join("cancel.duckdb");
        let sql = "SELECT CAST(sum(a.range+b.range) AS BIGINT) AS id FROM range(1000000000) a CROSS JOIN range(1000000000) b";
        let request = request(&path, sql);
        let mut peer = Peer::start(&path, &request);
        let session = peer.prepare(request.clone());
        let req = peer.req();
        peer.channel
            .send(
                &Frame::ExecuteBuild {
                    req,
                    handle: peer.handle.clone(),
                    payload: Doc::inline(ExecuteBuildRequest {
                        session,
                        queries: vec![BuildQuery {
                            table: name("rows"),
                            sql: sql.into(),
                        }],
                    }),
                },
                &[],
            )
            .unwrap();
        std::thread::sleep(Duration::from_millis(75));
        if cancel {
            peer.channel.send(&Frame::Cancel { req }, &[]).unwrap();
            let mut packet = peer.channel.receive().unwrap();
            if let Frame::BuildFinished {
                result: Doc::Inline(result),
                ..
            } = &packet.frame
            {
                // An interrupt can establish a failed invocation before the SDK
                // handles cancellation. Preserve that known terminal result.
                assert_eq!(result.inline.status, BuildExecutionStatus::Failed);
                assert!(result.inline.completion.is_none());
                packet = peer.channel.receive().unwrap();
            }
            assert!(
                matches!(packet.frame, Frame::CancelAck { .. }),
                "{:?}",
                packet.frame
            );
            assert!(grv_adapter_duckdb::lock::WorkspaceLock::acquire(&path).is_ok());
        }
        drop(peer.channel);
        peer.child.join().unwrap();
        let mut store = grv_adapter_duckdb::build::BuildStore::open(
            &path,
            request.identity.root.clone(),
            Some(request.identity.workspace_id.clone()),
            &Resources::default(),
        )
        .unwrap();
        let record = store.open_session(&request.identity).unwrap();
        assert_eq!(record.state, BuildState::Executing);
        assert!(record.candidate.is_none());
        assert!(record.completion.is_none());
        assert!(
            store
                .execute(ExecuteBuildRequest {
                    session: record.session,
                    queries: vec![BuildQuery {
                        table: name("rows"),
                        sql: sql.into()
                    }]
                })
                .is_err()
        );
    }
}
#[test]
fn eof_closes_retained_discovery_transaction_before_any_session_exists() {
    let dir = directory();
    let path = dir.path().join("discovery.duckdb");
    let request = request(&path, "SELECT 1::BIGINT AS id");
    let mut peer = Peer::start(&path, &request);
    let req = peer.req();
    peer.channel
        .send(
            &Frame::DiscoverBuild {
                req,
                handle: peer.handle.clone(),
                payload: Doc::inline(request.clone()),
            },
            &[],
        )
        .unwrap();
    assert!(matches!(
        peer.channel.receive().unwrap().frame,
        Frame::BuildDiscovered { .. }
    ));
    drop(peer.channel);
    peer.child.join().unwrap();
    let mut store = grv_adapter_duckdb::build::BuildStore::open(
        &path,
        request.identity.root.clone(),
        Some(request.identity.workspace_id.clone()),
        &Resources::default(),
    )
    .unwrap();
    assert!(store.workspace_id().is_none());
    assert!(store.discover(request).is_err());
}
// EOF is the only fence here: the external protocol invents no invocation, so the
// retained preparation owner simply closes. holds_confirmed is only the request
// flag: no real core hold, no source preparation and no cloud read.
#[test]
fn external_empty_gcs_sdk_eof_joins_prepared_owner_without_receipt() {
    let dir = directory();
    let path = dir.path().join("external-eof.duckdb");
    let mut request = request(&path, "SELECT 1::BIGINT AS id");
    request.identity.root = "gs://test-bucket/empty-cancel".into();
    request.execution = BuildExecution::External;
    request.self_input = true;
    // The self-input output stays a table reference; no query is ever admitted.
    request.outputs[0].source = json!({"table": "rows"});
    let mut peer = Peer::start(&path, &request);
    let session = peer.prepare(request.clone());
    assert_eq!(session.execution, BuildExecution::External);
    assert!(session.self_input);
    assert_eq!(session.base_contracts.len(), 1);
    assert_eq!(session.base_contracts[0].table, name("rows"));
    // Empty private view: no verified cloud file was carried, so no GcsView
    // relation, reader or authenticated read was reached.
    assert!(session.inputs.is_empty());
    // The serving child still owns the real workspace lock before EOF.
    assert!(grv_adapter_duckdb::lock::WorkspaceLock::acquire(&path).is_err());
    drop(peer.channel);
    peer.child.join().unwrap();
    assert!(grv_adapter_duckdb::lock::WorkspaceLock::acquire(&path).is_ok());
    let mut store = grv_adapter_duckdb::build::BuildStore::open(
        &path,
        request.identity.root.clone(),
        Some(request.identity.workspace_id.clone()),
        &Resources::default(),
    )
    .unwrap();
    let record = store.open_session(&request.identity).unwrap();
    assert_eq!(record.state, BuildState::Prepared);
    assert_eq!(record.session, session);
    assert!(record.candidate.is_none());
    assert!(record.completion.is_none());
    assert!(record.outcome.is_none());
    drop(store);
    let mut store = grv_adapter_duckdb::build::BuildStore::open(
        &path,
        request.identity.root.clone(),
        Some(request.identity.workspace_id.clone()),
        &Resources::default(),
    )
    .unwrap();
    let again = store.open_session(&request.identity).unwrap();
    assert_eq!(again.state, record.state);
    assert_eq!(again.session, record.session);
    assert!(again.candidate.is_none());
    assert!(again.completion.is_none());
    assert!(again.outcome.is_none());
}
