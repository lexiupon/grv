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
    time::Duration,
};
struct Peer {
    channel: Channel<UnixStream>,
    child: Child,
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
        let child = command.spawn().unwrap();
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
                    mode: Mode::ManagedBuild,
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
                    mode: Mode::ManagedBuild,
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
                        base_revision: U64::new(0).unwrap(),
                        self_input: false,
                        base_contracts: vec![],
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
        let output = peer.child.wait_with_output().unwrap();
        assert!(output.stdout.is_empty());
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
    let _ = peer.child.wait_with_output().unwrap();
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
