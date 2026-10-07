//! Task 29: actual SDK cancellation/EOF at the native transaction boundary.
//! Only a library-test child installs the hook; ordinary builds have no hook or
//! environment-variable switch. The side channel is synchronization, not stop:
//! stop must travel through SDK StopToken -> PullWorker::stop -> native interrupt.
use crate::{native::NativeEngine, pull::PullReceipt};
use grv_adapter_api::*;
use grv_adapter_wire::{Channel, state::Role};
use grv_types::{
    DeclarationIdentity, LatestRevision, PullRequestIdentity, RequestedRevision,
    declaration_digest, pull_request_digest,
};
use serde_json::json;
use std::{
    io::{self, Read, Write},
    os::{
        fd::AsRawFd,
        unix::{
            net::{UnixListener, UnixStream},
            process::CommandExt,
        },
    },
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

struct Boundary {
    after: bool,
    socket: PathBuf,
}
static BOUNDARY: OnceLock<Boundary> = OnceLock::new();

pub(crate) fn pause_at_commit(
    after: bool,
    receipt: &PullReceipt,
    stopping: &Option<Arc<AtomicBool>>,
) {
    let Some(boundary) = BOUNDARY.get().filter(|boundary| boundary.after == after) else {
        return;
    };
    let mut socket = UnixStream::connect(&boundary.socket).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let bytes = serde_json::to_vec(&receipt.wire_receipt().unwrap()).unwrap();
    socket
        .write_all(&(bytes.len() as u64).to_be_bytes())
        .unwrap();
    socket.write_all(&bytes).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !stopping
        .as_ref()
        .expect("real PullWorker stopping flag")
        .load(Ordering::Acquire)
    {
        assert!(
            Instant::now() < deadline,
            "SDK stop never reached the native owner"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    // stop() sets the flag and interrupts before joining. Keep the owner alive
    // until the parent has checked that no stopped acknowledgement escaped.
    socket.write_all(b"S").unwrap();
    let mut release = [0];
    socket.read_exact(&mut release).unwrap();
    assert_eq!(release, *b"R");
}

#[test]
fn sdk_native_commit_boundary_child_probe() {
    let Ok(socket) = std::env::var("GRV_TEST_BOUNDARY_SOCKET") else {
        return;
    };
    if std::env::var("GRV_TEST_BOUNDARY_ENABLED").unwrap() == "true" {
        assert!(
            BOUNDARY
                .set(Boundary {
                    after: std::env::var("GRV_TEST_BOUNDARY_AFTER").unwrap() == "true",
                    socket: socket.into(),
                })
                .is_ok()
        );
    }
    let result = grv_adapter_sdk::run_fd3(crate::process::DuckDbAdapter::default());
    if std::env::var("GRV_TEST_BOUNDARY_EOF").unwrap() == "true" {
        assert!(result.unwrap_err().to_string().contains("channel EOF"));
    } else {
        result.unwrap();
    }
}

struct Peer {
    channel: Channel<UnixStream>,
    observer: UnixStream,
    child: Child,
    handle: Handle,
    identity: String,
    workspace: Uuid,
    adapter: AdapterIdentity,
    registry: Registry,
    next: u64,
    prepares: usize,
    applies: usize,
}
impl Peer {
    fn start(database: &Path, root: &Path, boundary: Option<(&Path, bool, bool)>) -> Self {
        let (parent, socket) = UnixStream::pair().unwrap();
        parent
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let fd = socket.as_raw_fd();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "commit_boundary_tests::sdk_native_commit_boundary_child_probe",
                "--nocapture",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        // Even unpaused peers run the same real SDK; a nonexistent boundary
        // selector is represented by not installing a matching hook.
        let idle_socket = root.join("unused-boundary.sock");
        let (control, after, eof) = boundary.unwrap_or((&idle_socket, false, false));
        command
            .env("GRV_TEST_BOUNDARY_SOCKET", control)
            .env("GRV_TEST_BOUNDARY_AFTER", after.to_string())
            .env("GRV_TEST_BOUNDARY_EOF", eof.to_string())
            .env("GRV_TEST_BOUNDARY_ENABLED", boundary.is_some().to_string());
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
        let observer = parent.try_clone().unwrap();
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
                    attempt: Uuid::v4(),
                    resources: resources.clone(),
                },
                &[],
            )
            .unwrap();
        let Frame::Identified {
            name,
            package_version,
            binding_schema_version,
            registry,
            capabilities,
            ..
        } = channel.receive().unwrap().frame
        else {
            panic!("identified");
        };
        assert!(capabilities.pull);
        let adapter = AdapterIdentity {
            name,
            package_version,
            binding_schema_version,
            interface_version: Req::new(1).unwrap(),
        };
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
                    connection: Doc::inline(json!({"database":database})),
                    mode: Mode::Pull,
                    run_id: None,
                },
                &[],
            )
            .unwrap();
        let Frame::ConnectionLocated {
            connection: Doc::Inline(locator),
            ..
        } = channel.receive().unwrap().frame
        else {
            panic!("locate");
        };
        let identity = locator.inline.identity.clone().unwrap();
        channel
            .send(
                &Frame::BindConnection {
                    req: Req::new(2).unwrap(),
                    locator: Doc::inline(locator.inline),
                    root: Some(root.to_str().unwrap().into()),
                    expected_identity: Some(identity.clone()),
                    expected_workspace_id: None,
                    mode: Mode::Pull,
                },
                &[],
            )
            .unwrap();
        let Frame::BindResult {
            handle,
            workspace_id,
            ..
        } = channel.receive().unwrap().frame
        else {
            panic!("bind");
        };
        Self {
            channel,
            observer,
            child,
            handle,
            identity,
            workspace: workspace_id.unwrap_or_else(Uuid::v4),
            adapter,
            registry,
            next: 3,
            prepares: 0,
            applies: 0,
        }
    }
    fn req(&mut self) -> Req {
        let req = Req::new(self.next).unwrap();
        self.next += 1;
        req
    }
    fn fixed(&self, database: &Path, root: &Path) -> RequestRecord {
        let declaration = crate::binding::validate_pull(json!({"kind":"pull","dataset":"data","adapter":"duckdb","connection":{"database":database},"target":{"schema":"app"},"tables":[{"name":"rows"}]})).unwrap();
        let declaration_sha256 = declaration_digest(&DeclarationIdentity {
            effective_declaration: declaration.clone(),
            adapter_identity: self.adapter.clone(),
            connection_identity: self.identity.clone(),
            canonical_connection: json!({"database":std::fs::canonicalize(database).unwrap()}),
        })
        .unwrap();
        let requested_revision = RequestedRevision::Latest(LatestRevision::Latest);
        let request_sha256 = pull_request_digest(&PullRequestIdentity {
            root: root.to_str().unwrap().into(),
            workspace_id: self.workspace.clone(),
            declaration_sha256: declaration_sha256.clone(),
            requested_revision: requested_revision.clone(),
        })
        .unwrap();
        RequestRecord {
            attempt_id: Uuid::v4(),
            root: root.to_str().unwrap().into(),
            dataset: Name::new("data").unwrap(),
            workspace_id: self.workspace.clone(),
            adapter_identity: self.adapter.clone(),
            connection_identity: self.identity.clone(),
            validation_input: declaration.clone(),
            effective_declaration: declaration,
            declaration_sha256,
            request_sha256,
            registry: self.registry.clone(),
            requested_revision,
        }
    }
    fn resolve(&mut self, request: &RequestRecord, compare: bool) -> PullResolution {
        let req = self.req();
        self.channel
            .send(
                &Frame::ResolvePull {
                    req,
                    handle: self.handle.clone(),
                    payload: Doc::inline(ResolvePullRequest {
                        phase: if compare {
                            ResolvePhase::Compare
                        } else {
                            ResolvePhase::Lookup
                        },
                        attempt_id: request.attempt_id.clone(),
                        root: request.root.clone(),
                        request: compare.then(|| request.clone()),
                    }),
                },
                &[],
            )
            .unwrap();
        let packet = self.channel.receive().unwrap();
        let Frame::ResolveResult {
            resolution: Doc::Inline(result),
            ..
        } = packet.frame
        else {
            panic!("resolve: {:?}", packet.frame);
        };
        result.inline
    }
    fn prepare(&mut self, request: &RequestRecord, revision: u64, source: &Path) -> PullPlan {
        assert_eq!(
            self.resolve(request, false).state,
            ResolutionState::NotCommitted
        );
        assert_eq!(
            self.resolve(request, true).state,
            ResolutionState::NotCommitted
        );
        self.prepares += 1;
        let bytes = std::fs::read(source).unwrap();
        let req = self.req();
        self.channel
            .send(
                &Frame::PreparePull {
                    req,
                    handle: self.handle.clone(),
                    payload: Doc::inline(PreparePullRequest {
                        request: request.clone(),
                        resolved_revision: U64::new(revision).unwrap(),
                        recovery: None,
                        tables: vec![PullTable {
                            name: Name::new("rows").unwrap(),
                            target: json!({"table":"rows"}),
                            select: None,
                            partitions: vec![json!({})],
                            source_contract: contract(),
                            output_contract: contract(),
                        }],
                        files: vec![VerifiedFile {
                            table: Name::new("rows").unwrap(),
                            partition: json!({}),
                            version: U64::new(revision).unwrap(),
                            schema: FileSchema {
                                columns: contract().columns,
                            },
                            access: FileAccess::Local,
                            location: source.to_str().unwrap().into(),
                            size: U64::new(bytes.len() as u64).unwrap(),
                            sha256: grv_types::sha256(&bytes),
                            validator: "test:immutable".into(),
                        }],
                    }),
                },
                &[],
            )
            .unwrap();
        let packet = self.channel.receive().unwrap();
        let Frame::PrepareResult {
            plan: Doc::Inline(plan),
            ..
        } = packet.frame
        else {
            panic!("prepare: {:?}", packet.frame);
        };
        plan.inline
    }
    fn dispatch(&mut self, plan: PullPlan) -> Req {
        self.applies += 1;
        let req = self.req();
        self.channel
            .send(
                &Frame::ApplyPull {
                    req,
                    handle: self.handle.clone(),
                    plan: Doc::inline(plan),
                },
                &[],
            )
            .unwrap();
        req
    }
    fn receipt(&mut self) -> Receipt {
        let packet = self.channel.receive().unwrap();
        let Frame::ApplyResult {
            receipt: Doc::Inline(receipt),
            ..
        } = packet.frame
        else {
            panic!("apply: {:?}", packet.frame);
        };
        receipt.inline
    }
    fn close(mut self) {
        let req = self.req();
        self.channel.send(&Frame::Close { req }, &[]).unwrap();
        assert!(matches!(
            self.channel.receive().unwrap().frame,
            Frame::CloseResult { .. }
        ));
        drop(self.observer);
        assert_child(self.child);
    }
}
fn assert_child(mut child: Child) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            child.kill().unwrap();
            panic!("SDK child failed to join/exit");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
fn contract() -> TableContract {
    TableContract {
        columns: vec![Column {
            name: "id".into(),
            logical_type: json!("int64"),
        }],
        partition_keys: vec![],
        extensions: json!({}),
        column_ext: json!({}),
    }
}
fn source(path: &Path, ids: Vec<i64>) {
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(ids))]).unwrap();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None)
            .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}
fn inspect(database: &Path, expected_rows: &[&str], revision: u64) {
    let mut engine = NativeEngine::open(database).unwrap();
    engine.authorize_relation("app", "rows").unwrap();
    let rows = engine
        .metadata_query("SELECT id FROM app.rows ORDER BY id")
        .unwrap();
    assert_eq!(
        rows.into_iter()
            .map(|row| row[0].clone().unwrap())
            .collect::<Vec<_>>(),
        expected_rows
    );
    let checkpoint = engine
        .metadata_query("SELECT receipt FROM _grv.pull_checkpoint")
        .unwrap();
    assert_eq!(checkpoint.len(), 1);
    let receipt: PullReceipt = serde_json::from_str(checkpoint[0][0].as_ref().unwrap()).unwrap();
    assert_eq!(receipt.committed_revision.get(), revision);
}

#[test]
fn sdk_native_cancel_and_eof_at_commit_boundary_fence_and_immutable_replay() {
    for after in [false, true] {
        for eof in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let root = directory.path().join("grv");
            std::fs::create_dir(&root).unwrap();
            let database = directory.path().join("boundary.duckdb");
            let source_path = directory.path().join("source.parquet");
            source(&source_path, vec![1]);
            let mut baseline = Peer::start(&database, &root, None);
            let first = baseline.fixed(&database, &root);
            let plan = baseline.prepare(&first, 1, &source_path);
            baseline.dispatch(plan);
            assert_eq!(baseline.receipt().committed_revision.get(), 1);
            baseline.close();

            source(&source_path, vec![7, 8]);
            let control_path = directory.path().join("boundary.sock");
            let listener = UnixListener::bind(&control_path).unwrap();
            listener.set_nonblocking(true).unwrap();
            let mut peer = Peer::start(&database, &root, Some((&control_path, after, eof)));
            let original_request = peer.fixed(&database, &root);
            let plan = peer.prepare(&original_request, 2, &source_path);
            let req = peer.dispatch(plan);
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut control = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        assert!(
                            Instant::now() < deadline,
                            "native owner never reached boundary"
                        );
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("boundary accept: {error}"),
                }
            };
            control.set_nonblocking(false).unwrap();
            control
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let mut length = [0; 8];
            control.read_exact(&mut length).unwrap();
            let length = usize::try_from(u64::from_be_bytes(length)).unwrap();
            assert!(
                length <= 1024 * 1024,
                "boundary receipt exceeds test budget"
            );
            let mut bytes = vec![0; length];
            control.read_exact(&mut bytes).unwrap();
            let original: Receipt = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(original.request, original_request);
            assert_eq!(original.committed_revision.get(), 2);
            assert_eq!(original.row_counts[0].rows.get(), 2);
            assert!(crate::lock::WorkspaceLock::acquire(&database).is_err());

            // EOF is a real wire EOF, not a process exit or StopToken imitation.
            let mut delivered = false;
            if eof {
                drop(peer.channel);
                drop(peer.observer);
                let mut stopped = [0];
                control.read_exact(&mut stopped).unwrap();
                assert_eq!(stopped, *b"S");
                assert!(
                    peer.child.try_wait().unwrap().is_none(),
                    "EOF returned before native join"
                );
                assert!(crate::lock::WorkspaceLock::acquire(&database).is_err());
                control.write_all(b"R").unwrap();
                assert_child(peer.child);
            } else {
                peer.channel.send(&Frame::Cancel { req }, &[]).unwrap();
                let mut stopped = [0];
                control.read_exact(&mut stopped).unwrap();
                assert_eq!(stopped, *b"S");
                // Inspect the socket without advancing wire protocol state.
                // No terminal result/ack can exist while the owner is held.
                let mut observer = peer.observer.try_clone().unwrap();
                observer
                    .set_read_timeout(Some(Duration::from_millis(100)))
                    .unwrap();
                let error = observer.read(&mut [0]).unwrap_err();
                assert!(matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ));
                observer
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                drop(observer);
                assert!(crate::lock::WorkspaceLock::acquire(&database).is_err());
                control.write_all(b"R").unwrap();
                let frame = peer.channel.receive().unwrap().frame;
                match frame {
                    Frame::ApplyResult {
                        receipt: Doc::Inline(receipt),
                        ..
                    } => {
                        delivered = true;
                        assert_eq!(receipt.inline, original);
                        assert!(
                            matches!(peer.channel.receive().unwrap().frame, Frame::CancelAck { req: ack, state: CancelState::Completed } if ack == req)
                        );
                    }
                    Frame::CancelAck {
                        req: ack,
                        state: CancelState::Stopped,
                    } if !after && ack == req => {}
                    unexpected => panic!("boundary cancel result: {unexpected:?}"),
                }
                assert!(crate::lock::WorkspaceLock::acquire(&database).is_ok());
                peer.close();
            }
            assert!(crate::lock::WorkspaceLock::acquire(&database).is_ok());
            std::fs::remove_file(&source_path).unwrap();
            let mut recovery = Peer::start(&database, &root, None);
            let resolved = recovery.resolve(&original_request, false);
            let committed = match resolved.state {
                ResolutionState::Committed => {
                    assert_eq!(resolved.receipt, Some(original.clone()));
                    assert_eq!(
                        recovery.resolve(&original_request, true).receipt,
                        Some(original.clone())
                    );
                    true
                }
                ResolutionState::NotCommitted if !after => {
                    assert!(resolved.receipt.is_none());
                    false
                }
                unexpected => panic!("boundary recovery: {unexpected:?}"),
            };
            assert!(
                !delivered || committed,
                "delivered receipt must agree with fenced transaction"
            );
            recovery.close();
            inspect(
                &database,
                if committed { &["7", "8"] } else { &["1"] },
                if committed { 2 } else { 1 },
            );

            source(&source_path, vec![11]);
            let mut newer = Peer::start(&database, &root, None);
            let later = newer.fixed(&database, &root);
            let plan = newer.prepare(&later, 3, &source_path);
            newer.dispatch(plan);
            assert_eq!(newer.receipt().committed_revision.get(), 3);
            newer.close();
            std::fs::remove_file(&source_path).unwrap();
            let mut replay = Peer::start(&database, &root, None);
            let lookup = replay.resolve(&original_request, false);
            let compare = replay.resolve(&original_request, true);
            if committed {
                assert_eq!(lookup.receipt, Some(original.clone()));
                assert_eq!(compare.receipt, Some(original));
                assert!(lookup.prior_source_contracts.is_empty());
                assert!(compare.prior_source_contracts.is_empty());
            } else {
                assert_eq!(lookup.state, ResolutionState::NotCommitted);
                assert_eq!(compare.state, ResolutionState::NotCommitted);
            }
            assert_eq!(
                (replay.prepares, replay.applies),
                (0, 0),
                "recovery must not prepare/apply"
            );
            replay.close();
            inspect(&database, &["11"], 3);
        }
    }
}
