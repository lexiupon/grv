#![cfg(feature = "native")]
use grv_adapter_api::*;
use grv_adapter_wire::{Channel, state::Role};
use grv_types::{
    DeclarationIdentity, LatestRevision, PullRequestIdentity, RequestedRevision,
    declaration_digest, pull_request_digest,
};
use serde_json::json;
use std::{
    io,
    os::{
        fd::AsRawFd,
        unix::{net::UnixStream, process::CommandExt},
    },
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};
struct Peer {
    channel: Channel<UnixStream>,
    child: Child,
    handle: Handle,
    identity: String,
    workspace: Uuid,
    adapter: AdapterIdentity,
    registry: Registry,
    next: u64,
}
impl Peer {
    fn start(database: &Path, root: &Path, attempt: Uuid) -> Self {
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
                    attempt,
                    resources: resources.clone(),
                },
                &[],
            )
            .unwrap();
        let Frame::Identified {
            name,
            package_version,
            binding_schema_version,
            capabilities,
            registry,
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
        let packet = channel.receive().unwrap();
        let Frame::ConnectionLocated {
            connection: Doc::Inline(locator),
            ..
        } = packet.frame
        else {
            panic!("locate: {:?}", packet.frame);
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
        let packet = channel.receive().unwrap();
        let Frame::BindResult {
            handle,
            workspace_id,
            binding,
            ..
        } = packet.frame
        else {
            panic!("bind: {:?}", packet.frame);
        };
        assert_eq!(
            workspace_id.is_none(),
            binding == BindingState::Uninitialized
        );
        // This is the parent's reservation, not an adapter-established binding.
        let workspace = workspace_id.unwrap_or_else(Uuid::v4);
        Self {
            channel,
            child,
            handle,
            identity,
            workspace,
            adapter,
            registry,
            next: 3,
        }
    }
    fn req(&mut self) -> Req {
        let req = Req::new(self.next).unwrap();
        self.next += 1;
        req
    }
    fn resolve(
        &mut self,
        root: &str,
        attempt: Uuid,
        request: Option<RequestRecord>,
    ) -> PullResolution {
        let phase = if request.is_some() {
            ResolvePhase::Compare
        } else {
            ResolvePhase::Lookup
        };
        let req = self.req();
        self.channel
            .send(
                &Frame::ResolvePull {
                    req,
                    handle: self.handle.clone(),
                    payload: Doc::inline(ResolvePullRequest {
                        phase,
                        attempt_id: attempt,
                        root: root.into(),
                        request,
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
    fn close(mut self) {
        let req = self.req();
        self.channel.send(&Frame::Close { req }, &[]).unwrap();
        assert!(matches!(
            self.channel.receive().unwrap().frame,
            Frame::CloseResult { .. }
        ));
        let output = self.child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stdout.is_empty());
        assert!(output.stderr.is_empty());
    }
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
fn fixed_request(
    peer: &Peer,
    database: &Path,
    root: &Path,
    attempt: Uuid,
    declaration: serde_json::Value,
) -> RequestRecord {
    let declaration = grv_adapter_duckdb::binding::validate_pull(declaration).unwrap();
    let declaration_sha256 = declaration_digest(&DeclarationIdentity {
        effective_declaration: declaration.clone(),
        adapter_identity: peer.adapter.clone(),
        connection_identity: peer.identity.clone(),
        canonical_connection: json!({"database":std::fs::canonicalize(database).unwrap()}),
    })
    .unwrap();
    let requested_revision = RequestedRevision::Latest(LatestRevision::Latest);
    let request_sha256 = pull_request_digest(&PullRequestIdentity {
        root: root.to_str().unwrap().into(),
        workspace_id: peer.workspace.clone(),
        declaration_sha256: declaration_sha256.clone(),
        requested_revision: requested_revision.clone(),
    })
    .unwrap();
    RequestRecord {
        attempt_id: attempt,
        root: root.to_str().unwrap().into(),
        dataset: Name::new("data").unwrap(),
        workspace_id: peer.workspace.clone(),
        adapter_identity: peer.adapter.clone(),
        connection_identity: peer.identity.clone(),
        validation_input: declaration.clone(),
        effective_declaration: declaration,
        declaration_sha256,
        request_sha256,
        registry: peer.registry.clone(),
        requested_revision,
    }
}
#[test]
fn sdk_pull_cancel_and_eof_join_owner_before_acknowledgement_and_fenced_retry() {
    for cancel in [true, false] {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("grv");
        std::fs::create_dir(&root).unwrap();
        let database = directory.path().join("stopped.duckdb");
        let attempt = Uuid::v4();
        let mut peer = Peer::start(&database, &root, attempt.clone());
        let query = "SELECT CAST(sum(a.range+b.range) AS BIGINT) AS id FROM range(1000000000) a CROSS JOIN range(1000000000) b";
        let request = fixed_request(
            &peer,
            &database,
            &root,
            attempt.clone(),
            json!({"kind":"pull","dataset":"data","adapter":"duckdb","connection":{"database":database},"target":{"schema":"app"},"tables":[{"name":"rows","select":{"sql":query},"columns":[{"name":"id","type":"int64"}]}]}),
        );
        peer.resolve(root.to_str().unwrap(), attempt.clone(), None);
        peer.resolve(
            root.to_str().unwrap(),
            attempt.clone(),
            Some(request.clone()),
        );
        let req = peer.req();
        peer.channel
            .send(
                &Frame::PreparePull {
                    req,
                    handle: peer.handle.clone(),
                    payload: Doc::inline(PreparePullRequest {
                        request,
                        resolved_revision: U64::new(0).unwrap(),
                        tables: vec![PullTable {
                            name: Name::new("rows").unwrap(),
                            target: json!({"table":"rows"}),
                            select: Some(json!({"sql":query})),
                            partitions: vec![],
                            source_contract: contract(),
                            output_contract: contract(),
                        }],
                        files: vec![],
                        recovery: None,
                    }),
                },
                &[],
            )
            .unwrap();
        let packet = peer.channel.receive().unwrap();
        let Frame::PrepareResult {
            plan: Doc::Inline(plan),
            ..
        } = packet.frame
        else {
            panic!("prepare: {:?}", packet.frame);
        };
        let req = peer.req();
        peer.channel
            .send(
                &Frame::ApplyPull {
                    req,
                    handle: peer.handle.clone(),
                    plan: Doc::inline(plan.inline),
                },
                &[],
            )
            .unwrap();
        std::thread::sleep(Duration::from_millis(50));
        if cancel {
            peer.channel.send(&Frame::Cancel { req }, &[]).unwrap();
            assert!(matches!(
                peer.channel.receive().unwrap().frame,
                Frame::CancelAck {
                    state: CancelState::Stopped,
                    ..
                }
            ));
            assert!(grv_adapter_duckdb::lock::WorkspaceLock::acquire(&database).is_ok());
            peer.close();
        } else {
            drop(peer.channel);
            let output = peer.child.wait_with_output().unwrap();
            assert_eq!(output.status.code(), Some(6));
            assert!(String::from_utf8_lossy(&output.stderr).contains("channel EOF"));
            assert!(grv_adapter_duckdb::lock::WorkspaceLock::acquire(&database).is_ok());
        }
        let mut retry = Peer::start(&database, &root, attempt.clone());
        assert_eq!(
            retry.resolve(root.to_str().unwrap(), attempt, None).state,
            ResolutionState::NotCommitted
        );
        retry.close();
    }
}
#[test]
fn sdk_pull_lookup_compare_pure_prepare_atomic_apply_and_source_free_restart_replay() {
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("grv");
    std::fs::create_dir(&root).unwrap();
    let database = directory.path().join("target.duckdb");
    let attempt = Uuid::v4();
    let mut peer = Peer::start(&database, &root, attempt.clone());
    let declaration=grv_adapter_duckdb::binding::validate_pull(json!({"kind":"pull","dataset":"data","adapter":"duckdb","connection":{"database":database},"target":{"schema":"app"},"tables":[{"name":"rows"}]})).unwrap();
    let declaration_sha256 = declaration_digest(&DeclarationIdentity {
        effective_declaration: declaration.clone(),
        adapter_identity: peer.adapter.clone(),
        connection_identity: peer.identity.clone(),
        canonical_connection: json!({"database":std::fs::canonicalize(&database).unwrap()}),
    })
    .unwrap();
    let requested_revision = RequestedRevision::Latest(LatestRevision::Latest);
    let request_sha256 = pull_request_digest(&PullRequestIdentity {
        root: root.to_str().unwrap().into(),
        workspace_id: peer.workspace.clone(),
        declaration_sha256: declaration_sha256.clone(),
        requested_revision: requested_revision.clone(),
    })
    .unwrap();
    let request = RequestRecord {
        attempt_id: attempt.clone(),
        root: root.to_str().unwrap().into(),
        dataset: Name::new("data").unwrap(),
        workspace_id: peer.workspace.clone(),
        adapter_identity: peer.adapter.clone(),
        connection_identity: peer.identity.clone(),
        validation_input: declaration.clone(),
        effective_declaration: declaration,
        declaration_sha256,
        request_sha256,
        registry: peer.registry.clone(),
        requested_revision,
    };
    assert_eq!(
        peer.resolve(root.to_str().unwrap(), attempt.clone(), None)
            .state,
        ResolutionState::NotCommitted
    );
    assert_eq!(
        peer.resolve(
            root.to_str().unwrap(),
            attempt.clone(),
            Some(request.clone())
        )
        .state,
        ResolutionState::NotCommitted
    );
    let source = directory.path().join("source.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&source).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let bytes = std::fs::read(&source).unwrap();
    let files = vec![VerifiedFile {
        table: Name::new("rows").unwrap(),
        partition: json!({}),
        version: U64::new(1).unwrap(),
        schema: FileSchema {
            columns: contract().columns,
        },
        access: FileAccess::Local,
        location: source.to_str().unwrap().into(),
        size: U64::new(bytes.len() as u64).unwrap(),
        sha256: grv_types::sha256(&bytes),
        validator: "test:immutable".into(),
    }];
    let tables = vec![PullTable {
        name: Name::new("rows").unwrap(),
        target: json!({"table":"rows"}),
        select: None,
        partitions: vec![json!({})],
        source_contract: contract(),
        output_contract: contract(),
    }];
    let req = peer.req();
    peer.channel
        .send(
            &Frame::PreparePull {
                req,
                handle: peer.handle.clone(),
                payload: Doc::inline(PreparePullRequest {
                    request: request.clone(),
                    resolved_revision: U64::new(1).unwrap(),
                    tables,
                    files,
                    recovery: None,
                }),
            },
            &[],
        )
        .unwrap();
    let packet = peer.channel.receive().unwrap();
    let Frame::PrepareResult {
        plan: Doc::Inline(plan),
        ..
    } = packet.frame
    else {
        panic!("prepare: {:?}", packet.frame);
    };
    assert_eq!(plan.inline.refresh, Refresh::Full);
    let req = peer.req();
    peer.channel
        .send(
            &Frame::ApplyPull {
                req,
                handle: peer.handle.clone(),
                plan: Doc::inline(plan.inline),
            },
            &[],
        )
        .unwrap();
    let packet = peer.channel.receive().unwrap();
    let Frame::ApplyResult {
        receipt: Doc::Inline(receipt),
        ..
    } = packet.frame
    else {
        panic!("apply: {:?}", packet.frame);
    };
    assert_eq!(receipt.inline.row_counts[0].rows.get(), 2);
    let original = receipt.inline;
    peer.close();
    std::fs::remove_file(source).unwrap();
    let mut replay = Peer::start(&database, &root, attempt.clone());
    let lookup = replay.resolve(root.to_str().unwrap(), attempt.clone(), None);
    assert_eq!(lookup.request, Some(request.clone()));
    assert_eq!(lookup.receipt, Some(original.clone()));
    assert!(lookup.prior_source_contracts.is_empty());
    let compared = replay.resolve(root.to_str().unwrap(), attempt, Some(request.clone()));
    assert!(compared.prior_source_contracts.is_empty());
    assert_eq!(compared.receipt, Some(original));
    replay.close();
    let next_attempt = Uuid::v4();
    let mut empty = Peer::start(&database, &root, next_attempt.clone());
    let mut next_request = request;
    next_request.attempt_id = next_attempt.clone();
    let lookup = empty.resolve(root.to_str().unwrap(), next_attempt.clone(), None);
    assert!(lookup.prior_source_contracts.is_empty());
    let compared = empty.resolve(
        root.to_str().unwrap(),
        next_attempt,
        Some(next_request.clone()),
    );
    assert_eq!(compared.state, ResolutionState::NotCommitted);
    assert_eq!(
        compared.prior_source_contracts,
        vec![NamedContract {
            table: Name::new("rows").unwrap(),
            contract: contract(),
        }]
    );
    let req = empty.req();
    empty
        .channel
        .send(
            &Frame::PreparePull {
                req,
                handle: empty.handle.clone(),
                payload: Doc::inline(PreparePullRequest {
                    request: next_request,
                    resolved_revision: U64::new(2).unwrap(),
                    tables: vec![PullTable {
                        name: Name::new("rows").unwrap(),
                        target: json!({"table":"rows"}),
                        select: None,
                        partitions: vec![],
                        source_contract: compared.prior_source_contracts[0].contract.clone(),
                        output_contract: contract(),
                    }],
                    files: vec![],
                    recovery: None,
                }),
            },
            &[],
        )
        .unwrap();
    let packet = empty.channel.receive().unwrap();
    let Frame::PrepareResult {
        plan: Doc::Inline(plan),
        ..
    } = packet.frame
    else {
        panic!("empty prepare: {:?}", packet.frame);
    };
    let req = empty.req();
    empty
        .channel
        .send(
            &Frame::ApplyPull {
                req,
                handle: empty.handle.clone(),
                plan: Doc::inline(plan.inline),
            },
            &[],
        )
        .unwrap();
    let packet = empty.channel.receive().unwrap();
    let Frame::ApplyResult {
        receipt: Doc::Inline(receipt),
        ..
    } = packet.frame
    else {
        panic!("empty apply: {:?}", packet.frame);
    };
    assert_eq!(receipt.inline.row_counts[0].rows.get(), 0);
    assert_eq!(receipt.inline.source_contracts[0].contract, contract());
    empty.close();
}
