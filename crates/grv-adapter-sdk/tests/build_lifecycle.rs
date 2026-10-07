use grv_adapter_api::*;
use grv_adapter_sdk::{
    Adapter, BoundConnection, BuildExport, BuildExportEvent, PreparedCommand, Registration, Result,
    StopToken,
};
use grv_adapter_wire::{Channel, state::Role};
use serde_json::json;
use std::{
    collections::VecDeque,
    os::unix::net::UnixStream,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};
fn req(i: u64) -> Req {
    Req::new(i).unwrap()
}
fn name(s: &str) -> Name {
    Name::new(s).unwrap()
}
fn unsupported<T>() -> Result<T> {
    Err(grv_types::PublicError {
        code: ErrorCode::UnsupportedCapability,
        message: "unsupported".into(),
        retryable: false,
        object: None,
    })
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
fn discovery_request() -> DiscoverBuildRequest {
    DiscoverBuildRequest {
        options: json!({}),
        identity: BuildIdentity {
            attempt_id: Uuid::v4(),
            root: "/root".into(),
            dataset: name("product"),
            run_id: RunId::new("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            workspace_id: Uuid::v4(),
            declaration_sha256: grv_types::sha256(b"fixed"),
            adapter_identity: AdapterIdentity {
                name: name("fixture"),
                package_version: "1".into(),
                interface_version: req(1),
                binding_schema_version: req(1),
            },
            connection_identity: "fixture-db".into(),
        },
        execution: BuildExecution::Managed,
        inputs: vec![],
        outputs: ["rows", "empty"]
            .into_iter()
            .map(|n| BuildOutput {
                table: name(n),
                source: json!({"sql":"SELECT 1 AS id"}),
                columns: json!([{"name":"id","type":"int64","source":"id"}]),
                contract: contract(),
            })
            .collect(),
        selected_outputs: vec![name("rows"), name("empty")],
        self_input: false,
    }
}
#[derive(Default)]
struct Durable {
    record: Option<BuildRecord>,
    retained_transaction: bool,
}
struct Fixture {
    durable: Arc<Mutex<Durable>>,
    executions: Arc<AtomicUsize>,
    stops: Arc<AtomicUsize>,
    request: Option<DiscoverBuildRequest>,
    oversized: bool,
}
impl Adapter for Fixture {
    fn registration(&self) -> Registration {
        Registration {
            name: name("fixture"),
            package_version: "1".into(),
            interface_versions: vec![req(1)],
            binding_schema_version: req(1),
            capabilities: Capabilities {
                push: true,
                managed_build: true,
                external_build: true,
                ..Capabilities::default()
            },
            registry: Registry {
                schema_bundle: json!({"$schema":"https://json-schema.org/draft/2020-12/schema"}),
                points: vec![],
            },
            commands: vec![],
        }
    }
    fn prepare_command(&self, _: &Name, _: &[String]) -> Result<PreparedCommand> {
        unsupported()
    }
    fn execute_command(&mut self, _: &CommandCall, _: &StopToken) -> Result<serde_json::Value> {
        unsupported()
    }
    fn bind_connection(
        &mut self,
        _: ConnectionLocator,
        _: Option<String>,
        _: Option<String>,
        workspace: Option<Uuid>,
        _: Mode,
    ) -> Result<BoundConnection> {
        Ok(BoundConnection {
            handle: Handle::new("fixture").unwrap(),
            identity: Some("fixture-db".into()),
            workspace_id: workspace,
            binding: BindingState::Bound,
            details: json!({}),
        })
    }
    fn discover_build(
        &mut self,
        _: Handle,
        request: DiscoverBuildRequest,
        _: &StopToken,
    ) -> Result<BuildDiscovery> {
        self.durable.lock().unwrap().retained_transaction = true;
        let result = BuildDiscovery {
            discovery_id: Uuid::v4(),
            identity: request.identity.clone(),
            inputs: vec![],
            outputs: request
                .outputs
                .iter()
                .map(|o| OutputBinding {
                    table: o.table.clone(),
                    source: o.source.clone(),
                    columns: o.columns.clone(),
                    engine_table: format!("_private.{}", o.table),
                    contract: o.contract.clone(),
                })
                .collect(),
        };
        self.request = Some(request);
        Ok(result)
    }
    fn prepare_build(
        &mut self,
        _: Handle,
        preparation: PrepareBuildRequest,
        _: &StopToken,
    ) -> Result<BuildSession> {
        let mut durable = self.durable.lock().unwrap();
        assert!(durable.retained_transaction);
        let request = self.request.as_ref().unwrap();
        let session = BuildSession {
            options: request.options.clone(),
            session_id: Uuid::v4(),
            identity: request.identity.clone(),
            execution: request.execution,
            base_revision: preparation.base_revision,
            base_contracts: preparation.base_contracts,
            inputs: preparation.discovery.inputs,
            outputs: preparation.discovery.outputs,
            selected_outputs: request.selected_outputs.clone(),
            self_input: request.self_input,
            adapter_details: if self.oversized {
                json!({"padding":"x".repeat(1_050_000)})
            } else {
                json!({})
            },
        };
        durable.record = Some(BuildRecord {
            session: session.clone(),
            state: BuildState::Prepared,
            completion: None,
            completion_sha256: None,
            candidate: None,
            row_counts: vec![],
            outcome: None,
        });
        durable.retained_transaction = false;
        Ok(session)
    }
    fn execute_build(
        &mut self,
        _: Handle,
        request: ExecuteBuildRequest,
        _: &StopToken,
    ) -> Result<BuildExecutionResult> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        let completion = BuildCompletion {
            result_version: req(1),
            run_id: request.session.identity.run_id.clone(),
            workspace_id: request.session.identity.workspace_id.clone(),
            declaration_sha256: request.session.identity.declaration_sha256.clone(),
            kind: if request.session.selected_outputs.is_empty() {
                CompletionKind::OmissionOnly
            } else {
                CompletionKind::Engine
            },
            invocation_id: "fixed-invocation".into(),
            status: CompletionStatus::Succeeded,
            writers_stopped: true,
            completed_at: Timestamp::new("2026-10-06T00:00:00Z").unwrap(),
            completed_outputs: request
                .session
                .outputs
                .iter()
                .filter(|o| request.session.selected_outputs.contains(&o.table))
                .map(|o| CompletedOutput {
                    table: o.table.clone(),
                    engine_table: o.engine_table.clone(),
                })
                .collect(),
        };
        let row_counts: Vec<_> = request
            .session
            .selected_outputs
            .iter()
            .map(|n| TableCount {
                table: n.clone(),
                rows: U64::new(u64::from(n.as_str() == "rows")).unwrap(),
            })
            .collect();
        let mut durable = self.durable.lock().unwrap();
        let record = durable.record.as_mut().unwrap();
        record.state = BuildState::Completed;
        record.candidate = Some(completion.clone());
        record.row_counts = row_counts.clone();
        Ok(BuildExecutionResult {
            status: BuildExecutionStatus::Succeeded,
            completion: Some(completion),
            row_counts,
        })
    }
    fn accept_build_completion(
        &mut self,
        _: Handle,
        _: Uuid,
        completion: BuildCompletion,
        _: &StopToken,
    ) -> Result<Digest> {
        let digest = completion.digest().unwrap();
        let mut d = self.durable.lock().unwrap();
        let r = d.record.as_mut().unwrap();
        r.completion = Some(completion);
        r.completion_sha256 = Some(digest.clone());
        Ok(digest)
    }
    fn export_build(
        &mut self,
        _: Handle,
        _: Uuid,
        _: Digest,
        _: Uuid,
        _: &StopToken,
    ) -> Result<Box<dyn BuildExport>> {
        Ok(Box::new(Export(VecDeque::from([
            BuildExportEvent::Batch {
                table: name("rows"),
                payload: vec![0; 8],
                rows: U64::new(1).unwrap(),
            },
            BuildExportEvent::TableComplete {
                table: name("rows"),
                row_count: U64::new(1).unwrap(),
            },
            BuildExportEvent::TableComplete {
                table: name("empty"),
                row_count: U64::new(0).unwrap(),
            },
            BuildExportEvent::Complete(json!({"fixed":true})),
        ]))))
    }
    fn open_build(&mut self, _: Handle, _: BuildIdentity, _: &StopToken) -> Result<BuildRecord> {
        Ok(self.durable.lock().unwrap().record.clone().unwrap())
    }
    fn inspect_build(
        &mut self,
        h: Handle,
        identity: BuildIdentity,
        stop: &StopToken,
    ) -> Result<BuildRecord> {
        self.open_build(h, identity, stop)
    }
    fn record_build_outcome(
        &mut self,
        _: Handle,
        _: Uuid,
        outcome: BuildOutcome,
        _: &StopToken,
    ) -> Result<()> {
        self.durable
            .lock()
            .unwrap()
            .record
            .as_mut()
            .unwrap()
            .outcome = Some(outcome);
        Ok(())
    }
    fn cleanup_build(&mut self, _: Handle, _: Uuid, _: &StopToken) -> Result<()> {
        Ok(())
    }
    fn stop_and_wait(&mut self) -> Result<()> {
        self.durable.lock().unwrap().retained_transaction = false;
        self.stops.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct Export(VecDeque<BuildExportEvent>);
impl BuildExport for Export {
    fn next(&mut self, _: &StopToken) -> Result<Option<BuildExportEvent>> {
        Ok(self.0.pop_front())
    }
}
struct Peer {
    channel: Channel<UnixStream>,
    join: thread::JoinHandle<std::result::Result<(), grv_adapter_sdk::SdkError>>,
    next: u64,
    durable: Arc<Mutex<Durable>>,
    executions: Arc<AtomicUsize>,
    stops: Arc<AtomicUsize>,
}
impl Peer {
    fn new(durable: Arc<Mutex<Durable>>, executions: Arc<AtomicUsize>, oversized: bool) -> Self {
        let (parent, child) = UnixStream::pair().unwrap();
        parent
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let stops = Arc::new(AtomicUsize::new(0));
        let fixture = Fixture {
            durable: durable.clone(),
            executions: executions.clone(),
            stops: stops.clone(),
            request: None,
            oversized,
        };
        let join = thread::spawn(move || grv_adapter_sdk::serve(fixture, child));
        let mut channel = Channel::new(parent, Role::Parent, 8 * 1024 * 1024);
        channel
            .send(
                &Frame::Hello {
                    interface_versions: vec![req(1)],
                    core: CoreIdentity {
                        name: "grv".into(),
                        version: "1".into(),
                    },
                    attempt: Uuid::v4(),
                    resources: Resources {
                        slots: req(1),
                        ..Resources::default()
                    },
                },
                &[],
            )
            .unwrap();
        assert!(matches!(
            channel.receive().unwrap().frame,
            Frame::Identified { .. }
        ));
        channel
            .send(
                &Frame::Ready {
                    interface_version: req(1),
                    binding_schema_version: req(1),
                    resources: Resources {
                        slots: req(1),
                        ..Resources::default()
                    },
                },
                &[],
            )
            .unwrap();
        Self {
            channel,
            join,
            next: 1,
            durable,
            executions,
            stops,
        }
    }
    fn recv(&mut self) -> Frame {
        loop {
            let f = self.channel.receive().unwrap().frame;
            match f {
                Frame::DocumentEnd { document_id } => self
                    .channel
                    .send(&Frame::DocumentAck { document_id }, &[])
                    .unwrap(),
                Frame::DocumentBegin { .. } | Frame::DocumentChunk { .. } => {}
                other => return other,
            }
        }
    }
    fn call(&mut self, f: impl FnOnce(Req) -> Frame) -> Frame {
        let id = req(self.next);
        self.next += 1;
        let frame = f(id);
        if serde_json::to_vec(&frame).unwrap().len() + 1 > grv_adapter_wire::FRAME_LIMIT {
            use base64::Engine;
            let mut value = serde_json::to_value(&frame).unwrap();
            let document = value
                .as_object_mut()
                .unwrap()
                .values_mut()
                .find(|v| v.get("inline").is_some())
                .unwrap();
            let bytes = grv_types::canonical_json(&document["inline"]).unwrap();
            let document_id = Uuid::v4();
            *document = json!({"document_id":document_id});
            self.channel
                .send(
                    &Frame::DocumentBegin {
                        document_id: document_id.clone(),
                        size: U64::new(bytes.len() as u64).unwrap(),
                        sha256: grv_types::sha256(&bytes),
                    },
                    &[],
                )
                .unwrap();
            for (index, data) in bytes.chunks(512 * 1024).enumerate() {
                self.channel
                    .send(
                        &Frame::DocumentChunk {
                            document_id: document_id.clone(),
                            index: SafeInt::new(index as u64).unwrap(),
                            data: base64::engine::general_purpose::STANDARD.encode(data),
                        },
                        &[],
                    )
                    .unwrap();
            }
            self.channel
                .send(
                    &Frame::DocumentEnd {
                        document_id: document_id.clone(),
                    },
                    &[],
                )
                .unwrap();
            assert!(
                matches!(self.recv(),Frame::DocumentAck {document_id:returned} if returned==document_id)
            );
            self.channel
                .send(&serde_json::from_value(value).unwrap(), &[])
                .unwrap();
        } else {
            self.channel.send(&frame, &[]).unwrap();
        }
        self.recv()
    }
    fn bind(&mut self, identity: &BuildIdentity) {
        assert!(matches!(
            self.call(|req| Frame::BindConnection {
                req,
                locator: Doc::inline(ConnectionLocator {
                    canonical_connection: json!({}),
                    identity: Some(identity.connection_identity.clone()),
                    engine_path: None,
                    session_lock_path: None
                }),
                root: Some(identity.root.clone()),
                expected_identity: Some(identity.connection_identity.clone()),
                expected_workspace_id: Some(identity.workspace_id.clone()),
                mode: Mode::ManagedBuild
            }),
            Frame::BindResult { .. }
        ));
    }
    fn prepare(&mut self) -> BuildSession {
        let request = discovery_request();
        self.bind(&request.identity);
        let Frame::BuildDiscovered {
            discovery: Doc::Inline(discovery),
            ..
        } = self.call(|req| Frame::DiscoverBuild {
            req,
            handle: Handle::new("fixture").unwrap(),
            payload: Doc::inline(request),
        })
        else {
            panic!("discovery missing")
        };
        assert!(self.durable.lock().unwrap().retained_transaction);
        let Frame::BuildPrepared {
            session: Doc::Inline(session),
            ..
        } = self.call(|req| Frame::PrepareBuild {
            req,
            handle: Handle::new("fixture").unwrap(),
            payload: Doc::inline(PrepareBuildRequest {
                discovery: discovery.inline,
                base_revision: U64::new(0).unwrap(),
                self_input: false,
                base_contracts: vec![],
                base_files: vec![],
                input_files: vec![],
                holds_confirmed: true,
            }),
        })
        else {
            panic!("preparation missing")
        };
        session.inline
    }
    fn execute(&mut self, session: &BuildSession) -> BuildCompletion {
        let Frame::BuildFinished {
            result: Doc::Inline(result),
            ..
        } = self.call(|req| Frame::ExecuteBuild {
            req,
            handle: Handle::new("fixture").unwrap(),
            payload: Doc::inline(ExecuteBuildRequest {
                session: session.clone(),
                queries: session
                    .selected_outputs
                    .iter()
                    .map(|table| BuildQuery {
                        table: table.clone(),
                        sql: "SELECT 1 AS id".into(),
                    })
                    .collect(),
            }),
        })
        else {
            panic!("execution missing")
        };
        result.inline.completion.unwrap()
    }
    fn close(mut self) {
        assert!(matches!(
            self.call(|req| Frame::Close { req }),
            Frame::CloseResult { .. }
        ));
        self.join.join().unwrap().unwrap();
        assert!(self.stops.load(Ordering::SeqCst) > 0);
    }
}
fn peer(oversized: bool) -> Peer {
    Peer::new(
        Arc::new(Mutex::new(Durable::default())),
        Arc::new(AtomicUsize::new(0)),
        oversized,
    )
}
#[test]
fn retained_discovery_execute_once_acceptance_and_credited_empty_export_reopen() {
    let mut p = peer(true);
    let session = p.prepare();
    let completion = p.execute(&session);
    assert_eq!(p.executions.load(Ordering::SeqCst), 1);
    let Frame::CompletionAccepted {
        completion_sha256: digest,
        ..
    } = p.call(|req| Frame::AcceptBuildCompletion {
        req,
        handle: Handle::new("fixture").unwrap(),
        session_id: session.session_id.clone(),
        completion: Doc::inline(completion.clone()),
    })
    else {
        panic!("acceptance missing")
    };
    assert_eq!(digest, completion.digest().unwrap());
    let durable = p.durable.clone();
    let executions = p.executions.clone();
    p.close();
    let mut p = Peer::new(durable, executions, false);
    p.bind(&session.identity);
    assert!(matches!(
        p.call(|req| Frame::OpenBuild {
            req,
            handle: Handle::new("fixture").unwrap(),
            payload: Doc::inline(session.identity.clone())
        }),
        Frame::BuildOpened { .. }
    ));
    let id = req(p.next);
    p.next += 1;
    p.channel
        .send(
            &Frame::ExportBuild {
                req: id,
                handle: Handle::new("fixture").unwrap(),
                session_id: session.session_id,
                completion_sha256: digest,
                stream_id: Uuid::v4(),
            },
            &[],
        )
        .unwrap();
    let Frame::Batch {
        table, seq, slot, ..
    } = p.recv()
    else {
        panic!("export batch missing")
    };
    p.channel
        .codec
        .io_mut()
        .set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    assert!(p.channel.codec.read().is_err());
    p.channel
        .codec
        .io_mut()
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    p.channel
        .send(
            &Frame::BatchAck {
                req: id,
                table,
                seq,
                slot,
            },
            &[],
        )
        .unwrap();
    assert!(matches!(p.recv(),Frame::BuildTableComplete { row_count,.. } if row_count.get()==1));
    assert!(matches!(p.recv(),Frame::BuildTableComplete { row_count,.. } if row_count.get()==0));
    assert!(matches!(p.recv(), Frame::ExportComplete { .. }));
    assert_eq!(p.executions.load(Ordering::SeqCst), 1);
    p.close();
}
#[test]
fn changed_preparation_repeated_execution_changed_acceptance_and_inspect_only_refuse() {
    for scenario in [
        "changed_discovery",
        "reuse_execute",
        "changed_completion",
        "inspect_only",
    ] {
        let mut p = peer(false);
        let session = p.prepare();
        let completion = p.execute(&session);
        let response = match scenario {
            "changed_discovery" => p.call(|req| Frame::DiscoverBuild {
                req,
                handle: Handle::new("fixture").unwrap(),
                payload: Doc::inline(discovery_request()),
            }),
            "reuse_execute" => p.call(|req| Frame::ExecuteBuild {
                req,
                handle: Handle::new("fixture").unwrap(),
                payload: Doc::inline(ExecuteBuildRequest {
                    session: session.clone(),
                    queries: session
                        .selected_outputs
                        .iter()
                        .map(|table| BuildQuery {
                            table: table.clone(),
                            sql: "SELECT 1 AS id".into(),
                        })
                        .collect(),
                }),
            }),
            "changed_completion" => {
                let mut changed = completion.clone();
                changed.completed_at = Timestamp::new("2026-10-06T00:00:01Z").unwrap();
                p.call(|req| Frame::AcceptBuildCompletion {
                    req,
                    handle: Handle::new("fixture").unwrap(),
                    session_id: session.session_id.clone(),
                    completion: Doc::inline(changed),
                })
            }
            "inspect_only" => {
                let durable = p.durable.clone();
                let executions = p.executions.clone();
                p.close();
                p = Peer::new(durable, executions, false);
                p.bind(&session.identity);
                assert!(matches!(
                    p.call(|req| Frame::InspectBuild {
                        req,
                        handle: Handle::new("fixture").unwrap(),
                        payload: Doc::inline(session.identity.clone())
                    }),
                    Frame::BuildInspected { .. }
                ));
                p.call(|req| Frame::CleanupBuild {
                    req,
                    handle: Handle::new("fixture").unwrap(),
                    session_id: session.session_id.clone(),
                })
            }
            _ => unreachable!(),
        };
        assert!(
            matches!(
                response,
                Frame::Error {
                    code: ErrorCode::ProtocolFailure,
                    ..
                }
            ),
            "{scenario}: {response:?}"
        );
        assert_eq!(p.executions.load(Ordering::SeqCst), 1);
        p.close();
    }
}
#[test]
fn cancel_exhausted_export_and_eof_wait_for_stopped_adapter() {
    for eof in [false, true] {
        let mut p = peer(false);
        let session = p.prepare();
        let completion = p.execute(&session);
        let digest = completion.digest().unwrap();
        p.call(|req| Frame::AcceptBuildCompletion {
            req,
            handle: Handle::new("fixture").unwrap(),
            session_id: session.session_id.clone(),
            completion: Doc::inline(completion),
        });
        let id = req(p.next);
        p.next += 1;
        p.channel
            .send(
                &Frame::ExportBuild {
                    req: id,
                    handle: Handle::new("fixture").unwrap(),
                    session_id: session.session_id,
                    completion_sha256: digest,
                    stream_id: Uuid::v4(),
                },
                &[],
            )
            .unwrap();
        assert!(matches!(p.recv(), Frame::Batch { .. }));
        if eof {
            let stops = p.stops.clone();
            drop(p.channel);
            assert!(p.join.join().unwrap().is_err());
            assert!(stops.load(Ordering::SeqCst) > 0);
        } else {
            p.channel.send(&Frame::Cancel { req: id }, &[]).unwrap();
            assert!(matches!(
                p.recv(),
                Frame::CancelAck {
                    state: CancelState::Stopped,
                    ..
                }
            ));
            assert!(p.stops.load(Ordering::SeqCst) > 0);
            p.close();
        }
    }
}
