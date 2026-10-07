#![cfg(feature = "native")]
use grv_adapter_api::*;
use grv_adapter_wire::{Channel, state::Role};
use serde_json::{Value, json};
use std::{
    io,
    os::{
        fd::AsRawFd,
        unix::{net::UnixStream, process::CommandExt},
    },
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};

fn fixture(path: &Path, mode: Option<&str>) {
    let mut command = Command::new(
        PathBuf::from(std::env::var("GRV_DUCKDB_NATIVE_LIB_DIR").unwrap())
            .join("grv_native_fixture"),
    );
    command.arg(path);
    if let Some(mode) = mode {
        command.arg(mode);
    }
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
struct Peer {
    channel: Channel<UnixStream>,
    child: Child,
    identity: String,
    handle: Handle,
    attempt: Uuid,
    adapter: AdapterIdentity,
    next: u64,
}
impl Peer {
    fn start(path: &Path, attempt: Uuid, resources: Resources) -> Self {
        let (parent, socket) = UnixStream::pair().unwrap();
        parent
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        parent
            .set_write_timeout(Some(Duration::from_secs(10)))
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
                    attempt: attempt.clone(),
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
            ..
        } = channel.receive().unwrap().frame
        else {
            panic!("identified")
        };
        assert!(capabilities.push);
        assert_eq!(capabilities.source_consistency, WireConsistency::Snapshot);
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
                    connection: Doc::inline(json!({"database":path})),
                    mode: Mode::Extract,
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
            panic!("located")
        };
        let identity = locator.inline.identity.clone().unwrap();
        channel
            .send(
                &Frame::BindConnection {
                    req: Req::new(2).unwrap(),
                    locator: Doc::inline(locator.inline),
                    root: None,
                    expected_identity: Some(identity.clone()),
                    expected_workspace_id: None,
                    mode: Mode::Extract,
                },
                &[],
            )
            .unwrap();
        let packet = channel.receive().unwrap();
        let Frame::BindResult {
            handle, binding, ..
        } = packet.frame
        else {
            panic!("bind: {:?}", packet.frame)
        };
        assert_eq!(binding, BindingState::NotApplicable);
        Self {
            channel,
            child,
            identity,
            handle,
            attempt,
            adapter,
            next: 3,
        }
    }
    fn extract(&mut self, root: &Path, tables: Vec<ExtractTable>) -> Req {
        let req = Req::new(self.next).unwrap();
        self.next += 1;
        let request = ExtractRequest {
            attempt_id: self.attempt.clone(),
            stream_id: Uuid::v4(),
            root: root.to_str().unwrap().into(),
            dataset: Name::new("data").unwrap(),
            run_id: RunId::new("01K6SE8XF57QEHBJB63GW2Z48Y").unwrap(),
            declaration_sha256: Digest::new("a".repeat(64)).unwrap(),
            adapter_identity: self.adapter.clone(),
            connection_identity: self.identity.clone(),
            selection: ExtractSelection {
                policy: SelectionPolicy::All,
            },
            tables,
            options: json!({}),
            resume: None,
        };
        self.channel
            .send(
                &Frame::Extract {
                    req,
                    handle: self.handle.clone(),
                    payload: Doc::inline(request),
                },
                &[],
            )
            .unwrap();
        req
    }
    fn close(mut self) {
        let req = Req::new(self.next).unwrap();
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
fn table(
    name: &str,
    relation: &str,
    filter: Option<&str>,
    columns: &[(&str, &str, Value)],
) -> ExtractTable {
    let mut source = json!({"table":relation});
    if let Some(filter) = filter {
        source["filter"] = json!(filter);
    }
    ExtractTable {
        name: Name::new(name).unwrap(),
        source,
        columns: Value::Array(
            columns
                .iter()
                .map(|(name, source, _)| json!({"name":name,"source":source}))
                .collect(),
        ),
        contract: TableContract {
            columns: columns
                .iter()
                .map(|(name, _, logical_type)| Column {
                    name: (*name).into(),
                    logical_type: logical_type.clone(),
                })
                .collect(),
            partition_keys: vec![],
            extensions: json!({}),
            column_ext: json!({}),
        },
    }
}

#[test]
fn source_projection_over_256_fields_is_byte_bounded_and_empty_members_complete() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("wide.duckdb");
    fixture(&path, None);
    let root = directory.path().join("grv");
    std::fs::create_dir(&root).unwrap();
    let columns: Vec<_> = (0..300)
        .map(|index| Column {
            name: format!("c{index}"),
            logical_type: json!("int64"),
        })
        .collect();
    let member = |name: &str, source: Value| ExtractTable {
        name: Name::new(name).unwrap(),
        source,
        columns: Value::Array(
            columns
                .iter()
                .map(|column| json!({"name":column.name,"source":"id"}))
                .collect(),
        ),
        contract: TableContract {
            columns: columns.clone(),
            partition_keys: vec![],
            extensions: json!({}),
            column_ext: json!({}),
        },
    };
    let mut peer = Peer::start(&path, Uuid::v4(), Resources::default());
    let req = peer.extract(
        &root,
        vec![
            member("wide", json!({"table":"main.first","filter":"id < 2"})),
            member("zero", json!({"table":"main.empty"})),
        ],
    );
    let checkpoint_id = checkpoint(&mut peer);
    peer.channel
        .send(&Frame::CheckpointAck { req, checkpoint_id }, &[])
        .unwrap();
    let mut counts = std::collections::BTreeMap::new();
    loop {
        let packet = peer.channel.receive().unwrap();
        match packet.frame {
            Frame::Batch {
                table,
                seq,
                slot,
                rows,
                ..
            } => {
                assert_eq!(table.as_str(), "wide");
                assert_eq!(rows.get(), 2);
                let mut reader = arrow_ipc::reader::StreamReader::try_new(
                    std::io::Cursor::new(packet.payload),
                    None,
                )
                .unwrap();
                let batch = reader.next().unwrap().unwrap();
                assert_eq!(batch.num_columns(), 300);
                for array in batch.columns() {
                    let values = array
                        .as_any()
                        .downcast_ref::<arrow_array::Int64Array>()
                        .unwrap();
                    assert_eq!(values.values().as_ref(), &[0, 1]);
                }
                peer.channel
                    .send(
                        &Frame::BatchAck {
                            req,
                            table,
                            seq,
                            slot,
                        },
                        &[],
                    )
                    .unwrap();
            }
            Frame::TableComplete {
                table, row_count, ..
            } => {
                counts.insert(table.as_str().to_owned(), row_count.get());
            }
            Frame::SourceComplete { .. } => break,
            unexpected => panic!("unexpected wide source frame: {unexpected:?}"),
        }
    }
    assert_eq!(
        counts,
        std::collections::BTreeMap::from([("wide".into(), 2), ("zero".into(), 0)])
    );
    peer.close();
}
#[test]
fn sdk_shared_snapshot_durable_all_member_checkpoint_credits_and_empty_table() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source.duckdb");
    fixture(&path, None);
    let root = directory.path().join("grv");
    std::fs::create_dir(&root).unwrap();
    let attempt = Uuid::v4();
    let mut peer = Peer::start(&path, attempt.clone(), Resources::default());
    let req=peer.extract(&root,vec![table("rows","main.first",Some("selected AND regexp_matches(upper(label), '^ROW-') AND abs(id) >= 0 AND date_part('year',DATE '2026-10-06')=2026"),&[("id","id",json!("int64")),("text","label",json!("string")),("mapped_rowid","rowid",json!("int64"))]),table("zero","main.empty",None,&[("id","id",json!("int64")),("text","label",json!("string"))])]);
    let packet = peer.channel.receive().unwrap();
    assert!(
        matches!(packet.frame, Frame::ExtractStarted { .. }),
        "{:?}",
        packet.frame
    );
    let packet = peer.channel.receive().unwrap();
    let Frame::Checkpoint {
        checkpoint_id,
        payload: Doc::Inline(checkpoint),
        ..
    } = packet.frame
    else {
        panic!("checkpoint {:?}", packet.frame)
    };
    assert_eq!(checkpoint.inline.tables.len(), 2);
    assert_eq!(
        checkpoint.inline.tables[0].snapshot_id,
        checkpoint.inline.tables[1].snapshot_id
    );
    assert!(
        checkpoint
            .inline
            .tables
            .iter()
            .all(|table| !table.reopenable)
    );
    let journal = PathBuf::from(format!("{}.grv-acquisitions", path.display()))
        .join(attempt.as_str())
        .join("checkpoint.json");
    let persisted: Checkpoint = serde_json::from_slice(&std::fs::read(journal).unwrap()).unwrap();
    assert_eq!(persisted, checkpoint.inline);
    peer.channel
        .send(&Frame::CheckpointAck { req, checkpoint_id }, &[])
        .unwrap();
    let mut rows = 0;
    let mut completed = Vec::new();
    loop {
        let packet = peer.channel.receive().unwrap();
        match packet.frame {
            Frame::Batch {
                table,
                seq,
                slot,
                rows: count,
                ..
            } => {
                let mut ipc = arrow_ipc::reader::StreamReader::try_new(
                    std::io::Cursor::new(packet.payload),
                    None,
                )
                .unwrap();
                let batch = ipc.next().unwrap().unwrap();
                assert_eq!(batch.num_rows() as u64, count.get());
                rows += count.get();
                assert!(ipc.next().is_none());
                peer.channel
                    .send(
                        &Frame::BatchAck {
                            req,
                            table,
                            seq,
                            slot,
                        },
                        &[],
                    )
                    .unwrap();
            }
            Frame::TableComplete {
                table, row_count, ..
            } => completed.push((table.as_str().to_owned(), row_count.get())),
            Frame::SourceComplete { .. } => break,
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(rows, 2500);
    assert_eq!(completed, vec![("rows".into(), 2500), ("zero".into(), 0)]);
    assert!(grv_adapter_duckdb::lock::WorkspaceLock::acquire(&path).is_ok());
    peer.close();
    let mut peer = Peer::start(&path, attempt, Resources::default());
    peer.extract(
        &root,
        vec![table(
            "zero",
            "main.empty",
            None,
            &[("id", "id", json!("int64"))],
        )],
    );
    assert!(matches!(
        peer.channel.receive().unwrap().frame,
        Frame::Error {
            code: ErrorCode::ExtractionIncomplete,
            ..
        }
    ));
    peer.close();
}

fn first_table() -> ExtractTable {
    table("rows", "main.first", None, &[("id", "id", json!("int64"))])
}
fn checkpoint(peer: &mut Peer) -> Uuid {
    assert!(matches!(
        peer.channel.receive().unwrap().frame,
        Frame::ExtractStarted { .. }
    ));
    let Frame::Checkpoint { checkpoint_id, .. } = peer.channel.receive().unwrap().frame else {
        panic!("expected checkpoint")
    };
    checkpoint_id
}
#[test]
fn cancellation_before_checkpoint_ack_joins_owner_and_refuses_same_attempt() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source.duckdb");
    fixture(&path, None);
    let root = directory.path().join("grv");
    std::fs::create_dir(&root).unwrap();
    let attempt = Uuid::v4();
    let mut peer = Peer::start(&path, attempt.clone(), Resources::default());
    let req = peer.extract(&root, vec![first_table()]);
    checkpoint(&mut peer);
    peer.channel.send(&Frame::Cancel { req }, &[]).unwrap();
    assert!(matches!(
        peer.channel.receive().unwrap().frame,
        Frame::CancelAck {
            state: CancelState::Stopped,
            ..
        }
    ));
    assert!(grv_adapter_duckdb::lock::WorkspaceLock::acquire(&path).is_ok());
    peer.close();
    let mut peer = Peer::start(&path, attempt, Resources::default());
    peer.extract(&root, vec![first_table()]);
    assert!(matches!(
        peer.channel.receive().unwrap().frame,
        Frame::Error {
            code: ErrorCode::ExtractionIncomplete,
            ..
        }
    ));
    peer.close();
}
#[test]
fn eof_before_checkpoint_ack_joins_native_owner() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source.duckdb");
    fixture(&path, None);
    let root = directory.path().join("grv");
    std::fs::create_dir(&root).unwrap();
    let mut peer = Peer::start(&path, Uuid::v4(), Resources::default());
    peer.extract(&root, vec![first_table()]);
    checkpoint(&mut peer);
    drop(peer.channel);
    let output = peer.child.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(6));
    assert!(String::from_utf8_lossy(&output.stderr).contains("channel EOF"));
    assert!(grv_adapter_duckdb::lock::WorkspaceLock::acquire(&path).is_ok());
}
#[test]
fn partial_member_failure_emits_no_checkpoint_and_cannot_reacquire() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source.duckdb");
    fixture(&path, None);
    let root = directory.path().join("grv");
    std::fs::create_dir(&root).unwrap();
    let attempt = Uuid::v4();
    let mut peer = Peer::start(&path, attempt.clone(), Resources::default());
    peer.extract(
        &root,
        vec![
            first_table(),
            table(
                "bad",
                "main.empty",
                None,
                &[("missing", "missing", json!("int64"))],
            ),
        ],
    );
    assert!(matches!(
        peer.channel.receive().unwrap().frame,
        Frame::Error { .. }
    ));
    let journal =
        PathBuf::from(format!("{}.grv-acquisitions", path.display())).join(attempt.as_str());
    assert!(journal.join("acquisition.json").is_file());
    assert!(!journal.join("checkpoint.json").exists());
    peer.close();
    let mut peer = Peer::start(&path, attempt, Resources::default());
    peer.extract(&root, vec![first_table()]);
    assert!(matches!(
        peer.channel.receive().unwrap().frame,
        Frame::Error {
            code: ErrorCode::ExtractionIncomplete,
            ..
        }
    ));
    peer.close();
}
#[test]
fn unmanaged_source_only_and_filter_no_other_relation() {
    for (mode, relation, filter) in [
        (Some("managed"), "main.first", None),
        (Some("unknown"), "main.first", None),
        (None, "main.forbidden_view", None),
        (None, "main.first", Some("id IN (SELECT id FROM empty)")),
        (None, "main.first", Some("true; SELECT true")),
        (None, "main.first", Some("true FROM empty")),
        (None, "main.first", Some("true AS hidden")),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source.duckdb");
        fixture(&path, mode);
        let root = directory.path().join("grv");
        std::fs::create_dir(&root).unwrap();
        let mut peer = Peer::start(&path, Uuid::v4(), Resources::default());
        peer.extract(
            &root,
            vec![table(
                "rows",
                relation,
                filter,
                &[("id", "id", json!("int64"))],
            )],
        );
        assert!(
            matches!(peer.channel.receive().unwrap().frame, Frame::Error { .. }),
            "mode {mode:?}, relation {relation}, filter {filter:?}"
        );
        peer.close();
    }
}

#[test]
fn cancelled_with_exhausted_credit_stops_owner_before_ack() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source.duckdb");
    fixture(&path, None);
    let root = directory.path().join("grv");
    std::fs::create_dir(&root).unwrap();
    let resources = Resources {
        slots: Req::new(1).unwrap(),
        ..Resources::default()
    };
    let mut peer = Peer::start(&path, Uuid::v4(), resources);
    let req = peer.extract(&root, vec![first_table()]);
    let checkpoint_id = checkpoint(&mut peer);
    peer.channel
        .send(&Frame::CheckpointAck { req, checkpoint_id }, &[])
        .unwrap();
    assert!(matches!(
        peer.channel.receive().unwrap().frame,
        Frame::Batch { .. }
    ));
    peer.channel.send(&Frame::Cancel { req }, &[]).unwrap();
    assert!(matches!(
        peer.channel.receive().unwrap().frame,
        Frame::CancelAck {
            state: CancelState::Stopped,
            ..
        }
    ));
    assert!(grv_adapter_duckdb::lock::WorkspaceLock::acquire(&path).is_ok());
    peer.close();
}
#[test]
fn sdk_source_projects_all_grv_types_with_exact_values() {
    use arrow_array::{
        Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
        StringArray, TimestampMicrosecondArray, TimestampNanosecondArray,
    };
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source.duckdb");
    fixture(&path, Some("recognized"));
    let root = directory.path().join("grv");
    std::fs::create_dir(&root).unwrap();
    let mut peer = Peer::start(&path, Uuid::v4(), Resources::default());
    let cols = vec![
        ("b", "b", json!("boolean")),
        ("i", "i", json!("int64")),
        ("f", "f", json!("float64")),
        ("d", "d", json!("float64")),
        ("s", "s", json!("string")),
        ("blob", "blob", json!("binary")),
        ("date", "date", json!("date")),
        (
            "decimal",
            "decimal",
            json!({"decimal":{"precision":38,"scale":10}}),
        ),
        ("sec", "sec", json!({"timestamp":{"unit":"us","utc":false}})),
        ("ms", "ms", json!({"timestamp":{"unit":"us","utc":false}})),
        ("us", "us", json!({"timestamp":{"unit":"us","utc":false}})),
        ("ns", "ns", json!({"timestamp":{"unit":"ns","utc":false}})),
        ("utc", "utc", json!({"timestamp":{"unit":"us","utc":true}})),
    ];
    let req = peer.extract(
        &root,
        vec![table(
            "typed",
            "main.typed",
            Some("hash(blob) IS NOT NULL AND s = '🍕'"),
            &cols,
        )],
    );
    let checkpoint_id = checkpoint(&mut peer);
    peer.channel
        .send(&Frame::CheckpointAck { req, checkpoint_id }, &[])
        .unwrap();
    let packet = peer.channel.receive().unwrap();
    let Frame::Batch {
        table, seq, slot, ..
    } = packet.frame
    else {
        panic!("expected batch")
    };
    let mut reader =
        arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(packet.payload), None)
            .unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        -127
    );
    assert!(
        batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
            .is_sign_negative()
    );
    assert_eq!(
        batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        1.25
    );
    assert_eq!(
        batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "🍕"
    );
    assert_eq!(
        batch
            .column(5)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        &[0, 255]
    );
    assert_eq!(
        batch
            .column(6)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap()
            .value(0),
        1
    );
    assert_eq!(
        batch
            .column(7)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        12345678901234567890123456781200000000
    );
    assert_eq!(
        batch
            .column(8)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .value(0),
        1_000_000
    );
    assert_eq!(
        batch
            .column(9)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .value(0),
        1_000
    );
    assert_eq!(
        batch
            .column(10)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .value(0),
        1
    );
    assert_eq!(
        batch
            .column(11)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .value(0),
        1
    );
    assert_eq!(
        batch
            .column(12)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .value(0),
        0
    );
    assert!(
        batch
            .columns()
            .iter()
            .all(|column| column.null_count() == 0)
    );
    peer.channel
        .send(
            &Frame::BatchAck {
                req,
                table,
                seq,
                slot,
            },
            &[],
        )
        .unwrap();
    assert!(matches!(
        peer.channel.receive().unwrap().frame,
        Frame::TableComplete { .. }
    ));
    assert!(matches!(
        peer.channel.receive().unwrap().frame,
        Frame::SourceComplete { .. }
    ));
    peer.close();
}

#[test]
fn negotiated_default_budgets_transport_a_five_mib_row_without_half_batch_restriction() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source.duckdb");
    fixture(&path, Some("large"));
    let root = directory.path().join("grv");
    std::fs::create_dir(&root).unwrap();
    let mut peer = Peer::start(&path, Uuid::v4(), Resources::default());
    let req = peer.extract(
        &root,
        vec![table(
            "large",
            "main.large",
            None,
            &[("payload", "payload", json!("string"))],
        )],
    );
    let checkpoint_id = checkpoint(&mut peer);
    peer.channel
        .send(&Frame::CheckpointAck { req, checkpoint_id }, &[])
        .unwrap();
    let packet = peer.channel.receive().unwrap();
    let Frame::Batch {
        table,
        seq,
        slot,
        rows,
        ..
    } = packet.frame
    else {
        panic!("expected large batch")
    };
    assert_eq!(rows.get(), 1);
    assert!(packet.payload.len() <= 8 * 1024 * 1024);
    let mut reader =
        arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(packet.payload), None)
            .unwrap();
    let batch = reader.next().unwrap().unwrap();
    let value = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap()
        .value(0);
    assert_eq!(value.len(), 5 * 1024 * 1024);
    assert!(value.bytes().all(|byte| byte == b'x'));
    peer.channel
        .send(
            &Frame::BatchAck {
                req,
                table,
                seq,
                slot,
            },
            &[],
        )
        .unwrap();
    assert!(matches!(
        peer.channel.receive().unwrap().frame,
        Frame::TableComplete { .. }
    ));
    assert!(matches!(
        peer.channel.receive().unwrap().frame,
        Frame::SourceComplete { .. }
    ));
    peer.close();
}
