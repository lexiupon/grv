use grv_adapter_api::*;
use grv_adapter_wire::{
    Codec, ipc,
    state::{Role, State},
};
use serde_json::{Value, json};
use std::io::{self, Cursor, Read, Write};
fn f(v: Value) -> Frame {
    serde_json::from_value(v).unwrap()
}
const UUID: &str = "359c6d0f-a9c1-4ae6-b804-0742a5e2b9de";
fn identity() -> Value {
    json!({"name":"fixture","package_version":"0.1.0","interface_version":1,"binding_schema_version":1})
}
fn boot() -> State {
    let mut s = State::new(Role::Parent);
    s.observe(&f(json!({"msg":"hello","interface_versions":[1],"core":{"name":"grv","version":"0.1.0"},"attempt":UUID,"resources":Resources::default()})),true).unwrap();
    s.observe(&f(json!({"msg":"identified","name":"fixture","package_version":"0.1.0","interface_versions":[1],"binding_schema_version":1,"capabilities":Capabilities::default(),"registry":{"schema_bundle":{"$schema":"https://json-schema.org/draft/2020-12/schema","$defs":{}},"points":[]},"commands":[]})),false).unwrap();
    s.observe(&f(json!({"msg":"ready","interface_version":1,"binding_schema_version":1,"resources":Resources::default()})),true).unwrap();
    s
}
fn extraction() -> State {
    let mut s = boot();
    s.observe(&f(json!({"msg":"extract","req":1,"handle":"h","payload":{"inline":{"attempt_id":UUID,"stream_id":Uuid::v4(),"root":"/test","dataset":"data","run_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","declaration_sha256":"a".repeat(64),"adapter_identity":identity(),"connection_identity":"fixture","selection":{"policy":"all"},"options":{},"tables":[{"name":"rows","source":{},"columns":{},"contract":{"columns":[{"name":"value","type":"int64"}],"partition_keys":[],"extensions":{},"column_ext":{}}},{"name":"empty","source":{},"columns":{},"contract":{"columns":[{"name":"value","type":"int64"}],"partition_keys":[],"extensions":{},"column_ext":{}}}],"resume":null}}})),true).unwrap();
    s.observe(&f(json!({"msg":"extract_started","req":1})), false)
        .unwrap();
    s
}
fn checkpoint() -> Frame {
    f(
        json!({"msg":"checkpoint","req":1,"checkpoint_id":UUID,"payload":{"inline":{"attempt_id":UUID,"adapter_identity":identity(),"connection_identity":"fixture","tables":[{"table":"rows","snapshot_id":"one","reopenable":false,"source_identity":{},"capture_start":"2026-10-06T00:00:00Z"},{"table":"empty","snapshot_id":"two","reopenable":false,"source_identity":{},"capture_start":"2026-10-06T00:00:00Z"}],"job":{}}}}),
    )
}
fn batch(seq: u64, slot: u64) -> Frame {
    f(json!({"msg":"batch","req":1,"table":"rows","seq":seq,"slot":slot,"size":"8","rows":"1"}))
}
fn complete(table: &str, count: &str) -> Frame {
    f(
        json!({"msg":"table_complete","req":1,"table":table,"row_count":count,"source_identity":{"inline":{}},"capture":{"start":"2026-10-06T00:00:00Z","end":"2026-10-06T00:00:01Z"}}),
    )
}
struct Fragmented {
    read: Cursor<Vec<u8>>,
    written: Vec<u8>,
    fragment: usize,
}
impl Read for Fragmented {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        let n = b.len().min(self.fragment);
        self.read.read(&mut b[..n])
    }
}
impl Write for Fragmented {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        let n = b.len().min(self.fragment);
        self.written.extend(&b[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
#[test]
fn eof_transport_errors_and_malformed_truncation_remain_distinct() {
    use grv_adapter_wire::ProtocolError;
    let mut empty = Codec::new(Cursor::new(Vec::<u8>::new()), 1024);
    assert!(matches!(empty.read(), Err(ProtocolError::Transport(_))));
    for bytes in [b"{\"msg\":\"close\",\"req\":1}".to_vec(), b"{}\n".to_vec(), b"{\"msg\":\"batch\",\"req\":1,\"table\":\"rows\",\"seq\":0,\"slot\":0,\"size\":\"8\",\"rows\":\"1\"}\nxx".to_vec()] {
        let mut malformed = Codec::new(Cursor::new(bytes), 1024);
        assert!(matches!(malformed.read(), Err(ProtocolError::Invalid(_))));
    }
    struct Broken;
    impl Read for Broken {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::ConnectionReset, "reset"))
        }
    }
    impl Write for Broken {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "pipe closed"))
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut failed = Codec::new(Broken, 1024);
    assert!(matches!(failed.read(), Err(ProtocolError::Transport(_))));
    assert!(matches!(
        failed.write(
            &Frame::Close {
                req: Req::new(1).unwrap()
            },
            &[]
        ),
        Err(ProtocolError::Transport(_))
    ));
}
#[test]
fn after_publish_is_closed_true_only_and_correlated() {
    let request = json!({"msg":"after_publish","req":1,"handle":"h","attempt_id":UUID,"declaration_sha256":"a".repeat(64),"outcome":{"kind":"no-op","revision":"0","operation_id":null}});
    let mut missing = request.clone();
    missing["outcome"]
        .as_object_mut()
        .unwrap()
        .remove("operation_id");
    assert!(serde_json::from_value::<Frame>(missing).is_err());
    let mut unknown = request.clone();
    unknown["source"] = json!({});
    assert!(serde_json::from_value::<Frame>(unknown).is_err());
    for result in [
        json!({"msg":"after_publish_result","req":2,"acknowledged":true}),
        json!({"msg":"after_publish_result","req":1,"acknowledged":false}),
        json!({"msg":"authenticate_result","req":1,"identity":"unexpected"}),
    ] {
        let mut state = boot();
        state.observe(&f(request.clone()), true).unwrap();
        assert!(state.observe(&f(result), false).is_err());
    }
    let mut state = boot();
    state.observe(&f(request), true).unwrap();
    state
        .observe(
            &f(json!({"msg":"after_publish_result","req":1,"acknowledged":true})),
            false,
        )
        .unwrap();
}
#[test]
fn inspection_has_exact_request_result_correlation_and_required_null_scope() {
    assert!(
        serde_json::from_value::<Frame>(
            json!({"msg":"inspect_connection","req":1,"handle":"h","root":null})
        )
        .is_err()
    );
    for response in [
        json!({"msg":"inspect_result","req":2,"details":{"inline":{}}}),
        json!({"msg":"authenticate_result","req":1,"identity":"wrong"}),
    ] {
        let mut state = boot();
        state.observe(&f(json!({"msg":"inspect_connection","req":1,"handle":"h","root":null,"declaration":null})),true).unwrap();
        assert!(state.observe(&f(response), false).is_err());
    }
    let mut state = boot();
    state.observe(&f(json!({"msg":"inspect_connection","req":1,"handle":"h","root":null,"declaration":{"inline":{"dataset":"data"}}})),true).unwrap();
    state
        .observe(
            &f(json!({"msg":"inspect_result","req":1,"details":{"inline":{}}})),
            false,
        )
        .unwrap();
    state
        .observe(&f(json!({"msg":"close","req":2})), true)
        .unwrap();
    state
        .observe(&f(json!({"msg":"close_result","req":2})), false)
        .unwrap();
    assert!(state.is_closed());
}
#[test]
fn fragmented_and_coalesced_frames_payloads_and_partial_writes() {
    for fragment in [1, 2, 7, 1024] {
        let input=b"{\"msg\":\"batch\",\"req\":1,\"table\":\"rows\",\"seq\":0,\"slot\":0,\"size\":\"8\",\"rows\":\"1\"}\n\0\n{}\xff\0\0\0{\"msg\":\"close\",\"req\":2}\n".to_vec();
        let mut c = Codec::new(
            Fragmented {
                read: Cursor::new(input),
                written: vec![],
                fragment,
            },
            1024,
        );
        let p = c.read().unwrap();
        assert_eq!(p.payload, b"\0\n{}\xff\0\0\0");
        assert!(matches!(c.read().unwrap().frame, Frame::Close { .. }));
        c.write(&p.frame, &p.payload).unwrap();
        assert!(c.into_inner().written.ends_with(&p.payload));
    }
}
#[test]
fn malformed_frames_reject_utf8_duplicates_unknown_fields_sizes_and_truncation() {
    for input in [
        b"\xff\n".as_slice(),
        b"\xef\xbb\xbf{}\n",
        b"\n",
        b"{\"msg\":\"close\",\"req\":1,\"req\":2}\n",
        b"{\"msg\":\"close\",\"req\":1,\"extra\":true}\n",
        b"{\"msg\":\"close\",\"req\":9007199254740992}\n",
        b"{\"msg\":\"close\",\"req\":1}{}\n",
        b"{\"msg\":\"close\",\"req\":1}",
    ] {
        assert!(
            Codec::new(Cursor::new(input.to_vec()), 1024)
                .read()
                .is_err()
        );
    }
    for size in ["0", "7", "1032", "01"] {
        let line = format!(
            "{{\"msg\":\"batch\",\"req\":1,\"table\":\"rows\",\"seq\":0,\"slot\":0,\"size\":\"{size}\",\"rows\":\"1\"}}\n"
        );
        assert!(
            Codec::new(Cursor::new(line.into_bytes()), 1024)
                .read()
                .is_err()
        );
    }
    assert!(
        Codec::new(
            Cursor::new(
                format!("{}\n123", serde_json::to_string(&batch(0, 0)).unwrap()).into_bytes()
            ),
            1024
        )
        .read()
        .is_err()
    );
}
#[test]
fn checkpoint_ack_precedes_rows_and_empty_table_completion() {
    let mut s = extraction();
    assert!(s.observe(&batch(0, 0), false).is_err());
    assert!(s.observe(&complete("empty", "0"), false).is_err());
    s.observe(&checkpoint(), false).unwrap();
    assert!(s.observe(&batch(0, 0), false).is_err());
    s.observe(
        &f(json!({"msg":"checkpoint_ack","req":1,"checkpoint_id":UUID})),
        true,
    )
    .unwrap();
    s.observe(&complete("empty", "0"), false).unwrap();
    s.observe(&batch(0, 0), false).unwrap();
    assert!(s.observe(&complete("rows", "1"), false).is_err());
}
#[test]
fn exhausted_credits_accept_cancel_but_freeze_new_acknowledgements() {
    let mut s = extraction();
    s.observe(&checkpoint(), false).unwrap();
    s.observe(
        &f(json!({"msg":"checkpoint_ack","req":1,"checkpoint_id":UUID})),
        true,
    )
    .unwrap();
    for i in 0..4 {
        s.observe(&batch(i, i), false).unwrap();
    }
    assert!(s.observe(&batch(4, 0), false).is_err());
    s.observe(&f(json!({"msg":"cancel","req":1})), true)
        .unwrap();
    assert!(s.acknowledgements_frozen());
    assert!(
        s.observe(
            &f(json!({"msg":"batch_ack","req":1,"table":"rows","seq":0,"slot":0})),
            true
        )
        .is_err()
    );
    s.observe(
        &f(json!({"msg":"cancel_ack","req":1,"state":"stopped"})),
        false,
    )
    .unwrap();
    assert!(s.observe(&batch(4, 0), false).is_err());
    assert!(
        s.observe(
            &f(json!({"msg":"prepare_command","req":2,"name":"echo","argv":{"inline":[]}})),
            true
        )
        .is_err()
    );
    s.observe(&f(json!({"msg":"close","req":2})), true).unwrap();
    s.observe(&f(json!({"msg":"close_result","req":2})), false)
        .unwrap();
}
#[test]
fn completion_wins_cancellation_race_and_duplicate_terminal_is_rejected() {
    let mut s = boot();
    s.observe(
        &f(json!({"msg":"prepare_command","req":1,"name":"echo","argv":{"inline":[]}})),
        true,
    )
    .unwrap();
    let terminal = f(
        json!({"msg":"command_prepared","req":1,"call":{"inline":{"command_id":UUID,"name":"echo","args":{},"connection":null}}}),
    );
    s.observe(&terminal, false).unwrap();
    s.observe(&f(json!({"msg":"cancel","req":1})), true)
        .unwrap();
    s.observe(
        &f(json!({"msg":"cancel_ack","req":1,"state":"completed"})),
        false,
    )
    .unwrap();
    assert!(s.observe(&terminal, false).is_err());
}
#[test]
fn document_hash_ack_canonicality_consumption_and_reuse() {
    use base64::Engine;
    let mut s = boot();
    let bytes = b"[\"hello\"]";
    let begin = f(
        json!({"msg":"document_begin","document_id":UUID,"size":bytes.len().to_string(),"sha256":grv_types::sha256(bytes)}),
    );
    s.observe(&begin, true).unwrap();
    s.observe(&f(json!({"msg":"document_chunk","document_id":UUID,"index":0,"data":base64::engine::general_purpose::STANDARD.encode(bytes)})),true).unwrap();
    s.observe(&f(json!({"msg":"document_end","document_id":UUID})), true)
        .unwrap();
    let request =
        f(json!({"msg":"prepare_command","req":1,"name":"echo","argv":{"document_id":UUID}}));
    assert!(s.observe(&request, true).is_err());
    s.observe(&f(json!({"msg":"document_ack","document_id":UUID})), false)
        .unwrap();
    assert!(matches!(
        s.resolved_frame(&request, true).unwrap(),
        Frame::PrepareCommand {
            argv: Doc::Inline(_),
            ..
        }
    ));
    s.observe(&request, true).unwrap();
    assert!(s.document(Role::Parent, UUID).is_err());
    assert!(s.observe(&begin, true).is_err());
    let mut s = boot();
    let bad = b"[ \"hello\" ]";
    s.observe(&f(json!({"msg":"document_begin","document_id":UUID,"size":bad.len().to_string(),"sha256":grv_types::sha256(bad)})),true).unwrap();
    s.observe(&f(json!({"msg":"document_chunk","document_id":UUID,"index":0,"data":base64::engine::general_purpose::STANDARD.encode(bad)})),true).unwrap();
    assert!(
        s.observe(&f(json!({"msg":"document_end","document_id":UUID})), true)
            .is_err()
    );
}
#[test]
fn request_correlation_role_and_resource_identity() {
    let mut s = boot();
    assert!(
        s.observe(&f(json!({"msg":"close","req":1})), false)
            .is_err()
    );
    s.observe(&f(json!({"msg":"close","req":1})), true).unwrap();
    assert!(
        s.observe(&f(json!({"msg":"close_result","req":2})), false)
            .is_err()
    );
    let mut s = State::new(Role::Parent);
    let r = Resources::default();
    s.observe(
        &Frame::Hello {
            interface_versions: vec![Req::new(1).unwrap()],
            core: CoreIdentity {
                name: "grv".into(),
                version: "1".into(),
            },
            attempt: Uuid::v4(),
            resources: r.clone(),
        },
        true,
    )
    .unwrap();
    assert!(
        s.observe(
            &Frame::Ready {
                interface_version: Req::new(1).unwrap(),
                binding_schema_version: Req::new(1).unwrap(),
                resources: r
            },
            true
        )
        .is_err()
    );
}
#[test]
fn ipc_two_message_validation_rejects_eos_trailing_and_bad_row_counts() {
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let mut bytes = vec![];
    {
        let mut w = arrow_ipc::writer::StreamWriter::try_new(&mut bytes, &schema).unwrap();
        w.write(&batch).unwrap();
    }
    assert_eq!(ipc::decode(&bytes, 3).unwrap().num_rows(), 3);
    assert!(ipc::decode(&bytes, 2).is_err());
    bytes.extend([255, 255, 255, 255, 0, 0, 0, 0]);
    assert!(ipc::decode(&bytes, 3).is_err());
}

#[test]
fn semantically_invalid_ipc_type_is_rejected_before_arrow_conversion_panics() {
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    let mut bytes = Vec::new();
    {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut bytes, &schema).unwrap();
        writer.write(&batch).unwrap();
    }
    let metadata = i32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    let message = arrow_ipc::root_as_message(&bytes[8..8 + metadata]).unwrap();
    let integer = message
        .header_as_schema()
        .unwrap()
        .fields()
        .unwrap()
        .get(0)
        .type_as_int()
        .unwrap();
    let location = 8
        + integer._tab.loc()
        + usize::from(integer._tab.vtable().get(arrow_ipc::Int::VT_BITWIDTH));
    bytes[location..location + 4].copy_from_slice(&17i32.to_le_bytes());
    let result = std::panic::catch_unwind(|| ipc::decode(&bytes, 1));
    assert!(result.is_ok());
    assert!(result.unwrap().is_err());
}

#[test]
fn oversized_adapter_document_has_distinct_typed_failure() {
    let mut state = boot();
    let error = state
        .observe(
            &Frame::DocumentBegin {
                document_id: Uuid::v4(),
                size: U64::new(64 * 1024 * 1024 + 1).unwrap(),
                sha256: Digest::new("a".repeat(64)).unwrap(),
            },
            false,
        )
        .unwrap_err();
    assert!(matches!(
        error,
        grv_adapter_wire::ProtocolError::DocumentTooLarge
    ));
}

fn build_session_value() -> Value {
    json!({"session_id":UUID,"options":{},"identity":{"attempt_id":UUID,"root":"/root","dataset":"product","run_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","workspace_id":UUID,"declaration_sha256":"a".repeat(64),"adapter_identity":identity(),"connection_identity":"fixture"},"execution":"managed","base_revision":"0","base_contracts":[],"inputs":[],"outputs":[{"table":"rows","source":{"sql":"SELECT 1 AS value"},"engine_table":"_private.rows","columns":[{"name":"value","type":"int64"}],"contract":{"columns":[{"name":"value","type":"int64"}],"partition_keys":[],"extensions":{},"column_ext":{}}},{"table":"empty","source":{"sql":"SELECT 1 AS value WHERE false"},"engine_table":"_private.empty","columns":[{"name":"value","type":"int64"}],"contract":{"columns":[{"name":"value","type":"int64"}],"partition_keys":[],"extensions":{},"column_ext":{}}}],"selected_outputs":["rows","empty"],"self_input":false,"adapter_details":{}})
}
fn build_completion_value() -> Value {
    json!({"result_version":1,"run_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","workspace_id":UUID,"declaration_sha256":"a".repeat(64),"kind":"engine","invocation_id":"fixed","status":"succeeded","writers_stopped":true,"completed_at":"2026-10-06T00:00:00Z","completed_outputs":[{"table":"rows","engine_table":"_private.rows"},{"table":"empty","engine_table":"_private.empty"}]})
}
fn prepared_build_state() -> State {
    let mut s = boot();
    let session = build_session_value();
    s.observe(&f(json!({"msg":"prepare_build","req":1,"handle":"h","payload":{"inline":{"discovery":{"discovery_id":UUID,"identity":session["identity"],"inputs":[],"outputs":session["outputs"]},"base_revision":"0","self_input":false,"base_contracts":[],"base_files":[],"input_files":[],"holds_confirmed":true}}})),true).unwrap();
    s.observe(
        &f(json!({"msg":"build_prepared","req":1,"session":{"inline":session}})),
        false,
    )
    .unwrap();
    s
}
fn accepted_build_state() -> State {
    let mut s = prepared_build_state();
    let session = build_session_value();
    let completion = build_completion_value();
    s.observe(&f(json!({"msg":"execute_build","req":2,"handle":"h","payload":{"inline":{"session":session,"queries":[{"table":"rows","sql":"SELECT 1 AS value"},{"table":"empty","sql":"SELECT 1 AS value WHERE false"}]}}})),true).unwrap();
    s.observe(&f(json!({"msg":"build_finished","req":2,"result":{"inline":{"status":"succeeded","completion":completion,"row_counts":[{"table":"rows","rows":"1"},{"table":"empty","rows":"0"}]}}})),false).unwrap();
    s.observe(&f(json!({"msg":"accept_build_completion","req":3,"handle":"h","session_id":UUID,"completion":{"inline":completion}})),true).unwrap();
    let digest = grv_types::sha256(&grv_types::canonical_json(&completion).unwrap());
    s.observe(&f(json!({"msg":"completion_accepted","req":3,"session_id":UUID,"completion_sha256":digest})),false).unwrap();
    s
}
fn start_build_export(s: &mut State) {
    let digest = grv_types::sha256(&grv_types::canonical_json(&build_completion_value()).unwrap());
    s.observe(&f(json!({"msg":"export_build","req":4,"handle":"h","session_id":UUID,"completion_sha256":digest,"stream_id":Uuid::v4()})),true).unwrap();
}
#[test]
fn build_rows_only_after_accepted_completion_and_never_during_execution() {
    let mut s = prepared_build_state();
    let digest = grv_types::sha256(&grv_types::canonical_json(&build_completion_value()).unwrap());
    assert!(s.observe(&f(json!({"msg":"export_build","req":2,"handle":"h","session_id":UUID,"completion_sha256":digest,"stream_id":Uuid::v4()})),true).is_err());
    let session = build_session_value();
    s.observe(&f(json!({"msg":"execute_build","req":2,"handle":"h","payload":{"inline":{"session":session,"queries":[{"table":"rows","sql":"SELECT 1 AS value"},{"table":"empty","sql":"SELECT 1 AS value WHERE false"}]}}})),true).unwrap();
    assert!(
        s.observe(
            &f(
                json!({"msg":"batch","req":2,"table":"rows","seq":0,"slot":0,"size":"8","rows":"1"})
            ),
            false
        )
        .is_err()
    );
}
#[test]
fn build_export_credit_drain_zero_completion_and_bound_terminal_identity() {
    let mut s = accepted_build_state();
    start_build_export(&mut s);
    s.observe(
        &f(json!({"msg":"batch","req":4,"table":"rows","seq":0,"slot":0,"size":"8","rows":"1"})),
        false,
    )
    .unwrap();
    assert!(
        s.observe(
            &f(json!({"msg":"build_table_complete","req":4,"table":"rows","row_count":"1"})),
            false
        )
        .is_err()
    );
    s.observe(
        &f(json!({"msg":"batch_ack","req":4,"table":"rows","seq":0,"slot":0})),
        true,
    )
    .unwrap();
    assert!(
        s.observe(
            &f(json!({"msg":"build_table_complete","req":4,"table":"rows","row_count":"2"})),
            false
        )
        .is_err()
    );
    // Failed observations may conservatively update sets, so the next adversary
    // starts with independently established valid lifecycle state.
    let mut s = accepted_build_state();
    start_build_export(&mut s);
    s.observe(
        &f(json!({"msg":"batch","req":4,"table":"rows","seq":0,"slot":0,"size":"8","rows":"1"})),
        false,
    )
    .unwrap();
    s.observe(
        &f(json!({"msg":"batch_ack","req":4,"table":"rows","seq":0,"slot":0})),
        true,
    )
    .unwrap();
    s.observe(
        &f(json!({"msg":"build_table_complete","req":4,"table":"rows","row_count":"1"})),
        false,
    )
    .unwrap();
    let digest = grv_types::sha256(&grv_types::canonical_json(&build_completion_value()).unwrap());
    let terminal = f(
        json!({"msg":"export_complete","req":4,"session_id":UUID,"completion_sha256":digest,"adapter_result":{"inline":{}}}),
    );
    assert!(s.observe(&terminal, false).is_err());
    s.observe(
        &f(json!({"msg":"build_table_complete","req":4,"table":"empty","row_count":"0"})),
        false,
    )
    .unwrap();
    s.observe(&terminal, false).unwrap();
}
#[test]
fn build_export_rejects_extraction_checkpoint_and_completion_fields() {
    for event in [
        json!({"msg":"extract_started","req":4}),
        json!({"msg":"table_complete","req":4,"table":"empty","row_count":"0","source_identity":{"inline":{}},"capture":{"start":"2026-10-06T00:00:00Z","end":"2026-10-06T00:00:00Z"}}),
    ] {
        let mut s = accepted_build_state();
        start_build_export(&mut s);
        assert!(s.observe(&f(event), false).is_err());
    }
}
