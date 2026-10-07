#![cfg(unix)]
use grv_adapter_api::*;
use grv_adapter_salesforce::process::SalesforceAdapter;
use grv_adapter_wire::{Channel, state::Role};
use serde_json::json;
use std::{
    fs,
    os::unix::{fs::PermissionsExt, net::UnixStream},
    path::PathBuf,
    time::Duration,
};

struct Root(PathBuf);
impl Drop for Root {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn script(root: &std::path::Path, name: &str, text: &str) -> std::ffi::OsString {
    let path = root.join(name);
    fs::write(&path, format!("#!/bin/sh\n{text}\n")).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    path.into_os_string()
}
fn req(n: u64) -> Req {
    Req::new(n).unwrap()
}
fn inline<T>(doc: Doc<T>) -> T {
    match doc {
        Doc::Inline(doc) => doc.inline,
        Doc::Reference(_) => panic!("small fixture is inline"),
    }
}

#[test]
fn production_helpers_sdk_rest_pages_credits_empty_table_and_retry_evidence_integrate() {
    exercise("rest", None);
}
#[test]
fn production_helpers_sdk_bulk_pages_credits_empty_table_and_retry_evidence_integrate() {
    exercise("bulk", None);
}
#[test]
fn bulk_ambiguous_creation_never_repeats_post_on_retry() {
    exercise("bulk", Some("ambiguous"));
}
#[test]
fn bulk_count_mismatch_never_completes_or_requeries_attempt() {
    exercise("bulk", Some("count"));
}
#[test]
fn bulk_unproven_text_refuses_before_creation_and_does_not_turn_into_unknown_creation() {
    exercise("bulk", Some("text"));
}
#[test]
fn bulk_cancellation_stops_http_poll_and_descendants_before_acknowledging() {
    exercise("bulk", Some("cancel"));
}
fn exercise(transport: &str, fault: Option<&str>) {
    let root = Root(
        fs::canonicalize(env!("CARGO_MANIFEST_DIR"))
            .unwrap()
            .join(format!(".extraction-test-{}", uuid::Uuid::new_v4())),
    );
    fs::create_dir(&root.0).unwrap();
    fs::set_permissions(&root.0, fs::Permissions::from_mode(0o700)).unwrap();
    let sf = script(
        &root.0,
        "sf",
        r#"[ "$SF_DISABLE_LOG_FILE" = true ] || exit 1
[ "$SFDX_DISABLE_LOG_FILE" = true ] || exit 1
printf '%s' '{"status":0,"result":{"id":"00D000000000001AAA","instanceUrl":"https://test.my.salesforce.com","accessToken":"credential-canary"}}'"#,
    );
    let bulk_script = r#"input=; while IFS= read -r line; do input="$input$line"; done
case "$input" in
 *Organization*) printf 'HTTP/1.1 200 OK\r\n\r\n%s' '{"totalSize":1,"done":true,"records":[{"Id":"00D000000000001AAA"}]}' ;;
 *sobjects/Case/describe*) printf 'HTTP/1.1 200 OK\r\n\r\n%s' '{"name":"Case","fields":[{"name":"Id","type":"id","nillable":false},{"name":"Amount__c","type":"currency","precision":38,"scale":6,"nillable":true},{"name":"SystemModstamp","type":"datetime","nillable":false}]}' ;;
 *sobjects/Contact/describe*) printf 'HTTP/1.1 200 OK\r\n\r\n%s' '{"name":"Contact","fields":[{"name":"Id","type":"id","nillable":false},{"name":"Amount__c","type":"currency","precision":38,"scale":6,"nillable":true},{"name":"SystemModstamp","type":"datetime","nillable":false}]}' ;;
 *results*locator=second*) printf 'HTTP/1.1 200 OK\r\nSforce-Locator: null\r\nSforce-NumberOfRecords: 1\r\n\r\nId,Amount__c,SystemModstamp\nsecond,0.000001,1970-01-01T00:00:00.000001Z\n' ;;
 *750000000000001/results*) printf 'HTTP/1.1 200 OK\r\nSforce-Locator: second\r\nSforce-NumberOfRecords: 1\r\n\r\nId,Amount__c,SystemModstamp\nfirst,12345678901234567890123456789012.345678,1970-01-01T00:00:00Z\n' ;;
 *750000000000002/results*) printf 'HTTP/1.1 200 OK\r\nSforce-Locator: null\r\nSforce-NumberOfRecords: 0\r\n\r\nId,Amount__c,SystemModstamp\n' ;;
 *750000000000001*) printf 'HTTP/1.1 200 OK\r\n\r\n%s' '{"id":"750000000000001","state":"JobComplete","numberRecordsProcessed":2}' ;;
 *750000000000002*) printf 'HTTP/1.1 200 OK\r\n\r\n%s' '{"id":"750000000000002","state":"JobComplete","numberRecordsProcessed":0}' ;;
 *'FROM Case'*) printf 'HTTP/1.1 200 OK\r\n\r\n%s' '{"id":"750000000000001"}' ;;
 *'FROM Contact'*) printf 'HTTP/1.1 200 OK\r\n\r\n%s' '{"id":"750000000000002"}' ;;
 *) exit 2;;
esac"#;
    let rest_script = r#"input=; while IFS= read -r line; do input="$input$line"; done
printf 'HTTP/1.1 200 OK\r\n\r\n'
case "$input" in
 *sobjects/Case/describe*) printf '%s' '{"name":"Case","fields":[{"name":"Id","type":"id","nillable":false},{"name":"Amount__c","type":"currency","precision":38,"scale":6,"nillable":true},{"name":"SystemModstamp","type":"datetime","nillable":false}]}' ;;
 *sobjects/Contact/describe*) printf '%s' '{"name":"Contact","fields":[{"name":"Id","type":"id","nillable":false},{"name":"Amount__c","type":"currency","precision":38,"scale":6,"nillable":true},{"name":"SystemModstamp","type":"datetime","nillable":false}]}' ;;
 *Organization*) printf '%s' '{"totalSize":1,"done":true,"records":[{"Id":"00D000000000001AAA"}]}' ;;
 *case-locator*) printf '%s' '{"totalSize":2,"done":true,"records":[{"Id":"second","Amount__c":0.000001,"SystemModstamp":"1970-01-01T00:00:00.000001Z"}]}' ;;
 *FROM%20Case*) printf '%s' '{"totalSize":2,"done":false,"nextRecordsUrl":"/services/data/v66.0/query/case-locator","records":[{"Id":"first","Amount__c":12345678901234567890123456789012.345678,"SystemModstamp":"1970-01-01T00:00:00Z"}]}' ;;
 *FROM%20Contact*) printf '%s' '{"totalSize":0,"done":true,"records":[]}' ;;
 *) exit 2;;
esac"#;
    let mut bulk_script = bulk_script.to_owned();
    match fault {
        Some("ambiguous") => {
            bulk_script = bulk_script.replace("'{\"id\":\"750000000000001\"}'", "'{\"id\":null}'")
        }
        Some("count") => {
            bulk_script = bulk_script.replace(
                "\"numberRecordsProcessed\":2",
                "\"numberRecordsProcessed\":3",
            )
        }
        Some("text") => {
            bulk_script = bulk_script.replace(
                "\"type\":\"id\",\"nillable\":false",
                "\"type\":\"string\",\"nillable\":true",
            )
        }
        _ => (),
    }
    let polling = root.0.join("polling");
    if fault == Some("cancel") {
        let marker = format!("'{}'", polling.to_string_lossy().replace('\'', "'\\''"));
        bulk_script = bulk_script.replace(
            "*750000000000001*) printf",
            &format!("*750000000000001*) printf 'polling' > {marker}; exec sleep 30; printf"),
        );
    }
    let calls = root.0.join("post-count");
    let quoted_calls = format!("'{}'", calls.to_string_lossy().replace('\'', "'\\''"));
    bulk_script = bulk_script.replace(
        r#"input=; while IFS= read -r line; do input="$input$line"; done"#,
        &format!(
            r#"input=; while IFS= read -r line; do input="$input$line"; done
case "$input" in *'request = "POST"'*) printf 'post\n' >> {quoted_calls};; esac"#
        ),
    );

    let curl = script(
        &root.0,
        "curl",
        if transport == "bulk" {
            &bulk_script
        } else {
            rest_script
        },
    );
    let (parent, child) = UnixStream::pair().unwrap();
    parent
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut adapter = SalesforceAdapter::with_programs(root.0.clone(), sf, curl);
    adapter.supervisor = Some(env!("CARGO_BIN_EXE_grv-adapter-salesforce").into());
    let server = std::thread::spawn(move || grv_adapter_sdk::serve(adapter, child));
    let resources = Resources {
        slots: req(1),
        ..Resources::default()
    };
    let mut channel = Channel::new(
        parent,
        Role::Parent,
        resources.max_batch_bytes.get() as usize,
    );
    channel
        .send(
            &Frame::Hello {
                interface_versions: vec![req(1)],
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
    assert!(matches!(
        channel.receive().unwrap().frame,
        Frame::Identified { .. }
    ));
    channel
        .send(
            &Frame::Ready {
                interface_version: req(1),
                binding_schema_version: req(1),
                resources,
            },
            &[],
        )
        .unwrap();
    channel
        .send(
            &Frame::LocateConnection {
                req: req(1),
                connection: Doc::inline(json!({"org":"user@example.org","api_version":"v66.0"})),
                mode: Mode::Extract,
                run_id: None,
            },
            &[],
        )
        .unwrap();
    let Frame::ConnectionLocated { connection, .. } = channel.receive().unwrap().frame else {
        panic!("locate failed")
    };
    channel
        .send(
            &Frame::BindConnection {
                req: req(2),
                locator: Doc::inline(inline(connection)),
                root: None,
                expected_identity: None,
                expected_workspace_id: None,
                mode: Mode::Extract,
            },
            &[],
        )
        .unwrap();
    let Frame::BindResult { handle, .. } = channel.receive().unwrap().frame else {
        panic!("bind failed")
    };
    channel
        .send(
            &Frame::Authenticate {
                req: req(3),
                handle: handle.clone(),
                expected_identity: None,
            },
            &[],
        )
        .unwrap();
    let Frame::AuthenticateResult { identity, .. } = channel.receive().unwrap().frame else {
        panic!("authenticate failed")
    };
    let contract = TableContract {
        columns: vec![
            Column {
                name: "id".into(),
                logical_type: json!("string"),
            },
            Column {
                name: "amount".into(),
                logical_type: json!({"decimal":{"precision":38,"scale":6}}),
            },
            Column {
                name: "modified".into(),
                logical_type: json!({"timestamp":{"unit":"us","utc":true}}),
            },
        ],
        partition_keys: vec![],
        extensions: json!({}),
        column_ext: json!({}),
    };
    let tables=[("cases","Case"),("contacts","Contact")].into_iter().map(|(name,object)|ExtractTable{name:Name::new(name).unwrap(),source:json!({"object":object}),columns:json!([{"name":"id","source":"Id"},{"name":"amount","source":"Amount__c"},{"name":"modified","source":"SystemModstamp"}]),contract:contract.clone()}).collect();
    let attempt = Uuid::v4();
    let request = ExtractRequest {
        attempt_id: attempt.clone(),
        stream_id: Uuid::v4(),
        root: "/fixture-grv".into(),
        dataset: Name::new("data").unwrap(),
        run_id: RunId::new("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
        declaration_sha256: Digest::new("1".repeat(64)).unwrap(),
        adapter_identity: AdapterIdentity {
            name: Name::new("salesforce").unwrap(),
            package_version: "0.1.0".into(),
            interface_version: req(1),
            binding_schema_version: req(1),
        },
        connection_identity: identity,
        selection: ExtractSelection {
            policy: SelectionPolicy::All,
        },
        options: json!({"transport":if transport == "bulk" {"bulk"} else {"auto"},"all_rows":false}),
        tables,
        resume: None,
    };
    channel
        .send(
            &Frame::Extract {
                req: req(4),
                handle: handle.clone(),
                payload: Doc::inline(request.clone()),
            },
            &[],
        )
        .unwrap();
    assert!(matches!(
        channel.receive().unwrap().frame,
        Frame::ExtractStarted { .. }
    ));
    let mut checkpoints = 0;
    let mut batches = 0;
    let mut complete = Vec::new();
    loop {
        let packet = channel.receive().unwrap();
        match packet.frame {
            Frame::Checkpoint {
                checkpoint_id,
                payload,
                ..
            } => {
                let checkpoint = inline(payload);
                checkpoints += 1;
                assert_eq!(checkpoint.tables.len(), checkpoints);
                assert!(checkpoint.tables.iter().all(|t| !t.reopenable));
                assert!(
                    checkpoint
                        .tables
                        .iter()
                        .all(|t| t.source_identity["org_id"] == "00D000000000001AAA")
                );
                channel
                    .send(
                        &Frame::CheckpointAck {
                            req: req(4),
                            checkpoint_id,
                        },
                        &[],
                    )
                    .unwrap();
                if fault == Some("cancel") {
                    let deadline = std::time::Instant::now() + Duration::from_secs(2);
                    while !polling.exists() {
                        assert!(
                            std::time::Instant::now() < deadline,
                            "Bulk polling helper did not start"
                        );
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    channel.send(&Frame::Cancel { req: req(4) }, &[]).unwrap();
                }
            }
            Frame::Batch {
                table,
                seq,
                slot,
                rows,
                ..
            } => {
                assert_eq!(checkpoints, 1);
                assert_eq!(table.as_str(), "cases");
                assert_eq!(rows.get(), 1);
                let batch = grv_adapter_wire::ipc::decode(&packet.payload, rows.get()).unwrap();
                assert_eq!(batch.num_columns(), 3);
                batches += 1;
                std::thread::sleep(Duration::from_millis(25));
                channel
                    .send(
                        &Frame::BatchAck {
                            req: req(4),
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
            } => complete.push((table.as_str().to_owned(), row_count.get())),
            Frame::SourceComplete { completion, .. } => {
                let completion = inline(completion);
                assert!(fault.is_none(), "fault must refuse terminal completion");
                assert_eq!(completion.job["transport"], transport);
                assert_eq!(
                    completion.job["job_ids"].as_array().unwrap().len(),
                    if transport == "bulk" { 2 } else { 1 }
                );
                break;
            }
            Frame::CancelAck { state, .. } if fault == Some("cancel") => {
                assert_eq!(state, CancelState::Stopped);
                break;
            }
            Frame::Error { code, .. } if fault.is_some() => {
                assert_eq!(
                    code,
                    match fault {
                        Some("ambiguous") => ErrorCode::OutcomeUnknown,
                        Some("text") => ErrorCode::UnsupportedCapability,
                        _ => ErrorCode::IntegrityFailure,
                    }
                );
                break;
            }
            other => panic!("unexpected source frame: {other:?}"),
        }
    }
    if fault.is_none() {
        assert_eq!(checkpoints, 2);
        assert_eq!(batches, 2);
        assert_eq!(complete, vec![("cases".into(), 2), ("contacts".into(), 0)]);
    } else {
        assert!(complete.is_empty());
        assert_eq!(
            checkpoints,
            if matches!(fault, Some("count" | "cancel")) {
                1
            } else {
                0
            }
        );
        assert_eq!(batches, if fault == Some("count") { 2 } else { 0 });
    }
    if fault.is_none() {
        channel
            .send(
                &Frame::Extract {
                    req: req(5),
                    handle,
                    payload: Doc::inline(request.clone()),
                },
                &[],
            )
            .unwrap();
        assert!(matches!(
            channel.receive().unwrap().frame,
            Frame::Error {
                code: ErrorCode::ExtractionIncomplete,
                ..
            }
        ));
    }
    if transport == "bulk" {
        let posts = fs::read_to_string(&calls).unwrap_or_default();
        assert_eq!(
            posts.lines().count(),
            match fault {
                Some("text") => 0,
                Some(_) => 1,
                None => 2,
            }
        );
    }
    channel.send(&Frame::Close { req: req(6) }, &[]).unwrap();
    assert!(matches!(
        channel.receive().unwrap().frame,
        Frame::CloseResult { .. }
    ));
    server.join().unwrap().unwrap();
    let journal = root.0.join(".local/state/grv/adapters/salesforce");
    // A failed source channel ends ordinary work. Retry opens independent
    // journal state and must fail before even attempting to spawn HTTP.
    let reopened = grv_adapter_salesforce::journal::Journal::open(&journal).unwrap();
    let session = std::sync::Arc::new(
        grv_adapter_salesforce::auth::AuthenticatedSession::new(
            grv_adapter_salesforce::auth::OrgId::parse("00D000000000001AAA").unwrap(),
            "https://test.my.salesforce.com".into(),
            "credential-canary".into(),
        )
        .unwrap(),
    );
    let mut request = request;
    request.stream_id = Uuid::v4();
    let retry = grv_adapter_salesforce::extraction::RestProducer::new(
        reopened,
        request,
        session,
        grv_adapter_salesforce::http::SalesforceHttp {
            executor: grv_adapter_salesforce::http::CurlHttp {
                program: "a-program-that-must-never-execute".into(),
                ..Default::default()
            },
        },
        "v66.0".into(),
        &Resources::default(),
    );
    let error = match retry {
        Err(error) => error,
        Ok(_) => panic!("retry must refuse recorded acquisition"),
    };
    assert_eq!(
        error.code,
        if fault == Some("ambiguous") {
            "OUTCOME_UNKNOWN"
        } else {
            "EXTRACTION_INCOMPLETE"
        }
    );
    let evidence = fs::read_to_string(journal.join(format!("{}--cases.json", attempt))).unwrap();
    assert_eq!(evidence.contains("capture_end"), fault.is_none());
    assert!(!evidence.contains("credential-canary"));
}
