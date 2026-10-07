use grv_adapter_api::*;
use grv_adapter_host::{
    discovery::{self, RootKind, SearchRoot},
    process::{Deadlines, Session},
};
use grv_adapter_salesforce::process::SalesforceAdapter;
use grv_core::{
    capture::{AcquisitionProgress, CaptureJob, CaptureReceipt, stage_group},
    clock::SystemClock,
    journal::{Envelope, Evidence, Journal},
    normalize::TablePlan,
    ownership::{Ownership, ReservationProgress, RunOwner},
    publication::{LeaseProgress, Publisher},
    store::{InitOptions, Store},
};
use grv_storage::{
    LocalBackend,
    model::{ChangeSet, ClaimOutcome, Counter},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{os::unix::fs::PermissionsExt, path::Path, time::Duration};
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Intent {
    request: ExtractRequest,
    run: RunOwner,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Terminal {
    revision: Counter,
    completion_sha256: Digest,
}
type Record = Envelope<Intent, CaptureReceipt, Value, Terminal>;
fn update(
    journal: &Journal,
    record: &mut Record,
    change: impl FnOnce(&mut Evidence<Intent, CaptureReceipt, Value, Terminal>),
) {
    let mut next = record.evidence.clone();
    change(&mut next);
    *record = journal.compare_and_swap(record.generation, next).unwrap();
}
fn main() {
    let mut args = std::env::args_os().skip(1);
    match args.next().as_deref() {
        Some(arg) if arg == grv_adapter_salesforce::containment::HELPER_ARG => {
            grv_adapter_salesforce::containment::exec(args)
        }
        Some(arg) if arg == "--salesforce-fixture" => {
            let home = std::path::PathBuf::from(args.next().expect("fixture home"));
            let mut adapter = SalesforceAdapter::with_programs(
                home.clone(),
                home.join("sf").into_os_string(),
                home.join("curl").into_os_string(),
            );
            adapter.supervisor = Some(std::env::current_exe().unwrap().into_os_string());
            grv_adapter_sdk::run_fd3(adapter).unwrap();
        }
        _ => {
            salesforce_capture_publication_and_source_free_replay("auto");
            salesforce_capture_publication_and_source_free_replay("bulk");
            println!("Salesforce REST and Bulk capture/publication/source-free replay passed");
        }
    }
}
fn salesforce_capture_publication_and_source_free_replay(transport: &str) {
    let temp = tempfile::tempdir_in(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap(),
    )
    .unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let package = temp.path().join("adapters/salesforce");
    std::fs::create_dir_all(&package).unwrap();
    std::fs::set_permissions(
        package.parent().unwrap(),
        std::fs::Permissions::from_mode(0o700),
    )
    .unwrap();
    std::fs::set_permissions(&package, std::fs::Permissions::from_mode(0o700)).unwrap();
    let entry = package.join("entry");
    write_script(
        &entry,
        &format!(
            "exec {} --salesforce-fixture {}",
            shell_quote(std::env::current_exe().unwrap().to_str().unwrap()),
            shell_quote(package.to_str().unwrap())
        ),
    );
    write_script(&package.join("sf"), SF);
    write_script(
        &package.join("curl"),
        if transport == "bulk" { BULK } else { REST },
    );
    std::fs::write(package.join("adapter.toml"),format!("name = 'salesforce'\nversion = '0.1.0'\ninterface_versions = [1]\nbinding_schema_version = 1\nentrypoint = {:?}\n",entry)).unwrap();
    let installations = discovery::discover(&[SearchRoot {
        path: temp.path().join("adapters"),
        kind: RootKind::User,
    }])
    .unwrap();
    let mut session = Session::spawn(
        &installations[0],
        Deadlines {
            response: Duration::from_secs(30),
            ..Deadlines::default()
        },
    )
    .unwrap();
    let locator = session
        .locate_connection(
            json!({"org":"sample-org","api_version":"v66.0"}),
            Mode::Extract,
            None,
        )
        .unwrap();
    let grv = temp.path().join("grv");
    std::fs::create_dir(&grv).unwrap();
    let store = Store::initialize(LocalBackend::open(&grv).unwrap(), InitOptions::default())
        .unwrap()
        .0;
    let bound = session
        .bind_connection(
            locator,
            Some(grv.to_str().unwrap().into()),
            None,
            None,
            Mode::Extract,
        )
        .unwrap();
    let identity = session
        .authenticate(
            bound.handle.clone(),
            Some("salesforce:00D000000000001AAA".into()),
        )
        .unwrap();
    let plans: Vec<_> = ["rows", "empty"]
        .into_iter()
        .map(|name| {
            TablePlan::from_declaration(
                &json!({}),
                &if name == "rows" {
                    json!({"name":name,"partition_keys":["month"],"columns":[{"name":"id","type":"utf8"},{"name":"amount","type":"decimal128(38,6)"},{"name":"created","type":"timestamp(us,UTC)"},{"name":"_month_","type":"utf8","derive":{"from":"created","format":"month"}}]})
                } else {
                    json!({"name":name,"columns":[{"name":"id","type":"utf8"},{"name":"amount","type":"decimal128(38,6)"},{"name":"created","type":"timestamp(us,UTC)"}]})
                },
            )
            .unwrap()
        })
        .collect();
    let request = ExtractRequest {
        attempt_id: Uuid::v4(),
        stream_id: Uuid::v4(),
        root: grv.to_str().unwrap().into(),
        dataset: Name::new("data").unwrap(),
        run_id: RunId::new("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
        declaration_sha256: Digest::new("a".repeat(64)).unwrap(),
        adapter_identity: grv_types::AdapterIdentity {
            name: session.descriptor.name.clone(),
            package_version: session.descriptor.package_version.clone(),
            interface_version: session.descriptor.interface_version,
            binding_schema_version: session.descriptor.binding_schema_version,
        },
        connection_identity: identity,
        selection: ExtractSelection {
            policy: SelectionPolicy::All,
        },
        options: json!({"transport":transport,"all_rows":false}),
        tables: plans
            .iter()
            .map(|plan| ExtractTable {
                name: plan.table.clone(),
                source: json!({"object":if plan.table.as_str() == "rows" {"Case"} else {"Contact"}}),
                columns: json!([{"name":"id","type":"utf8","source":"id"},{"name":"amount","type":"decimal128(38,6)","source":"AMOUNT__c"},{"name":"created","type":"timestamp(us,UTC)","source":"systemmodstamp"}]),
                contract: plan.input_contract().unwrap(),
            })
            .collect(),
        resume: None,
    };
    let clock = SystemClock::default();
    let ownership = Ownership::new(&store, &clock, 60).unwrap();
    let mut run = ownership
        .prepare_run(
            request.dataset.clone(),
            request.run_id.clone(),
            0.into(),
            vec![],
            None,
        )
        .unwrap();
    let journal = Journal::create(
        temp.path()
            .join(format!("state/push/{}", request.attempt_id)),
        std::slice::from_ref(&grv),
    )
    .unwrap();
    let mut record:Record=journal.create_evidence(Evidence{intent:Intent{request:request.clone(),run:run.clone()},capture:None,
        progress:json!({"acquisition":AcquisitionProgress::prepared(&request),"reservations":[]}),terminal:None}).unwrap();
    ownership.commit_run(&run).unwrap();
    ownership.confirm_holds(&mut run).unwrap();
    let job = CaptureJob {
        ownership: &ownership,
        run: &run,
        journal: &journal,
        request: &request,
        plans: &plans,
        renewal_interval: Duration::from_secs(5),
    };
    let mut progress = AcquisitionProgress::prepared(&request);
    job.acquire(&mut session, bound.handle, &mut progress, |next| {
        update(&journal, &mut record, |e| {
            e.progress["acquisition"] = serde_json::to_value(next).unwrap()
        });
        let durable: Record = journal.read().unwrap();
        assert_eq!(durable.generation, record.generation);
        Ok(())
    })
    .unwrap();
    assert!(progress.stopped);
    assert_eq!(
        progress
            .tables
            .iter()
            .map(|t| t.row_count.get())
            .sum::<u64>(),
        2
    );
    assert_eq!(
        job.acquire(
            &mut session,
            Handle::new("salesforce-handle").unwrap(),
            &mut progress,
            |_| panic!("must not restart")
        )
        .unwrap_err()
        .code,
        ErrorCode::ExtractionIncomplete
    );
    let receipt = job
        .canonicalize(&progress, grv_types::SourceConsistency::CaptureWindow)
        .unwrap();
    let digest = receipt.digest().unwrap();
    update(&journal, &mut record, |e| e.capture = Some(receipt.clone()));
    assert_eq!(request.options["transport"], transport);
    assert_eq!(
        receipt.completion.job["transport"],
        if transport == "auto" { "rest" } else { "bulk" }
    );
    assert!(
        receipt
            .checkpoints
            .iter()
            .flat_map(|checkpoint| &checkpoint.tables)
            .all(|table| !table.reopenable)
    );
    // CaptureJob already obtained the process's clean close/stopped evidence.
    drop(session);
    // Destroy every source-side helper, alias context and acquisition journal
    // before reopening accepted capture and publishing from core evidence.
    std::fs::remove_dir_all(temp.path().join("adapters")).unwrap();
    let canonical_replay = job
        .canonicalize(&progress, grv_types::SourceConsistency::CaptureWindow)
        .unwrap();
    assert_eq!(canonical_replay.digest().unwrap(), digest);
    let accepted: Record = journal.read().unwrap();
    assert!(
        !serde_json::to_string(&accepted)
            .unwrap()
            .contains("credential-canary")
    );
    let replay_receipt = accepted.evidence.capture.unwrap();
    replay_receipt.verify(&journal, &request).unwrap();
    assert_eq!(replay_receipt.digest().unwrap(), digest);
    let receipt = replay_receipt;
    for table in &receipt.tables {
        for group in &table.groups {
            let intent = ownership
                .prepare_reservation(
                    &run,
                    table.plan.layout(),
                    group.partition.clone(),
                    &table.plan.contract,
                )
                .unwrap();
            let index = record.evidence.progress["reservations"]
                .as_array()
                .unwrap()
                .len();
            update(&journal, &mut record, |e| {
                e.progress["reservations"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"intent":intent,"progress":ReservationProgress::Prepared}))
            });
            let mut reservation_progress = ReservationProgress::Prepared;
            let mut reservation = ownership
                .reserve_authorized(&run, &intent, &mut reservation_progress, |next| {
                    update(&journal, &mut record, |e| {
                        e.progress["reservations"][index]["progress"] =
                            serde_json::to_value(next).unwrap()
                    });
                    Ok(())
                })
                .unwrap();
            let staged = stage_group(&journal, &table.plan, group).unwrap();
            if table.plan.table.as_str() == "rows" {
                use arrow_array::{Decimal128Array, StringArray, TimestampMicrosecondArray};
                let file = std::fs::File::open(&staged.files[0].path).unwrap();
                let mut reader =
                    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                        .unwrap()
                        .build()
                        .unwrap();
                let batch = reader.next().unwrap().unwrap();
                assert_eq!(batch.num_rows(), 2);
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let amounts = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap();
                let timestamps = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                assert_eq!([ids.value(0), ids.value(1)], ["first", "second"]);
                assert_eq!(
                    amounts.value(0),
                    12_345_678_901_234_567_890_123_456_789_012_345_678_i128
                );
                assert_eq!(amounts.value(1), 1);
                assert_eq!([timestamps.value(0), timestamps.value(1)], [0, 1]);
            } else {
                assert_eq!(staged.row_count, 0);
            }
            ownership
                .write_group(&run, &mut reservation, &table.plan.contract, &staged, None)
                .unwrap();
            update(&journal, &mut record, |e| {
                e.progress["reservations"][index]["written"] = json!(true)
            });
            ownership
                .release(&mut reservation, ClaimOutcome::Finalized)
                .unwrap();
            update(&journal, &mut record, |e| {
                e.progress["reservations"][index]["released"] = json!(true)
            });
        }
    }
    let sealed = ownership.seal(&mut run).unwrap();
    assert_eq!(sealed.entries.len(), 2);
    update(&journal, &mut record, |e| {
        e.progress["sealed_run"] = serde_json::to_value(&sealed).unwrap()
    });
    let publisher = Publisher::new(&store, &clock, 60).unwrap();
    let lease_intent = publisher
        .prepare_lease(request.dataset.clone(), "conformance".into())
        .unwrap();
    update(&journal, &mut record, |e| {
        e.progress["lease_intent"] = serde_json::to_value(&lease_intent).unwrap()
    });
    let mut lease = publisher
        .acquire_authorized(&lease_intent, &mut LeaseProgress::Prepared, |next| {
            update(&journal, &mut record, |e| {
                e.progress["lease_progress"] = serde_json::to_value(next).unwrap()
            });
            Ok(())
        })
        .unwrap();
    let candidate = publisher
        .prepare(
            &mut lease,
            ChangeSet {
                runs: vec![request.run_id.clone()],
                expected_revision: Some(0.into()),
                ..Default::default()
            },
            journal.directory(),
        )
        .unwrap();
    let intent = candidate.intent().clone();
    let known = publisher
        .commit(candidate, |intent| {
            update(&journal, &mut record, |e| {
                e.progress["publication"] = serde_json::to_value(intent).unwrap()
            });
            Ok(())
        })
        .unwrap();
    assert_eq!(known.revision.get(), 1);
    assert!(known.maintenance_error.is_none());
    let resolve = publisher
        .prepare_lease(request.dataset.clone(), "resolve".into())
        .unwrap();
    update(&journal, &mut record, |e| {
        e.progress["resolution_intent"] = serde_json::to_value(&resolve).unwrap()
    });
    assert_eq!(
        publisher
            .resolve(
                &intent,
                &resolve,
                &mut LeaseProgress::Prepared,
                |next| {
                    update(&journal, &mut record, |e| {
                        e.progress["resolution_progress"] = serde_json::to_value(next).unwrap()
                    });
                    Ok(())
                },
                journal.directory()
            )
            .unwrap(),
        Some(1.into())
    );
    update(&journal, &mut record, |e| {
        e.terminal = Some(Terminal {
            revision: known.revision,
            completion_sha256: digest.clone(),
        })
    });
    // A later REST page fails only AFTER the production adapter/SDK emits
    // and durably records bootstrap rows. That partial attempt must not omit
    // the populated snapshot accepted above, including its exact file bytes.
    if transport == "auto" {
        later_page_failure_preserves_snapshot(temp.path(), &grv, &store, &request, &plans);
    }
    drop(journal);
    drop(store);
    // Neither process executability nor source/GRV availability is needed to
    // replay an established terminal result, including after version pruning.
    std::fs::remove_dir_all(&grv).unwrap();
    let reopened = Journal::open(
        temp.path()
            .join(format!("state/push/{}", request.attempt_id)),
        &[grv],
    )
    .unwrap();
    let replay: Record = reopened.read().unwrap();
    let terminal = replay.evidence.terminal.unwrap();
    assert_eq!(terminal.revision.get(), 1);
    assert_eq!(terminal.completion_sha256, digest);
}

fn later_page_failure_preserves_snapshot(
    temp: &Path,
    grv: &Path,
    store: &Store<LocalBackend>,
    seed: &ExtractRequest,
    plans: &[TablePlan],
) {
    fn snapshot(root: &Path) -> std::collections::BTreeMap<String, Vec<u8>> {
        fn visit(
            root: &Path,
            directory: &Path,
            out: &mut std::collections::BTreeMap<String, Vec<u8>>,
        ) {
            for entry in std::fs::read_dir(directory).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    visit(root, &path, out);
                } else {
                    let relative = path.strip_prefix(root).unwrap().to_str().unwrap();
                    // Ignore the fresh failed run's lease/control; preserve all
                    // published revision, manifest and data-file bytes exactly.
                    if !relative.contains("/.runs/") && !relative.ends_with("/_control.json") {
                        out.insert(relative.into(), std::fs::read(path).unwrap());
                    }
                }
            }
        }
        let mut out = std::collections::BTreeMap::new();
        visit(root, root, &mut out);
        out
    }
    let before = snapshot(grv);
    let package = temp.join("adapters/salesforce");
    std::fs::create_dir_all(&package).unwrap();
    for directory in [temp.join("adapters"), package.clone()] {
        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let entry = package.join("entry");
    write_script(
        &entry,
        &format!(
            "exec {} --salesforce-fixture {}",
            shell_quote(std::env::current_exe().unwrap().to_str().unwrap()),
            shell_quote(package.to_str().unwrap())
        ),
    );
    write_script(&package.join("sf"), SF);
    let counter = temp.join("failed-page-calls");
    let bootstrap = temp.join("bootstrap-calls");
    let curl = format!(
        "input=; while IFS= read -r line; do input=\"$input$line\"; done\ncase \"$input\" in *case-locator*) printf x >> {}; printf 'HTTP/1.1 503 Unavailable\\r\\n\\r\\n%s' '{{\"error\":\"later page failure\"}}'; exit 0;; *FROM%20Case*) printf x >> {};; esac\n{}",
        shell_quote(counter.to_str().unwrap()),
        shell_quote(bootstrap.to_str().unwrap()),
        REST.split_once("printf 'HTTP/1.1 200 OK")
            .map(|(_, rest)| format!("printf 'HTTP/1.1 200 OK{rest}"))
            .unwrap()
    );
    write_script(&package.join("curl"), &curl);
    std::fs::write(package.join("adapter.toml"), format!("name='salesforce'\nversion='0.1.0'\ninterface_versions=[1]\nbinding_schema_version=1\nentrypoint={entry:?}\n")).unwrap();
    let installations = discovery::discover(&[SearchRoot {
        path: temp.join("adapters"),
        kind: RootKind::User,
    }])
    .unwrap();
    let start = || {
        let mut session = Session::spawn(
            &installations[0],
            Deadlines {
                response: Duration::from_secs(30),
                ..Deadlines::default()
            },
        )
        .unwrap();
        let locator = session
            .locate_connection(
                json!({"org":"synthetic-fixture","api_version":"v66.0"}),
                Mode::Extract,
                None,
            )
            .unwrap();
        let bound = session
            .bind_connection(
                locator,
                Some(grv.to_str().unwrap().into()),
                None,
                None,
                Mode::Extract,
            )
            .unwrap();
        session
            .authenticate(bound.handle.clone(), Some(seed.connection_identity.clone()))
            .unwrap();
        (session, bound.handle)
    };
    let mut request = seed.clone();
    request.attempt_id = Uuid::v4();
    request.stream_id = Uuid::v4();
    request.run_id = RunId::new("01ARZ3NDEKTSV4RRFFQ69G5FAW").unwrap();
    let clock = SystemClock::default();
    let ownership = Ownership::new(store, &clock, 60).unwrap();
    let mut run = ownership
        .prepare_run(
            request.dataset.clone(),
            request.run_id.clone(),
            1.into(),
            vec![],
            None,
        )
        .unwrap();
    ownership.commit_run(&run).unwrap();
    ownership.confirm_holds(&mut run).unwrap();
    let journal = Journal::create(
        temp.join(format!("failed-state/push/{}", request.attempt_id)),
        &[grv.into()],
    )
    .unwrap();
    let mut failed: Record = journal
        .create_evidence(Evidence {
            intent: Intent {
                request: request.clone(),
                run: run.clone(),
            },
            capture: None,
            progress: json!({"acquisition":AcquisitionProgress::prepared(&request)}),
            terminal: None,
        })
        .unwrap();
    let job = CaptureJob {
        ownership: &ownership,
        run: &run,
        journal: &journal,
        request: &request,
        plans,
        renewal_interval: Duration::from_secs(5),
    };
    let mut progress = AcquisitionProgress::prepared(&request);
    let (mut session, handle) = start();
    let error = job
        .acquire(&mut session, handle, &mut progress, |next| {
            update(&journal, &mut failed, |e| {
                e.progress["acquisition"] = serde_json::to_value(next).unwrap()
            });
            Ok(())
        })
        .unwrap_err();
    assert!(!error.message.contains("credential-canary"));
    assert!(progress.started && !progress.stopped);
    assert!(
        progress
            .tables
            .iter()
            .any(|table| !table.batches.is_empty()),
        "later-page failure must follow durable raw rows"
    );
    assert_eq!(std::fs::read(&bootstrap).unwrap(), b"x");
    assert_eq!(std::fs::read(&counter).unwrap(), b"x");
    assert_eq!(
        job.canonicalize(&progress, grv_types::SourceConsistency::CaptureWindow)
            .unwrap_err()
            .code,
        ErrorCode::ExtractionIncomplete
    );
    let durable: Record = journal.read().unwrap();
    assert!(durable.evidence.capture.is_none() && durable.evidence.terminal.is_none());
    let durable_progress: AcquisitionProgress =
        serde_json::from_value(durable.evidence.progress["acquisition"].clone()).unwrap();
    assert!(durable_progress.started && !durable_progress.stopped);
    assert_eq!(
        snapshot(grv),
        before,
        "partial acquisition changed published membership/file bytes"
    );
    drop(session);
    let (mut reopened, handle) = start();
    assert_eq!(
        job.acquire(&mut reopened, handle, &mut progress, |_| panic!(
            "must refuse before source calls"
        ))
        .unwrap_err()
        .code,
        ErrorCode::ExtractionIncomplete
    );
    assert_eq!(std::fs::read(&bootstrap).unwrap(), b"x");
    assert_eq!(std::fs::read(&counter).unwrap(), b"x");
    assert_eq!(snapshot(grv), before);
    drop(reopened);
    std::fs::remove_dir_all(temp.join("adapters")).unwrap();
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}
fn write_script(path: &Path, body: &str) {
    std::fs::write(path, format!("#!/bin/sh\n{body}\n")).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
}
const SF: &str = r#"[ "$SF_DISABLE_LOG_FILE" = true ] || exit 1
[ "$SFDX_DISABLE_LOG_FILE" = true ] || exit 1
[ "$SF_TEMP_SHOW_SECRETS" = true ] || exit 1
printf '%s' '{"status":0,"result":{"id":"00D000000000001AAA","instanceUrl":"https://test.my.salesforce.com","accessToken":"credential-canary"}}'"#;
const BULK: &str = r#"input=; while IFS= read -r line; do input="$input$line"; done
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
const REST: &str = r#"input=; while IFS= read -r line; do input="$input$line"; done
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
