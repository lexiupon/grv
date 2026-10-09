use grv_adapter_api::*;
use grv_adapter_host::{
    discovery::{self, RootKind, SearchRoot},
    process::{Deadlines, Session},
};
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
fn verify_exact_typed_capture(receipt: &CaptureReceipt, journal: &Journal) {
    use arrow_array::*;
    let typed = receipt
        .tables
        .iter()
        .find(|table| table.plan.table.as_str() == "typed")
        .unwrap();
    assert_eq!(typed.row_count.get(), 1);
    let file = &typed.groups[0].files[0];
    let mut reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(journal.directory().join(file.key.as_str())).unwrap(),
    )
    .unwrap()
    .build()
    .unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    macro_rules! value {
        ($name:literal,$array:ty) => {
            batch
                .column_by_name($name)
                .unwrap()
                .as_any()
                .downcast_ref::<$array>()
                .unwrap()
                .value(0)
        };
    }
    assert!(value!("b", BooleanArray));
    assert_eq!(value!("i", Int64Array), -127);
    assert_eq!(value!("f", Float64Array).to_bits(), (-0.0f64).to_bits());
    assert_eq!(value!("d", Float64Array), 1.25);
    assert_eq!(value!("s", StringArray), "🍕");
    assert_eq!(value!("blob", BinaryArray), &[0, 255]);
    assert_eq!(value!("date", Date32Array), 1);
    assert_eq!(
        value!("decimal", Decimal128Array),
        "123456789012345678901234567812".parse::<i128>().unwrap()
    );
    assert_eq!(value!("sec", TimestampMillisecondArray), 1000);
    assert_eq!(value!("ms", TimestampMillisecondArray), 1);
    assert_eq!(value!("us", TimestampMicrosecondArray), 1);
    assert_eq!(value!("ns", TimestampNanosecondArray), 1);
    assert_eq!(value!("utc", TimestampMicrosecondArray), 0);
    assert!(reader.next().is_none());
}
#[test]
fn native_duckdb_shared_snapshot_exact_capture_publication_and_source_free_replay() {
    let temp = tempfile::tempdir_in(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap(),
    )
    .unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let database = temp.path().join("source.duckdb");
    let output = std::process::Command::new(
        Path::new(env!("GRV_DUCKDB_NATIVE_LIB_DIR")).join("grv_native_fixture"),
    )
    .arg(&database)
    .output()
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let package = temp.path().join("adapters/duckdb");
    std::fs::create_dir_all(&package).unwrap();
    std::fs::write(package.join("adapter.toml"),format!("name = 'duckdb'\nversion = {:?}\ninterface_versions = [1]\nbinding_schema_version = 1\nentrypoint = {:?}\n",env!("CARGO_PKG_VERSION"),env!("CARGO_BIN_EXE_fixture-duckdb"))).unwrap();
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
        .locate_connection(json!({"database":database}), Mode::Extract, None)
        .unwrap();
    let grv = temp.path().join("grv");
    std::fs::create_dir(&grv).unwrap();
    let store = Store::initialize(LocalBackend::open(&grv).unwrap(), InitOptions::default())
        .unwrap()
        .0;
    let bound = session
        .bind_connection(
            locator.clone(),
            Some(grv.to_str().unwrap().into()),
            locator.identity.clone(),
            None,
            Mode::Extract,
        )
        .unwrap();
    let identity = session
        .authenticate(bound.handle.clone(), bound.identity)
        .unwrap();
    let tables = vec![
        json!({"name":"rows","source":{"table":"main.first","filter":"selected AND regexp_matches(upper(label), '^ROW-')"},"columns":[{"name":"id","source":"id","type":"int64"},{"name":"text","source":"label","type":"utf8"},{"name":"mapped_rowid","source":"rowid","type":"int64"}]}),
        json!({"name":"empty","source":{"table":"main.empty"},"columns":[{"name":"id","source":"id","type":"int64"},{"name":"text","source":"label","type":"utf8"}]}),
        json!({"name":"typed","source":{"table":"main.typed"},"columns":[
            {"name":"b","source":"b","type":"bool"},{"name":"i","source":"i","type":"int64"},
            {"name":"f","source":"f","type":"double"},{"name":"d","source":"d","type":"double"},
            {"name":"s","source":"s","type":"utf8"},{"name":"blob","source":"blob","type":"binary"},
            {"name":"date","source":"date","type":"date32"},{"name":"decimal","source":"decimal","type":"decimal128(30,2)"},
            {"name":"sec","source":"sec","type":"timestamp(ms)"},
            {"name":"ms","source":"ms","type":"timestamp(ms)"},
            {"name":"us","source":"us","type":"timestamp(us)"},
            {"name":"ns","source":"ns","type":"timestamp(ns)"},
            {"name":"utc","source":"utc","type":"timestamp(us,UTC)"}]}),
    ];
    let plans: Vec<_> = tables
        .iter()
        .map(|table| TablePlan::from_declaration(&json!({}), table).unwrap())
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
        options: json!({}),
        tables: plans
            .iter()
            .zip(&tables)
            .map(|(plan, table)| ExtractTable {
                name: plan.table.clone(),
                source: table["source"].clone(),
                columns: table["columns"].clone(),
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
        if next
            .tables
            .iter()
            .any(|table| !table.batches.is_empty() || table.completion.is_some())
        {
            assert_eq!(
                next.checkpoints.len(),
                1,
                "rows/completion require durably acknowledged checkpoint"
            );
            assert_eq!(next.checkpoints[0].tables.len(), 3);
        }
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
        2501
    );
    assert_eq!(
        job.acquire(
            &mut session,
            Handle::new("source-must-not-be-reopened").unwrap(),
            &mut progress,
            |_| panic!("must not restart")
        )
        .unwrap_err()
        .code,
        ErrorCode::ExtractionIncomplete
    );
    let receipt = job
        .canonicalize(&progress, grv_types::SourceConsistency::TransactionSnapshot)
        .unwrap();
    let digest = receipt.digest().unwrap();
    update(&journal, &mut record, |e| e.capture = Some(receipt.clone()));
    assert_eq!(
        receipt.source_consistency,
        grv_types::SourceConsistency::TransactionSnapshot
    );
    assert_eq!(receipt.checkpoints.len(), 1);
    let checkpoint = &receipt.checkpoints[0];
    assert_eq!(checkpoint.tables.len(), 3);
    assert!(checkpoint.tables.iter().all(|table| !table.reopenable && table.snapshot_id == checkpoint.tables[0].snapshot_id));
    let empty = receipt
        .tables
        .iter()
        .find(|table| table.plan.table.as_str() == "empty")
        .unwrap();
    assert_eq!(empty.row_count.get(), 0);
    assert_eq!(empty.completion.row_count.get(), 0);
    verify_exact_typed_capture(&receipt, &journal);
    drop(session);
    // Complete raw capture and accepted canonical capture are sufficient even
    // after deleting the adapter installation, source and acquisition journal.
    std::fs::remove_file(&database).unwrap();
    std::fs::remove_dir_all(format!("{}.grv-acquisitions", database.display())).unwrap();
    std::fs::remove_dir_all(temp.path().join("adapters")).unwrap();
    let replayed = job
        .canonicalize(&progress, grv_types::SourceConsistency::TransactionSnapshot)
        .unwrap();
    assert_eq!(replayed.digest().unwrap(), digest);
    let accepted: Record = journal.read().unwrap();
    let receipt = accepted.evidence.capture.unwrap();
    receipt.verify(&journal, &request).unwrap();
    assert_eq!(receipt.digest().unwrap(), digest);
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
    assert_eq!(sealed.entries.len(), 3);
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
