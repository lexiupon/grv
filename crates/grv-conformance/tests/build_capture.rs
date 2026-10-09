#![cfg(feature = "native-duckdb")]
use grv_adapter_api::*;
use grv_adapter_host::{
    discovery::{self, RootKind, SearchRoot},
    process::{Deadlines, Session},
};
use grv_core::{
    build::{self, PreparationContext},
    build_export::{self, ExportJob, ExportProgress, ExportReceipt},
    build_publication::{self, Finalizer, Policy, Progress, Selection},
    clock::{Clock, SystemClock, new_run_id},
    journal::{Envelope, Evidence, Journal},
    normalize::TablePlan,
    ownership::Ownership,
    store::{InitOptions, Store},
};
use grv_storage::{LocalBackend, model::RunPhase};
use serde_json::{Value, json};
use std::{fs, os::unix::fs::PermissionsExt, path::Path, time::Duration};
type Record = Envelope<Value, ExportReceipt, Value, Value>;
fn save(
    journal: &Journal,
    record: &mut Record,
    change: impl FnOnce(&mut Evidence<Value, ExportReceipt, Value, Value>),
) {
    let mut evidence = record.evidence.clone();
    change(&mut evidence);
    *record = journal
        .compare_and_swap(record.generation, evidence)
        .unwrap();
}
fn spawn(installation: &grv_adapter_host::discovery::Installation) -> Session {
    Session::spawn(
        installation,
        Deadlines {
            response: Duration::from_secs(30),
            ..Default::default()
        },
    )
    .unwrap()
}
#[test]
fn native_process_core_build_preparation_acceptance_credited_empty_export_publication_and_source_free_replay()
 {
    let temp = tempfile::tempdir_in(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap(),
    )
    .unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let package = temp.path().join("adapters/duckdb");
    fs::create_dir_all(&package).unwrap();
    fs::write(package.join("adapter.toml"),format!("name='duckdb'\nversion={:?}\ninterface_versions=[1]\nbinding_schema_version=1\nentrypoint={:?}\n",env!("CARGO_PKG_VERSION"),env!("CARGO_BIN_EXE_fixture-duckdb-build"))).unwrap();
    let installs = discovery::discover(&[SearchRoot {
        path: temp.path().join("adapters"),
        kind: RootKind::User,
    }])
    .unwrap();
    let root = temp.path().join("grv");
    fs::create_dir(&root).unwrap();
    let store = Store::initialize(LocalBackend::open(&root).unwrap(), InitOptions::default())
        .unwrap()
        .0;
    let clock = SystemClock::default();
    let engine = temp.path().join("engine.duckdb");
    let workspace = Uuid::v4();
    let run_id = new_run_id(&clock.now()).unwrap();
    let attempt = Uuid::v4();
    let journal = Journal::create(temp.path().join("state"), std::slice::from_ref(&root)).unwrap();
    let mut record: Record = journal
        .create_evidence(Evidence {
            intent: json!({"workspace":workspace,"run_id":run_id,"attempt_id":attempt}),
            capture: None,
            progress: json!({}),
            terminal: None,
        })
        .unwrap();
    let mut process = spawn(&installs[0]);
    let locator = process
        .locate_connection(json!({"database":engine}), Mode::ManagedBuild, None)
        .unwrap();
    let expected = locator.identity.clone();
    let bound = process
        .bind_connection(
            locator,
            Some(root.to_str().unwrap().into()),
            expected.clone(),
            Some(workspace.clone()),
            Mode::ManagedBuild,
        )
        .unwrap();
    let connection_identity = process
        .authenticate(bound.handle.clone(), expected.or(bound.identity))
        .unwrap();
    let mut plans = vec![];
    let mut outputs = vec![];
    for (name, sql) in [
        (
            "rows",
            "SELECT 3::BIGINT AS id UNION ALL SELECT 1::BIGINT AS id UNION ALL SELECT 1::BIGINT AS id",
        ),
        ("empty", "SELECT 0::BIGINT AS id WHERE false"),
    ] {
        let columns = json!([{"name":"id","type":"int64"}]);
        let plan = TablePlan::from_declaration(&json!({}), &json!({"name":name,"columns":columns}))
            .unwrap();
        outputs.push(BuildOutput {
            table: plan.table.clone(),
            source: json!({"sql":sql}),
            columns,
            contract: plan.input_contract().unwrap(),
        });
        plans.push(plan);
    }
    let request = DiscoverBuildRequest {
        identity: BuildIdentity {
            attempt_id: attempt,
            root: root.to_str().unwrap().into(),
            dataset: Name::new("derived").unwrap(),
            run_id: run_id.clone(),
            workspace_id: workspace,
            declaration_sha256: grv_types::sha256(b"fixed-build"),
            adapter_identity: AdapterIdentity {
                name: process.descriptor.name.clone(),
                package_version: process.descriptor.package_version.clone(),
                interface_version: process.descriptor.interface_version,
                binding_schema_version: process.descriptor.binding_schema_version,
            },
            connection_identity,
        },
        execution: BuildExecution::Managed,
        options: json!({}),
        inputs: vec![],
        outputs,
        selected_outputs: plans.iter().map(|p| p.table.clone()).collect(),
        self_input: false,
    };
    save(&journal, &mut record, |e| {
        e.progress = json!({"fixed_request":request})
    });
    let discovery = process
        .discover_build(bound.handle.clone(), request.clone())
        .unwrap();
    let intent = build::prepare_intent(
        &store,
        &clock,
        60,
        request.clone(),
        discovery,
        journal.directory(),
    )
    .unwrap();
    assert!(!root.join("datasets/derived").exists());
    let mut prepared = build::prepare_session(
        process,
        bound.handle.clone(),
        PreparationContext {
            store: &store,
            clock: &clock,
            ttl: 60,
            scratch: journal.directory(),
        },
        intent,
        |intent, progress| {
            save(&journal, &mut record, |e| {
                e.progress = json!({"intent":intent,"preparation":progress})
            });
            Ok(())
        },
    )
    .unwrap();
    assert!(prepared.owner.control().holds_confirmed);
    assert_eq!(prepared.owner.control().phase, RunPhase::Open);
    let session = prepared.session.clone();
    let queries = session
        .outputs
        .iter()
        .map(|o| BuildQuery {
            table: o.table.clone(),
            sql: o.source["sql"].as_str().unwrap().into(),
        })
        .collect();
    let executed = prepared
        .process_mut()
        .execute_build(
            bound.handle.clone(),
            ExecuteBuildRequest {
                session: session.clone(),
                queries,
            },
        )
        .unwrap();
    assert_eq!(executed.status, BuildExecutionStatus::Succeeded);
    assert_eq!(executed.row_counts[0].rows.get(), 3);
    assert_eq!(executed.row_counts[1].rows.get(), 0);
    let completion = executed.completion.unwrap();
    assert!(completion.writers_stopped);
    let accepted = build_export::accept(
        prepared.process_mut(),
        bound.handle.clone(),
        session,
        completion,
        |progress| {
            save(&journal, &mut record, |e| {
                e.progress = json!({"completion":progress})
            });
            Ok(())
        },
    )
    .unwrap();
    let ownership = Ownership::new(&store, &clock, 60).unwrap();
    let owner = prepared.owner.clone();
    let job = ExportJob {
        ownership: &ownership,
        run: &owner,
        journal: &journal,
        accepted: &accepted,
        plans: &plans,
    };
    let mut progress = ExportProgress::prepared(&accepted, Uuid::v4());
    job.acquire(
        prepared.process_mut(),
        bound.handle.clone(),
        &mut progress,
        |progress| {
            save(&journal, &mut record, |e| {
                e.progress = json!({"export":progress})
            });
            Ok(())
        },
    )
    .unwrap();
    assert!(progress.stopped);
    assert_eq!(progress.tables[1].completion.unwrap().get(), 0);
    let receipt = job.canonicalize(&progress).unwrap();
    save(&journal, &mut record, |e| e.capture = Some(receipt.clone()));
    assert_eq!(receipt.tables[0].row_count.get(), 3);
    assert_eq!(receipt.tables[1].groups.len(), 1);
    let plan = build_publication::prepare_plan(
        &store,
        &clock,
        60,
        &owner,
        &receipt,
        &plans,
        &Selection {
            policy: Policy::All,
            ..Default::default()
        },
        &journal,
        journal.directory(),
    )
    .unwrap();
    assert_eq!(plan.groups[0].plan.table.as_str(), "empty");
    let mut publication = Progress::planned(owner, plan).unwrap();
    save(&journal, &mut record, |e| {
        e.progress = json!({"publication":publication})
    });
    let finalizer = Finalizer::new(&store, &clock, 60).unwrap();
    let published = finalizer
        .finalize(
            &mut publication,
            &journal,
            journal.directory(),
            |progress| {
                save(&journal, &mut record, |e| {
                    e.progress = json!({"publication":progress})
                });
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(published.revision.get(), 1);
    save(&journal, &mut record, |e| {
        e.terminal = Some(serde_json::to_value(&published).unwrap())
    });
    drop(prepared);
    let mut reopened = spawn(&installs[0]);
    let locator = reopened
        .locate_connection(json!({"database":engine}), Mode::ManagedBuild, None)
        .unwrap();
    let expected = locator.identity.clone();
    let bound = reopened
        .bind_connection(
            locator,
            Some(root.to_str().unwrap().into()),
            expected.clone(),
            Some(accepted.session.identity.workspace_id.clone()),
            Mode::ManagedBuild,
        )
        .unwrap();
    reopened
        .authenticate(bound.handle.clone(), expected.or(bound.identity))
        .unwrap();
    let reopened_record = reopened
        .open_build(bound.handle.clone(), accepted.session.identity.clone())
        .unwrap();
    assert_eq!(
        reopened_record.completion_sha256,
        Some(accepted.completion_sha256.clone())
    );
    reopened
        .record_build_outcome(
            bound.handle.clone(),
            accepted.session.session_id.clone(),
            BuildOutcome {
                kind: OutcomeKind::Published,
                revision: Some(U64::new(1).unwrap()),
                operation_id: Some(run_id),
            },
        )
        .unwrap();
    reopened
        .cleanup_build(bound.handle, accepted.session.session_id.clone())
        .unwrap();
    reopened.close().unwrap();
    fs::remove_dir_all(&package).unwrap();
    fs::remove_file(&engine).unwrap();
    receipt.verify(&journal, &accepted).unwrap();
    fs::remove_dir_all(&root).unwrap();
    assert_eq!(
        finalizer
            .finalize(&mut publication, &journal, journal.directory(), |_| panic!(
                "terminal replay must be read-only"
            ))
            .unwrap()
            .revision
            .get(),
        1
    );
}
