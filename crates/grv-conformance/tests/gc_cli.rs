use arrow_array::{Int64Array, RecordBatch};
use grv_adapter_api::{Column, TableContract};
use grv_adapter_host::validate_output;
use grv_core::{
    canonical::Sorter,
    clock::{Clock, SystemClock, new_run_id},
    ownership::{Ownership, ReservationProgress},
    publication::{LeaseProgress, Publisher},
    revision,
    store::{InitOptions, Store},
};
use grv_storage::{LocalBackend, model::*};
use grv_types::{Name, Uuid};
use serde_json::{Value, json};
use std::{fs, os::unix::fs::PermissionsExt, path::Path, process::Command, sync::Arc};
fn name(s: &str) -> Name {
    Name::new(s).unwrap()
}
fn cli(args: &[&str]) -> Value {
    let output = Command::new(env!("CARGO_BIN_EXE_grv-conformance-cli"))
        .arg("--json")
        .args(args)
        .output()
        .unwrap();
    let value: Value = serde_json::from_slice(&output.stdout).unwrap_or_else(|_| {
        panic!(
            "invalid CLI JSON: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    });
    validate_output(&value).unwrap();
    assert_eq!(output.status.success(), value["ok"] == true);
    value
}
fn seed(store: &Store<LocalBackend>, scratch: &Path, clock: &SystemClock, value: i64) -> Counter {
    let ownership = Ownership::new(store, clock, 60).unwrap();
    let base = revision::read_latest(store, &name("data"))
        .unwrap()
        .map_or(Counter::from(0), |r| r.0.revision);
    let mut owner = ownership
        .prepare_run(
            name("data"),
            new_run_id(&clock.now()).unwrap(),
            base,
            vec![],
            None,
        )
        .unwrap();
    ownership.commit_run(&owner).unwrap();
    ownership.confirm_holds(&mut owner).unwrap();
    let contract = TableContract {
        columns: vec![Column {
            name: "id".into(),
            logical_type: json!("int64"),
        }],
        partition_keys: vec![],
        extensions: json!({}),
        column_ext: json!({}),
    };
    let mut sorter = Sorter::new(contract.clone(), scratch).unwrap();
    sorter
        .append(
            &RecordBatch::try_new(
                grv_core::contract::arrow_schema(&contract).unwrap(),
                vec![Arc::new(Int64Array::from(vec![value]))],
            )
            .unwrap(),
        )
        .unwrap();
    let staged = sorter.finish().unwrap();
    let intent = ownership
        .prepare_reservation(
            &owner,
            TableLayout {
                table: name("rows"),
                partition_keys: vec![],
                extensions: None,
            },
            Partition::new(),
            &contract,
        )
        .unwrap();
    let mut reservation = ownership
        .reserve_authorized(&owner, &intent, &mut ReservationProgress::Prepared, |_| {
            Ok(())
        })
        .unwrap();
    ownership
        .write_group(&owner, &mut reservation, &contract, &staged, None)
        .unwrap();
    ownership
        .release(&mut reservation, ClaimOutcome::Finalized)
        .unwrap();
    ownership.seal(&mut owner).unwrap();
    let publisher = Publisher::new(store, clock, 60).unwrap();
    let lease = publisher
        .prepare_publication_lease(name("data"), "gc-fixture".into())
        .unwrap();
    let mut lease = publisher
        .acquire_authorized(&lease, &mut LeaseProgress::Prepared, |_| Ok(()))
        .unwrap();
    let candidate = publisher
        .prepare(
            &mut lease,
            ChangeSet {
                runs: vec![owner.control().run_id.clone()],
                ..Default::default()
            },
            scratch,
        )
        .unwrap();
    publisher.commit(candidate, |_| Ok(())).unwrap().revision
}
#[test]
fn gc_cli_preview_pin_release_prune_cleanup_and_repeat_keep_history_and_current_version() {
    let temp = tempfile::tempdir_in(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap(),
    )
    .unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let root = temp.path().join("grv");
    fs::create_dir(&root).unwrap();
    let store = Store::initialize(
        LocalBackend::open(&root).unwrap(),
        InitOptions {
            max_clock_skew: Some(0),
            pending_grace: Some(1),
            ..Default::default()
        },
    )
    .unwrap()
    .0;
    let clock = SystemClock::default();
    assert_eq!(seed(&store, temp.path(), &clock, 1).get(), 1);
    assert_eq!(seed(&store, temp.path(), &clock, 2).get(), 2);
    let state = temp.path().join("state");
    let pin = Uuid::v4();
    let root_text = root.to_str().unwrap();
    let state_text = state.to_str().unwrap();
    let pinned = cli(&[
        "pin",
        "data",
        "--grv",
        root_text,
        "--revision",
        "1",
        "--reason",
        "GC race fixture",
        "--pin",
        pin.as_str(),
        "--state",
        state_text,
    ]);
    assert_eq!(pinned["ok"], true, "{pinned}");
    let protected = cli(&["gc", "data", "--grv", root_text, "--dry-run"]);
    assert_eq!(protected["ok"], true, "{protected}");
    assert!(
        protected["result"]["versions"]
            .as_array()
            .unwrap()
            .iter()
            .all(|v| v["state"] == "protected")
    );
    let released = cli(&[
        "unpin",
        "data",
        "--grv",
        root_text,
        "--revision",
        "1",
        "--pin",
        pin.as_str(),
        "--state",
        state_text,
    ]);
    assert_eq!(released["ok"], true, "{released}");
    std::thread::sleep(std::time::Duration::from_secs(2));
    let preview = cli(&["gc", "data", "--grv", root_text]);
    assert_eq!(preview["ok"], true, "{preview}");
    assert!(
        preview["result"]["versions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["version"] == "1" && v["state"] == "eligible")
    );
    let applied = cli(&[
        "gc", "data", "--grv", root_text, "--apply", "--state", state_text,
    ]);
    assert_eq!(applied["ok"], true, "{applied}");
    assert!(
        applied["result"]["versions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["version"] == "1"
                && v["state"] == "deleted"
                && v["bytes"].as_str().unwrap().parse::<u64>().unwrap() > 0)
    );
    assert_eq!(
        applied["result"]["completed_operation_ids"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let old = root.join("datasets/data/rows/version=1");
    assert!(old.join(".pruned").is_file());
    assert!(!old.join("data.parquet").exists());
    assert!(!old.join("manifest.json").exists());
    assert!(
        root.join("datasets/data/rows/version=2/data.parquet")
            .is_file()
    );
    assert!(
        root.join(revision::revision_key(&name("data"), Counter::from(1)).as_str())
            .is_file()
    );
    assert!(
        root.join(revision::revision_key(&name("data"), Counter::from(2)).as_str())
            .is_file()
    );
    assert!(root.join("datasets/data/rows/.layout.json").is_file());
    assert_eq!(
        revision::read_latest(&store, &name("data"))
            .unwrap()
            .unwrap()
            .0
            .revision
            .get(),
        2
    );
    assert!(revision::committed(&store, &name("data"), Counter::from(1), temp.path()).unwrap());
    let repeat = cli(&[
        "gc", "data", "--grv", root_text, "--apply", "--state", state_text,
    ]);
    assert_eq!(repeat["ok"], true, "{repeat}");
    assert_eq!(repeat["result"]["completed_operation_ids"], json!([]));
    assert!(
        root.join("datasets/data/rows/version=2/data.parquet")
            .is_file()
    );
}
