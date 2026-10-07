//! Real CLI recovery gates: journals stay outside GRV, dry-run has no effects,
//! and default recovery conservatively waits without trusted writer evidence.
use grv_adapter_host::validate_output;
use grv_core::{
    clock::Clock,
    ownership::{Ownership, ReservationProgress},
    revision,
    store::{InitOptions, Store},
};
use grv_storage::{Backend, LocalBackend, ObjectKey, model::*};
use grv_types::{Name, RunId, Timestamp};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};
struct OldClock;
impl Clock for OldClock {
    fn now(&self) -> Timestamp {
        Timestamp::new("2020-01-01T00:00:00Z").unwrap()
    }
    fn elapsed(&self) -> Duration {
        Duration::ZERO
    }
}
struct Fixture {
    root: PathBuf,
    state: PathBuf,
    _temp: tempfile::TempDir,
}
impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir_in(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
                .parent()
                .unwrap(),
        )
        .unwrap();
        std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let root = temp.path().join("grv");
        let state = temp.path().join("state");
        Store::initialize(LocalBackend::create(&root).unwrap(), InitOptions::default()).unwrap();
        Self {
            root,
            state,
            _temp: temp,
        }
    }
    fn store(&self) -> Store<LocalBackend> {
        Store::open(LocalBackend::open(&self.root).unwrap()).unwrap()
    }
    fn run(&self) -> RunId {
        let store = self.store();
        let own = Ownership::new(&store, &OldClock, 60).unwrap();
        let id = grv_core::clock::new_run_id(&OldClock.now()).unwrap();
        let mut owner = own
            .prepare_run(name("data"), id.clone(), 0.into(), vec![], None)
            .unwrap();
        own.commit_run(&owner).unwrap();
        own.confirm_holds(&mut owner).unwrap();
        id
    }
    fn control(&self, id: &RunId) -> RunControl {
        decode_record(
            &std::fs::read(
                self.root
                    .join(format!("datasets/data/.runs/{id}.control.json")),
            )
            .unwrap(),
        )
        .unwrap()
    }
    fn cli(&self, extra: &[&str]) -> Value {
        let output = Command::new(env!("CARGO_BIN_EXE_grv-conformance-cli"))
            .args([
                "--json",
                "recover",
                "data",
                "--grv",
                self.root.to_str().unwrap(),
                "--state",
                self.state.to_str().unwrap(),
            ])
            .args(extra)
            .env("GRV_ADAPTERS_DIR", self._temp.path().join("no-adapters"))
            .output()
            .unwrap();
        let value: Value = serde_json::from_slice(&output.stdout)
            .unwrap_or_else(|error| panic!("{error}: {}", String::from_utf8_lossy(&output.stderr)));
        validate_output(&value).unwrap_or_else(|error| panic!("{error}: {value}"));
        assert_eq!(output.status.success(), value["ok"] == true);
        value
    }
}
fn name(value: &str) -> Name {
    Name::new(value).unwrap()
}
fn object(suffix: &str) -> ObjectKey {
    ObjectKey::new(format!("datasets/data/{suffix}")).unwrap()
}
fn snapshot(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &Path, path: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for item in std::fs::read_dir(path).unwrap() {
            let path = item.unwrap().path();
            if path.is_dir() {
                visit(root, &path, files);
            } else {
                files.insert(
                    path.strip_prefix(root).unwrap().to_owned(),
                    std::fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut result = BTreeMap::new();
    visit(root, root, &mut result);
    result
}
#[test]
fn recovery_dry_run_reports_unpublished_expired_run_without_adapter_journal_or_mutation() {
    let f = Fixture::new();
    let id = f.run();
    let before = snapshot(&f.root);
    let result = f.cli(&["--dry-run"]);
    assert_eq!(result["ok"], true, "{result}");
    assert_eq!(result["result"]["runs"][0]["state"], "eligible");
    assert!(result["result"]["pending"].is_null());
    assert!(!f.state.exists());
    assert_eq!(snapshot(&f.root), before);
    assert!(
        !result.to_string().contains(
            serde_json::to_value(f.control(&id).owner_token)
                .unwrap()
                .as_str()
                .unwrap()
        )
    );
}
#[test]
fn recovery_seals_unpublished_runs_materializes_missing_payload_and_repeats_without_rewrite() {
    let f = Fixture::new();
    let expired = f.run();
    let missing = f.run();
    let store = f.store();
    let clock = grv_core::clock::SystemClock::default();
    let own = Ownership::new(&store, &clock, 60).unwrap();
    let mut sealed = own.recover_run(name("data"), missing.clone()).unwrap();
    own.seal(&mut sealed).unwrap();
    store
        .backend
        .delete(&object(&format!(".runs/{missing}.json")))
        .unwrap();
    let output = f.cli(&[]);
    assert_eq!(output["ok"], true, "{output}");
    for id in [expired, missing] {
        assert_eq!(f.control(&id).phase, RunPhase::Sealed);
        let bytes = std::fs::read(f.root.join(format!("datasets/data/.runs/{id}.json"))).unwrap();
        let run: SealedRun = decode_record(&bytes).unwrap();
        run.validate_control(&f.control(&id)).unwrap();
    }
    assert!(!f.root.join("datasets/data/.states/LATEST").exists());
    let before = snapshot(&f.root);
    let again = f.cli(&[]);
    assert_eq!(again["ok"], true, "{again}");
    assert!(
        again["result"]["runs"]
            .as_array()
            .unwrap()
            .iter()
            .all(|run| run["state"] == "complete")
    );
    assert_eq!(snapshot(&f.root), before);
}
#[test]
fn requested_live_run_is_busy_and_pending_unrelated_corruption_is_isolated() {
    let f = Fixture::new();
    let expired = f.run();
    let store = f.store();
    let clock = grv_core::clock::SystemClock::default();
    let own = Ownership::new(&store, &clock, 60).unwrap();
    let id = grv_core::clock::new_run_id(&clock.now()).unwrap();
    let live = own
        .prepare_run(name("data"), id.clone(), 0.into(), vec![], None)
        .unwrap();
    own.commit_run(&live).unwrap();
    let before = snapshot(&f.root);
    let busy = f.cli(&["--run", id.as_str()]);
    assert_eq!(busy["errors"][0]["code"], "STATE_CONFLICT", "{busy}");
    assert_eq!(busy["result"]["runs"][0]["state"], "waiting");
    assert_eq!(snapshot(&f.root), before);
    let mut latest = Latest::empty();
    let operation = grv_core::clock::new_run_id(&clock.now()).unwrap();
    latest.pending = Some(format!(".states/operations/{operation}.json"));
    store
        .backend
        .create_bytes(
            &revision::latest_key(&name("data")),
            &encode_record(&latest).unwrap(),
        )
        .unwrap();
    store
        .backend
        .create_bytes(
            &object(&format!(".states/operations/{operation}.json")),
            b"malformed unrelated pending description",
        )
        .unwrap();
    store
        .backend
        .create_bytes(
            &object(".runs/garbage.control.json"),
            b"malformed unrelated run",
        )
        .unwrap();
    let latest = std::fs::read(f.root.join("datasets/data/.states/LATEST")).unwrap();
    let targeted = f.cli(&["--run", expired.as_str()]);
    assert_eq!(targeted["ok"], true, "{targeted}");
    assert!(
        targeted["result"]["completed_operation_ids"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        std::fs::read(f.root.join("datasets/data/.states/LATEST")).unwrap(),
        latest
    );
    assert_eq!(f.control(&id).phase, RunPhase::Open);
    let unscoped = f.cli(&["--dry-run"]);
    assert_eq!(unscoped["errors"][0]["code"], "PROTOCOL_FAILURE");
}
#[test]
fn recovery_retains_claims_waiting_without_attestation_but_finishes_other_eligible_work() {
    let f = Fixture::new();
    let blocked = f.run();
    let healthy = f.run();
    let store = f.store();
    let own = Ownership::new(&store, &OldClock, 60).unwrap();
    // The original owner is still active according to its acquisition clock;
    // the CLI sees both old run and claim expired, without authority to assume
    // the source writer stopped merely from those wall-clock observations.
    let owner = own
        .prepare_run(
            name("data"),
            grv_core::clock::new_run_id(&OldClock.now()).unwrap(),
            0.into(),
            vec![],
            None,
        )
        .unwrap();
    let mut owner = owner;
    own.commit_run(&owner).unwrap();
    own.confirm_holds(&mut owner).unwrap();
    let allocated = owner.control().run_id.clone();
    let contract = grv_adapter_api::TableContract {
        columns: vec![grv_adapter_api::Column {
            name: "id".into(),
            logical_type: json!("int64"),
        }],
        partition_keys: vec![],
        extensions: json!({}),
        column_ext: json!({}),
    };
    let intent = own
        .prepare_reservation(
            &owner,
            TableLayout {
                table: name("events"),
                partition_keys: vec![],
                extensions: None,
            },
            Partition::new(),
            &contract,
        )
        .unwrap();
    own.reserve_authorized(&owner, &intent, &mut ReservationProgress::Prepared, |_| {
        Ok(())
    })
    .unwrap();
    let claim = std::fs::read(f.root.join("datasets/data/events/.claim")).unwrap();
    let control = f.control(&allocated);
    let result = f.cli(&[]);
    assert_eq!(result["errors"][0]["code"], "STATE_CONFLICT", "{result}");
    assert_eq!(f.control(&allocated), control);
    assert_eq!(
        std::fs::read(f.root.join("datasets/data/events/.claim")).unwrap(),
        claim
    );
    assert_eq!(f.control(&healthy).phase, RunPhase::Sealed);
    assert_eq!(f.control(&blocked).phase, RunPhase::Sealed);
    assert!(
        result["result"]["runs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|run| run["run_id"] == json!(allocated) && run["state"] == "waiting")
    );
    assert!(
        !result.to_string().contains(
            serde_json::to_value(control.owner_token)
                .unwrap()
                .as_str()
                .unwrap()
        )
    );
}
#[test]
fn recovery_replays_only_committed_pending_effects_reports_ids_and_preserves_data() {
    let f = Fixture::new();
    let id = f.run();
    let store = f.store();
    let operation = grv_core::clock::new_run_id(&OldClock.now()).unwrap();
    let pin_id = grv_types::Uuid::v4();
    let scope = PinScope::Table(TableScope {
        table: name("events"),
    });
    let record = OperationRecord {
        operation_id: operation.clone(),
        dataset: name("data"),
        created_at: OldClock.now(),
        created_by: "recovery-fixture".into(),
        body: OperationPayload::Pin(PinPayload {
            pin_id: pin_id.clone(),
            scope: scope.clone(),
            reason: Some("already authorized pending pin".into()),
        }),
    };
    store
        .backend
        .create_bytes(
            &object("events/.layout.json"),
            &encode_record(&TableLayout {
                table: name("events"),
                partition_keys: vec![],
                extensions: None,
            })
            .unwrap(),
        )
        .unwrap();
    store
        .backend
        .create_bytes(
            &object(&format!(".states/operations/{operation}.json")),
            &encode_record(&record).unwrap(),
        )
        .unwrap();
    let mut latest = Latest::empty();
    latest.pending = Some(format!(".states/operations/{operation}.json"));
    store
        .backend
        .create_bytes(
            &revision::latest_key(&name("data")),
            &encode_record(&latest).unwrap(),
        )
        .unwrap();
    let output = f.cli(&[]);
    assert_eq!(output["ok"], true, "{output}");
    assert_eq!(
        output["result"]["completed_operation_ids"],
        json!([operation])
    );
    assert!(output["result"]["pending"].is_null());
    let latest: Latest =
        decode_record(&std::fs::read(f.root.join("datasets/data/.states/LATEST")).unwrap())
            .unwrap();
    assert!(latest.pending.is_none());
    assert!(latest.lease.is_none());
    assert_eq!(latest.high_water.get(), 0);
    assert_eq!(latest.revision.get(), 0);
    let pin = grv_core::retention::pin_path(&store, &name("data"), &scope, &pin_id, false).unwrap();
    assert!(store.backend.head(&pin).is_ok());
    assert_eq!(f.control(&id).phase, RunPhase::Sealed);
    assert!(!f.root.join("datasets/data/.states/released-holds").exists());
    let before = snapshot(&f.root);
    assert_eq!(f.cli(&[])["ok"], true);
    assert_eq!(snapshot(&f.root), before);
}
