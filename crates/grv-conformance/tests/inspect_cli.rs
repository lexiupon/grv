//! Process-backed inspection fixtures use canonical publication, and validate
//! every success and failure against the public command envelope schema.
use grv_adapter_host::validate_output;
use grv_core::{
    clock::{Clock, SystemClock, new_run_id},
    publication::{LeaseProgress, Publisher},
    revision,
    store::Store,
};
use grv_storage::{Backend, LocalBackend, ObjectKey, model::*};
use grv_types::Name;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    process::Command,
};
struct Fixture {
    _temp: tempfile::TempDir,
    root: PathBuf,
    adapters: PathBuf,
    state: PathBuf,
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
        let adapters = temp.path().join("adapters");
        let package = adapters.join("fixture");
        std::fs::create_dir_all(&package).unwrap();
        std::fs::write(package.join("adapter.toml"),format!("name = 'fixture'\nversion = '0.1.0'\ninterface_versions = [1]\nbinding_schema_version = 1\nentrypoint = {:?}\n",env!("CARGO_BIN_EXE_fixture"))).unwrap();
        let root = temp.path().join("grv");
        let state = temp.path().join("state");
        let decl = temp.path().join("push.yml");
        std::fs::write(&decl,"declaration_version: 1\nkind: push\ndataset: data\nadapter: fixture\nconnection: {}\ntables:\n  - name: rows\n    source: {}\n    columns:\n      - {name: value, source: value, type: int64}\n  - name: empty\n    source: {}\n    columns:\n      - {name: value, source: value, type: int64}\n").unwrap();
        let result = Self {
            _temp: temp,
            root,
            adapters,
            state,
        };
        assert_eq!(
            result.cli(&["init", "--grv", result.root.to_str().unwrap()])["ok"],
            true
        );
        let pushed = result.cli(&[
            "push",
            "--grv",
            result.root.to_str().unwrap(),
            "--decl",
            decl.to_str().unwrap(),
            "--state",
            result.state.to_str().unwrap(),
        ]);
        assert_eq!(pushed["ok"], true, "{pushed}");
        result
    }
    fn cli(&self, args: &[&str]) -> Value {
        let output = Command::new(env!("CARGO_BIN_EXE_grv-conformance-cli"))
            .arg("--json")
            .args(args)
            .env("GRV_ADAPTERS_DIR", &self.adapters)
            .output()
            .unwrap();
        let value: Value = serde_json::from_slice(&output.stdout)
            .unwrap_or_else(|error| panic!("{error}: {}", String::from_utf8_lossy(&output.stderr)));
        validate_output(&value).unwrap_or_else(|error| panic!("{error}: {value}"));
        assert_eq!(output.status.success(), value["ok"] == true, "{value}");
        value
    }
    fn inspect(&self, command: &str, extra: &[&str]) -> Value {
        let mut args = vec![command, "data", "--grv", self.root.to_str().unwrap()];
        args.extend_from_slice(extra);
        self.cli(&args)
    }
    fn store(&self) -> Store<LocalBackend> {
        Store::open(LocalBackend::open(&self.root).unwrap()).unwrap()
    }
    fn omit_rows(&self) {
        let store = self.store();
        let clock = SystemClock::default();
        let publisher = Publisher::new(&store, &clock, 60).unwrap();
        let intent = publisher
            .prepare_lease(name("data"), "inspection-fixture".into())
            .unwrap();
        let mut owner = publisher
            .acquire_authorized(&intent, &mut LeaseProgress::Prepared, |_| Ok(()))
            .unwrap();
        let candidate = publisher
            .prepare(
                &mut owner,
                ChangeSet {
                    omissions: vec![Omission {
                        table: name("rows"),
                        partition: None,
                    }],
                    reason: Some("remove rows for retained-history test".into()),
                    ..Default::default()
                },
                self._temp.path(),
            )
            .unwrap();
        assert_eq!(
            publisher
                .commit(candidate, |_| Ok(()))
                .unwrap()
                .revision
                .get(),
            2
        );
    }
}
fn name(value: &str) -> Name {
    Name::new(value).unwrap()
}
fn snapshot(root: &Path) -> BTreeMap<PathBuf, (Vec<u8>, i64, i64)> {
    fn walk(root: &Path, path: &Path, result: &mut BTreeMap<PathBuf, (Vec<u8>, i64, i64)>) {
        for item in std::fs::read_dir(path).unwrap() {
            let item = item.unwrap();
            let path = item.path();
            if path.is_dir() {
                walk(root, &path, result);
            } else {
                let metadata = std::fs::metadata(&path).unwrap();
                result.insert(
                    path.strip_prefix(root).unwrap().to_owned(),
                    (
                        std::fs::read(path).unwrap(),
                        metadata.mtime(),
                        metadata.mtime_nsec(),
                    ),
                );
            }
        }
    }
    let mut result = BTreeMap::new();
    walk(root, root, &mut result);
    result
}
#[test]
fn all_inspections_are_schema_valid_read_only_without_adapter_and_keep_historic_schema() {
    let fixture = Fixture::new();
    let store = fixture.store();
    let schema = ObjectKey::new("datasets/data/rows/.schema.json").unwrap();
    let (bytes, metadata) = store.backend.read_bytes(&schema, 1024 * 1024).unwrap();
    let mut baseline: SchemaBaseline = decode_record(&bytes).unwrap();
    baseline.columns.push(StorageColumn {
        name: "later".into(),
        logical_type: serde_json::json!("string"),
        ext: None,
    });
    baseline.mutation_id = grv_types::Uuid::v4();
    store
        .backend
        .put_bytes(
            &schema,
            &metadata.validator,
            &encode_record(&baseline).unwrap(),
        )
        .unwrap();
    // A supervised adapter canary would write this marker if any inspection
    // started it. Retained GRV records suffice even with the fixture gone.
    let marker = fixture._temp.path().join("adapter-started");
    let script = fixture._temp.path().join("canary.sh");
    std::fs::write(
        &script,
        format!("#!/bin/sh\ntouch '{}'\nexit 99\n", marker.display()),
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(fixture.adapters.join("fixture/adapter.toml"),format!("name = 'fixture'\nversion = '0.1.0'\ninterface_versions = [1]\nbinding_schema_version = 1\nentrypoint = {:?}\n",script)).unwrap();
    let before = snapshot(&fixture.root);
    let datasets = fixture.cli(&["ls", "--grv", fixture.root.to_str().unwrap()]);
    assert_eq!(datasets["result"]["items"][0]["current_revision"], "1");
    let tables = fixture.inspect("ls", &[]);
    assert_eq!(tables["result"]["items"].as_array().unwrap().len(), 2);
    let partitions = fixture.inspect("ls", &["--table", "empty"]);
    assert_eq!(partitions["result"]["items"].as_array().unwrap().len(), 1);
    assert_eq!(partitions["result"]["items"][0]["object"]["version"], "1");
    let show = fixture.inspect("show", &[]);
    assert_eq!(show["ok"], true, "{show}");
    let rows = show["result"]["tables"]
        .as_array()
        .unwrap()
        .iter()
        .find(|table| table["table"] == "rows")
        .unwrap();
    assert_eq!(
        rows["baseline_schema"]["columns"].as_array().unwrap().len(),
        2
    );
    assert_eq!(
        rows["revision_schema"]["columns"].as_array().unwrap().len(),
        1
    );
    assert_eq!(fixture.inspect("status", &[])["ok"], true);
    let closed = fixture.inspect("status", &["--decl", "nonexistent.yml"]);
    assert_eq!(closed["errors"][0]["code"], "INVALID_ARGUMENT");
    assert_eq!(closed["result"]["current_revision"], "1");
    assert_eq!(
        fixture.inspect("log", &[])["result"]["entries"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let equal = fixture.inspect("diff", &["--from", "1", "--to", "latest"]);
    assert_eq!(equal["ok"], true);
    assert_eq!(equal["result"]["changed"], false);
    let added = fixture.inspect("diff", &["--from", "0", "--to", "1"]);
    assert_eq!(added["result"]["changed"], true);
    assert_eq!(added["result"]["entries"].as_array().unwrap().len(), 2);
    let verified = fixture.inspect("verify", &["--full"]);
    assert_eq!(verified["ok"], true, "{verified}");
    assert!(
        verified["result"]["checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| check["check"] == "file-sha256")
    );
    let tokens: Vec<String> = before
        .iter()
        .filter(|(path, _)| {
            path.to_string_lossy().ends_with(".control.json")
                || path.to_string_lossy().ends_with("manifest.json")
        })
        .filter_map(|(_, (bytes, _, _))| serde_json::from_slice::<Value>(bytes).ok())
        .flat_map(|record| {
            ["owner_token", "claim_token"]
                .into_iter()
                .filter_map(move |field| record[field].as_str().map(str::to_owned))
        })
        .collect();
    for token in tokens {
        assert!(!show.to_string().contains(&token));
    }
    assert!(!marker.exists());
    assert_eq!(snapshot(&fixture.root), before);
}
#[test]
fn committed_history_survives_pruning_and_orphan_paths_never_become_endpoints() {
    let fixture = Fixture::new();
    fixture.omit_rows();
    let store = fixture.store();
    let mut orphan = revision::read(&store, &name("data"), 1.into(), fixture._temp.path()).unwrap();
    orphan.revision = 99.into();
    orphan.previous_revision = 1.into();
    let file = revision::encode(&orphan, fixture._temp.path()).unwrap();
    store
        .backend
        .conditional_create(
            &revision::revision_key(&name("data"), 99.into()),
            &mut std::fs::File::open(file.path()).unwrap(),
        )
        .unwrap();
    let folder = "datasets/data/rows/version=1";
    let clock = SystemClock::default();
    let marker = PrunedMarker {
        operation_id: new_run_id(&clock.now()).unwrap(),
        pruned_by: "conformance-authorized-fixture".into(),
        pruned_at: clock.now(),
        table: name("rows"),
        partition: Partition::new(),
        version: 1.into(),
    };
    store
        .backend
        .create_bytes(
            &ObjectKey::new(format!("{folder}/.pruned")).unwrap(),
            &encode_record(&marker).unwrap(),
        )
        .unwrap();
    store
        .backend
        .delete(&ObjectKey::new(format!("{folder}/data.parquet")).unwrap())
        .unwrap();
    store
        .backend
        .delete(&ObjectKey::new(format!("{folder}/manifest.json")).unwrap())
        .unwrap();
    let before = snapshot(&fixture.root);
    let show = fixture.inspect("show", &["--revision", "1", "--retention"]);
    assert_eq!(show["ok"], true, "{show}");
    let rows = show["result"]["tables"]
        .as_array()
        .unwrap()
        .iter()
        .find(|table| table["table"] == "rows")
        .unwrap();
    assert_eq!(rows["availability"], "unavailable");
    assert!(rows["revision_schema"].is_null());
    assert!(rows["provenance"][0]["run"].is_object());
    let log = fixture.inspect("log", &[]);
    assert_eq!(log["result"]["entries"].as_array().unwrap().len(), 2);
    assert_eq!(log["result"]["entries"][0]["revision"], "2");
    let limited = fixture.inspect("log", &["--limit", "1"]);
    assert_eq!(limited["result"]["has_more"], true);
    let diff = fixture.inspect("diff", &["--from", "1", "--to", "latest"]);
    assert_eq!(diff["ok"], true, "{diff}");
    assert_eq!(diff["result"]["entries"][0]["kind"], "removed");
    assert_eq!(diff["result"]["schemas"][1]["state"], "unknown");
    let verified = fixture.inspect("verify", &["--revision", "1"]);
    assert_eq!(verified["errors"][0]["code"], "UNAVAILABLE", "{verified}");
    assert_eq!(
        verified["result"]["unavailable"].as_array().unwrap().len(),
        1
    );
    let orphan_show = fixture.inspect("show", &["--revision", "99"]);
    assert_eq!(orphan_show["errors"][0]["code"], "NOT_FOUND");
    let orphan_verify = fixture.inspect("verify", &["--revision", "99"]);
    assert_eq!(orphan_verify["errors"][0]["code"], "PROTOCOL_FAILURE");
    let orphan_diff = fixture.inspect("diff", &["--from", "99", "--to", "2"]);
    assert_eq!(orphan_diff["errors"][0]["code"], "NOT_FOUND");
    assert_eq!(
        fixture.inspect("ls", &[])["result"]["items"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(snapshot(&fixture.root), before);
}
#[test]
fn verification_distinguishes_hash_corruption_malformed_coordination_and_missing_data() {
    let fixture = Fixture::new();
    let file = fixture
        .root
        .join("datasets/data/rows/version=1/data.parquet");
    let original = std::fs::read(&file).unwrap();
    let mut corrupt = original.clone();
    corrupt[10] ^= 1;
    std::fs::write(&file, &corrupt).unwrap();
    for flags in [vec![], vec!["--full"]] {
        let report = fixture.inspect("verify", &flags);
        assert_eq!(report["errors"][0]["code"], "INTEGRITY_FAILURE", "{report}");
        assert!(
            report["result"]["unavailable"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }
    std::fs::write(&file, &original).unwrap();
    std::fs::remove_file(&file).unwrap();
    let missing = fixture.inspect("verify", &[]);
    assert_eq!(missing["errors"][0]["code"], "UNAVAILABLE");
    std::fs::write(&file, &original).unwrap();
    let latest = fixture.root.join("datasets/data/.states/LATEST");
    let bytes = std::fs::read(&latest).unwrap();
    std::fs::write(&latest, b"{\"revision\":1,\"revision\":2}").unwrap();
    let malformed = fixture.inspect("verify", &[]);
    assert_eq!(malformed["errors"][0]["code"], "PROTOCOL_FAILURE");
    std::fs::write(&latest, bytes).unwrap();
    let control = std::fs::read_dir(fixture.root.join("datasets/data/.runs"))
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| path.to_string_lossy().ends_with(".control.json"))
        .unwrap();
    let control_bytes = std::fs::read(&control).unwrap();
    std::fs::remove_file(&control).unwrap();
    let missing_control = fixture.inspect("verify", &[]);
    assert_eq!(
        missing_control["errors"][0]["code"], "PROTOCOL_FAILURE",
        "{missing_control}"
    );
    std::fs::write(&control, control_bytes).unwrap();
}
#[test]
fn zero_revision_missing_coordination_pending_and_released_pins_are_observed_without_repair() {
    let fixture = Fixture::new();
    let store = fixture.store();
    store
        .backend
        .create_bytes(
            &revision::latest_key(&name("zero")),
            &encode_record(&Latest::empty()).unwrap(),
        )
        .unwrap();
    std::fs::create_dir_all(fixture.root.join("datasets/unpublished/physical")).unwrap();
    let before = snapshot(&fixture.root);
    for command in ["ls", "show", "log", "verify"] {
        let output = fixture.cli(&[command, "zero", "--grv", fixture.root.to_str().unwrap()]);
        assert_eq!(output["ok"], true, "{output}");
    }
    let listing = fixture.cli(&["ls", "--grv", fixture.root.to_str().unwrap()]);
    let unpublished = listing["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["object"]["dataset"] == "unpublished")
        .unwrap();
    assert_eq!(unpublished["coordination"], "missing");
    assert!(unpublished["current_revision"].is_null());
    assert_eq!(
        fixture.cli(&[
            "show",
            "unpublished",
            "--grv",
            fixture.root.to_str().unwrap()
        ])["errors"][0]["code"],
        "NOT_FOUND"
    );
    assert_eq!(snapshot(&fixture.root), before);
    let pin = grv_types::Uuid::v4();
    let pinned = fixture.cli(&[
        "pin",
        "data",
        "--grv",
        fixture.root.to_str().unwrap(),
        "--revision",
        "1",
        "--pin",
        pin.as_str(),
        "--reason",
        "inspection audit",
        "--state",
        fixture.state.to_str().unwrap(),
    ]);
    assert_eq!(pinned["ok"], true);
    let unpinned = fixture.cli(&[
        "unpin",
        "data",
        "--grv",
        fixture.root.to_str().unwrap(),
        "--revision",
        "1",
        "--pin",
        pin.as_str(),
        "--state",
        fixture.state.to_str().unwrap(),
    ]);
    assert_eq!(unpinned["ok"], true);
    let clock = SystemClock::default();
    let publisher = Publisher::new(&store, &clock, 60).unwrap();
    let intent = publisher
        .prepare_lease(name("data"), "read-only-holder".into())
        .unwrap();
    let owner = publisher
        .acquire_authorized(&intent, &mut LeaseProgress::Prepared, |_| Ok(()))
        .unwrap();
    let latest_path = revision::latest_key(&name("data"));
    let (bytes, metadata) = store.backend.read_bytes(&latest_path, 1024 * 1024).unwrap();
    let mut latest: Latest = decode_record(&bytes).unwrap();
    let operation = OperationRecord {
        operation_id: new_run_id(&clock.now()).unwrap(),
        dataset: name("data"),
        created_at: clock.now(),
        created_by: "pending-reader-fixture".into(),
        body: OperationPayload::Pin(PinPayload {
            pin_id: grv_types::Uuid::v4(),
            scope: PinScope::Revision(RevisionScope { revision: 1.into() }),
            reason: Some("not replayed".into()),
        }),
    };
    let path = format!(".states/operations/{}.json", operation.operation_id);
    store
        .backend
        .create_bytes(
            &ObjectKey::new(format!("datasets/data/{path}")).unwrap(),
            &encode_record(&operation).unwrap(),
        )
        .unwrap();
    latest.pending = Some(path);
    store
        .backend
        .put_bytes(
            &latest_path,
            &metadata.validator,
            &encode_record(&latest).unwrap(),
        )
        .unwrap();
    let before = snapshot(&fixture.root);
    let status = fixture.inspect("status", &[]);
    assert_eq!(status["result"]["lease"]["holder"], "read-only-holder");
    assert_eq!(status["result"]["pending"]["kind"], "pin");
    assert_eq!(fixture.inspect("ls", &[])["ok"], true);
    let retained = fixture.inspect("show", &["--retention"]);
    assert_eq!(retained["ok"], true, "{retained}");
    assert_eq!(retained["result"]["retention"]["pins"][0]["active"], false);
    assert_eq!(
        retained["result"]["retention"]["pins"][0]["reason"],
        "inspection audit"
    );
    let private = serde_json::to_value(&owner).unwrap();
    let token = private["intent"]["token"].as_str().unwrap();
    assert!(!status.to_string().contains(token));
    assert_eq!(snapshot(&fixture.root), before);
    for (command, flags) in [
        ("log", vec!["--limit", "0"]),
        ("show", vec!["--revision", "01"]),
        ("verify", vec!["--revision", "1", "--revision", "1"]),
    ] {
        assert_eq!(
            fixture.inspect(command, &flags)["errors"][0]["code"],
            "INVALID_ARGUMENT"
        );
    }
}

#[test]
fn retention_distinguishes_live_releasable_and_durably_released_dependency_holds() {
    use grv_core::{
        admin::{Admin, AdminProgress},
        holds::Holds,
        ownership::Ownership,
    };
    let fixture = Fixture::new();
    let store = fixture.store();
    let clock = SystemClock::default();
    let ownership = Ownership::new(&store, &clock, 60).unwrap();
    let holds = Holds::new(&store, &clock, fixture._temp.path());
    let mut consumer = ownership
        .prepare_run(
            name("consumer"),
            new_run_id(&clock.now()).unwrap(),
            0.into(),
            vec![RunInput {
                dataset: name("data"),
                revision: 1.into(),
                retention_id: grv_types::Uuid::v4(),
            }],
            None,
        )
        .unwrap();
    ownership.commit_run(&consumer).unwrap();
    let record = holds
        .prepare(consumer.dataset(), consumer.control())
        .unwrap()
        .remove(0);
    let proof = holds.acquire(&record).unwrap();
    ownership
        .confirm_dependency_holds(&mut consumer, fixture._temp.path(), &[proof])
        .unwrap();
    let before = snapshot(&fixture.root);
    let active = fixture.inspect("show", &["--retention"]);
    assert_eq!(active["ok"], true, "{active}");
    let protection = active["result"]["retention"]["protections"]
        .as_array()
        .unwrap()
        .iter()
        .find(|value| value["kind"] == "hold")
        .unwrap();
    assert_eq!(protection["active"], true);
    assert_eq!(protection["releasable"], false);
    assert_eq!(protection["details"]["consumer_dataset"], "consumer");
    assert_eq!(snapshot(&fixture.root), before);
    ownership.seal(&mut consumer).unwrap();
    let before = snapshot(&fixture.root);
    let releasable = fixture.inspect("show", &["--retention"]);
    let protection = releasable["result"]["retention"]["protections"]
        .as_array()
        .unwrap()
        .iter()
        .find(|value| value["kind"] == "hold")
        .unwrap();
    assert_eq!(protection["active"], true);
    assert_eq!(protection["releasable"], true);
    assert_eq!(snapshot(&fixture.root), before);
    let publisher = Publisher::new(&store, &clock, 60).unwrap();
    let lease = publisher
        .prepare_lease(name("data"), "release-test-fixture".into())
        .unwrap();
    let mut owner = publisher
        .acquire_authorized(&lease, &mut LeaseProgress::Prepared, |_| Ok(()))
        .unwrap();
    let admin = Admin::new(&store, &clock, 60).unwrap();
    let operation = admin
        .prepare_release_holds(
            name("data"),
            vec![HoldRelease {
                consumer_dataset: record.target_dataset.clone(),
                revision: record.revision,
                retention_id: record.retention_id.clone(),
            }],
        )
        .unwrap();
    admin
        .execute(
            &mut owner,
            &operation,
            &mut AdminProgress::Prepared,
            fixture._temp.path(),
            |_, _| Ok(()),
        )
        .unwrap();
    publisher.release(&mut owner).unwrap();
    let before = snapshot(&fixture.root);
    let released = fixture.inspect("show", &["--retention"]);
    assert_eq!(released["ok"], true, "{released}");
    let protection = released["result"]["retention"]["protections"]
        .as_array()
        .unwrap()
        .iter()
        .find(|value| value["kind"] == "hold")
        .unwrap();
    assert_eq!(protection["active"], false);
    assert_eq!(protection["releasable"], false);
    assert!(protection["details"]["released_at"].is_string());
    assert_eq!(
        protection["details"]["release_operation_id"],
        operation.operation().operation_id.as_str()
    );
    assert_eq!(snapshot(&fixture.root), before);
}
