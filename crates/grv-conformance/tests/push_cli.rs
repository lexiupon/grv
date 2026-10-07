use grv_adapter_host::validate_output;
use serde_json::Value;
use std::{os::unix::fs::PermissionsExt, path::Path, process::Command};

fn cli(adapters: &Path, args: &[&str]) -> Value {
    let output = Command::new(env!("CARGO_BIN_EXE_grv-conformance-cli"))
        .arg("--json")
        .args(args)
        .env("GRV_ADAPTERS_DIR", adapters)
        .output()
        .unwrap();
    let value: Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|e| panic!("{e}: {}", String::from_utf8_lossy(&output.stderr)));
    validate_output(&value).unwrap();
    assert_eq!(output.status.success(), value["ok"] == true);
    value
}

#[test]
fn real_cli_push_publishes_empty_table_deduplicates_and_replays_without_source_or_grv() {
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
    std::fs::write(package.join("adapter.toml"), format!(
        "name = 'fixture'\nversion = '0.1.0'\ninterface_versions = [1]\nbinding_schema_version = 1\nentrypoint = {:?}\n", env!("CARGO_BIN_EXE_fixture")
    )).unwrap();
    let root = temp.path().join("grv");
    let state = temp.path().join("state");
    let decl = temp.path().join("push.yml");
    let authored = "declaration_version: 1\nkind: push\ndataset: data\nadapter: fixture\nconnection: {}\ntables:\n  - name: rows\n    source: {}\n    columns:\n      - {name: value, source: value, type: int64}\n  - name: empty\n    source: {}\n    columns:\n      - {name: value, source: value, type: int64}\n";
    std::fs::write(&decl, authored).unwrap();
    assert_eq!(
        cli(&adapters, &["init", "--grv", root.to_str().unwrap()])["ok"],
        true
    );
    let attempt = grv_types::Uuid::v4();
    let invoke = |id: &str| {
        cli(
            &adapters,
            &[
                "push",
                "--grv",
                root.to_str().unwrap(),
                "--decl",
                decl.to_str().unwrap(),
                "--state",
                state.to_str().unwrap(),
                "--attempt",
                id,
            ],
        )
    };
    // Adapter validation can fail after YAML parsing. It must leave no empty
    // attempt journal that would prevent correcting and retrying this ID.
    std::fs::write(
        &decl,
        authored.replace("connection: {}", "connection: {invalid: true}"),
    )
    .unwrap();
    let rejected = invoke(attempt.as_str());
    assert_eq!(
        rejected["errors"][0]["code"], "INVALID_DECLARATION",
        "{rejected}"
    );
    assert!(!state.join("push").join(attempt.as_str()).exists());
    std::fs::write(&decl, authored).unwrap();
    let first = invoke(attempt.as_str());
    assert_eq!(first["ok"], true, "{first}");
    assert_eq!(first["result"]["outcome"]["kind"], "published");
    assert_eq!(first["result"]["outcome"]["revision"], "1");
    assert_eq!(first["result"]["replayed"], false);
    assert_eq!(first["result"]["adapter_result"]["rows"], "3");
    let second_attempt = grv_types::Uuid::v4();
    let second = invoke(second_attempt.as_str());
    assert_eq!(second["ok"], true, "{second}");
    assert_eq!(second["result"]["outcome"]["kind"], "no-op");
    assert_eq!(second["result"]["outcome"]["revision"], "1");
    assert!(second["result"]["outcome"]["operation_id"].is_null());
    let pin_id = grv_types::Uuid::v4();
    let pin = |reason: &str, revision: &str| {
        cli(
            &adapters,
            &[
                "pin",
                "data",
                "--grv",
                root.to_str().unwrap(),
                "--revision",
                revision,
                "--pin",
                pin_id.as_str(),
                "--reason",
                reason,
                "--state",
                state.to_str().unwrap(),
            ],
        )
    };
    let latest_path = root.join("datasets/data/.states/LATEST");
    let before: Value = serde_json::from_slice(&std::fs::read(&latest_path).unwrap()).unwrap();
    let pinned = pin("retain for conformance", "1");
    assert_eq!(pinned["ok"], true, "{pinned}");
    assert_eq!(pinned["result"]["pin"]["active"], true);
    assert_eq!(pinned["result"]["no_op"], false);
    let duplicate = pin("retain for conformance", "1");
    assert_eq!(duplicate["ok"], true, "{duplicate}");
    assert_eq!(duplicate["result"]["no_op"], true);
    assert_eq!(duplicate["result"]["pin"], pinned["result"]["pin"]);
    let changed_reason = pin("different audit reason", "1");
    assert_eq!(
        changed_reason["errors"][0]["code"], "STATE_CONFLICT",
        "{changed_reason}"
    );
    let unknown_revision = pin("retain for conformance", "999");
    assert_eq!(unknown_revision["ok"], false, "{unknown_revision}");
    let unpin = |revision: &str| {
        cli(
            &adapters,
            &[
                "unpin",
                "data",
                "--grv",
                root.to_str().unwrap(),
                "--revision",
                revision,
                "--pin",
                pin_id.as_str(),
                "--state",
                state.to_str().unwrap(),
            ],
        )
    };
    let wrong_scope = unpin("999");
    assert_eq!(
        wrong_scope["errors"][0]["code"], "NOT_FOUND",
        "{wrong_scope}"
    );
    let released = unpin("1");
    assert_eq!(released["ok"], true, "{released}");
    assert_eq!(released["result"]["pin"]["active"], false);
    assert_eq!(released["result"]["no_op"], false);
    assert!(released["result"]["pin"]["released_at"].is_string());
    assert!(
        released["result"]["remaining_protections"]
            .as_array()
            .unwrap()
            .iter()
            .any(|protection| protection["kind"] == "current" && protection["active"] == true)
    );
    let already_released = unpin("1");
    assert_eq!(already_released["ok"], true, "{already_released}");
    assert_eq!(already_released["result"]["no_op"], true);
    assert_eq!(already_released["result"]["pin"], released["result"]["pin"]);
    let reuse = pin("retain for conformance", "1");
    assert_eq!(reuse["errors"][0]["code"], "STATE_CONFLICT", "{reuse}");
    let after: Value = serde_json::from_slice(&std::fs::read(latest_path).unwrap()).unwrap();
    assert_eq!(before["revision"], after["revision"]);
    assert_eq!(before["high_water"], after["high_water"]);
    assert!(after["lease"].is_null());
    // GC defaults to a strictly read-only preview, even if a state location is
    // supplied. Current entries and immutable administration history survive.
    let gc_state = temp.path().join("gc-state");
    let snapshot = || {
        fn walk(path: &Path, output: &mut std::collections::BTreeMap<std::path::PathBuf, Vec<u8>>) {
            for entry in std::fs::read_dir(path).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    walk(&path, output)
                } else {
                    output.insert(path.clone(), std::fs::read(path).unwrap());
                }
            }
        }
        let mut files = std::collections::BTreeMap::new();
        walk(&root, &mut files);
        files
    };
    let before_gc = snapshot();
    let preview = cli(
        &adapters,
        &[
            "gc",
            "data",
            "--grv",
            root.to_str().unwrap(),
            "--state",
            gc_state.to_str().unwrap(),
        ],
    );
    assert_eq!(preview["ok"], true, "{preview}");
    assert_eq!(preview["result"]["mode"], "dry-run");
    assert!(
        preview["result"]["versions"]
            .as_array()
            .unwrap()
            .iter()
            .all(|v| v["state"] == "protected")
    );
    assert_eq!(
        preview["result"]["completed_operation_ids"],
        serde_json::json!([])
    );
    assert_eq!(before_gc, snapshot());
    assert!(!gc_state.exists());
    let conflict = cli(
        &adapters,
        &[
            "gc",
            "data",
            "--grv",
            root.to_str().unwrap(),
            "--dry-run",
            "--apply",
        ],
    );
    assert_eq!(conflict["errors"][0]["code"], "INVALID_ARGUMENT");
    assert_eq!(before_gc, snapshot());
    let applied = cli(
        &adapters,
        &[
            "gc",
            "data",
            "--grv",
            root.to_str().unwrap(),
            "--apply",
            "--state",
            gc_state.to_str().unwrap(),
        ],
    );
    assert_eq!(applied["ok"], true, "{applied}");
    assert_eq!(applied["result"]["mode"], "apply");
    assert!(
        applied["result"]["versions"]
            .as_array()
            .unwrap()
            .iter()
            .all(|v| v["state"] == "protected")
    );
    let after_gc: Value =
        serde_json::from_slice(&std::fs::read(root.join("datasets/data/.states/LATEST")).unwrap())
            .unwrap();
    assert_eq!(after_gc["revision"], after["revision"]);
    assert_eq!(after_gc["high_water"], after["high_water"]);
    assert!(after_gc["lease"].is_null());
    for (path, bytes) in before_gc {
        if path.file_name().unwrap() != "LATEST" {
            assert_eq!(std::fs::read(path).unwrap(), bytes);
        }
    }
    // Crash boundaries before source acquisition: no run create, run created,
    // and holds confirmed. The durable request must continue without replacing
    // its run or allocating versions for this unchanged capture.
    {
        use grv_core::{
            capture::AcquisitionProgress,
            clock::{Clock, SystemClock, new_run_id},
            journal::{Evidence, Journal},
            ownership::Ownership,
            store::Store,
        };
        let original: Value = serde_json::from_slice(
            &std::fs::read(
                state
                    .join("push")
                    .join(attempt.as_str())
                    .join("journal.json"),
            )
            .unwrap(),
        )
        .unwrap();
        let store = Store::open(grv_storage::LocalBackend::open(&root).unwrap()).unwrap();
        let clock = SystemClock::default();
        let ownership = Ownership::new(&store, &clock, 900).unwrap();
        for stage in 0..3 {
            let id = grv_types::Uuid::v4();
            let run_id = new_run_id(&clock.now()).unwrap();
            let prepared = ownership
                .prepare_run(
                    grv_types::Name::new("data").unwrap(),
                    run_id.clone(),
                    1.into(),
                    vec![],
                    None,
                )
                .unwrap();
            let mut intent = original["evidence"]["intent"].clone();
            intent["run"] = serde_json::to_value(&prepared).unwrap();
            intent["request"]["attempt_id"] = serde_json::to_value(&id).unwrap();
            intent["request"]["stream_id"] = serde_json::to_value(grv_types::Uuid::v4()).unwrap();
            intent["request"]["run_id"] = serde_json::to_value(&run_id).unwrap();
            let request: grv_adapter_api::ExtractRequest =
                serde_json::from_value(intent["request"].clone()).unwrap();
            let journal = Journal::create(
                state.join("push").join(id.as_str()),
                std::slice::from_ref(&root),
            )
            .unwrap();
            let evidence: Evidence<Value, Value, Value, Value> = Evidence {
                intent,
                capture: None,
                progress: serde_json::json!({
                    "acquisition": AcquisitionProgress::prepared(&request),
                    "push": null,
                    "owner": null,
                    "aborted": null,
                    "abort_claims": [],
                    "initial_latest": null,
                    "after_publish": null
                }),
                terminal: None,
            };
            journal.create_evidence(evidence).unwrap();
            if stage >= 1 {
                ownership.commit_run(&prepared).unwrap();
            }
            if stage == 2 {
                ownership.confirm_holds(&mut prepared.clone()).unwrap();
            }
            drop(journal);
            let result = invoke(id.as_str());
            assert_eq!(result["ok"], true, "stage {stage}: {result}");
            assert_eq!(result["result"]["outcome"]["kind"], "no-op");
            assert_eq!(result["result"]["run_id"], run_id.as_str());
            assert_eq!(result["result"]["outcome"]["revision"], "1");
        }
    }
    std::fs::write(&decl, authored.replace("dataset: data", "dataset: other")).unwrap();
    let mismatch = invoke(attempt.as_str());
    assert_eq!(mismatch["ok"], false);
    assert_eq!(mismatch["errors"][0]["code"], "REQUEST_MISMATCH");
    std::fs::write(&decl, authored).unwrap();
    std::fs::remove_dir_all(&adapters).unwrap();
    std::fs::remove_dir_all(&root).unwrap();
    let replay = invoke(attempt.as_str());
    assert_eq!(replay["ok"], true, "{replay}");
    let mut expected = first["result"].clone();
    expected["replayed"] = true.into();
    assert_eq!(replay["result"], expected);
    // Reconstruct the crash window after durable core outcome but before the
    // outer CLI result was recorded. Both the source and GRV are unavailable.
    let record_path = state
        .join("push")
        .join(attempt.as_str())
        .join("journal.json");
    let mut recorded: Value =
        serde_json::from_slice(&std::fs::read(&record_path).unwrap()).unwrap();
    assert!(!recorded["evidence"]["progress"]["push"]["terminal"].is_null());
    recorded["evidence"]["terminal"] = Value::Null;
    std::fs::write(&record_path, serde_json::to_vec(&recorded).unwrap()).unwrap();
    std::fs::File::open(&record_path)
        .unwrap()
        .sync_all()
        .unwrap();
    let replay = invoke(attempt.as_str());
    assert_eq!(replay["ok"], true, "{replay}");
    assert_eq!(replay["result"], expected);
}

#[test]
#[ignore = "requires an explicitly configured dedicated S3 root and profile"]
fn live_cli_s3_capture_publication_deduplication_and_source_free_terminal_replay() {
    use grv_storage::{
        Backend, ListEntry, ListMode, ObjectKey, ObjectPrefix,
        cloud::{CloudBackend, CloudOptions},
    };
    let parent = std::env::var("GRV_S3_TEST_ROOT").expect("dedicated test root required");
    assert!(parent.starts_with("s3://"));
    let root = format!(
        "{}/cli-conformance-{}",
        parent.trim_end_matches('/'),
        grv_types::Uuid::v4()
    );
    eprintln!("S3 CLI conformance root: {root}");
    let backend = CloudBackend::open(&root, CloudOptions::default()).unwrap();
    struct Cleanup<'a>(&'a CloudBackend);
    impl Drop for Cleanup<'_> {
        fn drop(&mut self) {
            match self.0.list(
                &ObjectPrefix::new("datasets/").unwrap(),
                ListMode::Recursive,
            ) {
                Ok(objects) => {
                    for object in objects {
                        if let ListEntry::Object(key) = object
                            && self.0.delete(&key).is_err()
                        {
                            eprintln!("S3 CLI test cleanup failed for {}", key.as_str());
                        }
                    }
                }
                Err(_) => eprintln!("S3 CLI test cleanup listing failed"),
            }
            if self.0.delete(&ObjectKey::new("grv.json").unwrap()).is_err() {
                eprintln!("S3 CLI test root metadata cleanup failed");
            }
        }
    }
    let _cleanup = Cleanup(&backend);
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
    std::fs::write(package.join("adapter.toml"), format!("name = 'fixture'\nversion = '0.1.0'\ninterface_versions = [1]\nbinding_schema_version = 1\nentrypoint = {:?}\n", env!("CARGO_BIN_EXE_fixture"))).unwrap();
    let decl = temp.path().join("push.yml");
    std::fs::write(&decl, "declaration_version: 1\nkind: push\ndataset: data\nadapter: fixture\nconnection: {}\ntables:\n  - name: rows\n    source: {}\n    columns:\n      - {name: value, source: value, type: int64}\n  - name: empty\n    source: {}\n    columns:\n      - {name: value, source: value, type: int64}\n").unwrap();
    let state = temp.path().join("state");
    assert_eq!(cli(&adapters, &["init", "--grv", &root])["ok"], true);
    let attempt = grv_types::Uuid::v4();
    let invoke = |id: &str| {
        cli(
            &adapters,
            &[
                "push",
                "--grv",
                &root,
                "--decl",
                decl.to_str().unwrap(),
                "--state",
                state.to_str().unwrap(),
                "--attempt",
                id,
            ],
        )
    };
    let published = invoke(attempt.as_str());
    assert_eq!(published["ok"], true, "{published}");
    assert_eq!(published["result"]["outcome"]["kind"], "published");
    assert_eq!(published["result"]["outcome"]["revision"], "1");
    let deduplicated = invoke(grv_types::Uuid::v4().as_str());
    assert_eq!(deduplicated["ok"], true, "{deduplicated}");
    assert_eq!(deduplicated["result"]["outcome"]["kind"], "no-op");
    assert_eq!(deduplicated["result"]["outcome"]["revision"], "1");
    std::fs::remove_dir_all(&adapters).unwrap();
    for object in backend
        .list(
            &ObjectPrefix::new("datasets/").unwrap(),
            ListMode::Recursive,
        )
        .unwrap()
    {
        if let ListEntry::Object(key) = object {
            backend.delete(&key).unwrap();
        }
    }
    backend
        .delete(&ObjectKey::new("grv.json").unwrap())
        .unwrap();
    let replay = invoke(attempt.as_str());
    assert_eq!(replay["ok"], true, "{replay}");
    let mut expected = published["result"].clone();
    expected["replayed"] = true.into();
    assert_eq!(replay["result"], expected);
}
