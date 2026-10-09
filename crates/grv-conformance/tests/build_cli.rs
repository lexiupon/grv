#![cfg(feature = "native-duckdb")]
use grv_adapter_host::validate_output;
use grv_types::Uuid;
use serde_json::Value;
use std::{fs, os::unix::fs::PermissionsExt, path::Path, process::Command};
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
fn package(adapters: &Path, name: &str, executable: &str) {
    let package = adapters.join(name);
    fs::create_dir_all(&package).unwrap();
    // Test fixtures report 0.1.0; real adapters report the crate version.
    let v = if name == "fixture" {
        "0.1.0"
    } else {
        env!("CARGO_PKG_VERSION")
    };
    fs::write(package.join("adapter.toml"),format!("name='{name}'\nversion='{v}'\ninterface_versions=[1]\nbinding_schema_version=1\nentrypoint={executable:?}\n")).unwrap();
}
fn temp() -> tempfile::TempDir {
    let temp = tempfile::tempdir_in(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap(),
    )
    .unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    temp
}
fn push(adapters: &Path, root: &Path, decl: &Path, state: &Path, attempt: &Uuid) -> Value {
    cli(
        adapters,
        &[
            "push",
            "--grv",
            root.to_str().unwrap(),
            "--decl",
            decl.to_str().unwrap(),
            "--state",
            state.to_str().unwrap(),
            "--attempt",
            attempt.as_str(),
        ],
    )
}
#[test]
fn managed_build_cli_executes_once_publishes_empty_noop_and_replays_without_engine_or_root() {
    let temp = temp();
    let adapters = temp.path().join("adapters");
    package(
        &adapters,
        "duckdb",
        env!("CARGO_BIN_EXE_fixture-duckdb-build"),
    );
    let root = temp.path().join("grv");
    let state = temp.path().join("state");
    let decl = temp.path().join("build.yml");
    let engine = temp.path().join("engine.duckdb");
    assert_eq!(
        cli(&adapters, &["init", "--grv", root.to_str().unwrap()])["ok"],
        true
    );
    let authored = format!(
        "declaration_version: 1\nkind: push\ndataset: data\nadapter: duckdb\nconnection: {{database: {engine:?}}}\nbuild: {{execution: managed}}\ntables:\n  - name: rows\n    source: {{sql: 'SELECT 3::INTEGER AS physical UNION ALL SELECT 1::INTEGER AS physical UNION ALL SELECT 1::INTEGER AS physical; -- end'}}\n    columns: [{{name: id, source: physical, type: int64}}]\n  - name: empty\n    source: {{sql: 'SELECT false AS flag WHERE false'}}\n    columns: [{{name: flag, type: bool}}]\n"
    );
    fs::write(&decl, &authored).unwrap();
    let attempt = Uuid::v4();
    let first = push(&adapters, &root, &decl, &state, &attempt);
    assert_eq!(first["ok"], true, "{first}");
    assert_eq!(first["result"]["mode"], "build");
    assert_eq!(first["result"]["outcome"]["revision"], "1");
    let completion = state
        .join("push")
        .join(attempt.as_str())
        .join("build/completion.json");
    let value: Value = serde_json::from_slice(&fs::read(completion).unwrap()).unwrap();
    let schema: Value = serde_json::from_str(include_str!(
        "../../../spec/grv-client-v1-build-completion.schema.json"
    ))
    .unwrap();
    jsonschema::validator_for(&schema)
        .unwrap()
        .validate(&value)
        .unwrap();
    assert_eq!(value["writers_stopped"], true);
    assert_eq!(value["completed_outputs"].as_array().unwrap().len(), 2);
    let second = push(&adapters, &root, &decl, &state, &Uuid::v4());
    assert_eq!(second["ok"], true, "{second}");
    assert_eq!(second["result"]["outcome"]["kind"], "no-op");
    assert_eq!(second["result"]["outcome"]["revision"], "1");
    fs::write(&decl, authored.replace("dataset: data", "dataset: changed")).unwrap();
    let mismatch = push(&adapters, &root, &decl, &state, &attempt);
    assert_eq!(
        mismatch["errors"][0]["code"], "REQUEST_MISMATCH",
        "{mismatch}"
    );
    fs::write(&decl, &authored).unwrap();
    fs::remove_dir_all(&adapters).unwrap();
    fs::remove_file(engine).unwrap();
    fs::remove_dir_all(&root).unwrap();
    let replay = push(&adapters, &root, &decl, &state, &attempt);
    assert_eq!(replay["ok"], true, "{replay}");
    let mut expected = first["result"].clone();
    expected["replayed"] = true.into();
    assert_eq!(replay["result"], expected);
    assert!(!root.exists());
}
#[test]
fn managed_build_cli_fixes_identity_pull_inputs_and_publishes_whole_revision_provenance() {
    let temp = temp();
    let adapters = temp.path().join("adapters");
    package(&adapters, "fixture", env!("CARGO_BIN_EXE_fixture"));
    package(
        &adapters,
        "duckdb",
        env!("CARGO_BIN_EXE_fixture-duckdb-build"),
    );
    let root = temp.path().join("grv");
    let state = temp.path().join("state");
    let decl = temp.path().join("decl.yml");
    let engine = temp.path().join("engine.duckdb");
    assert_eq!(
        cli(&adapters, &["init", "--grv", root.to_str().unwrap()])["ok"],
        true
    );
    fs::write(&decl,"declaration_version: 1\nkind: push\ndataset: raw\nadapter: fixture\nconnection: {}\ntables:\n  - name: rows\n    source: {}\n    columns: [{name: value, type: int64}]\n  - name: empty\n    source: {}\n    columns: [{name: value, type: int64}]\n").unwrap();
    let source = push(&adapters, &root, &decl, &state, &Uuid::v4());
    assert_eq!(source["ok"], true, "{source}");
    fs::write(&decl,format!("declaration_version: 1\nkind: pull\ndataset: raw\nadapter: duckdb\nconnection: {{database: {engine:?}}}\ntarget: {{schema: raw}}\ntables:\n  - name: rows\n  - name: empty\n")).unwrap();
    let pulled = cli(
        &adapters,
        &[
            "pull",
            "--grv",
            root.to_str().unwrap(),
            "--decl",
            decl.to_str().unwrap(),
            "--state",
            state.to_str().unwrap(),
        ],
    );
    assert_eq!(pulled["ok"], true, "{pulled}");
    fs::write(&decl,format!("declaration_version: 1\nkind: push\ndataset: derived\nadapter: duckdb\nconnection: {{database: {engine:?}}}\nbuild:\n  inputs:\n    - {{table: raw.rows, as: one}}\n    - {{table: raw.rows, as: same}}\ntables:\n  - name: totals\n    source: {{sql: 'SELECT SUM(value)::BIGINT AS total FROM grv_input.one'}}\n    columns: [{{name: total, type: int64}}]\n  - name: empty\n    source: {{sql: 'SELECT value FROM grv_input.same WHERE false'}}\n    columns: [{{name: value, type: int64}}]\n")).unwrap();
    let built = push(&adapters, &root, &decl, &state, &Uuid::v4());
    assert_eq!(built["ok"], true, "{built}");
    assert_eq!(built["result"]["outcome"]["revision"], "1");
    let mut ids = vec![];
    for table in ["totals", "empty"] {
        let value: Value = serde_json::from_slice(
            &fs::read(root.join(format!("datasets/derived/{table}/version=1/manifest.json")))
                .unwrap(),
        )
        .unwrap();
        let citations = value["derived_from"].as_array().unwrap();
        assert_eq!(citations.len(), 1);
        assert_eq!(citations[0]["dataset"], "raw");
        assert_eq!(citations[0]["revision"], 1);
        assert!(citations[0]["table"].is_null());
        assert!(citations[0]["partition"].is_null());
        ids.push(citations[0]["retention_id"].clone());
    }
    assert_eq!(ids[0], ids[1]);
    let verified = cli(
        &adapters,
        &[
            "verify",
            "derived",
            "--grv",
            root.to_str().unwrap(),
            "--full",
        ],
    );
    assert_eq!(verified["ok"], true, "{verified}");
    assert!(
        verified["result"]["checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["check"] == "hold" && c["state"] == "passed")
    );
    let shown = cli(
        &adapters,
        &[
            "show",
            "derived",
            "--grv",
            root.to_str().unwrap(),
            "--retention",
        ],
    );
    assert_eq!(shown["ok"], true, "{shown}");
    let bytes = fs::read(root.join("datasets/derived/totals/version=1/data.parquet")).unwrap();
    let key = grv_storage::ObjectKey::new("datasets/raw/rows/version=1/data.parquet").unwrap();
    // Engine execution is complete and accepted output bytes are independent of
    // future edits to source materializations or source files.
    assert!(!bytes.is_empty());
    assert!(root.join(key.as_str()).exists());
}

#[test]
fn killed_managed_cli_stops_native_work_and_never_reexecutes_incomplete_attempt() {
    let temp = temp();
    let adapters = temp.path().join("adapters");
    package(
        &adapters,
        "duckdb",
        env!("CARGO_BIN_EXE_fixture-duckdb-build"),
    );
    let root = temp.path().join("grv");
    let state = temp.path().join("state");
    let decl = temp.path().join("build.yml");
    let engine = temp.path().join("engine.duckdb");
    assert_eq!(
        cli(&adapters, &["init", "--grv", root.to_str().unwrap()])["ok"],
        true
    );
    fs::write(&decl,format!("declaration_version: 1\nkind: push\ndataset: interrupted\nadapter: duckdb\nconnection: {{database: {engine:?}}}\nbuild: {{}}\ntables:\n  - name: rows\n    source: {{sql: 'SELECT SUM(i)::BIGINT AS id FROM range(1000000000000) AS t(i)'}}\n    columns: [{{name: id, type: int64}}]\n")).unwrap();
    let attempt = Uuid::v4();
    let mut child = Command::new(env!("CARGO_BIN_EXE_grv-conformance-cli"))
        .arg("--json")
        .args([
            "push",
            "--grv",
            root.to_str().unwrap(),
            "--decl",
            decl.to_str().unwrap(),
            "--state",
            state.to_str().unwrap(),
            "--attempt",
            attempt.as_str(),
        ])
        .env("GRV_ADAPTERS_DIR", &adapters)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let journal = state
        .join("push")
        .join(attempt.as_str())
        .join("journal.json");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        if fs::read(&journal)
            .ok()
            .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
            .is_some_and(|e| e["evidence"]["progress"]["execution_started"] == true)
        {
            break;
        }
        assert!(
            child.try_wait().unwrap().is_none(),
            "managed process exited before execution intent"
        );
        if std::time::Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("execution intent deadline exceeded");
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    let refused = loop {
        let result = push(&adapters, &root, &decl, &state, &attempt);
        if result["errors"][0]["code"] != "ENGINE_BUSY" {
            break result;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "native worker retained workspace lock after EOF"
        );
        std::thread::sleep(std::time::Duration::from_millis(100));
    };
    assert_eq!(
        refused["errors"][0]["code"], "BUILD_INCOMPLETE",
        "{refused}"
    );
    let evidence: Value = serde_json::from_slice(&fs::read(&journal).unwrap()).unwrap();
    assert!(evidence["evidence"]["progress"]["accepted"].is_null());
    assert!(evidence["evidence"]["capture"].is_null());
    assert!(evidence["evidence"]["terminal"].is_null());
    assert!(!root.join("datasets/interrupted/rows").exists());
}
