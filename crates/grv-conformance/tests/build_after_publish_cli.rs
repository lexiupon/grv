#![cfg(feature = "native-duckdb")]
use grv_adapter_api::{BuildIdentity, Resources};
use grv_adapter_duckdb::build::BuildStore;
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
    let value: Value = serde_json::from_slice(&output.stdout).unwrap_or_else(|_| {
        panic!(
            "build CLI returned no JSON; exit {:?}",
            output.status.code()
        )
    });
    validate_output(&value).unwrap();
    assert_eq!(output.status.success(), value["ok"] == true);
    value
}
fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}
fn count(directory: &Path, prefix: &str) -> usize {
    fs::read_dir(directory)
        .unwrap()
        .filter(|entry| {
            entry
                .as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains(prefix)
        })
        .count()
}

#[test]
fn known_build_outcome_pending_hook_replays_without_engine_declaration_or_grv() {
    let temp = tempfile::tempdir_in(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap(),
    )
    .unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let adapters = temp.path().join("adapters");
    let package = adapters.join("duckdb");
    let hooks = temp.path().join("hooks");
    fs::create_dir_all(&package).unwrap();
    fs::create_dir(&hooks).unwrap();
    fs::set_permissions(&hooks, fs::Permissions::from_mode(0o700)).unwrap();
    let executable = package.join("adapter");
    fs::write(
        &executable,
        format!(
            "#!/bin/sh\nexec {} --after-publish-dir {}\n",
            quote(env!("CARGO_BIN_EXE_fixture-duckdb-build")),
            quote(hooks.to_str().unwrap())
        ),
    )
    .unwrap();
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(package.join("adapter.toml"), format!("name='duckdb'\nversion={:?}\ninterface_versions=[1]\nbinding_schema_version=1\nentrypoint='adapter'\n", env!("CARGO_PKG_VERSION"))).unwrap();
    let root = temp.path().join("grv");
    let state = temp.path().join("state");
    let declaration = temp.path().join("build.yml");
    let engine = temp.path().join("engine.duckdb");
    let executions = temp.path().join("engine.duckdb.grv-fixture-executions");
    fs::write(&executions, []).unwrap();
    fs::write(&declaration, format!("declaration_version: 1\nkind: push\ndataset: derived\nadapter: duckdb\nconnection: {{database: {engine:?}}}\nbuild: {{execution: managed}}\ntables:\n  - name: rows\n    source: {{sql: 'SELECT 7::BIGINT AS value'}}\n    columns: [{{name: value, type: int64}}]\n")).unwrap();
    assert_eq!(
        cli(&adapters, &["init", "--grv", root.to_str().unwrap()])["ok"],
        true
    );
    let attempt = Uuid::v4();
    let push = || {
        cli(
            &adapters,
            &[
                "push",
                "--grv",
                root.to_str().unwrap(),
                "--decl",
                declaration.to_str().unwrap(),
                "--state",
                state.to_str().unwrap(),
                "--attempt",
                attempt.as_str(),
            ],
        )
    };
    let journal = state
        .join("push")
        .join(attempt.as_str())
        .join("journal.json");
    fs::write(hooks.join("fail"), "").unwrap();
    let failed = push();
    assert_eq!(failed["ok"], false, "{failed}");
    assert_eq!(failed["errors"][0]["code"], "ADAPTER_FAILURE", "{failed}");
    assert_eq!(failed["result"]["mode"], "build");
    assert_eq!(failed["result"]["outcome"]["kind"], "published");
    assert_eq!(failed["result"]["outcome"]["revision"], "1");
    let record: Value = serde_json::from_slice(&fs::read(&journal).unwrap()).unwrap();
    assert_eq!(record["evidence"]["progress"]["after_publish"], "pending");
    let identity: BuildIdentity =
        serde_json::from_value(record["evidence"]["intent"]["request"]["identity"].clone())
            .unwrap();
    let mut native = BuildStore::open(
        &engine,
        identity.root.clone(),
        Some(identity.workspace_id.clone()),
        &Resources::default(),
    )
    .unwrap();
    let accepted = native.open_session(&identity).unwrap();
    assert!(accepted.completion.is_some());
    assert!(accepted.completion_sha256.is_some());
    assert_eq!(
        serde_json::to_value(accepted.outcome).unwrap(),
        failed["result"]["outcome"]
    );
    drop(native);
    assert_eq!(fs::read(&executions).unwrap(), b"execute\n");
    assert_eq!(count(&hooks, ".call-"), 1, "{failed}");
    fs::remove_file(&engine).unwrap();
    fs::remove_file(&declaration).unwrap();
    fs::remove_dir_all(&root).unwrap();
    fs::remove_file(hooks.join("fail")).unwrap();
    fs::write(hooks.join("require_auth"), "").unwrap();
    let replay = push();
    assert_eq!(replay["ok"], true, "{replay}");
    assert_eq!(replay["result"]["outcome"], failed["result"]["outcome"]);
    assert_eq!(replay["result"]["replayed"], true);
    let record: Value = serde_json::from_slice(&fs::read(&journal).unwrap()).unwrap();
    assert_eq!(record["evidence"]["progress"]["after_publish"], "complete");
    assert_eq!(count(&hooks, ".call-"), 2);
    assert_eq!(count(&hooks, ".auth-"), 1);
    assert_eq!(fs::read(&executions).unwrap(), b"execute\n");
    assert!(!engine.exists());
    assert!(!root.exists());
    assert!(!declaration.exists());
    fs::remove_dir_all(&adapters).unwrap();
    let offline = push();
    assert_eq!(offline["ok"], true, "{offline}");
    assert_eq!(offline["result"]["outcome"], failed["result"]["outcome"]);
    assert_eq!(count(&hooks, ".call-"), 2);
}
