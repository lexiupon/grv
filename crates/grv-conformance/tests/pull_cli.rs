#![cfg(feature = "native-duckdb")]
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
fn package(adapters: &Path, name: &str, executable: &str) {
    let package = adapters.join(name);
    std::fs::create_dir_all(&package).unwrap();
    // Test fixtures report 0.1.0; real adapters report the crate version.
    let v = if name == "fixture" {
        "0.1.0"
    } else {
        env!("CARGO_PKG_VERSION")
    };
    std::fs::write(package.join("adapter.toml"), format!("name = '{name}'\nversion = '{v}'\ninterface_versions = [1]\nbinding_schema_version = 1\nentrypoint = {executable:?}\n")).unwrap();
}
#[test]
fn cli_pulls_verified_published_files_and_replays_old_receipts_without_source_or_private_state() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let temp = tempfile::tempdir_in(repository).unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let adapters = temp.path().join("adapters");
    package(&adapters, "fixture", env!("CARGO_BIN_EXE_fixture"));
    package(&adapters, "duckdb", env!("CARGO_BIN_EXE_fixture-duckdb"));
    let root = temp.path().join("store");
    let state = temp.path().join("state");
    let push = temp.path().join("push.yml");
    std::fs::write(&push, "declaration_version: 1\nkind: push\ndataset: data\nadapter: fixture\nconnection: {}\ntables:\n  - name: rows\n    source: {}\n    columns: [{name: value, type: int64}]\n  - name: empty\n    source: {}\n    columns: [{name: value, type: int64}]\n").unwrap();
    let init = cli(&adapters, &["init", "--grv", root.to_str().unwrap()]);
    assert_eq!(init["ok"], true, "{init}");
    let result = cli(
        &adapters,
        &[
            "push",
            "--grv",
            root.to_str().unwrap(),
            "--decl",
            push.to_str().unwrap(),
            "--state",
            state.to_str().unwrap(),
        ],
    );
    assert_eq!(result["ok"], true, "{result}");
    let mut originals = Vec::new();
    for write in ["replace", "append"] {
        let database = temp.path().join(format!("{write}.duckdb"));
        let decl = temp.path().join(format!("{write}.yml"));
        let text = format!(
            "declaration_version: 1\nkind: pull\ndataset: data\nadapter: duckdb\nwrite: {write}\nconnection: {{database: {database:?}}}\ntarget: {{schema: app}}\noptions: {{materialization: local, refresh: auto}}\ntables:\n  - name: rows\n    target: {{table: copied_rows}}\n  - name: empty\n"
        );
        std::fs::write(&decl, &text).unwrap();
        let attempt = grv_types::Uuid::v4();
        let invoke = |id: &str| {
            cli(
                &adapters,
                &[
                    "pull",
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
        let first = invoke(attempt.as_str());
        assert_eq!(first["ok"], true, "{first}");
        assert_eq!(first["result"]["committed_revision"], "1");
        assert_eq!(first["result"]["replayed"], false);
        let evidence: Value = serde_json::from_slice(
            &std::fs::read(
                state
                    .join("pull")
                    .join(attempt.as_str())
                    .join("journal.json"),
            )
            .unwrap(),
        )
        .unwrap();
        let counts = evidence["evidence"]["terminal"]["row_counts"]
            .as_array()
            .unwrap();
        assert_eq!(
            counts.iter().find(|row| row["table"] == "rows").unwrap()["rows"],
            "3"
        );
        assert_eq!(
            counts.iter().find(|row| row["table"] == "empty").unwrap()["rows"],
            "0"
        );
        assert_eq!(
            evidence["evidence"]["progress"]["plan"]["files"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        // A newer successful application must not overwrite old attempt facts.
        let second = invoke(grv_types::Uuid::v4().as_str());
        assert_eq!(second["ok"], true, "{second}");
        assert_ne!(
            first["result"]["generation_id"],
            second["result"]["generation_id"]
        );
        // Source corruption is rejected before apply. The same uncommitted
        // attempt can proceed after the exact source bytes are restored.
        let version = root.join("datasets/data/rows/version=1");
        let manifest: Value =
            serde_json::from_slice(&std::fs::read(version.join("manifest.json")).unwrap()).unwrap();
        let file = version.join(manifest["data_files"][0]["name"].as_str().unwrap());
        let original_bytes = std::fs::read(&file).unwrap();
        let mut corrupted = original_bytes.clone();
        corrupted[4] ^= 1;
        std::fs::write(&file, corrupted).unwrap();
        let retry = grv_types::Uuid::v4();
        let rejected = invoke(retry.as_str());
        assert_eq!(
            rejected["errors"][0]["code"], "INTEGRITY_FAILURE",
            "{rejected}"
        );
        let pending: Value = serde_json::from_slice(
            &std::fs::read(state.join("pull").join(retry.as_str()).join("journal.json")).unwrap(),
        )
        .unwrap();
        assert!(pending["evidence"]["progress"]["plan"].is_null());
        assert!(pending["evidence"]["terminal"].is_null());
        std::fs::write(&file, original_bytes).unwrap();
        let recovered = invoke(retry.as_str());
        assert_eq!(recovered["ok"], true, "{recovered}");
        assert_eq!(recovered["result"]["replayed"], false);
        std::fs::write(&decl, text.replace("dataset: data", "dataset: different")).unwrap();
        let mismatch = invoke(attempt.as_str());
        assert_eq!(
            mismatch["errors"][0]["code"], "REQUEST_MISMATCH",
            "{mismatch}"
        );
        std::fs::write(&decl, &text).unwrap();
        let mut expected = first["result"].clone();
        std::fs::write(
            &decl,
            text.replace("    target: {table: copied_rows}\n", ""),
        )
        .unwrap();
        let changed_mapping = invoke(attempt.as_str());
        assert_eq!(
            changed_mapping["errors"][0]["code"], "REQUEST_MISMATCH",
            "{changed_mapping}"
        );
        std::fs::write(
            &decl,
            text.replace("options: {materialization: local, refresh: auto}\n", ""),
        )
        .unwrap();
        let defaulted = invoke(attempt.as_str());
        assert_eq!(defaulted["ok"], true, "{defaulted}");
        assert_eq!(
            defaulted["result"]["generation_id"],
            first["result"]["generation_id"]
        );
        assert_eq!(defaulted["result"]["replayed"], true);
        std::fs::write(&decl, &text).unwrap();
        expected["replayed"] = true.into();
        originals.push((database, decl, attempt, expected));
    }
    // Receipt lookup remains authoritative even without private journals or GRV.
    std::fs::remove_dir_all(&root).unwrap();
    std::fs::remove_dir_all(&state).unwrap();
    for (database, decl, attempt, expected) in originals {
        let invoke = || {
            cli(
                &adapters,
                &[
                    "pull",
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
        };
        let replay = invoke();
        assert_eq!(replay["ok"], true, "{replay}");
        assert_eq!(replay["result"], expected);
        assert!(!state.exists(), "terminal replay created execution state");
        // The persistent managed-evidence marker forbids treating deleted
        // destination history as a new workspace or executing again.
        std::fs::remove_file(database).unwrap();
        let lost = invoke();
        assert_eq!(lost["ok"], false, "{lost}");
        assert!(
            matches!(
                lost["errors"][0]["code"].as_str(),
                Some("OUTCOME_UNKNOWN" | "PROTOCOL_FAILURE")
            ),
            "{lost}"
        );
    }
}

#[test]
fn cli_revision_zero_requires_an_explicit_or_checked_prior_source_contract() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let temp = tempfile::tempdir_in(repository).unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let adapters = temp.path().join("adapters");
    package(&adapters, "duckdb", env!("CARGO_BIN_EXE_fixture-duckdb"));
    let root = temp.path().join("store");
    let state = temp.path().join("state");
    let database = temp.path().join("empty.duckdb");
    let decl = temp.path().join("pull.yml");
    let text = format!(
        "declaration_version: 1\nkind: pull\ndataset: data\nadapter: duckdb\nconnection: {{database: {database:?}}}\ntarget: {{schema: app}}\ntables:\n  - name: rows\n"
    );
    std::fs::write(&decl, &text).unwrap();
    assert_eq!(
        cli(&adapters, &["init", "--grv", root.to_str().unwrap()])["ok"],
        true
    );
    let invoke = |attempt: &str| {
        cli(
            &adapters,
            &[
                "pull",
                "--grv",
                root.to_str().unwrap(),
                "--decl",
                decl.to_str().unwrap(),
                "--state",
                state.to_str().unwrap(),
                "--attempt",
                attempt,
                "--revision",
                "0",
            ],
        )
    };
    let missing = invoke(grv_types::Uuid::v4().as_str());
    assert_eq!(
        missing["errors"][0]["code"], "INVALID_DECLARATION",
        "{missing}"
    );
    std::fs::write(
        &decl,
        format!("{text}    expect:\n      columns: [{{name: value, type: int64}}]\n"),
    )
    .unwrap();
    let first = invoke(grv_types::Uuid::v4().as_str());
    assert_eq!(first["ok"], true, "{first}");
    assert_eq!(first["result"]["requested_revision"], "0");
    assert_eq!(first["result"]["committed_revision"], "0");
    // The next attempt may use only the checked prior source contract returned
    // by Compare. No table baseline or file exists in this GRV root.
    std::fs::write(&decl, &text).unwrap();
    let second = invoke(grv_types::Uuid::v4().as_str());
    assert_eq!(second["ok"], true, "{second}");
    assert_eq!(
        second["result"]["workspace_id"],
        first["result"]["workspace_id"]
    );
    assert_ne!(
        second["result"]["generation_id"],
        first["result"]["generation_id"]
    );
}
