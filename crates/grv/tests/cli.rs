use serde_json::Value;
use std::{path::Path, process::Command};
fn protected() -> tempfile::TempDir {
    tempfile::tempdir_in(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap(),
    )
    .unwrap()
}
#[test]
fn cli_manifest_only_list_override_and_unknown_command_envelopes() {
    let temp = protected();
    let root = temp.path().join("override");
    std::fs::create_dir_all(&root).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_grv"))
        .args(["--json", "adapter", "list"])
        .env("GRV_ADAPTERS_DIR", &root)
        .env("XDG_CONFIG_HOME", temp.path().join("unused"))
        .output()
        .unwrap();
    assert!(output.status.success());
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    grv_adapter_host::validate_output(&value).unwrap();
    assert_eq!(value["result"]["search_roots"], serde_json::json!([root]));
    assert_eq!(value["result"]["adapters"], serde_json::json!([]));
    assert!(value["root"].is_null());
    let output = Command::new(env!("CARGO_BIN_EXE_grv"))
        .args(["--json", "unknown"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    grv_adapter_host::validate_output(&value).unwrap();
    assert_eq!(value["errors"][0]["code"], "INVALID_ARGUMENT");
}
#[test]
fn cli_local_init_canonical_root_idempotence_parameters_and_damage_checks() {
    let temp = protected();
    let root = temp.path().join("root");
    let run = |flags: &[&str]| {
        let output = Command::new(env!("CARGO_BIN_EXE_grv"))
            .args(["--json", "init", "--grv"])
            .arg(&root)
            .args(flags)
            .output()
            .unwrap();
        let value: Value = serde_json::from_slice(&output.stdout).unwrap();
        grv_adapter_host::validate_output(&value).unwrap();
        (output, value)
    };
    let (output, value) = run(&[]);
    assert!(output.status.success());
    assert_eq!(
        value["root"],
        std::fs::canonicalize(&root).unwrap().to_str().unwrap()
    );
    assert_eq!(value["result"]["created"], true);
    assert_eq!(value["result"]["format_version"], 2);
    assert_eq!(
        value["result"]["parameters"],
        serde_json::json!({"max_clock_skew":30,"max_lease_ttl":900,"pending_grace":604800})
    );
    let (output, value) = run(&[]);
    assert!(output.status.success());
    assert_eq!(value["result"]["created"], false);
    let (output, value) = run(&["--max-clock-skew", "31"]);
    assert_eq!(output.status.code(), Some(3));
    assert_eq!(value["errors"][0]["code"], "STATE_CONFLICT");
    std::fs::remove_file(root.join("grv.json")).unwrap();
    std::fs::create_dir_all(root.join("datasets/data")).unwrap();
    std::fs::write(root.join("datasets/data/orphan"), b"orphan").unwrap();
    let (output, value) = run(&[]);
    assert_eq!(output.status.code(), Some(5));
    assert_eq!(value["errors"][0]["code"], "INTEGRITY_FAILURE");
    assert!(!root.join("grv.json").exists());
}
