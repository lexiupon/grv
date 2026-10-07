use grv_adapter_host::protected_document::{canonical_path, publish, read};
use grv_types::ErrorCode;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt, symlink},
};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Context {
    identity: String,
    token: String,
    evidence: serde_json::Value,
}
fn context() -> Context {
    Context {
        identity: "fixed".into(),
        token: "private-token".into(),
        evidence: json!({"z":1,"a":true}),
    }
}

#[test]
fn canonical_immutable_publication_and_same_value_replay_are_protected_and_durable() {
    let dir = tempfile::tempdir().unwrap();
    let path = canonical_path(&dir.path().join("context.json"), &[]).unwrap();
    let value = context();
    publish(&path, &value, 4096).unwrap();
    assert_eq!(read::<Context>(&path, 4096).unwrap(), value);
    assert_eq!(fs::metadata(&path).unwrap().mode() & 0o7777, 0o600);
    let bytes = fs::read(&path).unwrap();
    assert_eq!(bytes, grv_types::canonical_json(&value).unwrap());
    publish(&path, &value, 4096).unwrap();
    let mut changed = context();
    changed.identity = "other-session".into();
    assert_eq!(
        publish(&path, &changed, 4096).unwrap_err().code,
        ErrorCode::RequestMismatch
    );
    assert_eq!(fs::read(&path).unwrap(), bytes);
    // Formatting/key ordering do not change the canonical identity.
    fs::write(&path, serde_json::to_string_pretty(&value).unwrap()).unwrap();
    publish(&path, &value, 4096).unwrap();
    assert_eq!(read::<Context>(&path, 4096).unwrap(), value);
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
}

#[test]
fn strict_closed_reader_refuses_duplicate_keys_unknowns_bad_utf8_trailing_values_and_sizes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("context.json");
    publish(&path, &context(), 4096).unwrap();
    for bytes in [
        br#"{"identity":"fixed","token":"private","evidence":{"x":1,"x":2}}"#.to_vec(),
        br#"{"identity":"fixed","token":"private","evidence":{},"unknown":true}"#.to_vec(),
        br#"{"identity":"fixed","token":"private","evidence":{}} {}"#.to_vec(),
        vec![255],
        vec![b' '; 4097],
    ] {
        fs::write(&path, bytes).unwrap();
        assert!(read::<Context>(&path, 4096).is_err());
    }
    assert!(publish(&dir.path().join("too-small.json"), &context(), 2).is_err());
    assert!(!dir.path().join("too-small.json").exists());
    assert!(read::<Context>(&path, 0).is_err());
}

#[test]
fn symlinks_hardlinks_permission_changes_and_unsafe_ancestors_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("context.json");
    publish(&path, &context(), 4096).unwrap();
    let alias = dir.path().join("alias.json");
    symlink(&path, &alias).unwrap();
    assert!(canonical_path(&alias, &[]).is_err());
    assert!(read::<Context>(&alias, 4096).is_err());
    assert!(publish(&alias, &context(), 4096).is_err());
    fs::remove_file(&alias).unwrap();
    fs::hard_link(&path, &alias).unwrap();
    assert!(read::<Context>(&path, 4096).is_err());
    assert!(publish(&path, &context(), 4096).is_err());
    fs::remove_file(&alias).unwrap();
    for mode in [0o644, 0o666, 0o700, 0o400] {
        fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
        assert!(read::<Context>(&path, 4096).is_err());
    }
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o777)).unwrap();
    assert!(read::<Context>(&path, 4096).is_err());
    assert!(publish(&dir.path().join("other.json"), &context(), 4096).is_err());
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
}

#[test]
fn excluded_roots_and_engine_files_cannot_be_bypassed_by_parent_aliases() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("grv");
    fs::create_dir(&root).unwrap();
    let alias = dir.path().join("alias");
    symlink(&root, &alias).unwrap();
    let engine = dir.path().join("engine.duckdb");
    fs::write(&engine, []).unwrap();
    assert!(canonical_path(&root.join("context.json"), &[&root, &engine]).is_err());
    assert!(canonical_path(&alias.join("context.json"), &[&root, &engine]).is_err());
    assert!(canonical_path(&engine, &[&root, &engine]).is_err());
    assert!(
        canonical_path(
            &dir.path().join("engine.duckdb-copy.json"),
            &[&root, &engine]
        )
        .is_ok()
    );
    assert!(canonical_path(&dir.path().join("outside.json"), &[&root, &engine]).is_ok());
    let absent_engine = dir.path().join("future.duckdb");
    assert!(canonical_path(&absent_engine, &[&absent_engine]).is_err());
}
