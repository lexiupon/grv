#![cfg(feature = "native-duckdb")]
//! Live gates use a unique child prefix. Only GRV's backend seeds/removes data;
//! the independently configured adapter and query probe perform S3 reads.
use grv_adapter_host::validate_output;
use grv_storage::{
    Backend, ListEntry, ListMode, ObjectKey, ObjectPrefix,
    cloud::{CloudBackend, CloudOptions},
};
use grv_types::Uuid;
use serde_json::{Value, json};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
};

struct Cleanup<'a>(&'a CloudBackend);
impl Drop for Cleanup<'_> {
    fn drop(&mut self) {
        if let Ok(objects) = self.0.list(
            &ObjectPrefix::new("datasets/").unwrap(),
            ListMode::Recursive,
        ) {
            for entry in objects {
                if let ListEntry::Object(key) = entry {
                    let _ = self.0.delete(&key);
                }
            }
        }
        let _ = self.0.delete(&ObjectKey::new("grv.json").unwrap());
    }
}
fn cli(adapters: &Path, args: &[&str], offline_home: Option<&Path>) -> Value {
    let mut command = Command::new(env!("CARGO_BIN_EXE_grv-conformance-cli"));
    command
        .arg("--json")
        .args(args)
        .env("GRV_ADAPTERS_DIR", adapters);
    if let Some(home) = offline_home {
        command
            .env("HOME", home)
            .env_remove("AWS_PROFILE")
            .env_remove("AWS_DEFAULT_PROFILE");
    }
    let output = command.output().unwrap();
    let value: Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|e| panic!("{e}: {}", String::from_utf8_lossy(&output.stderr)));
    validate_output(&value).unwrap();
    assert_eq!(output.status.success(), value["ok"] == true);
    value
}
fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
// An independent engine reader verifies persisted view behavior. It is confined
// to the harness and gets profile identifiers directly, never adapter secrets.
fn query(engine: &Path, setup: &[String], sql: &str) -> Result<Vec<i64>, ()> {
    use std::ffi::{CString, c_char, c_void};
    #[repr(C)]
    struct NativeResult {
        deprecated_column_count: u64,
        deprecated_row_count: u64,
        deprecated_rows_changed: u64,
        deprecated_columns: *mut c_void,
        deprecated_error_message: *mut c_char,
        internal_data: *mut c_void,
    }
    unsafe extern "C" {
        fn duckdb_open(path: *const c_char, database: *mut *mut c_void) -> u32;
        fn duckdb_connect(database: *mut c_void, connection: *mut *mut c_void) -> u32;
        fn duckdb_query(
            connection: *mut c_void,
            query: *const c_char,
            result: *mut NativeResult,
        ) -> u32;
        fn duckdb_row_count(result: *mut NativeResult) -> u64;
        fn duckdb_value_int64(result: *mut NativeResult, column: u64, row: u64) -> i64;
        fn duckdb_destroy_result(result: *mut NativeResult);
        fn duckdb_disconnect(connection: *mut *mut c_void);
        fn duckdb_close(database: *mut *mut c_void);
    }
    let path = CString::new(engine.to_str().unwrap()).unwrap();
    let mut database = std::ptr::null_mut();
    let mut connection = std::ptr::null_mut();
    unsafe {
        assert_eq!(duckdb_open(path.as_ptr(), &mut database), 0);
        grv_adapter_duckdb::native::load_static_extensions(database).unwrap();
        assert_eq!(duckdb_connect(database, &mut connection), 0);
        let mut answer = Ok(vec![]);
        for (index, sql) in setup.iter().map(String::as_str).chain([sql]).enumerate() {
            let text = CString::new(sql).unwrap();
            let mut result: NativeResult = std::mem::zeroed();
            let status = duckdb_query(connection, text.as_ptr(), &mut result);
            if status != 0 {
                // Print only fixed classifications; provider text can contain
                // credential identifiers, object locations or signed URLs.
                let error = if result.deprecated_error_message.is_null() {
                    std::borrow::Cow::Borrowed("")
                } else {
                    std::ffi::CStr::from_ptr(result.deprecated_error_message).to_string_lossy()
                };
                let categories: Vec<_> = [
                    "404",
                    "403",
                    "No files",
                    "glob",
                    "Secret",
                    "credential",
                    "HTTP",
                    "IO Error",
                    "Invalid",
                    "Binder",
                    "Catalog",
                    "extension",
                ]
                .into_iter()
                .filter(|category| error.contains(category))
                .collect();
                eprintln!("query probe failed at statement {index}: {categories:?}");
                answer = Err(());
            } else if index == setup.len() {
                answer = Ok((0..duckdb_row_count(&mut result))
                    .map(|row| duckdb_value_int64(&mut result, 0, row))
                    .collect());
            }
            duckdb_destroy_result(&mut result);
            if answer.is_err() {
                break;
            }
        }
        duckdb_disconnect(&mut connection);
        duckdb_close(&mut database);
        answer
    }
}
fn reader_setup(root: &str, profile: &str, region: &str, extensions: &Path) -> Vec<String> {
    let scope = grv_adapter_duckdb::s3_config::sql_filename(root).unwrap();
    vec![
        "SET autoinstall_known_extensions=false".into(),
        "SET autoload_known_extensions=false".into(),
        "SET enable_logging=false".into(),
        format!(
            "LOAD {}",
            literal(extensions.join("httpfs.duckdb_extension").to_str().unwrap())
        ),
        format!(
            "LOAD {}",
            literal(extensions.join("aws.duckdb_extension").to_str().unwrap())
        ),
        "SET s3_url_compatibility_mode=true".into(),
        "SET enable_http_metadata_cache=false".into(),
        "SET enable_external_file_cache=false".into(),
        format!(
            "CREATE SECRET grv_probe (TYPE S3, PROVIDER credential_chain, PROFILE {}, REGION {}, SCOPE {}, URL_COMPATIBILITY_MODE true)",
            literal(profile),
            literal(region),
            literal(&scope)
        ),
    ]
}

#[test]
#[ignore = "requires a dedicated writable GRV S3 prefix, independent adapter reader and pinned extensions"]
fn live_s3_views_multifile_refresh_atomic_checks_and_source_free_receipts() {
    let parent = std::env::var("GRV_S3_TEST_ROOT").expect("dedicated test root required");
    let profile =
        std::env::var("GRV_DUCKDB_S3_READ_PROFILE").expect("independent reader profile required");
    let region = std::env::var("AWS_REGION").expect("explicit region required");
    let extensions = PathBuf::from(
        std::env::var("GRV_DUCKDB_EXTENSIONS_DIR").expect("pinned local extensions required"),
    );
    let root = format!(
        "{}/s3-view-{}/space%20and%25",
        parent.trim_end_matches('/'),
        Uuid::v4()
    );
    assert!(root.starts_with("s3://"));
    eprintln!("S3 view conformance root: {root}");
    let backend = CloudBackend::open(&root, CloudOptions::default()).unwrap();
    let _cleanup = Cleanup(&backend);
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let temp = tempfile::tempdir_in(repository).unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let adapters = temp.path().join("adapters");
    let package = adapters.join("duckdb");
    fs::create_dir_all(&package).unwrap();
    fs::write(package.join("adapter.toml"), format!("name='duckdb'\nversion='0.1.0'\ninterface_versions=[1]\nbinding_schema_version=1\nentrypoint={:?}\n", env!("CARGO_BIN_EXE_fixture-duckdb-build"))).unwrap();
    let state = temp.path().join("state");
    let push = temp.path().join("push.yml");
    let pull = temp.path().join("pull.yml");
    let source_engine = temp.path().join("source.duckdb");
    let engine = temp.path().join("views.duckdb");
    let config = temp.path().join("views.duckdb.grv-s3-read.json");
    fs::write(&config, serde_json::to_vec(&json!({"config_version":1,"readers":[{"scope":parent.trim_end_matches('/'),"profile":profile,"region":region}]})).unwrap()).unwrap();
    fs::set_permissions(&config, fs::Permissions::from_mode(0o600)).unwrap();
    let setup = reader_setup(&root, &profile, &region, &extensions);
    assert_eq!(cli(&adapters, &["init", "--grv", &root], None)["ok"], true);
    let publish = |rows: &str, risky: &str| {
        fs::write(&push, format!("declaration_version: 1\nkind: push\ndataset: data\nadapter: duckdb\nconnection: {{database: {source_engine:?}}}\nbuild: {{execution: managed}}\ntables:\n  - name: rows\n    source: {{sql: {rows:?}}}\n    columns: [{{name: id, type: int64}}]\n  - name: empty\n    source: {{sql: 'SELECT false AS flag WHERE false'}}\n    columns: [{{name: flag, type: bool}}]\n  - name: risky\n    source: {{sql: {risky:?}}}\n    columns: [{{name: id, type: int64}}]\n")).unwrap();
        let result = cli(
            &adapters,
            &[
                "push",
                "--grv",
                &root,
                "--decl",
                push.to_str().unwrap(),
                "--state",
                state.to_str().unwrap(),
            ],
            None,
        );
        assert_eq!(result["ok"], true, "{result}");
        result
    };
    assert_eq!(
        publish(
            "SELECT i::BIGINT AS id FROM range(10000) t(i)",
            "SELECT 900::BIGINT AS id"
        )["result"]["outcome"]["revision"],
        "1"
    );
    let mut bytes = vec![];
    backend
        .get(
            &ObjectKey::new("datasets/data/rows/version=1/manifest.json").unwrap(),
            &mut bytes,
        )
        .unwrap();
    let manifest: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(
        manifest["data_files"].as_array().unwrap().len() >= 3,
        "multi-file gate did not seed multiple files"
    );
    let declaration = format!(
        "declaration_version: 1\nkind: pull\ndataset: data\nadapter: duckdb\nwrite: replace\nconnection: {{database: {engine:?}}}\ntarget: {{schema: app}}\noptions: {{materialization: s3-view, refresh: auto}}\ntables:\n  - name: rows\n  - name: empty\n  - name: risky\n"
    );
    fs::write(&pull, &declaration).unwrap();
    let invoke = |attempt: &Uuid, home| {
        cli(
            &adapters,
            &[
                "pull",
                "--grv",
                &root,
                "--decl",
                pull.to_str().unwrap(),
                "--state",
                state.to_str().unwrap(),
                "--attempt",
                attempt.as_str(),
            ],
            home,
        )
    };
    let attempt = Uuid::v4();
    let first = invoke(&attempt, None);
    assert_eq!(first["ok"], true, "{first}");
    assert_eq!(first["result"]["committed_revision"], "1");
    // Opening the guarded read-only owner also links this independent C-API
    // probe against the same pinned guard artifact used by the adapter process.
    drop(grv_adapter_duckdb::native::NativeEngine::open_readonly(&engine).unwrap());
    assert_eq!(query(&engine, &[], "SELECT count(*) FROM duckdb_views() WHERE schema_name='app' AND view_name IN ('rows','empty','risky')").unwrap(), [3]);
    assert_eq!(
        query(&engine, &setup, "SELECT count(*) FROM app.rows").unwrap(),
        [10000]
    );
    assert_eq!(
        query(&engine, &setup, "SELECT sum(id) FROM app.rows").unwrap(),
        [49_995_000]
    );
    assert_eq!(
        query(&engine, &setup, "SELECT count(*) FROM app.empty").unwrap(),
        [0]
    );
    assert_eq!(query(&engine, &[], "SELECT count(*) FROM duckdb_views() WHERE schema_name='app' AND view_name='rows' AND contains(sql,'s3://') AND NOT contains(sql,'/dev/fd/')").unwrap(), [1]);
    assert_eq!(
        publish(
            "SELECT (100+i)::BIGINT AS id FROM range(5) t(i)",
            "SELECT NULL::BIGINT AS id"
        )["result"]["outcome"]["revision"],
        "2"
    );
    fs::write(
        &pull,
        format!("{declaration}checks:\n  - {{table: risky, not_null: [id]}}\n"),
    )
    .unwrap();
    let rejected = invoke(&Uuid::v4(), None);
    assert_eq!(rejected["ok"], false, "{rejected}");
    assert_eq!(
        query(&engine, &setup, "SELECT count(*) FROM app.rows").unwrap(),
        [10000]
    );
    assert_eq!(
        query(&engine, &setup, "SELECT id FROM app.risky").unwrap(),
        [900]
    );
    // Every relation and current generation survive a late failing table check.
    let mut expected = first["result"].clone();
    expected["replayed"] = true.into();
    fs::write(&pull, &declaration).unwrap();
    assert_eq!(invoke(&attempt, None)["result"], expected);
    let refresh = invoke(&Uuid::v4(), None);
    assert_eq!(refresh["ok"], true, "{refresh}");
    assert_eq!(refresh["result"]["committed_revision"], "2");
    assert_ne!(
        refresh["result"]["generation_id"],
        first["result"]["generation_id"]
    );
    assert_eq!(
        query(&engine, &setup, "SELECT sum(id) FROM app.rows").unwrap(),
        [510]
    );
    // Pruning source objects makes the persistent views unavailable. Immutable
    // old receipts remain authoritative without auth, source or private state.
    for entry in backend
        .list(
            &ObjectPrefix::new("datasets/").unwrap(),
            ListMode::Recursive,
        )
        .unwrap()
    {
        if let ListEntry::Object(key) = entry {
            backend.delete(&key).unwrap();
        }
    }
    backend
        .delete(&ObjectKey::new("grv.json").unwrap())
        .unwrap();
    assert!(query(&engine, &setup, "SELECT count(*) FROM app.rows").is_err());
    fs::remove_file(&config).unwrap();
    fs::remove_dir_all(&state).unwrap();
    let offline = temp.path().join("offline-home");
    fs::create_dir(&offline).unwrap();
    let replay = invoke(&attempt, Some(&offline));
    assert_eq!(replay["ok"], true, "{replay}");
    assert_eq!(replay["result"], expected);
    assert!(!state.exists(), "receipt replay created execution state");
    fs::remove_file(&engine).unwrap();
    let lost = invoke(&attempt, Some(&offline));
    assert_eq!(lost["ok"], false, "{lost}");
    assert!(
        matches!(
            lost["errors"][0]["code"].as_str(),
            Some("OUTCOME_UNKNOWN" | "PROTOCOL_FAILURE")
        ),
        "{lost}"
    );
}

#[test]
#[ignore = "requires a dedicated writable S3 prefix, named read profile and pinned extensions"]
fn live_s3_native_reader_exact_uri_validator_hash_fallback_and_changed_hash_refusal() {
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use grv_adapter_api::{Column, FileAccess, FileSchema, TableContract, VerifiedFile};
    use grv_adapter_duckdb::{pull as p, s3_config::S3Reader};
    use grv_types::{Digest, LatestRevision, Name, RequestedRevision, U64};
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;
    let parent = std::env::var("GRV_S3_TEST_ROOT").unwrap();
    let root = format!("{}/s3-read-{}", parent.trim_end_matches('/'), Uuid::v4());
    let backend = CloudBackend::open(&root, CloudOptions::default()).unwrap();
    let _cleanup = Cleanup(&backend);
    let key =
        ObjectKey::new("datasets/probe/rows/version=1/owner@example space%/data.parquet").unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let mut bytes = vec![];
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let validator = backend.create_bytes(&key, &bytes).unwrap();
    let contract = TableContract {
        columns: vec![Column {
            name: "id".into(),
            logical_type: json!("int64"),
        }],
        partition_keys: vec![],
        extensions: json!({}),
        column_ext: json!({}),
    };
    let remote = VerifiedFile {
        table: Name::new("rows").unwrap(),
        partition: json!({}),
        version: U64::new(1).unwrap(),
        schema: FileSchema {
            columns: contract.columns.clone(),
        },
        access: FileAccess::S3View,
        location: backend.s3_data_uri(&key).unwrap().unwrap(),
        size: U64::new(bytes.len() as u64).unwrap(),
        sha256: grv_types::sha256(&bytes),
        validator: validator.as_str().into(),
    };
    remote.validate().unwrap();
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let temp = tempfile::tempdir_in(repository).unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let engine = temp.path().join("reader.duckdb");
    let reader = S3Reader {
        scope: root.clone(),
        profile: std::env::var("GRV_DUCKDB_S3_READ_PROFILE").unwrap(),
        region: std::env::var("AWS_REGION").unwrap(),
    };
    let plan = p::PullPlan {
        identity: p::PullIdentity { binding: p::WorkspaceBinding { canonical_root: root.clone(), workspace_id: Uuid::v4() }, attempt_id: Uuid::v4(), request_sha256: Digest::new("a".repeat(64)).unwrap(), adapter_identity: serde_json::from_value(json!({"name":"duckdb","package_version":"0.1.0","interface_version":1,"binding_schema_version":1})).unwrap(), dataset: Name::new("probe").unwrap(), target_schema: p::RelationName::new("app").unwrap(), write_mode: p::WriteMode::Replace, scope_mode: p::ScopeMode::Complete, transform_mode: p::TransformMode::Identity, requested_revision: RequestedRevision::Latest(LatestRevision::Latest) },
        committed_revision: U64::new(1).unwrap(), generation_id: Uuid::v4(), request_record: None, selected_partitions: None, materialization_mode: p::MaterializationMode::S3View,
        tables: vec![p::PullTable { name: remote.table.clone(), target_table: p::RelationName::new("rows").unwrap(), source_contract: contract.clone(), output_contract: contract.clone(), files: vec![p::SourceFile { path: PathBuf::new(), bytes: remote.size, sha256: remote.sha256.clone(), contract, partition: remote.partition.clone(), remote: Some(remote) }], sql: None, not_null: vec![] }],
    };
    for fallback in [false, true] {
        let mut current = plan.clone();
        current.identity.attempt_id = Uuid::v4();
        current.identity.request_sha256 =
            grv_types::sha256(if fallback { b"fallback" } else { b"normal" });
        current.generation_id = Uuid::v4();
        if fallback {
            // A prior observation can legitimately have another opaque validator;
            // the native owner must establish the same complete byte identity.
            current.tables[0].files[0]
                .remote
                .as_mut()
                .unwrap()
                .validator = "prior-validator".into();
        }
        let mut store = p::PullStore::open(&engine).unwrap();
        store.configure_s3_reader(&reader).unwrap();
        let receipt = store
            .apply_identity(&current)
            .unwrap_or_else(|e| panic!("read branch fallback={fallback}: {e:?}"));
        assert_eq!(receipt.table_counts, json!({"rows":"3"}));
        drop(store);
        let extensions = PathBuf::from(std::env::var("GRV_DUCKDB_EXTENSIONS_DIR").unwrap());
        let setup = reader_setup(&root, &reader.profile, &reader.region, &extensions);
        assert_eq!(
            query(&engine, &setup, "SELECT count(*) FROM app.rows").unwrap(),
            [3]
        );
    }
    let mut changed = plan.clone();
    changed.identity.attempt_id = Uuid::v4();
    changed.identity.request_sha256 = grv_types::sha256(b"changed");
    changed.generation_id = Uuid::v4();
    let wrong = Digest::new("b".repeat(64)).unwrap();
    changed.tables[0].files[0].sha256 = wrong.clone();
    let remote = changed.tables[0].files[0].remote.as_mut().unwrap();
    remote.sha256 = wrong;
    remote.validator = "prior-validator".into();
    let mut store = p::PullStore::open(&engine).unwrap();
    store.configure_s3_reader(&reader).unwrap();
    assert!(store.apply_identity(&changed).is_err());
    assert!(matches!(
        store.resolve(&changed.identity).unwrap(),
        p::PullResolution::NotCommitted
    ));
}
