#![cfg(feature = "native-duckdb")]
//! Live external-build recovery gate. Every backend write is confined to one
//! exact child root supplied by the parent orchestrator. The parent owns bucket
//! admission (labeled, private) and bucket cleanup; this test only deletes backend
//! objects inside that child. The external driver independently acquires GCS OAuth.
use arrow_array::Array;
use grv_adapter_api::{BuildSession, CompletedOutput, CompletionKind, Resources};
use grv_adapter_duckdb::{build::BuildStore, driver::ExternalInvocation};
use grv_adapter_host::{protected_document, validate_output};
use grv_core::clock::{Clock, SystemClock};
use grv_storage::{
    Backend, ErrorKind, ListEntry, ListMode, ObjectKey, ObjectPrefix,
    cloud::{CloudBackend, CloudOptions},
    model::{RunControl, RunPhase, decode_record},
};
use grv_types::Uuid;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    ffi::{CString, c_char, c_void},
    fs,
    io::Write,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};
const LIMIT: usize = 64 * 1024 * 1024;
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
fn cli(adapters: &Path, args: &[&str], offline: Option<&Path>) -> Value {
    let mut command = Command::new(env!("CARGO_BIN_EXE_grv-conformance-cli"));
    command
        .arg("--json")
        .args(args)
        .env("GRV_ADAPTERS_DIR", adapters)
        // This capability opt-in belongs only to the conformance fixture binary.
        .env("GRV_CONFORMANCE_GCS_VIEW", "yes")
        .env(
            "GRV_GCS_ACCOUNT",
            std::env::var("GRV_GCS_ACCOUNT").expect("explicit GCS account required"),
        )
        .env(
            "GRV_GCS_PROJECT",
            std::env::var("GRV_GCS_PROJECT").expect("explicit GCS project required"),
        );
    if let Some(home) = offline {
        command
            .env("HOME", home)
            .env_remove("GRV_GCS_ACCOUNT")
            .env_remove("GRV_GCS_PROJECT")
            .env_remove("CLOUDSDK_CONFIG");
    }
    let output = command.output().unwrap();
    let value: Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|_| panic!("GRV did not produce a JSON envelope"));
    validate_output(&value).unwrap();
    assert_eq!(output.status.success(), value["ok"] == true);
    value
}
fn record(path: &Path) -> Value {
    protected_document::read(path, LIMIT as u64).unwrap()
}
fn object(backend: &impl Backend, path: &str) -> Value {
    serde_json::from_slice(
        &backend
            .read_bytes(&ObjectKey::new(path).unwrap(), LIMIT)
            .unwrap()
            .0,
    )
    .unwrap()
}
fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
fn relation(value: &str) -> String {
    let (schema, table) = value.split_once('.').unwrap();
    format!(
        "\"{}\".\"{}\"",
        schema.replace('"', "\"\""),
        table.replace('"', "\"\"")
    )
}
#[repr(C)]
struct NativeResult {
    columns: u64,
    rows: u64,
    changed: u64,
    data: *mut c_void,
    error: *mut c_char,
    internal: *mut c_void,
}
unsafe extern "C" {
    fn duckdb_open(path: *const c_char, database: *mut *mut c_void) -> u32;
    fn duckdb_connect(database: *mut c_void, connection: *mut *mut c_void) -> u32;
    fn duckdb_query(connection: *mut c_void, sql: *const c_char, result: *mut NativeResult) -> u32;
    fn duckdb_row_count(result: *mut NativeResult) -> u64;
    fn duckdb_value_int64(result: *mut NativeResult, column: u64, row: u64) -> i64;
    fn duckdb_destroy_result(result: *mut NativeResult);
    fn duckdb_disconnect(connection: *mut *mut c_void);
    fn duckdb_close(database: *mut *mut c_void);
}
struct Engine {
    database: *mut c_void,
    connection: *mut c_void,
    token: Option<grv_adapter_duckdb::gcs_token::AccessToken>,
}
impl Engine {
    fn open(path: &Path) -> Self {
        let mut engine = Self {
            database: std::ptr::null_mut(),
            connection: std::ptr::null_mut(),
            token: None,
        };
        unsafe {
            assert_eq!(
                duckdb_open(
                    CString::new(path.to_str().unwrap()).unwrap().as_ptr(),
                    &mut engine.database
                ),
                0
            );
            grv_adapter_duckdb::native::load_static_extensions(engine.database).unwrap();
            assert_eq!(duckdb_connect(engine.database, &mut engine.connection), 0);
        }
        engine
    }
    fn query(&self, sql: &str) -> Result<Vec<i64>, ()> {
        if let Some(token) = &self.token {
            token.ensure_live().map_err(|_| ())?;
        }
        // Never expose SQL via NulError or a DuckDB setup/query diagnostic.
        let sql = CString::new(sql).map_err(|_| ())?;
        unsafe {
            let mut result: NativeResult = std::mem::zeroed();
            let status = duckdb_query(self.connection, sql.as_ptr(), &mut result);
            let answer = if status == 0 {
                Ok((0..duckdb_row_count(&mut result))
                    .map(|row| duckdb_value_int64(&mut result, 0, row))
                    .collect())
            } else {
                // DuckDB can include the secret-bearing setup SQL in errors.
                // Destroy the result without reading or printing provider text.
                Err(())
            };
            duckdb_destroy_result(&mut result);
            answer
        }
    }
}
impl Drop for Engine {
    fn drop(&mut self) {
        unsafe {
            duckdb_disconnect(&mut self.connection);
            duckdb_close(&mut self.database);
        }
    }
}
fn setup(
    engine: &mut Engine,
    root: &str,
    account: &str,
    project: &str,
    extensions: &Path,
) -> Result<(), ()> {
    let scope = grv_adapter_duckdb::gcs_config::sql_filename(root).map_err(|_| ())?;
    // Explicit selectors and expiry are validated from the same projected helper
    // response. No parent backend token, token argument/env or document is used.
    engine.token = Some(
        grv_adapter_duckdb::gcs_token::acquire(
            account,
            project,
            std::time::Duration::from_secs(300),
        )
        .map_err(|_| ())?,
    );
    for sql in [
        "SET autoinstall_known_extensions=false".into(),
        "SET autoload_known_extensions=false".into(),
        "SET enable_logging=false".into(),
        "SET allow_persistent_secrets=false".into(),
        format!(
            "LOAD {}",
            literal(extensions.join("httpfs.duckdb_extension").to_str().unwrap())
        ),
        // Signed, pinned stock HTTPFS on an ordinary C API consumer connection:
        // deliberately no GRV exact-file/connection guard is installed here.
        "SET enable_http_metadata_cache=false".into(),
        "SET enable_external_file_cache=false".into(),
    ] {
        engine.query(&sql)?;
    }
    engine.query(&format!(
        "CREATE TEMPORARY SECRET grv_external_probe (TYPE HTTP, PROVIDER config, BEARER_TOKEN {}, SCOPE {}, EXTRA_HTTP_HEADERS MAP {{'x-goog-user-project': {}}})",
        literal(engine.token.as_ref().ok_or(())?.bearer().map_err(|_| ())?),
        literal(&format!("{scope}/")),
        literal(project),
    ))?;
    Ok(())
}
fn mapping<'a>(session: &'a BuildSession, kind: &str, name: &str) -> &'a str {
    let key = if kind == "input" { "alias" } else { "table" };
    session.adapter_details[format!("{kind}_mappings")]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v[key] == name)
        .unwrap()["engine_table"]
        .as_str()
        .unwrap()
}
fn assert_catalog_kind(engine: &Engine, table: &str, view: bool) {
    let (schema, name) = table.split_once('.').unwrap();
    let (catalog, field) = if view {
        ("duckdb_views()", "view_name")
    } else {
        ("duckdb_tables()", "table_name")
    };
    assert_eq!(
        engine
            .query(&format!(
                "SELECT count(*)::BIGINT FROM {catalog} WHERE schema_name={} AND {field}={}",
                literal(schema),
                literal(name)
            ))
            .unwrap(),
        [1]
    );
}
// The child retains the inherited descriptor until its C API connections close.
// Only identifiers/configuration are passed in its protected document; no
// credentials or GRV backend ownership tokens cross this driver boundary.
#[test]
fn external_gcs_build_driver() {
    let Some(path) = std::env::var_os("GRV_GCS_BUILD_DRIVER") else {
        return;
    };
    let config = record(Path::new(&path));
    let session: BuildSession = serde_json::from_value(config["session"].clone()).unwrap();
    let mut engine = Engine::open(Path::new(config["database"].as_str().unwrap()));
    setup(
        &mut engine,
        config["root"].as_str().unwrap(),
        config["account"].as_str().unwrap(),
        config["project"].as_str().unwrap(),
        Path::new(config["extensions"].as_str().unwrap()),
    )
    .unwrap();
    for value in session.adapter_details["input_mappings"]
        .as_array()
        .unwrap()
        .iter()
        .chain(session.adapter_details["self_mappings"].as_array().unwrap())
    {
        assert_catalog_kind(&engine, value["engine_table"].as_str().unwrap(), true);
    }
    for output in &session.outputs {
        assert_catalog_kind(&engine, &output.engine_table, false);
    }
    let rows = relation(mapping(&session, "input", "numbers"));
    let small = relation(mapping(&session, "input", "local_numbers"));
    let empty = relation(mapping(&session, "input", "blanks"));
    let own = relation(mapping(&session, "self", "total"));
    let own_empty = relation(mapping(&session, "self", "empty"));
    assert_eq!(
        engine
            .query(&format!("SELECT sum(id)::BIGINT FROM {rows}"))
            .unwrap(),
        [49_995_000]
    );
    assert_eq!(
        engine
            .query(&format!("SELECT sum(id)::BIGINT FROM {small}"))
            .unwrap(),
        [24]
    );
    assert_eq!(
        engine
            .query(&format!("SELECT sum(id)::BIGINT FROM {own}"))
            .unwrap(),
        [7]
    );
    assert_eq!(
        engine
            .query(&format!("SELECT count(*)::BIGINT FROM {empty}"))
            .unwrap(),
        [0]
    );
    assert_eq!(
        engine
            .query(&format!("SELECT count(*)::BIGINT FROM {own_empty}"))
            .unwrap(),
        [0]
    );
    // Working tracking generations changed after prepare; the private readers
    // retain the previously held verified revision, including locally pulled
    // input data represented as GCS views in an external GCS build.
    assert_eq!(
        engine
            .query("SELECT sum(id)::BIGINT FROM remote.rows")
            .unwrap(),
        [1506]
    );
    assert_eq!(
        engine
            .query("SELECT sum(id)::BIGINT FROM local.small")
            .unwrap(),
        [184]
    );
    let total = relation(
        &session
            .outputs
            .iter()
            .find(|o| o.table.as_str() == "total")
            .unwrap()
            .engine_table,
    );
    let output_empty = relation(
        &session
            .outputs
            .iter()
            .find(|o| o.table.as_str() == "empty")
            .unwrap()
            .engine_table,
    );
    engine.query("BEGIN TRANSACTION").unwrap();
    engine.query(&format!("DELETE FROM {total}; INSERT INTO {total}(id) SELECT (SELECT sum(id) FROM {rows})+(SELECT sum(id) FROM {small})+(SELECT sum(id) FROM {own}); DELETE FROM {output_empty}; INSERT INTO {output_empty}(flag) SELECT flag FROM {empty}")).unwrap();
    engine.query("COMMIT").unwrap();
    assert_eq!(
        engine.query(&format!("SELECT id FROM {total}")).unwrap(),
        [49_995_031]
    );
    assert_eq!(
        engine
            .query(&format!("SELECT count(*)::BIGINT FROM {output_empty}"))
            .unwrap(),
        [0]
    );
}

fn seed(
    adapters: &Path,
    root: &str,
    state: &Path,
    decl: &Path,
    engine: &Path,
    dataset: &str,
    sources: (&str, &str),
) -> Value {
    let (rows, small) = sources;
    let tables = if dataset == "upstream" {
        format!(
            "  - name: rows\n    source: {{sql: {rows:?}}}\n    columns: [{{name: id, type: int64}}]\n  - name: small\n    source: {{sql: {small:?}}}\n    columns: [{{name: id, type: int64}}]\n  - name: empty\n    source: {{sql: 'SELECT false AS flag WHERE false'}}\n    columns: [{{name: flag, type: bool}}]\n"
        )
    } else {
        "  - name: total\n    source: {sql: 'SELECT 7::BIGINT AS id'}\n    columns: [{name: id, type: int64}]\n  - name: empty\n    source: {sql: 'SELECT false AS flag WHERE false'}\n    columns: [{name: flag, type: bool}]\n".into()
    };
    fs::write(decl,format!("declaration_version: 1\nkind: push\ndataset: {dataset}\nadapter: duckdb\nconnection: {{database: {engine:?}}}\nbuild: {{execution: managed}}\ntables:\n{tables}")).unwrap();
    cli(
        adapters,
        &[
            "push",
            "--grv",
            root,
            "--decl",
            decl.to_str().unwrap(),
            "--state",
            state.to_str().unwrap(),
        ],
        None,
    )
}
fn pull(
    adapters: &Path,
    root: &str,
    state: &Path,
    decl: &Path,
    engine: &Path,
    remote: bool,
) -> Value {
    let (namespace, tables, options) = if remote {
        (
            "remote",
            "[{name: rows}, {name: empty}]",
            "options: {materialization: gcs-view, refresh: auto}\n",
        )
    } else {
        ("local", "[{name: small}]", "")
    };
    fs::write(decl,format!("declaration_version: 1\nkind: pull\ndataset: upstream\nadapter: duckdb\nconnection: {{database: {engine:?}}}\ntarget: {{schema: {namespace}}}\n{options}tables: {tables}\n")).unwrap();
    cli(
        adapters,
        &[
            "pull",
            "--grv",
            root,
            "--decl",
            decl.to_str().unwrap(),
            "--state",
            state.to_str().unwrap(),
        ],
        None,
    )
}
fn output_values(backend: &impl Backend, parent: &Path, path: &str) -> Vec<i64> {
    let manifest = object(backend, &format!("{path}/manifest.json"));
    let mut values = vec![];
    for file in manifest["data_files"].as_array().unwrap() {
        let mut staged = tempfile::NamedTempFile::new_in(parent).unwrap();
        backend
            .get(
                &ObjectKey::new(format!("{path}/{}", file["name"].as_str().unwrap())).unwrap(),
                staged.as_file_mut(),
            )
            .unwrap();
        staged.flush().unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            staged.reopen().unwrap(),
        )
        .unwrap()
        .build()
        .unwrap();
        for batch in reader {
            let batch = batch.unwrap();
            let numbers = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap();
            assert_eq!(numbers.null_count(), 0);
            values.extend_from_slice(numbers.values());
        }
    }
    values
}

#[test]
#[ignore = "requires parent-owned private GCS bucket child, explicit account/project and pinned extensions"]
fn live_external_gcs_build_fixed_view_inputs_self_base_accepted_export_restart_and_terminal_replay()
{
    let root =
        std::env::var("GRV_GCS_TEST_ROOT").expect("exact authorized GCS child root required");
    let account = std::env::var("GRV_GCS_ACCOUNT").expect("explicit GCS account required");
    let project = std::env::var("GRV_GCS_PROJECT").expect("explicit GCS project required");
    assert!(
        !account.is_empty() && !project.is_empty(),
        "explicit GCS selectors required"
    );
    // Do not trim, reinterpret, append a UUID to, or discover an ambient root.
    // Parent admission owns the bucket; bucket roots are never accepted here.
    grv_adapter_duckdb::gcs_config::sql_filename(&root).expect("canonical gs root required");
    let (_, child) = root
        .strip_prefix("gs://")
        .unwrap()
        .split_once('/')
        .expect("bucket child required");
    assert!(!child.is_empty(), "bucket child required");
    let extensions = PathBuf::from(
        std::env::var("GRV_DUCKDB_EXTENSIONS_DIR").expect("pinned local extensions required"),
    );
    eprintln!("external GCS build test child: {root}");
    let backend = CloudBackend::open(
        &root,
        CloudOptions {
            gcs_account: Some(account.clone()),
            gcs_project: Some(project.clone()),
            ..CloudOptions::default()
        },
    )
    .unwrap();
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
    fs::write(package.join("adapter.toml"),format!("name='duckdb'\nversion={:?}\ninterface_versions=[1]\nbinding_schema_version=1\nentrypoint={:?}\n",env!("CARGO_PKG_VERSION"),env!("CARGO_BIN_EXE_fixture-duckdb-gcs-build"))).unwrap();
    let state = temp.path().join("state");
    let seed_decl = temp.path().join("seed.yml");
    let pull_decl = temp.path().join("pull.yml");
    let decl = temp.path().join("external.yml");
    let source_engine = temp.path().join("source.duckdb");
    let engine = temp.path().join("external.duckdb");
    let context = temp.path().join("context.json");
    let completion_path = temp.path().join("completion.json");
    let reader = PathBuf::from(format!("{}.grv-gcs-read.json", engine.display()));
    protected_document::publish(&reader, &json!({"config_version":1,"readers":[{"scope":root,"provider":"gcloud","account":account,"project":project}]}), LIMIT as u64).unwrap();
    assert_eq!(cli(&adapters, &["init", "--grv", &root], None)["ok"], true);
    let upstream = seed(
        &adapters,
        &root,
        &state,
        &seed_decl,
        &source_engine,
        "upstream",
        (
            "SELECT i::BIGINT AS id FROM range(10000) t(i)",
            "SELECT * FROM (VALUES (11::BIGINT),(13::BIGINT)) t(id)",
        ),
    );
    assert_eq!(upstream["ok"], true, "{upstream}");
    assert_eq!(upstream["result"]["outcome"]["revision"], "1");
    assert!(
        object(&backend, "datasets/upstream/rows/version=1/manifest.json")["data_files"]
            .as_array()
            .unwrap()
            .len()
            >= 3
    );
    let initial_remote = pull(&adapters, &root, &state, &pull_decl, &engine, true);
    assert_eq!(initial_remote["ok"], true, "{initial_remote}");
    let initial_local = pull(&adapters, &root, &state, &pull_decl, &engine, false);
    assert_eq!(initial_local["ok"], true, "{initial_local}");
    let base = seed(
        &adapters,
        &root,
        &state,
        &seed_decl,
        &source_engine,
        "derived",
        ("", ""),
    );
    assert_eq!(base["ok"], true, "{base}");
    assert_eq!(base["result"]["outcome"]["revision"], "1");
    fs::write(&decl,format!("declaration_version: 1\nkind: push\ndataset: derived\nadapter: duckdb\nselection: {{policy: all}}\nconnection: {{database: {engine:?}}}\nbuild:\n  execution: external\n  self_input: true\n  inputs:\n    - {{table: remote.rows, as: numbers}}\n    - {{table: local.small, as: local_numbers}}\n    - {{table: remote.empty, as: blanks}}\ntables:\n  - name: total\n    source: {{table: total}}\n    columns: [{{name: id, type: int64}}]\n  - name: empty\n    source: {{table: empty}}\n    columns: [{{name: flag, type: bool}}]\n")).unwrap();
    let attempt = Uuid::v4();
    let prepared = cli(
        &adapters,
        &[
            "session",
            "prepare",
            "--session",
            context.to_str().unwrap(),
            "--grv",
            &root,
            "--decl",
            decl.to_str().unwrap(),
            "--state",
            state.to_str().unwrap(),
            "--attempt",
            attempt.as_str(),
        ],
        None,
    );
    assert_eq!(prepared["ok"], true, "{prepared}");
    let session: BuildSession =
        serde_json::from_value(record(&context)["session"].clone()).unwrap();
    assert_eq!(session.inputs.len(), 3);
    assert_eq!(session.base_contracts.len(), 2);
    assert_eq!(session.base_revision.get(), 1);
    for input in &session.inputs {
        assert_eq!(input.revision.get(), 1);
    }
    let journal = state
        .join("push")
        .join(attempt.as_str())
        .join("journal.json");
    let fixed = record(&journal);
    let run_control: RunControl =
        serde_json::from_value(fixed["evidence"]["progress"]["owner"]["control"].clone()).unwrap();
    assert!(run_control.holds_confirmed);
    assert_eq!(run_control.inputs.len(), 1);
    assert_eq!(run_control.base_revision.get(), 1);
    let citations: BTreeMap<_, _> = run_control
        .inputs
        .iter()
        .map(|input| {
            (
                (input.dataset.as_str().to_owned(), input.revision.get()),
                input.retention_id.as_str().to_owned(),
            )
        })
        .collect();
    assert!(citations.contains_key(&("upstream".into(), 1)));
    assert!(
        !citations.contains_key(&("derived".into(), 1)),
        "self-input uses the fixed target base, never a self-hold"
    );
    // Updating tracking generations after fixed preparation cannot redirect any
    // input/self view. No GRV source object protected by the holds is mutated.
    let updated = seed(
        &adapters,
        &root,
        &state,
        &seed_decl,
        &source_engine,
        "upstream",
        (
            "SELECT i::BIGINT AS id FROM range(501,504) t(i)",
            "SELECT * FROM (VALUES (91::BIGINT),(93::BIGINT)) t(id)",
        ),
    );
    assert_eq!(updated["ok"], true, "{updated}");
    assert_eq!(updated["result"]["outcome"]["revision"], "2");
    for (remote, old) in [(true, &initial_remote), (false, &initial_local)] {
        let refreshed = pull(&adapters, &root, &state, &pull_decl, &engine, remote);
        assert_eq!(refreshed["ok"], true, "{refreshed}");
        assert_eq!(refreshed["result"]["committed_revision"], "2");
        assert_ne!(
            refreshed["result"]["generation_id"],
            old["result"]["generation_id"]
        );
    }
    let driver = temp.path().join("driver.json");
    protected_document::publish(&driver,&json!({"session":session,"database":engine,"root":root,"account":account,"project":project,"extensions":extensions}),LIMIT as u64).unwrap();
    let store = BuildStore::open(
        &engine,
        root.clone(),
        Some(session.identity.workspace_id.clone()),
        &Resources::default(),
    )
    .unwrap();
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "external_gcs_build_driver", "--nocapture"])
        .env("GRV_GCS_BUILD_DRIVER", &driver)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let invocation =
        ExternalInvocation::start(store, session.clone(), Resources::default(), command).unwrap();
    let key = ObjectKey::new(format!(
        "datasets/derived/.runs/{}.control.json",
        session.identity.run_id
    ))
    .unwrap();
    let completion = invocation
        .complete(
            CompletionKind::Engine,
            session
                .outputs
                .iter()
                .map(|output| CompletedOutput {
                    table: output.table.clone(),
                    engine_table: output.engine_table.clone(),
                })
                .collect(),
            &completion_path,
            || {
                let (bytes, _) = backend
                    .read_bytes(&key, LIMIT)
                    .map_err(|_| std::io::Error::other("run ownership lookup failed"))?;
                let current: RunControl = decode_record(&bytes)
                    .map_err(|_| std::io::Error::other("invalid run ownership"))?;
                Ok(current.phase == RunPhase::Open
                    && current.owner_token == run_control.owner_token
                    && current.run_id == run_control.run_id
                    && current.created_at == run_control.created_at
                    && current.base_revision == run_control.base_revision
                    && current.inputs == run_control.inputs
                    && current.holds_confirmed
                    && current
                        .expires_at
                        .as_ref()
                        .is_some_and(|expiry| expiry > &SystemClock::default().now()))
            },
        )
        .unwrap();
    assert_eq!(completion.completed_outputs.len(), 2);
    assert!(completion.writers_stopped);
    let marker = PathBuf::from(format!("{}.grv-fixture-fail-export-once", engine.display()));
    fs::write(&marker, b"fail").unwrap();
    let failed = cli(
        &adapters,
        &[
            "push",
            "--session",
            context.to_str().unwrap(),
            "--build-result",
            completion_path.to_str().unwrap(),
        ],
        None,
    );
    assert_eq!(failed["ok"], false, "{failed}");
    let partial = record(&journal);
    assert!(partial["evidence"]["progress"]["accepted"].is_object());
    assert!(partial["evidence"]["capture"].is_null());
    let export = &partial["evidence"]["progress"]["export"];
    assert_eq!(export["started"], true);
    assert_eq!(export["stopped"], false);
    assert!(
        export["tables"]
            .as_array()
            .unwrap()
            .iter()
            .any(|table| !table["batches"].as_array().unwrap().is_empty())
    );
    assert_eq!(
        object(&backend, "datasets/derived/.states/LATEST")["revision"],
        1
    );
    assert!(
        backend
            .list(
                &ObjectPrefix::new(format!(
                    "datasets/derived/.runs/{}.allocations/",
                    session.identity.run_id
                ))
                .unwrap(),
                ListMode::Recursive
            )
            .unwrap()
            .is_empty(),
        "partial accepted export must not allocate destination versions"
    );
    // Accepted results are physical outputs. Remove the private reader config,
    // original acquisition engine/declarations and completion document; keep
    // all held immutable GRV objects untouched while retrying the export.
    fs::remove_file(&reader).unwrap();
    fs::remove_file(&source_engine).unwrap();
    fs::remove_file(&decl).unwrap();
    fs::remove_file(&completion_path).unwrap();
    let published = cli(
        &adapters,
        &["push", "--session", context.to_str().unwrap()],
        None,
    );
    assert_eq!(published["ok"], true, "{published}");
    assert_eq!(published["result"]["outcome"]["revision"], "2");
    let complete = record(&journal);
    assert_ne!(
        export["stream_id"],
        complete["evidence"]["progress"]["export"]["stream_id"]
    );
    assert_eq!(
        partial["evidence"]["progress"]["accepted"],
        complete["evidence"]["progress"]["accepted"]
    );
    assert_eq!(complete["evidence"]["progress"]["export"]["stopped"], true);
    assert_eq!(
        output_values(&backend, temp.path(), "datasets/derived/total/version=2"),
        [49_995_031]
    );
    let empty = object(&backend, "datasets/derived/empty/version=2/manifest.json");
    assert_eq!(empty["row_count"], 0);
    for table in ["total", "empty"] {
        let manifest = object(
            &backend,
            &format!("datasets/derived/{table}/version=2/manifest.json"),
        );
        let actual: BTreeMap<_, _> = manifest["derived_from"]
            .as_array()
            .unwrap()
            .iter()
            .map(|reference| {
                (
                    (
                        reference["dataset"].as_str().unwrap().to_owned(),
                        reference["revision"].as_u64().unwrap(),
                    ),
                    reference["retention_id"].as_str().unwrap().to_owned(),
                )
            })
            .collect();
        assert_eq!(actual, citations);
        for ((source, revision), retention) in &actual {
            let hold = object(
                &backend,
                &format!("datasets/{source}/.holds/derived/revision={revision}/{retention}.json"),
            );
            assert_eq!(hold["target_run_id"], session.identity.run_id.as_str());
            let release=ObjectKey::new(format!("datasets/{source}/.states/released-holds/derived/revision={revision}/{retention}.json")).unwrap();
            assert_eq!(
                backend.head(&release).unwrap_err().kind,
                ErrorKind::NotFound
            );
        }
    }
    let verified = cli(
        &adapters,
        &["verify", "derived", "--grv", &root, "--full"],
        None,
    );
    assert_eq!(verified["ok"], true, "{verified}");
    assert_eq!(
        object(&backend, "datasets/derived/.states/LATEST")["revision"],
        2
    );
    fs::remove_dir_all(&adapters).unwrap();
    fs::remove_file(&engine).unwrap();
    fs::remove_file(&driver).unwrap();
    let offline = temp.path().join("offline-home");
    fs::create_dir(&offline).unwrap();
    let replay = cli(
        &adapters,
        &[
            "push",
            "--session",
            context.to_str().unwrap(),
            "--build-result",
            completion_path.to_str().unwrap(),
        ],
        Some(&offline),
    );
    assert_eq!(replay["ok"], true, "{replay}");
    let mut expected = published["result"].clone();
    expected["replayed"] = true.into();
    assert_eq!(replay["result"], expected);
}
