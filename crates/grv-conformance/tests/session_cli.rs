#![cfg(feature = "native-duckdb")]
use grv_adapter_api::{BuildSession, CompletedOutput, CompletionKind, Resources};
use grv_adapter_duckdb::{build::BuildStore, driver::ExternalInvocation, native::NativeEngine};
use grv_adapter_host::{protected_document, validate_output};
use grv_types::Uuid;
use serde_json::Value;
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
};
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
struct Fixture {
    temp: tempfile::TempDir,
    adapters: PathBuf,
    root: PathBuf,
    decl: PathBuf,
    state: PathBuf,
    context: PathBuf,
    engine: PathBuf,
    attempt: Uuid,
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
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let adapters = temp.path().join("adapters");
        let package = adapters.join("duckdb");
        fs::create_dir_all(&package).unwrap();
        fs::write(package.join("adapter.toml"),format!("name='duckdb'\nversion={:?}\ninterface_versions=[1]\nbinding_schema_version=1\nentrypoint={:?}\n",env!("CARGO_PKG_VERSION"),env!("CARGO_BIN_EXE_fixture-duckdb-build"))).unwrap();
        let root = temp.path().join("grv");
        let decl = temp.path().join("build.yml");
        let state = temp.path().join("state");
        let engine = temp.path().join("engine.duckdb");
        let context = temp.path().join("context.json");
        assert_eq!(
            cli(&adapters, &["init", "--grv", root.to_str().unwrap()])["ok"],
            true
        );
        fs::write(&decl,format!("declaration_version: 1\nkind: push\ndataset: data\nadapter: duckdb\nconnection: {{database: {engine:?}}}\nbuild: {{execution: external}}\ntables:\n  - name: rows\n    source: {{table: rows}}\n    columns: [{{name: id, type: int64}}]\n")).unwrap();
        Self {
            temp,
            adapters,
            root,
            decl,
            state,
            context,
            engine,
            attempt: Uuid::v4(),
        }
    }
    fn prepare(&self) -> Value {
        cli(
            &self.adapters,
            &[
                "session",
                "prepare",
                "--session",
                self.context.to_str().unwrap(),
                "--decl",
                self.decl.to_str().unwrap(),
                "--grv",
                self.root.to_str().unwrap(),
                "--state",
                self.state.to_str().unwrap(),
                "--attempt",
                self.attempt.as_str(),
            ],
        )
    }
    fn session(&self, command: &str) -> Value {
        cli(
            &self.adapters,
            &[
                "session",
                command,
                "--session",
                self.context.to_str().unwrap(),
            ],
        )
    }
    fn fixed_session(&self) -> BuildSession {
        protected_document::read::<Value>(&self.context, 64 * 1024 * 1024)
            .map(|v| serde_json::from_value(v["session"].clone()).unwrap())
            .unwrap()
    }
}
#[test]
fn external_session_renew_during_driver_accept_empty_export_and_terminal_source_free_replay() {
    let f = Fixture::new();
    let prepared = f.prepare();
    assert_eq!(prepared["ok"], true, "{prepared}");
    assert_eq!(prepared["result"]["session"]["run"]["phase"], "open");
    assert_eq!(f.prepare()["ok"], true);
    let latest: Value =
        serde_json::from_slice(&fs::read(f.root.join("datasets/data/.states/LATEST")).unwrap())
            .unwrap();
    assert_eq!(latest["revision"], 0);
    assert!(latest["lease"].is_null());
    let authored = fs::read(&f.decl).unwrap();
    let context = fs::read(&f.context).unwrap();
    fs::remove_file(&f.decl).unwrap();
    let replay = f.prepare();
    assert_eq!(replay["ok"], true, "{replay}");
    assert_eq!(context, fs::read(&f.context).unwrap());
    fs::write(&f.decl, authored).unwrap();
    let session = f.fixed_session();
    let store = BuildStore::open(
        &f.engine,
        f.root.to_str().unwrap().into(),
        Some(session.identity.workspace_id.clone()),
        &Resources::default(),
    )
    .unwrap();
    let mut command = Command::new("/bin/sh");
    command.args(["-c", "sleep 0.1"]);
    let invocation =
        ExternalInvocation::start(store, session.clone(), Resources::default(), command).unwrap();
    let renewed = f.session("renew");
    assert_eq!(renewed["ok"], true, "{renewed}");
    assert_eq!(f.session("show")["errors"][0]["code"], "ENGINE_BUSY");
    assert_eq!(f.session("abort")["errors"][0]["code"], "ENGINE_BUSY");
    let completion_path = f.temp.path().join("completion.json");
    let completion = invocation
        .complete(
            CompletionKind::Direct,
            vec![CompletedOutput {
                table: session.outputs[0].table.clone(),
                engine_table: session.outputs[0].engine_table.clone(),
            }],
            &completion_path,
            || Ok(true),
        )
        .unwrap();
    assert_eq!(completion.completed_outputs.len(), 1);
    let pushed = cli(
        &f.adapters,
        &[
            "push",
            "--session",
            f.context.to_str().unwrap(),
            "--build-result",
            completion_path.to_str().unwrap(),
        ],
    );
    assert_eq!(pushed["ok"], true, "{pushed}");
    assert_eq!(pushed["result"]["outcome"]["revision"], "1");
    let mut store = BuildStore::open(
        &f.engine,
        f.root.to_str().unwrap().into(),
        Some(session.identity.workspace_id.clone()),
        &Resources::default(),
    )
    .unwrap();
    let recorded = store.open_session(&session.identity).unwrap();
    let outcome = recorded.outcome.unwrap();
    assert_eq!(
        serde_json::to_value(&outcome.operation_id).unwrap(),
        pushed["result"]["outcome"]["operation_id"]
    );
    assert_ne!(outcome.operation_id, Some(session.identity.run_id.clone()));
    drop(store);
    let shown = f.session("show");
    assert_eq!(shown["ok"], true, "{shown}");
    assert_eq!(shown["result"]["session"]["run"]["phase"], "sealed");
    assert_eq!(shown["result"]["session"]["outcome"]["kind"], "published");
    assert!(
        !serde_json::to_string(&shown)
            .unwrap()
            .contains("owner_token")
    );
    fs::remove_dir_all(&f.adapters).unwrap();
    fs::remove_file(&f.engine).unwrap();
    fs::remove_dir_all(&f.root).unwrap();
    fs::remove_file(&completion_path).unwrap();
    fs::remove_file(&f.decl).unwrap();
    let replay = cli(
        &f.adapters,
        &[
            "push",
            "--session",
            f.context.to_str().unwrap(),
            "--build-result",
            completion_path.to_str().unwrap(),
        ],
    );
    assert_eq!(replay["ok"], true, "{replay}");
    assert_eq!(replay["result"]["replayed"], true);
}
#[test]
fn external_session_requires_completion_protects_context_and_abort_is_durable() {
    let f = Fixture::new();
    let prepared = f.prepare();
    assert_eq!(prepared["ok"], true, "{prepared}");
    let missing = cli(
        &f.adapters,
        &["push", "--session", f.context.to_str().unwrap()],
    );
    assert_eq!(
        missing["errors"][0]["code"], "BUILD_INCOMPLETE",
        "{missing}"
    );
    assert!(!f.root.join("datasets/data/rows/version=1").exists());
    let engine = NativeEngine::open(&f.engine).unwrap();
    assert_eq!(f.session("renew")["ok"], true);
    assert_eq!(f.session("abort")["errors"][0]["code"], "ENGINE_BUSY");
    drop(engine);
    let aborted = f.session("abort");
    assert_eq!(aborted["ok"], true, "{aborted}");
    assert_eq!(aborted["result"]["outcome"]["kind"], "aborted");
    assert_eq!(f.session("abort")["result"], aborted["result"]);
    let rejected = cli(
        &f.adapters,
        &["push", "--session", f.context.to_str().unwrap()],
    );
    assert_eq!(rejected["errors"][0]["code"], "BUILD_INCOMPLETE");
    assert_eq!(f.session("renew")["ok"], false);
    let bad = f.temp.path().join("changed.json");
    let mut value: Value = protected_document::read(&f.context, 64 * 1024 * 1024).unwrap();
    value["session"]["base_revision"] = serde_json::json!("1");
    protected_document::publish(&bad, &value, 64 * 1024 * 1024).unwrap();
    let changed = cli(
        &f.adapters,
        &["session", "renew", "--session", bad.to_str().unwrap()],
    );
    assert_eq!(changed["errors"][0]["code"], "REQUEST_MISMATCH");
}

// This fixture invokes DuckDB's public C API as a supervised external process.
// It retains the workspace descriptor inherited from ExternalInvocation.
#[test]
fn external_driver_fixture_probe() {
    let Some(path) = std::env::var_os("GRV_SESSION_TEST_DRIVER_DATABASE") else {
        return;
    };
    use std::ffi::{CString, c_char, c_void};
    unsafe extern "C" {
        fn duckdb_open(path: *const c_char, database: *mut *mut c_void) -> u32;
        fn duckdb_connect(database: *mut c_void, connection: *mut *mut c_void) -> u32;
        fn duckdb_query(connection: *mut c_void, query: *const c_char, result: *mut c_void) -> u32;
        fn duckdb_disconnect(connection: *mut *mut c_void);
        fn duckdb_close(database: *mut *mut c_void);
    }
    let path = CString::new(path.to_str().unwrap()).unwrap();
    let sql = CString::new(std::env::var("GRV_SESSION_TEST_DRIVER_SQL").unwrap()).unwrap();
    let mut database = std::ptr::null_mut();
    let mut connection = std::ptr::null_mut();
    unsafe {
        assert_eq!(duckdb_open(path.as_ptr(), &mut database), 0);
        assert_eq!(duckdb_connect(database, &mut connection), 0);
        let status = duckdb_query(connection, sql.as_ptr(), std::ptr::null_mut());
        duckdb_disconnect(&mut connection);
        duckdb_close(&mut database);
        assert_eq!(status, 0);
    }
}
#[test]
fn external_nonempty_driver_results_export_exact_sorted_rows_and_reject_wrong_attestation_before_allocation()
 {
    let f = Fixture::new();
    assert_eq!(f.prepare()["ok"], true);
    let session = f.fixed_session();
    let store = BuildStore::open(
        &f.engine,
        f.root.to_str().unwrap().into(),
        Some(session.identity.workspace_id.clone()),
        &Resources::default(),
    )
    .unwrap();
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "external_driver_fixture_probe", "--nocapture"])
        .env("GRV_SESSION_TEST_DRIVER_DATABASE", &f.engine)
        .env(
            "GRV_SESSION_TEST_DRIVER_SQL",
            format!(
                "INSERT INTO {}(id) VALUES(3),(1),(1)",
                session.outputs[0].engine_table
            ),
        )
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    let invocation =
        ExternalInvocation::start(store, session.clone(), Resources::default(), command).unwrap();
    let completion_path = f.temp.path().join("completion.json");
    let completion = invocation
        .complete(
            CompletionKind::Engine,
            vec![CompletedOutput {
                table: session.outputs[0].table.clone(),
                engine_table: session.outputs[0].engine_table.clone(),
            }],
            &completion_path,
            || Ok(true),
        )
        .unwrap();
    let mut wrong = completion.clone();
    wrong.run_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
    let wrong_path = f.temp.path().join("wrong.json");
    protected_document::publish(&wrong_path, &wrong, 2 * 1024 * 1024).unwrap();
    let rejected = cli(
        &f.adapters,
        &[
            "push",
            "--session",
            f.context.to_str().unwrap(),
            "--build-result",
            wrong_path.to_str().unwrap(),
        ],
    );
    assert_eq!(rejected["ok"], false, "{rejected}");
    assert!(!f.root.join("datasets/data/rows/version=1").exists());
    let pushed = cli(
        &f.adapters,
        &[
            "push",
            "--build-result",
            completion_path.to_str().unwrap(),
            "--session",
            f.context.to_str().unwrap(),
        ],
    );
    assert_eq!(pushed["ok"], true, "{pushed}");
    let verified = cli(
        &f.adapters,
        &[
            "verify",
            "data",
            "--grv",
            f.root.to_str().unwrap(),
            "--full",
        ],
    );
    assert_eq!(verified["ok"], true, "{verified}");
    let manifest: Value = serde_json::from_slice(
        &fs::read(f.root.join("datasets/data/rows/version=1/manifest.json")).unwrap(),
    )
    .unwrap();
    let name = manifest["data_files"][0]["name"].as_str().unwrap();
    let file = fs::File::open(f.root.join("datasets/data/rows/version=1").join(name)).unwrap();
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let mut actual = vec![];
    for batch in reader {
        let batch = batch.unwrap();
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        actual.extend(values.values().iter().copied());
    }
    assert_eq!(actual, vec![1, 1, 3]);
}
