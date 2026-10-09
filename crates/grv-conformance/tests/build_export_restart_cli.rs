#![cfg(feature = "native-duckdb")]
use grv_adapter_api::{BuildIdentity, Resources};
use grv_adapter_duckdb::build::BuildStore;
use grv_adapter_host::validate_output;
use grv_core::{
    journal::{Envelope, Journal},
    store::{InitOptions, Store},
};
use grv_storage::LocalBackend;
use grv_types::Uuid;
use serde_json::Value;
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant},
};
type Record = Envelope<Value, Value, Value, Value>;
struct Fixture {
    temp: tempfile::TempDir,
    adapters: PathBuf,
    root: PathBuf,
    state: PathBuf,
    decl: PathBuf,
    engine: PathBuf,
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
        Store::initialize(
            LocalBackend::create(&root).unwrap(),
            InitOptions {
                max_clock_skew: Some(0),
                pending_grace: Some(1),
                ..Default::default()
            },
        )
        .unwrap();
        let state = temp.path().join("state");
        let decl = temp.path().join("build.yml");
        let engine = temp.path().join("engine.duckdb");
        Self {
            temp,
            adapters,
            root,
            state,
            decl,
            engine,
        }
    }
    fn cli(&self, args: &[&str]) -> Value {
        let output = Command::new(env!("CARGO_BIN_EXE_grv-conformance-cli"))
            .arg("--json")
            .args(args)
            .env("GRV_ADAPTERS_DIR", &self.adapters)
            .output()
            .unwrap();
        let value: Value = serde_json::from_slice(&output.stdout)
            .unwrap_or_else(|e| panic!("{e}: {}", String::from_utf8_lossy(&output.stderr)));
        validate_output(&value).unwrap_or_else(|e| panic!("{e}: {value}"));
        assert_eq!(output.status.success(), value["ok"] == true);
        value
    }
    fn declaration(&self, dataset: &str, sql: &str) {
        fs::write(&self.decl,format!("declaration_version: 1\nkind: push\ndataset: {dataset}\nadapter: duckdb\nconnection: {{database: {:?}}}\nbuild: {{execution: managed}}\ntables:\n  - name: rows\n    source: {{sql: {sql:?}}}\n    columns: [{{name: value, type: int64}}]\n",self.engine)).unwrap();
    }
    fn push(&self, attempt: &Uuid) -> Value {
        self.cli(&[
            "push",
            "--grv",
            self.root.to_str().unwrap(),
            "--decl",
            self.decl.to_str().unwrap(),
            "--state",
            self.state.to_str().unwrap(),
            "--attempt",
            attempt.as_str(),
        ])
    }
    fn marker(&self, suffix: &str) -> PathBuf {
        let mut path = self.engine.as_os_str().to_os_string();
        path.push(suffix);
        path.into()
    }
    fn record(&self, attempt: &Uuid) -> Record {
        Journal::open(
            self.state.join("push").join(attempt.as_str()),
            std::slice::from_ref(&self.root),
        )
        .unwrap()
        .read()
        .unwrap()
    }
    fn pull_raw(&self) {
        fs::write(&self.decl,format!("declaration_version: 1\nkind: pull\ndataset: raw\nadapter: duckdb\nconnection: {{database: {:?}}}\ntarget: {{schema: raw}}\ntables: [{{name: rows}}]\n",self.engine)).unwrap();
        let pulled = self.cli(&[
            "pull",
            "--grv",
            self.root.to_str().unwrap(),
            "--decl",
            self.decl.to_str().unwrap(),
            "--state",
            self.state.to_str().unwrap(),
        ]);
        assert_eq!(pulled["ok"], true, "{pulled}");
    }
    fn external_declaration(&self) {
        fs::write(&self.decl,format!("declaration_version: 1\nkind: push\ndataset: derived\nadapter: duckdb\nconnection: {{database: {:?}}}\nbuild:\n  execution: external\n  inputs: [{{table: raw.rows, as: source}}]\ntables:\n  - name: rows\n    source: {{table: rows}}\n    columns: [{{name: value, type: int64}}]\n",self.engine)).unwrap();
    }
    fn gc_raw(&self) -> Value {
        self.cli(&[
            "gc",
            "raw",
            "--grv",
            self.root.to_str().unwrap(),
            "--state",
            self.state.to_str().unwrap(),
            "--apply",
        ])
    }
}
#[test]
fn accepted_partial_export_restarts_with_fresh_stream_without_reexecuting_managed_sql() {
    let f = Fixture::new();
    let attempt = Uuid::v4();
    f.declaration(
        "data",
        "SELECT i::BIGINT AS value FROM range(10000) AS t(i)",
    );
    let counter = f.marker(".grv-fixture-executions");
    fs::write(&counter, []).unwrap();
    fs::write(f.marker(".grv-fixture-fail-export-once"), b"fail").unwrap();
    let failed = f.push(&attempt);
    assert_eq!(failed["ok"], false, "{failed}");
    let partial = f.record(&attempt);
    assert!(partial.evidence.capture.is_none());
    assert!(partial.evidence.progress["accepted"].is_object());
    let export = &partial.evidence.progress["export"];
    assert_eq!(export["started"], true);
    assert_eq!(export["stopped"], false);
    assert_eq!(export["tables"][0]["batches"].as_array().unwrap().len(), 1);
    assert_eq!(fs::read(&counter).unwrap(), b"execute\n");
    let identity: BuildIdentity =
        serde_json::from_value(partial.evidence.intent["request"]["identity"].clone()).unwrap();
    let mut store = BuildStore::open(
        &f.engine,
        partial.evidence.intent["root"].as_str().unwrap().into(),
        Some(identity.workspace_id.clone()),
        &Resources::default(),
    )
    .unwrap();
    let accepted = store.open_session(&identity).unwrap();
    assert!(accepted.completion.is_some());
    drop(store);
    fs::remove_file(&f.decl).unwrap();
    let published = f.push(&attempt);
    assert_eq!(published["ok"], true, "{published}");
    assert_eq!(fs::read(&counter).unwrap(), b"execute\n");
    let complete = f.record(&attempt);
    assert_ne!(
        export["stream_id"],
        complete.evidence.progress["export"]["stream_id"]
    );
    assert_eq!(
        complete.evidence.progress["accepted"],
        partial.evidence.progress["accepted"]
    );
    assert_eq!(complete.evidence.progress["export"]["stopped"], true);
    assert_eq!(
        complete.evidence.progress["export"]["tables"][0]["row_count"],
        "10000"
    );
    assert_eq!(published["result"]["outcome"]["revision"], "1");
    let verified = f.cli(&[
        "verify",
        "data",
        "--grv",
        f.root.to_str().unwrap(),
        "--full",
    ]);
    assert_eq!(verified["ok"], true, "{verified}");
    fs::remove_dir_all(&f.adapters).unwrap();
    fs::remove_file(&f.engine).unwrap();
    fs::remove_dir_all(&f.root).unwrap();
    let replay = f.push(&attempt);
    assert_eq!(replay["ok"], true, "{replay}");
    assert_eq!(replay["result"]["replayed"], true);
}
#[test]
fn source_pruned_after_discovery_refuses_preparation_before_destination_allocations() {
    let f = Fixture::new();
    f.declaration("raw", "SELECT 1::BIGINT AS value");
    assert_eq!(f.push(&Uuid::v4())["ok"], true);
    f.pull_raw();
    f.declaration("raw", "SELECT 2::BIGINT AS value");
    assert_eq!(f.push(&Uuid::v4())["ok"], true);
    f.external_declaration();
    let pause = f.marker(".grv-fixture-pause-discovery");
    let ready = f.marker(".grv-fixture-discovery-ready");
    fs::write(&pause, []).unwrap();
    let attempt = Uuid::v4();
    let context = f.temp.path().join("context.json");
    let mut child = Command::new(env!("CARGO_BIN_EXE_grv-conformance-cli"))
        .arg("--json")
        .args([
            "session",
            "prepare",
            "--session",
            context.to_str().unwrap(),
            "--grv",
            f.root.to_str().unwrap(),
            "--decl",
            f.decl.to_str().unwrap(),
            "--state",
            f.state.to_str().unwrap(),
            "--attempt",
            attempt.as_str(),
        ])
        .env("GRV_ADAPTERS_DIR", &f.adapters)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while !ready.exists() {
        if child.try_wait().unwrap().is_some() {
            let output = child.wait_with_output().unwrap();
            panic!(
                "discovery exited: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("discovery did not pause");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    std::thread::sleep(Duration::from_secs(2));
    let gc = f.gc_raw();
    fs::remove_file(&pause).unwrap();
    assert_eq!(gc["ok"], true, "{gc}");
    assert!(
        !f.root
            .join("datasets/raw/rows/version=1/data.parquet")
            .exists()
    );
    let output = child.wait_with_output().unwrap();
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    validate_output(&result).unwrap();
    assert_eq!(result["ok"], false, "{result}");
    assert!(!context.exists());
    let dataset = f.root.join("datasets/derived");
    if dataset.exists() {
        assert!(!contains_allocation(&dataset));
    }
}
fn contains_allocation(path: &Path) -> bool {
    fs::read_dir(path).unwrap().any(|entry| {
        let path = entry.unwrap().path();
        path.to_string_lossy().contains(".allocations/")
            || path.is_dir() && contains_allocation(&path)
    })
}
#[test]
fn confirmed_build_input_hold_survives_new_source_revision_and_actual_gc() {
    let f = Fixture::new();
    f.declaration("raw", "SELECT 1::BIGINT AS value");
    assert_eq!(f.push(&Uuid::v4())["ok"], true);
    f.pull_raw();
    f.external_declaration();
    let context = f.temp.path().join("context.json");
    let attempt = Uuid::v4();
    let prepared = f.cli(&[
        "session",
        "prepare",
        "--session",
        context.to_str().unwrap(),
        "--grv",
        f.root.to_str().unwrap(),
        "--decl",
        f.decl.to_str().unwrap(),
        "--state",
        f.state.to_str().unwrap(),
        "--attempt",
        attempt.as_str(),
    ]);
    assert_eq!(prepared["ok"], true, "{prepared}");
    assert_eq!(
        prepared["result"]["session"]["input_revisions"][0]["revision"],
        "1"
    );
    f.declaration("raw", "SELECT 2::BIGINT AS value");
    assert_eq!(f.push(&Uuid::v4())["ok"], true);
    std::thread::sleep(Duration::from_secs(2));
    let gc = f.gc_raw();
    assert_eq!(gc["ok"], true, "{gc}");
    assert!(
        f.root
            .join("datasets/raw/rows/version=1/data.parquet")
            .exists()
    );
    assert!(!f.root.join("datasets/raw/rows/version=1/.pruned").exists());
    let shown = f.cli(&["session", "show", "--session", context.to_str().unwrap()]);
    assert_eq!(shown["ok"], true, "{shown}");
    assert_eq!(shown["result"]["session"]["run"]["phase"], "open");
    assert_eq!(
        shown["result"]["session"]["input_revisions"],
        prepared["result"]["session"]["input_revisions"]
    );
    let aborted = f.cli(&["session", "abort", "--session", context.to_str().unwrap()]);
    assert_eq!(aborted["ok"], true, "{aborted}");
}
