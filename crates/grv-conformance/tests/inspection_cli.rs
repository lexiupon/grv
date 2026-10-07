#![cfg(feature = "native-duckdb")]
use grv_adapter_duckdb::native::NativeEngine;
use grv_adapter_host::validate_output;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
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
fn package(adapters: &Path, name: &str, executable: &str) {
    let path = adapters.join(name);
    std::fs::create_dir_all(&path).unwrap();
    std::fs::write(path.join("adapter.toml"),format!("name = '{name}'\nversion = '0.1.0'\ninterface_versions = [1]\nbinding_schema_version = 1\nentrypoint = {executable:?}\n")).unwrap();
}
fn delete_engine_build_history(path: &Path) {
    use std::ffi::{CString, c_char, c_void};
    unsafe extern "C" {
        fn duckdb_open(path: *const c_char, database: *mut *mut c_void) -> u32;
        fn duckdb_connect(database: *mut c_void, connection: *mut *mut c_void) -> u32;
        fn duckdb_query(connection: *mut c_void, query: *const c_char, result: *mut c_void) -> u32;
        fn duckdb_disconnect(connection: *mut *mut c_void);
        fn duckdb_close(database: *mut *mut c_void);
    }
    let path = CString::new(path.to_str().unwrap()).unwrap();
    let query = CString::new("DROP TABLE _grv.build_sessions").unwrap();
    let mut database = std::ptr::null_mut();
    let mut connection = std::ptr::null_mut();
    unsafe {
        assert_eq!(duckdb_open(path.as_ptr(), &mut database), 0);
        assert_eq!(duckdb_connect(database, &mut connection), 0);
        let status = duckdb_query(connection, query.as_ptr(), std::ptr::null_mut());
        duckdb_disconnect(&mut connection);
        duckdb_close(&mut database);
        assert_eq!(status, 0);
    }
}
fn snapshot(path: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn walk(root: &Path, path: &Path, result: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                walk(root, &path, result);
            } else {
                result.insert(
                    path.strip_prefix(root).unwrap().into(),
                    std::fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut result = BTreeMap::new();
    walk(path, path, &mut result);
    result
}
struct Fixture {
    temp: tempfile::TempDir,
    adapters: PathBuf,
    root: PathBuf,
    state: PathBuf,
    push: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        Self::with_adapter(env!("CARGO_BIN_EXE_fixture-duckdb"))
    }
    fn with_adapter(executable: &str) -> Self {
        let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap();
        let temp = tempfile::tempdir_in(repository).unwrap();
        std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let adapters = temp.path().join("adapters");
        package(&adapters, "fixture", env!("CARGO_BIN_EXE_fixture"));
        package(&adapters, "duckdb", executable);
        let root = temp.path().join("root");
        let state = temp.path().join("state");
        let push = temp.path().join("push.yml");
        std::fs::write(&push,"declaration_version: 1\nkind: push\ndataset: data\nadapter: fixture\nconnection: {}\nselection: {policy: all}\ntables:\n  - name: rows\n    source: {}\n    columns: [{name: value, type: int64}]\n  - name: empty\n    source: {}\n    columns: [{name: value, type: int64}]\n").unwrap();
        let fixture = Self {
            temp,
            adapters,
            root,
            state,
            push,
        };
        fixture.command("init", None);
        fixture.command("push", Some(&fixture.push));
        fixture
    }
    fn command(&self, command: &str, decl: Option<&Path>) -> Value {
        let mut args = vec![command, "--grv", self.root.to_str().unwrap()];
        if let Some(decl) = decl {
            args.extend([
                "--decl",
                decl.to_str().unwrap(),
                "--state",
                self.state.to_str().unwrap(),
            ]);
        }
        let result = cli(&self.adapters, &args);
        assert_eq!(result["ok"], true, "{result}");
        result
    }
    fn status(&self, decl: Option<&Path>) -> Value {
        let mut args = vec!["status", "data", "--grv", self.root.to_str().unwrap()];
        if let Some(decl) = decl {
            args.extend(["--decl", decl.to_str().unwrap()]);
        }
        cli(&self.adapters, &args)
    }
    fn pull(
        &self,
        name: &str,
        write: &str,
        target: &str,
        revision: Option<u64>,
        sql: bool,
    ) -> (PathBuf, PathBuf) {
        let path = self.temp.path().join(format!("{name}.yml"));
        let engine = self.temp.path().join(format!("{name}.duckdb"));
        let revision = revision
            .map(|r| format!("revision: {r}\n"))
            .unwrap_or_default();
        let select = if sql {
            "\n    columns: [{name: value, type: int64}]\n    select: {sql: 'SELECT value FROM grv_source.rows'}"
        } else {
            ""
        };
        std::fs::write(&path,format!("declaration_version: 1\nkind: pull\ndataset: data\nadapter: duckdb\nwrite: {write}\n{revision}connection: {{database: {engine:?}}}\ntarget: {{schema: {target}}}\ntables:\n  - name: rows{select}\n  - name: empty\n")).unwrap();
        self.command("pull", Some(&path));
        (path, engine)
    }
}
#[test]
fn status_decl_is_readonly_uninitialized_busy_scoped_and_unsupported_inspection_preserves_backend_facts()
 {
    let f = Fixture::new();
    let database = f.temp.path().join("uninitialized.duckdb");
    drop(NativeEngine::open(&database).unwrap());
    let decl = f.temp.path().join("inspect.yml");
    let canary = f.temp.path().join("never-created");
    std::fs::write(&decl,format!("declaration_version: 1\nkind: pull\ndataset: data\nadapter: duckdb\nconnection: {{database: {database:?}}}\ntarget: {{schema: app}}\ntables:\n  - name: rows\n    columns: [{{name: value, type: int64}}]\n    select: {{sql: \"COPY (SELECT 1) TO '{canary}'\"}}\n",canary=canary.display())).unwrap();
    let files = snapshot(&f.root);
    let engine = std::fs::read(&database).unwrap();
    let state = snapshot(&f.state);
    let observed = f.status(None);
    assert_eq!(observed["ok"], true);
    assert_eq!(observed["result"]["adapter_state"], Value::Null);
    let inspected = f.status(Some(&decl));
    assert_eq!(inspected["ok"], true, "{inspected}");
    let details = &inspected["result"]["adapter_state"]["details"];
    assert_eq!(details["binding"], "uninitialized");
    assert_eq!(details["workspace_id"], Value::Null);
    assert_eq!(details["materializations"], json!([]));
    assert_eq!(details["sessions"], json!([]));
    assert_eq!(details["imports"], json!([]));
    assert!(!canary.exists());
    let owner = NativeEngine::open_readonly(&database).unwrap();
    let busy = f.status(Some(&decl));
    assert_eq!(busy["errors"][0]["code"], "ENGINE_BUSY", "{busy}");
    assert_eq!(busy["result"]["current_revision"], "1");
    assert_eq!(busy["result"]["adapter_state"], Value::Null);
    drop(owner);
    let unsupported = f.status(Some(&f.push));
    assert_eq!(unsupported["errors"][0]["code"], "UNSUPPORTED_CAPABILITY");
    assert_eq!(unsupported["result"]["current_revision"], "1");
    assert_eq!(snapshot(&f.root), files);
    assert_eq!(snapshot(&f.state), state);
    assert_eq!(std::fs::read(&database).unwrap(), engine);
    // A missing engine is never created by optional inspection.
    std::fs::remove_file(&database).unwrap();
    let absent = f.status(Some(&decl));
    assert_eq!(absent["ok"], false);
    assert_eq!(absent["result"]["current_revision"], "1");
    assert!(!database.exists());
}
#[test]
fn status_decl_projects_current_behind_fixed_and_successful_sql_append_imports_without_repair() {
    let f = Fixture::new();
    let (decl, database) = f.pull("latest", "replace", "app", None, false);
    let before = std::fs::read(&database).unwrap();
    let root_before = snapshot(&f.root);
    let first = f.status(Some(&decl));
    assert_eq!(first["ok"], true, "{first}");
    let materialization = &first["result"]["adapter_state"]["details"]["materializations"][0];
    assert_eq!(materialization["state"], "current");
    assert_eq!(materialization["committed_revision"], "1");
    assert_eq!(materialization["mappings"].as_array().unwrap().len(), 2);
    assert_eq!(std::fs::read(&database).unwrap(), before);
    assert_eq!(snapshot(&f.root), root_before);
    f.command("push", Some(&f.push));
    let behind = f.status(Some(&decl));
    assert_eq!(behind["ok"], true, "{behind}");
    assert_eq!(
        behind["result"]["adapter_state"]["details"]["materializations"][0]["state"],
        "behind"
    );
    let (fixed, _) = f.pull("fixed", "replace", "fixed", Some(1), false);
    let observed = f.status(Some(&fixed));
    assert_eq!(observed["ok"], true, "{observed}");
    assert_eq!(
        observed["result"]["adapter_state"]["details"]["materializations"][0]["state"],
        "fixed"
    );
    let (append, engine) = f.pull("append", "append", "imports", None, true);
    let original = std::fs::read(&engine).unwrap();
    let observed = f.status(Some(&append));
    assert_eq!(observed["ok"], true, "{observed}");
    let details = &observed["result"]["adapter_state"]["details"];
    assert_eq!(details["materializations"], json!([]));
    assert_eq!(details["imports"][0]["write_mode"], "append");
    assert_eq!(details["imports"][0]["transform_mode"], "sql");
    assert_eq!(details["imports"][0]["source_revision"], "2");
    assert_eq!(std::fs::read(&engine).unwrap(), original);
    // Foreign-root inspection rejects before claiming an engine observation.
    let foreign = f.temp.path().join("foreign");
    let initialized = cli(&f.adapters, &["init", "--grv", foreign.to_str().unwrap()]);
    assert_eq!(initialized["ok"], true);
    let pushed = cli(
        &f.adapters,
        &[
            "push",
            "--grv",
            foreign.to_str().unwrap(),
            "--decl",
            f.push.to_str().unwrap(),
            "--state",
            f.state.to_str().unwrap(),
        ],
    );
    assert_eq!(pushed["ok"], true, "{pushed}");
    let refused = cli(
        &f.adapters,
        &[
            "status",
            "data",
            "--grv",
            foreign.to_str().unwrap(),
            "--decl",
            decl.to_str().unwrap(),
        ],
    );
    assert_eq!(refused["errors"][0]["code"], "STATE_CONFLICT", "{refused}");
    assert_eq!(refused["result"]["adapter_state"], Value::Null);
}
#[test]
fn status_decl_combines_durable_session_outcome_with_backend_run_and_redacts_authority() {
    let f = Fixture::with_adapter(env!("CARGO_BIN_EXE_fixture-duckdb-build"));
    let database = f.temp.path().join("build.duckdb");
    let decl = f.temp.path().join("build.yml");
    std::fs::write(&decl,format!("declaration_version: 1\nkind: push\ndataset: data\nadapter: duckdb\nconnection: {{database: {database:?}}}\nbuild: {{execution: managed, inputs: []}}\ntables:\n  - name: literal\n    source: {{sql: 'SELECT 7::BIGINT AS id'}}\n    columns: [{{name: id, type: int64}}]\n")).unwrap();
    let built = f.command("push", Some(&decl));
    let before = std::fs::read(&database).unwrap();
    let backend = snapshot(&f.root);
    let private = snapshot(&f.state);
    let status = f.status(Some(&decl));
    assert_eq!(status["ok"], true, "{status}");
    let sessions = &status["result"]["adapter_state"]["details"]["sessions"];
    assert_eq!(sessions.as_array().unwrap().len(), 1);
    let session = &sessions[0];
    assert_eq!(session["run_id"], built["result"]["run_id"]);
    assert_eq!(session["state"], "completed");
    assert_eq!(
        session["outcome"]["operation_id"],
        built["result"]["outcome"]["operation_id"]
    );
    assert_ne!(session["outcome"]["operation_id"], session["run_id"]);
    assert_eq!(session["run"]["phase"], "sealed");
    assert!(session["completion_sha256"].is_string());
    let encoded = status.to_string();
    for forbidden in [
        "owner_token",
        "claim_token",
        "authorization",
        "access_token",
        "request_sha256",
        "queries_sha256",
    ] {
        assert!(
            !encoded.contains(forbidden),
            "private authority leaked: {forbidden}"
        );
    }
    assert_eq!(std::fs::read(&database).unwrap(), before);
    assert_eq!(snapshot(&f.root), backend);
    assert_eq!(snapshot(&f.state), private);
    delete_engine_build_history(&database);
    let damaged = std::fs::read(&database).unwrap();
    let refused = f.status(Some(&decl));
    assert_eq!(refused["errors"][0]["code"], "OUTCOME_UNKNOWN", "{refused}");
    assert_eq!(refused["result"]["adapter_state"], Value::Null);
    assert_eq!(refused["result"]["current_revision"], "2");
    assert_eq!(std::fs::read(&database).unwrap(), damaged);
    assert_eq!(snapshot(&f.root), backend);
    assert_eq!(snapshot(&f.state), private);
}
