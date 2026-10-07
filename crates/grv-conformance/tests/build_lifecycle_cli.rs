#![cfg(feature = "native-duckdb")]
use arrow_array::Array;
use grv_adapter_api::{
    BuildCompletion, BuildSession, CompletedOutput, CompletionKind, CompletionStatus, Resources,
};
use grv_adapter_duckdb::{build::BuildStore, driver::ExternalInvocation};
use grv_adapter_host::{protected_document, validate_output};
use grv_core::clock::{Clock, SystemClock};
use grv_types::{Req, Uuid};
use serde_json::Value;
use std::{
    collections::BTreeMap,
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
    validate_output(&value).unwrap_or_else(|e| panic!("{e}: {value}"));
    assert_eq!(output.status.success(), value["ok"] == true);
    value
}
struct Fixture {
    temp: tempfile::TempDir,
    adapters: PathBuf,
    root: PathBuf,
    decl: PathBuf,
    state: PathBuf,
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
        fs::write(package.join("adapter.toml"), format!("name='duckdb'\nversion='0.1.0'\ninterface_versions=[1]\nbinding_schema_version=1\nentrypoint={:?}\n", env!("CARGO_BIN_EXE_fixture-duckdb-build"))).unwrap();
        let root = temp.path().join("grv");
        assert_eq!(
            cli(&adapters, &["init", "--grv", root.to_str().unwrap()])["ok"],
            true
        );
        Self {
            decl: temp.path().join("build.yml"),
            state: temp.path().join("state"),
            engine: temp.path().join("engine.duckdb"),
            temp,
            adapters,
            root,
        }
    }
    fn declaration(&self, dataset: &str, build: &str, tables: &str, checks: &str) {
        fs::write(&self.decl, format!("declaration_version: 1\nkind: push\ndataset: {dataset}\nadapter: duckdb\nconnection: {{database: {:?}}}\nbuild:\n{build}tables:\n{tables}{checks}", self.engine)).unwrap();
    }
    fn push(&self, attempt: &Uuid) -> Value {
        cli(
            &self.adapters,
            &[
                "push",
                "--grv",
                self.root.to_str().unwrap(),
                "--decl",
                self.decl.to_str().unwrap(),
                "--state",
                self.state.to_str().unwrap(),
                "--attempt",
                attempt.as_str(),
            ],
        )
    }
    fn seed_two_outputs(&self) {
        self.declaration(
            "data",
            "  execution: managed\n",
            &format!(
                "{}{}",
                managed_table(
                    "alpha",
                    "SELECT * FROM (VALUES (3::BIGINT),(1::BIGINT),(2::BIGINT)) t(id)"
                ),
                managed_table("beta", "SELECT 900::BIGINT AS id")
            ),
            "",
        );
        let seeded = self.push(&Uuid::v4());
        assert_eq!(seeded["ok"], true, "{seeded}");
        assert_eq!(seeded["result"]["outcome"]["revision"], "1");
    }
    fn latest(&self, dataset: &str) -> Value {
        serde_json::from_slice(
            &fs::read(self.root.join(format!("datasets/{dataset}/.states/LATEST"))).unwrap(),
        )
        .unwrap()
    }
    fn verify(&self, dataset: &str) {
        let verified = cli(
            &self.adapters,
            &[
                "verify",
                dataset,
                "--grv",
                self.root.to_str().unwrap(),
                "--full",
            ],
        );
        assert_eq!(verified["ok"], true, "{verified}");
    }
    fn assert_current_versions(&self, dataset: &str, versions: &[(&str, &str)]) {
        let tables = cli(
            &self.adapters,
            &["ls", dataset, "--grv", self.root.to_str().unwrap()],
        );
        assert_eq!(tables["ok"], true, "{tables}");
        assert_eq!(
            tables["result"]["items"].as_array().unwrap().len(),
            versions.len()
        );
        for (table, version) in versions {
            let partitions = cli(
                &self.adapters,
                &[
                    "ls",
                    dataset,
                    "--table",
                    table,
                    "--grv",
                    self.root.to_str().unwrap(),
                ],
            );
            assert_eq!(partitions["ok"], true, "{partitions}");
            let items = partitions["result"]["items"].as_array().unwrap();
            assert_eq!(items.len(), 1, "{partitions}");
            assert_eq!(items[0]["object"]["version"], *version);
            assert_eq!(items[0]["object"]["partition"], serde_json::json!({}));
        }
    }
    fn allocation_count(&self, dataset: &str) -> usize {
        let runs = self.root.join(format!("datasets/{dataset}/.runs"));
        fs::read_dir(runs)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".allocations")
            })
            .map(|entry| fs::read_dir(entry.path()).unwrap().count())
            .sum()
    }
    fn assert_no_first_publication_or_allocations(&self, dataset: &str) {
        let latest = self.latest(dataset);
        assert_eq!(latest["revision"], 0, "{latest}");
        assert!(latest["pending"].is_null(), "{latest}");
        assert!(
            !self
                .root
                .join(format!("datasets/{dataset}/.states/revisions"))
                .exists()
        );
        assert_eq!(self.allocation_count(dataset), 0);
        for table in ["first", "second"] {
            assert!(
                !self
                    .root
                    .join(format!("datasets/{dataset}/{table}"))
                    .exists(),
                "no layout, schema, claim or version may be created before complete checked staging"
            );
        }
    }
}
fn managed_table(name: &str, sql: &str) -> String {
    format!(
        "  - name: {name}\n    source: {{sql: {sql:?}}}\n    columns: [{{name: id, type: int64}}]\n"
    )
}
fn parquet_values(path: &Path) -> Vec<i64> {
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        fs::File::open(path).unwrap(),
    )
    .unwrap()
    .build()
    .unwrap();
    reader
        .flat_map(|batch| {
            let batch = batch.unwrap();
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap();
            assert_eq!(values.null_count(), 0);
            values.values().to_vec()
        })
        .collect()
}
fn version_values(root: &Path, dataset: &str, table: &str, version: u64) -> Vec<i64> {
    let folder = root.join(format!("datasets/{dataset}/{table}/version={version}"));
    let manifest: Value =
        serde_json::from_slice(&fs::read(folder.join("manifest.json")).unwrap()).unwrap();
    let values: Vec<_> = manifest["data_files"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|file| parquet_values(&folder.join(file["name"].as_str().unwrap())))
        .collect();
    assert_eq!(manifest["row_count"], values.len());
    values
}
fn files(path: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(base: &Path, directory: &Path, result: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(base, &path, result);
            } else {
                result.insert(
                    path.strip_prefix(base).unwrap().into(),
                    fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut result = BTreeMap::new();
    visit(path, path, &mut result);
    result
}
// Test-only arbitrary local edits are deliberately made outside GRV's managed
// query guard. Discovery must rely on tracking identity metadata, then import
// the verified held GRV files rather than trust these mutable working rows.
fn native_sql(path: &Path, sql: &str) -> Vec<i64> {
    use std::ffi::{CStr, CString, c_char, c_void};
    // Exact duckdb_result layout from the pinned 1.5.6 duckdb.h. This helper
    // observes test-owned working tables, never production GRV coordination.
    #[repr(C)]
    struct NativeResult {
        column_count: u64,
        row_count: u64,
        rows_changed: u64,
        columns: *mut c_void,
        error: *mut c_char,
        internal: *mut c_void,
    }
    unsafe extern "C" {
        fn duckdb_open(path: *const c_char, database: *mut *mut c_void) -> u32;
        fn duckdb_connect(database: *mut c_void, connection: *mut *mut c_void) -> u32;
        fn duckdb_query(
            connection: *mut c_void,
            query: *const c_char,
            result: *mut NativeResult,
        ) -> u32;
        fn duckdb_result_error(result: *mut NativeResult) -> *const c_char;
        fn duckdb_column_count(result: *mut NativeResult) -> u64;
        fn duckdb_row_count(result: *mut NativeResult) -> u64;
        fn duckdb_value_int64(result: *mut NativeResult, column: u64, row: u64) -> i64;
        fn duckdb_destroy_result(result: *mut NativeResult);
        fn duckdb_disconnect(connection: *mut *mut c_void);
        fn duckdb_close(database: *mut *mut c_void);
    }
    let path = CString::new(path.to_str().unwrap()).unwrap();
    let sql = CString::new(sql).unwrap();
    let mut database = std::ptr::null_mut();
    let mut connection = std::ptr::null_mut();
    unsafe {
        assert_eq!(duckdb_open(path.as_ptr(), &mut database), 0);
        assert_eq!(duckdb_connect(database, &mut connection), 0);
        let mut result: NativeResult = std::mem::zeroed();
        let status = duckdb_query(connection, sql.as_ptr(), &mut result);
        let error = duckdb_result_error(&mut result);
        let message = if error.is_null() {
            String::new()
        } else {
            CStr::from_ptr(error).to_string_lossy().into_owned()
        };
        let rows = if status == 0 && duckdb_column_count(&mut result) == 1 {
            (0..duckdb_row_count(&mut result))
                .map(|row| duckdb_value_int64(&mut result, 0, row))
                .collect()
        } else {
            vec![]
        };
        duckdb_destroy_result(&mut result);
        duckdb_disconnect(&mut connection);
        duckdb_close(&mut database);
        assert_eq!(status, 0, "{message}");
        rows
    }
}

#[test]
fn managed_self_replaces_old_rows_and_all_queries_read_the_same_immutable_base() {
    let f = Fixture::new();
    f.seed_two_outputs();
    f.declaration(
        "data",
        "  execution: managed\n  self_input: true\n",
        &format!(
            "{}{}",
            managed_table(
                "alpha",
                "SELECT id*10 AS id FROM grv_self.alpha WHERE id<>2"
            ),
            managed_table("beta", "SELECT id+100 AS id FROM grv_self.alpha")
        ),
        "",
    );
    let built = f.push(&Uuid::v4());
    assert_eq!(built["ok"], true, "{built}");
    assert_eq!(built["result"]["outcome"]["revision"], "2");
    assert_eq!(f.latest("data")["revision"], 2);
    assert_eq!(version_values(&f.root, "data", "alpha", 2), [10, 30]);
    assert_eq!(version_values(&f.root, "data", "beta", 2), [101, 102, 103]);
    assert_eq!(version_values(&f.root, "data", "alpha", 1), [1, 2, 3]);
    assert_eq!(version_values(&f.root, "data", "beta", 1), [900]);
    f.assert_current_versions("data", &[("alpha", "2"), ("beta", "2")]);
    f.verify("data");
}

#[test]
fn managed_discovery_reconstitutes_held_input_after_tracking_rows_were_edited() {
    let f = Fixture::new();
    f.declaration(
        "raw",
        "  execution: managed\n",
        &managed_table(
            "rows",
            "SELECT * FROM (VALUES (3::BIGINT),(1::BIGINT),(2::BIGINT)) t(id)",
        ),
        "",
    );
    assert_eq!(f.push(&Uuid::v4())["ok"], true);
    fs::write(&f.decl, format!("declaration_version: 1\nkind: pull\ndataset: raw\nadapter: duckdb\nconnection: {{database: {:?}}}\ntarget: {{schema: raw}}\ntables:\n  - name: rows\n", f.engine)).unwrap();
    let pulled = cli(
        &f.adapters,
        &[
            "pull",
            "--grv",
            f.root.to_str().unwrap(),
            "--decl",
            f.decl.to_str().unwrap(),
            "--state",
            f.state.to_str().unwrap(),
        ],
    );
    assert_eq!(pulled["ok"], true, "{pulled}");
    let edited = native_sql(
        &f.engine,
        "DELETE FROM raw.rows; INSERT INTO raw.rows(id) VALUES(999); SELECT id FROM raw.rows ORDER BY id",
    );
    assert_eq!(edited, [999]);
    f.declaration(
        "derived",
        "  execution: managed\n  inputs:\n    - {table: raw.rows, as: source}\n",
        &managed_table("copied", "SELECT id FROM grv_input.source"),
        "",
    );
    let built = f.push(&Uuid::v4());
    assert_eq!(built["ok"], true, "{built}");
    assert_eq!(built["result"]["outcome"]["revision"], "1");
    assert_eq!(version_values(&f.root, "derived", "copied", 1), [1, 2, 3]);
    assert_eq!(f.latest("derived")["revision"], 1);
    let manifest: Value = serde_json::from_slice(
        &fs::read(
            f.root
                .join("datasets/derived/copied/version=1/manifest.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(manifest["derived_from"][0]["dataset"], "raw");
    assert_eq!(manifest["derived_from"][0]["revision"], 1);
    f.assert_current_versions("derived", &[("copied", "1")]);
    f.verify("derived");
}

#[test]
fn second_managed_output_sql_or_not_null_failure_creates_no_allocation_or_publication() {
    for (dataset, second, checks) in [
        (
            "sql_failure",
            "SELECT error('second output failed')::BIGINT AS id",
            "",
        ),
        (
            "check_failure",
            "SELECT NULL::BIGINT AS id",
            "checks:\n  - {table: second, not_null: [id]}\n",
        ),
    ] {
        let f = Fixture::new();
        f.declaration(
            dataset,
            "  execution: managed\n",
            &format!(
                "{}{}",
                managed_table("first", "SELECT 7::BIGINT AS id"),
                managed_table("second", second)
            ),
            checks,
        );
        let attempt = Uuid::v4();
        let failed = f.push(&attempt);
        assert_eq!(failed["ok"], false, "{failed}");
        assert_eq!(failed["errors"][0]["code"], "ENGINE_FAILURE", "{failed}");
        f.assert_no_first_publication_or_allocations(dataset);
        let record: Value = serde_json::from_slice(
            &fs::read(
                f.state
                    .join("push")
                    .join(attempt.as_str())
                    .join("journal.json"),
            )
            .unwrap(),
        )
        .unwrap();
        assert!(record["evidence"]["progress"]["publication"].is_null());
        assert!(record["evidence"]["capture"].is_null());
        if dataset == "sql_failure" {
            assert!(record["evidence"]["progress"]["accepted"].is_null());
        } else {
            assert!(
                record["evidence"]["progress"]["accepted"].is_object(),
                "check applies to completed export rather than preflight"
            );
        }
    }
}

#[test]
fn external_missing_second_completion_cannot_publish_untouched_self_seeded_output() {
    let f = Fixture::new();
    f.seed_two_outputs();
    f.declaration("data", "  execution: external\n  self_input: true\n", "  - name: alpha\n    source: {table: alpha}\n    columns: [{name: id, type: int64}]\n  - name: beta\n    source: {table: beta}\n    columns: [{name: id, type: int64}]\n", "");
    let context = f.temp.path().join("context.json");
    let prepared = cli(
        &f.adapters,
        &[
            "session",
            "prepare",
            "--session",
            context.to_str().unwrap(),
            "--decl",
            f.decl.to_str().unwrap(),
            "--grv",
            f.root.to_str().unwrap(),
            "--state",
            f.state.to_str().unwrap(),
            "--attempt",
            Uuid::v4().as_str(),
        ],
    );
    assert_eq!(prepared["ok"], true, "{prepared}");
    let session: BuildSession = serde_json::from_value(
        protected_document::read::<Value>(&context, 64 * 1024 * 1024).unwrap()["session"].clone(),
    )
    .unwrap();
    let completed = session
        .outputs
        .iter()
        .find(|o| o.table.as_str() == "alpha")
        .unwrap();
    let untouched = session
        .outputs
        .iter()
        .find(|o| o.table.as_str() == "beta")
        .unwrap();
    let before_alpha = files(&f.root.join("datasets/data/alpha"));
    let before_beta = files(&f.root.join("datasets/data/beta"));
    let before_latest = f.latest("data");
    let allocations = f.allocation_count("data");
    let store = BuildStore::open(
        &f.engine,
        f.root.to_str().unwrap().into(),
        Some(session.identity.workspace_id.clone()),
        &Resources::default(),
    )
    .unwrap();
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "external_lifecycle_driver_probe", "--nocapture"])
        .env("GRV_LIFECYCLE_DRIVER_DATABASE", &f.engine)
        .env(
            "GRV_LIFECYCLE_DRIVER_SQL",
            format!(
                "DELETE FROM {}; INSERT INTO {}(id) VALUES(77)",
                completed.engine_table, completed.engine_table
            ),
        )
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    let invocation =
        ExternalInvocation::start(store, session.clone(), Resources::default(), command).unwrap();
    let incomplete = BuildCompletion {
        result_version: Req::new(1).unwrap(),
        run_id: session.identity.run_id.clone(),
        workspace_id: session.identity.workspace_id.clone(),
        declaration_sha256: session.identity.declaration_sha256.clone(),
        kind: CompletionKind::Engine,
        invocation_id: invocation.invocation_id().as_str().into(),
        status: CompletionStatus::Succeeded,
        writers_stopped: true,
        completed_at: SystemClock::default().now(),
        completed_outputs: vec![CompletedOutput {
            table: completed.table.clone(),
            engine_table: completed.engine_table.clone(),
        }],
    };
    let completion_path = f.temp.path().join("incomplete.json");
    assert!(
        invocation
            .complete(
                CompletionKind::Engine,
                incomplete.completed_outputs.clone(),
                &completion_path,
                || Ok(true)
            )
            .is_err()
    );
    assert!(
        !completion_path.exists(),
        "trusted driver must not emit incomplete success"
    );
    assert_eq!(
        native_sql(
            &f.engine,
            &format!("SELECT id FROM {} ORDER BY id", completed.engine_table)
        ),
        [77]
    );
    assert_eq!(
        native_sql(
            &f.engine,
            &format!("SELECT id FROM {} ORDER BY id", untouched.engine_table)
        ),
        [900],
        "the uncompleted second output really contains self-input seed rows"
    );
    // Simulate a syntactically valid but semantically incomplete driver file.
    // The public CLI must independently enforce the same coverage boundary.
    incomplete.validate().unwrap();
    protected_document::publish(&completion_path, &incomplete, 2 * 1024 * 1024).unwrap();
    let rejected = cli(
        &f.adapters,
        &[
            "push",
            "--session",
            context.to_str().unwrap(),
            "--build-result",
            completion_path.to_str().unwrap(),
        ],
    );
    assert_eq!(
        rejected["errors"][0]["code"], "BUILD_INCOMPLETE",
        "{rejected}"
    );
    assert_eq!(f.latest("data"), before_latest);
    assert_eq!(f.allocation_count("data"), allocations);
    assert_eq!(files(&f.root.join("datasets/data/alpha")), before_alpha);
    assert_eq!(files(&f.root.join("datasets/data/beta")), before_beta);
    let shown = cli(
        &f.adapters,
        &["session", "show", "--session", context.to_str().unwrap()],
    );
    assert_eq!(
        shown["result"]["session"]["run"]["phase"], "open",
        "{shown}"
    );
    assert_eq!(version_values(&f.root, "data", "alpha", 1), [1, 2, 3]);
    assert_eq!(version_values(&f.root, "data", "beta", 1), [900]);
    f.assert_current_versions("data", &[("alpha", "1"), ("beta", "1")]);
}

#[test]
fn external_lifecycle_driver_probe() {
    let Some(path) = std::env::var_os("GRV_LIFECYCLE_DRIVER_DATABASE") else {
        return;
    };
    native_sql(
        Path::new(&path),
        &std::env::var("GRV_LIFECYCLE_DRIVER_SQL").unwrap(),
    );
}
