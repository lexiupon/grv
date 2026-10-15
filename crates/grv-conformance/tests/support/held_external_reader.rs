//! Independent private reader child for the held evolution gate. The parent keeps its
//! actual cooperative workspace lock and hands the child the inherited duplicate
//! descriptor; the child is an ordinary C API consumer that reads the privately mapped
//! relation through the stock signed HTTPFS path (local slice: a locally materialized
//! table, no credential at all) and publishes only its own decoded result. No exact GRV
//! connection guard is installed here: this module does not qualify or configure shipping
//! transport policy, it inherits stock behaviour and offers no deadline guarantee. Nothing
//! is deleted and no cloud cleanup is done.
use grv_adapter_api::BuildSession;
use grv_adapter_duckdb::{gcs_config, gcs_token, lock::WorkspaceLock, native};
use grv_adapter_host::protected_document;
use serde::{Deserialize, Serialize};
use std::{
    ffi::{CString, c_char, c_void},
    fs::{self, File},
    io,
    os::fd::{AsRawFd, FromRawFd, RawFd},
    os::unix::{
        fs::{MetadataExt, PermissionsExt},
        process::CommandExt,
    },
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    slice,
    time::{Duration, Instant},
};

pub(crate) const CONFIG_ENV: &str = "GRV_HELD_EXTERNAL_READER_CONFIG";
const LOCK_FD_ENV: &str = "GRV_DUCKDB_WORKSPACE_LOCK_FD";
const EXTENSIONS_ENV: &str = "GRV_DUCKDB_EXTENSIONS_DIR";
/// Fixed identity of the parent driver foundation, required on the cloud slice.
const EXPECTED_ACCOUNT: &str = "jiacao@on.sinch.com";
const EXPECTED_PROJECT: &str = "entit-finance-prod";
const CHILD_TEST: &str = "held_external_reader_child";
const LIMIT: u64 = 64 * 1024;
/// Fixed decode bound for the one varchar cell of this tiny fixture. DuckDB's own varchar
/// allocation is not bounded, so the bound is enforced before any whole copy.
const VARCHAR_LIMIT: usize = 16;
/// Decode cap and the exact expected shape of this fixture.
const ROW_CAP: u64 = 2;
const ROW_EXACT: u64 = 1;
const COLUMN_EXACT: u64 = 2;
/// The child's own credential execution budget, anchored before acquisition.
const SETUP_EXECUTION: Duration = Duration::from_secs(300);
/// Disclosed harness containment for the parent's own child wait, not a shipping limit and
/// not a native hard stop. The local slice has no credential work at all.
const LOCAL_WAIT_LIMIT: Duration = Duration::from_secs(30);
/// Trial bound for the cloud slice, whose own credential subprocess is separately bounded;
/// it stays far below the whole-gate budget and was set for the live trial only.
const CLOUD_WAIT_LIMIT: Duration = Duration::from_secs(120);

/// Closed cloud reader identity: selectors and the frozen pinned extension directory only.
/// No credential field, and no token, is admitted here or in the durable result.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CloudRead {
    root: String,
    account: String,
    project: String,
    extensions: PathBuf,
}

/// Closed local reader configuration; the cloud section is present exactly when the slice
/// runs over the real backend.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    database: String,
    relation: String,
    result_path: String,
    cloud: Option<CloudRead>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Row {
    id: i64,
    note: String,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RowResult {
    rows: Vec<Row>,
}

fn expected() -> RowResult {
    RowResult {
        rows: vec![Row {
            id: 2,
            note: "new".to_owned(),
        }],
    }
}

/// Closed-shape validation performed before any authentication: identifiers only, the
/// canonical owned child root through the shared coordinate gate, and the fixed parent
/// identity. It returns a Result so a refusal stays plain control flow.
fn validate(config: &Config) -> Result<(), ()> {
    let Some(cloud) = &config.cloud else {
        return Ok(());
    };
    gcs_config::sql_filename(&cloud.root).map_err(|_| ())?;
    if !super::root_is_owned(&cloud.root) {
        return Err(());
    }
    if cloud.account != EXPECTED_ACCOUNT || cloud.project != EXPECTED_PROJECT {
        return Err(());
    }
    let extensions = fs::canonicalize(&cloud.extensions).map_err(|_| ())?;
    if extensions != cloud.extensions || !fs::metadata(&extensions).is_ok_and(|m| m.is_dir()) {
        return Err(());
    }
    Ok(())
}

fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn quoted(relation: &str) -> String {
    let (schema, table) = relation.split_once('.').unwrap();
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
    fn duckdb_column_count(result: *mut NativeResult) -> u64;
    fn duckdb_row_count(result: *mut NativeResult) -> u64;
    fn duckdb_value_int64(result: *mut NativeResult, column: u64, row: u64) -> i64;
    fn duckdb_value_varchar(result: *mut NativeResult, column: u64, row: u64) -> *mut c_char;
    fn duckdb_value_is_null(result: *mut NativeResult, column: u64, row: u64) -> bool;
    fn duckdb_free(ptr: *mut c_void);
    fn duckdb_destroy_result(result: *mut NativeResult);
    fn duckdb_disconnect(connection: *mut *mut c_void);
    fn duckdb_close(database: *mut *mut c_void);
}

/// One stock varchar allocation, always returned to the stock allocator on every path.
struct Varchar(*mut c_char);

impl Drop for Varchar {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { duckdb_free(self.0 as *mut c_void) };
        }
    }
}

/// Query result kept at a stable address and destroyed on every path, so a closed-shape
/// refusal cannot leak it.
struct QueryResult {
    result: Box<NativeResult>,
}

impl QueryResult {
    fn columns(&mut self) -> u64 {
        unsafe { duckdb_column_count(self.result.as_mut()) }
    }

    fn rows(&mut self) -> u64 {
        unsafe { duckdb_row_count(self.result.as_mut()) }
    }

    fn is_null(&mut self, column: u64, row: u64) -> bool {
        unsafe { duckdb_value_is_null(self.result.as_mut(), column, row) }
    }

    fn int64(&mut self, column: u64, row: u64) -> i64 {
        unsafe { duckdb_value_int64(self.result.as_mut(), column, row) }
    }

    /// Bound first (`strnlen` past the limit refuses without a whole copy), then a strict
    /// UTF-8 read of exactly that length; the allocation stays owned by `Varchar`.
    fn bounded(&mut self, column: u64, row: u64) -> Result<String, ()> {
        let cell = Varchar(unsafe { duckdb_value_varchar(self.result.as_mut(), column, row) });
        if cell.0.is_null() {
            return Err(());
        }
        let length = unsafe { libc::strnlen(cell.0, VARCHAR_LIMIT + 1) };
        if length > VARCHAR_LIMIT {
            return Err(());
        }
        let bytes = unsafe { slice::from_raw_parts(cell.0 as *const u8, length) };
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| ())
    }
}

impl Drop for QueryResult {
    fn drop(&mut self) {
        unsafe { duckdb_destroy_result(self.result.as_mut()) };
    }
}

struct Engine {
    database: *mut c_void,
    connection: *mut c_void,
    /// The child's own independently acquired credential; never a parent token, never in
    /// argv, the environment, a configuration document or the durable result.
    token: Option<gcs_token::AccessToken>,
    deadline: Option<Instant>,
}

impl Engine {
    fn open(path: &Path, cloud: Option<&CloudRead>) -> Result<Self, ()> {
        let mut engine = Self {
            database: std::ptr::null_mut(),
            connection: std::ptr::null_mut(),
            token: None,
            deadline: None,
        };
        unsafe {
            assert_eq!(
                duckdb_open(
                    CString::new(path.to_str().unwrap()).unwrap().as_ptr(),
                    &mut engine.database
                ),
                0
            );
            native::load_static_extensions(engine.database).map_err(|_| ())?;
            assert_eq!(duckdb_connect(engine.database, &mut engine.connection), 0);
        }
        if let Some(cloud) = cloud {
            // The reader's own bound is anchored before the acquisition it performs; the
            // credential helper's own expiry is longer, and no lifetime is inferred from it.
            engine.deadline = Some(Instant::now() + SETUP_EXECUTION);
            engine.token = Some(
                gcs_token::acquire(&cloud.account, &cloud.project, SETUP_EXECUTION)
                    .map_err(|_| ())?,
            );
            engine.ensure_live()?;
            engine.check_deadline()?;
            let scope = gcs_config::sql_filename(&cloud.root).map_err(|_| ())?;
            // The stock pinned signed HTTPFS path: no exact GRV file/connection guard.
            for sql in [
                "SET autoinstall_known_extensions=false".to_owned(),
                "SET autoload_known_extensions=false".to_owned(),
                "SET enable_logging=false".to_owned(),
                "SET allow_persistent_secrets=false".to_owned(),
                format!(
                    "LOAD {}",
                    literal(
                        cloud
                            .extensions
                            .join("httpfs.duckdb_extension")
                            .to_str()
                            .unwrap()
                    )
                ),
                "SET enable_http_metadata_cache=false".to_owned(),
                "SET enable_external_file_cache=false".to_owned(),
            ] {
                engine.query_ok(&sql)?;
            }
            let mut bearer = literal(engine.token.as_ref().unwrap().bearer().map_err(|_| ())?);
            let sql = format!(
                "CREATE TEMPORARY SECRET grv_external_probe (TYPE HTTP, PROVIDER config, BEARER_TOKEN {}, SCOPE {}, EXTRA_HTTP_HEADERS MAP {{'x-goog-user-project': {}}})",
                bearer,
                literal(&format!("{scope}/")),
                literal(&cloud.project)
            );
            // Best effort only: the statement text and this module's own copy of the bearer
            // literal are wiped. The stock CString handed to the C API is a stock copy and is
            // not erased; nothing here claims universal heap zeroing.
            unsafe { bearer.as_bytes_mut().fill(0) };
            engine.sensitive(sql)?;
        }
        Ok(engine)
    }

    fn ensure_live(&self) -> Result<(), ()> {
        if let Some(token) = &self.token {
            token.ensure_live().map_err(|_| ())?;
        }
        Ok(())
    }

    fn check_deadline(&self) -> Result<(), ()> {
        let Some(deadline) = self.deadline else {
            return Ok(());
        };
        if Instant::now() >= deadline {
            // A late result is refused here; this is not a hard stop of stock HTTP.
            return Err(());
        }
        Ok(())
    }

    fn query_result(&mut self, sql: &str) -> Result<QueryResult, ()> {
        self.ensure_live()?;
        self.check_deadline()?;
        let sql = CString::new(sql).map_err(|_| ())?;
        // A failed statement can echo the secret-bearing setup SQL in its diagnostic, so no
        // DuckDB error text is read or printed anywhere in this module.
        let mut result = QueryResult {
            result: Box::new(unsafe { std::mem::zeroed() }),
        };
        if unsafe { duckdb_query(self.connection, sql.as_ptr(), result.result.as_mut()) } != 0 {
            return Err(());
        }
        self.ensure_live()?;
        self.check_deadline()?;
        Ok(result)
    }

    fn query_ok(&mut self, sql: &str) -> Result<(), ()> {
        self.query_result(sql).map(|_| ())
    }

    /// Best effort: the statement text that carried the credential is wiped after the stock
    /// copy has been handed over, on success and on failure alike (the CString itself is a
    /// stock copy and is not erased).
    fn sensitive(&mut self, mut sql: String) -> Result<(), ()> {
        let outcome = self.query_ok(&sql);
        unsafe { sql.as_bytes_mut().fill(0) };
        outcome
    }

    fn query_scalar_i64(&mut self, sql: &str) -> Result<i64, ()> {
        let mut result = self.query_result(sql)?;
        if result.columns() != 1 || result.rows() != 1 || result.is_null(0, 0) {
            return Err(());
        }
        let answer = result.int64(0, 0);
        self.check_deadline()?;
        Ok(answer)
    }

    /// Catalog proof for the privately mapped relation: a view on the live path, a locally
    /// materialized table on the local path.
    fn assert_catalog_kind(&mut self, cloud: bool, relation: &str) -> Result<(), ()> {
        let (catalog, field) = if cloud {
            ("duckdb_views()", "view_name")
        } else {
            ("duckdb_tables()", "table_name")
        };
        let (schema, name) = relation.split_once('.').unwrap();
        if self.query_scalar_i64(&format!(
            "SELECT count(*)::BIGINT FROM {catalog} WHERE schema_name={} AND {field}={}",
            literal(schema),
            literal(name)
        ))? != 1
        {
            return Err(());
        }
        Ok(())
    }

    /// Read only the closed shape: the exact approved column count and row count, both
    /// cells non-null, and the bounded strict note. Approved SQL casts both scalars.
    fn query_rows(&mut self, sql: &str) -> Result<Vec<Row>, ()> {
        let mut result = self.query_result(sql)?;
        let rows = result.rows();
        if rows > ROW_CAP {
            return Err(());
        }
        if result.columns() != COLUMN_EXACT || rows != ROW_EXACT {
            return Err(());
        }
        if result.is_null(0, 0) || result.is_null(1, 0) {
            return Err(());
        }
        let id = result.int64(0, 0);
        let note = result.bounded(1, 0)?;
        self.check_deadline()?;
        Ok(vec![Row { id, note }])
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        unsafe {
            if !self.connection.is_null() {
                duckdb_disconnect(&mut self.connection);
            }
            if !self.database.is_null() {
                duckdb_close(&mut self.database);
            }
        }
    }
}

/// The child entry point, reached only when the parent passed a private configuration.
pub(crate) fn child_entry() {
    let path = std::env::var_os(CONFIG_ENV).unwrap();
    let config: Config = protected_document::read(Path::new(&path), LIMIT).unwrap();
    let database = PathBuf::from(&config.database);
    let result = PathBuf::from(&config.result_path);
    // The private configuration, its local database and its result target all sit in the
    // same parent-owned private directory.
    let directory = Path::new(&path).parent().unwrap();
    assert_eq!(database.parent().unwrap(), directory);
    assert_eq!(result.parent().unwrap(), directory);
    // Closed shape and fixed identity are refused as plain control flow, before any auth.
    validate(&config).unwrap();
    let descriptor: RawFd = std::env::var(LOCK_FD_ENV).unwrap().parse().unwrap();
    assert!(descriptor > 2, "inherited descriptor must not shadow stdio");
    // The parent's duplicate is inherited explicitly; the child owns exactly one File
    // over it and keeps it until its own connection has closed.
    let inherited = unsafe { File::from_raw_fd(descriptor) };
    let metadata = inherited.metadata().unwrap();
    assert!(
        metadata.is_file()
            && metadata.nlink() == 1
            && metadata.uid() == unsafe { libc::geteuid() }
            && metadata.mode() & 0o077 == 0
    );
    // The inherited descriptor is the same protected helper object the parent owns.
    let helper = {
        let mut path = database.as_os_str().to_os_string();
        path.push(".grv-lock");
        PathBuf::from(path)
    };
    let named = fs::symlink_metadata(&helper).unwrap();
    assert!(
        named.file_type().is_file()
            && named.nlink() == 1
            && named.uid() == unsafe { libc::geteuid() }
            && named.mode() & 0o7777 == 0o600
    );
    assert_eq!((named.dev(), named.ino()), (metadata.dev(), metadata.ino()));
    let mut engine = Engine::open(&database, config.cloud.as_ref()).unwrap();
    engine
        .assert_catalog_kind(config.cloud.is_some(), &config.relation)
        .unwrap();
    let rows = engine
        .query_rows(&format!(
            "SELECT id::BIGINT, CAST(note AS VARCHAR) FROM {} ORDER BY id",
            quoted(&config.relation)
        ))
        .unwrap();
    assert_eq!(rows, expected().rows);
    drop(engine);
    // The published record carries only the actual decoded values.
    protected_document::publish(&result, &RowResult { rows }, LIMIT).unwrap();
    drop(inherited);
}

/// Parent-owned child: if the parent ever leaves the scope without a reap result, this
/// guard stops only its own still-unreaped child.
struct OwnedChild {
    child: Option<Child>,
    reaped: bool,
}

impl OwnedChild {
    fn new(child: Child) -> Self {
        Self {
            child: Some(child),
            reaped: false,
        }
    }

    fn poll(&mut self) -> io::Result<Option<ExitStatus>> {
        let status = self.child.as_mut().unwrap().try_wait()?;
        if status.is_some() {
            self.reaped = true;
        }
        Ok(status)
    }

    fn stop(&mut self) -> io::Result<ExitStatus> {
        assert!(!self.reaped, "this child was already reaped");
        let child = self.child.as_mut().unwrap();
        child.kill()?;
        let status = child.wait()?;
        self.reaped = true;
        Ok(status)
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        if self.reaped {
            return;
        }
        let Some(child) = self.child.as_mut() else {
            return;
        };
        // Only a still-unreaped child is stopped, and only this guard owns it.
        if !matches!(child.try_wait(), Ok(None)) {
            return;
        }
        let _ = child.kill();
        let _ = child.wait();
    }
}

/// Parent side: the real cooperative lock stays retained by the parent while the child runs
/// on the inherited duplicate descriptor. The busy probe below only confirms that the
/// parent's own lock is not inherited as a second child retention claim; it is a finite
/// cooperative lock, not a child causal proof. The child gets its own explicit configuration
/// and clears only known proxy variables from its environment; identity is untouched and the
/// stock reader path keeps its own defaults, so no transport policy is claimed here.
pub(crate) fn invoke(
    session: &BuildSession,
    database: &Path,
    directory: &Path,
    selectors: Option<(&str, &str)>,
) {
    fs::create_dir_all(directory).unwrap();
    fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
    let relation = session.adapter_details["input_mappings"]
        .as_array()
        .unwrap()
        .iter()
        .next()
        .unwrap()["engine_table"]
        .as_str()
        .unwrap()
        .to_owned();
    if let Some((account, project)) = selectors {
        assert_eq!(account, EXPECTED_ACCOUNT);
        assert_eq!(project, EXPECTED_PROJECT);
    }
    let cloud = selectors.map(|(account, project)| CloudRead {
        root: session.identity.root.clone(),
        account: account.to_owned(),
        project: project.to_owned(),
        extensions: fs::canonicalize(std::env::var_os(EXTENSIONS_ENV).unwrap()).unwrap(),
    });
    if let Some(cloud) = &cloud {
        assert_eq!(cloud.root, session.identity.root);
    }
    let config_path = directory.join("held-external-reader-config.json");
    let result_path = directory.join("held-external-reader-result.json");
    let config = Config {
        database: database.to_str().unwrap().to_owned(),
        relation,
        result_path: result_path.to_str().unwrap().to_owned(),
        cloud: cloud.clone(),
    };
    protected_document::publish(&config_path, &config, LIMIT).unwrap();
    let lock = WorkspaceLock::acquire(database).unwrap();
    assert_eq!(
        WorkspaceLock::acquire(database).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    let reference = lock.descendant_reference().unwrap();
    let descriptor = reference.as_raw_fd();
    assert!(descriptor > 2, "duplicate descriptor must not shadow stdio");
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", CHILD_TEST, "--test-threads=1"])
        .env(CONFIG_ENV, &config_path)
        .env(LOCK_FD_ENV, descriptor.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    for variable in [
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
    ] {
        command.env_remove(variable);
    }
    unsafe {
        command.pre_exec(move || {
            // Clear close-on-exec on the duplicate so the child inherits the lock.
            if libc::fcntl(descriptor, libc::F_SETFD, 0) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = OwnedChild::new(command.spawn().unwrap());
    // The parent keeps only the main lock; its own duplicate closes right after spawn.
    drop(reference);
    let limit = if cloud.is_some() {
        CLOUD_WAIT_LIMIT
    } else {
        LOCAL_WAIT_LIMIT
    };
    let deadline = Instant::now() + limit;
    let status: ExitStatus = loop {
        if let Some(status) = child.poll().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            // The parent owns this unreaped child only; it stops it and fails closed. The
            // child's own provider subprocess is separately bounded by the driver helper.
            let stopped = child.stop().unwrap();
            panic!("held external reader child exceeded its bounded wait ({stopped})");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    assert!(status.success(), "child must exit successfully");
    // The child has closed its connection and its own descriptor; the parent then releases
    // its retained lock and the workspace becomes acquirable again.
    drop(child);
    drop(lock);
    drop(WorkspaceLock::acquire(database).unwrap());
    assert_eq!(
        protected_document::read::<RowResult>(&result_path, LIMIT).unwrap(),
        expected()
    );
}

// Integration test modules are compiled without cfg(test); keep the oracle module live.
mod tests {
    use super::*;

    /// Closed-shape decoder oracle over a fresh in-memory database: only the approved
    /// two-column single-row shape with a bounded note decodes. Constants only; this
    /// bypasses the session mapping and proves the refusals, not any cloud path.
    #[test]
    fn local_decoder_refuses_extra_columns_rows_null_and_oversized_note() {
        let mut engine = Engine::open(Path::new(":memory:"), None).unwrap();
        assert_eq!(
            engine
                .query_rows("SELECT 2::BIGINT AS id, CAST('new' AS VARCHAR) AS note")
                .unwrap(),
            expected().rows
        );
        assert!(
            engine
                .query_rows(
                    "SELECT 2::BIGINT AS id, CAST('new' AS VARCHAR) AS note, 1::BIGINT AS extra"
                )
                .is_err(),
            "a third column must be refused"
        );
        assert!(
            engine
                .query_rows("SELECT id::BIGINT, CAST(note AS VARCHAR) AS note FROM (VALUES (2::BIGINT,'new'),(3::BIGINT,'x')) t(id,note)")
                .is_err(),
            "more rows than the fixture must be refused"
        );
        assert!(
            engine
                .query_rows("SELECT 2::BIGINT AS id, CAST('new' AS VARCHAR) AS note WHERE false")
                .is_err(),
            "no row must be refused"
        );
        assert!(
            engine
                .query_rows("SELECT 2::BIGINT AS id, CAST(NULL AS VARCHAR) AS note")
                .is_err(),
            "a null note must be refused"
        );
        assert!(
            engine
                .query_rows("SELECT 2::BIGINT AS id, CAST(repeat('x', 17) AS VARCHAR) AS note")
                .is_err(),
            "a note past the bound must be refused"
        );
    }
}
