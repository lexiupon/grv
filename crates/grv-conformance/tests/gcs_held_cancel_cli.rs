#![cfg(feature = "native-duckdb")]
//! Local-only intersection proof: a real core-confirmed hold survives a supported
//! managed native build whose owning CLI process is killed, and the same attempt is
//! refused without re-execution. The stop is caused by channel EOF after the owned
//! process died; nothing here claims a protocol Stopped frame, a hard exit, or
//! performance. The boundary is generic over the backend, so the ignored live slice
//! drives the identical body over a real `CloudBackend` with real GCS preparation and
//! real bytes; it has NOT been run here and therefore claims nothing yet. No GcsView
//! input, no native cloud read authority, no SDK/network-fault claim, and no ambient
//! environment mutation: explicit selectors are set on the spawned CLI only. The added
//! second live slice is the one that reads the input through the GcsView path, via the
//! engine-adjacent protected reader configuration carrying the explicit selectors (and
//! nothing credential-shaped); the local slice must have no such configuration.
use grv_adapter_api::{BuildSession, BuildState, Resources};
use grv_adapter_duckdb::{build::BuildStore, lock::WorkspaceLock};
use grv_adapter_host::{protected_document, validate_output};
use grv_core::{
    clock::SystemClock,
    holds::{Holds, hold_key, release_key},
    store::{InitOptions, Store},
};
use grv_storage::{
    Backend, ErrorKind, ListEntry, ListMode, LocalBackend, ObjectKey, ObjectPrefix,
    cloud::{CloudBackend, CloudOptions},
    model::{HoldRecord, RunControl, RunPhase, decode_record},
};
use grv_types::{Name, Uuid};
use serde_json::{Value, json};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

const LIMIT: u64 = 4 * 1024 * 1024;
const DISPATCH_ALLOWANCE: Duration = Duration::from_millis(75);
/// Marker-watch budget for the local case, unchanged by this pass.
const LOCAL_INTENT_BUDGET: Duration = Duration::from_secs(20);
/// Disclosed live widening, not a shipped default change and not a shipping deadline:
/// a real managed push on GCS can spend minutes of genuine cloud preparation (the
/// number of preparation requests is unknown, so this is a phase bound, not a
/// per-request delay allowance) before the single dispatch marker appears. The
/// harness-wide whole-run bound (1800s) belongs to the parent driver later.
const LIVE_INTENT_BUDGET: Duration = Duration::from_secs(600);
/// Unchanged native local-compute EOF window.
const NATIVE_EOF_BUDGET: Duration = Duration::from_secs(10);

/// Strict pure root gate: accept ONLY the canonical exclusively owned child
/// `gs://grv-gcs-held-<24 lowercase hex>/terminal` (the parent driver foundation).
/// No wildcard, prefix, or unknown-resource authority.
fn root_is_owned(root: &str) -> bool {
    root.strip_prefix("gs://grv-gcs-held-")
        .and_then(|s| s.strip_suffix("/terminal"))
        .is_some_and(|s| {
            s.len() == 24
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        })
}

static COMMAND_BASE: OnceLock<Instant> = OnceLock::new();
static COMMAND_ORDINAL: AtomicUsize = AtomicUsize::new(0);

/// Closed stage set for diagnostics only; raw argument vectors are never echoed.
fn stage_kind(args: &[&str]) -> &'static str {
    match args {
        ["init", ..] => "init",
        ["push", ..] => "push",
        ["pull", ..] => "pull",
        _ => "other",
    }
}

fn cli(adapters: &Path, args: &[&str], selectors: Option<(&str, &str)>) -> Value {
    let mut command = Command::new(env!("CARGO_BIN_EXE_grv-conformance-cli"));
    command
        .arg("--json")
        .args(args)
        .env("GRV_ADAPTERS_DIR", adapters);
    // Explicit fixed selected credentials, applied to this child only: no ambient
    // fallback, no process-wide mutation, and no GcsView/reader opt-in anywhere here.
    if let Some((account, project)) = selectors {
        command
            .env("GRV_GCS_ACCOUNT", account)
            .env("GRV_GCS_PROJECT", project);
    }
    let ordinal = COMMAND_ORDINAL.fetch_add(1, Ordering::Relaxed) + 1;
    let base = *COMMAND_BASE.get_or_init(Instant::now);
    eprintln!(
        "held cancel command: ordinal={ordinal} stage={} state=start elapsed_ms={}",
        stage_kind(args),
        Instant::now().duration_since(base).as_millis()
    );
    let started = Instant::now();
    let output = command.output().unwrap();
    eprintln!(
        "held cancel command: ordinal={ordinal} stage={} state=end duration_ms={}",
        stage_kind(args),
        started.elapsed().as_millis()
    );
    let value: Value = serde_json::from_slice(&output.stdout)
        .expect("CLI must emit authored JSON envelope; stderr suppressed");
    validate_output(&value).unwrap();
    assert_eq!(output.status.success(), value["ok"] == true);
    value
}

fn package(adapters: &Path, name: &str, executable: &str) {
    let package = adapters.join(name);
    fs::create_dir_all(&package).unwrap();
    fs::write(package.join("adapter.toml"),format!("name='{name}'\nversion={:?}\ninterface_versions=[1]\nbinding_schema_version=1\nentrypoint={:?}\n",env!("CARGO_PKG_VERSION"),executable)).unwrap();
}

fn temp() -> tempfile::TempDir {
    let temp = tempfile::tempdir_in(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap(),
    )
    .unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    temp
}

/// The root is passed through verbatim (a local directory or a canonical `gs://`
/// child); nothing here joins it with local path semantics.
fn push(
    adapters: &Path,
    root: &str,
    decl: &Path,
    state: &Path,
    attempt: &Uuid,
    selectors: Option<(&str, &str)>,
) -> Value {
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
            "--attempt",
            attempt.as_str(),
        ],
        selectors,
    )
}

fn read_record<T: grv_storage::model::Validate + serde::de::DeserializeOwned>(
    backend: &impl Backend,
    path: &str,
) -> (T, String) {
    let (bytes, meta) = backend
        .read_bytes(&ObjectKey::new(path).unwrap(), LIMIT as usize)
        .unwrap();
    (
        decode_record(&bytes).unwrap(),
        meta.validator.as_str().to_owned(),
    )
}

/// Owned CLI child. Drop only terminates a child this test still owns and has not
/// already reaped; a reap verified in the body is never re-signalled. Cleanup stays
/// per owned generation here: this test never assumes a detached owner process, and
/// the parent driver owns generation cleanup for live resources.
struct OwnedChild {
    child: Child,
    reaped: bool,
}

impl OwnedChild {
    fn running(&mut self) -> bool {
        match self.child.try_wait().unwrap() {
            None => true,
            // Exited on its own: known reaped, so Drop never re-signals it.
            Some(_) => {
                self.reaped = true;
                false
            }
        }
    }

    fn kill_and_reap(&mut self) {
        assert!(self.running(), "owned CLI exited before the kill");
        self.child.kill().unwrap();
        self.child.wait().unwrap();
        self.reaped = true;
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        if !self.reaped && self.child.try_wait().ok().is_some_and(|s| s.is_none()) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// Spawn the managed consumer build as an owned child, with the same explicit
/// child-only selectors the other CLI stages get.
fn spawn_push(
    adapters: &Path,
    root: &str,
    decl: &Path,
    state: &Path,
    attempt: &Uuid,
    selectors: Option<(&str, &str)>,
) -> OwnedChild {
    let mut command = Command::new(env!("CARGO_BIN_EXE_grv-conformance-cli"));
    command
        .arg("--json")
        .args([
            "push",
            "--grv",
            root,
            "--decl",
            decl.to_str().unwrap(),
            "--state",
            state.to_str().unwrap(),
            "--attempt",
            attempt.as_str(),
        ])
        .env("GRV_ADAPTERS_DIR", adapters)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some((account, project)) = selectors {
        command
            .env("GRV_GCS_ACCOUNT", account)
            .env("GRV_GCS_PROJECT", project);
    }
    let ordinal = COMMAND_ORDINAL.fetch_add(1, Ordering::Relaxed) + 1;
    let base = *COMMAND_BASE.get_or_init(Instant::now);
    eprintln!(
        "held cancel command: ordinal={ordinal} stage=push state=spawned elapsed_ms={}",
        Instant::now().duration_since(base).as_millis()
    );
    OwnedChild {
        child: command.spawn().unwrap(),
        reaped: false,
    }
}

/// The whole boundary, generic over the real backend. The managed source publication
/// and the full source-verification pull are real CLI stages writing real bytes
/// through the actual backend; the managed consumer holds from actual backend
/// metadata and then computes natively on a local engine file. The kill, the
/// quiescent journal, the noncontended workspace lock and the refusal of the exact
/// same attempt are identical for both wrappers.
fn case<B: Backend>(store: &Store<B>, root: &str, selectors: Option<(&str, &str)>) {
    let intent_budget = if selectors.is_some() {
        LIVE_INTENT_BUDGET
    } else {
        LOCAL_INTENT_BUDGET
    };
    let scratch = temp();
    let adapters = scratch.path().join("adapters");
    package(
        &adapters,
        "duckdb",
        env!("CARGO_BIN_EXE_fixture-duckdb-gcs-build"),
    );
    let state = scratch.path().join("state");
    let seed = scratch.path().join("seed.yml");
    let pull = scratch.path().join("pull.yml");
    let decl = scratch.path().join("consumer.yml");
    let source = scratch.path().join("source.duckdb");
    let engine = scratch.path().join("engine.duckdb");
    let executions = scratch.path().join("engine.duckdb.grv-fixture-executions");
    fs::write(&executions, []).unwrap();

    fs::write(&seed,format!("declaration_version: 1\nkind: push\ndataset: upstream\nadapter: duckdb\nconnection: {{database: {source:?}}}\nbuild: {{execution: managed}}\ntables:\n  - name: rows\n    source: {{sql: 'SELECT 1::BIGINT AS id'}}\n    columns: [{{name: id, type: int64}}]\n")).unwrap();
    fs::write(&pull,format!("declaration_version: 1\nkind: pull\ndataset: upstream\nadapter: duckdb\nconnection: {{database: {engine:?}}}\ntarget: {{schema: remote}}\ntables: [{{name: rows}}]\n")).unwrap();
    fs::write(&decl,format!("declaration_version: 1\nkind: push\ndataset: derived\nadapter: duckdb\nconnection: {{database: {engine:?}}}\nbuild:\n  execution: managed\n  inputs: [{{table: remote.rows, as: numbers}}]\ntables:\n  - name: rows\n    source: {{sql: 'SELECT CAST(sum(a.range+b.range) AS BIGINT) AS id FROM grv_input.numbers n CROSS JOIN range(1000000000) a CROSS JOIN range(1000000000) b'}}\n    columns: [{{name: id, type: int64}}]\n")).unwrap();

    let seeded = push(&adapters, root, &seed, &state, &Uuid::v4(), selectors);
    assert_eq!(seeded["ok"], true, "{seeded}");
    assert_eq!(seeded["result"]["outcome"]["revision"], "1");

    // The pull stage stays the default (non-view) one: it fully verifies the source
    // and stages the owned bytes into the local engine file.
    let pulled = cli(
        adapters.as_path(),
        &[
            "pull",
            "--grv",
            root,
            "--decl",
            pull.to_str().unwrap(),
            "--state",
            state.to_str().unwrap(),
        ],
        selectors,
    );
    assert_eq!(pulled["ok"], true, "{pulled}");

    let attempt = Uuid::v4();
    let mut owned = spawn_push(&adapters, root, &decl, &state, &attempt, selectors);
    let journal = state
        .join("push")
        .join(attempt.as_str())
        .join("journal.json");

    // Poll only the fixture's own execution counter, which the fixture appends before
    // delegating the native call: one exact line is actual dispatch of this attempt.
    // Empty means not yet; any other bytes are a foreign write. The protected journal
    // is deliberately not read here while the core writer still owns it.
    let deadline = Instant::now() + intent_budget;
    loop {
        let counter = fs::read(&executions).unwrap();
        if counter == b"execute\n" {
            break;
        }
        assert!(
            counter.is_empty(),
            "unexpected fixture counter: {counter:?}"
        );
        assert!(owned.running(), "owned CLI exited before execution intent");
        if Instant::now() >= deadline {
            panic!("execution intent deadline exceeded");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    // Bounds scheduling only; it is not evidence of native I/O entry.
    std::thread::sleep(DISPATCH_ALLOWANCE);

    owned.kill_and_reap();
    assert!(owned.reaped);

    // The owning writer closed at channel EOF, so the protected journal is quiescent.
    let record = protected_document::read::<Value>(&journal, LIMIT).unwrap();
    let progress = record["evidence"]["progress"].clone();
    assert_eq!(progress["execution_started"], Value::Bool(true));
    let session: BuildSession = serde_json::from_value(progress["session"].clone())
        .expect("confirmed journal session must parse");
    let control: RunControl = serde_json::from_value(progress["owner"]["control"].clone()).unwrap();
    assert!(control.holds_confirmed);
    assert_eq!(control.phase, RunPhase::Open);
    assert_eq!(control.inputs.len(), 1);
    assert_eq!(session.identity.run_id, control.run_id);
    // Both snapshots are taken after the CLI loss and before the one legitimate
    // recovery; no pre-loss raw-byte claim is made for the control or the hold.
    let control_path = format!("datasets/derived/.runs/{}.control.json", control.run_id);
    // Lease renewal is a legitimate mutation of the run control, so only its fixed
    // identity fields are compared below; control byte and validator equality is not
    // claimed. Hold records are immutable, so those are compared byte for byte.
    let (control_before, _) = read_record::<RunControl>(&store.backend, &control_path);
    assert!(control_before.holds_confirmed);
    assert_eq!(control_before.phase, RunPhase::Open);
    assert_eq!(control_before.inputs, control.inputs);
    let input = &control_before.inputs[0];
    let hold = HoldRecord {
        retention_id: input.retention_id.clone(),
        dataset: input.dataset.clone(),
        revision: input.revision,
        target_dataset: Name::new("derived").unwrap(),
        target_run_id: control_before.run_id.clone(),
        created_at: control_before.created_at.clone(),
    };
    let hold_path = format!(
        "datasets/upstream/.holds/derived/revision=1/{}.json",
        hold.retention_id
    );
    let hold_object = ObjectKey::new(&hold_path).unwrap();
    assert_eq!(hold_key(&hold), hold_object);
    assert_eq!(hold.dataset, Name::new("upstream").unwrap());
    assert_eq!(hold.revision.get(), 1);
    let (hold_bytes_before, hold_meta_before) = store
        .backend
        .read_bytes(&hold_object, LIMIT as usize)
        .unwrap();
    assert_eq!(
        decode_record::<HoldRecord>(&hold_bytes_before).unwrap(),
        hold
    );

    let deadline = Instant::now() + NATIVE_EOF_BUDGET;
    let lock = loop {
        match WorkspaceLock::acquire(&engine) {
            Ok(lock) => break lock,
            // Darwin reports flock contention as WouldBlock (EWOULDBLOCK is the same
            // errno alias); no other error is treated as pending.
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(_) => panic!("workspace lock error other than contention"),
        }
        if Instant::now() >= deadline {
            panic!("native worker never returned the workspace lock after EOF");
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    drop(lock);
    // The gap between releasing the polled lock and reopening the store is fine for a
    // non-adversarial private fixture; no atomic universal claim is made here.
    let mut native = BuildStore::open(
        &engine,
        session.identity.root.clone(),
        Some(session.identity.workspace_id.clone()),
        &Resources::default(),
    )
    .unwrap();
    // Counter plus durable Executing state bound actual dispatch (both precede the
    // engine transaction); core hold confirmation for this run is established by the
    // quiescent journal together with this durable record, not native I/O entry.
    let record = native.open_session(&session.identity).unwrap();
    assert_eq!(record.state, BuildState::Executing);
    assert!(record.completion.is_none());
    assert!(record.candidate.is_none());
    assert!(record.outcome.is_none());
    assert_eq!(record.session, session);
    drop(native);

    let refused = push(&adapters, root, &decl, &state, &attempt, selectors);
    assert_eq!(refused["ok"], false, "{refused}");
    assert_eq!(
        refused["errors"][0]["code"], "BUILD_INCOMPLETE",
        "{refused}"
    );
    assert_eq!(fs::read(&executions).unwrap(), b"execute\n");

    // A legitimate renewal CAS may change the control. Independently prove the
    // adapter record is still incomplete and has no completion/publication evidence.
    let mut recovered = BuildStore::open(
        &engine,
        session.identity.root.clone(),
        Some(session.identity.workspace_id.clone()),
        &Resources::default(),
    )
    .unwrap();
    let reopened = recovered.open_session(&session.identity).unwrap();
    assert_eq!(reopened.state, BuildState::Executing);
    assert_eq!(reopened.session, session);
    assert!(reopened.candidate.is_none());
    assert!(reopened.completion.is_none());
    assert!(reopened.outcome.is_none());
    drop(recovered);

    let (hold_bytes_after, hold_meta_after) = store
        .backend
        .read_bytes(&hold_object, LIMIT as usize)
        .unwrap();
    assert_eq!(hold_bytes_after, hold_bytes_before);
    assert_eq!(hold_meta_after.validator, hold_meta_before.validator);
    let (control_after, _) = read_record::<RunControl>(&store.backend, &control_path);
    assert_eq!(control_after.run_id, control_before.run_id);
    assert_eq!(control_after.owner_token, control_before.owner_token);
    assert_eq!(control_after.base_revision, control_before.base_revision);
    assert_eq!(control_after.inputs, control_before.inputs);
    assert!(control_after.holds_confirmed);
    assert_eq!(control_after.phase, RunPhase::Open);

    // The active hold survives the CLI loss because the actual hold record matches the
    // confirmed control inputs and the run's durable Executing evidence.
    let clock = SystemClock::default();
    let holds = Holds::new(store, &clock, scratch.path());
    assert!(holds.active(&hold).unwrap());
    assert!(!holds.releasable(&hold).unwrap());
    assert_eq!(
        store.backend.head(&release_key(&hold)).unwrap_err().kind,
        ErrorKind::NotFound
    );

    for entry in store
        .backend
        .list(
            &ObjectPrefix::new("datasets/derived/").unwrap(),
            ListMode::Recursive,
        )
        .unwrap()
    {
        let ListEntry::Object(key) = entry else {
            panic!("recursive inventory returned prefix");
        };
        assert!(!key.as_str().contains(".allocations/"), "{key:?}");
        assert!(!key.as_str().contains("/version="), "{key:?}");
    }
}

#[test]
fn local_confirmed_hold_survives_managed_cli_eof_and_incomplete_attempt_refusal() {
    let temp = temp();
    let adapters = temp.path().join("adapters");
    package(
        &adapters,
        "duckdb",
        env!("CARGO_BIN_EXE_fixture-duckdb-gcs-build"),
    );
    let root = temp.path().join("grv");
    assert_eq!(
        cli(&adapters, &["init", "--grv", root.to_str().unwrap()], None,)["ok"],
        true
    );
    let store = Store::open(LocalBackend::open(&root).unwrap()).unwrap();
    case(&store, root.to_str().unwrap(), None);
}

#[test]
fn root_validator_accepts_only_the_canonical_owned_child() {
    let canonical = format!("gs://grv-gcs-held-{}/terminal", "a".repeat(24));
    assert!(root_is_owned(&canonical));
    // The gate is pure and canonical on both sides: the owned child maps to exactly
    // one HTTPS object filename, with no local path joining involved.
    assert_eq!(
        grv_adapter_duckdb::gcs_config::sql_filename(&canonical).unwrap(),
        "https://storage.googleapis.com/grv-gcs-held-aaaaaaaaaaaaaaaaaaaaaaaa/terminal"
    );
    for root in [
        "gs://unknown/terminal",
        "gs://grv-gcs-held-a/terminal",
        "gs://grv-gcs-held-aaaaaaaaaaaaaaaaaaaaaaaa/terminal-extra",
        "gs://grv-gcs-held-aaaaaaaaaaaaaaaaaaaaaaaa",
        "gs://grv-gcs-held-aaaaaaaaaaaaaaaaaaaaaaaA/terminal",
        "gs://grv-gcs-held-aaaaaaaaaaaaaaaaaaaaaaaaaa/terminal",
    ] {
        assert!(!root_is_owned(root));
    }
}

/// Live slice: NOT run by this pass, no live result and no test-side bucket cleanup
/// (no deletes; the parent driver owns exact generation cleanup, the bucket-404 and
/// exclusive-child preflight, and supplies one fresh exclusively owned child root).
/// It drives the identical generic body over a real `CloudBackend` with the fixed
/// selectors passed explicitly to every stage including the spawned CLI, so the real
/// GCS preparation stays real while the native compute stays local.
fn live_case() {
    let root = std::env::var("GRV_GCS_TEST_ROOT").unwrap();
    assert!(root_is_owned(&root));
    let account = std::env::var("GRV_GCS_ACCOUNT").unwrap();
    let project = std::env::var("GRV_GCS_PROJECT").unwrap();
    assert_eq!(account, "jiacao@on.sinch.com");
    assert_eq!(project, "entit-finance-prod");
    grv_adapter_duckdb::gcs_config::sql_filename(&root).unwrap();
    let backend = CloudBackend::open(
        &root,
        CloudOptions {
            gcs_account: Some(account.clone()),
            gcs_project: Some(project.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    let (store, _) = Store::initialize(
        backend,
        InitOptions {
            max_clock_skew: Some(0),
            pending_grace: Some(1),
            ..Default::default()
        },
    )
    .unwrap();
    case(&store, &root, Some((&account, &project)));
}

#[test]
#[ignore = "requires parent-reviewed frozen artifacts and an exact exclusively owned new GCS fixture child; no test-side bucket cleanup"]
fn live_gcs_confirmed_hold_survives_managed_cli_eof_and_incomplete_attempt_refusal() {
    live_case();
}

/// Live-only GcsView slice: the same validated machinery as the local intersection
/// proof (actual source push through the bound backend, full preparation pull, real
/// confirmed core hold, managed execution on the shipping in-process adapter, owned
/// child killed and reaped so the SDK sees channel EOF), except that the input is read
/// through the GcsView path, so the engine-adjacent protected reader configuration must
/// exist. That document is version 1, strict, deny-unknown, carries only the explicit
/// environment selectors and no token: it is re-read after publication to prove no token
/// field was written, the production strict loader must resolve it to exactly those
/// selectors, and the harness diagnostics echo only closed stage kinds, never argument
/// vectors, so no token can reach a durable artifact or a log. The watched marker below
/// is actual dispatch of this attempt at the adapter boundary; it is NOT proof of native
/// I/O entry, and nothing here claims a protocol Stopped frame, a hard native stop, a
/// finite HTTP overrun window, or that an interrupt landed inside a specific read. Only
/// that dispatch happened, the loss was observed, re-execution was refused, and the
/// durable outcome resolved.
fn gcs_view_case<B: Backend>(store: &Store<B>, root: &str, account: &str, project: &str) {
    let selectors = (account, project);
    let scratch = temp();
    let adapters = scratch.path().join("adapters");
    package(
        &adapters,
        "duckdb",
        env!("CARGO_BIN_EXE_fixture-duckdb-gcs-build"),
    );
    let state = scratch.path().join("state");
    let seed = scratch.path().join("seed.yml");
    let pull = scratch.path().join("pull.yml");
    let decl = scratch.path().join("consumer.yml");
    let source = scratch.path().join("source.duckdb");
    let engine = scratch.path().join("engine.duckdb");
    let executions = scratch.path().join("engine.duckdb.grv-fixture-executions");
    let reader = PathBuf::from(format!("{}.grv-gcs-read.json", engine.display()));
    fs::write(&executions, []).unwrap();

    // Published before any stage runs, exactly as the sibling held-lifecycle gates do:
    // the fixed explicit selectors only, no token field, no ambient fallback.
    let published_document = json!({"config_version":1,"readers":[{"scope":root,"provider":"gcloud","account":account,"project":project}]});
    protected_document::publish(&reader, &published_document, LIMIT).unwrap();
    let re_read = protected_document::read::<Value>(&reader, LIMIT).unwrap();
    assert_eq!(re_read, published_document);
    let mut fields: Vec<&str> = re_read["readers"][0]
        .as_object()
        .unwrap()
        .keys()
        .map(|key| key.as_str())
        .collect();
    fields.sort_unstable();
    assert_eq!(fields, ["account", "project", "provider", "scope"]);
    assert!(
        !serde_json::to_string(&re_read)
            .unwrap()
            .to_ascii_lowercase()
            .contains("token"),
        "the durable reader configuration must stay credential-free"
    );

    fs::write(&seed,format!("declaration_version: 1\nkind: push\ndataset: upstream\nadapter: duckdb\nconnection: {{database: {source:?}}}\nbuild: {{execution: managed}}\ntables:\n  - name: rows\n    source: {{sql: 'SELECT 1::BIGINT AS id'}}\n    columns: [{{name: id, type: int64}}]\n")).unwrap();
    fs::write(&pull,format!("declaration_version: 1\nkind: pull\ndataset: upstream\nadapter: duckdb\nconnection: {{database: {engine:?}}}\ntarget: {{schema: remote}}\ntables: [{{name: rows}}]\n")).unwrap();
    fs::write(&decl,format!("declaration_version: 1\nkind: push\ndataset: derived\nadapter: duckdb\nconnection: {{database: {engine:?}}}\nbuild:\n  execution: managed\n  inputs: [{{table: remote.rows, as: numbers}}]\ntables:\n  - name: rows\n    source: {{sql: 'SELECT CAST(sum(a.range+b.range) AS BIGINT) AS id FROM grv_input.numbers n CROSS JOIN range(1000000000) a CROSS JOIN range(1000000000) b'}}\n    columns: [{{name: id, type: int64}}]\n")).unwrap();

    let seeded = push(&adapters, root, &seed, &state, &Uuid::v4(), Some(selectors));
    assert_eq!(seeded["ok"], true, "{seeded}");
    assert_eq!(seeded["result"]["outcome"]["revision"], "1");

    let pulled = cli(
        adapters.as_path(),
        &[
            "pull",
            "--grv",
            root,
            "--decl",
            pull.to_str().unwrap(),
            "--state",
            state.to_str().unwrap(),
        ],
        Some(selectors),
    );
    assert_eq!(pulled["ok"], true, "{pulled}");

    // The production strict loader must resolve the published protected document to
    // exactly the explicit selectors; this is the reader authority the view path uses.
    let resolved = grv_adapter_duckdb::gcs_config::load_reader(&engine, root).unwrap();
    assert_eq!(resolved.scope, root);
    assert_eq!(resolved.provider, "gcloud");
    assert_eq!(resolved.account, account);
    assert_eq!(resolved.project, project);

    let attempt = Uuid::v4();
    let mut owned = spawn_push(&adapters, root, &decl, &state, &attempt, Some(selectors));
    let journal = state
        .join("push")
        .join(attempt.as_str())
        .join("journal.json");

    // The fixture appends its own counter line when this attempt is dispatched to the
    // adapter, so one exact line means actual dispatch of this attempt: it does NOT
    // prove entry into a native read. Empty means not yet; any other bytes are foreign.
    let deadline = Instant::now() + LIVE_INTENT_BUDGET;
    loop {
        let counter = fs::read(&executions).unwrap();
        if counter == b"execute\n" {
            break;
        }
        assert!(
            counter.is_empty(),
            "unexpected fixture counter: {counter:?}"
        );
        assert!(owned.running(), "owned CLI exited before execution intent");
        if Instant::now() >= deadline {
            panic!("execution intent deadline exceeded");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    // Bounds scheduling only; it is not evidence of native I/O entry.
    std::thread::sleep(DISPATCH_ALLOWANCE);

    owned.kill_and_reap();
    assert!(owned.reaped);

    // The protected journal is only read once it is quiescent: the owning writer closed
    // at channel EOF and the workspace lock is acquirable again first.
    let deadline = Instant::now() + NATIVE_EOF_BUDGET;
    let lock = loop {
        match WorkspaceLock::acquire(&engine) {
            Ok(lock) => break lock,
            // Darwin reports flock contention as WouldBlock (EWOULDBLOCK is the same
            // errno alias); no other error is treated as pending.
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(_) => panic!("workspace lock error other than contention"),
        }
        if Instant::now() >= deadline {
            panic!("native worker never returned the workspace lock after EOF");
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    drop(lock);
    let record = protected_document::read::<Value>(&journal, LIMIT).unwrap();
    let progress = record["evidence"]["progress"].clone();
    assert_eq!(progress["execution_started"], Value::Bool(true));
    let session: BuildSession = serde_json::from_value(progress["session"].clone())
        .expect("confirmed journal session must parse");
    let control: RunControl = serde_json::from_value(progress["owner"]["control"].clone()).unwrap();
    assert!(control.holds_confirmed);
    assert_eq!(control.phase, RunPhase::Open);
    assert_eq!(control.inputs.len(), 1);
    assert_eq!(session.identity.run_id, control.run_id);

    // Both snapshots below are taken after the loss and before the one legitimate
    // recovery; no pre-loss raw-byte claim is made for the control or the hold.
    let control_path = format!("datasets/derived/.runs/{}.control.json", control.run_id);
    let (control_before, _) = read_record::<RunControl>(&store.backend, &control_path);
    assert!(control_before.holds_confirmed);
    assert_eq!(control_before.phase, RunPhase::Open);
    assert_eq!(control_before.inputs, control.inputs);
    let hold = HoldRecord {
        retention_id: control_before.inputs[0].retention_id.clone(),
        dataset: control_before.inputs[0].dataset.clone(),
        revision: control_before.inputs[0].revision,
        target_dataset: Name::new("derived").unwrap(),
        target_run_id: control_before.run_id.clone(),
        created_at: control_before.created_at.clone(),
    };
    let hold_object = ObjectKey::new(format!(
        "datasets/upstream/.holds/derived/revision=1/{}.json",
        hold.retention_id
    ))
    .unwrap();
    assert_eq!(hold_key(&hold), hold_object);
    assert_eq!(hold.dataset, Name::new("upstream").unwrap());
    assert_eq!(hold.revision.get(), 1);
    let (hold_bytes_before, hold_meta_before) = store
        .backend
        .read_bytes(&hold_object, LIMIT as usize)
        .unwrap();
    assert_eq!(
        decode_record::<HoldRecord>(&hold_bytes_before).unwrap(),
        hold
    );

    // Durable Executing state on the shipping in-process path, bound to the single
    // dispatch marker: together with the quiescent journal this is the nonvacuous
    // evidence that this attempt was dispatched, not that a native read was entered.
    // The gap between releasing the polled lock and reopening the store is fine for a
    // non-adversarial private fixture; no atomic universal claim is made here.
    let mut native = BuildStore::open(
        &engine,
        session.identity.root.clone(),
        Some(session.identity.workspace_id.clone()),
        &Resources::default(),
    )
    .unwrap();
    let dispatched = native.open_session(&session.identity).unwrap();
    assert_eq!(dispatched.state, BuildState::Executing);
    assert!(dispatched.candidate.is_none());
    assert!(dispatched.completion.is_none());
    assert!(dispatched.outcome.is_none());
    assert_eq!(dispatched.session, session);
    drop(native);

    let refused = push(&adapters, root, &decl, &state, &attempt, Some(selectors));
    assert_eq!(refused["ok"], false, "{refused}");
    assert_eq!(
        refused["errors"][0]["code"], "BUILD_INCOMPLETE",
        "{refused}"
    );
    // Exactly one dispatch line survives the refused retry: no second execution attempt
    // ever reached the adapter, so no retried effects were produced.
    assert_eq!(fs::read(&executions).unwrap(), b"execute\n");

    // Recovery reopens the same session; a legitimate renewal CAS may change run-control
    // expiry/mutation fields, so only its fixed typed identity is compared here.
    let mut recovered = BuildStore::open(
        &engine,
        session.identity.root.clone(),
        Some(session.identity.workspace_id.clone()),
        &Resources::default(),
    )
    .unwrap();
    let reopened = recovered.open_session(&session.identity).unwrap();
    assert_eq!(reopened.state, BuildState::Executing);
    assert_eq!(reopened.session, session);
    assert!(reopened.candidate.is_none());
    assert!(reopened.completion.is_none());
    assert!(reopened.outcome.is_none());
    drop(recovered);

    // Hold bytes and validator are invariant after the loss and across recovery; this is
    // not a pre/post-loss equality claim, because nothing was read before the loss.
    let (hold_bytes_after, hold_meta_after) = store
        .backend
        .read_bytes(&hold_object, LIMIT as usize)
        .unwrap();
    assert_eq!(hold_bytes_after, hold_bytes_before);
    assert_eq!(hold_meta_after.validator, hold_meta_before.validator);
    let (control_post_recovery, _) = read_record::<RunControl>(&store.backend, &control_path);
    // Only the fixed typed identity is compared across recovery; a legitimate renewal CAS
    // may change expiry and mutation fields, so whole-byte equality is not claimed.
    let fixed_identity = |c: &RunControl| {
        (
            c.run_id.clone(),
            c.owner_token.clone(),
            c.base_revision,
            c.inputs.clone(),
            c.phase,
            c.holds_confirmed,
        )
    };
    assert_eq!(
        fixed_identity(&control_post_recovery),
        fixed_identity(&control_before)
    );
    for control in [&control_before, &control_post_recovery] {
        assert_eq!(control.run_id, session.identity.run_id);
        assert_eq!(control.base_revision, control_before.base_revision);
        assert_eq!(control.inputs.len(), 1);
        assert_eq!(control.inputs[0].dataset, Name::new("upstream").unwrap());
        assert_eq!(control.inputs[0].revision.get(), 1);
        assert!(control.holds_confirmed);
        assert_eq!(control.phase, RunPhase::Open);
    }

    // The real core hold for the current revision is still active and unreleasable: holds
    // never expire and only a committed source-GC/Admin operation may release one, so
    // there is no release marker and no `.pruned` tombstone on the held revision.
    let clock = SystemClock::default();
    let holds = Holds::new(store, &clock, scratch.path());
    assert!(holds.active(&hold).unwrap());
    assert!(!holds.releasable(&hold).unwrap());
    assert_eq!(
        store.backend.head(&release_key(&hold)).unwrap_err().kind,
        ErrorKind::NotFound
    );
    assert_eq!(
        store
            .backend
            .head(&ObjectKey::new("datasets/upstream/rows/version=1/.pruned").unwrap())
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );

    // The refused attempt published nothing on the consumer side either.
    for entry in store
        .backend
        .list(
            &ObjectPrefix::new("datasets/derived/").unwrap(),
            ListMode::Recursive,
        )
        .unwrap()
    {
        let ListEntry::Object(key) = entry else {
            panic!("recursive inventory returned prefix");
        };
        assert!(!key.as_str().contains(".allocations/"), "{key:?}");
        assert!(!key.as_str().contains("/version="), "{key:?}");
        assert!(!key.as_str().contains(".pruned"), "{key:?}");
    }
}

/// Live slice only: no live result from this pass, no test-side bucket cleanup and no
/// ambient environment mutation. The parent driver owns generation cleanup and supplies
/// one fresh exclusively owned canonical child root plus the fixed selectors, which are
/// passed explicitly to every stage including the spawned CLI child.
#[test]
#[ignore = "requires parent-reviewed frozen artifacts and an exact exclusively owned new GCS fixture child; no test-side bucket cleanup"]
fn live_gcs_held_gcs_view_cancellation_refuses_reexecution_after_supported_interrupt() {
    let root = std::env::var("GRV_GCS_TEST_ROOT").unwrap();
    assert!(root_is_owned(&root));
    let account = std::env::var("GRV_GCS_ACCOUNT").unwrap();
    let project = std::env::var("GRV_GCS_PROJECT").unwrap();
    assert_eq!(account, "jiacao@on.sinch.com");
    assert_eq!(project, "entit-finance-prod");
    grv_adapter_duckdb::gcs_config::sql_filename(&root).unwrap();
    let backend = CloudBackend::open(
        &root,
        CloudOptions {
            gcs_account: Some(account.clone()),
            gcs_project: Some(project.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    let (store, _) = Store::initialize(
        backend,
        InitOptions {
            max_clock_skew: Some(0),
            pending_grace: Some(1),
            ..Default::default()
        },
    )
    .unwrap();
    gcs_view_case(&store, &root, &account, &project);
}
