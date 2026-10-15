#![cfg(feature = "native-duckdb")]
//! Finite terminal hold lifecycle through actual CLI. The parent alone owns
//! bucket admission and all-version/generation-qualified cleanup; no best-effort
//! Drop deletion and no mutation of unknown cloud resources.
use grv_adapter_api::BuildSession;
use grv_adapter_host::{protected_document, validate_output};
use grv_core::{
    clock::SystemClock,
    holds::{Holds, hold_key, release_key},
    store::{InitOptions, Store},
};
use grv_storage::{
    Backend, ErrorKind, ListEntry, ListMode, ObjectKey, ObjectPrefix,
    cloud::{CloudBackend, CloudOptions},
    model::{HoldRecord, HoldReleaseMarker, RunControl, RunPhase, decode_record},
};
use grv_types::{Name, Uuid};
use serde_json::{Value, json};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};
const LIMIT: u64 = 4 * 1024 * 1024;
fn root_is_owned(root: &str) -> bool {
    root.strip_prefix("gs://grv-gcs-held-")
        .and_then(|s| s.strip_suffix("/terminal"))
        .is_some_and(|s| {
            s.len() == 24
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        })
}
fn cli(adapters: &Path, args: &[&str], selectors: Option<(&str, &str)>) -> Value {
    let mut c = Command::new(env!("CARGO_BIN_EXE_grv-conformance-cli"));
    c.arg("--json")
        .args(args)
        .env("GRV_ADAPTERS_DIR", adapters)
        .env("GRV_CONFORMANCE_GCS_VIEW", "yes");
    if let Some((a, p)) = selectors {
        c.env("GRV_GCS_ACCOUNT", a).env("GRV_GCS_PROJECT", p);
    }
    let ordinal = COMMAND_ORDINAL.fetch_add(1, Ordering::Relaxed) + 1;
    let base = *COMMAND_BASE.get_or_init(Instant::now);
    let started = Instant::now();
    let elapsed = started.duration_since(base).as_millis();
    eprintln!(
        "{}",
        command_line(ordinal, args, "start", "elapsed_ms", elapsed)
    );
    let output = c.output().unwrap();
    let duration = started.elapsed().as_millis();
    eprintln!(
        "{}",
        command_line(ordinal, args, "end", "duration_ms", duration)
    );
    let value: Value = serde_json::from_slice(&output.stdout)
        .expect("CLI must emit authored JSON envelope; stderr suppressed");
    validate_output(&value).unwrap();
    assert_eq!(output.status.success(), value["ok"] == true);
    value
}
/// Test-global monotonic base plus per-command ordinal; diagnostics only and
/// never authority, args, paths, env or provider output.
static COMMAND_BASE: OnceLock<Instant> = OnceLock::new();
static COMMAND_ORDINAL: AtomicUsize = AtomicUsize::new(0);

/// Known stage kinds, decided from the first two args only; anything else is
/// `other` so no unrecognized value is ever echoed.
fn stage_kind(args: &[&str]) -> &'static str {
    match args {
        ["session", "prepare", ..] => "session_prepare",
        ["session", "abort", ..] => "session_abort",
        ["push", ..] => "push",
        ["pull", ..] => "pull",
        ["gc", ..] => "gc",
        _ => "other",
    }
}

/// One chronological diagnostic line: ordinal, stage kind, distinct phase and
/// monotonic millis. No identity or private values.
fn command_line(ordinal: usize, args: &[&str], phase: &str, field: &str, millis: u128) -> String {
    format!(
        "held terminal command: ordinal={ordinal} stage={} state={phase} {field}={millis}",
        stage_kind(args)
    )
}

fn read_record<T: grv_storage::model::Validate + serde::de::DeserializeOwned>(
    b: &impl Backend,
    path: &str,
) -> T {
    decode_record(
        &b.read_bytes(&ObjectKey::new(path).unwrap(), LIMIT as usize)
            .unwrap()
            .0,
    )
    .unwrap()
}
fn absent(b: &impl Backend, path: &str) {
    assert_eq!(
        b.head(&ObjectKey::new(path).unwrap()).unwrap_err().kind,
        ErrorKind::NotFound
    );
}
fn latest(b: &impl Backend, dataset: &str) -> Value {
    serde_json::from_slice(
        &b.read_bytes(
            &ObjectKey::new(format!("datasets/{dataset}/.states/LATEST")).unwrap(),
            LIMIT as usize,
        )
        .unwrap()
        .0,
    )
    .unwrap()
}
fn no_derived_output(b: &impl Backend) {
    for entry in b
        .list(
            &ObjectPrefix::new("datasets/derived/").unwrap(),
            ListMode::Recursive,
        )
        .unwrap()
    {
        let ListEntry::Object(key) = entry else {
            panic!("recursive inventory returned prefix")
        };
        assert!(
            !key.as_str().contains(".allocations/"),
            "terminal prepared run must not allocate"
        );
        assert!(
            !key.as_str().contains("/version="),
            "terminal prepared run must not create output version"
        );
    }
}
/// Independent test lifecycles carved out of the full matrix. `Full` is the
/// original single-continuity path and stays untouched; the two scoped slices
/// each prove one gate on their own fixture so neither claims the full
/// original continuity.
#[derive(Clone, Copy)]
enum LifecycleSlice {
    Full,
    ProtectOnly,
    ReleaseOnly,
}

fn lifecycle<B: Backend>(store: &Store<B>, root: &str, selectors: Option<(&str, &str)>) {
    lifecycle_slice(store, root, selectors, LifecycleSlice::Full);
}

fn lifecycle_slice<B: Backend>(
    store: &Store<B>,
    root: &str,
    selectors: Option<(&str, &str)>,
    slice: LifecycleSlice,
) {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let temp = tempfile::tempdir_in(repository).unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let package = temp.path().join("adapters/duckdb");
    fs::create_dir_all(&package).unwrap();
    let adapters = package.parent().unwrap();
    fs::write(package.join("adapter.toml"),format!("name='duckdb'\nversion={:?}\ninterface_versions=[1]\nbinding_schema_version=1\nentrypoint={:?}\n",env!("CARGO_PKG_VERSION"),env!("CARGO_BIN_EXE_fixture-duckdb-gcs-build"))).unwrap();
    let state = temp.path().join("state");
    let source = temp.path().join("source.duckdb");
    let engine = temp.path().join("consumer.duckdb");
    let seed = temp.path().join("seed.yml");
    let pull = temp.path().join("pull.yml");
    let decl = temp.path().join("external.yml");
    let context = temp.path().join("context.json");
    if let Some((a, p)) = selectors {
        protected_document::publish(&PathBuf::from(format!("{}.grv-gcs-read.json",engine.display())),&json!({"config_version":1,"readers":[{"scope":root,"provider":"gcloud","account":a,"project":p}]}),LIMIT).unwrap();
    }
    let push = |value: u64| {
        fs::write(&seed,format!("declaration_version: 1\nkind: push\ndataset: upstream\nadapter: duckdb\nconnection: {{database: {source:?}}}\nbuild: {{execution: managed}}\ntables:\n  - name: rows\n    source: {{sql: 'SELECT {value}::BIGINT AS id'}}\n    columns: [{{name: id, type: int64}}]\n")).unwrap();
        let v = cli(
            adapters,
            &[
                "push",
                "--grv",
                root,
                "--decl",
                seed.to_str().unwrap(),
                "--state",
                state.to_str().unwrap(),
            ],
            selectors,
        );
        assert_eq!(v["ok"], true, "{v}");
        v
    };
    assert_eq!(push(1)["result"]["outcome"]["revision"], "1");
    let options = if selectors.is_some() {
        "options: {materialization: gcs-view, refresh: auto}\n"
    } else {
        ""
    };
    fs::write(&pull,format!("declaration_version: 1\nkind: pull\ndataset: upstream\nadapter: duckdb\nconnection: {{database: {engine:?}}}\ntarget: {{schema: remote}}\n{options}tables: [{{name: rows}}]\n")).unwrap();
    let v = cli(
        adapters,
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
    assert_eq!(v["ok"], true, "{v}");
    fs::write(&decl,format!("declaration_version: 1\nkind: push\ndataset: derived\nadapter: duckdb\nconnection: {{database: {engine:?}}}\nbuild:\n  execution: external\n  inputs: [{{table: remote.rows, as: numbers}}]\ntables:\n  - name: rows\n    source: {{table: rows}}\n    columns: [{{name: id, type: int64}}]\n")).unwrap();
    let attempt = Uuid::v4();
    let prepared = cli(
        adapters,
        &[
            "session",
            "prepare",
            "--session",
            context.to_str().unwrap(),
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
    );
    assert_eq!(prepared["ok"], true, "{prepared}");
    let session: BuildSession = serde_json::from_value(
        protected_document::read::<Value>(&context, LIMIT).unwrap()["session"].clone(),
    )
    .unwrap();
    let journal = state
        .join("push")
        .join(attempt.as_str())
        .join("journal.json");
    let evidence: Value = protected_document::read(&journal, LIMIT).unwrap();
    let control: RunControl =
        serde_json::from_value(evidence["evidence"]["progress"]["owner"]["control"].clone())
            .unwrap();
    assert!(control.holds_confirmed);
    assert_eq!(control.phase, RunPhase::Open);
    assert_eq!(control.inputs.len(), 1);
    assert_eq!(session.inputs[0].revision.get(), 1);
    let input = &control.inputs[0];
    assert_eq!(input.dataset, Name::new("upstream").unwrap());
    assert_eq!(input.revision.get(), 1);
    let hold_path = format!(
        "datasets/upstream/.holds/derived/revision=1/{}.json",
        input.retention_id
    );
    let hold: HoldRecord = read_record(&store.backend, &hold_path);
    assert_eq!(hold.target_run_id, session.identity.run_id);
    assert_eq!(hold_key(&hold).as_str(), hold_path);
    let clock = SystemClock::default();
    let holds = Holds::new(store, &clock, temp.path());
    assert!(holds.active(&hold).unwrap());
    assert!(!holds.releasable(&hold).unwrap());
    absent(&store.backend, release_key(&hold).as_str());
    no_derived_output(&store.backend);
    let manifest_key = ObjectKey::new("datasets/upstream/rows/version=1/manifest.json").unwrap();
    let old_manifest = store
        .backend
        .read_bytes(&manifest_key, LIMIT as usize)
        .unwrap()
        .0;
    let manifest: Value = serde_json::from_slice(&old_manifest).unwrap();
    let old_data: Vec<_> = manifest["data_files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| {
            ObjectKey::new(format!(
                "datasets/upstream/rows/version=1/{}",
                f["name"].as_str().unwrap()
            ))
            .unwrap()
        })
        .collect();
    assert!(!old_data.is_empty());
    assert_eq!(push(2)["result"]["outcome"]["revision"], "2");
    std::thread::sleep(std::time::Duration::from_secs(2));
    let gc = || {
        let v = cli(
            adapters,
            &[
                "gc",
                "upstream",
                "--grv",
                root,
                "--state",
                state.to_str().unwrap(),
                "--apply",
            ],
            selectors,
        );
        assert_eq!(v["ok"], true, "{v}");
        v
    };
    // The first GC is the protect proof. `ReleaseOnly` skips exactly this GC
    // and its first-GC-only assertions; the post-seal GC is its first pass.
    if !matches!(slice, LifecycleSlice::ReleaseOnly) {
        gc();
    }
    assert!(holds.active(&hold).unwrap());
    assert!(!holds.releasable(&hold).unwrap());
    assert_eq!(
        store
            .backend
            .read_bytes(&manifest_key, LIMIT as usize)
            .unwrap()
            .0,
        old_manifest
    );
    if !matches!(slice, LifecycleSlice::ReleaseOnly) {
        for key in &old_data {
            store.backend.head(key).unwrap();
        }
        absent(&store.backend, "datasets/upstream/rows/version=1/.pruned");
    }
    absent(&store.backend, release_key(&hold).as_str());
    let abort = cli(
        adapters,
        &["session", "abort", "--session", context.to_str().unwrap()],
        selectors,
    );
    assert_eq!(abort["ok"], true, "{abort}");
    let sealed: RunControl = read_record(
        &store.backend,
        &format!("datasets/derived/.runs/{}.control.json", control.run_id),
    );
    assert_eq!(sealed.phase, RunPhase::Sealed);
    assert!(sealed.holds_confirmed);
    assert!(holds.active(&hold).unwrap());
    assert!(holds.releasable(&hold).unwrap());
    absent(&store.backend, release_key(&hold).as_str());
    no_derived_output(&store.backend);
    match store.backend.read_bytes(
        &ObjectKey::new("datasets/derived/.states/LATEST").unwrap(),
        LIMIT as usize,
    ) {
        Ok((bytes, _)) => assert_eq!(
            serde_json::from_slice::<Value>(&bytes).unwrap()["revision"],
            0
        ),
        Err(e) => assert_eq!(e.kind, ErrorKind::NotFound),
    }
    let abort_again = cli(
        adapters,
        &["session", "abort", "--session", context.to_str().unwrap()],
        selectors,
    );
    assert_eq!(abort_again["ok"], true, "{abort_again}");
    assert_eq!(
        read_record::<RunControl>(
            &store.backend,
            &format!("datasets/derived/.runs/{}.control.json", control.run_id)
        ),
        sealed
    );
    if matches!(slice, LifecycleSlice::ProtectOnly) {
        // Stop before the source-GC release: the hold is still protected and
        // stays active for parent-owned cleanup. Nothing here claims a source
        // protocol release, so the release marker must still be absent.
        assert!(holds.active(&hold).unwrap());
        absent(&store.backend, release_key(&hold).as_str());
        return;
    }
    gc();
    assert!(!holds.active(&hold).unwrap());
    let release = release_key(&hold);
    let (released_bytes, released_metadata) =
        store.backend.read_bytes(&release, LIMIT as usize).unwrap();
    let marker: HoldReleaseMarker = decode_record(&released_bytes).unwrap();
    assert_eq!(marker.retention_id, hold.retention_id);
    assert_eq!(read_record::<HoldRecord>(&store.backend, &hold_path), hold);
    let pruned: grv_storage::model::PrunedMarker =
        read_record(&store.backend, "datasets/upstream/rows/version=1/.pruned");
    assert_eq!(pruned.table, Name::new("rows").unwrap());
    assert_eq!(pruned.version.get(), 1);
    // Marker references must have an actual committed source operation, not
    // merely an independently writable marker. Preserve exact operation bytes.
    let release_operation = ObjectKey::new(format!(
        "datasets/upstream/.states/operations/{}.json",
        marker.operation_id
    ))
    .unwrap();
    let release_operation_bytes = store
        .backend
        .read_bytes(&release_operation, LIMIT as usize)
        .unwrap()
        .0;
    let operation: grv_storage::model::OperationRecord =
        decode_record(&release_operation_bytes).unwrap();
    let grv_storage::model::OperationPayload::ReleaseHold(payload) = operation.body else {
        panic!("release marker must reference a source hold-release operation");
    };
    assert_eq!(
        payload.releases,
        vec![grv_storage::model::HoldRelease {
            consumer_dataset: Name::new("derived").unwrap(),
            revision: hold.revision,
            retention_id: hold.retention_id.clone(),
        }]
    );
    let _: grv_storage::model::OperationRecord = read_record(
        &store.backend,
        &format!(
            "datasets/upstream/.states/operations/{}.json",
            pruned.operation_id
        ),
    );
    store
        .backend
        .head(&ObjectKey::new("datasets/upstream/rows/version=2/manifest.json").unwrap())
        .unwrap();
    assert_eq!(latest(&store.backend, "upstream")["revision"], 2);
    gc();
    let (again, metadata) = store.backend.read_bytes(&release, LIMIT as usize).unwrap();
    assert_eq!(again, released_bytes);
    assert_eq!(metadata.validator, released_metadata.validator);
    assert_eq!(
        store
            .backend
            .read_bytes(&release_operation, LIMIT as usize)
            .unwrap()
            .0,
        release_operation_bytes
    );
    assert_eq!(latest(&store.backend, "upstream")["revision"], 2);
    assert!(!holds.active(&hold).unwrap());
    no_derived_output(&store.backend);
}
#[test]
fn held_terminal_owned_root_gate_is_exact() {
    assert!(root_is_owned(&format!(
        "gs://grv-gcs-held-{}/terminal",
        "a".repeat(24)
    )));
    for root in [
        "gs://unknown/terminal",
        "gs://grv-gcs-held-a/terminal",
        "gs://grv-gcs-held-aaaaaaaaaaaaaaaaaaaaaaaa/terminal-extra",
        "gs://grv-gcs-held-aaaaaaaaaaaaaaaaaaaaaaaa",
    ] {
        assert!(!root_is_owned(root));
    }
}
#[test]
fn command_diagnostic_lines_carry_no_args() {
    assert_eq!(
        command_line(
            1,
            &["push", "--grv", "gs://bucket/dataset"],
            "start",
            "elapsed_ms",
            0
        ),
        "held terminal command: ordinal=1 stage=push state=start elapsed_ms=0"
    );
    assert_eq!(
        command_line(
            2,
            &["session", "prepare", "--session", "/p"],
            "end",
            "duration_ms",
            3
        ),
        "held terminal command: ordinal=2 stage=session_prepare state=end duration_ms=3"
    );
    assert_eq!(
        command_line(3, &["gc", "upstream", "--apply"], "end", "duration_ms", 4),
        "held terminal command: ordinal=3 stage=gc state=end duration_ms=4"
    );
    let unknown = command_line(
        4,
        &["session", "forget", "--session", "/p"],
        "start",
        "elapsed_ms",
        5,
    );
    assert_eq!(
        unknown,
        "held terminal command: ordinal=4 stage=other state=start elapsed_ms=5"
    );
    assert!(!unknown.contains("forget") && !unknown.contains('/'));
}
fn local_slice(slice: LifecycleSlice) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("grv");
    let (store, _) = Store::initialize(
        grv_storage::LocalBackend::create(&root).unwrap(),
        InitOptions {
            max_clock_skew: Some(0),
            pending_grace: Some(1),
            ..Default::default()
        },
    )
    .unwrap();
    lifecycle_slice(&store, root.to_str().unwrap(), None, slice);
}
#[test]
fn local_confirmed_hold_protect_then_abort() {
    local_slice(LifecycleSlice::ProtectOnly);
}
#[test]
fn local_sealed_hold_source_gc_release_prune_and_repeat() {
    local_slice(LifecycleSlice::ReleaseOnly);
}
#[test]
fn local_confirmed_hold_abort_then_source_gc_release_and_prune() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("grv");
    let (store, _) = Store::initialize(
        grv_storage::LocalBackend::create(&root).unwrap(),
        InitOptions {
            max_clock_skew: Some(0),
            pending_grace: Some(1),
            ..Default::default()
        },
    )
    .unwrap();
    lifecycle(&store, root.to_str().unwrap(), None);
}
fn live_slice(slice: LifecycleSlice) {
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
    lifecycle_slice(&store, &root, Some((&account, &project)), slice);
}
#[test]
#[ignore = "requires parent-reviewed frozen artifacts and exact exclusively owned GCS terminal child; no test-side bucket cleanup"]
fn live_gcs_confirmed_hold_protect_then_abort() {
    live_slice(LifecycleSlice::ProtectOnly);
}
#[test]
#[ignore = "requires parent-reviewed frozen artifacts and exact exclusively owned GCS terminal child; no test-side bucket cleanup"]
fn live_gcs_sealed_hold_source_gc_release_prune_and_repeat() {
    live_slice(LifecycleSlice::ReleaseOnly);
}
#[test]
#[ignore = "requires parent-reviewed frozen artifacts and exact exclusively owned GCS terminal child; no test-side bucket cleanup"]
fn live_gcs_confirmed_hold_abort_then_source_gc_release_and_prune() {
    live_slice(LifecycleSlice::Full);
}
