#![cfg(feature = "native-duckdb")]
use grv_adapter_api::{Mode, NamedContract, PullPlan, Receipt, RequestRecord, TableCount};
use grv_adapter_host::registry::CheckedRegistry;
use grv_adapter_sdk::Adapter;
use grv_core::journal::{Envelope, Evidence, Journal};
use grv_types::{
    AdapterIdentity, Column, DeclarationIdentity, LatestRevision, Name, PullRequestIdentity, Req,
    RequestedRevision, TableContract, Timestamp, U64, Uuid,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
};
#[derive(Debug, PartialEq, Eq)]
enum SnapshotEntry {
    Directory,
    File(Vec<u8>),
    Link(PathBuf),
}
fn snapshot(base: &Path) -> Vec<(PathBuf, SnapshotEntry)> {
    fn visit(base: &Path, dir: &Path, out: &mut Vec<(PathBuf, SnapshotEntry)>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let p = entry.unwrap().path();
            let relative = p.strip_prefix(base).unwrap().to_owned();
            let metadata = std::fs::symlink_metadata(&p).unwrap();
            if metadata.file_type().is_symlink() {
                out.push((
                    relative,
                    SnapshotEntry::Link(std::fs::read_link(&p).unwrap()),
                ));
            } else if metadata.is_dir() {
                out.push((relative, SnapshotEntry::Directory));
                visit(base, &p, out);
            } else {
                assert!(
                    metadata.is_file(),
                    "unexpected special fixture entry: {p:?}"
                );
                out.push((relative, SnapshotEntry::File(std::fs::read(p).unwrap())));
            }
        }
    }
    let mut out = vec![];
    visit(base, base, &mut out);
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}
fn audit_entries(audit: &Path) -> BTreeSet<String> {
    std::fs::read_dir(audit)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            assert!(std::fs::symlink_metadata(entry.path()).unwrap().is_file());
            Uuid::new(std::fs::read_to_string(entry.path()).unwrap()).unwrap();
            entry.file_name().into_string().unwrap()
        })
        .collect()
}
fn package(adapters: &Path) -> PathBuf {
    let package = adapters.join("duckdb");
    std::fs::create_dir_all(&package).unwrap();
    std::fs::write(
        package.join("adapter.toml"),
        format!(
            "name='duckdb'\nversion={:?}\ninterface_versions=[1]\nbinding_schema_version=1\nentrypoint={:?}\n",
            env!("CARGO_PKG_VERSION"),
            env!("CARGO_BIN_EXE_fixture-stale-pull"),
        ),
    )
    .unwrap();
    package
}
fn write_decl(decl: &Path, coordinates: &Value, materialization: &str) {
    std::fs::write(
        decl,
        format!(
            "declaration_version: 1\nkind: pull\ndataset: data\nadapter: duckdb\nwrite: replace\nconnection: {{database: {database:?}}}\ntarget: {{schema: app}}\noptions: {{materialization: {materialization}, refresh: auto}}\ntables:\n  - name: rows\n",
            database = coordinates["database"].as_str().unwrap(),
        ),
    )
    .unwrap();
}
fn run_pull(
    root: &str,
    decl: &Path,
    state: &Path,
    attempt: &str,
    adapters: &Path,
    private: &Path,
) -> (std::process::Output, Value) {
    let before_private = snapshot(private);
    let output = Command::new(env!("CARGO_BIN_EXE_grv-conformance-cli"))
        .arg("--json")
        .args([
            "pull",
            "--grv",
            root,
            "--decl",
            decl.to_str().unwrap(),
            "--state",
            state.to_str().unwrap(),
            "--attempt",
            attempt,
        ])
        .env("GRV_ADAPTERS_DIR", adapters)
        .output()
        .unwrap();
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    grv_adapter_host::validate_output(&value).unwrap();
    assert_eq!(
        snapshot(private),
        before_private,
        "owned private history must remain immutable"
    );
    (output, value)
}
/// Genuine recorded request + receipt built with the actual full registry,
/// pure binding validation and the production digest functions.
fn recorded_pull(
    materialization: &str,
    attempt: &Uuid,
    root: &str,
    identity: &str,
    workspace: &Uuid,
    coordinates: &Value,
) -> (RequestRecord, Receipt) {
    let registration = grv_adapter_duckdb::process::DuckDbAdapter::default().registration();
    let adapter = AdapterIdentity {
        name: registration.name,
        package_version: registration.package_version,
        interface_version: registration.interface_versions[0],
        binding_schema_version: registration.binding_schema_version,
    };
    let registry = registration.registry;
    let checked = CheckedRegistry::recorded(registry.clone()).unwrap();
    let mut validation_input = json!({
        "declaration_version": 1,
        "kind": "pull",
        "dataset": "data",
        "adapter": "duckdb",
        "write": "replace",
        "connection": coordinates,
        "target": {"schema": "app"},
        "options": {"materialization": materialization, "refresh": "auto"},
        "tables": [{"name": "rows"}],
        "revision": "latest",
    });
    grv_core::declaration::apply_point_defaults(&mut validation_input, &checked).unwrap();
    let effective = grv_adapter_duckdb::process::DuckDbAdapter::default()
        .validate_binding(validation_input.clone(), Mode::Pull, Req::new(1).unwrap())
        .unwrap();
    let declaration_sha256 = grv_types::declaration_digest(&DeclarationIdentity {
        effective_declaration: effective.clone(),
        adapter_identity: adapter.clone(),
        connection_identity: identity.to_owned(),
        canonical_connection: coordinates.clone(),
    })
    .unwrap();
    let request_sha256 = grv_types::pull_request_digest(&PullRequestIdentity {
        root: root.to_owned(),
        workspace_id: workspace.clone(),
        declaration_sha256: declaration_sha256.clone(),
        requested_revision: RequestedRevision::Latest(LatestRevision::Latest),
    })
    .unwrap();
    let request = RequestRecord {
        attempt_id: attempt.clone(),
        root: root.to_owned(),
        dataset: Name::new("data").unwrap(),
        workspace_id: workspace.clone(),
        adapter_identity: adapter,
        connection_identity: identity.to_owned(),
        effective_declaration: effective,
        validation_input,
        declaration_sha256,
        request_sha256,
        registry,
        requested_revision: RequestedRevision::Latest(LatestRevision::Latest),
    };
    request.validate().unwrap();
    let contract = TableContract {
        columns: vec![Column {
            name: "id".into(),
            logical_type: json!("int64"),
        }],
        partition_keys: vec![],
        extensions: json!({}),
        column_ext: json!({}),
    };
    let rows = Name::new("rows").unwrap();
    let receipt = Receipt {
        request: request.clone(),
        committed_revision: U64::new(1).unwrap(),
        generation_id: Uuid::v4(),
        pulled_at: Timestamp::new("2025-01-01T00:00:00Z").unwrap(),
        row_counts: vec![TableCount {
            table: rows.clone(),
            rows: U64::new(0).unwrap(),
        }],
        source_contracts: vec![NamedContract {
            table: rows.clone(),
            contract: contract.clone(),
        }],
        output_contracts: vec![NamedContract {
            table: rows,
            contract,
        }],
        adapter_result: json!({
            "target_schema": "app",
            "write_mode": "replace",
            "transform_mode": "identity",
            "scope_mode": "complete",
            "materialization_mode": materialization,
            "source_partitions": [],
        }),
    };
    receipt.validate().unwrap();
    (request, receipt)
}
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FixedMirror {
    request: RequestRecord,
    canonical_connection: Value,
}
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ProgressMirror {
    plan: Option<PullPlan>,
}
type RecordMirror = Envelope<FixedMirror, Value, ProgressMirror, Receipt>;
fn create_journal(
    state: &Path,
    attempt: &str,
    root_exclusion: Option<&Path>,
    request: &RequestRecord,
    coordinates: &Value,
    terminal: Option<Receipt>,
) {
    let directory = state.join("pull").join(attempt);
    let exclusions: Vec<PathBuf> = root_exclusion
        .map(|root| std::fs::canonicalize(root).unwrap())
        .into_iter()
        .collect();
    let journal = Journal::create(&directory, &exclusions).unwrap();
    journal
        .create_evidence(Evidence {
            intent: FixedMirror {
                request: request.clone(),
                canonical_connection: coordinates.clone(),
            },
            capture: None::<Value>,
            progress: ProgressMirror { plan: None },
            terminal,
        })
        .unwrap();
}
fn read_journal(state: &Path, attempt: &str) -> RecordMirror {
    let raw = std::fs::read(state.join("pull").join(attempt).join("journal.json")).unwrap();
    let value: Value = serde_json::from_slice(&raw).unwrap();
    serde_json::from_value(value).unwrap()
}
fn private_state(
    repo: &Path,
    audit: Option<&Path>,
    files: &[(&str, &str)],
) -> (PathBuf, tempfile::TempDir) {
    let dir = tempfile::tempdir_in(repo).unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    if let Some(audit) = audit {
        std::fs::write(dir.path().join("audit"), audit.to_str().unwrap()).unwrap();
    }
    for (name, content) in files {
        std::fs::write(dir.path().join(name), content).unwrap();
    }
    (dir.path().to_path_buf(), dir)
}
fn audit_dir(repo: &Path) -> (PathBuf, tempfile::TempDir) {
    let dir = tempfile::tempdir_in(repo).unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    (dir.path().to_path_buf(), dir)
}
#[test]
fn agreeing_peer_stale_gcs_options_refuse_after_offline_probe_before_auth_or_mutation() {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let temp = tempfile::tempdir_in(repo).unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let adapters = temp.path().join("adapters");
    let package = package(&adapters);
    let root = temp.path().join("root");
    std::fs::create_dir(&root).unwrap();
    let state = temp.path().join("state");
    std::fs::create_dir(&state).unwrap();
    let database = temp.path().join("target.duckdb");
    std::fs::write(&database, b"canary must not be opened or replaced").unwrap();
    let decl = temp.path().join("pull.yml");
    std::fs::write(&decl,format!("declaration_version: 1\nkind: pull\ndataset: data\nadapter: duckdb\nwrite: replace\nconnection: {{database: {database:?}}}\ntarget: {{schema: app}}\noptions: {{materialization: gcs-view, refresh: auto}}\ntables:\n  - name: rows\n")).unwrap();
    let before = snapshot(temp.path());
    let output = Command::new(env!("CARGO_BIN_EXE_grv-conformance-cli"))
        .arg("--json")
        .args([
            "pull",
            "--grv",
            root.to_str().unwrap(),
            "--decl",
            decl.to_str().unwrap(),
            "--state",
            state.to_str().unwrap(),
        ])
        .env("GRV_ADAPTERS_DIR", &adapters)
        .output()
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    grv_adapter_host::validate_output(&value).unwrap();
    assert!(!output.status.success());
    assert_eq!(value["errors"][0]["code"], "INVALID_DECLARATION", "{value}");
    assert!(!package.join("forbidden-auth-or-effect").exists());
    assert_eq!(
        snapshot(temp.path()),
        before,
        "refusal must not initialize target/root/journal or rewrite installed schema"
    );
    // Control: the real host/fixture protocol can reach authentication and
    // records its canary. No cloud credentials or target operations exist.
    use grv_adapter_api::Mode;
    use grv_adapter_host::{
        discovery::{self, RootKind, SearchRoot},
        process::{Deadlines, Session},
    };
    let installations = discovery::discover(&[SearchRoot {
        path: adapters.clone(),
        kind: RootKind::User,
    }])
    .unwrap();
    let mut session = Session::spawn(&installations[0], Deadlines::default()).unwrap();
    let locator = session
        .locate_connection(serde_json::json!({"database":database}), Mode::Pull, None)
        .unwrap();
    let identity = locator.identity.clone();
    let bound = session
        .bind_connection(
            locator,
            Some(root.to_str().unwrap().into()),
            identity,
            None,
            Mode::Pull,
        )
        .unwrap();
    assert_eq!(
        session.authenticate(bound.handle, None).unwrap_err().code,
        grv_types::ErrorCode::AdapterFailure
    );
    session.close().unwrap();
    assert!(package.join("forbidden-auth-or-effect").exists());
}
// Row 2: prior-bound identity/workspace, stale options enum, gcs-view
// declaration. The offline probe runs; current validation refuses before
// authentication, journal creation or any effect.
#[test]
fn prior_bound_stale_gcs_options_refuse_before_auth_or_mutation() {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let temp = tempfile::tempdir_in(repo).unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let adapters = temp.path().join("adapters");
    let package = package(&adapters);
    let root = temp.path().join("root");
    std::fs::create_dir(&root).unwrap();
    let state = temp.path().join("state");
    std::fs::create_dir(&state).unwrap();
    let database = temp.path().join("target.duckdb");
    std::fs::write(&database, b"canary must not be opened or replaced").unwrap();
    let (audit, _audit_guard) = audit_dir(repo);
    let workspace = Uuid::v4();
    let (private, _private_guard) = private_state(
        repo,
        Some(&audit),
        &[
            ("binding", "bound"),
            ("identity", "offline-bound-identity-task24"),
            ("workspace", workspace.as_str()),
        ],
    );
    let coordinates = json!({"database": private.to_string_lossy()});
    let decl = temp.path().join("pull.yml");
    write_decl(&decl, &coordinates, "gcs-view");
    let before = snapshot(temp.path());
    let before_private = snapshot(&private);
    let (output, value) = run_pull(
        root.to_str().unwrap(),
        &decl,
        &state,
        &Uuid::v4().to_string(),
        &adapters,
        &private,
    );
    assert!(!output.status.success());
    assert_eq!(value["errors"][0]["code"], "INVALID_DECLARATION", "{value}");
    assert!(!package.join("forbidden-auth-or-effect").exists());
    assert_eq!(
        audit_entries(&audit),
        BTreeSet::from(["probe-lookup".to_owned()]),
        "the offline lookup probe ran; compare/prepare/apply never ran"
    );
    assert_eq!(snapshot(temp.path()), before);
    assert_eq!(snapshot(&private), before_private);
}
// Row 3: a valid matching committed historical GCS receipt replays
// source-free even though the current options schema is stale. The gs root
// is parsed lazily without storage authentication; no journal, authentication,
// preparation, apply or target mutation occurs.
#[test]
fn committed_historical_gcs_receipt_replays_source_free_despite_stale_current_options() {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let temp = tempfile::tempdir_in(repo).unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let adapters = temp.path().join("adapters");
    let package = package(&adapters);
    let root = temp.path().join("root");
    std::fs::create_dir(&root).unwrap();
    let state = temp.path().join("state");
    std::fs::create_dir(&state).unwrap();
    let database = temp.path().join("target.duckdb");
    std::fs::write(&database, b"canary must not be opened or replaced").unwrap();
    let (audit, _audit_guard) = audit_dir(repo);
    let raw_root = "gs://grv-task24-bucket";
    let root = grv_storage::cloud::CloudRoot::parse(raw_root)
        .unwrap()
        .canonical()
        .to_owned();
    let attempt = Uuid::v4();
    let workspace = Uuid::v4();
    let identity = "offline-bound-identity-task24";
    let (private, _private_guard) = private_state(
        repo,
        Some(&audit),
        &[
            ("binding", "bound"),
            ("identity", identity),
            ("workspace", workspace.as_str()),
        ],
    );
    let coordinates = json!({"database": private.to_string_lossy()});
    let (_request, receipt) = recorded_pull(
        "gcs-view",
        &attempt,
        &root,
        identity,
        &workspace,
        &coordinates,
    );
    std::fs::write(
        private.join("receipt.json"),
        serde_json::to_string(&receipt).unwrap(),
    )
    .unwrap();
    let decl = temp.path().join("pull.yml");
    write_decl(&decl, &coordinates, "gcs-view");
    let before = snapshot(temp.path());
    let before_private = snapshot(&private);
    let (output, value) = run_pull(&root, &decl, &state, attempt.as_str(), &adapters, &private);
    assert!(output.status.success(), "{value}");
    assert_eq!(value["ok"], true, "{value}");
    assert_eq!(value["result"]["replayed"], true, "{value}");
    assert_eq!(value["result"]["committed_revision"], "1", "{value}");
    assert_eq!(
        value["result"]["workspace_id"],
        workspace.as_str(),
        "{value}"
    );
    assert_eq!(value["root"], root, "{value}");
    assert!(!package.join("forbidden-auth-or-effect").exists());
    assert_eq!(
        audit_entries(&audit),
        BTreeSet::from(["probe-lookup".to_owned(), "probe-compare".to_owned()]),
        "lookup and the pure compare ran; no preparation or apply followed"
    );
    assert_eq!(
        snapshot(temp.path()),
        before,
        "replay must not create a journal or mutate target/root/package"
    );
    assert_eq!(snapshot(&private), before_private);
}
// Row 3b: the same historical replay with a recorded journal completes the
// journal terminal exactly once; that is the only permitted mutation.
#[test]
fn committed_historical_gcs_receipt_replays_with_recorded_journal_terminal_completion() {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let temp = tempfile::tempdir_in(repo).unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let adapters = temp.path().join("adapters");
    let package = package(&adapters);
    let root = temp.path().join("root");
    std::fs::create_dir(&root).unwrap();
    let state = temp.path().join("state");
    std::fs::create_dir(&state).unwrap();
    let database = temp.path().join("target.duckdb");
    std::fs::write(&database, b"canary must not be opened or replaced").unwrap();
    let (audit, _audit_guard) = audit_dir(repo);
    let raw_root = "gs://grv-task24-bucket";
    let root = grv_storage::cloud::CloudRoot::parse(raw_root)
        .unwrap()
        .canonical()
        .to_owned();
    let attempt = Uuid::v4();
    let workspace = Uuid::v4();
    let identity = "offline-bound-identity-task24";
    let (private, _private_guard) = private_state(
        repo,
        Some(&audit),
        &[
            ("binding", "bound"),
            ("identity", identity),
            ("workspace", workspace.as_str()),
        ],
    );
    let coordinates = json!({"database": private.to_string_lossy()});
    let (request, receipt) = recorded_pull(
        "gcs-view",
        &attempt,
        &root,
        identity,
        &workspace,
        &coordinates,
    );
    std::fs::write(
        private.join("receipt.json"),
        serde_json::to_string(&receipt).unwrap(),
    )
    .unwrap();
    create_journal(&state, attempt.as_str(), None, &request, &coordinates, None);
    let decl = temp.path().join("pull.yml");
    write_decl(&decl, &coordinates, "gcs-view");
    let before = snapshot(temp.path());
    let before_private = snapshot(&private);
    let (output, value) = run_pull(&root, &decl, &state, attempt.as_str(), &adapters, &private);
    assert!(output.status.success(), "{value}");
    assert_eq!(value["result"]["replayed"], true, "{value}");
    assert!(!package.join("forbidden-auth-or-effect").exists());
    assert_eq!(
        audit_entries(&audit),
        BTreeSet::from(["probe-lookup".to_owned(), "probe-compare".to_owned()])
    );
    let after = snapshot(temp.path());
    let mut changed: Vec<PathBuf> = after
        .iter()
        .filter(|(p, v)| !before.iter().any(|(bp, bv)| bp == p && bv == v))
        .map(|(p, _)| p.clone())
        .collect();
    for (p, _) in &before {
        if !after.iter().any(|(ap, _)| ap == p) {
            changed.push(p.clone());
        }
    }
    changed.sort();
    assert_eq!(
        changed,
        vec![
            Path::new("state")
                .join("pull")
                .join(attempt.as_str())
                .join("journal.json")
        ],
        "only the legitimate terminal journal completion may change"
    );
    let record = read_journal(&state, attempt.as_str());
    assert_eq!(record.generation.get(), 2);
    assert_eq!(record.evidence.terminal, Some(receipt));
    assert_eq!(
        record.evidence.intent.request, request,
        "fixed intent stays immutable"
    );
    assert_eq!(snapshot(&private), before_private);
    // A later replay has no destination effect. The existing journal contract
    // still performs a CAS (generation advances), with identical fixed intent
    // and terminal data; this is not a no-journal-write claim.
    let before_second = snapshot(temp.path());
    let (output, value) = run_pull(&root, &decl, &state, attempt.as_str(), &adapters, &private);
    assert!(output.status.success(), "{value}");
    assert_eq!(value["result"]["replayed"], true);
    let second = read_journal(&state, attempt.as_str());
    assert_eq!(second.generation.get(), 3);
    assert_eq!(
        second.evidence.intent.request,
        record.evidence.intent.request
    );
    assert_eq!(second.evidence.terminal, record.evidence.terminal);
    let after_second = snapshot(temp.path());
    assert_eq!(before_second.len(), after_second.len());
    let changed: Vec<_> = after_second
        .iter()
        .zip(&before_second)
        .filter(|(after, before)| after != before)
        .map(|(after, _)| after.0.clone())
        .collect();
    assert_eq!(
        changed,
        vec![
            Path::new("state")
                .join("pull")
                .join(attempt.as_str())
                .join("journal.json")
        ]
    );
    assert!(!package.join("forbidden-auth-or-effect").exists());
}
// Row 4: terminal journal evidence with a stored receipt, but the destination
// lookup finds no committed receipt: the outcome is unknown, never
// NotCommitted, before any authentication or effect.
#[test]
fn terminal_journal_without_stored_receipt_is_outcome_unknown() {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let temp = tempfile::tempdir_in(repo).unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let adapters = temp.path().join("adapters");
    let package = package(&adapters);
    let root = temp.path().join("root");
    std::fs::create_dir(&root).unwrap();
    let state = temp.path().join("state");
    std::fs::create_dir(&state).unwrap();
    let database = temp.path().join("target.duckdb");
    std::fs::write(&database, b"canary must not be opened or replaced").unwrap();
    let (audit, _audit_guard) = audit_dir(repo);
    let attempt = Uuid::v4();
    let workspace = Uuid::v4();
    let identity = "offline-bound-identity-task24";
    let (private, _private_guard) = private_state(
        repo,
        Some(&audit),
        &[
            ("binding", "bound"),
            ("identity", identity),
            ("workspace", workspace.as_str()),
        ],
    );
    let coordinates = json!({"database": private.to_string_lossy()});
    let (request, receipt) = recorded_pull(
        "gcs-view",
        &attempt,
        root.canonicalize().unwrap().to_str().unwrap(),
        identity,
        &workspace,
        &coordinates,
    );
    create_journal(
        &state,
        attempt.as_str(),
        Some(&root),
        &request,
        &coordinates,
        Some(receipt),
    );
    let decl = temp.path().join("pull.yml");
    write_decl(&decl, &coordinates, "gcs-view");
    let before = snapshot(temp.path());
    let (output, value) = run_pull(
        root.canonicalize().unwrap().to_str().unwrap(),
        &decl,
        &state,
        attempt.as_str(),
        &adapters,
        &private,
    );
    assert!(!output.status.success());
    assert_eq!(value["errors"][0]["code"], "OUTCOME_UNKNOWN", "{value}");
    assert!(!package.join("forbidden-auth-or-effect").exists());
    assert_eq!(
        audit_entries(&audit),
        BTreeSet::from(["probe-lookup".to_owned()]),
        "the lookup probe ran; no compare or effect followed"
    );
    assert_eq!(
        snapshot(temp.path()),
        before,
        "the terminal journal is immutable evidence"
    );
}
// Row 5: the same terminal journal, but the destination lookup itself is
// unknown: still OUTCOME_UNKNOWN before any authentication or effect.
#[test]
fn terminal_journal_with_unknown_lookup_is_outcome_unknown() {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let temp = tempfile::tempdir_in(repo).unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let adapters = temp.path().join("adapters");
    let package = package(&adapters);
    let root = temp.path().join("root");
    std::fs::create_dir(&root).unwrap();
    let state = temp.path().join("state");
    std::fs::create_dir(&state).unwrap();
    let database = temp.path().join("target.duckdb");
    std::fs::write(&database, b"canary must not be opened or replaced").unwrap();
    let (audit, _audit_guard) = audit_dir(repo);
    let attempt = Uuid::v4();
    let workspace = Uuid::v4();
    let identity = "offline-bound-identity-task24";
    let (private, _private_guard) = private_state(
        repo,
        Some(&audit),
        &[
            ("binding", "bound"),
            ("identity", identity),
            ("workspace", workspace.as_str()),
            ("unknown", "destination history unreadable"),
        ],
    );
    let coordinates = json!({"database": private.to_string_lossy()});
    let (request, receipt) = recorded_pull(
        "gcs-view",
        &attempt,
        root.canonicalize().unwrap().to_str().unwrap(),
        identity,
        &workspace,
        &coordinates,
    );
    create_journal(
        &state,
        attempt.as_str(),
        Some(&root),
        &request,
        &coordinates,
        Some(receipt),
    );
    let decl = temp.path().join("pull.yml");
    write_decl(&decl, &coordinates, "gcs-view");
    let before = snapshot(temp.path());
    let (output, value) = run_pull(
        root.canonicalize().unwrap().to_str().unwrap(),
        &decl,
        &state,
        attempt.as_str(),
        &adapters,
        &private,
    );
    assert!(!output.status.success());
    assert_eq!(value["errors"][0]["code"], "OUTCOME_UNKNOWN", "{value}");
    assert!(!package.join("forbidden-auth-or-effect").exists());
    assert_eq!(
        audit_entries(&audit),
        BTreeSet::from(["probe-lookup".to_owned()])
    );
    assert_eq!(snapshot(temp.path()), before);
}
// Row 6: a prior-bound journal whose recorded root differs from the current
// root is refused before the session is even spawned. A live executable
// startup canary below proves that absence, independently of lookup markers.
#[test]
fn prior_bound_journal_root_mismatch_refuses_before_session_spawn() {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let temp = tempfile::tempdir_in(repo).unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let adapters = temp.path().join("adapters");
    let package = package(&adapters);
    let root = temp.path().join("root");
    std::fs::create_dir(&root).unwrap();
    let state = temp.path().join("state");
    std::fs::create_dir(&state).unwrap();
    let database = temp.path().join("target.duckdb");
    std::fs::write(&database, b"canary must not be opened or replaced").unwrap();
    let (audit, _audit_guard) = audit_dir(repo);
    let attempt = Uuid::v4();
    let workspace = Uuid::v4();
    let identity = "offline-bound-identity-task24";
    let (private, _private_guard) = private_state(
        repo,
        Some(&audit),
        &[
            ("binding", "bound"),
            ("identity", identity),
            ("workspace", workspace.as_str()),
        ],
    );
    let coordinates = json!({"database": private.to_string_lossy()});
    let (request, _receipt) = recorded_pull(
        "gcs-view",
        &attempt,
        "gs://other-bucket-task24",
        identity,
        &workspace,
        &coordinates,
    );
    create_journal(
        &state,
        attempt.as_str(),
        Some(&root),
        &request,
        &coordinates,
        None,
    );
    let decl = temp.path().join("pull.yml");
    write_decl(&decl, &coordinates, "gcs-view");
    // Positive startup control: this private executable records every launch.
    // Install it as the peer only in this pre-spawn refusal case.
    let startup = package.join("startup-probe");
    let startup_marker = startup.with_extension("started");
    std::fs::write(
        &startup,
        b"#!/bin/sh\nprintf '%s' started > \"$0.started\"\nexit 7\n",
    )
    .unwrap();
    std::fs::set_permissions(&startup, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(Command::new(&startup).status().unwrap().code(), Some(7));
    assert_eq!(std::fs::read(&startup_marker).unwrap(), b"started");
    std::fs::remove_file(&startup_marker).unwrap();
    std::fs::write(package.join("adapter.toml"), format!(
        "name='duckdb'\nversion={:?}\ninterface_versions=[1]\nbinding_schema_version=1\nentrypoint={:?}\n",
        env!("CARGO_PKG_VERSION"), startup,
    )).unwrap();
    let before = snapshot(temp.path());
    let (output, value) = run_pull(
        root.canonicalize().unwrap().to_str().unwrap(),
        &decl,
        &state,
        attempt.as_str(),
        &adapters,
        &private,
    );
    assert!(!output.status.success());
    assert_eq!(value["errors"][0]["code"], "REQUEST_MISMATCH", "{value}");
    assert!(!package.join("forbidden-auth-or-effect").exists());
    assert!(audit_entries(&audit).is_empty(), "no lookup probe ran");
    assert!(
        !startup_marker.exists(),
        "the peer startup canary must not run"
    );
    assert_eq!(snapshot(temp.path()), before);
}
// Row 7a: a prior-bound journal workspace that differs from the bound
// workspace is refused at the host bind check, before lookup or auth.
#[test]
fn prior_bound_journal_workspace_mismatch_refuses_at_bind() {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let temp = tempfile::tempdir_in(repo).unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let adapters = temp.path().join("adapters");
    let package = package(&adapters);
    let root = temp.path().join("root");
    std::fs::create_dir(&root).unwrap();
    let state = temp.path().join("state");
    std::fs::create_dir(&state).unwrap();
    let database = temp.path().join("target.duckdb");
    std::fs::write(&database, b"canary must not be opened or replaced").unwrap();
    let (audit, _audit_guard) = audit_dir(repo);
    let attempt = Uuid::v4();
    let current_workspace = Uuid::v4();
    let recorded_workspace = Uuid::v4();
    let identity = "offline-bound-identity-task24";
    let (private, _private_guard) = private_state(
        repo,
        Some(&audit),
        &[
            ("binding", "bound"),
            ("identity", identity),
            ("workspace", current_workspace.as_str()),
        ],
    );
    let coordinates = json!({"database": private.to_string_lossy()});
    let (request, _receipt) = recorded_pull(
        "gcs-view",
        &attempt,
        root.canonicalize().unwrap().to_str().unwrap(),
        identity,
        &recorded_workspace,
        &coordinates,
    );
    create_journal(
        &state,
        attempt.as_str(),
        Some(&root),
        &request,
        &coordinates,
        None,
    );
    let decl = temp.path().join("pull.yml");
    write_decl(&decl, &coordinates, "gcs-view");
    let before = snapshot(temp.path());
    let (output, value) = run_pull(
        root.canonicalize().unwrap().to_str().unwrap(),
        &decl,
        &state,
        attempt.as_str(),
        &adapters,
        &private,
    );
    assert!(!output.status.success());
    assert_eq!(value["errors"][0]["code"], "REQUEST_MISMATCH", "{value}");
    assert!(!package.join("forbidden-auth-or-effect").exists());
    assert!(
        audit_entries(&audit).is_empty(),
        "bind failed before any lookup probe"
    );
    assert_eq!(snapshot(temp.path()), before);
}
// Row 7b: matching root but a recorded identity different from the bound
// identity is refused at the fixed-request comparison, after the lookup probe
// and before compare, authentication or any effect.
#[test]
fn prior_bound_journal_identity_mismatch_refuses_before_compare() {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let temp = tempfile::tempdir_in(repo).unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let adapters = temp.path().join("adapters");
    let package = package(&adapters);
    let root = temp.path().join("root");
    std::fs::create_dir(&root).unwrap();
    let state = temp.path().join("state");
    std::fs::create_dir(&state).unwrap();
    let database = temp.path().join("target.duckdb");
    std::fs::write(&database, b"canary must not be opened or replaced").unwrap();
    let (audit, _audit_guard) = audit_dir(repo);
    let attempt = Uuid::v4();
    let workspace = Uuid::v4();
    let current_identity = "offline-bound-identity-task24";
    let recorded_identity = "offline-bound-identity-other";
    let (private, _private_guard) = private_state(
        repo,
        Some(&audit),
        &[
            ("binding", "bound"),
            ("identity", current_identity),
            ("workspace", workspace.as_str()),
        ],
    );
    let coordinates = json!({"database": private.to_string_lossy()});
    let (request, _receipt) = recorded_pull(
        "local",
        &attempt,
        root.canonicalize().unwrap().to_str().unwrap(),
        recorded_identity,
        &workspace,
        &coordinates,
    );
    create_journal(
        &state,
        attempt.as_str(),
        Some(&root),
        &request,
        &coordinates,
        None,
    );
    let decl = temp.path().join("pull.yml");
    write_decl(&decl, &coordinates, "local");
    let before = snapshot(temp.path());
    let (output, value) = run_pull(
        root.canonicalize().unwrap().to_str().unwrap(),
        &decl,
        &state,
        attempt.as_str(),
        &adapters,
        &private,
    );
    assert!(!output.status.success());
    assert_eq!(value["errors"][0]["code"], "REQUEST_MISMATCH", "{value}");
    assert!(!package.join("forbidden-auth-or-effect").exists());
    assert_eq!(
        audit_entries(&audit),
        BTreeSet::from(["probe-lookup".to_owned()]),
        "the lookup probe ran; the fixed-request refusal precedes compare and auth"
    );
    assert_eq!(snapshot(temp.path()), before);
}
// Row 8: unknown durable history from the destination is OUTCOME_UNKNOWN
// before authentication or effects.
#[test]
fn unknown_history_is_outcome_unknown_before_auth() {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let temp = tempfile::tempdir_in(repo).unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let adapters = temp.path().join("adapters");
    let package = package(&adapters);
    let root = temp.path().join("root");
    std::fs::create_dir(&root).unwrap();
    let state = temp.path().join("state");
    std::fs::create_dir(&state).unwrap();
    let database = temp.path().join("target.duckdb");
    std::fs::write(&database, b"canary must not be opened or replaced").unwrap();
    let (audit, _audit_guard) = audit_dir(repo);
    let workspace = Uuid::v4();
    let identity = "offline-bound-identity-task24";
    let (private, _private_guard) = private_state(
        repo,
        Some(&audit),
        &[
            ("binding", "bound"),
            ("identity", identity),
            ("workspace", workspace.as_str()),
            ("unknown", "destination history unreadable"),
        ],
    );
    let coordinates = json!({"database": private.to_string_lossy()});
    let decl = temp.path().join("pull.yml");
    write_decl(&decl, &coordinates, "gcs-view");
    let before = snapshot(temp.path());
    let (output, value) = run_pull(
        root.canonicalize().unwrap().to_str().unwrap(),
        &decl,
        &state,
        &Uuid::v4().to_string(),
        &adapters,
        &private,
    );
    assert!(!output.status.success());
    assert_eq!(value["errors"][0]["code"], "OUTCOME_UNKNOWN", "{value}");
    assert!(!package.join("forbidden-auth-or-effect").exists());
    assert_eq!(
        audit_entries(&audit),
        BTreeSet::from(["probe-lookup".to_owned()])
    );
    assert_eq!(snapshot(temp.path()), before);
}
// Row 8b: a present receipt that is unreadable or not a valid closed typed
// record is unknown history, never absence: OUTCOME_UNKNOWN before auth.
#[test]
fn malformed_stored_receipt_is_outcome_unknown_not_absence() {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let temp = tempfile::tempdir_in(repo).unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let adapters = temp.path().join("adapters");
    let package = package(&adapters);
    let root = temp.path().join("root");
    std::fs::create_dir(&root).unwrap();
    let state = temp.path().join("state");
    std::fs::create_dir(&state).unwrap();
    let database = temp.path().join("target.duckdb");
    std::fs::write(&database, b"canary must not be opened or replaced").unwrap();
    let (audit, _audit_guard) = audit_dir(repo);
    let attempt = Uuid::v4();
    let workspace = Uuid::v4();
    let identity = "offline-bound-identity-task24";
    let (private, _private_guard) = private_state(
        repo,
        Some(&audit),
        &[
            ("binding", "bound"),
            ("identity", identity),
            ("workspace", workspace.as_str()),
        ],
    );
    let coordinates = json!({"database": private.to_string_lossy()});
    let (_request, receipt) = recorded_pull(
        "gcs-view",
        &attempt,
        root.canonicalize().unwrap().to_str().unwrap(),
        identity,
        &workspace,
        &coordinates,
    );
    let canonical_root = root.canonicalize().unwrap().to_str().unwrap().to_owned();
    let decl = temp.path().join("pull.yml");
    write_decl(&decl, &coordinates, "gcs-view");
    let before = snapshot(temp.path());
    for (label, kind) in [
        ("unknown top-level field", 0),
        ("corrupted request digest", 1),
        ("unknown materialization", 2),
    ] {
        let mut value: Value = serde_json::to_value(&receipt).unwrap();
        match kind {
            0 => value["bogus_field"] = json!(1),
            1 => value["request"]["request_sha256"] = json!("f".repeat(64)),
            2 => value["adapter_result"]["materialization_mode"] = json!("unknown-cloud-view"),
            _ => unreachable!(),
        }
        std::fs::write(
            private.join("receipt.json"),
            serde_json::to_string(&value).unwrap(),
        )
        .unwrap();
        let (output, value) = run_pull(
            &canonical_root,
            &decl,
            &state,
            attempt.as_str(),
            &adapters,
            &private,
        );
        assert!(!output.status.success());
        assert_eq!(
            value["errors"][0]["code"], "OUTCOME_UNKNOWN",
            "{label}: {value}"
        );
        assert!(!package.join("forbidden-auth-or-effect").exists());
        assert_eq!(
            audit_entries(&audit),
            BTreeSet::from(["probe-lookup".to_owned()]),
            "{label}: the lookup probe ran; no compare or effect followed"
        );
        assert_eq!(
            snapshot(temp.path()),
            before,
            "{label}: present malformed history is immutable evidence"
        );
    }
}

#[test]
fn invalid_private_history_entries_refuse_instead_of_clean_absence() {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    for fault in [
        "missing-binding",
        "unknown-binding",
        "bad-workspace",
        "missing-identity",
        "dangling-binding",
        "dangling-receipt",
        "directory-receipt",
        "invalid-utf8-receipt",
        "dangling-audit",
    ] {
        let temp = tempfile::tempdir_in(repo).unwrap();
        let adapters = temp.path().join("adapters");
        let package = package(&adapters);
        let root = temp.path().join("root");
        std::fs::create_dir(&root).unwrap();
        let state = temp.path().join("state");
        std::fs::create_dir(&state).unwrap();
        let (audit, _audit_guard) = audit_dir(repo);
        let workspace = Uuid::v4();
        let (private, _private_guard) = private_state(
            repo,
            Some(&audit),
            &[
                ("binding", "bound"),
                ("identity", "offline-bound-identity-task24"),
                ("workspace", workspace.as_str()),
            ],
        );
        let dangling_target = private.join("not-created");
        match fault {
            "missing-binding" => std::fs::remove_file(private.join("binding")).unwrap(),
            "unknown-binding" => {
                std::fs::write(private.join("binding"), b"future-binding").unwrap()
            }
            "bad-workspace" => std::fs::write(private.join("workspace"), b"not-a-uuid").unwrap(),
            "missing-identity" => std::fs::remove_file(private.join("identity")).unwrap(),
            "dangling-binding" => {
                std::fs::remove_file(private.join("binding")).unwrap();
                std::os::unix::fs::symlink(&dangling_target, private.join("binding")).unwrap();
            }
            "dangling-receipt" => {
                std::os::unix::fs::symlink(&dangling_target, private.join("receipt.json")).unwrap()
            }
            "directory-receipt" => std::fs::create_dir(private.join("receipt.json")).unwrap(),
            "invalid-utf8-receipt" => std::fs::write(private.join("receipt.json"), [0xff]).unwrap(),
            "dangling-audit" => {
                std::fs::remove_file(private.join("audit")).unwrap();
                std::os::unix::fs::symlink(&dangling_target, private.join("audit")).unwrap();
            }
            _ => unreachable!(),
        }
        let coordinates = json!({"database": private.to_string_lossy()});
        let decl = temp.path().join("pull.yml");
        write_decl(&decl, &coordinates, "gcs-view");
        let before = snapshot(temp.path());
        let attempt = Uuid::v4();
        let (output, value) = run_pull(
            root.to_str().unwrap(),
            &decl,
            &state,
            attempt.as_str(),
            &adapters,
            &private,
        );
        assert!(!output.status.success(), "{fault}: {value}");
        assert_eq!(
            value["errors"][0]["code"], "OUTCOME_UNKNOWN",
            "{fault}: {value}"
        );
        assert!(
            !package.join("forbidden-auth-or-effect").exists(),
            "{fault}"
        );
        let expected = if matches!(
            fault,
            "dangling-receipt" | "directory-receipt" | "invalid-utf8-receipt"
        ) {
            BTreeSet::from(["probe-lookup".to_owned()])
        } else {
            BTreeSet::new()
        };
        assert_eq!(audit_entries(&audit), expected, "{fault}");
        assert_eq!(snapshot(temp.path()), before, "{fault}");
        assert!(
            !dangling_target.exists(),
            "fixture must not follow/create link targets"
        );
    }
}
