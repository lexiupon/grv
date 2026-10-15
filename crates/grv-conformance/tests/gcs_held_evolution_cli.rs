#![cfg(feature = "native-duckdb")]
//! Local ordinary-consumer evolution gate: one upstream source publishes a new
//! column (whole replacement, never an append or a typed NULL of old files) and
//! an ordinary external session prepares the current confirmed revision under a
//! real hold. Every path executed here is local; no cloud root, no GcsView view
//! materialization and no reader/typed-NULL path is claimed. The source publishes
//! through the actual CLI push; the consumer side is ordinary preparation only and
//! never publishes. Every fixture exists only to support that ordinary path.
//!
//! The whole body is generic over the real backend, so the ignored live slice drives
//! the identical body over a real `CloudBackend` with real GCS publication, a real
//! full source-verification pull and real bytes; that slice has NOT been run here and
//! therefore claims nothing yet. Discovery materialization stays the default LOCAL one
//! for both wrappers (the pull declaration carries no materialization option), so the
//! private relation is a locally materialized table fed by the actual full source
//! preparation. Held verification is not gated on that discovered kind: for an external
//! build core reconstructs the immutable revision files through the bound backend, so on
//! a GCS backend the held preparation reads through the view path and the live slice must
//! publish the engine-adjacent protected reader configuration with the fixed selectors,
//! exactly as the sibling held-lifecycle gates do. The local slice must have no such
//! configuration, which is checked by an actual open failing with ENOENT. The native guard
//! row read is therefore executed only in the local slice; the live private view query is
//! not executed here (no direct API, raw FFI, fixture or production transport), so every
//! live value assertion comes from the full source-prepared parquet bytes plus the core
//! manifest record, not from native view rows. The signed native prepare and the held
//! admission are executed in both slices. Explicit fixed credentials are set on the
//! spawned CLI children only, with no ambient environment mutation, and this test does
//! no cloud cleanup. Live environment: `GRV_GCS_TEST_ROOT` (one fresh exclusively owned
//! canonical child of the parent driver foundation), `GRV_GCS_ACCOUNT` and
//! `GRV_GCS_PROJECT`; requires the `native-duckdb` feature.
use arrow_array::{Int64Array, StringArray};
use arrow_schema::{DataType, Schema};
use grv_adapter_api::{BuildSession, Resources};
use grv_adapter_duckdb::{build::BuildStore, conversion::Scalar, native::NativeEngine};
use grv_adapter_host::{protected_document, validate_output};
use grv_core::{
    clock::SystemClock,
    holds::{Holds, hold_key, release_key},
    store::{InitOptions, Store},
};
use grv_storage::{
    Backend, ErrorKind, LocalBackend, ObjectKey,
    cloud::{CloudBackend, CloudOptions},
    model::{HoldRecord, RunControl, RunPhase, VersionManifest, decode_record},
};
use grv_types::{Name, Uuid};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
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
#[path = "support/held_external_reader.rs"]
mod external_reader;

const LIMIT: u64 = 4 * 1024 * 1024;
const BATCH_ALLOWANCE: usize = 4096;
const SOURCE_ALLOWANCE: usize = 32 * 1024 * 1024;

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
        ["session", "prepare", ..] => "session_prepare",
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
    // fallback and no process-wide mutation. The local wrapper passes `None` and every
    // stage then runs with no cloud selector at all, exactly as before.
    if let Some((account, project)) = selectors {
        command
            .env("GRV_GCS_ACCOUNT", account)
            .env("GRV_GCS_PROJECT", project);
    }
    let ordinal = COMMAND_ORDINAL.fetch_add(1, Ordering::Relaxed) + 1;
    let base = *COMMAND_BASE.get_or_init(Instant::now);
    eprintln!(
        "held evolution command: ordinal={ordinal} stage={} state=start elapsed_ms={}",
        stage_kind(args),
        Instant::now().duration_since(base).as_millis()
    );
    let started = Instant::now();
    let output = command.output().unwrap();
    eprintln!(
        "held evolution command: ordinal={ordinal} stage={} state=end duration_ms={}",
        stage_kind(args),
        started.elapsed().as_millis()
    );
    let value: Value =
        serde_json::from_slice(&output.stdout).expect("CLI must emit an authored JSON envelope");
    validate_output(&value).unwrap();
    // Authored fixed diagnostic only: a failure prints no raw envelope and no provider
    // text, because provider error detail can carry credential identifiers or locations.
    assert_eq!(
        output.status.success(),
        value["ok"] == true,
        "CLI stage must return an authored successful envelope"
    );
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

/// Local-slice-only guard: an actual open of the engine-adjacent reader configuration
/// path must fail with ENOENT. It is not a universal claim, because the live slice
/// publishes its own selectors for the view path used by held verification.
fn path_absent_enoent(path: &Path) {
    match fs::File::open(path) {
        Err(error) => assert_eq!(
            error.kind(),
            std::io::ErrorKind::NotFound,
            "reader configuration path must be absent for the local slice"
        ),
        Ok(_) => panic!("no reader configuration may exist for a locally materialized input"),
    }
}

fn latest(b: &impl Backend, dataset: &str) -> u64 {
    serde_json::from_slice::<Value>(
        &b.read_bytes(
            &ObjectKey::new(format!("datasets/{dataset}/.states/LATEST")).unwrap(),
            LIMIT as usize,
        )
        .unwrap()
        .0,
    )
    .unwrap()["revision"]
        .as_u64()
        .unwrap()
}

/// Actual committed file schema through the core manifest record plus its own
/// parquet data files; nothing is inferred from the authoring document.
fn manifest_rows(b: &impl Backend, staging: &Path, version: u64, columns: usize) {
    let manifest: VersionManifest = read_record(
        b,
        &format!("datasets/upstream/rows/version={version}/manifest.json"),
    );
    assert_eq!(manifest.table, Name::new("rows").unwrap());
    assert_eq!(manifest.version.get(), version);
    // One authored source file publishes exactly one committed data file.
    assert_eq!(manifest.data_files.len(), 1);
    for file in &manifest.data_files {
        let path = format!("datasets/upstream/rows/version={version}/{}", file.name);
        let bytes = b
            .read_bytes(&ObjectKey::new(&path).unwrap(), LIMIT as usize)
            .unwrap()
            .0;
        let staged = staging.join(format!("version-{version}-{}", file.name));
        fs::write(&staged, &bytes).unwrap();
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(fs::File::open(&staged).unwrap()).unwrap();
        fs::remove_file(&staged).unwrap();
        let schema: &Schema = builder.schema();
        let fields: Vec<(String, DataType)> = schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone()))
            .collect();
        assert_eq!(
            fields,
            vec![
                ("id".to_owned(), DataType::Int64),
                ("note".to_owned(), DataType::Utf8)
            ]
            .into_iter()
            .take(columns)
            .collect::<Vec<_>>()
        );
        let mut reader = builder.with_batch_size(1024).build().unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            version as i64
        );
        if columns == 2 {
            assert_eq!(
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                "new"
            );
        }
        // Full reader: the whole committed file is this one row, so nothing is
        // hidden behind an unread second batch.
        assert!(reader.next().is_none());
    }
}

fn quoted(relation: &str) -> String {
    let (schema, table) = relation.split_once('.').unwrap();
    format!(
        "\"{}\".\"{}\"",
        schema.replace('"', "\"\""),
        table.replace('"', "\"\"")
    )
}

/// The root is passed through verbatim to every CLI stage. Only a local root is
/// canonicalized by the filesystem; a canonical `gs://` root is used as it stands, with
/// no local path joining and no file URI.
fn canonical_root(root: &str) -> String {
    if root.starts_with("gs://") {
        root.to_owned()
    } else {
        fs::canonicalize(root).unwrap().to_str().unwrap().to_owned()
    }
}

fn init_options() -> InitOptions {
    InitOptions {
        max_clock_skew: Some(0),
        pending_grace: Some(1),
        ..Default::default()
    }
}

/// The whole ordinary boundary, generic over the real backend: real publication of the
/// new column through the actual CLI push, a real default (local-materialization) full
/// source-verification pull, and ordinary external-session preparation whose hold comes
/// from actual backend metadata. The prepared input group is nonempty and entirely
/// locally staged, so the discovered relation is a local table materialized from those
/// verified files. Held verification is not gated on that discovered kind: it reconstructs
/// the revision files through the bound backend, so a GCS backend needs the published
/// engine-adjacent reader configuration (live slice) while the local slice must have none.
/// The native guard row read is executed only when no selectors are passed, so no view row
/// is read anywhere in this test.
fn case<B: Backend>(store: &Store<B>, root: &str, selectors: Option<(&str, &str)>) {
    let scratch = temp();
    let adapters = scratch.path().join("adapters");
    package(
        &adapters,
        "duckdb",
        env!("CARGO_BIN_EXE_fixture-duckdb-gcs-build"),
    );
    let state = scratch.path().join("state");
    let source = scratch.path().join("source.duckdb");
    let engine = scratch.path().join("consumer.duckdb");
    let seed = scratch.path().join("seed.yml");
    let pull = scratch.path().join("pull.yml");
    let decl = scratch.path().join("external.yml");
    let context = scratch.path().join("context.json");
    // Same pattern as the sibling held-lifecycle gates: a live (external) backend reads
    // the held input through the view path, so the engine-adjacent protected reader
    // configuration carrying the fixed selectors must exist before any stage runs.
    if let Some((account, project)) = selectors {
        protected_document::publish(
            &PathBuf::from(format!("{}.grv-gcs-read.json", engine.display())),
            &json!({"config_version":1,"readers":[{"scope":root,"provider":"gcloud","account":account,"project":project}]}),
            LIMIT,
        )
        .unwrap();
    }
    let push = |value: u64, evolution: bool| {
        let columns = if evolution {
            "[{name: id, type: int64}, {name: note, type: utf8}]"
        } else {
            "[{name: id, type: int64}]"
        };
        let select = if evolution {
            format!("SELECT {value}::BIGINT AS id, 'new' AS note")
        } else {
            format!("SELECT {value}::BIGINT AS id")
        };
        fs::write(&seed,format!("declaration_version: 1\nkind: push\ndataset: upstream\nadapter: duckdb\nconnection: {{database: {source:?}}}\nbuild: {{execution: managed}}\ntables:\n  - name: rows\n    source: {{sql: \"{select}\"}}\n    columns: {columns}\n")).unwrap();
        cli(
            &adapters,
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
        )
    };
    assert_eq!(push(1, false)["result"]["outcome"]["revision"], "1");
    assert_eq!(latest(&store.backend, "upstream"), 1);
    manifest_rows(&store.backend, scratch.path(), 1, 1);
    let old_manifest = store
        .backend
        .read_bytes(
            &ObjectKey::new("datasets/upstream/rows/version=1/manifest.json").unwrap(),
            LIMIT as usize,
        )
        .unwrap()
        .0;
    let second = push(2, true);
    assert_eq!(second["result"]["outcome"]["revision"], "2", "{second}");
    assert_eq!(latest(&store.backend, "upstream"), 2);
    // Whole replacement: revision 1 keeps its own single-column bytes untouched
    // and contributes no rows to the revision the consumer prepares.
    assert_eq!(
        store
            .backend
            .read_bytes(
                &ObjectKey::new("datasets/upstream/rows/version=1/manifest.json").unwrap(),
                LIMIT as usize,
            )
            .unwrap()
            .0,
        old_manifest
    );
    manifest_rows(&store.backend, scratch.path(), 2, 2);
    // Revision 1 is still exactly itself after the new-column publication.
    manifest_rows(&store.backend, scratch.path(), 1, 1);
    fs::write(&pull,format!("declaration_version: 1\nkind: pull\ndataset: upstream\nadapter: duckdb\nconnection: {{database: {engine:?}}}\ntarget: {{schema: remote}}\ntables: [{{name: rows}}]\n")).unwrap();
    assert_eq!(
        cli(
            &adapters,
            &[
                "pull",
                "--grv",
                root,
                "--decl",
                pull.to_str().unwrap(),
                "--state",
                state.to_str().unwrap()
            ],
            selectors,
        )["result"]["committed_revision"],
        "2"
    );
    // Local slice only: an actual open of the engine-adjacent path must fail with ENOENT,
    // so the acquired relation below can only be the local table built from the verified
    // local staging. The live slice publishes its own selectors above for the view path.
    if selectors.is_none() {
        path_absent_enoent(&PathBuf::from(format!(
            "{}.grv-gcs-read.json",
            engine.display()
        )));
    }
    fs::write(&decl,format!("declaration_version: 1\nkind: push\ndataset: derived\nadapter: duckdb\nconnection: {{database: {engine:?}}}\nbuild:\n  execution: external\n  inputs: [{{table: remote.rows, as: numbers}}]\ntables:\n  - name: rows\n    source: {{table: rows}}\n    columns: [{{name: id, type: int64}}]\n")).unwrap();
    let attempt = Uuid::v4();
    assert_eq!(
        cli(
            &adapters,
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
        )["ok"],
        json!(true),
        "held external session prepare must succeed"
    );
    let session: BuildSession = serde_json::from_value(
        protected_document::read::<Value>(&context, LIMIT).unwrap()["session"].clone(),
    )
    .unwrap();
    assert_eq!(session.inputs.len(), 1);
    assert_eq!(session.inputs[0].dataset, Name::new("upstream").unwrap());
    assert_eq!(session.inputs[0].revision.get(), 2);
    let contract = json!([{"name": "id", "type": "int64"}, {"name": "note", "type": "string"}]);
    assert_eq!(
        json!([session.inputs[0].contract.columns[0]]),
        json!([{"name":"id","type":"int64"}])
    );
    assert_eq!(
        serde_json::to_value(&session.inputs[0].contract.columns).unwrap(),
        contract
    );
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
    assert_eq!(control.inputs[0].dataset, Name::new("upstream").unwrap());
    assert_eq!(control.inputs[0].revision.get(), 2);
    assert_eq!(control.run_id, session.identity.run_id);
    // Core's canonical key for the control's own citation (consumer derived,
    // source upstream, revision 2, that retention id) is where the record lives.
    let expected_hold = format!(
        "datasets/upstream/.holds/derived/revision=2/{}.json",
        control.inputs[0].retention_id
    );
    let clock = SystemClock::default();
    let holds = Holds::new(store, &clock, scratch.path());
    let hold: HoldRecord = read_record(&store.backend, &expected_hold);
    assert_eq!(hold_key(&hold).as_str(), expected_hold);
    assert_eq!(hold.target_run_id, session.identity.run_id);
    assert!(holds.active(&hold).unwrap());
    assert!(!holds.releasable(&hold).unwrap());
    absent(&store.backend, release_key(&hold).as_str());
    // Independent reader of the recorded session through the existing native
    // guard: the store lock must be released before the engine is opened.
    let identity = session.identity.clone();
    let mapping = session.adapter_details["input_mappings"][0]["engine_table"]
        .as_str()
        .unwrap()
        .to_owned();
    let canonical_root = canonical_root(root);
    assert_eq!(identity.root, canonical_root);
    let mut consumer = BuildStore::open(
        &engine,
        canonical_root.clone(),
        Some(identity.workspace_id.clone()),
        &Resources::default(),
    )
    .unwrap();
    let record = consumer.open_session(&identity).unwrap();
    assert_eq!(record.session, session);
    assert_eq!(record.state, grv_adapter_api::BuildState::Prepared);
    assert!(record.completion.is_none() && record.candidate.is_none() && record.outcome.is_none());
    drop(consumer);
    // The native reader row oracle is executed only for the local slice (no selectors). In
    // the live slice the private input is reached through the view path, and the independent
    // child below reads it with the stock signed reader (no exact GRV connection guard); this
    // test still drives no direct cloud read, so the live value assertions also stay the full
    // source-prepared parquet bytes and the core manifest record above.
    if selectors.is_none() {
        let (schema, table) = mapping.split_once('.').unwrap();
        let database = engine.clone();
        let mut engine = NativeEngine::open(&database).unwrap();
        engine.authorize_relation(schema, table).unwrap();
        let acquired = engine
            .acquire(&format!(
                "SELECT id::BIGINT, note FROM {} ORDER BY id",
                quoted(&mapping)
            ))
            .unwrap();
        assert_eq!(acquired.types.len(), 2);
        assert_eq!(acquired.rows, 1);
        let window = engine
            .fetch_acquired(&acquired, BATCH_ALLOWANCE, SOURCE_ALLOWANCE)
            .unwrap()
            .unwrap();
        assert_eq!(window.row_count(), 1);
        let mut visited = 0;
        window
            .visit(|row, column, scalar| {
                assert_eq!((row, column), (0, visited % 2));
                match visited % 2 {
                    0 => assert_eq!(scalar, Scalar::Integer(2)),
                    _ => assert_eq!(scalar, Scalar::Utf8(b"new")),
                }
                visited += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(visited, 2);
        assert!(
            engine
                .fetch_acquired(&acquired, BATCH_ALLOWANCE, SOURCE_ALLOWANCE)
                .unwrap()
                .is_none()
        );
        drop(engine);
    }
    // The local native guard has closed, and the live slice opens no local guard, so the
    // independent child owns its own C API connection while the parent retains the real
    // cooperative workspace lock.
    external_reader::invoke(&session, &engine, scratch.path(), selectors);
    // Nothing was consumed or published by the independent read: reopened through a separate
    // store handle the consumer session is still exactly Prepared with no candidate,
    // completion or outcome, and the same actual core hold is still active and unreleasable
    // with an immutable journal. No invocation bookkeeping is added for a read-only reader.
    let mut consumer = BuildStore::open(
        &engine,
        canonical_root,
        Some(identity.workspace_id.clone()),
        &Resources::default(),
    )
    .unwrap();
    let record = consumer.open_session(&identity).unwrap();
    assert_eq!(record.session, session);
    assert_eq!(record.state, grv_adapter_api::BuildState::Prepared);
    assert!(record.completion.is_none() && record.candidate.is_none() && record.outcome.is_none());
    drop(consumer);
    let after: HoldRecord = read_record(&store.backend, &expected_hold);
    assert_eq!(after.target_run_id, session.identity.run_id);
    assert!(holds.active(&after).unwrap());
    assert!(!holds.releasable(&after).unwrap());
    absent(&store.backend, release_key(&after).as_str());
    assert_eq!(
        protected_document::read::<Value>(&journal, LIMIT).unwrap(),
        evidence
    );
}

/// Child entry point of the local independent reader; it runs only when the parent
/// driver passed it a private configuration through the environment.
#[test]
fn held_external_reader_child() {
    if std::env::var_os(external_reader::CONFIG_ENV).is_none() {
        return;
    }
    external_reader::child_entry();
}

#[test]
fn local_ordinary_external_session_prepares_evolved_confirmed_revision() {
    let scratch = temp();
    let root = scratch.path().join("grv");
    let (store, _) =
        Store::initialize(LocalBackend::create(&root).unwrap(), init_options()).unwrap();
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

/// Live slice: NOT run by this pass, no live result and no test-side bucket cleanup (no
/// deletes and no changes to any existing resource; the parent alone owns bucket
/// admission, the exact exclusively owned new child root and its
/// all-version/generation-qualified cleanup). It drives the identical generic body over
/// a real `CloudBackend` on that new root, with the fixed selectors passed explicitly to
/// every stage, so the real GCS publication and full source verification stay real
/// while materialization and the native compute stay local. The initialization options
/// are this new root's own and match the local gate (zero skew, one grace, default TTL).
/// Its held preparation is real (real publication, real full source verification over real
/// bytes through the bound backend, with the published reader configuration authenticating
/// the view path), and its row read is executed by the independent child through the stock
/// signed reader. The child also gets the frozen pinned extension directory from the
/// documented environment; no proxy is inherited by it.
fn live_case() {
    let root = std::env::var("GRV_GCS_TEST_ROOT").unwrap();
    assert!(root_is_owned(&root));
    let account = std::env::var("GRV_GCS_ACCOUNT").unwrap();
    let project = std::env::var("GRV_GCS_PROJECT").unwrap();
    assert_eq!(account, "jiacao@on.sinch.com");
    assert_eq!(project, "entit-finance-prod");
    grv_adapter_duckdb::gcs_config::sql_filename(&root).unwrap();
    let (store, _) = Store::initialize(
        CloudBackend::open(
            &root,
            CloudOptions {
                gcs_account: Some(account.clone()),
                gcs_project: Some(project.clone()),
                ..Default::default()
            },
        )
        .unwrap(),
        init_options(),
    )
    .unwrap();
    case(&store, &root, Some((&account, &project)));
}

#[test]
#[ignore = "requires parent-reviewed frozen artifacts and an exact exclusively owned new GCS fixture child; no test-side bucket cleanup"]
fn live_gcs_ordinary_external_session_reads_evolved_confirmed_revision() {
    live_case();
}
