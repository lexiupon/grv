#![cfg(feature = "native-duckdb")]
//! Real elapsed-wall-clock expiry of the run lease through the actual CLI.
//!
//! Scope is one gate only: an actual CLI-prepared external run whose own durable
//! lease lapses through genuine elapsed wall time, after which the CLI refuses
//! `session renew` before any reader or engine restart and before any output
//! effect. Ordinary backend credential reads remain required. Nothing here is a
//! release proof: no GC, no abort, no release marker. The fixture deliberately
//! ends
//! with an expired open run and an active hold, and the parent alone owns bucket
//! admission and all-version/generation-qualified cleanup of this new owned
//! fixture. No fake clock and no durable timestamp editing are used; the store's
//! maximum lease TTL is chosen at initialization of this test-owned root only, so
//! the real wait stays bounded. Storage reads still use ordinary backend
//! credentials: this slice claims nothing about OAuth or a cloud-view source
//! restart, and it acquires no engine or reader session after preparation.
use grv_adapter_api::BuildSession;
use grv_adapter_host::{protected_document, validate_output};
use grv_core::{
    clock::{Clock, SystemClock, parse},
    holds::{Holds, hold_key, release_key},
    store::{InitOptions, Store},
};
use grv_storage::{
    Backend, ErrorKind, ListEntry, ListMode, ObjectKey, ObjectPrefix, Validator,
    cloud::{CloudBackend, CloudOptions},
    model::{HoldRecord, Latest, RunControl, RunPhase, decode_record},
};
use grv_types::{Name, Uuid};
use serde_json::{Value, json};
use std::{
    cmp::min,
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
const LIMIT: u64 = 4 * 1024 * 1024;
/// Offline preparation is a few seconds, so a 30-second store maximum keeps the
/// real wait short. Live preparation keeps the original provider budget, so the
/// test-owned live root gets the largest bounded maximum. Both are initialization
/// choices for this new test root only and change no existing root policy.
const OFFLINE_TTL_SECS: u64 = 30;
const LIVE_TTL_SECS: u64 = 180;
/// Bounds for the genuine wait: preparation may consume up to the original live
/// provider budget, and the expiry wait itself must stay inside this window.
const MAX_PREP_SECS: u64 = 300;
const MAX_LOOP_SECS: u64 = 180;
const MIN_OBSERVED_REMAINING_SECS: u64 = 5;
const SETTLE_SECS: u64 = 1;

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
        ["session", "renew", ..] => "session_renew",
        ["push", ..] => "push",
        ["pull", ..] => "pull",
        _ => "other",
    }
}

/// One chronological diagnostic line: ordinal, stage kind, distinct phase and
/// monotonic millis. No identity or private values.
fn command_line(ordinal: usize, args: &[&str], phase: &str, field: &str, millis: u128) -> String {
    format!(
        "held expiry command: ordinal={ordinal} stage={} state={phase} {field}={millis}",
        stage_kind(args)
    )
}

fn read_object(b: &impl Backend, path: &str) -> (Vec<u8>, Validator) {
    let (bytes, meta) = b
        .read_bytes(&ObjectKey::new(path).unwrap(), LIMIT as usize)
        .unwrap();
    (bytes, meta.validator)
}

fn read_record<T: grv_storage::model::Validate + serde::de::DeserializeOwned>(
    b: &impl Backend,
    path: &str,
) -> T {
    decode_record(&read_object(b, path).0).unwrap()
}

fn absent(b: &impl Backend, path: &str) {
    assert_eq!(
        b.head(&ObjectKey::new(path).unwrap()).unwrap_err().kind,
        ErrorKind::NotFound
    );
}

/// Exact observed object inventory of one prefix. Scoped and finite on purpose:
/// only the target control, hold and source pointer bytes plus validators are
/// compared, together with this closed derived inventory. Whole-tree immutability
/// across every dataset is not measured here and is not claimed.
fn inventory(b: &impl Backend, prefix: &str) -> Vec<String> {
    let mut keys = Vec::new();
    for entry in b
        .list(&ObjectPrefix::new(prefix).unwrap(), ListMode::Recursive)
        .unwrap()
    {
        let ListEntry::Object(key) = entry else {
            panic!("recursive inventory returned prefix")
        };
        keys.push(key.as_str().to_owned());
    }
    keys.sort();
    keys
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
            "an expired open run must not allocate output"
        );
        assert!(
            !key.as_str().contains("/version="),
            "an expired open run must not create output version"
        );
    }
}

/// Elapsed real wall clock past expiry plus the settle margin. Pure predicate:
/// the clock is observed, never adjusted.
fn past_expiry_at(
    now: &grv_types::Timestamp,
    expires_at: &grv_types::Timestamp,
    settle: u64,
) -> bool {
    parse(now)
        .signed_duration_since(parse(expires_at))
        .to_std()
        .is_ok_and(|elapsed| elapsed >= Duration::from_secs(settle))
}

fn past_expiry(clock: &SystemClock, expires_at: &grv_types::Timestamp, settle: u64) -> bool {
    past_expiry_at(&clock.now(), expires_at, settle)
}

fn expiry_slice<B: Backend>(
    store: &Store<B>,
    root: &str,
    selectors: Option<(&str, &str)>,
    ttl: u64,
) {
    let started = Instant::now();
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
    fs::write(&seed,"declaration_version: 1\nkind: push\ndataset: upstream\nadapter: duckdb\nconnection: {database: \"REPLACE_SOURCE\"}\nbuild: {execution: managed}\ntables:\n  - name: rows\n    source: {sql: 'SELECT 1::BIGINT AS id'}\n    columns: [{name: id, type: int64}]\n").unwrap();
    let seed_body = fs::read_to_string(&seed)
        .unwrap()
        .replace("REPLACE_SOURCE", source.to_str().unwrap());
    fs::write(&seed, seed_body).unwrap();
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
    assert_eq!(v["result"]["outcome"]["revision"], "1");

    // Snapshot the committed source pointer exactly as the managed push left it,
    // including its absent lease, before anything else touches the root.
    let latest_key = "datasets/upstream/.states/LATEST";
    let latest_snapshot = read_object(&store.backend, latest_key);
    let latest: Latest = decode_record(&latest_snapshot.0).unwrap();
    assert_eq!(latest.revision.get(), 1);
    assert!(latest.lease.is_none());

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
    // The adapter process is closed when preparation returns, so nothing is left
    // running that could extend this run's lease during the real wait.
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

    let control_path = format!("datasets/derived/.runs/{}.control.json", control.run_id);
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

    // Exact durable bytes and validators before the lease is allowed to lapse.
    let control_snapshot = read_object(&store.backend, &control_path);
    let hold_snapshot = read_object(&store.backend, &hold_path);
    let journal_bytes = fs::read(&journal).unwrap();
    let context_bytes = fs::read(&context).unwrap();
    let derived_inventory = inventory(&store.backend, "datasets/derived/");
    let expires_at = control
        .expires_at
        .clone()
        .expect("prepared run carries a durable expiry");

    // Genuine elapsed wall time: the lease must still be live now, must not
    // exceed this root's initialization maximum, and must be a real wait.
    assert!(
        started.elapsed() <= Duration::from_secs(MAX_PREP_SECS),
        "preparation exceeded the bounded budget: {}s",
        started.elapsed().as_secs()
    );
    let mut remaining = parse(&expires_at)
        .signed_duration_since(parse(&clock.now()))
        .to_std()
        .expect("lease must not have lapsed during preparation");
    assert!(
        remaining <= Duration::from_secs(ttl),
        "observed remaining lease {remaining:?} exceeds the {ttl}s maximum"
    );
    assert!(
        remaining >= Duration::from_secs(MIN_OBSERVED_REMAINING_SECS),
        "observed remaining lease {remaining:?} is too small for a real wait"
    );
    eprintln!(
        "held expiry wait: stage=session_renew state=start remaining_ms={} ttl={ttl}",
        remaining.as_millis()
    );
    let wait_start = Instant::now();
    while !past_expiry(&clock, &expires_at, SETTLE_SECS) {
        assert!(
            wait_start.elapsed() <= Duration::from_secs(MAX_LOOP_SECS + 5),
            "real expiry did not arrive inside the bounded window"
        );
        let step = if remaining.is_zero() {
            Duration::from_millis(100)
        } else {
            min(remaining, Duration::from_secs(1))
        };
        std::thread::sleep(step);
        remaining = remaining.saturating_sub(step);
    }
    eprintln!(
        "held expiry wait: stage=session_renew state=elapsed duration_ms={}",
        wait_start.elapsed().as_millis()
    );

    // The refusal happens before any reader or engine restart and before any
    // output effect: the actual CLI ownership gate reports the expired confirmed
    // run as incomplete. Ordinary backend credential reads stay required here.
    let renew = cli(
        adapters,
        &["session", "renew", "--session", context.to_str().unwrap()],
        selectors,
    );
    assert_eq!(renew["ok"], false, "{renew}");
    assert_eq!(
        renew["errors"][0]["code"], "EXTRACTION_INCOMPLETE",
        "{renew}"
    );
    assert_ne!(renew["errors"][0]["code"], "STATE_CONFLICT");

    // Nothing was written: same durable control and hold bytes and validators,
    // same source pointer, and the closed derived inventory is unchanged. That
    // inventory is unchanged, and output absence is proven by that inventory
    // holding no allocations and no output versions.
    assert_eq!(read_object(&store.backend, &control_path), control_snapshot);
    assert_eq!(read_object(&store.backend, &hold_path), hold_snapshot);
    assert_eq!(read_object(&store.backend, latest_key), latest_snapshot);
    assert_eq!(
        inventory(&store.backend, "datasets/derived/"),
        derived_inventory
    );
    assert_eq!(fs::read(&journal).unwrap(), journal_bytes);
    assert_eq!(fs::read(&context).unwrap(), context_bytes);
    let after: RunControl = read_record(&store.backend, &control_path);
    assert_eq!(after, control);
    assert_eq!(after.phase, RunPhase::Open);
    assert!(after.holds_confirmed);
    assert_eq!(after.expires_at.as_ref(), Some(&expires_at));
    no_derived_output(&store.backend);

    // The hold is still active and still not releasable, and no release marker
    // was written: this slice proves the refusal only, never a release.
    assert!(holds.active(&hold).unwrap());
    assert!(!holds.releasable(&hold).unwrap());
    absent(&store.backend, release_key(&hold).as_str());
    eprintln!(
        "held expiry wait: stage=session_renew state=budget total_ms={}",
        started.elapsed().as_millis()
    );
    // No GC, no abort and no release here: the fixture ends with an expired open
    // run and an active hold for parent-owned cleanup of this new fixture.
}

#[test]
fn past_expiry_gate_is_strict_about_the_settle_margin() {
    let expires = grv_types::Timestamp::new("2026-01-01T00:01:00Z").unwrap();
    let before = grv_types::Timestamp::new("2026-01-01T00:00:59Z").unwrap();
    let exactly = grv_types::Timestamp::new("2026-01-01T00:01:00Z").unwrap();
    let after = grv_types::Timestamp::new("2026-01-01T00:01:01Z").unwrap();
    assert!(!past_expiry_at(&before, &expires, 1), "before expiry");
    assert!(!past_expiry_at(&exactly, &expires, 1), "at expiry");
    assert!(
        past_expiry_at(&after, &expires, 1),
        "past expiry plus settle"
    );
}

#[test]
fn owned_root_gate_is_exact() {
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
            &["pull", "--grv", "gs://bucket/dataset"],
            "start",
            "elapsed_ms",
            0
        ),
        "held expiry command: ordinal=1 stage=pull state=start elapsed_ms=0"
    );
    assert_eq!(
        command_line(
            2,
            &["session", "prepare", "--session", "/p"],
            "end",
            "duration_ms",
            3
        ),
        "held expiry command: ordinal=2 stage=session_prepare state=end duration_ms=3"
    );
    assert_eq!(
        command_line(
            3,
            &["session", "renew", "--session", "/p"],
            "end",
            "duration_ms",
            4
        ),
        "held expiry command: ordinal=3 stage=session_renew state=end duration_ms=4"
    );
    let unknown = command_line(4, &["gc", "upstream", "--apply"], "start", "elapsed_ms", 5);
    assert_eq!(
        unknown,
        "held expiry command: ordinal=4 stage=other state=start elapsed_ms=5"
    );
    assert!(!unknown.contains("gc") && !unknown.contains('/'));
}

fn local_slice(ttl: u64) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("grv");
    let (store, _) = Store::initialize(
        grv_storage::LocalBackend::create(&root).unwrap(),
        InitOptions {
            max_clock_skew: Some(0),
            max_lease_ttl: Some(ttl),
            pending_grace: Some(1),
        },
    )
    .unwrap();
    expiry_slice(&store, root.to_str().unwrap(), None, ttl);
}

#[test]
fn local_expired_confirmed_run_refuses_renew_without_releasing_hold() {
    local_slice(OFFLINE_TTL_SECS);
}

fn live_slice(ttl: u64) {
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
            max_lease_ttl: Some(ttl),
            pending_grace: Some(1),
        },
    )
    .unwrap();
    expiry_slice(&store, &root, Some((&account, &project)), ttl);
}

#[test]
#[ignore = "requires parent-reviewed frozen artifacts and an exact exclusively owned new GCS fixture child; no test-side bucket cleanup"]
fn live_gcs_expired_confirmed_run_refuses_renew_without_releasing_hold() {
    live_slice(LIVE_TTL_SECS);
}
