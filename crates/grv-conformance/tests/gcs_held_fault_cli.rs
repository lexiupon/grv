#![cfg(feature = "native-duckdb")]
//! Finite GCS-delegated hold fault boundary. This pass owns one file and one
//! gate shape: the managed source publication runs through the actual CLI with
//! real bytes on real storage, and the ambiguous conditional-write boundary runs
//! through a generic wrapper that delegates every operation to the real backend.
//! The armed exact target is the hold object in the create case and the consumer
//! run control in the confirmation case. Only the acknowledgement of the armed
//! write is modeled as lost (via the public `effect` field) and only that target's
//! readback is afterwards refused.
//! This is explicitly not an actual HTTPS response-loss wire fault and claims
//! nothing about SDK, provider, native writer, or cloud-read code paths.
//!
//! The passing tests run the whole boundary against a real `LocalBackend`; the
//! live `CloudBackend` variants are `#[ignore]`d and have NOT been run here, so
//! they prove nothing yet. The same generic boundary and the same assertion bodies
//! drive both; only the underlying backend differs. The modeled acknowledgement loss
//! and readback refusal stay above the backend, outside HTTPS; the writes
//! themselves are real once the parent driver runs the live slice against GCS.
//! No GcsView reader, no native reader, no cancellation, and no source
//! preparation waiver is claimed or implied for either variant.
use grv_adapter_host::validate_output;
use grv_core::{
    clock::{Clock, SystemClock, new_run_id},
    holds::{Holds, hold_key, release_key},
    ownership::Ownership,
    store::{InitOptions, Store},
};
use grv_storage::{
    Backend, ErrorKind, ListEntry, ListMode, LocalBackend, ObjectKey, ObjectMeta, ObjectPrefix,
    Validator, WriteEffect,
    cloud::{CloudBackend, CloudOptions},
    model::{Counter, HoldRecord, RunControl, RunInput, RunPhase, decode_record, encode_record},
};
use grv_types::{ErrorCode, Name, Uuid};
use serde_json::Value;
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::Path,
    process::Command,
    sync::{
        OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Instant,
};
const LIMIT: usize = 4 * 1024 * 1024;
const OWNERSHIP_TTL: u64 = 900;

/// Strict pure root gate: accept ONLY the canonical exclusively owned child
/// `gs://grv-gcs-held-<24 lowercase hex>/terminal` (the parent driver
/// foundation). No wildcard, prefix, or unknown-resource authority.
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
    c.arg("--json").args(args).env("GRV_ADAPTERS_DIR", adapters);
    // Explicit fixed selected credentials only; no ambient fallback and no
    // GcsView/reader configuration: this source writer is a regular managed build.
    if let Some((account, project)) = selectors {
        c.env("GRV_GCS_ACCOUNT", account)
            .env("GRV_GCS_PROJECT", project);
    }
    let ordinal = COMMAND_ORDINAL.fetch_add(1, Ordering::Relaxed) + 1;
    let base = *COMMAND_BASE.get_or_init(Instant::now);
    eprintln!(
        "held fault command: ordinal={ordinal} stage={} state=start elapsed_ms={}",
        stage_kind(args),
        Instant::now().duration_since(base).as_millis()
    );
    let started = Instant::now();
    let output = c.output().unwrap();
    eprintln!(
        "held fault command: ordinal={ordinal} stage={} state=end duration_ms={}",
        stage_kind(args),
        started.elapsed().as_millis()
    );
    let value: Value = serde_json::from_slice(&output.stdout).expect("CLI must emit authored JSON");
    validate_output(&value).unwrap();
    assert_eq!(output.status.success(), value["ok"] == true);
    value
}
static COMMAND_BASE: OnceLock<Instant> = OnceLock::new();
static COMMAND_ORDINAL: AtomicUsize = AtomicUsize::new(0);
fn stage_kind(args: &[&str]) -> &'static str {
    match args {
        ["push", ..] => "push",
        _ => "other",
    }
}

/// Which conditional write the wrapper watches on the exact target key.
#[derive(Clone, Copy, PartialEq, Eq)]
enum WriteKind {
    Create,
    Put,
}

/// Which exact durable object the fixture arms: the hold object (create boundary)
/// or the consumer run control (confirmation boundary). Closed to these two.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Case {
    Create,
    Confirm,
}
impl Case {
    fn write_kind(self) -> WriteKind {
        match self {
            Case::Create => WriteKind::Create,
            Case::Confirm => WriteKind::Put,
        }
    }
}

/// Test-only generic wrapper owning the real backend. Every operation, including
/// the target write, is delegated for real; only the acknowledgement of an armed
/// exact-target write is modeled as lost, and only that exact target's readback is
/// afterwards refused. It never deletes or corrupts history and never invents a
/// false absence. Bounded local proof boundary, not a universal crash proof.
struct Boundary<B> {
    inner: B,
    target: ObjectKey,
    kind: WriteKind,
    armed: AtomicBool,
    blocked: AtomicBool,
    attempts: AtomicUsize,
    applied: AtomicUsize,
    refused_reads: AtomicUsize,
}
impl<B: Backend> Boundary<B> {
    fn new(inner: B, target: ObjectKey, kind: WriteKind) -> Self {
        Self {
            inner,
            target,
            kind,
            armed: AtomicBool::new(false),
            blocked: AtomicBool::new(false),
            attempts: AtomicUsize::new(0),
            applied: AtomicUsize::new(0),
            refused_reads: AtomicUsize::new(0),
        }
    }
    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
    /// Remove the modeled fault without performing any additional backend effect.
    fn disarm(&self) {
        self.armed.store(false, Ordering::SeqCst);
        self.blocked.store(false, Ordering::SeqCst);
    }
    fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }
    fn applied(&self) -> usize {
        self.applied.load(Ordering::SeqCst)
    }
    fn refused(&self) -> usize {
        self.refused_reads.load(Ordering::SeqCst)
    }
    fn lost(&self) -> grv_storage::Error {
        let mut error = grv_storage::Error::new(ErrorKind::Io, "modeled lost hold acknowledgement");
        error.effect = WriteEffect::MaybeApplied;
        error
    }
    fn watches(&self, key: &ObjectKey, kind: WriteKind) -> bool {
        *key == self.target && self.kind == kind
    }
    fn refuses_read(&self, key: &ObjectKey) -> bool {
        *key == self.target && self.blocked.load(Ordering::SeqCst)
    }
}
impl<B: Backend> Backend for Boundary<B> {
    fn s3_data_uri(&self, key: &ObjectKey) -> grv_storage::Result<Option<String>> {
        self.inner.s3_data_uri(key)
    }
    fn gcs_data_uri(&self, key: &ObjectKey) -> grv_storage::Result<Option<String>> {
        self.inner.gcs_data_uri(key)
    }
    fn gcs_view_eligible(&self) -> grv_storage::Result<()> {
        self.inner.gcs_view_eligible()
    }
    fn get(
        &self,
        key: &ObjectKey,
        sink: &mut dyn std::io::Write,
    ) -> grv_storage::Result<ObjectMeta> {
        if self.refuses_read(key) {
            self.refused_reads.fetch_add(1, Ordering::SeqCst);
            return Err(grv_storage::Error::new(
                ErrorKind::Io,
                "test boundary refuses the ambiguous target readback",
            ));
        }
        self.inner.get(key, sink)
    }
    fn read_range(
        &self,
        key: &ObjectKey,
        expected: &Validator,
        offset: u64,
        length: usize,
        sink: &mut dyn std::io::Write,
    ) -> grv_storage::Result<ObjectMeta> {
        self.inner.read_range(key, expected, offset, length, sink)
    }
    fn head(&self, key: &ObjectKey) -> grv_storage::Result<ObjectMeta> {
        self.inner.head(key)
    }
    fn list(&self, prefix: &ObjectPrefix, mode: ListMode) -> grv_storage::Result<Vec<ListEntry>> {
        self.inner.list(prefix, mode)
    }
    fn delete(&self, key: &ObjectKey) -> grv_storage::Result<()> {
        self.inner.delete(key)
    }
    fn conditional_create(
        &self,
        key: &ObjectKey,
        source: &mut dyn std::io::Read,
    ) -> grv_storage::Result<Validator> {
        let result = self.inner.conditional_create(key, source);
        if self.watches(key, WriteKind::Create) {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            if result.is_ok() {
                self.applied.fetch_add(1, Ordering::SeqCst);
            }
            if self.armed.load(Ordering::SeqCst) && result.is_ok() {
                self.blocked.store(true, Ordering::SeqCst);
                return Err(self.lost());
            }
        }
        result
    }
    fn conditional_put(
        &self,
        key: &ObjectKey,
        expected: &Validator,
        source: &mut dyn std::io::Read,
    ) -> grv_storage::Result<Validator> {
        let result = self.inner.conditional_put(key, expected, source);
        if self.watches(key, WriteKind::Put) {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            if result.is_ok() {
                self.applied.fetch_add(1, Ordering::SeqCst);
            }
            if self.armed.load(Ordering::SeqCst) && result.is_ok() {
                self.blocked.store(true, Ordering::SeqCst);
                return Err(self.lost());
            }
        }
        result
    }
}

fn read_object(b: &impl Backend, path: &ObjectKey) -> (Vec<u8>, Validator) {
    let (bytes, meta) = b.read_bytes(path, LIMIT).unwrap();
    (bytes, meta.validator)
}
fn read_record<T: grv_storage::model::Validate + serde::de::DeserializeOwned>(
    b: &impl Backend,
    path: &ObjectKey,
) -> T {
    decode_record(&read_object(b, path).0).unwrap()
}
fn absent(b: &impl Backend, path: &ObjectKey) {
    assert_eq!(b.head(path).unwrap_err().kind, ErrorKind::NotFound);
}
fn control_path(dataset: &str, control: &RunControl) -> ObjectKey {
    ObjectKey::new(format!(
        "datasets/{dataset}/.runs/{}.control.json",
        control.run_id
    ))
    .unwrap()
}
/// The consumer folder holds only its durable control: no allocation and no output
/// version were created by this boundary.
fn no_consumer_output(b: &impl Backend, control: &ObjectKey) {
    for entry in b
        .list(
            &ObjectPrefix::new("datasets/consumer/").unwrap(),
            ListMode::Recursive,
        )
        .unwrap()
    {
        let ListEntry::Object(key) = entry else {
            panic!("recursive inventory returned prefix")
        };
        assert_eq!(
            &key, control,
            "consumer folder gained an unexpected output effect"
        );
    }
}

fn name(value: &str) -> Name {
    Name::new(value).unwrap()
}

/// One independent fixture on a caller-supplied root and an already-initialized
/// store over the real backend. The managed source publication is one real CLI
/// command writing real bytes; prepare/commit and the hold boundary all go
/// through the actual backend, wrapped only by `Boundary`, which is armed here
/// once the source and the consumer run exist durably. The create target is armed
/// by the fixture; the confirmation target stays unarmed until the case body
/// completes a real unarmed acquire and arms it itself.
fn fixture_with_store<B, F>(
    store: Store<B>,
    root: &str,
    selectors: Option<(&str, &str)>,
    case: Case,
    f: F,
) where
    B: Backend,
    F: FnOnce(&Store<Boundary<B>>, &SystemClock, &Path, &RunControl, &HoldRecord),
{
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
    let source_db = temp.path().join("source.duckdb");
    let seed = temp.path().join("seed.yml");
    fs::write(
        &seed,
        format!("declaration_version: 1\nkind: push\ndataset: source\nadapter: duckdb\nconnection: {{database: {:?}}}\nbuild: {{execution: managed}}\ntables:\n  - name: events\n    source: {{sql: 'SELECT 1::BIGINT AS id'}}\n    columns: [{{name: id, type: int64}}]\n", source_db),
    )
    .unwrap();

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

    let clock = SystemClock::default();
    let ownership = Ownership::new(&store, &clock, OWNERSHIP_TTL).unwrap();
    let owner = ownership
        .prepare_run(
            name("consumer"),
            new_run_id(&clock.now()).unwrap(),
            Counter::from(0),
            vec![RunInput {
                dataset: name("source"),
                revision: Counter::from(1),
                retention_id: Uuid::v4(),
            }],
            None,
        )
        .unwrap();
    ownership.commit_run(&owner).unwrap();
    let record = Holds::new(&store, &clock, temp.path())
        .prepare(&name("consumer"), owner.control())
        .unwrap()
        .remove(0);

    // Arm only now that the source and the consumer run exist durably.
    let Store {
        backend: inner,
        parameters,
    } = store;
    let target = match case {
        Case::Create => hold_key(&record),
        Case::Confirm => control_path("consumer", owner.control()),
    };
    let store = Store {
        backend: Boundary::new(inner, target, case.write_kind()),
        parameters,
    };
    if case == Case::Create {
        store.backend.arm();
    }
    f(&store, &clock, temp.path(), owner.control(), &record);
}

/// Fresh exclusively owned local root and initialized store per test. No env edits,
/// no ambient fallback and no shared resource across cases.
fn local_store() -> (tempfile::TempDir, Store<LocalBackend>) {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let temp = tempfile::tempdir_in(repository).unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let root = temp.path().join("grv");
    let (store, _) = Store::initialize(
        LocalBackend::create(&root).unwrap(),
        InitOptions {
            max_clock_skew: Some(0),
            pending_grace: Some(1),
            ..Default::default()
        },
    )
    .unwrap();
    (temp, store)
}

#[test]
fn local_hold_create_unknown_then_exact_proof_adoption() {
    let (temp, store) = local_store();
    fixture_with_store(
        store,
        temp.path().join("grv").to_str().unwrap(),
        None,
        Case::Create,
        create_unknown_case::<LocalBackend>,
    );
}

#[test]
fn local_hold_confirmation_unknown_then_exact_control_adoption() {
    let (temp, store) = local_store();
    fixture_with_store(
        store,
        temp.path().join("grv").to_str().unwrap(),
        None,
        Case::Confirm,
        confirmation_unknown_case::<LocalBackend>,
    );
}

#[test]
fn root_validator_accepts_only_the_canonical_owned_child() {
    assert!(root_is_owned(&format!(
        "gs://grv-gcs-held-{}/terminal",
        "a".repeat(24)
    )));
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

/// The create-case assertion body, identical for the local and the ignored live
/// slice.
fn create_unknown_case<B: Backend>(
    store: &Store<Boundary<B>>,
    clock: &SystemClock,
    scratch: &Path,
    control: &RunControl,
    record: &HoldRecord,
) {
    let holds = Holds::new(store, clock, scratch);
    let control_path = control_path("consumer", control);
    // The exact target is still absent, so the armed create is the first real
    // effect and only its acknowledgement is modeled as lost.
    assert_eq!(
        store.backend.attempts(),
        0,
        "nothing may reach the backend before arming"
    );
    let unknown = holds.acquire(record).unwrap_err();
    assert_eq!(unknown.code, ErrorCode::OutcomeUnknown);
    assert_eq!(store.backend.attempts(), 1);
    assert_eq!(store.backend.applied(), 1);
    assert_eq!(store.backend.refused(), 1);
    // The real backend really installed the exact bytes and its own validator.
    let before = read_object(&store.backend.inner, &hold_key(record));
    assert_eq!(before.0, encode_record(record).unwrap());
    assert_eq!(
        store
            .backend
            .inner
            .head(&hold_key(record))
            .unwrap()
            .validator,
        before.1
    );
    // Disarm without any further effect and retry the exact identity: the real
    // conditional create now refuses on precondition, so there is still exactly
    // one durable effect even though attempts grew to two.
    store.backend.disarm();
    let proof = holds.acquire(record).unwrap();
    assert_eq!(store.backend.attempts(), 2);
    assert_eq!(store.backend.applied(), 1);
    let after = read_object(&store.backend.inner, &hold_key(record));
    assert_eq!(after.0, before.0);
    assert_eq!(after.1, before.1);
    // The control was unconfirmed throughout the ambiguity and is confirmed only
    // now, through this store's own immutable proof.
    assert!(!read_record::<RunControl>(&store.backend.inner, &control_path).holds_confirmed);
    let confirmed = holds
        .confirm_run(&name("consumer"), control, std::slice::from_ref(&proof))
        .unwrap();
    assert!(confirmed.holds_confirmed);
    // The single durable control write is this legitimate confirmation CAS, and
    // the returned record is exactly what the real backend now holds.
    assert_eq!(
        read_record::<RunControl>(&store.backend.inner, &control_path),
        confirmed
    );
    assert_eq!(
        read_object(&store.backend.inner, &control_path).1,
        store.backend.inner.head(&control_path).unwrap().validator
    );
    assert!(holds.active(record).unwrap());
    absent(&store.backend.inner, &release_key(record));
    no_consumer_output(&store.backend.inner, &control_path);
}

/// The confirmation-case assertion body, identical for the local and the ignored
/// live slice. The exact target is the consumer run control, and it is armed only
/// after a real unarmed `Holds::acquire` has done its real hold write and its full
/// source verification through the same wrapper (so the proof pointer is the very
/// backend `confirm_run` will check). The confirmation CAS then really lands on the
/// backend, but only its acknowledgement is modeled as lost and only its readback
/// is refused, so the caller gets `OutcomeUnknown` with no durable proof. After
/// disarming (no added backend effect), the same original control and the same proof
/// re-read the exact installed record and adopt it with no second CAS: exactly one
/// durable write on the target, the immutable hold evidence untouched, and no
/// allocation or output effect anywhere in the consumer folder.
fn confirmation_unknown_case<B: Backend>(
    store: &Store<Boundary<B>>,
    clock: &SystemClock,
    scratch: &Path,
    control: &RunControl,
    record: &HoldRecord,
) {
    let holds = Holds::new(store, clock, scratch);
    let control_path = control_path("consumer", control);
    let inner = &store.backend.inner;
    assert_eq!(
        store.backend.attempts(),
        0,
        "the confirmation target must start unarmed"
    );
    // The acquire runs for real and unarmed on this target: real hold create, full
    // source verification and the active-hold check, all delegated to the backend.
    let proof = holds.acquire(record).unwrap();
    assert_eq!(store.backend.attempts(), 0);
    assert_eq!(store.backend.applied(), 0);
    assert_eq!(store.backend.refused(), 0);
    // Snapshot the immutable hold evidence straight from the real backend before
    // any fault is armed, and keep the pre-fault control as the caller left it.
    let hold_before = read_object(inner, &hold_key(record));
    assert_eq!(hold_before.0, encode_record(record).unwrap());
    assert_eq!(
        inner.head(&hold_key(record)).unwrap().validator,
        hold_before.1
    );
    assert_eq!(read_record::<RunControl>(inner, &control_path), *control);
    assert!(!control.holds_confirmed);
    assert_eq!(control.phase, RunPhase::Open);
    // Arm the exact target and confirm: the CAS write really lands, its
    // acknowledgement is modeled as lost and its readback is refused, so this
    // caller cannot establish the outcome and gets no authority to proceed.
    store.backend.arm();
    let unknown = holds
        .confirm_run(&name("consumer"), control, std::slice::from_ref(&proof))
        .unwrap_err();
    assert_eq!(unknown.code, ErrorCode::OutcomeUnknown);
    assert_eq!(store.backend.attempts(), 1);
    assert_eq!(store.backend.applied(), 1);
    assert_eq!(store.backend.refused(), 1);
    // The backend really installed the confirmed control even though this caller
    // lost its acknowledgement; the caller's own copy is still unconfirmed.
    let installed = read_record::<RunControl>(inner, &control_path);
    assert!(installed.holds_confirmed);
    assert!(!control.holds_confirmed);
    // Save the actual durable bytes and validator after the unknown outcome, so the
    // adoption can be proven against real backend state and not only counters.
    let durable = read_object(inner, &control_path);
    assert_eq!(inner.head(&control_path).unwrap().validator, durable.1);
    // Disarm without any additional backend effect and retry with the same original
    // control and the same proof: the exact installed record is adopted as-is.
    store.backend.disarm();
    assert_eq!(store.backend.attempts(), 1);
    assert_eq!(store.backend.applied(), 1);
    let adopted = holds
        .confirm_run(&name("consumer"), control, std::slice::from_ref(&proof))
        .unwrap();
    assert_eq!(
        store.backend.attempts(),
        1,
        "adoption must not perform a second CAS write"
    );
    assert_eq!(store.backend.applied(), 1);
    // Identity is exact and durable: same owner and run, the mutation id minted by
    // the single durable write, still an open live control that was never fenced,
    // sealed or treated as absent on the way back.
    assert_eq!(adopted, installed);
    assert_eq!(read_record::<RunControl>(inner, &control_path), adopted);
    assert_eq!(read_object(inner, &control_path), durable);
    assert_eq!(inner.head(&control_path).unwrap().validator, durable.1);
    assert_eq!(adopted.run_id, control.run_id);
    assert_eq!(adopted.owner_token, control.owner_token);
    assert_ne!(adopted.mutation_id, control.mutation_id);
    assert_eq!(adopted.inputs, control.inputs);
    assert_eq!(adopted.phase, RunPhase::Open);
    assert!(adopted.holds_confirmed);
    assert!(adopted.sealed_at.is_none() && adopted.entries.is_none());
    // The immutable hold evidence and its validator survived the fault unchanged, the
    // release marker is genuinely absent, and no consumer output was created.
    assert_eq!(read_object(inner, &hold_key(record)), hold_before);
    assert_eq!(
        inner.head(&hold_key(record)).unwrap().validator,
        hold_before.1
    );
    assert!(holds.active(record).unwrap());
    absent(inner, &release_key(record));
    no_consumer_output(inner, &control_path);
}

/// Live slice: NOT run by this pass, no live result and no test-side cleanup Drop
/// (no deletes; the parent driver owns exact generation cleanup and supplies one
/// fresh exclusively owned root per case). It reuses the generic fixture and the
/// identical assertion bodies over a real `CloudBackend`, so the modeled
/// acknowledgement/readback fault stays above the backend while every write is a
/// real GCS write.
fn live_case(case: Case) {
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
    let body: fn(&Store<Boundary<CloudBackend>>, &SystemClock, &Path, &RunControl, &HoldRecord) =
        match case {
            Case::Create => create_unknown_case,
            Case::Confirm => confirmation_unknown_case,
        };
    fixture_with_store(store, &root, Some((&account, &project)), case, body);
}

#[test]
#[ignore = "requires parent-reviewed frozen artifacts and an exact exclusively owned new GCS fixture child; no test-side bucket cleanup"]
fn live_gcs_hold_create_unknown_then_exact_proof_adoption() {
    live_case(Case::Create);
}

#[test]
#[ignore = "requires parent-reviewed frozen artifacts and an exact exclusively owned new GCS fixture child; no test-side bucket cleanup"]
fn live_gcs_hold_confirmation_unknown_then_exact_control_adoption() {
    live_case(Case::Confirm);
}
