use grv_adapter_host::{protected_document, validate_output};
use grv_core::{
    clock::{Clock, SystemClock, new_run_id},
    journal::{Envelope, Journal},
    normalize::TablePlan,
    ownership::{Ownership, ReservationProgress, RunOwner},
    store::{InitOptions, Store},
};
use grv_storage::{
    LocalBackend,
    model::{
        AllocationRecord, AllocationState, ClaimOutcome, ClaimRecord, RunControl, decode_record,
    },
};
use grv_types::{Name, Uuid};
use serde_json::{Value, json};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
};
type Record = Envelope<Value, Value, Value, Value>;
struct Fixture {
    _temp: tempfile::TempDir,
    adapters: PathBuf,
    root: PathBuf,
    state: PathBuf,
    decl: PathBuf,
    context: PathBuf,
    attempt: Uuid,
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
        let package = adapters.join("fixture");
        fs::create_dir_all(&package).unwrap();
        fs::write(package.join("adapter.toml"),format!("name='fixture'\nversion='0.1.0'\ninterface_versions=[1]\nbinding_schema_version=1\nentrypoint={:?}\n",env!("CARGO_BIN_EXE_fixture"))).unwrap();
        let root = temp.path().join("grv");
        let state = temp.path().join("state");
        let decl = temp.path().join("extract.yml");
        let context = temp.path().join("context.json");
        fs::write(&decl,"declaration_version: 1\nkind: push\ndataset: data\nadapter: fixture\nconnection: {}\ntables:\n  - name: rows\n    source: {}\n    columns: [{name: value, source: value, type: int64}]\n  - name: empty\n    source: {}\n    columns: [{name: value, source: value, type: int64}]\n").unwrap();
        Store::initialize(LocalBackend::create(&root).unwrap(), InitOptions::default()).unwrap();
        Self {
            _temp: temp,
            adapters,
            root,
            state,
            decl,
            context,
            attempt: Uuid::v4(),
        }
    }
    fn cli(&self, args: &[&str]) -> Value {
        let output = Command::new(env!("CARGO_BIN_EXE_grv-conformance-cli"))
            .arg("--json")
            .args(args)
            .env("GRV_ADAPTERS_DIR", &self.adapters)
            .output()
            .unwrap();
        let result: Value = serde_json::from_slice(&output.stdout)
            .unwrap_or_else(|e| panic!("{e}: {}", String::from_utf8_lossy(&output.stderr)));
        validate_output(&result).unwrap_or_else(|e| panic!("{e}: {result}"));
        assert_eq!(output.status.success(), result["ok"] == true);
        result
    }
    fn prepare(&self) -> Value {
        self.cli(&[
            "session",
            "prepare",
            "--session",
            self.context.to_str().unwrap(),
            "--grv",
            self.root.to_str().unwrap(),
            "--decl",
            self.decl.to_str().unwrap(),
            "--state",
            self.state.to_str().unwrap(),
            "--attempt",
            self.attempt.as_str(),
        ])
    }
    fn session(&self, command: &str) -> Value {
        self.cli(&[
            "session",
            command,
            "--session",
            self.context.to_str().unwrap(),
        ])
    }
    fn push(&self) -> Value {
        self.cli(&["push", "--session", self.context.to_str().unwrap()])
    }
    fn directory(&self) -> PathBuf {
        self.state.join("push").join(self.attempt.as_str())
    }
    fn journal(&self) -> Journal {
        Journal::open(self.directory(), std::slice::from_ref(&self.root)).unwrap()
    }
    fn store(&self) -> Store<LocalBackend> {
        Store::open(LocalBackend::open(&self.root).unwrap()).unwrap()
    }
    fn record(&self) -> Record {
        self.journal().read().unwrap()
    }
    fn control_path(&self) -> PathBuf {
        let record = self.record();
        self.root.join(format!(
            "datasets/data/.runs/{}.control.json",
            record.evidence.intent["request"]["run_id"]
                .as_str()
                .unwrap()
        ))
    }
    fn remove_source(&self) {
        fs::remove_dir_all(&self.adapters).unwrap();
        if self.decl.exists() {
            fs::remove_file(&self.decl).unwrap();
        }
    }
}
fn tree(root: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    fn read(root: &Path, at: &Path, out: &mut Vec<(PathBuf, Vec<u8>)>) {
        if !at.exists() {
            return;
        }
        for item in fs::read_dir(at).unwrap() {
            let path = item.unwrap().path();
            if path.is_dir() {
                read(root, &path, out);
            } else {
                out.push((
                    path.strip_prefix(root).unwrap().into(),
                    fs::read(path).unwrap(),
                ));
            }
        }
    }
    let mut out = vec![];
    read(root, root, &mut out);
    out.sort();
    out
}
#[test]
fn preparation_queries_no_source_and_show_renew_use_only_locked_backend_evidence() {
    let f = Fixture::new();
    let prepared = f.prepare();
    assert_eq!(prepared["ok"], true, "{prepared}");
    assert_eq!(prepared["result"]["session"]["mode"], "extract");
    assert!(prepared["result"]["engine_path"].is_null());
    assert!(prepared["result"]["session"]["workspace_id"].is_null());
    assert!(prepared["result"]["session"]["capture"].is_null());
    let record = f.record();
    assert_eq!(record.evidence.progress["acquisition"]["started"], false);
    assert!(record.evidence.capture.is_none());
    assert!(record.evidence.progress["push"].is_null());
    assert!(
        tree(&f.root)
            .iter()
            .all(|(p, _)| !p.to_string_lossy().contains("allocations")
                && !p.to_string_lossy().ends_with(".parquet")
                && !p.to_string_lossy().ends_with(".claim"))
    );
    let context = fs::read(&f.context).unwrap();
    let before = tree(&f.root);
    f.remove_source();
    let replay = f.prepare();
    assert_eq!(replay["ok"], true, "{replay}");
    assert_eq!(prepared["result"], replay["result"]);
    assert_eq!(before, tree(&f.root));
    let shown = f.session("show");
    assert_eq!(shown["ok"], true, "{shown}");
    assert_eq!(before, tree(&f.root));
    assert_eq!(f.session("renew")["ok"], true);
    assert_eq!(context, fs::read(&f.context).unwrap());
    let control: RunControl = decode_record(&fs::read(f.control_path()).unwrap()).unwrap();
    assert!(
        !shown.to_string().contains(
            serde_json::to_value(&control.owner_token)
                .unwrap()
                .as_str()
                .unwrap()
        )
    );
    let locked = f.journal();
    let alias = f.context.with_file_name("alias.json");
    fs::copy(&f.context, &alias).unwrap();
    let busy = f.cli(&["session", "renew", "--session", alias.to_str().unwrap()]);
    assert_eq!(busy["errors"][0]["code"], "ENGINE_BUSY");
    drop(locked);
}
#[test]
fn prepared_abort_seals_empty_run_replays_and_permanently_refuses_finalization() {
    let f = Fixture::new();
    assert_eq!(f.prepare()["ok"], true);
    f.remove_source();
    let aborted = f.session("abort");
    assert_eq!(aborted["ok"], true, "{aborted}");
    assert_eq!(aborted["result"]["outcome"]["kind"], "aborted");
    assert_eq!(aborted["result"]["finalized_entries"], json!([]));
    let backend = tree(&f.root);
    let again = f.session("abort");
    assert_eq!(again["result"], aborted["result"]);
    assert_eq!(backend, tree(&f.root));
    let shown = f.session("show");
    assert_eq!(shown["result"]["session"]["run"]["phase"], "sealed");
    assert_eq!(shown["result"]["session"]["outcome"]["kind"], "aborted");
    assert_eq!(f.push()["errors"][0]["code"], "STATE_CONFLICT");
    assert_eq!(f.session("renew")["errors"][0]["code"], "STATE_CONFLICT");
    assert!(
        backend
            .iter()
            .all(|(p, _)| !p.to_string_lossy().contains("allocations")
                && !p.to_string_lossy().ends_with(".claim"))
    );
}
#[test]
fn session_publication_and_terminal_attempt_replay_need_neither_declaration_adapter_nor_backend() {
    let f = Fixture::new();
    assert_eq!(f.prepare()["ok"], true);
    fs::remove_file(&f.decl).unwrap();
    let result = f.push();
    assert_eq!(result["ok"], true, "{result}");
    assert_eq!(result["result"]["outcome"]["revision"], "1");
    let shown = f.session("show");
    assert_eq!(shown["ok"], true, "{shown}");
    assert_eq!(
        shown["result"]["session"]["capture"]["tables"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        shown["result"]["session"]["capture"]["tables"][1]["row_count"],
        "0"
    );
    assert_eq!(
        shown["result"]["session"]["capture"]["source_consistency"],
        "transaction-snapshot"
    );
    assert!(shown["result"]["session"]["workspace_id"].is_null());
    f.remove_source();
    assert_eq!(f.session("show")["ok"], true);
    let aborted = f.session("abort");
    assert_eq!(aborted["result"]["outcome"]["kind"], "published");
    fs::remove_dir_all(&f.root).unwrap();
    let replay = f.push();
    assert_eq!(replay["ok"], true, "{replay}");
    assert_eq!(replay["result"]["replayed"], true);
    let plain = f.cli(&[
        "push",
        "--grv",
        f.root.to_str().unwrap(),
        "--state",
        f.state.to_str().unwrap(),
        "--attempt",
        f.attempt.as_str(),
    ]);
    assert_eq!(plain["ok"], true, "{plain}");
    assert_eq!(plain["result"], replay["result"]);
}
fn blocking_claim(f: &Fixture) -> grv_core::ownership::Reservation {
    let record = f.record();
    let plan: TablePlan =
        serde_json::from_value(record.evidence.intent["plans"][0].clone()).unwrap();
    let store = f.store();
    let clock = SystemClock::default();
    let ownership = Ownership::new(&store, &clock, 900).unwrap();
    let mut owner = ownership
        .prepare_run(
            Name::new("data").unwrap(),
            new_run_id(&clock.now()).unwrap(),
            0.into(),
            vec![],
            None,
        )
        .unwrap();
    ownership.commit_run(&owner).unwrap();
    ownership.confirm_holds(&mut owner).unwrap();
    let intent = ownership
        .prepare_reservation(&owner, plan.layout(), Default::default(), &plan.contract)
        .unwrap();
    ownership
        .reserve_authorized(&owner, &intent, &mut ReservationProgress::Prepared, |_| {
            Ok(())
        })
        .unwrap()
}
#[test]
fn accepted_capture_retries_publish_after_adapter_and_authoring_disappear_without_source_query() {
    let f = Fixture::new();
    assert_eq!(f.prepare()["ok"], true);
    let mut blocking = blocking_claim(&f);
    let blocked = f.push();
    assert_eq!(blocked["ok"], false, "{blocked}");
    let record = f.record();
    assert!(record.evidence.capture.is_some());
    assert_eq!(record.evidence.progress["acquisition"]["stopped"], true);
    let store = f.store();
    let clock = SystemClock::default();
    Ownership::new(&store, &clock, 900)
        .unwrap()
        .release(&mut blocking, ClaimOutcome::Abandoned)
        .unwrap();
    f.remove_source();
    let published = f.push();
    assert_eq!(published["ok"], true, "{published}");
    assert_eq!(published["result"]["outcome"]["kind"], "published");
    assert_eq!(published["result"]["adapter_result"]["rows"], "3");
}
#[test]
fn local_recovery_requires_unlocked_complete_capture_and_exact_original_owner() {
    let f = Fixture::new();
    assert_eq!(f.prepare()["ok"], true);
    let mut blocking = blocking_claim(&f);
    assert_eq!(f.push()["ok"], false);
    let store = f.store();
    let clock = SystemClock::default();
    let ownership = Ownership::new(&store, &clock, 900).unwrap();
    ownership
        .release(&mut blocking, ClaimOutcome::Abandoned)
        .unwrap();
    let journal = f.journal();
    let record: Record = journal.read().unwrap();
    assert!(record.evidence.capture.is_some());
    assert_eq!(record.evidence.progress["acquisition"]["stopped"], true);
    let owner: RunOwner =
        serde_json::from_value(record.evidence.progress["push"]["owner"].clone()).unwrap();
    let phase = record.evidence.progress["push"]["groups"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["phase"] == "reserving")
        .unwrap();
    let intent = serde_json::from_value(phase["intent"].clone()).unwrap();
    let mut progress = serde_json::from_value(phase["progress"].clone()).unwrap();
    let reservation = ownership
        .reserve_authorized(&owner, &intent, &mut progress, |_| Ok(()))
        .unwrap();
    drop(journal);
    let control_path = f.control_path();
    let mut control: RunControl = decode_record(&fs::read(&control_path).unwrap()).unwrap();
    control.expires_at = Some(grv_types::Timestamp::new("2020-01-01T00:00:00Z").unwrap());
    control.mutation_id = Uuid::v4();
    fs::write(
        &control_path,
        grv_storage::model::encode_record(&control).unwrap(),
    )
    .unwrap();
    let claim_path = f.root.join(format!(
        "datasets/data/{}/.claim",
        reservation.allocation().table
    ));
    let mut claim: ClaimRecord = decode_record(&fs::read(&claim_path).unwrap()).unwrap();
    claim.claimed_at = Some(grv_types::Timestamp::new("2020-01-01T00:00:00Z").unwrap());
    claim.expires_at = Some(grv_types::Timestamp::new("2020-01-01T00:15:00Z").unwrap());
    claim.mutation_id = Uuid::v4();
    fs::write(
        &claim_path,
        grv_storage::model::encode_record(&claim).unwrap(),
    )
    .unwrap();
    f.remove_source();
    let recover = || {
        f.cli(&[
            "recover",
            "data",
            "--grv",
            f.root.to_str().unwrap(),
            "--state",
            f.state.to_str().unwrap(),
            "--run",
            control.run_id.as_str(),
        ])
    };
    let writer = f.journal();
    let waiting = recover();
    assert_eq!(waiting["errors"][0]["code"], "STATE_CONFLICT", "{waiting}");
    assert_eq!(
        decode_record::<RunControl>(&fs::read(&control_path).unwrap()).unwrap(),
        control
    );
    assert_eq!(
        decode_record::<ClaimRecord>(&fs::read(&claim_path).unwrap()).unwrap(),
        claim
    );
    drop(writer);
    let mut changed = control.clone();
    changed
        .metadata
        .as_mut()
        .unwrap()
        .insert("substituted-owner".into(), json!(true));
    fs::write(
        &control_path,
        grv_storage::model::encode_record(&changed).unwrap(),
    )
    .unwrap();
    let waiting = recover();
    assert_eq!(waiting["errors"][0]["code"], "STATE_CONFLICT", "{waiting}");
    assert_eq!(
        decode_record::<ClaimRecord>(&fs::read(&claim_path).unwrap()).unwrap(),
        claim
    );
    fs::write(
        &control_path,
        grv_storage::model::encode_record(&control).unwrap(),
    )
    .unwrap();
    let result = recover();
    assert_eq!(result["ok"], true, "{result}");
    assert_eq!(result["result"]["runs"][0]["state"], "sealed");
    assert_eq!(
        decode_record::<RunControl>(&fs::read(&control_path).unwrap())
            .unwrap()
            .phase,
        grv_storage::model::RunPhase::Sealed
    );
    let allocation_path = reservation
        .allocation()
        .claim_token
        .allocation_key(reservation.dataset(), &reservation.allocation().run_id)
        .unwrap();
    assert_eq!(
        decode_record::<AllocationRecord>(
            &fs::read(f.root.join(allocation_path.as_str())).unwrap()
        )
        .unwrap()
        .state,
        AllocationState::Abandoned
    );
    assert_eq!(
        decode_record::<ClaimRecord>(&fs::read(&claim_path).unwrap())
            .unwrap()
            .high_water,
        claim.high_water
    );
    assert!(result.to_string().find("owner_token").is_none());
}
#[test]
fn abort_resolves_exact_allocated_claim_without_allocating_pending_export_work() {
    let f = Fixture::new();
    let mut authored = fs::read_to_string(&f.decl).unwrap();
    authored.push_str("  - name: zpending\n    source: {}\n    columns: [{name: value, source: value, type: int64}]\n");
    fs::write(&f.decl, authored).unwrap();
    assert_eq!(f.prepare()["ok"], true);
    let mut blocking = blocking_claim(&f);
    assert_eq!(f.push()["ok"], false);
    let store = f.store();
    let clock = SystemClock::default();
    let ownership = Ownership::new(&store, &clock, 900).unwrap();
    ownership
        .release(&mut blocking, ClaimOutcome::Abandoned)
        .unwrap();
    let journal = f.journal();
    let record: Record = journal.read().unwrap();
    let owner: RunOwner =
        serde_json::from_value(record.evidence.progress["push"]["owner"].clone()).unwrap();
    let index = record.evidence.progress["push"]["groups"]
        .as_array()
        .unwrap()
        .iter()
        .position(|p| p["phase"] == "reserving")
        .unwrap();
    let intent =
        serde_json::from_value(record.evidence.progress["push"]["groups"][index]["intent"].clone())
            .unwrap();
    let mut progress = serde_json::from_value(
        record.evidence.progress["push"]["groups"][index]["progress"].clone(),
    )
    .unwrap();
    let reservation = ownership
        .reserve_authorized(&owner, &intent, &mut progress, |_| Ok(()))
        .unwrap();
    let mut next = record.evidence.clone();
    next.progress["push"]["groups"][index] = json!({"phase":"reserved","reservation":reservation});
    journal.compare_and_swap(record.generation, next).unwrap();
    drop(journal);
    let highwaters = claim_highwaters(&f.root);
    f.remove_source();
    let aborted = f.session("abort");
    assert_eq!(aborted["ok"], true, "{aborted}");
    assert!(
        aborted["result"]["finalized_entries"]
            .as_array()
            .unwrap()
            .iter()
            .all(|e| e["table"] != "rows")
    );
    let allocation_path = reservation
        .allocation()
        .claim_token
        .allocation_key(reservation.dataset(), &reservation.allocation().run_id)
        .unwrap();
    let allocation: AllocationRecord =
        decode_record(&fs::read(f.root.join(allocation_path.as_str())).unwrap()).unwrap();
    assert_eq!(allocation.state, AllocationState::Abandoned);
    assert_eq!(highwaters, claim_highwaters(&f.root));
    assert!(
        tree(&f.root)
            .iter()
            .all(|(p, _)| !p.to_string_lossy().contains("tables/zpending/"))
    );
    assert_eq!(f.session("abort")["result"], aborted["result"]);
}
#[test]
fn incomplete_capture_and_tampered_context_fail_closed_without_adapter_or_mutations() {
    let f = Fixture::new();
    assert_eq!(f.prepare()["ok"], true);
    let journal = f.journal();
    let record: Record = journal.read().unwrap();
    let mut next = record.evidence.clone();
    next.progress["acquisition"]["started"] = json!(true);
    journal.compare_and_swap(record.generation, next).unwrap();
    drop(journal);
    f.remove_source();
    let before = tree(&f.root);
    assert_eq!(f.push()["errors"][0]["code"], "EXTRACTION_INCOMPLETE");
    assert_eq!(f.session("abort")["errors"][0]["code"], "OUTCOME_UNKNOWN");
    assert_eq!(before, tree(&f.root));
    let mut context: Value = protected_document::read(&f.context, 64 * 1024 * 1024).unwrap();
    context["fixed"]["request"]["connection_identity"] = json!("substituted-source");
    fs::write(&f.context, grv_types::canonical_json(&context).unwrap()).unwrap();
    assert_eq!(f.session("show")["errors"][0]["code"], "REQUEST_MISMATCH");
    assert_eq!(before, tree(&f.root));
}

fn claim_highwaters(root: &Path) -> Vec<(PathBuf, u64)> {
    tree(root)
        .into_iter()
        .filter(|(p, _)| p.to_string_lossy().ends_with(".claim"))
        .map(|(p, bytes)| {
            let claim: ClaimRecord = decode_record(&bytes).unwrap();
            (p, claim.high_water.get())
        })
        .collect()
}
#[test]
fn expired_preparation_stops_capture_and_renewal_but_exact_stopped_abort_remains_safe() {
    let f = Fixture::new();
    assert_eq!(f.prepare()["ok"], true);
    let path = f.control_path();
    let mut control: RunControl = decode_record(&fs::read(&path).unwrap()).unwrap();
    control.expires_at = Some(grv_types::Timestamp::new("2020-01-01T00:00:00Z").unwrap());
    control.mutation_id = Uuid::v4();
    fs::write(&path, grv_storage::model::encode_record(&control).unwrap()).unwrap();
    f.remove_source();
    let before = tree(&f.root);
    assert_eq!(f.push()["errors"][0]["code"], "EXTRACTION_INCOMPLETE");
    assert_eq!(
        f.session("renew")["errors"][0]["code"],
        "EXTRACTION_INCOMPLETE"
    );
    assert_eq!(before, tree(&f.root));
    assert_eq!(
        f.session("show")["result"]["session"]["run"]["expired"],
        true
    );
    let aborted = f.session("abort");
    assert_eq!(aborted["ok"], true, "{aborted}");
    assert_eq!(aborted["result"]["outcome"]["kind"], "aborted");
}
#[test]
fn lost_abort_journal_after_sealed_run_materializes_same_result_without_backend_rewrite() {
    let f = Fixture::new();
    assert_eq!(f.prepare()["ok"], true);
    let before = f.record();
    let aborted = f.session("abort");
    assert_eq!(aborted["ok"], true);
    let journal = f.journal();
    let current: Record = journal.read().unwrap();
    journal
        .compare_and_swap(current.generation, before.evidence)
        .unwrap();
    drop(journal);
    let backend = tree(&f.root);
    f.remove_source();
    let replay = f.session("abort");
    assert_eq!(replay["ok"], true, "{replay}");
    assert_eq!(replay["result"], aborted["result"]);
    assert_eq!(backend, tree(&f.root));
}
