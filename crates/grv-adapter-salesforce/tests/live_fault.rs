//! Opt-in fault selector: --test live_fault
//! live_bulk_response_loss_never_reposts_or_adopts_after_restart -- --ignored --exact
//! Requires sf, curl, python3, an authorized GrvFix__c object, and explicit
//! GRV_SALESFORCE_TEST_ORG / GRV_SALESFORCE_TEST_IDENTITY=salesforce:<18-char org Id>.
//! This creates one real query job, then deletes only that privately captured job.
//! Limitations: the fault is injected after curl receives a successful response;
//! restart means dropping/reopening Journal, not killing the test process. Cleanup
//! runs on unwinding failures, but cannot survive process abort, loss of credentials,
//! or a real response loss before the cleanup-only job ID is captured. No cargo or
//! live execution was performed while writing this test; the parent serializes it.
#[path = "../../grv-conformance/src/release_validation_config.rs"]
#[allow(dead_code)]
mod private_scope;

use grv_adapter_salesforce::{
    Error, Result,
    acquisition::begin_bulk,
    auth::{AuthenticatedSession, BoundConnection, CliAuthentication, OrgId, locate_connection},
    config::{Connection, Transport},
    http::{CurlHttp, HttpExecutor, HttpResponse, SalesforceHttp},
    journal::{AcquisitionIdentity, AcquisitionRecord, AcquisitionState, Journal},
    offline::FileMetadataStore,
    runtime::{self, ProcessSpec},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
use uuid::Uuid;

const FAULT_QUERY: &str = "SELECT Id FROM GrvFix__c LIMIT 1";
const FAULT_API: &str = "v66.0";
const FAULT_SUPERVISOR: &str = env!("CARGO_BIN_EXE_grv-adapter-salesforce");

struct FaultProtectedRoot(PathBuf);
impl FaultProtectedRoot {
    fn new() -> Self {
        let path = fs::canonicalize(env!("CARGO_MANIFEST_DIR"))
            .expect("canonical repository path")
            .join(format!(".live-fault-{}", Uuid::new_v4()));
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            fs::DirBuilder::new()
                .mode(0o700)
                .create(&path)
                .expect("create private repository-local journal root");
        }
        #[cfg(not(unix))]
        panic!("live fault test requires protected Unix journal paths");
        Self(path)
    }
}
impl Drop for FaultProtectedRoot {
    fn drop(&mut self) {
        // Do not mask a test failure with a second panic during unwinding.
        let result = fs::remove_dir_all(&self.0);
        if !std::thread::panicking() {
            result.expect("remove private test journal");
        }
    }
}

fn fault_identity(connection_identity: String) -> AcquisitionIdentity {
    AcquisitionIdentity {
        attempt_id: Uuid::new_v4(),
        table: "grv_fix".into(),
        object: "GrvFix__c".into(),
        request_sha256: format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&json!({
                    "object": "GrvFix__c", "query": FAULT_QUERY,
                    "api_version": FAULT_API, "transport": "bulk", "all_rows": false,
                    "connection_identity": &connection_identity,
                }))
                .expect("serialize fixed test request")
            )
        ),
        connection_identity,
        query_sha256: format!("{:x}", Sha256::digest(FAULT_QUERY)),
        api_version: FAULT_API.into(),
        transport: Transport::Bulk,
        all_rows: false,
    }
}

fn fault_disk_record(root: &Path, identity: &AcquisitionIdentity) -> AcquisitionRecord {
    let path = root.join(format!("{}--{}.json", identity.attempt_id, identity.table));
    let bytes = fs::read(path).expect("creation evidence must already be on disk");
    serde_json::from_slice(&bytes).expect("disk evidence must be a complete record")
}

// This wrapper never reveals a successful creation response to SalesforceHttp.
// Its private job ID is for cleanup only, never for adoption or journal repair.
struct LoseSuccessfulBulkResponse {
    inner: CurlHttp,
    root: PathBuf,
    identity: AcquisitionIdentity,
    calls: usize,
    intent: Option<AcquisitionRecord>,
    private_job: Arc<Mutex<Option<String>>>,
    successful_status: Option<u16>,
}
impl HttpExecutor for LoseSuccessfulBulkResponse {
    fn request(
        &mut self,
        session: &AuthenticatedSession,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
    ) -> Result<HttpResponse> {
        self.calls += 1;
        assert_eq!(
            self.calls, 1,
            "refuse any second transport call, including a status query"
        );
        assert_eq!(
            session.org_id().identity(),
            self.identity.connection_identity
        );
        assert_eq!(method, "POST");
        assert_eq!(path, "/services/data/v66.0/jobs/query");
        let request: Value = serde_json::from_slice(body.expect("Bulk POST body"))
            .expect("Bulk POST must contain JSON");
        assert_eq!(
            request,
            json!({
                "operation": "query", "query": FAULT_QUERY, "contentType": "CSV",
                "columnDelimiter": "COMMA", "lineEnding": "LF",
            })
        );

        // Read the final file, not the still-open Journal's in-memory state.
        let intent = fault_disk_record(&self.root, &self.identity);
        assert_eq!(intent.identity, self.identity);
        assert_eq!(intent.state, AcquisitionState::Intent);
        assert!(intent.job_ids().is_empty());
        assert!(!intent.checkpoint_acked && !intent.reopenable);
        self.intent = Some(intent);

        let response = self.inner.request(session, method, path, body)?;
        assert!(
            (200..=299).contains(&response.status),
            "real Bulk POST must succeed; HTTP status {}",
            response.status
        );
        let value: Value =
            serde_json::from_slice(&response.body).expect("successful Bulk POST must return JSON");
        let job_id = value
            .get("id")
            .and_then(Value::as_str)
            .expect("successful Bulk POST must return a private job ID");
        assert!(
            matches!(job_id.len(), 15 | 18) && job_id.bytes().all(|b| b.is_ascii_alphanumeric()),
            "private job ID must be valid"
        );
        *self.private_job.lock().unwrap() = Some(job_id.to_owned());
        self.successful_status = Some(response.status);
        Err(Error::new(
            "ADAPTER_FAILURE",
            "injected loss of successful Bulk creation response",
        ))
    }
}

// Python receives credentials through a supervised private stdin pipe. It emits
// only a numeric DELETE status, never a URL, job response, exception, or token.
// Redirects/proxies are disabled, and every cleanup request targets the exact ID.
const FAULT_DELETE_SCRIPT: &str = r#"
import json, sys, time, urllib.request, urllib.error
class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None
try:
    data = json.load(sys.stdin)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    url = data['instance'].rstrip('/') + '/services/data/v66.0/jobs/query/' + data['job']
    status = 0
    for attempt in range(20):
        request = urllib.request.Request(url, method='DELETE', headers={
            'Authorization': 'Bearer ' + data['token'], 'Accept': 'application/json'})
        try:
            with opener.open(request, timeout=10) as response:
                status = response.status
        except urllib.error.HTTPError as error:
            status = error.code
            error.close()
        if status not in (400, 409):
            break
        # A just-created query can briefly be in a state that forbids deletion.
        # Retry DELETE only: no listing, polling GET, or adoption.
        time.sleep(1)
    print(json.dumps({'delete_status': status}))
    sys.exit(0 if status == 204 else 1)
except Exception:
    sys.exit(1)
"#;

fn fault_delete_exact_job(session: &AuthenticatedSession, job_id: &str) -> Result<()> {
    let stdin = session.with_credentials(|instance, token| {
        serde_json::to_vec(&json!({"instance": instance, "token": token, "job": job_id}))
            .expect("serialize private cleanup input")
    });
    let output = runtime::run(
        ProcessSpec {
            program: "python3".into(),
            supervisor: Some(FAULT_SUPERVISOR.into()),
            args: vec!["-I".into(), "-c".into(), FAULT_DELETE_SCRIPT.into()],
            env: vec![],
            stdin,
            stdout_limit: 1024,
            timeout: Duration::from_secs(60),
        },
        &Default::default(),
    )?;
    let status = serde_json::from_slice::<Value>(&output.stdout)
        .ok()
        .and_then(|value| value["delete_status"].as_u64());
    if !output.status.success() || status != Some(204) {
        return Err(Error::new(
            "ADAPTER_FAILURE",
            "exact-job cleanup did not confirm DELETE HTTP 204",
        ));
    }
    Ok(())
}

#[test]
#[ignore = "creates one real Bulk job; requires explicit org/18-char identity and sf/curl/python3"]
fn live_bulk_response_loss_never_reposts_or_adopts_after_restart() {
    let scope = private_scope::load().sf;
    let org = scope.org;
    let expected = format!("salesforce:{}", scope.org_id);
    let expected_id = scope.org_id.as_str();
    assert_eq!(OrgId::parse(expected_id).unwrap().identity(), expected);
    let connection = Connection {
        org,
        api_version: FAULT_API.into(),
    };
    let home = std::env::var_os("HOME").expect("HOME required for offline metadata");
    let locator = locate_connection(&connection, &mut FileMetadataStore::for_home(home.into()))
        .expect("locate explicit connection offline");
    let mut bound = BoundConnection::bind(locator).unwrap();
    let mut authentication = CliAuthentication {
        program: "sf".into(),
        supervisor: Some(FAULT_SUPERVISOR.into()),
        verifier: SalesforceHttp {
            executor: CurlHttp {
                supervisor: Some(FAULT_SUPERVISOR.into()),
                ..Default::default()
            },
        },
        cancellation: Default::default(),
    };
    let verified = bound
        .authenticate(&mut authentication, Some(&expected))
        .expect("authenticate and independently verify the explicitly authorized org");
    assert_eq!(verified, expected);
    let session = bound.session().unwrap();
    assert_eq!(session.org_id().as_str(), expected_id);

    let root = FaultProtectedRoot::new();
    let identity = fault_identity(verified);
    let private_job = Arc::new(Mutex::new(None));
    let mut http = SalesforceHttp {
        executor: LoseSuccessfulBulkResponse {
            inner: CurlHttp {
                supervisor: Some(FAULT_SUPERVISOR.into()),
                ..Default::default()
            },
            root: root.0.clone(),
            identity: identity.clone(),
            calls: 0,
            intent: None,
            private_job: private_job.clone(),
            successful_status: None,
        },
    };

    // Catch every fault/assertion after POST so exact-job cleanup also runs on
    // failure. Do not rely on a panicking Drop (which could abort unwinding).
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let journal = Journal::open(&root.0).expect("protected repository ancestors are required");
        assert_eq!(
            begin_bulk(&journal, &identity, FAULT_QUERY, session, &mut http)
                .unwrap_err()
                .code,
            "OUTCOME_UNKNOWN"
        );
        assert_eq!(http.executor.calls, 1);
        assert!(
            http.executor
                .successful_status
                .is_some_and(|status| (200..=299).contains(&status))
        );
        assert!(
            private_job.lock().unwrap().is_some(),
            "real successful POST captured a job for cleanup"
        );
        let evidence = journal.required(&identity).unwrap();
        assert_eq!(evidence.identity, identity);
        assert_eq!(evidence.state, AcquisitionState::OutcomeUnknown);
        assert!(evidence.job_ids().is_empty());
        assert!(!evidence.checkpoint_acked && !evidence.reopenable);
        assert!(evidence.page_locators.is_empty());
        let intent = http.executor.intent.as_ref().unwrap();
        assert_eq!(evidence.snapshot_id, intent.snapshot_id);
        assert_eq!(evidence.capture_start, intent.capture_start);
        assert_eq!(fault_disk_record(&root.0, &identity), evidence);
        let disk_before_restart = fs::read(
            root.0
                .join(format!("{}--{}.json", identity.attempt_id, identity.table)),
        )
        .unwrap();
        drop(journal);

        let reopened = Journal::open(&root.0).unwrap();
        assert_eq!(reopened.required(&identity).unwrap(), evidence);
        assert_eq!(
            begin_bulk(&reopened, &identity, FAULT_QUERY, session, &mut http)
                .unwrap_err()
                .code,
            "OUTCOME_UNKNOWN"
        );
        assert_eq!(
            http.executor.calls, 1,
            "retry must neither repost nor query status/adopt"
        );
        assert_eq!(reopened.required(&identity).unwrap(), evidence);
        assert_eq!(fault_disk_record(&root.0, &identity), evidence);
        assert_eq!(
            fs::read(
                root.0
                    .join(format!("{}--{}.json", identity.attempt_id, identity.table))
            )
            .unwrap(),
            disk_before_restart
        );
    }));

    let job = private_job
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .take();
    let cleanup = job
        .as_deref()
        .map(|job| fault_delete_exact_job(session, job));
    if let Some(result) = cleanup {
        result.expect("must delete exactly the privately captured job, on success or failure");
    }
    if let Err(failure) = outcome {
        resume_unwind(failure);
    }
    assert!(
        job.is_some(),
        "successful fault exercise requires a real job and confirmed cleanup"
    );
}
