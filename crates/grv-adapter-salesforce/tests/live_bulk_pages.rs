//! Opt-in: cargo test -p grv-adapter-salesforce --test live_bulk_pages -- --ignored
//! Requires explicit GRV_SALESFORCE_TEST_ORG and
//! GRV_SALESFORCE_TEST_IDENTITY=salesforce:<18-char org Id>, sf/curl/python3,
//! and the fully restored 1,000-row generate.py fixture (900 Active rows).
//! Creates two query jobs, never writes records, and deletes only captured IDs.
//! Exercises real SourceHttp create/poll/results, metadata proof, CSV parsing, Arrow
//! IPC and durable checkpoint acknowledgement. This is an acquisition driver,
//! not an SDK wire/credit test. No live execution was performed while writing it.
//! Drop cleanup covers unwinding, not process abort or lost credentials; an
//! unknown creation response cannot supply a cleanup ID. No job-list adoption.
#![cfg(unix)]

use arrow_array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use grv_adapter_api::{Column, TableContract};
use grv_adapter_salesforce::{
    Result,
    acquisition::{BulkCreation, SourceHttp},
    auth::{AuthenticatedSession, BoundConnection, CliAuthentication, OrgId, locate_connection},
    bulk::{EmptyPolicy, decode_page_bounded},
    config::{Connection, Transport},
    http::{CurlHttp, HttpExecutor, HttpResponse, SalesforceHttp},
    journal::{AcquisitionIdentity, AcquisitionRecord, AcquisitionState, Journal},
    offline::FileMetadataStore,
    row,
    runtime::{self, ProcessSpec},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    os::unix::fs::DirBuilderExt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
use uuid::Uuid;

const API: &str = "v66.0";
const SUPERVISOR: &str = env!("CARGO_BIN_EXE_grv-adapter-salesforce");
const FIELDS: [&str; 7] = [
    "Id",
    "GrvInt__c",
    "GrvDecimal__c",
    "GrvBool__c",
    "GrvDate__c",
    "GrvDateTime__c",
    "GrvPartitionDate__c",
];
const BASE: &str = "GrvStatus__c = 'Active'";
const EMPTY: &str = "GrvStatus__c = 'Active' AND Name = 'GRVFIX-EMPTY-NONEXISTENT'";

struct PrivateRoot(PathBuf);
impl PrivateRoot {
    fn new() -> Self {
        let path = fs::canonicalize(env!("CARGO_MANIFEST_DIR"))
            .unwrap()
            .join(format!(".live-bulk-pages-{}", Uuid::new_v4()));
        fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
        Self(path)
    }
}
impl Drop for PrivateRoot {
    fn drop(&mut self) {
        let result = fs::remove_dir_all(&self.0);
        if !std::thread::panicking() {
            result.expect("remove private journal");
        }
    }
}

fn disk(root: &Path, id: &AcquisitionIdentity) -> AcquisitionRecord {
    serde_json::from_slice(
        &fs::read(root.join(format!("{}--{}.json", id.attempt_id, id.table))).unwrap(),
    )
    .unwrap()
}
fn identity(attempt: Uuid, table: &str, query: &str, connection: &str) -> AcquisitionIdentity {
    AcquisitionIdentity {
        attempt_id: attempt,
        table: table.into(),
        object: "GrvFix__c".into(),
        request_sha256: format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&json!({
                    "table": table, "query": query, "projection": FIELDS, "connection": connection,
                    "api_version": API, "transport": "bulk", "all_rows": false,
                }))
                .unwrap()
            )
        ),
        connection_identity: connection.into(),
        query_sha256: format!("{:x}", Sha256::digest(query)),
        api_version: API.into(),
        transport: Transport::Bulk,
        all_rows: false,
    }
}
fn contract() -> TableContract {
    TableContract {
        columns: [
            ("id", json!("string")),
            ("int_value", json!("int64")),
            (
                "decimal_value",
                json!({"decimal":{"precision":18,"scale":10}}),
            ),
            ("bool_value", json!("boolean")),
            ("date_value", json!("date")),
            (
                "datetime_value",
                json!({"timestamp":{"unit":"us","utc":true}}),
            ),
            ("partition_date", json!("date")),
        ]
        .into_iter()
        .map(|(name, logical_type)| Column {
            name: name.into(),
            logical_type,
        })
        .collect(),
        partition_keys: vec![],
        extensions: json!({}),
        column_ext: json!({}),
    }
}

// Uses a supported smaller Bulk request page size for the scaled correctness
// fixture. Response bytes/locators still pass through unchanged production
// decoding; production maxRecords=1000 defaults are not modified.
// Capture successful POST IDs before production response parsing/journal writes,
// so subsequent failures still have exact-job Drop cleanup.
struct ObserveHttp {
    inner: CurlHttp,
    root: PathBuf,
    identity: AcquisitionIdentity,
    jobs: Arc<Mutex<Vec<String>>>,
    calls: usize,
    pages: Vec<(u64, Option<String>)>,
    status_count: Option<u64>,
}
impl HttpExecutor for ObserveHttp {
    fn request(
        &mut self,
        session: &AuthenticatedSession,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
    ) -> Result<HttpResponse> {
        assert_eq!(
            session.org_id().identity(),
            self.identity.connection_identity
        );
        let prefix = format!("/services/data/{API}/jobs/query");
        let record = disk(&self.root, &self.identity);
        if method == "POST" {
            assert_eq!(path, prefix);
            assert_eq!(record.state, AcquisitionState::Intent);
            assert!(!record.checkpoint_acked && record.job_ids().is_empty());
            let payload: Value = serde_json::from_slice(body.unwrap()).unwrap();
            assert_eq!(payload["operation"], "query");
            assert_eq!(payload["contentType"], "CSV");
            assert_eq!(
                format!("{:x}", Sha256::digest(payload["query"].as_str().unwrap())),
                self.identity.query_sha256
            );
        } else if path.starts_with(&format!("{prefix}/")) {
            assert!(
                record.checkpoint_acked,
                "no polling/result GET before durable ACK"
            );
            assert_eq!(record.job_ids().len(), 1);
            let job_prefix = format!("{prefix}/{}", record.job_ids()[0]);
            if path.contains("/results?") {
                let expected = match record.page_locators.last() {
                    Some(locator) => format!(
                        "{job_prefix}/results?maxRecords=1000&locator={}",
                        grv_adapter_salesforce::http::percent_encode(locator)
                    ),
                    None => format!("{job_prefix}/results?maxRecords=1000"),
                };
                assert_eq!(
                    path, expected,
                    "use unmodified production default page size"
                );
            } else {
                assert_eq!(path, job_prefix);
            }
        } else {
            assert_eq!(method, "GET");
            assert_eq!(
                path,
                format!("/services/data/{API}/sobjects/GrvFix__c/describe")
            );
            assert_eq!(record.state, AcquisitionState::Preparing);
        }
        self.calls += 1;
        let requested = if path.contains("/results?") {
            path.replace("maxRecords=1000", "maxRecords=100")
        } else {
            path.to_owned()
        };
        let response = self.inner.request(session, method, &requested, body)?;
        if method == "POST"
            && (200..300).contains(&response.status)
            && let Ok(value) = serde_json::from_slice::<Value>(&response.body)
            && let Some(id) = value["id"].as_str().filter(|id| {
                matches!(id.len(), 15 | 18) && id.bytes().all(|c| c.is_ascii_alphanumeric())
            })
        {
            self.jobs.lock().unwrap().push(id.into());
        }
        if path.contains("/results?") && response.status == 200 {
            let count = response.headers["sforce-numberofrecords"].parse().unwrap();
            let next = &response.headers["sforce-locator"];
            self.pages
                .push((count, (next != "null").then(|| next.clone())));
        } else if method == "GET"
            && path.starts_with(&format!("{prefix}/"))
            && response.status == 200
        {
            let value: Value = serde_json::from_slice(&response.body).unwrap();
            if value["state"] == "JobComplete" {
                self.status_count = Some(value["numberRecordsProcessed"].as_u64().unwrap());
            }
        }
        Ok(response)
    }
}

// CurlHttp deliberately permits GET/POST only. Cleanup uses a separate private
// supervised Python stdin pipe, with no credentials in argv/output, no proxy or
// redirect, and DELETE only on the exact IDs captured from this test's POSTs.
const DELETE: &str = r#"
import json, sys, time, urllib.request, urllib.error
class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args): return None
try:
    d = json.load(sys.stdin)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    url = d['instance'].rstrip('/') + '/services/data/v66.0/jobs/query/' + d['job']
    status = 0
    for _ in range(20):
        req = urllib.request.Request(url, method='DELETE', headers={'Authorization': 'Bearer ' + d['token']})
        try:
            with opener.open(req, timeout=10) as r: status = r.status
        except urllib.error.HTTPError as e:
            status = e.code
            e.close()
        if status not in (400, 409): break
        time.sleep(1)
    print(status)
    sys.exit(0 if status == 204 else 1)
except Exception: sys.exit(1)
"#;
struct DeleteJobs<'a> {
    session: &'a AuthenticatedSession,
    jobs: Arc<Mutex<Vec<String>>>,
}
impl Drop for DeleteJobs<'_> {
    fn drop(&mut self) {
        let jobs = self.jobs.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let mut failed = false;
        for job in jobs {
            let stdin = self.session.with_credentials(|instance, token| {
                serde_json::to_vec(&json!({"instance":instance,"token":token,"job":job})).unwrap()
            });
            let result = runtime::run(
                ProcessSpec {
                    program: "python3".into(),
                    supervisor: Some(SUPERVISOR.into()),
                    args: vec!["-I".into(), "-c".into(), DELETE.into()],
                    env: vec![],
                    stdin,
                    stdout_limit: 1024,
                    timeout: Duration::from_secs(240),
                },
                &Default::default(),
            );
            failed |= !result.is_ok_and(|out| out.status.success() && out.stdout == b"204\n");
        }
        if failed {
            eprintln!("exact-job DELETE cleanup failed (no credentials or responses logged)");
            assert!(
                std::thread::panicking(),
                "exact-job cleanup must confirm HTTP 204"
            );
        }
    }
}

// Independent Python stdlib arithmetic, not Salesforce/GRV conversion code.
// Expected source semantics come from grvfix_stored_values, not input CSV:
// omitted booleans default false; loaded DateTimes have whole-second precision.
const ORACLE: &str = r#"
import json, sys, runpy
from datetime import date, datetime, timezone
from decimal import Decimal
m = runpy.run_path(json.load(sys.stdin)['generator'])
epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
def days(v): return None if v is None else (date.fromisoformat(v) - epoch.date()).days
def micros(v):
    if v is None: return None
    d = datetime.fromisoformat(v.replace('Z', '+00:00')) - epoch
    return (d.days * 86400 + d.seconds) * 1000000 + d.microseconds
out = {}
for i in range(m['ROWS']):
    v = m['grvfix_stored_values'](i)
    if v['GrvStatus__c'] != 'Active': continue
    dec = v['GrvDecimal__c']
    out[v['Name']] = [v['GrvInt__c'], None if dec is None else str(int(Decimal(dec) * 10**10)),
        v['GrvBool__c'], days(v['GrvDate__c']), micros(v['GrvDateTime__c']), days(v['GrvPartitionDate__c'])]
assert len(out) == 900
json.dump(out, sys.stdout)
"#;
fn oracle() -> BTreeMap<String, Vec<Value>> {
    let generator = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../spec/fixtures/release-validation/salesforce/generate.py")
        .canonicalize()
        .unwrap();
    let out = runtime::run(
        ProcessSpec {
            program: "python3".into(),
            supervisor: Some(SUPERVISOR.into()),
            args: vec!["-I".into(), "-c".into(), ORACLE.into()],
            env: vec![],
            stdin: serde_json::to_vec(&json!({"generator":generator})).unwrap(),
            stdout_limit: 16 * 1024 * 1024,
            timeout: Duration::from_secs(60),
        },
        &Default::default(),
    )
    .unwrap();
    assert!(out.status.success(), "independent Python oracle failed");
    serde_json::from_slice(&out.stdout).unwrap()
}
fn crossmap(
    session: &AuthenticatedSession,
    oracle: &BTreeMap<String, Vec<Value>>,
) -> BTreeMap<String, String> {
    let mut http = SalesforceHttp {
        executor: CurlHttp {
            supervisor: Some(SUPERVISOR.into()),
            ..Default::default()
        },
    };
    let mut page = http
        .rest_query(
            session,
            API,
            &format!("SELECT Id,Name FROM GrvFix__c WHERE {BASE}"),
            false,
        )
        .unwrap();
    let mut ids = BTreeMap::new();
    let mut names = BTreeSet::new();
    let mut locators = BTreeSet::new();
    loop {
        assert_eq!(
            page.total_size, 900,
            "restore the full fixture before running"
        );
        for record in &page.records {
            let id = record["Id"].as_str().unwrap();
            let name = record["Name"].as_str().unwrap();
            assert_eq!(id.len(), 18);
            assert!(id.bytes().all(|c| c.is_ascii_alphanumeric()));
            assert!(oracle.contains_key(name));
            assert!(names.insert(name.to_owned()));
            assert!(ids.insert(id.to_owned(), name.to_owned()).is_none());
        }
        if page.done {
            assert!(page.next_records_url.is_none());
            break;
        }
        let locator = page.next_records_url.as_deref().unwrap();
        assert!(locators.insert(locator.to_owned()));
        page = http.rest_next(session, locator).unwrap();
    }
    assert_eq!(names, oracle.keys().cloned().collect());
    assert_eq!(ids.len(), 900);
    ids
}

// The SDK StopToken has no public standalone constructor. This deliberately
// uses an explicit test-side driver, not a claim to exercise SDK flow control.
fn start_driver(
    journal: &Journal,
    id: &AcquisitionIdentity,
    query: &str,
    session: &AuthenticatedSession,
    http: &mut impl SourceHttp,
    contract: &TableContract,
    sources: &[String],
) -> Vec<EmptyPolicy> {
    assert_eq!(id.connection_identity, session.org_id().identity());
    assert_eq!(id.query_sha256, format!("{:x}", Sha256::digest(query)));
    journal.prepare(id).unwrap();
    let policies = grv_adapter_salesforce::metadata::bulk_policies(
        http,
        session,
        API,
        "GrvFix__c",
        contract,
        sources,
    )
    .unwrap();
    journal.creation_intent(id).unwrap();
    match http.bulk_create(session, API, query, false) {
        BulkCreation::Created { job_id } => {
            journal.created(id, vec![job_id], None).unwrap();
        }
        BulkCreation::Rejected => {
            journal.creation_failed(id, false).unwrap();
            panic!("real Bulk creation rejected");
        }
        BulkCreation::Ambiguous => {
            journal.creation_failed(id, true).unwrap();
            panic!("real Bulk creation unresolved; do not repost");
        }
    }
    policies
}
fn take_page(
    journal: &Journal,
    id: &AcquisitionIdentity,
    session: &AuthenticatedSession,
    http: &mut impl SourceHttp,
    contract: &TableContract,
    sources: &[String],
    policies: &[EmptyPolicy],
) -> Result<grv_adapter_salesforce::bulk::BulkPage> {
    let record = journal.required(id)?;
    if !record.checkpoint_acked {
        return Err(grv_adapter_salesforce::Error::new(
            "INTEGRITY_FAILURE",
            "test driver requires durable checkpoint ACK",
        ));
    }
    assert!(matches!(record.state, AcquisitionState::Active { .. }));
    let job = &record.job_ids()[0];
    if matches!(
        record.state,
        AcquisitionState::Active {
            expected_rows: None,
            ..
        }
    ) {
        let deadline = std::time::Instant::now() + Duration::from_secs(600);
        loop {
            assert!(
                std::time::Instant::now() < deadline,
                "Bulk polling deadline"
            );
            let status = http.bulk_status(session, API, job)?;
            assert_eq!(status["id"].as_str(), Some(job.as_str()));
            match status["state"].as_str() {
                Some("JobComplete") => {
                    journal
                        .expected_rows(id, status["numberRecordsProcessed"].as_u64().unwrap())?;
                    break;
                }
                Some("Open" | "UploadComplete" | "InProgress") => {
                    std::thread::sleep(Duration::from_millis(100))
                }
                _ => panic!("Bulk job failed or returned an unrecognized state"),
            }
        }
    }
    decode_page_bounded(
        http.bulk_page(
            session,
            API,
            job,
            record.page_locators.last().map(String::as_str),
        )?,
        contract,
        sources,
        policies,
        http.source_budget(),
    )
}

fn assert_batch(
    batch: &arrow_array::RecordBatch,
    ids: &BTreeMap<String, String>,
    expected: &BTreeMap<String, Vec<Value>>,
    seen: &mut BTreeSet<String>,
) {
    assert_eq!(batch.num_columns(), 7);
    for i in 0..batch.num_rows() {
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(!id_array.is_null(i));
        let id = id_array.value(i);
        let name = ids
            .get(id)
            .expect("Bulk Id must match REST Name/Id crossmap");
        assert!(seen.insert(id.into()), "duplicate Bulk Id");
        let want = &expected[name];
        for (j, v) in want.iter().enumerate() {
            let array = batch.column(j + 1);
            assert_eq!(
                array.is_null(i),
                v.is_null(),
                "{name}, field {} nullness",
                FIELDS[j + 1]
            );
            if v.is_null() {
                continue;
            }
            let got = match j {
                0 => json!(
                    array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(i)
                ),
                1 => json!(
                    array
                        .as_any()
                        .downcast_ref::<Decimal128Array>()
                        .unwrap()
                        .value(i)
                        .to_string()
                ),
                2 => json!(
                    array
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .value(i)
                ),
                3 | 5 => json!(
                    array
                        .as_any()
                        .downcast_ref::<Date32Array>()
                        .unwrap()
                        .value(i)
                ),
                4 => json!(
                    array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap()
                        .value(i)
                ),
                _ => unreachable!(),
            };
            assert_eq!(&got, v, "{name}, field {} exact value", FIELDS[j + 1]);
        }
    }
}

#[test]
#[ignore = "two real Bulk jobs; explicit pinned org identity and fully restored fixture required"]
fn live_bulk_scaled_pages_exact_oracle_checkpoint_empty_and_cleanup() {
    let org = std::env::var("GRV_SALESFORCE_TEST_ORG")
        .expect("explicit test org required; no default org");
    assert!(!org.trim().is_empty());
    let pinned = std::env::var("GRV_SALESFORCE_TEST_IDENTITY")
        .expect("explicit salesforce:<18-char org Id> required");
    let org_id = pinned.strip_prefix("salesforce:").unwrap();
    assert_eq!(org_id.len(), 18);
    assert_eq!(OrgId::parse(org_id).unwrap().identity(), pinned);
    let connection = Connection {
        org,
        api_version: API.into(),
    };
    let home = std::env::var_os("HOME").expect("HOME for offline connection metadata");
    let locator =
        locate_connection(&connection, &mut FileMetadataStore::for_home(home.into())).unwrap();
    let mut bound = BoundConnection::bind(locator).unwrap();
    let mut auth = CliAuthentication {
        program: "sf".into(),
        supervisor: Some(SUPERVISOR.into()),
        verifier: SalesforceHttp {
            executor: CurlHttp {
                supervisor: Some(SUPERVISOR.into()),
                ..Default::default()
            },
        },
        cancellation: Default::default(),
    };
    assert_eq!(
        bound.authenticate(&mut auth, Some(&pinned)).unwrap(),
        pinned
    );
    let session = bound.session().unwrap();
    assert_eq!(session.org_id().as_str(), org_id);
    let expected = oracle();
    let ids = crossmap(session, &expected); // fail partial fixture before creating jobs
    let root = PrivateRoot::new();
    let jobs = Arc::new(Mutex::new(Vec::new()));
    let cleanup = DeleteJobs {
        session,
        jobs: jobs.clone(),
    };
    let attempt = Uuid::new_v4();
    let contract = contract();
    let sources: Vec<String> = FIELDS.into_iter().map(String::from).collect();
    let mut completed = Vec::new();
    for (table, filter, count) in [("eligible", BASE, 900_u64), ("empty", EMPTY, 0)] {
        let query = format!("SELECT {} FROM GrvFix__c WHERE {filter}", FIELDS.join(","));
        let id = identity(attempt, table, &query, &pinned);
        let journal = Journal::open(&root.0).unwrap();
        let mut http = SalesforceHttp {
            executor: ObserveHttp {
                inner: CurlHttp {
                    supervisor: Some(SUPERVISOR.into()),
                    ..Default::default()
                },
                root: root.0.clone(),
                identity: id.clone(),
                jobs: jobs.clone(),
                calls: 0,
                pages: vec![],
                status_count: None,
            },
        };
        let policies = start_driver(
            &journal, &id, &query, session, &mut http, &contract, &sources,
        );
        let checkpoint = journal.required(&id).unwrap();
        assert_eq!(disk(&root.0, &id), checkpoint);
        assert!(!checkpoint.reopenable && !checkpoint.checkpoint_acked);
        assert_eq!(checkpoint.job_ids().len(), 1);
        assert_eq!(
            checkpoint.source_identity().unwrap(),
            json!({"org_id":org_id,"object":"GrvFix__c","query_sha256":id.query_sha256})
        );
        assert!(matches!(
            checkpoint.state,
            AcquisitionState::Active {
                expected_rows: None,
                ..
            }
        ));
        let calls = http.executor.calls;
        let refusal = take_page(
            &journal, &id, session, &mut http, &contract, &sources, &policies,
        );
        let error = match refusal {
            Err(error) => error,
            Ok(_) => panic!("must refuse before ACK"),
        };
        assert_eq!(error.code, "INTEGRITY_FAILURE");
        assert_eq!(
            http.executor.calls, calls,
            "pre-ACK refusal must not poll/download"
        );
        assert_eq!(
            journal.finish(&id, count).unwrap_err().code,
            "PROTOCOL_FAILURE"
        );
        assert_eq!(
            journal.acknowledge(&id, Uuid::new_v4()).unwrap_err().code,
            "PROTOCOL_FAILURE"
        );
        assert_eq!(disk(&root.0, &id), checkpoint);
        journal.acknowledge(&id, checkpoint.snapshot_id).unwrap();
        assert!(disk(&root.0, &id).checkpoint_acked);
        let mut seen = BTreeSet::new();
        let mut rows = 0_u64;
        let mut batches = 0;
        loop {
            let page = take_page(
                &journal, &id, session, &mut http, &contract, &sources, &policies,
            )
            .unwrap();
            let values = page.rows;
            assert_eq!(http.executor.pages.last().unwrap().0, values.len() as u64);
            rows += values.len() as u64;
            if !values.is_empty() {
                assert_eq!(table, "eligible", "empty acquisition emits no batch");
                let batch = row::decode_rows(&contract, &sources, &values).unwrap();
                let ipc = row::ipc(&batch, 8 * 1024 * 1024).unwrap();
                let decoded = grv_adapter_wire::ipc::decode(&ipc, values.len() as u64).unwrap();
                assert_batch(&decoded, &ids, &expected, &mut seen);
                batches += 1;
            }
            let before_finish = disk(&root.0, &id);
            assert!(
                matches!(before_finish.state, AcquisitionState::Active { expected_rows: Some(n), .. } if n == count)
            );
            assert!(before_finish.capture_window().is_none());
            assert!(
                rows <= count,
                "never capture more than the completed job count"
            );
            if let Some(locator) = page.next_locator {
                journal.bulk_locator(&id, &locator).unwrap();
            } else {
                journal.finish(&id, rows).unwrap();
                break;
            }
        }
        let pages = http.executor.pages.clone();
        assert!(!pages.is_empty());
        assert_eq!(pages.iter().map(|p| p.0).sum::<u64>(), count);
        assert!(pages.iter().all(|p| p.0 <= 100));
        assert!(pages.last().unwrap().1.is_none());
        assert!(pages[..pages.len() - 1].iter().all(|p| p.1.is_some()));
        assert_eq!(http.executor.status_count, Some(count));
        assert_eq!(rows, count);
        if count > 0 {
            assert!(
                pages.len() >= 9,
                "observe >=9 real requested-100 pages, not assumed exact"
            );
            assert_eq!(seen, ids.keys().cloned().collect());
            assert!(batches >= 9);
        } else {
            assert_eq!(batches, 0);
            assert!(seen.is_empty());
        }
        let record = journal.required(&id).unwrap();
        assert_eq!(disk(&root.0, &id), record);
        assert_eq!(record.snapshot_id, checkpoint.snapshot_id);
        assert_eq!(record.capture_start, checkpoint.capture_start);
        assert_eq!(record.job_ids(), checkpoint.job_ids());
        assert!(record.checkpoint_acked && !record.reopenable);
        assert!(matches!(record.state, AcquisitionState::Complete { rows: n, .. } if n == count));
        let locators: Vec<String> = pages.iter().filter_map(|p| p.1.clone()).collect();
        assert_eq!(record.page_locators, locators);
        assert_eq!(
            locators.iter().collect::<BTreeSet<_>>().len(),
            locators.len()
        );
        let window = record.capture_window().unwrap();
        assert!(
            chrono::DateTime::parse_from_rfc3339(window.end.as_str()).unwrap()
                >= chrono::DateTime::parse_from_rfc3339(window.start.as_str()).unwrap()
        );
        drop(journal);
        let reopened = Journal::open(&root.0).unwrap();
        assert_eq!(reopened.required(&id).unwrap(), record);
        let calls = http.executor.calls;
        assert_eq!(
            reopened.prepare(&id).unwrap_err().code,
            "EXTRACTION_INCOMPLETE"
        );
        assert_eq!(
            http.executor.calls, calls,
            "restart must not create/poll/fetch again"
        );
        assert_eq!(disk(&root.0, &id), record);
        eprintln!(
            "{table}: observed {} real pages, {rows} exact rows, {batches} Arrow batches",
            pages.len()
        );
        completed.push(record);
    }
    let captured = jobs.lock().unwrap().clone();
    assert_eq!(captured.len(), 2);
    assert_ne!(captured[0], captured[1]);
    assert_eq!(
        completed
            .iter()
            .flat_map(|r| r.job_ids().iter().cloned())
            .collect::<Vec<_>>(),
        captured
    );
    drop(cleanup); // Drop confirms exact DELETE on normal success as well as unwind.
}
