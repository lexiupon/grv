#![cfg(unix)]
use grv_adapter_salesforce::{
    Result,
    acquisition::{BulkCreation, SourceHttp},
    auth::*,
    config::Connection,
    http::*,
    offline::FileMetadataStore,
    runtime::*,
};
use serde_json::json;
use std::{
    ffi::OsString,
    fs,
    os::unix::fs::PermissionsExt,
    time::{Duration, Instant},
};

const ORG: &str = "00D000000000001AAA";
fn session() -> AuthenticatedSession {
    AuthenticatedSession::new(
        OrgId::parse(ORG).unwrap(),
        "https://test.my.salesforce.com".into(),
        "credential-canary".into(),
    )
    .unwrap()
}
fn script(root: &std::path::Path, body: &str) -> OsString {
    let path = root.join("helper");
    fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    path.into_os_string()
}
fn spec(body: &str, limit: usize) -> ProcessSpec {
    ProcessSpec {
        supervisor: Some(env!("CARGO_BIN_EXE_grv-adapter-salesforce").into()),
        program: "/bin/sh".into(),
        args: vec!["-c".into(), body.into()],
        env: vec![],
        stdin: vec![],
        stdout_limit: limit,
        timeout: Duration::from_secs(3),
    }
}

#[test]
fn supervised_helper_bounds_output_drains_stderr_and_redacts_failures() {
    let output = run(spec("i=0; while [ $i -lt 10000 ]; do printf 'credential-canary stderr flood\\n' >&2; i=$((i+1)); done; printf 'ok'", 8), &Cancellation::default()).unwrap();
    assert_eq!(output.stdout, b"ok");
    assert!(output.status.success());
    let error = run(
        spec("while :; do printf 'credential-canary'; done", 16),
        &Cancellation::default(),
    )
    .err()
    .unwrap();
    assert_eq!(error.code, "INTEGRITY_FAILURE");
    assert!(!error.message.contains("credential-canary"));
}

#[test]
fn cancellation_stops_helper_and_pipe_holding_descendants_before_return() {
    let cancel = Cancellation::default();
    let child_cancel = cancel.clone();
    let start = Instant::now();
    let worker = std::thread::spawn(move || run(spec("exec sleep 30", 8), &child_cancel));
    std::thread::sleep(Duration::from_millis(50));
    cancel.cancel();
    assert_eq!(
        worker.join().unwrap().err().unwrap().code,
        "EXTRACTION_INCOMPLETE"
    );
    assert!(start.elapsed() < Duration::from_secs(2));
}

#[test]
fn offline_reader_reads_aliases_only_and_never_opens_auth_files() {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join(".sf")).unwrap();
    fs::write(
        root.path().join(".sf/alias.json"),
        br#"{"orgs":{"sample-org":"user@example.org"}}"#,
    )
    .unwrap();
    fs::write(
        root.path().join(".sf/user@example.org.json"),
        "credential-canary invalid auth json",
    )
    .unwrap();
    let mut store = FileMetadataStore::for_home(root.path().into());
    let connection = Connection {
        org: "sample-org".into(),
        api_version: "v66.0".into(),
    };
    let locator = locate_connection(&connection, &mut store).unwrap();
    assert_eq!(locator.canonical_connection.org, "user@example.org");
    assert!(locator.identity.is_none());
    assert_eq!(
        store
            .lookup("unknown@example.org")
            .unwrap()
            .map(|v| v.username),
        None
    );
    assert!(
        !serde_json::to_string(&locator)
            .unwrap()
            .contains("credential-canary")
    );
}

#[test]
fn cli_authentication_disables_file_logs_and_verifies_org_using_private_session() {
    struct Verify;
    impl OrgVerifier for Verify {
        fn verify_org(&mut self, session: &AuthenticatedSession, version: &str) -> Result<OrgId> {
            assert_eq!(version, "v66.0");
            assert!(session.with_credentials(|_, token| token == "credential-canary"));
            OrgId::parse(ORG)
        }
    }
    let root = tempfile::tempdir().unwrap();
    let response = json!({"status":0,"result":{"id":ORG,"instanceUrl":"https://test.my.salesforce.com","accessToken":"credential-canary"}}).to_string();
    let program = script(
        root.path(),
        &format!(
            "[ \"$SF_DISABLE_LOG_FILE\" = true ] || exit 2\n[ \"$SFDX_DISABLE_LOG_FILE\" = true ] || exit 2\n[ \"$SF_TEMP_SHOW_SECRETS\" = true ] || exit 2\n[ -z \"$DEBUG\" ] || exit 2\nprintf '%s' '{response}'"
        ),
    );
    let mut auth = CliAuthentication {
        supervisor: Some(env!("CARGO_BIN_EXE_grv-adapter-salesforce").into()),
        program,
        verifier: Verify,
        cancellation: Default::default(),
    };
    let result = auth
        .authenticate_and_verify(&Connection {
            org: "sample-org".into(),
            api_version: "v66.0".into(),
        })
        .unwrap();
    assert_eq!(result.org_id().as_str(), ORG);
}

#[test]
fn redacted_cli_token_is_refused_before_any_authentication_http_call() {
    struct NeverVerify;
    impl OrgVerifier for NeverVerify {
        fn verify_org(&mut self, _: &AuthenticatedSession, _: &str) -> Result<OrgId> {
            panic!("redacted token must never leave the private CLI boundary")
        }
    }
    let root = tempfile::tempdir().unwrap();
    let response = json!({"status":0,"result":{"id":ORG,"instanceUrl":"https://test.my.salesforce.com","accessToken":"[REDACTED] Use sf org auth show-access-token"}}).to_string();
    let mut auth = CliAuthentication {
        program: script(root.path(), &format!("printf '%s' '{response}'")),
        supervisor: Some(env!("CARGO_BIN_EXE_grv-adapter-salesforce").into()),
        verifier: NeverVerify,
        cancellation: Default::default(),
    };
    let error = auth
        .authenticate_and_verify(&Connection {
            org: "sample-org".into(),
            api_version: "v66.0".into(),
        })
        .err()
        .unwrap();
    assert_eq!(error.code, "ADAPTER_FAILURE");
    assert!(!error.message.contains("REDACTED"));
}

#[test]
fn curl_credentials_use_private_stdin_and_tls_retry_redirect_controls_are_present() {
    let root = tempfile::tempdir().unwrap();
    let program = script(
        root.path(),
        "case \"$*\" in *credential-canary*) exit 3;; esac\ncase \"$*\" in *'--retry 0'*'--max-redirs 0'*'--proto =https'*) ;; *) exit 4;; esac\ninput=; while IFS= read -r line; do input=\"$input$line\"; done\ncase \"$input\" in *'Authorization: Bearer credential-canary'*) ;; *) exit 5;; esac\nprintf 'HTTP/1.1 200 OK\\r\\nContent-Length: 2\\r\\n\\r\\n{}'",
    );
    let mut executor = CurlHttp {
        supervisor: Some(env!("CARGO_BIN_EXE_grv-adapter-salesforce").into()),
        program,
        cancellation: Default::default(),
        timeout: Duration::from_secs(3),
        resources: Default::default(),
    };
    let response = executor
        .request(&session(), "GET", "/services/data/v66.0/query?q=test", None)
        .unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"{}");
}

struct MockHttp {
    calls: Vec<(String, String, Option<Vec<u8>>)>,
    responses: std::collections::VecDeque<HttpResponse>,
}
impl HttpExecutor for MockHttp {
    fn request(
        &mut self,
        _: &AuthenticatedSession,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
    ) -> Result<HttpResponse> {
        self.calls
            .push((method.into(), path.into(), body.map(<[u8]>::to_vec)));
        Ok(self.responses.pop_front().unwrap())
    }
}
fn response(status: u16, body: serde_json::Value) -> HttpResponse {
    HttpResponse {
        status,
        headers: Default::default(),
        body: serde_json::to_vec(&body).unwrap(),
    }
}

#[test]
fn http_query_all_bulk_create_and_results_are_exactly_scoped_and_never_retry_post() {
    let mock = MockHttp {
        calls: vec![],
        responses: vec![
            response(200, json!({"totalSize":0,"done":true,"records":[]})),
            response(500, json!({"error":"ambiguous"})),
            response(200, json!({})),
        ]
        .into(),
    };
    let mut http = SalesforceHttp { executor: mock };
    http.rest_query(
        &session(),
        "v66.0",
        "SELECT Id FROM Case WHERE Name = 'å'",
        true,
    )
    .unwrap();
    assert!(
        http.executor.calls[0]
            .1
            .starts_with("/services/data/v66.0/queryAll?q=")
    );
    assert!(http.executor.calls[0].1.contains("%C3%A5"));
    assert!(matches!(
        http.bulk_create(&session(), "v66.0", "SELECT Id FROM Case", false),
        BulkCreation::Ambiguous
    ));
    assert_eq!(
        http.executor.calls.iter().filter(|c| c.0 == "POST").count(),
        1
    );
    http.bulk_page(
        &session(),
        "v66.0",
        "750000000000001AAA",
        Some("locator+next"),
    )
    .unwrap();
    assert!(
        http.executor.calls[2]
            .1
            .contains("maxRecords=1000&locator=locator%2Bnext")
    );
}

#[test]
fn response_parsing_rejects_duplicate_source_facts_and_rest_duplicate_fields() {
    assert!(
        decode_response(
            b"HTTP/1.1 200 OK\r\nSforce-Locator: a\r\nSforce-Locator: b\r\n\r\nbody".to_vec()
        )
        .is_err()
    );
    assert!(
        grv_adapter_salesforce::acquisition::RestPage::parse(
            br#"{"totalSize":1,"done":true,"records":[{"Id":"a","Id":"b"}]}"#
        )
        .is_err()
    );
    let response = decode_response(
        b"HTTP/1.1 200 Connection established\r\n\r\nHTTP/2 200 OK\r\n\r\n{}".to_vec(),
    )
    .unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"{}");
}
