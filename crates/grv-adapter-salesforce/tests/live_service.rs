//! Explicitly opt-in source validation. This test never creates a Bulk job,
//! writes source records, or contacts a default org chosen by the CLI.
#[path = "../../grv-conformance/src/release_validation_config.rs"]
#[allow(dead_code)]
mod private_scope;

use grv_adapter_api::{Column, TableContract};
use grv_adapter_salesforce::{
    acquisition::SourceHttp,
    auth::{BoundConnection, CliAuthentication, locate_connection},
    config::Connection,
    http::{CurlHttp, HttpExecutor, HttpResponse, SalesforceHttp},
    offline::FileMetadataStore,
    row,
};
use serde_json::json;
use std::sync::{Arc, Mutex};

#[derive(Default, Debug)]
struct HttpDiagnostic {
    status: u16,
    category: &'static str,
    org_id_matches: bool,
}
struct DiagnosticHttp {
    inner: CurlHttp,
    expected: String,
    facts: Arc<Mutex<HttpDiagnostic>>,
}
impl HttpExecutor for DiagnosticHttp {
    fn request(
        &mut self,
        session: &grv_adapter_salesforce::auth::AuthenticatedSession,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
    ) -> grv_adapter_salesforce::Result<HttpResponse> {
        let matches = session.org_id().identity() == self.expected;
        self.facts.lock().unwrap().org_id_matches = matches;
        if !matches {
            return Err(grv_adapter_salesforce::Error::new(
                "REQUEST_MISMATCH",
                "live diagnostic org differs from explicitly supplied identity",
            ));
        }
        let response = self.inner.request(session, method, path, body)?;
        let category = if (200..=299).contains(&response.status) {
            "success"
        } else {
            let value = grv_adapter_salesforce::source_json::parse(
                &response.body,
                response.body.capacity(),
                32 * 1024 * 1024,
            )
            .ok();
            match value
                .as_ref()
                .and_then(|value| value.as_array())
                .and_then(|errors| errors.first())
                .and_then(|error| error.get("errorCode"))
                .and_then(serde_json::Value::as_str)
            {
                Some("INVALID_SESSION_ID") => "INVALID_SESSION_ID",
                Some("API_DISABLED_FOR_ORG") => "API_DISABLED_FOR_ORG",
                Some("INSUFFICIENT_ACCESS") | Some("INSUFFICIENT_ACCESS_OR_READONLY") => {
                    "INSUFFICIENT_ACCESS"
                }
                Some("MALFORMED_QUERY") => "MALFORMED_QUERY",
                Some("INVALID_FIELD") => "INVALID_FIELD",
                Some("NOT_FOUND") => "NOT_FOUND",
                _ => "other rejection",
            }
        };
        let mut facts = self.facts.lock().unwrap();
        facts.status = response.status;
        facts.category = category;
        Ok(response)
    }
}

#[test]
#[ignore = "requires an explicitly provided GRV_SALESFORCE_TEST_ORG and installed sf/curl"]
fn named_org_authentication_metadata_and_read_only_rest_query() {
    let scope = private_scope::load().sf;
    let org = scope.org;
    let expected = format!("salesforce:{}", scope.org_id);
    let connection = Connection {
        org,
        api_version: "v66.0".into(),
    };
    connection.validate().unwrap();
    let home = std::env::var_os("HOME").expect("HOME is required for offline alias metadata");
    let locator =
        locate_connection(&connection, &mut FileMetadataStore::for_home(home.into())).unwrap();
    let canonical = locator.canonical_connection.clone();
    let mut bound = BoundConnection::bind(locator).unwrap();
    let facts = Arc::new(Mutex::new(HttpDiagnostic::default()));
    let backend = CliAuthentication {
        program: "sf".into(),
        supervisor: Some(env!("CARGO_BIN_EXE_grv-adapter-salesforce").into()),
        verifier: SalesforceHttp {
            executor: DiagnosticHttp {
                inner: CurlHttp {
                    supervisor: Some(env!("CARGO_BIN_EXE_grv-adapter-salesforce").into()),
                    ..Default::default()
                },
                expected: expected.clone(),
                facts: facts.clone(),
            },
        },
        cancellation: Default::default(),
    };
    struct Diagnostic<T> {
        backend: T,
        failure: Option<grv_adapter_salesforce::Error>,
    }
    impl<T: grv_adapter_salesforce::auth::AuthenticationBackend>
        grv_adapter_salesforce::auth::AuthenticationBackend for Diagnostic<T>
    {
        fn authenticate_and_verify(
            &mut self,
            canonical: &Connection,
        ) -> grv_adapter_salesforce::Result<grv_adapter_salesforce::auth::AuthenticatedSession>
        {
            let result = self.backend.authenticate_and_verify(canonical);
            if let Err(error) = &result {
                self.failure = Some(error.clone());
            }
            result
        }
    }
    let mut backend = Diagnostic {
        backend,
        failure: None,
    };
    let identity = bound
        .authenticate(&mut backend, Some(&expected))
        .unwrap_or_else(|_| {
            panic!(
                "read-only org verification failed: {:?}; safe HTTP facts: {:?}",
                backend.failure,
                facts.lock().unwrap()
            )
        });
    let session = bound.session().unwrap();
    assert_eq!(identity, session.org_id().identity());
    let mut http = SalesforceHttp {
        executor: CurlHttp {
            supervisor: Some(env!("CARGO_BIN_EXE_grv-adapter-salesforce").into()),
            ..Default::default()
        },
    };
    let describe = http
        .describe(session, &canonical.api_version, "Organization")
        .unwrap();
    assert_eq!(describe["name"], "Organization");
    let contract = TableContract {
        columns: vec![Column {
            name: "id".into(),
            logical_type: json!("string"),
        }],
        partition_keys: vec![],
        extensions: json!({}),
        column_ext: json!({}),
    };
    grv_adapter_salesforce::metadata::rest_projection(
        &mut http,
        session,
        &canonical.api_version,
        "Organization",
        &contract,
        &["Id".into()],
    )
    .unwrap();
    let page = http
        .rest_query(
            session,
            &canonical.api_version,
            "SELECT Id FROM Organization LIMIT 1",
            false,
        )
        .unwrap();
    assert!(page.done && page.next_records_url.is_none());
    assert_eq!(page.total_size, 1);
    let batch = row::decode_rows(&contract, &["Id".into()], &page.records).unwrap();
    let ipc = row::ipc(&batch, 8 * 1024 * 1024).unwrap();
    assert_eq!(
        grv_adapter_wire::ipc::decode(&ipc, 1).unwrap().num_rows(),
        1
    );
}
