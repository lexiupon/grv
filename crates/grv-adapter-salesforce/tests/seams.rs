use grv_adapter_salesforce::{
    Result, acquisition::*, auth::*, config::*, conversion::*, journal::*,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::VecDeque,
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
use uuid::Uuid;

const QUERY: &str = "SELECT Id FROM Case";
const ORG: &str = "00D000000000001AAA";
static NEXT_ROOT: AtomicU64 = AtomicU64::new(0);

struct ProtectedRoot(PathBuf);
impl ProtectedRoot {
    fn new() -> Self {
        let root = fs::canonicalize(env!("CARGO_MANIFEST_DIR"))
            .unwrap()
            .join(format!(
                ".journal-test-{}-{}",
                std::process::id(),
                NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
            ));
        fs::create_dir(&root).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        }
        Self(root)
    }
}
impl Drop for ProtectedRoot {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn identity(transport: Transport) -> AcquisitionIdentity {
    AcquisitionIdentity {
        attempt_id: Uuid::new_v4(),
        table: "cases".into(),
        object: "Case".into(),
        request_sha256: "1".repeat(64),
        connection_identity: format!("salesforce:{ORG}"),
        query_sha256: format!("{:x}", Sha256::digest(QUERY)),
        api_version: "v66.0".into(),
        transport,
        all_rows: false,
    }
}
fn session() -> AuthenticatedSession {
    AuthenticatedSession::new(
        OrgId::parse(ORG).unwrap(),
        "https://example.my.salesforce.com".into(),
        "credential-canary".into(),
    )
    .unwrap()
}
fn page(rows: usize, total: u64, next: Option<&str>) -> RestPage {
    RestPage {
        total_size: total,
        done: next.is_none(),
        records: (0..rows).map(|i| json!({"Id": format!("id{i}")})).collect(),
        next_records_url: next.map(str::to_string),
    }
}
struct MockHttp {
    pages: VecDeque<RestPage>,
    rest_calls: usize,
    bulk_calls: usize,
    creation: Option<BulkCreation>,
}
impl MockHttp {
    fn new(pages: Vec<RestPage>) -> Self {
        Self {
            pages: pages.into(),
            rest_calls: 0,
            bulk_calls: 0,
            creation: None,
        }
    }
}
impl SourceHttp for MockHttp {
    fn rest_query(
        &mut self,
        _: &AuthenticatedSession,
        version: &str,
        query: &str,
        all_rows: bool,
    ) -> Result<RestPage> {
        assert_eq!(version, "v66.0");
        assert_eq!(query, QUERY);
        assert!(!all_rows);
        self.rest_calls += 1;
        Ok(self.pages.pop_front().unwrap())
    }
    fn rest_next(&mut self, _: &AuthenticatedSession, _: &str) -> Result<RestPage> {
        self.rest_calls += 1;
        Ok(self.pages.pop_front().unwrap())
    }
    fn bulk_create(&mut self, _: &AuthenticatedSession, _: &str, _: &str, _: bool) -> BulkCreation {
        self.bulk_calls += 1;
        self.creation.take().unwrap_or(BulkCreation::Ambiguous)
    }
}

#[test]
fn pure_defaults_preserve_alias_auto_and_common_authoring_values() {
    let declaration = json!({"kind":"push","adapter":"salesforce","connection":{"org":"sample-org"},"options":{"transport":"auto"},
        "dataset":"credit_notes","tables":[{"name":"cases","source":{"object":"Case","filter":"CALENDAR_MONTH(CreatedDate) = 10 AND Account.Name LIKE 'A%'"},
            "columns":[{"name":"id","source":"Id","type":"utf8"},{"name":"account","source":"Account.Name","type":"utf8"}]}]});
    let effective = validate_binding(&declaration).unwrap();
    assert_eq!(
        effective["connection"],
        json!({"org":"sample-org","api_version":"v66.0"})
    );
    assert_eq!(
        effective["options"],
        json!({"transport":"auto","all_rows":false})
    );
    assert_eq!(effective["tables"], declaration["tables"]);
    assert_eq!(effective["dataset"], declaration["dataset"]);
    assert_eq!(
        resolve_transport(
            Transport::Auto,
            TransportEligibility {
                rest_lossless: true,
                bulk_lossless: true
            }
        )
        .unwrap(),
        Transport::Rest
    );
    assert!(
        resolve_transport(
            Transport::Rest,
            TransportEligibility {
                rest_lossless: false,
                bulk_lossless: true
            }
        )
        .is_err()
    );
}

#[test]
fn schema_descriptors_supply_whole_point_defaults_and_closed_fragments() {
    let registry = extraction_registry();
    assert_eq!(registry.points.len(), 8);
    for point in &registry.points {
        assert!(
            registry
                .schema_bundle
                .pointer(&point.schema_pointer)
                .is_some()
        );
    }
    let options = registry
        .points
        .iter()
        .find(|p| p.point == "options")
        .unwrap();
    assert!(options.has_default);
    assert_eq!(options.default_value, json!({}));
    assert!(
        serde_json::from_value::<Connection>(
            json!({"org":"sample-org","access_token":"forbidden"})
        )
        .is_err()
    );
    assert!(serde_json::from_value::<Options>(json!({"batch_size":1})).is_err());
    assert!(serde_json::from_value::<OrgId>(json!("not-an-org")).is_err());
}

#[test]
fn soql_predicates_allow_unexported_fields_relationships_functions_dates_and_picklists() {
    use grv_adapter_salesforce::predicate::SoqlPredicateCompiler;
    let mut parser = SoqlPredicateCompiler;
    for expression in [
        "Type = 'Credit Note' AND CreditAmount__c > 0",
        "CALENDAR_MONTH(convertTimezone(CreatedDate)) = 10",
        "Account.Name LIKE 'A\\_%' AND NOT (Status = 'Closed' OR IsDeleted = true)",
        "CreatedDate >= LAST_N_DAYS:30 AND SystemModstamp < 2026-10-06T12:00:00+02:00",
        "Picklist__c INCLUDES ('A;B', 'C') AND Picklist__c EXCLUDES ('D')",
        "Id NOT IN ('id1','id2') OR CustomFunction__c(Account.Id, 'text', 12) > 1",
        "CloseDate = 2026-10-06 AND IsClosed != false AND ParentId = null",
        "DISTANCE(Location__c, GEOLOCATION(37.775, -122.418), 'mi') < 20",
    ] {
        parser
            .validate_row_predicate(expression)
            .unwrap_or_else(|error| panic!("{expression}: {error}"));
        let source = Source {
            object: "Case".into(),
            filter: Some(expression.into()),
        };
        assert_eq!(
            generate_query(&source, &["Id".into()], &mut parser).unwrap(),
            format!("SELECT Id FROM Case WHERE {expression}")
        );
    }
}

#[test]
fn soql_predicates_reject_relation_reads_trailing_clauses_and_malformed_grammar() {
    use grv_adapter_salesforce::predicate::SoqlPredicateCompiler;
    for expression in [
        "",
        "Id IN (SELECT Id FROM Account)",
        "Id = 'x' LIMIT 1",
        "Id = 'x'; SELECT Id FROM Account",
        "Id = 'x' -- trailing",
        "Id = :bind",
        "Id",
        "Id == 1",
        "Id <> 1",
        "Id = 'unterminated",
        "Name = 'x' OR",
        "Id IN ()",
        "(Id = 'x'",
    ] {
        assert!(
            SoqlPredicateCompiler
                .validate_row_predicate(expression)
                .is_err(),
            "{expression}"
        );
    }
}

#[test]
fn offline_location_does_not_authenticate_and_authentication_verifies_fixed_identity_once() {
    struct Metadata(usize);
    impl OfflineMetadataStore for Metadata {
        fn lookup(&mut self, alias: &str) -> Result<Option<OfflineMetadata>> {
            self.0 += 1;
            assert_eq!(alias, "sample-org");
            Ok(Some(OfflineMetadata {
                username: "test@example.org".into(),
                org_id: None,
            }))
        }
    }
    struct Auth(usize);
    impl AuthenticationBackend for Auth {
        fn authenticate_and_verify(
            &mut self,
            canonical: &Connection,
        ) -> Result<AuthenticatedSession> {
            self.0 += 1;
            assert_eq!(canonical.org, "test@example.org");
            Ok(session())
        }
    }
    let mut metadata = Metadata(0);
    let mut auth = Auth(0);
    let connection = Connection {
        org: "sample-org".into(),
        api_version: "v66.0".into(),
    };
    let locator = locate_connection(&connection, &mut metadata).unwrap();
    assert_eq!(metadata.0, 1);
    assert_eq!(auth.0, 0);
    assert_eq!(locator.identity, None);
    assert_eq!(locator.engine_path, None);
    assert_eq!(locator.session_lock_path, None);
    let mut bound = BoundConnection::bind(locator).unwrap();
    assert_eq!(auth.0, 0);
    assert_eq!(
        bound.authenticate(&mut auth, None).unwrap(),
        format!("salesforce:{ORG}")
    );
    assert!(bound.authenticate(&mut auth, None).is_err());
    assert_eq!(auth.0, 1);
    let helper = authentication_helper(&connection).unwrap();
    assert!(
        helper
            .env
            .contains(&("SFDX_DISABLE_LOG_FILE".into(), "true".into()))
    );
    assert!(
        helper
            .env
            .contains(&("SF_DISABLE_LOG_FILE".into(), "true".into()))
    );
}

#[test]
fn changed_org_identity_fails_before_acquisition() {
    struct Auth;
    impl AuthenticationBackend for Auth {
        fn authenticate_and_verify(&mut self, _: &Connection) -> Result<AuthenticatedSession> {
            Ok(session())
        }
    }
    let locator = ConnectionLocator {
        canonical_connection: Connection {
            org: "alias".into(),
            api_version: "v66.0".into(),
        },
        identity: None,
        engine_path: None,
        session_lock_path: None,
    };
    let mut bound = BoundConnection::bind(locator).unwrap();
    assert_eq!(
        bound
            .authenticate(&mut Auth, Some("salesforce:another-org"))
            .unwrap_err()
            .code,
        "REQUEST_MISMATCH"
    );
    assert!(bound.session().is_err());
}

#[test]
fn exact_decimals_keep_more_than_binary_float_precision_and_reject_rounding() {
    let value: Value = serde_json::from_str("12345678901234567890123456789012.345678").unwrap();
    assert_eq!(
        json_decimal128(&value, 38, 6).unwrap(),
        Some(12345678901234567890123456789012345678)
    );
    assert_eq!(decimal128("-1.230000e2", 8, 2).unwrap(), -12300);
    assert_eq!(decimal128("-0.000000000000000", 1, 0).unwrap(), 0);
    assert_eq!(decimal128("9.99e-2", 5, 4).unwrap(), 999);
    assert!(decimal128("1.001", 4, 2).is_err());
    assert!(decimal128("1000", 3, 0).is_err());
    for input in [
        "NaN",
        "inf",
        "1e9223372036854775807",
        "1e-9223372036854775808",
        "١",
        "",
        "--1",
        "1.2.3",
    ] {
        assert!(decimal128(input, 38, 6).is_err(), "{input}");
    }
}

#[test]
fn timestamps_preserve_offsets_pre_epoch_values_and_reject_any_precision_loss() {
    assert_eq!(
        timestamp_utc(
            "1970-01-01T01:00:00.123000+0100",
            TimestampUnit::Microsecond
        )
        .unwrap(),
        123000
    );
    assert_eq!(
        timestamp_utc("1969-12-31T23:59:59.999Z", TimestampUnit::Millisecond).unwrap(),
        -1
    );
    assert!(
        timestamp_utc(
            "2020-01-01T00:00:00.1230000001Z",
            TimestampUnit::Microsecond
        )
        .is_err()
    );
    assert!(timestamp_utc("2020-01-01T00:00:00.123001Z", TimestampUnit::Millisecond).is_err());
    assert!(timestamp_utc("2016-12-31T23:59:60Z", TimestampUnit::Microsecond).is_err());
    assert!(timestamp_utc("2020-01-01T00:00:00", TimestampUnit::Microsecond).is_err());
    assert!(timestamp_utc("你好不好呀", TimestampUnit::Microsecond).is_err());
    assert!(timestamp_utc("1970-01-01T00:00:00Z", TimestampUnit::Nanosecond).is_err());
}

#[test]
fn rest_bootstrap_is_private_until_checkpoint_ack_then_pages_and_counts_complete() {
    let root = ProtectedRoot::new();
    let journal = Journal::open(&root.0).unwrap();
    let id = identity(Transport::Rest);
    let mut http = MockHttp::new(vec![
        page(2, 3, Some("/services/data/v66.0/query/locator-1")),
        page(1, 3, None),
    ]);
    let mut acquisition =
        RestAcquisition::begin(&journal, id.clone(), QUERY, &session(), &mut http).unwrap();
    let mut rows = 0;
    let mut sink = |values: &[Value]| {
        rows += values.len();
        Ok(())
    };
    assert_eq!(
        acquisition
            .emit_page(&journal, &session(), &mut http, &mut sink)
            .unwrap_err()
            .code,
        "PROTOCOL_FAILURE"
    );
    let checkpoint = acquisition.checkpoint(&journal).unwrap();
    assert!(!checkpoint.reopenable);
    acquisition
        .acknowledge(&journal, checkpoint.snapshot_id)
        .unwrap();
    assert!(
        !acquisition
            .emit_page(&journal, &session(), &mut http, &mut sink)
            .unwrap()
    );
    assert!(
        acquisition
            .emit_page(&journal, &session(), &mut http, &mut sink)
            .unwrap()
    );
    assert_eq!(rows, 3);
    assert_eq!(http.rest_calls, 2);
    assert!(matches!(
        journal.required(&id).unwrap().state,
        AcquisitionState::Complete { rows: 3, .. }
    ));
    let files: String = fs::read_dir(&root.0)
        .unwrap()
        .filter_map(|p| fs::read_to_string(p.unwrap().path()).ok())
        .collect();
    assert!(!files.contains("credential-canary"));
}

#[test]
fn zero_row_rest_has_empty_job_ids_and_still_requires_checkpoint_ack() {
    let root = ProtectedRoot::new();
    let journal = Journal::open(&root.0).unwrap();
    let id = identity(Transport::Rest);
    let mut http = MockHttp::new(vec![page(0, 0, None)]);
    let mut acquisition =
        RestAcquisition::begin(&journal, id.clone(), QUERY, &session(), &mut http).unwrap();
    let checkpoint = acquisition.checkpoint(&journal).unwrap();
    assert!(checkpoint.job_ids().is_empty());
    assert_eq!(journal.finish(&id, 0).unwrap_err().code, "PROTOCOL_FAILURE");
    acquisition
        .acknowledge(&journal, checkpoint.snapshot_id)
        .unwrap();
    assert!(
        acquisition
            .emit_page(&journal, &session(), &mut http, &mut |_| panic!(
                "empty tables do not emit batches"
            ))
            .unwrap()
    );
}

#[test]
fn incomplete_nonresumable_rest_never_queries_again_under_same_attempt() {
    let root = ProtectedRoot::new();
    let id = identity(Transport::Rest);
    let mut http = MockHttp::new(vec![page(1, 2, Some("/services/data/v66.0/query/locator"))]);
    {
        let journal = Journal::open(&root.0).unwrap();
        RestAcquisition::begin(&journal, id.clone(), QUERY, &session(), &mut http).unwrap();
    }
    let journal = Journal::open(&root.0).unwrap();
    assert!(RestAcquisition::begin(&journal, id, QUERY, &session(), &mut http).is_err());
    assert_eq!(http.rest_calls, 1);
}

#[test]
fn ambiguous_bulk_creation_persists_empty_job_ids_and_never_repeats_post() {
    let root = ProtectedRoot::new();
    let id = identity(Transport::Bulk);
    let mut http = MockHttp::new(vec![]);
    {
        let journal = Journal::open(&root.0).unwrap();
        assert_eq!(
            begin_bulk(&journal, &id, QUERY, &session(), &mut http)
                .unwrap_err()
                .code,
            "OUTCOME_UNKNOWN"
        );
    }
    let journal = Journal::open(&root.0).unwrap();
    let evidence = journal.required(&id).unwrap();
    assert_eq!(evidence.state, AcquisitionState::OutcomeUnknown);
    assert!(evidence.job_ids().is_empty());
    assert_eq!(
        begin_bulk(&journal, &id, QUERY, &session(), &mut http)
            .unwrap_err()
            .code,
        "OUTCOME_UNKNOWN"
    );
    assert_eq!(http.bulk_calls, 1);
}

#[test]
fn crash_after_bulk_intent_is_unknown_and_completed_evidence_reopens_without_source() {
    let root = ProtectedRoot::new();
    let id = identity(Transport::Bulk);
    let mut http = MockHttp::new(vec![]);
    {
        let journal = Journal::open(&root.0).unwrap();
        journal.begin(&id).unwrap();
    }
    let journal = Journal::open(&root.0).unwrap();
    assert_eq!(
        begin_bulk(&journal, &id, QUERY, &session(), &mut http)
            .unwrap_err()
            .code,
        "OUTCOME_UNKNOWN"
    );
    assert_eq!(http.bulk_calls, 0);
    let id2 = identity(Transport::Rest);
    let record = journal.begin(&id2).unwrap();
    journal.created(&id2, vec![], Some(0)).unwrap();
    journal.acknowledge(&id2, record.snapshot_id).unwrap();
    journal.finish(&id2, 0).unwrap();
    assert!(matches!(
        journal.required(&id2).unwrap().state,
        AcquisitionState::Complete { rows: 0, .. }
    ));
}

#[test]
fn count_mismatch_does_not_create_terminal_evidence() {
    let root = ProtectedRoot::new();
    let journal = Journal::open(&root.0).unwrap();
    let id = identity(Transport::Rest);
    let mut http = MockHttp::new(vec![page(1, 2, None)]);
    let mut stream =
        RestAcquisition::begin(&journal, id.clone(), QUERY, &session(), &mut http).unwrap();
    stream
        .acknowledge(&journal, stream.checkpoint(&journal).unwrap().snapshot_id)
        .unwrap();
    assert_eq!(
        stream
            .emit_page(&journal, &session(), &mut http, &mut |_| Ok(()))
            .unwrap_err()
            .code,
        "INTEGRITY_FAILURE"
    );
    assert!(matches!(
        journal.required(&id).unwrap().state,
        AcquisitionState::Active { .. }
    ));
}

#[test]
fn protected_journal_locks_and_rejects_identity_changes_corruption_and_symlinks() {
    let root = ProtectedRoot::new();
    let journal = Journal::open(&root.0).unwrap();
    assert!(Journal::open(&root.0).is_err());
    let id = identity(Transport::Rest);
    journal.begin(&id).unwrap();
    let mut changed = id.clone();
    changed.all_rows = true;
    assert_eq!(
        journal.required(&changed).unwrap_err().code,
        "REQUEST_MISMATCH"
    );
    let path = root.0.join(format!("{}--{}.json", id.attempt_id, id.table));
    fs::write(&path, b"{}").unwrap();
    assert_eq!(journal.required(&id).unwrap_err().code, "INTEGRITY_FAILURE");
    #[cfg(unix)]
    {
        use std::os::unix::fs::{PermissionsExt, symlink};
        fs::remove_file(&path).unwrap();
        symlink("session.lock", &path).unwrap();
        assert!(journal.required(&id).is_err());
        drop(journal);
        fs::set_permissions(&root.0, fs::Permissions::from_mode(0o777)).unwrap();
        assert!(Journal::open(&root.0).is_err());
    }
}

#[test]
fn acquisition_table_names_follow_grv_names_and_persisted_duplicate_keys_are_rejected() {
    let root = ProtectedRoot::new();
    let journal = Journal::open(&root.0).unwrap();
    let mut id = identity(Transport::Rest);
    id.table = "2026-cases".into();
    journal.begin(&id).unwrap();
    let path = root.0.join(format!("{}--{}.json", id.attempt_id, id.table));
    let text = fs::read_to_string(&path).unwrap();
    fs::write(
        path,
        text.replace(
            "\"reopenable\":false",
            "\"reopenable\":false,\"reopenable\":false",
        ),
    )
    .unwrap();
    assert_eq!(journal.required(&id).unwrap_err().code, "INTEGRITY_FAILURE");
}
#[test]
fn malformed_successful_bulk_job_id_is_durably_ambiguous_instead_of_authorizing_recreation() {
    let root = ProtectedRoot::new();
    let journal = Journal::open(&root.0).unwrap();
    let id = identity(Transport::Bulk);
    let mut http = MockHttp::new(vec![]);
    http.creation = Some(BulkCreation::Created {
        job_id: "unresolved".into(),
    });
    assert_eq!(
        begin_bulk(&journal, &id, QUERY, &session(), &mut http)
            .unwrap_err()
            .code,
        "OUTCOME_UNKNOWN"
    );
    assert_eq!(journal.required(&id).unwrap().job_ids(), &[] as &[String]);
    assert_eq!(
        begin_bulk(&journal, &id, QUERY, &session(), &mut http)
            .unwrap_err()
            .code,
        "OUTCOME_UNKNOWN"
    );
    assert_eq!(http.bulk_calls, 1);
}
