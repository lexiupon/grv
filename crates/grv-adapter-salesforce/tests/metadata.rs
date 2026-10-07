use grv_adapter_api::{Column, TableContract};
use grv_adapter_salesforce::{
    Result,
    acquisition::{BulkCreation, RestPage, SourceHttp},
    auth::{AuthenticatedSession, OrgId},
    bulk::EmptyPolicy,
    metadata::{bulk_policies, rest_projection},
};
use serde_json::{Value, json};
use std::collections::BTreeMap;
struct Metadata {
    objects: BTreeMap<String, Value>,
    reads: usize,
}
impl SourceHttp for Metadata {
    fn describe(&mut self, _: &AuthenticatedSession, _: &str, object: &str) -> Result<Value> {
        self.reads += 1;
        Ok(self
            .objects
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(object))
            .unwrap()
            .1
            .clone())
    }
    fn rest_query(
        &mut self,
        _: &AuthenticatedSession,
        _: &str,
        _: &str,
        _: bool,
    ) -> Result<RestPage> {
        panic!("metadata lookup cannot query")
    }
    fn rest_next(&mut self, _: &AuthenticatedSession, _: &str) -> Result<RestPage> {
        panic!("metadata lookup cannot paginate")
    }
    fn bulk_create(&mut self, _: &AuthenticatedSession, _: &str, _: &str, _: bool) -> BulkCreation {
        panic!("metadata lookup cannot create jobs")
    }
}
fn session() -> AuthenticatedSession {
    AuthenticatedSession::new(
        OrgId::parse("00D000000000001AAA").unwrap(),
        "https://test.my.salesforce.com".into(),
        "private".into(),
    )
    .unwrap()
}
fn contract(logicals: &[Value]) -> TableContract {
    TableContract {
        columns: logicals
            .iter()
            .enumerate()
            .map(|(i, logical)| Column {
                name: format!("c{i}"),
                logical_type: logical.clone(),
            })
            .collect(),
        partition_keys: vec![],
        extensions: json!({}),
        column_ext: json!({}),
    }
}
#[test]
fn formula_and_nonnullable_text_are_proven_nullable_numeric_is_exact_and_describe_is_cached() {
    let mut http = Metadata {
        objects: BTreeMap::from([(
            "Case".into(),
            json!({"name":"Case","fields":[
        {"name":"Formula__c","type":"string","calculated":true,"nillable":false},
        {"name":"Amount__c","type":"currency","precision":18,"scale":6,"nillable":true},
        {"name":"Id","type":"id","nillable":false}]}),
        )]),
        reads: 0,
    };
    let policies = bulk_policies(
        &mut http,
        &session(),
        "v66.0",
        "Case",
        &contract(&[
            json!("string"),
            json!({"decimal":{"precision":38,"scale":6}}),
            json!("string"),
        ]),
        &["Formula__c".into(), "Amount__c".into(), "Id".into()],
    )
    .unwrap();
    assert_eq!(
        policies,
        vec![EmptyPolicy::String, EmptyPolicy::Null, EmptyPolicy::Reject]
    );
    assert_eq!(http.reads, 1);
}
#[test]
fn nullable_text_and_nullable_parent_text_refuse_only_the_unproven_csv_representation() {
    let mut http = Metadata {
        objects: BTreeMap::from([
            (
                "Case".into(),
                json!({"name":"Case","fields":[{"name":"Subject","type":"string","nillable":true},{"name":"AccountId","type":"reference","nillable":true,"relationshipName":"Account","referenceTo":["Account"]}]}),
            ),
            (
                "Account".into(),
                json!({"name":"Account","fields":[{"name":"Name","type":"string","nillable":false},{"name":"Amount__c","type":"double","nillable":false}]}),
            ),
        ]),
        reads: 0,
    };
    for source in ["Subject", "Account.Name"] {
        assert_eq!(
            bulk_policies(
                &mut http,
                &session(),
                "v66.0",
                "Case",
                &contract(&[json!("string")]),
                &[source.into()]
            )
            .unwrap_err()
            .code,
            "UNSUPPORTED_CAPABILITY"
        );
    }
    assert_eq!(
        bulk_policies(
            &mut http,
            &session(),
            "v66.0",
            "Case",
            &contract(&[json!("float64")]),
            &["Account.Amount__c".into()]
        )
        .unwrap(),
        vec![EmptyPolicy::Null]
    );
    assert!(
        bulk_policies(
            &mut http,
            &session(),
            "v66.0",
            "Case",
            &contract(&[json!("string")]),
            &["Account.Amount__c".into()]
        )
        .is_err()
    );
}
#[test]
fn polymorphic_relationships_require_all_target_representations_to_agree() {
    let mut http = Metadata {
        objects: BTreeMap::from([
            (
                "Task".into(),
                json!({"name":"Task","fields":[{"name":"WhoId","type":"reference","nillable":false,"relationshipName":"Who","referenceTo":["Contact","Lead"]}]}),
            ),
            (
                "Contact".into(),
                json!({"name":"Contact","fields":[{"name":"Name","type":"string","nillable":false}]}),
            ),
            (
                "Lead".into(),
                json!({"name":"Lead","fields":[{"name":"Name","type":"string","nillable":true}]}),
            ),
        ]),
        reads: 0,
    };
    assert!(
        bulk_policies(
            &mut http,
            &session(),
            "v66.0",
            "Task",
            &contract(&[json!("string")]),
            &["Who.Name".into()]
        )
        .is_err()
    );
}

#[test]
fn custom_number_scale_zero_has_exact_int64_descriptor_proof() {
    for (precision, scale, accepted) in [
        (10, 0, true),
        (18, 0, true),
        (19, 0, false),
        (10, 1, false),
        (0, 0, false),
    ] {
        let mut http = Metadata {
            objects: BTreeMap::from([(
                "Case".into(),
                json!({"name":"Case","fields":[
                    {"name":"Integer__c","type":"double","precision":precision,"scale":scale,"nillable":true}
                ]}),
            )]),
            reads: 0,
        };
        let declared = contract(&[json!("int64")]);
        let selector = ["Integer__c".into()];
        assert_eq!(
            rest_projection(&mut http, &session(), "v66.0", "Case", &declared, &selector).is_ok(),
            accepted
        );
        assert_eq!(
            bulk_policies(&mut http, &session(), "v66.0", "Case", &declared, &selector).is_ok(),
            accepted
        );
    }
    assert!(grv_adapter_salesforce::row::int64(&json!(1.5)).is_err());
    assert!(grv_adapter_salesforce::row::int64(&json!("9223372036854775808")).is_err());
}

#[test]
fn both_transports_prove_decimal_widths_and_forbid_cross_domain_casts_without_rows() {
    let description = json!({"name":"Case","fields":[
        {"name":"Amount__c","type":"currency","precision":18,"scale":6,"nillable":true},
        {"name":"Count__c","type":"int","nillable":true},
        {"name":"Float__c","type":"double","precision":18,"scale":6,"nillable":true},
        {"name":"CreatedDate","type":"datetime","nillable":true}]});
    let mut http = Metadata {
        objects: BTreeMap::from([("Case".into(), description)]),
        reads: 0,
    };
    for (field, logical) in [
        ("Amount__c", json!({"decimal":{"precision":17,"scale":6}})),
        ("Amount__c", json!({"decimal":{"precision":38,"scale":5}})),
        ("Amount__c", json!("int64")),
        ("Amount__c", json!("float64")),
        ("Count__c", json!("float64")),
        ("Float__c", json!("int64")),
        (
            "CreatedDate",
            json!({"timestamp":{"unit":"us","utc":false}}),
        ),
    ] {
        for bulk in [false, true] {
            let declared = contract(std::slice::from_ref(&logical));
            let selector = [field.into()];
            let error = if bulk {
                bulk_policies(&mut http, &session(), "v66.0", "Case", &declared, &selector)
                    .map(|_| ())
            } else {
                rest_projection(&mut http, &session(), "v66.0", "Case", &declared, &selector)
            }
            .unwrap_err();
            assert_eq!(error.code, "INVALID_DECLARATION");
            assert!(error.message.contains("column c0"));
        }
    }
    rest_projection(
        &mut http,
        &session(),
        "v66.0",
        "Case",
        &contract(&[
            json!({"decimal":{"precision":38,"scale":6}}),
            json!("float64"),
            json!("int64"),
        ]),
        &["Amount__c".into(), "Float__c".into(), "Count__c".into()],
    )
    .unwrap();
}

#[test]
fn rest_preserves_nullable_text_encodings_and_checks_every_polymorphic_target_width() {
    let mut http = Metadata {
        objects: BTreeMap::from([
            (
                "Task".into(),
                json!({"name":"Task","fields":[{"name":"Text__c","type":"string","nillable":true,"calculated":true},{"name":"WhoId","type":"reference","relationshipName":"Who","referenceTo":["Contact","Lead"],"nillable":true}]}),
            ),
            (
                "Contact".into(),
                json!({"name":"Contact","fields":[{"name":"Amount__c","type":"currency","precision":18,"scale":6,"nillable":true}]}),
            ),
            (
                "Lead".into(),
                json!({"name":"Lead","fields":[{"name":"Amount__c","type":"currency","precision":18,"scale":9,"nillable":true}]}),
            ),
        ]),
        reads: 0,
    };
    for logical in [
        json!("string"),
        json!("int64"),
        json!("float64"),
        json!("date"),
        json!({"decimal":{"precision":38,"scale":6}}),
        json!({"timestamp":{"unit":"us","utc":true}}),
    ] {
        rest_projection(
            &mut http,
            &session(),
            "v66.0",
            "Task",
            &contract(&[logical]),
            &["Text__c".into()],
        )
        .unwrap();
    }
    assert!(
        rest_projection(
            &mut http,
            &session(),
            "v66.0",
            "Task",
            &contract(&[json!({"decimal":{"precision":38,"scale":6}})]),
            &["Who.Amount__c".into()]
        )
        .is_err()
    );
    rest_projection(
        &mut http,
        &session(),
        "v66.0",
        "Task",
        &contract(&[json!({"decimal":{"precision":38,"scale":9}})]),
        &["Who.Amount__c".into()],
    )
    .unwrap();
}

#[test]
fn failed_projection_records_intent_and_never_queries_zero_or_all_null_attempts() {
    use grv_adapter_salesforce::{
        acquisition::RestAcquisition,
        config::Transport,
        journal::{AcquisitionIdentity, AcquisitionState, Journal},
    };
    for scale in [0, 6] {
        let temp = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR")).unwrap();
        std::fs::create_dir(temp.path().join("journal")).unwrap();
        let journal = Journal::open(&temp.path().join("journal")).unwrap();
        let query = "SELECT Amount__c FROM Case";
        let identity = AcquisitionIdentity {
            attempt_id: uuid::Uuid::new_v4(),
            table: "rows".into(),
            object: "Case".into(),
            request_sha256: "a".repeat(64),
            connection_identity: session().org_id().identity(),
            query_sha256: grv_types::sha256(query.as_bytes()).as_str().into(),
            api_version: "v66.0".into(),
            transport: Transport::Rest,
            all_rows: false,
        };
        let mut http = Metadata {
            objects: BTreeMap::from([(
                "Case".into(),
                json!({"name":"Case","fields":[{"name":"Amount__c","type":"currency","precision":18,"scale":6,"nillable":true}]}),
            )]),
            reads: 0,
        };
        let declared = contract(&[json!({"decimal":{"precision":17,"scale":scale}})]);
        let error = RestAcquisition::begin_projected(
            &journal,
            identity.clone(),
            query,
            &session(),
            &mut http,
            &declared,
            &["Amount__c".into()],
        )
        .err()
        .unwrap();
        assert_eq!(error.code, "INVALID_DECLARATION");
        assert_eq!(
            journal.required(&identity).unwrap().state,
            AcquisitionState::Preparing
        );
        assert_eq!(http.reads, 1);
        assert_eq!(
            RestAcquisition::begin_projected(
                &journal,
                identity,
                query,
                &session(),
                &mut http,
                &declared,
                &["Amount__c".into()]
            )
            .err()
            .unwrap()
            .code,
            "EXTRACTION_INCOMPLETE"
        );
        assert_eq!(http.reads, 1);
    }
}

#[test]
fn api_names_match_case_insensitively_and_ambiguous_descriptors_are_refused() {
    let mut http = Metadata {
        objects: BTreeMap::from([
            (
                "Case".into(),
                json!({"name":"Case","fields":[{"name":"AccountId","relationshipName":"Account","type":"reference","referenceTo":["Account"],"nillable":true}]}),
            ),
            (
                "Account".into(),
                json!({"name":"Account","fields":[{"name":"Name","type":"string","nillable":true}]}),
            ),
        ]),
        reads: 0,
    };
    rest_projection(
        &mut http,
        &session(),
        "v66.0",
        "case",
        &contract(&[json!("string")]),
        &["account.NAME".into()],
    )
    .unwrap();
    http.objects.get_mut("Account").unwrap()["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"NAME","type":"string","nillable":true}));
    assert_eq!(
        rest_projection(
            &mut http,
            &session(),
            "v66.0",
            "CASE",
            &contract(&[json!("string")]),
            &["ACCOUNT.name".into()]
        )
        .unwrap_err()
        .code,
        "INTEGRITY_FAILURE"
    );
}
