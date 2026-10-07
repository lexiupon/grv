use arrow_array::{Array, Decimal128Array, Int64Array, TimestampNanosecondArray};
use grv_adapter_api::{Column, TableContract};
use grv_adapter_salesforce::{bulk::*, http::HttpResponse, row::*};
use serde_json::{Value, json};

fn contract(types: Vec<(&str, Value)>) -> TableContract {
    TableContract {
        columns: types
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

#[test]
fn all_client_primitives_decode_to_exact_logical_arrow_and_valid_ipc() {
    let contract = contract(vec![
        ("boolean", json!("boolean")),
        ("integer", json!("int64")),
        ("float", json!("float64")),
        ("text", json!("string")),
        ("binary", json!("binary")),
        ("date", json!("date")),
        ("decimal", json!({"decimal":{"precision":38,"scale":6}})),
        ("local_ms", json!({"timestamp":{"unit":"ms","utc":false}})),
        ("local_us", json!({"timestamp":{"unit":"us","utc":false}})),
        ("local_ns", json!({"timestamp":{"unit":"ns","utc":false}})),
        ("utc_ms", json!({"timestamp":{"unit":"ms","utc":true}})),
        ("utc_us", json!({"timestamp":{"unit":"us","utc":true}})),
    ]);
    let sources: Vec<_> = contract.columns.iter().map(|c| c.name.clone()).collect();
    let row = json!({"boolean":true,"integer":"9223372036854775807","float":"0.12500","text":"å\ntext","binary":"AAH/","date":"1969-12-31","decimal":"12345678901234567890123456789012.345678","local_ms":"1969-12-31T23:59:59.999","local_us":"1970-01-01T00:00:00.000001","local_ns":"1970-01-01T00:00:00.000000001","utc_ms":"1970-01-01T01:00:00.123+0100","utc_us":"1970-01-01T00:00:00.123456Z"});
    let batch = decode_rows(
        &contract,
        &sources,
        &[
            row.clone(),
            Value::Object(
                contract
                    .columns
                    .iter()
                    .map(|column| (column.name.clone(), Value::Null))
                    .collect(),
            ),
        ],
    )
    .unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert!(batch.columns().iter().all(|array| array.is_null(1)));
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        i64::MAX
    );
    assert_eq!(
        batch
            .column(6)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        12345678901234567890123456789012345678
    );
    assert_eq!(
        batch
            .column(9)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .value(0),
        1
    );
    let bytes = ipc(&batch, 8 * 1024 * 1024).unwrap();
    assert_eq!(
        grv_adapter_wire::ipc::decode(&bytes, 2).unwrap().num_rows(),
        2
    );
}

#[test]
fn primitive_decoders_reject_rounding_overflow_lossy_casts_and_missing_fields() {
    for value in [json!("9223372036854775808"), json!("1.1"), json!(true)] {
        assert!(int64(&value).is_err());
    }
    assert_eq!(int64(&json!("1.00e3")).unwrap(), Some(1000));
    for value in [
        json!("9007199254740993"),
        json!("0.10000000000000001"),
        json!("NaN"),
        json!("1e999"),
    ] {
        assert!(float64(&value).is_err());
    }
    assert!(date32("2026-02-30").is_err());
    assert!(date32("2026-2-3").is_err());
    assert!(field(&json!({"Account":{}}), "Account.Name").is_err());
    assert!(
        field(&json!({"Account":null}), "Account.Name")
            .unwrap()
            .is_null()
    );
    let c = contract(vec![("name", json!("string"))]);
    assert!(decode_rows(&c, &["Id".into()], &[json!({"Id":1})]).is_err());
}

fn csv_response(body: &str, count: u64) -> HttpResponse {
    HttpResponse {
        status: 200,
        headers: std::collections::BTreeMap::from([
            ("sforce-numberofrecords".into(), count.to_string()),
            ("sforce-locator".into(), "null".into()),
        ]),
        body: body.as_bytes().to_vec(),
    }
}

#[test]
fn bulk_csv_exact_quote_newline_numeric_boolean_null_policy_and_count_decoding() {
    let c = contract(vec![
        ("text", json!("string")),
        ("decimal", json!({"decimal":{"precision":38,"scale":6}})),
        ("boolean", json!("boolean")),
    ]);
    let sources = vec!["Name".into(), "Amount__c".into(), "Active__c".into()];
    let page=decode_page(csv_response("Name,Amount__c,Active__c\n\"comma,quote\"\"newline\ntext\",12345678901234567890123456789012.345678,true\n,,false\n",2),&c,&sources,&[EmptyPolicy::Null,EmptyPolicy::Null,EmptyPolicy::Null]).unwrap();
    assert_eq!(page.rows[0]["Name"], "comma,quote\"newline\ntext");
    assert!(page.rows[1]["Name"].is_null());
    assert!(page.next_locator.is_none());
    let batch = decode_rows(&c, &sources, &page.rows).unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert!(
        decode_page(
            csv_response("Name,Amount__c,Active__c\n,,false\n", 1),
            &c,
            &sources,
            &[EmptyPolicy::Reject, EmptyPolicy::Null, EmptyPolicy::Null]
        )
        .is_err()
    );
    assert!(
        decode_page(
            csv_response("Name,Amount__c,Active__c\nvalue,1,true\n", 2),
            &c,
            &sources,
            &[EmptyPolicy::Null; 3]
        )
        .is_err()
    );
    assert!(
        decode_page(
            csv_response("Name,Name,Active__c\nvalue,1,true\n", 1),
            &c,
            &sources,
            &[EmptyPolicy::Null; 3]
        )
        .is_err()
    );
}

#[test]
fn bulk_encoded_and_decoded_buffers_share_source_budget_and_ipc_output_is_bounded() {
    let c = contract(vec![("text", json!("string"))]);
    let body = format!("Name\n{}\n", "x".repeat(300_000));
    assert!(
        decode_page_bounded(
            csv_response(&body, 1),
            &c,
            &["Name".into()],
            &[EmptyPolicy::String],
            1024 * 1024
        )
        .is_err()
    );
    let page = decode_page_bounded(
        csv_response(&body, 1),
        &c,
        &["Name".into()],
        &[EmptyPolicy::String],
        2 * 1024 * 1024,
    )
    .unwrap();
    let scratch = scratch_upper_bound(&c, &["Name".into()], &page.rows).unwrap();
    assert!(scratch > 1_800_000);
    let batch = decode_rows(&c, &["Name".into()], &page.rows).unwrap();
    assert!(ipc(&batch, 1024).is_err());
    assert!(ipc(&batch, 512 * 1024).is_ok());
}
#[test]
fn canonical_api_field_case_matches_authored_rest_and_bulk_selectors_unambiguously() {
    use grv_adapter_salesforce::{
        bulk::{EmptyPolicy, decode_page},
        http::HttpResponse,
    };
    use std::collections::BTreeMap;
    let contract = grv_adapter_api::TableContract {
        columns: vec![
            grv_adapter_api::Column {
                name: "first".into(),
                logical_type: serde_json::json!("string"),
            },
            grv_adapter_api::Column {
                name: "again".into(),
                logical_type: serde_json::json!("string"),
            },
        ],
        partition_keys: vec![],
        extensions: serde_json::json!({}),
        column_ext: serde_json::json!({}),
    };
    let selectors = ["id".into(), "ID".into()];
    let rest = [serde_json::json!({"Id":"value"})];
    assert_eq!(
        grv_adapter_salesforce::row::decode_rows(&contract, &selectors, &rest)
            .unwrap()
            .num_columns(),
        2
    );
    assert_eq!(
        grv_adapter_salesforce::row::field(
            &serde_json::json!({"Account":{"Name":"value"}}),
            "account.NAME"
        )
        .unwrap(),
        "value"
    );
    assert!(
        grv_adapter_salesforce::row::field(&serde_json::json!({"Id":"first","id":"second"}), "id")
            .is_err()
    );
    let response = |body: &[u8]| HttpResponse {
        status: 200,
        headers: BTreeMap::from([
            ("sforce-numberofrecords".into(), "1".into()),
            ("sforce-locator".into(), "null".into()),
        ]),
        body: body.to_vec(),
    };
    let page = decode_page(
        response(b"Id\nvalue\n"),
        &contract,
        &selectors,
        &[EmptyPolicy::Reject, EmptyPolicy::Reject],
    )
    .unwrap();
    assert_eq!(
        grv_adapter_salesforce::row::decode_rows(&contract, &selectors, &page.rows)
            .unwrap()
            .num_columns(),
        2
    );
    assert!(
        decode_page(
            response(b"Id,id\nfirst,second\n"),
            &contract,
            &selectors,
            &[EmptyPolicy::Reject, EmptyPolicy::Reject]
        )
        .is_err()
    );
}
