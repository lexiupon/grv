use grv_adapter_salesforce::source_json;
use serde_json::json;
const BUDGET: usize = 32 * 1024 * 1024;
#[test]
fn source_json_keeps_numeric_tokens_utf8_and_rejects_duplicate_or_reserved_keys() {
    let bytes =
        br#"{"n":123456789012345678901234567890.123456,"nested":[true,null,{"name":"\u00e5"}]}"#;
    let value = source_json::parse(bytes, bytes.len(), BUDGET).unwrap();
    assert_eq!(
        value["n"].to_string(),
        "123456789012345678901234567890.123456"
    );
    assert_eq!(value["nested"][2]["name"], json!("å"));
    for bytes in [
        br#"{"x":{"a":1,"a":2}}"#.as_slice(),
        br#"{"$serde_json::private::Number":"12"}"#,
        br#"{"\u0024serde_json::private::Number":"12"}"#,
        b"{}{}",
        b"1e999",
        b"\xff",
    ] {
        assert!(source_json::parse(bytes, bytes.len(), BUDGET).is_err());
    }
}
#[test]
fn encoded_and_decoded_source_data_are_charged_together_before_tree_adoption() {
    let bytes = serde_json::to_vec(&json!({"text":"x".repeat(700_000)})).unwrap();
    assert!(source_json::parse(&bytes, bytes.capacity(), 1024 * 1024).is_err());
    assert!(source_json::parse(&bytes, bytes.capacity(), 4 * 1024 * 1024).is_ok());
    let wide = format!(
        "[{}]",
        std::iter::repeat_n("{}", 10_000)
            .collect::<Vec<_>>()
            .join(",")
    );
    assert!(source_json::parse(wide.as_bytes(), wide.len(), 512 * 1024).is_err());
    assert!(source_json::parse(wide.as_bytes(), wide.len(), 2 * 1024 * 1024).is_ok());
    let deep = format!("{}0{}", "[".repeat(129), "]".repeat(129));
    assert!(source_json::parse(deep.as_bytes(), deep.len(), BUDGET).is_err());
}
