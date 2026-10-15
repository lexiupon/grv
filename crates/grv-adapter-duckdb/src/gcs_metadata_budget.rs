//! Adapter caller-copy accounting uses the shared internal GCS JSON reserve.
//! Logical content/structure admission, not engine heap/RSS protection.
use std::io;
#[cfg(test)]
const LIMIT: usize = grv_adapter_wire::metadata::GCS_LOGICAL_BYTES;
#[cfg(test)]
const MAX_DEPTH: usize = grv_adapter_wire::metadata::GCS_DEPTH;
pub(crate) struct Admission(grv_adapter_wire::metadata::Admission);
impl Admission {
    pub(crate) fn new() -> Self {
        Self(grv_adapter_wire::metadata::Admission::default())
    }
    pub(crate) fn include(&mut self, value: &impl serde::Serialize) -> io::Result<()> {
        self.0
            .include(value)
            .map_err(|_| io::Error::other("GCS caller metadata exceeds logical reserve"))
    }
    #[cfg(any(test, feature = "native"))]
    pub(crate) fn include_copies(
        &mut self,
        value: &impl serde::Serialize,
        copies: usize,
    ) -> io::Result<()> {
        for _ in 0..copies {
            self.include(value)?;
        }
        Ok(())
    }
}
pub(crate) fn admit(value: &impl serde::Serialize) -> io::Result<()> {
    Admission::new().include(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn borrowed_metadata_admission_counts_containers_copies_and_escaped_strings() {
        let normal = serde_json::json!({"files":[{"location":"gs://bucket/data/%25.parquet","schema":{"columns":[{"name":"id","logical_type":"int64"}]}}]});
        admit(&normal).unwrap();
        admit(&serde_json::json!({"quoted":"\\\"{[unicode🍕"})).unwrap();
        assert!(admit(&vec![""; 40000]).is_err());
        assert!(admit(&"x".repeat(LIMIT + 1)).is_err());
        let bytes = "x".repeat(LIMIT / 2);
        let mut budget = Admission::new();
        budget.include(&bytes).unwrap();
        assert!(budget.include(&bytes).is_err());
    }
    #[test]
    fn fifth_discovery_copy_is_charged_not_hidden_by_four_copy_admission() {
        let discovery = serde_json::json!({"outputs":[{"contract":{"extensions":{"synthetic":"x".repeat(1700000)}}}]});
        let mut four = Admission::new();
        four.include_copies(&discovery, 4).unwrap();
        assert!(four.include(&discovery).is_err());
        let mut original_plus_four = Admission::new();
        original_plus_four.include(&discovery).unwrap();
        assert!(original_plus_four.include_copies(&discovery, 4).is_err());
    }
    #[test]
    fn borrowed_metadata_nesting_refuses_without_cloning_value() {
        let mut value = serde_json::Value::Null;
        for _ in 0..MAX_DEPTH + 1 {
            value = serde_json::json!([value]);
        }
        assert!(admit(&value).is_err());
    }
}
