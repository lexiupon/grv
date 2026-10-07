//! Closed public core results, separate from persisted storage records.
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Parameters {
    pub max_clock_skew: u64,
    pub max_lease_ttl: u64,
    pub pending_grace: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitResult {
    pub created: bool,
    pub format: String,
    pub format_version: u32,
    pub parameters: Parameters,
}
