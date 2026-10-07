//! Validated, storage-independent GRV identities and public contracts.
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use std::{fmt, str::FromStr};
pub mod logical;
pub mod results;
pub use logical::{Column, TableContract, authoring_type, validate_logical_type};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError(pub String);
impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for ValidationError {}

macro_rules! string_scalar {
    ($name:ident, $check:expr) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);
        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, ValidationError> {
                let value = value.into();
                if ($check)(&value) {
                    Ok(Self(value))
                } else {
                    Err(ValidationError(format!("invalid {}", stringify!($name))))
                }
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
        impl FromStr for $name {
            type Err = ValidationError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::new(s)
            }
        }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                Self::new(String::deserialize(d)?).map_err(serde::de::Error::custom)
            }
        }
    };
}
string_scalar!(Name, |s: &str| !s.is_empty()
    && s.len() <= 64
    && s.bytes()
        .next()
        .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
    && s.bytes().all(|b| b.is_ascii_lowercase()
        || b.is_ascii_digit()
        || b == b'_'
        || b == b'-'));
string_scalar!(Digest, |s: &str| s.len() == 64
    && s.bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)));
string_scalar!(Handle, |s: &str| !s.is_empty());
string_scalar!(RunId, |s: &str| s.len() == 26
    && s.as_bytes()[0] <= b'7'
    && s.bytes()
        .all(|b| b"0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(&b)));
string_scalar!(Uuid, |s: &str| {
    uuid::Uuid::parse_str(s).is_ok_and(|id| {
        id.hyphenated().to_string() == s
            && matches!(id.get_version_num(), 4 | 7)
            && id.get_variant() == uuid::Variant::RFC4122
    })
});
string_scalar!(Timestamp, |s: &str| {
    let b = s.as_bytes();
    b.len() >= 20
        && b[4] == b'-'
        && b[7] == b'-'
        && b[10] == b'T'
        && b[13] == b':'
        && b[16] == b':'
        && b[b.len() - 1] == b'Z'
        && [0usize, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18]
            .iter()
            .all(|i| b[*i].is_ascii_digit())
        && &s[17..19] <= "59"
        && (b.len() == 20
            || (b[19] == b'.' && b.len() > 21 && b[20..b.len() - 1].iter().all(u8::is_ascii_digit)))
        && chrono::DateTime::parse_from_rfc3339(s).is_ok()
});

impl Uuid {
    pub fn v4() -> Self {
        Self(uuid::Uuid::new_v4().hyphenated().to_string())
    }
}

pub const MAX_SAFE_INTEGER: u64 = (1u64 << 53) - 1;
macro_rules! integer_scalar {
    ($name:ident, $min:expr) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(u64);
        impl $name {
            pub fn new(v: u64) -> Result<Self, ValidationError> {
                if ($min..=MAX_SAFE_INTEGER).contains(&v) {
                    Ok(Self(v))
                } else {
                    Err(ValidationError(format!("invalid {}", stringify!($name))))
                }
            }
            pub fn get(self) -> u64 {
                self.0
            }
        }
        impl TryFrom<u64> for $name {
            type Error = ValidationError;
            fn try_from(v: u64) -> Result<Self, Self::Error> {
                Self::new(v)
            }
        }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                Self::new(u64::deserialize(d)?).map_err(serde::de::Error::custom)
            }
        }
    };
}
integer_scalar!(SafeInt, 0);
integer_scalar!(Req, 1);
/// Canonical decimal-string integer, deliberately limited to signed int64.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct U64(u64);
impl U64 {
    pub fn new(v: u64) -> Result<Self, ValidationError> {
        if v <= i64::MAX as u64 {
            Ok(Self(v))
        } else {
            Err(ValidationError("U64 exceeds int64".into()))
        }
    }
    pub fn get(self) -> u64 {
        self.0
    }
}
impl fmt::Display for U64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl FromStr for U64 {
    type Err = ValidationError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty()
            || (s.len() > 1 && s.starts_with('0'))
            || !s.bytes().all(|b| b.is_ascii_digit())
        {
            return Err(ValidationError("noncanonical U64".into()));
        }
        Self::new(
            s.parse()
                .map_err(|_| ValidationError("invalid U64".into()))?,
        )
    }
}
impl Serialize for U64 {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string())
    }
}
impl<'de> Deserialize<'de> for U64 {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        String::deserialize(d)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}
pub type Revision = U64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterIdentity {
    pub name: Name,
    pub package_version: String,
    pub interface_version: Req,
    pub binding_schema_version: Req,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceConsistency {
    #[serde(rename = "transaction-snapshot")]
    TransactionSnapshot,
    #[serde(rename = "capture-window")]
    CaptureWindow,
    #[serde(rename = "adapter-defined")]
    AdapterDefined,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    InvalidArgument,
    InvalidDeclaration,
    RequestMismatch,
    BuildIncomplete,
    EngineBusy,
    StateConflict,
    OwnershipLost,
    NotFound,
    Unavailable,
    IntegrityFailure,
    ProtocolFailure,
    BackendFailure,
    EngineFailure,
    OutcomeUnknown,
    UnsupportedCapability,
    ExtractionIncomplete,
    AdapterFailure,
}
impl ErrorCode {
    pub fn exit_status(self) -> u8 {
        match self {
            Self::InvalidArgument
            | Self::InvalidDeclaration
            | Self::RequestMismatch
            | Self::BuildIncomplete
            | Self::UnsupportedCapability => 2,
            Self::EngineBusy | Self::StateConflict | Self::OwnershipLost => 3,
            Self::NotFound | Self::Unavailable => 4,
            Self::IntegrityFailure | Self::ProtocolFailure => 5,
            _ => 6,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ObjectIdentity {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataset: Option<Name>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<Name>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition: Option<std::collections::BTreeMap<Name, Name>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision: Option<Revision>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<U64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<RunId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<RunId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pin_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicError {
    pub code: ErrorCode,
    pub message: String,
    pub object: Option<Box<ObjectIdentity>>,
    pub retryable: bool,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandOutput<T = Value> {
    pub output_version: u8,
    pub command: String,
    pub root: Option<String>,
    pub ok: bool,
    pub exit_status: u8,
    pub result: Option<T>,
    pub errors: Vec<PublicError>,
}
impl<T> CommandOutput<T> {
    pub fn success(command: impl Into<String>, result: T) -> Self {
        Self {
            output_version: 1,
            command: command.into(),
            root: None,
            ok: true,
            exit_status: 0,
            result: Some(result),
            errors: vec![],
        }
    }
    pub fn failure(command: impl Into<String>, error: PublicError) -> Self {
        Self {
            output_version: 1,
            command: command.into(),
            root: None,
            ok: false,
            exit_status: error.code.exit_status(),
            result: None,
            errors: vec![error],
        }
    }
}
mod canonical;
/// RFC 8785 canonical bytes: UTF-16 property order and ECMAScript numbers.
/// JSON integers must be exactly representable in binary64. Larger counters
/// and exact source decimals belong in strings, outside JCS numeric semantics.
pub fn canonical_json<T: Serialize>(value: &T) -> Result<Vec<u8>, serde_json::Error> {
    canonical::to_vec(value)
}
pub fn sha256(bytes: &[u8]) -> Digest {
    Digest(format!("{:x}", Sha256::digest(bytes)))
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeclarationIdentity {
    pub effective_declaration: Value,
    pub adapter_identity: AdapterIdentity,
    pub connection_identity: String,
    pub canonical_connection: Value,
}
pub fn declaration_digest(input: &DeclarationIdentity) -> Result<Digest, serde_json::Error> {
    canonical_json(input).map(|bytes| sha256(&bytes))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RequestedRevision {
    Latest(LatestRevision),
    Revision(Revision),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LatestRevision {
    #[serde(rename = "latest")]
    Latest,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PullRequestIdentity {
    pub root: String,
    pub workspace_id: Uuid,
    pub declaration_sha256: Digest,
    pub requested_revision: RequestedRevision,
}
pub fn pull_request_digest(input: &PullRequestIdentity) -> Result<Digest, serde_json::Error> {
    canonical_json(input).map(|bytes| sha256(&bytes))
}
impl AdapterIdentity {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.package_version.is_empty() {
            Err(ValidationError("empty package version".into()))
        } else {
            Ok(())
        }
    }
}
impl ObjectIdentity {
    pub fn validate(&self) -> Result<(), ValidationError> {
        let v = serde_json::to_value(self).expect("object identity serializes");
        if v.as_object().is_some_and(|o| o.is_empty())
            || self.path.as_ref().is_some_and(|p| p.is_empty())
            || self.version.is_some_and(|v| v.get() == 0)
            || self.partition.as_ref().is_some_and(|p| {
                p.keys()
                    .any(|k| matches!(k.as_str(), "version" | "revision"))
            })
        {
            Err(ValidationError("invalid object identity".into()))
        } else {
            Ok(())
        }
    }
}
impl PublicError {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.message.is_empty() {
            return Err(ValidationError("empty public error message".into()));
        }
        if let Some(o) = &self.object {
            o.validate()?;
        }
        Ok(())
    }
}
impl<T> CommandOutput<T> {
    pub fn validate(&self) -> Result<(), ValidationError> {
        const COMMANDS: &[&str] = &[
            "init",
            "ls",
            "show",
            "status",
            "log",
            "diff",
            "verify",
            "pull",
            "session prepare",
            "session show",
            "session renew",
            "session abort",
            "push",
            "pin",
            "unpin",
            "gc",
            "recover",
            "unknown",
            "adapter list",
            "adapter install",
            "adapter capabilities",
            "adapter command",
        ];
        if self.output_version != 1
            || !COMMANDS.contains(&self.command.as_str())
            || self.ok != (self.exit_status == 0)
            || (self.ok
                && (self.result.is_none() || !self.errors.is_empty() || self.command == "unknown"))
            || (!self.ok
                && (self.errors.is_empty()
                    || self.exit_status != self.errors[0].code.exit_status()))
            || (self.command.starts_with("adapter ") && self.root.is_some())
            || (self.ok
                && !self.command.starts_with("adapter ")
                && self.root.as_ref().is_none_or(|r| r.is_empty()))
            || (self.command == "unknown" && self.result.is_some())
        {
            return Err(ValidationError("invalid command output envelope".into()));
        }
        for error in &self.errors {
            error.validate()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scalars_reject_noncanonical_forms() {
        for s in ["", "A", "_x", "a/b"] {
            assert!(Name::new(s).is_err());
        }
        for s in ["01", "+1", "-1", "9223372036854775808", "1.0"] {
            assert!(s.parse::<U64>().is_err());
        }
        assert!(serde_json::from_str::<U64>("1").is_err());
        assert!(Req::new(0).is_err());
        assert!(SafeInt::new(1 << 53).is_err());
        assert!(Uuid::new("359C6D0F-A9C1-4AE6-B804-0742A5E2B9DE").is_err());
        assert!(Timestamp::new("2026-02-30T00:00:00Z").is_err());
    }
    #[test]
    fn identity_golden_vectors() {
        let vectors: Value =
            serde_json::from_str(include_str!("../../../spec/fixtures/identity-v1.json")).unwrap();
        for vector in vectors.as_array().unwrap() {
            let canonical = canonical_json(&vector["input"]).unwrap();
            assert_eq!(
                std::str::from_utf8(&canonical).unwrap(),
                vector["canonical"].as_str().unwrap()
            );
            assert_eq!(
                sha256(&canonical).as_str(),
                vector["sha256"].as_str().unwrap()
            );
        }
    }
}
