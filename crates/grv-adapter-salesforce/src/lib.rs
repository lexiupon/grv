//! Salesforce extraction with exact conversions, bounded supervised helpers,
//! durable nonresumable acquisition, and SDK lifecycle integration.
pub mod acquisition;
pub mod auth;
pub mod bulk;
pub mod config;
pub mod containment;
pub mod conversion;
pub mod extraction;
pub mod http;
pub mod journal;
mod keychain;
pub mod metadata;
pub mod offline;
pub mod predicate;
pub mod process;
pub mod row;
pub mod runtime;
pub mod source_json;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    pub code: &'static str,
    pub message: String,
}

impl Error {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn invalid(message: impl Into<String>) -> Error {
    Error::new("INVALID_DECLARATION", message)
}

pub(crate) fn integrity(message: impl Into<String>) -> Error {
    Error::new("INTEGRITY_FAILURE", message)
}
