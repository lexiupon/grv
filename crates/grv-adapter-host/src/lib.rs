//! Trusted discovery and process supervision for process adapters.
pub mod discovery;
pub mod install;
pub mod process;
pub mod protected_document;
pub mod registry;
pub mod session_lock;
use grv_types::{ErrorCode, PublicError};
#[derive(Debug)]
pub struct Error {
    pub code: ErrorCode,
    pub message: String,
    pub object: Option<Box<grv_types::ObjectIdentity>>,
    pub retryable: bool,
}
impl Error {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            object: None,
            retryable: false,
        }
    }
    pub fn public(self) -> PublicError {
        PublicError {
            code: self.code,
            message: self.message,
            object: self.object,
            retryable: self.retryable,
        }
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}
impl std::error::Error for Error {}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::new(ErrorCode::AdapterFailure, e.to_string())
    }
}
impl From<grv_adapter_wire::ProtocolError> for Error {
    fn from(e: grv_adapter_wire::ProtocolError) -> Self {
        let code = match e {
            grv_adapter_wire::ProtocolError::DocumentTooLarge
            | grv_adapter_wire::ProtocolError::Transport(_) => ErrorCode::AdapterFailure,
            grv_adapter_wire::ProtocolError::Invalid(_) => ErrorCode::ProtocolFailure,
        };
        Self::new(code, e.to_string())
    }
}
pub type Result<T> = std::result::Result<T, Error>;
pub fn validate_output(value: &serde_json::Value) -> Result<()> {
    static VALIDATOR: std::sync::OnceLock<jsonschema::Validator> = std::sync::OnceLock::new();
    let validator = VALIDATOR.get_or_init(|| {
        let schema: serde_json::Value = serde_json::from_str(include_str!(
            "../../../spec/grv-client-v1-command-output.schema.json"
        ))
        .expect("public schema JSON");
        jsonschema::options()
            .should_validate_formats(true)
            .build(&schema)
            .expect("public schema compiles")
    });
    if validator.is_valid(value) {
        Ok(())
    } else {
        Err(Error::new(
            ErrorCode::ProtocolFailure,
            "public output failed schema validation",
        ))
    }
}

#[cfg(test)]
mod transport_tests {
    use super::*;
    #[test]
    fn adapter_transport_failure_is_distinct_from_malformed_protocol() {
        let io = grv_adapter_wire::ProtocolError::from(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "connection reset",
        ));
        assert_eq!(Error::from(io).code, ErrorCode::AdapterFailure);
        assert_eq!(
            Error::from(grv_adapter_wire::ProtocolError::Transport(
                "channel EOF".into()
            ))
            .code,
            ErrorCode::AdapterFailure
        );
        assert_eq!(
            Error::from(grv_adapter_wire::ProtocolError::Invalid(
                "truncated frame".into()
            ))
            .code,
            ErrorCode::ProtocolFailure
        );
        assert_eq!(
            Error::from(grv_adapter_wire::ProtocolError::Invalid(
                "uncorrelated response".into()
            ))
            .code,
            ErrorCode::ProtocolFailure
        );
    }
}
