//! Initialization and checked opening of a v2 backend.
use grv_storage::{
    Backend, ErrorKind, ListMode, ObjectKey, ObjectPrefix, WriteEffect,
    model::{Counter, StoreParameters, decode_record, encode_record},
};
use grv_types::{
    ErrorCode, PublicError,
    results::{InitResult, Parameters},
};

const METADATA_LIMIT: usize = 64 * 1024 * 1024;
pub type Result<T> = std::result::Result<T, PublicError>;
#[derive(Debug, Clone, Default)]
pub struct InitOptions {
    pub max_clock_skew: Option<u64>,
    pub max_lease_ttl: Option<u64>,
    pub pending_grace: Option<u64>,
}
pub struct Store<B: Backend> {
    pub backend: B,
    pub parameters: StoreParameters,
}
pub fn public_error(code: ErrorCode, message: impl Into<String>) -> PublicError {
    PublicError {
        code,
        message: message.into(),
        retryable: false,
        object: None,
    }
}
pub fn backend_error(error: grv_storage::Error) -> PublicError {
    let code = if error.effect == WriteEffect::MaybeApplied {
        ErrorCode::OutcomeUnknown
    } else {
        match error.kind {
            ErrorKind::NotFound => ErrorCode::NotFound,
            ErrorKind::PreconditionFailed => ErrorCode::StateConflict,
            ErrorKind::InvalidRecord | ErrorKind::Integrity => ErrorCode::IntegrityFailure,
            ErrorKind::InvalidKey => ErrorCode::InvalidArgument,
            ErrorKind::Unsupported => ErrorCode::UnsupportedCapability,
            ErrorKind::Io => ErrorCode::BackendFailure,
        }
    };
    let mut output = public_error(code, error.to_string());
    output.retryable = matches!(code, ErrorCode::BackendFailure | ErrorCode::OutcomeUnknown);
    output
}
fn parameters_result(parameters: &StoreParameters, created: bool) -> InitResult {
    InitResult {
        created,
        format: parameters.format.clone(),
        format_version: parameters.format_version,
        parameters: Parameters {
            max_clock_skew: parameters.max_clock_skew_seconds.get(),
            max_lease_ttl: parameters.max_lease_ttl_seconds.get(),
            pending_grace: parameters.pending_grace_seconds.get(),
        },
    }
}
fn matches_explicit(options: &InitOptions, parameters: &StoreParameters) -> Result<()> {
    for (offered, stored) in [
        (options.max_clock_skew, parameters.max_clock_skew_seconds),
        (options.max_lease_ttl, parameters.max_lease_ttl_seconds),
        (options.pending_grace, parameters.pending_grace_seconds),
    ] {
        if offered.is_some_and(|offered| offered != stored.get()) {
            return Err(public_error(
                ErrorCode::StateConflict,
                "explicit initialization parameters differ from existing root",
            ));
        }
    }
    Ok(())
}
impl<B: Backend> Store<B> {
    pub fn open(backend: B) -> Result<Self> {
        let key = ObjectKey::new("grv.json").unwrap();
        let (bytes, _) = match backend.read_bytes(&key, METADATA_LIMIT) {
            Ok(value) => value,
            Err(error) if error.kind == ErrorKind::NotFound => {
                if !backend
                    .list(
                        &ObjectPrefix::new("datasets/").unwrap(),
                        ListMode::Recursive,
                    )
                    .map_err(backend_error)?
                    .is_empty()
                {
                    return Err(public_error(
                        ErrorCode::IntegrityFailure,
                        "missing grv.json with existing dataset objects",
                    ));
                }
                return Err(public_error(
                    ErrorCode::NotFound,
                    "GRV root is uninitialized",
                ));
            }
            Err(error) => return Err(backend_error(error)),
        };
        let parameters = decode_record(&bytes).map_err(backend_error)?;
        Ok(Self {
            backend,
            parameters,
        })
    }
    pub fn initialize(backend: B, options: InitOptions) -> Result<(Self, InitResult)> {
        let key = ObjectKey::new("grv.json").unwrap();
        match backend.read_bytes(&key, METADATA_LIMIT) {
            Ok((bytes, _)) => {
                let parameters: StoreParameters = decode_record(&bytes).map_err(backend_error)?;
                matches_explicit(&options, &parameters)?;
                let result = parameters_result(&parameters, false);
                return Ok((
                    Self {
                        backend,
                        parameters,
                    },
                    result,
                ));
            }
            Err(error) if error.kind == ErrorKind::NotFound => {}
            Err(error) => return Err(backend_error(error)),
        }
        if !backend
            .list(
                &ObjectPrefix::new("datasets/").unwrap(),
                ListMode::Recursive,
            )
            .map_err(backend_error)?
            .is_empty()
        {
            return Err(public_error(
                ErrorCode::IntegrityFailure,
                "missing grv.json with existing dataset objects",
            ));
        }
        let mut parameters = StoreParameters::default();
        let count = |value| {
            Counter::new(value).map_err(|_| {
                public_error(ErrorCode::InvalidArgument, "root parameter exceeds int64")
            })
        };
        if let Some(value) = options.max_clock_skew {
            parameters.max_clock_skew_seconds = count(value)?;
        }
        if let Some(value) = options.max_lease_ttl {
            parameters.max_lease_ttl_seconds = count(value)?;
        }
        if let Some(value) = options.pending_grace {
            parameters.pending_grace_seconds = count(value)?;
        }
        if parameters.max_lease_ttl_seconds <= parameters.max_clock_skew_seconds
            || parameters.pending_grace_seconds.get() == 0
        {
            return Err(public_error(
                ErrorCode::InvalidArgument,
                "lease TTL must exceed clock skew and pending grace must be positive",
            ));
        }
        let bytes = encode_record(&parameters).map_err(backend_error)?;
        let created = match backend.create_bytes(&key, &bytes) {
            Ok(_) => true,
            Err(error)
                if error.kind == ErrorKind::PreconditionFailed
                    || error.effect == WriteEffect::MaybeApplied =>
            {
                // Durable reread, not a listing, is the authority to adopt an
                // initializer's acknowledged or ambiguous conditional create.
                let (found, _) = backend
                    .read_bytes(&key, METADATA_LIMIT)
                    .map_err(backend_error)?;
                let recorded: StoreParameters = decode_record(&found).map_err(backend_error)?;
                matches_explicit(&options, &recorded)?;
                let ours = recorded == parameters;
                parameters = recorded;
                ours
            }
            Err(error) => return Err(backend_error(error)),
        };
        let result = parameters_result(&parameters, created);
        Ok((
            Self {
                backend,
                parameters,
            },
            result,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grv_storage::{FaultInjector, FaultPoint, LocalBackend};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    struct LostAck(AtomicBool);
    impl FaultInjector for LostAck {
        fn check(&self, point: FaultPoint) -> std::io::Result<()> {
            if point == FaultPoint::AfterInstall && !self.0.swap(true, Ordering::SeqCst) {
                Err(std::io::Error::other("lost creation acknowledgement"))
            } else {
                Ok(())
            }
        }
    }
    #[test]
    fn init_is_idempotent_and_explicit_parameters_must_match() {
        let temp = tempfile::tempdir().unwrap();
        let (store, first) = Store::initialize(
            LocalBackend::open(temp.path()).unwrap(),
            InitOptions::default(),
        )
        .unwrap();
        assert!(first.created);
        let (_, second) = Store::initialize(
            LocalBackend::open(temp.path()).unwrap(),
            InitOptions::default(),
        )
        .unwrap();
        assert!(!second.created);
        assert_eq!(first.parameters, second.parameters);
        assert!(
            Store::initialize(
                store.backend,
                InitOptions {
                    max_clock_skew: Some(31),
                    ..Default::default()
                }
            )
            .is_err()
        );
    }
    #[test]
    fn ambiguous_init_is_adopted_only_after_durable_readback() {
        let temp = tempfile::tempdir().unwrap();
        let (_, result) = Store::initialize(
            LocalBackend::open(temp.path())
                .unwrap()
                .with_faults(Arc::new(LostAck(AtomicBool::new(false)))),
            InitOptions::default(),
        )
        .unwrap();
        assert!(result.created);
        Store::open(LocalBackend::open(temp.path()).unwrap()).unwrap();
    }
    #[test]
    fn damaged_root_and_invalid_new_parameters_are_refused() {
        let temp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::open(temp.path()).unwrap();
        backend
            .create_bytes(&ObjectKey::new("datasets/data/orphan").unwrap(), b"orphan")
            .unwrap();
        assert_eq!(
            Store::initialize(backend, InitOptions::default())
                .err()
                .unwrap()
                .code,
            ErrorCode::IntegrityFailure
        );
        let temp = tempfile::tempdir().unwrap();
        assert_eq!(
            Store::initialize(
                LocalBackend::open(temp.path()).unwrap(),
                InitOptions {
                    max_lease_ttl: Some(30),
                    ..Default::default()
                }
            )
            .err()
            .unwrap()
            .code,
            ErrorCode::InvalidArgument
        );
    }
    #[test]
    fn readonly_open_never_initializes_or_creates_missing_paths() {
        let temp = tempfile::tempdir().unwrap();
        let missing = temp.path().join("missing");
        assert!(LocalBackend::open(&missing).is_err());
        assert!(!missing.exists());
        assert_eq!(
            Store::open(LocalBackend::open(temp.path()).unwrap())
                .err()
                .unwrap()
                .code,
            ErrorCode::NotFound
        );
        assert!(!temp.path().join("grv.json").exists());
    }
}
