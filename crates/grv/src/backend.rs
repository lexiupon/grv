use grv_core::store::{Result, backend_error, public_error};
use grv_storage::{Backend, LocalBackend};
use grv_types::ErrorCode;
use std::path::{Path, PathBuf};

pub(super) struct Root {
    pub canonical: String,
    pub local: Option<PathBuf>,
}
impl Root {
    pub fn parse(value: &str) -> Result<Self> {
        if value.starts_with("s3://") || value.starts_with("gs://") {
            #[cfg(feature = "cloud")]
            {
                return Ok(Self {
                    canonical: grv_storage::cloud::CloudRoot::parse(value)
                        .map_err(backend_error)?
                        .canonical()
                        .into(),
                    local: None,
                });
            }
            #[cfg(not(feature = "cloud"))]
            {
                return Err(public_error(
                    ErrorCode::UnsupportedCapability,
                    "this executable was built without cloud support",
                ));
            }
        }
        let path = canonical_local(Path::new(value))?;
        let canonical = path
            .to_str()
            .ok_or_else(|| public_error(ErrorCode::InvalidArgument, "root must be UTF-8"))?
            .into();
        Ok(Self {
            canonical,
            local: Some(path),
        })
    }
    pub fn open(&self, create_local: bool) -> Result<Box<dyn Backend>> {
        if let Some(path) = &self.local {
            let backend = if create_local {
                LocalBackend::create(path)
            } else {
                LocalBackend::open(path)
            }
            .map_err(backend_error)?;
            return Ok(Box::new(backend));
        }
        #[cfg(feature = "cloud")]
        {
            Ok(Box::new(
                grv_storage::cloud::CloudBackend::open(&self.canonical, Default::default())
                    .map_err(backend_error)?,
            ))
        }
        #[cfg(not(feature = "cloud"))]
        {
            Err(public_error(
                ErrorCode::UnsupportedCapability,
                "this executable was built without cloud support",
            ))
        }
    }
    pub fn exclusions(&self) -> &[PathBuf] {
        self.local.as_slice()
    }
}
fn canonical_local(path: &Path) -> Result<PathBuf> {
    let mut path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|_| public_error(ErrorCode::InvalidArgument, "working directory unavailable"))?
            .join(path)
    };
    let mut suffix = Vec::new();
    loop {
        match std::fs::canonicalize(&path) {
            Ok(mut canonical) => {
                for name in suffix.into_iter().rev() {
                    canonical.push(name);
                }
                return Ok(canonical);
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                suffix.push(
                    path.file_name()
                        .ok_or_else(|| {
                            public_error(ErrorCode::InvalidArgument, "root cannot be resolved")
                        })?
                        .to_os_string(),
                );
                path.pop();
            }
            Err(_) => {
                return Err(public_error(
                    ErrorCode::BackendFailure,
                    "root cannot be resolved",
                ));
            }
        }
    }
}
