//! Offline local alias metadata only. Auth files and sf subprocesses are never
//! touched; unknown usernames retain a null identity until authentication.
use crate::{
    Error, Result,
    auth::{OfflineMetadata, OfflineMetadataStore},
};
use std::{fs::OpenOptions, io::Read, os::unix::fs::OpenOptionsExt, path::PathBuf};

pub struct FileMetadataStore {
    pub alias_files: Vec<PathBuf>,
}
impl FileMetadataStore {
    pub fn for_home(home: PathBuf) -> Self {
        Self {
            alias_files: vec![home.join(".sf/alias.json"), home.join(".sfdx/alias.json")],
        }
    }
}
impl OfflineMetadataStore for FileMetadataStore {
    fn lookup(&mut self, alias: &str) -> Result<Option<OfflineMetadata>> {
        for path in &self.alias_files {
            let file = match OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
                .open(path)
            {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(_) => {
                    return Err(Error::new(
                        "ADAPTER_FAILURE",
                        "Salesforce alias metadata cannot be read",
                    ));
                }
            };
            if !file
                .metadata()
                .is_ok_and(|m| m.is_file() && m.len() <= 1024 * 1024)
            {
                return Err(Error::new(
                    "INTEGRITY_FAILURE",
                    "Salesforce alias metadata exceeds bounds",
                ));
            }
            let mut bytes = Vec::new();
            file.take(1024 * 1024 + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| {
                    Error::new(
                        "ADAPTER_FAILURE",
                        "Salesforce alias metadata cannot be read",
                    )
                })?;
            let value = grv_adapter_wire::json::parse(&bytes).map_err(|_| {
                Error::new("INTEGRITY_FAILURE", "Salesforce alias metadata is invalid")
            })?;
            let orgs = value
                .get("orgs")
                .and_then(serde_json::Value::as_object)
                .ok_or_else(|| {
                    Error::new(
                        "INTEGRITY_FAILURE",
                        "Salesforce alias metadata lacks org aliases",
                    )
                })?;
            if let Some(username) = orgs.get(alias) {
                let username = username
                    .as_str()
                    .filter(|s| !s.is_empty() && !s.contains('\0'))
                    .ok_or_else(|| {
                        Error::new(
                            "INTEGRITY_FAILURE",
                            "Salesforce alias metadata username is invalid",
                        )
                    })?;
                return Ok(Some(OfflineMetadata {
                    username: username.into(),
                    org_id: None,
                }));
            }
        }
        Ok(None)
    }
}
