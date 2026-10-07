//! Durable nonresumable acquisition evidence. Recording an intent precedes
//! opening the snapshot; an existing (even partial) marker forbids reacquisition.
use grv_adapter_api::Checkpoint;
use grv_types::{AdapterIdentity, Digest, Timestamp, Uuid};
use serde::{Deserialize, Serialize};
use std::{
    ffi::CString,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    os::unix::{
        fs::{MetadataExt, OpenOptionsExt},
        io::{AsRawFd, FromRawFd},
    },
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcquisitionIntent {
    pub attempt_id: Uuid,
    pub request_sha256: Digest,
    pub adapter_identity: AdapterIdentity,
    pub connection_identity: String,
    pub snapshot_id: Uuid,
    pub capture_start: Timestamp,
    pub reopenable: bool,
}

/// Directory must be adapter-private and outside GRV; callers allocate it before
/// source acquisition. Files are created relative to a pinned protected dir fd.
pub struct AcquisitionJournal {
    directory: File,
    path: PathBuf,
    intent: AcquisitionIntent,
    intent_inode: Option<(u64, u64)>,
}
impl AcquisitionJournal {
    pub fn start(directory: &Path, mut intent: AcquisitionIntent) -> io::Result<Self> {
        // This seam cannot represent a resumable source and never upgrades one.
        intent.reopenable = false;
        let marker = directory.join("acquisition.json");
        crate::lock::check_ancestors(&marker)?;
        let opened = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_DIRECTORY)
            .open(directory)?;
        let metadata = opened.metadata()?;
        if metadata.uid() != unsafe { libc::geteuid() } || metadata.mode() & 0o077 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "acquisition directory must be owner-private",
            ));
        }
        let mut journal = Self {
            directory: opened,
            path: directory.into(),
            intent,
            intent_inode: None,
        };
        journal.recheck()?;
        // A failed/partial write leaves the exclusive marker in place. Retrying
        // never infers that a missing completion authorizes another source query.
        journal.intent_inode = Some(journal.write_once("acquisition.json", &journal.intent)?);
        journal.recheck()?;
        Ok(journal)
    }
    pub fn intent(&self) -> &AcquisitionIntent {
        &self.intent
    }
    pub fn recheck(&self) -> io::Result<()> {
        let opened = self.directory.metadata()?;
        let named = fs::symlink_metadata(&self.path)?;
        if !named.is_dir() || (opened.dev(), opened.ino()) != (named.dev(), named.ino()) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "acquisition directory replaced",
            ));
        }
        if let Some(inode) = self.intent_inode {
            let marker = fs::symlink_metadata(self.path.join("acquisition.json"))?;
            if !marker.is_file() || marker.nlink() != 1 || inode != (marker.dev(), marker.ino()) {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "acquisition intent replaced or removed",
                ));
            }
        }
        Ok(())
    }
    /// Persist every selected table's acquisition identity, including empty
    /// tables, before producing the checkpoint event for parent acknowledgement.
    pub fn checkpoint_ready(&self, checkpoint: &Checkpoint) -> io::Result<()> {
        self.recheck()?;
        if checkpoint.attempt_id != self.intent.attempt_id
            || checkpoint.adapter_identity != self.intent.adapter_identity
            || checkpoint.connection_identity != self.intent.connection_identity
            || checkpoint.tables.is_empty()
            || checkpoint.tables.iter().any(|table| {
                table.reopenable
                    || table.snapshot_id != self.intent.snapshot_id.as_str()
                    || table.capture_start != self.intent.capture_start
            })
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "checkpoint changes fixed nonresumable acquisition identity",
            ));
        }
        let mut names = std::collections::BTreeSet::new();
        if checkpoint
            .tables
            .iter()
            .any(|table| !names.insert(&table.table))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "duplicate checkpoint table",
            ));
        }
        self.write_once("checkpoint.json", checkpoint).map(|_| ())
    }
    fn write_once(&self, name: &str, value: &impl Serialize) -> io::Result<(u64, u64)> {
        let bytes = serde_json::to_vec(value).map_err(io::Error::other)?;
        if bytes.len() > 1024 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "acquisition metadata exceeds journal bound",
            ));
        }
        let name = CString::new(name).unwrap();
        let descriptor = unsafe {
            libc::openat(
                self.directory.as_raw_fd(),
                name.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                0o600,
            )
        };
        if descriptor < 0 {
            return Err(io::Error::last_os_error());
        }
        let mut file = unsafe { File::from_raw_fd(descriptor) };
        file.write_all(&bytes)?;
        file.sync_all()?;
        self.directory.sync_all()?;
        self.recheck()?;
        let metadata = file.metadata()?;
        Ok((metadata.dev(), metadata.ino()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grv_adapter_api::CheckpointTable;
    use grv_types::Name;
    use std::os::unix::fs::PermissionsExt;
    fn private_directory() -> tempfile::TempDir {
        let directory = tempfile::tempdir().unwrap();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
        directory
    }
    fn intent() -> AcquisitionIntent {
        AcquisitionIntent {
            attempt_id: Uuid::v4(),request_sha256:Digest::new("1".repeat(64)).unwrap(),
            adapter_identity:serde_json::from_value(serde_json::json!({"name":"duckdb","package_version":"0.1.0","interface_version":1,"binding_schema_version":1})).unwrap(),
            connection_identity:"local-canonical-identity".into(),snapshot_id:Uuid::v4(),capture_start:Timestamp::new("2026-10-06T00:00:00Z").unwrap(),reopenable:false,
        }
    }
    #[test]
    fn restart_never_reacquires_even_without_completed_checkpoint() {
        let directory = private_directory();
        let identity = intent();
        let journal = AcquisitionJournal::start(directory.path(), identity.clone()).unwrap();
        assert!(!journal.intent().reopenable);
        drop(journal);
        let error = AcquisitionJournal::start(directory.path(), identity)
            .err()
            .unwrap();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    }
    #[test]
    fn immutable_checkpoint_covers_empty_table_and_same_snapshot() {
        let directory = private_directory();
        let identity = intent();
        let journal = AcquisitionJournal::start(directory.path(), identity.clone()).unwrap();
        let mut checkpoint = Checkpoint {
            attempt_id: identity.attempt_id,
            adapter_identity: identity.adapter_identity,
            connection_identity: identity.connection_identity,
            tables: vec![CheckpointTable {
                table: Name::new("empty").unwrap(),
                snapshot_id: identity.snapshot_id.as_str().into(),
                reopenable: false,
                source_identity: serde_json::json!({"relation":"main.empty","database":"/source.duckdb"}),
                capture_start: identity.capture_start,
            }],
            job: serde_json::json!({}),
        };
        journal.checkpoint_ready(&checkpoint).unwrap();
        assert!(journal.checkpoint_ready(&checkpoint).is_err());
        checkpoint.tables[0].reopenable = true;
        assert!(journal.checkpoint_ready(&checkpoint).is_err());
    }
    #[test]
    fn partial_marker_and_symlinked_directory_never_authorize_source() {
        let directory = private_directory();
        fs::write(directory.path().join("acquisition.json"), b"{").unwrap();
        assert_eq!(
            AcquisitionJournal::start(directory.path(), intent())
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::AlreadyExists
        );
        let alias = directory.path().join("alias");
        std::os::unix::fs::symlink(directory.path(), &alias).unwrap();
        assert!(AcquisitionJournal::start(&alias, intent()).is_err());
    }
    #[test]
    fn removed_intent_is_not_authority_to_enter_a_source_snapshot() {
        let directory = private_directory();
        let journal = AcquisitionJournal::start(directory.path(), intent()).unwrap();
        fs::remove_file(directory.path().join("acquisition.json")).unwrap();
        assert!(journal.recheck().is_err());
    }
}
