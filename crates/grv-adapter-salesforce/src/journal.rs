//! Private acquisition evidence is outside GRV. A persistent exclusive lock and
//! fsync-before-rename records ensure retries cannot authorize a second query.
use crate::{
    Error, Result,
    config::{Transport, api_version_valid, identifier},
    integrity,
};
use serde::{Deserialize, Serialize};
#[cfg(unix)]
use std::os::unix::{
    fs::{MetadataExt, OpenOptionsExt},
    io::AsRawFd,
};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};
use uuid::Uuid;

const MAX_RECORD_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcquisitionIdentity {
    pub attempt_id: Uuid,
    pub table: String,
    pub object: String,
    pub request_sha256: String,
    pub connection_identity: String,
    pub query_sha256: String,
    pub api_version: String,
    pub transport: Transport,
    pub all_rows: bool,
}

impl AcquisitionIdentity {
    fn validate(&self) -> Result<()> {
        let digest = |s: &str| {
            s.len() == 64
                && s.bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        };
        if !matches!(self.attempt_id.get_version_num(), 4 | 7)
            || self.attempt_id.get_variant() != uuid::Variant::RFC4122
            || grv_types::Name::new(&self.table).is_err()
            || !identifier(&self.object)
            || !digest(&self.request_sha256)
            || !digest(&self.query_sha256)
            || self.connection_identity.is_empty()
            || !api_version_valid(&self.api_version)
            || self.transport == Transport::Auto
        {
            return Err(integrity("invalid fixed acquisition identity"));
        }
        let org_id = self
            .connection_identity
            .strip_prefix("salesforce:")
            .ok_or_else(|| integrity("fixed connection is not a Salesforce org identity"))?;
        crate::auth::OrgId::parse(org_id)
            .map_err(|_| integrity("fixed org identity is invalid"))?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum AcquisitionState {
    Preparing,
    Intent,
    Active {
        job_ids: Vec<String>,
        expected_rows: Option<u64>,
    },
    Complete {
        job_ids: Vec<String>,
        rows: u64,
        capture_end: grv_types::Timestamp,
    },
    OutcomeUnknown,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcquisitionRecord {
    pub identity: AcquisitionIdentity,
    pub snapshot_id: Uuid,
    pub reopenable: bool,
    pub checkpoint_acked: bool,
    pub capture_start: grv_types::Timestamp,
    pub state: AcquisitionState,
    #[serde(default)]
    pub page_locators: Vec<String>,
}

impl AcquisitionRecord {
    fn validate(&self) -> Result<()> {
        self.identity.validate()?;
        if self.reopenable
            || self.snapshot_id.get_version_num() != 4
            || self.snapshot_id.get_variant() != uuid::Variant::RFC4122
        {
            return Err(integrity("invalid nonresumable acquisition evidence"));
        }
        if self.page_locators.iter().enumerate().any(|(i, locator)| {
            locator.is_empty() || locator.len() > 4096 || self.page_locators[..i].contains(locator)
        }) {
            return Err(integrity("invalid Bulk page locator evidence"));
        }
        if matches!(self.state, AcquisitionState::Complete { .. }) && !self.checkpoint_acked {
            return Err(integrity(
                "completed acquisition lacks checkpoint acknowledgement",
            ));
        }
        if self.checkpoint_acked
            && !matches!(
                self.state,
                AcquisitionState::Active { .. } | AcquisitionState::Complete { .. }
            )
        {
            return Err(integrity(
                "inactive acquisition has acknowledgement evidence",
            ));
        }
        if let AcquisitionState::Complete {
            rows, capture_end, ..
        } = &self.state
        {
            if *rows > i64::MAX as u64 {
                return Err(integrity("completed row count exceeds counter"));
            }
            let start = chrono::DateTime::parse_from_rfc3339(self.capture_start.as_str())
                .expect("validated timestamp");
            let end = chrono::DateTime::parse_from_rfc3339(capture_end.as_str())
                .expect("validated timestamp");
            if end < start {
                return Err(integrity("source capture window is reversed"));
            }
        }
        if let AcquisitionState::Active { job_ids, .. }
        | AcquisitionState::Complete { job_ids, .. } = &self.state
        {
            if self.identity.transport == Transport::Bulk
                && (job_ids.len() != 1 || !valid_bulk_job(&job_ids[0]))
            {
                return Err(integrity("Bulk acquisition job evidence is invalid"));
            }
            if job_ids
                .iter()
                .enumerate()
                .any(|(i, id)| id.is_empty() || job_ids[..i].contains(id))
            {
                return Err(integrity("invalid source job IDs"));
            }
        }
        Ok(())
    }
    pub fn job_ids(&self) -> &[String] {
        match &self.state {
            AcquisitionState::Active { job_ids, .. }
            | AcquisitionState::Complete { job_ids, .. } => job_ids,
            _ => &[],
        }
    }

    pub fn source_job(&self) -> serde_json::Value {
        serde_json::json!({"transport":self.identity.transport,"api_version":self.identity.api_version,"job_ids":self.job_ids()})
    }

    pub fn source_identity(&self) -> Result<serde_json::Value> {
        let org_id = self
            .identity
            .connection_identity
            .strip_prefix("salesforce:")
            .ok_or_else(|| integrity("acquisition identity is not a Salesforce org"))?;
        crate::auth::OrgId::parse(org_id)?;
        Ok(
            serde_json::json!({"org_id":org_id,"object":self.identity.object,"query_sha256":self.identity.query_sha256}),
        )
    }

    pub fn capture_window(&self) -> Option<grv_adapter_api::CaptureWindow> {
        if let AcquisitionState::Complete { capture_end, .. } = &self.state {
            Some(grv_adapter_api::CaptureWindow {
                start: self.capture_start.clone(),
                end: capture_end.clone(),
            })
        } else {
            None
        }
    }
}

fn now() -> grv_types::Timestamp {
    grv_types::Timestamp::new(
        chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
    )
    .expect("current UTC timestamp")
}

pub struct Journal {
    root: PathBuf,
    _lock: File,
}

fn state_io(_: std::io::Error) -> Error {
    // Never include raw HTTP data, credentials, or potentially secret paths.
    Error::new("STATE_CONFLICT", "acquisition evidence I/O failed")
}

#[cfg(unix)]
fn check_protected(path: &Path, directory: bool) -> Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(state_io)?;
    let uid = unsafe { libc::geteuid() };
    if metadata.file_type().is_symlink()
        || (directory && !metadata.is_dir())
        || (!directory && !metadata.is_file())
        || metadata.mode() & 0o022 != 0
        || ![0, uid].contains(&metadata.uid())
    {
        return Err(Error::new(
            "STATE_CONFLICT",
            "acquisition evidence path is not protected",
        ));
    }
    Ok(())
}

impl Journal {
    /// The caller creates a protected private root. All ancestors must be
    /// trusted and non-writable by group/others, including test paths.
    #[cfg(unix)]
    pub fn open(root: &Path) -> Result<Self> {
        if !root.is_absolute()
            || root
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            return Err(Error::new(
                "STATE_CONFLICT",
                "journal requires an absolute protected path",
            ));
        }
        for ancestor in root.ancestors() {
            check_protected(ancestor, true)?;
        }
        let root = fs::canonicalize(root).map_err(state_io)?;
        let lock_path = root.join("session.lock");
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&lock_path)
            .map_err(state_io)?;
        check_protected(&lock_path, false)?;
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(Error::new(
                "STATE_CONFLICT",
                "acquisition journal is already locked",
            ));
        }
        lock.sync_all().map_err(state_io)?;
        File::open(&root)
            .and_then(|f| f.sync_all())
            .map_err(state_io)?;
        Ok(Self { root, _lock: lock })
    }

    #[cfg(not(unix))]
    pub fn open(_: &Path) -> Result<Self> {
        Err(Error::new(
            "UNSUPPORTED_CAPABILITY",
            "journal locking requires Linux or macOS",
        ))
    }

    fn path(&self, identity: &AcquisitionIdentity) -> Result<PathBuf> {
        identity.validate()?;
        Ok(self
            .root
            .join(format!("{}--{}.json", identity.attempt_id, identity.table)))
    }

    pub fn load(&self, identity: &AcquisitionIdentity) -> Result<Option<AcquisitionRecord>> {
        let path = self.path(identity)?;
        match fs::symlink_metadata(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(state_io(error)),
            Ok(_) => (),
        }
        #[cfg(unix)]
        check_protected(&path, false)?;
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        options.custom_flags(libc::O_NOFOLLOW);
        let file = options.open(path).map_err(state_io)?;
        if file.metadata().map_err(state_io)?.len() > MAX_RECORD_BYTES {
            return Err(integrity("oversized acquisition evidence"));
        }
        let mut bytes = Vec::new();
        file.take(MAX_RECORD_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(state_io)?;
        if bytes.len() as u64 > MAX_RECORD_BYTES {
            return Err(integrity("oversized acquisition evidence"));
        }
        let value = grv_adapter_wire::json::parse(&bytes)
            .map_err(|_| integrity("invalid acquisition evidence"))?;
        let record: AcquisitionRecord =
            serde_json::from_value(value).map_err(|_| integrity("invalid acquisition evidence"))?;
        record.validate()?;
        if record.identity != *identity {
            return Err(Error::new(
                "REQUEST_MISMATCH",
                "fixed acquisition identity differs",
            ));
        }
        Ok(Some(record))
    }

    fn store(&self, record: &AcquisitionRecord) -> Result<()> {
        record.validate()?;
        let destination = self.path(&record.identity)?;
        let scratch = self.root.join(format!(".write-{}", Uuid::new_v4()));
        let bytes =
            serde_json::to_vec(record).map_err(|_| integrity("invalid acquisition evidence"))?;
        if bytes.len() as u64 > MAX_RECORD_BYTES {
            return Err(integrity("oversized acquisition evidence"));
        }
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        let mut file = options.open(&scratch).map_err(state_io)?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(state_io)?;
        fs::rename(&scratch, destination).map_err(state_io)?;
        File::open(&self.root)
            .and_then(|f| f.sync_all())
            .map_err(state_io)
    }

    pub fn begin(&self, identity: &AcquisitionIdentity) -> Result<AcquisitionRecord> {
        self.begin_state(identity, AcquisitionState::Intent)
    }
    pub fn prepare(&self, identity: &AcquisitionIdentity) -> Result<AcquisitionRecord> {
        self.begin_state(identity, AcquisitionState::Preparing)
    }
    pub fn creation_intent(&self, identity: &AcquisitionIdentity) -> Result<()> {
        let mut record = self.required(identity)?;
        if record.state != AcquisitionState::Preparing {
            return Err(integrity("creation intent requires fresh preparation"));
        }
        record.state = AcquisitionState::Intent;
        self.store(&record)
    }
    fn begin_state(
        &self,
        identity: &AcquisitionIdentity,
        initial: AcquisitionState,
    ) -> Result<AcquisitionRecord> {
        if let Some(record) = self.load(identity)? {
            return Err(match record.state {
                AcquisitionState::Intent | AcquisitionState::OutcomeUnknown
                    if identity.transport == Transport::Bulk =>
                {
                    Error::new(
                        "OUTCOME_UNKNOWN",
                        "Bulk creation was recorded; do not repeat its POST",
                    )
                }
                _ => Error::new(
                    "EXTRACTION_INCOMPLETE",
                    "nonresumable acquisition already exists; start a new attempt",
                ),
            });
        }
        let record = AcquisitionRecord {
            identity: identity.clone(),
            snapshot_id: Uuid::new_v4(),
            reopenable: false,
            checkpoint_acked: false,
            capture_start: now(),
            state: initial,
            page_locators: vec![],
        };
        self.store(&record)?;
        Ok(record)
    }

    pub fn created(
        &self,
        identity: &AcquisitionIdentity,
        job_ids: Vec<String>,
        expected_rows: Option<u64>,
    ) -> Result<AcquisitionRecord> {
        let mut record = self.required(identity)?;
        if record.state != AcquisitionState::Intent {
            return Err(integrity("source creation transition is not from intent"));
        }
        record.state = AcquisitionState::Active {
            job_ids,
            expected_rows,
        };
        self.store(&record)?;
        Ok(record)
    }

    pub fn creation_failed(&self, identity: &AcquisitionIdentity, ambiguous: bool) -> Result<()> {
        let mut record = self.required(identity)?;
        if record.state != AcquisitionState::Intent {
            return Err(integrity("creation failure transition is not from intent"));
        }
        record.state = if ambiguous {
            AcquisitionState::OutcomeUnknown
        } else {
            AcquisitionState::Rejected
        };
        self.store(&record)
    }

    pub fn acknowledge(&self, identity: &AcquisitionIdentity, snapshot_id: Uuid) -> Result<()> {
        let mut record = self.required(identity)?;
        if record.snapshot_id != snapshot_id
            || !matches!(record.state, AcquisitionState::Active { .. })
        {
            return Err(Error::new(
                "PROTOCOL_FAILURE",
                "checkpoint acknowledgement does not cover active acquisition",
            ));
        }
        record.checkpoint_acked = true;
        self.store(&record)
    }

    /// Record new pagination locator facts durably before the corresponding GET.
    pub fn record_locator(&self, identity: &AcquisitionIdentity, locator: &str) -> Result<()> {
        let mut record = self.required(identity)?;
        let AcquisitionState::Active { job_ids, .. } = &mut record.state else {
            return Err(integrity("acquisition is not active"));
        };
        if !job_ids.iter().any(|id| id == locator) {
            job_ids.push(locator.into());
        }
        self.store(&record)
    }

    pub fn expected_rows(&self, identity: &AcquisitionIdentity, rows: u64) -> Result<()> {
        let mut record = self.required(identity)?;
        let AcquisitionState::Active { expected_rows, .. } = &mut record.state else {
            return Err(integrity("acquisition is not active"));
        };
        if rows > i64::MAX as u64 || expected_rows.is_some_and(|expected| expected != rows) {
            return Err(integrity("source result count changed or exceeds counter"));
        }
        *expected_rows = Some(rows);
        self.store(&record)
    }

    /// Bulk pagination cursors are durable acquisition facts, not job IDs.
    pub fn bulk_locator(&self, identity: &AcquisitionIdentity, locator: &str) -> Result<()> {
        let mut record = self.required(identity)?;
        if !record.checkpoint_acked
            || !matches!(record.state, AcquisitionState::Active { .. })
            || locator.is_empty()
            || locator.len() > 4096
            || record.page_locators.iter().any(|v| v == locator)
        {
            return Err(integrity("Bulk pagination locator repeated or invalid"));
        }
        record.page_locators.push(locator.into());
        self.store(&record)
    }

    pub fn finish(&self, identity: &AcquisitionIdentity, rows: u64) -> Result<AcquisitionRecord> {
        let mut record = self.required(identity)?;
        if !record.checkpoint_acked {
            return Err(Error::new(
                "PROTOCOL_FAILURE",
                "table completion requires checkpoint acknowledgement",
            ));
        }
        let AcquisitionState::Active {
            job_ids,
            expected_rows,
        } = &record.state
        else {
            return Err(integrity("acquisition is not active"));
        };
        if expected_rows.is_some_and(|expected| expected != rows) {
            return Err(integrity("source count disagrees with captured rows"));
        }
        record.state = AcquisitionState::Complete {
            job_ids: job_ids.clone(),
            rows,
            capture_end: now(),
        };
        self.store(&record)?;
        Ok(record)
    }

    pub fn required(&self, identity: &AcquisitionIdentity) -> Result<AcquisitionRecord> {
        self.load(identity)?
            .ok_or_else(|| integrity("missing required acquisition evidence"))
    }
}

pub(crate) fn valid_bulk_job(id: &str) -> bool {
    matches!(id.len(), 15 | 18) && id.bytes().all(|c| c.is_ascii_alphanumeric())
}
