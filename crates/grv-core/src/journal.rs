//! Protected consumer evidence outside GRV. One persistent session lock covers
//! aliases and every journal update; record generations provide stale-writer
//! detection within the process. Evidence never appears in diagnostics.
use crate::store::{Result, backend_error, public_error};
use grv_storage::{
    Backend, ErrorKind, LocalBackend, ObjectKey, Validator, WriteEffect, model::Counter,
};
use grv_types::{ErrorCode, Uuid};
use serde::{Deserialize, Deserializer, Serialize, de::DeserializeOwned};
use std::{
    ffi::CString,
    fmt,
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    },
    path::{Component, Path, PathBuf},
};
const LIMIT: usize = 64 * 1024 * 1024;
const RECORD: &str = "journal.json";
const LOCK: &str = "session.lock";
fn integrity() -> grv_types::PublicError {
    public_error(
        ErrorCode::IntegrityFailure,
        "consumer journal is missing, corrupt or no longer protected",
    )
}
fn io_error(_: std::io::Error) -> grv_types::PublicError {
    public_error(ErrorCode::BackendFailure, "consumer state operation failed")
}
fn storage_error(error: grv_storage::Error) -> grv_types::PublicError {
    let mut public = backend_error(error);
    public.message = "consumer state operation failed".into();
    public
}
fn required_option<'de, D, T>(d: D) -> std::result::Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::deserialize(d)
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    bound(
        deserialize = "I: Deserialize<'de>, C: Deserialize<'de>, P: Deserialize<'de>, O: Deserialize<'de>"
    )
)]
pub struct Evidence<I, C, P, O> {
    pub intent: I,
    #[serde(deserialize_with = "required_option")]
    pub capture: Option<C>,
    pub progress: P,
    #[serde(deserialize_with = "required_option")]
    pub terminal: Option<O>,
}
impl<I, C, P, O> fmt::Debug for Evidence<I, C, P, O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Evidence([private])")
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Envelope<I, C, P, O> {
    pub format_version: u32,
    pub generation: Counter,
    pub mutation_id: Uuid,
    pub evidence: Evidence<I, C, P, O>,
}
impl<I, C, P, O> fmt::Debug for Envelope<I, C, P, O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Envelope")
            .field("generation", &self.generation)
            .field("evidence", &"[private]")
            .finish()
    }
}
/// Holds the canonical session.lock inode for the whole invocation. Opening a
/// journal never infers an attempt or a capture from directory listings.
pub struct Journal {
    backend: LocalBackend,
    directory: PathBuf,
    directory_fd: File,
    lock: File,
}
impl fmt::Debug for Journal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Journal([private])")
    }
}
impl Journal {
    /// Explicit creation only. Caller chooses the prescribed push/<attempt-id>
    /// directory and passes every local GRV root it uses as a forbidden tree.
    pub fn create(directory: impl AsRef<Path>, forbidden_roots: &[PathBuf]) -> Result<Self> {
        Self::acquire(directory.as_ref(), forbidden_roots, true)
    }
    pub fn open(directory: impl AsRef<Path>, forbidden_roots: &[PathBuf]) -> Result<Self> {
        Self::acquire(directory.as_ref(), forbidden_roots, false)
    }
    pub fn directory(&self) -> &Path {
        &self.directory
    }
    fn artifact_path(&self, key: &ObjectKey, create_parent: bool) -> Result<PathBuf> {
        self.check_trust()?;
        if key
            .as_str()
            .split('/')
            .any(|component| component == RECORD || component == LOCK)
        {
            return Err(public_error(
                ErrorCode::InvalidArgument,
                "artifact name is reserved for consumer coordination",
            ));
        }
        let path = self.directory.join(key.as_str());
        let parent = path.parent().unwrap();
        if create_parent {
            establish_directory(parent, true)?;
        } else {
            trust_path(parent, true)?;
        }
        Ok(path)
    }
    pub fn create_artifact(&self, key: &ObjectKey, source: &mut dyn Read) -> Result<Validator> {
        let path = self.artifact_path(key, true)?;
        match std::fs::symlink_metadata(&path) {
            Ok(metadata) => private_file(&metadata)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error(error)),
        }
        self.backend
            .conditional_create(key, source)
            .map_err(storage_error)
    }
    pub fn get_artifact(
        &self,
        key: &ObjectKey,
        sink: &mut dyn Write,
    ) -> Result<grv_storage::ObjectMeta> {
        let path = self.artifact_path(key, false)?;
        private_file(&std::fs::symlink_metadata(path).map_err(|_| integrity())?)?;
        self.backend.get(key, sink).map_err(storage_error)
    }
    pub fn head_artifact(&self, key: &ObjectKey) -> Result<grv_storage::ObjectMeta> {
        let path = self.artifact_path(key, false)?;
        private_file(&std::fs::symlink_metadata(path).map_err(|_| integrity())?)?;
        self.backend.head(key).map_err(storage_error)
    }
    fn acquire(directory: &Path, forbidden_roots: &[PathBuf], create: bool) -> Result<Self> {
        let directory = absolute(directory)?;
        for root in forbidden_roots {
            let root = if root.exists() {
                std::fs::canonicalize(root).map_err(io_error)?
            } else {
                absolute(root)?
            };
            if trees_overlap(&directory, &root)? {
                return Err(public_error(
                    ErrorCode::InvalidArgument,
                    "consumer state and GRV storage must occupy separate trees",
                ));
            }
        }
        establish_directory(&directory, create)?;
        trust_path(&directory, true)?;
        let directory_fd = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&directory)
            .map_err(io_error)?;
        let lock_name = CString::new(LOCK).unwrap();
        let flags = libc::O_RDWR
            | libc::O_NOFOLLOW
            | libc::O_CLOEXEC
            | libc::O_NONBLOCK
            | if create { libc::O_CREAT } else { 0 };
        let fd =
            unsafe { libc::openat(directory_fd.as_raw_fd(), lock_name.as_ptr(), flags, 0o600) };
        if fd < 0 {
            return Err(integrity());
        }
        let lock = unsafe { File::from_raw_fd(fd) };
        private_file(&lock.metadata().map_err(io_error)?)?;
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EWOULDBLOCK)
                || error.raw_os_error() == Some(libc::EAGAIN)
            {
                let mut result = public_error(ErrorCode::EngineBusy, "consumer session is busy");
                result.retryable = true;
                return Err(result);
            }
            return Err(io_error(error));
        }
        lock.sync_all().map_err(io_error)?;
        sync_path(&directory)?;
        let backend = LocalBackend::open(&directory).map_err(storage_error)?;
        let journal = Self {
            backend,
            directory,
            directory_fd,
            lock,
        };
        journal.check_trust()?;
        Ok(journal)
    }
    fn check_trust(&self) -> Result<()> {
        trust_path(&self.directory, true)?;
        let actual = std::fs::symlink_metadata(&self.directory).map_err(|_| integrity())?;
        let opened = self.directory_fd.metadata().map_err(io_error)?;
        if actual.dev() != opened.dev() || actual.ino() != opened.ino() {
            return Err(integrity());
        }
        let actual =
            std::fs::symlink_metadata(self.directory.join(LOCK)).map_err(|_| integrity())?;
        private_file(&actual)?;
        let opened = self.lock.metadata().map_err(io_error)?;
        if actual.dev() != opened.dev() || actual.ino() != opened.ino() {
            return Err(integrity());
        }
        Ok(())
    }
    fn check_record(&self) -> Result<()> {
        self.check_trust()?;
        private_file(
            &std::fs::symlink_metadata(self.directory.join(RECORD)).map_err(|_| integrity())?,
        )
    }
    fn read_record<
        I: DeserializeOwned,
        C: DeserializeOwned,
        P: DeserializeOwned,
        O: DeserializeOwned,
    >(
        &self,
    ) -> Result<(Envelope<I, C, P, O>, Validator)> {
        self.check_record()?;
        let (bytes, meta) = self
            .backend
            .read_bytes(&ObjectKey::new(RECORD).unwrap(), LIMIT)
            .map_err(storage_error)?;
        let value = grv_adapter_wire::json::parse(&bytes).map_err(|_| integrity())?;
        let record: Envelope<I, C, P, O> =
            serde_json::from_value(value).map_err(|_| integrity())?;
        if record.format_version != 1 || record.generation.get() == 0 {
            return Err(integrity());
        }
        Ok((record, meta.validator))
    }
    pub fn read<
        I: DeserializeOwned,
        C: DeserializeOwned,
        P: DeserializeOwned,
        O: DeserializeOwned,
    >(
        &self,
    ) -> Result<Envelope<I, C, P, O>> {
        self.read_record().map(|v| v.0)
    }
    pub fn create_evidence<
        I: Serialize + DeserializeOwned,
        C: Serialize + DeserializeOwned,
        P: Serialize + DeserializeOwned,
        O: Serialize + DeserializeOwned,
    >(
        &self,
        evidence: Evidence<I, C, P, O>,
    ) -> Result<Envelope<I, C, P, O>> {
        self.check_trust()?;
        let record = Envelope {
            format_version: 1,
            generation: Counter::from(1),
            mutation_id: Uuid::v4(),
            evidence,
        };
        let bytes = serialize(&record)?;
        match self
            .backend
            .create_bytes(&ObjectKey::new(RECORD).unwrap(), &bytes)
        {
            Ok(_) => Ok(record),
            Err(error)
                if error.kind == ErrorKind::PreconditionFailed
                    || error.effect == WriteEffect::MaybeApplied =>
            {
                let found: Envelope<I, C, P, O> = self.read()?;
                if equal(&found.evidence, &record.evidence)? {
                    Ok(found)
                } else {
                    Err(public_error(
                        ErrorCode::RequestMismatch,
                        "consumer attempt already has a different fixed request or state",
                    ))
                }
            }
            Err(error) => Err(storage_error(error)),
        }
    }
    pub fn compare_and_swap<
        I: Serialize + DeserializeOwned,
        C: Serialize + DeserializeOwned,
        P: Serialize + DeserializeOwned,
        O: Serialize + DeserializeOwned,
    >(
        &self,
        expected_generation: Counter,
        next: Evidence<I, C, P, O>,
    ) -> Result<Envelope<I, C, P, O>> {
        let (current, validator): (Envelope<I, C, P, O>, _) = self.read_record()?;
        if current.generation != expected_generation {
            return Err(public_error(
                ErrorCode::StateConflict,
                "consumer journal generation changed",
            ));
        }
        if !equal(&current.evidence.intent, &next.intent)?
            || (current.evidence.capture.is_some()
                && !equal(&current.evidence.capture, &next.capture)?)
            || (current.evidence.terminal.is_some()
                && !equal(&current.evidence.terminal, &next.terminal)?)
        {
            return Err(public_error(
                ErrorCode::RequestMismatch,
                "fixed intent, accepted capture and terminal outcome are immutable",
            ));
        }
        let generation = current.generation.next().map_err(|_| {
            public_error(
                ErrorCode::StateConflict,
                "consumer journal generation is exhausted",
            )
        })?;
        let record = Envelope {
            format_version: 1,
            generation,
            mutation_id: Uuid::v4(),
            evidence: next,
        };
        let bytes = serialize(&record)?;
        match self
            .backend
            .put_bytes(&ObjectKey::new(RECORD).unwrap(), &validator, &bytes)
        {
            Ok(_) => Ok(record),
            Err(error) if error.effect == WriteEffect::MaybeApplied => {
                let found: Envelope<I, C, P, O> = self.read()?;
                if found.mutation_id == record.mutation_id {
                    Ok(found)
                } else {
                    Err(public_error(
                        ErrorCode::OutcomeUnknown,
                        "consumer journal update could not be proved",
                    ))
                }
            }
            Err(error) => Err(storage_error(error)),
        }
    }
}
fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    // Serde's ordinary JSON serializer maps nonfinite floats to null. The
    // canonical serializer's finite-number guard prevents that data loss;
    // persisted bytes still use ordinary JSON to retain exact int64 counters.
    grv_types::canonical_json(value).map_err(|_| integrity())?;
    let bytes = serde_json::to_vec(value).map_err(|_| integrity())?;
    if bytes.len() > LIMIT {
        return Err(public_error(
            ErrorCode::InvalidArgument,
            "consumer evidence exceeds journal budget",
        ));
    }
    Ok(bytes)
}
fn equal<A: Serialize, B: Serialize>(a: &A, b: &B) -> Result<bool> {
    Ok(serde_json::to_value(a).map_err(|_| integrity())?
        == serde_json::to_value(b).map_err(|_| integrity())?)
}
fn absolute(path: &Path) -> Result<PathBuf> {
    let source = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().map_err(io_error)?.join(path)
    };
    let mut result = PathBuf::new();
    for component in source.components() {
        match component {
            Component::RootDir => result.push(component.as_os_str()),
            Component::Normal(v) => result.push(v),
            Component::CurDir => {}
            _ => {
                return Err(public_error(
                    ErrorCode::InvalidArgument,
                    "consumer path contains a parent or unsupported component",
                ));
            }
        }
    }
    Ok(result)
}
fn trees_overlap(directory: &Path, root: &Path) -> Result<bool> {
    if directory.starts_with(root) || root.starts_with(directory) {
        return Ok(true);
    }
    // Case aliases on macOS and bind mounts can name the same directory with
    // different path strings. Compare ancestor inode identities as well.
    for (target, ancestors) in [(root, directory), (directory, root)] {
        let metadata = match std::fs::metadata(target) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(io_error(e)),
        };
        for ancestor in ancestors.ancestors() {
            match std::fs::metadata(ancestor) {
                Ok(m) if m.dev() == metadata.dev() && m.ino() == metadata.ino() => return Ok(true),
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(io_error(e)),
            }
        }
    }
    Ok(false)
}
fn trust_path(path: &Path, private: bool) -> Result<()> {
    let uid = unsafe { libc::geteuid() };
    for (index, ancestor) in path.ancestors().enumerate() {
        let metadata = std::fs::symlink_metadata(ancestor).map_err(|_| integrity())?;
        if !metadata.is_dir()
            || (metadata.uid() != 0 && metadata.uid() != uid)
            || metadata.mode() & 0o022 != 0
            || (index == 0 && private && (metadata.uid() != uid || metadata.mode() & 0o077 != 0))
        {
            return Err(integrity());
        }
    }
    Ok(())
}
fn private_file(metadata: &std::fs::Metadata) -> Result<()> {
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o077 != 0
    {
        Err(integrity())
    } else {
        Ok(())
    }
}
fn establish_directory(path: &Path, create: bool) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => trust_path(path, true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && create => {
            let parent = path.parent().ok_or_else(integrity)?;
            match std::fs::symlink_metadata(parent) {
                Ok(_) => trust_path(parent, false)?,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    establish_directory(parent, true)?
                }
                Err(e) => return Err(io_error(e)),
            }
            match std::fs::DirBuilder::new().mode(0o700).create(path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(e) => return Err(io_error(e)),
            }
            trust_path(path, true)?;
            sync_path(path)
        }
        Err(_) => Err(integrity()),
    }
}
fn sync_path(path: &Path) -> Result<()> {
    for ancestor in path.ancestors() {
        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(ancestor)
            .and_then(|file| file.sync_all())
            .map_err(io_error)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        os::unix::fs::{PermissionsExt, symlink},
        sync::{Arc, Barrier},
    };
    type TestEvidence = Evidence<String, String, u64, String>;
    type TestEnvelope = Envelope<String, String, u64, String>;
    fn protected() -> tempfile::TempDir {
        let directory = tempfile::Builder::new()
            .prefix(".grv-journal-test-")
            .tempdir_in(std::env::current_dir().unwrap())
            .unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        directory
    }
    fn initial() -> TestEvidence {
        Evidence {
            intent: "fixed-request".into(),
            capture: None,
            progress: 0,
            terminal: None,
        }
    }
    #[test]
    fn journal_generation_cas_serializes_threads_and_session_aliases_share_lock() {
        let root = protected();
        let journal = Arc::new(Journal::create(root.path(), &[]).unwrap());
        let first = journal.create_evidence(initial()).unwrap();
        let alias = root.path().join(".");
        assert_eq!(
            Journal::open(alias, &[]).unwrap_err().code,
            ErrorCode::EngineBusy
        );
        let start = Arc::new(Barrier::new(8));
        let threads: Vec<_> = (0..8)
            .map(|n| {
                let journal = journal.clone();
                let start = start.clone();
                std::thread::spawn(move || {
                    let mut next = initial();
                    next.progress = n;
                    start.wait();
                    journal.compare_and_swap(first.generation, next)
                })
            })
            .collect();
        let results: Vec<_> = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        assert_eq!(results.iter().filter(|v| v.is_ok()).count(), 1);
        assert!(
            results
                .iter()
                .filter_map(|v| v.as_ref().err())
                .all(|e| e.code == ErrorCode::StateConflict)
        );
        assert_eq!(
            journal
                .read::<String, String, u64, String>()
                .unwrap()
                .generation
                .get(),
            2
        );
        drop(journal);
        Journal::open(root.path(), &[]).unwrap();
    }
    #[test]
    fn fixed_intent_accepted_capture_and_terminal_result_are_immutable_and_private() {
        let root = protected();
        let journal = Journal::create(root.path(), &[]).unwrap();
        let first = journal.create_evidence(initial()).unwrap();
        let mut next = first.evidence.clone();
        next.capture = Some("accepted-capture-canary".into());
        next.terminal = Some("terminal-result-canary".into());
        let accepted = journal.compare_and_swap(first.generation, next).unwrap();
        assert!(!format!("{accepted:?}").contains("canary"));
        assert!(!format!("{:?}", accepted.evidence).contains("canary"));
        for field in ["intent", "capture", "terminal"] {
            let mut changed = accepted.evidence.clone();
            match field {
                "intent" => changed.intent = "different".into(),
                "capture" => changed.capture = None,
                _ => changed.terminal = Some("different".into()),
            };
            assert_eq!(
                journal
                    .compare_and_swap(accepted.generation, changed)
                    .unwrap_err()
                    .code,
                ErrorCode::RequestMismatch
            );
        }
        let mut progress = accepted.evidence.clone();
        progress.progress = 9;
        let updated = journal
            .compare_and_swap(accepted.generation, progress)
            .unwrap();
        assert_eq!(updated.evidence.capture, accepted.evidence.capture);
        assert_eq!(updated.evidence.terminal, accepted.evidence.terminal);
    }
    #[test]
    fn duplicate_unknown_missing_null_and_nonfinite_evidence_is_refused_without_logging_contents() {
        let root = protected();
        let journal = Journal::create(root.path(), &[]).unwrap();
        let first = journal.create_evidence(initial()).unwrap();
        let original = std::fs::read(root.path().join(RECORD)).unwrap();
        let malformed = String::from_utf8(original.clone()).unwrap().replacen(
            "\"generation\":1",
            "\"generation\":1,\"generation\":2",
            1,
        );
        std::fs::write(root.path().join(RECORD), malformed).unwrap();
        let error = journal.read::<String, String, u64, String>().unwrap_err();
        assert_eq!(error.code, ErrorCode::IntegrityFailure);
        assert!(!error.message.contains("fixed-request"));
        let mut value: serde_json::Value = serde_json::from_slice(&original).unwrap();
        value["evidence"].as_object_mut().unwrap().remove("capture");
        std::fs::write(
            root.path().join(RECORD),
            serde_json::to_vec(&value).unwrap(),
        )
        .unwrap();
        assert_eq!(
            journal
                .read::<String, String, u64, String>()
                .unwrap_err()
                .code,
            ErrorCode::IntegrityFailure
        );
        value = serde_json::from_slice(&original).unwrap();
        value["password-canary"] = "credential-canary".into();
        std::fs::write(
            root.path().join(RECORD),
            serde_json::to_vec(&value).unwrap(),
        )
        .unwrap();
        let error = journal.read::<String, String, u64, String>().unwrap_err();
        assert!(!error.message.contains("canary"));
        std::fs::write(root.path().join(RECORD), original).unwrap();
        assert_eq!(
            journal
                .read::<String, String, u64, String>()
                .unwrap()
                .generation,
            first.generation
        );
        assert!(
            serialize(&Evidence::<String, String, f64, String> {
                intent: "fixed".into(),
                capture: None,
                progress: f64::NAN,
                terminal: None
            })
            .is_err()
        );
    }
    #[test]
    fn state_overlap_unsafe_ancestors_symlinks_and_replaced_lock_inode_are_refused() {
        let root = protected();
        let grv = root.path().join("grv");
        std::fs::create_dir(&grv).unwrap();
        assert_eq!(
            Journal::create(grv.join("state"), std::slice::from_ref(&grv))
                .unwrap_err()
                .code,
            ErrorCode::InvalidArgument
        );
        assert!(!grv.join("state").exists());
        assert_eq!(
            Journal::create(root.path(), std::slice::from_ref(&grv))
                .unwrap_err()
                .code,
            ErrorCode::InvalidArgument
        );
        let unsafe_parent = root.path().join("unsafe");
        std::fs::create_dir(&unsafe_parent).unwrap();
        std::fs::set_permissions(&unsafe_parent, std::fs::Permissions::from_mode(0o777)).unwrap();
        assert_eq!(
            Journal::create(unsafe_parent.join("state"), &[])
                .unwrap_err()
                .code,
            ErrorCode::IntegrityFailure
        );
        assert!(!unsafe_parent.join("state").exists());
        let target = root.path().join("target");
        std::fs::create_dir(&target).unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o700)).unwrap();
        symlink(&target, root.path().join("alias")).unwrap();
        assert_eq!(
            Journal::create(root.path().join("alias/state"), &[])
                .unwrap_err()
                .code,
            ErrorCode::IntegrityFailure
        );
        let state = root.path().join("state");
        let journal = Journal::create(&state, &[]).unwrap();
        journal.create_evidence(initial()).unwrap();
        std::fs::rename(state.join(LOCK), state.join("old.lock")).unwrap();
        std::fs::write(state.join(LOCK), b"").unwrap();
        std::fs::set_permissions(state.join(LOCK), std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(
            journal
                .read::<String, String, u64, String>()
                .unwrap_err()
                .code,
            ErrorCode::IntegrityFailure
        );
    }
    #[test]
    fn private_immutable_artifacts_are_streamed_durable_and_cannot_replace_coordination() {
        let root = protected();
        let journal = Journal::create(root.path(), &[]).unwrap();
        let path = ObjectKey::new("capture/table/data.parquet").unwrap();
        let data = vec![5; 200_000];
        let validator = journal
            .create_artifact(&path, &mut data.as_slice())
            .unwrap();
        let mut output = vec![];
        let metadata = journal.get_artifact(&path, &mut output).unwrap();
        assert_eq!(output, data);
        assert_eq!(metadata.validator, validator);
        assert_eq!(journal.head_artifact(&path).unwrap(), metadata);
        assert_eq!(
            std::fs::metadata(root.path().join(path.as_str()))
                .unwrap()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(root.path().join("capture/table"))
                .unwrap()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            journal
                .create_artifact(&path, &mut b"replacement".as_slice())
                .unwrap_err()
                .code,
            ErrorCode::StateConflict
        );
        assert_eq!(
            journal
                .create_artifact(&ObjectKey::new(LOCK).unwrap(), &mut b"x".as_slice())
                .unwrap_err()
                .code,
            ErrorCode::InvalidArgument
        );
        std::fs::set_permissions(
            root.path().join(path.as_str()),
            std::fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        assert_eq!(
            journal.head_artifact(&path).unwrap_err().code,
            ErrorCode::IntegrityFailure
        );
        struct SecretError;
        impl Read for SecretError {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("credential-canary-from-reader"))
            }
        }
        let error = journal
            .create_artifact(
                &ObjectKey::new("capture/failed.parquet").unwrap(),
                &mut SecretError,
            )
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::BackendFailure);
        assert!(!error.message.contains("canary"));
    }
    struct CrashInstall;
    impl grv_storage::FaultInjector for CrashInstall {
        fn check(&self, point: grv_storage::FaultPoint) -> std::io::Result<()> {
            if point == grv_storage::FaultPoint::AfterInstall {
                std::process::exit(77);
            }
            Ok(())
        }
    }
    #[test]
    #[ignore = "subprocess helper automatically invoked by journal process-crash test"]
    fn crash_worker() {
        let Some(root) = std::env::var_os("GRV_TEST_JOURNAL_CRASH_ROOT") else {
            return;
        };
        let mut journal = Journal::open(root, &[]).unwrap();
        journal.backend = journal.backend.with_faults(Arc::new(CrashInstall));
        if std::env::var_os("GRV_TEST_JOURNAL_CREATE").is_some() {
            journal.create_evidence(initial()).unwrap();
        } else {
            let current: TestEnvelope = journal.read().unwrap();
            let mut next = current.evidence;
            next.capture = Some("accepted".into());
            journal.compare_and_swap(current.generation, next).unwrap();
        }
        panic!("crash boundary did not fire");
    }
    #[test]
    fn actual_process_crash_after_journal_install_retains_complete_evidence_and_releases_lock() {
        for create in [true, false] {
            let root = protected();
            let journal = Journal::create(root.path(), &[]).unwrap();
            if !create {
                journal.create_evidence(initial()).unwrap();
            }
            drop(journal);
            let mut process = std::process::Command::new(std::env::current_exe().unwrap());
            process
                .args(["--exact", "journal::tests::crash_worker", "--ignored"])
                .env("GRV_TEST_JOURNAL_CRASH_ROOT", root.path())
                .env_remove("GRV_TEST_JOURNAL_CREATE");
            if create {
                process.env("GRV_TEST_JOURNAL_CREATE", "1");
            }
            let output = process.output().unwrap();
            assert_eq!(output.status.code(), Some(77));
            let journal = Journal::open(root.path(), &[]).unwrap();
            let recorded: TestEnvelope = journal.read().unwrap();
            assert_eq!(recorded.evidence.intent, "fixed-request");
            assert_eq!(recorded.generation.get(), if create { 1 } else { 2 });
            if !create {
                assert_eq!(recorded.evidence.capture, Some("accepted".into()));
            }
        }
    }
}
