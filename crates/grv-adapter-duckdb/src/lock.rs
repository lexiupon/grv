//! Persistent nonblocking flock ownership, including canonical path and inode checks.

#[cfg(feature = "native")]
use std::os::unix::fs::FileExt;
use std::{
    fs::{self, File, OpenOptions},
    io,
    os::unix::{
        fs::{MetadataExt, OpenOptionsExt},
        io::AsRawFd,
    },
    path::{Path, PathBuf},
};

#[cfg(feature = "native")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ManagedEvidence {
    Absent,
    Managed,
    BuildHistory,
}

fn unsafe_path(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message)
}

/// Canonicalizes symlink aliases. Existing engines must have exactly one hard link.
/// A nonexistent engine uses its canonical existing parent, without creating the engine.
pub fn canonical_engine_path(path: &Path) -> io::Result<PathBuf> {
    if path.as_os_str().is_empty() || path == Path::new(":memory:") {
        return Err(unsafe_path("DuckDB requires a local native database file"));
    }
    match fs::metadata(path) {
        Ok(metadata) => {
            if !metadata.is_file() || metadata.nlink() != 1 {
                return Err(unsafe_path(
                    "database is not a regular file with exactly one hard link",
                ));
            }
            fs::canonicalize(path)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            // A dangling symlink must not be interpreted as an authorizing missing file.
            if fs::symlink_metadata(path).is_ok() {
                return Err(unsafe_path("dangling database symlink"));
            }
            let name = path
                .file_name()
                .ok_or_else(|| unsafe_path("database filename missing"))?;
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            Ok(fs::canonicalize(parent)?.join(name))
        }
        Err(error) => Err(error),
    }
}

pub(crate) fn check_ancestors(path: &Path) -> io::Result<()> {
    // Private temporary directories may be beneath a root-owned sticky directory.
    let uid = unsafe { libc::geteuid() };
    for ancestor in path.ancestors().skip(1) {
        let metadata = fs::metadata(ancestor)?;
        if !metadata.is_dir() || (metadata.uid() != uid && metadata.uid() != 0) {
            return Err(unsafe_path("untrusted lock ancestor owner"));
        }
        if metadata.mode() & 0o022 != 0 && !(metadata.uid() == 0 && metadata.mode() & 0o1000 != 0) {
            return Err(unsafe_path("writable lock ancestor"));
        }
    }
    Ok(())
}

/// Owns a persistent workspace helper. Drop closes the fd; it never unlinks the path.
/// Cloned descriptors share the same OS ownership and do not explicitly unlock it.
#[derive(Debug)]
pub struct WorkspaceLock {
    file: File,
    engine: PathBuf,
    helper: PathBuf,
    original_engine: Option<(u64, u64)>,
}

impl WorkspaceLock {
    pub fn acquire(path: &Path) -> io::Result<Self> {
        let engine = canonical_engine_path(path)?;
        let mut helper_os = engine.as_os_str().to_os_string();
        helper_os.push(".grv-lock");
        let helper = PathBuf::from(helper_os);
        check_ancestors(&helper)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&helper)?;
        let metadata = file.metadata()?;
        let uid = unsafe { libc::geteuid() };
        if !metadata.is_file()
            || metadata.nlink() != 1
            || metadata.uid() != uid
            || metadata.mode() & 0o077 != 0
        {
            return Err(unsafe_path(
                "lock helper is not a protected regular consumer file",
            ));
        }
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let original_engine = fs::metadata(&engine).ok().map(|m| (m.dev(), m.ino()));
        let ownership = Self {
            file,
            engine,
            helper,
            original_engine,
        };
        ownership.recheck()?;
        Ok(ownership)
    }

    pub fn engine_path(&self) -> &Path {
        &self.engine
    }
    pub fn helper_path(&self) -> &Path {
        &self.helper
    }

    /// A conservative corruption signal, never a receipt or root binding.
    /// Write before an initial COMMIT; clear only after known rollback. Its
    /// presence forbids interpreting lost engine metadata as a new workspace.
    #[cfg(feature = "native")]
    pub(crate) fn managed_evidence_state(&self) -> io::Result<ManagedEvidence> {
        self.recheck()?;
        let length = self.file.metadata()?.len();
        if length == 0 {
            return Ok(ManagedEvidence::Absent);
        }
        let mut bytes = [0; 64];
        if length > bytes.len() as u64
            || self.file.read_at(&mut bytes[..length as usize], 0)? != length as usize
        {
            return Err(unsafe_path("invalid workspace managed-evidence marker"));
        }
        match &bytes[..length as usize] {
            b"grv-managed-v1\n" => Ok(ManagedEvidence::Managed),
            b"grv-managed-build-v1\n" => Ok(ManagedEvidence::BuildHistory),
            _ => Err(unsafe_path("invalid workspace managed-evidence marker")),
        }
    }
    #[cfg(feature = "native")]
    pub(crate) fn managed_evidence_recorded(&self) -> io::Result<bool> {
        Ok(self.managed_evidence_state()? != ManagedEvidence::Absent)
    }
    #[cfg(feature = "native")]
    pub(crate) fn build_evidence_recorded(&self) -> io::Result<bool> {
        Ok(self.managed_evidence_state()? == ManagedEvidence::BuildHistory)
    }
    /// Restore only a state captured before a transaction whose rollback is proved.
    #[cfg(feature = "native")]
    pub(crate) fn restore_managed_evidence(&self, state: ManagedEvidence) -> io::Result<()> {
        self.recheck()?;
        let bytes: &[u8] = match state {
            ManagedEvidence::Absent => b"",
            ManagedEvidence::Managed => b"grv-managed-v1\n",
            ManagedEvidence::BuildHistory => b"grv-managed-build-v1\n",
        };
        self.file.write_all_at(bytes, 0)?;
        self.file.set_len(bytes.len() as u64)?;
        self.file.sync_all()?;
        File::open(self.helper.parent().expect("canonical helper parent"))?.sync_all()?;
        self.recheck()
    }
    #[cfg(feature = "native")]
    pub(crate) fn record_build_evidence(&self) -> io::Result<()> {
        self.managed_evidence_state()?;
        self.restore_managed_evidence(ManagedEvidence::BuildHistory)
    }
    #[cfg(feature = "native")]
    pub(crate) fn record_managed_evidence(&self, recorded: bool) -> io::Result<()> {
        let previous = self.managed_evidence_state()?;
        if previous == ManagedEvidence::BuildHistory {
            return if recorded {
                Ok(())
            } else {
                Err(unsafe_path("cannot erase durable build history evidence"))
            };
        }
        self.restore_managed_evidence(if recorded {
            ManagedEvidence::Managed
        } else {
            ManagedEvidence::Absent
        })
    }

    /// Call under lock before opening/reopening a native connection.
    pub fn recheck(&self) -> io::Result<()> {
        check_ancestors(&self.helper)?;
        let opened = self.file.metadata()?;
        let named = fs::symlink_metadata(&self.helper)?;
        let uid = unsafe { libc::geteuid() };
        if !opened.is_file()
            || opened.nlink() != 1
            || opened.uid() != uid
            || opened.mode() & 0o077 != 0
            || !named.is_file()
            || named.nlink() != 1
            || named.uid() != uid
            || named.mode() & 0o077 != 0
            || (opened.dev(), opened.ino()) != (named.dev(), named.ino())
        {
            return Err(unsafe_path("workspace lock helper was replaced"));
        }
        if canonical_engine_path(&self.engine)? != self.engine {
            return Err(unsafe_path("database identity changed"));
        }
        if let Some(original) = self.original_engine {
            let metadata = fs::metadata(&self.engine)?;
            if original != (metadata.dev(), metadata.ino()) {
                return Err(unsafe_path("database inode was replaced"));
            }
        }
        Ok(())
    }

    /// Captures the inode after native initialization creates a previously absent file.
    pub fn record_created_engine(&mut self) -> io::Result<()> {
        self.recheck()?;
        let metadata = fs::metadata(&self.engine)?;
        self.original_engine = Some((metadata.dev(), metadata.ino()));
        Ok(())
    }

    /// Returns a duplicate of the same open-file description. A supervised child's
    /// pre-exec hook must clear FD_CLOEXEC on this fd; keep it open until that child stops.
    pub fn descendant_reference(&self) -> io::Result<File> {
        self.file.try_clone()
    }
}
