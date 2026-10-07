//! Non-expiring session mutation ownership. Acquire before the workspace lock;
//! external invocations hold only their workspace lock, so renewals can proceed.
use crate::{Error, Result};
use grv_types::{ErrorCode, RunId};
use std::{
    fs::{self, File, OpenOptions},
    io,
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
};
fn invalid(message: &str) -> Error {
    Error::new(ErrorCode::InvalidArgument, message)
}
fn canonical_engine(path: &Path) -> Result<PathBuf> {
    if path.as_os_str().is_empty() || path == Path::new(":memory:") {
        return Err(invalid(
            "session ownership requires a local native database file",
        ));
    }
    match fs::metadata(path) {
        Ok(metadata) => {
            if !metadata.is_file() || metadata.nlink() != 1 {
                return Err(invalid(
                    "engine must be one regular native file without hard-link aliases",
                ));
            }
            fs::canonicalize(path).map_err(Error::from)
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            if fs::symlink_metadata(path).is_ok() {
                return Err(invalid("engine symlink is dangling"));
            }
            let filename = path
                .file_name()
                .ok_or_else(|| invalid("engine filename missing"))?;
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            Ok(fs::canonicalize(parent)?.join(filename))
        }
        Err(e) => Err(e.into()),
    }
}
fn ancestors(path: &Path) -> Result<()> {
    let uid = unsafe { libc::geteuid() };
    for ancestor in path.ancestors().skip(1) {
        let m = fs::metadata(ancestor)?;
        if !m.is_dir()
            || (m.uid() != uid && m.uid() != 0)
            || (m.mode() & 0o022 != 0 && !(m.uid() == 0 && m.mode() & 0o1000 != 0))
        {
            return Err(invalid("session lock has an untrusted ancestor"));
        }
    }
    Ok(())
}
fn protected(m: &fs::Metadata) -> bool {
    m.is_file() && m.nlink() == 1 && m.uid() == unsafe { libc::geteuid() } && m.mode() & 0o077 == 0
}
#[derive(Debug)]
pub struct SessionMutationLock {
    file: File,
    engine: PathBuf,
    helper: PathBuf,
    run: RunId,
    original_engine: Option<(u64, u64)>,
}
impl SessionMutationLock {
    pub fn acquire(engine_path: &Path, run_id: &RunId) -> Result<Self> {
        let engine = canonical_engine(engine_path)?;
        let mut filename = engine.as_os_str().to_os_string();
        filename.push(format!(".grv-session-{run_id}.lock"));
        let helper = PathBuf::from(filename);
        ancestors(&helper)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&helper)
            .map_err(|e| {
                Error::new(
                    ErrorCode::InvalidArgument,
                    format!("session lock file unavailable: {e}"),
                )
            })?;
        if !protected(&file.metadata()?) {
            return Err(invalid(
                "session lock is not a protected regular consumer file",
            ));
        }
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            let error = io::Error::last_os_error();
            return Err(Error::new(
                if error.kind() == io::ErrorKind::WouldBlock {
                    ErrorCode::EngineBusy
                } else {
                    ErrorCode::AdapterFailure
                },
                "session mutation lock is busy or unavailable",
            ));
        }
        let owner = Self {
            file,
            helper,
            original_engine: fs::metadata(&engine).ok().map(|m| (m.dev(), m.ino())),
            engine,
            run: run_id.clone(),
        };
        owner.recheck()?;
        Ok(owner)
    }
    pub fn engine_path(&self) -> &Path {
        &self.engine
    }
    pub fn helper_path(&self) -> &Path {
        &self.helper
    }
    pub fn run_id(&self) -> &RunId {
        &self.run
    }
    pub fn recheck(&self) -> Result<()> {
        ancestors(&self.helper)?;
        let opened = self.file.metadata()?;
        let named = fs::symlink_metadata(&self.helper)?;
        if !protected(&opened)
            || !protected(&named)
            || (opened.dev(), opened.ino()) != (named.dev(), named.ino())
            || canonical_engine(&self.engine)? != self.engine
        {
            return Err(invalid("session lock or engine identity changed"));
        }
        if let Some(original) = self.original_engine {
            let current = fs::metadata(&self.engine)?;
            if original != (current.dev(), current.ino()) {
                return Err(invalid("engine inode changed under session lock"));
            }
        }
        Ok(())
    }
}
// Drop closes the open-file description; never unlock or unlink the helper.
