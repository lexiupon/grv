//! Protected, immutable external-driver completion files. A record is published
//! only by the stopped invocation, never inferred from prepared output tables.
use grv_adapter_api::{BuildCompletion, BuildSession};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
};
pub const MAX_COMPLETION_BYTES: u64 = 2 * 1024 * 1024;
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn protected(m: &fs::Metadata) -> bool {
    m.is_file() && m.nlink() == 1 && m.uid() == unsafe { libc::geteuid() } && m.mode() & 0o077 == 0
}
pub fn read(path: &Path, session: &BuildSession) -> io::Result<BuildCompletion> {
    crate::lock::check_ancestors(path)?;
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    let before = file.metadata()?;
    if !protected(&before) || before.len() > MAX_COMPLETION_BYTES {
        return Err(invalid(
            "completion file is unprotected or exceeds its bound",
        ));
    }
    let mut bytes = Vec::with_capacity(before.len() as usize);
    (&mut file)
        .take(MAX_COMPLETION_BYTES + 1)
        .read_to_end(&mut bytes)?;
    let after = file.metadata()?;
    let named = fs::symlink_metadata(path)?;
    if bytes.len() as u64 != before.len()
        || !protected(&after)
        || !protected(&named)
        || (
            before.dev(),
            before.ino(),
            before.len(),
            before.mtime(),
            before.mtime_nsec(),
            before.ctime(),
            before.ctime_nsec(),
        ) != (
            after.dev(),
            after.ino(),
            after.len(),
            after.mtime(),
            after.mtime_nsec(),
            after.ctime(),
            after.ctime_nsec(),
        )
        || (after.dev(), after.ino()) != (named.dev(), named.ino())
    {
        return Err(invalid("completion file changed while being read"));
    }
    let value = grv_adapter_wire::json::parse(&bytes).map_err(io::Error::other)?;
    let completion: BuildCompletion = serde_json::from_value(value).map_err(io::Error::other)?;
    completion.validate_for(session).map_err(io::Error::other)?;
    Ok(completion)
}

/// Atomically installs canonical bytes without overwriting an earlier record.
/// An existing file must contain the identical canonical completion.
pub fn publish(
    path: &Path,
    session: &BuildSession,
    completion: &BuildCompletion,
) -> io::Result<()> {
    completion.validate_for(session).map_err(io::Error::other)?;
    let bytes = grv_types::canonical_json(completion).map_err(io::Error::other)?;
    if bytes.len() as u64 > MAX_COMPLETION_BYTES {
        return Err(invalid("completion document exceeds its bound"));
    }
    crate::lock::check_ancestors(path)?;
    match read(path, session) {
        Ok(old) => {
            return if old == *completion {
                Ok(())
            } else {
                Err(invalid(
                    "completion record differs from established invocation",
                ))
            };
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let directory = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let temporary = directory.join(format!(".grv-completion-{}", grv_types::Uuid::v4()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        // link is a no-replace atomic publication on both supported OSes.
        match fs::hard_link(&temporary, path) {
            Ok(()) => {
                fs::remove_file(&temporary)?;
                File::open(directory)?.sync_all()?;
                if read(path, session)? != *completion {
                    return Err(invalid("published completion changed"));
                }
                Ok(())
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                fs::remove_file(&temporary)?;
                if read(path, session)? == *completion {
                    Ok(())
                } else {
                    Err(invalid("competing completion differs"))
                }
            }
            Err(error) => Err(error),
        }
    })();
    let _ = fs::remove_file(&temporary);
    result
}
