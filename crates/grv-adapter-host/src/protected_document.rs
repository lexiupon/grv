//! Protected token-bearing consumer documents. Closed records must use serde's
//! deny_unknown_fields; this module separately rejects duplicate JSON keys.
use crate::{Error, Result};
use grv_types::ErrorCode;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};
fn invalid(message: &str) -> Error {
    Error::new(ErrorCode::InvalidArgument, message)
}
fn io_error(error: io::Error) -> Error {
    Error::new(
        ErrorCode::InvalidArgument,
        format!("protected document unavailable: {error}"),
    )
}
fn ancestors(path: &Path) -> Result<()> {
    let uid = unsafe { libc::geteuid() };
    for ancestor in path.ancestors().skip(1) {
        let m = fs::metadata(ancestor).map_err(io_error)?;
        if !m.is_dir()
            || (m.uid() != uid && m.uid() != 0)
            || (m.mode() & 0o022 != 0 && !(m.uid() == 0 && m.mode() & 0o1000 != 0))
        {
            return Err(invalid("protected document has an untrusted ancestor"));
        }
    }
    Ok(())
}
fn resolved(path: &Path) -> Result<PathBuf> {
    let name = path
        .file_name()
        .ok_or_else(|| invalid("document filename missing"))?;
    match fs::symlink_metadata(path) {
        Ok(metadata) if !protected(&metadata) => {
            return Err(invalid(
                "existing document must be a protected owner-0600 regular file",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(io_error(error)),
    }
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let result = fs::canonicalize(parent).map_err(io_error)?.join(name);
    ancestors(&result)?;
    Ok(result)
}
/// Resolve a document's existing canonical parent and refuse placement in
/// excluded GRV roots or at/in an excluded engine path. Exclusions are resolved
/// independently so symlinked ancestor aliases cannot bypass this check.
pub fn canonical_path(path: &Path, excluded: &[&Path]) -> Result<PathBuf> {
    let document = resolved(path)?;
    for exclusion in excluded {
        let excluded = match fs::canonicalize(exclusion) {
            Ok(path) => path,
            Err(error) if error.kind() == io::ErrorKind::NotFound => resolved(exclusion)?,
            Err(error) => return Err(io_error(error)),
        };
        if document.starts_with(excluded) {
            return Err(invalid(
                "document lies within excluded GRV or engine storage",
            ));
        }
    }
    Ok(document)
}
fn protected(m: &fs::Metadata) -> bool {
    m.is_file()
        && m.nlink() == 1
        && m.uid() == unsafe { libc::geteuid() }
        && m.mode() & 0o7777 == 0o600
}
fn fingerprint(m: &fs::Metadata) -> (u64, u64, u64, i64, i64, i64, i64) {
    (
        m.dev(),
        m.ino(),
        m.len(),
        m.mtime(),
        m.mtime_nsec(),
        m.ctime(),
        m.ctime_nsec(),
    )
}
fn read_value(path: &Path, max_bytes: u64) -> Result<Value> {
    if max_bytes == 0 {
        return Err(invalid("document size bound must be positive"));
    }
    let path = resolved(path)?;
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&path)
        .map_err(io_error)?;
    let before = file.metadata().map_err(io_error)?;
    if !protected(&before) || before.len() > max_bytes {
        return Err(invalid("document is unprotected or exceeds its size bound"));
    }
    let capacity = usize::try_from(before.len())
        .map_err(|_| invalid("document cannot fit a bounded buffer"))?;
    let mut bytes = Vec::with_capacity(capacity);
    (&mut file)
        .take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(io_error)?;
    let after = file.metadata().map_err(io_error)?;
    let named = fs::symlink_metadata(&path).map_err(io_error)?;
    if bytes.len() as u64 != before.len()
        || !protected(&after)
        || !protected(&named)
        || fingerprint(&before) != fingerprint(&after)
        || (after.dev(), after.ino()) != (named.dev(), named.ino())
    {
        return Err(invalid("document changed while being read"));
    }
    grv_adapter_wire::json::parse(&bytes).map_err(|_| invalid("document is not strict UTF-8 JSON"))
}
/// Parse a caller-defined closed record. Field-level validation remains the
/// caller's responsibility and must precede use of ownership tokens.
pub fn read<T: DeserializeOwned>(path: &Path, max_bytes: u64) -> Result<T> {
    serde_json::from_value(read_value(path, max_bytes)?)
        .map_err(|_| invalid("document does not match its closed record schema"))
}
fn same_and_sync(path: &Path, expected: &[u8], max_bytes: u64) -> Result<()> {
    let actual = grv_types::canonical_json(&read_value(path, max_bytes)?)
        .map_err(|_| invalid("document is not canonicalizable"))?;
    if actual != expected {
        return Err(Error::new(
            ErrorCode::RequestMismatch,
            "document differs from its established immutable value",
        ));
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(io_error)?;
    if !protected(&file.metadata().map_err(io_error)?) {
        return Err(invalid(
            "document protection changed before durability check",
        ));
    }
    file.sync_all().map_err(io_error)?;
    File::open(path.parent().expect("resolved document parent"))
        .map_err(io_error)?
        .sync_all()
        .map_err(io_error)?;
    let actual = grv_types::canonical_json(&read_value(path, max_bytes)?)
        .map_err(|_| invalid("document is not canonicalizable"))?;
    if actual != expected {
        return Err(invalid("document changed during durability check"));
    }
    Ok(())
}
/// Atomically publish canonical bytes, fsync file and directory, and never
/// overwrite a different record. Same-value replay verifies protection and
/// refreshes durability. Call canonical_path first to enforce excluded roots.
pub fn publish<T: Serialize>(path: &Path, value: &T, max_bytes: u64) -> Result<()> {
    let path = resolved(path)?;
    let bytes =
        grv_types::canonical_json(value).map_err(|_| invalid("document is not canonicalizable"))?;
    if bytes.is_empty() || bytes.len() as u64 > max_bytes {
        return Err(invalid("document exceeds its size bound"));
    }
    match fs::symlink_metadata(&path) {
        Ok(_) => return same_and_sync(&path, &bytes, max_bytes),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(io_error(error)),
    }
    let directory = path.parent().expect("resolved document parent");
    let temporary = directory.join(format!(".grv-document-{}", grv_types::Uuid::v4()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&temporary)
            .map_err(io_error)?;
        file.write_all(&bytes).map_err(io_error)?;
        file.sync_all().map_err(io_error)?;
        // Atomic no-replace publication on both macOS and Linux.
        match fs::hard_link(&temporary, &path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(io_error(error)),
        }
        fs::remove_file(&temporary).map_err(io_error)?;
        same_and_sync(&path, &bytes, max_bytes)
    })();
    let _ = fs::remove_file(&temporary);
    result
}
