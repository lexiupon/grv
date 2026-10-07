//! Stage, verify and smoke-test before publishing an adapter installation.
use crate::{
    Error, Result,
    discovery::{SearchRoot, read_installation, trusted},
    process::{Deadlines, Session},
};
use grv_adapter_api::{AdapterInstallResult, Manifest};
use grv_types::{ErrorCode, Uuid};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Component, Path, PathBuf},
};

pub fn install(
    source: &Path,
    root: &SearchRoot,
    replace: bool,
    deadlines: Deadlines,
) -> Result<AdapterInstallResult> {
    fs::create_dir_all(&root.path)?;
    trusted(&root.path, None)?;
    let stage = root.path.join(format!(".install-{}", Uuid::v4()));
    fs::create_dir(&stage)?;
    fs::set_permissions(&stage, fs::Permissions::from_mode(0o700))?;
    let result = (|| {
        let content = stage.join("content");
        fs::create_dir(&content)?;
        if source.is_dir() {
            copy_directory(source, &content)?;
        } else {
            unpack(source, &content)?;
        }
        let package = if content.join("adapter.toml").is_file() {
            content.clone()
        } else {
            let dirs: Vec<_> = fs::read_dir(&content)?.collect::<std::io::Result<Vec<_>>>()?;
            if dirs.len() != 1 || !dirs[0].path().is_dir() {
                return Err(Error::new(
                    ErrorCode::InvalidArgument,
                    "tarball must contain one adapter package",
                ));
            }
            dirs[0].path()
        };
        let manifest: Manifest = toml::from_str(&fs::read_to_string(package.join("adapter.toml"))?)
            .map_err(|_| Error::new(ErrorCode::InvalidArgument, "invalid installation manifest"))?;
        manifest
            .validate()
            .map_err(|e| Error::new(ErrorCode::InvalidArgument, e.to_string()))?;
        let named = stage.join(manifest.name.as_str());
        fs::rename(package, &named)?;
        let target = root.path.join(manifest.name.as_str());
        if target.exists() && !replace {
            return Err(Error::new(
                ErrorCode::StateConflict,
                "adapter exists; use --replace for explicit replacement",
            ));
        }
        let candidate = read_installation(&named, root.kind)?;
        let mut process = Session::spawn(&candidate, deadlines)?;
        process.close()?;
        // All bytes and directory entries durable before atomic publication.
        sync_tree(&named)?;
        let replaced = target.exists();
        if replaced {
            read_installation(&target, root.kind)?;
            exchange(&named, &target)?;
        } else {
            fs::rename(&named, &target)?;
        }
        fs::File::open(&root.path)?.sync_all()?;
        fs::File::open(&stage)?.sync_all()?;
        let installed = read_installation(&target, root.kind)?;
        if replaced {
            fs::remove_dir_all(&named)?;
        }
        Ok(AdapterInstallResult {
            adapter: installed.entry(),
            replaced,
        })
    })();
    let _ = fs::remove_dir_all(&stage);
    result
}
fn exchange(a: &Path, b: &Path) -> Result<()> {
    let a = std::ffi::CString::new(a.as_os_str().as_encoded_bytes())
        .map_err(|_| Error::new(ErrorCode::InvalidArgument, "invalid package path"))?;
    let b = std::ffi::CString::new(b.as_os_str().as_encoded_bytes())
        .map_err(|_| Error::new(ErrorCode::InvalidArgument, "invalid package path"))?;
    #[cfg(target_os = "linux")]
    let status = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            a.as_ptr(),
            libc::AT_FDCWD,
            b.as_ptr(),
            libc::RENAME_EXCHANGE,
        )
    };
    #[cfg(target_os = "macos")]
    let status = unsafe { libc::renamex_np(a.as_ptr(), b.as_ptr(), libc::RENAME_SWAP) };
    if status != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}
fn copy_directory(source: &Path, target: &Path) -> Result<()> {
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let m = entry.path().symlink_metadata()?;
        let dest = target.join(entry.file_name());
        if m.is_dir() {
            fs::create_dir(&dest)?;
            copy_directory(&entry.path(), &dest)?;
        } else if m.is_file() {
            fs::copy(entry.path(), &dest)?;
            let mode = if m.permissions().mode() & 0o111 != 0 {
                0o700
            } else {
                0o600
            };
            fs::set_permissions(dest, fs::Permissions::from_mode(mode))?;
        } else {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                "installation package requires regular files and directories",
            ));
        }
    }
    Ok(())
}
fn unpack(source: &Path, target: &Path) -> Result<()> {
    use std::io::Read;
    let mut f = fs::File::open(source)?;
    let mut magic = [0; 2];
    f.read_exact(&mut magic)?;
    drop(f);
    let f = fs::File::open(source)?;
    let reader: Box<dyn Read> = if magic == [0x1f, 0x8b] {
        Box::new(flate2::read::GzDecoder::new(f))
    } else {
        Box::new(f)
    };
    let mut archive = tar::Archive::new(reader);
    let mut total = 0u64;
    for (i, entry) in archive.entries()?.enumerate() {
        if i > 10000 {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                "too many installation files",
            ));
        }
        let mut entry = entry?;
        let path: PathBuf = entry.path()?.into_owned();
        if path
            .components()
            .any(|c| !matches!(c, Component::Normal(_) | Component::CurDir))
        {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                "tarball path escapes package",
            ));
        }
        let dest = target.join(path);
        let kind = entry.header().entry_type();
        if kind.is_dir() {
            fs::create_dir_all(&dest)?;
        } else if kind.is_file() {
            total = total.checked_add(entry.size()).ok_or_else(|| {
                Error::new(ErrorCode::InvalidArgument, "installation size overflow")
            })?;
            if total > 1024 * 1024 * 1024 {
                return Err(Error::new(
                    ErrorCode::InvalidArgument,
                    "installation exceeds supported size",
                ));
            }
            if let Some(parent) = dest.parent() {
                fs::create_dir_all(parent)?;
            }
            let mode = if entry.header().mode()? & 0o111 != 0 {
                0o700
            } else {
                0o600
            };
            let mut out = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&dest)?;
            std::io::copy(&mut entry, &mut out)?;
            fs::set_permissions(&dest, fs::Permissions::from_mode(mode))?;
        } else {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                "tarball contains link or special file",
            ));
        }
    }
    Ok(())
}
fn sync_tree(path: &Path) -> Result<()> {
    for e in fs::read_dir(path)? {
        let e = e?;
        if e.path().is_dir() {
            sync_tree(&e.path())?;
        } else {
            fs::File::open(e.path())?.sync_all()?;
        }
    }
    fs::File::open(path)?.sync_all()?;
    Ok(())
}
