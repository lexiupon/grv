//! Exact pinned signed extension copies. The native loader also verifies DuckDB's
//! signature; private immutable copies prevent pathname replacement after hashing.
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};
pub(crate) struct Extensions {
    pub paths: [PathBuf; 2],
    directory: PathBuf,
}
impl Drop for Extensions {
    fn drop(&mut self) {
        let _ = fs::set_permissions(&self.directory, fs::Permissions::from_mode(0o700));
        for path in &self.paths {
            let _ = fs::remove_file(path);
        }
        let _ = fs::remove_dir(&self.directory);
    }
}
impl Extensions {
    pub fn stage(engine: &Path) -> io::Result<Self> {
        let packaged = std::env::current_exe()?
            .parent()
            .ok_or_else(|| io::Error::other("executable directory unavailable"))?
            .join("extensions");
        let source = if packaged.exists() {
            packaged
        } else {
            PathBuf::from(
                std::env::var_os("GRV_DUCKDB_EXTENSIONS_DIR")
                    .ok_or_else(|| io::Error::other("packaged signed S3 extensions unavailable"))?,
            )
        };
        let source = fs::canonicalize(source)?;
        let platform = match (std::env::consts::OS, std::env::consts::ARCH) {
            ("macos", "aarch64") => "osx_arm64",
            ("macos", "x86_64") => "osx_amd64",
            ("linux", "aarch64") => "linux_arm64",
            ("linux", "x86_64") => "linux_amd64",
            _ => return Err(io::Error::other("unsupported native extension platform")),
        };
        let manifest: serde_json::Value =
            serde_json::from_str(include_str!("../../../notices/native-extensions.json"))
                .map_err(io::Error::other)?;
        let directory = engine
            .parent()
            .ok_or_else(|| io::Error::other("engine directory unavailable"))?
            .join(format!(".grv-extensions-{}", grv_types::Uuid::v4()));
        fs::DirBuilder::new().mode(0o700).create(&directory)?;
        let staged = Self {
            paths: [
                directory.join("httpfs.duckdb_extension"),
                directory.join("aws.duckdb_extension"),
            ],
            directory,
        };
        for (index, name) in ["httpfs", "aws"].iter().enumerate() {
            let artifact = manifest["artifacts"]
                .as_array()
                .unwrap()
                .iter()
                .find(|a| a["platform"] == platform && a["name"] == *name)
                .ok_or_else(|| io::Error::other("pinned extension artifact unavailable"))?;
            let path = source.join(format!("{name}.duckdb_extension"));
            crate::lock::check_ancestors(&path)?;
            let mut input = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(&path)?;
            let before = input.metadata()?;
            if !before.is_file()
                || before.nlink() != 1
                || before.uid() != unsafe { libc::geteuid() }
                || before.mode() & 0o022 != 0
                || Some(before.len()) != artifact["bytes"].as_u64()
            {
                return Err(io::Error::other("untrusted pinned extension file"));
            }
            let mut out = OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(&staged.paths[index])?;
            let mut hash = Sha256::new();
            let mut buffer = [0; 65536];
            loop {
                let count = input.read(&mut buffer)?;
                if count == 0 {
                    break;
                }
                hash.update(&buffer[..count]);
                out.write_all(&buffer[..count])?;
            }
            let after = input.metadata()?;
            let named = fs::symlink_metadata(&path)?;
            let identity = |m: &fs::Metadata| {
                (
                    m.dev(),
                    m.ino(),
                    m.len(),
                    m.mode(),
                    m.uid(),
                    m.nlink(),
                    m.mtime(),
                    m.mtime_nsec(),
                )
            };
            if identity(&before) != identity(&after)
                || identity(&after) != identity(&named)
                || format!("{:x}", hash.finalize()) != artifact["sha256"].as_str().unwrap()
            {
                return Err(io::Error::other(
                    "pinned extension changed or hash mismatch",
                ));
            }
            out.sync_all()?;
            fs::set_permissions(&staged.paths[index], fs::Permissions::from_mode(0o400))?;
        }
        File::open(&staged.directory)?.sync_all()?;
        fs::set_permissions(&staged.directory, fs::Permissions::from_mode(0o500))?;
        Ok(staged)
    }
}
use std::os::unix::fs::DirBuilderExt;
