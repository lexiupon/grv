use crate::{Error, Result};
use grv_adapter_api::{AdapterListEntry, AdapterListResult, Manifest};
use grv_types::{ErrorCode, Name};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, Metadata},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Copy)]
pub enum RootKind {
    User,
    System,
}
#[derive(Debug, Clone)]
pub struct SearchRoot {
    pub path: PathBuf,
    pub kind: RootKind,
}
#[derive(Debug, Clone)]
pub struct Installation {
    pub manifest: Manifest,
    pub manifest_path: PathBuf,
    pub directory: PathBuf,
    pub executable: PathBuf,
}
pub fn search_roots(system: Option<PathBuf>) -> Result<Vec<SearchRoot>> {
    if let Some(path) = std::env::var_os("GRV_ADAPTERS_DIR") {
        return Ok(vec![SearchRoot {
            path: absolute(PathBuf::from(path))?,
            kind: RootKind::User,
        }]);
    }
    let config = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|p| PathBuf::from(p).join(".config")))
        .ok_or_else(|| {
            Error::new(
                ErrorCode::InvalidArgument,
                "HOME or XDG_CONFIG_HOME is required",
            )
        })?;
    let mut roots = vec![SearchRoot {
        path: absolute(config.join("grv/adapters"))?,
        kind: RootKind::User,
    }];
    if let Some(path) = system {
        roots.push(SearchRoot {
            path: absolute(path)?,
            kind: RootKind::System,
        });
    }
    Ok(roots)
}
fn absolute(p: PathBuf) -> Result<PathBuf> {
    if p.is_absolute() {
        Ok(p)
    } else {
        Ok(std::env::current_dir()?.join(p))
    }
}
pub fn trusted(path: &Path, required_owner: Option<u32>) -> Result<PathBuf> {
    let canonical = fs::canonicalize(path)?;
    let uid = unsafe { libc::geteuid() };
    for (i, part) in canonical.ancestors().enumerate() {
        let m = fs::metadata(part)?;
        if (m.uid() != 0 && m.uid() != uid)
            || m.mode() & 0o022 != 0
            || (i == 0 && required_owner.is_some_and(|u| m.uid() != u))
        {
            return Err(Error::new(
                ErrorCode::AdapterFailure,
                format!("untrusted adapter path: {}", part.display()),
            ));
        }
    }
    Ok(canonical)
}
pub fn read_installation(directory: &Path, kind: RootKind) -> Result<Installation> {
    let owner = match kind {
        RootKind::User => unsafe { libc::geteuid() },
        RootKind::System => 0,
    };
    let resolved_dir = trusted(directory, Some(owner))?;
    if !fs::metadata(&resolved_dir)?.is_dir() {
        return Err(Error::new(
            ErrorCode::AdapterFailure,
            "adapter directory is not a directory",
        ));
    }
    let manifest_path = trusted(&resolved_dir.join("adapter.toml"), Some(owner))?;
    let metadata = fs::metadata(&manifest_path)?;
    if !metadata.is_file() || metadata.len() > 1024 * 1024 {
        return Err(Error::new(
            ErrorCode::AdapterFailure,
            "invalid adapter manifest file",
        ));
    }
    let manifest: Manifest = toml::from_str(&fs::read_to_string(&manifest_path)?)
        .map_err(|_| Error::new(ErrorCode::AdapterFailure, "invalid closed adapter manifest"))?;
    manifest
        .validate()
        .map_err(|e| Error::new(ErrorCode::AdapterFailure, e.to_string()))?;
    if directory.file_name().and_then(|s| s.to_str()) != Some(manifest.name.as_str()) {
        return Err(Error::new(
            ErrorCode::AdapterFailure,
            "adapter manifest name differs from directory",
        ));
    }
    let path = Path::new(&manifest.entrypoint);
    let executable = if path.is_absolute() {
        path.to_owned()
    } else {
        resolved_dir.join(path)
    };
    let executable = trusted(&executable, None)?;
    let m = fs::metadata(&executable)?;
    if !m.is_file() || m.mode() & 0o111 == 0 {
        return Err(Error::new(
            ErrorCode::AdapterFailure,
            "entrypoint is not a regular executable",
        ));
    }
    Ok(Installation {
        manifest,
        manifest_path,
        directory: resolved_dir,
        executable,
    })
}
pub fn discover(roots: &[SearchRoot]) -> Result<Vec<Installation>> {
    let mut winners = BTreeMap::new();
    for root in roots {
        if !root.path.exists() {
            continue;
        }
        trusted(&root.path, None)?;
        let mut dirs: Vec<_> = fs::read_dir(&root.path)?.collect::<std::io::Result<Vec<_>>>()?;
        dirs.sort_by_key(|e| e.file_name());
        for entry in dirs {
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if Name::new(&name).is_err() || winners.contains_key(&name) {
                continue;
            }
            if !entry.path().join("adapter.toml").exists() {
                continue;
            }
            winners.insert(name, read_installation(&entry.path(), root.kind)?);
        }
    }
    Ok(winners.into_values().collect())
}
impl Installation {
    pub fn entry(&self) -> AdapterListEntry {
        AdapterListEntry {
            name: self.manifest.name.clone(),
            package_version: self.manifest.version.clone(),
            interface_versions: self.manifest.interface_versions.clone(),
            binding_schema_version: self.manifest.binding_schema_version,
            manifest: self.manifest_path.display().to_string(),
        }
    }
    pub(crate) fn verified_executable(&self) -> Result<(File, Metadata)> {
        trusted(&self.executable, None)?;
        let mut f = File::open(&self.executable)?;
        let metadata = f.metadata()?;
        if !metadata.is_file() || metadata.mode() & 0o111 == 0 {
            return Err(Error::new(ErrorCode::AdapterFailure, "entrypoint changed"));
        }
        if let Some(expected) = &self.manifest.entrypoint_sha256 {
            let mut hasher = Sha256::new();
            std::io::copy(&mut f, &mut hasher)?;
            if format!("{:x}", hasher.finalize()) != expected.as_str() {
                return Err(Error::new(
                    ErrorCode::IntegrityFailure,
                    "entrypoint digest differs",
                ));
            }
        }
        Ok((f, metadata))
    }
}
pub fn list(roots: &[SearchRoot]) -> Result<AdapterListResult> {
    Ok(AdapterListResult {
        search_roots: roots.iter().map(|r| r.path.display().to_string()).collect(),
        adapters: discover(roots)?.iter().map(Installation::entry).collect(),
    })
}
