//! Read-only native keychain bridge for the pinned Salesforce CLI. Both the
//! native credential reader and Node retain the no-fork helper containment.
//! Key material crosses a private pipe only; it is never an argument or file.
use crate::{Error, Result, integrity, runtime::*};
use sha2::{Digest, Sha256};
use std::{
    ffi::{OsStr, OsString},
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

const CLI_VERSION: &str = "2.152.14";
const CORE_VERSION: &str = "9.2.2";
const ENTRY_HASH: &str = "fa2dfe2a89e652be067a06cfa2c766d52d3340342d6a19c255a8b9b917befb6e";
const MODULE_HASH: &str = "892e2d137f817c60838011297ea6ed9bd037f1d0108ad79396aca6598fa4a65b";
const BOOTSTRAP: &str = include_str!("keychain_bootstrap.cjs");
const GETTER: &str = include_str!("keychain_getter.cjs");

fn failure() -> Error {
    Error::new(
        "ADAPTER_FAILURE",
        "Salesforce read-only keychain bridge unavailable",
    )
}

fn resolve(program: &OsStr) -> Result<PathBuf> {
    let path = Path::new(program);
    let found = if path.components().count() > 1 {
        path.to_path_buf()
    } else {
        std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
            .map(|dir| dir.join(path))
            .find(|candidate| candidate.is_file())
            .ok_or_else(failure)?
    };
    fs::canonicalize(found).map_err(|_| failure())
}

fn bounded_read(path: &Path) -> Result<Vec<u8>> {
    use std::io::Read;
    let file = fs::File::open(path).map_err(|_| failure())?;
    let mut bytes = Vec::new();
    file.take(256 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| failure())?;
    if bytes.len() > 256 * 1024 {
        return Err(failure());
    }
    Ok(bytes)
}
fn check_package(path: &Path, name: &str, version: &str) -> Result<()> {
    let value = grv_adapter_wire::json::parse(&bounded_read(path)?).map_err(|_| failure())?;
    if value.get("name").and_then(serde_json::Value::as_str) != Some(name)
        || value.get("version").and_then(serde_json::Value::as_str) != Some(version)
    {
        return Err(failure());
    }
    Ok(())
}
fn check_hash(path: &Path, expected: &str) -> Result<()> {
    if format!("{:x}", Sha256::digest(bounded_read(path)?)) != expected {
        return Err(failure());
    }
    Ok(())
}

struct Installation {
    entry: PathBuf,
    module: PathBuf,
    node: PathBuf,
}
impl Installation {
    fn inspect(program: &OsStr) -> Result<Option<Self>> {
        let entry = resolve(program)?;
        // Test fixtures and other executables use the ordinary contained path.
        // A Salesforce package at this location must match the complete pin.
        if entry.file_name() != Some(OsStr::new("run.js"))
            || entry.parent().and_then(Path::file_name) != Some(OsStr::new("bin"))
        {
            return Ok(None);
        }
        let root = entry.parent().and_then(Path::parent).ok_or_else(failure)?;
        let package = root.join("package.json");
        let value =
            grv_adapter_wire::json::parse(&bounded_read(&package)?).map_err(|_| failure())?;
        if value.get("name").and_then(serde_json::Value::as_str) != Some("@salesforce/cli") {
            return Ok(None);
        }
        check_package(&package, "@salesforce/cli", CLI_VERSION)?;
        let core = root.join("node_modules/@salesforce/core");
        check_package(&core.join("package.json"), "@salesforce/core", CORE_VERSION)?;
        let module = core.join("lib/crypto/keyChainImpl.js");
        if fs::canonicalize(&module).map_err(|_| failure())? != module {
            return Err(failure());
        }
        check_hash(&entry, ENTRY_HASH)?;
        check_hash(&module, MODULE_HASH)?;
        Ok(Some(Self {
            entry,
            module,
            node: resolve(OsStr::new("node"))?,
        }))
    }
}

fn key_text(bytes: &[u8]) -> Result<&str> {
    let bytes = bytes.strip_suffix(b"\n").unwrap_or(bytes);
    let text = std::str::from_utf8(bytes).map_err(|_| failure())?;
    if text.is_empty() || text.len() > 512 || text.chars().any(char::is_control) {
        return Err(failure());
    }
    Ok(text)
}

fn bridge_spec(spec: ProcessSpec, install: Installation, key: &str) -> Result<ProcessSpec> {
    let args: Vec<_> = spec
        .args
        .iter()
        .map(|arg| arg.to_str().ok_or_else(failure))
        .collect::<Result<_>>()?;
    let payload = serde_json::json!({
        "entry": install.entry, "module": install.module, "key": key,
        "platform": std::env::consts::OS, "args": args
    });
    let stdin = serde_json::to_vec(&payload).map_err(|_| failure())?;
    if stdin.len() > 64 * 1024 {
        return Err(integrity("Salesforce keychain bridge input exceeds bound"));
    }
    Ok(ProcessSpec {
        program: install.node.into_os_string(),
        args: vec![
            "--no-deprecation".into(),
            "-e".into(),
            format!("{GETTER}\n{BOOTSTRAP}").into(),
        ],
        stdin,
        ..spec
    })
}

/// Keep native credential lookup separate from the CLI. No create/set command
/// is available, even when the credential is missing or cannot decrypt auth.
pub(crate) fn prepare(spec: ProcessSpec, cancellation: &Cancellation) -> Result<ProcessSpec> {
    cancellation.check()?;
    let Some(install) = Installation::inspect(&spec.program)? else {
        return Ok(spec);
    };
    // The generic Unix backend reads its existing protected file without a
    // child process. Do not change a user's selected keychain implementation.
    if [
        "SF_USE_GENERIC_UNIX_KEYCHAIN",
        "SFDX_USE_GENERIC_UNIX_KEYCHAIN",
    ]
    .iter()
    .any(|name| std::env::var(name).is_ok_and(|value| value.eq_ignore_ascii_case("true")))
    {
        return Ok(spec);
    }
    #[cfg(target_os = "macos")]
    let (program, args) = (
        OsString::from("/usr/bin/security"),
        vec!["find-generic-password", "-a", "local", "-s", "sfdx", "-w"]
            .into_iter()
            .map(OsString::from)
            .collect(),
    );
    #[cfg(target_os = "linux")]
    let (program, args) = {
        let program = std::env::var_os("SFDX_SECRET_TOOL_PATH")
            .unwrap_or_else(|| "/usr/bin/secret-tool".into());
        if !Path::new(&program).is_file() {
            return Ok(spec);
        }
        (
            program,
            vec!["lookup", "user", "local", "domain", "sfdx"]
                .into_iter()
                .map(OsString::from)
                .collect(),
        )
    };
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    return Err(failure());
    let output = run(
        ProcessSpec {
            program,
            args,
            supervisor: spec.supervisor.clone(),
            env: spec.env.clone(),
            stdin: vec![],
            stdout_limit: 1024,
            timeout: Duration::from_secs(15),
        },
        cancellation,
    )?;
    if !output.status.success() {
        return Err(failure());
    }
    let key = key_text(&output.stdout)?;
    bridge_spec(spec, install, key)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_key_is_bounded_single_line_and_errors_are_redacted() {
        assert_eq!(key_text(b"private-canary\n").unwrap(), "private-canary");
        for bytes in [
            b"".as_slice(),
            b"private-canary\nother",
            b"private-canary\r\n",
            &[0xff],
            &[b'a'; 513],
        ] {
            let error = key_text(bytes).unwrap_err();
            assert!(!error.to_string().contains("private-canary"));
        }
    }
    #[test]
    fn bridge_material_is_only_private_stdin_and_no_files_are_created() {
        let root = tempfile::tempdir().unwrap();
        let spec = ProcessSpec {
            program: "sf".into(),
            supervisor: None,
            args: vec!["org".into(), "display".into()],
            env: vec![],
            stdin: vec![],
            stdout_limit: 1024,
            timeout: Duration::from_secs(1),
        };
        let installation = Installation {
            entry: root.path().join("bin/run.js"),
            module: root
                .path()
                .join("node_modules/@salesforce/core/lib/crypto/keyChainImpl.js"),
            node: "node".into(),
        };
        let spec = bridge_spec(spec, installation, "private-canary").unwrap();
        assert!(spec.stdin.windows(14).any(|word| word == b"private-canary"));
        assert!(
            !spec
                .args
                .iter()
                .any(|arg| arg.to_string_lossy().contains("private-canary"))
        );
        assert!(spec.env.is_empty());
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
    }
    #[test]
    fn package_and_content_mismatches_are_refused() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("package.json");
        fs::write(&path, br#"{"name":"@salesforce/core","version":"9.2.3"}"#).unwrap();
        assert!(check_package(&path, "@salesforce/core", CORE_VERSION).is_err());
        assert!(check_hash(&path, MODULE_HASH).is_err());
    }
}
