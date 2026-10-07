//! Test-only explicit private authorization coordinates, never credentials.
use serde::Deserialize;
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    pub sf: Salesforce,
    pub gcs: Gcs,
    pub s3: S3,
    pub archive: serde_json::Map<String, serde_json::Value>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Salesforce {
    pub org: String,
    pub org_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Gcs {
    pub root: String,
    pub account: String,
    pub project: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3 {
    pub root: String,
    pub profile: String,
    pub region: String,
}

fn no_secrets(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(map) => map.iter().all(|(key, value)| {
            let key = key.to_ascii_lowercase().replace(['_', '-'], "");
            !["token", "secret", "password", "credential", "accesskey"]
                .iter()
                .any(|word| key.contains(word))
                && no_secrets(value)
        }),
        serde_json::Value::Array(values) => values.iter().all(no_secrets),
        _ => true,
    }
}
fn scoped_root(root: &str, scheme: &str) -> bool {
    root.strip_prefix(scheme).is_some_and(|tail| {
        let Some((bucket, prefix)) = tail.split_once('/') else {
            return false;
        };
        !bucket.is_empty()
            && bucket
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
            && prefix.split('/').all(|p| {
                !p.is_empty()
                    && p != "."
                    && p != ".."
                    && p.bytes()
                        .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
            })
    })
}

pub fn load() -> Scope {
    let path = std::env::var_os("GRV_RELEASE_VALIDATION_CONFIG")
        .expect("explicit GRV_RELEASE_VALIDATION_CONFIG is required");
    read(std::path::Path::new(&path)).expect("invalid private release-validation config")
}
pub fn read(path: &std::path::Path) -> Result<Scope, &'static str> {
    let before = std::fs::symlink_metadata(path).map_err(|_| "cannot inspect config")?;
    if !before.is_file()
        || before.permissions().mode() & 0o7777 != 0o600
        || before.uid() != unsafe { libc::geteuid() }
    {
        return Err("config must be an owned regular nonsymlink file with mode 0600");
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|_| "cannot open config")?;
    let opened = file.metadata().map_err(|_| "cannot inspect open config")?;
    if before.dev() != opened.dev() || before.ino() != opened.ino() {
        return Err("config changed while opening");
    }
    let mut bytes = Vec::new();
    file.take(65537)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read config")?;
    if bytes.len() > 65536 {
        return Err("config exceeds size bound");
    }
    let scope: Scope = serde_json::from_slice(&bytes).map_err(|_| "invalid config schema")?;
    for field in [
        &scope.sf.org,
        &scope.sf.org_id,
        &scope.gcs.root,
        &scope.gcs.account,
        &scope.gcs.project,
        &scope.s3.root,
        &scope.s3.profile,
        &scope.s3.region,
    ] {
        if field.is_empty() || field.trim() != field {
            return Err("empty or untrimmed scope");
        }
    }
    if scope.sf.org_id.len() != 18
        || !scope.sf.org_id.bytes().all(|c| c.is_ascii_alphanumeric())
        || !scoped_root(&scope.gcs.root, "gs://")
        || !scoped_root(&scope.s3.root, "s3://")
        || !no_secrets(&serde_json::Value::Object(scope.archive.clone()))
    {
        return Err("invalid coordinates or forbidden credentials");
    }
    Ok(scope)
}
