//! Adapter-private, credential-free named reader configuration. Loading this
//! file performs no authentication, credential lookup or network operation.
use serde::Deserialize;
use std::{
    collections::BTreeSet,
    fs::{self, OpenOptions},
    io::{self, Read},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
};
const LIMIT: u64 = 64 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3Reader {
    pub scope: String,
    pub profile: String,
    pub region: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
    config_version: u16,
    readers: Vec<S3Reader>,
}
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
fn valid_scope(scope: &str) -> bool {
    let Some(authority) = scope.strip_prefix("s3://") else {
        return false;
    };
    let (bucket, prefix) = authority.split_once('/').unwrap_or((authority, ""));
    (3..=63).contains(&bucket.len())
        && !bucket.starts_with(['-', '.'])
        && !bucket.ends_with(['-', '.'])
        && bucket
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'-' | b'.'))
        && !scope.contains(['?', '#', '*', '[', ']', '\\'])
        && !scope.chars().any(char::is_whitespace)
        && !scope.chars().any(char::is_control)
        && (prefix.is_empty()
            || prefix
                .split('/')
                .all(|s| !s.is_empty() && s != "." && s != ".."))
        && !scope.ends_with('/')
}
/// DuckDB HTTPFS 1.5.6 accepts an opaque S3 filename, not a percent-encoded
/// URI. Decode the canonical GRV key exactly once; callers must use URL
/// compatibility mode and an exact file capability, never glob expansion.
/// Protocol identities and hashes continue to use the original canonical URI.
pub fn sql_filename(canonical: &str) -> io::Result<String> {
    if !valid_scope(canonical) || canonical.len() > 8192 {
        return Err(invalid("invalid canonical S3 coordinate"));
    }
    let authority = canonical.strip_prefix("s3://").unwrap();
    let (bucket, key) = authority.split_once('/').unwrap_or((authority, ""));
    let hex = |byte: u8| match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    };
    let mut decoded = Vec::with_capacity(key.len());
    let bytes = key.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let byte = if bytes[index] == b'%' {
            let high = bytes
                .get(index + 1)
                .and_then(|b| hex(*b))
                .ok_or_else(|| invalid("invalid S3 percent escape"))?;
            let low = bytes
                .get(index + 2)
                .and_then(|b| hex(*b))
                .ok_or_else(|| invalid("invalid S3 percent escape"))?;
            index += 3;
            let value = high * 16 + low;
            if value == b'/' {
                return Err(invalid("encoded S3 separator is forbidden"));
            }
            value
        } else {
            let value = bytes[index];
            index += 1;
            value
        };
        if byte < 32 || byte == 127 || byte == b'\\' {
            return Err(invalid("invalid S3 key byte"));
        }
        decoded.push(byte);
    }
    let key_decoded = String::from_utf8(decoded).map_err(|_| invalid("S3 key is not UTF-8"))?;
    if !key_decoded.is_empty()
        && key_decoded
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(invalid("invalid S3 path segment"));
    }
    let mut encoded = String::new();
    for byte in key_decoded.bytes() {
        if byte == b'/' || ((33..=126).contains(&byte) && !b"\"#%<>?`{}[]*\\".contains(&byte)) {
            encoded.push(byte as char);
        } else {
            use std::fmt::Write;
            write!(&mut encoded, "%{byte:02X}").unwrap();
        }
    }
    if encoded != key {
        return Err(invalid("noncanonical S3 URI escaping"));
    }
    Ok(if key_decoded.is_empty() {
        format!("s3://{bucket}")
    } else {
        format!("s3://{bucket}/{key_decoded}")
    })
}
/// Read the fixed engine-adjacent file. Only profile names, region and scopes
/// are allowed; secret values and endpoints cannot enter this closed record.
/// Missing configuration is an error for an explicitly requested S3 lifecycle.
pub fn load_reader(engine: &Path, root: &str) -> io::Result<S3Reader> {
    if !valid_scope(root) {
        return Err(invalid("S3 reader requires a canonical S3 root"));
    }
    let engine = crate::lock::canonical_engine_path(engine)?;
    let mut path = engine.as_os_str().to_os_string();
    path.push(".grv-s3-read.json");
    let path = std::path::PathBuf::from(path);
    crate::lock::check_ancestors(&path)?;
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&path)?;
    let before = file.metadata()?;
    if !before.is_file()
        || before.nlink() != 1
        || before.uid() != unsafe { libc::geteuid() }
        || before.mode() & 0o077 != 0
        || before.len() > LIMIT
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "S3 reader configuration must be a private regular file",
        ));
    }
    let mut bytes = Vec::with_capacity(before.len() as usize);
    (&mut file).take(LIMIT + 1).read_to_end(&mut bytes)?;
    let after = file.metadata()?;
    let named = fs::symlink_metadata(&path)?;
    let identity = |m: &fs::Metadata| {
        (
            m.dev(),
            m.ino(),
            m.len(),
            m.mtime(),
            m.mtime_nsec(),
            m.mode(),
            m.uid(),
            m.nlink(),
        )
    };
    if bytes.len() as u64 != before.len()
        || identity(&before) != identity(&after)
        || identity(&after) != identity(&named)
        || !named.is_file()
    {
        return Err(invalid("S3 reader configuration changed while reading"));
    }
    let value = grv_adapter_wire::json::parse(&bytes)
        .map_err(|_| invalid("invalid S3 reader configuration JSON"))?;
    let config: Configuration = serde_json::from_value(value)
        .map_err(|_| invalid("invalid S3 reader configuration fields"))?;
    if config.config_version != 1 || config.readers.is_empty() || config.readers.len() > 64 {
        return Err(invalid(
            "unsupported S3 reader configuration version or size",
        ));
    }
    let mut scopes = BTreeSet::new();
    for reader in &config.readers {
        if !valid_scope(&reader.scope)
            || !scopes.insert(&reader.scope)
            || reader.profile.is_empty()
            || reader.profile.len() > 1024
            || reader.profile.chars().any(char::is_control)
            || reader.region.is_empty()
            || reader.region.len() > 128
            || !reader
                .region
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return Err(invalid("invalid S3 reader scope, profile or region"));
        }
    }
    config
        .readers
        .into_iter()
        .filter(|reader| {
            root == reader.scope
                || root
                    .strip_prefix(&reader.scope)
                    .is_some_and(|suffix| suffix.starts_with('/'))
        })
        .max_by_key(|reader| reader.scope.len())
        .ok_or_else(|| invalid("no independently configured S3 reader matches the requested root"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    fn fixture() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
        let temp = tempfile::tempdir().unwrap();
        let engine = temp.path().join("engine.duckdb");
        let config = temp.path().join("engine.duckdb.grv-s3-read.json");
        (temp, engine, config)
    }
    fn write(path: &Path, bytes: &[u8]) {
        fs::write(path, bytes).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    }
    #[test]
    fn sql_filename_decodes_once_without_percent_aliases_or_glob_authority() {
        assert_eq!(
            sql_filename(
                "s3://test-bucket/space%20key/literal%25/%2A%5B%5D/version=1/owner@example"
            )
            .unwrap(),
            "s3://test-bucket/space key/literal%/*[]/version=1/owner@example"
        );
        assert_ne!(
            sql_filename("s3://test-bucket/percent%2520").unwrap(),
            sql_filename("s3://test-bucket/percent%20").unwrap()
        );
        for value in [
            "s3://user@test-bucket/a",
            "s3://test-bucket/%2F",
            "s3://test-bucket/%2E%2E",
            "s3://test-bucket/%61",
            "s3://test-bucket/%zz",
            "s3://test-bucket/a?auth=bad",
        ] {
            assert!(sql_filename(value).is_err());
        }
    }
    #[test]
    fn offline_configuration_selects_longest_complete_scope_without_creating_engine() {
        let (_temp, engine, path) = fixture();
        write(&path, br#"{"config_version":1,"readers":[{"scope":"s3://test-bucket/grv-dev","profile":"broad","region":"eu-west-1"},{"scope":"s3://test-bucket/grv-dev/subset","profile":"specific","region":"eu-west-1"}]}"#);
        assert_eq!(
            load_reader(&engine, "s3://test-bucket/grv-dev/subset/run")
                .unwrap()
                .profile,
            "specific"
        );
        assert!(load_reader(&engine, "s3://test-bucket/grv-dev-foreign").is_err());
        assert!(load_reader(&engine, "gs://test-bucket/grv-dev").is_err());
        assert!(!engine.exists());
    }
    #[test]
    fn closed_private_configuration_refuses_secrets_duplicates_aliases_and_untrusted_files() {
        let (_temp, engine, path) = fixture();
        let valid = br#"{"config_version":1,"readers":[{"scope":"s3://test-bucket/grv-dev","profile":"test","region":"eu-west-1"}]}"#;
        for bytes in [
            br#"{"config_version":1,"config_version":1,"readers":[]}"#.as_slice(),
            br#"{"config_version":2,"readers":[]}"#,
            br#"{"config_version":1,"readers":[{"scope":"s3://test-bucket/a","profile":"test","region":"eu-west-1","secret":"credential-canary"}]}"#,
            br#"{"config_version":1,"readers":[{"scope":"s3://test-bucket/a/../grv-dev","profile":"test","region":"eu-west-1"}]}"#,
            br#"{"config_version":1,"readers":[{"scope":"s3://test-bucket/grv-dev","profile":"test","region":"eu-west-1","endpoint":"https://foreign"}]}"#,
        ] {
            write(&path, bytes);
            assert!(load_reader(&engine, "s3://test-bucket/grv-dev").is_err());
        }
        write(&path, valid);
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(load_reader(&engine, "s3://test-bucket/grv-dev").is_err());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let alias = path.with_extension("alias");
        fs::hard_link(&path, &alias).unwrap();
        assert!(load_reader(&engine, "s3://test-bucket/grv-dev").is_err());
        fs::remove_file(&alias).unwrap();
        fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink(&alias, &path).unwrap();
        assert!(load_reader(&engine, "s3://test-bucket/grv-dev").is_err());
        assert!(!engine.exists());
    }
}
