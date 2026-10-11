//! Adapter-private, credential-free GCS reader selectors. This module performs
//! no credential lookup, authentication, environment lookup or network operation.
use serde::Deserialize;
use std::{
    collections::BTreeSet,
    fs::{self, OpenOptions},
    io::{self, Read},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
};

const LIMIT: u64 = 64 * 1024;
const URI_LIMIT: usize = 8192;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcsReader {
    pub scope: String,
    pub provider: String,
    pub account: String,
    pub project: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
    config_version: u16,
    readers: Vec<GcsReader>,
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn valid_bucket(bucket: &str) -> bool {
    (3..=222).contains(&bucket.len())
        && bucket.split('.').all(|part| {
            !part.is_empty()
                && part.len() <= 63
                && !part.starts_with('-')
                && !part.ends_with('-')
                && part
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        })
}

fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Decode exactly once and re-encode to prove the spelling is canonical. GRV
/// URL path segments admit printable ASCII except the URL-sensitive bytes below;
/// glob bytes are also encoded, as in exact object coordinates. Legacy gs roots
/// with raw glob bytes are deliberately not accepted as exact reader scopes.
fn coordinate(canonical: &str) -> io::Result<(&str, String)> {
    if canonical.len() > URI_LIMIT {
        return Err(invalid("GCS coordinate exceeds limit"));
    }
    let authority = canonical
        .strip_prefix("gs://")
        .ok_or_else(|| invalid("GCS reader requires a canonical gs coordinate"))?;
    let (bucket, key) = authority.split_once('/').unwrap_or((authority, ""));
    if !valid_bucket(bucket) || canonical.ends_with('/') {
        return Err(invalid("invalid GCS bucket or path"));
    }
    let mut decoded = Vec::with_capacity(key.len());
    let bytes = key.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let byte = if bytes[index] == b'%' {
            let high = bytes
                .get(index + 1)
                .and_then(|b| hex(*b))
                .ok_or_else(|| invalid("invalid GCS percent escape"))?;
            let low = bytes
                .get(index + 2)
                .and_then(|b| hex(*b))
                .ok_or_else(|| invalid("invalid GCS percent escape"))?;
            index += 3;
            let value = high * 16 + low;
            if value == b'/' {
                return Err(invalid("encoded GCS separator is forbidden"));
            }
            value
        } else {
            let value = bytes[index];
            index += 1;
            value
        };
        if byte < 32 || byte == 127 || byte == b'\\' {
            return Err(invalid("invalid GCS key byte"));
        }
        decoded.push(byte);
    }
    let decoded = String::from_utf8(decoded).map_err(|_| invalid("GCS key is not UTF-8"))?;
    if decoded.chars().any(char::is_control)
        || (!decoded.is_empty()
            && decoded
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == ".."))
    {
        return Err(invalid("invalid GCS path segment"));
    }
    let mut encoded = String::new();
    for byte in decoded.bytes() {
        if byte == b'/' || ((33..=126).contains(&byte) && !b"\"#%<>?`{}[]*\\".contains(&byte)) {
            encoded.push(byte as char);
        } else {
            use std::fmt::Write;
            write!(&mut encoded, "%{byte:02X}").unwrap();
        }
    }
    if encoded != key {
        return Err(invalid("noncanonical GCS URI escaping"));
    }
    Ok((bucket, decoded))
}

/// Produce a native HTTPFS HTTPS filename, not a gs URI, signed URL or JSON API
/// endpoint. Only unreserved object bytes and path separators remain literal.
/// Percent-looking keys are never decoded twice. The caller must still enforce
/// an exact-file capability and disable production glob expansion.
pub fn sql_filename(canonical: &str) -> io::Result<String> {
    let (bucket, key) = coordinate(canonical)?;
    let mut filename = format!("https://storage.googleapis.com/{bucket}");
    if !key.is_empty() {
        filename.push('/');
        for byte in key.bytes() {
            if byte.is_ascii_alphanumeric() || b"-._~/".contains(&byte) {
                filename.push(byte as char);
            } else {
                use std::fmt::Write;
                write!(&mut filename, "%{byte:02X}").unwrap();
            }
        }
    }
    if filename.len() > URI_LIMIT {
        return Err(invalid("GCS HTTPS filename exceeds limit"));
    }
    Ok(filename)
}

fn valid_account(account: &str) -> bool {
    if account.len() > 320 {
        return false;
    }
    let Some((local, domain)) = account.split_once('@') else {
        return false;
    };
    !local.is_empty()
        && local.len() <= 64
        && !local.starts_with('.')
        && !local.ends_with('.')
        && !local.contains("..")
        && local
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._+-".contains(&b))
        && domain.len() <= 253
        && domain.contains('.')
        && domain.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
}

fn valid_project(project: &str) -> bool {
    (6..=30).contains(&project.len())
        && project.as_bytes()[0].is_ascii_lowercase()
        && !project.ends_with('-')
        && project
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

fn private_file(metadata: &fs::Metadata, uid: u32) -> bool {
    metadata.is_file()
        && metadata.nlink() == 1
        && metadata.uid() == uid
        && metadata.mode() & 0o077 == 0
        && metadata.len() <= LIMIT
}

/// Load the fixed engine-adjacent .grv-gcs-read.json record. All selectors are
/// explicit: there is no ambient account/project or default-provider fallback.
pub fn load_reader(engine: &Path, root: &str) -> io::Result<GcsReader> {
    coordinate(root)?;
    let engine = crate::lock::canonical_engine_path(engine)?;
    let mut path = engine.as_os_str().to_os_string();
    path.push(".grv-gcs-read.json");
    let path = std::path::PathBuf::from(path);
    crate::lock::check_ancestors(&path)?;
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(&path)?;
    let before = file.metadata()?;
    if !private_file(&before, unsafe { libc::geteuid() }) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "GCS reader configuration must be a private regular file",
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
            m.ctime(),
            m.ctime_nsec(),
            m.mode(),
            m.uid(),
            m.gid(),
            m.nlink(),
        )
    };
    if bytes.len() as u64 != before.len()
        || identity(&before) != identity(&after)
        || identity(&after) != identity(&named)
        || !named.is_file()
    {
        return Err(invalid("GCS reader configuration changed while reading"));
    }
    let value = grv_adapter_wire::json::parse(&bytes)
        .map_err(|_| invalid("invalid GCS reader configuration JSON"))?;
    let config: Configuration = serde_json::from_value(value)
        .map_err(|_| invalid("invalid GCS reader configuration fields"))?;
    if config.config_version != 1 || config.readers.is_empty() || config.readers.len() > 64 {
        return Err(invalid(
            "unsupported GCS reader configuration version or size",
        ));
    }
    let mut scopes = BTreeSet::new();
    for reader in &config.readers {
        if coordinate(&reader.scope).is_err()
            || !scopes.insert(&reader.scope)
            || reader.provider != "gcloud"
            || !valid_account(&reader.account)
            || !valid_project(&reader.project)
        {
            return Err(invalid("invalid GCS reader scope or selectors"));
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
        .ok_or_else(|| invalid("no independently configured GCS reader matches the requested root"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn fixture() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
        let temp = tempfile::tempdir().unwrap();
        let engine = temp.path().join("engine.duckdb");
        let config = temp.path().join("engine.duckdb.grv-gcs-read.json");
        (temp, engine, config)
    }
    fn write(path: &Path, bytes: &[u8]) {
        fs::write(path, bytes).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    }
    fn reader(scope: &str) -> serde_json::Value {
        serde_json::json!({"scope":scope,"provider":"gcloud","account":"reader@example.com","project":"test-project"})
    }
    fn config(readers: Vec<serde_json::Value>) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({"config_version":1,"readers":readers})).unwrap()
    }

    #[test]
    fn filename_maps_special_bytes_once_to_exact_https_paths() {
        assert_eq!(
            sql_filename("gs://test-bucket").unwrap(),
            "https://storage.googleapis.com/test-bucket"
        );
        assert_eq!(
            sql_filename(
                "gs://test-bucket/space%20key/literal%25/%2A%5B%5D/version=1/owner@example"
            )
            .unwrap(),
            "https://storage.googleapis.com/test-bucket/space%20key/literal%25/%2A%5B%5D/version%3D1/owner%40example"
        );
        for (key, expected) in [
            ("percent%2520", "percent%2520"),
            ("percent%20", "percent%20"),
            ("%22%23%3C%3E%3F%60%7B%7D", "%22%23%3C%3E%3F%60%7B%7D"),
            (
                "a+b:c;d,e!f$g&h(i)'j",
                "a%2Bb%3Ac%3Bd%2Ce%21f%24g%26h%28i%29%27j",
            ),
            ("caf%C3%A9", "caf%C3%A9"),
            ("a%252Fb", "a%252Fb"),
        ] {
            assert_eq!(
                sql_filename(&format!("gs://test-bucket/{key}")).unwrap(),
                format!("https://storage.googleapis.com/test-bucket/{expected}")
            );
        }
        assert_ne!(
            sql_filename("gs://test-bucket/percent%2520").unwrap(),
            sql_filename("gs://test-bucket/percent%20").unwrap()
        );
    }

    #[test]
    fn coordinates_refuse_aliases_and_noncanonical_or_unsafe_bytes() {
        for uri in [
            "s3://test-bucket/a",
            "https://storage.googleapis.com/test-bucket/a",
            "GS://test-bucket/a",
            "gs://Test-bucket/a",
            "gs://user@test-bucket/a",
            "gs://test-bucket:443/a",
            "gs://test-bucket/a?auth=bad",
            "gs://test-bucket/a#bad",
            "gs://test-bucket/",
            "gs://test-bucket/a//b",
            "gs://test-bucket/./a",
            "gs://test-bucket/a/..",
            "gs://test-bucket/%2E%2E",
            "gs://test-bucket/%2F",
            "gs://test-bucket/%2f",
            "gs://test-bucket/%61",
            "gs://test-bucket/%",
            "gs://test-bucket/%zz",
            "gs://test-bucket/%00",
            "gs://test-bucket/%7F",
            "gs://test-bucket/%5C",
            "gs://test-bucket/%C2%85",
            "gs://test-bucket/%FF",
            "gs://test-bucket/%C0%AF",
            "gs://test-bucket/raw space",
            "gs://test-bucket/é",
            "gs://test-bucket/*",
            "gs://test-bucket/[a]",
            "gs://test-bucket/a\\b",
            "gs://a..b/x",
            "gs://-abc/x",
            "gs://abc-/x",
            "gs://ab/x",
            "gs://abc.-def/x",
        ] {
            assert!(sql_filename(uri).is_err(), "{uri}");
        }
        assert!(valid_bucket(&format!(
            "{}.{}.{}.{}",
            "a".repeat(63),
            "b".repeat(63),
            "c".repeat(63),
            "d".repeat(30)
        )));
        assert!(!valid_bucket(&format!("{}.abc", "a".repeat(64))));
        assert!(!valid_bucket(&"a".repeat(223)));
        assert!(sql_filename(&format!("gs://test-bucket/{}", "a".repeat(8192))).is_err());
        // HTTPS's stricter escaping can exceed the bound even for a short URI.
        assert!(sql_filename(&format!("gs://test-bucket/{}", "=".repeat(3000))).is_err());
    }

    #[test]
    fn configuration_is_offline_explicit_and_selects_longest_component_scope() {
        let (_temp, engine, path) = fixture();
        assert!(load_reader(&engine, "gs://test-bucket/dev").is_err());
        let mut specific = reader("gs://test-bucket/dev/subset");
        specific["account"] = "specific@example.com".into();
        write(
            &path,
            &config(vec![reader("gs://test-bucket/dev"), specific]),
        );
        assert_eq!(
            load_reader(&engine, "gs://test-bucket/dev/subset/run")
                .unwrap()
                .account,
            "specific@example.com"
        );
        assert_eq!(
            load_reader(&engine, "gs://test-bucket/dev/subset-foreign")
                .unwrap()
                .account,
            "reader@example.com"
        );
        assert!(load_reader(&engine, "gs://test-bucket/dev-foreign").is_err());
        assert!(load_reader(&engine, "s3://test-bucket/dev").is_err());
        assert!(load_reader(&engine, "gs://test-bucket/dev/%61").is_err());
        assert!(!engine.exists());
    }

    #[test]
    fn closed_configuration_refuses_duplicates_secrets_aliases_and_defaults() {
        let (_temp, engine, path) = fixture();
        let scope = "gs://test-bucket/dev";
        for bytes in [
            br#"{"config_version":1,"config_version":1,"readers":[]}"#.as_slice(),
            br#"{"config_version":1,"readers":[{"scope":"gs://test-bucket/dev","scope":"gs://test-bucket/dev","provider":"gcloud","account":"reader@example.com","project":"test-project"}]}"#,
            br#"{"config_version":2,"readers":[]}"#,
            br#"{"config_version":1}"#,
        ] {
            write(&path, bytes);
            assert!(load_reader(&engine, scope).is_err());
        }
        for field in [
            "secret",
            "token",
            "access_token",
            "credentials",
            "service_account",
            "endpoint",
            "expires_at",
        ] {
            let mut entry = reader(scope);
            entry[field] = "credential-canary".into();
            write(&path, &config(vec![entry]));
            let error = load_reader(&engine, scope).unwrap_err();
            assert!(!error.to_string().contains("credential-canary"));
            let mut top = serde_json::json!({"config_version":1,"readers":[reader(scope)]});
            top[field] = "credential-canary".into();
            write(&path, &serde_json::to_vec(&top).unwrap());
            assert!(load_reader(&engine, scope).is_err());
        }
        for field in ["scope", "provider", "account", "project"] {
            let mut entry = reader(scope);
            entry.as_object_mut().unwrap().remove(field);
            write(&path, &config(vec![entry]));
            assert!(load_reader(&engine, scope).is_err());
        }
        for (field, value) in [
            ("scope", "gs://test-bucket/a/../dev"),
            ("scope", "gs://test-bucket/%64ev"),
            ("scope", "s3://test-bucket/dev"),
            ("provider", "GCloud"),
            ("provider", "default"),
            ("account", ""),
            ("account", "default"),
            ("account", "--account=x@example.com"),
            ("account", "a@example.com\n"),
            ("account", "é@example.com"),
            ("account", "a@b@example.com"),
            ("project", ""),
            ("project", "Project"),
            ("project", "--project=x"),
            ("project", "test project"),
            ("project", "test-project\r"),
        ] {
            let mut entry = reader(scope);
            entry[field] = value.into();
            write(&path, &config(vec![entry]));
            assert!(load_reader(&engine, scope).is_err(), "{field}: {value:?}");
        }
        for entries in [
            vec![],
            vec![reader(scope), reader(scope)],
            (0..65)
                .map(|i| reader(&format!("gs://test-bucket/{i}")))
                .collect(),
        ] {
            write(&path, &config(entries));
            assert!(load_reader(&engine, scope).is_err());
        }
        assert!(valid_account("worker@test-project.iam.gserviceaccount.com"));
        assert!(!valid_account(&format!("{}@example.com", "a".repeat(65))));
        assert!(!valid_project(&"a".repeat(31)));
    }

    #[test]
    fn fifo_configuration_refused_without_blocking() {
        let (_temp, engine, path) = fixture();
        let named = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(named.as_ptr(), 0o600) }, 0);
        let start = std::time::Instant::now();
        assert!(load_reader(&engine, "gs://test-bucket").is_err());
        assert!(start.elapsed() < std::time::Duration::from_secs(1));
    }
    #[test]
    fn protected_configuration_rejects_wrong_owner_mode_links_and_size() {
        let (_temp, engine, path) = fixture();
        let scope = "gs://test-bucket/dev";
        write(&path, &config(vec![reader(scope)]));
        let metadata = fs::metadata(&path).unwrap();
        assert!(private_file(&metadata, unsafe { libc::geteuid() }));
        // Exercise the actual ownership predicate without requiring root/chown.
        assert!(!private_file(&metadata, metadata.uid().wrapping_add(1)));
        fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
        assert_eq!(
            load_reader(&engine, scope).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let alias = path.with_extension("alias");
        fs::hard_link(&path, &alias).unwrap();
        assert!(load_reader(&engine, scope).is_err());
        fs::remove_file(&alias).unwrap();
        fs::rename(&path, &alias).unwrap();
        std::os::unix::fs::symlink(&alias, &path).unwrap();
        assert!(load_reader(&engine, scope).is_err());
        fs::remove_file(&path).unwrap();
        write(&path, &vec![b' '; LIMIT as usize + 1]);
        assert!(load_reader(&engine, scope).is_err());
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();
        assert!(load_reader(&engine, scope).is_err());
        assert!(!engine.exists());
    }
}
