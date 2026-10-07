//! Private profile credentials. No credential values, helper output or provider
//! errors are included in diagnostics. Credential export refuses enabled CLI
//! history recording before obtaining any private credentials.
use chrono::{DateTime, Utc};
use object_store::{aws::AwsCredential, client::CredentialProvider};
use serde::Deserialize;
use std::{
    fmt,
    io::Read,
    os::unix::process::CommandExt,
    process::{Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

pub(super) fn failure() -> object_store::Error {
    object_store::Error::Generic {
        store: "GRV",
        source: "cloud profile authentication failed".into(),
    }
}
pub(super) fn helper(arguments: &[&str]) -> Result<Vec<u8>, object_store::Error> {
    run_helper(arguments, false)
}
fn run_helper(
    arguments: &[&str],
    allow_missing_config: bool,
) -> Result<Vec<u8>, object_store::Error> {
    run_program("aws", arguments, allow_missing_config)
}
fn run_program(
    program: &str,
    arguments: &[&str],
    allow_missing_config: bool,
) -> Result<Vec<u8>, object_store::Error> {
    let mut child = Command::new(program)
        .args(arguments)
        .env("AWS_PAGER", "")
        .env("AWS_CLI_AUTO_PROMPT", "off")
        .env("CLOUDSDK_CORE_DISABLE_FILE_LOGGING", "true")
        .env("CLOUDSDK_CORE_LOG_HTTP", "false")
        .env("CLOUDSDK_CORE_DISABLE_PROMPTS", "true")
        .env("CLOUDSDK_CORE_VERBOSITY", "error")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .process_group(0)
        .spawn()
        .map_err(|_| failure())?;
    let process_group = child.id() as i32;
    let stopped = Arc::new(AtomicBool::new(false));
    let stopping = stopped.clone();
    let mut output = child.stdout.take().ok_or_else(failure)?;
    use std::os::fd::AsRawFd;
    let flags = unsafe { libc::fcntl(output.as_raw_fd(), libc::F_GETFL) };
    if flags < 0
        || unsafe { libc::fcntl(output.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0
    {
        unsafe {
            libc::kill(-process_group, libc::SIGKILL);
        }
        let _ = child.wait();
        return Err(failure());
    }
    let reader = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let mut buffer = [0u8; 4096];
        loop {
            match output.read(&mut buffer) {
                Ok(0) => break,
                Ok(n) if bytes.len() + n <= 64 * 1024 => bytes.extend_from_slice(&buffer[..n]),
                Ok(_) => return Err(failure()),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if stopping.load(Ordering::Acquire) {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(_) => return Err(failure()),
            }
        }
        Ok(bytes)
    });
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Some(status),
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(5)),
            _ => break None,
        }
    };
    // Remaining pipe holders cannot prolong the bounded reader. This helper
    // doesn't provide adapter stopped-work attestation or execute source jobs.
    unsafe {
        libc::kill(-process_group, libc::SIGKILL);
    }
    let _ = child.wait();
    stopped.store(true, Ordering::Release);
    let bytes = reader.join().map_err(|_| failure())??;
    if !status.is_some_and(|s| {
        s.success() || (allow_missing_config && s.code() == Some(1) && bytes.is_empty())
    }) {
        return Err(failure());
    }
    Ok(bytes)
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct Export {
    version: u8,
    access_key_id: String,
    secret_access_key: String,
    #[serde(default)]
    session_token: Option<String>,
    #[serde(default)]
    expiration: Option<DateTime<Utc>>,
}
type Cached = (Arc<AwsCredential>, Option<DateTime<Utc>>);
pub(super) struct Gcloud {
    pub account: String,
    pub project: Option<String>,
}
impl fmt::Debug for Gcloud {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Gcloud([private])")
    }
}
#[async_trait::async_trait]
impl CredentialProvider for Gcloud {
    type Credential = object_store::gcp::GcpCredential;
    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let account = self.account.clone();
        let project = self.project.clone();
        tokio::task::spawn_blocking(move || {
            let mut arguments = vec![
                "auth",
                "print-access-token",
                "--account",
                &account,
                "--quiet",
            ];
            if let Some(project) = &project {
                arguments.extend(["--project", project]);
            }
            let bytes = run_program("gcloud", &arguments, false)?;
            let bearer = std::str::from_utf8(&bytes).map_err(|_| failure())?.trim();
            if bearer.is_empty()
                || bearer.len() > 16 * 1024
                || !bearer
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-._~+/=".contains(&b))
            {
                return Err(failure());
            }
            Ok(Arc::new(object_store::gcp::GcpCredential {
                bearer: bearer.into(),
            }))
        })
        .await
        .map_err(|_| failure())?
    }
}
pub(super) struct Profile {
    name: String,
    cached: Arc<Mutex<Option<Cached>>>,
}
impl fmt::Debug for Profile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Profile([private])")
    }
}
impl Profile {
    pub fn new(name: String) -> Self {
        Self {
            name,
            cached: Arc::new(Mutex::new(None)),
        }
    }
    pub fn region(&self) -> Result<Option<String>, object_store::Error> {
        let bytes = run_helper(
            &["configure", "get", "region", "--profile", &self.name],
            true,
        )?;
        let value = std::str::from_utf8(&bytes).map_err(|_| failure())?.trim();
        if value.is_empty() {
            Ok(None)
        } else if value.len() <= 128
            && value
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            Ok(Some(value.into()))
        } else {
            Err(failure())
        }
    }
}
#[async_trait::async_trait]
impl CredentialProvider for Profile {
    type Credential = AwsCredential;
    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        let name = self.name.clone();
        let cache = self.cached.clone();
        tokio::task::spawn_blocking(move || {
            let mut cache = cache.lock().map_err(|_| failure())?;
            if let Some((credentials, expiration)) = cache.as_ref()
                && expiration
                    .is_none_or(|expiry| expiry > Utc::now() + chrono::Duration::seconds(300))
            {
                return Ok(credentials.clone());
            }
            let history = run_helper(
                &["configure", "get", "cli_history", "--profile", &name],
                true,
            )?;
            if !matches!(
                std::str::from_utf8(&history).map_err(|_| failure())?.trim(),
                "" | "disabled"
            ) {
                return Err(failure());
            }
            let bytes = helper(&[
                "configure",
                "export-credentials",
                "--profile",
                &name,
                "--format",
                "process",
            ])?;
            let export: Export = serde_json::from_slice(&bytes).map_err(|_| failure())?;
            if export.version != 1
                || export.access_key_id.is_empty()
                || export.secret_access_key.is_empty()
                || export.expiration.is_some_and(|expiry| expiry <= Utc::now())
            {
                return Err(failure());
            }
            let credentials = Arc::new(AwsCredential {
                key_id: export.access_key_id,
                secret_key: export.secret_access_key,
                token: export.session_token,
            });
            *cache = Some((credentials.clone(), export.expiration));
            Ok(credentials)
        })
        .await
        .map_err(|_| failure())?
    }
}
