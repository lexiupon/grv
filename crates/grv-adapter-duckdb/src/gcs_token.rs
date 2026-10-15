//! Explicit gcloud OAuth acquisition with a bounded, private helper channel.
//! Tokens/expiry come from the same config-helper response. No ambient principal,
//! serialized token, token argument or raw helper error crosses this boundary.
use chrono::{DateTime, Utc};
use std::{
    fmt,
    io::{self, Read},
    os::unix::process::CommandExt,
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use zeroize::Zeroizing;

const OUTPUT_LIMIT: usize = 64 * 1024;
const HELPER_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_EXECUTION: Duration = Duration::from_secs(300);
const EXPIRY_MARGIN: Duration = Duration::from_secs(120);

fn failure() -> io::Error {
    io::Error::other("explicit GCS OAuth acquisition or lifetime validation failed")
}

/// Private, deliberately not Serialize/Clone and with redacted Debug.
pub struct AccessToken {
    bearer: Zeroizing<String>,
    deadline: Instant,
}
impl fmt::Debug for AccessToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AccessToken([private])")
    }
}
impl AccessToken {
    /// Trusted setup only; callers must not log, persist or pass through SQL errors.
    pub fn bearer(&self) -> io::Result<&str> {
        self.ensure_live()?;
        Ok(&self.bearer)
    }
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
    #[cfg(all(test, feature = "native"))]
    pub(crate) fn synthetic_for_test(execution: Duration) -> Self {
        Self {
            bearer: Zeroizing::new("synthetic-not-a-credential".into()),
            deadline: Instant::now() + execution,
        }
    }
    /// Test-only: a distinct synthetic canary per owner. Non-Clone and
    /// non-Serialize in production are unchanged; the bearer is zeroized on
    /// drop and the Debug rendering stays redacted.
    #[cfg(all(test, feature = "native"))]
    pub(crate) fn synthetic_with_canary_for_test(execution: Duration, canary: &str) -> Self {
        Self {
            bearer: Zeroizing::new(canary.into()),
            deadline: Instant::now() + execution,
        }
    }
    pub fn ensure_live(&self) -> io::Result<()> {
        if Instant::now() < self.deadline {
            Ok(())
        } else {
            Err(failure())
        }
    }
}

trait Helper {
    fn output(&self, arguments: &[&str]) -> io::Result<Zeroizing<Vec<u8>>>;
}
struct GcloudHelper;
impl Helper for GcloudHelper {
    fn output(&self, arguments: &[&str]) -> io::Result<Zeroizing<Vec<u8>>> {
        bounded_output(arguments)
    }
}

/// Acquire only after pure reader/file admission. No process-global config changes.
pub fn acquire(account: &str, project: &str, execution: Duration) -> io::Result<AccessToken> {
    if std::env::vars_os().any(|(key, value)| {
        key.to_string_lossy()
            .to_ascii_uppercase()
            .starts_with("CLOUDSDK_AUTH_")
            && !value.is_empty()
    }) {
        return Err(failure());
    }
    acquire_with(
        &GcloudHelper,
        account,
        project,
        execution,
        Utc::now,
        Instant::now,
    )
}

fn acquire_with(
    helper: &impl Helper,
    account: &str,
    project: &str,
    execution: Duration,
    wall: impl Fn() -> DateTime<Utc>,
    monotonic: impl Fn() -> Instant,
) -> io::Result<AccessToken> {
    if account.is_empty()
        || account.len() > 320
        || !account.is_ascii()
        || !account.contains('@')
        || account
            .bytes()
            .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
        || project.is_empty()
        || project.len() > 128
        || !project
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        || execution.is_zero()
        || execution > MAX_EXECUTION
    {
        return Err(failure());
    }
    let selectors = ["--account", account, "--project", project, "--quiet"];
    let mut check = vec![
        "config",
        "list",
        "--all",
        "--format=json(auth.impersonate_service_account,auth.credential_file_override,auth.access_token_file)",
    ];
    check.extend(selectors);
    let bytes = helper.output(&check)?;
    #[derive(serde::Deserialize)]
    struct AuthConfiguration {
        auth: Auth,
    }
    let config: AuthConfiguration = serde_json::from_slice(&bytes).map_err(|_| failure())?;
    config.auth.validate()?;
    drop(bytes);
    let mut args = vec![
        "config",
        "config-helper",
        "--force-auth-refresh",
        "--format=json(credential.access_token,credential.token_expiry,configuration.properties.core.account,configuration.properties.core.project,configuration.properties.auth.impersonate_service_account,configuration.properties.auth.credential_file_override,configuration.properties.auth.access_token_file)",
    ];
    args.extend(selectors);
    // Anchor before the request so acquisition/parsing time consumes the lease.
    let anchor = monotonic();
    let bytes = helper.output(&args)?;
    parse_token(
        &bytes,
        account,
        project,
        execution,
        wall(),
        anchor,
        monotonic(),
    )
}
#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Auth {
    impersonate_service_account: Option<String>,
    credential_file_override: Option<String>,
    access_token_file: Option<String>,
}
impl Auth {
    fn validate(&self) -> io::Result<()> {
        if [
            &self.impersonate_service_account,
            &self.credential_file_override,
            &self.access_token_file,
        ]
        .iter()
        .any(|value| value.as_ref().is_some_and(|v| !v.is_empty()))
        {
            return Err(failure());
        }
        Ok(())
    }
}

fn parse_token(
    bytes: &[u8],
    account: &str,
    project: &str,
    execution: Duration,
    now: DateTime<Utc>,
    anchor: Instant,
    current: Instant,
) -> io::Result<AccessToken> {
    // Only projected unescaped JSON is admitted. Reject before deserialization
    // so secret escape decoding cannot populate serde_json's non-wiping scratch.
    if bytes.contains(&b'\\') {
        return Err(failure());
    }
    // Token input/final allocation zeroize; no generic credential-bearing Value.
    fn unescaped_token<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Zeroizing<String>, D::Error> {
        // Borrow directly from zeroizing input: escaped JSON token spellings are
        // refused rather than decoded via serde_json's non-wiping scratch buffer.
        let value = <&str as serde::Deserialize>::deserialize(deserializer)?;
        Ok(Zeroizing::new(value.to_owned()))
    }
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Credential {
        #[serde(deserialize_with = "unescaped_token")]
        access_token: Zeroizing<String>,
        token_expiry: String,
    }
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Export {
        credential: Credential,
        configuration: Configuration,
    }
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Configuration {
        properties: Properties,
    }
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Properties {
        core: Core,
        #[serde(default)]
        auth: Auth,
    }
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Core {
        account: String,
        project: String,
    }
    // Zeroizing's Deserialize requires no serialization support in AccessToken.
    let export: Export = serde_json::from_slice(bytes).map_err(|_| failure())?;
    let core = &export.configuration.properties.core;
    if core.account != account || core.project != project {
        return Err(failure());
    }
    export.configuration.properties.auth.validate()?;
    let bearer = export.credential.access_token;
    if bearer.is_empty()
        || bearer.len() > 16 * 1024
        || !bearer
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-._~+/=".contains(&b))
    {
        return Err(failure());
    }
    let expires = DateTime::parse_from_rfc3339(&export.credential.token_expiry)
        .map_err(|_| failure())?
        .with_timezone(&Utc);
    let remaining = expires
        .signed_duration_since(now)
        .to_std()
        .map_err(|_| failure())?;
    let required = execution.checked_add(EXPIRY_MARGIN).ok_or_else(failure)?;
    if remaining <= required {
        return Err(failure());
    }
    let deadline = anchor.checked_add(execution).ok_or_else(failure)?;
    if current >= deadline {
        return Err(failure());
    }
    Ok(AccessToken { bearer, deadline })
}

fn bounded_output(arguments: &[&str]) -> io::Result<Zeroizing<Vec<u8>>> {
    bounded_program("gcloud", arguments, HELPER_TIMEOUT)
}
struct HelperChild(std::process::Child);
impl Drop for HelperChild {
    fn drop(&mut self) {
        unsafe {
            libc::kill(-(self.0.id() as i32), libc::SIGKILL);
        }
        let _ = self.0.wait();
    }
}
fn bounded_program(
    program: &str,
    arguments: &[&str],
    timeout: Duration,
) -> io::Result<Zeroizing<Vec<u8>>> {
    let mut command = Command::new(program);
    command
        .args(arguments)
        .env("CLOUDSDK_CORE_DISABLE_FILE_LOGGING", "true")
        .env("CLOUDSDK_CORE_LOG_HTTP", "false")
        .env("CLOUDSDK_CORE_DISABLE_PROMPTS", "true")
        .env("CLOUDSDK_CORE_VERBOSITY", "error")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .process_group(0);
    // Overrides are refused by acquire; strip them as defense in depth.
    for (key, _) in std::env::vars_os() {
        if key
            .to_string_lossy()
            .to_ascii_uppercase()
            .starts_with("CLOUDSDK_AUTH_")
        {
            command.env_remove(key);
        }
    }
    let mut child = HelperChild(command.spawn().map_err(|_| failure())?);
    let group = child.0.id() as i32;
    let stopping = Arc::new(AtomicBool::new(false));
    let stop_reader = stopping.clone();
    let mut output = child.0.stdout.take().ok_or_else(failure)?;
    use std::os::fd::AsRawFd;
    let flags = unsafe { libc::fcntl(output.as_raw_fd(), libc::F_GETFL) };
    if flags < 0
        || unsafe { libc::fcntl(output.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0
    {
        unsafe {
            libc::kill(-group, libc::SIGKILL);
        }
        return Err(failure());
    }
    let reader = std::thread::Builder::new()
        .spawn(move || {
            // Fixed capacity prevents freed reallocations retaining earlier token bytes.
            let mut bytes = Zeroizing::new(Vec::with_capacity(OUTPUT_LIMIT));
            let mut buffer = Zeroizing::new([0u8; 4096]);
            loop {
                match output.read(&mut *buffer) {
                    Ok(0) => break,
                    Ok(n) if bytes.len() + n <= OUTPUT_LIMIT => {
                        bytes.extend_from_slice(&buffer[..n])
                    }
                    Ok(_) => return Err(failure()),
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        if stop_reader.load(Ordering::Acquire) {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(_) => return Err(failure()),
                }
            }
            Ok(bytes)
        })
        .map_err(|_| failure())?;
    let deadline = Instant::now() + timeout;
    let success = loop {
        // Observe without reaping so the process-group ID cannot be reused before kill.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        let status = unsafe {
            libc::waitid(
                libc::P_PID,
                group as libc::id_t,
                &mut info,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        if status != 0 {
            break false;
        }
        if unsafe { info.si_pid() } != 0 {
            break info.si_code == libc::CLD_EXITED && unsafe { info.si_status() } == 0;
        }
        if Instant::now() >= deadline {
            break false;
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    drop(child); // Kill descendants before reaping the leader, including on failure.
    stopping.store(true, Ordering::Release);
    let bytes = reader.join().map_err(|_| failure())??;
    if !success {
        return Err(failure());
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::os::unix::fs::PermissionsExt;
    fn response(expiry: &str) -> Vec<u8> {
        format!(r#"{{"credential":{{"access_token":"synthetic-token","token_expiry":"{expiry}"}},"configuration":{{"properties":{{"core":{{"account":"reader@example.com","project":"selected-project"}},"auth":{{}}}}}}}}"#).into_bytes()
    }
    fn parse(bytes: &[u8]) -> io::Result<AccessToken> {
        let anchor = Instant::now();
        parse_token(
            bytes,
            "reader@example.com",
            "selected-project",
            Duration::from_secs(300),
            DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            anchor,
            anchor,
        )
    }
    #[test]
    fn private_token_has_bounded_monotonic_lease() {
        let token = parse(&response("2026-01-01T01:00:00Z")).unwrap();
        assert_eq!(token.bearer().unwrap(), "synthetic-token");
        assert_eq!(format!("{token:?}"), "AccessToken([private])");
        assert!(token.ensure_live().is_ok());
        assert!(token.deadline() <= Instant::now() + MAX_EXECUTION);
    }
    #[test]
    fn unknown_expired_or_insufficient_expiry_refused_without_token_in_error() {
        for expiry in [
            "",
            "invalid",
            "2025-12-31T23:59:59Z",
            "2026-01-01T00:07:00Z",
        ] {
            let err = parse(&response(expiry)).unwrap_err();
            assert!(!err.to_string().contains("synthetic-token"));
        }
    }
    #[test]
    fn mismatched_identity_and_overrides_refused() {
        let text = String::from_utf8(response("2026-01-01T01:00:00Z")).unwrap();
        for value in [
            text.replace("reader@example.com", "other@example.com"),
            text.replace("selected-project", "other-project"),
            text.replace(
                "\"auth\":{}",
                "\"auth\":{\"impersonate_service_account\":\"other\"}",
            ),
            text.replace("synthetic-token", "bad\\nheader"),
            text.replace("synthetic-token", "synthetic\\u002Dtoken"),
            text.replace(
                "\"auth\":{}",
                "\"auth\":{\"access_token_file\":\"other\",\"access_token_file\":null}",
            ),
            text.replace(
                "\"access_token\":\"synthetic-token\"",
                "\"access_token\":\"synthetic-token\",\"access_token\":\"other\"",
            ),
        ] {
            assert!(parse(value.as_bytes()).is_err());
        }
    }
    #[test]
    fn helper_uses_explicit_selectors_and_same_response_expiry() {
        struct Fake(RefCell<Vec<Vec<String>>>);
        impl Helper for Fake {
            fn output(&self, args: &[&str]) -> io::Result<Zeroizing<Vec<u8>>> {
                self.0
                    .borrow_mut()
                    .push(args.iter().map(|s| (*s).into()).collect());
                Ok(Zeroizing::new(if args[1] == "list" {
                    b"{\"auth\":{}}".to_vec()
                } else {
                    response("2026-01-01T01:00:00Z")
                }))
            }
        }
        let helper = Fake(RefCell::new(vec![]));
        let token = acquire_with(
            &helper,
            "reader@example.com",
            "selected-project",
            Duration::from_secs(300),
            || {
                DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc)
            },
            Instant::now,
        )
        .unwrap();
        assert_eq!(token.bearer().unwrap(), "synthetic-token");
        let calls = helper.0.borrow();
        assert_eq!(calls.len(), 2);
        for call in calls.iter() {
            assert!(
                call.windows(2)
                    .any(|p| p == ["--account", "reader@example.com"])
            );
            assert!(
                call.windows(2)
                    .any(|p| p == ["--project", "selected-project"])
            );
        }
        assert!(calls[1].iter().any(|a| a == "--force-auth-refresh"));
    }
    #[test]
    fn bounded_private_helper_limits_failure_output_and_lifetime() {
        let short = Duration::from_millis(100);
        assert_eq!(
            &*bounded_program("/bin/sh", &["-c", "printf synthetic"], short).unwrap(),
            b"synthetic"
        );
        for script in [
            "printf private-error; exit 7",
            "sleep 3",
            "yes private | head -c 70000",
        ] {
            let started = Instant::now();
            let error = bounded_program("/bin/sh", &["-c", script], short).unwrap_err();
            assert!(started.elapsed() < Duration::from_secs(2));
            assert!(!error.to_string().contains("private"));
        }
    }
    #[test]
    fn expired_deadline_refuses_bearer_without_exposure() {
        let token = AccessToken {
            bearer: Zeroizing::new("synthetic-token".into()),
            deadline: Instant::now() - Duration::from_secs(1),
        };
        assert!(token.bearer().is_err());
    }
    #[test]
    #[ignore = "requires explicitly authorized GRV_GCS_ACCOUNT and GRV_GCS_PROJECT gcloud identity"]
    fn explicitly_selected_gcloud_returns_private_token_with_expiry() {
        let account = std::env::var("GRV_GCS_ACCOUNT").expect("explicit account required");
        let project = std::env::var("GRV_GCS_PROJECT").expect("explicit project required");
        let token = acquire(&account, &project, Duration::from_secs(300)).unwrap();
        assert!(token.ensure_live().is_ok());
        assert!(!token.bearer().unwrap().is_empty());
    }
    #[test]
    #[ignore = "actual fresh/concurrent OAuth gate: parent launches with GRV_GCS_ACCOUNT=jiacao@on.sinch.com and GRV_GCS_PROJECT=entit-finance-prod; no token output, no cloud resource, stop on reauth mismatch, no automatic retry"]
    fn explicitly_selected_fresh_and_concurrent_acquisition_is_independent_per_invocation() {
        let account = std::env::var("GRV_GCS_ACCOUNT").expect("explicit account required");
        let project = std::env::var("GRV_GCS_PROJECT").expect("explicit project required");
        let budget = Duration::from_secs(300);
        // Two sequential fresh independent acquisitions: each is its own
        // --force-auth-refresh provider invocation; no fallback, no reuse.
        let first = acquire(&account, &project, budget).unwrap();
        let second = acquire(&account, &project, budget).unwrap();
        assert!(first.ensure_live().is_ok());
        assert!(!first.bearer().unwrap().is_empty());
        assert!(second.ensure_live().is_ok());
        assert!(!second.bearer().unwrap().is_empty());
        // Two concurrent acquisitions of the same selected identity, each in
        // its own private bounded helper process. No comparison of token
        // values is made: the provider may legitimately return the same live
        // token. Already-launched helpers are joined privately and bounded by
        // the helper timeout; a reauthentication mismatch stops, it never
        // auto-retries.
        let (left_account, right_account) = (account.clone(), account);
        let (left_project, right_project) = (project.clone(), project);
        let (left, right) = std::thread::scope(|scope| {
            let left = scope.spawn(move || acquire(&left_account, &left_project, budget));
            let right = scope.spawn(move || acquire(&right_account, &right_project, budget));
            // Join BOTH before inspecting either result; scoped teardown also
            // waits for launched threads if launch/join unexpectedly panics.
            let left = left.join();
            let right = right.join();
            (
                left.expect("left acquisition thread panicked"),
                right.expect("right acquisition thread panicked"),
            )
        });
        let left = left.expect("left fresh acquisition failed");
        let right = right.expect("right fresh acquisition failed");
        assert!(left.ensure_live().is_ok());
        assert!(!left.bearer().unwrap().is_empty());
        assert!(right.ensure_live().is_ok());
        assert!(!right.bearer().unwrap().is_empty());
    }
    /// Private child-process fixture: a fake gcloud in a temp dir plus a
    /// helper counter. Only the owning (child) process environment is
    /// mutated; every mutation is restored on drop, so the baseline
    /// inherited state is unchanged in any process.
    struct AmbientFixture {
        /// Held only so the temp dir (and the fake gcloud inside it) is
        /// removed by TempDir's own drop; never read again.
        #[allow(dead_code)]
        dir: tempfile::TempDir,
        counter: std::path::PathBuf,
        old_path: Option<std::ffi::OsString>,
        removed: Vec<(String, Option<std::ffi::OsString>)>,
    }
    impl AmbientFixture {
        fn new() -> Option<Self> {
            let dir = tempfile::tempdir().ok()?;
            let counter = dir.path().join("helper-counter");
            let fake = dir.path().join("gcloud");
            if std::fs::write(
                &fake,
                "#!/bin/sh\necho x >> \"$GRV_AMBIENT_HELPER_COUNTER\"\nexit 7\n",
            )
            .is_err()
            {
                return None;
            }
            if std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).is_err() {
                return None;
            }
            let removed = std::env::vars_os()
                .filter(|(key, _)| {
                    key.to_string_lossy()
                        .to_ascii_uppercase()
                        .starts_with("CLOUDSDK_AUTH_")
                })
                .map(|(key, value)| (key.to_string_lossy().into_owned(), Some(value)))
                .collect::<Vec<_>>();
            for (key, _) in &removed {
                unsafe { std::env::remove_var(key) };
            }
            let old_path = std::env::var_os("PATH");
            unsafe {
                std::env::set_var(
                    "PATH",
                    format!(
                        "{}:{}",
                        dir.path().display(),
                        old_path
                            .as_ref()
                            .map(|value| value.to_string_lossy())
                            .unwrap_or_default()
                    ),
                );
                std::env::set_var("GRV_AMBIENT_HELPER_COUNTER", counter.clone());
            }
            Some(Self {
                dir,
                counter,
                old_path,
                removed,
            })
        }
    }
    impl Drop for AmbientFixture {
        fn drop(&mut self) {
            unsafe {
                std::env::remove_var("GRV_AMBIENT_HELPER_COUNTER");
                std::env::remove_var("CLOUDSDK_AUTH_ACCESS_TOKEN_FILE");
                match self.old_path.take() {
                    Some(value) => std::env::set_var("PATH", value),
                    None => std::env::remove_var("PATH"),
                }
                for (key, value) in self.removed.iter().rev() {
                    match value {
                        Some(value) => std::env::set_var(key, value),
                        None => std::env::remove_var(key),
                    }
                }
            }
        }
    }
    fn helper_invocations(counter: &std::path::Path) -> usize {
        std::fs::read_to_string(counter)
            .map(|text| text.lines().count())
            .unwrap_or(0)
    }
    /// Child mode: from a clean environment the explicit helper is the only
    /// acquisition path; the fake helper fails closed and is observed via its
    /// private counter, never through process-global state.
    #[test]
    fn child_ambient_control_proves_helper_invocation() {
        if std::env::var("GRV_TEST_GCS_AMBIENT_CHILD").as_deref()
            != Ok("gcs_token::tests::child_ambient_control_proves_helper_invocation")
        {
            return;
        }
        let fixture = AmbientFixture::new().expect("private fixture");
        let error = acquire(
            "reader@example.com",
            "selected-project",
            Duration::from_secs(300),
        )
        .expect_err("the fake helper must fail closed");
        assert!(!error.to_string().contains("reader@example.com"));
        assert!(!error.to_string().contains("selected-project"));
        assert!(
            helper_invocations(&fixture.counter) > 0,
            "clean acquire must reach the explicit helper"
        );
        println!("GRV_AMBIENT_PROBE_HELPER_INVOKED");
        drop(fixture);
    }
    /// Child mode: the clean phase invokes the helper; the ambient override
    /// phase refuses BEFORE any helper invocation.
    #[test]
    fn child_ambient_probe_refuses_before_helper_under_override() {
        if std::env::var("GRV_TEST_GCS_AMBIENT_CHILD").as_deref()
            != Ok("gcs_token::tests::child_ambient_probe_refuses_before_helper_under_override")
        {
            return;
        }
        let fixture = AmbientFixture::new().expect("private fixture");
        let error = acquire(
            "reader@example.com",
            "selected-project",
            Duration::from_secs(300),
        )
        .expect_err("the fake helper must fail closed");
        assert!(!error.to_string().contains("reader@example.com"));
        let invoked = helper_invocations(&fixture.counter);
        assert!(invoked > 0, "clean acquire must reach the explicit helper");
        println!("GRV_AMBIENT_PROBE_HELPER_INVOKED");
        unsafe { std::env::set_var("CLOUDSDK_AUTH_ACCESS_TOKEN_FILE", "/never/read") };
        let error = acquire(
            "reader@example.com",
            "selected-project",
            Duration::from_secs(300),
        )
        .expect_err("an ambient override must refuse acquisition");
        assert!(!error.to_string().contains("/never/read"));
        assert_eq!(
            helper_invocations(&fixture.counter),
            invoked,
            "the helper must not be invoked under an ambient override"
        );
        println!("GRV_AMBIENT_PROBE_OVERRIDE_REFUSED");
        drop(fixture);
    }
    /// Parent mode: spawn exact child processes of this test binary (one per
    /// probe) with inherited ambient overrides removed from the child
    /// environment only; the parent process environment is never mutated and
    /// is asserted byte-for-byte unchanged afterwards.
    #[test]
    fn ambient_auth_override_refuses_before_any_helper_invocation() {
        let environment = || {
            std::env::vars_os()
                .collect::<std::collections::BTreeMap<std::ffi::OsString, std::ffi::OsString>>()
        };
        let baseline = environment();
        let spawn = |filter: &str| -> (bool, String) {
            let mut command = std::process::Command::new(
                std::env::current_exe().expect("test binary path required"),
            );
            command
                .arg(filter)
                .arg("--exact")
                .arg("--test-threads=1")
                .arg("--nocapture")
                .env("GRV_TEST_GCS_AMBIENT_CHILD", filter)
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped());
            for (key, _) in std::env::vars_os() {
                if key
                    .to_string_lossy()
                    .to_ascii_uppercase()
                    .starts_with("CLOUDSDK_AUTH_")
                {
                    command.env_remove(key);
                }
            }
            let output = command.output().expect("child probe spawn");
            (
                output.status.success(),
                String::from_utf8_lossy(&output.stdout).into_owned(),
            )
        };
        let (control_ok, control_out) =
            spawn("gcs_token::tests::child_ambient_control_proves_helper_invocation");
        assert!(control_ok, "control child failed: {control_out}");
        assert!(
            control_out.contains("GRV_AMBIENT_PROBE_HELPER_INVOKED"),
            "control child marker missing: {control_out}"
        );
        let (probe_ok, probe_out) =
            spawn("gcs_token::tests::child_ambient_probe_refuses_before_helper_under_override");
        assert!(probe_ok, "probe child failed: {probe_out}");
        assert!(
            probe_out.contains("GRV_AMBIENT_PROBE_HELPER_INVOKED"),
            "probe child invocation marker missing: {probe_out}"
        );
        assert!(
            probe_out.contains("GRV_AMBIENT_PROBE_OVERRIDE_REFUSED"),
            "probe child refusal marker missing: {probe_out}"
        );
        // Compare keys AND values without rendering potentially private env.
        assert!(
            environment() == baseline,
            "parent environment must remain unchanged"
        );
    }
    #[test]
    fn subprocess_token_canary_never_enters_public_evidence() {
        let canary = "synthetic-bearer-canary-0123456789abcdef";
        // Token output followed by a non-zero exit: the public error is generic.
        let error = bounded_program(
            "/bin/sh",
            &["-c", &format!("printf '{canary}'; exit 7")],
            Duration::from_secs(5),
        )
        .unwrap_err();
        assert!(!error.to_string().contains(canary));
        // Token output followed by a hang: the timeout path is canary-free.
        let error = bounded_program(
            "/bin/sh",
            &["-c", &format!("printf '{canary}'; sleep 30")],
            Duration::from_millis(200),
        )
        .unwrap_err();
        assert!(!error.to_string().contains(canary));
        // Partial output beyond the bounded limit: only the bounded prefix is
        // retained and the error is generic.
        let error = bounded_program(
            "/bin/sh",
            &["-c", &format!("yes '{canary}' | head -c 70000")],
            Duration::from_millis(200),
        )
        .unwrap_err();
        assert!(!error.to_string().contains(canary));
        // stderr is never captured: a canary written there cannot enter evidence.
        let error = bounded_program(
            "/bin/sh",
            &["-c", &format!("printf '{canary}' >&2; exit 7")],
            Duration::from_secs(5),
        )
        .unwrap_err();
        assert!(!error.to_string().contains(canary));
        // The success path returns only the bounded payload.
        let bytes = bounded_program(
            "/bin/sh",
            &["-c", &format!("printf '{canary}'")],
            Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(&*bytes, canary.as_bytes());
    }
    #[test]
    fn slow_acquisition_cannot_extend_execution_lease() {
        let anchor = Instant::now();
        assert!(
            parse_token(
                &response("2026-01-01T01:00:00Z"),
                "reader@example.com",
                "selected-project",
                Duration::from_secs(300),
                DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc),
                anchor,
                anchor + Duration::from_secs(301)
            )
            .is_err()
        );
    }
}
