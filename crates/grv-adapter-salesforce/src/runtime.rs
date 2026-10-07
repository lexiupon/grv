//! Bounded supervised helpers. Child output is private and errors never include
//! its contents. Each helper has its own process group, including descendants.
use crate::{Error, Result};
use grv_adapter_sdk::StopToken;
use std::{
    ffi::OsString,
    io::{Read, Write},
    os::unix::process::CommandExt,
    process::{Command, ExitStatus, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

#[derive(Clone, Default)]
pub struct Cancellation {
    local: Arc<AtomicBool>,
    sdk: Option<StopToken>,
}
impl Cancellation {
    pub fn from_sdk(token: &StopToken) -> Self {
        Self {
            local: Default::default(),
            sdk: Some(token.clone()),
        }
    }
    pub fn cancel(&self) {
        self.local.store(true, Ordering::Release);
    }
    pub fn is_cancelled(&self) -> bool {
        self.local.load(Ordering::Acquire) || self.sdk.as_ref().is_some_and(StopToken::is_cancelled)
    }
    pub fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            Err(Error::new(
                "EXTRACTION_INCOMPLETE",
                "Salesforce operation stopped",
            ))
        } else {
            Ok(())
        }
    }
}

/// Intentionally not Debug/Serialize: stdin can contain bearer credentials.
pub struct ProcessSpec {
    pub program: OsString,
    pub supervisor: Option<OsString>,
    pub args: Vec<OsString>,
    pub env: Vec<(OsString, OsString)>,
    pub stdin: Vec<u8>,
    pub stdout_limit: usize,
    pub timeout: Duration,
}
pub struct ProcessOutput {
    pub stdout: Vec<u8>,
    pub status: ExitStatus,
}

fn drain(mut reader: impl Read, limit: usize, exceeded: &AtomicBool) -> std::io::Result<Vec<u8>> {
    let mut output = Vec::new();
    let mut chunk = [0; 8192];
    loop {
        let n = reader.read(&mut chunk)?;
        if n == 0 {
            return Ok(output);
        }
        let remaining = limit.saturating_sub(output.len());
        let retained = n.min(remaining);
        if output.capacity() - output.len() < retained {
            let target = output
                .capacity()
                .saturating_mul(2)
                .max(output.len() + retained)
                .min(limit);
            output.reserve_exact(target - output.len());
        }
        output.extend_from_slice(&chunk[..retained]);
        if n > remaining {
            exceeded.store(true, Ordering::Release);
        }
    }
}

pub fn run(spec: ProcessSpec, cancellation: &Cancellation) -> Result<ProcessOutput> {
    cancellation.check()?;
    if spec.stdout_limit == 0 || spec.timeout.is_zero() || spec.stdin.len() > 32 * 1024 * 1024 {
        return Err(Error::new(
            "INVALID_ARGUMENT",
            "invalid helper resource budgets",
        ));
    }
    let supervisor = spec
        .supervisor
        .unwrap_or_else(|| std::env::current_exe().unwrap_or_default().into_os_string());
    let mut command = Command::new(supervisor);
    command
        .arg(crate::containment::HELPER_ARG)
        .arg(spec.program)
        .args(spec.args)
        .envs(spec.env)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    unsafe {
        command.pre_exec(|| {
            if libc::setpgid(0, 0) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command
        .spawn()
        .map_err(|_| Error::new("ADAPTER_FAILURE", "Salesforce helper could not start"))?;
    let pgid = child.id() as i32;
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");
    let mut stdin = child.stdin.take().expect("piped stdin");
    let exceeded = Arc::new(AtomicBool::new(false));
    let output_exceeded = exceeded.clone();
    let output_reader = thread::spawn(move || drain(stdout, spec.stdout_limit, &output_exceeded));
    let error_reader = thread::spawn(move || drain(stderr, 64 * 1024, &AtomicBool::new(false)));
    let input_writer = thread::spawn(move || {
        let result = stdin.write_all(&spec.stdin);
        drop(stdin);
        result
    });
    let deadline = Instant::now() + spec.timeout;
    let (status, failure) = loop {
        let failure = if cancellation.is_cancelled() {
            Some(Error::new(
                "EXTRACTION_INCOMPLETE",
                "Salesforce helper stopped",
            ))
        } else if exceeded.load(Ordering::Acquire) {
            Some(Error::new(
                "INTEGRITY_FAILURE",
                "Salesforce helper output exceeds resource budget",
            ))
        } else if Instant::now() >= deadline {
            Some(Error::new(
                "ADAPTER_FAILURE",
                "Salesforce helper deadline expired",
            ))
        } else {
            None
        };
        if failure.is_some() {
            unsafe {
                libc::kill(-pgid, libc::SIGTERM);
                libc::kill(pgid, libc::SIGTERM);
            }
            thread::sleep(Duration::from_millis(100));
            unsafe {
                libc::kill(-pgid, libc::SIGKILL);
                libc::kill(pgid, libc::SIGKILL);
            }
            break (child.wait(), failure);
        }
        match child.try_wait() {
            Ok(Some(status)) => {
                // The inherited no-fork containment guarantees no descendants.
                // try_wait has reaped this PID: never signal a potentially
                // reused PID/group after successful status collection.
                break (Ok(status), None);
            }
            Ok(None) => thread::sleep(Duration::from_millis(10)),
            Err(_) => {
                unsafe {
                    libc::kill(-pgid, libc::SIGKILL);
                    libc::kill(pgid, libc::SIGKILL);
                }
                break (
                    child.wait(),
                    Some(Error::new(
                        "ADAPTER_FAILURE",
                        "Salesforce helper supervision failed",
                    )),
                );
            }
        }
    };
    let input = input_writer.join();
    let output = output_reader.join();
    let _ = error_reader.join();
    if let Some(error) = failure {
        return Err(error);
    }
    let stdout = output
        .ok()
        .and_then(std::result::Result::ok)
        .ok_or_else(|| Error::new("ADAPTER_FAILURE", "Salesforce helper private output failed"))?;
    if exceeded.load(Ordering::Acquire) {
        return Err(Error::new(
            "INTEGRITY_FAILURE",
            "Salesforce helper output exceeds resource budget",
        ));
    }
    let status =
        status.map_err(|_| Error::new("ADAPTER_FAILURE", "Salesforce helper status failed"))?;
    if status.success() && !matches!(input, Ok(Ok(()))) {
        return Err(Error::new(
            "ADAPTER_FAILURE",
            "Salesforce helper private input failed",
        ));
    }
    Ok(ProcessOutput { stdout, status })
}
