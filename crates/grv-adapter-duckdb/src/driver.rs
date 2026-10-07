//! Reusable supervised external invocation; this is a library seam, not a public
//! command. Callers obtain actual output results from their driver. Children must
//! retain the inherited workspace descriptor until their engine work stops.
use crate::{build::BuildStore, lock::WorkspaceLock};
use grv_adapter_api::*;
use std::{
    io,
    os::{
        fd::AsRawFd,
        unix::{ffi::OsStrExt, process::CommandExt},
    },
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus},
    sync::Arc,
    time::{Duration, Instant},
};
fn failure(error: impl std::fmt::Debug) -> io::Error {
    io::Error::other(format!("{error:?}"))
}
fn signal_group(pid: u32, signal: i32) -> io::Result<()> {
    if unsafe { libc::kill(-(pid as i32), signal) } == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(io::Error::new(
            error.kind(),
            format!("signal external process group: {error}"),
        ))
    }
}
fn exited(child: &Child) -> io::Result<bool> {
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    // Do not reap yet: the leader's PID remains reserved while its group is
    // signalled, preventing a signal from reaching a reused PID/process group.
    if unsafe {
        libc::waitid(
            libc::P_PID,
            child.id(),
            &mut info,
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    } != 0
    {
        let error = io::Error::last_os_error();
        return Err(io::Error::new(
            error.kind(),
            format!("observe external leader without reaping: {error}"),
        ));
    }
    Ok(unsafe { info.si_pid() } != 0)
}

pub struct ExternalInvocation {
    child: Child,
    ownership: Option<Arc<WorkspaceLock>>,
    path: PathBuf,
    session: BuildSession,
    resources: Resources,
    invocation_id: Uuid,
    reaped: bool,
}
impl ExternalInvocation {
    /// Takes the prepared store, durably records invocation intent and closes
    /// its native connections before spawning the external process. Spawn
    /// failure leaves evidence and forbids another invocation of this session.
    pub fn start(
        mut store: BuildStore,
        session: BuildSession,
        resources: Resources,
        mut command: Command,
    ) -> io::Result<Self> {
        session.validate().map_err(failure)?;
        resources.validate().map_err(failure)?;
        let invocation_id = Uuid::v4();
        let mut fingerprint = Vec::new();
        for argument in std::iter::once(command.get_program()).chain(command.get_args()) {
            let bytes = argument.as_bytes();
            fingerprint.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
            fingerprint.extend_from_slice(bytes);
        }
        let ownership = store
            .begin_external(&session, &invocation_id, grv_types::sha256(&fingerprint))
            .map_err(failure)?;
        let path = ownership.engine_path().to_owned();
        let reference = ownership.descendant_reference()?;
        let fd = reference.as_raw_fd();
        command.env("GRV_DUCKDB_WORKSPACE_LOCK_FD", fd.to_string());
        unsafe {
            command.pre_exec(move || {
                if libc::setpgid(0, 0) != 0 || libc::fcntl(fd, libc::F_SETFD, 0) < 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        // Native connections stop while the cloned workspace ownership remains.
        drop(store);
        let child = command.spawn()?;
        drop(reference);
        Ok(Self {
            child,
            ownership: Some(ownership),
            path,
            session,
            resources,
            invocation_id,
            reaped: false,
        })
    }
    pub fn invocation_id(&self) -> &Uuid {
        &self.invocation_id
    }
    pub fn process_id(&self) -> u32 {
        self.child.id()
    }

    fn stop_writers(&mut self) -> io::Result<(WorkspaceLock, ExitStatus, bool)> {
        let leader_exited = exited(&self.child)?;
        self.ownership.take();
        let mut ownership_error = None;
        let unfinished_descendant = leader_exited
            && match WorkspaceLock::acquire(&self.path) {
                Ok(owner) => {
                    // Every compliant engine writer retains this shared lock.
                    // No signal is needed when both leader and ownership stop.
                    let status = self.child.wait()?;
                    self.reaped = true;
                    return Ok((owner, status, false));
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => true,
                Err(error) => {
                    ownership_error = Some(error);
                    true
                }
            };
        if let Err(error) = signal_group(self.child.id(), libc::SIGTERM) {
            if leader_exited {
                let _ = self.child.wait();
                self.reaped = true;
            }
            return Err(error);
        }
        let start = Instant::now();
        let mut killed = false;
        loop {
            if !killed && start.elapsed() >= Duration::from_millis(250) {
                signal_group(self.child.id(), libc::SIGKILL)?;
                killed = true;
            }
            if killed && exited(&self.child)? {
                if let Some(error) = ownership_error.take() {
                    let _ = self.child.wait();
                    self.reaped = true;
                    return Err(error);
                }
                match WorkspaceLock::acquire(&self.path) {
                    Ok(owner) => {
                        let status = self.child.wait()?;
                        self.reaped = true;
                        return Ok((owner, status, unfinished_descendant));
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    Err(e) => {
                        let _ = self.child.wait();
                        self.reaped = true;
                        return Err(e);
                    }
                }
            }
            if start.elapsed() >= Duration::from_secs(2) {
                // In particular, an escaped descendant still holding inherited
                // ownership prevents stopped-writer attestation and cleanup.
                if exited(&self.child)? {
                    let _ = self.child.wait();
                    self.reaped = true;
                }
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "external writer ownership has not stopped",
                ));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    /// Stops the entire supervised process group and waits for inherited
    /// ownership to disappear. Failure preserves the executing session.
    pub fn abort(mut self) -> io::Result<()> {
        self.stop_writers().map(|_| ())
    }
    /// Polls run ownership while the invocation runs. False/error stops writers
    /// and refuses completion. The supplied output results are the driver's
    /// actual successful results, including explicit zero-row outputs.
    pub fn complete(
        mut self,
        kind: CompletionKind,
        completed_outputs: Vec<CompletedOutput>,
        completion_path: &Path,
        mut still_owned: impl FnMut() -> io::Result<bool>,
    ) -> io::Result<BuildCompletion> {
        while !exited(&self.child)? {
            if !still_owned()? {
                let _ = self.stop_writers();
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "external run ownership lost",
                ));
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        let (ownership, status, unfinished_descendant) = self.stop_writers()?;
        if !status.success() || unfinished_descendant || !still_owned()? {
            return Err(io::Error::other(
                "external invocation failed or lost run ownership",
            ));
        }
        ownership.recheck()?;
        drop(ownership);
        // Reopening verifies the native engine file lock as well as cooperative
        // helper ownership; a remaining connection cannot pass this barrier.
        let mut store = BuildStore::open(
            &self.path,
            self.session.identity.root.clone(),
            Some(self.session.identity.workspace_id.clone()),
            &self.resources,
        )
        .map_err(failure)?;
        let ownership = store
            .validate_external_finish(&self.session, &self.invocation_id, &completed_outputs)
            .map_err(failure)?;
        let completion = BuildCompletion {
            result_version: Req::new(1).unwrap(),
            run_id: self.session.identity.run_id.clone(),
            workspace_id: self.session.identity.workspace_id.clone(),
            declaration_sha256: self.session.identity.declaration_sha256.clone(),
            kind,
            invocation_id: self.invocation_id.as_str().into(),
            status: CompletionStatus::Succeeded,
            writers_stopped: true,
            completed_at: Timestamp::new(
                chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            )
            .map_err(failure)?,
            completed_outputs,
        };
        completion.validate_for(&self.session).map_err(failure)?;
        // No native connection remains when the completion becomes visible.
        // Workspace ownership is retained through durable file publication.
        drop(store);
        ownership.recheck()?;
        if !still_owned()? {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "external run ownership lost before completion",
            ));
        }
        crate::completion::publish(completion_path, &self.session, &completion)?;
        ownership.recheck()?;
        Ok(completion)
    }
}
impl Drop for ExternalInvocation {
    fn drop(&mut self) {
        if !self.reaped {
            let _ = self.stop_writers();
        }
    }
}
