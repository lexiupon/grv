//! One build engine owner survives SDK calls and retained preparation. Only
//! immutable typed requests enter its bounded queue; interrupt bypasses it.
use crate::{
    build::{BuildError, BuildStore, Result},
    native::NativeInterrupt,
    pull_worker::StopSignal,
    worker::Interrupt,
};
use grv_adapter_api::{Resources, Uuid};
use std::{
    io,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};
type Job = Box<dyn FnOnce(&mut BuildStore) + Send>;
pub struct BuildWorker {
    sender: Option<mpsc::SyncSender<Job>>,
    interrupt: NativeInterrupt,
    stopping: Arc<AtomicBool>,
    owner: Option<thread::JoinHandle<()>>,
    workspace: Option<Uuid>,
}
impl BuildWorker {
    pub fn open(
        path: &Path,
        root: String,
        workspace: Option<Uuid>,
        resources: Resources,
    ) -> Result<Self> {
        let path = path.to_owned();
        let (sender, commands) = mpsc::sync_channel::<Job>(1);
        let (ready, initialized) = mpsc::sync_channel(1);
        let stopping = Arc::new(AtomicBool::new(false));
        let stop = stopping.clone();
        let owner = thread::Builder::new()
            .name("grv-duckdb-build-owner".into())
            .spawn(move || {
                let mut store = match BuildStore::open(&path, root, workspace, &resources) {
                    Ok(store) => store,
                    Err(error) => {
                        let _ = ready.send(Err(error));
                        return;
                    }
                };
                store.set_stopping(stop.clone());
                if ready
                    .send(Ok((store.interrupt_handle(), store.workspace_id())))
                    .is_err()
                {
                    return;
                }
                while let Ok(job) = commands.recv() {
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    job(&mut store);
                }
            })?;
        let (interrupt, workspace) = match initialized.recv() {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => {
                let _ = owner.join();
                return Err(error);
            }
            Err(_) => {
                let _ = owner.join();
                return Err(BuildError::Unknown(
                    "build owner initialization ended without evidence".into(),
                ));
            }
        };
        Ok(Self {
            sender: Some(sender),
            interrupt,
            stopping,
            owner: Some(owner),
            workspace,
        })
    }
    pub fn workspace_id(&self) -> Option<Uuid> {
        self.workspace.clone()
    }
    pub fn interrupt_handle(&self) -> NativeInterrupt {
        self.interrupt.clone()
    }
    pub fn call<T: Send + 'static>(
        &mut self,
        job: impl FnOnce(&mut BuildStore) -> Result<T> + Send + 'static,
        stop: &impl StopSignal,
    ) -> Result<T> {
        if stop.is_cancelled() {
            self.stop()?;
            return Err(BuildError::Incomplete(
                "build stopped before dispatch".into(),
            ));
        }
        let (reply, response) = mpsc::sync_channel(1);
        self.sender
            .as_ref()
            .ok_or_else(|| BuildError::Incomplete("build owner already stopped".into()))?
            .send(Box::new(move |store| {
                let _ = reply.send(job(store));
            }))
            .map_err(|_| BuildError::Unknown("build owner ended before dispatch".into()))?;
        loop {
            match response.try_recv() {
                Ok(result) => return result,
                Err(mpsc::TryRecvError::Disconnected) => {
                    return Err(BuildError::Unknown(
                        "build owner ended without terminal evidence".into(),
                    ));
                }
                Err(mpsc::TryRecvError::Empty) => {}
            }
            if stop.is_cancelled() {
                self.stop()?;
                return response.try_recv().unwrap_or_else(|_| {
                    Err(BuildError::Incomplete(
                        "build stopped after writer fencing; never repeat incomplete invocation"
                            .into(),
                    ))
                });
            }
            match response.recv_timeout(Duration::from_millis(20)) {
                Ok(result) => return result,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(_) => {
                    return Err(BuildError::Unknown(
                        "build owner ended without terminal evidence".into(),
                    ));
                }
            }
        }
    }
    pub fn stop(&mut self) -> io::Result<()> {
        self.stopping.store(true, Ordering::Release);
        self.interrupt.interrupt();
        self.sender.take();
        self.owner.take().map_or(Ok(()), |owner| {
            owner
                .join()
                .map_err(|_| io::Error::other("build owner panicked"))
        })
    }
}
impl Drop for BuildWorker {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

#[cfg(test)]
mod predispatch_tests {
    use super::*;
    use crate::build::BuildStore;
    use crate::lock::WorkspaceLock;
    use grv_adapter_api::Resources;
    use std::sync::atomic::AtomicUsize;

    /// Cancellation set BEFORE any job is dispatched: the counter closure is
    /// never executed, the owner is joined, and the workspace lock is released.
    /// No incomplete invocation is ever repeated.
    #[test]
    fn cancel_before_dispatch_joins_owner_and_never_runs_the_job() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("predispatch-build.duckdb");
        let mut owner = BuildWorker::open(
            &database,
            "file:///private/grv".into(),
            None,
            Resources::default(),
        )
        .unwrap();
        assert!(WorkspaceLock::acquire(&database).is_err());
        let counter = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(true));
        let count = counter.clone();
        let error = owner
            .call(
                move |store: &mut BuildStore| {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(store.workspace_id())
                },
                stop.as_ref(),
            )
            .unwrap_err();
        assert!(
            matches!(
                error,
                BuildError::Incomplete(ref message) if message.contains("stopped before dispatch")
            ),
            "{error:?}"
        );
        // The job closure was never dispatched to the owner.
        assert_eq!(counter.load(Ordering::SeqCst), 0);
        // The owner thread has joined: the command channel and owner handle are
        // empty, and the workspace lock is released under the real WorkspaceLock.
        assert!(owner.sender.is_none());
        assert!(owner.owner.is_none());
        assert!(WorkspaceLock::acquire(&database).is_ok());
        // A stopped owner is not reused: a second dispatch is refused the same way.
        let count = counter.clone();
        let again = owner
            .call(
                move |store: &mut BuildStore| {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(store.workspace_id())
                },
                stop.as_ref(),
            )
            .unwrap_err();
        assert!(matches!(again, BuildError::Incomplete(_)), "{again:?}");
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }
}
