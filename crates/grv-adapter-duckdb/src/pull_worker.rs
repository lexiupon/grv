//! A single native owner holds the workspace lock through outcome resolution,
//! staging and commit. Cancellation joins it before returning stopped evidence.
use crate::{
    native::NativeInterrupt,
    pull::{
        PullError, PullIdentity, PullPlan, PullPreview, PullReceipt, PullResolution, PullStore,
        RelationName, WorkspaceBinding,
    },
    worker::Interrupt,
};
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
pub trait StopSignal {
    fn is_cancelled(&self) -> bool;
}
impl StopSignal for grv_adapter_sdk::StopToken {
    fn is_cancelled(&self) -> bool {
        self.is_cancelled()
    }
}
impl StopSignal for AtomicBool {
    fn is_cancelled(&self) -> bool {
        self.load(Ordering::Acquire)
    }
}
enum Command {
    ConfigureS3(crate::s3_config::S3Reader),
    ConfigureGcs(crate::gcs_config::GcsReader),
    Binding(String),
    Lookup(grv_types::Uuid, String),
    Resolve(PullIdentity),
    Preview(PullIdentity, Vec<RelationName>),
    PriorContracts(PullIdentity, Vec<grv_types::Name>),
    Apply(Box<PullPlan>),
}
enum Reply {
    ConfiguredS3,
    ConfiguredGcs,
    Binding(Option<WorkspaceBinding>),
    Preview(PullPreview),
    PriorContracts(Vec<grv_adapter_api::NamedContract>),
    Resolved(PullResolution),
    Applied(Box<PullReceipt>),
}
pub struct PullWorker {
    sender: Option<mpsc::SyncSender<Command>>,
    receiver: mpsc::Receiver<Result<Reply, PullError>>,
    interrupt: NativeInterrupt,
    stopping: Arc<AtomicBool>,
    owner: Option<thread::JoinHandle<()>>,
}
impl PullWorker {
    pub fn open(path: &Path, previously_bound: bool) -> io::Result<Self> {
        Self::open_with_resources(
            path,
            previously_bound,
            grv_adapter_api::Resources::default(),
        )
    }
    pub fn open_with_resources(
        path: &Path,
        previously_bound: bool,
        resources: grv_adapter_api::Resources,
    ) -> io::Result<Self> {
        let path = path.to_owned();
        let (sender, commands) = mpsc::sync_channel(1);
        let (reply, receiver) = mpsc::sync_channel(1);
        let (ready, initialized) = mpsc::sync_channel(1);
        let stopping = Arc::new(AtomicBool::new(false));
        let stop = stopping.clone();
        let owner = thread::Builder::new()
            .name("grv-duckdb-pull-owner".into())
            .spawn(move || {
                let opened = if previously_bound {
                    PullStore::open_bound(&path)
                } else {
                    PullStore::open(&path)
                };
                let mut store = match opened {
                    Ok(store) => store,
                    Err(error) => {
                        let _ = ready.send(Err(error));
                        return;
                    }
                };
                if let Err(error) = store.configure_resources(&resources) {
                    let _ = ready.send(Err(error));
                    return;
                }
                store.set_stopping(stop.clone());
                if ready.send(Ok(store.interrupt_handle())).is_err() {
                    return;
                }
                while let Ok(command) = commands.recv() {
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    let result = match command {
                        Command::ConfigureS3(reader) => store
                            .configure_s3_reader(&reader)
                            .map(|()| Reply::ConfiguredS3),
                        Command::ConfigureGcs(reader) => store
                            .configure_gcs_reader(&reader)
                            .map(|()| Reply::ConfiguredGcs),
                        Command::Binding(root) => store.binding(&root).map(Reply::Binding),
                        Command::Lookup(attempt, root) => {
                            store.lookup(&attempt, &root).map(Reply::Resolved)
                        }
                        Command::Preview(identity, targets) => {
                            store.preview(&identity, &targets).map(Reply::Preview)
                        }
                        Command::Resolve(identity) => store.resolve(&identity).map(Reply::Resolved),
                        Command::PriorContracts(identity, tables) => store
                            .prior_source_contracts(&identity, &tables)
                            .map(Reply::PriorContracts),
                        Command::Apply(plan) => store.apply(&plan).map(Reply::Applied),
                    };
                    // Preserve established terminal results even if cancellation
                    // races after COMMIT. At most one reply is outstanding.
                    if reply.send(result).is_err() || stop.load(Ordering::Acquire) {
                        break;
                    }
                }
            })?;
        let interrupt = match initialized.recv() {
            Ok(Ok(interrupt)) => interrupt,
            Ok(Err(error)) => {
                let _ = owner.join();
                return Err(error);
            }
            Err(_) => {
                let _ = owner.join();
                return Err(io::Error::other("pull owner initialization failed"));
            }
        };
        Ok(Self {
            sender: Some(sender),
            receiver,
            interrupt,
            stopping,
            owner: Some(owner),
        })
    }
    pub fn interrupt_handle(&self) -> NativeInterrupt {
        self.interrupt.clone()
    }
    pub fn configure_s3_reader(
        &mut self,
        reader: crate::s3_config::S3Reader,
        stop: &impl StopSignal,
    ) -> Result<(), PullError> {
        match self.call(Command::ConfigureS3(reader), stop)? {
            Reply::ConfiguredS3 => Ok(()),
            _ => Err(PullError::OutcomeUnknown(
                "unexpected S3 reader owner reply".into(),
            )),
        }
    }
    pub fn configure_gcs_reader(
        &mut self,
        reader: crate::gcs_config::GcsReader,
        stop: &impl StopSignal,
    ) -> Result<(), PullError> {
        match self.call(Command::ConfigureGcs(reader), stop)? {
            Reply::ConfiguredGcs => Ok(()),
            _ => Err(PullError::OutcomeUnknown(
                "unexpected GCS reader owner reply".into(),
            )),
        }
    }
    pub fn binding(
        &mut self,
        root: String,
        stop: &impl StopSignal,
    ) -> Result<Option<WorkspaceBinding>, PullError> {
        match self.call(Command::Binding(root), stop)? {
            Reply::Binding(result) => Ok(result),
            _ => Err(PullError::OutcomeUnknown("unexpected owner reply".into())),
        }
    }
    pub fn lookup(
        &mut self,
        attempt: grv_types::Uuid,
        root: String,
        stop: &impl StopSignal,
    ) -> Result<PullResolution, PullError> {
        match self.call(Command::Lookup(attempt, root), stop)? {
            Reply::Resolved(result) => Ok(result),
            _ => Err(PullError::OutcomeUnknown("unexpected owner reply".into())),
        }
    }
    pub fn preview(
        &mut self,
        identity: PullIdentity,
        targets: Vec<RelationName>,
        stop: &impl StopSignal,
    ) -> Result<PullPreview, PullError> {
        match self.call(Command::Preview(identity, targets), stop)? {
            Reply::Preview(result) => Ok(result),
            _ => Err(PullError::OutcomeUnknown("unexpected owner reply".into())),
        }
    }
    pub fn resolve(
        &mut self,
        identity: PullIdentity,
        stop: &impl StopSignal,
    ) -> Result<PullResolution, PullError> {
        match self.call(Command::Resolve(identity), stop)? {
            Reply::Resolved(result) => Ok(result),
            _ => Err(PullError::OutcomeUnknown(
                "unexpected pull owner reply".into(),
            )),
        }
    }
    pub fn prior_source_contracts(
        &mut self,
        identity: PullIdentity,
        tables: Vec<grv_types::Name>,
        stop: &impl StopSignal,
    ) -> Result<Vec<grv_adapter_api::NamedContract>, PullError> {
        match self.call(Command::PriorContracts(identity, tables), stop)? {
            Reply::PriorContracts(result) => Ok(result),
            _ => Err(PullError::OutcomeUnknown(
                "unexpected pull owner reply".into(),
            )),
        }
    }
    pub fn apply(
        &mut self,
        plan: PullPlan,
        stop: &impl StopSignal,
    ) -> Result<Box<PullReceipt>, PullError> {
        match self.call(Command::Apply(Box::new(plan)), stop)? {
            Reply::Applied(result) => Ok(result),
            _ => Err(PullError::OutcomeUnknown(
                "unexpected pull owner reply".into(),
            )),
        }
    }
    fn call(&mut self, command: Command, stop: &impl StopSignal) -> Result<Reply, PullError> {
        if stop.is_cancelled() {
            self.stop()?;
            return Err(PullError::OutcomeUnknown(
                "pull stopped before dispatch; resolve attempt under fenced ownership".into(),
            ));
        }
        self.sender
            .as_ref()
            .ok_or_else(|| io::Error::other("pull owner stopped"))?
            .send(command)
            .map_err(|_| io::Error::other("pull owner stopped"))?;
        loop {
            // Prefer a completed result over a subsequent cancellation.
            match self.receiver.try_recv() {
                Ok(result) => return result,
                Err(mpsc::TryRecvError::Disconnected) => {
                    return Err(PullError::OutcomeUnknown(
                        "pull owner stopped without terminal evidence".into(),
                    ));
                }
                Err(mpsc::TryRecvError::Empty) => {}
            }
            if stop.is_cancelled() {
                self.stop()?;
                return self.receiver.try_recv().unwrap_or_else(|_| {
                    Err(PullError::OutcomeUnknown(
                        "pull stopped; resolve attempt after writer fencing".into(),
                    ))
                });
            }
            match self.receiver.recv_timeout(Duration::from_millis(20)) {
                Ok(result) => return result,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(_) => {
                    return Err(PullError::OutcomeUnknown(
                        "pull owner stopped without terminal evidence".into(),
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
                .map_err(|_| io::Error::other("pull owner panicked"))
        })
    }
}
impl Drop for PullWorker {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

#[cfg(test)]
mod predispatch_tests {
    use super::*;
    use crate::lock::WorkspaceLock;
    use crate::pull::{ScopeMode, TransformMode, WriteMode};
    use grv_types::{Digest, Name, RequestedRevision, Uuid};
    use serde_json::json;

    fn identity() -> PullIdentity {
        PullIdentity {
            binding: WorkspaceBinding {
                canonical_root: "file:///private/grv".into(),
                workspace_id: Uuid::v4(),
            },
            attempt_id: Uuid::v4(),
            request_sha256: Digest::new("a".repeat(64)).unwrap(),
            adapter_identity: serde_json::from_value(json!({
                "name": "duckdb",
                "package_version": "0.1.0",
                "interface_version": 1,
                "binding_schema_version": 1
            }))
            .unwrap(),
            dataset: Name::new("data").unwrap(),
            target_schema: RelationName::new("app").unwrap(),
            write_mode: WriteMode::Replace,
            scope_mode: ScopeMode::Complete,
            transform_mode: TransformMode::Identity,
            requested_revision: RequestedRevision::Latest(grv_types::LatestRevision::Latest),
        }
    }

    /// Cancellation set BEFORE any command is dispatched: the owner is joined
    /// and the command never runs. The workspace is left exactly as found, so a
    /// fresh source-free resolution reports the true absence (Uninitialized),
    /// never a false NotCommitted, and no effect is retried.
    #[test]
    fn cancel_before_dispatch_joins_owner_and_refuses_without_effects() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("predispatch.duckdb");
        let identity = identity();
        let mut owner = PullWorker::open(&database, false).unwrap();
        assert!(WorkspaceLock::acquire(&database).is_err());
        // The stop signal is already cancelled before the first dispatch.
        let stop = Arc::new(AtomicBool::new(true));
        let error = owner
            .lookup(
                identity.attempt_id.clone(),
                identity.binding.canonical_root.clone(),
                stop.as_ref(),
            )
            .unwrap_err();
        assert!(
            matches!(
                error,
                PullError::OutcomeUnknown(ref message) if message.contains("stopped before dispatch")
            ),
            "{error:?}"
        );
        // The owner thread has joined: the command channel and owner handle are
        // empty, and the workspace lock is released under the real WorkspaceLock.
        assert!(owner.sender.is_none());
        assert!(owner.owner.is_none());
        assert!(WorkspaceLock::acquire(&database).is_ok());
        // A stopped owner is not reused: a second dispatch is refused the same way.
        let again = owner
            .lookup(
                identity.attempt_id.clone(),
                identity.binding.canonical_root.clone(),
                stop.as_ref(),
            )
            .unwrap_err();
        assert!(matches!(again, PullError::OutcomeUnknown(_)), "{again:?}");
        drop(owner);
        // The lookup closure never ran: a fresh source-free resolution reports the
        // true absence, not a false NotCommitted, and no effect is re-attempted.
        let mut store = PullStore::open(&database).unwrap();
        assert_eq!(
            store.resolve(&identity).unwrap(),
            PullResolution::Uninitialized
        );
    }
}
