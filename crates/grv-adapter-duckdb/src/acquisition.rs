//! One native connection owner for an entire multi-table acquisition. Fetch and
//! IPC encoding occur only on demand; interrupt is independent of this queue.
use crate::{
    conversion::Conversion,
    journal::AcquisitionJournal,
    native::{AcquiredSchema, NativeEngine, NativeInterrupt, SourceSelection},
    worker::{Engine, Interrupt},
};
use arrow_schema::Schema;
use grv_adapter_api::Resources;
use grv_adapter_sdk::StopToken;
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

enum Command {
    Acquire {
        sources: Vec<SourceSelection>,
        schemas: Vec<Arc<Schema>>,
        journal: Arc<AcquisitionJournal>,
    },
    Fetch {
        table: usize,
        resources: Resources,
    },
}
enum Reply {
    Acquired(Vec<AcquiredSchema>),
    Batch(Option<(Vec<u8>, u64)>),
}
pub struct AcquisitionWorker {
    sender: Option<mpsc::SyncSender<Command>>,
    receiver: mpsc::Receiver<io::Result<Reply>>,
    interrupt: NativeInterrupt,
    stopping: Arc<AtomicBool>,
    owner: Option<thread::JoinHandle<()>>,
}
impl AcquisitionWorker {
    pub fn open_readonly(path: &Path) -> io::Result<Self> {
        let path = path.to_owned();
        let (sender, commands) = mpsc::sync_channel(1);
        let (reply, receiver) = mpsc::sync_channel(1);
        let (ready, initialized) = mpsc::sync_channel(1);
        let stopping = Arc::new(AtomicBool::new(false));
        let stop = stopping.clone();
        let owner=thread::Builder::new().name("grv-duckdb-snapshot-owner".into()).spawn(move|| {
            let mut engine=match NativeEngine::open_readonly(&path) {Ok(engine)=>engine,Err(error)=>{let _=ready.send(Err(error));return;}};
            if ready.send(Ok(engine.interrupt_handle())).is_err(){return;}
            let mut acquired=Vec::new();
            let mut output_schemas=Vec::new();
            while let Ok(command)=commands.recv() {
                if stop.load(Ordering::Acquire){break;}
                let result=match command {
                    Command::Acquire {sources,schemas,journal} => {
                        if sources.len()!=schemas.len() {Err(io::Error::new(io::ErrorKind::InvalidInput,"acquisition schema/member count mismatch"))}
                        else {
                            engine.acquire_sources(&sources,&journal).and_then(|members| {
                                for (member,schema) in members.iter().zip(&schemas) {
                                    if member.types.len()!=schema.fields().len(){return Err(io::Error::other("source projection differs from output contract"));}
                                    for (source,field) in member.types.iter().zip(schema.fields()) {Conversion::prepare(source.clone(),field.data_type()).map_err(io::Error::other)?;}
                                }
                                acquired=members.clone();output_schemas=schemas;Ok(Reply::Acquired(members))
                            })
                        }
                    }
                    Command::Fetch {table,resources} => {
                        if table>=acquired.len(){Err(io::Error::new(io::ErrorKind::InvalidInput,"unacquired table fetch"))}
                        else if acquired[table].rows == 0 {Ok(Reply::Batch(None))}
                        else {
                            let batch=(resources.max_batch_bytes.get() as usize).min(crate::MAX_BATCH_BYTES);
                            let source=(resources.max_source_unit_bytes.get() as usize).min(crate::SOURCE_BUDGET_BYTES);
                            let scratch=(resources.max_scratch_bytes.get() as usize).min(crate::SCRATCH_BUDGET_BYTES);
                            // Leave room for IPC framing/metadata and conversion copies.
                            let native_allowance=crate::ipc::native_allowance(&output_schemas[table],&acquired[table].types,batch,scratch).unwrap_or(0);
                            if native_allowance<256 {Err(io::Error::other("negotiated resource budget cannot fit one encoded row"))}
                            else {engine.fetch_acquired(&acquired[table],native_allowance,source).and_then(|window| match window {
                                None=>Ok(Reply::Batch(None)),Some(window)=>crate::ipc::encode(&window,output_schemas[table].clone(),batch,scratch).map(|bytes|Reply::Batch(Some((bytes,window.row_count())))),
                            })}
                        }
                    }
                };
                if stop.load(Ordering::Acquire)||reply.send(result).is_err(){break;}
            }
            // Engine/result/transaction close here, on their one owning thread.
        })?;
        let interrupt = initialized
            .recv()
            .map_err(|_| io::Error::other("snapshot owner initialization failed"))??;
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
    pub fn acquire(
        &mut self,
        sources: Vec<SourceSelection>,
        schemas: Vec<Arc<Schema>>,
        journal: Arc<AcquisitionJournal>,
        stop: &StopToken,
    ) -> io::Result<Vec<AcquiredSchema>> {
        match self.call(
            Command::Acquire {
                sources,
                schemas,
                journal,
            },
            stop,
        )? {
            Reply::Acquired(members) => Ok(members),
            _ => Err(io::Error::other("unexpected snapshot owner reply")),
        }
    }
    pub fn fetch(
        &mut self,
        table: usize,
        resources: &Resources,
        stop: &StopToken,
    ) -> io::Result<Option<(Vec<u8>, u64)>> {
        match self.call(
            Command::Fetch {
                table,
                resources: resources.clone(),
            },
            stop,
        )? {
            Reply::Batch(batch) => Ok(batch),
            _ => Err(io::Error::other("unexpected snapshot owner reply")),
        }
    }
    fn call(&mut self, command: Command, stop: &StopToken) -> io::Result<Reply> {
        if stop.is_cancelled() {
            self.stop()?;
            return Err(io::Error::other("snapshot work stopped"));
        }
        self.sender
            .as_ref()
            .ok_or_else(|| io::Error::other("snapshot owner stopped"))?
            .send(command)
            .map_err(|_| io::Error::other("snapshot owner stopped"))?;
        loop {
            if stop.is_cancelled() {
                self.stop()?;
                return Err(io::Error::other("snapshot work stopped"));
            }
            match self.receiver.recv_timeout(Duration::from_millis(20)) {
                Ok(result) => return result,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(_) => return Err(io::Error::other("snapshot owner stopped")),
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
                .map_err(|_| io::Error::other("snapshot owner panicked"))
        })
    }
}
impl Drop for AcquisitionWorker {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}
