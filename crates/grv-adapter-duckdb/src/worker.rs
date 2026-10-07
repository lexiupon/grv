//! One connection owner, with demand-driven fetches and independently safe interruption.
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
};

pub trait Interrupt: Clone + Send + Sync + 'static {
    fn interrupt(&self);
}

/// Implementations create and own their connection on the worker thread. A fetch must
/// enforce its byte allowance before allocating converted output, including large cells.
pub trait Engine: 'static {
    type Interrupt: Interrupt;
    fn interrupt_handle(&self) -> Self::Interrupt;
    fn begin(&mut self, query: &str) -> io::Result<()>;
    fn fetch(&mut self, allowance: usize) -> io::Result<Option<Vec<u8>>>;
}

enum Command {
    Begin(String),
    Fetch(usize),
}
enum Reply {
    Begun,
    Chunk(Option<Vec<u8>>),
}

pub struct Worker<I: Interrupt> {
    sender: Option<mpsc::SyncSender<Command>>,
    receiver: mpsc::Receiver<io::Result<Reply>>,
    interrupt: I,
    stopping: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl<I: Interrupt> Worker<I> {
    pub fn spawn<E, F>(create: F) -> io::Result<Self>
    where
        E: Engine<Interrupt = I>,
        F: FnOnce() -> io::Result<E> + Send + 'static,
    {
        let (sender, commands) = mpsc::sync_channel(1);
        let (replies, receiver) = mpsc::sync_channel(1);
        let (ready, initialized) = mpsc::sync_channel(1);
        let stopping = Arc::new(AtomicBool::new(false));
        let stop_worker = Arc::clone(&stopping);
        let thread = thread::Builder::new()
            .name("grv-duckdb-owner".into())
            .spawn(move || {
                let mut engine = match create() {
                    Ok(engine) => engine,
                    Err(error) => {
                        let _ = ready.send(Err(error));
                        return;
                    }
                };
                if ready.send(Ok(engine.interrupt_handle())).is_err() {
                    return;
                }
                while let Ok(command) = commands.recv() {
                    if stop_worker.load(Ordering::Acquire) {
                        break;
                    }
                    let result = match command {
                        Command::Begin(query) => engine.begin(&query).map(|()| Reply::Begun),
                        Command::Fetch(allowance) => engine.fetch(allowance).and_then(|chunk| {
                            if chunk.as_ref().is_some_and(|bytes| bytes.len() > allowance) {
                                Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "engine exceeded fetch allowance",
                                ))
                            } else {
                                Ok(Reply::Chunk(chunk))
                            }
                        }),
                    };
                    if stop_worker.load(Ordering::Acquire) {
                        break;
                    }
                    if replies.send(result).is_err() {
                        break;
                    }
                }
                // The connection and result are destroyed on their owning thread.
            })?;
        let interrupt = initialized
            .recv()
            .map_err(|_| io::Error::other("native worker initialization failed"))??;
        Ok(Self {
            sender: Some(sender),
            receiver,
            interrupt,
            stopping,
            thread: Some(thread),
        })
    }

    pub fn interrupt_handle(&self) -> I {
        self.interrupt.clone()
    }

    pub fn begin(&mut self, query: &str) -> io::Result<()> {
        match self.call(Command::Begin(query.to_owned()))? {
            Reply::Begun => Ok(()),
            _ => Err(io::Error::other("unexpected worker reply")),
        }
    }

    /// A caller grants one fetch only after it reserves one outbound credit.
    /// There is no producer queue and no fetch while waiting for a credit.
    pub fn fetch(&mut self, allowance: usize) -> io::Result<Option<Vec<u8>>> {
        if allowance == 0 || allowance > crate::MAX_BATCH_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid fetch allowance",
            ));
        }
        match self.call(Command::Fetch(allowance))? {
            Reply::Chunk(chunk) => Ok(chunk),
            _ => Err(io::Error::other("unexpected worker reply")),
        }
    }

    fn call(&mut self, command: Command) -> io::Result<Reply> {
        self.sender
            .as_ref()
            .ok_or_else(|| io::Error::other("worker stopped"))?
            .send(command)
            .map_err(|_| io::Error::other("worker stopped"))?;
        self.receiver
            .recv()
            .map_err(|_| io::Error::other("worker stopped"))?
    }

    /// Returns only after the engine owner and connection have stopped.
    pub fn stop(&mut self) -> io::Result<()> {
        self.stopping.store(true, Ordering::Release);
        self.interrupt.interrupt();
        self.sender.take();
        self.thread.take().map_or(Ok(()), |thread| {
            thread
                .join()
                .map_err(|_| io::Error::other("engine worker panicked"))
        })
    }
}

impl<I: Interrupt> Drop for Worker<I> {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    #[derive(Clone)]
    struct Stop(Arc<AtomicBool>);
    impl Interrupt for Stop {
        fn interrupt(&self) {
            self.0.store(true, Ordering::Release);
        }
    }
    struct Counter {
        fetched: Arc<AtomicUsize>,
        closed: Arc<AtomicBool>,
        stop: Stop,
    }
    impl Engine for Counter {
        type Interrupt = Stop;
        fn interrupt_handle(&self) -> Stop {
            self.stop.clone()
        }
        fn begin(&mut self, _: &str) -> io::Result<()> {
            Ok(())
        }
        fn fetch(&mut self, _: usize) -> io::Result<Option<Vec<u8>>> {
            self.fetched.fetch_add(1, Ordering::Relaxed);
            Ok(Some(vec![1]))
        }
    }
    impl Drop for Counter {
        fn drop(&mut self) {
            self.closed.store(true, Ordering::Release);
        }
    }
    #[test]
    fn fetch_is_demand_driven_and_stop_joins_owner() {
        let fetched = Arc::new(AtomicUsize::new(0));
        let count = fetched.clone();
        let closed = Arc::new(AtomicBool::new(false));
        let observed = closed.clone();
        let mut worker = Worker::spawn(move || {
            Ok(Counter {
                fetched: count,
                closed,
                stop: Stop(Arc::new(AtomicBool::new(false))),
            })
        })
        .unwrap();
        worker.begin("select 1").unwrap();
        assert_eq!(fetched.load(Ordering::Relaxed), 0);
        assert_eq!(worker.fetch(1).unwrap(), Some(vec![1]));
        assert_eq!(fetched.load(Ordering::Relaxed), 1);
        assert!(worker.fetch(crate::MAX_BATCH_BYTES + 1).is_err());
        worker.stop().unwrap();
        assert!(observed.load(Ordering::Acquire));
    }
}
