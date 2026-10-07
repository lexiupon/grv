//! Scoped renewal for blocking IO. Dropping the stop sender also covers an
//! unwinding operation, so scope teardown cannot wait on a sleeping worker.
use crate::store::{Result, public_error};
use grv_types::ErrorCode;
use std::{
    io::Read,
    sync::{Mutex, mpsc},
    time::Duration,
};

pub(crate) struct Watch(Mutex<Option<grv_types::PublicError>>);
impl Watch {
    pub fn check(&self) -> Result<()> {
        match self.0.lock().unwrap().as_ref() {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }
    pub fn reader<'a, R: Read + ?Sized>(&'a self, source: &'a mut R) -> impl Read + 'a {
        struct Guard<'a, R: ?Sized> {
            watch: &'a Watch,
            source: &'a mut R,
        }
        impl<R: Read + ?Sized> Read for Guard<'_, R> {
            fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
                self.watch
                    .check()
                    .map_err(|_| std::io::Error::other("ownership renewal failed"))?;
                self.source.read(buffer)
            }
        }
        Guard {
            watch: self,
            source,
        }
    }
}

struct Stop(mpsc::Sender<()>);
impl Drop for Stop {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

pub(crate) fn during<R: Send, T>(
    mut owner: R,
    interval: Duration,
    mut renew: impl FnMut(&mut R) -> Result<()> + Send,
    operation: impl FnOnce(&Watch) -> Result<T>,
) -> Result<(T, R)> {
    let (sender, receiver) = mpsc::channel();
    let watch = Watch(Mutex::new(None));
    std::thread::scope(|scope| {
        let watched = &watch;
        let worker = scope.spawn(move || -> Result<R> {
            while matches!(
                receiver.recv_timeout(interval),
                Err(mpsc::RecvTimeoutError::Timeout)
            ) {
                if let Err(error) = renew(&mut owner) {
                    *watched.0.lock().unwrap() = Some(error.clone());
                    return Err(error);
                }
            }
            Ok(owner)
        });
        let stop = Stop(sender);
        let result = operation(&watch);
        drop(stop);
        let owner = worker.join().map_err(|_| {
            public_error(ErrorCode::BackendFailure, "ownership renewal worker failed")
        })??;
        Ok((result?, owner))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    #[test]
    fn blocking_work_renews_and_returns_updated_authority() {
        let seen = Arc::new(AtomicUsize::new(0));
        let observed = seen.clone();
        let (_, owner) = during(
            0usize,
            Duration::from_millis(1),
            move |owner| {
                *owner += 1;
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
            |_| {
                while seen.load(Ordering::SeqCst) < 2 {
                    std::thread::yield_now();
                }
                Ok(())
            },
        )
        .unwrap();
        assert!(owner >= 2);
    }
    #[test]
    fn renewal_failure_prevents_success_and_panics_stop_scope_workers() {
        let seen = Arc::new(AtomicUsize::new(0));
        let observed = seen.clone();
        let error = during(
            (),
            Duration::from_millis(1),
            move |_| {
                observed.store(1, Ordering::SeqCst);
                Err(public_error(ErrorCode::OwnershipLost, "injected fence"))
            },
            |_| {
                while seen.load(Ordering::SeqCst) == 0 {
                    std::thread::yield_now();
                }
                Ok(())
            },
        )
        .unwrap_err();
        assert_eq!(error.code, ErrorCode::OwnershipLost);
        assert!(
            std::panic::catch_unwind(|| {
                let _: Result<((), ())> = during(
                    (),
                    Duration::from_secs(900),
                    |_| Ok(()),
                    |_| panic!("injected unwind"),
                );
            })
            .is_err()
        );
    }
}
