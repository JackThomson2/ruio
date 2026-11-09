//! An engine that parks and runs no I/O: [`Parker`].
//!
//! The thread sleeps on a condition variable of the standard library until
//! another thread unparks it or its timer is due, so it runs wherever the
//! standard library does. It is the engine of the systems ruio has no I/O
//! engine for yet, on which the executor runs tasks, timers and channels
//! and nothing does I/O; and, as the smallest engine that keeps the
//! contract, where a new engine can start from.

use std::cell::Cell;
use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use crate::{Build, Engine, LocalWaker, Unpark};

/// The ID of the next parker.
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// The point the parkers' clock counts from: its first reading.
static EPOCH: OnceLock<Instant> = OnceLock::new();

/// An engine that parks the thread and runs no I/O; see the
/// [module](self).
///
/// Like any engine, it belongs to the thread that made it.
///
/// ```compile_fail
/// fn assert_send<T: Send>() {}
/// assert_send::<ruio_engine::park::Parker>();
/// ```
pub struct Parker {
    inner: Rc<Inner>,
}

struct Inner {
    id: u64,
    notify: Arc<Notify>,
    /// The deadline the timer is armed for, if it is.
    armed: Cell<Option<Duration>>,
    /// Whether the timer has fired since `timer_fired` last asked.
    fired: Cell<bool>,
}

/// What a parker shares with its unparkers.
///
/// An unpark sets `notified` and, if it was clear, takes the lock and
/// signals the condition variable. A park clears the flag with the lock
/// held, and waits only if it was clear, releasing the lock as it waits:
/// an unpark that sets the flag after the park found it clear gets the lock
/// only once the park waits, so its signal is never lost.
#[derive(Debug, Default)]
struct Notify {
    notified: AtomicBool,
    lock: Mutex<()>,
    condvar: Condvar,
}

impl Notify {
    fn unpark(&self) {
        // Once the flag is set, the park that will clear it is either still
        // to look at it or has been signalled by whoever set it.
        if !self.notified.swap(true, Ordering::SeqCst) {
            drop(self.lock.lock().unwrap_or_else(PoisonError::into_inner));
            self.condvar.notify_one();
        }
    }
}

impl Parker {
    fn new() -> Self {
        Self {
            inner: Rc::new(Inner {
                id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
                notify: Arc::default(),
                armed: Cell::new(None),
                fired: Cell::new(false),
            }),
        }
    }

    /// Records that the timer fired, if its deadline has passed.
    fn check_timer(&self) {
        if let Some(deadline) = self.inner.armed.get()
            && deadline <= Self::monotonic_now()
        {
            self.inner.armed.set(None);
            self.inner.fired.set(true);
        }
    }
}

impl Engine for Parker {
    type Builder = Builder;
    type Unparker = Unparker;
    type Entered = Entered;

    fn id(&self) -> u64 {
        self.inner.id
    }

    fn monotonic_now() -> Duration {
        let epoch = *EPOCH.get_or_init(Instant::now);
        Instant::now().saturating_duration_since(epoch)
    }

    fn submit(&self) -> io::Result<()> {
        self.check_timer();
        Ok(())
    }

    fn has_work(&self) -> bool {
        self.inner.notify.notified.load(Ordering::Acquire)
            || self
                .inner
                .armed
                .get()
                .is_some_and(|deadline| deadline <= Self::monotonic_now())
    }

    fn park(&self, timeout: Option<Duration>) -> io::Result<()> {
        let after = timeout.map(|timeout| Self::monotonic_now().saturating_add(timeout));
        let until = match (after, self.inner.armed.get()) {
            (Some(after), Some(armed)) => Some(after.min(armed)),
            (after, armed) => after.or(armed),
        };
        let notify = &self.inner.notify;
        let mut guard = notify.lock.lock().unwrap_or_else(PoisonError::into_inner);
        while !notify.notified.swap(false, Ordering::SeqCst) {
            guard = match until {
                None => notify
                    .condvar
                    .wait(guard)
                    .unwrap_or_else(PoisonError::into_inner),
                Some(until) => {
                    let left = until.saturating_sub(Self::monotonic_now());
                    if left.is_zero() {
                        break;
                    }
                    notify
                        .condvar
                        .wait_timeout(guard, left)
                        .unwrap_or_else(PoisonError::into_inner)
                        .0
                }
            };
        }
        drop(guard);
        self.check_timer();
        Ok(())
    }

    fn wake_at(&self, deadline: Duration) -> io::Result<()> {
        let armed = &self.inner.armed;
        if armed.get().is_none_or(|armed| deadline < armed) {
            armed.set(Some(deadline));
        }
        Ok(())
    }

    fn timer_fired(&self) -> bool {
        self.inner.fired.replace(false)
    }

    fn unparker(&self) -> Unparker {
        Unparker {
            notify: Arc::clone(&self.inner.notify),
        }
    }

    fn enter(&self) -> Entered {
        Entered {
            _thread: PhantomData,
        }
    }

    /// Does nothing: a parker holds no wakers.
    unsafe fn set_local_waker(&self, _local: LocalWaker) {}
}

impl fmt::Debug for Parker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Parker")
            .field("id", &self.inner.id)
            .finish_non_exhaustive()
    }
}

/// Configures and creates a [`Parker`], which has nothing to configure.
#[derive(Clone, Debug, Default)]
pub struct Builder {
    _private: (),
}

impl Build for Builder {
    type Engine = Parker;

    /// Changes nothing: a parker has no operations in flight to wait for.
    fn shutdown_timeout(self, _timeout: Duration) -> Self {
        self
    }

    fn build(&self) -> io::Result<Parker> {
        Ok(Parker::new())
    }
}

/// Wakes a [`Parker`] from any thread.
#[derive(Clone, Debug)]
pub struct Unparker {
    notify: Arc<Notify>,
}

impl Unpark for Unparker {
    fn unpark(&self) {
        self.notify.unpark();
    }
}

/// Keeps a [`Parker`] the thread's current engine until dropped. Nothing
/// looks for a parker as the current engine, so this does nothing else.
#[derive(Debug)]
#[must_use = "the engine is current only until the guard is dropped"]
pub struct Entered {
    _thread: PhantomData<Rc<()>>,
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::{Builder, Parker};
    use crate::{Build, Engine};

    #[test]
    fn a_parker_keeps_the_contract() {
        crate::conformance::check::<Parker>(&Builder::default());
    }

    #[test]
    fn a_park_ends_at_the_earlier_of_its_timeout_and_the_timer() {
        let parker = Builder::default().build().unwrap();
        parker
            .wake_at(Parker::monotonic_now() + Duration::from_secs(3600))
            .unwrap();
        let started = Instant::now();
        parker.park(Some(Duration::from_millis(10))).unwrap();
        assert!(started.elapsed() >= Duration::from_millis(10));
        assert!(started.elapsed() < Duration::from_secs(60));
        assert!(!parker.timer_fired());
        assert!(!parker.has_work());
    }
}
