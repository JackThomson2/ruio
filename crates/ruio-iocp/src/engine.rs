//! The engine: a completion port, the thread's current engine, and turning
//! the port.

use std::cell::{Cell, RefCell};
use std::fmt;
use std::io;
use std::mem;
use std::ptr;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use ruio_engine::{Build, Engine, LocalWaker, Unpark};

use crate::sys;

/// The completion key of the packets unparks post.
const WAKE: usize = 0;

/// The most completions a turn takes from the port at once.
const BATCH: usize = 64;

/// The ID of the next engine.
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// The point the engines' clock counts from: its first reading.
static EPOCH: OnceLock<Instant> = OnceLock::new();

thread_local! {
    /// The engine the thread entered last, for what is made on the thread
    /// without being handed one.
    static CURRENT: RefCell<Option<Iocp>> = const { RefCell::new(None) };
}

/// An engine on an I/O completion port; see the [crate] documentation.
///
/// Like any engine, it belongs to the thread that made it. A clone is the
/// same engine.
///
/// ```compile_fail
/// fn assert_send<T: Send>() {}
/// assert_send::<ruio_iocp::Iocp>();
/// ```
#[derive(Clone)]
pub struct Iocp {
    inner: Rc<Inner>,
}

struct Inner {
    id: u64,
    shared: Arc<Shared>,
    /// Completions that a check for work took from the port, which the next
    /// turn delivers.
    stash: RefCell<Vec<sys::OverlappedEntry>>,
    /// The deadline the timer is armed for, if it is.
    armed: Cell<Option<Duration>>,
    /// Whether the timer has fired since `timer_fired` last asked.
    fired: Cell<bool>,
}

/// What an engine shares with its unparkers: the port, which the last of
/// them to go closes, so that an unpark never posts to a handle that has
/// been closed, whose value may name another object by then.
struct Shared {
    port: sys::Handle,
    /// Whether an unpark has posted a packet that no turn has taken yet. An
    /// unpark posts only if it sets this, so that several before a turn
    /// wake it once.
    notified: AtomicBool,
}

// SAFETY: a completion port's handle may be used from any thread, and the
// flag is atomic.
unsafe impl Send for Shared {}

// SAFETY: as for `Send`; several threads posting to one port at once is
// what ports are for.
unsafe impl Sync for Shared {}

impl Shared {
    fn unpark(&self) {
        if self.notified.swap(true, Ordering::SeqCst) {
            return;
        }
        // SAFETY: the port is open while `self` is.
        let posted =
            unsafe { sys::PostQueuedCompletionStatus(self.port, 0, WAKE, ptr::null_mut()) };
        if posted == 0 {
            // No packet is on its way to clear the flag, so clear it here,
            // for the next unpark to try again.
            self.notified.store(false, Ordering::SeqCst);
        }
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        // SAFETY: the port is open, and nothing can use it once its last
        // holder is gone.
        unsafe { sys::CloseHandle(self.port) };
    }
}

/// The time on the engines' clock: a monotonic one, counted from its first
/// reading.
fn now() -> Duration {
    let epoch = *EPOCH.get_or_init(Instant::now);
    Instant::now().saturating_duration_since(epoch)
}

/// How many milliseconds there are until `deadline`, a time on the engines'
/// clock, rounded up so that a wait for them does not end early, and short
/// of `INFINITE`.
fn milliseconds_until(deadline: Duration) -> u32 {
    let left = deadline
        .saturating_sub(now())
        .as_nanos()
        .div_ceil(1_000_000);
    u32::try_from(left).map_or(sys::INFINITE - 1, |ms| ms.min(sys::INFINITE - 1))
}

impl Iocp {
    fn new() -> io::Result<Self> {
        // SAFETY: given no file and no port, this makes a port, for one
        // thread, or fails and returns null.
        let port = unsafe {
            sys::CreateIoCompletionPort(sys::INVALID_HANDLE_VALUE, ptr::null_mut(), 0, 1)
        };
        if port.is_null() {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            inner: Rc::new(Inner {
                id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
                shared: Arc::new(Shared {
                    port,
                    notified: AtomicBool::new(false),
                }),
                stash: RefCell::default(),
                armed: Cell::new(None),
                fired: Cell::new(false),
            }),
        })
    }

    /// The engine the thread has entered, as a runtime enters its own while
    /// it runs, or `None` if it has entered none.
    pub fn current() -> Option<Self> {
        CURRENT
            .try_with(|current| current.borrow().clone())
            .ok()
            .flatten()
    }
}

impl Inner {
    /// Takes up to [`BATCH`] completions from the port into `entries`,
    /// waiting up to `milliseconds` for the first, and says how many.
    fn dequeue(
        &self,
        entries: &mut [sys::OverlappedEntry; BATCH],
        milliseconds: u32,
    ) -> io::Result<usize> {
        let mut removed: u32 = 0;
        // SAFETY: the port is open, and `entries` has room for the count it
        // is given.
        let ok = unsafe {
            sys::GetQueuedCompletionStatusEx(
                self.shared.port,
                entries.as_mut_ptr(),
                BATCH as u32,
                &mut removed,
                milliseconds,
                0,
            )
        };
        if ok != 0 {
            return Ok(removed as usize);
        }
        match sys::GetLastError() {
            sys::WAIT_TIMEOUT => Ok(0),
            code => Err(io::Error::from_raw_os_error(code as i32)),
        }
    }

    /// Delivers completions taken from the port.
    fn deliver(&self, entries: &[sys::OverlappedEntry]) {
        for entry in entries {
            if entry.completion_key == WAKE {
                self.shared.notified.store(false, Ordering::SeqCst);
            }
        }
    }

    /// Delivers what a check for work took from the port and what the port
    /// has, waiting for a completion until `deadline`, a time on the
    /// engines' clock, or for ever if there is none, unless the check took
    /// some already.
    fn turn(&self, deadline: Option<Duration>) -> io::Result<()> {
        let stashed = mem::take(&mut *self.stash.borrow_mut());
        let deadline = if stashed.is_empty() {
            deadline
        } else {
            self.deliver(&stashed);
            Some(Duration::ZERO)
        };
        let mut entries = [sys::OverlappedEntry::EMPTY; BATCH];
        loop {
            let milliseconds = deadline.map_or(sys::INFINITE, milliseconds_until);
            let taken = self.dequeue(&mut entries, milliseconds)?;
            self.deliver(&entries[..taken]);
            // A wait may end a little before its timeout, at a tick of
            // Windows' timer, so one that took nothing waits out the rest.
            if taken > 0 || milliseconds == 0 {
                break;
            }
        }
        self.check_timer();
        Ok(())
    }

    /// Records that the timer fired, if its deadline has passed.
    fn check_timer(&self) {
        if let Some(deadline) = self.armed.get()
            && deadline <= now()
        {
            self.armed.set(None);
            self.fired.set(true);
        }
    }
}

impl Engine for Iocp {
    type Builder = Builder;
    type Unparker = Unparker;
    type Entered = Entered;

    fn id(&self) -> u64 {
        self.inner.id
    }

    fn monotonic_now() -> Duration {
        now()
    }

    fn submit(&self) -> io::Result<()> {
        self.inner.turn(Some(Duration::ZERO))
    }

    fn has_work(&self) -> bool {
        let inner = &self.inner;
        if inner.shared.notified.load(Ordering::SeqCst)
            || !inner.stash.borrow().is_empty()
            || inner.armed.get().is_some_and(|deadline| deadline <= now())
        {
            return true;
        }
        let mut entries = [sys::OverlappedEntry::EMPTY; BATCH];
        match inner.dequeue(&mut entries, 0) {
            Ok(0) => false,
            Ok(taken) => {
                inner
                    .stash
                    .borrow_mut()
                    .extend_from_slice(&entries[..taken]);
                true
            }
            // The next turn meets the error again, and reports it.
            Err(_) => true,
        }
    }

    fn park(&self, timeout: Option<Duration>) -> io::Result<()> {
        let after = timeout.map(|timeout| now().saturating_add(timeout));
        let until = match (after, self.inner.armed.get()) {
            (Some(after), Some(armed)) => Some(after.min(armed)),
            (after, armed) => after.or(armed),
        };
        self.inner.turn(until)
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
            shared: Arc::clone(&self.inner.shared),
        }
    }

    fn enter(&self) -> Entered {
        let previous = CURRENT
            .try_with(|current| current.replace(Some(self.clone())))
            .ok()
            .flatten();
        Entered { previous }
    }

    /// Does nothing: the engine clones the wakers it holds.
    unsafe fn set_local_waker(&self, _local: LocalWaker) {}
}

impl fmt::Debug for Iocp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Iocp")
            .field("id", &self.inner.id)
            .finish_non_exhaustive()
    }
}

/// Configures and creates an [`Iocp`].
#[derive(Clone, Debug, Default)]
pub struct Builder {
    _private: (),
}

impl Builder {
    /// A builder of the defaults.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Build for Builder {
    type Engine = Iocp;

    /// Changes nothing yet: the engine has no operations in flight to wait
    /// for.
    fn shutdown_timeout(self, _timeout: Duration) -> Self {
        self
    }

    fn build(&self) -> io::Result<Iocp> {
        Iocp::new()
    }
}

/// Wakes an [`Iocp`] from any thread, by posting a packet to its port.
#[derive(Clone)]
pub struct Unparker {
    shared: Arc<Shared>,
}

impl Unpark for Unparker {
    fn unpark(&self) {
        self.shared.unpark();
    }
}

impl fmt::Debug for Unparker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Unparker").finish_non_exhaustive()
    }
}

/// Keeps an [`Iocp`] the thread's current engine until dropped, when the
/// engine that was current before it is current again.
#[must_use = "the engine is current only until the guard is dropped"]
pub struct Entered {
    previous: Option<Iocp>,
}

impl Drop for Entered {
    fn drop(&mut self) {
        let previous = self.previous.take();
        let left = CURRENT
            .try_with(|current| current.replace(previous))
            .ok()
            .flatten();
        // Dropped once the thread's cell is let go of, as dropping the last
        // clone of an engine turns it.
        drop(left);
    }
}

impl fmt::Debug for Entered {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Entered").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use ruio_engine::{Build, Engine, Unpark};

    use super::{Builder, Iocp};

    #[test]
    fn the_engine_keeps_the_contract() {
        ruio_engine::conformance::check::<Iocp>(&Builder::new());
    }

    #[test]
    fn a_park_after_a_check_for_work_returns_at_once_and_once() {
        let engine = Builder::new().build().unwrap();
        engine.unparker().unpark();
        assert!(engine.has_work());
        engine.park(None).unwrap();
        assert!(!engine.has_work());
        let started = Instant::now();
        engine.park(Some(Duration::from_millis(20))).unwrap();
        assert!(started.elapsed() >= Duration::from_millis(20));
    }

    #[test]
    fn a_park_ends_at_the_earlier_of_its_timeout_and_the_timer() {
        let engine = Builder::new().build().unwrap();
        engine
            .wake_at(Iocp::monotonic_now() + Duration::from_secs(3600))
            .unwrap();
        let started = Instant::now();
        engine.park(Some(Duration::from_millis(10))).unwrap();
        assert!(started.elapsed() >= Duration::from_millis(10));
        assert!(started.elapsed() < Duration::from_secs(60));
        assert!(!engine.timer_fired());
    }

    #[test]
    fn entering_makes_an_engine_current_until_the_guard_drops() {
        assert!(Iocp::current().is_none());
        let outer = Builder::new().build().unwrap();
        let inner = Builder::new().build().unwrap();
        let entered = outer.enter();
        {
            let _inner = inner.enter();
            assert_eq!(Iocp::current().map(|e| e.id()), Some(inner.id()));
        }
        assert_eq!(Iocp::current().map(|e| e.id()), Some(outer.id()));
        drop(entered);
        assert!(Iocp::current().is_none());
    }
}
