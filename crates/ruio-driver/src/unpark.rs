//! Waking a parked driver from other threads.

use std::fs::File;
use std::io::{self, Write as _};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

const EMPTY: u8 = 0;
const PARKED: u8 = 1;
const NOTIFIED: u8 = 2;

/// The parking state a driver shares with its [`Unparker`]s.
///
/// `state` records whether the driver is parked and whether an unpark is
/// waiting for it. A parked driver has a poll on `eventfd` in flight, which a
/// write completes.
///
/// Parking and unparking each change the state with one read-modify-write,
/// which are totally ordered, so one of the two always sees the other: an
/// unparker that finds the driver parked writes the eventfd, and a driver
/// that finds an unpark waiting does not wait. A park that ends puts the
/// state back with a plain store; an unpark that came meanwhile has written
/// the eventfd already, and what it announces, work queued elsewhere, has a
/// flag of its own that the driver's loop checks before it parks again.
#[derive(Debug)]
pub(crate) struct Notify {
    state: AtomicU8,
    eventfd: File,
}

impl Notify {
    pub(crate) fn new() -> io::Result<Self> {
        // SAFETY: eventfd creates a descriptor and touches no memory.
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `fd` is a new descriptor that nothing else owns.
        let eventfd = File::from(unsafe { OwnedFd::from_raw_fd(fd) });
        Ok(Self {
            state: AtomicU8::new(EMPTY),
            eventfd,
        })
    }

    pub(crate) fn eventfd(&self) -> RawFd {
        self.eventfd.as_raw_fd()
    }

    /// Records that the driver is about to park, and returns `true`; or, if
    /// an unpark is waiting, consumes it and returns `false`.
    pub(crate) fn begin_park(&self) -> bool {
        if self.state.swap(PARKED, Ordering::SeqCst) == EMPTY {
            return true;
        }
        // An unpark that comes between the swap and the store finds the
        // driver parked and writes the eventfd, which the next park's poll
        // finds readable: a wake for nothing, and no wake lost.
        self.state.store(EMPTY, Ordering::SeqCst);
        false
    }

    /// Records that the driver is no longer parked. An unpark that arrived
    /// meanwhile found it parked, and has written the eventfd.
    pub(crate) fn end_park(&self) {
        self.state.store(EMPTY, Ordering::Release);
    }

    fn unpark(&self) {
        // Only a parked driver needs the system call. Otherwise the state
        // alone makes its next park return.
        if self.state.swap(NOTIFIED, Ordering::SeqCst) == PARKED {
            // The write completes the driver's poll on the eventfd. It can
            // fail only once the counter is full, when the eventfd stays
            // readable anyway.
            let _ = (&self.eventfd).write(&1u64.to_ne_bytes());
        }
    }
}

/// Wakes a parked [`Driver`](crate::Driver) from any thread.
///
/// [`unpark`](Unparker::unpark) makes the driver's current
/// [`park`](crate::Driver::park) return or, if it is not parked, its next
/// one. Unparks do not add up: several before a park make it return once.
#[derive(Clone, Debug)]
pub struct Unparker {
    notify: Arc<Notify>,
}

impl Unparker {
    pub(crate) fn new(notify: Arc<Notify>) -> Self {
        Self { notify }
    }

    /// Wakes the driver, or makes its next park return at once.
    ///
    /// This costs a system call only while the driver is parked, and works
    /// after the driver is gone, when it does nothing.
    pub fn unpark(&self) {
        self.notify.unpark();
    }
}
