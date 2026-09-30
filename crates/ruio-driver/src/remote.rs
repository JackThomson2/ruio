//! Rings as other threads see them, and direct descriptors sent from one
//! ring's file table to another's with `IORING_OP_MSG_RING`.

use std::fmt;
use std::io;
use std::mem;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use ruio_sys as sys;

use crate::{Completion, DirectFd, Driver, Operation, Unparker};

/// A ring as other threads see it: one to send direct descriptors to, with
/// [`DirectFd::send_to`]. It is `Send`, `Sync` and cheap to clone, and does
/// not keep the ring open. Made by [`Driver::remote`].
#[derive(Clone)]
pub struct RemoteRing {
    shared: Arc<Remote>,
}

/// What a driver shares with the threads that send it descriptors.
pub(crate) struct Remote {
    /// The driver's ID.
    id: u64,
    state: Mutex<RemoteState>,
    /// Set once `state` has slots to close.
    pending: AtomicBool,
    unparker: Unparker,
}

struct RemoteState {
    /// The ring's descriptor, until the ring closes.
    fd: Option<RawFd>,
    /// The slots of descriptors sent to the ring and dropped on other
    /// threads, which the ring closes at its next turn.
    closes: Vec<u32>,
}

impl Remote {
    pub(crate) fn new(id: u64, fd: RawFd, unparker: Unparker) -> Arc<Self> {
        Arc::new(Self {
            id,
            state: Mutex::new(RemoteState {
                fd: Some(fd),
                closes: Vec::new(),
            }),
            pending: AtomicBool::new(false),
            unparker,
        })
    }

    fn lock(&self) -> MutexGuard<'_, RemoteState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Forgets the ring's descriptor, before the ring closes it: senders
    /// from then on find the ring closed.
    pub(crate) fn close(&self) {
        let mut state = self.lock();
        state.fd = None;
        // The ring's file table goes with it.
        state.closes.clear();
    }

    /// The slots that other threads have asked the ring to close.
    pub(crate) fn take_closes(&self) -> Vec<u32> {
        // A slot queued after the check sets the flag again, and unparks the
        // ring: its next turn takes it.
        if !self.pending.swap(false, Ordering::Acquire) {
            return Vec::new();
        }
        mem::take(&mut self.lock().closes)
    }

    /// Asks the ring to close `slot` at its next turn, unless it is closed.
    fn close_later(&self, slot: u32) {
        {
            let mut state = self.lock();
            if state.fd.is_none() {
                return;
            }
            state.closes.push(slot);
        }
        self.pending.store(true, Ordering::Release);
        self.unparker.unpark();
    }
}

impl RemoteRing {
    pub(crate) fn new(shared: Arc<Remote>) -> Self {
        Self { shared }
    }

    /// The ID of the ring's driver; see [`Driver::id`].
    pub fn id(&self) -> u64 {
        self.shared.id
    }

    /// A duplicate of the ring's descriptor, which stays open, and its number
    /// that ring's, however soon the ring closes.
    ///
    /// # Errors
    ///
    /// `EBADF` if the ring is closed, and what duplicating the descriptor
    /// reports.
    fn duplicate(&self) -> io::Result<OwnedFd> {
        let state = self.shared.lock();
        let fd = state
            .fd
            .ok_or_else(|| io::Error::from_raw_os_error(libc::EBADF))?;
        // SAFETY: `fcntl` touches no memory, and the ring closes the
        // descriptor only once it has taken the lock held here.
        let duplicate = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
        if duplicate < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the duplicate is new, and nothing else owns it.
        Ok(unsafe { OwnedFd::from_raw_fd(duplicate) })
    }
}

impl fmt::Debug for RemoteRing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteRing")
            .field("id", &self.shared.id)
            .finish_non_exhaustive()
    }
}

/// A direct descriptor sent to a ring by [`DirectFd::send_to`], in that
/// ring's file table, for the ring's thread to take with
/// [`claim`](SentFd::claim). Unlike a [`DirectFd`] it is `Send`, so it can
/// go to that thread from the one that sent it.
///
/// Dropping it closes the file: at once on its ring's thread, and at the
/// ring's next turn on any other.
pub struct SentFd {
    slot: u32,
    /// Taken as the descriptor is claimed.
    ring: Option<RemoteRing>,
}

impl SentFd {
    /// The slot the file is in, in its ring's table.
    pub fn slot(&self) -> u32 {
        self.slot
    }

    /// The ID of the driver whose ring the file is in; see [`Driver::id`].
    pub fn ring(&self) -> u64 {
        self.ring.as_ref().map_or(0, RemoteRing::id)
    }

    /// Takes the descriptor, on its ring's thread.
    ///
    /// # Errors
    ///
    /// Hands the token back on any other thread, or once the ring is gone.
    pub fn claim(mut self) -> Result<DirectFd, Self> {
        let ring = self.ring();
        if Driver::find(ring).is_none() {
            return Err(self);
        }
        // The descriptor takes over from the token's drop.
        self.ring = None;
        Ok(DirectFd::new(ring, self.slot))
    }
}

impl Drop for SentFd {
    fn drop(&mut self) {
        let Some(ring) = self.ring.take() else {
            return;
        };
        match Driver::find(ring.id()) {
            Some(driver) => driver.close_direct(self.slot),
            None => ring.shared.close_later(self.slot),
        }
    }
}

impl fmt::Debug for SentFd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SentFd")
            .field("slot", &self.slot)
            .field("ring", &self.ring())
            .finish()
    }
}

/// Sends a direct descriptor to another ring's file table: what
/// [`DirectFd::send_to`] runs, on the descriptor's own ring, whose table
/// the source slot is in.
pub(crate) struct SendDirect {
    fd: DirectFd,
    to: RemoteRing,
    /// A duplicate of the other ring's descriptor, which the entry names.
    ring_fd: OwnedFd,
}

impl SendDirect {
    /// Sends `fd` to `to`, or hands `fd` back with the error of
    /// [`RemoteRing::duplicate`].
    pub(crate) fn new(fd: DirectFd, to: &RemoteRing) -> Result<Self, (io::Error, DirectFd)> {
        match to.duplicate() {
            Ok(ring_fd) => Ok(Self {
                fd,
                to: to.clone(),
                ring_fd,
            }),
            Err(err) => Err((err, fd)),
        }
    }
}

// SAFETY: the entry points to no memory: it names slots and a descriptor,
// which the operation keeps open until it completes. The message posts one
// completion here, and, skipped, none to the other ring.
unsafe impl Operation for SendDirect {
    type Output = Result<SentFd, (io::Error, DirectFd)>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_MSG_RING;
        sqe.fd = self.ring_fd.as_raw_fd();
        sqe.addr = sys::IORING_MSG_SEND_FD;
        sqe.addr3 = u64::from(self.fd.slot());
        sqe.file_index = sys::IORING_FILE_INDEX_ALLOC;
        // The other ring learns of the file from the token instead.
        sqe.op_flags = sys::IORING_MSG_RING_CQE_SKIP;
    }

    fn complete(self, completion: Completion) -> Self::Output {
        match completion.result() {
            // The other ring's table holds the file now, and dropping the
            // descriptor frees this ring's slot.
            Ok(slot) => Ok(SentFd {
                slot,
                ring: Some(self.to),
            }),
            Err(err) => Err((err, self.fd)),
        }
    }
}

impl fmt::Debug for SendDirect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendDirect")
            .field("fd", &self.fd)
            .field("to", &self.to)
            .finish_non_exhaustive()
    }
}
