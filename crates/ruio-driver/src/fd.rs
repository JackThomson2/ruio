//! The files operations act on: plain file descriptors, and direct
//! descriptors, which name slots of a ring's registered file table.

use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::mem;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};

use ruio_sys as sys;

use crate::Driver;
use crate::ops::CloseSlot;
use crate::remote::{RemoteRing, SendDirect, SentFd};

/// The file an operation acts on: a plain file descriptor, or a direct
/// descriptor, a slot of the registered file table of the ring the operation
/// runs on.
///
/// With a direct descriptor the kernel skips looking the file up, and taking
/// and dropping a reference to it, for every operation (`IOSQE_FIXED_FILE`).
/// The number means something only to its own ring: anywhere else it names
/// another file, or none.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Fd {
    /// A file descriptor of the process.
    Plain(RawFd),
    /// A slot of the ring's registered file table.
    Direct(u32),
}

impl Fd {
    /// Points `sqe` at the file.
    #[inline]
    pub(crate) fn apply(self, sqe: &mut sys::io_uring_sqe) {
        match self {
            Fd::Plain(fd) => sqe.fd = fd,
            Fd::Direct(slot) => {
                // The kernel reads the index back as unsigned, so a slot
                // beyond `i32::MAX` is out of range there rather than here.
                sqe.fd = slot.cast_signed();
                sqe.flags |= sys::IOSQE_FIXED_FILE;
            }
        }
    }
}

impl From<RawFd> for Fd {
    #[inline]
    fn from(fd: RawFd) -> Self {
        Fd::Plain(fd)
    }
}

impl From<BorrowedFd<'_>> for Fd {
    #[inline]
    fn from(fd: BorrowedFd<'_>) -> Self {
        Fd::Plain(fd.as_raw_fd())
    }
}

/// A direct descriptor: a file in a slot of one ring's registered file table,
/// which only that ring's operations can reach, as [`Fd::Direct`]. Dropping
/// it closes the file and frees the slot, with a request that reaches the
/// kernel at the driver's next [`submit`](Driver::submit) or
/// [`park`](Driver::park).
///
/// Operations that create files, such as [`Socket`](crate::ops::Socket),
/// make direct descriptors on request. One costs no file descriptor of the
/// process, but nothing outside the ring can use it either.
///
/// A `DirectFd` names its ring rather than holding on to it, and does not
/// keep it open; once the ring is closed, so is the file. It belongs to the
/// thread its ring belongs to:
///
/// ```compile_fail
/// fn assert_send<T: Send>() {}
/// assert_send::<ruio_driver::DirectFd>();
/// ```
pub struct DirectFd {
    slot: u32,
    /// The ID of the driver whose ring the slot belongs to.
    ring: u64,
    /// Makes the descriptor `!Send` and `!Sync`, as its ring is.
    _ring_thread: PhantomData<*const ()>,
}

impl DirectFd {
    /// Takes ownership of the file in `slot` of the table of the ring with
    /// ID `ring`.
    pub(crate) fn new(ring: u64, slot: u32) -> Self {
        Self {
            slot,
            ring,
            _ring_thread: PhantomData,
        }
    }

    /// The slot the file is in.
    #[inline]
    pub fn slot(&self) -> u32 {
        self.slot
    }

    /// The descriptor, for operations on its ring.
    #[inline]
    pub fn fd(&self) -> Fd {
        Fd::Direct(self.slot)
    }

    /// The driver of the ring the descriptor belongs to, unless it has been
    /// dropped.
    pub fn driver(&self) -> Option<Driver> {
        Driver::find(self.ring)
    }

    /// Whether the descriptor belongs to `driver`'s ring: a comparison,
    /// where [`driver`](DirectFd::driver) searches the thread's drivers.
    #[inline]
    pub fn belongs_to(&self, driver: &Driver) -> bool {
        driver.id() == self.ring
    }

    /// Closes the file, on its ring, reporting what closing it found, such
    /// as an error writing back data, where dropping the descriptor closes
    /// it without saying.
    ///
    /// A ring that is gone took its file table with it, so there is nothing
    /// to close. The future does nothing until polled, and dropped before
    /// then it closes the file as dropping the descriptor does.
    ///
    /// # Errors
    ///
    /// What `close(2)` reports.
    pub async fn close(self) -> io::Result<()> {
        let Some(driver) = self.driver() else {
            return Ok(());
        };
        let slot = self.slot;
        // The close below takes over from the descriptor's drop.
        mem::forget(self);
        driver.op(CloseSlot(slot)).await
    }

    /// Sends the descriptor to another ring's file table, with
    /// `IORING_OP_MSG_RING` on its own ring: the kernel puts the file in a
    /// free slot there, this ring lets go of it, and the output is a
    /// [`SentFd`] for the other ring's thread to claim.
    ///
    /// # Errors
    ///
    /// Hands the descriptor back with the error: `EBADF` if either ring is
    /// closed, `ENFILE` if the other's table is full, `EOWNERDEAD` if its
    /// thread has exited, and `EINVAL` if it is this ring.
    pub async fn send_to(self, to: &RemoteRing) -> Result<SentFd, (io::Error, DirectFd)> {
        let Some(driver) = self.driver() else {
            return Err((io::Error::from_raw_os_error(libc::EBADF), self));
        };
        driver.op(SendDirect::new(self, to)?).await
    }
}

impl Drop for DirectFd {
    fn drop(&mut self) {
        // A ring that is gone took its file table with it.
        if let Some(driver) = self.driver() {
            driver.close_direct(self.slot);
        }
    }
}

impl fmt::Debug for DirectFd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DirectFd")
            .field("slot", &self.slot)
            .field("ring", &self.ring)
            .finish()
    }
}

/// A direct descriptor of the polled ring: a file in a slot of the file
/// table of the ring that [polled](crate::Operation::polled) operations run
/// on, which they name as [`Fd::Direct`]; made by
/// [`Driver::install_polled`]. The slot holds a reference to the file of
/// its own, beside the plain descriptor the file was installed from, which
/// the main ring's operations keep using.
///
/// Dropping it empties the slot, with an update that reaches the kernel
/// after the operations queued on the polled ring before it. Like a
/// [`DirectFd`], it names its ring rather than holding on to it, and
/// belongs to the ring's thread:
///
/// ```compile_fail
/// fn assert_send<T: Send>() {}
/// assert_send::<ruio_driver::PolledFd>();
/// ```
pub struct PolledFd {
    slot: u32,
    /// The ID of the driver whose polled ring the slot belongs to.
    ring: u64,
    /// Makes the descriptor `!Send` and `!Sync`, as its ring is.
    _ring_thread: PhantomData<*const ()>,
}

impl PolledFd {
    /// Takes ownership of the file in `slot` of the polled ring's table of
    /// the driver with ID `ring`.
    pub(crate) fn new(ring: u64, slot: u32) -> Self {
        Self {
            slot,
            ring,
            _ring_thread: PhantomData,
        }
    }

    /// The slot the file is in.
    #[inline]
    pub fn slot(&self) -> u32 {
        self.slot
    }

    /// The descriptor, for polled operations on its ring.
    #[inline]
    pub fn fd(&self) -> Fd {
        Fd::Direct(self.slot)
    }

    /// Whether the descriptor belongs to `driver`'s polled ring.
    #[inline]
    pub fn belongs_to(&self, driver: &Driver) -> bool {
        driver.id() == self.ring
    }
}

impl Drop for PolledFd {
    fn drop(&mut self) {
        // A ring that is gone took its file table with it.
        if let Some(driver) = Driver::find(self.ring) {
            driver.release_polled(self.slot);
        }
    }
}

impl fmt::Debug for PolledFd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PolledFd")
            .field("slot", &self.slot)
            .field("ring", &self.ring)
            .finish()
    }
}

/// A file an operation made: a plain file descriptor or a direct descriptor.
/// Dropping it closes the file.
#[derive(Debug)]
pub enum Descriptor {
    /// A file descriptor of the process.
    Plain(OwnedFd),
    /// A direct descriptor of the ring that made it.
    Direct(DirectFd),
}

impl Descriptor {
    /// The descriptor, for operations; a direct descriptor only means
    /// something to its own ring.
    pub fn fd(&self) -> Fd {
        match self {
            Descriptor::Plain(fd) => Fd::from(fd.as_fd()),
            Descriptor::Direct(direct) => direct.fd(),
        }
    }

    /// Whether this is a direct descriptor.
    pub fn is_direct(&self) -> bool {
        matches!(self, Descriptor::Direct(_))
    }
}

impl From<OwnedFd> for Descriptor {
    fn from(fd: OwnedFd) -> Self {
        Descriptor::Plain(fd)
    }
}

impl From<DirectFd> for Descriptor {
    fn from(fd: DirectFd) -> Self {
        Descriptor::Direct(fd)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direct_descriptors_select_the_file_table() {
        let mut sqe = sys::io_uring_sqe {
            flags: sys::IOSQE_BUFFER_SELECT,
            ..Default::default()
        };
        Fd::Direct(7).apply(&mut sqe);
        assert_eq!(sqe.fd, 7);
        assert_eq!(sqe.flags, sys::IOSQE_BUFFER_SELECT | sys::IOSQE_FIXED_FILE);

        let mut sqe = sys::io_uring_sqe::default();
        Fd::from(5).apply(&mut sqe);
        assert_eq!((sqe.fd, sqe.flags), (5, 0));
    }
}
