//! The files operations act on: plain file descriptors, and direct
//! descriptors, which name slots of a ring's registered file table.

use std::fmt;
use std::marker::PhantomData;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};

use ruio_sys as sys;

use crate::Driver;

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
    fn from(fd: RawFd) -> Self {
        Fd::Plain(fd)
    }
}

impl From<BorrowedFd<'_>> for Fd {
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
    pub fn slot(&self) -> u32 {
        self.slot
    }

    /// The descriptor, for operations on its ring.
    pub fn fd(&self) -> Fd {
        Fd::Direct(self.slot)
    }

    /// The driver of the ring the descriptor belongs to, unless it has been
    /// dropped.
    pub fn driver(&self) -> Option<Driver> {
        Driver::find(self.ring)
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
