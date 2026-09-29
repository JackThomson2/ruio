//! The files operations act on: plain file descriptors, and direct
//! descriptors, which name slots of a ring's registered file table.

use std::os::fd::{AsRawFd, BorrowedFd, RawFd};

use ruio_sys as sys;

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
