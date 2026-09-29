//! Socket operations.

use std::io;
use std::os::fd::{FromRawFd, OwnedFd};

use ruio_sys as sys;

use crate::{Completion, Descriptor, DirectFd, Driver, Operation};

/// Where an operation that makes a file puts it.
#[derive(Clone, Copy, Debug)]
enum Install {
    /// In a new file descriptor of the process, closed on `exec`.
    Plain,
    /// In a free slot of the registered file table of the ring with this
    /// ID.
    Direct(u64),
}

impl Install {
    /// The entry's `file_index`.
    fn file_index(self) -> u32 {
        match self {
            Install::Plain => 0,
            Install::Direct(_) => sys::IORING_FILE_INDEX_ALLOC,
        }
    }

    /// Takes ownership of the file the kernel made, which the result names.
    fn descriptor(self, res: u32) -> Descriptor {
        match self {
            Install::Plain => {
                // SAFETY: the kernel made the descriptor for this operation,
                // and nothing else owns it.
                Descriptor::Plain(unsafe { OwnedFd::from_raw_fd(res.cast_signed()) })
            }
            Install::Direct(ring) => Descriptor::Direct(DirectFd::new(ring, res)),
        }
    }
}

/// Makes a socket, like `socket(2)`.
///
/// The socket is a plain descriptor, closed on `exec`, unless
/// [`direct`](Socket::direct) asks for a direct descriptor.
#[derive(Debug)]
pub struct Socket {
    domain: i32,
    ty: i32,
    protocol: i32,
    install: Install,
}

impl Socket {
    /// A socket of `domain`, such as `AF_INET`, and type, such as
    /// `SOCK_STREAM`, for `protocol`, or the type's default protocol with 0.
    pub fn new(domain: i32, ty: i32, protocol: i32) -> Self {
        Self {
            domain,
            ty,
            protocol,
            install: Install::Plain,
        }
    }

    /// Makes the socket a direct descriptor, in a free slot of the
    /// registered file table of `driver`'s ring. The operation must run on
    /// that ring: on another, the descriptor would name the wrong file.
    ///
    /// The operation fails with `ENFILE` if the table has no free slot.
    pub fn direct(mut self, driver: &Driver) -> Self {
        self.install = Install::Direct(driver.id());
        self
    }
}

// SAFETY: the entry points to no memory, and a socket request posts one
// completion.
unsafe impl Operation for Socket {
    type Output = io::Result<Descriptor>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_SOCKET;
        sqe.fd = self.domain;
        // The kernel refuses close-on-exec for a direct descriptor, which
        // has nothing to close.
        let ty = match self.install {
            Install::Plain => self.ty | libc::SOCK_CLOEXEC,
            Install::Direct(_) => self.ty & !libc::SOCK_CLOEXEC,
        };
        sqe.off = u64::from(ty.cast_unsigned());
        sqe.len = self.protocol.cast_unsigned();
        sqe.file_index = self.install.file_index();
    }

    fn complete(self, completion: Completion) -> Self::Output {
        completion.result().map(|res| self.install.descriptor(res))
    }
}
