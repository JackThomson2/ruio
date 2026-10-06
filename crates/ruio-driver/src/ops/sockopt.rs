//! Socket options and queue sizes through the ring, and installing a direct
//! descriptor as a plain one.
//!
//! `setsockopt(2)`, `getsockopt(2)` and the `SIOCINQ` and `SIOCOUTQ`
//! `ioctl`s take a file descriptor, which a direct descriptor is not: the
//! kernel offers them as commands of [`IORING_OP_URING_CMD`] instead, which
//! take an [`Fd`] of either kind, as every other operation does. The kernel
//! runs each inline as the request is submitted, so a command completes in
//! the same `io_uring_enter(2)` that submits it, and one submitted beside
//! other requests costs no system call of its own. An option is set at any
//! level; the kernel reads only those of `SOL_SOCKET`, and refuses a read at
//! another level with `EOPNOTSUPP`.
//!
//! [`InstallFd`] gives a direct descriptor a plain file descriptor of the
//! process too, for what only a file descriptor can do: `getsockname(2)`,
//! reading an option of another level, or handing the file to a library.
//!
//! [`IORING_OP_URING_CMD`]: sys::IORING_OP_URING_CMD

use std::io;
use std::mem;
use std::os::fd::{FromRawFd, OwnedFd};

use ruio_sys as sys;

use crate::{Completion, Fd, Operation};

/// A value a socket option takes or gives, as the kernel reads and writes
/// it: plain data of a fixed size, every byte pattern of which is a value.
///
/// # Safety
///
/// Every sequence of `size_of::<Self>()` bytes must be a valid value of the
/// type: the kernel writes any bytes it likes into one, and bytes it does
/// not write stay zero.
pub unsafe trait OptionValue: Copy + 'static {}

// SAFETY: every byte pattern is one of these integers.
unsafe impl OptionValue for i32 {}
// SAFETY: as above.
unsafe impl OptionValue for u32 {}
// SAFETY: as above.
unsafe impl OptionValue for i64 {}
// SAFETY: as above.
unsafe impl OptionValue for u64 {}
// SAFETY: a `linger` is two `int`s, every byte pattern of which is a value.
unsafe impl OptionValue for libc::linger {}
// SAFETY: a `timeval` is two integers, every byte pattern of which is a
// value.
unsafe impl OptionValue for libc::timeval {}
// SAFETY: every byte pattern is an array of bytes.
unsafe impl<const N: usize> OptionValue for [u8; N] {}

/// Points `sqe` at a socket command: `cmd_op` is the first half of `off`.
fn prepare_command(sqe: &mut sys::io_uring_sqe, fd: Fd, command: u32) {
    sqe.opcode = sys::IORING_OP_URING_CMD;
    fd.apply(sqe);
    sqe.off = u64::from(command);
}

/// Points `sqe` at an option: `level` and `optname` share `addr`, `optlen`
/// is `file_index`, and `optval` is `addr3`.
fn prepare_option(
    sqe: &mut sys::io_uring_sqe,
    level: i32,
    name: i32,
    value: *const (),
    len: usize,
) {
    sqe.addr = u64::from(level.cast_unsigned()) | u64::from(name.cast_unsigned()) << 32;
    sqe.file_index = u32::try_from(len).expect("an option value fits in a `u32`");
    sqe.addr3 = value.expose_provenance() as u64;
}

/// Reads a socket option of `SOL_SOCKET`, like `getsockopt(2)`, into a
/// value of `T`.
/// value of `T`.
///
/// The output is the value, with any bytes the kernel did not write left
/// zero: an option whose value is shorter than `T` is read in full, and one
/// longer is cut to `T`, as `getsockopt(2)` cuts it to `optlen`.
///
/// The kernel reads options of `SOL_SOCKET` only through the ring, and
/// refuses another level, such as `IPPROTO_TCP`, with `EOPNOTSUPP`: an
/// option of another level is read with `getsockopt(2)` on a plain
/// descriptor, or on the descriptor [`InstallFd`] gives a direct one.
///
/// ```no_run
/// use ruio_driver::ops::GetSockOpt;
/// # let driver = ruio_driver::Driver::new()?;
/// # let fd = ruio_driver::Fd::Plain(0);
///
/// let keepalive = GetSockOpt::<i32>::new(fd, libc::SOL_SOCKET, libc::SO_KEEPALIVE);
/// # let _ = driver.op(keepalive);
/// # Ok::<(), std::io::Error>(())
/// ```
#[derive(Debug)]
pub struct GetSockOpt<T: OptionValue> {
    fd: Fd,
    level: i32,
    name: i32,
    /// Where the kernel writes the value: memory of the operation's own,
    /// which stays put when the operation moves.
    value: Box<T>,
}

impl<T: OptionValue> GetSockOpt<T> {
    /// Reads the option `name` of `level`, such as `SO_KEEPALIVE` of
    /// `SOL_SOCKET`, on the socket `fd`.
    pub fn new(fd: impl Into<Fd>, level: i32, name: i32) -> Self {
        Self {
            fd: fd.into(),
            level,
            name,
            // SAFETY: every byte pattern is a value of an `OptionValue`,
            // zeroes among them.
            value: Box::new(unsafe { mem::zeroed() }),
        }
    }
}

// SAFETY: the entry points only to the boxed value, which the operation owns
// and which stays put when the operation moves. A command posts one
// completion.
unsafe impl<T: OptionValue> Operation for GetSockOpt<T> {
    type Output = io::Result<T>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        prepare_command(sqe, self.fd, sys::SOCKET_URING_OP_GETSOCKOPT);
        let value: *const T = &*self.value;
        prepare_option(sqe, self.level, self.name, value.cast(), size_of::<T>());
    }

    fn complete(self, completion: Completion) -> Self::Output {
        // The result is how many bytes the kernel wrote; the rest are zero.
        completion.result().map(|_| *self.value)
    }
}

/// Sets a socket option, like `setsockopt(2)`, to a value of `T`.
///
/// ```no_run
/// use ruio_driver::ops::SetSockOpt;
/// # let driver = ruio_driver::Driver::new()?;
/// # let fd = ruio_driver::Fd::Plain(0);
///
/// let nodelay = SetSockOpt::new(fd, libc::IPPROTO_TCP, libc::TCP_NODELAY, 1i32);
/// # let _ = driver.op(nodelay);
/// # Ok::<(), std::io::Error>(())
/// ```
#[derive(Debug)]
pub struct SetSockOpt<T: OptionValue> {
    fd: Fd,
    level: i32,
    name: i32,
    /// The value the kernel reads: memory of the operation's own, which
    /// stays put when the operation moves.
    value: Box<T>,
}

impl<T: OptionValue> SetSockOpt<T> {
    /// Sets the option `name` of `level`, such as `TCP_NODELAY` of
    /// `IPPROTO_TCP`, on the socket `fd` to `value`.
    pub fn new(fd: impl Into<Fd>, level: i32, name: i32, value: T) -> Self {
        Self {
            fd: fd.into(),
            level,
            name,
            value: Box::new(value),
        }
    }
}

// SAFETY: as for `GetSockOpt`.
unsafe impl<T: OptionValue> Operation for SetSockOpt<T> {
    type Output = io::Result<()>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        prepare_command(sqe, self.fd, sys::SOCKET_URING_OP_SETSOCKOPT);
        let value: *const T = &*self.value;
        prepare_option(sqe, self.level, self.name, value.cast(), size_of::<T>());
    }

    fn complete(self, completion: Completion) -> Self::Output {
        completion.result().map(drop)
    }
}

/// How many bytes wait in a socket to be read, like the `SIOCINQ` `ioctl`:
/// for a stream, the bytes received and not yet read; for a datagram
/// socket, the size of the next datagram.
#[derive(Debug)]
pub struct UnreadBytes {
    fd: Fd,
}

impl UnreadBytes {
    /// Asks the socket `fd`.
    pub fn new(fd: impl Into<Fd>) -> Self {
        Self { fd: fd.into() }
    }
}

// SAFETY: the entry points to no memory, and a command posts one completion.
unsafe impl Operation for UnreadBytes {
    type Output = io::Result<u32>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        prepare_command(sqe, self.fd, sys::SOCKET_URING_OP_SIOCINQ);
    }

    fn complete(self, completion: Completion) -> Self::Output {
        completion.result()
    }
}

/// How many bytes wait in a socket to be sent, like the `SIOCOUTQ` `ioctl`:
/// for a stream, the bytes written and not yet acknowledged by the peer.
#[derive(Debug)]
pub struct UnsentBytes {
    fd: Fd,
}

impl UnsentBytes {
    /// Asks the socket `fd`.
    pub fn new(fd: impl Into<Fd>) -> Self {
        Self { fd: fd.into() }
    }
}

// SAFETY: as for `UnreadBytes`.
unsafe impl Operation for UnsentBytes {
    type Output = io::Result<u32>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        prepare_command(sqe, self.fd, sys::SOCKET_URING_OP_SIOCOUTQ);
    }

    fn complete(self, completion: Completion) -> Self::Output {
        completion.result()
    }
}

/// Installs the file of a direct descriptor as a plain file descriptor of
/// the process, like `IORING_OP_FIXED_FD_INSTALL`: the output is the new
/// descriptor, which refers to the same open file as the slot, so that
/// closing either leaves the other open. The descriptor is closed on `exec`
/// unless [`inherited`](InstallFd::inherited).
///
/// A plain descriptor has no slot to install from: the kernel refuses it
/// with `EBADF`.
///
/// ```no_run
/// use ruio_driver::ops::InstallFd;
/// # let driver = ruio_driver::Driver::new()?;
/// # let slot = 0;
///
/// let install = InstallFd::new(ruio_driver::Fd::Direct(slot));
/// # let _ = driver.op(install);
/// # Ok::<(), std::io::Error>(())
/// ```
#[derive(Debug)]
pub struct InstallFd {
    fd: Fd,
    flags: u32,
}

impl InstallFd {
    /// Installs the file in the slot `fd` names.
    pub fn new(fd: impl Into<Fd>) -> Self {
        Self {
            fd: fd.into(),
            flags: 0,
        }
    }

    /// Leaves the new descriptor open across `exec`, for a child process to
    /// inherit.
    pub fn inherited(mut self) -> Self {
        self.flags |= sys::IORING_FIXED_FD_NO_CLOEXEC;
        self
    }
}

// SAFETY: the entry points to no memory, and an install posts one
// completion.
unsafe impl Operation for InstallFd {
    type Output = io::Result<OwnedFd>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_FIXED_FD_INSTALL;
        self.fd.apply(sqe);
        sqe.op_flags = self.flags;
    }

    fn complete(self, completion: Completion) -> Self::Output {
        // SAFETY: the kernel installed a new descriptor, which nothing else
        // owns.
        completion
            .result()
            .map(|fd| unsafe { OwnedFd::from_raw_fd(fd.cast_signed()) })
    }
}
