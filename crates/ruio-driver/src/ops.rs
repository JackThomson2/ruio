//! The operations the driver provides.
//!
//! Operations that take a raw file descriptor do not own it. The caller keeps
//! it open until the operation is submitted, at the next [`Driver::submit`]
//! or [`Driver::park`]; after that the kernel holds its own reference to the
//! file. A descriptor closed too early fails with `EBADF`, or reaches
//! whatever file reused its number, which misdirects the I/O but cannot
//! corrupt memory.
//!
//! [`Driver::submit`]: crate::Driver::submit
//! [`Driver::park`]: crate::Driver::park

use std::io;
use std::os::fd::RawFd;
use std::ptr;
use std::time::Duration;

use ruio_sys as sys;

use crate::driver::timespec;
use crate::{Completion, Operation};

/// The `off` value that makes reads and writes use the file position.
const CURRENT_POSITION: u64 = u64::MAX;

/// Does nothing and completes with `Ok(())`, which makes it a measure of the
/// driver's own overhead.
#[derive(Clone, Copy, Debug, Default)]
pub struct Nop;

// SAFETY: a no-op points to no memory and posts one completion.
unsafe impl Operation for Nop {
    type Output = io::Result<()>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_NOP;
    }

    fn complete(self, completion: Completion) -> io::Result<()> {
        completion.result().map(drop)
    }
}

/// Completes once a duration has passed on the kernel's monotonic clock.
#[derive(Debug)]
pub struct Timeout {
    // Boxed so that the entry points to memory that does not move with the
    // operation.
    timespec: Box<sys::__kernel_timespec>,
}

impl Timeout {
    /// A timeout that completes `duration` after it is submitted.
    pub fn new(duration: Duration) -> Self {
        Self {
            timespec: Box::new(timespec(duration)),
        }
    }
}

// SAFETY: the entry points only to the boxed timespec, which the operation
// owns and which stays put when the operation moves. A timeout with no
// completion count posts one completion.
unsafe impl Operation for Timeout {
    type Output = io::Result<()>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_TIMEOUT;
        sqe.addr = ptr::from_ref(&*self.timespec).expose_provenance() as u64;
        sqe.len = 1;
    }

    fn complete(self, completion: Completion) -> io::Result<()> {
        if completion.res() == -libc::ETIME {
            Ok(())
        } else {
            completion.result().map(drop)
        }
    }
}

/// Reads from a file descriptor into a buffer's spare capacity, like
/// `read(2)`, or `pread(2)` with an offset.
///
/// The bytes read are appended to the buffer's contents, up to its spare
/// capacity, and the buffer comes back with the result.
#[derive(Debug)]
pub struct Read {
    fd: RawFd,
    buf: Vec<u8>,
    offset: u64,
}

impl Read {
    /// Reads at the file position, advancing it.
    pub fn new(fd: RawFd, buf: Vec<u8>) -> Self {
        Self::at(fd, buf, CURRENT_POSITION)
    }

    /// Reads at `offset`, leaving the file position alone.
    pub fn at(fd: RawFd, buf: Vec<u8>, offset: u64) -> Self {
        Self { fd, buf, offset }
    }
}

// SAFETY: the entry points to the vector's spare capacity, on the heap, which
// stays put when the operation moves; nothing reallocates the vector until
// `complete`. A read posts one completion.
unsafe impl Operation for Read {
    /// The number of bytes read, and the buffer.
    type Output = (io::Result<usize>, Vec<u8>);

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        let spare = self.buf.spare_capacity_mut();
        sqe.opcode = sys::IORING_OP_READ;
        sqe.fd = self.fd;
        sqe.addr = spare.as_mut_ptr().expose_provenance() as u64;
        sqe.len = u32::try_from(spare.len()).unwrap_or(u32::MAX);
        sqe.off = self.offset;
    }

    fn complete(mut self, completion: Completion) -> Self::Output {
        let result = completion.result().map(|read| {
            let spare = self.buf.capacity() - self.buf.len();
            let read = (read as usize).min(spare);
            // SAFETY: the kernel initialized `read` bytes of the spare
            // capacity, which begins at the end of the contents.
            unsafe { self.buf.set_len(self.buf.len() + read) };
            read
        });
        (result, self.buf)
    }
}

/// Writes a buffer's contents to a file descriptor, like `write(2)`, or
/// `pwrite(2)` with an offset.
///
/// The buffer comes back with the result.
#[derive(Debug)]
pub struct Write {
    fd: RawFd,
    buf: Vec<u8>,
    offset: u64,
}

impl Write {
    /// Writes at the file position, advancing it.
    pub fn new(fd: RawFd, buf: Vec<u8>) -> Self {
        Self::at(fd, buf, CURRENT_POSITION)
    }

    /// Writes at `offset`, leaving the file position alone.
    pub fn at(fd: RawFd, buf: Vec<u8>, offset: u64) -> Self {
        Self { fd, buf, offset }
    }
}

// SAFETY: the entry points to the vector's contents, on the heap, which stay
// put when the operation moves. A write posts one completion.
unsafe impl Operation for Write {
    /// The number of bytes written, and the buffer.
    type Output = (io::Result<usize>, Vec<u8>);

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_WRITE;
        sqe.fd = self.fd;
        sqe.addr = self.buf.as_ptr().expose_provenance() as u64;
        sqe.len = u32::try_from(self.buf.len()).unwrap_or(u32::MAX);
        sqe.off = self.offset;
    }

    fn complete(self, completion: Completion) -> Self::Output {
        (
            completion.result().map(|written| written as usize),
            self.buf,
        )
    }
}
