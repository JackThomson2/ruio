//! Pipe operations: moving data between a pipe and another file, duplicating
//! it between pipes, and the poll that lets them wait without a worker.
//!
//! The kernel runs a splice or a tee in an io-wq worker, as the calls they
//! stand for can block, and it polls neither file for readiness first: one
//! that has nothing to read, or a full pipe to write to, holds the worker
//! until it can go on. A [`PollAdd`] linked before it, with
//! [`Op::link`](crate::Op::link), waits for the file instead and spares the
//! worker the wait.

use std::io;

use ruio_sys as sys;

use crate::{Completion, Fd, Operation};

/// Points the entry's `splice_fd_in` at `fd`; a direct descriptor marks
/// itself as a slot of the file table in `splice_flags`.
fn apply_fd_in(fd: Fd, sqe: &mut sys::io_uring_sqe) {
    match fd {
        Fd::Plain(fd) => sqe.file_index = fd.cast_unsigned(),
        Fd::Direct(slot) => {
            sqe.file_index = slot;
            sqe.op_flags |= sys::SPLICE_F_FD_IN_FIXED;
        }
    }
}

/// An offset as the entry carries it: all ones for none, which means the
/// file's position or, for a pipe or a socket, that there is no offset.
fn splice_offset(offset: Option<u64>) -> u64 {
    offset.unwrap_or(u64::MAX)
}

/// Moves up to `len` bytes from one file to another, one of them a pipe,
/// like `splice(2)`: the bytes pass through the kernel without a buffer in
/// user space. The output is the bytes moved, 0 at the end of the input.
///
/// The kernel runs the splice in an io-wq worker. One from a socket with
/// nothing to read, or into a pipe that is full without `SPLICE_F_NONBLOCK`,
/// holds the worker until it can go on; a [`PollAdd`] linked before it, with
/// [`Op::link`](crate::Op::link), spares the worker that wait.
#[derive(Debug)]
pub struct Splice {
    fd_in: Fd,
    off_in: Option<u64>,
    fd_out: Fd,
    off_out: Option<u64>,
    len: u32,
    flags: u32,
}

impl Splice {
    /// Moves up to `len` bytes from `fd_in` to `fd_out`, each from its file
    /// position, or from wherever a pipe or a socket is up to.
    pub fn new(fd_in: impl Into<Fd>, fd_out: impl Into<Fd>, len: u32) -> Self {
        Self {
            fd_in: fd_in.into(),
            off_in: None,
            fd_out: fd_out.into(),
            off_out: None,
            len,
            flags: 0,
        }
    }

    /// Reads from `offset` of `fd_in`, a file, leaving its position alone.
    /// A pipe or a socket has no offset to read from: the kernel fails with
    /// `ESPIPE`.
    pub fn offset_in(mut self, offset: u64) -> Self {
        self.off_in = Some(offset);
        self
    }

    /// Writes at `offset` of `fd_out`, a file, leaving its position alone.
    /// A pipe or a socket has no offset to write at: the kernel fails with
    /// `ESPIPE`.
    pub fn offset_out(mut self, offset: u64) -> Self {
        self.off_out = Some(offset);
        self
    }

    /// Adds `SPLICE_F_*` flags, such as `SPLICE_F_NONBLOCK`, `SPLICE_F_MORE`
    /// or `SPLICE_F_MOVE`.
    pub fn flags(mut self, flags: u32) -> Self {
        self.flags |= flags;
        self
    }
}

// SAFETY: the entry points to no memory, and a splice posts one completion.
unsafe impl Operation for Splice {
    type Output = io::Result<u32>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_SPLICE;
        self.fd_out.apply(sqe);
        sqe.off = splice_offset(self.off_out);
        sqe.addr = splice_offset(self.off_in);
        sqe.len = self.len;
        sqe.op_flags = self.flags;
        apply_fd_in(self.fd_in, sqe);
    }

    fn complete(self, completion: Completion) -> Self::Output {
        completion.result()
    }
}

/// Duplicates up to `len` bytes from one pipe into another, like `tee(2)`,
/// leaving them in the first. The output is the bytes duplicated.
///
/// As a [`Splice`], a tee runs in an io-wq worker, which it holds while the
/// first pipe is empty or the second full, unless `SPLICE_F_NONBLOCK` is
/// among its flags.
#[derive(Debug)]
pub struct Tee {
    fd_in: Fd,
    fd_out: Fd,
    len: u32,
    flags: u32,
}

impl Tee {
    /// Duplicates up to `len` bytes from the pipe `fd_in` into the pipe
    /// `fd_out`.
    pub fn new(fd_in: impl Into<Fd>, fd_out: impl Into<Fd>, len: u32) -> Self {
        Self {
            fd_in: fd_in.into(),
            fd_out: fd_out.into(),
            len,
            flags: 0,
        }
    }

    /// Adds `SPLICE_F_*` flags, such as `SPLICE_F_NONBLOCK`.
    pub fn flags(mut self, flags: u32) -> Self {
        self.flags |= flags;
        self
    }
}

// SAFETY: the entry points to no memory, and a tee posts one completion.
unsafe impl Operation for Tee {
    type Output = io::Result<u32>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_TEE;
        self.fd_out.apply(sqe);
        sqe.len = self.len;
        sqe.op_flags = self.flags;
        apply_fd_in(self.fd_in, sqe);
    }

    fn complete(self, completion: Completion) -> Self::Output {
        completion.result()
    }
}

/// Waits for a file to be ready, like `poll(2)` on one descriptor: completes
/// once the file has any of the events asked for, with the events it has as
/// the output.
///
/// Linked before a [`Splice`], or any operation the kernel would otherwise
/// hold an io-wq worker on until the file is ready, the poll does the
/// waiting, and the operation runs once the file is ready. A regular file is
/// always ready, so a poll on one completes at once.
#[derive(Debug)]
pub struct PollAdd {
    fd: Fd,
    events: u32,
}

impl PollAdd {
    /// Waits until `fd` has any of `events`, a mask of `libc::POLLIN`,
    /// `libc::POLLOUT` and the other `POLL*` events.
    pub fn new(fd: impl Into<Fd>, events: u32) -> Self {
        Self {
            fd: fd.into(),
            events,
        }
    }
}

// SAFETY: the entry points to no memory, and a single-shot poll posts one
// completion.
unsafe impl Operation for PollAdd {
    type Output = io::Result<u32>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_POLL_ADD;
        self.fd.apply(sqe);
        sqe.op_flags = self.events;
    }

    fn complete(self, completion: Completion) -> Self::Output {
        completion.result()
    }
}
