//! Pipe operations: moving data between a pipe and another file, duplicating
//! it between pipes, and the polls that let them wait without a worker, and
//! that wait for any file's readiness.
//!
//! The kernel runs a splice or a tee in an io-wq worker, as the calls they
//! stand for can block, and it polls neither file for readiness first: one
//! that has nothing to read, or a full pipe to write to, holds the worker
//! until it can go on. A [`PollAdd`] linked before it, with
//! [`Op::link`](crate::Op::link), waits for the file instead and spares the
//! worker the wait. A [`PollMulti`] reports a file's readiness again and
//! again on one request, which [`PollStream`] keeps armed.

use std::future::poll_fn;
use std::io;
use std::task::{Context, Poll, ready};

use ruio_sys as sys;

use crate::{Completion, Driver, Fd, MultiOp, Multishot, Operation};

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

/// Reports a file's readiness again and again on one request, like
/// [`PollAdd`] kept armed (`IORING_POLL_ADD_MULTI`): each completion carries
/// the events the file has, among those asked for. The kernel ends the
/// request when it fails, when it is cancelled, or when the completion
/// queue has no room for a completion; [`PollStream`] arms it again then.
///
/// A completion comes for each time the file is woken for the events, not
/// for as long as it stays ready: a reader that takes some of what a file
/// has and waits for the next completion waits for the next arrival, so it
/// should take everything the file has, until a read would block, as with
/// edge-triggered readiness.
#[derive(Debug)]
pub struct PollMulti {
    fd: Fd,
    events: u32,
}

impl PollMulti {
    /// Reports each time `fd` has any of `events`, a mask of `libc::POLLIN`,
    /// `libc::POLLOUT` and the other `POLL*` events.
    pub fn new(fd: impl Into<Fd>, events: u32) -> Self {
        Self {
            fd: fd.into(),
            events,
        }
    }
}

// SAFETY: the entry points to no memory, and a multishot poll's completions
// carry IORING_CQE_F_MORE until the last.
unsafe impl Multishot for PollMulti {
    type Item = io::Result<u32>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_POLL_ADD;
        self.fd.apply(sqe);
        sqe.op_flags = self.events;
        sqe.len = sys::IORING_POLL_ADD_MULTI;
    }

    fn item(&mut self, completion: Completion) -> Self::Item {
        completion.result()
    }
}

/// A file's readiness, reported each time it is woken for the events asked
/// for, by a multishot poll that the stream keeps armed.
///
/// The kernel ends a multishot poll now and then for reasons of its own,
/// such as a full completion queue; the stream arms it again at once, and a
/// poll armed on a file that is ready completes at once, so nothing is
/// missed. An error ends the request too: the stream yields it, and arms
/// the request again when polled again. The stream never ends.
#[derive(Debug)]
pub struct PollStream {
    stream: MultiOp<PollMulti>,
}

impl PollStream {
    /// A stream of the events `request` reports.
    pub fn new(driver: &Driver, request: PollMulti) -> Self {
        Self {
            stream: driver.stream(request),
        }
    }

    /// The events the file was woken for, or the error that ended the
    /// request.
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<u32>> {
        loop {
            match ready!(self.stream.poll_next(cx)) {
                Some(events) => return Poll::Ready(events),
                None => self.stream.rearm(),
            }
        }
    }

    /// The next events; see [`poll_next`](PollStream::poll_next).
    pub async fn next(&mut self) -> io::Result<u32> {
        poll_fn(|cx| self.poll_next(cx)).await
    }

    /// The next events if their completion has arrived already, or `None`,
    /// without waiting; see [`MultiOp::next_ready`].
    pub fn next_ready(&mut self) -> Option<io::Result<u32>> {
        self.stream.next_ready()
    }
}
