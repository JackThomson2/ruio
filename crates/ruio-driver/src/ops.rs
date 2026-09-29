//! The operations the driver provides.
//!
//! Operations act on an [`Fd`]: a plain file descriptor, or a direct
//! descriptor of the ring they run on. They do not own it. The caller keeps
//! it open until the operation is submitted, at the next [`Driver::submit`]
//! or [`Driver::park`]; after that the kernel holds its own reference to the
//! file. A descriptor closed too early fails with `EBADF`, or reaches
//! whatever file reused its number, which misdirects the I/O but cannot
//! corrupt memory.
//!
//! Operations that move data take their buffer by value, as an
//! [`IoBuf`] or [`IoBufMut`], and hand it back with the result.
//!
//! [`Driver::submit`]: crate::Driver::submit
//! [`Driver::park`]: crate::Driver::park

use std::future::poll_fn;
use std::io;
use std::ptr;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use ruio_sys as sys;

use crate::buf::{BufLease, BufPool, BufResult, FixedIoBuf, IoBuf, IoBufMut};
use crate::driver::timespec;
use crate::{Completion, Driver, Fd, MultiOp, Multishot, Operation};

mod net;

pub use net::{Accept, Connect, Recv, RecvFrom, Send, SendTo, Shutdown, Socket};

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

/// Reads from a file descriptor into a buffer, like `read(2)`, or `pread(2)`
/// with an offset.
///
/// The read fills the buffer from its start, up to
/// [`bytes_total`](IoBuf::bytes_total), and marks the bytes it read as
/// initialized; slice the buffer to read elsewhere in it. The buffer comes
/// back with the result.
#[derive(Debug)]
pub struct Read<B> {
    fd: Fd,
    buf: B,
    offset: u64,
}

impl<B: IoBufMut> Read<B> {
    /// Reads at the file position, advancing it.
    pub fn new(fd: impl Into<Fd>, buf: B) -> Self {
        Self::at(fd, buf, CURRENT_POSITION)
    }

    /// Reads at `offset`, leaving the file position alone.
    pub fn at(fd: impl Into<Fd>, buf: B, offset: u64) -> Self {
        Self {
            fd: fd.into(),
            buf,
            offset,
        }
    }
}

// SAFETY: the entry points into the buffer, which `IoBufMut` keeps valid and
// in place while the operation holds it, even as it moves. A read posts one
// completion.
unsafe impl<B: IoBufMut> Operation for Read<B> {
    /// The number of bytes read, and the buffer.
    type Output = BufResult<usize, B>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        prepare_read(
            sqe,
            sys::IORING_OP_READ,
            self.fd,
            &mut self.buf,
            self.offset,
        );
    }

    fn complete(self, completion: Completion) -> Self::Output {
        complete_read(self.buf, completion)
    }
}

fn prepare_read<B: IoBufMut>(
    sqe: &mut sys::io_uring_sqe,
    opcode: u8,
    fd: Fd,
    buf: &mut B,
    offset: u64,
) {
    sqe.opcode = opcode;
    fd.apply(sqe);
    sqe.addr = buf.stable_mut_ptr().expose_provenance() as u64;
    sqe.len = u32::try_from(buf.bytes_total()).unwrap_or(u32::MAX);
    sqe.off = offset;
}

fn complete_read<B: IoBufMut>(mut buf: B, completion: Completion) -> BufResult<usize, B> {
    let result = completion.result().map(|read| {
        let read = (read as usize).min(buf.bytes_total());
        // SAFETY: the kernel initialized the first `read` bytes, which are
        // within the buffer.
        unsafe { buf.set_init(read) };
        read
    });
    (result, buf)
}

fn prepare_write<B: IoBuf>(sqe: &mut sys::io_uring_sqe, opcode: u8, fd: Fd, buf: &B, offset: u64) {
    sqe.opcode = opcode;
    fd.apply(sqe);
    sqe.addr = buf.stable_ptr().expose_provenance() as u64;
    sqe.len = u32::try_from(buf.bytes_init()).unwrap_or(u32::MAX);
    sqe.off = offset;
}

/// Writes a buffer's initialized bytes to a file descriptor, like `write(2)`,
/// or `pwrite(2)` with an offset.
///
/// The buffer comes back with the result.
#[derive(Debug)]
pub struct Write<B> {
    fd: Fd,
    buf: B,
    offset: u64,
}

impl<B: IoBuf> Write<B> {
    /// Writes at the file position, advancing it.
    pub fn new(fd: impl Into<Fd>, buf: B) -> Self {
        Self::at(fd, buf, CURRENT_POSITION)
    }

    /// Writes at `offset`, leaving the file position alone.
    pub fn at(fd: impl Into<Fd>, buf: B, offset: u64) -> Self {
        Self {
            fd: fd.into(),
            buf,
            offset,
        }
    }
}

// SAFETY: the entry points into the buffer, which `IoBuf` keeps valid, in
// place and unchanged while the operation holds it. A write posts one
// completion.
unsafe impl<B: IoBuf> Operation for Write<B> {
    /// The number of bytes written, and the buffer.
    type Output = BufResult<usize, B>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        prepare_write(sqe, sys::IORING_OP_WRITE, self.fd, &self.buf, self.offset);
    }

    fn complete(self, completion: Completion) -> Self::Output {
        (
            completion.result().map(|written| written as usize),
            self.buf,
        )
    }
}

/// Reads into a registered buffer, like [`Read`], without the kernel mapping
/// the buffer's memory for the read.
#[derive(Debug)]
pub struct ReadFixed<B> {
    fd: Fd,
    buf: B,
    offset: u64,
}

impl<B: IoBufMut + FixedIoBuf> ReadFixed<B> {
    /// Reads at the file position, advancing it.
    pub fn new(fd: impl Into<Fd>, buf: B) -> Self {
        Self::at(fd, buf, CURRENT_POSITION)
    }

    /// Reads at `offset`, leaving the file position alone.
    pub fn at(fd: impl Into<Fd>, buf: B, offset: u64) -> Self {
        Self {
            fd: fd.into(),
            buf,
            offset,
        }
    }
}

// SAFETY: as for `Read`; the buffer index names registered memory, which the
// kernel checks the buffer lies in.
unsafe impl<B: IoBufMut + FixedIoBuf> Operation for ReadFixed<B> {
    /// The number of bytes read, and the buffer.
    type Output = BufResult<usize, B>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        prepare_read(
            sqe,
            sys::IORING_OP_READ_FIXED,
            self.fd,
            &mut self.buf,
            self.offset,
        );
        sqe.buf_index = self.buf.buf_index();
    }

    fn complete(self, completion: Completion) -> Self::Output {
        complete_read(self.buf, completion)
    }
}

/// Writes from a registered buffer, like [`Write`], without the kernel
/// mapping the buffer's memory for the write.
#[derive(Debug)]
pub struct WriteFixed<B> {
    fd: Fd,
    buf: B,
    offset: u64,
}

impl<B: IoBuf + FixedIoBuf> WriteFixed<B> {
    /// Writes at the file position, advancing it.
    pub fn new(fd: impl Into<Fd>, buf: B) -> Self {
        Self::at(fd, buf, CURRENT_POSITION)
    }

    /// Writes at `offset`, leaving the file position alone.
    pub fn at(fd: impl Into<Fd>, buf: B, offset: u64) -> Self {
        Self {
            fd: fd.into(),
            buf,
            offset,
        }
    }
}

// SAFETY: as for `Write`; the buffer index names registered memory, which the
// kernel checks the buffer lies in.
unsafe impl<B: IoBuf + FixedIoBuf> Operation for WriteFixed<B> {
    /// The number of bytes written, and the buffer.
    type Output = BufResult<usize, B>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        prepare_write(
            sqe,
            sys::IORING_OP_WRITE_FIXED,
            self.fd,
            &self.buf,
            self.offset,
        );
        sqe.buf_index = self.buf.buf_index();
    }

    fn complete(self, completion: Completion) -> Self::Output {
        (
            completion.result().map(|written| written as usize),
            self.buf,
        )
    }
}

/// Reads from a file descriptor into a buffer the kernel picks from a
/// [`BufPool`] once data arrives, like [`Read`].
///
/// The output is the data, as a lease on the buffer, or `None` at the end of
/// the file; it is `ENOBUFS` if the pool had no free buffer.
#[derive(Debug)]
pub struct ReadPooled {
    fd: Fd,
    pool: BufPool,
    offset: u64,
}

impl ReadPooled {
    /// Reads at the file position, advancing it.
    pub fn new(fd: impl Into<Fd>, pool: &BufPool) -> Self {
        Self::at(fd, pool, CURRENT_POSITION)
    }

    /// Reads at `offset`, leaving the file position alone.
    pub fn at(fd: impl Into<Fd>, pool: &BufPool, offset: u64) -> Self {
        Self {
            fd: fd.into(),
            pool: pool.clone(),
            offset,
        }
    }
}

// SAFETY: the entry points to no memory of its own: the kernel writes into a
// buffer of the pool, which the operation keeps alive. A read posts one
// completion.
unsafe impl Operation for ReadPooled {
    type Output = io::Result<Option<BufLease>>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_READ;
        self.fd.apply(sqe);
        sqe.flags |= sys::IOSQE_BUFFER_SELECT;
        sqe.buf_index = self.pool.group();
        sqe.len = u32::try_from(self.pool.buffer_size()).unwrap_or(u32::MAX);
        sqe.off = self.offset;
    }

    fn complete(self, completion: Completion) -> Self::Output {
        let len = usize::try_from(completion.res()).unwrap_or(0);
        // Taken before looking at the result, so that a buffer the read
        // consumed returns to the pool whatever happened.
        let lease = self.pool.lease(completion.flags(), len);
        pooled_data(completion, lease)
    }
}

/// The data of a completion into a pool: the lease, `None` at the end of the
/// data, or the error.
fn pooled_data(completion: Completion, lease: Option<BufLease>) -> io::Result<Option<BufLease>> {
    match completion.result()? {
        0 => Ok(None),
        _ => lease
            .map(Some)
            .ok_or_else(|| io::Error::other("the kernel filled no pooled buffer")),
    }
}

/// Receives from a socket into a [`BufPool`] again and again as data
/// arrives: a multishot `recv(2)` (`IORING_RECV_MULTISHOT`).
///
/// Each item is one receive's data, as a lease, or `None` once the peer has
/// shut down, which ends the request. When the pool runs dry the kernel ends
/// the request with `ENOBUFS`; [`RecvStream`] waits for a buffer and re-arms
/// it.
#[derive(Debug)]
pub struct RecvMulti {
    fd: Fd,
    pool: BufPool,
}

impl RecvMulti {
    /// Receives from `fd` into `pool`.
    pub fn new(fd: impl Into<Fd>, pool: &BufPool) -> Self {
        Self {
            fd: fd.into(),
            pool: pool.clone(),
        }
    }
}

// SAFETY: the entry points to no memory of its own: the kernel writes into
// buffers of the pool, which the request keeps alive. A multishot receive's
// completions carry IORING_CQE_F_MORE until the last.
unsafe impl Multishot for RecvMulti {
    type Item = io::Result<Option<BufLease>>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_RECV;
        self.fd.apply(sqe);
        sqe.ioprio = sys::IORING_RECV_MULTISHOT;
        sqe.flags |= sys::IOSQE_BUFFER_SELECT;
        sqe.buf_index = self.pool.group();
        // `len` stays zero: each receive fills up to a whole buffer.
    }

    fn item(&mut self, completion: Completion) -> Self::Item {
        let len = usize::try_from(completion.res()).unwrap_or(0);
        let lease = self.pool.lease(completion.flags(), len);
        pooled_data(completion, lease)
    }
}

/// The data arriving on a socket, received into a [`BufPool`] by a multishot
/// receive that the stream keeps armed.
///
/// The kernel ends a multishot receive when the pool runs dry, and now and
/// then for reasons of its own, such as a full completion queue. The stream
/// then waits for a buffer to return, if it must, and re-arms the request, so
/// the data keeps arriving in order and none is lost. The stream ends when
/// the peer shuts down, or after yielding an error.
#[derive(Debug)]
pub struct RecvStream {
    stream: MultiOp<RecvMulti>,
    pool: BufPool,
    /// The pool ran dry; the request re-arms once a buffer returns.
    starved: bool,
    finished: bool,
}

impl RecvStream {
    /// A stream of the data arriving on `fd`, received into `pool`.
    pub fn new(driver: &Driver, fd: impl Into<Fd>, pool: &BufPool) -> Self {
        Self {
            stream: driver.stream(RecvMulti::new(fd, pool)),
            pool: pool.clone(),
            starved: false,
            finished: false,
        }
    }

    /// The next data, or `None` once the peer has shut down or an error has
    /// been yielded.
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<BufLease>>> {
        loop {
            if self.finished {
                return Poll::Ready(None);
            }
            if self.starved {
                ready!(self.pool.poll_available(cx));
                self.starved = false;
                self.stream.rearm();
            }
            match ready!(self.stream.poll_next(cx)) {
                Some(Ok(Some(lease))) => return Poll::Ready(Some(Ok(lease))),
                Some(Ok(None)) => {
                    self.finished = true;
                    return Poll::Ready(None);
                }
                Some(Err(err)) if err.raw_os_error() == Some(libc::ENOBUFS) => {
                    self.starved = true;
                }
                Some(Err(err)) => {
                    self.finished = true;
                    return Poll::Ready(Some(Err(err)));
                }
                // Ended by the kernel for its own reasons: carry on.
                None => self.stream.rearm(),
            }
        }
    }

    /// The next data; see [`poll_next`](RecvStream::poll_next).
    pub async fn next(&mut self) -> Option<io::Result<BufLease>> {
        poll_fn(|cx| self.poll_next(cx)).await
    }
}
