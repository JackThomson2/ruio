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

use crate::buf::{BufBundle, BufLease, BufPool, BufResult, FixedIoBuf, IoBuf, IoBufMut};
use crate::driver::timespec;
use crate::{Completion, Descriptor, DirectFd, Driver, Fd, MultiOp, Multishot, Operation};

mod fs;
mod net;

pub(crate) use fs::CloseSlot;
pub use fs::{Close, Fallocate, Fsync, Ftruncate, MkdirAt, OpenAt, RenameAt, Statx, UnlinkAt};
pub use net::{
    Accept, AcceptMulti, AcceptStream, Connect, Recv, RecvFrom, Send, SendTo, SendVectored, SendZc,
    Shutdown, Socket,
};

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
                Descriptor::Plain(unsafe { std::os::fd::FromRawFd::from_raw_fd(res.cast_signed()) })
            }
            Install::Direct(ring) => Descriptor::Direct(DirectFd::new(ring, res)),
        }
    }
}

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
/// arrives: a multishot `recv(2)` (`IORING_RECV_MULTISHOT`) that fills as
/// many buffers as the data waiting needs (`IORING_RECVSEND_BUNDLE`).
///
/// Each item is one receive's data, as a bundle of leases, or `None` once the
/// peer has shut down, which ends the request. When the pool runs dry the
/// kernel ends the request with `ENOBUFS`; [`RecvStream`] waits for a buffer
/// and re-arms it.
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
    type Item = io::Result<Option<BufBundle>>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_RECV;
        self.fd.apply(sqe);
        sqe.ioprio = sys::IORING_RECV_MULTISHOT | sys::IORING_RECVSEND_BUNDLE;
        sqe.flags |= sys::IOSQE_BUFFER_SELECT;
        sqe.buf_index = self.pool.group();
        // `len` stays zero: each receive fills as many buffers as it needs.
    }

    fn item(&mut self, completion: Completion) -> Self::Item {
        // Taken before looking at the result, as for `ReadPooled`.
        let bundle = self.pool.bundle(completion.flags(), completion.res());
        match completion.result()? {
            0 => Ok(None),
            _ => bundle
                .map(Some)
                .ok_or_else(|| io::Error::other("the kernel filled no pooled buffer")),
        }
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
    inner: Rearming<RecvMulti>,
}

impl RecvStream {
    /// A stream of the data arriving on `fd`, received into `pool`.
    pub fn new(driver: &Driver, fd: impl Into<Fd>, pool: &BufPool) -> Self {
        Self {
            inner: Rearming::new(driver, RecvMulti::new(fd, pool), pool),
        }
    }

    /// A stream of the data arriving on `fd`, received into a pool of its
    /// own, of `count` buffers of `size` bytes, which the kernel consumes
    /// incrementally (`IOU_PBUF_RING_INC`, Linux 6.12): each receive takes
    /// what its data fills of a buffer, and the next carries on where it
    /// left off, so that a large buffer holds many small receives. A buffer
    /// returns to the pool once the kernel has filled it and every lease of
    /// a part of it is dropped; a lease's room is only its data.
    ///
    /// The pool is the stream's alone: a completion does not say where in
    /// its buffer its data starts, which the pool works out from the
    /// completions before it, so it must see every one, in order.
    ///
    /// # Errors
    ///
    /// `Unsupported` before Linux 6.12, and as for [`BufPool::new`].
    pub fn incremental(
        driver: &Driver,
        fd: impl Into<Fd>,
        count: u16,
        size: u32,
    ) -> io::Result<Self> {
        let pool = BufPool::incremental(driver, count, size)?;
        Ok(Self {
            inner: Rearming::new(driver, RecvMulti::new(fd, &pool), &pool),
        })
    }

    /// The next data, or `None` once the peer has shut down or an error has
    /// been yielded.
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<BufBundle>>> {
        self.inner.poll_next(cx)
    }

    /// The next data; see [`poll_next`](RecvStream::poll_next).
    pub async fn next(&mut self) -> Option<io::Result<BufBundle>> {
        poll_fn(|cx| self.poll_next(cx)).await
    }

    /// The next data if it has arrived already, without waiting, or `None`:
    /// nothing has, or the stream has ended, which the next
    /// [`poll_next`](RecvStream::poll_next) tells. It leaves the stream as it
    /// is otherwise, so a stream the kernel ended re-arms at the next poll.
    ///
    /// A message that arrives while the stream is armed can come in two
    /// receives, since the kernel takes a single buffer when it does not
    /// know yet how much is waiting; with this, a reader takes both at once.
    pub fn next_ready(&mut self) -> Option<io::Result<BufBundle>> {
        self.inner.next_ready()
    }
}

/// Reads from a file that can be polled, such as a pipe, a FIFO or a
/// character device, into a [`BufPool`] again and again as data arrives: a
/// multishot `read(2)` (`IORING_OP_READ_MULTISHOT`) at the file's position.
///
/// Each item is one read's data, in one buffer of the pool, or `None` at
/// the end of the file, which ends the request. The kernel refuses a file
/// it cannot poll, such as a regular file, with `EBADFD`. A file that does
/// not say it can be read without blocking, and is not `O_NONBLOCK`, as a
/// FIFO or a character device may not be, it reads only while the file
/// polls as readable: a FIFO whose writers have gone polls only as hung
/// up, and its end never comes. When the pool runs dry the kernel ends the
/// request with `ENOBUFS`; [`ReadStream`] waits for a buffer and re-arms
/// it.
#[derive(Debug)]
pub struct ReadMulti {
    fd: Fd,
    pool: BufPool,
}

impl ReadMulti {
    /// Reads from `fd` into `pool`.
    pub fn new(fd: impl Into<Fd>, pool: &BufPool) -> Self {
        Self {
            fd: fd.into(),
            pool: pool.clone(),
        }
    }
}

// SAFETY: the entry points to no memory of its own: the kernel writes into
// buffers of the pool, which the request keeps alive. A multishot read's
// completions carry IORING_CQE_F_MORE until the last.
unsafe impl Multishot for ReadMulti {
    type Item = io::Result<Option<BufLease>>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_READ_MULTISHOT;
        self.fd.apply(sqe);
        sqe.flags |= sys::IOSQE_BUFFER_SELECT;
        sqe.buf_index = self.pool.group();
        sqe.off = CURRENT_POSITION;
        // `addr` and `len` stay zero, as the kernel asks: each read fills
        // one buffer of the pool, as much of it as the data takes.
    }

    fn item(&mut self, completion: Completion) -> Self::Item {
        let len = usize::try_from(completion.res()).unwrap_or(0);
        // Taken before looking at the result, as for `ReadPooled`.
        let lease = self.pool.lease(completion.flags(), len);
        pooled_data(completion, lease)
    }
}

/// The data arriving on a file that can be polled, such as a pipe, read
/// into a [`BufPool`] by a multishot read that the stream keeps armed, as
/// [`RecvStream`] keeps a receive armed: re-armed once a buffer returns to
/// a pool that ran dry, and when the kernel ends it for reasons of its own.
///
/// Each item is one read's data, in one buffer. The stream ends at the end
/// of the file, as when a pipe's writing end closes, or after yielding an
/// error, such as `EBADFD` for a file that cannot be polled.
#[derive(Debug)]
pub struct ReadStream {
    inner: Rearming<ReadMulti>,
}

impl ReadStream {
    /// A stream of the data arriving on `fd`, read into `pool`.
    pub fn new(driver: &Driver, fd: impl Into<Fd>, pool: &BufPool) -> Self {
        Self {
            inner: Rearming::new(driver, ReadMulti::new(fd, pool), pool),
        }
    }

    /// A stream of the data arriving on `fd`, read into a pool of its own
    /// whose buffers the kernel consumes incrementally, so that a large
    /// buffer holds many small reads; see [`RecvStream::incremental`].
    ///
    /// # Errors
    ///
    /// `Unsupported` before Linux 6.12, and as for [`BufPool::new`].
    pub fn incremental(
        driver: &Driver,
        fd: impl Into<Fd>,
        count: u16,
        size: u32,
    ) -> io::Result<Self> {
        let pool = BufPool::incremental(driver, count, size)?;
        Ok(Self {
            inner: Rearming::new(driver, ReadMulti::new(fd, &pool), &pool),
        })
    }

    /// The next data, or `None` at the end of the file or once an error has
    /// been yielded.
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<BufLease>>> {
        self.inner.poll_next(cx)
    }

    /// The next data; see [`poll_next`](ReadStream::poll_next).
    pub async fn next(&mut self) -> Option<io::Result<BufLease>> {
        poll_fn(|cx| self.poll_next(cx)).await
    }

    /// The next data if it has arrived already, without waiting, or `None`;
    /// see [`RecvStream::next_ready`].
    pub fn next_ready(&mut self) -> Option<io::Result<BufLease>> {
        self.inner.next_ready()
    }
}

/// A multishot request into a pool, kept armed: what [`RecvStream`] and
/// [`ReadStream`] share. Its items are data, `None` at the end of the data,
/// or an error.
#[derive(Debug)]
struct Rearming<T: Multishot> {
    stream: MultiOp<T>,
    pool: BufPool,
    /// The pool ran dry; the request re-arms once a buffer returns.
    starved: bool,
    finished: bool,
}

impl<T, D> Rearming<T>
where
    T: Multishot<Item = io::Result<Option<D>>>,
{
    fn new(driver: &Driver, request: T, pool: &BufPool) -> Self {
        Self {
            stream: driver.stream(request),
            pool: pool.clone(),
            starved: false,
            finished: false,
        }
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<D>>> {
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
                Some(Ok(Some(data))) => return Poll::Ready(Some(Ok(data))),
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

    fn next_ready(&mut self) -> Option<io::Result<D>> {
        if self.finished || self.starved {
            return None;
        }
        match self.stream.next_ready()? {
            Ok(Some(data)) => Some(Ok(data)),
            Ok(None) => {
                self.finished = true;
                None
            }
            Err(err) if err.raw_os_error() == Some(libc::ENOBUFS) => {
                self.starved = true;
                None
            }
            Err(err) => {
                self.finished = true;
                Some(Err(err))
            }
        }
    }
}
