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
pub use fs::{
    Close, Fallocate, Fsync, Ftruncate, InstallPolled, MkdirAt, OpenAt, RenameAt, Statx, UnlinkAt,
};
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
    #[inline]
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

    #[inline]
    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_NOP;
    }

    #[inline]
    fn complete(self, completion: Completion) -> io::Result<()> {
        completion.result().map(drop)
    }
}

/// Cancels a request in flight, named by its `user_data`: an
/// `IORING_OP_ASYNC_CANCEL`. Completes with `Ok(())` once the request is on
/// its way out, `ENOENT` if none by that name is in flight, or `EALREADY`
/// if it is finishing already.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Cancel {
    target: u64,
}

impl Cancel {
    /// Cancels the request whose `user_data` is `target`.
    pub(crate) fn new(target: u64) -> Self {
        Self { target }
    }
}

// SAFETY: a cancellation points to no memory, naming its target by value,
// and posts one completion.
unsafe impl Operation for Cancel {
    type Output = io::Result<()>;

    #[inline]
    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_ASYNC_CANCEL;
        sqe.addr = self.target;
    }

    #[inline]
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
    polled: bool,
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
            polled: false,
        }
    }

    /// Runs the read on the driver's polled ring, which polls the device for
    /// its completion; see [`Operation::polled`]. The file must be open for
    /// direct I/O on a block device with poll queues, as a plain descriptor
    /// or as a direct descriptor of the polled ring, a
    /// [`PolledFd`](crate::PolledFd): a slot of the main ring's table means
    /// nothing there.
    pub fn polled(mut self, polled: bool) -> Self {
        self.polled = polled;
        self
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

    #[inline]
    fn polled(&self) -> bool {
        self.polled
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
    polled: bool,
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
            polled: false,
        }
    }

    /// Runs the write on the driver's polled ring; see [`Read::polled`].
    pub fn polled(mut self, polled: bool) -> Self {
        self.polled = polled;
        self
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

    #[inline]
    fn polled(&self) -> bool {
        self.polled
    }
}

/// Reads into a registered buffer, like [`Read`], without the kernel mapping
/// the buffer's memory for the read.
#[derive(Debug)]
pub struct ReadFixed<B> {
    fd: Fd,
    buf: B,
    offset: u64,
    polled: bool,
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
            polled: false,
        }
    }

    /// Runs the read on the driver's polled ring, which shares the ring's
    /// registered buffers; see [`Read::polled`].
    pub fn polled(mut self, polled: bool) -> Self {
        self.polled = polled;
        self
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

    #[inline]
    fn polled(&self) -> bool {
        self.polled
    }
}

/// Writes from a registered buffer, like [`Write`], without the kernel
/// mapping the buffer's memory for the write.
#[derive(Debug)]
pub struct WriteFixed<B> {
    fd: Fd,
    buf: B,
    offset: u64,
    polled: bool,
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
            polled: false,
        }
    }

    /// Runs the write on the driver's polled ring, which shares the ring's
    /// registered buffers; see [`Read::polled`].
    pub fn polled(mut self, polled: bool) -> Self {
        self.polled = polled;
        self
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

    #[inline]
    fn polled(&self) -> bool {
        self.polled
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

    #[inline]
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
#[inline]
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
    /// How many buffers the largest message seen took: 1 until one spans
    /// more; see [`RecvMulti::buffers`].
    message: u32,
    /// The buffers of the message under way that the receives before this
    /// one took: each filled its whole ask with more waiting, so the message
    /// goes on in the next.
    carry: u32,
    /// Whether the thread waits on one peer at a time, so that the ask is a
    /// buffer short of the message, and the kernel hands over a message's
    /// first part before the rest.
    split: bool,
    /// How much the thread has lately looked to be waiting on one peer at
    /// a time, between `-STREAK` and [`STREAK`]: an item from a turn that
    /// delivered one completion counts two towards it, one from a turn that
    /// delivered more counts one against.
    quiet: i8,
    /// The ask the request was last prepared with, in buffers, and in bytes
    /// as the entry's `len`; 0 leaves it to the kernel.
    asked: u32,
    asked_len: u32,
}

/// How far `quiet` runs before a receive splits its messages, or stops: a
/// split message costs a completion and a send more, and a quiet thread's
/// turns then deliver the send's own completion with the next data, which
/// must not undo the split; so quiet turns weigh twice as much as busy
/// ones, and the split holds while more than a third of the turns are
/// quiet.
const STREAK: i8 = 8;

impl RecvMulti {
    /// Receives from `fd` into `pool`.
    pub fn new(fd: impl Into<Fd>, pool: &BufPool) -> Self {
        Self {
            fd: fd.into(),
            pool: pool.clone(),
            message: 1,
            carry: 0,
            split: false,
            quiet: 0,
            asked: 0,
            asked_len: 0,
        }
    }

    /// Takes the messages to run to `count` buffers from the start, where
    /// the stream otherwise learns that from the messages themselves.
    ///
    /// The kernel maps the buffers of a receive before it knows how much is
    /// waiting: asked for one buffer's worth, it maps one, and a message of
    /// several buffers' worth that has already arrived takes two calls into
    /// the socket, one for the first buffer and one for the rest, once the
    /// first has reported them; asked for more, it maps as many buffers as
    /// that needs, so such a message takes one call, and each receive still
    /// takes only what has arrived and reports only the buffers it filled.
    /// But every receive then walks that many ring entries, which costs a
    /// message that fits one buffer a percent or two, and a receive that
    /// fills the whole ask is capped (`IORING_RECV_MSHOT_CAP`): the kernel
    /// appends no more to it and queues the request again for the rest, so
    /// an ask smaller than the messages costs each a completion and a
    /// requeue.
    ///
    /// So the ask is left to the kernel, which takes a message of several
    /// buffers' worth in two calls, as long as the thread is busy: asking
    /// for the whole message instead gains nothing measurable, as the
    /// second call costs little next to the copies. While the thread waits
    /// on one peer at a time, which the ring's turns delivering one
    /// completion each tell (`Driver::completions_last_turn`), the ask is a
    /// buffer short of the largest message seen, counting a capped receive
    /// and the one that takes the rest as one message: the kernel then hands
    /// over all but the last buffer of a message as soon as it has them,
    /// and the last on its own, so that a task answering the message has
    /// most of it while the kernel still copies the rest, as it would with
    /// a peer that sends as it goes; a connection echoing 64 KiB messages
    /// gains a fifth that way. Under load the split would cost each message
    /// a completion and a send, so the ask goes back to the kernel's then.
    /// Each change of the ask submits the request again (see
    /// [`Multishot::changed`]). A pool the kernel consumes incrementally
    /// takes from a buffer what the data fills, and asks for nothing
    /// whatever the messages.
    pub fn buffers(mut self, count: u32) -> Self {
        self.message = self.message.max(count);
        self
    }

    /// The ask, in buffers, for the messages seen and the load: 0 leaves it
    /// to the kernel.
    fn ask(&self) -> u32 {
        if self.split && self.message > 1 {
            self.message - 1
        } else {
            0
        }
    }
}

// SAFETY: the entry points to no memory of its own: the kernel writes into
// buffers of the pool, which the request keeps alive. A multishot receive's
// completions carry IORING_CQE_F_MORE until the last.
unsafe impl Multishot for RecvMulti {
    type Item = io::Result<Option<BufBundle>>;

    /// The entry's `len` is how much the kernel maps for a receive before it
    /// knows how much is waiting, and caps a receive at; 0 leaves it to the
    /// kernel, which maps one buffer and appends what it then finds
    /// waiting. See [`RecvMulti::buffers`].
    ///
    /// A capped receive is first issued only once the socket polls readable
    /// (`IORING_RECVSEND_POLL_FIRST`), as every receive is after its first
    /// completion. Without the flag, a receive issued at submission while
    /// data waits, and stopped short by the cap, posts its completion from
    /// there, then arms poll, finds the socket readable and queues itself to
    /// go on (`io_recv_finish`, `io_queue_async`, `__io_arm_poll_handler`
    /// in Linux 7.0's io_uring); issued from poll, it posts the completion
    /// and queues itself again (`IOU_REQUEUE`, `io_poll_task_func`). Both
    /// leave the request armed with the rest to come, and the receive
    /// stream's tests have run both without a stall. The flag is a
    /// precaution: a stream was once seen to stop completing after a capped
    /// completion of a receive issued at submission, with data in the
    /// socket, and the cause was not found; see DESIGN.md.
    #[inline]
    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_RECV;
        self.fd.apply(sqe);
        sqe.ioprio = sys::IORING_RECV_MULTISHOT | sys::IORING_RECVSEND_BUNDLE;
        sqe.flags |= sys::IOSQE_BUFFER_SELECT;
        sqe.buf_index = self.pool.group();
        self.asked = self.ask();
        self.asked_len = 0;
        if self.asked > 0 && !self.pool.is_incremental() {
            let size = u32::try_from(self.pool.buffer_size()).unwrap_or(u32::MAX);
            self.asked_len = size.saturating_mul(self.asked);
            sqe.len = self.asked_len;
            sqe.ioprio |= sys::IORING_RECVSEND_POLL_FIRST;
        }
    }

    #[inline]
    fn item(&mut self, completion: Completion) -> Self::Item {
        // Taken before looking at the result, as for `ReadPooled`.
        let bundle = self.pool.bundle(completion.flags(), completion.res());
        if let Some(bundle) = &bundle
            && !self.pool.is_incremental()
        {
            // A pool consumed incrementally hands out parts of a buffer,
            // which say nothing of the messages.
            let filled = u32::try_from(bundle.buffers()).unwrap_or(u32::MAX);
            let capped = self.asked_len > 0 && completion.res().unsigned_abs() >= self.asked_len;
            let more = completion.flags() & sys::IORING_CQE_F_SOCK_NONEMPTY != 0;
            if capped && more {
                // The message goes on in the next receive.
                self.carry = self.carry.saturating_add(filled);
            } else {
                // No message takes more buffers than the pool holds.
                let count = u32::try_from(self.pool.count()).unwrap_or(u32::MAX);
                let whole = self.carry.saturating_add(filled).min(count);
                self.message = self.message.max(whole);
                self.carry = 0;
            }
        }
        match completion.result()? {
            0 => Ok(None),
            _ => bundle
                .map(Some)
                .ok_or_else(|| io::Error::other("the kernel filled no pooled buffer")),
        }
    }

    #[inline]
    fn changed(&mut self, driver: &Driver) -> bool {
        // Messages that fit one buffer leave the ask to the kernel whatever
        // the load, so there is nothing to weigh for them.
        if self.message <= 1 || self.pool.is_incremental() {
            return false;
        }
        self.quiet = if driver.completions_last_turn() <= 1 {
            self.quiet.saturating_add(2).min(STREAK)
        } else {
            self.quiet.saturating_sub(1).max(-STREAK)
        };
        if self.quiet >= STREAK {
            self.split = true;
        } else if self.quiet <= -STREAK {
            self.split = false;
        }
        self.ask() != self.asked
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

    /// Takes the messages to run to `count` buffers from the start, for a
    /// stream whose messages are known to be large; the stream otherwise
    /// learns that from the messages themselves, and sizes each receive's
    /// ask by it while the thread waits on one peer at a time (see
    /// [`RecvMulti::buffers`]). Set before the stream is first polled,
    /// which arms the receive.
    pub fn buffers(mut self, count: u32) -> Self {
        if let Some(request) = self.inner.stream.get_mut() {
            request.message = request.message.max(count);
        }
        self
    }

    /// The next data, or `None` once the peer has shut down or an error has
    /// been yielded.
    #[inline]
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
    #[inline]
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
    #[inline]
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<BufLease>>> {
        self.inner.poll_next(cx)
    }

    /// The next data; see [`poll_next`](ReadStream::poll_next).
    pub async fn next(&mut self) -> Option<io::Result<BufLease>> {
        poll_fn(|cx| self.poll_next(cx)).await
    }

    /// The next data if it has arrived already, without waiting, or `None`;
    /// see [`RecvStream::next_ready`].
    #[inline]
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
