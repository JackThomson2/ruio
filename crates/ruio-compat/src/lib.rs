//! Adapters from ruio's streams to the poll-based I/O traits that the
//! ecosystem builds on: `AsyncRead`, `AsyncBufRead` and `AsyncWrite` of
//! [`tokio::io`](https://docs.rs/tokio/latest/tokio/io/), with the `tokio`
//! feature, and of [`futures-io`](https://docs.rs/futures-io), with the
//! `futures-io` feature. Both are on by default. tokio's traits need none of
//! its runtime, which ruio's takes the place of.
//!
//! ruio's I/O is completion-based: an operation takes a buffer of its own,
//! the kernel fills or sends it, and the operation hands it back. The
//! poll-based traits lend the caller's buffer for one call instead, which
//! the kernel cannot use once the call returns. [`Compat`] bridges the two
//! with buffers of its own:
//!
//! - A read receives into the adapter's buffer, and copies out as much as
//!   the caller's takes; the rest waits for the next read, and
//!   `AsyncBufRead` lends it out as it is. A receive stays in flight from
//!   one poll to the next, however long the caller takes to poll again, so
//!   nothing it receives is lost; dropping the adapter cancels it. An
//!   adapter made [`with_pool`](Compat::with_pool) receives into the
//!   buffers of a pool instead, with one multishot receive that stays
//!   armed, as [`TcpStream::recv_stream`] does, so that its reads cost the
//!   ring no request each.
//! - A write copies the caller's bytes and sends them, as
//!   [`TcpStream::write_behind`] does: it is done, as far as the caller can
//!   tell, once the send is on its way, and the next write waits for it to
//!   finish. How the send went comes out later: a failure fails the writes,
//!   flushes and closes after it. Dropping the adapter leaves the send to
//!   finish, and the stream closes after it.
//! - A flush has nothing to wait for, since the adapter holds nothing back:
//!   it reports a failure if one is known, as a socket's flush would. Were
//!   it to wait for the send to finish, each flush would cost its task
//!   another wake-up, which a server flushing every response pays dearly for
//!   with many connections.
//! - Closing waits for the send in flight, then shuts down the writing half
//!   of the connection.
//!
//! Like the streams, an adapter belongs to its thread, whose runtime's ring
//! runs its I/O.
//!
//! With the `hyper` feature, on by default, [`Timer`] gives hyper ruio's
//! timers, which its timeouts need: that for reading a request's head, say.
//!
//! ```no_run
//! use ruio::net::TcpStream;
//! use ruio_compat::Compat;
//! use tokio::io::{AsyncReadExt, AsyncWriteExt};
//!
//! #[ruio::main]
//! async fn main() -> std::io::Result<()> {
//!     let mut stream = Compat::new(TcpStream::connect("127.0.0.1:7878").await?);
//!     stream.write_all(b"hello").await?;
//!     stream.shutdown().await?;
//!     let mut reply = Vec::new();
//!     stream.read_to_end(&mut reply).await?;
//!     Ok(())
//! }
//! ```
//!
//! With both families implemented, a method such as `write_all` is
//! ambiguous where the extension traits of both are in scope: import one
//! family's, or call through the trait, as in
//! `futures_util::AsyncWriteExt::write_all(&mut stream, data)`.

use std::fmt;
use std::future::Future;
use std::io::{self, IoSlice};
use std::mem;
use std::net::Shutdown;
use std::pin::Pin;
use std::task::{Context, Poll, Waker, ready};

use ruio::buf::{BufBundle, BufLease, BufPool};
use ruio::net::{TcpStream, UnixStream};
use ruio::uring::ops::{self, Recv, Send};
use ruio::uring::{Driver, Fd, Op};

#[cfg(feature = "futures-io")]
mod futures_impls;
#[cfg(feature = "hyper")]
mod timer;
#[cfg(feature = "tokio")]
mod tokio_impls;

#[cfg(feature = "hyper")]
pub use timer::Timer;

/// How many bytes a read asks the kernel for, unless the adapter was made
/// [`with_capacity`](Compat::with_capacity).
const DEFAULT_CAPACITY: usize = 16 << 10;

/// The most bytes a write takes at once, and copies: a bound on the memory
/// a write in flight holds.
const MAX_WRITE: usize = 256 << 10;

/// The largest write buffer an adapter keeps for its next write, once the
/// send is done with it, rather than freeing it: what an idle connection
/// may hold.
const KEPT_WRITE: usize = 16 << 10;

/// The flags of the adapter's sends: the kernel sends on through a short
/// send, so one stops short only when the connection breaks.
const SEND_FLAGS: i32 = libc::MSG_WAITALL;

/// A stream socket of ruio's that a [`Compat`] adapts: a [`TcpStream`] or a
/// [`UnixStream`].
pub trait StreamSocket: sealed::Sealed {}

impl StreamSocket for TcpStream {}
impl StreamSocket for UnixStream {}

mod sealed {
    use super::*;

    /// What an adapter needs of its stream: the descriptor its operations
    /// run on.
    pub trait Sealed {
        fn fd(&self) -> Fd;
    }

    impl Sealed for TcpStream {
        fn fd(&self) -> Fd {
            TcpStream::fd(self)
        }
    }

    impl Sealed for UnixStream {
        fn fd(&self) -> Fd {
            UnixStream::fd(self)
        }
    }
}

/// A ruio stream with the poll-based I/O traits: `AsyncRead`,
/// `AsyncBufRead` and `AsyncWrite` of `tokio::io` and of futures-io; see
/// the [crate documentation](crate) for how they map to the ring.
///
/// Reads go through a buffer of the adapter's, 16 KiB unless made
/// [`with_capacity`](Compat::with_capacity), which a receive fills with
/// whatever has arrived, or through the buffers of a pool, made
/// [`with_pool`](Compat::with_pool). Writes copy up to 256 KiB at a time,
/// and send them without waiting for the kernel.
pub struct Compat<S> {
    // Fields drop in order: the operations, of which the receive and the
    // shutdown cancel and the send is left to finish, then the stream,
    // which closes after them.
    reader: Reader,
    writer: Writer,
    /// The shutdown in flight.
    shutdown: Option<Op<ops::Shutdown>>,
    /// Whether the writing half of the connection has been shut down.
    closed: bool,
    /// The ring the adapter's own operations run on: the thread's, taken at
    /// the first of them. Holding it keeps the ring open, and a direct
    /// descriptor's slot with it.
    driver: Option<Driver>,
    stream: S,
}

/// Where an adapter's reads come from.
enum Reader {
    /// A buffer of the adapter's, which one receive at a time fills.
    Buffer {
        /// The receive in flight, which holds the buffer meanwhile.
        read: Option<Op<Recv<Vec<u8>>>>,
        /// What was received and not yet taken: `buf[pos..]`.
        buf: Vec<u8>,
        pos: usize,
        /// How many bytes a receive asks for.
        capacity: usize,
    },
    /// The buffers of a pool, which a multishot receive fills as data
    /// arrives.
    Pool {
        /// The receive, from the first read on.
        recv: Option<ops::RecvStream>,
        pool: BufPool,
        /// The buffer being read: what was received and not yet taken is
        /// its data from `pos`, then that of the buffers after it in
        /// `rest`. Each goes back to the pool as soon as it is read out.
        lease: Option<BufLease>,
        pos: usize,
        /// The rest of the bundle the lease came from, in order.
        rest: Option<<BufBundle as IntoIterator>::IntoIter>,
        /// What the receive failed with, when a read found it after taking
        /// data, which it reported first.
        error: Option<io::Error>,
    },
}

impl Reader {
    /// Ready once data has been received and not yet taken, or the stream
    /// has ended. `target` gives the ring and descriptor to receive with.
    fn poll_fill(
        &mut self,
        cx: &mut Context<'_>,
        target: impl FnOnce() -> (Driver, Fd),
    ) -> Poll<io::Result<()>> {
        match self {
            Reader::Buffer {
                read,
                buf,
                pos,
                capacity,
            } => {
                if *pos < buf.len() {
                    return Poll::Ready(Ok(()));
                }
                if read.is_none() {
                    let mut buf = mem::take(buf);
                    buf.clear();
                    buf.reserve(*capacity);
                    *pos = 0;
                    let (driver, fd) = target();
                    *read = Some(driver.op(Recv::new(fd, buf)));
                }
                let op = read.as_mut().expect("a receive is in flight");
                let (received, filled) = ready!(Pin::new(op).poll(cx));
                *read = None;
                *buf = filled;
                Poll::Ready(received.map(drop))
            }
            Reader::Pool {
                recv,
                pool,
                lease,
                pos,
                rest,
                error,
            } => {
                if lease.is_some() {
                    return Poll::Ready(Ok(()));
                }
                if let Some(err) = error.take() {
                    return Poll::Ready(Err(err));
                }
                if recv.is_none() {
                    let (driver, fd) = target();
                    *recv = Some(ops::RecvStream::new(&driver, fd, pool));
                }
                let stream = recv.as_mut().expect("the receive is armed");
                // Ended by the peer, or by an error it yielded, the stream
                // yields nothing more: the end of the stream, from then on.
                match ready!(stream.poll_next(cx)) {
                    Some(Ok(bundle)) => {
                        take_in(bundle, lease, pos, rest);
                        Poll::Ready(Ok(()))
                    }
                    Some(Err(err)) => Poll::Ready(Err(err)),
                    None => Poll::Ready(Ok(())),
                }
            }
        }
    }

    /// Takes in what a pool's receive has brought in since, if what was
    /// taken in before has all been read, without waiting for more.
    fn fill_ready(&mut self) {
        let Reader::Pool {
            recv: Some(recv),
            lease,
            pos,
            rest,
            error,
            ..
        } = self
        else {
            return;
        };
        if lease.is_some() || error.is_some() {
            return;
        }
        match recv.next_ready() {
            Some(Ok(bundle)) => take_in(bundle, lease, pos, rest),
            Some(Err(err)) => *error = Some(err),
            None => {}
        }
    }

    /// Data received and not yet taken, as it lies in one buffer: empty if
    /// there is none.
    fn buffered(&self) -> &[u8] {
        match self {
            Reader::Buffer { buf, pos, .. } => &buf[*pos..],
            Reader::Pool { lease, pos, .. } => lease.as_deref().map_or(&[], |lease| &lease[*pos..]),
        }
    }

    /// Takes `amount` bytes of [`buffered`](Reader::buffered).
    fn consume(&mut self, amount: usize) {
        match self {
            Reader::Buffer { buf, pos, .. } => *pos = (*pos + amount).min(buf.len()),
            Reader::Pool {
                lease, pos, rest, ..
            } => {
                let Some(current) = lease else {
                    return;
                };
                *pos = (*pos + amount).min(current.len());
                if *pos == current.len() {
                    // Back to the pool at once, for the kernel to fill again.
                    *lease = rest.as_mut().and_then(Iterator::next);
                    *pos = 0;
                    if lease.is_none() {
                        *rest = None;
                    }
                }
            }
        }
    }
}

/// Takes in a bundle a pool's receive brought, for reading from its first
/// buffer on.
#[inline]
fn take_in(
    bundle: BufBundle,
    lease: &mut Option<BufLease>,
    pos: &mut usize,
    rest: &mut Option<<BufBundle as IntoIterator>::IntoIter>,
) {
    let mut leases = bundle.into_iter();
    *lease = leases.next();
    *pos = 0;
    *rest = Some(leases);
}

impl<S: StreamSocket> Compat<S> {
    /// Adapts `stream`, reading 16 KiB at a time.
    pub fn new(stream: S) -> Self {
        Self::with_capacity(DEFAULT_CAPACITY, stream)
    }

    /// Adapts `stream`, reading up to `capacity` bytes at a time into a
    /// buffer that size.
    ///
    /// # Panics
    ///
    /// If `capacity` is 0.
    pub fn with_capacity(capacity: usize, stream: S) -> Self {
        assert!(capacity > 0, "a read buffer holds at least one byte");
        Self::with_reader(
            Reader::Buffer {
                read: None,
                buf: Vec::new(),
                pos: 0,
                capacity,
            },
            stream,
        )
    }

    /// Adapts `stream`, receiving into `pool`, which must belong to the
    /// thread's ring, with one multishot receive, which stays armed from the
    /// first read on, as a stream's [`recv_stream`](TcpStream::recv_stream)
    /// does. A read then costs the ring no request of its own, and the
    /// connection holds a buffer only while it has data waiting to be read.
    ///
    /// The data waits in as many of the pool's buffers as it fills until it
    /// is read, however many that is. A pool so suits connections whose
    /// reader reads what arrives promptly, as hyper does: one that is not
    /// read while data keeps arriving can take every buffer, and leave the
    /// others' receives waiting for one.
    pub fn with_pool(pool: &BufPool, stream: S) -> Self {
        Self::with_reader(
            Reader::Pool {
                recv: None,
                pool: pool.clone(),
                lease: None,
                pos: 0,
                rest: None,
                error: None,
            },
            stream,
        )
    }

    fn with_reader(reader: Reader, stream: S) -> Self {
        Self {
            reader,
            writer: Writer::default(),
            shutdown: None,
            closed: false,
            driver: None,
            stream,
        }
    }

    /// The stream.
    pub fn get_ref(&self) -> &S {
        &self.stream
    }

    /// The data received and not yet read, as far as it lies in one buffer:
    /// all of it, unless the adapter receives into a pool, whose later
    /// buffers may hold more.
    pub fn buffer(&self) -> &[u8] {
        self.reader.buffered()
    }

    /// The stream back. A receive in flight is cancelled, and what was
    /// received and not yet read is lost, as the
    /// [`buffer`](Compat::buffer) is; a write in flight carries on.
    pub fn into_inner(self) -> S {
        self.stream
    }

    /// The ring to run an operation on, and the stream's descriptor for it.
    fn target(&mut self) -> (Driver, Fd) {
        target(&mut self.driver, &self.stream)
    }

    /// The data received and not yet taken, as it lies in one buffer,
    /// receiving some first if there is none; empty once the stream has
    /// ended.
    fn poll_fill_buf(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        let Compat {
            reader,
            driver,
            stream,
            ..
        } = self;
        ready!(reader.poll_fill(cx, || target(driver, stream)))?;
        Poll::Ready(Ok(self.reader.buffered()))
    }

    /// Takes `amount` bytes of what [`poll_fill_buf`](Self::poll_fill_buf)
    /// gave.
    fn consume(&mut self, amount: usize) {
        self.reader.consume(amount);
    }

    /// Copies out as much of the data received as `room` bytes take, waiting
    /// for some if there is none, and hands it to `put`, a buffer's worth
    /// at a time. From a pool, that takes in all that its receive has
    /// brought in.
    fn poll_copy(
        &mut self,
        cx: &mut Context<'_>,
        room: usize,
        mut put: impl FnMut(&[u8]),
    ) -> Poll<io::Result<usize>> {
        if room == 0 {
            return Poll::Ready(Ok(0));
        }
        ready!(self.poll_fill_buf(cx))?;
        let mut copied = 0;
        loop {
            if copied < room && self.reader.buffered().is_empty() {
                self.reader.fill_ready();
            }
            let data = self.reader.buffered();
            let len = data.len().min(room - copied);
            if len == 0 {
                return Poll::Ready(Ok(copied));
            }
            put(&data[..len]);
            self.reader.consume(len);
            copied += len;
        }
    }

    /// Reads into `dst`, as much of the data received as it takes.
    fn poll_read(&mut self, cx: &mut Context<'_>, dst: &mut [u8]) -> Poll<io::Result<usize>> {
        let mut filled = 0;
        self.poll_copy(cx, dst.len(), |data| {
            dst[filled..filled + data.len()].copy_from_slice(data);
            filled += data.len();
        })
    }

    /// Writes as many of the bytes of `bufs`, one after another, as one
    /// write takes, once the write before has finished.
    fn poll_write(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let total = bufs.iter().map(|buf| buf.len()).sum::<usize>();
        if total == 0 {
            return Poll::Ready(Ok(0));
        }
        if self.closed {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        ready!(self.writer.poll_done(cx))?;
        let len = total.min(MAX_WRITE);
        // The last write's buffer is back by now, as its send has finished.
        let mut data = mem::take(&mut self.writer.spare);
        data.reserve(len);
        for buf in bufs {
            let take = buf.len().min(len - data.len());
            data.extend_from_slice(&buf[..take]);
        }
        let (driver, fd) = self.target();
        self.writer.send(&driver, fd, data);
        Poll::Ready(Ok(len))
    }

    /// Reports how the writes went, as far as is known, without waiting for
    /// the write in flight, whose failure the next write, flush or close
    /// reports.
    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.writer.take_ready();
        Poll::Ready(self.writer.outcome())
    }

    /// Waits for the write in flight, then shuts down the writing half of
    /// the connection.
    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.closed {
            return Poll::Ready(Ok(()));
        }
        if self.shutdown.is_none() {
            ready!(self.writer.poll_done(cx))?;
            let (driver, fd) = self.target();
            self.shutdown = Some(driver.op(ops::Shutdown::new(fd, Shutdown::Write)));
        }
        let shutdown = self.shutdown.as_mut().expect("a shutdown is in flight");
        let result = ready!(Pin::new(shutdown).poll(cx));
        self.shutdown = None;
        self.closed = result.is_ok();
        Poll::Ready(result)
    }
}

/// The ring the adapter runs its operations on, which the first of them
/// takes, and the stream's descriptor for it.
fn target<S: StreamSocket>(driver: &mut Option<Driver>, stream: &S) -> (Driver, Fd) {
    let driver = driver.get_or_insert_with(ruio::driver).clone();
    (driver, stream.fd())
}

/// Where an adapter's writes go: one send at a time, which the adapter
/// keeps, in flight or finished, until the next write, flush or close takes
/// its result and its buffer.
///
/// The send is submitted with a waker that wakes nothing, so its completion
/// wakes no task: a task that waits for it, as a write does while the one
/// before is still in flight, leaves its own waker with the send instead.
/// Its result waits in the send's slot of the driver's table meanwhile,
/// where a detached operation's would have the driver copy the operation
/// into the slot and out again to finish it there.
#[derive(Default)]
struct Writer {
    /// The send of the last write, until its result is taken.
    send: Option<Op<Send<Vec<u8>>>>,
    /// How many bytes the send was given.
    len: usize,
    /// Whether a task waits for the send, having left its waker with it,
    /// which a poll with no waker must not replace.
    waited: bool,
    /// The buffer of the last write, emptied, once its send is done with
    /// it, for the next write to fill: a write then allocates nothing.
    spare: Vec<u8>,
    /// What a send failed with, as an errno: the writes, flushes and closes
    /// after it report it as the kernel would have. A send that failed, or
    /// was cut short, leaves a gap in the stream, so every write after it
    /// fails, as a stream's writes do once it has broken.
    error: Option<i32>,
}

impl Writer {
    /// Sends `data` on `fd`, with nobody waiting for it.
    fn send(&mut self, driver: &Driver, fd: Fd, data: Vec<u8>) {
        self.len = data.len();
        let mut send = driver.op(Send::new(fd, data).flags(SEND_FLAGS));
        match Pin::new(&mut send).poll(&mut Context::from_waker(Waker::noop())) {
            // Refused by the ring: it fails at once.
            Poll::Ready((sent, data)) => self.finish(sent, data),
            Poll::Pending => self.send = Some(send),
        }
    }

    /// Ready once no send is in flight, having taken the result of the last
    /// one: `Ok`, unless a send failed.
    fn poll_done(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(send) = &mut self.send {
            let Poll::Ready((sent, data)) = Pin::new(send).poll(cx) else {
                self.waited = true;
                return Poll::Pending;
            };
            self.send = None;
            self.waited = false;
            self.finish(sent, data);
        }
        Poll::Ready(self.outcome())
    }

    /// Takes the result of the last send if it has finished, without
    /// waiting, unless a task waits for it, whose waker the send keeps.
    fn take_ready(&mut self) {
        if self.waited {
            return;
        }
        if let Some(send) = &mut self.send
            && let Poll::Ready((sent, data)) =
                Pin::new(send).poll(&mut Context::from_waker(Waker::noop()))
        {
            self.send = None;
            self.finish(sent, data);
        }
    }

    /// Records how the last send went, and keeps its buffer for the next
    /// write unless it has grown past what an idle connection should hold.
    fn finish(&mut self, sent: io::Result<usize>, mut data: Vec<u8>) {
        // Asked to carry on through short sends, the kernel stops short only
        // when the connection breaks, and reports the bytes sent rather than
        // why.
        let error = match sent {
            Ok(sent) if sent == self.len => None,
            Ok(_) => Some(libc::EPIPE),
            Err(err) => Some(err.raw_os_error().unwrap_or(libc::EIO)),
        };
        if self.error.is_none() {
            self.error = error;
        }
        if data.capacity() <= KEPT_WRITE {
            data.clear();
            self.spare = data;
        }
    }

    /// `Ok`, unless a send failed.
    fn outcome(&self) -> io::Result<()> {
        match self.error {
            None => Ok(()),
            Some(code) => Err(io::Error::from_raw_os_error(code)),
        }
    }
}

impl Drop for Writer {
    /// The send in flight is left to finish, rather than cancelled as a
    /// dropped operation is.
    fn drop(&mut self) {
        if let Some(send) = self.send.take() {
            send.detach();
        }
    }
}

// Nothing is pinned: the operations are `Unpin`, and the stream is only
// ever borrowed.
impl<S> Unpin for Compat<S> {}

impl<S: fmt::Debug> fmt::Debug for Compat<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Compat")
            .field("stream", &self.stream)
            .field("pooled", &matches!(self.reader, Reader::Pool { .. }))
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}
