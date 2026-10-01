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
//! - A write copies the caller's bytes and writes them behind the stream,
//!   as [`TcpStream::write_behind`] does: it is done, as far as the caller
//!   can tell, once the send is on its way, and the next write waits for it
//!   to finish. How the send went comes out later: a failure fails the
//!   writes, flushes and closes after it. Dropping the adapter leaves the
//!   send to finish, and the stream closes after it.
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

use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::io::{self, IoSlice};
use std::mem;
use std::net::Shutdown;
use std::pin::Pin;
use std::task::{Context, Poll, Waker, ready};

use ruio::buf::{BufLease, BufPool};
use ruio::net::{Flush, TcpStream, UnixStream, WriteBehind};
use ruio::uring::ops::{self, Recv};
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

/// A stream socket of ruio's that a [`Compat`] adapts: a [`TcpStream`] or a
/// [`UnixStream`].
pub trait StreamSocket: sealed::Sealed {}

impl StreamSocket for TcpStream {}
impl StreamSocket for UnixStream {}

mod sealed {
    use super::*;

    /// What an adapter needs of its stream.
    pub trait Sealed {
        fn fd(&self) -> Fd;

        fn write_behind(&self, bufs: Vec<Vec<u8>>) -> WriteBehind<'_, Vec<Vec<u8>>>;

        fn flush(&self) -> Flush<'_>;
    }

    impl Sealed for TcpStream {
        fn fd(&self) -> Fd {
            TcpStream::fd(self)
        }

        fn write_behind(&self, bufs: Vec<Vec<u8>>) -> WriteBehind<'_, Vec<Vec<u8>>> {
            TcpStream::write_behind(self, bufs)
        }

        fn flush(&self) -> Flush<'_> {
            TcpStream::flush(self)
        }
    }

    impl Sealed for UnixStream {
        fn fd(&self) -> Fd {
            UnixStream::fd(self)
        }

        fn write_behind(&self, bufs: Vec<Vec<u8>>) -> WriteBehind<'_, Vec<Vec<u8>>> {
            UnixStream::write_behind(self, bufs)
        }

        fn flush(&self) -> Flush<'_> {
            UnixStream::flush(self)
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
/// and are written behind the stream.
pub struct Compat<S> {
    // Fields drop in order: the operations, which cancel, then the stream,
    // which closes after them.
    reader: Reader,
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
        /// What was received and not yet taken: the first lease's data from
        /// `pos`, then the others'.
        leases: VecDeque<BufLease>,
        pos: usize,
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
                leases,
                pos,
                error,
            } => {
                if !leases.is_empty() {
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
                        leases.extend(bundle);
                        *pos = 0;
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
            leases,
            pos,
            error,
            ..
        } = self
        else {
            return;
        };
        if !leases.is_empty() || error.is_some() {
            return;
        }
        match recv.next_ready() {
            Some(Ok(bundle)) => {
                leases.extend(bundle);
                *pos = 0;
            }
            Some(Err(err)) => *error = Some(err),
            None => {}
        }
    }

    /// Data received and not yet taken, as it lies in one buffer: empty if
    /// there is none.
    fn buffered(&self) -> &[u8] {
        match self {
            Reader::Buffer { buf, pos, .. } => &buf[*pos..],
            Reader::Pool { leases, pos, .. } => leases.front().map_or(&[], |lease| &lease[*pos..]),
        }
    }

    /// Takes `amount` bytes of [`buffered`](Reader::buffered).
    fn consume(&mut self, amount: usize) {
        match self {
            Reader::Buffer { buf, pos, .. } => *pos = (*pos + amount).min(buf.len()),
            Reader::Pool { leases, pos, .. } => {
                let Some(lease) = leases.front() else {
                    return;
                };
                *pos = (*pos + amount).min(lease.len());
                if *pos == lease.len() {
                    // Back to the pool at once, for the kernel to fill again.
                    leases.pop_front();
                    *pos = 0;
                }
            }
        }
    }
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
                leases: VecDeque::new(),
                pos: 0,
                error: None,
            },
            stream,
        )
    }

    fn with_reader(reader: Reader, stream: S) -> Self {
        Self {
            reader,
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
        ready!(self.poll_behind(cx))?;
        let len = total.min(MAX_WRITE);
        let mut data = Vec::with_capacity(len);
        for buf in bufs {
            let take = buf.len().min(len - data.len());
            data.extend_from_slice(&buf[..take]);
        }
        // Nothing is in flight now, so the write goes ahead at once.
        ready!(Pin::new(&mut self.stream.write_behind(vec![data])).poll(cx))?;
        Poll::Ready(Ok(len))
    }

    /// Waits for the write in flight, and reports how the writes went.
    fn poll_behind(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream.flush()).poll(cx)
    }

    /// Reports how the writes went, as far as is known, without waiting for
    /// the write in flight, whose failure the next write, flush or close
    /// reports.
    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Asked with a waker that does nothing, as nobody waits here.
        let mut asking = Context::from_waker(Waker::noop());
        match Pin::new(&mut self.stream.flush()).poll(&mut asking) {
            Poll::Pending => Poll::Ready(Ok(())),
            known => known,
        }
    }

    /// Waits for the write in flight, then shuts down the writing half of
    /// the connection.
    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.closed {
            return Poll::Ready(Ok(()));
        }
        if self.shutdown.is_none() {
            ready!(self.poll_behind(cx))?;
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
