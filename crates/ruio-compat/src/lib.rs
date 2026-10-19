//! Adapters from ruio's streams and files to the poll-based I/O traits that
//! the ecosystem builds on: `AsyncRead`, `AsyncBufRead` and `AsyncWrite` of
//! [`tokio::io`](https://docs.rs/tokio/latest/tokio/io/), with the `tokio`
//! feature, and of [`futures-io`](https://docs.rs/futures-io), with the
//! `futures-io` feature. Both are on by default. tokio's traits need none of
//! its runtime, which ruio's takes the place of.
//!
//! ruio's I/O is completion-based: an operation takes a buffer of its own,
//! the kernel fills or sends it, and the operation hands it back. The
//! poll-based traits lend the caller's buffer for one call instead, which
//! the kernel cannot use once the call returns. [`Compat`] bridges the two
//! with buffers of its own, for a [`TcpStream`] or a [`UnixStream`], a
//! [`File`] at its position, or any other descriptor in an [`AsyncFd`]:
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
//!   of a connection; a file or a descriptor has no half to shut.
//!
//! Like the streams, an adapter belongs to its thread, whose runtime's ring
//! runs its I/O.
//!
//! [`PortableCompat`] adapts ruio's portable streams, of
//! [`ruio::net::portable`], and a child's pipes, of [`ruio::process`], to
//! the same traits with no buffers of its own: they read and write with
//! system calls made at once on whichever thread polls them, so a read goes
//! into the caller's buffer and a write comes from the caller's, and the
//! adapter, as the stream, is `Send` and `Sync`, for what needs a connection
//! that is, as hyper-util's client does. A child's standard output and
//! error are read through it, and its standard input written.
//! With the `client` feature, [`client`] runs hyper-util's client on the
//! portable streams: a connector of portable TCP streams, and the client's
//! builder with ruio's executor and timer.
//!
//! With the `tokio-reactor` feature, [`TokioReactor`] is tokio's reactor as
//! ruio's foreign one ([`ruio::io::set_foreign_reactor`]): set so, the
//! portable streams and listeners and a child's pipes polled on the threads
//! of a tokio runtime wait on that runtime's reactor, as its own sockets do,
//! rather than on the foreign ring, where each wait cost two wakes across
//! threads.
//!
//! With the `hyper` feature, on by default, an adapter has hyper's own I/O
//! traits, `hyper::rt::Read` and `Write`, so hyper serves it, or connects
//! through it, as it is, with nothing between them; [`Executor`] spawns
//! the tasks hyper's HTTP/2 needs on the thread's runtime; and [`Timer`]
//! gives hyper ruio's timers, which its timeouts need: that for reading a
//! request's head, say.
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
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::task::{Context, Poll, Waker, ready};

use ruio::buf::{BufBundle, BufLease, BufPool, BufResult};
use ruio::fs::File;
use ruio::io::AsyncFd;
use ruio::net::{TcpStream, UnixStream};
use ruio::uring::ops::{self, Read, Recv, Send, Write};
use ruio::uring::{Driver, Fd, Op, Operation};

#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "hyper")]
mod executor;
#[cfg(feature = "futures-io")]
mod futures_impls;
#[cfg(feature = "hyper")]
mod hyper_impls;
mod portable;
#[cfg(feature = "hyper")]
mod timer;
#[cfg(feature = "tokio")]
mod tokio_impls;
#[cfg(feature = "tokio-reactor")]
mod tokio_reactor;

#[cfg(feature = "hyper")]
pub use executor::Executor;
pub use portable::{PortableCompat, PortableReader, PortableStream, PortableWriter};
#[cfg(feature = "hyper")]
pub use timer::Timer;
#[cfg(feature = "tokio-reactor")]
pub use tokio_reactor::TokioReactor;

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

/// What a [`Compat`] adapts: a [`TcpStream`] or a [`UnixStream`], a
/// [`File`], read and written at its position, or any descriptor in an
/// [`AsyncFd`]. Its reads and writes are the type's own operations, which
/// take the descriptor rather than borrow the value, so the adapter holds
/// nothing pinned and boxes nothing.
///
/// The trait is sealed: only this crate's types implement it.
pub trait Adaptable: sealed::Sealed {}

impl Adaptable for TcpStream {}
impl Adaptable for UnixStream {}
impl Adaptable for File {}
impl<T: AsRawFd> Adaptable for AsyncFd<T> {}

/// A stream socket of ruio's that a [`Compat`] adapts: a [`TcpStream`] or a
/// [`UnixStream`], which can receive into a pool
/// ([`with_pool`](Compat::with_pool)) as a file cannot.
pub trait StreamSocket: Adaptable {}

impl StreamSocket for TcpStream {}
impl StreamSocket for UnixStream {}

mod sealed {
    use super::*;

    /// What an adapter needs of what it adapts: the descriptor its
    /// operations run on, and the operations, which read into and write
    /// from a vector the adapter owns.
    pub trait Sealed {
        /// The operation that reads into the adapter's buffer: a receive on
        /// a socket, a read at the position of a file.
        type Read: Operation<Output = BufResult<usize, Vec<u8>>>;
        /// The operation that writes the adapter's buffer: a send on a
        /// socket, a write at the position of a file.
        type Write: Operation<Output = BufResult<usize, Vec<u8>>>;
        /// Whether closing shuts down the writing half, as a connection has
        /// one; a file has not.
        const SHUTDOWN: bool;

        fn fd(&self) -> Fd;
        fn read(fd: Fd, buf: Vec<u8>) -> Self::Read;
        fn write(fd: Fd, data: Vec<u8>) -> Self::Write;
    }

    impl Sealed for TcpStream {
        type Read = Recv<Vec<u8>>;
        type Write = Send<Vec<u8>>;
        const SHUTDOWN: bool = true;

        fn fd(&self) -> Fd {
            TcpStream::fd(self)
        }

        fn read(fd: Fd, buf: Vec<u8>) -> Self::Read {
            Recv::new(fd, buf)
        }

        fn write(fd: Fd, data: Vec<u8>) -> Self::Write {
            Send::new(fd, data).flags(SEND_FLAGS)
        }
    }

    impl Sealed for UnixStream {
        type Read = Recv<Vec<u8>>;
        type Write = Send<Vec<u8>>;
        const SHUTDOWN: bool = true;

        fn fd(&self) -> Fd {
            UnixStream::fd(self)
        }

        fn read(fd: Fd, buf: Vec<u8>) -> Self::Read {
            Recv::new(fd, buf)
        }

        fn write(fd: Fd, data: Vec<u8>) -> Self::Write {
            Send::new(fd, data).flags(SEND_FLAGS)
        }
    }

    impl Sealed for File {
        type Read = Read<Vec<u8>>;
        type Write = Write<Vec<u8>>;
        const SHUTDOWN: bool = false;

        fn fd(&self) -> Fd {
            File::fd(self)
        }

        fn read(fd: Fd, buf: Vec<u8>) -> Self::Read {
            Read::new(fd, buf)
        }

        fn write(fd: Fd, data: Vec<u8>) -> Self::Write {
            Write::new(fd, data)
        }
    }

    impl<T: AsRawFd> Sealed for AsyncFd<T> {
        type Read = Read<Vec<u8>>;
        type Write = Write<Vec<u8>>;
        const SHUTDOWN: bool = false;

        fn fd(&self) -> Fd {
            Fd::Plain(self.as_raw_fd())
        }

        fn read(fd: Fd, buf: Vec<u8>) -> Self::Read {
            Read::new(fd, buf)
        }

        fn write(fd: Fd, data: Vec<u8>) -> Self::Write {
            Write::new(fd, data)
        }
    }
}

/// A ruio stream, file or descriptor with the poll-based I/O traits:
/// `AsyncRead`, `AsyncBufRead` and `AsyncWrite` of `tokio::io` and of
/// futures-io; see the [crate documentation](crate) for how they map to the
/// ring, and [`Adaptable`] for what can be adapted.
///
/// Reads go through a buffer of the adapter's, 16 KiB unless made
/// [`with_capacity`](Compat::with_capacity), which a receive fills with
/// whatever has arrived, or, for a stream socket, through the buffers of a
/// pool, made [`with_pool`](Compat::with_pool). Writes copy up to 256 KiB
/// at a time, and send them without waiting for the kernel.
pub struct Compat<S: Adaptable> {
    // Fields drop in order: the operations, of which the receive and the
    // shutdown cancel and the send is left to finish, then the stream,
    // which closes after them.
    reader: Reader<S>,
    writer: Writer<S>,
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
enum Reader<S: Adaptable> {
    /// A buffer of the adapter's, which one read at a time fills.
    Buffer {
        /// The read in flight, which holds the buffer meanwhile.
        read: Option<Op<S::Read>>,
        /// What was read and not yet taken: `buf[pos..]`.
        buf: Vec<u8>,
        pos: usize,
        /// How many bytes a read asks for.
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

impl<S: Adaptable> Reader<S> {
    /// Ready once data has been read and not yet taken, or the stream has
    /// ended. `target` gives the ring and descriptor to read with.
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
                    *read = Some(driver.op(S::read(fd, buf)));
                }
                let op = read.as_mut().expect("a read is in flight");
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

impl<S: Adaptable> Compat<S> {
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
    fn with_reader(reader: Reader<S>, stream: S) -> Self {
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

    /// The stream, to change: the owner of an [`AsyncFd`], say.
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.stream
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
        self.writer.send(driver, fd, data);
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
    /// a connection; a file or a descriptor has no half to shut, so its close
    /// is the wait alone, and it is left open for more.
    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.closed {
            return Poll::Ready(Ok(()));
        }
        if self.shutdown.is_none() {
            ready!(self.writer.poll_done(cx))?;
            if !S::SHUTDOWN {
                return Poll::Ready(Ok(()));
            }
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

impl<S: StreamSocket> Compat<S> {
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
}

/// The ring the adapter runs its operations on, which the first of them
/// takes, and the stream's descriptor for it.
fn target<S: Adaptable>(driver: &mut Option<Driver>, stream: &S) -> (Driver, Fd) {
    let driver = driver.get_or_insert_with(ruio::driver).clone();
    (driver, stream.fd())
}

/// Where an adapter's writes go: one write at a time, which the adapter
/// keeps, in flight or finished, until the next write, flush or close takes
/// its result and its buffer.
///
/// The write is submitted with a waker that wakes nothing, so its completion
/// wakes no task: a task that waits for it, as a write does while the one
/// before is still in flight, leaves its own waker with the write instead.
/// Its result waits in the write's slot of the driver's table meanwhile,
/// where a detached operation's would have the driver copy the operation
/// into the slot and out again to finish it there. A write the kernel cut
/// short, as it may a write to a pipe, goes on with the rest of the buffer,
/// so that the bytes the caller was told were taken all arrive.
struct Writer<S: Adaptable> {
    /// The write of the last write, until its result is taken.
    send: Option<Op<S::Write>>,
    /// How many bytes the write was given.
    len: usize,
    /// Whether a task waits for the write, having left its waker with it,
    /// which a poll with no waker must not replace.
    waited: bool,
    /// The buffer of the last write, emptied, once its write is done with
    /// it, for the next write to fill: a write then allocates nothing.
    spare: Vec<u8>,
    /// What a write failed with, as an errno: the writes, flushes and closes
    /// after it report it as the kernel would have. A write that failed, or
    /// wrote nothing, leaves a gap in the stream, so every write after it
    /// fails, as a stream's writes do once it has broken.
    error: Option<i32>,
    /// The ring and descriptor of the writes, from the first, for carrying
    /// on a write the kernel cut short.
    target: Option<(Driver, Fd)>,
}

impl<S: Adaptable> Default for Writer<S> {
    fn default() -> Self {
        Self {
            send: None,
            len: 0,
            waited: false,
            spare: Vec::new(),
            error: None,
            target: None,
        }
    }
}

impl<S: Adaptable> Writer<S> {
    /// Writes `data` on `fd`, with nobody waiting for it.
    fn send(&mut self, driver: Driver, fd: Fd, data: Vec<u8>) {
        self.len = data.len();
        let mut send = driver.op(S::write(fd, data));
        self.target = Some((driver, fd));
        match Pin::new(&mut send).poll(&mut Context::from_waker(Waker::noop())) {
            // Refused by the ring: it fails at once.
            Poll::Ready((sent, data)) => {
                if let Some(rest) = self.finish(sent, data) {
                    self.send_rest(rest);
                }
            }
            Poll::Pending => self.send = Some(send),
        }
    }

    /// Writes `rest`, what a write the kernel cut short left, as the next
    /// write; nobody waits for it either.
    #[cold]
    fn send_rest(&mut self, rest: Vec<u8>) {
        let (driver, fd) = self.target.clone().expect("a write has a target");
        self.send(driver, fd, rest);
    }

    /// Ready once no write is in flight, having taken the result of the last
    /// one: `Ok`, unless a write failed.
    fn poll_done(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while let Some(send) = &mut self.send {
            let Poll::Ready((sent, data)) = Pin::new(send).poll(cx) else {
                self.waited = true;
                return Poll::Pending;
            };
            self.send = None;
            self.waited = false;
            if let Some(rest) = self.finish(sent, data) {
                self.send_rest(rest);
            }
        }
        Poll::Ready(self.outcome())
    }

    /// Takes the result of the last write if it has finished, without
    /// waiting, unless a task waits for it, whose waker the write keeps.
    fn take_ready(&mut self) {
        if self.waited {
            return;
        }
        if let Some(send) = &mut self.send
            && let Poll::Ready((sent, data)) =
                Pin::new(send).poll(&mut Context::from_waker(Waker::noop()))
        {
            self.send = None;
            if let Some(rest) = self.finish(sent, data) {
                self.send_rest(rest);
            }
        }
    }

    /// Records how the last write went, and keeps its buffer for the next
    /// write unless it has grown past what an idle connection should hold;
    /// or, for a write the kernel cut short, gives back the rest of the
    /// buffer, to write next.
    fn finish(&mut self, sent: io::Result<usize>, mut data: Vec<u8>) -> Option<Vec<u8>> {
        let error = match sent {
            Ok(sent) if sent == self.len => None,
            // Asked to carry on through short sends, a socket stops short
            // only when the connection breaks; a pipe or a file takes what
            // it has room for, and the rest goes next.
            Ok(sent) if sent > 0 => {
                data.drain(..sent);
                return Some(data);
            }
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
        None
    }

    /// `Ok`, unless a send failed.
    fn outcome(&self) -> io::Result<()> {
        match self.error {
            None => Ok(()),
            Some(code) => Err(io::Error::from_raw_os_error(code)),
        }
    }
}

impl<S: Adaptable> Drop for Writer<S> {
    /// The write in flight is left to finish, rather than cancelled as a
    /// dropped operation is.
    fn drop(&mut self) {
        if let Some(send) = self.send.take() {
            send.detach();
        }
    }
}

// Nothing is pinned: the operations are `Unpin`, and the stream is only
// ever borrowed.
impl<S: Adaptable> Unpin for Compat<S> {}

impl<S: Adaptable + fmt::Debug> fmt::Debug for Compat<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Compat")
            .field("stream", &self.stream)
            .field("pooled", &matches!(self.reader, Reader::Pool { .. }))
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}
