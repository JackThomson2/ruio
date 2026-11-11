//! Networking: sockets whose I/O runs on the I/O completion port of the
//! thread's runtime.
//!
//! This is the part of ruio's `net` that the completion port engine has so
//! far, a proof of concept, under the names and with the signatures it has
//! on io_uring, so that code written to it builds for both: TCP listeners
//! and streams, UDP sockets, and the resolving of addresses.
//!
//! Sockets move data with owned buffers, as the port's operations do: a
//! read takes a buffer and hands it back filled, a write takes one and
//! hands it back once sent, each with its result. Any
//! [`IoBuf`] will do, a slice of one included. The
//! system may use a buffer until its operation completes, so a future
//! dropped before then cancels its operation, and the buffer is freed once
//! the cancellation has completed.
//!
//! An operation borrows its socket, which so stays open until the
//! operation is done with:
//!
//! ```compile_fail
//! # async fn read(stream: ruio::net::TcpStream) {
//! let read = stream.read(Vec::with_capacity(8));
//! drop(stream);
//! let _ = read.await;
//! # }
//! ```
//!
//! Each method's future has a name of its own here, after the method:
//! [`Accept`], [`SendTo`], [`RecvFrom`], and [`Read`] and [`Write`] for a
//! stream's reads and writes and a connected datagram socket's receives and
//! sends alike. So code that names a future names no operation of the
//! engine's.
//!
//! An operation's `timeout` cancels it if it has not completed in time, as
//! a linked timeout does on io_uring: it then fails with `ERROR_TIMEOUT`,
//! of [`io::ErrorKind::TimedOut`], and hands its buffer back, as with any
//! other error. A transfer of a whole buffer, such as
//! [`TcpStream::write_all`]'s, counts the time from its first operation on,
//! and each operation after it has what is left of it.
//!
//! An echo server:
//!
//! ```no_run
//! use ruio::net::TcpListener;
//!
//! #[ruio::main]
//! async fn main() -> std::io::Result<()> {
//!     let listener = TcpListener::bind("127.0.0.1:7878")?;
//!     let mut incoming = listener.incoming();
//!     while let Some(stream) = incoming.next().await {
//!         let stream = stream?;
//!         ruio::spawn(async move {
//!             loop {
//!                 let (read, buf) = stream.read(Vec::with_capacity(4096)).await;
//!                 if !matches!(read, Ok(1..)) {
//!                     break;
//!                 }
//!                 if stream.write_all(buf).await.0.is_err() {
//!                     break;
//!                 }
//!             }
//!         });
//!     }
//!     Ok(())
//! }
//! ```
//!
//! [`TcpStream::connect`] takes this module's [`ToSocketAddrs`]: an
//! address, or a string that parses as one, resolves at once, with no
//! allocation and no runtime, and a host name resolves on the blocking
//! pool, so that the thread's tasks run meanwhile; [`lookup_host`] resolves
//! one so for anything else. The binds take the standard library's trait,
//! and resolve a host name on the thread, as a program binds its listeners
//! as it starts.
//!
//! A stream also receives into a pool of buffers, a
//! [`BufPool`], as a [`RecvStream`]: it takes a
//! buffer only once the connection has data, so that the connections that
//! wait hold no memory, and hands the data back as leases of the pool's
//! buffers.
//!
//! A stream writes several buffers as one, any [`IoBufs`], a bundle it
//! received included, with [`TcpStream::write_all_vectored`]. A write may
//! also be left behind the stream, with [`TcpStream::write_behind`]: the
//! engine sends it and drops its buffers while the task goes on, and the
//! stream's writes after it wait for it, and fail if it failed.
//!
//! Like the port their I/O completes to, sockets belong to their thread:
//! they are neither `Send` nor `Sync`.
//!
//! ```compile_fail
//! fn assert_send<T: Send>() {}
//! assert_send::<ruio::net::TcpStream>();
//! ```
//!
//! The sockets of [`portable`] are `Send` and `Sync`, for code whose
//! futures must be: TCP streams and listeners, which no port has, read and
//! written with calls made at once on whichever thread polls them, which
//! wait on that thread's port only when there is nothing to read or no
//! room to write.
//!
//! Not here yet, of what `net` has on io_uring: datagrams received into a
//! pool (`recv_stream` and `recv_from_stream` of a UDP socket), sockets
//! taken from the standard library (`from_std`), `bind_reuseport`, and
//! moving a stream to another core. Unix sockets are Unix's.

use std::fmt;
use std::future::{Future, poll_fn};
use std::io;
use std::marker::PhantomData;
use std::net::{self, Shutdown, SocketAddr};
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use ruio_iocp::net as iocp;

use crate::buf::{BufBundle, BufPool, IoBuf, IoBufMut, IoBufs};

#[path = "../net/addr.rs"]
mod addr;
#[path = "net/portable.rs"]
pub mod portable;

use addr::resolve;

pub use addr::{ToSocketAddrs, lookup_host};
pub use ruio_iocp::net::{Flush, RecvFrom, SendTo, UdpSocket, WriteAllVectored, WriteBehind};
pub use ruio_iocp::{Read, ReadExact, Write, WriteAll};

/// A TCP listener.
///
/// [`accept`](TcpListener::accept) accepts one connection, and
/// [`incoming`](TcpListener::incoming) one after another.
pub struct TcpListener {
    listener: iocp::TcpListener,
}

impl TcpListener {
    /// A listener bound to `addr`, or to the first address it resolves to
    /// that works.
    ///
    /// A host name is resolved on the thread, as the standard library
    /// resolves it: a program binds its listeners as it starts, where
    /// blocking holds nothing up. A bind that must not block resolves the
    /// name with [`lookup_host`], on the blocking pool, and binds the
    /// addresses.
    ///
    /// # Errors
    ///
    /// What binding and listening report, `InvalidInput` if `addr` resolves
    /// to no address, and an error of kind `Other` where no runtime runs.
    pub fn bind(addr: impl net::ToSocketAddrs) -> io::Result<Self> {
        Ok(Self {
            listener: iocp::TcpListener::bind(addr)?,
        })
    }

    /// The address the listener is bound to.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Accepts a connection, and reports the address of its peer; see
    /// [`Accept`].
    pub fn accept(&self) -> Accept<'_, TcpStream> {
        Accept {
            accept: self.listener.accept(),
            _made: PhantomData,
        }
    }

    /// The connections arriving on the listener, accepted one after
    /// another; see [`Incoming`].
    pub fn incoming(&self) -> Incoming<'_, TcpStream> {
        Incoming {
            listener: &self.listener,
            accept: None,
            _made: PhantomData,
        }
    }
}

impl fmt::Debug for TcpListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.listener, f)
    }
}

/// The future of [`TcpListener::accept`]: the stream of the connection it
/// accepted, and the address of the stream's peer.
///
/// It starts the accept when first polled. Dropping it before the accept
/// completes cancels the accept, and a connection it had accepted by then
/// is closed.
///
/// Its parameters are those it has on io_uring, where one future serves
/// listeners of several kinds: the stream it makes, and the address it
/// reports.
#[must_use = "futures do nothing unless polled"]
pub struct Accept<'a, S, A = SocketAddr> {
    accept: iocp::Accept<'a>,
    _made: PhantomData<fn() -> (S, A)>,
}

impl<S, A> Accept<'_, S, A> {
    /// Cancels the accept if no connection has come `duration` after it
    /// starts: it then fails with [`io::ErrorKind::TimedOut`]. A connection
    /// accepted before the cancellation could stop the accept is the
    /// accept's.
    ///
    /// This has no effect once the accept has started.
    pub fn timeout(self, duration: Duration) -> Self {
        Self {
            accept: self.accept.timeout(duration),
            ..self
        }
    }
}

impl Future for Accept<'_, TcpStream> {
    type Output = io::Result<(TcpStream, SocketAddr)>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let accepted = ready!(Pin::new(&mut self.accept).poll(cx));
        Poll::Ready(accepted.map(|(stream, peer)| (TcpStream { stream }, peer)))
    }
}

impl<S, A> fmt::Debug for Accept<'_, S, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.accept, f)
    }
}

/// The connections arriving on a listener, each accepted as the one before
/// it is taken. Made by [`TcpListener::incoming`].
///
/// Errors are yielded, and the stream carries on after them, as a server
/// would after a failed accept; it never ends. Peers' addresses are not
/// reported. Dropping it cancels the accept in flight, and a connection
/// that one had accepted by then is closed.
///
/// Its parameter is the one it has on io_uring, where it serves listeners
/// of several kinds: the stream it yields.
pub struct Incoming<'a, S> {
    listener: &'a iocp::TcpListener,
    /// The accept in flight, from the poll that starts it to the one it
    /// completes in.
    accept: Option<iocp::Accept<'a>>,
    _made: PhantomData<fn() -> S>,
}

impl Incoming<'_, TcpStream> {
    /// The next connection, or an error the system reported instead.
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<TcpStream>>> {
        let listener = self.listener;
        let accept = self.accept.get_or_insert_with(|| listener.accept());
        let accepted = ready!(Pin::new(accept).poll(cx));
        self.accept = None;
        Poll::Ready(Some(accepted.map(|(stream, _)| TcpStream { stream })))
    }

    /// The next connection; see [`poll_next`](Incoming::poll_next).
    pub async fn next(&mut self) -> Option<io::Result<TcpStream>> {
        poll_fn(|cx| self.poll_next(cx)).await
    }
}

impl<S> fmt::Debug for Incoming<'_, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Incoming")
            .field("listener", self.listener)
            .finish_non_exhaustive()
    }
}

/// Each item is a connection, or an error the system reported instead; the
/// stream never ends.
#[cfg(feature = "futures")]
impl futures_core::Stream for Incoming<'_, TcpStream> {
    type Item = io::Result<TcpStream>;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Incoming::poll_next(self.get_mut(), cx)
    }
}

/// Never terminated: the connections never end.
#[cfg(feature = "futures")]
impl futures_core::stream::FusedStream for Incoming<'_, TcpStream> {
    #[inline]
    fn is_terminated(&self) -> bool {
        false
    }
}

/// A TCP connection.
///
/// Reads and writes take owned buffers and hand them back with the result.
/// A read and a write may be in flight at once, as a task reading a
/// connection and another writing it have them.
///
/// A write may also be left behind the stream, for the engine to finish;
/// see [`write_behind`](TcpStream::write_behind). A stream dropped with one
/// in flight closes once it has been sent.
///
/// The addresses and the options are asked of the system at once. They are
/// `async` because they are on io_uring, where a socket the ring alone
/// holds answers through the ring.
pub struct TcpStream {
    stream: iocp::TcpStream,
}

// The getters and setters wait for nothing here; they are `async` to have
// the signatures they have on io_uring.
#[expect(
    clippy::unused_async,
    reason = "the signatures are io_uring's, where these wait for the ring"
)]
impl TcpStream {
    /// Connects to `addr`, or to the first address it resolves to that
    /// accepts the connection.
    ///
    /// A host name is resolved on the blocking pool, so the thread's other
    /// tasks run meanwhile; an address, or a string that parses as one,
    /// costs no lookup, no allocation and no thread. See [`ToSocketAddrs`].
    ///
    /// # Errors
    ///
    /// What resolving `addr` reports, the error of the last address tried,
    /// `InvalidInput` if `addr` resolves to no address, and an error of
    /// kind `Other` where no runtime runs.
    pub async fn connect(addr: impl ToSocketAddrs) -> io::Result<Self> {
        let mut last = None;
        for addr in resolve(&addr).await? {
            match iocp::TcpStream::connect(addr).await {
                Ok(stream) => return Ok(Self { stream }),
                Err(err) => last = Some(err),
            }
        }
        Err(last.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "could not resolve to any addresses",
            )
        }))
    }

    /// The address of this end of the connection.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub async fn local_addr(&self) -> io::Result<SocketAddr> {
        self.stream.local_addr()
    }

    /// The address of the peer.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub async fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.stream.peer_addr()
    }

    /// Sets whether small writes are sent at once (`TCP_NODELAY`) rather
    /// than gathered.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub async fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        self.stream.set_nodelay(nodelay)
    }

    /// Whether small writes are sent at once.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub async fn nodelay(&self) -> io::Result<bool> {
        self.stream.nodelay()
    }

    /// Sets the time to live of the packets the stream sends (`IP_TTL`).
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub async fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        self.stream.set_ttl(ttl)
    }

    /// The time to live of the packets the stream sends.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub async fn ttl(&self) -> io::Result<u32> {
        self.stream.ttl()
    }

    /// Sets for how long closing the stream waits for what it has left to
    /// send to go (`SO_LINGER`), in whole seconds, or `None` for not at
    /// all, which is the default. A stream that lingers holds up its thread
    /// for up to that long as it is dropped.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub async fn set_linger(&self, linger: Option<Duration>) -> io::Result<()> {
        self.stream.set_linger(linger)
    }

    /// For how long closing the stream waits for what it has left to send
    /// to go, or `None` if it does not.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub async fn linger(&self) -> io::Result<Option<Duration>> {
        self.stream.linger()
    }

    /// Takes the error the socket holds (`SO_ERROR`), if it holds one,
    /// which clears it.
    ///
    /// # Errors
    ///
    /// What asking for it reports.
    pub async fn take_error(&self) -> io::Result<Option<io::Error>> {
        self.stream.take_error()
    }

    /// Reads into `buf`, up to its `bytes_total`, what the peer has sent:
    /// 0 bytes once the peer has shut down; see [`Read`].
    pub fn read<B: IoBufMut>(&self, buf: B) -> Read<'_, B> {
        self.stream.read(buf)
    }

    /// Fills `buf` from its start, up to its `bytes_total`, with as many
    /// reads as that takes; see [`ReadExact`].
    pub fn read_exact<B: IoBufMut>(&self, buf: B) -> ReadExact<'_, B> {
        self.stream.read_exact(buf)
    }

    /// Writes `buf`'s initialized bytes, or as many as the system takes;
    /// see [`Write`]. It waits for a [`write_behind`](Self::write_behind)
    /// in flight, and fails if one failed.
    pub fn write<B: IoBuf>(&self, buf: B) -> Write<'_, B> {
        self.stream.write(buf)
    }

    /// Writes all of `buf`'s initialized bytes, with as many writes as
    /// that takes; see [`WriteAll`]. As [`write`](Self::write), it waits
    /// for a write behind in flight, and fails if one failed.
    pub fn write_all<B: IoBuf>(&self, buf: B) -> WriteAll<'_, B> {
        self.stream.write_all(buf)
    }

    /// Writes all of the initialized bytes of `bufs`, one buffer after
    /// another, such as a bundle received; see [`WriteAllVectored`]. As
    /// [`write`](Self::write), it waits for a write behind in flight, and
    /// fails if one failed.
    pub fn write_all_vectored<V: IoBufs>(&self, bufs: V) -> WriteAllVectored<'_, V> {
        self.stream.write_all_vectored(bufs)
    }

    /// Writes all of the initialized bytes of `bufs` without waiting for
    /// the system to be done with them; see [`WriteBehind`].
    pub fn write_behind<V: IoBufs>(&self, bufs: V) -> WriteBehind<'_, V> {
        self.stream.write_behind(bufs)
    }

    /// Waits for a [`write_behind`](Self::write_behind) in flight to finish,
    /// and reports whether one failed; see [`Flush`].
    pub fn flush(&self) -> Flush<'_> {
        self.stream.flush()
    }

    /// Reports whether a [`write_behind`](Self::write_behind) failed, if
    /// none is in flight, without waiting for one that is: `None` then, as
    /// how it goes is not yet known. Unlike [`flush`](Self::flush), this
    /// arranges for nothing to be woken.
    pub fn try_flush(&self) -> Option<io::Result<()>> {
        self.stream.try_flush()
    }

    /// The data arriving on the connection, received into buffers of
    /// `pool` as it arrives; see [`RecvStream`].
    pub fn recv_stream(&self, pool: &BufPool) -> RecvStream<'_> {
        RecvStream {
            stream: self.stream.recv_stream(pool),
        }
    }

    /// Shuts down the reading or writing half of the connection, or both,
    /// once a [`write_behind`](Self::write_behind) in flight has finished.
    /// The peer's reads find the end of the stream once the writing half
    /// is shut down.
    ///
    /// # Errors
    ///
    /// What a write behind failed with, and what the system reports.
    pub async fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        self.stream.flush().await?;
        self.stream.shutdown(how)
    }
}

impl fmt::Debug for TcpStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.stream, f)
    }
}

/// The data arriving on a connection, received into a [`BufPool`]. Made by
/// [`TcpStream::recv_stream`].
///
/// Each item is one receive's data, as a bundle of leases on buffers of the
/// pool. The stream ends when the peer shuts down, or after yielding an
/// error.
///
/// A stream that waits for data holds no buffer: it takes one of the pool
/// once the connection has data, and waits for one to return when every
/// buffer is leased. A receive fills one buffer here, where io_uring's
/// fills as many as the data waiting takes, so data that fills several
/// comes as several items, which [`next_ready`](RecvStream::next_ready)
/// gathers.
///
/// Dropping the stream leaves what arrives after in the socket, for what
/// reads it next.
pub struct RecvStream<'a> {
    stream: iocp::RecvStream<'a>,
}

impl RecvStream<'_> {
    /// The next data, or `None` once the peer has shut down or an error has
    /// been yielded.
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<BufBundle>>> {
        self.stream.poll_next(cx)
    }

    /// The next data; see [`poll_next`](RecvStream::poll_next).
    pub async fn next(&mut self) -> Option<io::Result<BufBundle>> {
        poll_fn(|cx| self.poll_next(cx)).await
    }

    /// The next data if it has arrived already, or `None`, without waiting.
    /// Appending it to the data taken before gathers everything that has
    /// arrived, to write back as it is:
    ///
    /// ```no_run
    /// # async fn echo(stream: ruio::net::TcpStream, pool: ruio::buf::BufPool) {
    /// let mut received = stream.recv_stream(&pool);
    /// while let Some(Ok(mut data)) = received.next().await {
    ///     while let Some(Ok(more)) = received.next_ready() {
    ///         data.append(more);
    ///     }
    ///     if stream.write_all_vectored(data).await.0.is_err() {
    ///         break;
    ///     }
    /// }
    /// # }
    /// ```
    pub fn next_ready(&mut self) -> Option<io::Result<BufBundle>> {
        self.stream.next_ready()
    }

    /// Whether the stream has ended: the peer has shut down, or an error has
    /// been yielded.
    pub fn is_ended(&self) -> bool {
        self.stream.is_ended()
    }
}

impl fmt::Debug for RecvStream<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.stream, f)
    }
}

#[cfg(feature = "futures")]
impl futures_core::Stream for RecvStream<'_> {
    type Item = io::Result<BufBundle>;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        RecvStream::poll_next(self.get_mut(), cx)
    }
}

/// Terminated once the peer has shut down or an error has been yielded.
#[cfg(feature = "futures")]
impl futures_core::stream::FusedStream for RecvStream<'_> {
    #[inline]
    fn is_terminated(&self) -> bool {
        self.is_ended()
    }
}
