//! Sockets that are `Send` and `Sync`: TCP streams, and the listeners that
//! accept them, which a future that must be `Send`, one that `async_trait`
//! boxes or one spawned onto another core, can hold across an `.await`, and
//! any thread can read and write.
//!
//! A portable socket is a plain socket, made nonblocking, which no port
//! has. Its reads and writes are Winsock's calls made at once, on whichever
//! thread polls them, which take what has arrived or give what there is
//! room for, with no turn of a port's loop; only one that finds nothing to
//! read, or no room, waits, with a poll of the AFD driver's that completes
//! to the port of the thread that polls it, and is made again once the
//! socket is ready. A future that moves to another thread mid-wait takes
//! the wait along, whose port then wakes it there. Each read and write that
//! moves data spends a step of the task's budget ([`coop`](crate::coop)),
//! so a peer that keeps a socket full cannot hold up the thread. Nothing is
//! read but what a read returns, so a read dropped while it waits loses
//! nothing.
//!
//! A thread where no runtime runs, one of another executor's say, reads and
//! writes a portable socket the same way. Its waits go to the foreign
//! reactor ([`set_foreign_reactor`](crate::io::set_foreign_reactor)) where
//! the thread is one of its, which then says when the socket may be ready,
//! as for that executor's own sockets. With none, a wait, which has no port
//! to run on, fails with an error of kind [`io::ErrorKind::Unsupported`],
//! so the caller can do its I/O another way.
//!
//! The streams read into and write from owned buffers, as the thread's
//! sockets do, and also, through their poll methods, into and from the
//! caller's own, with no copy, as the poll-based I/O traits read and write.
//! Connecting is a `connect` made at once, which may connect at once, and
//! otherwise waits for the socket to be writable, as it is once the
//! connection is made or has failed; accepting is an `accept`, made at
//! once, waiting while there is no connection to take. Socket options and
//! addresses are plain calls, made with no port and no `.await`.
//!
//! The sockets of [`net`](super), whose operations complete to their
//! thread's port, receive into pools of buffers and leave writes behind,
//! are what a server on one core is fastest with: they belong to their
//! thread. These are for code that has to be `Send`. What io_uring has here
//! besides, `bind_reuseport` and Unix sockets, Windows has not.
//!
//! ```no_run
//! use ruio::net::portable::{TcpListener, TcpStream};
//!
//! fn assert_send<T: Send + Sync>(_: &T) {}
//!
//! # ruio::Runtime::new().unwrap().block_on(async {
//! let listener = TcpListener::bind("127.0.0.1:0")?;
//! let addr = listener.local_addr()?;
//! let echo = ruio::spawn(async move {
//!     let (stream, _) = listener.accept().await?;
//!     let (read, buf) = stream.read(Vec::with_capacity(64)).await;
//!     read?;
//!     stream.write_all(buf).await.0
//! });
//! let stream = TcpStream::connect(addr).await?;
//! assert_send(&stream);
//! // A future that must be `Send`, holding the stream across its awaits.
//! let exchange: std::pin::Pin<Box<dyn Future<Output = std::io::Result<Vec<u8>>> + Send + '_>> =
//!     Box::pin(async {
//!         stream.write_all("hello").await.0?;
//!         let (read, reply) = stream.read_exact(Vec::with_capacity(5)).await;
//!         read.map(|()| reply)
//!     });
//! assert_eq!(exchange.await?, b"hello");
//! echo.await.unwrap()?;
//! # Ok::<(), std::io::Error>(())
//! # }).unwrap();
//! ```

use std::fmt;
use std::future::poll_fn;
use std::io::{self, IoSlice};
use std::mem::{ManuallyDrop, MaybeUninit};
use std::net::{self, Shutdown, SocketAddr};
use std::os::windows::io::{AsRawSocket, AsSocket, BorrowedSocket, FromRawSocket, RawSocket};
use std::task::{Context, Poll, ready};
use std::time::Duration;

use ruio_iocp::portable::{ReadyWait, Socket};

use super::addr::{ToSocketAddrs, resolve};
use crate::buf::{IoBuf, IoBufMut, IoBufs};
use crate::io::{
    AsyncRead, AsyncWrite, Nonblocking, PortableRead, PortableReadExact, PortableWrite,
    PortableWriteAll, PortableWriteAllVectored, READABLE, WRITABLE,
};

/// What a portable stream holds: its socket, and the waits of its poll
/// methods, which take the stream mutably and so have them to themselves.
struct Stream {
    /// The wait of `poll_read`.
    read: ReadyWait,
    /// The wait of `poll_write` and `poll_write_vectored`.
    write: ReadyWait,
    io: Nonblocking,
}

impl Stream {
    fn new(socket: Socket) -> Self {
        Self {
            read: ReadyWait::new(),
            write: ReadyWait::new(),
            io: Nonblocking::new(socket),
        }
    }
}

/// A TCP connection that is `Send` and `Sync`: its reads and writes are
/// calls made at once on whichever thread polls them, which wait on that
/// thread's port only when there is nothing to read or no room to write;
/// see the [module's documentation](self).
///
/// It reads into and writes from owned buffers, as the thread's
/// [`TcpStream`](crate::net::TcpStream) does, and, through its poll methods,
/// into and from the caller's own buffers with no copy.
pub struct TcpStream {
    stream: Stream,
    /// The peer's address, where connecting or accepting told it, which
    /// spares [`peer_addr`](TcpStream::peer_addr) the asking.
    peer: Option<SocketAddr>,
}

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
    /// or `InvalidInput` if `addr` resolves to no address; and, for a
    /// connection that is not made at once where no runtime runs and the
    /// foreign reactor does not take the wait, an error of kind
    /// [`io::ErrorKind::Unsupported`].
    pub async fn connect(addr: impl ToSocketAddrs) -> io::Result<Self> {
        let mut last = None;
        for addr in resolve(&addr).await? {
            match Self::connect_to(addr).await {
                Ok(stream) => return Ok(stream),
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

    /// Connects to `addr`: a `connect` made at once, which connects at once
    /// or starts connecting, and then a wait for the socket to be writable,
    /// as it is once the connection is made or has failed, which `SO_ERROR`
    /// then says.
    async fn connect_to(addr: SocketAddr) -> io::Result<Self> {
        let (socket, connected) = Socket::connect(&addr)?;
        let stream = Self {
            stream: Stream::new(socket),
            peer: Some(addr),
        };
        if !connected {
            stream.stream.io.ready(WRITABLE).await?;
            if let Some(err) = stream.take_error()? {
                return Err(err);
            }
        }
        Ok(stream)
    }

    /// Takes over a connected socket from the standard library, made
    /// nonblocking.
    ///
    /// # Errors
    ///
    /// What making it nonblocking reports.
    pub fn from_std(stream: net::TcpStream) -> io::Result<Self> {
        Ok(Self {
            stream: Stream::new(Socket::new(stream.into())?),
            peer: None,
        })
    }

    /// The standard library's stream, blocking again, which closes at once
    /// when dropped, as it does.
    pub fn into_std(self) -> net::TcpStream {
        net::TcpStream::from(self.stream.io.into_owned_socket())
    }

    /// Runs `f` on the standard library's view of the socket.
    fn with_std<R>(&self, f: impl FnOnce(&net::TcpStream) -> R) -> R {
        // SAFETY: the socket is the stream's own TCP socket, open for as long
        // as the stream is, which the view does not outlive; the view is
        // never dropped, so it closes nothing.
        let view =
            ManuallyDrop::new(unsafe { net::TcpStream::from_raw_socket(self.as_raw_socket()) });
        f(&view)
    }

    /// The local address of the connection, with one `getsockname`.
    ///
    /// # Errors
    ///
    /// What `getsockname` reports.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.with_std(net::TcpStream::local_addr)
    }

    /// The peer's address: the one connected to, or accepted from, or else
    /// what `getpeername` reports.
    ///
    /// # Errors
    ///
    /// What `getpeername` reports: `NotConnected` if the stream is not.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        match self.peer {
            Some(peer) => Ok(peer),
            None => self.with_std(net::TcpStream::peer_addr),
        }
    }

    /// Sets `TCP_NODELAY`: whether a write goes out at once, rather than
    /// waiting for the peer to acknowledge the last as Nagle's algorithm has
    /// it; with one `setsockopt`.
    ///
    /// # Errors
    ///
    /// What `setsockopt` reports.
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        self.with_std(|stream| stream.set_nodelay(nodelay))
    }

    /// `TCP_NODELAY`, with one `getsockopt`.
    ///
    /// # Errors
    ///
    /// What `getsockopt` reports.
    pub fn nodelay(&self) -> io::Result<bool> {
        self.with_std(net::TcpStream::nodelay)
    }

    /// Sets `IP_TTL`, the time to live of the packets sent.
    ///
    /// # Errors
    ///
    /// What `setsockopt` reports.
    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        self.with_std(|stream| stream.set_ttl(ttl))
    }

    /// `IP_TTL`.
    ///
    /// # Errors
    ///
    /// What `getsockopt` reports.
    pub fn ttl(&self) -> io::Result<u32> {
        self.with_std(net::TcpStream::ttl)
    }

    /// Sets `SO_LINGER`: for how long closing the stream waits for data not
    /// yet sent to go, in whole seconds, or `None` for not at all, which is
    /// the default. A stream that lingers for no time resets its connection
    /// as it closes, discarding what is left.
    ///
    /// # Errors
    ///
    /// What `setsockopt` reports.
    pub fn set_linger(&self, linger: Option<Duration>) -> io::Result<()> {
        self.stream.io.socket().set_linger(linger)
    }

    /// `SO_LINGER`.
    ///
    /// # Errors
    ///
    /// What `getsockopt` reports.
    pub fn linger(&self) -> io::Result<Option<Duration>> {
        self.stream.io.socket().linger()
    }

    /// Reads into `buf`, up to its capacity, what has arrived: a call made
    /// at once, which waits on the port of the thread polling it only if
    /// nothing has. Reads 0 bytes once the peer has shut its writing half
    /// down.
    #[inline]
    pub fn read<B: IoBufMut>(&self, buf: B) -> PortableRead<'_, B> {
        self.stream.io.read(buf)
    }

    /// Reads until `buf` is full, with as many reads as it takes, each a
    /// call made at once, waiting whenever nothing has arrived;
    /// `UnexpectedEof` if the stream ends first. See [`PortableReadExact`].
    #[inline]
    pub fn read_exact<B: IoBufMut>(&self, buf: B) -> PortableReadExact<'_, B> {
        self.stream.io.read_exact(buf)
    }

    /// Writes `buf`'s initialized bytes, or as many as there is room for: a
    /// call made at once, which waits on the port of the thread polling it
    /// only if there is no room.
    #[inline]
    pub fn write<B: IoBuf>(&self, buf: B) -> PortableWrite<'_, B> {
        self.stream.io.write(buf)
    }

    /// Writes all of `buf`'s initialized bytes, with as many writes as it
    /// takes, each a call made at once, waiting whenever there is no room.
    /// See [`PortableWriteAll`].
    #[inline]
    pub fn write_all<B: IoBuf>(&self, buf: B) -> PortableWriteAll<'_, B> {
        self.stream.io.write_all(buf)
    }

    /// Writes every initialized byte of `bufs`, one after the other, with
    /// as many vectored writes as it takes, each of up to 64 buffers.
    #[inline]
    pub fn write_all_vectored<V: IoBufs>(&self, bufs: V) -> PortableWriteAllVectored<'_, V> {
        self.stream.io.write_all_vectored(bufs)
    }

    /// Reads into the caller's `buf` what has arrived, as
    /// [`read`](Self::read) does, polled: a call made at once, and if
    /// nothing has arrived, a wait the stream keeps until it is polled
    /// again, so that the next poll goes on with it. The first bytes of
    /// `buf`, as many as were read, are then initialized.
    ///
    /// The waits of the poll methods are the stream's own, which they
    /// borrow it mutably to keep; the owned-buffer methods have their own. A
    /// wait runs on the port of the thread polling it; see the
    /// [module's documentation](self).
    ///
    /// # Errors
    ///
    /// What Winsock reports, and, for a wait where no runtime runs and the
    /// foreign reactor does not take it, an error of kind
    /// [`io::ErrorKind::Unsupported`].
    pub fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [MaybeUninit<u8>],
    ) -> Poll<io::Result<usize>> {
        let stream = &mut self.stream;
        stream.io.poll_read(cx, buf, &mut stream.read)
    }

    /// Writes as much of the caller's `buf` as there is room for, as
    /// [`write`](Self::write) does, polled; see
    /// [`poll_read`](Self::poll_read).
    ///
    /// # Errors
    ///
    /// As for [`poll_read`](Self::poll_read).
    pub fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let stream = &mut self.stream;
        stream.io.poll_write(cx, buf, &mut stream.write)
    }

    /// Writes as much of the caller's `bufs`, one after the other, as there
    /// is room for, with one `WSASend`; see
    /// [`poll_write`](Self::poll_write).
    ///
    /// # Errors
    ///
    /// As for [`poll_read`](Self::poll_read).
    pub fn poll_write_vectored(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let stream = &mut self.stream;
        stream.io.poll_write_vectored(cx, bufs, &mut stream.write)
    }

    /// Reads into `buf` what has arrived, with one call and no wait:
    /// `WouldBlock` if nothing has.
    ///
    /// # Errors
    ///
    /// `WouldBlock`, and what Winsock reports.
    pub fn try_read(&self, buf: &mut [u8]) -> io::Result<usize> {
        // SAFETY: the read writes none but initialized bytes, so the bytes
        // stay initialized.
        let buf = unsafe { &mut *(std::ptr::from_mut(buf) as *mut [MaybeUninit<u8>]) };
        self.stream.io.try_read(buf)
    }

    /// Writes as much of `buf` as there is room for, with one call and no
    /// wait: `WouldBlock` if there is none.
    ///
    /// # Errors
    ///
    /// `WouldBlock`, and what Winsock reports.
    pub fn try_write(&self, buf: &[u8]) -> io::Result<usize> {
        self.stream.io.try_write(buf)
    }

    /// Writes as much of `bufs` as there is room for, with one call and no
    /// wait: `WouldBlock` if there is none.
    ///
    /// # Errors
    ///
    /// `WouldBlock`, and what Winsock reports.
    pub fn try_write_vectored(&self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        self.stream.io.try_write_vectored(bufs)
    }

    /// Waits for the stream to be readable: for data, the end, or an error
    /// to have arrived, which a read then takes, with a poll on the port of
    /// the thread polling it. Another reader may take it first, so a
    /// [`try_read`](Self::try_read) after it can still find nothing.
    ///
    /// # Errors
    ///
    /// What the system reports for the poll, and, where no runtime runs and
    /// the foreign reactor does not take the wait, an error of kind
    /// [`io::ErrorKind::Unsupported`].
    pub async fn readable(&self) -> io::Result<()> {
        self.stream.io.ready(READABLE).await.map(drop)
    }

    /// Waits for the stream to be writable, as
    /// [`readable`](Self::readable) waits for it to be readable.
    ///
    /// # Errors
    ///
    /// As for [`readable`](Self::readable).
    pub async fn writable(&self) -> io::Result<()> {
        self.stream.io.ready(WRITABLE).await.map(drop)
    }

    /// Shuts down the reading half, the writing half, or both, with one
    /// `shutdown`: the peer reads the end once it has read what was written
    /// before.
    ///
    /// # Errors
    ///
    /// What `shutdown` reports: `NotConnected` if the stream is not.
    pub fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        self.with_std(|stream| stream.shutdown(how))
    }

    /// The error pending on the socket, which reading `SO_ERROR` clears.
    ///
    /// # Errors
    ///
    /// What `getsockopt` reports.
    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        self.with_std(net::TcpStream::take_error)
    }
}

impl AsRawSocket for TcpStream {
    #[inline]
    fn as_raw_socket(&self) -> RawSocket {
        self.stream.io.socket().as_raw_socket()
    }
}

impl AsSocket for TcpStream {
    #[inline]
    fn as_socket(&self) -> BorrowedSocket<'_> {
        self.stream.io.socket().as_socket()
    }
}

impl AsyncRead for TcpStream {
    type Read<'a, B: IoBufMut> = PortableRead<'a, B>;

    #[inline]
    fn read<B: IoBufMut>(&self, buf: B) -> Self::Read<'_, B> {
        TcpStream::read(self, buf)
    }
}

impl AsyncWrite for TcpStream {
    type Write<'a, B: IoBuf> = PortableWrite<'a, B>;

    #[inline]
    fn write<B: IoBuf>(&self, buf: B) -> Self::Write<'_, B> {
        TcpStream::write(self, buf)
    }
}

impl TryFrom<net::TcpStream> for TcpStream {
    type Error = io::Error;

    /// Made nonblocking, as [`from_std`](TcpStream::from_std) makes it.
    fn try_from(stream: net::TcpStream) -> io::Result<Self> {
        Self::from_std(stream)
    }
}

impl fmt::Debug for TcpStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpStream")
            .field("socket", &self.as_raw_socket())
            .field("peer", &self.peer)
            .finish()
    }
}

/// A TCP socket listening for connections, which is `Send` and `Sync`, and
/// accepts [`TcpStream`]s: with an `accept` made at once, which waits on
/// the port of the thread polling it only while there is no connection to
/// take.
pub struct TcpListener {
    /// The wait of `poll_accept`.
    wait: ReadyWait,
    io: Nonblocking,
}

impl TcpListener {
    /// A listener bound to `addr`, as the thread's
    /// [`TcpListener`](crate::net::TcpListener) binds one: with the backlog
    /// of the standard library's listeners, and, on an IPv6 address,
    /// accepting connections from IPv4 addresses too, whose peers it reports
    /// in their IPv6 form.
    ///
    /// A host name is resolved on the thread, as the standard library
    /// resolves it: a program binds its listeners as it starts.
    ///
    /// # Errors
    ///
    /// What binding and listening report, or `InvalidInput` if `addr`
    /// resolves to no address.
    pub fn bind(addr: impl net::ToSocketAddrs) -> io::Result<Self> {
        Ok(Self::with(Socket::listen(addr)?))
    }

    fn with(socket: Socket) -> Self {
        Self {
            wait: ReadyWait::new(),
            io: Nonblocking::new(socket),
        }
    }

    /// Takes over a listening socket from the standard library, made
    /// nonblocking.
    ///
    /// # Errors
    ///
    /// What making it nonblocking reports.
    pub fn from_std(listener: net::TcpListener) -> io::Result<Self> {
        Ok(Self::with(Socket::new(listener.into())?))
    }

    /// The standard library's listener, blocking again.
    pub fn into_std(self) -> net::TcpListener {
        net::TcpListener::from(self.io.into_owned_socket())
    }

    /// The address the listener is bound to, with one `getsockname`.
    ///
    /// # Errors
    ///
    /// What `getsockname` reports.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        // SAFETY: the socket is the listener's own, open for as long as the
        // listener is, which the view does not outlive; the view is never
        // dropped, so it closes nothing.
        let view =
            ManuallyDrop::new(unsafe { net::TcpListener::from_raw_socket(self.as_raw_socket()) });
        view.local_addr()
    }

    /// Takes the next connection, and the peer's address, waiting for one if
    /// none is waiting.
    ///
    /// # Errors
    ///
    /// What `accept` reports, bar a connection reset before it was taken,
    /// which the next replaces; and, for a wait where no runtime runs and the
    /// foreign reactor does not take it, an error of kind
    /// [`io::ErrorKind::Unsupported`].
    pub async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        let mut wait = ReadyWait::new();
        let taken = poll_fn(|cx| self.io.poll_take(cx, READABLE, &mut wait, try_accept)).await;
        taken.map(accepted)
    }

    /// Takes the next connection, as [`accept`](TcpListener::accept) does,
    /// polled: a wait the listener keeps until it is polled again, so that
    /// the next poll goes on with it.
    ///
    /// # Errors
    ///
    /// As for [`accept`](TcpListener::accept).
    pub fn poll_accept(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<(TcpStream, SocketAddr)>> {
        let taken = ready!(self.io.poll_take(cx, READABLE, &mut self.wait, try_accept));
        Poll::Ready(taken.map(accepted))
    }
}

/// Takes a connection from the listener of `io`, with one `accept` made at
/// once.
fn try_accept(io: &Nonblocking) -> io::Result<(Socket, SocketAddr)> {
    io.socket().try_accept()
}

/// The stream of a connection accepted, with its peer's address.
fn accepted((socket, peer): (Socket, SocketAddr)) -> (TcpStream, SocketAddr) {
    let stream = TcpStream {
        stream: Stream::new(socket),
        peer: Some(peer),
    };
    (stream, peer)
}

impl AsRawSocket for TcpListener {
    #[inline]
    fn as_raw_socket(&self) -> RawSocket {
        self.io.socket().as_raw_socket()
    }
}

impl AsSocket for TcpListener {
    #[inline]
    fn as_socket(&self) -> BorrowedSocket<'_> {
        self.io.socket().as_socket()
    }
}

impl TryFrom<net::TcpListener> for TcpListener {
    type Error = io::Error;

    /// Made nonblocking, as [`from_std`](TcpListener::from_std) makes it.
    fn try_from(listener: net::TcpListener) -> io::Result<Self> {
        Self::from_std(listener)
    }
}

impl fmt::Debug for TcpListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpListener")
            .field("socket", &self.as_raw_socket())
            .finish()
    }
}
