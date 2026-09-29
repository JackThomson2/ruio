//! TCP: listeners, and the streams they accept or that connect to them.

use std::cell::Cell;
use std::fmt;
use std::future::poll_fn;
use std::io;
use std::marker::PhantomData;
use std::net::{self, Shutdown, SocketAddr, ToSocketAddrs};
use std::os::fd::{AsRawFd, RawFd};
use std::rc::Rc;
use std::task::{Context, Poll, ready};

use ruio_driver::buf::{BufPool, IoBuf, IoBufMut, IoBufs};
use ruio_driver::ops::{self, Accept, AcceptMulti, AcceptStream, Connect, Recv, Send, Socket};
use ruio_driver::{Descriptor, Driver, Fd};

use super::behind::{Behind, Flush, WriteBehind};
use super::socket::{
    ReadExact, RecvStream, SendZc, Sock, SocketOp, ThreadBound, WriteAll, WriteAllVectored,
};

/// How many connections a listener's backlog holds, as in tokio. The
/// standard library's 128 overflows with a burst of clients.
const BACKLOG: i32 = 1024;

/// Makes a listening socket's backlog [`BACKLOG`] long. Listening again
/// only changes the backlog.
pub(crate) fn listen(fd: RawFd) -> io::Result<()> {
    // SAFETY: `listen` touches no memory.
    if unsafe { libc::listen(fd, BACKLOG) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// A TCP socket listening for connections.
///
/// [`incoming`](TcpListener::incoming) accepts connection after connection
/// with one multishot request; [`accept`](TcpListener::accept) accepts one,
/// and reports the peer's address.
///
/// The connections are plain descriptors unless
/// [`set_direct`](TcpListener::set_direct) asks for direct ones.
pub struct TcpListener {
    socket: net::TcpListener,
    direct: Cell<bool>,
    _thread: ThreadBound,
}

impl TcpListener {
    /// A listener bound to `addr`, or to the first address it resolves to
    /// that works, with `SO_REUSEADDR` and a backlog of 1024 connections.
    ///
    /// Resolving a host name blocks the thread, as it does in the standard
    /// library.
    ///
    /// # Errors
    ///
    /// What binding and listening report, or `InvalidInput` if `addr`
    /// resolves to no address.
    pub fn bind(addr: impl ToSocketAddrs) -> io::Result<Self> {
        let socket = net::TcpListener::bind(addr)?;
        listen(socket.as_raw_fd())?;
        Ok(Self::from_std(socket))
    }

    /// Wraps a listening socket from the standard library.
    pub fn from_std(socket: net::TcpListener) -> Self {
        Self {
            socket,
            direct: Cell::new(false),
            _thread: PhantomData,
        }
    }

    /// The address the listener is bound to.
    ///
    /// # Errors
    ///
    /// What `getsockname(2)` reports.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Makes the connections accepted from now on direct descriptors of the
    /// thread's ring, or plain descriptors again.
    ///
    /// A direct descriptor saves the kernel looking up the socket for each
    /// operation, and costs no file descriptor of the process. But only the
    /// ring can use it: a direct [`TcpStream`] cannot tell its local address
    /// or change socket options, and knows its peer's address only if
    /// [`accept`](TcpListener::accept) accepted it.
    ///
    /// The ring's registered file table holds the direct descriptors; see
    /// [`ruio_driver::Builder::files`]. When it is full, the kernel closes the
    /// connection it could not put there and the accept fails with `ENFILE`;
    /// [`Incoming`] then carries on with plain descriptors.
    pub fn set_direct(&self, direct: bool) {
        self.direct.set(direct);
    }

    /// Accepts a connection, with the peer's address.
    pub fn accept(&self) -> SocketOp<'_, Accept, io::Result<(TcpStream, SocketAddr)>> {
        let driver = crate::driver();
        let mut accept = Accept::new(self.socket.as_raw_fd());
        if self.direct.get() {
            accept = accept.direct(&driver);
        }
        SocketOp::new(driver.op(accept), |accepted| {
            let (descriptor, peer) = accepted?;
            let peer = peer.as_inet().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "not an internet address")
            })?;
            Ok((TcpStream::new(descriptor, Some(peer)), peer))
        })
    }

    /// The connections arriving on the listener, accepted with one multishot
    /// request as they come.
    pub fn incoming(&self) -> Incoming<'_, TcpStream> {
        Incoming::new(self.socket.as_raw_fd(), self.direct.get(), |accepted| {
            TcpStream::new(accepted, None)
        })
    }
}

impl AsRawFd for TcpListener {
    fn as_raw_fd(&self) -> RawFd {
        self.socket.as_raw_fd()
    }
}

impl fmt::Debug for TcpListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpListener")
            .field("socket", &self.socket)
            .field("direct", &self.direct.get())
            .finish()
    }
}

/// The connections arriving on a listener, accepted by a multishot accept
/// that the stream keeps armed. Made by [`TcpListener::incoming`] and
/// [`UnixListener::incoming`](super::UnixListener::incoming).
///
/// Errors are yielded, and the stream carries on after them, as a server
/// would after a failed `accept(2)`; it never ends. Peers' addresses are not
/// reported.
pub struct Incoming<'a, S> {
    accept: AcceptStream,
    driver: Driver,
    listener: RawFd,
    direct: bool,
    make: fn(Descriptor) -> S,
    _listener: PhantomData<&'a ()>,
}

impl<S> Incoming<'_, S> {
    pub(crate) fn new(listener: RawFd, direct: bool, make: fn(Descriptor) -> S) -> Self {
        let driver = crate::driver();
        Self {
            accept: AcceptStream::new(&driver, Self::request(&driver, listener, direct)),
            driver,
            listener,
            direct,
            make,
            _listener: PhantomData,
        }
    }

    fn request(driver: &Driver, listener: RawFd, direct: bool) -> AcceptMulti {
        let request = AcceptMulti::new(listener);
        if direct {
            request.direct(driver)
        } else {
            request
        }
    }

    /// The next connection, or an error the kernel reported instead.
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<S>>> {
        let accepted = ready!(self.accept.poll_next(cx));
        if self.direct
            && let Err(err) = &accepted
            && err.raw_os_error() == Some(libc::ENFILE)
        {
            // The file table is full, and the kernel closed the connection
            // it had no slot for. Plain descriptors keep the rest coming.
            self.direct = false;
            let request = Self::request(&self.driver, self.listener, false);
            self.accept = AcceptStream::new(&self.driver, request);
        }
        Poll::Ready(Some(accepted.map(self.make)))
    }

    /// The next connection; see [`poll_next`](Incoming::poll_next).
    pub async fn next(&mut self) -> Option<io::Result<S>> {
        poll_fn(|cx| self.poll_next(cx)).await
    }
}

impl<S> fmt::Debug for Incoming<'_, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Incoming")
            .field("listener", &self.listener)
            .field("direct", &self.direct)
            .finish_non_exhaustive()
    }
}

/// A TCP connection.
///
/// Reads and writes take owned buffers and hand them back with the result.
/// [`recv_stream`](TcpStream::recv_stream) receives into a [`BufPool`]
/// instead, with one multishot request, and
/// [`write_behind`](TcpStream::write_behind) writes without waiting for the
/// kernel.
pub struct TcpStream {
    sock: Sock<net::TcpStream>,
    /// The peer's address, where the kernel told it: a direct descriptor
    /// cannot ask.
    peer: Option<SocketAddr>,
    /// The writes behind the stream, which every write waits for.
    behind: Rc<Behind>,
    _thread: ThreadBound,
}

impl TcpStream {
    pub(crate) fn new(descriptor: Descriptor, peer: Option<SocketAddr>) -> Self {
        Self {
            sock: Sock::new(descriptor),
            peer,
            behind: Rc::default(),
            _thread: PhantomData,
        }
    }

    /// Connects to `addr`, or to the first address it resolves to that
    /// accepts the connection.
    ///
    /// Resolving a host name blocks the thread, as it does in the standard
    /// library.
    ///
    /// # Errors
    ///
    /// The error of the last address tried, or `InvalidInput` if `addr`
    /// resolves to no address.
    pub async fn connect(addr: impl ToSocketAddrs) -> io::Result<Self> {
        let mut last = None;
        for addr in addr.to_socket_addrs()? {
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

    async fn connect_to(addr: SocketAddr) -> io::Result<Self> {
        let driver = crate::driver();
        let domain = if addr.is_ipv4() {
            libc::AF_INET
        } else {
            libc::AF_INET6
        };
        let socket = driver.op(Socket::new(domain, libc::SOCK_STREAM, 0)).await?;
        driver.op(Connect::new(socket.fd(), addr)).await?;
        Ok(Self::new(socket, Some(addr)))
    }

    /// Wraps a connected socket from the standard library.
    pub fn from_std(stream: net::TcpStream) -> Self {
        Self {
            sock: Sock::Plain(stream),
            peer: None,
            behind: Rc::default(),
            _thread: PhantomData,
        }
    }

    /// The local address of the connection.
    ///
    /// # Errors
    ///
    /// What `getsockname(2)` reports, and `Unsupported` for a direct
    /// descriptor.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.sock.plain()?.local_addr()
    }

    /// The peer's address.
    ///
    /// # Errors
    ///
    /// What `getpeername(2)` reports, and `Unsupported` for a direct
    /// descriptor from [`TcpListener::incoming`], which has no way to learn
    /// it.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        match (&self.sock, self.peer) {
            (Sock::Plain(stream), _) => stream.peer_addr(),
            (Sock::Direct(_), Some(peer)) => Ok(peer),
            (Sock::Direct(_), None) => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "the peer's address of a direct descriptor from a multishot accept is unknown",
            )),
        }
    }

    /// Sets `TCP_NODELAY`, which sends small writes at once rather than
    /// holding them back to fill a segment.
    ///
    /// # Errors
    ///
    /// What `setsockopt(2)` reports, and `Unsupported` for a direct
    /// descriptor.
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        self.sock.plain()?.set_nodelay(nodelay)
    }

    /// Whether `TCP_NODELAY` is set.
    ///
    /// # Errors
    ///
    /// As for [`set_nodelay`](TcpStream::set_nodelay).
    pub fn nodelay(&self) -> io::Result<bool> {
        self.sock.plain()?.nodelay()
    }

    /// Whether the stream is a direct descriptor; see
    /// [`TcpListener::set_direct`].
    pub fn is_direct(&self) -> bool {
        self.sock.is_direct()
    }

    /// The descriptor, for operations of one's own on the thread's ring.
    pub fn fd(&self) -> Fd {
        self.sock.fd()
    }

    /// Reads into `buf`, from its start up to its
    /// [`bytes_total`](IoBuf::bytes_total); reads 0 bytes once the peer has
    /// shut down.
    pub fn read<B: IoBufMut>(&self, buf: B) -> SocketOp<'_, Recv<B>> {
        self.sock.read(buf)
    }

    /// Reads until `buf` is full; see [`ReadExact`].
    pub fn read_exact<B: IoBufMut>(&self, buf: B) -> ReadExact<'_, B> {
        self.sock.read_exact(buf)
    }

    /// Writes `buf`'s initialized bytes, or as many as the socket takes.
    ///
    /// Like every write, it waits for a [`write_behind`](Self::write_behind)
    /// in flight, and fails if one failed.
    pub fn write<B: IoBuf>(&self, buf: B) -> SocketOp<'_, Send<B>> {
        self.sock.write(buf).behind(&self.behind)
    }

    /// Writes all of `buf`'s initialized bytes; see [`WriteAll`].
    pub fn write_all<B: IoBuf>(&self, buf: B) -> WriteAll<'_, B> {
        self.sock.write_all(buf).behind(&self.behind)
    }

    /// Writes all of the initialized bytes of `bufs`, one buffer after
    /// another, such as a bundle received; see [`WriteAllVectored`].
    pub fn write_all_vectored<V: IoBufs>(&self, bufs: V) -> WriteAllVectored<'_, V> {
        self.sock.write_all_vectored(bufs).behind(&self.behind)
    }

    /// Writes all of the initialized bytes of `bufs` without waiting for the
    /// kernel to be done with them, which frees the task from waking up for
    /// it; see [`WriteBehind`].
    ///
    /// How the write went comes out later: a failure, or a write cut short,
    /// fails the next write, [`flush`](Self::flush) or
    /// [`shutdown`](Self::shutdown), and every one after it.
    pub fn write_behind<V: IoBufs>(&self, bufs: V) -> WriteBehind<'_, V> {
        self.sock.write_behind(&self.behind, bufs)
    }

    /// Waits for a [`write_behind`](Self::write_behind) in flight to finish,
    /// and reports whether one failed; see [`Flush`].
    pub fn flush(&self) -> Flush<'_> {
        Flush::new(&self.behind)
    }

    /// Writes all of `buf`'s initialized bytes without copying them; see
    /// [`ops::SendZc`].
    pub fn send_zc<B: IoBuf>(&self, buf: B) -> SendZc<'_, B> {
        self.sock.send_zc(buf).behind(&self.behind)
    }

    /// The data arriving on the connection, received into `pool`, which
    /// must belong to the stream's ring, as it arrives.
    pub fn recv_stream(&self, pool: &BufPool) -> RecvStream<'_> {
        self.sock.recv_stream(pool)
    }

    /// Shuts down the reading or writing half of the connection, or both,
    /// once a [`write_behind`](Self::write_behind) in flight has finished.
    pub fn shutdown(&self, how: Shutdown) -> SocketOp<'_, ops::Shutdown> {
        self.sock.shutdown(how).behind(&self.behind)
    }
}

impl fmt::Debug for TcpStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpStream")
            .field("socket", &self.sock)
            .finish_non_exhaustive()
    }
}

impl From<net::TcpStream> for TcpStream {
    fn from(stream: net::TcpStream) -> Self {
        Self::from_std(stream)
    }
}

impl From<net::TcpListener> for TcpListener {
    fn from(listener: net::TcpListener) -> Self {
        Self::from_std(listener)
    }
}
