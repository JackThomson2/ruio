//! Unix sockets: streams, their listeners, and datagrams.

use std::cell::Cell;
use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::net::Shutdown;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::{self, SocketAddr};
use std::path::Path;
use std::rc::Rc;

use ruio_driver::buf::{BufPool, BufResult, IoBuf, IoBufMut, IoBufs};
use ruio_driver::ops::{self, Accept, Connect, Recv, RecvFrom, Send, SendTo, Socket};
use ruio_driver::{Descriptor, Fd, SockAddr};

use super::behind::{Behind, Flush, WriteBehind};
use super::socket::{
    ReadExact, RecvStream, Sock, SocketOp, ThreadBound, WriteAll, WriteAllVectored,
};
use super::tcp::{Incoming, listen};

/// A Unix socket listening for connections.
///
/// Like a [`TcpListener`](super::TcpListener), it accepts connections one
/// at a time or as they come, as plain descriptors or, on request, direct
/// ones.
pub struct UnixListener {
    socket: net::UnixListener,
    direct: Cell<bool>,
    _thread: ThreadBound,
}

impl UnixListener {
    /// A listener bound to `path`, with a backlog of 1024 connections.
    ///
    /// # Errors
    ///
    /// What binding and listening report; binding fails if `path` exists.
    pub fn bind(path: impl AsRef<Path>) -> io::Result<Self> {
        let socket = net::UnixListener::bind(path)?;
        listen(socket.as_raw_fd())?;
        Ok(Self::from_std(socket))
    }

    /// Wraps a listening socket from the standard library.
    pub fn from_std(socket: net::UnixListener) -> Self {
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

    /// Makes the connections accepted from now on direct descriptors, or
    /// plain ones again; see
    /// [`TcpListener::set_direct`](super::TcpListener::set_direct).
    pub fn set_direct(&self, direct: bool) {
        self.direct.set(direct);
    }

    /// Accepts a connection, with the peer's address, which is usually
    /// unnamed.
    pub fn accept(&self) -> SocketOp<'_, Accept, io::Result<(UnixStream, SocketAddr)>> {
        let driver = crate::driver();
        let mut accept = Accept::new(self.socket.as_raw_fd());
        if self.direct.get() {
            accept = accept.direct(&driver);
        }
        SocketOp::new(driver.op(accept), |accepted| {
            let (descriptor, peer) = accepted?;
            Ok((UnixStream::new(descriptor), unix(peer)?))
        })
    }

    /// The connections arriving on the listener, accepted with one multishot
    /// request as they come.
    pub fn incoming(&self) -> Incoming<'_, UnixStream> {
        Incoming::new(self.socket.as_raw_fd(), self.direct.get(), UnixStream::new)
    }
}

impl AsRawFd for UnixListener {
    fn as_raw_fd(&self) -> RawFd {
        self.socket.as_raw_fd()
    }
}

impl fmt::Debug for UnixListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnixListener")
            .field("socket", &self.socket)
            .field("direct", &self.direct.get())
            .finish()
    }
}

/// A connected Unix stream socket.
///
/// It reads and writes like a [`TcpStream`](super::TcpStream), except that
/// it cannot send zero-copy.
pub struct UnixStream {
    sock: Sock<net::UnixStream>,
    /// The writes behind the stream, which every write waits for.
    behind: Rc<Behind>,
    _thread: ThreadBound,
}

impl UnixStream {
    fn new(descriptor: Descriptor) -> Self {
        Self {
            sock: Sock::new(descriptor),
            behind: Rc::default(),
            _thread: PhantomData,
        }
    }

    /// Connects to the socket bound to `path`.
    ///
    /// # Errors
    ///
    /// What `connect(2)` reports, and `InvalidInput` if `path` does not fit
    /// in a Unix socket address.
    pub async fn connect(path: impl AsRef<Path>) -> io::Result<Self> {
        let addr = SockAddr::unix(path)?;
        let driver = crate::driver();
        let socket = driver
            .op(Socket::new(libc::AF_UNIX, libc::SOCK_STREAM, 0))
            .await?;
        driver.op(Connect::new(socket.fd(), addr)).await?;
        Ok(Self::new(socket))
    }

    /// A pair of connected sockets.
    ///
    /// # Errors
    ///
    /// What `socketpair(2)` reports.
    pub fn pair() -> io::Result<(Self, Self)> {
        let (a, b) = net::UnixStream::pair()?;
        Ok((Self::from_std(a), Self::from_std(b)))
    }

    /// Wraps a connected socket from the standard library.
    pub fn from_std(stream: net::UnixStream) -> Self {
        Self {
            sock: Sock::Plain(stream),
            behind: Rc::default(),
            _thread: PhantomData,
        }
    }

    /// The socket's own address.
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
    /// descriptor.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.sock.plain()?.peer_addr()
    }

    /// Whether the stream is a direct descriptor; see
    /// [`UnixListener::set_direct`].
    pub fn is_direct(&self) -> bool {
        self.sock.is_direct()
    }

    /// The descriptor, for operations of one's own on the thread's ring.
    pub fn fd(&self) -> Fd {
        self.sock.fd()
    }

    /// Reads into `buf`; see
    /// [`TcpStream::read`](super::TcpStream::read).
    pub fn read<B: IoBufMut>(&self, buf: B) -> SocketOp<'_, Recv<B>> {
        self.sock.read(buf)
    }

    /// Reads until `buf` is full; see [`ReadExact`].
    pub fn read_exact<B: IoBufMut>(&self, buf: B) -> ReadExact<'_, B> {
        self.sock.read_exact(buf)
    }

    /// Writes `buf`'s initialized bytes, or as many as the socket takes.
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
    /// kernel to be done with them; see
    /// [`TcpStream::write_behind`](super::TcpStream::write_behind).
    pub fn write_behind<V: IoBufs>(&self, bufs: V) -> WriteBehind<'_, V> {
        self.sock.write_behind(&self.behind, bufs)
    }

    /// Waits for a [`write_behind`](Self::write_behind) in flight to finish,
    /// and reports whether one failed; see [`Flush`].
    pub fn flush(&self) -> Flush<'_> {
        Flush::new(&self.behind)
    }

    /// The data arriving on the connection, received into `pool`, which
    /// must belong to the stream's ring, as it arrives.
    pub fn recv_stream(&self, pool: &BufPool) -> RecvStream<'_> {
        self.sock.recv_stream(pool)
    }

    /// Shuts down the reading or writing half of the connection, or both.
    pub fn shutdown(&self, how: Shutdown) -> SocketOp<'_, ops::Shutdown> {
        self.sock.shutdown(how).behind(&self.behind)
    }
}

impl fmt::Debug for UnixStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnixStream")
            .field("socket", &self.sock)
            .finish_non_exhaustive()
    }
}

impl From<net::UnixStream> for UnixStream {
    fn from(stream: net::UnixStream) -> Self {
        Self::from_std(stream)
    }
}

/// A Unix datagram socket.
pub struct UnixDatagram {
    sock: Sock<net::UnixDatagram>,
    _thread: ThreadBound,
}

impl UnixDatagram {
    /// A socket bound to `path`.
    ///
    /// # Errors
    ///
    /// What binding reports; it fails if `path` exists.
    pub fn bind(path: impl AsRef<Path>) -> io::Result<Self> {
        net::UnixDatagram::bind(path).map(Self::from_std)
    }

    /// A socket with no address, which can send but not be sent to.
    ///
    /// # Errors
    ///
    /// What `socket(2)` reports.
    pub fn unbound() -> io::Result<Self> {
        net::UnixDatagram::unbound().map(Self::from_std)
    }

    /// A pair of connected sockets.
    ///
    /// # Errors
    ///
    /// What `socketpair(2)` reports.
    pub fn pair() -> io::Result<(Self, Self)> {
        let (a, b) = net::UnixDatagram::pair()?;
        Ok((Self::from_std(a), Self::from_std(b)))
    }

    /// Wraps a socket from the standard library.
    pub fn from_std(socket: net::UnixDatagram) -> Self {
        Self {
            sock: Sock::Plain(socket),
            _thread: PhantomData,
        }
    }

    fn socket(&self) -> &net::UnixDatagram {
        match &self.sock {
            Sock::Plain(socket) => socket,
            Sock::Direct(_) => unreachable!("Unix datagram sockets have plain descriptors"),
        }
    }

    /// Connects the socket to the one bound to `path`, which
    /// [`send`](UnixDatagram::send) then sends to, and
    /// [`recv`](UnixDatagram::recv) only receives from.
    ///
    /// # Errors
    ///
    /// As for [`std::os::unix::net::UnixDatagram::connect`].
    pub fn connect(&self, path: impl AsRef<Path>) -> io::Result<()> {
        self.socket().connect(path)
    }

    /// The socket's own address.
    ///
    /// # Errors
    ///
    /// What `getsockname(2)` reports.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket().local_addr()
    }

    /// The address of the socket this one is connected to.
    ///
    /// # Errors
    ///
    /// What `getpeername(2)` reports: `NotConnected` if it is not.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.socket().peer_addr()
    }

    /// The descriptor, for operations of one's own.
    pub fn fd(&self) -> Fd {
        self.sock.fd()
    }

    /// Sends `buf`'s initialized bytes as a datagram to the socket this one
    /// is connected to.
    pub fn send<B: IoBuf>(&self, buf: B) -> SocketOp<'_, Send<B>> {
        self.sock.write(buf)
    }

    /// Receives a datagram from the socket this one is connected to.
    pub fn recv<B: IoBufMut>(&self, buf: B) -> SocketOp<'_, Recv<B>> {
        self.sock.read(buf)
    }

    /// Sends `buf`'s initialized bytes as a datagram to the socket bound to
    /// `path`.
    ///
    /// A `path` that makes no address, being empty, too long, or holding a
    /// nul byte, fails the send with `EINVAL`
    /// ([`io::ErrorKind::InvalidInput`]), as the kernel fails a send to the
    /// unnamed address.
    pub fn send_to<B: IoBuf>(&self, buf: B, path: impl AsRef<Path>) -> SocketOp<'_, SendTo<B>> {
        let addr = SockAddr::unix(path).unwrap_or_else(|_| unnamed());
        self.sock.op(|fd| SendTo::new(fd, buf, addr))
    }

    /// Sends `buf`'s initialized bytes as a datagram to `addr`, such as an
    /// address [`recv_from`](UnixDatagram::recv_from) reported.
    pub fn send_to_addr<B: IoBuf>(&self, buf: B, addr: &SocketAddr) -> SocketOp<'_, SendTo<B>> {
        let addr = SockAddr::try_from(addr).unwrap_or_else(|_| unnamed());
        self.sock.op(|fd| SendTo::new(fd, buf, addr))
    }

    /// Receives a datagram, and reports the address it came from, which is
    /// unnamed if the sender has none.
    pub fn recv_from<B: IoBufMut>(
        &self,
        buf: B,
    ) -> SocketOp<'_, RecvFrom<B>, BufResult<(usize, SocketAddr), B>> {
        self.sock.op_map(
            |fd| RecvFrom::new(fd, buf),
            |(received, buf)| {
                let received = received.and_then(|(len, from)| Ok((len, unix(from)?)));
                (received, buf)
            },
        )
    }

    /// The datagrams arriving on the socket, each received into a buffer of
    /// `pool`, which must belong to the thread's ring.
    pub fn recv_stream(&self, pool: &BufPool) -> RecvStream<'_> {
        self.sock.recv_stream(pool)
    }
}

/// The unnamed address, which the kernel refuses to send to.
fn unnamed() -> SockAddr {
    SockAddr::unix("").expect("the unnamed address fits")
}

/// The Unix socket address the kernel reported.
fn unix(addr: SockAddr) -> io::Result<SocketAddr> {
    addr.as_unix()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "not a Unix socket address"))
}

impl fmt::Debug for UnixDatagram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnixDatagram")
            .field("socket", self.socket())
            .finish_non_exhaustive()
    }
}

impl From<net::UnixDatagram> for UnixDatagram {
    fn from(socket: net::UnixDatagram) -> Self {
        Self::from_std(socket)
    }
}
