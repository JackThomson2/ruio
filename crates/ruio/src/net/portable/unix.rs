//! Portable Unix streams and listeners.

use std::fmt;
use std::io::{self, IoSlice};
use std::mem::{ManuallyDrop, MaybeUninit};
use std::net::Shutdown;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::{self, SocketAddr};
use std::path::Path;
use std::task::{Context, Poll, ready};

use ruio_driver::SockAddr;
use ruio_driver::buf::{IoBuf, IoBufMut, IoBufs};

use super::{Listener, Stream, connect, poll_accept, socket};
use crate::io::{
    AsyncRead, AsyncWrite, PortableRead, PortableReadExact, PortableWrite, PortableWriteAll,
    PortableWriteAllVectored,
};
use crate::net::tcp::listen;

/// A Unix stream socket that is `Send` and `Sync`, as a portable
/// [`TcpStream`](super::TcpStream) is; see the [module's
/// documentation](super).
pub struct UnixStream {
    stream: Stream,
}

impl UnixStream {
    /// Connects to the socket bound to `path`.
    ///
    /// # Errors
    ///
    /// What `connect(2)` reports, such as `NotFound` for a path no socket is
    /// bound to and `ConnectionRefused` for one whose listener has gone, and
    /// `WouldBlock` if the listener's backlog is full; `InvalidInput` for a
    /// path too long for a socket address, or with a nul byte.
    pub async fn connect(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::connect_to(SockAddr::unix(path)?).await
    }

    /// Connects to the socket bound to `addr`: a path, or a name in the
    /// abstract namespace.
    ///
    /// # Errors
    ///
    /// As for [`connect`](UnixStream::connect).
    pub async fn connect_addr(addr: &SocketAddr) -> io::Result<Self> {
        Self::connect_to(SockAddr::try_from(addr)?).await
    }

    async fn connect_to(addr: SockAddr) -> io::Result<Self> {
        let stream = Stream::from_nonblocking(socket(libc::AF_UNIX, libc::SOCK_STREAM)?);
        connect(&stream.io, &addr).await?;
        Ok(Self { stream })
    }

    /// Two streams connected to each other.
    ///
    /// # Errors
    ///
    /// What `socketpair(2)` reports.
    pub fn pair() -> io::Result<(Self, Self)> {
        let (one, other) = net::UnixStream::pair()?;
        Ok((Self::from_std(one)?, Self::from_std(other)?))
    }

    /// Takes over a connected socket from the standard library, made
    /// nonblocking.
    ///
    /// # Errors
    ///
    /// What making it nonblocking reports.
    pub fn from_std(stream: net::UnixStream) -> io::Result<Self> {
        Ok(Self {
            stream: Stream::new(stream.into())?,
        })
    }

    /// The standard library's stream, blocking again, which closes at once
    /// when dropped, as it does.
    pub fn into_std(self) -> net::UnixStream {
        net::UnixStream::from(self.stream.io.into_owned_fd())
    }

    /// Runs `f` on the standard library's view of the socket.
    fn with_std<R>(&self, f: impl FnOnce(&net::UnixStream) -> R) -> R {
        // SAFETY: the descriptor is the stream's own Unix socket, open for as
        // long as the stream is, which the view does not outlive; the view is
        // never dropped, so it closes nothing.
        let view = ManuallyDrop::new(unsafe { net::UnixStream::from_raw_fd(self.as_raw_fd()) });
        f(&view)
    }

    /// The address the socket is bound to, unnamed unless it was bound.
    ///
    /// # Errors
    ///
    /// What `getsockname(2)` reports.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.with_std(net::UnixStream::local_addr)
    }

    /// The peer's address.
    ///
    /// # Errors
    ///
    /// What `getpeername(2)` reports.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.with_std(net::UnixStream::peer_addr)
    }

    stream_methods!();
}

stream_impls!(UnixStream);

impl TryFrom<net::UnixStream> for UnixStream {
    type Error = io::Error;

    /// Made nonblocking, as [`from_std`](UnixStream::from_std) makes it.
    fn try_from(stream: net::UnixStream) -> io::Result<Self> {
        Self::from_std(stream)
    }
}

impl fmt::Debug for UnixStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnixStream")
            .field("fd", &self.as_raw_fd())
            .finish()
    }
}

/// A Unix stream socket listening for connections, which is `Send` and
/// `Sync`, and accepts [`UnixStream`]s, as a portable
/// [`TcpListener`](super::TcpListener) accepts TCP connections.
pub struct UnixListener {
    listener: Listener,
}

impl UnixListener {
    /// A listener bound to `path`, with a backlog of 1024 connections.
    ///
    /// # Errors
    ///
    /// What binding and listening report: `AddrInUse` if a file is at the
    /// path already.
    pub fn bind(path: impl AsRef<Path>) -> io::Result<Self> {
        let listener = net::UnixListener::bind(path)?;
        listen(listener.as_raw_fd())?;
        Self::from_std(listener)
    }

    /// A listener bound to `addr`, a path or a name in the abstract
    /// namespace, as [`bind`](UnixListener::bind) binds one.
    ///
    /// # Errors
    ///
    /// As for [`bind`](UnixListener::bind).
    pub fn bind_addr(addr: &SocketAddr) -> io::Result<Self> {
        let listener = net::UnixListener::bind_addr(addr)?;
        listen(listener.as_raw_fd())?;
        Self::from_std(listener)
    }

    /// Takes over a listening socket from the standard library, made
    /// nonblocking.
    ///
    /// # Errors
    ///
    /// What making it nonblocking reports.
    pub fn from_std(listener: net::UnixListener) -> io::Result<Self> {
        Ok(Self {
            listener: Listener::new(listener.into())?,
        })
    }

    /// The standard library's listener, blocking again.
    pub fn into_std(self) -> net::UnixListener {
        net::UnixListener::from(self.listener.io.into_owned_fd())
    }

    /// The address the listener is bound to.
    ///
    /// # Errors
    ///
    /// What `getsockname(2)` reports.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        // SAFETY: the descriptor is the listener's own socket, open for as
        // long as the listener is, which the view does not outlive; the view
        // is never dropped, so it closes nothing.
        let view = ManuallyDrop::new(unsafe { net::UnixListener::from_raw_fd(self.as_raw_fd()) });
        view.local_addr()
    }

    /// Takes the next connection, and the peer's address, unnamed unless the
    /// peer bound its socket, waiting for one if none is waiting.
    ///
    /// # Errors
    ///
    /// What `accept4(2)` reports, bar a connection given up before it was
    /// taken, which the next replaces; and, for a wait where no runtime runs
    /// and neither the foreign reactor nor a foreign ring takes it, `ENOSYS`,
    /// whose kind is [`io::ErrorKind::Unsupported`].
    pub async fn accept(&self) -> io::Result<(UnixStream, SocketAddr)> {
        let mut wait = ruio_driver::ReadyWait::new();
        let (fd, addr) =
            std::future::poll_fn(|cx| poll_accept(&self.listener.io, cx, &mut wait)).await?;
        accepted(fd, &addr)
    }

    /// Takes the next connection, as [`accept`](UnixListener::accept) does,
    /// polled: a wait the listener keeps until it is polled again, so that the
    /// next poll goes on with it.
    ///
    /// # Errors
    ///
    /// As for [`accept`](UnixListener::accept).
    pub fn poll_accept(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<(UnixStream, SocketAddr)>> {
        let listener = &mut self.listener;
        let (fd, addr) = ready!(poll_accept(&listener.io, cx, &mut listener.wait))?;
        Poll::Ready(accepted(fd, &addr))
    }
}

/// The stream of a connection accepted, with its peer's address.
fn accepted(fd: OwnedFd, addr: &SockAddr) -> io::Result<(UnixStream, SocketAddr)> {
    let stream = UnixStream {
        stream: Stream::from_nonblocking(fd),
    };
    let peer = addr.as_unix().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "an accepted Unix connection's address is not a Unix address",
        )
    })?;
    Ok((stream, peer))
}

impl AsRawFd for UnixListener {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        self.listener.io.fd().as_raw_fd()
    }
}

impl AsFd for UnixListener {
    #[inline]
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.listener.io.fd().as_fd()
    }
}

impl TryFrom<net::UnixListener> for UnixListener {
    type Error = io::Error;

    /// Made nonblocking, as [`from_std`](UnixListener::from_std) makes it.
    fn try_from(listener: net::UnixListener) -> io::Result<Self> {
        Self::from_std(listener)
    }
}

impl fmt::Debug for UnixListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnixListener")
            .field("fd", &self.as_raw_fd())
            .finish()
    }
}
