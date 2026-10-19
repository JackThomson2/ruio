//! Portable TCP streams and listeners.

use std::fmt;
use std::io::{self, IoSlice};
use std::mem::{ManuallyDrop, MaybeUninit};
use std::net::{self, Shutdown, SocketAddr};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, RawFd};
use std::task::{Context, Poll, ready};
use std::time::Duration;

use ruio_driver::SockAddr;
use ruio_driver::buf::{IoBuf, IoBufMut, IoBufs};

use super::{Listener, Stream, connect, poll_accept, socket};
use crate::io::{
    AsyncRead, AsyncWrite, PortableRead, PortableReadExact, PortableWrite, PortableWriteAll,
    PortableWriteAllVectored,
};
use crate::net::addr::{ToSocketAddrs, resolve};
use crate::net::socket::{bind_reuseport, getsockopt, setsockopt};
use crate::net::tcp::listen;

/// A TCP connection that is `Send` and `Sync`: its reads and writes are
/// system calls made at once on whichever thread polls them, which wait on
/// that thread's ring only when there is nothing to read or no room to
/// write; see the [module's documentation](super).
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
    /// or `InvalidInput` if `addr` resolves to no address.
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

    async fn connect_to(addr: SocketAddr) -> io::Result<Self> {
        let domain = if addr.is_ipv4() {
            libc::AF_INET
        } else {
            libc::AF_INET6
        };
        let stream = Stream::from_nonblocking(socket(domain, libc::SOCK_STREAM)?);
        connect(&stream.io, &SockAddr::from(addr)).await?;
        Ok(Self {
            stream,
            peer: Some(addr),
        })
    }

    /// Takes over a connected socket from the standard library, made
    /// nonblocking.
    ///
    /// # Errors
    ///
    /// What making it nonblocking reports.
    pub fn from_std(stream: net::TcpStream) -> io::Result<Self> {
        Ok(Self {
            stream: Stream::new(stream.into())?,
            peer: None,
        })
    }

    /// The standard library's stream, blocking again, which closes at once
    /// when dropped, as it does.
    pub fn into_std(self) -> net::TcpStream {
        net::TcpStream::from(self.stream.io.into_owned_fd())
    }

    /// Runs `f` on the standard library's view of the socket.
    fn with_std<R>(&self, f: impl FnOnce(&net::TcpStream) -> R) -> R {
        // SAFETY: the descriptor is the stream's own TCP socket, open for as
        // long as the stream is, which the view does not outlive; the view
        // is never dropped, so it closes nothing.
        let view = ManuallyDrop::new(unsafe { net::TcpStream::from_raw_fd(self.as_raw_fd()) });
        f(&view)
    }

    /// The local address of the connection, with one `getsockname(2)`.
    ///
    /// # Errors
    ///
    /// What `getsockname(2)` reports.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.with_std(net::TcpStream::local_addr)
    }

    /// The peer's address: the one connected to, or accepted from, or else
    /// what `getpeername(2)` reports.
    ///
    /// # Errors
    ///
    /// What `getpeername(2)` reports: `NotConnected` if the stream is not.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        match self.peer {
            Some(peer) => Ok(peer),
            None => self.with_std(net::TcpStream::peer_addr),
        }
    }

    /// Sets `TCP_NODELAY`: whether a write goes out at once, rather than
    /// waiting for the peer to acknowledge the last as Nagle's algorithm has
    /// it; with one `setsockopt(2)`.
    ///
    /// # Errors
    ///
    /// What `setsockopt(2)` reports.
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        self.with_std(|stream| stream.set_nodelay(nodelay))
    }

    /// `TCP_NODELAY`, with one `getsockopt(2)`.
    ///
    /// # Errors
    ///
    /// What `getsockopt(2)` reports.
    pub fn nodelay(&self) -> io::Result<bool> {
        self.with_std(net::TcpStream::nodelay)
    }

    /// Sets `IP_TTL`, the time to live of the packets sent.
    ///
    /// # Errors
    ///
    /// What `setsockopt(2)` reports.
    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        self.with_std(|stream| stream.set_ttl(ttl))
    }

    /// `IP_TTL`.
    ///
    /// # Errors
    ///
    /// What `getsockopt(2)` reports.
    pub fn ttl(&self) -> io::Result<u32> {
        self.with_std(net::TcpStream::ttl)
    }

    /// Sets `SO_LINGER`: for how long closing the stream waits for data not
    /// yet sent to go, or `None` for not at all, which is the default.
    ///
    /// # Errors
    ///
    /// What `setsockopt(2)` reports.
    pub fn set_linger(&self, linger: Option<Duration>) -> io::Result<()> {
        let value = libc::linger {
            l_onoff: i32::from(linger.is_some()),
            l_linger: linger.map_or(0, |linger| {
                i32::try_from(linger.as_secs()).unwrap_or(i32::MAX)
            }),
        };
        setsockopt(self.as_raw_fd(), libc::SOL_SOCKET, libc::SO_LINGER, &value)
    }

    /// `SO_LINGER`.
    ///
    /// # Errors
    ///
    /// What `getsockopt(2)` reports.
    pub fn linger(&self) -> io::Result<Option<Duration>> {
        let linger: libc::linger = getsockopt(self.as_raw_fd(), libc::SOL_SOCKET, libc::SO_LINGER)?;
        Ok((linger.l_onoff != 0)
            .then(|| Duration::from_secs(linger.l_linger.cast_unsigned().into())))
    }

    stream_methods!();
}

stream_impls!(TcpStream);

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
            .field("fd", &self.as_raw_fd())
            .field("peer", &self.peer)
            .finish()
    }
}

/// A TCP socket listening for connections, which is `Send` and `Sync`, and
/// accepts [`TcpStream`]s: with an `accept4(2)` made at once, which waits on
/// the ring of the thread polling it only while there is no connection to
/// take.
pub struct TcpListener {
    listener: Listener,
}

impl TcpListener {
    /// A listener bound to `addr`, with a backlog of 1024 connections, as the
    /// thread's [`TcpListener`](crate::net::TcpListener) has.
    ///
    /// A host name is resolved on the thread, as the standard library
    /// resolves it: a program binds its listeners as it starts.
    ///
    /// # Errors
    ///
    /// What binding and listening report, or `InvalidInput` if `addr`
    /// resolves to no address.
    pub fn bind(addr: impl net::ToSocketAddrs) -> io::Result<Self> {
        let listener = net::TcpListener::bind(addr)?;
        listen(listener.as_raw_fd())?;
        Self::from_std(listener)
    }

    /// A listener bound to `addr` as [`bind`](TcpListener::bind) binds one,
    /// with `SO_REUSEPORT` too, so that listeners on several cores can bind
    /// one address and the kernel spreads the connections among them.
    ///
    /// # Errors
    ///
    /// As for [`bind`](TcpListener::bind): `AddrInUse` if a socket without
    /// `SO_REUSEPORT`, or another user's, has the address.
    pub fn bind_reuseport(addr: impl net::ToSocketAddrs) -> io::Result<Self> {
        let listener = net::TcpListener::from(bind_reuseport(addr, libc::SOCK_STREAM)?);
        listen(listener.as_raw_fd())?;
        Self::from_std(listener)
    }

    /// Takes over a listening socket from the standard library, made
    /// nonblocking.
    ///
    /// # Errors
    ///
    /// What making it nonblocking reports.
    pub fn from_std(listener: net::TcpListener) -> io::Result<Self> {
        Ok(Self {
            listener: Listener::new(listener.into())?,
        })
    }

    /// The standard library's listener, blocking again.
    pub fn into_std(self) -> net::TcpListener {
        net::TcpListener::from(self.listener.io.into_owned_fd())
    }

    /// The address the listener is bound to, with one `getsockname(2)`.
    ///
    /// # Errors
    ///
    /// What `getsockname(2)` reports.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        // SAFETY: the descriptor is the listener's own socket, open for as
        // long as the listener is, which the view does not outlive; the view
        // is never dropped, so it closes nothing.
        let view = ManuallyDrop::new(unsafe { net::TcpListener::from_raw_fd(self.as_raw_fd()) });
        view.local_addr()
    }

    /// Takes the next connection, and the peer's address, waiting for one if
    /// none is waiting.
    ///
    /// # Errors
    ///
    /// What `accept4(2)` reports, bar a connection given up before it was
    /// taken, which the next replaces; and, for a wait where no runtime runs
    /// and neither the foreign reactor nor a foreign ring takes it, `ENOSYS`,
    /// whose kind is [`io::ErrorKind::Unsupported`].
    pub async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        let mut wait = ruio_driver::ReadyWait::new();
        let (fd, addr) =
            std::future::poll_fn(|cx| poll_accept(&self.listener.io, cx, &mut wait)).await?;
        accepted(fd, &addr)
    }

    /// Takes the next connection, as [`accept`](TcpListener::accept) does,
    /// polled: a wait the listener keeps until it is polled again, so that the
    /// next poll goes on with it.
    ///
    /// # Errors
    ///
    /// As for [`accept`](TcpListener::accept).
    pub fn poll_accept(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<(TcpStream, SocketAddr)>> {
        let listener = &mut self.listener;
        let (fd, addr) = ready!(poll_accept(&listener.io, cx, &mut listener.wait))?;
        Poll::Ready(accepted(fd, &addr))
    }
}

/// The stream of a connection accepted, with its peer's address.
fn accepted(fd: std::os::fd::OwnedFd, addr: &SockAddr) -> io::Result<(TcpStream, SocketAddr)> {
    let stream = Stream::from_nonblocking(fd);
    let peer = addr.as_inet().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "an accepted TCP connection's address is not an internet address",
        )
    })?;
    Ok((
        TcpStream {
            stream,
            peer: Some(peer),
        },
        peer,
    ))
}

impl AsRawFd for TcpListener {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        self.listener.io.fd().as_raw_fd()
    }
}

impl AsFd for TcpListener {
    #[inline]
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.listener.io.fd().as_fd()
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
            .field("fd", &self.as_raw_fd())
            .finish()
    }
}
