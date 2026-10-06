//! TCP: listeners, and the streams they accept or that connect to them.

use std::cell::Cell;
use std::fmt;
use std::future::poll_fn;
use std::io;
use std::marker::PhantomData;
use std::net::{self, Shutdown, SocketAddr};
use std::os::fd::{AsRawFd, RawFd};
use std::rc::Rc;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use ruio_driver::buf::{BufPool, IoBuf, IoBufMut, IoBufs};
use ruio_driver::ops::{self, Accept, AcceptMulti, AcceptStream, Connect, Recv, Send, Socket};
use ruio_driver::{Descriptor, Driver, Fd};

use super::addr::{ToSocketAddrs, resolve};
use super::behind::{Behind, Flush, WriteBehind};
use super::socket::{
    Plain, ReadExact, RecvStream, SendZc, Sent, Sock, SocketOp, ThreadBound, WriteAll,
    WriteAllVectored, bind_reuseport,
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
    socket: Plain<net::TcpListener>,
    direct: Cell<bool>,
    _thread: ThreadBound,
}

impl TcpListener {
    /// A listener bound to `addr`, or to the first address it resolves to
    /// that works, with `SO_REUSEADDR` and a backlog of 1024 connections.
    ///
    /// A host name is resolved on the thread, as the standard library
    /// resolves it: a program binds its listeners as it starts, where
    /// blocking holds nothing up. A bind that must not block resolves the
    /// name with [`lookup_host`](super::lookup_host), on the blocking pool,
    /// and binds the addresses.
    ///
    /// # Errors
    ///
    /// What binding and listening report, or `InvalidInput` if `addr`
    /// resolves to no address.
    pub fn bind(addr: impl net::ToSocketAddrs) -> io::Result<Self> {
        let socket = net::TcpListener::bind(addr)?;
        listen(socket.as_raw_fd())?;
        Ok(Self::from_std(socket))
    }

    /// A listener bound to `addr` as [`bind`](TcpListener::bind) binds one,
    /// a host name resolved on the thread included, but with `SO_REUSEPORT`
    /// too: listeners that all have it can bind one address, and the kernel
    /// spreads the connections among them by a hash of each one's addresses
    /// and ports. A listener on each core, all bound to one address, so
    /// share the connections out, and no core has to hand any on.
    ///
    /// # Errors
    ///
    /// As for [`bind`](TcpListener::bind): `AddrInUse` if a socket without
    /// `SO_REUSEPORT`, or another user's, has the address.
    pub fn bind_reuseport(addr: impl net::ToSocketAddrs) -> io::Result<Self> {
        let socket = net::TcpListener::from(bind_reuseport(addr, libc::SOCK_STREAM)?);
        listen(socket.as_raw_fd())?;
        Ok(Self::from_std(socket))
    }

    /// Wraps a listening socket from the standard library.
    pub fn from_std(socket: net::TcpListener) -> Self {
        Self {
            socket: Plain::new(socket),
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
    /// operation, and costs no file descriptor of the process. Its options
    /// are set and read through the ring as a plain stream's are; its
    /// addresses, which only a file descriptor can ask for, cost it one
    /// installed for the call, unless [`accept`](TcpListener::accept) told
    /// it its peer's.
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
    /// The peer's address, where the kernel told it as the stream connected
    /// or was accepted, which spares [`peer_addr`](TcpStream::peer_addr) the
    /// asking.
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
            sock: Sock::from_std(stream),
            peer: None,
            behind: Rc::default(),
            _thread: PhantomData,
        }
    }

    /// The local address of the connection.
    ///
    /// A plain descriptor answers at once, with `getsockname(2)`. A direct
    /// descriptor has no file descriptor to ask with, so one is installed for
    /// the call through the ring and closed after it; [`into_plain`] gives a
    /// stream that is asked often a plain descriptor for good.
    ///
    /// # Errors
    ///
    /// What `getsockname(2)` reports.
    ///
    /// [`into_plain`]: TcpStream::into_plain
    pub async fn local_addr(&self) -> io::Result<SocketAddr> {
        self.sock.with_plain(|stream| stream.local_addr()).await
    }

    /// The peer's address: the one the stream connected to, or was accepted
    /// from by [`TcpListener::accept`], at once; otherwise what
    /// `getpeername(2)` reports, asked as [`local_addr`](TcpStream::local_addr)
    /// asks.
    ///
    /// # Errors
    ///
    /// What `getpeername(2)` reports.
    pub async fn peer_addr(&self) -> io::Result<SocketAddr> {
        match self.peer {
            Some(peer) => Ok(peer),
            None => self.sock.with_plain(|stream| stream.peer_addr()).await,
        }
    }

    /// Sets `TCP_NODELAY`, which sends small writes at once rather than
    /// holding them back to fill a segment.
    ///
    /// A plain descriptor's option is set with `setsockopt(2)`, at once, as
    /// the standard library sets it; a direct descriptor has no file
    /// descriptor for the call, so its option goes through the ring, which
    /// completes it as it is submitted.
    ///
    /// # Errors
    ///
    /// What `setsockopt(2)` reports.
    pub async fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        self.sock
            .set_option(libc::IPPROTO_TCP, libc::TCP_NODELAY, i32::from(nodelay))
            .await
    }

    /// Whether `TCP_NODELAY` is set.
    ///
    /// A plain descriptor answers at once, with `getsockopt(2)`. The kernel
    /// reads an option of `IPPROTO_TCP` through the ring for no descriptor,
    /// so a direct one answers through a descriptor installed for the call,
    /// as for [`local_addr`](TcpStream::local_addr).
    ///
    /// # Errors
    ///
    /// What `getsockopt(2)` reports.
    pub async fn nodelay(&self) -> io::Result<bool> {
        let nodelay: i32 = self
            .sock
            .option(libc::IPPROTO_TCP, libc::TCP_NODELAY)
            .await?;
        Ok(nodelay != 0)
    }

    /// Sets `IP_TTL`, the time-to-live of the packets sent, as
    /// [`set_nodelay`](TcpStream::set_nodelay) sets its option.
    ///
    /// # Errors
    ///
    /// What `setsockopt(2)` reports: `EINVAL` for a value above 255.
    pub async fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        let ttl = i32::try_from(ttl).unwrap_or(i32::MAX);
        self.sock
            .set_option(libc::IPPROTO_IP, libc::IP_TTL, ttl)
            .await
    }

    /// `IP_TTL`, read as [`nodelay`](TcpStream::nodelay) reads its option.
    ///
    /// # Errors
    ///
    /// What `getsockopt(2)` reports.
    pub async fn ttl(&self) -> io::Result<u32> {
        let ttl: i32 = self.sock.option(libc::IPPROTO_IP, libc::IP_TTL).await?;
        Ok(ttl.cast_unsigned())
    }

    /// Sets `SO_LINGER`: for how long closing the stream waits for data not
    /// yet sent to go, or `None` for not at all, which is the default; set as
    /// [`set_nodelay`](TcpStream::set_nodelay) sets its option.
    ///
    /// # Errors
    ///
    /// What `setsockopt(2)` reports.
    pub async fn set_linger(&self, linger: Option<Duration>) -> io::Result<()> {
        let value = libc::linger {
            l_onoff: i32::from(linger.is_some()),
            l_linger: linger.map_or(0, |linger| {
                i32::try_from(linger.as_secs()).unwrap_or(i32::MAX)
            }),
        };
        self.sock
            .set_option(libc::SOL_SOCKET, libc::SO_LINGER, value)
            .await
    }

    /// `SO_LINGER`: a plain descriptor answers at once, with `getsockopt(2)`,
    /// and a direct one through the ring, which reads an option of
    /// `SOL_SOCKET` for it.
    ///
    /// # Errors
    ///
    /// What `getsockopt(2)` reports.
    pub async fn linger(&self) -> io::Result<Option<Duration>> {
        let linger: libc::linger = self.sock.option(libc::SOL_SOCKET, libc::SO_LINGER).await?;
        Ok((linger.l_onoff != 0)
            .then(|| Duration::from_secs(linger.l_linger.cast_unsigned().into())))
    }

    /// Takes the error pending on the socket, `SO_ERROR`, if there is one,
    /// which clears it; read as [`linger`](TcpStream::linger) is.
    ///
    /// # Errors
    ///
    /// What `getsockopt(2)` reports.
    pub async fn take_error(&self) -> io::Result<Option<io::Error>> {
        let errno: i32 = self.sock.option(libc::SOL_SOCKET, libc::SO_ERROR).await?;
        Ok((errno != 0).then(|| io::Error::from_raw_os_error(errno)))
    }

    /// The stream as a plain descriptor: a plain stream as it is, and a
    /// direct one with its file installed as a descriptor of the process
    /// through the ring and its slot freed, for what only a file descriptor
    /// can do, such as handing the connection to a library, or asking its
    /// addresses often. The writes behind the stream finish first: a chain
    /// of them is issued one send after another on the slot, and a send not
    /// yet issued would find the slot gone. One that failed fails the next
    /// write, as it would have.
    ///
    /// # Errors
    ///
    /// What installing the descriptor reports: `EMFILE` if the process has
    /// no descriptor left.
    pub async fn into_plain(self) -> io::Result<Self> {
        // The error, if one failed, waits for the next write to report it.
        let _ = poll_fn(|cx| self.behind.poll_done(cx)).await;
        Ok(Self {
            sock: self.sock.into_plain().await?,
            peer: self.peer,
            behind: self.behind,
            _thread: PhantomData,
        })
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

    /// Reports whether a [`write_behind`](Self::write_behind) failed, if
    /// none is in flight, without waiting for one that is: `None` then, as
    /// how it goes is not yet known. Unlike [`flush`](Self::flush), this
    /// arranges for nothing to be woken, so a task that only wants to know
    /// what is known so far costs the stream nothing.
    pub fn try_flush(&self) -> Option<io::Result<()>> {
        self.behind.status()
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

    /// The data arriving on the connection, received into a pool of the
    /// stream's own, of `count` buffers of `size` bytes, which the kernel
    /// consumes incrementally (Linux 6.12): each receive takes only what its
    /// data fills of a buffer, and the next carries on in it, so that large
    /// buffers hold many small receives; see
    /// [`RecvStream::incremental`](crate::uring::ops::RecvStream::incremental).
    ///
    /// # Errors
    ///
    /// `Unsupported` before Linux 6.12, and as for
    /// [`BufPool::new`](crate::buf::BufPool::new).
    pub fn recv_stream_incremental(&self, count: u16, size: u32) -> io::Result<RecvStream<'_>> {
        self.sock.recv_stream_incremental(count, size)
    }

    /// Shuts down the reading or writing half of the connection, or both,
    /// once a [`write_behind`](Self::write_behind) in flight has finished.
    pub fn shutdown(&self, how: Shutdown) -> SocketOp<'_, ops::Shutdown> {
        self.sock.shutdown(how).behind(&self.behind)
    }

    /// Sends the stream to core `core` of the thread's run, for a task there
    /// to [`receive`](SentTcpStream::receive), once a
    /// [`write_behind`](Self::write_behind) in flight has finished. A plain
    /// descriptor goes as it is, and a direct one moves to that core's ring,
    /// with `IORING_OP_MSG_RING`; see [`DirectFd::send_to`].
    ///
    /// # Errors
    ///
    /// Hands the stream back with what the write behind failed with, or what
    /// moving a direct descriptor reports: `ENFILE` if the other ring's file
    /// table is full, say.
    ///
    /// # Panics
    ///
    /// Outside [`Cores::run`](crate::Cores::run), or if the run has no core
    /// `core`.
    ///
    /// [`DirectFd::send_to`]: crate::uring::DirectFd::send_to
    pub async fn send_to(self, core: usize) -> Result<SentTcpStream, (io::Error, TcpStream)> {
        if let Err(err) = self.flush().await {
            return Err((err, self));
        }
        let TcpStream {
            sock,
            peer,
            behind,
            _thread,
        } = self;
        match sock.send_to(core).await {
            Ok(sock) => Ok(SentTcpStream { sock, peer }),
            Err((err, sock)) => Err((
                err,
                TcpStream {
                    sock,
                    peer,
                    behind,
                    _thread,
                },
            )),
        }
    }
}

/// A TCP stream on its way to another core: `Send`, unlike a [`TcpStream`].
/// Made by [`TcpStream::send_to`]; on the core it was sent to, a task makes
/// it a stream again with [`receive`](SentTcpStream::receive). Dropping it
/// closes the connection.
#[derive(Debug)]
pub struct SentTcpStream {
    sock: Sent<net::TcpStream>,
    peer: Option<SocketAddr>,
}

impl SentTcpStream {
    /// The stream, on the core it was sent to.
    ///
    /// # Panics
    ///
    /// On any other core, if the stream is a direct descriptor, which only
    /// that core's ring holds.
    pub fn receive(self) -> TcpStream {
        TcpStream {
            sock: self.sock.receive(),
            peer: self.peer,
            behind: Rc::default(),
            _thread: PhantomData,
        }
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
