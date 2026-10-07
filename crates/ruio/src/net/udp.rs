//! UDP sockets.

use std::fmt;
use std::future::poll_fn;
use std::io;
use std::marker::PhantomData;
use std::net::{self, SocketAddr};
use std::ops::{Deref, DerefMut};
use std::task::{Context, Poll};

use ruio_driver::buf::{BufLease, BufPool, BufResult, IoBuf, IoBufMut};
use ruio_driver::ops::{self, Recv, RecvFrom, Send, SendTo};
use ruio_driver::{Fd, SockAddr};

use super::socket::{Desc, RecvStream, SendZc, Sock, SocketOp, ThreadBound, bind_reuseport};

/// A UDP socket.
///
/// Each send is a datagram, and each receive takes one. A datagram larger
/// than the buffer it is received into is cut short, and the rest of it
/// lost.
pub struct UdpSocket {
    sock: Sock<net::UdpSocket>,
    _thread: ThreadBound,
}

impl UdpSocket {
    /// A socket bound to `addr`, or to the first address it resolves to
    /// that works.
    ///
    /// A host name is resolved on the thread, as the standard library
    /// resolves it: a program binds its sockets as it starts, where blocking
    /// holds nothing up. A bind that must not block resolves the name with
    /// [`lookup_host`](super::lookup_host), on the blocking pool, and binds
    /// the addresses.
    ///
    /// # Errors
    ///
    /// As for [`std::net::UdpSocket::bind`].
    pub fn bind(addr: impl net::ToSocketAddrs) -> io::Result<Self> {
        net::UdpSocket::bind(addr).map(Self::from_std)
    }

    /// A socket bound to `addr` as [`bind`](UdpSocket::bind) binds one, a
    /// host name resolved on the thread included, but with `SO_REUSEPORT`
    /// too: sockets that all have it can bind one address, and the kernel
    /// spreads the datagrams among them by a hash of each one's addresses and
    /// ports, so that a sender's datagrams all reach one socket.
    ///
    /// # Errors
    ///
    /// As for [`bind`](UdpSocket::bind): `AddrInUse` if a socket without
    /// `SO_REUSEPORT`, or another user's, has the address.
    pub fn bind_reuseport(addr: impl net::ToSocketAddrs) -> io::Result<Self> {
        let socket = bind_reuseport(addr, libc::SOCK_DGRAM)?;
        Ok(Self::from_std(net::UdpSocket::from(socket)))
    }

    /// Wraps a socket from the standard library.
    pub fn from_std(socket: net::UdpSocket) -> Self {
        Self {
            sock: Sock::from_std(socket),
            _thread: PhantomData,
        }
    }

    fn socket(&self) -> &net::UdpSocket {
        match self.sock.desc() {
            Desc::Plain(socket) => socket,
            Desc::Direct(_) => unreachable!("UDP sockets have plain descriptors"),
        }
    }

    /// The address the socket is bound to.
    ///
    /// # Errors
    ///
    /// What `getsockname(2)` reports.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket().local_addr()
    }

    /// The address the socket is connected to.
    ///
    /// # Errors
    ///
    /// What `getpeername(2)` reports: `NotConnected` if it is not.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.socket().peer_addr()
    }

    /// Connects the socket to `addr`, which [`send`](UdpSocket::send) then
    /// sends to, and [`recv`](UdpSocket::recv) only receives from. Nothing
    /// goes over the network, so this does not wait, except to resolve a
    /// host name, which it does on the thread as the standard library does;
    /// [`lookup_host`](super::lookup_host) resolves one on the blocking pool
    /// first for a connect that must not block.
    ///
    /// # Errors
    ///
    /// As for [`std::net::UdpSocket::connect`].
    pub fn connect(&self, addr: impl net::ToSocketAddrs) -> io::Result<()> {
        self.socket().connect(addr)
    }

    /// Sets `SO_BROADCAST`, which allows sending to broadcast addresses.
    ///
    /// # Errors
    ///
    /// What `setsockopt(2)` reports.
    pub fn set_broadcast(&self, broadcast: bool) -> io::Result<()> {
        self.socket().set_broadcast(broadcast)
    }

    /// Whether `SO_BROADCAST` is set.
    ///
    /// # Errors
    ///
    /// What `getsockopt(2)` reports.
    pub fn broadcast(&self) -> io::Result<bool> {
        self.socket().broadcast()
    }

    /// The descriptor, for operations of one's own.
    pub fn fd(&self) -> Fd {
        self.sock.fd()
    }

    /// Sends `buf`'s initialized bytes as a datagram to the address the
    /// socket is connected to.
    pub fn send<B: IoBuf>(&self, buf: B) -> SocketOp<'_, Send<B>> {
        self.sock.write(buf)
    }

    /// Receives a datagram from the address the socket is connected to.
    pub fn recv<B: IoBufMut>(&self, buf: B) -> SocketOp<'_, Recv<B>> {
        self.sock.read(buf)
    }

    /// Sends `buf`'s initialized bytes as a datagram to `addr`.
    pub fn send_to<B: IoBuf>(&self, buf: B, addr: SocketAddr) -> SocketOp<'_, SendTo<B>> {
        self.sock.op(|fd| SendTo::new(fd, buf, addr))
    }

    /// Receives a datagram, and reports the address it came from.
    pub fn recv_from<B: IoBufMut>(
        &self,
        buf: B,
    ) -> SocketOp<'_, RecvFrom<B>, BufResult<(usize, SocketAddr), B>> {
        self.sock.op_map(
            |fd| RecvFrom::new(fd, buf),
            |(received, buf)| {
                let received = received.and_then(|(len, from)| Ok((len, inet(from)?)));
                (received, buf)
            },
        )
    }

    /// Sends `buf`'s initialized bytes as a datagram to the address the
    /// socket is connected to, without copying them; see [`ops::SendZc`].
    pub fn send_zc<B: IoBuf>(&self, buf: B) -> SendZc<'_, B> {
        SendZc::new(ops::SendZc::new(&crate::driver(), self.fd(), buf))
    }

    /// Sends `buf`'s initialized bytes as a datagram to `addr`, without
    /// copying them; see [`ops::SendZc`].
    pub fn send_zc_to<B: IoBuf>(&self, buf: B, addr: SocketAddr) -> SendZc<'_, B> {
        SendZc::new(ops::SendZc::new(&crate::driver(), self.fd(), buf).to(addr))
    }

    /// The datagrams arriving on the socket, each received into a buffer of
    /// `pool`, which must belong to the thread's ring, without the address
    /// it came from: for a connected socket, whose datagrams all come from
    /// its peer. [`recv_from_stream`](UdpSocket::recv_from_stream) gives each
    /// datagram's address, for a socket that answers whoever sends to it.
    pub fn recv_stream(&self, pool: &BufPool) -> RecvStream<'_> {
        self.sock.recv_stream(pool)
    }

    /// The datagrams arriving on the socket, each with the address it came
    /// from, received into buffers of `pool`, which must belong to the
    /// thread's ring, by one multishot `recvmsg` that the stream keeps armed:
    /// what a socket that is not connected answers its senders from.
    ///
    /// Each datagram takes one buffer, in which a header and the address come
    /// before the payload: 32 bytes of it for an IPv4 socket, and 48 for an
    /// IPv6 one. A datagram longer than the rest of the buffer is cut short,
    /// and the rest of it lost, as by [`recv_from`](UdpSocket::recv_from)
    /// into a buffer of that size, which [`Datagram::is_truncated`] tells.
    ///
    /// ```no_run
    /// use ruio::buf::BufPool;
    /// use ruio::net::UdpSocket;
    ///
    /// # async fn echo() -> std::io::Result<()> {
    /// let socket = UdpSocket::bind("127.0.0.1:7878")?;
    /// let pool = BufPool::new(&ruio::driver(), 256, 2048)?;
    /// let mut datagrams = socket.recv_from_stream(&pool)?;
    /// while let Some(datagram) = datagrams.next().await {
    ///     let (data, from) = datagram?.into_parts();
    ///     socket.send_to(data, from).await.0?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// `InvalidInput` if the pool's buffers have no room for a payload after
    /// the header and the address, and what `getsockname(2)` reports.
    pub fn recv_from_stream(&self, pool: &BufPool) -> io::Result<RecvFromStream<'_>> {
        let family = match self.local_addr()? {
            SocketAddr::V4(_) => libc::AF_INET,
            SocketAddr::V6(_) => libc::AF_INET6,
        };
        let (driver, fd) = self.sock.target();
        Ok(RecvFromStream {
            stream: ops::RecvFromStream::new(&driver, fd, pool, family)?,
            _socket: PhantomData,
        })
    }
}

/// The internet address a datagram came from.
fn inet(addr: SockAddr) -> io::Result<SocketAddr> {
    addr.as_inet()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "not an internet address"))
}

impl fmt::Debug for UdpSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UdpSocket")
            .field("socket", self.socket())
            .finish_non_exhaustive()
    }
}

impl From<net::UdpSocket> for UdpSocket {
    fn from(socket: net::UdpSocket) -> Self {
        Self::from_std(socket)
    }
}

/// The datagrams arriving on a UDP socket, each with the address it came
/// from, received into a [`BufPool`] by a multishot `recvmsg` that the
/// stream keeps armed. Made by [`UdpSocket::recv_from_stream`]; see
/// [`ops::RecvFromStream`].
///
/// When the pool runs dry, the stream waits for a buffer to return, and the
/// datagrams that arrive meanwhile wait in the socket, as many as its
/// receive buffer holds. The stream ends after yielding an error, such as
/// `ECONNREFUSED` on a socket connected to a port no socket is bound to.
#[derive(Debug)]
pub struct RecvFromStream<'a> {
    stream: ops::RecvFromStream,
    _socket: PhantomData<&'a ()>,
}

impl RecvFromStream<'_> {
    /// The next datagram, or `None` once an error has been yielded.
    #[inline]
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<Datagram>>> {
        self.stream
            .poll_next(cx)
            .map(|datagram| datagram.map(|datagram| datagram.and_then(Datagram::new)))
    }

    /// The next datagram; see [`poll_next`](RecvFromStream::poll_next).
    pub async fn next(&mut self) -> Option<io::Result<Datagram>> {
        poll_fn(|cx| self.poll_next(cx)).await
    }

    /// The next datagram if it has arrived already, or `None`, without
    /// waiting; see [`ops::RecvFromStream::next_ready`]. A task that hands
    /// the datagrams on in batches, to another task through a channel say,
    /// takes those that have arrived so after each one it waits for:
    ///
    /// ```no_run
    /// use ruio::net::Datagram;
    /// use ruio::sync::local::mpsc;
    ///
    /// # async fn feed(socket: ruio::net::UdpSocket, pool: ruio::buf::BufPool) -> std::io::Result<()> {
    /// let (batches, _worker) = mpsc::channel::<Vec<Datagram>>(16);
    /// let mut datagrams = socket.recv_from_stream(&pool)?;
    /// while let Some(datagram) = datagrams.next().await {
    ///     let mut batch = vec![datagram?];
    ///     while let Some(datagram) = datagrams.next_ready() {
    ///         batch.push(datagram?);
    ///     }
    ///     if batches.send(batch).await.is_err() {
    ///         break;
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn next_ready(&mut self) -> Option<io::Result<Datagram>> {
        self.stream
            .next_ready()
            .map(|datagram| datagram.and_then(Datagram::new))
    }

    /// Whether the stream has ended, after yielding an error.
    #[inline]
    pub fn is_ended(&self) -> bool {
        self.stream.is_ended()
    }
}

/// A datagram that a UDP socket received into a buffer of a pool, with the
/// address it came from. It derefs to the payload. Made by
/// [`RecvFromStream`].
///
/// The buffer returns to the pool when the datagram is dropped, or when the
/// lease of its payload that [`into_data`](Datagram::into_data) makes is.
pub struct Datagram {
    datagram: ops::Datagram,
    from: SocketAddr,
}

impl Datagram {
    #[inline]
    fn new(datagram: ops::Datagram) -> io::Result<Self> {
        let from = datagram
            .inet_from()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "not an internet address"))?;
        Ok(Self { datagram, from })
    }

    /// The address the datagram came from.
    #[inline]
    pub fn from(&self) -> SocketAddr {
        self.from
    }

    /// Whether the datagram was longer than its buffer had room for, so that
    /// the kernel cut it short, and the rest of it was lost.
    #[inline]
    pub fn is_truncated(&self) -> bool {
        self.datagram.is_truncated()
    }

    /// The payload, as a lease of its buffer whose data and room leave out
    /// the header and the address before it, to keep or to send as any
    /// buffer.
    #[inline]
    pub fn into_data(self) -> BufLease {
        self.datagram.into_data()
    }

    /// The payload, as [`into_data`](Datagram::into_data) leases it, and the
    /// address it came from: what an answer to the datagram takes.
    #[inline]
    pub fn into_parts(self) -> (BufLease, SocketAddr) {
        (self.datagram.into_data(), self.from)
    }
}

impl Deref for Datagram {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        &self.datagram
    }
}

impl DerefMut for Datagram {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.datagram
    }
}

impl fmt::Debug for Datagram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Datagram")
            .field("from", &self.from)
            .field("len", &self.len())
            .field("truncated", &self.is_truncated())
            .finish()
    }
}
