//! UDP sockets.

use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::net::{self, SocketAddr, ToSocketAddrs};

use ruio_driver::buf::{BufPool, BufResult, IoBuf, IoBufMut};
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
    /// # Errors
    ///
    /// As for [`std::net::UdpSocket::bind`].
    pub fn bind(addr: impl ToSocketAddrs) -> io::Result<Self> {
        net::UdpSocket::bind(addr).map(Self::from_std)
    }

    /// A socket bound to `addr` as [`bind`](UdpSocket::bind) binds one, but
    /// with `SO_REUSEPORT` too: sockets that all have it can bind one address,
    /// and the kernel spreads the datagrams among them by a hash of each one's
    /// addresses and ports, so that a sender's datagrams all reach one socket.
    ///
    /// # Errors
    ///
    /// As for [`bind`](UdpSocket::bind): `AddrInUse` if a socket without
    /// `SO_REUSEPORT`, or another user's, has the address.
    pub fn bind_reuseport(addr: impl ToSocketAddrs) -> io::Result<Self> {
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
    /// goes over the network, so this does not wait.
    ///
    /// # Errors
    ///
    /// As for [`std::net::UdpSocket::connect`].
    pub fn connect(&self, addr: impl ToSocketAddrs) -> io::Result<()> {
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
    /// `pool`, which must belong to the thread's ring.
    pub fn recv_stream(&self, pool: &BufPool) -> RecvStream<'_> {
        self.sock.recv_stream(pool)
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
