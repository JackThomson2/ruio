//! UDP sockets.

use std::fmt;
use std::future::poll_fn;
use std::io;
use std::net::{self, SocketAddr};
use std::task::{Context, Poll};

use ruio_iocp::net as iocp;

use super::{Datagram, Read, RecvFrom, RecvStream, SendTo, Write};
use crate::buf::{BufPool, IoBuf, IoBufMut};

/// A UDP socket.
///
/// Each send is a datagram, and each receive takes one. A datagram larger
/// than the buffer it is received into is cut short, and the rest of it
/// lost, as on Linux, where Windows by itself would fail the receive. And a
/// datagram sent to a port that nobody is bound to fails none of the
/// socket's receives after it, where Windows by itself would fail the next.
pub struct UdpSocket {
    socket: iocp::UdpSocket,
}

impl UdpSocket {
    /// A socket bound to `addr`, or to the first address it resolves to
    /// that works. One bound to an IPv6 address sends to and receives from
    /// IPv4 addresses too, in their IPv6 form, as on Linux.
    ///
    /// A host name is resolved on the thread, as the standard library
    /// resolves it: a program binds its sockets as it starts, where blocking
    /// holds nothing up. A bind that must not block resolves the name with
    /// [`lookup_host`](super::lookup_host), on the blocking pool, and binds
    /// the addresses.
    ///
    /// # Errors
    ///
    /// What binding reports, `InvalidInput` if `addr` resolves to no
    /// address, and an error of kind `Other` where no runtime runs.
    pub fn bind(addr: impl net::ToSocketAddrs) -> io::Result<Self> {
        Ok(Self {
            socket: iocp::UdpSocket::bind(addr)?,
        })
    }

    /// Wraps a socket from the standard library, for the port of the
    /// thread's runtime.
    ///
    /// # Panics
    ///
    /// If the thread has no runtime of ruio's, which on io_uring the first
    /// operation on the socket needs, or the system refuses the socket: to
    /// associate it with the runtime's port, as when another runtime's has
    /// it, or to keep its receives from failing after a datagram that found
    /// nobody at its port.
    pub fn from_std(socket: net::UdpSocket) -> Self {
        Self {
            socket: iocp::UdpSocket::from_std(socket),
        }
    }

    /// The address the socket is bound to.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// The address the socket is connected to.
    ///
    /// # Errors
    ///
    /// What the system reports: `NotConnected` if it is not.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.socket.peer_addr()
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
    /// What resolving and connecting report.
    pub fn connect(&self, addr: impl net::ToSocketAddrs) -> io::Result<()> {
        self.socket.connect(addr)
    }

    /// Sets `SO_BROADCAST`, which allows sending to broadcast addresses.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub fn set_broadcast(&self, broadcast: bool) -> io::Result<()> {
        self.socket.set_broadcast(broadcast)
    }

    /// Whether `SO_BROADCAST` is set.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub fn broadcast(&self) -> io::Result<bool> {
        self.socket.broadcast()
    }

    /// Sends `buf`'s initialized bytes as a datagram to the address the
    /// socket is connected to; see [`Write`].
    pub fn send<B: IoBuf>(&self, buf: B) -> Write<'_, B> {
        self.socket.send(buf)
    }

    /// Receives a datagram from the address the socket is connected to into
    /// `buf`, up to its `bytes_total`; see [`Read`].
    pub fn recv<B: IoBufMut>(&self, buf: B) -> Read<'_, B> {
        self.socket.recv(buf)
    }

    /// Sends `buf`'s initialized bytes as a datagram to `addr`; see
    /// [`SendTo`].
    pub fn send_to<B: IoBuf>(&self, buf: B, addr: SocketAddr) -> SendTo<'_, B> {
        self.socket.send_to(buf, addr)
    }

    /// Receives a datagram into `buf`, up to its `bytes_total`, and reports
    /// the address it came from; see [`RecvFrom`].
    pub fn recv_from<B: IoBufMut>(&self, buf: B) -> RecvFrom<'_, B> {
        self.socket.recv_from(buf)
    }

    /// The datagrams arriving on the socket, each received into a buffer of
    /// `pool` as it arrives, without the address it came from: for a
    /// connected socket, whose datagrams all come from its peer; see
    /// [`RecvStream`]. [`recv_from_stream`](UdpSocket::recv_from_stream)
    /// gives each datagram's address, for a socket that answers whoever
    /// sends to it.
    pub fn recv_stream(&self, pool: &BufPool) -> RecvStream<'_> {
        RecvStream {
            stream: self.socket.recv_stream(pool),
        }
    }

    /// The datagrams arriving on the socket, each with the address it came
    /// from, received into buffers of `pool` as they arrive: what a socket
    /// that is not connected answers its senders from; see
    /// [`RecvFromStream`].
    ///
    /// Each datagram takes one buffer, of which it leaves out as much as a
    /// header and the address take before the payload on io_uring: 32 bytes
    /// for an IPv4 socket, and 48 for an IPv6 one, so that a pool holds as
    /// much of a datagram here as there. A datagram longer than the rest of
    /// the buffer is cut short, and the rest of it lost, as by
    /// [`recv_from`](UdpSocket::recv_from) into a buffer of that size, which
    /// [`Datagram::is_truncated`] tells.
    ///
    /// ```no_run
    /// use ruio::buf::BufPool;
    /// use ruio::net::UdpSocket;
    ///
    /// # async fn echo() -> std::io::Result<()> {
    /// let socket = UdpSocket::bind("127.0.0.1:7878")?;
    /// let pool = BufPool::new(256, 2048)?;
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
    /// what each datagram leaves out, and what the system reports of the
    /// socket's address.
    pub fn recv_from_stream(&self, pool: &BufPool) -> io::Result<RecvFromStream<'_>> {
        Ok(RecvFromStream {
            stream: self.socket.recv_from_stream(pool)?,
        })
    }
}

impl fmt::Debug for UdpSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.socket, f)
    }
}

impl From<net::UdpSocket> for UdpSocket {
    fn from(socket: net::UdpSocket) -> Self {
        Self::from_std(socket)
    }
}

/// The datagrams arriving on a UDP socket, each with the address it came
/// from, received into a [`BufPool`]. Made by
/// [`UdpSocket::recv_from_stream`].
///
/// A stream that waits for a datagram holds no buffer: it takes one of the
/// pool once the socket has a datagram, and then receives each of those
/// that have arrived behind it. When the pool runs dry, the stream waits
/// for a buffer to return, and the datagrams that arrive meanwhile wait in
/// the socket, as many as its receive buffer holds. The stream ends after
/// yielding an error.
///
/// Dropping the stream leaves the datagrams that arrive after in the
/// socket, for what receives them next.
pub struct RecvFromStream<'a> {
    stream: iocp::RecvFromStream<'a>,
}

impl RecvFromStream<'_> {
    /// The next datagram, or `None` once an error has been yielded.
    #[inline]
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<Datagram>>> {
        self.stream.poll_next(cx)
    }

    /// The next datagram; see [`poll_next`](RecvFromStream::poll_next).
    pub async fn next(&mut self) -> Option<io::Result<Datagram>> {
        poll_fn(|cx| self.poll_next(cx)).await
    }

    /// The next datagram if it has arrived already, or `None`, without
    /// waiting. A task that answers in batches takes those that have
    /// arrived after each one it waits for:
    ///
    /// ```no_run
    /// # async fn answer(socket: ruio::net::UdpSocket, pool: ruio::buf::BufPool) -> std::io::Result<()> {
    /// let mut datagrams = socket.recv_from_stream(&pool)?;
    /// while let Some(datagram) = datagrams.next().await {
    ///     let mut batch = vec![datagram?];
    ///     while let Some(datagram) = datagrams.next_ready() {
    ///         batch.push(datagram?);
    ///     }
    ///     for datagram in batch {
    ///         let (data, from) = datagram.into_parts();
    ///         socket.send_to(data, from).await.0?;
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn next_ready(&mut self) -> Option<io::Result<Datagram>> {
        self.stream.next_ready()
    }

    /// Whether the stream has ended, after yielding an error.
    #[inline]
    pub fn is_ended(&self) -> bool {
        self.stream.is_ended()
    }
}

impl fmt::Debug for RecvFromStream<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.stream, f)
    }
}

#[cfg(feature = "futures")]
impl futures_core::Stream for RecvFromStream<'_> {
    type Item = io::Result<Datagram>;

    #[inline]
    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        RecvFromStream::poll_next(self.get_mut(), cx)
    }
}

/// Terminated once an error has been yielded.
#[cfg(feature = "futures")]
impl futures_core::stream::FusedStream for RecvFromStream<'_> {
    #[inline]
    fn is_terminated(&self) -> bool {
        self.is_ended()
    }
}
