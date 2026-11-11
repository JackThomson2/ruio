//! UDP sockets, which send and receive datagrams through the port: to and
//! from any address with `WSASendTo` and `WSARecvFrom`, and to and from a
//! connected socket's peer with `WSASend` and `WSARecv`.

use std::fmt;
use std::future::Future;
use std::io;
use std::net::{self, SocketAddr, ToSocketAddrs};
use std::os::windows::io::AsRawSocket;
use std::pin::Pin;
use std::ptr;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use ruio_buf::{BufResult, IoBuf, IoBufMut};

use super::{
    RawAddr, bind_to, family, first_bound, handle_of, last_error, open_socket, outcome, socket_of,
    start_recv, start_send, taken,
};
use crate::handle::{IoHandle, Raw};
use crate::op::{Limit, Op, Read, Started, Write};
use crate::sys;

impl Raw for net::UdpSocket {
    fn raw(&self) -> sys::Handle {
        handle_of(self.as_raw_socket())
    }
}

/// A UDP socket, whose sends and receives complete to the port of the
/// engine current where it was bound, and which belongs to that engine's
/// thread.
///
/// A send or a receive takes its buffer and hands it back with the result,
/// as ruio's I/O does on io_uring. Two things are as they are on other
/// systems rather than as Windows has them by default. A datagram longer
/// than the buffer it is received into fills the buffer, and the rest of it
/// is lost, without an error. And a datagram sent to a port that nobody is
/// bound to fails no later receive of the socket's.
pub struct UdpSocket {
    socket: IoHandle<net::UdpSocket>,
}

impl UdpSocket {
    /// Binds a socket to `addr`, or to the first address it resolves to
    /// that it can be bound to, as `std::net::UdpSocket::bind` does: a host
    /// name is resolved on the calling thread, which waits for it. A socket
    /// on an IPv6 address sends to and receives from IPv4 addresses too, as
    /// on Linux, in their IPv6 form.
    ///
    /// # Errors
    ///
    /// What binding reports, as when the address is in use, `InvalidInput`
    /// if `addr` resolves to no address, and an error of kind `Other` if
    /// the thread has no current engine, as outside a runtime.
    pub fn bind(addr: impl ToSocketAddrs) -> io::Result<Self> {
        let socket = first_bound(addr, |addr| {
            let socket = open_socket(family(addr), sys::SOCK_DGRAM, sys::IPPROTO_UDP)?;
            bind_to(&socket, addr)?;
            Ok(net::UdpSocket::from(socket))
        })?;
        // By default, a datagram that finds nobody at its port has Windows
        // fail the sender's receives with a reset, until one reports it.
        let report: u32 = 0;
        let mut returned = 0;
        // SAFETY: the socket is open; the call reads the flag and writes how
        // much it returned, which is nothing, and given no `OVERLAPPED` is
        // done when it returns.
        let failed = unsafe {
            sys::WSAIoctl(
                socket.as_raw_socket() as sys::Socket,
                sys::SIO_UDP_CONNRESET,
                ptr::from_ref(&report).cast(),
                size_of::<u32>() as u32,
                ptr::null_mut(),
                0,
                &mut returned,
                ptr::null_mut(),
                ptr::null(),
            )
        };
        if failed != 0 {
            return Err(last_error());
        }
        Ok(Self {
            socket: IoHandle::new(socket)?,
        })
    }

    /// The address the socket is bound to.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.io().local_addr()
    }

    /// The address the socket is connected to.
    ///
    /// # Errors
    ///
    /// What the system reports, as when the socket is not connected.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.socket.io().peer_addr()
    }

    /// Connects the socket to `addr`, the peer that [`send`](Self::send)
    /// sends to and the only one [`recv`](Self::recv) receives from. Nothing
    /// goes over the network, but a host name is resolved on the calling
    /// thread, as for [`bind`](Self::bind).
    ///
    /// # Errors
    ///
    /// What resolving and connecting report.
    pub fn connect(&self, addr: impl ToSocketAddrs) -> io::Result<()> {
        self.socket.io().connect(addr)
    }

    /// Sets whether the socket may send to broadcast addresses
    /// (`SO_BROADCAST`).
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub fn set_broadcast(&self, broadcast: bool) -> io::Result<()> {
        self.socket.io().set_broadcast(broadcast)
    }

    /// Whether the socket may send to broadcast addresses.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub fn broadcast(&self) -> io::Result<bool> {
        self.socket.io().broadcast()
    }

    /// Sends `buf`'s bytes as a datagram to the peer the socket is
    /// connected to; see [`Write`].
    pub fn send<B: IoBuf>(&self, buf: B) -> Write<'_, B> {
        Write::with(self.socket.target(), buf, 0, start_send::<B>, outcome)
    }

    /// Receives a datagram from the peer the socket is connected to into
    /// `buf`, up to its `bytes_total`; see [`Read`].
    pub fn recv<B: IoBufMut>(&self, buf: B) -> Read<'_, B> {
        let target = self.socket.target();
        Read::with(target, buf, 0, start_recv::<B>, datagram_outcome)
    }

    /// Sends `buf`'s bytes as a datagram to `addr`; see [`SendTo`].
    pub fn send_to<B: IoBuf>(&self, buf: B, addr: SocketAddr) -> SendTo<'_, B> {
        let sending = Sending {
            buf,
            to: RawAddr::new(&addr),
        };
        SendTo {
            op: Op::new(self.socket.target(), sending, 0),
        }
    }

    /// Receives a datagram into `buf`, up to its `bytes_total`, with the
    /// address it came from; see [`RecvFrom`].
    pub fn recv_from<B: IoBufMut>(&self, buf: B) -> RecvFrom<'_, B> {
        let receiving = Receiving {
            buf,
            from: RawAddr::room(),
        };
        RecvFrom {
            op: Op::new(self.socket.target(), receiving, 0),
        }
    }
}

impl fmt::Debug for UdpSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UdpSocket")
            .field("socket", self.socket.io())
            .finish()
    }
}

/// Reads the outcome of a datagram's receive, as `op`'s `Outcome` says. A
/// datagram longer than its buffer is received, as far as it fits, rather
/// than failed, as Winsock has it.
///
/// # Safety
///
/// As for `Outcome`.
unsafe fn datagram_outcome(
    socket: sys::Handle,
    overlapped: *const sys::Overlapped,
    bytes: u32,
) -> io::Result<u32> {
    // SAFETY: the caller's promise is the one `outcome` asks for.
    match unsafe { outcome(socket, overlapped, bytes) } {
        Err(err) if err.raw_os_error() == Some(sys::WSAEMSGSIZE) => Ok(bytes),
        result => result,
    }
}

/// What a send's slot holds while the system has the send: the datagram,
/// and the address it goes to.
struct Sending<B> {
    buf: B,
    to: RawAddr,
}

/// Starts a send of a datagram to an address, as `op`'s `Start` says.
///
/// # Safety
///
/// As for `Start`.
unsafe fn start_send_to<B: IoBuf>(
    socket: sys::Handle,
    sending: &mut Sending<B>,
    overlapped: *mut sys::Overlapped,
) -> io::Result<Started> {
    let buffer = sys::WsaBuf {
        len: u32::try_from(sending.buf.bytes_init()).unwrap_or(u32::MAX),
        buf: sending.buf.stable_ptr().cast_mut(),
    };
    let mut sent = 0;
    // SAFETY: the caller's promise for the socket, the slot and the
    // `OVERLAPPED`; the buffer holds that many initialized bytes, as `IoBuf`
    // promises, which a send only reads; the address is in the slot; and
    // Winsock copies the `WSABUF` and writes the count before the call
    // returns.
    let failed = unsafe {
        sys::WSASendTo(
            socket_of(socket),
            &buffer,
            1,
            &mut sent,
            0,
            sending.to.bytes.as_ptr(),
            sending.to.len,
            overlapped,
            ptr::null(),
        )
    };
    taken(failed == 0, sent)
}

/// The future of [`UdpSocket::send_to`]. It hands the buffer, an [`IoBuf`],
/// back with how many bytes it sent: all of them, as a datagram goes whole
/// or not at all.
///
/// It starts the send when first polled. Dropping it before the send
/// completes cancels the send, and the buffer is dropped once the
/// cancellation has completed.
#[must_use = "futures do nothing unless polled"]
pub struct SendTo<'a, B: IoBuf> {
    op: Op<'a, Sending<B>>,
}

impl<B: IoBuf> SendTo<'_, B> {
    /// Cancels the send if it has not completed `duration` after it starts:
    /// it then fails with `ERROR_TIMEOUT`, of [`io::ErrorKind::TimedOut`],
    /// and hands the buffer back, as with any other error.
    ///
    /// This has no effect once the send has started.
    pub fn timeout(mut self, duration: Duration) -> Self {
        self.op.set_limit(Limit::After(duration));
        self
    }
}

impl<B: IoBuf> Future for SendTo<'_, B> {
    type Output = BufResult<usize, B>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (result, sending) = ready!(self.op.poll_with(cx, start_send_to::<B>, outcome));
        Poll::Ready((result.map(|sent| sent as usize), sending.buf))
    }
}

impl<B: IoBuf> fmt::Debug for SendTo<'_, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendTo").finish_non_exhaustive()
    }
}

/// What a receive's slot holds while the system has the receive: the
/// buffer, and the room for the sender's address and its length, which the
/// system writes as the receive completes.
struct Receiving<B> {
    buf: B,
    from: RawAddr,
}

/// Starts a receive of a datagram and its sender's address, as `op`'s
/// `Start` says.
///
/// # Safety
///
/// As for `Start`.
unsafe fn start_recv_from<B: IoBufMut>(
    socket: sys::Handle,
    receiving: &mut Receiving<B>,
    overlapped: *mut sys::Overlapped,
) -> io::Result<Started> {
    let mut buffer = sys::WsaBuf {
        len: u32::try_from(receiving.buf.bytes_total()).unwrap_or(u32::MAX),
        buf: receiving.buf.stable_mut_ptr(),
    };
    let mut flags = 0;
    let mut received = 0;
    // SAFETY: the caller's promise for the socket, the slot and the
    // `OVERLAPPED`; the buffer is writable for that many bytes, as
    // `IoBufMut` promises; the room for the address and its length are in
    // the slot, where the system writes them as the receive completes; and
    // Winsock copies the `WSABUF`, reads the flags and writes the count
    // before the call returns.
    let failed = unsafe {
        sys::WSARecvFrom(
            socket_of(socket),
            &mut buffer,
            1,
            &mut received,
            &mut flags,
            receiving.from.bytes.as_mut_ptr(),
            &mut receiving.from.len,
            overlapped,
            ptr::null(),
        )
    };
    taken(failed == 0, received)
}

/// The future of [`UdpSocket::recv_from`]. It hands the buffer, an
/// [`IoBufMut`], back with how many bytes of the datagram it holds and the
/// address the datagram came from.
///
/// It starts the receive when first polled. Dropping it before the receive
/// completes cancels the receive, and the buffer is dropped once the
/// cancellation has completed.
#[must_use = "futures do nothing unless polled"]
pub struct RecvFrom<'a, B: IoBufMut> {
    op: Op<'a, Receiving<B>>,
}

impl<B: IoBufMut> RecvFrom<'_, B> {
    /// Cancels the receive if no datagram has come `duration` after it
    /// starts: it then fails with `ERROR_TIMEOUT`, of
    /// [`io::ErrorKind::TimedOut`], and hands the buffer back, as with any
    /// other error. A datagram received before the cancellation can stop
    /// the receive is the receive's.
    ///
    /// This has no effect once the receive has started.
    pub fn timeout(mut self, duration: Duration) -> Self {
        self.op.set_limit(Limit::After(duration));
        self
    }
}

impl<B: IoBufMut> Future for RecvFrom<'_, B> {
    type Output = BufResult<(usize, SocketAddr), B>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let start = start_recv_from::<B>;
        let (result, receiving) = ready!(self.op.poll_with(cx, start, datagram_outcome));
        let Receiving { mut buf, from } = receiving;
        let result = result.and_then(|received| {
            // A count beyond the buffer would be the length of a datagram
            // that did not fit, of which the buffer holds what did.
            let received = (received as usize).min(buf.bytes_total());
            // SAFETY: the system wrote the bytes it reports receiving from
            // the start of the buffer, within its `bytes_total`.
            unsafe { buf.set_init(received) };
            Ok((received, from.to_socket_addr()?))
        });
        Poll::Ready((result, buf))
    }
}

impl<B: IoBufMut> fmt::Debug for RecvFrom<'_, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecvFrom").finish_non_exhaustive()
    }
}
