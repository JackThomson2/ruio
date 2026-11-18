//! A UDP socket's datagrams, received into a pool of buffers as they
//! arrive, each with the address it came from.

use std::fmt;
use std::future::poll_fn;
use std::io;
use std::net::{self, SocketAddr};
use std::ops::{Deref, DerefMut};
use std::ptr;
use std::task::{Context, Poll, ready};

use crate::buf::{BufLease, BufPool, Claim};
use crate::handle::IoHandle;
use crate::op::Op;
use crate::sys;

use super::recv::{poll_lease, poll_wait, start_peek};
use super::{RawAddr, socket_of};

/// How many bytes of each buffer an IPv4 socket's datagram leaves out
/// before its payload: those that the header of a receive and the address
/// take on io_uring.
const IPV4_HEADER: usize = 32;

/// How many bytes of each buffer an IPv6 socket's datagram leaves out, as
/// for [`IPV4_HEADER`].
const IPV6_HEADER: usize = 48;

/// The datagrams arriving on a UDP socket, each with the address it came
/// from, received into a [`BufPool`]. Made by
/// [`UdpSocket::recv_from_stream`](super::UdpSocket::recv_from_stream).
///
/// Each datagram takes one buffer, whose first bytes it leaves out, as many
/// as a header and the address take before the payload on io_uring: 32 of
/// them for an IPv4 socket, and 48 for an IPv6 one. So a pool's buffers
/// hold as much of a datagram here as they do there. A datagram longer than
/// the rest of its buffer is cut short, and the rest of it lost, which
/// [`Datagram::is_truncated`] tells.
///
/// A stream that waits for a datagram holds no buffer. A receive of no
/// bytes that peeks waits in its place, which the system completes once
/// the socket has a datagram, leaving it there, and only then does the
/// stream take a buffer of the pool, to receive the datagram into without
/// waiting, and then each of those behind it, until the socket has none
/// left. When the pool runs dry, the stream waits for a buffer to return,
/// and the datagrams that arrive meanwhile wait in the socket, as many as
/// its receive buffer holds.
///
/// As for a [`RecvStream`](super::RecvStream), the first poll makes the
/// socket non-blocking, and a receive of a datagram that has arrived costs
/// no turn of the port, and is counted with the operations that finish as
/// they start, so that a stream of datagrams keeps no other task from
/// running.
///
/// The stream ends after yielding an error. Dropping it cancels the receive
/// that waits; the datagrams that have arrived stay in the socket.
pub struct RecvFromStream<'a> {
    datagrams: Datagrams<'a>,
}

impl<'a> RecvFromStream<'a> {
    /// The datagrams arriving on `socket`, bound to `local`, received into
    /// `pool`.
    pub(super) fn new(
        socket: &'a IoHandle<net::UdpSocket>,
        pool: &BufPool,
        local: SocketAddr,
    ) -> io::Result<Self> {
        let header = match local {
            SocketAddr::V4(_) => IPV4_HEADER,
            SocketAddr::V6(_) => IPV6_HEADER,
        };
        if pool.buffer_size() <= header {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the pool's buffers have no room for a datagram after its header and address",
            ));
        }
        Ok(Self {
            datagrams: Datagrams::new(socket, pool, header),
        })
    }

    /// The next datagram, or `None` once an error has been yielded.
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<Datagram>>> {
        self.step(Some(cx))
    }

    /// The next datagram; see [`poll_next`](RecvFromStream::poll_next).
    pub async fn next(&mut self) -> Option<io::Result<Datagram>> {
        poll_fn(|cx| self.poll_next(cx)).await
    }

    /// The next datagram if it has arrived already, or `None`, without
    /// waiting.
    ///
    /// A datagram has arrived if the system has said so since the stream
    /// was last polled, or if the stream has received one since, which may
    /// have had others behind it. It is `None` all the same once the stream
    /// has received as many as it may between two turns of the port. A task
    /// that answers in batches takes those that have arrived after each one
    /// it waits for:
    ///
    /// ```no_run
    /// # async fn answer(socket: ruio_iocp::net::UdpSocket, pool: ruio_iocp::buf::BufPool) -> std::io::Result<()> {
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
    pub fn next_ready(&mut self) -> Option<io::Result<Datagram>> {
        match self.step(None) {
            Poll::Ready(next) => next,
            Poll::Pending => None,
        }
    }

    /// Whether the stream has ended, after yielding an error.
    pub fn is_ended(&self) -> bool {
        self.datagrams.is_ended()
    }

    fn step(&mut self, cx: Option<&mut Context<'_>>) -> Poll<Option<io::Result<Datagram>>> {
        self.datagrams
            .step(cx)
            .map(|next| next.map(|received| received.and_then(Datagram::new)))
    }
}

impl fmt::Debug for RecvFromStream<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecvFromStream")
            .field("ended", &self.is_ended())
            .finish_non_exhaustive()
    }
}

/// A datagram that a UDP socket received into a buffer of a pool, with the
/// address it came from. It derefs to the payload. Made by
/// [`RecvFromStream`].
///
/// The buffer returns to the pool when the datagram is dropped, or when the
/// lease of its payload that [`into_data`](Datagram::into_data) makes is.
pub struct Datagram {
    data: BufLease,
    from: SocketAddr,
    truncated: bool,
}

impl Datagram {
    fn new(received: Received) -> io::Result<Self> {
        Ok(Self {
            from: received.from.to_socket_addr()?,
            data: received.lease,
            truncated: received.truncated,
        })
    }

    /// The address the datagram came from.
    #[inline]
    pub fn from(&self) -> SocketAddr {
        self.from
    }

    /// Whether the datagram was longer than its buffer had room for, so that
    /// the system cut it short, and the rest of it was lost.
    #[inline]
    pub fn is_truncated(&self) -> bool {
        self.truncated
    }

    /// The payload, as a lease of its buffer whose data and room leave out
    /// the start of the buffer that the datagram left out, to keep or to
    /// send as any buffer.
    #[inline]
    pub fn into_data(self) -> BufLease {
        self.data
    }

    /// The payload, as [`into_data`](Datagram::into_data) leases it, and the
    /// address it came from: what an answer to the datagram takes.
    #[inline]
    pub fn into_parts(self) -> (BufLease, SocketAddr) {
        (self.data, self.from)
    }
}

impl Deref for Datagram {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        &self.data
    }
}

impl DerefMut for Datagram {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

impl fmt::Debug for Datagram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Datagram")
            .field("from", &self.from)
            .field("len", &self.len())
            .field("truncated", &self.truncated)
            .finish()
    }
}

/// A UDP socket's datagrams, each received into a buffer of a pool as it
/// arrives, which a [`RecvFromStream`] yields with their addresses, and a
/// UDP socket's [`RecvStream`](super::RecvStream) without.
pub(super) struct Datagrams<'a> {
    socket: &'a IoHandle<net::UdpSocket>,
    claim: Claim,
    /// How many bytes of each buffer a datagram leaves out before it.
    header: usize,
    /// Whether the socket was made non-blocking, as the first wait makes
    /// it.
    unblocked: bool,
    state: State<'a>,
}

enum State<'a> {
    /// The socket has no datagram that the stream knows of: the next poll
    /// starts waiting for one.
    Idle,
    /// A receive of no bytes that peeks is in flight, which completes once
    /// the socket has a datagram. Its slot holds the byte that the receive
    /// names as its buffer.
    Waiting(Op<'a, u8>),
    /// The socket may have datagrams, to receive once the pool has a
    /// buffer.
    Arrived,
    /// An error was yielded.
    Ended,
}

/// A datagram as it was received: into a lease that leaves out the start
/// of its buffer, from the address the system wrote, and whether the system
/// cut it short.
pub(super) struct Received {
    pub(super) lease: BufLease,
    from: RawAddr,
    truncated: bool,
}

impl<'a> Datagrams<'a> {
    /// The datagrams arriving on `socket`, received into `pool`, each
    /// leaving out the first `header` bytes of its buffer.
    pub(super) fn new(socket: &'a IoHandle<net::UdpSocket>, pool: &BufPool, header: usize) -> Self {
        Self {
            socket,
            claim: Claim::new(pool),
            header,
            unblocked: false,
            state: State::Idle,
        }
    }

    /// Whether an error has been yielded, which ends the datagrams.
    pub(super) fn is_ended(&self) -> bool {
        matches!(self.state, State::Ended)
    }

    /// Takes the datagrams as far as they go without waiting: to the next,
    /// or their end. Where it has to wait, it arranges for the task of `cx`
    /// to be woken, or, without one, starts nothing and arranges nothing.
    pub(super) fn step(
        &mut self,
        mut cx: Option<&mut Context<'_>>,
    ) -> Poll<Option<io::Result<Received>>> {
        loop {
            match &mut self.state {
                State::Ended => return Poll::Ready(None),
                State::Idle => {
                    if cx.is_none() {
                        return Poll::Pending;
                    }
                    if !self.unblocked {
                        if let Err(err) = self.socket.io().set_nonblocking(true) {
                            return self.fail(err);
                        }
                        self.unblocked = true;
                    }
                    self.state = State::Waiting(Op::new(self.socket.target(), 0, 0));
                }
                State::Waiting(op) => match ready!(poll_wait(op, cx.as_deref_mut(), start_peek)) {
                    Ok(_) => self.state = State::Arrived,
                    // The peek fails so for any datagram but one of no
                    // bytes, and leaves it in the socket all the same.
                    Err(err) if err.raw_os_error() == Some(sys::WSAEMSGSIZE) => {
                        self.state = State::Arrived;
                    }
                    Err(err) => return self.fail(err),
                },
                State::Arrived => {
                    let engine = self.socket.engine();
                    let mut lease = ready!(poll_lease(engine, &mut self.claim, cx.as_deref_mut()));
                    lease.leave_out(self.header);
                    let mut from = RawAddr::room();
                    match receive(self.socket.raw(), &mut lease, &mut from) {
                        // Others may wait behind it, so the stream stays
                        // where it is.
                        Ok(truncated) => {
                            engine.finished_at_once();
                            let received = Received {
                                lease,
                                from,
                                truncated,
                            };
                            return Poll::Ready(Some(Ok(received)));
                        }
                        // The socket has no datagram left.
                        Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                            self.state = State::Idle;
                        }
                        Err(err) => return self.fail(err),
                    }
                }
            }
        }
    }

    /// Ends the datagrams with `err`, which they yield.
    fn fail(&mut self, err: io::Error) -> Poll<Option<io::Result<Received>>> {
        self.state = State::Ended;
        Poll::Ready(Some(Err(err)))
    }
}

/// Receives a datagram that has arrived on `socket`, a UDP socket that does
/// not block, into `lease`'s room without waiting, and the address it came
/// from into `from`: whether the datagram was longer than the room, which
/// then holds its start, the rest of it lost; or `WouldBlock` if the socket
/// has none.
fn receive(socket: sys::Handle, lease: &mut BufLease, from: &mut RawAddr) -> io::Result<bool> {
    let mut truncated = false;
    lease.fill(|room| {
        let mut buffer = sys::WsaBuf {
            len: u32::try_from(room.len()).unwrap_or(u32::MAX),
            buf: room.as_mut_ptr(),
        };
        let mut flags = 0;
        let mut received = 0;
        // SAFETY: the socket is open, as its owner is borrowed; the buffer
        // is writable for its length, and the room for the address for its
        // own; and given no `OVERLAPPED`, the call is done with them, the
        // `WSABUF`, the flags and the count when it returns.
        let failed = unsafe {
            sys::WSARecvFrom(
                socket_of(socket),
                &mut buffer,
                1,
                &mut received,
                &mut flags,
                from.bytes.as_mut_ptr(),
                &mut from.len,
                ptr::null_mut(),
                ptr::null(),
            )
        };
        if failed == 0 {
            return Ok(received as usize);
        }
        match sys::WSAGetLastError() {
            // The room holds as much of the datagram as fits.
            sys::WSAEMSGSIZE => {
                truncated = true;
                Ok(room.len())
            }
            code => Err(io::Error::from_raw_os_error(code)),
        }
    })?;
    Ok(truncated)
}
