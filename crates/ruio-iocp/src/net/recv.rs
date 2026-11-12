//! A stream's data, or a UDP socket's datagrams, received into a pool of
//! buffers as they arrive.

use std::fmt;
use std::future::poll_fn;
use std::io::{self, Read as _};
use std::net;
use std::rc::Rc;
use std::task::{Context, Poll, Waker, ready};

use crate::buf::{BufBundle, BufLease, BufPool, Claim};
use crate::engine::Iocp;
use crate::handle::IoHandle;
use crate::op::{Op, Start, Started};
use crate::sys;

use super::datagrams::Datagrams;
use super::{Socket, outcome, socket_of, taken};

/// The data arriving on a stream, received into a [`BufPool`]. Made by
/// [`TcpStream::recv_stream`](super::TcpStream::recv_stream), and by
/// [`UdpSocket::recv_stream`](super::UdpSocket::recv_stream) for the
/// datagrams arriving on a UDP socket.
///
/// Each item is one receive's data, as a bundle of one lease on a buffer of
/// the pool. The stream ends when the peer shuts down, or after yielding an
/// error.
///
/// A UDP socket's items are its datagrams, one to a buffer, without the
/// addresses they came from, and its stream ends only after yielding an
/// error. A datagram longer than its buffer is cut short, and the rest of it
/// lost.
///
/// A stream that waits for data holds no buffer. A receive of no bytes
/// waits in its place, which the system completes once the socket has data,
/// and only then does the stream take a buffer of the pool, to receive into
/// without waiting. So the streams that wait leave the pool's buffers to
/// those that have data, and when every buffer is leased, a stream that has
/// data waits for one to return. A UDP socket's receive of no bytes peeks,
/// so that the datagram it completes for stays in the socket.
///
/// The first poll makes the socket non-blocking, so that a receive finding
/// that another took the data fails rather than waits. The stream's other
/// operations go through the port, and are the same either way.
///
/// A receive of data that has arrived goes through no port, and so costs
/// no turn of it, as an operation that finishes as it starts does, and it
/// is counted with those: once as many have finished so since the port was
/// last turned as may between two, the stream wakes its task and waits for
/// the next turn. So a peer that sends faster than the stream takes its
/// data keeps no other task from running.
///
/// Dropping the stream cancels the receive that waits. Data that has
/// arrived stays in the socket, for what reads it next.
pub struct RecvStream<'a> {
    source: Source<'a>,
}

/// What a stream receives.
enum Source<'a> {
    /// A TCP stream's data, in whatever pieces it arrives in.
    Data(Data<'a>),
    /// A UDP socket's datagrams, each in a buffer of its own.
    Datagrams(Datagrams<'a>),
}

/// A TCP stream's data, as it is received into a pool.
struct Data<'a> {
    stream: &'a IoHandle<Rc<Socket>>,
    claim: Claim,
    state: State<'a>,
}

enum State<'a> {
    /// The socket has no data that the stream knows of: the next poll
    /// starts waiting for some.
    Idle,
    /// A receive of no bytes is in flight, which completes once the socket
    /// has data to receive, or its stream has ended. Its slot holds the
    /// byte that the receive names as its buffer.
    Waiting(Op<'a, u8>),
    /// The socket may have data, to receive once the pool has a buffer.
    Arrived,
    /// The peer shut down, or an error was yielded.
    Ended,
}

impl<'a> RecvStream<'a> {
    pub(super) fn new(stream: &'a IoHandle<Rc<Socket>>, pool: &BufPool) -> Self {
        let data = Data {
            stream,
            claim: Claim::new(pool),
            state: State::Idle,
        };
        Self {
            source: Source::Data(data),
        }
    }

    /// The stream of a UDP socket's `datagrams`, which leave none of their
    /// buffers out.
    pub(super) fn of_datagrams(datagrams: Datagrams<'a>) -> Self {
        Self {
            source: Source::Datagrams(datagrams),
        }
    }

    /// The next data, or `None` once the peer has shut down or an error has
    /// been yielded.
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<BufBundle>>> {
        self.step(Some(cx))
    }

    /// The next data; see [`poll_next`](RecvStream::poll_next).
    pub async fn next(&mut self) -> Option<io::Result<BufBundle>> {
        poll_fn(|cx| self.poll_next(cx)).await
    }

    /// The next data if it has arrived already, or `None`, without waiting.
    ///
    /// Data has arrived if the system has said so since the stream was last
    /// polled, or if the last receive filled its buffer, which may have
    /// left more behind, or took a datagram, which may have had others
    /// behind it. It is `None` all the same once the stream has received as
    /// much as it may between two turns of the port. Appending it to the
    /// data taken before gathers what has arrived:
    ///
    /// ```no_run
    /// # async fn gather(stream: ruio_iocp::net::TcpStream, pool: ruio_iocp::buf::BufPool) {
    /// let mut received = stream.recv_stream(&pool);
    /// while let Some(Ok(mut data)) = received.next().await {
    ///     while let Some(Ok(more)) = received.next_ready() {
    ///         data.append(more);
    ///     }
    ///     println!("{} bytes in {} buffers", data.len(), data.buffers());
    /// }
    /// # }
    /// ```
    pub fn next_ready(&mut self) -> Option<io::Result<BufBundle>> {
        match self.step(None) {
            Poll::Ready(next) => next,
            Poll::Pending => None,
        }
    }

    /// Whether the stream has ended: the peer has shut down, or an error has
    /// been yielded.
    pub fn is_ended(&self) -> bool {
        match &self.source {
            Source::Data(data) => matches!(data.state, State::Ended),
            Source::Datagrams(datagrams) => datagrams.is_ended(),
        }
    }

    /// Takes the stream as far as it goes without waiting: to its next
    /// data, or its end. Where it has to wait, it arranges for the task of
    /// `cx` to be woken, or, without one, starts nothing and arranges
    /// nothing.
    fn step(&mut self, cx: Option<&mut Context<'_>>) -> Poll<Option<io::Result<BufBundle>>> {
        match &mut self.source {
            Source::Data(data) => data.step(cx),
            Source::Datagrams(datagrams) => datagrams.step(cx).map(|next| {
                next.map(|received| received.map(|datagram| BufBundle::of(datagram.lease)))
            }),
        }
    }
}

impl Data<'_> {
    /// Takes the data as far as it goes without waiting, as
    /// [`RecvStream::step`] does.
    fn step(&mut self, mut cx: Option<&mut Context<'_>>) -> Poll<Option<io::Result<BufBundle>>> {
        loop {
            match &mut self.state {
                State::Ended => return Poll::Ready(None),
                State::Idle => {
                    if cx.is_none() {
                        return Poll::Pending;
                    }
                    if let Err(err) = self.stream.io().unblock() {
                        return self.fail(err);
                    }
                    self.state = State::Waiting(Op::new(self.stream.target(), 0, 0));
                }
                State::Waiting(op) => match ready!(poll_wait(op, cx.as_deref_mut(), start_wait)) {
                    Ok(_) => self.state = State::Arrived,
                    // The half that receives was shut down on this end.
                    Err(err) if err.raw_os_error() == Some(sys::WSAESHUTDOWN) => {
                        self.state = State::Ended;
                    }
                    Err(err) => return self.fail(err),
                },
                State::Arrived => {
                    let engine = self.stream.engine();
                    let mut lease = ready!(poll_lease(engine, &mut self.claim, cx.as_deref_mut()));
                    match receive(&self.stream.io().socket, &mut lease) {
                        // Nothing more comes: the peer shut down.
                        Ok(0) => self.state = State::Ended,
                        Ok(received) => {
                            engine.finished_at_once();
                            // A receive that filled its buffer may have
                            // left data behind.
                            self.state = if received == lease.capacity() {
                                State::Arrived
                            } else {
                                State::Idle
                            };
                            return Poll::Ready(Some(Ok(BufBundle::of(lease))));
                        }
                        // Another receive took the data first.
                        Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                            self.state = State::Idle;
                        }
                        Err(err) => return self.fail(err),
                    }
                }
            }
        }
    }

    /// Ends the stream with `err`, which it yields.
    fn fail(&mut self, err: io::Error) -> Poll<Option<io::Result<BufBundle>>> {
        self.state = State::Ended;
        Poll::Ready(Some(Err(err)))
    }
}

impl fmt::Debug for RecvStream<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecvStream")
            .field("ended", &self.is_ended())
            .finish_non_exhaustive()
    }
}

/// Polls `op`, a receive of no bytes that `start` starts, for how it
/// completed: with the task of `cx` to wake while it waits, or, without
/// one, only once its completion has been delivered, starting nothing and
/// arranging nothing.
pub(super) fn poll_wait(
    op: &mut Op<'_, u8>,
    cx: Option<&mut Context<'_>>,
    start: Start<u8>,
) -> Poll<io::Result<u32>> {
    let (result, _) = match cx {
        Some(cx) => ready!(op.poll_with(cx, start, outcome)),
        // A receive whose completion has been delivered is finished by its
        // poll, which keeps no waker.
        None if op.is_done() => {
            let mut cx = Context::from_waker(Waker::noop());
            ready!(op.poll_with(&mut cx, start, outcome))
        }
        None => return Poll::Pending,
    };
    Poll::Ready(result)
}

/// A buffer of `claim`'s pool, as a lease that holds no data yet, to
/// receive what has arrived into without waiting: once `engine` may finish
/// another operation as it starts before the next turn of its port, and the
/// pool has a buffer for the reader. Until then it arranges for the task of
/// `cx` to be woken, or, without one, arranges nothing.
pub(super) fn poll_lease(
    engine: &Iocp,
    claim: &mut Claim,
    cx: Option<&mut Context<'_>>,
) -> Poll<BufLease> {
    // The receive costs no turn of the port, so it waits for one once as
    // many operations have finished as they started as may between two.
    if engine.at_once_spent() {
        if let Some(cx) = cx {
            cx.waker().wake_by_ref();
        }
        return Poll::Pending;
    }
    match cx {
        Some(cx) => claim.poll_take(cx),
        None => claim.take().map_or(Poll::Pending, Poll::Ready),
    }
}

/// Receives what `socket`, which does not block, has into `lease`'s buffer,
/// without waiting: how many bytes, none of them once the peer has shut
/// down, or `WouldBlock` if the socket has no data.
fn receive(mut socket: &net::TcpStream, lease: &mut BufLease) -> io::Result<usize> {
    lease.fill(|buf| socket.read(buf))
}

/// Starts a receive of no bytes, which completes once `socket` has data to
/// receive or its stream has ended, and takes none of the data, as `op`'s
/// `Start` says. `byte` is what it names as its buffer, and does not write.
///
/// # Safety
///
/// As for `Start`.
unsafe fn start_wait(
    socket: sys::Handle,
    byte: &mut u8,
    overlapped: *mut sys::Overlapped,
) -> io::Result<Started> {
    // SAFETY: the caller's promise is the one `start_empty` asks for.
    unsafe { start_empty(socket, byte, overlapped, 0) }
}

/// Starts a receive of no bytes that peeks, which completes once `socket`,
/// a UDP socket, has a datagram, and leaves the datagram in it, as `op`'s
/// `Start` says: it succeeds for a datagram of no bytes, and fails for any
/// other with `WSAEMSGSIZE`, as for a datagram longer than its buffer.
/// `byte` is what it names as its buffer, and does not write.
///
/// # Safety
///
/// As for `Start`.
pub(super) unsafe fn start_peek(
    socket: sys::Handle,
    byte: &mut u8,
    overlapped: *mut sys::Overlapped,
) -> io::Result<Started> {
    // SAFETY: the caller's promise is the one `start_empty` asks for.
    unsafe { start_empty(socket, byte, overlapped, sys::MSG_PEEK) }
}

/// Starts a receive of no bytes on `socket` with `flags`, as `op`'s `Start`
/// says. `byte` is what it names as its buffer, and does not write.
///
/// # Safety
///
/// As for `Start`.
unsafe fn start_empty(
    socket: sys::Handle,
    byte: &mut u8,
    overlapped: *mut sys::Overlapped,
    mut flags: u32,
) -> io::Result<Started> {
    let mut buffer = sys::WsaBuf { len: 0, buf: byte };
    let mut received = 0;
    // SAFETY: the caller's promise for the socket and the `OVERLAPPED`; the
    // buffer is none of the byte it starts at, which the slot keeps in place
    // all the same; and Winsock copies the `WSABUF`, reads the flags and
    // writes the count before the call returns.
    let failed = unsafe {
        sys::WSARecv(
            socket_of(socket),
            &mut buffer,
            1,
            &mut received,
            &mut flags,
            overlapped,
            std::ptr::null(),
        )
    };
    taken(failed == 0, received)
}
