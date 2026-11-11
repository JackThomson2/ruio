//! A stream's data, received into a pool of buffers as it arrives.

use std::fmt;
use std::future::poll_fn;
use std::io::{self, Read as _};
use std::net;
use std::rc::Rc;
use std::task::{Context, Poll, Waker, ready};

use crate::buf::{BufBundle, BufLease, BufPool, Claim};
use crate::handle::IoHandle;
use crate::op::Op;
use crate::sys;

use super::{Socket, outcome, socket_of, taken};

/// The data arriving on a stream, received into a [`BufPool`]. Made by
/// [`TcpStream::recv_stream`](super::TcpStream::recv_stream).
///
/// Each item is one receive's data, as a bundle of one lease on a buffer of
/// the pool. The stream ends when the peer shuts down, or after yielding an
/// error.
///
/// A stream that waits for data holds no buffer. A receive of no bytes
/// waits in its place, which the system completes once the socket has data,
/// and only then does the stream take a buffer of the pool, to receive into
/// without waiting. So the streams that wait leave the pool's buffers to
/// those that have data, and when every buffer is leased, a stream that has
/// data waits for one to return.
///
/// The first poll makes the socket non-blocking, so that a receive finding
/// that another took the data fails rather than waits. The stream's other
/// operations go through the port, and are the same either way.
///
/// Dropping the stream cancels the receive that waits. Data that has
/// arrived stays in the socket, for what reads it next.
pub struct RecvStream<'a> {
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
        Self {
            stream,
            claim: Claim::new(pool),
            state: State::Idle,
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
    /// left more behind. Appending it to the data taken before gathers what
    /// has arrived:
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
        matches!(self.state, State::Ended)
    }

    /// Takes the stream as far as it goes without waiting: to its next
    /// data, or its end. Where it has to wait, it arranges for the task of
    /// `cx` to be woken, or, without one, starts nothing and arranges
    /// nothing.
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
                State::Waiting(op) => {
                    let (result, _) = match cx.as_deref_mut() {
                        Some(cx) => ready!(op.poll_with(cx, start_wait, outcome)),
                        // A receive whose completion has been delivered is
                        // finished by its poll, which keeps no waker.
                        None if op.is_done() => {
                            let mut cx = Context::from_waker(Waker::noop());
                            ready!(op.poll_with(&mut cx, start_wait, outcome))
                        }
                        None => return Poll::Pending,
                    };
                    match result {
                        Ok(_) => self.state = State::Arrived,
                        // The half that receives was shut down on this end.
                        Err(err) if err.raw_os_error() == Some(sys::WSAESHUTDOWN) => {
                            self.state = State::Ended;
                        }
                        Err(err) => return self.fail(err),
                    }
                }
                State::Arrived => {
                    let mut lease = match cx.as_deref_mut() {
                        Some(cx) => ready!(self.claim.poll_take(cx)),
                        None => match self.claim.take() {
                            Some(lease) => lease,
                            None => return Poll::Pending,
                        },
                    };
                    match receive(&self.stream.io().socket, &mut lease) {
                        // Nothing more comes: the peer shut down.
                        Ok(0) => self.state = State::Ended,
                        Ok(received) => {
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
) -> io::Result<()> {
    let mut buffer = sys::WsaBuf { len: 0, buf: byte };
    let mut flags = 0;
    // SAFETY: the caller's promise for the socket and the `OVERLAPPED`; the
    // buffer is none of the byte it starts at, which the slot keeps in place
    // all the same; and Winsock copies the `WSABUF` and reads the flags
    // before the call returns.
    let failed = unsafe {
        sys::WSARecv(
            socket_of(socket),
            &mut buffer,
            1,
            std::ptr::null_mut(),
            &mut flags,
            overlapped,
            std::ptr::null(),
        )
    };
    taken(failed == 0)
}
