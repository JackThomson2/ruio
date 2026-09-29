//! What the socket types share: their descriptors, and the futures of their
//! operations.

use std::fmt;
use std::future::{Future, poll_fn};
use std::io;
use std::marker::PhantomData;
use std::mem;
use std::net;
use std::os::fd::{AsRawFd, OwnedFd};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use ruio_driver::buf::{BufBundle, BufPool, BufResult, IoBuf, IoBufMut, Slice};
use ruio_driver::ops::{self, Recv, Send, Shutdown};
use ruio_driver::{Descriptor, DirectFd, Driver, Fd, Op, Operation};

/// Makes a type `!Send` and `!Sync`: sockets belong to the thread whose ring
/// their I/O runs on. See DESIGN.md on keeping work stealing possible.
pub(crate) type ThreadBound = PhantomData<*const ()>;

/// A socket's descriptor: a plain one, as the standard library's socket
/// type, which does what the ring cannot, or a direct descriptor.
pub(crate) enum Sock<S> {
    Plain(S),
    Direct(DirectFd),
}

impl<S: From<OwnedFd>> Sock<S> {
    pub(crate) fn new(descriptor: Descriptor) -> Self {
        match descriptor {
            Descriptor::Plain(fd) => Sock::Plain(S::from(fd)),
            Descriptor::Direct(fd) => Sock::Direct(fd),
        }
    }
}

impl<S: AsRawFd> Sock<S> {
    /// The descriptor, for operations on the socket's ring.
    pub(crate) fn fd(&self) -> Fd {
        match self {
            Sock::Plain(socket) => Fd::Plain(socket.as_raw_fd()),
            Sock::Direct(fd) => fd.fd(),
        }
    }

    pub(crate) fn is_direct(&self) -> bool {
        matches!(self, Sock::Direct(_))
    }

    /// The ring to run an operation on, and the descriptor to give it: the
    /// thread's for a plain descriptor, and its own for a direct one.
    fn target(&self) -> (Driver, Fd) {
        match self {
            Sock::Plain(socket) => (crate::driver(), Fd::Plain(socket.as_raw_fd())),
            Sock::Direct(fd) => match fd.driver() {
                Some(driver) => (driver, fd.fd()),
                // The ring is gone, and the file with it. No table has this
                // slot, so the kernel fails the operation with `EBADF`.
                None => (crate::driver(), Fd::Direct(u32::MAX)),
            },
        }
    }

    /// The standard library's socket, for what only a plain descriptor can
    /// do.
    pub(crate) fn plain(&self) -> io::Result<&S> {
        match self {
            Sock::Plain(socket) => Ok(socket),
            Sock::Direct(_) => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "not available for a direct descriptor",
            )),
        }
    }

    /// Runs the operation `make` makes for the descriptor on its ring.
    pub(crate) fn op<T: Operation>(&self, make: impl FnOnce(Fd) -> T) -> SocketOp<'_, T> {
        self.op_map(make, |output| output)
    }

    /// Like [`Sock::op`], turning the output into what the socket reports.
    pub(crate) fn op_map<T: Operation, U>(
        &self,
        make: impl FnOnce(Fd) -> T,
        output: fn(T::Output) -> U,
    ) -> SocketOp<'_, T, U> {
        let (driver, fd) = self.target();
        SocketOp::new(driver.op(make(fd)), output)
    }

    pub(crate) fn read<B: IoBufMut>(&self, buf: B) -> SocketOp<'_, Recv<B>> {
        self.op(|fd| Recv::new(fd, buf))
    }

    pub(crate) fn read_exact<B: IoBufMut>(&self, buf: B) -> ReadExact<'_, B> {
        let (driver, fd) = self.target();
        ReadExact {
            transfer: Transfer::new(driver, fd, buf),
            _socket: PhantomData,
        }
    }

    pub(crate) fn write<B: IoBuf>(&self, buf: B) -> SocketOp<'_, Send<B>> {
        self.op(|fd| Send::new(fd, buf))
    }

    pub(crate) fn write_all<B: IoBuf>(&self, buf: B) -> WriteAll<'_, B> {
        let (driver, fd) = self.target();
        WriteAll {
            transfer: Transfer::new(driver, fd, buf),
            _socket: PhantomData,
        }
    }

    pub(crate) fn send_zc<B: IoBuf>(&self, buf: B) -> SendZc<'_, B> {
        let (driver, fd) = self.target();
        SendZc::new(ops::SendZc::new(&driver, fd, buf).flags(libc::MSG_WAITALL))
    }

    pub(crate) fn recv_stream(&self, pool: &BufPool) -> RecvStream<'_> {
        let (driver, fd) = self.target();
        RecvStream {
            stream: ops::RecvStream::new(&driver, fd, pool),
            _socket: PhantomData,
        }
    }

    pub(crate) fn shutdown(&self, how: net::Shutdown) -> SocketOp<'_, Shutdown> {
        self.op(|fd| Shutdown::new(fd, how))
    }
}

impl<S: fmt::Debug> fmt::Debug for Sock<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Sock::Plain(socket) => socket.fmt(f),
            Sock::Direct(fd) => fd.fmt(f),
        }
    }
}

/// An operation on a socket, which it borrows so that the descriptor stays
/// open until the kernel has the operation.
///
/// [`timeout`](SocketOp::timeout) bounds how long the kernel may take over
/// it.
#[must_use = "an operation does nothing until polled"]
pub struct SocketOp<'a, T: Operation, U = <T as Operation>::Output> {
    op: Op<T>,
    output: fn(T::Output) -> U,
    _socket: PhantomData<&'a ()>,
}

impl<T: Operation, U> SocketOp<'_, T, U> {
    pub(crate) fn new(op: Op<T>, output: fn(T::Output) -> U) -> Self {
        Self {
            op,
            output,
            _socket: PhantomData,
        }
    }

    /// Cancels the operation if the kernel has not completed it `duration`
    /// after it is submitted, with a linked timeout; see [`Op::timeout`].
    ///
    /// The operation then fails with [`io::ErrorKind::TimedOut`], and a
    /// buffer it holds comes back, as with any other error.
    pub fn timeout(self, duration: Duration) -> Self {
        Self {
            op: self.op.timeout(duration),
            ..self
        }
    }
}

impl<T: Operation, U> Future for SocketOp<'_, T, U> {
    type Output = U;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<U> {
        let this = self.get_mut();
        Pin::new(&mut this.op).poll(cx).map(this.output)
    }
}

impl<T: Operation, U> fmt::Debug for SocketOp<'_, T, U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SocketOp")
            .field("op", &self.op)
            .finish_non_exhaustive()
    }
}

/// Moves a whole buffer, one operation at a time, until it is done; what
/// [`WriteAll`] and [`ReadExact`] share.
///
/// Each operation asks the kernel to carry on through short transfers
/// itself (`MSG_WAITALL`). What it still leaves undone goes in another
/// operation, which also makes the kernel report why it stopped.
struct Transfer<T: Operation, B> {
    driver: Driver,
    fd: Fd,
    state: TransferState<T, B>,
    timeout: Option<Duration>,
    /// When the time [`Transfer::timeout`] allows is up, from the first
    /// operation on.
    deadline: Option<Instant>,
}

enum TransferState<T: Operation, B> {
    Start(B),
    Moving { op: Op<T>, done: usize },
    Finished,
}

impl<T: Operation, B> Transfer<T, B> {
    fn new(driver: Driver, fd: Fd, buf: B) -> Self {
        Self {
            driver,
            fd,
            state: TransferState::Start(buf),
            timeout: None,
            deadline: None,
        }
    }

    /// Starts the operation `make` makes to move the buffer on from `done`,
    /// with the time left, if the transfer has a timeout. When none is left
    /// the linked timeout fires at once, unless the kernel completes the
    /// operation straight away.
    fn start(&mut self, buf: B, done: usize, make: impl FnOnce(Fd, B) -> T) {
        let mut op = self.driver.op(make(self.fd, buf));
        if let Some(timeout) = self.timeout {
            let now = Instant::now();
            let left = match self.deadline {
                Some(deadline) => deadline.saturating_duration_since(now),
                None => {
                    // A deadline past what an `Instant` holds is never.
                    self.deadline = now.checked_add(timeout);
                    timeout
                }
            };
            op = op.timeout(left);
        }
        self.state = TransferState::Moving { op, done };
    }
}

/// Writes a whole buffer to a stream socket. Made by the sockets'
/// `write_all`.
///
/// The output is the buffer, and `Ok(())` once all of its initialized bytes
/// are sent, or the error that stopped the write.
#[must_use = "a write does nothing until polled"]
pub struct WriteAll<'a, B: IoBuf> {
    transfer: Transfer<Send<Slice<B>>, B>,
    _socket: PhantomData<&'a ()>,
}

impl<B: IoBuf> WriteAll<'_, B> {
    /// Fails the write with [`io::ErrorKind::TimedOut`] if it has not
    /// finished `duration` after it starts, cancelling the send in flight
    /// with a linked timeout; see [`SocketOp::timeout`].
    pub fn timeout(mut self, duration: Duration) -> Self {
        self.transfer.timeout = Some(duration);
        self
    }
}

impl<B: IoBuf> Future for WriteAll<'_, B> {
    type Output = BufResult<(), B>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let transfer = &mut self.get_mut().transfer;
        let send = |fd, buf: B, done| Send::new(fd, buf.slice(done..)).flags(libc::MSG_WAITALL);
        loop {
            match mem::replace(&mut transfer.state, TransferState::Finished) {
                TransferState::Start(buf) if buf.bytes_init() == 0 => {
                    return Poll::Ready((Ok(()), buf));
                }
                TransferState::Start(buf) => transfer.start(buf, 0, |fd, buf| send(fd, buf, 0)),
                TransferState::Moving { mut op, done } => {
                    let Poll::Ready((sent, slice)) = Pin::new(&mut op).poll(cx) else {
                        transfer.state = TransferState::Moving { op, done };
                        return Poll::Pending;
                    };
                    let buf = slice.into_inner();
                    let done = match sent {
                        Ok(0) => {
                            let err = io::ErrorKind::WriteZero.into();
                            return Poll::Ready((Err(err), buf));
                        }
                        Ok(sent) => done + sent,
                        Err(err) => return Poll::Ready((Err(err), buf)),
                    };
                    if done >= buf.bytes_init() {
                        return Poll::Ready((Ok(()), buf));
                    }
                    transfer.start(buf, done, |fd, buf| send(fd, buf, done));
                }
                TransferState::Finished => panic!("`WriteAll` polled after it completed"),
            }
        }
    }
}

// A transfer never pins its buffer, whose memory, by the contract of
// `IoBuf`, is not inside it.
impl<B: IoBuf> Unpin for WriteAll<'_, B> {}

impl<B: IoBuf> fmt::Debug for WriteAll<'_, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteAll").finish_non_exhaustive()
    }
}

/// Fills a whole buffer from a stream socket. Made by the sockets'
/// `read_exact`.
///
/// The read fills the buffer from its start, up to its
/// [`bytes_total`](IoBuf::bytes_total). The output is the buffer, and
/// `Ok(())` once it is full, or the error that stopped the read:
/// [`io::ErrorKind::UnexpectedEof`] if the peer shut down first. What
/// arrived is marked initialized either way.
#[must_use = "a read does nothing until polled"]
pub struct ReadExact<'a, B: IoBufMut> {
    transfer: Transfer<Recv<Slice<B>>, B>,
    _socket: PhantomData<&'a ()>,
}

impl<B: IoBufMut> ReadExact<'_, B> {
    /// Fails the read with [`io::ErrorKind::TimedOut`] if it has not
    /// finished `duration` after it starts; see [`WriteAll::timeout`].
    pub fn timeout(mut self, duration: Duration) -> Self {
        self.transfer.timeout = Some(duration);
        self
    }
}

impl<B: IoBufMut> Future for ReadExact<'_, B> {
    type Output = BufResult<(), B>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let transfer = &mut self.get_mut().transfer;
        let recv = |fd, buf: B, done| Recv::new(fd, buf.slice(done..)).flags(libc::MSG_WAITALL);
        loop {
            match mem::replace(&mut transfer.state, TransferState::Finished) {
                TransferState::Start(buf) if buf.bytes_total() == 0 => {
                    return Poll::Ready((Ok(()), buf));
                }
                TransferState::Start(buf) => transfer.start(buf, 0, |fd, buf| recv(fd, buf, 0)),
                TransferState::Moving { mut op, done } => {
                    let Poll::Ready((received, slice)) = Pin::new(&mut op).poll(cx) else {
                        transfer.state = TransferState::Moving { op, done };
                        return Poll::Pending;
                    };
                    let buf = slice.into_inner();
                    let done = match received {
                        Ok(0) => {
                            let err = io::ErrorKind::UnexpectedEof.into();
                            return Poll::Ready((Err(err), buf));
                        }
                        Ok(received) => done + received,
                        Err(err) => return Poll::Ready((Err(err), buf)),
                    };
                    if done >= buf.bytes_total() {
                        return Poll::Ready((Ok(()), buf));
                    }
                    transfer.start(buf, done, |fd, buf| recv(fd, buf, done));
                }
                TransferState::Finished => panic!("`ReadExact` polled after it completed"),
            }
        }
    }
}

// As for `WriteAll`.
impl<B: IoBufMut> Unpin for ReadExact<'_, B> {}

impl<B: IoBufMut> fmt::Debug for ReadExact<'_, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadExact").finish_non_exhaustive()
    }
}

/// A zero-copy send on a socket, which it borrows. Made by the sockets'
/// `send_zc`; see [`ops::SendZc`].
#[must_use = "a send does nothing until polled"]
pub struct SendZc<'a, B: IoBuf> {
    send: ops::SendZc<B>,
    _socket: PhantomData<&'a ()>,
}

impl<B: IoBuf> SendZc<'_, B> {
    pub(crate) fn new(send: ops::SendZc<B>) -> Self {
        Self {
            send,
            _socket: PhantomData,
        }
    }
}

impl<B: IoBuf> Future for SendZc<'_, B> {
    /// The number of bytes sent, and the buffer.
    type Output = BufResult<usize, B>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().send).poll(cx)
    }
}

impl<B: IoBuf> fmt::Debug for SendZc<'_, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendZc").finish_non_exhaustive()
    }
}

/// The data arriving on a socket, received into a [`BufPool`] by a
/// multishot receive that the stream keeps armed. Made by the sockets'
/// `recv_stream`; see [`ops::RecvStream`].
///
/// Each item is one receive's data, as a bundle of leases on buffers of the
/// pool: as many as the data waiting fills. The stream ends when the peer
/// shuts down, or after yielding an error.
#[derive(Debug)]
pub struct RecvStream<'a> {
    stream: ops::RecvStream,
    _socket: PhantomData<&'a ()>,
}

impl RecvStream<'_> {
    /// The next data, or `None` once the peer has shut down or an error has
    /// been yielded.
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<BufBundle>>> {
        self.stream.poll_next(cx)
    }

    /// The next data; see [`poll_next`](RecvStream::poll_next).
    pub async fn next(&mut self) -> Option<io::Result<BufBundle>> {
        poll_fn(|cx| self.poll_next(cx)).await
    }
}
