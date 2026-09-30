//! What the socket types share: their descriptors, and the futures of their
//! operations.

use std::fmt;
use std::future::{Future, poll_fn};
use std::io;
use std::marker::PhantomData;
use std::mem;
use std::net;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::pin::Pin;
use std::ptr;
use std::task::{Context, Poll, ready};
use std::time::{Duration, Instant};

use ruio_driver::buf::{BufBundle, BufPool, BufResult, IoBuf, IoBufMut, IoBufs, Slice};
use ruio_driver::ops::{self, Recv, Send, SendVectored, Shutdown};
use ruio_driver::{Completion, Descriptor, DirectFd, Driver, Fd, Op, Operation, SentFd, SockAddr};

use std::rc::Rc;

use super::behind::{Behind, WriteBehind};

/// Waits for the writes behind the stream, if the write has a gate still:
/// ready once none is in flight, with the error one failed with. The gate is
/// gone then, as the write goes ahead or fails.
fn poll_gate(gate: &mut Option<&Behind>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    let Some(behind) = *gate else {
        return Poll::Ready(Ok(()));
    };
    let result = ready!(behind.poll_done(cx));
    *gate = None;
    Poll::Ready(result)
}

/// Makes a type `!Send` and `!Sync`: sockets and files belong to the thread
/// whose ring their I/O runs on. See DESIGN.md on keeping work stealing
/// possible.
pub(crate) type ThreadBound = PhantomData<*const ()>;

/// A socket of `kind`, such as `SOCK_STREAM`, bound to `addr`, or to the
/// first address it resolves to that works, with `SO_REUSEPORT`: sockets
/// that all have it can bind one address, and the kernel spreads what
/// arrives among them. A stream socket has `SO_REUSEADDR` too, as the
/// standard library's listeners do.
///
/// # Errors
///
/// The error of the last address tried, or `InvalidInput` if `addr`
/// resolves to no address.
pub(crate) fn bind_reuseport(
    addr: impl net::ToSocketAddrs,
    kind: libc::c_int,
) -> io::Result<OwnedFd> {
    let mut last = None;
    for addr in addr.to_socket_addrs()? {
        match bind_reuseport_to(addr, kind) {
            Ok(socket) => return Ok(socket),
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

fn bind_reuseport_to(addr: net::SocketAddr, kind: libc::c_int) -> io::Result<OwnedFd> {
    let domain = if addr.is_ipv4() {
        libc::AF_INET
    } else {
        libc::AF_INET6
    };
    // SAFETY: `socket` touches no memory.
    let fd = unsafe { libc::socket(domain, kind | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the descriptor is new, and nothing else owns it.
    let socket = unsafe { OwnedFd::from_raw_fd(fd) };
    if kind == libc::SOCK_STREAM {
        enable(&socket, libc::SO_REUSEADDR)?;
    }
    enable(&socket, libc::SO_REUSEPORT)?;
    let addr = SockAddr::from(addr);
    let len = addr.len() as libc::socklen_t;
    // SAFETY: `bind` reads the address's `len` bytes, which it has.
    if unsafe { libc::bind(socket.as_raw_fd(), addr.as_ptr(), len) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(socket)
}

/// Turns on `option`, a socket option at `SOL_SOCKET`.
fn enable(socket: &OwnedFd, option: libc::c_int) -> io::Result<()> {
    let on: libc::c_int = 1;
    let len = size_of::<libc::c_int>() as libc::socklen_t;
    let value = ptr::from_ref(&on).cast();
    // SAFETY: `setsockopt` reads the `c_int` it is given, and nothing else.
    let set = unsafe { libc::setsockopt(socket.as_raw_fd(), libc::SOL_SOCKET, option, value, len) };
    if set == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// A socket's or a file's descriptor: a plain one, as the standard library's
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

impl<S> Sock<S> {
    /// Sends the socket to core `core` of the thread's run: a plain
    /// descriptor as it is, and a direct one to that core's ring.
    ///
    /// # Errors
    ///
    /// Hands the socket back with what moving a direct descriptor reports.
    ///
    /// # Panics
    ///
    /// Outside a run of several cores, or if it has no core `core`.
    pub(crate) async fn send_to(self, core: usize) -> Result<Sent<S>, (io::Error, Self)> {
        let handle = crate::handle_of(core);
        match self {
            Sock::Plain(socket) => Ok(Sent::Plain(socket)),
            Sock::Direct(fd) => match fd.send_to(handle.ring()).await {
                Ok(sent) => Ok(Sent::Direct(sent)),
                Err((err, fd)) => Err((err, Sock::Direct(fd))),
            },
        }
    }
}

/// A socket on its way to another core. A direct descriptor is in that
/// core's ring's file table, so only that core can take it.
#[derive(Debug)]
pub(crate) enum Sent<S> {
    Plain(S),
    Direct(SentFd),
}

impl<S> Sent<S> {
    /// The socket, on the core it was sent to.
    ///
    /// # Panics
    ///
    /// For a direct descriptor, on any other core.
    pub(crate) fn receive(self) -> Sock<S> {
        match self {
            Sent::Plain(socket) => Sock::Plain(socket),
            Sent::Direct(sent) => match sent.claim() {
                Ok(fd) => Sock::Direct(fd),
                Err(_) => panic!("a direct descriptor is received on the core it was sent to"),
            },
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
    pub(crate) fn target(&self) -> (Driver, Fd) {
        match self {
            Sock::Plain(socket) => (crate::driver(), Fd::Plain(socket.as_raw_fd())),
            Sock::Direct(fd) => {
                // A direct descriptor almost always belongs to the thread's
                // own ring, which takes no search to find.
                let current = crate::driver();
                if fd.belongs_to(&current) {
                    return (current, fd.fd());
                }
                match fd.driver() {
                    Some(driver) => (driver, fd.fd()),
                    // The ring is gone, and the file with it. No table has
                    // this slot, so the kernel fails the operation with
                    // `EBADF`.
                    None => (current, Fd::Direct(u32::MAX)),
                }
            }
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
            gate: None,
        }
    }

    pub(crate) fn write_all_vectored<V: IoBufs>(&self, bufs: V) -> WriteAllVectored<'_, V> {
        let (driver, fd) = self.target();
        WriteAllVectored {
            transfer: Transfer::new(driver, fd, bufs),
            gate: None,
        }
    }

    pub(crate) fn send_zc<B: IoBuf>(&self, buf: B) -> SendZc<'_, B> {
        let (driver, fd) = self.target();
        SendZc::new(ops::SendZc::new(&driver, fd, buf).flags(libc::MSG_WAITALL))
    }

    pub(crate) fn write_behind<'a, V: IoBufs>(
        &self,
        behind: &'a Rc<Behind>,
        bufs: V,
    ) -> WriteBehind<'a, V> {
        WriteBehind::new(behind, self.target(), bufs)
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
    /// Taken if the operation fails before it is submitted.
    op: Option<Op<T>>,
    output: fn(T::Output) -> U,
    /// The writes behind the stream, which a write waits for before it is
    /// submitted.
    gate: Option<&'a Behind>,
}

impl<'a, T: Operation, U> SocketOp<'a, T, U> {
    pub(crate) fn new(op: Op<T>, output: fn(T::Output) -> U) -> Self {
        Self {
            op: Some(op),
            output,
            gate: None,
        }
    }

    /// Submits the operation only once the writes behind the stream have
    /// finished, and fails it with the error one failed with.
    pub(crate) fn behind(mut self, gate: &'a Behind) -> Self {
        self.gate = Some(gate);
        self
    }

    /// Cancels the operation if the kernel has not completed it `duration`
    /// after it is submitted, with a linked timeout; see [`Op::timeout`].
    ///
    /// The operation then fails with [`io::ErrorKind::TimedOut`], and a
    /// buffer it holds comes back, as with any other error.
    pub fn timeout(self, duration: Duration) -> Self {
        Self {
            op: self.op.map(|op| op.timeout(duration)),
            ..self
        }
    }
}

impl<T: Operation, U> Future for SocketOp<'_, T, U> {
    type Output = U;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<U> {
        let this = self.get_mut();
        if let Err(err) = ready!(poll_gate(&mut this.gate, cx)) {
            let operation = this
                .op
                .take()
                .and_then(Op::into_inner)
                .expect("`SocketOp` polled after it completed");
            let output = operation.complete(Completion::from_error(&err));
            return Poll::Ready((this.output)(output));
        }
        let op = this
            .op
            .as_mut()
            .expect("`SocketOp` polled after it completed");
        Pin::new(op).poll(cx).map(this.output)
    }
}

impl<T: Operation, U> fmt::Debug for SocketOp<'_, T, U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SocketOp")
            .field("op", &self.op)
            .field("gate", &self.gate)
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

    /// The buffer of a transfer that has not started, which it gives up: a
    /// write whose gate failed it.
    fn unstarted(&mut self) -> B {
        match mem::replace(&mut self.state, TransferState::Finished) {
            TransferState::Start(buf) => buf,
            _ => unreachable!("a transfer starts only once its gate opens"),
        }
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
    /// The writes behind the stream, which the write waits for.
    gate: Option<&'a Behind>,
}

impl<'a, B: IoBuf> WriteAll<'a, B> {
    /// Starts the write only once the writes behind the stream have
    /// finished, and fails it with the error one failed with.
    pub(crate) fn behind(mut self, gate: &'a Behind) -> Self {
        self.gate = Some(gate);
        self
    }

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
        let this = self.get_mut();
        if let Err(err) = ready!(poll_gate(&mut this.gate, cx)) {
            return Poll::Ready((Err(err), this.transfer.unstarted()));
        }
        let transfer = &mut this.transfer;
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

/// Writes several buffers to a stream socket, one after another, sending as
/// many of them at once as the kernel takes. Made by the streams'
/// `write_all_vectored`, as for a bundle of received data.
///
/// The output is the buffers, and `Ok(())` once all their initialized bytes
/// are sent, or the error that stopped the write.
#[must_use = "a write does nothing until polled"]
pub struct WriteAllVectored<'a, V: IoBufs> {
    transfer: Transfer<SendVectored<V>, V>,
    /// The writes behind the stream, which the write waits for.
    gate: Option<&'a Behind>,
}

impl<'a, V: IoBufs> WriteAllVectored<'a, V> {
    /// As for [`WriteAll`].
    pub(crate) fn behind(mut self, gate: &'a Behind) -> Self {
        self.gate = Some(gate);
        self
    }

    /// Fails the write with [`io::ErrorKind::TimedOut`] if it has not
    /// finished `duration` after it starts; see [`WriteAll::timeout`].
    pub fn timeout(mut self, duration: Duration) -> Self {
        self.transfer.timeout = Some(duration);
        self
    }
}

impl<V: IoBufs> Future for WriteAllVectored<'_, V> {
    type Output = BufResult<(), V>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Err(err) = ready!(poll_gate(&mut this.gate, cx)) {
            return Poll::Ready((Err(err), this.transfer.unstarted()));
        }
        let transfer = &mut this.transfer;
        let send = |fd, bufs, done| {
            SendVectored::new(fd, bufs)
                .skip(done)
                .flags(libc::MSG_WAITALL)
        };
        loop {
            match mem::replace(&mut transfer.state, TransferState::Finished) {
                TransferState::Start(bufs) if bufs.bytes_init() == 0 => {
                    return Poll::Ready((Ok(()), bufs));
                }
                TransferState::Start(bufs) => transfer.start(bufs, 0, |fd, bufs| send(fd, bufs, 0)),
                TransferState::Moving { mut op, done } => {
                    let Poll::Ready((sent, bufs)) = Pin::new(&mut op).poll(cx) else {
                        transfer.state = TransferState::Moving { op, done };
                        return Poll::Pending;
                    };
                    let done = match sent {
                        Ok(0) => {
                            let err = io::ErrorKind::WriteZero.into();
                            return Poll::Ready((Err(err), bufs));
                        }
                        Ok(sent) => done + sent,
                        Err(err) => return Poll::Ready((Err(err), bufs)),
                    };
                    if done >= bufs.bytes_init() {
                        return Poll::Ready((Ok(()), bufs));
                    }
                    transfer.start(bufs, done, |fd, bufs| send(fd, bufs, done));
                }
                TransferState::Finished => {
                    panic!("`WriteAllVectored` polled after it completed")
                }
            }
        }
    }
}

// As for `WriteAll`.
impl<V: IoBufs> Unpin for WriteAllVectored<'_, V> {}

impl<V: IoBufs> fmt::Debug for WriteAllVectored<'_, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteAllVectored").finish_non_exhaustive()
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
    /// Taken if the send fails before it is submitted.
    send: Option<ops::SendZc<B>>,
    /// The writes behind the stream, which the send waits for.
    gate: Option<&'a Behind>,
}

impl<'a, B: IoBuf> SendZc<'a, B> {
    pub(crate) fn new(send: ops::SendZc<B>) -> Self {
        Self {
            send: Some(send),
            gate: None,
        }
    }

    /// As for [`WriteAll`].
    pub(crate) fn behind(mut self, gate: &'a Behind) -> Self {
        self.gate = Some(gate);
        self
    }
}

impl<B: IoBuf> Future for SendZc<'_, B> {
    /// The number of bytes sent, and the buffer.
    type Output = BufResult<usize, B>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Err(err) = ready!(poll_gate(&mut this.gate, cx)) {
            let buf = this
                .send
                .take()
                .and_then(ops::SendZc::into_inner)
                .expect("`SendZc` polled after it completed");
            return Poll::Ready((Err(err), buf));
        }
        let send = this
            .send
            .as_mut()
            .expect("`SendZc` polled after it completed");
        Pin::new(send).poll(cx)
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

    /// The next data if it has arrived already, or `None`, without waiting;
    /// see [`ops::RecvStream::next_ready`]. On a stream socket, appending it
    /// to the data taken before gathers everything that has arrived:
    ///
    /// ```no_run
    /// # async fn echo(stream: ruio::net::TcpStream, pool: ruio::buf::BufPool) {
    /// let mut received = stream.recv_stream(&pool);
    /// while let Some(Ok(mut data)) = received.next().await {
    ///     while let Some(Ok(more)) = received.next_ready() {
    ///         data.append(more);
    ///     }
    ///     if stream.write_all_vectored(data).await.0.is_err() {
    ///         break;
    ///     }
    /// }
    /// # }
    /// ```
    pub fn next_ready(&mut self) -> Option<io::Result<BufBundle>> {
        self.stream.next_ready()
    }
}
