//! Socket operations.

use std::future::{Future, poll_fn};
use std::io;
use std::mem;
use std::net;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::ptr;
use std::task::{Context, Poll, ready};

use ruio_sys as sys;

use super::{Install, Rearming, complete_read};
use crate::buf::{BufLease, BufPool, BufResult, IoBuf, IoBufMut, IoBufs};
use crate::{Completion, Descriptor, Driver, Fd, MultiOp, Multishot, Operation, Probe, SockAddr};

impl Install {
    /// The entry's `accept_flags`: the kernel refuses close-on-exec for a
    /// direct descriptor, which has nothing to close.
    fn accept_flags(self) -> u32 {
        match self {
            Install::Plain => libc::SOCK_CLOEXEC.cast_unsigned(),
            Install::Direct(_) => 0,
        }
    }
}

/// Makes a socket, like `socket(2)`.
///
/// The socket is a plain descriptor, closed on `exec`, unless
/// [`direct`](Socket::direct) asks for a direct descriptor.
#[derive(Debug)]
pub struct Socket {
    domain: i32,
    ty: i32,
    protocol: i32,
    install: Install,
}

impl Socket {
    /// A socket of `domain`, such as `AF_INET`, and type, such as
    /// `SOCK_STREAM`, for `protocol`, or the type's default protocol with 0.
    pub fn new(domain: i32, ty: i32, protocol: i32) -> Self {
        Self {
            domain,
            ty,
            protocol,
            install: Install::Plain,
        }
    }

    /// Makes the socket a direct descriptor, in a free slot of the
    /// registered file table of `driver`'s ring. The operation must run on
    /// that ring: on another, the descriptor would name the wrong file.
    ///
    /// The operation fails with `ENFILE` if the table has no free slot.
    pub fn direct(mut self, driver: &Driver) -> Self {
        self.install = Install::Direct(driver.id());
        self
    }
}

// SAFETY: the entry points to no memory, and a socket request posts one
// completion.
unsafe impl Operation for Socket {
    type Output = io::Result<Descriptor>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_SOCKET;
        sqe.fd = self.domain;
        // The kernel refuses close-on-exec for a direct descriptor, which
        // has nothing to close.
        let ty = match self.install {
            Install::Plain => self.ty | libc::SOCK_CLOEXEC,
            Install::Direct(_) => self.ty & !libc::SOCK_CLOEXEC,
        };
        sqe.off = u64::from(ty.cast_unsigned());
        sqe.len = self.protocol.cast_unsigned();
        sqe.file_index = self.install.file_index();
    }

    fn complete(self, completion: Completion) -> Self::Output {
        completion.result().map(|res| self.install.descriptor(res))
    }
}

/// Connects a socket to an address, like `connect(2)`.
#[derive(Debug)]
pub struct Connect {
    fd: Fd,
    // Boxed so that the entry points to memory that does not move with the
    // operation.
    addr: Box<SockAddr>,
}

impl Connect {
    /// Connects `fd` to `addr`.
    pub fn new(fd: impl Into<Fd>, addr: impl Into<SockAddr>) -> Self {
        Self {
            fd: fd.into(),
            addr: Box::new(addr.into()),
        }
    }
}

// SAFETY: the entry points only to the boxed address, which the operation
// owns and which stays put when the operation moves. A connect request posts
// one completion.
unsafe impl Operation for Connect {
    type Output = io::Result<()>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_CONNECT;
        self.fd.apply(sqe);
        sqe.addr = self.addr.as_ptr().expose_provenance() as u64;
        // The length goes in `addr2` by value.
        sqe.off = self.addr.len() as u64;
    }

    fn complete(self, completion: Completion) -> Self::Output {
        completion.result().map(drop)
    }
}

/// Accepts a connection on a listening socket, like `accept4(2)`, and
/// reports the peer's address.
///
/// The connection is a plain descriptor, closed on `exec`, unless
/// [`direct`](Accept::direct) asks for a direct descriptor.
#[derive(Debug)]
pub struct Accept {
    fd: Fd,
    // Boxed, as for `Connect`; the kernel writes the peer's address here.
    peer: Box<SockAddr>,
    install: Install,
}

impl Accept {
    /// Accepts a connection on `fd`.
    pub fn new(fd: impl Into<Fd>) -> Self {
        Self {
            fd: fd.into(),
            peer: Box::new(SockAddr::empty()),
            install: Install::Plain,
        }
    }

    /// Makes the connection a direct descriptor, in a free slot of the
    /// registered file table of `driver`'s ring, where the operation must
    /// run; see [`Socket::direct`].
    ///
    /// Without a free slot, the kernel closes the connection it accepted
    /// and the operation fails with `ENFILE`.
    pub fn direct(mut self, driver: &Driver) -> Self {
        self.install = Install::Direct(driver.id());
        self
    }
}

// SAFETY: the entry points only to the boxed address and its length, which
// the operation owns and which stay put when it moves. An accept request
// without IORING_ACCEPT_MULTISHOT posts one completion.
unsafe impl Operation for Accept {
    /// The connection, and the peer's address.
    type Output = io::Result<(Descriptor, SockAddr)>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_ACCEPT;
        self.fd.apply(sqe);
        let (addr, len) = self.peer.as_mut_ptrs();
        sqe.addr = addr.expose_provenance() as u64;
        sqe.off = len.expose_provenance() as u64;
        sqe.op_flags = self.install.accept_flags();
        sqe.file_index = self.install.file_index();
    }

    fn complete(self, completion: Completion) -> Self::Output {
        let descriptor = self.install.descriptor(completion.result()?);
        Ok((descriptor, *self.peer))
    }
}

/// Accepts connections on a listening socket again and again as they
/// arrive: a multishot `accept4(2)` (`IORING_ACCEPT_MULTISHOT`).
///
/// Each item is a connection, a plain descriptor closed on `exec` unless
/// [`direct`](AcceptMulti::direct) asks for direct descriptors. Peers'
/// addresses are not reported: each connection would overwrite the last
/// one's before it could be read. [`AcceptStream`] keeps the request armed.
#[derive(Debug)]
pub struct AcceptMulti {
    fd: Fd,
    install: Install,
}

impl AcceptMulti {
    /// Accepts connections on `fd`.
    pub fn new(fd: impl Into<Fd>) -> Self {
        Self {
            fd: fd.into(),
            install: Install::Plain,
        }
    }

    /// Makes each connection a direct descriptor, in a free slot of the
    /// registered file table of `driver`'s ring, where the request must run;
    /// see [`Socket::direct`].
    ///
    /// Once the table is full, the kernel closes the connection it could not
    /// put in it, and ends the request with `ENFILE`.
    pub fn direct(mut self, driver: &Driver) -> Self {
        self.install = Install::Direct(driver.id());
        self
    }
}

// SAFETY: the entry points to no memory, since it asks for no addresses,
// and a multishot accept's completions carry IORING_CQE_F_MORE until the
// last.
unsafe impl Multishot for AcceptMulti {
    type Item = io::Result<Descriptor>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_ACCEPT;
        self.fd.apply(sqe);
        sqe.ioprio = sys::IORING_ACCEPT_MULTISHOT;
        sqe.op_flags = self.install.accept_flags();
        sqe.file_index = self.install.file_index();
    }

    fn item(&mut self, completion: Completion) -> Self::Item {
        completion.result().map(|res| self.install.descriptor(res))
    }
}

/// The connections arriving on a listening socket, accepted by a multishot
/// accept that the stream keeps armed.
///
/// The kernel ends a multishot accept now and then for reasons of its own,
/// such as a full completion queue; the stream re-arms it at once, and the
/// connections that arrived meanwhile wait in the listener's backlog. An
/// error ends the request too: the stream yields it, and re-arms the request
/// when polled again, so that a server can carry on past errors such as
/// `EMFILE`, as it would with `accept(2)`. The stream never ends.
#[derive(Debug)]
pub struct AcceptStream {
    stream: MultiOp<AcceptMulti>,
}

impl AcceptStream {
    /// A stream of the connections `request` accepts.
    pub fn new(driver: &Driver, request: AcceptMulti) -> Self {
        Self {
            stream: driver.stream(request),
        }
    }

    /// The next connection, or the error that ended the request.
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Descriptor>> {
        loop {
            match ready!(self.stream.poll_next(cx)) {
                Some(accepted) => return Poll::Ready(accepted),
                None => self.stream.rearm(),
            }
        }
    }

    /// The next connection; see [`poll_next`](AcceptStream::poll_next).
    pub async fn next(&mut self) -> io::Result<Descriptor> {
        poll_fn(|cx| self.poll_next(cx)).await
    }
}

/// Sends a buffer's initialized bytes on a connected socket, like `send(2)`.
///
/// The buffer comes back with the result. A send never raises `SIGPIPE`.
#[derive(Debug)]
pub struct Send<B> {
    fd: Fd,
    buf: B,
    flags: i32,
}

impl<B: IoBuf> Send<B> {
    /// Sends `buf` on `fd`.
    pub fn new(fd: impl Into<Fd>, buf: B) -> Self {
        Self {
            fd: fd.into(),
            buf,
            flags: 0,
        }
    }

    /// Sets the `send(2)` flags. With `MSG_WAITALL` the kernel keeps
    /// sending on a stream socket until it has sent everything, or fails,
    /// rather than stopping short.
    pub fn flags(mut self, flags: i32) -> Self {
        self.flags = flags;
        self
    }
}

// SAFETY: the entry points into the buffer, which `IoBuf` keeps valid, in
// place and unchanged while the operation holds it. A send posts one
// completion.
unsafe impl<B: IoBuf> Operation for Send<B> {
    /// The number of bytes sent, and the buffer.
    type Output = BufResult<usize, B>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        prepare_send(sqe, self.fd, &self.buf, self.flags);
    }

    fn complete(self, completion: Completion) -> Self::Output {
        (completion.result().map(|sent| sent as usize), self.buf)
    }
}

fn prepare_send<B: IoBuf>(sqe: &mut sys::io_uring_sqe, fd: Fd, buf: &B, flags: i32) {
    sqe.opcode = sys::IORING_OP_SEND;
    fd.apply(sqe);
    sqe.addr = buf.stable_ptr().expose_provenance() as u64;
    sqe.len = u32::try_from(buf.bytes_init()).unwrap_or(u32::MAX);
    sqe.op_flags = flags.cast_unsigned();
}

/// Sends several buffers' initialized bytes, one after another, on a
/// connected socket, like `sendmsg(2)` with a vector of them: a stream
/// socket sends them as one stream of bytes.
///
/// A single buffer, or what of a single one is left after
/// [`skip`](SendVectored::skip) and [`limit`](SendVectored::limit), goes as
/// a plain [`Send`]. At most `IOV_MAX` (1024) buffers go in one send; the
/// rest wait for another. The buffers come back with the result.
pub struct SendVectored<V> {
    fd: Fd,
    bufs: V,
    skip: usize,
    /// The most bytes to send, after those skipped.
    limit: usize,
    flags: i32,
    /// The vector the kernel reads, filled only for a send of more than one
    /// buffer. Its elements live on the heap, and stay put when the
    /// operation moves.
    iovecs: Vec<libc::iovec>,
    /// The header naming the vector for a `sendmsg`, on a kernel without
    /// vectorized sends. Boxed, as for `Connect`.
    header: Option<Box<libc::msghdr>>,
}

impl<V: std::fmt::Debug> std::fmt::Debug for SendVectored<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendVectored")
            .field("fd", &self.fd)
            .field("bufs", &self.bufs)
            .field("skip", &self.skip)
            .field("limit", &self.limit)
            .field("flags", &self.flags)
            .finish_non_exhaustive()
    }
}

/// The most buffers one `sendmsg` takes.
const IOV_MAX: usize = 1024;

impl<V: IoBufs> SendVectored<V> {
    /// Sends `bufs` on `fd`.
    pub fn new(fd: impl Into<Fd>, bufs: V) -> Self {
        Self {
            fd: fd.into(),
            bufs,
            skip: 0,
            limit: usize::MAX,
            flags: 0,
            iovecs: Vec::new(),
            header: None,
        }
    }

    /// Leaves out the first `bytes` bytes, sent before.
    pub fn skip(mut self, bytes: usize) -> Self {
        self.skip = bytes;
        self
    }

    /// Sends at most `bytes` bytes, after those skipped, leaving the rest
    /// for another send.
    pub fn limit(mut self, bytes: usize) -> Self {
        self.limit = bytes;
        self
    }

    /// Sets the `sendmsg(2)` flags; see [`Send::flags`].
    pub fn flags(mut self, flags: i32) -> Self {
        self.flags = flags;
        self
    }
}

// SAFETY: the entry points into the buffers, which `IoBufs` keeps valid, in
// place and unchanged while the operation holds them, or to the vector's heap
// elements, or to the boxed header, which points to them; the operation owns
// all of them, and they stay put when it moves. A send posts one completion.
unsafe impl<V: IoBufs> Operation for SendVectored<V> {
    /// The number of bytes sent, and the buffers.
    type Output = BufResult<usize, V>;

    /// A send of more than one buffer names a vector of them in the entry
    /// itself (`IORING_SEND_VECTORIZED`, Linux 6.17), which the kernel
    /// imports as it takes the entry, as it does a `sendmsg`'s vector, but
    /// without a header to copy in and check first. A kernel without that
    /// gets the `sendmsg`.
    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        let bufs = &self.bufs;
        let count = bufs.buf_count();
        // Where the data left starts: a buffer, and an offset in it.
        let mut first = 0;
        let mut offset = self.skip;
        while first < count && offset >= bufs.buf(first).1 {
            offset -= bufs.buf(first).1;
            first += 1;
        }
        self.fd.apply(sqe);
        sqe.op_flags = self.flags.cast_unsigned();
        let (start, len) = match first {
            first if first < count => bufs.buf(first),
            _ => (ptr::null(), 0),
        };
        let len = (len - offset.min(len)).min(self.limit);
        if first + 1 >= count || len == self.limit {
            sqe.opcode = sys::IORING_OP_SEND;
            sqe.addr = start.wrapping_add(offset).expose_provenance() as u64;
            sqe.len = u32::try_from(len).unwrap_or(u32::MAX);
            return;
        }
        self.iovecs.clear();
        self.iovecs.reserve((count - first).min(IOV_MAX));
        let mut left = self.limit;
        for index in first..count.min(first + IOV_MAX) {
            let (start, len) = bufs.buf(index);
            let skip = if index == first { offset } else { 0 };
            let len = (len - skip).min(left);
            self.iovecs.push(libc::iovec {
                iov_base: start.wrapping_add(skip).cast_mut().cast(),
                iov_len: len,
            });
            left -= len;
            if left == 0 {
                break;
            }
        }
        let vectorized = Probe::current().is_ok_and(Probe::supports_vectorized_send);
        if vectorized {
            sqe.opcode = sys::IORING_OP_SEND;
            sqe.ioprio = sys::IORING_SEND_VECTORIZED;
            sqe.addr = self.iovecs.as_ptr().expose_provenance() as u64;
            sqe.len = u32::try_from(self.iovecs.len()).unwrap_or(u32::MAX);
            return;
        }
        let header = self.header.get_or_insert_with(|| {
            // SAFETY: all zeroes is a valid `msghdr`, with null pointers.
            Box::new(unsafe { mem::zeroed() })
        });
        header.msg_iov = self.iovecs.as_mut_ptr();
        header.msg_iovlen = self.iovecs.len();
        sqe.opcode = sys::IORING_OP_SENDMSG;
        sqe.addr = (&raw const **header).expose_provenance() as u64;
        sqe.len = 1;
    }

    fn complete(self, completion: Completion) -> Self::Output {
        (completion.result().map(|sent| sent as usize), self.bufs)
    }
}

/// Sends a buffer's initialized bytes to an address, like `sendto(2)`, on a
/// socket that need not be connected, such as a datagram socket.
///
/// The buffer comes back with the result.
#[derive(Debug)]
pub struct SendTo<B> {
    fd: Fd,
    buf: B,
    // Boxed, as for `Connect`.
    addr: Box<SockAddr>,
    flags: i32,
}

impl<B: IoBuf> SendTo<B> {
    /// Sends `buf` on `fd` to `addr`.
    pub fn new(fd: impl Into<Fd>, buf: B, addr: impl Into<SockAddr>) -> Self {
        Self {
            fd: fd.into(),
            buf,
            addr: Box::new(addr.into()),
            flags: 0,
        }
    }

    /// Sets the `sendto(2)` flags.
    pub fn flags(mut self, flags: i32) -> Self {
        self.flags = flags;
        self
    }
}

// SAFETY: the entry points into the buffer, as for `Send`, and to the boxed
// address, which the operation owns and which stays put when it moves. A
// send posts one completion.
unsafe impl<B: IoBuf> Operation for SendTo<B> {
    /// The number of bytes sent, and the buffer.
    type Output = BufResult<usize, B>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        prepare_send(sqe, self.fd, &self.buf, self.flags);
        set_destination(sqe, &self.addr);
    }

    fn complete(self, completion: Completion) -> Self::Output {
        (completion.result().map(|sent| sent as usize), self.buf)
    }
}

/// Sends a buffer's initialized bytes without copying them into the kernel:
/// a zero-copy `send(2)`, or `sendto(2)` with [`to`](SendZc::to)
/// (`IORING_OP_SEND_ZC`).
///
/// The network stack sends straight from the buffer, so the kernel reports
/// twice: the result, and later a notification that it no longer needs the
/// buffer, which for TCP comes only once the peer has acknowledged the data.
/// The future resolves with the buffer after both. Dropped early, it cancels
/// the send, and the driver keeps the buffer until the notification.
///
/// Only TCP and UDP sockets send zero-copy; others fail with `EOPNOTSUPP`.
/// Pinning the pages and waiting for the notification cost more than
/// copying a small buffer.
#[must_use = "a send does nothing until polled"]
#[derive(Debug)]
pub struct SendZc<B: IoBuf> {
    /// Taken once the future has resolved.
    stream: Option<MultiOp<ZcRequest<B>>>,
}

/// The request a [`SendZc`] runs.
#[derive(Debug)]
struct ZcRequest<B> {
    fd: Fd,
    buf: B,
    // Boxed, as for `Connect`.
    to: Option<Box<SockAddr>>,
    flags: i32,
    /// The result, once reported.
    sent: Option<io::Result<usize>>,
}

impl<B: IoBuf> SendZc<B> {
    /// Sends `buf` on `fd`, on `driver`'s ring.
    pub fn new(driver: &Driver, fd: impl Into<Fd>, buf: B) -> Self {
        let request = ZcRequest {
            fd: fd.into(),
            buf,
            to: None,
            flags: 0,
            sent: None,
        };
        Self {
            stream: Some(driver.stream(request)),
        }
    }

    /// Sends to `addr`, as [`SendTo`] does.
    pub fn to(mut self, addr: impl Into<SockAddr>) -> Self {
        self.request().to = Some(Box::new(addr.into()));
        self
    }

    /// Sets the `send(2)` flags; see [`Send::flags`].
    pub fn flags(mut self, flags: i32) -> Self {
        self.request().flags = flags;
        self
    }

    /// The buffer back, if the send has not been submitted. Otherwise the
    /// send is dropped, which cancels it.
    pub fn into_inner(mut self) -> Option<B> {
        match self.stream.take()?.into_inner() {
            Ok(request) => Some(request.buf),
            Err(stream) => {
                self.stream = Some(stream);
                None
            }
        }
    }

    /// The request, which is not armed before the first poll.
    fn request(&mut self) -> &mut ZcRequest<B> {
        self.stream
            .as_mut()
            .and_then(MultiOp::get_mut)
            .expect("a `SendZc` is set up before it is polled")
    }
}

impl<B: IoBuf> Future for SendZc<B> {
    /// The number of bytes sent, and the buffer.
    type Output = BufResult<usize, B>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let stream = this
            .stream
            .as_mut()
            .expect("`SendZc` polled after it completed");
        // The result, then the notification, which ends the request.
        while ready!(stream.poll_next(cx)).is_some() && stream.is_armed() {}
        let Some(Ok(request)) = this.stream.take().map(MultiOp::into_inner) else {
            unreachable!("the request has ended");
        };
        let sent = request
            .sent
            .unwrap_or_else(|| Err(io::Error::other("the kernel reported no result")));
        Poll::Ready((sent, request.buf))
    }
}

// A `SendZc` never pins its request: the memory the kernel uses is not
// inside it.
impl<B: IoBuf> Unpin for SendZc<B> {}

// SAFETY: the entry points into the buffer, which `IoBuf` keeps valid, in
// place and unchanged while the request holds it, and to the boxed address,
// which the request owns and which stays put when it moves; the driver keeps
// the request until its last completion, the notification, which the kernel
// posts once it is done with the buffer. A zero-copy send posts its result
// with IORING_CQE_F_MORE and then the notification without it, or, if it
// fails before it has a notification to post, just the result, without it.
unsafe impl<B: IoBuf> Multishot for ZcRequest<B> {
    type Item = ();

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        prepare_send(sqe, self.fd, &self.buf, self.flags);
        sqe.opcode = sys::IORING_OP_SEND_ZC;
        if let Some(to) = &self.to {
            set_destination(sqe, to);
        }
    }

    fn item(&mut self, completion: Completion) {
        if completion.flags() & sys::IORING_CQE_F_NOTIF == 0 {
            self.sent = Some(completion.result().map(|sent| sent as usize));
        }
    }
}

/// Makes a send go to `addr`: `addr2`, with the length in `addr_len`, the
/// first half of `file_index`.
fn set_destination(sqe: &mut sys::io_uring_sqe, addr: &SockAddr) {
    sqe.off = addr.as_ptr().expose_provenance() as u64;
    // Every address fits: the largest, `sockaddr_storage`, has 128 bytes.
    sqe.file_index = u32::from(addr.len() as u16);
}

/// Receives from a socket into a buffer, like `recv(2)`.
///
/// The receive fills the buffer from its start, up to
/// [`bytes_total`](IoBuf::bytes_total), and marks the bytes it received as
/// initialized; it receives 0 bytes once the peer has shut down its side of
/// a stream. The buffer comes back with the result.
#[derive(Debug)]
pub struct Recv<B> {
    fd: Fd,
    buf: B,
    flags: i32,
}

impl<B: IoBufMut> Recv<B> {
    /// Receives from `fd` into `buf`.
    pub fn new(fd: impl Into<Fd>, buf: B) -> Self {
        Self {
            fd: fd.into(),
            buf,
            flags: 0,
        }
    }

    /// Sets the `recv(2)` flags. With `MSG_WAITALL` the kernel keeps
    /// receiving on a stream socket until the buffer is full, the peer shuts
    /// down or the receive fails.
    pub fn flags(mut self, flags: i32) -> Self {
        self.flags = flags;
        self
    }
}

// SAFETY: the entry points into the buffer, which `IoBufMut` keeps valid and
// in place while the operation holds it, even as it moves. A receive without
// IORING_RECV_MULTISHOT posts one completion.
unsafe impl<B: IoBufMut> Operation for Recv<B> {
    /// The number of bytes received, and the buffer.
    type Output = BufResult<usize, B>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_RECV;
        self.fd.apply(sqe);
        sqe.addr = self.buf.stable_mut_ptr().expose_provenance() as u64;
        sqe.len = u32::try_from(self.buf.bytes_total()).unwrap_or(u32::MAX);
        sqe.op_flags = self.flags.cast_unsigned();
    }

    fn complete(self, completion: Completion) -> Self::Output {
        complete_read(self.buf, completion)
    }
}

/// Receives from a socket into a buffer, and reports the sender's address,
/// like `recvfrom(2)`, for sockets that need not be connected, such as
/// datagram sockets. The kernel does it with `recvmsg`.
///
/// The buffer is filled as by [`Recv`]; a datagram too large for it is cut
/// short, and the rest of it lost.
#[derive(Debug)]
pub struct RecvFrom<B> {
    fd: Fd,
    buf: B,
    // Boxed, as for `Connect`; the kernel reads the header and writes the
    // address and the header's results.
    msg: Box<RecvMsg>,
    flags: i32,
}

/// What a `recvmsg` request points to.
struct RecvMsg {
    header: libc::msghdr,
    iov: libc::iovec,
    from: SockAddr,
}

impl std::fmt::Debug for RecvMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecvMsg").finish_non_exhaustive()
    }
}

impl<B: IoBufMut> RecvFrom<B> {
    /// Receives from `fd` into `buf`.
    pub fn new(fd: impl Into<Fd>, buf: B) -> Self {
        Self {
            fd: fd.into(),
            buf,
            msg: Box::new(RecvMsg {
                // SAFETY: all zeroes is a valid `msghdr`, with null pointers.
                header: unsafe { mem::zeroed() },
                iov: libc::iovec {
                    iov_base: ptr::null_mut(),
                    iov_len: 0,
                },
                from: SockAddr::empty(),
            }),
            flags: 0,
        }
    }

    /// Sets the `recvmsg(2)` flags.
    pub fn flags(mut self, flags: i32) -> Self {
        self.flags = flags;
        self
    }
}

// SAFETY: the entry points to the boxed header, which points to the boxed
// iovec and address; all of them the operation owns, and they stay put when
// it moves. The iovec points into the buffer, which `IoBufMut` keeps valid
// and in place while the operation holds it. A `recvmsg` request without
// IORING_RECV_MULTISHOT posts one completion.
unsafe impl<B: IoBufMut> Operation for RecvFrom<B> {
    /// The number of bytes received and the sender's address, and the
    /// buffer.
    type Output = BufResult<(usize, SockAddr), B>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        let msg = &mut *self.msg;
        msg.iov = libc::iovec {
            iov_base: self.buf.stable_mut_ptr().cast(),
            iov_len: self.buf.bytes_total(),
        };
        msg.from = SockAddr::empty();
        // The room for the address; the kernel sets it to the address's
        // length.
        msg.header.msg_namelen = msg.from.len() as libc::socklen_t;
        msg.header.msg_name = msg.from.as_mut_ptrs().0.cast();
        msg.header.msg_iov = &raw mut msg.iov;
        msg.header.msg_iovlen = 1;
        sqe.opcode = sys::IORING_OP_RECVMSG;
        self.fd.apply(sqe);
        sqe.addr = (&raw const msg.header).expose_provenance() as u64;
        sqe.len = 1;
        sqe.op_flags = self.flags.cast_unsigned();
    }

    fn complete(mut self, completion: Completion) -> Self::Output {
        let namelen = self.msg.header.msg_namelen;
        let from = &mut self.msg.from;
        from.set_len(namelen);
        let from = *from;
        let (received, buf) = complete_read(self.buf, completion);
        (received.map(|received| (received, from)), buf)
    }
}

/// How many bytes the header that a multishot `recvmsg` writes at the start
/// of each buffer takes.
const RECVMSG_OUT: usize = size_of::<sys::io_uring_recvmsg_out>();

/// The room a buffer keeps for the address of a sender of `family`: that
/// family's address, or any family's, rounded up to 16 bytes, so that the
/// payload starts 16-byte aligned in a buffer that is.
fn address_room(family: i32) -> usize {
    let len = match family {
        libc::AF_INET => size_of::<libc::sockaddr_in>(),
        libc::AF_INET6 => size_of::<libc::sockaddr_in6>(),
        libc::AF_UNIX => size_of::<libc::sockaddr_un>(),
        _ => size_of::<libc::sockaddr_storage>(),
    };
    len.next_multiple_of(16)
}

/// Receives datagrams from a socket into a [`BufPool`] again and again as
/// they arrive, each with its sender's address: a multishot `recvmsg(2)`
/// (`IORING_RECV_MULTISHOT`), for sockets that need not be connected, such
/// as datagram sockets.
///
/// Each datagram takes one buffer, which the kernel fills with a header, the
/// sender's address, in the room the request keeps for an address of the
/// socket's family, and the payload, in the rest: a datagram longer than
/// that is cut short, and the rest of it lost, as one received into a
/// buffer of that size is, which the [`Datagram`] says. A datagram never
/// spans buffers, as data of a stream does in a bundle, so the buffers want
/// room for the largest datagram expected. When the pool runs dry the
/// kernel ends the request with `ENOBUFS`; [`RecvFromStream`] waits for a
/// buffer and re-arms it.
pub struct RecvFromMulti {
    fd: Fd,
    pool: BufPool,
    /// What the kernel reads the room for the address from: of its fields,
    /// only `msg_namelen` is set. Boxed, as the request moves while the
    /// kernel may read it.
    header: Box<libc::msghdr>,
    /// Where the payload starts in each buffer: past the header and the room
    /// for the address.
    payload: usize,
}

impl RecvFromMulti {
    /// Receives datagrams from `fd`, a socket of address family `family`,
    /// such as `AF_INET6`, into `pool`. The family sizes the room each buffer
    /// keeps for the sender's address; a family other than IPv4's, IPv6's
    /// and Unix sockets' gets room for any address.
    ///
    /// # Errors
    ///
    /// `InvalidInput` if the pool's buffers have no room for a payload after
    /// the header and the address.
    pub fn new(fd: impl Into<Fd>, pool: &BufPool, family: i32) -> io::Result<Self> {
        let room = address_room(family);
        let payload = RECVMSG_OUT + room;
        if pool.buffer_size() <= payload {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the pool's buffers have no room for a datagram after its header and address",
            ));
        }
        // SAFETY: all zeroes is a valid `msghdr`, with null pointers.
        let mut header: Box<libc::msghdr> = Box::new(unsafe { mem::zeroed() });
        // The kernel writes the address into each buffer, not through the
        // null `msg_name`, and with no `msg_iov` a datagram may fill the rest
        // of its buffer.
        header.msg_namelen = room as libc::socklen_t;
        Ok(Self {
            fd: fd.into(),
            pool: pool.clone(),
            header,
            payload,
        })
    }
}

impl std::fmt::Debug for RecvFromMulti {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecvFromMulti")
            .field("fd", &self.fd)
            .field("pool", &self.pool)
            .field("payload", &self.payload)
            .finish_non_exhaustive()
    }
}

// SAFETY: the entry points to the boxed header, which the request owns and
// which stays put when it moves, and the kernel writes into buffers of the
// pool, which the request keeps alive. A multishot `recvmsg`'s completions
// carry IORING_CQE_F_MORE until the last.
unsafe impl Multishot for RecvFromMulti {
    type Item = io::Result<Option<Datagram>>;

    /// `len` is 1, for the one header, as for any `recvmsg`; the kernel
    /// takes how much of a buffer a datagram may fill from the header's
    /// iovec, of which there is none, so that one may fill all of it.
    #[inline]
    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_RECVMSG;
        self.fd.apply(sqe);
        sqe.addr = (&raw const *self.header).expose_provenance() as u64;
        sqe.len = 1;
        sqe.ioprio = sys::IORING_RECV_MULTISHOT;
        sqe.flags |= sys::IOSQE_BUFFER_SELECT;
        sqe.buf_index = self.pool.group();
    }

    #[inline]
    fn item(&mut self, completion: Completion) -> Self::Item {
        let len = usize::try_from(completion.res()).unwrap_or(0);
        // Taken before looking at the result, so that a buffer the receive
        // consumed returns to the pool whatever happened.
        let lease = self.pool.lease(completion.flags(), len);
        completion.result()?;
        let lease = lease.ok_or_else(|| io::Error::other("the kernel filled no pooled buffer"))?;
        Datagram::new(lease, self.payload).map(Some)
    }
}

/// A datagram that a [`RecvFromMulti`] received into a buffer of a pool,
/// with its sender's address, which the buffer holds before the payload.
/// It derefs to the payload.
///
/// The buffer returns to the pool when the datagram is dropped, or the
/// lease of its payload that [`into_data`](Datagram::into_data) makes.
pub struct Datagram {
    /// What the kernel wrote into the buffer: the header, the address in its
    /// room, and the payload.
    lease: BufLease,
    /// Where the payload starts in the lease.
    payload: usize,
    /// The address's length, as the kernel reported it.
    name_len: u32,
    /// The `recvmsg(2)` result flags.
    flags: u32,
}

impl Datagram {
    /// The datagram in `lease`, whose payload starts at `payload`.
    #[inline]
    fn new(lease: BufLease, payload: usize) -> io::Result<Self> {
        if lease.len() < payload {
            return Err(io::Error::other(
                "the kernel wrote less than a datagram's header and address",
            ));
        }
        // SAFETY: the lease holds a whole header, read unaligned, as the
        // buffers need not be aligned, and any bytes are a valid one.
        let header = unsafe {
            lease
                .as_ptr()
                .cast::<sys::io_uring_recvmsg_out>()
                .read_unaligned()
        };
        Ok(Self {
            lease,
            payload,
            name_len: header.namelen,
            flags: header.flags,
        })
    }

    /// The sender's address. A datagram from a Unix socket without an
    /// address comes from the empty one, which [`SockAddr::as_unix`] makes
    /// the unnamed address.
    pub fn from(&self) -> SockAddr {
        SockAddr::from_kernel(self.name())
    }

    /// The sender's address if it is an IPv4 or IPv6 one, as a UDP
    /// socket's senders' are: what [`from`](Datagram::from) and then
    /// [`SockAddr::as_inet`] give, read straight from the buffer.
    #[inline]
    pub fn inet_from(&self) -> Option<net::SocketAddr> {
        crate::sockaddr::inet(self.name())
    }

    /// Whether the datagram was longer than its buffer had room for, so
    /// that the kernel cut it short, and the rest of it was lost.
    #[inline]
    pub fn is_truncated(&self) -> bool {
        self.flags & libc::MSG_TRUNC.cast_unsigned() != 0
    }

    /// The payload, as a lease of the buffer whose data and room leave the
    /// header and the address out.
    #[inline]
    pub fn into_data(self) -> BufLease {
        let mut lease = self.lease;
        lease.skip(self.payload);
        lease
    }

    /// The address, as the kernel wrote it, up to its room.
    #[inline]
    fn name(&self) -> &[u8] {
        let len = (self.name_len as usize).min(self.payload - RECVMSG_OUT);
        &self.lease[RECVMSG_OUT..RECVMSG_OUT + len]
    }
}

impl Deref for Datagram {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        &self.lease[self.payload..]
    }
}

impl DerefMut for Datagram {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.lease[self.payload..]
    }
}

impl std::fmt::Debug for Datagram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Datagram")
            .field("from", &self.from())
            .field("len", &self.len())
            .field("truncated", &self.is_truncated())
            .finish()
    }
}

/// The datagrams arriving on a socket, each with its sender's address,
/// received into a [`BufPool`] by a multishot `recvmsg` that the stream
/// keeps armed, as [`RecvStream`](super::RecvStream) keeps a receive armed:
/// re-armed once a buffer returns to a pool that ran dry, and when the
/// kernel ends it for reasons of its own, as it does after an empty
/// datagram, which it takes for the end of the data.
///
/// The stream ends after yielding an error, such as `ECONNREFUSED` on a
/// connected UDP socket whose peer has no socket bound.
#[derive(Debug)]
pub struct RecvFromStream {
    inner: Rearming<RecvFromMulti>,
}

impl RecvFromStream {
    /// A stream of the datagrams arriving on `fd`, a socket of address
    /// family `family`, received into `pool`; see [`RecvFromMulti::new`].
    ///
    /// # Errors
    ///
    /// As for [`RecvFromMulti::new`].
    pub fn new(
        driver: &Driver,
        fd: impl Into<Fd>,
        pool: &BufPool,
        family: i32,
    ) -> io::Result<Self> {
        let request = RecvFromMulti::new(fd, pool, family)?;
        Ok(Self {
            inner: Rearming::new(driver, request, pool),
        })
    }

    /// The next datagram, or `None` once an error has been yielded.
    #[inline]
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<Datagram>>> {
        self.inner.poll_next(cx)
    }

    /// The next datagram; see [`poll_next`](RecvFromStream::poll_next).
    pub async fn next(&mut self) -> Option<io::Result<Datagram>> {
        poll_fn(|cx| self.poll_next(cx)).await
    }

    /// The next datagram if it has arrived already, without waiting, or
    /// `None`: nothing has, or the stream has ended, which the next
    /// [`poll_next`](RecvFromStream::poll_next) tells. A server that takes
    /// the datagrams that have arrived so, after each one it waits for,
    /// answers them all before the thread next waits.
    #[inline]
    pub fn next_ready(&mut self) -> Option<io::Result<Datagram>> {
        self.inner.next_ready()
    }

    /// Whether the stream has ended, after yielding an error.
    #[inline]
    pub fn is_ended(&self) -> bool {
        self.inner.finished
    }
}

/// Shuts down one or both directions of a connection, like `shutdown(2)`.
#[derive(Debug)]
pub struct Shutdown {
    fd: Fd,
    how: net::Shutdown,
}

impl Shutdown {
    /// Shuts down `how` much of the connection on `fd`.
    pub fn new(fd: impl Into<Fd>, how: net::Shutdown) -> Self {
        Self { fd: fd.into(), how }
    }
}

// SAFETY: the entry points to no memory, and a shutdown request posts one
// completion.
unsafe impl Operation for Shutdown {
    type Output = io::Result<()>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_SHUTDOWN;
        self.fd.apply(sqe);
        sqe.len = match self.how {
            net::Shutdown::Read => libc::SHUT_RD,
            net::Shutdown::Write => libc::SHUT_WR,
            net::Shutdown::Both => libc::SHUT_RDWR,
        }
        .cast_unsigned();
    }

    fn complete(self, completion: Completion) -> Self::Output {
        completion.result().map(drop)
    }
}
