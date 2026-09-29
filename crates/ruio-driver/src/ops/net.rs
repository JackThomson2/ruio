//! Socket operations.

use std::io;
use std::mem;
use std::net;
use std::os::fd::{FromRawFd, OwnedFd};
use std::ptr;

use ruio_sys as sys;

use super::complete_read;
use crate::buf::{BufResult, IoBuf, IoBufMut};
use crate::{Completion, Descriptor, DirectFd, Driver, Fd, Operation, SockAddr};

/// Where an operation that makes a file puts it.
#[derive(Clone, Copy, Debug)]
enum Install {
    /// In a new file descriptor of the process, closed on `exec`.
    Plain,
    /// In a free slot of the registered file table of the ring with this
    /// ID.
    Direct(u64),
}

impl Install {
    /// The entry's `accept_flags`: the kernel refuses close-on-exec for a
    /// direct descriptor, which has nothing to close.
    fn accept_flags(self) -> u32 {
        match self {
            Install::Plain => libc::SOCK_CLOEXEC.cast_unsigned(),
            Install::Direct(_) => 0,
        }
    }

    /// The entry's `file_index`.
    fn file_index(self) -> u32 {
        match self {
            Install::Plain => 0,
            Install::Direct(_) => sys::IORING_FILE_INDEX_ALLOC,
        }
    }

    /// Takes ownership of the file the kernel made, which the result names.
    fn descriptor(self, res: u32) -> Descriptor {
        match self {
            Install::Plain => {
                // SAFETY: the kernel made the descriptor for this operation,
                // and nothing else owns it.
                Descriptor::Plain(unsafe { OwnedFd::from_raw_fd(res.cast_signed()) })
            }
            Install::Direct(ring) => Descriptor::Direct(DirectFd::new(ring, res)),
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

/// Makes a send go to `addr`: `addr2`, with the length in `addr_len`, the
/// first half of `file_index`.
pub(super) fn set_destination(sqe: &mut sys::io_uring_sqe, addr: &SockAddr) {
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
