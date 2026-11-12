//! TCP and UDP sockets, which accept, connect, read and write, send and
//! receive through the port.
//!
//! A listener accepts with `AcceptEx` and a stream connects with
//! `ConnectEx`, the calls of Winsock that complete to a port, which a
//! socket's provider hands out as function pointers; a stream reads with
//! `WSARecv` and writes with `WSASend`. An operation's outcome is read with
//! `WSAGetOverlappedResult`, which gives Winsock's own errors, those the
//! standard library's sockets report. A socket is made here, opened for
//! overlapped I/O, and bound here too, so that one of IPv6 reaches IPv4
//! addresses as well, in their IPv6 form, as it does on Linux, where
//! Windows makes it reach IPv6 addresses alone. What needs no waiting
//! besides, addresses and options, goes through the standard library's
//! sockets, which the types here hold.
//!
//! A stream also receives into a pool of buffers, as a `RecvStream`, which
//! holds no buffer while it waits for data, and a UDP socket receives its
//! datagrams so, as a `RecvStream` too or, each with the address it came
//! from, as a `RecvFromStream`.
//!
//! A stream's write may be left behind it, for the engine to finish: the
//! send is started with no future to wait for it, shares the socket with
//! the stream, so that the socket closes only once the send has finished,
//! and records how it went for the writes after it.
//!
//! A stream also moves to the port of another engine, on another thread:
//! the system takes its socket off its port once the port has delivered
//! the completions of all of its operations, and the other engine's port
//! takes it as it takes a new one.
//!
//! A stream's socket asks the system to queue no completion for an
//! operation that succeeds during the call that starts it, where its
//! provider allows that. So a read of data that has arrived, and a write
//! that the system's buffer takes, are finished by the poll that starts
//! them, and cost no turn of the port.

use std::cell::Cell;
use std::ffi::c_void;
use std::fmt;
use std::future::Future;
use std::io;
use std::net::{
    self, IpAddr, Ipv4Addr, Ipv6Addr, Shutdown, SocketAddr, SocketAddrV6, ToSocketAddrs,
};
use std::os::windows::io::{AsRawSocket, FromRawSocket, OwnedSocket, RawSocket};
use std::pin::Pin;
use std::ptr;
use std::rc::Rc;
use std::sync::OnceLock;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use ruio_buf::{IoBuf, IoBufMut, IoBufs, Slice};

use crate::behind::Behind;
use crate::buf::BufPool;
use crate::handle::{IoHandle, Raw};
use crate::op::{Limit, Op, Read, ReadExact, Started, Write, WriteAll};
use crate::sys;

mod datagrams;
mod recv;
mod sent;
mod udp;
mod vectored;
mod write_behind;

pub use datagrams::{Datagram, RecvFromStream};
pub use recv::RecvStream;
pub use sent::SentTcpStream;
pub use udp::{RecvFrom, SendTo, UdpSocket};
pub use vectored::WriteAllVectored;
pub use write_behind::{Flush, WriteBehind};

/// Room for one of the two addresses `AcceptEx` writes: the largest address
/// there is, and the 16 bytes more that it asks for.
const ADDR_ROOM: usize = 128 + 16;

/// How many connections the system queues for a listener until they are
/// accepted, as for the standard library's listeners.
const BACKLOG: i32 = 128;

impl Raw for net::TcpListener {
    fn raw(&self) -> sys::Handle {
        handle_of(self.as_raw_socket())
    }
}

/// A stream's socket, and whether its calls that take no `OVERLAPPED` wait.
struct Socket {
    socket: net::TcpStream,
    /// Whether the socket was made non-blocking, as receiving into a pool
    /// makes it.
    nonblocking: Cell<bool>,
}

impl Socket {
    fn new(socket: net::TcpStream) -> Self {
        Self {
            socket,
            nonblocking: Cell::new(false),
        }
    }

    /// Makes the socket's calls that take no `OVERLAPPED` fail rather than
    /// wait, which leaves its operations through the port as they are.
    fn unblock(&self) -> io::Result<()> {
        if !self.nonblocking.get() {
            self.socket.set_nonblocking(true)?;
            self.nonblocking.set(true);
        }
        Ok(())
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        // A socket that lingers closes for certain only if it blocks: one
        // that does not refuses to close while it has data left to send,
        // and its handle would stay open.
        if self.nonblocking.get() {
            let _ = self.socket.set_nonblocking(false);
        }
    }
}

impl Raw for Rc<Socket> {
    fn raw(&self) -> sys::Handle {
        handle_of(self.socket.as_raw_socket())
    }

    /// A stream's socket may, if its provider gives it a handle of the
    /// system's own. A provider layered over that one has to see the
    /// socket's completions, and would not see those the port is spared.
    fn may_skip_port(&self) -> bool {
        let mut info = sys::ProtocolInfo::EMPTY;
        let mut len = size_of::<sys::ProtocolInfo>() as i32;
        // SAFETY: the socket is open, and the provider's description has
        // room for as many bytes as the length says, which are written
        // during the call.
        let failed = unsafe {
            sys::getsockopt(
                socket_of(self.raw()),
                sys::SOL_SOCKET,
                sys::SO_PROTOCOL_INFOW,
                ptr::from_mut(&mut info).cast(),
                &mut len,
            )
        };
        failed == 0 && info.service_flags1 & sys::XP1_IFS_HANDLES != 0
    }
}

/// A socket as a handle, which it is to the calls that take one.
fn handle_of(socket: RawSocket) -> sys::Handle {
    ptr::without_provenance_mut(socket as usize)
}

/// A socket's handle as the socket, as Winsock's calls take it.
fn socket_of(handle: sys::Handle) -> sys::Socket {
    handle.addr()
}

/// The calling thread's last Winsock error.
pub(crate) fn last_error() -> io::Error {
    io::Error::from_raw_os_error(sys::WSAGetLastError())
}

/// Starts Winsock for the process, which it must be before a socket is made
/// here rather than by the standard library, which starts it for its own.
fn startup() -> io::Result<()> {
    static STARTED: OnceLock<i32> = OnceLock::new();
    let code = *STARTED.get_or_init(|| {
        // Room for a `WSADATA`, of some 400 bytes, which nothing here reads.
        let mut data = [0_u64; 64];
        // SAFETY: the call writes a `WSADATA`, which the room holds, at its
        // alignment.
        unsafe { sys::WSAStartup(0x0202, data.as_mut_ptr().cast()) }
    });
    match code {
        0 => Ok(()),
        code => Err(io::Error::from_raw_os_error(code)),
    }
}

/// The address family of `addr`.
pub(crate) fn family(addr: &SocketAddr) -> u16 {
    if addr.is_ipv6() {
        sys::AF_INET6
    } else {
        sys::AF_INET
    }
}

/// A TCP socket of `family`, unbound, as [`open_socket`] makes it.
pub(crate) fn new_socket(family: u16) -> io::Result<OwnedSocket> {
    open_socket(family, sys::SOCK_STREAM, sys::IPPROTO_TCP)
}

/// A socket of `family`, `kind` and `protocol`, unbound, opened for
/// overlapped I/O and not inherited by child processes, as the standard
/// library's are. One of IPv6 reaches IPv4 addresses too, in their IPv6
/// form, `::ffff:` and the IPv4 address, as it does on Linux.
fn open_socket(family: u16, kind: i32, protocol: i32) -> io::Result<OwnedSocket> {
    startup()?;
    // SAFETY: the call is given no protocol information, and so no memory.
    let socket = unsafe {
        sys::WSASocketW(
            i32::from(family),
            kind,
            protocol,
            ptr::null(),
            0,
            sys::WSA_FLAG_OVERLAPPED | sys::WSA_FLAG_NO_HANDLE_INHERIT,
        )
    };
    if socket == sys::INVALID_SOCKET {
        return Err(last_error());
    }
    // SAFETY: the socket was just opened, and nothing else owns it.
    let socket = unsafe { OwnedSocket::from_raw_socket(socket as RawSocket) };
    if family == sys::AF_INET6 {
        let only: u32 = 0;
        // SAFETY: the socket is open, and the option's value, a `DWORD`, is
        // read during the call.
        let failed = unsafe {
            sys::setsockopt(
                socket.as_raw_socket() as sys::Socket,
                sys::IPPROTO_IPV6,
                sys::IPV6_V6ONLY,
                ptr::from_ref(&only).cast(),
                size_of::<u32>() as i32,
            )
        };
        if failed != 0 {
            return Err(last_error());
        }
    }
    Ok(socket)
}

/// Binds `socket` to `addr`.
fn bind_to(socket: &OwnedSocket, addr: &SocketAddr) -> io::Result<()> {
    let addr = RawAddr::new(addr);
    // SAFETY: the socket is open, and the address is read during the call.
    let failed = unsafe {
        sys::bind(
            socket.as_raw_socket() as sys::Socket,
            addr.bytes.as_ptr(),
            addr.len,
        )
    };
    if failed != 0 {
        return Err(last_error());
    }
    Ok(())
}

/// What `make` makes of the first of the addresses `addr` resolves to that
/// it makes something of, as the standard library's sockets bind: a host
/// name is resolved on the calling thread, which waits for it. If it makes
/// nothing, the error of the last address, or one of kind `InvalidInput` if
/// `addr` resolves to none.
fn first_bound<T>(
    addr: impl ToSocketAddrs,
    make: impl Fn(&SocketAddr) -> io::Result<T>,
) -> io::Result<T> {
    let mut last = None;
    for addr in addr.to_socket_addrs()? {
        match make(&addr) {
            Ok(made) => return Ok(made),
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

/// A TCP socket bound to `addr`, or to the first address it resolves to
/// that it can be bound to, as [`first_bound`] binds, and listening, with
/// the backlog of the standard library's listeners.
pub(crate) fn listening(addr: impl ToSocketAddrs) -> io::Result<OwnedSocket> {
    first_bound(addr, |addr| {
        let socket = new_socket(family(addr))?;
        bind_to(&socket, addr)?;
        // SAFETY: the socket is open and bound.
        let failed = unsafe { sys::listen(socket.as_raw_socket() as sys::Socket, BACKLOG) };
        if failed != 0 {
            return Err(last_error());
        }
        Ok(socket)
    })
}

/// Sets `SO_LINGER` of `socket`, an open socket: for how long closing it
/// waits for what it has left to send to go, in whole seconds, or `None`
/// for not at all.
pub(crate) fn set_linger_of(socket: sys::Socket, linger: Option<Duration>) -> io::Result<()> {
    let value = sys::Linger {
        onoff: u16::from(linger.is_some()),
        linger: linger.map_or(0, |linger| {
            u16::try_from(linger.as_secs()).unwrap_or(u16::MAX)
        }),
    };
    // SAFETY: the caller has the socket open, and the value is read during
    // the call.
    let failed = unsafe {
        sys::setsockopt(
            socket,
            sys::SOL_SOCKET,
            sys::SO_LINGER,
            ptr::from_ref(&value).cast(),
            size_of::<sys::Linger>() as i32,
        )
    };
    if failed != 0 {
        return Err(last_error());
    }
    Ok(())
}

/// `SO_LINGER` of `socket`, an open socket.
pub(crate) fn linger_of(socket: sys::Socket) -> io::Result<Option<Duration>> {
    let mut value = sys::Linger::default();
    let mut len = size_of::<sys::Linger>() as i32;
    // SAFETY: the caller has the socket open, and the value has room for as
    // many bytes as the length says, which are written during the call.
    let failed = unsafe {
        sys::getsockopt(
            socket,
            sys::SOL_SOCKET,
            sys::SO_LINGER,
            ptr::from_mut(&mut value).cast(),
            &mut len,
        )
    };
    if failed != 0 {
        return Err(last_error());
    }
    Ok((value.onoff != 0).then(|| Duration::from_secs(value.linger.into())))
}

/// A socket address as Winsock takes it: the bytes of a `sockaddr_in` or of
/// a `sockaddr_in6`, and how many they are.
#[repr(C, align(4))]
pub(crate) struct RawAddr {
    pub(crate) bytes: [u8; 28],
    pub(crate) len: i32,
}

impl RawAddr {
    pub(crate) fn new(addr: &SocketAddr) -> Self {
        let mut bytes = [0; 28];
        bytes[2..4].copy_from_slice(&addr.port().to_be_bytes());
        let len = match addr {
            SocketAddr::V4(v4) => {
                bytes[..2].copy_from_slice(&sys::AF_INET.to_ne_bytes());
                bytes[4..8].copy_from_slice(&v4.ip().octets());
                16
            }
            SocketAddr::V6(v6) => {
                bytes[..2].copy_from_slice(&sys::AF_INET6.to_ne_bytes());
                bytes[4..8].copy_from_slice(&v6.flowinfo().to_ne_bytes());
                bytes[8..24].copy_from_slice(&v6.ip().octets());
                bytes[24..28].copy_from_slice(&v6.scope_id().to_ne_bytes());
                28
            }
        };
        Self { bytes, len }
    }

    /// Any address and port of `peer`'s family, for the system to choose
    /// those a socket connects to `peer` from.
    fn any_for(peer: &SocketAddr) -> Self {
        let ip = if peer.is_ipv6() {
            IpAddr::V6(Ipv6Addr::UNSPECIFIED)
        } else {
            IpAddr::V4(Ipv4Addr::UNSPECIFIED)
        };
        Self::new(&SocketAddr::new(ip, 0))
    }

    /// Room for the system to write an address of either family to, and
    /// how much room it is.
    fn room() -> Self {
        Self {
            bytes: [0; 28],
            len: 28,
        }
    }

    /// The address the system wrote.
    fn to_socket_addr(&self) -> io::Result<SocketAddr> {
        let bytes = &self.bytes;
        let family = u16::from_ne_bytes([bytes[0], bytes[1]]);
        let port = u16::from_be_bytes([bytes[2], bytes[3]]);
        match (family, self.len) {
            (sys::AF_INET, 16..) => {
                let ip = Ipv4Addr::new(bytes[4], bytes[5], bytes[6], bytes[7]);
                Ok(SocketAddr::new(IpAddr::V4(ip), port))
            }
            (sys::AF_INET6, 28..) => {
                let flowinfo = u32::from_ne_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
                let mut ip = [0; 16];
                ip.copy_from_slice(&bytes[8..24]);
                let scope_id = u32::from_ne_bytes([bytes[24], bytes[25], bytes[26], bytes[27]]);
                let addr = SocketAddrV6::new(Ipv6Addr::from(ip), port, flowinfo, scope_id);
                Ok(SocketAddr::V6(addr))
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "an address that is neither IPv4's nor IPv6's",
            )),
        }
    }
}

/// The calls of Winsock that it does not export by name, which a socket's
/// provider hands out as pointers.
struct Extensions {
    accept_ex: sys::AcceptEx,
    connect_ex: sys::ConnectEx,
    get_accept_ex_sockaddrs: sys::GetAcceptExSockaddrs,
}

// A function pointer in an `Option` is a pointer that may be null, which is
// what a provider writes.
const _: () = assert!(size_of::<Option<sys::AcceptEx>>() == size_of::<usize>());

/// The extensions, asked of `socket`'s provider the first time and kept for
/// the process, whose TCP sockets all have the one provider.
fn extensions(socket: sys::Socket) -> io::Result<&'static Extensions> {
    static EXTENSIONS: OnceLock<Extensions> = OnceLock::new();
    if let Some(extensions) = EXTENSIONS.get() {
        return Ok(extensions);
    }
    let mut accept_ex: Option<sys::AcceptEx> = None;
    // SAFETY: the option is a pointer that may be null, of the type of the
    // function the ID names.
    unsafe { extension(socket, &sys::WSAID_ACCEPTEX, (&raw mut accept_ex).cast()) }?;
    let mut connect_ex: Option<sys::ConnectEx> = None;
    // SAFETY: as above.
    unsafe { extension(socket, &sys::WSAID_CONNECTEX, (&raw mut connect_ex).cast()) }?;
    let mut get_accept_ex_sockaddrs: Option<sys::GetAcceptExSockaddrs> = None;
    // SAFETY: as above.
    unsafe {
        extension(
            socket,
            &sys::WSAID_GETACCEPTEXSOCKADDRS,
            (&raw mut get_accept_ex_sockaddrs).cast(),
        )
    }?;
    let (Some(accept_ex), Some(connect_ex), Some(get_accept_ex_sockaddrs)) =
        (accept_ex, connect_ex, get_accept_ex_sockaddrs)
    else {
        return Err(io::Error::other(
            "Winsock has no AcceptEx, ConnectEx or GetAcceptExSockaddrs",
        ));
    };
    Ok(EXTENSIONS.get_or_init(|| Extensions {
        accept_ex,
        connect_ex,
        get_accept_ex_sockaddrs,
    }))
}

/// Asks `socket`'s provider for the function that `id` names, and writes
/// its address to `function`.
///
/// # Safety
///
/// `function` must be valid for writing a pointer, which may be null, to a
/// function of the type `id` names.
unsafe fn extension(socket: sys::Socket, id: &sys::Guid, function: *mut c_void) -> io::Result<()> {
    let mut returned = 0;
    // SAFETY: the call reads the ID and writes a pointer where the caller
    // vouches for; given no `OVERLAPPED`, it is done when it returns.
    let failed = unsafe {
        sys::WSAIoctl(
            socket,
            sys::SIO_GET_EXTENSION_FUNCTION_POINTER,
            ptr::from_ref(id).cast(),
            size_of::<sys::Guid>() as u32,
            function,
            size_of::<usize>() as u32,
            &mut returned,
            ptr::null_mut(),
            ptr::null(),
        )
    };
    if failed != 0 {
        return Err(last_error());
    }
    Ok(())
}

/// How Winsock took an operation whose call `succeeded` or did not, having
/// transferred `bytes` if it did: the call then completed the operation,
/// whose completion is queued to the port all the same unless the socket
/// asked for none.
fn taken(succeeded: bool, bytes: u32) -> io::Result<Started> {
    if succeeded {
        return Ok(Started::AtOnce(bytes));
    }
    match sys::WSAGetLastError() {
        sys::WSA_IO_PENDING => Ok(Started::Later),
        code => Err(io::Error::from_raw_os_error(code)),
    }
}

/// Reads the outcome of a socket's operation, as `op`'s `Outcome` says,
/// with Winsock's own error if it failed.
///
/// # Safety
///
/// As for `Outcome`.
unsafe fn outcome(
    socket: sys::Handle,
    overlapped: *const sys::Overlapped,
    _bytes: u32,
) -> io::Result<u32> {
    let mut transferred = 0;
    let mut flags = 0;
    // SAFETY: the socket is open and its operation has completed, by the
    // caller's promise, so the call reads the `OVERLAPPED` without waiting.
    let ok = unsafe {
        sys::WSAGetOverlappedResult(
            socket_of(socket),
            overlapped,
            &mut transferred,
            0,
            &mut flags,
        )
    };
    if ok == 0 {
        return Err(last_error());
    }
    Ok(transferred)
}

/// A TCP listener, whose accepts complete to the port of the engine current
/// where it was bound, and which belongs to that engine's thread.
pub struct TcpListener {
    listener: IoHandle<net::TcpListener>,
    /// The listener's address family, which the sockets it accepts
    /// connections on must have.
    family: u16,
}

impl TcpListener {
    /// Binds a listener to `addr`, or to the first address it resolves to
    /// that it can be bound to, as `std::net::TcpListener::bind` does: a
    /// host name is resolved on the calling thread, which waits for it. A
    /// listener on an IPv6 address accepts connections from IPv4 addresses
    /// too, as on Linux, whose peers it reports in their IPv6 form.
    ///
    /// # Errors
    ///
    /// What binding and listening report, as when the address is in use,
    /// `InvalidInput` if `addr` resolves to no address, and an error of
    /// kind `Other` if the thread has no current engine, as outside a
    /// runtime.
    pub fn bind(addr: impl ToSocketAddrs) -> io::Result<Self> {
        let listener = net::TcpListener::from(listening(addr)?);
        let family = family(&listener.local_addr()?);
        Ok(Self {
            listener: IoHandle::new(listener)?,
            family,
        })
    }

    /// Takes a listening socket from the standard library, for the port of
    /// the thread's current engine, with the signature `from_std` has in
    /// ruio's `net` on io_uring, where taking a socket checks nothing. The
    /// socket keeps the options it has: one of IPv6 accepts connections
    /// from IPv4 addresses only if it was made to.
    ///
    /// # Panics
    ///
    /// If the thread has no current engine, as outside a runtime, or the
    /// system refuses to associate the socket with its port, as when
    /// another port has it already.
    pub fn from_std(listener: net::TcpListener) -> Self {
        let family = match listener.local_addr() {
            Ok(addr) => family(&addr),
            Err(err) => panic!("a listening socket has an address: {err}"),
        };
        Self {
            listener: IoHandle::taken(listener),
            family,
        }
    }

    /// The address the listener is bound to.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.io().local_addr()
    }

    /// Accepts a connection; see [`Accept`].
    pub fn accept(&self) -> Accept<'_> {
        let accepting = Accepting {
            family: self.family,
            socket: None,
            addrs: [0; 2 * ADDR_ROOM],
        };
        Accept {
            op: Op::new(self.listener.target(), accepting, 0),
            listener: self,
        }
    }

    /// Makes a stream of the socket that an accept completed on.
    fn accepted(&self, mut accepting: Accepting) -> io::Result<(TcpStream, SocketAddr)> {
        let listener = socket_of(self.listener.raw());
        let peer = accepting.peer(listener)?;
        let socket = accepting
            .socket
            .expect("an accept that completed made its socket");
        // SAFETY: both sockets are open, and the option's value, the
        // listener's socket, is read during the call.
        let failed = unsafe {
            sys::setsockopt(
                socket.as_raw_socket() as sys::Socket,
                sys::SOL_SOCKET,
                sys::SO_UPDATE_ACCEPT_CONTEXT,
                ptr::from_ref(&listener).cast(),
                size_of::<sys::Socket>() as i32,
            )
        };
        if failed != 0 {
            return Err(last_error());
        }
        let stream = net::TcpStream::from(socket);
        let stream = Rc::new(Socket::new(stream));
        let stream = IoHandle::on(stream, self.listener.engine().clone())?;
        Ok((TcpStream::new(stream), peer))
    }
}

impl fmt::Debug for TcpListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpListener")
            .field("listener", self.listener.io())
            .finish()
    }
}

/// What an accept's slot holds while the system has the accept.
struct Accepting {
    family: u16,
    /// The socket the connection arrives on, made as the accept starts.
    socket: Option<OwnedSocket>,
    /// Where the system writes the connection's two addresses, of which
    /// the peer's is read once the accept has completed, rather than asked
    /// of the stream with a call more.
    addrs: [u8; 2 * ADDR_ROOM],
}

impl Accepting {
    /// The address of the peer of the connection that the accept from
    /// `listener` completed, as the system wrote it.
    fn peer(&mut self, listener: sys::Socket) -> io::Result<SocketAddr> {
        let get_accept_ex_sockaddrs = extensions(listener)?.get_accept_ex_sockaddrs;
        let (mut local, mut local_len) = (ptr::null_mut(), 0);
        let (mut remote, mut remote_len) = (ptr::null_mut(), 0);
        // SAFETY: the room holds what the accept wrote, given the lengths it
        // was given and no data to receive; the call writes four values, to
        // the places it is given, and reads nothing beyond the room.
        unsafe {
            get_accept_ex_sockaddrs(
                self.addrs.as_mut_ptr().cast(),
                0,
                ADDR_ROOM as u32,
                ADDR_ROOM as u32,
                &mut local,
                &mut local_len,
                &mut remote,
                &mut remote_len,
            );
        }
        // The address lies within the room, where the call points.
        let at = remote.addr().wrapping_sub(self.addrs.as_ptr().addr());
        let written = usize::try_from(remote_len).ok().and_then(|len| {
            let end = at.checked_add(len)?;
            self.addrs.get(at..end)
        });
        let Some(written) = written else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "AcceptEx wrote no address of the peer",
            ));
        };
        let mut addr = RawAddr::room();
        let len = written.len().min(addr.bytes.len());
        addr.bytes[..len].copy_from_slice(&written[..len]);
        addr.len = len as i32;
        addr.to_socket_addr()
    }
}

/// Starts an accept on `listener`, as `op`'s `Start` says.
///
/// # Safety
///
/// As for `Start`.
unsafe fn start_accept(
    listener: sys::Handle,
    accepting: &mut Accepting,
    overlapped: *mut sys::Overlapped,
) -> io::Result<Started> {
    let listener = socket_of(listener);
    let accept_ex = extensions(listener)?.accept_ex;
    let accepted = accepting
        .socket
        .insert(new_socket(accepting.family)?)
        .as_raw_socket() as sys::Socket;
    let mut received = 0;
    // SAFETY: both sockets are open, the listener's by the caller's promise;
    // the room for the addresses and the `OVERLAPPED` are in the slot, which
    // stays in place until the completion is delivered; and the count is
    // written before the call returns, if it is.
    let ok = unsafe {
        accept_ex(
            listener,
            accepted,
            accepting.addrs.as_mut_ptr().cast(),
            0,
            ADDR_ROOM as u32,
            ADDR_ROOM as u32,
            &mut received,
            overlapped,
        )
    };
    taken(ok != 0, received)
}

/// The future of [`TcpListener::accept`]: the stream of the connection it
/// accepted, and the address of the stream's peer.
///
/// It starts the accept when first polled. Dropping it before the accept
/// completes cancels the accept, and a connection it had accepted by then
/// is closed.
#[must_use = "futures do nothing unless polled"]
pub struct Accept<'a> {
    op: Op<'a, Accepting>,
    listener: &'a TcpListener,
}

impl Accept<'_> {
    /// Cancels the accept if no connection has come `duration` after it
    /// starts: it then fails with `ERROR_TIMEOUT`, of
    /// [`io::ErrorKind::TimedOut`]. A connection accepted before the
    /// cancellation can stop the accept is the accept's.
    ///
    /// This has no effect once the accept has started.
    pub fn timeout(mut self, duration: Duration) -> Self {
        self.op.set_limit(Limit::After(duration));
        self
    }
}

impl Future for Accept<'_> {
    type Output = io::Result<(TcpStream, SocketAddr)>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (result, accepting) = ready!(self.op.poll_with(cx, start_accept, outcome));
        Poll::Ready(result.and_then(|_| self.listener.accepted(accepting)))
    }
}

impl fmt::Debug for Accept<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Accept")
            .field("listener", self.listener)
            .finish_non_exhaustive()
    }
}

/// A TCP stream, whose reads and writes complete to the port of the engine
/// current where it was connected or accepted, and which belongs to that
/// engine's thread.
///
/// A read or a write takes its buffer and hands it back with the result,
/// as ruio's I/O does on io_uring. A read and a write may be in flight at
/// once.
///
/// A write may also be left behind the stream, for the engine to finish;
/// see [`write_behind`](TcpStream::write_behind). The stream's writes after
/// it wait for it, and fail with its error if it failed. A stream dropped
/// with a write behind it in flight closes once that has finished.
pub struct TcpStream {
    /// The socket, which the sends left behind the stream share.
    stream: IoHandle<Rc<Socket>>,
    /// How the writes left behind the stream go.
    behind: Rc<Behind>,
}

impl TcpStream {
    /// Connects to `addr`.
    ///
    /// Dropping the future before the connection is made cancels it.
    ///
    /// # Errors
    ///
    /// What connecting reports, as when nothing listens at `addr`, and an
    /// error of kind `Other` if the thread has no current engine, as outside
    /// a runtime.
    pub async fn connect(addr: SocketAddr) -> io::Result<Self> {
        let socket = Socket::new(net::TcpStream::from(new_socket(family(&addr))?));
        let stream = IoHandle::new(Rc::new(socket))?;
        let socket = socket_of(stream.raw());
        // `ConnectEx` connects a socket that is bound already.
        let any = RawAddr::any_for(&addr);
        // SAFETY: the socket is open, and the address is read during the
        // call.
        let failed = unsafe { sys::bind(socket, any.bytes.as_ptr(), any.len) };
        if failed != 0 {
            return Err(last_error());
        }
        Connect {
            op: Op::new(stream.target(), RawAddr::new(&addr), 0),
        }
        .await?;
        // SAFETY: the socket is open, and the option takes no value.
        let failed = unsafe {
            sys::setsockopt(
                socket,
                sys::SOL_SOCKET,
                sys::SO_UPDATE_CONNECT_CONTEXT,
                ptr::null(),
                0,
            )
        };
        if failed != 0 {
            return Err(last_error());
        }
        Ok(Self::new(stream))
    }

    /// Takes a connected socket from the standard library, for the port of
    /// the thread's current engine, with the signature `from_std` has in
    /// ruio's `net` on io_uring, where taking a socket checks nothing.
    ///
    /// # Panics
    ///
    /// If the thread has no current engine, as outside a runtime, or the
    /// system refuses to associate the socket with its port, as when
    /// another port has it already.
    pub fn from_std(stream: net::TcpStream) -> Self {
        // A socket made non-blocking blocks again, as the stream's own do
        // until a receive into a pool needs otherwise: one that lingers
        // closes for certain only if it blocks. Its operations through the
        // port wait for nothing either way.
        let _ = stream.set_nonblocking(false);
        Self::new(IoHandle::taken(Rc::new(Socket::new(stream))))
    }

    /// The stream of a connected socket, with no write behind it.
    fn new(stream: IoHandle<Rc<Socket>>) -> Self {
        Self {
            stream,
            behind: Rc::default(),
        }
    }

    /// Reads into `buf`, up to its `bytes_total`, what the peer has sent;
    /// see [`Read`].
    pub fn read<B: IoBufMut>(&self, buf: B) -> Read<'_, B> {
        Read::with(self.stream.target(), buf, 0, start_recv::<B>, outcome)
    }

    /// Writes `buf`'s bytes to the peer; see [`Write`]. It waits for a
    /// [`write_behind`](Self::write_behind) in flight, and fails if one
    /// failed.
    pub fn write<B: IoBuf>(&self, buf: B) -> Write<'_, B> {
        Write::with(self.stream.target(), buf, 0, start_send::<B>, outcome).behind(&self.behind)
    }

    /// Fills `buf` from its start, up to its `bytes_total`, with what the
    /// peer sends, in as many reads as that takes; see [`ReadExact`].
    pub fn read_exact<B: IoBufMut>(&self, buf: B) -> ReadExact<'_, B> {
        let target = self.stream.target();
        ReadExact::with(target, buf, 0, 0, start_recv::<Slice<B>>, outcome)
    }

    /// Writes all of `buf`'s bytes to the peer, in as many writes as that
    /// takes; see [`WriteAll`]. As [`write`](Self::write), it waits for a
    /// write behind in flight, and fails if one failed.
    pub fn write_all<B: IoBuf>(&self, buf: B) -> WriteAll<'_, B> {
        let target = self.stream.target();
        WriteAll::with(target, buf, 0, start_send::<Slice<B>>, outcome).behind(&self.behind)
    }

    /// Writes all of the bytes of `bufs` to the peer, one buffer after
    /// another, such as a bundle received; see [`WriteAllVectored`]. As
    /// [`write`](Self::write), it waits for a write behind in flight, and
    /// fails if one failed.
    pub fn write_all_vectored<V: IoBufs>(&self, bufs: V) -> WriteAllVectored<'_, V> {
        WriteAllVectored::new(self.stream.target(), bufs).behind(&self.behind)
    }

    /// Writes all of the bytes of `bufs` to the peer without waiting for
    /// the system to be done with them; see [`WriteBehind`].
    pub fn write_behind<V: IoBufs>(&self, bufs: V) -> WriteBehind<'_, V> {
        WriteBehind::new(self, bufs)
    }

    /// Waits for a [`write_behind`](Self::write_behind) in flight to finish,
    /// and reports whether one failed; see [`Flush`].
    pub fn flush(&self) -> Flush<'_> {
        Flush::new(&self.behind)
    }

    /// Reports whether a [`write_behind`](Self::write_behind) failed, if
    /// none is in flight, without waiting for one that is: `None` then, as
    /// how it goes is not yet known. Unlike [`flush`](Self::flush), this
    /// arranges for nothing to be woken.
    pub fn try_flush(&self) -> Option<io::Result<()>> {
        self.behind.status()
    }

    /// The data arriving on the connection, received into buffers of
    /// `pool` as it arrives; see [`RecvStream`].
    pub fn recv_stream(&self, pool: &BufPool) -> RecvStream<'_> {
        RecvStream::new(&self.stream, pool)
    }

    /// The standard library's socket, for what needs no waiting.
    fn socket(&self) -> &net::TcpStream {
        &self.stream.io().socket
    }

    /// The address of this end of the connection.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket().local_addr()
    }

    /// The address of the peer.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.socket().peer_addr()
    }

    /// Sets whether small writes are sent at once (`TCP_NODELAY`) rather
    /// than gathered.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        self.socket().set_nodelay(nodelay)
    }

    /// Whether small writes are sent at once.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub fn nodelay(&self) -> io::Result<bool> {
        self.socket().nodelay()
    }

    /// Sets the time to live of the packets the stream sends (`IP_TTL`).
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        self.socket().set_ttl(ttl)
    }

    /// The time to live of the packets the stream sends.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub fn ttl(&self) -> io::Result<u32> {
        self.socket().ttl()
    }

    /// Sets for how long closing the stream waits for what it has left to
    /// send to go (`SO_LINGER`), in whole seconds, or `None` for not at all,
    /// which is the default: the system then sends what is left after the
    /// stream has closed.
    ///
    /// A stream that lingers holds up its thread for up to that long as its
    /// socket closes, and one that lingers for no time resets its
    /// connection instead, discarding what is left.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub fn set_linger(&self, linger: Option<Duration>) -> io::Result<()> {
        set_linger_of(socket_of(self.stream.raw()), linger)
    }

    /// For how long closing the stream waits for what it has left to send
    /// to go, or `None` if it does not.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub fn linger(&self) -> io::Result<Option<Duration>> {
        linger_of(socket_of(self.stream.raw()))
    }

    /// Takes the error the socket holds (`SO_ERROR`), if it holds one,
    /// which clears it.
    ///
    /// # Errors
    ///
    /// What asking for it reports.
    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        self.socket().take_error()
    }

    /// Shuts down the reading or the writing half of the connection, or
    /// both. The peer's reads find the end of the stream once the writing
    /// half is shut down.
    ///
    /// It does not wait for a [`write_behind`](Self::write_behind) in
    /// flight, which shutting down the writing half may cut short:
    /// [`flush`](Self::flush) first.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        self.socket().shutdown(how)
    }
}

impl fmt::Debug for TcpStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpStream")
            .field("stream", self.socket())
            .finish()
    }
}

/// The connecting of a bound socket to the address in its slot.
struct Connect<'a> {
    op: Op<'a, RawAddr>,
}

impl Future for Connect<'_> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (result, _) = ready!(self.op.poll_with(cx, start_connect, outcome));
        Poll::Ready(result.map(|_| ()))
    }
}

/// Starts connecting `socket` to `addr`, as `op`'s `Start` says.
///
/// # Safety
///
/// As for `Start`, and the socket must be bound.
unsafe fn start_connect(
    socket: sys::Handle,
    addr: &mut RawAddr,
    overlapped: *mut sys::Overlapped,
) -> io::Result<Started> {
    let socket = socket_of(socket);
    let connect_ex = extensions(socket)?.connect_ex;
    let mut sent = 0;
    // SAFETY: the socket is open and bound, by the caller's promise; the
    // address and the `OVERLAPPED` are in the slot, which stays in place
    // until the completion is delivered; nothing is sent with the
    // connection; and the count is written before the call returns.
    let ok = unsafe {
        connect_ex(
            socket,
            addr.bytes.as_ptr(),
            addr.len,
            ptr::null(),
            0,
            &mut sent,
            overlapped,
        )
    };
    taken(ok != 0, sent)
}

/// Starts a receive into `buf`, as `op`'s `Start` says.
///
/// # Safety
///
/// As for `Start`.
unsafe fn start_recv<B: IoBufMut>(
    socket: sys::Handle,
    buf: &mut B,
    overlapped: *mut sys::Overlapped,
) -> io::Result<Started> {
    let mut buffer = sys::WsaBuf {
        len: u32::try_from(buf.bytes_total()).unwrap_or(u32::MAX),
        buf: buf.stable_mut_ptr(),
    };
    let mut flags = 0;
    let mut received = 0;
    // SAFETY: the caller's promise for the socket, the buffer and the
    // `OVERLAPPED`; the buffer is writable for that many bytes, as
    // `IoBufMut` promises; and Winsock copies the `WSABUF`, reads the flags
    // and writes the count before the call returns.
    let failed = unsafe {
        sys::WSARecv(
            socket_of(socket),
            &mut buffer,
            1,
            &mut received,
            &mut flags,
            overlapped,
            ptr::null(),
        )
    };
    taken(failed == 0, received)
}

/// Starts a send of `buf`'s bytes, as `op`'s `Start` says.
///
/// # Safety
///
/// As for `Start`.
unsafe fn start_send<B: IoBuf>(
    socket: sys::Handle,
    buf: &mut B,
    overlapped: *mut sys::Overlapped,
) -> io::Result<Started> {
    let buffer = sys::WsaBuf {
        len: u32::try_from(buf.bytes_init()).unwrap_or(u32::MAX),
        buf: buf.stable_ptr().cast_mut(),
    };
    let mut sent = 0;
    // SAFETY: the caller's promise for the socket, the buffer and the
    // `OVERLAPPED`; the buffer holds that many initialized bytes, as `IoBuf`
    // promises, which a send only reads; and Winsock copies the `WSABUF`
    // and writes the count before the call returns.
    let failed = unsafe {
        sys::WSASend(
            socket_of(socket),
            &buffer,
            1,
            &mut sent,
            0,
            overlapped,
            ptr::null(),
        )
    };
    taken(failed == 0, sent)
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::io::{self, Read as _, Write as _};
    use std::net::{self, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};
    use std::pin::pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};
    use std::time::{Duration, Instant};

    use ruio_engine::{Build, Engine};

    use super::{RawAddr, TcpStream, UdpSocket};
    use crate::buf::BufPool;
    use crate::engine::{AT_ONCE, Builder, Iocp, skipped};
    use crate::handle::IoHandle;

    /// A task's waker, which counts how often the task was woken.
    struct Woken(AtomicUsize);

    impl Wake for Woken {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A task that counts its wakes, and its waker.
    fn task() -> (Arc<Woken>, Waker) {
        let woken = Arc::new(Woken(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&woken));
        (woken, waker)
    }

    /// Runs `future` on `engine`, whose port it turns between polls.
    fn block_on<F: Future>(engine: &Iocp, future: F) -> F::Output {
        let mut future = pin!(future);
        let mut cx = Context::from_waker(Waker::noop());
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
                return output;
            }
            assert!(Instant::now() < deadline, "the future did not finish");
            engine.park(Some(Duration::from_millis(1))).unwrap();
        }
    }

    /// Polls `future` once, as the task of no waker, and drops it.
    fn poll_once<F: Future>(future: F) -> Poll<F::Output> {
        pin!(future).poll(&mut Context::from_waker(Waker::noop()))
    }

    /// A stream of `engine`'s, which the thread has entered, connected to a
    /// socket of the standard library's, and that socket; or `None` where
    /// the stream's operations all complete through the port, as under a
    /// provider layered over the system's, and there is nothing to test.
    fn pair(engine: &Iocp) -> io::Result<Option<(TcpStream, net::TcpStream)>> {
        let listener = net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        let stream = block_on(engine, TcpStream::connect(listener.local_addr()?))?;
        let (peer, _) = listener.accept()?;
        if !stream.stream.skips() {
            skipped("a stream's completions all come through the port");
            return Ok(None);
        }
        // The operations counted from here are the test's.
        engine.submit()?;
        Ok(Some((stream, peer)))
    }

    /// How many operations of `engine`'s the port has yet to deliver.
    fn in_flight(engine: &Iocp) -> String {
        let engine = format!("{engine:?}");
        let from = engine.find("in_flight").expect("an engine says so");
        engine[from..].to_owned()
    }

    #[test]
    fn what_has_arrived_is_read_by_the_poll_that_starts_the_read() -> io::Result<()> {
        let engine = Builder::new().build()?;
        let _entered = engine.enter();
        let Some((stream, mut peer)) = pair(&engine)? else {
            return Ok(());
        };
        peer.write_all(b"hello")?;
        // Until it has arrived.
        assert_eq!(stream.socket().peek(&mut [0])?, 1);

        let Poll::Ready((read, buf)) = poll_once(stream.read(Vec::with_capacity(16))) else {
            panic!("a read of what had arrived waited for the port");
        };
        assert_eq!(read?, 5);
        assert_eq!(buf, b"hello");
        // Nothing is left for the port to deliver.
        assert!(in_flight(&engine).starts_with("in_flight: 0,"));

        // The end of the stream is read the same way.
        drop(peer);
        assert_eq!(stream.socket().peek(&mut [0])?, 0);
        let Poll::Ready((read, _)) = poll_once(stream.read(Vec::with_capacity(16))) else {
            panic!("a read of a stream that had ended waited for the port");
        };
        assert_eq!(read?, 0);
        Ok(())
    }

    #[test]
    fn only_so_many_writes_finish_as_they_start_between_two_turns() -> io::Result<()> {
        let engine = Builder::new().build()?;
        let _entered = engine.enter();
        let Some((stream, mut peer)) = pair(&engine)? else {
            return Ok(());
        };
        // The system's buffer takes each, which finishes it there and then.
        for _ in 0..AT_ONCE {
            let Poll::Ready((written, _)) = poll_once(stream.write(vec![7])) else {
                panic!("a write that the system's buffer takes waited for the port");
            };
            assert_eq!(written?, 1);
        }
        assert!(in_flight(&engine).starts_with("in_flight: 0,"));

        // The next waits for the port to be turned, and is not started: its
        // task is woken to try again, which it does in vain until the turn.
        let (woken, waker) = task();
        let mut cx = Context::from_waker(&waker);
        let mut write = pin!(stream.write(vec![8]));
        assert!(write.as_mut().poll(&mut cx).is_pending());
        assert_eq!(woken.0.load(Ordering::Relaxed), 1);
        assert!(write.as_mut().poll(&mut cx).is_pending());
        assert_eq!(woken.0.load(Ordering::Relaxed), 2);
        assert!(in_flight(&engine).starts_with("in_flight: 0,"));
        engine.submit()?;
        let Poll::Ready((written, _)) = write.as_mut().poll(&mut cx) else {
            panic!("a write waited for more than a turn of the port");
        };
        assert_eq!(written?, 1);

        let mut arrived = vec![0; AT_ONCE as usize + 1];
        peer.read_exact(&mut arrived)?;
        assert_eq!(arrived[AT_ONCE as usize - 1..], [7, 8]);
        Ok(())
    }

    #[test]
    fn a_write_behind_that_the_buffer_takes_is_finished_as_it_starts() -> io::Result<()> {
        let engine = Builder::new().build()?;
        let _entered = engine.enter();
        let Some((stream, mut peer)) = pair(&engine)? else {
            return Ok(());
        };
        for _ in 0..AT_ONCE {
            let sent = poll_once(stream.write_behind(vec![vec![7_u8]]));
            assert!(matches!(sent, Poll::Ready(Ok(()))));
            // How it went is known with no turn of the port.
            assert!(matches!(stream.try_flush(), Some(Ok(()))));
        }
        assert!(in_flight(&engine).starts_with("in_flight: 0,"));

        // A task that only writes behind yields to the port all the same.
        let (woken, waker) = task();
        let mut cx = Context::from_waker(&waker);
        let mut behind = pin!(stream.write_behind(vec![vec![8_u8]]));
        assert!(behind.as_mut().poll(&mut cx).is_pending());
        assert_eq!(woken.0.load(Ordering::Relaxed), 1);
        engine.submit()?;
        assert!(matches!(behind.as_mut().poll(&mut cx), Poll::Ready(Ok(()))));
        assert!(matches!(stream.try_flush(), Some(Ok(()))));

        let mut arrived = vec![0; AT_ONCE as usize + 1];
        peer.read_exact(&mut arrived)?;
        assert_eq!(arrived[AT_ONCE as usize - 1..], [7, 8]);
        Ok(())
    }

    #[test]
    fn a_stream_that_receives_without_the_port_yields_to_it_all_the_same() -> io::Result<()> {
        let engine = Builder::new().build()?;
        let _entered = engine.enter();
        let Some((stream, mut peer)) = pair(&engine)? else {
            return Ok(());
        };
        // More bytes than the stream may receive between two turns, into
        // buffers of one byte, all of them arrived.
        let sent = 2 * AT_ONCE as usize;
        peer.write_all(&vec![7; sent])?;
        let deadline = Instant::now() + Duration::from_secs(10);
        while stream.socket().peek(&mut vec![0; sent])? < sent {
            assert!(Instant::now() < deadline, "the data did not arrive");
            std::thread::sleep(Duration::from_millis(1));
        }
        let pool = BufPool::new(1, 1)?;
        let mut received = stream.recv_stream(&pool);

        // Each receive takes what has arrived without the port, until as
        // many operations have finished so as may between two turns.
        let (woken, waker) = task();
        let mut cx = Context::from_waker(&waker);
        let mut items = 0;
        while let Poll::Ready(item) = received.poll_next(&mut cx) {
            item.expect("data that has arrived")?;
            items += 1;
            assert!(items <= AT_ONCE, "a stream with data never yielded");
        }
        assert!(items > 0);
        // Then the stream has its task woken, to try again after the turn.
        assert_eq!(woken.0.load(Ordering::Relaxed), 1);
        assert!(in_flight(&engine).starts_with("in_flight: 0,"));
        engine.submit()?;
        let Poll::Ready(Some(item)) = received.poll_next(&mut cx) else {
            panic!("a stream with data waited for more than a turn of the port");
        };
        item?;
        Ok(())
    }

    #[test]
    fn datagrams_received_without_the_port_yield_to_it_all_the_same() -> io::Result<()> {
        let engine = Builder::new().build()?;
        let _entered = engine.enter();
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?;
        let peer = net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?;
        // More datagrams than the stream may receive between two turns, all
        // of them arrived.
        for _ in 0..2 * AT_ONCE {
            assert_eq!(peer.send_to(&[7], socket.local_addr()?)?, 1);
        }
        std::thread::sleep(Duration::from_millis(50));
        let pool = BufPool::new(1, 64)?;
        let mut datagrams = socket.recv_from_stream(&pool)?;
        // The wait for the first goes through the port.
        let first = block_on(&engine, datagrams.next()).expect("a datagram")?;
        assert_eq!((&*first, first.from()), (&[7][..], peer.local_addr()?));
        drop(first);
        engine.submit()?;

        // Each of the rest is received without the port, until as many
        // operations have finished so as may between two turns.
        let (woken, waker) = task();
        let mut cx = Context::from_waker(&waker);
        let mut items = 0;
        while let Poll::Ready(item) = datagrams.poll_next(&mut cx) {
            assert_eq!(*item.expect("a datagram that has arrived")?, [7]);
            items += 1;
            assert!(items <= AT_ONCE, "a stream with datagrams never yielded");
        }
        assert_eq!(items, AT_ONCE);
        // Then the stream has its task woken, to try again after the turn.
        assert_eq!(woken.0.load(Ordering::Relaxed), 1);
        assert!(in_flight(&engine).starts_with("in_flight: 0,"));
        engine.submit()?;
        let Poll::Ready(Some(item)) = datagrams.poll_next(&mut cx) else {
            panic!("a stream with datagrams waited for more than a turn of the port");
        };
        item?;
        Ok(())
    }

    #[test]
    fn a_socket_leaves_its_port_once_the_port_has_delivered_its_operations() -> io::Result<()> {
        let engine = Builder::new().build()?;
        let _entered = engine.enter();
        let listener = net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        let stream = block_on(&engine, TcpStream::connect(listener.local_addr()?))?;
        let (mut peer, _) = listener.accept()?;
        engine.submit()?;
        // A read that waits for data, cancelled as its future is dropped.
        assert!(poll_once(stream.read(Vec::with_capacity(8))).is_pending());
        let Err((err, handle)) = stream.stream.release() else {
            panic!("a socket left its port with a read in flight");
        };
        assert_eq!(err.kind(), io::ErrorKind::ResourceBusy);
        // The cancellation completes at once, but its completion waits in
        // the port, and the read with it, until a turn takes it.
        std::thread::sleep(Duration::from_millis(20));
        let Err((err, handle)) = handle.release() else {
            panic!("a socket left its port before the port delivered its read");
        };
        assert_eq!(err.kind(), io::ErrorKind::ResourceBusy);
        let deadline = Instant::now() + Duration::from_secs(10);
        while !in_flight(&engine).starts_with("in_flight: 0,") {
            assert!(Instant::now() < deadline, "the cancellation never came");
            engine.park(Some(Duration::from_millis(1)))?;
        }
        let socket = handle.release().map_err(|(err, _)| err)?;

        // Another engine's port takes the socket, and its operations
        // complete there.
        let other = Builder::new().build()?;
        let moved = TcpStream::new(IoHandle::on(socket, other.clone())?);
        peer.write_all(b"moved")?;
        let (read, buf) = block_on(&other, moved.read(Vec::with_capacity(8)));
        assert_eq!(read?, 5);
        assert_eq!(buf, b"moved");
        assert!(in_flight(&engine).starts_with("in_flight: 0,"));
        Ok(())
    }

    #[test]
    fn addresses_are_laid_out_as_winsock_reads_them() {
        let v4 = RawAddr::new(&"127.0.0.1:8080".parse().unwrap());
        assert_eq!(v4.len, 16);
        // AF_INET, the port in network order, the address, and padding.
        assert_eq!(v4.bytes[..8], [2, 0, 0x1f, 0x90, 127, 0, 0, 1]);
        assert_eq!(v4.bytes[8..], [0; 20]);

        let addr = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 443, 0, 7);
        let v6 = RawAddr::new(&SocketAddr::V6(addr));
        assert_eq!(v6.len, 28);
        // AF_INET6, the port in network order, and no flow information.
        assert_eq!(v6.bytes[..8], [23, 0, 0x01, 0xbb, 0, 0, 0, 0]);
        assert_eq!(v6.bytes[8..24], Ipv6Addr::LOCALHOST.octets());
        assert_eq!(v6.bytes[24..], 7_u32.to_ne_bytes());
    }

    #[test]
    fn a_socket_binds_to_any_address_of_its_peers_family() {
        let v4 = RawAddr::any_for(&"192.0.2.1:80".parse().unwrap());
        assert_eq!((v4.len, v4.bytes[0]), (16, 2));
        assert_eq!(v4.bytes[2..], [0; 26]);
        let v6 = RawAddr::any_for(&"[2001:db8::1]:80".parse().unwrap());
        assert_eq!((v6.len, v6.bytes[0]), (28, 23));
        assert_eq!(v6.bytes[2..], [0; 26]);
    }

    #[test]
    fn an_address_the_system_wrote_reads_back() {
        for addr in ["127.0.0.1:8080", "[::1]:443", "[fe80::1%7]:9"] {
            let addr: SocketAddr = addr.parse().unwrap();
            assert_eq!(RawAddr::new(&addr).to_socket_addr().unwrap(), addr);
        }
        let flow = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 1, 0x1234, 0);
        let read = RawAddr::new(&SocketAddr::V6(flow)).to_socket_addr();
        assert_eq!(read.unwrap(), SocketAddr::V6(flow));
        // Nothing written: no family.
        assert!(RawAddr::room().to_socket_addr().is_err());
    }

    #[test]
    fn a_read_that_has_to_wait_still_completes_through_the_port() -> io::Result<()> {
        let engine = Builder::new().build()?;
        let _entered = engine.enter();
        let Some((stream, mut peer)) = pair(&engine)? else {
            return Ok(());
        };
        let (woken, waker) = task();
        let mut cx = Context::from_waker(&waker);
        let mut read = pin!(stream.read(Vec::with_capacity(16)));
        // Nothing has arrived, so the system keeps the read.
        assert!(read.as_mut().poll(&mut cx).is_pending());
        assert!(in_flight(&engine).starts_with("in_flight: 1,"));
        peer.write_all(b"later")?;
        let deadline = Instant::now() + Duration::from_secs(10);
        while woken.0.load(Ordering::Relaxed) == 0 && Instant::now() < deadline {
            engine.park(Some(Duration::from_millis(5)))?;
        }
        assert_eq!(woken.0.load(Ordering::Relaxed), 1);
        let Poll::Ready((read, buf)) = read.as_mut().poll(&mut cx) else {
            panic!("a read whose completion was delivered was not ready");
        };
        assert_eq!(read?, 5);
        assert_eq!(buf, b"later");
        assert!(in_flight(&engine).starts_with("in_flight: 0,"));
        Ok(())
    }
}
