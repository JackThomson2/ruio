//! TCP and UDP sockets, which accept, connect, read and write, send and
//! receive through the port.
//!
//! A listener accepts with `AcceptEx` and a stream connects with
//! `ConnectEx`, the calls of Winsock that complete to a port, which a
//! socket's provider hands out as function pointers; a stream reads with
//! `WSARecv` and writes with `WSASend`. An operation's outcome is read with
//! `WSAGetOverlappedResult`, which gives Winsock's own errors, those the
//! standard library's sockets report. What needs no waiting, binding a
//! listener, addresses and options, goes through the standard library's
//! sockets, which the types here hold.

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
use std::sync::OnceLock;
use std::task::{Context, Poll, ready};

use ruio_buf::{IoBuf, IoBufMut, Slice};

use crate::handle::{IoHandle, Raw};
use crate::op::{Op, Read, ReadExact, Write, WriteAll};
use crate::sys;

mod udp;

pub use udp::{RecvFrom, SendTo, UdpSocket};

/// Room for one of the two addresses `AcceptEx` writes: the largest address
/// there is, and the 16 bytes more that it asks for.
const ADDR_ROOM: usize = 128 + 16;

impl Raw for net::TcpListener {
    fn raw(&self) -> sys::Handle {
        handle_of(self.as_raw_socket())
    }
}

impl Raw for net::TcpStream {
    fn raw(&self) -> sys::Handle {
        handle_of(self.as_raw_socket())
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
fn last_error() -> io::Error {
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
fn family(addr: &SocketAddr) -> u16 {
    if addr.is_ipv6() {
        sys::AF_INET6
    } else {
        sys::AF_INET
    }
}

/// A TCP socket of `family`, unbound, opened for overlapped I/O and not
/// inherited by child processes, as the standard library's are.
fn new_socket(family: u16) -> io::Result<OwnedSocket> {
    startup()?;
    // SAFETY: the call is given no protocol information, and so no memory.
    let socket = unsafe {
        sys::WSASocketW(
            i32::from(family),
            sys::SOCK_STREAM,
            sys::IPPROTO_TCP,
            ptr::null(),
            0,
            sys::WSA_FLAG_OVERLAPPED | sys::WSA_FLAG_NO_HANDLE_INHERIT,
        )
    };
    if socket == sys::INVALID_SOCKET {
        return Err(last_error());
    }
    // SAFETY: the socket was just opened, and nothing else owns it.
    Ok(unsafe { OwnedSocket::from_raw_socket(socket as RawSocket) })
}

/// A socket address as Winsock takes it: the bytes of a `sockaddr_in` or of
/// a `sockaddr_in6`, and how many they are.
#[repr(C, align(4))]
struct RawAddr {
    bytes: [u8; 28],
    len: i32,
}

impl RawAddr {
    fn new(addr: &SocketAddr) -> Self {
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
    let (Some(accept_ex), Some(connect_ex)) = (accept_ex, connect_ex) else {
        return Err(io::Error::other("Winsock has no AcceptEx or ConnectEx"));
    };
    Ok(EXTENSIONS.get_or_init(|| Extensions {
        accept_ex,
        connect_ex,
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

/// Whether Winsock took an operation whose call `succeeded` or did not. A
/// call that succeeded completed the operation at once, and its completion
/// is queued to the port all the same.
fn taken(succeeded: bool) -> io::Result<()> {
    if succeeded {
        return Ok(());
    }
    match sys::WSAGetLastError() {
        sys::WSA_IO_PENDING => Ok(()),
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
    /// Binds a listener to `addr`, as `std::net::TcpListener::bind` does:
    /// a host name is resolved on the calling thread, which waits for it.
    ///
    /// # Errors
    ///
    /// What binding reports, as when the address is in use, and an error of
    /// kind `Other` if the thread has no current engine, as outside a
    /// runtime.
    pub fn bind(addr: impl ToSocketAddrs) -> io::Result<Self> {
        let listener = net::TcpListener::bind(addr)?;
        let family = family(&listener.local_addr()?);
        Ok(Self {
            listener: IoHandle::new(listener)?,
            family,
        })
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
    fn accepted(&self, accepting: Accepting) -> io::Result<(TcpStream, SocketAddr)> {
        let socket = accepting
            .socket
            .expect("an accept that completed made its socket");
        let listener = socket_of(self.listener.raw());
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
        let peer = stream.peer_addr()?;
        let stream = IoHandle::on(stream, self.listener.engine().clone())?;
        Ok((TcpStream { stream }, peer))
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
    /// Where the system writes the connection's two addresses. It needs the
    /// room, though nothing reads them: the stream is asked for its peer's.
    addrs: [u8; 2 * ADDR_ROOM],
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
) -> io::Result<()> {
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
    taken(ok != 0)
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
pub struct TcpStream {
    stream: IoHandle<net::TcpStream>,
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
        let stream = IoHandle::new(net::TcpStream::from(new_socket(family(&addr))?))?;
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
        Ok(Self { stream })
    }

    /// Reads into `buf`, up to its `bytes_total`, what the peer has sent;
    /// see [`Read`].
    pub fn read<B: IoBufMut>(&self, buf: B) -> Read<'_, B> {
        Read::with(self.stream.target(), buf, 0, start_recv::<B>, outcome)
    }

    /// Writes `buf`'s bytes to the peer; see [`Write`].
    pub fn write<B: IoBuf>(&self, buf: B) -> Write<'_, B> {
        Write::with(self.stream.target(), buf, 0, start_send::<B>, outcome)
    }

    /// Fills `buf` from its start, up to its `bytes_total`, with what the
    /// peer sends, in as many reads as that takes; see [`ReadExact`].
    pub fn read_exact<B: IoBufMut>(&self, buf: B) -> ReadExact<'_, B> {
        let target = self.stream.target();
        ReadExact::with(target, buf, 0, 0, start_recv::<Slice<B>>, outcome)
    }

    /// Writes all of `buf`'s bytes to the peer, in as many writes as that
    /// takes; see [`WriteAll`].
    pub fn write_all<B: IoBuf>(&self, buf: B) -> WriteAll<'_, B> {
        let target = self.stream.target();
        WriteAll::with(target, buf, 0, start_send::<Slice<B>>, outcome)
    }

    /// The address of this end of the connection.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.stream.io().local_addr()
    }

    /// The address of the peer.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.stream.io().peer_addr()
    }

    /// Sets whether small writes are sent at once (`TCP_NODELAY`) rather
    /// than gathered.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        self.stream.io().set_nodelay(nodelay)
    }

    /// Whether small writes are sent at once.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub fn nodelay(&self) -> io::Result<bool> {
        self.stream.io().nodelay()
    }

    /// Sets the time to live of the packets the stream sends (`IP_TTL`).
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        self.stream.io().set_ttl(ttl)
    }

    /// The time to live of the packets the stream sends.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub fn ttl(&self) -> io::Result<u32> {
        self.stream.io().ttl()
    }

    /// Takes the error the socket holds (`SO_ERROR`), if it holds one,
    /// which clears it.
    ///
    /// # Errors
    ///
    /// What asking for it reports.
    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        self.stream.io().take_error()
    }

    /// Shuts down the reading or the writing half of the connection, or
    /// both. The peer's reads find the end of the stream once the writing
    /// half is shut down.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        self.stream.io().shutdown(how)
    }
}

impl fmt::Debug for TcpStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpStream")
            .field("stream", self.stream.io())
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
) -> io::Result<()> {
    let socket = socket_of(socket);
    let connect_ex = extensions(socket)?.connect_ex;
    // SAFETY: the socket is open and bound, by the caller's promise; the
    // address and the `OVERLAPPED` are in the slot, which stays in place
    // until the completion is delivered; and nothing is sent with the
    // connection.
    let ok = unsafe {
        connect_ex(
            socket,
            addr.bytes.as_ptr(),
            addr.len,
            ptr::null(),
            0,
            ptr::null_mut(),
            overlapped,
        )
    };
    taken(ok != 0)
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
) -> io::Result<()> {
    let mut buffer = sys::WsaBuf {
        len: u32::try_from(buf.bytes_total()).unwrap_or(u32::MAX),
        buf: buf.stable_mut_ptr(),
    };
    let mut flags = 0;
    // SAFETY: the caller's promise for the socket, the buffer and the
    // `OVERLAPPED`; the buffer is writable for that many bytes, as
    // `IoBufMut` promises; and Winsock copies the `WSABUF` and reads the
    // flags before the call returns.
    let failed = unsafe {
        sys::WSARecv(
            socket_of(socket),
            &mut buffer,
            1,
            ptr::null_mut(),
            &mut flags,
            overlapped,
            ptr::null(),
        )
    };
    taken(failed == 0)
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
) -> io::Result<()> {
    let buffer = sys::WsaBuf {
        len: u32::try_from(buf.bytes_init()).unwrap_or(u32::MAX),
        buf: buf.stable_ptr().cast_mut(),
    };
    // SAFETY: the caller's promise for the socket, the buffer and the
    // `OVERLAPPED`; the buffer holds that many initialized bytes, as `IoBuf`
    // promises, which a send only reads; and Winsock copies the `WSABUF`
    // before the call returns.
    let failed = unsafe {
        sys::WSASend(
            socket_of(socket),
            &buffer,
            1,
            ptr::null_mut(),
            0,
            overlapped,
            ptr::null(),
        )
    };
    taken(failed == 0)
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};

    use super::RawAddr;

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
}
