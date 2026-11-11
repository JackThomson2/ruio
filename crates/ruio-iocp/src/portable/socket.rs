//! A TCP socket that no port has, which any thread may read and write with
//! calls that do not wait, as a nonblocking descriptor is read on Unix.
//!
//! A [`Socket`] is made nonblocking, so that a receive takes what has
//! arrived, or fails with `WouldBlock` if nothing has, and a send gives
//! what there is room for, or fails the same way if there is none. Its
//! calls are Winsock's own, `WSARecv` and `WSASend` given no `OVERLAPPED`,
//! which complete during the call, on whichever thread makes it, with no
//! port. A caller that has to wait waits with a [`ReadyWait`] for the
//! socket to be ready, through the AFD driver's poll of its base socket,
//! which the socket finds as it is made.
//!
//! Connecting is a `connect` made at once, which starts the connection and
//! fails with `WouldBlock`, unless it connects at once: the socket is then
//! writable once the connection is made or has failed, which `SO_ERROR`
//! says. Accepting is an `accept`, made at once, which fails with
//! `WouldBlock` while no connection waits. What needs no waiting besides,
//! addresses and options, goes through the standard library's sockets.

use std::fmt;
use std::io::{self, IoSlice};
use std::mem::{ManuallyDrop, MaybeUninit};
use std::net::{self, SocketAddr, ToSocketAddrs};
use std::os::windows::io::{
    AsRawSocket, AsSocket, BorrowedSocket, FromRawSocket, OwnedSocket, RawSocket,
};
use std::ptr;
use std::slice;
use std::task::{Context, Poll};
use std::time::Duration;

use super::{ReadyWait, base_socket};
use crate::net::{RawAddr, family, last_error, linger_of, listening, new_socket, set_linger_of};
use crate::sys;

/// A TCP socket, made nonblocking and associated with no port, read and
/// written with calls that do not wait, from any thread; see the
/// [module](self).
pub struct Socket {
    socket: OwnedSocket,
    /// The base socket beneath any layered provider's, which the AFD
    /// driver's polls name.
    base: RawSocket,
}

impl Socket {
    /// Takes over `socket`, a TCP socket, made nonblocking with one
    /// `ioctlsocket`, which changes the socket, and so every handle of it.
    ///
    /// # Errors
    ///
    /// What making it nonblocking reports.
    pub fn new(socket: OwnedSocket) -> io::Result<Self> {
        set_nonblocking(&socket, true)?;
        let base = base_socket(socket.as_socket());
        Ok(Self { socket, base })
    }

    /// A TCP socket of `addr`'s family, which starts connecting to `addr`
    /// with a `connect` made at once, and whether it connected at once.
    /// One that did not is writable once the connection is made or has
    /// failed, and holds the error it failed with as `SO_ERROR`.
    ///
    /// # Errors
    ///
    /// What making the socket and `connect` report, but `WouldBlock`.
    pub fn connect(addr: &SocketAddr) -> io::Result<(Self, bool)> {
        let socket = Self::new(new_socket(family(addr))?)?;
        let raw = RawAddr::new(addr);
        // SAFETY: the socket is open, and the address is read during the
        // call.
        let failed = unsafe { sys::connect(socket.raw(), raw.bytes.as_ptr(), raw.len) };
        if failed == 0 {
            return Ok((socket, true));
        }
        let err = last_error();
        if err.raw_os_error() == Some(sys::WSAEWOULDBLOCK) {
            return Ok((socket, false));
        }
        Err(err)
    }

    /// A TCP socket bound to `addr`, or to the first address it resolves to
    /// that it can be bound to, and listening, as the crate's
    /// [`TcpListener`](crate::net::TcpListener) binds: a host name is
    /// resolved on the calling thread, which waits for it, and one on an
    /// IPv6 address accepts connections from IPv4 addresses too.
    ///
    /// # Errors
    ///
    /// What binding and listening report, or `InvalidInput` if `addr`
    /// resolves to no address.
    pub fn listen(addr: impl ToSocketAddrs) -> io::Result<Self> {
        Self::new(listening(addr)?)
    }

    /// The socket as Winsock's calls take it.
    fn raw(&self) -> sys::Socket {
        self.socket.as_raw_socket() as sys::Socket
    }

    /// Receives into `buf` what has arrived, with one `WSARecv` made at
    /// once: how many bytes, of which the first of `buf` are then
    /// initialized; 0 at the end, as once the peer has shut its writing
    /// half down, or this end its reading half; or `WouldBlock` if nothing
    /// has arrived.
    ///
    /// # Errors
    ///
    /// `WouldBlock`, and what Winsock reports, as for a connection reset.
    pub fn try_read(&self, buf: &mut [MaybeUninit<u8>]) -> io::Result<usize> {
        let mut wsabuf = sys::WsaBuf {
            len: clamp(buf.len()),
            buf: buf.as_mut_ptr().cast(),
        };
        let mut received = 0;
        let mut flags = 0;
        // SAFETY: the socket is open; the call takes no `OVERLAPPED`, so it
        // completes during the call, writing at most `len` bytes into the
        // buffer, which has room for them, and how many to `received`.
        let failed = unsafe {
            sys::WSARecv(
                self.raw(),
                &mut wsabuf,
                1,
                &mut received,
                &mut flags,
                ptr::null_mut(),
                ptr::null(),
            )
        };
        if failed == 0 {
            return Ok(received as usize);
        }
        let err = last_error();
        if err.raw_os_error() == Some(sys::WSAESHUTDOWN) {
            // Shut down for reading, which reads as the end, as the
            // standard library reads it.
            return Ok(0);
        }
        Err(err)
    }

    /// Sends as much of `buf` as there is room for, with one `WSASend` made
    /// at once: how many bytes, or `WouldBlock` if there is no room.
    ///
    /// # Errors
    ///
    /// `WouldBlock`, and what Winsock reports, as for a peer that has gone.
    pub fn try_write(&self, buf: &[u8]) -> io::Result<usize> {
        let wsabuf = sys::WsaBuf {
            len: clamp(buf.len()),
            buf: buf.as_ptr().cast_mut(),
        };
        self.send(slice::from_ref(&wsabuf))
    }

    /// Sends as much of `bufs`, one after the other, as there is room for,
    /// with one `WSASend` made at once, as [`try_write`](Self::try_write)
    /// sends one buffer.
    ///
    /// # Errors
    ///
    /// As for [`try_write`](Self::try_write).
    pub fn try_write_vectored(&self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        if bufs.is_empty() {
            return self.try_write(&[]);
        }
        // SAFETY: an `IoSlice` is a `WSABUF` on Windows, as the standard
        // library documents, so the slices are as many `WSABUF`s, borrowed
        // for as long as they are.
        let wsabufs = unsafe { slice::from_raw_parts(bufs.as_ptr().cast(), bufs.len()) };
        self.send(wsabufs)
    }

    /// Sends the bytes `bufs` name, which stay readable while they are
    /// borrowed, with one `WSASend` made at once.
    fn send(&self, bufs: &[sys::WsaBuf]) -> io::Result<usize> {
        let count = clamp(bufs.len());
        let mut sent = 0;
        // SAFETY: the socket is open; the call takes no `OVERLAPPED`, so it
        // completes during the call, reading the first `count` buffers and
        // the bytes each names, and writing how many it sent to `sent`.
        let failed = unsafe {
            sys::WSASend(
                self.raw(),
                bufs.as_ptr(),
                count,
                &mut sent,
                0,
                ptr::null_mut(),
                ptr::null(),
            )
        };
        if failed != 0 {
            return Err(last_error());
        }
        Ok(sent as usize)
    }

    /// Takes a connection from the listening socket with one `accept`, made
    /// at once: the connection's socket, made nonblocking, and the peer's
    /// address; or `WouldBlock` if no connection waits. A connection that
    /// the peer reset before it was taken is passed over for the next.
    ///
    /// # Errors
    ///
    /// `WouldBlock`, and what Winsock reports.
    pub fn try_accept(&self) -> io::Result<(Self, SocketAddr)> {
        loop {
            match self.with_listener(net::TcpListener::accept) {
                Ok((stream, peer)) => return Ok((Self::new(stream.into())?, peer)),
                Err(err) if err.raw_os_error() == Some(sys::WSAECONNRESET) => {}
                Err(err) => return Err(err),
            }
        }
    }

    /// Runs `f` on the standard library's view of the socket as a listener.
    fn with_listener<R>(&self, f: impl FnOnce(&net::TcpListener) -> R) -> R {
        // SAFETY: the socket is open for as long as `self` is, which the
        // view does not outlive; the view is never dropped, so it closes
        // nothing.
        let view = ManuallyDrop::new(unsafe {
            net::TcpListener::from_raw_socket(self.socket.as_raw_socket())
        });
        f(&view)
    }

    /// Waits with `wait` for the socket to have any of `events`, the bits of
    /// [`events`](super::events), as [`ReadyWait::poll_ready`] does, polling
    /// the socket's base socket.
    ///
    /// # Errors
    ///
    /// As for [`ReadyWait::poll_ready`].
    pub fn poll_ready(
        &self,
        cx: &mut Context<'_>,
        events: u32,
        wait: &mut ReadyWait,
    ) -> Poll<io::Result<u32>> {
        // SAFETY: the base socket is the socket itself, or the one beneath
        // it, which is open for as long as the socket is, and so for the
        // borrow.
        let base = unsafe { BorrowedSocket::borrow_raw(self.base) };
        wait.poll_ready(cx, base, events)
    }

    /// Sets `SO_LINGER`: for how long closing the socket waits for what it
    /// has left to send to go, in whole seconds, or `None` for not at all,
    /// which is the default.
    ///
    /// # Errors
    ///
    /// What Winsock reports.
    pub fn set_linger(&self, linger: Option<Duration>) -> io::Result<()> {
        set_linger_of(self.raw(), linger)
    }

    /// `SO_LINGER`.
    ///
    /// # Errors
    ///
    /// What Winsock reports.
    pub fn linger(&self) -> io::Result<Option<Duration>> {
        linger_of(self.raw())
    }

    /// The socket, blocking again, as the standard library's are.
    pub fn into_socket(self) -> OwnedSocket {
        // Only a handle that is no socket fails, which this is not.
        let _ = set_nonblocking(&self.socket, false);
        self.socket
    }
}

impl AsSocket for Socket {
    #[inline]
    fn as_socket(&self) -> BorrowedSocket<'_> {
        self.socket.as_socket()
    }
}

impl AsRawSocket for Socket {
    #[inline]
    fn as_raw_socket(&self) -> RawSocket {
        self.socket.as_raw_socket()
    }
}

impl fmt::Debug for Socket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Socket")
            .field("socket", &self.socket.as_raw_socket())
            .field("base", &self.base)
            .finish()
    }
}

/// Makes `socket` nonblocking, or blocking again, with one `ioctlsocket`.
fn set_nonblocking(socket: &OwnedSocket, nonblocking: bool) -> io::Result<()> {
    let mut on = u32::from(nonblocking);
    // SAFETY: the socket is open, and FIONBIO reads the one `u_long` it is
    // given.
    let failed = unsafe {
        sys::ioctlsocket(
            socket.as_raw_socket() as sys::Socket,
            sys::FIONBIO,
            &raw mut on,
        )
    };
    if failed != 0 {
        return Err(last_error());
    }
    Ok(())
}

/// A length as Winsock takes it, at most `u32::MAX`: a buffer longer is
/// read or written in part, as a call may be anyway.
fn clamp(len: usize) -> u32 {
    u32::try_from(len).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use std::io::{Read as _, Write as _};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Wake, Waker};
    use std::time::Instant;

    use ruio_engine::{Build, Engine};

    use super::super::events;
    use super::*;
    use crate::engine::{Builder, Iocp};

    struct Woken(AtomicUsize);

    impl Wake for Woken {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Turns `engine` until `wait`'s poll of `socket` for `events` is
    /// ready, for up to five seconds, and gives what it found.
    fn ready(engine: &Iocp, socket: &Socket, wait: &mut ReadyWait, events: u32) -> u32 {
        let woken = Arc::new(Woken(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&woken));
        let mut cx = Context::from_waker(&waker);
        let started = Instant::now();
        loop {
            if let Poll::Ready(found) = socket.poll_ready(&mut cx, events, wait) {
                return found.unwrap();
            }
            while woken.0.swap(0, Ordering::SeqCst) == 0 {
                assert!(started.elapsed() < Duration::from_secs(5), "never ready");
                engine.park(Some(Duration::from_millis(50))).unwrap();
            }
        }
    }

    /// The standard library's view of `socket` as a stream.
    fn with_stream<R>(socket: &Socket, f: impl FnOnce(&net::TcpStream) -> R) -> R {
        // SAFETY: the socket is open for as long as `socket` is, which the
        // view does not outlive; the view is never dropped, so it closes
        // nothing.
        let view =
            ManuallyDrop::new(unsafe { net::TcpStream::from_raw_socket(socket.as_raw_socket()) });
        f(&view)
    }

    /// A connected pair: a socket of this module's, and the standard
    /// library's peer.
    fn pair() -> (Socket, net::TcpStream) {
        let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
        let peer = net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (ours, _) = listener.accept().unwrap();
        (Socket::new(ours.into()).unwrap(), peer)
    }

    #[test]
    fn a_socket_reads_what_has_arrived_and_would_block_otherwise() {
        let engine = Builder::new().build().unwrap();
        let _entered = engine.enter();
        let (ours, mut peer) = pair();
        let mut buf = [MaybeUninit::uninit(); 16];
        let err = ours.try_read(&mut buf).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
        peer.write_all(b"hello").unwrap();
        let mut wait = ReadyWait::new();
        let found = ready(&engine, &ours, &mut wait, events::READABLE);
        assert_ne!(found & events::RECEIVE, 0, "found {found:#x}");
        assert_eq!(ours.try_read(&mut buf).unwrap(), 5);
        let bufs = [IoSlice::new(b"one "), IoSlice::new(b"two")];
        assert_eq!(ours.try_write_vectored(&bufs).unwrap(), 7);
        assert_eq!(ours.try_write(b"!").unwrap(), 1);
        let mut got = [0; 8];
        peer.read_exact(&mut got).unwrap();
        assert_eq!(&got, b"one two!");
        drop(peer);
        ready(&engine, &ours, &mut wait, events::READABLE);
        assert_eq!(ours.try_read(&mut buf).unwrap(), 0, "the end");
    }

    #[test]
    fn a_listener_accepts_once_a_connection_waits() {
        let engine = Builder::new().build().unwrap();
        let _entered = engine.enter();
        let listener = Socket::listen("127.0.0.1:0").unwrap();
        let addr = listener
            .with_listener(net::TcpListener::local_addr)
            .unwrap();
        let err = listener.try_accept().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
        let (client, connected) = Socket::connect(&addr).unwrap();
        let mut wait = ReadyWait::new();
        if !connected {
            ready(&engine, &client, &mut wait, events::WRITABLE);
        }
        assert!(
            with_stream(&client, net::TcpStream::take_error)
                .unwrap()
                .is_none()
        );
        ready(&engine, &listener, &mut wait, events::READABLE);
        let (server, peer) = listener.try_accept().unwrap();
        let mut buf = [MaybeUninit::uninit(); 4];
        let err = server.try_read(&mut buf).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock, "made nonblocking");
        let local = with_stream(&client, net::TcpStream::local_addr).unwrap();
        assert_eq!(peer, local);
    }

    #[test]
    fn a_connection_refused_is_writable_and_holds_its_error() {
        let engine = Builder::new().build().unwrap();
        let _entered = engine.enter();
        let addr = {
            let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap()
        };
        let (client, connected) = Socket::connect(&addr).unwrap();
        assert!(!connected);
        let mut wait = ReadyWait::new();
        let found = ready(&engine, &client, &mut wait, events::WRITABLE);
        assert_ne!(found & events::CONNECT_FAIL, 0, "found {found:#x}");
        let error = with_stream(&client, net::TcpStream::take_error).unwrap();
        let error = error.expect("the connection's error");
        assert_eq!(error.kind(), io::ErrorKind::ConnectionRefused);
    }

    #[test]
    fn the_linger_is_set_and_read_and_the_socket_given_back() {
        let (ours, peer) = pair();
        assert_eq!(ours.linger().unwrap(), None);
        ours.set_linger(Some(Duration::from_secs(3))).unwrap();
        assert_eq!(ours.linger().unwrap(), Some(Duration::from_secs(3)));
        let mut blocking = net::TcpStream::from(ours.into_socket());
        let writer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            let mut peer = peer;
            peer.write_all(b"x").unwrap();
            peer
        });
        let mut buf = [0; 1];
        blocking.read_exact(&mut buf).expect("a read that waits");
        assert_eq!(&buf, b"x");
        drop(writer.join().unwrap());
    }
}
