//! Socket options and queue sizes through the ring, on plain and direct
//! descriptors, and installing a direct descriptor as a plain one.

mod support;

use std::io::{self, Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::os::fd::{AsRawFd, OwnedFd};
use std::time::{Duration, Instant};

use ruio_driver::ops::{
    Connect, GetSockOpt, InstallFd, Send, SetSockOpt, Socket, UnreadBytes, UnsentBytes,
};
use ruio_driver::{Descriptor, Driver, Fd};

use support::{block_on, settle};

/// A direct stream socket of `driver`'s ring.
fn direct_socket(driver: &Driver) -> Descriptor {
    let socket = Socket::new(libc::AF_INET, libc::SOCK_STREAM, 0).direct(driver);
    block_on(driver, driver.op(socket)).unwrap()
}

/// A connected pair: a direct socket connected through the ring, and the
/// plain socket a listener accepted it as.
fn direct_pair(driver: &Driver) -> (Descriptor, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let socket = direct_socket(driver);
    let connect = Connect::new(socket.fd(), listener.local_addr().unwrap());
    block_on(driver, driver.op(connect)).unwrap();
    let (accepted, _) = listener.accept().unwrap();
    (socket, accepted)
}

/// An option of `socket`, read with `getsockopt(2)` rather than the ring.
fn read_option(socket: &impl AsRawFd, level: i32, name: i32) -> i32 {
    let mut value: i32 = 0;
    let mut len = size_of::<i32>() as libc::socklen_t;
    // SAFETY: `value` and `len` are valid for the kernel to write an `int`
    // and its length through.
    let got = unsafe {
        libc::getsockopt(
            socket.as_raw_fd(),
            level,
            name,
            (&raw mut value).cast(),
            &mut len,
        )
    };
    assert_eq!(got, 0, "{}", io::Error::last_os_error());
    value
}

#[test]
fn an_option_set_through_the_ring_is_set() {
    let driver = Driver::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let socket = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    assert!(!socket.nodelay().unwrap());

    // At any level: `TCP_NODELAY` is one of `IPPROTO_TCP`.
    let set = SetSockOpt::new(
        socket.as_raw_fd(),
        libc::IPPROTO_TCP,
        libc::TCP_NODELAY,
        1i32,
    );
    block_on(&driver, driver.op(set)).unwrap();
    assert!(socket.nodelay().unwrap(), "set as `setsockopt(2)` would");

    // Read back through the ring, for an option of `SOL_SOCKET`.
    assert_eq!(
        read_option(&socket, libc::SOL_SOCKET, libc::SO_KEEPALIVE),
        0
    );
    let set = SetSockOpt::new(
        socket.as_raw_fd(),
        libc::SOL_SOCKET,
        libc::SO_KEEPALIVE,
        1i32,
    );
    block_on(&driver, driver.op(set)).unwrap();
    assert_eq!(
        read_option(&socket, libc::SOL_SOCKET, libc::SO_KEEPALIVE),
        1
    );
    let get = GetSockOpt::<i32>::new(socket.as_raw_fd(), libc::SOL_SOCKET, libc::SO_KEEPALIVE);
    assert_eq!(block_on(&driver, driver.op(get)).unwrap(), 1);
    let get = GetSockOpt::<i32>::new(socket.as_raw_fd(), libc::SOL_SOCKET, libc::SO_TYPE);
    assert_eq!(
        block_on(&driver, driver.op(get)).unwrap(),
        libc::SOCK_STREAM
    );
}

#[test]
fn options_reach_a_direct_socket() {
    let driver = Driver::new().unwrap();
    let socket = direct_socket(&driver);
    assert!(socket.is_direct());
    let fd = socket.fd();

    let get = GetSockOpt::<i32>::new(fd, libc::SOL_SOCKET, libc::SO_TYPE);
    assert_eq!(
        block_on(&driver, driver.op(get)).unwrap(),
        libc::SOCK_STREAM
    );

    let get = GetSockOpt::<i32>::new(fd, libc::SOL_SOCKET, libc::SO_KEEPALIVE);
    assert_eq!(block_on(&driver, driver.op(get)).unwrap(), 0);
    let set = SetSockOpt::new(fd, libc::SOL_SOCKET, libc::SO_KEEPALIVE, 1i32);
    block_on(&driver, driver.op(set)).unwrap();
    let get = GetSockOpt::<i32>::new(fd, libc::SOL_SOCKET, libc::SO_KEEPALIVE);
    assert_eq!(block_on(&driver, driver.op(get)).unwrap(), 1);
    let set = SetSockOpt::new(fd, libc::IPPROTO_TCP, libc::TCP_NODELAY, 1i32);
    block_on(&driver, driver.op(set)).unwrap();

    // A value of another type than `int`.
    let linger = libc::linger {
        l_onoff: 1,
        l_linger: 7,
    };
    let set = SetSockOpt::new(fd, libc::SOL_SOCKET, libc::SO_LINGER, linger);
    block_on(&driver, driver.op(set)).unwrap();
    let get = GetSockOpt::<libc::linger>::new(fd, libc::SOL_SOCKET, libc::SO_LINGER);
    let linger = block_on(&driver, driver.op(get)).unwrap();
    assert_eq!((linger.l_onoff, linger.l_linger), (1, 7));
}

/// An option the socket does not have is refused as `getsockopt(2)` refuses
/// it; a read at a level other than `SOL_SOCKET` is refused by the kernel's
/// rule for the ring, whatever the option.
#[test]
fn an_option_the_socket_does_not_have_is_refused() {
    let driver = Driver::new().unwrap();
    let socket = direct_socket(&driver);
    let get = GetSockOpt::<i32>::new(socket.fd(), libc::SOL_SOCKET, 0x7fff);
    let err = block_on(&driver, driver.op(get)).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOPROTOOPT));

    let get = GetSockOpt::<i32>::new(socket.fd(), libc::IPPROTO_TCP, libc::TCP_NODELAY);
    let err = block_on(&driver, driver.op(get)).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EOPNOTSUPP));

    let set = SetSockOpt::new(Fd::Plain(-1), libc::IPPROTO_TCP, libc::TCP_NODELAY, 1i32);
    let err = block_on(&driver, driver.op(set)).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EBADF));
}

#[test]
fn the_queued_bytes_are_counted() {
    let driver = Driver::new().unwrap();
    let (socket, mut peer) = direct_pair(&driver);
    let fd = socket.fd();
    assert_eq!(
        block_on(&driver, driver.op(UnreadBytes::new(fd))).unwrap(),
        0
    );

    peer.write_all(b"five!").unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let unread = loop {
        let unread = block_on(&driver, driver.op(UnreadBytes::new(fd))).unwrap();
        if unread > 0 || Instant::now() > deadline {
            break unread;
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    assert_eq!(unread, 5);

    // Sent and acknowledged over loopback within the wait, or still in the
    // queue: either way a count the kernel gives.
    let (sent, _) = block_on(&driver, driver.op(Send::new(fd, "back")));
    assert_eq!(sent.unwrap(), 4);
    let unsent = block_on(&driver, driver.op(UnsentBytes::new(fd))).unwrap();
    assert!(unsent <= 4, "{unsent} unsent");
    let mut back = [0; 4];
    peer.read_exact(&mut back).unwrap();
    assert_eq!(&back, b"back");
}

/// A direct socket installed as a plain descriptor is the same open file:
/// the descriptor answers `getsockopt(2)` and `getpeername(2)`, and closing
/// it leaves the slot's file open.
#[test]
fn a_direct_socket_is_installed_as_a_plain_descriptor() {
    let driver = Driver::new().unwrap();
    let (socket, peer) = direct_pair(&driver);
    let set = SetSockOpt::new(socket.fd(), libc::IPPROTO_TCP, libc::TCP_NODELAY, 1i32);
    block_on(&driver, driver.op(set)).unwrap();

    let installed: OwnedFd = block_on(&driver, driver.op(InstallFd::new(socket.fd()))).unwrap();
    assert_eq!(
        read_option(&installed, libc::IPPROTO_TCP, libc::TCP_NODELAY),
        1
    );
    // SAFETY: `fcntl` with this command touches no memory.
    let flags = unsafe { libc::fcntl(installed.as_raw_fd(), libc::F_GETFD) };
    assert_ne!(flags & libc::FD_CLOEXEC, 0, "closed on exec unless asked");
    let plain = TcpStream::from(installed);
    assert_eq!(plain.peer_addr().unwrap(), peer.local_addr().unwrap());
    assert_eq!(plain.local_addr().unwrap(), peer.peer_addr().unwrap());
    drop(plain);

    // The slot still holds the file: it sends, and the peer reads.
    let (sent, _) = block_on(&driver, driver.op(Send::new(socket.fd(), "still open")));
    assert_eq!(sent.unwrap(), 10);
    let mut peer = peer;
    let mut read = [0; 10];
    peer.read_exact(&mut read).unwrap();
    assert_eq!(&read, b"still open");
    drop(socket);
    settle(&driver);
}

#[test]
fn an_installed_descriptor_can_be_inherited() {
    let driver = Driver::new().unwrap();
    let socket = direct_socket(&driver);
    let installed = block_on(&driver, driver.op(InstallFd::new(socket.fd()).inherited())).unwrap();
    // SAFETY: `fcntl` with this command touches no memory.
    let flags = unsafe { libc::fcntl(installed.as_raw_fd(), libc::F_GETFD) };
    assert_eq!(flags & libc::FD_CLOEXEC, 0);
}

#[test]
fn a_plain_descriptor_cannot_be_installed() {
    let driver = Driver::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let err = block_on(&driver, driver.op(InstallFd::new(listener.as_raw_fd()))).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EBADF));
}
