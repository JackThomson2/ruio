//! Socket operations against real sockets: TCP and UDP on the loopback
//! interface, and Unix sockets.

mod support;

use std::fs;
use std::io::{Read as _, Write as _};
use std::net::{self, SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::os::fd::AsRawFd;
use std::os::linux::net::SocketAddrExt;
use std::os::unix::net::UnixDatagram;
use std::path::PathBuf;

use ruio_driver::buf::BufPool;
use ruio_driver::ops::{
    Accept, Connect, Recv, RecvFrom, RecvStream, Send, SendTo, SendVectored, Shutdown, Socket,
};
use ruio_driver::{Descriptor, Driver, SockAddr};

use support::{PATIENCE, block_on, poll_once};

/// A connected stream whose blocking reads give up after [`PATIENCE`], so
/// that a missing reply fails the test rather than hanging it.
fn patient(stream: TcpStream) -> TcpStream {
    stream.set_read_timeout(Some(PATIENCE)).unwrap();
    stream
}

fn close_on_exec(fd: &Descriptor) -> bool {
    let Descriptor::Plain(fd) = fd else {
        panic!("only plain descriptors have descriptor flags");
    };
    // SAFETY: `F_GETFD` only reads the flags of an open descriptor.
    let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFD) };
    assert!(flags >= 0);
    flags & libc::FD_CLOEXEC != 0
}

fn tcp_socket(driver: &Driver) -> Descriptor {
    let socket = Socket::new(libc::AF_INET, libc::SOCK_STREAM, 0);
    block_on(driver, driver.op(socket)).unwrap()
}

#[test]
fn a_connection_carries_data_both_ways() {
    let driver = Driver::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = tcp_socket(&driver);
    assert!(close_on_exec(&client));
    let Descriptor::Plain(client) = client else {
        panic!("a plain socket was asked for");
    };
    let client = patient(TcpStream::from(client));

    // The connect and the accept go to the kernel together.
    let mut connect = driver.op(Connect::new(
        client.as_raw_fd(),
        listener.local_addr().unwrap(),
    ));
    assert!(poll_once(&mut connect).is_pending());
    let (server, peer) = block_on(&driver, driver.op(Accept::new(listener.as_raw_fd()))).unwrap();
    block_on(&driver, connect).unwrap();
    assert!(close_on_exec(&server));
    assert_eq!(peer.as_inet(), Some(client.local_addr().unwrap()));

    let send = Send::new(client.as_raw_fd(), b"ping".to_vec());
    assert_eq!(block_on(&driver, driver.op(send)).0.unwrap(), 4);
    let recv = Recv::new(server.fd(), Vec::with_capacity(16));
    let (received, buf) = block_on(&driver, driver.op(recv));
    assert_eq!(received.unwrap(), 4);
    assert_eq!(buf, b"ping");

    let send = Send::new(server.fd(), b"pong".to_vec());
    assert_eq!(block_on(&driver, driver.op(send)).0.unwrap(), 4);
    let shutdown = Shutdown::new(server.fd(), net::Shutdown::Write);
    block_on(&driver, driver.op(shutdown)).unwrap();
    let mut rest = String::new();
    (&client).read_to_string(&mut rest).unwrap();
    assert_eq!(rest, "pong");
}

/// With `MSG_WAITALL` the kernel carries on until the buffer is full, or
/// everything is sent, however the data trickles in or out.
#[test]
fn waiting_for_all_of_it_takes_several_goes() {
    let driver = Driver::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut peer = patient(TcpStream::connect(listener.local_addr().unwrap()).unwrap());
    let (socket, _) = listener.accept().unwrap();

    let mut recv =
        driver.op(Recv::new(socket.as_raw_fd(), Vec::with_capacity(6)).flags(libc::MSG_WAITALL));
    assert!(poll_once(&mut recv).is_pending());
    for piece in [&b"ab"[..], b"cd", b"ef"] {
        peer.write_all(piece).unwrap();
        driver.submit().unwrap();
    }
    let (received, buf) = block_on(&driver, recv);
    assert_eq!(received.unwrap(), 6);
    assert_eq!(buf, b"abcdef");

    // More than the socket buffers hold: the send finishes only as the peer
    // reads.
    let data: Vec<u8> = (0..8 << 20).map(|index: u32| index as u8).collect();
    let send = Send::new(socket.as_raw_fd(), data.clone()).flags(libc::MSG_WAITALL);
    let reader = std::thread::spawn(move || {
        let mut received = Vec::new();
        peer.read_to_end(&mut received).unwrap();
        received
    });
    let (sent, _) = block_on(&driver, driver.op(send));
    assert_eq!(sent.unwrap(), data.len());
    drop(socket);
    assert!(reader.join().unwrap() == data);
}

#[test]
fn a_direct_connection_carries_data() {
    let driver = Driver::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut client = patient(TcpStream::connect(listener.local_addr().unwrap()).unwrap());
    let accept = Accept::new(listener.as_raw_fd()).direct(&driver);
    let (server, peer) = block_on(&driver, driver.op(accept)).unwrap();
    assert!(server.is_direct());
    assert_eq!(peer.as_inet(), Some(client.local_addr().unwrap()));

    // A multishot receive into a pool, and a send of what it received.
    client.write_all(b"hello").unwrap();
    let pool = BufPool::new(&driver, 4, 64).unwrap();
    let mut stream = RecvStream::new(&driver, server.fd(), &pool);
    let bundle = block_on(&driver, stream.next()).unwrap().unwrap();
    assert_eq!(bundle.to_vec(), b"hello");
    let lease = bundle.into_iter().next().unwrap();
    let (sent, lease) = block_on(&driver, driver.op(Send::new(server.fd(), lease)));
    assert_eq!(sent.unwrap(), 5);
    drop(lease);
    let mut echoed = [0; 5];
    client.read_exact(&mut echoed).unwrap();
    assert_eq!(&echoed, b"hello");

    drop(client);
    assert!(block_on(&driver, stream.next()).is_none());
    assert_eq!(pool.available(), 4);
}

/// Buffers go out one after another as one stream of bytes, leaving out
/// what was sent before; a bundle received goes back out as it came.
#[test]
fn vectored_sends_send_buffers_one_after_another() {
    let driver = Driver::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut peer = patient(TcpStream::connect(listener.local_addr().unwrap()).unwrap());
    let (socket, _) = listener.accept().unwrap();

    let bufs = vec![b"one ".to_vec(), b"two ".to_vec(), b"three".to_vec()];
    let send = SendVectored::new(socket.as_raw_fd(), bufs).skip(2);
    let (sent, bufs) = block_on(&driver, driver.op(send));
    assert_eq!(sent.unwrap(), 11);
    // One buffer left after skipping goes as a plain send.
    let send = SendVectored::new(socket.as_raw_fd(), bufs).skip(8);
    let (sent, bufs) = block_on(&driver, driver.op(send));
    assert_eq!(sent.unwrap(), 5);
    assert_eq!(bufs.len(), 3);
    let mut received = [0; 16];
    peer.read_exact(&mut received).unwrap();
    assert_eq!(&received, b"e two threethree");

    let pool = BufPool::new(&driver, 8, 4).unwrap();
    let data: Vec<u8> = (0..20).collect();
    peer.write_all(&data).unwrap();
    let mut stream = RecvStream::new(&driver, socket.as_raw_fd(), &pool);
    let mut echoed = 0;
    while echoed < data.len() {
        let bundle = block_on(&driver, stream.next()).unwrap().unwrap();
        let len = bundle.len();
        let send = SendVectored::new(socket.as_raw_fd(), bundle).flags(libc::MSG_WAITALL);
        assert_eq!(block_on(&driver, driver.op(send)).0.unwrap(), len);
        echoed += len;
    }
    let mut received = vec![0; data.len()];
    peer.read_exact(&mut received).unwrap();
    assert_eq!(received, data);
    assert_eq!(pool.available(), 8);
}

#[test]
fn a_direct_socket_connects() {
    let driver = Driver::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let socket = Socket::new(libc::AF_INET, libc::SOCK_STREAM, 0).direct(&driver);
    let socket = block_on(&driver, driver.op(socket)).unwrap();
    let connect = Connect::new(socket.fd(), listener.local_addr().unwrap());
    block_on(&driver, driver.op(connect)).unwrap();
    let mut accepted = patient(listener.accept().unwrap().0);

    let send = Send::new(socket.fd(), "over a direct descriptor");
    assert_eq!(block_on(&driver, driver.op(send)).0.unwrap(), 24);
    // The close goes to the kernel with the next submission.
    drop(socket);
    driver.submit().unwrap();
    let mut received = String::new();
    accepted.read_to_string(&mut received).unwrap();
    assert_eq!(received, "over a direct descriptor");
}

#[test]
fn a_refused_connection_fails() {
    let driver = Driver::new().unwrap();
    // A port that was free a moment ago, with nothing listening on it.
    let addr = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let socket = tcp_socket(&driver);
    let err = block_on(&driver, driver.op(Connect::new(socket.fd(), addr))).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ECONNREFUSED));
}

#[test]
fn datagrams_come_with_their_senders_address() {
    let driver = Driver::new().unwrap();
    let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
    let sender = UdpSocket::bind("[::1]:0")
        .or_else(|_| UdpSocket::bind("127.0.0.1:0"))
        .unwrap();
    let receiver = if sender.local_addr().unwrap().is_ipv6() {
        UdpSocket::bind("[::1]:0").unwrap()
    } else {
        receiver
    };
    let to: SocketAddr = receiver.local_addr().unwrap();
    sender.set_read_timeout(Some(PATIENCE)).unwrap();

    let send = SendTo::new(sender.as_raw_fd(), b"datagram".to_vec(), to);
    assert_eq!(block_on(&driver, driver.op(send)).0.unwrap(), 8);
    // A buffer too small for the datagram gets its start.
    let recv = RecvFrom::new(receiver.as_raw_fd(), Vec::with_capacity(4));
    let (received, buf) = block_on(&driver, driver.op(recv));
    let (len, from) = received.unwrap();
    assert_eq!((len, &buf[..]), (4, &b"data"[..]));
    assert_eq!(from.as_inet(), Some(sender.local_addr().unwrap()));

    // The address the datagram came from takes a reply.
    let reply = SendTo::new(receiver.as_raw_fd(), "reply", from);
    assert_eq!(block_on(&driver, driver.op(reply)).0.unwrap(), 5);
    let mut buf = [0; 8];
    assert_eq!(sender.recv(&mut buf).unwrap(), 5);
    assert_eq!(&buf[..5], b"reply");
}

#[test]
fn unix_datagrams_come_with_their_senders_address() {
    let driver = Driver::new().unwrap();
    // Not in the target directory, whose path can be too long for a socket
    // address, which holds 107 bytes.
    let file = format!("ruio-driver-{}.sock", std::process::id());
    let mut path = std::env::temp_dir().join(&file);
    if path.as_os_str().len() >= 100 {
        path = PathBuf::from("/tmp").join(file);
    }
    let _ = fs::remove_file(&path);
    let receiver = UnixDatagram::bind(&path).unwrap();
    let name = format!("ruio-driver-{}", std::process::id());
    let named = std::os::unix::net::SocketAddr::from_abstract_name(&name).unwrap();
    let senders = [
        UnixDatagram::bind_addr(&named).unwrap(),
        UnixDatagram::unbound().unwrap(),
    ];

    for sender in &senders {
        let send = SendTo::new(sender.as_raw_fd(), "hi", SockAddr::unix(&path).unwrap());
        assert_eq!(block_on(&driver, driver.op(send)).0.unwrap(), 2);
        let recv = RecvFrom::new(receiver.as_raw_fd(), Vec::with_capacity(8));
        let (received, buf) = block_on(&driver, driver.op(recv));
        let (len, from) = received.unwrap();
        assert_eq!((len, &buf[..]), (2, &b"hi"[..]));
        let from = from.as_unix().unwrap();
        let local = sender.local_addr().unwrap();
        assert_eq!(from.as_abstract_name(), local.as_abstract_name());
        assert_eq!(from.is_unnamed(), local.is_unnamed());
    }
    fs::remove_file(&path).unwrap();
}
