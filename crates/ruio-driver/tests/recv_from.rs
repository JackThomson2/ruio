//! Multishot `recvmsg` into a pool: datagrams with their senders' addresses,
//! and the stream that keeps the request armed.

mod support;

use std::io::ErrorKind;
use std::net::UdpSocket;
use std::os::fd::AsRawFd;
use std::os::linux::net::SocketAddrExt;
use std::os::unix::net::{SocketAddr, UnixDatagram};
use std::process;
use std::task::Context;
use std::thread;

use ruio_driver::Driver;
use ruio_driver::buf::BufPool;
use ruio_driver::ops::{RecvFromMulti, RecvFromStream, SendTo};

use support::{Counter, block_on, settle};

/// An IPv4 UDP socket on a port of its own of the loopback address.
fn udp() -> UdpSocket {
    UdpSocket::bind("127.0.0.1:0").unwrap()
}

/// The byte at position `index` of the test data.
fn pattern(index: usize) -> u8 {
    (index % 251) as u8
}

#[test]
fn datagrams_come_with_their_senders_addresses() {
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 8, 64).unwrap();
    let socket = udp();
    let to = socket.local_addr().unwrap();
    let senders = [udp(), udp(), udp()];
    for (index, sender) in senders.iter().enumerate() {
        sender.send_to(&[index as u8; 10], to).unwrap();
    }
    let mut stream =
        RecvFromStream::new(&driver, socket.as_raw_fd(), &pool, libc::AF_INET).unwrap();
    for (index, sender) in senders.iter().enumerate() {
        let datagram = block_on(&driver, stream.next()).unwrap().unwrap();
        assert_eq!(*datagram, [index as u8; 10]);
        let from = sender.local_addr().unwrap();
        assert_eq!(datagram.inet_from(), Some(from));
        assert_eq!(datagram.from().as_inet(), Some(from));
        assert!(!datagram.is_truncated());
    }
    assert_eq!(pool.available(), 8);
}

/// An IPv6 sender's address takes more room than an IPv4 one's, which the
/// family gives it, port, flow and scope included.
#[test]
fn ipv6_senders_have_their_whole_address() {
    let Ok(socket) = UdpSocket::bind("[::1]:0") else {
        eprintln!("skipped: no IPv6 loopback address");
        return;
    };
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 4, 64).unwrap();
    let sender = UdpSocket::bind("[::1]:0").unwrap();
    // The room after the header and the address, every byte of it.
    let sent: Vec<u8> = (0..16).map(pattern).collect();
    sender.send_to(&sent, socket.local_addr().unwrap()).unwrap();
    let mut stream =
        RecvFromStream::new(&driver, socket.as_raw_fd(), &pool, libc::AF_INET6).unwrap();
    let datagram = block_on(&driver, stream.next()).unwrap().unwrap();
    assert_eq!(*datagram, *sent);
    assert!(!datagram.is_truncated());
    assert_eq!(datagram.inet_from(), Some(sender.local_addr().unwrap()));
    assert_eq!(datagram.into_data().capacity(), 16);
}

/// A datagram longer than its buffer has room for after the header and the
/// address is cut short, and says so; the next one comes whole.
#[test]
fn a_datagram_too_long_for_its_buffer_is_cut_short() {
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 4, 64).unwrap();
    let socket = udp();
    let sender = udp();
    let to = socket.local_addr().unwrap();
    let long: Vec<u8> = (0..40).map(pattern).collect();
    sender.send_to(&long, to).unwrap();
    sender.send_to(&long[..32], to).unwrap();
    let mut stream =
        RecvFromStream::new(&driver, socket.as_raw_fd(), &pool, libc::AF_INET).unwrap();

    let cut = block_on(&driver, stream.next()).unwrap().unwrap();
    assert_eq!(*cut, long[..32]);
    assert!(cut.is_truncated());
    assert_eq!(cut.inet_from(), Some(sender.local_addr().unwrap()));
    let whole = block_on(&driver, stream.next()).unwrap().unwrap();
    assert_eq!(*whole, long[..32]);
    assert!(!whole.is_truncated());
}

/// The kernel takes an empty datagram for the end of the data, and ends the
/// request with it; the stream yields it as the datagram it is, and re-arms
/// the request for the next.
#[test]
fn an_empty_datagram_is_one_like_any_other() {
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 4, 64).unwrap();
    let socket = udp();
    let sender = udp();
    let to = socket.local_addr().unwrap();
    sender.send_to(b"", to).unwrap();
    sender.send_to(b"after", to).unwrap();
    let mut stream =
        RecvFromStream::new(&driver, socket.as_raw_fd(), &pool, libc::AF_INET).unwrap();
    let before = driver.stats().operations;

    let empty = block_on(&driver, stream.next()).unwrap().unwrap();
    assert!(empty.is_empty());
    assert_eq!(empty.inet_from(), Some(sender.local_addr().unwrap()));
    let after = block_on(&driver, stream.next()).unwrap().unwrap();
    assert_eq!(*after, *b"after");
    assert_eq!(
        driver.stats().operations - before,
        2,
        "the request was submitted again after the empty datagram"
    );
    assert!(!stream.is_ended());
}

/// A Unix socket's sender has a path, an abstract name, or no address.
#[test]
fn unix_senders_come_with_their_addresses() {
    let name = |which: &str| format!("ruio-recv-from-{}-{which}", process::id());
    let bind = |which: &str| {
        let addr = SocketAddr::from_abstract_name(name(which)).unwrap();
        UnixDatagram::bind_addr(&addr).unwrap()
    };
    let socket = bind("socket");
    let named = bind("sender");
    let unnamed = UnixDatagram::unbound().unwrap();
    let to = socket.local_addr().unwrap();
    named.send_to_addr(b"named", &to).unwrap();
    unnamed.send_to_addr(b"unnamed", &to).unwrap();

    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 4, 256).unwrap();
    let mut stream =
        RecvFromStream::new(&driver, socket.as_raw_fd(), &pool, libc::AF_UNIX).unwrap();
    let first = block_on(&driver, stream.next()).unwrap().unwrap();
    assert_eq!(*first, *b"named");
    let from = first.from().as_unix().unwrap();
    assert_eq!(from.as_abstract_name(), Some(name("sender").as_bytes()));
    assert!(first.inet_from().is_none());
    let second = block_on(&driver, stream.next()).unwrap().unwrap();
    assert_eq!(*second, *b"unnamed");
    assert!(second.from().as_unix().unwrap().is_unnamed());
}

/// Each buffer must have room for a payload after the header and the room
/// the family's address takes, rounded up to 16 bytes; a family the request
/// does not know gets room for any address.
#[test]
fn a_pool_without_room_for_a_payload_is_refused() {
    let driver = Driver::new().unwrap();
    let socket = udp();
    let fd = socket.as_raw_fd();
    let request = |size, family| {
        let pool = BufPool::new(&driver, 2, size).unwrap();
        RecvFromMulti::new(fd, &pool, family).map(drop)
    };
    let refused = request(32, libc::AF_INET).unwrap_err();
    assert_eq!(refused.kind(), ErrorKind::InvalidInput);
    assert!(request(33, libc::AF_INET).is_ok());
    assert!(request(48, libc::AF_INET6).is_err());
    assert!(request(49, libc::AF_INET6).is_ok());
    assert!(request(128, libc::AF_UNIX).is_err());
    assert!(request(129, libc::AF_UNIX).is_ok());
    assert!(request(144, libc::AF_PACKET).is_err());
    assert!(request(145, libc::AF_PACKET).is_ok());
}

/// The payload's lease leaves the header and the address out of its data
/// and its room, and goes out as any buffer does: back to the sender.
#[test]
fn the_payload_is_a_lease_of_its_own() {
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 2, 64).unwrap();
    let socket = udp();
    let sender = udp();
    sender
        .send_to(b"payload", socket.local_addr().unwrap())
        .unwrap();
    let mut stream =
        RecvFromStream::new(&driver, socket.as_raw_fd(), &pool, libc::AF_INET).unwrap();
    let datagram = block_on(&driver, stream.next()).unwrap().unwrap();
    let from = datagram.inet_from().unwrap();
    let data = datagram.into_data();
    assert_eq!(&data[..], b"payload");
    assert_eq!(data.capacity(), 32);
    assert_eq!(pool.available(), 1);

    let (sent, data) = block_on(
        &driver,
        driver.op(SendTo::new(socket.as_raw_fd(), data, from)),
    );
    assert_eq!(sent.unwrap(), 7);
    let mut echo = [0; 16];
    let (len, echoed_by) = sender.recv_from(&mut echo).unwrap();
    assert_eq!(
        (&echo[..len], echoed_by),
        (&b"payload"[..], socket.local_addr().unwrap())
    );
    drop(data);
    assert_eq!(pool.available(), 2);
}

/// `next_ready` takes the datagrams that have arrived without waiting:
/// those waiting when the request is armed all come in the same entry into
/// the kernel.
#[test]
fn next_ready_takes_the_datagrams_that_have_arrived() {
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 8, 64).unwrap();
    let socket = udp();
    let sender = udp();
    let mut stream =
        RecvFromStream::new(&driver, socket.as_raw_fd(), &pool, libc::AF_INET).unwrap();
    assert!(stream.next_ready().is_none(), "nothing is even submitted");
    for index in 0..3 {
        sender
            .send_to(&[index; 4], socket.local_addr().unwrap())
            .unwrap();
    }
    let first = block_on(&driver, stream.next()).unwrap().unwrap();
    assert_eq!(*first, [0; 4]);
    let rest: Vec<Vec<u8>> = (0..2)
        .map(|_| stream.next_ready().unwrap().unwrap().to_vec())
        .collect();
    assert_eq!(rest, [[1; 4], [2; 4]]);
    assert!(stream.next_ready().is_none());
    assert!(!stream.is_ended());
}

/// With every buffer leased, the kernel ends the request with ENOBUFS and
/// the stream waits; a returning buffer wakes it, it re-arms the request,
/// and the datagrams that waited in the socket come, in order.
#[test]
fn the_stream_rearms_once_an_exhausted_pool_has_a_buffer() {
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 2, 64).unwrap();
    let socket = udp();
    let sender = udp();
    for index in 0..5 {
        sender
            .send_to(&[index; 8], socket.local_addr().unwrap())
            .unwrap();
    }
    let mut stream =
        RecvFromStream::new(&driver, socket.as_raw_fd(), &pool, libc::AF_INET).unwrap();
    let held = [
        block_on(&driver, stream.next()).unwrap().unwrap(),
        block_on(&driver, stream.next()).unwrap().unwrap(),
    ];
    assert_eq!(pool.available(), 0);

    let (woken, waker) = Counter::waker();
    let mut cx = Context::from_waker(&waker);
    for _ in 0..3 {
        assert!(stream.poll_next(&mut cx).is_pending());
        driver.submit().unwrap();
    }
    let starved = woken.count();
    for _ in 0..3 {
        assert!(stream.poll_next(&mut cx).is_pending());
        driver.submit().unwrap();
    }
    assert_eq!(woken.count(), starved);

    let [first, second] = held;
    assert_eq!((&*first, &*second), (&[0; 8][..], &[1; 8][..]));
    drop(first);
    assert_eq!(woken.count(), starved + 1, "a returning buffer wakes it");
    drop(second);
    for index in 2..5 {
        let datagram = block_on(&driver, stream.next()).unwrap().unwrap();
        assert_eq!(*datagram, [index; 8]);
    }
    assert_eq!(pool.available(), 2);
}

/// A sender that outpaces a receiver with two buffers keeps running the pool
/// dry, and the stream keeps re-arming the request: every datagram still
/// comes, whole and in order. A Unix socket's sender waits for room rather
/// than lose a datagram, as a UDP one's may not.
#[test]
fn no_datagram_is_lost_across_many_rearms() {
    const COUNT: usize = 5_000;
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 2, 256).unwrap();
    let (socket, peer) = UnixDatagram::pair().unwrap();
    let sender = thread::spawn(move || {
        for index in 0..COUNT {
            let datagram: Vec<u8> = (index..index + index % 100).map(pattern).collect();
            peer.send(&datagram).unwrap();
        }
    });

    let mut stream =
        RecvFromStream::new(&driver, socket.as_raw_fd(), &pool, libc::AF_UNIX).unwrap();
    // One datagram is kept until the next arrives, so that the kernel keeps
    // finding a single buffer in the ring.
    let mut held = None;
    for index in 0..COUNT {
        let datagram = block_on(&driver, stream.next()).unwrap().unwrap();
        let expected: Vec<u8> = (index..index + index % 100).map(pattern).collect();
        assert_eq!(*datagram, *expected, "datagram {index}");
        assert!(datagram.from().as_unix().unwrap().is_unnamed());
        held = Some(datagram);
    }
    drop(held);
    sender.join().unwrap();
    assert!(
        driver.stats().operations >= 2,
        "the request was re-armed at least once"
    );
    assert_eq!(pool.available(), 2);
}

/// Datagrams the kernel received for a stream that is dropped before taking
/// them go back to the pool.
#[test]
fn a_dropped_stream_returns_every_buffer() {
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 4, 64).unwrap();
    let socket = udp();
    let sender = udp();
    let to = socket.local_addr().unwrap();
    sender.send_to(b"kept", to).unwrap();
    let mut stream =
        RecvFromStream::new(&driver, socket.as_raw_fd(), &pool, libc::AF_INET).unwrap();
    let kept = block_on(&driver, stream.next()).unwrap().unwrap();
    for _ in 0..3 {
        sender.send_to(b"left", to).unwrap();
    }
    let completions = driver.stats().completions;
    driver.submit().unwrap();
    assert!(driver.stats().completions > completions);

    drop(stream);
    settle(&driver);
    assert_eq!(pool.available(), 3);
    drop(kept);
    assert_eq!(pool.available(), 4);
}
