//! Zero-copy sends, which report twice: the result, and a notification once
//! the kernel is done with the buffer.

mod support;

use std::cell::Cell;
use std::io::Read as _;
use std::net::{TcpListener, TcpStream, UdpSocket};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::rc::Rc;
use std::thread;

use ruio_driver::Driver;
use ruio_driver::buf::IoBuf;
use ruio_driver::ops::SendZc;

use support::{PATIENCE, block_on, settle};

/// A connected pair: the stream to send on, and its peer.
fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let peer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    peer.set_read_timeout(Some(PATIENCE)).unwrap();
    (listener.accept().unwrap().0, peer)
}

fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|index| (index % 251) as u8).collect()
}

#[test]
fn the_buffer_comes_back_once_the_kernel_is_done_with_it() {
    let driver = Driver::new().unwrap();
    let (socket, mut peer) = tcp_pair();
    let data = pattern(4 << 20);
    let reader = thread::spawn(move || {
        let mut received = Vec::new();
        peer.read_to_end(&mut received).unwrap();
        received
    });

    let send = SendZc::new(&driver, socket.as_raw_fd(), data).flags(libc::MSG_WAITALL);
    let (sent, data) = block_on(&driver, send);
    assert_eq!(sent.unwrap(), data.len());
    // The result and the notification have both arrived.
    assert_eq!(driver.in_flight(), 0);
    assert!(driver.stats().completions >= 2);
    drop(socket);
    assert!(reader.join().unwrap() == data);
}

/// Over loopback the data is copied when it reaches the peer's socket, which
/// releases the buffer before the peer reads anything.
#[test]
fn loopback_releases_the_buffer_before_the_peer_reads() {
    let driver = Driver::new().unwrap();
    let (socket, mut peer) = tcp_pair();
    let send = SendZc::new(&driver, socket.as_raw_fd(), pattern(1000));
    let (sent, data) = block_on(&driver, send);
    assert_eq!(sent.unwrap(), 1000);
    let mut received = vec![0; 1000];
    peer.read_exact(&mut received).unwrap();
    assert_eq!(received, data);
}

#[test]
fn a_datagram_goes_zero_copy_to_an_address() {
    let driver = Driver::new().unwrap();
    let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
    receiver.set_read_timeout(Some(PATIENCE)).unwrap();
    let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
    let send = SendZc::new(&driver, sender.as_raw_fd(), "zero-copy datagram")
        .to(receiver.local_addr().unwrap());
    assert_eq!(block_on(&driver, send).0.unwrap(), 18);
    let mut buf = [0; 32];
    let (len, from) = receiver.recv_from(&mut buf).unwrap();
    assert_eq!(&buf[..len], b"zero-copy datagram");
    assert_eq!(from, sender.local_addr().unwrap());
}

#[test]
fn unix_sockets_refuse_to_send_zero_copy() {
    let driver = Driver::new().unwrap();
    let (socket, _peer) = UnixStream::pair().unwrap();
    let (sent, buf) = block_on(
        &driver,
        SendZc::new(&driver, socket.as_raw_fd(), vec![1; 8]),
    );
    assert_eq!(sent.unwrap_err().raw_os_error(), Some(libc::EOPNOTSUPP));
    assert_eq!(buf, [1; 8]);
    assert_eq!(driver.in_flight(), 0);
}

/// A buffer that counts its drops.
struct Counted {
    data: Vec<u8>,
    drops: Rc<Cell<usize>>,
}

// SAFETY: the vector's contents stay put and unchanged while the operation
// holds the buffer.
unsafe impl IoBuf for Counted {
    fn stable_ptr(&self) -> *const u8 {
        self.data.stable_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.data.bytes_init()
    }

    fn bytes_total(&self) -> usize {
        self.data.bytes_total()
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

/// Dropped at any point, a send gives its buffer to the driver, which drops
/// it after the last completion. The peer keeps reading: data the kernel has
/// taken but not delivered holds on to the buffer, and without a reader the
/// notification would never come.
#[test]
fn sends_dropped_after_every_number_of_polls() {
    let driver = Driver::new().unwrap();
    let (socket, mut peer) = tcp_pair();
    let reader = thread::spawn(move || {
        let mut received = Vec::new();
        peer.read_to_end(&mut received).unwrap();
        received.len()
    });
    let drops = Rc::new(Cell::new(0));
    for polls in 0..4 {
        let buf = Counted {
            data: pattern(64 << 10),
            drops: drops.clone(),
        };
        let mut send = SendZc::new(&driver, socket.as_raw_fd(), buf);
        for _ in 0..polls {
            if support::poll_once(&mut send).is_ready() {
                break;
            }
            driver.submit().unwrap();
        }
        drop(send);
        settle(&driver);
        assert_eq!(drops.get(), polls + 1, "{polls} polls");
    }
    drop(socket);
    // A send cancelled part of the way may have sent part of its buffer.
    assert!(reader.join().unwrap() <= 3 * (64 << 10));
}
