//! Multishot receives into a pool, and the stream that keeps them armed.

mod support;

use std::io::Write as _;
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::task::Context;
use std::thread;

use ruio_driver::Driver;
use ruio_driver::buf::BufPool;
use ruio_driver::ops::RecvStream;

use support::{Counter, block_on, settle};

/// The byte at position `index` of the test data.
fn pattern(index: usize) -> u8 {
    (index % 251) as u8
}

#[test]
fn data_arrives_until_the_peer_shuts_down() {
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 4, 64).unwrap();
    let (socket, mut peer) = UnixStream::pair().unwrap();
    let mut stream = RecvStream::new(&driver, socket.as_raw_fd(), &pool);

    peer.write_all(b"hello ").unwrap();
    peer.write_all(b"world").unwrap();
    drop(peer);
    let mut received = Vec::new();
    while let Some(lease) = block_on(&driver, stream.next()) {
        received.extend_from_slice(&lease.unwrap());
    }
    assert_eq!(received, b"hello world");
    assert!(
        block_on(&driver, stream.next()).is_none(),
        "the stream stays ended"
    );
    assert_eq!(pool.available(), 4);
}

/// With every buffer leased, the kernel ends the receive with ENOBUFS and
/// the stream waits. Returning a buffer wakes it; it re-arms the receive and
/// the rest of the data arrives, in order.
#[test]
fn the_stream_rearms_once_an_exhausted_pool_has_a_buffer() {
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 2, 16).unwrap();
    let (socket, mut peer) = UnixStream::pair().unwrap();
    let sent: Vec<u8> = (0..64).map(pattern).collect();
    peer.write_all(&sent).unwrap();
    let mut stream = RecvStream::new(&driver, socket.as_raw_fd(), &pool);

    let first = block_on(&driver, stream.next()).unwrap().unwrap();
    let second = block_on(&driver, stream.next()).unwrap().unwrap();
    assert_eq!((first.len(), second.len()), (16, 16));
    assert_eq!(pool.available(), 0);

    // More data is waiting, but there is no buffer for it.
    let (woken, waker) = Counter::waker();
    let mut cx = Context::from_waker(&waker);
    for _ in 0..3 {
        assert!(stream.poll_next(&mut cx).is_pending());
        driver.submit().unwrap();
    }
    assert_eq!(woken.count(), 0);

    let mut received = [&*first, &*second].concat();
    drop(first);
    assert_eq!(woken.count(), 1, "returning a buffer wakes the stream");
    while received.len() < sent.len() {
        let lease = block_on(&driver, stream.next()).unwrap().unwrap();
        received.extend_from_slice(&lease);
    }
    assert_eq!(received, sent);
    drop((second, peer));
    assert!(block_on(&driver, stream.next()).is_none());
    assert_eq!(pool.available(), 2);
}

/// A producer outpaces a consumer that has two small buffers, so the kernel
/// keeps running the pool dry and the stream keeps re-arming the receive;
/// every byte must still arrive, in order.
#[test]
fn no_data_is_lost_across_many_rearms() {
    const TOTAL: usize = 20_000;
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 2, 8).unwrap();
    let (socket, mut peer) = UnixStream::pair().unwrap();
    let producer = thread::spawn(move || {
        let data: Vec<u8> = (0..TOTAL).map(pattern).collect();
        for chunk in data.chunks(100) {
            peer.write_all(chunk).unwrap();
        }
    });

    let mut stream = RecvStream::new(&driver, socket.as_raw_fd(), &pool);
    let mut received = Vec::with_capacity(TOTAL);
    while let Some(lease) = block_on(&driver, stream.next()) {
        received.extend_from_slice(&lease.unwrap());
    }
    producer.join().unwrap();

    assert_eq!(received.len(), TOTAL);
    assert!(
        received
            .iter()
            .enumerate()
            .all(|(index, &byte)| byte == pattern(index))
    );
    // Each submission after the first re-armed a receive the kernel ended.
    let submissions = driver.stats().operations;
    assert!(
        submissions >= 10,
        "the receive was submitted {submissions} times"
    );
    assert_eq!(pool.available(), 2);
}

#[test]
fn a_dropped_stream_returns_every_buffer() {
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 4, 16).unwrap();
    let (socket, mut peer) = UnixStream::pair().unwrap();
    peer.write_all(&[7; 64]).unwrap();
    let mut stream = RecvStream::new(&driver, socket.as_raw_fd(), &pool);
    let kept = block_on(&driver, stream.next()).unwrap().unwrap();
    // Let the kernel fill the other buffers, which the stream then holds.
    driver.submit().unwrap();

    drop(stream);
    settle(&driver);
    assert_eq!(pool.available(), 3);
    drop(kept);
    assert_eq!(pool.available(), 4);
}
