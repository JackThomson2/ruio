//! Multishot receives into a pool, and the stream that keeps them armed.

mod support;

use std::io::Write as _;
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::task::{Context, Poll};
use std::thread;

use ruio_driver::Driver;
use ruio_driver::buf::{BufLease, BufPool};
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
    while let Some(bundle) = block_on(&driver, stream.next()) {
        received.extend(bundle.unwrap().to_vec());
    }
    assert_eq!(received, b"hello world");
    assert!(
        block_on(&driver, stream.next()).is_none(),
        "the stream stays ended"
    );
    assert_eq!(pool.available(), 4);
}

/// Data waiting fills as many buffers as it needs per completion. Not all
/// of it need come at once: a request's first receive does not know how
/// much is waiting, and takes a single buffer.
#[test]
fn a_receive_fills_as_many_buffers_as_it_needs() {
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 8, 16).unwrap();
    let (socket, mut peer) = UnixStream::pair().unwrap();
    let sent: Vec<u8> = (0..40).map(pattern).collect();
    peer.write_all(&sent).unwrap();
    let mut stream = RecvStream::new(&driver, socket.as_raw_fd(), &pool);

    let mut received = Vec::new();
    let mut shapes = Vec::new();
    let mut held = Vec::new();
    while received.len() < sent.len() {
        let bundle = block_on(&driver, stream.next()).unwrap().unwrap();
        received.extend(bundle.to_vec());
        shapes.push(bundle.iter().map(BufLease::len).collect::<Vec<_>>());
        held.push(bundle);
    }
    assert_eq!(received, sent);
    for shape in &shapes {
        let (_, full) = shape.split_last().unwrap();
        assert!(full.iter().all(|&len| len == 16), "{shapes:?}");
    }
    assert!(shapes.iter().any(|shape| shape.len() > 1), "{shapes:?}");
    let buffers: usize = held.iter().map(|bundle| bundle.buffers()).sum();
    assert_eq!(pool.available(), 8 - buffers);
    drop(held);
    assert_eq!(pool.available(), 8);
}

/// `next_ready` takes what has arrived, without waiting, and leaves the
/// waker the last poll left, so that data still wakes the task. A message
/// that comes in two receives is whole once both are taken.
#[test]
fn next_ready_takes_what_has_arrived_without_waiting() {
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 8, 16).unwrap();
    let (socket, mut peer) = UnixStream::pair().unwrap();
    let mut stream = RecvStream::new(&driver, socket.as_raw_fd(), &pool);
    assert!(stream.next_ready().is_none(), "nothing is even submitted");

    let (woken, waker) = Counter::waker();
    let mut cx = Context::from_waker(&waker);
    assert!(stream.poll_next(&mut cx).is_pending());
    driver.submit().unwrap();
    assert!(stream.next_ready().is_none());
    let sent: Vec<u8> = (0..40).map(pattern).collect();
    peer.write_all(&sent).unwrap();
    driver.submit().unwrap();
    assert_eq!(woken.count(), 1, "the poll's waker stayed");

    let Poll::Ready(Some(Ok(mut data))) = stream.poll_next(&mut cx) else {
        panic!("the data arrived");
    };
    while let Some(more) = stream.next_ready() {
        data.append(more.unwrap());
    }
    assert_eq!(data.to_vec(), sent);
    assert!(stream.next_ready().is_none());
    drop((data, peer));
    assert!(block_on(&driver, stream.next()).is_none());
    assert_eq!(pool.available(), 8);
}

/// Leases kept for a while and returned out of order leave the ring's slots
/// holding buffers in no particular order, which a bundle's completion,
/// naming only its first buffer, must still be read right against.
#[test]
fn bundles_come_out_right_whatever_order_buffers_return_in() {
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 8, 16).unwrap();
    let (socket, mut peer) = UnixStream::pair().unwrap();
    let mut stream = RecvStream::new(&driver, socket.as_raw_fd(), &pool);
    let mut kept: Vec<BufLease> = Vec::new();
    let mut offset = 0;
    for round in 0..200 {
        let len = 1 + round * 7 % 60;
        let sent: Vec<u8> = (offset..offset + len).map(pattern).collect();
        offset += len;
        peer.write_all(&sent).unwrap();
        let mut received = Vec::new();
        while received.len() < len {
            let bundle = block_on(&driver, stream.next()).unwrap().unwrap();
            received.extend(bundle.to_vec());
            // Keep some leases a round or two, return the rest last first.
            let mut leases: Vec<BufLease> = bundle.into_iter().collect();
            if round % 3 == 0 {
                kept.extend(leases.drain(..1));
            }
            while let Some(lease) = leases.pop() {
                drop(lease);
            }
        }
        assert_eq!(received, sent, "round {round}");
        if kept.len() > 2 {
            kept.remove(1);
        }
    }
    drop(kept);
    assert_eq!(pool.available(), 8);
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

    // The first receive takes both buffers.
    let bundle = block_on(&driver, stream.next()).unwrap().unwrap();
    assert_eq!(bundle.len(), 32);
    assert_eq!(pool.available(), 0);

    // More data is waiting, but there is no buffer for it. Once the kernel
    // has ended the receive for that, only a returning buffer wakes the
    // stream.
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

    let mut received = bundle.to_vec();
    let mut leases = bundle.into_iter();
    drop(leases.next());
    assert_eq!(
        woken.count(),
        starved + 1,
        "returning a buffer wakes the stream"
    );
    drop(leases);
    while received.len() < sent.len() {
        let bundle = block_on(&driver, stream.next()).unwrap().unwrap();
        received.extend(bundle.to_vec());
    }
    assert_eq!(received, sent);
    drop(peer);
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
    while let Some(bundle) = block_on(&driver, stream.next()) {
        received.extend(bundle.unwrap().to_vec());
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

/// Buffers the kernel filled for a stream that is dropped before taking
/// them go back to the pool.
#[test]
fn a_dropped_stream_returns_every_buffer() {
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 4, 16).unwrap();
    let (socket, mut peer) = UnixStream::pair().unwrap();
    let mut stream = RecvStream::new(&driver, socket.as_raw_fd(), &pool);
    peer.write_all(&[7; 16]).unwrap();
    let kept = block_on(&driver, stream.next()).unwrap().unwrap();
    assert_eq!(kept.buffers(), 1);
    // The kernel fills the other buffers, and the stream holds the
    // completion, which the pool learns of only once it is taken.
    let completions = driver.stats().completions;
    peer.write_all(&[8; 48]).unwrap();
    driver.submit().unwrap();
    assert!(driver.stats().completions > completions);

    drop(stream);
    settle(&driver);
    assert_eq!(pool.available(), 3);
    drop(kept);
    assert_eq!(pool.available(), 4);

    // The pool accounted for every buffer: all of them take data again.
    let mut stream = RecvStream::new(&driver, socket.as_raw_fd(), &pool);
    let sent: Vec<u8> = (0..64).map(pattern).collect();
    peer.write_all(&sent).unwrap();
    let mut received = Vec::new();
    let mut held = Vec::new();
    while received.len() < sent.len() {
        let bundle = block_on(&driver, stream.next()).unwrap().unwrap();
        received.extend(bundle.to_vec());
        held.push(bundle);
    }
    assert_eq!(received, sent);
    assert_eq!(pool.available(), 0);
}
