//! Streams into pools whose buffers the kernel consumes incrementally.

mod support;

use std::io::{self, Write as _};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::task::Context;
use std::thread;

use ruio_driver::buf::{BufLease, IoBuf};
use ruio_driver::ops::{ReadStream, RecvStream};
use ruio_driver::{Driver, Probe};

use support::{Counter, block_on, settle};

/// The byte at position `index` of the test data.
fn pattern(index: usize) -> u8 {
    (index % 251) as u8
}

/// Whether the kernel consumes buffers incrementally; says so if not.
fn supported() -> bool {
    let supported = Probe::current().unwrap().supports_incremental_buffers();
    if !supported {
        eprintln!("skipped: the kernel does not consume buffers incrementally");
    }
    supported
}

/// Small receives fill one large buffer one after another, each lease the
/// part of it its data took.
#[test]
fn small_receives_share_a_buffer() {
    if !supported() {
        return;
    }
    let driver = Driver::new().unwrap();
    let (socket, mut peer) = UnixStream::pair().unwrap();
    let mut stream = RecvStream::incremental(&driver, socket.as_raw_fd(), 2, 4096).unwrap();
    let mut leases: Vec<BufLease> = Vec::new();
    for round in 0..10 {
        let sent = format!("message {round}");
        peer.write_all(sent.as_bytes()).unwrap();
        let bundle = block_on(&driver, stream.next()).unwrap().unwrap();
        assert_eq!(bundle.to_vec(), sent.as_bytes());
        leases.extend(bundle);
    }
    assert_eq!(leases.len(), 10, "one buffer each");
    for pair in leases.windows(2) {
        let end = pair[0].stable_ptr().wrapping_add(pair[0].len());
        assert_eq!(pair[1].stable_ptr(), end, "each carries on from the last");
        assert_eq!(pair[0].capacity(), pair[0].len());
    }
    drop((leases, peer));
    assert!(block_on(&driver, stream.next()).is_none());
}

/// Data of every size arrives whole and in order, in parts of buffers and
/// across them, whatever order the leases return in.
#[test]
fn data_comes_out_right_across_buffers() {
    if !supported() {
        return;
    }
    let driver = Driver::new().unwrap();
    let (socket, mut peer) = UnixStream::pair().unwrap();
    let mut stream = RecvStream::incremental(&driver, socket.as_raw_fd(), 8, 64).unwrap();
    let mut kept: Vec<BufLease> = Vec::new();
    let mut offset = 0;
    for round in 0..300 {
        let len = 1 + round * 37 % 150;
        let sent: Vec<u8> = (offset..offset + len).map(pattern).collect();
        offset += len;
        peer.write_all(&sent).unwrap();
        let mut received = Vec::new();
        while received.len() < len {
            let bundle = block_on(&driver, stream.next()).unwrap().unwrap();
            received.extend(bundle.to_vec());
            let mut leases: Vec<BufLease> = bundle.into_iter().collect();
            if round % 3 == 0 {
                kept.extend(leases.drain(..1));
            }
            while let Some(lease) = leases.pop() {
                drop(lease);
            }
        }
        assert_eq!(received, sent, "round {round}");
        if kept.len() > 3 {
            kept.remove(1);
        }
    }
    drop((kept, peer));
    assert!(block_on(&driver, stream.next()).is_none());
    settle(&driver);
}

/// With every buffer filled and leased, the kernel ends the receive with
/// ENOBUFS; returning the leases returns the buffers, and the stream
/// re-arms.
#[test]
fn the_stream_rearms_once_its_buffers_return() {
    if !supported() {
        return;
    }
    let driver = Driver::new().unwrap();
    let (socket, mut peer) = UnixStream::pair().unwrap();
    let sent: Vec<u8> = (0..64).map(pattern).collect();
    peer.write_all(&sent).unwrap();
    let mut stream = RecvStream::incremental(&driver, socket.as_raw_fd(), 2, 16).unwrap();
    let mut received = Vec::new();
    let mut held = Vec::new();
    while received.len() < 32 {
        let bundle = block_on(&driver, stream.next()).unwrap().unwrap();
        received.extend(bundle.to_vec());
        held.push(bundle);
    }
    assert_eq!(received.len(), 32, "both buffers, filled");

    let (woken, waker) = Counter::waker();
    let mut cx = Context::from_waker(&waker);
    for _ in 0..3 {
        assert!(stream.poll_next(&mut cx).is_pending());
        driver.submit().unwrap();
    }
    let starved = woken.count();
    assert!(stream.poll_next(&mut cx).is_pending());
    assert_eq!(woken.count(), starved);
    drop(held);
    assert!(
        woken.count() > starved,
        "returning the buffers wakes the stream"
    );
    while received.len() < sent.len() {
        let bundle = block_on(&driver, stream.next()).unwrap().unwrap();
        received.extend(bundle.to_vec());
    }
    assert_eq!(received, sent);
}

/// A writer outpaces a reader with two small buffers; every byte arrives,
/// in order, through many re-arms.
#[test]
fn no_data_is_lost_across_many_rearms() {
    if !supported() {
        return;
    }
    const TOTAL: usize = 20_000;
    let driver = Driver::new().unwrap();
    let (socket, mut peer) = UnixStream::pair().unwrap();
    let producer = thread::spawn(move || {
        let data: Vec<u8> = (0..TOTAL).map(pattern).collect();
        for chunk in data.chunks(77) {
            peer.write_all(chunk).unwrap();
        }
    });
    let mut stream = RecvStream::incremental(&driver, socket.as_raw_fd(), 2, 64).unwrap();
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
}

/// Multishot reads of a pipe share buffers the same way.
#[test]
fn small_reads_share_a_buffer() {
    if !supported() {
        return;
    }
    let driver = Driver::new().unwrap();
    let (reader, mut writer) = io::pipe().unwrap();
    let mut stream = ReadStream::incremental(&driver, reader.as_raw_fd(), 2, 4096).unwrap();
    writer.write_all(b"first").unwrap();
    let first = block_on(&driver, stream.next()).unwrap().unwrap();
    writer.write_all(b"second").unwrap();
    let second = block_on(&driver, stream.next()).unwrap().unwrap();
    assert_eq!((&*first, &*second), (&b"first"[..], &b"second"[..]));
    assert_eq!(second.stable_ptr(), first.stable_ptr().wrapping_add(5));
    drop(writer);
    assert!(block_on(&driver, stream.next()).is_none());
}

/// A stream dropped with completions it never took hands them to the pool,
/// which the kernel then has back.
#[test]
fn a_dropped_stream_settles() {
    if !supported() {
        return;
    }
    let driver = Driver::new().unwrap();
    let (socket, mut peer) = UnixStream::pair().unwrap();
    let mut stream = RecvStream::incremental(&driver, socket.as_raw_fd(), 4, 32).unwrap();
    peer.write_all(&[1; 10]).unwrap();
    let kept = block_on(&driver, stream.next()).unwrap().unwrap();
    peer.write_all(&[2; 50]).unwrap();
    driver.submit().unwrap();
    drop(stream);
    settle(&driver);
    assert_eq!(driver.in_flight(), 0);
    assert_eq!(kept.to_vec(), [1; 10]);
}
