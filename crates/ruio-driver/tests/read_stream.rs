//! Multishot reads into a pool, and the stream that keeps them armed.

mod support;

use std::io::{self, Write as _};
use std::os::fd::AsRawFd;
use std::task::Context;
use std::thread;

use ruio_driver::Driver;
use ruio_driver::buf::BufPool;
use ruio_driver::ops::ReadStream;

use support::{Counter, block_on, settle};

/// The byte at position `index` of the test data.
fn pattern(index: usize) -> u8 {
    (index % 251) as u8
}

/// Reads everything until the end of the file, returning each buffer as
/// soon as it is read: even the end of the file takes a read, and so a
/// buffer.
fn read_all(driver: &Driver, stream: &mut ReadStream) -> Vec<u8> {
    let mut data = Vec::new();
    while let Some(lease) = block_on(driver, stream.next()) {
        data.extend_from_slice(&lease.unwrap());
    }
    data
}

#[test]
fn data_arrives_until_the_writer_closes() {
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 4, 64).unwrap();
    let (reader, mut writer) = io::pipe().unwrap();
    let mut stream = ReadStream::new(&driver, reader.as_raw_fd(), &pool);

    writer.write_all(b"hello ").unwrap();
    let first = block_on(&driver, stream.next()).unwrap().unwrap();
    assert_eq!(&*first, b"hello ");
    writer.write_all(b"world").unwrap();
    drop(writer);
    assert_eq!(read_all(&driver, &mut stream), b"world");
    assert!(
        block_on(&driver, stream.next()).is_none(),
        "the stream stays ended"
    );
    // One request read both, and the end.
    assert_eq!(driver.stats().operations, 1);
    drop(first);
    assert_eq!(pool.available(), 4);
}

/// A regular file cannot be polled, which the kernel says at once.
#[test]
fn a_file_that_cannot_be_polled_is_refused() {
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 2, 64).unwrap();
    let file = std::fs::File::open("/proc/self/exe").unwrap();
    let mut stream = ReadStream::new(&driver, file.as_raw_fd(), &pool);
    let err = block_on(&driver, stream.next()).unwrap().unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EBADFD));
    assert!(block_on(&driver, stream.next()).is_none());
    assert_eq!(pool.available(), 2);
}

/// With every buffer leased, the kernel ends the read with ENOBUFS and the
/// stream waits. Returning a buffer wakes it; it re-arms the read and the
/// rest of the data arrives, in order.
#[test]
fn the_stream_rearms_once_an_exhausted_pool_has_a_buffer() {
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 2, 16).unwrap();
    let (reader, mut writer) = io::pipe().unwrap();
    let sent: Vec<u8> = (0..64).map(pattern).collect();
    writer.write_all(&sent).unwrap();
    let mut stream = ReadStream::new(&driver, reader.as_raw_fd(), &pool);

    let first = block_on(&driver, stream.next()).unwrap().unwrap();
    let second = block_on(&driver, stream.next()).unwrap().unwrap();
    assert_eq!(first.len() + second.len(), 32);
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
    assert_eq!(woken.count(), starved, "nothing wakes a starved stream");

    let mut received = first.to_vec();
    received.extend_from_slice(&second);
    drop(first);
    assert_eq!(
        woken.count(),
        starved + 1,
        "returning a buffer wakes the stream"
    );
    drop(second);
    while received.len() < sent.len() {
        let lease = block_on(&driver, stream.next()).unwrap().unwrap();
        received.extend_from_slice(&lease);
    }
    assert_eq!(received, sent);
    drop(writer);
    assert!(block_on(&driver, stream.next()).is_none());
    assert_eq!(pool.available(), 2);
}

/// A writer outpaces a reader that has two small buffers, so the kernel
/// keeps running the pool dry and the stream keeps re-arming the read;
/// every byte must still arrive, in order.
#[test]
fn no_data_is_lost_across_many_rearms() {
    const TOTAL: usize = 20_000;
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 2, 8).unwrap();
    let (reader, mut writer) = io::pipe().unwrap();
    let producer = thread::spawn(move || {
        let data: Vec<u8> = (0..TOTAL).map(pattern).collect();
        for chunk in data.chunks(100) {
            writer.write_all(chunk).unwrap();
        }
    });

    let mut stream = ReadStream::new(&driver, reader.as_raw_fd(), &pool);
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
    let submissions = driver.stats().operations;
    assert!(
        submissions >= 10,
        "the read was submitted {submissions} times"
    );
    assert_eq!(pool.available(), 2);
}

/// `next_ready` takes what has been read, without waiting.
#[test]
fn next_ready_takes_what_has_been_read() {
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 8, 4).unwrap();
    let (reader, mut writer) = io::pipe().unwrap();
    let mut stream = ReadStream::new(&driver, reader.as_raw_fd(), &pool);
    assert!(stream.next_ready().is_none(), "nothing is even submitted");
    let (_, waker) = Counter::waker();
    assert!(
        stream
            .poll_next(&mut Context::from_waker(&waker))
            .is_pending()
    );
    writer.write_all(b"0123456789").unwrap();
    driver.submit().unwrap();

    let mut received = Vec::new();
    let mut leases = Vec::new();
    while let Some(lease) = stream.next_ready() {
        let lease = lease.unwrap();
        received.extend_from_slice(&lease);
        leases.push(lease);
    }
    assert_eq!(received, b"0123456789");
    assert_eq!(leases.len(), 3, "four bytes to a buffer");
    drop((leases, writer));
    assert!(block_on(&driver, stream.next()).is_none());
}

/// Buffers the kernel filled for a stream that is dropped before taking
/// them go back to the pool, and the pool takes data again.
#[test]
fn a_dropped_stream_returns_every_buffer() {
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 4, 16).unwrap();
    let (reader, mut writer) = io::pipe().unwrap();
    let mut stream = ReadStream::new(&driver, reader.as_raw_fd(), &pool);
    writer.write_all(&[7; 16]).unwrap();
    let kept = block_on(&driver, stream.next()).unwrap().unwrap();
    let completions = driver.stats().completions;
    writer.write_all(&[8; 48]).unwrap();
    driver.submit().unwrap();
    assert!(driver.stats().completions > completions);

    drop(stream);
    settle(&driver);
    assert_eq!(pool.available(), 3);
    drop(kept);
    assert_eq!(pool.available(), 4);

    let mut stream = ReadStream::new(&driver, reader.as_raw_fd(), &pool);
    let sent: Vec<u8> = (0..64).map(pattern).collect();
    writer.write_all(&sent).unwrap();
    drop(writer);
    assert_eq!(read_all(&driver, &mut stream), sent);
    assert_eq!(pool.available(), 4);
}
