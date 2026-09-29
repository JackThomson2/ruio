//! Linked timeouts: operations the kernel cuts short.

mod support;

use std::io::{self, Write as _};
use std::os::fd::AsRawFd;
use std::task::Poll;
use std::time::{Duration, Instant};

use ruio_driver::Driver;
use ruio_driver::ops::{Nop, Read};

use support::{block_on, poll_once, settle};

#[test]
fn a_timed_out_read_fails_and_hands_back_its_buffer() {
    let driver = Driver::new().unwrap();
    let (reader, _writer) = io::pipe().unwrap();
    let read = Read::new(reader.as_raw_fd(), Vec::with_capacity(8));

    let start = Instant::now();
    let op = driver.op(read).timeout(Duration::from_millis(20));
    let (read, buf) = block_on(&driver, op);
    let err = read.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::TimedOut);
    assert_eq!(err.raw_os_error(), Some(libc::ETIMEDOUT));
    assert!(start.elapsed() >= Duration::from_millis(20));
    assert_eq!(buf.capacity(), 8);
    assert_eq!(driver.in_flight(), 0);
}

/// Once the operation is done, its timeout is too: it neither fires later
/// nor touches the next operation in the operation's slot.
#[test]
fn an_operation_that_finishes_in_time_is_unaffected() {
    let driver = Driver::new().unwrap();
    let (reader, mut writer) = io::pipe().unwrap();
    writer.write_all(b"data").unwrap();
    let read = Read::new(reader.as_raw_fd(), Vec::with_capacity(8));
    let op = driver.op(read).timeout(Duration::from_millis(20));
    let (read, buf) = block_on(&driver, op);
    assert_eq!(read.unwrap(), 4);
    assert_eq!(buf, b"data");

    let mut next = driver.op(Read::new(reader.as_raw_fd(), Vec::with_capacity(8)));
    assert!(poll_once(&mut next).is_pending());
    let start = Instant::now();
    while start.elapsed() < Duration::from_millis(60) {
        driver.park(Some(Duration::from_millis(10))).unwrap();
    }
    assert!(
        poll_once(&mut next).is_pending(),
        "cut short by a stale timeout"
    );
    writer.write_all(b"more").unwrap();
    let (read, buf) = block_on(&driver, next);
    assert_eq!(read.unwrap(), 4);
    assert_eq!(buf, b"more");
}

/// An operation and its timeout take two entries, which must reach the
/// kernel together: with room for two, every timed operation fills the
/// queue, and an untimed one before it leaves room for only half of it.
#[test]
fn timed_operations_fit_the_smallest_queue() {
    for entries in [1, 2, 4] {
        let driver = Driver::builder().entries(entries).build().unwrap();
        let mut ops: Vec<_> = (0..20)
            .map(|index| {
                let op = driver.op(Nop);
                if index % 3 == 0 {
                    op
                } else {
                    op.timeout(Duration::from_secs(10))
                }
            })
            .collect();
        for op in &mut ops {
            assert!(poll_once(op).is_pending());
        }
        let start = Instant::now();
        settle(&driver);
        assert!(start.elapsed() < Duration::from_secs(5));
        for op in &mut ops {
            assert!(matches!(poll_once(op), Poll::Ready(Ok(()))));
        }
    }
}

#[test]
fn dropping_a_timed_operation_cancels_it_at_once() {
    let driver = Driver::new().unwrap();
    let (reader, _writer) = io::pipe().unwrap();
    let read = Read::new(reader.as_raw_fd(), Vec::with_capacity(8));
    let mut op = driver.op(read).timeout(Duration::from_secs(60));
    assert!(poll_once(&mut op).is_pending());
    driver.submit().unwrap();

    let start = Instant::now();
    drop(op);
    settle(&driver);
    assert!(start.elapsed() < Duration::from_secs(1));
}

/// A timeout set after submission would have to reach the kernel with the
/// operation, which has gone already.
#[test]
fn a_timeout_set_after_submission_has_no_effect() {
    let driver = Driver::new().unwrap();
    let (reader, mut writer) = io::pipe().unwrap();
    let mut op = driver.op(Read::new(reader.as_raw_fd(), Vec::with_capacity(8)));
    assert!(poll_once(&mut op).is_pending());
    let mut op = op.timeout(Duration::from_millis(1));
    let start = Instant::now();
    while start.elapsed() < Duration::from_millis(30) {
        driver.park(Some(Duration::from_millis(10))).unwrap();
    }
    assert!(poll_once(&mut op).is_pending());
    writer.write_all(b"late").unwrap();
    assert_eq!(block_on(&driver, op).0.unwrap(), 4);
}
