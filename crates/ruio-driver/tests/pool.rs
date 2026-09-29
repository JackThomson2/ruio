//! Provided-buffer pools and pooled reads.

mod support;

use std::io::{self, Write as _};
use std::os::fd::AsRawFd;

use ruio_driver::Driver;
use ruio_driver::buf::BufPool;
use ruio_driver::ops::ReadPooled;

use support::{block_on, poll_once, settle};

#[test]
fn data_arrives_in_a_lease_that_returns_its_buffer() {
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 4, 64).unwrap();
    assert_eq!(
        (pool.count(), pool.buffer_size(), pool.available()),
        (4, 64, 4)
    );
    let (reader, mut writer) = io::pipe().unwrap();
    writer.write_all(b"pooled").unwrap();

    let lease = block_on(
        &driver,
        driver.op(ReadPooled::new(reader.as_raw_fd(), &pool)),
    )
    .unwrap()
    .unwrap();
    assert_eq!(&*lease, b"pooled");
    assert_eq!((lease.len(), lease.capacity()), (6, 64));
    assert_eq!(pool.available(), 3);
    drop(lease);
    assert_eq!(pool.available(), 4);
}

/// With every buffer leased, a read fails with ENOBUFS; once a lease
/// returns its buffer, reads work again.
#[test]
fn an_exhausted_pool_fails_reads_until_a_buffer_returns() {
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 2, 64).unwrap();
    let (reader, mut writer) = io::pipe().unwrap();
    let mut read = || {
        writer.write_all(b"x").unwrap();
        block_on(
            &driver,
            driver.op(ReadPooled::new(reader.as_raw_fd(), &pool)),
        )
    };

    let first = read().unwrap().unwrap();
    let second = read().unwrap().unwrap();
    assert_eq!(pool.available(), 0);
    let err = read().unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOBUFS));

    drop(first);
    // The data of the failed read is still in the pipe, followed by this one.
    let third = read().unwrap().unwrap();
    assert_eq!(&*third, b"xx");
    drop((second, third));
    assert_eq!(pool.available(), 2);
}

#[test]
fn the_end_of_the_file_takes_no_buffer() {
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 2, 64).unwrap();
    let (reader, writer) = io::pipe().unwrap();
    drop(writer);
    let read = block_on(
        &driver,
        driver.op(ReadPooled::new(reader.as_raw_fd(), &pool)),
    );
    assert!(read.unwrap().is_none());
    assert_eq!(pool.available(), 2);
}

#[test]
fn buffers_are_used_again_and_again() {
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 2, 16).unwrap();
    let (reader, mut writer) = io::pipe().unwrap();
    for round in 0..1000u32 {
        let message = round.to_string();
        writer.write_all(message.as_bytes()).unwrap();
        let lease = block_on(
            &driver,
            driver.op(ReadPooled::new(reader.as_raw_fd(), &pool)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(&*lease, message.as_bytes());
    }
    assert_eq!(pool.available(), 2);
}

#[test]
fn a_cancelled_read_takes_no_buffer() {
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 2, 64).unwrap();
    let (reader, _writer) = io::pipe().unwrap();
    let mut op = driver.op(ReadPooled::new(reader.as_raw_fd(), &pool));
    assert!(poll_once(&mut op).is_pending());
    driver.submit().unwrap();
    drop(op);
    settle(&driver);
    assert_eq!(pool.available(), 2);
}

#[test]
fn a_lease_outlives_its_pool_handle() {
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 1, 64).unwrap();
    let (reader, mut writer) = io::pipe().unwrap();
    writer.write_all(b"kept").unwrap();
    let lease = block_on(
        &driver,
        driver.op(ReadPooled::new(reader.as_raw_fd(), &pool)),
    )
    .unwrap()
    .unwrap();
    drop(pool);
    assert_eq!(&*lease, b"kept");
}

#[test]
fn pools_of_one_ring_keep_to_their_own_buffers() {
    let driver = Driver::new().unwrap();
    let small = BufPool::new(&driver, 1, 4).unwrap();
    let large = BufPool::new(&driver, 1, 64).unwrap();
    let (reader, mut writer) = io::pipe().unwrap();
    writer.write_all(b"0123456789").unwrap();

    let head = block_on(
        &driver,
        driver.op(ReadPooled::new(reader.as_raw_fd(), &small)),
    );
    assert_eq!(&*head.unwrap().unwrap(), b"0123");
    let rest = block_on(
        &driver,
        driver.op(ReadPooled::new(reader.as_raw_fd(), &large)),
    );
    assert_eq!(&*rest.unwrap().unwrap(), b"456789");
}

#[test]
fn pools_need_a_power_of_two_count_and_a_size() {
    let driver = Driver::new().unwrap();
    for (count, size) in [(3, 64), (0, 64), (4, 0)] {
        let err = BufPool::new(&driver, count, size).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }
}
