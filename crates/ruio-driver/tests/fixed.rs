//! Registered buffers and the fixed operations.

mod support;

use std::fs::{self, File};
use std::io::{self, Write as _};
use std::os::fd::AsRawFd;
use std::path::PathBuf;

use ruio_driver::Driver;
use ruio_driver::buf::{FixedBuffers, IoBuf};
use ruio_driver::ops::{ReadFixed, WriteFixed};

use support::{block_on, poll_once, settle};

const SIZE: usize = 4096;

#[test]
fn data_goes_through_registered_buffers() {
    let driver = Driver::new().unwrap();
    let buffers = FixedBuffers::new(&driver, 4, SIZE).unwrap();
    let (reader, writer) = io::pipe().unwrap();

    let mut out = buffers.take().unwrap();
    out.extend_from_slice(b"registered");
    let (written, out) = block_on(&driver, driver.op(WriteFixed::new(writer.as_raw_fd(), out)));
    assert_eq!(written.unwrap(), 10);
    assert_eq!(&*out, b"registered");

    let into = buffers.take().unwrap();
    assert_ne!(into.index(), out.index());
    let (read, into) = block_on(&driver, driver.op(ReadFixed::new(reader.as_raw_fd(), into)));
    assert_eq!(read.unwrap(), 10);
    assert_eq!(&*into, b"registered");
}

#[test]
fn fixed_reads_take_offsets_and_slices() {
    let driver = Driver::new().unwrap();
    let buffers = FixedBuffers::new(&driver, 1, SIZE).unwrap();
    let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("fixed-offsets");
    File::create(&path)
        .unwrap()
        .write_all(b"0123456789")
        .unwrap();
    let file = File::open(&path).unwrap();

    let mut buf = buffers.take().unwrap();
    buf.extend_from_slice(b"ab");
    let spare = buf.slice(2..);
    let read = ReadFixed::at(file.as_raw_fd(), spare, 6);
    let (read, spare) = block_on(&driver, driver.op(read));
    assert_eq!(read.unwrap(), 4);
    assert_eq!(&*spare.into_inner(), b"ab6789");
    fs::remove_file(path).unwrap();
}

#[test]
fn buffers_are_checked_out_once_and_come_back() {
    let driver = Driver::new().unwrap();
    let buffers = FixedBuffers::new(&driver, 2, SIZE).unwrap();
    assert_eq!((buffers.count(), buffers.buffer_size()), (2, SIZE));

    let first = buffers.take().unwrap();
    let second = buffers.take().unwrap();
    assert!(buffers.take().is_none());
    assert!(buffers.check_out(first.index()).is_none());
    assert!(buffers.check_out(2).is_none(), "there is no buffer 2");

    let index = first.index();
    drop(first);
    assert_eq!(buffers.check_out(index).unwrap().index(), index);
    drop(second);
}

#[test]
fn buffers_start_empty_and_zeroed() {
    let driver = Driver::new().unwrap();
    let buffers = FixedBuffers::new(&driver, 1, 64).unwrap();
    let mut buf = buffers.take().unwrap();
    assert!(buf.is_empty());
    buf.set_len(64);
    assert!(buf.iter().all(|&byte| byte == 0));
}

#[test]
fn a_ring_holds_one_set_until_it_is_gone() {
    let driver = Driver::new().unwrap();
    let buffers = FixedBuffers::new(&driver, 1, SIZE).unwrap();
    let err = FixedBuffers::new(&driver, 1, SIZE).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EBUSY));

    // A checked-out buffer keeps the set registered, and usable.
    let mut kept = buffers.take().unwrap();
    drop(buffers);
    assert!(FixedBuffers::new(&driver, 1, SIZE).is_err());
    let (reader, writer) = io::pipe().unwrap();
    kept.extend_from_slice(b"still registered");
    let (written, kept) = block_on(
        &driver,
        driver.op(WriteFixed::new(writer.as_raw_fd(), kept)),
    );
    assert_eq!(written.unwrap(), 16);
    drop((kept, reader));

    // Gone entirely, the set was unregistered, making room for another.
    FixedBuffers::new(&driver, 1, SIZE).unwrap();
}

#[test]
fn a_buffer_of_another_ring_is_refused() {
    let driver = Driver::new().unwrap();
    let other = Driver::new().unwrap();
    let buffers = FixedBuffers::new(&other, 1, SIZE).unwrap();
    let (reader, _writer) = io::pipe().unwrap();
    let read = ReadFixed::new(reader.as_raw_fd(), buffers.take().unwrap());
    let (read, _) = block_on(&driver, driver.op(read));
    assert_eq!(read.unwrap_err().raw_os_error(), Some(libc::EFAULT));
}

#[test]
fn a_cancelled_fixed_read_returns_its_buffer() {
    let driver = Driver::new().unwrap();
    let buffers = FixedBuffers::new(&driver, 1, SIZE).unwrap();
    let (reader, _writer) = io::pipe().unwrap();
    let mut op = driver.op(ReadFixed::new(reader.as_raw_fd(), buffers.take().unwrap()));
    assert!(poll_once(&mut op).is_pending());
    driver.submit().unwrap();
    drop(op);
    assert!(buffers.take().is_none(), "the kernel may still write to it");
    settle(&driver);
    assert!(buffers.take().is_some());
}

#[test]
fn zero_buffers_are_refused() {
    let driver = Driver::new().unwrap();
    let err = FixedBuffers::new(&driver, 0, SIZE).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}
