//! The operations in `ruio_driver::ops`, against real files and pipes.

mod support;

use std::cell::Cell;
use std::fs::{self, File};
use std::io::{self, Read as _, Seek, SeekFrom, Write as _};
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::{Duration, Instant};

use ruio_driver::buf::IoBuf;
use ruio_driver::ops::{Read, Timeout, Write};
use ruio_driver::{Completion, Driver, Fd, Operation};
use ruio_sys as sys;

use support::{block_on, poll_once, settle};

/// A file in the test's scratch directory, removed when dropped.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> (Self, File) {
        let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name);
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        (Self(path), file)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

#[test]
fn a_timeout_completes_after_its_duration() {
    let driver = Driver::new().unwrap();
    let start = Instant::now();
    block_on(&driver, driver.op(Timeout::new(Duration::from_millis(20)))).unwrap();
    assert!(start.elapsed() >= Duration::from_millis(20));
}

#[test]
fn bytes_written_to_a_pipe_can_be_read_back() {
    let driver = Driver::new().unwrap();
    let (reader, writer) = io::pipe().unwrap();

    let write = Write::new(writer.as_raw_fd(), b"hello".to_vec());
    let (written, buf) = block_on(&driver, driver.op(write));
    assert_eq!(written.unwrap(), 5);
    assert_eq!(buf, b"hello");

    let read = Read::new(reader.as_raw_fd(), Vec::with_capacity(16));
    let (read, buf) = block_on(&driver, driver.op(read));
    assert_eq!(read.unwrap(), 5);
    assert_eq!(buf, b"hello");
}

#[test]
fn a_read_fills_the_buffer_from_its_start() {
    let driver = Driver::new().unwrap();
    let (reader, mut writer) = io::pipe().unwrap();
    writer.write_all(b"new data").unwrap();

    let buf = Vec::from(b"old".to_vec().into_boxed_slice());
    let (read, buf) = block_on(&driver, driver.op(Read::new(reader.as_raw_fd(), buf)));
    assert_eq!(read.unwrap(), 3);
    assert_eq!(buf, b"new");
}

#[test]
fn a_read_into_the_spare_capacity_appends() {
    let driver = Driver::new().unwrap();
    let (reader, mut writer) = io::pipe().unwrap();
    writer.write_all(b"cd").unwrap();

    let mut buf = Vec::with_capacity(8);
    buf.extend_from_slice(b"ab");
    let spare = buf.slice(2..);
    let (read, spare) = block_on(&driver, driver.op(Read::new(reader.as_raw_fd(), spare)));
    assert_eq!(read.unwrap(), 2);
    assert_eq!(spare.into_inner(), b"abcd");
}

#[test]
fn an_empty_slice_reads_nothing() {
    let driver = Driver::new().unwrap();
    let (reader, mut writer) = io::pipe().unwrap();
    writer.write_all(b"unread").unwrap();

    let buf = Vec::from(b"full".to_vec().into_boxed_slice()).slice(4..);
    let (read, buf) = block_on(&driver, driver.op(Read::new(reader.as_raw_fd(), buf)));
    assert_eq!(read.unwrap(), 0);
    assert_eq!(buf.into_inner(), b"full");
}

#[test]
fn writes_take_any_buffer_or_part_of_one() {
    let driver = Driver::new().unwrap();
    let (mut reader, writer) = io::pipe().unwrap();
    let fd = writer.as_raw_fd();
    assert_eq!(
        block_on(&driver, driver.op(Write::new(fd, "static ")))
            .0
            .unwrap(),
        7
    );
    let part = b"<boxed>".to_vec().into_boxed_slice().slice(1..6);
    assert_eq!(
        block_on(&driver, driver.op(Write::new(fd, part)))
            .0
            .unwrap(),
        5
    );
    drop(writer);

    let mut written = String::new();
    reader.read_to_string(&mut written).unwrap();
    assert_eq!(written, "static boxed");
}

#[test]
fn offsets_leave_the_file_position_alone() {
    let driver = Driver::new().unwrap();
    let (_scratch, mut file) = Scratch::new("offsets");
    file.write_all(b"0123456789").unwrap();
    file.seek(SeekFrom::Start(2)).unwrap();

    let write = Write::at(file.as_raw_fd(), b"abc".to_vec(), 5);
    assert_eq!(block_on(&driver, driver.op(write)).0.unwrap(), 3);
    let read = Read::at(file.as_raw_fd(), Vec::with_capacity(4), 4);
    let (read, buf) = block_on(&driver, driver.op(read));
    assert_eq!(read.unwrap(), 4);
    assert_eq!(buf, b"4abc");

    // The position is still where `seek` left it.
    let mut rest = String::new();
    file.read_to_string(&mut rest).unwrap();
    assert_eq!(rest, "234abc89");
}

#[test]
fn errors_come_back_with_the_buffer() {
    let driver = Driver::new().unwrap();
    let (read, buf) = block_on(&driver, driver.op(Read::new(-1, Vec::with_capacity(4))));
    assert_eq!(read.unwrap_err().raw_os_error(), Some(libc::EBADF));
    assert_eq!(buf.capacity(), 4);
}

/// An operation that records the result it completed with.
struct Tracked<T> {
    operation: T,
    result: Rc<Cell<Option<i32>>>,
}

fn tracked<T>(operation: T) -> (Tracked<T>, Rc<Cell<Option<i32>>>) {
    let result = Rc::new(Cell::new(None));
    (
        Tracked {
            operation,
            result: Rc::clone(&result),
        },
        result,
    )
}

// SAFETY: as for the operation it wraps.
unsafe impl<T: Operation> Operation for Tracked<T> {
    type Output = ();

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        self.operation.prepare(sqe);
    }

    fn complete(self, completion: Completion) {
        self.result.set(Some(completion.res()));
        drop(self.operation.complete(completion));
    }
}

/// Detached before or after it is submitted, an operation runs to its end,
/// where dropping it would cancel it.
#[test]
fn a_detached_operation_runs_to_completion() {
    let driver = Driver::new().unwrap();
    let (reader, mut writer) = io::pipe().unwrap();
    for submitted in [false, true] {
        let (read, result) = tracked(Read::new(reader.as_raw_fd(), Vec::with_capacity(8)));
        let mut op = driver.op(read);
        if submitted {
            assert!(poll_once(&mut op).is_pending());
            driver.submit().unwrap();
        }
        op.detach();
        driver.submit().unwrap();
        assert_eq!(result.get(), None, "waiting for data");
        writer.write_all(b"data").unwrap();
        settle(&driver);
        assert_eq!(result.get(), Some(4), "submitted first: {submitted}");
    }

    let (read, result) = tracked(Read::new(reader.as_raw_fd(), Vec::with_capacity(8)));
    let mut op = driver.op(read);
    assert!(poll_once(&mut op).is_pending());
    driver.submit().unwrap();
    drop(op);
    settle(&driver);
    assert_eq!(result.get(), Some(-libc::ECANCELED));
}

#[test]
fn a_detached_operation_times_out_as_a_polled_one_does() {
    let driver = Driver::new().unwrap();
    let (reader, _writer) = io::pipe().unwrap();
    let (read, result) = tracked(Read::new(reader.as_raw_fd(), Vec::with_capacity(8)));
    driver.op(read).timeout(Duration::from_millis(10)).detach();
    settle(&driver);
    assert_eq!(result.get(), Some(-libc::ETIMEDOUT));
}

/// Only an operation that has not been submitted comes back.
#[test]
fn an_operation_comes_back_only_before_it_is_submitted() {
    let driver = Driver::new().unwrap();
    let (reader, _writer) = io::pipe().unwrap();
    let (read, result) = tracked(Read::new(reader.as_raw_fd(), Vec::with_capacity(8)));
    assert!(driver.op(read).into_inner().is_some());
    assert_eq!(driver.stats().operations, 0);
    assert_eq!(result.get(), None);

    let (read, result) = tracked(Read::new(reader.as_raw_fd(), Vec::with_capacity(8)));
    let mut op = driver.op(read);
    assert!(poll_once(&mut op).is_pending());
    assert!(op.into_inner().is_none());
    settle(&driver);
    assert_eq!(result.get(), Some(-libc::ECANCELED));
}

/// A direct descriptor names a slot of the ring's registered file table,
/// never the process's descriptor of the same number.
#[test]
fn a_direct_descriptor_is_no_plain_one() {
    let driver = Driver::new().unwrap();
    let (reader, mut writer) = io::pipe().unwrap();
    writer.write_all(b"unread").unwrap();
    let slot = u32::try_from(reader.as_raw_fd()).unwrap();
    let read = Read::new(Fd::Direct(slot), Vec::with_capacity(8));
    let (read, _) = block_on(&driver, driver.op(read));
    assert_eq!(read.unwrap_err().raw_os_error(), Some(libc::EBADF));

    let read = Read::new(Fd::Plain(reader.as_raw_fd()), Vec::with_capacity(8));
    let (read, buf) = block_on(&driver, driver.op(read));
    assert_eq!(read.unwrap(), 6);
    assert_eq!(buf, b"unread");
}

#[test]
fn parking_times_out_with_operations_in_flight() {
    let driver = Driver::new().unwrap();
    let mut op = driver.op(Timeout::new(Duration::from_secs(60)));
    assert!(poll_once(&mut op).is_pending());

    let start = Instant::now();
    driver.park(Some(Duration::from_millis(20))).unwrap();
    assert!(start.elapsed() >= Duration::from_millis(20));
    assert!(poll_once(&mut op).is_pending());
    assert_eq!(driver.in_flight(), 1);
}

#[test]
fn dropping_a_blocked_read_cancels_it() {
    let driver = Driver::new().unwrap();
    let (reader, _writer) = io::pipe().unwrap();
    let mut op = driver.op(Read::new(reader.as_raw_fd(), Vec::with_capacity(8)));
    assert!(poll_once(&mut op).is_pending());
    driver.submit().unwrap();
    assert_eq!(driver.in_flight(), 1);

    let start = Instant::now();
    drop(op);
    settle(&driver);
    assert!(start.elapsed() < Duration::from_secs(1));
}

#[test]
fn dropping_a_long_timeout_cancels_it() {
    let driver = Driver::new().unwrap();
    let mut op = driver.op(Timeout::new(Duration::from_secs(60)));
    assert!(poll_once(&mut op).is_pending());
    driver.submit().unwrap();

    let start = Instant::now();
    drop(op);
    settle(&driver);
    assert!(start.elapsed() < Duration::from_secs(1));
}
