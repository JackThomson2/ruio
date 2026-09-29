//! Multishot requests and their streams, through a multishot poll.

mod support;

use std::cell::Cell;
use std::io::{self, Write as _};
use std::os::fd::{AsRawFd, RawFd};
use std::rc::Rc;
use std::task::Poll;
use std::time::{Duration, Instant};

use ruio_driver::{Completion, Driver, Multishot};
use ruio_sys as sys;

use support::{block_on, settle};

/// A multishot poll for readability, counting every completion it is given,
/// including those that arrive after its stream is dropped.
struct PollReadable {
    fd: RawFd,
    delivered: Rc<Cell<usize>>,
}

impl PollReadable {
    fn new(fd: RawFd) -> (Self, Rc<Cell<usize>>) {
        let delivered = Rc::new(Cell::new(0));
        let poll = Self {
            fd,
            delivered: delivered.clone(),
        };
        (poll, delivered)
    }
}

// SAFETY: a poll points to no memory, and a multishot poll's completions
// carry IORING_CQE_F_MORE until the last.
unsafe impl Multishot for PollReadable {
    type Item = io::Result<u32>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_POLL_ADD;
        sqe.fd = self.fd;
        sqe.op_flags = libc::POLLIN as u32;
        sqe.len = sys::IORING_POLL_ADD_MULTI;
    }

    fn item(&mut self, completion: Completion) -> io::Result<u32> {
        self.delivered.set(self.delivered.get() + 1);
        completion.result()
    }
}

/// Polls a stream once, with a waker that does nothing.
fn poll_once<T: Multishot>(stream: &mut ruio_driver::MultiOp<T>) -> Poll<Option<T::Item>> {
    stream.poll_next(&mut std::task::Context::from_waker(std::task::Waker::noop()))
}

#[test]
fn each_readiness_is_an_item() {
    let driver = Driver::new().unwrap();
    let (reader, mut writer) = io::pipe().unwrap();
    let (poll, delivered) = PollReadable::new(reader.as_raw_fd());
    let mut stream = driver.stream(poll);
    assert!(poll_once(&mut stream).is_pending());
    assert!(stream.is_armed());

    for round in 1..=3 {
        writer.write_all(b"x").unwrap();
        let events = block_on(&driver, stream.next()).unwrap().unwrap();
        assert_ne!(events & libc::POLLIN as u32, 0);
        assert_eq!(delivered.get(), round);
        assert!(stream.is_armed(), "a multishot poll stays armed");
    }
}

#[test]
fn an_ended_request_can_be_rearmed() {
    let driver = Driver::new().unwrap();
    // The kernel ends a poll on a bad descriptor at once, without
    // IORING_CQE_F_MORE.
    let (poll, delivered) = PollReadable::new(-1);
    let mut stream = driver.stream(poll);
    for round in 1..=2 {
        let err = block_on(&driver, stream.next()).unwrap().unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EBADF));
        assert!(!stream.is_armed());
        assert!(
            block_on(&driver, stream.next()).is_none(),
            "ended until rearmed"
        );
        assert_eq!(delivered.get(), round);
        stream.rearm();
    }
    assert_eq!(driver.in_flight(), 0);
}

#[test]
fn a_dropped_stream_is_cancelled_and_still_given_its_completions() {
    let driver = Driver::new().unwrap();
    let (reader, mut writer) = io::pipe().unwrap();
    let (poll, delivered) = PollReadable::new(reader.as_raw_fd());
    let mut stream = driver.stream(poll);
    assert!(poll_once(&mut stream).is_pending());
    driver.submit().unwrap();
    writer.write_all(b"x").unwrap();
    driver.submit().unwrap();
    assert_eq!(delivered.get(), 0, "the completion waits in the stream");

    drop(stream);
    assert_eq!(delivered.get(), 1, "the untaken completion is handed over");
    let start = Instant::now();
    settle(&driver);
    assert!(start.elapsed() < Duration::from_secs(1));
    assert_eq!(delivered.get(), 2, "and so is the cancellation");
}

#[test]
fn dropping_the_driver_drains_abandoned_streams() {
    let (reader, _writer) = io::pipe().unwrap();
    let driver = Driver::new().unwrap();
    let (poll, delivered) = PollReadable::new(reader.as_raw_fd());
    let mut stream = driver.stream(poll);
    assert!(poll_once(&mut stream).is_pending());
    driver.submit().unwrap();
    drop(stream);
    drop(driver);
    assert_eq!(delivered.get(), 1, "the cancellation reached the value");
}

#[test]
fn streams_dropped_after_every_number_of_polls() {
    let driver = Driver::new().unwrap();
    let (reader, mut writer) = io::pipe().unwrap();
    for polls in 0..5 {
        let (poll, delivered) = PollReadable::new(reader.as_raw_fd());
        let mut stream = driver.stream(poll);
        let mut taken = 0;
        for _ in 0..polls {
            if let Poll::Ready(item) = poll_once(&mut stream) {
                item.unwrap().unwrap();
                taken += 1;
            }
            writer.write_all(b"x").unwrap();
            driver.submit().unwrap();
        }
        drop(stream);
        settle(&driver);
        assert!(delivered.get() >= taken, "{polls} polls");
    }
    assert_eq!(driver.in_flight(), 0);
}
