//! Dropping a driver while the kernel still has operations.

mod support;

use std::cell::Cell;
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::rc::Rc;
use std::time::{Duration, Instant};

use ruio_driver::{Completion, Driver, Operation};
use ruio_sys as sys;

use support::poll_once;

/// Reads from a descriptor into a boxed buffer, recording when it is
/// dropped.
struct TrackedRead {
    fd: RawFd,
    buf: Box<[u8; 16]>,
    dropped: Rc<Cell<bool>>,
}

impl TrackedRead {
    fn new(fd: RawFd) -> (Self, Rc<Cell<bool>>) {
        let dropped = Rc::new(Cell::new(false));
        let read = Self {
            fd,
            buf: Box::new([0; 16]),
            dropped: dropped.clone(),
        };
        (read, dropped)
    }
}

// SAFETY: the entry points into the boxed buffer, which stays put when the
// operation moves. A read posts one completion.
unsafe impl Operation for TrackedRead {
    type Output = io::Result<u32>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_READ;
        sqe.fd = self.fd;
        sqe.addr = self.buf.as_mut_ptr().expose_provenance() as u64;
        sqe.len = self.buf.len() as u32;
        sqe.off = u64::MAX;
    }

    fn complete(self, completion: Completion) -> io::Result<u32> {
        completion.result()
    }
}

impl Drop for TrackedRead {
    fn drop(&mut self) {
        self.dropped.set(true);
    }
}

#[test]
fn dropping_the_driver_waits_for_the_kernel_to_finish() {
    let (reader, _writer) = io::pipe().unwrap();
    // Dropped after the read reached the kernel, and before.
    for submitted in [true, false] {
        let driver = Driver::new().unwrap();
        let (read, dropped) = TrackedRead::new(reader.as_raw_fd());
        let mut op = driver.op(read);
        assert!(poll_once(&mut op).is_pending());
        if submitted {
            driver.submit().unwrap();
        }
        drop(op);
        assert!(!dropped.get(), "released while the read may be in flight");

        let start = Instant::now();
        drop(driver);
        assert!(dropped.get(), "not released once the kernel was done");
        assert!(start.elapsed() < Duration::from_secs(1));
    }
}

#[test]
fn dropping_an_idle_driver_does_not_wait() {
    let driver = Driver::builder()
        .shutdown_timeout(Duration::from_secs(60))
        .build()
        .unwrap();
    let start = Instant::now();
    drop(driver);
    assert!(start.elapsed() < Duration::from_secs(1));
}
