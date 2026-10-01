//! Chains of linked operations.

mod support;

use std::io::{self, PipeReader, Read as _, Write as _};
use std::os::fd::AsRawFd;
use std::panic::{self, AssertUnwindSafe};
use std::time::{Duration, Instant};

use ruio_driver::ops::{Nop, Read, Write};
use ruio_driver::{Completion, Driver, Operation};
use ruio_sys as sys;

use support::{block_on, poll_once, settle};

/// Whether data waits in the pipe, which this leaves there.
fn has_data(pipe: &PipeReader) -> bool {
    let mut poll = libc::pollfd {
        fd: pipe.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: `poll` reads and writes the one `pollfd` it is given.
    let ready = unsafe { libc::poll(&mut poll, 1, 0) };
    assert!(ready >= 0, "{}", io::Error::last_os_error());
    ready == 1
}

/// Lets the kernel run for a while, completing what it can.
fn idle(driver: &Driver) {
    driver.park(Some(Duration::from_millis(20))).unwrap();
}

fn errno<T>(result: io::Result<T>) -> Option<i32> {
    result.err().and_then(|err| err.raw_os_error())
}

/// The write waits in the kernel until the read before it has completed.
#[test]
fn an_operation_waits_for_the_one_before() {
    let driver = Driver::new().unwrap();
    let (input, mut feed) = io::pipe().unwrap();
    let (mut output, sink) = io::pipe().unwrap();
    let mut chain = driver
        .op(Read::new(input.as_raw_fd(), Vec::with_capacity(4)))
        .link(driver.op(Write::new(sink.as_raw_fd(), b"done".to_vec())));
    assert!(poll_once(&mut chain).is_pending());
    driver.submit().unwrap();
    idle(&driver);
    assert!(!has_data(&output), "the write waits for the read");

    feed.write_all(b"ping").unwrap();
    let ((read, data), (written, _)) = block_on(&driver, chain);
    assert_eq!(read.unwrap(), 4);
    assert_eq!(data, b"ping");
    assert_eq!(written.unwrap(), 4);
    let mut out = [0; 4];
    output.read_exact(&mut out).unwrap();
    assert_eq!(&out, b"done");
}

/// A failure cancels the rest of the chain, which hands back its buffers.
#[test]
fn a_failed_operation_cancels_the_rest() {
    let driver = Driver::new().unwrap();
    let (input, _feed) = io::pipe().unwrap();
    let (output, sink) = io::pipe().unwrap();
    // A pipe's reading end cannot be written to.
    let chain = driver
        .op(Write::new(input.as_raw_fd(), b"wrong".to_vec()))
        .link(driver.op(Write::new(sink.as_raw_fd(), b"never".to_vec())));
    let ((first, _), (second, buf)) = block_on(&driver, chain);
    assert_eq!(errno(first), Some(libc::EBADF));
    assert_eq!(errno(second), Some(libc::ECANCELED));
    assert_eq!(buf, b"never");
    assert!(!has_data(&output));
}

/// A read that fills less than it asked for counts as failing.
#[test]
fn a_short_read_breaks_the_chain() {
    let driver = Driver::new().unwrap();
    let (input, mut feed) = io::pipe().unwrap();
    let (output, sink) = io::pipe().unwrap();
    feed.write_all(b"abc").unwrap();
    let chain = driver
        .op(Read::new(input.as_raw_fd(), Vec::with_capacity(8)))
        .link(driver.op(Write::new(sink.as_raw_fd(), b"never".to_vec())));
    let ((read, data), (written, _)) = block_on(&driver, chain);
    assert_eq!(read.unwrap(), 3);
    assert_eq!(data, b"abc");
    assert_eq!(errno(written), Some(libc::ECANCELED));
    assert!(!has_data(&output));
}

/// The first operation's timeout cancels it, as `ETIMEDOUT`, and the rest
/// of the chain with it.
#[test]
fn a_timeout_cancels_the_rest_of_the_chain() {
    let driver = Driver::new().unwrap();
    let (input, _feed) = io::pipe().unwrap();
    let (output, sink) = io::pipe().unwrap();
    let chain = driver
        .op(Read::new(input.as_raw_fd(), Vec::with_capacity(4)))
        .timeout(Duration::from_millis(20))
        .link(driver.op(Write::new(sink.as_raw_fd(), b"never".to_vec())));
    let ((read, _), (written, _)) = block_on(&driver, chain);
    assert_eq!(read.unwrap_err().kind(), io::ErrorKind::TimedOut);
    assert_eq!(errno(written), Some(libc::ECANCELED));
    assert!(!has_data(&output));
}

/// Timeouts that do not fire carry the chain on, wherever they are in it,
/// and post nothing.
#[test]
fn timeouts_that_do_not_fire_carry_the_chain_on() {
    let driver = Driver::new().unwrap();
    let (mut output, sink) = io::pipe().unwrap();
    let long = Duration::from_secs(10);
    let started = Instant::now();
    let chain = driver
        .op(Nop)
        .timeout(long)
        .link(
            driver
                .op(Write::new(sink.as_raw_fd(), b"one".to_vec()))
                .timeout(long),
        )
        .link(
            driver
                .op(Write::new(sink.as_raw_fd(), b"two".to_vec()))
                .timeout(long),
        );
    let ((nop, (one, _)), (two, _)) = block_on(&driver, chain);
    nop.unwrap();
    assert_eq!(one.unwrap(), 3);
    assert_eq!(two.unwrap(), 3);
    assert!(started.elapsed() < long);
    let mut out = [0; 6];
    output.read_exact(&mut out).unwrap();
    assert_eq!(&out, b"onetwo");
    settle(&driver);
}

/// A later operation's own timeout cancels it as a failure before it
/// would: as `ECANCELED`.
#[test]
fn a_later_timeout_cancels_its_operation() {
    let driver = Driver::new().unwrap();
    let (input, _feed) = io::pipe().unwrap();
    let chain = driver.op(Nop).link(
        driver
            .op(Read::new(input.as_raw_fd(), Vec::with_capacity(4)))
            .timeout(Duration::from_millis(20)),
    );
    let (nop, (read, _)) = block_on(&driver, chain);
    nop.unwrap();
    assert_eq!(errno(read), Some(libc::ECANCELED));
}

/// Chains nest, and run in the order they are written, past the timeouts
/// between their operations.
#[test]
fn a_chain_of_three_runs_in_order() {
    let driver = Driver::new().unwrap();
    let (input, mut feed) = io::pipe().unwrap();
    let (mut output, sink) = io::pipe().unwrap();
    let long = Duration::from_secs(10);
    let mut chain = driver
        .op(Read::new(input.as_raw_fd(), Vec::with_capacity(2)))
        .timeout(long)
        .link(
            driver
                .op(Write::new(sink.as_raw_fd(), b"second".to_vec()))
                .timeout(long),
        )
        .link(driver.op(Write::new(sink.as_raw_fd(), b" third".to_vec())));
    assert!(poll_once(&mut chain).is_pending());
    driver.submit().unwrap();
    idle(&driver);
    assert!(!has_data(&output));
    feed.write_all(b"go").unwrap();
    let (((read, data), (second, _)), (third, _)) = block_on(&driver, chain);
    assert_eq!((read.unwrap(), data.as_slice()), (2, &b"go"[..]));
    assert_eq!(second.unwrap(), 6);
    assert_eq!(third.unwrap(), 6);
    let mut out = [0; 12];
    output.read_exact(&mut out).unwrap();
    assert_eq!(&out, b"second third");
}

/// Dropping a chain in flight cancels it: what had not run never does.
#[test]
fn dropping_a_chain_cancels_it() {
    let driver = Driver::new().unwrap();
    let (input, mut feed) = io::pipe().unwrap();
    let (output, sink) = io::pipe().unwrap();
    let mut chain = driver
        .op(Read::new(input.as_raw_fd(), Vec::with_capacity(4)))
        .link(driver.op(Write::new(sink.as_raw_fd(), b"never".to_vec())));
    assert!(poll_once(&mut chain).is_pending());
    driver.submit().unwrap();
    drop(chain);
    settle(&driver);
    feed.write_all(b"late").unwrap();
    idle(&driver);
    assert!(!has_data(&output));
    assert_eq!(driver.in_flight(), 0);
}

/// A chain the submission queue cannot hold fails as a whole, and hands
/// back every buffer.
#[test]
fn a_chain_too_long_for_the_ring_fails_every_operation() {
    let driver = Driver::builder().entries(2).build().unwrap();
    let (output, sink) = io::pipe().unwrap();
    let second = Duration::from_secs(1);
    let chain = driver
        .op(Write::new(sink.as_raw_fd(), b"first".to_vec()))
        .timeout(second)
        .link(
            driver
                .op(Write::new(sink.as_raw_fd(), b"second".to_vec()))
                .timeout(second),
        );
    let ((first, one), (second, two)) = block_on(&driver, chain);
    assert_eq!(errno(first), Some(libc::EBUSY));
    assert_eq!(errno(second), Some(libc::EBUSY));
    assert_eq!(
        (one.as_slice(), two.as_slice()),
        (&b"first"[..], &b"second"[..])
    );
    assert!(!has_data(&output));
    assert_eq!(driver.in_flight(), 0);
}

/// An operation whose `prepare` panics.
struct Panics;

// SAFETY: the entry is never filled in.
unsafe impl Operation for Panics {
    type Output = ();

    fn prepare(&mut self, _: &mut sys::io_uring_sqe) {
        panic!("prepare panics");
    }

    fn complete(self, _: Completion) {}
}

/// A panic preparing a later operation leaves nothing of the chain queued,
/// so that the earlier ones' buffers, which the panic frees, never reach
/// the kernel.
#[test]
fn a_panic_preparing_a_chain_queues_none_of_it() {
    let driver = Driver::new().unwrap();
    let (output, sink) = io::pipe().unwrap();
    let mut chain = driver
        .op(Write::new(sink.as_raw_fd(), b"freed".to_vec()))
        .link(driver.op(Panics));
    let panicked = panic::catch_unwind(AssertUnwindSafe(|| poll_once(&mut chain)));
    assert!(panicked.is_err());
    drop(chain);
    driver.submit().unwrap();
    idle(&driver);
    assert!(!has_data(&output));
    assert_eq!(driver.in_flight(), 0);
    assert_eq!(driver.stats().operations, 0);
}

#[test]
#[should_panic(expected = "an operation can be linked only before it is submitted")]
fn a_submitted_operation_cannot_be_linked() {
    let driver = Driver::new().unwrap();
    let mut first = driver.op(Nop);
    let _ = poll_once(&mut first);
    drop(first.link(driver.op(Nop)));
}

#[test]
#[should_panic(expected = "linked operations must be on the same ring")]
fn operations_on_different_rings_cannot_be_linked() {
    let driver = Driver::new().unwrap();
    let other = Driver::new().unwrap();
    drop(driver.op(Nop).link(other.op(Nop)));
}
