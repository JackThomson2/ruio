//! The driver's submission and completion machinery, through its public API.

mod support;

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use ruio_driver::ops::Nop;
use ruio_driver::{Driver, Op};

use support::{Counter, block_on, poll_once, settle};

#[test]
fn a_nop_completes() {
    let driver = Driver::new().unwrap();
    block_on(&driver, driver.op(Nop)).unwrap();
    assert_eq!(driver.in_flight(), 0);
}

#[test]
fn queued_operations_share_one_system_call() {
    let driver = Driver::builder().entries(256).build().unwrap();
    let mut ops: Vec<Op<Nop>> = (0..100).map(|_| driver.op(Nop)).collect();
    for op in &mut ops {
        assert!(poll_once(op).is_pending());
    }
    // Nothing reaches the kernel until the driver is entered.
    assert_eq!(driver.stats().enters, 0);
    assert_eq!(driver.in_flight(), 100);

    driver.park(None).unwrap();
    let stats = driver.stats();
    assert_eq!(stats.enters, 1);
    assert_eq!(stats.operations, 100);
    assert_eq!(stats.completions, 100);
    for op in &mut ops {
        assert!(matches!(poll_once(op), Poll::Ready(Ok(()))));
    }

    // With nothing queued or pending, submitting needs no system call.
    driver.submit().unwrap();
    assert_eq!(driver.stats().enters, 1);
}

/// With four submission and sixteen completion slots, queueing a hundred
/// operations fills the submission queue again and again, and their
/// completions overflow the completion queue. None may be lost.
#[test]
fn a_full_submission_queue_is_submitted_early() {
    let driver = Driver::builder().entries(4).build().unwrap();
    let mut ops: Vec<Op<Nop>> = (0..100).map(|_| driver.op(Nop)).collect();
    for op in &mut ops {
        assert!(poll_once(op).is_pending());
    }
    assert!(driver.stats().enters >= 24, "{:?}", driver.stats());

    settle(&driver);
    for op in &mut ops {
        assert!(matches!(poll_once(op), Poll::Ready(Ok(()))));
    }
    assert_eq!(driver.stats().completions, 100);
}

#[test]
fn the_latest_waker_is_woken() {
    let driver = Driver::new().unwrap();
    let mut op = driver.op(Nop);
    let (first, first_waker) = Counter::waker();
    let (second, second_waker) = Counter::waker();
    let poll = |op: &mut Op<Nop>, waker| Pin::new(op).poll(&mut Context::from_waker(waker));

    assert!(poll(&mut op, &first_waker).is_pending());
    assert!(poll(&mut op, &second_waker).is_pending());
    driver.park(None).unwrap();

    assert_eq!((first.count(), second.count()), (0, 1));
    assert!(matches!(poll(&mut op, &second_waker), Poll::Ready(Ok(()))));
}

#[test]
fn an_op_dropped_before_polling_never_reaches_the_kernel() {
    let driver = Driver::new().unwrap();
    drop(driver.op(Nop));
    driver.submit().unwrap();
    assert_eq!(driver.stats().operations, 0);
    assert_eq!(driver.in_flight(), 0);
}

#[test]
fn an_op_dropped_in_flight_is_released_by_its_completion() {
    let driver = Driver::new().unwrap();
    let mut op = driver.op(Nop);
    assert!(poll_once(&mut op).is_pending());
    drop(op);
    assert_eq!(driver.in_flight(), 1);
    settle(&driver);
}

#[test]
fn an_op_dropped_after_completing_is_released_at_once() {
    let driver = Driver::new().unwrap();
    let mut op = driver.op(Nop);
    assert!(poll_once(&mut op).is_pending());
    driver.park(None).unwrap();
    assert_eq!(driver.in_flight(), 0);
    drop(op);
    assert_eq!(driver.in_flight(), 0);
}

#[test]
fn parking_with_nothing_in_flight_times_out() {
    let driver = Driver::new().unwrap();
    let start = Instant::now();
    driver.park(Some(Duration::from_millis(20))).unwrap();
    assert!(start.elapsed() >= Duration::from_millis(20));
}

#[test]
fn a_driver_outlives_its_handles_while_ops_exist() {
    let driver = Driver::new().unwrap();
    let mut op = driver.op(Nop);
    let handle = driver.clone();
    drop(driver);
    assert!(poll_once(&mut op).is_pending());
    handle.park(None).unwrap();
    assert!(matches!(poll_once(&mut op), Poll::Ready(Ok(()))));
}
