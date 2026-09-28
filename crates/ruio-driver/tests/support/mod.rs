//! Helpers shared by the integration tests.

#![allow(dead_code)]

use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use ruio_driver::Driver;

/// How long a test may wait for the driver before it is considered hung.
pub(crate) const PATIENCE: Duration = Duration::from_secs(10);

/// Runs `future` to completion, parking on `driver` whenever it is pending.
///
/// # Panics
///
/// If the future is still pending after [`PATIENCE`].
pub(crate) fn block_on<F: Future>(driver: &Driver, future: F) -> F::Output {
    let mut future = pin!(future);
    let woken = Arc::new(Flag(AtomicBool::new(true)));
    let waker = Waker::from(woken.clone());
    let mut cx = Context::from_waker(&waker);
    let deadline = Instant::now() + PATIENCE;
    loop {
        if woken.0.swap(false, Ordering::AcqRel) {
            if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
                return output;
            }
        } else {
            let now = Instant::now();
            assert!(
                now < deadline,
                "the future is still pending after {PATIENCE:?}"
            );
            driver.park(Some(deadline - now)).unwrap();
        }
    }
}

/// Parks until every operation, including dropped ones, has completed.
///
/// # Panics
///
/// If operations are still in flight after [`PATIENCE`].
pub(crate) fn settle(driver: &Driver) {
    let deadline = Instant::now() + PATIENCE;
    while driver.in_flight() > 0 {
        let now = Instant::now();
        assert!(
            now < deadline,
            "{} operations still in flight after {PATIENCE:?}",
            driver.in_flight()
        );
        driver.park(Some(deadline - now)).unwrap();
    }
}

/// Polls `future` once with a waker that does nothing.
pub(crate) fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    Pin::new(future).poll(&mut Context::from_waker(Waker::noop()))
}

struct Flag(AtomicBool);

impl Wake for Flag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::Release);
    }
}

/// A waker that counts how often it is woken.
#[derive(Default)]
pub(crate) struct Counter(AtomicUsize);

impl Counter {
    pub(crate) fn waker() -> (Arc<Counter>, Waker) {
        let counter = Arc::new(Counter::default());
        (counter.clone(), Waker::from(counter))
    }

    pub(crate) fn count(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }
}

impl Wake for Counter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}
