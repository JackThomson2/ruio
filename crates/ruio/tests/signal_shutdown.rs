//! A signal that arrives as a runtime ends.
//!
//! Every runtime with a listener keeps a read of the one eventfd that
//! signals are counted on, and the read that takes a count hands the
//! signals on. A runtime that ends after its read took a count, before its
//! dispatcher ran again, still hands them on. The test has a binary of its
//! own, so that no other runtime's read can take the count first.

mod support;

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use ruio::signal::{SignalKind, signal};
use ruio::time::timeout;

use support::within;

/// How long a signal handed on may take to reach a listener.
const SOON: Duration = Duration::from_millis(500);

#[test]
fn a_runtime_that_ends_as_its_read_takes_a_signal_hands_it_on() {
    let kind = SignalKind::user_defined1();
    let ending = ruio::Runtime::new().unwrap();
    ending.block_on(async {
        let _listening = signal(kind).unwrap();
        let driver = ruio::driver();
        // The dispatcher's first poll queues its read of the eventfd, which
        // goes to the kernel and waits there for a count.
        let idle = driver.in_flight();
        for _ in 0..100 {
            if driver.in_flight() > idle {
                break;
            }
            ruio::yield_now().await;
        }
        assert_eq!(driver.in_flight(), idle + 1, "the dispatcher reads");
        driver.submit().unwrap();
        // The handler runs on this thread before `raise` returns, and its
        // write wakes the read, which takes the count as the ring waits.
        // SAFETY: `raise` of a signal whose handler `signal` installed.
        assert_eq!(unsafe { libc::raise(kind.as_raw_value()) }, 0);
        let mut completed = false;
        for _ in 0..10 {
            driver.park(Some(Duration::from_millis(500))).unwrap();
            if driver.completions_last_turn() > 0 {
                completed = true;
                break;
            }
        }
        assert!(completed, "the read takes the count");
        // Done: `block_on` returns before the dispatcher, which the read's
        // completion woke, runs again.
    });

    let (made, listening) = mpsc::channel();
    let other = thread::spawn(move || {
        ruio::Runtime::new().unwrap().block_on(async {
            let mut later = signal(kind).unwrap();
            made.send(()).unwrap();
            within(timeout(SOON, later.recv()))
                .await
                .expect("the signal is handed on")
                .expect("the listener is open");
        });
    });
    listening.recv().unwrap();
    drop(ending);
    other.join().unwrap();
}
