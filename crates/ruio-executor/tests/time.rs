//! Timers: sleep, sleep_until and timeout.

use std::cell::{Cell, RefCell};
use std::future::{Future, pending};
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

use ruio_executor::time::{Elapsed, Sleep, sleep, sleep_until, timeout};
use ruio_executor::{Runtime, spawn, yield_now};

/// How late a timer may fire on a loaded machine before a test fails.
const SLACK: Duration = Duration::from_millis(250);

#[test]
fn a_sleep_waits_for_its_duration() {
    let runtime = Runtime::new().unwrap();
    let start = Instant::now();
    runtime.block_on(sleep(Duration::from_millis(20)));
    let elapsed = start.elapsed();
    assert!(elapsed >= Duration::from_millis(20), "{elapsed:?}");
    assert!(elapsed < Duration::from_millis(20) + SLACK, "{elapsed:?}");
}

#[test]
fn sleeps_end_in_deadline_order() {
    let runtime = Runtime::new().unwrap();
    let order = Rc::new(RefCell::new(Vec::new()));
    let start = Instant::now();
    runtime.block_on(async {
        let tasks: Vec<_> = (0..50u64)
            .rev()
            .map(|n| {
                let order = order.clone();
                spawn(async move {
                    sleep(Duration::from_millis(2 * n)).await;
                    order.borrow_mut().push(n);
                })
            })
            .collect();
        for task in tasks {
            task.await.unwrap();
        }
    });
    let order = order.borrow();
    assert!(order.is_sorted(), "{order:?}");
    assert_eq!(order.len(), 50);
    assert!(start.elapsed() < Duration::from_millis(98) + SLACK);
}

#[test]
fn a_timeout_returns_the_output_in_time() {
    let runtime = Runtime::new().unwrap();
    let result = runtime.block_on(timeout(Duration::from_secs(10), async { 5 }));
    assert_eq!(result, Ok(5));
}

#[test]
fn a_timeout_drops_the_future_when_time_runs_out() {
    let runtime = Runtime::new().unwrap();
    let dropped = Rc::new(Cell::new(false));
    struct Flag(Rc<Cell<bool>>);
    impl Drop for Flag {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }
    let flag = Flag(dropped.clone());
    let start = Instant::now();
    let result = runtime.block_on(timeout(Duration::from_millis(20), async move {
        let _flag = flag;
        pending::<()>().await;
    }));
    let elapsed: Elapsed = result.unwrap_err();
    assert_eq!(elapsed.to_string(), "deadline has elapsed");
    assert!(dropped.get());
    assert!(start.elapsed() >= Duration::from_millis(20));

    let err = io::Error::from(elapsed);
    assert_eq!(err.kind(), io::ErrorKind::TimedOut);
}

#[test]
fn past_deadlines_end_at_once() {
    let runtime = Runtime::new().unwrap();
    let start = Instant::now();
    runtime.block_on(async {
        sleep(Duration::ZERO).await;
        sleep_until(start).await;
    });
    assert!(start.elapsed() < SLACK);
}

#[test]
fn a_reset_sleep_waits_for_the_new_deadline() {
    let runtime = Runtime::new().unwrap();
    let start = Instant::now();
    runtime.block_on(async {
        let mut nap = sleep(Duration::from_secs(60));
        nap.reset(Instant::now() + Duration::from_millis(10));
        nap.await;
    });
    assert!(start.elapsed() < Duration::from_millis(10) + SLACK);
}

#[test]
fn dropped_and_endless_sleeps_do_not_hold_the_runtime() {
    let runtime = Runtime::new().unwrap();
    let start = Instant::now();
    runtime.block_on(async {
        let endless = spawn(sleep(Duration::MAX));
        let long = spawn(sleep(Duration::from_secs(60)));
        yield_now().await;
        long.abort();
        sleep(Duration::from_millis(10)).await;
        endless.abort();
    });
    assert!(start.elapsed() < Duration::from_millis(10) + SLACK);
}

#[test]
fn timers_cost_the_kernel_one_timeout() {
    let runtime = Runtime::new().unwrap();
    runtime.block_on(async {
        // One deadline for all, however long spawning takes: with a deadline
        // each, a slow machine spreads them over several ticks, and the
        // ring is entered once per tick.
        let deadline = Instant::now() + Duration::from_millis(20);
        let tasks: Vec<_> = (0..1000).map(|_| spawn(sleep_until(deadline))).collect();
        for task in tasks {
            task.await.unwrap();
        }
    });
    let stats = runtime.driver().stats();
    // No kernel operations at all: one wait for the ring, or a few if it is
    // woken early.
    assert_eq!(stats.operations, 0);
    assert!(stats.enters <= 3, "{stats:?}");
}

#[test]
fn timers_fire_while_tasks_keep_yielding() {
    let runtime = Runtime::new().unwrap();
    let start = Instant::now();
    runtime.block_on(async {
        let spinners: Vec<_> = (0..100)
            .map(|_| {
                spawn(async {
                    loop {
                        yield_now().await;
                    }
                })
            })
            .collect();
        sleep(Duration::from_millis(20)).await;
        for spinner in &spinners {
            spinner.abort();
        }
    });
    assert!(start.elapsed() < Duration::from_millis(20) + SLACK);
}

/// A waker that counts its wakes.
#[derive(Default)]
struct Count(AtomicUsize);

impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Polls `nap` once inside `runtime`, with a waker that counts: its timer
/// goes into that runtime's wheel.
fn arm(runtime: &Runtime, nap: &mut Sleep) -> Arc<Count> {
    let count = Arc::new(Count::default());
    let waker = Waker::from(count.clone());
    runtime.block_on(async {
        let polled = Pin::new(nap).poll(&mut Context::from_waker(&waker));
        assert!(polled.is_pending());
    });
    count
}

#[test]
fn sleeps_and_timeouts_can_be_sent() {
    fn send_sync<T: Send + Sync>(_: &T) {}
    fn send<T: Send>(_: &T) {}
    send_sync(&sleep(Duration::from_secs(1)));
    send(&timeout(
        Duration::from_secs(1),
        sleep(Duration::from_secs(1)),
    ));
}

/// A future spawned from another thread, which must be `Send`, can sleep
/// and time out.
#[test]
fn a_future_sent_to_a_runtime_can_sleep() {
    let runtime = Runtime::new().unwrap();
    let handle = runtime.handle();
    let start = Instant::now();
    let task = thread::spawn(move || {
        handle.spawn(async {
            sleep(Duration::from_millis(20)).await;
            timeout(Duration::from_millis(10), pending::<()>()).await
        })
    })
    .join()
    .unwrap();
    assert!(runtime.block_on(task).unwrap().is_err());
    let elapsed = start.elapsed();
    assert!(elapsed >= Duration::from_millis(30), "{elapsed:?}");
    assert!(elapsed < Duration::from_millis(30) + SLACK, "{elapsed:?}");
}

/// Polled by a runtime on another thread, a sleep armed in one ends at its
/// deadline there, and its timer leaves the first runtime's wheel, which
/// would otherwise wake whoever polled it there.
#[test]
fn a_sleep_polled_by_another_runtime_moves_its_timer_there() {
    let first = Runtime::new().unwrap();
    let deadline = Instant::now() + Duration::from_millis(30);
    let mut nap = sleep_until(deadline);
    let woken = arm(&first, &mut nap);
    let nap = thread::spawn(move || {
        let second = Runtime::new().unwrap();
        // A timer in the same place of this wheel as the nap's in the
        // first, which the nap must not take for its own.
        let mut long = sleep(Duration::from_secs(60));
        arm(&second, &mut long);
        let ended = second.block_on(timeout(Duration::from_secs(10), &mut nap));
        assert_eq!(ended, Ok(()));
        let now = Instant::now();
        assert!(now >= deadline);
        assert!(now < deadline + SLACK, "{:?} late", now - deadline);
        nap
    })
    .join()
    .unwrap();
    assert!(nap.is_elapsed());
    // Past the deadline, the first runtime has no timer of the sleep's left
    // to fire.
    first.block_on(sleep(Duration::from_millis(10)));
    assert_eq!(woken.0.load(Ordering::SeqCst), 0);
}

/// A sleep dropped on another thread takes its timer out of its runtime's
/// wheel.
#[test]
fn a_sleep_dropped_on_another_thread_removes_its_timer() {
    let runtime = Runtime::new().unwrap();
    let deadline = Instant::now() + Duration::from_millis(20);
    let mut nap = sleep_until(deadline);
    let woken = arm(&runtime, &mut nap);
    thread::spawn(move || drop(nap)).join().unwrap();
    runtime.block_on(sleep_until(deadline + Duration::from_millis(10)));
    assert_eq!(woken.0.load(Ordering::SeqCst), 0);
}

/// A sleep can outlive the runtime its timer was in, and be polled by
/// another.
#[test]
fn a_sleep_outlives_its_runtime() {
    let mut nap = sleep(Duration::from_millis(10));
    let first = Runtime::new().unwrap();
    arm(&first, &mut nap);
    drop(first);
    Runtime::new().unwrap().block_on(&mut nap);

    let mut nap = sleep(Duration::from_secs(60));
    let first = Runtime::new().unwrap();
    arm(&first, &mut nap);
    drop(first);
    drop(nap);
}

#[test]
#[should_panic(expected = "inside a ruio runtime")]
fn polling_a_sleep_outside_a_runtime_panics() {
    let mut nap = sleep(Duration::from_secs(1));
    let _ = Pin::new(&mut nap).poll(&mut Context::from_waker(Waker::noop()));
}
