//! Intervals: ticks at a period, what follows a missed one, and resets.

use std::future::poll_fn;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use ruio_executor::time::{Interval, MissedTickBehavior, interval, interval_at, sleep, timeout};
use ruio_executor::{Runtime, spawn, yield_now};

/// How late a timer may fire on a loaded machine before a test fails.
const SLACK: Duration = Duration::from_millis(250);

/// Long enough for any of these to finish; a lost wake runs it out.
const PATIENCE: Duration = Duration::from_secs(20);

/// The period of the intervals that keep up.
const PERIOD: Duration = Duration::from_millis(20);

/// The period of the intervals whose task falls behind: long enough that a
/// task late by [`SLACK`] is still within half a period, so that the ticks
/// due at once are exactly those expected.
const LONG: Duration = Duration::from_millis(600);

/// Runs `future` on a runtime of its own, failing if it takes longer than
/// [`PATIENCE`], as it does if a wake is lost.
fn run<F: Future>(future: F) -> F::Output {
    Runtime::new()
        .unwrap()
        .block_on(timeout(PATIENCE, future))
        .expect("finished in time, with no wake lost")
}

/// Takes the ticks that are due, without waiting for one.
async fn due(ticks: &mut Interval) -> Vec<Instant> {
    let mut due = Vec::new();
    loop {
        match poll_fn(|cx| Poll::Ready(ticks.poll_tick(cx))).await {
            Poll::Ready(tick) => due.push(tick),
            Poll::Pending => return due,
        }
    }
}

/// The clock has reached `tick`, and not run on past it by [`SLACK`].
fn on_time(tick: Instant) {
    let now = Instant::now();
    assert!(now >= tick, "{:?} early", tick - now);
    assert!(now < tick + SLACK, "{:?} late", now - tick);
}

#[test]
fn the_first_tick_is_immediate_and_the_rest_a_period_apart() {
    let start = Instant::now();
    run(async {
        let mut ticks = interval(PERIOD);
        let made = Instant::now();
        let first = ticks.tick().await;
        assert!(first >= start && first <= made);
        assert!(start.elapsed() < SLACK);
        for n in 1..=3 {
            let tick = ticks.tick().await;
            assert_eq!(tick, first + PERIOD * n);
            on_time(tick);
        }
    });
}

#[test]
fn an_interval_at_starts_at_its_start() {
    run(async {
        let start = Instant::now() + Duration::from_millis(30);
        let mut ticks = interval_at(start, PERIOD);
        assert_eq!(ticks.tick().await, start);
        on_time(start);
        assert_eq!(ticks.tick().await, start + PERIOD);
        on_time(start + PERIOD);
    });
}

/// Three and a half periods after the first tick, three are due; a burst
/// takes them at once and keeps their cadence.
#[test]
fn a_burst_takes_the_missed_ticks_at_once() {
    run(async {
        let mut ticks = interval(LONG);
        let first = ticks.tick().await;
        sleep(LONG * 7 / 2).await;
        let due = due(&mut ticks).await;
        assert_eq!(due, [first + LONG, first + LONG * 2, first + LONG * 3]);
        let fourth = ticks.tick().await;
        assert_eq!(fourth, first + LONG * 4);
        on_time(fourth);
    });
}

/// A delay takes the missed tick, and the next comes a period after it was
/// taken.
#[test]
fn a_delay_starts_the_cadence_over_from_the_late_tick() {
    run(async {
        let mut ticks = interval(LONG);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let first = ticks.tick().await;
        sleep(LONG * 7 / 2).await;
        let before = Instant::now();
        let due = due(&mut ticks).await;
        let after = Instant::now();
        assert_eq!(due, [first + LONG]);
        let next = ticks.tick().await;
        assert!(next >= before + LONG, "{:?} early", before + LONG - next);
        assert!(next <= after + LONG, "{:?} late", next - after - LONG);
        on_time(next);
    });
}

/// A skip takes the missed tick, drops the ones passed meanwhile, and the
/// next comes at the cadence's first deadline ahead.
#[test]
fn a_skip_rejoins_the_cadence() {
    run(async {
        let mut ticks = interval(LONG);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let first = ticks.tick().await;
        sleep(LONG * 7 / 2).await;
        let due = due(&mut ticks).await;
        assert_eq!(due, [first + LONG]);
        let next = ticks.tick().await;
        assert_eq!(next, first + LONG * 4);
        on_time(next);
    });
}

/// A task back less than a period late has missed nothing: the cadence
/// holds, whatever the behaviour.
#[test]
fn a_tick_taken_less_than_a_period_late_is_not_missed() {
    run(async {
        let mut ticks = interval(LONG);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let first = ticks.tick().await;
        sleep(LONG * 3 / 2).await;
        assert_eq!(due(&mut ticks).await, [first + LONG]);
        assert_eq!(ticks.tick().await, first + LONG * 2);
    });
}

#[test]
fn a_reset_puts_the_next_tick_a_period_from_now() {
    run(async {
        let mut ticks = interval_at(Instant::now() + Duration::from_secs(60), PERIOD);
        let before = Instant::now();
        ticks.reset();
        let after = Instant::now();
        let tick = ticks.tick().await;
        assert!(
            tick >= before + PERIOD && tick <= after + PERIOD,
            "{tick:?}"
        );
        on_time(tick);
        assert_eq!(ticks.tick().await, tick + PERIOD);
    });
}

#[test]
fn a_reset_immediately_ticks_at_once() {
    run(async {
        let mut ticks = interval_at(Instant::now() + Duration::from_secs(60), PERIOD);
        let before = Instant::now();
        ticks.reset_immediately();
        let after = Instant::now();
        let tick = ticks.tick().await;
        assert!(tick >= before && tick <= after, "{tick:?}");
        assert!(before.elapsed() < SLACK);
        assert_eq!(ticks.tick().await, tick + PERIOD);
    });
}

#[test]
fn a_reset_after_waits_as_long_as_asked() {
    run(async {
        let mut ticks = interval_at(Instant::now() + Duration::from_secs(60), PERIOD);
        let before = Instant::now();
        ticks.reset_after(Duration::from_millis(40));
        let after = Instant::now();
        let tick = ticks.tick().await;
        assert!(tick >= before + Duration::from_millis(40), "{tick:?}");
        assert!(tick <= after + Duration::from_millis(40), "{tick:?}");
        on_time(tick);
        assert_eq!(ticks.tick().await, tick + PERIOD);
    });
}

#[test]
fn a_reset_at_ticks_at_the_deadline_and_keeps_the_period_after() {
    run(async {
        let mut ticks = interval(PERIOD);
        let first = ticks.tick().await;
        let deadline = first + Duration::from_millis(50);
        ticks.reset_at(deadline);
        assert_eq!(ticks.tick().await, deadline);
        on_time(deadline);
        assert_eq!(ticks.tick().await, deadline + PERIOD);
    });
}

#[test]
fn an_interval_tells_its_period_and_behaviour() {
    let mut ticks = interval(PERIOD);
    assert_eq!(ticks.period(), PERIOD);
    assert_eq!(ticks.missed_tick_behavior(), MissedTickBehavior::Burst);
    assert_eq!(MissedTickBehavior::default(), MissedTickBehavior::Burst);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
    assert_eq!(ticks.missed_tick_behavior(), MissedTickBehavior::Skip);
    let debug = format!("{ticks:?}");
    assert!(debug.starts_with("Interval {"), "{debug}");
    assert!(debug.contains("Skip"), "{debug}");

    let later = interval_at(Instant::now(), Duration::from_secs(3));
    assert_eq!(later.period(), Duration::from_secs(3));
}

#[test]
#[should_panic(expected = "must not be zero")]
fn an_interval_with_a_zero_period_panics() {
    let _ = interval(Duration::ZERO);
}

#[test]
#[should_panic(expected = "must not be zero")]
fn an_interval_at_with_a_zero_period_panics() {
    let _ = interval_at(Instant::now(), Duration::ZERO);
}

#[test]
fn an_interval_ticks_in_a_spawned_task() {
    let start = Instant::now();
    let taken = run(async {
        spawn(async {
            let mut ticks = interval(PERIOD);
            let mut taken = Vec::new();
            for _ in 0..4 {
                taken.push(ticks.tick().await);
            }
            taken
        })
        .await
        .unwrap()
    });
    assert_eq!(taken.len(), 4);
    for pair in taken.windows(2) {
        assert_eq!(pair[1] - pair[0], PERIOD);
    }
    let elapsed = start.elapsed();
    assert!(elapsed >= PERIOD * 3, "{elapsed:?}");
    assert!(elapsed < PERIOD * 3 + SLACK, "{elapsed:?}");
}

/// An interval dropped, or a task aborted, with a tick pending takes its
/// timer out of the wheel: nothing holds the runtime up.
#[test]
fn a_dropped_interval_does_not_hold_the_runtime() {
    let runtime = Runtime::new().unwrap();
    let start = Instant::now();
    runtime.block_on(async {
        let far = Instant::now() + Duration::from_secs(60);
        let mut ticks = interval_at(far, Duration::from_secs(60));
        let polled = poll_fn(|cx| Poll::Ready(ticks.poll_tick(cx))).await;
        assert!(polled.is_pending());
        drop(ticks);
        let task = spawn(async move {
            let mut ticks = interval_at(far, Duration::from_secs(60));
            ticks.tick().await;
        });
        yield_now().await;
        task.abort();
        sleep(Duration::from_millis(10)).await;
    });
    drop(runtime);
    assert!(start.elapsed() < Duration::from_millis(10) + SLACK);
}

#[test]
fn intervals_can_be_sent() {
    fn send_sync<T: Send + Sync>(_: &T) {}
    send_sync(&interval(PERIOD));
}

#[test]
fn a_due_tick_completes_outside_a_runtime() {
    let mut ticks = interval(PERIOD);
    let polled = ticks.poll_tick(&mut Context::from_waker(Waker::noop()));
    assert!(polled.is_ready());
}

#[test]
#[should_panic(expected = "inside a ruio runtime")]
fn polling_a_tick_outside_a_runtime_panics() {
    let mut ticks = interval_at(Instant::now() + Duration::from_secs(1), PERIOD);
    let _ = ticks.poll_tick(&mut Context::from_waker(Waker::noop()));
}
