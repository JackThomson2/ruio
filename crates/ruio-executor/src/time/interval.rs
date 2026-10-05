//! Ticking at a period: an [`Interval`] yields a tick every so long, for
//! work that recurs, and says what to do about a tick its task was too late
//! to take.
//!
//! An interval is a [`Sleep`] whose deadline is the next tick's, with the
//! period beside it. Taking a tick polls the sleep; once it has ended, the
//! interval reads the clock once, finds the deadline of the tick after from
//! its [`MissedTickBehavior`], moves the sleep's deadline there, and yields
//! the deadline of the tick it took, which the cadence set, rather than the
//! instant the task took it. So a tick costs the removal of one timer from
//! the runtime's wheel and the arming of one, and allocates nothing; and an
//! interval is `Send` and `Sync`, as a sleep is.
//!
//! A tick is due once a period, and only the ticks a task has missed come
//! at once, so an interval, like a sleep, spends no step of the task's
//! cooperative budget.

use std::fmt;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use super::{Instant, Sleep, deadline_after, sleep_until};

/// Ticks every `period`, the first tick at once.
///
/// # Panics
///
/// If `period` is zero.
pub fn interval(period: Duration) -> Interval {
    interval_at(Instant::now(), period)
}

/// Ticks every `period`, the first tick at `start`.
///
/// # Panics
///
/// If `period` is zero.
pub fn interval_at(start: Instant, period: Duration) -> Interval {
    assert!(
        period > Duration::ZERO,
        "an interval's period must not be zero"
    );
    Interval {
        sleep: sleep_until(start),
        period,
        missed_tick_behavior: MissedTickBehavior::default(),
    }
}

/// Ticks at a period. Returned by [`interval`] and [`interval_at`].
///
/// Each tick is due a period after the one before, whether or not the task
/// has taken that one yet; a task that takes a tick a period or more late
/// has missed it, and the interval's [`MissedTickBehavior`] says when the
/// one after comes. A tick yields its deadline, not the instant it was
/// taken, so the ticks of a task that keeps up are exactly a period apart.
///
/// ```
/// use std::time::Duration;
///
/// use ruio_executor::time::interval;
///
/// # ruio_executor::Runtime::new()?.block_on(async {
/// let mut ticks = interval(Duration::from_millis(10));
/// let first = ticks.tick().await;
/// let second = ticks.tick().await;
/// assert_eq!(second - first, Duration::from_millis(10));
/// # });
/// # Ok::<(), std::io::Error>(())
/// ```
#[must_use = "an interval does nothing until its ticks are polled"]
pub struct Interval {
    /// Ends at the next tick's deadline.
    sleep: Sleep,
    period: Duration,
    missed_tick_behavior: MissedTickBehavior,
}

impl Interval {
    /// The next tick, waiting for it: its deadline.
    ///
    /// # Panics
    ///
    /// If the tick is not due and the thread is not inside a runtime.
    pub async fn tick(&mut self) -> Instant {
        poll_fn(|cx| self.poll_tick(cx)).await
    }

    /// The next tick's deadline if the tick is due; otherwise arranges for
    /// the task to be woken when it is.
    ///
    /// # Panics
    ///
    /// If the tick is not due and the thread is not inside a runtime.
    #[inline]
    pub fn poll_tick(&mut self, cx: &mut Context<'_>) -> Poll<Instant> {
        ready!(Pin::new(&mut self.sleep).poll(cx));
        let deadline = self.sleep.deadline();
        // The one read of the clock a tick takes: none while it is pending.
        let now = Instant::now();
        let next = self
            .missed_tick_behavior
            .next_tick(deadline, now, self.period);
        self.sleep.reset(next);
        Poll::Ready(deadline)
    }

    /// Puts the next tick a period from now, and the ones after at the
    /// period from it: the cadence starts over, and a tick that was due is
    /// forgotten.
    pub fn reset(&mut self) {
        self.sleep.reset(deadline_after(self.period));
    }

    /// Puts the next tick now, and the ones after at the period from it.
    pub fn reset_immediately(&mut self) {
        self.sleep.reset(Instant::now());
    }

    /// Puts the next tick `after` from now, and the ones after at the period
    /// from it.
    pub fn reset_after(&mut self, after: Duration) {
        self.sleep.reset(deadline_after(after));
    }

    /// Puts the next tick at `deadline`, and the ones after at the period
    /// from it.
    pub fn reset_at(&mut self, deadline: Instant) {
        self.sleep.reset(deadline);
    }

    /// How far apart the ticks are.
    #[inline]
    pub fn period(&self) -> Duration {
        self.period
    }

    /// What the interval does about a tick its task has missed.
    #[inline]
    pub fn missed_tick_behavior(&self) -> MissedTickBehavior {
        self.missed_tick_behavior
    }

    /// Sets what the interval does about a tick its task has missed, from
    /// the next tick on.
    #[inline]
    pub fn set_missed_tick_behavior(&mut self, behavior: MissedTickBehavior) {
        self.missed_tick_behavior = behavior;
    }
}

impl fmt::Debug for Interval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Interval")
            .field("deadline", &self.sleep.deadline())
            .field("period", &self.period)
            .field("missed_tick_behavior", &self.missed_tick_behavior)
            .finish_non_exhaustive()
    }
}

/// What an [`Interval`] does about a tick its task has missed.
///
/// A task takes a tick when it polls the interval and finds the tick due.
/// One that is back a period or more after a tick was due has missed it: the
/// tick after is due too, or has been. The interval yields the missed tick
/// all the same, as nothing else tells the task its work fell behind, and
/// the behaviour says when the one after comes. In the examples below the
/// period is 10 ms and ticks are due at 10, 20, 30 and 40 ms; the task takes
/// the tick of 10 ms on time and is back at 35 ms.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum MissedTickBehavior {
    /// The ticks keep their deadlines, and those missed come at once, until
    /// the interval has caught up: the task takes the ticks of 20 and 30 ms
    /// back to back at 35 ms, and the tick of 40 ms at 40 ms. The default,
    /// under which the ticks over any stretch of time add up to its length
    /// in periods, however late some of them come.
    #[default]
    Burst,
    /// The next tick comes a period after the missed one is taken, and the
    /// ones after follow it: the task takes the tick of 20 ms at 35 ms, the
    /// next at 45 ms and then one at 55 ms. For work that needs a period's
    /// rest between runs, however late it falls.
    Delay,
    /// The next tick comes at the first deadline of the original cadence
    /// still ahead, and those passed are dropped: the task takes the tick of
    /// 20 ms at 35 ms, the next at 40 ms and then one at 50 ms. For work
    /// that must stay in step with the clock.
    Skip,
}

impl MissedTickBehavior {
    /// The deadline of the tick after the one due at `deadline`, taken at
    /// `now`.
    #[inline]
    fn next_tick(self, deadline: Instant, now: Instant, period: Duration) -> Instant {
        let next = later(deadline, period);
        if now <= next {
            return next;
        }
        match self {
            Self::Burst => next,
            Self::Delay => later(now, period),
            Self::Skip => {
                // The periods from the missed tick to `now`, and one more,
                // so that the next tick is after `now`; the period is not
                // zero.
                let elapsed = now.saturating_duration_since(deadline).as_nanos();
                let periods = elapsed / period.as_nanos() + 1;
                let skipped = u32::try_from(periods)
                    .ok()
                    .and_then(|periods| period.checked_mul(periods))
                    .unwrap_or(Duration::MAX);
                later(deadline, skipped)
            }
        }
    }
}

/// The instant `duration` after `instant`, or a century after it if that
/// is later than an `Instant` can hold.
#[inline]
fn later(instant: Instant, duration: Duration) -> Instant {
    instant
        .checked_add(duration)
        .or_else(|| instant.checked_add(Duration::from_secs(100 * 365 * 24 * 60 * 60)))
        .unwrap_or(instant)
}
