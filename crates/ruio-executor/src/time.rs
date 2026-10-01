//! Timers: waiting for a deadline, and bounding how long a future may take.
//!
//! Timers live in each runtime's timing wheel, in user space, with a
//! resolution of a millisecond. The kernel sees a single timeout: how long the
//! runtime may park before the wheel's next deadline.
//!
//! A [`Sleep`] is `Send` and `Sync`, although its timer is in the wheel of
//! one runtime, so that it can be part of a future that must be: one given
//! to [`spawn_on`](crate::spawn_on), say, or one that a library such as
//! hyper asks of a timer.

use std::cell::{Cell, RefCell};
use std::error::Error;
use std::fmt;
use std::future::{Future, IntoFuture, poll_fn};
use std::io;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

pub use std::time::Instant;

use crate::context;
use crate::scheduler::Shared;
use crate::wheel::{Key, Wheel};

/// A runtime's timers: its wheel, whose ticks are milliseconds since `start`.
pub(crate) struct Timers {
    start: Instant,
    wheel: RefCell<Wheel>,
    /// Wakers of the timers that fired together, kept to reuse the
    /// allocation.
    fired: RefCell<Vec<Waker>>,
    /// When the wheel next has work, as its last change left it, or `None`
    /// if it has none: firing the timers before then has nothing to do.
    next: Cell<Option<Instant>>,
    /// `start`, as near as can be, on `CLOCK_MONOTONIC`, which the ring's
    /// timer takes deadlines on: read after `start`, so that a deadline it
    /// gives the ring is not early.
    start_monotonic: Duration,
}

impl Timers {
    pub(crate) fn new() -> Self {
        let start = Instant::now();
        Self {
            start,
            wheel: RefCell::new(Wheel::new()),
            fired: RefCell::new(Vec::new()),
            next: Cell::new(None),
            start_monotonic: ruio_driver::monotonic_now(),
        }
    }

    /// When the wheel next has work, on `CLOCK_MONOTONIC`: the deadline to
    /// wake the ring at.
    pub(crate) fn next_deadline(&self) -> Option<Duration> {
        let next = self.next.get()?;
        Some(self.start_monotonic + next.saturating_duration_since(self.start))
    }

    /// Records when the wheel next has work, after a change to it.
    fn refresh(&self, wheel: &Wheel) {
        let next = wheel
            .next_expiration()
            .and_then(|tick| self.start.checked_add(Duration::from_millis(tick)));
        self.next.set(next);
    }

    /// The last tick that has begun by `instant`.
    fn tick_at(&self, instant: Instant) -> u64 {
        let millis = instant.saturating_duration_since(self.start).as_millis();
        u64::try_from(millis).unwrap_or(u64::MAX)
    }

    /// The first tick that begins at or after `deadline`, so a timer never
    /// fires early.
    fn tick_after(&self, deadline: Instant) -> u64 {
        let nanos = deadline.saturating_duration_since(self.start).as_nanos();
        u64::try_from(nanos.div_ceil(1_000_000)).unwrap_or(u64::MAX)
    }

    fn insert(&self, deadline: Instant, waker: Waker) -> Key {
        let mut wheel = self.wheel.borrow_mut();
        let tick = self.tick_after(deadline).max(wheel.elapsed() + 1);
        let key = wheel.insert(tick, waker);
        self.refresh(&wheel);
        key
    }

    /// Removes a timer, fired or not. Returns its waker, if it still had one,
    /// for the caller to drop after releasing the wheel.
    pub(crate) fn remove(&self, key: Key) -> Option<Waker> {
        let mut wheel = self.wheel.borrow_mut();
        let waker = wheel.remove(key);
        self.refresh(&wheel);
        waker
    }

    /// Fires the timers due by `now`.
    ///
    /// Before the wheel next has work this does nothing, not even move the
    /// wheel's time on: a timer queued meanwhile goes on a higher level, and
    /// moves down as time comes to it.
    pub(crate) fn fire(&self, now: Instant) {
        if self.next.get().is_none_or(|next| now < next) {
            return;
        }
        let mut fired = self.fired.take();
        {
            let mut wheel = self.wheel.borrow_mut();
            wheel.advance(self.tick_at(now), &mut fired);
            self.refresh(&wheel);
        }
        // Woken after the wheel is released: waking can run code that uses it.
        for waker in fired.drain(..) {
            waker.wake();
        }
        *self.fired.borrow_mut() = fired;
    }
}

/// Waits until a deadline. Returned by [`sleep`] and [`sleep_until`].
///
/// A `Sleep` is `Send` and `Sync`. Its timer is in the wheel of the runtime
/// that polls it, and moves to another runtime's wheel if that one polls it
/// next. Dropping the sleep removes its timer: at once on its runtime's
/// thread, inside the runtime, and otherwise at the runtime's next turn.
#[must_use = "futures do nothing unless polled"]
pub struct Sleep {
    deadline: Instant,
    timer: Option<Armed>,
}

/// The timer of a [`Sleep`]: `key` in the wheel of the runtime whose shared
/// part is `owner`, which removals from where the wheel cannot be reached
/// go through.
struct Armed {
    owner: Arc<Shared>,
    key: Key,
}

/// Waits for `duration`.
pub fn sleep(duration: Duration) -> Sleep {
    sleep_until(deadline_after(duration))
}

/// Waits until `deadline`.
pub fn sleep_until(deadline: Instant) -> Sleep {
    Sleep {
        deadline,
        timer: None,
    }
}

impl Sleep {
    /// The instant the sleep ends.
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Whether the deadline has passed.
    pub fn is_elapsed(&self) -> bool {
        Instant::now() >= self.deadline
    }

    /// Moves the deadline, without making a new `Sleep`.
    pub fn reset(&mut self, deadline: Instant) {
        self.deadline = deadline;
        self.disarm();
    }

    fn disarm(&mut self) {
        let Some(Armed { owner, key }) = self.timer.take() else {
            return;
        };
        match context::current() {
            Some(scheduler) if Arc::ptr_eq(scheduler.shared(), &owner) => {
                let waker = scheduler.timers().remove(key);
                drop(waker);
            }
            _ => owner.remove_timer(key),
        }
    }
}

impl Future for Sleep {
    type Output = ();

    /// # Panics
    ///
    /// If the sleep has not ended and the thread is not inside a runtime.
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if this.is_elapsed() {
            this.disarm();
            return Poll::Ready(());
        }
        let scheduler = context::current().expect("a `Sleep` must be polled inside a ruio runtime");
        let timers = scheduler.timers();
        match &this.timer {
            Some(armed)
                if Arc::ptr_eq(&armed.owner, scheduler.shared())
                    && !timers.wheel.borrow().fired(armed.key) =>
            {
                let replaced = timers.wheel.borrow_mut().set_waker(armed.key, cx.waker());
                drop(replaced);
            }
            _ => {
                // Not armed yet, armed by another runtime, or fired at the
                // edge of the wheel's reach, before the deadline: arm here.
                this.disarm();
                let key = timers.insert(this.deadline, cx.waker().clone());
                this.timer = Some(Armed {
                    owner: Arc::clone(scheduler.shared()),
                    key,
                });
            }
        }
        Poll::Pending
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        self.disarm();
    }
}

impl fmt::Debug for Sleep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sleep")
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

/// Runs `future` until it completes or `duration` passes, whichever comes
/// first. The time starts now, not when the returned future is first polled.
///
/// If the time runs out, the future is dropped, which cancels what it was
/// doing, and the result is [`Elapsed`].
pub fn timeout<F: IntoFuture>(
    duration: Duration,
    future: F,
) -> impl Future<Output = Result<F::Output, Elapsed>> {
    timeout_at(deadline_after(duration), future)
}

/// Runs `future` until it completes or `deadline` passes, whichever comes
/// first; see [`timeout`].
pub fn timeout_at<F: IntoFuture>(
    deadline: Instant,
    future: F,
) -> impl Future<Output = Result<F::Output, Elapsed>> {
    let future = future.into_future();
    async move {
        let mut future = pin!(future);
        let mut sleep = sleep_until(deadline);
        // The future goes first, so one that is ready wins over a deadline
        // that has just passed.
        poll_fn(|cx| {
            if let Poll::Ready(output) = future.as_mut().poll(cx) {
                return Poll::Ready(Ok(output));
            }
            Pin::new(&mut sleep).poll(cx).map(|()| Err(Elapsed(())))
        })
        .await
    }
}

/// The error of a [`timeout`] whose time ran out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Elapsed(());

impl fmt::Display for Elapsed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("deadline has elapsed")
    }
}

impl Error for Elapsed {}

impl From<Elapsed> for io::Error {
    fn from(elapsed: Elapsed) -> Self {
        io::Error::new(io::ErrorKind::TimedOut, elapsed)
    }
}

/// The instant `duration` from now, or a century from now if that is later
/// than an `Instant` can hold.
fn deadline_after(duration: Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(duration)
        .or_else(|| now.checked_add(Duration::from_secs(100 * 365 * 24 * 60 * 60)))
        .unwrap_or(now)
}
