//! Timers: waiting for a deadline, and bounding how long a future may take.
//!
//! Timers live in each runtime's timing wheel, in user space, with a
//! resolution of a millisecond. The kernel sees a single timeout: how long the
//! runtime may park before the wheel's next deadline.

use std::cell::RefCell;
use std::error::Error;
use std::fmt;
use std::future::{Future, IntoFuture, poll_fn};
use std::io;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

pub use std::time::Instant;

use crate::context;
use crate::wheel::{Key, Wheel};

/// A runtime's timers: its wheel, whose ticks are milliseconds since `start`.
pub(crate) struct Timers {
    start: Instant,
    wheel: RefCell<Wheel>,
    /// Wakers of the timers that fired together, kept to reuse the
    /// allocation.
    fired: RefCell<Vec<Waker>>,
}

impl Timers {
    pub(crate) fn new() -> Self {
        Self {
            start: Instant::now(),
            wheel: RefCell::new(Wheel::new()),
            fired: RefCell::new(Vec::new()),
        }
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
        wheel.insert(tick, waker)
    }

    /// Fires the timers due by `now`.
    pub(crate) fn fire(&self, now: Instant) {
        let mut fired = self.fired.take();
        self.wheel
            .borrow_mut()
            .advance(self.tick_at(now), &mut fired);
        // Woken after the wheel is released: waking can run code that uses it.
        for waker in fired.drain(..) {
            waker.wake();
        }
        *self.fired.borrow_mut() = fired;
    }

    /// How long from `now` until the wheel next has work, if it has any.
    pub(crate) fn next_timeout(&self, now: Instant) -> Option<Duration> {
        let tick = self.wheel.borrow().next_expiration()?;
        let at = self.start.checked_add(Duration::from_millis(tick));
        Some(at.map_or(Duration::MAX, |at| at.saturating_duration_since(now)))
    }
}

/// Waits until a deadline. Returned by [`sleep`] and [`sleep_until`].
///
/// A `Sleep` belongs to the runtime that first polls it. Dropping it removes
/// its timer.
#[must_use = "futures do nothing unless polled"]
pub struct Sleep {
    deadline: Instant,
    timer: Option<(Rc<Timers>, Key)>,
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
        if let Some((timers, key)) = self.timer.take() {
            let waker = timers.wheel.borrow_mut().remove(key);
            drop(waker);
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
        match &this.timer {
            Some((timers, key)) if !timers.wheel.borrow().fired(*key) => {
                let replaced = timers.wheel.borrow_mut().set_waker(*key, cx.waker());
                drop(replaced);
            }
            _ => {
                // Not armed yet, or fired at the edge of the wheel's reach,
                // before the deadline: arm, or arm again.
                this.disarm();
                let timers = Rc::clone(
                    context::current()
                        .expect("a `Sleep` must be polled inside a ruio runtime")
                        .timers(),
                );
                let key = timers.insert(this.deadline, cx.waker().clone());
                this.timer = Some((timers, key));
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
