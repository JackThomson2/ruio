//! The cooperative budget: how much a task may do in one poll before the
//! primitives it waits on make it yield.
//!
//! An io_uring operation always yields at least once, as its completion
//! comes through the ring. A channel, a lock or a semaphore can be ready
//! again and again, and a task looping on one would run on for ever,
//! holding up the other tasks and the ring. So each poll of a task, and of
//! the future given to `block_on`, may take 128 steps: a primitive that is
//! ready takes one, and once none are left it returns `Pending` instead,
//! having woken the task, which so goes to the back of the run queue.
//! Outside a runtime there is no budget, and primitives always proceed.
//!
//! A primitive of another crate's that can be ready again and again takes
//! part with [`poll_proceed`]: I/O that completes without the ring, such as
//! a read of a nonblocking socket that finds data waiting, or a future of
//! its own that wraps a queue.
//!
//! ```
//! use std::collections::VecDeque;
//! use std::future::poll_fn;
//! use std::task::Poll;
//!
//! use ruio_executor::coop;
//!
//! /// Takes the next value, or `None` once there are none, spending a step of
//! /// the task's budget for each value it takes.
//! async fn next(queue: &mut VecDeque<u32>) -> Option<u32> {
//!     poll_fn(|cx| {
//!         let proceed = std::task::ready!(coop::poll_proceed(cx));
//!         let value = queue.pop_front();
//!         if value.is_some() {
//!             proceed.made_progress();
//!         }
//!         Poll::Ready(value)
//!     })
//!     .await
//! }
//! # let mut queue = VecDeque::from([1, 2]);
//! # ruio_executor::Runtime::new().unwrap().block_on(async {
//! #     assert_eq!(next(&mut queue).await, Some(1));
//! # });
//! ```

use std::cell::Cell;
use std::task::{Context, Poll};

/// How many steps one poll may take.
const BUDGET: u32 = 128;

thread_local! {
    /// The steps left to the poll the thread is running, if it has a budget.
    static LEFT: Cell<Option<u32>> = const { Cell::new(None) };
}

/// Runs `poll`, a poll of a task or of the future given to `block_on`,
/// with a full budget, and gives the thread back the budget it had before.
#[inline]
pub(crate) fn budgeted<R>(poll: impl FnOnce() -> R) -> R {
    /// Restores the budget before, even if the poll panics.
    struct Restore(Option<u32>);

    impl Drop for Restore {
        #[inline]
        fn drop(&mut self) {
            let _ = LEFT.try_with(|left| left.set(self.0));
        }
    }

    let before = LEFT.try_with(|left| left.replace(Some(BUDGET)));
    let _restore = Restore(before.ok().flatten());
    poll()
}

/// Ready if the poll may take a step, which the primitive then spends with
/// [`Proceed::made_progress`] if it does make progress; otherwise wakes the
/// task, so that it is polled again after the others, and is `Pending`.
///
/// A primitive asks before it does anything: one that would be ready,
/// asked once the budget is spent, waits with the rest of the poll for the
/// task's next turn. One that is not ready drops the step unspent, and so
/// costs the budget nothing. Outside a runtime it is always ready.
#[inline]
pub fn poll_proceed(cx: &mut Context<'_>) -> Poll<Proceed> {
    if LEFT.try_with(Cell::get).ok().flatten() == Some(0) {
        cx.waker().wake_by_ref();
        return Poll::Pending;
    }
    Poll::Ready(Proceed(()))
}

/// A step that the poll may take; see [`poll_proceed`].
#[derive(Debug)]
#[must_use = "a step is spent with `made_progress`, or dropped unspent"]
pub struct Proceed(());

impl Proceed {
    /// Spends the step, for a primitive that was ready.
    #[inline]
    pub fn made_progress(self) {
        let _ = LEFT.try_with(|left| {
            if let Some(steps) = left.get() {
                left.set(Some(steps.saturating_sub(1)));
            }
        });
    }
}
