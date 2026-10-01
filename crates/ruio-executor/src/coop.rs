//! The cooperative budget: how much a task may do in one poll before the
//! primitives it waits on make it yield.
//!
//! An io_uring operation always yields at least once, as its completion
//! comes through the ring. A channel, a lock or a semaphore can be ready
//! again and again, and a task looping on one would run on for ever,
//! holding up the other tasks and the ring. So each poll of a task, and of
//! the future given to `block_on`, may take [`BUDGET`] steps: a primitive
//! that is ready takes one, and once none are left it returns `Pending`
//! instead, having woken the task, which so goes to the back of the run
//! queue. Outside a runtime there is no budget, and primitives always
//! proceed.

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
pub(crate) fn budgeted<R>(poll: impl FnOnce() -> R) -> R {
    /// Restores the budget before, even if the poll panics.
    struct Restore(Option<u32>);

    impl Drop for Restore {
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
pub(crate) fn poll_proceed(cx: &mut Context<'_>) -> Poll<Proceed> {
    if LEFT.try_with(Cell::get).ok().flatten() == Some(0) {
        cx.waker().wake_by_ref();
        return Poll::Pending;
    }
    Poll::Ready(Proceed(()))
}

/// A step that the poll may take; see [`poll_proceed`].
pub(crate) struct Proceed(());

impl Proceed {
    /// Spends the step, for a primitive that was ready.
    pub(crate) fn made_progress(self) {
        let _ = LEFT.try_with(|left| {
            if let Some(steps) = left.get() {
                left.set(Some(steps.saturating_sub(1)));
            }
        });
    }
}
