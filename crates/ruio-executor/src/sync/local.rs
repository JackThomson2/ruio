//! Channels whose halves stay on one thread, and so take no atomic
//! operation.
//!
//! The channels of [`sync`](super) carry values between cores, and pay for
//! it on every value: a send takes a locked instruction to claim its slot
//! and more to hand the receiver its wake, and a receive takes one to
//! register for it. In a thread-per-core program most values go between
//! tasks of one thread, where none of that is needed: a value moves
//! through a plain cell, and a task on its own thread is woken by a plain
//! flag and a push on its runtime's queue. So these channels are built on
//! `Rc` and `Cell`, and neither half is `Send`; nor need the values be,
//! so a channel may carry an `Rc`. A task that waits on one has its waker
//! kept across its waits, and is woken by reference, so that the wait and
//! the wake clone and drop no waker, which would count on the task
//! atomically.
//!
//! Use them wherever both halves stay on the thread that made them: a
//! task talking to the tasks it spawned, or the connections of one core
//! feeding its one writer. Use [`sync::mpsc`](super::mpsc) and
//! [`sync::oneshot`](super::oneshot) when a half may go to another core,
//! as one does with [`spawn_on`](crate::spawn_on), or to a thread of its
//! own. [`mpsc`] and [`oneshot`] mirror those module for module: the same
//! functions, futures and errors, with the same bound, order and budget.

use std::cell::{Cell, RefCell};
use std::task::Waker;

pub mod mpsc;
pub mod oneshot;

/// The waker of the one task that waits on a channel, or on a set of
/// tasks, kept across its waits and woken by reference.
///
/// The waker is kept once registered, so that a task that waits again
/// clones nothing, and is woken by reference, so that waking it gives
/// nothing up. A waker's clone, drop and wake can each run any code, so
/// none is called while the cell is borrowed: the waker is taken out for
/// its wake, and put back after, unless the wake registered another.
pub(crate) struct WakerCell {
    /// Whether the task waits to be woken.
    waiting: Cell<bool>,
    waker: RefCell<Option<Waker>>,
}

impl WakerCell {
    pub(crate) const fn new() -> Self {
        Self {
            waiting: Cell::new(false),
            waker: RefCell::new(None),
        }
    }

    /// Asks for `waker` to be woken by the next [`wake`](Self::wake). The
    /// caller then looks again for what it waits for, as cloning a waker
    /// can run any code.
    pub(crate) fn register(&self, waker: &Waker) {
        self.waiting.set(true);
        if self
            .waker
            .borrow()
            .as_ref()
            .is_some_and(|old| old.will_wake(waker))
        {
            return;
        }
        let waker = waker.clone();
        let old = self.waker.borrow_mut().replace(waker);
        // Dropped outside the borrow: dropping a waker can run any code.
        drop(old);
    }

    /// Wakes the registered waker, if its task waits.
    pub(crate) fn wake(&self) {
        if !self.waiting.replace(false) {
            return;
        }
        let Some(waker) = self.waker.borrow_mut().take() else {
            return;
        };
        waker.wake_by_ref();
        let mut slot = self.waker.borrow_mut();
        if slot.is_none() {
            *slot = Some(waker);
            return;
        }
        // The wake registered another: this one goes, outside the borrow.
        drop(slot);
        drop(waker);
    }
}
