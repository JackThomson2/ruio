//! The runtime the current thread is running, if any.

use std::cell::{Cell, RefCell};
use std::ptr;
use std::rc::Rc;
use std::sync::Arc;

use crate::scheduler::{Scheduler, Shared};

thread_local! {
    /// The thread's current scheduler, which the thread's `block_on` keeps
    /// alive.
    static CURRENT: RefCell<Option<Rc<Scheduler>>> = const { RefCell::new(None) };

    /// The same scheduler, as the hot paths reach it: a plain pointer, null
    /// outside a runtime, read without the cell's bookkeeping. Set after
    /// `CURRENT` and cleared before it, so that it never names a scheduler
    /// `CURRENT` does not keep alive.
    static ACTIVE: Cell<*const Scheduler> = const { Cell::new(ptr::null()) };
}

/// Makes `scheduler` the thread's current one until the guard is dropped.
///
/// # Panics
///
/// If the thread is already running a runtime.
pub(crate) fn enter(scheduler: Rc<Scheduler>) -> Enter {
    let active = Rc::as_ptr(&scheduler);
    CURRENT.with(|current| {
        let mut current = current.borrow_mut();
        assert!(
            current.is_none(),
            "cannot run a ruio runtime from inside another one on the same thread"
        );
        *current = Some(scheduler);
    });
    ACTIVE.with(|slot| slot.set(active));
    Enter(())
}

/// Like [`enter`], but does nothing if the thread is running a runtime.
pub(crate) fn enter_if_idle(scheduler: Rc<Scheduler>) -> Option<Enter> {
    let idle = CURRENT
        .try_with(|current| current.borrow().is_none())
        .unwrap_or(false);
    idle.then(|| enter(scheduler))
}

/// Restores the thread to running no runtime when dropped.
pub(crate) struct Enter(());

impl Drop for Enter {
    fn drop(&mut self) {
        let _ = ACTIVE.try_with(|slot| slot.set(ptr::null()));
        // Dropped outside the borrow, since dropping a scheduler runs code
        // that may look at the current runtime.
        let previous = CURRENT.try_with(|current| current.borrow_mut().take());
        drop(previous);
    }
}

/// The thread's current scheduler.
pub(crate) fn current() -> Option<Rc<Scheduler>> {
    CURRENT
        .try_with(|current| current.borrow().clone())
        .ok()
        .flatten()
}

/// Runs `f` with the thread's current scheduler, if the thread is inside a
/// runtime.
pub(crate) fn with_current<R>(f: impl FnOnce(&Scheduler) -> R) -> Option<R> {
    let scheduler = ACTIVE.try_with(Cell::get).unwrap_or(ptr::null());
    if scheduler.is_null() {
        return None;
    }
    // SAFETY: `ACTIVE` names a scheduler only while `CURRENT` keeps it
    // alive, and the thread's `block_on` holds `CURRENT` for as long as it
    // runs, so the scheduler outlives this call.
    Some(f(unsafe { &*scheduler }))
}

/// Runs `f` with the thread's current scheduler, if that is the one whose
/// shared part is at `owner`: the owner of what the address was taken from.
/// Compared by address, so that the caller need not keep a reference to the
/// shared part alive across `f`.
pub(crate) fn with_owner<R>(owner: *const Shared, f: impl FnOnce(&Scheduler) -> R) -> Option<R> {
    with_current(|scheduler| ptr::eq(Arc::as_ptr(scheduler.shared()), owner).then(|| f(scheduler)))
        .flatten()
}
