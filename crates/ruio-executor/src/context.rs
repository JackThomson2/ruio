//! The runtime the current thread is running, if any.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use crate::scheduler::{Scheduler, Shared, TaskRef};

thread_local! {
    static CURRENT: RefCell<Option<Rc<Scheduler>>> = const { RefCell::new(None) };
}

/// Makes `scheduler` the thread's current one until the guard is dropped.
///
/// # Panics
///
/// If the thread is already running a runtime.
pub(crate) fn enter(scheduler: Rc<Scheduler>) -> Enter {
    CURRENT.with(|current| {
        let mut current = current.borrow_mut();
        assert!(
            current.is_none(),
            "cannot run a ruio runtime from inside another one on the same thread"
        );
        *current = Some(scheduler);
    });
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

/// Whether the thread is running the runtime `shared` belongs to.
pub(crate) fn is_current(shared: &Arc<Shared>) -> bool {
    CURRENT
        .try_with(|current| {
            current
                .borrow()
                .as_ref()
                .is_some_and(|scheduler| Arc::ptr_eq(scheduler.shared(), shared))
        })
        .unwrap_or(false)
}

/// Queues `task` on the thread's scheduler, unless it is queued there
/// already, if that is the scheduler whose shared part is at `owner`: its
/// own. Otherwise hands the task back. Compared by address, so that the
/// caller needs no reference to the shared part.
pub(crate) fn schedule_local(owner: *const Shared, task: TaskRef) -> Result<(), TaskRef> {
    let mut task = Some(task);
    let _ = CURRENT.try_with(|current| {
        if let Some(scheduler) = current.borrow().as_ref()
            && std::ptr::eq(Arc::as_ptr(scheduler.shared()), owner)
            && let Some(task) = task.take()
            && task.claim()
        {
            scheduler.schedule(task);
        }
    });
    task.map_or(Ok(()), Err)
}
