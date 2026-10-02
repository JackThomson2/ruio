//! Wakers the driver holds without cloning: those of the executor whose
//! thread the driver runs on.

use std::task::RawWakerVTable;

/// How the driver holds the wakers of one executor's tasks, which run on
/// the driver's thread: by a reference the executor counts itself, without
/// atomic operations, where any other waker is cloned.
///
/// An executor gives its driver one with
/// [`Driver::set_local_waker`](crate::Driver::set_local_waker). The driver
/// recognises the executor's wakers by their `vtable`, and asks `retain`
/// whether it may hold each by reference: the executor counts a reference
/// to a task it may count on this thread, and says so, and the driver then
/// holds the waker's `data` until a `release`, waking it with `wake`
/// meanwhile, as `Waker::wake_by_ref` would. A waker of the same vtable
/// that names a task of another thread, which a waker sent there can, the
/// executor refuses, and the driver clones it. The executor so counts the
/// driver's references to a task along with its own, instead of the
/// waker's clone and drop each taking an atomic operation.
#[derive(Clone, Copy, Debug)]
pub struct LocalWaker {
    /// The vtable of the wakers to hold this way, compared by address.
    pub vtable: &'static RawWakerVTable,
    /// Counts a reference to the task a waker's `data` names, if the
    /// executor may count it on this thread, and returns whether it did.
    pub retain: unsafe fn(*const ()) -> bool,
    /// Lets go of a reference `retain` counted.
    pub release: unsafe fn(*const ()),
    /// Wakes the task a waker's `data` names, which `retain` holds.
    pub wake: unsafe fn(*const ()),
}
