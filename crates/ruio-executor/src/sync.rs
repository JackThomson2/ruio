//! Channels, which carry values between tasks, and the primitives that
//! coordinate them, on one core or across cores.
//!
//! Both halves of a channel are `Send` when their values are, and wake a
//! task wherever it runs: a task on another core is woken through its
//! runtime's queue of remote wakes. [`oneshot`] carries one value;
//! [`mpsc`] carries many, from any number of senders, with a bound.
//! [`local`] has the same two channels for halves that stay on one
//! thread, which take no atomic operation.
//!
//! [`Mutex`] and [`RwLock`] lock a value for tasks that may hold it
//! across awaits, [`Semaphore`] counts out permits, and [`Notify`] wakes
//! tasks waiting for something to happen. Each serves its waiters in the
//! order they came, and works across cores as the channels do.
//!
//! These can be ready again and again, as the ring's operations never
//! are, so each that completes at once spends a step of the task's budget
//! for the poll. Once the budget is spent, the next one yields: it wakes
//! the task and returns `Pending`, and the task carries on after the
//! others have run.

use std::sync::{Mutex as StdMutex, MutexGuard as StdMutexGuard, PoisonError};

pub mod broadcast;
pub mod local;
pub mod mpsc;
mod mutex;
mod notify;
pub mod oneshot;
mod rwlock;
mod semaphore;
mod waker_slot;
pub mod watch;

pub use mutex::{Mutex, MutexGuard};
pub use notify::{Notified, Notify};
pub use rwlock::{RwLock, RwLockReadGuard, RwLockWriteGuard};
pub use semaphore::{Acquire, AcquireOwned, OwnedSemaphorePermit, Semaphore, SemaphorePermit};

/// Locks `mutex`, whose data no panic can leave half-changed: every critical
/// section here only moves values in and out.
#[inline]
fn lock<T>(mutex: &StdMutex<T>) -> StdMutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
