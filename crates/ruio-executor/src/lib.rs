//! The thread-per-core executor of ruio.
//!
//! A [`Runtime`] runs futures on the thread that created it, and gives them an
//! io_uring [`Driver`] for their I/O. [`Runtime::block_on`] runs a future to
//! completion; meanwhile [`spawn`] adds tasks, which run on the same thread
//! and need not be `Send`. A task is a single allocation holding its future
//! and, once it finishes, its output, which its [`JoinHandle`] awaits; a
//! [`JoinSet`] holds many tasks, and gives their outputs as they finish.
//!
//! [`Cores`] runs a runtime on each of several CPUs, each on a thread pinned
//! to its CPU: thread per core. [`spawn_on`] sends a `Send` future from one
//! core to another, and the channels of [`sync`] carry values between them.
//!
//! Wakers are `Send + Sync`: a task woken on another thread goes to its
//! runtime's queue of remote wakes, and the runtime's ring is unparked.
//! [`spawn_blocking`] runs a closure that would block, a name lookup or
//! long work, on a thread of a pool ([`blocking`]), and hands its output
//! back so.
//!
//! Timers ([`time`]), sleeps, timeouts and intervals, live in a timing wheel
//! in user space; the kernel sees a single timeout, until the next deadline.
//!
//! Each poll of a task has a budget: the channels of [`sync`] and
//! [`JoinHandle`]s, which can be ready every time they are polled, spend a
//! step of it each time they are, and once it is spent make the task
//! yield, so that a task that always finds them ready cannot hold up the
//! others or the ring.
//!
//! ```
//! use ruio_driver::ops::Nop;
//! use ruio_executor::Runtime;
//!
//! let runtime = Runtime::new()?;
//! let answer = runtime.block_on(async {
//!     let task = ruio_executor::spawn(async {
//!         ruio_executor::driver().op(Nop).await?;
//!         Ok::<_, std::io::Error>(42)
//!     });
//!     task.await.unwrap()
//! })?;
//! assert_eq!(answer, 42);
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! With the `futures` feature, the channels' receivers and intervals
//! implement the `Stream` and `FusedStream` of futures-core, each as one
//! call to its own `poll_recv` or `poll_tick`, and a watch channel's values
//! stream through [`sync::watch::WatchStream`], so they compose with
//! futures-util's combinators.

pub mod blocking;
mod context;
mod coop;
mod cores;
#[cfg(feature = "futures")]
mod futures_impls;
mod handle;
mod join_set;
mod runtime;
mod scheduler;
pub mod sync;
mod task;
pub mod time;
mod wheel;

use std::future::{self, Future};
use std::sync::Arc;
use std::task::Poll;

pub use blocking::spawn_blocking;
pub use cores::{Core, Cores, core, core_count, handle_of, spawn_on};
pub use handle::Handle;
pub use join_set::JoinSet;
pub use ruio_driver::Driver;
pub use runtime::{Builder, Runtime};
pub use task::{AbortHandle, JoinError, JoinHandle, RemoteJoinHandle};

/// A handle that spawns tasks onto the thread's runtime from any thread;
/// see [`Handle`].
///
/// # Panics
///
/// If the thread is not inside [`Runtime::block_on`].
pub fn handle() -> Handle {
    context::with_current(|scheduler| Handle::new(Arc::clone(scheduler.shared()))).expect(OUTSIDE)
}

const OUTSIDE: &str = "must be called from inside a ruio runtime";

/// Spawns a task on the thread's runtime.
///
/// The task runs on this thread, so its future need not be `Send`.
///
/// # Panics
///
/// If the thread is not inside [`Runtime::block_on`].
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    // The scheduler by the thread's plain pointer to it, which the thread's
    // `block_on` keeps alive, rather than a clone of its `Rc`.
    context::with_current(|scheduler| scheduler.spawn(future)).expect(OUTSIDE)
}

/// The io_uring driver of the thread's runtime.
///
/// # Panics
///
/// If the thread is not inside [`Runtime::block_on`].
#[inline]
pub fn driver() -> Driver {
    try_driver().expect(OUTSIDE)
}

/// The io_uring driver of the thread's runtime, or `None` if the thread is
/// not inside one, as when code that may run anywhere, such as a
/// destructor, has I/O for the ring if there is one.
#[inline]
pub fn try_driver() -> Option<Driver> {
    context::with_current(|scheduler| scheduler.driver().clone())
}

/// Lets the other ready tasks run before the current one continues.
pub async fn yield_now() {
    let mut yielded = false;
    future::poll_fn(|cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}
