//! The thread-per-core executor of ruio.
//!
//! A [`Runtime`] runs futures on the thread that created it, and gives them
//! the thread's I/O engine for their I/O. [`Runtime::block_on`] runs a
//! future to completion; meanwhile [`spawn`] adds tasks, which run on the
//! same thread and need not be `Send`. A task is a single allocation
//! holding its future and, once it finishes, its output, which its
//! [`JoinHandle`] awaits; a [`JoinSet`] holds many tasks, and gives their
//! outputs as they finish.
//!
//! The engine is the thread's source of events, which the runtime turns
//! between its tasks and parks on while none is ready: on Linux, the
//! io_uring ring of `ruio-driver`; on Windows, the I/O completion port of
//! `ruio-iocp`; elsewhere, until ruio has an engine there, the park engine
//! of `ruio-engine`, which runs no I/O, and which building with
//! `--cfg ruio_engine="park"` chooses on Linux and Windows too. The
//! runtime drives its engine through the contract of `ruio-engine` alone,
//! so the engine is chosen when the crate is built, and no engine pays at
//! run time for the others being possible.
//!
//! [`Cores`] runs a runtime on each of several CPUs, each on a thread pinned
//! to its CPU: thread per core. [`spawn_on`] sends a `Send` future from one
//! core to another, and the channels of [`sync`] carry values between them.
//!
//! Wakers are `Send + Sync`: a task woken on another thread goes to its
//! runtime's queue of remote wakes, and the runtime's engine is unparked.
//! [`spawn_blocking`] runs a closure that would block, a name lookup or
//! long work, on a thread of a pool ([`blocking`]), and hands its output
//! back so.
//!
//! Timers ([`time`]), sleeps, timeouts and intervals, live in a timing wheel
//! in user space; the engine sees a single timer, for the next deadline.
//!
//! Each poll of a task has a budget: the channels of [`sync`] and
//! [`JoinHandle`]s, which can be ready every time they are polled, spend a
//! step of it each time they are, and once it is spent make the task
//! yield, so that a task that always finds them ready cannot hold up the
//! others or the engine. [`coop`] lets the primitives of other crates take
//! part.
//!
//! ```
//! use std::time::Duration;
//!
//! use ruio_executor::Runtime;
//!
//! let runtime = Runtime::new()?;
//! let answer = runtime.block_on(async {
//!     let task = ruio_executor::spawn(async {
//!         ruio_executor::time::sleep(Duration::from_millis(1)).await;
//!         42
//!     });
//!     task.await.unwrap()
//! });
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
pub mod coop;
mod cores;
mod engine;
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
// The engine's own API, which only io_uring's has so far.
#[cfg(ruio_uring)]
pub use engine::public::*;
pub use handle::Handle;
pub use join_set::JoinSet;
pub use runtime::{Builder, Runtime};
pub use task::{AbortHandle, JoinError, JoinHandle, RemoteAbortHandle, RemoteJoinHandle};

/// A handle that spawns tasks onto the thread's runtime from any thread;
/// see [`Handle`].
///
/// # Panics
///
/// If the thread is not inside [`Runtime::block_on`].
pub fn handle() -> Handle {
    try_handle().expect(OUTSIDE)
}

/// A handle that spawns tasks onto the thread's runtime from any thread, or
/// `None` if the thread is not inside [`Runtime::block_on`], as when code
/// that may run anywhere, such as a destructor, has a task for the runtime
/// if there is one.
///
/// ```
/// use ruio_executor::Runtime;
///
/// assert!(ruio_executor::try_handle().is_none());
/// let runtime = Runtime::new()?;
/// runtime.block_on(async { assert!(ruio_executor::try_handle().is_some()) });
/// # Ok::<(), std::io::Error>(())
/// ```
pub fn try_handle() -> Option<Handle> {
    context::with_current(|scheduler| Handle::new(Arc::clone(scheduler.shared())))
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
