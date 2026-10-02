//! The thread-per-core executor of ruio.
//!
//! A [`Runtime`] runs futures on the thread that created it, and gives them an
//! io_uring [`Driver`] for their I/O. [`Runtime::block_on`] runs a future to
//! completion; meanwhile [`spawn`] adds tasks, which run on the same thread
//! and need not be `Send`. A task is a single allocation holding its future
//! and, once it finishes, its output, which its [`JoinHandle`] awaits.
//!
//! [`Cores`] runs a runtime on each of several CPUs, each on a thread pinned
//! to its CPU: thread per core. [`spawn_on`] sends a `Send` future from one
//! core to another, and the channels of [`sync`] carry values between them.
//!
//! Wakers are `Send + Sync`: a task woken on another thread goes to its
//! runtime's queue of remote wakes, and the runtime's ring is unparked.
//!
//! Timers ([`time`]) live in a timing wheel in user space; the kernel sees a
//! single timeout, until the next deadline.
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

mod context;
mod coop;
mod cores;
mod handle;
mod runtime;
mod scheduler;
pub mod sync;
mod task;
pub mod time;
mod wheel;

use std::future::{self, Future};
use std::sync::Arc;
use std::task::Poll;

pub use cores::{Core, Cores, core, core_count, handle_of, spawn_on};
pub use handle::{Handle, RemoteJoinHandle};
pub use ruio_driver::Driver;
pub use runtime::{Builder, Runtime};
pub use task::{JoinError, JoinHandle};

/// A handle that spawns tasks onto the thread's runtime from any thread;
/// see [`Handle`].
///
/// # Panics
///
/// If the thread is not inside [`Runtime::block_on`].
pub fn handle() -> Handle {
    let scheduler = context::current().expect(OUTSIDE);
    Handle::new(Arc::clone(scheduler.shared()))
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
    context::current().expect(OUTSIDE).spawn(future)
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
    context::current().map(|scheduler| scheduler.driver().clone())
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
