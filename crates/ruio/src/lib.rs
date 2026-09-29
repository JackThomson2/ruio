//! An async runtime for Linux, built on io_uring.
//!
//! Each thread runs its own [`Runtime`] around its own io_uring ring. Tasks
//! spawned with [`spawn`] stay on their thread, so their futures need not be
//! `Send`, and the I/O they start reaches the kernel in batches: one system
//! call per turn of the runtime's loop, however many operations are queued.
//! Timers ([`time`]) cost the kernel nothing but the timeout of that call.
//!
//! ```
//! use std::time::Duration;
//!
//! let runtime = ruio::Runtime::new()?;
//! let greeting = runtime.block_on(async {
//!     let task = ruio::spawn(async {
//!         ruio::time::sleep(Duration::from_millis(1)).await;
//!         "hello"
//!     });
//!     task.await.unwrap()
//! });
//! assert_eq!(greeting, "hello");
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! Operations on file descriptors run on the thread's ring, which
//! [`driver`] returns; [`uring`] has the operations and the types for
//! writing new ones.

pub use ruio_executor::time;
pub use ruio_executor::{Builder, JoinError, JoinHandle, Runtime, driver, spawn, yield_now};
pub use ruio_macros::{main, test};

pub use ruio_driver::buf;

/// The io_uring driver: operations, the futures that run them, and the types
/// for writing new ones.
pub mod uring {
    pub use ruio_driver::{
        BaselineError, Completion, Driver, Op, Operation, Probe, Stats, Unparker, ops,
    };
}

/// What the expansions of `#[ruio::main]` and `#[ruio::test]` call.
#[doc(hidden)]
pub mod __private {
    use std::future::Future;

    /// Runs `future` on a new runtime.
    ///
    /// # Panics
    ///
    /// If the runtime cannot start, or `future` panics.
    pub fn block_on<F: Future>(future: F) -> F::Output {
        crate::Runtime::new()
            .unwrap_or_else(|err| panic!("cannot start the ruio runtime: {err}"))
            .block_on(future)
    }
}

/// The attributes reject what they cannot run:
///
/// ```compile_fail
/// #[ruio::main]
/// fn main() {}
/// ```
///
/// ```compile_fail
/// #[ruio::main(threads = 4)]
/// async fn main() {}
/// ```
///
/// ```compile_fail
/// #[ruio::test]
/// struct NotAFunction;
/// ```
#[cfg(doctest)]
struct AttributeErrors;
