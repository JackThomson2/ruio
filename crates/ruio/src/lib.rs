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
//! [`net`] has sockets, which read and write with owned buffers and can
//! receive into a pool of buffers that the kernel picks from.
//! Their operations, like all others, run on the thread's ring, which
//! [`driver`] returns; [`uring`] has the operations and the types for
//! writing new ones.
//!
//! Thread per core: [`Cores`] runs a runtime on each of several CPUs, on a
//! thread pinned to it, as `#[ruio::main(cores = "all")]` runs `main`.
//! [`spawn_on`] spawns a `Send` future from one core onto another, and the
//! channels of [`sync`] carry values between cores. A server has a listener
//! on each core, all bound to one address, and the kernel spreads the
//! connections among them:
//!
//! ```no_run
//! use ruio::net::TcpListener;
//!
//! #[ruio::main(cores = "all")]
//! async fn main() -> std::io::Result<()> {
//!     let listener = TcpListener::bind_reuseport("127.0.0.1:7878")?;
//!     let mut incoming = listener.incoming();
//!     while let Some(stream) = incoming.next().await {
//!         let stream = stream?;
//!         ruio::spawn(async move {
//!             let (read, buf) = stream.read(Vec::with_capacity(4096)).await;
//!             if read.is_ok() {
//!                 let _ = stream.write_all(buf).await;
//!             }
//!         });
//!     }
//!     Ok(())
//! }
//! ```

pub use ruio_executor::{
    Builder, Core, Cores, Handle, JoinError, JoinHandle, RemoteJoinHandle, Runtime, core,
    core_count, driver, handle, spawn, spawn_on, yield_now,
};
pub use ruio_executor::{sync, time};
pub use ruio_macros::{main, test};

pub use ruio_driver::buf;

pub mod fs;
pub mod net;

/// The io_uring driver: operations, the futures that run them, and the types
/// for writing new ones, down to the kernel's own structures in
/// [`sys`](uring::sys).
pub mod uring {
    pub use ruio_driver::{
        BaselineError, Completion, Descriptor, DirectFd, Driver, Fd, MultiOp, Multishot, Op,
        Operation, Probe, RemoteRing, SentFd, SockAddr, Stats, Unparker, ops,
    };
    pub use ruio_sys as sys;
}

/// What the expansions of `#[ruio::main]` and `#[ruio::test]` call.
#[doc(hidden)]
pub mod __private {
    use std::future::Future;

    use crate::{Core, Cores};

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

    /// Runs `main` on the first `count` CPUs the process may run on, or on
    /// every one, a runtime on each, and merges the outputs; see
    /// [`Cores::run`].
    ///
    /// # Panics
    ///
    /// If there are fewer CPUs, a runtime cannot start, or a `main` panics.
    pub fn block_on_cores<F, Fut>(count: Option<usize>, main: F) -> Fut::Output
    where
        F: Fn(Core) -> Fut + Sync,
        Fut: Future,
        Fut::Output: Outputs + Send,
    {
        let cores = match count {
            Some(count) => Cores::first(count),
            None => Cores::all(),
        };
        let outputs = cores
            .and_then(|cores| cores.run(main))
            .unwrap_or_else(|err| panic!("cannot start the ruio runtimes: {err}"));
        Outputs::merge(outputs)
    }

    /// What a function run on several cores can return: what the cores'
    /// outputs make together.
    #[diagnostic::on_unimplemented(
        message = "a function run on several cores returns `()` or `Result<(), E>`, not `{Self}`"
    )]
    pub trait Outputs: Sized {
        /// The cores' outputs, in core order, as one.
        fn merge(outputs: Vec<Self>) -> Self;
    }

    impl Outputs for () {
        fn merge(_: Vec<Self>) {}
    }

    /// The first error, in core order.
    impl<E> Outputs for Result<(), E> {
        fn merge(outputs: Vec<Self>) -> Self {
            outputs.into_iter().collect()
        }
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
/// #[ruio::main(cores = 0)]
/// async fn main() {}
/// ```
///
/// ```compile_fail
/// #[ruio::main(cores = "most")]
/// async fn main() {}
/// ```
///
/// A function run on several cores returns what their outputs can make
/// together:
///
/// ```compile_fail
/// #[ruio::main(cores = 1)]
/// async fn answer() -> u32 {
///     42
/// }
/// ```
///
/// ```compile_fail
/// #[ruio::test]
/// struct NotAFunction;
/// ```
#[cfg(doctest)]
struct AttributeErrors;
