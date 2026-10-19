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
//! receive into a pool of buffers that the kernel picks from, and [`fs`]
//! has files, which read and write the same way; [`io`] has the traits
//! both implement, `copy` and `splice` over them, and `AsyncFd`, which
//! brings a descriptor of any other kind onto the ring; [`signal`]
//! delivers signals to tasks; [`process`] spawns child processes, with
//! pipes to their standard streams, and waits for them on the ring. Their
//! operations, like all others, run on the thread's ring, which [`driver`]
//! returns; [`uring`] has the operations and the types for writing new
//! ones. Libraries that take their I/O through the poll-based traits of
//! `tokio::io` or futures-io, such as hyper and rustls, run over ruio's
//! streams through the adapters of the `ruio-compat` crate.
//!
//! A file or socket belongs to its thread's ring, and a future that holds
//! one is not `Send`. The functions of [`fs`] that take a path, but
//! `read_dir`, and everything in [`process`], run their operations on the
//! ring of the thread that polls them instead, so their futures are `Send`,
//! for code whose futures must be, such as `async_trait`'s, at the cost of a
//! comparison per poll.
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
//!
//! With the `futures` feature, the streams here, of connections, of data,
//! of datagrams, of directory entries, of readiness and of signals, and the
//! channels' receivers and intervals of the executor, implement the `Stream`
//! and `FusedStream` of futures-core, each as one call to its own
//! `poll_next`, so they compose with futures-util's combinators: a server's
//! connections merge with its shutdown signal through `select`.

pub use ruio_executor::{
    AbortHandle, Builder, Core, Cores, Handle, JoinError, JoinHandle, JoinSet, RemoteAbortHandle,
    RemoteJoinHandle, Runtime, core, core_count, driver, handle, handle_of, spawn, spawn_blocking,
    spawn_on, yield_now,
};
pub use ruio_executor::{blocking, coop, sync, time};
pub use ruio_macros::{main, test};

pub use ruio_driver::buf;

pub mod fs;
#[cfg(feature = "futures")]
mod futures_impls;
pub mod io;
pub mod net;
pub mod process;
pub mod signal;

/// The io_uring driver: operations, the futures that run them, and the types
/// for writing new ones, down to the kernel's own structures in
/// [`sys`](uring::sys). A [`PortableOp`](uring::PortableOp), unlike an
/// [`Op`](uring::Op), is `Send`: it runs on the ring of the thread that
/// first polls it, and a [`PortableFd`](uring::PortableFd) is a plain
/// descriptor whose operations are portable ones, and which a
/// [`ReadyWait`](uring::ReadyWait) waits on to be ready.
///
/// Portable operations first polled where no runtime runs, on a thread of
/// another executor's, such as tokio's, or of `futures::executor::block_on`,
/// go to the foreign ring, which [`set_foreign_ring`](uring::set_foreign_ring)
/// sets: that of one of the program's runtimes, through its
/// [`Handle::ring`](crate::Handle::ring), say. So the portable sockets,
/// a child's pipes and its wait, and the path functions of [`fs`]
/// work from such a thread too. With no foreign ring set they fail with
/// `ENOSYS`, whose kind is [`std::io::ErrorKind::Unsupported`], so that the
/// caller can do its I/O another way.
///
/// ```no_run
/// # fn main() -> std::io::Result<()> {
/// // A runtime on a thread of its own, whose ring other threads' portable
/// // operations run on.
/// let (ring, ready) = std::sync::mpsc::channel();
/// std::thread::spawn(move || {
///     ruio::Runtime::new()?.block_on(async {
///         ring.send(ruio::handle().ring().clone()).unwrap();
///         std::future::pending::<()>().await
///     });
///     std::io::Result::Ok(())
/// });
/// ruio::uring::set_foreign_ring(&ready.recv().unwrap());
/// // A thread with no runtime of its own reads a file through it.
/// let data = futures_executor::block_on(ruio::fs::read("/etc/hostname"))?;
/// # drop(data);
/// # Ok(())
/// # }
/// ```
pub mod uring {
    pub use ruio_driver::{
        BaselineError, Chain, Completion, Descriptor, DirectFd, Driver, Entered, Fd, Link, MultiOp,
        Multishot, Op, Operation, PortableFd, PortableOp, Probe, ReadyWait, RemoteRing, SentFd,
        SockAddr, Stats, Unparker, clear_foreign_ring, foreign_ring, ops, set_foreign_ring,
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
