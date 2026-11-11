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
#![cfg_attr(
    ruio_uring,
    doc = r#"[`net`] has sockets, which read and write with owned buffers and can
receive into a pool of buffers that the kernel picks from, and [`fs`] has
files, which read and write the same way; [`io`] has the traits both
implement, `copy` and `splice` over them, and `AsyncFd`, which brings a
descriptor of any other kind onto the ring; [`signal`] delivers signals
to tasks; [`process`] spawns child processes, with pipes to their
standard streams, and waits for them on the ring. Their operations, like
all others, run on the thread's ring, which [`driver`] returns; [`uring`]
has the operations and the types for writing new ones. Libraries that
take their I/O through the poll-based traits of `tokio::io` or
futures-io, such as hyper and rustls, run over ruio's streams through the
adapters of the `ruio-compat` crate.

A file or socket belongs to its thread's ring, and a future that holds
one is not `Send`. The functions of [`fs`] that take a path, and
everything in [`process`], run their operations on the ring of the
thread that polls them instead, so their futures are `Send`, for code
whose futures must be, such as `async_trait`'s, at the cost of a
comparison per poll; so are the directory reader `read_dir` makes and its
entries.
"#
)]
//!
//! Thread per core: [`Cores`] runs a runtime on each of several CPUs, on a
//! thread pinned to it, as `#[ruio::main(cores = "all")]` runs `main`.
//! [`spawn_on`] spawns a `Send` future from one core onto another, and the
//! channels of [`sync`] carry values between cores.
#![cfg_attr(
    ruio_uring,
    doc = r#"A server has a listener on each core, all bound to one address, and
the kernel spreads the connections among them:

```no_run
use ruio::net::TcpListener;

#[ruio::main(cores = "all")]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind_reuseport("127.0.0.1:7878")?;
    let mut incoming = listener.incoming();
    while let Some(stream) = incoming.next().await {
        let stream = stream?;
        ruio::spawn(async move {
            let (read, buf) = stream.read(Vec::with_capacity(4096)).await;
            if read.is_ok() {
                let _ = stream.write_all(buf).await;
            }
        });
    }
    Ok(())
}
```

With the `futures` feature, the streams here, of connections, of data, of
datagrams, of directory entries, of readiness and of signals, and the
channels' receivers and intervals of the executor, implement the `Stream`
and `FusedStream` of futures-core, each as one call to its own
`poll_next`, so they compose with futures-util's combinators: a server's
connections merge with its shutdown signal through `select`."#
)]
#![cfg_attr(
    ruio_iocp,
    doc = r#"
Built for Windows, ruio runs on the I/O completion port engine of
`ruio-iocp`, and has part of its I/O there, a proof of concept. [`net`]
has TCP and UDP sockets, which read and write with owned buffers as they
do on io_uring; a stream also receives into a pool, writes several
buffers as one, and leaves a write behind it. [`fs`] has files, which
read and write the same way, with what Windows has no overlapped form
of, opening a file, syncing it and the functions that take a path, on
the blocking pool. [`io`] has the traits both implement, and `copy` over
them, and [`buf`] has the engine's pool of buffers beside the buffer
traits that every engine shares. All four are under the names and with
the signatures they have on io_uring, so that code written to them
builds for both. Beside them is the executor's API: runtimes, tasks,
timers, the channels and locks of [`sync`], the blocking pool and
[`Cores`], whose threads are pinned to their CPUs. The rest of the I/O
and io_uring's own API, `process`, `signal`, the rest of `io`, the
registered buffers of `buf`, `uring` and `driver`, come with io_uring
alone so far. Libraries that take their I/O through the poll-based
traits of `tokio::io` or futures-io, such as hyper and rustls, run over
the streams and files through the adapter of the `ruio-compat` crate.
With the `futures` feature, a listener's connections, what arrives on a
connection, a file's data, a directory's entries and the channels'
receivers and intervals implement the `Stream` and `FusedStream` of
futures-core."#
)]
#![cfg_attr(
    not(any(ruio_uring, ruio_iocp)),
    doc = r#"
Built for a system whose engine ruio offers no I/O on yet, ruio has the
executor's API alone: on macOS, or wherever it is built with
`--cfg ruio_engine="park"`, on the park engine of `ruio-engine`, which
runs no I/O. That is runtimes, tasks, timers, the channels and locks of
[`sync`], the blocking pool and [`Cores`], whose threads are pinned on
Linux and Windows alone, and the buffer traits of [`buf`], which every
engine shares.
The I/O and io_uring's own API, `net`, `fs`, `io`, `process`, `signal`,
the pools and registered buffers of `buf`, `uring` and `driver`, come with
an engine that offers them: io_uring, and for part of `net`, `fs` and `io`
the I/O completion port of Windows. With the `futures` feature, the
channels' receivers and intervals implement the `Stream` and `FusedStream`
of futures-core."#
)]

pub use ruio_executor::{
    AbortHandle, Builder, Core, Cores, Handle, JoinError, JoinHandle, JoinSet, RemoteAbortHandle,
    RemoteJoinHandle, Runtime, core, core_count, handle, handle_of, spawn, spawn_blocking,
    spawn_on, try_handle, yield_now,
};
pub use ruio_executor::{blocking, coop, sync, time};
pub use ruio_macros::{main, test};

pub mod buf;

// The I/O, which runs on io_uring and, in part, on the I/O completion port,
// and io_uring's own API.
#[cfg(ruio_uring)]
pub use ruio_executor::driver;

#[cfg(ruio_uring)]
pub mod fs;
// The completion port engine's part of `fs`, under io_uring's names.
#[cfg(ruio_iocp)]
#[path = "iocp/fs.rs"]
pub mod fs;
#[cfg(all(feature = "futures", ruio_uring))]
mod futures_impls;
#[cfg(ruio_uring)]
pub mod io;
// The completion port engine's part of `io`, under io_uring's names.
#[cfg(ruio_iocp)]
#[path = "iocp/io.rs"]
pub mod io;
#[cfg(ruio_uring)]
pub mod net;
// The completion port engine's part of `net`, under io_uring's names.
#[cfg(ruio_iocp)]
#[path = "iocp/net.rs"]
pub mod net;
#[cfg(ruio_uring)]
pub mod process;
#[cfg(ruio_uring)]
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
/// caller can do its I/O another way. The waits of the portable sockets and
/// a child's pipes go to the foreign reactor instead where one is set and
/// the thread is one of its executor's
/// ([`set_foreign_reactor`](crate::io::set_foreign_reactor)): tokio's, say,
/// whose reactor then wakes the task as for its own sockets.
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
#[cfg(ruio_uring)]
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
