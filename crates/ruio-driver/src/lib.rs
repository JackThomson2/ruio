//! The io_uring driver of ruio.
//!
//! A [`Driver`] owns one io_uring instance, the operations in flight on it,
//! and the memory the kernel is using for them. [`Driver::op`] turns an
//! [`Operation`] into an [`Op`] future. Polling the future queues the
//! operation; [`Driver::submit`] and [`Driver::park`] hand everything queued
//! to the kernel in one system call and wake the tasks whose operations
//! completed.
//!
//! Dropping an [`Op`] whose operation is in flight cancels it, and the driver
//! keeps the operation's memory until the kernel is done with it. An
//! [`Unparker`] wakes a parked driver from any thread.
//!
//! An `Op` belongs to its driver's thread. A [`PortableOp`] is `Send`: it
//! queues its operation on the ring of the thread's current driver
//! ([`Driver::enter`]) when first polled, and a future that moves to another
//! thread meanwhile hands the operation back to that ring, which finishes
//! it there. First polled on a thread with no current driver, one of
//! another executor's, it sends its entry to the foreign ring the embedder
//! chose ([`set_foreign_ring`]), and fails with `ENOSYS`, which the standard
//! library names `Unsupported`, if none is set. A [`PortableFd`] is a plain
//! descriptor whose operations are portable ones, and which closes after
//! them on the ring they ran on; a [`ReadyWait`] waits for one to be ready,
//! so that a nonblocking read or write can wait for the next attempt.
//!
//! Operations act on an [`Fd`]: a plain file descriptor, or a direct
//! descriptor, a slot of the ring's registered file table, which saves the
//! kernel looking the file up. Operations that make files can put them
//! there, as a [`DirectFd`].
//!
//! [`Probe`] reports what the running kernel supports and checks it against
//! ruio's baseline, the capabilities of Linux 6.10.
//!
//! With the `futures` feature, the streams of multishot requests implement
//! the `Stream` and `FusedStream` of futures-core, each as one call to its
//! own `poll_next`, so they compose with futures-util's combinators.
//!
//! ```
//! use std::pin::Pin;
//! use std::task::{Context, Poll, Waker};
//!
//! use ruio_driver::Driver;
//! use ruio_driver::ops::Nop;
//!
//! let driver = Driver::new()?;
//! let mut op = driver.op(Nop);
//! let mut cx = Context::from_waker(Waker::noop());
//! let result = loop {
//!     if let Poll::Ready(result) = Pin::new(&mut op).poll(&mut cx) {
//!         break result;
//!     }
//!     driver.park(None)?;
//! };
//! result?;
//! # Ok::<(), std::io::Error>(())
//! ```

mod aligned;
pub mod buf;
mod driver;
mod fd;
mod fixed;
mod foreign;
#[cfg(feature = "futures")]
mod futures_impls;
mod link;
mod mem;
mod op;
pub mod ops;
mod orphan;
mod pool;
mod portable;
mod probe;
mod remote;
mod ring;
mod sockaddr;
mod stream;
mod table;
mod unpark;
mod waker;

pub use driver::{Builder, Driver, Entered, Stats, monotonic_now};
pub use fd::{Descriptor, DirectFd, Fd, PolledFd};
pub use foreign::{clear_foreign_ring, foreign_ring, set_foreign_ring};
pub use link::{Chain, Link};
pub use op::{Completion, Op, Operation};
pub use portable::{PortableFd, PortableOp, ReadyWait};
pub use probe::{BaselineError, Probe};
pub use remote::{RemoteRing, SentFd};
pub use sockaddr::SockAddr;
pub use stream::{MultiOp, Multishot};
pub use unpark::Unparker;
pub use waker::LocalWaker;
