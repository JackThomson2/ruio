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
//! [`Probe`] reports what the running kernel supports and checks it against
//! ruio's baseline, the capabilities of Linux 6.1.
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

pub mod buf;
mod driver;
mod fixed;
mod mem;
mod op;
pub mod ops;
mod pool;
mod probe;
mod ring;
mod table;
mod unpark;

pub use driver::{Builder, Driver, Stats};
pub use op::{Completion, Op, Operation};
pub use probe::{BaselineError, Probe};
pub use unpark::Unparker;
