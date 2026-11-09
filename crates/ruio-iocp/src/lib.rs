//! The I/O completion port engine of ruio, for Windows: a proof of concept.
//!
//! `Iocp` keeps the contract of `ruio-engine` on an I/O completion port,
//! the queue to which Windows posts the completions of the operations on
//! the handles associated with it, as io_uring's completion queue holds
//! those of the ring's on Linux. A park waits on the port with
//! `GetQueuedCompletionStatusEx`, an unpark posts a packet of its own to
//! it, and the engine's one deadline timer is the park's timeout, which
//! Windows counts in milliseconds and ends at a tick of its timer.
//!
//! Off Windows the crate is empty.
#![cfg(windows)]

mod engine;
mod sys;

pub use engine::{Builder, Entered, Iocp, Unparker};
