//! Raw bindings to the Linux io_uring interface.
//!
//! This crate mirrors `include/uapi/linux/io_uring.h`: the structures shared
//! with the kernel, the constants that go in them, and thin wrappers around
//! the io_uring system calls. It holds no policy; the safe interface is built
//! on top of it in `ruio-driver`.
//!
//! Each group of constants comes with a table of `(name, value)` pairs, such
//! as [`SETUP_FLAGS`] or [`OPCODES`], for printing readable names.
//!
//! The system calls are [`io_uring_setup`], [`io_uring_enter`] and
//! [`io_uring_register`]. They report failure as [`std::io::Error`] and
//! otherwise return what the kernel does.

mod abi;
mod syscall;

pub use abi::*;
pub use syscall::*;
