//! I/O that any thread may hold: waits, operations and pipes whose futures
//! are `Send` and `Sync`, for handles that belong to no one engine's port.
//!
//! The rest of the crate's I/O belongs to the engine whose port its handle
//! is associated with, as a handle can be associated with one port alone,
//! and to that engine's thread. What a future that moves between threads
//! waits for runs instead on the port of the engine current on the thread
//! that polls it as it starts, which then wakes its task wherever it runs:
//!
//! - [`HandleWait`] waits for a handle to be signaled, as a process is once
//!   it has exited, or an event once an operation that names it has
//!   completed, with a wait completion packet that the system posts to the
//!   port.
//! - [`ReadyWait`] waits for a socket to be ready, with a poll of the AFD
//!   driver's that completes to the port, through a handle of the driver's
//!   that each engine opens: so a socket that no port has, read and written
//!   with calls that do not wait, waits for its readiness wherever it is
//!   polled.
//! - [`read_at`] and [`write_at`] read and write a handle that no port has,
//!   a file opened for overlapped I/O, with owned buffers, through
//!   operations that name events of their own, which a [`HandleWait`] waits
//!   for.
//! - [`Pipe`] reads and writes a pipe's end that no port has, with calls
//!   that do not wait, as a nonblocking descriptor is read on Unix, keeping
//!   a read and a write of its own in flight, whose completions make it
//!   ready.
//!
//! Polled on a thread with no engine current, a wait fails with
//! [`io::ErrorKind::Unsupported`](std::io::ErrorKind::Unsupported), as one
//! of io_uring's portable operations does with no foreign ring set.

mod overlapped;
mod pipe;
pub(crate) mod ready;
pub(crate) mod wait;

pub use overlapped::{ReadAt, WriteAt, read_at, write_at};
pub use pipe::Pipe;
pub use ready::{ReadyWait, base_socket, events};
pub use wait::HandleWait;
