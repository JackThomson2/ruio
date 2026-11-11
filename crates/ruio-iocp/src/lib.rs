//! The I/O completion port engine of ruio, for Windows: a proof of concept.
//!
//! `Iocp` keeps the contract of `ruio-engine` on an I/O completion port,
//! the queue to which Windows posts the completions of the operations on
//! the handles associated with it, as io_uring's completion queue holds
//! those of the ring's on Linux. A park waits on the port with
//! `GetQueuedCompletionStatusEx`, an unpark posts a packet of its own to
//! it, and the engine's one deadline timer is a high-resolution waitable
//! timer, whose expiry a wait completion packet posts to the port, so
//! that a park ends within a fraction of a millisecond of its deadline.
//! The park's own timeout stands behind it, which Windows counts in
//! milliseconds and ends at a tick of its timer, some 15 ms apart, and
//! stands alone before Windows 10's version 1803.
//!
//! Files (`fs::File`), named pipes (`pipe::NamedPipe`) and TCP and UDP
//! sockets (`net::TcpListener`, `net::TcpStream`, `net::UdpSocket`) read
//! and write through the port
//! with owned buffers, as ruio's I/O does on io_uring: an operation takes
//! its buffer and hands it back with the result, and the system may use the
//! buffer until the operation's completion comes, so a dropped operation is
//! cancelled, and its buffer freed only once the cancellation's completion
//! has come.
//!
//! `buf::BufPool` is a pool of buffers for readers to take from as data
//! arrives, which is the thread's, as a port has none for the system to
//! pick from. A stream receives into it (`net::RecvStream`), holding no
//! buffer while it waits for data, and a file reads into it
//! (`fs::ReadStream`). A stream also writes several buffers as one, and
//! leaves a write behind it for the engine to finish.
//!
//! A stream's socket and a file on a disk ask the system to queue no
//! completion for an operation that succeeds during the call that starts
//! it. Such an operation is finished by the poll that started it, and
//! costs no turn of the port; and a turn that would not wait asks the port
//! for nothing while no operation is in flight.
//!
//! Off Windows the crate is empty, but for the tests of what needs nothing
//! of Windows': the pool, and the bookkeeping of a stream's writes.

#[cfg(any(windows, test))]
mod behind;
#[cfg(any(windows, test))]
pub mod buf;
#[cfg(windows)]
mod engine;
#[cfg(windows)]
pub mod fs;
#[cfg(any(windows, test))]
mod gather;
#[cfg(windows)]
mod handle;
#[cfg(windows)]
pub mod net;
#[cfg(windows)]
mod op;
#[cfg(windows)]
pub mod pipe;
#[cfg(windows)]
pub mod portable;
#[cfg(windows)]
mod sys;

#[cfg(windows)]
pub use engine::{Builder, Entered, Iocp, Unparker};
#[cfg(windows)]
pub use op::{Read, ReadExact, Write, WriteAll};
