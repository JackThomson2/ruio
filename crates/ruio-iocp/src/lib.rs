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
//! pick from.
//!
//! Off Windows the crate is empty, but for the tests of the pool, which
//! needs nothing of Windows'.

#[cfg(any(windows, test))]
pub mod buf;
#[cfg(windows)]
mod engine;
#[cfg(windows)]
pub mod fs;
#[cfg(windows)]
mod handle;
#[cfg(windows)]
pub mod net;
#[cfg(windows)]
mod op;
#[cfg(windows)]
pub mod pipe;
#[cfg(windows)]
mod sys;

#[cfg(windows)]
pub use engine::{Builder, Entered, Iocp, Unparker};
#[cfg(windows)]
pub use op::{Read, ReadExact, Write, WriteAll};
