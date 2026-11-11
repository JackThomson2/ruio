//! Reading and writing with owned buffers, generically: the traits ruio's
//! streams and files share, and what is built on them.
//!
//! This is the part of ruio's `io` that the completion port engine has so
//! far, a proof of concept, under the names and with the signatures it has
//! on io_uring, so that code written to it builds for both: the traits,
//! which a [`TcpStream`] and a [`File`] at its position implement, the
//! futures that come with them, and [`copy`]; the reads and writes of the
//! portable sockets; and the foreign reactor they wait on where no runtime
//! runs.
//!
//! [`AsyncRead::read`] and [`AsyncWrite::write`] take a buffer by value and
//! hand it back with the result, as the sockets' and the files' own methods
//! do, and each gives the type's own future, named by an associated type, so
//! that code generic over a reader or a writer compiles to the operations
//! the type's methods make: no boxing, no dynamic dispatch, no allocation
//! beyond the buffer the caller brings. A reference to a reader or writer,
//! an [`Rc`](std::rc::Rc) or a [`Box`] of one, is one too.
//!
//! [`read_exact`](AsyncRead::read_exact), [`read_to_end`](AsyncRead::read_to_end)
//! and [`write_all`](AsyncWrite::write_all) come with the traits, as
//! futures that run the type's own future again until the buffer is done
//! with, each polled where it lies. [`copy`] moves everything a reader has
//! to a writer with two buffers, the next read in flight while a write
//! runs, so that the writer and the reader keep each other busy.
//!
//! A stream's own `read_exact` and `write_all` stay what they are: a method
//! call on a stream reaches them, and the traits' through a generic bound
//! only.
//!
//! The portable sockets of [`net::portable`] read and write with calls of
//! Winsock's made at once instead, on whichever thread polls them, and wait
//! on that thread's port only when there is nothing to read or no room to
//! write: their reads and writes are [`PortableRead`]s and
//! [`PortableWrite`]s, which are `Send` and `Sync`, and spend a step of the
//! task's budget ([`coop`](crate::coop)) each time they move data.
//!
//! Polled on a thread of another executor's, where no runtime of ruio's
//! runs and so no port, they wait on that executor's reactor once it is set
//! as the foreign one ([`set_foreign_reactor`]), as the executor's own
//! sockets do; with none, their waits fail as unsupported.
//!
//! ```no_run
//! use ruio::io::{AsyncRead, AsyncWrite};
//!
//! /// Sends a greeting and reads the reply, over any stream.
//! async fn greet<S: AsyncRead + AsyncWrite>(stream: &S) -> std::io::Result<Vec<u8>> {
//!     stream.write_all("hello").await.0?;
//!     let (read, reply) = stream.read_exact(Vec::with_capacity(5)).await;
//!     read?;
//!     Ok(reply)
//! }
//! ```
//!
//! Not here yet, of what `io` has on io_uring: `splice`, which moves data
//! through a pipe in the kernel, and the two `AsyncFd`s, which wait for a
//! descriptor of any kind to be ready.

use crate::buf::{IoBuf, IoBufMut};
use crate::fs::{self, File};
use crate::net::{self, TcpStream};

#[path = "io/foreign.rs"]
mod foreign;
#[path = "io/nonblocking.rs"]
mod nonblocking;
#[path = "io/portable.rs"]
pub mod portable;
#[path = "../io/traits.rs"]
mod traits;

pub use foreign::{
    Reactor, Registration, clear_foreign_reactor, foreign_reactor, set_foreign_reactor,
};
pub(crate) use nonblocking::{Nonblocking, READABLE, WRITABLE};
pub use nonblocking::{
    PortableRead, PortableReadExact, PortableWrite, PortableWriteAll, PortableWriteAllVectored,
};
pub use traits::{AsyncRead, AsyncWrite, Copy, ReadExact, ReadToEnd, WriteAll, copy};

impl AsyncRead for TcpStream {
    type Read<'a, B: IoBufMut> = net::Read<'a, B>;

    #[inline]
    fn read<B: IoBufMut>(&self, buf: B) -> Self::Read<'_, B> {
        TcpStream::read(self, buf)
    }
}

impl AsyncWrite for TcpStream {
    type Write<'a, B: IoBuf> = net::Write<'a, B>;

    #[inline]
    fn write<B: IoBuf>(&self, buf: B) -> Self::Write<'_, B> {
        TcpStream::write(self, buf)
    }
}

/// Reads at the file's position, which each read advances, as
/// [`File::read`] does.
impl AsyncRead for File {
    type Read<'a, B: IoBufMut> = fs::Read<'a, B>;

    #[inline]
    fn read<B: IoBufMut>(&self, buf: B) -> Self::Read<'_, B> {
        File::read(self, buf)
    }
}

/// Writes at the file's position, which each write advances, as
/// [`File::write`] does.
impl AsyncWrite for File {
    type Write<'a, B: IoBuf> = fs::Write<'a, B>;

    #[inline]
    fn write<B: IoBuf>(&self, buf: B) -> Self::Write<'_, B> {
        File::write(self, buf)
    }
}
