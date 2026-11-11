//! Reading and writing with owned buffers, generically: the traits ruio's
//! sockets and files share, and what is built on them.
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
//! runs, so that the writer and the reader keep each other busy; [`splice`]
//! moves everything one descriptor has to another through a pipe in the
//! kernel, with no buffer in user space at all. [`AsyncFd`] wraps any other
//! descriptor, a timer, an inotify instance, a device, so that a task can
//! wait for it to be ready, once or again and again, and read and write it
//! through the ring.
//!
//! The portable sockets of [`net::portable`] and a
//! child's pipes read and write with system calls made at once instead, on
//! whichever thread polls them, and wait on that thread's ring only when
//! there is nothing to read or no room to write: their reads and writes are
//! [`PortableRead`]s and [`PortableWrite`]s, which are `Send` and `Sync`, and
//! spend a step of the task's budget ([`coop`](crate::coop)) each time they
//! move data.
//!
//! Polled on a thread of another executor's, where no runtime of ruio's
//! runs, they wait on that executor's reactor once it is set as the foreign
//! one ([`set_foreign_reactor`]), as the executor's own sockets do, rather
//! than on the foreign ring, which costs each wait two wakes across threads;
//! `ruio-compat`'s `TokioReactor` is tokio's.
//!
//! [`portable::AsyncFd`] wraps any descriptor for a task that must be
//! `Send`, with the readiness API of tokio's `AsyncFd`: its waits run on the
//! ring of the thread that polls them, and the owner reads and writes with
//! nonblocking calls of its own.
//!
//! A stream socket's own `read_exact` and `write_all` stay what they are,
//! with their linked timeouts and `MSG_WAITALL`: a method call on a stream
//! reaches them, and the traits' through a generic bound only.
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

use crate::buf::{IoBuf, IoBufMut};
use crate::fs::{self, File};
use crate::net::{self, TcpStream, UnixStream};

mod async_fd;
mod foreign;
mod nonblocking;
pub mod portable;
mod splice;
mod traits;

pub use async_fd::{AsyncFd, FdOp, FdRead, FdWrite, Readiness, Ready};
pub use foreign::{
    Reactor, Registration, clear_foreign_reactor, foreign_reactor, set_foreign_reactor,
};
pub(crate) use nonblocking::{Kind, Nonblocking};
pub use nonblocking::{
    PortableRead, PortableReadExact, PortableWrite, PortableWriteAll, PortableWriteAllVectored,
};
pub use splice::{Splice, Spliceable, splice};
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

impl AsyncRead for UnixStream {
    type Read<'a, B: IoBufMut> = net::Read<'a, B>;

    #[inline]
    fn read<B: IoBufMut>(&self, buf: B) -> Self::Read<'_, B> {
        UnixStream::read(self, buf)
    }
}

impl AsyncWrite for UnixStream {
    type Write<'a, B: IoBuf> = net::Write<'a, B>;

    #[inline]
    fn write<B: IoBuf>(&self, buf: B) -> Self::Write<'_, B> {
        UnixStream::write(self, buf)
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
