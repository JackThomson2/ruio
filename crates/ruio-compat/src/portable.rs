//! The adapter for ruio's portable streams and a child's pipes, which reads
//! into the caller's buffer and writes from it, and so needs no buffer of
//! its own.

use std::fmt;
use std::io::{self, IoSlice};
use std::mem::MaybeUninit;
use std::net::Shutdown;
use std::task::{Context, Poll};

use ruio::net::portable::{TcpStream, UnixStream};
use ruio::process::{ChildStderr, ChildStdin, ChildStdout};

/// What a [`PortableCompat`] adapts: a portable
/// [`TcpStream`](ruio::net::portable::TcpStream) or
/// [`UnixStream`](ruio::net::portable::UnixStream), which it reads and writes,
/// or a child's pipe, which it reads, a
/// [`ChildStdout`](ruio::process::ChildStdout) or a
/// [`ChildStderr`](ruio::process::ChildStderr), or writes, a
/// [`ChildStdin`](ruio::process::ChildStdin), through the poll methods of
/// its own. [`PortableReader`] and [`PortableWriter`] say which way.
///
/// The trait is sealed: only this crate's types implement it.
pub trait PortableStream: sealed::Sealed {}

/// A [`PortableStream`] that the adapter reads: a stream, or the pipe of a
/// child's standard output or error. Its adapter has the `AsyncRead` of
/// tokio and of futures-io, and hyper's `rt::Read`.
///
/// The trait is sealed: only this crate's types implement it.
pub trait PortableReader: PortableStream + sealed::Read {}

/// A [`PortableStream`] that the adapter writes: a stream, or the pipe of a
/// child's standard input. Its adapter has the `AsyncWrite` of tokio and of
/// futures-io, and hyper's `rt::Write`.
///
/// The trait is sealed: only this crate's types implement it.
pub trait PortableWriter: PortableStream + sealed::Write {}

impl PortableStream for TcpStream {}
impl PortableStream for UnixStream {}
impl PortableStream for ChildStdin {}
impl PortableStream for ChildStdout {}
impl PortableStream for ChildStderr {}

impl PortableReader for TcpStream {}
impl PortableReader for UnixStream {}
impl PortableReader for ChildStdout {}
impl PortableReader for ChildStderr {}

impl PortableWriter for TcpStream {}
impl PortableWriter for UnixStream {}
impl PortableWriter for ChildStdin {}

mod sealed {
    use super::*;

    /// What the adapter adapts.
    pub trait Sealed {}

    /// The poll method the adapter reads through.
    pub trait Read {
        fn poll_read(
            &mut self,
            cx: &mut Context<'_>,
            buf: &mut [MaybeUninit<u8>],
        ) -> Poll<io::Result<usize>>;
    }

    /// The poll methods the adapter writes through, and what shutting down
    /// does.
    pub trait Write {
        fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>>;
        fn poll_write_vectored(
            &mut self,
            cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>>;
        /// Shuts the writing half of a connection down; a pipe has none.
        fn shutdown_write(&self) -> io::Result<()>;
    }

    impl Sealed for TcpStream {}
    impl Sealed for UnixStream {}
    impl Sealed for ChildStdin {}
    impl Sealed for ChildStdout {}
    impl Sealed for ChildStderr {}

    /// The reading poll method of each `$reader`.
    macro_rules! reader {
        ($($reader:ty),*) => {$(
            impl Read for $reader {
                #[inline]
                fn poll_read(
                    &mut self,
                    cx: &mut Context<'_>,
                    buf: &mut [MaybeUninit<u8>],
                ) -> Poll<io::Result<usize>> {
                    <$reader>::poll_read(self, cx, buf)
                }
            }
        )*};
    }

    /// The writing poll methods of `$writer`, and `$shutdown`, which shuts
    /// its writing half down with the writer bound to `$this`.
    macro_rules! writer {
        ($writer:ty, $this:pat => $shutdown:expr) => {
            impl Write for $writer {
                #[inline]
                fn poll_write(
                    &mut self,
                    cx: &mut Context<'_>,
                    buf: &[u8],
                ) -> Poll<io::Result<usize>> {
                    <$writer>::poll_write(self, cx, buf)
                }

                #[inline]
                fn poll_write_vectored(
                    &mut self,
                    cx: &mut Context<'_>,
                    bufs: &[IoSlice<'_>],
                ) -> Poll<io::Result<usize>> {
                    <$writer>::poll_write_vectored(self, cx, bufs)
                }

                #[inline]
                fn shutdown_write(&self) -> io::Result<()> {
                    let $this = self;
                    $shutdown
                }
            }
        };
    }

    reader!(TcpStream, UnixStream, ChildStdout, ChildStderr);
    writer!(TcpStream, stream => stream.shutdown(Shutdown::Write));
    writer!(UnixStream, stream => stream.shutdown(Shutdown::Write));
    // Closed as it is dropped, which the child reads as the end of its
    // input: shutting down has nothing to do before then.
    writer!(ChildStdin, _ => Ok(()));
}

/// An adapter from one of ruio's portable streams, or a child's pipe, to the
/// poll-based I/O traits: tokio's, futures-io's and hyper's, each behind the
/// feature of its name. A stream has the reading traits and the writing
/// ones; a child's standard output or error only the reading ones
/// ([`PortableReader`]), and its standard input only the writing ones
/// ([`PortableWriter`]).
///
/// A portable stream, as a child's pipe, reads and writes with system calls
/// made at once, on whichever thread polls it, which wait on that thread's
/// ring only when there is nothing to read or no room to write, so the
/// adapter needs no buffer of its own, as [`Compat`](crate::Compat) needs
/// for a stream of its thread's ring: a read is a `read(2)` into the
/// caller's buffer, a write a `send(2)`, or a vectored `sendmsg(2)`, from
/// the caller's buffers, or to a pipe a `write(2)` or a `writev(2)`, with no
/// copy either way. A flush has nothing to wait for, and is ready at once;
/// shutting down (tokio's `poll_shutdown`, futures-io's `poll_close`) shuts
/// a connection's writing half down, and is ready at once for a child's
/// standard input, as tokio's own `ChildStdin` is, which closes as the
/// adapter, or the pipe, is dropped. A read or a write that waits keeps its
/// wait in the stream until it is polled again, as the poll-based traits
/// poll it.
///
/// It is `Send` and `Sync`, as the stream is, so a connection that must be,
/// as hyper-util's client and hyper's upgrades want theirs, can be one of
/// ruio's, and it moves between threads and cores with the task that holds
/// it: whichever thread polls it next waits on its own ring.
///
/// ```no_run
/// use ruio::net::portable::TcpStream;
/// use ruio_compat::PortableCompat;
/// use tokio::io::{AsyncReadExt, AsyncWriteExt};
///
/// fn assert_send<T: Send + Sync>(_: &T) {}
///
/// # ruio::Runtime::new().unwrap().block_on(async {
/// let mut stream = PortableCompat::new(TcpStream::connect("127.0.0.1:7878").await?);
/// assert_send(&stream);
/// stream.write_all(b"hello").await?;
/// stream.shutdown().await?;
/// let mut reply = Vec::new();
/// stream.read_to_end(&mut reply).await?;
/// # Ok::<(), std::io::Error>(())
/// # }).unwrap();
/// ```
///
/// A child's pipes, with tokio's traits:
///
/// ```no_run
/// use std::process::Stdio;
///
/// use ruio::process::Command;
/// use ruio_compat::PortableCompat;
/// use tokio::io::{AsyncReadExt, AsyncWriteExt};
///
/// # ruio::Runtime::new().unwrap().block_on(async {
/// let mut child = Command::new("cat")
///     .stdin(Stdio::piped())
///     .stdout(Stdio::piped())
///     .spawn()?;
/// let mut stdin = PortableCompat::new(child.stdin.take().unwrap());
/// stdin.write_all(b"hello").await?;
/// // Closes the pipe: cat's input ends.
/// drop(stdin);
/// let mut stdout = PortableCompat::new(child.stdout.take().unwrap());
/// let mut out = Vec::new();
/// stdout.read_to_end(&mut out).await?;
/// assert_eq!(out, b"hello");
/// # Ok::<(), std::io::Error>(())
/// # }).unwrap();
/// ```
pub struct PortableCompat<S> {
    stream: S,
}

impl<S: PortableStream> PortableCompat<S> {
    /// Adapts `stream`.
    pub fn new(stream: S) -> Self {
        Self { stream }
    }

    /// The stream.
    pub fn get_ref(&self) -> &S {
        &self.stream
    }

    /// The stream, mutably.
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.stream
    }

    /// The stream, out of the adapter, which holds nothing back.
    pub fn into_inner(self) -> S {
        self.stream
    }
}

impl<S: PortableWriter> PortableCompat<S> {
    /// Shuts the writing half of the connection down, at once; a pipe has
    /// none, and is ready at once.
    fn poll_shutdown_write(&mut self) -> Poll<io::Result<()>> {
        Poll::Ready(self.stream.shutdown_write())
    }
}

impl<S: PortableStream> From<S> for PortableCompat<S> {
    fn from(stream: S) -> Self {
        Self::new(stream)
    }
}

impl<S: fmt::Debug> fmt::Debug for PortableCompat<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PortableCompat")
            .field("stream", &self.stream)
            .finish()
    }
}

#[cfg(feature = "tokio")]
mod tokio_impls {
    use std::pin::Pin;
    use std::task::ready;

    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

    use super::*;

    impl<S: PortableReader + Unpin> AsyncRead for PortableCompat<S> {
        /// Reads into the unfilled part of `buf` what has arrived, waiting
        /// for something to if nothing has.
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            // SAFETY: the read writes none but initialized bytes, so it
            // de-initializes nothing.
            let unfilled = unsafe { buf.unfilled_mut() };
            let read = ready!(this.stream.poll_read(cx, unfilled))?;
            // SAFETY: the read initialized the first `read` unfilled bytes.
            unsafe { buf.assume_init(read) };
            buf.advance(read);
            Poll::Ready(Ok(()))
        }
    }

    impl<S: PortableWriter + Unpin> AsyncWrite for PortableCompat<S> {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.get_mut().stream.poll_write(cx, buf)
        }

        fn poll_write_vectored(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            self.get_mut().stream.poll_write_vectored(cx, bufs)
        }

        /// Yes: the buffers go together, with one `sendmsg(2)`, or one
        /// `writev(2)` to a pipe.
        fn is_write_vectored(&self) -> bool {
            true
        }

        /// Ready at once: the adapter holds nothing back.
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        /// Shuts the writing half of the connection down, at once; a pipe
        /// has none to shut, and closes as it is dropped.
        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.get_mut().poll_shutdown_write()
        }
    }
}

#[cfg(feature = "futures-io")]
mod futures_impls {
    use std::pin::Pin;

    use futures_io::{AsyncRead, AsyncWrite};

    use super::*;

    impl<S: PortableReader + Unpin> AsyncRead for PortableCompat<S> {
        /// Reads into `buf` what has arrived, waiting for something to if
        /// nothing has.
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            // SAFETY: the read writes none but initialized bytes, so the
            // bytes stay initialized.
            let buf = unsafe { &mut *(std::ptr::from_mut(buf) as *mut [MaybeUninit<u8>]) };
            self.get_mut().stream.poll_read(cx, buf)
        }
    }

    impl<S: PortableWriter + Unpin> AsyncWrite for PortableCompat<S> {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.get_mut().stream.poll_write(cx, buf)
        }

        fn poll_write_vectored(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            self.get_mut().stream.poll_write_vectored(cx, bufs)
        }

        /// Ready at once: the adapter holds nothing back.
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        /// Shuts the writing half of the connection down, at once; a pipe
        /// has none to shut, and closes as it is dropped.
        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.get_mut().poll_shutdown_write()
        }
    }
}

#[cfg(feature = "hyper")]
mod hyper_impls {
    use std::pin::Pin;
    use std::task::ready;

    use hyper::rt::{Read, ReadBufCursor, Write};

    use super::*;

    impl<S: PortableReader + Unpin> Read for PortableCompat<S> {
        /// Reads into hyper's buffer what has arrived, waiting for something
        /// to if nothing has.
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            mut buf: ReadBufCursor<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            // SAFETY: the read writes none but initialized bytes, so it
            // de-initializes nothing.
            let room = unsafe { buf.as_mut() };
            let read = ready!(this.stream.poll_read(cx, room))?;
            // SAFETY: the read initialized the first `read` bytes of the
            // cursor's room.
            unsafe { buf.advance(read) };
            Poll::Ready(Ok(()))
        }
    }

    impl<S: PortableWriter + Unpin> Write for PortableCompat<S> {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.get_mut().stream.poll_write(cx, buf)
        }

        fn poll_write_vectored(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            self.get_mut().stream.poll_write_vectored(cx, bufs)
        }

        /// Yes: hyper hands a message's head and body over at once, which go
        /// with one `sendmsg(2)`, or one `writev(2)` to a pipe.
        fn is_write_vectored(&self) -> bool {
            true
        }

        /// Ready at once: the adapter holds nothing back.
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        /// Shuts the writing half of the connection down, at once; a pipe
        /// has none to shut, and closes as it is dropped.
        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.get_mut().poll_shutdown_write()
        }
    }
}
