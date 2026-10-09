//! The adapter for ruio's portable streams, which reads into the caller's
//! buffer and writes from it, and so needs no buffer of its own.

use std::fmt;
use std::io::{self, IoSlice};
use std::mem::MaybeUninit;
use std::net::Shutdown;
use std::task::{Context, Poll};

use ruio::net::portable::{TcpStream, UnixStream};

/// What a [`PortableCompat`] adapts: a portable
/// [`TcpStream`](ruio::net::portable::TcpStream) or
/// [`UnixStream`](ruio::net::portable::UnixStream), whose own poll methods it
/// reads and writes through.
///
/// The trait is sealed: only this crate's types implement it.
pub trait PortableStream: sealed::Sealed {}

impl PortableStream for TcpStream {}
impl PortableStream for UnixStream {}

mod sealed {
    use super::*;

    /// The portable streams' poll methods, which the adapter calls.
    pub trait Sealed {
        fn poll_read(
            &mut self,
            cx: &mut Context<'_>,
            buf: &mut [MaybeUninit<u8>],
        ) -> Poll<io::Result<usize>>;
        fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>>;
        fn poll_write_vectored(
            &mut self,
            cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>>;
        fn shutdown(&self, how: Shutdown) -> io::Result<()>;
    }

    macro_rules! sealed {
        ($stream:ty) => {
            impl Sealed for $stream {
                #[inline]
                fn poll_read(
                    &mut self,
                    cx: &mut Context<'_>,
                    buf: &mut [MaybeUninit<u8>],
                ) -> Poll<io::Result<usize>> {
                    <$stream>::poll_read(self, cx, buf)
                }

                #[inline]
                fn poll_write(
                    &mut self,
                    cx: &mut Context<'_>,
                    buf: &[u8],
                ) -> Poll<io::Result<usize>> {
                    <$stream>::poll_write(self, cx, buf)
                }

                #[inline]
                fn poll_write_vectored(
                    &mut self,
                    cx: &mut Context<'_>,
                    bufs: &[IoSlice<'_>],
                ) -> Poll<io::Result<usize>> {
                    <$stream>::poll_write_vectored(self, cx, bufs)
                }

                #[inline]
                fn shutdown(&self, how: Shutdown) -> io::Result<()> {
                    <$stream>::shutdown(self, how)
                }
            }
        };
    }

    sealed!(TcpStream);
    sealed!(UnixStream);
}

/// An adapter from one of ruio's portable streams to the poll-based I/O
/// traits: tokio's, futures-io's and hyper's, each behind the feature of its
/// name.
///
/// A portable stream reads and writes with system calls made at once, on
/// whichever thread polls it, which wait on that thread's ring only when
/// there is nothing to read or no room to write, so the adapter needs no
/// buffer of its own, as [`Compat`](crate::Compat) needs for a stream of
/// its thread's ring: a read is a `read(2)` into the caller's buffer, a
/// write a `send(2)`, or a vectored `sendmsg(2)`, from the caller's buffers,
/// with no copy either way. A flush has nothing to wait for, and is ready at
/// once; shutting down (tokio's `poll_shutdown`, futures-io's `poll_close`)
/// shuts the connection's writing half down. A read or a write that waits
/// keeps its wait in the stream until it is polled again, as the
/// poll-based traits poll it.
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

    /// Shuts the writing half of the connection down, at once.
    fn poll_shutdown_write(&mut self) -> Poll<io::Result<()>> {
        Poll::Ready(self.stream.shutdown(Shutdown::Write))
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

    impl<S: PortableStream + Unpin> AsyncRead for PortableCompat<S> {
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

    impl<S: PortableStream + Unpin> AsyncWrite for PortableCompat<S> {
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

        /// Yes: the buffers go together, with one `sendmsg(2)`.
        fn is_write_vectored(&self) -> bool {
            true
        }

        /// Ready at once: the adapter holds nothing back.
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        /// Shuts the writing half of the connection down, at once.
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

    impl<S: PortableStream + Unpin> AsyncRead for PortableCompat<S> {
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

    impl<S: PortableStream + Unpin> AsyncWrite for PortableCompat<S> {
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

        /// Shuts the writing half of the connection down, at once.
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

    impl<S: PortableStream + Unpin> Read for PortableCompat<S> {
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

    impl<S: PortableStream + Unpin> Write for PortableCompat<S> {
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
        /// with one `sendmsg(2)`.
        fn is_write_vectored(&self) -> bool {
            true
        }

        /// Ready at once: the adapter holds nothing back.
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        /// Shuts the writing half of the connection down, at once.
        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.get_mut().poll_shutdown_write()
        }
    }
}
