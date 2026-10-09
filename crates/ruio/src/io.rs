//! Reading and writing with owned buffers, generically: the traits ruio's
//! sockets and files share, and what is built on them.
//!
//! [`AsyncRead::read`] and [`AsyncWrite::write`] take a buffer by value and
//! hand it back with the result, as the sockets' and the files' own methods
//! do, and each gives the type's own future, named by an associated type, so
//! that code generic over a reader or a writer compiles to the operations
//! the type's methods make: no boxing, no dynamic dispatch, no allocation
//! beyond the buffer the caller brings. A reference to a reader or writer,
//! an [`Rc`] or a [`Box`] of one, is one too.
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
//! A child's pipes read and write with system calls made at once instead,
//! on whichever thread polls them, and wait on that thread's ring only when
//! there is nothing to read or no room to write: their reads and writes are
//! [`PortableRead`]s and [`PortableWrite`]s, which are `Send` and `Sync`, and
//! spend a step of the task's budget ([`coop`](crate::coop)) each time they
//! move data.
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

use std::fmt;
use std::future::Future;
use std::io;
use std::mem;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, ready};

use ruio_driver::buf::{BufResult, IoBuf, IoBufMut, Slice};
use ruio_driver::ops::{self, Read, Recv, Write};

use crate::fs::{File, FileOp};
use crate::net::{SocketOp, TcpStream, UnixStream};

mod async_fd;
mod nonblocking;
mod splice;

pub use async_fd::{AsyncFd, FdOp, Readiness, Ready};
pub(crate) use nonblocking::Nonblocking;
pub use nonblocking::{PortableRead, PortableWrite};
pub use splice::{Splice, Spliceable, splice};

/// How many bytes each of [`copy`]'s two buffers holds.
const COPY_BUF: usize = 64 << 10;

/// How much spare room [`read_to_end`](AsyncRead::read_to_end) makes at
/// least before each read.
const READ_TO_END_ROOM: usize = 32 << 10;

/// A source of bytes read with owned buffers: a stream socket, or a file at
/// its position.
///
/// [`read`](AsyncRead::read) fills a buffer from its start, up to its
/// [`bytes_total`](IoBuf::bytes_total), and hands it back with how many
/// bytes it read, 0 at the end: once the peer has shut down, or at the end
/// of a file. The future is the reader's own, named by
/// [`Read`](AsyncRead::Read), so a caller generic over the reader pays
/// nothing for it.
pub trait AsyncRead {
    /// The future of a read, which borrows the reader and holds the buffer
    /// until it is done.
    type Read<'a, B: IoBufMut>: Future<Output = BufResult<usize, B>>
    where
        Self: 'a;

    /// Reads into `buf`, from its start up to its capacity, and hands it
    /// back with how many bytes it read; 0 at the end.
    fn read<B: IoBufMut>(&self, buf: B) -> Self::Read<'_, B>;

    /// Reads until `buf` is full, to its capacity, one read after another;
    /// see [`ReadExact`].
    #[inline]
    fn read_exact<B: IoBufMut>(&self, buf: B) -> ReadExact<'_, Self, B> {
        ReadExact {
            reader: self,
            state: Transfer::Start(buf),
        }
    }

    /// Reads everything to the end, after what `buf` holds, growing it as
    /// needed; see [`ReadToEnd`].
    #[inline]
    fn read_to_end(&self, buf: Vec<u8>) -> ReadToEnd<'_, Self> {
        ReadToEnd {
            reader: self,
            state: Transfer::Start(buf),
            read: 0,
        }
    }
}

/// A sink of bytes written with owned buffers: a stream socket, or a file at
/// its position.
///
/// [`write`](AsyncWrite::write) writes a buffer's initialized bytes, or as
/// many as the sink takes, and hands it back with how many it wrote. The
/// future is the writer's own, named by [`Write`](AsyncWrite::Write).
pub trait AsyncWrite {
    /// The future of a write, which borrows the writer and holds the buffer
    /// until it is done.
    type Write<'a, B: IoBuf>: Future<Output = BufResult<usize, B>>
    where
        Self: 'a;

    /// Writes `buf`'s initialized bytes, or as many as the writer takes,
    /// and hands it back with how many it wrote.
    fn write<B: IoBuf>(&self, buf: B) -> Self::Write<'_, B>;

    /// Writes all of `buf`'s initialized bytes, one write after another;
    /// see [`WriteAll`].
    #[inline]
    fn write_all<B: IoBuf>(&self, buf: B) -> WriteAll<'_, Self, B> {
        WriteAll {
            writer: self,
            state: Transfer::Start(buf),
        }
    }
}

impl AsyncRead for TcpStream {
    type Read<'a, B: IoBufMut> = SocketOp<'a, Recv<B>>;

    #[inline]
    fn read<B: IoBufMut>(&self, buf: B) -> Self::Read<'_, B> {
        TcpStream::read(self, buf)
    }
}

impl AsyncWrite for TcpStream {
    type Write<'a, B: IoBuf> = SocketOp<'a, ops::Send<B>>;

    #[inline]
    fn write<B: IoBuf>(&self, buf: B) -> Self::Write<'_, B> {
        TcpStream::write(self, buf)
    }
}

impl AsyncRead for UnixStream {
    type Read<'a, B: IoBufMut> = SocketOp<'a, Recv<B>>;

    #[inline]
    fn read<B: IoBufMut>(&self, buf: B) -> Self::Read<'_, B> {
        UnixStream::read(self, buf)
    }
}

impl AsyncWrite for UnixStream {
    type Write<'a, B: IoBuf> = SocketOp<'a, ops::Send<B>>;

    #[inline]
    fn write<B: IoBuf>(&self, buf: B) -> Self::Write<'_, B> {
        UnixStream::write(self, buf)
    }
}

/// Reads at the file's position, which each read advances, as
/// [`File::read`] does.
impl AsyncRead for File {
    type Read<'a, B: IoBufMut> = FileOp<'a, Read<B>>;

    #[inline]
    fn read<B: IoBufMut>(&self, buf: B) -> Self::Read<'_, B> {
        File::read(self, buf)
    }
}

/// Writes at the file's position, which each write advances, as
/// [`File::write`] does.
impl AsyncWrite for File {
    type Write<'a, B: IoBuf> = FileOp<'a, Write<B>>;

    #[inline]
    fn write<B: IoBuf>(&self, buf: B) -> Self::Write<'_, B> {
        File::write(self, buf)
    }
}

impl<T: AsyncRead + ?Sized> AsyncRead for &T {
    type Read<'a, B: IoBufMut>
        = T::Read<'a, B>
    where
        Self: 'a;

    #[inline]
    fn read<B: IoBufMut>(&self, buf: B) -> Self::Read<'_, B> {
        T::read(self, buf)
    }
}

impl<T: AsyncWrite + ?Sized> AsyncWrite for &T {
    type Write<'a, B: IoBuf>
        = T::Write<'a, B>
    where
        Self: 'a;

    #[inline]
    fn write<B: IoBuf>(&self, buf: B) -> Self::Write<'_, B> {
        T::write(self, buf)
    }
}

impl<T: AsyncRead + ?Sized> AsyncRead for Rc<T> {
    type Read<'a, B: IoBufMut>
        = T::Read<'a, B>
    where
        Self: 'a;

    #[inline]
    fn read<B: IoBufMut>(&self, buf: B) -> Self::Read<'_, B> {
        T::read(self, buf)
    }
}

impl<T: AsyncWrite + ?Sized> AsyncWrite for Rc<T> {
    type Write<'a, B: IoBuf>
        = T::Write<'a, B>
    where
        Self: 'a;

    #[inline]
    fn write<B: IoBuf>(&self, buf: B) -> Self::Write<'_, B> {
        T::write(self, buf)
    }
}

impl<T: AsyncRead + ?Sized> AsyncRead for Box<T> {
    type Read<'a, B: IoBufMut>
        = T::Read<'a, B>
    where
        Self: 'a;

    #[inline]
    fn read<B: IoBufMut>(&self, buf: B) -> Self::Read<'_, B> {
        T::read(self, buf)
    }
}

impl<T: AsyncWrite + ?Sized> AsyncWrite for Box<T> {
    type Write<'a, B: IoBuf>
        = T::Write<'a, B>
    where
        Self: 'a;

    #[inline]
    fn write<B: IoBuf>(&self, buf: B) -> Self::Write<'_, B> {
        T::write(self, buf)
    }
}

/// Where a transfer through one future after another is: what
/// [`ReadExact`], [`ReadToEnd`] and [`WriteAll`] share.
///
/// The future is pinned where it lies in the variant: a transfer never
/// moves it, and replaces the state only by assignment, which drops it in
/// place. The buffer is never pinned, and moves freely.
enum Transfer<F, B> {
    /// Not started: the buffer, as it was given.
    Start(B),
    /// A future moving the buffer on, past the `done` bytes before it.
    Moving { future: F, done: usize },
    /// Over: the buffer has been handed back.
    Finished,
}

impl<F, B> Transfer<F, B> {
    /// The buffer of a transfer that has not started.
    #[inline]
    fn take_start(&mut self) -> B {
        match mem::replace(self, Transfer::Finished) {
            Transfer::Start(buf) => buf,
            _ => unreachable!("a transfer starts once"),
        }
    }
}

/// Fills a whole buffer from a reader, one read after another until it is
/// full; made by [`AsyncRead::read_exact`].
///
/// The read fills the buffer from its start, up to its
/// [`bytes_total`](IoBuf::bytes_total). The output is the buffer, and
/// `Ok(())` once it is full, or the error that stopped the read:
/// [`io::ErrorKind::UnexpectedEof`] if the reader ended first. What arrived
/// is marked initialized either way.
#[must_use = "a read does nothing until polled"]
pub struct ReadExact<'a, R: AsyncRead + ?Sized, B: IoBufMut> {
    reader: &'a R,
    state: Transfer<R::Read<'a, Slice<B>>, B>,
}

impl<'a, R: AsyncRead + ?Sized, B: IoBufMut> Future for ReadExact<'a, R, B> {
    type Output = BufResult<(), B>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: the future in the state is the only thing pinned here, and
        // it is never moved: it is polled through a pin made in place, and
        // let go of by assigning to the state, which drops it where it lies.
        let this = unsafe { self.get_unchecked_mut() };
        loop {
            match &mut this.state {
                Transfer::Start(_) => {
                    let buf = this.state.take_start();
                    if buf.bytes_total() == 0 {
                        return Poll::Ready((Ok(()), buf));
                    }
                    let future = this.reader.read(buf.slice(..));
                    this.state = Transfer::Moving { future, done: 0 };
                }
                Transfer::Moving { future, done } => {
                    // SAFETY: as above: the future stays where it is until
                    // the state is assigned, which drops it there.
                    let future = unsafe { Pin::new_unchecked(future) };
                    let (read, slice) = ready!(future.poll(cx));
                    let done = *done;
                    let buf = slice.into_inner();
                    this.state = Transfer::Finished;
                    match read {
                        Ok(0) => {
                            let err = io::ErrorKind::UnexpectedEof.into();
                            return Poll::Ready((Err(err), buf));
                        }
                        Ok(read) if done + read >= buf.bytes_total() => {
                            return Poll::Ready((Ok(()), buf));
                        }
                        Ok(read) => {
                            let done = done + read;
                            let future = this.reader.read(buf.slice(done..));
                            this.state = Transfer::Moving { future, done };
                        }
                        Err(err) => return Poll::Ready((Err(err), buf)),
                    }
                }
                Transfer::Finished => panic!("`ReadExact` polled after it completed"),
            }
        }
    }
}

// The buffer is never pinned, so the read is `Unpin` whenever the reader's
// future is.
impl<'a, R: AsyncRead + ?Sized, B: IoBufMut> Unpin for ReadExact<'a, R, B> where
    R::Read<'a, Slice<B>>: Unpin
{
}

impl<R: AsyncRead + ?Sized, B: IoBufMut> fmt::Debug for ReadExact<'_, R, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadExact").finish_non_exhaustive()
    }
}

/// Reads everything a reader has, to the end, after what a vector holds,
/// growing the vector as needed; made by [`AsyncRead::read_to_end`].
///
/// The output is the vector, and `Ok` with how many bytes were added, or
/// the error that stopped the read. What arrived before an error is in the
/// vector either way. Each read goes into all of the vector's spare room,
/// which is made at least 32 KiB first, so a long input costs few reads.
#[must_use = "a read does nothing until polled"]
pub struct ReadToEnd<'a, R: AsyncRead + ?Sized> {
    reader: &'a R,
    state: Transfer<R::Read<'a, Slice<Vec<u8>>>, Vec<u8>>,
    /// How many bytes have been added.
    read: usize,
}

impl<'a, R: AsyncRead + ?Sized> ReadToEnd<'a, R> {
    /// Starts a read into the vector's spare room, made at least
    /// [`READ_TO_END_ROOM`] first.
    #[inline]
    fn start(&mut self, mut buf: Vec<u8>) {
        if buf.capacity() - buf.len() < READ_TO_END_ROOM {
            buf.reserve(READ_TO_END_ROOM);
        }
        let done = buf.len();
        let future = self.reader.read(buf.slice(done..));
        self.state = Transfer::Moving { future, done };
    }
}

impl<R: AsyncRead + ?Sized> Future for ReadToEnd<'_, R> {
    type Output = BufResult<usize, Vec<u8>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: as for `ReadExact`: the future in the state is never
        // moved, and is dropped in place by assigning to the state.
        let this = unsafe { self.get_unchecked_mut() };
        loop {
            match &mut this.state {
                Transfer::Start(_) => {
                    let buf = this.state.take_start();
                    this.start(buf);
                }
                Transfer::Moving { future, .. } => {
                    // SAFETY: as for `ReadExact`.
                    let future = unsafe { Pin::new_unchecked(future) };
                    let (read, slice) = ready!(future.poll(cx));
                    let buf = slice.into_inner();
                    this.state = Transfer::Finished;
                    match read {
                        Ok(0) => return Poll::Ready((Ok(this.read), buf)),
                        Ok(read) => {
                            this.read += read;
                            this.start(buf);
                        }
                        Err(err) => return Poll::Ready((Err(err), buf)),
                    }
                }
                Transfer::Finished => panic!("`ReadToEnd` polled after it completed"),
            }
        }
    }
}

// As for `ReadExact`.
impl<'a, R: AsyncRead + ?Sized> Unpin for ReadToEnd<'a, R> where R::Read<'a, Slice<Vec<u8>>>: Unpin {}

impl<R: AsyncRead + ?Sized> fmt::Debug for ReadToEnd<'_, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadToEnd")
            .field("read", &self.read)
            .finish_non_exhaustive()
    }
}

/// Writes a whole buffer to a writer, one write after another until all of
/// it is written; made by [`AsyncWrite::write_all`].
///
/// The output is the buffer, and `Ok(())` once all of its initialized bytes
/// are written, or the error that stopped the write:
/// [`io::ErrorKind::WriteZero`] if the writer took nothing.
#[must_use = "a write does nothing until polled"]
pub struct WriteAll<'a, W: AsyncWrite + ?Sized, B: IoBuf> {
    writer: &'a W,
    state: Transfer<W::Write<'a, Slice<B>>, B>,
}

impl<'a, W: AsyncWrite + ?Sized, B: IoBuf> Future for WriteAll<'a, W, B> {
    type Output = BufResult<(), B>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: as for `ReadExact`: the future in the state is never
        // moved, and is dropped in place by assigning to the state.
        let this = unsafe { self.get_unchecked_mut() };
        loop {
            match &mut this.state {
                Transfer::Start(_) => {
                    let buf = this.state.take_start();
                    if buf.bytes_init() == 0 {
                        return Poll::Ready((Ok(()), buf));
                    }
                    let future = this.writer.write(buf.slice(..));
                    this.state = Transfer::Moving { future, done: 0 };
                }
                Transfer::Moving { future, done } => {
                    // SAFETY: as for `ReadExact`.
                    let future = unsafe { Pin::new_unchecked(future) };
                    let (written, slice) = ready!(future.poll(cx));
                    let done = *done;
                    let buf = slice.into_inner();
                    this.state = Transfer::Finished;
                    match written {
                        Ok(0) => {
                            let err = io::ErrorKind::WriteZero.into();
                            return Poll::Ready((Err(err), buf));
                        }
                        Ok(written) if done + written >= buf.bytes_init() => {
                            return Poll::Ready((Ok(()), buf));
                        }
                        Ok(written) => {
                            let done = done + written;
                            let future = this.writer.write(buf.slice(done..));
                            this.state = Transfer::Moving { future, done };
                        }
                        Err(err) => return Poll::Ready((Err(err), buf)),
                    }
                }
                Transfer::Finished => panic!("`WriteAll` polled after it completed"),
            }
        }
    }
}

// As for `ReadExact`.
impl<'a, W: AsyncWrite + ?Sized, B: IoBuf> Unpin for WriteAll<'a, W, B> where
    W::Write<'a, Slice<B>>: Unpin
{
}

impl<W: AsyncWrite + ?Sized, B: IoBuf> fmt::Debug for WriteAll<'_, W, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteAll").finish_non_exhaustive()
    }
}

/// Copies everything `reader` has to `writer`, until the reader ends; see
/// [`struct@Copy`].
///
/// ```no_run
/// use ruio::fs::File;
/// use ruio::net::TcpStream;
///
/// # ruio::Runtime::new().unwrap().block_on(async {
/// let file = File::open("page.html").await?;
/// let stream = TcpStream::connect("127.0.0.1:8080").await?;
/// let sent = ruio::io::copy(&file, &stream).await?;
/// println!("{sent} bytes");
/// # Ok::<(), std::io::Error>(())
/// # }).unwrap();
/// ```
pub fn copy<'a, R: AsyncRead + ?Sized, W: AsyncWrite + ?Sized>(
    reader: &'a R,
    writer: &'a W,
) -> Copy<'a, R, W> {
    Copy {
        reader,
        writer,
        read: None,
        write: None,
        written: 0,
        filled: None,
        free: [
            Some(Vec::with_capacity(COPY_BUF)),
            Some(Vec::with_capacity(COPY_BUF)),
        ],
        copied: 0,
        ended: false,
    }
}

/// Copies everything a reader has to a writer; made by [`copy`]. The output
/// is how many bytes were copied, or the error that stopped the copy.
///
/// The copy has two buffers of 64 KiB, and keeps a read and a write in
/// flight at once: while the writer takes one buffer, the reader fills the
/// other, so a reader and a writer that could each go faster than the
/// other's turn do. A buffer the writer took only part of goes back for the
/// rest, and one the reader filled waits for the writer to be done with the
/// one before, so the reader never gets ahead by more than a buffer.
#[must_use = "a copy does nothing until polled"]
pub struct Copy<'a, R: AsyncRead + ?Sized, W: AsyncWrite + ?Sized> {
    reader: &'a R,
    writer: &'a W,
    /// The read in flight, if one is; pinned where it lies, as for
    /// [`Transfer`].
    read: Option<R::Read<'a, Vec<u8>>>,
    /// The write in flight, if one is; likewise.
    write: Option<W::Write<'a, Slice<Vec<u8>>>>,
    /// How much of the buffer being written is written.
    written: usize,
    /// A buffer filled, waiting for the write slot.
    filled: Option<Vec<u8>>,
    /// The buffers free for a read.
    free: [Option<Vec<u8>>; 2],
    copied: u64,
    /// Whether the reader has ended.
    ended: bool,
}

impl<'a, R: AsyncRead + ?Sized, W: AsyncWrite + ?Sized> Copy<'a, R, W> {
    /// Puts `buf` among the free buffers, emptied.
    #[inline]
    fn free(&mut self, mut buf: Vec<u8>) {
        buf.clear();
        let slot = if self.free[0].is_none() {
            &mut self.free[0]
        } else {
            &mut self.free[1]
        };
        *slot = Some(buf);
    }
}

impl<R: AsyncRead + ?Sized, W: AsyncWrite + ?Sized> Future for Copy<'_, R, W> {
    type Output = io::Result<u64>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: the read and the write in flight are the only things
        // pinned here, each where it lies in its slot: polled through a pin
        // made in place, and let go of by assigning to the slot, which drops
        // it there. The buffers and the references move freely.
        let this = unsafe { self.get_unchecked_mut() };
        loop {
            if this.write.is_none()
                && let Some(buf) = this.filled.take()
            {
                this.written = 0;
                this.write = Some(this.writer.write(buf.slice(..)));
            }
            if this.read.is_none()
                && !this.ended
                && let Some(buf) = this.free[0].take().or_else(|| this.free[1].take())
            {
                this.read = Some(this.reader.read(buf));
            }
            if this.ended && this.write.is_none() && this.filled.is_none() {
                return Poll::Ready(Ok(this.copied));
            }
            let mut progressed = false;
            if let Some(write) = &mut this.write {
                // SAFETY: as above: the write stays where it is until the
                // slot is assigned, which drops it there.
                let write = unsafe { Pin::new_unchecked(write) };
                if let Poll::Ready((written, slice)) = write.poll(cx) {
                    this.write = None;
                    let buf = slice.into_inner();
                    match written {
                        Ok(0) => return Poll::Ready(Err(io::ErrorKind::WriteZero.into())),
                        Ok(written) => {
                            this.written += written;
                            if this.written < buf.len() {
                                let rest = buf.slice(this.written..);
                                this.write = Some(this.writer.write(rest));
                            } else {
                                this.copied += buf.len() as u64;
                                this.free(buf);
                            }
                        }
                        Err(err) => return Poll::Ready(Err(err)),
                    }
                    progressed = true;
                }
            }
            if let Some(read) = &mut this.read {
                // SAFETY: as for the write.
                let read = unsafe { Pin::new_unchecked(read) };
                if let Poll::Ready((read, buf)) = read.poll(cx) {
                    this.read = None;
                    match read {
                        Ok(0) => {
                            this.ended = true;
                            this.free(buf);
                        }
                        Ok(_) => this.filled = Some(buf),
                        Err(err) => return Poll::Ready(Err(err)),
                    }
                    progressed = true;
                }
            }
            if !progressed {
                return Poll::Pending;
            }
        }
    }
}

impl<R: AsyncRead + ?Sized, W: AsyncWrite + ?Sized> fmt::Debug for Copy<'_, R, W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Copy")
            .field("copied", &self.copied)
            .field("ended", &self.ended)
            .finish_non_exhaustive()
    }
}
