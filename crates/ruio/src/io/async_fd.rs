//! Readiness and I/O for any descriptor: a timer, an inotify instance, a
//! device, a socket some other library owns, wrapped so that a task can wait
//! for it on the ring.

use std::cell::OnceCell;
use std::fmt;
use std::future::{Future, poll_fn};
use std::io;
use std::marker::PhantomData;
use std::os::fd::{AsRawFd, RawFd};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use ruio_driver::buf::{IoBuf, IoBufMut};
use ruio_driver::ops::{PollAdd, PollMulti, PollStream, Read, Write};
use ruio_driver::{Driver, Fd, Op, Operation};

use super::{AsyncRead, AsyncWrite};

/// A descriptor of any kind, wrapped so that a task can wait for it to be
/// ready and read and write it on the ring: a timer, an inotify instance, a
/// device, a socket another library owns and drives with calls of its own.
///
/// [`readable`](AsyncFd::readable), [`writable`](AsyncFd::writable) and
/// [`ready`](AsyncFd::ready) each wait for the descriptor once, with one
/// poll request, after which the owner makes its own call, which must not
/// block: a call that would is made again after the next wait, as with
/// `poll(2)`. [`readiness`](AsyncFd::readiness) reports the descriptor's
/// readiness again and again on one request, for a task that waits for it
/// for ever. [`AsyncRead`] and [`AsyncWrite`] read and write the descriptor
/// through the ring, which waits for it itself, so a byte-oriented
/// descriptor, a pipe or a serial line, needs no waiting of its own.
///
/// The wrapper keeps the descriptor's owner, which [`get_ref`](AsyncFd::get_ref)
/// and [`into_inner`](AsyncFd::into_inner) give back; an operation borrows
/// the wrapper, so the descriptor stays open until the kernel has it. Like
/// the ring its operations run on, it belongs to its thread.
///
/// ```no_run
/// use std::os::fd::OwnedFd;
///
/// use ruio::io::AsyncFd;
///
/// # ruio::Runtime::new().unwrap().block_on(async {
/// # let timer: OwnedFd = todo!();
/// // A timerfd, armed elsewhere: each expiry makes it readable, and a read
/// // of eight bytes takes the count.
/// let timer = AsyncFd::new(timer);
/// loop {
///     timer.readable().await?;
///     let (read, count) = ruio::io::AsyncRead::read(&timer, Vec::with_capacity(8)).await;
///     read?;
///     println!("{} expiries", u64::from_ne_bytes(count[..8].try_into().unwrap()));
/// }
/// # Ok::<(), std::io::Error>(())
/// # }).unwrap();
/// ```
pub struct AsyncFd<T: AsRawFd> {
    inner: T,
    /// The thread's ring, found at the first operation and kept, so that
    /// the wrapper can be made outside a runtime and used inside one.
    driver: OnceCell<Driver>,
}

impl<T: AsRawFd> AsyncFd<T> {
    /// Wraps `inner`, whose descriptor the operations act on.
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            driver: OnceCell::new(),
        }
    }

    /// The descriptor's owner.
    #[inline]
    pub fn get_ref(&self) -> &T {
        &self.inner
    }

    /// The descriptor's owner, to change.
    #[inline]
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    /// The descriptor's owner, back.
    pub fn into_inner(self) -> T {
        self.inner
    }

    #[inline]
    fn driver(&self) -> &Driver {
        self.driver.get_or_init(crate::driver)
    }

    #[inline]
    fn fd(&self) -> Fd {
        Fd::Plain(self.inner.as_raw_fd())
    }

    /// Runs `operation` on the thread's ring, borrowing the wrapper.
    #[inline]
    fn op<O: Operation>(&self, operation: O) -> FdOp<'_, O> {
        FdOp {
            op: self.driver().op(operation),
            _fd: PhantomData,
        }
    }

    /// Waits until the descriptor is readable (`POLLIN`); see
    /// [`ready`](AsyncFd::ready).
    pub fn readable(&self) -> Ready<'_> {
        self.ready(libc::POLLIN.cast_unsigned().into())
    }

    /// Waits until the descriptor is writable (`POLLOUT`); see
    /// [`ready`](AsyncFd::ready).
    pub fn writable(&self) -> Ready<'_> {
        self.ready(libc::POLLOUT.cast_unsigned().into())
    }

    /// Waits until the descriptor has any of `events`, a mask of
    /// `libc::POLLIN`, `libc::POLLOUT` and the other `POLL*` events, with one
    /// poll request; the output is the events it has. A regular file is
    /// always ready.
    pub fn ready(&self, events: u32) -> Ready<'_> {
        self.op(PollAdd::new(self.fd(), events))
    }

    /// The descriptor's readiness for `events`, reported each time it is
    /// woken for them, on one request kept armed; see [`Readiness`].
    pub fn readiness(&self, events: u32) -> Readiness<'_> {
        Readiness {
            stream: PollStream::new(self.driver(), PollMulti::new(self.fd(), events)),
            _fd: PhantomData,
        }
    }
}

impl<T: AsRawFd> AsRawFd for AsyncFd<T> {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

impl<T: AsRawFd + fmt::Debug> fmt::Debug for AsyncFd<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AsyncFd")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

/// Reads the descriptor through the ring, which waits for it to be readable
/// itself, at its position if it has one.
impl<T: AsRawFd> AsyncRead for AsyncFd<T> {
    type Read<'a, B: IoBufMut>
        = FdOp<'a, Read<B>>
    where
        Self: 'a;

    #[inline]
    fn read<B: IoBufMut>(&self, buf: B) -> Self::Read<'_, B> {
        self.op(Read::new(self.fd(), buf))
    }
}

/// Writes the descriptor through the ring, which waits for it to be writable
/// itself, at its position if it has one.
impl<T: AsRawFd> AsyncWrite for AsyncFd<T> {
    type Write<'a, B: IoBuf>
        = FdOp<'a, Write<B>>
    where
        Self: 'a;

    #[inline]
    fn write<B: IoBuf>(&self, buf: B) -> Self::Write<'_, B> {
        self.op(Write::new(self.fd(), buf))
    }
}

/// An operation on an [`AsyncFd`]'s descriptor, which it borrows so that the
/// descriptor stays open until the kernel has the operation; its output is
/// the operation's.
#[must_use = "an operation does nothing until polled"]
pub struct FdOp<'a, T: Operation> {
    op: Op<T>,
    _fd: PhantomData<&'a ()>,
}

impl<T: Operation> FdOp<'_, T> {
    /// Cancels the operation if the kernel has not completed it `duration`
    /// after it is submitted, with a linked timeout; see [`Op::timeout`].
    /// The operation then fails with [`io::ErrorKind::TimedOut`].
    pub fn timeout(self, duration: Duration) -> Self {
        Self {
            op: self.op.timeout(duration),
            ..self
        }
    }
}

impl<T: Operation> Future for FdOp<'_, T> {
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T::Output> {
        Pin::new(&mut self.get_mut().op).poll(cx)
    }
}

impl<T: Operation> fmt::Debug for FdOp<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FdOp").field("op", &self.op).finish()
    }
}

/// A wait for an [`AsyncFd`]'s descriptor to be ready, made by
/// [`AsyncFd::ready`] and its kin: the output is the events the descriptor
/// has, among those asked for.
pub type Ready<'a> = FdOp<'a, PollAdd>;

/// An [`AsyncFd`]'s readiness, reported each time its descriptor is woken
/// for the events asked for, by one multishot poll the stream keeps armed;
/// made by [`AsyncFd::readiness`]. Each item is the events the descriptor
/// has, or the error that ended the request, after which the request is
/// armed again. The stream never ends.
///
/// An item comes for each wakeup rather than for as long as the descriptor
/// stays ready: a task that takes some of what the descriptor has and waits
/// for the next item waits for the next arrival, so it should take
/// everything there is, until a call would block, as with edge-triggered
/// readiness.
#[must_use = "a stream does nothing until polled"]
pub struct Readiness<'a> {
    stream: PollStream,
    _fd: PhantomData<&'a ()>,
}

impl Readiness<'_> {
    /// The events the descriptor was woken for.
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<u32>> {
        self.stream.poll_next(cx)
    }

    /// The next events; see [`poll_next`](Readiness::poll_next).
    pub async fn next(&mut self) -> io::Result<u32> {
        poll_fn(|cx| self.poll_next(cx)).await
    }

    /// The next events if they have been reported already, or `None`,
    /// without waiting.
    pub fn next_ready(&mut self) -> Option<io::Result<u32>> {
        self.stream.next_ready()
    }
}

impl fmt::Debug for Readiness<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Readiness")
            .field("stream", &self.stream)
            .finish()
    }
}
