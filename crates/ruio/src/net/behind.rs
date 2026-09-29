//! Writes that do not wait for the kernel: a stream's `write_behind`.

use std::cell::{Cell, RefCell};
use std::fmt;
use std::future::Future;
use std::io;
use std::mem;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker, ready};
use std::time::Duration;

use ruio_driver::buf::IoBufs;
use ruio_driver::ops::SendVectored;
use ruio_driver::{Completion, Driver, Fd, Operation};
use ruio_sys as sys;

/// The writes behind a stream: whether one is in flight, and the error the
/// first to fail failed with.
///
/// Nobody waits for a write behind, so how it went can only be reported
/// later. A write that failed, or was cut short, leaves a gap in the
/// stream, so every write after it fails with its error, as a stream's
/// writes do once it has broken.
#[derive(Default)]
pub(crate) struct Behind {
    in_flight: Cell<bool>,
    /// The error, as an errno: the writes it fails report it as the kernel
    /// would have.
    error: Cell<Option<i32>>,
    /// Tasks waiting for the write in flight to finish.
    waiters: RefCell<Vec<Waker>>,
}

impl Behind {
    /// Ready once no write behind is in flight: `Ok`, unless one failed.
    pub(crate) fn poll_done(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.in_flight.get() {
            let mut waiters = self.waiters.borrow_mut();
            if !waiters.iter().any(|waiter| waiter.will_wake(cx.waker())) {
                waiters.push(cx.waker().clone());
            }
            return Poll::Pending;
        }
        Poll::Ready(match self.error.get() {
            None => Ok(()),
            Some(code) => Err(io::Error::from_raw_os_error(code)),
        })
    }

    /// Records how the write in flight went, failing with `error` if it is
    /// one, and wakes those waiting for it.
    fn finish(&self, error: Option<i32>) {
        if self.error.get().is_none() {
            self.error.set(error);
        }
        self.in_flight.set(false);
        let waiters = mem::take(&mut *self.waiters.borrow_mut());
        for waiter in waiters {
            waiter.wake();
        }
    }
}

impl fmt::Debug for Behind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Behind")
            .field("in_flight", &self.in_flight.get())
            .field("error", &self.error.get())
            .finish_non_exhaustive()
    }
}

/// A send left to finish behind a stream, which records how it went.
struct SendBehind<V> {
    send: SendVectored<V>,
    /// How many bytes it has to send.
    len: usize,
    /// Whether it has a linked timeout.
    timed: bool,
    behind: Rc<Behind>,
}

// SAFETY: as for the send it wraps, which it prepares and completes, and
// which posts one completion.
unsafe impl<V: IoBufs> Operation for SendBehind<V> {
    type Output = ();

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        self.send.prepare(sqe);
    }

    fn complete(self, completion: Completion) {
        let (sent, bufs) = self.send.complete(completion);
        drop(bufs);
        // Asked to carry on through short sends, the kernel stops short only
        // when the send is cancelled, by its timeout, or the connection
        // breaks; either way it reports the bytes sent rather than why.
        let error = match sent {
            Ok(sent) if sent == self.len => None,
            Ok(_) if self.timed => Some(libc::ETIMEDOUT),
            Ok(_) => Some(libc::EPIPE),
            Err(err) => Some(err.raw_os_error().unwrap_or(libc::EIO)),
        };
        self.behind.finish(error);
    }
}

/// Writes buffers to a stream socket without waiting for the kernel to be
/// done with them. Made by the streams' `write_behind`.
///
/// The send goes to the kernel as it is, asking it to carry on through
/// short sends, and the driver drops the buffers once the kernel is done
/// with them. The future resolves once the send is on its way: at once,
/// unless an earlier write behind is still in flight, as writes to a stream
/// go one at a time, in order. It fails with the error an earlier write
/// behind failed with, dropping the buffers unsent.
#[must_use = "a write does nothing until polled"]
pub struct WriteBehind<'a, V: IoBufs> {
    behind: &'a Rc<Behind>,
    driver: Driver,
    fd: Fd,
    /// Taken once the send is on its way.
    bufs: Option<V>,
    timeout: Option<Duration>,
}

impl<'a, V: IoBufs> WriteBehind<'a, V> {
    pub(crate) fn new(behind: &'a Rc<Behind>, (driver, fd): (Driver, Fd), bufs: V) -> Self {
        Self {
            behind,
            driver,
            fd,
            bufs: Some(bufs),
            timeout: None,
        }
    }

    /// Fails the send with [`io::ErrorKind::TimedOut`] if the kernel has not
    /// finished it `duration` after it is submitted, with a linked timeout.
    /// The writes after it then fail too, as for any error.
    pub fn timeout(mut self, duration: Duration) -> Self {
        self.timeout = Some(duration);
        self
    }
}

impl<V: IoBufs> Future for WriteBehind<'_, V> {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.behind.poll_done(cx))?;
        let bufs = this
            .bufs
            .take()
            .expect("`WriteBehind` polled after it completed");
        let len = bufs.bytes_init();
        if len == 0 {
            return Poll::Ready(Ok(()));
        }
        this.behind.in_flight.set(true);
        let send = SendBehind {
            send: SendVectored::new(this.fd, bufs).flags(libc::MSG_WAITALL),
            len,
            timed: this.timeout.is_some(),
            behind: Rc::clone(this.behind),
        };
        let mut op = this.driver.op(send);
        if let Some(duration) = this.timeout {
            op = op.timeout(duration);
        }
        op.detach();
        Poll::Ready(Ok(()))
    }
}

// Nothing is pinned: the buffers are only ever moved.
impl<V: IoBufs> Unpin for WriteBehind<'_, V> {}

impl<V: IoBufs> fmt::Debug for WriteBehind<'_, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteBehind")
            .field("behind", &self.behind)
            .finish_non_exhaustive()
    }
}

/// Waits for the writes behind a stream to finish. Made by the streams'
/// `flush`.
///
/// Resolves with the error a write behind failed with, if one did.
#[must_use = "a flush does nothing until polled"]
#[derive(Debug)]
pub struct Flush<'a> {
    behind: &'a Behind,
}

impl<'a> Flush<'a> {
    pub(crate) fn new(behind: &'a Behind) -> Self {
        Self { behind }
    }
}

impl Future for Flush<'_> {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.behind.poll_done(cx)
    }
}
