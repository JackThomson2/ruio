//! A channel for one value.
//!
//! Its halves meet in one atomic state, in which the sender finishes, by
//! sending or by going, the receiver registers its waker, and the receiver
//! goes; each is one read-modify-write, and whichever comes second does
//! what is left to do: the sender wakes a registered receiver, the
//! receiver takes a value it finds sent, and the one that finds the other
//! gone keeps the value.

use std::cell::UnsafeCell;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker, ready};

use crate::coop;

/// The sender has finished: sent the value, or gone without.
const COMPLETE: usize = 1;
/// The receiver's waker is registered, to be woken as the sender finishes.
const RX_TASK: usize = 2;
/// The receiver is gone.
const RX_CLOSED: usize = 4;

/// A channel for one value: the sender sends it, the receiver awaits it.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Inner {
        state: AtomicUsize::new(0),
        value: UnsafeCell::new(None),
        waker: UnsafeCell::new(None),
    });
    (
        Sender {
            inner: Some(Arc::clone(&inner)),
        },
        Receiver { inner },
    )
}

struct Inner<T> {
    state: AtomicUsize,
    /// Written by the sender before `COMPLETE`; then the receiver's, unless
    /// the sender found `RX_CLOSED`, which makes it the sender's again.
    value: UnsafeCell<Option<T>>,
    /// The task awaiting the value: written by the receiver only while
    /// `RX_TASK` is clear and `COMPLETE` is not set, read by the sender
    /// once it set `COMPLETE` and found `RX_TASK`.
    waker: UnsafeCell<Option<Waker>>,
}

// SAFETY: the value and the waker are each reached by one half at a time,
// as the state hands them over, so the value only moves between threads.
unsafe impl<T: Send> Send for Inner<T> {}
// SAFETY: as above.
unsafe impl<T: Send> Sync for Inner<T> {}

impl<T> Inner<T> {
    /// Finishes the sender's part, the value written or not, and wakes the
    /// receiver if it waits: the state as it was.
    fn complete(&self) -> usize {
        // Release: the receiver that sees `COMPLETE` sees the value.
        // Acquire: the waker read is the one registered.
        let state = self.state.fetch_or(COMPLETE, Ordering::AcqRel);
        if state & (RX_TASK | RX_CLOSED) == RX_TASK {
            // SAFETY: with `COMPLETE` set the receiver no longer writes the
            // waker, and `RX_TASK` says it wrote one.
            let waker = unsafe { &*self.waker.get() };
            if let Some(waker) = waker {
                waker.wake_by_ref();
            }
        }
        state
    }
}

/// Sends the channel's value. Dropping it without sending closes the
/// channel.
pub struct Sender<T> {
    /// Taken as it sends.
    inner: Option<Arc<Inner<T>>>,
}

impl<T> Sender<T> {
    /// Sends `value`, or hands it back if the receiver is gone.
    pub fn send(mut self, value: T) -> Result<(), T> {
        let inner = self.inner.take().expect("only taken here");
        // Acquire: as below, for a receiver gone before.
        if inner.state.load(Ordering::Acquire) & RX_CLOSED != 0 {
            return Err(value);
        }
        // SAFETY: until `COMPLETE` the value is the sender's alone.
        unsafe { *inner.value.get() = Some(value) };
        if inner.complete() & RX_CLOSED != 0 {
            // SAFETY: the receiver went before `COMPLETE` was set, and so
            // never takes the value: it is the sender's again.
            let value = unsafe { (*inner.value.get()).take() };
            return Err(value.expect("just written"));
        }
        Ok(())
    }

    /// Whether the receiver is gone, so that sending is in vain.
    pub fn is_closed(&self) -> bool {
        let inner = self.inner.as_ref().expect("only taken by `send`");
        inner.state.load(Ordering::Acquire) & RX_CLOSED != 0
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if let Some(inner) = &self.inner {
            inner.complete();
        }
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender").finish_non_exhaustive()
    }
}

/// Awaits the channel's value. The output is the value, or [`RecvError`]
/// if the sender was dropped without sending.
pub struct Receiver<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Receiver<T> {
    /// The value if it has been sent, without waiting.
    ///
    /// # Errors
    ///
    /// [`TryRecvError::Empty`] if it has not been sent yet, and
    /// [`TryRecvError::Closed`] if it never will be.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        // Acquire: the value sent before `COMPLETE` is seen.
        if self.inner.state.load(Ordering::Acquire) & COMPLETE == 0 {
            return Err(TryRecvError::Empty);
        }
        self.take().ok_or(TryRecvError::Closed)
    }

    /// Takes the value once the sender has finished.
    fn take(&mut self) -> Option<T> {
        // SAFETY: the caller saw `COMPLETE`, after which only the receiver
        // touches the value.
        unsafe { (*self.inner.value.get()).take() }
    }
}

impl<T> Future for Receiver<T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let proceed = ready!(coop::poll_proceed(cx));
        let inner = &*this.inner;
        // Acquire: the value sent before `COMPLETE` is seen, and the waker
        // this registered before is the one in the slot.
        let mut state = inner.state.load(Ordering::Acquire);
        if state & COMPLETE == 0 && state & RX_TASK != 0 {
            // SAFETY: the receiver alone writes the waker, so reading it
            // races only with the sender's reading.
            let waker = unsafe { &*inner.waker.get() };
            if waker.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
                return Poll::Pending;
            }
            // Acquire, as above. Once `RX_TASK` is clear before the sender
            // finishes, the sender leaves the waker alone.
            state = inner.state.fetch_and(!RX_TASK, Ordering::AcqRel);
        }
        if state & COMPLETE == 0 {
            // SAFETY: `RX_TASK` is clear and the sender has not finished, so
            // it does not read the waker until this sets `RX_TASK` again.
            let old = unsafe { (*inner.waker.get()).replace(cx.waker().clone()) };
            // Release: a sender that finds `RX_TASK` reads the new waker.
            // Acquire: as above.
            state = inner.state.fetch_or(RX_TASK, Ordering::AcqRel);
            // Dropped once the state is settled: dropping a waker can run
            // any code.
            drop(old);
            if state & COMPLETE == 0 {
                return Poll::Pending;
            }
        }
        proceed.made_progress();
        Poll::Ready(this.take().ok_or(RecvError(())))
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        // Acquire: a value sent before `COMPLETE` is seen, to be dropped.
        // Release, for the sender, which then hands its value back.
        let state = self.inner.state.fetch_or(RX_CLOSED, Ordering::AcqRel);
        if state & COMPLETE != 0 {
            // Dropped outside any lock: a value's drop can run any code.
            drop(self.take());
        }
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver").finish_non_exhaustive()
    }
}

/// The sender was dropped without sending.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecvError(());

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the sender was dropped without sending")
    }
}

impl Error for RecvError {}

/// Why [`Receiver::try_recv`] has no value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TryRecvError {
    /// The value has not been sent yet.
    Empty,
    /// The sender was dropped without sending.
    Closed,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TryRecvError::Empty => "the value has not been sent yet",
            TryRecvError::Closed => "the sender was dropped without sending",
        })
    }
}

impl Error for TryRecvError {}
