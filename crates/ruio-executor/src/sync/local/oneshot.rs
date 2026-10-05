//! A channel for one value, on one thread.
//!
//! Its halves share a cell for the value, a flag for each of them saying
//! it has finished, and the receiver's waker. The sender finishes by
//! sending or by going, and wakes the receiver if it waits; the receiver
//! takes a value it finds sent, and the one that finds the other gone
//! keeps the value.

use std::cell::Cell;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, ready};

use super::WakerCell;
use crate::coop;

/// A channel for one value: the sender sends it, the receiver awaits it.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Rc::new(Inner {
        complete: Cell::new(false),
        rx_closed: Cell::new(false),
        value: Cell::new(None),
        waker: WakerCell::new(),
    });
    (
        Sender {
            inner: Some(Rc::clone(&inner)),
        },
        Receiver { inner },
    )
}

struct Inner<T> {
    /// Whether the sender has finished: sent the value, or gone without.
    complete: Cell<bool>,
    /// Whether the receiver is gone.
    rx_closed: Cell<bool>,
    /// Written by the sender before `complete`; then the receiver's.
    value: Cell<Option<T>>,
    /// The task awaiting the value.
    waker: WakerCell,
}

impl<T> Inner<T> {
    /// Finishes the sender's part, the value written or not, and wakes the
    /// receiver if it waits.
    fn complete(&self) {
        self.complete.set(true);
        self.waker.wake();
    }
}

/// Sends the channel's value. Dropping it without sending closes the
/// channel.
pub struct Sender<T> {
    /// Taken as it sends.
    inner: Option<Rc<Inner<T>>>,
}

impl<T> Sender<T> {
    /// Sends `value`, or hands it back if the receiver is gone.
    pub fn send(mut self, value: T) -> Result<(), T> {
        let inner = self.inner.take().expect("only taken here");
        if inner.rx_closed.get() {
            return Err(value);
        }
        inner.value.set(Some(value));
        inner.complete();
        Ok(())
    }

    /// Whether the receiver is gone, so that sending is in vain.
    pub fn is_closed(&self) -> bool {
        let inner = self.inner.as_ref().expect("only taken by `send`");
        inner.rx_closed.get()
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
    inner: Rc<Inner<T>>,
}

impl<T> Receiver<T> {
    /// The value if it has been sent, without waiting.
    ///
    /// # Errors
    ///
    /// [`TryRecvError::Empty`] if it has not been sent yet, and
    /// [`TryRecvError::Closed`] if it never will be.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        if !self.inner.complete.get() {
            return Err(TryRecvError::Empty);
        }
        self.inner.value.take().ok_or(TryRecvError::Closed)
    }
}

impl<T> Future for Receiver<T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let proceed = ready!(coop::poll_proceed(cx));
        let inner = &*this.inner;
        if !inner.complete.get() {
            inner.waker.register(cx.waker());
            // Again, now that the sender wakes this as it finishes: it may
            // have finished as the waker was cloned.
            if !inner.complete.get() {
                return Poll::Pending;
            }
        }
        proceed.made_progress();
        Poll::Ready(inner.value.take().ok_or(RecvError(())))
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.inner.rx_closed.set(true);
        // Dropped outside any borrow: a value's drop can run any code.
        drop(self.inner.value.take());
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
