//! A channel for many values, from any number of senders to one receiver,
//! which holds as many as are sent, all on one thread.
//!
//! The values wait in a deque, which grows as they are sent. Nothing here
//! is atomic: the halves share the channel by `Rc`, and keep its state in
//! cells. Nobody waits but the receiver, for a value: a send puts its
//! value at the back and wakes it.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::fmt;
use std::future::poll_fn;
use std::mem;
use std::rc::Rc;
use std::task::{Context, Poll, ready};

use super::{SendError, TryRecvError};
use crate::coop;
use crate::sync::local::WakerCell;

/// A channel that holds as many values as are sent: sending never waits.
///
/// ```
/// use std::rc::Rc;
///
/// use ruio_executor::sync::local::mpsc::unbounded_channel;
///
/// # ruio_executor::Runtime::new()?.block_on(async {
/// let (tx, mut rx) = unbounded_channel();
/// let producer = ruio_executor::spawn(async move {
///     for value in 0..3 {
///         tx.send(Rc::new(value)).unwrap();
///     }
/// });
/// let mut sum = 0;
/// while let Some(value) = rx.recv().await {
///     sum += *value;
/// }
/// assert_eq!(sum, 3);
/// producer.await.unwrap();
/// # });
/// # Ok::<(), std::io::Error>(())
/// ```
pub fn unbounded_channel<T>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let chan = Rc::new(Chan {
        values: RefCell::new(VecDeque::new()),
        receiver_waker: WakerCell::new(),
        senders: Cell::new(1),
        closed: Cell::new(false),
    });
    (
        UnboundedSender {
            chan: Rc::clone(&chan),
        },
        UnboundedReceiver { chan, ended: false },
    )
}

/// What the halves of a channel share.
struct Chan<T> {
    /// The values sent and not yet taken, oldest first.
    values: RefCell<VecDeque<T>>,
    /// The receiver's task, waiting for a value.
    receiver_waker: WakerCell,
    /// How many senders there are.
    senders: Cell<usize>,
    /// Whether the receiver is gone, or has closed the channel.
    closed: Cell<bool>,
}

impl<T> Chan<T> {
    /// Takes the value at the front, if there is one.
    fn pop(&self) -> Option<T> {
        self.values.borrow_mut().pop_front()
    }
}

/// Sends values into the channel, without waiting. Clones send into the
/// same channel; once every one is dropped, the receiver sees the channel
/// end.
pub struct UnboundedSender<T> {
    chan: Rc<Chan<T>>,
}

impl<T> UnboundedSender<T> {
    /// Sends `value`, or hands it back if the receiver is gone or has closed
    /// the channel. It never waits: the channel grows to hold whatever is
    /// sent.
    ///
    /// # Errors
    ///
    /// [`SendError`] with the value, if the receiver is gone or has closed
    /// the channel.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        if self.chan.closed.get() {
            return Err(SendError(value));
        }
        self.chan.values.borrow_mut().push_back(value);
        self.chan.receiver_waker.wake();
        Ok(())
    }

    /// Whether the receiver is gone or has closed the channel, so that
    /// sending is in vain.
    pub fn is_closed(&self) -> bool {
        self.chan.closed.get()
    }
}

impl<T> Clone for UnboundedSender<T> {
    fn clone(&self) -> Self {
        self.chan.senders.set(self.chan.senders.get() + 1);
        Self {
            chan: Rc::clone(&self.chan),
        }
    }
}

impl<T> Drop for UnboundedSender<T> {
    fn drop(&mut self) {
        let senders = self.chan.senders.get() - 1;
        self.chan.senders.set(senders);
        if senders == 0 {
            self.chan.receiver_waker.wake();
        }
    }
}

impl<T> fmt::Debug for UnboundedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnboundedSender").finish_non_exhaustive()
    }
}

/// Receives the channel's values, in the order they were sent.
pub struct UnboundedReceiver<T> {
    chan: Rc<Chan<T>>,
    /// Whether a receive has found the channel ended: every sender gone, or
    /// the channel closed, and every value taken, so that receives give
    /// `None` from now on.
    ended: bool,
}

impl<T> UnboundedReceiver<T> {
    /// The next value, waiting for one, or `None` once the channel has
    /// ended: every sender is gone, or the receiver closed it, and every
    /// value sent before has been taken.
    pub async fn recv(&mut self) -> Option<T> {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    /// The next value, or `None` once the channel has ended, as for
    /// [`recv`](Self::recv); arranges for the task to be woken otherwise,
    /// or, once its budget is spent, wakes it at once.
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let proceed = ready!(coop::poll_proceed(cx));
        let value = match self.take(Some(cx)) {
            Ok(value) => Some(value),
            Err(TryRecvError::Closed) => None,
            Err(TryRecvError::Empty) => return Poll::Pending,
        };
        proceed.made_progress();
        Poll::Ready(value)
    }

    /// The next value if there is one, without waiting.
    ///
    /// # Errors
    ///
    /// [`TryRecvError::Empty`] if the channel is empty, and
    /// [`TryRecvError::Closed`] if it has ended as well: every sender is
    /// gone, or the receiver closed it.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.take(None)
    }

    /// Closes the channel: a send from now on fails and hands its value
    /// back, while every value sent before stays to be taken, after which
    /// [`recv`](Self::recv) gives `None`, senders or no senders.
    pub fn close(&mut self) {
        self.chan.closed.set(true);
    }

    /// Takes the next value; if there is none, and a context is given,
    /// registers its waker and looks again, so that no send is missed.
    fn take(&mut self, cx: Option<&Context<'_>>) -> Result<T, TryRecvError> {
        if let Some(value) = self.chan.pop() {
            return Ok(value);
        }
        if let Some(cx) = cx {
            self.chan.receiver_waker.register(cx.waker());
            if let Some(value) = self.chan.pop() {
                return Ok(value);
            }
        }
        if self.chan.senders.get() == 0 || self.chan.closed.get() {
            return Err(self.end());
        }
        Err(TryRecvError::Empty)
    }

    /// Notes that the channel has ended, which it stays.
    fn end(&mut self) -> TryRecvError {
        self.ended = true;
        TryRecvError::Closed
    }

    /// Whether a receive has found the channel ended.
    #[cfg(feature = "futures")]
    pub(crate) fn is_ended(&self) -> bool {
        self.ended
    }
}

impl<T> Drop for UnboundedReceiver<T> {
    fn drop(&mut self) {
        self.chan.closed.set(true);
        let values = mem::take(&mut *self.chan.values.borrow_mut());
        // Dropped outside any borrow: a value's drop can run any code.
        drop(values);
    }
}

impl<T> fmt::Debug for UnboundedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnboundedReceiver")
            .field("ended", &self.ended)
            .finish_non_exhaustive()
    }
}
