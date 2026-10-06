//! A channel for many values, from any number of senders to one receiver,
//! which holds a bounded number of them, all on one thread.
//!
//! The values wait in a deque, with room for all of them from the start.
//! Nothing here is atomic: the halves share the channel by `Rc`, and keep
//! its state in cells. A sender that finds the channel full waits in a
//! list of the tasks wanting room, each kept there, with a flag saying
//! whether it waits now, until the next wake finds it not waiting; the
//! receiver, as it takes a value, wakes every one that waits, as one
//! woken alone could have been dropped, and waste the wake.
//!
//! [`unbounded_channel`] makes a channel with no bound, whose senders never
//! wait: its values wait in a deque that grows as they are sent, and its
//! halves hand out the same errors as this one's.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::future::{Future, poll_fn};
use std::mem;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker, ready};

use super::WakerCell;
use crate::coop;

mod unbounded;

pub use unbounded::{UnboundedReceiver, UnboundedSender, unbounded_channel};

/// A channel that holds up to `capacity` values: sending waits while it is
/// full. Room for all of them is allocated at once.
///
/// # Panics
///
/// If `capacity` is zero, or too large to allocate.
pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity > 0, "a channel holds at least one value");
    let chan = Rc::new(Chan {
        values: RefCell::new(VecDeque::with_capacity(capacity)),
        receiver_waker: WakerCell::new(),
        senders: Cell::new(1),
        closed: Cell::new(false),
        room_wanted: Cell::new(false),
        sender_wakers: RefCell::new(Vec::new()),
        capacity,
    });
    (
        Sender {
            chan: Rc::clone(&chan),
        },
        Receiver { chan, ended: false },
    )
}

/// What the halves of a channel share.
struct Chan<T> {
    /// The values sent and not yet taken, oldest first; never more than
    /// `capacity`.
    values: RefCell<VecDeque<T>>,
    /// The receiver's task, waiting for a value.
    receiver_waker: WakerCell,
    /// How many senders there are.
    senders: Cell<usize>,
    /// Whether the receiver is gone.
    closed: Cell<bool>,
    /// Whether a sender in `sender_wakers` waits for room.
    room_wanted: Cell<bool>,
    /// Senders' tasks that waited for room.
    sender_wakers: RefCell<Vec<Waiter>>,
    capacity: usize,
}

/// A sender's task in the list of those that waited for room.
struct Waiter {
    waker: Waker,
    /// Whether the task waits for room now. Set as it registers, cleared
    /// as it is woken; a task found not waiting by a wake left the list
    /// without a word, as a task that got its room or was dropped does,
    /// and is taken off it.
    waiting: bool,
}

/// Why a value could not go in.
enum PushError<T> {
    Full(T),
    Closed(T),
}

impl<T> Chan<T> {
    /// Puts `value` at the back, and wakes the receiver.
    fn push(&self, value: T) -> Result<(), PushError<T>> {
        if self.closed.get() {
            return Err(PushError::Closed(value));
        }
        {
            let mut values = self.values.borrow_mut();
            if values.len() == self.capacity {
                return Err(PushError::Full(value));
            }
            values.push_back(value);
        }
        self.receiver_waker.wake();
        Ok(())
    }

    /// Takes the value at the front, if there is one, and wakes the senders
    /// waiting for the room that makes.
    fn pop(&self) -> Option<T> {
        let value = self.values.borrow_mut().pop_front()?;
        if self.room_wanted.get() {
            self.wake_senders();
        }
        Some(value)
    }

    /// Asks for the task of `cx` to be woken when room is made.
    fn want_room(&self, cx: &Context<'_>) {
        let registered = {
            let mut waiters = self.sender_wakers.borrow_mut();
            match waiters
                .iter_mut()
                .find(|waiter| waiter.waker.will_wake(cx.waker()))
            {
                Some(waiter) => {
                    waiter.waiting = true;
                    true
                }
                None => false,
            }
        };
        if !registered {
            // Cloned outside the borrow: cloning a waker can run any code.
            let waker = cx.waker().clone();
            self.sender_wakers.borrow_mut().push(Waiter {
                waker,
                waiting: true,
            });
        }
        self.room_wanted.set(true);
    }

    /// Wakes every sender waiting for room, and takes off the list the
    /// ones that were not. The list is taken out for the wakes, which can
    /// run any code, and the tasks that registered meanwhile are kept
    /// with it.
    fn wake_senders(&self) {
        self.room_wanted.set(false);
        let mut waiters = mem::take(&mut *self.sender_wakers.borrow_mut());
        waiters.retain_mut(|waiter| {
            if !mem::replace(&mut waiter.waiting, false) {
                return false;
            }
            waiter.waker.wake_by_ref();
            true
        });
        let mut registered = self.sender_wakers.borrow_mut();
        waiters.append(&mut registered);
        *registered = waiters;
    }
}

/// Sends values into the channel. Clones send into the same channel; once
/// every one is dropped, the receiver sees the channel end.
pub struct Sender<T> {
    chan: Rc<Chan<T>>,
}

impl<T> Sender<T> {
    /// Sends `value`, waiting while the channel is full; see [`Sending`].
    pub fn send(&self, value: T) -> Sending<'_, T> {
        Sending {
            sender: self,
            value: Some(value),
        }
    }

    /// Sends `value` if the channel has room, or hands it back.
    ///
    /// # Errors
    ///
    /// [`TrySendError::Full`] if the channel is full, and
    /// [`TrySendError::Closed`] if the receiver is gone.
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        self.chan.push(value).map_err(|err| match err {
            PushError::Full(value) => TrySendError::Full(value),
            PushError::Closed(value) => TrySendError::Closed(value),
        })
    }

    /// Whether the receiver is gone, so that sending is in vain.
    pub fn is_closed(&self) -> bool {
        self.chan.closed.get()
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.chan.senders.set(self.chan.senders.get() + 1);
        Self {
            chan: Rc::clone(&self.chan),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let senders = self.chan.senders.get() - 1;
        self.chan.senders.set(senders);
        if senders == 0 {
            self.chan.receiver_waker.wake();
        }
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender").finish_non_exhaustive()
    }
}

/// Sends a value into a channel, waiting while it is full. Made by
/// [`Sender::send`]; the output hands the value back if the receiver is
/// gone.
#[must_use = "a send does nothing until polled"]
pub struct Sending<'a, T> {
    sender: &'a Sender<T>,
    /// Taken once sent or handed back.
    value: Option<T>,
}

impl<T> Future for Sending<'_, T> {
    type Output = Result<(), SendError<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        assert!(this.value.is_some(), "`Sending` polled after it completed");
        let proceed = ready!(coop::poll_proceed(cx));
        let chan = &*this.sender.chan;
        let value = this.value.take().expect("checked above");
        let value = match chan.push(value) {
            Ok(()) => {
                proceed.made_progress();
                return Poll::Ready(Ok(()));
            }
            Err(PushError::Closed(value)) => {
                proceed.made_progress();
                return Poll::Ready(Err(SendError(value)));
            }
            Err(PushError::Full(value)) => value,
        };
        chan.want_room(cx);
        // Again, now that the receiver wakes this if it makes room: it may
        // have made some as the waker was cloned.
        match chan.push(value) {
            Ok(()) => {
                proceed.made_progress();
                Poll::Ready(Ok(()))
            }
            Err(PushError::Closed(value)) => {
                proceed.made_progress();
                Poll::Ready(Err(SendError(value)))
            }
            Err(PushError::Full(value)) => {
                this.value = Some(value);
                Poll::Pending
            }
        }
    }
}

// Nothing is pinned: the value is only ever moved.
impl<T> Unpin for Sending<'_, T> {}

impl<T> fmt::Debug for Sending<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sending").finish_non_exhaustive()
    }
}

/// Receives the channel's values, in the order they were sent.
pub struct Receiver<T> {
    chan: Rc<Chan<T>>,
    /// Whether a receive has found the channel ended: every sender gone and
    /// every value taken, so that receives give `None` from now on.
    ended: bool,
}

impl<T> Receiver<T> {
    /// The next value, waiting for one, or `None` once every sender is gone
    /// and the channel is empty.
    pub async fn recv(&mut self) -> Option<T> {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    /// The next value, or `None` once every sender is gone and the channel
    /// is empty; arranges for the task to be woken otherwise, or, once its
    /// budget is spent, wakes it at once.
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
    /// [`TryRecvError::Closed`] if every sender is gone as well.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.take(None)
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
        if self.chan.senders.get() == 0 {
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

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.chan.closed.set(true);
        let waiters = mem::take(&mut *self.chan.sender_wakers.borrow_mut());
        let values = mem::take(&mut *self.chan.values.borrow_mut());
        // Dropped outside any borrow: a value's drop can run any code.
        drop(values);
        for waiter in &waiters {
            if waiter.waiting {
                waiter.waker.wake_by_ref();
            }
        }
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver")
            .field("ended", &self.ended)
            .finish_non_exhaustive()
    }
}

/// The receiver is gone; the value comes back.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendError").finish_non_exhaustive()
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the receiver is gone")
    }
}

impl<T> Error for SendError<T> {}

/// Why [`Sender::try_send`] could not send; the value comes back.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TrySendError<T> {
    /// The channel is full.
    Full(T),
    /// The receiver is gone.
    Closed(T),
}

impl<T> fmt::Debug for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TrySendError::Full(_) => "Full(..)",
            TrySendError::Closed(_) => "Closed(..)",
        })
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TrySendError::Full(_) => "the channel is full",
            TrySendError::Closed(_) => "the receiver is gone",
        })
    }
}

impl<T> Error for TrySendError<T> {}

/// Why [`Receiver::try_recv`] has no value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TryRecvError {
    /// The channel is empty.
    Empty,
    /// The channel is empty, and every sender is gone.
    Closed,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TryRecvError::Empty => "the channel is empty",
            TryRecvError::Closed => "the channel is empty, and every sender is gone",
        })
    }
}

impl Error for TryRecvError {}
