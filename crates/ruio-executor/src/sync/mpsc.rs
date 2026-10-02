//! A channel for many values, from any number of senders to one receiver,
//! which holds a bounded number of them.
//!
//! The values wait in a ring of slots, as in Vyukov's bounded queue: each
//! slot has a stamp saying for which turn round the ring it is free to
//! fill, or filled and free to take. A sender claims a slot by moving the
//! tail on past it, fills it and stamps it filled; the receiver, alone at
//! the head, takes the value and stamps the slot free for the next turn.
//! Neither takes a lock: only a sender that finds the channel full does,
//! to wait in a list for room.

use std::cell::UnsafeCell;
use std::error::Error;
use std::fmt;
use std::future::{Future, poll_fn};
use std::mem::{self, MaybeUninit};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker, ready};

use super::lock;
use super::waker_slot::WakerSlot;
use crate::coop;

/// A channel that holds up to `capacity` values: sending waits while it is
/// full. Room for all of them is allocated at once.
///
/// # Panics
///
/// If `capacity` is zero, or too large to allocate.
pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity > 0, "a channel holds at least one value");
    let one_lap = capacity
        .checked_add(1)
        .and_then(usize::checked_next_power_of_two)
        .filter(|&lap| lap <= usize::MAX / 4)
        .expect("a channel's capacity fits its positions");
    let slots = (0..capacity)
        .map(|index| Slot {
            stamp: AtomicUsize::new(index),
            value: UnsafeCell::new(MaybeUninit::uninit()),
        })
        .collect();
    let chan = Arc::new(Chan {
        tail: AtomicUsize::new(0),
        receiver_waker: WakerSlot::new(),
        senders: AtomicUsize::new(1),
        closed: AtomicBool::new(false),
        room_wanted: AtomicBool::new(false),
        sender_wakers: Mutex::new(Vec::new()),
        capacity,
        one_lap,
        slots,
    });
    (
        Sender {
            chan: Arc::clone(&chan),
        },
        Receiver { chan, head: 0 },
    )
}

/// What the halves of a channel share.
///
/// A position names a slot and a turn round the ring: the low bits, below
/// `one_lap`, are the slot's index, and the bits above count the turns, so
/// that positions only grow, wrapping round only after the counter has.
struct Chan<T> {
    /// The position the next sender fills.
    tail: AtomicUsize,
    /// The receiver's task, waiting for a value.
    receiver_waker: WakerSlot,
    /// How many senders there are.
    senders: AtomicUsize,
    /// Whether the receiver is gone.
    closed: AtomicBool,
    /// Whether a sender may be in `sender_wakers`; set under its lock,
    /// read by the receiver without it.
    room_wanted: AtomicBool,
    /// Senders' tasks, waiting for room.
    sender_wakers: Mutex<Vec<Waker>>,
    capacity: usize,
    /// The power of two above the largest index, by which positions count
    /// the turns round the ring.
    one_lap: usize,
    slots: Box<[Slot<T>]>,
}

struct Slot<T> {
    /// The position at which the slot is free to fill, or one past the
    /// position at which it was filled.
    stamp: AtomicUsize,
    /// Holds a value from when the stamp says it is filled until the
    /// receiver takes it.
    value: UnsafeCell<MaybeUninit<T>>,
}

// SAFETY: a value is written by the one sender that claimed its slot and
// read by the receiver once the stamp says it is there, so it only moves
// between threads; the rest is atomic or locked.
unsafe impl<T: Send> Send for Chan<T> {}
// SAFETY: as above.
unsafe impl<T: Send> Sync for Chan<T> {}

/// Why a value could not go in.
enum PushError<T> {
    Full(T),
    Closed(T),
}

impl<T> Chan<T> {
    /// The position after `position`, on the next turn after the last
    /// slot.
    #[inline]
    fn next(&self, position: usize) -> usize {
        let index = position & (self.one_lap - 1);
        if index + 1 < self.capacity {
            position + 1
        } else {
            (position & !(self.one_lap - 1)).wrapping_add(self.one_lap)
        }
    }

    /// Puts `value` in the next slot, and wakes the receiver.
    fn push(&self, value: T) -> Result<(), PushError<T>> {
        // Acquire: a receiver dropped before this saw nothing of the value.
        if self.closed.load(Ordering::Acquire) {
            return Err(PushError::Closed(value));
        }
        let mut tail = self.tail.load(Ordering::Relaxed);
        loop {
            let slot = &self.slots[tail & (self.one_lap - 1)];
            // Acquire: the receiver's taking of the value that was there
            // happened before this fills the slot again. Sequentially
            // consistent, with the store to `room_wanted` before a sender
            // looks again and the receiver's freeing and reading of it,
            // so that either this sees the slot free or the receiver sees
            // the sender waiting.
            let stamp = slot.stamp.load(Ordering::SeqCst);
            if stamp == tail {
                // Relaxed: the claim publishes nothing; the stamp does.
                match self.tail.compare_exchange_weak(
                    tail,
                    self.next(tail),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // SAFETY: the claim makes this the slot's one
                        // writer, until the stamp hands it to the receiver.
                        unsafe { (*slot.value.get()).write(value) };
                        // Release: the receiver that sees the stamp sees
                        // the value.
                        slot.stamp.store(tail.wrapping_add(1), Ordering::Release);
                        self.receiver_waker.wake();
                        return Ok(());
                    }
                    Err(actual) => tail = actual,
                }
            } else if (stamp.wrapping_sub(tail) as isize) < 0 {
                // The slot still holds, or is being given, the value of
                // the turn before: the channel is full.
                return Err(PushError::Full(value));
            } else {
                // Another sender filled the slot since the tail was read.
                tail = self.tail.load(Ordering::Relaxed);
            }
        }
    }

    /// Wakes every sender waiting for room: one woken alone could have been
    /// dropped, and waste the wake.
    fn wake_senders(&self) {
        let wakers = {
            let mut wakers = lock(&self.sender_wakers);
            // Sequentially consistent, as every access is: see `Chan::push`.
            self.room_wanted.store(false, Ordering::SeqCst);
            mem::take(&mut *wakers)
        };
        for waker in wakers {
            waker.wake();
        }
    }
}

impl<T> Drop for Chan<T> {
    fn drop(&mut self) {
        for (index, slot) in self.slots.iter_mut().enumerate() {
            if *slot.stamp.get_mut() & (self.one_lap - 1) == index + 1 {
                // SAFETY: the stamp says the slot holds a value, which
                // nothing took: the receiver is gone.
                unsafe { slot.value.get_mut().assume_init_drop() };
            }
        }
    }
}

/// Sends values into the channel. Clones send into the same channel; once
/// every one is dropped, the receiver sees the channel end.
pub struct Sender<T> {
    chan: Arc<Chan<T>>,
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
        self.chan.closed.load(Ordering::Acquire)
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        // Relaxed: this sender keeps the count above zero meanwhile.
        self.chan.senders.fetch_add(1, Ordering::Relaxed);
        Self {
            chan: Arc::clone(&self.chan),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        // Release: the receiver that sees the count reach zero sees every
        // value sent; acquire, so that the last sender's wake comes after
        // the others' values too.
        if self.chan.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
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
        {
            let mut wakers = lock(&chan.sender_wakers);
            if !wakers.iter().any(|waker| waker.will_wake(cx.waker())) {
                wakers.push(cx.waker().clone());
            }
            // Sequentially consistent, with the load of the stamp in the
            // push that follows: see `Chan::push`.
            chan.room_wanted.store(true, Ordering::SeqCst);
        }
        // Again, now that the receiver wakes this if it makes room.
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
    chan: Arc<Chan<T>>,
    /// The position of the next value to take.
    head: usize,
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
        if let Some(value) = self.pop() {
            return Ok(value);
        }
        if let Some(cx) = cx {
            // SAFETY: the receiver, borrowed mutably, is the slot's one
            // owner.
            unsafe { self.chan.receiver_waker.register(cx.waker()) };
            if let Some(value) = self.pop() {
                return Ok(value);
            }
        }
        // Acquire: with no sender left, every value sent is seen.
        if self.chan.senders.load(Ordering::Acquire) == 0 {
            return self.pop().ok_or(TryRecvError::Closed);
        }
        Err(TryRecvError::Empty)
    }

    /// Takes the value at the head, if it is there, and wakes the senders
    /// waiting for the room that makes.
    fn pop(&mut self) -> Option<T> {
        let chan = &*self.chan;
        let slot = &chan.slots[self.head & (chan.one_lap - 1)];
        // Acquire: the value the stamp says is there is seen.
        if slot.stamp.load(Ordering::Acquire) != self.head.wrapping_add(1) {
            return None;
        }
        // SAFETY: the stamp says the slot holds a value, and until the
        // stamp says otherwise only the receiver touches it.
        let cell = unsafe { &*slot.value.get() };
        // SAFETY: as above; the stamp, set free below, keeps the value from
        // being read twice.
        let value = unsafe { cell.assume_init_read() };
        // Release: the sender that fills the slot next sees it taken.
        // Sequentially consistent, as is the load of `room_wanted` that
        // follows: see `Chan::push`.
        slot.stamp
            .store(self.head.wrapping_add(chan.one_lap), Ordering::SeqCst);
        self.head = chan.next(self.head);
        if chan.room_wanted.load(Ordering::SeqCst) {
            chan.wake_senders();
        }
        Some(value)
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        // Release, read by senders that acquire it; the lock of the
        // senders' wakers orders it before the look of a sender that
        // registered after it was set.
        self.chan.closed.store(true, Ordering::Release);
        let wakers = mem::take(&mut *lock(&self.chan.sender_wakers));
        // Dropped outside any lock: a value's drop can run any code. A
        // value sent as the receiver went is left for the channel to drop.
        while let Some(value) = self.pop() {
            drop(value);
        }
        for waker in wakers {
            waker.wake();
        }
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver").finish_non_exhaustive()
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
