//! A channel for many values, from any number of senders to any number of
//! receivers, each of which gets every value sent while it is subscribed.
//!
//! The values wait in a ring of slots, which keeps the last `capacity` of
//! them, rounded up to a power of two: a send fills the slot after the one
//! filled last, round and round, over whatever value was sent `capacity`
//! sends before, read or not. Each receiver keeps its own place in the
//! sequence of values and reads the slot at it, so that no receiver holds
//! the others back; one that falls more than `capacity` values behind finds
//! the slot at its place filled again since, and is told how many values it
//! missed before it carries on from the oldest still kept. A slot counts the
//! receivers yet to read it, and the last of them takes the value where the
//! others clone it, so that a value goes as soon as every receiver has it.
//!
//! Sends take their turns under the tail's lock, and receivers come and go
//! under it, so that each slot is stamped with the receivers that will
//! read it. A send holds it while it fills its slot, under the slot's own
//! lock, and takes the wakers of the receivers waiting out with it, to
//! wake them after. A receive that finds its value takes no lock but the
//! slot's, shared with the other receivers reading the same value, and one
//! that finds nothing takes the tail's to leave its waker; what a receiver
//! asks about the channel, how much is left or whether it is closed, takes
//! none.
//!
//! Both halves are `Send` and `Sync` when the value is both: receivers on
//! several threads clone one value at once.
//!
//! ```compile_fail
//! fn send<T: Send>() {}
//! send::<ruio_executor::sync::broadcast::Receiver<std::cell::Cell<u8>>>();
//! ```
//!
//! ```
//! use ruio_executor::sync::broadcast;
//!
//! # ruio_executor::Runtime::new()?.block_on(async {
//! let (tx, mut first) = broadcast::channel(16);
//! let mut second = tx.subscribe();
//! let listener = ruio_executor::spawn(async move {
//!     let mut heard = Vec::new();
//!     while let Ok(value) = second.recv().await {
//!         heard.push(value);
//!     }
//!     heard
//! });
//! tx.send("one").unwrap();
//! tx.send("two").unwrap();
//! drop(tx);
//! assert_eq!(first.recv().await, Ok("one"));
//! assert_eq!(first.recv().await, Ok("two"));
//! assert_eq!(first.recv().await, Err(broadcast::RecvError::Closed));
//! assert_eq!(listener.await.unwrap(), ["one", "two"]);
//! # });
//! # Ok::<(), std::io::Error>(())
//! ```

use std::error::Error;
use std::fmt;
use std::future::poll_fn;
use std::mem;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::task::{Context, Poll, Waker, ready};

use super::lock;
use crate::coop;

/// A channel that keeps the last `capacity` values, rounded up to a power
/// of two, for the receivers yet to read them. Room for all of them is
/// allocated at once.
///
/// # Panics
///
/// If `capacity` is zero, or too large to allocate.
pub fn channel<T: Clone>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity > 0, "a channel holds at least one value");
    let len = capacity
        .checked_next_power_of_two()
        .expect("a channel's capacity fits its positions");
    let slots = (0..len)
        .map(|index| Slot {
            pos: AtomicU64::new((index as u64).wrapping_sub(len as u64)),
            rem: AtomicUsize::new(0),
            value: RwLock::new(None),
        })
        .collect();
    let chan = Arc::new(Chan {
        sent: AtomicU64::new(0),
        closed: AtomicBool::new(false),
        senders: AtomicUsize::new(1),
        tail: Mutex::new(Tail {
            receivers: 1,
            wakers: Vec::new(),
        }),
        mask: len - 1,
        slots,
    });
    (
        Sender {
            chan: Arc::clone(&chan),
        },
        Receiver { chan, next: 0 },
    )
}

/// What the halves of a channel share.
///
/// A position counts the values sent, from zero, and names the slot whose
/// index is its low bits; positions never wrap, as a `u64` outlasts any
/// program's sends.
struct Chan<T> {
    /// The position the next send fills: how many values have been sent.
    /// Changed only with the tail locked; read by receivers without it.
    sent: AtomicU64,
    /// Whether every sender is gone. Set with the tail locked; read by
    /// receivers without it.
    closed: AtomicBool,
    /// How many senders there are.
    senders: AtomicUsize,
    tail: Mutex<Tail>,
    /// The bits of a position that are its slot's index.
    mask: usize,
    slots: Box<[Slot<T>]>,
}

/// What a send changes along with its slot, and what changes with the
/// receivers: locked together, so that a send stamps its slot with the
/// receivers that will read it, each of which then reads it or counts
/// itself out as it goes, and finds the wakers of those waiting for it.
struct Tail {
    /// How many receivers there are.
    receivers: usize,
    /// The tasks of receivers waiting for a value.
    wakers: Vec<Waker>,
}

struct Slot<T> {
    /// The position whose value the slot holds, stored after the count and
    /// the value, under the slot's lock, so that a receiver that sees the
    /// position sees them. Before the first, the position one turn before
    /// it: older than any a receiver asks for.
    pos: AtomicU64,
    /// How many receivers have yet to read the value: those there were
    /// when it was sent, less those that have read it or gone since. The
    /// last to read it takes it instead of cloning it.
    rem: AtomicUsize,
    /// The value, from when it is sent until its last reader takes it or a
    /// send `capacity` later replaces it.
    value: RwLock<Option<T>>,
}

/// Locks `value` for reading. A panic poisons it only in a writer, and the
/// writers only move values in and out.
#[inline]
fn read<T>(value: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    value.read().unwrap_or_else(PoisonError::into_inner)
}

/// Locks `value` for writing; see [`read`].
#[inline]
fn write<T>(value: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    value.write().unwrap_or_else(PoisonError::into_inner)
}

impl<T> Chan<T> {
    /// The slot of position `pos`.
    #[inline]
    fn slot(&self, pos: u64) -> &Slot<T> {
        &self.slots[(pos as usize) & self.mask]
    }

    /// How many values the ring keeps.
    #[inline]
    fn capacity(&self) -> u64 {
        self.slots.len() as u64
    }

    /// A receiver of the values sent from now on.
    fn subscribe(chan: &Arc<Self>) -> Receiver<T> {
        let next = {
            let mut tail = lock(&chan.tail);
            tail.receivers += 1;
            // Relaxed: changed only under the lock this holds.
            chan.sent.load(Ordering::Relaxed)
        };
        Receiver {
            chan: Arc::clone(chan),
            next,
        }
    }

    /// Marks the channel closed, every sender being gone, and wakes the
    /// receivers waiting for a value, to learn so.
    #[cold]
    fn close(&self) {
        let wakers = {
            let mut tail = lock(&self.tail);
            // Release: a receiver that sees the channel closed sees every
            // value sent; the lock orders it for one that looks under it.
            self.closed.store(true, Ordering::Release);
            mem::take(&mut tail.wakers)
        };
        for waker in wakers {
            waker.wake();
        }
    }
}

impl<T: Clone> Slot<T> {
    /// Takes the value as its last reader, if the slot still holds the one
    /// at `pos`; or says which position it holds instead.
    fn take(&self, pos: u64) -> Result<T, u64> {
        let mut value = write(&self.value);
        // Relaxed: the slot's lock orders the sender's stores before this.
        let held = self.pos.load(Ordering::Relaxed);
        if held != pos {
            return Err(held);
        }
        let value = value
            .take()
            .expect("a value stays until every receiver counted has read it");
        // Relaxed: the slot's lock orders this before the next send's store,
        // and no other reader is left to change the count.
        self.rem.store(0, Ordering::Relaxed);
        Ok(value)
    }

    /// Clones the value, counting this reader out, if the slot still holds
    /// the one at `pos`; or says which position it holds instead.
    fn share(&self, pos: u64) -> Result<T, u64> {
        let (value, last) = {
            let value = read(&self.value);
            // Relaxed: the slot's lock orders the sender's stores before this.
            let held = self.pos.load(Ordering::Relaxed);
            if held != pos {
                return Err(held);
            }
            let value = value
                .as_ref()
                .expect("a value stays until every receiver counted has read it")
                .clone();
            // Relaxed: the slot's lock orders the clone before the last
            // reader's take, and the count's changes are ordered among
            // themselves, so that exactly one reader finds itself last.
            let last = self.rem.fetch_sub(1, Ordering::Relaxed) == 1;
            (value, last)
        };
        if last {
            // Another reader, or a receiver going, was counted out since the
            // count was looked at: no reader is left, and the value goes.
            self.discard(pos);
        }
        Ok(value)
    }

    /// Drops the value if the slot still holds the one at `pos`, which no
    /// reader is left for.
    #[cold]
    fn discard(&self, pos: u64) {
        let value = {
            let mut value = write(&self.value);
            // Relaxed: the slot's lock orders the sender's stores before this.
            (self.pos.load(Ordering::Relaxed) == pos).then(|| value.take())
        };
        // Dropped outside the lock: a value's drop can run any code.
        drop(value);
    }
}

/// Sends values to every receiver of the channel. Clones send into the same
/// channel; once every one is dropped, the receivers see the channel end,
/// after the values sent before.
pub struct Sender<T> {
    chan: Arc<Chan<T>>,
}

impl<T> Sender<T> {
    /// Sends `value` to every receiver there is, and says how many that is.
    /// A receiver that has yet to read the value sent `capacity` sends
    /// before loses it to this one.
    ///
    /// # Errors
    ///
    /// Hands the value back if there is no receiver.
    pub fn send(&self, value: T) -> Result<usize, SendError<T>> {
        let chan = &*self.chan;
        let (receivers, replaced, wakers) = {
            let mut tail = lock(&chan.tail);
            let receivers = tail.receivers;
            if receivers == 0 {
                return Err(SendError(value));
            }
            // Relaxed: changed only under the lock this holds.
            let pos = chan.sent.load(Ordering::Relaxed);
            let slot = chan.slot(pos);
            let replaced = {
                let mut held = write(&slot.value);
                let replaced = held.replace(value);
                // Relaxed: published by the store of the position below.
                slot.rem.store(receivers, Ordering::Relaxed);
                // Release: a receiver that sees the position sees the count.
                slot.pos.store(pos, Ordering::Release);
                replaced
            };
            // Release: a receiver that sees the count sees the slot filled.
            chan.sent.store(pos + 1, Ordering::Release);
            (receivers, replaced, mem::take(&mut tail.wakers))
        };
        // Dropped outside every lock: a value's drop can run any code.
        drop(replaced);
        for waker in wakers {
            waker.wake();
        }
        Ok(receivers)
    }

    /// A receiver of the values sent from now on.
    pub fn subscribe(&self) -> Receiver<T> {
        Chan::subscribe(&self.chan)
    }

    /// How many receivers there are.
    pub fn receiver_count(&self) -> usize {
        lock(&self.chan.tail).receivers
    }

    /// How many values are kept that some receiver has yet to read. A value
    /// stays until every receiver there was when it was sent has read it or
    /// gone, or until the send `capacity` later replaces it, read or not,
    /// so there are at most `capacity`.
    pub fn len(&self) -> usize {
        let chan = &*self.chan;
        // Held so that nothing is sent meanwhile; receives go on.
        let _tail = lock(&chan.tail);
        // Relaxed: changed only under the lock this holds.
        let sent = chan.sent.load(Ordering::Relaxed);
        let kept = sent.min(chan.capacity());
        let oldest = sent - kept;
        // Receivers read the values kept in order, so of two of them the
        // older has no more readers to come than the newer: a binary search
        // finds the oldest some receiver has yet to read.
        let (mut low, mut high) = (0, kept);
        while low < high {
            let mid = low + (high - low) / 2;
            // Relaxed: a count for the caller.
            if chan.slot(oldest + mid).rem.load(Ordering::Relaxed) == 0 {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        (kept - low) as usize
    }

    /// Whether every value sent has been read by every receiver it was sent
    /// to, or replaced since.
    pub fn is_empty(&self) -> bool {
        let chan = &*self.chan;
        // Held so that nothing is sent meanwhile; receives go on.
        let _tail = lock(&chan.tail);
        // Relaxed: changed only under the lock this holds.
        let sent = chan.sent.load(Ordering::Relaxed);
        // The newest value has the most readers to come; see `len`.
        // Relaxed: a count for the caller.
        sent == 0 || chan.slot(sent - 1).rem.load(Ordering::Relaxed) == 0
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
        // Release: a receiver that sees the count reach zero sees every
        // value sent; acquire, so that the last sender's closing comes
        // after the others' values too.
        if self.chan.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.chan.close();
        }
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender").finish_non_exhaustive()
    }
}

/// Receives the values sent since it subscribed, in the order they were
/// sent. Each value reaches every receiver, so it is cloned for all but
/// the last to read it.
pub struct Receiver<T> {
    chan: Arc<Chan<T>>,
    /// The position of the next value to read.
    next: u64,
}

/// Why a look at a receiver's position found no value.
enum Missing {
    /// Nothing has been sent at the position yet.
    Nothing,
    /// The receiver fell behind by more than the ring keeps, and has moved
    /// on to the oldest value kept, missing this many.
    Lagged(u64),
}

impl Missing {
    /// As the error of a receive, with `nothing` for [`Missing::Nothing`].
    fn or(self, nothing: TryRecvError) -> TryRecvError {
        match self {
            Missing::Nothing => nothing,
            Missing::Lagged(missed) => TryRecvError::Lagged(missed),
        }
    }
}

/// What a receiver that found nothing at its position learns with the tail
/// locked.
enum Waited {
    /// A value came since it looked.
    Sent,
    /// Every sender is gone, and nothing more is coming.
    Closed,
    /// Nothing has come, and its waker is registered for the next send.
    Registered,
}

impl<T: Clone> Receiver<T> {
    /// The next value, waiting for one; see [`poll_recv`](Receiver::poll_recv).
    ///
    /// # Errors
    ///
    /// [`RecvError::Lagged`] if the receiver fell more than `capacity`
    /// values behind, with how many it missed; the next receive gives the
    /// oldest kept. [`RecvError::Closed`] once every sender is gone and
    /// every value sent has been read.
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    /// The next value, or [`RecvError`]: that the receiver missed values,
    /// or that every sender is gone and nothing is left to read; arranges
    /// for the task to be woken otherwise, or, once its budget is spent,
    /// wakes it at once.
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Result<T, RecvError>> {
        let proceed = ready!(coop::poll_proceed(cx));
        let result = loop {
            match self.look() {
                Ok(value) => break Ok(value),
                Err(Missing::Lagged(missed)) => break Err(RecvError::Lagged(missed)),
                Err(Missing::Nothing) => match self.wait(cx.waker()) {
                    Waited::Sent => {}
                    Waited::Closed => break Err(RecvError::Closed),
                    Waited::Registered => return Poll::Pending,
                },
            }
        };
        proceed.made_progress();
        Poll::Ready(result)
    }

    /// The next value if there is one, without waiting.
    ///
    /// # Errors
    ///
    /// [`TryRecvError::Empty`] if every value sent has been read,
    /// [`TryRecvError::Lagged`] if the receiver fell more than `capacity`
    /// values behind, as for [`recv`](Receiver::recv), and
    /// [`TryRecvError::Closed`] if every value has been read and every
    /// sender is gone.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        match self.look() {
            // Acquire: every value sent before the last sender went is seen
            // by the look that follows.
            Err(Missing::Nothing) if self.chan.closed.load(Ordering::Acquire) => self
                .look()
                .map_err(|missing| missing.or(TryRecvError::Closed)),
            found => found.map_err(|missing| missing.or(TryRecvError::Empty)),
        }
    }

    /// The value at the receiver's position, moving it on; or why there is
    /// none there.
    #[inline]
    fn look(&mut self) -> Result<T, Missing> {
        let slot = self.chan.slot(self.next);
        // Acquire: the count of readers stored before the position is seen
        // with it, and not an older one.
        let pos = slot.pos.load(Ordering::Acquire);
        if pos != self.next {
            return Err(self.missing(pos));
        }
        // Relaxed: ordered after the position by the acquire above. Only
        // receivers yet to read the value change the count, so if this one
        // is the only one left, it stays so.
        let found = if slot.rem.load(Ordering::Relaxed) == 1 {
            slot.take(self.next)
        } else {
            slot.share(self.next)
        };
        match found {
            Ok(value) => {
                self.next += 1;
                Ok(value)
            }
            Err(pos) => Err(self.missing(pos)),
        }
    }

    /// Why the slot at the receiver's position holds `pos` instead: nothing
    /// has been sent there yet if `pos` is older; and if it is newer, the
    /// receiver has fallen behind by more than the ring keeps, and moves on
    /// to the oldest value kept, missing those before it.
    #[cold]
    fn missing(&mut self, pos: u64) -> Missing {
        if (pos.wrapping_sub(self.next) as i64) < 0 {
            return Missing::Nothing;
        }
        // Relaxed: only the position is wanted, for arithmetic; the slot it
        // leads to is acquired on its own. It may be behind the position
        // seen in the slot, which was sent too.
        let sent = self.chan.sent.load(Ordering::Relaxed).max(pos + 1);
        let oldest = sent - self.chan.capacity();
        let missed = oldest - self.next;
        self.next = oldest;
        Missing::Lagged(missed)
    }

    /// Waits for a value, having found nothing at the receiver's position:
    /// with the tail locked, where the next position to be sent is settled,
    /// either a value came since the look, or every sender is gone, or
    /// `waker` is left for the next send to wake. A send after this finds
    /// the waker, and one before it is seen here, so the receiver need not
    /// look again before it returns `Pending`.
    #[cold]
    fn wait(&self, waker: &Waker) -> Waited {
        let chan = &*self.chan;
        let mut tail = lock(&chan.tail);
        // Relaxed: changed only under the lock this holds.
        if chan.sent.load(Ordering::Relaxed) != self.next {
            return Waited::Sent;
        }
        // Relaxed: set only under the lock this holds.
        if chan.closed.load(Ordering::Relaxed) {
            return Waited::Closed;
        }
        if !tail
            .wakers
            .iter()
            .any(|registered| registered.will_wake(waker))
        {
            tail.wakers.push(waker.clone());
        }
        Waited::Registered
    }
}

impl<T> Receiver<T> {
    /// A receiver of the same channel at the newest value: it gets the
    /// values sent from now on, not those this one has yet to read.
    pub fn resubscribe(&self) -> Receiver<T> {
        Chan::subscribe(&self.chan)
    }

    /// How many values have been sent that the receiver has yet to read.
    /// Until a receive tells it that it fell behind, the values it missed
    /// count too, and there may be more than `capacity`.
    pub fn len(&self) -> usize {
        // Acquire: a receive that follows finds the values counted.
        (self.chan.sent.load(Ordering::Acquire) - self.next) as usize
    }

    /// Whether the receiver has read every value sent.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether every sender is gone, so that the channel ends once the
    /// values left are read.
    pub fn is_closed(&self) -> bool {
        // Acquire: a receive that follows finds every value sent.
        self.chan.closed.load(Ordering::Acquire)
    }

    /// Whether `other` receives from the same channel.
    pub fn same_channel(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.chan, &other.chan)
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let chan = &*self.chan;
        let mut unread = Vec::new();
        {
            let mut tail = lock(&chan.tail);
            tail.receivers -= 1;
            // Relaxed: changed only under the lock this holds.
            let sent = chan.sent.load(Ordering::Relaxed);
            let oldest = sent.saturating_sub(chan.capacity());
            // Counted out of every value kept that it has yet to read; those
            // it missed are gone already.
            for pos in self.next.max(oldest)..sent {
                let slot = chan.slot(pos);
                // Relaxed: the lock keeps the senders out, and the slot's
                // lock orders the other readers' clones before the take.
                if slot.rem.fetch_sub(1, Ordering::Relaxed) == 1 {
                    unread.extend(write(&slot.value).take());
                }
            }
        }
        // Dropped outside every lock: a value's drop can run any code.
        drop(unread);
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver")
            .field("next", &self.next)
            .finish_non_exhaustive()
    }
}

/// There is no receiver; the value comes back.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendError").finish_non_exhaustive()
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("there is no receiver")
    }
}

impl<T> Error for SendError<T> {}

/// Why [`Receiver::recv`] has no value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecvError {
    /// Every sender is gone, and every value sent has been read.
    Closed,
    /// The receiver fell more than `capacity` values behind, and this many
    /// were replaced before it read them; it goes on from the oldest kept.
    Lagged(u64),
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecvError::Closed => f.write_str("every sender is gone, and nothing is left to read"),
            RecvError::Lagged(missed) => {
                write!(
                    f,
                    "the receiver fell behind, and {missed} values were replaced unread"
                )
            }
        }
    }
}

impl Error for RecvError {}

/// Why [`Receiver::try_recv`] has no value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TryRecvError {
    /// Every value sent has been read.
    Empty,
    /// Every value sent has been read, and every sender is gone.
    Closed,
    /// The receiver fell more than `capacity` values behind, and this many
    /// were replaced before it read them; it goes on from the oldest kept.
    Lagged(u64),
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryRecvError::Empty => f.write_str("every value sent has been read"),
            TryRecvError::Closed => {
                f.write_str("every sender is gone, and nothing is left to read")
            }
            TryRecvError::Lagged(missed) => {
                write!(
                    f,
                    "the receiver fell behind, and {missed} values were replaced unread"
                )
            }
        }
    }
}

impl Error for TryRecvError {}
