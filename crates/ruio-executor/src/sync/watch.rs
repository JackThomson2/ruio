//! A channel of one value, which its senders replace and its receivers
//! watch: each receiver sees the latest value, and waits for one it has
//! not seen.
//!
//! The value is behind a reader-writer lock, which a send takes for
//! writing and a borrow for reading, so receivers borrow it at the same
//! time and a send waits for their borrows to end. Beside it a version
//! counts the changes, and each receiver remembers the version it saw
//! last. A send bumps the version with the value still locked, so that a
//! borrow, which reads the version with the value locked too, has the
//! version of the value it holds. A receiver that finds the version it
//! remembers has nothing new to see, and waits: it puts its waker in a
//! locked list, sets a flag beside the list, and looks at the version
//! again; a send bumps the version and reads the flag, and only if it is
//! set takes the lock, empties the list and wakes every waker in it,
//! outside every lock. Either the send finds the flag set or the receiver
//! finds the change, so no wake is lost; and a send with nobody waiting
//! costs the write lock and one atomic operation, a wait that finds a
//! change one load, and a borrow the read lock. However many values are
//! sent before a receiver looks, it sees one change, and the latest value.
//!
//! The version's low bit is set once every sender is gone: a receiver
//! that finds it set, with nothing left unseen, learns that the channel
//! has ended, and the last value stays to be borrowed. Senders waiting in
//! [`Sender::closed`] for the receivers to go are kept as receivers
//! waiting for a change are, in a list with a flag, which the last
//! receiver to go reads.
//!
//! ```
//! use ruio_executor::sync::watch;
//!
//! # ruio_executor::Runtime::new()?.block_on(async {
//! let (tx, mut rx) = watch::channel("starting");
//! let watcher = ruio_executor::spawn(async move {
//!     rx.changed().await.unwrap();
//!     // Only the latest value is there, however many were sent.
//!     *rx.borrow_and_update()
//! });
//! tx.send("running").unwrap();
//! tx.send("stopping").unwrap();
//! assert_eq!(watcher.await.unwrap(), "stopping");
//! # });
//! # Ok::<(), std::io::Error>(())
//! ```

use std::error::Error;
use std::fmt;
use std::future::poll_fn;
use std::mem;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::task::{Context, Poll, Waker, ready};

use super::lock;
use crate::coop;

/// Set in a channel's version once every sender is gone.
const CLOSED: usize = 1;

/// One change, in a channel's version, above the flag.
const CHANGE: usize = 2;

/// A channel whose value is `init`, with one sender and one receiver,
/// which has seen the value: its first wait is for a send.
pub fn channel<T>(init: T) -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        value: RwLock::new(init),
        version: AtomicUsize::new(0),
        senders: AtomicUsize::new(1),
        receivers: AtomicUsize::new(1),
        changed: Waiters::new(),
        closed: Waiters::new(),
    });
    (
        Sender {
            shared: Arc::clone(&shared),
        },
        Receiver { shared, seen: 0 },
    )
}

/// What the halves of a channel share.
struct Shared<T> {
    /// The value, which a send locks for writing and a borrow for reading.
    value: RwLock<T>,
    /// The `CLOSED` flag, and above it the count of changes, bumped with
    /// the value locked for writing.
    version: AtomicUsize,
    /// How many senders there are.
    senders: AtomicUsize,
    /// How many receivers there are.
    receivers: AtomicUsize,
    /// Receivers' tasks, waiting for a change.
    changed: Waiters,
    /// Senders' tasks, waiting for the receivers to go.
    closed: Waiters,
}

impl<T> Shared<T> {
    /// The version of the value, without the flag, with the value locked
    /// by the caller for reading or writing.
    #[inline]
    fn version_locked(&self) -> usize {
        // Relaxed: the lock orders this after the bump of the value held,
        // made with the value locked for writing, and before the next.
        self.version.load(Ordering::Relaxed) & !CLOSED
    }

    /// Ready with the version, without the flag, once it is not `seen`, or
    /// with [`RecvError`] once every sender is gone and it is; otherwise
    /// has the task woken by the next change or the last sender's going,
    /// or, once its budget is spent, at once.
    #[inline]
    fn poll_changed(&self, seen: usize, cx: &mut Context<'_>) -> Poll<Result<usize, RecvError>> {
        let proceed = ready!(coop::poll_proceed(cx));
        // Acquire: a change seen was written, under the lock, before this.
        let mut version = self.version.load(Ordering::Acquire);
        if version == seen {
            self.changed.register(cx.waker());
            // Again, now that a send wakes the task. Sequentially
            // consistent: see `Waiters`.
            version = self.version.load(Ordering::SeqCst);
            if version == seen {
                return Poll::Pending;
            }
        }
        proceed.made_progress();
        if version & !CLOSED == seen {
            Poll::Ready(Err(RecvError(())))
        } else {
            Poll::Ready(Ok(version & !CLOSED))
        }
    }

    /// Ready once no receiver is left; otherwise has the task woken by the
    /// last receiver's going, or, once its budget is spent, at once.
    fn poll_closed(&self, cx: &mut Context<'_>) -> Poll<()> {
        let proceed = ready!(coop::poll_proceed(cx));
        // Relaxed: a count, which says only how many there are now.
        if self.receivers.load(Ordering::Relaxed) != 0 {
            self.closed.register(cx.waker());
            // Again, now that the last receiver's going wakes the task.
            // Sequentially consistent: see `Waiters`.
            if self.receivers.load(Ordering::SeqCst) != 0 {
                return Poll::Pending;
            }
        }
        proceed.made_progress();
        Poll::Ready(())
    }
}

/// Tasks waiting for something to happen, and a flag saying whether any
/// may be, which whatever makes it happen reads before it takes the lock:
/// with nobody waiting, the common case, it costs one load.
///
/// A task that waits puts its waker in the list, sets the flag, and looks
/// again at what it waits for; the other side makes it happen, and then
/// reads the flag. All four are sequentially consistent, so that either
/// the task sees what happened or the other side sees the flag: a store
/// and a load that another thread may reorder need a barrier between
/// them.
struct Waiters {
    /// Whether a task may be in `wakers`; set under its lock, read
    /// without.
    waiting: AtomicBool,
    wakers: Mutex<Vec<Waker>>,
}

impl Waiters {
    fn new() -> Self {
        Self {
            waiting: AtomicBool::new(false),
            wakers: Mutex::new(Vec::new()),
        }
    }

    /// Puts `waker` in the list, unless it is there, and sets the flag;
    /// the caller then looks again at what it waits for.
    #[cold]
    fn register(&self, waker: &Waker) {
        let mut wakers = lock(&self.wakers);
        if !wakers.iter().any(|known| known.will_wake(waker)) {
            wakers.push(waker.clone());
        }
        // Sequentially consistent, with the look that follows and the
        // other side's change and load of the flag: see above.
        self.waiting.store(true, Ordering::SeqCst);
    }

    /// Wakes every task in the list, if the flag says there may be one.
    #[inline]
    fn wake_all(&self) {
        // Sequentially consistent, as every access is: see above.
        if self.waiting.load(Ordering::SeqCst) {
            self.wake_waiting();
        }
    }

    /// Takes the wakers out of the list, under its lock, and wakes them
    /// outside it: a wake can run any code.
    #[cold]
    fn wake_waiting(&self) {
        let wakers = {
            let mut wakers = lock(&self.wakers);
            // Sequentially consistent, as every access is: see above.
            self.waiting.store(false, Ordering::SeqCst);
            mem::take(&mut *wakers)
        };
        for waker in wakers {
            waker.wake();
        }
    }
}

/// Locks the value for reading, poisoned or not: a panic in a sender's
/// closure leaves the value as the closure left it, which is a value
/// still.
#[inline]
fn read<T>(value: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    value.read().unwrap_or_else(PoisonError::into_inner)
}

/// Locks the value for writing, poisoned or not; see `read`.
#[inline]
fn write<T>(value: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    value.write().unwrap_or_else(PoisonError::into_inner)
}

/// Replaces the channel's value, for the receivers to see. Clones send
/// into the same channel; once every one is dropped, the receivers see
/// the channel end, after the last value.
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Sender<T> {
    /// Replaces the value with `value`, for the receivers to see, and
    /// drops the value replaced; or hands `value` back if no receiver is
    /// alive, leaving the value as it is.
    ///
    /// # Errors
    ///
    /// [`SendError`], with the value, if no receiver is alive.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        if self.is_closed() {
            return Err(SendError(value));
        }
        // Dropped outside every lock: a value's drop can run any code.
        drop(self.send_replace(value));
        Ok(())
    }

    /// Lets `modify` change the value in place, with the value locked for
    /// writing, so that nothing borrows it meanwhile, and then tells the
    /// receivers of a change, whether or not `modify` made one. A panic in
    /// `modify` leaves the value as `modify` left it, with no receiver
    /// told.
    pub fn send_modify(&self, modify: impl FnOnce(&mut T)) {
        self.send_if_modified(|value| {
            modify(value);
            true
        });
    }

    /// Lets `modify` change the value in place, with the value locked for
    /// writing, so that nothing borrows it meanwhile, and tells the
    /// receivers of a change only if `modify` returns `true`, which this
    /// returns too: a `modify` that left the value as it was need wake
    /// nobody. A panic in `modify` leaves the value as `modify` left it,
    /// with no receiver told.
    #[inline]
    pub fn send_if_modified(&self, modify: impl FnOnce(&mut T) -> bool) -> bool {
        {
            let mut value = write(&self.shared.value);
            if !modify(&mut value) {
                return false;
            }
            // Bumped with the value still locked, so that a borrow, which
            // reads the version with the value locked for reading, has
            // the version of the value it holds. Release: a receiver that
            // acquires the version sees the write it counts; sequentially
            // consistent besides, with the load of the flag that follows:
            // see `Waiters`.
            self.shared.version.fetch_add(CHANGE, Ordering::SeqCst);
        }
        self.shared.changed.wake_all();
        true
    }

    /// Replaces the value with `value`, for the receivers to see, whether
    /// or not any is alive, and returns the value replaced.
    pub fn send_replace(&self, mut value: T) -> T {
        self.send_modify(|current| mem::swap(current, &mut value));
        value
    }

    /// The value as it is now; see [`Ref`] for how long it may be held. A
    /// sender has nothing unseen, so the borrow's
    /// [`has_changed`](Ref::has_changed) is `false`.
    #[inline]
    pub fn borrow(&self) -> Ref<'_, T> {
        Ref {
            value: read(&self.shared.value),
            has_changed: false,
        }
    }

    /// A receiver that has seen the value as it is now, so that its first
    /// wait is for the next send. If every receiver had gone, the channel
    /// is open again for it.
    pub fn subscribe(&self) -> Receiver<T> {
        let shared = Arc::clone(&self.shared);
        // Relaxed: a count, which publishes nothing.
        shared.receivers.fetch_add(1, Ordering::Relaxed);
        // Acquire: a change seen was written, under the lock, before this.
        let seen = shared.version.load(Ordering::Acquire) & !CLOSED;
        Receiver { shared, seen }
    }

    /// How many receivers there are.
    pub fn receiver_count(&self) -> usize {
        // Relaxed: a count, which says only how many there are now.
        self.shared.receivers.load(Ordering::Relaxed)
    }

    /// Whether no receiver is left, so that sending is in vain.
    pub fn is_closed(&self) -> bool {
        self.receiver_count() == 0
    }

    /// Waits until no receiver is left. A receiver made by
    /// [`subscribe`](Sender::subscribe) afterwards opens the channel
    /// again, and another call waits for it to go.
    pub async fn closed(&self) {
        poll_fn(|cx| self.shared.poll_closed(cx)).await
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        // Relaxed: this sender keeps the count above zero meanwhile.
        self.shared.senders.fetch_add(1, Ordering::Relaxed);
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        // Release, and acquire: the last sender's closing of the channel
        // comes after every sender's sends.
        if self.shared.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
            // Sequentially consistent, with the load of the flag that
            // follows: see `Waiters`.
            self.shared.version.fetch_or(CLOSED, Ordering::SeqCst);
            self.shared.changed.wake_all();
        }
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender").finish_non_exhaustive()
    }
}

/// Watches the channel's value: borrows it as it is, and waits for a
/// value it has not seen. Clones watch the same channel, and each keeps
/// its own account of what it has seen.
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    /// The version seen last, without the `CLOSED` flag.
    seen: usize,
}

impl<T> Receiver<T> {
    /// The value as it is now, not marked seen; see [`Ref`] for how long
    /// it may be held.
    #[inline]
    pub fn borrow(&self) -> Ref<'_, T> {
        let value = read(&self.shared.value);
        let has_changed = self.shared.version_locked() != self.seen;
        Ref { value, has_changed }
    }

    /// The value as it is now, marked seen, so that the next
    /// [`changed`](Receiver::changed) waits for a send after this; see
    /// [`Ref`] for how long it may be held.
    #[inline]
    pub fn borrow_and_update(&mut self) -> Ref<'_, T> {
        let value = read(&self.shared.value);
        let version = self.shared.version_locked();
        let has_changed = version != self.seen;
        self.seen = version;
        Ref { value, has_changed }
    }

    /// Waits for a value this receiver has not seen, and marks it seen;
    /// [`borrow`](Receiver::borrow) then has it. However many values were
    /// sent meanwhile, there is one change to see, and the latest value.
    ///
    /// # Errors
    ///
    /// [`RecvError`] once every sender is gone and nothing is left unseen;
    /// the last value is still there to borrow.
    pub async fn changed(&mut self) -> Result<(), RecvError> {
        poll_fn(|cx| self.poll_changed(cx)).await
    }

    /// Ready once a value this receiver has not seen is there, marked
    /// seen, or with [`RecvError`] once every sender is gone and nothing
    /// is left unseen; arranges for the task to be woken otherwise, or,
    /// once its budget is spent, wakes it at once.
    pub fn poll_changed(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), RecvError>> {
        let version = ready!(self.shared.poll_changed(self.seen, cx))?;
        self.seen = version;
        Poll::Ready(Ok(()))
    }

    /// Whether a value this receiver has not seen is there, without
    /// waiting for one or marking it seen.
    ///
    /// # Errors
    ///
    /// [`RecvError`] once every sender is gone and nothing is left unseen.
    #[inline]
    pub fn has_changed(&self) -> Result<bool, RecvError> {
        // Acquire: a change seen was written, under the lock, before this.
        let version = self.shared.version.load(Ordering::Acquire);
        if version & !CLOSED != self.seen {
            Ok(true)
        } else if version & CLOSED != 0 {
            Err(RecvError(()))
        } else {
            Ok(false)
        }
    }

    /// Marks the value unseen, so that the next
    /// [`changed`](Receiver::changed) is ready at once, with the value as
    /// it is then.
    pub fn mark_changed(&mut self) {
        self.seen = self.seen.wrapping_sub(CHANGE);
    }

    /// Marks the value seen without borrowing it, so that the next
    /// [`changed`](Receiver::changed) waits for a send after this.
    pub fn mark_unchanged(&mut self) {
        // Acquire: a change seen was written, under the lock, before this.
        self.seen = self.shared.version.load(Ordering::Acquire) & !CLOSED;
    }

    /// Waits for a value that satisfies `predicate`, and borrows it,
    /// marked seen: `predicate` sees the value as it is now first, seen or
    /// not, and then each value sent. It runs with the value locked for
    /// reading, so a send waits while it does; it must not await or
    /// block.
    ///
    /// # Errors
    ///
    /// [`RecvError`] once every sender is gone and the last value does not
    /// satisfy `predicate`.
    pub async fn wait_for(
        &mut self,
        mut predicate: impl FnMut(&T) -> bool,
    ) -> Result<Ref<'_, T>, RecvError> {
        let (shared, seen) = (&*self.shared, &mut self.seen);
        loop {
            let proceed = poll_fn(coop::poll_proceed).await;
            {
                let value = read(&shared.value);
                let version = shared.version_locked();
                let has_changed = version != *seen;
                *seen = version;
                if predicate(&value) {
                    proceed.made_progress();
                    return Ok(Ref { value, has_changed });
                }
            }
            // Once every sender is gone, the value looked at was the last.
            poll_fn(|cx| shared.poll_changed(*seen, cx)).await?;
        }
    }

    /// Whether `other` watches the same channel.
    pub fn same_channel(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }
}

impl<T> Clone for Receiver<T> {
    /// A receiver that has seen what this one has.
    fn clone(&self) -> Self {
        // Relaxed: this receiver keeps the count above zero meanwhile.
        self.shared.receivers.fetch_add(1, Ordering::Relaxed);
        Self {
            shared: Arc::clone(&self.shared),
            seen: self.seen,
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        // Sequentially consistent, with the load of the flag that follows:
        // see `Waiters`.
        if self.shared.receivers.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.shared.closed.wake_all();
        }
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver").finish_non_exhaustive()
    }
}

/// The values of a channel as a stream, with the `futures` feature: each
/// item is a clone of the latest value, once a value the receiver has not
/// seen is there, however many were sent meanwhile; the stream ends once
/// every sender is gone and nothing is left unseen. Made by
/// [`WatchStream::new`], which yields the value as it is first, or
/// [`WatchStream::from_changes`], which yields only the values sent from
/// then on.
///
/// ```
/// use futures_util::StreamExt;
/// use ruio_executor::sync::watch::{self, WatchStream};
///
/// # ruio_executor::Runtime::new()?.block_on(async {
/// let (tx, rx) = watch::channel(1);
/// let mut values = WatchStream::new(rx);
/// assert_eq!(values.next().await, Some(1));
/// tx.send(2).unwrap();
/// tx.send(3).unwrap();
/// assert_eq!(values.next().await, Some(3), "the latest");
/// drop(tx);
/// assert_eq!(values.next().await, None);
/// # });
/// # Ok::<(), std::io::Error>(())
/// ```
#[cfg(feature = "futures")]
#[must_use = "a stream does nothing until polled"]
pub struct WatchStream<T> {
    receiver: Receiver<T>,
}

#[cfg(feature = "futures")]
impl<T> WatchStream<T> {
    /// A stream of `receiver`'s channel whose first item is the value as it
    /// is now, seen or not.
    pub fn new(mut receiver: Receiver<T>) -> Self {
        receiver.mark_changed();
        Self { receiver }
    }

    /// A stream of `receiver`'s channel whose items are the values sent
    /// from now on: the value as it is now comes only if the receiver has
    /// not seen it.
    pub fn from_changes(receiver: Receiver<T>) -> Self {
        Self { receiver }
    }

    /// The receiver.
    pub fn get_ref(&self) -> &Receiver<T> {
        &self.receiver
    }

    /// The receiver back, having seen what the stream has yielded.
    pub fn into_inner(self) -> Receiver<T> {
        self.receiver
    }
}

#[cfg(feature = "futures")]
impl<T: Clone> futures_core::stream::Stream for WatchStream<T> {
    type Item = T;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let receiver = &mut self.get_mut().receiver;
        Poll::Ready(match ready!(receiver.poll_changed(cx)) {
            Ok(()) => Some(T::clone(&receiver.borrow())),
            Err(RecvError(())) => None,
        })
    }
}

/// Terminated once every sender is gone and nothing is left unseen.
#[cfg(feature = "futures")]
impl<T: Clone> futures_core::stream::FusedStream for WatchStream<T> {
    fn is_terminated(&self) -> bool {
        self.receiver.has_changed().is_err()
    }
}

#[cfg(feature = "futures")]
impl<T> fmt::Debug for WatchStream<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WatchStream")
            .field("receiver", &self.receiver)
            .finish()
    }
}

/// A borrow of the channel's value, made by [`Sender::borrow`],
/// [`Receiver::borrow`] and [`Receiver::borrow_and_update`].
///
/// It holds the value's lock for reading, which a send takes for writing:
/// the send blocks its thread until every borrow is dropped. So a borrow
/// must not be held across an await, where a send on another thread would
/// wait as long as the task does, and one on the same thread for ever;
/// nor may the task holding one send.
pub struct Ref<'a, T> {
    value: RwLockReadGuard<'a, T>,
    /// Whether the receiver that borrowed the value had not seen it.
    has_changed: bool,
}

impl<T> Ref<'_, T> {
    /// Whether the value borrowed was one the receiver that borrowed it
    /// had not seen; `false` for a sender's borrow.
    pub fn has_changed(&self) -> bool {
        self.has_changed
    }
}

impl<T> Deref for Ref<'_, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T: fmt::Debug> fmt::Debug for Ref<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ref")
            .field("value", &&*self.value)
            .field("has_changed", &self.has_changed)
            .finish_non_exhaustive()
    }
}

/// No receiver is alive; the value comes back.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendError").finish_non_exhaustive()
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("every receiver is gone")
    }
}

impl<T> Error for SendError<T> {}

/// Every sender is gone, and nothing is left unseen.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecvError(());

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("every sender is gone, and nothing is left unseen")
    }
}

impl Error for RecvError {}
