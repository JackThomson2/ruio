//! Cancellation tokens: telling tasks to stop, and waiting to be told.
//!
//! A token is one word of state, which its clones share. One bit says it
//! is cancelled, which [`CancellationToken::is_cancelled`] reads with one
//! load and [`CancellationToken::cancel`] sets with one read-modify-write;
//! two more say whether anything has waited on the token and whether it
//! has children. The tasks waiting are kept behind a lock, each in a slot
//! of its own, whose index is its ticket, so that a waiter dropped takes
//! exactly its own waker out and nobody is woken for a wait that has
//! ended; a slot freed is taken by the next to wait, so a task that waits
//! on a token and gives up with every turn of a loop allocates nothing
//! after the first. The children are kept weakly in another list. Each
//! list's bit is set with the list locked, as the list first gets
//! something in it, so a cancellation looks in a list only when the bit
//! says it may find something there, and cancelling a token nobody has
//! waited on, with no children, takes no lock. A wait and a cancellation
//! that race see each other: each is a change of the state, and whichever
//! comes second sees the first, so the wait finds the token cancelled, or
//! the cancellation finds the waiter.
//!
//! A child is cancelled with its parent, and on its own without it. A
//! parent prunes the children that are gone from its list whenever one is
//! added, so making and dropping children for ever does not grow it, and a
//! cancellation works down the tree with a list of what is left to cancel
//! rather than by recursion, so a deep tree cannot overflow the stack.

use std::fmt;
use std::future::{Future, poll_fn};
use std::mem;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker, ready};

use super::lock;
use crate::coop;

/// The token is cancelled; once set, never cleared.
const CANCELLED: usize = 1;
/// A waiter has been listed. Set as the first is, with the waiters locked,
/// and never cleared: a cancellation then looks in the list, which costs
/// it one lock, where clearing the bit as the last waiter left would cost
/// every wait that is given up an atomic operation.
const WAITERS: usize = 2;
/// A child has been listed. Set as the first is, with the children locked,
/// and never cleared.
const CHILDREN: usize = 4;

/// Tells tasks to stop: cancelled once, by any clone, for all of them and
/// for its children.
///
/// Clones are the same token. [`cancelled`](CancellationToken::cancelled)
/// waits for the cancellation,
/// [`run_until_cancelled`](CancellationToken::run_until_cancelled) runs a
/// future until it comes, and
/// [`is_cancelled`](CancellationToken::is_cancelled) looks without
/// waiting, with one load, so a loop may ask on every turn. A
/// [`child_token`](CancellationToken::child_token) is cancelled with its
/// parent, or on its own, leaving the parent as it is, and a
/// [`drop_guard`](CancellationToken::drop_guard) cancels the token when it
/// is dropped. It works across threads, and so across cores, and a wait
/// that is ready at once spends a step of the task's budget.
///
/// ```
/// use ruio_executor::sync::CancellationToken;
///
/// # ruio_executor::Runtime::new()?.block_on(async {
/// let token = CancellationToken::new();
/// let worker = {
///     let token = token.clone();
///     ruio_executor::spawn(async move {
///         let mut turns = 0;
///         while !token.is_cancelled() {
///             turns += 1;
///             ruio_executor::yield_now().await;
///         }
///         turns
///     })
/// };
/// ruio_executor::yield_now().await;
/// token.cancel();
/// assert!(worker.await.unwrap() > 0);
/// # });
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct CancellationToken {
    inner: Arc<Inner>,
}

/// What a token's clones share.
struct Inner {
    /// The [`CANCELLED`], [`WAITERS`] and [`CHILDREN`] bits.
    state: AtomicUsize,
    waiters: Mutex<Waiters>,
    /// The children, weakly: a parent does not keep its children, and a
    /// child does not keep its parent.
    children: Mutex<Vec<Weak<Inner>>>,
}

/// A token's waiters: the wakers of the tasks waiting, each in a slot of
/// its own, whose index is its ticket.
struct Waiters {
    /// A waker in each slot that is taken, and `None` in each that is free.
    slots: Vec<Option<Waker>>,
    /// The free slots, by index, for the next waiters to take.
    free: Vec<usize>,
    /// Whether `WAITERS` has been set in the token's state.
    flagged: bool,
}

impl Waiters {
    /// Lists `waker`, in a slot of its own, whose index is its ticket.
    fn insert(&mut self, waker: Waker) -> usize {
        match self.free.pop() {
            Some(ticket) => {
                self.slots[ticket] = Some(waker);
                ticket
            }
            None => {
                self.slots.push(Some(waker));
                self.slots.len() - 1
            }
        }
    }

    /// The waker listed under `ticket`, if one is.
    fn get(&self, ticket: usize) -> Option<&Waker> {
        self.slots.get(ticket)?.as_ref()
    }

    /// Takes the waker with `ticket` out, if it is listed, and frees its
    /// slot.
    fn remove(&mut self, ticket: usize) -> Option<Waker> {
        let waker = self.slots.get_mut(ticket)?.take()?;
        self.free.push(ticket);
        Some(waker)
    }

    /// How many waiters are listed.
    fn len(&self) -> usize {
        self.slots.len() - self.free.len()
    }

    /// Takes every waker out, for the cancellation to wake; nothing is
    /// listed again after it.
    fn drain(&mut self) -> Vec<Option<Waker>> {
        self.free = Vec::new();
        mem::take(&mut self.slots)
    }
}

impl Inner {
    fn new(state: usize) -> Self {
        Self {
            state: AtomicUsize::new(state),
            waiters: Mutex::new(Waiters {
                slots: Vec::new(),
                free: Vec::new(),
                flagged: false,
            }),
            children: Mutex::new(Vec::new()),
        }
    }

    /// Whether the token is cancelled, asked with one of its lists locked,
    /// before something goes into the list; if it is not, sets `flag`, the
    /// list's bit, unless `listed` says the bit is set already. The bit is
    /// set with a read-modify-write, as the token is cancelled, so of the
    /// two whichever comes second in the state's changes sees the first:
    /// the cancellation finds the bit, and looks in the list once the lock
    /// is released, or this finds the token cancelled. A bit set before is
    /// set still, as neither bit is ever cleared, so a load does then.
    fn cancelled_or_flagged(&self, flag: usize, listed: bool) -> bool {
        let state = if listed {
            // Acquire: as in `is_cancelled`.
            self.state.load(Ordering::Acquire)
        } else {
            // Acquire: as in `is_cancelled`. The bit publishes nothing of
            // its own; the lock publishes the list.
            self.state.fetch_or(flag, Ordering::Acquire)
        };
        state & CANCELLED != 0
    }

    /// Lists `waker` for the cancellation to wake, under the ticket in
    /// `ticket` if it has one and is the waker listed under it, and under a
    /// new one otherwise; or, if the token is cancelled, takes the waker
    /// listed out and says so.
    #[cold]
    fn wait(&self, ticket: &mut Option<usize>, waker: &Waker) -> bool {
        if let Some(listed) = ticket.take() {
            let (cancelled, old) = {
                let mut waiters = lock(&self.waiters);
                // Acquire: as in `is_cancelled`. With the list locked, so
                // that a cancellation after this finds the bit set and the
                // waker listed.
                let cancelled = self.state.load(Ordering::Acquire) & CANCELLED != 0;
                if !cancelled {
                    let registered = waiters
                        .get(listed)
                        .expect("a waiter is listed until the token is cancelled");
                    if registered.will_wake(waker) {
                        *ticket = Some(listed);
                        return false;
                    }
                }
                (cancelled, waiters.remove(listed))
            };
            // Dropped outside the lock: dropping a waker can run any code.
            drop(old);
            if cancelled {
                return true;
            }
        }
        // Cloned before the lock is taken, as cloning a waker can run any
        // code too.
        let waker = waker.clone();
        let mut waiters = lock(&self.waiters);
        if self.cancelled_or_flagged(WAITERS, waiters.flagged) {
            drop(waiters);
            drop(waker);
            return true;
        }
        waiters.flagged = true;
        *ticket = Some(waiters.insert(waker));
        false
    }

    /// Takes the waker listed under `ticket` out, for a waiter that gives
    /// up.
    #[cold]
    fn leave(&self, ticket: usize) {
        let waker = lock(&self.waiters).remove(ticket);
        // Dropped outside the lock: dropping a waker can run any code.
        drop(waker);
    }

    /// Wakes the waiters of a token this call has just cancelled, whose
    /// state before was `state`, and cancels its children, and theirs, down
    /// the tree: each level's children go on a list of what is left to
    /// cancel, rather than being cancelled by recursion, so a deep tree
    /// cannot overflow the stack.
    #[cold]
    fn finish_cancel(&self, state: usize) {
        let mut left = Vec::new();
        self.wake_and_collect(state, &mut left);
        while let Some(child) = left.pop() {
            // Release: as in `CancellationToken::cancel`.
            let state = child.state.fetch_or(CANCELLED, Ordering::Release);
            if state & CANCELLED == 0 {
                child.wake_and_collect(state, &mut left);
            }
        }
    }

    /// Wakes the waiters of a token just cancelled, if `state`, its state
    /// before, says it has any, and adds the children it still has to
    /// `left`.
    fn wake_and_collect(&self, state: usize, left: &mut Vec<Arc<Inner>>) {
        if state & WAITERS != 0 {
            let wakers = lock(&self.waiters).drain();
            for waker in wakers.into_iter().flatten() {
                waker.wake();
            }
        }
        if state & CHILDREN != 0 {
            let children = mem::take(&mut *lock(&self.children));
            left.extend(children.iter().filter_map(Weak::upgrade));
        }
    }
}

impl CancellationToken {
    /// A token that is not cancelled, with no children.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner::new(0)),
        }
    }

    /// Cancels the token, every clone of it and every child, and wakes the
    /// tasks waiting on any of them. Cancelling again does nothing.
    ///
    /// With nobody having waited and no children, this is one atomic
    /// operation: the state says whether either list may have anything in
    /// it, so the lists are locked only if it does.
    #[inline]
    pub fn cancel(&self) {
        // Release: what was done before the cancel is seen by whoever sees
        // the token cancelled. The bits come back with the state: a waiter
        // or a child listed before this is in its list, and one that comes
        // after finds the token cancelled.
        let state = self.inner.state.fetch_or(CANCELLED, Ordering::Release);
        if state & CANCELLED == 0 && state & (WAITERS | CHILDREN) != 0 {
            self.inner.finish_cancel(state);
        }
    }

    /// Whether the token is cancelled: one load, so a loop may ask on
    /// every turn.
    #[inline]
    pub fn is_cancelled(&self) -> bool {
        // Acquire: what was done before the cancel is seen by whoever sees
        // the token cancelled.
        self.inner.state.load(Ordering::Acquire) & CANCELLED != 0
    }

    /// Waits for the token to be cancelled; made of a cancelled token, the
    /// wait is over at once.
    pub fn cancelled(&self) -> WaitForCancellation<'_> {
        WaitForCancellation {
            token: self,
            wait: Wait::new(),
        }
    }

    /// Waits for the token to be cancelled, holding the token, so that the
    /// wait can be moved into a task of its own.
    pub fn cancelled_owned(self) -> WaitForCancellationOwned {
        WaitForCancellationOwned {
            token: self,
            wait: Wait::new(),
        }
    }

    /// Runs `future` until it finishes or the token is cancelled: its
    /// output, or `None` if the cancellation came first, the future then
    /// dropped. The future is polled first on each turn, so one that is
    /// ready wins over a cancellation that has just come.
    pub async fn run_until_cancelled<F: Future>(&self, future: F) -> Option<F::Output> {
        let mut future = pin!(future);
        let mut cancelled = self.cancelled();
        poll_fn(|cx| {
            if let Poll::Ready(output) = future.as_mut().poll(cx) {
                return Poll::Ready(Some(output));
            }
            Pin::new(&mut cancelled).poll(cx).map(|()| None)
        })
        .await
    }

    /// A token that is cancelled when this one is, and can be cancelled on
    /// its own, leaving this one as it is; a child of a cancelled token
    /// starts cancelled.
    pub fn child_token(&self) -> CancellationToken {
        let inner = &*self.inner;
        // Acquire: as in `is_cancelled`. Looked at before the lock, as a
        // child of a cancelled token needs no place in the list.
        if inner.state.load(Ordering::Acquire) & CANCELLED == 0 {
            let mut children = lock(&inner.children);
            if !inner.cancelled_or_flagged(CHILDREN, !children.is_empty()) {
                let child = Arc::new(Inner::new(0));
                // Children that are gone leave the list as a new one joins
                // it, so making and dropping children for ever does not
                // grow it.
                children.retain(|child| child.strong_count() > 0);
                children.push(Arc::downgrade(&child));
                return CancellationToken { inner: child };
            }
        }
        CancellationToken {
            inner: Arc::new(Inner::new(CANCELLED)),
        }
    }

    /// A guard that cancels the token when it is dropped, unless it is
    /// disarmed first.
    pub fn drop_guard(self) -> DropGuard {
        DropGuard { token: Some(self) }
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for CancellationToken {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl fmt::Debug for CancellationToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let waiters = lock(&self.inner.waiters).len();
        let children = lock(&self.inner.children).len();
        f.debug_struct("CancellationToken")
            .field("cancelled", &self.is_cancelled())
            .field("waiters", &waiters)
            .field("children", &children)
            .finish()
    }
}

/// A wait's place among a token's waiters.
struct Wait {
    /// The waiter's ticket, the index of its slot, once it waits.
    ticket: Option<usize>,
}

impl Wait {
    const fn new() -> Self {
        Self { ticket: None }
    }

    /// Ready once the token is cancelled: with one load if it is already,
    /// and otherwise once the waker given has been listed, or found listed,
    /// and the token looked at again, so that a cancellation that raced the
    /// listing is not missed.
    #[inline]
    fn poll(&mut self, inner: &Inner, cx: &mut Context<'_>) -> Poll<()> {
        let proceed = ready!(coop::poll_proceed(cx));
        // Acquire: as in `is_cancelled`.
        let cancelled = inner.state.load(Ordering::Acquire) & CANCELLED != 0
            || inner.wait(&mut self.ticket, cx.waker());
        if !cancelled {
            return Poll::Pending;
        }
        // The waker listed, if any, is the cancellation's to take, or
        // `Inner::wait` took it, so there is nothing to leave.
        self.ticket = None;
        proceed.made_progress();
        Poll::Ready(())
    }

    /// Leaves the list, for a waiter dropped before the cancellation.
    #[inline]
    fn leave(&mut self, inner: &Inner) {
        if let Some(ticket) = self.ticket.take() {
            inner.leave(ticket);
        }
    }
}

/// Waits for a [`CancellationToken`] to be cancelled; made by
/// [`CancellationToken::cancelled`]. Once over, it stays over.
#[must_use = "futures do nothing unless polled"]
pub struct WaitForCancellation<'a> {
    token: &'a CancellationToken,
    wait: Wait,
}

impl Future for WaitForCancellation<'_> {
    type Output = ();

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        this.wait.poll(&this.token.inner, cx)
    }
}

impl Drop for WaitForCancellation<'_> {
    #[inline]
    fn drop(&mut self) {
        self.wait.leave(&self.token.inner);
    }
}

impl fmt::Debug for WaitForCancellation<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WaitForCancellation")
            .field("cancelled", &self.token.is_cancelled())
            .finish_non_exhaustive()
    }
}

/// Waits for a [`CancellationToken`] it holds to be cancelled, and so can
/// be moved into a task of its own; made by
/// [`CancellationToken::cancelled_owned`]. Once over, it stays over.
#[must_use = "futures do nothing unless polled"]
pub struct WaitForCancellationOwned {
    token: CancellationToken,
    wait: Wait,
}

impl Future for WaitForCancellationOwned {
    type Output = ();

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        this.wait.poll(&this.token.inner, cx)
    }
}

impl Drop for WaitForCancellationOwned {
    #[inline]
    fn drop(&mut self) {
        self.wait.leave(&self.token.inner);
    }
}

impl fmt::Debug for WaitForCancellationOwned {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WaitForCancellationOwned")
            .field("cancelled", &self.token.is_cancelled())
            .finish_non_exhaustive()
    }
}

/// Cancels its [`CancellationToken`] when it is dropped, however it comes
/// to be dropped; made by [`CancellationToken::drop_guard`].
/// [`disarm`](DropGuard::disarm) takes the token back instead.
#[must_use = "dropping a guard cancels its token at once"]
pub struct DropGuard {
    /// Taken by `disarm`.
    token: Option<CancellationToken>,
}

impl DropGuard {
    /// Takes the token back, so that it is not cancelled when the guard
    /// goes.
    pub fn disarm(mut self) -> CancellationToken {
        self.token.take().expect("only taken here")
    }
}

impl Drop for DropGuard {
    fn drop(&mut self) {
        if let Some(token) = &self.token {
            token.cancel();
        }
    }
}

impl fmt::Debug for DropGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let token = self
            .token
            .as_ref()
            .expect("only taken by `disarm`, which consumes the guard");
        f.debug_struct("DropGuard").field("token", token).finish()
    }
}
