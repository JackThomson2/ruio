//! A counting semaphore, fair to its waiters.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker, ready};

use super::lock;
use crate::coop;

/// A semaphore: a count of permits, which tasks acquire and give back by
/// dropping them, waiting while there are too few.
///
/// It is fair: waiters are served in the order they began to wait, each
/// with all the permits it asked for at once, so a waiter that needs many
/// is not passed by later ones that need fewer. A waiter dropped once its
/// permits were set aside for it gives them back. It works across threads,
/// and so across cores, and an acquire that is ready at once spends a step
/// of the task's budget.
///
/// ```
/// use std::sync::Arc;
///
/// use ruio_executor::sync::Semaphore;
///
/// # ruio_executor::Runtime::new()?.block_on(async {
/// // At most two downloads at once.
/// let semaphore = Arc::new(Semaphore::new(2));
/// let tasks: Vec<_> = (0..10)
///     .map(|_| {
///         let semaphore = Arc::clone(&semaphore);
///         ruio_executor::spawn(async move {
///             let _permit = semaphore.acquire().await;
///             // ...
///         })
///     })
///     .collect();
/// for task in tasks {
///     task.await.unwrap();
/// }
/// # });
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct Semaphore {
    state: Mutex<State>,
}

struct State {
    /// The permits neither held nor set aside for a waiter.
    permits: usize,
    /// The waiters that have no permits yet, by ticket: in the order they
    /// began to wait.
    queue: BTreeMap<u64, Waiter>,
    /// The tickets of waiters whose permits are set aside, until they take
    /// them.
    granted: BTreeSet<u64>,
    next_ticket: u64,
}

struct Waiter {
    permits: usize,
    waker: Waker,
}

impl State {
    /// Sets permits aside for the waiters at the head of the queue, as many
    /// as there are permits for, and returns their wakers.
    fn grant(&mut self) -> Vec<Waker> {
        let mut wakers = Vec::new();
        while let Some(entry) = self.queue.first_entry() {
            if entry.get().permits > self.permits {
                break;
            }
            let (ticket, waiter) = entry.remove_entry();
            self.permits -= waiter.permits;
            self.granted.insert(ticket);
            wakers.push(waiter.waker);
        }
        wakers
    }
}

impl Semaphore {
    /// The most permits a semaphore can have.
    pub const MAX_PERMITS: usize = usize::MAX >> 3;

    /// A semaphore with `permits` permits.
    ///
    /// # Panics
    ///
    /// If `permits` is more than [`MAX_PERMITS`](Semaphore::MAX_PERMITS).
    pub const fn new(permits: usize) -> Self {
        assert!(permits <= Self::MAX_PERMITS, "too many permits");
        Self {
            state: Mutex::new(State {
                permits,
                queue: BTreeMap::new(),
                granted: BTreeSet::new(),
                next_ticket: 0,
            }),
        }
    }

    /// How many permits can be acquired now, as far as nobody waits.
    pub fn available_permits(&self) -> usize {
        lock(&self.state).permits
    }

    /// Adds `permits` permits, which go to the waiters first.
    ///
    /// # Panics
    ///
    /// If the semaphore would have more than
    /// [`MAX_PERMITS`](Semaphore::MAX_PERMITS).
    pub fn add_permits(&self, permits: usize) {
        self.release(permits);
    }

    /// Waits for a permit, which goes back when the permit is dropped.
    pub fn acquire(&self) -> Acquire<'_> {
        self.acquire_many(1)
    }

    /// Waits for `permits` permits at once.
    pub fn acquire_many(&self, permits: usize) -> Acquire<'_> {
        Acquire {
            semaphore: self,
            wait: Wait::new(permits),
        }
    }

    /// Waits for a permit, which holds a reference to the semaphore, and so
    /// can be moved into a task of its own.
    pub fn acquire_owned(self: Arc<Self>) -> AcquireOwned {
        self.acquire_many_owned(1)
    }

    /// Waits for `permits` permits at once, which hold a reference to the
    /// semaphore.
    pub fn acquire_many_owned(self: Arc<Self>, permits: usize) -> AcquireOwned {
        AcquireOwned {
            semaphore: Some(self),
            wait: Wait::new(permits),
        }
    }

    /// A permit, if one is free and nobody waits for one.
    pub fn try_acquire(&self) -> Option<SemaphorePermit<'_>> {
        self.try_acquire_many(1)
    }

    /// `permits` permits, if they are free and nobody waits.
    pub fn try_acquire_many(&self, permits: usize) -> Option<SemaphorePermit<'_>> {
        // Made only once the permits are taken: dropped, it gives them back.
        self.take(permits).then(|| SemaphorePermit {
            semaphore: self,
            permits,
        })
    }

    /// A permit holding a reference to the semaphore, if one is free and
    /// nobody waits.
    pub fn try_acquire_owned(self: Arc<Self>) -> Option<OwnedSemaphorePermit> {
        self.try_acquire_many_owned(1)
    }

    /// `permits` permits holding a reference to the semaphore, if they are
    /// free and nobody waits.
    pub fn try_acquire_many_owned(self: Arc<Self>, permits: usize) -> Option<OwnedSemaphorePermit> {
        if !self.take(permits) {
            return None;
        }
        Some(OwnedSemaphorePermit {
            semaphore: self,
            permits,
        })
    }

    /// Takes `permits` permits at once if they are free and nobody waits.
    fn take(&self, permits: usize) -> bool {
        let mut state = lock(&self.state);
        if state.queue.is_empty() && state.permits >= permits {
            state.permits -= permits;
            true
        } else {
            false
        }
    }

    /// Gives back `permits` permits, and wakes the waiters they go to.
    fn release(&self, permits: usize) {
        if permits == 0 {
            return;
        }
        let wakers = {
            let mut state = lock(&self.state);
            state.permits = state
                .permits
                .checked_add(permits)
                .filter(|&total| total <= Self::MAX_PERMITS)
                .expect("too many permits");
            state.grant()
        };
        for waker in wakers {
            waker.wake();
        }
    }
}

impl fmt::Debug for Semaphore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = lock(&self.state);
        f.debug_struct("Semaphore")
            .field("permits", &state.permits)
            .field("waiters", &state.queue.len())
            .finish()
    }
}

/// An acquire's place among the semaphore's waiters.
struct Wait {
    permits: usize,
    /// The waiter's ticket, once it waits.
    ticket: Option<u64>,
    /// Set once the acquire has its permits.
    done: bool,
}

impl Wait {
    fn new(permits: usize) -> Self {
        Self {
            permits,
            ticket: None,
            done: false,
        }
    }

    /// Ready once the permits are the acquire's.
    ///
    /// # Panics
    ///
    /// If the acquire has its permits already.
    fn poll(&mut self, semaphore: &Semaphore, cx: &mut Context<'_>) -> Poll<()> {
        assert!(!self.done, "an acquire polled after it completed");
        let proceed = ready!(coop::poll_proceed(cx));
        let mut state = lock(&semaphore.state);
        match self.ticket {
            None => {
                if state.queue.is_empty() && state.permits >= self.permits {
                    state.permits -= self.permits;
                } else {
                    let ticket = state.next_ticket;
                    state.next_ticket += 1;
                    let waiter = Waiter {
                        permits: self.permits,
                        waker: cx.waker().clone(),
                    };
                    state.queue.insert(ticket, waiter);
                    self.ticket = Some(ticket);
                    return Poll::Pending;
                }
            }
            Some(ticket) => {
                if !state.granted.remove(&ticket) {
                    let waiter = state
                        .queue
                        .get_mut(&ticket)
                        .expect("a waiter is queued until it is granted");
                    if !waiter.waker.will_wake(cx.waker()) {
                        waiter.waker = cx.waker().clone();
                    }
                    return Poll::Pending;
                }
                self.ticket = None;
            }
        }
        self.done = true;
        proceed.made_progress();
        Poll::Ready(())
    }

    /// Leaves the queue, giving back the permits set aside for the waiter,
    /// if any, and letting the waiters behind it have what it held up.
    fn cancel(&mut self, semaphore: &Semaphore) {
        let Some(ticket) = self.ticket.take() else {
            return;
        };
        let wakers = {
            let mut state = lock(&semaphore.state);
            if state.granted.remove(&ticket) {
                state.permits += self.permits;
            } else {
                state.queue.remove(&ticket);
            }
            state.grant()
        };
        for waker in wakers {
            waker.wake();
        }
    }
}

/// Waits for permits; made by [`Semaphore::acquire`] and
/// [`Semaphore::acquire_many`]. Dropping it gives up its place, and any
/// permits already set aside for it.
#[must_use = "futures do nothing unless polled"]
pub struct Acquire<'a> {
    semaphore: &'a Semaphore,
    wait: Wait,
}

impl<'a> Future for Acquire<'a> {
    type Output = SemaphorePermit<'a>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        ready!(this.wait.poll(this.semaphore, cx));
        Poll::Ready(SemaphorePermit {
            semaphore: this.semaphore,
            permits: this.wait.permits,
        })
    }
}

impl Drop for Acquire<'_> {
    fn drop(&mut self) {
        self.wait.cancel(self.semaphore);
    }
}

impl fmt::Debug for Acquire<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Acquire")
            .field("permits", &self.wait.permits)
            .finish_non_exhaustive()
    }
}

/// Waits for permits that hold a reference to their semaphore; made by
/// [`Semaphore::acquire_owned`] and [`Semaphore::acquire_many_owned`].
#[must_use = "futures do nothing unless polled"]
pub struct AcquireOwned {
    /// Taken by the permit.
    semaphore: Option<Arc<Semaphore>>,
    wait: Wait,
}

impl Future for AcquireOwned {
    type Output = OwnedSemaphorePermit;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let semaphore = this
            .semaphore
            .as_deref()
            .expect("`AcquireOwned` polled after it completed");
        ready!(this.wait.poll(semaphore, cx));
        Poll::Ready(OwnedSemaphorePermit {
            semaphore: this.semaphore.take().expect("checked above"),
            permits: this.wait.permits,
        })
    }
}

impl Drop for AcquireOwned {
    fn drop(&mut self) {
        if let Some(semaphore) = &self.semaphore {
            self.wait.cancel(semaphore);
        }
    }
}

impl fmt::Debug for AcquireOwned {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AcquireOwned")
            .field("permits", &self.wait.permits)
            .finish_non_exhaustive()
    }
}

/// Permits of a [`Semaphore`], which go back when this is dropped.
#[must_use = "dropping a permit gives it back at once"]
pub struct SemaphorePermit<'a> {
    semaphore: &'a Semaphore,
    permits: usize,
}

impl SemaphorePermit<'_> {
    /// How many permits this holds.
    pub fn num_permits(&self) -> usize {
        self.permits
    }

    /// Lets go of the permits without giving them back: the semaphore has
    /// that many fewer.
    pub fn forget(mut self) {
        self.permits = 0;
    }
}

impl Drop for SemaphorePermit<'_> {
    fn drop(&mut self) {
        self.semaphore.release(self.permits);
    }
}

impl fmt::Debug for SemaphorePermit<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SemaphorePermit")
            .field("permits", &self.permits)
            .finish_non_exhaustive()
    }
}

/// Permits of a [`Semaphore`] that hold a reference to it, and so need not
/// stay where it is; they go back when this is dropped.
#[must_use = "dropping a permit gives it back at once"]
pub struct OwnedSemaphorePermit {
    semaphore: Arc<Semaphore>,
    permits: usize,
}

impl OwnedSemaphorePermit {
    /// How many permits this holds.
    pub fn num_permits(&self) -> usize {
        self.permits
    }

    /// The semaphore the permits are from.
    pub fn semaphore(&self) -> &Arc<Semaphore> {
        &self.semaphore
    }

    /// Lets go of the permits without giving them back: the semaphore has
    /// that many fewer.
    pub fn forget(mut self) {
        self.permits = 0;
    }
}

impl Drop for OwnedSemaphorePermit {
    fn drop(&mut self) {
        self.semaphore.release(self.permits);
    }
}

impl fmt::Debug for OwnedSemaphorePermit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedSemaphorePermit")
            .field("permits", &self.permits)
            .finish_non_exhaustive()
    }
}
