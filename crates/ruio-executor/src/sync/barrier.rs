//! A barrier, at which a number of tasks meet before any goes on.
//!
//! The barrier counts the tasks that have arrived, behind a lock with the
//! wakers of those waiting, and keeps the number of the generation now
//! gathering in an atomic beside the lock, which changes only with the
//! lock held. The last to arrive moves the generation on, takes every
//! waker out, unlocks and wakes them all; so a waiter polled again has
//! only to read the generation, with no lock, to see that it has been let
//! through. The wakers are kept by ticket, so that a wait dropped before
//! its generation is released takes back exactly its own arrival and
//! waker, and the others keep waiting for a real last one.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker, ready};

use super::lock;
use crate::coop;

/// A barrier: `n` tasks wait at it, and none goes on until every one has
/// arrived.
///
/// The last to arrive lets the others through and is told so, as the
/// leader; the barrier is then ready for the next `n`, which form a
/// generation of their own. A [`Wait`] dropped before its generation is
/// let through takes its arrival back, so the others wait for a real last
/// one rather than go through one short. It works across threads, and so
/// across cores, and a wait that is ready at once spends a step of the
/// task's budget.
///
/// ```
/// use std::sync::Arc;
///
/// use ruio_executor::sync::Barrier;
///
/// # ruio_executor::Runtime::new()?.block_on(async {
/// let barrier = Arc::new(Barrier::new(3));
/// let tasks: Vec<_> = (0..3)
///     .map(|_| {
///         let barrier = Arc::clone(&barrier);
///         ruio_executor::spawn(async move { barrier.wait().await.is_leader() })
///     })
///     .collect();
/// let mut leaders = 0;
/// for task in tasks {
///     if task.await.unwrap() {
///         leaders += 1;
///     }
/// }
/// assert_eq!(leaders, 1);
/// # });
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct Barrier {
    /// How many arrivals let a generation through.
    n: usize,
    /// The generation now gathering: moved on only with `state` locked,
    /// and read without it by a waiter looking to see whether its own has
    /// been let through.
    generation: AtomicU64,
    state: Mutex<State>,
}

struct State {
    /// How many have arrived in the generation now gathering.
    arrived: usize,
    /// The wakers of the arrivals that wait, by ticket.
    waiters: BTreeMap<u64, Waker>,
    next_ticket: u64,
}

/// A wait's place in its generation.
#[derive(Clone, Copy)]
struct Arrival {
    /// The generation the wait arrived in.
    generation: u64,
    /// Its ticket among the waiters.
    ticket: u64,
}

impl Barrier {
    /// A barrier that lets every `n` tasks through together; one of none
    /// lets each through alone, as one of one does.
    pub const fn new(n: usize) -> Self {
        Self {
            n: if n == 0 { 1 } else { n },
            generation: AtomicU64::new(0),
            state: Mutex::new(State {
                arrived: 0,
                waiters: BTreeMap::new(),
                next_ticket: 0,
            }),
        }
    }

    /// Arrives at the barrier, and waits for the rest of the generation.
    pub fn wait(&self) -> Wait<'_> {
        Wait {
            barrier: self,
            arrival: None,
            done: false,
        }
    }

    /// Counts an arrival. The last of a generation lets it through and
    /// gets no place among the waiters; any other takes one, to have
    /// `waker` woken when the generation is let through.
    fn arrive(&self, waker: &Waker) -> Option<Arrival> {
        let mut state = lock(&self.state);
        state.arrived += 1;
        if state.arrived < self.n {
            // Relaxed: the generation changes only with the lock held,
            // which orders every change before this.
            let generation = self.generation.load(Ordering::Relaxed);
            let ticket = state.next_ticket;
            state.next_ticket += 1;
            state.waiters.insert(ticket, waker.clone());
            return Some(Arrival { generation, ticket });
        }
        state.arrived = 0;
        let waiters = mem::take(&mut state.waiters);
        // Release: a waiter that sees the generation move on sees what
        // every task of it did before arriving, which the lock orders
        // before this.
        self.generation.fetch_add(1, Ordering::Release);
        drop(state);
        for waker in waiters.into_values() {
            waker.wake();
        }
        None
    }

    /// Whether the generation of `arrival` has been let through; if not,
    /// has `waker` woken when it is, in place of the one listed if that
    /// would wake another task.
    #[cold]
    fn refresh(&self, arrival: Arrival, waker: &Waker) -> bool {
        let mut state = lock(&self.state);
        // Relaxed: the lock orders the move that let the generation
        // through, if any, before this.
        if self.generation.load(Ordering::Relaxed) != arrival.generation {
            return true;
        }
        let listed = state
            .waiters
            .get_mut(&arrival.ticket)
            .expect("a waiter is listed until its generation is let through");
        if !listed.will_wake(waker) {
            *listed = waker.clone();
        }
        false
    }

    /// Takes back the arrival of a wait dropped before its generation was
    /// let through; a dropped wait of one that was leaves nothing to take.
    #[cold]
    fn leave(&self, arrival: Arrival) {
        let mut state = lock(&self.state);
        // Relaxed: as in `refresh`.
        if self.generation.load(Ordering::Relaxed) != arrival.generation {
            return;
        }
        state.arrived -= 1;
        state.waiters.remove(&arrival.ticket);
    }
}

impl fmt::Debug for Barrier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let arrived = lock(&self.state).arrived;
        f.debug_struct("Barrier")
            .field("n", &self.n)
            .field("arrived", &arrived)
            .finish_non_exhaustive()
    }
}

/// Waits for the rest of a [`Barrier`]'s generation; made by
/// [`Barrier::wait`]. Dropped before the generation is let through, it
/// takes its arrival back.
///
/// # Panics
///
/// If polled again after it completed.
#[must_use = "futures do nothing unless polled"]
pub struct Wait<'a> {
    barrier: &'a Barrier,
    /// The wait's place in its generation, once it has arrived and is not
    /// the leader.
    arrival: Option<Arrival>,
    /// Set once the generation has been let through.
    done: bool,
}

impl Future for Wait<'_> {
    type Output = BarrierWaitResult;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<BarrierWaitResult> {
        let this = self.get_mut();
        assert!(!this.done, "`Wait` polled after it completed");
        let proceed = ready!(coop::poll_proceed(cx));
        let is_leader = match this.arrival {
            None => match this.barrier.arrive(cx.waker()) {
                None => true,
                Some(arrival) => {
                    this.arrival = Some(arrival);
                    return Poll::Pending;
                }
            },
            Some(arrival) => {
                // Acquire: with the generation moved on, what every task
                // of it did before arriving is seen, as the leader's move
                // published it.
                let generation = this.barrier.generation.load(Ordering::Acquire);
                if generation == arrival.generation && !this.barrier.refresh(arrival, cx.waker()) {
                    return Poll::Pending;
                }
                false
            }
        };
        this.arrival = None;
        this.done = true;
        proceed.made_progress();
        Poll::Ready(BarrierWaitResult { is_leader })
    }
}

impl Drop for Wait<'_> {
    #[inline]
    fn drop(&mut self) {
        if let Some(arrival) = self.arrival.take() {
            self.barrier.leave(arrival);
        }
    }
}

impl fmt::Debug for Wait<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Wait")
            .field("arrived", &self.arrival.is_some())
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

/// What a [`Wait`] completes with: whether the task was the last of its
/// generation to arrive.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct BarrierWaitResult {
    is_leader: bool,
}

impl BarrierWaitResult {
    /// Whether the task was the last of its generation to arrive, which is
    /// true for exactly one of the waits let through together.
    #[inline]
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

impl fmt::Debug for BarrierWaitResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BarrierWaitResult")
            .field("is_leader", &self.is_leader)
            .finish()
    }
}
