//! Notifying waiting tasks, with no value.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll, Waker, ready};

use super::lock;
use crate::coop;

/// Wakes tasks waiting for something to happen, which they then look at
/// for themselves.
///
/// [`notify_one`](Notify::notify_one) wakes the task that has waited
/// longest, or, if none waits, lets the next to wait go at once: such a
/// permit does not add up, so several notifications with nobody waiting
/// let only one go. [`notify_waiters`](Notify::notify_waiters) wakes every
/// [`Notified`] made before it, polled or not, and leaves no permit. A
/// waiter dropped after `notify_one` chose it, before it saw the
/// notification, passes it on. It works across threads, and so across
/// cores.
///
/// ```
/// use std::sync::Arc;
///
/// use ruio_executor::sync::Notify;
///
/// # ruio_executor::Runtime::new()?.block_on(async {
/// let notify = Arc::new(Notify::new());
/// let waiter = {
///     let notify = Arc::clone(&notify);
///     ruio_executor::spawn(async move { notify.notified().await })
/// };
/// notify.notify_one();
/// waiter.await.unwrap();
/// # });
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct Notify {
    state: Mutex<State>,
}

struct State {
    /// A notification that found nobody waiting, for the next to wait.
    permit: bool,
    /// The waiters that have been polled, by ticket: in the order they
    /// began to wait.
    waiters: BTreeMap<u64, Waiter>,
    next_ticket: u64,
    /// How many times `notify_waiters` has run: a `Notified` made before
    /// the latest finds it different.
    round: u64,
}

struct Waiter {
    waker: Waker,
    /// Set once `notify_one` chose this waiter.
    notified: bool,
}

impl State {
    /// Notifies the waiter that has waited longest and has not been
    /// notified, and returns its waker; or, with none, leaves a permit.
    fn notify_one(&mut self) -> Option<Waker> {
        match self.waiters.values_mut().find(|waiter| !waiter.notified) {
            Some(waiter) => {
                waiter.notified = true;
                Some(waiter.waker.clone())
            }
            None => {
                self.permit = true;
                None
            }
        }
    }
}

impl Notify {
    /// A `Notify` with nobody waiting, and no permit.
    pub const fn new() -> Self {
        Self {
            state: Mutex::new(State {
                permit: false,
                waiters: BTreeMap::new(),
                next_ticket: 0,
                round: 0,
            }),
        }
    }

    /// Waits for a notification: one of [`notify_one`](Notify::notify_one)
    /// after this is first polled, or a permit left before, or one of
    /// [`notify_waiters`](Notify::notify_waiters) after this is made.
    pub fn notified(&self) -> Notified<'_> {
        let round = lock(&self.state).round;
        Notified {
            notify: self,
            round,
            ticket: None,
            done: false,
        }
    }

    /// Wakes the task that has waited longest, or lets the next to wait go
    /// at once.
    pub fn notify_one(&self) {
        let waker = lock(&self.state).notify_one();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Wakes every [`Notified`] made before this call, without leaving a
    /// permit.
    pub fn notify_waiters(&self) {
        let waiters = {
            let mut state = lock(&self.state);
            state.round += 1;
            std::mem::take(&mut state.waiters)
        };
        for waiter in waiters.into_values() {
            waiter.waker.wake();
        }
    }
}

impl Default for Notify {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Notify {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = lock(&self.state);
        f.debug_struct("Notify")
            .field("permit", &state.permit)
            .field("waiters", &state.waiters.len())
            .finish()
    }
}

/// Waits for a notification of a [`Notify`]; made by
/// [`Notify::notified`].
#[must_use = "futures do nothing unless polled"]
pub struct Notified<'a> {
    notify: &'a Notify,
    /// The `notify_waiters` round the future was made in.
    round: u64,
    /// The waiter's ticket, once it waits.
    ticket: Option<u64>,
    /// Set once the notification has been seen.
    done: bool,
}

impl Future for Notified<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(());
        }
        let proceed = ready!(coop::poll_proceed(cx));
        {
            let mut state = lock(&this.notify.state);
            let notified = if state.round != this.round {
                // `notify_waiters` took the waiter out of the list.
                true
            } else if let Some(ticket) = this.ticket {
                let waiter = state
                    .waiters
                    .get_mut(&ticket)
                    .expect("a waiter is listed until it is notified");
                if waiter.notified {
                    state.waiters.remove(&ticket);
                    true
                } else {
                    if !waiter.waker.will_wake(cx.waker()) {
                        waiter.waker = cx.waker().clone();
                    }
                    false
                }
            } else if state.permit {
                state.permit = false;
                true
            } else {
                let ticket = state.next_ticket;
                state.next_ticket += 1;
                let waiter = Waiter {
                    waker: cx.waker().clone(),
                    notified: false,
                };
                state.waiters.insert(ticket, waiter);
                this.ticket = Some(ticket);
                false
            };
            if !notified {
                return Poll::Pending;
            }
        }
        this.ticket = None;
        this.done = true;
        proceed.made_progress();
        Poll::Ready(())
    }
}

impl Drop for Notified<'_> {
    fn drop(&mut self) {
        let Some(ticket) = self.ticket else {
            return;
        };
        let waker = {
            let mut state = lock(&self.notify.state);
            match state.waiters.remove(&ticket) {
                // Chosen by `notify_one` and never told: the next waiter
                // has the notification instead.
                Some(waiter) if waiter.notified => state.notify_one(),
                _ => None,
            }
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl fmt::Debug for Notified<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Notified")
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}
