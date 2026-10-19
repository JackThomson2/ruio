//! The foreign ring: the ring that a portable operation first polled on a
//! thread with no current driver goes to, a thread of another executor
//! say, as the embedder chooses it ([`set_foreign_ring`]); the tickets
//! through which that ring sends such an operation's completion back; and
//! the graves where the operations of futures dropped in flight wait for
//! theirs.
//!
//! The operation never leaves the thread that polls it. Its entry is
//! prepared there and sent to the ring as a request, which the ring's
//! thread queues at its next turn, as an orphan whose payload is the
//! ticket; the completion goes into the ticket and wakes the task, and the
//! future completes the operation where it is. So an operation that is not
//! `Send` can run on the foreign ring as well as one that is: only the
//! entry and the completion, plain data, cross threads.

use std::cell::RefCell;
use std::fmt;
use std::io;
use std::mem;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Poll, Waker};
use std::time::Duration;

use ruio_sys as sys;

use crate::op::{Completion, Operation, Orphan, seen};
use crate::remote::{Remote, RemoteRing, Request};

/// The foreign ring, if one is set.
static FOREIGN: Mutex<Option<Arc<Remote>>> = Mutex::new(None);

/// Moves on with each setting of [`FOREIGN`], so that a thread's copy of it
/// knows when it is stale.
static GENERATION: AtomicU64 = AtomicU64::new(1);

thread_local! {
    /// The thread's copy of the foreign ring, and the generation it was
    /// taken at, 0 before it first is: what the thread's operations read
    /// without the lock, and without counting a reference.
    static CACHE: RefCell<(u64, Option<Arc<Remote>>)> = const { RefCell::new((0, None)) };

    /// The operations of futures dropped on this thread while the kernel
    /// had them, which wait here for their completions.
    static GRAVES: RefCell<Vec<Box<dyn Grave>>> = const { RefCell::new(Vec::new()) };
}

/// Makes `ring` the foreign ring: the ring that a
/// [`PortableOp`](crate::PortableOp) first polled on a thread with no
/// current driver ([`Driver::enter`](crate::Driver::enter)) goes to, as one
/// polled on a thread of another executor, tokio's, say, or of
/// `futures::executor::block_on`, would. The process has one, which this
/// replaces; none until one is set.
///
/// The operation's entry is prepared on the thread that polls it and sent
/// to the ring, which its thread queues at the ring's next turn, as it
/// takes what other threads ask of it, and whose completion it sends back
/// to wake the polling task; the operation itself stays with its future.
/// So the ring must be one that its thread keeps turning, as a runtime's
/// is while it runs: one of the embedder's cores, say, whose ring ruio's
/// executor gives as `Handle::ring`.
///
/// Once the ring closes, operations first polled where no driver is current
/// fail as if none were set, until another is.
pub fn set_foreign_ring(ring: &RemoteRing) {
    set(Some(Arc::clone(ring.shared())));
}

/// Leaves the process without a foreign ring: a portable operation first
/// polled on a thread with no current driver then fails with `ENOSYS`,
/// whose kind is [`io::ErrorKind::Unsupported`], having reached no kernel;
/// see [`set_foreign_ring`]. Operations sent to the ring before go on.
pub fn clear_foreign_ring() {
    set(None);
}

/// The foreign ring, if one is set; see [`set_foreign_ring`].
pub fn foreign_ring() -> Option<RemoteRing> {
    lock().clone().map(RemoteRing::new)
}

fn lock() -> MutexGuard<'static, Option<Arc<Remote>>> {
    FOREIGN.lock().unwrap_or_else(PoisonError::into_inner)
}

fn set(remote: Option<Arc<Remote>>) {
    let previous = {
        let mut foreign = lock();
        let previous = mem::replace(&mut *foreign, remote);
        // Release: a thread that sees the new generation takes the ring
        // under the lock, which orders it in any case.
        GENERATION.fetch_add(1, Ordering::Release);
        previous
    };
    drop(previous);
}

/// Runs `f` with the foreign ring, if one is set: the thread's copy of it,
/// taken again if the setting has changed since. A thread whose locals are
/// gone, as when this runs in a destructor of one, reads the setting
/// itself.
pub(crate) fn with<R>(f: impl FnOnce(Option<&Remote>) -> R) -> R {
    // Acquire: as in `set`.
    let generation = GENERATION.load(Ordering::Acquire);
    let mut f = Some(f);
    let cached = CACHE.try_with(|cache| {
        let mut cache = cache.try_borrow_mut().ok()?;
        if cache.0 != generation {
            *cache = (generation, lock().clone());
        }
        let f = f.take().expect("called once");
        Some(f(cache.1.as_deref()))
    });
    match cached {
        Ok(Some(result)) => result,
        _ => {
            let remote = lock().clone();
            let f = f.take().expect("not called with the thread's copy");
            f(remote.as_deref())
        }
    }
}

/// The error of an operation first polled where no driver is current and
/// no foreign ring is open: `ENOSYS`, which the standard library names
/// [`io::ErrorKind::Unsupported`], unlike any error the kernel gives an
/// operation that reaches it.
pub(crate) fn no_ring() -> io::Error {
    io::Error::from_raw_os_error(libc::ENOSYS)
}

/// Sends `operation`'s entry to the foreign ring, followed there by a
/// linked timeout if `timeout` is set, with a ticket for its completion
/// that wakes `waker`; the ticket comes back, with the ring's ID.
///
/// The entry is prepared only once the ring has taken the request, under
/// its lock, so that what preparing does, such as a close taking over its
/// descriptor, happens only for an entry that reaches the kernel.
///
/// # Errors
///
/// [`no_ring`] if no foreign ring is set, or it has closed: `operation`
/// is then as it was given.
pub(crate) fn send<T: Operation>(
    operation: &mut T,
    timeout: Option<Duration>,
    waker: &Waker,
) -> io::Result<(Arc<Ticket>, u64)> {
    with(|remote| {
        let remote = remote.ok_or_else(no_ring)?;
        let ticket = Arc::new(Ticket::new(waker.clone()));
        let sent = Arc::clone(&ticket);
        remote
            .post_with(operation, move |operation| {
                let mut sqe = sys::io_uring_sqe::default();
                operation.prepare(&mut sqe);
                Request::Submit {
                    sqe,
                    polled: operation.polled(),
                    timeout,
                    ticket: sent,
                }
            })
            .map_err(|_| no_ring())?;
        Ok((ticket, remote.id()))
    })
}

/// What a ring and a future share for an operation the future's thread
/// sent to the ring: where the ring says it has queued the entry, and
/// sends the completion, which the future, keeping the operation, takes.
pub(crate) struct Ticket {
    state: Mutex<TicketState>,
}

struct TicketState {
    stage: Stage,
    /// The task to wake as the completion comes, while one waits.
    waker: Option<Waker>,
    /// Whether the future went before the operation completed, and asked
    /// for it to be cancelled: by the ring as it queues the entry, if it
    /// has not yet, and by a request the future makes otherwise.
    cancel: bool,
}

#[derive(Clone, Copy, Debug)]
enum Stage {
    /// Sent, and not yet taken by the ring.
    Sent,
    /// Queued by the ring, for the operation in slot `user_data`.
    Queued(u64),
    /// Completed by the kernel, or refused by the ring, with this.
    Done(Completion),
}

/// What became of an operation whose future let go of it; see
/// [`Ticket::leave`].
pub(crate) enum Left {
    /// The kernel may still have it: queued in slot `user_data` of its
    /// ring, or about to be, if that is none.
    InFlight(Option<u64>),
    /// The kernel is done with it, with this.
    Done(Completion),
}

impl Ticket {
    fn new(waker: Waker) -> Self {
        Self {
            state: Mutex::new(TicketState {
                stage: Stage::Sent,
                waker: Some(waker),
                cancel: false,
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, TicketState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The completion, if it has come; arranges for `waker` to be woken as
    /// it comes otherwise.
    pub(crate) fn poll(&self, waker: &Waker) -> Poll<Completion> {
        let replaced = {
            let mut state = self.lock();
            if let Stage::Done(completion) = state.stage {
                return Poll::Ready(completion);
            }
            match &state.waker {
                Some(current) if current.will_wake(waker) => None,
                _ => state.waker.replace(waker.clone()),
            }
        };
        // Dropped once the lock is released: dropping a waker runs code.
        drop(replaced);
        Poll::Pending
    }

    /// The completion, if it has come, waking nobody.
    fn completed(&self) -> Option<Completion> {
        match self.lock().stage {
            Stage::Done(completion) => Some(completion),
            Stage::Sent | Stage::Queued(_) => None,
        }
    }

    /// Lets go of the operation, as its future goes: nobody is to be woken
    /// any more, and the operation, if `cancel` says to, is cancelled.
    pub(crate) fn leave(&self, cancel: bool) -> Left {
        let (left, waker) = {
            let mut state = self.lock();
            state.cancel |= cancel;
            let left = match state.stage {
                Stage::Sent => Left::InFlight(None),
                Stage::Queued(user_data) => Left::InFlight(Some(user_data)),
                Stage::Done(completion) => Left::Done(completion),
            };
            (left, state.waker.take())
        };
        drop(waker);
        left
    }

    /// Runs `queue`, which queues the operation's entry on the ring and
    /// returns its `user_data`, and records that; whether the future has
    /// asked for the operation to be cancelled meanwhile comes back with
    /// the `user_data`, for the ring to cancel it. Held across `queue`, the
    /// lock orders the recording against the future's letting go, which so
    /// finds either the slot to cancel, or the request to.
    ///
    /// # Errors
    ///
    /// What `queue` reports, for the ring to complete the operation with.
    pub(crate) fn queue(&self, queue: impl FnOnce() -> io::Result<u64>) -> io::Result<(u64, bool)> {
        let mut state = self.lock();
        let user_data = queue()?;
        state.stage = Stage::Queued(user_data);
        Ok((user_data, state.cancel))
    }

    /// Records the completion, and wakes the task that waits for it.
    fn complete(&self, completion: Completion) {
        let waker = {
            let mut state = self.lock();
            state.stage = Stage::Done(completion);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl fmt::Debug for Ticket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ticket")
            .field("stage", &self.lock().stage)
            .finish_non_exhaustive()
    }
}

/// The payload, in its ring's table, of an operation another thread sent:
/// the ticket that the completion goes to.
pub(crate) struct Relayed(pub(crate) Arc<Ticket>);

impl Orphan for Relayed {
    fn deliver(&mut self, completion: Completion) {
        self.0.complete(completion);
    }
}

/// An operation whose future went while the kernel had it, as a grave
/// keeps it.
trait Grave {
    /// Completes the operation, dropping its output, once its completion
    /// has come; whether it had.
    fn settle(&mut self) -> bool;
}

/// The grave of an operation of type `T`.
struct Buried<T: Operation> {
    /// Taken as it completes.
    operation: Option<T>,
    ticket: Arc<Ticket>,
    /// Whether a linked timeout went with it, which a cancellation then
    /// stands for.
    timed: bool,
}

impl<T: Operation> Grave for Buried<T> {
    fn settle(&mut self) -> bool {
        let Some(completion) = self.ticket.completed() else {
            return false;
        };
        if let Some(operation) = self.operation.take() {
            drop(operation.complete(seen(completion, self.timed)));
        }
        true
    }
}

impl<T: Operation> Drop for Buried<T> {
    fn drop(&mut self) {
        let Some(operation) = self.operation.take() else {
            return;
        };
        match self.ticket.completed() {
            // As it would have settled, so that an output that owns what
            // the kernel made, a descriptor say, lets go of it.
            Some(completion) => drop(operation.complete(seen(completion, self.timed))),
            // The kernel may still use the operation's memory, as when the
            // thread ends first: leaking it is safe.
            None => mem::forget(operation),
        }
    }
}

/// Keeps `operation`, whose future went while the kernel had it, in a
/// grave of the thread until its completion comes through `ticket`: the
/// next settling on the thread after that completes it, as the ring would
/// an orphan, and drops its output, here, where it was made. Every grave
/// is settled first, so a thread keeps no more than the operations still
/// in flight. One left in flight as the thread ends is leaked.
pub(crate) fn bury<T: Operation>(operation: T, ticket: Arc<Ticket>, timed: bool) {
    let mut grave: Option<Box<dyn Grave>> = Some(Box::new(Buried {
        operation: Some(operation),
        ticket,
        timed,
    }));
    settle();
    let _ = GRAVES.try_with(|graves| {
        if let Ok(mut graves) = graves.try_borrow_mut() {
            graves.extend(grave.take());
        }
    });
    // The thread's locals are gone, or in use: what the kernel may still
    // use is leaked, which is safe.
    if let Some(grave) = grave {
        mem::forget(grave);
    }
}

/// Settles the graves of the thread whose operations' completions have
/// come: completes each, drops its output, and lets go of its grave.
pub(crate) fn settle() {
    let graves = GRAVES.try_with(|graves| match graves.try_borrow_mut() {
        // Taken out to settle, as settling runs the operations' own code,
        // which may bury another.
        Ok(mut graves) if !graves.is_empty() => mem::take(&mut *graves),
        _ => Vec::new(),
    });
    let Ok(mut graves) = graves else {
        return;
    };
    if graves.is_empty() {
        return;
    }
    graves.retain_mut(|grave| !grave.settle());
    let mut left = Some(graves);
    let _ = GRAVES.try_with(|graves| {
        if let Ok(mut graves) = graves.try_borrow_mut() {
            let mut left = left.take().expect("put back once");
            left.append(&mut graves);
            *graves = left;
        }
    });
    if let Some(left) = left {
        // As in `bury`.
        mem::forget(left);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::task::Wake;

    use super::*;

    /// Counts its wakes.
    #[derive(Default)]
    struct Count(AtomicUsize);

    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn a_ticket_wakes_its_task_once_and_keeps_the_completion() {
        let count = Arc::new(Count::default());
        let waker = Waker::from(Arc::clone(&count));
        let ticket = Ticket::new(waker.clone());
        assert!(ticket.poll(&waker).is_pending());
        assert_eq!(ticket.queue(|| Ok(7)).unwrap(), (7, false));
        assert!(ticket.poll(&waker).is_pending());
        let completion = Completion::new(3, 0);
        ticket.complete(completion);
        assert_eq!(count.0.load(Ordering::SeqCst), 1);
        assert_eq!(ticket.poll(&waker), Poll::Ready(completion));
    }

    #[test]
    fn a_future_that_leaves_finds_the_slot_or_has_the_ring_cancel() {
        let ticket = Ticket::new(Waker::noop().clone());
        // Before the ring queues it: the ring is to cancel it as it does.
        assert!(matches!(ticket.leave(true), Left::InFlight(None)));
        assert_eq!(ticket.queue(|| Ok(9)).unwrap(), (9, true));

        let ticket = Ticket::new(Waker::noop().clone());
        ticket.queue(|| Ok(4)).unwrap();
        assert!(matches!(ticket.leave(true), Left::InFlight(Some(4))));
        ticket.complete(Completion::new(0, 0));
        assert!(matches!(ticket.leave(true), Left::Done(_)));
    }

    #[test]
    fn a_refusal_leaves_the_ticket_sent() {
        let ticket = Ticket::new(Waker::noop().clone());
        let err = ticket.queue(|| Err(io::Error::from_raw_os_error(libc::EBUSY)));
        assert_eq!(err.unwrap_err().raw_os_error(), Some(libc::EBUSY));
        assert!(matches!(ticket.leave(false), Left::InFlight(None)));
    }

    #[test]
    fn no_ring_is_unsupported() {
        assert_eq!(no_ring().kind(), io::ErrorKind::Unsupported);
    }
}
