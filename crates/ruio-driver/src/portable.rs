//! Operations that are not bound to the ring of one thread: [`PortableOp`],
//! and what keeps one sound as its future moves between threads.

use std::fmt;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker, ready};
use std::time::Duration;

use crate::Driver;
use crate::op::{Completion, Operation, Orphan, Seen, seen};
use crate::remote::{Remote, Request};

/// An operation future that can move between threads: `Send` when its
/// operation and its output are, so that a future that must be, one that
/// `async_trait` boxes or a runtime spawns onto another core, can hold it
/// across an `.await`.
///
/// It queues the operation on the ring of the thread that first polls it,
/// whose driver must be the thread's current one ([`Driver::enter`]), as a
/// runtime's is while it runs; that ring is then the operation's. While the
/// future stays on that thread, as one in a task does, it costs what an
/// [`Op`](crate::Op) does: a poll finds the ring by comparing its ID with
/// the current driver's, and does nothing more.
///
/// A future that moves to another thread while its operation is in flight
/// hands the operation back to its ring, which goes on with it: polled
/// there, the ring's thread completes the operation and sends the output
/// over; dropped there, the ring's thread takes the operation over and
/// cancels it, as dropping an `Op` does. Either goes as a request that the
/// ring takes at its next turn, which the request unparks it for.
///
/// A ring dropped while the future of one of its operations is away, with
/// the operation not yet handed back, finishes it as it shuts down, and
/// leaves its completion for the future, which then completes with it
/// wherever it is. An operation the kernel has not finished when the ring's
/// shutdown gives up on it ([`Builder::shutdown_timeout`](crate::Builder::shutdown_timeout))
/// is leaked, as the kernel may still use its memory, and its future never
/// completes.
///
/// ```
/// use ruio_driver::ops::Nop;
/// use ruio_driver::{Driver, PortableOp};
///
/// fn assert_send<T: Send>(_: &T) {}
///
/// let driver = Driver::new()?;
/// let _entered = driver.enter();
/// let mut op = PortableOp::new(Nop);
/// assert_send(&op);
/// # use std::future::Future;
/// # use std::pin::Pin;
/// # use std::task::{Context, Poll, Waker};
/// let mut cx = Context::from_waker(Waker::noop());
/// let result = loop {
///     if let Poll::Ready(result) = Pin::new(&mut op).poll(&mut cx) {
///         break result;
///     }
///     driver.park(None)?;
/// };
/// result?;
/// # Ok::<(), std::io::Error>(())
/// ```
#[must_use = "an operation does nothing until polled"]
pub struct PortableOp<'a, T: Operation> {
    state: State<T>,
    timeout: Option<Duration>,
    /// Where the descriptor the operation names records the ring it is
    /// queued on, for the descriptor to close after it.
    ring: Option<&'a AtomicU64>,
}

enum State<T: Operation> {
    Unsubmitted(T),
    /// Queued on the ring of the driver with ID `home`, in slot `user_data`.
    Queued {
        operation: T,
        home: u64,
        user_data: u64,
        /// Whether a linked timeout went to the kernel with it.
        timed: bool,
    },
    /// Handed back to the ring of `home` from another thread: the ring's
    /// thread completes it and sends the output to `relay`.
    Forwarded {
        relay: Arc<Relay<T::Output>>,
        home: u64,
        user_data: u64,
    },
    /// Leaked with its ring, which never finished it: it never completes.
    Lost,
    Done,
}

impl<T: Operation> PortableOp<'static, T> {
    /// A future that queues `operation` on the ring of the thread that first
    /// polls it.
    pub fn new(operation: T) -> Self {
        Self {
            state: State::Unsubmitted(operation),
            timeout: None,
            ring: None,
        }
    }
}

impl<'a, T: Operation> PortableOp<'a, T> {
    /// Cancels the operation if it has not completed `duration` after it is
    /// submitted, with a linked timeout, as [`Op::timeout`](crate::Op::timeout)
    /// does. This has no effect once the operation has been submitted.
    pub fn timeout(mut self, duration: Duration) -> Self {
        self.timeout = Some(duration);
        self
    }

    /// Leaves the operation to finish with nobody waiting for it, as
    /// [`Op::detach`](crate::Op::detach) does: submits it on the thread's
    /// current driver if it has not been, and hands it to its ring, which
    /// drops its output once the kernel is done. An operation not yet
    /// submitted, on a thread with no current driver, is dropped, nothing of
    /// it having reached a kernel.
    pub fn detach(mut self) {
        match mem::replace(&mut self.state, State::Done) {
            State::Unsubmitted(operation) => {
                let Some(driver) = Driver::current() else {
                    return;
                };
                let timed = self.timeout.is_some();
                let detached = Seen { operation, timed };
                if let Err((err, detached)) = driver.detach(detached, self.timeout) {
                    drop(detached.complete(Completion::from_error(&err)));
                }
            }
            state => {
                self.state = state;
                self.let_go(false);
            }
        }
    }

    /// Queues the operation on the thread's current driver.
    fn submit(&mut self, cx: &mut Context<'_>) -> Poll<T::Output> {
        let driver = Driver::current().expect(
            "a `PortableOp` is first polled on a thread with a current driver, as a runtime's is",
        );
        let State::Unsubmitted(mut operation) = mem::replace(&mut self.state, State::Done) else {
            unreachable!("only an unsubmitted operation is submitted");
        };
        match driver.queue(&mut operation, self.timeout, cx.waker()) {
            Ok(user_data) => {
                let home = driver.id();
                if let Some(ring) = self.ring {
                    // Relaxed: the descriptor reads it as it is dropped, once
                    // this borrow of it is over, which orders the two.
                    ring.store(home, Ordering::Relaxed);
                }
                self.state = State::Queued {
                    operation,
                    home,
                    user_data,
                    timed: self.timeout.is_some(),
                };
                Poll::Pending
            }
            Err(err) => Poll::Ready(operation.complete(Completion::from_error(&err))),
        }
    }

    /// The output of the queued operation, from its completion.
    #[inline]
    fn complete(&mut self, completion: Completion) -> T::Output {
        let State::Queued {
            operation, timed, ..
        } = mem::replace(&mut self.state, State::Done)
        else {
            unreachable!("only a queued operation completes");
        };
        operation.complete(seen(completion, timed))
    }

    /// Polls the queued operation from away from its ring's thread: from
    /// another thread, which the future has moved to, or from one whose
    /// ring is gone.
    #[cold]
    fn poll_away(&mut self, cx: &mut Context<'_>) -> Poll<T::Output> {
        let State::Queued {
            home, user_data, ..
        } = self.state
        else {
            unreachable!("only a queued operation is polled away");
        };
        // A ring dropped already, here or on another thread, has left the
        // operation's fate.
        if let Some(fate) = stranded::fate(home, user_data) {
            return self.meet(fate);
        }
        // The ring is on another thread, which the future came from: only a
        // future whose operation and output are `Send` crosses threads.
        let State::Queued {
            operation, timed, ..
        } = mem::replace(&mut self.state, State::Done)
        else {
            unreachable!();
        };
        let relay = Arc::new(Relay::new());
        let forward = Forward {
            operation: Some(operation),
            relay: Arc::clone(&relay),
            timed,
        };
        // SAFETY: the forward goes to the ring's thread, which the
        // operation and the relay came from; see `AssertSend`.
        let forward = unsafe { AssertSend::new(forward) };
        let posted = post(home, forward, |forward| Request::Adopt {
            user_data,
            payload: Box::new(forward),
            cancel: false,
        });
        match posted {
            Ok(()) => {
                // From here the output comes through the relay, which the
                // poll registers with before the ring can send it.
                let polled = relay.poll(cx.waker());
                self.state = match polled {
                    Poll::Ready(_) => State::Done,
                    Poll::Pending => State::Forwarded {
                        relay,
                        home,
                        user_data,
                    },
                };
                polled
            }
            Err(forward) => {
                // The ring is closing, and leaves the fate as it goes: this
                // waits for it.
                let forward = forward.into_inner();
                self.state = State::Queued {
                    operation: forward
                        .operation
                        .expect("a forward refused delivers nothing"),
                    home,
                    user_data,
                    timed,
                };
                match stranded::wait(home, user_data, cx.waker()) {
                    Some(fate) => self.meet(fate),
                    None => Poll::Pending,
                }
            }
        }
    }

    /// Completes the queued operation as its ring, dropped first, left it.
    #[cold]
    fn meet(&mut self, fate: Fate) -> Poll<T::Output> {
        match fate {
            Fate::Done(completion) => Poll::Ready(self.complete(completion)),
            Fate::Lost => {
                let State::Queued { operation, .. } = mem::replace(&mut self.state, State::Lost)
                else {
                    unreachable!();
                };
                // The kernel may still use its memory.
                mem::forget(operation);
                Poll::Pending
            }
        }
    }

    /// Lets go of an operation in flight: hands it to its ring, which
    /// cancels it if `cancel` says to and keeps it until the kernel is done.
    fn let_go(&mut self, cancel: bool) {
        match mem::replace(&mut self.state, State::Done) {
            State::Queued {
                operation,
                home,
                user_data,
                timed,
            } => {
                // Detached, it completes as its future would have seen it.
                let operation = Seen {
                    operation,
                    timed: timed && !cancel,
                };
                match Driver::home(home) {
                    Some(driver) => driver.orphan(user_data, operation, cancel),
                    None => hand_back(home, user_data, operation, cancel),
                }
            }
            State::Forwarded {
                relay,
                home,
                user_data,
            } => {
                relay.close();
                if cancel {
                    // Nothing to cancel on a ring that is closing, which
                    // cancels everything itself.
                    let _ = post(home, (), |()| Request::Cancel { user_data });
                }
            }
            state => self.state = state,
        }
    }
}

impl<T: Operation> Future for PortableOp<'_, T> {
    type Output = T::Output;

    /// # Panics
    ///
    /// If first polled on a thread with no current driver, or polled after
    /// it completed.
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T::Output> {
        let this = self.get_mut();
        match &this.state {
            State::Queued {
                home, user_data, ..
            } => {
                let (home, user_data) = (*home, *user_data);
                match Driver::home(home) {
                    Some(driver) => match driver.poll_completion(user_data, cx.waker()) {
                        Some(completion) => Poll::Ready(this.complete(completion)),
                        None => Poll::Pending,
                    },
                    None => this.poll_away(cx),
                }
            }
            State::Unsubmitted(_) => this.submit(cx),
            State::Forwarded { relay, .. } => {
                let output = ready!(relay.poll(cx.waker()));
                this.state = State::Done;
                Poll::Ready(output)
            }
            State::Lost => Poll::Pending,
            State::Done => panic!("`PortableOp` polled after it completed"),
        }
    }
}

impl<T: Operation> Drop for PortableOp<'_, T> {
    fn drop(&mut self) {
        if matches!(self.state, State::Queued { .. } | State::Forwarded { .. }) {
            self.let_go(true);
        }
    }
}

// A `PortableOp` never pins its operation: the memory the kernel uses is not
// inside it, by the contract of `Operation`.
impl<T: Operation> Unpin for PortableOp<'_, T> {}

impl<T: Operation> fmt::Debug for PortableOp<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = match &self.state {
            State::Unsubmitted(_) => "unsubmitted",
            State::Queued { .. } => "queued",
            State::Forwarded { .. } => "forwarded",
            State::Lost => "lost",
            State::Done => "done",
        };
        f.debug_struct("PortableOp")
            .field("state", &state)
            .finish_non_exhaustive()
    }
}

/// Asks the ring of the driver with ID `home` for what `make` makes of
/// `value`, or hands `value` back if the ring is closed or gone.
fn post<V>(home: u64, value: V, make: impl FnOnce(V) -> Request) -> Result<(), V> {
    match Remote::find(home) {
        Some(remote) => remote.post_with(value, make),
        None => Err(value),
    }
}

/// Hands an operation whose future is dropped, or detached, away from its
/// ring's thread back to its ring, or, once the ring has closed, leaves it to
/// the fate the ring records.
#[cold]
fn hand_back<T: Operation>(home: u64, user_data: u64, operation: T, cancel: bool) {
    if let Some(fate) = stranded::fate(home, user_data) {
        return fate.dispose(operation);
    }
    // SAFETY: as in `poll_away`, the operation goes to its ring's thread, or
    // stays with the record that thread disposes of it through.
    let operation = unsafe { AssertSend::new(Some(operation)) };
    let posted = post(home, operation, |operation| Request::Adopt {
        user_data,
        payload: Box::new(operation),
        cancel,
    });
    if let Err(operation) = posted {
        stranded::park(home, user_data, Box::new(operation));
    }
}

/// A value bound for the thread of a ring, which its operation was queued
/// on, made from a [`PortableOp`] on a thread where the ring is not found.
///
/// That is the ring's own thread, if the ring is mid-way through being
/// dropped there and runs the future's code as it goes, or a thread the
/// future moved to from the ring's. A future that moves between threads is
/// `Send`, and is only if its operation and output are, which so are what
/// this carries back; and on the ring's own thread nothing crosses threads.
/// Either way the value is only ever handled on the ring's thread, as a
/// request the ring takes or a record the ring disposes of, or on the
/// thread that made it.
struct AssertSend<P>(P);

impl<P> AssertSend<P> {
    /// # Safety
    ///
    /// `value` goes only to the thread of the ring its operation was queued
    /// on, or stays on this one, as for a [`PortableOp`] that cannot find its
    /// ring on this thread.
    unsafe fn new(value: P) -> Self {
        Self(value)
    }

    fn into_inner(self) -> P {
        self.0
    }
}

// SAFETY: see the type's documentation.
unsafe impl<P> Send for AssertSend<P> {}

impl<P: Orphan> Orphan for AssertSend<P> {
    fn deliver(&mut self, completion: Completion) {
        self.0.deliver(completion);
    }
}

/// An operation handed back to its ring by a future polled on another
/// thread: as it completes, its output goes to the future through `relay`.
struct Forward<T: Operation> {
    /// Taken as it completes.
    operation: Option<T>,
    relay: Arc<Relay<T::Output>>,
    timed: bool,
}

impl<T: Operation> Orphan for Forward<T> {
    fn deliver(&mut self, completion: Completion) {
        if let Some(operation) = self.operation.take() {
            self.relay
                .send(operation.complete(seen(completion, self.timed)));
        }
    }
}

/// Where the thread of a ring sends the output of an operation handed back
/// to it, for the future on another thread to take.
struct Relay<O> {
    state: Mutex<RelayState<O>>,
}

struct RelayState<O> {
    output: Option<O>,
    waker: Option<Waker>,
    /// The future is gone, and the output is dropped as it comes.
    closed: bool,
}

impl<O> Relay<O> {
    fn new() -> Self {
        Self {
            state: Mutex::new(RelayState {
                output: None,
                waker: None,
                closed: false,
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, RelayState<O>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn send(&self, output: O) {
        // Woken, or dropped, once the lock is released: either runs code of
        // its own.
        let (waker, unwanted) = {
            let mut state = self.lock();
            if state.closed {
                (None, Some(output))
            } else {
                state.output = Some(output);
                (state.waker.take(), None)
            }
        };
        drop(unwanted);
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn poll(&self, waker: &Waker) -> Poll<O> {
        let replaced = {
            let mut state = self.lock();
            if let Some(output) = state.output.take() {
                return Poll::Ready(output);
            }
            match &state.waker {
                Some(current) if current.will_wake(waker) => None,
                _ => state.waker.replace(waker.clone()),
            }
        };
        drop(replaced);
        Poll::Pending
    }

    fn close(&self) {
        let (output, waker) = {
            let mut state = self.lock();
            state.closed = true;
            (state.output.take(), state.waker.take())
        };
        drop((output, waker));
    }
}

/// How an operation of a ring ended, as the ring found it while it was
/// dropped, with the operation's future away from it.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Fate {
    /// The kernel finished it, with this completion.
    Done(Completion),
    /// The kernel had not finished it when the ring's shutdown gave up, and
    /// may still use its memory.
    Lost,
}

impl Fate {
    /// Drops `operation` if the kernel is done with it, and leaks it if not.
    fn dispose<P>(self, operation: P) {
        match self {
            Fate::Done(_) => drop(operation),
            Fate::Lost => mem::forget(operation),
        }
    }
}

/// Records, for each operation a ring leaves as it is dropped, how it
/// ended: `stranded` has the `user_data` of each, and its completion if it
/// has one. These are operations of portable futures that are away from
/// the ring's thread, or gone; see [`Table::strand`](crate::table::Table::strand).
pub(crate) fn record(ring: u64, stranded: Vec<(u64, Option<Completion>)>) {
    if !stranded.is_empty() {
        stranded::record(ring, stranded);
    }
}

/// The fates rings record as they are dropped, for the futures of their
/// operations to find wherever they are; rarely holds anything.
pub(crate) mod stranded {
    use std::mem;
    use std::sync::{Mutex, MutexGuard, PoisonError};
    use std::task::Waker;

    use super::Fate;
    use crate::Completion;

    /// The record of one operation.
    struct Strand {
        ring: u64,
        user_data: u64,
        /// Until the ring records it, nothing.
        fate: Option<Fate>,
        /// The future waiting for the fate, which the ring wakes as it
        /// records it.
        waker: Option<Waker>,
        /// The operation of a future dropped before the fate came, which the
        /// ring drops or leaks as it records it.
        parked: Option<Box<dyn Send>>,
    }

    static STRANDED: Mutex<Vec<Strand>> = Mutex::new(Vec::new());

    fn lock() -> MutexGuard<'static, Vec<Strand>> {
        STRANDED.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn find(strands: &[Strand], ring: u64, user_data: u64) -> Option<usize> {
        strands
            .iter()
            .position(|strand| strand.ring == ring && strand.user_data == user_data)
    }

    /// The fate of the operation in slot `user_data` of ring `ring`, with its
    /// record taken, if the ring has recorded it.
    pub(crate) fn fate(ring: u64, user_data: u64) -> Option<Fate> {
        let mut strands = lock();
        let index = find(&strands, ring, user_data)?;
        let fate = strands[index].fate?;
        drop(strands.swap_remove(index));
        Some(fate)
    }

    /// The fate, as for [`fate`], or, if the ring has not recorded it yet,
    /// none, with `waker` to be woken as the ring does.
    pub(super) fn wait(ring: u64, user_data: u64, waker: &Waker) -> Option<Fate> {
        let replaced = {
            let mut strands = lock();
            match find(&strands, ring, user_data) {
                Some(index) if strands[index].fate.is_some() => {
                    let strand = strands.swap_remove(index);
                    drop(strands);
                    return strand.fate;
                }
                Some(index) => {
                    let current = &mut strands[index].waker;
                    match current {
                        Some(current) if current.will_wake(waker) => None,
                        _ => current.replace(waker.clone()),
                    }
                }
                None => {
                    strands.push(Strand {
                        ring,
                        user_data,
                        fate: None,
                        waker: Some(waker.clone()),
                        parked: None,
                    });
                    None
                }
            }
        };
        // Dropped once the lock is released: dropping a waker can run code.
        drop(replaced);
        None
    }

    /// Leaves `operation`, whose future is gone, for the ring to dispose of
    /// as it records the fate; or disposes of it now, if the ring has.
    pub(super) fn park(ring: u64, user_data: u64, operation: Box<dyn Send>) {
        let (fate, operation, waker) = {
            let mut strands = lock();
            match find(&strands, ring, user_data) {
                Some(index) if strands[index].fate.is_some() => {
                    let strand = strands.swap_remove(index);
                    (strand.fate, Some(operation), strand.waker)
                }
                Some(index) => {
                    strands[index].parked = Some(operation);
                    (None, None, strands[index].waker.take())
                }
                None => {
                    strands.push(Strand {
                        ring,
                        user_data,
                        fate: None,
                        waker: None,
                        parked: Some(operation),
                    });
                    (None, None, None)
                }
            }
        };
        drop(waker);
        if let (Some(fate), Some(operation)) = (fate, operation) {
            fate.dispose(operation);
        }
    }

    /// Records the fates of the operations ring `ring` leaves; see
    /// [`record`](super::record).
    pub(super) fn record(ring: u64, stranded: Vec<(u64, Option<Completion>)>) {
        let mut wakers = Vec::new();
        let mut parked = Vec::new();
        {
            let mut strands = lock();
            for (user_data, completion) in stranded {
                let fate = completion.map_or(Fate::Lost, Fate::Done);
                match find(&strands, ring, user_data) {
                    Some(index) if strands[index].parked.is_some() => {
                        let mut strand = strands.swap_remove(index);
                        parked.extend(strand.parked.take().map(|operation| (fate, operation)));
                        wakers.extend(strand.waker.take());
                    }
                    Some(index) => {
                        strands[index].fate = Some(fate);
                        wakers.extend(strands[index].waker.take());
                    }
                    None => strands.push(Strand {
                        ring,
                        user_data,
                        fate: Some(fate),
                        waker: None,
                        parked: None,
                    }),
                }
            }
        }
        // Outside the lock: what an operation's drop and a waker run is code
        // of their own.
        for (fate, operation) in parked {
            fate.dispose(operation);
        }
        for waker in mem::take(&mut wakers) {
            waker.wake();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::{Wake, Waker};

    use super::*;

    /// Counts its wakes.
    #[derive(Default)]
    struct Count(AtomicUsize);

    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Sets its flag as it is dropped. It has no size, so a test that leaks
    /// one allocates nothing that a leak checker would report.
    struct Flag(&'static AtomicBool);

    impl Drop for Flag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    /// Rings no driver has, so that tests running at once cannot meet.
    const RING: u64 = u64::MAX - 1;

    #[test]
    fn a_future_waiting_for_its_fate_is_woken_as_the_ring_records_it() {
        let count = Arc::new(Count::default());
        let waker = Waker::from(Arc::clone(&count));
        assert!(stranded::wait(RING, 1, &waker).is_none());
        assert!(stranded::wait(RING, 1, &waker).is_none(), "registered once");
        assert!(stranded::fate(RING, 1).is_none());
        let completion = Completion::new(7, 0);
        record(RING, vec![(1, Some(completion))]);
        assert_eq!(count.0.load(Ordering::SeqCst), 1);
        match stranded::fate(RING, 1) {
            Some(Fate::Done(done)) => assert_eq!(done, completion),
            other => panic!("unexpected fate {other:?}"),
        }
        assert!(stranded::fate(RING, 1).is_none(), "the record is taken");
    }

    #[test]
    fn a_parked_operation_is_dropped_once_done_and_leaked_once_lost() {
        static DONE: AtomicBool = AtomicBool::new(false);
        static LOST: AtomicBool = AtomicBool::new(false);
        // Parked before the ring records the fates.
        stranded::park(RING - 1, 1, Box::new(Flag(&DONE)));
        stranded::park(RING - 1, 2, Box::new(Flag(&LOST)));
        assert!(!DONE.load(Ordering::SeqCst));
        record(RING - 1, vec![(1, Some(Completion::new(0, 0))), (2, None)]);
        assert!(
            DONE.load(Ordering::SeqCst),
            "dropped once the kernel is done"
        );
        assert!(
            !LOST.load(Ordering::SeqCst),
            "leaked while the kernel may use it"
        );
        assert!(stranded::fate(RING - 1, 1).is_none());
        assert!(stranded::fate(RING - 1, 2).is_none());
    }

    #[test]
    fn parked_after_its_fate_an_operation_is_disposed_of_at_once() {
        static DONE: AtomicBool = AtomicBool::new(false);
        static LOST: AtomicBool = AtomicBool::new(false);
        record(RING - 2, vec![(1, Some(Completion::new(0, 0))), (2, None)]);
        stranded::park(RING - 2, 1, Box::new(Flag(&DONE)));
        assert!(DONE.load(Ordering::SeqCst));
        stranded::park(RING - 2, 2, Box::new(Flag(&LOST)));
        assert!(!LOST.load(Ordering::SeqCst));
        assert!(stranded::fate(RING - 2, 1).is_none());
    }
}
