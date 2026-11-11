//! Another executor's reactor, which the sockets that read and write with
//! calls made at once wait on from that executor's threads: [`Reactor`],
//! the [`Registration`] of a socket with it, and the setting,
//! [`set_foreign_reactor`].
//!
//! Polled where no runtime of ruio's runs, a portable socket or listener
//! that finds nothing to read, or no room, has no port to wait on: its wait
//! fails as unsupported. A thread of an executor with a reactor of its own
//! waits there instead, once the reactor is set: the socket is registered
//! with it as it first waits on such a thread, and from then on the reactor
//! says when the socket may be ready, and the call is made again then, as
//! that executor's own sockets are, with no other thread in between.
//!
//! A registration is the reactor's to make, as is what waits on it:
//! [`Registration::poll_io`] keeps one task each way, as a poll-based
//! reactor does. Many tasks can wait on a socket of ruio's, as each read or
//! accept of a shared one does, so ruio gives the reactor a waker of its
//! own each way, which wakes every task that waits so.

use std::fmt;
use std::io;
use std::mem;
use std::os::windows::io::BorrowedSocket;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::task::{Context, Poll, Wake, Waker};

use ruio_iocp::Iocp;
use ruio_iocp::portable::{Socket, events};

use super::portable::Interest;

/// An executor's reactor, which the portable sockets and listeners wait on
/// from that executor's threads once it is set as the foreign reactor
/// ([`set_foreign_reactor`]), where they would have no port to wait on.
pub trait Reactor: Send + Sync {
    /// `socket`, a nonblocking socket that no port has, registered with the
    /// reactor of the executor whose thread calls this, for readiness both
    /// ways; or `None` if the thread is none of that executor's, for the
    /// wait to fail as unsupported.
    ///
    /// The socket stays open for as long as the registration lives: ruio
    /// drops the registration before it closes the socket, or hands it to
    /// its owner.
    ///
    /// # Errors
    ///
    /// What registering it reports. Its waits then fail as unsupported.
    fn register(&self, socket: BorrowedSocket<'_>) -> Option<io::Result<Box<dyn Registration>>>;
}

/// A socket registered with a [`Reactor`].
pub trait Registration: Send + Sync {
    /// Whether a task polled on the calling thread can wait on the reactor:
    /// a thread of the executor's, which runs it. Asked as a wait begins,
    /// once [`poll_io`](Registration::poll_io) has found the socket not
    /// ready; elsewhere the wait fails as unsupported.
    fn is_current(&self) -> bool;

    /// Makes `call`, a nonblocking call on the socket, once the reactor says
    /// the socket may be ready for `interest`, readable or writable, and is
    /// `Pending` until it does, having kept `cx`'s waker to wake then: only
    /// the latest poll's each way, as a poll-based reactor keeps it. The
    /// output is the call's.
    ///
    /// A call that fails with [`io::ErrorKind::WouldBlock`] found the
    /// readiness gone: the reactor forgets it, and waits for more. One that
    /// moves fewer than `len` bytes has drained the socket, or filled it,
    /// and the reactor forgets the readiness too, but the call's output
    /// stands; a `len` of 0 is a call that moves no bytes.
    fn poll_io(
        &self,
        cx: &mut Context<'_>,
        interest: Interest,
        len: usize,
        call: &mut dyn FnMut() -> io::Result<usize>,
    ) -> Poll<io::Result<usize>>;
}

/// The foreign reactor, if one is set.
static REACTOR: Mutex<Option<&'static dyn Reactor>> = Mutex::new(None);

fn setting() -> MutexGuard<'static, Option<&'static dyn Reactor>> {
    REACTOR.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Makes `reactor` the foreign reactor, which the portable sockets and
/// listeners wait on from the threads of its executor, where no runtime of
/// ruio's runs. The process has one, which this replaces; none until one is
/// set.
///
/// A socket is registered with the reactor as it first waits on one of
/// those threads, and keeps the registration until it closes, even should
/// the setting change. Threads where a runtime of ruio's runs wait on their
/// port; on threads that are none of the reactor's executor's, a wait fails
/// as unsupported.
///
/// ```no_run
/// # struct Executor;
/// # impl ruio::io::Reactor for Executor {
/// #     fn register(
/// #         &self,
/// #         _: std::os::windows::io::BorrowedSocket<'_>,
/// #     ) -> Option<std::io::Result<Box<dyn ruio::io::Registration>>> {
/// #         None
/// #     }
/// # }
/// // The reactor of the executor the program runs its tasks on.
/// static EXECUTOR: Executor = Executor;
/// ruio::io::set_foreign_reactor(&EXECUTOR);
/// ```
pub fn set_foreign_reactor(reactor: &'static dyn Reactor) {
    *setting() = Some(reactor);
}

/// Leaves the process without a foreign reactor: sockets not yet registered
/// with one cannot wait wherever no runtime of ruio's runs; see
/// [`set_foreign_reactor`].
pub fn clear_foreign_reactor() {
    *setting() = None;
}

/// The foreign reactor, if one is set; see [`set_foreign_reactor`].
pub fn foreign_reactor() -> Option<&'static dyn Reactor> {
    *setting()
}

/// What a socket has of the foreign reactor: nothing yet, or its
/// registration, or the reactor's refusal, made as it first waited on one
/// of the reactor's threads.
pub(crate) struct Foreign {
    state: OnceLock<State>,
}

enum State {
    Registered(Registered),
    /// The reactor refused the socket: its waits fail as unsupported.
    Refused,
}

impl Foreign {
    pub(crate) const fn new() -> Self {
        Self {
            state: OnceLock::new(),
        }
    }

    /// The registration to wait with on this thread, where no runtime of
    /// ruio's runs: the socket `socket`'s, made now with the foreign reactor
    /// if it has none, should the reactor take it from this thread.
    pub(crate) fn here(&self, socket: BorrowedSocket<'_>) -> Option<&Registered> {
        if Iocp::current().is_some() {
            return None;
        }
        let state = match self.state.get() {
            Some(state) => state,
            None => {
                let state = match foreign_reactor()?.register(socket)? {
                    Ok(registration) => State::Registered(Registered::new(registration)),
                    Err(_) => State::Refused,
                };
                // Another thread may have made one meanwhile, which stays:
                // this one is dropped, and leaves its reactor.
                let _ = self.state.set(state);
                self.state.get()?
            }
        };
        match state {
            State::Registered(registered) => Some(registered),
            State::Refused => None,
        }
    }
}

impl fmt::Debug for Foreign {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = match self.state.get() {
            None => "none",
            Some(State::Registered(_)) => "registered",
            Some(State::Refused) => "refused",
        };
        f.write_str(state)
    }
}

/// A socket's registration with the foreign reactor, and the tasks waiting
/// on it each way.
pub(crate) struct Registered {
    registration: Box<dyn Registration>,
    read: Waiters,
    write: Waiters,
}

impl Registered {
    fn new(registration: Box<dyn Registration>) -> Self {
        Self {
            registration,
            read: Waiters::new(),
            write: Waiters::new(),
        }
    }

    /// Makes `call` once the reactor says the socket may be ready for
    /// `interest`, as [`Registration::poll_io`] does, waking the task of
    /// `cx` then, and every other task that waits on the socket so; or
    /// `None` if the call would have the task wait, and the reactor says the
    /// thread is none of its.
    pub(crate) fn poll_io(
        &self,
        cx: &mut Context<'_>,
        interest: Interest,
        len: usize,
        call: &mut dyn FnMut() -> io::Result<usize>,
    ) -> Option<Poll<io::Result<usize>>> {
        let waiters = if interest.is_writable() {
            &self.write
        } else {
            &self.read
        };
        let wakes = waiters.tasks.wakes();
        let mut fan_out = Context::from_waker(&waiters.waker);
        match self.registration.poll_io(&mut fan_out, interest, len, call) {
            Poll::Ready(done) => Some(Poll::Ready(done)),
            Poll::Pending if self.registration.is_current() => {
                waiters.tasks.wait(cx.waker(), wakes);
                Some(Poll::Pending)
            }
            // Woken, the reactor would find no task waiting: the reactor's
            // waker it keeps wakes nobody.
            Poll::Pending => None,
        }
    }
}

/// The tasks waiting on a registration one way, and the waker the reactor
/// keeps for them, which wakes them all.
struct Waiters {
    tasks: Arc<Tasks>,
    waker: Waker,
}

impl Waiters {
    fn new() -> Self {
        let tasks = Arc::new(Tasks {
            waiting: Mutex::new(Waiting::default()),
            wakes: AtomicU64::new(0),
        });
        let waker = Waker::from(Arc::clone(&tasks));
        Self { tasks, waker }
    }
}

/// The tasks waiting one way, which the reactor's wake wakes, all of them.
struct Tasks {
    waiting: Mutex<Waiting>,
    /// How many times the reactor has woken them, so that a task that comes
    /// to wait just after a wake knows it missed one.
    wakes: AtomicU64,
}

/// The wakers of the tasks waiting: the first apart, so that one task
/// waiting, as is usual, allocates nothing.
#[derive(Default)]
struct Waiting {
    first: Option<Waker>,
    rest: Vec<Waker>,
}

impl Tasks {
    fn lock(&self) -> MutexGuard<'_, Waiting> {
        self.waiting.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// How many times the reactor has woken the tasks: read before the poll
    /// that leaves the reactor's waker with it.
    fn wakes(&self) -> u64 {
        // SeqCst: ordered with the wake's increment and the waiting tasks'
        // lock, as `wait` says.
        self.wakes.load(Ordering::SeqCst)
    }

    /// Has `waker`'s task woken as the reactor next wakes the tasks, its
    /// poll having left the reactor's waker with the reactor after `wakes`
    /// wakes. A wake since then found the task not yet waiting, and so
    /// wakes it now.
    fn wait(&self, waker: &Waker, wakes: u64) {
        {
            let mut waiting = self.lock();
            let waiting = &mut *waiting;
            let known = waiting
                .first
                .iter()
                .chain(&waiting.rest)
                .any(|known| known.will_wake(waker));
            if !known {
                match &waiting.first {
                    None => waiting.first = Some(waker.clone()),
                    Some(_) => waiting.rest.push(waker.clone()),
                }
            }
        }
        // SeqCst: a wake's increment comes before it takes the tasks, so
        // either it took this one, or this load sees it.
        if self.wakes.load(Ordering::SeqCst) != wakes {
            waker.wake_by_ref();
        }
    }
}

impl Wake for Tasks {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        // SeqCst: see `wait`.
        self.wakes.fetch_add(1, Ordering::SeqCst);
        let Waiting { first, rest } = mem::take(&mut *self.lock());
        // Woken once the lock is released: waking runs code of its own.
        for task in first.into_iter().chain(rest) {
            task.wake();
        }
    }
}

/// The interest a wait for AFD `events` has: writable for room to send,
/// readable otherwise.
#[inline]
pub(crate) fn interest(events: u32) -> Interest {
    if events & events::SEND != 0 {
        Interest::WRITABLE
    } else {
        Interest::READABLE
    }
}

/// Whether `socket` has any of `events`, or an end or an error, now, as
/// [`Socket::ready_now`] finds it with one `WSAPoll` that does not wait.
/// The output is the events it has, or `WouldBlock` if it has none: the
/// call a wait for readiness makes through a [`Registration`].
pub(crate) fn level(socket: &Socket, events: u32) -> io::Result<usize> {
    match socket.ready_now(events)? {
        0 => Err(io::ErrorKind::WouldBlock.into()),
        found => Ok(found as usize),
    }
}
