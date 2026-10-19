//! Signals, delivered to tasks: `SIGINT` from the terminal, `SIGTERM` from a
//! supervisor, `SIGHUP` asking for a reload, and the rest.
//!
//! [`signal`] gives a [`Signal`] for a [`SignalKind`], whose
//! [`recv`](Signal::recv) completes each time the signal arrives; [`ctrl_c`]
//! waits for `SIGINT`. A process-wide handler is installed for each kind
//! once, the first time a task asks for it, and stays; the handler it
//! replaces, if a library installed one, is called after it, so the two
//! coexist. Signals of one kind that arrive while nobody is receiving
//! coalesce into one: a `Signal` says that the signal has arrived since it
//! was last asked, not how many times.
//!
//! The handler does two things, both safe in a signal handler: it sets the
//! kind's bit in one atomic word, and writes to an eventfd the process has
//! for its signals, whose count cannot fill as a pipe can. Each runtime with
//! a listener keeps one read of the eventfd on its ring, which costs nothing
//! until a signal comes; the runtime whose read takes the count swaps the
//! word for zero and hands each kind in it to the kind's `watch` channel,
//! whose receivers, on this runtime and every other, are the listeners. So
//! a signal costs its handler an atomic operation and a system call, the
//! ring one completion, and each listener one wake, wherever it runs.
//!
//! A listener made where no runtime runs, on a thread of another executor's
//! say, has no runtime's dispatcher to count on, and hands signals on
//! itself while it is received: it takes the eventfd's count with a
//! nonblocking read, and waits for the eventfd to be readable with a poll
//! on the foreign ring
//! ([`set_foreign_ring`](crate::uring::set_foreign_ring)), as any portable
//! wait polled there does. Listeners are `Send`, and wherever a listener is
//! polled, the dispatcher of the runtime that made it, or its own wait,
//! wakes it.

use std::cell::Cell;
use std::fmt;
use std::future::poll_fn;
use std::io;
use std::mem;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicI32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock, PoisonError};
use std::task::{Context, Poll};

use libc::c_int;
use ruio_driver::ops::Read;
use ruio_driver::{Driver, PortableFd, ReadyWait};
use ruio_executor::sync::watch;

/// What a listener dispatching for itself waits for the eventfd to be.
const READABLE: u32 = libc::POLLIN as u32;

/// How many signal numbers Linux has, `1..=64`; the word of pending kinds
/// has a bit for each.
const SIGNALS: usize = 64;

/// A kind of signal, by its number.
///
/// The constructors name the signals a program commonly listens for;
/// [`from_raw`](SignalKind::from_raw) takes any number, such as a real-time
/// signal's.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SignalKind(c_int);

impl SignalKind {
    /// The signal with number `signum`, as `libc` names them.
    pub const fn from_raw(signum: c_int) -> Self {
        Self(signum)
    }

    /// The signal's number.
    pub const fn as_raw_value(self) -> c_int {
        self.0
    }

    /// `SIGALRM`: a timer set with `alarm(2)` has expired.
    pub const fn alarm() -> Self {
        Self(libc::SIGALRM)
    }

    /// `SIGCHLD`: a child process has stopped or ended.
    pub const fn child() -> Self {
        Self(libc::SIGCHLD)
    }

    /// `SIGHUP`: the terminal has hung up, or, for a daemon, a reload is
    /// asked for.
    pub const fn hangup() -> Self {
        Self(libc::SIGHUP)
    }

    /// `SIGINT`: the interrupt from the terminal, Ctrl-C.
    pub const fn interrupt() -> Self {
        Self(libc::SIGINT)
    }

    /// `SIGIO`: I/O is possible on a descriptor set up for it.
    pub const fn io() -> Self {
        Self(libc::SIGIO)
    }

    /// `SIGPIPE`: a write to a pipe or socket nobody reads. Rust programs
    /// ignore it by default, so a write fails with `EPIPE` instead.
    pub const fn pipe() -> Self {
        Self(libc::SIGPIPE)
    }

    /// `SIGQUIT`: the quit from the terminal, Ctrl-\.
    pub const fn quit() -> Self {
        Self(libc::SIGQUIT)
    }

    /// `SIGTERM`: a request to end, as a supervisor sends.
    pub const fn terminate() -> Self {
        Self(libc::SIGTERM)
    }

    /// `SIGUSR1`: the first signal left to the program.
    pub const fn user_defined1() -> Self {
        Self(libc::SIGUSR1)
    }

    /// `SIGUSR2`: the second signal left to the program.
    pub const fn user_defined2() -> Self {
        Self(libc::SIGUSR2)
    }

    /// `SIGWINCH`: the terminal's window has changed size.
    pub const fn window_change() -> Self {
        Self(libc::SIGWINCH)
    }

    /// The signal's index in the word of pending kinds, if it can be
    /// listened for: a number in `1..=64` that is not `SIGKILL` or
    /// `SIGSTOP`, which cannot be caught, and not one the kernel sends
    /// synchronously for a fault in the program, `SIGSEGV`, `SIGBUS`,
    /// `SIGILL` or `SIGFPE`, which a task could not meaningfully receive.
    fn index(self) -> io::Result<usize> {
        let caught = (1..=SIGNALS as c_int).contains(&self.0)
            && !matches!(
                self.0,
                libc::SIGKILL
                    | libc::SIGSTOP
                    | libc::SIGSEGV
                    | libc::SIGBUS
                    | libc::SIGILL
                    | libc::SIGFPE
            );
        if caught {
            Ok((self.0 - 1) as usize)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("signal {} cannot be listened for", self.0),
            ))
        }
    }
}

impl From<c_int> for SignalKind {
    fn from(signum: c_int) -> Self {
        Self(signum)
    }
}

impl From<SignalKind> for c_int {
    fn from(kind: SignalKind) -> Self {
        kind.0
    }
}

/// The eventfd the handlers write to; set before any handler is installed.
static EVENT: AtomicI32 = AtomicI32::new(-1);

/// A bit for each kind of signal that has arrived and not been handed on.
static PENDING: AtomicU64 = AtomicU64::new(0);

/// What the kernel did with each signal before a handler was installed for
/// it: the handler, as a `usize`, which the installed one calls after
/// itself, or `SIG_DFL` or `SIG_IGN`, which it does not.
static PREVIOUS: [AtomicUsize; SIGNALS] = [const { AtomicUsize::new(libc::SIG_DFL) }; SIGNALS];

/// The flags of each previous handler, for how to call it.
static PREVIOUS_FLAGS: [AtomicI32; SIGNALS] = [const { AtomicI32::new(0) }; SIGNALS];

/// What the process has for its signals, made at the first `signal`.
struct Globals {
    /// Written by the handlers, read by the runtimes' dispatchers and by
    /// the listeners that dispatch for themselves, which wait for it as a
    /// portable descriptor.
    event: PortableFd,
    /// The channel of each kind, made as its handler is installed.
    senders: [OnceLock<watch::Sender<u64>>; SIGNALS],
    /// Holds the installation of a handler, so that two tasks asking for
    /// one kind at once install it once.
    install: Mutex<()>,
}

static GLOBALS: OnceLock<Globals> = OnceLock::new();

fn globals() -> io::Result<&'static Globals> {
    if let Some(globals) = GLOBALS.get() {
        return Ok(globals);
    }
    // SAFETY: `eventfd` makes a new descriptor, which nothing else owns.
    let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the descriptor is new, and owned by nobody else.
    let event = PortableFd::new(unsafe { OwnedFd::from_raw_fd(fd) });
    let globals = GLOBALS.get_or_init(|| Globals {
        event,
        senders: [const { OnceLock::new() }; SIGNALS],
        install: Mutex::new(()),
    });
    // Another thread's eventfd may have won; the handlers use the one kept.
    // Release: a handler that reads the descriptor sees it made.
    EVENT.store(globals.event.as_raw_fd(), Ordering::Release);
    Ok(globals)
}

impl Globals {
    /// The channel of `kind`, installing its handler first if none is.
    fn sender(&'static self, kind: SignalKind) -> io::Result<&'static watch::Sender<u64>> {
        let index = kind.index()?;
        if let Some(sender) = self.senders[index].get() {
            return Ok(sender);
        }
        let _installing = self.install.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(sender) = self.senders[index].get() {
            return Ok(sender);
        }
        install(kind.0, index)?;
        let (sender, _) = watch::channel(0);
        Ok(self.senders[index].get_or_init(|| sender))
    }
}

/// Installs [`handler`] for `signum`, remembering what was there before.
fn install(signum: c_int, index: usize) -> io::Result<()> {
    // SAFETY: a zeroed `sigaction` is a valid one, filled in below.
    let mut previous: libc::sigaction = unsafe { mem::zeroed() };
    // SAFETY: `previous` is a valid place for the current action; no new
    // action is given.
    if unsafe { libc::sigaction(signum, std::ptr::null(), &mut previous) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // Relaxed: the handler reads these after the installation below, a
    // system call, which orders them before it.
    PREVIOUS[index].store(previous.sa_sigaction, Ordering::Relaxed);
    PREVIOUS_FLAGS[index].store(previous.sa_flags, Ordering::Relaxed);

    // SAFETY: as above.
    let mut action: libc::sigaction = unsafe { mem::zeroed() };
    action.sa_sigaction =
        handler as extern "C" fn(c_int, *mut libc::siginfo_t, *mut libc::c_void) as usize;
    // Blocking calls elsewhere in the process go on after the handler, as
    // they would with no handler; the one that takes the signal's
    // information is called with it.
    action.sa_flags = libc::SA_SIGINFO | libc::SA_RESTART;
    // SAFETY: `sa_mask` is a valid place for an empty set.
    unsafe { libc::sigemptyset(&mut action.sa_mask) };
    // SAFETY: `action` is a valid action, and the old one is not asked for.
    if unsafe { libc::sigaction(signum, &action, std::ptr::null_mut()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// The handler of every kind listened for: notes the kind, wakes a
/// dispatcher, and calls the handler it replaced. Everything here is safe in
/// a signal handler: atomic operations, `write(2)`, and a call.
extern "C" fn handler(signum: c_int, info: *mut libc::siginfo_t, context: *mut libc::c_void) {
    let index = (signum - 1) as usize;
    // Release, with the dispatcher's swap acquiring: the bit is all that is
    // published, and a swap sees every bit set before it in any case.
    PENDING.fetch_or(1 << index, Ordering::Release);
    let event = EVENT.load(Ordering::Acquire);
    let one: u64 = 1;
    // SAFETY: eight bytes of a `u64`, which an eventfd write takes; the
    // descriptor is the process's, open for as long as it runs. An error,
    // which only a full count could cause, is ignored: the bit is set, and
    // a full count has readers coming.
    unsafe { libc::write(event, (&raw const one).cast(), 8) };

    let previous = PREVIOUS[index].load(Ordering::Relaxed);
    if previous == libc::SIG_DFL || previous == libc::SIG_IGN {
        return;
    }
    if PREVIOUS_FLAGS[index].load(Ordering::Relaxed) & libc::SA_SIGINFO != 0 {
        // SAFETY: the kernel gave this pointer back as the handler of a
        // `SA_SIGINFO` action, which has this signature.
        let previous: extern "C" fn(c_int, *mut libc::siginfo_t, *mut libc::c_void) =
            unsafe { mem::transmute(previous) };
        previous(signum, info, context);
    } else {
        // SAFETY: as above, for an action without `SA_SIGINFO`.
        let previous: extern "C" fn(c_int) = unsafe { mem::transmute(previous) };
        previous(signum);
    }
}

thread_local! {
    /// The runtime whose dispatcher runs on this thread, by its driver's
    /// identity, which no other runtime ever has.
    static DISPATCHING: Cell<Option<u64>> = const { Cell::new(None) };
}

/// Spawns the thread's runtime's dispatcher, unless it runs already.
///
/// # Panics
///
/// If the thread is not inside a runtime's `block_on`.
fn start_dispatcher(globals: &'static Globals) {
    let driver = crate::driver();
    let id = driver.id();
    DISPATCHING.with(|dispatching| {
        if dispatching.get() == Some(id) {
            return;
        }
        dispatching.set(Some(id));
        drop(crate::spawn(dispatch(globals)));
    });
}

/// The runtime's dispatcher: reads the eventfd, and hands each kind of
/// signal that has arrived to its channel, for the listeners everywhere.
///
/// Every runtime with a listener reads the one eventfd, and so does every
/// listener dispatching for itself; the kernel gives a count to one of
/// them, and the rest wait on. Whichever gets it swaps the pending word for
/// zero, so each arrival is handed on once.
async fn dispatch(globals: &'static Globals) {
    let fd: RawFd = globals.event.as_raw_fd();
    let mut buf = Vec::with_capacity(8);
    loop {
        let (read, taken) = crate::driver().op(Read::new(fd, buf)).await;
        buf = taken;
        buf.clear();
        match read {
            Ok(_) => {}
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            // The ring is shutting down, or the descriptor is gone with the
            // process: nothing more to dispatch here.
            Err(_) => return,
        }
        hand_on(globals);
    }
}

/// Hands each kind of signal that has arrived, since whoever took the
/// eventfd's count last did this, to its channel.
fn hand_on(globals: &Globals) {
    // AcqRel: the bits set before the count was written are seen, and
    // taken once.
    let mut pending = PENDING.swap(0, Ordering::AcqRel);
    while pending != 0 {
        let index = pending.trailing_zeros() as usize;
        pending &= !(1 << index);
        if let Some(sender) = globals.senders[index].get() {
            sender.send_modify(|arrivals| *arrivals += 1);
        }
    }
}

/// Takes the eventfd's count with one nonblocking `read(2)`: whether there
/// was one, which signals have arrived since it was last taken, or none.
fn take_count(globals: &Globals) -> io::Result<bool> {
    let mut count = 0_u64;
    loop {
        // SAFETY: an eventfd's read writes the eight bytes of the `u64` it
        // is given.
        let read = unsafe {
            libc::read(
                globals.event.as_raw_fd(),
                (&raw mut count).cast(),
                mem::size_of::<u64>(),
            )
        };
        if read >= 0 {
            return Ok(true);
        }
        let err = io::Error::last_os_error();
        match err.kind() {
            io::ErrorKind::WouldBlock => return Ok(false),
            io::ErrorKind::Interrupted => {}
            _ => return Err(err),
        }
    }
}

/// Listens for `kind`: a [`Signal`] whose [`recv`](Signal::recv) completes
/// each time the signal arrives from now on. The signal's handler is
/// installed if it is not already, and the handler it replaces is called
/// after it.
///
/// Inside a runtime's `block_on`, the runtime's dispatcher hands the
/// signals on, and is started if it has not been. Elsewhere, on a thread of
/// another executor's say, the listener hands them on itself as it is
/// received, waiting with a poll on the foreign ring
/// ([`set_foreign_ring`](crate::uring::set_foreign_ring)), or on the ring of
/// a driver the thread has made current.
///
/// # Errors
///
/// [`io::ErrorKind::InvalidInput`] for a kind that cannot be listened for:
/// `SIGKILL` and `SIGSTOP`, which cannot be caught, the fault signals
/// `SIGSEGV`, `SIGBUS`, `SIGILL` and `SIGFPE`, and numbers outside `1..=64`;
/// [`io::ErrorKind::Unsupported`] where no runtime runs and no ring could
/// wait for the listener, with no driver current and no foreign ring set,
/// before anything is installed; otherwise what making the eventfd or
/// installing the handler reports.
///
/// ```no_run
/// use ruio::signal::{SignalKind, signal};
///
/// # ruio::Runtime::new().unwrap().block_on(async {
/// let mut reload = signal(SignalKind::hangup())?;
/// while reload.recv().await.is_some() {
///     println!("reloading");
/// }
/// # Ok::<(), std::io::Error>(())
/// # }).unwrap();
/// ```
pub fn signal(kind: SignalKind) -> io::Result<Signal> {
    let in_runtime = ruio_executor::try_driver().is_some();
    if !in_runtime && Driver::current().is_none() && ruio_driver::foreign_ring().is_none() {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "no ruio runtime runs on this thread, and no foreign ring is set to wait on",
        ));
    }
    let globals = globals()?;
    let sender = globals.sender(kind)?;
    let dispatch = if in_runtime {
        start_dispatcher(globals);
        None
    } else {
        Some(ReadyWait::new())
    };
    Ok(Signal {
        kind,
        arrivals: sender.subscribe(),
        dispatch,
    })
}

/// Waits for `SIGINT`, the interrupt from the terminal: Ctrl-C. The
/// listener is made, and its handler installed, as the future is first
/// polled, inside a runtime or not, as [`signal`] makes one.
///
/// # Errors
///
/// What [`signal`] reports.
///
/// ```no_run
/// # ruio::Runtime::new().unwrap().block_on(async {
/// ruio::signal::ctrl_c().await?;
/// println!("shutting down");
/// # Ok::<(), std::io::Error>(())
/// # }).unwrap();
/// ```
pub async fn ctrl_c() -> io::Result<()> {
    signal(SignalKind::interrupt())?.recv().await;
    Ok(())
}

/// A listener for one kind of signal, made by [`signal`]. It is `Send` and
/// `Sync`, and receives on whichever thread polls it.
///
/// Signals that arrive while the listener is not receiving coalesce: the
/// next [`recv`](Signal::recv) completes once, however many came. One that
/// arrived before the listener was made is not delivered to it.
///
/// One made inside a runtime is handed its signals by that runtime's
/// dispatcher, wherever it is polled, for as long as the runtime runs. One
/// made where no runtime runs hands them on itself as it is received, for
/// itself and every other listener: it takes the eventfd's count with a
/// nonblocking read, and otherwise waits for the eventfd, as a portable
/// descriptor, on the ring of the thread that polls it, or the foreign
/// ring.
#[must_use = "a listener does nothing until it receives"]
pub struct Signal {
    kind: SignalKind,
    /// How many times the signal has arrived, as the kind's channel counts;
    /// a receive is a change of the count.
    arrivals: watch::Receiver<u64>,
    /// For a listener made where no runtime runs, the wait for the eventfd
    /// with which it hands the signals on itself.
    dispatch: Option<ReadyWait>,
}

impl Signal {
    /// The kind listened for.
    pub fn kind(&self) -> SignalKind {
        self.kind
    }

    /// Waits for the signal to arrive, once, however many times it does
    /// meanwhile. `None` comes only for a listener made where no runtime
    /// runs that can wait no more, as when the foreign ring it waited on is
    /// cleared or closes and its thread has no driver of its own; the
    /// channel behind the listener lives as long as the process.
    pub async fn recv(&mut self) -> Option<()> {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    /// Whether the signal has arrived since the last receive; arranges for
    /// the task to be woken when it does otherwise.
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<()>> {
        loop {
            if let Poll::Ready(changed) = self.arrivals.poll_changed(cx) {
                return Poll::Ready(changed.ok());
            }
            let (Some(wait), Some(globals)) = (&mut self.dispatch, GLOBALS.get()) else {
                return Poll::Pending;
            };
            // With no runtime's dispatcher to count on: the count, taken
            // here, hands every kind that has arrived on, this one
            // included, if it is among them.
            match take_count(globals) {
                Ok(true) => hand_on(globals),
                Ok(false) => match globals.event.poll_ready(cx, READABLE, wait) {
                    Poll::Ready(Ok(_)) => {}
                    Poll::Ready(Err(_)) => return Poll::Ready(None),
                    Poll::Pending => return Poll::Pending,
                },
                Err(_) => return Poll::Ready(None),
            }
        }
    }
}

impl fmt::Debug for Signal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Signal")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}
