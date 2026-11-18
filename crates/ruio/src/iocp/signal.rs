//! Signals, delivered to tasks: on Windows, the control events of the
//! console, Ctrl+C above all.
//!
//! This is ruio's `signal` on the I/O completion port engine, under the
//! names and with the signatures it has on io_uring, so that code written
//! to it builds for both. A console program on Windows gets no Unix
//! signals; its console sends it control events instead: Ctrl+C and
//! Ctrl+Break typed in it, its window closed, and, for a service, the user
//! logging off and the system shutting down. [`ctrl_c`] waits for Ctrl+C,
//! as it waits for `SIGINT` on Linux, and [`signal`] gives a [`Signal`]
//! for any of the events, by its [`SignalKind`], whose
//! [`recv`](Signal::recv) completes each time the event arrives. Events
//! of one kind that arrive while nobody is receiving coalesce into one: a
//! `Signal` says that the event has arrived since it was last asked, not
//! how many times.
//!
//! A handler of the process's, installed with `SetConsoleCtrlHandler` as
//! the first listener is made, stays. The system calls it for each event on
//! a thread of its own, made for the call, where anything may be done, so
//! the handler hands the event straight to the kind's `watch` channel,
//! whose receivers are the listeners, and each listener is woken wherever
//! it is polled. No runtime is in between: a listener receives on a thread
//! of another executor's as well as inside a runtime.
//!
//! An event that a listener is made for is handled: the handler says so,
//! and the system's own handling, which ends the process for Ctrl+C and
//! Ctrl+Break, does not run. An event of a kind with no listener is passed
//! on, to the handlers installed before this one and at last to the
//! system's, as if ruio had none. The system ends the process as soon as a
//! handler returns from a window closed, a log off or a shut down, so for
//! those, with a listener, the handler does not return: the program has
//! until the system's time is up, five seconds for a window closed, to wind
//! down, and ends as it is done.
//!
//! A process whose parent ignored Ctrl+C, or that was started in a
//! process group of its own, as one spawned with `CREATE_NEW_PROCESS_GROUP`
//! is, starts with Ctrl+C ignored, and receives Ctrl+Break alone, until
//! `SetConsoleCtrlHandler(NULL, FALSE)` lets it have Ctrl+C again; ruio
//! leaves that to the program.

use std::fmt;
use std::future::poll_fn;
use std::io;
use std::sync::{Mutex, OnceLock, PoisonError};
use std::task::{Context, Poll};

use ruio_executor::sync::watch;

/// `CTRL_C_EVENT`.
const CTRL_C_EVENT: u32 = 0;
/// `CTRL_BREAK_EVENT`.
const CTRL_BREAK_EVENT: u32 = 1;
/// `CTRL_CLOSE_EVENT`.
const CTRL_CLOSE_EVENT: u32 = 2;
/// `CTRL_LOGOFF_EVENT`.
const CTRL_LOGOFF_EVENT: u32 = 5;
/// `CTRL_SHUTDOWN_EVENT`.
const CTRL_SHUTDOWN_EVENT: u32 = 6;

/// How many kinds of event the console sends.
const KINDS: usize = 5;

/// The signature of a console's control handler.
type HandlerRoutine = unsafe extern "system" fn(u32) -> i32;

#[link(name = "kernel32")]
unsafe extern "system" {
    fn SetConsoleCtrlHandler(handler: Option<HandlerRoutine>, add: i32) -> i32;
}

/// A kind of control event of the console's, by its number.
///
/// The constructors name the five events the console sends;
/// [`from_raw`](SignalKind::from_raw) takes a number, as the `CTRL_*_EVENT`
/// constants of the Windows API have them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SignalKind(u32);

impl SignalKind {
    /// The event with number `event`, as the Windows API numbers them.
    pub const fn from_raw(event: u32) -> Self {
        Self(event)
    }

    /// The event's number.
    pub const fn as_raw_value(self) -> u32 {
        self.0
    }

    /// `CTRL_C_EVENT`: Ctrl+C, typed in the console, the interrupt that
    /// `SIGINT` is on Linux.
    pub const fn interrupt() -> Self {
        Self(CTRL_C_EVENT)
    }

    /// `CTRL_BREAK_EVENT`: Ctrl+Break, typed in the console, or sent to a
    /// process group with `GenerateConsoleCtrlEvent`.
    pub const fn ctrl_break() -> Self {
        Self(CTRL_BREAK_EVENT)
    }

    /// `CTRL_CLOSE_EVENT`: the console's window is being closed. The
    /// process is ended once the handler returns, which, with a listener,
    /// it does not, or after five seconds.
    pub const fn ctrl_close() -> Self {
        Self(CTRL_CLOSE_EVENT)
    }

    /// `CTRL_LOGOFF_EVENT`: a user is logging off. Services alone receive
    /// it.
    pub const fn ctrl_logoff() -> Self {
        Self(CTRL_LOGOFF_EVENT)
    }

    /// `CTRL_SHUTDOWN_EVENT`: the system is shutting down. Services alone
    /// receive it.
    pub const fn ctrl_shutdown() -> Self {
        Self(CTRL_SHUTDOWN_EVENT)
    }

    /// The event's index among the kinds, if the console sends it.
    fn index(self) -> Option<usize> {
        match self.0 {
            CTRL_C_EVENT => Some(0),
            CTRL_BREAK_EVENT => Some(1),
            CTRL_CLOSE_EVENT => Some(2),
            CTRL_LOGOFF_EVENT => Some(3),
            CTRL_SHUTDOWN_EVENT => Some(4),
            _ => None,
        }
    }

    /// Whether the system ends the process as soon as a handler returns
    /// from the event.
    fn ends_the_process(self) -> bool {
        matches!(
            self.0,
            CTRL_CLOSE_EVENT | CTRL_LOGOFF_EVENT | CTRL_SHUTDOWN_EVENT
        )
    }
}

impl From<u32> for SignalKind {
    fn from(event: u32) -> Self {
        Self(event)
    }
}

impl From<SignalKind> for u32 {
    fn from(kind: SignalKind) -> Self {
        kind.0
    }
}

/// What the process has for its events, made at the first `signal`.
struct Globals {
    /// The channel of each kind, which counts its arrivals.
    senders: [watch::Sender<u64>; KINDS],
    /// Whether the handler is installed; held while it is being installed,
    /// so that it is installed once.
    installed: Mutex<bool>,
}

static GLOBALS: OnceLock<Globals> = OnceLock::new();

fn globals() -> &'static Globals {
    GLOBALS.get_or_init(|| Globals {
        senders: std::array::from_fn(|_| watch::channel(0).0),
        installed: Mutex::new(false),
    })
}

impl Globals {
    /// Installs [`handler`], unless it is installed.
    fn install(&self) -> io::Result<()> {
        let mut installed = self
            .installed
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if *installed {
            return Ok(());
        }
        // SAFETY: the handler is a function of the right signature, which
        // stays for as long as the process runs.
        if unsafe { SetConsoleCtrlHandler(Some(handler), 1) } == 0 {
            return Err(io::Error::last_os_error());
        }
        *installed = true;
        Ok(())
    }

    /// Hands an arrival of `kind` to its channel, and says whether any
    /// listener is there to receive it.
    fn deliver(&self, kind: SignalKind) -> bool {
        let Some(index) = kind.index() else {
            return false;
        };
        let sender = &self.senders[index];
        sender.send_modify(|arrivals| *arrivals += 1);
        sender.receiver_count() > 0
    }
}

/// The handler of the console's events, which the system calls on a thread
/// it makes for the call: hands the event to its listeners, and says it is
/// handled if any are there, or passes it on. For an event after which the
/// system ends the process, it does not return once a listener has it, so
/// that the program can wind down first.
unsafe extern "system" fn handler(event: u32) -> i32 {
    let kind = SignalKind(event);
    let Some(globals) = GLOBALS.get() else {
        return 0;
    };
    if !globals.deliver(kind) {
        return 0;
    }
    if kind.ends_the_process() {
        loop {
            std::thread::park();
        }
    }
    1
}

/// Listens for `kind`: a [`Signal`] whose [`recv`](Signal::recv) completes
/// each time the event arrives from now on, and which keeps the event from
/// the system's own handling while it lives. The process's handler is
/// installed if it is not already.
///
/// The listener needs no runtime: it receives wherever it is polled.
///
/// # Errors
///
/// [`io::ErrorKind::InvalidInput`] for a number the console sends no event
/// with, and what `SetConsoleCtrlHandler` reports.
///
/// ```no_run
/// use ruio::signal::{SignalKind, signal};
///
/// # ruio::Runtime::new().unwrap().block_on(async {
/// let mut breaks = signal(SignalKind::ctrl_break())?;
/// while breaks.recv().await.is_some() {
///     println!("Ctrl+Break");
/// }
/// # Ok::<(), std::io::Error>(())
/// # }).unwrap();
/// ```
pub fn signal(kind: SignalKind) -> io::Result<Signal> {
    let Some(index) = kind.index() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("the console sends no control event {}", kind.0),
        ));
    };
    let globals = globals();
    let arrivals = globals.senders[index].subscribe();
    globals.install()?;
    Ok(Signal { kind, arrivals })
}

/// Waits for Ctrl+C, typed in the console. The listener is made, and the
/// handler installed, as the future is first polled, as [`signal`] makes
/// one.
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

/// A listener for one kind of control event, made by [`signal`]. It is
/// `Send` and `Sync`, and receives on whichever thread polls it, inside a
/// runtime or not.
///
/// Events that arrive while the listener is not receiving coalesce: the
/// next [`recv`](Signal::recv) completes once, however many came. One that
/// arrived before the listener was made is not delivered to it. While a
/// listener for a kind lives, the process handles the kind's events, and
/// the system does not end it for them.
#[must_use = "a listener does nothing until it receives"]
pub struct Signal {
    kind: SignalKind,
    /// How many times the event has arrived, as the kind's channel counts;
    /// a receive is a change of the count.
    arrivals: watch::Receiver<u64>,
}

impl Signal {
    /// The kind listened for.
    pub fn kind(&self) -> SignalKind {
        self.kind
    }

    /// Waits for the event to arrive, once, however many times it does
    /// meanwhile. It never comes to `None`: the channel behind the
    /// listener lives as long as the process.
    pub async fn recv(&mut self) -> Option<()> {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    /// Whether the event has arrived since the last receive; arranges for
    /// the task to be woken when it does otherwise.
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<()>> {
        self.arrivals.poll_changed(cx).map(Result::ok)
    }
}

impl fmt::Debug for Signal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Signal")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

/// Each item is an arrival of the event, however many arrived together;
/// the stream never ends.
#[cfg(feature = "futures")]
impl futures_core::Stream for Signal {
    type Item = ();

    #[inline]
    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<()>> {
        self.get_mut().poll_recv(cx)
    }
}

/// Never terminated: the event may always arrive again.
#[cfg(feature = "futures")]
impl futures_core::stream::FusedStream for Signal {
    #[inline]
    fn is_terminated(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::Runtime;

    /// An event as the system hands it to the handler, on a thread of its
    /// own, without the process ever being ended: whether a listener had it.
    fn arrive(kind: SignalKind) -> bool {
        std::thread::spawn(move || globals().deliver(kind))
            .join()
            .unwrap()
    }

    /// Whether `signal` receives within a short wait.
    async fn receives(signal: &mut Signal) -> bool {
        crate::time::timeout(Duration::from_millis(500), signal.recv())
            .await
            .is_ok()
    }

    /// A listener receives an event that arrives, once however many arrive
    /// together, and the event is handled while it lives.
    #[test]
    fn a_listener_receives_each_arrival_once() {
        Runtime::new().unwrap().block_on(async {
            let mut breaks = signal(SignalKind::ctrl_break()).unwrap();
            assert_eq!(breaks.kind(), SignalKind::ctrl_break());
            for _ in 0..3 {
                assert!(arrive(SignalKind::ctrl_break()), "handled");
            }
            assert!(receives(&mut breaks).await);
            assert!(!receives(&mut breaks).await, "the three coalesced");
            assert!(arrive(SignalKind::ctrl_break()));
            assert!(receives(&mut breaks).await);
        });
    }

    /// Every listener of a kind receives, on any runtime or none; one made
    /// after an event does not receive it.
    #[test]
    fn every_listener_receives_wherever_it_is_polled() {
        let mut here = signal(SignalKind::ctrl_logoff()).unwrap();
        let mut elsewhere = signal(SignalKind::ctrl_logoff()).unwrap();
        let other = std::thread::spawn(move || {
            Runtime::new()
                .unwrap()
                .block_on(async move { receives(&mut elsewhere).await })
        });
        std::thread::sleep(Duration::from_millis(20));
        assert!(arrive(SignalKind::ctrl_logoff()));
        assert!(futures_executor::block_on(here.recv()).is_some());
        assert!(other.join().unwrap());
        let mut later = signal(SignalKind::ctrl_logoff()).unwrap();
        assert!(
            !Runtime::new()
                .unwrap()
                .block_on(async { receives(&mut later).await }),
            "made after the event"
        );
    }

    /// With no listener left, an event is passed on, for the system to
    /// handle as it would with no handler of ruio's.
    #[test]
    fn an_event_with_no_listener_is_passed_on() {
        drop(signal(SignalKind::ctrl_shutdown()).unwrap());
        assert!(!arrive(SignalKind::ctrl_shutdown()));
    }

    #[test]
    fn events_the_console_does_not_send_are_refused() {
        for event in [3, 4, 7, u32::MAX] {
            let err = signal(SignalKind::from_raw(event)).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "event {event}");
        }
        assert_eq!(u32::from(SignalKind::ctrl_close()), 2);
        assert_eq!(SignalKind::from(0), SignalKind::interrupt());
        assert_eq!(SignalKind::ctrl_break().as_raw_value(), 1);
    }

    #[test]
    fn a_listener_is_send_and_sync() {
        fn is_send_sync<T: Send + Sync>() {}
        is_send_sync::<Signal>();
    }
}
