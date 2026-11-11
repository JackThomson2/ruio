//! The engine: a completion port, the thread's current engine, and turning
//! the port.

use std::cell::{Cell, RefCell};
use std::fmt;
use std::io;
use std::mem;
use std::ptr;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use ruio_engine::{Build, Engine, LocalWaker, Unpark};

use crate::{op, sys};

/// The completion key of the packets unparks post.
const WAKE: usize = 0;

/// The completion key of the handles associated with the port, under which
/// their operations complete.
const IO: usize = 1;

/// The completion key of the packets the engine's fine timer posts.
const TIMER: usize = 2;

/// How many operations may finish during the calls that start them between
/// two turns of the port: as many as the executor runs tasks between two.
/// Such an operation costs no turn, so a task whose operations all finish
/// so would otherwise never let the port be turned, nor another task run.
pub(crate) const AT_ONCE: u32 = 64;

/// The most completions a turn takes from the port at once.
const BATCH: usize = 64;

/// The ID of the next engine.
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// The point the engines' clock counts from: its first reading.
static EPOCH: OnceLock<Instant> = OnceLock::new();

thread_local! {
    /// The engine the thread entered last, for what is made on the thread
    /// without being handed one.
    static CURRENT: RefCell<Option<Iocp>> = const { RefCell::new(None) };
}

/// An engine on an I/O completion port; see the [crate] documentation.
///
/// Like any engine, it belongs to the thread that made it, and so do the
/// files, pipes and sockets associated with its port. A clone is the same
/// engine. Dropping the last clone, once those files, pipes and sockets
/// are gone, waits for the operations still in flight to complete.
///
/// ```compile_fail
/// fn assert_send<T: Send>() {}
/// assert_send::<ruio_iocp::Iocp>();
/// ```
#[derive(Clone)]
pub struct Iocp {
    inner: Rc<Inner>,
}

struct Inner {
    id: u64,
    shared: Arc<Shared>,
    /// Completions that a check for work took from the port, which the next
    /// turn delivers.
    stash: RefCell<Vec<sys::OverlappedEntry>>,
    /// The deadline the timer is armed for, if it is.
    armed: Cell<Option<Duration>>,
    /// Whether the timer has fired since `timer_fired` last asked.
    fired: Cell<bool>,
    /// The timer that wakes the port when a wait's deadline comes, where
    /// the system has one finer than the tick its own waits end at.
    fine: Option<FineTimer>,
    /// How many operations the system has taken whose completions have not
    /// been delivered.
    in_flight: Cell<usize>,
    /// How many operations have finished during the calls that started
    /// them since the port was last turned.
    at_once: Cell<u32>,
    /// The operations in flight that were left to finish by themselves,
    /// which no future's drop cancels.
    detached: op::DetachedOps,
    /// Whether the engine is being dropped, when completions wake nothing.
    closing: Cell<bool>,
    /// How long dropping the engine waits for the operations in flight.
    shutdown_timeout: Duration,
}

/// What an engine shares with its unparkers: the port, which the last of
/// them to go closes, so that an unpark never posts to a handle that has
/// been closed, whose value may name another object by then.
struct Shared {
    port: sys::Handle,
    /// Whether an unpark has posted a packet that no turn has taken yet. An
    /// unpark posts only if it sets this, so that several before a turn
    /// wake it once.
    notified: AtomicBool,
}

// SAFETY: a completion port's handle may be used from any thread, and the
// flag is atomic.
unsafe impl Send for Shared {}

// SAFETY: as for `Send`; several threads posting to one port at once is
// what ports are for.
unsafe impl Sync for Shared {}

impl Shared {
    fn unpark(&self) {
        if self.notified.swap(true, Ordering::SeqCst) {
            return;
        }
        // SAFETY: the port is open while `self` is.
        let posted =
            unsafe { sys::PostQueuedCompletionStatus(self.port, 0, WAKE, ptr::null_mut()) };
        if posted == 0 {
            // No packet is on its way to clear the flag, so clear it here,
            // for the next unpark to try again.
            self.notified.store(false, Ordering::SeqCst);
        }
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        // SAFETY: the port is open, and nothing can use it once its last
        // holder is gone.
        unsafe { sys::CloseHandle(self.port) };
    }
}

/// The time on the engines' clock: a monotonic one, counted from its first
/// reading.
fn now() -> Duration {
    let epoch = *EPOCH.get_or_init(Instant::now);
    Instant::now().saturating_duration_since(epoch)
}

/// How many milliseconds there are until `deadline`, a time on the engines'
/// clock, rounded up so that a wait for them does not end early, and short
/// of `INFINITE`.
fn milliseconds_until(deadline: Duration) -> u32 {
    let left = deadline
        .saturating_sub(now())
        .as_nanos()
        .div_ceil(1_000_000);
    u32::try_from(left).map_or(sys::INFINITE - 1, |ms| ms.min(sys::INFINITE - 1))
}

/// A timer that ends a wait on the port when its deadline comes, rather
/// than at the tick of Windows' timer after it, which may be 15 ms later:
/// a high-resolution waitable timer, and the wait completion packet
/// through which its expiry is posted to the port.
///
/// The packet waits for the timer, not for a deadline, so a timer that is
/// set anew while the packet waits takes one call. Once the timer has
/// expired, the packet lies in the port until a turn takes it, and is then
/// associated again the next time the timer is set.
struct FineTimer {
    timer: sys::Handle,
    packet: sys::Handle,
    /// The deadline the timer is set for, while the packet waits for it or
    /// lies in the port: `None` before the timer is first set, and once a
    /// turn has taken the packet.
    set: Cell<Option<Duration>>,
}

impl FineTimer {
    /// A timer of the finest kind the system has, and a packet for it, or
    /// `None` where the system has not both, as before Windows 10's
    /// version 1803: waits there end at the ticks of Windows' timer.
    fn new() -> Option<Self> {
        // SAFETY: a timer with no name and the default security.
        let timer = unsafe {
            sys::CreateWaitableTimerExW(
                ptr::null(),
                ptr::null(),
                sys::CREATE_WAITABLE_TIMER_HIGH_RESOLUTION,
                sys::TIMER_ALL_ACCESS,
            )
        };
        if timer.is_null() {
            return None;
        }
        let mut packet = ptr::null_mut();
        // SAFETY: the handle is written to `packet` during the call, and
        // the packet gets the default attributes.
        let status = unsafe {
            sys::NtCreateWaitCompletionPacket(&mut packet, sys::GENERIC_ALL, ptr::null())
        };
        if status < sys::STATUS_SUCCESS || packet.is_null() {
            // SAFETY: the timer was just made, and nothing else has it.
            unsafe { sys::CloseHandle(timer) };
            return None;
        }
        Some(Self {
            timer,
            packet,
            set: Cell::new(None),
        })
    }

    /// Sets the timer to post a packet to `port` at `deadline`, a time on
    /// the engines' clock, unless it is set for it already, and says
    /// whether the deadline came as it did, when no packet follows. A timer
    /// the system fails to set is left as it was: the wait then ends by its
    /// own timeout.
    fn set(&self, port: sys::Handle, deadline: Duration) -> bool {
        let waits = match self.set.get() {
            Some(at) if at == deadline => return false,
            // The packet waits for the timer, or lies in the port, where
            // the next turn takes it, and the timer is then set again.
            Some(_) => true,
            None => false,
        };
        // In units of 100 ns, of which a timer from now takes one at least.
        let left = deadline.saturating_sub(now()).as_nanos().div_ceil(100);
        let due = -i64::try_from(left).unwrap_or(i64::MAX).max(1);
        // SAFETY: the timer is open, and the time is read during the call;
        // it has no period, no completion routine, and wakes no system.
        let set =
            unsafe { sys::SetWaitableTimer(self.timer, &due, 0, ptr::null(), ptr::null(), 0) };
        if set == 0 {
            return false;
        }
        if waits {
            self.set.set(Some(deadline));
            return false;
        }
        let mut signaled = 0;
        // SAFETY: the packet, the port and the timer are open; the key is
        // no pointer, and the packet carries nothing else; and `signaled`
        // is written during the call.
        let status = unsafe {
            sys::NtAssociateWaitCompletionPacket(
                self.packet,
                port,
                self.timer,
                ptr::without_provenance_mut(TIMER),
                ptr::null_mut(),
                sys::STATUS_SUCCESS,
                0,
                &mut signaled,
            )
        };
        if status < sys::STATUS_SUCCESS {
            return false;
        }
        if signaled != 0 {
            // The timer expired between the two calls. The packet it may
            // have posted is taken back, so that the packet is free for
            // the next deadline whether it was posted or not.
            self.cancel();
            return true;
        }
        self.set.set(Some(deadline));
        false
    }
    /// Ends the packet's wait for the timer, and takes the packet it posted
    /// out of the port, if no turn has taken it.
    fn cancel(&self) {
        // SAFETY: the packet is open. One that neither waits nor lies in a
        // port is left as it is.
        unsafe { sys::NtCancelWaitCompletionPacket(self.packet, 1) };
    }

    /// Records that a turn took the packet the timer posted, which is the
    /// engine's to associate again.
    fn taken(&self) {
        self.set.set(None);
    }
}

impl Drop for FineTimer {
    fn drop(&mut self) {
        // The packet waits for the timer no more before either is closed.
        self.cancel();
        // SAFETY: the packet is open, and the engine's alone.
        unsafe { sys::CloseHandle(self.packet) };
        // SAFETY: as for the packet.
        unsafe { sys::CloseHandle(self.timer) };
    }
}

impl Iocp {
    fn new(builder: &Builder) -> io::Result<Self> {
        // SAFETY: given no file and no port, this makes a port, for one
        // thread, or fails and returns null.
        let port = unsafe {
            sys::CreateIoCompletionPort(sys::INVALID_HANDLE_VALUE, ptr::null_mut(), 0, 1)
        };
        if port.is_null() {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            inner: Rc::new(Inner {
                id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
                shared: Arc::new(Shared {
                    port,
                    notified: AtomicBool::new(false),
                }),
                stash: RefCell::default(),
                armed: Cell::new(None),
                fired: Cell::new(false),
                fine: FineTimer::new(),
                in_flight: Cell::new(0),
                at_once: Cell::new(0),
                detached: op::DetachedOps::default(),
                closing: Cell::new(false),
                shutdown_timeout: builder.shutdown_timeout,
            }),
        })
    }

    /// The engine the thread has entered, as a runtime enters its own while
    /// it runs, or `None` if it has entered none.
    pub fn current() -> Option<Self> {
        CURRENT
            .try_with(|current| current.borrow().clone())
            .ok()
            .flatten()
    }

    /// Associates `handle`, opened for overlapped I/O, with the port, so
    /// that its operations complete there.
    pub(crate) fn associate(&self, handle: sys::Handle) -> io::Result<()> {
        // SAFETY: the handle and the port are open. A handle can be
        // associated with one port at most, and the system refuses another.
        let port = unsafe { sys::CreateIoCompletionPort(handle, self.inner.shared.port, IO, 0) };
        if port.is_null() {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Counts an operation the system took, whose completion is to come.
    pub(crate) fn started(&self) {
        let in_flight = &self.inner.in_flight;
        in_flight.set(in_flight.get() + 1);
    }

    /// Whether as many operations have finished during the calls that
    /// started them as may between two turns of the port.
    pub(crate) fn at_once_spent(&self) -> bool {
        self.inner.at_once.get() >= AT_ONCE
    }

    /// Counts an operation that finished during the call that started it.
    pub(crate) fn finished_at_once(&self) {
        let at_once = &self.inner.at_once;
        at_once.set(at_once.get() + 1);
    }

    /// The operations in flight that were left to finish by themselves.
    pub(crate) fn detached(&self) -> &op::DetachedOps {
        &self.inner.detached
    }
}

impl Inner {
    /// Takes up to [`BATCH`] completions from the port into `entries`,
    /// waiting up to `milliseconds` for the first, and says how many.
    fn dequeue(
        &self,
        entries: &mut [sys::OverlappedEntry; BATCH],
        milliseconds: u32,
    ) -> io::Result<usize> {
        let mut removed: u32 = 0;
        // SAFETY: the port is open, and `entries` has room for the count it
        // is given.
        let ok = unsafe {
            sys::GetQueuedCompletionStatusEx(
                self.shared.port,
                entries.as_mut_ptr(),
                BATCH as u32,
                &mut removed,
                milliseconds,
                0,
            )
        };
        if ok != 0 {
            return Ok(removed as usize);
        }
        match sys::GetLastError() {
            sys::WAIT_TIMEOUT => Ok(0),
            code => Err(io::Error::from_raw_os_error(code as i32)),
        }
    }

    /// Delivers completions taken from the port, and says how many there
    /// were besides the fine timer's own packets.
    fn deliver(&self, entries: &[sys::OverlappedEntry]) -> usize {
        let mut delivered = entries.len();
        for entry in entries {
            match entry.completion_key {
                WAKE => self.shared.notified.store(false, Ordering::SeqCst),
                IO => {
                    self.in_flight.set(self.in_flight.get() - 1);
                    // SAFETY: a completion under the key of the handles is
                    // that of an operation `op` started on one, which the
                    // port hands back once.
                    unsafe { op::complete(entry, &self.detached, !self.closing.get()) };
                }
                TIMER => {
                    if let Some(fine) = &self.fine {
                        fine.taken();
                    }
                    delivered -= 1;
                }
                // Nothing posts under another key.
                _ => {}
            }
        }
        delivered
    }

    /// Delivers what a check for work took from the port and what the port
    /// has, waiting for a completion until `deadline`, a time on the
    /// engines' clock, or for ever if there is none, unless the check took
    /// some already. A turn that would not wait asks the port for nothing
    /// when the port can hold nothing for it.
    fn turn(&self, deadline: Option<Duration>) -> io::Result<()> {
        self.at_once.set(0);
        let mut deadline = deadline;
        if !self.stash.borrow().is_empty() {
            let stashed = mem::take(&mut *self.stash.borrow_mut());
            self.deliver(&stashed);
            deadline = Some(Duration::ZERO);
        }
        // The clock is not read for a turn that asks for no wait at all,
        // which is what the executor makes between its tasks.
        if self.idle() && deadline.is_some_and(|deadline| deadline.is_zero() || deadline <= now()) {
            self.check_timer();
            return Ok(());
        }
        let mut entries = [sys::OverlappedEntry::EMPTY; BATCH];
        loop {
            let milliseconds = deadline.map_or(sys::INFINITE, milliseconds_until);
            // A wait with a deadline ends by the fine timer's packet when
            // the deadline comes; its own timeout, which Windows counts in
            // milliseconds from a tick of its timer, stands behind that.
            if milliseconds > 0
                && let (Some(fine), Some(deadline)) = (&self.fine, deadline)
                && fine.set(self.shared.port, deadline)
            {
                continue;
            }
            let taken = self.dequeue(&mut entries, milliseconds)?;
            let delivered = self.deliver(&entries[..taken]);
            // A wait may end a little before its deadline, at a tick of
            // Windows' timer, or by a packet of the fine timer's from when
            // it was set for another, so one that delivered nothing waits
            // out the rest.
            if delivered > 0 || milliseconds == 0 {
                break;
            }
        }
        self.check_timer();
        Ok(())
    }

    /// Whether the port holds nothing for a turn to deliver: no operation
    /// is in flight, and no unpark has posted a packet. A packet of the
    /// fine timer's may lie there, which is no work, and which the next
    /// turn that waits takes.
    fn idle(&self) -> bool {
        self.in_flight.get() == 0 && !self.shared.notified.load(Ordering::SeqCst)
    }

    /// Records that the timer fired, if its deadline has passed.
    fn check_timer(&self) {
        if let Some(deadline) = self.armed.get()
            && deadline <= now()
        {
            self.armed.set(None);
            self.fired.set(true);
        }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        // The operations still in flight are those whose futures were
        // dropped, which cancelled them, those forgotten, and those left to
        // finish by themselves, which are cancelled here. A completion frees
        // the slot of an operation that no future has, buffer and all; one
        // that has not come by the timeout leaves its slot leaked, as the
        // system may still write to its buffer.
        self.closing.set(true);
        self.detached.cancel();
        let deadline = now().saturating_add(self.shutdown_timeout);
        while self.in_flight.get() > 0 && now() < deadline {
            if self.turn(Some(deadline)).is_err() {
                break;
            }
        }
    }
}

impl Engine for Iocp {
    type Builder = Builder;
    type Unparker = Unparker;
    type Entered = Entered;

    fn id(&self) -> u64 {
        self.inner.id
    }

    fn monotonic_now() -> Duration {
        now()
    }

    fn submit(&self) -> io::Result<()> {
        self.inner.turn(Some(Duration::ZERO))
    }

    fn has_work(&self) -> bool {
        let inner = &self.inner;
        if inner.shared.notified.load(Ordering::SeqCst)
            || !inner.stash.borrow().is_empty()
            || inner.armed.get().is_some_and(|deadline| deadline <= now())
        {
            return true;
        }
        // No unpark has posted, so with no operation in flight the port
        // holds no work, and is not asked.
        if inner.in_flight.get() == 0 {
            return false;
        }
        let mut entries = [sys::OverlappedEntry::EMPTY; BATCH];
        match inner.dequeue(&mut entries, 0) {
            Ok(0) => false,
            Ok(taken) => {
                let mut stash = inner.stash.borrow_mut();
                for entry in &entries[..taken] {
                    match (&inner.fine, entry.completion_key) {
                        // The timer's packet is no work of its own: the
                        // deadline it was set for is, once it has come.
                        (Some(fine), TIMER) => fine.taken(),
                        _ => stash.push(*entry),
                    }
                }
                !stash.is_empty() || inner.armed.get().is_some_and(|deadline| deadline <= now())
            }
            // The next turn meets the error again, and reports it.
            Err(_) => true,
        }
    }

    fn park(&self, timeout: Option<Duration>) -> io::Result<()> {
        let after = timeout.map(|timeout| now().saturating_add(timeout));
        let until = match (after, self.inner.armed.get()) {
            (Some(after), Some(armed)) => Some(after.min(armed)),
            (after, armed) => after.or(armed),
        };
        self.inner.turn(until)
    }

    fn wake_at(&self, deadline: Duration) -> io::Result<()> {
        let armed = &self.inner.armed;
        if armed.get().is_none_or(|armed| deadline < armed) {
            armed.set(Some(deadline));
        }
        Ok(())
    }

    fn timer_fired(&self) -> bool {
        self.inner.fired.replace(false)
    }

    fn unparker(&self) -> Unparker {
        Unparker {
            shared: Arc::clone(&self.inner.shared),
        }
    }

    fn enter(&self) -> Entered {
        let previous = CURRENT
            .try_with(|current| current.replace(Some(self.clone())))
            .ok()
            .flatten();
        Entered { previous }
    }

    /// Does nothing: the engine clones the wakers it holds.
    unsafe fn set_local_waker(&self, _local: LocalWaker) {}
}

impl fmt::Debug for Iocp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Iocp")
            .field("id", &self.inner.id)
            .field("in_flight", &self.inner.in_flight.get())
            .finish_non_exhaustive()
    }
}

/// Configures and creates an [`Iocp`].
#[derive(Clone, Debug)]
pub struct Builder {
    shutdown_timeout: Duration,
}

impl Builder {
    /// A builder of the defaults.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            shutdown_timeout: Duration::from_secs(1),
        }
    }
}

impl Build for Builder {
    type Engine = Iocp;

    /// Sets how long dropping the engine waits for the operations still in
    /// flight, which the drops of their futures cancelled, or the engine as
    /// it is dropped, to complete, before it leaks their buffers. The
    /// default is one second.
    fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    fn build(&self) -> io::Result<Iocp> {
        Iocp::new(self)
    }
}

/// Wakes an [`Iocp`] from any thread, by posting a packet to its port.
#[derive(Clone)]
pub struct Unparker {
    shared: Arc<Shared>,
}

impl Unpark for Unparker {
    fn unpark(&self) {
        self.shared.unpark();
    }
}

impl fmt::Debug for Unparker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Unparker").finish_non_exhaustive()
    }
}

/// Keeps an [`Iocp`] the thread's current engine until dropped, when the
/// engine that was current before it is current again.
#[must_use = "the engine is current only until the guard is dropped"]
pub struct Entered {
    previous: Option<Iocp>,
}

impl Drop for Entered {
    fn drop(&mut self) {
        let previous = self.previous.take();
        let left = CURRENT
            .try_with(|current| current.replace(previous))
            .ok()
            .flatten();
        // Dropped once the thread's cell is let go of, as dropping the last
        // clone of an engine turns it.
        drop(left);
    }
}

impl fmt::Debug for Entered {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Entered").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use ruio_engine::{Build, Engine, Unpark};

    use super::{Builder, Iocp};

    #[test]
    fn the_engine_keeps_the_contract() {
        ruio_engine::conformance::check::<Iocp>(&Builder::new());
    }

    #[test]
    fn a_park_after_a_check_for_work_returns_at_once_and_once() {
        let engine = Builder::new().build().unwrap();
        engine.unparker().unpark();
        assert!(engine.has_work());
        engine.park(None).unwrap();
        assert!(!engine.has_work());
        let started = Instant::now();
        engine.park(Some(Duration::from_millis(20))).unwrap();
        assert!(started.elapsed() >= Duration::from_millis(20));
    }

    #[test]
    fn a_turn_that_does_not_wait_takes_the_packet_of_an_unpark() {
        let engine = Builder::new().build().unwrap();
        // Nothing is in flight and no unpark has posted: nothing to take.
        engine.submit().unwrap();
        assert!(!engine.has_work());
        engine.unparker().unpark();
        assert!(engine.has_work());
        engine.submit().unwrap();
        assert!(!engine.has_work());
        // The packet is gone, so a park waits out its timeout.
        let started = Instant::now();
        engine.park(Some(Duration::from_millis(20))).unwrap();
        assert!(started.elapsed() >= Duration::from_millis(20));
    }

    #[test]
    fn a_turn_that_asks_the_port_for_nothing_still_notices_the_timer() {
        let engine = Builder::new().build().unwrap();
        engine.wake_at(Iocp::monotonic_now()).unwrap();
        engine.submit().unwrap();
        assert!(engine.timer_fired());
        assert!(!engine.timer_fired());

        // One that is due later is noticed once it is.
        engine
            .wake_at(Iocp::monotonic_now() + Duration::from_millis(5))
            .unwrap();
        engine.submit().unwrap();
        assert!(!engine.timer_fired());
        std::thread::sleep(Duration::from_millis(10));
        assert!(engine.has_work());
        engine.submit().unwrap();
        assert!(engine.timer_fired());
        // A park whose timeout has passed is such a turn too.
        engine.wake_at(Iocp::monotonic_now()).unwrap();
        engine.park(Some(Duration::ZERO)).unwrap();
        assert!(engine.timer_fired());
    }

    #[test]
    fn a_park_ends_at_the_earlier_of_its_timeout_and_the_timer() {
        let engine = Builder::new().build().unwrap();
        engine
            .wake_at(Iocp::monotonic_now() + Duration::from_secs(3600))
            .unwrap();
        let started = Instant::now();
        engine.park(Some(Duration::from_millis(10))).unwrap();
        assert!(started.elapsed() >= Duration::from_millis(10));
        assert!(started.elapsed() < Duration::from_secs(60));
        assert!(!engine.timer_fired());
    }

    #[test]
    fn entering_makes_an_engine_current_until_the_guard_drops() {
        assert!(Iocp::current().is_none());
        let outer = Builder::new().build().unwrap();
        let inner = Builder::new().build().unwrap();
        let entered = outer.enter();
        {
            let _inner = inner.enter();
            assert_eq!(Iocp::current().map(|e| e.id()), Some(inner.id()));
        }
        assert_eq!(Iocp::current().map(|e| e.id()), Some(outer.id()));
        drop(entered);
        assert!(Iocp::current().is_none());
    }

    /// Whether `engine` has a timer finer than the ticks of Windows' own,
    /// which a system before Windows 10's version 1803 does not give it:
    /// its parks then end at those ticks, and the tests of when they end
    /// have nothing to test.
    fn has_fine_timer(engine: &Iocp) -> bool {
        let fine = engine.inner.fine.is_some();
        if !fine {
            eprintln!("skipped: the system has no high-resolution timer");
        }
        fine
    }

    /// How long `parks` parks of `each` take: the least of three tries, as
    /// what else runs on the machine can only make them longer.
    fn parks(engine: &Iocp, parks: u32, each: Duration) -> Duration {
        (0..3)
            .map(|_| {
                let started = Instant::now();
                for _ in 0..parks {
                    let before = Instant::now();
                    engine.park(Some(each)).unwrap();
                    assert!(before.elapsed() >= each, "a park ended early");
                }
                started.elapsed()
            })
            .min()
            .unwrap()
    }

    #[test]
    fn parks_end_at_their_deadlines_and_not_at_the_ticks_after() {
        let engine = Builder::new().build().unwrap();
        if !has_fine_timer(&engine) {
            return;
        }
        // Each park's packet is taken, and the timer's packet associated
        // again for the next. Waits that end at the ticks of Windows'
        // timer, 15.6 ms apart, would take 300 ms.
        let took = parks(&engine, 20, Duration::from_millis(2));
        assert!(
            took < Duration::from_millis(150),
            "20 parks of 2 ms took {took:?}"
        );
    }

    #[test]
    fn a_timer_set_for_another_deadline_ends_the_park_at_the_new_one() {
        let engine = Builder::new().build().unwrap();
        if !has_fine_timer(&engine) {
            return;
        }
        for _ in 0..3 {
            // The park sets the timer for an hour from now, and is woken at
            // once, with the packet still waiting for the timer.
            engine.unparker().unpark();
            engine.park(Some(Duration::from_secs(3600))).unwrap();
            let took = parks(&engine, 1, Duration::from_millis(3));
            assert!(
                took < Duration::from_millis(15),
                "a park of 3 ms took {took:?}"
            );
        }
    }

    #[test]
    fn a_packet_of_an_earlier_deadline_does_not_end_a_later_park() {
        let engine = Builder::new().build().unwrap();
        if !has_fine_timer(&engine) {
            return;
        }
        // The park sets the timer for a millisecond from now and is woken
        // at once; the timer then expires with no turn to take its packet.
        engine.unparker().unpark();
        engine.park(Some(Duration::from_millis(1))).unwrap();
        std::thread::sleep(Duration::from_millis(30));
        let started = Instant::now();
        engine.park(Some(Duration::from_millis(40))).unwrap();
        let took = started.elapsed();
        assert!(took >= Duration::from_millis(40), "the park took {took:?}");
        // Long before the tick after it, which is as late as a wait that
        // the timer does not end can be.
        assert!(took < Duration::from_millis(50), "the park took {took:?}");
    }

    #[test]
    fn the_timer_wakes_a_park_without_a_timeout_when_it_is_due() {
        let engine = Builder::new().build().unwrap();
        if !has_fine_timer(&engine) {
            return;
        }
        let took = (0..3)
            .map(|_| {
                let started = Instant::now();
                engine
                    .wake_at(Iocp::monotonic_now() + Duration::from_millis(3))
                    .unwrap();
                engine.park(None).unwrap();
                assert!(engine.timer_fired());
                started.elapsed()
            })
            .min()
            .unwrap();
        assert!(took >= Duration::from_millis(3), "the park took {took:?}");
        assert!(took < Duration::from_millis(15), "the park took {took:?}");
    }
}
