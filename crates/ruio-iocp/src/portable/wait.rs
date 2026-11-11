//! Waits for handles to be signaled, which any thread may hold and poll.
//!
//! A [`HandleWait`] runs on the port of the engine current on the thread
//! that polls it as it starts: it associates a wait completion packet of
//! its own with the handle, which the system posts to that port, under the
//! engine's key for such packets, once the handle is signaled, and the
//! engine's turn hands it on as a wake of the task that polled the wait
//! last, on whichever thread that task runs.
//!
//! The engine keeps the waits it has, in [`Waits`], so that it can cancel
//! them as it is dropped: their tasks are woken, and their next polls start
//! them again on the engines of their threads then. A wait dropped or
//! cancelled on any thread takes its packet back from the system, unless
//! the engine has taken the packet from the port already, in which case
//! the engine lets go of the wait as it delivers it. The system holds the
//! object a packet waits for while it does, so closing the handle meanwhile
//! changes nothing.

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::os::windows::io::{AsRawHandle, BorrowedHandle};
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

use crate::engine::{Iocp, SIGNALED, Shared};
use crate::sys;

/// The most idle packets kept for the waits to come.
const SPARE_PACKETS: usize = 64;

/// Packets of waits since dropped, idle, by their handles' values, for the
/// next waits to take rather than make packets of their own.
static SPARE: Mutex<Vec<usize>> = Mutex::new(Vec::new());

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The error of a wait polled where no engine is current.
fn no_runtime() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "the thread has no ruio runtime for the wait to run on",
    )
}

/// A wait for a handle to be signaled, which any thread may hold and poll:
/// it runs on the port of the engine current on the thread that polls it as
/// it starts; see the [module](self).
///
/// [`poll_wait`](HandleWait::poll_wait) starts it, and is ready once the
/// handle has been signaled since; the wait is then idle, for the next poll
/// to start again. A wait is a packet of the system's, which the first
/// wait to start makes and a dropped one leaves for the next.
#[derive(Default)]
pub struct HandleWait {
    slot: Option<Arc<Slot>>,
}

/// A wait's packet, and where it stands, which the wait shares with the
/// engine that has it.
pub(crate) struct Slot {
    /// The wait completion packet, idle unless the state is `Waiting`.
    packet: sys::Handle,
    state: Mutex<State>,
}

// SAFETY: a wait completion packet's handle may be used from any thread,
// and the state is behind a lock.
unsafe impl Send for Slot {}

// SAFETY: as for `Send`; the calls on a packet are the system's to order.
unsafe impl Sync for Slot {}

enum State {
    /// The packet is idle.
    Idle,
    /// The packet waits for the handle, to post to the port of `engine`,
    /// whose turn then wakes `waker`.
    Waiting { engine: Arc<Shared>, waker: Waker },
    /// The handle was signaled, which the wait's next poll returns.
    Signaled,
    /// The engine the packet waited on was dropped first, and cancelled it.
    Abandoned,
}

impl HandleWait {
    /// A wait that has not started.
    pub const fn new() -> Self {
        Self { slot: None }
    }

    /// Whether the wait has started, and the handle has not been signaled
    /// since.
    pub fn is_waiting(&self) -> bool {
        self.slot
            .as_ref()
            .is_some_and(|slot| matches!(*slot.lock(), State::Waiting { .. }))
    }

    /// Waits for `handle` to be signaled: starts the wait on the port of the
    /// engine current on the thread, if it has not started, keeps `cx`'s
    /// waker to wake once the handle is, and is ready then. A handle
    /// signaled already makes it ready at once. The wait is idle once it
    /// has been ready, and the next poll starts it again.
    ///
    /// Polled again while it waits, the wait goes on where it started,
    /// whichever thread polls it, and wakes the task of the latest poll; if
    /// the engine it started on was dropped meanwhile, which woke its task,
    /// it starts again where it is polled.
    ///
    /// # Errors
    ///
    /// `Unsupported` if the wait has to start and the thread has no engine
    /// current, and what the system reports for a handle that cannot be
    /// waited for.
    pub fn poll_wait(
        &mut self,
        cx: &mut Context<'_>,
        handle: BorrowedHandle<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(slot) = &self.slot {
            let mut state = slot.lock();
            match &mut *state {
                State::Waiting { waker, .. } => {
                    waker.clone_from(cx.waker());
                    return Poll::Pending;
                }
                State::Signaled => {
                    *state = State::Idle;
                    return Poll::Ready(Ok(()));
                }
                State::Idle | State::Abandoned => *state = State::Idle,
            }
        }
        let engine = Iocp::current().ok_or_else(no_runtime)?;
        let slot = match &self.slot {
            Some(slot) => Arc::clone(slot),
            None => {
                let slot = Arc::new(Slot::new()?);
                self.slot = Some(Arc::clone(&slot));
                slot
            }
        };
        if slot.start(engine.shared(), handle, cx.waker())? {
            return Poll::Ready(Ok(()));
        }
        Poll::Pending
    }

    /// Ends the wait, if it has started, and forgets a signal it was to
    /// return: the next poll starts it again.
    pub fn cancel(&mut self) {
        let Some(slot) = &self.slot else {
            return;
        };
        if !slot.withdraw() {
            // The engine has taken the packet, and lets go of the slot as it
            // delivers it: the next wait makes a slot of its own.
            self.slot = None;
        }
    }
}

impl Drop for HandleWait {
    fn drop(&mut self) {
        self.cancel();
    }
}

impl fmt::Debug for HandleWait {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HandleWait")
            .field("waiting", &self.is_waiting())
            .finish()
    }
}

impl Slot {
    /// A slot with an idle packet: one a dropped wait left, or a new one.
    fn new() -> io::Result<Self> {
        let spare = lock(&SPARE).pop();
        let packet = match spare {
            Some(packet) => ptr::without_provenance_mut(packet),
            None => {
                let mut packet = ptr::null_mut();
                // SAFETY: the handle is written to `packet` during the call,
                // and the packet gets the default attributes.
                let status = unsafe {
                    sys::NtCreateWaitCompletionPacket(&mut packet, sys::GENERIC_ALL, ptr::null())
                };
                if status < sys::STATUS_SUCCESS {
                    return Err(io::Error::from_raw_os_error(
                        sys::RtlNtStatusToDosError(status) as i32,
                    ));
                }
                packet
            }
        };
        Ok(Self {
            packet,
            state: Mutex::new(State::Idle),
        })
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        lock(&self.state)
    }

    /// The key the slot's packet carries to the port: its address.
    fn key(self: &Arc<Self>) -> usize {
        Arc::as_ptr(self).addr()
    }

    /// Starts the wait for `handle` on the port of `engine`, the engine of
    /// the calling thread, with `waker` to wake, and says whether the handle
    /// was signaled already, when no packet follows.
    fn start(
        self: &Arc<Self>,
        engine: &Arc<Shared>,
        handle: BorrowedHandle<'_>,
        waker: &Waker,
    ) -> io::Result<bool> {
        let key = self.key();
        engine.waits.add(key, Arc::clone(self));
        *self.lock() = State::Waiting {
            engine: Arc::clone(engine),
            waker: waker.clone(),
        };
        let mut signaled = 0;
        // SAFETY: the packet is idle, and the port and the handle are open,
        // as the engine and the borrow are; the packet carries the slot's
        // address, which is no pointer the system follows, and the call
        // writes `signaled`.
        let status = unsafe {
            sys::NtAssociateWaitCompletionPacket(
                self.packet,
                engine.port(),
                handle.as_raw_handle(),
                ptr::without_provenance_mut(SIGNALED),
                ptr::without_provenance_mut(key),
                sys::STATUS_SUCCESS,
                0,
                &mut signaled,
            )
        };
        if status < sys::STATUS_SUCCESS {
            engine.waits.remove(key);
            *self.lock() = State::Idle;
            return Err(io::Error::from_raw_os_error(
                sys::RtlNtStatusToDosError(status) as i32,
            ));
        }
        // A handle signaled already has the packet posted at once, which
        // is taken back here, before the turn it would cost.
        Ok(signaled != 0 && self.withdraw())
    }

    /// Takes the packet back from the system, if it waits or lies in the
    /// port, and says whether no packet follows: true, unless the engine
    /// has taken the packet from the port already, and delivers it.
    fn withdraw(self: &Arc<Self>) -> bool {
        let engine = {
            let mut state = self.lock();
            match &*state {
                State::Waiting { engine, .. } => Arc::clone(engine),
                State::Signaled | State::Abandoned => {
                    *state = State::Idle;
                    return true;
                }
                State::Idle => return true,
            }
        };
        let mut armed = engine.waits.lock();
        // SAFETY: the packet is open. The engine, whose turn would take it,
        // waits for the lock to take it from the list, so whether it has
        // taken it is settled while this runs.
        let status = unsafe { sys::NtCancelWaitCompletionPacket(self.packet, 1) };
        if status != sys::STATUS_SUCCESS {
            return false;
        }
        engine.waits.forget(&mut armed, self.key());
        drop(armed);
        *self.lock() = State::Idle;
        true
    }

    /// Records that the handle was signaled, and gives the waker to wake.
    fn signal(&self) -> Option<Waker> {
        let mut state = self.lock();
        match std::mem::replace(&mut *state, State::Signaled) {
            State::Waiting { waker, .. } => Some(waker),
            other => {
                *state = other;
                None
            }
        }
    }

    /// Records that the engine the wait was on cancelled it, and gives the
    /// waker to wake.
    fn abandon(&self) -> Option<Waker> {
        let mut state = self.lock();
        match std::mem::replace(&mut *state, State::Abandoned) {
            State::Waiting { waker, .. } => Some(waker),
            other => {
                *state = other;
                None
            }
        }
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        // Nothing has the slot, so its packet is idle: kept for the next
        // wait, unless enough are.
        let mut spare = lock(&SPARE);
        if spare.len() < SPARE_PACKETS {
            spare.push(self.packet.addr());
            return;
        }
        drop(spare);
        // SAFETY: the packet is open, and the slot's alone.
        unsafe { sys::CloseHandle(self.packet) };
    }
}

/// The waits an engine has: those whose packets wait to post to its port,
/// or lie in it, by their keys.
#[derive(Default)]
pub(crate) struct Waits {
    armed: Mutex<HashMap<usize, Arc<Slot>>>,
    /// How many there are, which the engine's turn reads without the lock.
    count: AtomicUsize,
}

impl Waits {
    fn lock(&self) -> MutexGuard<'_, HashMap<usize, Arc<Slot>>> {
        lock(&self.armed)
    }

    /// How many waits the engine has.
    pub(crate) fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    fn add(&self, key: usize, slot: Arc<Slot>) {
        self.lock().insert(key, slot);
        self.count.fetch_add(1, Ordering::SeqCst);
    }

    fn remove(&self, key: usize) -> Option<Arc<Slot>> {
        let mut armed = self.lock();
        self.forget(&mut armed, key)
    }

    fn forget(&self, armed: &mut HashMap<usize, Arc<Slot>>, key: usize) -> Option<Arc<Slot>> {
        let slot = armed.remove(&key)?;
        self.count.fetch_sub(1, Ordering::SeqCst);
        Some(slot)
    }

    /// Delivers the packet of the wait `key`, which a turn took from the
    /// port: its task is woken, and the engine lets go of it.
    pub(crate) fn signaled(&self, key: usize) {
        let Some(slot) = self.remove(key) else {
            return;
        };
        // Woken once the slot's lock is let go of, as the wake may run code
        // of its own.
        if let Some(waker) = slot.signal() {
            waker.wake();
        }
    }

    /// Cancels the waits whose packets the system has not posted yet, and
    /// wakes their tasks, as the engine is dropped; those whose packets lie
    /// in the port are delivered as the engine takes what is left there.
    pub(crate) fn cancel_all(&self) {
        let mut woken = Vec::new();
        {
            let mut armed = self.lock();
            armed.retain(|_, slot| {
                // SAFETY: the packet is open.
                let status = unsafe { sys::NtCancelWaitCompletionPacket(slot.packet, 1) };
                if status != sys::STATUS_SUCCESS {
                    return true;
                }
                woken.extend(slot.abandon());
                false
            });
            self.count.store(armed.len(), Ordering::SeqCst);
        }
        for waker in woken {
            waker.wake();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::{Future, poll_fn};
    use std::os::windows::io::AsHandle;
    use std::pin::pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::task::{Context, Poll, Wake, Waker};
    use std::thread;
    use std::time::{Duration, Instant};

    use ruio_engine::{Build, Engine};

    use super::HandleWait;
    use crate::engine::{Builder, Iocp};

    /// A waker that counts its wakes.
    struct Woken(AtomicUsize);

    impl Wake for Woken {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn task() -> (Arc<Woken>, Waker) {
        let woken = Arc::new(Woken(AtomicUsize::new(0)));
        (Arc::clone(&woken), Waker::from(woken))
    }

    /// A thread that exits once told to, whose handle is signaled then.
    fn thread_to_end() -> (thread::JoinHandle<()>, mpsc::Sender<()>) {
        let (end, ending) = mpsc::channel::<()>();
        let thread = thread::spawn(move || {
            let _ = ending.recv();
        });
        (thread, end)
    }

    /// Turns `engine` until `woken` has been woken, for up to five seconds.
    fn turn_until_woken(engine: &Iocp, woken: &Woken) {
        let started = Instant::now();
        while woken.0.load(Ordering::SeqCst) == 0 {
            assert!(started.elapsed() < Duration::from_secs(5), "never woken");
            engine.park(Some(Duration::from_millis(50))).unwrap();
        }
    }

    #[test]
    fn a_wait_ends_once_its_handle_is_signaled() {
        let engine = Builder::new().build().unwrap();
        let _entered = engine.enter();
        let (thread, end) = thread_to_end();
        let (woken, waker) = task();
        let mut cx = Context::from_waker(&waker);
        let mut wait = HandleWait::new();
        assert!(wait.poll_wait(&mut cx, thread.as_handle()).is_pending());
        assert!(wait.is_waiting());
        end.send(()).unwrap();
        turn_until_woken(&engine, &woken);
        assert!(matches!(
            wait.poll_wait(&mut cx, thread.as_handle()),
            Poll::Ready(Ok(()))
        ));
        assert!(!wait.is_waiting());
        assert_eq!(engine.shared().waits.count(), 0);
        thread.join().unwrap();
    }

    #[test]
    fn a_handle_signaled_already_ends_the_wait_at_once() {
        let engine = Builder::new().build().unwrap();
        let _entered = engine.enter();
        let (thread, end) = thread_to_end();
        end.send(()).unwrap();
        while !thread.is_finished() {
            thread::sleep(Duration::from_millis(1));
        }
        let (_, waker) = task();
        let mut cx = Context::from_waker(&waker);
        let mut wait = HandleWait::new();
        for _ in 0..3 {
            assert!(matches!(
                wait.poll_wait(&mut cx, thread.as_handle()),
                Poll::Ready(Ok(()))
            ));
        }
        // No packet was left in the port for a turn to take.
        assert_eq!(engine.shared().waits.count(), 0);
        assert!(!engine.has_work());
        thread.join().unwrap();
    }

    #[test]
    fn a_wait_starts_again_once_it_has_ended() {
        let engine = Builder::new().build().unwrap();
        let _entered = engine.enter();
        let (woken, waker) = task();
        let mut cx = Context::from_waker(&waker);
        let mut wait = HandleWait::new();
        for _ in 0..3 {
            woken.0.store(0, Ordering::SeqCst);
            let (thread, end) = thread_to_end();
            assert!(wait.poll_wait(&mut cx, thread.as_handle()).is_pending());
            end.send(()).unwrap();
            turn_until_woken(&engine, &woken);
            assert_eq!(woken.0.load(Ordering::SeqCst), 1);
            assert!(wait.poll_wait(&mut cx, thread.as_handle()).is_ready());
            thread.join().unwrap();
        }
    }

    #[test]
    fn a_wait_dropped_on_another_thread_leaves_the_engine_nothing() {
        let engine = Builder::new().build().unwrap();
        let _entered = engine.enter();
        let (thread, end) = thread_to_end();
        let (woken, waker) = task();
        let mut wait = HandleWait::new();
        assert!(
            wait.poll_wait(&mut Context::from_waker(&waker), thread.as_handle())
                .is_pending()
        );
        assert_eq!(engine.shared().waits.count(), 1);
        thread::spawn(move || drop(wait)).join().unwrap();
        assert_eq!(engine.shared().waits.count(), 0);
        end.send(()).unwrap();
        thread.join().unwrap();
        // Nothing comes to the port, so the park waits out its timeout.
        let started = Instant::now();
        engine.park(Some(Duration::from_millis(30))).unwrap();
        assert!(started.elapsed() >= Duration::from_millis(30));
        assert_eq!(woken.0.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn a_wait_polled_on_another_thread_wakes_its_task_there() {
        let engine = Builder::new().build().unwrap();
        let _entered = engine.enter();
        let (thread, end) = thread_to_end();
        let (_, waker) = task();
        let mut wait = HandleWait::new();
        assert!(
            wait.poll_wait(&mut Context::from_waker(&waker), thread.as_handle())
                .is_pending()
        );
        // A thread with no engine of its own polls the wait that goes on on
        // this engine's port, and waits to be woken.
        let handle = thread.as_handle().try_clone_to_owned().unwrap();
        let waiter = thread::spawn(move || {
            let (woken, waker) = task();
            let mut cx = Context::from_waker(&waker);
            assert!(wait.poll_wait(&mut cx, handle.as_handle()).is_pending());
            let started = Instant::now();
            while woken.0.load(Ordering::SeqCst) == 0 {
                assert!(started.elapsed() < Duration::from_secs(5), "never woken");
                thread::sleep(Duration::from_millis(1));
            }
            assert!(wait.poll_wait(&mut cx, handle.as_handle()).is_ready());
        });
        thread::sleep(Duration::from_millis(20));
        end.send(()).unwrap();
        while !waiter.is_finished() {
            engine.park(Some(Duration::from_millis(10))).unwrap();
        }
        waiter.join().unwrap();
        thread.join().unwrap();
    }

    #[test]
    fn a_dropped_engine_cancels_its_waits_which_start_again_elsewhere() {
        let (thread, end) = thread_to_end();
        let (woken, waker) = task();
        let mut cx = Context::from_waker(&waker);
        let mut wait = HandleWait::new();
        {
            let first = Builder::new().build().unwrap();
            let _entered = first.enter();
            assert!(wait.poll_wait(&mut cx, thread.as_handle()).is_pending());
        }
        // The first engine is gone, and woke the task as it went.
        assert_eq!(woken.0.load(Ordering::SeqCst), 1);
        assert!(!wait.is_waiting());
        let second = Builder::new().build().unwrap();
        let _entered = second.enter();
        assert!(wait.poll_wait(&mut cx, thread.as_handle()).is_pending());
        end.send(()).unwrap();
        while woken.0.load(Ordering::SeqCst) < 2 {
            second.park(Some(Duration::from_millis(50))).unwrap();
        }
        assert!(wait.poll_wait(&mut cx, thread.as_handle()).is_ready());
        thread.join().unwrap();
    }

    #[test]
    fn a_wait_with_no_engine_current_is_unsupported() {
        let (thread, end) = thread_to_end();
        let mut wait = HandleWait::new();
        let polled = pin!(poll_fn(|cx| wait.poll_wait(cx, thread.as_handle())));
        let (_, waker) = task();
        match polled.poll(&mut Context::from_waker(&waker)) {
            Poll::Ready(Err(err)) => assert_eq!(err.kind(), std::io::ErrorKind::Unsupported),
            other => panic!("the wait gave {other:?}"),
        }
        end.send(()).unwrap();
        thread.join().unwrap();
    }
}
