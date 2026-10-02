//! The waker of a channel's receiver, which stays registered across waits
//! and is woken by reference, from any thread.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Waker;

/// The receiver waits to be woken.
const PARKED: usize = 1;
/// The receiver is replacing the waker.
const REGISTERING: usize = 2;
/// One waker reading the waker to wake it; the rest of the state counts
/// these.
const READER: usize = 4;

/// One waker, set by its one owner and woken by anyone.
///
/// The waker stays in its cell once registered, so that a receiver that
/// waits again from the same task clones nothing, and a wake takes it by
/// reference, so that it takes no reference either. Every change of the
/// state is a read-modify-write: a wake after a value was sent, and a
/// registration before the receiver looks for a value again, so that of a
/// wake and a registration that race, the later sees what the earlier did.
/// Each also extends the release sequence of the ones before it, so that
/// whichever reads the state acquires everything published before every
/// earlier change.
///
/// Nobody ever waits on the state: a waker that finds the receiver not
/// waiting, or the waker being replaced, leaves it, and the owner that
/// finds the waker being read wakes its new waker at once instead of
/// replacing the old one.
pub(super) struct WakerSlot {
    state: AtomicUsize,
    /// Written only by the owner, while `REGISTERING` keeps every reader
    /// out; read by the owner at any time, and by a waker counted as a
    /// reader.
    waker: UnsafeCell<Option<Waker>>,
}

// SAFETY: the waker is written only by the owner while no other thread
// reads it, and read from other threads only by reference; a `Waker` is
// `Send` and `Sync`.
unsafe impl Send for WakerSlot {}
// SAFETY: as above.
unsafe impl Sync for WakerSlot {}

impl WakerSlot {
    pub(super) fn new() -> Self {
        Self {
            state: AtomicUsize::new(0),
            waker: UnsafeCell::new(None),
        }
    }

    /// Asks for `waker` to be woken by the next [`wake`](Self::wake). The
    /// caller then looks again for what it waits for: a wake that came
    /// before this may have found nobody waiting.
    ///
    /// # Safety
    ///
    /// Only one thread at a time registers: the slot's owner.
    pub(super) unsafe fn register(&self, waker: &Waker) {
        // SAFETY: only the owner, the caller, writes the waker, so reading
        // it races only with other reads.
        let current = unsafe { &*self.waker.get() };
        if current.as_ref().is_some_and(|old| old.will_wake(waker)) {
            // Acquire: the look that follows sees every value sent before
            // a wake that came first. The waker itself is unchanged, so a
            // wake that comes later reads it whatever it acquires.
            self.state.fetch_or(PARKED, Ordering::AcqRel);
            return;
        }
        // Cloned first, so that nothing can panic while `REGISTERING` is set.
        let waker = waker.clone();
        let mut state = self.state.load(Ordering::Relaxed);
        loop {
            if state >= READER {
                // A wake is reading the old waker: rather than wait for it,
                // the task is woken now, to register when it is polled.
                waker.wake_by_ref();
                return;
            }
            // Acquire: once the last reader is gone, its reading of the old
            // waker happened before it is replaced. `PARKED` goes, so that
            // no wake starts reading meanwhile.
            match self.state.compare_exchange_weak(
                state,
                (state | REGISTERING) & !PARKED,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => state = actual,
            }
        }
        // SAFETY: `REGISTERING` keeps every wake from reading the waker,
        // and no reader was left.
        let old = unsafe { (*self.waker.get()).replace(waker) };
        // Release: a wake that sees `PARKED` reads the new waker. Acquire:
        // the look that follows sees every value sent before a wake that
        // found `REGISTERING` and left.
        self.state.fetch_xor(REGISTERING | PARKED, Ordering::AcqRel);
        // Dropped once the state is settled: dropping a waker can run any
        // code.
        drop(old);
    }

    /// Wakes the registered waker if its owner waits, after whatever it
    /// waits for has been published.
    pub(super) fn wake(&self) {
        let mut state = self.state.load(Ordering::Relaxed);
        loop {
            let waiting = state & (PARKED | REGISTERING) == PARKED;
            let new = if waiting {
                (state & !PARKED) + READER
            } else {
                state
            };
            // A change even when nothing changes, so that the registration
            // that follows, if this comes first, acquires what was
            // published before it. Release for that; acquire, so that the
            // waker read is the one registered.
            match self
                .state
                .compare_exchange_weak(state, new, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) if waiting => break,
                Ok(_) => return,
                Err(actual) => state = actual,
            }
        }
        /// Counts the reader out, even if the wake panics.
        struct Reading<'a>(&'a AtomicUsize);
        impl Drop for Reading<'_> {
            fn drop(&mut self) {
                // Release: the reading of the waker happens before the
                // owner, which acquires this, replaces it.
                self.0.fetch_sub(READER, Ordering::Release);
            }
        }
        let _reading = Reading(&self.state);
        // SAFETY: counted as a reader, which keeps the owner from replacing
        // the waker until this is done.
        let waker = unsafe { &*self.waker.get() };
        if let Some(waker) = waker {
            waker.wake_by_ref();
        }
    }
}
