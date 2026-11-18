//! The writes left behind a stream: how those in flight go, which the
//! writes after them wait to know.

use std::cell::{Cell, RefCell};
use std::fmt;
use std::io;
use std::mem;
use std::task::{Context, Poll, Waker, ready};

/// The writes behind a stream: how many sends of them are still to finish,
/// and the error the first to fail failed with.
///
/// Nobody waits for a write behind, so how it went can only be reported
/// later. A write that failed, or was cut short, leaves a gap in the
/// stream, so every write after it fails with its error, as a stream's
/// writes do once it has broken.
#[derive(Default)]
pub(crate) struct Behind {
    /// The sends left behind the stream that have not finished.
    in_flight: Cell<usize>,
    /// The error, as the system's code: the writes it fails report it as
    /// the system would have.
    error: Cell<Option<i32>>,
    /// Tasks waiting for the sends in flight to finish.
    waiters: RefCell<Vec<Waker>>,
}

impl Behind {
    /// Ready once no write behind is in flight: `Ok`, unless one failed.
    pub(crate) fn poll_done(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.in_flight.get() > 0 {
            let waits = self
                .waiters
                .borrow()
                .iter()
                .any(|waiter| waiter.will_wake(cx.waker()));
            if !waits {
                // Cloned outside the borrow, as cloning runs code of the
                // task's.
                let waker = cx.waker().clone();
                self.waiters.borrow_mut().push(waker);
            }
            return Poll::Pending;
        }
        Poll::Ready(self.outcome())
    }

    /// How the writes behind went, if none is in flight, without arranging
    /// to be told when one finishes: `None` while one is.
    pub(crate) fn status(&self) -> Option<io::Result<()>> {
        (self.in_flight.get() == 0).then(|| self.outcome())
    }

    /// `Ok`, unless a write behind failed.
    fn outcome(&self) -> io::Result<()> {
        match self.error.get() {
            None => Ok(()),
            Some(code) => Err(io::Error::from_raw_os_error(code)),
        }
    }

    /// Counts a send left behind the stream as in flight.
    pub(crate) fn started(&self) {
        self.in_flight.set(self.in_flight.get() + 1);
    }

    /// Records how a send left behind the stream went, failing with `error`
    /// if it is one, and, once the last in flight has finished, wakes the
    /// tasks waiting for them if `wake`, and lets go of them if not. The
    /// error kept is the first.
    pub(crate) fn finished(&self, error: Option<i32>, wake: bool) {
        if self.error.get().is_none() {
            self.error.set(error);
        }
        let left = self.in_flight.get() - 1;
        self.in_flight.set(left);
        if left > 0 {
            return;
        }
        // Woken, or dropped, outside the borrow, as either runs code of the
        // task's, and the vector goes back, with its room, for the next
        // write's waiters, unless one has already been put there meanwhile.
        let mut waiters = mem::take(&mut *self.waiters.borrow_mut());
        for waiter in waiters.drain(..) {
            if wake {
                waiter.wake();
            }
        }
        let mut current = self.waiters.borrow_mut();
        if current.is_empty() {
            *current = waiters;
        }
    }
}

impl fmt::Debug for Behind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Behind")
            .field("in_flight", &self.in_flight.get())
            .field("error", &self.error.get())
            .finish_non_exhaustive()
    }
}

/// Waits, before a write to a stream starts, for the writes behind the
/// stream that `gate` names, if it names any: ready with `Ok` once none is
/// in flight, after which the write waits for none again, or with the error
/// that one failed with.
pub(crate) fn poll_gate(gate: &mut Option<&Behind>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    let Some(behind) = *gate else {
        return Poll::Ready(Ok(()));
    };
    ready!(behind.poll_done(cx))?;
    *gate = None;
    Poll::Ready(Ok(()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Wake;

    use super::*;

    /// A task's waker, which counts how often the task was woken.
    struct Woken(AtomicUsize);

    impl Wake for Woken {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    impl Woken {
        fn times(&self) -> usize {
            self.0.load(Ordering::Relaxed)
        }
    }

    fn task() -> (Arc<Woken>, Waker) {
        let woken = Arc::new(Woken(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&woken));
        (woken, waker)
    }

    /// The system's code of the error `result` is, if it is one.
    fn code(result: io::Result<()>) -> Option<i32> {
        result.err().and_then(|err| err.raw_os_error())
    }

    /// Whether the writes behind are done, as the task of `waker` asks, and
    /// with which error if so.
    fn done(behind: &Behind, waker: &Waker) -> Poll<Option<i32>> {
        behind.poll_done(&mut Context::from_waker(waker)).map(code)
    }

    #[test]
    fn with_nothing_in_flight_nothing_is_waited_for() {
        let behind = Behind::default();
        let (woken, waker) = task();
        assert_eq!(done(&behind, &waker), Poll::Ready(None));
        assert_eq!(behind.status().map(code), Some(None));
        let mut gate = Some(&behind);
        let passed = poll_gate(&mut gate, &mut Context::from_waker(&waker));
        assert!(matches!(passed, Poll::Ready(Ok(()))));
        assert!(gate.is_none());
        // A write of what has no writes behind it has no gate.
        let passed = poll_gate(&mut None, &mut Context::from_waker(&waker));
        assert!(matches!(passed, Poll::Ready(Ok(()))));
        assert_eq!(woken.times(), 0);
        assert!(format!("{behind:?}").contains("in_flight: 0"));
    }

    #[test]
    fn those_waiting_are_woken_once_the_last_send_has_finished() {
        let behind = Behind::default();
        let ((woken_first, first), (woken_second, second)) = (task(), task());
        behind.started();
        behind.started();
        assert!(done(&behind, &first).is_pending());
        assert!(done(&behind, &second).is_pending());
        assert!(behind.status().is_none());

        behind.finished(None, true);
        assert!(behind.status().is_none());
        assert_eq!((woken_first.times(), woken_second.times()), (0, 0));
        behind.finished(None, true);
        assert_eq!((woken_first.times(), woken_second.times()), (1, 1));
        assert_eq!(done(&behind, &first), Poll::Ready(None));
        assert_eq!(behind.status().map(code), Some(None));

        // The next write behind is waited for anew.
        behind.started();
        assert!(done(&behind, &second).is_pending());
        behind.finished(None, true);
        assert_eq!((woken_first.times(), woken_second.times()), (1, 2));
    }

    #[test]
    fn the_first_error_fails_every_write_after_it() {
        let behind = Behind::default();
        let (_woken, waker) = task();
        behind.started();
        behind.started();
        behind.finished(Some(5), true);
        // Not known until the last has finished.
        assert!(behind.status().is_none());
        behind.finished(Some(7), true);
        assert_eq!(behind.status().map(code), Some(Some(5)));
        assert_eq!(done(&behind, &waker), Poll::Ready(Some(5)));

        // A write waits at its gate for nothing more, and never passes it.
        let mut gate = Some(&behind);
        for _ in 0..2 {
            let refused = poll_gate(&mut gate, &mut Context::from_waker(&waker));
            assert_eq!(refused.map(code), Poll::Ready(Some(5)));
            assert!(gate.is_some());
        }

        // A send that goes well after it mends nothing.
        behind.started();
        behind.finished(None, true);
        assert_eq!(behind.status().map(code), Some(Some(5)));
    }

    #[test]
    fn a_write_waits_at_its_gate_for_the_sends_in_flight() {
        let behind = Behind::default();
        let (woken, waker) = task();
        behind.started();
        let mut gate = Some(&behind);
        let held = poll_gate(&mut gate, &mut Context::from_waker(&waker));
        assert!(held.is_pending());
        assert!(gate.is_some());

        behind.finished(None, true);
        assert_eq!(woken.times(), 1);
        let passed = poll_gate(&mut gate, &mut Context::from_waker(&waker));
        assert!(matches!(passed, Poll::Ready(Ok(()))));
        assert!(gate.is_none());
        // Once past its gate, a write waits for no write behind made since.
        behind.started();
        let passed = poll_gate(&mut gate, &mut Context::from_waker(&waker));
        assert!(matches!(passed, Poll::Ready(Ok(()))));
    }

    #[test]
    fn those_waiting_are_let_go_of_when_they_may_not_be_woken() {
        let behind = Behind::default();
        let (woken, waker) = task();
        behind.started();
        assert!(done(&behind, &waker).is_pending());
        // The task, its waker, and the waker kept for it.
        assert_eq!(Arc::strong_count(&woken), 3);
        behind.finished(Some(9), false);
        assert_eq!(woken.times(), 0);
        assert_eq!(Arc::strong_count(&woken), 2);
        assert_eq!(done(&behind, &waker), Poll::Ready(Some(9)));
    }
}
