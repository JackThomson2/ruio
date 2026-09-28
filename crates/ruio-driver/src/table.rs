//! The operation table: a slot for every operation the kernel may still own.
//!
//! An operation's `user_data` is its slot index in the low 32 bits and the
//! slot's generation in the high 32. A slot's generation changes every time it
//! is freed, so a completion for an earlier occupant of the slot is recognised
//! as stale.

use std::mem;
use std::task::Waker;

use crate::op::Completion;

/// The `user_data` of internal requests, such as cancellations, whose
/// completions are ignored.
pub(crate) const IGNORED: u64 = u64::MAX;

/// Marks the end of the free list.
const NONE: u32 = u32::MAX;

/// The operations the kernel has been given, keyed by `user_data`.
///
/// `P` is the payload of an operation whose future was dropped before it
/// completed: the memory the kernel may still use, kept until the completion
/// arrives.
pub(crate) struct Table<P> {
    slots: Vec<Slot<P>>,
    /// Head of the list of free slots, threaded through `State::Free`.
    free: u32,
    /// Slots that are waiting or orphaned.
    in_flight: usize,
}

struct Slot<P> {
    generation: u32,
    state: State<P>,
}

enum State<P> {
    Free {
        next: u32,
    },
    /// Submitted and not yet completed; completion wakes the waker.
    Waiting(Option<Waker>),
    /// Completed; the result waits for its future to collect it.
    Completed(Completion),
    /// Submitted, and its future dropped; the payload waits for completion.
    Orphaned(P),
}

/// What a completion means for the table.
pub(crate) enum Outcome<P> {
    /// An operation completed; wake its task.
    Wake(Option<Waker>),
    /// An orphaned operation completed; its payload can be released.
    Release(P),
    /// The completion belongs to no current operation.
    Ignore,
}

impl<P> Table<P> {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            slots: Vec::with_capacity(capacity),
            free: NONE,
            in_flight: 0,
        }
    }

    /// Operations the kernel may still own: waiting or orphaned.
    pub(crate) fn in_flight(&self) -> usize {
        self.in_flight
    }

    /// Adds a submitted operation and returns its `user_data`.
    pub(crate) fn insert(&mut self, waker: Option<Waker>) -> u64 {
        let index = if self.free == NONE {
            let index = u32::try_from(self.slots.len())
                .ok()
                .filter(|&index| index != NONE)
                .expect("operation table is full");
            self.slots.push(Slot {
                generation: 1,
                state: State::Waiting(waker),
            });
            index
        } else {
            let index = self.free;
            let slot = &mut self.slots[index as usize];
            let State::Free { next } = slot.state else {
                unreachable!("free list points at a slot in use");
            };
            self.free = next;
            slot.state = State::Waiting(waker);
            index
        };
        self.in_flight += 1;
        user_data(index, self.slots[index as usize].generation)
    }

    /// Takes the operation's completion, freeing its slot, or, if it has not
    /// completed, arranges for `waker` to be woken when it does.
    pub(crate) fn poll(&mut self, user_data: u64, waker: &Waker) -> Option<Completion> {
        let index = self.index(user_data)?;
        match &mut self.slots[index as usize].state {
            State::Completed(completion) => {
                let completion = *completion;
                self.free(index);
                Some(completion)
            }
            State::Waiting(current) => {
                if !current
                    .as_ref()
                    .is_some_and(|current| current.will_wake(waker))
                {
                    *current = Some(waker.clone());
                }
                None
            }
            State::Orphaned(_) | State::Free { .. } => {
                debug_assert!(false, "polled an operation that has no future");
                None
            }
        }
    }

    /// Takes the operation's completion and frees its slot, if it has
    /// completed.
    pub(crate) fn take_completed(&mut self, user_data: u64) -> Option<Completion> {
        let index = self.index(user_data)?;
        let State::Completed(completion) = self.slots[index as usize].state else {
            return None;
        };
        self.free(index);
        Some(completion)
    }

    /// Keeps the payload of a waiting operation whose future was dropped until
    /// the operation completes.
    pub(crate) fn orphan(&mut self, user_data: u64, payload: P) {
        match self.index(user_data) {
            Some(index) if matches!(self.slots[index as usize].state, State::Waiting(_)) => {
                self.slots[index as usize].state = State::Orphaned(payload);
            }
            _ => {
                // Unreachable. Leaking is the safe response: the kernel may
                // still be using the payload.
                debug_assert!(false, "orphaned an operation that is not waiting");
                mem::forget(payload);
            }
        }
    }

    /// Records the kernel's completion for `user_data`.
    pub(crate) fn complete(&mut self, user_data: u64, completion: Completion) -> Outcome<P> {
        let Some(index) = self.index(user_data) else {
            return Outcome::Ignore;
        };
        let state = &mut self.slots[index as usize].state;
        match state {
            State::Waiting(waker) => {
                let waker = waker.take();
                *state = State::Completed(completion);
                self.in_flight -= 1;
                Outcome::Wake(waker)
            }
            State::Orphaned(_) => {
                let State::Orphaned(payload) = self.free(index) else {
                    unreachable!();
                };
                self.in_flight -= 1;
                Outcome::Release(payload)
            }
            State::Completed(_) | State::Free { .. } => {
                debug_assert!(false, "second completion for {user_data:#x}");
                Outcome::Ignore
            }
        }
    }

    /// Removes the payloads of all orphaned operations, for when the kernel
    /// will never complete them.
    pub(crate) fn take_orphans(&mut self) -> Vec<P> {
        let mut orphans = Vec::new();
        for index in 0..self.slots.len() {
            if matches!(self.slots[index].state, State::Orphaned(_)) {
                let State::Orphaned(payload) = self.free(index as u32) else {
                    unreachable!();
                };
                self.in_flight -= 1;
                orphans.push(payload);
            }
        }
        orphans
    }

    /// The slot index `user_data` names, if its generation is current and the
    /// slot is in use.
    fn index(&self, user_data: u64) -> Option<u32> {
        if user_data == IGNORED {
            return None;
        }
        let index = user_data as u32;
        let slot = self.slots.get(index as usize)?;
        let current = slot.generation == (user_data >> 32) as u32;
        (current && !matches!(slot.state, State::Free { .. })).then_some(index)
    }

    /// Frees a slot, returning its previous state.
    fn free(&mut self, index: u32) -> State<P> {
        let slot = &mut self.slots[index as usize];
        // Generation 0 is never used, so a zeroed `user_data` never matches.
        slot.generation = slot.generation.wrapping_add(1).max(1);
        let state = mem::replace(&mut slot.state, State::Free { next: self.free });
        self.free = index;
        state
    }
}

fn user_data(index: u32, generation: u32) -> u64 {
    u64::from(generation) << 32 | u64::from(index)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Wake;

    use super::*;

    #[derive(Default)]
    struct Count(AtomicUsize);

    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn waker() -> (Arc<Count>, Waker) {
        let count = Arc::new(Count::default());
        (count.clone(), Waker::from(count))
    }

    fn wake(outcome: Outcome<()>) {
        match outcome {
            Outcome::Wake(waker) => waker.expect("a waker").wake(),
            _ => panic!("expected a wake"),
        }
    }

    const DONE: Completion = Completion::new(5, 0);

    #[test]
    fn completion_wakes_and_is_collected_once() {
        let mut table = Table::<()>::with_capacity(4);
        let (count, waker) = waker();
        let op = table.insert(Some(waker.clone()));
        assert_eq!(table.in_flight(), 1);
        assert_eq!(table.poll(op, &waker), None);

        wake(table.complete(op, DONE));
        assert_eq!(count.0.load(Ordering::Relaxed), 1);
        assert_eq!(table.in_flight(), 0);

        assert_eq!(table.poll(op, &waker), Some(DONE));
        assert!(matches!(table.complete(op, DONE), Outcome::Ignore));
    }

    #[test]
    fn the_latest_waker_is_woken() {
        let mut table = Table::<()>::with_capacity(4);
        let (first, first_waker) = waker();
        let (second, second_waker) = waker();
        let op = table.insert(Some(first_waker));
        assert_eq!(table.poll(op, &second_waker), None);

        wake(table.complete(op, DONE));
        assert_eq!(first.0.load(Ordering::Relaxed), 0);
        assert_eq!(second.0.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn freed_slots_are_reused_with_a_new_generation() {
        let mut table = Table::<()>::with_capacity(4);
        let (_, waker) = waker();
        let first = table.insert(None);
        let _ = table.complete(first, DONE);
        assert_eq!(table.take_completed(first), Some(DONE));

        let second = table.insert(None);
        assert_eq!(second as u32, first as u32, "same slot");
        assert_ne!(second, first, "new generation");
        assert_eq!(table.slots.len(), 1);

        // A late completion for the first occupant must not touch the second.
        assert!(matches!(table.complete(first, DONE), Outcome::Ignore));
        assert_eq!(table.poll(second, &waker), None);
        assert_eq!(table.in_flight(), 1);
    }

    #[test]
    fn orphans_are_released_by_their_completion() {
        let mut table = Table::<&str>::with_capacity(4);
        let op = table.insert(None);
        assert_eq!(table.take_completed(op), None);
        table.orphan(op, "payload");
        assert_eq!(table.in_flight(), 1);

        assert!(matches!(
            table.complete(op, DONE),
            Outcome::Release("payload")
        ));
        assert_eq!(table.in_flight(), 0);
        assert!(matches!(table.complete(op, DONE), Outcome::Ignore));
    }

    #[test]
    fn completed_operations_can_be_taken_instead_of_orphaned() {
        let mut table = Table::<()>::with_capacity(4);
        let op = table.insert(None);
        let _ = table.complete(op, DONE);
        assert_eq!(table.take_completed(op), Some(DONE));
        assert_eq!(table.in_flight(), 0);
    }

    #[test]
    fn orphans_can_be_taken_back() {
        let mut table = Table::<u32>::with_capacity(4);
        let waiting = table.insert(None);
        let orphaned = table.insert(None);
        table.orphan(orphaned, 7);

        assert_eq!(table.take_orphans(), [7]);
        assert_eq!(table.in_flight(), 1);
        assert!(matches!(table.complete(orphaned, DONE), Outcome::Ignore));
        assert!(matches!(table.complete(waiting, DONE), Outcome::Wake(None)));
    }

    #[test]
    fn internal_completions_are_ignored() {
        let mut table = Table::<()>::with_capacity(4);
        assert!(matches!(table.complete(IGNORED, DONE), Outcome::Ignore));
        assert!(matches!(table.complete(0, DONE), Outcome::Ignore));
    }
}
