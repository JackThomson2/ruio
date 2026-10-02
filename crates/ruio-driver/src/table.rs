//! The operation table: a slot for every operation the kernel may still own.
//!
//! An operation's `user_data` is its slot index in the low 32 bits and the
//! slot's generation in the high 32. A slot's generation changes every time it
//! is freed, so a completion for an earlier occupant of the slot is recognised
//! as stale.

use std::collections::VecDeque;
use std::mem;
use std::ptr;
use std::task::Waker;

use ruio_sys as sys;

use crate::op::Completion;
use crate::waker::LocalWaker;

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
    /// Slots whose request the kernel has not finished.
    in_flight: usize,
    /// The wakers of the executor on this thread, held without cloning; see
    /// [`LocalWaker`].
    local: Option<LocalWaker>,
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
    Waiting(Wake),
    /// Completed; the result waits for its future to collect it.
    Completed(Completion),
    /// Submitted, and its future dropped; the payload waits for completion.
    Orphaned(P),
    /// A multishot request; its completions queue until its stream takes
    /// them.
    Streaming(Stream),
    /// A multishot request whose stream was dropped: its payload handles the
    /// completions still to come.
    OrphanedStream(OrphanStream<P>),
}

/// How a slot wakes the task waiting on it.
///
/// A local wake is only ever let go of through [`Table::release`], which
/// hands its reference back to the executor; dropping it would leak that.
pub(crate) enum Wake {
    /// Nobody waits.
    None,
    /// A waker of the executor on this thread, held by the `data` of the
    /// waker, which the executor counts; see [`LocalWaker`].
    Local(*const ()),
    /// Any other waker, cloned.
    Foreign(Waker),
}

struct Stream {
    completions: VecDeque<Completion>,
    waker: Wake,
    /// Whether the last completion, without `IORING_CQE_F_MORE`, arrived.
    ended: bool,
}

struct OrphanStream<P> {
    /// Absent while the driver hands it completions.
    payload: Option<P>,
    completions: VecDeque<Completion>,
    ended: bool,
}

/// Whether `completion` is the last of its request.
fn is_last(completion: &Completion) -> bool {
    completion.flags() & sys::IORING_CQE_F_MORE == 0
}

/// What a completion means for the table.
pub(crate) enum Outcome<P> {
    /// An operation completed; wake its task, unless the table has, with a
    /// local waker.
    Wake(Option<Waker>),
    /// An orphaned operation completed; its payload can be released.
    Release(P),
    /// An orphaned stream has completions for its payload.
    Deliver(u64),
    /// The completion belongs to no current operation.
    Ignore,
}

/// What orphaning a stream found.
pub(crate) struct Abandoned {
    /// The stream's waker, for the caller to drop after releasing the table.
    pub(crate) waker: Option<Waker>,
    /// Completions the stream had not taken, for its payload.
    pub(crate) completions: VecDeque<Completion>,
    /// Whether the request had ended; if so the slot is freed.
    pub(crate) ended: bool,
}

impl<P> Table<P> {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            slots: Vec::with_capacity(capacity),
            free: NONE,
            in_flight: 0,
            local: None,
        }
    }

    /// Holds the wakers `local` describes by reference from now on.
    pub(crate) fn set_local(&mut self, local: LocalWaker) {
        self.local = Some(local);
    }

    /// Operations the kernel may still own: waiting or orphaned.
    pub(crate) fn in_flight(&self) -> usize {
        self.in_flight
    }

    /// How a slot is to wake `waker`: by a counted reference if it is one of
    /// the executor's on this thread, and otherwise by a clone.
    pub(crate) fn wake_of(&self, waker: &Waker) -> Wake {
        match self.local {
            Some(local) if ptr::eq(waker.vtable(), local.vtable) => {
                // SAFETY: the waker's vtable is the executor's, so its data
                // names a task of this thread, which the executor keeps alive
                // while the reference is counted; see `LocalWaker`.
                unsafe { (local.retain)(waker.data()) };
                Wake::Local(waker.data())
            }
            _ => Wake::Foreign(waker.clone()),
        }
    }

    /// Whether `wake` wakes the task `waker` does.
    fn will_wake(&self, wake: &Wake, waker: &Waker) -> bool {
        match wake {
            Wake::None => false,
            Wake::Local(data) => {
                ptr::eq(*data, waker.data())
                    && self
                        .local
                        .is_some_and(|local| ptr::eq(waker.vtable(), local.vtable))
            }
            Wake::Foreign(current) => current.will_wake(waker),
        }
    }

    /// Lets go of `wake`: a local one is handed back to the executor, and a
    /// foreign one returned, for the caller to drop after releasing the
    /// table, since dropping a waker can run arbitrary code.
    fn release(&self, wake: Wake) -> Option<Waker> {
        match wake {
            Wake::None => None,
            Wake::Local(data) => {
                let local = self.local.expect("a local wake comes from a local waker");
                // SAFETY: `data` was retained through this table's local
                // waker, and is released once.
                unsafe { (local.release)(data) };
                None
            }
            Wake::Foreign(waker) => Some(waker),
        }
    }

    /// Wakes `wake`: a local one here, as the executor is on this thread,
    /// and a foreign one by returning it, for the caller to wake after
    /// releasing the table. `wake` stays as it is.
    fn wake(&self, wake: &mut Wake) -> Option<Waker> {
        match wake {
            Wake::None => None,
            Wake::Local(data) => {
                let local = self.local.expect("a local wake comes from a local waker");
                // SAFETY: `data` is retained through this table's local
                // waker, so the task it names is alive.
                unsafe { (local.wake)(*data) };
                None
            }
            Wake::Foreign(_) => match mem::replace(wake, Wake::None) {
                Wake::Foreign(waker) => Some(waker),
                _ => unreachable!(),
            },
        }
    }

    /// Adds a submitted operation and returns its `user_data`.
    pub(crate) fn insert(&mut self, waker: Option<&Waker>) -> u64 {
        let wake = waker.map_or(Wake::None, |waker| self.wake_of(waker));
        self.insert_wake(wake)
    }

    /// Adds a submitted operation to be woken by `wake`, and returns its
    /// `user_data`.
    pub(crate) fn insert_wake(&mut self, wake: Wake) -> u64 {
        let index = self.occupy();
        let slot = &mut self.slots[index as usize];
        // Written into the slot where it is, rather than built elsewhere
        // and copied: this is the table's hottest path. The slot was free,
        // and so owned nothing.
        forget_trivial(mem::replace(&mut slot.state, State::Waiting(wake)));
        user_data(index, slot.generation)
    }

    /// Adds a submitted multishot request and returns its `user_data`.
    pub(crate) fn insert_stream(&mut self, waker: Option<&Waker>) -> u64 {
        let waker = waker.map_or(Wake::None, |waker| self.wake_of(waker));
        let index = self.occupy();
        let slot = &mut self.slots[index as usize];
        let stream = State::Streaming(Stream {
            completions: VecDeque::new(),
            waker,
            ended: false,
        });
        forget_trivial(mem::replace(&mut slot.state, stream));
        user_data(index, slot.generation)
    }

    /// Takes a free slot, which stays in its free state for the caller to
    /// write, counting it in flight, and returns its index.
    fn occupy(&mut self) -> u32 {
        let index = if self.free == NONE {
            let index = u32::try_from(self.slots.len())
                .ok()
                .filter(|&index| index != NONE)
                .expect("operation table is full");
            self.slots.push(Slot {
                generation: 1,
                state: State::Free { next: NONE },
            });
            index
        } else {
            let index = self.free;
            let State::Free { next } = self.slots[index as usize].state else {
                unreachable!("free list points at a slot in use");
            };
            self.free = next;
            index
        };
        self.in_flight += 1;
        index
    }

    /// Takes the operation's completion, freeing its slot, or, if it has not
    /// completed, arranges for `waker` to be woken when it does.
    ///
    /// A waker that `waker` replaces is returned rather than dropped, so the
    /// caller can drop it after releasing the table: dropping a waker can run
    /// arbitrary code.
    pub(crate) fn poll(
        &mut self,
        user_data: u64,
        waker: &Waker,
    ) -> Result<Completion, Option<Waker>> {
        let Some(index) = self.index(user_data) else {
            debug_assert!(false, "polled an operation that is not in the table");
            return Err(None);
        };
        match &self.slots[index as usize].state {
            State::Completed(completion) => {
                let completion = *completion;
                forget_trivial(self.free(index));
                Ok(completion)
            }
            State::Waiting(current) => {
                if self.will_wake(current, waker) {
                    return Err(None);
                }
                let wake = self.wake_of(waker);
                let State::Waiting(current) = &mut self.slots[index as usize].state else {
                    unreachable!();
                };
                let replaced = mem::replace(current, wake);
                Err(self.release(replaced))
            }
            State::Orphaned(_)
            | State::Streaming(_)
            | State::OrphanedStream(_)
            | State::Free { .. } => {
                debug_assert!(false, "polled an operation that has no future");
                Err(None)
            }
        }
    }

    /// Takes the stream's next completion, freeing the slot after the last,
    /// or, if none is queued, arranges for `waker` to be woken when one
    /// arrives. A replaced waker is returned, as by [`Table::poll`], and so
    /// is the stream's own after the last completion.
    pub(crate) fn poll_stream(
        &mut self,
        user_data: u64,
        waker: &Waker,
    ) -> Result<Completion, Option<Waker>> {
        let Some(index) = self.index(user_data) else {
            debug_assert!(false, "polled a stream that is not in the table");
            return Err(None);
        };
        let State::Streaming(stream) = &mut self.slots[index as usize].state else {
            debug_assert!(false, "polled a stream that is not streaming");
            return Err(None);
        };
        if let Some(completion) = stream.completions.pop_front() {
            if is_last(&completion) {
                drop(self.free_stream(index));
            }
            return Ok(completion);
        }
        let State::Streaming(stream) = &self.slots[index as usize].state else {
            unreachable!();
        };
        if self.will_wake(&stream.waker, waker) {
            return Err(None);
        }
        let wake = self.wake_of(waker);
        let State::Streaming(stream) = &mut self.slots[index as usize].state else {
            unreachable!();
        };
        let replaced = mem::replace(&mut stream.waker, wake);
        Err(self.release(replaced))
    }

    /// Takes the stream's next completion if one is queued, freeing the slot
    /// after the last, and leaves its waker alone.
    pub(crate) fn take_stream(&mut self, user_data: u64) -> Option<Completion> {
        let index = self.index(user_data)?;
        let State::Streaming(stream) = &mut self.slots[index as usize].state else {
            debug_assert!(false, "took from a stream that is not streaming");
            return None;
        };
        let completion = stream.completions.pop_front()?;
        if is_last(&completion) {
            drop(self.free_stream(index));
        }
        Some(completion)
    }

    /// Detaches a stream that is being dropped, handing back what it had not
    /// taken. Unless the request had ended, the slot waits for the payload,
    /// which [`Table::adopt`] gives it.
    pub(crate) fn abandon(&mut self, user_data: u64) -> Option<Abandoned> {
        let index = self.index(user_data)?;
        let State::Streaming(stream) = &mut self.slots[index as usize].state else {
            debug_assert!(false, "abandoned a stream that is not streaming");
            return None;
        };
        let (wake, completions, ended) = (
            mem::replace(&mut stream.waker, Wake::None),
            mem::take(&mut stream.completions),
            stream.ended,
        );
        let waker = self.release(wake);
        if ended {
            drop(self.free_stream(index));
        } else {
            self.slots[index as usize].state = State::OrphanedStream(OrphanStream {
                payload: None,
                completions: VecDeque::new(),
                ended: false,
            });
        }
        Some(Abandoned {
            waker,
            completions,
            ended,
        })
    }

    /// Gives an abandoned stream's slot its payload, or hands the payload back
    /// if the slot is gone. Returns whether completions arrived meanwhile.
    pub(crate) fn adopt(&mut self, user_data: u64, payload: P) -> Result<bool, P> {
        match self.orphan_stream(user_data) {
            Some(orphan) if orphan.payload.is_none() => {
                orphan.payload = Some(payload);
                Ok(!orphan.completions.is_empty())
            }
            _ => Err(payload),
        }
    }

    /// Takes an orphaned stream's payload and the completions queued for it,
    /// freeing the slot if the last has arrived. The payload goes back with
    /// [`Table::adopt`] unless the request has ended.
    pub(crate) fn take_deliveries(
        &mut self,
        user_data: u64,
    ) -> Option<(P, VecDeque<Completion>, bool)> {
        let index = self.index(user_data)?;
        let orphan = self.orphan_stream(user_data)?;
        if orphan.completions.is_empty() {
            return None;
        }
        let payload = orphan.payload.take()?;
        let completions = mem::take(&mut orphan.completions);
        let ended = orphan.ended;
        if ended {
            self.free(index);
        }
        Some((payload, completions, ended))
    }

    fn orphan_stream(&mut self, user_data: u64) -> Option<&mut OrphanStream<P>> {
        let index = self.index(user_data)?;
        match &mut self.slots[index as usize].state {
            State::OrphanedStream(orphan) => Some(orphan),
            _ => None,
        }
    }

    /// Takes the operation's completion and frees its slot, if it has
    /// completed.
    pub(crate) fn take_completed(&mut self, user_data: u64) -> Option<Completion> {
        let index = self.index(user_data)?;
        let State::Completed(completion) = self.slots[index as usize].state else {
            return None;
        };
        forget_trivial(self.free(index));
        Some(completion)
    }

    /// Keeps the payload of a waiting operation whose future was dropped until
    /// the operation completes. Returns the operation's waker, for the caller
    /// to drop after releasing the table.
    pub(crate) fn orphan(&mut self, user_data: u64, payload: P) -> Option<Waker> {
        match self.index(user_data) {
            Some(index) if matches!(self.slots[index as usize].state, State::Waiting(_)) => {
                let state = &mut self.slots[index as usize].state;
                let State::Waiting(wake) = mem::replace(state, State::Orphaned(payload)) else {
                    unreachable!();
                };
                self.release(wake)
            }
            _ => {
                // Unreachable. Leaking is the safe response: the kernel may
                // still be using the payload.
                debug_assert!(false, "orphaned an operation that is not waiting");
                mem::forget(payload);
                None
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
            State::Waiting(wake) => {
                // Woken before the reference is let go of, so that the
                // task stays alive for the wake.
                let mut wake = mem::replace(wake, Wake::None);
                let waker = self.wake(&mut wake);
                let dropped = self.release(wake);
                debug_assert!(dropped.is_none(), "a foreign waker is taken by the wake");
                // Its wake taken, the state owns nothing.
                let state = &mut self.slots[index as usize].state;
                forget_trivial(mem::replace(state, State::Completed(completion)));
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
            State::Streaming(stream) => {
                if is_last(&completion) {
                    stream.ended = true;
                    self.in_flight -= 1;
                }
                stream.completions.push_back(completion);
                // A local waker stays, so the stream's next poll finds it
                // and clones nothing; a foreign one is taken to be woken.
                let mut wake = mem::replace(&mut stream.waker, Wake::None);
                let waker = self.wake(&mut wake);
                let State::Streaming(stream) = &mut self.slots[index as usize].state else {
                    unreachable!();
                };
                stream.waker = wake;
                Outcome::Wake(waker)
            }
            State::OrphanedStream(orphan) => {
                if is_last(&completion) {
                    orphan.ended = true;
                    self.in_flight -= 1;
                }
                orphan.completions.push_back(completion);
                Outcome::Deliver(user_data)
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
            match &mut self.slots[index].state {
                State::Orphaned(_) => {
                    let State::Orphaned(payload) = self.free(index as u32) else {
                        unreachable!();
                    };
                    self.in_flight -= 1;
                    orphans.push(payload);
                }
                State::OrphanedStream(orphan) => {
                    let (payload, ended) = (orphan.payload.take(), orphan.ended);
                    self.free(index as u32);
                    if !ended {
                        self.in_flight -= 1;
                    }
                    orphans.extend(payload);
                }
                _ => {}
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

    /// Frees a slot, returning its previous state, which owns no wake: a
    /// slot that waits is freed through [`Table::free_stream`] or has had
    /// its wake taken.
    fn free(&mut self, index: u32) -> State<P> {
        let slot = &mut self.slots[index as usize];
        // Generation 0 is never used, so a zeroed `user_data` never matches.
        slot.generation = slot.generation.wrapping_add(1).max(1);
        let state = mem::replace(&mut slot.state, State::Free { next: self.free });
        self.free = index;
        debug_assert!(
            !matches!(
                &state,
                State::Waiting(Wake::Local(_))
                    | State::Streaming(Stream {
                        waker: Wake::Local(_),
                        ..
                    })
            ),
            "a slot is freed with its local wake released"
        );
        state
    }

    /// Frees a streaming slot, letting go of its wake, and returns the
    /// stream's foreign waker if it had one, for the caller to drop.
    fn free_stream(&mut self, index: u32) -> Option<Waker> {
        let State::Streaming(stream) = &mut self.slots[index as usize].state else {
            unreachable!("only a streaming slot is freed as a stream");
        };
        let wake = mem::replace(&mut stream.waker, Wake::None);
        let waker = self.release(wake);
        drop(self.free(index));
        waker
    }
}

impl<P> Drop for Table<P> {
    fn drop(&mut self) {
        // The wakes of slots still waiting go back to the executor.
        for index in 0..self.slots.len() {
            let wake = match &mut self.slots[index].state {
                State::Waiting(wake) => mem::replace(wake, Wake::None),
                State::Streaming(stream) => mem::replace(&mut stream.waker, Wake::None),
                _ => continue,
            };
            drop(self.release(wake));
        }
    }
}

fn user_data(index: u32, generation: u32) -> u64 {
    u64::from(generation) << 32 | u64::from(index)
}

/// Lets go of a state that owns nothing: free, completed, or waiting with
/// its wake taken. Dropping it would run the state's drop glue for nothing,
/// on the hottest path of the table, so it is forgotten instead, which leaks
/// nothing of such a state.
fn forget_trivial<P>(state: State<P>) {
    debug_assert!(
        matches!(
            state,
            State::Free { .. } | State::Completed(_) | State::Waiting(Wake::None)
        ),
        "a state that owns something is dropped, not forgotten"
    );
    mem::forget(state);
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
        let op = table.insert(Some(&waker));
        assert_eq!(table.in_flight(), 1);
        assert!(matches!(table.poll(op, &waker), Err(None)));

        wake(table.complete(op, DONE));
        assert_eq!(count.0.load(Ordering::Relaxed), 1);
        assert_eq!(table.in_flight(), 0);

        assert!(matches!(table.poll(op, &waker), Ok(DONE)));
        assert!(matches!(table.complete(op, DONE), Outcome::Ignore));
    }

    #[test]
    fn the_latest_waker_is_woken() {
        let mut table = Table::<()>::with_capacity(4);
        let (first, first_waker) = waker();
        let (second, second_waker) = waker();
        let op = table.insert(Some(&first_waker));
        let replaced = table.poll(op, &second_waker).unwrap_err();
        assert!(replaced.is_some(), "the first waker is handed back");

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
        assert!(matches!(table.poll(second, &waker), Err(None)));
        assert_eq!(table.in_flight(), 1);
    }

    #[test]
    fn orphans_are_released_by_their_completion() {
        let mut table = Table::<&str>::with_capacity(4);
        let op = table.insert(None);
        assert_eq!(table.take_completed(op), None);
        assert!(table.orphan(op, "payload").is_none());
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
        let _ = table.orphan(orphaned, 7);

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
