//! A hierarchical timing wheel.
//!
//! Time is counted in ticks since the wheel was created; the runtime uses
//! milliseconds. There are six levels of 64 slots, and a slot on level `n`
//! spans `64^n` ticks: level 0 holds the timers due in the current 64-tick
//! window, level 1 those due in the current 4096-tick window, and so on, to
//! 2^36 ticks (about 795 days at a millisecond each). A timer sits on the
//! level of the highest six-bit digit in which its deadline differs from the
//! current time. When the wheel reaches a slot above level 0, the timers in
//! it move down to the levels their deadlines now call for, until they reach
//! level 0 and fire.
//!
//! Inserting and removing a timer take constant time, and finding the next
//! deadline takes one bit scan per level.

use std::mem;
use std::task::Waker;

const LEVELS: usize = 6;
const SLOT_BITS: u32 = 6;
const SLOTS: usize = 1 << SLOT_BITS;

/// How far ahead of the current time a timer can be. The top level wraps
/// around, like a ring, to reach this far.
const MAX_DISTANCE: u64 = 1 << (SLOT_BITS * LEVELS as u32);

/// Marks the end of a list.
const NONE: u32 = u32::MAX;

/// Names a timer in a [`Wheel`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Key {
    index: u32,
    generation: u32,
}

/// Timers with wakers, ordered by deadline.
pub(crate) struct Wheel {
    /// The current tick. Every queued timer is due after it.
    elapsed: u64,
    levels: [Level; LEVELS],
    entries: Vec<Entry>,
    /// Head of the list of free entries.
    free: u32,
    /// The timers of a slot being processed, kept to reuse the allocation.
    scratch: Vec<u32>,
}

struct Level {
    /// Bit `n` is set when slot `n` holds a timer.
    occupied: u64,
    /// The first timer of each slot's list.
    heads: [u32; SLOTS],
}

struct Entry {
    generation: u32,
    deadline: u64,
    waker: Option<Waker>,
    state: State,
}

enum State {
    Free {
        next: u32,
    },
    /// In the list of a slot, linked by entry index.
    Queued {
        level: u8,
        slot: u8,
        prev: u32,
        next: u32,
    },
    /// Due, and its waker taken to be woken.
    Fired,
}

impl Wheel {
    pub(crate) fn new() -> Self {
        Self {
            elapsed: 0,
            levels: [(); LEVELS].map(|()| Level {
                occupied: 0,
                heads: [NONE; SLOTS],
            }),
            entries: Vec::new(),
            free: NONE,
            scratch: Vec::new(),
        }
    }

    /// The current tick.
    pub(crate) fn elapsed(&self) -> u64 {
        self.elapsed
    }

    /// Adds a timer due at `deadline`, which should be after the current
    /// tick. A deadline beyond the wheel's reach is brought in to its edge,
    /// so the timer fires early there and its owner has to check the time.
    pub(crate) fn insert(&mut self, deadline: u64, waker: Waker) -> Key {
        debug_assert!(
            deadline > self.elapsed,
            "{deadline} is not after {}",
            self.elapsed
        );
        let deadline = deadline.clamp(self.elapsed + 1, self.elapsed + MAX_DISTANCE - 1);
        let index = self.allocate();
        let entry = &mut self.entries[index as usize];
        entry.deadline = deadline;
        entry.waker = Some(waker);
        let key = Key {
            index,
            generation: entry.generation,
        };
        self.link(index);
        key
    }

    /// Whether the timer has fired, or is no longer in the wheel.
    pub(crate) fn fired(&self, key: Key) -> bool {
        self.entry(key)
            .is_none_or(|entry| matches!(entry.state, State::Fired))
    }

    /// Makes a timer that has not fired wake `waker`. Returns the waker it
    /// replaces, for the caller to drop after releasing the wheel: dropping a
    /// waker can drop a task, and with it timers of this wheel.
    pub(crate) fn set_waker(&mut self, key: Key, waker: &Waker) -> Option<Waker> {
        let entry = self.entry_mut(key)?;
        if !matches!(entry.state, State::Queued { .. }) {
            return None;
        }
        if entry
            .waker
            .as_ref()
            .is_some_and(|current| current.will_wake(waker))
        {
            return None;
        }
        entry.waker.replace(waker.clone())
    }

    /// Removes a timer, fired or not. Returns its waker, if it still had one,
    /// for the caller to drop after releasing the wheel.
    pub(crate) fn remove(&mut self, key: Key) -> Option<Waker> {
        self.entry(key)?;
        self.unlink(key.index);
        let entry = &mut self.entries[key.index as usize];
        entry.generation = entry.generation.wrapping_add(1);
        entry.state = State::Free { next: self.free };
        self.free = key.index;
        entry.waker.take()
    }

    /// The next tick at which the wheel has work: timers to fire, or timers
    /// to move down a level.
    pub(crate) fn next_expiration(&self) -> Option<u64> {
        self.next_slot().map(|(_, _, tick)| tick)
    }

    /// Moves time forward to `now`, collecting the wakers of the timers that
    /// are due by then.
    pub(crate) fn advance(&mut self, now: u64, wakers: &mut Vec<Waker>) {
        while let Some((level, slot, tick)) = self.next_slot() {
            if tick > now {
                break;
            }
            // Take the whole slot before relinking any of it: a timer brought
            // in from the top level can land back in the same slot.
            let mut taken = mem::take(&mut self.scratch);
            let mut index = mem::replace(&mut self.levels[level].heads[slot], NONE);
            self.levels[level].occupied &= !(1 << slot);
            while index != NONE {
                taken.push(index);
                let State::Queued { next, .. } = self.entries[index as usize].state else {
                    unreachable!("a slot lists a timer that is not queued");
                };
                index = next;
            }
            self.elapsed = tick;
            for &index in &taken {
                let entry = &mut self.entries[index as usize];
                if entry.deadline <= tick {
                    entry.state = State::Fired;
                    wakers.extend(entry.waker.take());
                } else {
                    self.link(index);
                }
            }
            taken.clear();
            self.scratch = taken;
        }
        self.elapsed = self.elapsed.max(now);
    }

    fn entry(&self, key: Key) -> Option<&Entry> {
        let entry = self.entries.get(key.index as usize)?;
        let live = entry.generation == key.generation && !matches!(entry.state, State::Free { .. });
        live.then_some(entry)
    }

    fn entry_mut(&mut self, key: Key) -> Option<&mut Entry> {
        let entry = self.entries.get_mut(key.index as usize)?;
        let live = entry.generation == key.generation && !matches!(entry.state, State::Free { .. });
        live.then_some(entry)
    }

    fn allocate(&mut self) -> u32 {
        if self.free == NONE {
            let index = u32::try_from(self.entries.len())
                .ok()
                .filter(|&index| index != NONE)
                .expect("too many timers");
            self.entries.push(Entry {
                generation: 0,
                deadline: 0,
                waker: None,
                state: State::Fired,
            });
            index
        } else {
            let index = self.free;
            let State::Free { next } = self.entries[index as usize].state else {
                unreachable!("the free list holds a timer in use");
            };
            self.free = next;
            index
        }
    }

    /// Puts a timer at the head of the slot its deadline calls for.
    fn link(&mut self, index: u32) {
        let deadline = self.entries[index as usize].deadline;
        let level = level_for(self.elapsed, deadline);
        let slot = slot_for(deadline, level);
        let head = self.levels[level].heads[slot];
        if let Some(State::Queued { prev, .. }) = self
            .entries
            .get_mut(head as usize)
            .map(|entry| &mut entry.state)
        {
            *prev = index;
        }
        self.entries[index as usize].state = State::Queued {
            level: level as u8,
            slot: slot as u8,
            prev: NONE,
            next: head,
        };
        self.levels[level].heads[slot] = index;
        self.levels[level].occupied |= 1 << slot;
    }

    /// Takes a timer out of its slot's list, if it is in one.
    fn unlink(&mut self, index: u32) {
        let State::Queued {
            level,
            slot,
            prev,
            next,
        } = self.entries[index as usize].state
        else {
            return;
        };
        let (level, slot) = (level as usize, slot as usize);
        match self
            .entries
            .get_mut(prev as usize)
            .map(|entry| &mut entry.state)
        {
            Some(State::Queued { next: link, .. }) => *link = next,
            _ => self.levels[level].heads[slot] = next,
        }
        if let Some(State::Queued { prev: link, .. }) = self
            .entries
            .get_mut(next as usize)
            .map(|entry| &mut entry.state)
        {
            *link = prev;
        }
        if self.levels[level].heads[slot] == NONE {
            self.levels[level].occupied &= !(1 << slot);
        }
        self.entries[index as usize].state = State::Fired;
    }

    /// The first slot with timers, on the lowest level that has any, and the
    /// tick it comes due. Timers on a lower level are always due before any
    /// on a higher one.
    fn next_slot(&self) -> Option<(usize, usize, u64)> {
        (0..LEVELS).find_map(|level| {
            let (slot, tick) = self.levels[level].next_slot(self.elapsed, level)?;
            Some((level, slot, tick))
        })
    }
}

impl Level {
    /// The first occupied slot at or after the one `elapsed` falls in, and
    /// the tick that slot begins.
    fn next_slot(&self, elapsed: u64, level: usize) -> Option<(usize, u64)> {
        if self.occupied == 0 {
            return None;
        }
        let shift = level as u32 * SLOT_BITS;
        let current = ((elapsed >> shift) % SLOTS as u64) as u32;
        let ahead = self.occupied.rotate_right(current).trailing_zeros();
        let slot = ((current + ahead) % SLOTS as u32) as usize;

        let slot_span = 1u64 << shift;
        let level_span = slot_span << SLOT_BITS;
        let level_start = elapsed & !(level_span - 1);
        let mut tick = level_start + slot as u64 * slot_span;
        if tick <= elapsed {
            // Only the top level wraps: the slot is in its next rotation.
            debug_assert_eq!(level, LEVELS - 1);
            tick += level_span;
        }
        Some((slot, tick))
    }
}

/// The level for a timer due at `deadline`: that of the highest six-bit digit
/// in which it differs from `elapsed`. Setting the low digit puts timers due
/// in the current window on level 0.
fn level_for(elapsed: u64, deadline: u64) -> usize {
    let masked = ((elapsed ^ deadline) | (SLOTS as u64 - 1)).min(MAX_DISTANCE - 1);
    let significant = 63 - masked.leading_zeros();
    (significant / SLOT_BITS) as usize
}

fn slot_for(deadline: u64, level: usize) -> usize {
    ((deadline >> (level as u32 * SLOT_BITS)) % SLOTS as u64) as usize
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::Wake;

    use super::*;

    /// A waker that records being woken.
    #[derive(Default)]
    struct Flag(AtomicBool);

    impl Wake for Flag {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn flag() -> (Arc<Flag>, Waker) {
        let flag = Arc::new(Flag::default());
        (flag.clone(), Waker::from(flag))
    }

    fn advance(wheel: &mut Wheel, now: u64) -> usize {
        let mut wakers = Vec::new();
        wheel.advance(now, &mut wakers);
        let fired = wakers.len();
        wakers.into_iter().for_each(Waker::wake);
        fired
    }

    #[test]
    fn timers_fire_at_their_deadline_on_every_level() {
        let mut wheel = Wheel::new();
        for deadline in [1, 63, 64, 65, 4095, 4096, 300_000, 1 << 30, (1 << 36) - 2] {
            let (fired, waker) = flag();
            let key = wheel.insert(wheel.elapsed() + deadline, waker);
            let deadline = wheel.elapsed() + deadline;
            // Step to each tick the wheel reports, never past the deadline.
            while let Some(next) = wheel.next_expiration() {
                assert!(next <= deadline, "{next} is after {deadline}");
                advance(&mut wheel, next);
                if wheel.fired(key) {
                    break;
                }
                assert!(!fired.0.load(Ordering::SeqCst));
            }
            assert!(
                fired.0.load(Ordering::SeqCst),
                "timer at {deadline} never fired"
            );
            assert_eq!(wheel.elapsed(), deadline);
            assert!(wheel.remove(key).is_none());
        }
    }

    #[test]
    fn removed_timers_never_fire() {
        let mut wheel = Wheel::new();
        let (fired, waker) = flag();
        let key = wheel.insert(100, waker);
        assert!(wheel.remove(key).is_some());
        assert_eq!(wheel.next_expiration(), None);
        assert_eq!(advance(&mut wheel, 1000), 0);
        assert!(!fired.0.load(Ordering::SeqCst));
        assert!(wheel.fired(key), "a removed key is no longer in the wheel");
    }

    #[test]
    fn stale_keys_do_not_touch_the_entry_that_reused_the_slot() {
        let mut wheel = Wheel::new();
        let (_, waker) = flag();
        let first = wheel.insert(10, waker.clone());
        wheel.remove(first);
        let second = wheel.insert(20, waker.clone());
        assert_eq!(first.index, second.index);
        assert!(wheel.remove(first).is_none());
        assert!(wheel.set_waker(first, &waker).is_none());
        assert!(!wheel.fired(second));
    }

    #[test]
    fn deadlines_beyond_reach_fire_at_the_edge() {
        let mut wheel = Wheel::new();
        let (_, waker) = flag();
        let key = wheel.insert(u64::MAX, waker);
        advance(&mut wheel, MAX_DISTANCE - 2);
        assert!(!wheel.fired(key));
        advance(&mut wheel, MAX_DISTANCE - 1);
        assert!(wheel.fired(key));
    }

    /// A small deterministic pseudo-random number generator.
    struct Lcg(u64);

    impl Lcg {
        fn below(&mut self, bound: u64) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (self.0 >> 33) % bound
        }
    }

    /// Random inserts, removals and advances, checked against a list of
    /// timers: each fires when time reaches its deadline, and not before.
    #[test]
    fn matches_a_simple_model() {
        let mut rng = Lcg(0x9e37_79b9_7f4a_7c15);
        let mut wheel = Wheel::new();
        let mut live: Vec<(Key, u64, Arc<Flag>)> = Vec::new();
        for _ in 0..20_000 {
            match rng.below(10) {
                0..=4 => {
                    // Distances across every level, and past the wheel's reach.
                    let digits = rng.below(7) as u32;
                    let distance = 1 + rng.below(1 << (6 * digits).min(40));
                    let deadline = wheel.elapsed() + distance;
                    let (fired, waker) = flag();
                    let key = wheel.insert(deadline, waker);
                    let due = deadline.min(wheel.elapsed() + MAX_DISTANCE - 1);
                    live.push((key, due, fired));
                }
                5 | 6 if !live.is_empty() => {
                    let (key, due, fired) = live.swap_remove(rng.below(live.len() as u64) as usize);
                    let was_fired = wheel.fired(key);
                    assert_eq!(was_fired, due <= wheel.elapsed());
                    assert_eq!(wheel.remove(key).is_none(), was_fired);
                    assert_eq!(fired.0.load(Ordering::SeqCst), was_fired);
                }
                _ => {
                    let now = match rng.below(3) {
                        0 => wheel.elapsed() + rng.below(100),
                        1 => wheel.next_expiration().unwrap_or(wheel.elapsed()),
                        _ => wheel.elapsed() + rng.below(1 << 30),
                    };
                    if let Some(next) = wheel.next_expiration() {
                        assert!(next > wheel.elapsed());
                        let earliest = live
                            .iter()
                            .filter(|(key, _, _)| !wheel.fired(*key))
                            .map(|&(_, due, _)| due)
                            .min();
                        assert!(earliest.is_some_and(|earliest| next <= earliest));
                    }
                    advance(&mut wheel, now);
                    for (key, due, fired) in &live {
                        let expected = *due <= now;
                        assert_eq!(wheel.fired(*key), expected, "timer due at {due}, now {now}");
                        assert_eq!(fired.0.load(Ordering::SeqCst), expected);
                    }
                }
            }
        }
    }
}
