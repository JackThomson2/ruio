//! A check that an engine keeps the contract, for the engine's own tests.

use std::thread;
use std::time::{Duration, Instant};

use crate::{Build, Engine, Unpark};

/// How long the check's waits are.
const SHORT: Duration = Duration::from_millis(20);

/// How long the check waits for what should come by `SHORT` before it
/// fails, rather than hang.
const PATIENCE: Duration = Duration::from_secs(10);

/// Builds an engine with `builder` and turns it as the executor does,
/// through the contract alone, failing where the engine breaks the
/// contract in a way a test with no I/O can see: unparks from this thread
/// and another, a park's timeout, and the deadline timer. It parks for a
/// tenth of a second or so in all.
///
/// An engine's tests call it with each configuration worth checking.
///
/// # Panics
///
/// If the engine cannot be built, a turn fails, or the engine breaks the
/// contract.
pub fn check<E: Engine>(builder: &E::Builder) {
    let engine = builder.build().expect("the engine is built");
    let other = builder.build().expect("a second engine is built");
    assert_ne!(engine.id(), other.id(), "engines have IDs of their own");
    drop(other);
    let entered = engine.enter();
    engine
        .submit()
        .expect("a turn with nothing queued succeeds");

    // Unparks before a park make it return at once, and once.
    let unparker = engine.unparker();
    unparker.unpark();
    unparker.unpark();
    assert!(engine.has_work(), "an unpark is work for the next park");
    engine.park(None).unwrap();
    let started = Instant::now();
    engine.park(Some(SHORT)).unwrap();
    let waited = started.elapsed();
    assert!(waited >= SHORT, "unparks do not add up: waited {waited:?}");

    // An unpark from another thread ends a park.
    let remote = engine.unparker();
    let unparking = thread::spawn(move || {
        thread::sleep(SHORT);
        remote.unpark();
    });
    engine.park(None).unwrap();
    unparking.join().unwrap();

    // The timer ends a park by its deadline, and says so once.
    assert!(!engine.timer_fired(), "no timer was armed");
    let deadline = E::monotonic_now() + SHORT;
    engine.wake_at(deadline).unwrap();
    park_until_the_timer_fires(&engine);
    assert!(E::monotonic_now() >= deadline, "the timer fired early");
    assert!(!engine.timer_fired(), "asking clears it");

    // A later deadline does not put off an earlier one.
    let deadline = E::monotonic_now() + SHORT;
    engine.wake_at(deadline).unwrap();
    engine
        .wake_at(deadline + Duration::from_secs(3600))
        .unwrap();
    park_until_the_timer_fires(&engine);
    assert!(E::monotonic_now() >= deadline, "the timer fired early");

    // An unparker outlives its engine, and then does nothing.
    drop(entered);
    drop(engine);
    unparker.unpark();
}

/// Parks until the armed timer fires, failing if it has not by
/// [`PATIENCE`].
fn park_until_the_timer_fires<E: Engine>(engine: &E) {
    let started = Instant::now();
    while !engine.timer_fired() {
        assert!(started.elapsed() < PATIENCE, "the timer never fired");
        engine.park(Some(PATIENCE)).unwrap();
    }
}
