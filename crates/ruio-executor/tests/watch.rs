//! Watch channels: a value replaced by senders and watched by receivers,
//! on one runtime and across threads.

use std::cell::Cell;
use std::future::poll_fn;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use ruio_executor::sync::watch;
use ruio_executor::time::timeout;
use ruio_executor::{Runtime, spawn, yield_now};

/// Long enough for any of these to finish; a lost wake runs it out.
const PATIENCE: Duration = Duration::from_secs(20);

/// Runs `future` on a runtime of its own, failing if it takes longer than
/// [`PATIENCE`], as it does if a wake is lost.
fn run<F: Future>(future: F) -> F::Output {
    Runtime::new()
        .unwrap()
        .block_on(timeout(PATIENCE, future))
        .expect("finished in time, with no wake lost")
}

/// Polls `future` once, inside a task.
async fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    let mut future = Some(future);
    poll_fn(|cx| Poll::Ready(future.take().expect("polled once").poll(cx))).await
}

/// A value that counts its drops.
struct Counted(Arc<AtomicUsize>);

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

/// A waker that counts its wakes.
struct Wakes(AtomicUsize);

impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// The receiver sees the value the channel was made with, as seen, and its
/// wait for a change ends with a send.
#[test]
fn a_receiver_sees_the_initial_value_and_waits_for_a_send() {
    run(async {
        let (tx, mut rx) = watch::channel(1);
        assert_eq!(*rx.borrow(), 1);
        assert_eq!(*tx.borrow(), 1);
        assert_eq!(rx.has_changed(), Ok(false));
        {
            let mut changed = pin!(rx.changed());
            assert!(poll_once(changed.as_mut()).await.is_pending());
            tx.send(2).unwrap();
            changed.await.unwrap();
        }
        assert_eq!(*rx.borrow(), 2);
        assert_eq!(rx.has_changed(), Ok(false));

        // A receiver waiting in another task is woken by the send too.
        let waiting = spawn(async move {
            rx.changed().await.unwrap();
            *rx.borrow()
        });
        yield_now().await;
        tx.send(3).unwrap();
        assert_eq!(waiting.await.unwrap(), 3);
    });
}

/// Values sent before the receiver looks are one change to it, and the
/// latest is the one it sees.
#[test]
fn sends_before_the_receiver_looks_are_one_change() {
    run(async {
        let (tx, mut rx) = watch::channel(0);
        for value in 1..=5 {
            tx.send(value).unwrap();
        }
        assert_eq!(rx.has_changed(), Ok(true));
        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow(), 5);
        assert_eq!(rx.has_changed(), Ok(false));
        assert!(poll_once(pin!(rx.changed())).await.is_pending());
    });
}

/// `borrow` leaves the value unseen and `borrow_and_update` marks it seen,
/// and each borrow says whether the value was unseen as it was borrowed.
#[test]
fn borrow_and_update_marks_the_value_seen() {
    let (tx, mut rx) = watch::channel(0);
    tx.send(1).unwrap();
    assert_eq!(rx.has_changed(), Ok(true));
    {
        let value = rx.borrow();
        assert_eq!(*value, 1);
        assert!(value.has_changed());
    }
    assert_eq!(rx.has_changed(), Ok(true));
    {
        let value = rx.borrow_and_update();
        assert_eq!(*value, 1);
        assert!(value.has_changed());
    }
    assert_eq!(rx.has_changed(), Ok(false));
    assert!(!rx.borrow().has_changed());
    assert!(!rx.borrow_and_update().has_changed());
    tx.send(2).unwrap();
    assert!(rx.borrow().has_changed());
    assert!(!tx.borrow().has_changed());
    assert_eq!(*tx.borrow(), 2);
}

/// With no receiver alive, `send` hands the value back and leaves the
/// channel's value as it is; the other sends go through regardless, and a
/// receiver subscribed afterwards finds the latest value.
#[test]
fn a_send_with_no_receiver_hands_the_value_back() {
    let (tx, rx) = watch::channel(1);
    assert_eq!(tx.receiver_count(), 1);
    assert!(!tx.is_closed());
    drop(rx);
    assert_eq!(tx.receiver_count(), 0);
    assert!(tx.is_closed());
    assert_eq!(tx.send(2), Err(watch::SendError(2)));
    assert_eq!(*tx.borrow(), 1);
    assert_eq!(tx.send_replace(3), 1);
    tx.send_modify(|value| *value += 1);
    assert!(tx.send_if_modified(|value| {
        *value += 1;
        true
    }));
    assert_eq!(*tx.borrow(), 5);
    let rx = tx.subscribe();
    assert!(!tx.is_closed());
    assert_eq!(*rx.borrow(), 5);
    assert_eq!(rx.has_changed(), Ok(false));
    tx.send(6).unwrap();
    assert_eq!(rx.has_changed(), Ok(true));
}

/// `send_modify` changes the value in place and counts as a change;
/// `send_if_modified` counts as one, and wakes the receivers, only if its
/// closure says so; `send_replace` hands the value replaced back. A
/// receiver polled again from the same task is woken once per change.
#[test]
fn send_modify_send_if_modified_and_send_replace() {
    let (tx, mut rx) = watch::channel(vec![1]);
    tx.send_modify(|value| value.push(2));
    assert_eq!(rx.has_changed(), Ok(true));
    assert_eq!(*rx.borrow_and_update(), [1, 2]);

    let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&wakes));
    let mut cx = Context::from_waker(&waker);
    assert!(rx.poll_changed(&mut cx).is_pending());
    // Modified, but not said to be: no change, and nobody woken.
    assert!(!tx.send_if_modified(|value| {
        value.push(3);
        false
    }));
    assert_eq!(wakes.0.load(Ordering::SeqCst), 0);
    assert_eq!(rx.has_changed(), Ok(false));
    assert!(rx.poll_changed(&mut cx).is_pending());
    assert_eq!(*rx.borrow(), [1, 2, 3]);
    assert!(tx.send_if_modified(|value| {
        value.push(4);
        true
    }));
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    assert_eq!(rx.poll_changed(&mut cx), Poll::Ready(Ok(())));
    assert_eq!(*rx.borrow(), [1, 2, 3, 4]);

    assert!(rx.poll_changed(&mut cx).is_pending());
    assert!(rx.poll_changed(&mut cx).is_pending());
    assert_eq!(tx.send_replace(vec![5]), [1, 2, 3, 4]);
    assert_eq!(wakes.0.load(Ordering::SeqCst), 2);
    assert_eq!(*rx.borrow_and_update(), [5]);
}

/// `wait_for` looks at the value as it is first, then at each value sent,
/// and ends with the channel if no value satisfies it.
#[test]
fn wait_for_finds_a_value_now_later_or_never() {
    run(async {
        let (tx, mut rx) = watch::channel(0);
        {
            let value = rx.wait_for(|current| *current == 0).await.unwrap();
            assert_eq!(*value, 0);
            assert!(!value.has_changed());
        }
        tx.send(1).unwrap();
        {
            // Unseen as it is borrowed, and seen after.
            let value = rx.wait_for(|current| *current == 1).await.unwrap();
            assert_eq!(*value, 1);
            assert!(value.has_changed());
        }
        assert_eq!(rx.has_changed(), Ok(false));

        let sender = spawn(async move {
            for value in 2..=5 {
                tx.send(value).unwrap();
                yield_now().await;
            }
            tx
        });
        {
            let value = rx.wait_for(|current| *current >= 4).await.unwrap();
            assert!(*value >= 4);
            assert!(value.has_changed());
        }
        let tx = sender.await.unwrap();
        assert_eq!(*rx.borrow(), 5);

        // Satisfied by the last value, sent as the sender goes.
        let waiting = spawn(async move {
            let found = rx
                .wait_for(|current| *current == 6)
                .await
                .map(|found| *found);
            (rx, found)
        });
        yield_now().await;
        tx.send(6).unwrap();
        drop(tx);
        let (mut rx, found) = waiting.await.unwrap();
        assert_eq!(found, Ok(6));
        assert!(rx.wait_for(|current| *current == 7).await.is_err());

        // Never satisfied: the senders go while it waits.
        let (tx, mut rx) = watch::channel(0);
        let waiting = spawn(async move {
            rx.wait_for(|current| *current > 0)
                .await
                .map(|found| *found)
        });
        yield_now().await;
        drop(tx);
        assert!(waiting.await.unwrap().is_err());
    });
}

/// `closed` resolves once the last receiver is gone, and waits again for
/// one subscribed after.
#[test]
fn closed_resolves_when_the_last_receiver_goes() {
    run(async {
        let (tx, rx) = watch::channel(0);
        let other = rx.clone();
        assert_eq!(tx.receiver_count(), 2);
        let closed = {
            let tx = tx.clone();
            spawn(async move {
                tx.closed().await;
                tx.is_closed()
            })
        };
        yield_now().await;
        drop(rx);
        yield_now().await;
        assert!(!closed.is_finished());
        drop(other);
        assert!(closed.await.unwrap());
        assert!(tx.is_closed());

        let rx = tx.subscribe();
        assert!(!tx.is_closed());
        assert!(poll_once(pin!(tx.closed())).await.is_pending());
        drop(rx);
        tx.closed().await;
    });
}

/// Once every sender is gone, a wait ends with an error, after the last
/// change has been seen; the last value stays to be borrowed.
#[test]
fn changed_fails_once_every_sender_is_gone_and_nothing_is_unseen() {
    run(async {
        let (tx, mut rx) = watch::channel(0);
        let other = tx.clone();
        tx.send(1).unwrap();
        drop(tx);
        assert_eq!(rx.has_changed(), Ok(true));
        drop(other);
        // The unseen change comes first.
        assert_eq!(rx.has_changed(), Ok(true));
        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow(), 1);
        assert!(rx.has_changed().is_err());
        assert!(rx.changed().await.is_err());
        assert_eq!(*rx.borrow(), 1);
        assert!(rx.wait_for(|_| false).await.is_err());

        // A receiver already waiting learns of the last sender's going.
        let (tx, mut rx) = watch::channel(0);
        let waiting = spawn(async move { rx.changed().await });
        yield_now().await;
        drop(tx);
        assert!(waiting.await.unwrap().is_err());
    });
}

/// `mark_changed` makes the value unseen, so that a wait ends at once, and
/// `mark_unchanged` makes it seen, so that a wait is for the next send.
#[test]
fn mark_changed_and_mark_unchanged_move_what_is_seen() {
    run(async {
        let (tx, mut rx) = watch::channel(0);
        rx.mark_changed();
        assert_eq!(rx.has_changed(), Ok(true));
        assert!(rx.borrow().has_changed());
        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow(), 0);
        assert_eq!(rx.has_changed(), Ok(false));

        tx.send(1).unwrap();
        assert_eq!(rx.has_changed(), Ok(true));
        rx.mark_unchanged();
        assert_eq!(rx.has_changed(), Ok(false));
        assert!(poll_once(pin!(rx.changed())).await.is_pending());
        assert_eq!(*rx.borrow(), 1);

        // Marked unseen and then sent to: one change.
        rx.mark_changed();
        tx.send(2).unwrap();
        rx.changed().await.unwrap();
        assert_eq!(rx.has_changed(), Ok(false));

        // The last value can be marked unseen after the senders are gone.
        drop(tx);
        assert!(rx.has_changed().is_err());
        rx.mark_changed();
        assert_eq!(rx.has_changed(), Ok(true));
        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow(), 2);
        assert!(rx.changed().await.is_err());
    });
}

/// Receivers on several threads, each on its own runtime, are each woken
/// for each of many changes: the sender waits for every receiver to see a
/// value before it sends the next, and a lost wake stops them all.
#[test]
fn receivers_on_many_runtimes_are_woken_for_every_change() {
    const THREADS: usize = 4;
    const CHANGES: usize = 10_000;
    let (tx, rx) = watch::channel(0);
    let (seen_tx, seen_rx) = std::sync::mpsc::channel();
    let threads: Vec<_> = (0..THREADS)
        .map(|_| {
            let (mut rx, seen_tx) = (rx.clone(), seen_tx.clone());
            thread::spawn(move || {
                run(async move {
                    let mut last = 0;
                    while rx.changed().await.is_ok() {
                        let value = *rx.borrow_and_update();
                        assert_eq!(value, last + 1, "every change is seen");
                        last = value;
                        seen_tx.send(value).unwrap();
                    }
                    last
                })
            })
        })
        .collect();
    drop((rx, seen_tx));
    for value in 1..=CHANGES {
        tx.send(value).unwrap();
        for _ in 0..THREADS {
            assert_eq!(seen_rx.recv().unwrap(), value);
        }
    }
    drop(tx);
    for thread in threads {
        assert_eq!(thread.join().unwrap(), CHANGES);
    }
}

/// The last receiver going on another thread wakes a sender waiting in
/// `closed`, and the last sender going there wakes a receiver waiting for
/// a change.
#[test]
fn the_other_half_going_on_another_thread_wakes_a_waiting_task() {
    for _ in 0..50 {
        let (tx, rx) = watch::channel(0);
        let (go, wait) = std::sync::mpsc::channel();
        let receiver = thread::spawn(move || {
            wait.recv().unwrap();
            drop(rx);
        });
        run(async {
            go.send(()).unwrap();
            tx.closed().await;
            assert!(tx.is_closed());
        });
        receiver.join().unwrap();

        let (tx, mut rx) = watch::channel(0);
        let (go, wait) = std::sync::mpsc::channel();
        let sender = thread::spawn(move || {
            wait.recv().unwrap();
            tx.send(1).unwrap();
        });
        run(async {
            go.send(()).unwrap();
            rx.changed().await.unwrap();
            assert_eq!(*rx.borrow(), 1);
            assert!(rx.changed().await.is_err());
        });
        sender.join().unwrap();
    }
}

/// A value that, as it is dropped, borrows its own channel's value: a drop
/// with the value locked for writing would deadlock.
struct LooksBackAsItGoes(Option<watch::Sender<LooksBackAsItGoes>>, Arc<AtomicUsize>);

impl Drop for LooksBackAsItGoes {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            drop(tx.borrow());
        }
        self.1.fetch_add(1, Ordering::Relaxed);
    }
}

/// A value is dropped exactly once: the one `send` replaces, outside every
/// lock; the one `send_replace`, or a `send` with no receiver, hands back;
/// and the one left in the channel as it goes.
#[test]
fn a_value_is_dropped_exactly_once() {
    let drops = Arc::new(AtomicUsize::new(0));
    let counted = || Counted(Arc::clone(&drops));
    let (tx, rx) = watch::channel(counted());
    tx.send(counted()).unwrap();
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    let replaced = tx.send_replace(counted());
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    drop(replaced);
    assert_eq!(drops.load(Ordering::Relaxed), 2);
    tx.send_modify(|_| {});
    assert!(!tx.send_if_modified(|_| false));
    assert_eq!(drops.load(Ordering::Relaxed), 2);
    drop(rx);
    let handed_back = tx.send(counted()).unwrap_err();
    assert_eq!(drops.load(Ordering::Relaxed), 2);
    drop(handed_back);
    assert_eq!(drops.load(Ordering::Relaxed), 3);
    drop(tx);
    assert_eq!(drops.load(Ordering::Relaxed), 4);

    // The value replaced is dropped outside the channel's locks.
    let drops = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = watch::channel(LooksBackAsItGoes(None, Arc::clone(&drops)));
    tx.send(LooksBackAsItGoes(Some(tx.clone()), Arc::clone(&drops)))
        .unwrap();
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    tx.send(LooksBackAsItGoes(None, Arc::clone(&drops)))
        .unwrap();
    assert_eq!(drops.load(Ordering::Relaxed), 2);
    drop((tx, rx));
    assert_eq!(drops.load(Ordering::Relaxed), 3);
}

/// A subscriber starts with the value as it is seen; a clone starts with
/// what the receiver cloned has seen, and what each sees after is its own.
#[test]
fn a_subscriber_starts_seen_and_a_clone_starts_as_its_original() {
    run(async {
        let (tx, mut rx) = watch::channel(0);
        tx.send(1).unwrap();
        let mut subscriber = tx.subscribe();
        let mut clone = rx.clone();
        assert_eq!(tx.receiver_count(), 3);
        assert!(rx.same_channel(&subscriber) && rx.same_channel(&clone));
        assert_eq!(subscriber.has_changed(), Ok(false));
        assert_eq!(*subscriber.borrow(), 1);
        assert_eq!(clone.has_changed(), Ok(true));
        rx.changed().await.unwrap();
        assert_eq!(clone.has_changed(), Ok(true));
        assert_eq!(rx.clone().has_changed(), Ok(false));
        clone.changed().await.unwrap();
        assert!(poll_once(pin!(subscriber.changed())).await.is_pending());
        let (_other_tx, other_rx) = watch::channel(0);
        assert!(!rx.same_channel(&other_rx));
    });
}

/// How many steps that are ready at once a task takes before the task
/// queued behind it runs: `steps` counts them as they are taken.
async fn steps_before_the_next_task(steps: Rc<Cell<usize>>) -> usize {
    spawn(async move { steps.get() }).await.unwrap()
}

/// A task that always finds its wait ready still lets the others run: what
/// is ready spends the task's budget, and once it is spent the task
/// yields.
#[test]
fn what_is_always_changed_does_not_starve_the_other_tasks() {
    const MANY: usize = 10_000;
    Runtime::new().unwrap().block_on(async {
        let (_tx, mut rx) = watch::channel(0);
        let changes = Rc::new(Cell::new(0));
        let changer = spawn({
            let changes = changes.clone();
            async move {
                for _ in 0..MANY {
                    rx.mark_changed();
                    rx.changed().await.unwrap();
                    changes.set(changes.get() + 1);
                }
                rx
            }
        });
        let seen = steps_before_the_next_task(changes.clone()).await;
        assert!(0 < seen && seen < MANY / 10, "{seen} changes");
        let mut rx = changer.await.unwrap();
        assert_eq!(changes.get(), MANY);

        let waits = Rc::new(Cell::new(0));
        let waiter = spawn({
            let waits = waits.clone();
            async move {
                for _ in 0..MANY {
                    rx.wait_for(|_| true).await.unwrap();
                    waits.set(waits.get() + 1);
                }
            }
        });
        let seen = steps_before_the_next_task(waits.clone()).await;
        assert!(0 < seen && seen < MANY / 10, "{seen} waits");
        waiter.await.unwrap();
    });
}

/// Both halves go between threads, and are shared between them, when the
/// value can be.
#[test]
fn both_halves_are_send_and_sync_when_the_value_is() {
    fn send_sync<T: Send + Sync>() {}
    send_sync::<watch::Sender<Vec<u8>>>();
    send_sync::<watch::Receiver<Vec<u8>>>();
}
