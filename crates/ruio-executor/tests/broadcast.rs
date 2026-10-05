//! Broadcast channels: every receiver gets every value, those that fall
//! behind are told so, on one runtime and across threads.

use std::cell::Cell;
use std::future::poll_fn;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use std::thread;
use std::time::Duration;

use ruio_executor::sync::broadcast::{self, RecvError, SendError, TryRecvError};
use ruio_executor::sync::{mpsc, oneshot};
use ruio_executor::time::{sleep, timeout};
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

/// How many times the values of a channel were cloned and dropped: once
/// everything is dropped, the drops are the originals and the clones
/// together.
#[derive(Default)]
struct Counts {
    clones: AtomicUsize,
    drops: AtomicUsize,
}

impl Counts {
    fn clones(&self) -> usize {
        self.clones.load(Ordering::Relaxed)
    }

    fn drops(&self) -> usize {
        self.drops.load(Ordering::Relaxed)
    }
}

/// A value that counts its clones and its drops.
struct Counted(Arc<Counts>);

impl Clone for Counted {
    fn clone(&self) -> Self {
        self.0.clones.fetch_add(1, Ordering::Relaxed);
        Counted(Arc::clone(&self.0))
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.drops.fetch_add(1, Ordering::Relaxed);
    }
}

/// Every value, in order, until the channel ends; a receiver that falls
/// behind fails.
async fn drain<T: Clone>(mut rx: broadcast::Receiver<T>) -> Vec<T> {
    let mut values = Vec::new();
    loop {
        match rx.recv().await {
            Ok(value) => values.push(value),
            Err(RecvError::Closed) => return values,
            Err(RecvError::Lagged(missed)) => panic!("missed {missed} values"),
        }
    }
}

#[test]
fn every_receiver_gets_every_value_in_order() {
    Runtime::new().unwrap().block_on(async {
        let (tx, rx) = broadcast::channel(128);
        let receivers: Vec<_> = (0..3)
            .map(|index| {
                let rx = if index == 0 {
                    rx.resubscribe()
                } else {
                    tx.subscribe()
                };
                spawn(drain(rx))
            })
            .collect();
        let main = spawn(drain(rx));
        for value in 0..100 {
            assert_eq!(tx.send(value), Ok(4));
            if value % 7 == 0 {
                yield_now().await;
            }
        }
        drop(tx);
        for receiver in receivers {
            assert_eq!(receiver.await.unwrap(), (0..100).collect::<Vec<_>>());
        }
        assert_eq!(main.await.unwrap(), (0..100).collect::<Vec<_>>());
    });
}

#[test]
fn a_subscriber_sees_only_what_is_sent_after_it() {
    Runtime::new().unwrap().block_on(async {
        let (tx, mut first) = broadcast::channel(8);
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        let mut second = tx.subscribe();
        assert!(second.is_empty());
        tx.send(3).unwrap();
        assert_eq!(first.recv().await, Ok(1));
        assert_eq!(first.recv().await, Ok(2));
        assert_eq!(first.recv().await, Ok(3));
        assert_eq!(second.recv().await, Ok(3));
        assert_eq!(second.try_recv(), Err(TryRecvError::Empty));
        assert_eq!(first.try_recv(), Err(TryRecvError::Empty));
    });
}

/// A receiver more than the capacity behind is told how many values it
/// missed, once, and then reads the oldest kept and everything after.
#[test]
fn a_receiver_that_falls_behind_is_told_how_much_and_carries_on() {
    Runtime::new().unwrap().block_on(async {
        let (tx, mut rx) = broadcast::channel(4);
        for value in 0..10 {
            tx.send(value).unwrap();
        }
        assert_eq!(rx.len(), 10, "counts what it missed until told");
        assert_eq!(rx.recv().await, Err(RecvError::Lagged(6)));
        assert_eq!(rx.len(), 4);
        for value in 6..10 {
            assert_eq!(rx.recv().await, Ok(value));
        }
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

        // Exactly the capacity behind is not behind at all.
        for value in 10..14 {
            tx.send(value).unwrap();
        }
        assert_eq!(rx.recv().await, Ok(10));
        tx.send(14).unwrap();
        tx.send(15).unwrap();
        assert_eq!(rx.recv().await, Err(RecvError::Lagged(1)));
        assert_eq!(rx.recv().await, Ok(12));

        // `try_recv` tells it too, and the capacity rounds up to a power
        // of two: a channel of three keeps four.
        let (tx, mut rx) = broadcast::channel(3);
        for value in 0..7 {
            tx.send(value).unwrap();
        }
        assert_eq!(rx.try_recv(), Err(TryRecvError::Lagged(3)));
        assert_eq!(rx.try_recv(), Ok(3));
        assert_eq!(rx.try_recv(), Ok(4));
        assert_eq!(rx.try_recv(), Ok(5));
        assert_eq!(rx.try_recv(), Ok(6));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    });
}

#[test]
fn a_send_with_no_receiver_hands_the_value_back() {
    let (tx, rx) = broadcast::channel(2);
    drop(rx);
    assert_eq!(tx.receiver_count(), 0);
    assert_eq!(tx.send(5), Err(SendError(5)));
    assert!(tx.is_empty(), "nothing was kept");
    let mut rx = tx.subscribe();
    assert_eq!(tx.send(6), Ok(1));
    assert_eq!(rx.try_recv(), Ok(6));
}

/// The channel ends for a receiver once every sender is gone and it has
/// read everything they sent, whether it waits for the end or comes later.
#[test]
fn the_channel_ends_once_every_sender_is_gone_and_is_drained() {
    Runtime::new().unwrap().block_on(async {
        let (tx, mut rx) = broadcast::channel(4);
        let other = tx.clone();
        tx.send(1).unwrap();
        drop(tx);
        assert!(!rx.is_closed());
        assert_eq!(rx.recv().await, Ok(1));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        other.send(2).unwrap();
        drop(other);
        assert!(rx.is_closed(), "every sender is gone");
        assert_eq!(rx.recv().await, Ok(2), "what was sent is still there");
        assert_eq!(rx.recv().await, Err(RecvError::Closed));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));

        // A receiver already waiting learns that the last sender went.
        let (tx, mut rx) = broadcast::channel::<u32>(4);
        let waiting = spawn(async move { rx.recv().await });
        yield_now().await;
        drop(tx);
        assert_eq!(waiting.await.unwrap(), Err(RecvError::Closed));

        // One that fell behind is told so before the end.
        let (tx, mut rx) = broadcast::channel(2);
        for value in 0..5 {
            tx.send(value).unwrap();
        }
        drop(tx);
        assert_eq!(rx.try_recv(), Err(TryRecvError::Lagged(3)));
        assert_eq!(rx.recv().await, Ok(3));
        assert_eq!(rx.recv().await, Ok(4));
        assert_eq!(rx.recv().await, Err(RecvError::Closed));
    });
}

#[test]
fn resubscribing_starts_at_the_newest_value() {
    Runtime::new().unwrap().block_on(async {
        let (tx, mut first) = broadcast::channel(8);
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        let mut second = first.resubscribe();
        assert!(first.same_channel(&second));
        assert_eq!(tx.receiver_count(), 2);
        assert!(second.is_empty());
        tx.send(3).unwrap();
        assert_eq!(second.recv().await, Ok(3));
        assert_eq!(second.try_recv(), Err(TryRecvError::Empty));
        assert_eq!(first.recv().await, Ok(1));
        assert_eq!(first.recv().await, Ok(2));
        assert_eq!(first.recv().await, Ok(3));
    });
}

/// Receivers on several threads, each on its own runtime, each told of
/// every value by a wake from the sender's thread, which waits for all of
/// them to answer before it sends the next: a wake lost on the way stops
/// the exchange.
#[test]
fn receivers_on_many_runtimes_are_woken_for_every_value() {
    const THREADS: usize = 4;
    const VALUES: u32 = 5_000;
    let (tx, rx) = broadcast::channel(2);
    let (answer, mut answers) = mpsc::channel(THREADS);
    let threads: Vec<_> = (0..THREADS)
        .map(|_| {
            let (mut rx, answer) = (tx.subscribe(), answer.clone());
            thread::spawn(move || {
                run(async move {
                    for expected in 0..VALUES {
                        assert_eq!(rx.recv().await, Ok(expected));
                        answer.send(()).await.unwrap();
                    }
                    assert_eq!(rx.recv().await, Err(RecvError::Closed));
                });
            })
        })
        .collect();
    drop((rx, answer));
    run(async move {
        for value in 0..VALUES {
            assert_eq!(tx.send(value), Ok(THREADS));
            for _ in 0..THREADS {
                answers.recv().await.unwrap();
            }
        }
        drop(tx);
        assert_eq!(answers.recv().await, None);
    });
    for thread in threads {
        thread.join().unwrap();
    }
}

/// Senders on several threads, each on its own runtime, racing for the
/// tail: every receiver, on a thread of its own, gets every value, in
/// order from each sender, and is woken for those it waits for.
#[test]
fn senders_on_many_runtimes_reach_every_receiver_in_order() {
    const SENDERS: usize = 3;
    const RECEIVERS: usize = 3;
    const VALUES: usize = 2_000;
    let (tx, rx) = broadcast::channel(SENDERS * VALUES);
    let receivers: Vec<_> = (0..RECEIVERS)
        .map(|_| {
            let mut rx = tx.subscribe();
            thread::spawn(move || {
                run(async move {
                    let mut next = [0; SENDERS];
                    let mut count = 0;
                    loop {
                        match rx.recv().await {
                            Ok((sender, value)) => {
                                assert_eq!(next[sender], value, "in order from each sender");
                                next[sender] += 1;
                                count += 1;
                                if count % 997 == 0 {
                                    sleep(Duration::from_millis(1)).await;
                                }
                            }
                            Err(RecvError::Closed) => break,
                            Err(RecvError::Lagged(missed)) => panic!("missed {missed}"),
                        }
                    }
                    assert_eq!(count, SENDERS * VALUES);
                });
            })
        })
        .collect();
    drop(rx);
    let senders: Vec<_> = (0..SENDERS)
        .map(|sender| {
            let tx = tx.clone();
            thread::spawn(move || {
                run(async move {
                    for value in 0..VALUES {
                        assert_eq!(tx.send((sender, value)), Ok(RECEIVERS));
                        if value % 100 == 0 {
                            yield_now().await;
                        }
                    }
                });
            })
        })
        .collect();
    drop(tx);
    for thread in senders.into_iter().chain(receivers) {
        thread.join().unwrap();
    }
}

/// Every receiver but the last to read a value clones it, and the last
/// takes it; a value replaced before a receiver that fell behind read it
/// is dropped by the send that replaces it. By the end, every original and
/// every clone has been dropped exactly once.
#[test]
fn every_clone_of_every_value_is_dropped_once() {
    const VALUES: usize = 20;
    const CAPACITY: usize = 4;
    let counts = Arc::new(Counts::default());
    Runtime::new().unwrap().block_on(async {
        // One receiver alone takes every value: nothing is cloned.
        let (tx, mut rx) = broadcast::channel(CAPACITY);
        for _ in 0..VALUES {
            tx.send(Counted(Arc::clone(&counts))).unwrap();
            drop(rx.recv().await.unwrap());
        }
        assert_eq!(counts.clones(), 0);
        assert_eq!(counts.drops(), VALUES);
        drop((tx, rx));
        assert_eq!(counts.drops(), VALUES);

        // Two receivers keep up and a third starts late, having missed all
        // but the last `CAPACITY`: the two clone every value, and the third
        // takes those it reads.
        let counts = Arc::new(Counts::default());
        let (tx, rx) = broadcast::channel(CAPACITY);
        let quick: Vec<_> = (0..2).map(|_| spawn(drain(tx.subscribe()))).collect();
        let (go, wait) = oneshot::channel();
        let late = spawn(async move {
            wait.await.unwrap();
            let mut rx = rx;
            assert_eq!(
                rx.recv().await.map(drop),
                Err(RecvError::Lagged((VALUES - CAPACITY) as u64))
            );
            drain(rx).await
        });
        for _ in 0..VALUES {
            tx.send(Counted(Arc::clone(&counts))).unwrap();
            yield_now().await;
        }
        drop(tx);
        go.send(()).unwrap();
        let quick: Vec<_> = quick
            .into_iter()
            .map(|task| async { task.await.unwrap() })
            .collect();
        let mut received = Vec::new();
        for task in quick {
            received.push(task.await);
        }
        received.push(late.await.unwrap());
        assert_eq!(
            counts.clones(),
            2 * VALUES,
            "two receivers clone each value"
        );
        assert_eq!(
            received.iter().map(Vec::len).sum::<usize>(),
            2 * VALUES + CAPACITY
        );
        assert_eq!(
            counts.drops(),
            VALUES - CAPACITY,
            "only the values replaced unread have gone"
        );
        drop(received);
        assert_eq!(counts.drops(), 3 * VALUES);
    });
}

/// Receivers on other threads that read as fast as they can, falling behind
/// now and then, and senders on others: whatever is cloned and whatever is
/// replaced unread, every original and every clone is dropped exactly once.
#[test]
fn every_clone_is_dropped_once_however_the_receivers_fall_behind() {
    const SENDERS: usize = 2;
    const RECEIVERS: usize = 3;
    const VALUES: usize = 5_000;
    let counts = Arc::new(Counts::default());
    let (tx, rx) = broadcast::channel(8);
    let receivers: Vec<_> = (0..RECEIVERS)
        .map(|index| {
            let mut rx = tx.subscribe();
            thread::spawn(move || {
                run(async move {
                    let (mut received, mut missed) = (0, 0);
                    loop {
                        match rx.recv().await {
                            Ok(value) => {
                                received += 1;
                                if received % (index + 1) * 100 == 0 {
                                    sleep(Duration::from_millis(1)).await;
                                }
                                drop(value);
                            }
                            Err(RecvError::Lagged(n)) => missed += n as usize,
                            Err(RecvError::Closed) => break,
                        }
                    }
                    assert_eq!(received + missed, SENDERS * VALUES);
                });
            })
        })
        .collect();
    drop(rx);
    let senders: Vec<_> = (0..SENDERS)
        .map(|_| {
            let (tx, counts) = (tx.clone(), Arc::clone(&counts));
            thread::spawn(move || {
                for _ in 0..VALUES {
                    tx.send(Counted(Arc::clone(&counts))).unwrap();
                }
            })
        })
        .collect();
    drop(tx);
    for thread in senders.into_iter().chain(receivers) {
        thread.join().unwrap();
    }
    assert_eq!(counts.drops(), SENDERS * VALUES + counts.clones());
}

/// A receiver dropped while it waits neither holds the others back nor
/// counts against the values they have yet to read.
#[test]
fn a_waiting_receiver_dropped_lets_the_others_proceed() {
    Runtime::new().unwrap().block_on(async {
        let (tx, mut rx) = broadcast::channel(4);
        let quitter = {
            let mut rx = tx.subscribe();
            spawn(async move {
                // Waits once, then goes.
                let pending = poll_fn(|cx| Poll::Ready(rx.poll_recv(cx).is_pending())).await;
                assert!(pending, "nothing sent yet");
                drop(rx);
            })
        };
        let stayer = {
            let mut rx = tx.subscribe();
            spawn(async move { rx.recv().await })
        };
        yield_now().await;
        quitter.await.unwrap();
        assert_eq!(tx.receiver_count(), 2);
        assert_eq!(tx.send(1), Ok(2));
        assert_eq!(stayer.await.unwrap(), Ok(1));
        assert_eq!(tx.len(), 1, "one receiver has yet to read it");
        assert_eq!(rx.recv().await, Ok(1));
        assert!(tx.is_empty());

        // Dropped with values unread, it counts itself out of them.
        let other = tx.subscribe();
        tx.send(2).unwrap();
        tx.send(3).unwrap();
        assert_eq!(tx.len(), 2);
        assert_eq!(rx.recv().await, Ok(2));
        drop(other);
        assert_eq!(tx.len(), 1);
        assert_eq!(rx.recv().await, Ok(3));
        assert!(tx.is_empty());
    });
}

/// A receiver that waits in one task and then in another, as a sender on
/// another thread sends: the task waiting last gets the value.
#[test]
fn a_receiver_moved_between_tasks_gets_the_value_in_the_last() {
    for _ in 0..200 {
        let (tx, mut rx) = broadcast::channel(1);
        let (go, wait) = std::sync::mpsc::channel();
        let sender = thread::spawn(move || {
            wait.recv().unwrap();
            tx.send(7).unwrap();
        });
        run(async move {
            let mut rx = spawn(async move {
                // Waits once here, then leaves for another task.
                let pending = poll_fn(|cx| Poll::Ready(rx.poll_recv(cx).is_pending())).await;
                assert!(pending, "nothing sent yet");
                rx
            })
            .await
            .unwrap();
            go.send(()).unwrap();
            let value = spawn(async move { rx.recv().await }).await.unwrap();
            assert_eq!(value, Ok(7));
        });
        sender.join().unwrap();
    }
}

/// What the halves say about the channel: how much is kept and left, how
/// many receivers there are, whether the senders are gone, and which
/// channel a receiver is of.
#[test]
fn the_halves_report_the_state_of_the_channel() {
    Runtime::new().unwrap().block_on(async {
        let (tx, mut first) = broadcast::channel(16);
        let mut second = tx.subscribe();
        let (_other_tx, other_rx) = broadcast::channel::<i32>(1);
        assert!(first.same_channel(&second));
        assert!(!first.same_channel(&other_rx));
        assert_eq!(tx.receiver_count(), 2);
        assert!(tx.is_empty());
        assert_eq!(tx.len(), 0);
        assert!(first.is_empty());
        assert_eq!(first.len(), 0);
        assert!(!first.is_closed());

        tx.send(10).unwrap();
        tx.send(20).unwrap();
        tx.send(30).unwrap();
        assert_eq!(tx.len(), 3);
        assert!(!tx.is_empty());
        assert_eq!(first.len(), 3);
        assert_eq!(second.len(), 3);
        assert_eq!(first.recv().await, Ok(10));
        assert_eq!(first.len(), 2);
        assert_eq!(tx.len(), 3, "the second has yet to read the first value");
        assert_eq!(second.recv().await, Ok(10));
        assert_eq!(tx.len(), 2);
        assert_eq!(second.recv().await, Ok(20));
        assert_eq!(second.recv().await, Ok(30));
        assert!(second.is_empty());
        assert_eq!(tx.len(), 2, "the first has yet to read two");
        assert_eq!(first.recv().await, Ok(20));
        assert_eq!(first.recv().await, Ok(30));
        assert!(tx.is_empty());
        assert!(first.is_empty());

        // A receiver that fell behind counts what it missed until told.
        for value in 0..20 {
            tx.send(value).unwrap();
        }
        assert_eq!(first.len(), 20);
        assert_eq!(tx.len(), 16, "the ring keeps sixteen");
        assert_eq!(first.recv().await, Err(RecvError::Lagged(4)));
        assert_eq!(first.len(), 16);

        drop(second);
        assert_eq!(tx.receiver_count(), 1);
        drop(tx);
        assert!(first.is_closed());
        assert_eq!(first.len(), 16, "closing loses nothing");
    });
}

/// A value that, as it is dropped, sends into its own channel: a drop under
/// a lock of the channel would deadlock.
struct SendsAsItGoes(Option<broadcast::Sender<SendsAsItGoes>>, Arc<AtomicUsize>);

impl Clone for SendsAsItGoes {
    fn clone(&self) -> Self {
        SendsAsItGoes(None, Arc::clone(&self.1))
    }
}

impl Drop for SendsAsItGoes {
    fn drop(&mut self) {
        let Some(tx) = self.0.take() else { return };
        let _ = tx.send(SendsAsItGoes(None, Arc::clone(&self.1)));
        self.1.fetch_add(1, Ordering::Relaxed);
    }
}

/// The values a channel drops, replaced by a send or left by its last
/// reader, are dropped outside its locks.
#[test]
fn values_are_dropped_outside_the_channels_locks() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = broadcast::channel(1);
    tx.send(SendsAsItGoes(Some(tx.clone()), Arc::clone(&drops)))
        .unwrap();
    // Replaces the first, whose drop sends again, replacing this one.
    tx.send(SendsAsItGoes(Some(tx.clone()), Arc::clone(&drops)))
        .unwrap();
    assert_eq!(drops.load(Ordering::Relaxed), 2);
    drop(rx);

    // A receiver going as the last reader of a value drops it.
    let drops = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = broadcast::channel(2);
    tx.send(SendsAsItGoes(Some(tx.clone()), Arc::clone(&drops)))
        .unwrap();
    drop(rx);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    drop(tx);
}

/// How many steps that are ready at once a task takes before the task
/// queued behind it runs: `steps` counts them as they are taken.
async fn steps_before_the_next_task(steps: Rc<Cell<usize>>) -> usize {
    spawn(async move { steps.get() }).await.unwrap()
}

/// A receiver that always finds a value ready still lets the other tasks
/// run: what is ready spends the task's budget, and once it is spent the
/// task yields.
#[test]
fn what_is_always_ready_does_not_starve_the_other_tasks() {
    const MANY: usize = 10_000;
    Runtime::new().unwrap().block_on(async {
        let (tx, mut rx) = broadcast::channel(MANY);
        for value in 0..MANY {
            tx.send(value).unwrap();
        }
        drop(tx);
        let received = Rc::new(Cell::new(0));
        let receiver = spawn({
            let received = received.clone();
            async move {
                while rx.recv().await.is_ok() {
                    received.set(received.get() + 1);
                }
            }
        });
        let seen = steps_before_the_next_task(received.clone()).await;
        assert!(0 < seen && seen < MANY / 10, "{seen} receives");
        receiver.await.unwrap();
        assert_eq!(received.get(), MANY);
    });
}
