//! Channels under races: halves on several threads, each running its own
//! runtime, closing from either side while values are in flight.

use std::future::poll_fn;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use std::thread;
use std::time::Duration;

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

/// A value that counts its drops.
struct Counted(Arc<AtomicUsize>);

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

/// Senders on several threads, each with its own runtime, waiting for room
/// in a small channel: every value arrives, in order from each sender,
/// and the receiver sees the end once they are all gone.
#[test]
fn senders_on_many_runtimes_reach_one_receiver_in_order() {
    const THREADS: usize = 6;
    const VALUES: usize = 20_000;
    let (tx, mut rx) = mpsc::channel(3);
    let threads: Vec<_> = (0..THREADS)
        .map(|thread| {
            let tx = tx.clone();
            thread::spawn(move || {
                run(async move {
                    for value in 0..VALUES {
                        tx.send((thread, value)).await.unwrap();
                        if value % 1000 == 0 {
                            yield_now().await;
                        }
                    }
                });
            })
        })
        .collect();
    drop(tx);
    run(async move {
        let mut next = [0; THREADS];
        let mut count = 0;
        while let Some((thread, value)) = rx.recv().await {
            assert_eq!(next[thread], value, "in order from each sender");
            next[thread] += 1;
            count += 1;
            if count % 4999 == 0 {
                sleep(Duration::from_millis(1)).await;
            }
        }
        assert_eq!(count, THREADS * VALUES);
    });
    for thread in threads {
        thread.join().unwrap();
    }
}

/// However many threads race to fill a channel, it takes no more than its
/// capacity, and each value taken makes room for exactly one more.
#[test]
fn racing_senders_never_overfill_a_channel() {
    const THREADS: usize = 8;
    const CAPACITY: usize = 5;
    let (tx, mut rx) = mpsc::channel(CAPACITY);
    let accepted = Arc::new(AtomicUsize::new(0));
    for round in 0..50 {
        let threads: Vec<_> = (0..THREADS)
            .map(|_| {
                let (tx, accepted) = (tx.clone(), Arc::clone(&accepted));
                thread::spawn(move || {
                    for _ in 0..200 {
                        if tx.try_send(round).is_ok() {
                            accepted.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        let expected = if round == 0 { CAPACITY } else { 1 };
        assert_eq!(accepted.swap(0, Ordering::Relaxed), expected);
        assert!(matches!(tx.try_send(0), Err(mpsc::TrySendError::Full(0))));
        // Takes one, leaving room for one in the next round.
        rx.try_recv().unwrap();
    }
    for _ in 0..CAPACITY - 1 {
        rx.try_recv().unwrap();
    }
    assert_eq!(rx.try_recv(), Err(mpsc::TryRecvError::Empty));
}

/// A value and its answer across two runtimes, back and forth through
/// channels of one value, many times: each wait is woken from the other
/// thread, and a wake lost on the way stops the exchange.
#[test]
fn a_ping_pong_across_runtimes_loses_no_wake() {
    const ROUNDS: u32 = 100_000;
    let (to_echo, mut echo_rx) = mpsc::channel(1);
    let (to_main, mut main_rx) = mpsc::channel(1);
    let echo = thread::spawn(move || {
        run(async move {
            while let Some(value) = echo_rx.recv().await {
                to_main.send(value + 1).await.unwrap();
            }
        });
    });
    run(async move {
        for round in 0..ROUNDS {
            to_echo.send(round).await.unwrap();
            assert_eq!(main_rx.recv().await, Some(round + 1));
        }
    });
    echo.join().unwrap();
}

/// Plain threads sending without waiting to a task, which keeps up only
/// in bursts: the task is woken for every value it waits for.
#[test]
fn values_sent_from_plain_threads_wake_the_receiver() {
    const THREADS: usize = 4;
    const VALUES: usize = 50_000;
    let (tx, mut rx) = mpsc::channel(8);
    let threads: Vec<_> = (0..THREADS)
        .map(|_| {
            let tx = tx.clone();
            thread::spawn(move || {
                let mut sent = 0;
                while sent < VALUES {
                    match tx.try_send(sent) {
                        Ok(()) => sent += 1,
                        Err(mpsc::TrySendError::Full(_)) => thread::yield_now(),
                        Err(mpsc::TrySendError::Closed(_)) => panic!("closed early"),
                    }
                }
            })
        })
        .collect();
    drop(tx);
    let count = run(async move {
        let mut count = 0;
        while rx.recv().await.is_some() {
            count += 1;
        }
        count
    });
    assert_eq!(count, THREADS * VALUES);
    for thread in threads {
        thread.join().unwrap();
    }
}

/// A receiver dropped while senders on other runtimes send, some waiting
/// for room: each sender learns of it, no value is dropped twice or kept,
/// and each comes back or is dropped by the channel.
#[test]
fn a_receiver_that_goes_mid_stream_stops_every_sender() {
    const THREADS: usize = 4;
    for _ in 0..20 {
        let drops = Arc::new(AtomicUsize::new(0));
        let (tx, mut rx) = mpsc::channel(2);
        let threads: Vec<_> = (0..THREADS)
            .map(|_| {
                let (tx, drops) = (tx.clone(), Arc::clone(&drops));
                thread::spawn(move || {
                    run(async move {
                        let mut made = 0;
                        loop {
                            made += 1;
                            if let Err(err) = tx.send(Counted(Arc::clone(&drops))).await {
                                assert!(tx.is_closed());
                                drop(err);
                                return made;
                            }
                        }
                    })
                })
            })
            .collect();
        drop(tx);
        run(async move {
            for _ in 0..100 {
                drop(rx.recv().await.unwrap());
            }
            drop(rx);
        });
        let made: usize = threads.into_iter().map(|t| t.join().unwrap()).sum();
        assert_eq!(drops.load(Ordering::Relaxed), made);
    }
}

/// Senders that go mid-stream, on other runtimes: the receiver takes all
/// they sent, and then sees the end, without waiting for it in vain.
#[test]
fn senders_that_go_mid_stream_end_the_channel() {
    for round in 0..50 {
        let (tx, mut rx) = mpsc::channel(4);
        let threads: Vec<_> = (0..3)
            .map(|thread| {
                let tx = tx.clone();
                thread::spawn(move || {
                    run(async move {
                        for value in 0..round * 10 + thread {
                            tx.send(value).await.unwrap();
                        }
                    });
                })
            })
            .collect();
        drop(tx);
        let count = run(async move {
            let mut count = 0;
            while rx.recv().await.is_some() {
                count += 1;
            }
            assert_eq!(rx.try_recv(), Err(mpsc::TryRecvError::Closed));
            count
        });
        assert_eq!(count, 3 * round * 10 + 3);
        for thread in threads {
            thread.join().unwrap();
        }
    }
}

/// A value that, as it is dropped, sends into its own channel: a drop under
/// a lock of the channel would deadlock.
struct SendsAsItGoes(Option<mpsc::Sender<SendsAsItGoes>>, Arc<AtomicUsize>);

impl Drop for SendsAsItGoes {
    fn drop(&mut self) {
        let Some(tx) = self.0.take() else { return };
        assert!(tx.is_closed());
        let echo = SendsAsItGoes(None, Arc::clone(&self.1));
        assert!(matches!(
            tx.try_send(echo),
            Err(mpsc::TrySendError::Closed(_))
        ));
        self.1.fetch_add(1, Ordering::Relaxed);
    }
}

/// The values left in a channel are dropped outside any lock of it.
#[test]
fn values_are_dropped_outside_the_channels_locks() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = mpsc::channel(4);
    for _ in 0..3 {
        tx.try_send(SendsAsItGoes(Some(tx.clone()), Arc::clone(&drops)))
            .unwrap_or_else(|_| panic!("room"));
    }
    drop(rx);
    assert_eq!(drops.load(Ordering::Relaxed), 3);

    // A oneshot's value, dropped by its receiver, uses the channel too.
    struct ChecksItsChannel(Option<oneshot::Sender<u8>>, Arc<AtomicUsize>);
    impl Drop for ChecksItsChannel {
        fn drop(&mut self) {
            let tx = self.0.take().unwrap();
            assert!(tx.is_closed());
            assert_eq!(tx.send(1), Err(1));
            self.1.fetch_add(1, Ordering::Relaxed);
        }
    }
    let (tx, rx) = oneshot::channel();
    let (inner_tx, inner_rx) = oneshot::channel();
    drop(inner_rx);
    tx.send(ChecksItsChannel(Some(inner_tx), Arc::clone(&drops)))
        .unwrap_or_else(|_| panic!("the receiver is there"));
    drop(rx);
    assert_eq!(drops.load(Ordering::Relaxed), 4);
}

/// A receiver that waits in one task and then in another, as the sender on
/// another thread sends: the task waiting last is the one woken.
#[test]
fn a_receiver_moved_between_tasks_wakes_the_last() {
    for _ in 0..200 {
        let (tx, mut rx) = mpsc::channel(1);
        let (go, wait) = std::sync::mpsc::channel();
        let sender = thread::spawn(move || {
            wait.recv().unwrap();
            tx.try_send(7).unwrap();
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
            assert_eq!(value, Some(7));
        });
        sender.join().unwrap();
    }
}

/// A oneshot's sender and receiver on two threads, racing to send, to drop
/// or to wait: the value is received, handed back, or dropped, once.
#[test]
fn a_oneshot_settles_every_race_once() {
    for round in 0..2000 {
        let drops = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = oneshot::channel();
        let value = Counted(Arc::clone(&drops));
        let sender = thread::spawn(move || {
            if round % 3 == 0 {
                drop(tx);
                return None;
            }
            tx.send(value).err()
        });
        let received = if round % 2 == 0 {
            drop(rx);
            None
        } else {
            run(rx).ok()
        };
        let handed_back = sender.join().unwrap();
        match (round % 3 == 0, round % 2 == 0) {
            // Sent and awaited: it arrives.
            (false, false) => assert!(received.is_some() && handed_back.is_none()),
            // Sent and dropped: it comes back or the receiver drops it.
            (false, true) => assert!(received.is_none()),
            // Never sent: the receiver learns so.
            (true, _) => assert!(received.is_none() && handed_back.is_none()),
        }
        let held = usize::from(received.is_some()) + usize::from(handed_back.is_some());
        drop((received, handed_back));
        assert_eq!(
            drops.load(Ordering::Relaxed),
            1,
            "dropped once ({held} held)"
        );
    }
}

/// A oneshot polled from one task and then awaited in another, with the
/// sender on another thread: the second task is woken.
#[test]
fn a_oneshot_moved_between_tasks_wakes_the_last() {
    for _ in 0..200 {
        let (tx, mut rx) = oneshot::channel();
        let (go, wait) = std::sync::mpsc::channel();
        let sender = thread::spawn(move || {
            wait.recv().unwrap();
            tx.send(5).unwrap();
        });
        run(async move {
            // In a tuple, for the receiver itself would be the output.
            let (rx,) = spawn(async move {
                let pending = poll_fn(|cx| Poll::Ready(pin!(&mut rx).poll(cx).is_pending())).await;
                assert!(pending, "nothing sent yet");
                (rx,)
            })
            .await
            .unwrap();
            go.send(()).unwrap();
            assert_eq!(spawn(rx).await.unwrap(), Ok(5));
        });
        sender.join().unwrap();
    }
}
