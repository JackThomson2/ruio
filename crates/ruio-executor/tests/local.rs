//! The channels of one thread: order, the bound, waking, and closing from
//! either side, all among the tasks of one runtime.

use std::cell::{Cell, RefCell};
use std::future::poll_fn;
use std::marker::PhantomData;
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use ruio_executor::sync::local::{mpsc, oneshot};
use ruio_executor::sync::{mpsc as send_mpsc, oneshot as send_oneshot};
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

/// Whether `T` is `Send`: the inherent constant, found before the trait's,
/// is there only when it is.
struct Sendness<T>(PhantomData<T>);

trait NotSend {
    const SEND: bool = false;
}

impl<T> NotSend for Sendness<T> {}

impl<T: Send> Sendness<T> {
    const SEND: bool = true;
}

/// Neither half of either channel is `Send`, whatever its values are, so
/// that the compiler keeps them on their thread; the channels that work
/// across cores are, as the check itself shows.
const _: () = {
    assert!(!Sendness::<mpsc::Sender<u8>>::SEND);
    assert!(!Sendness::<mpsc::Receiver<u8>>::SEND);
    assert!(!Sendness::<oneshot::Sender<u8>>::SEND);
    assert!(!Sendness::<oneshot::Receiver<u8>>::SEND);
    assert!(Sendness::<send_mpsc::Sender<u8>>::SEND);
    assert!(Sendness::<send_mpsc::Receiver<u8>>::SEND);
    assert!(Sendness::<send_oneshot::Sender<u8>>::SEND);
    assert!(Sendness::<send_oneshot::Receiver<u8>>::SEND);
};

/// A value that counts its drops.
struct Counted(Rc<Cell<usize>>);

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

/// A waker that counts how often it is woken, and whose clones are counted
/// by its `Arc`.
struct Counter(AtomicUsize);

impl Counter {
    fn new() -> Arc<Self> {
        Arc::new(Self(AtomicUsize::new(0)))
    }

    fn woken(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

impl Wake for Counter {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

/// Values come out in the order they went in, from each sender, and the
/// channel takes no more than its capacity until one is taken.
#[test]
fn values_come_out_in_order_and_within_the_bound() {
    const CAPACITY: usize = 4;
    let (tx, mut rx) = mpsc::channel(CAPACITY);
    for value in 0..CAPACITY {
        tx.try_send(value).unwrap();
    }
    assert!(matches!(tx.try_send(9), Err(mpsc::TrySendError::Full(9))));
    assert_eq!(rx.try_recv(), Ok(0));
    tx.try_send(CAPACITY).unwrap();
    assert!(matches!(tx.try_send(9), Err(mpsc::TrySendError::Full(9))));
    for value in 1..=CAPACITY {
        assert_eq!(rx.try_recv(), Ok(value));
    }
    assert_eq!(rx.try_recv(), Err(mpsc::TryRecvError::Empty));

    // Interleaved senders, each in order, through a small channel.
    const SENDERS: usize = 3;
    const VALUES: usize = 5000;
    let (tx, mut rx) = mpsc::channel(3);
    run(async move {
        let senders: Vec<_> = (0..SENDERS)
            .map(|sender| {
                let tx = tx.clone();
                spawn(async move {
                    for value in 0..VALUES {
                        tx.send((sender, value)).await.unwrap();
                        if value % 7 == 0 {
                            yield_now().await;
                        }
                    }
                })
            })
            .collect();
        drop(tx);
        let mut next = [0; SENDERS];
        let mut count = 0;
        while let Some((sender, value)) = rx.recv().await {
            assert_eq!(next[sender], value, "in order from each sender");
            next[sender] += 1;
            count += 1;
        }
        assert_eq!(count, SENDERS * VALUES);
        assert_eq!(rx.try_recv(), Err(mpsc::TryRecvError::Closed));
        for sender in senders {
            sender.await.unwrap();
        }
    });
}

/// Senders that wait for room in a full channel are woken as the receiver
/// makes some, and only then: a wake lost here would hang the test.
#[test]
fn senders_waiting_for_room_are_woken_as_it_appears() {
    const SENDERS: usize = 4;
    const VALUES: usize = 1000;
    run(async move {
        let (tx, mut rx) = mpsc::channel(1);
        let polled = Rc::new(Cell::new(0));
        let senders: Vec<_> = (0..SENDERS)
            .map(|_| {
                let (tx, polled) = (tx.clone(), Rc::clone(&polled));
                spawn(async move {
                    for value in 0..VALUES {
                        let mut send = pin!(tx.send(value));
                        poll_fn(|cx| {
                            polled.set(polled.get() + 1);
                            send.as_mut().poll(cx)
                        })
                        .await
                        .unwrap();
                    }
                })
            })
            .collect();
        drop(tx);
        // Lets every sender fill the channel and wait, before taking any.
        for _ in 0..8 {
            yield_now().await;
        }
        assert_eq!(
            polled.get(),
            SENDERS + 1,
            "one sent and waits for its second; the rest wait for their first"
        );
        let mut count = 0;
        while rx.recv().await.is_some() {
            count += 1;
            if count % 97 == 0 {
                yield_now().await;
            }
        }
        assert_eq!(count, SENDERS * VALUES);
        for sender in senders {
            sender.await.unwrap();
        }
    });
}

/// A sender that waits again from the same task clones no waker, and is
/// woken once for each time room is made while it waits.
#[test]
fn a_sender_that_waits_again_keeps_its_waker() {
    let counter = Counter::new();
    let waker = Waker::from(Arc::clone(&counter));
    let mut cx = Context::from_waker(&waker);
    let (tx, mut rx) = mpsc::channel(1);
    tx.try_send(0).unwrap();
    let mut send = pin!(tx.send(1));
    assert!(send.as_mut().poll(&mut cx).is_pending());
    assert_eq!(Arc::strong_count(&counter), 3, "one clone, registered");
    assert!(send.as_mut().poll(&mut cx).is_pending());
    assert_eq!(Arc::strong_count(&counter), 3, "the same waker");
    assert_eq!(rx.try_recv(), Ok(0));
    assert_eq!(counter.woken(), 1, "woken once, by reference");
    assert_eq!(Arc::strong_count(&counter), 3);
    assert!(send.as_mut().poll(&mut cx).is_ready());
    // Full again, with what was just sent.
    let mut send = pin!(tx.send(2));
    assert!(send.as_mut().poll(&mut cx).is_pending());
    assert_eq!(Arc::strong_count(&counter), 3, "still the same waker");
    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(counter.woken(), 2);
    assert!(send.as_mut().poll(&mut cx).is_ready());
    assert_eq!(rx.try_recv(), Ok(2));
    assert_eq!(counter.woken(), 2, "nobody waited");
    drop(tx);
    drop(rx);
    assert_eq!(Arc::strong_count(&counter), 2, "released with the channel");
}

/// A receiver that waits again from the same task clones no waker, and is
/// woken by reference.
#[test]
fn a_receiver_that_waits_again_keeps_its_waker() {
    let counter = Counter::new();
    let waker = Waker::from(Arc::clone(&counter));
    let mut cx = Context::from_waker(&waker);
    let (tx, mut rx) = mpsc::channel(2);
    assert!(rx.poll_recv(&mut cx).is_pending());
    assert_eq!(Arc::strong_count(&counter), 3, "one clone, registered");
    assert!(rx.poll_recv(&mut cx).is_pending());
    assert_eq!(Arc::strong_count(&counter), 3, "the same waker");
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();
    assert_eq!(counter.woken(), 1, "woken once: it waited once");
    assert_eq!(rx.poll_recv(&mut cx), Poll::Ready(Some(1)));
    assert_eq!(rx.poll_recv(&mut cx), Poll::Ready(Some(2)));
    assert!(rx.poll_recv(&mut cx).is_pending());
    drop(tx);
    assert_eq!(counter.woken(), 2, "woken by the last sender going");
    assert_eq!(rx.poll_recv(&mut cx), Poll::Ready(None));
    assert_eq!(Arc::strong_count(&counter), 3);
    drop(rx);
    assert_eq!(Arc::strong_count(&counter), 2, "released with the channel");

    // A oneshot's receiver, the same way.
    let (tx, mut rx) = oneshot::channel();
    assert!(pin!(&mut rx).poll(&mut cx).is_pending());
    assert!(pin!(&mut rx).poll(&mut cx).is_pending());
    assert_eq!(Arc::strong_count(&counter), 3);
    tx.send(5).unwrap();
    assert_eq!(counter.woken(), 3);
    assert_eq!(pin!(&mut rx).poll(&mut cx), Poll::Ready(Ok(5)));
    drop(rx);
    assert_eq!(Arc::strong_count(&counter), 2);
}

/// A receiver dropped while senders send, some waiting for room: each
/// sender learns of it, and each value comes back or is dropped, once.
#[test]
fn a_receiver_that_goes_mid_stream_stops_every_sender() {
    run(async move {
        let drops = Rc::new(Cell::new(0));
        let (tx, mut rx) = mpsc::channel(2);
        let senders: Vec<_> = (0..4)
            .map(|_| {
                let (tx, drops) = (tx.clone(), Rc::clone(&drops));
                spawn(async move {
                    let mut made = 0;
                    loop {
                        made += 1;
                        if let Err(err) = tx.send(Counted(Rc::clone(&drops))).await {
                            assert!(tx.is_closed());
                            assert!(matches!(
                                tx.try_send(Counted(Rc::clone(&drops))),
                                Err(mpsc::TrySendError::Closed(_))
                            ));
                            drop(err);
                            return made + 1;
                        }
                    }
                })
            })
            .collect();
        drop(tx);
        for _ in 0..100 {
            drop(rx.recv().await.unwrap());
        }
        drop(rx);
        let mut made = 0;
        for sender in senders {
            made += sender.await.unwrap();
        }
        assert_eq!(drops.get(), made);
    });
}

/// Senders that go mid-stream: the receiver takes all they sent, and then
/// sees the end, without waiting for it in vain.
#[test]
fn senders_that_go_mid_stream_end_the_channel() {
    for round in 0..20 {
        let count = run(async move {
            let (tx, mut rx) = mpsc::channel(4);
            let senders: Vec<_> = (0..3)
                .map(|sender| {
                    let tx = tx.clone();
                    spawn(async move {
                        for value in 0..round * 10 + sender {
                            tx.send(value).await.unwrap();
                        }
                    })
                })
                .collect();
            drop(tx);
            let mut count = 0;
            while rx.recv().await.is_some() {
                count += 1;
            }
            assert_eq!(rx.try_recv(), Err(mpsc::TryRecvError::Closed));
            for sender in senders {
                sender.await.unwrap();
            }
            count
        });
        assert_eq!(count, 3 * round * 10 + 3);
    }
}

/// A value that, as it is dropped, sends into its own channel and looks at
/// it: a drop under a borrow of the channel would panic.
struct SendsAsItGoes(Option<mpsc::Sender<SendsAsItGoes>>, Rc<Cell<usize>>);

impl Drop for SendsAsItGoes {
    fn drop(&mut self) {
        let Some(tx) = self.0.take() else { return };
        assert!(tx.is_closed());
        let echo = SendsAsItGoes(None, Rc::clone(&self.1));
        assert!(matches!(
            tx.try_send(echo),
            Err(mpsc::TrySendError::Closed(_))
        ));
        self.1.set(self.1.get() + 1);
    }
}

/// The values left in a channel are dropped outside any borrow of it.
#[test]
fn values_are_dropped_outside_the_channels_borrows() {
    let drops = Rc::new(Cell::new(0));
    let (tx, rx) = mpsc::channel(4);
    for _ in 0..3 {
        tx.try_send(SendsAsItGoes(Some(tx.clone()), Rc::clone(&drops)))
            .unwrap_or_else(|_| panic!("room"));
    }
    drop(rx);
    assert_eq!(drops.get(), 3);
    // One handed back is dropped by the one that sent it.
    assert!(matches!(
        tx.try_send(SendsAsItGoes(Some(tx.clone()), Rc::clone(&drops))),
        Err(mpsc::TrySendError::Closed(_))
    ));
    assert_eq!(drops.get(), 4);

    // A oneshot's value, dropped by its receiver, uses the channel too.
    struct ChecksItsChannel(Option<oneshot::Sender<u8>>, Rc<Cell<usize>>);
    impl Drop for ChecksItsChannel {
        fn drop(&mut self) {
            let tx = self.0.take().unwrap();
            assert!(tx.is_closed());
            assert_eq!(tx.send(1), Err(1));
            self.1.set(self.1.get() + 1);
        }
    }
    let (tx, rx) = oneshot::channel();
    let (inner_tx, inner_rx) = oneshot::channel();
    drop(inner_rx);
    tx.send(ChecksItsChannel(Some(inner_tx), Rc::clone(&drops)))
        .unwrap_or_else(|_| panic!("the receiver is there"));
    drop(rx);
    assert_eq!(drops.get(), 5);

    // A value whose drop sends into the channel it is received from, while
    // the receiver is there: the send goes in.
    struct SendsBack(Option<mpsc::Sender<SendsBack>>, u8);
    impl Drop for SendsBack {
        fn drop(&mut self) {
            if let Some(tx) = self.0.take() {
                tx.try_send(SendsBack(None, self.1 + 1)).unwrap();
            }
        }
    }
    let (tx, mut rx) = mpsc::channel(2);
    tx.try_send(SendsBack(Some(tx.clone()), 1)).unwrap();
    drop(rx.try_recv().unwrap());
    assert_eq!(rx.try_recv().unwrap().1, 2);
    assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
}

/// A channel, and what a waker took from it: the waker's, by the thread,
/// as a waker is `Send` and the channel is not.
struct Used {
    tx: mpsc::Sender<u8>,
    rx: mpsc::Receiver<u8>,
    got: Option<u8>,
}

thread_local! {
    static USED: RefCell<Option<Used>> = const { RefCell::new(None) };
}

/// A waker that, as it is woken, takes from the channel it waits on, sends
/// into it and waits on it again.
struct UsesItsChannel;

impl Wake for UsesItsChannel {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        USED.with_borrow_mut(|used| {
            let used = used.as_mut().unwrap();
            used.got = used.rx.try_recv().ok();
            used.tx.try_send(9).unwrap();
            let mut cx = Context::from_waker(Waker::noop());
            assert_eq!(used.rx.poll_recv(&mut cx), Poll::Ready(Some(9)));
            assert!(used.rx.poll_recv(&mut cx).is_pending());
        });
    }
}

/// A waker that uses the channel it waits on as it is woken: nothing of the
/// channel is borrowed while a waker runs.
#[test]
fn a_waker_may_use_its_channel_as_it_is_woken() {
    let (tx, rx) = mpsc::channel(1);
    USED.set(Some(Used {
        tx: tx.clone(),
        rx,
        got: None,
    }));
    let waker = Waker::from(Arc::new(UsesItsChannel));
    let mut cx = Context::from_waker(&waker);
    let poll_recv =
        |cx: &mut Context<'_>| USED.with_borrow_mut(|used| used.as_mut().unwrap().rx.poll_recv(cx));
    let got = || USED.with_borrow(|used| used.as_ref().unwrap().got);
    assert!(poll_recv(&mut cx).is_pending());
    tx.try_send(4).unwrap();
    assert_eq!(got(), Some(4));
    // The wake left a waker that wakes nothing in this one's stead; waiting
    // again from this one puts it back.
    assert!(poll_recv(&mut cx).is_pending());
    tx.try_send(5).unwrap();
    assert_eq!(got(), Some(5));
    drop(tx);
    // Taken out before it is dropped: the last sender's going wakes the
    // receiver, and a waker runs with nothing of the thread's borrowed.
    drop(USED.take());
}

/// A receiver that waits in one task and then in another, as a sender
/// sends: the task waiting last is the one woken.
#[test]
fn a_receiver_moved_between_tasks_wakes_the_last() {
    run(async move {
        let (tx, mut rx) = mpsc::channel(1);
        let mut rx = spawn(async move {
            // Waits once here, then leaves for another task.
            let pending = poll_fn(|cx| Poll::Ready(rx.poll_recv(cx).is_pending())).await;
            assert!(pending, "nothing sent yet");
            rx
        })
        .await
        .unwrap();
        let receiver = spawn(async move { rx.recv().await });
        yield_now().await;
        tx.try_send(7).unwrap();
        assert_eq!(receiver.await.unwrap(), Some(7));

        // A oneshot's receiver, the same way.
        let (tx, mut rx) = oneshot::channel();
        let (rx,) = spawn(async move {
            let pending = poll_fn(|cx| Poll::Ready(pin!(&mut rx).poll(cx).is_pending())).await;
            assert!(pending, "nothing sent yet");
            (rx,)
        })
        .await
        .unwrap();
        let receiver = spawn(rx);
        yield_now().await;
        tx.send(5).unwrap();
        assert_eq!(receiver.await.unwrap(), Ok(5));
    });
}

/// A oneshot's sender and receiver, sending, dropping or waiting in every
/// order: the value is received, handed back, or dropped, once.
#[test]
fn a_oneshot_settles_every_order_once() {
    for round in 0..6 {
        let drops = Rc::new(Cell::new(0));
        let value = Counted(Rc::clone(&drops));
        let (tx, mut rx) = oneshot::channel();
        let (sent, handed_back, received) = match round {
            0 => {
                // Sent, then awaited.
                let handed_back = tx.send(value).err();
                (true, handed_back, run(rx).ok())
            }
            1 => {
                // Awaited in a task, then sent.
                run(async move {
                    let receiver = spawn(rx);
                    yield_now().await;
                    assert!(!tx.is_closed());
                    let handed_back = tx.send(value).err();
                    (true, handed_back, receiver.await.unwrap().ok())
                })
            }
            2 => {
                // The receiver goes, then it is sent: the sender keeps it.
                drop(rx);
                assert!(tx.is_closed());
                (true, tx.send(value).err(), None)
            }
            3 => {
                // Sent, then the receiver goes: the receiver drops it.
                let handed_back = tx.send(value).err();
                drop(rx);
                (true, handed_back, None)
            }
            4 => {
                // The sender goes without sending.
                assert!(matches!(rx.try_recv(), Err(oneshot::TryRecvError::Empty)));
                drop(tx);
                assert!(matches!(rx.try_recv(), Err(oneshot::TryRecvError::Closed)));
                drop(value);
                (false, None, None)
            }
            _ => {
                // Awaited in a task, and the sender goes without sending.
                run(async move {
                    let receiver = spawn(rx);
                    yield_now().await;
                    drop(tx);
                    assert!(receiver.await.unwrap().is_err());
                    drop(value);
                    (false, None, None)
                })
            }
        };
        let held = usize::from(received.is_some()) + usize::from(handed_back.is_some());
        if sent {
            assert_eq!(held, usize::from(round < 3), "round {round}");
        }
        drop((received, handed_back));
        assert_eq!(
            drops.get(),
            1,
            "dropped once ({held} held) in round {round}"
        );
    }
}

/// A oneshot's value can be taken without waiting once it is sent, and a
/// receiver that took it sees the channel closed after.
#[test]
fn a_oneshot_is_taken_once() {
    let (tx, mut rx) = oneshot::channel();
    assert_eq!(rx.try_recv(), Err(oneshot::TryRecvError::Empty));
    tx.send(Rc::new(3)).unwrap();
    assert_eq!(rx.try_recv().as_deref(), Ok(&3));
    assert_eq!(rx.try_recv(), Err(oneshot::TryRecvError::Closed));
    assert!(run(rx).is_err());
}

/// A task that always finds a channel ready yields once its budget is
/// spent, so that the other tasks run.
#[test]
fn a_ready_channel_spends_the_budget() {
    run(async move {
        let (tx, mut rx) = mpsc::channel(1);
        let ran = Rc::new(Cell::new(false));
        let other = spawn({
            let ran = Rc::clone(&ran);
            async move { ran.set(true) }
        });
        let mut rounds = 0;
        while !ran.get() {
            tx.try_send(rounds).unwrap();
            rx.recv().await.unwrap();
            rounds += 1;
            assert!(rounds < 10_000, "the other task ran in time");
        }
        other.await.unwrap();
    });
}
