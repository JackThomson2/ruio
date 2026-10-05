//! Channels without a bound: values from many threads through a list of
//! blocks that grows and is filled again, closing from either side with
//! values in flight, and the same among the tasks of one thread.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use ruio_executor::sync::local::mpsc as local;
use ruio_executor::sync::mpsc;
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

/// Counts the thread's allocations, so that a test can see a channel
/// allocate nothing.
struct Counting;

thread_local! {
    /// How many allocations the thread has made.
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

// SAFETY: every allocation and deallocation goes to the system allocator as
// it came; the count beside it is a cell of the thread's own.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
        // SAFETY: the caller's layout, passed on.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller's pointer and layout, passed on.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

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

/// The halves that work across cores are `Send` when their values are, and
/// neither half of the local channel is, whatever its values are.
const _: () = {
    assert!(Sendness::<mpsc::UnboundedSender<u8>>::SEND);
    assert!(Sendness::<mpsc::UnboundedReceiver<u8>>::SEND);
    assert!(!Sendness::<mpsc::UnboundedSender<Rc<u8>>>::SEND);
    assert!(!Sendness::<mpsc::UnboundedReceiver<Rc<u8>>>::SEND);
    assert!(!Sendness::<local::UnboundedSender<u8>>::SEND);
    assert!(!Sendness::<local::UnboundedReceiver<u8>>::SEND);
};

/// A value that counts its drops.
struct Counted(Arc<AtomicUsize>);

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

/// A value that counts its drops, on one thread.
struct LocalCounted(Rc<Cell<usize>>);

impl Drop for LocalCounted {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

/// Senders on several threads, each with its own runtime, sending without
/// waiting while the receiver keeps up only in bursts: every value arrives,
/// in order from each sender, through a list of many blocks, and the
/// receiver sees the end once they are all gone.
#[test]
fn senders_on_many_runtimes_reach_one_receiver_in_order() {
    const THREADS: usize = 6;
    const VALUES: usize = 20_000;
    let (tx, mut rx) = mpsc::unbounded_channel();
    let threads: Vec<_> = (0..THREADS)
        .map(|thread| {
            let tx = tx.clone();
            thread::spawn(move || {
                run(async move {
                    for value in 0..VALUES {
                        tx.send((thread, value)).unwrap();
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
        assert_eq!(rx.try_recv(), Err(mpsc::TryRecvError::Closed));
    });
    for thread in threads {
        thread.join().unwrap();
    }
}

/// A value and its answer across two runtimes, back and forth, many times:
/// each wait is woken from the other thread, and a wake lost on the way
/// stops the exchange.
#[test]
fn a_ping_pong_across_runtimes_loses_no_wake() {
    const ROUNDS: u32 = 100_000;
    let (to_echo, mut echo_rx) = mpsc::unbounded_channel();
    let (to_main, mut main_rx) = mpsc::unbounded_channel();
    let echo = thread::spawn(move || {
        run(async move {
            while let Some(value) = echo_rx.recv().await {
                to_main.send(value + 1).unwrap();
            }
        });
    });
    run(async move {
        for round in 0..ROUNDS {
            to_echo.send(round).unwrap();
            assert_eq!(main_rx.recv().await, Some(round + 1));
        }
    });
    echo.join().unwrap();
}

/// Plain threads, with no runtime, sending to a task: the task is woken for
/// every value it waits for.
#[test]
fn values_sent_from_plain_threads_wake_the_receiver() {
    const THREADS: usize = 4;
    const VALUES: usize = 50_000;
    let (tx, mut rx) = mpsc::unbounded_channel();
    let threads: Vec<_> = (0..THREADS)
        .map(|_| {
            let tx = tx.clone();
            thread::spawn(move || {
                for value in 0..VALUES {
                    tx.send(value).unwrap();
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

/// Taking without waiting finds the channel empty, then its values, then
/// empty again, and closed once every sender is gone.
#[test]
fn try_recv_reports_empty_then_closed() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    assert_eq!(rx.try_recv(), Err(mpsc::TryRecvError::Empty));
    let other = tx.clone();
    tx.send(1).unwrap();
    other.send(2).unwrap();
    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(rx.try_recv(), Ok(2));
    assert_eq!(rx.try_recv(), Err(mpsc::TryRecvError::Empty));
    drop(tx);
    assert_eq!(
        rx.try_recv(),
        Err(mpsc::TryRecvError::Empty),
        "one sender left"
    );
    other.send(3).unwrap();
    drop(other);
    assert_eq!(rx.try_recv(), Ok(3));
    assert_eq!(rx.try_recv(), Err(mpsc::TryRecvError::Closed));
    assert_eq!(run(rx.recv()), None);
}

/// A send after the receiver is gone fails, handing the value back, which
/// the sender then drops; the sender can tell beforehand.
#[test]
fn a_send_after_the_receiver_is_gone_hands_the_value_back() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = mpsc::unbounded_channel();
    assert!(!tx.is_closed());
    drop(rx);
    assert!(tx.is_closed());
    let err = tx.send(Counted(Arc::clone(&drops))).unwrap_err();
    assert_eq!(drops.load(Ordering::Relaxed), 0, "handed back, not dropped");
    assert_eq!(err.to_string(), "the receiver is gone");
    let mpsc::SendError(value) = err;
    drop(value);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

/// Closing the channel fails the sends after it, while the values sent
/// before are still taken; then the receiver sees the end, though senders
/// remain.
#[test]
fn closing_fails_later_sends_while_the_queued_values_drain() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    for value in 0..40 {
        tx.send(value).unwrap();
    }
    rx.close();
    assert!(tx.is_closed());
    assert_eq!(tx.send(40), Err(mpsc::SendError(40)));
    for value in 0..40 {
        assert_eq!(rx.try_recv(), Ok(value));
    }
    assert_eq!(rx.try_recv(), Err(mpsc::TryRecvError::Closed));
    assert_eq!(run(rx.recv()), None);
    rx.close();
    assert_eq!(
        rx.try_recv(),
        Err(mpsc::TryRecvError::Closed),
        "closed twice"
    );
    assert_eq!(tx.send(41), Err(mpsc::SendError(41)));
    drop(tx);
    assert_eq!(rx.try_recv(), Err(mpsc::TryRecvError::Closed));
}

/// A receiver that closes the channel while senders on other runtimes send
/// takes every value whose send succeeded before it sees the end, and the
/// senders get the rest back: each value is dropped once.
#[test]
fn closing_takes_every_value_whose_send_succeeded() {
    const THREADS: usize = 4;
    for _ in 0..10 {
        let drops = Arc::new(AtomicUsize::new(0));
        let (tx, mut rx) = mpsc::unbounded_channel();
        let threads: Vec<_> = (0..THREADS)
            .map(|_| {
                let (tx, drops) = (tx.clone(), Arc::clone(&drops));
                thread::spawn(move || {
                    run(async move {
                        let mut sent = 0;
                        loop {
                            match tx.send(Counted(Arc::clone(&drops))) {
                                Ok(()) => sent += 1,
                                Err(err) => {
                                    assert!(tx.is_closed());
                                    drop(err);
                                    return sent;
                                }
                            }
                            yield_now().await;
                        }
                    })
                })
            })
            .collect();
        drop(tx);
        let received = run(async move {
            let mut received = 0;
            for _ in 0..100 {
                drop(rx.recv().await.unwrap());
                received += 1;
            }
            rx.close();
            while let Some(value) = rx.recv().await {
                drop(value);
                received += 1;
            }
            assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Closed)));
            received
        });
        let sent: usize = threads.into_iter().map(|t| t.join().unwrap()).sum();
        assert_eq!(received, sent, "every value sent was taken");
        assert_eq!(drops.load(Ordering::Relaxed), sent + THREADS);
    }
}

/// A value that, as it is dropped, sends into its own channel: the send
/// fails, and the value it sent comes back to be dropped in turn.
struct SendsAsItGoes(
    Option<mpsc::UnboundedSender<SendsAsItGoes>>,
    Arc<AtomicUsize>,
);

impl Drop for SendsAsItGoes {
    fn drop(&mut self) {
        let Some(tx) = self.0.take() else { return };
        assert!(tx.is_closed());
        let echo = SendsAsItGoes(None, Arc::clone(&self.1));
        assert!(tx.send(echo).is_err());
        self.1.fetch_add(1, Ordering::Relaxed);
    }
}

/// The values sent and not taken are dropped once: by the receiver as it
/// goes, or, once the last sender has gone, as the receiver goes after
/// taking what it wants; a value's drop may use the channel.
#[test]
fn values_left_in_the_channel_are_dropped_once() {
    const VALUES: usize = 100;
    // The receiver goes first, with values waiting.
    let drops = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = mpsc::unbounded_channel();
    for _ in 0..VALUES {
        tx.send(Counted(Arc::clone(&drops))).unwrap();
    }
    drop(rx);
    assert_eq!(
        drops.load(Ordering::Relaxed),
        VALUES,
        "dropped by the receiver"
    );
    drop(tx);
    assert_eq!(drops.load(Ordering::Relaxed), VALUES);

    // The last sender goes first: the values wait for the receiver, which
    // takes some and drops the rest as it goes.
    let drops = Arc::new(AtomicUsize::new(0));
    let (tx, mut rx) = mpsc::unbounded_channel();
    for _ in 0..VALUES {
        tx.send(Counted(Arc::clone(&drops))).unwrap();
    }
    drop(tx);
    for _ in 0..VALUES / 2 {
        let value = rx.try_recv().unwrap();
        drop(value);
    }
    assert_eq!(drops.load(Ordering::Relaxed), VALUES / 2);
    drop(rx);
    assert_eq!(drops.load(Ordering::Relaxed), VALUES);

    // Values whose drop sends into the channel they wait in.
    let drops = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = mpsc::unbounded_channel();
    for _ in 0..3 {
        tx.send(SendsAsItGoes(Some(tx.clone()), Arc::clone(&drops)))
            .unwrap_or_else(|_| panic!("the receiver is there"));
    }
    drop(rx);
    assert_eq!(drops.load(Ordering::Relaxed), 3);
    // One handed back is dropped by the one that sent it.
    assert!(
        tx.send(SendsAsItGoes(Some(tx.clone()), Arc::clone(&drops)))
            .is_err()
    );
    assert_eq!(drops.load(Ordering::Relaxed), 4);

    // A value whose drop sends into the channel it is received from, while
    // the receiver is there: the send goes in.
    struct SendsBack(Option<mpsc::UnboundedSender<SendsBack>>, u8);
    impl Drop for SendsBack {
        fn drop(&mut self) {
            if let Some(tx) = self.0.take() {
                tx.send(SendsBack(None, self.1 + 1)).unwrap();
            }
        }
    }
    let (tx, mut rx) = mpsc::unbounded_channel();
    tx.send(SendsBack(Some(tx.clone()), 1)).unwrap();
    drop(rx.try_recv().unwrap());
    assert_eq!(rx.try_recv().unwrap().1, 2);
    assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
}

/// A receiver dropped while senders on other runtimes keep sending: each
/// sender learns of it, and each value is taken, handed back, or dropped by
/// the channel, once.
#[test]
fn a_receiver_that_goes_mid_stream_stops_every_sender() {
    const THREADS: usize = 4;
    for _ in 0..20 {
        let drops = Arc::new(AtomicUsize::new(0));
        let (tx, mut rx) = mpsc::unbounded_channel();
        let threads: Vec<_> = (0..THREADS)
            .map(|_| {
                let (tx, drops) = (tx.clone(), Arc::clone(&drops));
                thread::spawn(move || {
                    run(async move {
                        let mut made = 0;
                        loop {
                            made += 1;
                            if let Err(err) = tx.send(Counted(Arc::clone(&drops))) {
                                assert!(tx.is_closed());
                                drop(err);
                                return made;
                            }
                            yield_now().await;
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

/// A channel in a steady state allocates nothing: the blocks its values
/// went through come back for the next ones, one value at a time or in
/// bursts that span several.
#[test]
fn a_channel_in_a_steady_state_allocates_nothing() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    // The first turns round the blocks make them.
    for value in 0..100 {
        tx.send(value).unwrap();
        assert_eq!(rx.try_recv(), Ok(value));
    }
    let before = ALLOCATIONS.get();
    for value in 0..10_000 {
        tx.send(value).unwrap();
        assert_eq!(rx.try_recv(), Ok(value));
    }
    assert_eq!(ALLOCATIONS.get(), before, "one value at a time");

    const BURST: usize = 50;
    for _ in 0..10 {
        for value in 0..BURST {
            tx.send(value).unwrap();
        }
        for value in 0..BURST {
            assert_eq!(rx.try_recv(), Ok(value));
        }
    }
    let before = ALLOCATIONS.get();
    for _ in 0..1000 {
        for value in 0..BURST {
            tx.send(value).unwrap();
        }
        for value in 0..BURST {
            assert_eq!(rx.try_recv(), Ok(value));
        }
    }
    assert_eq!(ALLOCATIONS.get(), before, "in bursts of {BURST}");
    assert_eq!(rx.try_recv(), Err(mpsc::TryRecvError::Empty));
}

/// A task that always finds a channel ready yields once its budget is
/// spent, so that the other tasks run; on either channel.
#[test]
fn a_ready_channel_spends_the_budget() {
    run(async move {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let (local_tx, mut local_rx) = local::unbounded_channel();
        let ran = Rc::new(Cell::new(false));
        let other = spawn({
            let ran = Rc::clone(&ran);
            async move { ran.set(true) }
        });
        let mut rounds = 0;
        while !ran.get() {
            tx.send(rounds).unwrap();
            assert_eq!(rx.recv().await, Some(rounds));
            rounds += 1;
            assert!(rounds < 10_000, "the other task ran in time");
        }
        other.await.unwrap();

        ran.set(false);
        let other = spawn({
            let ran = Rc::clone(&ran);
            async move { ran.set(true) }
        });
        let mut rounds = 0;
        while !ran.get() {
            local_tx.send(rounds).unwrap();
            assert_eq!(local_rx.recv().await, Some(rounds));
            rounds += 1;
            assert!(rounds < 10_000, "the other task ran in time");
        }
        other.await.unwrap();
    });
}

/// Values from several tasks of one runtime come out in order from each
/// sender, and the receiver sees the end once every sender is gone.
#[test]
fn local_values_come_out_in_order_from_each_sender() {
    const SENDERS: usize = 3;
    const VALUES: usize = 5000;
    run(async move {
        let (tx, mut rx) = local::unbounded_channel();
        let senders: Vec<_> = (0..SENDERS)
            .map(|sender| {
                let tx = tx.clone();
                spawn(async move {
                    for value in 0..VALUES {
                        tx.send((sender, value)).unwrap();
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
        assert_eq!(rx.try_recv(), Err(local::TryRecvError::Closed));
        for sender in senders {
            sender.await.unwrap();
        }
    });
}

/// A local channel carries values that are not `Send`, such as an `Rc`.
#[test]
fn a_local_channel_carries_an_rc() {
    let shared = Rc::new(5);
    let (tx, mut rx) = local::unbounded_channel();
    tx.send(Rc::clone(&shared)).unwrap();
    assert_eq!(Rc::strong_count(&shared), 2);
    let received = run(async {
        let value = rx.recv().await.unwrap();
        assert!(Rc::ptr_eq(&value, &shared));
        value
    });
    assert_eq!(Rc::strong_count(&shared), 2);
    drop(received);
    assert_eq!(Rc::strong_count(&shared), 1);
    drop(tx);
}

/// Taking without waiting from a local channel finds it empty, then its
/// values, then empty again, and closed once every sender is gone.
#[test]
fn a_local_try_recv_reports_empty_then_closed() {
    let (tx, mut rx) = local::unbounded_channel();
    assert_eq!(rx.try_recv(), Err(local::TryRecvError::Empty));
    let other = tx.clone();
    tx.send(1).unwrap();
    other.send(2).unwrap();
    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(rx.try_recv(), Ok(2));
    assert_eq!(rx.try_recv(), Err(local::TryRecvError::Empty));
    drop(tx);
    assert_eq!(
        rx.try_recv(),
        Err(local::TryRecvError::Empty),
        "one sender left"
    );
    other.send(3).unwrap();
    drop(other);
    assert_eq!(rx.try_recv(), Ok(3));
    assert_eq!(rx.try_recv(), Err(local::TryRecvError::Closed));
    assert_eq!(run(rx.recv()), None);
}

/// A send into a local channel after the receiver is gone fails, handing
/// the value back; the sender can tell beforehand.
#[test]
fn a_local_send_after_the_receiver_is_gone_hands_the_value_back() {
    let drops = Rc::new(Cell::new(0));
    let (tx, rx) = local::unbounded_channel();
    assert!(!tx.is_closed());
    drop(rx);
    assert!(tx.is_closed());
    let err = tx.send(LocalCounted(Rc::clone(&drops))).unwrap_err();
    assert_eq!(drops.get(), 0, "handed back, not dropped");
    assert_eq!(err.to_string(), "the receiver is gone");
    let local::SendError(value) = err;
    drop(value);
    assert_eq!(drops.get(), 1);
}

/// Closing a local channel fails the sends after it, while the values sent
/// before are still taken; then the receiver sees the end, though senders
/// remain.
#[test]
fn closing_a_local_channel_fails_later_sends_while_the_queued_values_drain() {
    let (tx, mut rx) = local::unbounded_channel();
    for value in 0..40 {
        tx.send(value).unwrap();
    }
    rx.close();
    assert!(tx.is_closed());
    assert_eq!(tx.send(40), Err(local::SendError(40)));
    for value in 0..40 {
        assert_eq!(rx.try_recv(), Ok(value));
    }
    assert_eq!(rx.try_recv(), Err(local::TryRecvError::Closed));
    assert_eq!(run(rx.recv()), None);
    rx.close();
    assert_eq!(
        rx.try_recv(),
        Err(local::TryRecvError::Closed),
        "closed twice"
    );
    drop(tx);
    assert_eq!(rx.try_recv(), Err(local::TryRecvError::Closed));
}

/// A value that, as it is dropped, sends into its own local channel and
/// looks at it: a drop under a borrow of the channel would panic.
struct LocalSendsAsItGoes(
    Option<local::UnboundedSender<LocalSendsAsItGoes>>,
    Rc<Cell<usize>>,
);

impl Drop for LocalSendsAsItGoes {
    fn drop(&mut self) {
        let Some(tx) = self.0.take() else { return };
        assert!(tx.is_closed());
        let echo = LocalSendsAsItGoes(None, Rc::clone(&self.1));
        assert!(tx.send(echo).is_err());
        self.1.set(self.1.get() + 1);
    }
}

/// The values left in a local channel are dropped once, outside any borrow
/// of it, whichever half goes first.
#[test]
fn a_local_channels_values_are_dropped_once_outside_its_borrows() {
    const VALUES: usize = 100;
    // The receiver goes first, with values waiting.
    let drops = Rc::new(Cell::new(0));
    let (tx, rx) = local::unbounded_channel();
    for _ in 0..VALUES {
        tx.send(LocalCounted(Rc::clone(&drops))).unwrap();
    }
    drop(rx);
    assert_eq!(drops.get(), VALUES, "dropped by the receiver");
    drop(tx);
    assert_eq!(drops.get(), VALUES);

    // The last sender goes first: the values wait for the receiver.
    let drops = Rc::new(Cell::new(0));
    let (tx, mut rx) = local::unbounded_channel();
    for _ in 0..VALUES {
        tx.send(LocalCounted(Rc::clone(&drops))).unwrap();
    }
    drop(tx);
    for _ in 0..VALUES / 2 {
        drop(rx.try_recv().unwrap());
    }
    assert_eq!(drops.get(), VALUES / 2);
    drop(rx);
    assert_eq!(drops.get(), VALUES);

    // Values whose drop uses the channel they wait in.
    let drops = Rc::new(Cell::new(0));
    let (tx, rx) = local::unbounded_channel();
    for _ in 0..3 {
        tx.send(LocalSendsAsItGoes(Some(tx.clone()), Rc::clone(&drops)))
            .unwrap_or_else(|_| panic!("the receiver is there"));
    }
    drop(rx);
    assert_eq!(drops.get(), 3);
    // One handed back is dropped by the one that sent it.
    assert!(
        tx.send(LocalSendsAsItGoes(Some(tx.clone()), Rc::clone(&drops)))
            .is_err()
    );
    assert_eq!(drops.get(), 4);

    // A value whose drop sends into the channel it is received from, while
    // the receiver is there: the send goes in.
    struct SendsBack(Option<local::UnboundedSender<SendsBack>>, u8);
    impl Drop for SendsBack {
        fn drop(&mut self) {
            if let Some(tx) = self.0.take() {
                tx.send(SendsBack(None, self.1 + 1)).unwrap();
            }
        }
    }
    let (tx, mut rx) = local::unbounded_channel();
    tx.send(SendsBack(Some(tx.clone()), 1)).unwrap();
    drop(rx.try_recv().unwrap());
    assert_eq!(rx.try_recv().unwrap().1, 2);
    assert!(matches!(rx.try_recv(), Err(local::TryRecvError::Empty)));
}

/// A local receiver dropped while sender tasks keep sending: each sender
/// learns of it, and each value is taken, handed back, or dropped, once.
#[test]
fn a_local_receiver_that_goes_mid_stream_stops_every_sender() {
    run(async move {
        let drops = Rc::new(Cell::new(0));
        let (tx, mut rx) = local::unbounded_channel();
        let senders: Vec<_> = (0..4)
            .map(|_| {
                let (tx, drops) = (tx.clone(), Rc::clone(&drops));
                spawn(async move {
                    let mut made = 0;
                    loop {
                        made += 1;
                        if let Err(err) = tx.send(LocalCounted(Rc::clone(&drops))) {
                            assert!(tx.is_closed());
                            drop(err);
                            return made;
                        }
                        yield_now().await;
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
