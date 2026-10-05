//! Cancellation tokens: waits, clones, children and guards, on one runtime
//! and across threads.

use std::cell::Cell;
use std::future::{Future, pending, poll_fn};
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use ruio_executor::sync::{
    CancellationToken, DropGuard, WaitForCancellation, WaitForCancellationOwned,
};
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

/// A waker that counts its wakes.
#[derive(Default)]
struct Count(AtomicUsize);

impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl Count {
    fn wakes(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

/// Polls `future` once by hand, with a waker that wakes `count`.
fn poll_counting<F: Future + Unpin>(future: &mut F, count: &Arc<Count>) -> Poll<F::Output> {
    let waker = Waker::from(Arc::clone(count));
    Pin::new(future).poll(&mut Context::from_waker(&waker))
}

/// A value whose drop sets a flag.
struct Flag(Rc<Cell<bool>>);

impl Drop for Flag {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

#[test]
fn a_cancellation_wakes_the_task_waiting_for_it() {
    run(async {
        let token = CancellationToken::new();
        let waiting = spawn({
            let token = token.clone();
            async move { token.cancelled().await }
        });
        let owned = spawn(token.clone().cancelled_owned());
        yield_now().await;
        assert!(!waiting.is_finished());
        assert!(!owned.is_finished());
        token.cancel();
        waiting.await.unwrap();
        owned.await.unwrap();
    });
}

#[test]
fn a_cancellation_from_another_thread_wakes_the_task() {
    run(async {
        let token = CancellationToken::new();
        let canceller = {
            let token = token.clone();
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(20));
                token.cancel();
            })
        };
        token.cancelled().await;
        assert!(token.is_cancelled());
        canceller.join().unwrap();
    });
}

/// Tasks on several runtimes, each on a thread of its own, wait on one
/// token: one cancellation wakes them all.
#[test]
fn a_cancellation_reaches_waiters_on_every_runtime() {
    let token = CancellationToken::new();
    let threads: Vec<_> = (0..4)
        .map(|_| {
            let token = token.clone();
            thread::spawn(move || run(async move { token.cancelled().await }))
        })
        .collect();
    thread::sleep(Duration::from_millis(20));
    token.cancel();
    for thread in threads {
        thread.join().unwrap();
    }
}

/// A wait and a cancellation on two threads, racing: however the two
/// fall, the wait ends.
#[test]
fn a_cancellation_that_races_a_wait_is_never_missed() {
    run(async {
        for _ in 0..2000 {
            let token = CancellationToken::new();
            let canceller = {
                let token = token.clone();
                thread::spawn(move || token.cancel())
            };
            token.cancelled().await;
            canceller.join().unwrap();
        }
    });
}

#[test]
fn a_wait_on_a_cancelled_token_is_over_at_once() {
    run(async {
        let token = CancellationToken::new();
        assert!(!token.is_cancelled());
        token.cancel();
        assert!(token.is_cancelled());
        token.cancelled().await;
        token.clone().cancelled_owned().await;
        // Cancelled again, it stays cancelled, and a wait stays over.
        token.cancel();
        let mut wait = pin!(token.cancelled());
        assert!(poll_once(wait.as_mut()).await.is_ready());
        assert!(poll_once(wait.as_mut()).await.is_ready());
    });
}

#[test]
fn clones_are_the_same_token() {
    let token = CancellationToken::default();
    let clone = token.clone();
    assert!(!clone.is_cancelled());
    clone.cancel();
    assert!(token.is_cancelled());
    assert!(clone.is_cancelled());
    assert!(!CancellationToken::new().is_cancelled(), "another token");
}

/// A child, and its children, are cancelled with their parent; a child
/// cancelled on its own leaves its parent and siblings as they are; and a
/// child of a cancelled token starts cancelled.
#[test]
fn a_child_is_cancelled_with_its_parent_and_not_the_other_way_round() {
    run(async {
        let parent = CancellationToken::new();
        let child = parent.child_token();
        let grandchild = child.child_token();
        let sibling = parent.child_token();

        sibling.cancel();
        assert!(sibling.is_cancelled());
        assert!(!parent.is_cancelled());
        assert!(!child.is_cancelled());
        assert!(!grandchild.is_cancelled());

        let waiting = spawn({
            let grandchild = grandchild.clone();
            async move { grandchild.cancelled().await }
        });
        yield_now().await;
        parent.cancel();
        assert!(child.is_cancelled());
        assert!(grandchild.is_cancelled());
        waiting.await.unwrap();

        assert!(parent.child_token().is_cancelled());
        assert!(grandchild.child_token().is_cancelled());
    });
}

/// Children a hundred thousand deep: cancelling the root reaches the
/// bottom, which recursion down the tree would not.
#[test]
fn a_cancellation_reaches_the_bottom_of_a_deep_tree() {
    const DEPTH: usize = 100_000;
    let root = CancellationToken::new();
    let mut tree = vec![root.clone()];
    for _ in 0..DEPTH {
        let child = tree.last().unwrap().child_token();
        tree.push(child);
    }
    root.cancel();
    assert!(tree.iter().all(CancellationToken::is_cancelled));
}

/// A child dropped leaves its parent, and its siblings, as they were, and
/// children dropped leave the parent's list as the next is made.
#[test]
fn dropped_children_neither_hold_up_nor_grow_their_parent() {
    let parent = CancellationToken::new();
    let kept = parent.child_token();
    drop(parent.child_token());
    assert!(!parent.is_cancelled());
    for _ in 0..10_000 {
        drop(parent.child_token());
    }
    let last = parent.child_token();
    assert_eq!(
        format!("{parent:?}"),
        "CancellationToken { cancelled: false, waiters: 0, children: 2 }"
    );
    drop(kept);
    parent.cancel();
    assert!(last.is_cancelled());
    assert_eq!(
        format!("{parent:?}"),
        "CancellationToken { cancelled: true, waiters: 0, children: 0 }"
    );
}

#[test]
fn a_drop_guard_cancels_its_token_unless_disarmed() {
    let token = CancellationToken::new();
    let guard = token.clone().drop_guard();
    assert_eq!(
        format!("{guard:?}"),
        "DropGuard { token: CancellationToken { cancelled: false, waiters: 0, children: 0 } }"
    );
    drop(guard.disarm());
    assert!(!token.is_cancelled());
    drop(token.clone().drop_guard());
    assert!(token.is_cancelled());

    // A guard held by a task cancels the token as the task is dropped.
    run(async {
        let token = CancellationToken::new();
        let guard = token.clone().drop_guard();
        let task = spawn(async move {
            let _guard = guard;
            pending::<()>().await;
        });
        yield_now().await;
        assert!(!token.is_cancelled());
        task.abort();
        token.cancelled().await;
    });
}

/// `run_until_cancelled` gives the future's output if it finishes first,
/// and `None` if the cancellation comes first, dropping the future; a
/// future that is ready wins over a cancellation that is there already.
#[test]
fn run_until_cancelled_gives_the_output_or_drops_the_future() {
    run(async {
        let token = CancellationToken::new();
        assert_eq!(token.run_until_cancelled(async { 5 }).await, Some(5));

        let dropped = Rc::new(Cell::new(false));
        let flag = Flag(dropped.clone());
        let running = spawn({
            let token = token.clone();
            async move {
                token
                    .run_until_cancelled(async move {
                        let _flag = flag;
                        pending::<()>().await;
                    })
                    .await
            }
        });
        yield_now().await;
        assert!(!dropped.get());
        token.cancel();
        assert_eq!(running.await.unwrap(), None);
        assert!(dropped.get());

        assert_eq!(token.run_until_cancelled(async { 6 }).await, Some(6));
        assert_eq!(token.run_until_cancelled(pending::<()>()).await, None);
    });
}

/// A hundred tasks wait on one token, half of them holding it: one
/// cancellation wakes them all.
#[test]
fn one_cancellation_wakes_every_waiter() {
    const WAITERS: usize = 100;
    run(async {
        let token = CancellationToken::new();
        let waiting = Rc::new(Cell::new(0));
        let waiters: Vec<_> = (0..WAITERS)
            .map(|n| {
                let (token, waiting) = (token.clone(), waiting.clone());
                spawn(async move {
                    waiting.set(waiting.get() + 1);
                    if n % 2 == 0 {
                        token.cancelled().await;
                    } else {
                        token.cancelled_owned().await;
                    }
                })
            })
            .collect();
        // A pass runs only so many tasks before this one runs again.
        while waiting.get() < WAITERS {
            yield_now().await;
        }
        assert_eq!(
            format!("{token:?}"),
            "CancellationToken { cancelled: false, waiters: 100, children: 0 }"
        );
        token.cancel();
        for waiter in waiters {
            waiter.await.unwrap();
        }
    });
}

/// A waiter dropped before the cancellation leaves no waker behind, and
/// one moved to another task leaves only the new task's: the cancellation
/// wakes the live waiters, once each, and nobody else.
#[test]
fn a_waiter_that_gives_up_leaves_no_waker_behind() {
    let token = CancellationToken::new();
    let (quitter, stayer, mover, moved) = (
        Arc::new(Count::default()),
        Arc::new(Count::default()),
        Arc::new(Count::default()),
        Arc::new(Count::default()),
    );
    let mut quitting = token.cancelled();
    let mut staying = token.cancelled();
    let mut moving = token.clone().cancelled_owned();
    assert!(poll_counting(&mut quitting, &quitter).is_pending());
    assert!(poll_counting(&mut staying, &stayer).is_pending());
    assert!(poll_counting(&mut moving, &mover).is_pending());
    // Polled again from the same task, the stayer is listed once.
    assert!(poll_counting(&mut staying, &stayer).is_pending());
    assert!(poll_counting(&mut moving, &moved).is_pending());
    assert_eq!(
        format!("{token:?}"),
        "CancellationToken { cancelled: false, waiters: 3, children: 0 }"
    );
    drop(quitting);
    assert_eq!(
        format!("{token:?}"),
        "CancellationToken { cancelled: false, waiters: 2, children: 0 }"
    );

    token.cancel();
    assert_eq!(quitter.wakes(), 0);
    assert_eq!(stayer.wakes(), 1);
    assert_eq!(mover.wakes(), 0);
    assert_eq!(moved.wakes(), 1);
    assert!(poll_counting(&mut staying, &stayer).is_ready());
    assert!(poll_counting(&mut moving, &moved).is_ready());
    drop((staying, moving));
    assert_eq!(stayer.wakes(), 1);
    assert_eq!(moved.wakes(), 1);

    // With every waiter gone, there is nobody to wake.
    let token = CancellationToken::new();
    let mut waiting = token.cancelled();
    let count = Arc::new(Count::default());
    assert!(poll_counting(&mut waiting, &count).is_pending());
    drop(waiting);
    token.cancel();
    assert_eq!(count.wakes(), 0);
}

/// A loop over waits on a cancelled token, which are ready every time,
/// yields once its budget is spent.
#[test]
fn waits_that_are_over_at_once_do_not_starve_the_other_tasks() {
    run(async {
        let token = CancellationToken::new();
        token.cancel();
        let waited = Rc::new(Cell::new(0));
        let looper = spawn({
            let (token, waited) = (token.clone(), waited.clone());
            async move {
                for _ in 0..10_000 {
                    token.cancelled().await;
                    waited.set(waited.get() + 1);
                }
            }
        });
        let seen = spawn({
            let waited = waited.clone();
            async move { waited.get() }
        })
        .await
        .unwrap();
        assert!(0 < seen && seen < 1000, "{seen} waits");
        looper.await.unwrap();
    });
}

/// A token, its waits and its guard can be sent to other threads, as a
/// future given to `spawn_on` asks, and the waits need no pinning.
#[test]
fn tokens_and_their_waits_can_be_sent() {
    fn send_sync<T: Send + Sync>() {}
    fn unpin<T: Unpin>() {}
    send_sync::<CancellationToken>();
    send_sync::<WaitForCancellation<'static>>();
    send_sync::<WaitForCancellationOwned>();
    send_sync::<DropGuard>();
    unpin::<WaitForCancellation<'static>>();
    unpin::<WaitForCancellationOwned>();
}
