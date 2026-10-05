//! Barriers: tasks on one runtime and on several, meeting at one.

use std::cell::Cell;
use std::future::poll_fn;
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use std::thread;
use std::time::Duration;

use ruio_executor::sync::Barrier;
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

/// Tasks of one runtime all pass the barrier together, once every one has
/// arrived, and exactly one of them is the leader.
#[test]
fn tasks_on_one_runtime_pass_together_with_one_leader() {
    const TASKS: usize = 8;
    run(async {
        let barrier = Arc::new(Barrier::new(TASKS));
        let arrived = Arc::new(AtomicUsize::new(0));
        let tasks: Vec<_> = (0..TASKS)
            .map(|task| {
                let (barrier, arrived) = (Arc::clone(&barrier), Arc::clone(&arrived));
                spawn(async move {
                    for _ in 0..task {
                        yield_now().await;
                    }
                    arrived.fetch_add(1, Ordering::Relaxed);
                    let result = barrier.wait().await;
                    assert_eq!(arrived.load(Ordering::Relaxed), TASKS, "all arrived first");
                    result.is_leader()
                })
            })
            .collect();
        let mut leaders = 0;
        for task in tasks {
            leaders += usize::from(task.await.unwrap());
        }
        assert_eq!(leaders, 1);
    });
}

/// Once a generation has passed, the next `n` arrivals form another, with
/// a leader of its own.
#[test]
fn a_barrier_is_reused_by_the_next_generation() {
    const TASKS: usize = 4;
    const ROUNDS: usize = 50;
    run(async {
        let barrier = Arc::new(Barrier::new(TASKS));
        let arrived = Arc::new(AtomicUsize::new(0));
        let tasks: Vec<_> = (0..TASKS)
            .map(|_| {
                let (barrier, arrived) = (Arc::clone(&barrier), Arc::clone(&arrived));
                spawn(async move {
                    let mut led = 0;
                    for round in 0..ROUNDS {
                        arrived.fetch_add(1, Ordering::Relaxed);
                        led += usize::from(barrier.wait().await.is_leader());
                        assert!(arrived.load(Ordering::Relaxed) >= (round + 1) * TASKS);
                    }
                    led
                })
            })
            .collect();
        let mut leaders = 0;
        for task in tasks {
            leaders += task.await.unwrap();
        }
        assert_eq!(leaders, ROUNDS, "one leader a round");
    });
}

/// Tasks on several threads, each with its own runtime, meet at one
/// barrier many times in a row: every round is woken across threads, and a
/// wake lost on the way stops the meeting.
#[test]
fn runtimes_on_several_threads_meet_many_times() {
    const THREADS: usize = 4;
    const ROUNDS: usize = 2000;
    let barrier = Arc::new(Barrier::new(THREADS));
    let arrived = Arc::new(AtomicUsize::new(0));
    let threads: Vec<_> = (0..THREADS)
        .map(|_| {
            let (barrier, arrived) = (Arc::clone(&barrier), Arc::clone(&arrived));
            thread::spawn(move || {
                run(async move {
                    let mut led = 0;
                    for round in 0..ROUNDS {
                        arrived.fetch_add(1, Ordering::Relaxed);
                        led += usize::from(barrier.wait().await.is_leader());
                        assert!(arrived.load(Ordering::Relaxed) >= (round + 1) * THREADS);
                    }
                    led
                })
            })
        })
        .collect();
    let leaders: usize = threads.into_iter().map(|t| t.join().unwrap()).sum();
    assert_eq!(leaders, ROUNDS, "one leader a round");
}

/// A barrier of one, or of none, lets each task through at once, as the
/// leader, and again the next time.
#[test]
fn a_barrier_of_one_or_none_lets_a_task_through_at_once() {
    run(async {
        for n in [0, 1] {
            let barrier = Barrier::new(n);
            for _ in 0..3 {
                let mut wait = pin!(barrier.wait());
                let result = poll_fn(|cx| match wait.as_mut().poll(cx) {
                    Poll::Ready(result) => Poll::Ready(result),
                    Poll::Pending => panic!("a barrier of {n} made a task wait"),
                })
                .await;
                assert!(result.is_leader());
            }
        }
    });
}

/// A wait dropped before its generation is let through takes its arrival
/// back: the barrier waits for a real last arrival rather than let the
/// others through one short.
#[test]
fn a_wait_dropped_before_release_does_not_count() {
    run(async {
        let barrier = Arc::new(Barrier::new(2));
        {
            let mut wait = pin!(barrier.wait());
            poll_fn(|cx| {
                assert!(wait.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            // Dropped here, having arrived.
        }
        let waiter = {
            let barrier = Arc::clone(&barrier);
            spawn(async move { barrier.wait().await.is_leader() })
        };
        yield_now().await;
        assert!(!waiter.is_finished(), "the dropped wait counted");
        assert!(barrier.wait().await.is_leader());
        assert!(!waiter.await.unwrap());
    });
}

/// A barrier of two whose second arrival comes from another thread, round
/// after round: each lets both through, one of them the leader.
#[test]
fn the_second_arrival_comes_from_another_thread() {
    const ROUNDS: usize = 200;
    let barrier = Arc::new(Barrier::new(2));
    let other = {
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            run(async move {
                let mut led = 0;
                for round in 0..ROUNDS {
                    if round % 2 == 0 {
                        thread::sleep(Duration::from_micros(50));
                    }
                    led += usize::from(barrier.wait().await.is_leader());
                }
                led
            })
        })
    };
    let led = run(async {
        let mut led = 0;
        for round in 0..ROUNDS {
            if round % 2 == 1 {
                thread::sleep(Duration::from_micros(50));
            }
            led += usize::from(barrier.wait().await.is_leader());
        }
        led
    });
    assert_eq!(led + other.join().unwrap(), ROUNDS, "one leader a round");
}

/// A task that always finds its barrier ready still lets the others run:
/// each wait that is ready spends a step of the task's budget, and once it
/// is spent the task yields.
#[test]
fn a_barrier_that_is_always_ready_does_not_starve_the_other_tasks() {
    const MANY: usize = 10_000;
    run(async {
        let passed = Rc::new(Cell::new(0));
        let passer = spawn({
            let passed = passed.clone();
            async move {
                let barrier = Barrier::new(1);
                for _ in 0..MANY {
                    barrier.wait().await;
                    passed.set(passed.get() + 1);
                }
            }
        });
        let seen = spawn(async move { passed.get() }).await.unwrap();
        assert!(0 < seen && seen < MANY / 10, "{seen} waits");
        passer.await.unwrap();
    });
}
