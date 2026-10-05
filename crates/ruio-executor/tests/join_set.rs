//! Join sets: tasks whose outputs come as they finish.

use std::cell::Cell;
use std::future::pending;
use std::rc::Rc;
use std::time::Duration;

use ruio_executor::time::{sleep, timeout};
use ruio_executor::{JoinSet, Runtime, spawn, yield_now};

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

/// Sets its flag when dropped.
struct DropFlag(Rc<Cell<bool>>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

fn drop_flag() -> (DropFlag, Rc<Cell<bool>>) {
    let flag = Rc::new(Cell::new(false));
    (DropFlag(flag.clone()), flag)
}

#[test]
fn outputs_come_in_the_order_the_tasks_finish() {
    run(async {
        let mut set = JoinSet::new();
        for (n, millis) in [(1, 30), (2, 10), (3, 20)] {
            set.spawn(async move {
                sleep(Duration::from_millis(millis)).await;
                n
            });
        }
        assert_eq!(set.len(), 3);
        let mut outputs = Vec::new();
        while let Some(output) = set.join_next().await {
            outputs.push(output.unwrap());
        }
        assert_eq!(outputs, [2, 3, 1]);
        assert!(set.is_empty());
    });
}

#[test]
fn an_empty_set_has_nothing_to_join() {
    run(async {
        let mut set: JoinSet<u32> = JoinSet::new();
        assert!(set.is_empty());
        assert!(set.join_next().await.is_none());
        assert!(set.try_join_next().is_none());
        set.spawn(async { 1 });
        assert_eq!(set.join_next().await.unwrap().unwrap(), 1);
        assert!(set.join_next().await.is_none());
    });
}

#[test]
fn tasks_that_finish_before_the_set_is_polled_are_kept() {
    run(async {
        let mut set = JoinSet::new();
        for n in 0..5 {
            set.spawn(async move { n });
        }
        for _ in 0..3 {
            yield_now().await;
        }
        let mut outputs = Vec::new();
        while let Some(output) = set.try_join_next() {
            outputs.push(output.unwrap());
        }
        outputs.sort_unstable();
        assert_eq!(outputs, [0, 1, 2, 3, 4]);
    });
}

#[test]
fn an_aborted_task_reports_that_it_was_cancelled() {
    run(async {
        let mut set = JoinSet::new();
        let (flag, dropped) = drop_flag();
        let handle = set.spawn(async move {
            let _flag = flag;
            pending::<u32>().await
        });
        yield_now().await;
        assert!(!handle.is_finished());
        handle.abort();
        let err = set.join_next().await.unwrap().unwrap_err();
        assert!(err.is_cancelled());
        assert!(dropped.get());
        assert!(handle.is_finished());
        assert!(set.join_next().await.is_none());
    });
}

#[test]
fn a_panicking_task_reports_its_panic() {
    run(async {
        let mut set = JoinSet::new();
        set.spawn(async { panic!("boom") });
        set.spawn(async { 2 });
        let mut panics = 0;
        let mut outputs = 0;
        while let Some(result) = set.join_next().await {
            match result {
                Ok(2) => outputs += 1,
                Ok(other) => panic!("unexpected output {other}"),
                Err(err) => {
                    assert!(err.is_panic());
                    assert_eq!(*err.into_panic().downcast::<&str>().unwrap(), "boom");
                    panics += 1;
                }
            }
        }
        assert_eq!((panics, outputs), (1, 1));
    });
}

#[test]
fn abort_all_cancels_every_task() {
    run(async {
        let mut set = JoinSet::new();
        for _ in 0..10 {
            set.spawn(pending::<()>());
        }
        set.abort_all();
        let mut cancelled = 0;
        while let Some(result) = set.join_next().await {
            assert!(result.unwrap_err().is_cancelled());
            cancelled += 1;
        }
        assert_eq!(cancelled, 10);
        assert!(set.is_empty());
    });
}

#[test]
fn shutdown_empties_the_set() {
    run(async {
        let mut set = JoinSet::new();
        set.spawn(pending());
        set.spawn(async { 1 });
        set.spawn(async {
            sleep(Duration::from_millis(5)).await;
            2
        });
        set.shutdown().await;
        assert!(set.is_empty());
        assert!(set.join_next().await.is_none());
    });
}

#[test]
fn dropping_the_set_aborts_its_tasks_and_drops_their_outputs() {
    let (pending_flag, pending_dropped) = drop_flag();
    let (output_flag, output_dropped) = drop_flag();
    run(async move {
        let mut set = JoinSet::new();
        set.spawn(async move {
            let _flag = pending_flag;
            pending::<Option<DropFlag>>().await
        });
        set.spawn(async move { Some(output_flag) });
        yield_now().await;
        assert!(!output_dropped.get(), "the output waits in the set");
        drop(set);
        assert!(output_dropped.get(), "the set drops the outputs it holds");
        yield_now().await;
        assert!(pending_dropped.get(), "the set aborts the tasks it holds");
    });
}

#[test]
fn detach_all_leaves_the_tasks_running() {
    run(async {
        let mut set = JoinSet::new();
        let ran = Rc::new(Cell::new(0));
        for _ in 0..3 {
            let ran = ran.clone();
            set.spawn(async move {
                yield_now().await;
                ran.set(ran.get() + 1);
                0
            });
        }
        set.detach_all();
        assert!(set.is_empty());
        while ran.get() < 3 {
            yield_now().await;
        }
        // The set is still good for more.
        set.spawn(async { 7 });
        assert_eq!(set.join_next().await.unwrap().unwrap(), 7);
    });
}

#[test]
fn try_join_next_takes_only_what_has_finished() {
    run(async {
        let mut set = JoinSet::new();
        let stuck = set.spawn(pending::<u32>());
        set.spawn(async { 1 });
        yield_now().await;
        assert_eq!(set.try_join_next().unwrap().unwrap(), 1);
        assert!(set.try_join_next().is_none());
        assert_eq!(set.len(), 1);
        stuck.abort();
        yield_now().await;
        assert!(set.try_join_next().unwrap().unwrap_err().is_cancelled());
        assert!(set.try_join_next().is_none());
    });
}

#[test]
fn join_all_collects_every_output() {
    run(async {
        let mut set = JoinSet::new();
        for n in 0..100 {
            set.spawn(async move {
                for _ in 0..(n % 7) {
                    yield_now().await;
                }
                n
            });
        }
        let mut outputs = set.join_all().await;
        outputs.sort_unstable();
        assert_eq!(outputs, (0..100).collect::<Vec<_>>());
    });
}

#[test]
#[should_panic(expected = "boom")]
fn join_all_resumes_a_tasks_panic() {
    run(async {
        let mut set = JoinSet::new();
        set.spawn(async { 1 });
        set.spawn(async { panic!("boom") });
        set.spawn(pending());
        set.join_all().await;
    });
}

#[test]
fn slots_are_reused_as_tasks_come_and_go() {
    run(async {
        let mut set = JoinSet::new();
        let mut total = 0;
        for round in 0..10_000u64 {
            set.spawn(async move { round });
            if round % 3 == 0 {
                set.spawn(async move { round });
            }
            total += set.join_next().await.unwrap().unwrap();
        }
        while let Some(output) = set.join_next().await {
            total += output.unwrap();
        }
        assert_eq!(
            total,
            (0..10_000u64).sum::<u64>() + (0..10_000u64).step_by(3).sum::<u64>()
        );
        assert!(set.is_empty());
        assert_eq!(format!("{set:?}"), "JoinSet { len: 0 }");
    });
}

#[test]
fn many_tasks_finishing_on_timers_all_report() {
    run(async {
        let mut set = JoinSet::new();
        for n in 0..1000u64 {
            set.spawn(async move {
                sleep(Duration::from_millis(n % 20)).await;
                n
            });
        }
        let mut sum = 0;
        while let Some(output) = set.join_next().await {
            sum += output.unwrap();
        }
        assert_eq!(sum, (0..1000u64).sum::<u64>());
    });
}

/// How many steps that are ready at once a task takes before the task
/// queued behind it runs: `steps` counts them as they are taken.
async fn steps_before_the_next_task(steps: Rc<Cell<usize>>) -> usize {
    spawn(async move { steps.get() }).await.unwrap()
}

/// A task that always finds a finished task in its set still lets the
/// others run: what is ready spends the task's budget, and once it is spent
/// the task yields.
#[test]
fn what_always_has_a_task_finished_does_not_starve_the_other_tasks() {
    const MANY: usize = 10_000;
    Runtime::new().unwrap().block_on(async {
        let joins = Rc::new(Cell::new(0));
        let ready = Rc::new(Cell::new(false));
        let go = Rc::new(Cell::new(false));
        let joiner = spawn({
            let (joins, ready, go) = (joins.clone(), ready.clone(), go.clone());
            async move {
                let mut set = JoinSet::new();
                let tasks: Vec<_> = (0..MANY).map(|_| set.spawn(async {})).collect();
                while !tasks.iter().all(|task| task.is_finished()) {
                    yield_now().await;
                }
                ready.set(true);
                while !go.get() {
                    yield_now().await;
                }
                // Every join is ready at once from here.
                while set.join_next().await.is_some() {
                    joins.set(joins.get() + 1);
                }
            }
        });
        while !ready.get() {
            yield_now().await;
        }
        // The joiner waits in the run queue; the task that reads the count
        // goes behind it, and runs once the joiner has spent its budget.
        go.set(true);
        let seen = steps_before_the_next_task(joins.clone()).await;
        assert!(0 < seen && seen < MANY / 10, "{seen} joins");
        joiner.await.unwrap();
        assert_eq!(joins.get(), MANY);
    });
}
