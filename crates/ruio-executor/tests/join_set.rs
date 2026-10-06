//! Join sets: tasks whose outputs come as they finish.

use std::cell::Cell;
use std::future::pending;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use ruio_executor::blocking::Pool;
use ruio_executor::time::{sleep, timeout};
use ruio_executor::{Handle, JoinSet, Runtime, spawn, yield_now};

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

/// A runtime on a thread of its own, which serves the tasks spawned through
/// its handle until it is dropped.
struct OtherRuntime {
    handle: Handle,
    /// Dropped to end the runtime's `block_on`.
    stop: Option<ruio_executor::sync::oneshot::Sender<()>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl OtherRuntime {
    fn start() -> Self {
        let (ready, started) = mpsc::channel();
        let (stop, stopped) = ruio_executor::sync::oneshot::channel::<()>();
        let thread = thread::spawn(move || {
            let runtime = Runtime::new().unwrap();
            ready.send(runtime.handle()).unwrap();
            runtime.block_on(async {
                let _ = stopped.await;
            });
        });
        Self {
            handle: started.recv().unwrap(),
            stop: Some(stop),
            thread: Some(thread),
        }
    }
}

impl Drop for OtherRuntime {
    fn drop(&mut self) {
        drop(self.stop.take());
        self.thread.take().unwrap().join().unwrap();
    }
}

/// Sets its flag when dropped, on whichever thread.
struct SendDropFlag(Arc<AtomicBool>);

impl Drop for SendDropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
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

#[test]
fn blocking_jobs_outputs_come_through_the_set() {
    run(async {
        let mut set = JoinSet::new();
        let here = thread::current().id();
        for n in 0..8u64 {
            set.spawn_blocking(move || (thread::current().id(), n));
        }
        set.spawn(async { (thread::current().id(), 100) });
        let mut sum = 0;
        let mut elsewhere = 0;
        while let Some(output) = set.join_next().await {
            let (thread, n) = output.unwrap();
            sum += n;
            if thread != here {
                elsewhere += 1;
            }
        }
        assert_eq!(sum, (0..8).sum::<u64>() + 100);
        assert_eq!(elsewhere, 8, "the jobs ran on the pool");
    });
}

#[test]
fn a_blocking_jobs_panic_is_the_tasks() {
    run(async {
        let mut set = JoinSet::new();
        set.spawn_blocking(|| -> u32 { panic!("boom") });
        let err = set.join_next().await.unwrap().unwrap_err();
        assert!(err.is_panic());
        assert_eq!(*err.into_panic().downcast::<&str>().unwrap(), "boom");
        assert!(set.join_next().await.is_none());
    });
}

/// A blocking task aborted before a thread takes its job is cancelled, and
/// the job never runs; one aborted after runs to its end, and is still
/// reported cancelled. The job is aborted as the scheduler drops the task's
/// future, so the test yields for that before it lets the pool go on.
#[test]
fn aborting_a_blocking_task_aborts_its_job() {
    run(async {
        let pool = Pool::builder().threads(1).build();
        let (release, held) = mpsc::channel::<()>();
        let (started, wait_started) = mpsc::channel::<()>();
        let mut set = JoinSet::new();
        let first = set.spawn_blocking_on(&pool, move || {
            started.send(()).unwrap();
            held.recv().unwrap_or(());
        });
        let ran = Arc::new(AtomicBool::new(false));
        let second = {
            let ran = ran.clone();
            set.spawn_blocking_on(&pool, move || ran.store(true, Ordering::Relaxed))
        };
        wait_started.recv().unwrap();
        second.abort();
        first.abort();
        yield_now().await;
        release.send(()).unwrap();
        let mut cancelled = 0;
        while let Some(result) = set.join_next().await {
            assert!(result.unwrap_err().is_cancelled());
            cancelled += 1;
        }
        assert_eq!(cancelled, 2);
        // The first job ran to its end; a thread never took the second.
        pool.spawn(|| ()).await.unwrap();
        assert!(!ran.load(Ordering::Relaxed), "an aborted job ran");
    });
}

#[test]
fn dropping_the_set_aborts_the_jobs_not_yet_taken() {
    run(async {
        let pool = Pool::builder().threads(1).build();
        let (release, held) = mpsc::channel::<()>();
        let (started, wait_started) = mpsc::channel::<()>();
        let ran = Arc::new(AtomicBool::new(false));
        let mut set = JoinSet::new();
        // Holds the one thread until released, so the second job waits.
        set.spawn_blocking_on(&pool, move || {
            started.send(()).unwrap();
            held.recv().unwrap_or(());
        });
        set.spawn_blocking_on(&pool, {
            let ran = ran.clone();
            move || ran.store(true, Ordering::Relaxed)
        });
        wait_started.recv().unwrap();
        drop(set);
        // The scheduler drops the tasks' futures, which abort the jobs.
        yield_now().await;
        release.send(()).unwrap();
        pool.spawn(|| ()).await.unwrap();
        assert!(!ran.load(Ordering::Relaxed), "a job of a dropped set ran");
    });
}

/// How many steps that are ready at once a task takes before the task
/// queued behind it runs: `steps` counts them as they are taken.
async fn steps_before_the_next_task(steps: Rc<Cell<usize>>) -> usize {
    spawn(async move { steps.get() }).await.unwrap()
}

/// Tasks spawned onto another runtime through its handle are tasks of the
/// set: their outputs come as the set's own do, beside them.
#[test]
fn a_set_spawns_onto_another_runtime() {
    let other = OtherRuntime::start();
    let home = thread::current().id();
    run(async {
        let mut set = JoinSet::new();
        for n in 0..10u32 {
            set.spawn_remote(&other.handle, async move {
                yield_now().await;
                (n, thread::current().id())
            });
        }
        set.spawn(async { (10, thread::current().id()) });
        assert_eq!(set.len(), 11);
        let mut outputs = Vec::new();
        while let Some(output) = set.join_next().await {
            outputs.push(output.unwrap());
        }
        outputs.sort_unstable_by_key(|(n, _)| *n);
        assert_eq!(
            outputs.iter().map(|(n, _)| *n).collect::<Vec<_>>(),
            (0..=10).collect::<Vec<_>>()
        );
        assert!(outputs[..10].iter().all(|(_, thread)| *thread != home));
        assert_eq!(outputs[10].1, home);
        assert!(set.is_empty());
    });
}

/// A remote task that panics or is aborted, through the handle the set
/// gives, ends with the error the set reports.
#[test]
fn a_remote_tasks_error_comes_through_the_set() {
    let other = OtherRuntime::start();
    run(async {
        let mut set = JoinSet::new();
        set.spawn_remote(&other.handle, async {
            if true {
                panic!("remote boom");
            }
            1
        });
        let abort = set.spawn_remote(&other.handle, pending::<u32>());
        abort.abort();
        let (mut panicked, mut cancelled) = (0, 0);
        while let Some(result) = set.join_next().await {
            let err = result.unwrap_err();
            if err.is_panic() {
                assert_eq!(*err.into_panic().downcast::<&str>().unwrap(), "remote boom");
                panicked += 1;
            } else {
                assert!(err.is_cancelled());
                cancelled += 1;
            }
        }
        assert_eq!((panicked, cancelled), (1, 1));
        assert!(abort.is_finished());
    });
}

/// Aborting the set's tasks, or dropping the set, aborts the remote tasks
/// too: the other runtime drops their futures.
#[test]
fn aborting_the_set_aborts_its_remote_tasks() {
    let other = OtherRuntime::start();
    let aborted = Arc::new(AtomicBool::new(false));
    let dropped_with_set = Arc::new(AtomicBool::new(false));
    run({
        let (aborted, dropped_with_set) = (aborted.clone(), dropped_with_set.clone());
        let other = &other;
        async move {
            let mut set = JoinSet::new();
            let flag = SendDropFlag(aborted);
            set.spawn_remote(&other.handle, async move {
                let _flag = flag;
                pending::<()>().await;
            });
            set.abort_all();
            assert!(set.join_next().await.unwrap().unwrap_err().is_cancelled());
            assert!(set.join_next().await.is_none());

            let flag = SendDropFlag(dropped_with_set);
            set.spawn_remote(&other.handle, async move {
                let _flag = flag;
                pending::<()>().await;
            });
            drop(set);
        }
    });
    let start = Instant::now();
    while !aborted.load(Ordering::Acquire) || !dropped_with_set.load(Ordering::Acquire) {
        assert!(
            start.elapsed() < PATIENCE,
            "the other runtime drops the futures"
        );
        thread::yield_now();
    }
    drop(other);
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
