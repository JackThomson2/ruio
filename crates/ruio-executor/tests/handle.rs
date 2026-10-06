//! Spawning onto a runtime from other threads.

use std::future::pending;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, ThreadId};

use ruio_executor::sync::oneshot;
use ruio_executor::{Runtime, yield_now};

/// Records the thread it is dropped on.
struct Witness(Arc<Mutex<Option<ThreadId>>>);

impl Drop for Witness {
    fn drop(&mut self) {
        *self.0.lock().unwrap() = Some(thread::current().id());
    }
}

/// A task spawned from another thread runs on the runtime's own, and its
/// output reaches the other thread.
#[test]
fn another_thread_spawns_onto_a_runtime() {
    let runtime = Runtime::new().unwrap();
    let handle = runtime.handle();
    let home = thread::current().id();
    let (result, received) = oneshot::channel();
    let other = thread::spawn(move || {
        let task = handle.spawn(async { (thread::current().id(), 21 * 2) });
        let output = Runtime::new().unwrap().block_on(task);
        result.send(output.unwrap()).unwrap();
    });
    assert_eq!(runtime.block_on(received).unwrap(), (home, 42));
    other.join().unwrap();
}

/// A future made on the runtime's thread, by a closure sent there, need not
/// be `Send`.
#[test]
fn a_future_made_on_the_runtime_need_not_be_send() {
    let runtime = Runtime::new().unwrap();
    let handle = runtime.handle();
    let home = thread::current().id();
    let (result, received) = oneshot::channel();
    let other = thread::spawn(move || {
        let task = handle.spawn_with(|| {
            // Not `Send`, and held across an await: made on the runtime's
            // thread, it never leaves it.
            let local = Rc::new(21);
            async move {
                yield_now().await;
                (thread::current().id(), *local * 2)
            }
        });
        let output = Runtime::new().unwrap().block_on(task);
        result.send(output.unwrap()).unwrap();
    });
    assert_eq!(runtime.block_on(received).unwrap(), (home, 42));
    other.join().unwrap();
}

/// A handle goes where it is awaited: spawned outside any runtime, awaited
/// on a third thread, while the runtime runs the task on its own.
#[test]
fn a_handle_is_awaited_wherever_it_goes() {
    let runtime = Runtime::new().unwrap();
    let handle = runtime.handle();
    let home = thread::current().id();
    let task = handle.spawn(async { thread::current().id() });
    assert!(!task.is_finished());
    let (result, received) = oneshot::channel();
    let other = thread::spawn(move || {
        let output = Runtime::new().unwrap().block_on(task);
        result.send(output.unwrap()).unwrap();
    });
    assert_eq!(runtime.block_on(received).unwrap(), home);
    other.join().unwrap();
}

/// A panic in making the future is the task's: the handle reports it, and
/// the runtime runs on.
#[test]
fn a_panic_in_making_the_future_is_the_tasks() {
    let runtime = Runtime::new().unwrap();
    let handle = runtime.handle();
    runtime.block_on(async {
        let panicked = handle.spawn_with(|| {
            if true {
                panic!("making the future panics");
            }
            async {}
        });
        let err = panicked.await.unwrap_err();
        assert!(err.is_panic());
        assert_eq!(
            err.into_panic().downcast_ref::<&str>(),
            Some(&"making the future panics")
        );
        assert_eq!(handle.spawn(async { 7 }).await.unwrap(), 7);
    });
}

/// A handle knows whether its task has finished, and can be polled by
/// reference until it has.
#[test]
fn a_handle_knows_whether_its_task_has_finished() {
    let runtime = Runtime::new().unwrap();
    let handle = runtime.handle();
    runtime.block_on(async {
        let mut task = handle.spawn(async {
            yield_now().await;
            3
        });
        assert!(!task.is_finished());
        assert_eq!((&mut task).await.unwrap(), 3);
        assert!(task.is_finished());
        let aborted = handle.spawn(pending::<()>());
        aborted.abort();
        assert!(
            !aborted.is_finished(),
            "cancelled only when the runtime reaches it"
        );
        yield_now().await;
        yield_now().await;
        assert!(aborted.is_finished());
    });
}

/// Dropping the handle detaches the task, which runs on; its output is
/// dropped on the task's thread if the handle went first, and with the
/// handle otherwise.
#[test]
fn dropping_the_handle_detaches_the_task() {
    let runtime = Runtime::new().unwrap();
    let handle = runtime.handle();
    let home = thread::current().id();
    let ran = Arc::new(AtomicUsize::new(0));
    let dropped_on = Arc::new(Mutex::new(None));
    let other = thread::spawn({
        let (ran, dropped_on) = (Arc::clone(&ran), Arc::clone(&dropped_on));
        move || {
            let task = handle.spawn(async move {
                ran.fetch_add(1, Ordering::Relaxed);
                Witness(dropped_on)
            });
            drop(task);
        }
    });
    other.join().unwrap();
    runtime.block_on(async {
        while ran.load(Ordering::Relaxed) == 0 {
            yield_now().await;
        }
    });
    assert_eq!(*dropped_on.lock().unwrap(), Some(home));

    let handle = runtime.handle();
    let dropped_on = Arc::new(Mutex::new(None));
    let task = handle.spawn({
        let dropped_on = Arc::clone(&dropped_on);
        async move { Witness(dropped_on) }
    });
    runtime.block_on(async {
        while !task.is_finished() {
            yield_now().await;
        }
    });
    assert_eq!(*dropped_on.lock().unwrap(), None);
    let other = thread::spawn(move || drop(task));
    let dropper = other.thread().id();
    other.join().unwrap();
    assert_eq!(*dropped_on.lock().unwrap(), Some(dropper));
}

/// Many threads spawn many tasks onto one runtime, and get every output.
#[test]
fn many_threads_spawn_onto_one_runtime() {
    const THREADS: usize = 4;
    const TASKS: usize = 1000;
    let runtime = Runtime::new().unwrap();
    let handle = runtime.handle();
    let (done, all_done) = oneshot::channel();
    let done = Arc::new(Mutex::new(Some(done)));
    let left = Arc::new(AtomicUsize::new(THREADS));
    let spawners: Vec<_> = (0..THREADS)
        .map(|thread| {
            let (handle, done, left) = (handle.clone(), Arc::clone(&done), Arc::clone(&left));
            thread::spawn(move || {
                let tasks: Vec<_> = (0..TASKS)
                    .map(|n| {
                        handle.spawn(async move {
                            if n % 3 == 0 {
                                yield_now().await;
                            }
                            thread * TASKS + n
                        })
                    })
                    .collect();
                let outputs = Runtime::new().unwrap().block_on(async {
                    let mut outputs = Vec::with_capacity(TASKS);
                    for task in tasks {
                        outputs.push(task.await.unwrap());
                    }
                    outputs
                });
                assert_eq!(
                    outputs,
                    (0..TASKS).map(|n| thread * TASKS + n).collect::<Vec<_>>()
                );
                if left.fetch_sub(1, Ordering::AcqRel) == 1 {
                    done.lock().unwrap().take().unwrap().send(()).unwrap();
                }
            })
        })
        .collect();
    runtime.block_on(all_done).unwrap();
    for spawner in spawners {
        spawner.join().unwrap();
    }
}

/// Aborts from another thread race the runs: every task ends, with its
/// output or cancelled, and none is lost.
#[test]
fn aborts_from_another_thread_race_the_runs() {
    const TASKS: usize = 2000;
    let runtime = Runtime::new().unwrap();
    let handle = runtime.handle();
    let (done, all_done) = oneshot::channel();
    let aborter = thread::spawn(move || {
        let tasks: Vec<_> = (0..TASKS)
            .map(|n| {
                let task = handle.spawn(async move {
                    for _ in 0..n % 4 {
                        yield_now().await;
                    }
                    n
                });
                if n % 2 == 0 {
                    task.abort();
                }
                task
            })
            .collect();
        let (mut outputs, mut cancelled) = (0, 0);
        Runtime::new().unwrap().block_on(async {
            for (n, task) in tasks.into_iter().enumerate() {
                match task.await {
                    Ok(output) => {
                        assert_eq!(output, n);
                        outputs += 1;
                    }
                    Err(err) => {
                        assert!(err.is_cancelled());
                        assert_eq!(n % 2, 0, "only aborted tasks are cancelled");
                        cancelled += 1;
                    }
                }
            }
        });
        assert_eq!(outputs + cancelled, TASKS);
        assert!(outputs >= TASKS / 2);
        done.send(()).unwrap();
    });
    runtime.block_on(all_done).unwrap();
    aborter.join().unwrap();
}

#[test]
fn a_remote_task_reports_a_panic() {
    let runtime = Runtime::new().unwrap();
    let handle = runtime.handle();
    runtime.block_on(async {
        let panicked = handle.spawn(async {
            if true {
                panic!("the task panics");
            }
        });
        assert!(panicked.await.unwrap_err().is_panic());
    });
}

/// Aborting reaches a remote task whether it has started or not.
#[test]
fn a_remote_task_can_be_aborted() {
    let runtime = Runtime::new().unwrap();
    let handle = runtime.handle();
    runtime.block_on(async {
        let started = handle.spawn(pending::<()>());
        yield_now().await;
        yield_now().await;
        started.abort();
        assert!(started.await.unwrap_err().is_cancelled());

        let unstarted = handle.spawn(pending::<()>());
        unstarted.abort();
        assert!(unstarted.await.unwrap_err().is_cancelled());
    });
}

/// An abort handle aborts the task from another thread, while the join
/// handle awaits it; clones abort the same task, and a finished task is left
/// as it is.
#[test]
fn an_abort_handle_aborts_from_another_thread() {
    let runtime = Runtime::new().unwrap();
    let handle = runtime.handle();
    let (started, has_started) = oneshot::channel();
    runtime.block_on(async {
        let task = handle.spawn(async move {
            started.send(()).unwrap();
            pending::<()>().await;
        });
        let abort = task.abort_handle();
        assert!(!abort.is_finished());
        has_started.await.unwrap();
        let aborter = thread::spawn(move || {
            let clone = abort.clone();
            clone.abort();
            abort
        });
        assert!(task.await.unwrap_err().is_cancelled());
        let abort = aborter.join().unwrap();
        assert!(abort.is_finished());

        let mut task = handle.spawn(async { 5 });
        let abort = task.abort_handle();
        assert_eq!((&mut task).await.unwrap(), 5);
        abort.abort();
        assert!(abort.is_finished());
        drop(task);
        // The handle keeps the task's memory, and still answers.
        assert!(abort.is_finished());
    });
}

/// Dropping every abort handle leaves the task to run and its join handle
/// to get the output.
#[test]
fn dropping_an_abort_handle_leaves_the_task_alone() {
    let runtime = Runtime::new().unwrap();
    let handle = runtime.handle();
    runtime.block_on(async {
        let task = handle.spawn(async {
            yield_now().await;
            8
        });
        drop(task.abort_handle());
        assert_eq!(task.await.unwrap(), 8);
    });
}

/// A runtime that shuts down drops the spawns it has not run, and refuses
/// those that come after: their handles report them cancelled.
#[test]
fn a_runtime_that_shuts_down_cancels_what_it_did_not_run() {
    let runtime = Runtime::new().unwrap();
    let handle = runtime.handle();
    let unrun = handle.spawn(async { 1 });
    drop(runtime);
    let late = handle.spawn(async { 2 });
    let other = Runtime::new().unwrap();
    assert!(other.block_on(unrun).unwrap_err().is_cancelled());
    assert!(other.block_on(late).unwrap_err().is_cancelled());
}
