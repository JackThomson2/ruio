//! Spawning onto a runtime from other threads.

use std::future::pending;
use std::rc::Rc;
use std::thread;

use ruio_executor::sync::oneshot;
use ruio_executor::{Runtime, yield_now};

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
