//! Abort handles: aborting a task without its join handle.

use std::cell::Cell;
use std::future::pending;
use std::rc::Rc;

use ruio_executor::{AbortHandle, Runtime, spawn, yield_now};

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
fn an_abort_handle_cancels_a_running_task() {
    let runtime = Runtime::new().unwrap();
    let (flag, dropped) = drop_flag();
    runtime.block_on(async move {
        let task = spawn(async move {
            let _flag = flag;
            pending::<()>().await;
        });
        let handle = task.abort_handle();
        yield_now().await;
        assert!(!handle.is_finished());
        handle.abort();
        let err = task.await.unwrap_err();
        assert!(err.is_cancelled());
        assert!(handle.is_finished());
        assert!(
            dropped.get(),
            "the future is dropped as the task is cancelled"
        );
    });
}

#[test]
fn an_abort_before_the_first_poll_drops_the_future_unpolled() {
    let runtime = Runtime::new().unwrap();
    let polled = Rc::new(Cell::new(false));
    runtime.block_on(async {
        let task = {
            let polled = polled.clone();
            spawn(async move {
                polled.set(true);
            })
        };
        task.abort_handle().abort();
        assert!(task.await.unwrap_err().is_cancelled());
    });
    assert!(!polled.get());
}

#[test]
fn dropping_an_abort_handle_leaves_the_task_running() {
    let runtime = Runtime::new().unwrap();
    runtime.block_on(async {
        let task = spawn(async {
            yield_now().await;
            7
        });
        drop(task.abort_handle());
        assert_eq!(task.await.unwrap(), 7);
    });
}

#[test]
fn clones_abort_the_same_task() {
    let runtime = Runtime::new().unwrap();
    runtime.block_on(async {
        let task = spawn(pending::<()>());
        let handle = task.abort_handle();
        let clone = handle.clone();
        drop(handle);
        clone.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(clone.is_finished());
    });
}

#[test]
fn an_abort_handle_outlives_the_join_handle() {
    let runtime = Runtime::new().unwrap();
    let (flag, dropped) = drop_flag();
    runtime.block_on(async move {
        let task = spawn(async move {
            let _flag = flag;
            pending::<()>().await;
        });
        let handle = task.abort_handle();
        // Detached: the task runs on, with nobody to take its output.
        drop(task);
        yield_now().await;
        assert!(!handle.is_finished());
        handle.abort();
        while !handle.is_finished() {
            yield_now().await;
        }
        assert!(dropped.get());
    });
}

#[test]
fn aborting_a_finished_task_does_nothing() {
    let runtime = Runtime::new().unwrap();
    runtime.block_on(async {
        let task = spawn(async { "done" });
        let handle = task.abort_handle();
        while !handle.is_finished() {
            yield_now().await;
        }
        handle.abort();
        assert_eq!(task.await.unwrap(), "done");
    });
}

#[test]
fn an_abort_handle_can_be_kept_past_the_task() {
    let runtime = Runtime::new().unwrap();
    let handle: AbortHandle = runtime.block_on(async {
        let task = spawn(async { 1 });
        let handle = task.abort_handle();
        assert_eq!(task.await.unwrap(), 1);
        handle
    });
    assert!(handle.is_finished());
    assert!(format!("{handle:?}").contains("finished: true"));
    handle.abort();
    drop(handle);
}
