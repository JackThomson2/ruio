//! The executor: running, spawning, joining, waking and shutting down.

use std::cell::{Cell, RefCell};
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::{Duration, Instant};

use ruio_driver::ops::{Nop, Timeout};
use ruio_executor::{Runtime, spawn, yield_now};

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

/// Checks that it is dropped on the thread that created it.
struct ThreadBound(thread::ThreadId);

impl ThreadBound {
    fn new() -> Self {
        Self(thread::current().id())
    }
}

impl Drop for ThreadBound {
    fn drop(&mut self) {
        assert_eq!(thread::current().id(), self.0, "dropped on another thread");
    }
}

/// A one-shot channel whose sender may live on another thread.
#[derive(Default)]
struct Slot {
    value: Mutex<(Option<u32>, Option<Waker>)>,
}

impl Slot {
    fn send(&self, value: u32) {
        let waker = {
            let mut slot = self.value.lock().unwrap();
            slot.0 = Some(value);
            slot.1.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn recv(&self) -> impl Future<Output = u32> + '_ {
        poll_fn(|cx| {
            let mut slot = self.value.lock().unwrap();
            match slot.0.take() {
                Some(value) => Poll::Ready(value),
                None => {
                    slot.1 = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        })
    }
}

#[test]
fn block_on_returns_the_output() {
    let runtime = Runtime::new().unwrap();
    assert_eq!(runtime.block_on(async { 7 }), 7);
    runtime.block_on(runtime.driver().op(Nop)).unwrap();
}

#[test]
fn spawned_tasks_run_and_are_joined() {
    let runtime = Runtime::new().unwrap();
    let total = runtime.block_on(async {
        let tasks: Vec<_> = (0..10u32)
            .map(|n| {
                spawn(async move {
                    ruio_executor::driver().op(Nop).await.unwrap();
                    n
                })
            })
            .collect();
        let mut total = 0;
        for task in tasks {
            total += task.await.unwrap();
        }
        total
    });
    assert_eq!(total, 45);
}

#[test]
fn tasks_need_not_be_send() {
    let runtime = Runtime::new().unwrap();
    let shared = Rc::new(RefCell::new(Vec::new()));
    runtime.block_on(async {
        let log = shared.clone();
        spawn(async move { log.borrow_mut().push("task") })
            .await
            .unwrap();
    });
    assert_eq!(*shared.borrow(), ["task"]);
}

#[test]
fn tasks_wait_on_io_concurrently() {
    let runtime = Runtime::new().unwrap();
    let start = Instant::now();
    runtime.block_on(async {
        let tasks: Vec<_> = (0..20)
            .map(|_| {
                spawn(async {
                    let timeout = Timeout::new(Duration::from_millis(50));
                    ruio_executor::driver().op(timeout).await.unwrap();
                })
            })
            .collect();
        for task in tasks {
            task.await.unwrap();
        }
    });
    let elapsed = start.elapsed();
    assert!(elapsed >= Duration::from_millis(50));
    assert!(elapsed < Duration::from_millis(500), "{elapsed:?}");
}

#[test]
fn detached_tasks_keep_running() {
    let runtime = Runtime::new().unwrap();
    let done = Rc::new(Cell::new(false));
    runtime.block_on(async {
        let flag = done.clone();
        drop(spawn(async move {
            ruio_executor::driver().op(Nop).await.unwrap();
            flag.set(true);
        }));
        while !done.get() {
            yield_now().await;
        }
    });
}

#[test]
fn a_panicking_task_is_reported_and_the_others_run_on() {
    let runtime = Runtime::new().unwrap();
    runtime.block_on(async {
        let bad = spawn(async { panic!("boom") });
        let good = spawn(async { 1 });
        let err = bad.await.unwrap_err();
        assert!(err.is_panic());
        assert_eq!(err.to_string(), "task panicked: boom");
        assert_eq!(*err.into_panic().downcast::<&str>().unwrap(), "boom");
        assert_eq!(good.await.unwrap(), 1);
    });
}

#[test]
fn an_aborted_task_is_cancelled_and_its_future_dropped() {
    let runtime = Runtime::new().unwrap();
    let (flag, dropped) = drop_flag();
    runtime.block_on(async move {
        let task = spawn(async move {
            let _flag = flag;
            let timeout = Timeout::new(Duration::from_secs(60));
            ruio_executor::driver().op(timeout).await.unwrap();
        });
        yield_now().await;
        task.abort();
        let err = task.await.unwrap_err();
        assert!(err.is_cancelled());
    });
    assert!(dropped.get());
}

#[test]
fn outputs_are_dropped_by_the_handle_or_on_completion() {
    let runtime = Runtime::new().unwrap();
    let (kept, kept_dropped) = drop_flag();
    let (detached, detached_dropped) = drop_flag();
    runtime.block_on(async move {
        let task = spawn(async move { kept });
        drop(spawn(async move { detached }));
        while !task.is_finished() {
            yield_now().await;
        }
        assert!(!kept_dropped.get(), "the output belongs to the handle");
        drop(task);
        assert!(kept_dropped.get());
        yield_now().await;
    });
    assert!(detached_dropped.get());
}

#[test]
fn another_thread_can_wake_the_main_future() {
    let runtime = Runtime::new().unwrap();
    let slot = Arc::new(Slot::default());
    let sender = slot.clone();
    let thread = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        sender.send(9);
    });
    assert_eq!(runtime.block_on(slot.recv()), 9);
    thread.join().unwrap();
}

#[test]
fn another_thread_can_wake_a_task() {
    let runtime = Runtime::new().unwrap();
    let slot = Arc::new(Slot::default());
    let sender = slot.clone();
    let value = runtime.block_on(async move {
        let task = spawn(async move { slot.recv().await });
        let thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            sender.send(11);
        });
        let value = task.await.unwrap();
        thread.join().unwrap();
        value
    });
    assert_eq!(value, 11);
}

#[test]
fn yielding_lets_other_tasks_run() {
    let runtime = Runtime::new().unwrap();
    let log = Rc::new(RefCell::new(String::new()));
    runtime.block_on(async {
        let tasks: Vec<_> = ["a", "b"]
            .into_iter()
            .map(|name| {
                let log = log.clone();
                spawn(async move {
                    for _ in 0..3 {
                        log.borrow_mut().push_str(name);
                        yield_now().await;
                    }
                })
            })
            .collect();
        for task in tasks {
            task.await.unwrap();
        }
    });
    assert_eq!(*log.borrow(), "ababab");
}

#[test]
fn io_completes_while_tasks_keep_yielding() {
    let runtime = Runtime::new().unwrap();
    runtime.block_on(async {
        let spinners: Vec<_> = (0..200)
            .map(|_| {
                spawn(async {
                    loop {
                        yield_now().await;
                    }
                })
            })
            .collect();
        let timeout = Timeout::new(Duration::from_millis(10));
        ruio_executor::driver().op(timeout).await.unwrap();
        for spinner in &spinners {
            spinner.abort();
        }
    });
}

#[test]
fn many_tasks() {
    let runtime = Runtime::new().unwrap();
    runtime.block_on(async {
        let tasks: Vec<_> = (0..10_000)
            .map(|_| spawn(async { ruio_executor::driver().op(Nop).await.unwrap() }))
            .collect();
        for task in tasks {
            task.await.unwrap();
        }
    });
}

#[test]
fn dropping_the_runtime_cancels_unfinished_tasks() {
    let runtime = Runtime::new().unwrap();
    let (flag, dropped) = drop_flag();
    let mut task = runtime.spawn(async move {
        let _flag = flag;
        let timeout = Timeout::new(Duration::from_secs(60));
        ruio_executor::driver().op(timeout).await.unwrap();
    });
    runtime.block_on(yield_now());
    assert!(!dropped.get());

    let start = Instant::now();
    drop(runtime);
    assert!(dropped.get());
    assert!(start.elapsed() < Duration::from_secs(1));
    let mut cx = Context::from_waker(Waker::noop());
    match Pin::new(&mut task).poll(&mut cx) {
        Poll::Ready(Err(err)) => assert!(err.is_cancelled()),
        other => panic!("{other:?}"),
    }
}

/// The last reference to the task is its waker, released on another thread
/// after the runtime is gone. The future must have been dropped on the
/// task's own thread, at shutdown.
#[test]
fn wakes_after_the_runtime_is_gone_are_harmless() {
    let slot = Arc::new(Slot::default());
    let runtime = Runtime::new().unwrap();
    let receiver = slot.clone();
    drop(runtime.spawn(async move {
        let _bound = ThreadBound::new();
        receiver.recv().await
    }));
    runtime.block_on(yield_now());
    drop(runtime);
    thread::spawn(move || slot.send(1)).join().unwrap();
}

#[test]
#[should_panic(expected = "inside another one")]
fn block_on_inside_block_on_panics() {
    let runtime = Runtime::new().unwrap();
    runtime.block_on(async { Runtime::new().unwrap().block_on(async {}) });
}

#[test]
#[should_panic(expected = "inside a ruio runtime")]
fn spawn_outside_a_runtime_panics() {
    drop(spawn(async {}));
}
