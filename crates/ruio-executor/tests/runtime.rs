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

#[test]
fn the_builder_sizes_the_file_table() {
    let runtime = Runtime::builder().files(3).build().unwrap();
    assert_eq!(runtime.driver().files(), 3);
    let runtime = Runtime::builder().files(0).build().unwrap();
    assert_eq!(runtime.driver().files(), 0);
}

/// The shutdown timeout reaches the driver's builder, and a runtime built
/// with a short one still drains cleanly: its drop cancels what is in
/// flight, and the kernel finishes with it well within the bound.
#[test]
fn the_builder_sets_the_shutdown_timeout() {
    let builder = Runtime::builder().shutdown_timeout(Duration::from_millis(10));
    assert!(format!("{builder:?}").contains("shutdown_timeout: 10ms"));
    let runtime = builder.build().unwrap();
    runtime.block_on(async {
        // Left in flight for the drop to cancel.
        drop(spawn(async {
            let timeout = Timeout::new(Duration::from_secs(60));
            ruio_executor::driver().op(timeout).await.unwrap();
        }));
        yield_now().await;
    });
    let start = Instant::now();
    drop(runtime);
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "{:?}",
        start.elapsed()
    );
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

/// Records, when dropped, the ID of the driver the thread finds then.
struct FindsDriver(Rc<Cell<Option<u64>>>);

impl Drop for FindsDriver {
    fn drop(&mut self) {
        self.0
            .set(ruio_executor::try_driver().map(|driver| driver.id()));
    }
}

/// Code inside a runtime finds its driver, destructors that the runtime
/// runs as it drops unfinished tasks included, and code outside finds none.
#[test]
fn the_thread_finds_its_runtimes_driver_only_inside_it() {
    assert!(ruio_executor::try_driver().is_none());
    let runtime = Runtime::new().unwrap();
    let id = runtime.driver().id();
    assert!(ruio_executor::try_driver().is_none());
    let inside = runtime.block_on(async { ruio_executor::try_driver().map(|driver| driver.id()) });
    assert_eq!(inside, Some(id));

    let found = Rc::new(Cell::new(None));
    let finds = FindsDriver(found.clone());
    drop(runtime.spawn(async move {
        let _finds = finds;
        std::future::pending::<()>().await;
    }));
    runtime.block_on(yield_now());
    drop(runtime);
    assert_eq!(found.get(), Some(id));
    assert!(ruio_executor::try_driver().is_none());
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

/// A task's waker can reach the ring of another thread's runtime, sent there
/// and used to poll an operation. That ring's driver holds it by a clone,
/// since the task's count of references from its own thread is not another
/// thread's to touch, and the operation's completion wakes the task from
/// there.
#[test]
fn a_waker_polled_on_another_threads_ring_wakes_its_task() {
    let runtime = Runtime::new().unwrap();
    let joined = runtime.block_on(async {
        let (to_other, from_task) = std::sync::mpsc::channel::<Waker>();
        let other = thread::spawn(move || {
            let waker = from_task.recv().unwrap();
            Runtime::new().unwrap().block_on(async move {
                let mut op = ruio_executor::driver().op(Nop);
                let mut cx = Context::from_waker(&waker);
                // The ring completes the no-op as its runtime turns it,
                // which the yields make it do.
                while Pin::new(&mut op).poll(&mut cx).is_pending() {
                    yield_now().await;
                }
            });
        });
        let task = spawn(async move {
            let mut sent = false;
            poll_fn(|cx| {
                if sent {
                    return Poll::Ready(());
                }
                sent = true;
                to_other.send(cx.waker().clone()).unwrap();
                Poll::Pending
            })
            .await;
        });
        let joined = ruio_executor::time::timeout(Duration::from_secs(10), task).await;
        other.join().unwrap();
        joined
    });
    joined.expect("the task was not woken").unwrap();
}

/// Another thread waking a task over and over, while the task waits, runs
/// and yields in turn, loses no wake: each signal the task waits for, it
/// sees, whether the wake lands while it waits, while it runs, or while it
/// is queued by a wake of its own thread.
#[test]
fn wakes_from_another_thread_are_never_lost() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    const ROUNDS: usize = 20_000;
    let signalled = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(AtomicUsize::new(0));
    let slot: Arc<Mutex<Option<Waker>>> = Arc::new(Mutex::new(None));
    // Set when the waiter gives up, so that the signaller does too.
    let stop = Arc::new(AtomicBool::new(false));
    let signaller = {
        let (signalled, seen, slot, stop) =
            (signalled.clone(), seen.clone(), slot.clone(), stop.clone());
        thread::spawn(move || {
            for round in 1..=ROUNDS {
                while seen.load(Ordering::Acquire) < round - 1 {
                    if stop.load(Ordering::Relaxed) {
                        return;
                    }
                    std::hint::spin_loop();
                }
                signalled.store(round, Ordering::Release);
                if let Some(waker) = slot.lock().unwrap().take() {
                    waker.wake();
                }
            }
        })
    };
    let runtime = Runtime::new().unwrap();
    let waiter = runtime.block_on(async move {
        let task = spawn(async move {
            for round in 1..=ROUNDS {
                poll_fn(|cx| {
                    if signalled.load(Ordering::Acquire) >= round {
                        return Poll::Ready(());
                    }
                    *slot.lock().unwrap() = Some(cx.waker().clone());
                    // Checked again after the waker is in place, as a
                    // channel does.
                    if signalled.load(Ordering::Acquire) >= round {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                })
                .await;
                seen.store(round, Ordering::Release);
                if round % 7 == 0 {
                    yield_now().await;
                }
            }
        });
        ruio_executor::time::timeout(Duration::from_secs(10), task).await
    });
    stop.store(true, Ordering::Relaxed);
    signaller.join().unwrap();
    waiter.expect("a wake was lost").unwrap();
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

/// Join handles can outlive their runtime: the tasks, one finished and one
/// cancelled at shutdown, are freed as the handles go, after the scheduler,
/// and only then let go of the runtime's shared part.
#[test]
fn join_handles_can_outlive_their_runtime() {
    let runtime = Runtime::new().unwrap();
    let pending = runtime.spawn(std::future::pending::<()>());
    let done = runtime.spawn(async { 7 });
    runtime.block_on(yield_now());
    assert!(done.is_finished());
    drop(runtime);
    drop((pending, done));
}

/// A task whose waker outlives its local references, here on its own
/// thread, is freed when the waker's reference goes, through the remote
/// queue of a runtime that has not shut down yet.
#[test]
fn a_task_outlived_by_its_waker_is_freed_after_it() {
    let runtime = Runtime::new().unwrap();
    let kept: Rc<RefCell<Option<Waker>>> = Rc::default();
    let task_kept = kept.clone();
    runtime.block_on(async move {
        let task = spawn(poll_fn(move |cx| {
            *task_kept.borrow_mut() = Some(cx.waker().clone());
            Poll::Ready(())
        }));
        task.await.unwrap();
    });
    let waker = kept.borrow_mut().take().expect("the task kept its waker");
    // Outside the runtime, so through its remote queue, which holds the
    // task's last reference until the runtime takes it or shuts down.
    waker.wake_by_ref();
    drop(waker);
    runtime.block_on(yield_now());
    drop(runtime);
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
