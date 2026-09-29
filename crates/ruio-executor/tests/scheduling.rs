//! The order ready tasks run in.

use std::cell::{Cell, RefCell};
use std::future::poll_fn;
use std::rc::Rc;
use std::task::{Poll, Waker};

use ruio_executor::{Runtime, spawn, yield_now};

/// A flag one task sets and another waits for, on the same thread.
#[derive(Default)]
struct Signal {
    set: Cell<bool>,
    waker: Cell<Option<Waker>>,
}

impl Signal {
    fn notify(&self) {
        self.set.set(true);
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    async fn wait(&self) {
        poll_fn(|cx| {
            if self.set.replace(false) {
                Poll::Ready(())
            } else {
                self.waker.set(Some(cx.waker().clone()));
                Poll::Pending
            }
        })
        .await;
    }
}

#[test]
fn a_task_woken_by_the_running_task_runs_next() {
    let runtime = Runtime::new().unwrap();
    let log = Rc::new(RefCell::new(Vec::new()));
    let signal = Rc::new(Signal::default());
    let record = |name| {
        let log = log.clone();
        move || log.borrow_mut().push(name)
    };
    runtime.block_on(async {
        let waiter = {
            let (signal, record) = (signal.clone(), record("waiter"));
            spawn(async move {
                signal.wait().await;
                record();
            })
        };
        yield_now().await;
        // Queued in this order; the first wakes the waiter.
        let waker = {
            let (signal, record) = (signal.clone(), record("waker"));
            spawn(async move {
                record();
                signal.notify();
            })
        };
        let first = spawn({
            let record = record("first");
            async move { record() }
        });
        let second = spawn({
            let record = record("second");
            async move { record() }
        });
        for task in [waiter, waker, first, second] {
            task.await.unwrap();
        }
    });
    assert_eq!(*log.borrow(), ["waker", "waiter", "first", "second"]);
}

#[test]
fn tasks_waking_each_other_do_not_starve_the_queue() {
    let runtime = Runtime::new().unwrap();
    let ping = Rc::new(Signal::default());
    let pong = Rc::new(Signal::default());
    let exchanges = Rc::new(Cell::new(0));
    let seen = Rc::new(Cell::new(None));
    runtime.block_on(async {
        let pinger = {
            let (ping, pong, exchanges) = (ping.clone(), pong.clone(), exchanges.clone());
            spawn(async move {
                for _ in 0..100 {
                    pong.notify();
                    ping.wait().await;
                    exchanges.set(exchanges.get() + 1);
                }
            })
        };
        let ponger = {
            let (ping, pong) = (ping.clone(), pong.clone());
            spawn(async move {
                for _ in 0..100 {
                    pong.wait().await;
                    ping.notify();
                }
            })
        };
        // Queued behind the two, which would keep the LIFO slot to
        // themselves without a limit.
        let bystander = {
            let (exchanges, seen) = (exchanges.clone(), seen.clone());
            spawn(async move { seen.set(Some(exchanges.get())) })
        };
        for task in [pinger, ponger, bystander] {
            task.await.unwrap();
        }
    });
    assert_eq!(exchanges.get(), 100);
    let seen = seen.get().expect("the bystander ran");
    assert!(seen < 10, "the bystander waited for {seen} exchanges");
}
