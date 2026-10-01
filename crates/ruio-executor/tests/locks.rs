//! Semaphores, mutexes, reader-writer locks and notifications, on one
//! runtime and across threads.

use std::cell::{Cell, RefCell};
use std::future::{Future, poll_fn};
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use std::thread;

use ruio_executor::sync::{Mutex, MutexGuard, Notify, RwLock, Semaphore};
use ruio_executor::{Runtime, spawn, yield_now};

/// Polls `future` once, inside a task.
async fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    let mut future = Some(future);
    poll_fn(|cx| Poll::Ready(future.take().expect("polled once").poll(cx))).await
}

/// Counts how many hold something at once, and the most there were.
#[derive(Default)]
struct Occupancy {
    now: AtomicUsize,
    most: AtomicUsize,
}

impl Occupancy {
    fn enter(&self) {
        let now = self.now.fetch_add(1, Ordering::SeqCst) + 1;
        self.most.fetch_max(now, Ordering::SeqCst);
    }

    fn leave(&self) {
        self.now.fetch_sub(1, Ordering::SeqCst);
    }

    fn most(&self) -> usize {
        self.most.load(Ordering::SeqCst)
    }
}

#[test]
fn a_semaphore_bounds_how_many_hold_permits() {
    Runtime::new().unwrap().block_on(async {
        let semaphore = Arc::new(Semaphore::new(3));
        let occupancy = Arc::new(Occupancy::default());
        let tasks: Vec<_> = (0..20)
            .map(|_| {
                let (semaphore, occupancy) = (semaphore.clone(), occupancy.clone());
                spawn(async move {
                    let _permit = semaphore.acquire().await;
                    occupancy.enter();
                    for _ in 0..3 {
                        yield_now().await;
                    }
                    occupancy.leave();
                })
            })
            .collect();
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(occupancy.most(), 3);
        assert_eq!(semaphore.available_permits(), 3);
    });
}

/// A waiter that needs more permits than are free holds up those behind
/// it that need fewer, and is served first.
#[test]
fn a_semaphore_serves_its_waiters_in_order() {
    Runtime::new().unwrap().block_on(async {
        let semaphore = Rc::new(Semaphore::new(2));
        let log = Rc::new(RefCell::new(Vec::new()));
        let first = semaphore.try_acquire().unwrap();
        let second = semaphore.try_acquire().unwrap();
        let many = spawn({
            let (semaphore, log) = (semaphore.clone(), log.clone());
            async move {
                let _permits = semaphore.acquire_many(2).await;
                log.borrow_mut().push("two");
            }
        });
        yield_now().await;
        let one = spawn({
            let (semaphore, log) = (semaphore.clone(), log.clone());
            async move {
                let _permit = semaphore.acquire().await;
                log.borrow_mut().push("one");
            }
        });
        yield_now().await;
        drop(first);
        yield_now().await;
        // The one free permit waits for the waiter at the head.
        assert!(log.borrow().is_empty());
        assert_eq!(semaphore.available_permits(), 1);
        assert!(semaphore.try_acquire().is_none(), "nobody jumps the queue");
        drop(second);
        many.await.unwrap();
        one.await.unwrap();
        assert_eq!(*log.borrow(), ["two", "one"]);
        assert_eq!(semaphore.available_permits(), 2);
    });
}

/// A waiter that gives up leaves the queue, and one that gives up after
/// its permits were set aside gives them back.
#[test]
fn a_waiter_that_gives_up_holds_nobody_up() {
    Runtime::new().unwrap().block_on(async {
        let semaphore = Semaphore::new(1);
        let held = semaphore.try_acquire().unwrap();

        let mut head = Box::pin(semaphore.acquire());
        assert!(poll_once(head.as_mut()).await.is_pending());
        let mut behind = pin!(semaphore.acquire());
        assert!(poll_once(behind.as_mut()).await.is_pending());
        drop(head);
        drop(held);
        let permit = behind.await;

        let mut granted = Box::pin(semaphore.acquire());
        assert!(poll_once(granted.as_mut()).await.is_pending());
        drop(permit);
        // The permit is set aside for the waiter, which never takes it.
        assert_eq!(semaphore.available_permits(), 0);
        drop(granted);
        assert_eq!(semaphore.available_permits(), 1);

        // A waiter for more than is free holds up one for less, until it
        // gives up.
        let semaphore = Semaphore::new(2);
        let held = semaphore.try_acquire().unwrap();
        let mut many = Box::pin(semaphore.acquire_many(2));
        assert!(poll_once(many.as_mut()).await.is_pending());
        let mut one = pin!(semaphore.acquire());
        assert!(poll_once(one.as_mut()).await.is_pending());
        drop(many);
        assert!(poll_once(one.as_mut()).await.is_ready());
        drop(held);
    });
}

#[test]
fn permits_can_be_owned_forgotten_and_added() {
    Runtime::new().unwrap().block_on(async {
        let semaphore = Arc::new(Semaphore::new(2));
        let permit = semaphore.clone().acquire_owned().await;
        assert_eq!(permit.num_permits(), 1);
        assert!(Arc::ptr_eq(permit.semaphore(), &semaphore));
        let holder = spawn(async move {
            yield_now().await;
            drop(permit);
        });
        let other = semaphore.clone().try_acquire_owned().unwrap();
        assert!(semaphore.try_acquire().is_none());
        holder.await.unwrap();
        assert_eq!(semaphore.available_permits(), 1);

        other.forget();
        assert_eq!(semaphore.available_permits(), 1);
        semaphore.add_permits(3);
        assert_eq!(semaphore.available_permits(), 4);
        let many = semaphore.acquire_many(4).await;
        assert_eq!(many.num_permits(), 4);
        assert!(semaphore.try_acquire().is_none());
    });
}

/// Tasks on several runtimes share the permits of one semaphore.
#[test]
fn a_semaphore_works_across_threads() {
    let semaphore = Arc::new(Semaphore::new(2));
    let occupancy = Arc::new(Occupancy::default());
    let threads: Vec<_> = (0..4)
        .map(|_| {
            let (semaphore, occupancy) = (semaphore.clone(), occupancy.clone());
            thread::spawn(move || {
                Runtime::new().unwrap().block_on(async move {
                    for _ in 0..200 {
                        let _permit = semaphore.acquire().await;
                        occupancy.enter();
                        yield_now().await;
                        occupancy.leave();
                    }
                });
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    assert!(
        occupancy.most() <= 2,
        "{} held permits at once",
        occupancy.most()
    );
    assert_eq!(semaphore.available_permits(), 2);
}

/// A loop over permits that are always free yields once its budget is
/// spent.
#[test]
fn free_permits_do_not_starve_the_other_tasks() {
    Runtime::new().unwrap().block_on(async {
        let semaphore = Rc::new(Semaphore::new(1));
        let acquired = Rc::new(Cell::new(0));
        let looper = spawn({
            let (semaphore, acquired) = (semaphore.clone(), acquired.clone());
            async move {
                for _ in 0..10_000 {
                    drop(semaphore.acquire().await);
                    acquired.set(acquired.get() + 1);
                }
            }
        });
        let seen = spawn({
            let acquired = acquired.clone();
            async move { acquired.get() }
        })
        .await
        .unwrap();
        assert!(0 < seen && seen < 1000, "{seen} acquires");
        looper.await.unwrap();
    });
}

/// Every increment lands, though each task yields between reading the
/// value and writing it back.
#[test]
fn a_mutex_gives_its_value_to_one_task_at_a_time() {
    Runtime::new().unwrap().block_on(async {
        let counter = Rc::new(Mutex::new(0));
        let tasks: Vec<_> = (0..10)
            .map(|_| {
                let counter = counter.clone();
                spawn(async move {
                    for _ in 0..100 {
                        let mut value = counter.lock().await;
                        let read = *value;
                        yield_now().await;
                        *value = read + 1;
                    }
                })
            })
            .collect();
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(*counter.lock().await, 1000);
    });
}

#[test]
fn a_mutex_works_across_threads() {
    let counter = Arc::new(Mutex::new(0u64));
    let threads: Vec<_> = (0..4)
        .map(|_| {
            let counter = counter.clone();
            thread::spawn(move || {
                Runtime::new().unwrap().block_on(async move {
                    let tasks: Vec<_> = (0..2)
                        .map(|_| {
                            let counter = counter.clone();
                            spawn(async move {
                                for _ in 0..500 {
                                    let mut value = counter.lock().await;
                                    let read = *value;
                                    yield_now().await;
                                    *value = read + 1;
                                }
                            })
                        })
                        .collect();
                    for task in tasks {
                        task.await.unwrap();
                    }
                });
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    let counter = Arc::try_unwrap(counter).unwrap();
    assert_eq!(counter.into_inner(), 4000);
}

#[test]
fn try_lock_fails_while_the_mutex_is_held_or_waited_for() {
    Runtime::new().unwrap().block_on(async {
        let mutex = Mutex::new(String::from("a"));
        let mut guard = mutex.try_lock().unwrap();
        guard.push('b');
        assert!(mutex.try_lock().is_none());
        assert_eq!(format!("{mutex:?}"), "Mutex { value: <locked>, .. }");
        {
            let mut waiting = pin!(mutex.lock());
            assert!(poll_once(waiting.as_mut()).await.is_pending());
            drop(guard);
            assert!(mutex.try_lock().is_none(), "the lock is the waiter's");
            assert_eq!(*waiting.await, "ab");
        }
        assert_eq!(format!("{mutex:?}"), "Mutex { value: \"ab\", .. }");
        let mut mutex = mutex;
        mutex.get_mut().push('c');
        assert_eq!(mutex.into_inner(), "abc");
    });
}

/// A future holding a guard across an await can move between threads, as
/// `spawn_on` asks, when the value can.
#[test]
fn guards_can_be_held_across_awaits_in_send_futures() {
    fn send<T: Send>(_: &T) {}
    fn sync<T: Sync>() {}
    let mutex = Arc::new(Mutex::new(0));
    let lock = Arc::new(RwLock::new(0));
    send(&async move {
        let mut value = mutex.lock().await;
        yield_now().await;
        *value += 1;
    });
    send(&async move {
        let value = lock.read().await;
        yield_now().await;
        drop(value);
        *lock.write().await += 1;
    });
    fn send_type<T: Send>() {}
    sync::<MutexGuard<'static, u8>>();
    sync::<Mutex<std::cell::Cell<u8>>>();
    send_type::<Mutex<u8>>();
    sync::<RwLock<u8>>();
}

#[test]
fn readers_share_a_lock_and_writers_have_it_alone() {
    Runtime::new().unwrap().block_on(async {
        let lock = RwLock::new(vec![1]);
        let first = lock.read().await;
        let second = lock.try_read().unwrap();
        assert_eq!(first.len() + second.len(), 2);
        assert!(lock.try_write().is_none());
        drop((first, second));
        let mut writer = lock.write().await;
        writer.push(2);
        assert!(lock.try_read().is_none());
        assert!(lock.try_write().is_none());
        drop(writer);
        assert_eq!(*lock.read().await, [1, 2]);
        let mut lock = lock;
        lock.get_mut().push(3);
        assert_eq!(lock.into_inner(), [1, 2, 3]);
    });
}

/// A writer waiting for the readers holds up the readers that come after
/// it, and gets the lock before them.
#[test]
fn a_waiting_writer_holds_up_later_readers() {
    Runtime::new().unwrap().block_on(async {
        let lock = Rc::new(RwLock::new(0));
        let log = Rc::new(RefCell::new(Vec::new()));
        let reading = lock.read().await;
        let writer = spawn({
            let (lock, log) = (lock.clone(), log.clone());
            async move {
                *lock.write().await += 1;
                log.borrow_mut().push("writer");
            }
        });
        yield_now().await;
        assert!(lock.try_read().is_none(), "the writer is waiting");
        let reader = spawn({
            let (lock, log) = (lock.clone(), log.clone());
            async move {
                let value = lock.read().await;
                log.borrow_mut().push("reader");
                *value
            }
        });
        yield_now().await;
        assert!(log.borrow().is_empty());
        drop(reading);
        writer.await.unwrap();
        assert_eq!(reader.await.unwrap(), 1);
        assert_eq!(*log.borrow(), ["writer", "reader"]);
    });
}

/// Writers and readers on several threads see every write.
#[test]
fn a_lock_works_across_threads() {
    let lock = Arc::new(RwLock::new(0u64));
    let threads: Vec<_> = (0..4)
        .map(|_| {
            let lock = lock.clone();
            thread::spawn(move || {
                Runtime::new().unwrap().block_on(async move {
                    for _ in 0..300 {
                        let before = *lock.read().await;
                        let mut value = lock.write().await;
                        assert!(*value >= before);
                        let read = *value;
                        yield_now().await;
                        *value = read + 1;
                    }
                });
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(Arc::try_unwrap(lock).unwrap().into_inner(), 1200);
}

#[test]
fn notify_one_wakes_the_task_that_waited_longest() {
    Runtime::new().unwrap().block_on(async {
        let notify = Rc::new(Notify::new());
        let woken = Rc::new(RefCell::new(Vec::new()));
        let waiters: Vec<_> = (0..3)
            .map(|id| {
                let (notify, woken) = (notify.clone(), woken.clone());
                spawn(async move {
                    notify.notified().await;
                    woken.borrow_mut().push(id);
                })
            })
            .collect();
        yield_now().await;
        notify.notify_one();
        yield_now().await;
        assert_eq!(*woken.borrow(), [0]);
        notify.notify_one();
        notify.notify_one();
        for waiter in waiters {
            waiter.await.unwrap();
        }
        assert_eq!(*woken.borrow(), [0, 1, 2]);
    });
}

/// With nobody waiting, a notification lets the next to wait go at once,
/// and several do not add up.
#[test]
fn a_notification_with_nobody_waiting_lets_the_next_go() {
    Runtime::new().unwrap().block_on(async {
        let notify = Notify::new();
        notify.notify_one();
        notify.notify_one();
        notify.notified().await;
        let mut second = pin!(notify.notified());
        assert!(poll_once(second.as_mut()).await.is_pending());
        notify.notify_one();
        second.await;
    });
}

/// `notify_waiters` wakes every waiter made before it, polled or not, and
/// leaves no permit for those after.
#[test]
fn notify_waiters_wakes_everyone_waiting() {
    Runtime::new().unwrap().block_on(async {
        let notify = Rc::new(Notify::new());
        let unpolled = notify.notified();
        let waiting = spawn({
            let notify = notify.clone();
            async move { notify.notified().await }
        });
        yield_now().await;
        notify.notify_waiters();
        unpolled.await;
        waiting.await.unwrap();
        let mut after = pin!(notify.notified());
        assert!(poll_once(after.as_mut()).await.is_pending());
    });
}

/// A waiter dropped after `notify_one` chose it, before it saw the
/// notification, passes it on to the next.
#[test]
fn a_waiter_that_gives_up_passes_its_notification_on() {
    Runtime::new().unwrap().block_on(async {
        let notify = Notify::new();
        let mut first = Box::pin(notify.notified());
        assert!(poll_once(first.as_mut()).await.is_pending());
        let mut second = pin!(notify.notified());
        assert!(poll_once(second.as_mut()).await.is_pending());
        notify.notify_one();
        drop(first);
        assert!(poll_once(second.as_mut()).await.is_ready());

        // With nobody else waiting, it is left for the next to wait.
        let mut chosen = Box::pin(notify.notified());
        assert!(poll_once(chosen.as_mut()).await.is_pending());
        notify.notify_one();
        drop(chosen);
        notify.notified().await;
    });
}

#[test]
fn a_notification_from_another_thread_wakes_the_task() {
    Runtime::new().unwrap().block_on(async {
        let notify = Arc::new(Notify::new());
        let notifier = {
            let notify = notify.clone();
            thread::spawn(move || {
                thread::sleep(std::time::Duration::from_millis(20));
                notify.notify_one();
            })
        };
        notify.notified().await;
        notifier.join().unwrap();
    });
}
