//! The blocking pool: jobs on threads of their own, their outputs awaited.

use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, mpsc};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

use ruio_executor::blocking::{self, BlockingHandle, Pool};
use ruio_executor::time::timeout;
use ruio_executor::{JoinSet, Runtime, spawn_blocking};

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

/// A value that counts its drops.
struct Counted(Arc<AtomicUsize>);

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn a_job_runs_on_another_thread_and_its_output_comes_back() {
    run(async {
        let here = thread::current().id();
        let (there, answer) = spawn_blocking(move || (thread::current().id(), 42))
            .await
            .unwrap();
        assert_ne!(there, here);
        assert_eq!(answer, 42);
    });
}

#[test]
fn jobs_run_in_parallel_on_threads_made_as_they_come() {
    run(async {
        let pool = Pool::new();
        let start = Instant::now();
        let mut set = JoinSet::new();
        for n in 0..16 {
            let job = pool.spawn(move || {
                thread::sleep(Duration::from_millis(50));
                n
            });
            set.spawn(async move { job.await.unwrap() });
        }
        let mut outputs = set.join_all().await;
        outputs.sort_unstable();
        assert_eq!(outputs, (0..16).collect::<Vec<_>>());
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(400),
            "{elapsed:?}: not in parallel"
        );
        assert!(pool.threads() > 1, "{} threads", pool.threads());
    });
}

#[test]
fn a_panicking_job_reports_its_panic() {
    run(async {
        let err = spawn_blocking(|| -> u32 { panic!("boom") })
            .await
            .unwrap_err();
        assert!(err.is_panic());
        assert_eq!(*err.into_panic().downcast::<&str>().unwrap(), "boom");
    });
}

#[test]
fn aborting_a_job_before_it_starts_cancels_it() {
    run(async {
        let pool = Pool::builder().threads(1).build();
        let (release, held) = mpsc::channel::<()>();
        // Holds the one thread until released.
        let first = pool.spawn(move || held.recv().unwrap_or(()));
        let ran = Arc::new(AtomicBool::new(false));
        let second = {
            let ran = ran.clone();
            pool.spawn(move || ran.store(true, Ordering::Relaxed))
        };
        assert!(!second.is_finished());
        second.abort();
        release.send(()).unwrap();
        first.await.unwrap();
        let err = second.await.unwrap_err();
        assert!(err.is_cancelled());
        assert!(!ran.load(Ordering::Relaxed), "a cancelled job ran");
    });
}

#[test]
fn aborting_a_started_job_lets_it_finish() {
    run(async {
        let (started, wait_started) = mpsc::channel::<()>();
        let (release, held) = mpsc::channel::<()>();
        let job = spawn_blocking(move || {
            started.send(()).unwrap();
            held.recv().unwrap();
            7
        });
        wait_started.recv().unwrap();
        job.abort();
        release.send(()).unwrap();
        assert_eq!(job.await.unwrap(), 7);
    });
}

#[test]
fn dropping_the_handle_leaves_the_job_to_run_and_drops_its_output() {
    run(async {
        let ran = Arc::new(AtomicBool::new(false));
        let drops = Arc::new(AtomicUsize::new(0));
        let (release, held) = mpsc::channel::<()>();
        let handle = {
            let (ran, drops) = (ran.clone(), drops.clone());
            spawn_blocking(move || {
                held.recv().unwrap();
                ran.store(true, Ordering::Relaxed);
                Counted(drops)
            })
        };
        drop(handle);
        release.send(()).unwrap();
        while drops.load(Ordering::Relaxed) == 0 {
            thread::sleep(Duration::from_millis(1));
        }
        assert!(ran.load(Ordering::Relaxed));
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    });
}

/// Polls a future on this thread, parked between polls, with no runtime.
fn block_on_this_thread<F: Future>(future: F) -> F::Output {
    struct Unpark(thread::Thread);

    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = Waker::from(Arc::new(Unpark(thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        thread::park();
    }
}

#[test]
fn a_handle_can_be_awaited_outside_a_runtime() {
    let handle = spawn_blocking(|| {
        thread::sleep(Duration::from_millis(10));
        "done"
    });
    assert_eq!(block_on_this_thread(handle).unwrap(), "done");
}

#[test]
fn a_handle_can_be_awaited_on_another_thread() {
    let handle = spawn_blocking(|| 9);
    let output = thread::spawn(move || run(handle)).join().unwrap();
    assert_eq!(output.unwrap(), 9);
}

#[test]
fn a_pool_keeps_within_its_thread_limit() {
    run(async {
        let pool = Pool::builder().threads(2).build();
        let running = Arc::new(AtomicUsize::new(0));
        let most = Arc::new(AtomicUsize::new(0));
        let mut set = JoinSet::new();
        for _ in 0..8 {
            let (running, most) = (running.clone(), most.clone());
            let job = pool.spawn(move || {
                let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                most.fetch_max(now, Ordering::SeqCst);
                thread::sleep(Duration::from_millis(20));
                running.fetch_sub(1, Ordering::SeqCst);
            });
            set.spawn(async move { job.await.unwrap() });
        }
        let start = Instant::now();
        set.join_all().await;
        assert!(
            start.elapsed() >= Duration::from_millis(80),
            "{:?}",
            start.elapsed()
        );
        assert!(
            most.load(Ordering::SeqCst) <= 2,
            "{} at once",
            most.load(Ordering::SeqCst)
        );
        assert!(pool.threads() <= 2, "{} threads", pool.threads());
    });
}

#[test]
fn idle_threads_go_after_the_keep_alive() {
    run(async {
        let pool = Pool::builder()
            .keep_alive(Duration::from_millis(20))
            .build();
        for _ in 0..4 {
            pool.spawn(|| ()).await.unwrap();
        }
        assert!(pool.threads() >= 1);
        let start = Instant::now();
        while pool.threads() > 0 {
            assert!(start.elapsed() < Duration::from_secs(5), "threads stayed");
            thread::sleep(Duration::from_millis(5));
        }
        // Threads come back for new jobs.
        assert_eq!(pool.spawn(|| 3).await.unwrap(), 3);
        assert_eq!(pool.threads(), 1);
    });
}

#[test]
fn a_dropped_pool_still_runs_the_jobs_queued() {
    run(async {
        let pool = Pool::builder().threads(1).build();
        let handles: Vec<_> = (0..3)
            .map(|n| {
                pool.spawn(move || {
                    thread::sleep(Duration::from_millis(10));
                    n
                })
            })
            .collect();
        drop(pool);
        for (n, handle) in handles.into_iter().enumerate() {
            assert_eq!(handle.await.unwrap(), n);
        }
    });
}

#[test]
fn the_handle_is_send_and_sync() {
    fn send_sync<T: Send + Sync>() {}
    send_sync::<BlockingHandle<Vec<u8>>>();
    send_sync::<Pool>();
}

#[test]
fn the_process_has_one_pool() {
    assert!(std::ptr::eq(blocking::global(), blocking::global()));
    assert!(format!("{:?}", blocking::global()).starts_with("Pool {"));
}

#[test]
fn a_finished_handle_says_so() {
    run(async {
        let handle = spawn_blocking(|| 1);
        let output = block_on_this_thread(async {
            let handle = handle;
            while !handle.is_finished() {
                thread::sleep(Duration::from_millis(1));
            }
            assert!(format!("{handle:?}").contains("finished: true"));
            handle.await
        });
        assert_eq!(output.unwrap(), 1);
    });
}

/// Jobs from runtimes on several threads, awaited one after another on
/// each, all come back, every wake crossing threads.
#[test]
fn jobs_from_many_runtimes_all_come_back() {
    const THREADS: usize = 4;
    const JOBS: u64 = 2000;
    let ready = Arc::new(Barrier::new(THREADS));
    let threads: Vec<_> = (0..THREADS)
        .map(|_| {
            let ready = ready.clone();
            thread::spawn(move || {
                ready.wait();
                run(async {
                    let mut sum = 0;
                    for n in 0..JOBS {
                        sum += spawn_blocking(move || n).await.unwrap();
                    }
                    assert_eq!(sum, (0..JOBS).sum::<u64>());
                });
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
}

#[test]
fn a_spinning_pool_runs_jobs_as_any_other() {
    run(async {
        let pool = Pool::builder()
            .threads(2)
            .spin(Duration::from_micros(200))
            .build();
        let mut sum = 0;
        for n in 0..1000u64 {
            sum += pool.spawn(move || n).await.unwrap();
        }
        assert_eq!(sum, (0..1000u64).sum::<u64>());
        let mut set = JoinSet::new();
        for n in 0..100u64 {
            let job = pool.spawn(move || n);
            set.spawn(async move { job.await.unwrap() });
        }
        assert_eq!(
            set.join_all().await.into_iter().sum::<u64>(),
            (0..100u64).sum::<u64>()
        );
    });
}
