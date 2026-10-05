//! Microbenchmarks of the runtime itself: what a turn of the loop, a task, a
//! wake, a timer or a channel costs in user space, apart from the kernel's
//! work on sockets and files, which the other benchmarks measure. Each is a
//! loop of one small step, and the result is steps per second: on ruio, and,
//! where tokio has the same thing, on tokio's current-thread runtime.
//!
//! - `nop 1`: one task awaiting no-ops, one at a time. Each is a turn of the
//!   loop: queue, enter the kernel and wait, deliver, wake, run.
//! - `nop 64`: 64 tasks awaiting no-ops, so that a turn delivers a batch.
//! - `spawn`: spawn a task that does nothing, and await it.
//! - `join set`: spawn a task that does nothing into a join set, and take
//!   its output from the set.
//! - `yield`: yield to the scheduler, which runs the task again.
//! - `channel`: a value sent to another task through a channel, and one
//!   back: two wakes and two runs.
//! - `local channel`: the same through the channels of one thread, which
//!   take no atomic operation.
//! - `unbounded channel`: the same through unbounded channels, whose send
//!   never waits.
//! - `local unbounded channel`: the same through the unbounded channels of
//!   one thread.
//! - `broadcast`: a value to another task and one back through broadcast
//!   channels with one receiver each.
//! - `watch`: a value to another task and one back through watch channels:
//!   each way a send, and a wait for the change.
//! - `remote`: a value sent to a thread of its own, which sends one back
//!   through a channel: a wake from another thread, and a park and unpark.
//! - `timer`: arm a timer by polling a sleep once, then drop it.
//! - `sleep`: 1000 tasks each sleeping a millisecond at a time: timers that
//!   fire.
//! - `mutex`: lock and unlock an uncontended async mutex.
//! - `barrier`: two tasks meeting at a barrier, again and again: each round
//!   a wake and a run.
//! - `cancel`: arm a wait for a token's cancellation by polling it once,
//!   then drop it.

use std::cell::Cell;
use std::fmt;
use std::future::{Future, poll_fn};
use std::io;
use std::pin::pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc as std_mpsc};
use std::task::Poll;
use std::thread;
use std::time::{Duration, Instant};

use ruio::uring::ops::Nop;

use crate::sys;

/// How long each measurement runs, and where.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Load {
    pub(crate) measure: Duration,
    /// The CPU the runtime runs on; the thread of `remote` runs on another.
    pub(crate) cpu: usize,
}

/// A microbenchmark: how to run it on each runtime, if the runtime has
/// what it measures.
#[derive(Clone, Copy)]
pub(crate) struct Micro {
    pub(crate) name: &'static str,
    ruio: fn(&Load) -> io::Result<f64>,
    tokio: Option<fn(&Load) -> io::Result<f64>>,
}

impl Micro {
    /// Steps per second on `runtime`, or `None` if it has no such thing.
    pub(crate) fn run(&self, runtime: Runtime, load: &Load) -> Option<io::Result<f64>> {
        match runtime {
            Runtime::Ruio => Some((self.ruio)(load)),
            Runtime::Tokio => self.tokio.map(|run| run(load)),
        }
    }
}

impl fmt::Debug for Micro {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Micro").field("name", &self.name).finish()
    }
}

/// A runtime to measure on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Runtime {
    Ruio,
    Tokio,
}

impl Runtime {
    pub(crate) const ALL: [Runtime; 2] = [Runtime::Tokio, Runtime::Ruio];
}

impl fmt::Display for Runtime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Runtime::Ruio => "ruio",
            Runtime::Tokio => "tokio",
        })
    }
}

/// Every microbenchmark, in the order they are reported.
pub(crate) const ALL: &[Micro] = &[
    Micro {
        name: "nop 1",
        ruio: |load| ruio_nop(load, 1),
        tokio: None,
    },
    Micro {
        name: "nop 64",
        ruio: |load| ruio_nop(load, 64),
        tokio: None,
    },
    Micro {
        name: "spawn",
        ruio: ruio_spawn,
        tokio: Some(tokio_spawn),
    },
    Micro {
        name: "join set",
        ruio: ruio_join_set,
        tokio: Some(tokio_join_set),
    },
    Micro {
        name: "yield",
        ruio: ruio_yield,
        tokio: Some(tokio_yield),
    },
    Micro {
        name: "channel",
        ruio: ruio_channel,
        tokio: Some(tokio_channel),
    },
    Micro {
        name: "local channel",
        ruio: ruio_local_channel,
        tokio: None,
    },
    Micro {
        name: "unbounded channel",
        ruio: ruio_unbounded,
        tokio: Some(tokio_unbounded),
    },
    Micro {
        name: "local unbounded channel",
        ruio: ruio_local_unbounded,
        tokio: None,
    },
    Micro {
        name: "broadcast",
        ruio: ruio_broadcast,
        tokio: Some(tokio_broadcast),
    },
    Micro {
        name: "watch",
        ruio: ruio_watch,
        tokio: Some(tokio_watch),
    },
    Micro {
        name: "remote",
        ruio: ruio_remote,
        tokio: Some(tokio_remote),
    },
    Micro {
        name: "timer",
        ruio: ruio_timer,
        tokio: Some(tokio_timer),
    },
    Micro {
        name: "sleep",
        ruio: ruio_sleep,
        tokio: Some(tokio_sleep),
    },
    Micro {
        name: "mutex",
        ruio: ruio_mutex,
        tokio: Some(tokio_mutex),
    },
    Micro {
        name: "barrier",
        ruio: ruio_barrier,
        tokio: Some(tokio_barrier),
    },
    Micro {
        name: "cancel",
        ruio: ruio_cancel,
        tokio: Some(tokio_cancel),
    },
];

/// How many tasks `sleep` runs.
const SLEEPERS: usize = 1000;

/// Runs `step` again and again for `duration`, and returns how many times a
/// second it ran. The clock is read once every 64 steps, so that reading it
/// is not what is measured.
async fn rate(duration: Duration, mut step: impl AsyncFnMut()) -> f64 {
    let start = Instant::now();
    let deadline = start + duration;
    let mut count: u64 = 0;
    loop {
        for _ in 0..64 {
            step().await;
        }
        count += 64;
        if Instant::now() >= deadline {
            return count as f64 / start.elapsed().as_secs_f64();
        }
    }
}

/// Counts how many times `step` ran, until `stop` is set.
async fn count_until(stop: impl Fn() -> bool, mut step: impl AsyncFnMut()) -> u64 {
    let mut count = 0;
    while !stop() {
        step().await;
        count += 1;
    }
    count
}

/// The CPU the thread of `remote` runs on: another core than the
/// runtime's, which has its SMT sibling at a fixed distance on the hosts
/// this is run on.
fn remote_cpu(load: &Load) -> usize {
    load.cpu + 2
}

fn ruio_runtime() -> io::Result<ruio::Runtime> {
    ruio::Runtime::new()
}

fn tokio_runtime() -> io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
}

/// `depth` tasks each awaiting no-ops one at a time, for `load.measure`.
fn ruio_nop(load: &Load, depth: usize) -> io::Result<f64> {
    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let driver = ruio::driver();
        let stop = Rc::new(Cell::new(false));
        let tasks: Vec<_> = (0..depth)
            .map(|_| {
                let (driver, stop) = (driver.clone(), Rc::clone(&stop));
                ruio::spawn(async move {
                    let mut count = 0;
                    while !stop.get() {
                        driver.op(Nop).await?;
                        count += 1;
                    }
                    Ok::<u64, io::Error>(count)
                })
            })
            .collect();
        let start = Instant::now();
        ruio::time::sleep(measure).await;
        stop.set(true);
        let mut total = 0;
        for task in tasks {
            total += task.await.map_err(io::Error::other)??;
        }
        Ok(total as f64 / start.elapsed().as_secs_f64())
    })
}

fn ruio_spawn(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        Ok(rate(measure, async || {
            ruio::spawn(async {}).await.expect("the task ran");
        })
        .await)
    })
}

fn tokio_spawn(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    tokio_runtime()?.block_on(async move {
        Ok(rate(measure, async || {
            tokio::spawn(async {}).await.expect("the task ran");
        })
        .await)
    })
}

/// A task that does nothing, spawned into a join set and taken out of it.
fn ruio_join_set(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let mut set = ruio::JoinSet::new();
        Ok(rate(measure, async || {
            set.spawn(async {});
            set.join_next()
                .await
                .expect("a task is in the set")
                .expect("the task ran");
        })
        .await)
    })
}

fn tokio_join_set(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    tokio_runtime()?.block_on(async move {
        let mut set = tokio::task::JoinSet::new();
        Ok(rate(measure, async || {
            set.spawn(async {});
            set.join_next()
                .await
                .expect("a task is in the set")
                .expect("the task ran");
        })
        .await)
    })
}

fn ruio_yield(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    ruio_runtime()?.block_on(async move { Ok(rate(measure, ruio::yield_now).await) })
}

fn tokio_yield(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    tokio_runtime()?.block_on(async move { Ok(rate(measure, tokio::task::yield_now).await) })
}

/// A value to a second task and one back, through bounded channels.
fn ruio_channel(load: &Load) -> io::Result<f64> {
    use ruio::sync::mpsc;

    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let (to_echo, mut echo_rx) = mpsc::channel::<u32>(1);
        let (to_main, mut main_rx) = mpsc::channel::<u32>(1);
        let echo = ruio::spawn(async move {
            while let Some(value) = echo_rx.recv().await {
                if to_main.send(value).await.is_err() {
                    break;
                }
            }
        });
        let rate = rate(measure, async || {
            to_echo.send(1).await.expect("the echo task runs");
            main_rx.recv().await.expect("the echo task answers");
        })
        .await;
        drop(to_echo);
        echo.await.map_err(io::Error::other)?;
        Ok(rate)
    })
}

/// A value to a second task and one back, through the bounded channels of
/// one thread.
fn ruio_local_channel(load: &Load) -> io::Result<f64> {
    use ruio::sync::local::mpsc;

    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let (to_echo, mut echo_rx) = mpsc::channel::<u32>(1);
        let (to_main, mut main_rx) = mpsc::channel::<u32>(1);
        let echo = ruio::spawn(async move {
            while let Some(value) = echo_rx.recv().await {
                if to_main.send(value).await.is_err() {
                    break;
                }
            }
        });
        let rate = rate(measure, async || {
            to_echo.send(1).await.expect("the echo task runs");
            main_rx.recv().await.expect("the echo task answers");
        })
        .await;
        drop(to_echo);
        echo.await.map_err(io::Error::other)?;
        Ok(rate)
    })
}

fn tokio_channel(load: &Load) -> io::Result<f64> {
    use tokio::sync::mpsc;

    let measure = load.measure;
    tokio_runtime()?.block_on(async move {
        let (to_echo, mut echo_rx) = mpsc::channel::<u32>(1);
        let (to_main, mut main_rx) = mpsc::channel::<u32>(1);
        let echo = tokio::spawn(async move {
            while let Some(value) = echo_rx.recv().await {
                if to_main.send(value).await.is_err() {
                    break;
                }
            }
        });
        let rate = rate(measure, async || {
            to_echo.send(1).await.expect("the echo task runs");
            main_rx.recv().await.expect("the echo task answers");
        })
        .await;
        drop(to_echo);
        echo.await.map_err(io::Error::other)?;
        Ok(rate)
    })
}

/// A value to a thread of its own, through a standard channel, and one back
/// through the runtime's, which wakes the task from that thread.
fn ruio_remote(load: &Load) -> io::Result<f64> {
    use ruio::sync::mpsc;

    let (measure, cpu) = (load.measure, remote_cpu(load));
    ruio_runtime()?.block_on(async move {
        let (to_thread, thread_rx) = std_mpsc::channel::<u32>();
        let (to_main, mut main_rx) = mpsc::channel::<u32>(1);
        let echo = thread::spawn(move || {
            // Not pinned if the host has no such CPU.
            let _ = sys::pin(&[cpu]);
            for value in thread_rx {
                if to_main.try_send(value).is_err() {
                    break;
                }
            }
        });
        let rate = rate(measure, async || {
            to_thread.send(1).expect("the thread runs");
            main_rx.recv().await.expect("the thread answers");
        })
        .await;
        drop(to_thread);
        echo.join()
            .map_err(|_| io::Error::other("the thread panicked"))?;
        Ok(rate)
    })
}

fn tokio_remote(load: &Load) -> io::Result<f64> {
    use tokio::sync::mpsc;

    let (measure, cpu) = (load.measure, remote_cpu(load));
    tokio_runtime()?.block_on(async move {
        let (to_thread, thread_rx) = std_mpsc::channel::<u32>();
        let (to_main, mut main_rx) = mpsc::channel::<u32>(1);
        let echo = thread::spawn(move || {
            let _ = sys::pin(&[cpu]);
            for value in thread_rx {
                if to_main.try_send(value).is_err() {
                    break;
                }
            }
        });
        let rate = rate(measure, async || {
            to_thread.send(1).expect("the thread runs");
            main_rx.recv().await.expect("the thread answers");
        })
        .await;
        drop(to_thread);
        echo.join()
            .map_err(|_| io::Error::other("the thread panicked"))?;
        Ok(rate)
    })
}

/// Polls `sleep` once, which arms its timer, and drops it, which disarms it.
async fn arm_and_drop<S: Future<Output = ()>>(sleep: S) {
    let mut sleep = pin!(sleep);
    poll_fn(|cx| {
        let _ = sleep.as_mut().poll(cx);
        Poll::Ready(())
    })
    .await;
}

fn ruio_timer(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        Ok(rate(measure, async || {
            arm_and_drop(ruio::time::sleep(Duration::from_secs(1))).await;
        })
        .await)
    })
}

fn tokio_timer(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    tokio_runtime()?.block_on(async move {
        Ok(rate(measure, async || {
            arm_and_drop(tokio::time::sleep(Duration::from_secs(1))).await;
        })
        .await)
    })
}

/// [`SLEEPERS`] tasks each sleeping a millisecond at a time.
fn ruio_sleep(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let stop = Rc::new(Cell::new(false));
        let tasks: Vec<_> = (0..SLEEPERS)
            .map(|_| {
                let stop = Rc::clone(&stop);
                ruio::spawn(async move {
                    count_until(
                        || stop.get(),
                        || ruio::time::sleep(Duration::from_millis(1)),
                    )
                    .await
                })
            })
            .collect();
        let start = Instant::now();
        ruio::time::sleep(measure).await;
        stop.set(true);
        let mut total = 0;
        for task in tasks {
            total += task.await.map_err(io::Error::other)?;
        }
        Ok(total as f64 / start.elapsed().as_secs_f64())
    })
}

fn tokio_sleep(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    tokio_runtime()?.block_on(async move {
        let stop = Arc::new(AtomicBool::new(false));
        let tasks: Vec<_> = (0..SLEEPERS)
            .map(|_| {
                let stop = Arc::clone(&stop);
                tokio::spawn(async move {
                    count_until(
                        || stop.load(Ordering::Relaxed),
                        || tokio::time::sleep(Duration::from_millis(1)),
                    )
                    .await
                })
            })
            .collect();
        let start = Instant::now();
        tokio::time::sleep(measure).await;
        stop.store(true, Ordering::Relaxed);
        let mut total = 0;
        for task in tasks {
            total += task.await.map_err(io::Error::other)?;
        }
        Ok(total as f64 / start.elapsed().as_secs_f64())
    })
}

fn ruio_mutex(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let mutex = ruio::sync::Mutex::new(0_u64);
        Ok(rate(measure, async || {
            *mutex.lock().await += 1;
        })
        .await)
    })
}

fn tokio_mutex(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    tokio_runtime()?.block_on(async move {
        let mutex = tokio::sync::Mutex::new(0_u64);
        Ok(rate(measure, async || {
            *mutex.lock().await += 1;
        })
        .await)
    })
}

/// A value to a second task and one back, through unbounded channels.
fn ruio_unbounded(load: &Load) -> io::Result<f64> {
    use ruio::sync::mpsc;

    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let (to_echo, mut echo_rx) = mpsc::unbounded_channel::<u32>();
        let (to_main, mut main_rx) = mpsc::unbounded_channel::<u32>();
        let echo = ruio::spawn(async move {
            while let Some(value) = echo_rx.recv().await {
                if to_main.send(value).is_err() {
                    break;
                }
            }
        });
        let rate = rate(measure, async || {
            to_echo.send(1).expect("the echo task runs");
            main_rx.recv().await.expect("the echo task answers");
        })
        .await;
        drop(to_echo);
        echo.await.map_err(io::Error::other)?;
        Ok(rate)
    })
}

/// A value to a second task and one back, through the unbounded channels of
/// one thread.
fn ruio_local_unbounded(load: &Load) -> io::Result<f64> {
    use ruio::sync::local::mpsc;

    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let (to_echo, mut echo_rx) = mpsc::unbounded_channel::<u32>();
        let (to_main, mut main_rx) = mpsc::unbounded_channel::<u32>();
        let echo = ruio::spawn(async move {
            while let Some(value) = echo_rx.recv().await {
                if to_main.send(value).is_err() {
                    break;
                }
            }
        });
        let rate = rate(measure, async || {
            to_echo.send(1).expect("the echo task runs");
            main_rx.recv().await.expect("the echo task answers");
        })
        .await;
        drop(to_echo);
        echo.await.map_err(io::Error::other)?;
        Ok(rate)
    })
}

fn tokio_unbounded(load: &Load) -> io::Result<f64> {
    use tokio::sync::mpsc;

    let measure = load.measure;
    tokio_runtime()?.block_on(async move {
        let (to_echo, mut echo_rx) = mpsc::unbounded_channel::<u32>();
        let (to_main, mut main_rx) = mpsc::unbounded_channel::<u32>();
        let echo = tokio::spawn(async move {
            while let Some(value) = echo_rx.recv().await {
                if to_main.send(value).is_err() {
                    break;
                }
            }
        });
        let rate = rate(measure, async || {
            to_echo.send(1).expect("the echo task runs");
            main_rx.recv().await.expect("the echo task answers");
        })
        .await;
        drop(to_echo);
        echo.await.map_err(io::Error::other)?;
        Ok(rate)
    })
}

/// A value to a second task and one back, through broadcast channels with
/// one receiver each.
fn ruio_broadcast(load: &Load) -> io::Result<f64> {
    use ruio::sync::broadcast;

    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let (to_echo, mut echo_rx) = broadcast::channel::<u32>(16);
        let (to_main, mut main_rx) = broadcast::channel::<u32>(16);
        let echo = ruio::spawn(async move {
            while let Ok(value) = echo_rx.recv().await {
                if to_main.send(value).is_err() {
                    break;
                }
            }
        });
        let rate = rate(measure, async || {
            to_echo.send(1).expect("the echo task runs");
            main_rx.recv().await.expect("the echo task answers");
        })
        .await;
        drop(to_echo);
        echo.await.map_err(io::Error::other)?;
        Ok(rate)
    })
}

fn tokio_broadcast(load: &Load) -> io::Result<f64> {
    use tokio::sync::broadcast;

    let measure = load.measure;
    tokio_runtime()?.block_on(async move {
        let (to_echo, mut echo_rx) = broadcast::channel::<u32>(16);
        let (to_main, mut main_rx) = broadcast::channel::<u32>(16);
        let echo = tokio::spawn(async move {
            while let Ok(value) = echo_rx.recv().await {
                if to_main.send(value).is_err() {
                    break;
                }
            }
        });
        let rate = rate(measure, async || {
            to_echo.send(1).expect("the echo task runs");
            main_rx.recv().await.expect("the echo task answers");
        })
        .await;
        drop(to_echo);
        echo.await.map_err(io::Error::other)?;
        Ok(rate)
    })
}

/// A value to a second task and one back, through watch channels: each way
/// a send, and a wait for the change.
fn ruio_watch(load: &Load) -> io::Result<f64> {
    use ruio::sync::watch;

    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let (to_echo, mut echo_rx) = watch::channel::<u32>(0);
        let (to_main, mut main_rx) = watch::channel::<u32>(0);
        let echo = ruio::spawn(async move {
            while echo_rx.changed().await.is_ok() {
                let value = *echo_rx.borrow_and_update();
                if to_main.send(value).is_err() {
                    break;
                }
            }
        });
        let rate = rate(measure, async || {
            to_echo.send(1).expect("the echo task runs");
            main_rx.changed().await.expect("the echo task answers");
            main_rx.borrow_and_update();
        })
        .await;
        drop(to_echo);
        echo.await.map_err(io::Error::other)?;
        Ok(rate)
    })
}

fn tokio_watch(load: &Load) -> io::Result<f64> {
    use tokio::sync::watch;

    let measure = load.measure;
    tokio_runtime()?.block_on(async move {
        let (to_echo, mut echo_rx) = watch::channel::<u32>(0);
        let (to_main, mut main_rx) = watch::channel::<u32>(0);
        let echo = tokio::spawn(async move {
            while echo_rx.changed().await.is_ok() {
                let value = *echo_rx.borrow_and_update();
                if to_main.send(value).is_err() {
                    break;
                }
            }
        });
        let rate = rate(measure, async || {
            to_echo.send(1).expect("the echo task runs");
            main_rx.changed().await.expect("the echo task answers");
            main_rx.borrow_and_update();
        })
        .await;
        drop(to_echo);
        echo.await.map_err(io::Error::other)?;
        Ok(rate)
    })
}

/// Two tasks meeting at a barrier, again and again. The other task is
/// aborted at the end, out of its wait.
fn ruio_barrier(load: &Load) -> io::Result<f64> {
    use ruio::sync::Barrier;

    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let barrier = Rc::new(Barrier::new(2));
        let other = {
            let barrier = Rc::clone(&barrier);
            ruio::spawn(async move {
                loop {
                    barrier.wait().await;
                }
            })
        };
        let rate = rate(measure, async || {
            barrier.wait().await;
        })
        .await;
        other.abort();
        let _ = other.await;
        Ok(rate)
    })
}

fn tokio_barrier(load: &Load) -> io::Result<f64> {
    use tokio::sync::Barrier;

    let measure = load.measure;
    tokio_runtime()?.block_on(async move {
        let barrier = Arc::new(Barrier::new(2));
        let other = {
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                loop {
                    barrier.wait().await;
                }
            })
        };
        let rate = rate(measure, async || {
            barrier.wait().await;
        })
        .await;
        other.abort();
        let _ = other.await;
        Ok(rate)
    })
}

/// Polls a wait for a token's cancellation once, which registers it, and
/// drops it, which takes it back.
fn ruio_cancel(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let token = ruio::sync::CancellationToken::new();
        Ok(rate(measure, async || {
            arm_and_drop(token.cancelled()).await;
        })
        .await)
    })
}

fn tokio_cancel(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    tokio_runtime()?.block_on(async move {
        let token = tokio_util::sync::CancellationToken::new();
        Ok(rate(measure, async || {
            arm_and_drop(token.cancelled()).await;
        })
        .await)
    })
}
