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
//! - `blocking`: a closure that does nothing, run on a thread of the
//!   blocking pool, and its output awaited: a job handed to a thread, and
//!   the task woken from it.
//! - `blocking spin`: the same on a pool of one thread that spins for its
//!   next job, pinned to another core, and a runtime that spins before it
//!   parks, so that neither side waits for the kernel to wake it.
//! - `read dir`: list a directory of 1000 entries: the batches of entries
//!   read on the blocking pool, and the entries taken from them.
//! - `resolve`: resolve `("127.0.0.1", 80)`, which parses, so costs no
//!   lookup and no pool.
//! - `copy`: 64 KiB chunks written into one connection as fast as it takes
//!   them, relayed into another by a task running `io::copy`, and read out
//!   of it a chunk at a time: the relay's reads and writes, the bytes twice
//!   through user space.
//! - `splice`: the same, the relay running `io::splice`: the bytes move
//!   through a pipe in the kernel and never reach user space.
//! - `sendfile`: a cached 1 MiB file spliced into a connection that a
//!   thread on another CPU drains, as a server sends a file; tokio copies
//!   it from its `File` with `io::copy`.
//! - `readiness`: write to an eventfd, wait for it to be readable through
//!   `AsyncFd`, one poll request a wait, and read it.
//! - `readiness stream`: the same, the waits on one multishot poll kept
//!   armed, so a wait submits nothing.
//! - `signal`: send the process a signal and wait for it to be received: the
//!   handler, the dispatcher and the listener's wake. ruio and tokio listen
//!   for different signals, so neither pays the other's handler.
//! - `timer`: arm a timer by polling a sleep once, then drop it.
//! - `sleep`: 1000 tasks each sleeping a millisecond at a time: timers that
//!   fire.
//! - `mutex`: lock and unlock an uncontended async mutex.
//! - `barrier`: two tasks meeting at a barrier, again and again: each round
//!   a wake and a run.
//! - `cancel`: arm a wait for a token's cancellation by polling it once,
//!   then drop it.

use std::cell::{Cell, RefCell};
use std::fmt;
use std::future::{Future, poll_fn};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
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
        name: "blocking",
        ruio: ruio_blocking,
        tokio: Some(tokio_blocking),
    },
    Micro {
        name: "blocking spin",
        ruio: ruio_blocking_spin,
        tokio: None,
    },
    Micro {
        name: "read dir",
        ruio: ruio_read_dir,
        tokio: Some(tokio_read_dir),
    },
    Micro {
        name: "resolve",
        ruio: ruio_resolve,
        tokio: Some(tokio_resolve),
    },
    Micro {
        name: "copy",
        ruio: ruio_copy,
        tokio: Some(tokio_copy),
    },
    Micro {
        name: "splice",
        ruio: ruio_splice,
        tokio: None,
    },
    Micro {
        name: "sendfile",
        ruio: ruio_sendfile,
        tokio: Some(tokio_sendfile),
    },
    Micro {
        name: "readiness",
        ruio: ruio_readiness,
        tokio: Some(tokio_readiness),
    },
    Micro {
        name: "readiness stream",
        ruio: ruio_readiness_stream,
        tokio: None,
    },
    Micro {
        name: "signal",
        ruio: ruio_signal,
        tokio: Some(tokio_signal),
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

/// A closure that does nothing, run on a thread of the blocking pool, and
/// its output awaited.
fn ruio_blocking(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        Ok(rate(measure, async || {
            ruio::spawn_blocking(|| {}).await.expect("the job ran");
        })
        .await)
    })
}

fn tokio_blocking(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    tokio_runtime()?.block_on(async move {
        Ok(rate(measure, async || {
            tokio::task::spawn_blocking(|| {})
                .await
                .expect("the job ran");
        })
        .await)
    })
}

/// The same on a pool of one thread that spins for its next job, pinned to
/// another core, and a runtime that spins before it parks.
fn ruio_blocking_spin(load: &Load) -> io::Result<f64> {
    let (measure, cpu) = (load.measure, remote_cpu(load));
    let budget = Duration::from_micros(100);
    let runtime = ruio::Runtime::builder().spin(budget).build()?;
    runtime.block_on(async move {
        let pool = ruio::blocking::Pool::builder()
            .threads(1)
            .spin(budget)
            .build();
        // The one thread, pinned by its first job; not pinned if the host has
        // no such CPU.
        pool.spawn(move || {
            let _ = sys::pin(&[cpu]);
        })
        .await
        .expect("the job ran");
        Ok(rate(measure, async || {
            pool.spawn(|| {}).await.expect("the job ran");
        })
        .await)
    })
}

/// How many entries the directory `read dir` lists has.
const DIR_ENTRIES: usize = 1000;

/// A directory of [`DIR_ENTRIES`] empty files, removed when dropped.
struct Listed(std::path::PathBuf);

impl Listed {
    fn new() -> io::Result<Self> {
        let dir = std::env::temp_dir().join(format!("ruio-bench-read-dir-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir(&dir)?;
        for index in 0..DIR_ENTRIES {
            std::fs::File::create(dir.join(format!("entry-{index:05}")))?;
        }
        Ok(Self(dir))
    }
}

impl Drop for Listed {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Lists a directory of [`DIR_ENTRIES`] entries: opened, read in batches on
/// the blocking pool, every entry taken.
fn ruio_read_dir(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let listed = Listed::new()?;
    ruio_runtime()?.block_on(async move {
        let dir = &listed.0;
        Ok(rate(measure, async || {
            let mut entries = ruio::fs::read_dir(dir).await.expect("the directory opens");
            let mut count = 0;
            while let Some(entry) = entries.next_entry().await.expect("the directory reads") {
                count += entry.file_name().len();
            }
            assert!(count > 0, "the directory has entries");
        })
        .await)
    })
}

fn tokio_read_dir(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let listed = Listed::new()?;
    tokio_runtime()?.block_on(async move {
        let dir = &listed.0;
        Ok(rate(measure, async || {
            let mut entries = tokio::fs::read_dir(dir).await.expect("the directory opens");
            let mut count = 0;
            while let Some(entry) = entries.next_entry().await.expect("the directory reads") {
                count += entry.file_name().len();
            }
            assert!(count > 0, "the directory has entries");
        })
        .await)
    })
}

/// Resolves a host and port that parse as an address: no lookup, and no
/// thread but the task's.
fn ruio_resolve(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        Ok(rate(measure, async || {
            let mut addrs = ruio::net::lookup_host(("127.0.0.1", 80))
                .await
                .expect("a literal resolves");
            assert!(addrs.next().is_some());
        })
        .await)
    })
}

fn tokio_resolve(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    tokio_runtime()?.block_on(async move {
        Ok(rate(measure, async || {
            let mut addrs = tokio::net::lookup_host(("127.0.0.1", 80))
                .await
                .expect("a literal resolves");
            assert!(addrs.next().is_some());
        })
        .await)
    })
}

/// How many bytes a `copy` or `splice` step relays.
const RELAY_CHUNK: usize = 64 << 10;

/// How large the file `sendfile` sends is.
const SENT_FILE: usize = 1 << 20;

/// A connected pair of ruio TCP streams on the loopback interface.
async fn ruio_pair() -> io::Result<(ruio::net::TcpStream, ruio::net::TcpStream)> {
    let listener = ruio::net::TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let client = ruio::spawn(ruio::net::TcpStream::connect(addr));
    let (server, _) = listener.accept().await?;
    Ok((client.await.map_err(io::Error::other)??, server))
}

/// A connected pair of tokio TCP streams on the loopback interface.
async fn tokio_pair() -> io::Result<(tokio::net::TcpStream, tokio::net::TcpStream)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let client = tokio::spawn(tokio::net::TcpStream::connect(addr));
    let (server, _) = listener.accept().await?;
    Ok((client.await.map_err(io::Error::other)??, server))
}

/// Chunks written into one connection as fast as it takes them, relayed
/// into another by `relay`, and read out of it: a step is a chunk out.
fn ruio_relay<F>(load: &Load, relay: F) -> io::Result<f64>
where
    F: FnOnce(ruio::net::TcpStream, ruio::net::TcpStream) -> ruio::JoinHandle<io::Result<u64>>,
{
    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let (feeder, relay_in) = ruio_pair().await?;
        let (relay_out, sink) = ruio_pair().await?;
        let relay = relay(relay_in, relay_out);
        let stop = Rc::new(Cell::new(false));
        let feed = ruio::spawn({
            let stop = stop.clone();
            async move {
                let mut chunk = vec![7; RELAY_CHUNK];
                while !stop.get() {
                    let (written, buf) = feeder.write_all(chunk).await;
                    chunk = buf;
                    if written.is_err() {
                        break;
                    }
                }
            }
        });
        let back = RefCell::new(Some(Vec::with_capacity(RELAY_CHUNK)));
        let rate = rate(measure, async || {
            let mut buf = back.borrow_mut().take().expect("the buffer is back");
            buf.clear();
            let (read, buf) = sink.read_exact(buf).await;
            read.expect("the relay hands chunks on");
            *back.borrow_mut() = Some(buf);
        })
        .await;
        // The feeder stops after the chunk it is on, which the sink takes,
        // and the relay ends with the feeder's connection.
        stop.set(true);
        let mut buf = back.into_inner().expect("the buffer is back");
        loop {
            buf.clear();
            let (read, taken) = sink.read(buf).await;
            buf = taken;
            if read? == 0 {
                break;
            }
        }
        feed.await.map_err(io::Error::other)?;
        relay.await.map_err(io::Error::other)??;
        Ok(rate)
    })
}

/// Chunks relayed from one connection to another through `io::copy`.
fn ruio_copy(load: &Load) -> io::Result<f64> {
    ruio_relay(load, |relay_in, relay_out| {
        ruio::spawn(async move { ruio::io::copy(&relay_in, &relay_out).await })
    })
}

/// Chunks relayed from one connection to another through `io::splice`.
fn ruio_splice(load: &Load) -> io::Result<f64> {
    ruio_relay(load, |relay_in, relay_out| {
        ruio::spawn(async move { ruio::io::splice(&relay_in, &relay_out).await })
    })
}

fn tokio_copy(load: &Load) -> io::Result<f64> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let measure = load.measure;
    tokio_runtime()?.block_on(async move {
        let (mut feeder, mut relay_in) = tokio_pair().await?;
        let (mut relay_out, mut sink) = tokio_pair().await?;
        let relay =
            tokio::spawn(async move { tokio::io::copy(&mut relay_in, &mut relay_out).await });
        let stop = Arc::new(AtomicBool::new(false));
        let feed = tokio::spawn({
            let stop = stop.clone();
            async move {
                let chunk = vec![7; RELAY_CHUNK];
                while !stop.load(Ordering::Relaxed) && feeder.write_all(&chunk).await.is_ok() {}
            }
        });
        let mut back = vec![0; RELAY_CHUNK];
        let rate = rate(measure, async || {
            sink.read_exact(&mut back)
                .await
                .expect("the relay hands chunks on");
        })
        .await;
        stop.store(true, Ordering::Relaxed);
        while sink.read(&mut back).await? > 0 {}
        feed.await.map_err(io::Error::other)?;
        relay.await.map_err(io::Error::other)??;
        Ok(rate)
    })
}

/// A file of [`SENT_FILE`] bytes, removed when dropped.
struct Served(std::path::PathBuf);

impl Served {
    fn new() -> io::Result<Self> {
        let path = std::env::temp_dir().join(format!("ruio-bench-sendfile-{}", std::process::id()));
        std::fs::write(&path, vec![9; SENT_FILE])?;
        Ok(Self(path))
    }
}

impl Drop for Served {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// A thread on another CPU that reads everything `sink` has, until it
/// closes; what a client of the file server would do elsewhere.
fn drain_on_thread(sink: std::net::TcpStream, cpu: usize) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        use std::io::Read;

        let _ = sys::pin(&[cpu]);
        let mut sink = sink;
        let mut buf = vec![0; SENT_FILE];
        while sink.read(&mut buf).map(|read| read > 0).unwrap_or(false) {}
    })
}

/// A cached file spliced into a connection, whole, as a server sends it.
fn ruio_sendfile(load: &Load) -> io::Result<f64> {
    let (measure, cpu) = (load.measure, remote_cpu(load));
    let served = Served::new()?;
    ruio_runtime()?.block_on(async move {
        let file = ruio::fs::File::open(&served.0).await?;
        let listener = ruio::net::TcpListener::bind("127.0.0.1:0")?;
        let sink = std::net::TcpStream::connect(listener.local_addr()?)?;
        let (sender, _) = listener.accept().await?;
        let drain = drain_on_thread(sink, cpu);
        let rate = rate(measure, async || {
            let sent = ruio::io::splice(&file, &sender)
                .offset_in(0)
                .limit(SENT_FILE as u64)
                .await
                .expect("the file splices");
            assert_eq!(sent, SENT_FILE as u64);
        })
        .await;
        // The socket closes through the ring, which runs only while this
        // task waits; the shutdown's end-of-file lets the thread go first.
        sender.shutdown(std::net::Shutdown::Write).await?;
        drain
            .join()
            .map_err(|_| io::Error::other("the thread panicked"))?;
        Ok(rate)
    })
}

fn tokio_sendfile(load: &Load) -> io::Result<f64> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    let (measure, cpu) = (load.measure, remote_cpu(load));
    let served = Served::new()?;
    tokio_runtime()?.block_on(async move {
        let mut file = tokio::fs::File::open(&served.0).await?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let sink = std::net::TcpStream::connect(listener.local_addr()?)?;
        let (mut sender, _) = listener.accept().await?;
        let drain = drain_on_thread(sink, cpu);
        let rate = rate(measure, async || {
            file.seek(io::SeekFrom::Start(0))
                .await
                .expect("the file seeks");
            let sent = tokio::io::copy(&mut (&mut file).take(SENT_FILE as u64), &mut sender)
                .await
                .expect("the file copies");
            assert_eq!(sent, SENT_FILE as u64);
        })
        .await;
        drop(sender);
        drain
            .join()
            .map_err(|_| io::Error::other("the thread panicked"))?;
        Ok(rate)
    })
}

/// An eventfd, nonblocking, whose count a write adds to and a read takes.
fn eventfd() -> io::Result<OwnedFd> {
    // SAFETY: `eventfd` makes a new descriptor, which nothing else owns.
    let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the descriptor is new, and owned by nobody else.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Adds one to an eventfd's count.
fn signal_eventfd(fd: RawFd) {
    let one: u64 = 1;
    // SAFETY: eight bytes of a `u64`, which an eventfd write takes.
    let written = unsafe { libc::write(fd, (&raw const one).cast(), 8) };
    assert_eq!(written, 8, "the eventfd takes the write");
}

/// Takes an eventfd's count, which resets it.
fn drain_eventfd(fd: RawFd) {
    let mut count: u64 = 0;
    // SAFETY: eight bytes of room, which an eventfd read fills.
    let read = unsafe { libc::read(fd, (&raw mut count).cast(), 8) };
    assert_eq!(read, 8, "the eventfd has a count");
}

/// Writes to an eventfd, waits for it to be readable, with one poll request,
/// and reads it.
fn ruio_readiness(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let event = ruio::io::AsyncFd::new(eventfd()?);
    ruio_runtime()?.block_on(async move {
        let fd = event.as_raw_fd();
        Ok(rate(measure, async || {
            signal_eventfd(fd);
            event.readable().await.expect("the eventfd is polled");
            drain_eventfd(fd);
        })
        .await)
    })
}

/// The same, the waits on one multishot poll kept armed.
fn ruio_readiness_stream(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let event = ruio::io::AsyncFd::new(eventfd()?);
    ruio_runtime()?.block_on(async move {
        let fd = event.as_raw_fd();
        let mut readiness = event.readiness(libc::POLLIN as u32);
        Ok(rate(measure, async || {
            signal_eventfd(fd);
            readiness.next().await.expect("the eventfd is polled");
            drain_eventfd(fd);
        })
        .await)
    })
}

fn tokio_readiness(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let event = eventfd()?;
    tokio_runtime()?.block_on(async move {
        let fd = event.as_raw_fd();
        let event = tokio::io::unix::AsyncFd::new(event)?;
        Ok(rate(measure, async || {
            signal_eventfd(fd);
            let mut guard = event.readable().await.expect("the eventfd is polled");
            drain_eventfd(fd);
            // As a driver that read until `WouldBlock` would leave it, so
            // that the next wait is a wait.
            guard.clear_ready();
        })
        .await)
    })
}

/// Sends the process `signum`.
fn raise(signum: libc::c_int) {
    // SAFETY: `raise` of a signal the runtime has a handler for.
    let raised = unsafe { libc::raise(signum) };
    assert_eq!(raised, 0, "the signal is sent");
}

/// Sends the process `SIGUSR1` and waits for a listener to receive it.
fn ruio_signal(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let mut listener = ruio::signal::signal(ruio::signal::SignalKind::user_defined1())?;
        Ok(rate(measure, async || {
            raise(libc::SIGUSR1);
            listener.recv().await.expect("the listener is open");
        })
        .await)
    })
}

/// Sends the process `SIGUSR2` and waits for tokio's listener to receive it.
fn tokio_signal(load: &Load) -> io::Result<f64> {
    use tokio::signal::unix::{SignalKind, signal};

    let measure = load.measure;
    tokio_runtime()?.block_on(async move {
        let mut listener = signal(SignalKind::user_defined2())?;
        Ok(rate(measure, async || {
            raise(libc::SIGUSR2);
            listener.recv().await.expect("the listener is open");
        })
        .await)
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
