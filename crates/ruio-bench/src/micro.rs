//! Microbenchmarks of the runtime itself: what a turn of the loop, a task, a
//! wake, a timer or a channel costs in user space, apart from the kernel's
//! work on sockets and files, which the other benchmarks measure. Each is a
//! loop of one small step, and the result is steps per second: on ruio, and,
//! where tokio has the same thing, on tokio's current-thread runtime.
//!
//! - `nop 1`: one task awaiting no-ops, one at a time. Each is a turn of the
//!   loop: queue, enter the kernel and wait, deliver, wake, run.
//! - `nop 64`: 64 tasks awaiting no-ops, so that a turn delivers a batch.
//! - `portable nop 1`, `portable nop 64`: the same through `PortableOp`,
//!   which finds its ring as the thread's current driver, and whose future
//!   is `Send`: what the difference from the two above costs.
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
//! - `stream next`: the `unbounded channel` round trip, both receivers
//!   driven through futures-util's `StreamExt::next` rather than their own
//!   `recv`: what the `Stream` trait costs over the method, which is
//!   nothing if the row reads as `unbounded channel` does.
//! - `drain`: a thousand values sent into an unbounded channel and taken out
//!   with `recv`, one after another.
//! - `stream fold`: the same thousand, taken out through
//!   `StreamExt::take(1000).fold`: the combinators over the trait against
//!   the loop over the method.
//! - `sockopt`: set `TCP_NODELAY` on a plain stream, on and off in turn: a
//!   `setsockopt(2)` on either runtime.
//! - `sockopt direct`: the same on a direct descriptor, which only the ring
//!   can set an option on: a request the kernel completes as it is
//!   submitted.
//! - `write read`: a byte written to the peer and read back from it, on a
//!   direct pair on ruio and a plain one on tokio: what the next two add
//!   their option to.
//! - `sockopt direct in turn`: set the option on the direct stream, write
//!   the byte and read it, each awaited before the next.
//! - `sockopt direct batched`: the same, the option and the write polled
//!   together, so that one submission takes both to the kernel: the option
//!   costs no system call of its own, as it costs none in a loaded runtime,
//!   whose turn submits every task's requests together. tokio's stream is
//!   plain, and its option a system call, in both rows.
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
//! - `spawn on`: a task that does nothing, spawned onto a second core and
//!   awaited: the task handed over, the core woken, and the awaiting task
//!   woken back from it. On tokio, two current-thread runtimes on the two
//!   cores, the one spawning onto the other through its `Handle`.
//! - `spawn on spin`: the same with both cores spinning before they park,
//!   so that neither waits for the kernel to wake it.
//! - `abort on`: a task that never finishes, spawned onto the second core
//!   and aborted at once through its abort handle, and the cancellation
//!   awaited.
//! - `join set on`: a task that does nothing, spawned onto the second core
//!   through a join set, and its output taken from the set.
//! - `read dir`: list a directory of 1000 entries: the batches of entries
//!   read on the blocking pool, and the entries taken from them.
//! - `metadata`: the status of a file by path: statx through the ring,
//!   which the kernel runs in an io-wq worker; tokio's `stat` on its
//!   blocking pool.
//! - `read file`: a 4 KiB file read whole by path with `fs::read`: opened,
//!   read, read to its end and closed, each on the ring of the thread that
//!   polls it, in a future that is `Send`; tokio's on its blocking pool.
//! - `write file`: 4 KiB written to a file by path with `fs::write`, which
//!   makes or empties it, and closes it: the open, which creates and
//!   truncates, in an io-wq worker; tokio's on its blocking pool. On tmpfs,
//!   where the host has it at `/dev/shm`: ext4 writes back a file emptied
//!   and written again as it is closed, which takes a millisecond on either
//!   runtime, and would be all that is measured.
//! - `symlink`: a symbolic link made and removed: two operations through
//!   the ring, each in an io-wq worker; tokio's two calls on its blocking
//!   pool.
//! - `hard link`: a hard link made and removed, likewise.
//! - `read link`: where a symbolic link points: `readlink` on the blocking
//!   pool of either runtime, as io_uring has no operation for it.
//! - `fallocate`: 4 KiB of a file allocated and the hole punched back: two
//!   operations through the ring, in io-wq workers; tokio has no fallocate.
//! - `advise`: `POSIX_FADV_SEQUENTIAL` for a file, which sets a mode and so
//!   completes on the ring without a worker; tokio has no fadvise.
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
//! - `recv from`: a 64-byte datagram from a client to an echo task on the
//!   same runtime, which takes it with `recv_from`, one request a datagram,
//!   and sends it back to its sender with `send_to`, and the echo received:
//!   a request and its answer, each way a send and a receive. tokio's
//!   receives wait for readiness, and each send and receive is a system
//!   call.
//! - `recv from stream`: the same, the echo task taking the datagrams and
//!   their senders from `recv_from_stream`, one multishot request kept
//!   armed.
//! - `datagrams in`: a thread on another core fills a socket's receive
//!   buffer with 128 datagrams of 64 bytes, and the runtime takes them with
//!   `recv_from`, one request a datagram; again and again, only the taking
//!   timed: what one socket on one core takes in a second, each datagram's
//!   sender included, apart from the sending. The skbs come from the other
//!   core, as a network card's interrupts would bring them, and the
//!   kernel's work on them is most of either runtime's.
//! - `datagrams in stream`: the same through `recv_from_stream`.
//! - `datagrams in without senders`: the same through `recv_stream`, a
//!   multishot receive, which leaves the senders out, and tokio's `recv`:
//!   what the addresses cost.
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
//! - `spawn wait`: `/bin/true` spawned and waited for: `posix_spawn(3)`, a
//!   pidfd, and a wait the kernel completes on the ring as the child exits,
//!   then the reap. The child inherits the runtime's CPU, so the parent is
//!   descheduled while it runs, on either runtime.
//! - `spawn output`: `/bin/echo` spawned, its output read from a pipe to
//!   the end, and waited for.
//! - `child pipes`: 64 bytes written to the input of a `cat` that runs the
//!   whole time and read back from its output: each way a write, a read and
//!   a wake of the other process, on the one CPU they share.
//! - `portable write read`: `write read` on a pair of portable streams: a
//!   `send(2)` and a `recv(2)` made at once, which find room and the byte,
//!   so that the ring is never entered.
//! - `round trip`: 64 bytes over TCP to an echo thread on another core and
//!   back, through a stream of the ring's on ruio: a write and a read, and a
//!   wait for the echo.
//! - `portable round trip`: the same through a portable stream: a send made
//!   at once, a receive that finds nothing yet and waits with a poll on the
//!   ring, and the receive made again as the echo arrives.
//! - `foreign nop`: a portable no-op polled on a plain thread, where no
//!   runtime runs, as `futures::executor::block_on` polls one: its entry
//!   goes to the foreign ring, a runtime's on another core, whose thread
//!   queues it and sends the completion back; each way a wake of the other
//!   thread. tokio has no such thing.
//! - `foreign round trip`: `portable round trip` polled by tokio's
//!   current-thread runtime, whose thread runs no ruio runtime: each wait
//!   goes to the foreign ring, a ruio runtime's on another core, and comes
//!   back as a wake from it; against tokio's own stream.
//! - `hyper get`: hyper-util's client sending GETs one at a time over one
//!   pooled connection to a server thread on another core, which answers
//!   each with five bytes: ruio's through `ruio_compat::client`'s connector
//!   and portable streams, tokio's through its `HttpConnector`, both with
//!   `TCP_NODELAY`.
//! - `hyper get 16`: the same from 16 tasks at once through one client and
//!   its pool, the server a thread a connection over eight other cores.

use std::cell::{Cell, RefCell};
use std::fmt;
use std::future::{Future, poll_fn};
use std::hint::black_box;
use std::io;
use std::mem;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::pin::pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc as std_mpsc};
use std::task::Poll;
use std::thread;
use std::time::{Duration, Instant};

use ruio::uring::PortableOp;
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
        name: "portable nop 1",
        ruio: |load| ruio_portable_nop(load, 1),
        tokio: None,
    },
    Micro {
        name: "portable nop 64",
        ruio: |load| ruio_portable_nop(load, 64),
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
        name: "stream next",
        ruio: ruio_stream_next,
        tokio: Some(tokio_stream_next),
    },
    Micro {
        name: "drain",
        ruio: ruio_drain,
        tokio: Some(tokio_drain),
    },
    Micro {
        name: "stream fold",
        ruio: ruio_stream_fold,
        tokio: Some(tokio_stream_fold),
    },
    Micro {
        name: "sockopt",
        ruio: ruio_sockopt,
        tokio: Some(tokio_sockopt),
    },
    Micro {
        name: "sockopt direct",
        ruio: ruio_sockopt_direct,
        tokio: None,
    },
    Micro {
        name: "write read",
        ruio: ruio_write_read,
        tokio: Some(tokio_write_read),
    },
    Micro {
        name: "sockopt direct in turn",
        ruio: ruio_sockopt_in_turn,
        tokio: Some(tokio_sockopt_in_turn),
    },
    Micro {
        name: "sockopt direct batched",
        ruio: ruio_sockopt_batched,
        tokio: Some(tokio_sockopt_in_turn),
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
        name: "spawn on",
        ruio: ruio_spawn_on,
        tokio: Some(tokio_spawn_on),
    },
    Micro {
        name: "spawn on spin",
        ruio: ruio_spawn_on_spin,
        tokio: None,
    },
    Micro {
        name: "abort on",
        ruio: ruio_abort_on,
        tokio: Some(tokio_abort_on),
    },
    Micro {
        name: "join set on",
        ruio: ruio_join_set_on,
        tokio: Some(tokio_join_set_on),
    },
    Micro {
        name: "read dir",
        ruio: ruio_read_dir,
        tokio: Some(tokio_read_dir),
    },
    Micro {
        name: "metadata",
        ruio: ruio_metadata,
        tokio: Some(tokio_metadata),
    },
    Micro {
        name: "read file",
        ruio: ruio_read_file,
        tokio: Some(tokio_read_file),
    },
    Micro {
        name: "write file",
        ruio: ruio_write_file,
        tokio: Some(tokio_write_file),
    },
    Micro {
        name: "symlink",
        ruio: ruio_symlink,
        tokio: Some(tokio_symlink),
    },
    Micro {
        name: "hard link",
        ruio: ruio_hard_link,
        tokio: Some(tokio_hard_link),
    },
    Micro {
        name: "read link",
        ruio: ruio_read_link,
        tokio: Some(tokio_read_link),
    },
    Micro {
        name: "fallocate",
        ruio: ruio_fallocate,
        tokio: None,
    },
    Micro {
        name: "advise",
        ruio: ruio_advise,
        tokio: None,
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
        name: "recv from",
        ruio: ruio_recv_from,
        tokio: Some(tokio_recv_from),
    },
    Micro {
        name: "recv from stream",
        ruio: ruio_recv_from_stream,
        tokio: Some(tokio_recv_from),
    },
    Micro {
        name: "datagrams in",
        ruio: ruio_datagrams_in,
        tokio: Some(tokio_datagrams_in),
    },
    Micro {
        name: "datagrams in stream",
        ruio: ruio_datagrams_in_stream,
        tokio: Some(tokio_datagrams_in),
    },
    Micro {
        name: "datagrams in without senders",
        ruio: ruio_datagrams_in_without_senders,
        tokio: Some(tokio_datagrams_in_without_senders),
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
    Micro {
        name: "spawn wait",
        ruio: ruio_spawn_wait,
        tokio: Some(tokio_spawn_wait),
    },
    Micro {
        name: "spawn output",
        ruio: ruio_spawn_output,
        tokio: Some(tokio_spawn_output),
    },
    Micro {
        name: "child pipes",
        ruio: ruio_child_pipes,
        tokio: Some(tokio_child_pipes),
    },
    Micro {
        name: "portable write read",
        ruio: ruio_portable_write_read,
        tokio: Some(tokio_write_read),
    },
    Micro {
        name: "round trip",
        ruio: ruio_round_trip,
        tokio: Some(tokio_round_trip),
    },
    Micro {
        name: "portable round trip",
        ruio: ruio_portable_round_trip,
        tokio: Some(tokio_round_trip),
    },
    Micro {
        name: "foreign nop",
        ruio: foreign_nop,
        tokio: None,
    },
    Micro {
        name: "foreign round trip",
        ruio: foreign_round_trip,
        tokio: Some(tokio_round_trip),
    },
    Micro {
        name: "hyper get",
        ruio: ruio_hyper_get,
        tokio: Some(tokio_hyper_get),
    },
    Micro {
        name: "hyper get 16",
        ruio: ruio_hyper_get_many,
        tokio: Some(tokio_hyper_get_many),
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

/// The CPU the runtime whose ring is the foreign one runs on, in the
/// `foreign` rows: a third core, beside the measured thread's and the
/// peer's.
fn foreign_cpu(load: &Load) -> usize {
    load.cpu + 4
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

/// `depth` tasks each awaiting portable no-ops one at a time: `ruio_nop`'s
/// loop, each no-op a `PortableOp`.
fn ruio_portable_nop(load: &Load, depth: usize) -> io::Result<f64> {
    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let stop = Rc::new(Cell::new(false));
        let tasks: Vec<_> = (0..depth)
            .map(|_| {
                let stop = Rc::clone(&stop);
                ruio::spawn(async move {
                    let mut count = 0;
                    while !stop.get() {
                        PortableOp::new(Nop).await?;
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

/// Runs `measure` on the first of two cores, the runtime's CPU and the
/// remote one, the second serving what the first spawns onto it, and
/// returns the rate it measured. On a host with no such second CPU, both
/// cores share the runtime's.
fn on_two_cores<Fut>(
    load: &Load,
    builder: ruio::Builder,
    measure: impl Fn() -> Fut + Sync,
) -> io::Result<f64>
where
    Fut: Future<Output = f64>,
{
    let run = |remote: usize| {
        ruio::Cores::cpus([load.cpu, remote])
            .builder(builder.clone())
            .run(|core| {
                let measure = &measure;
                async move {
                    if core.index() == 0 {
                        measure().await
                    } else {
                        0.0
                    }
                }
            })
    };
    let rates = match run(remote_cpu(load)) {
        // A core that cannot be pinned keeps the run from starting, so
        // nothing has been measured yet.
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => run(load.cpu)?,
        rates => rates?,
    };
    Ok(rates[0])
}

/// A task that does nothing, spawned onto the second core and awaited.
fn ruio_spawn_on(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    on_two_cores(load, ruio::Runtime::builder(), || async move {
        rate(measure, async || {
            ruio::spawn_on(1, async {}).await.expect("the task ran");
        })
        .await
    })
}

/// The same with both cores spinning before they park.
fn ruio_spawn_on_spin(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let builder = ruio::Runtime::builder().spin(Duration::from_micros(100));
    on_two_cores(load, builder, || async move {
        rate(measure, async || {
            ruio::spawn_on(1, async {}).await.expect("the task ran");
        })
        .await
    })
}

/// A task that never finishes, spawned onto the second core, aborted through
/// its abort handle, and awaited for the cancellation.
fn ruio_abort_on(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    on_two_cores(load, ruio::Runtime::builder(), || async move {
        rate(measure, async || {
            let task = ruio::spawn_on(1, std::future::pending::<()>());
            task.abort_handle().abort();
            let err = task.await.expect_err("the task was aborted");
            assert!(err.is_cancelled());
        })
        .await
    })
}

/// A task that does nothing, spawned onto the second core through a join set
/// and taken out of it.
fn ruio_join_set_on(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    on_two_cores(load, ruio::Runtime::builder(), || async move {
        let mut set = ruio::JoinSet::new();
        rate(measure, async || {
            set.spawn_on(1, async {});
            set.join_next()
                .await
                .expect("a task is in the set")
                .expect("the task ran");
        })
        .await
    })
}

/// A second current-thread runtime of tokio's, on a thread pinned to the
/// remote CPU, which runs until this is dropped.
struct TokioRemote {
    handle: tokio::runtime::Handle,
    /// Dropped to end the runtime's `block_on`.
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl TokioRemote {
    fn start(load: &Load) -> io::Result<Self> {
        let cpu = remote_cpu(load);
        let (ready, started) = std_mpsc::channel();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let thread = thread::spawn(move || {
            // Not pinned if the host has no such CPU.
            let _ = sys::pin(&[cpu]);
            let runtime = match tokio_runtime() {
                Ok(runtime) => runtime,
                Err(err) => {
                    let _ = ready.send(Err(err));
                    return;
                }
            };
            let _ = ready.send(Ok(runtime.handle().clone()));
            runtime.block_on(async {
                let _ = stopped.await;
            });
        });
        let handle = started
            .recv()
            .map_err(|_| io::Error::other("the remote runtime's thread ended"))??;
        Ok(Self {
            handle,
            stop: Some(stop),
            thread: Some(thread),
        })
    }
}

impl Drop for TokioRemote {
    fn drop(&mut self) {
        drop(self.stop.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn tokio_spawn_on(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let remote = TokioRemote::start(load)?;
    let handle = remote.handle.clone();
    tokio_runtime()?.block_on(async move {
        Ok(rate(measure, async || {
            handle.spawn(async {}).await.expect("the task ran");
        })
        .await)
    })
}

fn tokio_abort_on(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let remote = TokioRemote::start(load)?;
    let handle = remote.handle.clone();
    tokio_runtime()?.block_on(async move {
        Ok(rate(measure, async || {
            let task = handle.spawn(std::future::pending::<()>());
            task.abort_handle().abort();
            let err = task.await.expect_err("the task was aborted");
            assert!(err.is_cancelled());
        })
        .await)
    })
}

fn tokio_join_set_on(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let remote = TokioRemote::start(load)?;
    let handle = remote.handle.clone();
    tokio_runtime()?.block_on(async move {
        let mut set = tokio::task::JoinSet::new();
        Ok(rate(measure, async || {
            set.spawn_on(async {}, &handle);
            set.join_next()
                .await
                .expect("a task is in the set")
                .expect("the task ran");
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

/// A directory of a file-system benchmark's own, with a five-byte file and
/// a symbolic link to it in it, removed when dropped.
struct Named {
    dir: std::path::PathBuf,
    file: std::path::PathBuf,
    link: std::path::PathBuf,
}

impl Named {
    fn new(name: &str) -> io::Result<Self> {
        Self::new_in(&std::env::temp_dir(), name)
    }

    /// The directory, made in `base`.
    fn new_in(base: &std::path::Path, name: &str) -> io::Result<Self> {
        let dir = base.join(format!("ruio-bench-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir(&dir)?;
        let file = dir.join("file");
        std::fs::write(&file, b"named")?;
        let link = dir.join("link");
        std::os::unix::fs::symlink("file", &link)?;
        Ok(Self { dir, file, link })
    }
}

/// Where `write file` writes: tmpfs, if the host has one at `/dev/shm`, so
/// that the writes are measured rather than the disk's write-back.
fn tmpfs_dir() -> std::path::PathBuf {
    let shm = std::path::Path::new("/dev/shm");
    if shm.is_dir() {
        shm.to_path_buf()
    } else {
        std::env::temp_dir()
    }
}

impl Drop for Named {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// The status of a file by path: statx through the ring, which the kernel
/// runs in an io-wq worker.
fn ruio_metadata(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let named = Named::new("metadata")?;
    ruio_runtime()?.block_on(async move {
        let path = &named.file;
        Ok(rate(measure, async || {
            let found = ruio::fs::metadata(path).await.expect("the file is there");
            assert_eq!(found.len(), 5);
        })
        .await)
    })
}

fn tokio_metadata(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let named = Named::new("metadata")?;
    tokio_runtime()?.block_on(async move {
        let path = &named.file;
        Ok(rate(measure, async || {
            let found = tokio::fs::metadata(path).await.expect("the file is there");
            assert_eq!(found.len(), 5);
        })
        .await)
    })
}

/// How many bytes `read file` and `write file` move.
const SMALL_FILE: usize = 4096;

/// What `write file` writes: borrowed for as long as the program runs, which
/// ruio writes as it is, and tokio copies to write on its pool.
static SMALL_DATA: [u8; SMALL_FILE] = [7; SMALL_FILE];

/// A 4 KiB file read whole by path.
fn ruio_read_file(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let named = Named::new("read-file")?;
    std::fs::write(&named.file, SMALL_DATA)?;
    ruio_runtime()?.block_on(async move {
        let path = &named.file;
        Ok(rate(measure, async || {
            let data = ruio::fs::read(path).await.expect("the file reads");
            assert_eq!(data.len(), SMALL_FILE);
        })
        .await)
    })
}

fn tokio_read_file(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let named = Named::new("read-file")?;
    std::fs::write(&named.file, SMALL_DATA)?;
    tokio_runtime()?.block_on(async move {
        let path = &named.file;
        Ok(rate(measure, async || {
            let data = tokio::fs::read(path).await.expect("the file reads");
            assert_eq!(data.len(), SMALL_FILE);
        })
        .await)
    })
}

/// 4 KiB written to a file by path, which is made the first time and
/// emptied each time after.
fn ruio_write_file(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let named = Named::new_in(&tmpfs_dir(), "write-file")?;
    ruio_runtime()?.block_on(async move {
        let path = named.dir.join("written");
        Ok(rate(measure, async || {
            ruio::fs::write(&path, &SMALL_DATA[..])
                .await
                .expect("the file is written");
        })
        .await)
    })
}

fn tokio_write_file(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let named = Named::new_in(&tmpfs_dir(), "write-file")?;
    tokio_runtime()?.block_on(async move {
        let path = named.dir.join("written");
        Ok(rate(measure, async || {
            tokio::fs::write(&path, &SMALL_DATA[..])
                .await
                .expect("the file is written");
        })
        .await)
    })
}

/// A symbolic link made and removed: two operations through the ring, each
/// in an io-wq worker.
fn ruio_symlink(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let named = Named::new("symlink")?;
    ruio_runtime()?.block_on(async move {
        let link = named.dir.join("made");
        Ok(rate(measure, async || {
            ruio::fs::symlink("file", &link)
                .await
                .expect("the link is made");
            ruio::fs::remove_file(&link)
                .await
                .expect("the link is removed");
        })
        .await)
    })
}

fn tokio_symlink(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let named = Named::new("symlink")?;
    tokio_runtime()?.block_on(async move {
        let link = named.dir.join("made");
        Ok(rate(measure, async || {
            tokio::fs::symlink("file", &link)
                .await
                .expect("the link is made");
            tokio::fs::remove_file(&link)
                .await
                .expect("the link is removed");
        })
        .await)
    })
}

/// A hard link made and removed, as [`ruio_symlink`] makes a symbolic one.
fn ruio_hard_link(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let named = Named::new("hard-link")?;
    ruio_runtime()?.block_on(async move {
        let link = named.dir.join("made");
        Ok(rate(measure, async || {
            ruio::fs::hard_link(&named.file, &link)
                .await
                .expect("the link is made");
            ruio::fs::remove_file(&link)
                .await
                .expect("the link is removed");
        })
        .await)
    })
}

fn tokio_hard_link(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let named = Named::new("hard-link")?;
    tokio_runtime()?.block_on(async move {
        let link = named.dir.join("made");
        Ok(rate(measure, async || {
            tokio::fs::hard_link(&named.file, &link)
                .await
                .expect("the link is made");
            tokio::fs::remove_file(&link)
                .await
                .expect("the link is removed");
        })
        .await)
    })
}

/// Where a symbolic link points: `readlink` on the blocking pool, as
/// io_uring has no operation for it.
fn ruio_read_link(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let named = Named::new("read-link")?;
    ruio_runtime()?.block_on(async move {
        let link = &named.link;
        Ok(rate(measure, async || {
            let target = ruio::fs::read_link(link).await.expect("the link reads");
            assert_eq!(target.as_os_str().len(), 4);
        })
        .await)
    })
}

fn tokio_read_link(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let named = Named::new("read-link")?;
    tokio_runtime()?.block_on(async move {
        let link = &named.link;
        Ok(rate(measure, async || {
            let target = tokio::fs::read_link(link).await.expect("the link reads");
            assert_eq!(target.as_os_str().len(), 4);
        })
        .await)
    })
}

/// 4 KiB of a file allocated and the hole punched back: two operations
/// through the ring, each in an io-wq worker.
fn ruio_fallocate(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let named = Named::new("fallocate")?;
    ruio_runtime()?.block_on(async move {
        let file = ruio::fs::File::options()
            .read(true)
            .write(true)
            .open(&named.file)
            .await?;
        let hole = libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE;
        Ok(rate(measure, async || {
            file.allocate(0, 4096).await.expect("the range allocates");
            file.fallocate(0, 4096, hole)
                .await
                .expect("the hole is punched");
        })
        .await)
    })
}

/// Advice that sets a mode on the file, which completes on the ring without
/// a worker: a round trip of the ring and nothing else.
fn ruio_advise(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let named = Named::new("advise")?;
    ruio_runtime()?.block_on(async move {
        let file = ruio::fs::File::open(&named.file).await?;
        Ok(rate(measure, async || {
            file.advise(0, 0, ruio::fs::Advice::Sequential)
                .await
                .expect("the advice is taken");
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

/// How many bytes the datagrams of `recv from` and `datagrams in` carry.
const DATAGRAM: usize = 64;

/// How large the buffers are that datagrams are received into: room for
/// any that fits an Ethernet frame.
const DATAGRAM_ROOM: usize = 2048;

/// How many datagrams `datagrams in` fills its socket with at a time.
const FILL: usize = 128;

/// How large the receive buffer of the socket that `datagrams in` fills is:
/// room for [`FILL`] datagrams many times over, or as much as the host
/// allows a socket, which is still room for them.
const FILLED_BUFFER: usize = 1 << 20;

/// Datagrams sent to `server`, the socket of an echo task on the runtime,
/// and their echoes received, one round trip a step; the empty datagram
/// sent last stops the echo task.
async fn ruio_round_trips(measure: Duration, server: SocketAddr) -> io::Result<f64> {
    let client = ruio::net::UdpSocket::bind("127.0.0.1:0")?;
    client.connect(server)?;
    let back = RefCell::new(Some(vec![7; DATAGRAM]));
    let rate = rate(measure, async || {
        let buf = back.borrow_mut().take().expect("the buffer is back");
        let (sent, mut buf) = client.send(buf).await;
        sent.expect("the echo task listens");
        buf.clear();
        let (received, buf) = client.recv(buf).await;
        assert_eq!(received.expect("the echo task answers"), DATAGRAM);
        *back.borrow_mut() = Some(buf);
    })
    .await;
    client.send(Vec::new()).await.0?;
    Ok(rate)
}

/// An echo task that takes each datagram with `recv_from`, one request a
/// datagram, and sends it back to its sender with `send_to`.
fn ruio_recv_from(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let server = ruio::net::UdpSocket::bind("127.0.0.1:0")?;
        let addr = server.local_addr()?;
        let echo = ruio::spawn(async move {
            let mut buf = Vec::with_capacity(DATAGRAM_ROOM);
            loop {
                buf.clear();
                let (received, datagram) = server.recv_from(buf).await;
                let (len, from) = received?;
                if len == 0 {
                    return io::Result::Ok(());
                }
                let (sent, datagram) = server.send_to(datagram, from).await;
                sent?;
                buf = datagram;
            }
        });
        let rate = ruio_round_trips(measure, addr).await?;
        echo.await.map_err(io::Error::other)??;
        Ok(rate)
    })
}

/// An echo task that takes each datagram and its sender from
/// `recv_from_stream`, and sends the datagram's buffer back to the sender.
fn ruio_recv_from_stream(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let server = ruio::net::UdpSocket::bind("127.0.0.1:0")?;
        let addr = server.local_addr()?;
        let pool = ruio::buf::BufPool::new(&ruio::driver(), 64, DATAGRAM_ROOM as u32)?;
        let echo = ruio::spawn(async move {
            let mut datagrams = server.recv_from_stream(&pool)?;
            while let Some(datagram) = datagrams.next().await {
                let datagram = datagram?;
                if datagram.is_empty() {
                    break;
                }
                let (data, from) = datagram.into_parts();
                server.send_to(data, from).await.0?;
            }
            io::Result::Ok(())
        });
        let rate = ruio_round_trips(measure, addr).await?;
        echo.await.map_err(io::Error::other)??;
        Ok(rate)
    })
}

fn tokio_recv_from(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    tokio_runtime()?.block_on(async move {
        let server = tokio::net::UdpSocket::bind("127.0.0.1:0").await?;
        let addr = server.local_addr()?;
        let echo = tokio::spawn(async move {
            let mut buf = vec![0; DATAGRAM_ROOM];
            loop {
                let (len, from) = server.recv_from(&mut buf).await?;
                if len == 0 {
                    return io::Result::Ok(());
                }
                server.send_to(&buf[..len], from).await?;
            }
        });
        let client = tokio::net::UdpSocket::bind("127.0.0.1:0").await?;
        client.connect(addr).await?;
        let mut buf = vec![7; DATAGRAM];
        let rate = rate(measure, async || {
            client.send(&buf).await.expect("the echo task listens");
            let received = client.recv(&mut buf).await.expect("the echo task answers");
            assert_eq!(received, DATAGRAM);
        })
        .await;
        client.send(&[]).await?;
        echo.await.map_err(io::Error::other)??;
        Ok(rate)
    })
}

/// A thread on another core than the runtime's that fills a socket's
/// receive buffer with [`FILL`] datagrams of [`DATAGRAM`] bytes each time
/// it is asked.
struct Filler {
    /// Taken when the filler is dropped, which ends the thread.
    ask: Option<std_mpsc::Sender<()>>,
    filled: std_mpsc::Receiver<io::Result<()>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Filler {
    /// A filler of the socket bound to `to`, whose receive buffer it sets to
    /// [`FILLED_BUFFER`] bytes through `fd`.
    fn start(load: &Load, fd: RawFd, to: SocketAddr) -> io::Result<Self> {
        sys::set_receive_buffer(fd, FILLED_BUFFER)?;
        let socket = std::net::UdpSocket::bind("127.0.0.1:0")?;
        socket.connect(to)?;
        let (ask, asked) = std_mpsc::channel::<()>();
        let (done, filled) = std_mpsc::channel();
        let cpu = remote_cpu(load);
        let thread = thread::spawn(move || {
            let _ = sys::pin(&[cpu]);
            for () in asked {
                if done.send(fill(&socket)).is_err() {
                    break;
                }
            }
        });
        Ok(Self {
            ask: Some(ask),
            filled,
            thread: Some(thread),
        })
    }

    /// Fills the socket's receive buffer, and waits until it is full. The
    /// runtime's thread waits outside the ring meanwhile, so that taking the
    /// datagrams after it is all that a drain measures.
    fn fill(&self) -> io::Result<()> {
        let gone = || io::Error::other("the filling thread is gone");
        let ask = self.ask.as_ref().ok_or_else(gone)?;
        ask.send(()).map_err(|_| gone())?;
        self.filled.recv().map_err(|_| gone())?
    }
}

impl Drop for Filler {
    fn drop(&mut self) {
        drop(self.ask.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Sends [`FILL`] datagrams of [`DATAGRAM`] bytes on `socket`, which is
/// connected, with as few `sendmmsg(2)` calls as the kernel takes them in.
fn fill(socket: &std::net::UdpSocket) -> io::Result<()> {
    let payload = [7u8; DATAGRAM];
    let mut iov = libc::iovec {
        iov_base: payload.as_ptr().cast_mut().cast(),
        iov_len: DATAGRAM,
    };
    // SAFETY: all zeroes is a valid `mmsghdr`, with null pointers.
    let mut headers: [libc::mmsghdr; FILL] = unsafe { mem::zeroed() };
    for header in &mut headers {
        header.msg_hdr.msg_iov = &raw mut iov;
        header.msg_hdr.msg_iovlen = 1;
    }
    let mut sent = 0;
    while sent < FILL {
        let left = &mut headers[sent..];
        // SAFETY: each header points to the one iovec, which points to the
        // payload; the kernel only reads them, and writes the headers'
        // `msg_len`.
        let count = unsafe {
            libc::sendmmsg(
                socket.as_raw_fd(),
                left.as_mut_ptr(),
                left.len() as libc::c_uint,
                0,
            )
        };
        sent += usize::try_from(count).map_err(|_| io::Error::last_os_error())?;
    }
    Ok(())
}

/// Fills the socket that `filler` sends to, takes the [`FILL`] datagrams
/// with `take`, one step each, and again, until the taking has run for
/// `measure`: steps per second of taking, the fills left out.
async fn drain_rate(
    measure: Duration,
    filler: &Filler,
    mut take: impl AsyncFnMut(),
) -> io::Result<f64> {
    let mut taking = Duration::ZERO;
    let mut taken: u64 = 0;
    while taking < measure {
        filler.fill()?;
        let start = Instant::now();
        for _ in 0..FILL {
            take().await;
        }
        taking += start.elapsed();
        taken += FILL as u64;
    }
    Ok(taken as f64 / taking.as_secs_f64())
}

/// The file descriptor of a ruio socket, which a UDP socket's always is.
fn plain_fd(fd: ruio::uring::Fd) -> RawFd {
    match fd {
        ruio::uring::Fd::Plain(fd) => fd,
        ruio::uring::Fd::Direct(_) => unreachable!("UDP sockets have plain descriptors"),
    }
}

/// A socket's datagrams taken with `recv_from`, one request a datagram.
fn ruio_datagrams_in(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let socket = ruio::net::UdpSocket::bind("127.0.0.1:0")?;
        let filler = Filler::start(load, plain_fd(socket.fd()), socket.local_addr()?)?;
        let back = RefCell::new(Some(Vec::with_capacity(DATAGRAM_ROOM)));
        drain_rate(measure, &filler, async || {
            let mut buf = back.borrow_mut().take().expect("the buffer is back");
            buf.clear();
            let (received, buf) = socket.recv_from(buf).await;
            let (len, from) = received.expect("the datagram arrives");
            assert_eq!(len, DATAGRAM);
            black_box(from);
            *back.borrow_mut() = Some(buf);
        })
        .await
    })
}

/// A socket's datagrams and their senders taken from `recv_from_stream`.
fn ruio_datagrams_in_stream(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let socket = ruio::net::UdpSocket::bind("127.0.0.1:0")?;
        let pool = ruio::buf::BufPool::new(&ruio::driver(), 256, DATAGRAM_ROOM as u32)?;
        let mut datagrams = socket.recv_from_stream(&pool)?;
        let filler = Filler::start(load, plain_fd(socket.fd()), socket.local_addr()?)?;
        drain_rate(measure, &filler, async || {
            let datagram = datagrams
                .next()
                .await
                .expect("the stream goes on")
                .expect("the datagram arrives");
            assert_eq!(datagram.len(), DATAGRAM);
            black_box(datagram.from());
        })
        .await
    })
}

/// A socket's datagrams taken from `recv_stream`, which leaves their senders
/// out.
fn ruio_datagrams_in_without_senders(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let socket = ruio::net::UdpSocket::bind("127.0.0.1:0")?;
        let pool = ruio::buf::BufPool::new(&ruio::driver(), 256, DATAGRAM_ROOM as u32)?;
        let mut datagrams = socket.recv_stream(&pool);
        let filler = Filler::start(load, plain_fd(socket.fd()), socket.local_addr()?)?;
        drain_rate(measure, &filler, async || {
            let datagram = datagrams
                .next()
                .await
                .expect("the stream goes on")
                .expect("the datagram arrives");
            assert_eq!(datagram.len(), DATAGRAM);
        })
        .await
    })
}

fn tokio_datagrams_in(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    tokio_runtime()?.block_on(async move {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await?;
        let filler = Filler::start(load, socket.as_raw_fd(), socket.local_addr()?)?;
        let mut buf = vec![0; DATAGRAM_ROOM];
        drain_rate(measure, &filler, async || {
            let (len, from) = socket
                .recv_from(&mut buf)
                .await
                .expect("the datagram arrives");
            assert_eq!(len, DATAGRAM);
            black_box(from);
        })
        .await
    })
}

fn tokio_datagrams_in_without_senders(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    tokio_runtime()?.block_on(async move {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await?;
        let filler = Filler::start(load, socket.as_raw_fd(), socket.local_addr()?)?;
        let mut buf = vec![0; DATAGRAM_ROOM];
        drain_rate(measure, &filler, async || {
            let len = socket.recv(&mut buf).await.expect("the datagram arrives");
            assert_eq!(len, DATAGRAM);
        })
        .await
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

/// The `unbounded channel` round trip, both receivers driven through
/// `StreamExt::next`.
fn ruio_stream_next(load: &Load) -> io::Result<f64> {
    use futures_util::StreamExt;
    use ruio::sync::mpsc;

    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let (to_echo, mut echo_rx) = mpsc::unbounded_channel::<u32>();
        let (to_main, mut main_rx) = mpsc::unbounded_channel::<u32>();
        let echo = ruio::spawn(async move {
            while let Some(value) = echo_rx.next().await {
                if to_main.send(value).is_err() {
                    break;
                }
            }
        });
        let rate = rate(measure, async || {
            to_echo.send(1).expect("the echo task runs");
            main_rx.next().await.expect("the echo task answers");
        })
        .await;
        drop(to_echo);
        echo.await.map_err(io::Error::other)?;
        Ok(rate)
    })
}

fn tokio_stream_next(load: &Load) -> io::Result<f64> {
    use futures_util::StreamExt;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::UnboundedReceiverStream;

    let measure = load.measure;
    tokio_runtime()?.block_on(async move {
        let (to_echo, echo_rx) = mpsc::unbounded_channel::<u32>();
        let (to_main, main_rx) = mpsc::unbounded_channel::<u32>();
        let mut echo_rx = UnboundedReceiverStream::new(echo_rx);
        let mut main_rx = UnboundedReceiverStream::new(main_rx);
        let echo = tokio::spawn(async move {
            while let Some(value) = echo_rx.next().await {
                if to_main.send(value).is_err() {
                    break;
                }
            }
        });
        let rate = rate(measure, async || {
            to_echo.send(1).expect("the echo task runs");
            main_rx.next().await.expect("the echo task answers");
        })
        .await;
        drop(to_echo);
        echo.await.map_err(io::Error::other)?;
        Ok(rate)
    })
}

/// How many values `drain` and `stream fold` send and take in a step.
const DRAINED: u32 = 1000;

/// A thousand values sent into an unbounded channel and taken out with
/// `recv`, one after another.
fn ruio_drain(load: &Load) -> io::Result<f64> {
    use ruio::sync::mpsc;

    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let (tx, mut rx) = mpsc::unbounded_channel::<u32>();
        Ok(rate(measure, async || {
            for value in 0..DRAINED {
                tx.send(value).expect("the receiver is here");
            }
            let mut sum = 0;
            for _ in 0..DRAINED {
                sum += rx.recv().await.expect("the values are there");
            }
            assert_eq!(sum, DRAINED * (DRAINED - 1) / 2);
        })
        .await)
    })
}

fn tokio_drain(load: &Load) -> io::Result<f64> {
    use tokio::sync::mpsc;

    let measure = load.measure;
    tokio_runtime()?.block_on(async move {
        let (tx, mut rx) = mpsc::unbounded_channel::<u32>();
        Ok(rate(measure, async || {
            for value in 0..DRAINED {
                tx.send(value).expect("the receiver is here");
            }
            let mut sum = 0;
            for _ in 0..DRAINED {
                sum += rx.recv().await.expect("the values are there");
            }
            assert_eq!(sum, DRAINED * (DRAINED - 1) / 2);
        })
        .await)
    })
}

/// The same thousand values, taken out through `StreamExt::take(1000).fold`.
fn ruio_stream_fold(load: &Load) -> io::Result<f64> {
    use futures_util::StreamExt;
    use ruio::sync::mpsc;

    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let (tx, mut rx) = mpsc::unbounded_channel::<u32>();
        Ok(rate(measure, async || {
            for value in 0..DRAINED {
                tx.send(value).expect("the receiver is here");
            }
            let sum = (&mut rx)
                .take(DRAINED as usize)
                .fold(0, |sum, value| async move { sum + value })
                .await;
            assert_eq!(sum, DRAINED * (DRAINED - 1) / 2);
        })
        .await)
    })
}

fn tokio_stream_fold(load: &Load) -> io::Result<f64> {
    use futures_util::StreamExt;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::UnboundedReceiverStream;

    let measure = load.measure;
    tokio_runtime()?.block_on(async move {
        let (tx, rx) = mpsc::unbounded_channel::<u32>();
        let mut rx = UnboundedReceiverStream::new(rx);
        Ok(rate(measure, async || {
            for value in 0..DRAINED {
                tx.send(value).expect("the receiver is here");
            }
            let sum = (&mut rx)
                .take(DRAINED as usize)
                .fold(0, |sum, value| async move { sum + value })
                .await;
            assert_eq!(sum, DRAINED * (DRAINED - 1) / 2);
        })
        .await)
    })
}

/// A connected pair of ruio TCP streams, the accepted one a direct
/// descriptor.
async fn ruio_direct_pair() -> io::Result<(ruio::net::TcpStream, ruio::net::TcpStream)> {
    let listener = ruio::net::TcpListener::bind("127.0.0.1:0")?;
    listener.set_direct(true);
    let addr = listener.local_addr()?;
    let client = ruio::spawn(ruio::net::TcpStream::connect(addr));
    let (server, _) = listener.accept().await?;
    Ok((server, client.await.map_err(io::Error::other)??))
}

/// Sets `TCP_NODELAY` on `stream`, on and off in turn, through the ring.
async fn ruio_toggle_nodelay(measure: Duration, stream: &ruio::net::TcpStream) -> f64 {
    let mut on = false;
    rate(measure, async || {
        on = !on;
        stream.set_nodelay(on).await.expect("the option is set");
    })
    .await
}

/// `TCP_NODELAY` set on a plain stream, on and off in turn, through the ring.
fn ruio_sockopt(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let (stream, _peer) = ruio_pair().await?;
        Ok(ruio_toggle_nodelay(measure, &stream).await)
    })
}

/// The same on a direct descriptor.
fn ruio_sockopt_direct(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let (stream, _peer) = ruio_direct_pair().await?;
        assert!(stream.is_direct());
        Ok(ruio_toggle_nodelay(measure, &stream).await)
    })
}

fn tokio_sockopt(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    tokio_runtime()?.block_on(async move {
        let (stream, _peer) = tokio_pair().await?;
        let mut on = false;
        Ok(rate(measure, async || {
            on = !on;
            stream.set_nodelay(on).expect("the option is set");
        })
        .await)
    })
}

/// A byte written to the peer and read back from it, on a direct pair.
fn ruio_write_read(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let (a, b) = ruio_direct_pair().await?;
        let mut buf = Some(Vec::with_capacity(1));
        Ok(rate(measure, async || {
            a.write("x").await.0.expect("the byte is written");
            let (read, taken) = b.read(buf.take().expect("the buffer is back")).await;
            assert_eq!(read.expect("the byte is read"), 1);
            buf = Some(taken);
        })
        .await)
    })
}

fn tokio_write_read(load: &Load) -> io::Result<f64> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let measure = load.measure;
    tokio_runtime()?.block_on(async move {
        let (mut a, mut b) = tokio_pair().await?;
        let mut byte = [0u8; 1];
        Ok(rate(measure, async || {
            a.write_all(b"x").await.expect("the byte is written");
            b.read_exact(&mut byte).await.expect("the byte is read");
        })
        .await)
    })
}

/// The option set on the direct stream, a byte written and read from the
/// peer, each awaited before the next.
fn ruio_sockopt_in_turn(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let (a, b) = ruio_direct_pair().await?;
        let mut on = false;
        let mut buf = Some(Vec::with_capacity(1));
        Ok(rate(measure, async || {
            on = !on;
            a.set_nodelay(on).await.expect("the option is set");
            a.write("x").await.0.expect("the byte is written");
            let (read, taken) = b.read(buf.take().expect("the buffer is back")).await;
            assert_eq!(read.expect("the byte is read"), 1);
            buf = Some(taken);
        })
        .await)
    })
}

/// The option and the write polled together, so that one submission takes
/// both to the kernel, then the byte read from the peer.
fn ruio_sockopt_batched(load: &Load) -> io::Result<f64> {
    use futures_util::future::join;

    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let (a, b) = ruio_direct_pair().await?;
        let mut on = false;
        let mut buf = Some(Vec::with_capacity(1));
        Ok(rate(measure, async || {
            on = !on;
            let (set, (written, _)) = join(a.set_nodelay(on), a.write("x")).await;
            set.expect("the option is set");
            written.expect("the byte is written");
            let (read, taken) = b.read(buf.take().expect("the buffer is back")).await;
            assert_eq!(read.expect("the byte is read"), 1);
            buf = Some(taken);
        })
        .await)
    })
}

fn tokio_sockopt_in_turn(load: &Load) -> io::Result<f64> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let measure = load.measure;
    tokio_runtime()?.block_on(async move {
        let (mut a, mut b) = tokio_pair().await?;
        let mut on = false;
        let mut byte = [0u8; 1];
        Ok(rate(measure, async || {
            on = !on;
            a.set_nodelay(on).expect("the option is set");
            a.write_all(b"x").await.expect("the byte is written");
            b.read_exact(&mut byte).await.expect("the byte is read");
        })
        .await)
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

/// `/bin/true` spawned and waited for.
fn ruio_spawn_wait(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let mut command = ruio::process::Command::new("/bin/true");
        Ok(rate(measure, async || {
            let status = command.status().await.expect("the child runs");
            assert!(status.success());
        })
        .await)
    })
}

fn tokio_spawn_wait(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    tokio_runtime()?.block_on(async move {
        let mut command = tokio::process::Command::new("/bin/true");
        Ok(rate(measure, async || {
            let status = command.status().await.expect("the child runs");
            assert!(status.success());
        })
        .await)
    })
}

/// `/bin/echo` spawned, its output collected, and waited for.
fn ruio_spawn_output(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let mut command = ruio::process::Command::new("/bin/echo");
        command.arg("hello");
        Ok(rate(measure, async || {
            let output = command.output().await.expect("the child runs");
            assert_eq!(output.stdout, b"hello\n");
        })
        .await)
    })
}

fn tokio_spawn_output(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    tokio_runtime()?.block_on(async move {
        let mut command = tokio::process::Command::new("/bin/echo");
        command.arg("hello");
        Ok(rate(measure, async || {
            let output = command.output().await.expect("the child runs");
            assert_eq!(output.stdout, b"hello\n");
        })
        .await)
    })
}

/// How many bytes `child pipes` sends through `cat` each way.
const PIPED: usize = 64;

/// What `child pipes` sends.
static PIPED_DATA: [u8; PIPED] = [7; PIPED];

/// 64 bytes written to a running `cat` and read back.
fn ruio_child_pipes(load: &Load) -> io::Result<f64> {
    use ruio::io::{AsyncRead, AsyncWrite};

    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let mut child = ruio::process::Command::new("cat")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()?;
        let (stdin, stdout) = (child.stdin.take().unwrap(), child.stdout.take().unwrap());
        let mut buf = Some(Vec::with_capacity(PIPED));
        let rate = rate(measure, async || {
            stdin
                .write_all(&PIPED_DATA[..])
                .await
                .0
                .expect("cat takes its input");
            let mut back = buf.take().expect("put back after each read");
            back.clear();
            let (read, back) = stdout.read_exact(back).await;
            read.expect("cat writes it back");
            buf = Some(back);
        })
        .await;
        drop(stdin);
        assert!(child.wait().await?.success());
        Ok(rate)
    })
}

fn tokio_child_pipes(load: &Load) -> io::Result<f64> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let measure = load.measure;
    tokio_runtime()?.block_on(async move {
        let mut child = tokio::process::Command::new("cat")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()?;
        let (mut stdin, mut stdout) = (child.stdin.take().unwrap(), child.stdout.take().unwrap());
        let mut back = [0; PIPED];
        let rate = rate(measure, async || {
            stdin
                .write_all(&PIPED_DATA)
                .await
                .expect("cat takes its input");
            stdout
                .read_exact(&mut back)
                .await
                .expect("cat writes it back");
        })
        .await;
        drop(stdin);
        assert!(child.wait().await?.success());
        Ok(rate)
    })
}

/// A byte written to the peer and read back from it, on a pair of portable
/// streams.
fn ruio_portable_write_read(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    ruio_runtime()?.block_on(async move {
        let (a, b) = portable_pair().await?;
        let mut buf = Some(Vec::with_capacity(1));
        Ok(rate(measure, async || {
            a.write("x").await.0.expect("the byte is written");
            let (read, taken) = b.read(buf.take().expect("the buffer is back")).await;
            assert_eq!(read.expect("the byte is read"), 1);
            buf = Some(taken);
        })
        .await)
    })
}

/// A connected pair of portable TCP streams on the loopback interface.
async fn portable_pair() -> io::Result<(
    ruio::net::portable::TcpStream,
    ruio::net::portable::TcpStream,
)> {
    let listener = ruio::net::portable::TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let client = ruio::net::portable::TcpStream::connect(addr).await?;
    let (server, _) = listener.accept().await?;
    Ok((client, server))
}

/// How many bytes `round trip` and `portable round trip` send each way.
const ROUND: usize = 64;

/// What `round trip` and `portable round trip` send.
static ROUND_DATA: [u8; ROUND] = [7; ROUND];

/// How many tasks `hyper get 16` makes requests from.
const HYPER_TASKS: usize = 16;

/// How many cores the server of `hyper get 16` spreads its connections over.
const HYPER_CORES: usize = 8;

/// What the server of the `hyper get` rows answers each request with.
const HELLO: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\nhello";

/// Threads that serve each connection to a listener on the loopback
/// interface, a thread a connection, each pinned to the next of a list of
/// CPUs in turn: the peers of the round trips and of hyper's clients, which
/// are the same whichever runtime is measured. Dropped, it stops accepting;
/// a connection's thread ends with its connection.
struct Peers {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    accepting: Option<thread::JoinHandle<()>>,
}

impl Peers {
    /// Serves each connection with `serve`, on `cpus` in turn.
    fn start(cpus: Vec<usize>, serve: fn(std::net::TcpStream)) -> io::Result<Self> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let stop = Arc::new(AtomicBool::new(false));
        let accepting = thread::spawn({
            let stop = Arc::clone(&stop);
            move || {
                for (index, stream) in listener.incoming().enumerate() {
                    if stop.load(Ordering::SeqCst) {
                        return;
                    }
                    let Ok(stream) = stream else {
                        continue;
                    };
                    let cpu = cpus[index % cpus.len()];
                    thread::spawn(move || {
                        let _ = sys::pin(&[cpu]);
                        let _ = sys::set_nodelay(stream.as_raw_fd());
                        serve(stream);
                    });
                }
            }
        });
        Ok(Self {
            addr,
            stop,
            accepting: Some(accepting),
        })
    }

    /// The URI of the server's root.
    fn uri(&self) -> io::Result<hyper::Uri> {
        format!("http://{}/", self.addr)
            .parse()
            .map_err(io::Error::other)
    }
}

impl Drop for Peers {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        // Wakes the accept, which then finds the flag set.
        let _ = std::net::TcpStream::connect(self.addr);
        if let Some(accepting) = self.accepting.take() {
            let _ = accepting.join();
        }
    }
}

/// Sends back what the connection sends, until it ends.
fn serve_echo(mut stream: std::net::TcpStream) {
    let mut buf = [0u8; 4096];
    loop {
        match std::io::Read::read(&mut stream, &mut buf) {
            Ok(0) | Err(_) => return,
            Ok(read) => {
                if std::io::Write::write_all(&mut stream, &buf[..read]).is_err() {
                    return;
                }
            }
        }
    }
}

/// Answers each request the connection sends with [`HELLO`], as an HTTP/1.1
/// server that keeps its connections alive does, until the connection
/// ends. The requests are GETs, which end with their heads.
fn serve_hello(mut stream: std::net::TcpStream) {
    let mut pending = Vec::with_capacity(4096);
    let mut buf = [0u8; 4096];
    loop {
        match std::io::Read::read(&mut stream, &mut buf) {
            Ok(0) | Err(_) => return,
            Ok(read) => pending.extend_from_slice(&buf[..read]),
        }
        while let Some(end) = pending.windows(4).position(|window| window == b"\r\n\r\n") {
            pending.drain(..end + 4);
            if std::io::Write::write_all(&mut stream, HELLO).is_err() {
                return;
            }
        }
    }
}

/// 64 bytes to an echo thread on another core and back, through a stream of
/// the ring's.
fn ruio_round_trip(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let peers = Peers::start(vec![remote_cpu(load)], serve_echo)?;
    let addr = peers.addr;
    ruio_runtime()?.block_on(async move {
        let stream = ruio::net::TcpStream::connect(addr).await?;
        stream.set_nodelay(true).await?;
        let mut buf = Some(Vec::with_capacity(ROUND));
        Ok(rate(measure, async || {
            stream
                .write_all(&ROUND_DATA[..])
                .await
                .0
                .expect("the bytes are written");
            let (read, back) = stream
                .read_exact(buf.take().expect("the buffer is back"))
                .await;
            read.expect("the bytes come back");
            buf = Some(back);
        })
        .await)
    })
}

/// The same through a portable stream.
fn ruio_portable_round_trip(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let peers = Peers::start(vec![remote_cpu(load)], serve_echo)?;
    let addr = peers.addr;
    ruio_runtime()?.block_on(async move {
        let stream = ruio::net::portable::TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let mut buf = Some(Vec::with_capacity(ROUND));
        Ok(rate(measure, async || {
            stream
                .write_all(&ROUND_DATA[..])
                .await
                .0
                .expect("the bytes are written");
            let (read, back) = stream
                .read_exact(buf.take().expect("the buffer is back"))
                .await;
            read.expect("the bytes come back");
            buf = Some(back);
        })
        .await)
    })
}

fn tokio_round_trip(load: &Load) -> io::Result<f64> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let measure = load.measure;
    let peers = Peers::start(vec![remote_cpu(load)], serve_echo)?;
    let addr = peers.addr;
    tokio_runtime()?.block_on(async move {
        let mut stream = tokio::net::TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let mut back = [0u8; ROUND];
        Ok(rate(measure, async || {
            stream
                .write_all(&ROUND_DATA)
                .await
                .expect("the bytes are written");
            stream
                .read_exact(&mut back)
                .await
                .expect("the bytes come back");
        })
        .await)
    })
}

/// A ruio runtime on a thread of its own, pinned to a CPU, whose ring is
/// the foreign one, which portable operations polled where no runtime runs
/// go to, until this is dropped.
struct ForeignRing {
    stop: Option<ruio::sync::oneshot::Sender<()>>,
    thread: Option<thread::JoinHandle<io::Result<()>>>,
}

impl ForeignRing {
    fn start(cpu: usize) -> io::Result<Self> {
        let (ready, started) = std_mpsc::channel();
        let (stop, stopped) = ruio::sync::oneshot::channel::<()>();
        let thread = thread::spawn(move || {
            // Not pinned if the host has no such CPU.
            let _ = sys::pin(&[cpu]);
            ruio_runtime()?.block_on(async move {
                let _ = ready.send(ruio::handle().ring().clone());
                let _ = stopped.await;
            });
            Ok(())
        });
        let ring = started
            .recv()
            .map_err(|_| io::Error::other("the foreign ring's runtime did not start"))?;
        ruio::uring::set_foreign_ring(&ring);
        Ok(Self {
            stop: Some(stop),
            thread: Some(thread),
        })
    }
}

impl Drop for ForeignRing {
    fn drop(&mut self) {
        ruio::uring::clear_foreign_ring();
        drop(self.stop.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Wakes the thread that waits for a future, as
/// `futures::executor::block_on`'s waker does.
struct Unpark(thread::Thread);

impl std::task::Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

/// Runs `future` to completion on this thread, which runs no runtime,
/// parking between polls until it is woken, as
/// `futures::executor::block_on` does.
fn block_on_plain<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let waker = std::task::Waker::from(Arc::new(Unpark(thread::current())));
    let mut cx = std::task::Context::from_waker(&waker);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        thread::park();
    }
}

/// Portable no-ops polled one at a time on this thread, which runs no
/// runtime, through the foreign ring of a runtime on another core.
fn foreign_nop(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let foreign = ForeignRing::start(foreign_cpu(load))?;
    let rate = block_on_plain(rate(measure, async || {
        PortableOp::new(Nop).await.expect("the no-op completes");
    }));
    drop(foreign);
    Ok(rate)
}

/// `portable round trip` polled by tokio's current-thread runtime, its waits
/// on the foreign ring of a ruio runtime on another core.
fn foreign_round_trip(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let foreign = ForeignRing::start(foreign_cpu(load))?;
    let peers = Peers::start(vec![remote_cpu(load)], serve_echo)?;
    let addr = peers.addr;
    let rate = tokio_runtime()?.block_on(async move {
        let stream = ruio::net::portable::TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let mut buf = Some(Vec::with_capacity(ROUND));
        io::Result::Ok(
            rate(measure, async || {
                stream
                    .write_all(&ROUND_DATA[..])
                    .await
                    .0
                    .expect("the bytes are written");
                let (read, back) = stream
                    .read_exact(buf.take().expect("the buffer is back"))
                    .await;
                read.expect("the bytes come back");
                buf = Some(back);
            })
            .await,
        )
    })?;
    drop(peers);
    drop(foreign);
    Ok(rate)
}

/// The body of the `hyper get` rows' requests, which is none.
type NoBody = http_body_util::Empty<hyper::body::Bytes>;

/// hyper-util's client on ruio, whose connections are portable streams.
fn ruio_client() -> hyper_util::client::legacy::Client<ruio_compat::client::Connector, NoBody> {
    ruio_compat::client::builder().build(ruio_compat::client::Connector::new())
}

/// hyper-util's client on tokio, with `TCP_NODELAY` and a timer for its
/// pool, as ruio's has them.
fn tokio_client()
-> hyper_util::client::legacy::Client<hyper_util::client::legacy::connect::HttpConnector, NoBody> {
    let mut connector = hyper_util::client::legacy::connect::HttpConnector::new();
    connector.set_nodelay(true);
    hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
        .pool_timer(hyper_util::rt::TokioTimer::new())
        .build(connector)
}

/// A GET of `uri`, its body read to its end, which must be `hello`.
async fn hyper_get<C>(client: &hyper_util::client::legacy::Client<C, NoBody>, uri: &hyper::Uri)
where
    C: hyper_util::client::legacy::connect::Connect + Clone + Send + Sync + 'static,
{
    use http_body_util::BodyExt;

    let response = client
        .get(uri.clone())
        .await
        .expect("the request is answered");
    let body = response
        .into_body()
        .collect()
        .await
        .expect("the body is read")
        .to_bytes();
    assert_eq!(&body[..], b"hello");
}

/// GETs one at a time through hyper-util's client to a server thread on
/// another core.
fn ruio_hyper_get(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let peers = Peers::start(vec![remote_cpu(load)], serve_hello)?;
    let uri = peers.uri()?;
    ruio_runtime()?.block_on(async move {
        let client = ruio_client();
        Ok(rate(measure, async || hyper_get(&client, &uri).await).await)
    })
}

fn tokio_hyper_get(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let peers = Peers::start(vec![remote_cpu(load)], serve_hello)?;
    let uri = peers.uri()?;
    tokio_runtime()?.block_on(async move {
        let client = tokio_client();
        Ok(rate(measure, async || hyper_get(&client, &uri).await).await)
    })
}

/// The cores the server of `hyper get 16` spreads its connections over.
fn hyper_cores(load: &Load) -> Vec<usize> {
    (0..HYPER_CORES)
        .map(|core| remote_cpu(load) + 2 * core)
        .collect()
}

/// GETs from 16 tasks at once through one client of hyper-util's.
fn ruio_hyper_get_many(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let peers = Peers::start(hyper_cores(load), serve_hello)?;
    let uri = peers.uri()?;
    ruio_runtime()?.block_on(async move {
        let client = ruio_client();
        let stop = Arc::new(AtomicBool::new(false));
        let tasks: Vec<_> = (0..HYPER_TASKS)
            .map(|_| {
                let (client, uri, stop) = (client.clone(), uri.clone(), Arc::clone(&stop));
                ruio::spawn(async move {
                    let mut count = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        hyper_get(&client, &uri).await;
                        count += 1;
                    }
                    count
                })
            })
            .collect();
        let start = Instant::now();
        ruio::time::sleep(measure).await;
        stop.store(true, Ordering::Relaxed);
        let mut total = 0;
        for task in tasks {
            total += task.await.map_err(io::Error::other)?;
        }
        Ok(total as f64 / start.elapsed().as_secs_f64())
    })
}

fn tokio_hyper_get_many(load: &Load) -> io::Result<f64> {
    let measure = load.measure;
    let peers = Peers::start(hyper_cores(load), serve_hello)?;
    let uri = peers.uri()?;
    tokio_runtime()?.block_on(async move {
        let client = tokio_client();
        let stop = Arc::new(AtomicBool::new(false));
        let tasks: Vec<_> = (0..HYPER_TASKS)
            .map(|_| {
                let (client, uri, stop) = (client.clone(), uri.clone(), Arc::clone(&stop));
                tokio::spawn(async move {
                    let mut count = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        hyper_get(&client, &uri).await;
                        count += 1;
                    }
                    count
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
