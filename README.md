# ruio

An async I/O runtime for Linux, built directly on io_uring: thread per
core, owned-buffer I/O, multishot operations as streams, and kernel-chosen
buffers for receives, with an API that reads like ordinary async Rust.

```rust
use ruio::buf::BufPool;
use ruio::net::TcpListener;

#[ruio::main(cores = "all")]
async fn main() -> std::io::Result<()> {
    // A listener on each core, all bound to one address.
    let listener = TcpListener::bind_reuseport("127.0.0.1:7878")?;
    let pool = BufPool::new(&ruio::driver(), 1024, 4096)?;
    let mut incoming = listener.incoming(); // one multishot accept
    while let Some(stream) = incoming.next().await {
        let stream = stream?;
        let pool = pool.clone();
        let _echo = ruio::spawn(async move {
            // One multishot receive, into buffers the kernel picks.
            let mut received = stream.recv_stream(&pool);
            while let Some(Ok(data)) = received.next().await {
                // Sent without waiting for the kernel to finish.
                if stream.write_behind(data).await.is_err() {
                    break;
                }
            }
        });
    }
    Ok(())
}
```

## Status

The milestones of [DESIGN.md](DESIGN.md), M0 to M7, are done: the driver,
the executor, buffers, networking, files, thread per core, and adapters
for the ecosystem's I/O traits. The crates are not published yet, as no
license has been chosen.

ruio needs Linux 6.10 or later, which it checks by capability rather than
by version, and the Rust toolchain that `rust-toolchain.toml` pins
(1.97.1).

## Crates

| Crate | What it has |
|---|---|
| [`ruio`](crates/ruio) | The runtime: `Runtime`, `Cores`, `spawn`, `spawn_on`, `spawn_blocking`, `JoinSet`, `time`, `sync`, `net` and `fs`, whose I/O runs on the thread's ring, `io`, the owned-buffer traits they share, with `copy`, `splice` and `AsyncFd` for any other descriptor, and `signal` |
| [`ruio-compat`](crates/ruio-compat) | `Compat`, which gives ruio's streams the `AsyncRead`, `AsyncBufRead` and `AsyncWrite` of `tokio::io` and of futures-io, for hyper, rustls and the rest of the ecosystem; and `Timer`, which gives hyper ruio's timers for its timeouts |
| [`ruio-executor`](crates/ruio-executor) | The per-thread scheduler, tasks, timers and the thread-per-core builder |
| [`ruio-driver`](crates/ruio-driver) | One io_uring ring: operations, the futures that run them, buffers and pools, registered files, the capability probe |
| [`ruio-sys`](crates/ruio-sys) | The kernel's io_uring ABI and system calls |
| [`ruio-macros`](crates/ruio-macros) | `#[ruio::main]` and `#[ruio::test]` |
| [`ruio-bench`](crates/ruio-bench) | Benchmarks against tokio and fio |

## hyper, rustls and the rest

`ruio-compat` adapts a `TcpStream` or a `UnixStream` to the poll-based
traits that most of the ecosystem takes its I/O through; tokio's traits
need none of tokio's runtime. hyper-util's `TokioIo` then makes it a
connection hyper serves, and tokio-rustls runs TLS over it. `Timer` gives
hyper ruio's timers, with which it times out a client that is slow to send
a request's head:

```rust
let io = TokioIo::new(Compat::with_pool(&pool, stream));
hyper::server::conn::http1::Builder::new()
    .timer(Timer::new())
    .serve_connection(io, service_fn(serve))
    .await?;
```

Two examples serve HTTP/1.1 and HTTPS on every core:

```text
cargo run --release -p ruio-compat --example hyper
curl http://127.0.0.1:3000/

cargo run --release -p ruio-compat --example rustls
curl --insecure https://127.0.0.1:3443/
```

## Performance

On a Xeon Platinum 8488C with Linux 7.0, over loopback (see
[Benchmarks](DESIGN.md#benchmarks) for the loads and the full tables):

- **TCP echo**, one core: ahead of tokio from 256 connections on, by up to
  13%, and level with fewer, bar 64 KiB messages, where it is 10% to 17%
  ahead at every connection count; with one connection, a runtime that
  spins before it parks (`Builder::spin`) makes twice tokio's round trips,
  with half the tail latency.
- **4 KiB `O_DIRECT` random reads**: 8% to 14% ahead of fio's io_uring
  engine at every queue depth, and polled, on a device with poll queues
  (`OpenOptions::polled`), 10% to 18% ahead of fio's polled engine, reading
  up to 14% more a second than through interrupts. It was within 5% of fio
  when first compared; the runtime's own paths, and submitting reads 32 to
  an entry into the kernel as a block plug takes them, made up the rest.
- **Across cores**: linear up to 16 cores, and up to 96 within 8% of what
  the host gives a busy loop.
- **hyper, HTTP/1.1**, one core, through the adapters, receiving into a
  pool: level with hyper on tokio with one connection, 1% to 8% ahead from
  256 connections on, and 3% behind at 16, where the difference is cache
  lines the client's core hands the server's for each wake, not the
  runtime's own work.
- **The runtime itself** (`ruio-bench micro`): a task's turn of the loop, a
  spawn and its join, a timer armed and dropped, and a wake on the task's
  own thread cost no locked instruction. Against tokio's current-thread
  runtime, spawning and joining a task runs 8.8 times as fast, and 10.2
  through a join set, a yield 16 times, arming and dropping a timer 5.3
  times, a channel round trip 5.4 times, 9.2 through the channels for one
  thread, an uncontended async mutex 2.1 times, a round trip through
  unbounded channels 4.7 times, through broadcast channels 3.1 and through
  watch channels 4.0, two tasks meet at a barrier 6.3 times as often,
  arming a wait for a cancellation token and dropping it runs 2.3 times as
  fast as tokio-util's, a closure sent to the blocking pool and its output
  awaited make 2.9 times the round trips of tokio's `spawn_blocking`, 858k
  a second with the pool and the runtime spinning, and a directory of a
  thousand entries lists 2.4 times as fast as through `tokio::fs::read_dir`.
  Nothing blocks the runtime's thread: host names resolve and directories
  are read on the blocking pool, directories read ahead so that a task
  taking the entries waits for the pool once. A relay between two
  connections moves 1.4 times as many 64 KiB chunks with `io::splice`,
  which keeps the bytes in the kernel, as with `io::copy`, itself 1.4 times
  tokio's, and a cached file goes into a connection at 13.6 GB/s, 14 times
  as fast as tokio copies it from its `File`. A wait for a foreign
  descriptor's readiness through `AsyncFd` runs 1.3 times as fast as
  tokio's, and a signal raised and received 1.3 times, each near the floor
  of its system calls.

## Development

```text
ci/check.sh     formatting, clippy, tests and documentation, as CI runs them
ci/asan.sh      the tests under AddressSanitizer with leak detection (nightly)
cargo run -p ruio-driver --example probe    what the kernel supports
cargo run --release -p ruio-bench -- echo   the echo benchmark; also http, randread, scale, micro
```

[DESIGN.md](DESIGN.md) describes the design, its soundness model, and what
the benchmarks found.
