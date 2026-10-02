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
| [`ruio`](crates/ruio) | The runtime: `Runtime`, `Cores`, `spawn`, `spawn_on`, `time`, `sync`, and `net` and `fs`, whose I/O runs on the thread's ring |
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
  13%, and level with fewer; with one connection, a runtime that spins
  before it parks (`Builder::spin`) makes twice tokio's round trips, with
  half the tail latency.
- **4 KiB `O_DIRECT` random reads**: 13% to 16% ahead of fio's io_uring
  engine at every queue depth. It was within 5% of fio when first compared;
  the runtime's own paths, and submitting reads 32 to an entry into the
  kernel as a block plug takes them, made up the rest.
- **Across cores**: linear up to 16 cores, and up to 96 within 8% of what
  the host gives a busy loop.
- **hyper, HTTP/1.1**, one core, through the adapters, receiving into a
  pool: level with hyper on tokio with one connection, 1% to 8% ahead from
  256 connections on, and 3% behind at 16.
- **The runtime itself** (`ruio-bench micro`): a task's turn of the loop and
  a wake on its own thread cost no atomic operation, and the runtime's loops
  run two to seventeen times as fast as on tokio's current-thread runtime:
  spawning and joining a task 6.9 times as fast, a yield 17 times, a channel
  round trip 3.9 times and an uncontended async mutex 2.1 times.

## Development

```text
ci/check.sh     formatting, clippy, tests and documentation, as CI runs them
ci/asan.sh      the tests under AddressSanitizer with leak detection (nightly)
cargo run -p ruio-driver --example probe    what the kernel supports
cargo run --release -p ruio-bench -- echo   the echo benchmark; also http, randread, scale, micro
```

[DESIGN.md](DESIGN.md) describes the design, its soundness model, and what
the benchmarks found.
