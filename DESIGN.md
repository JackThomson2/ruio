# ruio design

ruio is an async I/O runtime for Linux built directly on io_uring. The aim is
the fastest way to do network and file I/O from Rust on current kernels, with
an API that still reads like ordinary async Rust.

## Goals

- Throughput and tail latency at least on par with the best Rust io_uring
  runtimes, and clearly ahead of epoll-based runtimes, on both network and
  file workloads.
- Soundness without caveats: nothing a caller does with a future (dropping
  it, `mem::forget`, racing it in `select!`) can make the kernel touch freed
  memory.
- Ergonomics: owned-buffer I/O that reads naturally, kernel-chosen buffers for
  receives, per-operation timeouts, and multishot operations as streams.
- Current io_uring features by default; newer ones detected at runtime.

## Non-goals

- Platforms other than Linux, and fallbacks to epoll or thread pools.
- Kernels older than 6.1.
- Being a drop-in replacement for tokio. Interoperability comes from adapters
  (milestone M7).

## Decisions

| Topic | Decision | Reason |
|---|---|---|
| Execution model | Thread-per-core: one ring and one executor per thread; spawned tasks need not be `Send`. | `SINGLE_ISSUER` and `DEFER_TASKRUN` assume one submitting thread, registered files and buffers belong to a single ring, and the hot path needs no atomics or locks. |
| Work stealing | Not in the first release, but kept possible (see [Keeping work stealing possible](#keeping-work-stealing-possible)). | It fights per-ring state, but some workloads want it. |
| Minimum kernel | Linux 6.1, the oldest LTS with `DEFER_TASKRUN`. Detected by capability, not by version string. | 6.1 has everything the core needs: `SINGLE_ISSUER`, `DEFER_TASKRUN`, provided-buffer rings, multishot accept and recv, zero-copy send. Distribution kernels backport features, so version numbers mislead. |
| Bindings | Our own, in `ruio-sys`: written by hand from the kernel's uapi header and tested against it. | Full control of the ABI surface and the hot path, no bindgen at build time, and new kernel features arrive when we need them. |
| Interop | `futures-io` and `tokio::io` adapters in `ruio-compat`, in the last milestone. | Ecosystem access matters, but must not shape the core API. |
| Toolchain | Newest stable Rust (1.97.1), edition 2024, pinned in `rust-toolchain.toml`. Nightly only for sanitizers. | No minimum supported Rust version is required. |
| Dependencies | `libc` only, in the core crates. | Small audit surface. |
| Direct descriptors | Supported throughout the driver; `ruio::net` accepts them on request (`set_direct`), plain descriptors by default. | On the 6.1 baseline a direct socket can neither report its addresses nor change options (`setsockopt` through `URING_CMD` is 6.7, `getsockname` 6.19), and a full file table makes the kernel close the connection it accepted. The echo benchmark measures the file lookups they save at about 1%. |

## Architecture

```
crates/
  ruio-sys/       kernel ABI: uapi structures and constants, raw system calls; no policy
  ruio-driver/    one ring: setup, SQ/CQ, operation table, Op<T> futures, buffers,
                  registered files, capability probe
  ruio-executor/  per-thread scheduler, tasks, timer wheel, thread-per-core builder
  ruio-macros/    #[ruio::main], #[ruio::test]
  ruio/           public API: runtime, time, uring, buf, net; later fs, io
  ruio-bench/     benchmarks against other runtimes, the one crate that depends on them
  ruio-compat/    futures-io and tokio::io adapters
```

Each crate is added when its milestone starts; M0 has `ruio-sys` and
`ruio-driver`.

The driver does not depend on the executor. It finishes an operation by
calling the `Waker` it was polled with, so anything that polls futures can
drive it. That keeps the driver testable on its own and is the seam a
different scheduler would plug into.

## Soundness model

With completion-based I/O the kernel reads and writes an operation's memory
after submission, up to the moment it posts the operation's final completion
(CQE). Rust does not guarantee that a future runs to completion, or even that
it is dropped. Three rules follow.

1. **Operations own their resources.** Every buffer, path, socket address,
   iovec array and timespec an SQE points to belongs to the operation from
   submission until its final CQE. Buffers are passed by value and handed
   back with the result:

   ```rust
   let (res, buf) = file.read_at(buf, 0).await;
   ```

   The unsafe `IoBuf` and `IoBufMut` traits promise a stable address and
   length while the kernel holds the buffer. They are implemented for
   `Vec<u8>` and `Box<[u8]>`, for `&'static [u8]`, `&'static str` and
   `String` (writes), for `Slice`s of any buffer, for registered buffers
   (`FixedBuf`) and for pool leases (`BufLease`). A read fills a buffer from
   its start and marks what it read as initialized; slicing a vector from its
   length reads into the spare capacity. A slice clamps itself to its buffer
   whenever it is used, and zeroes any gap before its start when it marks
   bytes initialized, so a vector never counts memory nothing wrote.
   `bytes::BytesMut` can come as an optional feature. Socket addresses, and
   the `msghdr` and iovec of a `recvmsg`, are boxed in the operation. The
   one exception is a linked timeout's duration, which the kernel copies
   when it consumes the entry (`IORING_FEAT_SUBMIT_STABLE`): it lives in the
   ring's memory for that entry's slot, which is not reused before then, so
   a timeout allocates nothing.
2. **Dropping an in-flight operation hands its resources to the driver.** The
   future moves them into its slot in the operation table, marks the slot
   orphaned and queues a cancellation. They are freed when the final CQE
   arrives. `mem::forget` leaks them instead, which is safe.
3. **Shutdown drains the ring.** Dropping a driver cancels everything in
   flight and waits for the completions. Anything that has not completed
   within a bound (a regular-file read stuck in an io-wq worker, say) is
   leaked, never freed, and only then is the ring closed.

Receives let the kernel choose the buffer from a provided-buffer ring
(`BufPool`), so idle connections do not pin memory. A completed receive
yields a `BufLease` that returns its buffer to the ring when dropped.

Memory registered with the kernel (registered buffers, pools) stays
registered and mapped for as long as anything that might let the kernel
write to it exists: every buffer, lease and operation using it holds a
reference, orphaned operations included. The registration refers to the
driver weakly, so an orphan holding it cannot keep the ring alive in a
cycle.

### Unsafe code policy

- Only `ruio-sys` issues io_uring system calls; only `ruio-driver` touches
  ring memory.
- Every `unsafe` block has a `// SAFETY:` comment, every `unsafe fn` documents
  its contract under `# Safety`, and each block holds one unsafe operation.
  The workspace lint configuration enforces all three and denies
  `unsafe_op_in_unsafe_fn`.

## Driver

### Ring setup

Default flags: `SINGLE_ISSUER | DEFER_TASKRUN | TASKRUN_FLAG | SUBMIT_ALL |
CQSIZE | CLAMP`, with a completion queue four times the submission queue, plus
`NO_SQARRAY` when the kernel has it (6.6). The ring's own descriptor is
registered (`IORING_REGISTER_RING_FDS`) so `io_uring_enter` skips the file
lookup on every call.

Every ring registers a sparse file table for direct descriptors
(`IORING_REGISTER_FILES2` with `IORING_RSRC_REGISTER_SPARSE`): 4096 slots by
default, or fewer if the process's soft `RLIMIT_NOFILE` is lower, since the
kernel refuses larger tables. `Builder::files` sets the size, 0 for none; a
ring whose default table is refused goes without. A ring has at least two
submission queue entries, room for an operation and its linked timeout.

`SQPOLL` and NAPI busy polling (6.9) are opt-in. The kernel rejects
`DEFER_TASKRUN` and `TASKRUN_FLAG` together with `SQPOLL`, so that mode uses
its own flag set.

### Submission and completion

- Operations write SQEs straight into the mapped submission queue; queueing
  costs no system call.
- The executor calls the driver once per scheduler pass. One `io_uring_enter`
  submits everything queued and, if no task is runnable, waits for a
  completion with the next timer deadline as its timeout (`EXT_ARG`). The
  cost is one system call per pass whether one or a thousand operations were
  queued.
- A full submission queue is submitted early instead of failing the
  operation. An operation with a linked timeout needs two entries that reach
  the kernel together, so queueing makes room for both.
- The completion queue is drained fully on every pass. Overflow is handled
  (`NODROP` backlog, `IORING_SQ_CQ_OVERFLOW`) rather than assumed away;
  multishot operations make it realistic.

### Operation table

- `user_data` encodes a slot index and a generation, so completions for a
  reused slot are recognised as stale.
- Slots are preallocated and reused, so the common path allocates nothing per
  operation. The payload (buffer, address, ...) lives in the future, not the
  slot.
- Slot states: free, waiting (with a `Waker`), completed (result and flags,
  until the future collects them), orphaned (the boxed payload of a dropped
  operation), streaming (the queued completions of a multishot request) and
  orphaned stream (the payload of a dropped stream, handed each completion
  until the last).
- Operations are lazy: nothing is queued before the first poll, which leaves
  room for combinators to shape the SQE at no cost. `Op::timeout(d)` queues
  the operation with `IOSQE_IO_LINK` and an `IORING_OP_LINK_TIMEOUT` behind
  it, which posts no completion when disarmed (`IOSQE_CQE_SKIP_SUCCESS`),
  and any it posts is ignored. An operation it cancels completes with
  `ECANCELED`, which the `Op` reports as `ETIMEDOUT`, since nothing else
  cancels an operation whose `Op` exists; and it hands back its buffer, which
  a timer in user space cannot, as it can only drop the `Op`. The driver
  builds the chain itself: an `Operation`'s entry may not link. A general
  `.link(next)` is not built. What `.fixed()` would have chosen is the
  operation's `Fd`.

### Operations

- An operation is a value implementing the unsafe `Operation` trait:
  `prepare` fills in its SQE, and `complete` turns the completion into the
  output. The contract is what makes the soundness model hold: every address
  in the SQE stays valid until `complete` runs, even when the value moves, so
  it may only point to heap memory the value owns (a read's `Vec`, a
  timeout's boxed timespec) or to `'static` memory; and the SQE produces
  exactly one completion.
- Multishot requests implement `Multishot` instead: `item` turns each
  completion into an item, and `Driver::stream` makes a `MultiOp` stream of
  them, which ends when a completion lacks `IORING_CQE_F_MORE` and can be
  re-armed. Dropping an armed stream hands the completions it had not taken
  to the request, cancels it, and keeps handing it completions until the
  last, so items that own resources, such as leased buffers, release them.
  Zero-copy sends fit the same contract, their result carrying
  `IORING_CQE_F_MORE` and the notification ending the request, so `SendZc`
  runs as a stream and resolves with its buffer after the notification.
- Operations act on an `Fd`: a plain descriptor, or a direct one, which goes
  in the SQE with `IOSQE_FIXED_FILE`. `Socket`, `Accept` and `AcceptMulti`
  make a `Descriptor`, plain or, on request, direct (a `DirectFd`, in a slot
  the kernel picks with `IORING_FILE_INDEX_ALLOC`). Dropping a `DirectFd`
  queues an `IORING_OP_CLOSE` of its slot at once, so the close reaches the
  kernel before anything queued later.
- The socket operations are `Socket`, `Connect`, `Accept`, `AcceptMulti`,
  `Send`, `SendTo` (`addr2`, 6.1), `SendZc`, `Recv`, `RecvFrom` (through
  `recvmsg`, for the sender's address) and `Shutdown`. With `MSG_WAITALL` the
  kernel carries on through short sends and receives on a stream socket, so
  one request moves a whole buffer. `SockAddr` holds an address as the
  kernel reads and writes it, and converts exactly to and from the standard
  library's internet and Unix socket addresses.
- `Driver::op` wraps an operation in an `Op` future, which is `Unpin`.
  Dropping it in flight moves the operation into its slot and queues an
  `ASYNC_CANCEL` whose success posts no completion. When the operation's own
  completion arrives, `complete` still runs and its output is dropped, so an
  output that owns a resource, such as a new descriptor, releases it.
- Waking, dropping a replaced waker and finishing an orphan all run after the
  driver's `RefCell` borrow is released, since each can run code that uses
  the driver. An SQE left half-prepared by a panicking `prepare` becomes an
  ignored no-op, so it cannot reach the kernel pointing at freed memory.
- `RecvStream` keeps a multishot receive into a pool armed: when the kernel
  ends it with `ENOBUFS` it waits for a lease to return a buffer (the pool
  wakes waiters), and when the kernel ends it for its own reasons it re-arms
  at once. The data waits in the socket meanwhile, so none is lost.
  `AcceptStream` does the same for a multishot accept, which the kernel ends
  when the completion queue is full; the connections wait in the backlog.
  It yields an error that ends the request and re-arms when polled again, as
  a server carries on after a failed `accept(2)`.

### Kernel features

- Multishot accept, recv and poll, exposed as streams and re-armed
  automatically when the kernel ends them (no `IORING_CQE_F_MORE`) or the
  buffer pool runs dry (`ENOBUFS`); multishot read (6.7) is still to come.
- Direct descriptors: socket and accept install straight into the registered
  file table (`IORING_FILE_INDEX_ALLOC`), so later operations skip the file
  lookup; open will in M5. The driver makes them on request; see the
  decision above for why sockets have plain ones by default.
- Provided-buffer rings, with incremental consumption (6.12) and send/recv
  bundles (6.10) when available.
- Zero-copy send, with the notification CQE keeping the buffer alive, on
  request (`send_zc`). Over loopback the kernel copies the data when it
  reaches the peer's socket, so only a network card can show where
  zero-copy starts to pay; the size threshold for choosing it automatically
  waits for a benchmark across two machines.
- Registered buffers for file I/O (`READ_FIXED`, `WRITE_FIXED`).
- `MSG_RING` for waking other rings and passing descriptors between them.

### Capability probe

`ruio_driver::Probe` uses throwaway rings to learn the kernel's feature bits,
which setup flags it accepts and which opcodes it supports
(`IORING_REGISTER_PROBE`). `Probe::check_baseline` names every missing
capability when the kernel is below the 6.1 baseline. Features past the
baseline are used only when probed.

## Executor

- One executor per thread, driving that thread's ring. `ruio::spawn` takes
  futures that need not be `Send`; `spawn_on(core, future)` (M6) sends a
  `Send` future to another core.
- A task is one `Arc` allocation holding its future and, once it finishes,
  its output, and is its own waker through std's `Wake` trait. Wakers are
  `Send + Sync`, as the `Waker` contract requires, while the future need not
  be, so the future, output and bookkeeping are only touched on the task's
  thread: by the scheduler and by the `JoinHandle`, which is not `Send`. The
  scheduler holds every unfinished task until its future is dropped on that
  thread, and the `JoinHandle` drops the output there, so a waker that
  releases the last reference elsewhere owns nothing that is not `Send`.
- Each loop pass runs up to 64 ready tasks, fires due timers, then turns the
  ring: it submits without waiting if anything is runnable, and otherwise
  parks until a completion, a wake or the next timer. A task woken by the
  running task takes a LIFO slot and runs next, at most three in a row;
  everything else queues in FIFO order. Every io_uring operation yields at
  least once, so a cooperative per-task budget can wait for primitives that
  are often ready at once, such as channels.
- Panics in tasks are caught and reported through `JoinError`; the payload
  only leaves by value, so `JoinError` is `Send + Sync`. Dropping the runtime
  drops every unfinished future on its thread before the driver drains their
  operations.
- Timers live in a hierarchical wheel in user space, with millisecond ticks.
  The kernel sees a single timeout: the wait deadline passed to
  `io_uring_enter`. A thousand concurrent sleeps cost one system call.
- A wake from another thread goes to the runtime's mutex-protected queue and
  unparks its ring. Unparking writes to an eventfd that a parked ring has a
  multishot poll on, which works from any thread, and costs no system call
  unless the ring is parked. `MSG_RING` can take over for wakes between rings
  in M6.
- The builder (M6) pins threads to CPUs and supports one `SO_REUSEPORT`
  listener per core.

### Keeping work stealing possible

- The driver never assumes the task it wakes runs on its thread; it only calls
  `Waker::wake`.
- I/O types are `!Send` through an explicit marker, not because they happen to
  contain an `Rc`, so a work-stealing mode can lift the restriction without
  changing their layout. The sockets and `DirectFd` do this.
- Handles to registered resources (files, buffers) record which ring owns
  them, so an operation from a migrated task can be routed back to that ring
  (`MSG_RING`) or fall back to a plain descriptor. A `DirectFd` names its
  ring by ID and finds it in its thread's registry of drivers, so it neither
  keeps the ring open nor holds an `Rc`; once the ring is gone, a socket
  aims its operations at a slot no table has, so they fail with `EBADF`
  rather than reach the file in the same slot of a later ring.
- The executor sits behind the driver's waker interface in its own crate, so
  another scheduler can be added without touching the driver.

## Networking

`ruio::net` has `TcpListener`, `TcpStream`, `UdpSocket`, `UnixListener`,
`UnixStream` and `UnixDatagram`, running on the ring of the thread's runtime.

- Reads and writes take owned buffers and hand them back with the result.
  `write_all` and `read_exact` ask for `MSG_WAITALL`, and send again what
  the kernel still leaves undone, which also surfaces why it stopped.
  `recv_stream` receives into a `BufPool` with one multishot request, and a
  listener's `incoming` accepts with one; `accept` takes one connection and
  reports the peer. TCP and UDP sockets send zero-copy with `send_zc`.
- Every operation borrows its socket, so the descriptor stays open until the
  kernel has the operation: dropping the socket first does not compile.
- `SocketOp::timeout` sets an operation's linked timeout. `WriteAll` and
  `ReadExact` take one for the whole transfer, each request getting the time
  left. A send cut short part of the way reports the bytes it sent rather
  than `ECANCELED`; the next request, with no time left, is what fails with
  `TimedOut`, and the buffer still comes back.
- Listeners bind with a backlog of 1024, as tokio's do, instead of the
  standard library's 128, which a burst of clients overflows. On request
  (`set_direct`) they accept direct descriptors, which cannot report their
  local address or change socket options, and know the peer's address only
  from `accept`. When the file table is full the kernel closes the
  connection it could not install and fails the accept with `ENFILE`;
  `Incoming` yields that and carries on with plain descriptors.
- Addresses resolve through `ToSocketAddrs`, so a host name blocks the
  thread, as in the standard library. A Unix datagram sent to a path that
  makes no address goes to the kernel as the unnamed address, which fails
  with `EINVAL`, so the buffer comes back with the error.
- Still to come: a `copy` using `splice`, owned-buffer read and write traits
  that files can share, vectored sends, `bind_reuseport` (M6), and socket
  options for direct descriptors on kernels that have them in `URING_CMD`.

## API sketch

Illustrative only; names will change as the API is built.

```rust
#[ruio::main(cores = "all")]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind_reuseport("0.0.0.0:8080")?; // one listener per core
    let mut conns = listener.incoming();                           // multishot accept
    while let Some(conn) = conns.next().await {
        let conn = conn?;
        ruio::spawn(async move {
            let mut rx = conn.recv_stream();                       // multishot recv, provided buffers
            while let Some(chunk) = rx.next().await {              // chunk: BufLease
                conn.send_all(chunk?).timeout(Duration::from_secs(5)).await.0?;
            }
            Ok::<_, std::io::Error>(())
        });
    }
    Ok(())
}
```

Owned-buffer read and write traits carry the usual helpers (`read_exact`,
`write_all`, and `copy` using `splice` between descriptors). The sockets
have `read_exact` and `write_all` of their own so far, and the sketch's
`send_all(chunk).timeout(d)` is `write_all(chunk).timeout(d)`.

## Verification

- **ABI test.** `ruio-sys` compiles a C program against the kernel's uapi
  header and compares every structure size, alignment, field offset and
  constant with the Rust definitions. It prefers the running kernel's header
  (`/usr/src/linux-headers-$(uname -r)`) over `/usr/include`; CI requires it
  to run.
- **Drop tests.** For each operation, poll it N times and then drop it, for
  every N up to completion, and drop pseudo-random mixes of operations on
  small queues. A tracked read's buffer asserts, when freed, that the kernel
  has finished with it. `ci/asan.sh` runs every test under AddressSanitizer
  with leak detection (nightly, `-Zbuild-std`). Miri cannot model io_uring,
  so this is the main safety net.
- **Fault injection.** `ENOBUFS`, `EAGAIN` and `ECANCELED` completions, and
  tiny rings to force a full submission queue and completion overflow, and
  to end multishot accepts with a burst of connections. Tiny file tables
  force `ENFILE`, and zero-length linked timeouts time operations out at
  once.
- **Sockets.** Socket tests run over the loopback interface and Unix
  sockets. Blocking std sockets in them have read timeouts, and waits inside
  the runtime a deadline, so a missing reply fails a test rather than
  hanging it.
- **Feature matrix.** Disable probed features to force the fallbacks, and run
  the 6.1 baseline in a VM (for example virtme-ng), since development hosts
  run newer kernels.

## Benchmarks

Compared against tokio (epoll), monoio, compio and glommio:

- TCP echo, 1 to 10k connections, 64 B to 64 KiB messages
- HTTP/1.1 plaintext
- 4 KiB `O_DIRECT` random reads, compared with fio
- timer churn

Measured: throughput, p99 latency, system calls per operation and CPU time per
request. A CI job fails on regressions.

### TCP echo, as built in M4

`ruio-bench echo` runs each server in a process of its own on one CPU, with
its SMT sibling idle, and the load generator, on tokio's multi-threaded
runtime, on 24 other CPUs. Each connection sends a message and waits for its
echo, again and again, for 3 seconds after 1 second of warm-up, and the
client checks every echo. The servers are single-threaded: ruio with plain
and with direct descriptors, and tokio's current-thread runtime. Both
receive into 16 KiB buffers, ruio from a pool with room for two messages per
connection, tokio into a buffer per connection, and both set `TCP_NODELAY`
on the listener. Round trips per second, measured on a Xeon Platinum 8488C
with Linux 7.0, over loopback:

| Connections | Message | tokio | ruio | ruio, direct | p99 tokio (µs) | p99 ruio (µs) |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 64 B | 48k | 47k (0.99×) | 1.03× | 28 | 28 |
| 1 | 1 KiB | 45k | 46k (1.03×) | 1.03× | 28 | 28 |
| 1 | 16 KiB | 31k | 33k (1.06×) | 1.04× | 40 | 38 |
| 1 | 64 KiB | 19k | 16k (0.86×) | 0.89× | 68 | 74 |
| 16 | 64 B | 212k | 209k (0.98×) | 0.99× | 94 | 136 |
| 16 | 1 KiB | 201k | 197k (0.98×) | 0.99× | 100 | 142 |
| 16 | 16 KiB | 117k | 122k (1.05×) | 1.05× | 189 | 177 |
| 16 | 64 KiB | 42k | 31k (0.74×) | 0.67× | 559 | 660 |
| 256 | 64 B | 205k | 203k (0.99×) | 1.00× | 1304 | 1795 |
| 256 | 1 KiB | 196k | 195k (1.00×) | 1.00× | 1377 | 1875 |
| 256 | 16 KiB | 155k | 147k (0.95×) | 0.95× | 1810 | 2487 |
| 256 | 64 KiB | 49k | 26k (0.53×) | 0.53× | 5535 | 10358 |
| 1,024 | 64 B | 222k | 214k (0.96×) | 0.96× | 4792 | 6643 |
| 1,024 | 1 KiB | 220k | 215k (0.98×) | 0.97× | 4787 | 6596 |
| 1,024 | 16 KiB | 140k | 135k (0.96×) | 0.96× | 7494 | 10261 |
| 1,024 | 64 KiB | 37k | 22k (0.59×) | 0.59× | 28103 | 47903 |
| 10,000 | 64 B | 219k | 181k (0.83×) | 0.83× | 46868 | 79848 |
| 10,000 | 1 KiB | 187k | 155k (0.83×) | 0.83× | 55582 | 92246 |
| 10,000 | 16 KiB | 93k | 79k (0.85×) | 0.84× | 109911 | 168502 |
| 10,000 | 64 KiB | 33k | 18k (0.56×) | 0.55× | 309906 | 761477 |

ruio is not ahead of tokio. Loaded, both servers spend 95% of their CPU in
the kernel: over loopback a server's CPU runs the transmit path of every
echo and the receive processing of the client's socket too, and profiles
put 85% to 90% of its time in the network stack (connection tracking
included, which this host has on) and 5% in copying. What the runtimes
spend comes to about 10% each: tokio 4% in epoll, 2% entering system calls
(cheap with enhanced IBRS) and 4% in user space; ruio a similar share in
io_uring's bookkeeping and 5% in user space. Within that slice ruio loses
where its structure costs it:

- Each piece of a message takes a trip through the loop: the received piece
  wakes the task, and the completion of its send wakes it again, while a
  tokio task reads and writes the whole message in one poll. With 64 KiB
  buffers, 256 connections with 64 KiB messages go from 0.53× to 0.73×, and
  to 0.97× with a pool of 256 buffers, one per connection, as well.
- A pool's ring hands out buffers in the order they came back, so every
  buffer of the pool is in use in turn, and a pool larger than its load
  spreads the copies over more memory than the cache holds: with 64 KiB
  buffers, 4096 of them where 256 would do cost a quarter of the throughput.
  A pool of one buffer per connection runs dry instead, since the next
  message can arrive before the echo's buffer is back, and costs a tenth
  with small messages.
- Sends reach the kernel at the end of a loop pass, after up to 64 tasks
  have run, which puts off every echo in the pass; that shows in the p99.

Direct descriptors save the kernel's file lookups, about 1% here, within the
noise. What could move the rest: bundles (6.10), which receive and send
several buffers per entry; a pool that hands out the buffers returned last
first, keeping only a shallow ring filled from spares, or incremental
consumption (6.12), which packs receives into fewer buffers; vectored and
linked sends; and submitting earlier in a pass for latency. Across two
machines the server no longer does the client's receive processing, which
leaves the runtime a larger share of its time; multi-core runtimes are M6.

## Milestones

| # | Scope | Done when |
|---|---|---|
| M0 | Workspace, lint policy, CI, `ruio-sys` ABI and system calls, capability probe | CI passes; the ABI test passes against the 7.0 header; the probe prints a report |
| M1 | Driver: ring mapping, SQ/CQ, operation table, `Op<T>`, cancel on drop, orphaned operations, batching, shutdown drain | Drop tests pass under AddressSanitizer |
| M2 | Executor: `block_on`, `spawn`, `JoinHandle`, timer wheel, cross-thread wake, macros | Scheduling and timer tests pass |
| M3 | Buffers: `IoBuf` traits, `BufResult`, slices, registered buffers, provided-buffer pools | Pool exhaustion and re-arm tests pass |
| M4 | Net: TCP, UDP, Unix sockets, multishot, zero-copy send, direct descriptors | Echo benchmark ahead of tokio |
| M5 | Fs: open, read, write, fsync, statx, `O_DIRECT`, aligned buffers | Within about 5% of fio on 4 KiB `O_DIRECT` random reads |
| M6 | Multi-core: builder, CPU pinning, `spawn_on`, cross-core channels, descriptor passing | Close to linear scaling up to 96 cores |
| M7 | Compat adapters, hyper and rustls examples, documentation | A hyper server runs through the adapters |

M0 to M3 are done. M4's scope is built, with linked timeouts and the
`.timeout(d)` combinator, but not its criterion: the echo benchmark finds
ruio level with tokio for small messages and behind for 64 KiB messages and
at 10,000 connections (see [TCP echo, as built in M4](#tcp-echo-as-built-in-m4)).
Still to come besides the later milestones: the probed extras for buffers
(multishot read from 6.7, bundles from 6.10, incremental consumption from
6.12); general linked operations (`.link(next)`); and channels and other
synchronization primitives, with the cooperative budget they need.

## Open questions

- License. Crates are `publish = false` until one is chosen.
