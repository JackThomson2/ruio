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

## Architecture

```
crates/
  ruio-sys/       kernel ABI: uapi structures and constants, raw system calls; no policy
  ruio-driver/    one ring: setup, SQ/CQ, operation table, Op<T> futures, buffers,
                  registered files, capability probe
  ruio-executor/  per-thread scheduler, tasks, timer wheel, thread-per-core builder
  ruio-macros/    #[ruio::main], #[ruio::test]
  ruio/           public API: runtime, time, uring; later net, fs, buf, io
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
   `Vec<u8>`, `Box<[u8]>`, `&'static [u8]`, `bytes::BytesMut` (optional
   feature), sub-slices of those, and registered buffers.
2. **Dropping an in-flight operation hands its resources to the driver.** The
   future moves them into its slot in the operation table, marks the slot
   orphaned and queues a cancellation. They are freed when the final CQE
   arrives. `mem::forget` leaks them instead, which is safe.
3. **Shutdown drains the ring.** Dropping a driver cancels everything in
   flight and waits for the completions. Anything that has not completed
   within a bound (a regular-file read stuck in an io-wq worker, say) is
   leaked, never freed, and only then is the ring closed.

Receives let the kernel choose the buffer from a provided-buffer ring, so idle
connections do not pin memory. A completed receive yields a `BufLease` that
returns its buffer to the ring when dropped.

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
  operation.
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
  until the future collects them) and orphaned (the boxed payload of a
  dropped operation). A multishot state, holding a queue of completions,
  arrives with the first multishot operation.
- Operations are lazy: nothing is queued before the first poll, which leaves
  room for combinators such as `.timeout(d)` (a linked timeout),
  `.link(next)` and `.fixed()` to shape the SQE at no cost. Linked entries
  have to be queued together, so they will not go through `Operation`, whose
  entries may not link.

### Operations

- An operation is a value implementing the unsafe `Operation` trait:
  `prepare` fills in its SQE, and `complete` turns the completion into the
  output. The contract is what makes the soundness model hold: every address
  in the SQE stays valid until `complete` runs, even when the value moves, so
  it may only point to heap memory the value owns (a read's `Vec`, a
  timeout's boxed timespec) or to `'static` memory; and the SQE produces
  exactly one completion. Multishot and zero-copy operations, which produce
  several, will get a trait of their own.
- `Driver::op` wraps an operation in an `Op` future, which is `Unpin`.
  Dropping it in flight moves the operation into its slot and queues an
  `ASYNC_CANCEL` whose success posts no completion. When the operation's own
  completion arrives, `complete` still runs and its output is dropped, so an
  output that owns a resource, such as a new descriptor, releases it.
- Waking, dropping a replaced waker and finishing an orphan all run after the
  driver's `RefCell` borrow is released, since each can run code that uses
  the driver. An SQE left half-prepared by a panicking `prepare` becomes an
  ignored no-op, so it cannot reach the kernel pointing at freed memory.
- Until M3's `IoBuf` traits, `Read` and `Write` take a `Vec<u8>`; a read
  appends to the vector's contents, filling its spare capacity.

### Kernel features

- Multishot accept, recv, read (6.7) and poll, exposed as streams and re-armed
  automatically when the kernel ends them (no `IORING_CQE_F_MORE`) or the
  buffer pool runs dry (`ENOBUFS`).
- Direct descriptors: accept, socket and open install straight into the
  registered file table (`IORING_FILE_INDEX_ALLOC`), so later operations skip
  the file lookup. Plain descriptors on request.
- Provided-buffer rings, with incremental consumption (6.12) and send/recv
  bundles (6.10) when available.
- Zero-copy send, with the notification CQE keeping the buffer alive; used
  above a size threshold chosen by benchmark.
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
  changing their layout.
- Handles to registered resources (files, buffers) record which ring owns
  them, so an operation from a migrated task can be routed back to that ring
  (`MSG_RING`) or fall back to a plain descriptor.
- The executor sits behind the driver's waker interface in its own crate, so
  another scheduler can be added without touching the driver.

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
`write_all`, and `copy` using `splice` between descriptors).

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
  tiny rings to force a full submission queue and completion overflow.
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

M0, M1 and M2 are done. Still to come besides the later milestones:
multishot operations, links and the `IoBuf` traits (M3), and channels and
other synchronization primitives, with the cooperative budget they need.

## Open questions

- License. Crates are `publish = false` until one is chosen.
