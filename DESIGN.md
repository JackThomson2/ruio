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
- Kernels older than 6.10.
- Being a drop-in replacement for tokio. Interoperability comes from adapters
  (`ruio-compat`; see [Compatibility](#compatibility)).

## Decisions

| Topic | Decision | Reason |
|---|---|---|
| Execution model | Thread-per-core: one ring and one executor per thread; spawned tasks need not be `Send`. | `SINGLE_ISSUER` and `DEFER_TASKRUN` assume one submitting thread, registered files and buffers belong to a single ring, and the hot path needs no atomics or locks. |
| Work stealing | Not in the first release, but kept possible (see [Keeping work stealing possible](#keeping-work-stealing-possible)). | It fights per-ring state, but some workloads want it. |
| Minimum kernel | Linux 6.10. Detected by capability, not by version string. | 6.1, the oldest LTS with `DEFER_TASKRUN`, had everything the core needs, but 6.10 adds bundles, which send and receive several provided buffers per request: the network path needs them to beat epoll on large messages (see [Benchmarks](#benchmarks)). It also brings `NO_SQARRAY` (6.6), multishot read (6.7) and socket options through `URING_CMD` (6.7). Distribution kernels backport features, so version numbers mislead. |
| Bindings | Our own, in `ruio-sys`: written by hand from the kernel's uapi header and tested against it. | Full control of the ABI surface and the hot path, no bindgen at build time, and new kernel features arrive when we need them. |
| Interop | `futures-io` and `tokio::io` adapters in `ruio-compat`, which hyper and rustls run over. | Ecosystem access matters, but must not shape the core API. |
| Toolchain | Newest stable Rust (1.97.1), edition 2024, pinned in `rust-toolchain.toml`. Nightly only for sanitizers. | No minimum supported Rust version is required. |
| Dependencies | `libc` only, in the core crates. | Small audit surface. |
| Direct descriptors | Supported throughout the driver; `ruio::net` accepts them on request (`set_direct`), plain descriptors by default. | A direct socket cannot report its addresses (`getsockname` through `URING_CMD` is 6.19) and can change options only with `URING_CMD` requests, and a full file table makes the kernel close the connection it accepted. The echo benchmark measures the file lookups they save at about 1%. |

## Architecture

```
crates/
  ruio-sys/       kernel ABI: uapi structures and constants, raw system calls; no policy
  ruio-driver/    one ring: setup, SQ/CQ, operation table, Op<T> futures, buffers,
                  registered files, capability probe
  ruio-executor/  per-thread scheduler, tasks, timer wheel, thread-per-core builder
  ruio-macros/    #[ruio::main], #[ruio::test]
  ruio/           public API: runtime, time, uring, buf, net, fs; later io
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
yields a `BufBundle` of the buffers it filled, as `BufLease`s that return
their buffers to the ring when dropped. A bundle's completion names only
its first buffer; the rest are whichever followed it in the ring. So the
pool keeps what it put in each slot, and reuses a slot only once a
completion has accounted for what the kernel took from it, even when
completions report takes out of order: otherwise a lease could name a
buffer the kernel still owns. It panics rather than lease a buffer that
its slots do not show the kernel took.

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
`NO_SQARRAY`. The ring's own descriptor is
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
  completion. The cost is one system call per pass whether one or a thousand
  operations were queued. Timers wake the ring through its own timer
  (`Driver::wake_at`), a timeout of the kernel's at the earliest deadline,
  so the wait passes no timeout (`EXT_ARG`), which the kernel would copy in
  on every wait.
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
- `Op::detach` hands an operation to the driver to finish with nobody
  waiting: it becomes an orphan, as a dropped `Op`'s does, but is not
  cancelled, and a timed one still completes with `ETIMEDOUT`. An operation
  that should say how it went records that in its `complete`. `Op::into_inner`
  gives back an operation that has not been submitted, which
  `Completion::from_error` can then fail, handing back its buffers.

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
  `Send`, `SendTo` (`addr2`, 6.1), `SendVectored` (`sendmsg` over an
  `IoBufs`, such as a bundle), `SendZc`, `Recv`, `RecvFrom` (through
  `recvmsg`, for the sender's address) and `Shutdown`. With `MSG_WAITALL` the
  kernel carries on through short sends and receives on a stream socket, so
  one request moves a whole buffer, or a whole vector. `SockAddr` holds an
  address as the kernel reads and writes it, and converts exactly to and from
  the standard library's internet and Unix socket addresses.
- The file operations are `OpenAt` (`openat2`, relative to a directory, with
  `RESOLVE_*` flags, into a plain or a direct descriptor), `Statx` (into a
  buffer the operation owns, which the kernel writes as it completes),
  `Fsync`, `Fallocate`, `Ftruncate` (6.9), `UnlinkAt`, `MkdirAt`, `RenameAt`
  and `Close`. The kernel copies paths as it takes the entry, and resolves
  them against plain descriptors only. `Close` owns its descriptor and gives
  it up to the kernel as its entry is prepared, so that the number, which
  another file may have by then, is never closed twice; `DirectFd::close`
  closes a direct descriptor with a result, on its own ring.
- `Driver::op` wraps an operation in an `Op` future, which is `Unpin`.
  Dropping it in flight moves the operation into its slot and queues an
  `ASYNC_CANCEL` whose success posts no completion. When the operation's own
  completion arrives, `complete` still runs and its output is dropped, so an
  output that owns a resource, such as a new descriptor, releases it.
- Waking, dropping a replaced waker and finishing an orphan all run after the
  driver's `RefCell` borrow is released, since each can run code that uses
  the driver. An SQE left half-prepared by a panicking `prepare` becomes an
  ignored no-op, so it cannot reach the kernel pointing at freed memory.
- `RecvStream` keeps a multishot receive into a pool armed, asking for
  bundles (`IORING_RECVSEND_BUNDLE`), so each item is all the data one
  receive found waiting, in as many buffers as it needed. When the kernel
  ends the receive with `ENOBUFS` it waits for a lease to return a buffer
  (the pool wakes waiters), and when the kernel ends it for its own reasons
  it re-arms at once. The data waits in the socket meanwhile, so none is
  lost. The kernel does not bundle a request's first receive, not knowing
  yet how much is waiting.
  `ReadStream` keeps a multishot read into a pool armed the same way, one
  buffer to a read, for files the kernel can poll: pipes, FIFOs and
  character devices; it refuses others with `EBADFD`. A file that does
  not say it can be read without blocking, and is not `O_NONBLOCK`, as a
  FIFO is not, it reads only while the file polls as readable, so a FIFO
  whose writers have gone, which polls only as hung up, would never end:
  such a file is opened `O_NONBLOCK` for streaming, which ruio leaves to
  the caller, as the flag belongs to the file description and so to
  everyone sharing it.
  `AcceptStream` does the same for a multishot accept, which the kernel ends
  when the completion queue is full; the connections wait in the backlog.
  It yields an error that ends the request and re-arms when polled again, as
  a server carries on after a failed `accept(2)`.

### Kernel features

- Multishot accept, recv, read (6.7) and poll, exposed as streams and
  re-armed automatically when the kernel ends them (no
  `IORING_CQE_F_MORE`) or the buffer pool runs dry (`ENOBUFS`).
- Direct descriptors: socket and accept install straight into the registered
  file table (`IORING_FILE_INDEX_ALLOC`), so later operations skip the file
  lookup, and so does open. The driver makes them on request; see the
  decision above for why sockets have plain ones by default.
- Provided-buffer rings, which receives fill in bundles (6.10). Vectored
  sends take bundles back out rather than send bundles, which would need a
  second ring holding the data to send. Incremental consumption (6.12) is
  still to come.
- Zero-copy send, with the notification CQE keeping the buffer alive, on
  request (`send_zc`). Over loopback the kernel copies the data when it
  reaches the peer's socket, so only a network card can show where
  zero-copy starts to pay; the size threshold for choosing it automatically
  waits for a benchmark across two machines.
- Registered buffers for file I/O (`READ_FIXED`, `WRITE_FIXED`).
- `MSG_RING` to move direct descriptors from one ring's file table to
  another's (`DirectFd::send_to`). The other ring posts no completion
  (`IORING_MSG_RING_CQE_SKIP`): the sender's says which slot the file went
  to, and comes only once it is there. Waking other rings with `MSG_RING`
  is still to come.

### Capability probe

`ruio_driver::Probe` uses throwaway rings to learn the kernel's feature bits,
which setup flags it accepts and which opcodes it supports
(`IORING_REGISTER_PROBE`). `Probe::check_baseline` names every missing
capability when the kernel is below the 6.10 baseline. Features past the
baseline are used only when probed.

## Executor

- One executor per thread, driving that thread's ring. `ruio::spawn` takes
  futures that need not be `Send`; `spawn_on(core, future)` sends a `Send`
  future to another core, and `Handle::spawn_with` a closure that makes
  the future there, which then need not be `Send`: it can hold a socket of
  that core's ring across its awaits.
- A task is one `Arc` allocation holding its future and, once it finishes,
  its output, and is its own waker, through a waker vtable of its own. Wakers
  are `Send + Sync`, as the `Waker` contract requires, while the future need
  not be, so the future, output and bookkeeping are only touched on the
  task's thread: by the scheduler and by the `JoinHandle`, which is not
  `Send`. The scheduler holds every unfinished task until its future is
  dropped on that thread, and the `JoinHandle` drops the output there, so a
  waker that releases the last reference elsewhere owns nothing that is not
  `Send`.
- Waking and running a task on its own thread takes no atomic operation. A
  run lends its poll a waker that borrows the scheduler's reference to the
  task, and takes a reference only when the future clones it. A wake on the
  task's thread queues it with a plain flag; a wake from another thread sets
  an atomic one and queues it on the remote queue, and a run clears that
  flag, with a full barrier, only if it is set. Moving remote wakes to the
  run queue checks the plain flag, so a task is never queued twice.
- Each loop pass runs up to 64 ready tasks, fires due timers, then turns the
  ring: it submits without waiting if anything is runnable, and otherwise
  parks until a completion, a wake or the next timer. A task woken by the
  running task takes a LIFO slot and runs next, at most three in a row;
  everything else queues in FIFO order.
- Every io_uring operation yields at least once, as its completion comes
  through the ring, but a channel or a lock can be ready every time it is
  polled. So each poll of a task, and of the future given to `block_on`,
  has a cooperative budget of 128 steps, in a thread-local cell. The
  primitives of `sync` and `JoinHandle` check it before they take
  anything, and spend a step when they are ready; once none are left they
  wake the task and return `Pending`, and it runs again after the others.
  Outside a runtime there is no budget.
- Panics in tasks are caught and reported through `JoinError`; the payload
  only leaves by value, so `JoinError` is `Send + Sync`. Dropping the runtime
  drops every unfinished future on its thread before the driver drains their
  operations.
- Timers live in a hierarchical wheel in user space, with millisecond ticks.
  The kernel sees a single timeout, the ring's timer at the wheel's next
  deadline, which moves only for an earlier one: a thousand concurrent
  sleeps cost one. A pass reads the clock and fires timers only once that
  timer has fired; before the wheel's next deadline, firing does nothing,
  not even move the wheel's time on, and a timer queued meanwhile goes on a
  higher level and moves down as time comes to it.
- A sleep is `Send` and `Sync`, so that a future given to `spawn_on`, or a
  sleep hyper asks of a timer, can hold one. Its timer is in the wheel of
  the runtime that polls it, which it names by the runtime's shared part.
  On that runtime's thread, inside it, it reaches the wheel through the
  thread's current runtime, with no lock. Polled by another
  runtime, it arms its timer there; dropped, or moved away, where its wheel
  cannot be reached, it queues the timer's key beside the wakes from other
  threads, and the runtime's next turn removes it.
- A wake from another thread goes to the runtime's mutex-protected queue,
  sets a flag so that a pass takes the lock only when the queue may hold
  tasks, and unparks its ring. Unparking writes to an eventfd that a parked
  ring has a multishot poll on, which works from any thread, and costs no
  system call unless the ring is parked. Spawns from other threads go the
  same way, and their thread makes them tasks outside the queue's lock.
- `Cores` runs a runtime on each of several CPUs, thread per core: every
  CPU the process may run on (its main thread's, as `taskset` or a cgroup's
  cpuset limit them, whichever thread asks), the first few, or a list.
  Each core's thread is pinned to its CPU before its runtime is built, so
  that the ring is local to it and has the thread as its one submitter.
  The cores meet before any `main` starts, each putting in its `Handle`, so
  that every core can reach every other (`spawn_on`, `handle_of`). A core
  whose `main` has returned keeps running what the others spawn onto it
  until every `main` has returned, and then drops its tasks while its
  thread is still that core; a core that cannot start keeps every `main`
  from running. `#[ruio::main(cores = "all")]`, or `cores = N`, runs `main`
  so, and returns `()` or the first core's error. The channels of `sync`,
  `oneshot` and a bounded `mpsc`, carry values between cores, and wake a
  task wherever it runs.
- `sync` also has a `Semaphore`, and on it a `Mutex` and an `RwLock` whose
  guards may be held across awaits, and `Notify`. Like the channels they
  keep their state behind a standard mutex, wake outside it, and work
  across cores. The semaphore serves its waiters in the order they came,
  each with all the permits it asked for at once, so one that needs many
  is not passed by later ones that need fewer, and a writer, which takes
  every permit a reader takes one of, cannot be starved by readers. A
  waiter dropped after its permits were set aside gives them back, and a
  `Notified` dropped after `notify_one` chose it passes the notification
  on, so cancelling a wait loses nothing.

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
  `recv_stream` receives into a `BufPool` with one multishot request,
  yielding a bundle per receive, which `write_all_vectored` or
  `write_behind` writes back with one send; a listener's `incoming` accepts
  with one request, and `accept` takes one connection and reports the peer.
  TCP and UDP sockets send zero-copy with `send_zc`.
- A stream's `write_behind` writes with a detached vectored send, and
  resolves once the send is on its way; the driver drops the buffers when the
  kernel is done. A task echoing what it receives is so polled once per
  message rather than twice. The stream's writes stay in order, one at a
  time: a write behind, every other write and `shutdown` first wait for the
  write behind in flight. Nobody waits for a write behind, so its failure
  comes out later: it fails every write after it, handing buffers back
  unsent, as the stream now has a gap, and `flush` waits for it and reports
  it. The kernel reports a `MSG_WAITALL` send it stopped short as the bytes
  sent, so one cut short counts as timed out if it had a timeout and as a
  broken pipe otherwise. The failure is kept as an errno, so the writes it
  fails report it as the kernel would have.
- Every operation borrows its socket, so the descriptor stays open until the
  kernel has the operation: dropping the socket first does not compile.
  An operation that outlives its future, as a write behind or a cancelled
  receive does, still sits in the submission queue until the end of the
  loop pass, so a plain descriptor closes as a direct one does: with an
  `IORING_OP_CLOSE` queued on the ring of the thread's runtime
  (`Driver::close_in_order`), which the kernel takes after everything
  queued before it, and at once only outside a runtime. Closed with
  `close(2)` as it is dropped, a socket would lose what it wrote behind
  just before, the send failing with `EBADF`, or reaching whichever file
  took the number. Listeners and files close so too.
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
- `TcpListener::bind_reuseport` and `UdpSocket::bind_reuseport` set
  `SO_REUSEPORT` before binding, so that a socket on each core can bind one
  address, and the kernel spreads the connections, or the datagrams, among
  them by a hash of their addresses and ports. The standard library sets no
  option before it binds, so these make the socket themselves.
- `TcpStream::send_to(core)` moves a stream to another core, once a write
  behind in flight has finished, so that what that core writes comes after
  it: a plain descriptor as it is, and a direct one with `MSG_RING`. The
  `SentTcpStream` it makes is `Send`, and a task on that core takes it back
  with `receive`.
- Still to come: a `copy` using `splice`, owned-buffer read and write traits
  that files can share, and socket options for direct descriptors through
  `URING_CMD`.

## Files

`ruio::fs` has `File`, `OpenOptions` and `Metadata`, and the functions
`metadata`, `symlink_metadata`, `read`, `read_to_string`, `write`,
`create_dir`, `create_dir_all`, `remove_file`, `remove_dir` and `rename`,
running on the ring of the thread's runtime.

- Reads and writes take owned buffers and hand them back with the result, at
  an offset, so any number can run at once. `read_exact_at` and
  `write_all_at` take as many as a buffer needs; `read_fixed_at` and
  `write_fixed_at` read and write registered buffers. `read` and `write` use
  the file's position, which nothing orders when they overlap.
  `read_stream` reads what arrives on a pipe, a FIFO or a character device
  into a pool, with one multishot read that stays armed.
- Every operation borrows its file, so the file cannot close before the
  kernel has the operation. `close` closes it with a result, which dropping
  it cannot report.
- `OpenOptions` mean what the standard library's do, and take custom flags,
  such as `O_DIRECT`, and `direct_descriptor`, which opens the file into the
  ring's table: its operations skip the file lookup, but it has no status to
  ask for, as statx takes plain descriptors only. Opening goes through
  `openat2`, which refuses a mode unless the open creates the file; the
  driver passes one only then, as `openat` ignores it otherwise.
- Direct I/O needs memory, offsets and lengths aligned as
  `Metadata::dio_alignment` says (`STATX_DIOALIGN`): an `AlignedBuf`, or a
  registered buffer whose size is a multiple of the page size. The kernel
  refuses anything else with `EINVAL`.
- The kernel always runs statx, fsync, fallocate, ftruncate and the
  operations on names in an io-wq worker, and an open whose lookup is not
  cached; so buffered reads and writes that miss the page cache. A direct
  read goes to the device from the ring.
- Still to come: reading directories, which io_uring has no operation for,
  `copy` with `splice`, links and permissions.

## Compatibility

Most of the ecosystem takes its I/O through poll-based traits, tokio's
`AsyncRead` and `AsyncWrite` or those of futures-io, which lend the
caller's buffer for one call; the kernel cannot use it once the call
returns. `ruio_compat::Compat` adapts a `TcpStream` or a `UnixStream` to
both families, and `AsyncBufRead` besides, with buffers of its own. tokio's
traits need none of tokio's runtime.

- A read receives into the adapter's buffer, 16 KiB unless made
  `with_capacity`, and copies out what the caller's takes; the rest waits
  for the next read. The receive stays in flight between polls, so nothing
  that arrives while the caller does something else is lost; dropping the
  adapter cancels it. Made `with_pool`, an adapter receives into a pool
  instead, with one multishot receive kept armed from the first read, so
  that a read costs the ring no request, and a buffer goes back to the pool
  as soon as it has been read out. A connection then holds buffers only
  while data waits, but as many as that data fills: a pool shared by
  connections suits readers that read what arrives promptly, as hyper does.
- A write copies up to 256 KiB of the caller's bytes and writes them behind
  the stream: done, as far as the caller can tell, once the send is on its
  way. The next write waits for it, and a failure fails the writes, flushes
  and closes after it, as for the stream's own writes behind. A dropped
  adapter leaves its send to finish, and the stream closes after it.
- A flush has nothing to wait for, since the adapter holds nothing back: it
  reports a failure if one is known, as a socket's flush would. hyper
  flushes every response, and a flush that waited for the kernel would
  cost each another wake of its task (see [HTTP/1.1](#http11)). Closing
  (tokio's `shutdown`) waits for the send in flight, then shuts the
  writing half down.
- The adapter runs its receives and its shutdown on the thread's ring,
  which it keeps from its first operation, so that a direct descriptor's
  slot always names the stream's file.

hyper-util's `TokioIo` gives hyper's own traits to anything with tokio's,
so `TokioIo::new(Compat::with_pool(&pool, stream))` is a connection hyper
serves, or a client's; HTTP/1.1 connections need no executor, and ruio's
tasks need not be `Send`. tokio-rustls runs TLS over an adapter as it is,
and its streams have tokio's traits too. The examples of `ruio-compat`
serve HTTP/1.1 and HTTPS so on every core.

hyper's timeouts, such as the one for reading a request's head, need a
timer, whose sleeps hyper boxes as `Send` and `Sync`. `ruio_compat::Timer`,
behind the `hyper` feature, on by default, is one whose sleeps are ruio's:
hyper polls them in the task serving the connection, so their timers go in
the wheel of that task's runtime, and a reset moves a sleep's deadline in
place. Given it, hyper's server cuts off a client that takes longer than
30 seconds, unless set otherwise, to send a request's head, as the
examples' servers do. hyper's upgrades need `Send` streams, which ruio's
are not.

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
`send_all(chunk).timeout(d)` is `write_all(chunk).timeout(d)`;
`#[ruio::main(cores = "all")]` and `bind_reuseport` work as sketched.

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
- **Files.** File tests run on files in scratch directories under the
  temporary directory, including direct I/O, at the alignment the file
  system reports; a file system without direct I/O skips those.
- **Sockets.** Socket tests run over the loopback interface and Unix
  sockets. Blocking std sockets in them have read timeouts, and waits inside
  the runtime a deadline, so a missing reply fails a test rather than
  hanging it.
- **Adapters.** The tests of `ruio-compat` run both trait families over TCP
  and Unix sockets, reading into a buffer and into pools too small for
  what arrives. hyper serves hyper's client through the adapters, over TLS
  too, and blocking clients of the standard library's and of rustls's,
  which read to the end of the stream. Given ruio's timer, hyper serves
  requests whose heads come in time over a connection that outlasts the
  timeout, then times out a head that never ends.
- **Feature matrix.** Disable probed features to force the fallbacks, and run
  the 6.10 baseline in a VM (for example virtme-ng), since development hosts
  run newer kernels.

## Benchmarks

Compared against tokio (epoll), monoio, compio and glommio:

- TCP echo, 1 to 10k connections, 64 B to 64 KiB messages
- HTTP/1.1 plaintext
- 4 KiB `O_DIRECT` random reads, compared with fio
- timer churn

Measured: throughput, p99 latency, system calls per operation and CPU time per
request. A CI job fails on regressions.

### TCP echo

`ruio-bench echo` runs each server in a process of its own on one CPU, with
its SMT sibling idle, and the load generator, on tokio's multi-threaded
runtime, on 24 other CPUs. Each connection sends a message and waits for its
echo, again and again, for 3 seconds after 1 second of warm-up, and the
client checks every echo. The servers are single-threaded, with a 16 KiB
buffer per connection each: tokio's current-thread runtime reads into its
connection's buffer and writes back what it read; ruio receives into a pool
of as many buffers, gathers whatever has arrived and writes it back behind,
with one vectored send. Both set `TCP_NODELAY` on the listener. Round trips
per second, medians of three runs taken in turns, on a Xeon Platinum 8488C
with Linux 7.0, over loopback:

| Connections | Message | tokio | ruio | p99 tokio (µs) | p99 ruio (µs) |
|---:|---:|---:|---:|---:|---:|
| 1 | 64 B | 48k | 47k (0.99×) | 28 | 28 |
| 1 | 1 KiB | 45k | 45k (1.01×) | 29 | 28 |
| 1 | 16 KiB | 32k | 32k (1.01×) | 40 | 39 |
| 1 | 64 KiB | 18k | 14k (0.78×) | 72 | 76 |
| 16 | 64 B | 211k | 207k (0.98×) | 94 | 100 |
| 16 | 1 KiB | 200k | 195k (0.97×) | 101 | 106 |
| 16 | 16 KiB | 117k | 122k (1.04×) | 187 | 176 |
| 16 | 64 KiB | 42k | 46k (1.09×) | 558 | 476 |
| 256 | 64 B | 204k | 206k (1.01×) | 1306 | 1339 |
| 256 | 1 KiB | 193k | 198k (1.03×) | 1395 | 1399 |
| 256 | 16 KiB | 155k | 163k (1.06×) | 1814 | 1739 |
| 256 | 64 KiB | 49k | 50k (1.01×) | 5442 | 6106 |
| 1,024 | 64 B | 222k | 227k (1.03×) | 4774 | 4710 |
| 1,024 | 1 KiB | 219k | 233k (1.06×) | 4795 | 4614 |
| 1,024 | 16 KiB | 140k | 155k (1.10×) | 7492 | 7477 |
| 1,024 | 64 KiB | 37k | 40k (1.08×) | 30183 | 27317 |
| 10,000 | 64 B | 217k | 222k (1.02×) | 47404 | 46561 |
| 10,000 | 1 KiB | 187k | 192k (1.03×) | 55255 | 53736 |
| 10,000 | 16 KiB | 92k | 89k (0.97×) | 111754 | 113884 |
| 10,000 | 64 KiB | 33k | 31k (0.96×) | 310059 | 323082 |

ruio is ahead of tokio from 256 connections on, by up to 10%, bar 10,000
connections with large messages (0.96× to 0.97×), and level at 1 and 16
connections, bar one connection with 64 KiB messages (0.78×). Its 99th
percentile is within 3% of tokio's or better, bar 256 connections with 64 KiB
messages (12% later) and those two large-message loads at 10,000 (2% to 4%).
Direct descriptors change nothing measurable. Runs of the same load differ by
up to a few percent on this host, which is shared.

Loaded, both servers spend 95% or more of their CPU in the kernel: over
loopback a server's CPU runs the transmit path of every echo and the receive
processing of the client's socket too, and the network stack and copying take
most of it (connection tracking included, which this host has on, and zeroing
every page it allocates). What the runtimes themselves spend is a few percent
each: tokio's epoll, system calls and user space against ruio's io_uring
bookkeeping and user space.

What got ruio here, in order of effect:

- **Bundles and vectored sends.** A message larger than a buffer took a
  receive, a send and a trip through the loop per buffer, and ruio ran at
  0.53× to 0.74× of tokio with 64 KiB messages; it now takes a receive or two
  and one send.
- **Writing behind.** An echo took two polls of its task, one for the data and
  one for the send's completion, where tokio's write completes within its one
  poll; with 10,000 connections that state is cold, and ruio's user-space
  share rose from 5% to 15%. A detached send needs no poll, which took 10,000
  connections from 0.79× to 1.02× and the 99th percentile from half as long
  again as tokio's to about the same, each echo waking its task once.
- **A small pool.** A pool's ring hands out buffers in the order they came
  back, so every buffer of the pool is in use in turn, and a pool larger than
  its load spreads the copies over more memory than the cache holds. With
  echoes written behind, a buffer goes back once its data is sent, and data a
  pool has no room for waits in the socket, so a buffer per connection is
  enough: with 1024 connections and 64 KiB messages, 8192 buffers where 1024
  do cost a fifth of the throughput.

What is left:

- tokio's echo is cut-through: it writes each 16 KiB as soon as it has read
  it, so the client receives while the server still reads. The kernel
  receives a whole message for ruio in one pass. With one connection, which
  is bound by latency, that costs ruio a fifth with 64 KiB messages; given
  64 KiB buffers, so that it too echoes whole messages, tokio does the same
  as ruio, 13.7k round trips per second. With 10,000 connections the same
  holds for the cache: tokio's 16 KiB stays in the first-level cache between
  read and write, where ruio's 64 KiB comes back from further out, and its
  sends' copies cost seven times as much (16% of the CPU against 2%).
- Sends reach the kernel at the end of a loop pass, after up to 64 tasks have
  run, which costs a few microseconds of latency with few connections.
- Each entry into the kernel runs at most 20 items of deferred work, such as
  receives, besides what it waits for.

Four experiments did not pay. Submitting after every task brings the 99th
percentile at 16 connections to tokio's, but costs 4% to 6% of the
throughput. Submitting a send within the poll of its write costs 8% to 12%
with small messages and 38% with 10,000 connections: every extra entry into
the kernel runs everyone's pending work and breaks up the batching; writing
behind removed the second poll instead. A shallow ring, holding only as many
buffers as the load needs and refilled with the ones returned last, turns
starvation into the common case, and the pool wakes every waiting stream for
each buffer that returns: 10,000 connections fell to 0.12×. Huge pages for the
pool (`MADV_HUGEPAGE`, which the kernel granted) changed nothing measurable.
Across two machines the server no longer does the client's receive
processing, which leaves the runtime a larger share of its time. How
runtimes on several cores fare is under
[Scaling across cores](#scaling-across-cores).

### Random reads with direct I/O

`ruio-bench randread PATH` compares 4 KiB random reads with direct I/O
against the io_uring engine of fio 3.43, each in a process of its own pinned
to the same CPU, three runs each in turns, and reports the medians. The
device is the kernel's null block device (`null_blk`, 16 GiB, 4 KiB blocks,
completing reads inline with `irqmode=0`), which moves no data, so what the
comparison measures is what each spends per read. fio runs at its best:
registered buffers and files, no latency statistics (`gtod_reduce`), and
reads submitted up to 32 at a time; by default it enters the kernel for every
read, and reads about 1.3M a second whatever the depth. ruio's reader runs a
task per outstanding read, each reading a registered buffer at a random
offset of a file opened for direct I/O as a direct descriptor. Reads per
second, on a Xeon Platinum 8488C with Linux 7.0:

| Depth | fio | ruio |
|---:|---:|---:|
| 1 | 1.32M | 1.33M (1.01×) |
| 4 | 1.89M | 1.93M (1.02×) |
| 16 | 2.33M | 2.34M (1.00×) |
| 32 | 2.49M | 2.51M (1.01×) |
| 64 | 2.51M | 2.42M (0.97×) |
| 128 | 2.41M | 2.28M (0.95×) |

ruio is within 5% of fio at every depth, and level with it up to 32. It
started at 0.82× at depth 1 and 0.93× at 32, where its user space took 29%
of the CPU; four things were in the way, all of them the runtime's:

- Waking and running a task took six locked instructions, which serialize
  the pipeline. On the task's own thread four of them are gone (see
  [Executor](#executor)), which gained 5% at depth 32; cloning the waker an
  operation keeps, and dropping the scheduler's reference after a run,
  remain.
- With a timer pending, as the benchmark's own measuring sleep is, a pass
  read the clock twice, walked the wheel twice, and parked with a timeout the
  kernel copied in on every wait: a tenth of a read at depth 1, where every
  read is a pass. The ring's timer now does it with one kernel timeout.
- Each pass took the lock of the remote wake queue twice, to find it empty.
- Each operation on a direct descriptor searched the thread's drivers for
  its ring.

At depth 64 and above ruio falls 3% to 5% behind, for a reason still to
find; the kernel runs at most 20 items of deferred work per entry, which
both sides live with.

### Scaling across cores

`ruio-bench scale` runs a load on more and more cores, every core loading
itself: the echo benchmark's server on a port of its own, and 32
connections of its own to it, each sending a 64 B message and waiting for
its echo, again and again, for 3 seconds after 1 second of warm-up.
Nothing crosses from core to core, so what the cores together lose comes
from what they share. ruio runs on `Cores`, and tokio, as a reference, on a
current-thread runtime per core, on a thread pinned to it; each run is a
process of its own, and the runtimes take turns. Round trips per second,
medians of three runs, on the Xeon Platinum 8488C with Linux 7.0, over
loopback, on CPUs 0 up: CPUs n and n + 48 are the two hardware threads of
one of its 48 cores. Scaling is the rate per core against one core's:

| Cores | tokio | Scaling | ruio | Scaling | Busy loop |
|---:|---:|---:|---:|---:|---:|
| 1 | 214k | 100% | 242k (1.13×) | 100% | 100% |
| 2 | 426k | 99% | 479k (1.12×) | 99% | |
| 4 | 855k | 100% | 963k (1.13×) | 99% | |
| 8 | 1.71M | 100% | 1.92M (1.12×) | 99% | |
| 16 | 3.40M | 99% | 3.82M (1.12×) | 99% | 97% |
| 24 | 4.83M | 94% | 5.38M (1.11×) | 93% | 94% |
| 32 | 6.08M | 89% | 6.67M (1.10×) | 86% | 90% |
| 48 | 8.16M | 79% | 8.56M (1.05×) | 74% | 80% |
| 64 | 8.39M | 61% | 8.62M (1.03×) | 56% | 58% |
| 96 | 8.60M | 42% | 9.05M (1.05×) | 39% | 39% |

The last column is what the host itself gives a busy loop on each core,
a Python loop in a process pinned to each CPU, sharing nothing: with 48
cores busy, 80% as much per core as with one, although the hypervisor
reports no time stolen and the clock it reports falls by only 4%; and
past 48, as the cores' second hardware threads join in, hardly more for
all of them together. ruio scales linearly up to 16 cores, and from there
keeps within 8% of what the busy loop gets, level with it from 64 cores
on: the host is what stops it. Past 48 cores the second hardware threads
add 5% to 6% for both runtimes. tokio keeps a few points closer to the busy
loop at 32 and 48 cores, for a reason still to find, and ruio's lead over
it narrows from 12% to 13% up to 16 cores to 3% to 5% from 48 on.

### HTTP/1.1

`ruio-bench http` runs hyper 1.11 serving HTTP/1.1 on one CPU as the echo
benchmark runs its servers, with the same load generator: each connection
sends a GET and waits for the response, a 13-byte plaintext body with its
head, again and again, for 3 seconds after 1 second of warm-up, and the
client checks every response. The server's code is the same on both
runtimes: on ruio through `Compat` and hyper-util's `TokioIo`, each
connection receiving into a buffer of its own, or into a pool of a buffer
per connection; and on tokio's current-thread runtime, through `TokioIo`.
Neither gives hyper a timer, so neither has hyper's timeouts. Requests per
second, medians of three runs taken in turns, on the Xeon Platinum 8488C
with Linux 7.0, over loopback:

| Connections | tokio | ruio | ruio, pool | p99 tokio (µs) | p99 ruio (µs) | p99 ruio, pool (µs) |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 45k | 44k (0.98×) | 45k (1.00×) | 28 | 28 | 28 |
| 16 | 191k | 172k (0.90×) | 181k (0.95×) | 104 | 125 | 119 |
| 256 | 177k | 169k (0.95×) | 180k (1.01×) | 1534 | 1628 | 1540 |
| 1,024 | 186k | 183k (0.98×) | 192k (1.03×) | 5656 | 5746 | 5512 |
| 10,000 | 171k | 170k (0.99×) | 176k (1.03×) | 60765 | 60278 | 58366 |

Receiving into a pool, hyper on ruio is level with hyper on tokio with one
connection, and from 256 connections on 1% to 3% ahead, with a 99th
percentile as short or shorter; at 16 connections it is 5% behind. Loaded,
both spend about 83% of their CPU in the kernel, 13% in hyper and the
runtime, and 4% in the C library; the adapters' copies, of a request out
of the pool's buffers and of a response into a buffer for the kernel, add
little to that.

Two things got the adapters there:

- **A flush that does not wait.** hyper flushes every response. Waiting for
  the kernel to finish the send woke the task again for each, as a write
  that waited did in the echo benchmark: 0.76× at 10,000 connections,
  with a 99th percentile four times as long, 259 ms.
- **A multishot receive into a pool.** Reading into a buffer of its own, a
  connection submits a receive for each read, which the kernel, finding
  nothing waiting so soon after the last response, parks on a poll; a
  multishot receive stays armed. That is worth 4% to 6% from 16
  connections on.

What is left: at 16 connections, for a reason still to find, ruio's CPU
goes where tokio's does, bar the kernel's receive path, where
`tcp_recvmsg_locked` and `__tcp_cleanup_rbuf` take twice their share under
tokio, besides io_uring's own task work and poll checks.

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

M0 to M7 are done. M4 built linked timeouts and the `.timeout(d)`
combinator, bundles on the 6.10 baseline and writing behind, and the echo
benchmark finds ruio ahead of tokio from 256 connections on, bar 10,000 with
large messages, and level with fewer, bar one connection with 64 KiB messages,
where tokio's echo is cut-through (see [TCP echo](#tcp-echo)). M5 built
`ruio::fs` and buffers aligned for direct I/O, and ruio's direct reads keep
within 5% of fio at every queue depth (see
[Random reads with direct I/O](#random-reads-with-direct-io)). M6 built
thread per core: `Cores`, which runs a runtime on a thread pinned to each
CPU and lets the cores reach each other (`spawn_on`, `handle_of`,
`Handle::spawn_with`), `#[ruio::main(cores = ...)]`, channels between
cores, a listener on each core with `bind_reuseport`, and streams that
move between cores, direct descriptors with `MSG_RING`. With every core
loading itself, ruio scales linearly up to 16 cores, and up to 96 keeps
within 8% of what the host gives a busy loop (see
[Scaling across cores](#scaling-across-cores)). M7 built `ruio-compat`:
`Compat`, which gives ruio's TCP and Unix streams the `AsyncRead`,
`AsyncBufRead` and `AsyncWrite` of `tokio::io` and of futures-io, reading
into a buffer of its own or into a pool with one multishot receive, and
writing behind, with a flush that need not wait for the kernel; examples
that serve HTTP/1.1 with hyper and HTTPS with rustls on every core; and an
HTTP/1.1 benchmark, in which hyper on ruio, receiving into a pool, is
level with hyper on tokio with one connection and 1% to 3% ahead from 256
on (see [HTTP/1.1](#http11)). On the way, plain descriptors came to close
through the ring, after the operations queued on them, as direct ones do:
a stream dropped just after a write behind lost what it wrote.

Since M7:

- Sleeps are `Send` and `Sync`, and `ruio_compat::Timer` gives hyper
  ruio's timers, for its timeouts (see [Compatibility](#compatibility)).
- A cooperative budget keeps a task that always finds its channel or lock
  ready from holding up the others, and `sync` has semaphores, mutexes,
  reader-writer locks and `Notify` (see [Executor](#executor)).
- Multishot reads stream what arrives on pipes, FIFOs and character
  devices (`File::read_stream`).

Still to come: incremental consumption (6.12); general linked operations
(`.link(next)`); reading directories; and waking rings with `MSG_RING`.

## Open questions

- License. Crates are `publish = false` until one is chosen.
