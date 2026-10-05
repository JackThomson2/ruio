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

A driver gets a second ring, with the same flags plus `IOPOLL`, once an
operation asks to be polled, or at once with `Builder::polled`; see
[Kernel features](#kernel-features) for what it is for and how parking
works with it.

### Submission and completion

- Operations write SQEs straight into the mapped submission queue; queueing
  costs no system call.
- The executor calls the driver once per scheduler pass. One `io_uring_enter`
  submits everything queued and, if no task is runnable, waits for a
  completion. The cost is one system call per pass whether one or a thousand
  operations were queued, unless more than 32 of them are reads and writes
  (below). Timers wake the ring through its own timer
  (`Driver::wake_at`), a timeout of the kernel's at the earliest deadline,
  so the wait passes no timeout (`EXT_ARG`), which the kernel would copy in
  on every wait. A turn borrows the driver's state once, for the entry,
  the reap and the overflow check; wakers other than the executor's,
  orphans to finish and streams to hand over to are taken out only if
  there are any, to run once the state is released. The timer's state
  lives in cells beside it, which the scheduler reads without a borrow.
- A wait leaves itself out of iowait accounting (`IORING_ENTER_NO_IOWAIT`,
  Linux 6.15, probed through `IORING_FEAT_NO_IOWAIT`). The kernel counts a
  wait as one for I/O while the task has requests in flight, which a ring
  always has, so every park of an idle or latency-bound server was counted
  as waiting for a disk.
- A full submission queue is submitted early instead of failing the
  operation. An operation with a linked timeout needs two entries that reach
  the kernel together, so queueing makes room for both.
- Reads and writes reach the kernel at most 32 to an entry. The kernel plugs
  the block layer for each entry that submits them, sets the requests of the
  plug's first 32 up in one batch and those of the rest one by one, and
  flushes the plug as the 33rd comes (`BLK_MAX_REQUEST_COUNT`). So the driver
  counts the queued entries whose opcodes plug, and when more than 32 are
  queued the ring submits them in batches of 32, each but the last in an
  entry of its own that waits for nothing but runs its completions' deferred
  work, and cuts a batch only where a chain ends (see
  [Random reads with direct I/O](#random-reads-with-direct-io)).
- The completion queue is drained fully on every pass, its tail read once
  for the batch, and the submission queue's head is read once per entry
  into the kernel, which consumes entries only while it is entered: no
  counter the kernel shares is read per operation. Overflow is handled
  (`NODROP` backlog, `IORING_SQ_CQ_OVERFLOW`) rather than assumed away;
  multishot operations make it realistic.

### Operation table

- `user_data` encodes a slot index and a generation, so completions for a
  reused slot are recognised as stale.
- Slots are preallocated and reused, so the common path allocates nothing per
  operation. The payload (buffer, address, ...) lives in the future, not the
  slot.
- Slot states: free, waiting (with a `Waker`), completed (result and flags,
  until the future collects them), orphaned (the payload of a dropped or
  detached operation), streaming (the queued completions of a multishot
  request) and orphaned stream (the payload of a dropped stream, handed each
  completion until the last). An orphaned operation lies in the slot itself
  when it fits (160 bytes, room for a write behind's send with its bundle
  of leased buffers), and in a box otherwise, so detaching an operation, as
  every write behind does, allocates nothing: a detached operation goes
  straight into an orphaned slot as it is queued. The slot is larger for it,
  184 bytes rather than 72, which the table pays once per slot, at the
  maximum in flight, and which costs nothing measurable: with slots of
  twice the size, the echo benchmark at 1,024 and 10,000 connections read
  within 1% of what it read with these (see
  [The runtime itself](#the-runtime-itself)).
- Operations are lazy: nothing is queued before the first poll, which leaves
  room for combinators to shape the SQE at no cost. `Op::timeout(d)` queues
  the operation with `IOSQE_IO_LINK` and an `IORING_OP_LINK_TIMEOUT` behind
  it, which posts no completion when disarmed (`IOSQE_CQE_SKIP_SUCCESS`),
  and any it posts is ignored. An operation it cancels completes with
  `ECANCELED`, which the `Op` reports as `ETIMEDOUT`, since nothing else
  cancels an operation whose `Op` exists; and it hands back its buffer, which
  a timer in user space cannot, as it can only drop the `Op`. The driver
  builds the chain itself: an `Operation`'s entry may not link. What
  `.fixed()` would have chosen is the operation's `Fd`.
- `Op::link(next)` links operations into a chain (`IOSQE_IO_LINK`), which
  the kernel runs in order, each once the one before has succeeded: a
  `Link`, a future of both outputs, which nests through a sealed `Chain`
  trait into chains of any length. It is queued as a whole at its first
  poll, and each `Op` is then left submitted, as if it had queued itself,
  so polling, cancelling on drop and timeouts are the `Op`'s own. The
  kernel links only entries submitted together, so the chain gets room
  for all of its entries first, or every operation fails with the error;
  each entry is prepared into a copy before any is queued, so a panic in
  a later `prepare` leaves nothing queued that points at memory the panic
  frees. A failure, including a read or a write the kernel counts as
  short, cancels the rest with `ECANCELED`. A member's linked timeout
  sits between it and the next, and carries the link on. Only the first
  member reports its timeout as `ETIMEDOUT`: a later one cannot tell its
  own timeout from a failure before it.
- `Op::detach` hands an operation to the driver to finish with nobody
  waiting: it becomes an orphan, as a dropped `Op`'s does, but is not
  cancelled, and a timed one still completes with `ETIMEDOUT`. An operation
  that should say how it went records that in its `complete`.
  `Driver::detach_chain` does the same for several, queued linked as a
  `Link` is, so the kernel runs them in order and a failure cancels the
  rest. `Op::into_inner` gives back an operation that has not been
  submitted, which `Completion::from_error` can then fail, handing back its
  buffers.

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
  `Send`, `SendTo` (`addr2`, 6.1), `SendVectored` (a send of an `IoBufs`,
  such as a bundle, naming the vector in the entry, `IORING_SEND_VECTORIZED`,
  6.17, probed, or through `sendmsg`), `SendZc`, `Recv`, `RecvFrom` (through
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
  ends the receive with `ENOBUFS` it waits for a lease to return a buffer,
  and when the kernel ends it for its own reasons it re-arms at once. The
  data waits in the socket meanwhile, so none is lost. Each buffer that
  returns to a pool wakes one waiting stream, the first to have asked:
  woken all at once, every one would ask the kernel again for the one
  buffer, and all but one would starve again. The kernel maps a receive's
  buffers before it knows how much is waiting, as many as the entry's `len`
  asks for: one buffer unless asked for more, so a message of several
  buffers' worth that has arrived whole takes two calls into the socket, one
  for the first buffer and one for the rest. Asking for the whole message
  instead gains nothing measurable: the second call costs little next to
  the copies. But a receive that fills its whole ask is capped
  (`IORING_RECV_MSHOT_CAP`): the kernel posts it as it is and queues the
  request again for the rest, which comes in a turn of its own. So a
  receive stream learns how many buffers its messages take, and while its
  thread waits on one peer at a time, which the ring's turns delivering one
  completion each tell, asks for a buffer less: the kernel hands over all
  but the last buffer of a message as soon as it has them, and the task
  answering the message echoes that part while the kernel still copies the
  rest, as tokio's echo, which reads and writes a buffer at a time, does by
  nature. Under load the split would cost each message a completion and a
  send, 10% at 16 connections, so the ask is then the kernel's own. A
  stream changes its ask by restarting its request: a cancellation,
  whose own completion ends the request, after which it is submitted
  again; the completions posted before are yielded as usual, and what the
  receive had not taken waits in the socket. The cancellation cannot miss
  a receive that is armed: the kernel keeps an armed multishot request in
  its cancellation table until it completes, the ring's thread is the
  only one that issues and runs its task work, and a cancellation that
  finds nothing (`ENOENT`) means the request has ended, whose last
  completion re-arms the stream. A capped receive is issued only once the
  socket polls readable (`IORING_RECVSEND_POLL_FIRST`), as every receive
  is after its first completion. The flag is a precaution: a stream was
  once seen to stop completing after a capped completion of a receive
  issued at submission, with data in the socket, and the cause was not
  found; the kernel's paths from either issue converge, and the stream's
  tests have run both without a stall (see
  [Open questions](#open-questions)).
  `RecvStream::buffers` sets how many buffers the messages take from the
  start, for a stream known to carry large ones.
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
  second ring holding the data to send.
- Incremental consumption (`IOU_PBUF_RING_INC`, 6.12, probed):
  `RecvStream::incremental` and `ReadStream::incremental` read into a pool
  of their own whose buffers the kernel lends to as many completions as
  fit, each taking only what its data fills, so large buffers hold many
  small receives. Leases are parts of buffers, whose room is only their
  data, as the kernel may fill on past it; a buffer returns once the
  kernel is done with it, which `IORING_CQE_F_BUF_MORE` says rather than
  the length, and its last lease is gone. A completion does not say where
  in its buffer its data starts, which the pool works out from the
  completions before it, so it must see all of them in order: the pool
  is one stream's alone, and never handed out.
- Zero-copy send, with the notification CQE keeping the buffer alive, on
  request (`send_zc`). Over loopback the kernel copies the data when it
  reaches the peer's socket, so only a network card can show where
  zero-copy starts to pay; the size threshold for choosing it automatically
  waits for a benchmark across two machines.
- Registered buffers for file I/O (`READ_FIXED`, `WRITE_FIXED`).
- Polled I/O (`IORING_SETUP_IOPOLL`), on a second ring: a read or a write
  of a file opened for direct I/O on a block device with poll queues can
  ask for it (`Operation::polled`, `Read::polled` and the three others),
  and the kernel then polls the device's completion queue for it rather
  than wait for an interrupt, as fio's `hipri` does; what that gains is
  measured under [Random reads with direct I/O](#random-reads-with-direct-io).
  The kernel's constraints, from Linux 7.0's `io_uring/io_uring.c`,
  `io_uring/rw.c`, `io_uring/opdef.c` and `block/blk-mq.c`, shape the
  design:
  - A ring set up with `IOPOLL` takes only opcodes marked `.iopoll` in
    `opdef.c`: reads and writes, plain, fixed and vectored, passthrough
    commands, no-ops, `MSG_RING`, and the buffer and file table updates;
    anything else, a socket operation, a timeout, `POLL_ADD`,
    `ASYNC_CANCEL`, a `LINK_TIMEOUT`, `CLOSE`, fails at submission with
    `EINVAL`. So it cannot be the
    runtime's one ring: the driver keeps its main ring for everything
    else, the wake timer and the unparkers' poll included, and polled
    operations go on the second, created on the first such operation or
    by the builder. Both rings share the operation table, so completions
    route by `user_data` as before, `Driver::in_flight` counts both, and
    `Stats` counts the polled operations and both rings' entries. Each
    ring counts the entries that plug the block layer for itself, so the
    polled ring submits reads in batches of 32 as the main ring does.
  - Its requests complete only as the ring is entered with `GETEVENTS`,
    which polls (`io_iopoll_check`): `min_complete` 0 makes one pass over
    the requests in flight (`io_do_iopoll`, `BLK_POLL_ONESHOT`), and
    `min_complete` n polls until n have completed, `need_resched()` says
    the CPU is wanted or a signal is pending, which can leave it short; an
    entry with completions still unread in the queue polls nothing at all,
    so the queue is drained before every entry. A timeout in the extended
    argument is validated but not applied (`io_validate_ext_arg`): there is
    no timed wait on such a ring. So every turn with polled operations in
    flight enters the polled ring for a pass, and reaps its completions
    through the same path as the main ring's.
  - A park with polled operations in flight must not block on the main
    ring, which would leave the polled completions unfound, and must not
    only poll, which would leave sockets, timers and unparks unseen. When
    nothing but a polled operation can complete, every operation in flight
    being polled, the wake timer not armed and the park without a
    timeout, the park enters the polled ring to wait for one completion,
    which polls inside the kernel until it comes: one entry however long
    the device takes, and the device's latency is the most an unpark can
    be held back by, as with any polling reader. Otherwise the park polls
    a pass at a time, an entry each, and between passes looks at the main
    ring without entering it: at `IORING_SQ_TASKRUN`, which the kernel sets
    when a completion of the main ring is ready to run, as a timer's
    firing or an unparker's write to the eventfd leaves one; at its
    completion queue; and at the unparkers' flag; then enters the main
    ring once, without waiting, to run that work. The second policy costs
    a system call per pass where the first costs one per completion, which
    a device that completes faster than the pass takes, as the null block
    device does, never shows.
  - A polled read or write needs `O_DIRECT` and a file whose operations
    can poll (`f_op->iopoll`), or it fails with `EOPNOTSUPP`
    (`io_rw_init_file`); and since 6.11 a polled request to a device whose
    queue has no poll queues (`/sys/block/DEV/queue/io_poll` 0) fails with
    `EOPNOTSUPP` too (`blk_mq_submit_bio`, `BLK_STS_NOTSUPP`), where
    earlier kernels fell back to an interrupt. The null block device puts a
    polled request on its poll queue's list and completes it only in
    `null_poll`, so nothing but polling finishes it.
  - The polled ring has a file table of its own, as large as the main
    ring's, since a direct descriptor means something only to its own
    ring's table: `Driver::install_polled` puts a plain descriptor's file
    into a slot of it, with an `IORING_OP_FILES_UPDATE` on the polled ring
    that allocates the slot (`IORING_FILE_INDEX_ALLOC`), and polled
    operations name the slot as a `PolledFd`, beside the plain descriptor
    that the main ring's operations keep using. Dropping the `PolledFd`
    queues the update that empties the slot on the same ring, so it
    reaches the kernel after the operations queued before it, as a
    `DirectFd`'s close does on the main ring. The lookup a plain descriptor
    costs the kernel on every operation is worth having the table for: with
    plain descriptors instead, the non-polled reader of the random-read
    benchmark lost 2% at depth 1 and 4% to 5% at depth 32 and 128 (medians
    of three runs of 2 s). The ring shares the main ring's registered
    buffers: those registered before it exists are cloned into it as it is
    created and those registered later as they are registered
    (`IORING_REGISTER_CLONE_BUFFERS`, 6.12, which gives the second ring
    references to the first's buffers), and unregistering on the main
    ring unregisters on both. A polled operation can have no linked
    timeout and join no chain, which the driver refuses with `EOPNOTSUPP`
    before the kernel sees them; dropping its `Op` orphans it without a
    cancellation, the kernel taking none on that ring and the device
    finishing the request regardless, and the driver's drop waits for it
    as for any orphan. A kernel without `IOPOLL`, or without cloning while
    buffers are registered, makes creating the polled ring fail with
    `Unsupported`.
- `MSG_RING` to move direct descriptors from one ring's file table to
  another's (`DirectFd::send_to`). The other ring posts no completion
  (`IORING_MSG_RING_CQE_SKIP`): the sender's says which slot the file went
  to, and comes only once it is there.
- Waking other rings with `MSG_RING` was measured, and not adopted. In a
  ping-pong between two drivers parked on separate physical cores, a wake
  sent as an `IORING_MSG_DATA` message, queued with
  `IOSQE_CQE_SKIP_SUCCESS` and so submitted with the sender's own next
  `io_uring_enter`, took 5.1 to 5.3 µs a round trip against 4.7 to 4.8 µs
  for the eventfd, though it saves the eventfd write's system call; only
  between hyperthread siblings was it ahead, at 4.8 against 5.2 µs
  (Linux 7.0, Xeon Platinum 8488C). The kernel's posting into another
  ring, one with `DEFER_TASKRUN`, costs more than the write, a busy sender
  would hold the wake back until its next enter, and threads without a
  ring need the eventfd in any case.

### Capability probe

`ruio_driver::Probe` uses throwaway rings to learn the kernel's feature bits,
which setup flags it accepts and which opcodes it supports
(`IORING_REGISTER_PROBE`), and whether it consumes provided buffers
incrementally, by registering such a buffer ring in memory the kernel
allocates. `Probe::check_baseline` names every missing
capability when the kernel is below the 6.10 baseline. Features past the
baseline are used only when probed.

## Executor

- One executor per thread, driving that thread's ring. `ruio::spawn` takes
  futures that need not be `Send`; `spawn_on(core, future)` sends a `Send`
  future to another core, and `Handle::spawn_with` a closure that makes
  the future there, which then need not be `Send`: it can hold a socket of
  that core's ring across its awaits.
- A task is one allocation holding its future and, once it finishes, its
  output, and is its own waker. It is referred to in two ways. References
  from its own thread, which the scheduler's table, the run queue, the
  `JoinHandle` and the driver's registrations hold, are counted in a plain
  cell, so taking and dropping one costs no atomic operation. References
  that may be anywhere, the wakers and the remote queue's entries, are
  counted atomically, and one of them, the share, stands for every local
  reference while there is any. Wakers are `Send + Sync`, as the `Waker`
  contract requires, while the future need not be, so the future, output
  and bookkeeping are only touched on the task's thread: by the scheduler
  and by the `JoinHandle`, which is not `Send`. The scheduler holds every
  unfinished task until its future is dropped on that thread, and the
  `JoinHandle` drops the output there, so the task is freed, wherever the
  last atomic reference goes, owning nothing that is not `Send`. A task
  that awaits another's `JoinHandle` is held by a local reference too,
  when it is a task of the runtime on the thread, and the share goes with
  a plain load when it is the last reference, as it nearly always is once
  a task has finished. A task holds the shared part of its scheduler,
  which its wakes from other threads reach, by a count of the runtime's
  thread, as its local references are counted, and by an atomic reference
  of its own only from when it may be freed on another thread: once its
  last local reference goes while a waker of it is held elsewhere. So a
  spawn and its join take no locked instruction on the task's thread.
- Every task's wakers share one vtable, whatever the task's future: the
  header of a task comes first in it, so a pointer to the task is a pointer
  to its header, through which the vtable reaches the rest. So the driver
  can tell a task's waker by its vtable, and holds it in its table by a
  local reference, through the retain, release, wake and wake-release
  hooks of a `LocalWaker` the executor registers, where it clones any
  other waker. The retain counts a reference only for a task of the
  runtime running on the thread: a waker sent to another thread, which the
  vtable does not tell apart, can poll an operation there, whose driver
  may not touch the task's local count. A completed operation hands the
  driver's reference to the run queue, through the wake-release hook, and
  a completed stream keeps its local waker, so the stream's next poll
  registers nothing. An operation's wake and run so cost no locked
  instruction on the task's thread: a run lends its poll a waker that
  borrows the run's reference, a wake on the task's thread queues it with a
  plain flag and a local reference, and a wake from another thread sets an
  atomic flag and queues an atomic reference on the remote queue, which a
  run clears with a swap, only if it is set, so that the poll sees what
  every waker that found it set woke it for. Moving remote wakes to the
  run queue checks the plain flag, so a task is never queued twice.
- The future given to `block_on` has a header shaped as a task's, whose
  wakers share the tasks' vtable: the driver holds them by a local
  reference, and a wake on the runtime's thread marks the future to poll
  with the plain flag that marks a task queued, queueing nothing. The loop
  polls the future when it finds the flag set, and the scheduler does not
  park while it is. A wake from another thread goes through the remote
  queue, as a task's does, and the scheduler sets the flag as it takes
  it. The header frees itself after its last reference, which a waker kept
  past `block_on` may hold.
- Each loop pass runs up to 64 ready tasks, fires due timers, then turns the
  ring: it submits without waiting if anything is runnable, and otherwise
  parks until a completion, a wake or the next timer. A task woken by the
  running task takes a LIFO slot and runs next, at most three in a row;
  everything else queues in FIFO order.
- A runtime may spin before it parks (`Builder::spin`, off unless set):
  with nothing runnable it submits what the pass queued, then polls, for
  up to the budget given, the ring's flags (`IORING_SQ_TASKRUN`, overflow),
  the completion queue's tail, the unpark state and its own queue of remote
  wakes, and parks only if nothing has come by then. It spins only if its
  last wait, spinning and parking together, ended within the budget, so an
  idle thread parks at once and a thread that waits on one peer at a time,
  whose turns come within microseconds, never sleeps. A spinning thread
  occupies its core, so this is for cores given to the runtime. The echo
  server with one connection, whose round trip is latency-bound, makes
  twice the round trips with a 50 µs budget, with a 99th percentile half
  as long and a dozen context switches in a run instead of one per echo;
  with 16 connections, where the server is busy, nothing changes.
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
  higher level and moves down as time comes to it. The wheel holds a task
  of the thread's runtime by a local reference, as a task holds the one
  awaiting its handle, so arming and firing a timer take no atomic
  operation; and a sleep polled inside the runtime takes the clock as the
  pass read it, which the scheduler reads at most once a pass, so one that
  arms many timers reads it once. A deadline that passed during the pass
  is armed, and fires at the next tick, rather than ending at once; none
  ends early.
  An `Interval` (`time::interval`) ticks at a period: it keeps one
  `Sleep`, which it resets to the next tick's deadline as each ends, so a
  tick costs the wheel one removal and one arm, reads the clock once and
  allocates nothing. A tick the task is late for by more than a period is
  made up at once, waited for a period from now, or skipped to the
  cadence, as its `MissedTickBehavior` says.
- A sleep is `Send` and `Sync`, so that a future given to `spawn_on`, or a
  sleep hyper asks of a timer, can hold one. Its timer is in the wheel of
  the runtime that polls it, which it names by the address of the runtime's
  shared part and the runtime's id rather than holding it, so that arming
  takes no atomic operation. On that runtime's thread, inside it, it
  reaches the wheel through the thread's current runtime, with no lock, the
  id telling the runtime from a later one at the same address. Polled by
  another runtime, it arms its timer there; dropped, or moved away, where
  its wheel cannot be reached, it finds the runtime by its id in a registry
  of the runtimes alive, queues the timer's key beside the wakes from other
  threads, and the runtime's next turn removes it; a runtime that has gone
  has no wheel left to remove it from.
- A task's memory comes from its scheduler's cache of the tasks freed on
  its thread, kept by layout, the first four layouts and up to 64 blocks
  of each, and goes back there; a task's layout is its future's, so a
  program has few, and a spawn and its free take no call into the
  allocator. A task freed on another thread goes to the allocator.
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
  task wherever it runs, with no lock on the path of a value. A send still
  takes two locked instructions, to claim its slot and to see whether the
  receiver waits, which no cross-thread channel can do without: a store
  and a load that another thread may reorder need a barrier between them,
  which costs what a locked instruction does. So `sync::local` has the
  same channels for the tasks of one thread, built on `Rc` and cells,
  whose halves are not `Send` and whose values need not be: a value moves
  through a plain cell, the receiver's waker stays registered and is woken
  by reference, and a round trip takes no atomic operation. An `mpsc`
  keeps its values in a ring of slots, each stamped with the turn round
  the ring at which it is free to fill, or was filled, as in Vyukov's
  bounded queue: a sender claims a slot with one compare-and-exchange on
  the tail, fills it and stamps it, and the one receiver takes the value
  and stamps the slot free. Only senders that find the channel full wait,
  in a locked list. The receiver's waker stays registered across waits
  and is woken by reference, so a task that waits again clones nothing. A
  `oneshot` is one atomic state, in which the sender finishes, by sending
  or going, and the receiver registers its waker and goes, each with one
  read-modify-write.
  An unbounded `mpsc` (`mpsc::unbounded_channel`), whose send never
  waits, keeps its values in a list of blocks of 32 slots: a sender claims
  a position with one atomic add, so that senders sending at once each
  get a slot of their own and nobody retries, finds the block from the
  channel's tail block, writes the value and publishes it by setting the
  slot's bit in the block's ready word, which the receiver reads with one
  load. A block the receiver has emptied goes back to the end of the list
  for the senders to fill again, once no sender can reach it: the sender
  that moves the tail block on past a full block leaves in it the tail as
  it was then, and the receiver puts the block back once it has taken
  every value before that position. So a channel in a steady state
  allocates nothing, and the receiver closing the channel sets a flag in
  the tail, so that every claim falls before the close, and is taken, or
  after it, and hands its value back. `sync::local` has the unbounded
  channel for one thread too, on a deque.
- `sync` also has a `Semaphore`, and on it a `Mutex` and an `RwLock` whose
  guards may be held across awaits, and `Notify`. They keep their waiters
  behind a standard mutex, wake outside it, and work across cores, as the
  channels do. The semaphore's free permits are an atomic count beside
  its queue, with a flag that is set while waiters are queued and changes
  only with the queue locked: an acquire that finds enough permits and
  nobody waiting takes them with one compare-and-exchange, and a release
  with nobody waiting gives them back with one, so an uncontended `Mutex`
  locks and unlocks without the lock. The semaphore serves its waiters in
  the order they came, each with all the permits it asked for at once, so
  one that needs many is not passed by later ones that need fewer, and a
  writer, which takes every permit a reader takes one of, cannot be
  starved by readers. A waiter dropped after its permits were set aside
  gives them back, and a `Notified` dropped after `notify_one` chose it
  passes the notification on, so cancelling a wait loses nothing.
- `sync::broadcast` carries every value to every receiver, keeping the
  last `capacity` of them, rounded up to a power of two, in a ring of
  slots. Each receiver keeps its own position and reads the slot at it,
  cloning the value under the slot's read lock, or taking it as the last
  of the receivers counted to read it, so a value goes as soon as every
  receiver has it; one that has fallen more than `capacity` behind finds
  the slot filled again since, is told how many values it missed, and
  goes on from the oldest kept. Sends take their turns under the tail's
  lock, which holds the receiver count and the wakers of the receivers
  waiting, so that each slot is stamped with the receivers that will read
  it; a receive that finds its value takes no lock but the slot's.
- `sync::watch` holds one value, behind a standard reader-writer lock,
  with a version beside it whose low bit says every sender is gone. A
  send changes the value under the write lock, bumps the version, and
  wakes the receivers waiting for a change only if the flag beside their
  list says any do; a receiver's `changed` compares the version with the
  one it last saw, one load when a change waits, and `borrow` takes the
  read lock, so a `Ref` must not be held across an await.
- A `Barrier` lets `n` tasks through together. The count arrived and the
  waiters' wakers, by ticket, are behind a lock, and the generation in an
  atomic beside it, which the last to arrive moves on before it wakes the
  rest outside the lock, so a waiter let through sees it with one load and
  no lock. A wait dropped before its generation is let through takes its
  arrival back, so a cancelled wait does not let the others through one
  short.
- A `CancellationToken` is one word of state that its clones share:
  `is_cancelled` is one load, and `cancel` one read-modify-write unless
  something has waited on the token or it has children, which bits of the
  state say, when it also wakes the waiters and cancels the children, down
  the tree with a list of what is left rather than by recursion. The
  waiters' wakers are in a slab behind a lock, each in a slot whose index
  is its ticket, so a wait dropped takes exactly its own waker out, and a
  task that waits on the token and gives up with every turn of a loop, as
  one selecting between the token and its work does, allocates nothing
  after the first. Children are held weakly, and pruned as one is added.

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
  message rather than twice. While the thread waits on one peer at a time,
  as a single connection's does, which the ring's last turn delivering one
  completion tells (`Driver::completions_last_turn`), a write of more than
  32 KiB goes as a chain of up to four linked sends, the first of 32 KiB
  each and the last of the rest, so that the peer receives the first while
  the kernel still copies the rest, as it would receive the writes of a
  stream that sends what it has as it goes; a send that fails cancels those
  after it. Under load a write goes as one send whatever its size: each send
  costs the kernel a transmit and the peer a wake-up, which pay only while
  the thread would otherwise wait for the peer (see [TCP echo](#tcp-echo)).
  The stream's writes stay in order, one at a time: a write behind, every
  other write and `shutdown` first wait for the write behind in flight, all
  of its sends. Nobody waits for a write behind, so its failure
  comes out later: it fails every write after it, handing buffers back
  unsent, as the stream now has a gap, and `flush` waits for it and reports
  it; `try_flush` reports what is known without waiting, and without
  arranging to be woken, which is what an adapter's flush wants. The
  kernel reports a `MSG_WAITALL` send it stopped short as the bytes
  sent, so one cut short counts as timed out if it had a timeout and as a
  broken pipe otherwise. The failure is kept as an errno, so the writes it
  fails report it as the kernel would have.
- Every operation borrows its socket, so the descriptor stays open until the
  kernel has the operation: dropping the socket first does not compile.
  A socket keeps the ring its first operation ran on, the thread's for a
  plain descriptor and its own for a direct one, so the operations after
  it are spared the search for the thread's runtime; it is taken then
  rather than as the socket is made, which `from_std` may do outside one.
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
`create_dir`, `create_dir_all`, `remove_file`, `remove_dir`, `rename` and
`read_dir`, running on the ring of the thread's runtime.

- Reads and writes take owned buffers and hand them back with the result, at
  an offset, so any number can run at once. `read_exact_at` and
  `write_all_at` take as many as a buffer needs; `read_fixed_at` and
  `write_fixed_at` read and write registered buffers. `read` and `write` use
  the file's position, which nothing orders when they overlap.
  `read_stream` reads what arrives on a pipe, a FIFO or a character device
  into a pool, with one multishot read that stays armed. `FileOp::link`
  links file operations into a `FileChain`, which borrows every file as
  the operations do: a write and the flush behind it reach the kernel in
  one submission, and the flush runs only if the write succeeded. Socket
  operations are not linked in `ruio::net`, since a write waits for the
  writes behind its stream before it is submitted, which a chain would
  have to wait for as a whole; `ruio::uring` links the driver's own.
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
- `OpenOptions::polled` opens a file for polled I/O: its reads and writes,
  plain, exact and fixed, go on the driver's polled ring (see
  [Kernel features](#kernel-features)), which the open creates if the
  thread has none yet, so a kernel without `IOPOLL` fails the open with
  `Unsupported`. The file must be opened `O_DIRECT`, and is a plain
  descriptor, which the open also installs into the polled ring's own file
  table (`Driver::install_polled`), so that its reads and writes name a
  slot there and skip the kernel's lookup of the file, as a direct
  descriptor's do; asking for a direct descriptor of the main ring as well
  fails with `InvalidInput`, as does leaving out `O_DIRECT`. Its reads and
  writes take no timeout and join no chain, both `EOPNOTSUPP`; everything
  else on the file, syncing, its status and closing, runs on the main ring
  through the plain descriptor as for any file.
- The kernel always runs statx, fsync, fallocate, ftruncate and the
  operations on names in an io-wq worker, and an open whose lookup is not
  cached; so buffered reads and writes that miss the page cache. A direct
  read goes to the device from the ring.
- io_uring has no operation that reads a directory, so `read_dir` opens
  one through the ring and `ReadDir` reads its entries with
  `getdents64(2)` on the thread, 32 KiB at a time, as resolving a host
  name runs on it: it blocks the thread while the kernel reads the
  directory, which is quick once it is cached. Before each batch after
  the first, the reader yields once, so that a large directory does not
  hold up the other tasks. Entries share the directory's descriptor, and
  their status comes from statx relative to it, so it is the status of
  the entry in the directory read, wherever the directory has moved; the
  type comes from the directory, or from statx where the file system
  gives none.
- Still to come: `copy` with `splice`, links and permissions.

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
- A write copies up to 256 KiB of the caller's bytes and sends them with an
  operation of the adapter's own: done, as far as the caller can tell, once
  the send is on its way. The send is submitted with a waker that wakes
  nothing, so its completion costs the task no wake-up, and its result waits
  in its slot of the driver's table until the next write, flush or close
  takes it; a write that finds the send before it still in flight leaves the
  task's own waker with it. The next write waits for it, and a failure fails
  the writes, flushes and closes after it, as for the stream's own writes
  behind. The buffer comes back with the result, for the next write to fill,
  unless it has grown past 16 KiB, so a connection's writes allocate nothing
  once it is going, and an idle one holds little. A dropped adapter leaves
  its send to finish, and the stream closes after it.
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
client checks every echo. The servers are single-threaded: tokio's
current-thread runtime reads into a 16 KiB buffer of each connection's and
writes back what it read; ruio receives into a pool of as many 16 KiB
buffers, up to 4096 (see below), gathers whatever has arrived and writes it
back behind, with one vectored send, or, with one connection, in segments.
Both set `TCP_NODELAY` on the listener. Round trips per second, medians of
three runs taken in turns, on a Xeon Platinum 8488C with Linux 7.0, over
loopback:

| Connections | Message | tokio | ruio | p99 tokio (µs) | p99 ruio (µs) |
|---:|---:|---:|---:|---:|---:|
| 1 | 64 B | 47k | 48k (1.02×) | 28 | 28 |
| 1 | 1 KiB | 45k | 45k (1.01×) | 28 | 28 |
| 1 | 16 KiB | 30k | 31k (1.01×) | 42 | 41 |
| 1 | 64 KiB | 17k | 20k (1.17×) | 76 | 57 |
| 16 | 64 B | 210k | 207k (0.98×) | 95 | 100 |
| 16 | 1 KiB | 200k | 195k (0.98×) | 102 | 106 |
| 16 | 16 KiB | 116k | 121k (1.04×) | 187 | 178 |
| 16 | 64 KiB | 42k | 46k (1.10×) | 559 | 468 |
| 256 | 64 B | 204k | 207k (1.02×) | 1320 | 1336 |
| 256 | 1 KiB | 194k | 199k (1.03×) | 1393 | 1396 |
| 256 | 16 KiB | 154k | 163k (1.06×) | 1818 | 1748 |
| 256 | 64 KiB | 49k | 49k (1.02×) | 5570 | 6192 |
| 1,024 | 64 B | 221k | 218k (0.99×) | 4792 | 5116 |
| 1,024 | 1 KiB | 222k | 231k (1.04×) | 4823 | 4668 |
| 1,024 | 16 KiB | 106k | 119k (1.13×) | 11932 | 12894 |
| 1,024 | 64 KiB | 36k | 40k (1.10×) | 29638 | 27406 |
| 10,000 | 64 B | 188k | 193k (1.03×) | 67675 | 59880 |
| 10,000 | 1 KiB | 177k | 178k (1.01×) | 69855 | 71134 |
| 10,000 | 16 KiB | 91k | 88k (0.97×) | 112809 | 117267 |
| 10,000 | 64 KiB | 32k | 35k (1.07×) | 314124 | 293676 |

ruio is ahead of tokio from 256 connections on, by up to 13%, bar 1,024
connections with 64 B messages (0.99×) and 10,000 with 16 KiB (0.97×), and
level at 1 and 16 connections, bar 16 connections with messages of a
kilobyte or less (0.98×) and the rows with 64 KiB messages, where it is
ahead by 10% to 17%. Its 99th percentile is within 7% of tokio's or better,
bar 1,024 connections with 64 B and 16 KiB messages and 256 with 64 KiB. The
rows up to 256 connections are from after the fifth performance pass,
medians of five runs; those from 1,024 on are from before the second, and
the passes since have not been on their path, bar the receive that the
64 KiB rows gained from, which the load there keeps the kernel's own.
Direct descriptors change nothing measurable. Runs
of the same load differ by up to a few percent on this host, which is
shared, and more while it is busy: in the run of the table the 10,000
connection rows came in such a spell, 64 B messages measuring 0.77× with the
server with direct descriptors, run in the same turns, at 0.92×, and 16 KiB
1.11×, so those four rows are a second run of their own, taken afterwards,
in which the server with direct descriptors measured 1.15× with 64 B
messages. A spinning server (`Builder::spin`, see [Executor](#executor))
changes the rows with one connection alone, where it is bound by latency:
with a budget of 50 µs, 96k round trips a second with 64 B messages
(2.05×, 99th percentile 13 µs against 29), 90k with 1 KiB (2.04×, 15 against
30), 45k with 16 KiB (1.44×, 29 against 41) and 21k with 64 KiB (1.22×, 52
against 75); with 16 connections nothing changes, the server being busy.

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
- **A pool no larger than the load.** A pool's ring hands out buffers in the
  order they came back, so every buffer of the pool is in use in turn, and a
  pool larger than its load spreads the copies over more memory than the
  cache holds. With echoes written behind, a buffer goes back once its data
  is sent, and data a pool has no room for waits in the socket, so a buffer
  per connection is more than enough: with 1024 connections and 64 KiB
  messages, 8192 buffers where 1024 do cost a fifth of the throughput, and
  with 10,000 connections and 64 KiB messages, 4096 buffers (64 MiB, which
  the last-level cache holds) beat a buffer per connection by a tenth, from
  0.96× to 1.01×, where a send's copy reads what a receive wrote up to forty
  receives earlier. Fewer starve: with 2048, the sends to 10,000 clients hold
  more buffers than that while the clients' acknowledgements are on their
  way, and the throughput fell by a third with some echoes taking seconds.
- **Segmented writes while idle.** tokio's echo is cut-through: it writes
  each 16 KiB as soon as it has read it, so the client receives while the
  server still reads. The kernel receives a whole 64 KiB message for ruio in
  one pass, and ruio sent it back in one send, which the kernel copies whole
  before it transmits any of it; with one connection, which is bound by
  latency, that cost a fifth (0.78×), and given 64 KiB buffers, so that it
  too echoed whole messages, tokio did the same as ruio. A write behind of
  more than 32 KiB now goes as a chain of linked sends while the thread is
  waiting on one peer at a time, the first of 32 KiB each, so the client
  receives the first while the server copies the rest: 0.94×. Only while the
  ring's last turn delivered a single completion: a send costs a transmit and
  the client a wake-up, and with 16 connections, where the server is busy
  throughout, four 16 KiB sends per echo cost 40% (48k round trips a second
  fell to 28k). 32 KiB segments do as well as 16 KiB ones with one
  connection and cost a sixth less CPU, and 8 KiB ones lost all of the gain:
  the kernel issues each link of a chain from its deferred work, so a chain
  of four takes two or three entries into the kernel.

What is left:

- With 10,000 connections and large messages, tokio's 16 KiB stays in the
  first-level cache between read and write, where ruio's 64 KiB, received
  up to forty receives earlier, comes back from the last-level cache, and
  its sends' copies cost several times as much. Each entry into the kernel
  runs up to 20 items of deferred work, such as receives, and a turn of the
  ring two entries' worth, before the tasks run and the sends go.
- Sends reach the kernel at the end of a loop pass, after up to 64 tasks have
  run, which costs a few microseconds of latency with few connections.

Four experiments did not pay. Submitting after every task brings the 99th
percentile at 16 connections to tokio's, but costs 4% to 6% of the
throughput. Submitting a send within the poll of its write costs 8% to 12%
with small messages and 38% with 10,000 connections: every extra entry into
the kernel runs everyone's pending work and breaks up the batching; writing
behind removed the second poll instead. A shallow ring, holding only as many
buffers as the load needs and refilled with the ones returned last, turns
starvation into the common case: 10,000 connections fell to 0.12× while the
pool woke every waiting stream for each buffer that returned, and each of
them asked the kernel again for the one buffer. The pool now wakes one, the
first to have asked, which makes starvation cheaper but not quick: the
woken stream's receive is re-armed at the end of the pass, by when a receive
that was armed, with data waiting, has taken the buffer, and with demand
beyond the pool some streams wait seconds. Huge pages for the pool
(`MADV_HUGEPAGE`, which the kernel granted) changed nothing measurable.

The second performance pass read the kernel's io_uring for Linux 7.0 and
tried four more. With `DEFER_TASKRUN` an entry into the kernel that asks for
events runs up to 20 items of deferred work, such as receives, in the submit
path and up to 20 more in the wait, which is why submitting after every task
cost what it did: a submission without `IORING_ENTER_GETEVENTS` runs none,
and sends over loopback complete inline either way, so one every 4, 8 or 16
queued entries, or every 3 µs of a pass, is a pure flush. It left the 99th
percentile at 16 connections where it was and cost 0.5% to 4% of the round
trips; ending the pass early when the ring's flags showed work pending
brought that percentile to tokio's but halved the throughput at 10,000
connections with 64 KiB messages, so sends stay at the end of the pass. A
park woken by one completion runs one item of deferred work and returns, the
rest waiting for the next entry; a park that waits for several
(`min_complete` with a bounded minimum wait, `IORING_FEAT_MIN_TIMEOUT`,
6.12) was meant to deliver a batch per wake instead, but the loaded server
never sleeps: at 16 connections and beyond a park's entry finds work pending
and returns at once, eight echoes an entry at 16 connections and forty at
256, whichever wait was asked for, and nothing moved but a 5% loss with one
connection, whose every turn it batched. `IORING_SETUP_SQ_REWIND` (7.0),
which spares each entry the submission queue's head and tail, changed
neither the microbenchmark's turns nor the echo rows. And asking the kernel
to map four buffers for every receive, so that a 64 KiB message takes one
call into the socket rather than two, gained 3% with one connection and
64 KiB messages and cost 1% to 1.5% with messages that fit one buffer, so it
was an option rather than the default; measured again in the fifth pass it
gained 0.4% to 1.6% and cost 0.6% to 1.3%, within the host's noise. What
did pay there was the opposite: a receive asked for a buffer less than
its messages take, so that the kernel hands over all but the last buffer
as soon as it has them, and the server echoes them while the kernel
copies the rest, as tokio's echo does (see [Networking](#networking)).
With one connection and 64 KiB messages that took ruio from 0.95x of
tokio to 1.12x, 16412 to 20258 round trips a second against tokio's
18156, medians of seven runs in turns with the server on one CPU and the
client on fourteen, the 99th percentile 57 us against 65 before and
tokio's 72, and every run ahead; a fixed ask of three buffers measured
the same, 20682, and of two 17180, so the gain is the one-buffer tail. The
split is gated on the thread waiting on one peer at a time: a fixed ask of
three cost 10% at 16 connections with 64 KiB messages, 42708 against
47391, where the gated ask, which stays the kernel's own under load, left
that row and the rows with small messages within 0.5%.

Across two machines the server no longer does the client's receive
processing, which leaves the runtime a larger share of its time. How
runtimes on several cores fare is under
[Scaling across cores](#scaling-across-cores).

### Random reads with direct I/O

`ruio-bench randread PATH` compares 4 KiB random reads with direct I/O
against the io_uring engine of fio 3.43, each in a process of its own pinned
to the same CPU, three runs each in turns, and reports the medians; with
`--polled` both sides poll the device for completions instead of waiting
for its interrupts (below). The device is the kernel's null block device
(`null_blk`, 16 GiB, 4 KiB blocks, completing reads inline with
`irqmode=0`), which moves no data, so what the comparison measures is what
each spends per read. fio runs at its best:
registered buffers and files, no latency statistics (`gtod_reduce`), and
reads submitted up to 32 at a time; by default it enters the kernel for every
read, and reads about 1.3M a second whatever the depth. ruio's reader runs a
task per outstanding read, each reading a registered buffer at a random
offset of a file opened for direct I/O as a direct descriptor. Reads per
second, on a Xeon Platinum 8488C with Linux 7.0:

| Depth | fio | ruio |
|---:|---:|---:|
| 1 | 1.27M | 1.44M (1.13×) |
| 4 | 1.81M | 2.09M (1.15×) |
| 16 | 2.23M | 2.55M (1.14×) |
| 32 | 2.34M | 2.71M (1.16×) |
| 64 | 2.40M | 2.73M (1.14×) |
| 128 | 2.40M | 2.71M (1.13×) |

ruio is ahead of fio at every depth, by 13% to 16%, and reads as fast at
depth 128 as at 32. When the comparison was first made, at the end of M5,
it was within 5% of fio at every depth: level with it up to 32, and 3% to
5% behind at 64 and above (2.42M to fio's 2.51M at depth 64, 2.28M to
2.41M at 128). It had started at 0.82× at depth 1 and 0.93× at 32, where
its user space took 29% of the CPU; four things were in the way, all of
them the runtime's:

- Waking and running a task took six locked instructions, which serialize
  the pipeline. On the task's own thread four of them went first (see
  [Executor](#executor)), which gained 5% at depth 32; the two that
  remained, cloning the waker an operation keeps and dropping the
  scheduler's reference after a run, went with the local references of the
  second performance pass, and the park's four with it (see
  [The runtime itself](#the-runtime-itself)).
- With a timer pending, as the benchmark's own measuring sleep is, a pass
  read the clock twice, walked the wheel twice, and parked with a timeout the
  kernel copied in on every wait: a tenth of a read at depth 1, where every
  read is a pass. The ring's timer now does it with one kernel timeout.
- Each pass took the lock of the remote wake queue twice, to find it empty.
- Each operation on a direct descriptor searched the thread's drivers for
  its ring.

The second and third performance passes took more of the runtime's own
costs away (see [The runtime itself](#the-runtime-itself)). fio was off the
host for the second; ruio before and after it, run in turns on the same
device, medians of three runs of four seconds: depth 1, 1.17M to 1.38M
reads a second (1.18×), where every read is a turn of the loop and a park;
depth 4, 1.90M to 2.00M (1.06×); depth 16, 2.31M to 2.42M (1.05×); depth
32, 2.27M to 2.37M (1.04×); depth 64, 2.26M to 2.36M (1.04×); depth 128,
2.25M to 2.35M (1.04×). After the third, ruio was 13% ahead of fio at depth
16 and 32 but only 9% at 64 and 128, where it read 2.64M and 2.62M a second
to its 2.72M at 32, while fio's rate stayed flat.

What held depth 64 and above back was how many reads an entry into the
kernel submitted: 32 at depth 32, by the driver's counts, and 40 at 64 and
128. An entry runs at most 40 items of deferred work, such as these reads'
completions, unless it is asked to wait for more: 20 as it submits, and 20
before it would wait (`IO_LOCAL_TW_DEFAULT_MAX`). With every read completing
inline, a turn of the ring so settled at 40 reads in and 40 completions
out, the rest of the depth waiting in the kernel's list of deferred work.
And the kernel plugs the block layer for each entry that submits reads: the
plug sets the requests of its first 32 up in one batch, is flushed as the
33rd comes, and sets every request after that up one by one, each with a
reference on the queue and a tag of its own (`BLK_MAX_REQUEST_COUNT`), 8
reads in each of these entries. fio's engine, as the harness runs it,
submits 32 to an entry. The ring now submits reads and writes in batches of
32 (see [Submission and completion](#submission-and-completion)): a pass of
64 reads takes two entries of 32, and each entry gets back the completions
of what it submitted. Five runs of each in turns, before and after: depth
64, 2.61M to 2.74M reads a second (1.05×); depth 128, 2.62M to 2.73M
(1.04×); depth 1 and 32 unchanged within 1%.

Measured and not adopted: capping each entry at 32 and leaving the rest
queued for the next pass, which leaves reads unsubmitted while the ring
waits, and a scheduler pass of 32 tasks, which changes every workload's
pass, each gained 4%; batches of 20, whose completions all fit in the work
an entry runs as it submits, lost 4% at depth 32 to the entries they cost.

The table is from after the plug batches, with fio 3.43 built from source.
The host was shared with other work, which both sides of the comparison
share; its load was between 2 and 5 as the table was measured. Two runs
while it was higher, up to 19, put ruio 9% to 29% ahead.

Polled I/O (`IORING_SETUP_IOPOLL`; see [Kernel features](#kernel-features))
spares each read its interrupt and the softirq that completes it: the
thread that submitted the read finds it done by asking the device, in the
same entry into the kernel when the device is as quick as this one. For
this comparison the device is loaded with a poll queue beside its one
submit queue and completing through a softirq otherwise
(`null_blk nr_devices=1 queue_mode=2 irqmode=1 gb=16 bs=4096 memory_backed=0
submit_queues=1 poll_queues=1 hw_queue_depth=1024`), so that a polled read
completes only in `null_poll` and an unpolled one only through the softirq.
`ruio-bench randread PATH --polled` runs ruio's reader on a file opened
with `OpenOptions::polled`, a plain descriptor installed into the polled
ring's file table, with the same registered buffers, against fio with
`hipri=1`, its own polled mode, with registered buffers and files as
before. Reads per second on the same host, Linux 7.0, CPU 28, with ruio and
fio in turns, 5 s runs after 1 s of warm-up; medians of three runs, and of
five at depth 64, whose first three had fio at 2.10M in one of them, with
the host's load between 4 and 9; the ratios are ruio against fio, and ruio
polled against fio polled:

| Depth | fio | fio `--hipri` | ruio | ruio polled |
|---:|---:|---:|---:|---:|
| 1 | 1.26M | 1.22M | 1.41M (1.12×) | 1.44M (1.18×) |
| 4 | 1.78M | 1.96M | 2.04M (1.14×) | 2.23M (1.14×) |
| 16 | 2.21M | 2.43M | 2.43M (1.10×) | 2.67M (1.10×) |
| 32 | 2.36M | 2.58M | 2.62M (1.11×) | 2.90M (1.13×) |
| 64 | 2.37M | 2.58M | 2.65M (1.12×) | 2.89M (1.12×) |
| 128 | 2.37M | 2.61M | 2.55M (1.08×) | 2.90M (1.11×) |

Polling gains ruio 9% to 14% from depth 4 up, 2.04M to 2.23M at depth 4
and 2.55M to 2.90M at 128, and 2% at depth 1, where each read is a turn of
the loop whichever way it completes; it gains fio 9% to 10% from depth 4
up and nothing at depth 1. ruio polled stays ahead of fio polled at every
depth, by 10% to 18%, as ruio is ahead of fio by 8% to 14%. Three things
were measured on the way, each as medians of three runs of 2 s of ruio's
reader alone, in turns with the variant it is compared with:

- The polled ring's file table. Without it a polled file is a plain
  descriptor, which the kernel looks up, and takes and drops a reference
  to, on every read; with the table the open installs the descriptor into
  a slot there, as fio's `registerfiles` does, and the reads name the
  slot. The non-polled reader with plain descriptors in place of direct
  ones read 2% fewer at depth 1 (1.40M to 1.43M) and 4% to 5% fewer at
  depth 32 and 128 (2.53M to 2.65M, 2.52M to 2.64M); the polled reader
  with plain descriptors, measured against fio polled in three runs of 5 s
  before the table was added, read 1.44M, 2.15M, 2.55M, 2.72M, 2.75M and
  2.79M at the six depths, 4% to 7% less than with it from depth 4 up.
- `IORING_SETUP_HYBRID_IOPOLL` (6.13), which sleeps, before polling for a
  request, for half the time the ring has seen a request take, so that a
  slow device does not cost a spinning CPU. On this device, which has
  completed before the poll begins, the sleep is pure cost: 436k reads a
  second against 1.43M at depth 1, 1.78M against 2.77M at depth 32 and
  1.61M against 2.74M at depth 128. The polled ring is set up without it;
  a device slow enough for the sleep to pay would want a measurement of
  its own.
- How a park waits with polled reads in flight. The benchmark's own
  measuring sleep keeps the wake timer armed, so its parks poll a pass at
  a time and look at the main ring between passes; forced to wait inside
  the kernel instead (one entry until a completion), or to poll a pass at
  a time whatever is in flight, the reader measured the same within 1% at
  depth 1, 32 and 128 (1.43M, 1.44M and 1.45M at depth 1). The null block
  device completes a read in the pass that follows its submission, so
  every policy takes one entry per turn there; they part on a device with
  latency, where the kernel's wait spins without entries and the passes
  cost one each.

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
| 2 | 429k | 100% | 484k (1.13×) | 100% | |
| 4 | 850k | 99% | 962k (1.13×) | 100% | |
| 8 | 1.70M | 99% | 1.92M (1.13×) | 99% | |
| 16 | 3.34M | 97% | 3.76M (1.13×) | 97% | 97% |
| 24 | 4.82M | 94% | 5.46M (1.13×) | 94% | 94% |
| 32 | 5.98M | 87% | 6.61M (1.10×) | 85% | 90% |
| 48 | 7.95M | 77% | 8.62M (1.08×) | 74% | 80% |
| 64 | 8.45M | 62% | 8.82M (1.04×) | 57% | 58% |
| 96 | 8.71M | 42% | 9.14M (1.05×) | 39% | 39% |

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
second, medians of runs taken in turns, on the Xeon Platinum 8488C with
Linux 7.0, over loopback: five runs up to 256 connections, three with 1,024
and five with 10,000, where a run of this host's gives either runtime a fifth
more or less from one run to the next, and three runs in a noisier spell had
put the pool at 0.93×:

| Connections | tokio | ruio | ruio, pool | p99 tokio (µs) | p99 ruio (µs) | p99 ruio, pool (µs) |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 46k | 45k (0.98×) | 46k (1.01×) | 27 | 28 | 27 |
| 16 | 191k | 177k (0.93×) | 187k (0.98×) | 103 | 120 | 115 |
| 256 | 178k | 175k (0.98×) | 186k (1.04×) | 1536 | 1573 | 1494 |
| 1,024 | 184k | 183k (0.99×) | 191k (1.04×) | 5926 | 6068 | 5633 |
| 10,000 | 156k | 163k (1.05×) | 168k (1.08×) | 82736 | 83222 | 67270 |

Receiving into a pool, hyper on ruio is level with hyper on tokio with one
connection, and from 256 connections on 4% to 8% ahead, with a 99th
percentile as short or shorter; at 16 connections it is 2% to 3% behind.
Loaded,
both spend about 83% of their CPU in the kernel, 13% in hyper and the
runtime, and 4% in the C library; the adapters' copies, of a request out
of the pool's buffers and of a response into a buffer for the kernel, add
little to that. The kernel does the same for each request under both: one
receive, one send, one segment each way and one wake-up of the client,
counted with probes.

Four things got the adapters there:

- **A flush that does not wait.** hyper flushes every response. Waiting for
  the kernel to finish the send woke the task again for each, as a write
  that waited did in the echo benchmark: 0.76× at 10,000 connections,
  with a 99th percentile four times as long, 259 ms.
- **A multishot receive into a pool.** Reading into a buffer of its own, a
  connection submits a receive for each read, which the kernel, finding
  nothing waiting so soon after the last response, parks on a poll; a
  multishot receive stays armed. That is worth 4% to 6% from 16
  connections on.
- **A write buffer kept.** The adapter copies each response into a buffer
  of its own for the kernel to send, and the buffer now comes back to it
  once the send is done, so a response allocates nothing where it allocated
  twice, and is copied into memory still in the cache: a percent or so,
  with the C library's allocator a third of what it was in the profile.
- **Less bookkeeping a request.** The adapter's send is an operation of its
  own, submitted with a waker that wakes nothing, whose result waits in its
  slot of the driver's table until the next write takes it, and the buffer
  with it. Written behind the stream, as before, the send was detached, and
  the driver copied the operation, 168 bytes with its slot's vtable, into
  the slot, out again when it completed and once more to finish it: a tenth
  of the C library's copying in the profile, more than the response itself.
  The pool's reader keeps the lease it reads from and the bundle's own
  iterator rather than a deque of leases, and a bundle of one buffer is made
  without collecting an empty vector. Together they take 330 instructions
  and some 200 cycles off the 8,450 instructions and 3,050 cycles a request
  took in user space, a percent of the request; seven runs of each binary in
  turns put the pool at 187k against 186k before, and the connections with
  buffers of their own at 177k against 176k, within the rows' noise.

What is left: at 16 connections a turn of the ring delivers 16 completions,
as `epoll_wait` returns 14 events, so what a turn costs is spread as thin
under both, and the kernel does the same for each request bar how the server
learns of it. ruio's CPU goes where tokio's does except for io_uring's own
share, some 900 cycles a request: the deferred work that runs a receive when
its socket wakes, and the poll bookkeeping around it, against epoll's
readiness list and the system calls' entry and exit, which come to more
under tokio. Yet the kernel takes 500 cycles a request more under ruio, in
fewer instructions, and the processor's counters put the difference in
cache lines taken from another core's cache: 49 a request under ruio to 45
under tokio, each worth a couple of hundred cycles on this host. The
multishot receive's request holds the deferred work's list node, the poll's
reference count and its stashed result, which the client's CPU writes when
the socket wakes and the server's CPU then reads, where epoll's callback
writes its entry's link into the ready list and little else; the two
functions that first touch the request, `llist_reverse_order` and
`io_poll_check_events`, take 580 cycles a request between them. The adapters
cannot reach that: it is the price of a wake-up through io_uring's poll.

### The runtime itself

The benchmarks above measure the runtime together with the kernel's work on
sockets and files, which takes most of the time, so a change to the
runtime's own paths shows in them only faintly. `ruio-bench micro` measures
those paths on their own: each is a loop of one small step, run on one CPU
for a second, and the result is steps per second, on ruio and, where tokio
has the same thing, on its current-thread runtime, the runtimes taking turns.
`nop 1` is a task awaiting no-ops one at a time, each a turn of the loop:
queue, enter the kernel and wait, deliver, wake, run. `nop 64` is 64 such
tasks, so that a turn delivers a batch. `spawn` spawns a task that does
nothing and awaits it; `yield` yields to the scheduler; `channel` sends a
value to another task through a bounded channel and gets one back, and
`local channel` does the same through the channels for one thread;
`remote` sends one to a thread of its own, which sends one back through
the runtime's channel, so the task is woken from outside and the ring
parked and unparked; `timer` arms a timer by polling a sleep once and drops
it;
`sleep` is a thousand tasks each sleeping a millisecond at a time, bound by
the millisecond; `mutex` locks and unlocks an uncontended async mutex.
Steps per second, medians of three runs, before and after the second
performance pass, on the Xeon Platinum 8488C with Linux 7.0:

| Step | tokio | ruio before | ruio after |
|---|---:|---:|---:|
| nop 1 | | 3.37M | 3.95M (1.17×) |
| nop 64 | | 14.1M | 17.9M (1.27×) |
| spawn | 2.2M | 6.7M | 9.1M (1.36×) |
| yield | 3.8M | 21.4M | 25.9M (1.21×) |
| channel | 2.0M | 5.8M | 6.3M (1.09×) |
| remote | 189k | 194k | 199k (1.03×) |
| timer | 3.1M | 10.7M | 10.7M (1.00×) |
| sleep | 460k | 500k | 500k (1.00×) |
| mutex | 24.8M | 29.4M | 28.3M (0.96×) |

What the pass changed, in order of effect:

- **Local references.** Every operation a task awaited cost two locked
  instructions on the task's own thread, the driver cloning the waker into
  its table and the scheduler dropping the run queue's reference after the
  run, and a wake by reference from a task's own poll, as yielding does,
  cloned and dropped once more: with 64 no-ops in flight these were a third
  of the user-space time. The task's references from its own thread are now
  counted in a plain cell, and the driver holds the executor's wakers by
  one of them (see [Executor](#executor)): `nop 64` gained a fifth,
  `spawn` and `yield` as much.
- **The park.** A park took four locked instructions before its system
  call, cloning and dropping the `Arc` of the unpark state, which lived
  behind the driver's cell, and a compare-and-exchange and a swap for the
  park's beginning and end; in the profile of `nop 1` most of the cycles of
  the park sat on them. The state lives beside the cell now, and the park
  swaps it once and stores it back: one locked instruction, which `nop 1`
  shows.
- **The table.** The table built each new slot's state on the caller's
  stack and copied all 64 bytes into the slot, where the copy stalled behind
  the smaller stores that built it, ran drop glue for states that own
  nothing, and moved an operation's state out and back in to find out that
  it had not completed; a turn of the ring swapped the flag of the slots
  other threads ask it to close, and every pass of `block_on` swapped the
  main future's flag. All of these go: a few percent each of `nop 64`.
- **Detached operations.** A write behind allocated for the operation it
  detached, and freed it when the send completed; it now goes straight into
  an orphaned slot, inline. The echo server with 16 connections allocates
  nothing per echo where it allocated once, and the adapter's flush, which
  registered a noop waker and allocated for it, asks without registering.

The `mutex` row is within the host's noise, the two runs before the pass
differing by as much between themselves. In the echo benchmark these show
as a percent or two where the server is busy, in the random reads (see
[Random reads with direct I/O](#random-reads-with-direct-io)) as the
user-space share of each read, and in the spin as the turn of the loop that
a spinning thread makes for every echo.

The third pass began with what the runtime's own costs were made of
between its crates. Nothing in the workspace was marked `#[inline]` and
nothing builds it with link-time optimization, so every non-generic
function of the driver's that an operation touches was a call that the
executor, and the futures compiled in the user's crate, could not inline:
the same code built with fat LTO and one codegen unit ran these loops 7%
to 16% faster. Before and after the third pass, medians of five runs taken
in turns, on the Xeon Platinum 8488C with Linux 7.0, with tokio's
current-thread runtime for reference:

| Step | tokio | ruio before | ruio after |
|---|---:|---:|---:|
| nop 1 | | 4.06M | 4.51M (1.11×) |
| nop 64 | | 17.6M | 20.4M (1.16×) |
| spawn | 2.2M | 9.1M | 15.2M (1.67×) |
| yield | 3.8M | 25.6M | 63.9M (2.50×) |
| channel | 2.0M | 6.4M | 8.1M (1.27×) |
| remote | 186k | 196k | 196k (1.00×) |
| timer | 3.1M | 10.7M | 11.0M (1.03×) |
| sleep | 455k | 500k | 500k (1.00×) |
| mutex | 24.8M | 28.8M | 53.0M (1.85×) |

What the pass changed, in order of effect:

- **One borrow a turn.** A turn of the ring borrowed the driver's state
  five times and moved six vectors out and back whether or not anything
  was in them, and the scheduler borrowed the state three times more a
  pass, for the wake timer. A turn borrows it once now, and the timer's
  state is in cells beside it. Reaping reads the completion queue's tail
  once, the submission queue's head is read once an entry into the kernel
  rather than twice an operation, and a completion hands the driver's
  reference to its task straight to the run queue: `yield`, whose step is
  a turn, 1.56×, `spawn` 1.21×, `nop 1` 1.12×.
- **The semaphore's free permits in an atomic.** An uncontended lock and
  unlock of a `Mutex` took the semaphore's standard mutex twice, four
  locked instructions, and takes two compare-and-exchanges now: `mutex`
  1.60×.
- **The main future's waker.** The future given to `block_on` had an
  `Arc` waker of its own, which the driver could not tell from any other:
  every operation it awaited cloned and dropped it, and every wake stored
  a flag with a full barrier, which the loop took with a swap, four locked
  instructions for an operation and two for a wake. It has a task's
  header now: `yield` 1.29×, `spawn` 1.08×.
- **A task's atomics, and the executor's inlining.** A task awaiting
  another's handle is held by a local reference, the share goes without a
  read-modify-write when it is the last reference, and the executor's hot
  paths are marked `#[inline]`: `spawn` 1.30×, `yield` 1.19×, `mutex`
  1.12×.
- **The driver's inlining.** The driver's small functions on the paths of
  an operation, a stream and a lease, marked `#[inline]`: `nop 64` 1.13×
  and `nop 1` 1.05×, as much as the LTO build gave them.
- **Timers without a clone of the runtime.** Arming and disarming a sleep
  reached the runtime by cloning its `Rc`, under a borrow, and now by the
  thread's pointer to it: `timer` 1.06×.
- **Parks out of iowait.** Every park was counted as waiting for a disk.
  The echo server with one connection showed CPU 0 waiting for I/O half
  its time, and shows none now; its round trips are within the runs'
  noise.

The benchmarks that the kernel dominates move by as much as the
runtime's share of them allows, and none moved back. Medians of five runs
taken in turns, before and after the pass: the echo benchmark with one
connection and 64 B messages 47.5k and 47.4k round trips a second, with
16 connections 205k and 206k, with 256 and 1 KiB messages 197k and 197k,
and with 1,024 and 16 KiB messages 152k and 152k; hyper, receiving into a
pool, 45.0k and 45.1k requests a second with one connection, 182k and
183k with 16, its 99th percentile 123 µs and 118, and 177k and 179k with
256.

The pass closed a hole that safe code could reach, too. Every task's
wakers share one vtable, which is how the driver tells them, and a task's
waker sent to another thread and used there to poll an operation of that
thread's ring made the driver count references to the task from the
wrong thread. The executor's retain hook now counts one only for a task
of the runtime running on the thread, which costs `nop 64` at most a
percent and a half. And a run, and `block_on`, clear a remote wake's flag
with a swap rather than a store, which on x86 was an `xchg` already, but
which the memory model needs for the poll to see what every waker that
found the flag set woke it for.

Measured and not adopted: putting a task's small fields before its
future rather than after made no difference, and inlining the release of
the share into every drop of a local reference only moved code about, as
did leaving the executor's inlining out of either half. Reaching the
runtime without a clone of its `Rc` for `spawn` and the driver measured
within the runs' noise, and the code does so anyway, as the sleeps and
the wakes do.

The fourth pass took up four costs the third left. Before and after it,
medians of five runs taken in turns, on a host busier than for the
third's, with tokio's current-thread runtime for reference:

| Step | tokio | ruio before | ruio after |
|---|---:|---:|---:|
| nop 1 | | 4.54M | 4.38M (0.97×) |
| nop 64 | | 20.6M | 20.3M (0.98×) |
| spawn | 2.2M | 15.3M | 17.7M (1.16×) |
| yield | 3.9M | 59.8M | 63.1M (1.06×) |
| channel | 1.7M | 8.1M | 10.8M (1.33×) |
| remote | 184k | 193k | 191k (0.99×) |
| timer | 3.0M | 11.0M | 11.0M (1.00×) |
| sleep | 464k | 500k | 500k (1.00×) |
| mutex | 20.0M | 53.0M | 53.0M (1.00×) |

- **A spawn without an atomic reference.** A task held an `Arc` of its
  scheduler's shared part, which its wakes from other threads reach,
  cloned as it was spawned and dropped as it was freed: the last two
  locked instructions of a spawn and its join on the task's thread. It
  holds the part by a count of the runtime's thread now, and by an atomic
  reference of its own only from when it may be freed on another thread:
  `spawn` 1.16×.
- **Channels without a lock.** In a ping-pong between two tasks, each
  value's way took the channel's standard mutex three times, for the send,
  for the receive that found nothing and kept its waker, and for the one
  that took the value, six locked instructions; and the receive cloned its
  waker into the channel, which the send took out and woke by value, two
  more. The values wait in a ring of stamped slots now, which a send
  claims with one compare-and-exchange and the receiver frees with one
  store, and the receiver's waker stays registered and is woken by
  reference: `channel` 1.33×. A wake from another thread, which `remote`
  measures, costs what it did.

`nop 1` and `nop 64` read 2% to 3% fewer steps a second after the pass,
with the same instructions per step, about 905 for `nop 1`, in more
cycles: the channels' change adds no code to their path, and with the
spawn's change alone they read as before, so it is where the code was
placed. The network benchmarks were not run again: their servers spawn a
task per connection rather than per message, and send nothing through a
channel; `Handle::spawn`, from another thread, is what hands its output
back through a `oneshot`.

Measured and not adopted:

- **Completions held back.** An entry into the kernel runs at most
  `max(20, min_complete)` items of deferred completion work in each of
  its two halves, and under load the echo server settled at 80
  completions an entry, with 256 connections and with 1,024. The kernel
  counts the completions an entry waits for from the head it was given,
  so a ring that keeps the slots of the completions it read until its
  next entry, and asks that entry for that many more, waits no longer but
  has the kernel run more of the work. It did, 115 and 124 completions an
  entry, but the benchmark made 5% and 13% fewer round trips, with a 99th
  percentile 1.6 and 2.1 times as long, whether a quarter of the
  completion queue was held back or 64 slots of it.
- **Other table slots.** A slot of the operation table is 184 bytes,
  most of it room for an orphaned operation, which every operation's poll
  and completion reach past. Slots twice the size left the echo benchmark
  at 1,024 and 10,000 connections within 1%, so their size costs nothing
  that the table's footprint shows, and slots with room for 48 bytes of
  an orphan rather than 160 did no better on the driver's own loops: the
  larger and the smaller both read 3% fewer `nop 1` steps a second than
  slots of this size.

The fifth pass took up the timers, the spawn's allocation, and channels
for one thread, and the echo benchmark's one connection with 64 KiB
messages (see [TCP echo](#tcp-echo)). Before and after it, medians of
five runs taken in turns, on a quiet host, with tokio's current-thread
runtime for reference:

| Step | tokio | ruio before | ruio after |
|---|---:|---:|---:|
| nop 1 | | 4.40M | 4.40M (1.00×) |
| nop 64 | | 20.5M | 20.6M (1.00×) |
| spawn | 2.2M | 17.6M | 19.2M (1.09×) |
| yield | 3.8M | 63.2M | 61.2M (0.97×) |
| channel | 2.0M | 10.8M | 10.7M (1.00×) |
| local channel | | | 18.6M |
| remote | 189k | 188k | 195k (1.04×) |
| timer | 3.1M | 11.0M | 16.5M (1.50×) |
| sleep | 456k | 500k | 500k (1.00×) |
| mutex | 25.0M | 53.0M | 53.0M (1.00×) |

- **Timers without atomic operations.** Arming a sleep and dropping it
  took four atomic operations and two readings of the clock: the wheel
  cloned the task's waker and dropped it, the sleep cloned the `Arc` of
  its runtime's shared part and dropped it, and each poll read the clock
  after the sleep had read it to set its deadline. The wheel holds a task
  by a local reference, the sleep names its runtime by address and id and
  finds it again through a registry of live runtimes only when dropped
  away from its thread, and a poll takes the clock as the pass read it:
  `timer` 1.50×, of which the local references give 1.03×.
- **A task's memory kept.** malloc and free were a sixteenth of a spawn
  and its join in the profile, and the traffic they cause more; the
  scheduler keeps the memory of the tasks freed on its thread, by layout,
  for its spawns: `spawn` 1.09×. In the scheduler itself the cache's 200
  bytes moved the fields every pass reads across cache lines and cost the
  no-op loops 7%, so it is boxed.
- **Channels for one thread.** The cross-thread channels' send takes two
  locked instructions, to claim its slot and to see whether the receiver
  waits, and that is the floor: between a store and a load that another
  thread may reorder stands a barrier, which costs what a locked
  instruction does, so a send that skipped the compare-and-exchange when
  nobody waits would need a fence in its place. `sync::local` has the
  same channels for the tasks of one thread, on `Rc` and cells, with the
  receiver's waker kept registered and woken by reference: `local
  channel` makes 1.69× the round trips of `channel`, and 9.2× tokio's
  channel's.
- **A large message's last buffer.** A receive asked for a buffer less
  than its messages take gets the kernel to hand over all but the last
  as soon as they have arrived, so the server echoes them while the
  kernel copies the rest: one connection with 64 KiB messages 1.17× tokio,
  from 0.96×, its 99th percentile 57 µs from 66, and the rows with small
  messages within a point of what they were.

Two things were looked at and left. Waking rings with `MSG_RING` rather
than the eventfd had been measured in the second pass and lost across
physical cores (see [Kernel features](#kernel-features)); and asking for
the whole of a large message per receive, which the second pass had made
an option, measured within the host's noise either way and gave way to
the ask of a buffer less. `yield` reads 3% fewer steps a second after the
pass with the same instructions per step, as the no-op loops did after the
fourth: where the code was placed, which this pass did not pursue.

The primitives that `sync` and `time` gained after the sixth pass, an
unbounded channel, across cores and for one thread, broadcast and watch
channels, a barrier and a cancellation token (see [Executor](#executor)),
were measured as they came, on the same host, medians of five runs taken
in turns, with tokio's current-thread runtime for reference and
tokio-util's token for `cancel`, and `channel` and `local channel` as
they read in the same runs. `unbounded channel` and `local unbounded
channel` are `channel` and `local channel` through the unbounded ones;
`broadcast` is a value to another task and one back through broadcast
channels with one receiver each, and `watch` through watch channels, each
way a send and a wait for the change; `barrier` is two tasks meeting at a
barrier again and again; and `cancel` arms a wait for a token's
cancellation by polling it once and drops it, as a loop selecting between
a token and its work does with every turn:

| Step | tokio | ruio |
|---|---:|---:|
| channel | 2.06M | 10.9M (5.29×) |
| local channel | | 17.6M |
| unbounded channel | 2.33M | 11.0M (4.71×) |
| local unbounded channel | | 23.3M |
| broadcast | 1.90M | 5.80M (3.05×) |
| watch | 1.29M | 5.16M (3.99×) |
| barrier | 2.15M | 13.5M (6.28×) |
| cancel | 11.1M | 25.1M (2.27×) |

- **The unbounded channel** makes the round trips of the bounded one: a
  send claims its position with one atomic add and publishes its value
  with one more, where the bounded send claims a slot with a
  compare-and-exchange, and the receiver reads a value with one load in
  either; the list of blocks, recycled, allocates nothing in a steady
  state, as the ring does not. The one for one thread makes 1.33× the
  round trips of the bounded local channel, having no room to check and
  no senders to wake for it.
- **Broadcast and watch** pay for what they carry. A broadcast round trip
  takes the tail's lock for each send and the slot's lock twice, for the
  write and the read, where the bounded channel takes none, and makes
  half its round trips; a watch round trip takes the value's write lock
  for each send and its read lock for each look, and the wait for the
  change costs one load. Both still make three to four times tokio's.
- **The barrier** costs each round the leader's lock and the other
  task's one load, and a wake and a run each way: 6.28× tokio's, whose
  barrier waits on a watch channel behind a mutex.
- **The token's wait** clones the waker, takes the waiters' lock to
  register and again to leave, and allocates nothing after the first:
  25.1M arms and drops a second, from 14.8M with the wakers in a
  `BTreeMap`, whose one node was allocated and freed each time, and the
  waiters' bit of the state set as the first registered and cleared as
  the last left, two atomic operations a wait that the bit, set once and
  left set, spares, at the price of one lock for the one cancellation of
  a token that has been waited on.

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
benchmark finds ruio ahead of tokio from 256 connections on, and level with
fewer and at 10,000, bar one connection with 64 KiB messages, where tokio's
echo is cut-through (see [TCP echo](#tcp-echo)). M5 built
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
  devices (`File::read_stream`), and `read_dir` reads directories (see
  [Files](#files)).
- Operations link into chains (`Op::link`, `FileOp::link`).
- Streams read into buffers the kernel consumes incrementally (6.12,
  probed), on request.
- Waking rings with `MSG_RING` was measured against the eventfd, and lost
  across physical cores, so it was not adopted (see
  [Kernel features](#kernel-features)).
- A performance pass: a large write behind goes as a chain of segments
  while the thread waits on one peer, so that the peer receives the first
  while the rest is copied (`Driver::detach_chain`, which leaves a chain to
  finish with nobody waiting), which took one connection with 64 KiB
  messages from 0.78× to 0.94× of tokio; a pool wakes one waiting stream
  per buffer that returns rather than all; the echo benchmark's pool stops
  at 4096 buffers, which took 10,000 connections with 64 KiB messages from
  0.96× to 1.01×; and `Compat` keeps its write buffer for the next write
  (see [TCP echo](#tcp-echo) and [HTTP/1.1](#http11)).
- A second performance pass, on the runtime's own costs, which
  `ruio-bench micro` measures apart from the kernel's (see
  [The runtime itself](#the-runtime-itself)): a park takes one locked
  instruction where it took four, and waking and running a task on its own
  thread takes none, the driver holding the executor's wakers by a counted
  local reference rather than a clone; the table writes its slots in place
  and drops nothing that owns nothing; a detached operation goes straight
  into its slot, inline, so a write behind allocates nothing; an adapter's
  flush asks how the writes went without registering, and a socket keeps
  its ring. `Builder::spin` has a runtime poll before it parks while its
  waits are short, which doubles the round trips of a single connection. A
  stream whose messages run to several buffers can ask the kernel to map
  that many per receive (`RecvStream::buffers`), and a send of several
  buffers names its vector in the entry (`IORING_SEND_VECTORIZED`, probed).
  Measured and not adopted: a mid-pass submission that runs no completion
  work, a park that waits for a batch, `IORING_SETUP_SQ_REWIND`, and the
  larger receive ask as the default.
- A third performance pass, on calls across the crates and on the
  runtime's own atomics and borrows (see
  [The runtime itself](#the-runtime-itself)): the hot paths are marked
  `#[inline]`, which nothing was; the main future's waker is a task's; a
  task awaiting a handle is held by a local reference, and a finished one
  freed without a read-modify-write; a turn of the ring borrows the driver
  once; an uncontended semaphore, and so a `Mutex`, takes and gives back
  its permits with a compare-and-exchange; sleeps and spawns reach the
  runtime without cloning it; and parks leave iowait out. It also made the
  driver hold a task's waker by reference only on the task's own thread,
  which a waker sent to another thread could have it do from there. After
  it, reads and writes went to the kernel in batches of 32, a block plug's,
  which made random reads at depth 64 and above as fast as at 32.
- A fourth performance pass (see
  [The runtime itself](#the-runtime-itself)): a task holds its scheduler's
  shared part by a count of the runtime's thread, so a spawn and its join
  take no locked instruction; and the channels carry values without a
  lock, the receiver's waker kept registered. Measured and not adopted:
  completions held back so that an entry into the kernel runs more of
  their work, and table slots smaller or larger than an orphaned write
  behind needs.
- A fifth performance pass (see
  [The runtime itself](#the-runtime-itself)): the wheel holds tasks by
  local reference and a sleep names its runtime rather than holding it,
  so arming and dropping a timer take no atomic operation, and a pass
  reads the clock once for the timers polled in it; a scheduler keeps the
  memory of its freed tasks for its spawns; `sync::local` has channels for
  the tasks of one thread, which take no atomic operation; and a receive
  stream whose thread waits on one peer asks for a buffer less than its
  messages take, so that the server answers a large message while the
  kernel still copies its last buffer, which took one connection with
  64 KiB messages from 0.95x of tokio to 1.12x.
- A sixth performance pass. The adapters send a response through an
  operation of their own, polled with a waker that wakes nobody, rather
  than a write behind whose payload was copied three times a response,
  and the pool's reader keeps a lease and the bundle's own iterator in
  place of a queue: 4% fewer instructions a request in user space, and the
  HTTP benchmark at 16 connections where it was, 0.97x of tokio, the rest
  of that gap being cache lines the client's core writes into the
  receive's request and the server's core reads, which no user-space
  change reaches (see [HTTP/1.1](#http11)). The receive stall of the fifth
  pass did not reproduce in 40 runs of its exact scenario and over a
  hundred benchmark runs without `IORING_RECVSEND_POLL_FIRST`, and the
  cancel, the restart and the kernel's two issue paths read sound, so the
  flag stays as a precaution and the stall is an open question. And
  polled I/O came: a second ring set up with `IOPOLL`, which files opened
  with `OpenOptions::polled` read and write through, with a file table of
  its own and the main ring's buffers; on null_blk with a poll queue it
  reads 9% to 14% more a second than the main ring from depth 4 up, and
  stays 10% to 18% ahead of fio's polled engine (see
  [Random reads with direct I/O](#random-reads-with-direct-io)).
  `IORING_SETUP_HYBRID_IOPOLL` measured far slower and was not adopted.
- `time::interval` ticks at a period, and `sync` has unbounded channels,
  across cores and for one thread, broadcast and watch channels, a
  `Barrier` and a `CancellationToken` (see [Executor](#executor)), each
  built as the primitives before them: hot paths without a lock, a flag
  that says whether anyone waits, wakers woken outside every lock, and a
  step of the task's budget for each that is ready at once. In
  `ruio-bench micro` a round trip through the unbounded channels makes
  4.7× the round trips of tokio's and through the unbounded channels for
  one thread 23.3M a second, through broadcast channels 3.1× tokio's and
  through watch channels 4.0×, two tasks meet at a barrier 6.3× as often,
  and arming a wait for a token's cancellation and dropping it runs 2.3×
  as fast as tokio-util's, which a slab for the waiters' wakers, in place
  of a map that allocated a node for each, took from 1.3× (see [The
  runtime itself](#the-runtime-itself)).

## Open questions

- License. Crates are `publish = false` until one is chosen.
- The receive stream's stall. While the ask of a buffer less (see
  [Networking](#networking)) was built, the test of many re-arms once hung
  within a few hundred cycles: a capped receive, issued at submission
  while data waited and the peer still wrote, posted its capped completion
  (`IORING_CQE_F_SOCK_NONEMPTY` set) and nothing more, with data in the
  socket; issued from poll (`IORING_RECVSEND_POLL_FIRST`) it never did, so
  the flag stayed. The cause was not found. In Linux 7.0's io_uring, a
  receive issued at submission that the cap stops short posts its
  completion and returns `IOU_RETRY` (net.c, `io_recv_finish`);
  `io_queue_async` (io_uring.c) arms poll on the socket, which
  `__io_arm_poll_handler` (poll.c) finds readable, so it takes ownership
  of the request and queues its task work with the mask, and
  `io_poll_task_func` receives the rest in the same entry into the kernel,
  whose task work runs after its submissions (io_uring.c,
  `__do_sys_io_uring_enter`; tw.c, `__io_run_local_work`). Issued from
  poll, the same receive returns `IOU_REQUEUE` and `io_poll_task_func`
  queues itself again with no mask, so the next run polls the socket
  first. Both leave the request in the cancellation table, owned by queued
  task work, with its wait entry on the socket, and a wake-up meanwhile
  adds a reference that makes `io_poll_check_events` loop once more. The
  restart's cancellation cannot miss it: `__io_poll_cancel` finds the
  request by `user_data` in the table, where it stays until it completes,
  marks it cancelled and kicks or joins its task work, and nothing but the
  ring's thread runs that work (`IORING_SETUP_DEFER_TASKRUN`); `ENOENT`
  means the request has ended, and its last completion re-arms the
  stream, which drops the cancellation's own result. Traced with the
  io_uring tracepoints, the tcp test of the ask ran the exact scenario:
  the restarted receive completed capped 5 µs after its submission, before
  its poll arm, and the rest followed from the task work the arm queued.
  Without the flag that test passed 40 runs of 40, the test of many
  re-arms 300 of 300, a harness that ran that test's cycle over 30 million
  times across eight pool geometries, re-arming the receive 3 million
  times, never stalled, and the echo benchmark did not stall in 35 runs with one
  connection and 64 KiB messages, 42 with 16 and 30 with 2, where the ask
  changes some ten times a run, on 7.0.0-1012-aws. One connection with
  64 KiB messages measured the same with the flag and without, medians of
  five runs in turns 16.9k and 16.8k round trips a second on a host whose
  noise ran from 16.4k to 19.7k. The flag bears only on a receive's first
  issue, so it costs nothing measurable, and stands until the stall is
  seen again or explained. `ReadStream` and `AcceptStream` take the same
  path from submission to poll, and single-shot receives have no cap; none
  has shown a stall.
