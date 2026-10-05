//! A pool of threads for blocking work, and [`spawn_blocking`], which runs a
//! closure on one and hands its output back to whoever awaits it.
//!
//! A runtime's thread must not block: a system call that waits, such as a
//! name lookup, or work that runs long, holds up every task of the runtime
//! and the ring under them. `spawn_blocking` runs such a closure on a thread
//! of a pool the process shares, and gives a [`BlockingHandle`] that awaits
//! the output from any thread, inside a runtime or not. The pool makes
//! threads as jobs come, up to a limit, and lets them go once they have been
//! idle for a while; a program that wants other limits, or a pool of its
//! own for some of its work, makes a [`Pool`].
//!
//! A job is one allocation: the closure, its output and the waker of the
//! task awaiting it, with one word of state they are handed over through,
//! as the halves of a [`oneshot`](crate::sync::oneshot) channel hand over
//! their value. The worker that runs the job writes the output and finds the
//! waker with one read-modify-write of the state, and the handle registers
//! its waker and takes the output with one each. The queue of jobs waiting
//! for a thread is behind a mutex, which a job takes once to go in and a
//! worker once to come out; a worker with nothing to do waits on a condition
//! variable, and a job wakes one worker, if one waits, or makes a thread if
//! none does and the limit allows, so that no job waits for a thread that
//! is not coming. A pool may have its workers spin for a while before they
//! wait, so that a job that comes soon after the last is taken without a
//! system call; off unless set, as a thread that spins takes its core from
//! a runtime pinned there.

use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, PoisonError};
use std::task::{Context, Poll, Waker, ready};
use std::thread;
use std::time::{Duration, Instant};

use crate::coop;
use crate::sync::lock;
use crate::task::JoinError;

/// A worker has taken the job: to run it, or to drop it if it was cancelled
/// first.
const CLAIMED: usize = 1;
/// The handle asked for the job not to run, which is honoured if it came
/// before `CLAIMED`.
const CANCELLED: usize = 2;
/// The output is written: the job is over.
const COMPLETE: usize = 4;
/// The handle's waker is registered, to be woken as the job completes.
const RX_TASK: usize = 8;
/// The handle is gone.
const RX_CLOSED: usize = 16;

/// Runs `f` on a thread of the process's pool of threads for blocking work,
/// and gives a handle that awaits its output; see [`Pool::spawn`].
///
/// The pool has the default configuration of [`Builder`]: up to 512
/// threads, each let go after ten seconds idle, and no spinning.
///
/// ```
/// # ruio_executor::Runtime::new()?.block_on(async {
/// let sum = ruio_executor::spawn_blocking(|| (1..=100u64).sum::<u64>())
///     .await
///     .unwrap();
/// assert_eq!(sum, 5050);
/// # });
/// # Ok::<(), std::io::Error>(())
/// ```
pub fn spawn_blocking<F, R>(f: F) -> BlockingHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    global().spawn(f)
}

/// The pool [`spawn_blocking`] runs its closures on, made at the first use
/// and never dropped.
pub fn global() -> &'static Pool {
    static GLOBAL: OnceLock<Pool> = OnceLock::new();
    GLOBAL.get_or_init(Pool::new)
}

/// Configures and creates a [`Pool`].
#[derive(Clone, Debug)]
pub struct Builder {
    threads: usize,
    keep_alive: Duration,
    spin: Duration,
    stack_size: Option<usize>,
}

impl Builder {
    /// A builder with the default configuration: up to 512 threads, each
    /// let go after ten seconds idle, no spinning, and the standard
    /// library's stack size.
    pub fn new() -> Self {
        Self {
            threads: 512,
            keep_alive: Duration::from_secs(10),
            spin: Duration::ZERO,
            stack_size: None,
        }
    }

    /// Sets how many threads the pool may have at once. Jobs beyond that
    /// wait in the queue for a thread to finish its job.
    ///
    /// # Panics
    ///
    /// If `threads` is zero.
    pub fn threads(mut self, threads: usize) -> Self {
        assert!(threads > 0, "a pool has at least one thread");
        self.threads = threads;
        self
    }

    /// Sets how long a thread with nothing to do waits for a job before it
    /// goes.
    pub fn keep_alive(mut self, keep_alive: Duration) -> Self {
        self.keep_alive = keep_alive;
        self
    }

    /// Has a thread that has finished a job spin for up to `budget`, looking
    /// for the next, before it waits for one: a job that comes within the
    /// budget is taken without the system calls that wake a waiting thread,
    /// and without the wait for the kernel to run it. A spinning thread
    /// occupies its core, so this is for pools whose jobs come close
    /// together, on hosts with cores to spare beside the runtimes' own.
    ///
    /// Off unless set; `Duration::ZERO` turns it off.
    pub fn spin(mut self, budget: Duration) -> Self {
        self.spin = budget;
        self
    }

    /// Sets the stack size of the pool's threads, in bytes; the standard
    /// library's unless set.
    pub fn stack_size(mut self, bytes: usize) -> Self {
        self.stack_size = Some(bytes);
        self
    }

    /// Creates the pool, with no threads until the first job comes.
    pub fn build(&self) -> Pool {
        Pool {
            inner: Arc::new(Inner {
                shared: Mutex::new(Shared {
                    queue: VecDeque::new(),
                    threads: 0,
                    idle: 0,
                    notified: 0,
                    spinning: 0,
                    closed: false,
                }),
                condvar: Condvar::new(),
                queued: AtomicUsize::new(0),
                limit: self.threads,
                keep_alive: self.keep_alive,
                spin: self.spin,
                stack_size: self.stack_size,
            }),
        }
    }
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

/// A pool of threads that run blocking closures; see [`spawn_blocking`],
/// which runs them on the process's.
///
/// Threads are made as jobs come, up to the pool's limit, and let go after
/// its keep-alive idle. Dropping the pool lets its threads go once they have
/// run the jobs queued; a handle to such a job still gets its output.
///
/// ```
/// use std::time::Duration;
///
/// use ruio_executor::blocking::Pool;
///
/// # ruio_executor::Runtime::new()?.block_on(async {
/// let pool = Pool::builder().threads(2).keep_alive(Duration::from_secs(1)).build();
/// let answer = pool.spawn(|| 42).await.unwrap();
/// assert_eq!(answer, 42);
/// # });
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct Pool {
    inner: Arc<Inner>,
}

/// What a pool and its workers share.
struct Inner {
    shared: Mutex<Shared>,
    /// Waited on by workers with nothing to do.
    condvar: Condvar,
    /// How many jobs wait in the queue: what spinning workers look at,
    /// without the lock.
    queued: AtomicUsize,
    /// How many threads the pool may have.
    limit: usize,
    /// How long an idle thread waits for a job before it goes.
    keep_alive: Duration,
    /// How long a thread spins for a job before it waits, if at all.
    spin: Duration,
    stack_size: Option<usize>,
}

struct Shared {
    /// The jobs waiting for a thread, oldest first.
    queue: VecDeque<RawJob>,
    /// How many threads the pool has, those being made included.
    threads: usize,
    /// How many threads wait on the condition variable without a wake
    /// coming to them: a job wakes one, counting it out here and into
    /// `notified`, so that the next job does not count on the same thread.
    idle: usize,
    /// How many wakes are on their way to waiting threads, each for a job.
    notified: usize,
    /// How many threads spin for a job, which take the next ones without
    /// being woken.
    spinning: usize,
    /// Whether the pool is dropped: the threads go once the queue is empty.
    closed: bool,
}

/// A job as the queue holds it, whatever its closure and output: a pointer
/// to the job, counting one of its references, and the function that runs
/// it, which knows the job's type and takes the reference over.
struct RawJob {
    job: *const (),
    run: unsafe fn(*const ()),
}

// SAFETY: the pointer is one of a job's atomic references, and a job's
// closure and output are `Send`, as `Pool::spawn` requires.
unsafe impl Send for RawJob {}

impl Inner {
    /// Queues `job`, and sees that a thread comes for it: wakes a waiting
    /// one, or makes one if none waits or spins and the limit allows.
    fn push(self: &Arc<Self>, job: RawJob) {
        let (notify, make) = {
            let mut shared = lock(&self.shared);
            shared.queue.push_back(job);
            // Release: a spinner that sees the count sees the job, which the
            // lock publishes to it in any case.
            self.queued.fetch_add(1, Ordering::Release);
            if shared.queue.len() <= shared.spinning {
                // A spinning thread takes it.
                (false, false)
            } else if shared.idle > 0 {
                shared.idle -= 1;
                shared.notified += 1;
                (true, false)
            } else if shared.threads < self.limit {
                shared.threads += 1;
                (false, true)
            } else {
                // Every thread is busy: one takes the job as it finishes.
                (false, false)
            }
        };
        if notify {
            self.condvar.notify_one();
        }
        if make {
            self.make_thread();
        }
    }

    /// Makes a thread, counted in `threads` already.
    ///
    /// # Panics
    ///
    /// If the system cannot make the thread.
    #[cold]
    fn make_thread(self: &Arc<Self>) {
        let inner = Arc::clone(self);
        let mut builder = thread::Builder::new().name("ruio-blocking".to_owned());
        if let Some(bytes) = self.stack_size {
            builder = builder.stack_size(bytes);
        }
        if let Err(err) = builder.spawn(move || inner.work()) {
            lock(&self.shared).threads -= 1;
            panic!("cannot make a thread of the blocking pool: {err}");
        }
    }

    /// A worker's life: jobs, one after another, until it has waited for one
    /// in vain for the keep-alive, or the pool is dropped and the queue
    /// empty.
    fn work(&self) {
        while let Some(job) = self.next_job() {
            // SAFETY: the function was made for the job the pointer names,
            // and the pointer counts a reference to it, which the function
            // takes over.
            unsafe { (job.run)(job.job) };
        }
    }

    /// The next job for a worker, waiting for one; or `None` once the worker
    /// is to go, counted out of the pool's threads.
    fn next_job(&self) -> Option<RawJob> {
        let mut shared = lock(&self.shared);
        loop {
            if let Some(job) = shared.queue.pop_front() {
                // Relaxed: the lock publishes the queue; the count only tells
                // spinners whether to take it.
                self.queued.fetch_sub(1, Ordering::Relaxed);
                return Some(job);
            }
            if shared.closed {
                shared.threads -= 1;
                return None;
            }
            if !self.spin.is_zero() {
                shared.spinning += 1;
                drop(shared);
                let found = self.spin_for_job();
                shared = lock(&self.shared);
                shared.spinning -= 1;
                if found {
                    // Looked for again under the lock: a job seen may have
                    // gone to another thread.
                    continue;
                }
            }
            shared.idle += 1;
            let (guard, timeout) = self
                .condvar
                .wait_timeout(shared, self.keep_alive)
                .unwrap_or_else(PoisonError::into_inner);
            shared = guard;
            if shared.notified > 0 {
                // Woken for a job, which the next turn of the loop takes;
                // the job counted this thread out of the idle ones.
                shared.notified -= 1;
                continue;
            }
            shared.idle -= 1;
            if timeout.timed_out() && shared.queue.is_empty() {
                shared.threads -= 1;
                return None;
            }
        }
    }

    /// Spins for the budget, looking for a job; whether one was seen.
    #[cold]
    fn spin_for_job(&self) -> bool {
        let start = Instant::now();
        loop {
            for _ in 0..64 {
                // Acquire: the job seen is taken under the lock, which
                // orders it in any case.
                if self.queued.load(Ordering::Acquire) > 0 {
                    return true;
                }
                std::hint::spin_loop();
            }
            if start.elapsed() >= self.spin {
                return false;
            }
        }
    }
}

impl Pool {
    /// A pool with the default configuration; see [`Builder`].
    pub fn new() -> Self {
        Builder::new().build()
    }

    /// A builder for a pool with a different configuration.
    pub fn builder() -> Builder {
        Builder::new()
    }

    /// Runs `f` on a thread of the pool, as soon as one is free, and gives a
    /// handle that awaits its output: the closure's return value, or a
    /// [`JoinError`] if it panicked or the job was aborted before it ran.
    /// The handle can be awaited from any thread, inside a runtime or not,
    /// and dropping it leaves the job to run, its output dropped as it is
    /// made.
    pub fn spawn<F, R>(&self, f: F) -> BlockingHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let job = Arc::new(Job {
            state: AtomicUsize::new(0),
            closure: UnsafeCell::new(Some(f)),
            output: UnsafeCell::new(None),
            waker: UnsafeCell::new(None),
        });
        let raw = RawJob {
            job: Arc::into_raw(Arc::clone(&job)).cast(),
            run: Job::<F, R>::run_raw,
        };
        self.inner.push(raw);
        BlockingHandle { job }
    }

    /// How many threads the pool has at the moment, busy or idle.
    pub fn threads(&self) -> usize {
        lock(&self.inner.shared).threads
    }
}

impl Default for Pool {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        {
            let mut shared = lock(&self.inner.shared);
            shared.closed = true;
        }
        self.inner.condvar.notify_all();
    }
}

impl fmt::Debug for Pool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let shared = lock(&self.inner.shared);
        f.debug_struct("Pool")
            .field("threads", &shared.threads)
            .field("queued", &shared.queue.len())
            .field("limit", &self.inner.limit)
            .finish_non_exhaustive()
    }
}

/// A job: its closure, until a worker takes it; its output, once the worker
/// is done; and the waker of the task awaiting it.
struct Job<F, R> {
    /// The [`CLAIMED`], [`CANCELLED`], [`COMPLETE`], [`RX_TASK`] and
    /// [`RX_CLOSED`] bits.
    state: AtomicUsize,
    /// The worker's alone, from its claim.
    closure: UnsafeCell<Option<F>>,
    /// Written by the worker before `COMPLETE`; then the handle's.
    output: UnsafeCell<Option<Result<R, JoinError>>>,
    /// The task awaiting the output: written by the handle only while
    /// `RX_TASK` is clear and `COMPLETE` is not set, read by the worker once
    /// it set `COMPLETE` and found `RX_TASK`.
    waker: UnsafeCell<Option<Waker>>,
}

// SAFETY: the closure, the output and the waker are each reached by one side
// at a time, as the state hands them over, so they only move between
// threads; the closure and the output are `Send`, as `Pool::spawn`
// requires, and a `Waker` is.
unsafe impl<F: Send, R: Send> Send for Job<F, R> {}
// SAFETY: as above.
unsafe impl<F: Send, R: Send> Sync for Job<F, R> {}

impl<F, R> Job<F, R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    /// Runs the job at `job` on this thread, letting go of the reference the
    /// queue held.
    ///
    /// # Safety
    ///
    /// `job` must be a pointer `Arc::into_raw` made of an `Arc<Job<F, R>>`,
    /// counting a reference that nothing else lets go of.
    unsafe fn run_raw(job: *const ()) {
        // SAFETY: the caller's.
        let job = unsafe { Arc::from_raw(job.cast::<Self>()) };
        job.run();
    }

    /// Runs the closure, unless the job was cancelled first, and completes
    /// the job with what it returned, or the error, waking the handle's task
    /// if it waits.
    fn run(&self) {
        // Acquire: a cancellation before the claim is seen. Of the claim and
        // the cancellation, read-modify-writes of one word, whichever comes
        // second sees the first.
        let state = self.state.fetch_or(CLAIMED, Ordering::Acquire);
        // SAFETY: the claim makes the closure the worker's alone.
        let closure = unsafe { (*self.closure.get()).take() };
        let closure = closure.expect("a job runs once");
        let result = if state & CANCELLED != 0 {
            // Dropped before anything is published: dropping a closure can
            // run any code.
            drop(closure);
            Err(JoinError::cancelled())
        } else {
            panic::catch_unwind(AssertUnwindSafe(closure)).map_err(JoinError::panicked)
        };
        // SAFETY: until `COMPLETE` the output is the worker's alone.
        unsafe { *self.output.get() = Some(result) };
        // Release: the handle that sees `COMPLETE` sees the output. Acquire:
        // the waker read is the one registered.
        let state = self.state.fetch_or(COMPLETE, Ordering::AcqRel);
        if state & (RX_TASK | RX_CLOSED) == RX_TASK {
            // SAFETY: with `COMPLETE` set the handle no longer writes the
            // waker, and `RX_TASK` says it wrote one.
            let waker = unsafe { &*self.waker.get() };
            if let Some(waker) = waker {
                waker.wake_by_ref();
            }
        }
    }
}

/// What a handle reaches of its job, whatever the closure.
trait Reach<R>: Send + Sync {
    fn state(&self) -> &AtomicUsize;
    fn output(&self) -> &UnsafeCell<Option<Result<R, JoinError>>>;
    fn waker(&self) -> &UnsafeCell<Option<Waker>>;
}

impl<F: Send, R: Send> Reach<R> for Job<F, R> {
    fn state(&self) -> &AtomicUsize {
        &self.state
    }

    fn output(&self) -> &UnsafeCell<Option<Result<R, JoinError>>> {
        &self.output
    }

    fn waker(&self) -> &UnsafeCell<Option<Waker>> {
        &self.waker
    }
}

/// Awaits the output of a blocking job; made by [`Pool::spawn`] and
/// [`spawn_blocking`]. `Send` and `Sync`, so it can be awaited from any
/// thread, and from the task that spawned the job or another.
///
/// The output is the closure's, or a [`JoinError`]: a panic's, with its
/// payload, if the closure panicked, or a cancelled one if the job was
/// aborted before a thread took it. Dropping the handle leaves the job to
/// run; its output is then dropped as it is made.
#[must_use = "futures do nothing unless polled"]
pub struct BlockingHandle<R> {
    job: Arc<dyn Reach<R>>,
}

impl<R> BlockingHandle<R> {
    /// Cancels the job if no thread has taken it yet, in which case awaiting
    /// the handle returns a cancelled [`JoinError`]. A job that has started
    /// runs to its end, as a thread in a blocking call cannot be stopped.
    pub fn abort(&self) {
        // Relaxed: the bit is all the worker reads, and of this and the
        // claim, read-modify-writes of one word, whichever comes second sees
        // the first.
        self.job.state().fetch_or(CANCELLED, Ordering::Relaxed);
    }

    /// Whether the job is over: run, or cancelled before it ran.
    pub fn is_finished(&self) -> bool {
        // Acquire: as in `poll`.
        self.job.state().load(Ordering::Acquire) & COMPLETE != 0
    }
}

impl<R> Future for BlockingHandle<R> {
    type Output = Result<R, JoinError>;

    /// # Panics
    ///
    /// If polled again after it completed.
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let job = &*self.get_mut().job;
        let proceed = ready!(coop::poll_proceed(cx));
        let state = job.state();
        // Acquire: the output written before `COMPLETE` is seen, and the
        // waker this registered before is the one in the cell.
        let mut current = state.load(Ordering::Acquire);
        if current & COMPLETE == 0 && current & RX_TASK != 0 {
            // SAFETY: the handle alone writes the waker, so reading it races
            // only with the worker's reading.
            let waker = unsafe { &*job.waker().get() };
            if waker.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
                return Poll::Pending;
            }
            // Acquire, as above. Once `RX_TASK` is clear before the job
            // completes, the worker leaves the waker alone.
            current = state.fetch_and(!RX_TASK, Ordering::AcqRel);
        }
        if current & COMPLETE == 0 {
            // SAFETY: `RX_TASK` is clear and the job has not completed, so
            // the worker does not read the waker until this sets `RX_TASK`
            // again.
            let old = unsafe { (*job.waker().get()).replace(cx.waker().clone()) };
            // Release: a worker that finds `RX_TASK` reads the new waker.
            // Acquire: as above.
            current = state.fetch_or(RX_TASK, Ordering::AcqRel);
            // Dropped once the state is settled: dropping a waker can run
            // any code.
            drop(old);
            if current & COMPLETE == 0 {
                return Poll::Pending;
            }
        }
        proceed.made_progress();
        // SAFETY: `COMPLETE` is set, after which only the handle touches the
        // output.
        let output = unsafe { (*job.output().get()).take() };
        Poll::Ready(output.expect("`BlockingHandle` polled after it completed"))
    }
}

impl<R> Drop for BlockingHandle<R> {
    fn drop(&mut self) {
        // Relaxed: the worker that sees the bit leaves the waker alone, and
        // the output goes with the job's last reference, wherever that is.
        self.job.state().fetch_or(RX_CLOSED, Ordering::Relaxed);
    }
}

impl<R> fmt::Debug for BlockingHandle<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockingHandle")
            .field("finished", &self.is_finished())
            .finish_non_exhaustive()
    }
}
