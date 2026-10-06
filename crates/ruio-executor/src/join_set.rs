//! A set of tasks, whose outputs come as the tasks finish.
//!
//! A set spawns tasks and hands their outputs back in the order they
//! finish. Each task's future is wrapped in a reporter, which pushes the
//! output into the set's queue as the future completes, or, if the future
//! is dropped first, as an aborted or a panicking task's is, a note that
//! the task ended without one; the task awaiting the set is woken by
//! reference. So a task's finishing costs a push on a deque and nothing
//! atomic, the set allocates nothing for a task beyond the task itself,
//! and taking an output pops the oldest rather than polling every task. The
//! set keeps each task's join handle, in a slab whose index the reporter
//! names its task by, for aborting the tasks and for the error of one that
//! ended without an output.
//!
//! A task spawned onto another core is a task of that core, and the set's
//! task for it is a reporter on its own, which awaits the remote handle and
//! pushes what comes: the output, or the error the task ended with there.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::fmt;
use std::future::{Future, poll_fn};
use std::mem;
use std::panic;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, ready};

use crate::blocking::{self, BlockingHandle, Pool};
use crate::coop;
use crate::cores::handle_of;
use crate::handle::Handle;
use crate::sync::local::WakerCell;
use crate::task::{AbortHandle, JoinError, JoinHandle, RemoteAbortHandle, RemoteJoinHandle};

/// A set of tasks spawned on the thread's runtime, whose outputs come, one
/// at a time, in the order the tasks finish.
///
/// [`spawn`](JoinSet::spawn) adds a task and gives an [`AbortHandle`] to
/// it; [`join_next`](JoinSet::join_next) waits for the next task to finish
/// and takes its output, or the [`JoinError`] of one that panicked or was
/// aborted, and is `None` once the set is empty. Dropping the set aborts
/// the tasks still in it. Like the tasks, the set stays on its thread.
///
/// ```compile_fail
/// fn assert_send<T: Send>() {}
/// assert_send::<ruio_executor::JoinSet<()>>();
/// ```
///
/// ```
/// use ruio_executor::JoinSet;
///
/// # ruio_executor::Runtime::new()?.block_on(async {
/// let mut set = JoinSet::new();
/// for n in 0..3 {
///     set.spawn(async move { n * 2 });
/// }
/// let mut outputs = Vec::new();
/// while let Some(output) = set.join_next().await {
///     outputs.push(output.unwrap());
/// }
/// outputs.sort_unstable();
/// assert_eq!(outputs, [0, 2, 4]);
/// # });
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct JoinSet<T> {
    /// What the tasks' reporters push into.
    inner: Rc<Inner<T>>,
    /// The tasks' join handles, by index, in a slab: a reporter names its
    /// task by the index, and the handle stays until the task has reported.
    handles: Vec<Option<JoinHandle<()>>>,
    /// The free indices of the slab.
    free: Vec<usize>,
}

/// What a set and the reporters of its tasks share.
struct Inner<T> {
    /// The tasks that have finished, oldest first.
    finished: RefCell<VecDeque<Finished<T>>>,
    /// The task awaiting the set.
    waker: WakerCell,
    /// Set once the set has let its tasks go, so that their reporters push
    /// nothing into a queue nobody reads.
    detached: Cell<bool>,
}

/// What a reporter pushes as its task finishes.
enum Finished<T> {
    /// The task at the index finished with this output.
    Output(usize, T),
    /// The task at the index ended without an output: it was aborted, or
    /// it panicked.
    Ended(usize),
    /// The task at the index ran on another core, and ended there with
    /// this error.
    Failed(usize, JoinError),
}

impl<T> Inner<T> {
    fn new() -> Self {
        Self {
            finished: RefCell::new(VecDeque::new()),
            waker: WakerCell::new(),
            detached: Cell::new(false),
        }
    }

    /// Queues what a task finished with, and wakes the task awaiting it.
    #[inline]
    fn push(&self, finished: Finished<T>) {
        if self.detached.get() {
            // Dropped outside any borrow: an output's drop can run any code.
            drop(finished);
            return;
        }
        self.finished.borrow_mut().push_back(finished);
        self.waker.wake();
    }
}

/// A task's future, wrapped to report to its set as it finishes.
struct Report<F: Future> {
    future: F,
    inner: Rc<Inner<F::Output>>,
    /// The task's index in the set's slab.
    index: usize,
    /// Set once the output has been pushed.
    reported: bool,
}

impl<F: Future> Future for Report<F> {
    type Output = ();

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: the future is pinned with the reporter, which never moves
        // it out, and nothing below moves it either.
        let this = unsafe { self.get_unchecked_mut() };
        // SAFETY: as above: the future stays where it is until the reporter
        // is dropped in place.
        let future = unsafe { Pin::new_unchecked(&mut this.future) };
        let output = ready!(future.poll(cx));
        this.reported = true;
        this.inner.push(Finished::Output(this.index, output));
        Poll::Ready(())
    }
}

impl<F: Future> Drop for Report<F> {
    #[inline]
    fn drop(&mut self) {
        if !self.reported {
            self.inner.push(Finished::Ended(self.index));
        }
    }
}

impl<T: 'static> JoinSet<T> {
    /// An empty set.
    pub fn new() -> Self {
        Self {
            inner: Rc::new(Inner::new()),
            handles: Vec::new(),
            free: Vec::new(),
        }
    }

    /// How many tasks are in the set: spawned, and not yet taken out by
    /// [`join_next`](JoinSet::join_next) or its like.
    #[inline]
    pub fn len(&self) -> usize {
        self.handles.len() - self.free.len()
    }

    /// Whether the set has no task in it.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Spawns `future` as a task of the thread's runtime, in the set, and
    /// gives a handle to abort it with. Its output, or the error of its
    /// ending without one, comes from [`join_next`](JoinSet::join_next).
    ///
    /// # Panics
    ///
    /// If the thread is not inside [`Runtime::block_on`](crate::Runtime).
    pub fn spawn<F>(&mut self, future: F) -> AbortHandle
    where
        F: Future<Output = T> + 'static,
    {
        let index = self.free.pop().unwrap_or(self.handles.len());
        let handle = crate::spawn(Report {
            future,
            inner: Rc::clone(&self.inner),
            index,
            reported: false,
        });
        let abort = handle.abort_handle();
        if index == self.handles.len() {
            self.handles.push(Some(handle));
        } else {
            self.handles[index] = Some(handle);
        }
        abort
    }

    /// Waits for the next task to finish and takes its output, or the
    /// [`JoinError`] of one that panicked or was aborted; `None` once the
    /// set is empty. Tasks come in the order they finish.
    pub async fn join_next(&mut self) -> Option<Result<T, JoinError>> {
        poll_fn(|cx| self.poll_join_next(cx)).await
    }

    /// The next task to have finished, as for [`join_next`](JoinSet::join_next);
    /// arranges for the task to be woken when one finishes otherwise, or,
    /// once its budget is spent, wakes it at once.
    pub fn poll_join_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<T, JoinError>>> {
        let proceed = ready!(coop::poll_proceed(cx));
        if self.is_empty() {
            proceed.made_progress();
            return Poll::Ready(None);
        }
        let Some(finished) = self.take(Some(cx)) else {
            return Poll::Pending;
        };
        proceed.made_progress();
        Poll::Ready(Some(self.collect(finished)))
    }

    /// The next task to have finished, if one has, without waiting; `None`
    /// if none has, or the set is empty.
    pub fn try_join_next(&mut self) -> Option<Result<T, JoinError>> {
        let finished = self.take(None)?;
        Some(self.collect(finished))
    }

    /// Takes the oldest finished task; if there is none, and a context is
    /// given, registers its waker and looks again, so that no finish is
    /// missed.
    #[inline]
    fn take(&mut self, cx: Option<&Context<'_>>) -> Option<Finished<T>> {
        if let Some(finished) = self.inner.finished.borrow_mut().pop_front() {
            return Some(finished);
        }
        let cx = cx?;
        self.inner.waker.register(cx.waker());
        self.inner.finished.borrow_mut().pop_front()
    }

    /// What the task `finished` reports finished with, its handle leaving
    /// the slab: its output, or, for a task that ended without one, the
    /// error its handle has, or the error it ended with on another core.
    #[inline]
    fn collect(&mut self, finished: Finished<T>) -> Result<T, JoinError> {
        let (index, output) = match finished {
            Finished::Output(index, output) => (index, Ok(Some(output))),
            Finished::Ended(index) => (index, Ok(None)),
            Finished::Failed(index, err) => (index, Err(err)),
        };
        let mut handle = self.handles[index]
            .take()
            .expect("a task reports once, and its handle stays until it has");
        self.free.push(index);
        match output {
            Ok(Some(output)) => Ok(output),
            Ok(None) => Err(handle
                .finished_output()
                .expect("a task that ended without an output has finished")
                .expect_err("a task that ended without an output did not finish with one")),
            Err(err) => Err(err),
        }
    }

    /// Aborts every task in the set. Each then finishes as cancelled, which
    /// [`join_next`](JoinSet::join_next) reports.
    pub fn abort_all(&mut self) {
        for handle in self.handles.iter().flatten() {
            handle.abort();
        }
    }

    /// Aborts every task in the set and waits for all of them to finish,
    /// leaving the set empty.
    pub async fn shutdown(&mut self) {
        self.abort_all();
        while self.join_next().await.is_some() {}
    }

    /// Lets every task go, without aborting it: the tasks run on, their
    /// outputs going nowhere, and the set is empty, the outputs it held
    /// dropped.
    pub fn detach_all(&mut self) {
        self.inner.detached.set(true);
        let finished = mem::take(&mut *self.inner.finished.borrow_mut());
        self.inner = Rc::new(Inner::new());
        self.free.clear();
        let handles = mem::take(&mut self.handles);
        // Dropped after the set has let the tasks' reports go, so that a
        // handle's detaching a finished task pushes no note of it, and
        // outside any borrow, as an output's drop can run any code.
        drop(handles);
        drop(finished);
    }

    /// Waits for every task to finish, and gives their outputs in the order
    /// they finished.
    ///
    /// # Panics
    ///
    /// If a task panicked, with its panic, or was aborted; the tasks left
    /// are aborted then.
    pub async fn join_all(mut self) -> Vec<T> {
        let mut outputs = Vec::with_capacity(self.len());
        while let Some(result) = self.join_next().await {
            match result {
                Ok(output) => outputs.push(output),
                Err(err) if err.is_panic() => panic::resume_unwind(err.into_panic()),
                Err(err) => panic!("{err}"),
            }
        }
        outputs
    }
}

impl<T: 'static> Default for JoinSet<T> {
    fn default() -> Self {
        Self::new()
    }
}
impl<T: Send + 'static> JoinSet<T> {
    /// Runs `f` on a thread of the process's blocking pool
    /// ([`spawn_blocking`](crate::spawn_blocking)), as a task of the set: its
    /// output comes from [`join_next`](JoinSet::join_next) as a task's does,
    /// and a panic in it as the task's panic. Aborting the task aborts the
    /// job when the scheduler next reaches the task and drops its future: a
    /// job no thread has taken by then does not run, and one that has
    /// started runs to its end, its output dropped; either way the set
    /// reports the task cancelled.
    ///
    /// # Panics
    ///
    /// If the thread is not inside [`Runtime::block_on`](crate::Runtime).
    pub fn spawn_blocking<F>(&mut self, f: F) -> AbortHandle
    where
        F: FnOnce() -> T + Send + 'static,
    {
        self.spawn_blocking_on(blocking::global(), f)
    }

    /// Runs `f` on a thread of `pool`, as a task of the set; see
    /// [`spawn_blocking`](JoinSet::spawn_blocking).
    ///
    /// # Panics
    ///
    /// If the thread is not inside [`Runtime::block_on`](crate::Runtime).
    pub fn spawn_blocking_on<F>(&mut self, pool: &Pool, f: F) -> AbortHandle
    where
        F: FnOnce() -> T + Send + 'static,
    {
        self.spawn(Blocking {
            handle: pool.spawn(f),
            done: false,
        })
    }

    /// Spawns `future` onto core `index` of the thread's run, as a task of
    /// the set; see [`spawn_remote`](JoinSet::spawn_remote).
    ///
    /// # Panics
    ///
    /// Outside [`Cores::run`](crate::Cores::run), or if the run has no core
    /// `index`.
    pub fn spawn_on<F>(&mut self, index: usize, future: F) -> RemoteAbortHandle
    where
        F: Future<Output = T> + Send + 'static,
    {
        self.spawn_remote(&handle_of(index), future)
    }

    /// Spawns `future` onto the runtime `handle` is of, as
    /// [`Handle::spawn`] does, as a task of the set: its output comes from
    /// [`join_next`](JoinSet::join_next) as a task's does, and so does the
    /// error of its panicking or being cancelled there. The handle given
    /// aborts it from whichever thread, as does aborting the set's tasks,
    /// which the set then reports cancelled as the other runtime drops the
    /// future.
    ///
    /// # Panics
    ///
    /// If the thread is not inside [`Runtime::block_on`](crate::Runtime).
    pub fn spawn_remote<F>(&mut self, handle: &Handle, future: F) -> RemoteAbortHandle
    where
        F: Future<Output = T> + Send + 'static,
    {
        let task = handle.spawn(future);
        let abort = task.abort_handle();
        let index = self.free.pop().unwrap_or(self.handles.len());
        let handle = crate::spawn(ReportRemote {
            task,
            inner: Rc::clone(&self.inner),
            index,
            reported: false,
        });
        if index == self.handles.len() {
            self.handles.push(Some(handle));
        } else {
            self.handles[index] = Some(handle);
        }
        abort
    }
}

/// The set's task for one spawned onto another core: awaits the remote
/// handle, and reports what it gives, the output or the error the task
/// ended with there. Dropped before, as the set aborts its tasks, it aborts
/// the remote task and notes that the task ended.
struct ReportRemote<T> {
    task: RemoteJoinHandle<T>,
    inner: Rc<Inner<T>>,
    /// The task's index in the set's slab.
    index: usize,
    /// Set once the result has been pushed.
    reported: bool,
}

impl<T> Future for ReportRemote<T> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let result = ready!(Pin::new(&mut self.task).poll(cx));
        self.reported = true;
        let finished = match result {
            Ok(output) => Finished::Output(self.index, output),
            Err(err) => Finished::Failed(self.index, err),
        };
        self.inner.push(finished);
        Poll::Ready(())
    }
}

impl<T> Drop for ReportRemote<T> {
    fn drop(&mut self) {
        if !self.reported {
            self.task.abort();
            self.inner.push(Finished::Ended(self.index));
        }
    }
}

/// A blocking job as a task of a set: the job's output, with a panic in the
/// closure raised again as the task's, so that the set reports it as it
/// reports a task's. Dropped before the job is done, as the task is aborted,
/// it aborts the job, which then does not run if no thread has taken it.
struct Blocking<T> {
    handle: BlockingHandle<T>,
    done: bool,
}

impl<T> Future for Blocking<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let result = ready!(Pin::new(&mut self.handle).poll(cx));
        self.done = true;
        match result {
            Ok(output) => Poll::Ready(output),
            Err(err) if err.is_panic() => panic::resume_unwind(err.into_panic()),
            Err(_) => unreachable!("the job is aborted only as its task is dropped"),
        }
    }
}

impl<T> Drop for Blocking<T> {
    fn drop(&mut self) {
        if !self.done {
            self.handle.abort();
        }
    }
}

impl<T> Drop for JoinSet<T> {
    fn drop(&mut self) {
        // The reports of the tasks aborted go nowhere, and the handles,
        // dropped after this, detach them. The outputs nobody took go now,
        // rather than with the last task's reference to the queue, and
        // outside the borrow, as an output's drop can run any code.
        self.inner.detached.set(true);
        let finished = mem::take(&mut *self.inner.finished.borrow_mut());
        drop(finished);
        for handle in self.handles.iter().flatten() {
            handle.abort();
        }
    }
}

impl<T> fmt::Debug for JoinSet<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinSet")
            .field("len", &(self.handles.len() - self.free.len()))
            .finish()
    }
}
