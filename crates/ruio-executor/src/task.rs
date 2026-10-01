//! Tasks: a spawned future, its output, and the handle that awaits it.

use std::any::Any;
use std::cell::{Cell, RefCell};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::mem::{self, ManuallyDrop};
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker, ready};

use crate::scheduler::{Runnable, Scheduler, Shared, TaskRef};
use crate::{context, coop};

/// A spawned future and, once it finishes, its output: one allocation, shared
/// by the scheduler, the task's wakers and its [`JoinHandle`].
pub(crate) struct Task<F: Future> {
    shared: Arc<Shared>,
    /// Whether another thread has queued the task on its scheduler's remote
    /// queue since it last ran, so that waking it again from elsewhere does
    /// not queue it twice.
    woken: AtomicBool,
    // The fields below are only used on the thread that spawned the task.
    /// Whether the task is in its scheduler's run queue, so that waking it
    /// again does not queue it twice. A plain cell: most wakes come from the
    /// task's own thread, and need no atomic operation.
    queued: Cell<bool>,
    stage: RefCell<Stage<F>>,
    /// The task's place in its scheduler's list of tasks.
    index: Cell<usize>,
    join_waker: Cell<Option<Waker>>,
    /// Set when the `JoinHandle` is dropped: the output has no one to go to.
    detached: Cell<bool>,
    /// Set by `JoinHandle::abort`.
    aborted: Cell<bool>,
}

enum Stage<F: Future> {
    Running(F),
    Finished(Result<F::Output, JoinError>),
    /// The output was taken, or nothing will ever take it.
    Consumed,
}

// SAFETY: other threads only reach `shared` and `woken`, which are Send and
// Sync, through the task's wakers. The future, the output and the `Cell`s
// are only used on the thread that spawned the task: by its scheduler, which
// runs there, and by its `JoinHandle`, which cannot leave it. The scheduler
// keeps a reference until the future is dropped, and the `JoinHandle` drops
// the output, both on that thread, so a reference released last on another
// thread drops nothing that is not Send.
unsafe impl<F: Future> Send for Task<F> {}
// SAFETY: as for `Send`.
unsafe impl<F: Future> Sync for Task<F> {}

impl<F> Task<F>
where
    F: Future + 'static,
    F::Output: 'static,
{
    /// Creates a task, queued to run, for `scheduler` to own.
    pub(crate) fn spawn(future: F, scheduler: &Scheduler) -> JoinHandle<F::Output> {
        let task = Arc::new(Self {
            shared: Arc::clone(scheduler.shared()),
            woken: AtomicBool::new(false),
            queued: Cell::new(true),
            stage: RefCell::new(Stage::Running(future)),
            index: Cell::new(0),
            join_waker: Cell::new(None),
            detached: Cell::new(false),
            aborted: Cell::new(false),
        });
        let index = scheduler.adopt(Arc::clone(&task) as TaskRef);
        task.index.set(index);
        scheduler.schedule_new(Arc::clone(&task) as TaskRef);
        JoinHandle { task }
    }

    /// Stores the task's result and wakes whoever awaits it.
    fn complete(&self, result: Result<F::Output, JoinError>) {
        // Replacing the stage drops the future in place, and dropping runs
        // code of the task's own, so a panic there is contained.
        let _ = panic::catch_unwind(AssertUnwindSafe(|| {
            let mut stage = self.stage.borrow_mut();
            *stage = Stage::Consumed;
            if !self.detached.get() {
                *stage = Stage::Finished(result);
            }
        }));
        if let Some(waker) = self.join_waker.take() {
            waker.wake();
        }
    }

    /// Queues the task to run: on its scheduler's run queue from its own
    /// thread, which claims it there, or on its remote queue from another.
    fn schedule(self: Arc<Self>) {
        let owner = Arc::as_ptr(&self.shared);
        if let Err(task) = context::schedule_local(owner, self) {
            task.wake_remote();
        }
    }

    /// The task's wakers, made by hand rather than from `Wake` so that a
    /// poll can lend one without taking a reference; see `run`. Each
    /// function gets a pointer from an `Arc<Self>`, whose reference the
    /// waker holds, except that the one `run` lends is never dropped.
    const WAKER: RawWakerVTable = RawWakerVTable::new(
        Self::clone_waker,
        Self::wake_waker,
        Self::wake_waker_by_ref,
        Self::drop_waker,
    );

    unsafe fn clone_waker(task: *const ()) -> RawWaker {
        // SAFETY: the waker holds a reference, so the task is alive.
        unsafe { Arc::increment_strong_count(task.cast::<Self>()) };
        RawWaker::new(task, &Self::WAKER)
    }

    unsafe fn wake_waker(task: *const ()) {
        // SAFETY: waking consumes the waker, and so its reference.
        let task = unsafe { Arc::from_raw(task.cast::<Self>()) };
        task.schedule();
    }

    unsafe fn wake_waker_by_ref(task: *const ()) {
        // SAFETY: the waker holds a reference, which stays with it.
        let task = ManuallyDrop::new(unsafe { Arc::from_raw(task.cast::<Self>()) });
        Arc::clone(&task).schedule();
    }

    unsafe fn drop_waker(task: *const ()) {
        // SAFETY: dropping the waker releases its reference.
        drop(unsafe { Arc::from_raw(task.cast::<Self>()) });
    }
}

impl<F> Runnable for Task<F>
where
    F: Future + 'static,
    F::Output: 'static,
{
    fn run(self: Arc<Self>, scheduler: &Scheduler) {
        // Cleared before polling, so a wake during the poll queues it again.
        // A remote wake's flag is cleared only if set, with the same order
        // as the wake's own swap, so that the poll sees what it woke for.
        self.queued.set(false);
        if self.woken.load(Ordering::SeqCst) {
            self.woken.store(false, Ordering::SeqCst);
        }
        if self.aborted.get() {
            if matches!(*self.stage.borrow(), Stage::Running(_)) {
                self.complete(Err(JoinError::cancelled()));
                scheduler.release(self.index.get());
            }
            return;
        }
        // A waker borrowing the scheduler's reference, which outlives the
        // poll: it takes a reference of its own only if cloned.
        let raw = RawWaker::new(Arc::as_ptr(&self).cast(), &Self::WAKER);
        // SAFETY: the vtable's functions treat the pointer as a reference, as
        // `RawWakerVTable` asks, and this one lives until the end of the
        // poll; never dropped, the waker releases no reference it does not
        // hold.
        let waker = ManuallyDrop::new(unsafe { Waker::from_raw(raw) });
        let mut cx = Context::from_waker(&waker);
        let poll = {
            let mut stage = self.stage.borrow_mut();
            let Stage::Running(future) = &mut *stage else {
                return;
            };
            // SAFETY: the future stays where it is, in the task's allocation,
            // until it is dropped in place when the stage is replaced.
            let future = unsafe { Pin::new_unchecked(future) };
            panic::catch_unwind(AssertUnwindSafe(|| coop::budgeted(|| future.poll(&mut cx))))
        };
        let result = match poll {
            Ok(Poll::Pending) => return,
            Ok(Poll::Ready(output)) => Ok(output),
            Err(payload) => Err(JoinError::panicked(payload)),
        };
        self.complete(result);
        scheduler.release(self.index.get());
    }

    fn cancel(&self) {
        if matches!(*self.stage.borrow(), Stage::Running(_)) {
            self.complete(Err(JoinError::cancelled()));
        }
    }

    fn claim(&self) -> bool {
        !self.queued.replace(true)
    }

    fn wake_remote(self: Arc<Self>) {
        if self.woken.swap(true, Ordering::SeqCst) {
            return;
        }
        Arc::clone(&self.shared).schedule_remote(self);
    }
}

/// What a [`JoinHandle`] needs from its task, whatever the future's type.
trait Join {
    type Output;

    fn poll_join(&self, cx: &mut Context<'_>) -> Poll<Result<Self::Output, JoinError>>;

    fn is_finished(&self) -> bool;

    fn abort(self: Arc<Self>);

    fn detach(&self);
}

impl<F> Join for Task<F>
where
    F: Future + 'static,
    F::Output: 'static,
{
    type Output = F::Output;

    fn poll_join(&self, cx: &mut Context<'_>) -> Poll<Result<F::Output, JoinError>> {
        let proceed = ready!(coop::poll_proceed(cx));
        let mut stage = self.stage.borrow_mut();
        match &*stage {
            Stage::Running(_) => {
                let current = self.join_waker.take();
                let waker = match current {
                    Some(current) if current.will_wake(cx.waker()) => current,
                    _ => cx.waker().clone(),
                };
                self.join_waker.set(Some(waker));
                Poll::Pending
            }
            Stage::Finished(_) => match mem::replace(&mut *stage, Stage::Consumed) {
                Stage::Finished(result) => {
                    proceed.made_progress();
                    Poll::Ready(result)
                }
                _ => unreachable!(),
            },
            Stage::Consumed => panic!("`JoinHandle` polled after it completed"),
        }
    }

    fn is_finished(&self) -> bool {
        !matches!(*self.stage.borrow(), Stage::Running(_))
    }

    fn abort(self: Arc<Self>) {
        if matches!(*self.stage.borrow(), Stage::Running(_)) {
            self.aborted.set(true);
            self.schedule();
        }
    }

    fn detach(&self) {
        self.detached.set(true);
        drop(self.join_waker.take());
        // A finished task's output belongs to the handle, and is dropped
        // here, on the task's thread.
        let output = {
            let mut stage = self.stage.borrow_mut();
            match *stage {
                Stage::Finished(_) => Some(mem::replace(&mut *stage, Stage::Consumed)),
                _ => None,
            }
        };
        drop(output);
    }
}

/// Awaits a spawned task's output.
///
/// Awaiting the handle returns the output, or a [`JoinError`] if the task
/// panicked, was aborted, or its runtime shut down first. Dropping the handle
/// detaches the task, which keeps running. The handle stays on the thread
/// that spawned the task.
///
/// ```compile_fail
/// fn assert_send<T: Send>() {}
/// assert_send::<ruio_executor::JoinHandle<()>>();
/// ```
#[must_use = "dropping a `JoinHandle` detaches the task"]
pub struct JoinHandle<T> {
    task: Arc<dyn Join<Output = T>>,
}

impl<T> JoinHandle<T> {
    /// Cancels the task. Its future is dropped the next time the scheduler
    /// reaches it, and awaiting the handle then returns a cancelled
    /// [`JoinError`]. A finished task is left as it is.
    pub fn abort(&self) {
        Arc::clone(&self.task).abort();
    }

    /// Whether the task has finished, by completing, panicking or being
    /// cancelled.
    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.task.poll_join(cx)
    }
}

impl<T> Drop for JoinHandle<T> {
    fn drop(&mut self) {
        self.task.detach();
    }
}

impl<T> fmt::Debug for JoinHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinHandle")
            .field("finished", &self.is_finished())
            .finish_non_exhaustive()
    }
}

/// Why a task did not produce its output.
pub struct JoinError {
    repr: Repr,
}

enum Repr {
    Cancelled,
    Panicked(Payload),
}

/// A panic's payload, which only ever leaves by value.
struct Payload(Box<dyn Any + Send>);

// SAFETY: the payload is reachable by reference only through
// `Payload::message`, which returns a reference to a `&'static str` or a
// `String`, both Sync; the rest of it only leaves by value, through
// `JoinError::into_panic`. Sharing a `Payload` never shares anything that is
// not Sync.
unsafe impl Sync for Payload {}

impl Payload {
    fn message(&self) -> Option<&str> {
        let payload = &*self.0;
        if let Some(message) = payload.downcast_ref::<&'static str>() {
            Some(message)
        } else {
            payload.downcast_ref::<String>().map(String::as_str)
        }
    }
}

impl JoinError {
    pub(crate) fn cancelled() -> Self {
        Self {
            repr: Repr::Cancelled,
        }
    }

    fn panicked(payload: Box<dyn Any + Send>) -> Self {
        Self {
            repr: Repr::Panicked(Payload(payload)),
        }
    }

    /// Whether the task was aborted, or its runtime shut down before it
    /// finished.
    pub fn is_cancelled(&self) -> bool {
        matches!(self.repr, Repr::Cancelled)
    }

    /// Whether the task panicked.
    pub fn is_panic(&self) -> bool {
        matches!(self.repr, Repr::Panicked(_))
    }

    /// The task's panic payload, for [`std::panic::resume_unwind`].
    ///
    /// # Panics
    ///
    /// If the task did not panic.
    pub fn into_panic(self) -> Box<dyn Any + Send> {
        self.try_into_panic()
            .expect("`JoinError::into_panic` on a task that did not panic")
    }

    /// The task's panic payload, or the error back if the task did not
    /// panic.
    ///
    /// # Errors
    ///
    /// If the task was cancelled.
    pub fn try_into_panic(self) -> Result<Box<dyn Any + Send>, JoinError> {
        match self.repr {
            Repr::Panicked(payload) => Ok(payload.0),
            Repr::Cancelled => Err(self),
        }
    }
}

impl fmt::Display for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.repr {
            Repr::Cancelled => f.write_str("task was cancelled"),
            Repr::Panicked(payload) => match payload.message() {
                Some(message) => write!(f, "task panicked: {message}"),
                None => f.write_str("task panicked"),
            },
        }
    }
}

impl fmt::Debug for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.repr {
            Repr::Cancelled => f.write_str("JoinError::Cancelled"),
            Repr::Panicked(payload) => f
                .debug_tuple("JoinError::Panicked")
                .field(&payload.message().unwrap_or("..."))
                .finish(),
        }
    }
}

impl Error for JoinError {}
