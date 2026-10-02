//! Tasks: a spawned future, its output, and the handle that awaits it.
//!
//! A task is one allocation, referred to in two ways. References from its
//! own thread, which the scheduler's table, the run queue, the join handle
//! and the driver's registrations hold, are counted in a plain cell: taking
//! and dropping one costs no atomic operation, and nearly every reference a
//! task is held by is one of these. References that may be anywhere, which
//! the wakers and the remote queue hold, are counted atomically, and one of
//! them, the share, stands for all the local references while there are
//! any. The task is freed when the atomic count reaches zero, by which time
//! its future and output are gone: the scheduler drops the future on the
//! task's thread, and the join handle its output, both before they let go of
//! their local references.

use std::any::Any;
use std::cell::{Cell, RefCell};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::mem::{self, ManuallyDrop};
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::ptr::{self, NonNull};
use std::sync::Arc;
use std::sync::atomic::{self, AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker, ready};

use ruio_driver::LocalWaker;

use crate::scheduler::{Scheduler, Shared};
use crate::{context, coop};

/// A spawned future and, once it finishes, its output. The header comes
/// first, so that a pointer to the task is a pointer to its header, through
/// which everything that does not know the future's type reaches it.
#[repr(C)]
pub(crate) struct Task<F: Future> {
    header: Header,
    // The fields below are only used on the thread that spawned the task.
    stage: RefCell<Stage<F>>,
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

/// What every task begins with, whatever its future: what the scheduler,
/// the wakers and the handle reach without knowing the future's type, and
/// the vtable through which they reach the rest.
pub(crate) struct Header {
    vtable: &'static Vtable,
    /// The shared part of the scheduler that owns the task.
    shared: Arc<Shared>,
    /// References that may be anywhere: wakers, whichever thread holds them,
    /// the entries of the remote queue, and the share, one reference standing
    /// for every local one while there is any.
    strong: AtomicUsize,
    /// References on the task's own thread: the scheduler's table entry, the
    /// run queue's, the join handle's and the driver's. A plain cell, as all
    /// of them come and go on that thread.
    local: Cell<usize>,
    /// Whether another thread has queued the task on its scheduler's remote
    /// queue since it last ran, so that waking it again from elsewhere does
    /// not queue it twice.
    woken: AtomicBool,
    /// Whether the task is in its scheduler's run queue, so that waking it
    /// again does not queue it twice.
    queued: Cell<bool>,
    /// The task's place in its scheduler's list of tasks.
    index: Cell<usize>,
}

/// What a header reaches of its task, knowing the future's type. Each
/// function takes the header of a live task of that type.
struct Vtable {
    run: unsafe fn(*const Header, &Scheduler),
    cancel: unsafe fn(*const Header),
    is_finished: unsafe fn(*const Header) -> bool,
    abort: unsafe fn(*const Header),
    detach: unsafe fn(*const Header),
    dealloc: unsafe fn(*const Header),
}

impl Header {
    /// Marks the task queued to run, unless it is already: whether the caller
    /// is to queue it. Only on the task's thread.
    fn claim(&self) -> bool {
        !self.queued.replace(true)
    }
}

/// A reference to a task from its own thread, counted without atomic
/// operations: what the scheduler, the run queue, the join handle and the
/// driver hold.
pub(crate) struct LocalRef {
    header: NonNull<Header>,
    /// Keeps the reference on its thread.
    _thread: PhantomData<*const ()>,
}

impl LocalRef {
    /// Takes over one of the local references `header` counts.
    ///
    /// # Safety
    ///
    /// `header` must be the header of a live task on this thread, counting a
    /// local reference that nothing else lets go of.
    unsafe fn from_raw(header: *const Header) -> Self {
        Self {
            // SAFETY: a live task's header is not null.
            header: unsafe { NonNull::new_unchecked(header.cast_mut()) },
            _thread: PhantomData,
        }
    }

    /// Another local reference to the task at `header`. The first local
    /// reference, when the task has none, takes a share of the atomic count.
    ///
    /// # Safety
    ///
    /// `header` must be the header of a live task on this thread.
    unsafe fn retain(header: *const Header) -> Self {
        // SAFETY: the task is alive, and this is its thread.
        let task = unsafe { &*header };
        let local = task.local.get();
        if local == 0 {
            task.strong.fetch_add(1, Ordering::Relaxed);
        }
        task.local.set(local + 1);
        // SAFETY: the reference just counted.
        unsafe { Self::from_raw(header) }
    }

    /// Makes a reference from anywhere a local one: the first local
    /// reference, when the task has none, takes it over as the share, and a
    /// later one lets it go.
    pub(crate) fn adopt(remote: RemoteRef) -> Self {
        let header = remote.header.as_ptr();
        // SAFETY: the remote reference keeps the task alive, and this is its
        // thread.
        let task = unsafe { &*header };
        let local = task.local.get();
        task.local.set(local + 1);
        if local == 0 {
            mem::forget(remote);
        } else {
            drop(remote);
        }
        // SAFETY: the reference just counted.
        unsafe { Self::from_raw(header) }
    }

    fn header(&self) -> &Header {
        // SAFETY: the reference keeps the task alive.
        unsafe { self.header.as_ref() }
    }

    /// The task's header, as wakers and the scheduler name the task.
    pub(crate) fn as_ptr(&self) -> *const Header {
        self.header.as_ptr()
    }

    /// Polls the task, finishing it if its future completes or it was
    /// aborted.
    pub(crate) fn run(&self, scheduler: &Scheduler) {
        // SAFETY: the reference keeps the task alive, and the vtable is the
        // task's own.
        unsafe { (self.header().vtable.run)(self.as_ptr(), scheduler) }
    }

    /// Drops the future of an unfinished task, which then reports that it
    /// was cancelled.
    pub(crate) fn cancel(&self) {
        // SAFETY: as for `run`.
        unsafe { (self.header().vtable.cancel)(self.as_ptr()) }
    }
}

impl Clone for LocalRef {
    fn clone(&self) -> Self {
        // SAFETY: the reference keeps the task alive, on this thread.
        unsafe { Self::retain(self.as_ptr()) }
    }
}

impl Drop for LocalRef {
    fn drop(&mut self) {
        let task = self.header();
        let left = task.local.get() - 1;
        task.local.set(left);
        if left == 0 {
            // The last local reference lets the share go.
            // SAFETY: the share is one of the atomic count.
            unsafe { release(self.as_ptr()) };
        }
    }
}

/// A reference to a task from any thread, of its atomic count: what the
/// wakers and the remote queue hold.
pub(crate) struct RemoteRef {
    header: NonNull<Header>,
}

// SAFETY: a remote reference reaches only what any thread may of the task:
// the atomic counts, the vtable and the owning scheduler's shared part. The
// cells are touched on the task's thread alone, by local references and by
// the scheduler when it takes the remote queue. The task is freed on
// whichever thread lets go of the last atomic reference, by which time its
// future and output are gone, so nothing that is not `Send` goes with it.
unsafe impl Send for RemoteRef {}
// SAFETY: as for `Send`.
unsafe impl Sync for RemoteRef {}

impl RemoteRef {
    /// Takes over one of the atomic references `header` counts.
    ///
    /// # Safety
    ///
    /// `header` must be the header of a live task, counting an atomic
    /// reference that nothing else lets go of.
    unsafe fn from_raw(header: *const Header) -> Self {
        Self {
            // SAFETY: a live task's header is not null.
            header: unsafe { NonNull::new_unchecked(header.cast_mut()) },
        }
    }

    /// Another atomic reference to the task at `header`.
    ///
    /// # Safety
    ///
    /// `header` must be the header of a live task.
    unsafe fn retain(header: *const Header) -> Self {
        // SAFETY: the task is alive.
        unsafe { (*header).strong.fetch_add(1, Ordering::Relaxed) };
        // SAFETY: the reference just counted.
        unsafe { Self::from_raw(header) }
    }

    fn header(&self) -> &Header {
        // SAFETY: the reference keeps the task alive.
        unsafe { self.header.as_ref() }
    }

    /// Marks the task queued to run, unless it is already: whether the caller
    /// is to queue it. Only on the task's thread, as the scheduler takes its
    /// remote queue.
    pub(crate) fn claim(&self) -> bool {
        self.header().claim()
    }

    /// Queues the task to run, consuming the reference: on its scheduler's
    /// run queue if this is its thread, and on the remote queue otherwise.
    fn wake(self) {
        let header = self.header.as_ptr();
        // The owner's address alone: the reference may be let go of below,
        // which could free the task.
        let owner = Arc::as_ptr(&self.header().shared);
        let mut remote = Some(self);
        let on_owner = context::with_owner(owner, |scheduler| {
            let remote = remote.take().expect("the reference is still held");
            if remote.claim() {
                scheduler.schedule(LocalRef::adopt(remote));
            }
        });
        if on_owner.is_none() {
            let remote = remote.take().expect("the reference is still held");
            debug_assert!(ptr::eq(remote.header.as_ptr(), header));
            remote.wake_remote();
        }
    }

    /// Queues the task on its scheduler's remote queue, unless it is there
    /// already, and wakes the scheduler.
    fn wake_remote(self) {
        if self.header().woken.swap(true, Ordering::SeqCst) {
            return;
        }
        // The shared part is kept alive for the call: the queue, if it has
        // closed, drops the reference, which could be the task's last.
        let shared = Arc::clone(&self.header().shared);
        shared.schedule_remote(self);
    }
}

impl Clone for RemoteRef {
    fn clone(&self) -> Self {
        // SAFETY: the reference keeps the task alive.
        unsafe { Self::retain(self.header.as_ptr()) }
    }
}

impl Drop for RemoteRef {
    fn drop(&mut self) {
        // SAFETY: the reference is one of the atomic count.
        unsafe { release(self.header.as_ptr()) };
    }
}

/// Lets go of one reference of the atomic count, freeing the task after the
/// last.
///
/// # Safety
///
/// `header` must be the header of a live task, counting an atomic reference
/// that nothing else lets go of.
unsafe fn release(header: *const Header) {
    // SAFETY: the task is alive.
    let task = unsafe { &*header };
    if task.strong.fetch_sub(1, Ordering::Release) == 1 {
        atomic::fence(Ordering::Acquire);
        let dealloc = task.vtable.dealloc;
        // SAFETY: the last reference is gone, so nothing reaches the task
        // again; the vtable is the task's own.
        unsafe { dealloc(header) };
    }
}

/// Queues the task at `header` to run, as a wake by reference does: on its
/// scheduler's run queue, by a local reference, if this is its thread, and
/// otherwise on the remote queue, by an atomic one.
///
/// # Safety
///
/// `header` must be the header of a live task.
unsafe fn wake_by_ref(header: *const Header) {
    // SAFETY: the task is alive.
    let task = unsafe { &*header };
    let on_owner = context::with_owner(Arc::as_ptr(&task.shared), |scheduler| {
        if task.claim() {
            // SAFETY: the task is alive, and this is its thread.
            scheduler.schedule(unsafe { LocalRef::retain(header) });
        }
    });
    if on_owner.is_none() {
        // SAFETY: the task is alive.
        unsafe { RemoteRef::retain(header) }.wake_remote();
    }
}

/// The vtable of every task's wakers, whatever the task's future: one, so
/// that the driver can tell a task's waker by it and hold it by a counted
/// reference rather than a clone (see [`LOCAL_WAKER`]). A waker's data is
/// the task's header, and the waker holds one atomic reference, except for
/// the one a run lends its poll, which is never dropped.
static WAKER: RawWakerVTable =
    RawWakerVTable::new(clone_waker, wake_waker, wake_waker_by_ref, drop_waker);

unsafe fn clone_waker(header: *const ()) -> RawWaker {
    // SAFETY: the waker holds a reference, so the task is alive.
    unsafe {
        (*header.cast::<Header>())
            .strong
            .fetch_add(1, Ordering::Relaxed)
    };
    RawWaker::new(header, &WAKER)
}

unsafe fn wake_waker(header: *const ()) {
    // SAFETY: waking consumes the waker, and so its reference.
    unsafe { RemoteRef::from_raw(header.cast()) }.wake();
}

unsafe fn wake_waker_by_ref(header: *const ()) {
    // SAFETY: the waker holds a reference, so the task is alive.
    unsafe { wake_by_ref(header.cast()) };
}

unsafe fn drop_waker(header: *const ()) {
    // SAFETY: dropping the waker releases its reference.
    unsafe { release(header.cast()) };
}

/// How the driver of a task's thread holds the task's wakers: by a local
/// reference. The driver sees only the wakers of tasks of its own thread,
/// since tasks and the operations they poll stay on theirs.
pub(crate) const LOCAL_WAKER: LocalWaker = LocalWaker {
    vtable: &WAKER,
    retain: retain_local,
    release: release_local,
    wake: wake_waker_by_ref,
};

unsafe fn retain_local(header: *const ()) {
    // SAFETY: the waker the driver was given holds a reference, so the task
    // is alive, and the driver is on the task's thread.
    mem::forget(unsafe { LocalRef::retain(header.cast()) });
}

unsafe fn release_local(header: *const ()) {
    // SAFETY: the reference `retain_local` counted, on the task's thread.
    drop(unsafe { LocalRef::from_raw(header.cast()) });
}

impl<F> Task<F>
where
    F: Future + 'static,
    F::Output: 'static,
{
    const VTABLE: Vtable = Vtable {
        run: Self::run,
        cancel: Self::cancel,
        is_finished: Self::is_finished,
        abort: Self::abort,
        detach: Self::detach,
        dealloc: Self::dealloc,
    };

    /// The task whose header `header` is.
    ///
    /// # Safety
    ///
    /// `header` must be the header of a live `Task<F>`.
    unsafe fn get<'a>(header: *const Header) -> &'a Self {
        // SAFETY: the header is the first field of a `#[repr(C)]` task, so
        // its address is the task's.
        unsafe { &*header.cast::<Self>() }
    }

    /// Creates a task, queued to run, for `scheduler` to own.
    pub(crate) fn spawn(future: F, scheduler: &Scheduler) -> JoinHandle<F::Output> {
        let task = Box::new(Self {
            header: Header {
                vtable: &Self::VTABLE,
                shared: Arc::clone(scheduler.shared()),
                strong: AtomicUsize::new(1),
                local: Cell::new(1),
                woken: AtomicBool::new(false),
                queued: Cell::new(true),
                index: Cell::new(0),
            },
            stage: RefCell::new(Stage::Running(future)),
            join_waker: Cell::new(None),
            detached: Cell::new(false),
            aborted: Cell::new(false),
        });
        let header: *const Header = &raw const Box::leak(task).header;
        // SAFETY: the new task counts one local reference, taken here, and
        // one atomic one, its share.
        let task = unsafe { LocalRef::from_raw(header) };
        let index = scheduler.adopt(task.clone());
        task.header().index.set(index);
        scheduler.schedule_new(task.clone());
        JoinHandle {
            task,
            poll_join: Self::poll_join,
        }
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

    unsafe fn run(header: *const Header, scheduler: &Scheduler) {
        // SAFETY: the vtable's contract.
        let this = unsafe { Self::get(header) };
        // Cleared before polling, so a wake during the poll queues it again.
        // A remote wake's flag is cleared only if set, with the same order
        // as the wake's own swap, so that the poll sees what it woke for.
        this.header.queued.set(false);
        if this.header.woken.load(Ordering::SeqCst) {
            this.header.woken.store(false, Ordering::SeqCst);
        }
        if this.aborted.get() {
            if matches!(*this.stage.borrow(), Stage::Running(_)) {
                this.complete(Err(JoinError::cancelled()));
                scheduler.release(this.header.index.get());
            }
            return;
        }
        // A waker borrowing the run's reference, which outlives the poll: it
        // takes a reference of its own only if cloned.
        let raw = RawWaker::new(header.cast(), &WAKER);
        // SAFETY: the vtable's functions treat the pointer as a reference, as
        // `RawWakerVTable` asks, and this one lives until the end of the
        // poll; never dropped, the waker releases no reference it does not
        // hold.
        let waker = ManuallyDrop::new(unsafe { Waker::from_raw(raw) });
        let mut cx = Context::from_waker(&waker);
        let poll = {
            let mut stage = this.stage.borrow_mut();
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
        this.complete(result);
        scheduler.release(this.header.index.get());
    }

    unsafe fn cancel(header: *const Header) {
        // SAFETY: the vtable's contract.
        let this = unsafe { Self::get(header) };
        if matches!(*this.stage.borrow(), Stage::Running(_)) {
            this.complete(Err(JoinError::cancelled()));
        }
    }

    unsafe fn is_finished(header: *const Header) -> bool {
        // SAFETY: the vtable's contract.
        let this = unsafe { Self::get(header) };
        !matches!(*this.stage.borrow(), Stage::Running(_))
    }

    unsafe fn abort(header: *const Header) {
        // SAFETY: the vtable's contract.
        let this = unsafe { Self::get(header) };
        if matches!(*this.stage.borrow(), Stage::Running(_)) {
            this.aborted.set(true);
            // SAFETY: the task is alive.
            unsafe { wake_by_ref(header) };
        }
    }

    unsafe fn detach(header: *const Header) {
        // SAFETY: the vtable's contract.
        let this = unsafe { Self::get(header) };
        this.detached.set(true);
        drop(this.join_waker.take());
        // A finished task's output belongs to the handle, and is dropped
        // here, on the task's thread.
        let output = {
            let mut stage = this.stage.borrow_mut();
            match *stage {
                Stage::Finished(_) => Some(mem::replace(&mut *stage, Stage::Consumed)),
                _ => None,
            }
        };
        drop(output);
    }

    unsafe fn dealloc(header: *const Header) {
        // SAFETY: the vtable's contract, and the header is the first field
        // of the `#[repr(C)]` task the box held.
        drop(unsafe { Box::from_raw(header.cast_mut().cast::<Self>()) });
    }

    unsafe fn poll_join(
        header: *const Header,
        cx: &mut Context<'_>,
    ) -> Poll<Result<F::Output, JoinError>> {
        // SAFETY: the vtable's contract.
        let this = unsafe { Self::get(header) };
        let proceed = ready!(coop::poll_proceed(cx));
        let mut stage = this.stage.borrow_mut();
        match &*stage {
            Stage::Running(_) => {
                let current = this.join_waker.take();
                let waker = match current {
                    Some(current) if current.will_wake(cx.waker()) => current,
                    _ => cx.waker().clone(),
                };
                this.join_waker.set(Some(waker));
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
    task: LocalRef,
    /// Polls the task for its output as `T`, which its header's vtable does
    /// not know.
    poll_join: unsafe fn(*const Header, &mut Context<'_>) -> Poll<Result<T, JoinError>>,
}

impl<T> JoinHandle<T> {
    /// Cancels the task. Its future is dropped the next time the scheduler
    /// reaches it, and awaiting the handle then returns a cancelled
    /// [`JoinError`]. A finished task is left as it is.
    pub fn abort(&self) {
        // SAFETY: the handle's reference keeps the task alive, and the
        // vtable is the task's own.
        unsafe { (self.task.header().vtable.abort)(self.task.as_ptr()) }
    }

    /// Whether the task has finished, by completing, panicking or being
    /// cancelled.
    pub fn is_finished(&self) -> bool {
        // SAFETY: as for `abort`.
        unsafe { (self.task.header().vtable.is_finished)(self.task.as_ptr()) }
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: the handle's reference keeps the task alive, and the
        // function was made for the task's future, whose output is `T`.
        unsafe { (self.poll_join)(self.task.as_ptr(), cx) }
    }
}

impl<T> Drop for JoinHandle<T> {
    fn drop(&mut self) {
        // SAFETY: as for `abort`.
        unsafe { (self.task.header().vtable.detach)(self.task.as_ptr()) }
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
