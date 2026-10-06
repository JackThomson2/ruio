//! Tasks spawned from other threads.
//!
//! A task spawned through a [`Handle`](crate::Handle) is made whole on the
//! thread that spawns it: its allocation, its header, and what makes its
//! future. It goes to its runtime's remote queue as one atomic reference,
//! which the scheduler takes in as it takes the queue: into its table and
//! its run queue, from where the task is one of the scheduler's own. The
//! first run makes the future, on the task's thread, so that the future need
//! not be `Send`, only what makes it.
//!
//! The output goes back the way a oneshot channel's value does, in the task
//! itself: in one state word the task finishes, and the handle registers its
//! waker and goes, each with one read-modify-write, and whichever comes
//! second does what is left to do. So a spawn from another thread costs the
//! task's allocation and nothing else: no channel, no boxed closure, and no
//! task beside it to relay the output.

use std::alloc::{self, Layout};
use std::cell::{Cell, UnsafeCell};
use std::fmt;
use std::future::Future;
use std::mem::{self, ManuallyDrop};
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::ptr::{self, NonNull};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::task::{Context, Poll, RawWaker, Waker, ready};

use super::{Header, Hold, JoinError, LocalRef, RemoteRef, UNADOPTED, Vtable, WAKER, wake_by_ref};
use crate::scheduler::{Scheduler, Shared};
use crate::{context, coop};

/// The task has finished: the stage holds its output, or nothing, and is
/// the handle's from then on.
const COMPLETE: u8 = 1;
/// The handle's waker is registered, to be woken as the task finishes.
const RX_TASK: u8 = 2;
/// The handle is gone.
const RX_CLOSED: u8 = 4;

/// A task spawned from another thread; see the module documentation. The
/// header comes first, as in a [`Task`](super::Task), so that a pointer to
/// the task is a pointer to its header.
#[repr(C)]
pub(crate) struct RemoteTask<M, F: Future> {
    header: Header,
    /// Set by the handle's abort, from whichever thread, before the wake
    /// that has the task's thread read it.
    aborted: AtomicBool,
    /// Where the task and its handle meet; see [`COMPLETE`] and the flags
    /// after it.
    state: AtomicU8,
    /// The task awaiting the handle: written by the handle only while
    /// `RX_TASK` is clear and `COMPLETE` is not set, read by the task's
    /// thread once it has set `COMPLETE` and found `RX_TASK`.
    waker: UnsafeCell<Option<Waker>>,
    /// The task's thread's until `COMPLETE` is set, and the handle's after,
    /// unless `RX_CLOSED` was set first, which leaves it the task's.
    stage: UnsafeCell<RemoteStage<M, F>>,
}

enum RemoteStage<M, F: Future> {
    /// What makes the future, until the first run.
    New(M),
    Running(F),
    Finished(Result<F::Output, JoinError>),
    /// The output was taken, or nothing will ever take it.
    Consumed,
}

impl<M, F> RemoteTask<M, F>
where
    M: FnOnce() -> F + Send + 'static,
    F: Future + 'static,
    F::Output: Send + 'static,
{
    /// The task's vtable. Unlike a task of the runtime's own thread, a task
    /// spawned from elsewhere has its abort, its detach and its join polled
    /// from whichever thread holds its handle; `run` and `dealloc` are as
    /// for any task, and `cancel` is for the thread that alone reaches an
    /// unfinished task: the runtime's, or the spawner's for a spawn the
    /// runtime refused.
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
    /// `header` must be the header of a live `RemoteTask<M, F>`.
    unsafe fn get<'a>(header: *const Header) -> &'a Self {
        // SAFETY: the header is the first field of a `#[repr(C)]` task, so
        // its address is the task's.
        unsafe { &*header.cast::<Self>() }
    }

    /// Makes the task of the future `make` makes, here, on whichever thread,
    /// and queues it for the runtime whose shared part `shared` is to take
    /// in at its next turn. A runtime that has shut down leaves it
    /// cancelled, which its handle reports.
    pub(crate) fn spawn(make: M, shared: &Arc<Shared>) -> RemoteJoinHandle<F::Output> {
        let layout = Layout::new::<Self>();
        // The runtime running on this thread, if any, may have a freed task's
        // memory of the layout to spare: the handles of the tasks it spawns
        // elsewhere are mostly dropped here, where their tasks are freed.
        let block = context::with_current(|scheduler| scheduler.alloc_task(layout))
            .unwrap_or_else(|| {
                // SAFETY: a task has a size, its header's at the least.
                let block = unsafe { alloc::alloc(layout) };
                NonNull::new(block).unwrap_or_else(|| alloc::handle_alloc_error(layout))
            })
            .cast::<Self>();
        // SAFETY: the `Arc` keeps the part alive, and the pointer is the one
        // it gives; the task lets the reference go as it is freed.
        unsafe { Arc::increment_strong_count(Arc::as_ptr(shared)) };
        // SAFETY: an `Arc`'s pointer is not null.
        let owner = unsafe { NonNull::new_unchecked(Arc::as_ptr(shared).cast_mut()) };
        let task = Self {
            header: Header {
                vtable: &Self::VTABLE,
                shared: owner,
                hold: Cell::new(Hold::Atomic),
                // The handle's reference and the remote queue's.
                strong: AtomicUsize::new(2),
                local: Cell::new(0),
                // Set, so that a wake from elsewhere queues the task nowhere
                // until its first run, which clears it: the scheduler takes
                // it in from the spawns, and runs it, whatever woke it.
                woken: AtomicBool::new(true),
                queued: Cell::new(false),
                index: Cell::new(UNADOPTED),
            },
            aborted: AtomicBool::new(false),
            state: AtomicU8::new(0),
            waker: UnsafeCell::new(None),
            stage: UnsafeCell::new(RemoteStage::New(make)),
        };
        // SAFETY: the block has the task's layout, and nothing else uses it.
        unsafe { block.write(task) };
        // The header is the first field of the `#[repr(C)]` task, so its
        // address is the task's.
        let header: *const Header = block.as_ptr().cast_const().cast();
        // SAFETY: the new task counts two atomic references, taken here and
        // below.
        let handle = unsafe { RemoteRef::from_raw(header) };
        // SAFETY: as above.
        let queued = unsafe { RemoteRef::from_raw(header) };
        if let Err(refused) = shared.spawn_remote(queued) {
            // The runtime has shut down, and nothing else will ever reach
            // the task: it is cancelled here, where what makes its future
            // was made, and the handle reports that.
            // SAFETY: the task is alive, and this thread alone reaches it.
            unsafe { Self::cancel(header) };
            drop(refused);
        }
        RemoteJoinHandle {
            task: handle,
            poll_join: Self::poll_join,
        }
    }

    /// Finishes the task with `result`: drops what the stage holds, which
    /// runs code of the task's own, so a panic there is contained, and hands
    /// the result to the handle, waking the task awaiting it, or drops it if
    /// the handle is gone. Only on the thread that holds the unfinished
    /// task, which sets `COMPLETE` here and nowhere else.
    fn complete(&self, result: Result<F::Output, JoinError>) {
        let stage = self.stage.get();
        // SAFETY: `COMPLETE` is not set, so the stage is this thread's. It
        // is written anew below without dropping it again, whatever its
        // drop did.
        let _ = panic::catch_unwind(AssertUnwindSafe(|| unsafe { ptr::drop_in_place(stage) }));
        // SAFETY: as above, and what was in the stage is dropped, or as good
        // as.
        unsafe { ptr::write(stage, RemoteStage::Finished(result)) };
        // Release: the handle that sees `COMPLETE` sees the output. Acquire:
        // the waker read is the one registered.
        let state = self.state.fetch_or(COMPLETE, Ordering::AcqRel);
        if state & RX_CLOSED != 0 {
            // The handle went first, and never takes the output: it is
            // dropped here, on the task's thread, contained as the future's
            // drop is.
            // SAFETY: the stage stays this thread's, as `RX_CLOSED` came
            // before `COMPLETE`.
            let output = unsafe { ptr::replace(stage, RemoteStage::Consumed) };
            let _ = panic::catch_unwind(AssertUnwindSafe(|| drop(output)));
        } else if state & RX_TASK != 0 {
            // SAFETY: with `COMPLETE` set the handle no longer writes the
            // waker, and `RX_TASK` says it wrote one.
            let waker = unsafe { &*self.waker.get() };
            if let Some(waker) = waker {
                waker.wake_by_ref();
            }
        }
    }

    unsafe fn run(header: *const Header, scheduler: &Scheduler) {
        // SAFETY: the vtable's contract.
        let this = unsafe { Self::get(header) };
        // As for a task of the thread's own; see `Task::run`.
        this.header.queued.set(false);
        if this.header.woken.load(Ordering::Relaxed) {
            this.header.woken.swap(false, Ordering::SeqCst);
        }
        // Only this thread sets `COMPLETE`, so the load reads its own store:
        // a task that finished in its last run and was woken meanwhile is
        // left alone.
        if this.state.load(Ordering::Relaxed) & COMPLETE != 0 {
            return;
        }
        // The abort's store is ordered before this load by the wake that
        // followed it: through the remote queue's lock, or, for a wake that
        // found `woken` set and queued nothing, through the swap above,
        // which read the flag it set.
        if this.aborted.load(Ordering::Relaxed) {
            this.complete(Err(JoinError::cancelled()));
            scheduler.release(this.header.index.get());
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
            // SAFETY: until `COMPLETE` is set, which only this thread does,
            // the stage is this thread's alone.
            let stage = unsafe { &mut *this.stage.get() };
            panic::catch_unwind(AssertUnwindSafe(|| {
                if matches!(stage, RemoteStage::New(_)) {
                    let RemoteStage::New(make) = mem::replace(stage, RemoteStage::Consumed) else {
                        unreachable!()
                    };
                    *stage = RemoteStage::Running(make());
                }
                let RemoteStage::Running(future) = stage else {
                    unreachable!("a task that has not finished is new or running")
                };
                // SAFETY: the future stays where it is, in the task's
                // allocation, until it is dropped in place when the stage is
                // replaced.
                let future = unsafe { Pin::new_unchecked(future) };
                coop::budgeted(|| future.poll(&mut cx))
            }))
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
        // Only the thread that holds the unfinished task sets `COMPLETE`.
        if this.state.load(Ordering::Relaxed) & COMPLETE == 0 {
            this.complete(Err(JoinError::cancelled()));
        }
    }

    unsafe fn is_finished(header: *const Header) -> bool {
        // SAFETY: the vtable's contract.
        let this = unsafe { Self::get(header) };
        this.state.load(Ordering::Acquire) & COMPLETE != 0
    }

    unsafe fn abort(header: *const Header) {
        // SAFETY: the vtable's contract.
        let this = unsafe { Self::get(header) };
        // A finished task is left as it is; one that finishes meanwhile
        // ignores the flag, as its runs are over.
        if this.state.load(Ordering::Acquire) & COMPLETE != 0 {
            return;
        }
        // Ordered before the run that reads it by the wake; see `run`.
        this.aborted.store(true, Ordering::Relaxed);
        // SAFETY: the task is alive.
        unsafe { wake_by_ref(header) };
    }

    unsafe fn detach(header: *const Header) {
        // SAFETY: the vtable's contract.
        let this = unsafe { Self::get(header) };
        // Acquire: an output put there before `COMPLETE` is seen, to be
        // dropped. Release, for the task, which then drops its own.
        let state = this.state.fetch_or(RX_CLOSED, Ordering::AcqRel);
        if state & COMPLETE != 0 {
            // SAFETY: `COMPLETE` is set, so the stage is the handle's, and
            // the handle is going: nothing reaches the stage after this.
            let output = unsafe { ptr::replace(this.stage.get(), RemoteStage::Consumed) };
            drop(output);
        }
    }

    unsafe fn dealloc(header: *const Header) {
        // SAFETY: the vtable's contract, and the header is the first field
        // of the `#[repr(C)]` task, so its address is the task's.
        let task = unsafe { Self::get(header) };
        // A task is cancelled or completes before its last reference can
        // go, and its output is taken or dropped before: nothing in the
        // stage that could not be dropped on any thread.
        // SAFETY: nothing reaches the task again.
        let stage = unsafe { &*task.stage.get() };
        debug_assert!(matches!(stage, RemoteStage::Consumed));
        let block = NonNull::from(task).cast::<u8>();
        // SAFETY: nothing reaches the task again, and `get`'s reference is
        // not used after this.
        unsafe { ptr::drop_in_place(block.cast::<Self>().as_ptr()) };
        // The memory goes to the runtime running on this thread, if there is
        // one, for its next spawn of the layout, and to the allocator
        // otherwise.
        let layout = Layout::new::<Self>();
        // SAFETY: the block is the allocator's, of `layout`, and the task
        // in it is dropped.
        let kept = context::with_current(|scheduler| unsafe { scheduler.free_task(block, layout) });
        if kept.is_none() {
            // SAFETY: as above.
            unsafe { alloc::dealloc(block.as_ptr(), layout) };
        }
    }

    unsafe fn poll_join(
        header: *const Header,
        cx: &mut Context<'_>,
    ) -> Poll<Result<F::Output, JoinError>> {
        // SAFETY: the vtable's contract.
        let this = unsafe { Self::get(header) };
        let proceed = ready!(coop::poll_proceed(cx));
        // As a oneshot channel's receiver polls; see `sync::oneshot`.
        // Acquire: the output put there before `COMPLETE` is seen, and the
        // waker this registered before is the one in the slot.
        let mut state = this.state.load(Ordering::Acquire);
        if state & COMPLETE == 0 && state & RX_TASK != 0 {
            // SAFETY: the handle alone writes the waker, so reading it races
            // only with the task's reading.
            let waker = unsafe { &*this.waker.get() };
            if waker.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
                return Poll::Pending;
            }
            // Acquire, as above. Once `RX_TASK` is clear before the task
            // finishes, the task leaves the waker alone.
            state = this.state.fetch_and(!RX_TASK, Ordering::AcqRel);
        }
        if state & COMPLETE == 0 {
            // SAFETY: `RX_TASK` is clear and the task has not finished, so
            // it does not read the waker until this sets `RX_TASK` again.
            let old = unsafe { (*this.waker.get()).replace(cx.waker().clone()) };
            // Release: a task that finds `RX_TASK` reads the new waker.
            // Acquire: as above.
            state = this.state.fetch_or(RX_TASK, Ordering::AcqRel);
            // Dropped once the state is settled: dropping a waker can run
            // any code.
            drop(old);
            if state & COMPLETE == 0 {
                return Poll::Pending;
            }
        }
        proceed.made_progress();
        // SAFETY: `COMPLETE` is set, and the handle, being polled, is not
        // gone: the stage is the handle's.
        let stage = unsafe { ptr::replace(this.stage.get(), RemoteStage::Consumed) };
        match stage {
            RemoteStage::Finished(result) => Poll::Ready(result),
            RemoteStage::Consumed => panic!("`RemoteJoinHandle` polled after it completed"),
            RemoteStage::New(_) | RemoteStage::Running(_) => {
                unreachable!("a finished task has no future")
            }
        }
    }
}

/// Takes in a task spawned from another thread, as the scheduler takes its
/// remote queue: into its table, at the index that from then on names it,
/// and the back of its run queue. Only on the scheduler's thread.
pub(crate) fn adopt_spawned(task: RemoteRef, scheduler: &Scheduler) {
    let task = LocalRef::adopt(task);
    let index = scheduler.adopt(task.clone());
    let header = task.header();
    header.index.set(index);
    header.queued.set(true);
    scheduler.schedule_new(task);
}

/// Cancels a task spawned from another thread that its scheduler never took
/// in, as the runtime shuts down: its handle then reports it cancelled. Only
/// on the scheduler's thread, for a task of its remote queue.
pub(crate) fn cancel_spawned(task: RemoteRef) {
    // SAFETY: the reference keeps the task alive, and the vtable is the
    // task's own; the scheduler's thread alone reaches a task it has not
    // taken in, as the task's own would.
    unsafe { (task.header().vtable.cancel)(task.as_ptr()) }
}

/// A task spawned from another thread, through a [`Handle`](crate::Handle):
/// `Send`, unlike a [`JoinHandle`](super::JoinHandle), and so awaitable on
/// any thread.
///
/// The output is the task's, or a [`JoinError`] if it panicked or was
/// cancelled: aborted, or its runtime shut down before it finished. Dropping
/// the handle leaves the task running, detached, its output dropped on its
/// own thread. The handle is a reference to the task, and keeps the task's
/// memory until it is dropped; [`abort_handle`](RemoteJoinHandle::abort_handle)
/// gives another, which aborts the task without awaiting it.
///
/// ```
/// fn assert_send_sync<T: Send + Sync>() {}
/// assert_send_sync::<ruio_executor::RemoteJoinHandle<()>>();
/// ```
#[must_use = "dropping a `RemoteJoinHandle` detaches the task"]
pub struct RemoteJoinHandle<T> {
    task: RemoteRef,
    /// Polls the task for its output as `T`, which its header's vtable does
    /// not know.
    poll_join: unsafe fn(*const Header, &mut Context<'_>) -> Poll<Result<T, JoinError>>,
}

// SAFETY: the handle reaches the output only through `&mut self`, moving it
// out, so the output goes between threads, as its bound allows, and is never
// shared; the rest, the reference and the task's state, any thread may use.
unsafe impl<T: Send> Send for RemoteJoinHandle<T> {}
// SAFETY: as for `Send`.
unsafe impl<T: Send> Sync for RemoteJoinHandle<T> {}

impl<T> RemoteJoinHandle<T> {
    /// Cancels the task. Its runtime drops its future at its next turn, and
    /// awaiting the handle then returns a cancelled [`JoinError`]. A finished
    /// task is left as it is.
    pub fn abort(&self) {
        // SAFETY: the reference keeps the task alive, the vtable is the
        // task's own, and the task is one spawned from another thread, whose
        // abort any thread may call.
        unsafe { (self.task.header().vtable.abort)(self.task.as_ptr()) }
    }

    /// Whether the task has finished, by completing, panicking or being
    /// cancelled.
    pub fn is_finished(&self) -> bool {
        // SAFETY: as for `abort`.
        unsafe { (self.task.header().vtable.is_finished)(self.task.as_ptr()) }
    }

    /// A handle that aborts the task without awaiting it, from whichever
    /// thread, which can be kept or given away while this handle awaits the
    /// output; see [`RemoteAbortHandle`].
    pub fn abort_handle(&self) -> RemoteAbortHandle {
        RemoteAbortHandle {
            task: self.task.clone(),
        }
    }
}

impl<T> Future for RemoteJoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: the handle's reference keeps the task alive, and the
        // function was made for the task's future, whose output is `T`.
        unsafe { (self.poll_join)(self.task.as_ptr(), cx) }
    }
}

impl<T> Drop for RemoteJoinHandle<T> {
    fn drop(&mut self) {
        // SAFETY: as for `abort`.
        unsafe { (self.task.header().vtable.detach)(self.task.as_ptr()) }
    }
}

/// Aborts a task spawned from another thread without awaiting its output;
/// made by [`RemoteJoinHandle::abort_handle`].
///
/// `Send` and `Sync`, unlike an [`AbortHandle`](super::AbortHandle): the
/// handle, and its clones, abort the task from whichever thread. Dropping
/// one neither aborts the task nor detaches it, as dropping the
/// [`RemoteJoinHandle`] does: the task runs on, and its output goes to the
/// join handle, if there is one. A handle keeps the task's memory until it
/// is dropped.
///
/// ```
/// fn assert_send_sync<T: Send + Sync>() {}
/// assert_send_sync::<ruio_executor::RemoteAbortHandle>();
/// ```
#[derive(Clone)]
pub struct RemoteAbortHandle {
    task: RemoteRef,
}

impl RemoteAbortHandle {
    /// Cancels the task; see [`RemoteJoinHandle::abort`].
    pub fn abort(&self) {
        // SAFETY: the reference keeps the task alive, the vtable is the
        // task's own, and the task is one spawned from another thread, whose
        // abort any thread may call.
        unsafe { (self.task.header().vtable.abort)(self.task.as_ptr()) }
    }

    /// Whether the task has finished, by completing, panicking or being
    /// cancelled.
    pub fn is_finished(&self) -> bool {
        // SAFETY: as for `abort`.
        unsafe { (self.task.header().vtable.is_finished)(self.task.as_ptr()) }
    }
}

impl fmt::Debug for RemoteAbortHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteAbortHandle")
            .field("finished", &self.is_finished())
            .finish_non_exhaustive()
    }
}

impl<T> fmt::Debug for RemoteJoinHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteJoinHandle")
            .field("finished", &self.is_finished())
            .finish_non_exhaustive()
    }
}
