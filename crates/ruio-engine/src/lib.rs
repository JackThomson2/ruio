//! The contract between ruio's executor and the I/O engine it runs on.
//!
//! An engine is a thread's source of events: io_uring's ring on Linux, and
//! in time kqueue on macOS and an I/O completion port on Windows. The
//! executor runs the ready tasks, and when none is, parks on its engine
//! ([`Engine::park`]) until an operation completes, the engine's timer is
//! due, or another thread [unparks](Unpark::unpark) it. The engine wakes
//! the tasks whose operations completed through the wakers they were polled
//! with, so it needs nothing of the executor but those.
//!
//! The executor reaches its engine only through [`Engine`], [`Build`] and
//! [`Unpark`], so another engine plugs in by implementing them. One engine
//! is chosen when a program is built, never at run time: the executor
//! names it by its type and calls it through these traits statically, so
//! that an engine pays nothing for the others being possible, no dynamic
//! dispatch, wrapper or field. What only one engine can do, such as
//! io_uring's registered files, stays that engine's own API, outside the
//! contract.
//!
//! [`LocalWaker`] lets an engine hold the wakers of the executor's tasks
//! by a reference the executor counts itself, rather than by a clone.
//!
//! [`park`] has the smallest engine that keeps the contract, which runs no
//! I/O, for the systems ruio has no I/O engine for yet; [`conformance`]
//! checks an engine against the contract, for the engine's own tests.

pub mod conformance;
pub mod park;

use std::fmt;
use std::io;
use std::task::RawWakerVTable;
use std::time::Duration;

/// A thread's I/O engine, as the executor running on the thread drives it.
///
/// An engine belongs to the thread that made it. Its operations, which the
/// engine's own API starts, complete as the engine is turned, by
/// [`submit`](Engine::submit) or [`park`](Engine::park), which wake the
/// tasks that polled them.
///
/// An error from a turn means that the engine has lost events, so that the
/// tasks waiting for them may never be woken; the executor gives up on the
/// runtime then.
pub trait Engine: Sized + 'static {
    /// Configures and creates the engine.
    type Builder: Build<Engine = Self>;

    /// Wakes the engine from any thread.
    type Unparker: Unpark;

    /// Keeps the engine the thread's current one until dropped; see
    /// [`Engine::enter`].
    type Entered;

    /// A number that identifies the engine among every one of its kind the
    /// process has had.
    fn id(&self) -> u64;

    /// The time on the clock that [`wake_at`](Engine::wake_at) takes
    /// deadlines on: a monotonic clock, as the time since a point of its
    /// own.
    fn monotonic_now() -> Duration;

    /// Starts the operations queued, and delivers the events that are ready,
    /// without waiting for more.
    ///
    /// # Errors
    ///
    /// If the engine cannot start what is queued or has lost events.
    fn submit(&self) -> io::Result<()>;

    /// Whether a [`park`](Engine::park) would return at once: events are
    /// ready, or an [`Unpark`] has been used since the last park. A thread
    /// that would rather not sleep polls this before it parks.
    fn has_work(&self) -> bool;

    /// Starts the operations queued, waits until an event is ready, an
    /// [`Unpark`] is used, the timer [`wake_at`](Engine::wake_at) armed is
    /// due, or `timeout` passes, and delivers the events.
    ///
    /// An unpark since the last park makes this return without waiting;
    /// several make it return once.
    ///
    /// # Errors
    ///
    /// As for [`Engine::submit`]. Timing out is not an error.
    fn park(&self, timeout: Option<Duration>) -> io::Result<()>;

    /// Makes sure the engine wakes by `deadline`, a time on the clock of
    /// [`monotonic_now`](Engine::monotonic_now): a park returns by then
    /// however long it would have waited, and
    /// [`timer_fired`](Engine::timer_fired) then says so.
    ///
    /// The engine has one such timer. A deadline earlier than the one it is
    /// armed for moves it; a later one may change nothing, as waking early
    /// does no harm. The executor arms it for its earliest timer, which
    /// spares each park a timeout of its own.
    ///
    /// # Errors
    ///
    /// As for [`Engine::submit`], if arming the timer needs a submission
    /// that fails.
    fn wake_at(&self, deadline: Duration) -> io::Result<()>;

    /// Whether the timer [`wake_at`](Engine::wake_at) armed has fired since
    /// this was last asked. Asking clears it.
    fn timer_fired(&self) -> bool;

    /// A handle that wakes this engine from any thread.
    fn unparker(&self) -> Self::Unparker;

    /// Makes this the thread's current engine until the guard is dropped,
    /// for what is made or first polled on the thread without being handed
    /// an engine to find this one. A runtime enters its engine while it runs.
    /// Guards nest: dropping one makes current again the engine that was
    /// before it.
    fn enter(&self) -> Self::Entered;

    /// Holds the wakers `local` describes, those of the executor on this
    /// thread, by a reference the executor counts rather than by a clone;
    /// see [`LocalWaker`]. An engine that holds no wakers may ignore it.
    ///
    /// # Safety
    ///
    /// Every waker with `local.vtable` that reaches this engine must name a
    /// live task. `local.retain` must return `true` only if `local.release`,
    /// `local.wake` and `local.wake_release` may then use the task from this
    /// thread, and must then keep it alive until the matching
    /// `local.release` or `local.wake_release`; `local.wake` must wake it as
    /// `Waker::wake_by_ref` would, and `local.wake_release` as `local.wake`
    /// and then `local.release` would, without using this engine.
    unsafe fn set_local_waker(&self, local: LocalWaker);
}

/// Configures and creates an [`Engine`].
pub trait Build: Clone + fmt::Debug + Default {
    /// The engine this builds.
    type Engine: Engine<Builder = Self>;

    /// Sets how long dropping the engine waits for the operations still in
    /// flight, which the drop cancels, to finish, before it leaks the
    /// memory the system may still use.
    #[must_use]
    fn shutdown_timeout(self, timeout: Duration) -> Self;

    /// Creates the engine, on the calling thread.
    ///
    /// # Errors
    ///
    /// If the system cannot give the engine what it needs, or has no such
    /// engine.
    fn build(&self) -> io::Result<Self::Engine>;
}

/// Wakes a parked [`Engine`] from any thread.
///
/// [`unpark`](Unpark::unpark) makes the engine's current
/// [`park`](Engine::park) return or, if it is not parked, its next one.
/// Unparks do not add up: several before a park make it return once.
pub trait Unpark: Clone + fmt::Debug + Send + Sync + 'static {
    /// Wakes the engine, or makes its next park return at once. Does
    /// nothing once the engine is gone.
    fn unpark(&self);
}

/// How an engine holds the wakers of one executor's tasks, which run on
/// the engine's thread: by a reference the executor counts itself, without
/// atomic operations, where any other waker is cloned.
///
/// An executor gives its engine one with
/// [`Engine::set_local_waker`]. The engine recognises the executor's wakers
/// by their `vtable`, and asks `retain` whether it may hold each by
/// reference: the executor counts a reference to a task it may count on
/// this thread, and says so, and the engine then holds the waker's `data`
/// until a `release`, waking it with `wake` meanwhile, as
/// `Waker::wake_by_ref` would. A waker of the same vtable that names a task
/// of another thread, which a waker sent there can, the executor refuses,
/// and the engine clones it. The executor so counts the engine's references
/// to a task along with its own, instead of the waker's clone and drop each
/// taking an atomic operation.
#[derive(Clone, Copy, Debug)]
pub struct LocalWaker {
    /// The vtable of the wakers to hold this way, compared by address.
    pub vtable: &'static RawWakerVTable,
    /// Counts a reference to the task a waker's `data` names, if the
    /// executor may count it on this thread, and returns whether it did.
    pub retain: unsafe fn(*const ()) -> bool,
    /// Lets go of a reference `retain` counted.
    pub release: unsafe fn(*const ()),
    /// Wakes the task a waker's `data` names, which `retain` holds.
    pub wake: unsafe fn(*const ()),
    /// Wakes the task a waker's `data` names as `wake` does, and lets go of
    /// the reference `retain` counted as `release` does, at once: as an
    /// operation completes, after which the engine holds its task no more,
    /// so that the executor can queue the task by that reference.
    pub wake_release: unsafe fn(*const ()),
}
