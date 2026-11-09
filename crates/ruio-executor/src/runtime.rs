//! The runtime: a scheduler, its ring, and the loop of `block_on`.

use std::fmt;
use std::future::Future;
use std::io;
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use crate::engine::{Build, Engine, NativeBuilder};
use crate::handle::Handle;
use crate::scheduler::Scheduler;
use crate::task::{JoinHandle, Main};
use crate::{context, coop};

/// Configures and creates a [`Runtime`].
#[derive(Clone, Debug, Default)]
pub struct Builder {
    pub(crate) engine: NativeBuilder,
    spin: Duration,
}

impl Builder {
    /// A builder with the default configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets how long dropping the runtime waits for the kernel to finish
    /// with the operations of its tasks, which the drop cancels, before it
    /// leaks what the kernel still holds and closes the engine. The default
    /// is one second. Through [`Cores::builder`](crate::Cores::builder) it
    /// bounds the shutdown of every core's runtime.
    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.engine = Build::shutdown_timeout(self.engine, timeout);
        self
    }

    /// Has the runtime, when it runs out of work, poll the ring for up to
    /// `budget` before it parks, as long as its waits are that short.
    ///
    /// A runtime with nothing to run parks its thread until a completion
    /// arrives, a timer is due or another thread wakes a task, and the
    /// kernel wakes the thread, which takes a few microseconds, and longer on
    /// a busy host. With a budget, a runtime whose last wait ended within it
    /// polls for the budget instead, with the thread awake, and parks only
    /// if nothing has come by then; a runtime whose waits are longer parks
    /// at once, so an idle one never polls. A thread that polls occupies its
    /// core, so this is for cores given to the runtime, as [`Cores`] pins
    /// them, and for work whose turns come within the budget: a single
    /// connection's round trip, say.
    ///
    /// Off unless set; `Duration::ZERO` turns it off.
    ///
    /// [`Cores`]: crate::Cores
    pub fn spin(mut self, budget: Duration) -> Self {
        self.spin = budget;
        self
    }

    /// Creates the runtime, and its engine on the calling thread.
    ///
    /// # Errors
    ///
    /// If the engine cannot be created: with io_uring, as for
    /// `ruio_driver::Builder::build`.
    pub fn build(&self) -> io::Result<Runtime> {
        let engine = Build::build(&self.engine)?;
        Ok(Runtime {
            scheduler: Rc::new(Scheduler::new(engine, self.spin)),
        })
    }
}

/// Runs futures on the thread that created it, with its I/O engine for
/// their I/O.
///
/// [`Runtime::block_on`] runs a future to completion, and runs the tasks
/// spawned meanwhile. Dropping the runtime drops the futures of the tasks
/// that have not finished, then closes its engine once the kernel is done
/// with their operations.
///
/// ```compile_fail
/// fn assert_send<T: Send>() {}
/// assert_send::<ruio_executor::Runtime>();
/// ```
pub struct Runtime {
    pub(crate) scheduler: Rc<Scheduler>,
}

impl Runtime {
    /// Creates a runtime with the default configuration; see [`Builder`].
    ///
    /// # Errors
    ///
    /// As for [`Builder::build`].
    pub fn new() -> io::Result<Self> {
        Builder::new().build()
    }

    /// A builder for a runtime with a different configuration.
    pub fn builder() -> Builder {
        Builder::new()
    }

    /// Runs `future` to completion on this thread, running spawned tasks and
    /// the engine meanwhile, and returns its output.
    ///
    /// # Panics
    ///
    /// If the thread is already inside `block_on`, or if `future` panics.
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        let _enter = context::enter(Rc::clone(&self.scheduler));
        // Portable operations first polled here queue on the runtime's ring,
        // and find it again with one comparison while they stay.
        let _current = Engine::enter(self.scheduler.engine());
        // Wakers of the future name it as a task's name the task, so that
        // waking it on this thread takes no atomic operation.
        let main = Main::new(&self.scheduler);
        let mut future = pin!(future);
        loop {
            if main.take_wake()
                && let Poll::Ready(output) =
                    main.poll(|cx| coop::budgeted(|| future.as_mut().poll(cx)))
            {
                return output;
            }
            self.scheduler.tick(&main);
        }
    }

    /// Spawns a task, which runs whenever this thread is inside
    /// [`Runtime::block_on`].
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        self.scheduler.spawn(future)
    }

    /// A handle that spawns tasks onto this runtime from any thread; see
    /// [`Handle`].
    pub fn handle(&self) -> Handle {
        Handle::new(Arc::clone(self.scheduler.shared()))
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        // Dropping futures runs their destructors, which may expect to be
        // inside the runtime, and drops the portable operations of its tasks,
        // which so find their ring at hand.
        let _enter = context::enter_if_idle(Rc::clone(&self.scheduler));
        let _current = Engine::enter(self.scheduler.engine());
        self.scheduler.shutdown();
    }
}

impl fmt::Debug for Runtime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runtime").finish_non_exhaustive()
    }
}
