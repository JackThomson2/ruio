//! The runtime: a scheduler, its ring, and the loop of `block_on`.

use std::fmt;
use std::future::Future;
use std::io;
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use ruio_driver::Driver;

use crate::handle::Handle;
use crate::scheduler::{Scheduler, Shared};
use crate::task::JoinHandle;
use crate::{context, coop};

/// Configures and creates a [`Runtime`].
#[derive(Clone, Debug, Default)]
pub struct Builder {
    driver: ruio_driver::Builder,
    spin: Duration,
}

impl Builder {
    /// A builder with the default configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the size of the ring's submission queue; see
    /// [`ruio_driver::Builder::entries`].
    pub fn entries(mut self, entries: u32) -> Self {
        self.driver = self.driver.entries(entries);
        self
    }

    /// Sets the number of slots in the ring's registered file table, which
    /// holds direct descriptors; see [`ruio_driver::Builder::files`].
    pub fn files(mut self, slots: u32) -> Self {
        self.driver = self.driver.files(slots);
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

    /// Creates the runtime.
    ///
    /// # Errors
    ///
    /// As for [`ruio_driver::Builder::build`].
    pub fn build(&self) -> io::Result<Runtime> {
        let driver = self.driver.build()?;
        Ok(Runtime {
            scheduler: Rc::new(Scheduler::new(driver, self.spin)),
        })
    }
}

/// Runs futures on the thread that created it, with an io_uring ring for
/// their I/O.
///
/// [`Runtime::block_on`] runs a future to completion, and runs the tasks
/// spawned meanwhile. Dropping the runtime drops the futures of the tasks
/// that have not finished, then closes the ring once the kernel is done with
/// their operations.
///
/// ```compile_fail
/// fn assert_send<T: Send>() {}
/// assert_send::<ruio_executor::Runtime>();
/// ```
pub struct Runtime {
    scheduler: Rc<Scheduler>,
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
    /// the ring meanwhile, and returns its output.
    ///
    /// # Panics
    ///
    /// If the thread is already inside `block_on`, or if `future` panics.
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        let _enter = context::enter(Rc::clone(&self.scheduler));
        let main = Arc::new(MainWaker {
            woken: AtomicBool::new(true),
            shared: Arc::clone(self.scheduler.shared()),
        });
        let waker = Waker::from(Arc::clone(&main));
        let mut cx = Context::from_waker(&waker);
        let mut future = pin!(future);
        loop {
            // A plain load first: most passes find the flag clear, and only
            // consuming a wake needs the read-modify-write.
            if main.woken.load(Ordering::Acquire)
                && main.woken.swap(false, Ordering::SeqCst)
                && let Poll::Ready(output) = coop::budgeted(|| future.as_mut().poll(&mut cx))
            {
                return output;
            }
            self.scheduler.tick(&main.woken);
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

    /// The runtime's io_uring driver.
    pub fn driver(&self) -> &Driver {
        self.scheduler.driver()
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
        // inside the runtime.
        let _enter = context::enter_if_idle(Rc::clone(&self.scheduler));
        self.scheduler.shutdown();
    }
}

impl fmt::Debug for Runtime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runtime").finish_non_exhaustive()
    }
}

/// The waker of the future given to `block_on`.
struct MainWaker {
    woken: AtomicBool,
    shared: Arc<Shared>,
}

impl Wake for MainWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.store(true, Ordering::SeqCst);
        // On the runtime's own thread the flag is enough: the loop checks it
        // before parking.
        if !context::is_current(&self.shared) {
            self.shared.unpark();
        }
    }
}
