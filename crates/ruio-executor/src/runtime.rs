//! The runtime: a scheduler, its ring, and the loop of `block_on`.

use std::fmt;
use std::future::Future;
use std::io;
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use ruio_driver::Driver;

use crate::context;
use crate::scheduler::{Scheduler, Shared};
use crate::task::JoinHandle;

/// Configures and creates a [`Runtime`].
#[derive(Clone, Debug, Default)]
pub struct Builder {
    driver: ruio_driver::Builder,
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

    /// Creates the runtime.
    ///
    /// # Errors
    ///
    /// As for [`ruio_driver::Builder::build`].
    pub fn build(&self) -> io::Result<Runtime> {
        let driver = self.driver.build()?;
        Ok(Runtime {
            scheduler: Rc::new(Scheduler::new(driver)),
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
            if main.woken.swap(false, Ordering::SeqCst)
                && let Poll::Ready(output) = future.as_mut().poll(&mut cx)
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
