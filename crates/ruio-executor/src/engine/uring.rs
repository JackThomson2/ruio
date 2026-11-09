//! The io_uring engine: the ring of `ruio-driver`, and what the executor
//! makes public of it beyond the engine contract.

use ruio_driver::{Driver, RemoteRing, Unparker};
use ruio_engine::{Engine, Unpark};

use crate::handle::Handle;
use crate::runtime::{Builder, Runtime};

/// The engine.
pub(crate) type Native = Driver;

/// What a runtime's shared part keeps of its ring for other threads: the
/// unparker, and the ring as they see it, which [`Handle::ring`] gives.
pub(crate) struct Remote {
    unparker: Unparker,
    ring: RemoteRing,
}

impl Remote {
    pub(crate) fn new(driver: &Driver) -> Self {
        Self {
            unparker: Engine::unparker(driver),
            ring: driver.remote(),
        }
    }

    /// Wakes the ring, or makes its next park return at once.
    #[inline]
    pub(crate) fn unpark(&self) {
        Unpark::unpark(&self.unparker);
    }
}

/// The io_uring API the crate root re-exports.
pub(crate) mod public {
    pub use ruio_driver::Driver;

    use crate::{OUTSIDE, context};

    /// The io_uring driver of the thread's runtime.
    ///
    /// ```
    /// use ruio_driver::ops::Nop;
    /// use ruio_executor::Runtime;
    ///
    /// let runtime = Runtime::new()?;
    /// runtime.block_on(async { ruio_executor::driver().op(Nop).await })?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    ///
    /// # Panics
    ///
    /// If the thread is not inside
    /// [`Runtime::block_on`](crate::Runtime::block_on).
    #[inline]
    pub fn driver() -> Driver {
        try_driver().expect(OUTSIDE)
    }

    /// The io_uring driver of the thread's runtime, or `None` if the thread
    /// is not inside one, as when code that may run anywhere, such as a
    /// destructor, has I/O for the ring if there is one.
    #[inline]
    pub fn try_driver() -> Option<Driver> {
        context::with_current(|scheduler| scheduler.engine().clone())
    }
}

impl Builder {
    /// Sets the size of the ring's submission queue; see
    /// [`ruio_driver::Builder::entries`].
    pub fn entries(mut self, entries: u32) -> Self {
        self.engine = self.engine.entries(entries);
        self
    }

    /// Sets the number of slots in the ring's registered file table, which
    /// holds direct descriptors; see [`ruio_driver::Builder::files`].
    pub fn files(mut self, slots: u32) -> Self {
        self.engine = self.engine.files(slots);
        self
    }
}

impl Runtime {
    /// The runtime's io_uring driver.
    pub fn driver(&self) -> &Driver {
        self.scheduler.engine()
    }
}

impl Handle {
    /// The runtime's ring, as other threads see it: where to send direct
    /// descriptors for the runtime's tasks to use; see
    /// [`DirectFd::send_to`](ruio_driver::DirectFd::send_to).
    pub fn ring(&self) -> &RemoteRing {
        &self.shared.engine.ring
    }
}
