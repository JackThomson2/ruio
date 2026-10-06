//! Spawning tasks onto a runtime from other threads.

use std::fmt;
use std::future::Future;
use std::sync::Arc;

use ruio_driver::RemoteRing;

use crate::scheduler::Shared;
use crate::task::{RemoteJoinHandle, RemoteTask};

/// A handle to a runtime, which any thread can spawn tasks onto: `Send`,
/// `Sync` and cheap to clone. Made by
/// [`Runtime::handle`](crate::Runtime::handle) or [`handle`](crate::handle).
#[derive(Clone)]
pub struct Handle {
    shared: Arc<Shared>,
}

impl Handle {
    pub(crate) fn new(shared: Arc<Shared>) -> Self {
        Self { shared }
    }

    /// Spawns `future` onto the runtime, from whichever thread: the task is
    /// made here, and the runtime's own thread takes it in at its next turn,
    /// which is why the future and its output must be `Send`; see
    /// [`RemoteJoinHandle`].
    ///
    /// A runtime that has shut down, or shuts down before its next turn,
    /// drops the future, and the handle then reports the task cancelled.
    pub fn spawn<F>(&self, future: F) -> RemoteJoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.spawn_with(move || future)
    }

    /// Spawns the future that `make` makes, as [`spawn`](Handle::spawn)
    /// spawns one, but made on the runtime's own thread as the task first
    /// runs: made there, the future need not be `Send`, only `make` and the
    /// output. So it can hold what cannot leave its thread, such as a socket
    /// of the runtime's ring. A panic in `make` is the task's, which the
    /// handle reports.
    pub fn spawn_with<M, F>(&self, make: M) -> RemoteJoinHandle<F::Output>
    where
        M: FnOnce() -> F + Send + 'static,
        F: Future + 'static,
        F::Output: Send + 'static,
    {
        RemoteTask::spawn(make, &self.shared)
    }

    /// The runtime's ring, as other threads see it: where to send direct
    /// descriptors for the runtime's tasks to use; see
    /// [`DirectFd::send_to`](ruio_driver::DirectFd::send_to).
    pub fn ring(&self) -> &RemoteRing {
        self.shared.ring()
    }
}

impl fmt::Debug for Handle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Handle").finish_non_exhaustive()
    }
}
