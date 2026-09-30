//! Spawning tasks onto a runtime from other threads.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Waker, ready};

use ruio_driver::RemoteRing;

use crate::scheduler::Shared;
use crate::sync::oneshot;
use crate::{JoinError, JoinHandle, spawn};

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

    /// Spawns `future` onto the runtime, from whichever thread: the
    /// runtime's own thread makes it a task at its next turn, which is why
    /// the future and its output must be `Send`; see [`RemoteJoinHandle`].
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

    /// Spawns the future that `make` makes, on the runtime's own thread at
    /// its next turn, as [`spawn`](Handle::spawn) spawns one: made there,
    /// the future need not be `Send`, only `make` and the output. So it can
    /// hold what cannot leave its thread, such as a socket of the runtime's
    /// ring.
    pub fn spawn_with<M, F>(&self, make: M) -> RemoteJoinHandle<F::Output>
    where
        M: FnOnce() -> F + Send + 'static,
        F: Future + 'static,
        F::Output: Send + 'static,
    {
        let (output, rx) = oneshot::channel();
        let abort = Arc::new(Abort::default());
        let relay_abort = Arc::clone(&abort);
        self.shared.spawn_remote(Box::new(move || {
            let task = spawn(make());
            let _relay = spawn(Relay {
                task,
                abort: relay_abort,
                aborted: false,
                output: Some(output),
            });
        }));
        RemoteJoinHandle { rx, abort }
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

/// A request to abort a task, from whichever thread.
#[derive(Default)]
struct Abort {
    requested: AtomicBool,
    /// The relay's task, woken when the request comes.
    waker: Mutex<Option<Waker>>,
}

impl Abort {
    fn request(&self) {
        self.requested.store(true, Ordering::Release);
        let waker = self
            .waker
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Whether abort has been requested; if not, arranges for the task to
    /// be woken when it is.
    fn requested(&self, cx: &Context<'_>) -> bool {
        if self.requested.load(Ordering::Acquire) {
            return true;
        }
        *self.waker.lock().unwrap_or_else(PoisonError::into_inner) = Some(cx.waker().clone());
        // A request made while the waker went in found none to wake.
        self.requested.load(Ordering::Acquire)
    }
}

/// Runs on the task's runtime, beside the task: hands its output to the
/// remote handle, and aborts it when the handle asks.
struct Relay<T> {
    task: JoinHandle<T>,
    abort: Arc<Abort>,
    aborted: bool,
    /// Taken once the output is sent.
    output: Option<oneshot::Sender<Result<T, JoinError>>>,
}

impl<T> Future for Relay<T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if !this.aborted && this.abort.requested(cx) {
            this.aborted = true;
            this.task.abort();
        }
        let result = ready!(Pin::new(&mut this.task).poll(cx));
        if let Some(output) = this.output.take() {
            // A handle dropped meanwhile detached the task: nobody asks.
            let _ = output.send(result);
        }
        Poll::Ready(())
    }
}

// Nothing is pinned: the fields are only ever moved.
impl<T> Unpin for Relay<T> {}

/// A task spawned from another thread, through a [`Handle`]: `Send`, unlike
/// a [`JoinHandle`], and so awaitable on any thread.
///
/// The output is the task's, or a [`JoinError`] if it panicked or was
/// cancelled, as when its runtime shut down before it finished. Dropping
/// the handle leaves the task running, detached.
pub struct RemoteJoinHandle<T> {
    rx: oneshot::Receiver<Result<T, JoinError>>,
    abort: Arc<Abort>,
}

impl<T> RemoteJoinHandle<T> {
    /// Cancels the task. Its runtime drops its future at its next turn, and
    /// awaiting the handle then returns a cancelled [`JoinError`]. A finished
    /// task is left as it is.
    pub fn abort(&self) {
        self.abort.request();
    }
}

impl<T> Future for RemoteJoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().rx)
            .poll(cx)
            // The future was dropped without running, with its runtime.
            .map(|result| result.unwrap_or_else(|_| Err(JoinError::cancelled())))
    }
}

impl<T> fmt::Debug for RemoteJoinHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteJoinHandle").finish_non_exhaustive()
    }
}
