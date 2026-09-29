//! The driver: a ring, the operations in flight on it, and the loop that
//! submits them and delivers their completions.

use std::cell::RefCell;
use std::ffi::c_void;
use std::fmt;
use std::io;
use std::mem;
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::task::Waker;
use std::time::{Duration, Instant};

use ruio_sys as sys;

use crate::Probe;
use crate::op::{Completion, Op, Operation, Orphan};
use crate::ring::Ring;
use crate::table::{IGNORED, Outcome, Table};
use crate::unpark::{Notify, Unparker};

/// The `user_data` of the poll on the eventfd that unparkers write to.
const WAKE: u64 = IGNORED - 1;

/// The setup flags of every ring, besides `IORING_SETUP_NO_SQARRAY` when the
/// kernel has it. See DESIGN.md.
const SETUP_FLAGS: u32 = sys::IORING_SETUP_SINGLE_ISSUER
    | sys::IORING_SETUP_DEFER_TASKRUN
    | sys::IORING_SETUP_TASKRUN_FLAG
    | sys::IORING_SETUP_SUBMIT_ALL
    | sys::IORING_SETUP_CLAMP;

/// Flags an [`Operation`] may not put on its entry: they would give it more
/// or fewer than one completion, or chain it to an unrelated entry.
const FORBIDDEN_SQE_FLAGS: u8 =
    sys::IOSQE_IO_LINK | sys::IOSQE_IO_HARDLINK | sys::IOSQE_CQE_SKIP_SUCCESS;

/// Configures and creates a [`Driver`].
#[derive(Clone, Debug)]
pub struct Builder {
    entries: u32,
    shutdown_timeout: Duration,
}

impl Builder {
    /// A builder with the default configuration: 256 submission queue
    /// entries, and a one-second shutdown timeout.
    pub fn new() -> Self {
        Self {
            entries: 256,
            shutdown_timeout: Duration::from_secs(1),
        }
    }

    /// Sets the size of the submission queue. The kernel rounds it up to a
    /// power of two and caps it at 32768; the completion queue is four times
    /// as large.
    ///
    /// This does not limit how many operations can be in flight: a full
    /// submission queue is submitted early.
    pub fn entries(mut self, entries: u32) -> Self {
        self.entries = entries;
        self
    }

    /// Sets how long dropping the driver waits for the kernel to finish with
    /// cancelled operations.
    ///
    /// The memory of an operation the kernel is still using after that, such
    /// as a read stuck in an uninterruptible wait, is leaked rather than
    /// freed. The default is one second.
    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// Creates the driver.
    ///
    /// # Errors
    ///
    /// Fails with [`io::ErrorKind::Unsupported`] if the kernel is below ruio's
    /// baseline (see [`Probe::check_baseline`]), or with the system's error if
    /// the ring cannot be created.
    pub fn build(&self) -> io::Result<Driver> {
        let probe = Probe::current()?;
        probe
            .check_baseline()
            .map_err(|err| io::Error::new(io::ErrorKind::Unsupported, err))?;
        let mut flags = SETUP_FLAGS;
        if probe.supports_setup_flags(sys::IORING_SETUP_NO_SQARRAY) {
            flags |= sys::IORING_SETUP_NO_SQARRAY;
        }
        let cq_entries = self.entries.saturating_mul(4);
        let ring = Ring::new(self.entries, cq_entries, flags)?;
        let notify = Arc::new(Notify::new()?);
        Ok(Driver {
            inner: Rc::new(RefCell::new(Inner {
                table: Table::with_capacity(cq_entries as usize),
                ring,
                notify,
                wake_armed: false,
                cancels: Vec::new(),
                wakers: Vec::new(),
                released: Vec::new(),
                stats: Stats::default(),
                shutdown_timeout: self.shutdown_timeout,
            })),
        })
    }
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

/// Counts of a driver's work, for tests and benchmarks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Stats {
    /// Operations queued for submission.
    pub operations: u64,
    /// Calls to `io_uring_enter(2)`.
    pub enters: u64,
    /// Completions processed, including those of internal requests.
    pub completions: u64,
}

/// An io_uring instance and the operations in flight on it.
///
/// [`Driver::op`] turns an [`Operation`] into an [`Op`] future. Polling the
/// future queues the operation; [`Driver::submit`] and [`Driver::park`] hand
/// everything queued to the kernel in one system call, then deliver the
/// completions that are ready and wake the tasks waiting for them.
///
/// A driver belongs to the thread that created it, and clones are handles to
/// the same ring. The ring is closed once every handle and every [`Op`] is
/// gone.
///
/// ```compile_fail
/// fn assert_send<T: Send>() {}
/// assert_send::<ruio_driver::Driver>();
/// ```
#[derive(Clone)]
pub struct Driver {
    inner: Rc<RefCell<Inner>>,
}

/// A handle to a driver that does not keep it alive, for resources that
/// outlive their use of the ring, such as registered memory, and would
/// otherwise keep the ring alive in a cycle through operations that hold
/// them.
#[derive(Clone)]
pub(crate) struct WeakDriver {
    inner: Weak<RefCell<Inner>>,
}

impl WeakDriver {
    pub(crate) fn upgrade(&self) -> Option<Driver> {
        self.inner.upgrade().map(|inner| Driver { inner })
    }
}

struct Inner {
    ring: Ring,
    /// Every operation the kernel may still own.
    table: Table<Box<dyn Orphan>>,
    /// What the driver shares with its unparkers.
    notify: Arc<Notify>,
    /// Whether the multishot poll on the unparkers' eventfd is in flight.
    wake_armed: bool,
    /// Dropped operations to cancel, by `user_data`.
    cancels: Vec<u64>,
    /// Wakers of a batch of completions, kept to reuse the allocation.
    wakers: Vec<Waker>,
    /// Orphaned payloads released by a batch of completions, likewise.
    released: Vec<(Box<dyn Orphan>, Completion)>,
    stats: Stats,
    shutdown_timeout: Duration,
}

impl Driver {
    /// Creates a driver with the default configuration; see [`Builder`].
    ///
    /// # Errors
    ///
    /// As for [`Builder::build`].
    pub fn new() -> io::Result<Self> {
        Builder::new().build()
    }

    /// A builder for a driver with a different configuration.
    pub fn builder() -> Builder {
        Builder::new()
    }

    /// Wraps `operation` in a future that queues it when first polled.
    pub fn op<T: Operation>(&self, operation: T) -> Op<T> {
        Op::new(self.clone(), operation)
    }

    /// Submits the queued operations, and delivers the completions that are
    /// ready without waiting for more.
    ///
    /// # Errors
    ///
    /// Fails if the kernel rejects the submission, or reports that
    /// completions were lost (`EBADR`).
    pub fn submit(&self) -> io::Result<()> {
        self.enter(0, None)
    }

    /// Submits the queued operations, waits until at least one completion is
    /// ready, an [`Unparker`] is used or `timeout` passes, and delivers the
    /// completions.
    ///
    /// If an unpark arrived since the last park, this returns without
    /// waiting.
    ///
    /// # Errors
    ///
    /// As for [`Driver::submit`]. Timing out is not an error.
    pub fn park(&self, timeout: Option<Duration>) -> io::Result<()> {
        let notify = Arc::clone(&self.inner.borrow().notify);
        if !notify.begin_park() {
            return self.enter(0, None);
        }
        let armed = self.inner.borrow_mut().arm_wake();
        let result = armed.and_then(|()| self.enter(1, timeout));
        notify.end_park();
        result
    }

    /// A handle that wakes this driver from any thread.
    pub fn unparker(&self) -> Unparker {
        Unparker::new(Arc::clone(&self.inner.borrow().notify))
    }

    /// The number of operations the kernel may still own, including those
    /// whose [`Op`] was dropped.
    pub fn in_flight(&self) -> usize {
        self.inner.borrow().table.in_flight()
    }

    /// Counts of the driver's work so far.
    pub fn stats(&self) -> Stats {
        self.inner.borrow().stats
    }

    pub(crate) fn downgrade(&self) -> WeakDriver {
        WeakDriver {
            inner: Rc::downgrade(&self.inner),
        }
    }

    /// Registers resources with the ring, or queries or configures it.
    ///
    /// # Errors
    ///
    /// `EBUSY` if called from code running inside the driver, or what the
    /// kernel reports.
    ///
    /// # Safety
    ///
    /// As for [`sys::io_uring_register`].
    pub(crate) unsafe fn register(
        &self,
        opcode: u32,
        arg: *mut c_void,
        nr_args: u32,
    ) -> io::Result<u32> {
        let inner = self
            .inner
            .try_borrow()
            .map_err(|_| io::Error::from_raw_os_error(libc::EBUSY))?;
        // SAFETY: the caller upholds the contract of `io_uring_register`.
        unsafe { inner.ring.register(opcode, arg, nr_args) }
    }

    fn enter(&self, min_complete: u32, timeout: Option<Duration>) -> io::Result<()> {
        let timeout = timeout.map(timespec);
        let result = self
            .inner
            .borrow_mut()
            .enter(min_complete, timeout.as_ref());
        self.deliver();
        // Completions that did not fit in the queue wait in the kernel, which
        // moves them in when entered again, now that there is room.
        while self.inner.borrow().ring.sq_flags() & sys::IORING_SQ_CQ_OVERFLOW != 0 {
            self.inner.borrow_mut().enter(0, None)?;
            self.deliver();
        }
        result
    }

    /// Processes the posted completions: wakes the tasks waiting for them and
    /// finishes orphaned operations. Both run code the driver does not
    /// control, which may use the driver, so they run after it is released.
    fn deliver(&self) {
        let (mut wakers, mut released) = {
            let mut inner = self.inner.borrow_mut();
            let mut wakers = mem::take(&mut inner.wakers);
            let mut released = mem::take(&mut inner.released);
            inner.reap(&mut wakers, &mut released);
            (wakers, released)
        };
        for waker in wakers.drain(..) {
            waker.wake();
        }
        for (payload, completion) in released.drain(..) {
            payload.finish(completion);
        }
        let mut inner = self.inner.borrow_mut();
        inner.wakers = wakers;
        inner.released = released;
    }

    /// Queues an operation's entry, returning its `user_data`.
    pub(crate) fn queue<T: Operation>(&self, operation: &mut T, waker: &Waker) -> io::Result<u64> {
        self.inner.borrow_mut().queue(operation, waker)
    }

    /// Takes the operation's completion, or arranges for `waker` to be woken
    /// when it arrives.
    pub(crate) fn poll_completion(&self, user_data: u64, waker: &Waker) -> Option<Completion> {
        let polled = self.inner.borrow_mut().table.poll(user_data, waker);
        // A replaced waker is dropped here, after the driver is released.
        polled.ok()
    }

    /// Takes over the operation of an `Op` dropped while in flight.
    pub(crate) fn orphan<T: Operation>(&self, user_data: u64, operation: T) {
        let Ok(mut inner) = self.inner.try_borrow_mut() else {
            // Dropped by code running inside the driver, such as a waker's
            // destructor. The kernel may still use the operation's memory, so
            // it cannot be freed; leaking it is safe.
            mem::forget(operation);
            return;
        };
        match inner.table.take_completed(user_data) {
            Some(completion) => {
                drop(inner);
                drop(operation.complete(completion));
            }
            None => {
                let waker = inner.table.orphan(user_data, Box::new(operation));
                inner.cancels.push(user_data);
                drop(inner);
                drop(waker);
            }
        }
    }
}

impl Inner {
    fn queue<T: Operation>(&mut self, operation: &mut T, waker: &Waker) -> io::Result<u64> {
        if self.ring.queued() == self.ring.sq_entries() {
            self.submit_queued()?;
        }
        let sqe = self
            .ring
            .next_sqe()
            .ok_or_else(|| io::Error::from_raw_os_error(libc::EBUSY))?;
        let mut entry = Entry {
            sqe,
            prepared: false,
        };
        operation.prepare(entry.sqe);
        debug_assert_eq!(entry.sqe.flags & FORBIDDEN_SQE_FLAGS, 0);
        entry.sqe.flags &= !FORBIDDEN_SQE_FLAGS;
        let user_data = self.table.insert(Some(waker.clone()));
        entry.sqe.user_data = user_data;
        entry.prepared = true;
        self.stats.operations += 1;
        Ok(user_data)
    }

    /// Queues cancellations for dropped operations, then enters the kernel,
    /// unless there is nothing to submit, no completion work pending and no
    /// need to wait.
    fn enter(
        &mut self,
        min_complete: u32,
        timeout: Option<&sys::__kernel_timespec>,
    ) -> io::Result<()> {
        self.queue_cancels()?;
        let pending = sys::IORING_SQ_TASKRUN | sys::IORING_SQ_CQ_OVERFLOW;
        if min_complete == 0 && self.ring.queued() == 0 && self.ring.sq_flags() & pending == 0 {
            return Ok(());
        }
        self.stats.enters += 1;
        // SAFETY: every queued entry was prepared by an `Operation`, whose
        // contract keeps the memory it points to valid while the operation
        // exists, and the table keeps the operation until its completion; or
        // it is an internal cancellation or no-op, which points to no memory.
        let entered = unsafe { self.ring.enter(min_complete, timeout) };
        match entered {
            Ok(_) => Ok(()),
            // Timed out, interrupted by a signal, or completions held back
            // until the queue has room: the caller delivers what is ready.
            Err(err)
                if matches!(
                    err.raw_os_error(),
                    Some(libc::ETIME | libc::EINTR | libc::EBUSY)
                ) =>
            {
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    /// Submits the queued entries, without waiting, to make room in the
    /// submission queue.
    fn submit_queued(&mut self) -> io::Result<()> {
        self.stats.enters += 1;
        // SAFETY: as in `Inner::enter`.
        let entered = unsafe { self.ring.enter(0, None) };
        if let Err(err) = entered
            && !matches!(err.raw_os_error(), Some(libc::EINTR | libc::EBUSY))
        {
            return Err(err);
        }
        if self.ring.queued() == self.ring.sq_entries() {
            return Err(io::Error::from_raw_os_error(libc::EBUSY));
        }
        Ok(())
    }

    fn queue_cancels(&mut self) -> io::Result<()> {
        while let Some(&user_data) = self.cancels.last() {
            let Some(sqe) = self.ring.next_sqe() else {
                self.submit_queued()?;
                continue;
            };
            sqe.opcode = sys::IORING_OP_ASYNC_CANCEL;
            sqe.addr = user_data;
            sqe.flags = sys::IOSQE_CQE_SKIP_SUCCESS;
            sqe.user_data = IGNORED;
            self.cancels.pop();
        }
        Ok(())
    }

    /// Queues the multishot poll on the unparkers' eventfd, unless it is in
    /// flight. The eventfd is never read: each write completes the poll once,
    /// and a poll armed while the eventfd is readable completes at once.
    fn arm_wake(&mut self) -> io::Result<()> {
        if self.wake_armed {
            return Ok(());
        }
        if self.ring.queued() == self.ring.sq_entries() {
            self.submit_queued()?;
        }
        let sqe = self
            .ring
            .next_sqe()
            .ok_or_else(|| io::Error::from_raw_os_error(libc::EBUSY))?;
        sqe.opcode = sys::IORING_OP_POLL_ADD;
        sqe.fd = self.notify.eventfd();
        sqe.op_flags = libc::POLLIN as u32;
        sqe.len = sys::IORING_POLL_ADD_MULTI;
        sqe.user_data = WAKE;
        self.wake_armed = true;
        Ok(())
    }

    /// Queues a cancellation of every request in flight.
    fn queue_cancel_all(&mut self) -> io::Result<()> {
        self.cancels.clear();
        if self.ring.queued() == self.ring.sq_entries() {
            self.submit_queued()?;
        }
        let sqe = self
            .ring
            .next_sqe()
            .ok_or_else(|| io::Error::from_raw_os_error(libc::EBUSY))?;
        sqe.opcode = sys::IORING_OP_ASYNC_CANCEL;
        sqe.op_flags = sys::IORING_ASYNC_CANCEL_ANY;
        sqe.flags = sys::IOSQE_CQE_SKIP_SUCCESS;
        sqe.user_data = IGNORED;
        Ok(())
    }

    /// Cancels everything in flight and waits, until the shutdown timeout,
    /// for the kernel to finish with it, finishing each orphaned operation as
    /// its completion arrives.
    fn drain(&mut self) {
        let deadline = Instant::now() + self.shutdown_timeout;
        if self.queue_cancel_all().is_err() {
            return;
        }
        let (mut wakers, mut released) = (Vec::new(), Vec::new());
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            // Once the time is up, one last pass collects what is ready.
            let min_complete = u32::from(!remaining.is_zero());
            if self
                .enter(min_complete, Some(&timespec(remaining)))
                .is_err()
            {
                break;
            }
            self.reap(&mut wakers, &mut released);
            for (payload, completion) in released.drain(..) {
                payload.finish(completion);
            }
            if self.table.in_flight() == 0 || remaining.is_zero() {
                break;
            }
        }
        // Only orphans are in flight: every waiting operation has an `Op`,
        // which keeps the driver alive.
        debug_assert!(wakers.is_empty());
    }

    /// Moves posted completions into the table, collecting the wakers to wake
    /// and the orphaned payloads to finish.
    fn reap(&mut self, wakers: &mut Vec<Waker>, released: &mut Vec<(Box<dyn Orphan>, Completion)>) {
        while let Some(cqe) = self.ring.pop_cqe() {
            self.stats.completions += 1;
            if cqe.user_data == WAKE {
                // An unparker's write. Without IORING_CQE_F_MORE the poll
                // has ended, and the next park arms another.
                if cqe.flags & sys::IORING_CQE_F_MORE == 0 {
                    self.wake_armed = false;
                }
                continue;
            }
            let completion = Completion::new(cqe.res, cqe.flags);
            match self.table.complete(cqe.user_data, completion) {
                Outcome::Wake(Some(waker)) => wakers.push(waker),
                Outcome::Wake(None) | Outcome::Ignore => {}
                Outcome::Release(payload) => released.push((payload, completion)),
            }
        }
        self.ring.release_cqes();
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if self.table.in_flight() > 0 {
            self.drain();
        }
        // Whatever the kernel may still be using cannot be freed. Leaking it
        // is safe; the ring is closed after this, and the kernel cancels what
        // is left.
        for payload in self.table.take_orphans() {
            mem::forget(payload);
        }
    }
}

impl fmt::Debug for Driver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("Driver");
        if let Ok(inner) = self.inner.try_borrow() {
            debug.field("in_flight", &inner.table.in_flight());
        }
        debug.finish_non_exhaustive()
    }
}

/// A submission queue entry being prepared. If preparation panics, the entry
/// becomes an ignored no-op, so that no half-prepared entry reaches the kernel
/// pointing at memory freed while unwinding.
struct Entry<'a> {
    sqe: &'a mut sys::io_uring_sqe,
    prepared: bool,
}

impl Drop for Entry<'_> {
    fn drop(&mut self) {
        if !self.prepared {
            *self.sqe = sys::io_uring_sqe {
                opcode: sys::IORING_OP_NOP,
                user_data: IGNORED,
                ..Default::default()
            };
        }
    }
}

pub(crate) fn timespec(duration: Duration) -> sys::__kernel_timespec {
    sys::__kernel_timespec {
        tv_sec: i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        tv_nsec: i64::from(duration.subsec_nanos()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    static STUCK_DROPPED: AtomicBool = AtomicBool::new(false);

    /// An operation the kernel never sees, standing in for one it never
    /// finishes. It has no size, so leaking it allocates nothing that a leak
    /// checker would report.
    struct Stuck;

    impl Drop for Stuck {
        fn drop(&mut self) {
            STUCK_DROPPED.store(true, Ordering::SeqCst);
        }
    }

    // SAFETY: never submitted; its entry would point to no memory anyway.
    unsafe impl Operation for Stuck {
        type Output = ();

        fn prepare(&mut self, _: &mut sys::io_uring_sqe) {}

        fn complete(self, _: Completion) {}
    }

    #[test]
    fn operations_the_kernel_never_finishes_are_leaked_at_shutdown() {
        let timeout = Duration::from_millis(50);
        let driver = Driver::builder().shutdown_timeout(timeout).build().unwrap();
        {
            let mut inner = driver.inner.borrow_mut();
            let user_data = inner.table.insert(None);
            let _ = inner.table.orphan(user_data, Box::new(Stuck));
        }

        let start = Instant::now();
        drop(driver);
        assert!(start.elapsed() >= timeout);
        assert!(
            !STUCK_DROPPED.load(Ordering::SeqCst),
            "freed while the kernel might still use it"
        );
    }
}
