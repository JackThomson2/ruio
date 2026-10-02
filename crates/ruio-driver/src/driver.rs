//! The driver: a ring, the operations in flight on it, and the loop that
//! submits them and delivers their completions.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::ffi::c_void;
use std::fmt;
use std::io;
use std::mem;
use std::os::fd::{IntoRawFd, OwnedFd, RawFd};
use std::panic::{self, AssertUnwindSafe};
use std::ptr;
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::Waker;
use std::time::{Duration, Instant};

use ruio_sys as sys;

use crate::Probe;
use crate::link::private::{Linkable, Member};
use crate::op::{Completion, Op, Operation, Orphan};
use crate::orphan::Payload;
use crate::remote::{Remote, RemoteRing};
use crate::ring::Ring;
use crate::stream::{MultiOp, Multishot, Stranded};
use crate::table::{IGNORED, Outcome, Table, Wake};
use crate::unpark::{Notify, Unparker};
use crate::waker::LocalWaker;

/// The `user_data` of the poll on the eventfd that unparkers write to.
const WAKE: u64 = IGNORED - 1;

/// The `user_data` of the timeout that wakes the ring at a deadline; see
/// [`Driver::wake_at`].
const TIMER: u64 = IGNORED - 2;

/// The time on the `CLOCK_MONOTONIC` clock, from its zero: the clock that
/// [`Driver::wake_at`] takes deadlines on, and that `Instant` counts on
/// Linux.
pub fn monotonic_now() -> Duration {
    let mut now = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: clock_gettime writes the one timespec it is given.
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut now) };
    Duration::new(
        u64::try_from(now.tv_sec).unwrap_or(0),
        u32::try_from(now.tv_nsec).unwrap_or(0),
    )
}

/// The setup flags of every ring. See DESIGN.md.
const SETUP_FLAGS: u32 = sys::IORING_SETUP_SINGLE_ISSUER
    | sys::IORING_SETUP_DEFER_TASKRUN
    | sys::IORING_SETUP_TASKRUN_FLAG
    | sys::IORING_SETUP_SUBMIT_ALL
    | sys::IORING_SETUP_CLAMP
    | sys::IORING_SETUP_NO_SQARRAY;

/// Flags an [`Operation`] may not put on its entry: they would give it more
/// or fewer than one completion, or chain it to an unrelated entry.
const FORBIDDEN_SQE_FLAGS: u8 =
    sys::IOSQE_IO_LINK | sys::IOSQE_IO_HARDLINK | sys::IOSQE_CQE_SKIP_SUCCESS;

/// The most slots a registered file table gets unless asked for more.
const DEFAULT_FILES: u32 = 4096;

/// The ID the next driver gets. IDs are never reused.
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    /// The drivers of this thread by ID, for resources that name their ring
    /// without keeping it alive, such as direct descriptors.
    static DRIVERS: RefCell<Vec<(u64, Weak<Shared>)>> = const { RefCell::new(Vec::new()) };
}

/// Configures and creates a [`Driver`].
#[derive(Clone, Debug)]
pub struct Builder {
    entries: u32,
    shutdown_timeout: Duration,
    /// The size of the registered file table, if set.
    files: Option<u32>,
}

impl Builder {
    /// A builder with the default configuration: 256 submission queue
    /// entries, a one-second shutdown timeout, and a registered file table
    /// of the default size.
    pub fn new() -> Self {
        Self {
            entries: 256,
            shutdown_timeout: Duration::from_secs(1),
            files: None,
        }
    }

    /// Sets the size of the submission queue. The kernel rounds it up to a
    /// power of two and caps it at 32768, and it is at least 2, so that an
    /// operation and its linked timeout fit; the completion queue is four
    /// times as large.
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

    /// Sets the number of slots in the ring's registered file table, which
    /// holds its direct descriptors ([`Fd::Direct`](crate::Fd::Direct)); 0
    /// leaves the ring without one.
    ///
    /// The kernel refuses a table larger than the process's soft
    /// `RLIMIT_NOFILE`, although direct descriptors do not count against
    /// it. Unless set, the table has 4096 slots, or as many as that limit
    /// allows if it is lower, and a ring whose table the kernel refuses goes
    /// without one.
    pub fn files(mut self, slots: u32) -> Self {
        self.files = Some(slots);
        self
    }

    /// Creates the driver.
    ///
    /// # Errors
    ///
    /// Fails with [`io::ErrorKind::Unsupported`] if the kernel is below ruio's
    /// baseline (see [`Probe::check_baseline`]), or with the system's error if
    /// the ring, or a file table of the size set by [`Builder::files`],
    /// cannot be created.
    pub fn build(&self) -> io::Result<Driver> {
        let probe = Probe::current()?;
        probe
            .check_baseline()
            .map_err(|err| io::Error::new(io::ErrorKind::Unsupported, err))?;
        let entries = self.entries.max(2);
        let cq_entries = entries.saturating_mul(4);
        let ring = Ring::new(entries, cq_entries, SETUP_FLAGS)?;
        let files = match self.files {
            Some(0) => 0,
            Some(slots) => {
                register_files(&ring, slots)?;
                slots
            }
            None => {
                let slots = DEFAULT_FILES.min(soft_nofile_limit());
                match register_files(&ring, slots) {
                    Ok(()) => slots,
                    Err(_) => 0,
                }
            }
        };
        let notify = Arc::new(Notify::new()?);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let remote = Remote::new(id, ring.raw_fd(), Unparker::new(Arc::clone(&notify)));
        let state = RefCell::new(Inner {
            id,
            table: Table::with_capacity(cq_entries as usize),
            ring,
            files,
            remote,
            wake_armed: false,
            timer: None,
            timer_at: Box::default(),
            timer_fired: false,
            next_group: 0,
            cancels: Vec::new(),
            wakers: Vec::new(),
            released: Vec::new(),
            deliveries: Vec::new(),
            last_turn: 0,
            stats: Stats::default(),
            shutdown_timeout: self.shutdown_timeout,
        });
        let inner = Rc::new(Shared { notify, state });
        DRIVERS.with(|drivers| {
            let mut drivers = drivers.borrow_mut();
            drivers.retain(|(_, driver)| driver.strong_count() > 0);
            drivers.push((id, Rc::downgrade(&inner)));
        });
        Ok(Driver { inner })
    }
}

/// Registers a table of `slots` empty slots for direct descriptors.
fn register_files(ring: &Ring, slots: u32) -> io::Result<()> {
    let mut table = sys::io_uring_rsrc_register {
        nr: slots,
        flags: sys::IORING_RSRC_REGISTER_SPARSE,
        ..Default::default()
    };
    let size = size_of::<sys::io_uring_rsrc_register>() as u32;
    // SAFETY: `table` is one valid registration for the kernel to read, and
    // a sparse table takes no memory of ours.
    unsafe {
        ring.register(
            sys::IORING_REGISTER_FILES2,
            ptr::from_mut(&mut table).cast(),
            size,
        )
    }?;
    Ok(())
}

/// The process's soft limit on open files, which also bounds the size of a
/// registered file table.
fn soft_nofile_limit() -> u32 {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `getrlimit` writes one `rlimit` to the pointer it is given.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return 0;
    }
    u32::try_from(limit.rlim_cur).unwrap_or(u32::MAX)
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
    inner: Rc<Shared>,
}

/// A handle to a driver that does not keep it alive, for resources that
/// outlive their use of the ring, such as registered memory, and would
/// otherwise keep the ring alive in a cycle through operations that hold
/// them.
#[derive(Clone)]
pub(crate) struct WeakDriver {
    inner: Weak<Shared>,
}

impl WeakDriver {
    pub(crate) fn upgrade(&self) -> Option<Driver> {
        self.inner.upgrade().map(|inner| Driver { inner })
    }
}

/// What every handle to a driver shares: the ring's state, behind the cell
/// that its one thread borrows for each use, and beside it what parking
/// reaches on every turn without borrowing anything.
struct Shared {
    /// What the driver shares with its unparkers.
    notify: Arc<Notify>,
    state: RefCell<Inner>,
}

struct Inner {
    /// The driver's ID, unique in the process.
    id: u64,
    ring: Ring,
    /// The number of slots in the registered file table.
    files: u32,
    /// Every operation the kernel may still own.
    table: Table<Payload>,
    /// What the driver shares with the threads that send it descriptors.
    remote: Arc<Remote>,
    /// Whether the multishot poll on the unparkers' eventfd is in flight.
    wake_armed: bool,
    /// The deadline the wake timer is armed for, while it is in flight, on
    /// `CLOCK_MONOTONIC`; see [`Driver::wake_at`].
    timer: Option<Duration>,
    /// Where the kernel reads the wake timer's deadline from as it takes an
    /// entry that arms or moves it: always the latest deadline.
    timer_at: Box<sys::__kernel_timespec>,
    /// Whether the wake timer has fired since [`Driver::timer_fired`] last
    /// said so.
    timer_fired: bool,
    /// The provided-buffer group ID the next pool gets.
    next_group: u16,
    /// Dropped operations to cancel, by `user_data`.
    cancels: Vec<u64>,
    /// Wakers of a batch of completions, kept to reuse the allocation.
    wakers: Vec<Waker>,
    /// Orphaned payloads released by a batch of completions, likewise.
    released: Vec<(Payload, Completion)>,
    /// Orphaned streams with completions to hand over, likewise.
    deliveries: Vec<u64>,
    /// How many completions the last turn delivered; see
    /// [`Driver::completions_last_turn`].
    last_turn: usize,
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

    /// Wraps a multishot request in a stream that queues it when first
    /// polled.
    pub fn stream<T: Multishot>(&self, request: T) -> MultiOp<T> {
        MultiOp::new(self.clone(), request)
    }

    /// Leaves `operations` to finish as a chain with nobody waiting for them:
    /// queues them linked, as a [`Link`](crate::Link) is queued, so that the
    /// kernel runs them in order, each once the one before has succeeded,
    /// and one that fails cancels the rest; and hands each to the driver, as
    /// [`Op::detach`] does, which drops its output once the kernel is done.
    /// Each operation's [`timeout`](Op::timeout) bounds it alone.
    ///
    /// If the chain cannot be queued, every operation completes with the
    /// error at once.
    ///
    /// # Panics
    ///
    /// If an operation has been submitted, or is on another ring.
    pub fn detach_chain<T: Operation>(&self, mut operations: Vec<Op<T>>) {
        assert!(
            operations.iter().all(Op::unsubmitted),
            "an operation can be linked only before it is submitted"
        );
        assert!(
            operations.iter().all(|op| self.same_ring(op.driver())),
            "linked operations must be on the same ring"
        );
        if operations.is_empty() {
            return;
        }
        let mut members: Vec<&mut dyn Member> = operations
            .iter_mut()
            .map(|op| op as &mut dyn Member)
            .collect();
        self.queue_chain(&mut members, Waiter::Nobody);
        // Each operation went into its slot as it was queued, leaving
        // nothing to detach; unless the chain was refused, when this
        // completes each with the error.
        for op in operations {
            op.detach();
        }
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

    /// Whether a [`park`](Driver::park) would return at once: the kernel has
    /// work for the ring, such as the completions of operations that have
    /// finished, which it runs when entered; completions are posted, or did
    /// not fit in the queue; or an [`Unparker`] has been used since the last
    /// park. A thread that would rather not sleep can poll this before it
    /// parks.
    pub fn has_work(&self) -> bool {
        let pending = sys::IORING_SQ_TASKRUN | sys::IORING_SQ_CQ_OVERFLOW;
        let inner = self.inner.state.borrow();
        inner.ring.sq_flags() & pending != 0
            || inner.ring.cq_ready()
            || self.inner.notify.is_notified()
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
        let notify = &self.inner.notify;
        if !notify.begin_park() {
            return self.enter(0, None);
        }
        let armed = self.inner.state.borrow_mut().arm_wake(notify.eventfd());
        let result = armed.and_then(|()| self.enter(1, timeout));
        notify.end_park();
        result
    }

    /// Makes sure the ring wakes by `deadline`, a time on the
    /// `CLOCK_MONOTONIC` clock (see [`monotonic_now`]), with a timeout of
    /// the kernel's: a park returns by then however long it would have
    /// waited, and [`timer_fired`](Driver::timer_fired) then says so.
    ///
    /// The ring has one such timer. A deadline earlier than the one it is
    /// armed for moves it, which takes an entry; a later one changes
    /// nothing, as waking early does no harm. A runtime, whose timers need
    /// the ring woken for the earliest only, so spares every park a timeout
    /// of its own, which the kernel would copy in on every wait.
    ///
    /// # Errors
    ///
    /// As for [`Driver::submit`], if the submission queue is full and
    /// submitting it fails.
    pub fn wake_at(&self, deadline: Duration) -> io::Result<()> {
        self.inner.state.borrow_mut().wake_at(deadline)
    }

    /// Whether the timer [`wake_at`](Driver::wake_at) armed has fired since
    /// this was last asked. Asking clears it.
    pub fn timer_fired(&self) -> bool {
        mem::take(&mut self.inner.state.borrow_mut().timer_fired)
    }

    /// Holds the wakers `local` describes, those of the executor on this
    /// thread, by a reference the executor counts rather than by a clone;
    /// see [`LocalWaker`].
    ///
    /// # Safety
    ///
    /// Every waker with `local.vtable` that reaches this driver must name a
    /// task of this thread, which `local.retain` keeps alive until the
    /// matching `local.release`; `local.wake` must wake it as
    /// `Waker::wake_by_ref` would, without using this driver.
    pub unsafe fn set_local_waker(&self, local: LocalWaker) {
        self.inner.state.borrow_mut().table.set_local(local);
    }

    /// A handle that wakes this driver from any thread.
    pub fn unparker(&self) -> Unparker {
        Unparker::new(Arc::clone(&self.inner.notify))
    }

    /// The driver's ring as other threads see it, for them to send direct
    /// descriptors to with [`DirectFd::send_to`](crate::DirectFd::send_to).
    pub fn remote(&self) -> RemoteRing {
        RemoteRing::new(Arc::clone(&self.inner.state.borrow().remote))
    }

    /// The number of operations the kernel may still own, including those
    /// whose [`Op`] was dropped.
    pub fn in_flight(&self) -> usize {
        self.inner.state.borrow().table.in_flight()
    }

    /// Counts of the driver's work so far.
    pub fn stats(&self) -> Stats {
        self.inner.state.borrow().stats
    }

    /// How many completions the last turn of the ring, a [`submit`] or a
    /// [`park`], delivered: a measure of the load on the thread. A thread
    /// that waits on one peer at a time, as a single connection's does, sees
    /// one completion a turn; a loaded one sees a batch.
    ///
    /// [`submit`]: Driver::submit
    /// [`park`]: Driver::park
    pub fn completions_last_turn(&self) -> usize {
        self.inner.state.borrow().last_turn
    }

    /// The number of slots in the ring's registered file table, which holds
    /// its direct descriptors; see [`Builder::files`].
    pub fn files(&self) -> u32 {
        self.inner.state.borrow().files
    }

    /// The number of entries in the ring's submission queue; see
    /// [`Builder::entries`]. A chain can have no more.
    pub fn sq_entries(&self) -> u32 {
        self.inner.state.borrow().ring.sq_entries()
    }

    /// A number that identifies the driver's ring among every ring the
    /// process has had.
    pub fn id(&self) -> u64 {
        self.inner.state.borrow().id
    }

    /// The driver of this thread with ID `id`, if it still exists.
    pub(crate) fn find(id: u64) -> Option<Driver> {
        DRIVERS
            .try_with(|drivers| {
                let drivers = drivers.try_borrow().ok()?;
                let (_, inner) = drivers.iter().find(|(driver, _)| *driver == id)?;
                inner.upgrade().map(|inner| Driver { inner })
            })
            .ok()
            .flatten()
    }

    /// Closes `fd`, a plain descriptor, with a request queued now: the kernel
    /// takes every operation queued before it, which may name the
    /// descriptor, before the number is free for another file. Dropping the
    /// descriptor instead closes it at once, and an operation still queued
    /// then reaches the kernel naming no file, or whichever file took the
    /// number meanwhile.
    ///
    /// Nothing reports how the close went, as nothing does for dropping the
    /// descriptor; [`ops::Close`](crate::ops::Close) does. If the driver is
    /// busy, as it is when code it runs closes a descriptor, or has no room
    /// for the request even after submitting, `fd` is closed at once.
    pub fn close_in_order(&self, fd: OwnedFd) {
        if let Ok(mut inner) = self.inner.state.try_borrow_mut() {
            // Without room for the request, the descriptor is dropped here.
            let _ = inner.queue_close_fd(fd);
        }
    }

    /// Frees slot `slot` of the registered file table, closing the direct
    /// descriptor in it, with a request queued now, so that it reaches the
    /// kernel before anything queued later.
    ///
    /// If the driver is busy, as it is when code it runs drops a direct
    /// descriptor, the slot stays taken until the ring is closed.
    pub(crate) fn close_direct(&self, slot: u32) {
        if let Ok(mut inner) = self.inner.state.try_borrow_mut() {
            // Without room even after submitting, the slot stays taken.
            let _ = inner.queue_close(slot);
        }
    }

    /// A provided-buffer group ID for a new pool.
    pub(crate) fn next_buffer_group(&self) -> u16 {
        let mut inner = self.inner.state.borrow_mut();
        let group = inner.next_group;
        inner.next_group = group.wrapping_add(1);
        group
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
            .state
            .try_borrow()
            .map_err(|_| io::Error::from_raw_os_error(libc::EBUSY))?;
        // SAFETY: the caller upholds the contract of `io_uring_register`.
        unsafe { inner.ring.register(opcode, arg, nr_args) }
    }

    fn enter(&self, min_complete: u32, timeout: Option<Duration>) -> io::Result<()> {
        let timeout = timeout.map(timespec);
        let (result, before) = {
            let mut inner = self.inner.state.borrow_mut();
            let before = inner.stats.completions;
            (inner.enter(min_complete, timeout.as_ref()), before)
        };
        self.deliver();
        // Completions that did not fit in the queue wait in the kernel, which
        // moves them in when entered again, now that there is room.
        while self.inner.state.borrow().ring.sq_flags() & sys::IORING_SQ_CQ_OVERFLOW != 0 {
            self.inner.state.borrow_mut().enter(0, None)?;
            self.deliver();
        }
        let mut inner = self.inner.state.borrow_mut();
        inner.last_turn = usize::try_from(inner.stats.completions - before).unwrap_or(usize::MAX);
        result
    }

    /// Processes the posted completions: wakes the tasks waiting for them and
    /// finishes orphaned operations. Both run code the driver does not
    /// control, which may use the driver, so they run after it is released.
    fn deliver(&self) {
        let (mut wakers, mut released, mut deliveries) = {
            let mut inner = self.inner.state.borrow_mut();
            let mut wakers = mem::take(&mut inner.wakers);
            let mut released = mem::take(&mut inner.released);
            let mut deliveries = mem::take(&mut inner.deliveries);
            inner.reap(&mut wakers, &mut released, &mut deliveries);
            (wakers, released, deliveries)
        };
        for waker in wakers.drain(..) {
            waker.wake();
        }
        for (mut payload, completion) in released.drain(..) {
            payload.deliver(completion);
        }
        for user_data in deliveries.drain(..) {
            self.deliver_stream(user_data);
        }
        let mut inner = self.inner.state.borrow_mut();
        inner.wakers = wakers;
        inner.released = released;
        // Anything abandoned meanwhile waits for the next delivery.
        deliveries.append(&mut inner.deliveries);
        inner.deliveries = deliveries;
    }

    /// Hands an orphaned stream's payload the completions queued for it,
    /// outside the driver, since the payload runs code of its own.
    fn deliver_stream(&self, user_data: u64) {
        loop {
            let taken = self
                .inner
                .state
                .borrow_mut()
                .table
                .take_deliveries(user_data);
            let Some((mut payload, completions, ended)) = taken else {
                return;
            };
            if !hand_over(&mut payload, completions) {
                // The kernel may still write where the payload points.
                mem::forget(payload);
                return;
            }
            if ended {
                return;
            }
            let adopted = self
                .inner
                .state
                .borrow_mut()
                .table
                .adopt(user_data, payload);
            match adopted {
                Ok(true) => {}
                Ok(false) => return,
                Err(payload) => {
                    drop(payload);
                    return;
                }
            }
        }
    }

    /// Queues an operation's entry, followed by a linked timeout if it has
    /// one, returning its `user_data`.
    pub(crate) fn queue<T: Operation>(
        &self,
        operation: &mut T,
        timeout: Option<Duration>,
        waker: &Waker,
    ) -> io::Result<u64> {
        self.inner
            .state
            .borrow_mut()
            .queue_entry(
                operation,
                |operation, sqe| operation.prepare(sqe),
                timeout,
                |table, _| table.insert(Some(waker)),
            )
            .map_err(|(err, _)| err)
    }

    /// Queues an operation's entry, followed by a linked timeout if it has
    /// one, with nobody waiting for it: the operation goes into its slot in
    /// the table, which drops its output once the kernel is done. If the
    /// entry cannot be queued, the operation comes back with the error.
    pub(crate) fn detach<T: Operation>(
        &self,
        operation: T,
        timeout: Option<Duration>,
    ) -> Result<(), (io::Error, T)> {
        self.inner
            .state
            .borrow_mut()
            .queue_entry(
                operation,
                |operation, sqe| operation.prepare(sqe),
                timeout,
                |table, operation| table.insert_orphan(Payload::operation(operation)),
            )
            .map(|_| ())
    }

    /// Queues the operations of a chain, each linked to the next, for
    /// `waiter`, and records in each its `user_data`, or, if the chain
    /// cannot be queued, the error; see [`Link`](crate::Link).
    pub(crate) fn queue_chain(&self, members: &mut [&mut dyn Member], waiter: Waiter<'_>) {
        self.inner.state.borrow_mut().queue_chain(members, waiter);
    }

    /// Whether `other` drives the same ring.
    pub(crate) fn same_ring(&self, other: &Driver) -> bool {
        Rc::ptr_eq(&self.inner, &other.inner)
    }

    /// Queues a multishot request's entry, returning its `user_data`.
    pub(crate) fn queue_stream<T: Multishot>(
        &self,
        request: &mut T,
        waker: &Waker,
    ) -> io::Result<u64> {
        self.inner
            .state
            .borrow_mut()
            .queue_entry(
                request,
                |request, sqe| request.prepare(sqe),
                None,
                |table, _| table.insert_stream(Some(waker)),
            )
            .map_err(|(err, _)| err)
    }

    /// Takes a stream's next completion, or arranges for `waker` to be woken
    /// when one arrives.
    pub(crate) fn poll_stream(&self, user_data: u64, waker: &Waker) -> Option<Completion> {
        let polled = self
            .inner
            .state
            .borrow_mut()
            .table
            .poll_stream(user_data, waker);
        // A replaced waker is dropped here, after the driver is released.
        polled.ok()
    }

    /// Takes a stream's next completion if it has arrived, without
    /// arranging for anything to be woken.
    pub(crate) fn take_stream(&self, user_data: u64) -> Option<Completion> {
        self.inner.state.borrow_mut().table.take_stream(user_data)
    }

    /// Takes over the request of a stream dropped while armed: hands it the
    /// completions the stream had not taken, cancels it, and keeps it until
    /// the kernel is done.
    pub(crate) fn abandon<T: Multishot>(&self, user_data: u64, request: T) {
        let Ok(mut inner) = self.inner.state.try_borrow_mut() else {
            // Dropped by code running inside the driver; see `orphan`.
            mem::forget(request);
            return;
        };
        let Some(abandoned) = inner.table.abandon(user_data) else {
            // The last completion was taken, so the kernel is done with it.
            drop(inner);
            drop(request);
            return;
        };
        if !abandoned.ended {
            inner.cancels.push(user_data);
        }
        drop(inner);
        drop(abandoned.waker);
        let mut payload = Payload::boxed(Stranded(request));
        if !hand_over(&mut payload, abandoned.completions) {
            mem::forget(payload);
            return;
        }
        if abandoned.ended {
            return;
        }
        let mut inner = self.inner.state.borrow_mut();
        match inner.table.adopt(user_data, payload) {
            Ok(true) => inner.deliveries.push(user_data),
            Ok(false) => {}
            Err(payload) => {
                drop(inner);
                drop(payload);
            }
        }
    }

    /// Takes the operation's completion, or arranges for `waker` to be woken
    /// when it arrives.
    pub(crate) fn poll_completion(&self, user_data: u64, waker: &Waker) -> Option<Completion> {
        let polled = self.inner.state.borrow_mut().table.poll(user_data, waker);
        // A replaced waker is dropped here, after the driver is released.
        polled.ok()
    }

    /// Takes over the operation of an `Op` dropped while in flight, and
    /// cancels it unless `cancel` is false, when it runs to completion.
    pub(crate) fn orphan<T: Operation>(&self, user_data: u64, operation: T, cancel: bool) {
        let Ok(mut inner) = self.inner.state.try_borrow_mut() else {
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
                let waker = inner.table.orphan(user_data, Payload::operation(operation));
                if cancel {
                    inner.cancels.push(user_data);
                }
                drop(inner);
                drop(waker);
            }
        }
    }
}

/// Who waits for the operations of a chain as they are queued.
pub(crate) enum Waiter<'a> {
    /// The task with this waker, which each operation's completion wakes.
    Task(&'a Waker),
    /// Nobody: each operation goes into its slot as it is queued, and
    /// finishes there, as a detached one does.
    Nobody,
}

/// Hands a payload its completions, in order. Returns `false` if it panicked,
/// in which case the payload must not be freed: the kernel may still use
/// memory it owns.
fn hand_over(payload: &mut Payload, completions: VecDeque<Completion>) -> bool {
    panic::catch_unwind(AssertUnwindSafe(|| {
        for completion in completions {
            payload.deliver(completion);
        }
    }))
    .is_ok()
}

impl Inner {
    /// Queues an entry that `prepare` fills in for `carried`, followed by a
    /// linked timeout if `timeout` is set, in the table slot `occupy` takes,
    /// given `carried` to keep there, and returns the entry's `user_data`.
    /// If the entry cannot be queued, `carried` comes back with the error.
    ///
    /// The linked timeout posts no completion when it is cancelled because
    /// the entry completed first (`IOSQE_CQE_SKIP_SUCCESS`), and any it does
    /// post is ignored: the entry's own completion tells whether it timed
    /// out.
    fn queue_entry<C>(
        &mut self,
        mut carried: C,
        prepare: impl FnOnce(&mut C, &mut sys::io_uring_sqe),
        timeout: Option<Duration>,
        occupy: impl FnOnce(&mut Table<Payload>, C) -> u64,
    ) -> Result<u64, (io::Error, C)> {
        // An entry and its linked timeout reach the kernel together.
        let needed = if timeout.is_some() { 2 } else { 1 };
        if self.ring.sq_space() < needed
            && let Err(err) = self.submit_queued(needed)
        {
            return Err((err, carried));
        }
        let Some(sqe) = self.ring.next_sqe() else {
            return Err((io::Error::from_raw_os_error(libc::EBUSY), carried));
        };
        let mut entry = Entry {
            sqe,
            prepared: false,
        };
        prepare(&mut carried, entry.sqe);
        debug_assert_eq!(entry.sqe.flags & FORBIDDEN_SQE_FLAGS, 0);
        entry.sqe.flags &= !FORBIDDEN_SQE_FLAGS;
        if timeout.is_some() {
            entry.sqe.flags |= sys::IOSQE_IO_LINK;
        }
        let user_data = occupy(&mut self.table, carried);
        entry.sqe.user_data = user_data;
        entry.prepared = true;
        drop(entry);
        if let Some(timeout) = timeout {
            // The room was made above, and nothing has taken it since.
            let link = self
                .ring
                .next_link_timeout(timespec(timeout))
                .expect("no room for a linked timeout");
            link.flags = sys::IOSQE_CQE_SKIP_SUCCESS;
            link.user_data = IGNORED;
        }
        self.stats.operations += 1;
        Ok(user_data)
    }

    /// Queues a chain's operations, each followed by its linked timeout if
    /// it has one, and each but the last linked to what follows; see
    /// [`Driver::queue_chain`].
    ///
    /// The kernel links only entries submitted together, so the chain gets
    /// room for all of its entries before any is queued. Every entry is
    /// prepared, and every wake taken, before any reaches the queue, so
    /// that a panic in an operation's `prepare`, or a waker's `clone`,
    /// leaves nothing queued that points to memory the panic frees.
    fn queue_chain(&mut self, members: &mut [&mut dyn Member], waiter: Waiter<'_>) {
        let needed = members
            .iter()
            .map(|member| if member.timeout().is_some() { 2 } else { 1 })
            .sum();
        if self.ring.sq_space() < needed
            && let Err(err) = self.submit_queued(needed)
        {
            for member in members {
                member.refused(&err);
            }
            return;
        }
        let entries: Vec<(sys::io_uring_sqe, Option<Wake>)> = members
            .iter_mut()
            .map(|member| {
                let mut sqe = sys::io_uring_sqe::default();
                member.prepare(&mut sqe);
                debug_assert_eq!(sqe.flags & FORBIDDEN_SQE_FLAGS, 0);
                sqe.flags &= !FORBIDDEN_SQE_FLAGS;
                let wake = match waiter {
                    Waiter::Task(waker) => Some(self.table.wake_of(waker)),
                    Waiter::Nobody => None,
                };
                (sqe, wake)
            })
            .collect();
        let last = members.len() - 1;
        for (index, (member, (entry, wake))) in members.iter_mut().zip(entries).enumerate() {
            let timeout = member.timeout();
            // The room was made above, and nothing has taken it since.
            let sqe = self.ring.next_sqe().expect("no room for a linked entry");
            *sqe = entry;
            if timeout.is_some() || index < last {
                sqe.flags |= sys::IOSQE_IO_LINK;
            }
            let user_data = match wake {
                Some(wake) => self.table.insert_wake(wake),
                None => self.table.insert_orphan(member.detached(index == 0)),
            };
            sqe.user_data = user_data;
            if let Some(timeout) = timeout {
                let link = self
                    .ring
                    .next_link_timeout(timespec(timeout))
                    .expect("no room for a linked timeout");
                // A timeout in the middle of a chain carries the link on.
                link.flags = sys::IOSQE_CQE_SKIP_SUCCESS;
                if index < last {
                    link.flags |= sys::IOSQE_IO_LINK;
                }
                link.user_data = IGNORED;
            }
            if matches!(waiter, Waiter::Task(_)) {
                member.queued(user_data, index == 0);
            }
            self.stats.operations += 1;
        }
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
        // Descriptors sent here, which other threads dropped unclaimed.
        for slot in self.remote.take_closes() {
            self.queue_close(slot)?;
        }
        let pending = sys::IORING_SQ_TASKRUN | sys::IORING_SQ_CQ_OVERFLOW;
        if min_complete == 0 && self.ring.queued() == 0 && self.ring.sq_flags() & pending == 0 {
            return Ok(());
        }
        self.stats.enters += 1;
        // SAFETY: every queued entry was prepared by an `Operation`, whose
        // contract keeps the memory it points to valid while the operation
        // exists, and the table keeps the operation until its completion; or
        // it is an internal cancellation or no-op, which points to no memory;
        // or a linked timeout, whose duration the kernel copies as it
        // consumes the entry and the ring keeps until then; or the wake
        // timer's, whose deadline the kernel copies likewise, from memory the
        // driver keeps.
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

    /// Submits the queued entries, without waiting, to make room for
    /// `needed` more in the submission queue.
    fn submit_queued(&mut self, needed: u32) -> io::Result<()> {
        self.stats.enters += 1;
        // SAFETY: as in `Inner::enter`.
        let entered = unsafe { self.ring.enter(0, None) };
        if let Err(err) = entered
            && !matches!(err.raw_os_error(), Some(libc::EINTR | libc::EBUSY))
        {
            return Err(err);
        }
        if self.ring.sq_space() < needed {
            return Err(io::Error::from_raw_os_error(libc::EBUSY));
        }
        Ok(())
    }

    fn queue_cancels(&mut self) -> io::Result<()> {
        while let Some(&user_data) = self.cancels.last() {
            let Some(sqe) = self.ring.next_sqe() else {
                self.submit_queued(1)?;
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

    /// Arms the wake timer for `deadline`, or moves it there if it is armed
    /// for later; see [`Driver::wake_at`].
    fn wake_at(&mut self, deadline: Duration) -> io::Result<()> {
        if self.timer.is_some_and(|armed| armed <= deadline) {
            return Ok(());
        }
        // An entry queued for an earlier deadline, and not yet submitted,
        // reads this one instead, which is as well.
        *self.timer_at = timespec(deadline);
        let at = (&raw const *self.timer_at).expose_provenance() as u64;
        let moving = self.timer.is_some();
        let sqe = loop {
            match self.ring.next_sqe() {
                Some(sqe) => break sqe,
                None => self.submit_queued(1)?,
            }
        };
        if moving {
            // Should the timer fire first, the update finds nothing to
            // move, and fails into the ignored `user_data`.
            sqe.opcode = sys::IORING_OP_TIMEOUT_REMOVE;
            sqe.addr = TIMER;
            sqe.off = at;
            sqe.op_flags = sys::IORING_TIMEOUT_UPDATE | sys::IORING_TIMEOUT_ABS;
            sqe.flags = sys::IOSQE_CQE_SKIP_SUCCESS;
            sqe.user_data = IGNORED;
        } else {
            sqe.opcode = sys::IORING_OP_TIMEOUT;
            sqe.addr = at;
            sqe.len = 1;
            sqe.op_flags = sys::IORING_TIMEOUT_ABS;
            sqe.user_data = TIMER;
        }
        self.timer = Some(deadline);
        Ok(())
    }

    /// Queues the multishot poll on `eventfd`, the unparkers', unless it is
    /// in flight. The eventfd is never read: each write completes the poll
    /// once, and a poll armed while the eventfd is readable completes at
    /// once.
    fn arm_wake(&mut self, eventfd: RawFd) -> io::Result<()> {
        if self.wake_armed {
            return Ok(());
        }
        if self.ring.sq_space() == 0 {
            self.submit_queued(1)?;
        }
        let sqe = self
            .ring
            .next_sqe()
            .ok_or_else(|| io::Error::from_raw_os_error(libc::EBUSY))?;
        sqe.opcode = sys::IORING_OP_POLL_ADD;
        sqe.fd = eventfd;
        sqe.op_flags = libc::POLLIN as u32;
        sqe.len = sys::IORING_POLL_ADD_MULTI;
        sqe.user_data = WAKE;
        self.wake_armed = true;
        Ok(())
    }

    /// Queues a close of the direct descriptor in `slot`, which frees the
    /// slot.
    fn queue_close(&mut self, slot: u32) -> io::Result<()> {
        if self.ring.sq_space() == 0 {
            self.submit_queued(1)?;
        }
        let sqe = self
            .ring
            .next_sqe()
            .ok_or_else(|| io::Error::from_raw_os_error(libc::EBUSY))?;
        sqe.opcode = sys::IORING_OP_CLOSE;
        // Slots count from 1 here; 0 would mean a plain descriptor.
        sqe.file_index = slot + 1;
        sqe.flags = sys::IOSQE_CQE_SKIP_SUCCESS;
        sqe.user_data = IGNORED;
        Ok(())
    }

    /// Queues a close of the plain descriptor `fd`; see
    /// [`Driver::close_in_order`]. Hands it back if there is no room.
    fn queue_close_fd(&mut self, fd: OwnedFd) -> Result<(), OwnedFd> {
        if self.ring.sq_space() == 0 && self.submit_queued(1).is_err() {
            return Err(fd);
        }
        let Some(sqe) = self.ring.next_sqe() else {
            return Err(fd);
        };
        sqe.opcode = sys::IORING_OP_CLOSE;
        // From here the kernel closes the descriptor.
        sqe.fd = fd.into_raw_fd();
        sqe.flags = sys::IOSQE_CQE_SKIP_SUCCESS;
        sqe.user_data = IGNORED;
        Ok(())
    }

    /// Queues a cancellation of every request in flight.
    fn queue_cancel_all(&mut self) -> io::Result<()> {
        self.cancels.clear();
        if self.ring.sq_space() == 0 {
            self.submit_queued(1)?;
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
        let (mut wakers, mut released, mut deliveries) = (Vec::new(), Vec::new(), Vec::new());
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
            self.reap(&mut wakers, &mut released, &mut deliveries);
            for (mut payload, completion) in released.drain(..) {
                payload.deliver(completion);
            }
            deliveries.append(&mut self.deliveries);
            for user_data in deliveries.drain(..) {
                self.deliver_stream(user_data);
            }
            if self.table.in_flight() == 0 || remaining.is_zero() {
                break;
            }
        }
        // Only orphans are in flight: every waiting operation has an `Op`,
        // which keeps the driver alive.
        debug_assert!(wakers.is_empty());
    }

    /// Hands an orphaned stream's payload its completions while the driver is
    /// being dropped, when nothing else can reach it; see
    /// [`Driver::deliver_stream`].
    fn deliver_stream(&mut self, user_data: u64) {
        while let Some((mut payload, completions, ended)) = self.table.take_deliveries(user_data) {
            if !hand_over(&mut payload, completions) {
                mem::forget(payload);
                return;
            }
            if ended {
                return;
            }
            match self.table.adopt(user_data, payload) {
                Ok(true) => {}
                Ok(false) => return,
                Err(payload) => {
                    drop(payload);
                    return;
                }
            }
        }
    }

    /// Moves posted completions into the table, collecting the wakers to wake,
    /// the orphaned payloads to finish and the orphaned streams to deliver
    /// to.
    fn reap(
        &mut self,
        wakers: &mut Vec<Waker>,
        released: &mut Vec<(Payload, Completion)>,
        deliveries: &mut Vec<u64>,
    ) {
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
            if cqe.user_data == TIMER {
                // The wake timer has ended: fired, or cancelled as the ring
                // shuts down.
                self.timer = None;
                self.timer_fired |= cqe.res == -libc::ETIME;
                continue;
            }
            let completion = Completion::new(cqe.res, cqe.flags);
            match self.table.complete(cqe.user_data, completion) {
                Outcome::Wake(Some(waker)) => wakers.push(waker),
                Outcome::Wake(None) | Outcome::Ignore => {}
                Outcome::Release(payload) => released.push((payload, completion)),
                Outcome::Deliver(user_data) => deliveries.push(user_data),
            }
        }
        self.ring.release_cqes();
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Before the ring closes its descriptor, which senders duplicate.
        self.remote.close();
        if self.table.in_flight() > 0 {
            self.drain();
        }
        // Whatever the kernel may still be using cannot be freed. Leaking it
        // is safe; the ring is closed after this, and the kernel cancels what
        // is left.
        for payload in self.table.take_orphans() {
            mem::forget(payload);
        }
        let id = self.id;
        let _ = DRIVERS.try_with(|drivers| {
            if let Ok(mut drivers) = drivers.try_borrow_mut() {
                drivers.retain(|&(driver, _)| driver != id);
            }
        });
    }
}

impl fmt::Debug for Driver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("Driver");
        if let Ok(inner) = self.inner.state.try_borrow() {
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

    /// The payload of an operation the kernel never sees, standing in for one
    /// it never finishes. It has no size, so leaking it allocates nothing
    /// that a leak checker would report.
    struct Stuck;

    impl Drop for Stuck {
        fn drop(&mut self) {
            STUCK_DROPPED.store(true, Ordering::SeqCst);
        }
    }

    impl Orphan for Stuck {
        fn deliver(&mut self, _: Completion) {}
    }

    // SAFETY: the entry is never filled in.
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
            // One payload in its slot, one in a box.
            let mut inner = driver.inner.state.borrow_mut();
            inner.table.insert_orphan(Payload::operation(Stuck));
            inner.table.insert_orphan(Payload::boxed(Stuck));
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
