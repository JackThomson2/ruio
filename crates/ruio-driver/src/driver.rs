//! The driver: a ring, the operations in flight on it, and the loop that
//! submits them and delivers their completions; and a second ring, which
//! polls its devices for completions, for the operations that ask for it.

use std::cell::{Cell, RefCell};
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
use crate::foreign::{Relayed, Ticket};
use crate::link::private::{Linkable, Member};
use crate::op::{Completion, Op, Operation, Orphan};
use crate::ops::InstallPolled;
use crate::orphan::Payload;
use crate::portable;
use crate::remote::{Remote, RemoteRing, Request};
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

/// What a file table update installs to empty a slot; see
/// [`Inner::queue_release_polled`].
static NO_FILE: i32 = -1;

/// The ID the next driver gets. IDs are never reused.
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    /// The drivers of this thread by ID, for resources that name their ring
    /// without keeping it alive, such as direct descriptors.
    static DRIVERS: RefCell<Vec<(u64, Weak<Shared>)>> = const { RefCell::new(Vec::new()) };

    /// The thread's current driver, as [`Driver::enter`] made it, holding a
    /// strong reference to it; null if none is.
    static CURRENT: Cell<*const Shared> = const { Cell::new(ptr::null()) };
}

/// Configures and creates a [`Driver`].
#[derive(Clone, Debug)]
pub struct Builder {
    entries: u32,
    shutdown_timeout: Duration,
    /// The size of the registered file table, if set.
    files: Option<u32>,
    /// Whether the polled ring is created with the driver.
    polled: bool,
}

impl Builder {
    /// A builder with the default configuration: 256 submission queue
    /// entries, a one-second shutdown timeout, a registered file table of
    /// the default size, and no polled ring until an operation asks for
    /// one.
    pub fn new() -> Self {
        Self {
            entries: 256,
            shutdown_timeout: Duration::from_secs(1),
            files: None,
            polled: false,
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

    /// Creates the polled ring with the driver, as
    /// [`Driver::enable_polling`] would, rather than at the first polled
    /// operation.
    pub fn polled(mut self, polled: bool) -> Self {
        self.polled = polled;
        self
    }

    /// Creates the driver.
    ///
    /// # Errors
    ///
    /// Fails with [`io::ErrorKind::Unsupported`] if the kernel is below ruio's
    /// baseline (see [`Probe::check_baseline`]), or with the system's error if
    /// the ring, or a file table of the size set by [`Builder::files`],
    /// cannot be created; and as for [`Driver::enable_polling`] with
    /// [`Builder::polled`].
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
        let polled = if self.polled {
            Some(Polled::new(&ring, entries, cq_entries, files, false)?)
        } else {
            None
        };
        let notify = Arc::new(Notify::new()?);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let remote = Remote::new(id, ring.raw_fd(), Unparker::new(Arc::clone(&notify)));
        let state = RefCell::new(Inner {
            id,
            table: Table::with_capacity(cq_entries as usize),
            ring,
            polled,
            entries,
            cq_entries,
            buffers: false,
            files,
            remote,
            wake_armed: false,
            timer_at: Box::default(),
            next_group: 0,
            cancels: Vec::new(),
            wakers: Vec::new(),
            released: Vec::new(),
            deliveries: Vec::new(),
            taken_closes: Vec::new(),
            taken_requests: Vec::new(),
            last_turn: 0,
            stats: Stats::default(),
            shutdown_timeout: self.shutdown_timeout,
        });
        let inner = Rc::new(Shared {
            id,
            notify,
            timer: Timer::default(),
            state,
        });
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
    /// Operations queued for submission, on either ring.
    pub operations: u64,
    /// Operations queued on the polled ring, counted in `operations` too.
    pub polled: u64,
    /// Calls to `io_uring_enter(2)`, on either ring.
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
/// Operations that ask to be [polled](Operation::polled), direct reads and
/// writes of files on devices with poll queues, go on a second ring, which
/// polls the devices for their completions rather than wait for interrupts;
/// see [`Driver::enable_polling`].
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

/// Makes current again, as it is dropped, the driver that was the thread's
/// current one before [`Driver::enter`] made another so. It cannot leave its
/// thread.
#[must_use = "the driver is the thread's current one only until the guard is dropped"]
pub struct Entered {
    /// The driver current before, whose strong reference the guard holds
    /// until it gives it back to the thread; null if none was.
    previous: *const Shared,
}

impl Drop for Entered {
    fn drop(&mut self) {
        // A thread whose locals are gone keeps both references for good.
        let Ok(entered) = CURRENT.try_with(|current| current.replace(self.previous)) else {
            return;
        };
        if !entered.is_null() {
            // SAFETY: the thread held this strong reference while the driver
            // was current, and lets go of it once, here. Dropped once the
            // thread's driver is the one before, as the last reference runs
            // the driver's drop, which may look for the current one.
            drop(unsafe { Rc::from_raw(entered) });
        }
    }
}

impl fmt::Debug for Entered {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Entered").finish_non_exhaustive()
    }
}

/// What every handle to a driver shares: the ring's state, behind the cell
/// that its one thread borrows for each use, and beside it what parking and
/// the runtime's timers reach on every turn without borrowing anything.
struct Shared {
    /// The driver's ID, which portable operations compare without
    /// borrowing the state.
    id: u64,
    /// What the driver shares with its unparkers.
    notify: Arc<Notify>,
    timer: Timer,
    state: RefCell<Inner>,
}

/// The wake timer's state; see [`Driver::wake_at`].
#[derive(Default)]
struct Timer {
    /// The deadline the timer is armed for, while it is in flight, on
    /// `CLOCK_MONOTONIC`.
    armed: Cell<Option<Duration>>,
    /// Whether the timer has fired since [`Driver::timer_fired`] last said
    /// so.
    fired: Cell<bool>,
}

struct Inner {
    /// The driver's ID, unique in the process.
    id: u64,
    ring: Ring,
    /// The ring that polled operations run on, once one has asked for it;
    /// see [`Driver::enable_polling`].
    polled: Option<Polled>,
    /// The size of the main ring's queues, which the polled ring takes too.
    entries: u32,
    cq_entries: u32,
    /// Whether the main ring has buffers registered, for the polled ring to
    /// share.
    buffers: bool,
    /// The number of slots in the registered file table.
    files: u32,
    /// Every operation the kernel may still own.
    table: Table<Payload>,
    /// What the driver shares with the threads that send it descriptors.
    remote: Arc<Remote>,
    /// Whether the multishot poll on the unparkers' eventfd is in flight.
    wake_armed: bool,
    /// Where the kernel reads the wake timer's deadline from as it takes an
    /// entry that arms or moves it: always the latest deadline.
    timer_at: Box<sys::__kernel_timespec>,
    /// The provided-buffer group ID the next pool gets.
    next_group: u16,
    /// Dropped operations to cancel, by `user_data`.
    cancels: Vec<u64>,
    /// Wakers of a batch of completions, other than the executor's, which
    /// the table wakes itself: woken once the driver is released, as waking
    /// can run code that uses it. Kept between batches, for the room.
    wakers: Vec<Waker>,
    /// Orphaned payloads released by a batch of completions, likewise.
    released: Vec<(Payload, Completion)>,
    /// Orphaned streams with completions to hand over, likewise.
    deliveries: Vec<u64>,
    /// The room of what other threads asked of the ring last turn, kept
    /// for the next; see [`Remote::take`].
    taken_closes: Vec<u32>,
    taken_requests: Vec<Request>,
    /// How many completions the last turn delivered; see
    /// [`Driver::completions_last_turn`].
    last_turn: usize,
    /// Counts of the driver's work, but for its entries into the kernel,
    /// which the rings count.
    stats: Stats,
    shutdown_timeout: Duration,
}

/// The ring that polled operations run on, set up with
/// `IORING_SETUP_IOPOLL`: the kernel completes its requests only as the
/// ring is entered, by polling the devices they went to, so the driver
/// enters it on every turn while it has operations in flight, and a turn
/// that waits with any in flight waits by polling; see [`Inner::enter`].
///
/// The kernel takes only reads, writes, passthrough commands, no-ops and
/// file table updates on such a ring, so the main ring keeps the timer, the
/// unparkers' poll, the cancellations and everything else; completions are
/// routed by `user_data` through the one table, as the main ring's are. The
/// ring has a file table of its own, as large as the main ring's, which
/// [`Driver::install_polled`] puts files into.
struct Polled {
    ring: Ring,
    /// Entries queued or submitted whose completions have not been reaped.
    /// Every entry on the ring posts one completion: none is linked,
    /// multishot or marked to skip its completion.
    pending: u32,
}

impl Polled {
    /// Creates the polled ring, with queues as large as `main`'s and a file
    /// table of `files` slots, sharing `main`'s registered buffers if
    /// `buffers` says it has any.
    fn new(
        main: &Ring,
        entries: u32,
        cq_entries: u32,
        files: u32,
        buffers: bool,
    ) -> io::Result<Self> {
        if !Probe::current()?.supports_setup_flags(sys::IORING_SETUP_IOPOLL) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "the kernel's io_uring has no polled rings",
            ));
        }
        let ring = Ring::new(entries, cq_entries, SETUP_FLAGS | sys::IORING_SETUP_IOPOLL)?;
        if files > 0 {
            register_files(&ring, files)?;
        }
        if buffers {
            clone_buffers(main, &ring)?;
        }
        Ok(Self { ring, pending: 0 })
    }
}

/// Shares the registered buffers of `from` with `to`
/// (`IORING_REGISTER_CLONE_BUFFERS`, Linux 6.12): `to` gets a reference to
/// each, at the same index, and the memory stays registered until both have
/// let go of it.
fn clone_buffers(from: &Ring, to: &Ring) -> io::Result<()> {
    let mut clone = sys::io_uring_clone_buffers {
        src_fd: from.raw_fd().cast_unsigned(),
        ..Default::default()
    };
    // SAFETY: `clone` is one valid argument for the kernel to read, and
    // cloning takes no memory of ours.
    let cloned = unsafe {
        to.register(
            sys::IORING_REGISTER_CLONE_BUFFERS,
            ptr::from_mut(&mut clone).cast(),
            1,
        )
    };
    match cloned {
        Ok(_) => Ok(()),
        // An older kernel refuses the opcode.
        Err(err) if err.raw_os_error() == Some(libc::EINVAL) => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "the kernel cannot share registered buffers with a polled ring",
        )),
        Err(err) => Err(err),
    }
}

/// What a turn that waits brings [`Inner::enter`] from outside the state,
/// for waiting with polled operations in flight: the unparkers'
/// notification, and whether the wake timer is armed.
struct Park<'a> {
    notify: &'a Notify,
    timer_armed: bool,
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
    #[inline]
    pub fn submit(&self) -> io::Result<()> {
        self.turn(0, None, false)
    }

    /// Whether a [`park`](Driver::park) would return at once: the kernel has
    /// work for the ring, such as the completions of operations that have
    /// finished, which it runs when entered; completions are posted, or did
    /// not fit in the queue; or an [`Unparker`] has been used since the last
    /// park. A thread that would rather not sleep can poll this before it
    /// parks.
    #[inline]
    pub fn has_work(&self) -> bool {
        let pending = sys::IORING_SQ_TASKRUN | sys::IORING_SQ_CQ_OVERFLOW;
        let inner = self.inner.state.borrow();
        inner.ring.sq_flags() & pending != 0
            || inner.ring.cq_ready()
            || inner.polled_cq_ready()
            || self.inner.notify.is_notified()
    }

    /// Submits the queued operations, waits until at least one completion is
    /// ready, an [`Unparker`] is used or `timeout` passes, and delivers the
    /// completions.
    ///
    /// If an unpark arrived since the last park, this returns without
    /// waiting. With polled operations in flight the wait polls their
    /// devices rather than sleep, and, when nothing but a polled operation
    /// can complete, polls inside the kernel until one does, which holds an
    /// unpark back until then; see DESIGN.md.
    ///
    /// # Errors
    ///
    /// As for [`Driver::submit`]. Timing out is not an error.
    pub fn park(&self, timeout: Option<Duration>) -> io::Result<()> {
        let notify = &self.inner.notify;
        if !notify.begin_park() {
            return self.turn(0, None, false);
        }
        let result = self.turn(1, timeout, true);
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
    #[inline]
    pub fn wake_at(&self, deadline: Duration) -> io::Result<()> {
        // Nearly every turn finds the timer armed for the deadline already.
        let armed = &self.inner.timer.armed;
        if armed.get().is_some_and(|armed| armed <= deadline) {
            return Ok(());
        }
        self.inner.state.borrow_mut().arm_timer(deadline, armed)
    }

    /// Whether the timer [`wake_at`](Driver::wake_at) armed has fired since
    /// this was last asked. Asking clears it.
    #[inline]
    pub fn timer_fired(&self) -> bool {
        self.inner.timer.fired.take()
    }

    /// Holds the wakers `local` describes, those of the executor on this
    /// thread, by a reference the executor counts rather than by a clone;
    /// see [`LocalWaker`].
    ///
    /// # Safety
    ///
    /// Every waker with `local.vtable` that reaches this driver must name a
    /// live task. `local.retain` must return `true` only if `local.release`,
    /// `local.wake` and `local.wake_release` may then use the task from this
    /// thread, and must then keep it alive until the matching
    /// `local.release` or `local.wake_release`; `local.wake` must wake it as
    /// `Waker::wake_by_ref` would, and `local.wake_release` as `local.wake`
    /// and then `local.release` would, without using this driver.
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
    #[inline]
    pub fn in_flight(&self) -> usize {
        self.inner.state.borrow().table.in_flight()
    }

    /// Counts of the driver's work so far.
    #[inline]
    pub fn stats(&self) -> Stats {
        let inner = self.inner.state.borrow();
        let polled = inner
            .polled
            .as_ref()
            .map_or(0, |polled| polled.ring.enters());
        Stats {
            enters: inner.ring.enters() + polled,
            ..inner.stats
        }
    }

    /// Creates the ring that [polled](Operation::polled) operations run on,
    /// unless the driver has one. The first polled operation creates it
    /// otherwise, and fails with the error if it cannot be created.
    ///
    /// The ring is set up with `IORING_SETUP_IOPOLL`, with queues as large as
    /// the main ring's, and shares the main ring's registered buffers
    /// (`IORING_REGISTER_CLONE_BUFFERS`, Linux 6.12), those registered later
    /// included, so a [`ReadFixed`](crate::ops::ReadFixed) can be polled. It
    /// has a file table of its own, as large as the main ring's (see
    /// [`Builder::files`]), which a polled operation's
    /// [`Fd::Direct`](crate::Fd::Direct) names a slot of:
    /// [`Driver::install_polled`] puts a file there.
    ///
    /// # Errors
    ///
    /// `Unsupported` if the kernel's io_uring has no polled rings, or cannot
    /// share registered buffers with one; `EBUSY` from code running inside
    /// the driver; or what creating the ring, or its file table, reports.
    pub fn enable_polling(&self) -> io::Result<()> {
        let mut inner = self
            .inner
            .state
            .try_borrow_mut()
            .map_err(|_| io::Error::from_raw_os_error(libc::EBUSY))?;
        inner.polled_ring().map(drop)
    }

    /// Puts the file open as `fd`, a plain descriptor, into a free slot of
    /// the polled ring's file table, with an `IORING_OP_FILES_UPDATE` on
    /// that ring, which the first poll of the future queues; the output is
    /// a [`PolledFd`](crate::PolledFd), which polled operations on the file
    /// name as their [`Fd`](crate::Fd). The slot holds a reference to the
    /// file of its own, so `fd` may close once the kernel has the entry, as
    /// for any operation. The ring is created if the driver has none yet,
    /// as by [`enable_polling`](Driver::enable_polling), whose errors the
    /// operation then completes with.
    ///
    /// The operation fails with `ENFILE` if the table is full, and `ENXIO`
    /// if the ring has none ([`Builder::files`] set to 0).
    pub fn install_polled(&self, fd: RawFd) -> Op<InstallPolled> {
        self.op(InstallPolled::new(self.id(), fd))
    }

    /// Empties slot `slot` of the polled ring's file table, which
    /// [`install_polled`](Driver::install_polled) filled, with an update
    /// queued on that ring now, so that it reaches the kernel after the
    /// operations queued before it, which may name the slot.
    ///
    /// If the driver is busy, as it is when code it runs drops a
    /// [`PolledFd`](crate::PolledFd), or has no room for the update even
    /// after submitting, the slot stays taken until the ring is closed.
    pub(crate) fn release_polled(&self, slot: u32) {
        if let Ok(mut inner) = self.inner.state.try_borrow_mut() {
            let _ = inner.queue_release_polled(slot);
        }
    }

    /// How many completions the last turn of the ring, a [`submit`] or a
    /// [`park`], delivered: a measure of the load on the thread. A thread
    /// that waits on one peer at a time, as a single connection's does, sees
    /// one completion a turn; a loaded one sees a batch.
    ///
    /// [`submit`]: Driver::submit
    /// [`park`]: Driver::park
    #[inline]
    pub fn completions_last_turn(&self) -> usize {
        self.inner.state.borrow().last_turn
    }

    /// The number of slots in the ring's registered file table, which holds
    /// its direct descriptors; see [`Builder::files`].
    #[inline]
    pub fn files(&self) -> u32 {
        self.inner.state.borrow().files
    }

    /// The number of entries in the ring's submission queue; see
    /// [`Builder::entries`]. A chain can have no more.
    #[inline]
    pub fn sq_entries(&self) -> u32 {
        self.inner.state.borrow().ring.sq_entries()
    }

    /// A number that identifies the driver's ring among every ring the
    /// process has had.
    #[inline]
    pub fn id(&self) -> u64 {
        self.inner.id
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

    /// Makes this driver the thread's current one until the guard is
    /// dropped: the ring on which [`PortableOp`](crate::PortableOp)s first
    /// polled on this thread queue their operations. A runtime enters its
    /// driver while it runs. Guards nest: dropping one makes current again
    /// the driver that was before it.
    pub fn enter(&self) -> Entered {
        let entered = Rc::into_raw(Rc::clone(&self.inner));
        let previous = CURRENT.with(|current| current.replace(entered));
        Entered { previous }
    }

    /// The thread's current driver, if it has one; see [`Driver::enter`].
    #[inline]
    pub fn current() -> Option<Driver> {
        let current = CURRENT.try_with(Cell::get).ok()?;
        if current.is_null() {
            return None;
        }
        // SAFETY: it is the thread's current driver.
        Some(unsafe { Driver::from_current(current) })
    }

    /// The driver with ID `id`, if it is on this thread: the current one, as
    /// it nearly always is for an operation queued on it, which takes one
    /// comparison to find, or another of the thread's.
    #[inline]
    pub(crate) fn home(id: u64) -> Option<Driver> {
        if let Ok(current) = CURRENT.try_with(Cell::get)
            && !current.is_null()
            // SAFETY: the thread holds a strong reference to its current
            // driver, so the pointer names a live shared part.
            && unsafe { (*current).id } == id
        {
            // SAFETY: it is the thread's current driver.
            return Some(unsafe { Driver::from_current(current) });
        }
        Driver::find(id)
    }

    /// A new handle to the thread's current driver, whose shared part
    /// `current` names.
    ///
    /// # Safety
    ///
    /// `current` must be what `CURRENT` holds, and not null.
    #[inline]
    unsafe fn from_current(current: *const Shared) -> Driver {
        // SAFETY: the thread holds a strong reference to its current driver,
        // so the pointer names a live shared part, as `Rc::into_raw` gave
        // it; the count raised is the new handle's.
        unsafe { Rc::increment_strong_count(current) };
        // SAFETY: as above.
        let inner = unsafe { Rc::from_raw(current) };
        Driver { inner }
    }

    /// Closes `fd`, a plain descriptor, with a request queued now: the kernel
    /// takes every operation queued before it, which may name the
    /// descriptor, before the number is free for another file. Dropping the
    /// descriptor instead closes it at once, and an operation still queued
    /// then reaches the kernel naming no file, or whichever file took the
    /// number meanwhile. What other threads have asked of the ring, which
    /// may send it an entry naming the descriptor, is taken first, and goes
    /// before the close.
    ///
    /// Nothing reports how the close went, as nothing does for dropping the
    /// descriptor; [`ops::Close`](crate::ops::Close) does. If the driver is
    /// busy, as it is when code it runs closes a descriptor, or has no room
    /// for the request even after submitting, `fd` is closed at once.
    pub fn close_in_order(&self, fd: OwnedFd) {
        if let Ok(mut inner) = self.inner.state.try_borrow_mut() {
            // What taking them leaves to deliver goes at the next turn, which
            // it keeps from waiting.
            let _ = inner.take_remote();
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
    /// Buffers registered or unregistered this way are shared with the
    /// polled ring, if there is one.
    ///
    /// # Errors
    ///
    /// `EBUSY` if called from code running inside the driver, or what the
    /// kernel reports; buffers the polled ring cannot share are unregistered
    /// again, and the error is the sharing's.
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
        let mut inner = self
            .inner
            .state
            .try_borrow_mut()
            .map_err(|_| io::Error::from_raw_os_error(libc::EBUSY))?;
        // SAFETY: the caller upholds the contract of `io_uring_register`.
        let registered = unsafe { inner.ring.register(opcode, arg, nr_args) }?;
        inner.share_buffers(opcode)?;
        Ok(registered)
    }

    /// Turns the ring: submits what is queued, and enters the kernel to wait
    /// for `min_complete` completions or until `timeout`, with the poll on
    /// the unparkers' eventfd armed first for a turn that `parks`; then
    /// takes the completions. The driver is borrowed once for all of it,
    /// unless the completions have wakers other than the executor's to
    /// wake, orphans to finish or streams to hand over to, which run code
    /// that may use the driver, and so run once it is released.
    fn turn(&self, min_complete: u32, timeout: Option<Duration>, parks: bool) -> io::Result<()> {
        let shared = &*self.inner;
        let mut inner = shared.state.borrow_mut();
        let before = inner.stats.completions;
        let mut result = if parks {
            inner.arm_wake(shared.notify.eventfd())
        } else {
            Ok(())
        };
        if result.is_ok() {
            let park = Park {
                notify: &shared.notify,
                timer_armed: shared.timer.armed.get().is_some(),
            };
            result = inner.enter(min_complete, timeout, Some(&park));
        }
        loop {
            inner.reap(&shared.timer);
            if inner.has_deliveries() {
                drop(inner);
                self.deliver();
                inner = shared.state.borrow_mut();
            }
            // Completions that did not fit in the queue wait in the kernel,
            // which moves them in when entered again, now that there is room.
            if !inner.overflowed() {
                break;
            }
            inner.enter(0, None, None)?;
        }
        inner.last_turn = usize::try_from(inner.stats.completions - before).unwrap_or(usize::MAX);
        result
    }

    /// Wakes the wakers, finishes the orphans and hands the streams the
    /// completions that reaping collected for them: code the driver does
    /// not control, which may use the driver, so it runs while the driver
    /// is not borrowed.
    fn deliver(&self) {
        let (mut wakers, mut released, mut deliveries) = {
            let mut inner = self.inner.state.borrow_mut();
            (
                mem::take(&mut inner.wakers),
                mem::take(&mut inner.released),
                mem::take(&mut inner.deliveries),
            )
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
        let polled = operation.polled();
        self.inner
            .state
            .borrow_mut()
            .queue_entry(
                operation,
                polled,
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
        let polled = operation.polled();
        self.inner
            .state
            .borrow_mut()
            .queue_entry(
                operation,
                polled,
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
    #[inline]
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
                false,
                |request, sqe| request.prepare(sqe),
                None,
                |table, _| table.insert_stream(Some(waker)),
            )
            .map_err(|(err, _)| err)
    }

    /// Takes a stream's next completion, or arranges for `waker` to be woken
    /// when one arrives.
    #[inline]
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
    #[inline]
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
    #[inline]
    pub(crate) fn poll_completion(&self, user_data: u64, waker: &Waker) -> Option<Completion> {
        let polled = self.inner.state.borrow_mut().table.poll(user_data, waker);
        // A replaced waker is dropped here, after the driver is released.
        polled.ok()
    }

    /// Takes over the operation of an `Op` dropped while in flight, and
    /// cancels it unless `cancel` is false, when it runs to completion. A
    /// polled operation runs to completion in any case: the kernel takes no
    /// cancellation on the polled ring, and the device finishes the
    /// operation regardless.
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
                let cancel = cancel && !operation.polled();
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
    ///
    /// A `polled` entry goes on the polled ring, created if the driver has
    /// none yet, where the kernel takes no linked timeout: such an entry
    /// with a timeout is refused with `EOPNOTSUPP`.
    fn queue_entry<C>(
        &mut self,
        mut carried: C,
        polled: bool,
        prepare: impl FnOnce(&mut C, &mut sys::io_uring_sqe),
        timeout: Option<Duration>,
        occupy: impl FnOnce(&mut Table<Payload>, C) -> u64,
    ) -> Result<u64, (io::Error, C)> {
        // An entry and its linked timeout reach the kernel together.
        let needed = if timeout.is_some() { 2 } else { 1 };
        if polled {
            if timeout.is_some() {
                return Err((io::Error::from_raw_os_error(libc::EOPNOTSUPP), carried));
            }
            if let Err(err) = self.polled_ring() {
                return Err((err, carried));
            }
        }
        if self.ring_of(polled).sq_space() < needed
            && let Err(err) = self.submit_queued(needed, polled)
        {
            return Err((err, carried));
        }
        let Inner {
            ring,
            polled: polled_ring,
            table,
            stats,
            ..
        } = self;
        let mut unpolled = 0;
        let (ring, pending) = match polled_ring.as_mut() {
            Some(polled_ring) if polled => (&mut polled_ring.ring, &mut polled_ring.pending),
            _ => (ring, &mut unpolled),
        };
        let Some(sqe) = ring.next_sqe() else {
            return Err((io::Error::from_raw_os_error(libc::EBUSY), carried));
        };
        // Counted before it is prepared: an entry whose preparation panics
        // reaches the kernel as a no-op, which posts a completion too.
        *pending += 1;
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
        let opcode = entry.sqe.opcode;
        let user_data = occupy(table, carried);
        entry.sqe.user_data = user_data;
        entry.prepared = true;
        drop(entry);
        if let Some(timeout) = timeout {
            // The room was made above, and nothing has taken it since.
            let link = ring
                .next_link_timeout(timespec(timeout))
                .expect("no room for a linked timeout");
            link.flags = sys::IOSQE_CQE_SKIP_SUCCESS;
            link.user_data = IGNORED;
        }
        ring.note_queued(opcode);
        stats.operations += 1;
        stats.polled += u64::from(polled);
        Ok(user_data)
    }

    /// The ring entries go on: the polled ring if `polled`, which must exist
    /// by then, and the main ring otherwise.
    #[inline]
    fn ring_of(&mut self, polled: bool) -> &mut Ring {
        match &mut self.polled {
            Some(polled_ring) if polled => &mut polled_ring.ring,
            _ => &mut self.ring,
        }
    }

    /// The polled ring, created if the driver has none yet; see
    /// [`Driver::enable_polling`].
    fn polled_ring(&mut self) -> io::Result<&mut Polled> {
        if self.polled.is_none() {
            let polled = Polled::new(
                &self.ring,
                self.entries,
                self.cq_entries,
                self.files,
                self.buffers,
            )?;
            self.polled = Some(polled);
        }
        Ok(self.polled.as_mut().expect("made above"))
    }

    /// Queues, on the polled ring, the update that empties slot `slot` of
    /// its file table; see [`Driver::release_polled`]. The entry's
    /// completion, which nothing waits for, is counted as every polled
    /// entry's is.
    fn queue_release_polled(&mut self, slot: u32) -> io::Result<()> {
        if self.polled.is_none() {
            return Err(io::Error::from_raw_os_error(libc::ENXIO));
        }
        if self.ring_of(true).sq_space() == 0 {
            self.submit_queued(1, true)?;
        }
        let polled = self.polled.as_mut().expect("checked above");
        let sqe = polled
            .ring
            .next_sqe()
            .ok_or_else(|| io::Error::from_raw_os_error(libc::EBUSY))?;
        polled.pending += 1;
        sqe.opcode = sys::IORING_OP_FILES_UPDATE;
        // The kernel reads the descriptor to install as it takes the entry,
        // from memory that lives as long as the program.
        sqe.addr = ptr::from_ref(&NO_FILE).expose_provenance() as u64;
        sqe.len = 1;
        sqe.off = u64::from(slot);
        sqe.user_data = IGNORED;
        Ok(())
    }

    /// Whether the polled ring has operations in flight, whose completions
    /// only polling finds.
    #[inline]
    fn polling(&self) -> bool {
        self.polled
            .as_ref()
            .is_some_and(|polled| polled.pending > 0)
    }

    /// Whether the polled ring has posted completions that have not been
    /// reaped.
    #[inline]
    fn polled_cq_ready(&self) -> bool {
        self.polled
            .as_ref()
            .is_some_and(|polled| polled.ring.cq_ready())
    }

    /// Whether either ring has completions that did not fit in its queue
    /// waiting in the kernel.
    #[inline]
    fn overflowed(&self) -> bool {
        let overflow = |ring: &Ring| ring.sq_flags() & sys::IORING_SQ_CQ_OVERFLOW != 0;
        overflow(&self.ring)
            || self
                .polled
                .as_ref()
                .is_some_and(|polled| overflow(&polled.ring))
    }

    /// Keeps the polled ring's registered buffers the main ring's, after a
    /// registration with `opcode` on the main ring: buffers registered are
    /// shared with it, and buffers unregistered are unregistered there too.
    /// Buffers the polled ring cannot share are unregistered from the main
    /// ring again, with the sharing's error.
    fn share_buffers(&mut self, opcode: u32) -> io::Result<()> {
        match opcode {
            sys::IORING_REGISTER_BUFFERS | sys::IORING_REGISTER_BUFFERS2 => {
                self.buffers = true;
                if let Some(polled) = &self.polled
                    && let Err(err) = clone_buffers(&self.ring, &polled.ring)
                {
                    self.buffers = false;
                    // SAFETY: unregistering takes no memory.
                    let _ = unsafe {
                        self.ring
                            .register(sys::IORING_UNREGISTER_BUFFERS, ptr::null_mut(), 0)
                    };
                    return Err(err);
                }
            }
            sys::IORING_UNREGISTER_BUFFERS => {
                self.buffers = false;
                if let Some(polled) = &self.polled {
                    // SAFETY: unregistering takes no memory.
                    let _ = unsafe {
                        polled
                            .ring
                            .register(sys::IORING_UNREGISTER_BUFFERS, ptr::null_mut(), 0)
                    };
                }
            }
            _ => {}
        }
        Ok(())
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
        // The kernel links nothing to a polled operation's entry: the
        // polled ring takes no timeout, and a chain cannot span rings.
        if members.iter().any(|member| member.polled()) {
            let err = io::Error::from_raw_os_error(libc::EOPNOTSUPP);
            for member in members {
                member.refused(&err);
            }
            return;
        }
        let needed = members
            .iter()
            .map(|member| if member.timeout().is_some() { 2 } else { 1 })
            .sum();
        if self.ring.sq_space() < needed
            && let Err(err) = self.submit_queued(needed, false)
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
            let opcode = entry.opcode;
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
            self.ring.note_queued(opcode);
            self.stats.operations += 1;
        }
    }

    /// Queues cancellations for dropped operations, then enters the kernel:
    /// the main ring, unless there is nothing to submit, no completion work
    /// pending and no need to wait; and the polled ring, if it has
    /// operations in flight, whose completions only polling finds.
    ///
    /// A turn that waits with polled operations in flight waits by polling,
    /// without blocking on the main ring; see [`Inner::poll_for`].
    fn enter(
        &mut self,
        min_complete: u32,
        timeout: Option<Duration>,
        park: Option<&Park<'_>>,
    ) -> io::Result<()> {
        // What other threads asked of the ring: closing descriptors sent
        // here that they dropped unclaimed, and taking back portable
        // operations, which can add cancellations.
        self.take_remote()?;
        // An operation taken back may have completed already, and what it
        // leaves to deliver waits for no completion: a turn that would wait
        // for one delivers it instead, as it ends.
        let min_complete = if self.has_deliveries() {
            0
        } else {
            min_complete
        };
        self.queue_cancels()?;
        if !self.polling() {
            return self.enter_main(min_complete, timeout);
        }
        // The main ring is entered for what it has to submit or run, without
        // waiting; the polled ring submits what it has and makes one pass.
        self.enter_main(0, None)?;
        self.enter_polled(0)?;
        if min_complete == 0 || self.has_completions(park) {
            return Ok(());
        }
        self.poll_for(timeout, park)
    }

    /// Waits, with polled operations in flight, for a completion on either
    /// ring, an unpark or `timeout`, by polling.
    ///
    /// When nothing but a polled operation can complete, every operation in
    /// flight being one, the wake timer not armed and there being no
    /// timeout, the polled ring is entered to wait for a completion, which
    /// polls the devices inside the kernel until one comes, the scheduler
    /// wants the CPU or a signal arrives: one entry into the kernel however
    /// long the device takes. An unpark that arrives meanwhile waits until
    /// then. Otherwise the polled ring makes a pass at a time, an entry
    /// into the kernel each, and between passes the main ring is looked at
    /// without entering it: for completion work the kernel has pending
    /// (`IORING_SQ_TASKRUN`), which a timer's firing or an unparker's write
    /// leaves, for completions posted, and for the unparkers' flag.
    fn poll_for(&mut self, timeout: Option<Duration>, park: Option<&Park<'_>>) -> io::Result<()> {
        let polled_pending = self.polled.as_ref().map_or(0, |polled| polled.pending);
        let only_polled = timeout.is_none()
            && park.is_some_and(|park| !park.timer_armed)
            && self.table.in_flight() == polled_pending as usize;
        if only_polled {
            self.enter_polled(1)?;
        } else {
            let deadline = timeout.map(|timeout| Instant::now() + timeout);
            loop {
                self.enter_polled(0)?;
                if self.has_completions(park)
                    || deadline.is_some_and(|deadline| Instant::now() >= deadline)
                {
                    break;
                }
            }
        }
        // Completion work the main ring has pending runs as it is entered.
        let pending = sys::IORING_SQ_TASKRUN | sys::IORING_SQ_CQ_OVERFLOW;
        if self.ring.sq_flags() & pending != 0 {
            self.enter_main(0, None)?;
        }
        Ok(())
    }

    /// Whether a turn that waits has something to deliver: completions
    /// posted on either ring, completion work pending on the main ring, or
    /// an unpark, if the turn `park`s.
    #[inline]
    fn has_completions(&self, park: Option<&Park<'_>>) -> bool {
        let pending = sys::IORING_SQ_TASKRUN | sys::IORING_SQ_CQ_OVERFLOW;
        self.polled_cq_ready()
            || self.ring.cq_ready()
            || self.ring.sq_flags() & pending != 0
            || park.is_some_and(|park| park.notify.is_notified())
    }

    /// Enters the main ring, unless there is nothing to submit, no
    /// completion work pending and no need to wait.
    fn enter_main(&mut self, min_complete: u32, timeout: Option<Duration>) -> io::Result<()> {
        let pending = sys::IORING_SQ_TASKRUN | sys::IORING_SQ_CQ_OVERFLOW;
        if min_complete == 0 && self.ring.queued() == 0 && self.ring.sq_flags() & pending == 0 {
            return Ok(());
        }
        let timeout = timeout.map(timespec);
        // SAFETY: every queued entry was prepared by an `Operation`, whose
        // contract keeps the memory it points to valid while the operation
        // exists, and the table keeps the operation until its completion; or
        // it is an internal cancellation or no-op, which points to no memory;
        // or a linked timeout, whose duration the kernel copies as it
        // consumes the entry and the ring keeps until then; or the wake
        // timer's, whose deadline the kernel copies likewise, from memory the
        // driver keeps.
        let entered = unsafe { self.ring.enter(min_complete, timeout.as_ref()) };
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

    /// Enters the polled ring, if there is one: submits what is queued on
    /// it, and polls its devices, once if `min_complete` is 0 and until
    /// that many completions otherwise; see [`Ring::enter`].
    fn enter_polled(&mut self, min_complete: u32) -> io::Result<()> {
        let Some(polled) = &mut self.polled else {
            return Ok(());
        };
        // SAFETY: every queued entry was prepared by an `Operation`, as in
        // `Inner::enter_main`, or is a no-op; the ring has no timeouts.
        let entered = unsafe { polled.ring.enter(min_complete, None) };
        match entered {
            Ok(_) => Ok(()),
            // Interrupted by a signal, or completions held back until the
            // queue has room: the caller delivers what is ready.
            Err(err) if matches!(err.raw_os_error(), Some(libc::EINTR | libc::EBUSY)) => Ok(()),
            Err(err) => Err(err),
        }
    }

    /// Submits the queued entries of the main ring, or of the polled ring if
    /// `polled`, without waiting, to make room for `needed` more in its
    /// submission queue.
    fn submit_queued(&mut self, needed: u32, polled: bool) -> io::Result<()> {
        let ring = self.ring_of(polled);
        // SAFETY: as in `Inner::enter_main`.
        let entered = unsafe { ring.enter(0, None) };
        if let Err(err) = entered
            && !matches!(err.raw_os_error(), Some(libc::EINTR | libc::EBUSY))
        {
            return Err(err);
        }
        if ring.sq_space() < needed {
            return Err(io::Error::from_raw_os_error(libc::EBUSY));
        }
        Ok(())
    }

    fn queue_cancels(&mut self) -> io::Result<()> {
        while let Some(&user_data) = self.cancels.last() {
            let Some(sqe) = self.ring.next_sqe() else {
                self.submit_queued(1, false)?;
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

    /// Queues the entry that arms the wake timer for `deadline`, or moves it
    /// there from the deadline `armed` holds, which then holds `deadline`;
    /// see [`Driver::wake_at`].
    fn arm_timer(&mut self, deadline: Duration, armed: &Cell<Option<Duration>>) -> io::Result<()> {
        // An entry queued for an earlier deadline, and not yet submitted,
        // reads this one instead, which is as well.
        *self.timer_at = timespec(deadline);
        let at = (&raw const *self.timer_at).expose_provenance() as u64;
        let moving = armed.get().is_some();
        let sqe = loop {
            match self.ring.next_sqe() {
                Some(sqe) => break sqe,
                None => self.submit_queued(1, false)?,
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
        armed.set(Some(deadline));
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
            self.submit_queued(1, false)?;
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
            self.submit_queued(1, false)?;
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
        if self.ring.sq_space() == 0 && self.submit_queued(1, false).is_err() {
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

    /// Takes what other threads asked of the ring since its last turn:
    /// closes of direct descriptors sent here and dropped there, requests
    /// for its portable operations and descriptors, and the entries of
    /// operations they send it.
    #[inline]
    fn take_remote(&mut self) -> io::Result<()> {
        // A plain load on every turn of the ring, which almost always finds
        // nothing.
        if !self.remote.is_pending() {
            return Ok(());
        }
        self.take_pending()
    }

    /// Takes and does what other threads asked, as
    /// [`take_remote`](Inner::take_remote) found them to have, into the
    /// vectors kept from the last time, which keep their room.
    #[cold]
    fn take_pending(&mut self) -> io::Result<()> {
        let mut closes = mem::take(&mut self.taken_closes);
        let mut requests = mem::take(&mut self.taken_requests);
        self.remote.take(&mut closes, &mut requests);
        for request in requests.drain(..) {
            self.handle(request);
        }
        let mut queued = Ok(());
        for slot in closes.drain(..) {
            // Without room even after submitting, the rest stay taken.
            if queued.is_ok() {
                queued = self.queue_close(slot);
            }
        }
        self.taken_closes = closes;
        self.taken_requests = requests;
        queued
    }

    /// Does what another thread asked for a portable operation of the
    /// ring's, whose future left this thread, or for a descriptor whose
    /// operations ran here, or queues the entry it sent; see [`Request`].
    /// What runs code of its own, a payload finishing or a waker, waits
    /// until the driver is released.
    fn handle(&mut self, request: Request) {
        match request {
            Request::Adopt {
                user_data,
                payload,
                cancel,
            } => {
                let payload = Payload::sent(payload);
                if let Some(completion) = self.table.take_completed(user_data) {
                    self.released.push((payload, completion));
                } else if self.table.is_waiting(user_data) {
                    // The waker of whoever polled the operation here last,
                    // which no longer waits for it: woken for nothing.
                    self.wakers.extend(self.table.orphan(user_data, payload));
                    if cancel {
                        self.cancels.push(user_data);
                    }
                } else {
                    // Only the future of the operation in the slot hands it
                    // over, once, so this cannot happen. The kernel may
                    // still use the payload, which leaking keeps safe.
                    debug_assert!(false, "adopted an operation that is not in flight");
                    mem::forget(payload);
                }
            }
            Request::Cancel { user_data } => {
                // Gone already if it has completed, as is the slot.
                if self.table.is_orphaned(user_data) {
                    self.cancels.push(user_data);
                }
            }
            Request::Close(fd) => {
                // Without room for the entry, even once what is queued is
                // submitted, the descriptor closes at once.
                let _ = self.queue_close_fd(fd);
            }
            Request::Submit {
                sqe,
                polled,
                timeout,
                ticket,
            } => self.queue_sent(sqe, polled, timeout, &ticket),
        }
    }

    /// Queues `sqe`, the entry of an operation another thread prepared, as
    /// an orphan whose completion goes to `ticket`: its future, which keeps
    /// the operation, takes the completion there. A future that went before
    /// the entry was queued has it cancelled at once, as dropping an `Op`
    /// whose entry is queued does; one refused, for want of room, completes
    /// with the error once the driver is released.
    fn queue_sent(
        &mut self,
        sqe: sys::io_uring_sqe,
        polled: bool,
        timeout: Option<Duration>,
        ticket: &Arc<Ticket>,
    ) {
        let mut refused = None;
        let queued = ticket.queue(|| {
            self.queue_entry(
                Relayed(Arc::clone(ticket)),
                polled,
                |_, entry| *entry = sqe,
                timeout,
                |table, relayed| table.insert_orphan(Payload::small(relayed)),
            )
            .map_err(|(err, relayed)| {
                refused = Some(relayed);
                err
            })
        });
        match queued {
            // The kernel takes no cancellation on the polled ring.
            Ok((user_data, cancel)) => {
                if cancel && !polled {
                    self.cancels.push(user_data);
                }
            }
            Err(err) => {
                if let Some(relayed) = refused {
                    let completion = Completion::from_error(&err);
                    self.released.push((Payload::small(relayed), completion));
                }
            }
        }
    }

    /// Queues a cancellation of every request in flight.
    fn queue_cancel_all(&mut self) -> io::Result<()> {
        self.cancels.clear();
        if self.ring.sq_space() == 0 {
            self.submit_queued(1, false)?;
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

    /// Cancels everything in flight on the main ring, and waits, until the
    /// shutdown timeout, for the kernel to finish with it and with the
    /// polled ring's operations, which their devices finish, finishing each
    /// orphaned operation as its completion arrives.
    fn drain(&mut self) {
        let deadline = Instant::now() + self.shutdown_timeout;
        if self.queue_cancel_all().is_err() {
            return;
        }
        // Wakers that a reap which panicked part way left behind: their
        // operations completed, and nobody waits for them now, every waiting
        // operation having an `Op`, which keeps the driver alive.
        drop(mem::take(&mut self.wakers));
        // Nothing asks about the wake timer any more.
        let timer = Timer::default();
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            // Once the time is up, one last pass collects what is ready.
            let min_complete = u32::from(!remaining.is_zero());
            if self.enter(min_complete, Some(remaining), None).is_err() {
                break;
            }
            self.reap(&timer);
            for (mut payload, completion) in mem::take(&mut self.released) {
                payload.deliver(completion);
            }
            for user_data in mem::take(&mut self.deliveries) {
                self.deliver_stream(user_data);
            }
            if self.table.in_flight() == 0 || remaining.is_zero() {
                break;
            }
        }
        // Only orphans are in flight: every waiting operation has an `Op`,
        // which keeps the driver alive.
        debug_assert!(self.wakers.is_empty());
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

    /// Whether reaping collected anything to deliver once the driver is
    /// released: wakers other than the executor's, orphans to finish or
    /// streams to hand over to.
    #[inline]
    fn has_deliveries(&self) -> bool {
        !(self.wakers.is_empty() && self.released.is_empty() && self.deliveries.is_empty())
    }

    /// Moves the posted completions into the table, all that each ring's
    /// queue held as the reap began: wakes the executor's tasks that wait
    /// for them, and collects the other wakers to wake, the orphaned
    /// payloads to finish and the orphaned streams to deliver to. The wake
    /// timer's completion goes to `timer`.
    fn reap(&mut self, timer: &Timer) {
        let mut completions = self.ring.completions();
        self.stats.completions += u64::from(completions.len());
        for cqe in &mut completions {
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
                timer.armed.set(None);
                if cqe.res == -libc::ETIME {
                    timer.fired.set(true);
                }
                continue;
            }
            route(
                &mut self.table,
                &mut self.wakers,
                &mut self.released,
                &mut self.deliveries,
                cqe,
            );
        }
        drop(completions);
        // The polled ring carries operations only, every one of which posts
        // one completion.
        if let Some(polled) = &mut self.polled {
            let mut completions = polled.ring.completions();
            let count = completions.len();
            self.stats.completions += u64::from(count);
            polled.pending = polled.pending.saturating_sub(count);
            for cqe in &mut completions {
                route(
                    &mut self.table,
                    &mut self.wakers,
                    &mut self.released,
                    &mut self.deliveries,
                    cqe,
                );
            }
        }
    }
}

/// Records an operation's completion in the table: wakes the executor's
/// task that waits for it, or collects its waker, its orphaned payload or
/// the orphaned stream to deliver to, which run once the driver is released.
#[inline]
fn route(
    table: &mut Table<Payload>,
    wakers: &mut Vec<Waker>,
    released: &mut Vec<(Payload, Completion)>,
    deliveries: &mut Vec<u64>,
    cqe: sys::io_uring_cqe,
) {
    let completion = Completion::new(cqe.res, cqe.flags);
    match table.complete(cqe.user_data, completion) {
        Outcome::Wake(Some(waker)) => wakers.push(waker),
        Outcome::Wake(None) | Outcome::Ignore => {}
        Outcome::Release(payload) => released.push((payload, completion)),
        Outcome::Deliver(user_data) => deliveries.push(user_data),
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        // What other threads asked of the ring before it closes goes to the
        // kernel with everything else queued, closes queued in order among
        // them, which a ring dropped without another turn would lose.
        let _ = self.enter(0, None, None);
        // Before the ring closes its descriptor, which senders duplicate;
        // what the last requests ask goes to the kernel before any other
        // thread finds the ring closed, and closes at once a descriptor that
        // an entry sent here may name.
        let remote = Arc::clone(&self.remote);
        remote.close(|requests| {
            for request in requests {
                self.handle(request);
            }
            if self.ring.queued() > 0 {
                let _ = self.submit_queued(0, false);
            }
            if self.polled.is_some() {
                let _ = self.submit_queued(0, true);
            }
        });
        // What handling the requests replaced, woken for nothing otherwise;
        // and the operations it took over that had completed.
        drop(mem::take(&mut self.wakers));
        for (mut payload, completion) in mem::take(&mut self.released) {
            payload.deliver(completion);
        }
        // Every operation still waiting is a portable one, whose future is on
        // another thread or gone, as an `Op` keeps the driver alive: nothing
        // is to be woken by its completion.
        drop(self.table.clear_wakes());
        if self.table.in_flight() > 0 {
            self.drain();
        } else {
            let _ = self.enter(0, None, None);
        }
        // Their futures find out how they ended wherever they are.
        portable::record(self.id, self.table.strand());
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
    #[inline]
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

#[inline]
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

    /// A portable operation whose future is away, and which the kernel never
    /// finishes, is recorded lost as its ring shuts down.
    #[test]
    fn a_portable_operation_the_kernel_never_finishes_is_lost_with_its_ring() {
        let timeout = Duration::from_millis(20);
        let driver = Driver::builder().shutdown_timeout(timeout).build().unwrap();
        // Waiting for a future that is elsewhere, with nothing submitted.
        let user_data = driver.inner.state.borrow_mut().table.insert(None);
        let id = driver.id();
        drop(driver);
        assert!(matches!(
            portable::stranded::fate(id, user_data),
            Some(portable::Fate::Lost)
        ));
    }
}
