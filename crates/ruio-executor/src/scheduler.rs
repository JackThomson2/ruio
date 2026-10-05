//! The scheduler of one thread: the tasks it owns, the queue of those ready to
//! run, and the loop that runs them and turns the ring.

use std::alloc::{self, Layout};
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::hint;
use std::io;
use std::mem;
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::{Duration, Instant};

use ruio_driver::{Driver, RemoteRing, Unparker};

use crate::task::{self, JoinHandle, LocalRef, Main, RemoteRef, Task};
use crate::time::Timers;
use crate::wheel::Key;

/// How many tasks run between turns of the ring, so that a flood of ready
/// tasks cannot hold up completions.
const BUDGET: usize = 64;

/// How many tasks in a row may come from the LIFO slot before the queue gets
/// a turn, so that tasks waking each other cannot starve the rest.
const LIFO_LIMIT: u32 = 3;

/// How many freed tasks' allocations a scheduler keeps for spawns to reuse,
/// of each of the layouts it keeps them for.
const CACHED_TASKS: usize = 64;

/// How many layouts of task a scheduler keeps freed allocations of: the
/// first that many whose tasks are freed on its thread.
const CACHED_LAYOUTS: usize = 4;

/// The allocations of tasks freed on the scheduler's thread, kept for spawns
/// of tasks of the same layout, which then take no call into the allocator,
/// as the frees did not. A task's layout is its future's, so a program has
/// few.
#[derive(Default)]
struct TaskCache {
    buckets: [Bucket; CACHED_LAYOUTS],
}

/// The kept blocks of one layout.
#[derive(Default)]
struct Bucket {
    /// The layout the blocks have, once a task of it has been freed.
    layout: Option<Layout>,
    blocks: Vec<NonNull<u8>>,
}

impl TaskCache {
    /// The bucket of `layout`, if a task of it has been freed.
    #[inline]
    fn bucket(&mut self, layout: Layout) -> Option<&mut Bucket> {
        self.buckets
            .iter_mut()
            .find(|bucket| bucket.layout == Some(layout))
    }

    /// The bucket of `layout`, or an empty one claimed for it, if any is.
    #[inline]
    fn bucket_for(&mut self, layout: Layout) -> Option<&mut Bucket> {
        let bucket = self
            .buckets
            .iter_mut()
            .find(|bucket| bucket.layout.is_none_or(|kept| kept == layout))?;
        bucket.layout = Some(layout);
        Some(bucket)
    }
}

/// The part of a scheduler that other threads reach: the tasks woken there,
/// the timers dropped there, and the unparker of the scheduler's ring.
pub(crate) struct Shared {
    /// The runtime's id, unique in the process: its driver's.
    id: u64,
    remote: Mutex<Remote>,
    /// Set once anything is queued in `remote`, so that the scheduler takes
    /// the lock only when there may be something to take.
    pending: AtomicBool,
    unparker: Unparker,
    /// The ring, for other rings to send descriptors to.
    ring: RemoteRing,
    /// What holds the part from the runtime's thread; see [`Holds`].
    holds: Holds,
}

/// How many hold a runtime's shared part by a count of the runtime's thread
/// rather than an atomic reference each: the scheduler, until it is dropped,
/// and the tasks it spawned, from their spawn until they are freed or may be
/// freed on another thread. Holding the part so takes no atomic operation,
/// and while anything is counted the count holds one reference of the
/// part's own for all of them, as a task's local references hold one of the
/// task's.
struct Holds(Cell<usize>);

// SAFETY: only the runtime's thread touches the count: the scheduler, which
// stays on its thread, and the tasks counted, which are spawned and freed
// there. A task that may be freed on another thread lets go of its count
// first, on the runtime's thread, and holds an atomic reference instead.
unsafe impl Sync for Holds {}

/// The runtimes alive in the process, by id, for what names a runtime
/// rather than holding it to find it again: a sleep dropped away from the
/// thread of the wheel its timer is in; see [`Shared::find`].
static RUNTIMES: Mutex<Vec<(u64, Weak<Shared>)>> = Mutex::new(Vec::new());

/// A spawn from another thread, which makes a task on the runtime's own.
pub(crate) type RemoteSpawn = Box<dyn FnOnce() + Send>;

struct Remote {
    tasks: Vec<RemoteRef>,
    spawns: Vec<RemoteSpawn>,
    /// Timers of the scheduler's wheel to remove, whose sleeps were dropped
    /// or moved where the wheel cannot be reached.
    timers: Vec<Key>,
    /// Set when the runtime shuts down, after which wakes, spawns and
    /// removals are dropped.
    closed: bool,
}

impl Shared {
    /// Queues a task woken on another thread and wakes the scheduler.
    pub(crate) fn schedule_remote(&self, task: RemoteRef) {
        let refused = {
            let mut remote = self.lock();
            if remote.closed {
                Some(task)
            } else {
                remote.tasks.push(task);
                None
            }
        };
        match refused {
            // The runtime is gone, and dropped its tasks' futures on its own
            // thread, so this reference holds nothing that is not Send.
            Some(task) => drop(task),
            None => {
                self.pending.store(true, Ordering::Release);
                self.unparker.unpark();
            }
        }
    }

    /// Queues a spawn from another thread, which the scheduler runs on its
    /// own at its next turn, and wakes the scheduler. A runtime that has shut
    /// down drops it.
    pub(crate) fn spawn_remote(&self, spawn: RemoteSpawn) {
        let refused = {
            let mut remote = self.lock();
            if remote.closed {
                Some(spawn)
            } else {
                remote.spawns.push(spawn);
                None
            }
        };
        match refused {
            Some(spawn) => drop(spawn),
            None => {
                self.pending.store(true, Ordering::Release);
                self.unparker.unpark();
            }
        }
    }

    /// Has the scheduler remove a timer from its wheel at its next turn,
    /// for a sleep that lets go of it where the wheel cannot be reached:
    /// on another thread, or outside the runtime. Until then the timer may
    /// still fire, waking for nothing whoever last polled the sleep. A
    /// runtime that has shut down has no wheel left to remove it from.
    pub(crate) fn remove_timer(&self, key: Key) {
        let queued = {
            let mut remote = self.lock();
            let open = !remote.closed;
            if open {
                remote.timers.push(key);
            }
            open
        };
        if queued {
            self.pending.store(true, Ordering::Release);
            self.unparker.unpark();
        }
    }

    /// The scheduler's ring, as other threads see it.
    pub(crate) fn ring(&self) -> &RemoteRing {
        &self.ring
    }

    /// The runtime's id, unique in the process.
    #[inline]
    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    /// The shared part of the runtime with `id`, if it is alive: registered
    /// as its scheduler is made, and struck off as the scheduler is dropped,
    /// after which there is no wheel to reach.
    pub(crate) fn find(id: u64) -> Option<Arc<Shared>> {
        let runtimes = RUNTIMES.lock().unwrap_or_else(PoisonError::into_inner);
        runtimes
            .iter()
            .find(|(known, _)| *known == id)
            .and_then(|(_, shared)| shared.upgrade())
    }

    /// Counts a hold of the part `this` names from the runtime's thread; the
    /// first takes the count's reference to the part. It takes the `Arc`
    /// rather than the part, so that the count's reference is taken through
    /// the `Arc`'s own pointer, which reaches its counts.
    ///
    /// # Safety
    ///
    /// Only on the runtime's thread, for its scheduler or a task of it.
    #[inline]
    pub(crate) unsafe fn hold(this: &Arc<Shared>) {
        let held = this.holds.0.get();
        if held == 0 {
            // SAFETY: the `Arc` keeps the part alive, and the pointer is the
            // one it gives.
            unsafe { Arc::increment_strong_count(Arc::as_ptr(this)) };
        }
        this.holds.0.set(held + 1);
    }

    /// Lets go of a hold that [`Shared::hold`] counted, and, with the last,
    /// of the count's reference to the part, which may free it.
    ///
    /// # Safety
    ///
    /// Only on the runtime's thread, for a counted hold that nothing else
    /// lets go of; `this` must be the pointer the part's `Arc` gives.
    #[inline]
    pub(crate) unsafe fn release_hold(this: *const Shared) {
        // SAFETY: the hold keeps the part alive.
        let holds = unsafe { &(*this).holds.0 };
        let left = holds.get() - 1;
        holds.set(left);
        if left == 0 {
            // SAFETY: the count's reference, which the first hold took.
            unsafe { Arc::decrement_strong_count(this) };
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Remote> {
        self.remote.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The tasks a scheduler owns, each at a fixed index until it finishes.
#[derive(Default)]
struct Tasks {
    slots: Vec<Option<LocalRef>>,
    free: Vec<usize>,
}

impl Tasks {
    #[inline]
    fn insert(&mut self, task: LocalRef) -> usize {
        match self.free.pop() {
            Some(index) => {
                self.slots[index] = Some(task);
                index
            }
            None => {
                self.slots.push(Some(task));
                self.slots.len() - 1
            }
        }
    }

    #[inline]
    fn remove(&mut self, index: usize) -> Option<LocalRef> {
        let task = self.slots.get_mut(index)?.take()?;
        self.free.push(index);
        Some(task)
    }

    fn take_all(&mut self) -> Vec<LocalRef> {
        self.free.clear();
        self.slots.drain(..).flatten().collect()
    }
}

/// Runs the tasks of one thread, and its ring.
pub(crate) struct Scheduler {
    driver: Driver,
    shared: Arc<Shared>,
    /// Tasks ready to run, in the order they were woken.
    queue: RefCell<VecDeque<LocalRef>>,
    /// The task most recently woken by the running task. It runs next, while
    /// what the running task produced for it is likely still in the cache.
    lifo: RefCell<Option<LocalRef>>,
    /// How many tasks in a row came from the LIFO slot.
    lifo_streak: Cell<u32>,
    /// The task being polled, if any, as a thin pointer.
    running: Cell<*const ()>,
    /// Every task whose future has not been dropped; this is what keeps a
    /// task alive while nothing else refers to it.
    tasks: RefCell<Tasks>,
    timers: Timers,
    /// How long a wait for work polls the ring before it parks; see
    /// [`Builder::spin`](crate::Builder::spin). Zero parks at once.
    spin: Duration,
    /// How long the last wait for work took, polling and parking together.
    /// A wait polls only if the one before ended within the budget.
    last_wait: Cell<Duration>,
    /// The clock as this pass has read it, if it has: the timers polled in a
    /// pass share one reading, so a task arming many reads it once. Cleared
    /// as the pass ends, after its wait; see [`Scheduler::now`].
    now: Cell<Option<Instant>>,
    /// Freed tasks' allocations, for spawns to reuse; boxed, so that the
    /// scheduler's own fields keep their places in the cache lines a pass
    /// reads.
    tasks_freed: Box<RefCell<TaskCache>>,
}
impl Scheduler {
    pub(crate) fn new(driver: Driver, spin: Duration) -> Self {
        let id = driver.id();
        let shared = Arc::new(Shared {
            id,
            remote: Mutex::new(Remote {
                tasks: Vec::new(),
                spawns: Vec::new(),
                timers: Vec::new(),
                closed: false,
            }),
            pending: AtomicBool::new(false),
            unparker: driver.unparker(),
            ring: driver.remote(),
            holds: Holds(Cell::new(0)),
        });
        // SAFETY: this is the runtime's thread. The scheduler lets go of its
        // hold as it is dropped.
        unsafe { Shared::hold(&shared) };
        RUNTIMES
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((id, Arc::downgrade(&shared)));
        // SAFETY: the retain counts a local reference only to a task of the
        // runtime running on this thread, the driver's, which keeps the task
        // alive until the release or the wake that lets go of it, on this
        // thread; and the wakes queue the task on its scheduler, or on its
        // remote queue, without using the driver.
        unsafe { driver.set_local_waker(task::LOCAL_WAKER) };
        Self {
            driver,
            shared,
            queue: RefCell::new(VecDeque::new()),
            lifo: RefCell::new(None),
            lifo_streak: Cell::new(0),
            running: Cell::new(ptr::null()),
            tasks: RefCell::new(Tasks::default()),
            timers: Timers::new(),
            spin,
            last_wait: Cell::new(Duration::ZERO),
            now: Cell::new(None),
            tasks_freed: Box::new(RefCell::new(TaskCache::default())),
        }
    }

    /// Memory for a task of `layout`: a freed task's, if one of that layout
    /// is kept, and otherwise the allocator's.
    #[inline]
    pub(crate) fn alloc_task(&self, layout: Layout) -> NonNull<u8> {
        if let Some(block) = self
            .tasks_freed
            .borrow_mut()
            .bucket(layout)
            .and_then(|bucket| bucket.blocks.pop())
        {
            return block;
        }
        // SAFETY: a task has a size, its header's at the least.
        let block = unsafe { alloc::alloc(layout) };
        NonNull::new(block).unwrap_or_else(|| alloc::handle_alloc_error(layout))
    }

    /// Takes back the memory of a task freed on this thread, to keep for the
    /// next spawn of its layout if there is room for it, and otherwise to
    /// give back to the allocator.
    ///
    /// # Safety
    ///
    /// `block` must be memory of `layout` that the allocator gave, with
    /// nothing in it any more.
    #[inline]
    pub(crate) unsafe fn free_task(&self, block: NonNull<u8>, layout: Layout) {
        if let Some(bucket) = self.tasks_freed.borrow_mut().bucket_for(layout)
            && bucket.blocks.len() < CACHED_TASKS
        {
            bucket.blocks.push(block);
            return;
        }
        // SAFETY: the caller's contract.
        unsafe { alloc::dealloc(block.as_ptr(), layout) };
    }

    /// The time, as the pass first read it: never later than now, and
    /// earlier by at most the time the pass has run. A timer polled against
    /// it may be armed with a deadline just past, and fires at the next
    /// tick, rather than ending at once; none ends early.
    #[inline]
    pub(crate) fn now(&self) -> Instant {
        match self.now.get() {
            Some(now) => now,
            None => {
                let now = Instant::now();
                self.now.set(Some(now));
                now
            }
        }
    }

    #[inline]
    pub(crate) fn timers(&self) -> &Timers {
        &self.timers
    }

    #[inline]
    pub(crate) fn driver(&self) -> &Driver {
        &self.driver
    }

    #[inline]
    pub(crate) fn shared(&self) -> &Arc<Shared> {
        &self.shared
    }

    pub(crate) fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        Task::spawn(future, self)
    }

    /// Takes ownership of a new task and returns its index.
    #[inline]
    pub(crate) fn adopt(&self, task: LocalRef) -> usize {
        self.tasks.borrow_mut().insert(task)
    }

    /// Lets go of a finished task.
    #[inline]
    pub(crate) fn release(&self, index: usize) {
        let task = self.tasks.borrow_mut().remove(index);
        drop(task);
    }

    /// Queues a woken task. One woken by the running task, other than
    /// itself, takes the LIFO slot; the task it displaces, and any other,
    /// joins the back of the queue.
    #[inline]
    pub(crate) fn schedule(&self, task: LocalRef) {
        let running = self.running.get();
        let task = if !running.is_null() && !ptr::eq(task.as_ptr().cast::<()>(), running) {
            match self.lifo.borrow_mut().replace(task) {
                Some(displaced) => displaced,
                None => return,
            }
        } else {
            task
        };
        self.queue.borrow_mut().push_back(task);
    }

    /// Queues a new task, at the back of the queue.
    #[inline]
    pub(crate) fn schedule_new(&self, task: LocalRef) {
        self.queue.borrow_mut().push_back(task);
    }

    /// The next task to run: the LIFO slot's, unless it has had its share.
    #[inline]
    fn next_task(&self) -> Option<LocalRef> {
        let lifo = self.lifo.borrow_mut().take();
        if let Some(task) = lifo {
            if self.lifo_streak.get() < LIFO_LIMIT {
                self.lifo_streak.set(self.lifo_streak.get() + 1);
                return Some(task);
            }
            self.queue.borrow_mut().push_back(task);
        }
        self.lifo_streak.set(0);
        self.queue.borrow_mut().pop_front()
    }

    #[inline]
    fn has_ready_tasks(&self) -> bool {
        !self.queue.borrow().is_empty() || self.lifo.borrow().is_some()
    }

    /// Runs up to [`BUDGET`] ready tasks and fires the due timers, then turns
    /// the ring: without waiting if anything is ready to run, otherwise
    /// parking until a completion arrives, another thread wakes a task, or the
    /// next timer is due.
    ///
    /// # Panics
    ///
    /// If the driver fails, which means completions were lost.
    pub(crate) fn tick(&self, main: &Main) {
        for _ in 0..BUDGET {
            let Some(task) = self.next_task() else {
                break;
            };
            self.running.set(task.as_ptr().cast());
            task.run(self);
            self.running.set(ptr::null());
        }
        self.take_remote();
        self.fire_timers();
        // The ring's own timer wakes it for the earliest timer, so a park
        // needs no timeout, and no turn needs the time until a timer is due.
        let turned = self
            .timers
            .next_deadline()
            .map_or(Ok(()), |deadline| self.driver.wake_at(deadline))
            .and_then(|()| {
                if !self.has_ready_tasks() && !main.is_woken() {
                    self.wait(main)
                } else {
                    self.driver.submit()
                }
            });
        if let Err(err) = turned {
            panic!("the io_uring driver failed: {err}");
        }
        self.take_remote();
        self.fire_timers();
        // The wait may have been long: the next pass reads the clock anew.
        self.now.set(None);
    }

    /// Waits for work: a completion, the ring's timer, or a task woken or
    /// spawned on another thread. Parks the thread, unless the scheduler
    /// spins and its last wait was short enough for spinning to have caught
    /// what ended it: then it polls for the work first, with the thread
    /// awake, for up to the spin budget.
    fn wait(&self, main: &Main) -> io::Result<()> {
        if self.spin.is_zero() {
            return self.driver.park(None);
        }
        let started = Instant::now();
        let waited = if self.last_wait.get() <= self.spin {
            self.spin_then_park(main)
        } else {
            self.driver.park(None)
        };
        self.last_wait.set(started.elapsed());
        waited
    }

    /// Polls for work for up to the spin budget, then parks if none has
    /// come; see [`Scheduler::wait`].
    fn spin_then_park(&self, main: &Main) -> io::Result<()> {
        // What the pass queued goes to the kernel first, as a park would
        // send it: what is waited for may be a reply to it. A completion
        // this delivers ends the wait before it begins.
        self.driver.submit()?;
        if self.has_ready_tasks() || main.is_woken() {
            return Ok(());
        }
        let started = Instant::now();
        loop {
            if self.driver.has_work() {
                // Something the park takes at once: the kernel's work,
                // posted completions, or an unpark, which it consumes.
                return self.driver.park(None);
            }
            if self.shared.pending.load(Ordering::Acquire) {
                // Work from another thread, which the pass takes next.
                return Ok(());
            }
            if started.elapsed() >= self.spin {
                return self.driver.park(None);
            }
            hint::spin_loop();
        }
    }

    /// Fires the timers due, once the ring's timer has said some may be.
    #[inline]
    fn fire_timers(&self) {
        if self.driver.timer_fired() {
            // Read anew, however stale the pass's reading, and kept for the
            // timers armed as the woken tasks run.
            let now = Instant::now();
            self.now.set(Some(now));
            self.timers.fire(now);
        }
    }

    /// Moves the tasks woken on other threads into the run queue, removes
    /// the timers let go of there, and makes the tasks spawned from other
    /// threads.
    #[inline]
    fn take_remote(&self) {
        // Anything queued after the check sets the flag again, and unparks
        // the ring: the next turn takes it.
        if self.shared.pending.load(Ordering::Acquire) {
            self.take_pending();
        }
    }

    /// What [`Scheduler::take_remote`] takes, once the flag says that
    /// something may have been queued.
    fn take_pending(&self) {
        self.shared.pending.store(false, Ordering::Relaxed);
        let (timers, spawns) = {
            let mut remote = self.shared.lock();
            if !remote.tasks.is_empty() {
                // A task woken here meanwhile is in the queue already.
                let claimed = remote
                    .tasks
                    .drain(..)
                    .filter(RemoteRef::claim)
                    .map(LocalRef::adopt);
                self.queue.borrow_mut().extend(claimed);
            }
            (mem::take(&mut remote.timers), mem::take(&mut remote.spawns))
        };
        // Outside the lock: dropping a waker can drop a task, and a spawn
        // runs code, either of which can reach this runtime or another.
        for key in timers {
            drop(self.timers.remove(key));
        }
        for spawn in spawns {
            spawn();
        }
    }

    /// Drops the future of every unfinished task, on this thread.
    pub(crate) fn shutdown(&self) {
        let (woken, spawns) = {
            let mut remote = self.shared.lock();
            remote.closed = true;
            // The wheel goes with the scheduler, timers and all.
            remote.timers.clear();
            (mem::take(&mut remote.tasks), mem::take(&mut remote.spawns))
        };
        drop((woken, spawns));
        // Dropping a future can spawn more tasks, so repeat until none is left.
        loop {
            let tasks = self.tasks.borrow_mut().take_all();
            if tasks.is_empty() {
                break;
            }
            for task in &tasks {
                task.cancel();
            }
        }
        let queued = mem::take(&mut *self.queue.borrow_mut());
        let lifo = self.lifo.borrow_mut().take();
        drop((queued, lifo));
    }
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        for bucket in &mut self.tasks_freed.get_mut().buckets {
            let Some(layout) = bucket.layout else {
                continue;
            };
            for block in bucket.blocks.drain(..) {
                // SAFETY: the blocks are the allocator's, of `layout`, with
                // nothing in them: the tasks they held were dropped as they
                // were freed.
                unsafe { alloc::dealloc(block.as_ptr(), layout) };
            }
        }
        // The wheel goes with the scheduler: nothing is left to find the
        // runtime for.
        let id = self.shared.id;
        RUNTIMES
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|(known, _)| *known != id);
        // Tasks still held, by their handles, may hold the part beyond it.
        // SAFETY: this is the runtime's thread, the scheduler stays on it,
        // and its hold, counted as it was made, is let go of once.
        unsafe { Shared::release_hold(Arc::as_ptr(&self.shared)) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Runtime;

    /// Freed blocks come back for the next task of their layout, each
    /// layout apart, up to the kept count; the rest go to the allocator.
    #[test]
    fn freed_tasks_are_kept_for_their_layout() {
        let runtime = Runtime::new().unwrap();
        runtime.block_on(async {
            crate::context::with_current(|scheduler| {
                let small = Layout::from_size_align(64, 8).unwrap();
                let large = Layout::from_size_align(256, 16).unwrap();
                let first = scheduler.alloc_task(small);
                let second = scheduler.alloc_task(large);
                // SAFETY: the blocks are the allocator's, of these layouts,
                // and nothing was put in them.
                unsafe { scheduler.free_task(first, small) };
                // SAFETY: as above.
                unsafe { scheduler.free_task(second, large) };
                assert_eq!(scheduler.alloc_task(small), first);
                assert_eq!(scheduler.alloc_task(large), second);
                let blocks: Vec<_> = (0..CACHED_TASKS + 1)
                    .map(|_| scheduler.alloc_task(small))
                    .collect();
                for &block in &blocks {
                    // SAFETY: as above.
                    unsafe { scheduler.free_task(block, small) };
                }
                let kept = scheduler.tasks_freed.borrow().buckets[0].blocks.len();
                assert_eq!(kept, CACHED_TASKS);
                // The two in use go back too: the small one to the allocator,
                // its bucket being full, and the large one to its bucket. The
                // scheduler frees what it kept as it is dropped.
                // SAFETY: as above.
                unsafe { scheduler.free_task(first, small) };
                // SAFETY: as above.
                unsafe { scheduler.free_task(second, large) };
            })
            .unwrap();
        });
    }

    #[test]
    fn a_runtime_is_found_by_its_id_while_it_lives() {
        let runtime = Runtime::new().unwrap();
        let id = runtime.driver().id();
        let found = Shared::find(id).expect("a live runtime is registered");
        assert_eq!(found.id(), id);
        drop(found);
        drop(runtime);
        assert!(
            Shared::find(id).is_none(),
            "a dropped runtime is struck off"
        );
        assert!(Shared::find(u64::MAX).is_none());
    }
}
