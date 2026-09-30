//! The scheduler of one thread: the tasks it owns, the queue of those ready to
//! run, and the loop that runs them and turns the ring.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::mem;
use std::ptr;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use ruio_driver::{Driver, Unparker};

use crate::task::{JoinHandle, Task};
use crate::time::Timers;

/// How many tasks run between turns of the ring, so that a flood of ready
/// tasks cannot hold up completions.
const BUDGET: usize = 64;

/// How many tasks in a row may come from the LIFO slot before the queue gets
/// a turn, so that tasks waking each other cannot starve the rest.
const LIFO_LIMIT: u32 = 3;

/// A task, as the scheduler and wakers hold it.
pub(crate) type TaskRef = Arc<dyn Runnable>;

/// What the scheduler needs from a task, whatever its future. Only called on
/// the thread that owns the task.
pub(crate) trait Runnable: Send + Sync {
    /// Polls the task, finishing it if its future completes or it was
    /// aborted.
    fn run(self: Arc<Self>, scheduler: &Scheduler);

    /// Drops the future of an unfinished task, which then reports that it
    /// was cancelled.
    fn cancel(&self);

    /// Marks the task queued to run, unless it is queued already: whether
    /// the caller is to queue it.
    fn claim(&self) -> bool;

    /// Queues the task on its scheduler's remote queue, from another thread,
    /// unless it is there already.
    fn wake_remote(self: Arc<Self>);
}

/// The part of a scheduler that other threads reach: the tasks woken there,
/// and the unparker of the scheduler's ring.
pub(crate) struct Shared {
    remote: Mutex<Remote>,
    /// Set once a task is queued in `remote`, so that the scheduler takes
    /// the lock only when there may be tasks to take.
    pending: AtomicBool,
    unparker: Unparker,
}

struct Remote {
    tasks: Vec<TaskRef>,
    /// Set when the runtime shuts down, after which wakes are dropped.
    closed: bool,
}

impl Shared {
    /// Queues a task woken on another thread and wakes the scheduler.
    pub(crate) fn schedule_remote(&self, task: TaskRef) {
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

    /// Wakes the scheduler's ring.
    pub(crate) fn unpark(&self) {
        self.unparker.unpark();
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Remote> {
        self.remote.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The tasks a scheduler owns, each at a fixed index until it finishes.
#[derive(Default)]
struct Tasks {
    slots: Vec<Option<TaskRef>>,
    free: Vec<usize>,
}

impl Tasks {
    fn insert(&mut self, task: TaskRef) -> usize {
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

    fn remove(&mut self, index: usize) -> Option<TaskRef> {
        let task = self.slots.get_mut(index)?.take()?;
        self.free.push(index);
        Some(task)
    }

    fn take_all(&mut self) -> Vec<TaskRef> {
        self.free.clear();
        self.slots.drain(..).flatten().collect()
    }
}

/// Runs the tasks of one thread, and its ring.
pub(crate) struct Scheduler {
    driver: Driver,
    shared: Arc<Shared>,
    /// Tasks ready to run, in the order they were woken.
    queue: RefCell<VecDeque<TaskRef>>,
    /// The task most recently woken by the running task. It runs next, while
    /// what the running task produced for it is likely still in the cache.
    lifo: RefCell<Option<TaskRef>>,
    /// How many tasks in a row came from the LIFO slot.
    lifo_streak: Cell<u32>,
    /// The task being polled, if any, as a thin pointer.
    running: Cell<*const ()>,
    /// Every task whose future has not been dropped; this is what keeps a
    /// task alive while nothing else refers to it.
    tasks: RefCell<Tasks>,
    timers: Rc<Timers>,
}

impl Scheduler {
    pub(crate) fn new(driver: Driver) -> Self {
        let shared = Arc::new(Shared {
            remote: Mutex::new(Remote {
                tasks: Vec::new(),
                closed: false,
            }),
            pending: AtomicBool::new(false),
            unparker: driver.unparker(),
        });
        Self {
            driver,
            shared,
            queue: RefCell::new(VecDeque::new()),
            lifo: RefCell::new(None),
            lifo_streak: Cell::new(0),
            running: Cell::new(ptr::null()),
            tasks: RefCell::new(Tasks::default()),
            timers: Rc::new(Timers::new()),
        }
    }

    pub(crate) fn timers(&self) -> &Rc<Timers> {
        &self.timers
    }

    pub(crate) fn driver(&self) -> &Driver {
        &self.driver
    }

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
    pub(crate) fn adopt(&self, task: TaskRef) -> usize {
        self.tasks.borrow_mut().insert(task)
    }

    /// Lets go of a finished task.
    pub(crate) fn release(&self, index: usize) {
        let task = self.tasks.borrow_mut().remove(index);
        drop(task);
    }

    /// Queues a woken task. One woken by the running task, other than
    /// itself, takes the LIFO slot; the task it displaces, and any other,
    /// joins the back of the queue.
    pub(crate) fn schedule(&self, task: TaskRef) {
        let running = self.running.get();
        let task = if !running.is_null() && !ptr::eq(Arc::as_ptr(&task).cast::<()>(), running) {
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
    pub(crate) fn schedule_new(&self, task: TaskRef) {
        self.queue.borrow_mut().push_back(task);
    }

    /// The next task to run: the LIFO slot's, unless it has had its share.
    fn next_task(&self) -> Option<TaskRef> {
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
    pub(crate) fn tick(&self, main_woken: &AtomicBool) {
        for _ in 0..BUDGET {
            let Some(task) = self.next_task() else {
                break;
            };
            self.running.set(Arc::as_ptr(&task).cast());
            task.run(self);
            self.running.set(ptr::null());
        }
        self.take_remote();
        let now = Instant::now();
        self.timers.fire(now);
        let idle = !self.has_ready_tasks() && !main_woken.load(Ordering::SeqCst);
        let turned = if idle {
            self.driver.park(self.timers.next_timeout(now))
        } else {
            self.driver.submit()
        };
        if let Err(err) = turned {
            panic!("the io_uring driver failed: {err}");
        }
        self.take_remote();
        self.timers.fire(Instant::now());
    }

    /// Moves the tasks woken on other threads into the run queue.
    fn take_remote(&self) {
        // A task queued after the check sets the flag again, and unparks the
        // ring: the next turn takes it.
        if !self.shared.pending.load(Ordering::Acquire) {
            return;
        }
        self.shared.pending.store(false, Ordering::Relaxed);
        let mut remote = self.shared.lock();
        if !remote.tasks.is_empty() {
            // A task woken here meanwhile is in the queue already.
            let claimed = remote.tasks.drain(..).filter(|task| task.claim());
            self.queue.borrow_mut().extend(claimed);
        }
    }

    /// Drops the future of every unfinished task, on this thread.
    pub(crate) fn shutdown(&self) {
        let woken = {
            let mut remote = self.shared.lock();
            remote.closed = true;
            mem::take(&mut remote.tasks)
        };
        drop(woken);
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
