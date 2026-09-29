//! The scheduler of one thread: the tasks it owns, the queue of those ready to
//! run, and the loop that runs them and turns the ring.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::mem;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use ruio_driver::{Driver, Unparker};

use crate::task::{JoinHandle, Task};

/// How many tasks run between turns of the ring, so that a flood of ready
/// tasks cannot hold up completions.
const BUDGET: usize = 64;

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
}

/// The part of a scheduler that other threads reach: the tasks woken there,
/// and the unparker of the scheduler's ring.
pub(crate) struct Shared {
    remote: Mutex<Remote>,
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
            None => self.unparker.unpark(),
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
    /// Every task whose future has not been dropped; this is what keeps a
    /// task alive while nothing else refers to it.
    tasks: RefCell<Tasks>,
}

impl Scheduler {
    pub(crate) fn new(driver: Driver) -> Self {
        let shared = Arc::new(Shared {
            remote: Mutex::new(Remote {
                tasks: Vec::new(),
                closed: false,
            }),
            unparker: driver.unparker(),
        });
        Self {
            driver,
            shared,
            queue: RefCell::new(VecDeque::new()),
            tasks: RefCell::new(Tasks::default()),
        }
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

    /// Queues a task to run.
    pub(crate) fn schedule(&self, task: TaskRef) {
        self.queue.borrow_mut().push_back(task);
    }

    /// Runs up to [`BUDGET`] ready tasks, then turns the ring: without
    /// waiting if anything is ready to run, otherwise parking until a
    /// completion arrives or another thread wakes a task.
    ///
    /// # Panics
    ///
    /// If the driver fails, which means completions were lost.
    pub(crate) fn tick(&self, main_woken: &AtomicBool) {
        for _ in 0..BUDGET {
            let Some(task) = self.queue.borrow_mut().pop_front() else {
                break;
            };
            task.run(self);
        }
        self.take_remote();
        let idle = self.queue.borrow().is_empty() && !main_woken.load(Ordering::SeqCst);
        let turned = if idle {
            self.driver.park(None)
        } else {
            self.driver.submit()
        };
        if let Err(err) = turned {
            panic!("the io_uring driver failed: {err}");
        }
        self.take_remote();
    }

    /// Moves the tasks woken on other threads into the run queue.
    fn take_remote(&self) {
        let mut remote = self.shared.lock();
        if !remote.tasks.is_empty() {
            self.queue.borrow_mut().extend(remote.tasks.drain(..));
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
        drop(queued);
    }
}
