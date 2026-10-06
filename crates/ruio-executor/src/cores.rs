//! Running a runtime on each of several cores: thread per core.

use std::cell::RefCell;
use std::future::{Future, poll_fn};
use std::io;
use std::mem;
use std::panic;
use std::process;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::task::{Poll, Waker};
use std::thread;

use crate::handle::Handle;
use crate::runtime::Builder;
use crate::task::RemoteJoinHandle;

/// A core of a [`Cores`] run: its index among the run's cores, and the CPU
/// its thread is pinned to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Core {
    index: usize,
    cpu: usize,
}

impl Core {
    /// The core's index, from 0: what [`spawn_on`] takes.
    pub fn index(self) -> usize {
        self.index
    }

    /// The CPU the core's thread runs on.
    pub fn cpu(self) -> usize {
        self.cpu
    }
}

/// The cores of a run, as each of them sees the others.
struct Registry {
    cores: Vec<(Core, Handle)>,
}

thread_local! {
    /// The thread's core and its run's registry, inside [`Cores::run`].
    static CURRENT: RefCell<Option<(Core, Arc<Registry>)>> = const { RefCell::new(None) };
}

/// The core the thread is, inside [`Cores::run`].
pub fn core() -> Option<Core> {
    CURRENT.with(|current| current.borrow().as_ref().map(|(core, _)| *core))
}

/// How many cores the thread's run has: 1 outside [`Cores::run`], as a
/// runtime on its own is one core.
pub fn core_count() -> usize {
    CURRENT.with(|current| {
        current
            .borrow()
            .as_ref()
            .map_or(1, |(_, registry)| registry.cores.len())
    })
}

/// Spawns `future` onto core `index` of the thread's run: the core makes it
/// a task at its next turn; see [`Handle::spawn`].
///
/// # Panics
///
/// Outside [`Cores::run`], or if the run has no core `index`.
pub fn spawn_on<F>(index: usize, future: F) -> RemoteJoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    handle_of(index).spawn(future)
}

/// The handle of core `index` of the thread's run: to spawn onto it, with a
/// future made there if need be ([`Handle::spawn_with`]), and to send it
/// direct descriptors ([`Handle::ring`]).
///
/// # Panics
///
/// Outside [`Cores::run`], or if the run has no core `index`.
pub fn handle_of(index: usize) -> Handle {
    CURRENT.with(|current| {
        let current = current.borrow();
        let (_, registry) = current
            .as_ref()
            .expect("only a core of a `Cores` run can reach the others");
        let (_, handle) = registry
            .cores
            .get(index)
            .unwrap_or_else(|| panic!("the run has no core {index}"));
        handle.clone()
    })
}

/// A set of CPUs to run on, a runtime on each: thread per core.
///
/// ```
/// # fn main() -> std::io::Result<()> {
/// use ruio_executor::{Cores, spawn_on};
///
/// let cores = Cores::all()?;
/// let count = cores.count();
/// // Each core spawns a task onto the next, which says where it ran.
/// let ran_on = cores.run(|core| async move {
///     let next = (core.index() + 1) % count;
///     let task = spawn_on(next, async { ruio_executor::core().unwrap().index() });
///     task.await.unwrap()
/// })?;
/// assert_eq!(ran_on, (0..count).map(|index| (index + 1) % count).collect::<Vec<_>>());
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Cores {
    cpus: Vec<usize>,
    builder: Builder,
}

impl Cores {
    /// Every CPU the process may run on, in order, whichever thread asks:
    /// those its main thread may run on, which `taskset(1)` or a cgroup's
    /// cpuset limits.
    ///
    /// # Errors
    ///
    /// What `sched_getaffinity(2)` reports.
    pub fn all() -> io::Result<Self> {
        Ok(Self::cpus(allowed_cpus()?))
    }

    /// The first `count` CPUs the process may run on.
    ///
    /// # Errors
    ///
    /// What `sched_getaffinity(2)` reports, and `InvalidInput` if `count` is
    /// zero or the process may run on fewer CPUs.
    pub fn first(count: usize) -> io::Result<Self> {
        let cpus = allowed_cpus()?;
        if count == 0 || count > cpus.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "{count} cores asked for, of the {} CPUs the process may run on",
                    cpus.len()
                ),
            ));
        }
        Ok(Self::cpus(cpus.into_iter().take(count)))
    }

    /// These CPUs, in this order: core `i` runs on the `i`th. A CPU given
    /// twice runs two cores.
    ///
    /// # Panics
    ///
    /// If there are none.
    pub fn cpus(cpus: impl IntoIterator<Item = usize>) -> Self {
        let cpus: Vec<usize> = cpus.into_iter().collect();
        assert!(!cpus.is_empty(), "a run needs at least one core");
        Self {
            cpus,
            builder: Builder::new(),
        }
    }

    /// Builds each core's runtime as `builder` says.
    pub fn builder(mut self, builder: Builder) -> Self {
        self.builder = builder;
        self
    }

    /// How many cores there are.
    pub fn count(&self) -> usize {
        self.cpus.len()
    }

    /// The CPUs, in core order.
    pub fn cpu_list(&self) -> &[usize] {
        &self.cpus
    }

    /// Runs `main` on each core, and returns their outputs in core order
    /// once every one has finished.
    ///
    /// Each core is a thread of its own, pinned to its CPU, with a runtime
    /// of its own, which runs `main(core)` as [`Runtime::block_on`] does:
    /// its tasks run on it. Every core's runtime is built, and every core can
    /// reach every other with [`spawn_on`], before any `main` starts, and
    /// each keeps running what others spawn onto it until every `main` has
    /// returned; then the runtimes end, with their tasks.
    ///
    /// # Errors
    ///
    /// If a thread cannot be spawned or pinned, or a runtime built; no
    /// `main` runs then.
    ///
    /// # Panics
    ///
    /// If a core's `main` panics: the panic carries on once every core's
    /// thread has ended, which one waiting for the core that panicked may
    /// never do.
    ///
    /// [`Runtime::block_on`]: crate::Runtime::block_on
    pub fn run<F, Fut>(&self, main: F) -> io::Result<Vec<Fut::Output>>
    where
        F: Fn(Core) -> Fut + Sync,
        Fut: Future,
        Fut::Output: Send,
    {
        let start = Start::new(self.cpus.len());
        let finish = Finish::new(self.cpus.len());
        let ends = thread::scope(|scope| {
            let mut threads = Vec::with_capacity(self.cpus.len());
            for (index, &cpu) in self.cpus.iter().enumerate() {
                let core = Core { index, cpu };
                let (builder, start, finish, main) = (&self.builder, &start, &finish, &main);
                let spawned = thread::Builder::new()
                    .name(format!("ruio-core-{index}"))
                    .spawn_scoped(scope, move || run_core(core, builder, start, finish, main));
                match spawned {
                    Ok(thread) => threads.push(thread),
                    Err(err) => {
                        // The cores spawned so far wait for this one.
                        start.fail(err);
                        break;
                    }
                }
            }
            threads
                .into_iter()
                .map(|thread| thread.join())
                .collect::<Vec<_>>()
        });
        let mut outputs = Vec::with_capacity(ends.len());
        for end in ends {
            match end {
                Ok(output) => outputs.push(output),
                // Only now that every core's thread has ended.
                Err(payload) => panic::resume_unwind(payload),
            }
        }
        match start.take_error() {
            Some(err) => Err(err),
            None => Ok(outputs
                .into_iter()
                .map(|output| output.expect("every core ran"))
                .collect()),
        }
    }
}

/// The cores' meeting point before any `main` starts: each puts its handle
/// in, or the first error that keeps it from starting.
struct Start {
    state: Mutex<StartState>,
    ready: Condvar,
}

struct StartState {
    handles: Vec<Option<(Core, Handle)>>,
    registered: usize,
    registry: Option<Arc<Registry>>,
    error: Option<io::Error>,
}

impl Start {
    fn new(cores: usize) -> Self {
        Self {
            state: Mutex::new(StartState {
                handles: (0..cores).map(|_| None).collect(),
                registered: 0,
                registry: None,
                error: None,
            }),
            ready: Condvar::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, StartState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records that a core cannot start, and lets the others know.
    fn fail(&self, err: io::Error) {
        self.lock().error.get_or_insert(err);
        self.ready.notify_all();
    }

    /// Puts a core's handle in, and waits for every core's: the run's
    /// registry, or `None` if a core cannot start.
    fn register(&self, core: Core, handle: Handle) -> Option<Arc<Registry>> {
        let mut state = self.lock();
        state.handles[core.index] = Some((core, handle));
        state.registered += 1;
        if state.registered == state.handles.len() {
            let cores = mem::take(&mut state.handles)
                .into_iter()
                .map(|entry| entry.expect("every core registered"))
                .collect();
            state.registry = Some(Arc::new(Registry { cores }));
            self.ready.notify_all();
        }
        loop {
            if state.error.is_some() {
                return None;
            }
            if let Some(registry) = &state.registry {
                return Some(Arc::clone(registry));
            }
            state = self
                .ready
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    fn take_error(&self) -> Option<io::Error> {
        self.lock().error.take()
    }
}

/// The cores' meeting point after their `main`s: each keeps running until
/// every one has returned, or panicked.
struct Finish {
    state: Mutex<FinishState>,
}

struct FinishState {
    /// How many cores' `main`s have yet to return.
    running: usize,
    /// The waker of each core waiting for them, at the core's index.
    waiting: Vec<Option<Waker>>,
}

impl Finish {
    fn new(cores: usize) -> Self {
        Self {
            state: Mutex::new(FinishState {
                running: cores,
                waiting: vec![None; cores],
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, FinishState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Counts a core's `main` as returned; the last wakes the others.
    fn arrive(&self) {
        let waiting = {
            let mut state = self.lock();
            state.running -= 1;
            if state.running == 0 {
                mem::take(&mut state.waiting)
            } else {
                Vec::new()
            }
        };
        for waker in waiting.into_iter().flatten() {
            waker.wake();
        }
    }

    /// Waits until every core's `main` has returned.
    async fn wait(&self, core: Core) {
        poll_fn(|cx| {
            let mut state = self.lock();
            if state.running == 0 {
                return Poll::Ready(());
            }
            // One waker a core, however often its loop polls this.
            let slot = &mut state.waiting[core.index];
            if !slot
                .as_ref()
                .is_some_and(|waker| waker.will_wake(cx.waker()))
            {
                *slot = Some(cx.waker().clone());
            }
            Poll::Pending
        })
        .await;
    }
}

/// Counts its core as arrived when dropped, unless it has been: a `main`
/// that panics counts too, so that the others do not wait for it.
struct Arrival<'a> {
    finish: Option<&'a Finish>,
}

impl Arrival<'_> {
    fn arrive(&mut self) {
        if let Some(finish) = self.finish.take() {
            finish.arrive();
        }
    }
}

impl Drop for Arrival<'_> {
    fn drop(&mut self) {
        self.arrive();
    }
}

/// What a core's thread does: pins itself, builds its runtime, meets the
/// others, runs `main`, and serves the others until they are done. `None`
/// if the run cannot start.
fn run_core<F, Fut>(
    core: Core,
    builder: &Builder,
    start: &Start,
    finish: &Finish,
    main: &F,
) -> Option<Fut::Output>
where
    F: Fn(Core) -> Fut,
    Fut: Future,
{
    // Dropped last, however the thread ends: the thread is still its core
    // while its runtime drops the tasks, whose destructors may reach others.
    let _clear = Clear;
    // Pinned first, so that what the kernel allocates for the ring is local
    // to the CPU.
    let runtime = match pin(core.cpu).and_then(|()| builder.build()) {
        Ok(runtime) => runtime,
        Err(err) => {
            start.fail(err);
            return None;
        }
    };
    let registry = start.register(core, runtime.handle())?;
    CURRENT.with(|current| *current.borrow_mut() = Some((core, registry)));
    let mut arrival = Arrival {
        finish: Some(finish),
    };
    Some(runtime.block_on(async {
        let output = main(core).await;
        arrival.arrive();
        finish.wait(core).await;
        output
    }))
}

/// Leaves the thread no core when dropped.
struct Clear;

impl Drop for Clear {
    fn drop(&mut self) {
        CURRENT.with(|current| current.borrow_mut().take());
    }
}

/// The CPUs the process may run on, in order: those of its main thread,
/// whose ID is the process's, since the thread asking may be a core's, which
/// may run on its own CPU alone.
fn allowed_cpus() -> io::Result<Vec<usize>> {
    // Linux's process IDs are `pid_t`s.
    let main = process::id() as libc::pid_t;
    // SAFETY: all zeroes is an empty CPU set.
    let mut set: libc::cpu_set_t = unsafe { mem::zeroed() };
    // SAFETY: sched_getaffinity writes at most the set's size into it.
    let ret = unsafe { libc::sched_getaffinity(main, mem::size_of::<libc::cpu_set_t>(), &mut set) };
    if ret != 0 {
        return Err(io::Error::last_os_error());
    }
    let cpus = (0..libc::CPU_SETSIZE as usize)
        // SAFETY: the index is below the set's size.
        .filter(|&cpu| unsafe { libc::CPU_ISSET(cpu, &set) })
        .collect();
    Ok(cpus)
}

/// Pins the calling thread to `cpu`.
fn pin(cpu: usize) -> io::Result<()> {
    if cpu >= libc::CPU_SETSIZE as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("no CPU {cpu}"),
        ));
    }
    // SAFETY: all zeroes is an empty CPU set.
    let mut set: libc::cpu_set_t = unsafe { mem::zeroed() };
    // SAFETY: the CPU is below the set's size.
    unsafe { libc::CPU_SET(cpu, &mut set) };
    // SAFETY: sched_setaffinity reads the one set it is given.
    let ret = unsafe { libc::sched_setaffinity(0, mem::size_of::<libc::cpu_set_t>(), &set) };
    if ret != 0 {
        let err = io::Error::last_os_error();
        return Err(io::Error::new(
            err.kind(),
            format!("pinning to CPU {cpu}: {err}"),
        ));
    }
    Ok(())
}
