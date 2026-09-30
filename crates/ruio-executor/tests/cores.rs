//! Runtimes on several cores.

use std::future::pending;
use std::io::ErrorKind;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ruio_executor::sync::{mpsc, oneshot};
use ruio_executor::time::sleep;
use ruio_executor::{Core, Cores, Runtime, core, core_count, handle_of, spawn, spawn_on};

/// A few cores, however many CPUs the machine has: on one with fewer, some
/// share a CPU.
fn few() -> Cores {
    let all = Cores::all().unwrap();
    Cores::cpus(all.cpu_list().iter().copied().cycle().take(4))
}

/// The CPU the calling thread is on.
fn current_cpu() -> usize {
    // SAFETY: sched_getcpu reads the thread's CPU and changes nothing.
    usize::try_from(unsafe { libc::sched_getcpu() }).unwrap()
}

#[test]
fn cores_are_cpus_the_process_may_run_on() {
    let all = Cores::all().unwrap();
    assert!(all.count() >= 1);
    assert!(all.cpu_list().windows(2).all(|pair| pair[0] < pair[1]));
    let first = Cores::first(1).unwrap();
    assert_eq!(first.cpu_list(), &all.cpu_list()[..1]);
    for count in [0, all.count() + 1] {
        let err = Cores::first(count).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput, "{err}");
    }
    // A core's thread runs on its own CPU alone, but the process on all.
    let seen = few()
        .run(|_| async { Cores::all().unwrap().cpu_list().to_vec() })
        .unwrap();
    assert!(seen.iter().all(|cpus| cpus == all.cpu_list()), "{seen:?}");
}

#[test]
fn each_core_runs_main_pinned_to_its_cpu() {
    let cores = few();
    let seen = cores
        .run(|core| async move {
            assert_eq!(ruio_executor::core(), Some(core));
            (
                core,
                current_cpu(),
                core_count(),
                std::thread::current().name().map(String::from),
            )
        })
        .unwrap();
    for (index, (core, cpu, count, name)) in seen.into_iter().enumerate() {
        assert_eq!(core.index(), index);
        assert_eq!(core.cpu(), cores.cpu_list()[index]);
        assert_eq!(cpu, core.cpu(), "pinned");
        assert_eq!(count, cores.count());
        assert_eq!(name.as_deref(), Some(format!("ruio-core-{index}").as_str()));
    }
    // Outside a run, a thread is no core.
    assert_eq!(core(), None);
    assert_eq!(core_count(), 1);
    Runtime::new()
        .unwrap()
        .block_on(async { assert_eq!(core(), None) });
}

/// Each core spawns onto the next, which runs the task as itself.
#[test]
fn cores_spawn_onto_each_other() {
    let cores = few();
    let count = cores.count();
    let answers = cores
        .run(|me: Core| async move {
            // A core's handle names its ring.
            let ring = handle_of(me.index()).ring().id();
            assert_eq!(ring, ruio_executor::driver().id());
            let next = (me.index() + 1) % count;
            let there = spawn_on(next, async { (core().unwrap(), current_cpu()) })
                .await
                .unwrap();
            (next, there)
        })
        .unwrap();
    for (next, (there, cpu)) in answers {
        assert_eq!(there.index(), next);
        assert_eq!(cpu, there.cpu());
    }
}

/// A token goes round a ring of channels between the cores, again and
/// again: every hop wakes a task on another core.
#[test]
fn a_token_goes_round_the_cores() {
    const ROUNDS: usize = 200;
    let cores = few();
    let count = cores.count();
    // A channel into each core, which it keeps the receiver of.
    let channels: Vec<_> = (0..count).map(|_| mpsc::channel::<usize>(1)).collect();
    let senders: Vec<_> = channels.iter().map(|(tx, _)| tx.clone()).collect();
    let receivers: Vec<_> = channels
        .into_iter()
        .map(|(_, rx)| Mutex::new(Some(rx)))
        .collect();
    let hops = AtomicUsize::new(0);
    cores
        .run(|me| {
            let mut rx = receivers[me.index()].lock().unwrap().take().unwrap();
            let next = senders[(me.index() + 1) % count].clone();
            let hops = &hops;
            async move {
                if me.index() == 0 {
                    next.send(0).await.unwrap();
                }
                while let Some(token) = rx.recv().await {
                    hops.fetch_add(1, Ordering::Relaxed);
                    if token == ROUNDS * count {
                        break;
                    }
                    next.send(token + 1).await.unwrap();
                    // The next token this core would get is past the last.
                    if token + count > ROUNDS * count {
                        break;
                    }
                }
            }
        })
        .unwrap();
    assert!(hops.load(Ordering::Relaxed) >= ROUNDS * count);
}

/// A core whose `main` has returned still runs what the others spawn onto
/// it, until every `main` has returned.
#[test]
fn a_core_serves_the_others_until_every_main_returns() {
    let cores = few();
    let (returning, returned) = oneshot::channel::<()>();
    let returning = Mutex::new(Some(returning));
    let returned = Mutex::new(Some(returned));
    let answers = cores
        .run(|me| {
            let (returning, returned) = (&returning, &returned);
            async move {
                match me.index() {
                    0 => {
                        returning.lock().unwrap().take().unwrap().send(()).unwrap();
                        None
                    }
                    1 => {
                        let returned = returned.lock().unwrap().take().unwrap();
                        returned.await.unwrap();
                        // Time for core 0 to be past its `main`.
                        sleep(Duration::from_millis(10)).await;
                        let task = spawn_on(0, async { core().unwrap().index() });
                        Some(task.await.unwrap())
                    }
                    _ => None,
                }
            }
        })
        .unwrap();
    assert_eq!(answers[1], Some(0));
}

/// When the run ends, each core's runtime drops the tasks left on it while
/// the thread is still that core.
#[test]
fn a_core_drops_its_tasks_as_itself() {
    /// Records which core it is dropped on.
    struct Witness(Arc<Mutex<Vec<Option<Core>>>>);

    impl Drop for Witness {
        fn drop(&mut self) {
            self.0.lock().unwrap().push(core());
        }
    }

    let cores = few();
    let dropped_on = Arc::new(Mutex::new(Vec::new()));
    cores
        .run(|_| {
            let witness = Witness(Arc::clone(&dropped_on));
            async move {
                // Never finishes, so it is left when the run ends.
                drop(spawn(async move {
                    let _witness = witness;
                    pending::<()>().await;
                }));
            }
        })
        .unwrap();
    let mut indices: Vec<_> = dropped_on
        .lock()
        .unwrap()
        .iter()
        .map(|core| core.expect("dropped on its core").index())
        .collect();
    indices.sort_unstable();
    assert_eq!(indices, (0..cores.count()).collect::<Vec<_>>());
}

/// A core that cannot start keeps every `main` from running, and lets the
/// cores that could start go.
#[test]
fn a_run_that_cannot_start_runs_nothing() {
    let ran = AtomicUsize::new(0);
    let cpu = Cores::all().unwrap().cpu_list()[0];
    let cores = Cores::cpus([cpu, 1 << 20]);
    let err = cores
        .run(|_| async {
            ran.fetch_add(1, Ordering::Relaxed);
        })
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput, "{err}");
    assert_eq!(ran.load(Ordering::Relaxed), 0);
}

/// A core's panic carries on once the others have finished.
#[test]
fn a_panic_on_a_core_ends_the_run() {
    let finished = AtomicUsize::new(0);
    let cores = few();
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        cores.run(|core| {
            let finished = &finished;
            async move {
                if core.index() == 0 {
                    panic!("core 0 panics");
                }
                finished.fetch_add(1, Ordering::Relaxed);
            }
        })
    }));
    assert!(result.is_err());
    assert_eq!(finished.load(Ordering::Relaxed), cores.count() - 1);
}
