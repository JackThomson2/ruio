//! Portable operations under runtimes: queued on the ring of the runtime
//! that first polls them, and carried by `Send` futures between cores.

use std::future::{Future, poll_fn};
use std::io::{PipeReader, PipeWriter, Write as _};
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::task::Poll;
use std::thread;
use std::time::{Duration, Instant};

use ruio_driver::buf::{IoBuf, IoBufMut};
use ruio_driver::ops::{Nop, Read};
use ruio_driver::{Driver, PortableOp};
use ruio_executor::time::sleep;
use ruio_executor::{Cores, Runtime, spawn, spawn_on};

fn pipe() -> (PipeReader, PipeWriter) {
    std::io::pipe().unwrap()
}

/// Two cores, on two CPUs if the machine has them.
fn two() -> Cores {
    let all = Cores::all().unwrap();
    Cores::cpus(all.cpu_list().iter().copied().cycle().take(2))
}

/// Polls `future` once, where it lies, and says whether it is pending.
async fn pending_once<F: Future + Unpin>(future: &mut F) -> bool {
    poll_fn(|cx| Poll::Ready(Pin::new(&mut *future).poll(cx).is_pending())).await
}

/// Waits, turning the ring, until `done` says to stop.
async fn until(mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(Instant::now() < deadline, "not done in time");
        sleep(Duration::from_millis(1)).await;
    }
}

/// A buffer that counts its drops.
struct Counted {
    buf: Vec<u8>,
    drops: Arc<AtomicUsize>,
}

impl Counted {
    fn new() -> (Self, Arc<AtomicUsize>) {
        let drops = Arc::new(AtomicUsize::new(0));
        let counted = Self {
            buf: Vec::with_capacity(16),
            drops: Arc::clone(&drops),
        };
        (counted, drops)
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

// SAFETY: as the vector's, which stays put while the value moves.
unsafe impl IoBuf for Counted {
    fn stable_ptr(&self) -> *const u8 {
        self.buf.stable_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.buf.bytes_init()
    }

    fn bytes_total(&self) -> usize {
        self.buf.bytes_total()
    }
}

// SAFETY: as the vector's.
unsafe impl IoBufMut for Counted {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.buf.stable_mut_ptr()
    }

    unsafe fn set_init(&mut self, pos: usize) {
        // SAFETY: the caller's contract, which is the vector's.
        unsafe { self.buf.set_init(pos) };
    }
}

#[test]
fn a_runtime_runs_portable_operations_on_its_ring() {
    let runtime = Runtime::new().unwrap();
    let before = runtime.driver().stats().operations;
    runtime.block_on(PortableOp::new(Nop)).unwrap();
    let id = runtime.block_on(async { Driver::current().unwrap().id() });
    assert_eq!(id, runtime.driver().id());
    assert_eq!(runtime.driver().stats().operations - before, 1);
    // Out of the runtime, no driver is current.
    assert!(Driver::current().is_none());
}

/// A future that holds a portable operation across an await is `Send`, and
/// runs on the ring of the core it is spawned onto.
#[test]
fn a_future_holding_a_portable_operation_spawns_onto_another_core() {
    let outputs = two()
        .run(|core| async move {
            if core.index() != 0 {
                return None;
            }
            let task = spawn_on(1, async {
                let (reader, mut writer) = pipe();
                writer.write_all(b"spawned").unwrap();
                let read = PortableOp::new(Read::new(reader.as_raw_fd(), Vec::with_capacity(16)));
                let (read, buf) = read.await;
                let ring = Driver::current().unwrap().id();
                (read.unwrap(), buf, ring, ruio_executor::driver().id())
            });
            Some(task.await.unwrap())
        })
        .unwrap();
    let (read, buf, ring, core_ring) = outputs[0].clone().unwrap();
    assert_eq!((read, &buf[..]), (7, &b"spawned"[..]));
    assert_eq!(ring, core_ring, "on the ring of the core it ran on");
}

/// A read polled on one core, then spawned onto another while in flight,
/// completes on its own ring, and its data comes to the other core.
#[test]
fn moved_to_another_core_mid_flight_it_completes_on_its_ring() {
    let outputs = two()
        .run(|core| async move {
            if core.index() != 0 {
                return None;
            }
            let (reader, mut writer) = pipe();
            let mut read = PortableOp::new(Read::new(reader.as_raw_fd(), Vec::with_capacity(16)));
            assert!(pending_once(&mut read).await, "queued here");
            let away = spawn_on(1, async move {
                let (read, buf) = read.await;
                (read.unwrap(), buf, ruio_executor::core().unwrap().index())
            });
            writer.write_all(b"moved").unwrap();
            let output = away.await.unwrap();
            drop(reader);
            Some(output)
        })
        .unwrap();
    let (read, buf, ran_on) = outputs[0].clone().unwrap();
    assert_eq!((read, &buf[..], ran_on), (5, &b"moved"[..], 1));
}

/// A read dropped on another core is cancelled by its ring.
#[test]
fn dropped_on_another_core_it_is_cancelled_by_its_ring() {
    two()
        .run(|core| async move {
            if core.index() != 0 {
                return;
            }
            let (reader, _writer) = pipe();
            let (buf, drops) = Counted::new();
            let mut read = PortableOp::new(Read::new(reader.as_raw_fd(), buf));
            assert!(pending_once(&mut read).await);
            spawn_on(1, async move { drop(read) }).await.unwrap();
            until(|| drops.load(Ordering::SeqCst) == 1).await;
            assert_eq!(ruio_executor::driver().in_flight(), 0);
        })
        .unwrap();
}

/// A runtime shut down while a read it ran is on another thread finishes
/// the read, and the future there completes with its data.
#[test]
fn a_runtime_shut_down_first_leaves_the_read_to_its_future() {
    let (reader, mut writer) = pipe();
    writer.write_all(b"leftover").unwrap();
    let (send, receive) = mpsc::channel();
    let fd = reader.as_raw_fd();
    thread::spawn(move || {
        let runtime = Runtime::new().unwrap();
        runtime.block_on(async {
            let mut read = PortableOp::new(Read::new(fd, Vec::with_capacity(16)));
            assert!(pending_once(&mut read).await);
            send.send(read).unwrap();
        });
    })
    .join()
    .unwrap();
    let read = receive.recv().unwrap();
    let (read, buf) = Runtime::new().unwrap().block_on(read);
    assert_eq!((read.unwrap(), &buf[..]), (8, &b"leftover"[..]));
}

/// A task whose read is in flight as its runtime shuts down has the read
/// cancelled, and its buffer freed once the kernel is done.
#[test]
fn a_task_dropped_with_its_runtime_lets_go_of_its_read() {
    let (reader, _writer) = pipe();
    let (buf, drops) = Counted::new();
    let runtime = Runtime::new().unwrap();
    let fd = reader.as_raw_fd();
    runtime.block_on(async {
        // Detached: the runtime drops it as it shuts down.
        drop(spawn(async move {
            let _ = PortableOp::new(Read::new(fd, buf)).await;
        }));
        sleep(Duration::from_millis(1)).await;
    });
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(runtime);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
