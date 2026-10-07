//! Portable operations: queued on the ring of the thread's current driver,
//! and handed back to that ring by futures that move to other threads.

mod support;

use std::future::Future;
use std::io::{PipeReader, PipeWriter, Write as _};
use std::os::fd::AsRawFd;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

use ruio_driver::buf::{IoBuf, IoBufMut};
use ruio_driver::ops::{Map, Nop, OpenAt, Read};
use ruio_driver::{Driver, PortableOp};

use support::{PATIENCE, block_on, poll_once, settle};

fn assert_send<T: Send>(_: &T) {}

fn pipe() -> (PipeReader, PipeWriter) {
    std::io::pipe().unwrap()
}

/// A read of up to 16 bytes from `reader`.
fn read(reader: &PipeReader) -> PortableOp<'static, Read<Vec<u8>>> {
    PortableOp::new(Read::new(reader.as_raw_fd(), Vec::with_capacity(16)))
}

/// Wakes a thread parked waiting for a future.
struct Unpark(thread::Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// Runs `future` to completion on this thread, which need have no driver,
/// parking between polls.
fn block_on_away<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let waker = Waker::from(Arc::new(Unpark(thread::current())));
    let mut cx = Context::from_waker(&waker);
    let deadline = Instant::now() + PATIENCE;
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        assert!(
            Instant::now() < deadline,
            "still pending after {PATIENCE:?}"
        );
        thread::park_timeout(Duration::from_millis(10));
    }
}

/// Turns `driver` until `done` says to stop.
fn turn_until(driver: &Driver, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + PATIENCE;
    while !done() {
        assert!(Instant::now() < deadline, "not done after {PATIENCE:?}");
        driver.park(Some(Duration::from_millis(10))).unwrap();
    }
}

/// A buffer that counts its drops, to tell an operation dropped from one
/// leaked.
struct Counted {
    buf: Vec<u8>,
    drops: Arc<AtomicUsize>,
}

impl Counted {
    fn new() -> (Self, Arc<AtomicUsize>) {
        let drops = Arc::new(AtomicUsize::new(0));
        let buf = Vec::with_capacity(16);
        (
            Self {
                buf,
                drops: Arc::clone(&drops),
            },
            drops,
        )
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
fn a_portable_operation_runs_on_the_current_driver() {
    let driver = Driver::new().unwrap();
    let _entered = driver.enter();
    let op = PortableOp::new(Nop);
    assert_send(&op);
    let before = driver.stats().operations;
    block_on(&driver, op).unwrap();
    assert_eq!(driver.stats().operations - before, 1);
    assert_eq!(driver.in_flight(), 0);
    let mapped = PortableOp::new(Map::new(Nop, |result| result.is_ok()));
    assert!(block_on(&driver, mapped));
}

#[test]
#[should_panic(expected = "current driver")]
fn a_portable_operation_starts_only_where_a_driver_is_current() {
    let _ = poll_once(&mut PortableOp::new(Nop));
}

#[test]
fn the_driver_entered_last_is_current_until_its_guard_goes() {
    assert!(Driver::current().is_none());
    let (first, second) = (Driver::new().unwrap(), Driver::new().unwrap());
    let entered_first = first.enter();
    assert_eq!(Driver::current().unwrap().id(), first.id());
    let entered_second = second.enter();
    assert_eq!(Driver::current().unwrap().id(), second.id());
    drop(entered_second);
    assert_eq!(Driver::current().unwrap().id(), first.id());
    drop(entered_first);
    assert!(Driver::current().is_none());
    // A guard keeps its driver alive, and the driver's drop, once it goes,
    // finds no driver current.
    let third = Driver::new().unwrap();
    let entered_third = third.enter();
    drop(third);
    assert!(Driver::current().is_some());
    drop(entered_third);
    assert!(Driver::current().is_none());
}

/// Polled on a thread without its ring, a read goes back to its ring, which
/// completes it and sends the data over.
#[test]
fn polled_on_another_thread_its_output_comes_from_its_ring() {
    let driver = Driver::new().unwrap();
    let _entered = driver.enter();
    let (reader, mut writer) = pipe();
    let mut op = read(&reader);
    assert!(poll_once(&mut op).is_pending());
    driver.submit().unwrap();
    assert_eq!(driver.in_flight(), 1);

    let (polled, first_poll) = mpsc::channel();
    let away = thread::spawn(move || {
        let mut op = op;
        assert!(poll_once(&mut op).is_pending(), "handed back, not done");
        polled.send(()).unwrap();
        block_on_away(op)
    });
    first_poll.recv().unwrap();
    writer.write_all(b"portable").unwrap();
    turn_until(&driver, || away.is_finished());
    let (read, buf) = away.join().unwrap();
    assert_eq!(read.unwrap(), 8);
    assert_eq!(buf, b"portable");
    assert_eq!(driver.in_flight(), 0);
}

/// A ring that parks with a handed-back read waiting in its mailbox, whose
/// data came before, delivers the read at once rather than wait for a
/// completion that will never come: the read's has been reaped already.
#[test]
fn a_parked_ring_delivers_what_it_takes_back_without_waiting() {
    let driver = Driver::new().unwrap();
    let _entered = driver.enter();
    let (reader, mut writer) = pipe();
    writer.write_all(b"early").unwrap();
    let mut op = read(&reader);
    assert!(poll_once(&mut op).is_pending());
    // Submitted and reaped: the read found its data at once.
    driver.submit().unwrap();
    assert_eq!(driver.in_flight(), 0);
    let unparker = driver.unparker();
    // Set before the thread away unparks the ring for the last time, so that
    // a turn that takes that unpark before the thread has ended stops the
    // loop, rather than a park that would wait for another.
    let done = Arc::new(AtomicBool::new(false));
    let away = thread::spawn({
        let done = Arc::clone(&done);
        move || {
            // Most likely the ring parks first, and the request arrives while
            // it waits, which the turn after takes.
            thread::sleep(Duration::from_millis(20));
            let output = block_on_away(op);
            done.store(true, Ordering::SeqCst);
            unparker.unpark();
            output
        }
    });
    while !done.load(Ordering::SeqCst) {
        let parked = Instant::now();
        driver.park(Some(PATIENCE)).unwrap();
        assert!(
            parked.elapsed() < PATIENCE / 2,
            "a park waited with a delivery to make"
        );
    }
    let (read, buf) = away.join().unwrap();
    assert_eq!((read.unwrap(), &buf[..]), (5, &b"early"[..]));
}

/// Dropped on a thread without its ring, a read goes back to its ring, which
/// cancels it.
#[test]
fn dropped_on_another_thread_it_is_cancelled_on_its_ring() {
    let driver = Driver::new().unwrap();
    let _entered = driver.enter();
    let (reader, _writer) = pipe();
    let (buf, drops) = Counted::new();
    let mut op = PortableOp::new(Read::new(reader.as_raw_fd(), buf));
    assert!(poll_once(&mut op).is_pending());
    driver.submit().unwrap();
    thread::spawn(move || drop(op)).join().unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 0, "the kernel still has it");
    settle(&driver);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

/// A future that hands its read back and then is dropped has the ring
/// cancel it.
#[test]
fn handed_back_and_then_dropped_it_is_cancelled_on_its_ring() {
    let driver = Driver::new().unwrap();
    let _entered = driver.enter();
    let (reader, _writer) = pipe();
    let mut op = read(&reader);
    assert!(poll_once(&mut op).is_pending());
    driver.submit().unwrap();
    thread::spawn(move || {
        let mut op = op;
        assert!(poll_once(&mut op).is_pending());
    })
    .join()
    .unwrap();
    settle(&driver);
}

/// A ring dropped while a future is away finishes its operation as it shuts
/// down, and the future completes with the read's data wherever it is.
#[test]
fn a_ring_dropped_first_leaves_the_completion_for_its_future() {
    for away in [true, false] {
        let driver = Driver::new().unwrap();
        let entered = driver.enter();
        let (reader, mut writer) = pipe();
        writer.write_all(b"before").unwrap();
        let mut op = read(&reader);
        assert!(poll_once(&mut op).is_pending());
        drop(entered);
        drop(driver);
        let (read, buf) = if away {
            thread::spawn(move || block_on_away(op)).join().unwrap()
        } else {
            block_on_away(op)
        };
        assert_eq!(read.unwrap(), 6);
        assert_eq!(buf, b"before");
    }
}

/// A future dropped after its ring finds the operation finished, which the
/// ring cancelled as it shut down, and drops it.
#[test]
fn dropped_after_its_ring_the_operation_is_freed() {
    let driver = Driver::new().unwrap();
    let entered = driver.enter();
    let (reader, _writer) = pipe();
    let (buf, drops) = Counted::new();
    let mut op = PortableOp::new(Read::new(reader.as_raw_fd(), buf));
    assert!(poll_once(&mut op).is_pending());
    drop(entered);
    drop(driver);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    thread::spawn(move || drop(op)).join().unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

/// A detached operation runs to its end on its ring, from wherever it was
/// detached.
#[test]
fn a_detached_operation_finishes_on_its_ring() {
    let driver = Driver::new().unwrap();
    let _entered = driver.enter();
    let (reader, mut writer) = pipe();
    read(&reader).detach();
    assert_eq!(driver.in_flight(), 1);
    let mut op = read(&reader);
    assert!(poll_once(&mut op).is_pending());
    driver.submit().unwrap();
    thread::spawn(move || op.detach()).join().unwrap();
    writer.write_all(&[7; 32]).unwrap();
    settle(&driver);
    let mut rest = Vec::new();
    drop(writer);
    std::io::Read::read_to_end(&mut &reader, &mut rest).unwrap();
    assert!(rest.is_empty(), "both reads took their data");
}

/// A portable operation whose output cannot cross threads cannot either:
/// a descriptor that may be direct belongs to its ring.
#[test]
fn an_operation_whose_output_cannot_move_stays_put() {
    let open = OpenAt::new("/").unwrap();
    let plain = PortableOp::new(Map::new(open, |opened| opened.map(drop)));
    assert_send(&plain);
}
