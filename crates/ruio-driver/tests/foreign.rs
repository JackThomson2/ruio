//! Portable operations first polled where no driver is current: their
//! entries go to the foreign ring, whose thread queues them and sends their
//! completions back, while the operations stay with their futures.
//!
//! The tests set the process's foreign ring, so they run one at a time.

mod support;

use std::future::{Future, poll_fn};
use std::io::{PipeReader, PipeWriter, Write as _};
use std::os::fd::AsRawFd;
use std::pin::pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, mpsc};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

use ruio_driver::buf::{IoBuf, IoBufMut};
use ruio_driver::ops::{Nop, Read};
use ruio_driver::{Driver, PortableFd, PortableOp, ReadyWait, RemoteRing, Unparker};

use support::{PATIENCE, poll_once, settle};

/// What a wait for a pipe's reading end to be readable waits for.
const READABLE: u32 = libc::POLLIN as u32;

/// Held by each test, which changes the process's foreign ring.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(PoisonError::into_inner)
}

fn pipe() -> (PipeReader, PipeWriter) {
    std::io::pipe().unwrap()
}

/// A ring on a thread of its own, which turns it until this is dropped, and
/// which is the foreign ring meanwhile.
struct Foreign {
    remote: RemoteRing,
    unparker: Unparker,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
    _serial: MutexGuard<'static, ()>,
}

impl Foreign {
    fn start() -> Self {
        let serial = serial();
        let stop = Arc::new(AtomicBool::new(false));
        let (send, receive) = mpsc::channel();
        let thread = thread::spawn({
            let stop = Arc::clone(&stop);
            move || {
                let driver = Driver::new().unwrap();
                send.send((driver.remote(), driver.unparker())).unwrap();
                while !stop.load(Ordering::SeqCst) {
                    driver.park(Some(Duration::from_millis(100))).unwrap();
                }
            }
        });
        let (remote, unparker) = receive.recv().unwrap();
        ruio_driver::set_foreign_ring(&remote);
        Self {
            remote,
            unparker,
            stop,
            thread: Some(thread),
            _serial: serial,
        }
    }
}

impl Drop for Foreign {
    /// Stops the ring's thread, which drops its driver, and clears the
    /// setting.
    fn drop(&mut self) {
        ruio_driver::clear_foreign_ring();
        self.stop.store(true, Ordering::SeqCst);
        self.unparker.unpark();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Wakes a thread parked waiting for a future.
struct Unpark(thread::Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// Runs `future` to completion on this thread, which has no driver,
/// parking between polls until its waker is woken.
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
        thread::park_timeout(PATIENCE);
    }
}

/// Runs foreign no-ops on this thread, each of which settles its graves,
/// until `done` says to stop.
fn settle_away(mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + PATIENCE;
    while !done() {
        assert!(Instant::now() < deadline, "not done after {PATIENCE:?}");
        block_on_away(PortableOp::new(Nop)).unwrap();
        thread::sleep(Duration::from_millis(1));
    }
}

/// How many bytes wait in the pipe of `reader`.
fn waiting(reader: &impl AsRawFd) -> usize {
    let mut waiting: libc::c_int = 0;
    // SAFETY: FIONREAD writes the one `c_int` it is given.
    let asked = unsafe { libc::ioctl(reader.as_raw_fd(), libc::FIONREAD, &raw mut waiting) };
    assert_eq!(asked, 0);
    usize::try_from(waiting).unwrap()
}

/// Whether the pipe has no reading end left, as the writing end reports
/// with `POLLERR`, without writing anything to it.
fn reader_gone(writer: &PipeWriter) -> bool {
    let mut poll = libc::pollfd {
        fd: writer.as_raw_fd(),
        events: libc::POLLOUT,
        revents: 0,
    };
    // SAFETY: poll writes the one `pollfd` it is given, and waits for none.
    let polled = unsafe { libc::poll(&raw mut poll, 1, 0) };
    assert_eq!(polled, 1);
    poll.revents & libc::POLLERR != 0
}

/// A buffer that counts its drops, to tell an operation freed from one
/// still kept.
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

/// A buffer that belongs to its thread, as one holding an `Rc` does.
struct Local {
    buf: Vec<u8>,
    _thread: Rc<()>,
}

// SAFETY: as the vector's.
unsafe impl IoBuf for Local {
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
unsafe impl IoBufMut for Local {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.buf.stable_mut_ptr()
    }

    unsafe fn set_init(&mut self, pos: usize) {
        // SAFETY: the caller's contract, which is the vector's.
        unsafe { self.buf.set_init(pos) };
    }
}

#[test]
fn an_operation_polled_where_no_driver_is_current_runs_on_the_foreign_ring() {
    let foreign = Foreign::start();
    assert_eq!(
        ruio_driver::foreign_ring().map(|ring| ring.id()),
        Some(foreign.remote.id())
    );
    assert!(Driver::current().is_none());
    block_on_away(PortableOp::new(Nop)).unwrap();
    let elsewhere = thread::spawn(|| block_on_away(PortableOp::new(Nop)));
    elsewhere.join().unwrap().unwrap();
}

/// A read sent to the foreign ring waits there for its data, which wakes the
/// task that polled it; the operation stays with its future, so one that
/// is not `Send` runs too.
#[test]
fn a_read_sent_to_the_foreign_ring_waits_for_its_data() {
    let _foreign = Foreign::start();
    let (reader, mut writer) = pipe();
    let buf = Local {
        buf: Vec::with_capacity(16),
        _thread: Rc::new(()),
    };
    let read = PortableOp::new(Read::new(reader.as_raw_fd(), buf));
    let later = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        writer.write_all(b"foreign").unwrap();
        writer
    });
    let (read, buf) = block_on_away(read);
    assert_eq!((read.unwrap(), &buf.buf[..]), (7, &b"foreign"[..]));
    drop(later.join().unwrap());
}

#[test]
fn a_timed_operation_times_out_on_the_foreign_ring() {
    let _foreign = Foreign::start();
    let (reader, _writer) = pipe();
    let read = PortableOp::new(Read::new(reader.as_raw_fd(), Vec::with_capacity(16)))
        .timeout(Duration::from_millis(20));
    let (read, _) = block_on_away(read);
    assert_eq!(read.unwrap_err().kind(), std::io::ErrorKind::TimedOut);
}

/// Dropped while the kernel has it, an operation is cancelled by its ring,
/// and kept on its thread until the kernel is done: the thread's next
/// foreign operation then frees it. The cancelled read takes nothing.
#[test]
fn dropped_in_flight_it_is_cancelled_and_freed_once_the_kernel_is_done() {
    let _foreign = Foreign::start();
    let (reader, mut writer) = pipe();
    let (buf, drops) = Counted::new();
    let mut read = PortableOp::new(Read::new(reader.as_raw_fd(), buf));
    assert!(poll_once(&mut read).is_pending());
    // Taken by the ring, which a no-op sent after it shows.
    block_on_away(PortableOp::new(Nop)).unwrap();
    drop(read);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "the kernel may still have it"
    );
    settle_away(|| drops.load(Ordering::SeqCst) == 1);
    writer.write_all(b"kept").unwrap();
    assert_eq!(waiting(&reader), 4, "the cancelled read took nothing");
}

/// A future that goes before the ring takes its entry still has the entry
/// reach the kernel, which takes what preparing it gave, as for an `Op`
/// dropped once queued, and the ring cancels it there.
#[test]
fn dropped_before_its_ring_takes_it_it_is_cancelled_as_it_is_queued() {
    let _serial = serial();
    // A ring of this thread's, which turns only when told.
    let driver = Driver::new().unwrap();
    ruio_driver::set_foreign_ring(&driver.remote());
    let (reader, mut writer) = pipe();
    let fd = reader.as_raw_fd();
    let (dropped, go) = (mpsc::channel(), mpsc::channel());
    let away = thread::spawn(move || {
        let (buf, drops) = Counted::new();
        let mut read = PortableOp::new(Read::new(fd, buf));
        assert!(poll_once(&mut read).is_pending());
        drop(read);
        dropped.0.send(()).unwrap();
        go.1.recv().unwrap();
        settle_away(|| drops.load(Ordering::SeqCst) == 1);
    });
    dropped.1.recv().unwrap();
    // Takes the entry, queues it and its cancellation, and delivers.
    driver.submit().unwrap();
    settle(&driver);
    go.0.send(()).unwrap();
    // The no-ops of the thread away need the ring turned.
    let deadline = Instant::now() + PATIENCE;
    while !away.is_finished() {
        assert!(Instant::now() < deadline);
        driver.park(Some(Duration::from_millis(1))).unwrap();
    }
    away.join().unwrap();
    writer.write_all(b"kept").unwrap();
    assert_eq!(waiting(&reader), 4, "the cancelled read took nothing");
    ruio_driver::clear_foreign_ring();
}

/// A detached operation runs to its end on the foreign ring.
#[test]
fn a_detached_operation_runs_to_its_end() {
    let _foreign = Foreign::start();
    let (reader, mut writer) = pipe();
    let (buf, drops) = Counted::new();
    PortableOp::new(Read::new(reader.as_raw_fd(), buf)).detach();
    writer.write_all(&[7; 8]).unwrap();
    settle_away(|| drops.load(Ordering::SeqCst) == 1);
    assert_eq!(waiting(&reader), 0, "the read took the data");
}

/// Moved to another thread while its operation is in flight, a future
/// completes there.
#[test]
fn a_future_moved_to_another_thread_completes_there() {
    let _foreign = Foreign::start();
    let (reader, mut writer) = pipe();
    let mut read = PortableOp::new(Read::new(reader.as_raw_fd(), Vec::with_capacity(16)));
    assert!(poll_once(&mut read).is_pending());
    let away = thread::spawn(move || block_on_away(read));
    writer.write_all(b"moved").unwrap();
    let (read, buf) = away.join().unwrap();
    assert_eq!((read.unwrap(), &buf[..]), (5, &b"moved"[..]));
}

/// A descriptor whose wait went to the foreign ring closes there, after
/// the wait, which keeps the file until it is let go of.
#[test]
fn a_descriptor_closes_after_the_wait_sent_for_it() {
    let _foreign = Foreign::start();
    let (reader, writer) = pipe();
    let reader = PortableFd::new(reader.into());
    let mut wait = ReadyWait::new();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(reader.poll_ready(&mut cx, READABLE, &mut wait).is_pending());
    drop(reader);
    // Taken after the wait and the close, which reached the kernel first.
    block_on_away(PortableOp::new(Nop)).unwrap();
    assert!(!reader_gone(&writer), "the armed poll holds the file");
    drop(wait);
    settle_away(|| reader_gone(&writer));
}

/// Dropped on the thread of the ring an entry naming it was sent to, before
/// the ring took the entry, a descriptor closes after it: the close takes
/// what others sent the ring first.
#[test]
fn a_close_on_the_rings_thread_goes_after_what_others_sent() {
    let _serial = serial();
    let driver = Driver::new().unwrap();
    ruio_driver::set_foreign_ring(&driver.remote());
    let (reader, writer) = pipe();
    let reader = PortableFd::new(reader.into());
    let (reader, wait) = thread::spawn(move || {
        let mut wait = ReadyWait::new();
        let mut cx = Context::from_waker(Waker::noop());
        assert!(reader.poll_ready(&mut cx, READABLE, &mut wait).is_pending());
        (reader, wait)
    })
    .join()
    .unwrap();
    // Found as a driver of this thread's: closed in order, with the wait
    // queued first.
    drop(reader);
    driver.submit().unwrap();
    assert!(!reader_gone(&writer), "the poll went to the kernel first");
    drop(wait);
    settle(&driver);
    assert!(reader_gone(&writer));
    ruio_driver::clear_foreign_ring();
}

/// A ring dropped with an operation sent to it in flight cancels it as it
/// shuts down, and the future completes with that; from then on nothing is
/// sent to it, as to no ring at all.
#[test]
fn a_ring_dropped_first_completes_what_was_sent_to_it() {
    let foreign = Foreign::start();
    let remote = foreign.remote.clone();
    let (reader, _writer) = pipe();
    let mut read = PortableOp::new(Read::new(reader.as_raw_fd(), Vec::with_capacity(16)));
    assert!(poll_once(&mut read).is_pending());
    block_on_away(PortableOp::new(Nop)).unwrap();
    drop(foreign);
    let (read, _) = block_on_away(read);
    assert_eq!(read.unwrap_err().raw_os_error(), Some(libc::ECANCELED));
    let err = block_on_away(PortableOp::new(Nop)).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::Unsupported);
    // A ring that has closed is no ring either.
    let _serial = serial();
    ruio_driver::set_foreign_ring(&remote);
    let err = block_on_away(PortableOp::new(Nop)).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOSYS));
    ruio_driver::clear_foreign_ring();
}

/// Threads that send many operations at once each get every completion.
#[test]
fn many_threads_send_many_operations_at_once() {
    const THREADS: usize = 8;
    const OPERATIONS: usize = 2000;
    let _foreign = Foreign::start();
    let threads: Vec<_> = (0..THREADS)
        .map(|_| {
            thread::spawn(|| {
                block_on_away(async {
                    for _ in 0..OPERATIONS {
                        PortableOp::new(Nop).await.unwrap();
                    }
                });
                // And some dropped as soon as they are sent.
                for _ in 0..OPERATIONS / 10 {
                    let mut nop = PortableOp::new(Nop);
                    assert!(poll_once(&mut nop).is_pending());
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
}

/// A wait polled again with the same events stays the one entry sent.
#[test]
fn a_ready_wait_sends_its_poll_once() {
    let _foreign = Foreign::start();
    let (reader, mut writer) = pipe();
    let reader = PortableFd::new(reader.into());
    let mut wait = ReadyWait::new();
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..3 {
        assert!(reader.poll_ready(&mut cx, READABLE, &mut wait).is_pending());
    }
    writer.write_all(b"x").unwrap();
    let ready = block_on_away(poll_fn(|cx| reader.poll_ready(cx, READABLE, &mut wait)));
    assert_ne!(ready.unwrap() & READABLE, 0);
    assert!(!wait.is_waiting());
}
