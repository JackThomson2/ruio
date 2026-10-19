//! Portable operations: queued on the ring of the thread's current driver,
//! and handed back to that ring by futures that move to other threads.

mod support;

use std::future::{Future, poll_fn};
use std::io::{PipeReader, PipeWriter, Write as _};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

use ruio_driver::buf::{IoBuf, IoBufMut};
use ruio_driver::ops::{Map, Nop, OpenAt, Read, Write};
use ruio_driver::{Driver, PortableFd, PortableOp, ReadyWait};

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

/// Where no driver is current, and no foreign ring is set, as none is in
/// this test binary, an operation completes at once with `ENOSYS`, which
/// is `Unsupported`, having reached no kernel: a close's descriptor is
/// closed as the operation goes, as it would be dropped unsubmitted, and a
/// wait for a descriptor fails likewise.
#[test]
fn where_no_driver_is_current_and_no_foreign_ring_is_set_it_is_unsupported() {
    assert!(ruio_driver::foreign_ring().is_none());
    let err = match poll_once(&mut PortableOp::new(Nop)) {
        Poll::Ready(result) => result.unwrap_err(),
        Poll::Pending => panic!("pending with no ring to run on"),
    };
    assert_eq!(err.kind(), std::io::ErrorKind::Unsupported);
    assert_eq!(err.raw_os_error(), Some(libc::ENOSYS));

    let (reader, mut writer) = pipe();
    let close = PortableOp::new(ruio_driver::ops::Close::new(reader.into()));
    assert!(block_on_away(close).is_err());
    assert!(reader_closed(&mut writer), "closed as the close went");

    let (reader, _writer) = pipe();
    let reader = PortableFd::new(reader.into());
    let mut wait = ReadyWait::new();
    let ready = block_on_away(poll_fn(|cx| reader.poll_ready(cx, READABLE, &mut wait)));
    assert_eq!(ready.unwrap_err().kind(), std::io::ErrorKind::Unsupported);
    assert!(!wait.is_waiting());

    // A detached operation is dropped, with nothing to run on.
    let (reader, _writer) = pipe();
    let (buf, drops) = Counted::new();
    PortableOp::new(Read::new(reader.as_raw_fd(), buf)).detach();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
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

/// A future that must be `Send` can hold, across an await, a portable
/// operation on a buffer that only `'static` borrows fit, made in a function
/// generic over the buffer: whether it is `Send` follows from its output's
/// type, which the compiler sees without the operation's implementation.
#[test]
fn a_send_future_holds_an_operation_on_a_static_buffer() {
    async fn write<B: IoBuf>(writer: &PipeWriter, buf: B) -> std::io::Result<usize> {
        PortableOp::new(Write::new(writer.as_raw_fd(), buf)).await.0
    }
    let driver = Driver::new().unwrap();
    let _entered = driver.enter();
    let (reader, writer) = pipe();
    let future: Pin<Box<dyn Future<Output = std::io::Result<usize>> + Send + '_>> =
        Box::pin(async { write(&writer, "static").await });
    assert_eq!(block_on(&driver, future).unwrap(), 6);
    let mut read = [0; 6];
    std::io::Read::read_exact(&mut &reader, &mut read).unwrap();
    assert_eq!(&read, b"static");
}

/// A portable operation whose output cannot cross threads cannot either:
/// a descriptor that may be direct belongs to its ring.
#[test]
fn an_operation_whose_output_cannot_move_stays_put() {
    let open = OpenAt::new("/").unwrap();
    let plain = PortableOp::new(Map::new(open, |opened| opened.map(drop)));
    assert_send(&plain);
}

/// Whether the pipe's reading end is closed, as a write to it finds.
fn reader_closed(writer: &mut PipeWriter) -> bool {
    match writer.write(b"x") {
        Ok(_) => false,
        Err(err) if err.kind() == std::io::ErrorKind::BrokenPipe => true,
        Err(err) => panic!("unexpected error {err}"),
    }
}

/// Dropped on its ring's thread, a portable descriptor closes with an entry
/// queued after the read queued before it.
#[test]
fn a_portable_descriptor_closes_after_what_its_ring_queued() {
    let driver = Driver::new().unwrap();
    let _entered = driver.enter();
    let (reader, mut writer) = pipe();
    let reader = PortableFd::new(reader.into());
    let mut op = reader.op(|fd| Read::new(fd, Vec::with_capacity(16)));
    assert!(poll_once(&mut op).is_pending());
    drop(op);
    drop(reader);
    assert!(!reader_closed(&mut writer), "closed before the ring's turn");
    settle(&driver);
    assert!(reader_closed(&mut writer));
}

/// Dropped on another thread, it closes on its ring, after what the ring
/// queued, once the ring takes the request.
#[test]
fn dropped_on_another_thread_it_closes_on_its_ring() {
    let driver = Driver::new().unwrap();
    let _entered = driver.enter();
    let (reader, mut writer) = pipe();
    let reader = PortableFd::new(reader.into());
    let mut op = reader.op(|fd| Read::new(fd, Vec::with_capacity(16)));
    assert!(poll_once(&mut op).is_pending());
    drop(op);
    thread::spawn(move || drop(reader)).join().unwrap();
    assert!(!reader_closed(&mut writer), "closed before the ring's turn");
    turn_until(&driver, || reader_closed(&mut writer));
    settle(&driver);
}

/// With no operation queued on a ring, or with its ring gone, it closes at
/// once, wherever it is.
#[test]
fn with_no_ring_to_close_on_it_closes_at_once() {
    let (reader, mut writer) = pipe();
    drop(PortableFd::new(reader.into()));
    assert!(reader_closed(&mut writer));

    let driver = Driver::new().unwrap();
    let entered = driver.enter();
    let (reader, mut writer) = pipe();
    let reader = PortableFd::new(reader.into());
    writer.write_all(b"data").unwrap();
    let (read, _) = block_on(
        &driver,
        reader.op(|fd| Read::new(fd, Vec::with_capacity(16))),
    );
    assert_eq!(read.unwrap(), 4);
    drop(entered);
    drop(driver);
    thread::spawn(move || drop(reader)).join().unwrap();
    assert!(reader_closed(&mut writer));
}

/// The descriptor taken out stays open, and closes as an `OwnedFd` does.
#[test]
fn a_descriptor_taken_out_stays_open() {
    let (reader, mut writer) = pipe();
    let reader = PortableFd::new(reader.into()).into_owned_fd();
    assert!(!reader_closed(&mut writer));
    drop(reader);
    assert!(reader_closed(&mut writer));
}

/// What a wait for a pipe's reading end to be readable waits for.
const READABLE: u32 = libc::POLLIN as u32;

/// What a wait for a socket to be writable waits for.
const WRITABLE: u32 = libc::POLLOUT as u32;

#[test]
fn a_ready_wait_is_send_sync_and_unpin() {
    fn assert_traits<T: Send + Sync + Unpin>() {}
    assert_traits::<ReadyWait>();
}

/// A wait on an empty pipe is pending, stays one poll in flight however
/// often it is polled, and completes with the readiness once data arrives;
/// polled again with the data still there, it completes at once.
#[test]
fn a_ready_wait_is_queued_once_and_completes_as_the_descriptor_is_ready() {
    let driver = Driver::new().unwrap();
    let _entered = driver.enter();
    let (reader, mut writer) = pipe();
    let reader = PortableFd::new(reader.into());
    let mut wait = ReadyWait::new();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(reader.poll_ready(&mut cx, READABLE, &mut wait).is_pending());
    assert!(wait.is_waiting());
    for _ in 0..3 {
        driver.submit().unwrap();
        assert!(reader.poll_ready(&mut cx, READABLE, &mut wait).is_pending());
    }
    driver.submit().unwrap();
    assert_eq!(driver.in_flight(), 1, "one poll for every call");
    writer.write_all(b"x").unwrap();
    let ready = block_on(
        &driver,
        poll_fn(|cx| reader.poll_ready(cx, READABLE, &mut wait)),
    );
    assert_ne!(ready.unwrap() & READABLE, 0);
    assert!(!wait.is_waiting());
    let again = block_on(
        &driver,
        poll_fn(|cx| reader.poll_ready(cx, READABLE, &mut wait)),
    );
    assert_ne!(again.unwrap() & READABLE, 0);
    assert_eq!(driver.in_flight(), 0);
}

/// Polled for other events, a wait lets go of its poll, which the ring
/// cancels, and waits for the new ones; dropped, it lets go of it too.
#[test]
fn a_ready_wait_lets_go_of_a_poll_for_other_events_and_as_it_is_dropped() {
    let driver = Driver::new().unwrap();
    let _entered = driver.enter();
    let (socket, _peer) = UnixStream::pair().unwrap();
    let socket = PortableFd::new(socket.into());
    let mut wait = ReadyWait::new();
    let mut cx = Context::from_waker(Waker::noop());
    // Nothing to read.
    assert!(socket.poll_ready(&mut cx, READABLE, &mut wait).is_pending());
    driver.submit().unwrap();
    assert_eq!(driver.in_flight(), 1);
    // Room to write, at once; the poll for reading goes.
    let writable = block_on(
        &driver,
        poll_fn(|cx| socket.poll_ready(cx, WRITABLE, &mut wait)),
    );
    assert_ne!(writable.unwrap() & WRITABLE, 0);
    settle(&driver);
    assert!(socket.poll_ready(&mut cx, READABLE, &mut wait).is_pending());
    driver.submit().unwrap();
    assert_eq!(driver.in_flight(), 1);
    drop(wait);
    settle(&driver);
}

/// Whether the pipe has no reading end left, as the writing end reports
/// with `POLLERR`, which a write would find too but this leaves the pipe as
/// it is: a byte written would make a poll for reading ready.
fn reader_gone(writer: &PipeWriter) -> bool {
    let mut poll = libc::pollfd {
        fd: writer.as_raw_fd(),
        events: libc::POLLOUT,
        revents: 0,
    };
    // SAFETY: poll writes the one `pollfd` it is given, and waits for none.
    let polled = unsafe { libc::poll(&raw mut poll, 1, 0) };
    assert_eq!(
        polled, 1,
        "a pipe's writing end is always ready for something"
    );
    poll.revents & libc::POLLERR != 0
}

/// The descriptor of a wait whose poll is queued closes after the poll,
/// once the ring takes the close queued after it, and the poll keeps the
/// file until it goes.
#[test]
fn a_descriptor_waited_on_closes_after_its_poll() {
    let driver = Driver::new().unwrap();
    let _entered = driver.enter();
    let (reader, writer) = pipe();
    let reader = PortableFd::new(reader.into());
    let mut wait = ReadyWait::new();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(reader.poll_ready(&mut cx, READABLE, &mut wait).is_pending());
    // Queued, not submitted: the close goes after it.
    drop(reader);
    assert!(!reader_gone(&writer), "closed before the ring's turn");
    driver.submit().unwrap();
    assert!(!reader_gone(&writer), "the armed poll holds the file");
    drop(wait);
    settle(&driver);
    assert!(reader_gone(&writer));
}

/// `ready` is the wait as a future of its own.
#[test]
fn ready_waits_with_a_poll_of_its_own() {
    let driver = Driver::new().unwrap();
    let _entered = driver.enter();
    let (socket, _peer) = UnixStream::pair().unwrap();
    let socket = PortableFd::new(socket.into());
    let writable = block_on(&driver, socket.ready(WRITABLE)).unwrap();
    assert_ne!(writable & WRITABLE, 0);
    assert_send(&socket.ready(READABLE));
}

/// A wait whose poll is in flight moves to another thread with its
/// descriptor, and completes there with the readiness its ring sends over.
#[test]
fn a_ready_wait_moved_to_another_thread_completes_from_its_ring() {
    let driver = Driver::new().unwrap();
    let _entered = driver.enter();
    let (reader, mut writer) = pipe();
    let reader = PortableFd::new(reader.into());
    let mut wait = ReadyWait::new();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(reader.poll_ready(&mut cx, READABLE, &mut wait).is_pending());
    driver.submit().unwrap();
    let (polled, first_poll) = mpsc::channel();
    let away = thread::spawn(move || {
        let mut cx = Context::from_waker(Waker::noop());
        assert!(reader.poll_ready(&mut cx, READABLE, &mut wait).is_pending());
        polled.send(()).unwrap();
        let ready = block_on_away(poll_fn(|cx| reader.poll_ready(cx, READABLE, &mut wait)));
        (ready.unwrap(), reader)
    });
    first_poll.recv().unwrap();
    writer.write_all(b"x").unwrap();
    turn_until(&driver, || away.is_finished());
    let (ready, reader) = away.join().unwrap();
    assert_ne!(ready & READABLE, 0);
    drop(reader);
    settle(&driver);
}
