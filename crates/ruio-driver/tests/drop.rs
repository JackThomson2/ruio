//! Dropping operations at every point in their life: before submission,
//! queued, in the kernel, completed but not collected, and collected.
//!
//! [`TrackedRead`] checks, when its buffer is freed, that the kernel is done
//! with it, and each test checks that every payload is freed in the end. Run
//! under AddressSanitizer by `ci/asan.sh`, the tests also catch the driver
//! touching memory it has freed, or leaking memory it could free.

mod support;

use std::cell::Cell;
use std::future::Future;
use std::io::{self, PipeReader, PipeWriter, Write as _};
use std::net::{self, TcpListener, TcpStream, UdpSocket};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::UnixStream;
use std::pin::Pin;
use std::rc::Rc;
use std::time::Duration;

use ruio_driver::ops::{
    Accept, Connect, Nop, Read, Recv, RecvFrom, Send, SendTo, SendVectored, Shutdown, Socket,
    Timeout, Write,
};
use ruio_driver::{Completion, Driver, Op, Operation};
use ruio_sys as sys;

use support::{block_on, poll_once, settle};

/// The only byte ever written to the test pipes, so any byte a read returns
/// can be checked.
const FILL: u8 = b'r';

/// What happened to a [`TrackedRead`].
#[derive(Default)]
struct Track {
    prepared: Cell<bool>,
    result: Cell<Option<i32>>,
    dropped: Cell<bool>,
}

/// A pipe read that checks, when its buffer is freed, that the kernel is
/// done with it.
struct TrackedRead {
    fd: RawFd,
    buf: Box<[u8; 16]>,
    track: Rc<Track>,
}

impl TrackedRead {
    fn new(fd: RawFd) -> (Self, Rc<Track>) {
        let track = Rc::new(Track::default());
        let read = Self {
            fd,
            buf: Box::new([0; 16]),
            track: track.clone(),
        };
        (read, track)
    }
}

// SAFETY: the entry points into the boxed buffer, which stays put when the
// operation moves. A read posts one completion.
unsafe impl Operation for TrackedRead {
    type Output = i32;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        self.track.prepared.set(true);
        sqe.opcode = sys::IORING_OP_READ;
        sqe.fd = self.fd;
        sqe.addr = self.buf.as_mut_ptr().expose_provenance() as u64;
        sqe.len = self.buf.len() as u32;
        sqe.off = u64::MAX;
    }

    fn complete(self, completion: Completion) -> i32 {
        let res = completion.res();
        if let Ok(read) = usize::try_from(res) {
            assert!(read > 0 && read <= self.buf.len(), "read {read} bytes");
            assert!(
                self.buf[..read].iter().all(|&byte| byte == FILL),
                "{:?}",
                self.buf
            );
        } else {
            assert!(
                [-libc::ECANCELED, -libc::ETIMEDOUT].contains(&res),
                "the read failed with {res}"
            );
        }
        self.track.result.set(Some(res));
        res
    }
}

impl Drop for TrackedRead {
    fn drop(&mut self) {
        assert!(
            !self.track.prepared.get() || self.track.result.get().is_some(),
            "the buffer was freed while the kernel may still write to it"
        );
        self.track.dropped.set(true);
    }
}

/// When the test writes to the pipe, relative to the polls.
#[derive(Clone, Copy, Debug)]
enum WriteAt {
    Never,
    Before,
    AfterPoll(usize),
}

fn drop_tracked_read(polls: usize, write_at: WriteAt) {
    let driver = Driver::new().unwrap();
    let (reader, mut writer) = io::pipe().unwrap();
    let (read, track) = TrackedRead::new(reader.as_raw_fd());
    let mut op = driver.op(read);
    if matches!(write_at, WriteAt::Before) {
        writer.write_all(&[FILL; 5]).unwrap();
    }
    for poll in 0..polls {
        if poll_once(&mut op).is_ready() {
            break;
        }
        if matches!(write_at, WriteAt::AfterPoll(after) if after == poll) {
            writer.write_all(&[FILL; 5]).unwrap();
        }
        driver.submit().unwrap();
    }
    drop(op);
    settle(&driver);

    let context = format!("{polls} polls, write {write_at:?}");
    assert!(track.dropped.get(), "{context}: the buffer was never freed");
    assert_eq!(
        track.result.get().is_some(),
        track.prepared.get(),
        "{context}: a read reached the kernel without completing, or the reverse"
    );
}

#[test]
fn tracked_reads_dropped_after_every_number_of_polls() {
    for polls in 0..5 {
        for write_at in [
            WriteAt::Never,
            WriteAt::Before,
            WriteAt::AfterPoll(0),
            WriteAt::AfterPoll(1),
            WriteAt::AfterPoll(2),
        ] {
            drop_tracked_read(polls, write_at);
        }
    }
}

/// Polls `op` up to `polls` times, with a driver pass after each, then drops
/// it and waits until the kernel has nothing left.
fn drop_after<T: Operation>(driver: &Driver, mut op: Op<T>, polls: usize) {
    for _ in 0..polls {
        if poll_once(&mut op).is_ready() {
            break;
        }
        driver.submit().unwrap();
    }
    drop(op);
    settle(driver);
}

#[test]
fn provided_operations_dropped_after_every_number_of_polls() {
    let driver = Driver::new().unwrap();
    let (reader, writer) = io::pipe().unwrap();
    for polls in 0..4 {
        drop_after(&driver, driver.op(Nop), polls);
        drop_after(&driver, driver.op(Timeout::new(Duration::ZERO)), polls);
        drop_after(
            &driver,
            driver.op(Timeout::new(Duration::from_secs(60))),
            polls,
        );
        let read = Read::new(reader.as_raw_fd(), Vec::with_capacity(64));
        drop_after(&driver, driver.op(read), polls);
        let write = Write::new(writer.as_raw_fd(), vec![FILL; 5]);
        drop_after(&driver, driver.op(write), polls);
        // With linked timeouts that fire at once, and that never do.
        for timeout in [Duration::ZERO, Duration::from_secs(60)] {
            drop_after(&driver, driver.op(Nop).timeout(timeout), polls);
            let read = Read::new(reader.as_raw_fd(), Vec::with_capacity(64));
            drop_after(&driver, driver.op(read).timeout(timeout), polls);
        }
    }
    assert_eq!(driver.in_flight(), 0);
}

/// Socket operations, including those that make descriptors and those that
/// hold addresses for the kernel to write, waiting for peers that come on
/// some rounds and never on others.
#[test]
fn socket_operations_dropped_after_every_number_of_polls() {
    let driver = Driver::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let udp = UdpSocket::bind("127.0.0.1:0").unwrap();
    let (stream, _peer) = UnixStream::pair().unwrap();
    let tcp = || Socket::new(libc::AF_INET, libc::SOCK_STREAM, 0);
    for polls in 0..6 {
        let _client = (polls % 2 == 0).then(|| TcpStream::connect(addr).unwrap());
        let accept = Accept::new(listener.as_raw_fd());
        drop_after(&driver, driver.op(accept), polls);
        let accept = Accept::new(listener.as_raw_fd()).direct(&driver);
        drop_after(&driver, driver.op(accept), polls);
        drop_after(&driver, driver.op(tcp()), polls);
        drop_after(&driver, driver.op(tcp().direct(&driver)), polls);
        let socket = block_on(&driver, driver.op(tcp())).unwrap();
        drop_after(&driver, driver.op(Connect::new(socket.fd(), addr)), polls);

        let recv = Recv::new(stream.as_raw_fd(), Vec::with_capacity(16));
        drop_after(&driver, driver.op(recv), polls);
        let recv = RecvFrom::new(udp.as_raw_fd(), Vec::with_capacity(16));
        drop_after(&driver, driver.op(recv), polls);
        let send = Send::new(stream.as_raw_fd(), vec![FILL; 5]);
        drop_after(&driver, driver.op(send), polls);
        let bufs = vec![vec![FILL; 3], vec![FILL; 2]];
        drop_after(
            &driver,
            driver.op(SendVectored::new(stream.as_raw_fd(), bufs)),
            polls,
        );
        let to = udp.local_addr().unwrap();
        let send = SendTo::new(udp.as_raw_fd(), vec![FILL; 5], to);
        drop_after(&driver, driver.op(send), polls);
    }
    let shutdown = Shutdown::new(stream.as_raw_fd(), net::Shutdown::Both);
    drop_after(&driver, driver.op(shutdown), 2);
    assert_eq!(driver.in_flight(), 0);
}

/// A small deterministic pseudo-random number generator.
struct Lcg(u64);

impl Lcg {
    /// A number below `bound`.
    fn below(&mut self, bound: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % bound
    }
}

type Task = Pin<Box<dyn Future<Output = ()>>>;

fn task<T: Operation>(op: Op<T>) -> Task {
    Box::pin(async move { drop(op.await) })
}

/// Operations of every kind at once, polled, driven and dropped in a
/// pseudo-random order, with queues small enough to fill up and overflow,
/// and table slots reused over and over.
#[test]
fn interleaved_operations_dropped_at_random_points() {
    let mut rng = Lcg(0x2545_f491_4f6c_dd1d);
    let pipes: Vec<(PipeReader, PipeWriter)> = (0..8).map(|_| io::pipe().unwrap()).collect();
    let mut tracks = Vec::new();

    for run in 0..6 {
        let driver = Driver::builder().entries(8).build().unwrap();
        for _ in 0..40 {
            let mut tasks: Vec<Task> = (0..12)
                .map(|_| {
                    let (reader, writer) = &pipes[rng.below(pipes.len() as u64) as usize];
                    match rng.below(5) {
                        0 => task(driver.op(Nop)),
                        1 => {
                            let wait = Duration::from_micros(rng.below(2_000));
                            task(driver.op(Timeout::new(wait)))
                        }
                        2 => task(driver.op(Write::new(writer.as_raw_fd(), vec![FILL; 3]))),
                        3 => {
                            let (read, track) = TrackedRead::new(reader.as_raw_fd());
                            tracks.push(track);
                            task(driver.op(read))
                        }
                        _ => {
                            let (read, track) = TrackedRead::new(reader.as_raw_fd());
                            tracks.push(track);
                            // A third fire at once, before the read is dropped.
                            let wait = Duration::from_millis(rng.below(3));
                            task(driver.op(read).timeout(wait))
                        }
                    }
                })
                .collect();
            for _ in 0..rng.below(6) {
                tasks.retain_mut(|task| rng.below(4) != 0 && poll_once(task).is_pending());
                driver.submit().unwrap();
            }
        }
        // Half the drivers wait for everything; the others are dropped with
        // cancelled operations still in the kernel.
        if run % 2 == 0 {
            settle(&driver);
        }
    }

    for (index, track) in tracks.iter().enumerate() {
        assert!(track.dropped.get(), "tracked read {index} was never freed");
    }
    // The run must have produced every outcome, or it proves less than it
    // claims.
    let count = |outcome: fn(&Track) -> bool| tracks.iter().filter(|track| outcome(track)).count();
    let unsubmitted = count(|track| !track.prepared.get());
    let cancelled = count(|track| track.result.get() == Some(-libc::ECANCELED));
    let timed_out = count(|track| track.result.get() == Some(-libc::ETIMEDOUT));
    let read = count(|track| track.result.get().is_some_and(|res| res > 0));
    eprintln!(
        "{} tracked reads: {unsubmitted} unsubmitted, {cancelled} cancelled, \
         {timed_out} timed out, {read} read data",
        tracks.len()
    );
    assert!(unsubmitted > 0 && cancelled > 0 && timed_out > 0 && read > 0);
}
