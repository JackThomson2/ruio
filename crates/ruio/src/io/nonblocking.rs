//! Descriptors read and written with system calls made at once, which wait
//! on the ring only when there is nothing to take or no room: the parent's
//! ends of a child's pipes.
//!
//! A read or a write is a system call, made on whichever thread polls it,
//! which takes what has arrived, or gives what there is room for, with no
//! turn of the ring's loop. Only one that finds the descriptor empty, or
//! full, waits, with a poll on the ring of the thread that polls it
//! ([`PortableFd::poll_ready`]), and is made again once the descriptor is
//! ready. Through the ring instead, each read and write would take a turn of
//! the loop, and a read that finds nothing would fail in the kernel first,
//! which then arms a poll of its own and tries again. A wait whose poll is
//! still in flight, polled again, polls its poll before anything else: the
//! task was woken for something else, and the system call would find what
//! the poll says it would.
//!
//! A descriptor that is ready again and again, as one a peer keeps full, is
//! read with no wait at all, so each read and write spends a step of the
//! task's budget ([`coop`]), and once the budget is spent the task yields.

use std::fmt;
use std::future::Future;
use std::io;
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, OwnedFd};
use std::pin::Pin;
use std::slice;
use std::task::{Context, Poll, ready};

use ruio_driver::buf::{BufResult, IoBuf, IoBufMut};
use ruio_driver::{PortableFd, ReadyWait};

use crate::coop;

/// The events a read waits for.
const READABLE: u32 = libc::POLLIN as u32;

/// The events a write waits for.
const WRITABLE: u32 = libc::POLLOUT as u32;

/// A descriptor made nonblocking, read and written with system calls made
/// at once, which waits with polls on the ring of the thread that polls each
/// wait.
pub(crate) struct Nonblocking {
    fd: PortableFd,
}

impl Nonblocking {
    /// Takes over `fd`, made nonblocking with one `ioctl(2)`, which changes
    /// the open file, and so every descriptor of it.
    pub(crate) fn new(fd: OwnedFd) -> io::Result<Self> {
        set_nonblocking(&fd, true)?;
        Ok(Self {
            fd: PortableFd::new(fd),
        })
    }

    /// The descriptor, which closes in order after the waits queued for it.
    #[inline]
    pub(crate) fn fd(&self) -> &PortableFd {
        &self.fd
    }

    /// The descriptor, blocking again, as the standard library's types are.
    pub(crate) fn into_owned_fd(self) -> OwnedFd {
        let fd = self.fd.into_owned_fd();
        // A descriptor that stays nonblocking reads and writes all the same.
        let _ = set_nonblocking(&fd, false);
        fd
    }

    /// Reads into `buf` with one `read(2)`, made at once: what has arrived,
    /// 0 at the end, or `WouldBlock` if nothing has.
    pub(crate) fn try_read(&self, buf: &mut [MaybeUninit<u8>]) -> io::Result<usize> {
        loop {
            // SAFETY: read(2) writes at most `buf.len()` bytes into `buf`,
            // which has room for them.
            let read =
                unsafe { libc::read(self.fd.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
            match transferred(read) {
                Some(result) => return result,
                // Interrupted by a signal before it read anything.
                None => continue,
            }
        }
    }

    /// Writes `buf` with one `write(2)`, made at once: as much as there is
    /// room for, or `WouldBlock` if there is none.
    pub(crate) fn try_write(&self, buf: &[u8]) -> io::Result<usize> {
        loop {
            // SAFETY: write(2) reads the `buf.len()` bytes of `buf`.
            let written =
                unsafe { libc::write(self.fd.as_raw_fd(), buf.as_ptr().cast(), buf.len()) };
            match transferred(written) {
                Some(result) => return result,
                // Interrupted by a signal before it wrote anything.
                None => continue,
            }
        }
    }

    /// Reads into `buf` what has arrived, 0 at the end, waiting with `wait`
    /// for something to arrive if nothing has; see the module's
    /// documentation.
    ///
    /// # Panics
    ///
    /// If nothing has arrived where no runtime runs.
    pub(crate) fn poll_read(
        &self,
        cx: &mut Context<'_>,
        buf: &mut [MaybeUninit<u8>],
        wait: &mut ReadyWait,
    ) -> Poll<io::Result<usize>> {
        self.poll_transfer(cx, READABLE, wait, |io| io.try_read(buf))
    }

    /// Writes as much of `buf` as there is room for, waiting with `wait` for
    /// room if there is none.
    ///
    /// # Panics
    ///
    /// If there is no room where no runtime runs.
    pub(crate) fn poll_write(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        wait: &mut ReadyWait,
    ) -> Poll<io::Result<usize>> {
        self.poll_transfer(cx, WRITABLE, wait, |io| io.try_write(buf))
    }

    /// Runs `transfer`, a system call made at once, once the task's budget
    /// allows, waiting with `wait` for `events` while it finds the descriptor
    /// not ready.
    #[inline]
    fn poll_transfer(
        &self,
        cx: &mut Context<'_>,
        events: u32,
        wait: &mut ReadyWait,
        mut transfer: impl FnMut(&Self) -> io::Result<usize>,
    ) -> Poll<io::Result<usize>> {
        let proceed = ready!(coop::poll_proceed(cx));
        loop {
            if wait.is_waiting() {
                // Woken for something else, or by the poll: the poll knows
                // which, and costs no system call to ask.
                ready!(self.fd.poll_ready(cx, events, wait))?;
            }
            match transfer(self) {
                Ok(done) => {
                    proceed.made_progress();
                    return Poll::Ready(Ok(done));
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    ready!(self.fd.poll_ready(cx, events, wait))?;
                }
                Err(err) => return Poll::Ready(Err(err)),
            }
        }
    }

    /// Reads into `buf`, up to its capacity, what has arrived, waiting for
    /// something to if nothing has; 0 bytes at the end.
    #[inline]
    pub(crate) fn read<B: IoBufMut>(&self, buf: B) -> PortableRead<'_, B> {
        PortableRead {
            io: self,
            buf: Some(buf),
            wait: ReadyWait::new(),
        }
    }

    /// Writes `buf`'s initialized bytes, or as many as there is room for,
    /// waiting for room if there is none.
    #[inline]
    pub(crate) fn write<B: IoBuf>(&self, buf: B) -> PortableWrite<'_, B> {
        PortableWrite {
            io: self,
            buf: Some(buf),
            wait: ReadyWait::new(),
        }
    }
}

impl fmt::Debug for Nonblocking {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Nonblocking")
            .field("fd", &self.fd.as_raw_fd())
            .finish()
    }
}

/// Makes `fd` nonblocking, or blocking again, with one `ioctl(2)`.
fn set_nonblocking(fd: &OwnedFd, nonblocking: bool) -> io::Result<()> {
    let on = libc::c_int::from(nonblocking);
    // SAFETY: FIONBIO reads the one `c_int` it is given.
    if unsafe { libc::ioctl(fd.as_raw_fd(), libc::FIONBIO, &raw const on) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// What a system call that moved bytes returned: how many, or the error it
/// set, `EAGAIN` as `WouldBlock`; or none if a signal interrupted it before
/// it moved anything, for the caller to make it again.
#[inline]
fn transferred(result: isize) -> Option<io::Result<usize>> {
    if let Ok(done) = usize::try_from(result) {
        return Some(Ok(done));
    }
    let err = io::Error::last_os_error();
    (err.kind() != io::ErrorKind::Interrupted).then_some(Err(err))
}

/// A read of a descriptor with a system call made at once, which waits on
/// the ring of the thread that polls it only if nothing has arrived: made by
/// the `read` of a child's standard output and error. The output is
/// how many bytes were read, 0 at the end, and the buffer, of which that many
/// are then initialized from its start.
///
/// It spends a step of the task's budget ([`coop`]) when it reads, so a
/// descriptor that always has something to read cannot hold up the thread.
/// Dropped while it waits, it lets go of its poll; nothing is read but
/// what it returns, so nothing is lost.
///
/// # Panics
///
/// Polled where no runtime runs, if nothing has arrived; and polled after it
/// completed.
#[must_use = "a read does nothing until polled"]
pub struct PortableRead<'a, B> {
    io: &'a Nonblocking,
    /// Taken as the read completes.
    buf: Option<B>,
    /// The wait for the descriptor to be readable, once it was found empty.
    wait: ReadyWait,
}

impl<B: IoBufMut> Future for PortableRead<'_, B> {
    type Output = BufResult<usize, B>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let buf = this
            .buf
            .as_mut()
            .expect("`PortableRead` polled after it completed");
        let room = buf.bytes_total();
        // SAFETY: the buffer has room for `bytes_total` bytes from its
        // pointer, which the slice borrows, mutably, for no longer than the
        // buffer is borrowed; the read writes none but initialized bytes.
        let into = unsafe { slice::from_raw_parts_mut(buf.stable_mut_ptr().cast(), room) };
        let read = ready!(this.io.poll_read(cx, into, &mut this.wait));
        let mut buf = this.buf.take().expect("taken once, as the read completes");
        Poll::Ready(match read {
            Ok(read) => {
                let read = read.min(room);
                // SAFETY: read(2) initialized the first `read` bytes.
                unsafe { buf.set_init(read) };
                (Ok(read), buf)
            }
            Err(err) => (Err(err), buf),
        })
    }
}

// Nothing is pinned: the buffer is only ever moved.
impl<B> Unpin for PortableRead<'_, B> {}

impl<B> fmt::Debug for PortableRead<'_, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PortableRead")
            .field("io", &self.io)
            .field("wait", &self.wait)
            .finish_non_exhaustive()
    }
}

/// A write to a descriptor with a system call made at once, which waits on
/// the ring of the thread that polls it only if there is no room: made by
/// the `write` of a child's standard input. The
/// output is how many bytes were written, and the buffer.
///
/// It spends a step of the task's budget ([`coop`]) when it writes. Dropped
/// while it waits, it lets go of its poll, having written nothing.
///
/// # Panics
///
/// Polled where no runtime runs, if there is no room; and polled after it
/// completed.
#[must_use = "a write does nothing until polled"]
pub struct PortableWrite<'a, B> {
    io: &'a Nonblocking,
    /// Taken as the write completes.
    buf: Option<B>,
    /// The wait for the descriptor to be writable, once it was found full.
    wait: ReadyWait,
}

impl<B: IoBuf> Future for PortableWrite<'_, B> {
    type Output = BufResult<usize, B>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let buf = this
            .buf
            .as_ref()
            .expect("`PortableWrite` polled after it completed");
        // SAFETY: the buffer's first `bytes_init` bytes from its pointer are
        // initialized, which the slice borrows for no longer than the buffer
        // is borrowed.
        let from = unsafe { slice::from_raw_parts(buf.stable_ptr(), buf.bytes_init()) };
        let written = ready!(this.io.poll_write(cx, from, &mut this.wait));
        let buf = this.buf.take().expect("taken once, as the write completes");
        Poll::Ready((written, buf))
    }
}

// Nothing is pinned: the buffer is only ever moved.
impl<B> Unpin for PortableWrite<'_, B> {}

impl<B> fmt::Debug for PortableWrite<'_, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PortableWrite")
            .field("io", &self.io)
            .field("wait", &self.wait)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::io::{Read as _, Write as _};
    use std::rc::Rc;
    use std::time::Duration;

    use super::*;
    use crate::Runtime;

    fn run<F: Future>(future: F) -> F::Output {
        Runtime::new()
            .unwrap()
            .block_on(crate::time::timeout(Duration::from_secs(20), future))
            .expect("finished in time")
    }

    fn assert_send_sync<T: Send + Sync>(_: &T) {}

    /// A pipe whose reading end is nonblocking.
    fn reading() -> (Nonblocking, std::io::PipeWriter) {
        let (reader, writer) = std::io::pipe().unwrap();
        (Nonblocking::new(reader.into()).unwrap(), writer)
    }

    /// A pipe whose writing end is nonblocking.
    fn writing() -> (std::io::PipeReader, Nonblocking) {
        let (reader, writer) = std::io::pipe().unwrap();
        (reader, Nonblocking::new(writer.into()).unwrap())
    }

    fn is_nonblocking(fd: &impl AsRawFd) -> bool {
        // SAFETY: F_GETFL reads the descriptor's flags and touches no memory.
        let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
        assert!(flags >= 0);
        flags & libc::O_NONBLOCK != 0
    }

    /// A read waits for something to arrive, reads it, and reads 0 at the
    /// end; the futures are `Send` and `Sync`.
    #[test]
    fn a_read_waits_for_data_and_reads_what_has_arrived() {
        run(async {
            let (reader, mut writer) = reading();
            let read = reader.read(Vec::with_capacity(16));
            assert_send_sync(&read);
            let later = std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(20));
                writer.write_all(b"later").unwrap();
                writer
            });
            let (read, buf) = read.await;
            assert_eq!((read.unwrap(), &buf[..]), (5, &b"later"[..]));
            drop(later.join().unwrap());
            let (read, _) = reader.read(Vec::with_capacity(16)).await;
            assert_eq!(read.unwrap(), 0, "the end");
        });
    }

    /// A write that finds the pipe full waits for room, and goes on as the
    /// other end reads; one whose reader has gone fails with `EPIPE`.
    #[test]
    fn a_write_waits_for_room_and_reports_a_reader_gone() {
        run(async {
            let (mut reader, writer) = writing();
            let chunk = vec![7u8; 64 << 10];
            loop {
                match writer.try_write(&chunk) {
                    Ok(_) => {}
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                    Err(err) => panic!("{err}"),
                }
            }
            let draining = std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(20));
                let mut sink = vec![0; 1 << 20];
                assert!(reader.read(&mut sink).unwrap() > 0);
                reader
            });
            let write = writer.write(chunk);
            assert_send_sync(&write);
            let (written, chunk) = write.await;
            assert!(written.unwrap() > 0);
            drop(draining.join().unwrap());
            let (written, _) = writer.write(chunk).await;
            assert_eq!(written.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
        });
    }

    /// A read with no wait in flight makes its system call at once; one whose
    /// poll is still in flight asks the poll first, and so finds nothing
    /// arrived without a system call while the poll says nothing has.
    #[test]
    fn a_wait_in_flight_is_asked_before_the_system_call() {
        run(async {
            let (reader, mut writer) = reading();
            let mut wait = ReadyWait::new();
            let mut buf = [MaybeUninit::uninit(); 8];
            let polled = std::future::poll_fn(|cx| {
                Poll::Ready(reader.poll_read(cx, &mut buf, &mut wait).is_pending())
            })
            .await;
            assert!(polled, "nothing has arrived");
            assert!(wait.is_waiting());
            writer.write_all(b"x").unwrap();
            let read = std::future::poll_fn(|cx| reader.poll_read(cx, &mut buf, &mut wait)).await;
            assert_eq!(read.unwrap(), 1);
            assert!(!wait.is_waiting());
        });
    }

    /// A descriptor that always has something to read spends the task's
    /// budget, and the task yields, so the others run.
    #[test]
    fn a_descriptor_always_ready_does_not_starve_the_other_tasks() {
        const READS: usize = 4096;
        run(async {
            let (reader, mut writer) = reading();
            writer.write_all(&[1; READS]).unwrap();
            let done = Rc::new(Cell::new(0));
            let reads = crate::spawn({
                let done = done.clone();
                async move {
                    for _ in 0..READS {
                        let (read, _) = reader.read(Vec::with_capacity(1)).await;
                        assert_eq!(read.unwrap(), 1);
                        done.set(done.get() + 1);
                    }
                }
            });
            let seen = crate::spawn(async move { done.get() }).await.unwrap();
            assert!(0 < seen && seen < READS / 4, "{seen} reads");
            reads.await.unwrap();
            drop(writer);
        });
    }

    /// The descriptor taken out is blocking again.
    #[test]
    fn the_descriptor_taken_out_is_blocking_again() {
        let (reader, _writer) = reading();
        assert!(is_nonblocking(reader.fd()));
        let fd = reader.into_owned_fd();
        assert!(!is_nonblocking(&fd));
    }
}
