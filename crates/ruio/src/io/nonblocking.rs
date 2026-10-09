//! Descriptors read and written with system calls made at once, which wait
//! on the ring only when there is nothing to take or no room: the parent's
//! ends of a child's pipes, and the portable sockets.
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
use std::io::{self, IoSlice};
use std::mem::{self, MaybeUninit};
use std::os::fd::{AsRawFd, OwnedFd};
use std::pin::Pin;
use std::slice;
use std::task::{Context, Poll, ready};

use ruio_driver::buf::{BufResult, IoBuf, IoBufMut, IoBufs};
use ruio_driver::{PortableFd, ReadyWait};

use crate::coop;

/// The events a read waits for.
const READABLE: u32 = libc::POLLIN as u32;

/// The events a write waits for.
const WRITABLE: u32 = libc::POLLOUT as u32;

/// The most buffers one vectored write takes, as Linux's `IOV_MAX`; a write
/// given more writes what the first of them hold.
const MAX_IOVECS: usize = 1024;

/// The most buffers of an owned vector one write of
/// [`PortableWriteAllVectored`] takes, from the stack: a vector of more is
/// written with as many writes as it takes.
const OWNED_IOVECS: usize = 64;

/// What a nonblocking descriptor is, which says how it is written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    /// A pipe, a FIFO or a character device, written with `write(2)` and
    /// `writev(2)`.
    Pipe,
    /// A socket, written with `send(2)` and `sendmsg(2)` with
    /// `MSG_NOSIGNAL`, so that one whose peer has gone fails with `EPIPE`
    /// rather than raising `SIGPIPE`, whatever the process does with it.
    Socket,
}

/// A descriptor made nonblocking, read and written with system calls made
/// at once, which waits with polls on the ring of the thread that polls each
/// wait.
pub(crate) struct Nonblocking {
    fd: PortableFd,
    kind: Kind,
}

impl Nonblocking {
    /// Takes over `fd`, made nonblocking with one `ioctl(2)`, which changes
    /// the open file, and so every descriptor of it.
    pub(crate) fn new(fd: OwnedFd, kind: Kind) -> io::Result<Self> {
        set_nonblocking(&fd, true)?;
        Ok(Self::from_nonblocking(fd, kind))
    }

    /// Takes over `fd`, nonblocking already, as a socket made or accepted
    /// with `SOCK_NONBLOCK` is.
    pub(crate) fn from_nonblocking(fd: OwnedFd, kind: Kind) -> Self {
        Self {
            fd: PortableFd::new(fd),
            kind,
        }
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

    /// Writes `buf` with one system call, made at once: as much as there is
    /// room for, or `WouldBlock` if there is none.
    pub(crate) fn try_write(&self, buf: &[u8]) -> io::Result<usize> {
        let fd = self.fd.as_raw_fd();
        loop {
            let written = match self.kind {
                // SAFETY: write(2) reads the `buf.len()` bytes of `buf`.
                Kind::Pipe => unsafe { libc::write(fd, buf.as_ptr().cast(), buf.len()) },
                // SAFETY: send(2) reads the `buf.len()` bytes of `buf`.
                Kind::Socket => unsafe {
                    libc::send(fd, buf.as_ptr().cast(), buf.len(), libc::MSG_NOSIGNAL)
                },
            };
            match transferred(written) {
                Some(result) => return result,
                // Interrupted by a signal before it wrote anything.
                None => continue,
            }
        }
    }

    /// Writes `bufs`, the first [`MAX_IOVECS`] of them, one after the other,
    /// with one system call, made at once: as much as there is room for, or
    /// `WouldBlock` if there is none.
    pub(crate) fn try_write_vectored(&self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        let bufs = &bufs[..bufs.len().min(MAX_IOVECS)];
        // An `IoSlice` has the layout of an `iovec`, which it is documented
        // to be on Unix, and borrows the bytes it names.
        self.write_iovecs(bufs.as_ptr().cast(), bufs.len())
    }

    /// Writes the initialized bytes of `bufs` past the first `skip`, of up
    /// to [`OWNED_IOVECS`] buffers, one after the other, with one system
    /// call, made at once.
    fn try_write_bufs<V: IoBufs>(&self, bufs: &V, skip: usize) -> io::Result<usize> {
        let empty = libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 0,
        };
        let mut iovecs = [empty; OWNED_IOVECS];
        let mut count = 0;
        let mut skip = skip;
        for index in 0..bufs.buf_count() {
            if count == OWNED_IOVECS {
                break;
            }
            let (start, len) = bufs.buf(index);
            if skip >= len {
                skip -= len;
                continue;
            }
            iovecs[count] = libc::iovec {
                // SAFETY: `skip` is less than `len`, so the pointer stays in
                // the buffer's initialized bytes.
                iov_base: unsafe { start.add(skip) }.cast_mut().cast(),
                iov_len: len - skip,
            };
            skip = 0;
            count += 1;
        }
        // The buffers keep their bytes in place while `bufs` is borrowed.
        self.write_iovecs(iovecs.as_ptr(), count)
    }

    /// Writes the `count` `iovec`s at `iovecs`, one after the other, with
    /// one `writev(2)` or `sendmsg(2)`, made at once. Each must name bytes
    /// that stay readable for the call.
    fn write_iovecs(&self, iovecs: *const libc::iovec, count: usize) -> io::Result<usize> {
        let fd = self.fd.as_raw_fd();
        loop {
            let written = match self.kind {
                Kind::Pipe => {
                    // At most `MAX_IOVECS`, which an `int` holds.
                    let count = count as libc::c_int;
                    // SAFETY: writev(2) reads the `count` `iovec`s and the
                    // bytes each names, which the caller keeps readable.
                    unsafe { libc::writev(fd, iovecs, count) }
                }
                Kind::Socket => {
                    // SAFETY: all zeroes is a valid `msghdr`, with null
                    // pointers.
                    let mut header: libc::msghdr = unsafe { mem::zeroed() };
                    header.msg_iov = iovecs.cast_mut();
                    header.msg_iovlen = count;
                    // SAFETY: sendmsg(2) reads the header, the `count`
                    // `iovec`s it names and the bytes each names, which the
                    // caller keeps readable, and writes none of them.
                    unsafe { libc::sendmsg(fd, &raw const header, libc::MSG_NOSIGNAL) }
                }
            };
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

    /// Writes as much of `bufs`, one after the other, as there is room for,
    /// waiting with `wait` for room if there is none.
    ///
    /// # Panics
    ///
    /// If there is no room where no runtime runs.
    pub(crate) fn poll_write_vectored(
        &self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        wait: &mut ReadyWait,
    ) -> Poll<io::Result<usize>> {
        self.poll_transfer(cx, WRITABLE, wait, |io| io.try_write_vectored(bufs))
    }

    /// Waits with `wait` for the descriptor to have any of `events`, as
    /// [`PortableFd::poll_ready`] does.
    #[inline]
    pub(crate) fn poll_ready(
        &self,
        cx: &mut Context<'_>,
        events: u32,
        wait: &mut ReadyWait,
    ) -> Poll<io::Result<u32>> {
        self.fd.poll_ready(cx, events, wait)
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

    /// Reads until `buf` is full, with as many reads as it takes, waiting
    /// whenever nothing has arrived.
    #[inline]
    pub(crate) fn read_exact<B: IoBufMut>(&self, buf: B) -> PortableReadExact<'_, B> {
        PortableReadExact {
            io: self,
            buf: Some(buf),
            done: 0,
            wait: ReadyWait::new(),
        }
    }

    /// Writes all of `buf`'s initialized bytes, with as many writes as it
    /// takes, waiting for room whenever there is none.
    #[inline]
    pub(crate) fn write_all<B: IoBuf>(&self, buf: B) -> PortableWriteAll<'_, B> {
        PortableWriteAll {
            io: self,
            buf: Some(buf),
            done: 0,
            wait: ReadyWait::new(),
        }
    }

    /// Writes every initialized byte of `bufs`, one after the other, with as
    /// many writes as it takes, waiting for room whenever there is none.
    #[inline]
    pub(crate) fn write_all_vectored<V: IoBufs>(&self, bufs: V) -> PortableWriteAllVectored<'_, V> {
        PortableWriteAllVectored {
            io: self,
            bufs: Some(bufs),
            done: 0,
            wait: ReadyWait::new(),
        }
    }
}

impl fmt::Debug for Nonblocking {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Nonblocking")
            .field("fd", &self.fd.as_raw_fd())
            .field("kind", &self.kind)
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
/// the `read` of the portable sockets and of a child's standard output and
/// error. The output is
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
/// the `write` of the portable sockets and of a child's standard input. The
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

/// A read that fills a whole buffer from a descriptor, with system calls
/// made at once, as many as it takes, which wait on the ring of the thread
/// that polls them only while nothing has arrived: made by the `read_exact`
/// of the portable sockets.
///
/// The read fills the buffer from its start, up to its
/// [`bytes_total`](IoBuf::bytes_total). The output is the buffer, and
/// `Ok(())` once it is full, or the error that stopped the read:
/// [`io::ErrorKind::UnexpectedEof`] if the descriptor ended first. What
/// arrived is marked initialized either way, as each read arrives.
///
/// Its fields hold no type of a trait's, so a future that must be `Send`
/// can hold it for any buffer that is `Send` and `Sync`, a borrowed one
/// among them; the same goes for [`PortableWriteAll`].
///
/// # Panics
///
/// Polled where no runtime runs, if nothing has arrived; and polled after it
/// completed.
#[must_use = "a read does nothing until polled"]
pub struct PortableReadExact<'a, B> {
    io: &'a Nonblocking,
    /// Taken as the read completes.
    buf: Option<B>,
    /// How many bytes have been read.
    done: usize,
    /// The wait for the descriptor to be readable, once it was found empty.
    wait: ReadyWait,
}

impl<B: IoBufMut> Future for PortableReadExact<'_, B> {
    type Output = BufResult<(), B>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let buf = this
            .buf
            .as_mut()
            .expect("`PortableReadExact` polled after it completed");
        let total = buf.bytes_total();
        let mut failed = None;
        while this.done < total {
            // SAFETY: `done` is less than `total`, so the pointer stays in the
            // buffer's room.
            let start = unsafe { buf.stable_mut_ptr().add(this.done) };
            // SAFETY: the buffer has room for `total` bytes from its pointer,
            // of which the slice borrows those past `done`, mutably, for no
            // longer than the buffer is borrowed; the read writes none but
            // initialized bytes.
            let into = unsafe { slice::from_raw_parts_mut(start.cast(), total - this.done) };
            let read = ready!(
                this.io
                    .poll_transfer(cx, READABLE, &mut this.wait, |io| { io.try_read(into) })
            );
            match read {
                Ok(0) => {
                    failed = Some(io::ErrorKind::UnexpectedEof.into());
                    break;
                }
                Ok(read) => {
                    this.done += read.min(total - this.done);
                    // SAFETY: the reads initialized the first `done` bytes.
                    unsafe { buf.set_init(this.done) };
                }
                Err(err) => {
                    failed = Some(err);
                    break;
                }
            }
        }
        let buf = this.buf.take().expect("taken once, as the read completes");
        Poll::Ready((failed.map_or(Ok(()), Err), buf))
    }
}

// Nothing is pinned: the buffer is only ever moved.
impl<B> Unpin for PortableReadExact<'_, B> {}

impl<B> fmt::Debug for PortableReadExact<'_, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PortableReadExact")
            .field("io", &self.io)
            .field("done", &self.done)
            .field("wait", &self.wait)
            .finish_non_exhaustive()
    }
}

/// A write of all of a buffer's initialized bytes to a descriptor, with
/// system calls made at once, as many as it takes, which wait on the ring of
/// the thread that polls them only while there is no room: made by the
/// `write_all` of the portable sockets. The output is the buffer, with how
/// the writes went: a write that writes nothing fails with `WriteZero`.
///
/// # Panics
///
/// Polled where no runtime runs, if there is no room; and polled after it
/// completed.
#[must_use = "a write does nothing until polled"]
pub struct PortableWriteAll<'a, B> {
    io: &'a Nonblocking,
    /// Taken as the write completes.
    buf: Option<B>,
    /// How many bytes have been written.
    done: usize,
    /// The wait for the descriptor to be writable, once it was found full.
    wait: ReadyWait,
}

impl<B: IoBuf> Future for PortableWriteAll<'_, B> {
    type Output = BufResult<(), B>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let buf = this
            .buf
            .as_ref()
            .expect("`PortableWriteAll` polled after it completed");
        let total = buf.bytes_init();
        let mut failed = None;
        while this.done < total {
            // SAFETY: `done` is less than `total`, so the pointer stays in the
            // buffer's initialized bytes.
            let start = unsafe { buf.stable_ptr().add(this.done) };
            // SAFETY: the buffer's first `total` bytes from its pointer are
            // initialized, of which the slice borrows those past `done` for no
            // longer than the buffer is borrowed.
            let from = unsafe { slice::from_raw_parts(start, total - this.done) };
            let written = ready!(
                this.io
                    .poll_transfer(cx, WRITABLE, &mut this.wait, |io| { io.try_write(from) })
            );
            match written {
                Ok(0) => {
                    failed = Some(io::ErrorKind::WriteZero.into());
                    break;
                }
                Ok(written) => this.done += written,
                Err(err) => {
                    failed = Some(err);
                    break;
                }
            }
        }
        let buf = this.buf.take().expect("taken once, as the write completes");
        Poll::Ready((failed.map_or(Ok(()), Err), buf))
    }
}

// Nothing is pinned: the buffer is only ever moved.
impl<B> Unpin for PortableWriteAll<'_, B> {}

impl<B> fmt::Debug for PortableWriteAll<'_, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PortableWriteAll")
            .field("io", &self.io)
            .field("done", &self.done)
            .field("wait", &self.wait)
            .finish_non_exhaustive()
    }
}

/// A write of every initialized byte of several buffers, one after the
/// other, to a descriptor with system calls made at once, as many as it
/// takes, each of up to 64 of the buffers, which wait on the ring of the
/// thread that polls them only while there is no room: made by the
/// `write_all_vectored` of the portable sockets. The output is the buffers,
/// with how the writes went: a write that writes nothing fails with
/// `WriteZero`.
///
/// It spends a step of the task's budget ([`coop`]) with each write that
/// moves data. Dropped part way, it has written what it has, and no more.
///
/// # Panics
///
/// Polled where no runtime runs, if there is no room; and polled after it
/// completed.
#[must_use = "a write does nothing until polled"]
pub struct PortableWriteAllVectored<'a, V> {
    io: &'a Nonblocking,
    /// Taken as the write completes.
    bufs: Option<V>,
    /// How many bytes have been written.
    done: usize,
    /// The wait for the descriptor to be writable, once it was found full.
    wait: ReadyWait,
}

impl<V: IoBufs> Future for PortableWriteAllVectored<'_, V> {
    type Output = BufResult<(), V>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let bufs = this
            .bufs
            .as_ref()
            .expect("`PortableWriteAllVectored` polled after it completed");
        let total = bufs.bytes_init();
        while this.done < total {
            let done = this.done;
            let written = ready!(this.io.poll_transfer(cx, WRITABLE, &mut this.wait, |io| {
                io.try_write_bufs(bufs, done)
            }));
            match written {
                Ok(0) => {
                    let bufs = this
                        .bufs
                        .take()
                        .expect("taken once, as the write completes");
                    return Poll::Ready((Err(io::ErrorKind::WriteZero.into()), bufs));
                }
                Ok(written) => this.done += written,
                Err(err) => {
                    let bufs = this
                        .bufs
                        .take()
                        .expect("taken once, as the write completes");
                    return Poll::Ready((Err(err), bufs));
                }
            }
        }
        let bufs = this
            .bufs
            .take()
            .expect("taken once, as the write completes");
        Poll::Ready((Ok(()), bufs))
    }
}

// Nothing is pinned: the buffers are only ever moved.
impl<V> Unpin for PortableWriteAllVectored<'_, V> {}

impl<V> fmt::Debug for PortableWriteAllVectored<'_, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PortableWriteAllVectored")
            .field("io", &self.io)
            .field("done", &self.done)
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
        (Nonblocking::new(reader.into(), Kind::Pipe).unwrap(), writer)
    }

    /// A pipe whose writing end is nonblocking.
    fn writing() -> (std::io::PipeReader, Nonblocking) {
        let (reader, writer) = std::io::pipe().unwrap();
        (reader, Nonblocking::new(writer.into(), Kind::Pipe).unwrap())
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

    /// A socket's writes go with `send`, and fail with `EPIPE` once the peer
    /// has gone; its vectored writes write every buffer in order, borrowed
    /// or owned, past a part already written.
    #[test]
    fn a_socket_is_written_whole_and_reports_a_peer_gone() {
        use std::os::unix::net::UnixStream;

        run(async {
            let (ours, theirs) = UnixStream::pair().unwrap();
            let ours = Nonblocking::new(ours.into(), Kind::Socket).unwrap();
            let theirs = Nonblocking::new(theirs.into(), Kind::Socket).unwrap();
            let mut wait = ReadyWait::new();
            let bufs = [IoSlice::new(b"one "), IoSlice::new(b"two")];
            let written =
                std::future::poll_fn(|cx| ours.poll_write_vectored(cx, &bufs, &mut wait)).await;
            assert_eq!(written.unwrap(), 7);
            assert_eq!(ours.try_write_bufs(&vec!["ab", "cd", "ef"], 3).unwrap(), 3);
            let owned = vec![" three", " four"];
            let write = ours.write_all_vectored(owned);
            assert_send_sync(&write);
            let (written, _) = write.await;
            written.unwrap();
            let (read, buf) = theirs.read(Vec::with_capacity(64)).await;
            assert_eq!(&buf[..read.unwrap()], b"one twodef three four");
            drop(theirs);
            let (written, _) = ours.write("gone").await;
            assert_eq!(written.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
        });
    }

    /// An owned vector of more buffers than one write takes is written with
    /// as many writes as it takes, through a socket a thread drains.
    #[test]
    fn an_owned_vector_of_many_buffers_is_written_whole() {
        use std::os::unix::net::UnixStream;

        run(async {
            let (ours, mut theirs) = UnixStream::pair().unwrap();
            let ours = Nonblocking::new(ours.into(), Kind::Socket).unwrap();
            let bufs: Vec<Vec<u8>> = (0..200u32).map(|index| vec![index as u8; 3000]).collect();
            let expected: Vec<u8> = bufs.concat();
            let total = expected.len();
            let reading = std::thread::spawn(move || {
                let mut got = vec![0; total];
                theirs.read_exact(&mut got).unwrap();
                got
            });
            let (written, _) = ours.write_all_vectored(bufs).await;
            written.unwrap();
            assert!(reading.join().unwrap() == expected);
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
