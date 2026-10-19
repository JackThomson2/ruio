//! A descriptor of any kind, wrapped so that a task that must be `Send` can
//! wait for it to be ready, with the readiness API of tokio's
//! `tokio::io::unix::AsyncFd`: [`AsyncFd`], its guards, and the [`Interest`]
//! and [`Ready`] they speak in.
//!
//! [`readable`](AsyncFd::readable), [`writable`](AsyncFd::writable) and
//! [`ready`](AsyncFd::ready) wait for the descriptor and hand back a guard,
//! with which the owner makes its own nonblocking call, in
//! [`try_io`](AsyncFdReadyGuard::try_io): a call that would block clears
//! the readiness, and the next wait waits again. Code written for tokio's
//! `AsyncFd` moves over by changing the import.
//!
//! The wrapper keeps the readiness it has seen and not cleared, so that a
//! wait it answers costs nothing: no system call, no allocation. Any other
//! wait is one single-shot `IORING_OP_POLL_ADD` as a
//! [`PortableOp`], queued on the ring of the
//! thread that polls it and costing there what an operation bound to that
//! ring does; a future that moves to another thread while its poll is in
//! flight takes the poll along, and the poll's ring sends the readiness
//! over. The poll is level-triggered, as `poll(2)` is: the kernel checks
//! the descriptor as it arms it, so clearing readiness that is still there
//! only costs a poll that completes at once.
//!
//! [`ruio::io::AsyncFd`](super::AsyncFd) is the wrapper of the thread's own
//! ring instead: it is not `Send`, reads and writes the descriptor through
//! the ring with owned buffers, and can report readiness again and again on
//! one multishot poll. This one leaves the reads and writes to the owner's
//! system calls, and is `Send` and `Sync` whenever the owner is, as are the
//! futures of its waits.
//!
//! ```no_run
//! use std::io::Read;
//! use std::os::fd::OwnedFd;
//!
//! use ruio::io::portable::AsyncFd;
//!
//! # async fn run(master: OwnedFd) -> std::io::Result<()> {
//! // A nonblocking descriptor, a PTY master, say, as a file.
//! let master = AsyncFd::new(std::fs::File::from(master))?;
//! let mut buf = [0; 4096];
//! loop {
//!     let mut guard = master.readable().await?;
//!     match guard.try_io(|fd| fd.get_ref().read(&mut buf)) {
//!         Ok(Ok(0)) => break,
//!         Ok(read) => println!("{} bytes", read?),
//!         // Nothing to read after all: the readiness is cleared, and the
//!         // next wait polls again.
//!         Err(_would_block) => continue,
//!     }
//! }
//! # Ok(())
//! # }
//! ```

use std::fmt;
use std::future::{Future, poll_fn};
use std::io;
use std::ops::{self, DerefMut};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, ready};

use ruio_driver::PortableOp;
use ruio_driver::ops::PollAdd;

use crate::coop;

/// A `POLL*` event as the bit of a poll mask.
const fn event(event: libc::c_short) -> u32 {
    event.cast_unsigned() as u32
}

const POLLIN: u32 = event(libc::POLLIN);
const POLLPRI: u32 = event(libc::POLLPRI);
const POLLOUT: u32 = event(libc::POLLOUT);
const POLLERR: u32 = event(libc::POLLERR);
const POLLHUP: u32 = event(libc::POLLHUP);
const POLLNVAL: u32 = event(libc::POLLNVAL);
const POLLRDHUP: u32 = event(libc::POLLRDHUP);

/// The events a poll reports whether asked for or not, and that every wait
/// therefore takes for an answer: a wait that took none of them would arm
/// its poll again, which would complete at once, again and again.
const ALWAYS: u32 = POLLERR | POLLHUP | POLLNVAL;

/// The kinds of readiness a wait asks for: readable, writable, priority or
/// error, or several of them joined with [`add`](Interest::add) or `|`.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Interest(u8);

impl Interest {
    /// Readable: data to read, or the read side closed (`POLLIN`,
    /// `POLLRDHUP`, `POLLHUP`).
    pub const READABLE: Interest = Interest(1);
    /// Writable: room to write, or the write side closed (`POLLOUT`,
    /// `POLLHUP`).
    pub const WRITABLE: Interest = Interest(1 << 1);
    /// Priority: urgent data, such as a socket's out-of-band data or a
    /// change of a PTY master's state (`POLLPRI`).
    pub const PRIORITY: Interest = Interest(1 << 2);
    /// Error: an error pending on the descriptor (`POLLERR`). A poll reports
    /// it whatever it asks for, so every wait comes back with it.
    pub const ERROR: Interest = Interest(1 << 3);

    /// Whether it asks for readable.
    pub const fn is_readable(self) -> bool {
        self.0 & Self::READABLE.0 != 0
    }

    /// Whether it asks for writable.
    pub const fn is_writable(self) -> bool {
        self.0 & Self::WRITABLE.0 != 0
    }

    /// Whether it asks for priority.
    pub const fn is_priority(self) -> bool {
        self.0 & Self::PRIORITY.0 != 0
    }

    /// Whether it asks for error.
    pub const fn is_error(self) -> bool {
        self.0 & Self::ERROR.0 != 0
    }

    /// What it asks for and what `other` does.
    ///
    /// ```
    /// use ruio::io::portable::Interest;
    ///
    /// const BOTH: Interest = Interest::READABLE.add(Interest::WRITABLE);
    /// assert!(BOTH.is_readable() && BOTH.is_writable());
    /// ```
    #[must_use = "this returns the joined interest, without changing either"]
    pub const fn add(self, other: Interest) -> Interest {
        Interest(self.0 | other.0)
    }

    /// What it asks for and `other` does not, or `None` if that is nothing.
    #[must_use = "this returns the remaining interest, without changing either"]
    pub fn remove(self, other: Interest) -> Option<Interest> {
        let rest = self.0 & !other.0;
        (rest != 0).then_some(Interest(rest))
    }

    /// The events a poll for it asks for.
    #[inline]
    const fn asked(self) -> u32 {
        let mut events = 0;
        if self.is_readable() {
            events |= POLLIN | POLLRDHUP;
        }
        if self.is_writable() {
            events |= POLLOUT;
        }
        if self.is_priority() {
            events |= POLLPRI;
        }
        if self.is_error() {
            events |= POLLERR;
        }
        events
    }

    /// The events that answer a wait for it: those its poll asks for, and
    /// those any poll reports.
    #[inline]
    const fn matching(self) -> u32 {
        self.asked() | ALWAYS
    }
}

impl ops::BitOr for Interest {
    type Output = Interest;

    #[inline]
    fn bitor(self, other: Interest) -> Interest {
        self.add(other)
    }
}

impl ops::BitOrAssign for Interest {
    #[inline]
    fn bitor_assign(&mut self, other: Interest) {
        *self = self.add(other);
    }
}

impl fmt::Debug for Interest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names = [
            (Self::READABLE, "READABLE"),
            (Self::WRITABLE, "WRITABLE"),
            (Self::PRIORITY, "PRIORITY"),
            (Self::ERROR, "ERROR"),
        ];
        let mut first = true;
        for (interest, name) in names {
            if self.0 & interest.0 != 0 {
                if !first {
                    f.write_str(" | ")?;
                }
                f.write_str(name)?;
                first = false;
            }
        }
        if first {
            f.write_str("(empty)")?;
        }
        Ok(())
    }
}

/// The readiness a guard holds: the kinds of it the descriptor was found to
/// have, among those its wait asked for, and an error, which every wait
/// reports.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Ready(u8);

impl Ready {
    /// No readiness.
    pub const EMPTY: Ready = Ready(0);
    /// Data to read (`POLLIN`).
    pub const READABLE: Ready = Ready(1);
    /// Room to write (`POLLOUT`).
    pub const WRITABLE: Ready = Ready(1 << 1);
    /// The read side closed: the peer has shut down its writes, or hung up
    /// (`POLLRDHUP`, `POLLHUP`).
    pub const READ_CLOSED: Ready = Ready(1 << 2);
    /// The write side closed: the peer has hung up (`POLLHUP`).
    pub const WRITE_CLOSED: Ready = Ready(1 << 3);
    /// Urgent data (`POLLPRI`).
    pub const PRIORITY: Ready = Ready(1 << 4);
    /// An error pending (`POLLERR`, or `POLLNVAL` for a descriptor that is
    /// not open).
    pub const ERROR: Ready = Ready(1 << 5);
    /// Every kind of readiness.
    pub const ALL: Ready = Ready((1 << 6) - 1);

    /// Whether it holds none.
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Whether it holds [`READABLE`](Ready::READABLE).
    pub const fn is_readable(self) -> bool {
        self.0 & Self::READABLE.0 != 0
    }

    /// Whether it holds [`WRITABLE`](Ready::WRITABLE).
    pub const fn is_writable(self) -> bool {
        self.0 & Self::WRITABLE.0 != 0
    }

    /// Whether it holds [`READ_CLOSED`](Ready::READ_CLOSED).
    pub const fn is_read_closed(self) -> bool {
        self.0 & Self::READ_CLOSED.0 != 0
    }

    /// Whether it holds [`WRITE_CLOSED`](Ready::WRITE_CLOSED).
    pub const fn is_write_closed(self) -> bool {
        self.0 & Self::WRITE_CLOSED.0 != 0
    }

    /// Whether it holds [`PRIORITY`](Ready::PRIORITY).
    pub const fn is_priority(self) -> bool {
        self.0 & Self::PRIORITY.0 != 0
    }

    /// Whether it holds [`ERROR`](Ready::ERROR).
    pub const fn is_error(self) -> bool {
        self.0 & Self::ERROR.0 != 0
    }

    /// The readiness poll `events` give a wait for `interest`.
    fn from_events(events: u32, interest: Interest) -> Ready {
        let mut ready = 0;
        if interest.is_readable() {
            if events & POLLIN != 0 {
                ready |= Self::READABLE.0;
            }
            if events & (POLLRDHUP | POLLHUP) != 0 {
                ready |= Self::READ_CLOSED.0;
            }
        }
        if interest.is_writable() {
            if events & POLLOUT != 0 {
                ready |= Self::WRITABLE.0;
            }
            if events & POLLHUP != 0 {
                ready |= Self::WRITE_CLOSED.0;
            }
        }
        if interest.is_priority() {
            if events & POLLPRI != 0 {
                ready |= Self::PRIORITY.0;
            }
            if events & POLLHUP != 0 {
                ready |= Self::READ_CLOSED.0;
            }
        }
        if events & (POLLERR | POLLNVAL) != 0 {
            ready |= Self::ERROR.0;
        }
        Ready(ready)
    }

    /// The poll events that give it.
    fn events(self) -> u32 {
        let mut events = 0;
        if self.is_readable() {
            events |= POLLIN;
        }
        if self.is_writable() {
            events |= POLLOUT;
        }
        if self.is_read_closed() {
            events |= POLLRDHUP | POLLHUP;
        }
        if self.is_write_closed() {
            events |= POLLHUP;
        }
        if self.is_priority() {
            events |= POLLPRI;
        }
        if self.is_error() {
            events |= POLLERR | POLLNVAL;
        }
        events
    }
}

impl ops::BitOr for Ready {
    type Output = Ready;

    #[inline]
    fn bitor(self, other: Ready) -> Ready {
        Ready(self.0 | other.0)
    }
}

impl ops::BitOrAssign for Ready {
    #[inline]
    fn bitor_assign(&mut self, other: Ready) {
        self.0 |= other.0;
    }
}

impl ops::BitAnd for Ready {
    type Output = Ready;

    #[inline]
    fn bitand(self, other: Ready) -> Ready {
        Ready(self.0 & other.0)
    }
}

impl ops::Sub for Ready {
    type Output = Ready;

    #[inline]
    fn sub(self, other: Ready) -> Ready {
        Ready(self.0 & !other.0)
    }
}

impl fmt::Debug for Ready {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ready")
            .field("is_readable", &self.is_readable())
            .field("is_writable", &self.is_writable())
            .field("is_read_closed", &self.is_read_closed())
            .field("is_write_closed", &self.is_write_closed())
            .field("is_priority", &self.is_priority())
            .field("is_error", &self.is_error())
            .finish()
    }
}

/// The error of [`try_io`](AsyncFdReadyGuard::try_io) whose call would
/// have blocked: the guard's readiness is cleared, and the next wait polls
/// the descriptor again.
#[derive(Debug)]
pub struct TryIoError(());

/// A poll in flight for a wait: a single-shot `IORING_OP_POLL_ADD` for
/// `events`.
struct Wait {
    events: u32,
    op: PortableOp<'static, PollAdd>,
}

/// Polls for `fd` to have any of the events that answer a wait for
/// `interest`, with `cache` holding those seen and not cleared, and `wait`
/// the poll kept from one call to the next: the output is the cached events
/// that answer the wait.
///
/// Events the cache holds answer at once. Otherwise the poll in the wait
/// that `wait` gives, made for the events the interest asks for if it was
/// made for others or is not there, is polled, and the events it completes
/// with go into the cache; `wait` is called only then, so that a wait kept
/// behind a lock is locked only then. A wait that is answered spends a step
/// of the task's budget ([`coop`](crate::coop)), as a primitive that can be
/// ready again and again does.
#[inline]
fn poll_events<W: DerefMut<Target = Option<Wait>>>(
    cache: &AtomicU32,
    fd: RawFd,
    cx: &mut Context<'_>,
    interest: Interest,
    wait: impl FnOnce() -> W,
) -> Poll<io::Result<u32>> {
    let proceed = ready!(coop::poll_proceed(cx));
    let matching = interest.matching();
    // Relaxed: the cache only says which calls are worth making; what they
    // take is the kernel's, which orders it itself.
    let seen = cache.load(Ordering::Relaxed) & matching;
    if seen != 0 {
        proceed.made_progress();
        return Poll::Ready(Ok(seen));
    }
    let mut wait = wait();
    let events = interest.asked();
    if wait.as_ref().is_some_and(|wait| wait.events != events) {
        // Its ring cancels it.
        *wait = None;
    }
    let poll = wait.get_or_insert_with(|| Wait {
        events,
        op: PortableOp::new(PollAdd::new(fd, events)),
    });
    let polled = ready!(Pin::new(&mut poll.op).poll(cx));
    *wait = None;
    let events = polled?;
    proceed.made_progress();
    // Relaxed: as above.
    let seen = cache.fetch_or(events, Ordering::Relaxed) | events;
    Poll::Ready(Ok(seen & matching))
}

/// The wait kept in `slot`, locked.
#[inline]
fn lock(slot: &Mutex<Option<Wait>>) -> MutexGuard<'_, Option<Wait>> {
    slot.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A descriptor of any kind, wrapped so that a task can wait for it to be
/// ready from any thread, with tokio's readiness API; see the
/// [module](self).
///
/// The wrapper owns `inner`, which owns the descriptor: dropping the
/// wrapper drops `inner`, which closes the descriptor however it does, and
/// [`into_inner`](AsyncFd::into_inner) hands it back. The wrapper does not
/// make the descriptor nonblocking, which the owner's calls in
/// [`try_io`](AsyncFdReadyGuard::try_io) need it to be.
///
/// Each wait runs on the ring of the thread that polls it, as a
/// [`PortableOp`] does. The futures of
/// [`readable`](AsyncFd::readable) and its kin each keep their own poll, so
/// that any number of tasks can wait at once; dropped while waiting, they
/// let go of it, which its ring cancels. [`poll_read_ready`](AsyncFd::poll_read_ready)
/// and [`poll_write_ready`](AsyncFd::poll_write_ready) keep one poll for each
/// direction in the wrapper instead, from one call to the next, and wake
/// only the waker of the latest call.
///
/// The wrapper is `Send` and `Sync` when `inner` is, and so are its guards;
/// the futures of its waits borrow it, and are `Send` when `inner` is
/// `Sync`.
pub struct AsyncFd<T: AsRawFd> {
    /// The poll events seen and not cleared.
    cache: AtomicU32,
    /// The read wait of [`poll_read_ready`](AsyncFd::poll_read_ready).
    read: Mutex<Option<Wait>>,
    /// The write wait of [`poll_write_ready`](AsyncFd::poll_write_ready).
    write: Mutex<Option<Wait>>,
    /// Dropped after the waits, whose polls name its descriptor.
    inner: T,
}

impl<T: AsRawFd> AsyncFd<T> {
    /// Wraps `inner`, whose descriptor the waits poll, for readable and
    /// writable readiness.
    ///
    /// # Errors
    ///
    /// None: the result is the shape of tokio's, whose registration can
    /// fail. A wait reports what the kernel says of the descriptor.
    #[inline]
    pub fn new(inner: T) -> io::Result<Self> {
        Self::with_interest(inner, Interest::READABLE | Interest::WRITABLE)
    }

    /// Wraps `inner`, whose descriptor the waits poll, for `interest`.
    ///
    /// The interest is tokio's shape: each wait polls for the interest it is
    /// given, so this is the same as [`new`](AsyncFd::new).
    ///
    /// # Errors
    ///
    /// None, as for [`new`](AsyncFd::new).
    #[inline]
    pub fn with_interest(inner: T, interest: Interest) -> io::Result<Self> {
        let _ = interest;
        Ok(Self {
            cache: AtomicU32::new(0),
            read: Mutex::new(None),
            write: Mutex::new(None),
            inner,
        })
    }

    /// The descriptor's owner.
    #[inline]
    pub fn get_ref(&self) -> &T {
        &self.inner
    }

    /// The descriptor's owner, to change.
    #[inline]
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    /// The descriptor's owner, back, with the descriptor open; a poll the
    /// wrapper kept is let go of, and its ring cancels it.
    pub fn into_inner(self) -> T {
        self.inner
    }

    /// Polls for the descriptor to be readable, with the poll kept in the
    /// wrapper from one call to the next; see the [type](AsyncFd).
    ///
    /// # Errors
    ///
    /// What the kernel reports for the poll, such as `EBADF`.
    pub fn poll_read_ready<'a>(
        &'a self,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<AsyncFdReadyGuard<'a, T>>> {
        let interest = Interest::READABLE;
        let fd = self.inner.as_raw_fd();
        let wait = || lock(&self.read);
        let events = ready!(poll_events(&self.cache, fd, cx, interest, wait))?;
        Poll::Ready(Ok(AsyncFdReadyGuard {
            async_fd: self,
            events,
            interest,
        }))
    }

    /// Polls for the descriptor to be readable, as
    /// [`poll_read_ready`](AsyncFd::poll_read_ready) does, with a guard that
    /// gives the owner mutably.
    ///
    /// # Errors
    ///
    /// As for [`poll_read_ready`](AsyncFd::poll_read_ready).
    pub fn poll_read_ready_mut<'a>(
        &'a mut self,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<AsyncFdReadyMutGuard<'a, T>>> {
        let interest = Interest::READABLE;
        let fd = self.inner.as_raw_fd();
        let wait = self.read.get_mut().unwrap_or_else(PoisonError::into_inner);
        let events = ready!(poll_events(&self.cache, fd, cx, interest, || wait))?;
        Poll::Ready(Ok(AsyncFdReadyMutGuard {
            async_fd: self,
            events,
            interest,
        }))
    }

    /// Polls for the descriptor to be writable, with the poll kept in the
    /// wrapper from one call to the next, beside the read one; see the
    /// [type](AsyncFd).
    ///
    /// # Errors
    ///
    /// What the kernel reports for the poll, such as `EBADF`.
    pub fn poll_write_ready<'a>(
        &'a self,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<AsyncFdReadyGuard<'a, T>>> {
        let interest = Interest::WRITABLE;
        let fd = self.inner.as_raw_fd();
        let wait = || lock(&self.write);
        let events = ready!(poll_events(&self.cache, fd, cx, interest, wait))?;
        Poll::Ready(Ok(AsyncFdReadyGuard {
            async_fd: self,
            events,
            interest,
        }))
    }

    /// Polls for the descriptor to be writable, as
    /// [`poll_write_ready`](AsyncFd::poll_write_ready) does, with a guard
    /// that gives the owner mutably.
    ///
    /// # Errors
    ///
    /// As for [`poll_write_ready`](AsyncFd::poll_write_ready).
    pub fn poll_write_ready_mut<'a>(
        &'a mut self,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<AsyncFdReadyMutGuard<'a, T>>> {
        let interest = Interest::WRITABLE;
        let fd = self.inner.as_raw_fd();
        let wait = self.write.get_mut().unwrap_or_else(PoisonError::into_inner);
        let events = ready!(poll_events(&self.cache, fd, cx, interest, || wait))?;
        Poll::Ready(Ok(AsyncFdReadyMutGuard {
            async_fd: self,
            events,
            interest,
        }))
    }

    /// Waits for the descriptor to have any of `interest`, and hands back a
    /// guard of the readiness it has.
    ///
    /// Readiness seen before and not cleared answers at once; otherwise the
    /// wait polls the descriptor once, with a poll of the future's own. A
    /// poll reports an error or a hang-up whatever it asks for, so the guard
    /// can hold readiness the interest did not ask for, an error, or none at
    /// all: an interest of [`ERROR`](Interest::ERROR) only, answered by a
    /// hang-up, is ready with nothing. The future is cancel-safe: dropped,
    /// it lets go of its poll, and readiness it saw stays for the next wait.
    ///
    /// # Errors
    ///
    /// What the kernel reports for the poll, such as `EBADF`.
    pub async fn ready(&self, interest: Interest) -> io::Result<AsyncFdReadyGuard<'_, T>> {
        let events = self.wait(interest).await?;
        Ok(AsyncFdReadyGuard {
            async_fd: self,
            events,
            interest,
        })
    }

    /// Waits for the descriptor to have any of `interest`, as
    /// [`ready`](AsyncFd::ready) does, with a guard that gives the owner
    /// mutably.
    ///
    /// # Errors
    ///
    /// As for [`ready`](AsyncFd::ready).
    pub async fn ready_mut(
        &mut self,
        interest: Interest,
    ) -> io::Result<AsyncFdReadyMutGuard<'_, T>> {
        let events = self.wait(interest).await?;
        Ok(AsyncFdReadyMutGuard {
            async_fd: self,
            events,
            interest,
        })
    }

    /// Waits for the descriptor to be readable; see [`ready`](AsyncFd::ready).
    ///
    /// # Errors
    ///
    /// As for [`ready`](AsyncFd::ready).
    pub async fn readable(&self) -> io::Result<AsyncFdReadyGuard<'_, T>> {
        self.ready(Interest::READABLE).await
    }

    /// Waits for the descriptor to be readable; see
    /// [`ready_mut`](AsyncFd::ready_mut).
    ///
    /// # Errors
    ///
    /// As for [`ready`](AsyncFd::ready).
    pub async fn readable_mut(&mut self) -> io::Result<AsyncFdReadyMutGuard<'_, T>> {
        self.ready_mut(Interest::READABLE).await
    }

    /// Waits for the descriptor to be writable; see [`ready`](AsyncFd::ready).
    ///
    /// # Errors
    ///
    /// As for [`ready`](AsyncFd::ready).
    pub async fn writable(&self) -> io::Result<AsyncFdReadyGuard<'_, T>> {
        self.ready(Interest::WRITABLE).await
    }

    /// Waits for the descriptor to be writable; see
    /// [`ready_mut`](AsyncFd::ready_mut).
    ///
    /// # Errors
    ///
    /// As for [`ready`](AsyncFd::ready).
    pub async fn writable_mut(&mut self) -> io::Result<AsyncFdReadyMutGuard<'_, T>> {
        self.ready_mut(Interest::WRITABLE).await
    }

    /// Calls `f` with the owner each time the descriptor is ready for
    /// `interest`, until it does not fail with
    /// [`WouldBlock`](io::ErrorKind::WouldBlock), and gives what it
    /// returns: a wait and a [`try_io`](AsyncFdReadyGuard::try_io) after
    /// another.
    ///
    /// # Errors
    ///
    /// What the wait reports, as for [`ready`](AsyncFd::ready), or what `f`
    /// does.
    pub async fn async_io<R>(
        &self,
        interest: Interest,
        mut f: impl FnMut(&T) -> io::Result<R>,
    ) -> io::Result<R> {
        loop {
            let mut guard = self.ready(interest).await?;
            if let Ok(result) = guard.try_io(|fd| f(fd.get_ref())) {
                return result;
            }
        }
    }

    /// Calls `f` with the owner, mutably, as [`async_io`](AsyncFd::async_io)
    /// does.
    ///
    /// # Errors
    ///
    /// As for [`async_io`](AsyncFd::async_io).
    pub async fn async_io_mut<R>(
        &mut self,
        interest: Interest,
        mut f: impl FnMut(&mut T) -> io::Result<R>,
    ) -> io::Result<R> {
        loop {
            let mut guard = self.ready_mut(interest).await?;
            if let Ok(result) = guard.try_io(|fd| f(fd.get_mut())) {
                return result;
            }
        }
    }

    /// The events that answer a wait for `interest`, polled for with a poll
    /// of the future's own.
    #[inline]
    async fn wait(&self, interest: Interest) -> io::Result<u32> {
        let fd = self.inner.as_raw_fd();
        let mut wait = None;
        poll_fn(|cx| poll_events(&self.cache, fd, cx, interest, || &mut wait)).await
    }
}

impl<T: AsRawFd> AsRawFd for AsyncFd<T> {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

impl<T: AsRawFd + AsFd> AsFd for AsyncFd<T> {
    #[inline]
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.inner.as_fd()
    }
}

impl<T: AsRawFd + fmt::Debug> fmt::Debug for AsyncFd<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AsyncFd")
            .field("inner", &self.inner)
            .field(
                "events",
                &format_args!("{:#x}", self.cache.load(Ordering::Relaxed)),
            )
            .finish_non_exhaustive()
    }
}

/// The readiness of an [`AsyncFd`] that a wait found, with the wrapper:
/// made by [`readable`](AsyncFd::readable) and its kin.
///
/// The owner makes its call with [`try_io`](AsyncFdReadyGuard::try_io),
/// which clears the readiness if the call would have blocked, or makes it
/// itself and clears it with [`clear_ready`](AsyncFdReadyGuard::clear_ready)
/// when it finds nothing to take or no room. Readiness not cleared stays,
/// whether the guard is dropped or [`retain_ready`](AsyncFdReadyGuard::retain_ready)
/// is called, and answers the next wait at once.
#[must_use = "a guard's readiness is cleared, or retained, by a method of it"]
pub struct AsyncFdReadyGuard<'a, T: AsRawFd> {
    async_fd: &'a AsyncFd<T>,
    /// The poll events the wait found, not cleared through the guard.
    events: u32,
    interest: Interest,
}

impl<'a, T: AsRawFd> AsyncFdReadyGuard<'a, T> {
    /// Clears the guard's readiness, so that the next wait for it polls the
    /// descriptor again. Call it only once a call has found the descriptor
    /// not ready, as one that fails with
    /// [`WouldBlock`](io::ErrorKind::WouldBlock) does.
    ///
    /// The poll is level-triggered, so readiness that came again in the
    /// meantime is not lost: the next poll finds it at once.
    pub fn clear_ready(&mut self) {
        // Relaxed: as in `poll_events`.
        self.async_fd
            .cache
            .fetch_and(!self.events, Ordering::Relaxed);
        self.events = 0;
    }

    /// Clears the kinds of the guard's readiness that `ready` holds, as
    /// [`clear_ready`](AsyncFdReadyGuard::clear_ready) does all of them: for
    /// a wait for several, those a call found not there.
    pub fn clear_ready_matching(&mut self, ready: Ready) {
        let cleared = self.events & ready.events();
        // Relaxed: as in `poll_events`.
        self.async_fd.cache.fetch_and(!cleared, Ordering::Relaxed);
        self.events &= !cleared;
    }

    /// Keeps the guard's readiness for the next wait, as dropping the guard
    /// does.
    pub fn retain_ready(&mut self) {}

    /// The readiness the guard holds, empty once cleared.
    pub fn ready(&self) -> Ready {
        Ready::from_events(self.events, self.interest)
    }

    /// Calls `f` with the wrapper, and clears the readiness if it fails with
    /// [`WouldBlock`](io::ErrorKind::WouldBlock), which it gives as a
    /// [`TryIoError`]; anything else it returns is the output.
    ///
    /// # Errors
    ///
    /// [`TryIoError`] if `f` would have blocked.
    pub fn try_io<R>(
        &mut self,
        f: impl FnOnce(&'a AsyncFd<T>) -> io::Result<R>,
    ) -> Result<io::Result<R>, TryIoError> {
        match f(self.async_fd) {
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                self.clear_ready();
                Err(TryIoError(()))
            }
            result => Ok(result),
        }
    }

    /// The wrapper.
    pub fn get_ref(&self) -> &'a AsyncFd<T> {
        self.async_fd
    }

    /// The descriptor's owner.
    pub fn get_inner(&self) -> &'a T {
        self.async_fd.get_ref()
    }
}

impl<T: AsRawFd + fmt::Debug> fmt::Debug for AsyncFdReadyGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AsyncFdReadyGuard")
            .field("async_fd", self.async_fd)
            .field("ready", &self.ready())
            .finish()
    }
}

/// The readiness of an [`AsyncFd`] that a wait found, with the wrapper
/// mutably: made by [`readable_mut`](AsyncFd::readable_mut) and its kin.
/// It is what an [`AsyncFdReadyGuard`] is, but for the owner it gives.
#[must_use = "a guard's readiness is cleared, or retained, by a method of it"]
pub struct AsyncFdReadyMutGuard<'a, T: AsRawFd> {
    async_fd: &'a mut AsyncFd<T>,
    /// The poll events the wait found, not cleared through the guard.
    events: u32,
    interest: Interest,
}

impl<T: AsRawFd> AsyncFdReadyMutGuard<'_, T> {
    /// Clears the guard's readiness; see
    /// [`AsyncFdReadyGuard::clear_ready`].
    pub fn clear_ready(&mut self) {
        *self.async_fd.cache.get_mut() &= !self.events;
        self.events = 0;
    }

    /// Clears the kinds of the guard's readiness that `ready` holds; see
    /// [`AsyncFdReadyGuard::clear_ready_matching`].
    pub fn clear_ready_matching(&mut self, ready: Ready) {
        let cleared = self.events & ready.events();
        *self.async_fd.cache.get_mut() &= !cleared;
        self.events &= !cleared;
    }

    /// Keeps the guard's readiness for the next wait, as dropping the guard
    /// does.
    pub fn retain_ready(&mut self) {}

    /// The readiness the guard holds, empty once cleared.
    pub fn ready(&self) -> Ready {
        Ready::from_events(self.events, self.interest)
    }

    /// Calls `f` with the wrapper, and clears the readiness if it fails with
    /// [`WouldBlock`](io::ErrorKind::WouldBlock); see
    /// [`AsyncFdReadyGuard::try_io`].
    ///
    /// # Errors
    ///
    /// [`TryIoError`] if `f` would have blocked.
    pub fn try_io<R>(
        &mut self,
        f: impl FnOnce(&mut AsyncFd<T>) -> io::Result<R>,
    ) -> Result<io::Result<R>, TryIoError> {
        match f(self.async_fd) {
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                self.clear_ready();
                Err(TryIoError(()))
            }
            result => Ok(result),
        }
    }

    /// The wrapper.
    pub fn get_ref(&self) -> &AsyncFd<T> {
        self.async_fd
    }

    /// The wrapper, mutably.
    pub fn get_mut(&mut self) -> &mut AsyncFd<T> {
        self.async_fd
    }

    /// The descriptor's owner.
    pub fn get_inner(&self) -> &T {
        self.async_fd.get_ref()
    }

    /// The descriptor's owner, mutably.
    pub fn get_inner_mut(&mut self) -> &mut T {
        self.async_fd.get_mut()
    }
}

impl<T: AsRawFd + fmt::Debug> fmt::Debug for AsyncFdReadyMutGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AsyncFdReadyMutGuard")
            .field("async_fd", self.async_fd)
            .field("ready", &self.ready())
            .finish()
    }
}
