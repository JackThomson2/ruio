//! Sockets read and written with calls made at once, which wait on the
//! port only when there is nothing to take or no room: the portable
//! sockets.
//!
//! A read or a write is a call of Winsock's on a nonblocking socket, made
//! on whichever thread polls it, which takes what has arrived, or gives
//! what there is room for, with no turn of the port's loop. Only one that
//! finds the socket empty, or full, waits, with a poll of the AFD driver's
//! that completes to the port of the thread that polls it
//! ([`Socket::poll_ready`]), and is made again once the socket is ready.
//! Through the port instead, each read and write would be an operation of
//! its own, whose completion takes a turn of the loop. A wait whose poll is
//! still in flight, polled again, polls its poll before anything else: the
//! task was woken for something else, and the call would find what the
//! poll says it would.
//!
//! A read that takes less than its buffer has drained the socket, and a
//! write that gives less than it was given has filled it, so the next
//! transfer that way would only find nothing, or no room: it starts its
//! poll first instead, without the call, as tokio clears the readiness it
//! has after such a transfer. A request whose answer has not come yet, the
//! common case of a client, so costs one read, not two. The driver checks
//! the events as the poll starts, so a socket that became ready meanwhile
//! completes the wait at the port's next turn: a hint gone stale costs that
//! turn, never the readiness.
//!
//! A socket that is ready again and again, as one a peer keeps full, is
//! read with no wait at all, so each read and write spends a step of the
//! task's budget ([`coop`]), and once the budget is spent the task yields.
//!
//! Polled where no runtime runs, by another executor's task say, a read or
//! a write is the same call. A wait goes to the foreign reactor
//! ([`set_foreign_reactor`](super::set_foreign_reactor)) where the thread
//! is one of its: the socket is registered with it as it first waits
//! there, and a call that finds the socket not ready is made again under
//! the reactor's readiness, which then says when it is worth making, with
//! no other thread in between, as that executor's own sockets wait. A call
//! that finds what it came for costs the reactor nothing: the next one this
//! way goes through the reactor first only if this one had to wait, or was
//! made as its task woke from a wait, or moved less than it was to, as a
//! request whose answer has yet to come does. Elsewhere a wait, which has
//! no port to run on, fails with [`io::ErrorKind::Unsupported`].

use std::fmt;
use std::future::{Future, poll_fn};
use std::io::{self, IoSlice};
use std::mem::MaybeUninit;
use std::os::windows::io::{AsRawSocket, AsSocket, OwnedSocket};
use std::pin::Pin;
use std::slice;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, ready};

use ruio_iocp::portable::{ReadyWait, Socket, events};

use super::foreign::{Foreign, interest, level};
use crate::buf::{BufResult, IoBuf, IoBufMut, IoBufs};
use crate::coop;

/// The events a read, and an accept, wait for.
pub(crate) const READABLE: u32 = events::READABLE;

/// The events a write, and a connect, wait for.
pub(crate) const WRITABLE: u32 = events::WRITABLE;

/// The most buffers of an owned vector one write of
/// [`PortableWriteAllVectored`] takes, from the stack: a vector of more is
/// written with as many writes as it takes.
const OWNED_IOVECS: usize = 64;

/// The most bytes one buffer of a vectored write names, as a `WSABUF`
/// holds them: a longer one is written in part.
const MAX_SLICE: usize = u32::MAX as usize;

/// A socket made nonblocking, read and written with calls made at once,
/// which waits with polls on the port of the thread that polls each wait,
/// or on the foreign reactor where that thread is one of its.
pub(crate) struct Nonblocking {
    /// The socket's registration with the foreign reactor, if it has waited
    /// on one of its threads: first, so that it leaves the reactor before
    /// the socket closes.
    foreign: Foreign,
    socket: Socket,
    /// What the last reads found.
    read: Hint,
    /// What the last writes found.
    write: Hint,
}

/// What the last transfers one way found, which says whether the next makes
/// its call at once, or waits for the socket to be ready first.
#[derive(Debug, Default)]
struct Hint {
    /// Whether the next transfer this way waits first: the last moved less
    /// than it was to, and so drained the socket or filled it; or, on the
    /// foreign reactor, it had to wait, or was made as the task woke from
    /// such a wait, so that the next is likely to wait too.
    first: AtomicBool,
    /// Whether the last transfer this way waited on the foreign reactor, so
    /// that the one made as the task wakes keeps `first` set.
    waited: AtomicBool,
}

impl Nonblocking {
    /// Takes over `socket`, nonblocking already, as a [`Socket`] is.
    pub(crate) fn new(socket: Socket) -> Self {
        Self {
            foreign: Foreign::new(),
            socket,
            read: Hint::default(),
            write: Hint::default(),
        }
    }

    /// The socket.
    #[inline]
    pub(crate) fn socket(&self) -> &Socket {
        &self.socket
    }

    /// The socket, blocking again, as the standard library's sockets are.
    pub(crate) fn into_owned_socket(self) -> OwnedSocket {
        let Self {
            foreign, socket, ..
        } = self;
        // Out of the reactor while the socket is still this one's.
        drop(foreign);
        socket.into_socket()
    }

    /// Reads into `buf` with one call, made at once: what has arrived, 0 at
    /// the end, or `WouldBlock` if nothing has.
    #[inline]
    pub(crate) fn try_read(&self, buf: &mut [MaybeUninit<u8>]) -> io::Result<usize> {
        self.socket.try_read(buf)
    }

    /// Writes `buf` with one call, made at once: as much as there is room
    /// for, or `WouldBlock` if there is none.
    #[inline]
    pub(crate) fn try_write(&self, buf: &[u8]) -> io::Result<usize> {
        self.socket.try_write(buf)
    }

    /// Writes `bufs`, one after the other, with one call, made at once: as
    /// much as there is room for, or `WouldBlock` if there is none.
    #[inline]
    pub(crate) fn try_write_vectored(&self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        self.socket.try_write_vectored(bufs)
    }

    /// Reads into `buf` what has arrived, 0 at the end, waiting with `wait`
    /// for something to arrive if nothing has; see the module's
    /// documentation.
    pub(crate) fn poll_read(
        &self,
        cx: &mut Context<'_>,
        buf: &mut [MaybeUninit<u8>],
        wait: &mut ReadyWait,
    ) -> Poll<io::Result<usize>> {
        let len = buf.len();
        self.poll_transfer(cx, READABLE, wait, len, |io| io.try_read(buf))
    }

    /// Writes as much of `buf` as there is room for, waiting with `wait` for
    /// room if there is none.
    pub(crate) fn poll_write(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        wait: &mut ReadyWait,
    ) -> Poll<io::Result<usize>> {
        self.poll_transfer(cx, WRITABLE, wait, buf.len(), |io| io.try_write(buf))
    }

    /// Writes as much of `bufs`, one after the other, as there is room for,
    /// waiting with `wait` for room if there is none.
    pub(crate) fn poll_write_vectored(
        &self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        wait: &mut ReadyWait,
    ) -> Poll<io::Result<usize>> {
        let len = bufs.iter().map(|buf| buf.len()).sum();
        self.poll_transfer(cx, WRITABLE, wait, len, |io| io.try_write_vectored(bufs))
    }

    /// Waits with `wait` for the socket to have any of `events`, as
    /// [`Socket::poll_ready`] does; or, where the socket waits on the
    /// foreign reactor, until the reactor says it may, and one `WSAPoll`
    /// that does not wait finds it does.
    #[inline]
    pub(crate) fn poll_ready(
        &self,
        cx: &mut Context<'_>,
        events: u32,
        wait: &mut ReadyWait,
    ) -> Poll<io::Result<u32>> {
        if !wait.is_waiting()
            && let Some(registered) = self.foreign.here(self.socket.as_socket())
            && let Some(polled) =
                registered.poll_io(cx, interest(events), 0, &mut || level(&self.socket, events))
        {
            // The AFD bits, which fit a `u32`.
            return polled.map_ok(|events| events as u32);
        }
        self.socket.poll_ready(cx, events, wait)
    }

    /// Waits for the socket to have any of `events`, as
    /// [`poll_ready`](Nonblocking::poll_ready) does, with a wait of its own.
    pub(crate) async fn ready(&self, events: u32) -> io::Result<u32> {
        let mut wait = ReadyWait::new();
        poll_fn(|cx| self.poll_ready(cx, events, &mut wait)).await
    }

    /// Runs `transfer`, a call made at once that is to move `len` bytes,
    /// once the task's budget allows, waiting with `wait` for `events` while
    /// it finds the socket not ready; after a transfer this way that moved
    /// less than it was to, it waits first. See the module's documentation.
    #[inline]
    fn poll_transfer(
        &self,
        cx: &mut Context<'_>,
        events: u32,
        wait: &mut ReadyWait,
        len: usize,
        transfer: impl FnMut(&Self) -> io::Result<usize>,
    ) -> Poll<io::Result<usize>> {
        let hint = if events == READABLE {
            &self.read
        } else {
            &self.write
        };
        self.poll_call(cx, events, wait, Some(hint), len, transfer, |done| *done)
    }

    /// Runs `call`, a call made at once that takes something from the
    /// socket, as accepting a connection does, once the task's budget
    /// allows, waiting with `wait` for `events` while it finds the socket not
    /// ready.
    #[inline]
    pub(crate) fn poll_take<T>(
        &self,
        cx: &mut Context<'_>,
        events: u32,
        wait: &mut ReadyWait,
        call: impl FnMut(&Self) -> io::Result<T>,
    ) -> Poll<io::Result<T>> {
        self.poll_call(cx, events, wait, None, 0, call, |_| 0)
    }

    /// Runs `call`, a call made at once that is to move `len` bytes, and
    /// that moved as many as `moved` says of its output, waiting with `wait`
    /// for `events` while it finds the socket not ready; where `hint` says
    /// the socket is likely not ready, it waits first. On a thread of the
    /// foreign reactor's, a call that finds the socket not ready, or one that
    /// would wait first, is made under the reactor's readiness instead, and
    /// waits on the reactor. Each call that succeeds spends a step of the
    /// task's budget.
    #[inline]
    #[allow(clippy::too_many_arguments)]
    fn poll_call<T>(
        &self,
        cx: &mut Context<'_>,
        events: u32,
        wait: &mut ReadyWait,
        hint: Option<&Hint>,
        len: usize,
        mut call: impl FnMut(&Self) -> io::Result<T>,
        moved: impl Fn(&T) -> usize,
    ) -> Poll<io::Result<T>> {
        let proceed = ready!(coop::poll_proceed(cx));
        // Relaxed: a hint, which says only whether the call or the wait
        // comes first.
        if !wait.is_waiting() && hint.is_some_and(|hint| hint.first.swap(false, Ordering::Relaxed))
        {
            if let Some(polled) = self.poll_registered(cx, events, hint, len, &mut call, &moved) {
                return progressed(polled, proceed);
            }
            ready!(self.socket.poll_ready(cx, events, wait))?;
        }
        loop {
            if wait.is_waiting() {
                // Woken for something else, or by the poll: the poll knows
                // which, and costs no call to ask.
                ready!(self.socket.poll_ready(cx, events, wait))?;
            }
            match call(self) {
                Ok(made) => {
                    proceed.made_progress();
                    if let Some(hint) = hint
                        && short(moved(&made), len)
                    {
                        hint.first.store(true, Ordering::Relaxed);
                    }
                    return Poll::Ready(Ok(made));
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    if let Some(polled) =
                        self.poll_registered(cx, events, hint, len, &mut call, &moved)
                    {
                        return progressed(polled, proceed);
                    }
                    ready!(self.socket.poll_ready(cx, events, wait))?;
                }
                Err(err) => return Poll::Ready(Err(err)),
            }
        }
    }

    /// Makes `call` once the foreign reactor says the socket may be ready
    /// for `events`, as [`Registration::poll_io`](super::Registration::poll_io)
    /// does, on a thread of the reactor's, where no runtime of ruio's runs
    /// and the socket is registered with the reactor, or can be; `None`
    /// elsewhere, for the wait to go to a port. A call that has to wait
    /// leaves `hint` so that the next this way waits first, as does the call
    /// made as the task wakes from that wait, and one that moved less than
    /// it was to, whose readiness the reactor has cleared.
    fn poll_registered<T>(
        &self,
        cx: &mut Context<'_>,
        events: u32,
        hint: Option<&Hint>,
        len: usize,
        call: &mut impl FnMut(&Self) -> io::Result<T>,
        moved: &impl Fn(&T) -> usize,
    ) -> Option<Poll<io::Result<T>>> {
        let registered = self.foreign.here(self.socket.as_socket())?;
        let mut made = None;
        let mut through = || {
            let output = call(self)?;
            let done = moved(&output);
            made = Some(output);
            Ok(done)
        };
        let polled = registered.poll_io(cx, interest(events), len, &mut through)?;
        Some(match polled {
            Poll::Ready(Ok(done)) => {
                if let Some(hint) = hint {
                    let woke = hint.waited.swap(false, Ordering::Relaxed);
                    if woke || short(done, len) {
                        hint.first.store(true, Ordering::Relaxed);
                    }
                }
                Poll::Ready(Ok(made.expect("a call that succeeded made its output")))
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Pending => {
                if let Some(hint) = hint {
                    hint.first.store(true, Ordering::Relaxed);
                    hint.waited.store(true, Ordering::Relaxed);
                }
                Poll::Pending
            }
        })
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
            .field("socket", &self.socket.as_raw_socket())
            .field("foreign", &self.foreign)
            .finish()
    }
}

/// Fills `slices` with the initialized bytes of `bufs` past the first
/// `skip`, of up to [`OWNED_IOVECS`] buffers, one slice each but for a
/// buffer longer than a slice may be, whose start is the last: how many
/// slices, and how many bytes they name.
fn gather<'a, V: IoBufs>(
    bufs: &'a V,
    skip: usize,
    slices: &mut [IoSlice<'a>; OWNED_IOVECS],
) -> (usize, usize) {
    let mut skip = skip;
    let mut count = 0;
    let mut total = 0;
    for index in 0..bufs.buf_count() {
        if count == OWNED_IOVECS {
            break;
        }
        let (start, len) = bufs.buf(index);
        if skip >= len {
            skip -= len;
            continue;
        }
        let left = len - skip;
        let taken = left.min(MAX_SLICE);
        // SAFETY: `skip` is less than `len`, so the pointer stays in the
        // buffer's initialized bytes.
        let from = unsafe { start.add(skip) };
        // SAFETY: the `taken` bytes from `from` are initialized bytes of the
        // buffer, which stay in place, unchanged, while `bufs` is borrowed,
        // and the slice borrows them for no longer.
        let bytes = unsafe { slice::from_raw_parts(from, taken) };
        slices[count] = IoSlice::new(bytes);
        skip = 0;
        count += 1;
        total += taken;
        if taken < left {
            break;
        }
    }
    (count, total)
}

/// Whether a call that was to move `len` bytes and moved `done` stopped
/// short of them, having drained the socket, or filled it.
#[inline]
fn short(done: usize, len: usize) -> bool {
    0 < done && done < len
}

/// `polled`, having spent `proceed` if it made something.
#[inline]
fn progressed<T>(polled: Poll<io::Result<T>>, proceed: coop::Proceed) -> Poll<io::Result<T>> {
    if let Poll::Ready(Ok(_)) = polled {
        proceed.made_progress();
    }
    polled
}

/// A read of a socket with a call made at once, which waits on the port of
/// the thread that polls it only if nothing has arrived: made by the `read`
/// of the portable sockets. The output is how many bytes were read, 0 at
/// the end, and the buffer, of which that many are then initialized from
/// its start.
///
/// It spends a step of the task's budget ([`coop`]) when it reads, so a
/// socket that always has something to read cannot hold up the thread.
/// Dropped while it waits, it lets go of its poll; nothing is read but what
/// it returns, so nothing is lost. Its wait runs on the foreign reactor
/// where the thread is one of its; where no runtime runs and the reactor
/// does not take it, a read that finds nothing fails with
/// [`io::ErrorKind::Unsupported`], as its wait has no port to run on.
///
/// # Panics
///
/// If polled after it completed.
#[must_use = "a read does nothing until polled"]
pub struct PortableRead<'a, B> {
    io: &'a Nonblocking,
    /// Taken as the read completes.
    buf: Option<B>,
    /// The wait for the socket to be readable, once it was found empty.
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
                // SAFETY: the receive initialized the first `read` bytes.
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

/// A write to a socket with a call made at once, which waits on the port of
/// the thread that polls it only if there is no room: made by the `write`
/// of the portable sockets. The output is how many bytes were written, and
/// the buffer.
///
/// It spends a step of the task's budget ([`coop`]) when it writes. Dropped
/// while it waits, it lets go of its poll, having written nothing. Its wait
/// runs as [`PortableRead`]'s does.
///
/// # Panics
///
/// If polled after it completed.
#[must_use = "a write does nothing until polled"]
pub struct PortableWrite<'a, B> {
    io: &'a Nonblocking,
    /// Taken as the write completes.
    buf: Option<B>,
    /// The wait for the socket to be writable, once it was found full.
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

/// A read that fills a whole buffer from a socket, with calls made at
/// once, as many as it takes, which wait on the port of the thread that
/// polls them only while nothing has arrived: made by the `read_exact` of
/// the portable sockets.
///
/// The read fills the buffer from its start, up to its
/// [`bytes_total`](IoBuf::bytes_total). The output is the buffer, and
/// `Ok(())` once it is full, or the error that stopped the read:
/// [`io::ErrorKind::UnexpectedEof`] if the socket ended first. What arrived
/// is marked initialized either way, as each read arrives.
///
/// Its fields hold no type of a trait's, so a future that must be `Send`
/// can hold it for any buffer that is `Send` and `Sync`, a borrowed one
/// among them; the same goes for [`PortableWriteAll`]. Its waits run as
/// [`PortableRead`]'s do.
///
/// # Panics
///
/// If polled after it completed.
#[must_use = "a read does nothing until polled"]
pub struct PortableReadExact<'a, B> {
    io: &'a Nonblocking,
    /// Taken as the read completes.
    buf: Option<B>,
    /// How many bytes have been read.
    done: usize,
    /// The wait for the socket to be readable, once it was found empty.
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
            let len = into.len();
            let read = ready!(
                this.io
                    .poll_transfer(cx, READABLE, &mut this.wait, len, |io| io.try_read(into))
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

/// A write of all of a buffer's initialized bytes to a socket, with calls
/// made at once, as many as it takes, which wait on the port of the thread
/// that polls them only while there is no room: made by the `write_all` of
/// the portable sockets. The output is the buffer, with how the writes
/// went: a write that writes nothing fails with `WriteZero`. Its waits run
/// as [`PortableRead`]'s do.
///
/// # Panics
///
/// If polled after it completed.
#[must_use = "a write does nothing until polled"]
pub struct PortableWriteAll<'a, B> {
    io: &'a Nonblocking,
    /// Taken as the write completes.
    buf: Option<B>,
    /// How many bytes have been written.
    done: usize,
    /// The wait for the socket to be writable, once it was found full.
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
            let written =
                ready!(
                    this.io
                        .poll_transfer(cx, WRITABLE, &mut this.wait, from.len(), |io| io
                            .try_write(from))
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
/// other, to a socket with calls made at once, as many as it takes, each of
/// up to 64 of the buffers, which wait on the port of the thread that polls
/// them only while there is no room: made by the `write_all_vectored` of
/// the portable sockets. The output is the buffers, with how the writes
/// went: a write that writes nothing fails with `WriteZero`.
///
/// It spends a step of the task's budget ([`coop`]) with each write that
/// moves data. Dropped part way, it has written what it has, and no more.
/// Its waits run as [`PortableRead`]'s do.
///
/// # Panics
///
/// If polled after it completed.
#[must_use = "a write does nothing until polled"]
pub struct PortableWriteAllVectored<'a, V> {
    io: &'a Nonblocking,
    /// Taken as the write completes.
    bufs: Option<V>,
    /// How many bytes have been written.
    done: usize,
    /// The wait for the socket to be writable, once it was found full.
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
        let mut failed = None;
        while this.done < total {
            let mut slices = [IoSlice::new(&[]); OWNED_IOVECS];
            let (count, len) = gather(bufs, this.done, &mut slices);
            let slices = &slices[..count];
            let written = ready!(
                this.io
                    .poll_transfer(cx, WRITABLE, &mut this.wait, len, |io| {
                        io.try_write_vectored(slices)
                    })
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
        let bufs = this
            .bufs
            .take()
            .expect("taken once, as the write completes");
        Poll::Ready((failed.map_or(Ok(()), Err), bufs))
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
    use std::net;
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

    /// A connected pair over loopback: a nonblocking socket, and the
    /// standard library's peer.
    fn pair() -> (Nonblocking, net::TcpStream) {
        let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
        let peer = net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (ours, _) = listener.accept().unwrap();
        (Nonblocking::new(Socket::new(ours.into()).unwrap()), peer)
    }

    /// A read waits for something to arrive, reads it, and reads 0 at the
    /// end; the futures are `Send` and `Sync`.
    #[test]
    fn a_read_waits_for_data_and_reads_what_has_arrived() {
        run(async {
            let (reader, mut writer) = pair();
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

    /// A read with no wait in flight makes its call at once; one whose poll
    /// is still in flight asks the poll first, and so finds nothing arrived
    /// without a call while the poll says nothing has.
    #[test]
    fn a_wait_in_flight_is_asked_before_the_call() {
        run(async {
            let (reader, mut writer) = pair();
            let mut wait = ReadyWait::new();
            let mut buf = [MaybeUninit::uninit(); 8];
            let polled =
                poll_fn(|cx| Poll::Ready(reader.poll_read(cx, &mut buf, &mut wait).is_pending()))
                    .await;
            assert!(polled, "nothing has arrived");
            assert!(wait.is_waiting());
            writer.write_all(b"x").unwrap();
            let read = poll_fn(|cx| reader.poll_read(cx, &mut buf, &mut wait)).await;
            assert_eq!(read.unwrap(), 1);
            assert!(!wait.is_waiting());
        });
    }

    /// Whether a read polled once completes on that poll, and with what.
    async fn read_at_once(
        reader: &Nonblocking,
        buf: &mut [MaybeUninit<u8>],
        wait: &mut ReadyWait,
    ) -> Option<usize> {
        poll_fn(|cx| {
            Poll::Ready(match reader.poll_read(cx, buf, wait) {
                Poll::Ready(read) => Some(read.unwrap()),
                Poll::Pending => None,
            })
        })
        .await
    }

    /// Waits until `reader` has data, without reading it.
    async fn arrived(reader: &Nonblocking) {
        reader.ready(READABLE).await.unwrap();
    }

    /// A read that took less than its buffer drained the socket, so the
    /// next waits for the port before it reads, even with data waiting, and
    /// then reads it.
    #[test]
    fn a_read_after_a_short_one_waits_for_the_port_first() {
        run(async {
            let (reader, mut writer) = pair();
            let mut wait = ReadyWait::new();
            let mut buf = [MaybeUninit::uninit(); 16];
            writer.write_all(b"one").unwrap();
            arrived(&reader).await;
            assert_eq!(read_at_once(&reader, &mut buf, &mut wait).await, Some(3));
            writer.write_all(b"two").unwrap();
            arrived(&reader).await;
            assert_eq!(read_at_once(&reader, &mut buf, &mut wait).await, None);
            assert!(wait.is_waiting(), "the poll is started, with no read made");
            let read = poll_fn(|cx| reader.poll_read(cx, &mut buf, &mut wait)).await;
            assert_eq!(read.unwrap(), 3);
        });
    }

    /// A read that filled its buffer may have left more, so the next reads
    /// at once.
    #[test]
    fn a_read_after_a_full_one_reads_at_once() {
        run(async {
            let (reader, mut writer) = pair();
            let mut wait = ReadyWait::new();
            let mut buf = [MaybeUninit::uninit(); 4];
            writer.write_all(b"12345678").unwrap();
            arrived(&reader).await;
            assert_eq!(read_at_once(&reader, &mut buf, &mut wait).await, Some(4));
            assert_eq!(read_at_once(&reader, &mut buf, &mut wait).await, Some(4));
            assert!(!wait.is_waiting());
            // Short at last, by the end of the data.
            writer.write_all(b"9").unwrap();
            arrived(&reader).await;
            assert_eq!(read_at_once(&reader, &mut buf, &mut wait).await, Some(1));
            assert_eq!(read_at_once(&reader, &mut buf, &mut wait).await, None);
        });
    }

    /// Writes that fill the socket wait for room, and eight megabytes go
    /// through whole as the peer drains them slowly.
    #[test]
    fn writes_that_fill_the_socket_wait_for_room_and_go_through() {
        run(async {
            let (ours, mut theirs) = pair();
            let data: Vec<u8> = (0..8u32 << 20).map(|index| (index % 253) as u8).collect();
            let expected = data.clone();
            let draining = std::thread::spawn(move || {
                let mut got = Vec::with_capacity(expected.len());
                let mut chunk = vec![0; 64 << 10];
                while got.len() < expected.len() {
                    let read = theirs.read(&mut chunk).unwrap();
                    assert!(read > 0);
                    got.extend_from_slice(&chunk[..read]);
                    std::thread::sleep(Duration::from_micros(50));
                }
                got == expected
            });
            let (written, _) = ours.write_all(data).await;
            written.unwrap();
            assert!(draining.join().unwrap());
        });
    }

    /// A socket that always has something to read spends the task's budget,
    /// and the task yields, so the others run.
    #[test]
    fn a_socket_always_ready_does_not_starve_the_other_tasks() {
        const READS: usize = 4096;
        run(async {
            let (reader, mut writer) = pair();
            writer.write_all(&[1; READS]).unwrap();
            arrived(&reader).await;
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

    /// Vectored writes write every buffer in order, borrowed or owned, past
    /// a part already written; and writes fail once the peer has gone.
    #[test]
    fn a_socket_is_written_whole_and_reports_a_peer_gone() {
        run(async {
            let (ours, theirs) = pair();
            let theirs = Nonblocking::new(Socket::new(theirs.into()).unwrap());
            let mut wait = ReadyWait::new();
            let bufs = [IoSlice::new(b"one "), IoSlice::new(b"two")];
            let written = poll_fn(|cx| ours.poll_write_vectored(cx, &bufs, &mut wait)).await;
            assert_eq!(written.unwrap(), 7);
            let mut slices = [IoSlice::new(&[]); OWNED_IOVECS];
            let owned = vec!["ab", "cd", "ef"];
            let (count, len) = gather(&owned, 3, &mut slices);
            assert_eq!((count, len), (2, 3));
            assert_eq!(ours.try_write_vectored(&slices[..count]).unwrap(), 3);
            let owned = vec![" three", " four"];
            let write = ours.write_all_vectored(owned);
            assert_send_sync(&write);
            let (written, _) = write.await;
            written.unwrap();
            let (read, buf) = theirs.read_exact(Vec::with_capacity(21)).await;
            read.unwrap();
            assert_eq!(&buf[..], b"one twodef three four");
            drop(theirs);
            let mut failed = None;
            for _ in 0..100 {
                if let (Err(err), _) = ours.write("gone").await {
                    failed = Some(err);
                    break;
                }
                crate::time::sleep(Duration::from_millis(1)).await;
            }
            let kind = failed.expect("a write to a peer gone fails").kind();
            assert!(
                matches!(
                    kind,
                    io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted
                ),
                "{kind:?}"
            );
        });
    }

    /// An owned vector of more buffers than one write takes is written with
    /// as many writes as it takes, through a socket a thread drains.
    #[test]
    fn an_owned_vector_of_many_buffers_is_written_whole() {
        run(async {
            let (ours, mut theirs) = pair();
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

    /// Where no runtime runs, a read that finds data reads it, and one that
    /// would wait fails as unsupported.
    #[test]
    fn without_a_runtime_reads_are_made_and_waits_are_unsupported() {
        let (reader, mut writer) = pair();
        writer.write_all(b"here").unwrap();
        let mut buf = [MaybeUninit::uninit(); 4];
        let mut wait = ReadyWait::new();
        let read = futures_executor::block_on(async {
            loop {
                match reader.try_read(&mut buf) {
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    read => break read,
                }
            }
        });
        assert_eq!(read.unwrap(), 4);
        let read =
            futures_executor::block_on(poll_fn(|cx| reader.poll_read(cx, &mut buf, &mut wait)));
        assert_eq!(read.unwrap_err().kind(), io::ErrorKind::Unsupported);
    }
}
