//! Moving bytes between descriptors without a buffer in user space, with
//! `splice(2)`.
//!
//! A [`splice`] moves what one descriptor has to another through a pipe the
//! kernel fills from the one and drains into the other, by moving the pages
//! that hold the data rather than copying them: a socket's or a file's pages
//! go into the pipe, and out of it into the other socket, or into the other
//! file's cache. Nothing crosses into user space, and the thread does no
//! copying.
//!
//! The pipe is the thread's: a splice takes one from a store the thread
//! keeps and gives it back when done, so a splice after the first costs no
//! system call to make its pipe; the pipe is grown to a megabyte, so that a
//! splice moves up to that much at once. A splice in, from the source into
//! the pipe, and a splice out, from the pipe into the destination, are in
//! flight at once, so the source and the destination keep each other busy,
//! and the bytes in the pipe are counted so that neither asks for more than
//! the pipe has, or has room for. The kernel runs each splice in an io-wq
//! worker, and would hold the worker while a source has nothing to read or a
//! destination no room to write; so each is linked behind a poll of its
//! socket, which the ring waits on itself, and the worker runs only when
//! there is something to do. A pipe whose slots are full, as it can be when
//! the source's data comes in many small pieces, refuses more rather than
//! hold the worker, and the splice in waits for the next splice out.

use std::cell::RefCell;
use std::fmt;
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use ruio_driver::ops::{self, PollAdd};
use ruio_driver::{Completion, Driver, Fd, Link, Op, Operation};
use ruio_sys as sys;

use crate::fs::File;
use crate::net::{TcpStream, UnixStream};

/// How large a splice's pipe is made, if the system allows: the most one
/// splice moves.
const PIPE_SIZE: u32 = 1 << 20;

/// How many idle pipes a thread keeps for its next splices.
const KEPT_PIPES: usize = 8;

thread_local! {
    /// The pipes of splices that are done, for the next ones.
    static PIPES: RefCell<Vec<Pipe>> = const { RefCell::new(Vec::new()) };
}

/// A pipe a splice moves its bytes through.
struct Pipe {
    read: OwnedFd,
    write: OwnedFd,
    /// How many bytes it holds at most.
    size: u32,
}

impl Pipe {
    /// A pipe: one the thread kept, or a new one, grown to [`PIPE_SIZE`]
    /// if the system allows.
    ///
    /// # Errors
    ///
    /// What `pipe2(2)` reports, such as `EMFILE`.
    fn take() -> io::Result<Rc<Self>> {
        if let Some(pipe) = PIPES.with(|pipes| pipes.borrow_mut().pop()) {
            return Ok(Rc::new(pipe));
        }
        let mut fds = [0; 2];
        // SAFETY: `pipe2` writes two descriptors into the array, which has
        // room for them.
        if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the descriptors are new, and nothing else owns them.
        let read = unsafe { OwnedFd::from_raw_fd(fds[0]) };
        // SAFETY: as above.
        let write = unsafe { OwnedFd::from_raw_fd(fds[1]) };
        // A pipe that cannot grow keeps its size, which is read back.
        // SAFETY: `fcntl` with this command touches no memory.
        unsafe { libc::fcntl(write.as_raw_fd(), libc::F_SETPIPE_SZ, PIPE_SIZE) };
        // SAFETY: as above.
        let size = unsafe { libc::fcntl(write.as_raw_fd(), libc::F_GETPIPE_SZ) };
        let size = u32::try_from(size).unwrap_or(PIPE_SIZE);
        Ok(Rc::new(Self { read, write, size }))
    }

    /// Keeps the pipe for the thread's next splice, if it has room for it;
    /// the pipe must be empty, with no operation on it in flight.
    fn keep(self) {
        PIPES.with(|pipes| {
            let mut pipes = pipes.borrow_mut();
            if pipes.len() < KEPT_PIPES {
                pipes.push(self);
            }
        });
    }
}

/// A splice through a pipe, which it holds: the pipe stays open until the
/// kernel is done with the splice, however soon the [`Splice`] is dropped,
/// as the kernel looks the descriptors up only when an io-wq worker runs
/// the splice.
struct PipeSplice {
    op: ops::Splice,
    _pipe: Rc<Pipe>,
}

// SAFETY: the entry is the inner splice's, which points to no memory; a
// splice posts one completion.
unsafe impl Operation for PipeSplice {
    type Output = io::Result<u32>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        self.op.prepare(sqe);
    }

    fn complete(self, completion: Completion) -> Self::Output {
        self.op.complete(completion)
    }
}

/// A poll of a socket or a file, and the splice that runs once it is ready.
type Gated = Link<Op<PollAdd>, Op<PipeSplice>>;

/// What a [`splice`] moves bytes from or to: a stream socket, or a file,
/// whose descriptor the kernel splices through a pipe. A reference to one,
/// an [`Rc`] or a [`Box`] of one, is one too.
///
/// The trait is sealed: only this crate's types implement it.
pub trait Spliceable: sealed::Spliceable {}

impl Spliceable for TcpStream {}
impl Spliceable for UnixStream {}
impl Spliceable for File {}
impl<T: Spliceable + ?Sized> Spliceable for &T {}
impl<T: Spliceable + ?Sized> Spliceable for Rc<T> {}
impl<T: Spliceable + ?Sized> Spliceable for Box<T> {}

mod sealed {
    use std::rc::Rc;

    use ruio_driver::Fd;

    use crate::fs::File;
    use crate::net::{TcpStream, UnixStream};

    /// The descriptor a splice moves bytes from or to.
    pub trait Spliceable {
        /// The descriptor, for the thread's ring.
        fn splice_fd(&self) -> Fd;
    }

    impl Spliceable for TcpStream {
        fn splice_fd(&self) -> Fd {
            self.fd()
        }
    }

    impl Spliceable for UnixStream {
        fn splice_fd(&self) -> Fd {
            self.fd()
        }
    }

    impl Spliceable for File {
        fn splice_fd(&self) -> Fd {
            self.fd()
        }
    }

    impl<T: Spliceable + ?Sized> Spliceable for &T {
        fn splice_fd(&self) -> Fd {
            T::splice_fd(self)
        }
    }

    impl<T: Spliceable + ?Sized> Spliceable for Rc<T> {
        fn splice_fd(&self) -> Fd {
            T::splice_fd(self)
        }
    }

    impl<T: Spliceable + ?Sized> Spliceable for Box<T> {
        fn splice_fd(&self) -> Fd {
            T::splice_fd(self)
        }
    }
}

/// Moves everything `from` has to `to`, until `from` ends, without a buffer
/// in user space; see [`Splice`].
///
/// A file is read from its position, or from [`offset_in`](Splice::offset_in),
/// and written at its position, or at [`offset_out`](Splice::offset_out);
/// [`limit`](Splice::limit) bounds how much is moved. A direct descriptor
/// must belong to the thread's ring, where the splice runs.
///
/// A splice into a stream does not wait for a `write_behind` in flight on
/// it, as the stream's own writes do: flush the stream first if the order
/// matters.
///
/// ```no_run
/// use ruio::fs::File;
/// use ruio::net::TcpStream;
///
/// # ruio::Runtime::new().unwrap().block_on(async {
/// let file = File::open("video.mp4").await?;
/// let stream = TcpStream::connect("127.0.0.1:8080").await?;
/// let sent = ruio::io::splice(&file, &stream).await?;
/// println!("{sent} bytes, none of them through user space");
/// # Ok::<(), std::io::Error>(())
/// # }).unwrap();
/// ```
pub fn splice<'a, R: Spliceable + ?Sized, W: Spliceable + ?Sized>(
    from: &'a R,
    to: &'a W,
) -> Splice<'a> {
    Splice {
        driver: crate::driver(),
        from: from.splice_fd(),
        to: to.splice_fd(),
        offset_in: None,
        offset_out: None,
        left: None,
        pipe: None,
        fill: None,
        drain: None,
        in_pipe: 0,
        full: false,
        ended: false,
        moved: 0,
        _ends: PhantomData,
    }
}

/// Moves bytes from one descriptor to another through a pipe; made by
/// [`splice`]. The output is how many bytes were moved, or the error that
/// stopped the splice.
///
/// Dropped before it is done, it cancels what it has in flight; the bytes
/// in its pipe then are lost, as they have left the source.
#[must_use = "a splice does nothing until polled"]
pub struct Splice<'a> {
    driver: Driver,
    from: Fd,
    to: Fd,
    /// Where the next splice in reads a file from, if not at its position.
    offset_in: Option<u64>,
    /// Where the next splice out writes a file at, if not at its position.
    offset_out: Option<u64>,
    /// How many bytes may still be read, if there is a limit.
    left: Option<u64>,
    pipe: Option<Rc<Pipe>>,
    /// The splice in, from the source into the pipe, in flight.
    fill: Option<Gated>,
    /// The splice out, from the pipe into the destination, in flight.
    drain: Option<Gated>,
    /// How many bytes the pipe holds.
    in_pipe: u64,
    /// Whether the pipe refused more: its slots are full, until a splice
    /// out frees some.
    full: bool,
    /// Whether the source has ended, or the limit is reached.
    ended: bool,
    /// How many bytes have reached the destination.
    moved: u64,
    _ends: PhantomData<&'a ()>,
}

impl Splice<'_> {
    /// Reads the source, a file, from `offset`, leaving its position alone.
    /// A socket has no offset to read from: the splice fails with `EINVAL`.
    pub fn offset_in(mut self, offset: u64) -> Self {
        self.offset_in = Some(offset);
        self
    }

    /// Writes the destination, a file, at `offset`, leaving its position
    /// alone. A socket has no offset to write at: the splice fails with
    /// `EINVAL`.
    pub fn offset_out(mut self, offset: u64) -> Self {
        self.offset_out = Some(offset);
        self
    }

    /// Moves at most `bytes`, and ends there if the source has not ended
    /// first.
    pub fn limit(mut self, bytes: u64) -> Self {
        self.left = Some(bytes);
        self
    }

    /// The pipe, made or taken from the thread's store on the first call.
    fn pipe(&mut self) -> io::Result<&Rc<Pipe>> {
        if self.pipe.is_none() {
            self.pipe = Some(Pipe::take()?);
        }
        Ok(self.pipe.as_ref().expect("the pipe was just made"))
    }

    /// A poll of `fd` for `events`, and `op` once it is ready.
    fn gated(&self, fd: Fd, events: i16, op: ops::Splice, pipe: &Rc<Pipe>) -> Gated {
        let poll = self
            .driver
            .op(PollAdd::new(fd, events.cast_unsigned().into()));
        let splice = self.driver.op(PipeSplice {
            op,
            _pipe: Rc::clone(pipe),
        });
        poll.link(splice)
    }

    /// Starts the splice in: as much as the pipe has room for, of what is
    /// left to move.
    fn start_fill(&mut self) -> io::Result<()> {
        let pipe = Rc::clone(self.pipe()?);
        let room = u64::from(pipe.size) - self.in_pipe;
        let len = self.left.map_or(room, |left| left.min(room));
        let len = u32::try_from(len).unwrap_or(u32::MAX);
        let mut op =
            ops::Splice::new(self.from, pipe.write.as_raw_fd(), len).flags(libc::SPLICE_F_NONBLOCK);
        if let Some(offset) = self.offset_in {
            op = op.offset_in(offset);
        }
        self.fill = Some(self.gated(self.from, libc::POLLIN, op, &pipe));
        Ok(())
    }

    /// Starts the splice out: everything the pipe holds.
    fn start_drain(&mut self) {
        let pipe = Rc::clone(self.pipe.as_ref().expect("a pipe with bytes in it"));
        let len = u32::try_from(self.in_pipe).unwrap_or(u32::MAX);
        let mut op =
            ops::Splice::new(pipe.read.as_raw_fd(), self.to, len).flags(libc::SPLICE_F_NONBLOCK);
        if let Some(offset) = self.offset_out {
            op = op.offset_out(offset);
        }
        self.drain = Some(self.gated(self.to, libc::POLLOUT, op, &pipe));
    }

    /// Takes in what a splice in did.
    fn filled(&mut self, (polled, spliced): (io::Result<u32>, io::Result<u32>)) -> io::Result<()> {
        polled?;
        match spliced {
            Ok(0) => self.ended = true,
            Ok(read) => {
                let read = u64::from(read);
                self.in_pipe += read;
                if let Some(offset) = &mut self.offset_in {
                    *offset += read;
                }
                if let Some(left) = &mut self.left {
                    *left -= read;
                    if *left == 0 {
                        self.ended = true;
                    }
                }
            }
            // The pipe's slots are full: the next splice out frees some.
            Err(err) if err.kind() == io::ErrorKind::WouldBlock && self.in_pipe > 0 => {
                self.full = true;
            }
            // Nothing in the pipe to wait for: the source said it was
            // ready and then was not, so it is asked again.
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
            Err(err) => return Err(err),
        }
        Ok(())
    }

    /// Takes in what a splice out did.
    fn drained(&mut self, (polled, spliced): (io::Result<u32>, io::Result<u32>)) -> io::Result<()> {
        polled?;
        match spliced {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(written) => {
                let written = u64::from(written);
                self.in_pipe -= written;
                self.moved += written;
                self.full = false;
                if let Some(offset) = &mut self.offset_out {
                    *offset += written;
                }
            }
            // The destination said it had room and then had not: it is
            // asked again.
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
            Err(err) => return Err(err),
        }
        Ok(())
    }

    /// Ends the splice, keeping the pipe for the thread's next one: it is
    /// empty, and nothing is in flight on it.
    fn finish(&mut self) -> u64 {
        if let Some(pipe) = self.pipe.take()
            && let Ok(pipe) = Rc::try_unwrap(pipe)
        {
            pipe.keep();
        }
        self.moved
    }
}

impl Future for Splice<'_> {
    type Output = io::Result<u64>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.left == Some(0) {
            this.ended = true;
        }
        loop {
            if this.ended && this.in_pipe == 0 && this.drain.is_none() {
                return Poll::Ready(Ok(this.finish()));
            }
            if this.drain.is_none() && this.in_pipe > 0 {
                this.start_drain();
            }
            if this.fill.is_none()
                && !this.ended
                && !this.full
                && this.in_pipe < u64::from(this.pipe()?.size)
            {
                this.start_fill()?;
            }
            let mut progressed = false;
            if let Some(drain) = &mut this.drain
                && let Poll::Ready(outputs) = Pin::new(drain).poll(cx)
            {
                this.drain = None;
                this.drained(outputs)?;
                progressed = true;
            }
            if let Some(fill) = &mut this.fill
                && let Poll::Ready(outputs) = Pin::new(fill).poll(cx)
            {
                this.fill = None;
                this.filled(outputs)?;
                progressed = true;
            }
            if !progressed {
                return Poll::Pending;
            }
        }
    }
}

impl fmt::Debug for Splice<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Splice")
            .field("from", &self.from)
            .field("to", &self.to)
            .field("moved", &self.moved)
            .field("in_pipe", &self.in_pipe)
            .field("ended", &self.ended)
            .finish_non_exhaustive()
    }
}
