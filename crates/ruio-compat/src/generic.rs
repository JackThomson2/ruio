//! The adapter for the streams and files of any engine, built on what
//! every engine's streams have: the reads and writes the traits of
//! `ruio::io` give, and a stream socket's own `write_all`. These are the
//! stream's own futures, which borrow the stream where the ring's
//! operations take a descriptor, so the adapter keeps the stream in an `Rc`
//! and lends it to them from there. It is the adapter wherever the engine
//! is not io_uring, and on io_uring when the build asks for it with
//! `--cfg ruio_compat="generic"`, which is how it is tested there.

use std::fmt;
use std::future::Future;
use std::io::{self, IoSlice};
use std::mem;
use std::net::Shutdown;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker, ready};

use ruio::buf::BufResult;
use ruio::fs::File;
use ruio::io::{AsyncRead, AsyncWrite};
#[cfg(ruio_uring)]
use ruio::net::UnixStream;
use ruio::net::{self, TcpStream};

/// How many bytes a read asks the system for, unless the adapter was made
/// [`with_capacity`](Compat::with_capacity).
const DEFAULT_CAPACITY: usize = 16 << 10;

/// The most bytes a write takes at once, and copies: a bound on the memory
/// a write in flight holds.
const MAX_WRITE: usize = 256 << 10;

/// The largest write buffer an adapter keeps for its next write, once the
/// write is done with it, rather than freeing it: what an idle connection
/// may hold.
const KEPT_WRITE: usize = 16 << 10;

/// What a [`Compat`] adapts: a [`TcpStream`], or a [`File`], read and
/// written at its position. Its reads are the type's own, as the traits of
/// [`ruio::io`] give them, and so are a file's writes; a stream socket's
/// are its `write_all`, which sees a whole buffer out by itself.
///
/// The trait is sealed: only this crate's types implement it.
pub trait Adaptable: sealed::Sealed {}

impl Adaptable for TcpStream {}
impl Adaptable for File {}
#[cfg(ruio_uring)]
impl Adaptable for UnixStream {}

/// The future that shuts down the writing half of a connection.
type Shut = Pin<Box<dyn Future<Output = io::Result<()>>>>;

mod sealed {
    use super::*;

    /// What an adapter needs of what it adapts: its reads and writes, into
    /// and from a vector the adapter owns, with the stream lent to them
    /// for as long as the adapter keeps it.
    pub trait Sealed: 'static {
        /// The stream's read into the adapter's buffer.
        type Read: Future<Output = BufResult<usize, Vec<u8>>> + Unpin + 'static;
        /// The stream's write of the adapter's buffer, which reports how
        /// many bytes it wrote.
        type Write: Future<Output = BufResult<usize, Vec<u8>>> + Unpin + 'static;

        fn read(&'static self, buf: Vec<u8>) -> Self::Read;
        fn write(&'static self, data: Vec<u8>) -> Self::Write;
        /// Shuts down the writing half, as a connection has one; a file
        /// has not.
        fn shutdown(&'static self) -> Option<Shut>;
    }

    impl Sealed for TcpStream {
        type Read = <TcpStream as AsyncRead>::Read<'static, Vec<u8>>;
        type Write = Whole<net::WriteAll<'static, Vec<u8>>>;

        fn read(&'static self, buf: Vec<u8>) -> Self::Read {
            AsyncRead::read(self, buf)
        }

        fn write(&'static self, data: Vec<u8>) -> Self::Write {
            Whole(TcpStream::write_all(self, data))
        }

        fn shutdown(&'static self) -> Option<Shut> {
            Some(Box::pin(TcpStream::shutdown(self, Shutdown::Write)))
        }
    }

    #[cfg(ruio_uring)]
    impl Sealed for UnixStream {
        type Read = <UnixStream as AsyncRead>::Read<'static, Vec<u8>>;
        type Write = Whole<net::WriteAll<'static, Vec<u8>>>;

        fn read(&'static self, buf: Vec<u8>) -> Self::Read {
            AsyncRead::read(self, buf)
        }

        fn write(&'static self, data: Vec<u8>) -> Self::Write {
            Whole(UnixStream::write_all(self, data))
        }

        fn shutdown(&'static self) -> Option<Shut> {
            Some(Box::pin(UnixStream::shutdown(self, Shutdown::Write)))
        }
    }

    impl Sealed for File {
        type Read = <File as AsyncRead>::Read<'static, Vec<u8>>;
        type Write = <File as AsyncWrite>::Write<'static, Vec<u8>>;

        fn read(&'static self, buf: Vec<u8>) -> Self::Read {
            AsyncRead::read(self, buf)
        }

        fn write(&'static self, data: Vec<u8>) -> Self::Write {
            AsyncWrite::write(self, data)
        }

        fn shutdown(&'static self) -> Option<Shut> {
            None
        }
    }

    /// A stream socket's write of a whole buffer, its `write_all`, as a
    /// write that reports how many bytes it wrote: all of them, unless it
    /// failed.
    ///
    /// The adapter starts a write and leaves it, with nobody to poll it
    /// until the next write, flush or close. A plain write the system cut
    /// short would leave the rest of the buffer unsent until then, and its
    /// reader waiting; the stream's `write_all` sees the whole buffer out
    /// itself, io_uring's by having the kernel carry on through a short
    /// send.
    #[derive(Debug)]
    pub struct Whole<F>(pub(super) F);

    impl<F> Future for Whole<F>
    where
        F: Future<Output = BufResult<(), Vec<u8>>> + Unpin,
    {
        type Output = BufResult<usize, Vec<u8>>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let (written, data) = ready!(Pin::new(&mut self.0).poll(cx));
            Poll::Ready((written.map(|()| data.len()), data))
        }
    }
}

/// A ruio stream or file with the poll-based I/O traits: `AsyncRead`,
/// `AsyncBufRead` and `AsyncWrite` of `tokio::io` and of futures-io; see the
/// [crate documentation](crate) for how they map to the stream's own reads
/// and writes, and [`Adaptable`] for what can be adapted.
///
/// Reads go through a buffer of the adapter's, 16 KiB unless made
/// [`with_capacity`](Compat::with_capacity), which a read fills with
/// whatever has arrived. Writes copy up to 256 KiB at a time, and start
/// the stream's write without waiting for it.
///
/// The adapter keeps the stream where its reads and writes borrow it, so
/// it lends the stream out ([`get_ref`](Compat::get_ref)) and does not give
/// it back.
pub struct Compat<S: Adaptable> {
    // Fields drop in order: the operations, which borrow the stream, then
    // the stream.
    /// The read in flight, which holds the buffer meanwhile.
    read: Option<S::Read>,
    /// What was read and not yet taken: `buf[pos..]`.
    buf: Vec<u8>,
    pos: usize,
    /// How many bytes a read asks for.
    capacity: usize,
    writer: Writer<S>,
    /// The shutdown in flight.
    shutdown: Option<Shut>,
    /// Whether the writing half of the connection has been shut down.
    closed: bool,
    /// The stream as the operations borrow it: for as long as they live,
    /// which the adapter, by the order of its fields, makes shorter than
    /// the stream does.
    lent: &'static S,
    /// The stream, shared so that it stays where it is however the adapter
    /// moves, and so that a write left in flight keeps it open.
    stream: Rc<S>,
}

impl<S: Adaptable> Compat<S> {
    /// Adapts `stream`, reading 16 KiB at a time.
    pub fn new(stream: S) -> Self {
        Self::with_capacity(DEFAULT_CAPACITY, stream)
    }

    /// Adapts `stream`, reading up to `capacity` bytes at a time into a
    /// buffer that size.
    ///
    /// # Panics
    ///
    /// If `capacity` is 0.
    pub fn with_capacity(capacity: usize, stream: S) -> Self {
        assert!(capacity > 0, "a read buffer holds at least one byte");
        let stream = Rc::new(stream);
        // SAFETY: the `Rc` keeps the stream where it is, shared and never
        // borrowed mutably, until the last `Rc` of it is dropped. The
        // reference, and the operations made with it, which borrow the
        // stream through it, are kept in the adapter, which drops them
        // before its `Rc` and never hands them out, or in the task that
        // finishes the adapter's last write, which does the same with an
        // `Rc` of its own. So nothing uses the reference once the stream
        // is gone.
        let lent = unsafe { &*Rc::as_ptr(&stream) };
        Self {
            read: None,
            buf: Vec::new(),
            pos: 0,
            capacity,
            writer: Writer::default(),
            shutdown: None,
            closed: false,
            lent,
            stream,
        }
    }

    /// The stream.
    pub fn get_ref(&self) -> &S {
        &self.stream
    }

    /// The data read and not yet taken.
    pub fn buffer(&self) -> &[u8] {
        &self.buf[self.pos..]
    }

    /// The data read and not yet taken, reading some first if there is
    /// none; empty once the stream has ended.
    pub(crate) fn poll_fill_buf(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        if self.pos >= self.buf.len() {
            if self.read.is_none() {
                let mut buf = mem::take(&mut self.buf);
                buf.clear();
                buf.reserve(self.capacity);
                self.pos = 0;
                self.read = Some(S::read(self.lent, buf));
            }
            let read = self.read.as_mut().expect("a read is in flight");
            let (received, filled) = ready!(Pin::new(read).poll(cx));
            self.read = None;
            self.buf = filled;
            received?;
        }
        Poll::Ready(Ok(&self.buf[self.pos..]))
    }

    /// Takes `amount` bytes of what [`poll_fill_buf`](Self::poll_fill_buf)
    /// gave.
    pub(crate) fn consume(&mut self, amount: usize) {
        self.pos = (self.pos + amount).min(self.buf.len());
    }

    /// Hands `put` as much of the data read as `room` bytes take, waiting
    /// for some if there is none.
    pub(crate) fn poll_copy(
        &mut self,
        cx: &mut Context<'_>,
        room: usize,
        mut put: impl FnMut(&[u8]),
    ) -> Poll<io::Result<usize>> {
        if room == 0 {
            return Poll::Ready(Ok(0));
        }
        let data = ready!(self.poll_fill_buf(cx))?;
        let len = data.len().min(room);
        put(&data[..len]);
        self.consume(len);
        Poll::Ready(Ok(len))
    }

    /// Reads into `dst`, as much of the data read as it takes.
    pub(crate) fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        dst: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_copy(cx, dst.len(), |data| {
            dst[..data.len()].copy_from_slice(data);
        })
    }

    /// Writes as many of the bytes of `bufs`, one after another, as one
    /// write takes, once the write before has finished.
    pub(crate) fn poll_write(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let total = bufs.iter().map(|buf| buf.len()).sum::<usize>();
        if total == 0 {
            return Poll::Ready(Ok(0));
        }
        if self.closed {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        ready!(self.writer.poll_done(self.lent, cx))?;
        let len = total.min(MAX_WRITE);
        // The last write's buffer is back by now, as the write has finished.
        let mut data = mem::take(&mut self.writer.spare);
        data.reserve(len);
        for buf in bufs {
            let take = buf.len().min(len - data.len());
            data.extend_from_slice(&buf[..take]);
        }
        self.writer.start(self.lent, data);
        Poll::Ready(Ok(len))
    }

    /// Reports how the writes went, as far as is known, without waiting for
    /// the write in flight, whose failure the next write, flush or close
    /// reports.
    pub(crate) fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.writer.take_ready(self.lent);
        Poll::Ready(self.writer.outcome())
    }

    /// Waits for the write in flight, then shuts down the writing half of
    /// a connection; a file has no half to shut, so its close is the wait
    /// alone, and it is left open for more.
    pub(crate) fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.closed {
            return Poll::Ready(Ok(()));
        }
        if self.shutdown.is_none() {
            ready!(self.writer.poll_done(self.lent, cx))?;
            let Some(shutdown) = S::shutdown(self.lent) else {
                return Poll::Ready(Ok(()));
            };
            self.shutdown = Some(shutdown);
        }
        let shutdown = self.shutdown.as_mut().expect("a shutdown is in flight");
        let result = ready!(shutdown.as_mut().poll(cx));
        self.shutdown = None;
        self.closed = result.is_ok();
        Poll::Ready(result)
    }
}

impl<S: Adaptable> Drop for Compat<S> {
    /// The write in flight is left to finish, in a task of its own that
    /// keeps the stream open until it has, rather than cancelled as a
    /// dropped operation is. The read and the shutdown are cancelled.
    fn drop(&mut self) {
        let Some(write) = self.writer.write.take() else {
            return;
        };
        // Outside a runtime nothing would run the task: the write is
        // cancelled as it is dropped here.
        if ruio::try_handle().is_none() {
            return;
        }
        let mut rest = Detached {
            write,
            len: self.writer.len,
            lent: self.lent,
            _stream: Rc::clone(&self.stream),
        };
        // Most writes have finished by now, with nobody to take the result.
        if Pin::new(&mut rest)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
        {
            drop(ruio::spawn(rest));
        }
    }
}

/// What a write failed with, kept for the writes, flushes and closes after
/// it to report: the system's code where the error has one, and its kind
/// otherwise.
#[derive(Clone, Copy)]
enum Failure {
    Os(i32),
    Kind(io::ErrorKind),
}

impl Failure {
    fn of(err: &io::Error) -> Self {
        err.raw_os_error()
            .map_or_else(|| Failure::Kind(err.kind()), Failure::Os)
    }

    fn error(self) -> io::Error {
        match self {
            Failure::Os(code) => io::Error::from_raw_os_error(code),
            Failure::Kind(kind) => kind.into(),
        }
    }
}

/// Where an adapter's writes go: one write at a time, which the adapter
/// keeps, in flight or finished, until the next write, flush or close takes
/// its result and its buffer.
///
/// The write is started with a waker that wakes nothing, so its completion
/// wakes no task: a task that waits for it, as a write does while the one
/// before is still in flight, leaves its own waker with the write instead.
/// A stream socket's write sees its whole buffer out by itself. A file's
/// may be cut short by the system, and then goes on with the rest of the
/// buffer once its result is taken, so that the bytes the caller was told
/// were taken are all written.
struct Writer<S: Adaptable> {
    /// The write of the last write, until its result is taken.
    write: Option<S::Write>,
    /// How many bytes the write was given.
    len: usize,
    /// Whether a task waits for the write, having left its waker with it,
    /// which a poll with no waker must not replace.
    waited: bool,
    /// The buffer of the last write, emptied, once its write is done with
    /// it, for the next write to fill: a write then allocates nothing.
    spare: Vec<u8>,
    /// What a write failed with: the writes, flushes and closes after it
    /// report it. A write that failed, or wrote nothing, leaves a gap in
    /// the stream, so every write after it fails, as a stream's writes do
    /// once it has broken.
    error: Option<Failure>,
}

impl<S: Adaptable> Default for Writer<S> {
    fn default() -> Self {
        Self {
            write: None,
            len: 0,
            waited: false,
            spare: Vec::new(),
            error: None,
        }
    }
}

impl<S: Adaptable> Writer<S> {
    /// Writes `data` to `stream`, with nobody waiting for it.
    fn start(&mut self, stream: &'static S, mut data: Vec<u8>) {
        loop {
            self.len = data.len();
            let mut write = S::write(stream, data);
            match Pin::new(&mut write).poll(&mut Context::from_waker(Waker::noop())) {
                // Refused by the system: it fails at once.
                Poll::Ready((written, back)) => match self.finish(written, back) {
                    Some(rest) => data = rest,
                    None => return,
                },
                Poll::Pending => {
                    self.write = Some(write);
                    return;
                }
            }
        }
    }

    /// Ready once no write is in flight, having taken the result of the last
    /// one: `Ok`, unless a write failed.
    fn poll_done(&mut self, stream: &'static S, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while let Some(write) = &mut self.write {
            let Poll::Ready((written, data)) = Pin::new(write).poll(cx) else {
                self.waited = true;
                return Poll::Pending;
            };
            self.write = None;
            self.waited = false;
            if let Some(rest) = self.finish(written, data) {
                self.start(stream, rest);
            }
        }
        Poll::Ready(self.outcome())
    }

    /// Takes the result of the last write if it has finished, without
    /// waiting, unless a task waits for it, whose waker the write keeps.
    fn take_ready(&mut self, stream: &'static S) {
        if self.waited {
            return;
        }
        if let Some(write) = &mut self.write
            && let Poll::Ready((written, data)) =
                Pin::new(write).poll(&mut Context::from_waker(Waker::noop()))
        {
            self.write = None;
            if let Some(rest) = self.finish(written, data) {
                self.start(stream, rest);
            }
        }
    }

    /// Records how the last write went, and keeps its buffer for the next
    /// write unless it has grown past what an idle connection should hold;
    /// or, for a write the system cut short, gives back the rest of the
    /// buffer, to write next.
    fn finish(&mut self, written: io::Result<usize>, mut data: Vec<u8>) -> Option<Vec<u8>> {
        let error = match written {
            Ok(written) if written >= self.len => None,
            Ok(written) if written > 0 => {
                data.drain(..written);
                return Some(data);
            }
            Ok(_) => Some(Failure::Kind(io::ErrorKind::WriteZero)),
            Err(err) => Some(Failure::of(&err)),
        };
        if self.error.is_none() {
            self.error = error;
        }
        if data.capacity() <= KEPT_WRITE {
            data.clear();
            self.spare = data;
        }
        None
    }

    /// `Ok`, unless a write failed.
    fn outcome(&self) -> io::Result<()> {
        self.error.map_or(Ok(()), |failure| Err(failure.error()))
    }
}

/// The write an adapter left in flight when it was dropped, run to its end
/// as a task: it goes on with what the system cuts short, as the adapter
/// would have, and lets go of the stream, which then closes, once all of it
/// is written or the write fails.
struct Detached<S: Adaptable> {
    // Dropped before the stream it borrows.
    write: S::Write,
    /// How many bytes the write was given.
    len: usize,
    lent: &'static S,
    /// Keeps the stream open, and where `lent` says it is.
    _stream: Rc<S>,
}

impl<S: Adaptable> Future for Detached<S> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        loop {
            let (written, mut data) = ready!(Pin::new(&mut this.write).poll(cx));
            match written {
                Ok(written) if written > 0 && written < this.len => {
                    data.drain(..written);
                    this.len = data.len();
                    this.write = S::write(this.lent, data);
                }
                _ => return Poll::Ready(()),
            }
        }
    }
}

impl<S: Adaptable + fmt::Debug> fmt::Debug for Compat<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Compat")
            .field("stream", &self.stream)
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}
