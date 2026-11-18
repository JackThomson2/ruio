//! Files, read and written through the port.

use std::fmt;
use std::fs;
use std::future::{Future, poll_fn};
use std::io;
use std::os::windows::fs::OpenOptionsExt;
use std::os::windows::io::AsRawHandle;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker, ready};

use ruio_buf::{IoBuf, IoBufMut};

use crate::buf::{BufLease, BufPool, Claim};
use crate::handle::{IoHandle, Raw, query_file};
use crate::op::{Read, ReadExact, Write, WriteAll};
use crate::sys;

impl Raw for Arc<fs::File> {
    fn raw(&self) -> sys::Handle {
        self.as_raw_handle()
    }

    /// A file on a disk may: a read or a write of one succeeds or fails
    /// outright. Of another kind of file, a pipe or a device, one may end
    /// with a warning, which the call that started it reports as a failure
    /// though the operation completed, and it is then not plain from the
    /// call whether a completion follows.
    fn may_skip_port(&self) -> bool {
        // SAFETY: the handle is open, as the file is borrowed.
        let kind = unsafe { sys::GetFileType(self.raw()) };
        kind == sys::FILE_TYPE_DISK
    }
}

/// A file opened for overlapped I/O, whose reads and writes complete to the
/// port of the engine current where it was opened, and which belongs to
/// that engine's thread.
///
/// A read or write takes its buffer and hands it back with the result, as
/// ruio's I/O does on io_uring, and always names its offset: the file keeps
/// no position. A file opened to append writes at its end whatever the
/// offset.
///
/// A file taken from the standard library that was opened for I/O that
/// waits, as the standard library opens one, cannot be associated with a
/// port; see [`from_std`](File::from_std).
pub struct File {
    handle: IoHandle<Arc<fs::File>>,
}

impl File {
    /// Opens the file at `path` for reading.
    ///
    /// # Errors
    ///
    /// What opening the file reports, and an error of kind `Other` if the
    /// thread has no current engine, as outside a runtime.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new().read(true).open(path)
    }

    /// Opens the file at `path` for writing, creating it if it does not
    /// exist and emptying it if it does, as `std::fs::File::create` does.
    ///
    /// # Errors
    ///
    /// As for [`File::open`].
    pub fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
    }

    /// The options a file is opened with, none of them set; see
    /// [`OpenOptions`].
    pub fn options() -> OpenOptions {
        OpenOptions::new()
    }

    /// Takes a file from the standard library, for the thread's current
    /// engine.
    ///
    /// A file opened for overlapped I/O, as [`OpenOptions::open_std`] opens
    /// one on any thread, is associated with the engine's port. One opened
    /// for I/O that waits, as the standard library's own options open one,
    /// cannot be: each of its reads and writes is made in full by the poll
    /// that starts it, which waits for the disk, and its futures' timeouts
    /// never run out.
    ///
    /// # Errors
    ///
    /// What the system reports for a file opened for overlapped I/O that
    /// another port has already, and an error of kind `Other` if the thread
    /// has no current engine.
    pub fn from_std(file: fs::File) -> io::Result<Self> {
        Ok(Self {
            handle: IoHandle::adopt(Arc::new(file))?,
        })
    }

    /// The file as the standard library's, shared so that another thread
    /// can have it, for the calls that have no overlapped form and wait for
    /// the disk: flushing the file, resizing it and asking about it.
    ///
    /// Reads and writes go through the port and not through this: the
    /// standard library ends the process if a read or a write of its own,
    /// on a file opened for overlapped I/O, does not complete at once.
    pub fn as_std(&self) -> &Arc<fs::File> {
        self.handle.io()
    }

    /// The file as the standard library's, as [`as_std`](File::as_std)
    /// shares it, out of the file. A file opened for overlapped I/O stays
    /// so, and associated with the engine's port.
    pub fn into_std(self) -> Arc<fs::File> {
        self.handle.into_io()
    }

    /// Whether the file was opened to append, so that its writes go to its
    /// end whatever offset they name. Not if the system cannot say.
    pub fn appends(&self) -> bool {
        query_file(self.handle.raw(), sys::FILE_ACCESS_INFORMATION).is_some_and(|access| {
            access & sys::FILE_APPEND_DATA != 0 && access & sys::FILE_WRITE_DATA == 0
        })
    }

    /// Reads into `buf`, up to its `bytes_total`, from `offset`; see
    /// [`Read`].
    pub fn read_at<B: IoBufMut>(&self, buf: B, offset: u64) -> Read<'_, B> {
        Read::new(self.handle.target(), buf, offset)
    }

    /// Writes `buf`'s bytes at `offset`; see [`Write`].
    pub fn write_at<B: IoBuf>(&self, buf: B, offset: u64) -> Write<'_, B> {
        Write::new(self.handle.target(), buf, offset)
    }

    /// Fills `buf` after its contents, up to its `bytes_total`, from
    /// `offset` on, in as many reads as that takes; see [`ReadExact`].
    pub fn read_exact_at<B: IoBufMut>(&self, buf: B, offset: u64) -> ReadExact<'_, B> {
        ReadExact::at(self.handle.target(), buf, offset)
    }

    /// Writes all of `buf`'s bytes from `offset` on, in as many writes as
    /// that takes; see [`WriteAll`].
    pub fn write_all_at<B: IoBuf>(&self, buf: B, offset: u64) -> WriteAll<'_, B> {
        WriteAll::at(self.handle.target(), buf, offset)
    }

    /// The file's data from `offset` on, read into buffers of `pool`; see
    /// [`ReadStream`].
    pub fn read_stream(&self, pool: &BufPool, offset: u64) -> ReadStream<'_> {
        ReadStream {
            file: self,
            claim: Claim::new(pool),
            offset,
            state: Reading::Idle,
        }
    }
}

impl fmt::Debug for File {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("File")
            .field("handle", &self.handle.raw())
            .finish()
    }
}

/// A file's data from an offset on, read into a [`BufPool`]. Made by
/// [`File::read_stream`].
///
/// Each item is one read's data, in one buffer of the pool, and the stream
/// ends at the end of the file, or after yielding an error. One read is in
/// flight at a time, started as the stream is polled: it takes a buffer of
/// the pool then, waiting for one to return if every buffer is leased, and
/// holds it until it completes.
///
/// So the stream suits a file on a disk, whose reads do not wait for data
/// to arrive. Of a pipe opened as a file it holds a buffer for as long as
/// it waits.
///
/// Dropping the stream cancels the read in flight, whose buffer returns to
/// the pool once the cancellation has completed.
pub struct ReadStream<'a> {
    file: &'a File,
    claim: Claim,
    /// Where the next read starts.
    offset: u64,
    state: Reading<'a>,
}

enum Reading<'a> {
    /// No read is in flight: the next poll starts one.
    Idle,
    /// A read into a buffer of the pool is in flight.
    Started(Read<'a, BufLease>),
    /// The file ended, or an error was yielded.
    Ended,
}

impl ReadStream<'_> {
    /// The next data, or `None` at the end of the file or once an error has
    /// been yielded.
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<BufLease>>> {
        self.step(Some(cx))
    }

    /// The next data; see [`poll_next`](ReadStream::poll_next).
    pub async fn next(&mut self) -> Option<io::Result<BufLease>> {
        poll_fn(|cx| self.poll_next(cx)).await
    }

    /// The next data if it has been read already, or `None`, without
    /// waiting, and without starting a read.
    pub fn next_ready(&mut self) -> Option<io::Result<BufLease>> {
        match self.step(None) {
            Poll::Ready(next) => next,
            Poll::Pending => None,
        }
    }

    /// Whether the stream has ended: the file has ended, or an error has
    /// been yielded.
    pub fn is_ended(&self) -> bool {
        matches!(self.state, Reading::Ended)
    }

    /// Where the next read starts: after the data the stream has yielded.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Takes the stream as far as it goes without waiting: to its next
    /// data, or its end. Where it has to wait, it arranges for the task of
    /// `cx` to be woken, or, without one, starts nothing and arranges
    /// nothing.
    fn step(&mut self, mut cx: Option<&mut Context<'_>>) -> Poll<Option<io::Result<BufLease>>> {
        loop {
            match &mut self.state {
                Reading::Ended => return Poll::Ready(None),
                Reading::Idle => {
                    let Some(cx) = cx.as_deref_mut() else {
                        return Poll::Pending;
                    };
                    let lease = ready!(self.claim.poll_take(cx));
                    let target = self.file.handle.target();
                    self.state = Reading::Started(Read::new(target, lease, self.offset));
                }
                Reading::Started(read) => {
                    let (result, lease) = match cx.as_deref_mut() {
                        Some(cx) => ready!(Pin::new(read).poll(cx)),
                        // A read whose completion has been delivered is
                        // finished by its poll, which keeps no waker.
                        None if read.is_done() => {
                            let mut cx = Context::from_waker(Waker::noop());
                            ready!(Pin::new(read).poll(&mut cx))
                        }
                        None => return Poll::Pending,
                    };
                    match result {
                        Ok(0) => self.state = Reading::Ended,
                        Ok(read) => {
                            self.offset += read as u64;
                            self.state = Reading::Idle;
                            return Poll::Ready(Some(Ok(lease)));
                        }
                        Err(err) => {
                            self.state = Reading::Ended;
                            return Poll::Ready(Some(Err(err)));
                        }
                    }
                }
            }
        }
    }
}

impl fmt::Debug for ReadStream<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadStream")
            .field("offset", &self.offset)
            .field("ended", &self.is_ended())
            .finish_non_exhaustive()
    }
}

/// How a file is opened for overlapped I/O: the options of the standard
/// library's `OpenOptions`, with the same meanings.
///
/// [`open`](OpenOptions::open) opens the file and associates it with the
/// port of the thread's engine. Opening may wait for the disk, so a runtime
/// that must not wait opens with [`open_std`](OpenOptions::open_std) on
/// another thread, and takes the file with [`File::from_std`].
#[derive(Clone, Debug)]
pub struct OpenOptions {
    options: fs::OpenOptions,
}

impl OpenOptions {
    /// Options with none set: a file opened with them as they are can be
    /// neither read nor written.
    pub fn new() -> Self {
        let mut options = fs::OpenOptions::new();
        options.custom_flags(sys::FILE_FLAG_OVERLAPPED);
        Self { options }
    }

    /// Opens the file for reading.
    pub fn read(&mut self, read: bool) -> &mut Self {
        self.options.read(read);
        self
    }

    /// Opens the file for writing.
    pub fn write(&mut self, write: bool) -> &mut Self {
        self.options.write(write);
        self
    }

    /// Opens the file for writing at its end, whatever offset a write
    /// names.
    pub fn append(&mut self, append: bool) -> &mut Self {
        self.options.append(append);
        self
    }

    /// Empties the file as it is opened, which must be for writing.
    pub fn truncate(&mut self, truncate: bool) -> &mut Self {
        self.options.truncate(truncate);
        self
    }

    /// Creates the file if it does not exist, which needs it opened for
    /// writing or appending.
    pub fn create(&mut self, create: bool) -> &mut Self {
        self.options.create(create);
        self
    }

    /// Creates the file, and fails with `AlreadyExists` if it exists.
    pub fn create_new(&mut self, create_new: bool) -> &mut Self {
        self.options.create_new(create_new);
        self
    }

    /// Sets flags of `CreateFileW`'s `dwFlagsAndAttributes` beside
    /// `FILE_FLAG_OVERLAPPED`, which is always set, in place of any set
    /// before: `FILE_FLAG_SEQUENTIAL_SCAN`, say.
    pub fn custom_flags(&mut self, flags: u32) -> &mut Self {
        self.options.custom_flags(flags | sys::FILE_FLAG_OVERLAPPED);
        self
    }

    /// Opens the file at `path` for overlapped I/O, as the standard
    /// library's file, on any thread, for [`File::from_std`] to take.
    ///
    /// # Errors
    ///
    /// What opening the file reports.
    pub fn open_std(&self, path: impl AsRef<Path>) -> io::Result<fs::File> {
        self.options.open(path)
    }

    /// Opens the file at `path`, and associates it with the port of the
    /// thread's current engine.
    ///
    /// # Errors
    ///
    /// What opening the file reports, and an error of kind `Other` if the
    /// thread has no current engine, as outside a runtime.
    pub fn open(&self, path: impl AsRef<Path>) -> io::Result<File> {
        File::from_std(self.open_std(path)?)
    }
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self::new()
    }
}
