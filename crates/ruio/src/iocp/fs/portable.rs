//! A file that is not bound to the port of one thread: [`File`], which is
//! `Send` and `Sync`, so that a future that must be `Send`, one that
//! `async_trait` boxes or one spawned onto another core with
//! [`spawn_on`](crate::spawn_on), can hold it across an `.await`.
//!
//! The file is opened for overlapped I/O and associated with no port, and
//! each of its reads and writes names an event of its own, which the port
//! of the runtime on the thread that polls it waits for
//! ([`ruio_iocp::portable::read_at`]): a future that moves to another
//! thread while its read is in flight waits on that thread's port. What
//! waits for the disk wherever it is called runs on the blocking pool
//! ([`spawn_blocking`](crate::spawn_blocking)), as the thread-bound
//! [`fs::File`](super::File)'s does: opening the file, syncing it, resizing
//! it and asking for its status. Dropped, the file closes on whichever
//! thread it is dropped.
//!
//! Windows keeps no position for a file opened for overlapped I/O, so the
//! file keeps its own, as the thread-bound one does, which its reads and
//! writes at the position share with its poll methods.
//!
//! What the thread-bound [`fs::File`](super::File) has and this one has
//! not is what belongs to one port: the stream of reads into a pool of
//! buffers, [`read_stream`](super::File::read_stream).
//!
//! Beside the operations on owned buffers, the file reads, writes and seeks
//! through poll methods, as tokio's file does, for the poll-based traits:
//! [`poll_read`](File::poll_read), [`poll_write`](File::poll_write),
//! [`poll_flush`](File::poll_flush), [`start_seek`](File::start_seek) and
//! [`poll_seek`](File::poll_seek). They keep a buffer of the file's own and
//! one operation in flight at the file's position: a read into the buffer,
//! whose bytes the caller's buffer takes, and a write of the caller's bytes
//! copied there, which goes behind, as tokio's does: done as far as the
//! caller can tell once it is on its way, and reporting a failure at the
//! next call, which waits for it to finish first. A flush waits for it, and
//! so does closing the file with [`close`](File::close); dropping the file
//! lets it finish, on the system's thread pool. A seek moves the file's
//! position once nothing is in flight, from where the reader is rather than
//! where the file's read ahead of it.
//!
//! Polled where no runtime of ruio's runs, a read or a write that has to
//! wait fails with `Unsupported`, as one of io_uring's portable operations
//! does with no foreign ring set.
//!
//! ```no_run
//! use ruio::fs::portable::File;
//!
//! let cores = ruio::Cores::all()?;
//! let greetings = cores.run(|core| async move {
//!     if core.index() != 0 {
//!         return Ok(None);
//!     }
//!     let file = File::create("greeting").await?;
//!     file.write_all_at("hello", 0).await.0?;
//!     file.sync_all().await?;
//!     // The file goes with the task to the last core, which reads it there.
//!     let task = ruio::spawn_on(ruio::core_count() - 1, async move {
//!         let (read, greeting) = file.read_exact_at(Vec::with_capacity(5), 0).await;
//!         read?;
//!         file.close().await?;
//!         std::io::Result::Ok(greeting)
//!     });
//!     task.await.map_err(std::io::Error::other)?.map(Some)
//! })?;
//! let greeting = greetings.into_iter().next().unwrap()?;
//! assert_eq!(greeting.as_deref(), Some(&b"hello"[..]));
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! The file and the futures of its operations are `Send`:
//!
//! ```
//! use ruio::fs::portable::File;
//!
//! fn assert_send_sync<T: Send + Sync>() {}
//! fn assert_send<T: Send>(_: &T) {}
//!
//! assert_send_sync::<File>();
//! # fn futures(file: &File) {
//! assert_send(&file.read_at(Vec::with_capacity(8), 0));
//! assert_send(&file.write_all_at(b"data".to_vec(), 0));
//! assert_send(&file.sync_all());
//! assert_send(&file.metadata());
//! # }
//! ```

use std::fmt;
use std::future::{Future, poll_fn};
use std::io::{self, Seek as _, SeekFrom};
use std::mem::{self, MaybeUninit};
use std::os::windows::io::{AsHandle, AsRawHandle, BorrowedHandle, RawHandle};
use std::path::Path;
use std::pin::Pin;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, ready};

use ruio_iocp::portable::{self as iocp, ReadAt, WriteAt};

use super::{Call, FileCall, Metadata, OpenOptions, pooled};
use crate::buf::{BufResult, IoBuf, IoBufMut, Slice};
use crate::io::{AsyncRead, AsyncWrite};

/// The most one read or write of the poll methods moves, which bounds the
/// buffer they keep, as tokio's file bounds its own.
const MAX_BUF: usize = 2 << 20;

/// An open file that is `Send` and `Sync`, whose reads and writes wait on
/// the port of the runtime on the thread that polls them; see the
/// [module](self).
///
/// Every operation borrows the file, so it cannot close before the
/// operation is done with:
///
/// ```compile_fail
/// # async fn run(file: ruio::fs::portable::File) {
/// let read = file.read_at(Vec::with_capacity(8), 0);
/// drop(file);
/// read.await;
/// # }
/// ```
pub struct File {
    /// What the poll methods keep from one call to the next, behind a lock
    /// that is never taken: only `&mut File` reaches it, through
    /// [`Mutex::get_mut`], and the lock makes the file `Sync` though the
    /// operations it holds are not. Dropped before the file, which so
    /// closes after them.
    polling: Mutex<Polling>,
    file: Arc<std::fs::File>,
    pos: Position,
}

impl File {
    /// Opens the file at `path` for reading.
    ///
    /// # Errors
    ///
    /// As for [`OpenOptions::open_portable`].
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new().read(true).open_portable(path).await
    }

    /// Opens the file at `path` for writing, creating it or emptying it.
    ///
    /// # Errors
    ///
    /// As for [`OpenOptions::open_portable`].
    pub async fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open_portable(path)
            .await
    }

    /// The file `file`, whose position starts at `at`, and which was opened
    /// to append if `append`.
    pub(super) fn new(file: std::fs::File, at: u64, append: bool) -> Self {
        let file = Arc::new(file);
        Self {
            polling: Mutex::new(Polling::new(Arc::clone(&file))),
            file,
            pos: Position::new(at, append),
        }
    }

    /// Wraps a file from the standard library, whose position the file's
    /// starts at.
    ///
    /// A file opened for overlapped I/O, as
    /// [`open_portable`](OpenOptions::open_portable) opens one, is read and
    /// written as the module says. Of a file opened as the standard library
    /// opens one, for I/O that waits, each read or write is made as it is
    /// first polled, waiting for the disk there. Of a file opened to append,
    /// the position after a write at it is where the write would have gone
    /// in a file not opened to append, as the file cannot tell.
    pub fn from_std(file: std::fs::File) -> Self {
        let at = (&file).stream_position().unwrap_or(0);
        Self::new(file, at, false)
    }

    /// The standard library's file, moved to the file's position. A write
    /// of the poll methods still in flight goes on, so flush it first
    /// ([`poll_flush`](File::poll_flush)); while it does, or a call of the
    /// blocking pool's whose future was dropped runs, the standard
    /// library's file is a handle of its own to the same file.
    pub fn into_std(self) -> std::fs::File {
        let Self { polling, file, pos } = self;
        drop(polling);
        let file = unshare(file);
        let _ = (&file).seek(SeekFrom::Start(pos.get(&file)));
        file
    }

    fn handle(&self) -> BorrowedHandle<'_> {
        self.file.as_handle()
    }

    /// Reads into `buf`, up to its capacity, from `offset` on. Fewer bytes
    /// come back only at the end of the file, where none do.
    pub fn read_at<B: IoBufMut>(&self, buf: B, offset: u64) -> Read<'_, B> {
        Read {
            read: iocp::read_at(self.handle(), buf, offset),
            pos: None,
        }
    }

    /// Writes `buf`'s initialized bytes, from `offset` on, or as many as the
    /// file takes.
    pub fn write_at<B: IoBuf>(&self, buf: B, offset: u64) -> Write<'_, B> {
        Write {
            write: iocp::write_at(self.handle(), buf, offset),
            pos: None,
        }
    }

    /// Fills `buf` to its capacity, after the bytes it holds already, from
    /// `offset` on, with as many reads as it takes.
    ///
    /// The output is the buffer, and `Ok(())` once it is full,
    /// `UnexpectedEof` if the file ends first, or the error that stopped the
    /// reads; either way the buffer holds what was read.
    pub async fn read_exact_at<B: IoBufMut>(&self, buf: B, offset: u64) -> BufResult<(), B> {
        let (mut buf, mut done) = (buf, 0_u64);
        while buf.bytes_init() < buf.bytes_total() {
            let start = buf.bytes_init();
            let (read, rest) =
                iocp::read_at(self.handle(), buf.slice(start..), offset + done).await;
            buf = rest.into_inner();
            match read {
                Ok(0) => return (Err(io::ErrorKind::UnexpectedEof.into()), buf),
                Ok(read) => done += read as u64,
                Err(err) => return (Err(err), buf),
            }
        }
        (Ok(()), buf)
    }

    /// Writes all of `buf`'s initialized bytes from `offset` on, with as
    /// many writes as it takes.
    ///
    /// The output is the buffer, and `Ok(())` once every byte is written, or
    /// the error that stopped the writes, `WriteZero` if the file took none.
    pub async fn write_all_at<B: IoBuf>(&self, buf: B, offset: u64) -> BufResult<(), B> {
        let total = buf.bytes_init();
        let (mut buf, mut done) = (buf, 0);
        while done < total {
            let at = offset + done as u64;
            let (written, rest) = iocp::write_at(self.handle(), buf.slice(done..), at).await;
            buf = rest.into_inner();
            match written {
                Ok(0) => return (Err(io::ErrorKind::WriteZero.into()), buf),
                Ok(written) => done += written,
                Err(err) => return (Err(err), buf),
            }
        }
        (Ok(()), buf)
    }

    /// Reads into `buf` at the file's position, which the read advances.
    ///
    /// Reads and writes at the position start where it is as they are
    /// called, and move it on once they complete, so they must not overlap;
    /// ones at offsets may.
    pub fn read<B: IoBufMut>(&self, buf: B) -> Read<'_, B> {
        Read {
            read: iocp::read_at(self.handle(), buf, self.pos.get(&self.file)),
            pos: Some(&self.pos),
        }
    }

    /// Writes `buf`'s initialized bytes at the file's position, which the
    /// write advances, or at its end if it was opened to append, where it
    /// leaves the position; see [`read`](File::read).
    pub fn write<B: IoBuf>(&self, buf: B) -> Write<'_, B> {
        Write {
            write: iocp::write_at(self.handle(), buf, self.pos.write_offset()),
            pos: Some(&self.pos),
        }
    }

    /// Flushes the file's data and its status to the disk, on the blocking
    /// pool.
    pub fn sync_all(&self) -> SyncAll<'_> {
        FileCall::new(Arc::clone(&self.file), Call::SyncAll)
    }

    /// Flushes the file's data to the disk, on the blocking pool. Windows
    /// has the one flush, so this is [`sync_all`](File::sync_all).
    pub fn sync_data(&self) -> SyncData<'_> {
        FileCall::new(Arc::clone(&self.file), Call::SyncData)
    }

    /// Makes the file `len` bytes long, cutting it short or padding it with
    /// zeroes, on the blocking pool. The file must be open for writing.
    pub fn set_len(&self, len: u64) -> SetLen<'_> {
        FileCall::new(Arc::clone(&self.file), Call::SetLen(len))
    }

    /// Makes room for `len` bytes of the file from `offset`, growing it if
    /// they reach past its end, on the blocking pool, as the thread-bound
    /// file's [`allocate`](super::File::allocate) does.
    pub fn allocate(&self, offset: u64, len: u64) -> Allocate<'_> {
        FileCall::new(Arc::clone(&self.file), Call::Allocate(offset, len))
    }

    /// The file's status, asked for on the blocking pool.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub async fn metadata(&self) -> io::Result<Metadata> {
        let file = Arc::clone(&self.file);
        pooled(move || file.metadata()).await.map(Metadata)
    }

    /// Closes the file, once a write of the poll methods still in flight has
    /// finished, and reports how it failed if it did. Windows reports
    /// nothing as a file closes that a write or a sync has not reported
    /// already.
    ///
    /// # Errors
    ///
    /// What the write in flight, if any, reports.
    pub async fn close(mut self) -> io::Result<()> {
        let flushed = poll_fn(|cx| self.poll_flush(cx)).await;
        drop(self);
        flushed
    }

    /// What the poll methods keep, and the file's position.
    fn polling(&mut self) -> (&mut Polling, &Position) {
        let polling = self
            .polling
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner);
        (polling, &self.pos)
    }

    /// Reads into the caller's `buf` from the file's position, which the
    /// read advances, polled, as tokio's file reads: what the file's own
    /// buffer holds from a read before, or else a read of as many bytes as
    /// `buf` has room for, up to 2 MiB, into that buffer, kept in flight
    /// from one poll to the next, of which `buf` takes what it has room for
    /// as it completes; 0 bytes at the end of the file. The first bytes of
    /// `buf`, as many as were read, are then initialized.
    ///
    /// A write of [`poll_write`](File::poll_write)'s still in flight
    /// finishes first. The poll methods share the file's position with
    /// [`read`](File::read) and [`write`](File::write), so the two must not
    /// be used at once.
    ///
    /// # Errors
    ///
    /// What the read reports, or how a write gone behind before it failed.
    pub fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [MaybeUninit<u8>],
    ) -> Poll<io::Result<usize>> {
        let (polling, pos) = self.polling();
        if matches!(polling.busy, Busy::Writing { .. }) {
            ready!(polling.poll_settle(pos, cx));
        }
        if let Some(err) = polling.failed.take() {
            return Poll::Ready(Err(err));
        }
        polling.seek_now(pos);
        if polling.pos < polling.buf.len() || buf.is_empty() {
            return Poll::Ready(Ok(polling.take_into(buf)));
        }
        if matches!(polling.busy, Busy::Idle) {
            let want = buf.len().min(MAX_BUF);
            let mut room = mem::take(&mut polling.buf);
            room.clear();
            room.reserve(want);
            polling.pos = 0;
            let at = pos.get(&polling.file);
            polling.busy = Busy::Reading(iocp::read_at(polling.handle, room.slice(..want), at));
        }
        let Busy::Reading(read) = &mut polling.busy else {
            unreachable!("a read is in flight");
        };
        let (read, slice) = ready!(Pin::new(read).poll(cx));
        polling.busy = Busy::Idle;
        polling.buf = slice.into_inner();
        polling.pos = 0;
        match read {
            Ok(read) => pos.read(read),
            Err(err) => {
                polling.buf.clear();
                return Poll::Ready(Err(err));
            }
        }
        Poll::Ready(Ok(polling.take_into(buf)))
    }

    /// Writes the caller's `buf`, up to 2 MiB of it, at the file's
    /// position, which the write advances, or at its end if the file was
    /// opened to append, polled, as tokio's file writes: the bytes are
    /// copied into the file's own buffer and the write goes behind, so that
    /// it is done, as far as the caller can tell, once it is on its way, and
    /// all it took is written, with as many writes as it takes. The next
    /// call of the poll methods waits for it to finish, and reports how it
    /// failed if it did; [`poll_flush`](File::poll_flush) waits for it.
    ///
    /// The bytes go where the reader is: data a read before took into the
    /// file's buffer and the caller has not, which lies before the file's
    /// position, is let go of, and the write goes before it.
    ///
    /// # Errors
    ///
    /// How the write gone behind before it failed, or what this write
    /// reports if it fails at once, as one to a file not open for writing
    /// does here.
    pub fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let (polling, pos) = self.polling();
        ready!(polling.poll_settle(pos, cx));
        if let Some(err) = polling.failed.take() {
            return Poll::Ready(Err(err));
        }
        polling.seek_now(pos);
        polling.unread(pos);
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let len = buf.len().min(MAX_BUF);
        let mut data = mem::take(&mut polling.buf);
        data.clear();
        data.extend_from_slice(&buf[..len]);
        polling.busy = Busy::Writing {
            write: iocp::write_at(polling.handle, data.slice(..), pos.write_offset()),
            done: 0,
        };
        // Polled once, to be on its way; one that fails at once takes
        // nothing, which this reports.
        if let Poll::Ready(()) = polling.poll_settle(pos, cx)
            && let Some(err) = polling.failed.take()
        {
            return Poll::Ready(Err(err));
        }
        Poll::Ready(Ok(len))
    }

    /// Waits for the write of [`poll_write`](File::poll_write)'s in flight,
    /// if any, and the read too, to finish, and reports how the write
    /// failed, if it did: what the file holds back.
    ///
    /// # Errors
    ///
    /// How the write gone behind failed.
    pub fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let (polling, pos) = self.polling();
        ready!(polling.poll_settle(pos, cx));
        Poll::Ready(polling.failed.take().map_or(Ok(()), Err))
    }

    /// Asks for the file's position to move to `pos`, as tokio's file's
    /// `start_seek` does: [`poll_seek`](File::poll_seek) makes the seek,
    /// once nothing is in flight, and gives the new position. Asked again
    /// before then, the last asking stands. From the current position, a
    /// seek starts where the reader is.
    ///
    /// # Errors
    ///
    /// None as it is asked; what the seek finds comes from `poll_seek`.
    pub fn start_seek(&mut self, pos: SeekFrom) -> io::Result<()> {
        self.polling().0.seek = Seek::Asked(pos);
        Ok(())
    }

    /// The file's position, once what is in flight has finished: where a
    /// seek asked for with [`start_seek`](File::start_seek) has moved it,
    /// or, with none asked, where the reader is, as tokio's file's
    /// `poll_complete` gives it.
    ///
    /// # Errors
    ///
    /// `InvalidInput` for a position before the start of the file or past
    /// what a signed offset counts, as `lseek(2)` reports, and what the
    /// system reports asked for the length of the file, to seek from its
    /// end.
    pub fn poll_seek(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        let (polling, pos) = self.polling();
        ready!(polling.poll_settle(pos, cx));
        Poll::Ready(match mem::replace(&mut polling.seek, Seek::None) {
            Seek::Asked(to) => polling.move_to(pos, to),
            Seek::Made(made) => made,
            Seek::None => Ok(polling.reader(pos)),
        })
    }
}

impl AsRawHandle for File {
    #[inline]
    fn as_raw_handle(&self) -> RawHandle {
        self.file.as_raw_handle()
    }
}

impl AsHandle for File {
    #[inline]
    fn as_handle(&self) -> BorrowedHandle<'_> {
        self.file.as_handle()
    }
}

impl fmt::Debug for File {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("File")
            .field("handle", &self.file.as_raw_handle())
            .field("pos", &self.pos.at.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// Reads at the file's position, which each read advances, as
/// [`File::read`] does.
impl AsyncRead for File {
    type Read<'a, B: IoBufMut> = self::Read<'a, B>;

    #[inline]
    fn read<B: IoBufMut>(&self, buf: B) -> Self::Read<'_, B> {
        File::read(self, buf)
    }
}

/// Writes at the file's position, which each write advances, as
/// [`File::write`] does.
impl AsyncWrite for File {
    type Write<'a, B: IoBuf> = self::Write<'a, B>;

    #[inline]
    fn write<B: IoBuf>(&self, buf: B) -> Self::Write<'_, B> {
        File::write(self, buf)
    }
}

/// The future of a portable file's [`read_at`](File::read_at) and
/// [`read`](File::read), which is `Send`; see [`fs::Read`](super::Read). It
/// hands the buffer back with how many bytes it read: 0 at or past the end
/// of the file.
///
/// It starts the read when first polled. Dropping it before the read
/// completes cancels the read, and waits for the cancellation to complete.
#[must_use = "futures do nothing unless polled"]
pub struct Read<'a, B> {
    read: ReadAt<'a, B>,
    /// The file's position, which a read at it moves on.
    pos: Option<&'a Position>,
}

impl<B: IoBufMut> Future for Read<'_, B> {
    type Output = BufResult<usize, B>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (result, buf) = ready!(Pin::new(&mut self.read).poll(cx));
        if let (Some(pos), Ok(read)) = (self.pos, &result) {
            pos.read(*read);
        }
        Poll::Ready((result, buf))
    }
}

impl<B> fmt::Debug for Read<'_, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.read, f)
    }
}

/// The future of a portable file's [`write_at`](File::write_at) and
/// [`write`](File::write), which is `Send`; see [`fs::Write`](super::Write).
/// It hands the buffer back with how many bytes it wrote.
///
/// It starts the write when first polled. Dropping it before the write
/// completes cancels the write, which may have written some of the buffer
/// by then, and waits for the cancellation to complete.
#[must_use = "futures do nothing unless polled"]
pub struct Write<'a, B> {
    write: WriteAt<'a, B>,
    /// The file's position, which a write at it moves on.
    pos: Option<&'a Position>,
}

impl<B: IoBuf> Future for Write<'_, B> {
    type Output = BufResult<usize, B>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (result, buf) = ready!(Pin::new(&mut self.write).poll(cx));
        if let (Some(pos), Ok(written)) = (self.pos, &result) {
            pos.wrote(*written);
        }
        Poll::Ready((result, buf))
    }
}

impl<B> fmt::Debug for Write<'_, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.write, f)
    }
}

/// The future of [`File::sync_all`], which is `Send`.
pub type SyncAll<'a> = FileCall<'a, File>;

/// The future of [`File::sync_data`], which is `Send`.
pub type SyncData<'a> = FileCall<'a, File>;

/// The future of [`File::set_len`], which is `Send`.
pub type SetLen<'a> = FileCall<'a, File>;

/// The future of [`File::allocate`], which is `Send`.
pub type Allocate<'a> = FileCall<'a, File>;

/// The file's position, which Windows keeps none of for a file opened for
/// overlapped I/O, and which the reads and writes at the position share
/// with the poll methods. They must not overlap, so a position moved from
/// one thread to the next goes with the task that moves it, and needs no
/// ordering of its own.
struct Position {
    at: AtomicU64,
    /// Whether the file was opened to append.
    append: bool,
    /// Whether a write at the position of a file opened to append has left
    /// the position at the end of the file, wherever that is, which what
    /// next starts from the position asks the system for.
    at_end: AtomicBool,
}

impl Position {
    fn new(at: u64, append: bool) -> Self {
        Self {
            at: AtomicU64::new(at),
            append,
            at_end: AtomicBool::new(false),
        }
    }

    /// Where a read at the position starts: the position, or the end of
    /// `file`, where a write at the position left it if the file was opened
    /// to append, which the system is asked for here, as the thread-bound
    /// file asks, the length of an open file being what the file system
    /// holds of it, with no disk to wait for. Where the system cannot say,
    /// the position is where the writes moved it on, and it is asked again
    /// the next time.
    fn get(&self, file: &std::fs::File) -> u64 {
        if self.at_end.load(Ordering::Relaxed)
            && let Ok(status) = file.metadata()
        {
            self.set(status.len());
        }
        self.at.load(Ordering::Relaxed)
    }

    /// Where a write at the position goes: the position, which a file
    /// opened to append ignores.
    fn write_offset(&self) -> u64 {
        self.at.load(Ordering::Relaxed)
    }

    fn set(&self, at: u64) {
        self.at.store(at, Ordering::Relaxed);
        self.at_end.store(false, Ordering::Relaxed);
    }

    /// Moves the position on past what a read at it took.
    fn read(&self, read: usize) {
        self.at.fetch_add(read as u64, Ordering::Relaxed);
    }

    /// Moves the position on past what a write at it wrote, and so to the
    /// end of a file opened to append.
    fn wrote(&self, written: usize) {
        self.at.fetch_add(written as u64, Ordering::Relaxed);
        if self.append {
            self.at_end.store(true, Ordering::Relaxed);
        }
    }
}

/// What the poll methods of a [`File`] keep from one call to the next.
struct Polling {
    /// The operation in flight at the file's position, if one is, which
    /// borrows `handle`.
    busy: Busy,
    /// The buffer, while no operation has it: data a read took that the
    /// caller has not yet, from `pos` to its length, or none.
    buf: Vec<u8>,
    pos: usize,
    /// How a write gone behind failed, for the next call to report.
    failed: Option<io::Error>,
    seek: Seek,
    /// The file's handle, which `file` holds open for as long as `busy`
    /// may borrow it.
    handle: BorrowedHandle<'static>,
    /// The file, which a write in flight as the file is dropped holds open
    /// until it has finished. Dropped after `busy`.
    file: Arc<std::fs::File>,
}

/// The operation in flight of the poll methods, at the file's position.
enum Busy {
    Idle,
    /// A read into the buffer, from its start.
    Reading(ReadAt<'static, Slice<Vec<u8>>>),
    /// A write of the buffer's bytes from `done` on, those before it being
    /// written.
    Writing {
        write: WriteAt<'static, Slice<Vec<u8>>>,
        done: usize,
    },
}

/// A seek of the poll methods.
enum Seek {
    None,
    /// Asked for, and made once nothing is in flight.
    Asked(SeekFrom),
    /// Made, as another poll method found it asked for, for `poll_seek` to
    /// report.
    Made(io::Result<u64>),
}

impl Polling {
    fn new(file: Arc<std::fs::File>) -> Self {
        let handle = file.as_raw_handle();
        // SAFETY: the handle is the file's, which `file` holds open until
        // the polling is dropped, after `busy`, the one field that borrows
        // it, as the polling's own `drop` leaves no write in `busy`.
        let handle = unsafe { BorrowedHandle::borrow_raw(handle) };
        Self {
            busy: Busy::Idle,
            buf: Vec::new(),
            pos: 0,
            failed: None,
            seek: Seek::None,
            handle,
            file,
        }
    }

    /// How many bytes a read took into the buffer that the caller has not.
    fn unread_len(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// Where the reader is: the file's position, less what a read took
    /// ahead of it.
    fn reader(&self, pos: &Position) -> u64 {
        pos.get(&self.file).saturating_sub(self.unread_len() as u64)
    }

    /// Waits for the operation in flight, if any, to finish: keeps the data
    /// a read took, writes on what a write left, and records how a write
    /// failed. A read that fails takes nothing, and the next read, made
    /// again, reports it.
    fn poll_settle(&mut self, pos: &Position, cx: &mut Context<'_>) -> Poll<()> {
        loop {
            match &mut self.busy {
                Busy::Idle => return Poll::Ready(()),
                Busy::Reading(read) => {
                    let (read, slice) = ready!(Pin::new(read).poll(cx));
                    self.busy = Busy::Idle;
                    self.buf = slice.into_inner();
                    self.pos = 0;
                    match read {
                        Ok(read) => pos.read(read),
                        Err(_) => self.buf.clear(),
                    }
                }
                Busy::Writing { write, done } => {
                    let done = *done;
                    let (written, slice) = ready!(Pin::new(write).poll(cx));
                    let mut data = slice.into_inner();
                    if let Ok(written) = written {
                        pos.wrote(written);
                    }
                    match written {
                        Ok(written) if written > 0 && done + written < data.len() => {
                            let done = done + written;
                            let rest = data.slice(done..);
                            self.busy = Busy::Writing {
                                write: iocp::write_at(self.handle, rest, pos.write_offset()),
                                done,
                            };
                        }
                        written => {
                            self.failed = match written {
                                Ok(0) => Some(io::ErrorKind::WriteZero.into()),
                                Ok(_) => None,
                                Err(err) => Some(err),
                            };
                            self.busy = Busy::Idle;
                            data.clear();
                            self.buf = data;
                            self.pos = 0;
                        }
                    }
                }
            }
        }
    }

    /// Copies into `into` what it has room for of the data a read took,
    /// which so goes to the caller, and says how much.
    fn take_into(&mut self, into: &mut [MaybeUninit<u8>]) -> usize {
        let held = &self.buf[self.pos..];
        let len = held.len().min(into.len());
        // SAFETY: `into` has room for `len` bytes, which `held` has, and the
        // two do not overlap, the one being the file's and the other the
        // caller's.
        unsafe { ptr::copy_nonoverlapping(held.as_ptr(), into.as_mut_ptr().cast::<u8>(), len) };
        self.pos += len;
        if self.pos == self.buf.len() {
            self.buf.clear();
            self.pos = 0;
        }
        len
    }

    /// Makes the seek asked for, if one is, which `poll_seek` then reports:
    /// what comes next goes where it moved the file to.
    fn seek_now(&mut self, pos: &Position) {
        if let Seek::Asked(to) = self.seek {
            self.seek = Seek::Made(self.move_to(pos, to));
        }
    }

    /// Moves the file's position to `to`, from where the reader is if it is
    /// the current one, letting go of what a read took ahead of the reader.
    /// A position past what a signed offset counts is refused, as
    /// `lseek(2)` refuses it.
    fn move_to(&mut self, pos: &Position, to: SeekFrom) -> io::Result<u64> {
        let moved = match to {
            SeekFrom::Start(offset) => Some(offset),
            SeekFrom::End(offset) => self.file.metadata()?.len().checked_add_signed(offset),
            SeekFrom::Current(offset) => self.reader(pos).checked_add_signed(offset),
        };
        let moved = moved
            .filter(|&moved| i64::try_from(moved).is_ok())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "a seek to before the start of the file, or past what an offset counts",
                )
            })?;
        pos.set(moved);
        self.buf.clear();
        self.pos = 0;
        Ok(moved)
    }

    /// Moves the file's position back to where the reader is, letting go of
    /// what a read took ahead of it, so that a write goes there.
    fn unread(&mut self, pos: &Position) {
        if self.unread_len() > 0 {
            pos.set(self.reader(pos));
            self.buf.clear();
            self.pos = 0;
        }
    }
}

impl Drop for Polling {
    /// Lets a write gone behind finish, as its caller counts on, on the
    /// system's thread pool, which holds the file open until it has; a read
    /// in flight is cancelled.
    fn drop(&mut self) {
        if let Busy::Writing { write, .. } = mem::replace(&mut self.busy, Busy::Idle) {
            write.detach(Arc::clone(&self.file));
        }
    }
}

/// The standard library's file that `file` shares, once nothing else has
/// it: a write let go of, or a call of the blocking pool's whose future was
/// dropped, holds it until it has finished. Until then a handle of its own
/// to the file stands in, which the system makes unless it is out of them.
fn unshare(mut file: Arc<std::fs::File>) -> std::fs::File {
    loop {
        match Arc::try_unwrap(file) {
            Ok(file) => return file,
            Err(shared) => {
                if let Ok(own) = shared.try_clone() {
                    return own;
                }
                file = shared;
                std::thread::yield_now();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A read that took more than the caller has yet leaves the rest ahead
    /// of the reader, which a seek from the current position starts from,
    /// and which a write after it goes before, where the reader is. Windows
    /// makes a read of a file in the cache at once, so the read ahead is
    /// arranged here as the read leaves it.
    #[test]
    fn a_write_after_a_read_ahead_goes_where_the_reader_is() -> io::Result<()> {
        let name = format!("ruio-portable-ahead-{}", std::process::id());
        let path = std::env::temp_dir().join(name);
        std::fs::write(&path, b"0123456789")?;
        let read = crate::Runtime::new()?.block_on(async {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open_portable(&path)
                .await?;
            {
                let (polling, pos) = file.polling();
                polling.buf = b"0123456789".to_vec();
                polling.pos = 3;
                pos.set(10);
            }
            assert_eq!(poll_fn(|cx| file.poll_seek(cx)).await?, 3);
            assert_eq!(poll_fn(|cx| file.poll_write(cx, b"ab")).await?, 2);
            poll_fn(|cx| file.poll_flush(cx)).await?;
            assert_eq!(poll_fn(|cx| file.poll_seek(cx)).await?, 5);
            let mut rest = [MaybeUninit::new(0); 16];
            let read = poll_fn(|cx| file.poll_read(cx, &mut rest)).await?;
            // SAFETY: the read initialized as many bytes.
            let read = rest[..read]
                .iter()
                .map(|byte| unsafe { byte.assume_init() });
            io::Result::Ok(read.collect::<Vec<_>>())
        });
        let written = std::fs::read(&path);
        let _ = std::fs::remove_file(&path);
        assert_eq!(read?, b"56789");
        assert_eq!(written?, b"012ab56789");
        Ok(())
    }

    /// A seek from the end asks the system for the file's length, and one to
    /// before the start of the file, or past what an offset counts, is
    /// refused, leaving the position as it was.
    #[test]
    fn seeks_from_the_end_ask_for_the_length() -> io::Result<()> {
        let name = format!("ruio-portable-end-{}", std::process::id());
        let path = std::env::temp_dir().join(name);
        std::fs::write(&path, b"0123456789")?;
        let seeks = crate::Runtime::new()?.block_on(async {
            let mut file = File::open(&path).await?;
            file.start_seek(SeekFrom::End(-4))?;
            let end = poll_fn(|cx| file.poll_seek(cx)).await?;
            file.start_seek(SeekFrom::End(-11))?;
            let err = poll_fn(|cx| file.poll_seek(cx)).await.unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
            file.start_seek(SeekFrom::Start(u64::MAX))?;
            let err = poll_fn(|cx| file.poll_seek(cx)).await.unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
            let (read, rest) = file.read(Vec::with_capacity(8)).await;
            read?;
            io::Result::Ok((end, rest))
        });
        let _ = std::fs::remove_file(&path);
        assert_eq!(seeks?, (6, b"6789".to_vec()));
        Ok(())
    }
}
