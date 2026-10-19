//! A file that is not bound to the ring of one thread: [`File`], which is
//! `Send` and `Sync`, so that a future that must be `Send`, one that
//! `async_trait` boxes or one spawned onto another core with
//! [`spawn_on`](crate::spawn_on), can hold it across an `.await`.
//!
//! The file is a plain descriptor, a [`PortableFd`], and each of its
//! operations is a [`PortableOp`], which queues on the ring of the thread
//! that first polls it and costs there what the ring's own operations do.
//! A future that moves to another thread while its operation is in flight
//! hands the operation back to the ring it queued on, which goes on with
//! it. Dropped, the file closes in order after the operations its ring
//! queued, on whichever thread it is dropped, so that no entry still queued
//! there finds the descriptor gone, or another file under its number;
//! [`close`](File::close) closes it on the ring of the thread that polls it,
//! and reports what closing it found.
//!
//! What the thread-bound [`fs::File`](super::File) has and this one has
//! not is what belongs to one ring: a direct descriptor, polled I/O,
//! registered buffers, multishot reads into a pool and linked chains.
//!
//! Beside the operations on owned buffers, the file reads, writes and seeks
//! through poll methods, as tokio's file does, for the poll-based traits
//! that `ruio-compat`'s adapter gives it: [`poll_read`](File::poll_read),
//! [`poll_write`](File::poll_write), [`poll_flush`](File::poll_flush),
//! [`start_seek`](File::start_seek) and [`poll_seek`](File::poll_seek).
//! They keep a buffer of the file's own and one operation in flight at the
//! file's position: a read into the buffer, whose bytes the caller's buffer
//! takes, and a write of the caller's bytes copied there, which goes behind,
//! as tokio's does: done as far as the caller can tell once it is on its
//! way, and reporting a failure at the next call, which waits for it to
//! finish first. A flush waits for it, and so does closing the file with
//! [`close`](File::close); dropping the file lets it finish. A seek is an
//! `lseek(2)`, made once nothing is in flight, from where the reader is
//! rather than where the file's read ahead of it.
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
//! # }
//! ```

use std::fmt;
use std::future::poll_fn;
use std::io::{self, SeekFrom};
use std::mem::{self, MaybeUninit};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};
use std::path::Path;
use std::ptr;
use std::task::{Context, Poll, ready};

use ruio_driver::buf::{BufResult, IoBuf, IoBufMut, Slice};
use ruio_driver::ops::{self, Fallocate, Fsync, Ftruncate, Read, Statx, Write};
use ruio_driver::{PortableFd, PortableOp};

use super::{METADATA, Metadata, OpenOptions};
use crate::io::{AsyncRead, AsyncWrite};

/// The most one read or write of the poll methods moves, which bounds the
/// buffer they keep, as tokio's file bounds its own.
const MAX_BUF: usize = 2 << 20;

/// An open file that is `Send` and `Sync`, whose every operation runs on
/// the ring of the thread that polls it; see the [module](self).
///
/// Every operation borrows the file, so it cannot close before the
/// operation is queued:
///
/// ```compile_fail
/// # async fn run(file: ruio::fs::portable::File) {
/// let read = file.read_at(Vec::with_capacity(8), 0);
/// drop(file);
/// read.await;
/// # }
/// ```
pub struct File {
    /// What the poll methods keep from one call to the next. Dropped before
    /// the descriptor, which so closes after what it had in flight.
    polling: Polling,
    fd: PortableFd,
}

impl File {
    /// Opens the file at `path` for reading.
    ///
    /// # Errors
    ///
    /// What `openat2(2)` reports, and `InvalidInput` if the path contains a
    /// nul byte.
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new().read(true).open_portable(path).await
    }

    /// Opens the file at `path` for writing, creating it or emptying it.
    ///
    /// # Errors
    ///
    /// As for [`File::open`].
    pub async fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open_portable(path)
            .await
    }

    /// The file whose descriptor `fd` is.
    pub(super) fn from_fd(fd: PortableFd) -> Self {
        Self {
            polling: Polling::new(),
            fd,
        }
    }

    /// Wraps a file from the standard library.
    pub fn from_std(file: std::fs::File) -> Self {
        Self::from_fd(PortableFd::new(OwnedFd::from(file)))
    }

    /// The standard library's file, which stays open. Closing it then closes
    /// it at once, before what a ring may still have queued naming it, as
    /// [`PortableFd::into_owned_fd`] says; dropping this file closes it in
    /// order. A write of the poll methods still in flight goes on, so flush
    /// it first ([`poll_flush`](File::poll_flush)).
    pub fn into_std(self) -> std::fs::File {
        let Self { polling, fd } = self;
        drop(polling);
        fd.into_owned_fd().into()
    }

    /// Reads into `buf`, up to its capacity, from `offset` on. Fewer bytes
    /// come back only at the end of the file, where none do.
    pub fn read_at<B: IoBufMut>(&self, buf: B, offset: u64) -> PortableOp<'_, Read<B>> {
        self.fd.op(|fd| Read::at(fd, buf, offset))
    }

    /// Writes `buf`'s initialized bytes, from `offset` on, or as many as the
    /// file takes.
    pub fn write_at<B: IoBuf>(&self, buf: B, offset: u64) -> PortableOp<'_, Write<B>> {
        self.fd.op(|fd| Write::at(fd, buf, offset))
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
            let (read, rest) = self
                .fd
                .op(|fd| Read::at(fd, buf.slice(start..), offset + done))
                .await;
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
            let (written, rest) = self
                .fd
                .op(|fd| Write::at(fd, buf.slice(done..), offset + done as u64))
                .await;
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
    /// Nothing orders reads and writes at the position that run at once, so
    /// they must not overlap; ones at offsets may.
    pub fn read<B: IoBufMut>(&self, buf: B) -> PortableOp<'_, Read<B>> {
        self.fd.op(|fd| Read::new(fd, buf))
    }

    /// Writes `buf`'s initialized bytes at the file's position, which the
    /// write advances, or at its end if it was opened to append; see
    /// [`read`](File::read).
    pub fn write<B: IoBuf>(&self, buf: B) -> PortableOp<'_, Write<B>> {
        self.fd.op(|fd| Write::new(fd, buf))
    }

    /// Flushes the file's data and metadata to its device, as `fsync(2)`
    /// does.
    pub fn sync_all(&self) -> PortableOp<'_, Fsync> {
        self.fd.op(Fsync::new)
    }

    /// Flushes the file's data, and only the metadata needed to read it
    /// back, as `fdatasync(2)` does.
    pub fn sync_data(&self) -> PortableOp<'_, Fsync> {
        self.fd.op(|fd| Fsync::new(fd).data_only())
    }

    /// Makes the file `len` bytes long, cutting it short or padding it with
    /// zeroes, as `ftruncate(2)` does.
    pub fn set_len(&self, len: u64) -> PortableOp<'_, Ftruncate> {
        self.fd.op(|fd| Ftruncate::new(fd, len))
    }

    /// Allocates space for `len` bytes of the file from `offset`, growing
    /// it if they reach past its end, as `fallocate(2)` does.
    pub fn allocate(&self, offset: u64, len: u64) -> PortableOp<'_, Fallocate> {
        self.fd.op(|fd| Fallocate::new(fd, offset, len))
    }

    /// The file's status, as `statx(2)` reports it.
    ///
    /// # Errors
    ///
    /// What `statx(2)` reports.
    pub async fn metadata(&self) -> io::Result<Metadata> {
        let raw = self.fd.as_raw_fd();
        let stat = self.fd.op(|_| Statx::fd(raw).mask(METADATA)).await?;
        Ok(Metadata { stat: *stat })
    }

    /// Closes the file on the ring of the thread that polls the close,
    /// after what that ring has queued, reporting what closing it found,
    /// such as an error writing back data, where dropping it closes it
    /// without saying. A write of the poll methods still in flight finishes
    /// first, and its failure, if it failed, is what comes back.
    ///
    /// # Errors
    ///
    /// What the write in flight, if any, and `close(2)` report.
    pub async fn close(mut self) -> io::Result<()> {
        let flushed = poll_fn(|cx| self.poll_flush(cx)).await;
        let Self { polling, fd } = self;
        drop(polling);
        let closed = PortableOp::new(ops::Close::new(fd.into_owned_fd())).await;
        flushed.and(closed)
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
        let Self { polling, fd } = self;
        if matches!(polling.busy, Busy::Writing { .. }) {
            ready!(polling.poll_settle(fd, cx));
        }
        if let Some(err) = polling.failed.take() {
            return Poll::Ready(Err(err));
        }
        polling.seek_now(fd);
        if polling.pos < polling.buf.len() || buf.is_empty() {
            return Poll::Ready(Ok(polling.take_into(buf)));
        }
        if matches!(polling.busy, Busy::Idle) {
            let want = buf.len().min(MAX_BUF);
            let mut room = mem::take(&mut polling.buf);
            room.clear();
            room.reserve(want);
            polling.pos = 0;
            polling.busy = Busy::Reading(PortableOp::new(Read::new(fd.fd(), room.slice(..want))));
        }
        let Busy::Reading(read) = &mut polling.busy else {
            unreachable!("a read is in flight");
        };
        let (read, slice) = ready!(fd.poll_op(cx, read));
        polling.busy = Busy::Idle;
        polling.buf = slice.into_inner();
        polling.pos = 0;
        if let Err(err) = read {
            polling.buf.clear();
            return Poll::Ready(Err(err));
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
    /// How the write gone behind before it failed, what the seek back to
    /// the reader reports, or what this write reports if it fails at once.
    pub fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let Self { polling, fd } = self;
        ready!(polling.poll_settle(fd, cx));
        if let Some(err) = polling.failed.take() {
            return Poll::Ready(Err(err));
        }
        polling.seek_now(fd);
        polling.unread(fd)?;
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let len = buf.len().min(MAX_BUF);
        let mut data = mem::take(&mut polling.buf);
        data.clear();
        data.extend_from_slice(&buf[..len]);
        polling.busy = Busy::Writing {
            write: PortableOp::new(Write::new(fd.fd(), data.slice(..))),
            done: 0,
        };
        // Polled once, to be on its way; one that fails at once takes
        // nothing, which this reports.
        if let Poll::Ready(()) = polling.poll_settle(fd, cx)
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
        let Self { polling, fd } = self;
        ready!(polling.poll_settle(fd, cx));
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
        self.polling.seek = Seek::Asked(pos);
        Ok(())
    }

    /// The file's position, once what is in flight has finished: where a
    /// seek asked for with [`start_seek`](File::start_seek) has moved it,
    /// with one `lseek(2)`, or, with none asked, where the reader is, as
    /// tokio's file's `poll_complete` gives it.
    ///
    /// # Errors
    ///
    /// What `lseek(2)` reports, such as `InvalidInput` for a position
    /// before the start of the file.
    pub fn poll_seek(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        let Self { polling, fd } = self;
        ready!(polling.poll_settle(fd, cx));
        Poll::Ready(match mem::replace(&mut polling.seek, Seek::None) {
            Seek::Asked(pos) => polling.move_to(fd, pos),
            Seek::Made(made) => made,
            Seek::None => polling.position(fd),
        })
    }
}

impl AsRawFd for File {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

impl AsFd for File {
    #[inline]
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

impl fmt::Debug for File {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("File")
            .field("fd", &self.fd.fd())
            .finish_non_exhaustive()
    }
}

/// Reads at the file's position, which each read advances, as
/// [`File::read`] does.
impl AsyncRead for File {
    type Read<'a, B: IoBufMut> = PortableOp<'a, Read<B>>;

    #[inline]
    fn read<B: IoBufMut>(&self, buf: B) -> Self::Read<'_, B> {
        File::read(self, buf)
    }
}

/// Writes at the file's position, which each write advances, as
/// [`File::write`] does.
impl AsyncWrite for File {
    type Write<'a, B: IoBuf> = PortableOp<'a, Write<B>>;

    #[inline]
    fn write<B: IoBuf>(&self, buf: B) -> Self::Write<'_, B> {
        File::write(self, buf)
    }
}

/// What the poll methods of a [`File`] keep from one call to the next.
struct Polling {
    /// The operation in flight at the file's position, if one is.
    busy: Busy,
    /// The buffer, while no operation has it: data a read took that the
    /// caller has not yet, from `pos` to its length, or none.
    buf: Vec<u8>,
    pos: usize,
    /// How a write gone behind failed, for the next call to report.
    failed: Option<io::Error>,
    seek: Seek,
}

/// The operation in flight of the poll methods, at the file's position.
enum Busy {
    Idle,
    /// A read into the buffer, from its start.
    Reading(PortableOp<'static, Read<Slice<Vec<u8>>>>),
    /// A write of the buffer's bytes from `done` on, those before it being
    /// written.
    Writing {
        write: PortableOp<'static, Write<Slice<Vec<u8>>>>,
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
    fn new() -> Self {
        Self {
            busy: Busy::Idle,
            buf: Vec::new(),
            pos: 0,
            failed: None,
            seek: Seek::None,
        }
    }

    /// How many bytes a read took into the buffer that the caller has not.
    fn unread_len(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// Waits for the operation in flight, if any, to finish: keeps the data
    /// a read took, writes on what a write left, and records how a write
    /// failed. A read that fails takes nothing, and the next read, made
    /// again, reports it.
    fn poll_settle(&mut self, fd: &PortableFd, cx: &mut Context<'_>) -> Poll<()> {
        loop {
            match &mut self.busy {
                Busy::Idle => return Poll::Ready(()),
                Busy::Reading(read) => {
                    let (read, slice) = ready!(fd.poll_op(cx, read));
                    self.busy = Busy::Idle;
                    self.buf = slice.into_inner();
                    self.pos = 0;
                    if read.is_err() {
                        self.buf.clear();
                    }
                }
                Busy::Writing { write, done } => {
                    let done = *done;
                    let (written, slice) = ready!(fd.poll_op(cx, write));
                    let mut data = slice.into_inner();
                    match written {
                        Ok(written) if written > 0 && done + written < data.len() => {
                            let done = done + written;
                            let rest = Write::new(fd.fd(), data.slice(done..));
                            self.busy = Busy::Writing {
                                write: PortableOp::new(rest),
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
    fn seek_now(&mut self, fd: &PortableFd) {
        if let Seek::Asked(pos) = self.seek {
            self.seek = Seek::Made(self.move_to(fd, pos));
        }
    }

    /// Moves the file's position to `pos`, from where the reader is if it is
    /// the current one, with one `lseek(2)`, letting go of what a read took
    /// ahead of the reader.
    fn move_to(&mut self, fd: &PortableFd, pos: SeekFrom) -> io::Result<u64> {
        let invalid = || io::Error::from(io::ErrorKind::InvalidInput);
        let (offset, whence) = match pos {
            SeekFrom::Start(offset) => (
                i64::try_from(offset).map_err(|_| invalid())?,
                libc::SEEK_SET,
            ),
            SeekFrom::End(offset) => (offset, libc::SEEK_END),
            // The data read ahead lies between the reader and the file's
            // position.
            SeekFrom::Current(offset) => {
                let ahead = i64::try_from(self.unread_len()).map_err(|_| invalid())?;
                (
                    offset.checked_sub(ahead).ok_or_else(invalid)?,
                    libc::SEEK_CUR,
                )
            }
        };
        let moved = lseek(fd, offset, whence)?;
        self.buf.clear();
        self.pos = 0;
        Ok(moved)
    }

    /// Moves the file's position back to where the reader is, letting go of
    /// what a read took ahead of it, so that a write goes there.
    fn unread(&mut self, fd: &PortableFd) -> io::Result<()> {
        if self.unread_len() > 0 {
            self.move_to(fd, SeekFrom::Current(0))?;
        }
        Ok(())
    }

    /// Where the reader is: the file's position, less what a read took
    /// ahead of it.
    fn position(&self, fd: &PortableFd) -> io::Result<u64> {
        let at = lseek(fd, 0, libc::SEEK_CUR)?;
        Ok(at.saturating_sub(self.unread_len() as u64))
    }
}

impl Drop for Polling {
    /// Lets a write gone behind finish, as its caller counts on, and a read
    /// go, which its ring cancels.
    fn drop(&mut self) {
        if let Busy::Writing { write, .. } = mem::replace(&mut self.busy, Busy::Idle) {
            write.detach();
        }
    }
}

/// Moves the position of the file `fd` with one `lseek(2)`, which touches
/// no file data and returns at once.
fn lseek(fd: &PortableFd, offset: i64, whence: libc::c_int) -> io::Result<u64> {
    // SAFETY: lseek(2) touches no memory.
    let at = unsafe { libc::lseek(fd.as_raw_fd(), offset, whence) };
    u64::try_from(at).map_err(|_| io::Error::last_os_error())
}
