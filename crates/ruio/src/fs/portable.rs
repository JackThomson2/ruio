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
use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};
use std::path::Path;

use ruio_driver::buf::{BufResult, IoBuf, IoBufMut};
use ruio_driver::ops::{self, Fallocate, Fsync, Ftruncate, Read, Statx, Write};
use ruio_driver::{PortableFd, PortableOp};

use super::{METADATA, Metadata, OpenOptions};
use crate::io::{AsyncRead, AsyncWrite};

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
        Self { fd }
    }

    /// Wraps a file from the standard library.
    pub fn from_std(file: std::fs::File) -> Self {
        Self::from_fd(PortableFd::new(OwnedFd::from(file)))
    }

    /// The standard library's file, which stays open. Closing it then closes
    /// it at once, before what a ring may still have queued naming it, as
    /// [`PortableFd::into_owned_fd`] says; dropping this file closes it in
    /// order.
    pub fn into_std(self) -> std::fs::File {
        self.fd.into_owned_fd().into()
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
    /// without saying.
    ///
    /// # Errors
    ///
    /// What `close(2)` reports.
    pub async fn close(self) -> io::Result<()> {
        PortableOp::new(ops::Close::new(self.fd.into_owned_fd())).await
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
