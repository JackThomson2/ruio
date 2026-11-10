//! Files, read and written through the port.

use std::fmt;
use std::fs;
use std::io;
use std::os::windows::fs::OpenOptionsExt;
use std::os::windows::io::OwnedHandle;
use std::path::Path;

use ruio_buf::{IoBuf, IoBufMut};

use crate::handle::IoHandle;
use crate::op::{Read, ReadExact, Write, WriteAll};
use crate::sys;

/// A file opened for overlapped I/O, whose reads and writes complete to the
/// port of the engine current where it was opened, and which belongs to
/// that engine's thread.
///
/// A read or write takes its buffer and hands it back with the result, as
/// ruio's I/O does on io_uring, and always names its offset: the file keeps
/// no position.
pub struct File {
    handle: IoHandle<OwnedHandle>,
}

impl File {
    /// Opens the file at `path` for reading.
    ///
    /// # Errors
    ///
    /// What opening the file reports, and an error of kind `Other` if the
    /// thread has no current engine, as outside a runtime.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::with(fs::OpenOptions::new().read(true), path)
    }

    /// Opens the file at `path` for writing, creating it if it does not
    /// exist and emptying it if it does, as `std::fs::File::create` does.
    ///
    /// # Errors
    ///
    /// As for [`File::open`].
    pub fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::with(
            fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true),
            path,
        )
    }

    fn with(options: &mut fs::OpenOptions, path: impl AsRef<Path>) -> io::Result<Self> {
        let file = options.custom_flags(sys::FILE_FLAG_OVERLAPPED).open(path)?;
        Ok(Self {
            handle: IoHandle::new(OwnedHandle::from(file))?,
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
}

impl fmt::Debug for File {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("File")
            .field("handle", &self.handle.raw())
            .finish()
    }
}
