//! Files, read and written through the port.

use std::fmt;
use std::fs;
use std::io;
use std::os::windows::fs::OpenOptionsExt;
use std::os::windows::io::AsRawHandle;
use std::path::Path;
use std::sync::Arc;

use ruio_buf::{IoBuf, IoBufMut};

use crate::handle::{IoHandle, Raw};
use crate::op::{Read, ReadExact, Write, WriteAll};
use crate::sys;

impl Raw for Arc<fs::File> {
    fn raw(&self) -> sys::Handle {
        self.as_raw_handle()
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

    /// Takes a file that was opened for overlapped I/O, as
    /// [`OpenOptions::open_std`] opens one on any thread, and associates it
    /// with the port of the thread's current engine.
    ///
    /// # Errors
    ///
    /// What the system reports for a file that was not opened for
    /// overlapped I/O, or that another port has already, and an error of
    /// kind `Other` if the thread has no current engine.
    pub fn from_std(file: fs::File) -> io::Result<Self> {
        Ok(Self {
            handle: IoHandle::new(Arc::new(file))?,
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
