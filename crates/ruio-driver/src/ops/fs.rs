//! File and file-system operations.
//!
//! The kernel copies a path when it takes the entry, and resolves paths
//! against plain descriptors only: none of these take a direct descriptor
//! for a directory. Most run in an io-wq worker, since the calls they stand
//! for can block: open when a lookup needs I/O, and statx, fsync, fallocate,
//! ftruncate, unlink, rename and mkdir always.

use std::ffi::CString;
use std::io;
use std::mem;
use std::os::fd::{IntoRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use ruio_sys as sys;

use super::Install;
use crate::{Completion, Descriptor, Driver, Fd, Operation, PolledFd};

/// A path as the kernel reads it, ending in a nul byte.
fn c_path(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "a path cannot contain a nul byte",
        )
    })
}

/// Where the kernel reads a path from.
fn path_addr(path: &CString) -> u64 {
    path.as_ptr().expose_provenance() as u64
}

/// Opens a file, like `openat2(2)`.
///
/// The file is a plain descriptor, closed on `exec`, unless
/// [`direct`](OpenAt::direct) asks for a direct descriptor.
#[derive(Debug)]
pub struct OpenAt {
    dir: RawFd,
    path: CString,
    // Boxed, as for `Connect`: the entry points to it.
    how: Box<libc::open_how>,
    install: Install,
}

impl OpenAt {
    /// Opens `path` for reading, relative to the working directory if it is
    /// relative.
    ///
    /// # Errors
    ///
    /// `InvalidInput` if the path contains a nul byte.
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::at(libc::AT_FDCWD, path)
    }

    /// Opens `path` for reading, relative to the directory open as `dir`, a
    /// plain descriptor, if it is relative.
    ///
    /// # Errors
    ///
    /// As for [`OpenAt::new`].
    pub fn at(dir: RawFd, path: impl AsRef<Path>) -> io::Result<Self> {
        // SAFETY: all zeroes is a valid `open_how`: read-only, resolving as
        // `openat(2)` does.
        let how: libc::open_how = unsafe { mem::zeroed() };
        Ok(Self {
            dir,
            path: c_path(path.as_ref())?,
            how: Box::new(how),
            install: Install::Plain,
        })
    }

    /// Sets the `open(2)` flags, such as `O_RDWR | O_CREAT` or `O_DIRECT`.
    /// `O_CLOEXEC` is added for a plain descriptor.
    pub fn flags(mut self, flags: i32) -> Self {
        self.how.flags = u64::from(flags.cast_unsigned());
        self
    }

    /// Sets the permissions of a file the open creates, before the umask.
    /// An open that cannot create a file, without `O_CREAT` or `O_TMPFILE`,
    /// ignores them, as `openat(2)` does; `openat2(2)` would refuse them.
    pub fn mode(mut self, mode: u32) -> Self {
        self.how.mode = u64::from(mode);
        self
    }

    /// Sets how the path resolves, with `RESOLVE_*` flags such as
    /// `RESOLVE_BENEATH`.
    pub fn resolve(mut self, resolve: u64) -> Self {
        self.how.resolve = resolve;
        self
    }

    /// Makes the file a direct descriptor, in a free slot of the registered
    /// file table of `driver`'s ring; see [`Socket::direct`](super::Socket::direct).
    pub fn direct(mut self, driver: &Driver) -> Self {
        self.install = Install::Direct(driver.id());
        self
    }
}

// SAFETY: the entry points to the path and to the boxed `open_how`, which the
// operation owns and which stay put when it moves. An open posts one
// completion.
unsafe impl Operation for OpenAt {
    type Output = io::Result<Descriptor>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        // The kernel refuses close-on-exec for a direct descriptor, which has
        // nothing to close.
        let cloexec = libc::O_CLOEXEC as u64;
        match self.install {
            Install::Plain => self.how.flags |= cloexec,
            Install::Direct(_) => self.how.flags &= !cloexec,
        }
        let tmpfile = libc::O_TMPFILE as u64;
        let creates =
            self.how.flags & libc::O_CREAT as u64 != 0 || self.how.flags & tmpfile == tmpfile;
        if !creates {
            self.how.mode = 0;
        }
        sqe.opcode = sys::IORING_OP_OPENAT2;
        sqe.fd = self.dir;
        sqe.addr = path_addr(&self.path);
        sqe.off = (&raw const *self.how).expose_provenance() as u64;
        sqe.len = mem::size_of::<libc::open_how>() as u32;
        sqe.file_index = self.install.file_index();
    }

    fn complete(self, completion: Completion) -> Self::Output {
        completion.result().map(|res| self.install.descriptor(res))
    }
}

/// Reads a file's status, like `statx(2)`: by path, or of an open file.
///
/// The kernel takes only plain descriptors, for the file as for a
/// directory.
#[derive(Debug)]
pub struct Statx {
    dir: RawFd,
    path: CString,
    flags: i32,
    mask: u32,
    // Boxed: the kernel writes it when it completes the operation.
    buf: Box<libc::statx>,
}

impl Statx {
    /// The status of the file at `path`, relative to the working directory
    /// if it is relative, following a final symbolic link.
    ///
    /// # Errors
    ///
    /// `InvalidInput` if the path contains a nul byte.
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::at(libc::AT_FDCWD, path)
    }

    /// The status of the file at `path`, relative to the directory open as
    /// `dir` if it is relative.
    ///
    /// # Errors
    ///
    /// As for [`Statx::new`].
    pub fn at(dir: RawFd, path: impl AsRef<Path>) -> io::Result<Self> {
        Ok(Self::with(dir, c_path(path.as_ref())?, 0))
    }

    /// The status of the file open as `fd`, a plain descriptor.
    pub fn fd(fd: RawFd) -> Self {
        Self::with(fd, CString::default(), libc::AT_EMPTY_PATH)
    }

    fn with(dir: RawFd, path: CString, flags: i32) -> Self {
        Self {
            dir,
            path,
            flags,
            mask: libc::STATX_BASIC_STATS | libc::STATX_BTIME,
            // SAFETY: all zeroes is a valid `statx`.
            buf: Box::new(unsafe { mem::zeroed() }),
        }
    }

    /// Adds `AT_*` flags, such as `AT_SYMLINK_NOFOLLOW`, which reports on a
    /// final symbolic link itself.
    pub fn flags(mut self, flags: i32) -> Self {
        self.flags |= flags;
        self
    }

    /// Asks for the `STATX_*` fields in `mask`, rather than the basic ones
    /// and the birth time. The kernel may fill in more or fewer, and says
    /// which in `stx_mask`.
    pub fn mask(mut self, mask: u32) -> Self {
        self.mask = mask;
        self
    }
}

// SAFETY: the entry points to the path and to the boxed buffer, which the
// operation owns and which stay put when it moves; the kernel writes the
// buffer before completing. A statx posts one completion.
unsafe impl Operation for Statx {
    type Output = io::Result<Box<libc::statx>>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_STATX;
        sqe.fd = self.dir;
        sqe.addr = path_addr(&self.path);
        sqe.off = (&raw mut *self.buf).expose_provenance() as u64;
        sqe.len = self.mask;
        sqe.op_flags = self.flags.cast_unsigned();
    }

    fn complete(self, completion: Completion) -> Self::Output {
        completion.result().map(|_| self.buf)
    }
}

/// Flushes a file to its device, like `fsync(2)`, or `fdatasync(2)` with
/// [`data_only`](Fsync::data_only).
#[derive(Debug)]
pub struct Fsync {
    fd: Fd,
    flags: u32,
}

impl Fsync {
    /// Flushes the data and metadata of the file open as `fd`.
    pub fn new(fd: impl Into<Fd>) -> Self {
        Self {
            fd: fd.into(),
            flags: 0,
        }
    }

    /// Flushes the data and only the metadata needed to read it back, such
    /// as the file's length but not its times.
    pub fn data_only(mut self) -> Self {
        self.flags |= sys::IORING_FSYNC_DATASYNC;
        self
    }
}

// SAFETY: the entry points to no memory, and an fsync posts one completion.
unsafe impl Operation for Fsync {
    type Output = io::Result<()>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_FSYNC;
        self.fd.apply(sqe);
        sqe.op_flags = self.flags;
    }

    fn complete(self, completion: Completion) -> Self::Output {
        completion.result().map(drop)
    }
}

/// Allocates space for a range of a file, like `fallocate(2)`; with a mode,
/// it can also punch holes or zero ranges.
#[derive(Debug)]
pub struct Fallocate {
    fd: Fd,
    mode: i32,
    offset: u64,
    len: u64,
}

impl Fallocate {
    /// Allocates `len` bytes of the file open as `fd` from `offset`,
    /// growing it if they reach past its end.
    pub fn new(fd: impl Into<Fd>, offset: u64, len: u64) -> Self {
        Self {
            fd: fd.into(),
            mode: 0,
            offset,
            len,
        }
    }

    /// Sets the `FALLOC_FL_*` mode, such as `FALLOC_FL_KEEP_SIZE`.
    pub fn mode(mut self, mode: i32) -> Self {
        self.mode = mode;
        self
    }
}

// SAFETY: the entry points to no memory, and a fallocate posts one
// completion.
unsafe impl Operation for Fallocate {
    type Output = io::Result<()>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_FALLOCATE;
        self.fd.apply(sqe);
        sqe.off = self.offset;
        sqe.addr = self.len;
        sqe.len = self.mode.cast_unsigned();
    }

    fn complete(self, completion: Completion) -> Self::Output {
        completion.result().map(drop)
    }
}

/// Sets a file's length, like `ftruncate(2)`, cutting it short or padding it
/// with zeroes.
#[derive(Debug)]
pub struct Ftruncate {
    fd: Fd,
    len: u64,
}

impl Ftruncate {
    /// Makes the file open as `fd` `len` bytes long.
    pub fn new(fd: impl Into<Fd>, len: u64) -> Self {
        Self { fd: fd.into(), len }
    }
}

// SAFETY: the entry points to no memory, and an ftruncate posts one
// completion.
unsafe impl Operation for Ftruncate {
    type Output = io::Result<()>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_FTRUNCATE;
        self.fd.apply(sqe);
        sqe.off = self.len;
    }

    fn complete(self, completion: Completion) -> Self::Output {
        completion.result().map(drop)
    }
}

/// Removes a name from the file system, like `unlinkat(2)`: a file's, or
/// with [`directory`](UnlinkAt::directory) an empty directory's.
#[derive(Debug)]
pub struct UnlinkAt {
    dir: RawFd,
    path: CString,
    flags: i32,
}

impl UnlinkAt {
    /// Removes `path`, relative to the working directory if it is relative.
    ///
    /// # Errors
    ///
    /// `InvalidInput` if the path contains a nul byte.
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::at(libc::AT_FDCWD, path)
    }

    /// Removes `path`, relative to the directory open as `dir` if it is
    /// relative.
    ///
    /// # Errors
    ///
    /// As for [`UnlinkAt::new`].
    pub fn at(dir: RawFd, path: impl AsRef<Path>) -> io::Result<Self> {
        Ok(Self {
            dir,
            path: c_path(path.as_ref())?,
            flags: 0,
        })
    }

    /// Removes an empty directory rather than a file (`AT_REMOVEDIR`).
    pub fn directory(mut self) -> Self {
        self.flags |= libc::AT_REMOVEDIR;
        self
    }
}

// SAFETY: the entry points to the path, which the operation owns. An unlink
// posts one completion.
unsafe impl Operation for UnlinkAt {
    type Output = io::Result<()>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_UNLINKAT;
        sqe.fd = self.dir;
        sqe.addr = path_addr(&self.path);
        sqe.op_flags = self.flags.cast_unsigned();
    }

    fn complete(self, completion: Completion) -> Self::Output {
        completion.result().map(drop)
    }
}

/// Makes a directory, like `mkdirat(2)`.
#[derive(Debug)]
pub struct MkdirAt {
    dir: RawFd,
    path: CString,
    mode: u32,
}

impl MkdirAt {
    /// Makes a directory at `path`, relative to the working directory if it
    /// is relative, with permissions `0o777` before the umask.
    ///
    /// # Errors
    ///
    /// `InvalidInput` if the path contains a nul byte.
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::at(libc::AT_FDCWD, path)
    }

    /// Makes a directory at `path`, relative to the directory open as `dir`
    /// if it is relative.
    ///
    /// # Errors
    ///
    /// As for [`MkdirAt::new`].
    pub fn at(dir: RawFd, path: impl AsRef<Path>) -> io::Result<Self> {
        Ok(Self {
            dir,
            path: c_path(path.as_ref())?,
            mode: 0o777,
        })
    }

    /// Sets the directory's permissions, before the umask.
    pub fn mode(mut self, mode: u32) -> Self {
        self.mode = mode;
        self
    }
}

// SAFETY: the entry points to the path, which the operation owns. A mkdir
// posts one completion.
unsafe impl Operation for MkdirAt {
    type Output = io::Result<()>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_MKDIRAT;
        sqe.fd = self.dir;
        sqe.addr = path_addr(&self.path);
        sqe.len = self.mode;
    }

    fn complete(self, completion: Completion) -> Self::Output {
        completion.result().map(drop)
    }
}

/// Renames a file, like `renameat2(2)`, replacing what the new name names
/// unless [`flags`](RenameAt::flags) say otherwise.
#[derive(Debug)]
pub struct RenameAt {
    old_dir: RawFd,
    old: CString,
    new_dir: RawFd,
    new: CString,
    flags: u32,
}

impl RenameAt {
    /// Renames `from` to `to`, each relative to the working directory if it
    /// is relative.
    ///
    /// # Errors
    ///
    /// `InvalidInput` if a path contains a nul byte.
    pub fn new(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<Self> {
        Self::at(libc::AT_FDCWD, from, libc::AT_FDCWD, to)
    }

    /// Renames `from`, relative to the directory open as `old_dir`, to `to`,
    /// relative to `new_dir`, where they are relative.
    ///
    /// # Errors
    ///
    /// As for [`RenameAt::new`].
    pub fn at(
        old_dir: RawFd,
        from: impl AsRef<Path>,
        new_dir: RawFd,
        to: impl AsRef<Path>,
    ) -> io::Result<Self> {
        Ok(Self {
            old_dir,
            old: c_path(from.as_ref())?,
            new_dir,
            new: c_path(to.as_ref())?,
            flags: 0,
        })
    }

    /// Sets the `RENAME_*` flags, such as `RENAME_NOREPLACE`.
    pub fn flags(mut self, flags: u32) -> Self {
        self.flags = flags;
        self
    }
}

// SAFETY: the entry points to the two paths, which the operation owns. A
// rename posts one completion.
unsafe impl Operation for RenameAt {
    type Output = io::Result<()>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_RENAMEAT;
        sqe.fd = self.old_dir;
        sqe.addr = path_addr(&self.old);
        sqe.off = path_addr(&self.new);
        sqe.len = self.new_dir.cast_unsigned();
        sqe.op_flags = self.flags;
    }

    fn complete(self, completion: Completion) -> Self::Output {
        completion.result().map(drop)
    }
}

/// Closes a plain descriptor, like `close(2)`, reporting what closing it
/// found, such as an error writing back data.
///
/// The operation owns the descriptor. Once the kernel has the entry the
/// descriptor is the kernel's to close, whatever the result says; dropped
/// before that, the operation closes it as dropping an [`OwnedFd`] does. A
/// direct descriptor closes with [`DirectFd::close`](crate::DirectFd::close).
#[derive(Debug)]
pub struct Close {
    /// Taken when the entry is prepared.
    fd: Option<OwnedFd>,
    raw: RawFd,
}

impl Close {
    /// Closes `fd`.
    pub fn new(fd: OwnedFd) -> Self {
        Self {
            fd: Some(fd),
            raw: -1,
        }
    }
}

// SAFETY: the entry points to no memory, and a close posts one completion.
unsafe impl Operation for Close {
    type Output = io::Result<()>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        // From here the kernel closes the descriptor: dropping the operation
        // must not close the number again, which could by then name another
        // file.
        if let Some(fd) = self.fd.take() {
            self.raw = fd.into_raw_fd();
        }
        sqe.opcode = sys::IORING_OP_CLOSE;
        sqe.fd = self.raw;
    }

    fn complete(self, completion: Completion) -> Self::Output {
        completion.result().map(drop)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(operation: &mut impl Operation) -> sys::io_uring_sqe {
        let mut sqe = sys::io_uring_sqe::default();
        operation.prepare(&mut sqe);
        sqe
    }

    /// What `fsync` and `fdatasync` do differs only on the device, where a
    /// test cannot see it; the entry shows which the kernel is asked for.
    #[test]
    fn a_data_only_fsync_asks_for_datasync() {
        let sqe = entry(&mut Fsync::new(3));
        assert_eq!((sqe.opcode, sqe.op_flags), (sys::IORING_OP_FSYNC, 0));
        let sqe = entry(&mut Fsync::new(3).data_only());
        assert_eq!(sqe.op_flags, sys::IORING_FSYNC_DATASYNC);
    }
}

/// Puts a plain descriptor into a free slot of the polled ring's file table
/// (`IORING_OP_FILES_UPDATE` with `IORING_FILE_INDEX_ALLOC`): what
/// [`Driver::install_polled`] runs. The output is the slot, as a
/// [`PolledFd`].
#[derive(Debug)]
pub struct InstallPolled {
    /// The ID of the driver whose polled ring takes the file.
    ring: u64,
    /// Boxed: the kernel reads the descriptor from it as it takes the
    /// entry, and writes the slot it chose back as it completes.
    fds: Box<[i32; 1]>,
}

impl InstallPolled {
    /// Installs the file open as `fd` into the polled ring of the driver
    /// with ID `ring`.
    pub(crate) fn new(ring: u64, fd: RawFd) -> Self {
        Self {
            ring,
            fds: Box::new([fd]),
        }
    }
}

// SAFETY: the entry points to the boxed array, which the operation owns and
// which stays put when it moves; the kernel writes it before completing. An
// update posts one completion.
unsafe impl Operation for InstallPolled {
    type Output = io::Result<PolledFd>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_FILES_UPDATE;
        sqe.addr = (&raw mut *self.fds).expose_provenance() as u64;
        sqe.len = 1;
        sqe.off = u64::from(sys::IORING_FILE_INDEX_ALLOC);
    }

    fn complete(self, completion: Completion) -> Self::Output {
        completion.result()?;
        // The kernel wrote the slot where the descriptor was.
        let slot = u32::try_from(self.fds[0])
            .map_err(|_| io::Error::other("the kernel reported no slot for the file"))?;
        Ok(PolledFd::new(self.ring, slot))
    }

    #[inline]
    fn polled(&self) -> bool {
        true
    }
}

/// Closes a slot of the registered file table: what
/// [`DirectFd::close`](crate::DirectFd::close) runs.
#[derive(Debug)]
pub(crate) struct CloseSlot(pub(crate) u32);

// SAFETY: the entry points to no memory, and a close posts one completion.
unsafe impl Operation for CloseSlot {
    type Output = io::Result<()>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_CLOSE;
        sqe.file_index = self.0 + 1;
    }

    fn complete(self, completion: Completion) -> Self::Output {
        completion.result().map(drop)
    }
}
