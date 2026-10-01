//! Reading directories.

use std::ffi::OsString;
use std::fmt;
use std::future::poll_fn;
use std::io;
use std::mem::offset_of;
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::ffi::OsStringExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::task::{Context, Poll};

use ruio_driver::Descriptor;
use ruio_driver::ops::{OpenAt, Statx};

use super::{FileType, METADATA, Metadata};
use crate::net::Plain;

/// How many bytes of entries one `getdents64(2)` reads.
const BATCH: usize = 32 << 10;

/// The entries of the directory at `path`, which [`ReadDir`] reads.
///
/// # Errors
///
/// What `openat2(2)` reports, such as `NotFound`, or `NotADirectory` for a
/// path that names something else, and `InvalidInput` if the path contains
/// a nul byte.
pub async fn read_dir(path: impl AsRef<Path>) -> io::Result<ReadDir> {
    let path = path.as_ref();
    let open = OpenAt::new(path)?.flags(libc::O_RDONLY | libc::O_DIRECTORY);
    let fd: OwnedFd = match crate::driver().op(open).await? {
        Descriptor::Plain(fd) => fd,
        Descriptor::Direct(_) => unreachable!("an open makes a plain descriptor unless asked"),
    };
    Ok(ReadDir {
        dir: Arc::new(Dir {
            fd: Plain::new(std::fs::File::from(fd)),
            path: path.to_owned(),
        }),
        buf: Vec::with_capacity(BATCH),
        pos: 0,
        state: State::Unread,
    })
}

/// An open directory, which its reader and entries share.
struct Dir {
    fd: Plain<std::fs::File>,
    /// The path it was opened at, which entries' paths start with.
    path: PathBuf,
}

/// The entries of a directory, but for `.` and `..`, in the order the file
/// system keeps them. Made by [`read_dir`].
///
/// io_uring has no operation that reads a directory, so the reader reads
/// its entries with `getdents64(2)`, 32 KiB of them at a time, on the
/// thread: that blocks the thread while the kernel reads the directory,
/// which is quick once it is cached, as resolving a host name blocks it.
/// Between batches the reader lets the thread's other tasks run.
///
/// ```no_run
/// # ruio::Runtime::new().unwrap().block_on(async {
/// let mut entries = ruio::fs::read_dir(".").await?;
/// while let Some(entry) = entries.next_entry().await? {
///     println!("{:?}: {:?}", entry.file_name(), entry.file_type().await?);
/// }
/// # Ok::<(), std::io::Error>(())
/// # }).unwrap();
/// ```
pub struct ReadDir {
    dir: Arc<Dir>,
    /// The entries read and not yet taken: `buf[pos..]`.
    buf: Vec<u8>,
    pos: usize,
    state: State,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    /// Nothing read yet.
    Unread,
    /// A batch was read, and the reader has not yielded since.
    Read,
    /// The reader yielded, and reads the next batch at its next poll.
    Yielded,
    /// The directory has no more entries.
    Ended,
}

impl ReadDir {
    /// The next entry, or `None` once there are no more.
    ///
    /// # Errors
    ///
    /// What `getdents64(2)` reports.
    pub async fn next_entry(&mut self) -> io::Result<Option<DirEntry>> {
        poll_fn(|cx| self.poll_next_entry(cx)).await
    }

    /// The next entry, or `None` once there are no more. Before it reads
    /// another batch it wakes the task and returns `Pending` once, so that
    /// the thread's other tasks run.
    ///
    /// # Errors
    ///
    /// As for [`next_entry`](ReadDir::next_entry).
    pub fn poll_next_entry(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Option<DirEntry>>> {
        loop {
            if let Some(entry) = self.take()? {
                return Poll::Ready(Ok(Some(entry)));
            }
            match self.state {
                State::Ended => return Poll::Ready(Ok(None)),
                State::Read => {
                    self.state = State::Yielded;
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                State::Unread | State::Yielded => self.fill()?,
            }
        }
    }

    /// Takes the next entry of those read, skipping `.` and `..`.
    fn take(&mut self) -> io::Result<Option<DirEntry>> {
        while self.pos < self.buf.len() {
            let record = &self.buf[self.pos..];
            let name_at = offset_of!(libc::dirent64, d_name);
            let reclen_at = offset_of!(libc::dirent64, d_reclen);
            let len = record
                .get(reclen_at..reclen_at + 2)
                .map(|bytes| usize::from(u16::from_ne_bytes([bytes[0], bytes[1]])))
                .filter(|&len| len > name_at && len <= record.len())
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "a malformed directory entry")
                })?;
            let record = &record[..len];
            self.pos += len;
            let ino_at = offset_of!(libc::dirent64, d_ino);
            let mut ino = [0; 8];
            ino.copy_from_slice(&record[ino_at..ino_at + 8]);
            let name = &record[name_at..];
            let name = &name[..name
                .iter()
                .position(|&byte| byte == 0)
                .unwrap_or(name.len())];
            if name == b"." || name == b".." {
                continue;
            }
            return Ok(Some(DirEntry {
                dir: Arc::clone(&self.dir),
                name: OsString::from_vec(name.to_vec()),
                ino: u64::from_ne_bytes(ino),
                kind: record[offset_of!(libc::dirent64, d_type)],
            }));
        }
        Ok(None)
    }

    /// Reads the next batch of entries.
    fn fill(&mut self) -> io::Result<()> {
        let fd = self.dir.fd.as_raw_fd();
        self.buf.clear();
        self.pos = 0;
        let read = loop {
            // SAFETY: getdents64 writes at most `capacity` bytes of entries
            // into the buffer, which has room for them, and nothing else.
            let read = unsafe {
                libc::syscall(
                    libc::SYS_getdents64,
                    fd,
                    self.buf.as_mut_ptr(),
                    self.buf.capacity(),
                )
            };
            match usize::try_from(read) {
                Ok(read) => break read,
                Err(_) => {
                    let err = io::Error::last_os_error();
                    if err.kind() != io::ErrorKind::Interrupted {
                        self.state = State::Ended;
                        return Err(err);
                    }
                }
            }
        };
        // SAFETY: the kernel wrote the first `read` bytes, which are within
        // the capacity it was given.
        unsafe { self.buf.set_len(read) };
        self.state = if read == 0 { State::Ended } else { State::Read };
        Ok(())
    }
}

impl fmt::Debug for ReadDir {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadDir")
            .field("path", &self.dir.path)
            .finish_non_exhaustive()
    }
}

/// An entry of a directory, which [`ReadDir`] reads. It keeps the
/// directory open, so that its status is that of the entry in the
/// directory read, though the directory be renamed meanwhile.
pub struct DirEntry {
    dir: Arc<Dir>,
    name: OsString,
    ino: u64,
    /// Its type, as a `DT_*` constant.
    kind: u8,
}

impl DirEntry {
    /// The entry's path: the directory's, as it was given to [`read_dir`],
    /// joined with the entry's name.
    pub fn path(&self) -> PathBuf {
        self.dir.path.join(&self.name)
    }

    /// The entry's name in the directory.
    pub fn file_name(&self) -> OsString {
        self.name.clone()
    }

    /// The entry's inode number.
    pub fn ino(&self) -> u64 {
        self.ino
    }

    /// The entry's type, which the directory says, or, if its file system
    /// does not, the entry's status; a symbolic link is a symbolic link.
    ///
    /// # Errors
    ///
    /// As for [`metadata`](DirEntry::metadata), when the type is not known.
    pub async fn file_type(&self) -> io::Result<FileType> {
        match file_type(self.kind) {
            Some(kind) => Ok(kind),
            None => Ok(self.metadata().await?.file_type()),
        }
    }

    /// The entry's status, as `statx(2)` reports it, of a symbolic link
    /// itself rather than what it points to.
    ///
    /// # Errors
    ///
    /// What `statx(2)` reports, such as `NotFound` if the entry was removed.
    pub async fn metadata(&self) -> io::Result<Metadata> {
        let stat = Statx::at(self.dir.fd.as_raw_fd(), &self.name)?
            .flags(libc::AT_SYMLINK_NOFOLLOW)
            .mask(METADATA);
        let stat = crate::driver().op(stat).await?;
        Ok(Metadata { stat: *stat })
    }
}

impl fmt::Debug for DirEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("DirEntry").field(&self.path()).finish()
    }
}

/// The type a directory entry's `DT_*` constant says, if it says one.
fn file_type(kind: u8) -> Option<FileType> {
    let mode = match kind {
        libc::DT_REG => libc::S_IFREG,
        libc::DT_DIR => libc::S_IFDIR,
        libc::DT_LNK => libc::S_IFLNK,
        libc::DT_BLK => libc::S_IFBLK,
        libc::DT_CHR => libc::S_IFCHR,
        libc::DT_FIFO => libc::S_IFIFO,
        libc::DT_SOCK => libc::S_IFSOCK,
        _ => return None,
    };
    Some(FileType(mode))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_types_map_to_file_types() {
        assert!(file_type(libc::DT_REG).unwrap().is_file());
        assert!(file_type(libc::DT_DIR).unwrap().is_dir());
        assert!(file_type(libc::DT_LNK).unwrap().is_symlink());
        assert!(file_type(libc::DT_BLK).unwrap().is_block_device());
        assert!(file_type(libc::DT_CHR).unwrap().is_char_device());
        assert!(file_type(libc::DT_FIFO).unwrap().is_fifo());
        assert!(file_type(libc::DT_SOCK).unwrap().is_socket());
        assert!(file_type(libc::DT_UNKNOWN).is_none());
    }
}
