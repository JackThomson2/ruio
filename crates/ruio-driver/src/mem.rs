//! Memory mapped for the kernel: the ring's queues, and buffer memory.

use std::ffi::c_int;
use std::io;
use std::os::fd::{AsRawFd, BorrowedFd};
use std::ptr::{self, NonNull};

/// A memory mapping, unmapped when dropped.
pub(crate) struct Mapping {
    ptr: NonNull<u8>,
    len: usize,
}

impl Mapping {
    /// Maps `len` bytes of `fd`, from `offset`, shared with the kernel, as
    /// the ring's queues are.
    pub(crate) fn shared(fd: BorrowedFd<'_>, offset: u64, len: usize) -> io::Result<Self> {
        Self::map(
            len,
            libc::MAP_SHARED | libc::MAP_POPULATE,
            fd.as_raw_fd(),
            offset,
        )
    }

    /// Maps `len` bytes of fresh memory, page-aligned and zeroed.
    pub(crate) fn anonymous(len: usize) -> io::Result<Self> {
        Self::map(len, libc::MAP_PRIVATE | libc::MAP_ANONYMOUS, -1, 0)
    }

    fn map(len: usize, flags: c_int, fd: c_int, offset: u64) -> io::Result<Self> {
        // SAFETY: this creates a new mapping and affects no existing memory.
        let ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                flags,
                fd,
                offset as libc::off_t,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let ptr = NonNull::new(ptr.cast()).ok_or_else(|| io::Error::other("mmap returned null"))?;
        Ok(Self { ptr, len })
    }

    pub(crate) fn ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: the mapping came from `mmap` with this length, and its
        // owner drops it only once nothing, the kernel included, uses it.
        unsafe { libc::munmap(self.ptr.as_ptr().cast(), self.len) };
    }
}
