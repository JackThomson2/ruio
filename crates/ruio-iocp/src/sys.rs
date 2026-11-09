//! The Windows functions and structures the engine uses, declared by hand
//! from the Windows SDK's headers (`minwinbase.h`, `ioapiset.h`,
//! `handleapi.h`), as `ruio-sys` declares io_uring's.

use std::ffi::c_void;
use std::ptr;

/// `HANDLE`.
pub(crate) type Handle = *mut c_void;

/// `BOOL`: zero for false.
pub(crate) type Bool = i32;

/// `INVALID_HANDLE_VALUE`, which `CreateIoCompletionPort` takes for no file.
pub(crate) const INVALID_HANDLE_VALUE: Handle = ptr::without_provenance_mut(usize::MAX);

/// `INFINITE`: a wait without a timeout.
pub(crate) const INFINITE: u32 = u32::MAX;

/// `WAIT_TIMEOUT`: the error of a wait that timed out.
pub(crate) const WAIT_TIMEOUT: u32 = 258;

/// `OVERLAPPED_ENTRY`: a completion taken from a port.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct OverlappedEntry {
    /// `lpCompletionKey`: the key of the handle the operation was on, or of
    /// the packet posted.
    pub(crate) completion_key: usize,
    /// `lpOverlapped`: the operation's `OVERLAPPED`, or what the packet
    /// posted carried.
    pub(crate) _overlapped: *mut c_void,
    /// `Internal`: reserved.
    pub(crate) _internal: usize,
    /// `dwNumberOfBytesTransferred`.
    pub(crate) _bytes_transferred: u32,
}

impl OverlappedEntry {
    /// An entry for the port to fill in.
    pub(crate) const EMPTY: Self = Self {
        completion_key: 0,
        _overlapped: ptr::null_mut(),
        _internal: 0,
        _bytes_transferred: 0,
    };
}

#[cfg(target_pointer_width = "64")]
const _: () = assert!(size_of::<OverlappedEntry>() == 32);
#[cfg(target_pointer_width = "32")]
const _: () = assert!(size_of::<OverlappedEntry>() == 16);

#[link(name = "kernel32")]
unsafe extern "system" {
    /// Makes a port, given no file and no port, or associates a file's
    /// handle with a port under a completion key. Null on failure.
    pub(crate) fn CreateIoCompletionPort(
        file: Handle,
        existing_port: Handle,
        completion_key: usize,
        concurrent_threads: u32,
    ) -> Handle;

    /// Takes up to `count` completions from the port into `entries`,
    /// waiting up to `milliseconds` for the first, and says how many in
    /// `removed`. False, with `WAIT_TIMEOUT`, if the wait timed out.
    pub(crate) fn GetQueuedCompletionStatusEx(
        port: Handle,
        entries: *mut OverlappedEntry,
        count: u32,
        removed: *mut u32,
        milliseconds: u32,
        alertable: Bool,
    ) -> Bool;

    /// Posts a packet of the caller's to the port.
    pub(crate) fn PostQueuedCompletionStatus(
        port: Handle,
        bytes_transferred: u32,
        completion_key: usize,
        overlapped: *mut c_void,
    ) -> Bool;

    /// Closes a handle.
    pub(crate) fn CloseHandle(handle: Handle) -> Bool;

    /// The calling thread's last error.
    pub(crate) safe fn GetLastError() -> u32;
}
