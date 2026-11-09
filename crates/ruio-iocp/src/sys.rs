//! The Windows functions and structures the engine uses, declared by hand
//! from the Windows SDK's headers (`minwinbase.h`, `ioapiset.h`,
//! `fileapi.h`, `namedpipeapi.h`, `handleapi.h`, `winternl.h`), as
//! `ruio-sys` declares io_uring's.

use std::ffi::c_void;
use std::ptr;

/// `HANDLE`.
pub(crate) type Handle = *mut c_void;

/// `BOOL`: zero for false.
pub(crate) type Bool = i32;

/// `INVALID_HANDLE_VALUE`, which `CreateIoCompletionPort` takes for no
/// file, and `CreateNamedPipeW` returns on failure.
pub(crate) const INVALID_HANDLE_VALUE: Handle = ptr::without_provenance_mut(usize::MAX);

/// `INFINITE`: a wait without a timeout.
pub(crate) const INFINITE: u32 = u32::MAX;

/// `WAIT_TIMEOUT`: the error of a wait that timed out.
pub(crate) const WAIT_TIMEOUT: u32 = 258;

/// `ERROR_HANDLE_EOF`: a read at or past the end of a file.
pub(crate) const ERROR_HANDLE_EOF: u32 = 38;

/// `ERROR_BROKEN_PIPE`: a read of a pipe whose other end has closed.
pub(crate) const ERROR_BROKEN_PIPE: u32 = 109;

/// `ERROR_IO_PENDING`: the system took an operation, which completes later.
pub(crate) const ERROR_IO_PENDING: u32 = 997;

/// `FILE_FLAG_OVERLAPPED`: a handle opened for overlapped I/O, whose
/// operations may complete after the calls that start them return.
pub(crate) const FILE_FLAG_OVERLAPPED: u32 = 0x4000_0000;

/// `PIPE_ACCESS_DUPLEX`: a pipe that both ends read and write.
pub(crate) const PIPE_ACCESS_DUPLEX: u32 = 0x0000_0003;

/// `FILE_FLAG_FIRST_PIPE_INSTANCE`: fail if the pipe exists already.
pub(crate) const FILE_FLAG_FIRST_PIPE_INSTANCE: u32 = 0x0008_0000;

/// `PIPE_REJECT_REMOTE_CLIENTS`: a pipe only this machine connects to. With
/// no other mode flag, the pipe carries bytes rather than messages.
pub(crate) const PIPE_REJECT_REMOTE_CLIENTS: u32 = 0x0000_0008;

/// `OVERLAPPED`: an operation's state, which the system fills in as it
/// runs, and where in a file it reads or writes.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct Overlapped {
    /// `Internal`: the operation's status, an `NTSTATUS`, once it completes.
    pub(crate) internal: usize,
    /// `InternalHigh`: how many bytes it transferred.
    pub(crate) internal_high: usize,
    /// `Offset`: the low half of where in a file it reads or writes.
    pub(crate) offset: u32,
    /// `OffsetHigh`: the high half.
    pub(crate) offset_high: u32,
    /// `hEvent`: an event to signal as well, which the engine never asks
    /// for.
    pub(crate) event: Handle,
}

/// `OVERLAPPED_ENTRY`: a completion taken from a port.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct OverlappedEntry {
    /// `lpCompletionKey`: the key of the handle the operation was on, or of
    /// the packet posted.
    pub(crate) completion_key: usize,
    /// `lpOverlapped`: the operation's `OVERLAPPED`, or what the packet
    /// posted carried.
    pub(crate) overlapped: *mut Overlapped,
    /// `Internal`: reserved.
    pub(crate) internal: usize,
    /// `dwNumberOfBytesTransferred`.
    pub(crate) bytes_transferred: u32,
}

impl OverlappedEntry {
    /// An entry for the port to fill in.
    pub(crate) const EMPTY: Self = Self {
        completion_key: 0,
        overlapped: ptr::null_mut(),
        internal: 0,
        bytes_transferred: 0,
    };
}

#[cfg(target_pointer_width = "64")]
const _: () = assert!(size_of::<Overlapped>() == 32 && size_of::<OverlappedEntry>() == 32);
#[cfg(target_pointer_width = "32")]
const _: () = assert!(size_of::<Overlapped>() == 20 && size_of::<OverlappedEntry>() == 16);

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
        overlapped: *mut Overlapped,
    ) -> Bool;

    /// Starts a read of `file` into `buffer`. True if it completed at once,
    /// false with `ERROR_IO_PENDING` if it completes later; either way its
    /// completion goes to the file's port. False with another error if it
    /// failed at once, when nothing goes to the port.
    pub(crate) fn ReadFile(
        file: Handle,
        buffer: *mut c_void,
        to_read: u32,
        read: *mut u32,
        overlapped: *mut Overlapped,
    ) -> Bool;

    /// Starts a write of `buffer` to `file`, as `ReadFile` starts a read.
    pub(crate) fn WriteFile(
        file: Handle,
        buffer: *const c_void,
        to_write: u32,
        written: *mut u32,
        overlapped: *mut Overlapped,
    ) -> Bool;

    /// Cancels the operation on `file` that `overlapped` names, whose
    /// completion still goes to the port, with `STATUS_CANCELLED` unless it
    /// completed first. False, with `ERROR_NOT_FOUND`, if it has completed.
    pub(crate) fn CancelIoEx(file: Handle, overlapped: *mut Overlapped) -> Bool;

    /// Creates a named pipe's instance, and returns its server's end, or
    /// `INVALID_HANDLE_VALUE` on failure.
    pub(crate) fn CreateNamedPipeW(
        name: *const u16,
        open_mode: u32,
        pipe_mode: u32,
        max_instances: u32,
        out_buffer_size: u32,
        in_buffer_size: u32,
        default_timeout: u32,
        security_attributes: *const c_void,
    ) -> Handle;

    /// Closes a handle.
    pub(crate) fn CloseHandle(handle: Handle) -> Bool;

    /// The calling thread's last error.
    pub(crate) safe fn GetLastError() -> u32;
}

#[link(name = "ntdll")]
unsafe extern "system" {
    /// The Windows error that an `NTSTATUS`, as an operation's `OVERLAPPED`
    /// holds, stands for.
    pub(crate) safe fn RtlNtStatusToDosError(status: i32) -> u32;
}
