//! The Windows functions and structures the engine uses, declared by hand
//! from the Windows SDK's headers (`minwinbase.h`, `winbase.h`,
//! `ioapiset.h`, `fileapi.h`, `namedpipeapi.h`, `handleapi.h`,
//! `synchapi.h`, `winternl.h`, `winsock2.h`, `mswsock.h`), as `ruio-sys`
//! declares io_uring's; and the wait completion packets of `ntdll`, which
//! the headers do not declare.

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

/// `STATUS_PENDING`: the status in the `OVERLAPPED` of an operation that
/// has not completed, which the calls that start one set before they ask
/// the system for it.
pub(crate) const STATUS_PENDING: u32 = 0x0000_0103;

/// `STATUS_SUCCESS`.
pub(crate) const STATUS_SUCCESS: i32 = 0;

/// `CREATE_WAITABLE_TIMER_HIGH_RESOLUTION`: a timer that expires when it
/// is due rather than at the next tick of the system's timer, which
/// Windows 10 has from its version 1803.
pub(crate) const CREATE_WAITABLE_TIMER_HIGH_RESOLUTION: u32 = 0x0000_0002;

/// `TIMER_ALL_ACCESS`: every right to a timer.
pub(crate) const TIMER_ALL_ACCESS: u32 = 0x001F_0003;

/// `GENERIC_ALL`: every right to an object.
pub(crate) const GENERIC_ALL: u32 = 0x1000_0000;

/// `FILE_FLAG_OVERLAPPED`: a handle opened for overlapped I/O, whose
/// operations may complete after the calls that start them return.
pub(crate) const FILE_FLAG_OVERLAPPED: u32 = 0x4000_0000;

/// `FILE_SKIP_COMPLETION_PORT_ON_SUCCESS`: no completion is queued to the
/// port for an operation on the handle that succeeds during the call that
/// starts it.
pub(crate) const FILE_SKIP_COMPLETION_PORT_ON_SUCCESS: u8 = 0x1;

/// `FILE_SKIP_SET_EVENT_ON_HANDLE`: the handle is not signaled as its
/// operations complete, which nothing of the engine's waits for.
pub(crate) const FILE_SKIP_SET_EVENT_ON_HANDLE: u8 = 0x2;

/// `FILE_TYPE_DISK`: a file on a disk.
pub(crate) const FILE_TYPE_DISK: u32 = 0x0001;

/// `PIPE_ACCESS_DUPLEX`: a pipe that both ends read and write.
pub(crate) const PIPE_ACCESS_DUPLEX: u32 = 0x0000_0003;

/// `FILE_FLAG_FIRST_PIPE_INSTANCE`: fail if the pipe exists already.
pub(crate) const FILE_FLAG_FIRST_PIPE_INSTANCE: u32 = 0x0008_0000;

/// `PIPE_REJECT_REMOTE_CLIENTS`: a pipe only this machine connects to. With
/// no other mode flag, the pipe carries bytes rather than messages.
pub(crate) const PIPE_REJECT_REMOTE_CLIENTS: u32 = 0x0000_0008;

/// `SOCKET`.
pub(crate) type Socket = usize;

/// `INVALID_SOCKET`, which `WSASocketW` returns on failure.
pub(crate) const INVALID_SOCKET: Socket = usize::MAX;

/// `WSA_IO_PENDING`: Winsock took an operation, which completes later.
pub(crate) const WSA_IO_PENDING: i32 = 997;

/// `WSAEMSGSIZE`: a datagram was longer than the buffer it was received
/// into, which holds its start.
pub(crate) const WSAEMSGSIZE: i32 = 10040;

/// `WSAESHUTDOWN`: the half of the socket that an operation needs was shut
/// down.
pub(crate) const WSAESHUTDOWN: i32 = 10058;

/// `AF_INET`: the address family of IPv4.
pub(crate) const AF_INET: u16 = 2;

/// `AF_INET6`: the address family of IPv6.
pub(crate) const AF_INET6: u16 = 23;

/// `SOCK_STREAM`: a stream socket.
pub(crate) const SOCK_STREAM: i32 = 1;

/// `IPPROTO_TCP`.
pub(crate) const IPPROTO_TCP: i32 = 6;

/// `WSA_FLAG_OVERLAPPED`: a socket for overlapped I/O.
pub(crate) const WSA_FLAG_OVERLAPPED: u32 = 0x01;

/// `WSA_FLAG_NO_HANDLE_INHERIT`: a socket that child processes do not
/// inherit.
pub(crate) const WSA_FLAG_NO_HANDLE_INHERIT: u32 = 0x80;

/// `SOL_SOCKET`: the level of the options of sockets themselves.
pub(crate) const SOL_SOCKET: i32 = 0xffff;

/// `SO_UPDATE_ACCEPT_CONTEXT`: makes a socket that `AcceptEx` accepted a
/// connection on a connected socket in full, given the listener's socket.
pub(crate) const SO_UPDATE_ACCEPT_CONTEXT: i32 = 0x700B;

/// `SO_UPDATE_CONNECT_CONTEXT`: the same for a socket that `ConnectEx`
/// connected.
pub(crate) const SO_UPDATE_CONNECT_CONTEXT: i32 = 0x7010;

/// `SO_LINGER`: whether closing a socket waits for what it has left to
/// send, and for how long; its value is a [`Linger`].
pub(crate) const SO_LINGER: i32 = 0x0080;

/// `SO_PROTOCOL_INFOW`: what the socket's provider is, as a
/// [`ProtocolInfo`].
pub(crate) const SO_PROTOCOL_INFOW: i32 = 0x2005;

/// `XP1_IFS_HANDLES`: a provider whose sockets are handles of the system's
/// own, with no layer between that would have to see their completions.
pub(crate) const XP1_IFS_HANDLES: u32 = 0x0002_0000;

/// `SIO_GET_EXTENSION_FUNCTION_POINTER`: the `WSAIoctl` that asks a
/// socket's provider for the function a `GUID` names.
pub(crate) const SIO_GET_EXTENSION_FUNCTION_POINTER: u32 = 0xC800_0006;

/// `SIO_UDP_CONNRESET`: the `WSAIoctl` that sets whether a UDP socket's
/// receives fail with a reset once a datagram it sent found nobody at its
/// port.
pub(crate) const SIO_UDP_CONNRESET: u32 = 0x9800_000C;

/// `WSAID_ACCEPTEX`: names `AcceptEx`.
pub(crate) const WSAID_ACCEPTEX: Guid = Guid {
    data1: 0xb536_7df1,
    data2: 0xcbac,
    data3: 0x11cf,
    data4: [0x95, 0xca, 0x00, 0x80, 0x5f, 0x48, 0xa1, 0x92],
};

/// `WSAID_CONNECTEX`: names `ConnectEx`.
pub(crate) const WSAID_CONNECTEX: Guid = Guid {
    data1: 0x25a2_07b9,
    data2: 0xddf3,
    data3: 0x4660,
    data4: [0x8e, 0xe9, 0x76, 0xe5, 0x8c, 0x74, 0x06, 0x3e],
};

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

/// `GUID`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct Guid {
    pub(crate) data1: u32,
    pub(crate) data2: u16,
    pub(crate) data3: u16,
    pub(crate) data4: [u8; 8],
}

/// `WSABUF`: a buffer that a socket's operation reads into or sends.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct WsaBuf {
    /// `len`: how many bytes it has.
    pub(crate) len: u32,
    /// `buf`: where they are.
    pub(crate) buf: *mut u8,
}

/// `LINGER`: the value of `SO_LINGER`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Linger {
    /// `l_onoff`: whether closing the socket waits, zero if not.
    pub(crate) onoff: u16,
    /// `l_linger`: for how many seconds it does.
    pub(crate) linger: u16,
}

/// `WSAPROTOCOL_INFOW`: what a socket's provider is and can do, of which
/// the engine reads the first of its flags.
#[repr(C)]
pub(crate) struct ProtocolInfo {
    /// `dwServiceFlags1`.
    pub(crate) service_flags1: u32,
    /// The rest of it.
    rest: [u8; 624],
}

impl ProtocolInfo {
    /// One for the system to fill in.
    pub(crate) const EMPTY: Self = Self {
        service_flags1: 0,
        rest: [0; 624],
    };
}

const _: () = assert!(size_of::<ProtocolInfo>() == 628);

/// `LPFN_ACCEPTEX`: starts an accept on `listener` of a connection onto
/// `accepted`, a socket made for it, and writes the connection's local and
/// remote addresses to `output`, which needs `local_len` and `remote_len`
/// bytes for them, each 16 more than the largest address. True if it
/// completed at once, false with `WSA_IO_PENDING` if it completes later.
pub(crate) type AcceptEx = unsafe extern "system" fn(
    listener: Socket,
    accepted: Socket,
    output: *mut c_void,
    receive_len: u32,
    local_len: u32,
    remote_len: u32,
    received: *mut u32,
    overlapped: *mut Overlapped,
) -> Bool;

/// `LPFN_CONNECTEX`: starts connecting `socket`, which must be bound, to
/// `name`, sending `send` with the connection if it is not null. True if it
/// completed at once, false with `WSA_IO_PENDING` if it completes later.
pub(crate) type ConnectEx = unsafe extern "system" fn(
    socket: Socket,
    name: *const u8,
    name_len: i32,
    send: *const c_void,
    send_len: u32,
    sent: *mut u32,
    overlapped: *mut Overlapped,
) -> Bool;

#[cfg(target_pointer_width = "64")]
const _: () = assert!(
    size_of::<Overlapped>() == 32
        && size_of::<OverlappedEntry>() == 32
        && size_of::<WsaBuf>() == 16
);
#[cfg(target_pointer_width = "32")]
const _: () = assert!(
    size_of::<Overlapped>() == 20 && size_of::<OverlappedEntry>() == 16 && size_of::<WsaBuf>() == 8
);
const _: () = assert!(size_of::<Guid>() == 16);

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

    /// Sets how the completions of `handle`'s operations are reported, as
    /// `modes` say: `FILE_SKIP_COMPLETION_PORT_ON_SUCCESS`,
    /// `FILE_SKIP_SET_EVENT_ON_HANDLE`, or both. False on failure.
    pub(crate) fn SetFileCompletionNotificationModes(handle: Handle, modes: u8) -> Bool;

    /// What kind of file `handle` is: `FILE_TYPE_DISK`, or another.
    pub(crate) fn GetFileType(handle: Handle) -> u32;

    /// Creates a waitable timer with no name, as `flags` say and with the
    /// rights of `access`, or returns null.
    pub(crate) fn CreateWaitableTimerExW(
        attributes: *const c_void,
        name: *const u16,
        flags: u32,
        access: u32,
    ) -> Handle;

    /// Sets `timer` to expire at `due`, in units of 100 ns: a time from now
    /// if negative. A timer that was set is set anew, and is not signaled
    /// meanwhile, so that what waits for it still does.
    pub(crate) fn SetWaitableTimer(
        timer: Handle,
        due: *const i64,
        period: i32,
        completion: *const c_void,
        completion_arg: *const c_void,
        resume: Bool,
    ) -> Bool;

    /// The calling thread's last error.
    pub(crate) safe fn GetLastError() -> u32;
}

#[link(name = "ntdll")]
unsafe extern "system" {
    /// The Windows error that an `NTSTATUS`, as an operation's `OVERLAPPED`
    /// holds, stands for.
    pub(crate) safe fn RtlNtStatusToDosError(status: i32) -> u32;
}

// By name from the library itself, as the SDK's import library need not
// have what its headers do not declare.
#[cfg_attr(
    target_arch = "x86",
    link(name = "ntdll", kind = "raw-dylib", import_name_type = "undecorated")
)]
#[cfg_attr(not(target_arch = "x86"), link(name = "ntdll", kind = "raw-dylib"))]
unsafe extern "system" {
    /// Creates a wait completion packet, which posts a packet to a port
    /// once an object it is associated with is signaled, and writes its
    /// handle to `packet`. An `NTSTATUS`: negative on failure.
    pub(crate) fn NtCreateWaitCompletionPacket(
        packet: *mut Handle,
        access: u32,
        attributes: *const c_void,
    ) -> i32;

    /// Has `packet` post a packet to `port`, under `key` and with
    /// `overlapped`, `status` and `information`, once `target` is signaled:
    /// once, after which it can be associated again. `signaled` is set if
    /// the target was signaled already. An `NTSTATUS`: negative on failure,
    /// as when the packet is associated already, or lies in a port.
    pub(crate) fn NtAssociateWaitCompletionPacket(
        packet: Handle,
        port: Handle,
        target: Handle,
        key: *mut c_void,
        overlapped: *mut c_void,
        status: i32,
        information: usize,
        signaled: *mut u8,
    ) -> i32;

    /// Ends `packet`'s wait for the object it is associated with, and takes
    /// the packet it posted out of its port if `remove_signaled` is not
    /// zero and no turn has taken it yet. An `NTSTATUS`.
    pub(crate) fn NtCancelWaitCompletionPacket(packet: Handle, remove_signaled: u8) -> i32;
}

#[link(name = "ws2_32")]
unsafe extern "system" {
    /// Starts Winsock for the process, at `version`, and describes it in
    /// `data`, a `WSADATA`. Returns zero, or the error.
    pub(crate) fn WSAStartup(version: u16, data: *mut c_void) -> i32;

    /// Makes a socket, or returns `INVALID_SOCKET`.
    pub(crate) fn WSASocketW(
        family: i32,
        kind: i32,
        protocol: i32,
        protocol_info: *const c_void,
        group: u32,
        flags: u32,
    ) -> Socket;

    /// Binds `socket` to the address `name`. Zero on success.
    pub(crate) fn bind(socket: Socket, name: *const u8, name_len: i32) -> i32;

    /// Sets an option of `socket`. Zero on success.
    pub(crate) fn setsockopt(
        socket: Socket,
        level: i32,
        name: i32,
        value: *const u8,
        value_len: i32,
    ) -> i32;

    /// Reads an option of `socket` into `value`, which has room for
    /// `value_len` bytes, and sets `value_len` to how many it wrote. Zero
    /// on success.
    pub(crate) fn getsockopt(
        socket: Socket,
        level: i32,
        name: i32,
        value: *mut u8,
        value_len: *mut i32,
    ) -> i32;

    /// Controls `socket`: here, asks its provider for a function. Zero on
    /// success.
    pub(crate) fn WSAIoctl(
        socket: Socket,
        code: u32,
        input: *const c_void,
        input_len: u32,
        output: *mut c_void,
        output_len: u32,
        returned: *mut u32,
        overlapped: *mut Overlapped,
        completion: *const c_void,
    ) -> i32;

    /// Starts a receive on `socket` into `buffers`, which it copies before
    /// it returns. Zero if it completed at once, or `SOCKET_ERROR` with
    /// `WSA_IO_PENDING` if it completes later; either way its completion
    /// goes to the socket's port.
    pub(crate) fn WSARecv(
        socket: Socket,
        buffers: *mut WsaBuf,
        count: u32,
        received: *mut u32,
        flags: *mut u32,
        overlapped: *mut Overlapped,
        completion: *const c_void,
    ) -> i32;

    /// Starts a send of `buffers` on `socket`, as `WSARecv` starts a
    /// receive.
    pub(crate) fn WSASend(
        socket: Socket,
        buffers: *const WsaBuf,
        count: u32,
        sent: *mut u32,
        flags: u32,
        overlapped: *mut Overlapped,
        completion: *const c_void,
    ) -> i32;

    /// Starts a send of `buffers` on `socket` to the address `to`, as
    /// `WSASend` starts one to the socket's peer.
    pub(crate) fn WSASendTo(
        socket: Socket,
        buffers: *const WsaBuf,
        count: u32,
        sent: *mut u32,
        flags: u32,
        to: *const u8,
        to_len: i32,
        overlapped: *mut Overlapped,
        completion: *const c_void,
    ) -> i32;

    /// Starts a receive on `socket` into `buffers`, as `WSARecv` does, and
    /// writes the sender's address to `from` and its length to `from_len`,
    /// both of which it uses until the operation completes.
    pub(crate) fn WSARecvFrom(
        socket: Socket,
        buffers: *mut WsaBuf,
        count: u32,
        received: *mut u32,
        flags: *mut u32,
        from: *mut u8,
        from_len: *mut i32,
        overlapped: *mut Overlapped,
        completion: *const c_void,
    ) -> i32;

    /// The outcome of the operation on `socket` that `overlapped` names:
    /// true and how many bytes it transferred, or false, with its error as
    /// the thread's last Winsock error.
    pub(crate) fn WSAGetOverlappedResult(
        socket: Socket,
        overlapped: *const Overlapped,
        transferred: *mut u32,
        wait: Bool,
        flags: *mut u32,
    ) -> Bool;

    /// The calling thread's last Winsock error.
    pub(crate) safe fn WSAGetLastError() -> i32;
}
