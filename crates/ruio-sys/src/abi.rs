//! Structures and constants from `include/uapi/linux/io_uring.h`.
//!
//! Names match the kernel's so that the header, the man pages and this crate
//! can be read side by side.

#![allow(non_camel_case_types)]

/// Defines a group of constants and a table naming each of them.
///
/// The table lists `(name, value)` pairs in definition order. It gives
/// diagnostics readable names and lets the ABI test check every constant
/// against the kernel header.
macro_rules! consts {
    (
        $(#[$table_meta:meta])*
        pub const $table:ident: [$ty:ty] = [
            $( $(#[$meta:meta])* $name:ident = $value:expr, )*
        ];
    ) => {
        $( $(#[$meta])* pub const $name: $ty = $value; )*

        $(#[$table_meta])*
        pub const $table: &[(&str, $ty)] = &[ $( (stringify!($name), $name), )* ];
    };
}

/// A submission queue entry (SQE).
///
/// The kernel declares several of these fields as unions. Each union is
/// represented by one field of the union's full width, named after its most
/// common member; the field's documentation lists the other members. Members
/// narrower than their union (`cmd_op`, `level` and `optname`, `addr_len`,
/// `write_stream`) occupy its leading bytes.
///
/// With [`IORING_SETUP_SQE128`] each entry is 64 bytes longer, and the command
/// data of [`IORING_OP_URING_CMD`] starts at `addr3`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct io_uring_sqe {
    /// The operation, one of the `IORING_OP_*` constants.
    pub opcode: u8,
    /// `IOSQE_*` flags.
    pub flags: u8,
    /// I/O priority, or `IORING_RECVSEND_*` and `IORING_ACCEPT_*` flags.
    pub ioprio: u16,
    /// File descriptor, or registered file index with [`IOSQE_FIXED_FILE`].
    pub fd: i32,
    /// File offset. Also `addr2`, and `cmd_op` in the first four bytes.
    pub off: u64,
    /// Buffer or iovec address. Also `splice_off_in`, and `level` and
    /// `optname` as two `u32`s.
    pub addr: u64,
    /// Buffer length or iovec count.
    pub len: u32,
    /// Per-opcode flags: `rw_flags`, `fsync_flags`, `poll32_events`,
    /// `timeout_flags`, `accept_flags`, `cancel_flags`, `open_flags`,
    /// `msg_flags`, `msg_ring_flags` and the other `*_flags` members of the
    /// kernel's union.
    pub op_flags: u32,
    /// Returned unchanged in each completion of the request.
    pub user_data: u64,
    /// Registered buffer index, or provided-buffer group (`buf_group`) with
    /// [`IOSQE_BUFFER_SELECT`].
    pub buf_index: u16,
    /// Credentials registered with [`IORING_REGISTER_PERSONALITY`], or 0.
    pub personality: u16,
    /// Registered file slot for operations that create files. Also
    /// `splice_fd_in`, `zcrx_ifq_idx` and `optlen`, and `addr_len` or
    /// `write_stream` in the first bytes.
    pub file_index: u32,
    /// Third address. Also `attr_ptr` and `optval`.
    pub addr3: u64,
    /// Padding. Also `attr_type_mask`.
    pub __pad2: [u64; 1],
}

/// A completion queue entry (CQE).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct io_uring_cqe {
    /// The `user_data` of the request this completion belongs to.
    pub user_data: u64,
    /// What the equivalent system call returns, or a negated errno.
    pub res: i32,
    /// `IORING_CQE_F_*` flags. With [`IORING_CQE_F_BUFFER`], the bits from
    /// [`IORING_CQE_BUFFER_SHIFT`] up hold the provided buffer's ID.
    pub flags: u32,
    /// Another 16 bytes with [`IORING_SETUP_CQE32`].
    pub big_cqe: [u64; 0],
}

/// Where the submission ring's fields sit in its mapping.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct io_sqring_offsets {
    /// Consumer index, advanced by the kernel.
    pub head: u32,
    /// Producer index, advanced by the application.
    pub tail: u32,
    /// Mask that turns an index into a slot.
    pub ring_mask: u32,
    /// Number of entries.
    pub ring_entries: u32,
    /// `IORING_SQ_*` flags.
    pub flags: u32,
    /// Number of invalid entries the kernel skipped.
    pub dropped: u32,
    /// Index array; absent with [`IORING_SETUP_NO_SQARRAY`].
    pub array: u32,
    /// Reserved.
    pub resv1: u32,
    /// Ring memory supplied by the application with [`IORING_SETUP_NO_MMAP`].
    pub user_addr: u64,
}

/// Where the completion ring's fields sit in its mapping.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct io_cqring_offsets {
    /// Consumer index, advanced by the application.
    pub head: u32,
    /// Producer index, advanced by the kernel.
    pub tail: u32,
    /// Mask that turns an index into a slot.
    pub ring_mask: u32,
    /// Number of entries.
    pub ring_entries: u32,
    /// Number of completions lost because the ring was full.
    pub overflow: u32,
    /// The array of completion entries.
    pub cqes: u32,
    /// `IORING_CQ_*` flags.
    pub flags: u32,
    /// Reserved.
    pub resv1: u32,
    /// Ring memory supplied by the application with [`IORING_SETUP_NO_MMAP`].
    pub user_addr: u64,
}

/// Parameters of `io_uring_setup(2)`, filled in further by the kernel.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct io_uring_params {
    /// Submission queue size, set by the kernel.
    pub sq_entries: u32,
    /// Completion queue size. Set by the kernel, or by the application with
    /// [`IORING_SETUP_CQSIZE`].
    pub cq_entries: u32,
    /// `IORING_SETUP_*` flags.
    pub flags: u32,
    /// CPU for the submission polling thread, with [`IORING_SETUP_SQ_AFF`].
    pub sq_thread_cpu: u32,
    /// Milliseconds the submission polling thread stays awake while idle.
    pub sq_thread_idle: u32,
    /// `IORING_FEAT_*` flags, set by the kernel.
    pub features: u32,
    /// Ring whose async workers to share, with [`IORING_SETUP_ATTACH_WQ`].
    pub wq_fd: u32,
    /// Reserved; must be zero.
    pub resv: [u32; 3],
    /// Submission ring layout, set by the kernel.
    pub sq_off: io_sqring_offsets,
    /// Completion ring layout, set by the kernel.
    pub cq_off: io_cqring_offsets,
}

/// One opcode's entry in an [`io_uring_probe`].
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct io_uring_probe_op {
    /// The opcode.
    pub op: u8,
    /// Reserved.
    pub resv: u8,
    /// [`IO_URING_OP_SUPPORTED`] if the kernel supports the opcode.
    pub flags: u16,
    /// Reserved.
    pub resv2: u32,
}

/// The result of [`IORING_REGISTER_PROBE`].
///
/// The kernel writes one [`io_uring_probe_op`] per opcode after the header;
/// the caller provides the room for them. The buffer must be zeroed.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct io_uring_probe {
    /// The highest opcode the kernel knows, supported or not.
    pub last_op: u8,
    /// Number of entries written to `ops`.
    pub ops_len: u8,
    /// Reserved.
    pub resv: u16,
    /// Reserved.
    pub resv2: [u32; 3],
    /// The per-opcode entries.
    pub ops: [io_uring_probe_op; 0],
}

/// Extended arguments of `io_uring_enter(2)`, with [`IORING_ENTER_EXT_ARG`].
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct io_uring_getevents_arg {
    /// Address of a signal mask to install while waiting, or 0.
    pub sigmask: u64,
    /// Size of the signal mask.
    pub sigmask_sz: u32,
    /// Microseconds to wait for the requested number of completions before
    /// settling for fewer, with [`IORING_FEAT_MIN_TIMEOUT`].
    pub min_wait_usec: u32,
    /// Address of a [`__kernel_timespec`] bounding the wait, or 0.
    pub ts: u64,
}

/// One entry of a registered resource table to set or clear.
///
/// With [`IORING_REGISTER_RING_FDS`], `data` is a ring's file descriptor and
/// `offset` the slot to register it in; `u32::MAX` picks a free slot, which
/// the kernel writes back into `offset`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct io_uring_rsrc_update {
    /// Slot in the table.
    pub offset: u32,
    /// Reserved; must be zero.
    pub resv: u32,
    /// The resource, such as a file descriptor.
    pub data: u64,
}

/// The kernel's 64-bit `timespec`, from `linux/time_types.h`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct __kernel_timespec {
    /// Seconds.
    pub tv_sec: i64,
    /// Nanoseconds.
    pub tv_nsec: i64,
}

const _: () = {
    assert!(size_of::<io_uring_sqe>() == 64);
    assert!(size_of::<io_uring_cqe>() == 16);
    assert!(size_of::<io_sqring_offsets>() == 40);
    assert!(size_of::<io_cqring_offsets>() == 40);
    assert!(size_of::<io_uring_params>() == 120);
    assert!(size_of::<io_uring_probe_op>() == 8);
    assert!(size_of::<io_uring_probe>() == 16);
    assert!(size_of::<io_uring_getevents_arg>() == 24);
    assert!(size_of::<io_uring_rsrc_update>() == 16);
    assert!(size_of::<__kernel_timespec>() == 16);
};

consts! {
    /// Every `IORING_SETUP_*` flag.
    pub const SETUP_FLAGS: [u32] = [
        /// Busy-poll for completions instead of waiting for interrupts. Only
        /// for `O_DIRECT` files on devices that support polling.
        IORING_SETUP_IOPOLL = 1 << 0,
        /// Have a kernel thread poll the submission queue, so submitting needs
        /// no system call while the thread is awake.
        IORING_SETUP_SQPOLL = 1 << 1,
        /// Pin the [`IORING_SETUP_SQPOLL`] thread to
        /// [`io_uring_params::sq_thread_cpu`].
        IORING_SETUP_SQ_AFF = 1 << 2,
        /// Size the completion queue from [`io_uring_params::cq_entries`].
        IORING_SETUP_CQSIZE = 1 << 3,
        /// Clamp ring sizes above the maximum instead of failing.
        IORING_SETUP_CLAMP = 1 << 4,
        /// Share the async workers of the ring in [`io_uring_params::wq_fd`].
        IORING_SETUP_ATTACH_WQ = 1 << 5,
        /// Create the ring disabled; enable it with
        /// [`IORING_REGISTER_ENABLE_RINGS`].
        IORING_SETUP_R_DISABLED = 1 << 6,
        /// Keep submitting a batch after one of its entries fails to submit.
        IORING_SETUP_SUBMIT_ALL = 1 << 7,
        /// Run completion work when the task next enters the kernel, instead
        /// of interrupting it.
        IORING_SETUP_COOP_TASKRUN = 1 << 8,
        /// Set [`IORING_SQ_TASKRUN`] while completion work is pending. Requires
        /// [`IORING_SETUP_COOP_TASKRUN`] or [`IORING_SETUP_DEFER_TASKRUN`].
        IORING_SETUP_TASKRUN_FLAG = 1 << 9,
        /// Submission queue entries are 128 bytes.
        IORING_SETUP_SQE128 = 1 << 10,
        /// Completion queue entries are 32 bytes.
        IORING_SETUP_CQE32 = 1 << 11,
        /// Only one task submits to the ring.
        IORING_SETUP_SINGLE_ISSUER = 1 << 12,
        /// Defer completion work until the task waits for completions.
        /// Requires [`IORING_SETUP_SINGLE_ISSUER`].
        IORING_SETUP_DEFER_TASKRUN = 1 << 13,
        /// The application provides the ring memory instead of mapping it from
        /// the kernel.
        IORING_SETUP_NO_MMAP = 1 << 14,
        /// Register the ring with itself and return the registered index
        /// instead of a file descriptor.
        IORING_SETUP_REGISTERED_FD_ONLY = 1 << 15,
        /// Consume submission queue entries in ring order, without the index
        /// array.
        IORING_SETUP_NO_SQARRAY = 1 << 16,
        /// Sleep for part of the expected completion time before polling.
        /// Requires [`IORING_SETUP_IOPOLL`].
        IORING_SETUP_HYBRID_IOPOLL = 1 << 17,
        /// Allow both 16 and 32 byte completion entries; 32 byte ones carry
        /// [`IORING_CQE_F_32`].
        IORING_SETUP_CQE_MIXED = 1 << 18,
        /// Allow both 64 and 128 byte submission entries; 128 byte ones use
        /// 128 byte opcodes such as [`IORING_OP_NOP128`].
        IORING_SETUP_SQE_MIXED = 1 << 19,
        /// Take submissions from the start of the queue on every submit call.
        /// Requires [`IORING_SETUP_NO_SQARRAY`]; incompatible with
        /// [`IORING_SETUP_SQPOLL`].
        IORING_SETUP_SQ_REWIND = 1 << 20,
    ];
}

consts! {
    /// Every `IORING_FEAT_*` flag.
    pub const FEATURES: [u32] = [
        /// The submission and completion rings share one mapping.
        IORING_FEAT_SINGLE_MMAP = 1 << 0,
        /// Completions are never dropped; overflow is kept in the kernel.
        IORING_FEAT_NODROP = 1 << 1,
        /// Data needed for async execution has been consumed once the entry is
        /// submitted.
        IORING_FEAT_SUBMIT_STABLE = 1 << 2,
        /// Reads and writes at offset -1 use and advance the file position.
        IORING_FEAT_RW_CUR_POS = 1 << 3,
        /// Requests run with the credentials of the task that submitted them.
        IORING_FEAT_CUR_PERSONALITY = 1 << 4,
        /// Requests on files that are not ready wait on internal polling
        /// instead of occupying an async worker.
        IORING_FEAT_FAST_POLL = 1 << 5,
        /// [`IORING_OP_POLL_ADD`] takes the full 32-bit event mask.
        IORING_FEAT_POLL_32BITS = 1 << 6,
        /// [`IORING_SETUP_SQPOLL`] rings can use unregistered files.
        IORING_FEAT_SQPOLL_NONFIXED = 1 << 7,
        /// `io_uring_enter(2)` takes an [`io_uring_getevents_arg`] with
        /// [`IORING_ENTER_EXT_ARG`].
        IORING_FEAT_EXT_ARG = 1 << 8,
        /// Async workers are threads of the submitting process.
        IORING_FEAT_NATIVE_WORKERS = 1 << 9,
        /// Registered files and buffers can carry tags, reported when they are
        /// released.
        IORING_FEAT_RSRC_TAGS = 1 << 10,
        /// [`IOSQE_CQE_SKIP_SUCCESS`] is supported.
        IORING_FEAT_CQE_SKIP = 1 << 11,
        /// Files of linked requests are looked up when each request starts.
        IORING_FEAT_LINKED_FILE = 1 << 12,
        /// `io_uring_register(2)` takes a registered ring index with
        /// [`IORING_REGISTER_USE_REGISTERED_RING`].
        IORING_FEAT_REG_REG_RING = 1 << 13,
        /// Sends and receives take bundles of provided buffers with
        /// [`IORING_RECVSEND_BUNDLE`].
        IORING_FEAT_RECVSEND_BUNDLE = 1 << 14,
        /// [`io_uring_getevents_arg::min_wait_usec`] is honoured.
        IORING_FEAT_MIN_TIMEOUT = 1 << 15,
        /// Reads and writes take attributes (`attr_ptr`, `attr_type_mask`).
        IORING_FEAT_RW_ATTR = 1 << 16,
        /// Waits can be left out of iowait accounting with
        /// [`IORING_ENTER_NO_IOWAIT`].
        IORING_FEAT_NO_IOWAIT = 1 << 17,
    ];
}

consts! {
    /// Every `IORING_OP_*` opcode.
    pub const OPCODES: [u8] = [
        /// Does nothing.
        IORING_OP_NOP = 0,
        /// `preadv2(2)`.
        IORING_OP_READV = 1,
        /// `pwritev2(2)`.
        IORING_OP_WRITEV = 2,
        /// `fsync(2)`, or `fdatasync(2)` with [`IORING_FSYNC_DATASYNC`].
        IORING_OP_FSYNC = 3,
        /// Read into a registered buffer.
        IORING_OP_READ_FIXED = 4,
        /// Write from a registered buffer.
        IORING_OP_WRITE_FIXED = 5,
        /// Wait for poll events on a file.
        IORING_OP_POLL_ADD = 6,
        /// Cancel or update an [`IORING_OP_POLL_ADD`] request.
        IORING_OP_POLL_REMOVE = 7,
        /// `sync_file_range(2)`.
        IORING_OP_SYNC_FILE_RANGE = 8,
        /// `sendmsg(2)`.
        IORING_OP_SENDMSG = 9,
        /// `recvmsg(2)`.
        IORING_OP_RECVMSG = 10,
        /// Complete after a timeout or a number of other completions.
        IORING_OP_TIMEOUT = 11,
        /// Cancel or update an [`IORING_OP_TIMEOUT`] request.
        IORING_OP_TIMEOUT_REMOVE = 12,
        /// `accept4(2)`.
        IORING_OP_ACCEPT = 13,
        /// Cancel in-flight requests.
        IORING_OP_ASYNC_CANCEL = 14,
        /// Timeout for the request this one is linked to.
        IORING_OP_LINK_TIMEOUT = 15,
        /// `connect(2)`.
        IORING_OP_CONNECT = 16,
        /// `fallocate(2)`.
        IORING_OP_FALLOCATE = 17,
        /// `openat(2)`.
        IORING_OP_OPENAT = 18,
        /// `close(2)`, or release a registered file.
        IORING_OP_CLOSE = 19,
        /// Update the registered file table.
        IORING_OP_FILES_UPDATE = 20,
        /// `statx(2)`.
        IORING_OP_STATX = 21,
        /// `pread(2)`, or `read(2)` at offset -1.
        IORING_OP_READ = 22,
        /// `pwrite(2)`, or `write(2)` at offset -1.
        IORING_OP_WRITE = 23,
        /// `posix_fadvise(2)`.
        IORING_OP_FADVISE = 24,
        /// `madvise(2)`.
        IORING_OP_MADVISE = 25,
        /// `send(2)`.
        IORING_OP_SEND = 26,
        /// `recv(2)`.
        IORING_OP_RECV = 27,
        /// `openat2(2)`.
        IORING_OP_OPENAT2 = 28,
        /// `epoll_ctl(2)`.
        IORING_OP_EPOLL_CTL = 29,
        /// `splice(2)`.
        IORING_OP_SPLICE = 30,
        /// Add buffers to a legacy provided-buffer group.
        IORING_OP_PROVIDE_BUFFERS = 31,
        /// Remove buffers from a legacy provided-buffer group.
        IORING_OP_REMOVE_BUFFERS = 32,
        /// `tee(2)`.
        IORING_OP_TEE = 33,
        /// `shutdown(2)`.
        IORING_OP_SHUTDOWN = 34,
        /// `renameat2(2)`.
        IORING_OP_RENAMEAT = 35,
        /// `unlinkat(2)`.
        IORING_OP_UNLINKAT = 36,
        /// `mkdirat(2)`.
        IORING_OP_MKDIRAT = 37,
        /// `symlinkat(2)`.
        IORING_OP_SYMLINKAT = 38,
        /// `linkat(2)`.
        IORING_OP_LINKAT = 39,
        /// Post a completion to another ring, or pass it a registered file.
        IORING_OP_MSG_RING = 40,
        /// `fsetxattr(2)`.
        IORING_OP_FSETXATTR = 41,
        /// `setxattr(2)`.
        IORING_OP_SETXATTR = 42,
        /// `fgetxattr(2)`.
        IORING_OP_FGETXATTR = 43,
        /// `getxattr(2)`.
        IORING_OP_GETXATTR = 44,
        /// `socket(2)`.
        IORING_OP_SOCKET = 45,
        /// A command for the file's driver or subsystem.
        IORING_OP_URING_CMD = 46,
        /// Zero-copy `send(2)`.
        IORING_OP_SEND_ZC = 47,
        /// Zero-copy `sendmsg(2)`.
        IORING_OP_SENDMSG_ZC = 48,
        /// Repeated reads into provided buffers.
        IORING_OP_READ_MULTISHOT = 49,
        /// `waitid(2)`.
        IORING_OP_WAITID = 50,
        /// Wait on a futex.
        IORING_OP_FUTEX_WAIT = 51,
        /// Wake a futex.
        IORING_OP_FUTEX_WAKE = 52,
        /// `futex_waitv(2)`.
        IORING_OP_FUTEX_WAITV = 53,
        /// Install a registered file as a regular file descriptor.
        IORING_OP_FIXED_FD_INSTALL = 54,
        /// `ftruncate(2)`.
        IORING_OP_FTRUNCATE = 55,
        /// `bind(2)`.
        IORING_OP_BIND = 56,
        /// `listen(2)`.
        IORING_OP_LISTEN = 57,
        /// Zero-copy receive through a registered interface queue.
        IORING_OP_RECV_ZC = 58,
        /// `epoll_wait(2)`.
        IORING_OP_EPOLL_WAIT = 59,
        /// Vectored read into registered buffers.
        IORING_OP_READV_FIXED = 60,
        /// Vectored write from registered buffers.
        IORING_OP_WRITEV_FIXED = 61,
        /// `pipe2(2)`.
        IORING_OP_PIPE = 62,
        /// [`IORING_OP_NOP`] in a 128 byte entry.
        IORING_OP_NOP128 = 63,
        /// [`IORING_OP_URING_CMD`] in a 128 byte entry.
        IORING_OP_URING_CMD128 = 64,
    ];
}

consts! {
    /// Every `IOSQE_*` flag, for [`io_uring_sqe::flags`].
    pub const SQE_FLAGS: [u8] = [
        /// `fd` is an index into the registered file table.
        IOSQE_FIXED_FILE = 1 << 0,
        /// Start only after every earlier request has completed.
        IOSQE_IO_DRAIN = 1 << 1,
        /// Start the next request only after this one succeeds.
        IOSQE_IO_LINK = 1 << 2,
        /// Like [`IOSQE_IO_LINK`], but the next request starts even if this
        /// one fails.
        IOSQE_IO_HARDLINK = 1 << 3,
        /// Execute in an async worker instead of trying inline first.
        IOSQE_ASYNC = 1 << 4,
        /// Take a buffer from the provided-buffer group in `buf_index`.
        IOSQE_BUFFER_SELECT = 1 << 5,
        /// Post no completion if the request succeeds.
        IOSQE_CQE_SKIP_SUCCESS = 1 << 6,
    ];
}

consts! {
    /// Every `IORING_CQE_F_*` flag, for [`io_uring_cqe::flags`].
    pub const CQE_FLAGS: [u32] = [
        /// A provided buffer was used; its ID is in the bits from
        /// [`IORING_CQE_BUFFER_SHIFT`] up.
        IORING_CQE_F_BUFFER = 1 << 0,
        /// The request will post more completions.
        IORING_CQE_F_MORE = 1 << 1,
        /// The socket still had data after this receive.
        IORING_CQE_F_SOCK_NONEMPTY = 1 << 2,
        /// A zero-copy send notification: the kernel is done with the buffer.
        IORING_CQE_F_NOTIF = 1 << 3,
        /// The provided buffer was only partly consumed and the kernel keeps
        /// using it (incrementally consumed buffer rings).
        IORING_CQE_F_BUF_MORE = 1 << 4,
        /// Padding before the ring wraps; ignore this entry.
        IORING_CQE_F_SKIP = 1 << 5,
        /// A 32 byte entry in a ring set up with [`IORING_SETUP_CQE_MIXED`].
        IORING_CQE_F_32 = 1 << 15,
    ];
}

consts! {
    /// Every `IORING_ENTER_*` flag.
    pub const ENTER_FLAGS: [u32] = [
        /// Wait for `min_complete` completions.
        IORING_ENTER_GETEVENTS = 1 << 0,
        /// Wake the submission polling thread.
        IORING_ENTER_SQ_WAKEUP = 1 << 1,
        /// Wait for room in the submission queue.
        IORING_ENTER_SQ_WAIT = 1 << 2,
        /// `arg` points to an [`io_uring_getevents_arg`].
        IORING_ENTER_EXT_ARG = 1 << 3,
        /// `fd` is a registered ring index.
        IORING_ENTER_REGISTERED_RING = 1 << 4,
        /// The wait timeout is an absolute time.
        IORING_ENTER_ABS_TIMER = 1 << 5,
        /// `arg` is an offset into a registered wait-argument region.
        IORING_ENTER_EXT_ARG_REG = 1 << 6,
        /// Leave the wait out of iowait accounting.
        IORING_ENTER_NO_IOWAIT = 1 << 7,
    ];
}

consts! {
    /// Every `IORING_SQ_*` flag, for the submission ring's `flags` field.
    pub const SQ_RING_FLAGS: [u32] = [
        /// The submission polling thread sleeps; wake it with
        /// [`IORING_ENTER_SQ_WAKEUP`].
        IORING_SQ_NEED_WAKEUP = 1 << 0,
        /// Completions overflowed the completion ring and wait in the kernel.
        IORING_SQ_CQ_OVERFLOW = 1 << 1,
        /// Completion work is pending; enter the kernel to run it.
        IORING_SQ_TASKRUN = 1 << 2,
    ];
}

consts! {
    /// Every `IORING_CQ_*` flag, for the completion ring's `flags` field.
    pub const CQ_RING_FLAGS: [u32] = [
        /// Do not signal the registered eventfd.
        IORING_CQ_EVENTFD_DISABLED = 1 << 0,
    ];
}

consts! {
    /// Every `IORING_OFF_*` offset for mapping ring memory with `mmap(2)`.
    pub const MMAP_OFFSETS: [u64] = [
        /// The submission ring.
        IORING_OFF_SQ_RING = 0,
        /// The completion ring.
        IORING_OFF_CQ_RING = 0x800_0000,
        /// The array of submission queue entries.
        IORING_OFF_SQES = 0x1000_0000,
        /// Base of the kernel-allocated provided-buffer rings; add the group
        /// ID shifted by [`IORING_OFF_PBUF_SHIFT`].
        IORING_OFF_PBUF_RING = 0x8000_0000,
        /// Mask that selects the region part of an offset.
        IORING_OFF_MMAP_MASK = 0xf800_0000,
    ];
}

consts! {
    /// Every `io_uring_register(2)` opcode.
    pub const REGISTER_OPS: [u32] = [
        /// Register buffers for fixed-buffer I/O.
        IORING_REGISTER_BUFFERS = 0,
        /// Unregister all registered buffers.
        IORING_UNREGISTER_BUFFERS = 1,
        /// Register a file table.
        IORING_REGISTER_FILES = 2,
        /// Unregister the file table.
        IORING_UNREGISTER_FILES = 3,
        /// Signal an eventfd when completions are posted.
        IORING_REGISTER_EVENTFD = 4,
        /// Stop signalling the registered eventfd.
        IORING_UNREGISTER_EVENTFD = 5,
        /// Replace entries of the registered file table.
        IORING_REGISTER_FILES_UPDATE = 6,
        /// Signal an eventfd only for completions posted by async workers.
        IORING_REGISTER_EVENTFD_ASYNC = 7,
        /// Report supported opcodes in an [`io_uring_probe`].
        IORING_REGISTER_PROBE = 8,
        /// Register the caller's credentials for [`io_uring_sqe::personality`].
        IORING_REGISTER_PERSONALITY = 9,
        /// Unregister credentials.
        IORING_UNREGISTER_PERSONALITY = 10,
        /// Restrict what a disabled ring accepts.
        IORING_REGISTER_RESTRICTIONS = 11,
        /// Enable a ring created with [`IORING_SETUP_R_DISABLED`].
        IORING_REGISTER_ENABLE_RINGS = 12,
        /// Register a file table, with tags.
        IORING_REGISTER_FILES2 = 13,
        /// Replace entries of the registered file table, with tags.
        IORING_REGISTER_FILES_UPDATE2 = 14,
        /// Register buffers, with tags.
        IORING_REGISTER_BUFFERS2 = 15,
        /// Replace registered buffers.
        IORING_REGISTER_BUFFERS_UPDATE = 16,
        /// Set the CPU affinity of async workers.
        IORING_REGISTER_IOWQ_AFF = 17,
        /// Reset the CPU affinity of async workers.
        IORING_UNREGISTER_IOWQ_AFF = 18,
        /// Set or read the maximum number of async workers.
        IORING_REGISTER_IOWQ_MAX_WORKERS = 19,
        /// Register ring file descriptors, for [`IORING_ENTER_REGISTERED_RING`].
        IORING_REGISTER_RING_FDS = 20,
        /// Unregister ring file descriptors.
        IORING_UNREGISTER_RING_FDS = 21,
        /// Register a provided-buffer ring.
        IORING_REGISTER_PBUF_RING = 22,
        /// Unregister a provided-buffer ring.
        IORING_UNREGISTER_PBUF_RING = 23,
        /// Cancel requests and wait for them synchronously.
        IORING_REGISTER_SYNC_CANCEL = 24,
        /// Restrict the file slots [`IORING_FILE_INDEX_ALLOC`] allocates from.
        IORING_REGISTER_FILE_ALLOC_RANGE = 25,
        /// Report the head of a provided-buffer ring.
        IORING_REGISTER_PBUF_STATUS = 26,
        /// Enable NAPI busy polling.
        IORING_REGISTER_NAPI = 27,
        /// Disable NAPI busy polling.
        IORING_UNREGISTER_NAPI = 28,
        /// Choose the clock used for wait timeouts.
        IORING_REGISTER_CLOCK = 29,
        /// Copy registered buffers from another ring.
        IORING_REGISTER_CLONE_BUFFERS = 30,
        /// Send an [`IORING_OP_MSG_RING`] message without a ring of one's own.
        IORING_REGISTER_SEND_MSG_RING = 31,
        /// Register a network device receive queue for zero-copy receive.
        IORING_REGISTER_ZCRX_IFQ = 32,
        /// Resize the rings.
        IORING_REGISTER_RESIZE_RINGS = 33,
        /// Register a memory region, for example for wait arguments.
        IORING_REGISTER_MEM_REGION = 34,
        /// Query io_uring capabilities.
        IORING_REGISTER_QUERY = 35,
        /// Configure a zero-copy receive queue.
        IORING_REGISTER_ZCRX_CTRL = 36,
        /// Register BPF filter programs.
        IORING_REGISTER_BPF_FILTER = 37,
        /// Flag for the opcode: `fd` is a registered ring index.
        IORING_REGISTER_USE_REGISTERED_RING = 1 << 31,
    ];
}

consts! {
    /// Every `IORING_FSYNC_*` flag, for `fsync_flags`.
    pub const FSYNC_FLAGS: [u32] = [
        /// Sync data only, like `fdatasync(2)`.
        IORING_FSYNC_DATASYNC = 1 << 0,
    ];
}

consts! {
    /// Every timeout flag, for `timeout_flags`.
    pub const TIMEOUT_FLAGS: [u32] = [
        /// The timeout is an absolute time.
        IORING_TIMEOUT_ABS = 1 << 0,
        /// Update an existing timeout ([`IORING_OP_TIMEOUT_REMOVE`]).
        IORING_TIMEOUT_UPDATE = 1 << 1,
        /// Use `CLOCK_BOOTTIME`.
        IORING_TIMEOUT_BOOTTIME = 1 << 2,
        /// Use `CLOCK_REALTIME`.
        IORING_TIMEOUT_REALTIME = 1 << 3,
        /// Update an existing linked timeout.
        IORING_LINK_TIMEOUT_UPDATE = 1 << 4,
        /// Report expiry as success rather than `-ETIME`.
        IORING_TIMEOUT_ETIME_SUCCESS = 1 << 5,
        /// Fire repeatedly, posting a completion each time.
        IORING_TIMEOUT_MULTISHOT = 1 << 6,
    ];
}

consts! {
    /// Every poll flag. They go in `len`, since `poll32_events` holds the
    /// event mask.
    pub const POLL_FLAGS: [u32] = [
        /// Keep reporting events, posting a completion for each.
        IORING_POLL_ADD_MULTI = 1 << 0,
        /// Update the events of an existing poll request.
        IORING_POLL_UPDATE_EVENTS = 1 << 1,
        /// Update the `user_data` of an existing poll request.
        IORING_POLL_UPDATE_USER_DATA = 1 << 2,
        /// Level-triggered poll.
        IORING_POLL_ADD_LEVEL = 1 << 3,
    ];
}

consts! {
    /// Every `IORING_ASYNC_CANCEL_*` flag, for `cancel_flags`.
    pub const ASYNC_CANCEL_FLAGS: [u32] = [
        /// Cancel every matching request, not just the first.
        IORING_ASYNC_CANCEL_ALL = 1 << 0,
        /// Match on file descriptor rather than `user_data`.
        IORING_ASYNC_CANCEL_FD = 1 << 1,
        /// Match every request.
        IORING_ASYNC_CANCEL_ANY = 1 << 2,
        /// The file descriptor is a registered file index.
        IORING_ASYNC_CANCEL_FD_FIXED = 1 << 3,
        /// Match on `user_data`, the default when no other key is given.
        IORING_ASYNC_CANCEL_USERDATA = 1 << 4,
        /// Match on opcode.
        IORING_ASYNC_CANCEL_OP = 1 << 5,
    ];
}

consts! {
    /// Every send and receive flag, for [`io_uring_sqe::ioprio`].
    pub const RECVSEND_FLAGS: [u16] = [
        /// Poll for readiness before trying the operation.
        IORING_RECVSEND_POLL_FIRST = 1 << 0,
        /// Keep receiving, posting a completion per provided buffer filled.
        IORING_RECV_MULTISHOT = 1 << 1,
        /// Use the registered buffer in `buf_index`.
        IORING_RECVSEND_FIXED_BUF = 1 << 2,
        /// Report in the notification whether zero-copy fell back to copying.
        IORING_SEND_ZC_REPORT_USAGE = 1 << 3,
        /// Send or receive as many provided buffers as possible at once.
        IORING_RECVSEND_BUNDLE = 1 << 4,
        /// `addr` points to an iovec array.
        IORING_SEND_VECTORIZED = 1 << 5,
    ];
}

consts! {
    /// Every accept flag, for [`io_uring_sqe::ioprio`].
    pub const ACCEPT_FLAGS: [u16] = [
        /// Keep accepting, posting a completion per connection.
        IORING_ACCEPT_MULTISHOT = 1 << 0,
        /// Fail with `-EAGAIN` instead of waiting for a connection.
        IORING_ACCEPT_DONTWAIT = 1 << 1,
        /// Poll for readiness before trying to accept.
        IORING_ACCEPT_POLL_FIRST = 1 << 2,
    ];
}

consts! {
    /// Every [`IORING_OP_MSG_RING`] command, for [`io_uring_sqe::addr`].
    pub const MSG_RING_COMMANDS: [u64] = [
        /// Post a completion with `len` as its result and `off` as its
        /// `user_data`.
        IORING_MSG_DATA = 0,
        /// Pass a registered file to the other ring.
        IORING_MSG_SEND_FD = 1,
    ];
}

consts! {
    /// Every [`IORING_OP_MSG_RING`] flag, for `msg_ring_flags`.
    pub const MSG_RING_FLAGS: [u32] = [
        /// Post no completion to the target ring (not for
        /// [`IORING_MSG_DATA`]).
        IORING_MSG_RING_CQE_SKIP = 1 << 0,
        /// Use `file_index` as the flags of the target's completion.
        IORING_MSG_RING_FLAGS_PASS = 1 << 1,
    ];
}

consts! {
    /// Every [`IORING_OP_NOP`] flag, for `nop_flags`.
    pub const NOP_FLAGS: [u32] = [
        /// Complete with the result in `len`.
        IORING_NOP_INJECT_RESULT = 1 << 0,
        /// Look up the file in `fd`.
        IORING_NOP_FILE = 1 << 1,
        /// Look up the registered file in `fd`.
        IORING_NOP_FIXED_FILE = 1 << 2,
        /// Look up the registered buffer in `buf_index`.
        IORING_NOP_FIXED_BUFFER = 1 << 3,
        /// Complete through task work.
        IORING_NOP_TW = 1 << 4,
        /// Post a 32 byte completion.
        IORING_NOP_CQE32 = 1 << 5,
    ];
}

consts! {
    /// Every [`IORING_OP_URING_CMD`] flag, for `uring_cmd_flags`.
    pub const URING_CMD_FLAGS: [u32] = [
        /// Use the registered buffer in `buf_index`.
        IORING_URING_CMD_FIXED = 1 << 0,
        /// Keep posting completions, taking provided buffers.
        IORING_URING_CMD_MULTISHOT = 1 << 1,
    ];
}

consts! {
    /// Every [`io_uring_probe_op`] flag.
    pub const PROBE_OP_FLAGS: [u16] = [
        /// The kernel supports the opcode.
        IO_URING_OP_SUPPORTED = 1 << 0,
    ];
}

consts! {
    /// The remaining constants.
    pub const MISC_CONSTANTS: [u32] = [
        /// For [`io_uring_sqe::file_index`]: allocate a free registered file
        /// slot and return its index as the result.
        IORING_FILE_INDEX_ALLOC = !0,
        /// Shift from [`io_uring_cqe::flags`] to the provided buffer ID.
        IORING_CQE_BUFFER_SHIFT = 16,
        /// Shift for the group ID in a provided-buffer ring's mmap offset.
        IORING_OFF_PBUF_SHIFT = 16,
        /// For `splice_flags`: `splice_fd_in` is a registered file index.
        SPLICE_F_FD_IN_FIXED = 1 << 31,
        /// In a zero-copy notification's result: the data was copied after
        /// all.
        IORING_NOTIF_USAGE_ZC_COPIED = 1 << 31,
        /// For `install_fd_flags`: do not set `O_CLOEXEC` on the new
        /// descriptor.
        IORING_FIXED_FD_NO_CLOEXEC = 1 << 0,
    ];
}
