//! An io_uring instance with its queues mapped into memory.

use std::ffi::{c_int, c_void};
use std::io;
use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd};
use std::ptr;
use std::sync::atomic::{AtomicU32, Ordering};

use ruio_sys as sys;

use crate::mem::Mapping;

/// Setup flags [`Ring`] cannot work with: they change the memory layout, who
/// consumes the submission queue, or how the ring is entered.
const UNSUPPORTED_FLAGS: u32 = sys::IORING_SETUP_IOPOLL
    | sys::IORING_SETUP_SQPOLL
    | sys::IORING_SETUP_SQE128
    | sys::IORING_SETUP_CQE32
    | sys::IORING_SETUP_NO_MMAP
    | sys::IORING_SETUP_REGISTERED_FD_ONLY
    | sys::IORING_SETUP_CQE_MIXED
    | sys::IORING_SETUP_SQE_MIXED
    | sys::IORING_SETUP_SQ_REWIND;

/// The most entries that plug the block layer, its reads and writes, that
/// [`Ring::enter`] submits in one entry into the kernel. The kernel plugs the
/// block layer for each entry that submits them, sets the requests of the
/// plug's first 32 up in one batch and those of the rest one by one, and
/// flushes the plug as its 33rd request comes (`BLK_MAX_REQUEST_COUNT`): a
/// read past the 32nd costs a reference and a tag of its own.
const PLUG_BATCH: u32 = 32;

/// Whether the kernel plugs the block layer for an entry with `opcode`: a
/// read, a write, or a passthrough command (`.plug` in io_uring's
/// `opdef.c`).
#[inline]
fn plugs(opcode: u8) -> bool {
    matches!(
        opcode,
        sys::IORING_OP_READV
            | sys::IORING_OP_WRITEV
            | sys::IORING_OP_READ_FIXED
            | sys::IORING_OP_WRITE_FIXED
            | sys::IORING_OP_READ
            | sys::IORING_OP_WRITE
            | sys::IORING_OP_URING_CMD
            | sys::IORING_OP_READV_FIXED
            | sys::IORING_OP_WRITEV_FIXED
            | sys::IORING_OP_URING_CMD128
    )
}

/// An io_uring instance with its submission and completion queues mapped.
///
/// The ring knows nothing about operations. Callers fill entries from
/// [`Ring::next_sqe`], hand them to the kernel with [`Ring::enter`], and read
/// completions with [`Ring::completions`].
pub(crate) struct Ring {
    sq: SubmissionQueue,
    cq: CompletionQueue,
    /// What `io_uring_enter` is called with: the registered index of the
    /// ring's descriptor, or the descriptor itself.
    enter_fd: c_int,
    /// `IORING_ENTER_REGISTERED_RING` when `enter_fd` is a registered index.
    enter_flags: u32,
    /// What an entry that waits adds to `enter_flags`:
    /// `IORING_ENTER_NO_IOWAIT`, if the kernel takes it, so that a ring
    /// waiting for its sockets, or its timer, is not counted as waiting for
    /// a disk.
    wait_flags: u32,
    /// How many times the ring has entered the kernel.
    enters: u64,
    // Declared after the queues, which point into them, and before the
    // descriptor, so they are unmapped before it is closed.
    _rings: Mapping,
    _sqes: Mapping,
    fd: OwnedFd,
}

/// The application's side of the submission queue.
struct SubmissionQueue {
    head: *mut u32,
    tail: *mut u32,
    flags: *mut u32,
    mask: u32,
    entries: u32,
    sqes: *mut sys::io_uring_sqe,
    /// One past the last entry written. Entries up to here reach the kernel
    /// when [`Ring::enter`] publishes the tail.
    local_tail: u32,
    /// The kernel's head, as the last [`Ring::enter`] left it: the kernel
    /// consumes entries only while it is entered, there being no polling
    /// thread, so between entries this is the head, read once per entry.
    local_head: u32,
    /// How many of the queued entries plug the block layer, as
    /// [`Ring::note_queued`] counted them; see [`PLUG_BATCH`].
    plugged: u32,
    /// The duration of each slot's linked timeout, which the slot's entry
    /// points to; see [`Ring::next_link_timeout`].
    link_timeouts: Box<[sys::__kernel_timespec]>,
}

/// The application's side of the completion queue.
struct CompletionQueue {
    head: *mut u32,
    tail: *mut u32,
    mask: u32,
    cqes: *const sys::io_uring_cqe,
    /// One past the last entry read. The kernel may reuse entries up to here
    /// once the [`Completions`] that read them, as it is dropped, publishes
    /// the head.
    local_head: u32,
}

impl Ring {
    /// Creates a ring with `entries` submission and `cq_entries` completion
    /// slots, both rounded up to powers of two, and maps its queues.
    ///
    /// # Panics
    ///
    /// If `flags` contains any of [`UNSUPPORTED_FLAGS`].
    pub(crate) fn new(entries: u32, cq_entries: u32, flags: u32) -> io::Result<Self> {
        assert_eq!(flags & UNSUPPORTED_FLAGS, 0, "unsupported setup flags");
        let mut params = sys::io_uring_params {
            flags: flags | sys::IORING_SETUP_CQSIZE,
            cq_entries,
            ..Default::default()
        };
        // SAFETY: IORING_SETUP_NO_MMAP is excluded above, so the kernel uses
        // no memory that `params` points to.
        let fd = unsafe { sys::io_uring_setup(entries, &mut params) }?;
        // SAFETY: `fd` is a new descriptor that nothing else owns;
        // IORING_SETUP_REGISTERED_FD_ONLY, which returns an index instead, is
        // excluded above.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        if params.features & sys::IORING_FEAT_SINGLE_MMAP == 0 {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "io_uring lacks IORING_FEAT_SINGLE_MMAP",
            ));
        }

        let (sq_off, cq_off) = (params.sq_off, params.cq_off);
        let has_sq_array = params.flags & sys::IORING_SETUP_NO_SQARRAY == 0;
        let sq_len = if has_sq_array {
            sq_off.array as usize + params.sq_entries as usize * size_of::<u32>()
        } else {
            0
        };
        let cq_len =
            cq_off.cqes as usize + params.cq_entries as usize * size_of::<sys::io_uring_cqe>();
        let rings = Mapping::shared(fd.as_fd(), sys::IORING_OFF_SQ_RING, sq_len.max(cq_len))?;
        let sqes = Mapping::shared(
            fd.as_fd(),
            sys::IORING_OFF_SQES,
            params.sq_entries as usize * size_of::<sys::io_uring_sqe>(),
        )?;
        let field = |offset: u32| rings.ptr().wrapping_byte_add(offset as usize).cast::<u32>();

        if has_sq_array {
            // Entry i of the array names SQE i, so the queue can be treated
            // as if it were the SQE array itself.
            let array = field(sq_off.array);
            for index in 0..params.sq_entries {
                let slot = array.wrapping_add(index as usize);
                // SAFETY: the array's `sq_entries` elements lie inside the
                // mapping, and the kernel reads them only for published
                // entries, of which there are none yet.
                unsafe { slot.write(index) };
            }
        }

        debug_assert!(params.sq_entries.is_power_of_two() && params.cq_entries.is_power_of_two());
        let sq = SubmissionQueue {
            head: field(sq_off.head),
            tail: field(sq_off.tail),
            flags: field(sq_off.flags),
            mask: params.sq_entries - 1,
            entries: params.sq_entries,
            sqes: sqes.ptr().cast(),
            local_tail: 0,
            local_head: 0,
            plugged: 0,
            link_timeouts: vec![sys::__kernel_timespec::default(); params.sq_entries as usize]
                .into_boxed_slice(),
        };
        let cq = CompletionQueue {
            head: field(cq_off.head),
            tail: field(cq_off.tail),
            mask: params.cq_entries - 1,
            cqes: rings.ptr().wrapping_byte_add(cq_off.cqes as usize).cast(),
            local_head: 0,
        };
        let (enter_fd, enter_flags) = match register(&fd) {
            Some(index) => (index, sys::IORING_ENTER_REGISTERED_RING),
            None => (fd.as_raw_fd(), 0),
        };
        // An older kernel refuses the flag, as it does any it does not know.
        let wait_flags = if params.features & sys::IORING_FEAT_NO_IOWAIT != 0 {
            sys::IORING_ENTER_NO_IOWAIT
        } else {
            0
        };
        Ok(Self {
            sq,
            cq,
            enter_fd,
            enter_flags,
            wait_flags,
            enters: 0,
            _rings: rings,
            _sqes: sqes,
            fd,
        })
    }

    /// Whether the ring's descriptor is registered, so entering the kernel
    /// skips looking it up.
    pub(crate) fn is_registered(&self) -> bool {
        self.enter_flags != 0
    }

    /// The ring's own descriptor, which other rings send messages to.
    pub(crate) fn raw_fd(&self) -> c_int {
        self.fd.as_raw_fd()
    }

    /// Registers resources with the ring, or queries or configures it; see
    /// [`sys::io_uring_register`].
    ///
    /// # Safety
    ///
    /// As for [`sys::io_uring_register`].
    pub(crate) unsafe fn register(
        &self,
        opcode: u32,
        arg: *mut c_void,
        nr_args: u32,
    ) -> io::Result<u32> {
        // SAFETY: the caller upholds the contract of `io_uring_register`.
        unsafe { sys::io_uring_register(self.fd.as_raw_fd(), opcode, arg, nr_args) }
    }

    /// The `IORING_SQ_*` flags.
    #[inline]
    pub(crate) fn sq_flags(&self) -> u32 {
        // SAFETY: `sq.flags` points to the flags field in the live mapping.
        unsafe { atomic(self.sq.flags) }.load(Ordering::Relaxed)
    }

    /// Entries written and not yet consumed by the kernel.
    #[inline]
    pub(crate) fn queued(&self) -> u32 {
        self.sq.local_tail.wrapping_sub(self.sq.local_head)
    }

    /// Free submission queue slots.
    #[inline]
    pub(crate) fn sq_space(&self) -> u32 {
        self.sq.entries - self.queued()
    }

    /// The number of submission queue slots.
    #[inline]
    pub(crate) fn sq_entries(&self) -> u32 {
        self.sq.entries
    }

    /// The next submission queue entry, zeroed, or `None` if the queue is
    /// full. The entry reaches the kernel at the next [`Ring::enter`].
    #[inline]
    pub(crate) fn next_sqe(&mut self) -> Option<&mut sys::io_uring_sqe> {
        if self.queued() == self.sq.entries {
            return None;
        }
        let sqe = self
            .sq
            .sqes
            .wrapping_add((self.sq.local_tail & self.sq.mask) as usize);
        self.sq.local_tail = self.sq.local_tail.wrapping_add(1);
        // SAFETY: fewer than `entries` entries were queued, so this slot is
        // not one the kernel has yet to consume: the kernel reads only
        // entries between its head and the published tail. The slot lies in
        // the live mapping, and nothing else refers to it.
        let sqe = unsafe { &mut *sqe };
        *sqe = sys::io_uring_sqe::default();
        Some(sqe)
    }

    /// The next submission queue entry, filled in as a linked timeout
    /// (`IORING_OP_LINK_TIMEOUT`) of `duration`, or `None` if the queue is
    /// full. It bounds the entry before it, which must carry
    /// `IOSQE_IO_LINK`.
    ///
    /// The duration is kept in memory of the entry's slot, which the entry
    /// points to. That is enough: the kernel copies the duration when it
    /// consumes the entry (`IORING_FEAT_SUBMIT_STABLE`), and the slot is not
    /// reused, so the duration not rewritten, before then.
    #[inline]
    pub(crate) fn next_link_timeout(
        &mut self,
        duration: sys::__kernel_timespec,
    ) -> Option<&mut sys::io_uring_sqe> {
        // A full queue's next slot holds an entry the kernel has yet to
        // consume, whose duration must stay as it is.
        if self.sq_space() == 0 {
            return None;
        }
        let slot = (self.sq.local_tail & self.sq.mask) as usize;
        let stored = &mut self.sq.link_timeouts[slot];
        *stored = duration;
        let addr = ptr::from_mut(stored).expose_provenance() as u64;
        let sqe = self.next_sqe()?;
        sqe.opcode = sys::IORING_OP_LINK_TIMEOUT;
        sqe.addr = addr;
        sqe.len = 1;
        Some(sqe)
    }

    /// Publishes the queued entries and enters the kernel, submitting them
    /// and waiting until `min_complete` completions are ready or `timeout`
    /// passes. Returns the number of entries the last entry into the kernel
    /// submitted.
    ///
    /// Entries that plug the block layer go in batches of [`PLUG_BATCH`],
    /// each but the last in an entry of its own that waits for nothing; the
    /// last goes with the rest.
    ///
    /// Always passes `IORING_ENTER_GETEVENTS`: a ring set up with
    /// `IORING_SETUP_DEFER_TASKRUN` only posts completions when asked to.
    ///
    /// # Safety
    ///
    /// Every queued entry must point only to memory that stays valid for as
    /// long as the kernel uses it: until its request completes, or, for what
    /// the kernel copies when it consumes the entry, until then. The ring
    /// keeps a linked timeout's duration that long.
    pub(crate) unsafe fn enter(
        &mut self,
        min_complete: u32,
        timeout: Option<&sys::__kernel_timespec>,
    ) -> io::Result<u32> {
        // SAFETY: `sq.tail` points to the tail field in the live mapping.
        unsafe { atomic(self.sq.tail) }.store(self.sq.local_tail, Ordering::Release);
        if self.sq.plugged > PLUG_BATCH {
            // SAFETY: the caller vouches for the queued entries.
            unsafe { self.submit_plug_batches() }?;
        }
        let to_submit = self.queued();
        let mut flags = sys::IORING_ENTER_GETEVENTS | self.enter_flags;
        if min_complete > 0 {
            flags |= self.wait_flags;
        }
        let arg;
        let (arg_ptr, arg_len) = match timeout {
            Some(timeout) => {
                arg = sys::io_uring_getevents_arg {
                    ts: ptr::from_ref(timeout).expose_provenance() as u64,
                    ..Default::default()
                };
                flags |= sys::IORING_ENTER_EXT_ARG;
                (ptr::from_ref(&arg).cast(), size_of_val(&arg))
            }
            None => (ptr::null(), 0),
        };
        // SAFETY: the caller vouches for the queued entries, and `arg` and the
        // timeout it points to outlive the call.
        let entered =
            unsafe { self.enter_kernel(to_submit, min_complete, flags, arg_ptr, arg_len) };
        if self.queued() == 0 {
            self.sq.plugged = 0;
        }
        entered
    }

    /// Submits the queued entries in batches of at most [`PLUG_BATCH`] that
    /// plug the block layer, each but the last in an entry into the kernel
    /// of its own, and leaves the last for the caller's entry; see
    /// [`Ring::first_plug_batch`]. Each entry waits for nothing, but runs
    /// the completion work of what it submitted, as `IORING_ENTER_GETEVENTS`
    /// has it, which a device that finishes inline leaves ready: an entry
    /// runs at most 40 items of it unless asked to wait for more, room for a
    /// batch's.
    ///
    /// # Safety
    ///
    /// As for [`Ring::enter`].
    unsafe fn submit_plug_batches(&mut self) -> io::Result<()> {
        let flags = sys::IORING_ENTER_GETEVENTS | self.enter_flags;
        while self.sq.plugged > PLUG_BATCH {
            let Some((len, plugged)) = self.first_plug_batch() else {
                break;
            };
            let head = self.sq.local_head;
            // SAFETY: the caller vouches for the queued entries, and the call
            // reads no argument.
            unsafe { self.enter_kernel(len, 0, flags, ptr::null(), 0) }?;
            if self.sq.local_head.wrapping_sub(head) != len {
                // The kernel took fewer: the rest go in the caller's entry.
                break;
            }
            self.sq.plugged = self.sq.plugged.saturating_sub(plugged);
        }
        Ok(())
    }

    /// The first batch of the queued entries, if they take more than one:
    /// how many entries it holds, and how many of them plug the block layer.
    /// It ends after the chain that holds its [`PLUG_BATCH`]th entry that
    /// plugs, as the kernel links only entries submitted together; and if
    /// no entry that plugs comes after that, the queued entries are one
    /// batch.
    fn first_plug_batch(&self) -> Option<(u32, u32)> {
        let head = self.sq.local_head;
        let mut position = head;
        let mut plugged = 0;
        let mut end: Option<u32> = None;
        while position != self.sq.local_tail {
            let sqe = self
                .sq
                .sqes
                .wrapping_add((position & self.sq.mask) as usize);
            // SAFETY: the entry lies between the head and the tail: written,
            // and not yet taken by the kernel, which only ever reads entries.
            let sqe = unsafe { &*sqe };
            position = position.wrapping_add(1);
            let plug = plugs(sqe.opcode);
            match end {
                Some(end) if plug => return Some((end.wrapping_sub(head), plugged)),
                Some(_) => {}
                None => {
                    plugged += u32::from(plug);
                    let linked = sqe.flags & (sys::IOSQE_IO_LINK | sys::IOSQE_IO_HARDLINK) != 0;
                    if plugged >= PLUG_BATCH && !linked {
                        end = Some(position);
                    }
                }
            }
        }
        None
    }

    /// Enters the kernel, counting the entry, and reads where it left the
    /// submission queue's head.
    ///
    /// # Safety
    ///
    /// As for [`Ring::enter`], for the first `to_submit` queued entries; and
    /// `arg` must point to `arg_len` bytes for the call to read, as `flags`
    /// describe them.
    unsafe fn enter_kernel(
        &mut self,
        to_submit: u32,
        min_complete: u32,
        flags: u32,
        arg: *const c_void,
        arg_len: usize,
    ) -> io::Result<u32> {
        self.enters += 1;
        // SAFETY: the caller's contract.
        let entered = unsafe {
            sys::io_uring_enter(self.enter_fd, to_submit, min_complete, flags, arg, arg_len)
        };
        // Read whether or not the call failed: it may have consumed entries
        // before failing.
        // SAFETY: `sq.head` points to the head field in the live mapping.
        self.sq.local_head = unsafe { atomic(self.sq.head) }.load(Ordering::Acquire);
        entered
    }

    /// Counts the entry just queued, with `opcode`, toward those that plug
    /// the block layer, which [`Ring::enter`] submits in batches; an entry
    /// left uncounted only goes in a larger batch.
    #[inline]
    pub(crate) fn note_queued(&mut self, opcode: u8) {
        if plugs(opcode) {
            self.sq.plugged += 1;
        }
    }

    /// How many times the ring has entered the kernel.
    #[inline]
    pub(crate) fn enters(&self) -> u64 {
        self.enters
    }

    /// Whether the kernel has posted completions that have not been popped.
    #[inline]
    pub(crate) fn cq_ready(&self) -> bool {
        // SAFETY: `cq.tail` points to the tail field in the live mapping.
        let tail = unsafe { atomic(self.cq.tail) }.load(Ordering::Acquire);
        self.cq.local_head != tail
    }

    /// The completions the kernel has posted, as many as the queue holds
    /// now, oldest first. Their slots go back to the kernel, once read, as
    /// the iterator is dropped.
    #[inline]
    pub(crate) fn completions(&mut self) -> Completions<'_> {
        // SAFETY: `cq.tail` points to the tail field in the live mapping.
        let tail = unsafe { atomic(self.cq.tail) }.load(Ordering::Acquire);
        Completions {
            cq: &mut self.cq,
            tail,
        }
    }
}

/// The completions a ring's queue held as [`Ring::completions`] looked: the
/// tail is read once for all of them.
pub(crate) struct Completions<'a> {
    cq: &'a mut CompletionQueue,
    tail: u32,
}

impl Completions<'_> {
    /// How many there are left to read.
    #[inline]
    pub(crate) fn len(&self) -> u32 {
        self.tail.wrapping_sub(self.cq.local_head)
    }
}

impl Iterator for Completions<'_> {
    type Item = sys::io_uring_cqe;

    #[inline]
    fn next(&mut self) -> Option<sys::io_uring_cqe> {
        if self.cq.local_head == self.tail {
            return None;
        }
        let cqe = self
            .cq
            .cqes
            .wrapping_add((self.cq.local_head & self.cq.mask) as usize);
        self.cq.local_head = self.cq.local_head.wrapping_add(1);
        // SAFETY: the entry lies before the tail the kernel published, so the
        // acquire load that read the tail makes its contents visible, and the
        // kernel does not reuse it until the head is released past it.
        Some(unsafe { cqe.read() })
    }
}

impl Drop for Completions<'_> {
    #[inline]
    fn drop(&mut self) {
        // Returns the slots read to the kernel.
        // SAFETY: `cq.head` points to the head field in the live mapping.
        unsafe { atomic(self.cq.head) }.store(self.cq.local_head, Ordering::Release);
    }
}

impl Drop for Ring {
    fn drop(&mut self) {
        if self.is_registered() {
            // The registration holds a reference to the ring, which would
            // otherwise live on until the thread exits.
            let mut update = sys::io_uring_rsrc_update {
                offset: self.enter_fd as u32,
                ..Default::default()
            };
            // SAFETY: `update` is one valid entry for the kernel to read.
            let _ = unsafe {
                sys::io_uring_register(
                    self.fd.as_raw_fd(),
                    sys::IORING_UNREGISTER_RING_FDS,
                    ptr::from_mut(&mut update).cast(),
                    1,
                )
            };
        }
    }
}

/// Registers the ring's descriptor with the calling thread, returning its
/// index, or `None` if the thread has no free registration slot.
fn register(fd: &OwnedFd) -> Option<c_int> {
    let mut update = sys::io_uring_rsrc_update {
        offset: u32::MAX,
        resv: 0,
        data: fd.as_raw_fd() as u64,
    };
    // SAFETY: `update` is one valid entry, which the kernel reads and writes
    // the chosen slot back into.
    let registered = unsafe {
        sys::io_uring_register(
            fd.as_raw_fd(),
            sys::IORING_REGISTER_RING_FDS,
            ptr::from_mut(&mut update).cast(),
            1,
        )
    };
    (registered.ok() == Some(1)).then_some(update.offset as c_int)
}

/// A `u32` in ring memory, viewed as an atomic.
///
/// # Safety
///
/// `ptr` must point to an aligned `u32` in a live ring mapping. The kernel
/// accesses these fields only with atomic operations.
#[inline]
unsafe fn atomic<'a>(ptr: *mut u32) -> &'a AtomicU32 {
    // SAFETY: the caller's guarantees are the requirements of `from_ptr`.
    unsafe { AtomicU32::from_ptr(ptr) }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;

    const FLAGS: u32 = sys::IORING_SETUP_SINGLE_ISSUER
        | sys::IORING_SETUP_DEFER_TASKRUN
        | sys::IORING_SETUP_TASKRUN_FLAG
        | sys::IORING_SETUP_SUBMIT_ALL
        | sys::IORING_SETUP_CLAMP;

    fn ring(entries: u32, flags: u32) -> Ring {
        Ring::new(entries, entries * 4, flags).unwrap()
    }

    fn queue_nop(ring: &mut Ring, user_data: u64) {
        let sqe = ring.next_sqe().expect("a free entry");
        sqe.opcode = sys::IORING_OP_NOP;
        sqe.user_data = user_data;
    }

    /// Queues a read of one byte from `fd` to `addr`, linked to the next
    /// entry if `linked`, and counts it as the driver counts an operation's.
    fn queue_read(ring: &mut Ring, fd: c_int, addr: u64, user_data: u64, linked: bool) {
        let sqe = ring.next_sqe().expect("a free entry");
        sqe.opcode = sys::IORING_OP_READ;
        sqe.fd = fd;
        sqe.addr = addr;
        sqe.len = 1;
        sqe.user_data = user_data;
        if linked {
            sqe.flags = sys::IOSQE_IO_LINK;
        }
        ring.note_queued(sys::IORING_OP_READ);
    }

    /// Submits the queued no-ops and returns the `user_data` of `n`
    /// completions, in order.
    fn complete(ring: &mut Ring, n: u32) -> Vec<u64> {
        // SAFETY: no-ops point to no memory.
        unsafe { ring.enter(n, None) }.unwrap();
        ring.completions()
            .map(|cqe| {
                assert_eq!(cqe.res, 0);
                cqe.user_data
            })
            .collect()
    }

    #[test]
    fn no_ops_complete_with_their_user_data() {
        let mut variants = vec![FLAGS];
        if crate::Probe::detect()
            .unwrap()
            .supports_setup_flags(sys::IORING_SETUP_NO_SQARRAY)
        {
            variants.push(FLAGS | sys::IORING_SETUP_NO_SQARRAY);
        }
        for flags in variants {
            let mut ring = ring(8, flags);
            for user_data in 1..=3 {
                queue_nop(&mut ring, user_data);
            }
            assert_eq!(ring.queued(), 3);
            assert_eq!(complete(&mut ring, 3), [1, 2, 3]);
            assert_eq!(ring.queued(), 0);
            assert_eq!(ring.completions().next().map(|cqe| cqe.user_data), None);
        }
    }

    #[test]
    fn a_full_queue_has_no_free_entry_until_submitted() {
        let mut ring = ring(4, FLAGS);
        assert_eq!(ring.sq_space(), 4);
        for user_data in 0..4 {
            queue_nop(&mut ring, user_data);
        }
        assert_eq!(ring.sq_space(), 0);
        assert!(ring.next_sqe().is_none());
        let timeout = sys::__kernel_timespec::default();
        assert!(ring.next_link_timeout(timeout).is_none());
        assert_eq!(complete(&mut ring, 4).len(), 4);
        assert!(ring.next_sqe().is_some());
    }

    #[test]
    fn indices_wrap_around_the_queues() {
        let mut ring = ring(2, FLAGS);
        for round in 0..100 {
            queue_nop(&mut ring, round * 2);
            queue_nop(&mut ring, round * 2 + 1);
            assert_eq!(complete(&mut ring, 2), [round * 2, round * 2 + 1]);
        }
    }

    #[test]
    fn enter_times_out() {
        let mut ring = ring(2, FLAGS);
        let timeout = sys::__kernel_timespec {
            tv_sec: 0,
            tv_nsec: 10_000_000,
        };
        let start = Instant::now();
        // SAFETY: nothing is queued.
        let err = unsafe { ring.enter(1, Some(&timeout)) }.unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ETIME));
        assert!(start.elapsed() >= Duration::from_millis(10));
    }

    /// A batch of reads ends after its 32nd, or after the chain that holds
    /// it, and only if another read comes after.
    #[test]
    fn plug_batches_end_where_chains_do() {
        let mut ring = ring(64, FLAGS);
        for user_data in 0..31 {
            queue_read(&mut ring, -1, 0, user_data, false);
        }
        // A chain of three, whose first entry is the 32nd read.
        for user_data in 31..34 {
            queue_read(&mut ring, -1, 0, user_data, user_data < 33);
        }
        assert_eq!(ring.first_plug_batch(), None, "no read comes after");
        for user_data in 34..40 {
            queue_nop(&mut ring, user_data);
        }
        assert_eq!(ring.first_plug_batch(), None, "no-ops plug nothing");
        queue_read(&mut ring, -1, 0, 40, false);
        assert_eq!(ring.first_plug_batch(), Some((34, 34)));
        // Dropped unsubmitted: none of these reaches the kernel.
    }

    /// Reads beyond a block plug's batch go in entries into the kernel of
    /// their own, and every one completes.
    #[test]
    fn reads_beyond_a_plug_batch_go_in_entries_of_their_own() {
        let zero = std::fs::File::open("/dev/zero").unwrap();
        let mut ring = ring(128, FLAGS);
        let mut bytes = [1_u8; 80];
        for (index, byte) in bytes.iter_mut().enumerate() {
            let addr = ptr::from_mut(byte).expose_provenance() as u64;
            queue_read(&mut ring, zero.as_raw_fd(), addr, index as u64, false);
        }
        // SAFETY: each read points to a byte of `bytes`, which outlives the
        // reads, as the entry waits for all of them to complete.
        unsafe { ring.enter(80, None) }.unwrap();
        // 32, 32, and the 16 that go with the entry that waits.
        assert_eq!(ring.enters(), 3);
        assert_eq!(ring.queued(), 0);
        let mut done: Vec<u64> = ring
            .completions()
            .map(|cqe| {
                assert_eq!(cqe.res, 1);
                cqe.user_data
            })
            .collect();
        done.sort_unstable();
        assert_eq!(done, (0..80).collect::<Vec<_>>());
        assert!(bytes.iter().all(|&byte| byte == 0));
    }

    /// A thread can register only 16 rings, so if dropping a ring did not
    /// release its registration, later rings would go unregistered.
    #[test]
    fn dropping_a_ring_releases_its_registration() {
        for _ in 0..40 {
            let mut ring = ring(2, FLAGS);
            assert!(ring.is_registered());
            queue_nop(&mut ring, 7);
            assert_eq!(complete(&mut ring, 1), [7]);
        }
    }
}
