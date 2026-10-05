//! Provided-buffer pools: buffers the kernel picks for reads and receives
//! when data arrives.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::fmt;
use std::io;
use std::iter;
use std::mem::offset_of;
use std::ops::{Deref, DerefMut};
use std::ptr;
use std::rc::Rc;
use std::slice;
use std::sync::atomic::{AtomicU16, AtomicU32, Ordering};
use std::task::{Context, Poll, Waker};
use std::vec;

use ruio_sys as sys;

use crate::buf::{IoBuf, IoBufMut};
use crate::driver::WeakDriver;
use crate::mem::Mapping;
use crate::{Driver, Probe};

/// A pool of equal-sized buffers that the kernel picks from as data arrives:
/// a provided-buffer ring (`IORING_REGISTER_PBUF_RING`).
///
/// An operation reading into a pool claims no buffer until data arrives, so
/// many idle readers can share a few buffers. The data comes back as a
/// [`BufLease`], which returns its buffer to the pool when dropped, or, from
/// a receive that took several buffers at once, as a [`BufBundle`] of them.
/// When every buffer is leased, reads into the pool fail with `ENOBUFS`.
///
/// The pool stays registered and mapped until it, every lease, and every
/// operation reading into it are gone, so the kernel never writes to memory
/// that has been freed.
#[derive(Clone)]
pub struct BufPool {
    pool: Rc<Pool>,
}

/// A pool's state, which tracks what is in each slot of its ring.
///
/// The kernel takes buffers from the ring's head, one slot after another,
/// and a bundle's completion names only the first buffer it took: the others
/// are whichever buffers followed it in the ring. So the pool keeps what it
/// put in each slot, and never reuses a slot the kernel has taken from until
/// a completion has said so. Positions count the buffers ever put in the
/// ring, wrapping; a position's slot is the position modulo `count`.
struct Pool {
    driver: WeakDriver,
    group: u16,
    /// The ring of buffer descriptors the kernel takes buffers from.
    ring: Mapping,
    /// The buffers, one after another.
    memory: Mapping,
    count: u16,
    size: u32,
    /// The position the next buffer goes in: the ring's tail, as the pool
    /// last published it.
    tail: Cell<u16>,
    /// The first position that no completion has reported taken. The
    /// kernel's head is here or past it, as completions can report takes out
    /// of order; the tail stays within `count` of it, so that no slot is
    /// reused before its buffer is accounted for.
    low: Cell<u16>,
    /// The buffer the pool last put in each slot.
    slots: Box<[Cell<u16>]>,
    /// The position each buffer was last put in the ring at.
    positions: Box<[Cell<u16>]>,
    /// Whether each buffer is in the ring, where the kernel may take it.
    in_ring: Box<[Cell<bool>]>,
    /// For each slot, whether a completion has reported its position taken
    /// while `low` was still behind it.
    taken: Box<[Cell<bool>]>,
    /// Buffers returned while no slot could be reused, the latest last.
    spare: RefCell<Vec<u16>>,
    /// Buffers in the ring, which the kernel may pick.
    available: Cell<usize>,
    /// Tasks waiting for a buffer to return to the ring, in the order they
    /// came. Each buffer that returns wakes the first of them, rather than
    /// all of them: woken all at once, every waiter would ask the kernel
    /// again for the one buffer, and all but one starve again.
    waiters: RefCell<VecDeque<Waker>>,
    /// Whether the kernel consumes the buffers incrementally
    /// (`IOU_PBUF_RING_INC`): each completion takes as much of a buffer as
    /// its data fills, from where the last left off, and the kernel keeps
    /// the buffer in the ring until it is full.
    incremental: bool,
    /// For each buffer in the ring, how much of it the kernel has filled;
    /// only ever above zero in incremental mode.
    filled: Box<[Cell<u32>]>,
    /// For each buffer, how many leases of parts of it there are, in
    /// incremental mode.
    shares: Box<[Cell<u32>]>,
}

impl BufPool {
    /// Maps `count` buffers of `size` bytes each, zeroed, and registers them
    /// as a provided-buffer ring of the driver's.
    ///
    /// # Errors
    ///
    /// `InvalidInput` unless `count` is a power of two up to 32768 and
    /// `size` is above zero, and what `mmap` and the kernel report.
    pub fn new(driver: &Driver, count: u16, size: u32) -> io::Result<Self> {
        Self::with(driver, count, size, 0)
    }

    /// A pool whose buffers the kernel consumes incrementally (Linux 6.12),
    /// for one reader at a time: a completion does not say where in its
    /// buffer its data starts, which the pool works out from the
    /// completions before it, so it must see them in the order the kernel
    /// posted them.
    ///
    /// # Errors
    ///
    /// `Unsupported` if the kernel cannot, and as for [`BufPool::new`].
    pub(crate) fn incremental(driver: &Driver, count: u16, size: u32) -> io::Result<Self> {
        if !Probe::current()?.supports_incremental_buffers() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "incremental buffer consumption needs Linux 6.12",
            ));
        }
        Self::with(driver, count, size, sys::IOU_PBUF_RING_INC)
    }

    /// A pool registered with the `IOU_PBUF_RING_*` flags `flags`.
    fn with(driver: &Driver, count: u16, size: u32, flags: u16) -> io::Result<Self> {
        if !count.is_power_of_two() || count > 1 << 15 || size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a pool needs a power-of-two count up to 32768 and a size above zero",
            ));
        }
        let ring = Mapping::anonymous(usize::from(count) * size_of::<sys::io_uring_buf>())?;
        let memory = Mapping::anonymous(usize::from(count) * size as usize)?;
        let cells = |value| (0..count).map(|_| Cell::new(value)).collect();
        let pool = Pool {
            driver: driver.downgrade(),
            group: driver.next_buffer_group(),
            ring,
            memory,
            count,
            size,
            tail: Cell::new(0),
            low: Cell::new(0),
            slots: cells(0),
            positions: cells(0),
            in_ring: (0..count).map(|_| Cell::new(false)).collect(),
            taken: (0..count).map(|_| Cell::new(false)).collect(),
            spare: RefCell::new(Vec::with_capacity(usize::from(count))),
            available: Cell::new(0),
            waiters: RefCell::new(VecDeque::new()),
            incremental: flags & sys::IOU_PBUF_RING_INC != 0,
            filled: (0..count).map(|_| Cell::new(0)).collect(),
            shares: (0..count).map(|_| Cell::new(0)).collect(),
        };
        for bid in 0..count {
            pool.put(bid);
        }
        let mut reg = sys::io_uring_buf_reg {
            ring_addr: pool.ring.ptr().expose_provenance() as u64,
            ring_entries: u32::from(count),
            bgid: pool.group,
            flags,
            ..Default::default()
        };
        // SAFETY: `reg` is one valid registration. The ring and the buffers
        // it describes stay mapped until the pool unregisters them, and only
        // the kernel, leases and operations holding the pool touch them.
        unsafe {
            driver.register(
                sys::IORING_REGISTER_PBUF_RING,
                ptr::from_mut(&mut reg).cast(),
                1,
            )
        }?;
        Ok(Self {
            pool: Rc::new(pool),
        })
    }

    /// The number of buffers.
    #[inline]
    pub fn count(&self) -> usize {
        usize::from(self.pool.count)
    }

    /// The size of each buffer.
    #[inline]
    pub fn buffer_size(&self) -> usize {
        self.pool.size as usize
    }

    /// The number of buffers the kernel may pick, which are not leased.
    #[inline]
    pub fn available(&self) -> usize {
        self.pool.available.get()
    }

    /// Whether a buffer is available; if not, arranges for the task to be
    /// woken when one returns. Buffers that return wake the tasks waiting
    /// in the order they asked, one for each buffer.
    pub fn poll_available(&self, cx: &mut Context<'_>) -> Poll<()> {
        if self.available() > 0 {
            return Poll::Ready(());
        }
        let mut waiters = self.pool.waiters.borrow_mut();
        // A task that asks again before it is woken is still at the back.
        // One that asks again later is woken once for each time it asked,
        // which costs a wake-up, not a buffer.
        if !waiters
            .back()
            .is_some_and(|waiter| waiter.will_wake(cx.waker()))
        {
            waiters.push_back(cx.waker().clone());
        }
        Poll::Pending
    }

    /// The buffer group ID that submission entries select from.
    #[inline]
    pub(crate) fn group(&self) -> u16 {
        self.pool.group
    }

    /// Whether the kernel consumes the buffers incrementally; see
    /// [`BufPool::incremental`].
    #[inline]
    pub(crate) fn is_incremental(&self) -> bool {
        self.pool.incremental
    }

    /// The lease of the buffer a completion reports, if it reports one.
    ///
    /// # Panics
    ///
    /// If the buffer is not in the ring, which would give the lease a buffer
    /// the kernel may still write to.
    pub(crate) fn lease(&self, flags: u32, len: usize) -> Option<BufLease> {
        if flags & sys::IORING_CQE_F_BUFFER == 0 {
            return None;
        }
        if self.pool.incremental {
            let mut leases = self.pool.take_incremental(flags, len);
            assert!(leases.len() <= 1, "the kernel read into several buffers");
            return leases.pop();
        }
        let bid = (flags >> sys::IORING_CQE_BUFFER_SHIFT) as u16;
        self.pool.take(bid);
        Some(self.pool.lease_taken(bid, len))
    }

    /// The leases of the buffers a completion of a bundle reports, if it
    /// reports any: `res` bytes in the buffers from the one it names on, in
    /// ring order, as the kernel fills them. A completion without data took
    /// at most one buffer.
    ///
    /// # Panics
    ///
    /// If the data does not fit the buffers in the ring from the first one
    /// on, which would give leases buffers the kernel may still write to.
    pub(crate) fn bundle(&self, flags: u32, res: i32) -> Option<BufBundle> {
        if flags & sys::IORING_CQE_F_BUFFER == 0 {
            return None;
        }
        if self.pool.incremental {
            let total = usize::try_from(res).unwrap_or(0);
            let mut leases = self.pool.take_incremental(flags, total).into_iter();
            return leases.next().map(|first| BufBundle {
                first,
                rest: leases.collect(),
            });
        }
        let pool = &self.pool;
        let first = (flags >> sys::IORING_CQE_BUFFER_SHIFT) as u16;
        assert!(
            first < pool.count && pool.in_ring[usize::from(first)].get(),
            "the kernel reported buffer {first}, which is not in the ring"
        );
        let start = pool.positions[usize::from(first)].get();
        let total = usize::try_from(res).unwrap_or(0);
        // The slots from the first buffer's on hold the buffers the kernel
        // filled, each to the length its entry gives: the kernel shortens the
        // last buffer it maps to the data it expects. The first walk checks
        // each slot still holds what the kernel took from it, a buffer in the
        // ring put there at that position, and counts them.
        let mut count: u16 = 0;
        let mut left = total;
        loop {
            let position = start.wrapping_add(count);
            let bid = pool.slots[pool.slot(position)].get();
            let in_ring = pool.in_ring[usize::from(bid)].get()
                && pool.positions[usize::from(bid)].get() == position;
            assert!(
                in_ring,
                "the kernel reported {res} bytes from buffer {first}, more than its buffers \
                 after it in the ring hold"
            );
            left -= left.min(pool.entry_len(position));
            count += 1;
            if left == 0 {
                break;
            }
        }
        // The second walk takes them. Taking puts spare buffers only in the
        // slot of the tail, whose last position was reported taken before
        // any of these, so it is none of theirs.
        let mut left = total;
        let mut lease = |index: u16| {
            let position = start.wrapping_add(index);
            let bid = pool.slots[pool.slot(position)].get();
            let len = left.min(pool.entry_len(position));
            left -= len;
            pool.take(bid);
            pool.lease_taken(bid, len)
        };
        let first = lease(0);
        // Most messages fit one buffer, whose bundle allocates nothing.
        let rest = if count > 1 {
            (1..count).map(lease).collect()
        } else {
            Vec::new()
        };
        Some(BufBundle { first, rest })
    }
}

impl Pool {
    #[inline]
    fn buffer(&self, bid: u16) -> *mut u8 {
        self.memory
            .ptr()
            .wrapping_add(usize::from(bid) * self.size as usize)
    }

    /// The slot of the ring that `position` falls in.
    #[inline]
    fn slot(&self, position: u16) -> usize {
        usize::from(position & (self.count - 1))
    }

    /// The length the entry at `position` gives its buffer, which the kernel
    /// shortens when it fills the buffer with less than a whole one.
    #[inline]
    fn entry_len(&self, position: u16) -> usize {
        let entry = self
            .ring
            .ptr()
            .cast::<sys::io_uring_buf>()
            .wrapping_add(self.slot(position));
        let len = entry
            .wrapping_byte_add(offset_of!(sys::io_uring_buf, len))
            .cast::<u32>();
        // SAFETY: the length lies in the ring mapping, aligned. The kernel
        // writes it, if at all, before it posts the completion that reports
        // the entry taken, which is the only kind of entry read; an atomic
        // load keeps even a misbehaving kernel from racing the read.
        let len = unsafe { AtomicU32::from_ptr(len) }.load(Ordering::Relaxed);
        (len as usize).min(self.size as usize)
    }

    /// A lease of buffer `bid`, taken from the ring, holding `len` bytes.
    #[inline]
    fn lease_taken(self: &Rc<Self>, bid: u16, len: usize) -> BufLease {
        BufLease {
            pool: Rc::clone(self),
            bid,
            start: 0,
            len: len.min(self.size as usize),
            room: self.size as usize,
        }
    }

    /// The leases of the data a completion reports in incremental mode:
    /// `total` bytes from where the kernel left off in the buffer the
    /// completion names, on through the buffers after it in the ring, each
    /// from its start. The kernel keeps the last buffer to fill on if the
    /// completion says so (`IORING_CQE_F_BUF_MORE`), and is done with every
    /// other, even one it filled only in part, as it does when it takes all
    /// of a buffer before it reads; a buffer it is done with returns to the
    /// ring once its leases are gone. A completion without data that leaves
    /// the kernel the buffer takes nothing.
    ///
    /// # Panics
    ///
    /// If the data does not fit the buffers in the ring from the first one
    /// on, or the kernel keeps a buffer it filled.
    fn take_incremental(self: &Rc<Self>, flags: u32, total: usize) -> Vec<BufLease> {
        let first = (flags >> sys::IORING_CQE_BUFFER_SHIFT) as u16;
        assert!(
            first < self.count && self.in_ring[usize::from(first)].get(),
            "the kernel reported buffer {first}, which is not in the ring"
        );
        let kept = flags & sys::IORING_CQE_F_BUF_MORE != 0;
        let size = self.size as usize;
        // The first walk works out what of each buffer the data fills, and
        // checks that each slot still holds what the kernel took from it, a
        // buffer in the ring put there at that position, and that only the
        // first, which the kernel's head is at, was begun.
        let start = self.positions[usize::from(first)].get();
        let mut parts: Vec<(u16, usize, usize)> = Vec::new();
        let mut left = total;
        loop {
            let position = start.wrapping_add(parts.len() as u16);
            let bid = self.slots[self.slot(position)].get();
            let in_ring = self.in_ring[usize::from(bid)].get()
                && self.positions[usize::from(bid)].get() == position;
            assert!(
                in_ring,
                "the kernel reported {total} bytes from buffer {first}, more than its buffers \
                 after it in the ring hold"
            );
            let filled = self.filled[usize::from(bid)].get() as usize;
            assert!(
                parts.is_empty() || filled == 0,
                "the kernel filled on into buffer {bid}, which it had begun"
            );
            let len = left.min(size - filled);
            left -= len;
            parts.push((bid, filled, len));
            if left == 0 {
                break;
            }
        }
        let &(last, filled, len) = parts.last().expect("the walk takes a buffer");
        assert!(
            !kept || filled + len < size,
            "the kernel kept buffer {last}, which it filled"
        );
        // The second walk takes them. Taking puts spare buffers only in the
        // slot of the tail, as for a bundle, which is none of these.
        let count = parts.len();
        let mut leases = Vec::with_capacity(count);
        for (index, (bid, filled, len)) in parts.into_iter().enumerate() {
            let buffer = usize::from(bid);
            self.filled[buffer].set((filled + len) as u32);
            if len > 0 {
                self.shares[buffer].set(self.shares[buffer].get() + 1);
                leases.push(BufLease {
                    pool: Rc::clone(self),
                    bid,
                    start: filled,
                    len,
                    // The kernel may fill on past the data.
                    room: len,
                });
            }
            if index + 1 < count || !kept {
                self.take(bid);
                if self.shares[buffer].get() == 0 {
                    self.put(bid);
                }
            }
        }
        leases
    }

    /// Lets go of a lease of part of buffer `bid`, in incremental mode: the
    /// buffer returns to the ring once its last lease is gone, if the
    /// kernel is done with it.
    fn release(&self, bid: u16) {
        let shares = &self.shares[usize::from(bid)];
        shares.set(shares.get() - 1);
        if shares.get() == 0 && !self.in_ring[usize::from(bid)].get() {
            self.put(bid);
        }
    }

    /// Records that the kernel took buffer `bid` from the ring, as a
    /// completion reported. A position reported in order moves `low` on,
    /// past any reported before it, and slots it frees take spare buffers.
    ///
    /// # Panics
    ///
    /// If the buffer is not in the ring.
    fn take(&self, bid: u16) {
        assert!(
            bid < self.count && self.in_ring[usize::from(bid)].get(),
            "the kernel reported buffer {bid}, which is not in the ring"
        );
        self.in_ring[usize::from(bid)].set(false);
        self.available.set(self.available.get() - 1);
        let position = self.positions[usize::from(bid)].get();
        if position != self.low.get() {
            self.taken[self.slot(position)].set(true);
            return;
        }
        let mut low = position.wrapping_add(1);
        while low != self.tail.get() && self.taken[self.slot(low)].get() {
            self.taken[self.slot(low)].set(false);
            low = low.wrapping_add(1);
        }
        self.low.set(low);
        let mut published = 0;
        while self.has_free_slot() {
            let Some(bid) = self.spare.borrow_mut().pop() else {
                break;
            };
            self.publish(bid);
            published += 1;
        }
        self.wake_waiters(published);
    }

    /// Whether the slot of the tail can take a buffer: its last buffer has
    /// been reported taken.
    #[inline]
    fn has_free_slot(&self) -> bool {
        self.tail.get().wrapping_sub(self.low.get()) < self.count
    }

    /// Puts buffer `bid` in the ring for the kernel to pick, or keeps it
    /// spare until a slot is free.
    #[inline]
    fn put(&self, bid: u16) {
        if !self.has_free_slot() {
            self.spare.borrow_mut().push(bid);
            return;
        }
        self.publish(bid);
        self.wake_waiters(1);
    }

    /// Puts buffer `bid` in the slot of the tail, which must be free, and
    /// publishes it.
    #[inline]
    fn publish(&self, bid: u16) {
        debug_assert!(self.has_free_slot(), "no slot is free");
        let tail = self.tail.get();
        let slot = self.slot(tail);
        self.slots[slot].set(bid);
        self.positions[usize::from(bid)].set(tail);
        self.in_ring[usize::from(bid)].set(true);
        self.filled[usize::from(bid)].set(0);
        let entry = self
            .ring
            .ptr()
            .cast::<sys::io_uring_buf>()
            .wrapping_add(slot);
        // Field by field: in the first entry, `resv` is the ring's tail.
        let addr = entry.wrapping_byte_add(offset_of!(sys::io_uring_buf, addr));
        let len = entry.wrapping_byte_add(offset_of!(sys::io_uring_buf, len));
        let bid_field = entry.wrapping_byte_add(offset_of!(sys::io_uring_buf, bid));
        // SAFETY: the entry lies in the ring mapping. It is not one the kernel
        // may be reading: those lie between its head and the published tail,
        // and the slot is free, the position `count` before the tail having
        // been reported taken, so it lies behind the kernel's head.
        unsafe {
            addr.cast::<u64>()
                .write(self.buffer(bid).expose_provenance() as u64)
        };
        // SAFETY: as above.
        unsafe { len.cast::<u32>().write(self.size) };
        // SAFETY: as above.
        unsafe { bid_field.cast::<u16>().write(bid) };
        let tail = tail.wrapping_add(1);
        self.tail.set(tail);
        let tail_field = self
            .ring
            .ptr()
            .wrapping_byte_add(offset_of!(sys::io_uring_buf_ring, tail))
            .cast::<u16>();
        // SAFETY: the tail lies in the ring mapping, aligned, and the kernel
        // only reads it, with an acquire load, which this release store
        // pairs with to publish the entry.
        unsafe { AtomicU16::from_ptr(tail_field) }.store(tail, Ordering::Release);
        self.available.set(self.available.get() + 1);
    }

    /// Wakes the first `count` tasks waiting for a buffer, one for each
    /// buffer that returned to the ring.
    #[inline]
    fn wake_waiters(&self, count: usize) {
        for _ in 0..count {
            let waiter = self.waiters.borrow_mut().pop_front();
            match waiter {
                // Outside the borrow: a wake can run code that asks for a
                // buffer.
                Some(waiter) => waiter.wake(),
                None => break,
            }
        }
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        // Leases and operations keep the pool alive, so none is left and the
        // kernel will pick no more buffers. The memory is unmapped after this.
        if let Some(driver) = self.driver.upgrade() {
            let mut reg = sys::io_uring_buf_reg {
                bgid: self.group,
                ..Default::default()
            };
            // SAFETY: `reg` is one valid registration naming the group.
            let _ = unsafe {
                driver.register(
                    sys::IORING_UNREGISTER_PBUF_RING,
                    ptr::from_mut(&mut reg).cast(),
                    1,
                )
            };
        }
    }
}

impl fmt::Debug for BufPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufPool")
            .field("count", &self.count())
            .field("buffer_size", &self.buffer_size())
            .field("available", &self.available())
            .finish_non_exhaustive()
    }
}

/// Data the kernel put in a pooled buffer. The buffer returns to its
/// [`BufPool`] when the lease is dropped, or, if the kernel consumes the
/// pool's buffers incrementally, once the kernel is done with it and every
/// lease of a part of it is dropped.
///
/// A lease is also a buffer of its own, to write its data out, or read more
/// into, up to its [`capacity`](BufLease::capacity).
pub struct BufLease {
    pool: Rc<Pool>,
    bid: u16,
    /// Where the data starts in the buffer: past what earlier completions
    /// filled, in incremental mode.
    start: usize,
    len: usize,
    /// How much the lease may hold, from `start` on.
    room: usize,
}

impl BufLease {
    /// How many bytes of data the lease holds.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the lease holds no data.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// How much the lease can hold: the size of the buffer, or, for a part
    /// of a buffer that the kernel consumes incrementally, and may fill on
    /// past the part, only its data.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.room
    }

    /// Where the lease's room starts.
    #[inline]
    fn ptr(&self) -> *mut u8 {
        self.pool.buffer(self.bid).wrapping_add(self.start)
    }
}

impl Deref for BufLease {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        // SAFETY: the buffer is mapped for as long as the pool lives, which
        // the lease keeps alive, and all of it is initialized. While leased,
        // the lease's room is out of the kernel's reach: the buffer is out of
        // the ring, or, consumed incrementally, the kernel fills on only past
        // what it reported filled.
        unsafe { slice::from_raw_parts(self.ptr(), self.len) }
    }
}

impl DerefMut for BufLease {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: as in `deref`; the lease is its room's only user.
        unsafe { slice::from_raw_parts_mut(self.ptr(), self.len) }
    }
}

// SAFETY: the room is part of the pool's mapping, which the lease keeps
// alive and which does not move; only the lease and the operation holding
// it use it, as for `Deref`. All of it is initialized.
unsafe impl IoBuf for BufLease {
    #[inline]
    fn stable_ptr(&self) -> *const u8 {
        self.ptr()
    }

    #[inline]
    fn bytes_init(&self) -> usize {
        self.len
    }

    #[inline]
    fn bytes_total(&self) -> usize {
        self.capacity()
    }
}

// SAFETY: as for `IoBuf`; `set_init` only moves the length within the
// room, all of which is initialized.
unsafe impl IoBufMut for BufLease {
    #[inline]
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.ptr()
    }

    #[inline]
    unsafe fn set_init(&mut self, pos: usize) {
        self.len = self.len.max(pos.min(self.capacity()));
    }
}

impl Drop for BufLease {
    #[inline]
    fn drop(&mut self) {
        if self.pool.incremental {
            self.pool.release(self.bid);
        } else {
            self.pool.put(self.bid);
        }
    }
}

impl fmt::Debug for BufLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufLease")
            .field("buffer", &self.bid)
            .field("start", &self.start)
            .field("len", &self.len)
            .finish()
    }
}

/// The data of one receive into a pool: the leases of the buffers it filled,
/// in order, each full but the last.
///
/// A receive fills several buffers at once when more data is waiting than
/// one holds (a bundle, `IORING_RECVSEND_BUNDLE`). The leases return their
/// buffers to the pool when dropped, whether together or apart.
pub struct BufBundle {
    first: BufLease,
    rest: Vec<BufLease>,
}

impl BufBundle {
    /// How many bytes of data the bundle holds.
    #[inline]
    pub fn len(&self) -> usize {
        self.iter().map(BufLease::len).sum()
    }

    /// Whether the bundle holds no data.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// How many buffers the data fills.
    #[inline]
    pub fn buffers(&self) -> usize {
        1 + self.rest.len()
    }

    /// The leases, in order.
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &BufLease> {
        iter::once(&self.first).chain(&self.rest)
    }

    /// Lease `index`, counting from 0.
    ///
    /// # Panics
    ///
    /// If the bundle has no lease `index`.
    #[inline]
    pub(crate) fn lease(&self, index: usize) -> &BufLease {
        match index {
            0 => &self.first,
            _ => &self.rest[index - 1],
        }
    }

    /// Adds `other`'s leases after this bundle's, as for data that arrived
    /// after it.
    #[inline]
    pub fn append(&mut self, other: BufBundle) {
        self.rest.reserve(other.buffers());
        self.rest.push(other.first);
        self.rest.extend(other.rest);
    }

    /// The data, copied out into one vector.
    pub fn to_vec(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(self.len());
        for lease in self {
            data.extend_from_slice(lease);
        }
        data
    }
}

impl IntoIterator for BufBundle {
    type Item = BufLease;
    type IntoIter = iter::Chain<iter::Once<BufLease>, vec::IntoIter<BufLease>>;

    fn into_iter(self) -> Self::IntoIter {
        iter::once(self.first).chain(self.rest)
    }
}

impl<'a> IntoIterator for &'a BufBundle {
    type Item = &'a BufLease;
    type IntoIter = iter::Chain<iter::Once<&'a BufLease>, slice::Iter<'a, BufLease>>;

    fn into_iter(self) -> Self::IntoIter {
        iter::once(&self.first).chain(&self.rest)
    }
}

impl fmt::Debug for BufBundle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self).finish()
    }
}

/// The pool's bookkeeping, fed completions as the kernel could post them.
/// No request reads the ring meanwhile, so the kernel never sees the takes
/// these tests make up.
#[cfg(test)]
mod tests {
    use super::*;

    /// The flags of a completion that names buffer `bid`.
    fn buffer(bid: u16) -> u32 {
        sys::IORING_CQE_F_BUFFER | (u32::from(bid) << sys::IORING_CQE_BUFFER_SHIFT)
    }

    fn bids(bundle: &BufBundle) -> Vec<u16> {
        bundle.iter().map(|lease| lease.bid).collect()
    }

    /// A take reported ahead of an earlier one holds its slot until the
    /// earlier one is reported too, so that a buffer returned meanwhile waits
    /// spare rather than overwriting what a bundle may still need to read.
    #[test]
    fn slots_are_reused_only_once_every_take_before_them_is_reported() {
        let driver = Driver::new().unwrap();
        let pool = BufPool::new(&driver, 4, 8).unwrap();
        let state = &*pool.pool;

        // The kernel took the buffers at positions 0 and 1, and the second
        // take is reported first.
        let second = pool.lease(buffer(1), 8).unwrap();
        assert_eq!(state.low.get(), 0);
        drop(second);
        assert_eq!(*state.spare.borrow(), [1]);
        assert_eq!(pool.available(), 3);

        let first = pool.lease(buffer(0), 8).unwrap();
        assert_eq!(state.low.get(), 2);
        assert!(state.spare.borrow().is_empty());
        assert_eq!(state.positions[1].get(), 4, "back in the ring after 3");
        assert_eq!(pool.available(), 3);

        // A bundle from buffer 2 over three slots takes 2, 3 and then 1.
        let bundle = pool.bundle(buffer(2), 20).unwrap();
        assert_eq!(bids(&bundle), [2, 3, 1]);
        let lens: Vec<usize> = bundle.iter().map(BufLease::len).collect();
        assert_eq!(lens, [8, 8, 4]);
        assert_eq!(state.low.get(), 5);
        assert_eq!(pool.available(), 0);
        drop((first, bundle));
        assert_eq!(pool.available(), 4);
    }

    /// The kernel shortens the last buffer it maps for a receive in its
    /// entry. Were the receive to get less data, that buffer would stay in
    /// the ring shortened, and the next bundle, starting at it, would fill it
    /// only that far: the bundle's lengths follow the entries.
    #[test]
    fn bundles_follow_the_lengths_the_kernel_left_in_the_ring() {
        let driver = Driver::new().unwrap();
        let pool = BufPool::new(&driver, 4, 8).unwrap();
        let state = &*pool.pool;
        let len = state
            .ring
            .ptr()
            .cast::<sys::io_uring_buf>()
            .wrapping_byte_add(offset_of!(sys::io_uring_buf, len))
            .cast::<u32>();
        // SAFETY: the entry lies in the ring mapping, and no request reads it.
        unsafe { len.write(3) };
        let bundle = pool.bundle(buffer(0), 11).unwrap();
        let lens: Vec<usize> = bundle.iter().map(BufLease::len).collect();
        assert_eq!(lens, [3, 8]);
    }

    #[test]
    fn appended_bundles_keep_their_order() {
        let driver = Driver::new().unwrap();
        let pool = BufPool::new(&driver, 4, 8).unwrap();
        let mut data = pool.bundle(buffer(0), 12).unwrap();
        data.append(pool.bundle(buffer(2), 5).unwrap());
        assert_eq!(bids(&data), [0, 1, 2]);
        let lens: Vec<usize> = data.iter().map(BufLease::len).collect();
        assert_eq!(lens, [8, 4, 5]);
        assert_eq!((data.len(), data.buffers()), (17, 3));
    }

    #[test]
    fn a_completion_without_data_takes_one_buffer() {
        let driver = Driver::new().unwrap();
        let pool = BufPool::new(&driver, 4, 8).unwrap();
        let bundle = pool.bundle(buffer(0), -libc::ECONNRESET).unwrap();
        assert_eq!((bids(&bundle), bundle.len()), (vec![0], 0));
        assert!(pool.bundle(0, 0).is_none(), "no buffer reported");
    }

    #[test]
    #[should_panic(expected = "not in the ring")]
    fn a_leased_buffer_cannot_be_reported_again() {
        let driver = Driver::new().unwrap();
        let pool = BufPool::new(&driver, 4, 8).unwrap();
        let _lease = pool.lease(buffer(0), 8);
        let _again = pool.lease(buffer(0), 8);
    }

    /// From position 2, the ring holds two buffers: data for three would
    /// run into a slot past the tail.
    #[test]
    #[should_panic(expected = "more than its buffers after it in the ring hold")]
    fn a_bundle_cannot_run_past_the_tail() {
        let driver = Driver::new().unwrap();
        let pool = BufPool::new(&driver, 4, 8).unwrap();
        let _bundle = pool.bundle(buffer(2), 17);
    }

    /// The flags of a completion that names buffer `bid`, which the kernel
    /// keeps to fill on.
    fn kept(bid: u16) -> u32 {
        buffer(bid) | sys::IORING_CQE_F_BUF_MORE
    }

    /// A pool whose buffers the kernel consumes incrementally, if it can.
    fn incremental(count: u16, size: u32) -> Option<(Driver, BufPool)> {
        if !Probe::current().unwrap().supports_incremental_buffers() {
            eprintln!("skipped: the kernel does not consume buffers incrementally");
            return None;
        }
        let driver = Driver::new().unwrap();
        let pool = BufPool::incremental(&driver, count, size).unwrap();
        Some((driver, pool))
    }

    /// Completions that fill a buffer bit by bit share it, each lease its
    /// own part; the buffer leaves the ring once full, and returns to it,
    /// from its start, once the last lease is gone.
    #[test]
    fn parts_of_a_buffer_share_it_until_it_is_full() {
        let Some((_driver, pool)) = incremental(2, 16) else {
            return;
        };
        let base = pool.pool.buffer(0);
        let first = pool.lease(kept(0), 5).unwrap();
        let second = pool.lease(kept(0), 5).unwrap();
        assert_eq!((first.ptr(), first.len()), (base, 5));
        assert_eq!((second.ptr(), second.len()), (base.wrapping_add(5), 5));
        assert_eq!(pool.available(), 2, "the kernel fills on into the buffer");
        let last = pool.lease(buffer(0), 6).unwrap();
        assert_eq!(last.ptr(), base.wrapping_add(10));
        assert_eq!(pool.available(), 1);
        drop((first, last));
        assert_eq!(pool.available(), 1, "a part of it is still leased");
        drop(second);
        assert_eq!(pool.available(), 2);
        assert_eq!(pool.pool.positions[0].get(), 2);
        assert_eq!(pool.pool.filled[0].get(), 0);
    }

    /// A bundle carries on from where the kernel left off in its first
    /// buffer, through whole buffers after it.
    #[test]
    fn a_bundle_carries_on_where_the_last_completion_left_off() {
        let Some((_driver, pool)) = incremental(4, 8) else {
            return;
        };
        let first = pool.bundle(kept(0), 3).unwrap();
        let bundle = pool.bundle(kept(0), 15).unwrap();
        assert_eq!(bids(&bundle), [0, 1, 2]);
        let parts: Vec<(usize, usize)> = bundle
            .iter()
            .map(|lease| (lease.start, lease.len()))
            .collect();
        assert_eq!(parts, [(3, 5), (0, 8), (0, 2)]);
        assert_eq!(pool.available(), 2, "the kernel is done with 0 and 1");
        drop(bundle);
        assert_eq!(pool.available(), 3);
        drop(first);
        assert_eq!(pool.available(), 4);
    }

    /// Without data, a completion that leaves the kernel its buffer takes
    /// nothing. One that does not is done with the buffer, however little
    /// it filled, as when the kernel took all of it before it read.
    #[test]
    fn the_kernel_says_whether_it_keeps_the_buffer() {
        let Some((_driver, pool)) = incremental(2, 16) else {
            return;
        };
        assert!(pool.lease(kept(0), 0).is_none());
        assert_eq!(pool.available(), 2);
        let short = pool.lease(buffer(0), 3).unwrap();
        assert_eq!(pool.available(), 1);
        drop(short);
        assert_eq!(pool.available(), 2);
        assert!(pool.lease(buffer(1), 0).is_none());
        assert_eq!(pool.available(), 2, "returned at once");
        assert_eq!(pool.pool.positions[1].get(), 3);
    }

    /// The kernel may fill on past a lease of a part of a buffer, so its
    /// room is only its data.
    #[test]
    fn a_part_of_a_buffer_has_room_only_for_its_data() {
        let Some((_driver, pool)) = incremental(2, 16) else {
            return;
        };
        let mut part = pool.lease(kept(0), 4).unwrap();
        assert_eq!((part.capacity(), part.bytes_total()), (4, 4));
        // SAFETY: the whole buffer is initialized.
        unsafe { part.set_init(12) };
        assert_eq!(part.len(), 4);
    }

    #[test]
    #[should_panic(expected = "which it filled")]
    fn the_kernel_cannot_keep_a_buffer_it_filled() {
        let Some((_driver, pool)) = incremental(2, 8) else {
            panic!("skipped: which it filled");
        };
        let _ = pool.lease(kept(0), 8);
    }
}
