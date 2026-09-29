//! Provided-buffer pools: buffers the kernel picks for reads and receives
//! when data arrives.

use std::cell::{Cell, RefCell};
use std::fmt;
use std::io;
use std::iter;
use std::mem::{self, offset_of};
use std::ops::{Deref, DerefMut};
use std::ptr;
use std::rc::Rc;
use std::slice;
use std::sync::atomic::{AtomicU16, AtomicU32, Ordering};
use std::task::{Context, Poll, Waker};
use std::vec;

use ruio_sys as sys;

use crate::Driver;
use crate::buf::{IoBuf, IoBufMut};
use crate::driver::WeakDriver;
use crate::mem::Mapping;

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
    /// Tasks waiting for a buffer to return to the ring.
    waiters: RefCell<Vec<Waker>>,
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
            waiters: RefCell::new(Vec::new()),
        };
        for bid in 0..count {
            pool.put(bid);
        }
        let mut reg = sys::io_uring_buf_reg {
            ring_addr: pool.ring.ptr().expose_provenance() as u64,
            ring_entries: u32::from(count),
            bgid: pool.group,
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
    pub fn count(&self) -> usize {
        usize::from(self.pool.count)
    }

    /// The size of each buffer.
    pub fn buffer_size(&self) -> usize {
        self.pool.size as usize
    }

    /// The number of buffers the kernel may pick, which are not leased.
    pub fn available(&self) -> usize {
        self.pool.available.get()
    }

    /// Whether a buffer is available; if not, arranges for the task to be
    /// woken when one returns.
    pub fn poll_available(&self, cx: &mut Context<'_>) -> Poll<()> {
        if self.available() > 0 {
            return Poll::Ready(());
        }
        let mut waiters = self.pool.waiters.borrow_mut();
        if !waiters.iter().any(|waiter| waiter.will_wake(cx.waker())) {
            waiters.push(cx.waker().clone());
        }
        Poll::Pending
    }

    /// The buffer group ID that submission entries select from.
    pub(crate) fn group(&self) -> u16 {
        self.pool.group
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
        let pool = &self.pool;
        let first = (flags >> sys::IORING_CQE_BUFFER_SHIFT) as u16;
        assert!(
            first < pool.count && pool.in_ring[usize::from(first)].get(),
            "the kernel reported buffer {first}, which is not in the ring"
        );
        let mut position = pool.positions[usize::from(first)].get();
        let mut left = usize::try_from(res).unwrap_or(0);
        // Walk the slots from the first buffer's, as the kernel filled them,
        // each to the length its entry holds: the kernel shortens the last
        // buffer of a bundle to the data it expects. Every buffer is read
        // before any is taken, since taking one can let spare buffers into
        // slots.
        let mut taken: Vec<(u16, usize)> = Vec::new();
        loop {
            // The slot still holds what the kernel took from it if that is
            // in the ring, put there at this position.
            let bid = pool.slots[pool.slot(position)].get();
            let in_ring = pool.in_ring[usize::from(bid)].get()
                && pool.positions[usize::from(bid)].get() == position;
            assert!(
                in_ring,
                "the kernel reported {res} bytes from buffer {first}, more than its buffers \
                 after it in the ring hold"
            );
            let len = left.min(pool.entry_len(position));
            taken.push((bid, len));
            left -= len;
            if left == 0 {
                break;
            }
            position = position.wrapping_add(1);
        }
        let mut leases = taken.into_iter().map(|(bid, len)| {
            pool.take(bid);
            pool.lease_taken(bid, len)
        });
        let first = leases.next()?;
        Some(BufBundle {
            first,
            rest: leases.collect(),
        })
    }
}

impl Pool {
    fn buffer(&self, bid: u16) -> *mut u8 {
        self.memory
            .ptr()
            .wrapping_add(usize::from(bid) * self.size as usize)
    }

    /// The slot of the ring that `position` falls in.
    fn slot(&self, position: u16) -> usize {
        usize::from(position & (self.count - 1))
    }

    /// The length the entry at `position` gives its buffer, which the kernel
    /// shortens when it fills the buffer with less than a whole one.
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
    fn lease_taken(self: &Rc<Self>, bid: u16, len: usize) -> BufLease {
        BufLease {
            pool: Rc::clone(self),
            bid,
            len: len.min(self.size as usize),
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
        let mut published = false;
        while self.has_free_slot() {
            let Some(bid) = self.spare.borrow_mut().pop() else {
                break;
            };
            self.publish(bid);
            published = true;
        }
        if published {
            self.wake_waiters();
        }
    }

    /// Whether the slot of the tail can take a buffer: its last buffer has
    /// been reported taken.
    fn has_free_slot(&self) -> bool {
        self.tail.get().wrapping_sub(self.low.get()) < self.count
    }

    /// Puts buffer `bid` in the ring for the kernel to pick, or keeps it
    /// spare until a slot is free.
    fn put(&self, bid: u16) {
        if !self.has_free_slot() {
            self.spare.borrow_mut().push(bid);
            return;
        }
        self.publish(bid);
        self.wake_waiters();
    }

    /// Puts buffer `bid` in the slot of the tail, which must be free, and
    /// publishes it.
    fn publish(&self, bid: u16) {
        debug_assert!(self.has_free_slot(), "no slot is free");
        let tail = self.tail.get();
        let slot = self.slot(tail);
        self.slots[slot].set(bid);
        self.positions[usize::from(bid)].set(tail);
        self.in_ring[usize::from(bid)].set(true);
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

    fn wake_waiters(&self) {
        let waiters = mem::take(&mut *self.waiters.borrow_mut());
        for waiter in waiters {
            waiter.wake();
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
/// [`BufPool`] when the lease is dropped.
///
/// A lease is also a buffer of its own, to write its data out, or read more
/// into.
pub struct BufLease {
    pool: Rc<Pool>,
    bid: u16,
    len: usize,
}

impl BufLease {
    /// How many bytes of data the lease holds.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the lease holds no data.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The size of the buffer.
    pub fn capacity(&self) -> usize {
        self.pool.size as usize
    }
}

impl Deref for BufLease {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        // SAFETY: the buffer is mapped for as long as the pool lives, which
        // the lease keeps alive, and all of it is initialized. While leased it
        // is out of the ring, so the kernel does not touch it.
        unsafe { slice::from_raw_parts(self.pool.buffer(self.bid), self.len) }
    }
}

impl DerefMut for BufLease {
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: as in `deref`; the lease is the buffer's only user.
        unsafe { slice::from_raw_parts_mut(self.pool.buffer(self.bid), self.len) }
    }
}

// SAFETY: the buffer is part of the pool's mapping, which the lease keeps
// alive and which does not move; out of the ring, only the lease and the
// operation holding it use it. All of it is initialized.
unsafe impl IoBuf for BufLease {
    fn stable_ptr(&self) -> *const u8 {
        self.pool.buffer(self.bid)
    }

    fn bytes_init(&self) -> usize {
        self.len
    }

    fn bytes_total(&self) -> usize {
        self.capacity()
    }
}

// SAFETY: as for `IoBuf`; `set_init` only moves the length within the
// buffer, all of which is initialized.
unsafe impl IoBufMut for BufLease {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.pool.buffer(self.bid)
    }

    unsafe fn set_init(&mut self, pos: usize) {
        self.len = self.len.max(pos.min(self.capacity()));
    }
}

impl Drop for BufLease {
    fn drop(&mut self) {
        self.pool.put(self.bid);
    }
}

impl fmt::Debug for BufLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufLease")
            .field("buffer", &self.bid)
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
    pub fn len(&self) -> usize {
        self.iter().map(BufLease::len).sum()
    }

    /// Whether the bundle holds no data.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// How many buffers the data fills.
    pub fn buffers(&self) -> usize {
        1 + self.rest.len()
    }

    /// The leases, in order.
    pub fn iter(&self) -> impl Iterator<Item = &BufLease> {
        iter::once(&self.first).chain(&self.rest)
    }

    /// Lease `index`, counting from 0.
    ///
    /// # Panics
    ///
    /// If the bundle has no lease `index`.
    pub(crate) fn lease(&self, index: usize) -> &BufLease {
        match index {
            0 => &self.first,
            _ => &self.rest[index - 1],
        }
    }

    /// Adds `other`'s leases after this bundle's, as for data that arrived
    /// after it.
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
}
