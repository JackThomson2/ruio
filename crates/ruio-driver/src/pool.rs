//! Provided-buffer pools: buffers the kernel picks for reads and receives
//! when data arrives.

use std::cell::Cell;
use std::fmt;
use std::io;
use std::mem::offset_of;
use std::ops::{Deref, DerefMut};
use std::ptr;
use std::rc::Rc;
use std::slice;
use std::sync::atomic::{AtomicU16, Ordering};

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
/// [`BufLease`], which returns its buffer to the pool when dropped. When every
/// buffer is leased, reads into the pool fail with `ENOBUFS`.
///
/// The pool stays registered and mapped until it, every lease, and every
/// operation reading into it are gone, so the kernel never writes to memory
/// that has been freed.
#[derive(Clone)]
pub struct BufPool {
    pool: Rc<Pool>,
}

struct Pool {
    driver: WeakDriver,
    group: u16,
    /// The ring of buffer descriptors the kernel takes buffers from.
    ring: Mapping,
    /// The buffers, one after another.
    memory: Mapping,
    count: u16,
    size: u32,
    /// The ring's tail as the pool last published it.
    tail: Cell<u16>,
    /// Buffers in the ring, which the kernel may pick.
    available: Cell<usize>,
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
        let pool = Pool {
            driver: driver.downgrade(),
            group: driver.next_buffer_group(),
            ring,
            memory,
            count,
            size,
            tail: Cell::new(0),
            available: Cell::new(0),
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

    /// The buffer group ID that submission entries select from.
    pub(crate) fn group(&self) -> u16 {
        self.pool.group
    }

    /// The lease of the buffer a completion reports, if it reports one.
    pub(crate) fn lease(&self, flags: u32, len: usize) -> Option<BufLease> {
        if flags & sys::IORING_CQE_F_BUFFER == 0 {
            return None;
        }
        let bid = (flags >> sys::IORING_CQE_BUFFER_SHIFT) as u16;
        assert!(bid < self.pool.count, "the kernel reported buffer {bid}");
        self.pool.available.set(self.pool.available.get() - 1);
        Some(BufLease {
            pool: Rc::clone(&self.pool),
            bid,
            len: len.min(self.buffer_size()),
        })
    }
}

impl Pool {
    fn buffer(&self, bid: u16) -> *mut u8 {
        self.memory
            .ptr()
            .wrapping_add(usize::from(bid) * self.size as usize)
    }

    /// Puts buffer `bid` in the ring for the kernel to pick.
    fn put(&self, bid: u16) {
        debug_assert!(
            self.available.get() < usize::from(self.count),
            "the ring is full"
        );
        let tail = self.tail.get();
        let entry = self
            .ring
            .ptr()
            .cast::<sys::io_uring_buf>()
            .wrapping_add(usize::from(tail & (self.count - 1)));
        // Field by field: in the first entry, `resv` is the ring's tail.
        let addr = entry.wrapping_byte_add(offset_of!(sys::io_uring_buf, addr));
        let len = entry.wrapping_byte_add(offset_of!(sys::io_uring_buf, len));
        let bid_field = entry.wrapping_byte_add(offset_of!(sys::io_uring_buf, bid));
        // SAFETY: the entry lies in the ring mapping. The pool puts back only
        // buffers it holds, so at most `count` are in the ring, and this slot
        // is not one the kernel may be reading: those lie between its head
        // and the published tail.
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
