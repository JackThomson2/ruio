//! Registered buffers: memory the kernel maps once, so that reads and writes
//! into it skip mapping it on every operation.

use std::cell::RefCell;
use std::fmt;
use std::io;
use std::ops::{Deref, DerefMut};
use std::ptr::{self, NonNull};
use std::rc::Rc;
use std::slice;

use ruio_sys as sys;

use crate::Driver;
use crate::buf::{IoBuf, IoBufMut, Slice};
use crate::driver::WeakDriver;
use crate::mem::Mapping;

/// A buffer inside memory registered with a ring, which the fixed operations
/// ([`ReadFixed`](crate::ops::ReadFixed),
/// [`WriteFixed`](crate::ops::WriteFixed)) name by its registration index.
///
/// The kernel checks that an operation's memory lies inside the registered
/// buffer it names, so a wrong index, or a buffer registered with another
/// ring, fails the operation with `EFAULT`.
pub trait FixedIoBuf: IoBuf {
    /// The index of the registered buffer that contains this memory.
    fn buf_index(&self) -> u16;
}

/// Buffers registered with a ring (`IORING_REGISTER_BUFFERS`).
///
/// Each buffer is checked out as a [`FixedBuf`] and returns to the set when
/// dropped. The memory stays registered and mapped until the set and every
/// buffer checked out of it are gone, including buffers held by operations
/// whose futures were dropped, so the kernel never writes to memory that has
/// been freed. A ring has room for one set at a time.
#[derive(Clone)]
pub struct FixedBuffers {
    registry: Rc<Registry>,
}

struct Registry {
    driver: WeakDriver,
    memory: Mapping,
    size: usize,
    /// Whether each buffer is checked out.
    checked_out: RefCell<Vec<bool>>,
}

impl FixedBuffers {
    /// Maps `count` buffers of `size` bytes each, zeroed, and registers them
    /// with the driver's ring.
    ///
    /// # Errors
    ///
    /// `EBUSY` if the ring already has registered buffers, `ENOMEM` if the
    /// memory would exceed `RLIMIT_MEMLOCK`, `InvalidInput` if `count` or
    /// `size` is zero, and what `mmap` reports.
    pub fn new(driver: &Driver, count: u16, size: usize) -> io::Result<Self> {
        let len = usize::from(count)
            .checked_mul(size)
            .filter(|&len| len > 0)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "registered buffers need a count and size above zero",
                )
            })?;
        let memory = Mapping::anonymous(len)?;
        let mut iovecs: Vec<libc::iovec> = (0..usize::from(count))
            .map(|index| libc::iovec {
                iov_base: memory.ptr().wrapping_add(index * size).cast(),
                iov_len: size,
            })
            .collect();
        // SAFETY: the iovecs describe `count` buffers inside `memory`, which
        // the registry keeps mapped until it unregisters them. Only buffers
        // checked out of the registry, and operations holding them, touch
        // the memory, and each keeps the registry alive.
        unsafe {
            driver.register(
                sys::IORING_REGISTER_BUFFERS,
                iovecs.as_mut_ptr().cast(),
                u32::from(count),
            )
        }?;
        Ok(Self {
            registry: Rc::new(Registry {
                driver: driver.downgrade(),
                memory,
                size,
                checked_out: RefCell::new(vec![false; usize::from(count)]),
            }),
        })
    }

    /// The number of buffers.
    pub fn count(&self) -> usize {
        self.registry.checked_out.borrow().len()
    }

    /// The size of each buffer.
    pub fn buffer_size(&self) -> usize {
        self.registry.size
    }

    /// Checks out buffer `index`, unless it is checked out already or does
    /// not exist.
    pub fn check_out(&self, index: u16) -> Option<FixedBuf> {
        let mut checked_out = self.registry.checked_out.borrow_mut();
        let slot = checked_out.get_mut(usize::from(index))?;
        if *slot {
            return None;
        }
        *slot = true;
        let offset = usize::from(index) * self.registry.size;
        let ptr = NonNull::new(self.registry.memory.ptr().wrapping_add(offset))
            .expect("a mapping is not at address zero");
        Some(FixedBuf {
            registry: Rc::clone(&self.registry),
            index,
            ptr,
            len: 0,
        })
    }

    /// Checks out any buffer that is free.
    pub fn take(&self) -> Option<FixedBuf> {
        let free = self
            .registry
            .checked_out
            .borrow()
            .iter()
            .position(|&out| !out)?;
        self.check_out(u16::try_from(free).expect("at most u16::MAX buffers"))
    }
}

impl Drop for Registry {
    fn drop(&mut self) {
        // Every buffer keeps the registry alive, so none is checked out and
        // no operation holds one. The memory is unmapped after this.
        if let Some(driver) = self.driver.upgrade() {
            // SAFETY: unregistering takes no memory.
            let _ = unsafe { driver.register(sys::IORING_UNREGISTER_BUFFERS, ptr::null_mut(), 0) };
        }
    }
}

impl fmt::Debug for FixedBuffers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FixedBuffers")
            .field("count", &self.count())
            .field("buffer_size", &self.buffer_size())
            .finish_non_exhaustive()
    }
}

/// A registered buffer, checked out of its [`FixedBuffers`], which it
/// returns to when dropped.
///
/// Like a vector of fixed capacity, it holds [`len`](FixedBuf::len) bytes of
/// data in [`capacity`](FixedBuf::capacity) bytes. All of its memory is
/// initialized, since it starts zeroed, so any length up to the capacity is
/// allowed.
pub struct FixedBuf {
    registry: Rc<Registry>,
    index: u16,
    ptr: NonNull<u8>,
    len: usize,
}

impl FixedBuf {
    /// The buffer's registration index.
    pub fn index(&self) -> u16 {
        self.index
    }

    /// How many bytes the buffer holds.
    pub fn capacity(&self) -> usize {
        self.registry.size
    }

    /// How many bytes of the buffer are data.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the buffer holds no data.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Sets how many bytes are data.
    ///
    /// # Panics
    ///
    /// If `len` exceeds the capacity.
    pub fn set_len(&mut self, len: usize) {
        assert!(len <= self.capacity(), "{len} exceeds the capacity");
        self.len = len;
    }

    /// Empties the buffer.
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Appends `data`.
    ///
    /// # Panics
    ///
    /// If it does not fit in the capacity.
    pub fn extend_from_slice(&mut self, data: &[u8]) {
        let (start, end) = (self.len, self.len + data.len());
        assert!(end <= self.capacity(), "{end} bytes exceed the capacity");
        self.whole_mut()[start..end].copy_from_slice(data);
        self.len = end;
    }

    fn whole_mut(&mut self) -> &mut [u8] {
        // SAFETY: the buffer's `capacity` bytes are mapped for as long as the
        // registry lives, which this buffer keeps alive. All of it is
        // initialized, and while the buffer is checked out nothing else uses
        // it: operations holding it have it by value.
        unsafe { slice::from_raw_parts_mut(self.ptr.as_ptr(), self.capacity()) }
    }
}

impl Deref for FixedBuf {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        // SAFETY: as in `whole_mut`, for the first `len` bytes.
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl DerefMut for FixedBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        let len = self.len;
        &mut self.whole_mut()[..len]
    }
}

// SAFETY: the buffer's memory is part of the registry's mapping, which the
// buffer keeps alive and which does not move; `len` of its `capacity` bytes
// are data, and all are initialized.
unsafe impl IoBuf for FixedBuf {
    fn stable_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len
    }

    fn bytes_total(&self) -> usize {
        self.capacity()
    }
}

// SAFETY: as for `IoBuf`; the memory is writable, and `set_init` only moves
// the length, which may be anything up to the capacity.
unsafe impl IoBufMut for FixedBuf {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    unsafe fn set_init(&mut self, pos: usize) {
        self.len = self.len.max(pos.min(self.capacity()));
    }
}

impl FixedIoBuf for FixedBuf {
    fn buf_index(&self) -> u16 {
        self.index
    }
}

impl<B: FixedIoBuf> FixedIoBuf for Slice<B> {
    fn buf_index(&self) -> u16 {
        self.get_ref().buf_index()
    }
}

impl Drop for FixedBuf {
    fn drop(&mut self) {
        self.registry.checked_out.borrow_mut()[usize::from(self.index)] = false;
    }
}

impl fmt::Debug for FixedBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FixedBuf")
            .field("index", &self.index)
            .field("len", &self.len)
            .field("capacity", &self.capacity())
            .finish()
    }
}
