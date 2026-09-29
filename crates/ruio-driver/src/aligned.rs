//! Buffers aligned for direct I/O.

use std::alloc::{self, Layout};
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::slice;

use crate::buf::{IoBuf, IoBufMut};

/// A heap buffer whose memory starts at a multiple of an alignment, as
/// direct I/O (`O_DIRECT`) needs: the file's `dio_mem_align`, commonly 512 or
/// 4096 bytes, which [`Statx`](crate::ops::Statx) reports with
/// `STATX_DIOALIGN`.
///
/// It is a byte vector of fixed capacity: a read fills it up to its
/// capacity, and a write sends the bytes it holds. Direct I/O also needs the
/// length and file offset of each transfer to be multiples of the file's
/// `dio_offset_align`; the buffer leaves those to the caller.
pub struct AlignedBuf {
    ptr: NonNull<u8>,
    len: usize,
    layout: Layout,
}

impl AlignedBuf {
    /// An empty buffer of `capacity` bytes, starting at a multiple of
    /// `align`.
    ///
    /// # Panics
    ///
    /// If `align` is not a power of two, or `capacity` rounded up to it
    /// exceeds `isize::MAX`; and if the allocation fails, as `Vec`'s does.
    pub fn with_capacity(capacity: usize, align: usize) -> Self {
        let layout = Layout::from_size_align(capacity, align)
            .expect("an alignment that is a power of two, and a capacity that fits");
        let ptr = if capacity == 0 {
            // A dangling pointer, aligned, for no memory at all.
            NonNull::new(align as *mut u8)
                .expect("an alignment is not zero")
                .cast()
        } else {
            // SAFETY: the layout has a size above zero.
            let ptr = unsafe { alloc::alloc(layout) };
            NonNull::new(ptr).unwrap_or_else(|| alloc::handle_alloc_error(layout))
        };
        Self {
            ptr,
            len: 0,
            layout,
        }
    }

    /// A buffer of `len` zero bytes, starting at a multiple of `align`.
    ///
    /// # Panics
    ///
    /// As for [`with_capacity`](AlignedBuf::with_capacity).
    pub fn zeroed(len: usize, align: usize) -> Self {
        let mut buf = Self::with_capacity(len, align);
        buf.resize(len, 0);
        buf
    }

    /// How many bytes the buffer can hold.
    pub fn capacity(&self) -> usize {
        self.layout.size()
    }

    /// The alignment the buffer's memory starts at.
    pub fn alignment(&self) -> usize {
        self.layout.align()
    }

    /// Empties the buffer, keeping its memory.
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Shortens the buffer to `len` bytes; a longer `len` changes nothing.
    pub fn truncate(&mut self, len: usize) {
        self.len = self.len.min(len);
    }

    /// Appends `data`.
    ///
    /// # Panics
    ///
    /// If the data does not fit in the buffer's capacity.
    pub fn extend_from_slice(&mut self, data: &[u8]) {
        let end = self.len + data.len();
        assert!(
            end <= self.capacity(),
            "the data does not fit in the buffer"
        );
        let tail = self.ptr.as_ptr().wrapping_add(self.len);
        // SAFETY: the buffer's memory has room for `end` bytes, and `data`
        // cannot overlap it, as the buffer is borrowed mutably.
        unsafe { tail.copy_from_nonoverlapping(data.as_ptr(), data.len()) };
        self.len = end;
    }

    /// Makes the buffer `len` bytes long, filling new bytes with `value`.
    ///
    /// # Panics
    ///
    /// If `len` exceeds the buffer's capacity.
    pub fn resize(&mut self, len: usize, value: u8) {
        assert!(len <= self.capacity(), "the length exceeds the capacity");
        if len > self.len {
            let tail = self.ptr.as_ptr().wrapping_add(self.len);
            // SAFETY: the buffer's memory has room for `len` bytes.
            unsafe { tail.write_bytes(value, len - self.len) };
        }
        self.len = len;
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        if self.layout.size() > 0 {
            // SAFETY: the memory came from `alloc` with this layout.
            unsafe { alloc::dealloc(self.ptr.as_ptr(), self.layout) };
        }
    }
}

impl Deref for AlignedBuf {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        // SAFETY: the first `len` bytes are initialized, and the buffer owns
        // them.
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl DerefMut for AlignedBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: as for `deref`, borrowed mutably.
        unsafe { slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl fmt::Debug for AlignedBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AlignedBuf")
            .field("len", &self.len)
            .field("capacity", &self.capacity())
            .field("alignment", &self.alignment())
            .finish()
    }
}

// SAFETY: the buffer owns its memory outright, as a `Vec<u8>` does.
unsafe impl Send for AlignedBuf {}

// SAFETY: as for `Send`; shared access only reads.
unsafe impl Sync for AlignedBuf {}

// SAFETY: the memory lives on the heap, which moving the buffer leaves in
// place, and its first `len` of `capacity` bytes are initialized.
unsafe impl IoBuf for AlignedBuf {
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

// SAFETY: as for `IoBuf`; `set_init` extends the length only as far as the
// caller vouches for.
unsafe impl IoBufMut for AlignedBuf {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    unsafe fn set_init(&mut self, pos: usize) {
        self.len = self.len.max(pos);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_starts_at_the_alignment() {
        for align in [1, 512, 4096, 1 << 16] {
            let buf = AlignedBuf::with_capacity(3 * align, align);
            assert_eq!(buf.stable_ptr().addr() % align, 0);
            assert_eq!(
                (buf.capacity(), buf.alignment(), buf.len()),
                (3 * align, align, 0)
            );
        }
    }

    #[test]
    fn it_fills_like_a_vector_of_fixed_capacity() {
        let mut buf = AlignedBuf::with_capacity(8, 8);
        buf.extend_from_slice(b"abc");
        buf.resize(5, b'-');
        assert_eq!(&*buf, b"abc--");
        buf[0] = b'A';
        buf.truncate(2);
        assert_eq!(&*buf, b"Ab");
        buf.clear();
        assert!(buf.is_empty());
        assert_eq!(&*AlignedBuf::zeroed(4, 4), [0; 4]);
    }

    #[test]
    #[should_panic(expected = "does not fit")]
    fn it_does_not_grow() {
        AlignedBuf::with_capacity(4, 4).extend_from_slice(b"12345");
    }

    #[test]
    fn it_can_hold_nothing() {
        let buf = AlignedBuf::with_capacity(0, 4096);
        assert_eq!((buf.capacity(), buf.stable_ptr().addr() % 4096), (0, 0));
        assert!(buf.is_empty());
    }

    #[test]
    #[should_panic(expected = "power of two")]
    fn alignments_are_powers_of_two() {
        AlignedBuf::with_capacity(8, 3);
    }
}
