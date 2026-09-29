//! Buffers that operations hand to the kernel.
//!
//! With completion-based I/O the kernel uses a buffer after the call that
//! started the operation has returned, so an operation takes its buffer by
//! value and hands it back with the result, as a [`BufResult`]. [`IoBuf`] and
//! [`IoBufMut`] are the promises that make this sound: a buffer's memory stays
//! where it is while an operation holds the buffer, however the buffer value
//! moves.
//!
//! A read fills a buffer from its start, up to
//! [`bytes_total`](IoBuf::bytes_total), and marks what it read as
//! initialized. A write sends a buffer's initialized bytes. [`IoBuf::slice`]
//! narrows either to part of a buffer: `vec.slice(vec.len()..)` reads into a
//! vector's spare capacity, after its contents.
//!
//! [`FixedBuffers`] are registered with a ring once, for reads and writes
//! that skip mapping the memory on every operation. A [`BufPool`] holds
//! buffers the kernel picks from when data arrives, so waiting readers pin
//! no memory, and hands the data back as a [`BufLease`].

use std::fmt;
use std::io;
use std::ops::{Bound, RangeBounds};

pub use crate::fixed::{FixedBuf, FixedBuffers, FixedIoBuf};
pub use crate::pool::{BufBundle, BufLease, BufPool};

/// The result of an operation that held a buffer, and the buffer.
pub type BufResult<T, B> = (io::Result<T>, B);

/// A buffer the kernel may read from while an operation holds it.
///
/// # Safety
///
/// `stable_ptr` must point to `bytes_total` bytes, the first `bytes_init` of
/// them initialized. That memory must stay valid and in place, and must not be
/// changed except by the operation, from when an operation takes the buffer
/// until it hands the buffer back, even while the buffer value moves. Memory
/// on the heap, or `'static` memory, qualifies; memory inside the value does
/// not.
pub unsafe trait IoBuf: 'static {
    /// A pointer to the start of the buffer.
    fn stable_ptr(&self) -> *const u8;

    /// How many bytes, from the start, hold data.
    fn bytes_init(&self) -> usize;

    /// How many bytes the buffer can hold.
    fn bytes_total(&self) -> usize;

    /// Narrows the buffer to `range`, measured from its start and reaching
    /// at most to [`bytes_total`](IoBuf::bytes_total).
    ///
    /// # Panics
    ///
    /// If the range ends before it starts, or beyond `bytes_total`.
    fn slice(self, range: impl RangeBounds<usize>) -> Slice<Self>
    where
        Self: Sized,
    {
        Slice::new(self, range)
    }
}

/// A buffer the kernel may write into while an operation holds it.
///
/// # Safety
///
/// As for [`IoBuf`]. In addition, `stable_mut_ptr` must point to the memory
/// `stable_ptr` does, writable for `bytes_total` bytes, and `set_init` must
/// never mark more bytes initialized than it is told to.
pub unsafe trait IoBufMut: IoBuf {
    /// A pointer to the start of the buffer, for writing.
    fn stable_mut_ptr(&mut self) -> *mut u8;

    /// Records that the first `pos` bytes hold data, if that is more than
    /// [`bytes_init`](IoBuf::bytes_init) says already.
    ///
    /// # Safety
    ///
    /// The first `pos` bytes must be initialized, and `pos` must be at most
    /// [`bytes_total`](IoBuf::bytes_total).
    unsafe fn set_init(&mut self, pos: usize);
}

// SAFETY: a vector's contents live on the heap, which moving the vector
// leaves in place. `len` of its `capacity` bytes are initialized.
unsafe impl IoBuf for Vec<u8> {
    fn stable_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }

    fn bytes_total(&self) -> usize {
        self.capacity()
    }
}

// SAFETY: as for `IoBuf`; `set_init` extends the length only as far as the
// caller vouches for.
unsafe impl IoBufMut for Vec<u8> {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.as_mut_ptr()
    }

    unsafe fn set_init(&mut self, pos: usize) {
        if pos > self.len() {
            // SAFETY: the caller guarantees that the first `pos` bytes are
            // initialized and that `pos` is within the capacity.
            unsafe { self.set_len(pos) };
        }
    }
}

// SAFETY: a boxed slice lives on the heap, all of it initialized.
unsafe impl IoBuf for Box<[u8]> {
    fn stable_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }

    fn bytes_total(&self) -> usize {
        self.len()
    }
}

// SAFETY: as for `IoBuf`; every byte is initialized already.
unsafe impl IoBufMut for Box<[u8]> {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.as_mut_ptr()
    }

    unsafe fn set_init(&mut self, _pos: usize) {}
}

// SAFETY: `'static` memory never moves, and a shared reference keeps it from
// changing.
unsafe impl IoBuf for &'static [u8] {
    fn stable_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }

    fn bytes_total(&self) -> usize {
        self.len()
    }
}

// SAFETY: as for `&'static [u8]`.
unsafe impl IoBuf for &'static str {
    fn stable_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }

    fn bytes_total(&self) -> usize {
        self.len()
    }
}

// SAFETY: a string's contents live on the heap, as for `Vec<u8>`.
unsafe impl IoBuf for String {
    fn stable_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }

    fn bytes_total(&self) -> usize {
        self.capacity()
    }
}

/// Part of a buffer, which operations treat as a buffer of its own. Made by
/// [`IoBuf::slice`].
///
/// A slice measures itself against its buffer each time it is used, so it
/// never reaches past the buffer, even if the buffer shrinks through
/// [`Slice::get_mut`].
pub struct Slice<B> {
    buf: B,
    begin: usize,
    end: usize,
}

impl<B: IoBuf> Slice<B> {
    fn new(buf: B, range: impl RangeBounds<usize>) -> Self {
        let total = buf.bytes_total();
        let begin = match range.start_bound() {
            Bound::Included(&begin) => begin,
            Bound::Excluded(&begin) => begin.checked_add(1).expect("slice start overflows"),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&end) => end.checked_add(1).expect("slice end overflows"),
            Bound::Excluded(&end) => end,
            Bound::Unbounded => total,
        };
        assert!(begin <= end, "slice starts at {begin} but ends at {end}");
        assert!(
            end <= total,
            "slice ends at {end}, beyond the buffer's {total} bytes"
        );
        Self { buf, begin, end }
    }

    /// Where the slice starts in the buffer.
    pub fn begin(&self) -> usize {
        self.begin
    }

    /// Where the slice ends in the buffer.
    pub fn end(&self) -> usize {
        self.end
    }

    /// The whole buffer.
    pub fn get_ref(&self) -> &B {
        &self.buf
    }

    /// The whole buffer.
    pub fn get_mut(&mut self) -> &mut B {
        &mut self.buf
    }

    /// The whole buffer, back.
    pub fn into_inner(self) -> B {
        self.buf
    }

    /// The slice's bounds, clamped to the buffer as it is now.
    fn bounds(&self) -> (usize, usize) {
        let total = self.buf.bytes_total();
        (self.begin.min(total), self.end.min(total))
    }
}

// SAFETY: the slice covers memory inside its buffer's, which is valid and in
// place for as long as the buffer's is, by the buffer's own promise; the
// bounds are clamped to the buffer as it is when asked.
unsafe impl<B: IoBuf> IoBuf for Slice<B> {
    fn stable_ptr(&self) -> *const u8 {
        self.buf.stable_ptr().wrapping_add(self.bounds().0)
    }

    fn bytes_init(&self) -> usize {
        let (begin, end) = self.bounds();
        self.buf.bytes_init().clamp(begin, end) - begin
    }

    fn bytes_total(&self) -> usize {
        let (begin, end) = self.bounds();
        end - begin
    }
}

// SAFETY: as for `IoBuf`. `set_init` marks the buffer initialized up to the
// end of what the caller vouches for, after zeroing any bytes between the
// buffer's data and the slice, so it never counts memory that was not
// written.
unsafe impl<B: IoBufMut> IoBufMut for Slice<B> {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        let (begin, _) = self.bounds();
        self.buf.stable_mut_ptr().wrapping_add(begin)
    }

    unsafe fn set_init(&mut self, pos: usize) {
        if pos == 0 {
            return;
        }
        let (begin, _) = self.bounds();
        let init = self.buf.bytes_init();
        if init < begin {
            let gap = self.buf.stable_mut_ptr().wrapping_add(init);
            // SAFETY: the bytes from `init` to `begin` lie inside the
            // buffer, which is writable there, and nothing else uses them.
            unsafe { gap.write_bytes(0, begin - init) };
        }
        // SAFETY: everything before `begin` is initialized, having held data
        // or just been zeroed, and the caller vouches for the `pos` bytes
        // from `begin`, which lie inside the buffer.
        unsafe { self.buf.set_init(begin + pos) };
    }
}

impl<B: IoBuf> fmt::Debug for Slice<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Slice")
            .field("begin", &self.begin)
            .field("end", &self.end)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pretends the kernel wrote `data` at the start of `buf`.
    fn fill<B: IoBufMut>(buf: &mut B, data: &[u8]) {
        assert!(data.len() <= buf.bytes_total());
        let ptr = buf.stable_mut_ptr();
        // SAFETY: `data` fits in the buffer's writable memory.
        unsafe { ptr.copy_from_nonoverlapping(data.as_ptr(), data.len()) };
        // SAFETY: the first `data.len()` bytes were just written.
        unsafe { buf.set_init(data.len()) };
    }

    #[test]
    fn vectors_report_contents_and_capacity() {
        let mut buf = Vec::with_capacity(8);
        buf.extend_from_slice(b"ab");
        assert_eq!((buf.bytes_init(), buf.bytes_total()), (2, 8));
        fill(&mut buf, b"xyz");
        assert_eq!(buf, b"xyz");
        // Marking less than is initialized never shrinks a vector.
        fill(&mut buf, b"q");
        assert_eq!(buf, b"qyz");
    }

    #[test]
    fn a_slice_of_the_spare_capacity_appends() {
        let mut buf = Vec::with_capacity(8);
        buf.extend_from_slice(b"ab");
        let mut slice = buf.slice(2..);
        assert_eq!((slice.bytes_init(), slice.bytes_total()), (0, 6));
        fill(&mut slice, b"cd");
        assert_eq!(slice.into_inner(), b"abcd");
    }

    #[test]
    fn slices_of_the_contents_see_their_part() {
        let slice = b"hello world".to_vec().slice(6..);
        assert_eq!((slice.bytes_init(), slice.bytes_total()), (5, 5));
        let nested = slice.slice(..2);
        assert_eq!((nested.bytes_init(), nested.bytes_total()), (2, 2));
        let bytes = nested.stable_ptr();
        // SAFETY: the slice's two initialized bytes.
        assert_eq!(unsafe { std::slice::from_raw_parts(bytes, 2) }, b"wo");
    }

    #[test]
    fn a_gap_before_a_slice_is_zeroed_when_it_fills() {
        let mut slice = Vec::with_capacity(8).slice(3..6);
        fill(&mut slice, b"xy");
        assert_eq!(slice.into_inner(), [0, 0, 0, b'x', b'y']);
    }

    #[test]
    fn slices_shrink_with_their_buffer() {
        let mut slice = Vec::<u8>::with_capacity(64).slice(10..40);
        assert_eq!(slice.bytes_total(), 30);
        slice.get_mut().shrink_to(20);
        let total = slice.get_ref().capacity();
        assert!(total < 40);
        assert_eq!(slice.bytes_total(), total.saturating_sub(10));
    }

    #[test]
    #[should_panic(expected = "beyond the buffer")]
    fn slices_cannot_reach_past_the_buffer() {
        let _ = Vec::<u8>::with_capacity(4).slice(..5);
    }

    #[test]
    fn other_buffers() {
        let boxed: Box<[u8]> = Box::new(*b"abc");
        assert_eq!((boxed.bytes_init(), boxed.bytes_total()), (3, 3));
        assert_eq!(
            ("static".bytes_init(), (b"static" as &[u8]).bytes_total()),
            (6, 6)
        );
        let string = String::from("text");
        assert_eq!(string.bytes_init(), 4);
    }
}
