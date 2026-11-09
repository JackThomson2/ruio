//! The buffers of `ruio::buf` that every engine shares.

use ruio::buf::{AlignedBuf, IoBuf, IoBufMut, IoBufs};

#[test]
fn a_slice_of_the_spare_capacity_reads_after_the_contents() {
    let mut buf = Vec::with_capacity(8);
    buf.extend_from_slice(b"ab");
    let mut spare = buf.slice(2..);
    assert_eq!((spare.bytes_init(), spare.bytes_total()), (0, 6));
    let ptr = spare.stable_mut_ptr();
    // SAFETY: two bytes fit in the slice's writable memory.
    unsafe { ptr.copy_from_nonoverlapping(b"cd".as_ptr(), 2) };
    // SAFETY: the first two bytes were just written.
    unsafe { spare.set_init(2) };
    assert_eq!(spare.into_inner(), b"abcd");
}

#[test]
fn several_buffers_send_as_one() {
    let bufs = [b"ab".to_vec(), b"cde".to_vec()];
    assert_eq!((bufs.buf_count(), bufs.bytes_init()), (2, 5));
}

#[test]
fn an_aligned_buffer_starts_at_its_alignment() {
    let buf = AlignedBuf::with_capacity(8192, 4096);
    assert_eq!(buf.stable_ptr().addr() % 4096, 0);
    assert_eq!(buf.bytes_total(), 8192);
}
