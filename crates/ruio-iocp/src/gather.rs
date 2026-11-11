//! Several buffers sent as one: which of their bytes are left to send.

use ruio_buf::IoBufs;

/// Buffers that are sent as one stream of bytes, in as many sends as that
/// takes, and how many of the bytes the sends so far took.
pub(crate) struct Gather<V> {
    pub(crate) bufs: V,
    /// How many bytes, counted from the start of the first buffer, have
    /// been sent.
    pub(crate) done: usize,
}

impl<V: IoBufs> Gather<V> {
    /// Calls `each` with the parts of the buffers that are left to send, in
    /// order: where a part starts, and how many bytes it has, which is
    /// never none. The parts hold `limit` bytes between them at most, the
    /// last of them cut short to that.
    pub(crate) fn rest(&self, limit: usize, mut each: impl FnMut(*const u8, usize)) {
        let mut skip = self.done;
        let mut left = limit;
        for index in 0..self.bufs.buf_count() {
            if left == 0 {
                break;
            }
            let (ptr, len) = self.bufs.buf(index);
            if skip >= len {
                skip -= len;
                continue;
            }
            let part = (len - skip).min(left);
            // Within the buffer, which holds more than `skip` bytes.
            each(ptr.wrapping_add(skip), part);
            left -= part;
            skip = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::slice;

    use super::*;

    const BUFS: [&[u8]; 5] = [b"abc", b"", b"defg", b"h", b""];

    /// The parts of [`BUFS`] left to send after `done` bytes, `limit` bytes
    /// of them at most, as the bytes they hold.
    fn rest(done: usize, limit: usize) -> Vec<Vec<u8>> {
        let gather = Gather {
            bufs: BUFS.to_vec(),
            done,
        };
        let mut parts = Vec::new();
        gather.rest(limit, |ptr, len| {
            // SAFETY: a part lies within one of the buffers, which are
            // constants.
            parts.push(unsafe { slice::from_raw_parts(ptr, len) }.to_vec());
        });
        parts
    }

    #[test]
    fn what_is_left_starts_where_the_sends_stopped() {
        assert_eq!(rest(0, usize::MAX), [&b"abc"[..], b"defg", b"h"]);
        assert_eq!(rest(2, usize::MAX), [&b"c"[..], b"defg", b"h"]);
        assert_eq!(rest(3, usize::MAX), [&b"defg"[..], b"h"]);
        assert_eq!(rest(5, usize::MAX), [&b"fg"[..], b"h"]);
        assert_eq!(rest(7, usize::MAX), [&b"h"[..]]);
        assert!(rest(8, usize::MAX).is_empty());
        assert!(rest(100, usize::MAX).is_empty());
    }

    #[test]
    fn a_send_takes_no_more_than_its_limit() {
        assert_eq!(rest(0, 3), [&b"abc"[..]]);
        assert_eq!(rest(0, 5), [&b"abc"[..], b"de"]);
        assert_eq!(rest(1, 6), [&b"bc"[..], b"defg"]);
        assert_eq!(rest(1, 7), [&b"bc"[..], b"defg", b"h"]);
        assert_eq!(rest(4, 1), [&b"e"[..]]);
        assert!(rest(0, 0).is_empty());
    }
}
