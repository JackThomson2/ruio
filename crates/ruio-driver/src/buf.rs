//! Buffers that operations hand to the kernel.
//!
//! The buffer traits, [`IoBuf`], [`IoBufMut`] and [`IoBufs`], with
//! [`Slice`], [`BufResult`] and [`AlignedBuf`], are those of `ruio-buf`,
//! which every engine shares, re-exported here; its documentation says how
//! they make completion-based I/O sound.
//!
//! The rest are the ring's own. [`FixedBuffers`] are registered with a ring
//! once, for reads and writes that skip mapping the memory on every
//! operation. A [`BufPool`] holds buffers the kernel picks from when data
//! arrives, so waiting readers pin no memory, and hands the data back as a
//! [`BufLease`], or a [`BufBundle`] of them, which a send takes as
//! [`IoBufs`]. Direct I/O (`O_DIRECT`) needs its memory aligned, as an
//! [`AlignedBuf`]'s is, or a registered buffer's whose size is a multiple of
//! the page size.

pub use ruio_buf::{AlignedBuf, BufResult, IoBuf, IoBufMut, IoBufs, Slice};

pub use crate::fixed::{FixedBuf, FixedBuffers, FixedIoBuf};
pub use crate::pool::{BufBundle, BufLease, BufPool};
