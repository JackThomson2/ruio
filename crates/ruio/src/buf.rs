//! Buffers for I/O: what an operation takes, and hands back with its
//! result.
//!
//! An operation takes its buffer by value and hands it back with its
//! result, as a [`BufResult`], so that the system may use the buffer's
//! memory after the call that started the operation has returned. [`IoBuf`]
//! and [`IoBufMut`] are the promises that make this sound, which the
//! standard library's byte vectors, boxed slices and strings keep.
//! [`IoBuf::slice`] narrows a buffer to part of itself, as
//! `vec.slice(vec.len()..)` reads into a vector's spare capacity;
//! [`IoBufs`] sends several buffers as one; and an [`AlignedBuf`] serves
//! direct I/O. These are `ruio-buf`'s, the same on every engine.
#![cfg_attr(
    ruio_uring,
    doc = r#"
With io_uring, the ring's own buffers are here too. A [`BufPool`] holds
buffers the kernel picks from when data arrives, so that waiting receives
pin no memory, and hands the data back as a [`BufLease`], or a
[`BufBundle`] of them. [`FixedBuffers`] are registered with the ring once,
for reads and writes that skip mapping the memory on every operation, as
[`FixedBuf`]s, or any [`FixedIoBuf`]."#
)]
#![cfg_attr(
    ruio_iocp,
    doc = r#"
On the I/O completion port, the engine's pool is here too. A [`BufPool`]
holds buffers that a stream takes from once it has data to receive, so
that waiting receives hold no memory, and hands the data back as a
[`BufLease`], or a [`BufBundle`] of them, which a write takes as
[`IoBufs`]."#
)]

pub use ruio_buf::{AlignedBuf, BufResult, IoBuf, IoBufMut, IoBufs, Slice};

// The ring's own buffers.
#[cfg(ruio_uring)]
pub use ruio_driver::buf::{BufBundle, BufLease, BufPool, FixedBuf, FixedBuffers, FixedIoBuf};

// The completion port engine's own buffers.
#[cfg(ruio_iocp)]
pub use ruio_iocp::buf::{BufBundle, BufLease, BufPool};
