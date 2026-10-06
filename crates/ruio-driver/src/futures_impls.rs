//! The `Stream` and `FusedStream` traits of futures-core, for the streams of
//! multishot requests, so that they compose with the combinators of
//! futures-util and with everything else that takes a `Stream`.
//!
//! Each implementation is one call to the type's own `poll_next`, which the
//! compiler inlines: the trait costs nothing over the method. The streams
//! are `Unpin`, as nothing the kernel uses is inside them, so no pin
//! projection is needed. A stream that never ends says it is never
//! terminated; one that ends says so once it has, and [`MultiOp`] while its
//! request has ended, until it is re-armed.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::stream::{FusedStream, Stream};

use crate::buf::{BufBundle, BufLease};
use crate::ops::{AcceptStream, PollStream, ReadStream, RecvStream};
use crate::{Descriptor, MultiOp, Multishot};

impl<T: Multishot> Stream for MultiOp<T> {
    type Item = T::Item;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T::Item>> {
        MultiOp::poll_next(self.get_mut(), cx)
    }
}

/// Terminated while the request has ended, until [`MultiOp::rearm`]
/// submits it again.
impl<T: Multishot> FusedStream for MultiOp<T> {
    #[inline]
    fn is_terminated(&self) -> bool {
        self.is_ended()
    }
}

impl Stream for AcceptStream {
    type Item = io::Result<Descriptor>;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        AcceptStream::poll_next(self.get_mut(), cx).map(Some)
    }
}

/// Never terminated: the stream never ends.
impl FusedStream for AcceptStream {
    #[inline]
    fn is_terminated(&self) -> bool {
        false
    }
}

impl Stream for RecvStream {
    type Item = io::Result<BufBundle>;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        RecvStream::poll_next(self.get_mut(), cx)
    }
}

/// Terminated once the peer has shut down or an error has been yielded.
impl FusedStream for RecvStream {
    #[inline]
    fn is_terminated(&self) -> bool {
        self.is_ended()
    }
}

impl Stream for ReadStream {
    type Item = io::Result<BufLease>;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        ReadStream::poll_next(self.get_mut(), cx)
    }
}

/// Terminated once the file has ended or an error has been yielded.
impl FusedStream for ReadStream {
    #[inline]
    fn is_terminated(&self) -> bool {
        self.is_ended()
    }
}

impl Stream for PollStream {
    type Item = io::Result<u32>;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        PollStream::poll_next(self.get_mut(), cx).map(Some)
    }
}

/// Never terminated: the stream never ends.
impl FusedStream for PollStream {
    #[inline]
    fn is_terminated(&self) -> bool {
        false
    }
}
