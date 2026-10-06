//! The `Stream` and `FusedStream` traits of futures-core, for the streams of
//! connections, data, directory entries, readiness and signals, so that they
//! compose with the combinators of futures-util and with everything else
//! that takes a `Stream`.
//!
//! Each implementation is one call to the type's own `poll_next`, which the
//! compiler inlines: the trait costs nothing over the method, and what the
//! method does between items, such as a directory reader's yield between
//! batches, it does through the trait too. A stream that never ends, of
//! connections, of readiness or of signals, says it is never terminated; one
//! that ends says so once it has.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::stream::{FusedStream, Stream};
use ruio_driver::buf::{BufBundle, BufLease};

use crate::fs::{DirEntry, ReadDir, ReadStream};
use crate::io::Readiness;
use crate::net::{Incoming, RecvStream};
use crate::signal::Signal;

/// Each item is a connection, or an error the kernel reported instead; the
/// stream never ends.
impl<S> Stream for Incoming<'_, S> {
    type Item = io::Result<S>;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Incoming::poll_next(self.get_mut(), cx)
    }
}

/// Never terminated: the connections never end.
impl<S> FusedStream for Incoming<'_, S> {
    #[inline]
    fn is_terminated(&self) -> bool {
        false
    }
}

impl Stream for RecvStream<'_> {
    type Item = io::Result<BufBundle>;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        RecvStream::poll_next(self.get_mut(), cx)
    }
}

/// Terminated once the peer has shut down or an error has been yielded.
impl FusedStream for RecvStream<'_> {
    #[inline]
    fn is_terminated(&self) -> bool {
        self.is_ended()
    }
}

impl Stream for ReadStream<'_> {
    type Item = io::Result<BufLease>;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        ReadStream::poll_next(self.get_mut(), cx)
    }
}

/// Terminated once the file has ended or an error has been yielded.
impl FusedStream for ReadStream<'_> {
    #[inline]
    fn is_terminated(&self) -> bool {
        self.is_ended()
    }
}

/// Each item is an entry, or the error that stopped the reading; the stream
/// ends at the end of the directory, and after an error.
impl Stream for ReadDir {
    type Item = io::Result<DirEntry>;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().poll_next_entry(cx).map(Result::transpose)
    }
}

/// Terminated once every entry has been taken.
impl FusedStream for ReadDir {
    #[inline]
    fn is_terminated(&self) -> bool {
        self.is_ended()
    }
}

/// Each item is the events the descriptor was woken for, or the error that
/// ended the request, after which it is armed again; the stream never ends.
impl Stream for Readiness<'_> {
    type Item = io::Result<u32>;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Readiness::poll_next(self.get_mut(), cx).map(Some)
    }
}

/// Never terminated: the descriptor's readiness never ends.
impl FusedStream for Readiness<'_> {
    #[inline]
    fn is_terminated(&self) -> bool {
        false
    }
}

/// Each item is an arrival of the signal, however many arrived together;
/// the stream never ends.
impl Stream for Signal {
    type Item = ();

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<()>> {
        self.get_mut().poll_recv(cx)
    }
}

/// Never terminated: the signal may always arrive again.
impl FusedStream for Signal {
    #[inline]
    fn is_terminated(&self) -> bool {
        false
    }
}
