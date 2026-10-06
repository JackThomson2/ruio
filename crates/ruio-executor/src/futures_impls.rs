//! The `Stream` and `FusedStream` traits of futures-core, for the channels'
//! receivers and for intervals, so that they compose with the combinators of
//! futures-util and with everything else that takes a `Stream`.
//!
//! Each implementation is one call to the type's own `poll_recv` or
//! `poll_tick`, which the compiler inlines: the trait costs nothing over the
//! method, and the cooperative budget the method spends is spent as before.
//! A receiver is terminated once a receive has found its channel ended, as
//! `poll_recv` gives `None` from then on; a broadcast receiver once every
//! sender is gone and it has read every value; an interval never is.

use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Instant;

use futures_core::stream::{FusedStream, Stream};

use crate::sync::{broadcast, local, mpsc};
use crate::time::Interval;

impl<T> Stream for mpsc::Receiver<T> {
    type Item = T;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.get_mut().poll_recv(cx)
    }
}

/// Terminated once a receive has found every sender gone and every value
/// taken.
impl<T> FusedStream for mpsc::Receiver<T> {
    #[inline]
    fn is_terminated(&self) -> bool {
        self.is_ended()
    }
}

impl<T> Stream for mpsc::UnboundedReceiver<T> {
    type Item = T;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.get_mut().poll_recv(cx)
    }
}

/// Terminated once a receive has found the channel ended: every sender
/// gone, or the channel closed, and every value taken.
impl<T> FusedStream for mpsc::UnboundedReceiver<T> {
    #[inline]
    fn is_terminated(&self) -> bool {
        self.is_ended()
    }
}

impl<T> Stream for local::mpsc::Receiver<T> {
    type Item = T;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.get_mut().poll_recv(cx)
    }
}

/// Terminated once a receive has found every sender gone and every value
/// taken.
impl<T> FusedStream for local::mpsc::Receiver<T> {
    #[inline]
    fn is_terminated(&self) -> bool {
        self.is_ended()
    }
}

impl<T> Stream for local::mpsc::UnboundedReceiver<T> {
    type Item = T;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.get_mut().poll_recv(cx)
    }
}

/// Terminated once a receive has found the channel ended: every sender
/// gone, or the channel closed, and every value taken.
impl<T> FusedStream for local::mpsc::UnboundedReceiver<T> {
    #[inline]
    fn is_terminated(&self) -> bool {
        self.is_ended()
    }
}

/// Each item is a value, or [`broadcast::RecvError::Lagged`] where the
/// receiver fell behind and missed some; the stream ends, rather than
/// yielding [`broadcast::RecvError::Closed`], once every sender is gone and
/// every value has been read.
impl<T: Clone> Stream for broadcast::Receiver<T> {
    type Item = Result<T, broadcast::RecvError>;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(match ready!(self.get_mut().poll_recv(cx)) {
            Err(broadcast::RecvError::Closed) => None,
            received => Some(received),
        })
    }
}

/// Terminated once every sender is gone and every value has been read.
impl<T: Clone> FusedStream for broadcast::Receiver<T> {
    #[inline]
    fn is_terminated(&self) -> bool {
        // The senders first: a value sent before the last one went is seen
        // by the look that follows, so a channel found closed and empty has
        // nothing more coming.
        self.is_closed() && self.is_empty()
    }
}

/// Each item is a tick's deadline; the stream never ends.
impl Stream for Interval {
    type Item = Instant;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Instant>> {
        self.get_mut().poll_tick(cx).map(Some)
    }
}

/// Never terminated: the ticks never end.
impl FusedStream for Interval {
    #[inline]
    fn is_terminated(&self) -> bool {
        false
    }
}
