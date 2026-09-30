//! The traits of futures-io.

use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_io::{AsyncBufRead, AsyncRead, AsyncWrite};

use crate::{Compat, StreamSocket};

impl<S: StreamSocket> AsyncRead for Compat<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().poll_read(cx, buf)
    }
}

impl<S: StreamSocket> AsyncBufRead for Compat<S> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        self.get_mut().poll_fill_buf(cx)
    }

    fn consume(self: Pin<&mut Self>, amount: usize) {
        self.get_mut().consume(amount);
    }
}

impl<S: StreamSocket> AsyncWrite for Compat<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().poll_write(cx, &[IoSlice::new(buf)])
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().poll_write(cx, bufs)
    }

    /// Ready at once, with a failure of the writes before if one is known:
    /// the adapter holds nothing back.
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().poll_flush(cx)
    }

    /// Waits for the write in flight, then shuts down the writing half of
    /// the connection, after which writes fail with
    /// [`io::ErrorKind::BrokenPipe`].
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().poll_close(cx)
    }
}
