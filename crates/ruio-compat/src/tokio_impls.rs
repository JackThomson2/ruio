//! The traits of `tokio::io`.

use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite, ReadBuf};

use crate::{Compat, StreamSocket};

impl<S: StreamSocket> AsyncRead for Compat<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let data = ready!(this.poll_fill_buf(cx))?;
        let len = data.len().min(buf.remaining());
        buf.put_slice(&data[..len]);
        this.consume(len);
        Poll::Ready(Ok(()))
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

    /// Yes: a write copies all the buffers it is given, up to what it
    /// takes at once, and sends them together.
    fn is_write_vectored(&self) -> bool {
        true
    }

    /// Ready at once, with a failure of the writes before if one is known:
    /// the adapter holds nothing back.
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().poll_flush(cx)
    }

    /// Waits for the write in flight, then shuts down the writing half of
    /// the connection, after which writes fail with
    /// [`io::ErrorKind::BrokenPipe`].
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().poll_close(cx)
    }
}
