//! The traits of `tokio::io`.

use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite, ReadBuf};

use crate::{Adaptable, Compat};

impl<S: Adaptable> AsyncRead for Compat<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let room = buf.remaining();
        ready!(
            self.get_mut()
                .poll_copy(cx, room, |data| buf.put_slice(data))
        )?;
        Poll::Ready(Ok(()))
    }
}

impl<S: Adaptable> AsyncBufRead for Compat<S> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        self.get_mut().poll_fill_buf(cx)
    }

    fn consume(self: Pin<&mut Self>, amount: usize) {
        self.get_mut().consume(amount);
    }
}

impl<S: Adaptable> AsyncWrite for Compat<S> {
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
    /// a connection, after which writes fail with
    /// [`io::ErrorKind::BrokenPipe`]; a file or a descriptor has no half to
    /// shut, and stays open for more.
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().poll_close(cx)
    }
}
