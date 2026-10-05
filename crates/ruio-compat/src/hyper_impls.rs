//! The I/O traits of hyper, so that hyper serves an adapter as it is, with
//! nothing between them.

use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use hyper::rt::{Read, ReadBufCursor, Write};

use crate::{Adaptable, Compat};

impl<S: Adaptable> Read for Compat<S> {
    /// Copies what the adapter has received into hyper's buffer, as much as
    /// it takes, receiving first if there is none.
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        let room = buf.remaining();
        ready!(
            self.get_mut()
                .poll_copy(cx, room, |data| buf.put_slice(data))
        )?;
        Poll::Ready(Ok(()))
    }
}

impl<S: Adaptable> Write for Compat<S> {
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
    /// takes at once, and sends them together, so hyper hands a response's
    /// head and body over at once.
    fn is_write_vectored(&self) -> bool {
        true
    }

    /// Ready at once, with a failure of the writes before if one is known:
    /// the adapter holds nothing back.
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().poll_flush(cx)
    }

    /// Waits for the write in flight, then shuts down the writing half of
    /// a connection.
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().poll_close(cx)
    }
}
