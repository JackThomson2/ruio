//! Writes that nothing waits for: a stream's `write_behind`.

use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, ready};

use ruio_buf::IoBufs;

use crate::behind::Behind;
use crate::gather::Gather;
use crate::handle::Raw;
use crate::op::{Detached, Started, detach};
use crate::sys;

use super::vectored::start_send_vectored;
use super::{Socket, TcpStream, outcome};

/// The error of a send that stopped short without one of its own: that of a
/// write to a pipe whose other end has gone.
const BROKEN: i32 = sys::ERROR_BROKEN_PIPE as i32;

/// A send left to finish behind a stream, which records how it went.
struct SendBehind<V> {
    gather: Gather<V>,
    /// How many bytes it has to send.
    len: usize,
    behind: Rc<Behind>,
    /// The stream's socket, which stays open until the send has finished,
    /// though the stream be dropped first.
    socket: Rc<Socket>,
}

impl<V: IoBufs> SendBehind<V> {
    /// Records how the send went, having `sent` as many bytes or failed,
    /// and lets go of what it held. Tasks are woken only if `wake`.
    fn finish(self, sent: io::Result<u32>, wake: bool) {
        let error = match sent {
            Ok(sent) if sent as usize == self.len => None,
            // The system sends all of what it takes, short of an error.
            Ok(_) => Some(BROKEN),
            Err(err) => Some(err.raw_os_error().unwrap_or(BROKEN)),
        };
        let Self {
            gather,
            behind,
            socket,
            ..
        } = self;
        // The buffers go, and the socket closes if its stream has gone,
        // before those waiting learn that the send has finished.
        drop(gather);
        drop(socket);
        behind.finished(error, wake);
    }
}

impl<V: IoBufs> Detached for SendBehind<V> {
    unsafe fn complete(self, overlapped: *const sys::Overlapped, bytes: u32, wake: bool) {
        // SAFETY: the caller's promise for the `OVERLAPPED`, and the socket
        // is open, as the send holds it.
        let sent = unsafe { outcome(self.socket.raw(), overlapped, bytes) };
        self.finish(sent, wake);
    }

    fn done(self, bytes: u32) {
        self.finish(Ok(bytes), true);
    }
}

/// Starts a send left behind a stream, as `op`'s `Start` says.
///
/// # Safety
///
/// As for `Start`.
unsafe fn start_send_behind<V: IoBufs>(
    socket: sys::Handle,
    send: &mut SendBehind<V>,
    overlapped: *mut sys::Overlapped,
) -> io::Result<Started> {
    // SAFETY: the caller's promise.
    unsafe { start_send_vectored(socket, &mut send.gather, overlapped) }
}

/// The future of [`TcpStream::write_behind`]: buffers written to a stream
/// without waiting for the system to be done with them.
///
/// The send goes to the system as it is, all the buffers at once, and the
/// engine drops them once the system is done with them. The future
/// resolves once the send is on its way: at once, unless an earlier write
/// behind is still in flight, as writes to a stream go one at a time, in
/// order, or the port is due a turn, when it yields to it first.
/// It fails with the error an earlier write behind failed with, or
/// the one the system refused this send with, and the buffers are then
/// dropped unsent; the stream's writes after it fail with that error too.
///
/// A stream dropped with the send in flight closes once the send has
/// finished. The engine, as it is dropped, cancels the send.
///
/// A write behind takes less than 4 GiB, which is what one send takes;
/// more fails with [`io::ErrorKind::InvalidInput`], and nothing is sent.
#[must_use = "futures do nothing unless polled"]
pub struct WriteBehind<'a, V: IoBufs> {
    stream: &'a TcpStream,
    /// Taken once the send is on its way.
    bufs: Option<V>,
}

impl<'a, V: IoBufs> WriteBehind<'a, V> {
    pub(super) fn new(stream: &'a TcpStream, bufs: V) -> Self {
        Self {
            stream,
            bufs: Some(bufs),
        }
    }
}

impl<V: IoBufs> Future for WriteBehind<'_, V> {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let behind = &this.stream.behind;
        ready!(behind.poll_done(cx))?;
        // A send that the system's buffer takes is finished as it starts,
        // so a task that only writes behind would never let the port be
        // turned: it waits for the turn once enough have.
        let target = this.stream.stream.target();
        if target.defers(cx) {
            return Poll::Pending;
        }
        let bufs = this
            .bufs
            .take()
            .expect("`WriteBehind` polled after it completed");
        let len = bufs.bytes_init();
        if len == 0 {
            return Poll::Ready(Ok(()));
        }
        if len > u32::MAX as usize {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a write behind takes less than 4 GiB",
            )));
        }
        let send = SendBehind {
            gather: Gather { bufs, done: 0 },
            len,
            behind: Rc::clone(behind),
            socket: Rc::clone(this.stream.stream.io()),
        };
        behind.started();
        let Err((err, send)) = detach(target, send, 0, start_send_behind::<V>) else {
            return Poll::Ready(Ok(()));
        };
        // The system refused the send, which leaves the stream broken as a
        // send that fails later does.
        drop(send);
        behind.finished(Some(err.raw_os_error().unwrap_or(BROKEN)), true);
        Poll::Ready(Err(err))
    }
}

// Nothing is pinned: the buffers are only ever moved.
impl<V: IoBufs> Unpin for WriteBehind<'_, V> {}

impl<V: IoBufs> fmt::Debug for WriteBehind<'_, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteBehind")
            .field("behind", &self.stream.behind)
            .finish_non_exhaustive()
    }
}

/// The future of [`TcpStream::flush`]: waits for the writes left behind a
/// stream to finish, and resolves with the error one failed with, if one
/// did.
#[must_use = "futures do nothing unless polled"]
#[derive(Debug)]
pub struct Flush<'a> {
    behind: &'a Behind,
}

impl<'a> Flush<'a> {
    pub(super) fn new(behind: &'a Behind) -> Self {
        Self { behind }
    }
}

impl Future for Flush<'_> {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.behind.poll_done(cx)
    }
}
