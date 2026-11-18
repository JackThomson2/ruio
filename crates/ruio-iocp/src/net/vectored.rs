//! Several buffers written to a stream as one.

use std::fmt;
use std::future::Future;
use std::io;
use std::mem;
use std::pin::Pin;
use std::ptr;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use ruio_buf::{BufResult, IoBufs};

use crate::behind::{Behind, poll_gate};
use crate::gather::Gather;
use crate::handle::Target;
use crate::op::{Limit, Op, Started};
use crate::sys;

use super::{outcome, socket_of, taken};

/// How many buffers a send names without allocating room for their names.
const FEW: usize = 16;

/// A `WSABUF` that names nothing.
const EMPTY: sys::WsaBuf = sys::WsaBuf {
    len: 0,
    buf: ptr::null_mut(),
};

/// Starts a send of what is left of `gather`'s buffers, as `op`'s `Start`
/// says: all of it, short of 4 GiB, as a completion counts the bytes it
/// sent in 32 bits.
///
/// # Safety
///
/// As for `Start`.
pub(super) unsafe fn start_send_vectored<V: IoBufs>(
    socket: sys::Handle,
    gather: &mut Gather<V>,
    overlapped: *mut sys::Overlapped,
) -> io::Result<Started> {
    let mut few = [EMPTY; FEW];
    let mut many = Vec::new();
    let count = gather.bufs.buf_count();
    let buffers = if count <= FEW {
        &mut few[..]
    } else {
        many.resize(count, EMPTY);
        &mut many[..]
    };
    let mut used = 0;
    let mut sent = 0;
    gather.rest(u32::MAX as usize, |ptr, len| {
        buffers[used] = sys::WsaBuf {
            len: len as u32,
            buf: ptr.cast_mut(),
        };
        used += 1;
    });
    // SAFETY: the caller's promise for the socket, the buffers and the
    // `OVERLAPPED`; each part named holds that many initialized bytes, as
    // `IoBufs` promises, which a send only reads; and Winsock copies the
    // `WSABUF`s and writes the count before the call returns.
    let failed = unsafe {
        sys::WSASend(
            socket_of(socket),
            buffers.as_ptr(),
            used as u32,
            &mut sent,
            0,
            overlapped,
            ptr::null(),
        )
    };
    taken(failed == 0, sent)
}

/// The future of
/// [`TcpStream::write_all_vectored`](super::TcpStream::write_all_vectored):
/// several buffers written to a stream, one after another, as many of them
/// in a send as the system takes. It hands the buffers, any [`IoBufs`],
/// such as a bundle of received data, back with `Ok(())` once all their
/// bytes are sent, or with the error that stopped the write.
///
/// Dropping it cancels the send in flight, which may have sent some of the
/// bytes by then, and the buffers are dropped once the cancellation has
/// completed.
///
/// It waits, before it starts, for the writes left behind the stream to
/// finish, and fails with the error that one of them failed with, handing
/// the buffers back unwritten.
#[must_use = "futures do nothing unless polled"]
pub struct WriteAllVectored<'a, V: IoBufs> {
    target: Target<'a>,
    state: Sending<'a, V>,
    /// The writes left behind a stream, which a write of the stream's waits
    /// for before it starts.
    gate: Option<&'a Behind>,
    /// When the send in flight is cancelled: the sends share the deadline
    /// the first fixes as it starts.
    limit: Limit,
}

/// Where a write of several buffers has got to.
enum Sending<'a, V> {
    /// Not started yet: the buffers.
    Start(V),
    /// A send of what is left of the buffers, which hold `len` bytes, in
    /// flight.
    Moving { op: Op<'a, Gather<V>>, len: usize },
    /// Finished, the buffers handed back.
    Finished,
}

// Nothing is pinned: the buffers are only ever moved.
impl<V: IoBufs> Unpin for WriteAllVectored<'_, V> {}

impl<'a, V: IoBufs> WriteAllVectored<'a, V> {
    pub(super) fn new(target: Target<'a>, bufs: V) -> Self {
        Self {
            target,
            state: Sending::Start(bufs),
            gate: None,
            limit: Limit::Never,
        }
    }

    /// Fails the write with `ERROR_TIMEOUT`, of [`io::ErrorKind::TimedOut`],
    /// if the buffers are not all sent `duration` after the first of the
    /// sends starts, which it does once the writes behind the stream have
    /// finished, cancelling the send in flight then: each send has what is
    /// left of the time.
    ///
    /// This has no effect once the first send has started.
    pub fn timeout(mut self, duration: Duration) -> Self {
        if matches!(self.state, Sending::Start(_)) {
            self.limit = Limit::After(duration);
        }
        self
    }

    /// Makes the write wait, before it starts, for the writes behind its
    /// stream that `gate` counts to finish, and fail with the error that
    /// one of them failed with.
    pub(super) fn behind(mut self, gate: &'a Behind) -> Self {
        self.gate = Some(gate);
        self
    }
}

impl<V: IoBufs> Future for WriteAllVectored<'_, V> {
    type Output = BufResult<(), V>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Err(err) = ready!(poll_gate(&mut this.gate, cx)) {
            // A write waits at its gate only before it has started.
            let Sending::Start(bufs) = mem::replace(&mut this.state, Sending::Finished) else {
                panic!("`WriteAllVectored` polled after it completed");
            };
            return Poll::Ready((Err(err), bufs));
        }
        loop {
            let (gather, len) = match mem::replace(&mut this.state, Sending::Finished) {
                Sending::Start(bufs) => {
                    let len = bufs.bytes_init();
                    (Gather { bufs, done: 0 }, len)
                }
                Sending::Moving { mut op, len } => {
                    let start = start_send_vectored::<V>;
                    let Poll::Ready((sent, mut gather)) = op.poll_with(cx, start, outcome) else {
                        this.state = Sending::Moving { op, len };
                        return Poll::Pending;
                    };
                    match sent {
                        Ok(0) => {
                            let err = io::ErrorKind::WriteZero.into();
                            return Poll::Ready((Err(err), gather.bufs));
                        }
                        Ok(sent) => gather.done += sent as usize,
                        Err(err) => return Poll::Ready((Err(err), gather.bufs)),
                    }
                    (gather, len)
                }
                Sending::Finished => panic!("`WriteAllVectored` polled after it completed"),
            };
            if gather.done >= len {
                return Poll::Ready((Ok(()), gather.bufs));
            }
            let mut op = Op::new(this.target, gather, 0);
            op.set_limit(this.limit.fix());
            this.state = Sending::Moving { op, len };
        }
    }
}

impl<V: IoBufs> fmt::Debug for WriteAllVectored<'_, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteAllVectored").finish_non_exhaustive()
    }
}
