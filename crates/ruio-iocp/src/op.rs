//! Operations through the port: the slot that an operation's `OVERLAPPED`
//! begins, which holds its buffer while the system has it, and the futures
//! that start operations and wait for their completions.
//!
//! A future starts its operation when first polled, moving its buffer into
//! a slot on the heap that begins with the operation's `OVERLAPPED`, whose
//! address the port hands back with the completion. The slot is the
//! future's until then, and again after it. A future dropped first cancels
//! the operation and leaves the slot to the engine, which frees it, buffer
//! and all, once the completion comes, as the system may use the buffer
//! until then. A future forgotten without being dropped leaks its slot,
//! which is as sound: nothing frees memory the system may still write.

use std::cell::{Cell, UnsafeCell};
use std::fmt;
use std::future::Future;
use std::io;
use std::mem;
use std::pin::Pin;
use std::ptr::{self, NonNull};
use std::task::{Context, Poll, Waker, ready};

use ruio_buf::{BufResult, IoBuf, IoBufMut};

use crate::handle::IoHandle;
use crate::sys;

/// What the system, the engine and an operation's future share: the start
/// of the operation's slot.
#[repr(C)]
struct Header {
    /// What the system fills in as the operation runs; first, so that the
    /// `OVERLAPPED` the port hands back is the header's address.
    overlapped: UnsafeCell<sys::Overlapped>,
    /// Whose the slot is.
    state: Cell<State>,
    /// The waker of the task waiting for the operation.
    waker: Cell<Option<Waker>>,
    /// How many bytes the operation transferred, once it completed.
    bytes: Cell<u32>,
    /// Frees the slot that the header begins.
    free: unsafe fn(*mut Header),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    /// The system has the operation, and the future waits for it.
    Waiting,
    /// The operation completed, and the slot is the future's again.
    Done,
    /// The future let go of the slot, which the engine frees once the
    /// operation completes.
    Orphaned,
}

/// An operation's slot: its header, and the buffer the system uses.
#[repr(C)]
struct Slot<B> {
    header: Header,
    buf: B,
}

impl Header {
    fn new<B>(offset: u64, waker: Waker) -> Self {
        Self {
            overlapped: UnsafeCell::new(sys::Overlapped {
                internal: 0,
                internal_high: 0,
                offset: offset as u32,
                offset_high: (offset >> 32) as u32,
                event: ptr::null_mut(),
            }),
            state: Cell::new(State::Waiting),
            waker: Cell::new(Some(waker)),
            bytes: Cell::new(0),
            free: free::<B>,
        }
    }
}

/// Frees the slot of a buffer of type `B` that `header` begins.
///
/// # Safety
///
/// `header` must begin a `Slot<B>` that `Box::leak` gave, which nothing uses
/// again.
unsafe fn free<B>(header: *mut Header) {
    // SAFETY: the caller's promise; a slot begins with its header, as it is
    // `repr(C)`.
    drop(unsafe { Box::from_raw(header.cast::<Slot<B>>()) });
}

/// Records the completion of the operation whose `OVERLAPPED` `entry` hands
/// back, and wakes its future if `wake`, or frees its slot if the future
/// let go of it.
///
/// # Safety
///
/// `entry` must be the completion of an operation that a future of this
/// module started, which the port hands back once.
pub(crate) unsafe fn complete(entry: &sys::OverlappedEntry, wake: bool) {
    let header = entry.overlapped.cast::<Header>();
    // SAFETY: by the caller's promise, the slot is allocated until its
    // completion is delivered, as this is; it begins with its header, which
    // begins with the `OVERLAPPED`.
    let slot = unsafe { &*header };
    if slot.state.get() == State::Orphaned {
        let free = slot.free;
        // SAFETY: the future let go of the slot and the system has finished
        // with it, so it is the engine's, which frees it this once.
        unsafe { free(header) };
        return;
    }
    slot.bytes.set(entry.bytes_transferred);
    slot.state.set(State::Done);
    // The wake may drop the future, which then frees the slot, so nothing
    // here touches the slot after it.
    let waker = slot.waker.take();
    if wake && let Some(waker) = waker {
        waker.wake();
    }
}

/// Starts an operation on `handle` with `buf`, under `overlapped`, and says
/// whether the system took it, in which case its completion is to come
/// through the port.
///
/// # Safety
///
/// `handle` must be open and associated with a port, and `buf`'s memory and
/// `overlapped` must stay valid and in place until the completion is
/// delivered.
type Start<B> = unsafe fn(sys::Handle, &mut B, *mut sys::Overlapped) -> io::Result<()>;

/// An operation on a handle of the port, and its buffer.
struct Op<'a, B: IoBuf> {
    handle: &'a IoHandle,
    offset: u64,
    state: OpState<B>,
}

enum OpState<B> {
    /// Not started yet: the buffer.
    Unstarted(B),
    /// Started: the slot, which the system and the engine may use.
    Started(NonNull<Slot<B>>),
    /// Finished, its buffer handed back.
    Finished,
}

// The buffer is never pinned: it moves into the slot, and out of it.
impl<B: IoBuf> Unpin for Op<'_, B> {}

impl<'a, B: IoBuf> Op<'a, B> {
    fn new(handle: &'a IoHandle, buf: B, offset: u64) -> Self {
        Self {
            handle,
            offset,
            state: OpState::Unstarted(buf),
        }
    }

    /// Polls the operation, which `start` starts on the first poll, for how
    /// many bytes it transferred, and the buffer.
    fn poll_with(&mut self, cx: &mut Context<'_>, start: Start<B>) -> Poll<BufResult<u32, B>> {
        match mem::replace(&mut self.state, OpState::Finished) {
            OpState::Unstarted(buf) => self.start(cx, buf, start),
            OpState::Started(slot) => self.poll_started(cx, slot),
            OpState::Finished => panic!("an operation polled after it finished"),
        }
    }

    fn start(&mut self, cx: &mut Context<'_>, buf: B, start: Start<B>) -> Poll<BufResult<u32, B>> {
        let slot = NonNull::from(Box::leak(Box::new(Slot {
            header: Header::new::<B>(self.offset, cx.waker().clone()),
            buf,
        })));
        // SAFETY: the slot was just made, and is the future's alone.
        let buf = unsafe { &mut (*slot.as_ptr()).buf };
        // SAFETY: as above; the `OVERLAPPED` lies apart from the buffer.
        let overlapped = unsafe { (*slot.as_ptr()).header.overlapped.get() };
        // SAFETY: the handle is open and associated with the port, as the
        // future borrows it; the slot, which holds the buffer and the
        // `OVERLAPPED`, stays where it is until the completion is delivered,
        // the future's until then, or the engine's.
        let started = unsafe { start(self.handle.raw(), buf, overlapped) };
        match started {
            Ok(()) => {
                self.handle.engine().started();
                self.state = OpState::Started(slot);
                Poll::Pending
            }
            Err(err) => {
                // SAFETY: the system did not take the operation, so nothing
                // will come through the port for it, and the slot is the
                // future's alone.
                let Slot { buf, .. } = *unsafe { Box::from_raw(slot.as_ptr()) };
                Poll::Ready((Err(err), buf))
            }
        }
    }

    fn poll_started(
        &mut self,
        cx: &mut Context<'_>,
        slot: NonNull<Slot<B>>,
    ) -> Poll<BufResult<u32, B>> {
        // SAFETY: the slot is allocated while the future has it, and only
        // this thread uses it.
        let waiting = unsafe { &slot.as_ref().header };
        if waiting.state.get() == State::Waiting {
            let waker = match waiting.waker.take() {
                Some(waker) if waker.will_wake(cx.waker()) => waker,
                _ => cx.waker().clone(),
            };
            waiting.waker.set(Some(waker));
            self.state = OpState::Started(slot);
            return Poll::Pending;
        }
        // SAFETY: the operation is done, so the system and the engine have
        // finished with the slot, which is the future's alone again.
        let Slot { header, buf } = *unsafe { Box::from_raw(slot.as_ptr()) };
        let status = header.overlapped.into_inner().internal;
        Poll::Ready((result(status, header.bytes.get()), buf))
    }
}

impl<B: IoBuf> Drop for Op<'_, B> {
    fn drop(&mut self) {
        let OpState::Started(slot) = &self.state else {
            return;
        };
        let slot = *slot;
        // SAFETY: the slot is allocated while the future has it, and only
        // this thread uses it.
        let header = unsafe { &slot.as_ref().header };
        if header.state.get() == State::Done {
            // SAFETY: the operation is done, so the slot is the future's
            // alone.
            drop(unsafe { Box::from_raw(slot.as_ptr()) });
            return;
        }
        header.state.set(State::Orphaned);
        drop(header.waker.take());
        // SAFETY: the handle is open, as the future borrows it, and the
        // `OVERLAPPED` names the operation, which may have completed
        // already, when the call does nothing.
        unsafe { sys::CancelIoEx(self.handle.raw(), header.overlapped.get()) };
    }
}

/// The outcome of an operation that completed with `status`, the
/// `NTSTATUS` its `OVERLAPPED` holds, having transferred `bytes`.
fn result(status: usize, bytes: u32) -> io::Result<u32> {
    // The status is the low half of `Internal`, negative for an error.
    let status = status as u32 as i32;
    if status >= 0 {
        return Ok(bytes);
    }
    let code = sys::RtlNtStatusToDosError(status);
    Err(io::Error::from_raw_os_error(code as i32))
}

/// Whether the system took an operation that its call returned `ok` for. A
/// call that succeeded completed the operation at once, and its completion
/// is queued to the port all the same.
fn taken(ok: sys::Bool) -> io::Result<()> {
    if ok != 0 {
        return Ok(());
    }
    match sys::GetLastError() {
        sys::ERROR_IO_PENDING => Ok(()),
        code => Err(io::Error::from_raw_os_error(code as i32)),
    }
}

/// Starts a read into `buf`, as [`Start`] says.
///
/// # Safety
///
/// As for [`Start`].
unsafe fn start_read<B: IoBufMut>(
    handle: sys::Handle,
    buf: &mut B,
    overlapped: *mut sys::Overlapped,
) -> io::Result<()> {
    let len = u32::try_from(buf.bytes_total()).unwrap_or(u32::MAX);
    let ptr = buf.stable_mut_ptr();
    // SAFETY: the caller's promise for the handle, the buffer and the
    // `OVERLAPPED`; the buffer is writable for `len` bytes, as `IoBufMut`
    // promises.
    let ok = unsafe { sys::ReadFile(handle, ptr.cast(), len, ptr::null_mut(), overlapped) };
    taken(ok)
}

/// Starts a write of `buf`'s bytes, as [`Start`] says.
///
/// # Safety
///
/// As for [`Start`].
unsafe fn start_write<B: IoBuf>(
    handle: sys::Handle,
    buf: &mut B,
    overlapped: *mut sys::Overlapped,
) -> io::Result<()> {
    let len = u32::try_from(buf.bytes_init()).unwrap_or(u32::MAX);
    let ptr = buf.stable_ptr();
    // SAFETY: the caller's promise for the handle, the buffer and the
    // `OVERLAPPED`; the buffer holds `len` initialized bytes, as `IoBuf`
    // promises.
    let ok = unsafe { sys::WriteFile(handle, ptr.cast(), len, ptr::null_mut(), overlapped) };
    taken(ok)
}

/// Whether a read's error means that there is nothing more to read: the end
/// of a file, or of a pipe whose other end has closed.
fn is_end(err: &io::Error) -> bool {
    err.raw_os_error().is_some_and(|code| {
        code == sys::ERROR_HANDLE_EOF as i32 || code == sys::ERROR_BROKEN_PIPE as i32
    })
}

/// The future of a read through the port:
/// [`File::read_at`](crate::fs::File::read_at) and
/// [`NamedPipe::read`](crate::pipe::NamedPipe::read). It hands the buffer,
/// an [`IoBufMut`], back with how many bytes it read: 0 at the end of a
/// file, or of a pipe whose other end has closed.
///
/// It starts the read when first polled. Dropping it before the read
/// completes cancels the read, and the buffer is dropped once the
/// cancellation has completed.
#[must_use = "futures do nothing unless polled"]
pub struct Read<'a, B: IoBufMut> {
    op: Op<'a, B>,
}

impl<'a, B: IoBufMut> Read<'a, B> {
    pub(crate) fn new(handle: &'a IoHandle, buf: B, offset: u64) -> Self {
        Self {
            op: Op::new(handle, buf, offset),
        }
    }
}

impl<B: IoBufMut> Future for Read<'_, B> {
    type Output = BufResult<usize, B>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (result, mut buf) = ready!(self.op.poll_with(cx, start_read::<B>));
        let result = match result {
            Ok(read) => {
                // SAFETY: the system wrote the bytes it reports reading from
                // the start of the buffer, within its `bytes_total`.
                unsafe { buf.set_init(read as usize) };
                Ok(read as usize)
            }
            Err(err) if is_end(&err) => Ok(0),
            Err(err) => Err(err),
        };
        Poll::Ready((result, buf))
    }
}

impl<B: IoBufMut> fmt::Debug for Read<'_, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Read")
            .field("offset", &self.op.offset)
            .finish_non_exhaustive()
    }
}

/// The future of a write through the port:
/// [`File::write_at`](crate::fs::File::write_at) and
/// [`NamedPipe::write`](crate::pipe::NamedPipe::write). It hands the buffer,
/// an [`IoBuf`], back with how many bytes it wrote.
///
/// It starts the write when first polled. Dropping it before the write
/// completes cancels the write, which may have written some of the buffer
/// by then, and the buffer is dropped once the cancellation has completed.
#[must_use = "futures do nothing unless polled"]
pub struct Write<'a, B: IoBuf> {
    op: Op<'a, B>,
}

impl<'a, B: IoBuf> Write<'a, B> {
    pub(crate) fn new(handle: &'a IoHandle, buf: B, offset: u64) -> Self {
        Self {
            op: Op::new(handle, buf, offset),
        }
    }
}

impl<B: IoBuf> Future for Write<'_, B> {
    type Output = BufResult<usize, B>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (result, buf) = ready!(self.op.poll_with(cx, start_write::<B>));
        Poll::Ready((result.map(|written| written as usize), buf))
    }
}

impl<B: IoBuf> fmt::Debug for Write<'_, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Write")
            .field("offset", &self.op.offset)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::result;

    #[test]
    fn statuses_become_windows_errors() {
        assert_eq!(result(0, 7).unwrap(), 7);
        // STATUS_END_OF_FILE, as a read past the end of a file completes.
        let end = result(0xC000_0011, 0).unwrap_err();
        assert_eq!(end.raw_os_error(), Some(38));
        // STATUS_CANCELLED, as a cancelled operation completes.
        let cancelled = result(0xC000_0120, 0).unwrap_err();
        assert_eq!(cancelled.raw_os_error(), Some(995));
    }
}
