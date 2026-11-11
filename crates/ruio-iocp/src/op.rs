//! Operations through the port: the slot that an operation's `OVERLAPPED`
//! begins, which holds what the system uses while it has the operation, a
//! read's or a write's buffer say, and the futures that start operations
//! and wait for their completions.
//!
//! A future starts its operation when first polled, moving its buffer into
//! a slot on the heap that begins with the operation's `OVERLAPPED`, whose
//! address the port hands back with the completion. The slot is the
//! future's until then, and again after it. A future dropped first cancels
//! the operation and leaves the slot to the engine, which frees it, buffer
//! and all, once the completion comes, as the system may use the buffer
//! until then. A future forgotten without being dropped leaks its slot,
//! which is as sound: nothing frees memory the system may still write.
//!
//! An operation may also be left to finish by itself, with no future to
//! wait for it. Its slot is the engine's from the start, and when the
//! completion comes the engine hands what the slot holds the outcome. No
//! future's drop cancels such an operation, so the engine keeps those in
//! flight, and cancels them as it is dropped.

use std::cell::{Cell, RefCell, UnsafeCell};
use std::fmt;
use std::future::Future;
use std::io;
use std::mem::{self, ManuallyDrop};
use std::pin::Pin;
use std::ptr::{self, NonNull};
use std::task::{Context, Poll, Waker, ready};

use ruio_buf::{BufResult, IoBuf, IoBufMut, Slice};

use crate::handle::Target;
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
    /// What the engine does with the slot that the header begins once the
    /// operation has completed and no future has the slot.
    release: Release,
    /// Where the engine keeps the operation among those left to finish by
    /// themselves, if it is one.
    place: Cell<usize>,
}

/// Disposes of the slot that a header begins, whose operation has completed
/// with no future to take it, having transferred as many bytes as the port
/// said: frees it, after handing what it holds the outcome if the operation
/// was left to finish by itself, which wakes tasks only if told that it may.
///
/// # Safety
///
/// The header must begin a slot that `Box::leak` gave, of the payload the
/// function is for, whose operation's completion has been delivered and
/// which nothing uses again.
type Release = unsafe fn(*mut Header, u32, bool);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    /// The system has the operation, and the future waits for it.
    Waiting,
    /// The operation completed, and the slot is the future's again.
    Done,
    /// The future let go of the slot, which the engine frees once the
    /// operation completes.
    Orphaned,
    /// No future ever had the slot: the operation was left to finish by
    /// itself, and the engine hands what the slot holds the outcome, and
    /// frees the slot, once the operation completes.
    Detached,
}

/// An operation's slot: its header, and what the system uses while it has
/// the operation, such as the buffer of a read.
#[repr(C)]
struct Slot<T> {
    header: Header,
    payload: T,
}

impl Header {
    /// The header of the slot of an operation at `offset`, which starts in
    /// `state`, with the waker of the task that waits for it, if one does.
    fn new(offset: u64, state: State, waker: Option<Waker>, release: Release) -> Self {
        Self {
            overlapped: UnsafeCell::new(sys::Overlapped {
                internal: sys::STATUS_PENDING as usize,
                internal_high: 0,
                offset: offset as u32,
                offset_high: (offset >> 32) as u32,
                event: ptr::null_mut(),
            }),
            state: Cell::new(state),
            waker: Cell::new(waker),
            bytes: Cell::new(0),
            release,
            place: Cell::new(0),
        }
    }
}

/// Frees the slot of a payload of type `T` that `header` begins, as
/// [`Release`] says.
///
/// # Safety
///
/// As for [`Release`].
unsafe fn free<T>(header: *mut Header, _bytes: u32, _wake: bool) {
    // SAFETY: the caller's promise; a slot begins with its header, as it is
    // `repr(C)`.
    drop(unsafe { Box::from_raw(header.cast::<Slot<T>>()) });
}

/// Records the completion of the operation whose `OVERLAPPED` `entry` hands
/// back, and wakes its future if `wake`, or releases its slot if no future
/// has it: one let go of it, or the operation is one of `detached`, left to
/// finish by itself.
///
/// # Safety
///
/// `entry` must be the completion of an operation that this module started
/// on the port of the engine that `detached` is of, which the port hands
/// back once.
pub(crate) unsafe fn complete(entry: &sys::OverlappedEntry, detached: &DetachedOps, wake: bool) {
    let header = entry.overlapped.cast::<Header>();
    // SAFETY: by the caller's promise, the slot is allocated until its
    // completion is delivered, as this is; it begins with its header, which
    // begins with the `OVERLAPPED`.
    let slot = unsafe { &*header };
    let state = slot.state.get();
    if state != State::Waiting {
        if state == State::Detached {
            detached.remove(slot);
        }
        let release = slot.release;
        // SAFETY: no future has the slot and the system has finished with
        // it, so it is the engine's, which releases it this once.
        unsafe { release(header, entry.bytes_transferred, wake) };
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

/// Starts an operation on `handle` with `payload`, under `overlapped`, and
/// says whether the system took it, in which case its completion is to come
/// through the port.
///
/// # Safety
///
/// `handle` must be open and associated with a port, and `payload`, with
/// the memory it names, and `overlapped` must stay valid and in place until
/// the completion is delivered.
pub(crate) type Start<T> = unsafe fn(sys::Handle, &mut T, *mut sys::Overlapped) -> io::Result<()>;

/// Reads the outcome of an operation on `handle` that has completed from
/// its `overlapped`: how many bytes it transferred, of which the last
/// argument is the count the port gave, or the error it failed with.
///
/// # Safety
///
/// `handle` must be open, and `overlapped` that of an operation on it whose
/// completion has been delivered, still where the operation was started
/// with it.
pub(crate) type Outcome = unsafe fn(sys::Handle, *const sys::Overlapped, u32) -> io::Result<u32>;

/// An operation on a handle of the port, and what the system uses for it.
pub(crate) struct Op<'a, T> {
    target: Target<'a>,
    offset: u64,
    state: OpState<T>,
}

enum OpState<T> {
    /// Not started yet: what the operation is to use.
    Unstarted(T),
    /// Started: the slot, which the system and the engine may use.
    Started(NonNull<Slot<T>>),
    /// Finished, what it used handed back.
    Finished,
}

// What an operation uses is never pinned: it moves into the slot, and out
// of it.
impl<T> Unpin for Op<'_, T> {}

impl<'a, T> Op<'a, T> {
    /// An operation on `target` that uses `payload`, at `offset` if the
    /// handle is a file's.
    pub(crate) fn new(target: Target<'a>, payload: T, offset: u64) -> Self {
        Self {
            target,
            offset,
            state: OpState::Unstarted(payload),
        }
    }

    /// Polls the operation, which `start` starts on the first poll, for how
    /// many bytes it transferred, as `outcome` reads it, and what it used.
    pub(crate) fn poll_with(
        &mut self,
        cx: &mut Context<'_>,
        start: Start<T>,
        outcome: Outcome,
    ) -> Poll<(io::Result<u32>, T)> {
        match mem::replace(&mut self.state, OpState::Finished) {
            OpState::Unstarted(payload) => self.start(cx, payload, start),
            OpState::Started(slot) => self.poll_started(cx, slot, outcome),
            OpState::Finished => panic!("an operation polled after it finished"),
        }
    }

    fn start(
        &mut self,
        cx: &mut Context<'_>,
        payload: T,
        start: Start<T>,
    ) -> Poll<(io::Result<u32>, T)> {
        let waker = Some(cx.waker().clone());
        let header = Header::new(self.offset, State::Waiting, waker, free::<T>);
        let slot = NonNull::from(Box::leak(Box::new(Slot { header, payload })));
        // SAFETY: the handle is open and associated with the port, as the
        // future borrows it; the slot was just made, and stays where it is
        // until the completion is delivered, the future's until then, or the
        // engine's.
        match unsafe { launch(self.target.handle, slot, start) } {
            Err(err) => {
                // SAFETY: the system did not take the operation, so nothing
                // will come through the port for it, and the slot is the
                // future's alone.
                let Slot { payload, .. } = *unsafe { Box::from_raw(slot.as_ptr()) };
                Poll::Ready((Err(err), payload))
            }
            // The outcome, an error included, is read once the completion
            // has been delivered.
            Ok(()) => {
                self.target.engine.started();
                self.state = OpState::Started(slot);
                Poll::Pending
            }
        }
    }

    fn poll_started(
        &mut self,
        cx: &mut Context<'_>,
        slot: NonNull<Slot<T>>,
        outcome: Outcome,
    ) -> Poll<(io::Result<u32>, T)> {
        // SAFETY: the slot is allocated while the future has it, and only
        // this thread uses it.
        let header = unsafe { &slot.as_ref().header };
        if header.state.get() == State::Waiting {
            let waker = match header.waker.take() {
                Some(waker) if waker.will_wake(cx.waker()) => waker,
                _ => cx.waker().clone(),
            };
            header.waker.set(Some(waker));
            self.state = OpState::Started(slot);
            return Poll::Pending;
        }
        // SAFETY: the handle is open, as the future borrows it, and the
        // operation's completion has been delivered, its `OVERLAPPED` still
        // in the slot it was started with.
        let result = unsafe {
            outcome(
                self.target.handle,
                header.overlapped.get(),
                header.bytes.get(),
            )
        };
        // SAFETY: the operation is done, so the system and the engine have
        // finished with the slot, which is the future's alone again.
        let Slot { payload, .. } = *unsafe { Box::from_raw(slot.as_ptr()) };
        Poll::Ready((result, payload))
    }

    /// Whether the operation's completion has been delivered and not yet
    /// read, so that a poll finishes it without waiting.
    pub(crate) fn is_done(&self) -> bool {
        let OpState::Started(slot) = &self.state else {
            return false;
        };
        // SAFETY: the slot is allocated while the future has it, and only
        // this thread uses it.
        let header = unsafe { &slot.as_ref().header };
        header.state.get() == State::Done
    }
}

impl<T> Drop for Op<'_, T> {
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
        unsafe { sys::CancelIoEx(self.target.handle, header.overlapped.get()) };
    }
}

/// Starts the operation of `slot` on `handle` with `start`, and says whether
/// the system took it, in which case its completion is to come through the
/// port: if not, with the error, and nothing comes.
///
/// # Safety
///
/// `handle` must be open and associated with a port, and `slot` one that
/// `Box::leak` gave and nothing else uses, which stays where it is until
/// the completion is delivered, if the system took the operation.
unsafe fn launch<T>(
    handle: sys::Handle,
    slot: NonNull<Slot<T>>,
    start: Start<T>,
) -> io::Result<()> {
    // SAFETY: the slot is allocated, and the caller's alone.
    let payload = unsafe { &mut (*slot.as_ptr()).payload };
    // SAFETY: as above; the `OVERLAPPED` lies apart from the payload.
    let overlapped = unsafe { (*slot.as_ptr()).header.overlapped.get() };
    // SAFETY: the caller's promise for the handle; the slot, which holds the
    // payload and the `OVERLAPPED`, stays where it is until the completion
    // is delivered.
    let started = unsafe { start(handle, payload, overlapped) };
    // A call that fails at once may have completed the operation all the
    // same, with a warning, and then its completion is queued too.
    // SAFETY: the slot is allocated, and the call has returned, so
    // nothing writes the `OVERLAPPED` while its status is read.
    let queued = started.is_ok() || completed(unsafe { (*overlapped).internal });
    match started {
        Err(err) if !queued => Err(err),
        _ => Ok(()),
    }
}

/// What an operation left to finish by itself uses, which is told how the
/// operation went; see [`detach`].
pub(crate) trait Detached: Sized {
    /// Takes the outcome of the operation, whose completion has come:
    /// `overlapped` holds it, for an [`Outcome`] to read, and `bytes` is the
    /// count the port gave. Tasks are woken only if `wake`: none is as the
    /// engine is dropped.
    ///
    /// # Safety
    ///
    /// `overlapped` must be that of the operation that used this, whose
    /// completion has been delivered, still where the operation was started
    /// with it, and valid until the call returns.
    unsafe fn complete(self, overlapped: *const sys::Overlapped, bytes: u32, wake: bool);
}

/// Hands the payload of type `T` in the slot that `header` begins the
/// outcome of its operation, and frees the slot, as [`Release`] says.
///
/// # Safety
///
/// As for [`Release`].
unsafe fn settle<T: Detached>(header: *mut Header, bytes: u32, wake: bool) {
    // SAFETY: the caller's promise; a slot begins with its header, as it is
    // `repr(C)`, and holds a `ManuallyDrop<T>` where it holds a `T`, the one
    // laid out as the other.
    let mut slot = unsafe { Box::from_raw(header.cast::<Slot<ManuallyDrop<T>>>()) };
    // SAFETY: the payload is taken this once, and the slot is dropped
    // without it.
    let payload = unsafe { ManuallyDrop::take(&mut slot.payload) };
    // SAFETY: the operation's completion has been delivered, by the caller's
    // promise, and its `OVERLAPPED` is in the slot, where the operation was
    // started with it, which is freed only after the call.
    unsafe { payload.complete(slot.header.overlapped.get(), bytes, wake) };
}

/// Starts an operation on `target` that uses `payload`, at `offset` if the
/// handle is a file's, and leaves it to finish by itself: no future waits
/// for it, and `payload` is told how it went when its completion comes; see
/// [`Detached`]. The engine cancels the operation as it is dropped, which
/// it can for as long as `payload` keeps the handle open.
///
/// # Errors
///
/// What `start` reports, with the payload, if the system did not take the
/// operation, of which the payload is then told nothing.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "nothing is left to finish by itself yet")
)]
pub(crate) fn detach<T: Detached>(
    target: Target<'_>,
    payload: T,
    offset: u64,
    start: Start<T>,
) -> Result<(), (io::Error, T)> {
    let header = Header::new(offset, State::Detached, None, settle::<T>);
    let slot = NonNull::from(Box::leak(Box::new(Slot { header, payload })));
    // SAFETY: the handle is open and associated with the port, as `target`
    // borrows it; the slot was just made, and stays where it is until the
    // completion is delivered, when the engine releases it.
    match unsafe { launch(target.handle, slot, start) } {
        Err(err) => {
            // SAFETY: the system did not take the operation, so nothing will
            // come through the port for it, and nothing else has the slot.
            let Slot { payload, .. } = *unsafe { Box::from_raw(slot.as_ptr()) };
            Err((err, payload))
        }
        Ok(()) => {
            target.engine.started();
            target.engine.detached().add(target.handle, slot.cast());
            Ok(())
        }
    }
}

/// The operations of an engine's that were left to finish by themselves and
/// have not: what the engine cancels as it is dropped.
#[derive(Default)]
pub(crate) struct DetachedOps {
    /// Each operation's handle, and the header of its slot, whose `place`
    /// is where in the vector the operation is.
    ops: RefCell<Vec<(sys::Handle, NonNull<Header>)>>,
}

impl DetachedOps {
    /// Adds an operation on `handle` that the system took, by the header of
    /// its slot.
    fn add(&self, handle: sys::Handle, header: NonNull<Header>) {
        let mut ops = self.ops.borrow_mut();
        // SAFETY: the slot is allocated until the operation's completion is
        // delivered, which it has not been.
        unsafe { header.as_ref() }.place.set(ops.len());
        ops.push((handle, header));
    }

    /// Removes the operation of `header`, whose completion has come.
    fn remove(&self, header: &Header) {
        let mut ops = self.ops.borrow_mut();
        let place = header.place.get();
        debug_assert!(
            ops.get(place)
                .is_some_and(|(_, at)| ptr::eq(at.as_ptr(), header)),
            "an operation left to finish by itself is where its header says"
        );
        ops.swap_remove(place);
        // The last of them took its place.
        if let Some((_, moved)) = ops.get(place) {
            // SAFETY: the operation's completion has not come, so its slot
            // is allocated.
            unsafe { moved.as_ref() }.place.set(place);
        }
    }

    /// Asks the system to cancel them all. Their completions come through
    /// the port all the same.
    pub(crate) fn cancel(&self) {
        for (handle, header) in self.ops.borrow().iter() {
            // SAFETY: the `OVERLAPPED` names the operation, whose slot is
            // allocated until its completion is delivered; a handle that
            // has closed since has no such operation to cancel.
            unsafe { sys::CancelIoEx(*handle, header.as_ptr().cast()) };
        }
    }
}

/// Whether the system completed an operation whose call failed at once, and
/// so queued its completion to the port, going by the status that the call
/// left in its `OVERLAPPED`.
///
/// The system queues the completion of an operation that completes during
/// the call unless it fails with an error status, and writes the status to
/// the `OVERLAPPED` in the same cases. A warning status is no error to it,
/// yet the call that started the operation reports it as one: a datagram
/// longer than its buffer fails `WSARecvFrom` with `WSAEMSGSIZE`, and its
/// completion comes through the port all the same. An `OVERLAPPED` starts
/// as pending, so a status that is neither that nor an error is one the
/// system wrote, with a completion behind it.
fn completed(status: usize) -> bool {
    // The status is the low half of `Internal`; errors have its top two bits
    // set.
    let status = status as u32;
    status != sys::STATUS_PENDING && status >> 30 != 0b11
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

/// Reads the outcome of an operation on a file or a pipe from the status in
/// its `OVERLAPPED`, as [`Outcome`] says.
///
/// # Safety
///
/// As for [`Outcome`].
unsafe fn status_outcome(
    _handle: sys::Handle,
    overlapped: *const sys::Overlapped,
    bytes: u32,
) -> io::Result<u32> {
    // SAFETY: the caller's promise: the operation has completed, so the
    // system writes its `OVERLAPPED` no more, and the slot still holds it.
    let status = unsafe { (*overlapped).internal };
    result(status, bytes)
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
/// [`File::read_at`](crate::fs::File::read_at),
/// [`NamedPipe::read`](crate::pipe::NamedPipe::read) and
/// [`TcpStream::read`](crate::net::TcpStream::read). It hands the buffer,
/// an [`IoBufMut`], back with how many bytes it read: 0 at the end of a
/// file, of a pipe whose other end has closed, or of a stream whose peer
/// has shut down.
///
/// It starts the read when first polled. Dropping it before the read
/// completes cancels the read, and the buffer is dropped once the
/// cancellation has completed.
#[must_use = "futures do nothing unless polled"]
pub struct Read<'a, B: IoBufMut> {
    op: Op<'a, B>,
    start: Start<B>,
    outcome: Outcome,
}

impl<'a, B: IoBufMut> Read<'a, B> {
    /// A read of a file, from `offset`, or of a pipe.
    pub(crate) fn new(target: Target<'a>, buf: B, offset: u64) -> Self {
        Self::with(target, buf, offset, start_read::<B>, status_outcome)
    }

    /// A read that `start` starts, and whose outcome `outcome` reads.
    pub(crate) fn with(
        target: Target<'a>,
        buf: B,
        offset: u64,
        start: Start<B>,
        outcome: Outcome,
    ) -> Self {
        Self {
            op: Op::new(target, buf, offset),
            start,
            outcome,
        }
    }

    /// Whether the read's completion has been delivered and not yet read,
    /// so that a poll finishes the read without waiting.
    pub(crate) fn is_done(&self) -> bool {
        self.op.is_done()
    }
}

impl<B: IoBufMut> Future for Read<'_, B> {
    type Output = BufResult<usize, B>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (start, outcome) = (self.start, self.outcome);
        let (result, mut buf) = ready!(self.op.poll_with(cx, start, outcome));
        let result = match result {
            Ok(read) => {
                // A count beyond the buffer would be the length of a datagram
                // that did not fit, of which the buffer holds what did.
                let read = (read as usize).min(buf.bytes_total());
                // SAFETY: the system wrote the bytes it reports reading from
                // the start of the buffer, within its `bytes_total`.
                unsafe { buf.set_init(read) };
                Ok(read)
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
/// [`File::write_at`](crate::fs::File::write_at),
/// [`NamedPipe::write`](crate::pipe::NamedPipe::write) and
/// [`TcpStream::write`](crate::net::TcpStream::write). It hands the buffer,
/// an [`IoBuf`], back with how many bytes it wrote.
///
/// It starts the write when first polled. Dropping it before the write
/// completes cancels the write, which may have written some of the buffer
/// by then, and the buffer is dropped once the cancellation has completed.
#[must_use = "futures do nothing unless polled"]
pub struct Write<'a, B: IoBuf> {
    op: Op<'a, B>,
    start: Start<B>,
    outcome: Outcome,
}

impl<'a, B: IoBuf> Write<'a, B> {
    /// A write to a file, at `offset`, or to a pipe.
    pub(crate) fn new(target: Target<'a>, buf: B, offset: u64) -> Self {
        Self::with(target, buf, offset, start_write::<B>, status_outcome)
    }

    /// A write that `start` starts, and whose outcome `outcome` reads.
    pub(crate) fn with(
        target: Target<'a>,
        buf: B,
        offset: u64,
        start: Start<B>,
        outcome: Outcome,
    ) -> Self {
        Self {
            op: Op::new(target, buf, offset),
            start,
            outcome,
        }
    }
}

impl<B: IoBuf> Future for Write<'_, B> {
    type Output = BufResult<usize, B>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (start, outcome) = (self.start, self.outcome);
        let (result, buf) = ready!(self.op.poll_with(cx, start, outcome));
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

/// Where the transfer of a whole buffer has got to.
enum Whole<F, B> {
    /// Not started yet: the buffer.
    Start(B),
    /// A read or a write of the rest of the buffer, in flight.
    Moving(F),
    /// Finished, the buffer handed back.
    Finished,
}

/// The future of a read that fills its buffer, in as many reads as that
/// takes: [`TcpStream::read_exact`](crate::net::TcpStream::read_exact) and
/// [`File::read_exact_at`](crate::fs::File::read_exact_at). It hands the
/// buffer, an [`IoBufMut`], back with `Ok(())` once the buffer is full, up
/// to its `bytes_total`, with [`io::ErrorKind::UnexpectedEof`] if the
/// stream or the file ends first, or with the error that stopped the
/// reads. What was read is marked initialized either way.
///
/// A stream's fills the buffer from its start, and a file's after the
/// buffer's contents, as ruio's do on io_uring.
///
/// Dropping it cancels the read in flight, and the buffer is dropped once
/// the cancellation has completed.
#[must_use = "futures do nothing unless polled"]
pub struct ReadExact<'a, B: IoBufMut> {
    target: Target<'a>,
    start: Start<Slice<B>>,
    outcome: Outcome,
    /// Where in a file the next read is from, which a stream's ignores.
    offset: u64,
    /// How much of the buffer is filled.
    done: usize,
    state: Whole<Read<'a, Slice<B>>, B>,
}

// Nothing is pinned: the buffer is only ever moved.
impl<B: IoBufMut> Unpin for ReadExact<'_, B> {}

impl<'a, B: IoBufMut> ReadExact<'a, B> {
    /// A read of a file that fills `buf` after its contents, from `offset`.
    pub(crate) fn at(target: Target<'a>, buf: B, offset: u64) -> Self {
        let done = buf.bytes_init();
        let start = start_read::<Slice<B>>;
        Self::with(target, buf, done, offset, start, status_outcome)
    }

    /// A read that fills `buf` from `done` on, with reads that `start`
    /// starts and whose outcomes `outcome` reads.
    pub(crate) fn with(
        target: Target<'a>,
        buf: B,
        done: usize,
        offset: u64,
        start: Start<Slice<B>>,
        outcome: Outcome,
    ) -> Self {
        Self {
            target,
            start,
            outcome,
            offset,
            done,
            state: Whole::Start(buf),
        }
    }
}

impl<B: IoBufMut> Future for ReadExact<'_, B> {
    type Output = BufResult<(), B>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            let buf = match mem::replace(&mut this.state, Whole::Finished) {
                Whole::Start(buf) => buf,
                Whole::Moving(mut read) => {
                    let Poll::Ready((result, rest)) = Pin::new(&mut read).poll(cx) else {
                        this.state = Whole::Moving(read);
                        return Poll::Pending;
                    };
                    let buf = rest.into_inner();
                    match result {
                        Ok(0) => {
                            let err = io::ErrorKind::UnexpectedEof.into();
                            return Poll::Ready((Err(err), buf));
                        }
                        Ok(read) => {
                            this.done += read;
                            this.offset += read as u64;
                        }
                        Err(err) => return Poll::Ready((Err(err), buf)),
                    }
                    buf
                }
                Whole::Finished => panic!("`ReadExact` polled after it completed"),
            };
            if this.done >= buf.bytes_total() {
                return Poll::Ready((Ok(()), buf));
            }
            let rest = buf.slice(this.done..);
            let read = Read::with(this.target, rest, this.offset, this.start, this.outcome);
            this.state = Whole::Moving(read);
        }
    }
}

impl<B: IoBufMut> fmt::Debug for ReadExact<'_, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadExact")
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

/// The future of a write of a whole buffer, in as many writes as that
/// takes: [`TcpStream::write_all`](crate::net::TcpStream::write_all) and
/// [`File::write_all_at`](crate::fs::File::write_all_at). It hands the
/// buffer, an [`IoBuf`], back with `Ok(())` once all of its initialized
/// bytes are written, or with the error that stopped the writes:
/// [`io::ErrorKind::WriteZero`] if one wrote nothing.
///
/// Dropping it cancels the write in flight, which may have written some of
/// the buffer by then, and the buffer is dropped once the cancellation has
/// completed.
#[must_use = "futures do nothing unless polled"]
pub struct WriteAll<'a, B: IoBuf> {
    target: Target<'a>,
    start: Start<Slice<B>>,
    outcome: Outcome,
    /// Where in a file the next write is to, which a stream's ignores.
    offset: u64,
    /// How much of the buffer is written.
    done: usize,
    state: Whole<Write<'a, Slice<B>>, B>,
}

// Nothing is pinned: the buffer is only ever moved.
impl<B: IoBuf> Unpin for WriteAll<'_, B> {}

impl<'a, B: IoBuf> WriteAll<'a, B> {
    /// A write of all of `buf` to a file, from `offset` on.
    pub(crate) fn at(target: Target<'a>, buf: B, offset: u64) -> Self {
        let start = start_write::<Slice<B>>;
        Self::with(target, buf, offset, start, status_outcome)
    }

    /// A write of all of `buf`, with writes that `start` starts and whose
    /// outcomes `outcome` reads.
    pub(crate) fn with(
        target: Target<'a>,
        buf: B,
        offset: u64,
        start: Start<Slice<B>>,
        outcome: Outcome,
    ) -> Self {
        Self {
            target,
            start,
            outcome,
            offset,
            done: 0,
            state: Whole::Start(buf),
        }
    }
}

impl<B: IoBuf> Future for WriteAll<'_, B> {
    type Output = BufResult<(), B>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            let buf = match mem::replace(&mut this.state, Whole::Finished) {
                Whole::Start(buf) => buf,
                Whole::Moving(mut write) => {
                    let Poll::Ready((result, rest)) = Pin::new(&mut write).poll(cx) else {
                        this.state = Whole::Moving(write);
                        return Poll::Pending;
                    };
                    let buf = rest.into_inner();
                    match result {
                        Ok(0) => {
                            let err = io::ErrorKind::WriteZero.into();
                            return Poll::Ready((Err(err), buf));
                        }
                        Ok(written) => {
                            this.done += written;
                            this.offset += written as u64;
                        }
                        Err(err) => return Poll::Ready((Err(err), buf)),
                    }
                    buf
                }
                Whole::Finished => panic!("`WriteAll` polled after it completed"),
            };
            if this.done >= buf.bytes_init() {
                return Poll::Ready((Ok(()), buf));
            }
            let rest = buf.slice(this.done..);
            let write = Write::with(this.target, rest, this.offset, this.start, this.outcome);
            this.state = Whole::Moving(write);
        }
    }
}

impl<B: IoBuf> fmt::Debug for WriteAll<'_, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteAll")
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::fs;
    use std::io::Write as _;
    use std::iter;
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::io::{FromRawHandle, OwnedHandle};
    use std::rc::Rc;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use ruio_engine::{Build, Engine};

    use super::*;
    use crate::engine::{Builder, Iocp};
    use crate::handle::{IoHandle, Raw};

    /// What a read left to finish by itself told: which read it was, what
    /// it read or the system's error, and whether tasks could be woken.
    type Told = (u8, Result<Vec<u8>, Option<i32>>, bool);

    /// What a read of a pipe that is left to finish by itself uses, which
    /// tells how the read went.
    struct Probe {
        id: u8,
        buf: Vec<u8>,
        /// The pipe's end, which stays open until the read has finished.
        pipe: Arc<fs::File>,
        told: Rc<RefCell<Vec<Told>>>,
    }

    impl Detached for Probe {
        unsafe fn complete(mut self, overlapped: *const sys::Overlapped, bytes: u32, wake: bool) {
            // SAFETY: the caller's promise, and the pipe's end is open.
            let read = unsafe { status_outcome(self.pipe.raw(), overlapped, bytes) };
            let read = match read {
                Ok(read) => {
                    // SAFETY: the system wrote that many bytes from the
                    // start of the buffer, within its capacity.
                    unsafe { self.buf.set_len(read as usize) };
                    Ok(mem::take(&mut self.buf))
                }
                Err(err) => Err(err.raw_os_error()),
            };
            self.told.borrow_mut().push((self.id, read, wake));
        }
    }

    /// Starts a probe's read, as `Start` says.
    ///
    /// # Safety
    ///
    /// As for `Start`.
    unsafe fn start_probe(
        pipe: sys::Handle,
        probe: &mut Probe,
        overlapped: *mut sys::Overlapped,
    ) -> io::Result<()> {
        // SAFETY: the caller's promise.
        unsafe { start_read(pipe, &mut probe.buf, overlapped) }
    }

    fn pipe_name(test: &str) -> String {
        format!(r"\\.\pipe\ruio-iocp-op-{}-{test}", std::process::id())
    }

    /// The server's end of a new pipe `name`, opened for overlapped I/O and
    /// associated with no port.
    fn server(name: &str) -> io::Result<Arc<fs::File>> {
        let wide: Vec<u16> = OsStr::new(name)
            .encode_wide()
            .chain(iter::once(0))
            .collect();
        // SAFETY: the name is UTF-16 ended by a zero, and the pipe gets the
        // default security, given no attributes.
        let handle = unsafe {
            sys::CreateNamedPipeW(
                wide.as_ptr(),
                sys::PIPE_ACCESS_DUPLEX
                    | sys::FILE_FLAG_OVERLAPPED
                    | sys::FILE_FLAG_FIRST_PIPE_INSTANCE,
                sys::PIPE_REJECT_REMOTE_CLIENTS,
                1,
                4096,
                4096,
                0,
                ptr::null(),
            )
        };
        if handle == sys::INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the handle was just opened, and nothing else owns it.
        let handle = unsafe { OwnedHandle::from_raw_handle(handle) };
        Ok(Arc::new(fs::File::from(handle)))
    }

    /// The client's end of the pipe `name`, as the standard library opens
    /// it: its writes go through no port.
    fn client(name: &str) -> io::Result<fs::File> {
        fs::OpenOptions::new().read(true).write(true).open(name)
    }

    /// Leaves a read of the pipe's end of `handle` to finish by itself, as
    /// the read `id`.
    fn probe(
        handle: &IoHandle<Arc<fs::File>>,
        id: u8,
        told: &Rc<RefCell<Vec<Told>>>,
    ) -> Result<(), (io::Error, Probe)> {
        let probe = Probe {
            id,
            buf: Vec::with_capacity(16),
            pipe: Arc::clone(handle.io()),
            told: Rc::clone(told),
        };
        detach(handle.target(), probe, 0, start_probe)
    }

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

    #[test]
    fn a_failed_call_completed_its_operation_unless_pending_or_an_error() {
        // STATUS_PENDING: the system never set a status.
        assert!(!completed(0x103));
        // STATUS_BUFFER_OVERFLOW, a warning, as a datagram longer than its
        // buffer completes.
        assert!(completed(0x8000_0005));
        assert!(completed(0));
        // STATUS_PIPE_BROKEN and STATUS_CONNECTION_RESET, errors.
        assert!(!completed(0xC000_014B));
        assert!(!completed(0xC000_020D));
        // Only the low half of `Internal` is the status.
        assert!(!completed(0xFFFF_FFFF_0000_0103_u64 as usize));
    }

    /// Turns `engine`'s port until `done` says so, for a while at most.
    fn turn_until(engine: &Iocp, done: impl Fn() -> bool) -> io::Result<()> {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !done() && Instant::now() < deadline {
            engine.park(Some(Duration::from_millis(5)))?;
        }
        Ok(())
    }

    #[test]
    fn operations_left_to_finish_by_themselves_tell_how_they_went() -> io::Result<()> {
        let engine = Builder::new().build()?;
        let _entered = engine.enter();
        let told = Rc::new(RefCell::new(Vec::new()));
        let mut ends = Vec::new();
        for id in 0..3 {
            let name = pipe_name(&format!("told-{id}"));
            let server = IoHandle::new(server(&name)?)?;
            let client = client(&name)?;
            probe(&server, id, &told).map_err(|(err, _)| err)?;
            ends.push((server, client));
        }
        // Nothing is written, so nothing is told.
        engine.park(Some(Duration::from_millis(20)))?;
        assert!(told.borrow().is_empty());

        // The reads finish in another order than they started in.
        let order = [1_u8, 0, 2];
        for id in order {
            let mut client = &ends[usize::from(id)].1;
            client.write_all(&[b'a' + id; 4])?;
            turn_until(&engine, || {
                told.borrow().last().is_some_and(|last| last.0 == id)
            })?;
        }
        let expected: Vec<Told> = order
            .into_iter()
            .map(|id| (id, Ok(vec![b'a' + id; 4]), true))
            .collect();
        assert_eq!(*told.borrow(), expected);
        Ok(())
    }

    #[test]
    fn an_engine_dropped_cancels_what_was_left_to_finish_by_itself() -> io::Result<()> {
        // Long enough that only the read's cancellation, not the timeout,
        // ends the wait in time.
        let engine = Builder::new()
            .shutdown_timeout(Duration::from_secs(60))
            .build()?;
        let told = Rc::new(RefCell::new(Vec::new()));
        let name = pipe_name("cancelled");
        let server = server(&name)?;
        // The client writes nothing and stays open, so the read is in
        // flight when the engine goes.
        let _client = client(&name)?;
        {
            let _entered = engine.enter();
            let handle = IoHandle::new(Arc::clone(&server))?;
            probe(&handle, 0, &told).map_err(|(err, _)| err)?;
        }
        assert_eq!(Arc::strong_count(&server), 2);
        let started = Instant::now();
        drop(engine);
        assert!(started.elapsed() < Duration::from_secs(30));
        // ERROR_OPERATION_ABORTED, and no task is woken as the engine goes.
        assert_eq!(*told.borrow(), [(0, Err(Some(995)), false)]);
        // The read let go of the pipe's end.
        assert_eq!(Arc::strong_count(&server), 1);
        Ok(())
    }

    #[test]
    fn an_operation_the_system_does_not_take_hands_back_what_it_was_to_use() -> io::Result<()> {
        let engine = Builder::new().build()?;
        let _entered = engine.enter();
        let told = Rc::new(RefCell::new(Vec::new()));
        // No client has opened the pipe, so a read of it fails at once.
        let handle = IoHandle::new(server(&pipe_name("refused"))?)?;
        let (err, back) = probe(&handle, 7, &told).expect_err("a read of a pipe nobody opened");
        assert!(err.raw_os_error().is_some(), "{err}");
        assert_eq!(back.id, 7);
        drop(back);
        engine.park(Some(Duration::from_millis(20)))?;
        assert!(told.borrow().is_empty());
        Ok(())
    }
}
