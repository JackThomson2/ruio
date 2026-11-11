//! Overlapped operations on handles that no port has, which any thread may
//! start and finish: each names an event of its own in its `OVERLAPPED`,
//! which the system signals as the operation completes, and which a
//! [`HandleWait`] waits for on the port of the engine current where the
//! operation is polled.
//!
//! An operation's `OVERLAPPED`, event and buffer live in a slot on the
//! heap, where they stay while the system has the operation. A future
//! dropped while its operation is in flight cancels it, and waits there
//! and then for the cancellation to complete, which takes the system no
//! longer than it takes to stop, so that its buffer, of a type that may
//! not be sent to another thread, is dropped where it belongs. What a pipe
//! leaves in flight as it is dropped finishes on the system's thread pool
//! instead ([`finish_later`]), as a write behind it must go on, and so does
//! a write let go of with [`WriteAt::detach`].

use std::cell::UnsafeCell;
use std::ffi::c_void;
use std::fmt;
use std::future::Future;
use std::io;
use std::mem;
use std::os::windows::io::{AsHandle, AsRawHandle, BorrowedHandle};
use std::pin::Pin;
use std::ptr;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};

use ruio_buf::{BufResult, IoBuf, IoBufMut};

use super::HandleWait;
use crate::sys;

/// The most idle events kept for the operations to come.
const SPARE_EVENTS: usize = 64;

/// Events of operations since done with, by their handles' values, for the
/// next operations to take rather than make events of their own.
static SPARE: Mutex<Vec<usize>> = Mutex::new(Vec::new());

fn spare() -> MutexGuard<'static, Vec<usize>> {
    SPARE.lock().unwrap_or_else(PoisonError::into_inner)
}

/// An event that an operation's `OVERLAPPED` names, which stays signaled
/// once the operation has completed, until the next operation that names
/// it starts, which resets it.
pub(crate) struct Event(sys::Handle);

// SAFETY: an event's handle may be used from any thread.
unsafe impl Send for Event {}

// SAFETY: as for `Send`.
unsafe impl Sync for Event {}

impl Event {
    /// An event an operation left, or a new one.
    pub(crate) fn new() -> io::Result<Self> {
        if let Some(event) = spare().pop() {
            return Ok(Self(ptr::without_provenance_mut(event)));
        }
        // SAFETY: an event with no name and the default security, which
        // stays signaled until reset, and is not signaled to begin with.
        let event = unsafe { sys::CreateEventW(ptr::null(), 1, 0, ptr::null()) };
        if event.is_null() {
            return Err(io::Error::last_os_error());
        }
        Ok(Self(event))
    }

    /// The event, to wait for.
    pub(crate) fn handle(&self) -> BorrowedHandle<'_> {
        // SAFETY: the event is open for as long as `self` is borrowed.
        unsafe { BorrowedHandle::borrow_raw(self.0) }
    }
}

impl Drop for Event {
    fn drop(&mut self) {
        let mut spare = spare();
        if spare.len() < SPARE_EVENTS {
            spare.push(self.0.addr());
            return;
        }
        drop(spare);
        // SAFETY: the event is open, and nothing else has it.
        unsafe { sys::CloseHandle(self.0) };
    }
}

impl fmt::Debug for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Event").field(&self.0).finish()
    }
}

/// `event` as an `OVERLAPPED` names it: with its low bit set, which keeps
/// the operation's completion from being queued to a port, should the
/// handle it is on have one, as a handle duplicated from one a port has
/// does. The system ignores the bit where it uses the event, which it
/// signals all the same, and which `GetOverlappedResult` waits for.
fn tagged(event: sys::Handle) -> sys::Handle {
    event.map_addr(|addr| addr | 1)
}

/// An operation's `OVERLAPPED`, the event it names, and what the operation
/// uses, which stay where they are while the system has the operation. The
/// `OVERLAPPED` names the event [`tagged`], so that the operation's
/// completion goes to no port even on a handle that has one, whose engine
/// would take the slot for an operation of its own.
pub(crate) struct Slot<T> {
    overlapped: UnsafeCell<sys::Overlapped>,
    event: Event,
    /// What the operation uses, its buffer: not to be touched while the
    /// system has the operation.
    pub(crate) payload: T,
}

// SAFETY: the `OVERLAPPED` is written by the system while an operation is
// in flight, and by the owner of the slot otherwise, which holds it
// exclusively; what the operation uses goes where the slot goes.
unsafe impl<T: Send> Send for Slot<T> {}

/// How the system took an operation that a call started.
pub(crate) enum Started {
    /// Completed during the call, having transferred as many bytes.
    AtOnce(u32),
    /// To complete later, and signal the slot's event then.
    Later,
}

impl<T> Slot<T> {
    /// A slot for an operation that uses `payload`, with an event of its own.
    pub(crate) fn new(payload: T) -> Result<Box<Self>, (io::Error, T)> {
        match Event::new() {
            Ok(event) => Ok(Box::new(Self {
                overlapped: UnsafeCell::new(sys::Overlapped {
                    internal: 0,
                    internal_high: 0,
                    offset: 0,
                    offset_high: 0,
                    event: tagged(event.0),
                }),
                event,
                payload,
            })),
            Err(err) => Err((err, payload)),
        }
    }

    /// The slot's event, which the system signals as the operation in
    /// flight completes.
    pub(crate) fn event(&self) -> BorrowedHandle<'_> {
        self.event.handle()
    }

    /// What the operation used, the slot done with.
    #[expect(
        clippy::boxed_local,
        reason = "the slot is made on the heap, where the system has it in place"
    )]
    pub(crate) fn into_payload(self: Box<Self>) -> T {
        self.payload
    }

    /// Whether the operation in flight has completed: the system has
    /// written a status other than pending, as it does before it signals
    /// the event.
    pub(crate) fn completed(&self) -> bool {
        let overlapped = self.overlapped.get();
        // SAFETY: the `OVERLAPPED` is in place, as the slot is; only the
        // address of its status is taken.
        let status = unsafe { &raw const (*overlapped).internal };
        // SAFETY: the status is in place; the system may write it meanwhile,
        // so it is read as memory that changes under the reader.
        let status = unsafe { ptr::read_volatile(status) };
        status as u32 != sys::STATUS_PENDING
    }

    /// Starts reading `handle` at `offset`, ignored for a pipe, into the `len`
    /// bytes at `buf`, under the slot's `OVERLAPPED`.
    ///
    /// # Safety
    ///
    /// `handle` must be open, no operation may be in flight under the slot,
    /// and the slot and the `len` bytes at `buf` must stay valid and in place
    /// until the operation has completed.
    pub(crate) unsafe fn start_read(
        &self,
        handle: sys::Handle,
        offset: u64,
        buf: *mut u8,
        len: u32,
    ) -> io::Result<Started> {
        let overlapped = self.reset(offset);
        // SAFETY: the caller's promise, for the handle, the buffer and the
        // slot; the call writes how much it read to nothing, which the
        // `OVERLAPPED` holds.
        let read = unsafe { sys::ReadFile(handle, buf.cast(), len, ptr::null_mut(), overlapped) };
        // SAFETY: as for the read.
        unsafe { self.started(handle, read) }
    }

    /// Starts writing the `len` bytes at `buf` to `handle` at `offset`,
    /// ignored for a pipe, at the end of the file if it is `u64::MAX`, as
    /// [`start_read`](Slot::start_read) starts a read.
    ///
    /// # Safety
    ///
    /// As for [`start_read`](Slot::start_read).
    pub(crate) unsafe fn start_write(
        &self,
        handle: sys::Handle,
        offset: u64,
        buf: *const u8,
        len: u32,
    ) -> io::Result<Started> {
        let overlapped = self.reset(offset);
        // SAFETY: as for `start_read`.
        let written =
            unsafe { sys::WriteFile(handle, buf.cast(), len, ptr::null_mut(), overlapped) };
        // SAFETY: as for the write.
        unsafe { self.started(handle, written) }
    }

    /// The slot's `OVERLAPPED`, set for an operation at `offset`.
    fn reset(&self, offset: u64) -> *mut sys::Overlapped {
        let overlapped = self.overlapped.get();
        // SAFETY: no operation is in flight, by the callers' promise, so the
        // `OVERLAPPED` is the slot's alone.
        unsafe {
            *overlapped = sys::Overlapped {
                internal: sys::STATUS_PENDING as usize,
                internal_high: 0,
                offset: offset as u32,
                offset_high: (offset >> 32) as u32,
                event: tagged(self.event.0),
            };
        }
        overlapped
    }

    /// How the system took the operation whose call returned `called`.
    ///
    /// # Safety
    ///
    /// `handle` must be open, and the operation the last one started under
    /// the slot, on it.
    unsafe fn started(&self, handle: sys::Handle, called: sys::Bool) -> io::Result<Started> {
        if called != 0 {
            // SAFETY: the caller's promise; the operation has completed.
            return unsafe { self.outcome(handle) }.map(Started::AtOnce);
        }
        match sys::GetLastError() {
            sys::ERROR_IO_PENDING => Ok(Started::Later),
            code => Err(io::Error::from_raw_os_error(code as i32)),
        }
    }

    /// The outcome of the operation, which has completed: how many bytes it
    /// transferred, or the error it failed with.
    ///
    /// # Safety
    ///
    /// `handle` must be open, and the operation the last one started under
    /// the slot, on it, and completed.
    pub(crate) unsafe fn outcome(&self, handle: sys::Handle) -> io::Result<u32> {
        let mut transferred = 0;
        // SAFETY: the caller's promise; the call reads the `OVERLAPPED` and
        // waits for nothing.
        let done =
            unsafe { sys::GetOverlappedResult(handle, self.overlapped.get(), &mut transferred, 0) };
        if done == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(transferred)
    }

    /// Cancels the operation in flight, and waits for it to complete, which
    /// a cancelled one does as soon as the system has stopped it.
    ///
    /// # Safety
    ///
    /// `handle` must be open, and the operation the last one started under
    /// the slot, on it.
    pub(crate) unsafe fn cancel(&self, handle: sys::Handle) {
        if self.completed() {
            return;
        }
        let overlapped = self.overlapped.get();
        // SAFETY: the caller's promise; one that has completed meanwhile is
        // not found, and nothing is cancelled.
        unsafe { sys::CancelIoEx(handle, overlapped) };
        let mut transferred = 0;
        // SAFETY: the caller's promise; the call waits on the slot's event,
        // which the operation's completion signals.
        unsafe { sys::GetOverlappedResult(handle, overlapped, &mut transferred, 1) };
    }
}

impl<T> fmt::Debug for Slot<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Slot")
            .field("event", &self.event)
            .field("completed", &self.completed())
            .finish_non_exhaustive()
    }
}

/// What [`finish_later`] leaves to the thread pool.
struct Finishing {
    /// Held, never read, to be dropped once the operation has completed.
    _payload: Box<dyn Send>,
}

/// Leaves `payload`, which an operation in flight uses, to the system's
/// thread pool, which drops it once `event` is signaled, as the operation
/// completes. `payload` must hold the event open until then.
///
/// Should the pool refuse the wait, `payload` is leaked instead, as the
/// system may use it still.
pub(crate) fn finish_later(event: BorrowedHandle<'_>, payload: Box<dyn Send>) {
    /// Drops what was left, and the pool's wait, from the pool's thread.
    unsafe extern "system" fn finished(
        _instance: *mut c_void,
        context: *mut c_void,
        wait: *mut c_void,
        _result: u32,
    ) {
        // SAFETY: the context is the box `finish_later` made, which the
        // pool hands to the callback once.
        drop(unsafe { Box::from_raw(context.cast::<Finishing>()) });
        // SAFETY: the wait is the pool's, which this callback is of; it is
        // freed once the callback returns.
        unsafe { sys::CloseThreadpoolWait(wait) };
    }

    let context = Box::into_raw(Box::new(Finishing { _payload: payload }));
    // SAFETY: the callback takes the context, a box of `Finishing`, back
    // once; no environment is given, for the process's default pool.
    let wait = unsafe { sys::CreateThreadpoolWait(finished, context.cast(), ptr::null()) };
    if wait.is_null() {
        // SAFETY: the box `into_raw` made, which no callback took; leaked,
        // as the system may still use what it holds.
        mem::forget(unsafe { Box::from_raw(context) });
        return;
    }
    // SAFETY: the wait is new, and the event is open until the callback
    // drops what holds it; with no timeout, the callback runs once the event
    // is signaled, at once if it is already.
    unsafe { sys::SetThreadpoolWait(wait, event.as_raw_handle(), ptr::null()) };
}

/// A read of a handle that no port has, at an offset, into an owned buffer,
/// which any thread may poll: [`read_at`] makes it.
///
/// The read starts as it is first polled, and may complete there; one that
/// waits does so with a [`HandleWait`], on the port of the engine current on
/// the thread that polls it. The output is how many bytes were read, 0 at
/// the end of a file or a pipe, and the buffer, of which that many are then
/// initialized from its start. Dropped while the read is in flight, it
/// cancels the read, and waits for the cancellation to complete.
///
/// # Panics
///
/// If polled after it completed.
#[must_use = "a read does nothing until polled"]
pub struct ReadAt<'a, B> {
    handle: BorrowedHandle<'a>,
    offset: u64,
    state: State<B>,
    wait: HandleWait,
}

/// A write to a handle that no port has, at an offset, from an owned
/// buffer, which any thread may poll: [`write_at`] makes it.
///
/// It runs as a [`ReadAt`] does. The output is how many bytes were written,
/// and the buffer.
///
/// # Panics
///
/// If polled after it completed.
#[must_use = "a write does nothing until polled"]
pub struct WriteAt<'a, B> {
    handle: BorrowedHandle<'a>,
    offset: u64,
    state: State<B>,
    wait: HandleWait,
}

enum State<B> {
    Unstarted(B),
    Started(Box<Slot<B>>),
    Finished,
}

/// Reads `handle`, opened for overlapped I/O and associated with no port,
/// into `buf`, up to its `bytes_total`, from `offset`; see [`ReadAt`]. A
/// handle not opened for overlapped I/O is read during the first poll.
pub fn read_at<B: IoBufMut>(handle: BorrowedHandle<'_>, buf: B, offset: u64) -> ReadAt<'_, B> {
    ReadAt {
        handle,
        offset,
        state: State::Unstarted(buf),
        wait: HandleWait::new(),
    }
}

/// Writes `buf`'s initialized bytes to `handle`, opened for overlapped I/O
/// and associated with no port, at `offset`, at the end of the file if it
/// is `u64::MAX`; see [`WriteAt`].
pub fn write_at<B: IoBuf>(handle: BorrowedHandle<'_>, buf: B, offset: u64) -> WriteAt<'_, B> {
    WriteAt {
        handle,
        offset,
        state: State::Unstarted(buf),
        wait: HandleWait::new(),
    }
}

/// Whether `err` is the end a read found, of a file or of a pipe.
pub(crate) fn is_end(err: &io::Error) -> bool {
    matches!(
        err.raw_os_error().map(|code| code as u32),
        Some(sys::ERROR_HANDLE_EOF | sys::ERROR_BROKEN_PIPE)
    )
}

/// Polls an operation under `state`, which `start` starts with what it is
/// to use, waiting with `wait`: the outcome, the end of a read as 0 bytes if
/// `reads`, and what it used.
fn poll_op<B>(
    cx: &mut Context<'_>,
    handle: BorrowedHandle<'_>,
    state: &mut State<B>,
    wait: &mut HandleWait,
    reads: bool,
    start: impl FnOnce(&Slot<B>) -> io::Result<Started>,
) -> Poll<(io::Result<u32>, B)> {
    let ended = |result: io::Result<u32>| match result {
        Err(err) if reads && is_end(&err) => Ok(0),
        other => other,
    };
    let raw = handle.as_raw_handle();
    let mut start = Some(start);
    loop {
        match mem::replace(state, State::Finished) {
            State::Unstarted(payload) => {
                let slot = match Slot::new(payload) {
                    Ok(slot) => slot,
                    Err((err, payload)) => return Poll::Ready((Err(err), payload)),
                };
                let start = start.take().expect("an operation starts once");
                match start(&slot) {
                    Ok(Started::AtOnce(done)) => {
                        return Poll::Ready((Ok(done), slot.into_payload()));
                    }
                    Ok(Started::Later) => *state = State::Started(slot),
                    Err(err) => return Poll::Ready((ended(Err(err)), slot.into_payload())),
                }
            }
            State::Started(slot) => {
                if !slot.completed() {
                    match wait.poll_wait(cx, slot.event()) {
                        Poll::Pending => {
                            *state = State::Started(slot);
                            return Poll::Pending;
                        }
                        Poll::Ready(Ok(())) => {
                            // Signaled, the event says the operation has
                            // completed.
                            *state = State::Started(slot);
                            continue;
                        }
                        Poll::Ready(Err(err)) => {
                            // SAFETY: the handle is open, as it is borrowed,
                            // and the operation is the slot's last.
                            unsafe { slot.cancel(raw) };
                            return Poll::Ready((Err(err), slot.into_payload()));
                        }
                    }
                }
                // SAFETY: the handle is open, as it is borrowed, and the
                // operation the slot's last, which has completed.
                let outcome = unsafe { slot.outcome(raw) };
                return Poll::Ready((ended(outcome), slot.into_payload()));
            }
            State::Finished => panic!("an operation polled after it completed"),
        }
    }
}

/// Cancels the operation under `state` if it is in flight, and waits for it.
fn cancel_op<B>(handle: BorrowedHandle<'_>, state: &State<B>) {
    if let State::Started(slot) = state {
        // SAFETY: the handle is open, as it is borrowed, and the operation
        // the slot's last.
        unsafe { slot.cancel(handle.as_raw_handle()) };
    }
}

impl<B: IoBufMut> Future for ReadAt<'_, B> {
    type Output = BufResult<usize, B>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let (handle, offset) = (this.handle, this.offset);
        let start = |slot: &Slot<B>| {
            let buf = slot.payload.stable_ptr().cast_mut();
            let len = u32::try_from(slot.payload.bytes_total()).unwrap_or(u32::MAX);
            // SAFETY: the handle is open, as it is borrowed; the slot, which
            // owns the buffer, stays in place on the heap until the read has
            // completed, as the future waits for it, or cancels it and waits
            // as it is dropped; and the buffer has room for `len` bytes.
            unsafe { slot.start_read(handle.as_raw_handle(), offset, buf, len) }
        };
        let (result, mut buf) = std::task::ready!(poll_op(
            cx,
            handle,
            &mut this.state,
            &mut this.wait,
            true,
            start
        ));
        let result = result.map(|read| {
            let read = read as usize;
            // SAFETY: the system wrote the first `read` bytes of the buffer.
            unsafe { buf.set_init(read) };
            read
        });
        Poll::Ready((result, buf))
    }
}

impl<B: IoBuf> Future for WriteAt<'_, B> {
    type Output = BufResult<usize, B>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let (handle, offset) = (this.handle, this.offset);
        let start = |slot: &Slot<B>| {
            let len = u32::try_from(slot.payload.bytes_init()).unwrap_or(u32::MAX);
            // SAFETY: as for a read; the buffer's first `len` bytes are
            // initialized.
            unsafe {
                slot.start_write(
                    handle.as_raw_handle(),
                    offset,
                    slot.payload.stable_ptr(),
                    len,
                )
            }
        };
        let (result, buf) = std::task::ready!(poll_op(
            cx,
            handle,
            &mut this.state,
            &mut this.wait,
            false,
            start
        ));
        Poll::Ready((result.map(|written| written as usize), buf))
    }
}

impl<B: Send + 'static> WriteAt<'_, B> {
    /// Lets the write go on without its future, which cancels it if
    /// dropped: once it has completed, the system's thread pool drops its
    /// buffer, and `owner`, which holds the handle written open until then,
    /// as the system writes it meanwhile. Of a write that is not in flight,
    /// not started yet or completed, the buffer and `owner` are dropped at
    /// once, and nothing more is written.
    ///
    /// # Panics
    ///
    /// If `owner`'s handle is not the one the write names.
    pub fn detach<H: AsHandle + Send + 'static>(mut self, owner: H) {
        assert_eq!(
            owner.as_handle().as_raw_handle(),
            self.handle.as_raw_handle(),
            "a write goes on with the handle it writes"
        );
        let State::Started(slot) = mem::replace(&mut self.state, State::Finished) else {
            return;
        };
        if slot.completed() {
            return;
        }
        let event = slot.event().as_raw_handle();
        // SAFETY: the event is the slot's, which the thread pool holds open
        // until it drops it.
        let event = unsafe { BorrowedHandle::borrow_raw(event) };
        finish_later(event, Box::new((slot, owner)));
    }
}

// A future's buffer is never pinned: it moves into the slot, and out of it.
impl<B> Unpin for ReadAt<'_, B> {}
impl<B> Unpin for WriteAt<'_, B> {}

impl<B> Drop for ReadAt<'_, B> {
    fn drop(&mut self) {
        cancel_op(self.handle, &self.state);
    }
}

impl<B> Drop for WriteAt<'_, B> {
    fn drop(&mut self) {
        cancel_op(self.handle, &self.state);
    }
}

impl<B> fmt::Debug for ReadAt<'_, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadAt")
            .field("handle", &self.handle)
            .field("offset", &self.offset)
            .finish_non_exhaustive()
    }
}

impl<B> fmt::Debug for WriteAt<'_, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteAt")
            .field("handle", &self.handle)
            .field("offset", &self.offset)
            .finish_non_exhaustive()
    }
}
