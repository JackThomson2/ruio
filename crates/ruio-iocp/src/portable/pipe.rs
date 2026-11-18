//! A pipe's end that no port has, which any thread may read and write with
//! calls that do not wait, as a nonblocking descriptor is read on Unix.
//!
//! A pipe gives no sign that it has data, or room, short of an operation
//! that completes, so a [`Pipe`] keeps a read and a write of its own in
//! flight, each with a buffer of its own, as mio's pipes do: a read takes
//! what its read in flight brought, and starts the next once that is all
//! taken; a write copies the caller's bytes into its buffer and starts
//! writing them, done as far as the caller can tell, and the next waits
//! for it to finish, and fails with its error if it failed. The pipe is
//! ready to read once its read in flight has completed, and to write once
//! its write has: waits a [`HandleWait`] makes for the event that the
//! operation's `OVERLAPPED` names, on the port of the engine current where
//! it is polled.
//!
//! A pipe dropped cancels its read in flight, and leaves its write in
//! flight to finish on the system's thread pool, which closes the handle
//! once it has, so that what was written reaches the reader before the end
//! does.

use std::fmt;
use std::io::{self, IoSlice};
use std::mem::{self, MaybeUninit};
use std::os::windows::io::{AsHandle, AsRawHandle, BorrowedHandle, OwnedHandle};
use std::ptr;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, ready};

use super::HandleWait;
use super::overlapped::{Slot, Started, finish_later, is_end};

/// How many bytes one read or write of a pipe moves at most, the size of
/// each of its buffers.
const BUFFER: usize = 64 << 10;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A pipe's end, opened for overlapped I/O and associated with no port,
/// read and written with calls that do not wait, from any thread.
///
/// It keeps a read and a write of its own in flight, each with a buffer of
/// 64 KiB: a read takes what the read in flight brought, and starts the
/// next once that is all taken; a write copies the caller's bytes into the
/// buffer and starts writing them, and the next waits for it to finish.
/// Dropped, it cancels its read, and leaves its write to finish on the
/// system's thread pool, which closes the handle after it.
pub struct Pipe {
    /// Taken as the pipe is dropped, or its handle taken out.
    inner: Option<Box<Inner>>,
}

struct Inner {
    handle: OwnedHandle,
    read: Mutex<Side>,
    write: Mutex<Side>,
}

/// A way of a pipe: its operation's slot, made as it is first needed, and
/// where the operation stands.
struct Side {
    slot: Option<Box<Slot<Vec<u8>>>>,
    state: State,
}

enum State {
    /// No operation is in flight, and nothing is left of the last.
    Idle,
    /// A read is in flight into the slot's buffer.
    Reading,
    /// A read brought the slot's buffer, of which the bytes from `start` on
    /// are left to take.
    Filled { start: usize },
    /// The writer has closed its end, and every read reads 0 bytes.
    Ended,
    /// A write is in flight of the slot's buffer, of which the first `done`
    /// bytes had been written before.
    Writing { done: usize },
    /// What a read in flight failed with, or a write behind, which the
    /// next call that way reports.
    Failed(io::Error),
}

impl Side {
    const fn new() -> Self {
        Self {
            slot: None,
            state: State::Idle,
        }
    }

    /// The side's slot, made if it has none.
    fn slot(&mut self) -> io::Result<&mut Slot<Vec<u8>>> {
        if self.slot.is_none() {
            let slot = Slot::new(Vec::with_capacity(BUFFER)).map_err(|(err, _)| err)?;
            self.slot = Some(slot);
        }
        Ok(self.slot.as_deref_mut().expect("the slot was just made"))
    }

    /// Starts a read of `handle` into the slot's buffer, and records how it
    /// went.
    fn start_read(&mut self, handle: &OwnedHandle) {
        let started = self.slot().and_then(|slot| {
            slot.payload.clear();
            let buf = slot.payload.as_mut_ptr();
            // SAFETY: the handle is open; no read is in flight; and the slot
            // stays in place on the heap, with its buffer, which has room
            // for `BUFFER` bytes, until the read has completed, as the pipe
            // waits for it before it lets go of the slot.
            unsafe { slot.start_read(handle.as_raw_handle(), 0, buf, BUFFER as u32) }
        });
        match started {
            Ok(Started::AtOnce(read)) => self.finish_read(Ok(read)),
            Ok(Started::Later) => self.state = State::Reading,
            Err(err) => self.finish_read(Err(err)),
        }
    }

    /// Records what a read in flight brought, if it has completed, and says
    /// whether it has.
    fn collect_read(&mut self, handle: &OwnedHandle) -> bool {
        let slot = self.slot.as_ref().expect("a read in flight has its slot");
        if !slot.completed() {
            return false;
        }
        // SAFETY: the handle is open, and the read the slot's last, which has
        // completed.
        let outcome = unsafe { slot.outcome(handle.as_raw_handle()) };
        self.finish_read(outcome);
        true
    }

    fn finish_read(&mut self, outcome: io::Result<u32>) {
        self.state = match outcome {
            // A writer's write of no bytes completes a read with none, which
            // is no end: the next read waits for more.
            Ok(0) => State::Idle,
            Ok(read) => {
                let slot = self.slot.as_mut().expect("a read has its slot");
                // SAFETY: the system wrote the first `read` bytes of the
                // buffer, within its capacity.
                unsafe { slot.payload.set_len(read as usize) };
                State::Filled { start: 0 }
            }
            Err(err) if is_end(&err) => State::Ended,
            Err(err) => State::Failed(err),
        };
    }

    /// Starts writing what the slot's buffer holds past its first `done`
    /// bytes to `handle`, and records how it went: as many writes as it
    /// takes while they complete at once.
    fn start_write(&mut self, handle: &OwnedHandle, done: usize) {
        let mut done = done;
        loop {
            let slot = self.slot.as_ref().expect("a write has its slot");
            let len = slot.payload.len();
            if done >= len {
                self.state = State::Idle;
                return;
            }
            // SAFETY: the handle is open; no write is in flight; and the
            // slot stays in place on the heap, with its buffer, whose bytes
            // past `done` are initialized, until the write has completed, as
            // the pipe waits for it, or leaves the slot to finish it.
            let started = unsafe {
                slot.start_write(
                    handle.as_raw_handle(),
                    0,
                    slot.payload[done..].as_ptr(),
                    (len - done) as u32,
                )
            };
            match started {
                Ok(Started::AtOnce(written)) => done += written as usize,
                Ok(Started::Later) => {
                    self.state = State::Writing { done };
                    return;
                }
                Err(err) => {
                    self.state = State::Failed(err);
                    return;
                }
            }
        }
    }

    /// Records how the write in flight went, if one is and it has completed,
    /// going on with what it left, and says whether no write is in flight
    /// now: a write the system took in part leaves the rest to a write of
    /// its own, which is.
    fn collect_write(&mut self, handle: &OwnedHandle) -> bool {
        let State::Writing { done } = self.state else {
            return true;
        };
        let slot = self.slot.as_ref().expect("a write in flight has its slot");
        if !slot.completed() {
            return false;
        }
        // SAFETY: the handle is open, and the write the slot's last, which
        // has completed.
        match unsafe { slot.outcome(handle.as_raw_handle()) } {
            Ok(written) => self.start_write(handle, done + written as usize),
            Err(err) => self.state = State::Failed(err),
        }
        !matches!(self.state, State::Writing { .. })
    }

    /// Whether an operation is in flight, started and not completed. An
    /// operation that failed as it started leaves its slot's status as it
    /// was set, pending, though nothing is in flight.
    fn in_flight(&self) -> bool {
        matches!(self.state, State::Reading | State::Writing { .. })
            && self.slot.as_ref().is_some_and(|slot| !slot.completed())
    }

    /// The slot's event, while an operation is in flight.
    fn event(&self) -> BorrowedHandle<'_> {
        self.slot
            .as_ref()
            .expect("an operation in flight has its slot")
            .event()
    }
}

impl Pipe {
    /// Takes over `handle`, a pipe's end opened for overlapped I/O and
    /// associated with no port, as the standard library opens the parent's
    /// ends of a child's pipes.
    pub fn new(handle: OwnedHandle) -> Self {
        Self {
            inner: Some(Box::new(Inner {
                handle,
                read: Mutex::new(Side::new()),
                write: Mutex::new(Side::new()),
            })),
        }
    }

    fn inner(&self) -> &Inner {
        self.inner
            .as_deref()
            .expect("a pipe has its inner part until dropped")
    }

    /// Reads into `buf` what the pipe's read in flight has brought, starting
    /// one if none is: as much of it as `buf` takes, the rest left for the
    /// next read; 0 once the writer has closed its end and everything has
    /// been read; or `WouldBlock` if nothing has arrived. Makes no call to
    /// the system unless a read is to start, or to be asked how it went.
    ///
    /// # Errors
    ///
    /// `WouldBlock`, and what the read in flight failed with, once.
    pub fn try_read(&self, buf: &mut [MaybeUninit<u8>]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let inner = self.inner();
        let mut side = lock(&inner.read);
        loop {
            match &mut side.state {
                State::Filled { start } => {
                    let start = *start;
                    let slot = side.slot.as_ref().expect("a read that filled its slot");
                    let left = &slot.payload[start..];
                    let taken = left.len().min(buf.len());
                    // SAFETY: both ranges are `taken` bytes long, within their
                    // slices, which do not overlap; the source is initialized.
                    unsafe {
                        ptr::copy_nonoverlapping(left.as_ptr(), buf.as_mut_ptr().cast(), taken);
                    }
                    side.state = if taken == left.len() {
                        State::Idle
                    } else {
                        State::Filled {
                            start: start + taken,
                        }
                    };
                    return Ok(taken);
                }
                State::Ended => return Ok(0),
                State::Failed(_) => {
                    let State::Failed(err) = mem::replace(&mut side.state, State::Idle) else {
                        unreachable!("the state was matched as failed");
                    };
                    return Err(err);
                }
                State::Reading => {
                    if !side.collect_read(&inner.handle) {
                        return Err(io::ErrorKind::WouldBlock.into());
                    }
                }
                State::Idle | State::Writing { .. } => {
                    side.start_read(&inner.handle);
                    if let State::Reading = side.state {
                        return Err(io::ErrorKind::WouldBlock.into());
                    }
                }
            }
        }
    }

    /// Writes as much of `buf` as the pipe's buffer takes, up to 64 KiB, by
    /// copying it there and starting a write of it, done as far as the
    /// caller can tell; or `WouldBlock` while the last write is in flight.
    ///
    /// # Errors
    ///
    /// `WouldBlock`, what starting the write reports, as `BrokenPipe` for a
    /// reader gone, and what the last write failed with, once.
    pub fn try_write(&self, buf: &[u8]) -> io::Result<usize> {
        self.write_with(buf.len(), |into| {
            into.extend_from_slice(&buf[..buf.len().min(BUFFER)]);
        })
    }

    /// Writes as much of `bufs`, one after the other, as the pipe's buffer
    /// takes, as [`try_write`](Pipe::try_write) does.
    ///
    /// # Errors
    ///
    /// As for [`try_write`](Pipe::try_write).
    pub fn try_write_vectored(&self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        let len = bufs.iter().map(|buf| buf.len()).sum();
        self.write_with(len, |into| {
            for buf in bufs {
                let room = BUFFER - into.len();
                if room == 0 {
                    break;
                }
                into.extend_from_slice(&buf[..buf.len().min(room)]);
            }
        })
    }

    /// Writes what `fill` puts in the pipe's buffer, of what `len` bytes
    /// offer, once no write is in flight.
    fn write_with(&self, len: usize, fill: impl FnOnce(&mut Vec<u8>)) -> io::Result<usize> {
        if len == 0 {
            return Ok(0);
        }
        let inner = self.inner();
        let mut side = lock(&inner.write);
        if let State::Writing { .. } = side.state
            && !side.collect_write(&inner.handle)
        {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        if let State::Failed(_) = side.state {
            let State::Failed(err) = mem::replace(&mut side.state, State::Idle) else {
                unreachable!("the state was matched as failed");
            };
            return Err(err);
        }
        let slot = side.slot()?;
        slot.payload.clear();
        fill(&mut slot.payload);
        let written = slot.payload.len();
        side.start_write(&inner.handle, 0);
        if let State::Failed(_) = side.state {
            let State::Failed(err) = mem::replace(&mut side.state, State::Idle) else {
                unreachable!("the state was matched as failed");
            };
            return Err(err);
        }
        Ok(written)
    }

    /// Waits with `wait` for the pipe to be readable: for its read in flight,
    /// which this starts if none is, to have completed, with data, the end
    /// or an error, which [`try_read`](Pipe::try_read) then takes.
    ///
    /// # Errors
    ///
    /// What the wait reports: `Unsupported` if the thread has no engine
    /// current and the pipe is not readable.
    pub fn poll_read_ready(
        &self,
        cx: &mut Context<'_>,
        wait: &mut HandleWait,
    ) -> Poll<io::Result<()>> {
        let inner = self.inner();
        let event = {
            let mut side = lock(&inner.read);
            loop {
                match side.state {
                    State::Filled { .. } | State::Ended | State::Failed(_) => {
                        return Poll::Ready(Ok(()));
                    }
                    State::Reading => {
                        if !side.collect_read(&inner.handle) {
                            break side.event().as_raw_handle();
                        }
                    }
                    State::Idle | State::Writing { .. } => side.start_read(&inner.handle),
                }
            }
        };
        // SAFETY: the event is the read side's, which the pipe, borrowed,
        // keeps open.
        let event = unsafe { BorrowedHandle::borrow_raw(event) };
        ready!(wait.poll_wait(cx, event))?;
        Poll::Ready(Ok(()))
    }

    /// Waits with `wait` for the pipe to be writable: for its write in
    /// flight, if one is, to have completed.
    ///
    /// # Errors
    ///
    /// What the wait reports: `Unsupported` if the thread has no engine
    /// current and a write is in flight.
    pub fn poll_write_ready(
        &self,
        cx: &mut Context<'_>,
        wait: &mut HandleWait,
    ) -> Poll<io::Result<()>> {
        let inner = self.inner();
        let event = {
            let mut side = lock(&inner.write);
            if !matches!(side.state, State::Writing { .. }) || side.collect_write(&inner.handle) {
                return Poll::Ready(Ok(()));
            }
            side.event().as_raw_handle()
        };
        // SAFETY: the event is the write side's, which the pipe, borrowed,
        // keeps open.
        let event = unsafe { BorrowedHandle::borrow_raw(event) };
        ready!(wait.poll_wait(cx, event))?;
        Poll::Ready(Ok(()))
    }

    /// The pipe's handle, with its read and write in flight cancelled, and
    /// what they brought, or held, dropped.
    pub fn into_handle(mut self) -> OwnedHandle {
        let inner = self
            .inner
            .take()
            .expect("a pipe has its inner part until dropped");
        let Inner {
            handle,
            read,
            write,
        } = *inner;
        for side in [read, write] {
            let side = side.into_inner().unwrap_or_else(PoisonError::into_inner);
            if side.in_flight()
                && let Some(slot) = &side.slot
            {
                // SAFETY: the handle is open, and the operation in flight the
                // slot's last.
                unsafe { slot.cancel(handle.as_raw_handle()) };
            }
        }
        handle
    }
}

impl AsHandle for Pipe {
    fn as_handle(&self) -> BorrowedHandle<'_> {
        self.inner().handle.as_handle()
    }
}

impl AsRawHandle for Pipe {
    fn as_raw_handle(&self) -> std::os::windows::io::RawHandle {
        self.inner().handle.as_raw_handle()
    }
}

impl Drop for Pipe {
    fn drop(&mut self) {
        let Some(mut inner) = self.inner.take() else {
            return;
        };
        let raw = inner.handle.as_raw_handle();
        let read = inner.read.get_mut().unwrap_or_else(PoisonError::into_inner);
        if read.in_flight()
            && let Some(slot) = &read.slot
        {
            // SAFETY: the handle is open, and the read in flight the slot's
            // last.
            unsafe { slot.cancel(raw) };
        }
        let write = inner
            .write
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner);
        let writing = matches!(write.state, State::Writing { .. }) && write.in_flight();
        if !writing {
            return;
        }
        let event = write.event().as_raw_handle();
        // SAFETY: the event is the write side's, which `inner` holds open
        // until the thread pool drops it.
        let event = unsafe { BorrowedHandle::borrow_raw(event) };
        finish_later(event, inner);
    }
}

impl fmt::Debug for Pipe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pipe")
            .field(
                "handle",
                &self
                    .inner
                    .as_ref()
                    .map(|inner| inner.handle.as_raw_handle()),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io;
    use std::mem::MaybeUninit;
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::{FromRawHandle, OwnedHandle};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};
    use std::time::{Duration, Instant};

    use ruio_engine::{Build, Engine};

    use super::Pipe;
    use crate::engine::{Builder, Iocp};
    use crate::handle::IoHandle;
    use crate::portable::HandleWait;
    use crate::sys;

    struct Woken(AtomicUsize);

    impl Wake for Woken {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn task() -> (Arc<Woken>, Waker) {
        let woken = Arc::new(Woken(AtomicUsize::new(0)));
        (Arc::clone(&woken), Waker::from(woken))
    }

    /// The two ends of a new named pipe, both opened for overlapped I/O and
    /// associated with no port, whose buffers hold `buffer` bytes each way.
    fn pair(test: &str, buffer: u32) -> (Pipe, Pipe) {
        let name = format!(r"\\.\pipe\ruio-iocp-portable-{}-{test}", std::process::id());
        let wide: Vec<u16> = name.encode_utf16().chain([0]).collect();
        // SAFETY: the name is read during the call, which makes the pipe's
        // first instance and returns its server's end.
        let server = unsafe {
            sys::CreateNamedPipeW(
                wide.as_ptr(),
                sys::PIPE_ACCESS_DUPLEX
                    | sys::FILE_FLAG_FIRST_PIPE_INSTANCE
                    | sys::FILE_FLAG_OVERLAPPED,
                sys::PIPE_REJECT_REMOTE_CLIENTS,
                1,
                buffer,
                buffer,
                0,
                std::ptr::null(),
            )
        };
        assert_ne!(server, sys::INVALID_HANDLE_VALUE);
        // SAFETY: the handle is new, and nothing else owns it.
        let server = unsafe { OwnedHandle::from_raw_handle(server) };
        let client = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(sys::FILE_FLAG_OVERLAPPED)
            .open(&name)
            .unwrap();
        (Pipe::new(server), Pipe::new(client.into()))
    }

    /// Reads `pipe` until it has `len` bytes, or ends, waiting on `engine`.
    fn read(engine: &Iocp, pipe: &Pipe, len: usize) -> Vec<u8> {
        let (woken, waker) = task();
        let mut cx = Context::from_waker(&waker);
        let mut wait = HandleWait::new();
        let mut read = Vec::new();
        let started = Instant::now();
        while read.len() < len {
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "read {} bytes",
                read.len()
            );
            let mut buf = vec![MaybeUninit::new(0); (len - read.len()).min(4096)];
            match pipe.try_read(&mut buf) {
                Ok(0) => break,
                // SAFETY: the read initialized as many bytes.
                Ok(taken) => read.extend(
                    buf[..taken]
                        .iter()
                        .map(|byte| unsafe { byte.assume_init() }),
                ),
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    while pipe.poll_read_ready(&mut cx, &mut wait).is_pending() {
                        while woken.0.swap(0, Ordering::SeqCst) == 0 {
                            assert!(started.elapsed() < Duration::from_secs(5), "never readable");
                            engine.park(Some(Duration::from_millis(20))).unwrap();
                        }
                    }
                }
                Err(err) => panic!("the read failed: {err}"),
            }
        }
        read
    }

    #[test]
    fn what_one_end_writes_the_other_reads() {
        let engine = Builder::new().build().unwrap();
        let _entered = engine.enter();
        let (server, client) = pair("ends", 4096);
        let mut buf = [MaybeUninit::new(0); 8];
        let err = server.try_read(&mut buf).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
        assert_eq!(client.try_write(b"ping").unwrap(), 4);
        assert_eq!(read(&engine, &server, 4), b"ping");
        assert_eq!(server.try_write(b"pong").unwrap(), 4);
        assert_eq!(read(&engine, &client, 4), b"pong");
    }

    #[test]
    fn a_read_takes_what_its_buffer_holds_and_leaves_the_rest() {
        let engine = Builder::new().build().unwrap();
        let _entered = engine.enter();
        let (server, client) = pair("rest", 4096);
        assert_eq!(client.try_write(b"abcdef").unwrap(), 6);
        assert_eq!(read(&engine, &server, 2), b"ab");
        let mut buf = [MaybeUninit::new(0); 2];
        assert_eq!(server.try_read(&mut buf).unwrap(), 2);
        assert_eq!(read(&engine, &server, 2), b"ef");
    }

    #[test]
    fn a_write_behind_reaches_the_reader_after_the_writer_is_dropped() {
        let engine = Builder::new().build().unwrap();
        let _entered = engine.enter();
        let (server, client) = pair("behind", 1024);
        let data: Vec<u8> = (0..60_000).map(|i| i as u8).collect();
        assert_eq!(client.try_write(&data).unwrap(), data.len());
        // The pipe's buffer takes a little of it: the rest goes on.
        let (_, waker) = task();
        let mut wait = HandleWait::new();
        assert!(
            client
                .poll_write_ready(&mut Context::from_waker(&waker), &mut wait)
                .is_pending()
        );
        drop(wait);
        drop(client);
        assert_eq!(read(&engine, &server, usize::MAX), data);
    }

    #[test]
    fn a_reader_reads_the_end_once_the_writer_is_gone() {
        let engine = Builder::new().build().unwrap();
        let _entered = engine.enter();
        let (server, client) = pair("end", 4096);
        assert_eq!(client.try_write(b"last").unwrap(), 4);
        drop(client);
        assert_eq!(read(&engine, &server, usize::MAX), b"last");
        let mut buf = [MaybeUninit::new(0); 8];
        assert_eq!(server.try_read(&mut buf).unwrap(), 0);
    }

    #[test]
    fn a_write_with_the_reader_gone_fails() {
        let engine = Builder::new().build().unwrap();
        let _entered = engine.enter();
        let (server, client) = pair("gone", 4096);
        drop(server);
        let err = client.try_write(b"lost").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe, "{err}");
    }

    #[test]
    fn a_pipe_is_read_and_written_from_any_thread() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Pipe>();
        let engine = Builder::new().build().unwrap();
        let _entered = engine.enter();
        let (server, client) = pair("threads", 4096);
        std::thread::spawn(move || {
            // No engine here: a write that need not wait needs none.
            assert_eq!(client.try_write(b"over").unwrap(), 4);
            let (_, waker) = task();
            let mut wait = HandleWait::new();
            match client.poll_write_ready(&mut Context::from_waker(&waker), &mut wait) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(err)) => assert_eq!(err.kind(), io::ErrorKind::Unsupported),
                Poll::Pending => panic!("a wait with no engine current"),
            }
        })
        .join()
        .unwrap();
        assert_eq!(read(&engine, &server, 4), b"over");
    }

    #[test]
    fn the_handle_taken_out_has_nothing_in_flight() {
        let engine = Builder::new().build().unwrap();
        let _entered = engine.enter();
        let (server, client) = pair("taken", 4096);
        let mut buf = [MaybeUninit::new(0); 8];
        assert!(server.try_read(&mut buf).is_err());
        let handle = server.into_handle();
        let server = Pipe::new(handle);
        assert_eq!(client.try_write(b"again").unwrap(), 5);
        assert_eq!(read(&engine, &server, 5), b"again");
    }

    /// A pipe's end that a port has, through a handle of its own on the
    /// same pipe, has no completion of its operations queued to the port,
    /// whose engine would take it for one of its own.
    #[test]
    fn an_end_a_port_has_gets_no_completion_of_its_operations() {
        let engine = Builder::new().build().unwrap();
        let _entered = engine.enter();
        let (server, client) = pair("ported", 4096);
        let handle = server.into_handle();
        let ported = IoHandle::new(handle.try_clone().unwrap()).unwrap();
        let server = Pipe::new(handle);
        let mut buf = [MaybeUninit::new(0); 8];
        let err = server.try_read(&mut buf).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock, "a read in flight");
        assert_eq!(client.try_write(b"ping").unwrap(), 4);
        assert_eq!(read(&engine, &server, 4), b"ping");
        assert_eq!(server.try_write(b"pong").unwrap(), 4);
        assert_eq!(read(&engine, &client, 4), b"pong");
        engine.park(Some(Duration::from_millis(20))).unwrap();
        drop(ported);
    }
}
