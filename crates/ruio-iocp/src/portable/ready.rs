//! Waits for sockets to be ready, which any thread may hold and poll: the
//! AFD driver's poll, through a helper handle of each engine's.
//!
//! Winsock's sockets are handles of the AFD driver, which polls one for
//! readiness with an `IOCTL_AFD_POLL` made on any handle of the driver's:
//! the poll completes, as any operation does, to the port that handle is
//! associated with, once the socket has one of the events asked for, and
//! at once if it has one already. Each engine opens such a handle as a
//! socket first waits on its thread, `\Device\Afd\Ruio`, and associates it
//! with its port under the key of the polls; a [`ReadyWait`] polls through
//! the handle of the engine current on the thread that polls it as it
//! starts. So a socket associated with no port, which any thread may read
//! and write with calls that do not wait, waits for readiness on whichever
//! thread polls it, with one call and one completion, as mio and libuv
//! wait for theirs. The socket itself is not touched: several polls may
//! wait for it at once, each completing on its own.
//!
//! A poll's slot is shared between the wait and the system: the wait hands
//! the system a reference as it starts the poll, which the engine takes
//! back as it delivers the completion, which comes whatever the outcome. A
//! wait dropped or cancelled on any thread cancels its poll, whose
//! completion then comes with nobody to wake. An engine dropped cancels the
//! polls made through its handle, and delivers their completions before it
//! closes the port: a wait whose poll was cancelled so wakes its task, and
//! starts again where it is polled next.

use std::cell::UnsafeCell;
use std::fmt;
use std::io;
use std::mem;
use std::os::windows::io::{AsRawSocket, BorrowedSocket, RawSocket};
use std::ptr;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

use crate::engine::{AFD, Iocp};
use crate::sys;

/// The events of the AFD driver's poll, which [`ReadyWait::poll_ready`]
/// waits for any of, and returns those it found.
pub mod events {
    /// `AFD_POLL_RECEIVE`: data to receive.
    pub const RECEIVE: u32 = 0x0001;
    /// `AFD_POLL_RECEIVE_EXPEDITED`: out-of-band data to receive.
    pub const RECEIVE_EXPEDITED: u32 = 0x0002;
    /// `AFD_POLL_SEND`: room to send.
    pub const SEND: u32 = 0x0004;
    /// `AFD_POLL_DISCONNECT`: the peer shut its sending half down.
    pub const DISCONNECT: u32 = 0x0008;
    /// `AFD_POLL_ABORT`: the connection was reset or aborted.
    pub const ABORT: u32 = 0x0010;
    /// `AFD_POLL_LOCAL_CLOSE`: the socket was closed while polled.
    pub const LOCAL_CLOSE: u32 = 0x0020;
    /// `AFD_POLL_ACCEPT`: a connection for a listener to accept.
    pub const ACCEPT: u32 = 0x0080;
    /// `AFD_POLL_CONNECT_FAIL`: a connection that failed to be made.
    pub const CONNECT_FAIL: u32 = 0x0100;

    /// What a socket that is readable has any of: data, a connection to
    /// accept, its end or an error, which a receive or an accept then takes.
    pub const READABLE: u32 = RECEIVE | DISCONNECT | ACCEPT | ABORT | CONNECT_FAIL;
    /// What a socket that is writable has any of: room, or an error, which
    /// a send then reports; a connection made, too.
    pub const WRITABLE: u32 = SEND | ABORT | CONNECT_FAIL;
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The error a native call's `status` stands for.
fn status_error(status: i32) -> io::Error {
    io::Error::from_raw_os_error(sys::RtlNtStatusToDosError(status) as i32)
}

/// The base socket of `socket`, which the AFD driver's poll names: `socket`
/// itself, unless a layered service provider has made a socket of its own
/// over it, or the provider will not say.
pub fn base_socket(socket: BorrowedSocket<'_>) -> RawSocket {
    let mut base: sys::Socket = sys::INVALID_SOCKET;
    let mut returned = 0;
    // SAFETY: the socket is open, as it is borrowed; the call writes the
    // base socket to `base`, which has room for it, and its size to
    // `returned`, and takes no `OVERLAPPED`, so it completes during the
    // call.
    let failed = unsafe {
        sys::WSAIoctl(
            socket.as_raw_socket() as sys::Socket,
            sys::SIO_BASE_HANDLE,
            ptr::null(),
            0,
            (&raw mut base).cast(),
            size_of::<sys::Socket>() as u32,
            &mut returned,
            ptr::null_mut(),
            ptr::null(),
        )
    };
    if failed != 0 || base == sys::INVALID_SOCKET {
        return socket.as_raw_socket();
    }
    base as RawSocket
}

/// An engine's handle of the AFD driver, associated with its port, which
/// the polls of the waits on its thread are made through.
pub(crate) struct Afd {
    handle: sys::Handle,
}

// SAFETY: a handle may be used from any thread; the polls made through it
// are the system's to order.
unsafe impl Send for Afd {}

// SAFETY: as for `Send`.
unsafe impl Sync for Afd {}

impl Afd {
    /// Opens a handle of the driver and associates it with `port`, under the
    /// key of the polls.
    pub(crate) fn open(port: sys::Handle) -> io::Result<Self> {
        let name: Vec<u16> = r"\Device\Afd\Ruio".encode_utf16().collect();
        let bytes = (name.len() * 2) as u16;
        let name = sys::UnicodeString {
            length: bytes,
            maximum_length: bytes,
            buffer: name.as_ptr(),
        };
        let attributes = sys::ObjectAttributes {
            length: size_of::<sys::ObjectAttributes>() as u32,
            root_directory: ptr::null_mut(),
            object_name: &name,
            attributes: 0,
            security_descriptor: ptr::null(),
            security_quality_of_service: ptr::null(),
        };
        let mut handle = ptr::null_mut();
        let mut status = sys::IoStatusBlock {
            status: 0,
            information: 0,
        };
        // SAFETY: the attributes, and the name they point to, are read
        // during the call, which writes the handle and the outcome to what
        // it is given.
        let opened = unsafe {
            sys::NtCreateFile(
                &mut handle,
                sys::SYNCHRONIZE,
                &attributes,
                &mut status,
                ptr::null(),
                0,
                sys::FILE_SHARE_READ | sys::FILE_SHARE_WRITE,
                sys::FILE_OPEN,
                0,
                ptr::null(),
                0,
            )
        };
        if opened < sys::STATUS_SUCCESS {
            return Err(status_error(opened));
        }
        let afd = Self { handle };
        // SAFETY: the handle and the port are open, and the handle is new.
        let associated = unsafe { sys::CreateIoCompletionPort(afd.handle, port, AFD, 0) };
        if associated.is_null() {
            return Err(io::Error::last_os_error());
        }
        // Nothing waits for the handle itself to be signaled as its polls
        // complete. Should the system not spare it, it is signaled in vain.
        // SAFETY: the handle is open.
        unsafe {
            sys::SetFileCompletionNotificationModes(afd.handle, sys::FILE_SKIP_SET_EVENT_ON_HANDLE)
        };
        Ok(afd)
    }

    /// Cancels every poll made through the handle, as the engine is
    /// dropped: their completions still come to the port.
    pub(crate) fn cancel_all(&self) {
        // SAFETY: the handle is open; with no `OVERLAPPED`, the call cancels
        // all that was started on it, on any thread.
        unsafe { sys::CancelIoEx(self.handle, ptr::null_mut()) };
    }
}

impl Drop for Afd {
    fn drop(&mut self) {
        // SAFETY: the handle is open, and nothing has it but this.
        unsafe { sys::CloseHandle(self.handle) };
    }
}

/// A wait for a socket to be ready, which any thread may hold and poll: it
/// runs on the port of the engine current on the thread that polls it as it
/// starts, through that engine's handle of the AFD driver; see the
/// [module](self).
///
/// [`poll_ready`](ReadyWait::poll_ready) starts it, and is ready with the
/// events that the socket has once it has any of those asked for; the wait
/// is then idle, for the next poll to start again.
#[derive(Default)]
pub struct ReadyWait {
    slot: Option<Arc<PollSlot>>,
}

/// A poll and where it stands, which the wait shares with the system while
/// the poll is in flight.
struct PollSlot {
    /// Where the system writes the poll's outcome; its address names the
    /// poll to cancel it.
    status: UnsafeCell<sys::IoStatusBlock>,
    /// The poll, which the system reads, and writes back with the events.
    info: UnsafeCell<sys::AfdPollInfo>,
    state: Mutex<PollState>,
}

// SAFETY: the status block and the poll are written by the system while the
// poll is in flight, and by the thread that starts it or delivers its
// completion otherwise, which the state, behind its lock, tells apart.
unsafe impl Send for PollSlot {}

// SAFETY: as for `Send`.
unsafe impl Sync for PollSlot {}

enum PollState {
    /// No poll is in flight.
    Idle,
    /// The poll is in flight, made through `afd`, to wake `waker` once it
    /// completes.
    Polling { afd: Arc<Afd>, waker: Waker },
    /// The poll was cancelled by its wait, and its completion is to come.
    Withdrawn,
    /// The poll completed: the events, or the error, for the next poll of
    /// the wait to return.
    Done(io::Result<u32>),
}

impl ReadyWait {
    /// A wait that has not started.
    pub const fn new() -> Self {
        Self { slot: None }
    }

    /// Whether the wait has started and has not been ready since: its poll
    /// is in flight, or has completed with what the next
    /// [`poll_ready`](ReadyWait::poll_ready) returns, which so goes on with
    /// it rather than ask the socket.
    pub fn is_waiting(&self) -> bool {
        self.slot.as_ref().is_some_and(|slot| {
            matches!(*slot.lock(), PollState::Polling { .. } | PollState::Done(_))
        })
    }

    /// Waits for `socket`, a base socket as [`base_socket`] gives it, to have
    /// any of `events`, the bits of [`events`]: starts a poll through the
    /// engine current on the thread, if none is in flight, keeps `cx`'s
    /// waker to wake once it completes, and is ready then with the events
    /// the socket has, which may be none. A socket that has some already
    /// makes the poll complete at the engine's next turn. The wait is idle
    /// once it has been ready, and the next poll starts it again.
    ///
    /// Polled again while its poll is in flight, the wait goes on where it
    /// started, whichever thread polls it, and wakes the task of the latest
    /// poll, for the events it started with; if the engine it started on
    /// was dropped meanwhile, which woke its task, it starts again where it
    /// is polled.
    ///
    /// # Errors
    ///
    /// `Unsupported` if the wait has to start and the thread has no engine
    /// current, and what the system reports for a poll that fails.
    pub fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        socket: BorrowedSocket<'_>,
        events: u32,
    ) -> Poll<io::Result<u32>> {
        if let Some(slot) = &self.slot {
            let mut state = slot.lock();
            match &mut *state {
                PollState::Polling { waker, .. } => {
                    waker.clone_from(cx.waker());
                    return Poll::Pending;
                }
                PollState::Done(_) => {
                    let PollState::Done(found) = mem::replace(&mut *state, PollState::Idle) else {
                        unreachable!("the state was matched as done");
                    };
                    return Poll::Ready(found);
                }
                PollState::Idle | PollState::Withdrawn => {}
            }
        }
        let engine = Iocp::current().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "the thread has no ruio runtime for the wait to run on",
            )
        })?;
        let slot = match &self.slot {
            Some(slot) if matches!(*slot.lock(), PollState::Idle) => Arc::clone(slot),
            _ => {
                let slot = Arc::new(PollSlot::new());
                self.slot = Some(Arc::clone(&slot));
                slot
            }
        };
        slot.start(&engine, socket.as_raw_socket(), events, cx.waker())?;
        Poll::Pending
    }

    /// Ends the wait, if its poll is in flight, and forgets what a poll that
    /// completed found: the next poll starts again.
    pub fn cancel(&mut self) {
        if let Some(slot) = &self.slot
            && !slot.withdraw()
        {
            // The system still has the slot, which the engine lets go of as
            // it delivers the completion: the next wait makes its own.
            self.slot = None;
        }
    }
}

impl Drop for ReadyWait {
    fn drop(&mut self) {
        self.cancel();
    }
}

impl fmt::Debug for ReadyWait {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadyWait")
            .field("waiting", &self.is_waiting())
            .finish()
    }
}

impl PollSlot {
    fn new() -> Self {
        Self {
            status: UnsafeCell::new(sys::IoStatusBlock {
                status: 0,
                information: 0,
            }),
            info: UnsafeCell::new(sys::AfdPollInfo {
                timeout: i64::MAX,
                number_of_handles: 0,
                exclusive: 0,
                handles: [sys::AfdPollHandleInfo {
                    handle: ptr::null_mut(),
                    events: 0,
                    status: 0,
                }],
            }),
            state: Mutex::new(PollState::Idle),
        }
    }

    fn lock(&self) -> MutexGuard<'_, PollState> {
        lock(&self.state)
    }

    /// Starts a poll of `socket` for `events` through the handle of
    /// `engine`, the engine of the calling thread, with `waker` to wake.
    fn start(
        self: &Arc<Self>,
        engine: &Iocp,
        socket: RawSocket,
        events: u32,
        waker: &Waker,
    ) -> io::Result<()> {
        let afd = engine.afd()?;
        // SAFETY: no poll is in flight, as the state is idle, so neither the
        // system nor the engine uses the status block and the poll, which
        // are written here alone.
        unsafe {
            *self.status.get() = sys::IoStatusBlock {
                status: sys::STATUS_PENDING as usize,
                information: 0,
            };
        }
        // SAFETY: as for the status block.
        unsafe {
            *self.info.get() = sys::AfdPollInfo {
                timeout: i64::MAX,
                number_of_handles: 1,
                exclusive: 0,
                handles: [sys::AfdPollHandleInfo {
                    handle: ptr::without_provenance_mut(socket as usize),
                    events,
                    status: 0,
                }],
            };
        }
        *self.lock() = PollState::Polling {
            afd: Arc::clone(&afd),
            waker: waker.clone(),
        };
        let context = Arc::into_raw(Arc::clone(self));
        let info = self.info.get();
        let len = size_of::<sys::AfdPollInfo>() as u32;
        // SAFETY: the helper's handle is open, as `afd` holds it; the status
        // block and the poll stay where they are, in the slot, until the
        // completion is delivered, which the reference the completion
        // carries keeps alive; and the poll names the socket, which the
        // caller keeps open, or the driver reports closed.
        let status = unsafe {
            sys::NtDeviceIoControlFile(
                afd.handle,
                ptr::null_mut(),
                ptr::null(),
                context.cast_mut().cast(),
                self.status.get(),
                sys::IOCTL_AFD_POLL,
                info.cast_const().cast(),
                len,
                info.cast(),
                len,
            )
        };
        // An error, as opposed to a warning, that the call returns is one
        // the system failed the poll with before taking it; anything else
        // has its completion come to the port.
        if (status as u32) < 0xC000_0000 {
            engine.started();
            return Ok(());
        }
        // SAFETY: the reference `into_raw` made for the system, which did
        // not take it.
        drop(unsafe { Arc::from_raw(context) });
        *self.lock() = PollState::Idle;
        Err(status_error(status))
    }

    /// Cancels the poll, if it is in flight, and says whether the slot is
    /// idle: false while the system has it, until the completion comes.
    fn withdraw(&self) -> bool {
        let mut state = self.lock();
        match mem::replace(&mut *state, PollState::Idle) {
            PollState::Polling { afd, .. } => {
                *state = PollState::Withdrawn;
                drop(state);
                // SAFETY: the helper's handle is open, as `afd` holds it, and
                // the status block names the poll, which stays in the slot
                // until its completion is delivered. A poll that has
                // completed meanwhile is not found, and nothing is
                // cancelled.
                unsafe { sys::CancelIoEx(afd.handle, self.status.get().cast()) };
                false
            }
            PollState::Withdrawn => {
                *state = PollState::Withdrawn;
                false
            }
            PollState::Idle | PollState::Done(_) => true,
        }
    }

    /// Records the poll's outcome, which the system has written, and gives
    /// the waker to wake, if the wait still waits.
    fn complete(&self) -> Option<Waker> {
        // SAFETY: the poll has completed, and its completion is being
        // delivered, so the system has finished with the status block,
        // which is read here alone.
        let status = unsafe { (*self.status.get()).status } as u32 as i32;
        // SAFETY: as for the status block.
        let info = unsafe { *self.info.get() };
        let mut state = self.lock();
        match mem::replace(&mut *state, PollState::Idle) {
            // Cancelled by no wait, so by the engine, as it is dropped: the
            // wait starts again where it is next polled.
            PollState::Polling { waker, .. } if status == sys::STATUS_CANCELLED => Some(waker),
            PollState::Polling { waker, .. } => {
                *state = PollState::Done(if status < sys::STATUS_SUCCESS {
                    Err(status_error(status))
                } else if info.number_of_handles == 0 {
                    Ok(0)
                } else {
                    Ok(info.handles[0].events)
                });
                Some(waker)
            }
            PollState::Withdrawn | PollState::Idle => None,
            done @ PollState::Done(_) => {
                *state = done;
                None
            }
        }
    }
}

/// Delivers the completion of a poll that `entry` hands back: records its
/// outcome, wakes its wait's task if the wait still waits, and lets go of
/// the reference the system had.
///
/// # Safety
///
/// `entry` must be the completion of a poll that [`ReadyWait`] started
/// through a handle of the engine's, which the port hands back once.
pub(crate) unsafe fn complete(entry: &sys::OverlappedEntry) {
    // SAFETY: by the caller's promise, the completion carries the reference
    // that the poll's start made for the system, which it hands back once.
    let slot = unsafe { Arc::from_raw(entry.overlapped.cast_const().cast::<PollSlot>()) };
    let waker = slot.complete();
    drop(slot);
    if let Some(waker) = waker {
        waker.wake();
    }
}

#[cfg(test)]
mod tests {
    use std::future::{Future, poll_fn};
    use std::io::{self, Write};
    use std::net::{TcpListener, TcpStream};
    use std::os::windows::io::{AsRawSocket, AsSocket, BorrowedSocket};
    use std::pin::pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};
    use std::thread;
    use std::time::{Duration, Instant};

    use ruio_engine::{Build, Engine};

    use super::{ReadyWait, base_socket, events};
    use crate::engine::{Builder, Iocp};

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

    /// A connected pair of nonblocking streams over loopback, and the
    /// listener.
    fn pair() -> (TcpListener, TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        server.set_nonblocking(true).unwrap();
        (listener, client, server)
    }

    fn base(socket: &impl AsSocket) -> BorrowedSocket<'_> {
        let raw = base_socket(socket.as_socket());
        assert_eq!(raw, socket.as_socket().as_raw_socket());
        socket.as_socket()
    }

    /// Turns `engine` until `wait`'s poll of `socket` for `events` is ready,
    /// for up to five seconds, and gives what it found.
    fn ready(engine: &Iocp, wait: &mut ReadyWait, socket: BorrowedSocket<'_>, events: u32) -> u32 {
        let (woken, waker) = task();
        let mut cx = Context::from_waker(&waker);
        let started = Instant::now();
        loop {
            if let Poll::Ready(found) = wait.poll_ready(&mut cx, socket, events) {
                return found.unwrap();
            }
            while woken.0.swap(0, Ordering::SeqCst) == 0 {
                assert!(started.elapsed() < Duration::from_secs(5), "never ready");
                engine.park(Some(Duration::from_millis(50))).unwrap();
            }
        }
    }

    fn in_flight(engine: &Iocp) -> String {
        format!("{engine:?}")
    }

    #[test]
    fn a_socket_is_readable_once_data_arrives() {
        let engine = Builder::new().build().unwrap();
        let _entered = engine.enter();
        let (_listener, mut client, server) = pair();
        let mut wait = ReadyWait::new();
        let (_, waker) = task();
        let polled = wait.poll_ready(
            &mut Context::from_waker(&waker),
            base(&server),
            events::READABLE,
        );
        assert!(polled.is_pending());
        assert!(wait.is_waiting());
        client.write_all(b"x").unwrap();
        let found = ready(&engine, &mut wait, base(&server), events::READABLE);
        assert_ne!(found & events::RECEIVE, 0, "found {found:#x}");
        assert!(!wait.is_waiting());
        assert!(in_flight(&engine).contains("in_flight: 0"));
    }

    #[test]
    fn a_socket_with_room_is_writable_at_the_next_turn() {
        let engine = Builder::new().build().unwrap();
        let _entered = engine.enter();
        let (_listener, _client, server) = pair();
        let mut wait = ReadyWait::new();
        for _ in 0..3 {
            let found = ready(&engine, &mut wait, base(&server), events::WRITABLE);
            assert_ne!(found & events::SEND, 0, "found {found:#x}");
        }
    }

    #[test]
    fn a_listener_is_readable_once_a_connection_arrives() {
        let engine = Builder::new().build().unwrap();
        let _entered = engine.enter();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let mut wait = ReadyWait::new();
        let (_, waker) = task();
        let polled = wait.poll_ready(
            &mut Context::from_waker(&waker),
            base(&listener),
            events::READABLE,
        );
        assert!(polled.is_pending());
        let _client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let found = ready(&engine, &mut wait, base(&listener), events::READABLE);
        assert_ne!(found & events::ACCEPT, 0, "found {found:#x}");
        assert!(listener.accept().is_ok());
    }

    #[test]
    fn a_peer_that_shuts_down_makes_the_socket_readable() {
        let engine = Builder::new().build().unwrap();
        let _entered = engine.enter();
        let (_listener, client, server) = pair();
        let mut wait = ReadyWait::new();
        let (_, waker) = task();
        let polled = wait.poll_ready(
            &mut Context::from_waker(&waker),
            base(&server),
            events::READABLE,
        );
        assert!(polled.is_pending());
        drop(client);
        let found = ready(&engine, &mut wait, base(&server), events::READABLE);
        assert_ne!(
            found & (events::DISCONNECT | events::ABORT),
            0,
            "found {found:#x}"
        );
    }

    #[test]
    fn a_dropped_wait_cancels_its_poll() {
        let engine = Builder::new().build().unwrap();
        let _entered = engine.enter();
        let (_listener, mut client, server) = pair();
        let (woken, waker) = task();
        let mut wait = ReadyWait::new();
        let polled = wait.poll_ready(
            &mut Context::from_waker(&waker),
            base(&server),
            events::READABLE,
        );
        assert!(polled.is_pending());
        assert!(in_flight(&engine).contains("in_flight: 1"));
        // Dropped on another thread, it cancels its poll, whose completion
        // comes with nobody to wake.
        thread::spawn(move || drop(wait)).join().unwrap();
        let started = Instant::now();
        while !in_flight(&engine).contains("in_flight: 0") {
            assert!(started.elapsed() < Duration::from_secs(5));
            engine.park(Some(Duration::from_millis(10))).unwrap();
        }
        client.write_all(b"x").unwrap();
        engine.park(Some(Duration::from_millis(20))).unwrap();
        assert_eq!(woken.0.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn a_wait_polled_on_another_thread_wakes_its_task_there() {
        let engine = Builder::new().build().unwrap();
        let _entered = engine.enter();
        let (_listener, mut client, server) = pair();
        let (_, waker) = task();
        let mut wait = ReadyWait::new();
        let polled = wait.poll_ready(
            &mut Context::from_waker(&waker),
            base(&server),
            events::READABLE,
        );
        assert!(polled.is_pending());
        let socket = server.as_raw_socket();
        let waiter = thread::spawn(move || {
            // SAFETY: the test keeps the socket open until the thread ends.
            let socket = unsafe { BorrowedSocket::borrow_raw(socket) };
            let (woken, waker) = task();
            let mut cx = Context::from_waker(&waker);
            assert!(
                wait.poll_ready(&mut cx, socket, events::READABLE)
                    .is_pending()
            );
            let started = Instant::now();
            while woken.0.load(Ordering::SeqCst) == 0 {
                assert!(started.elapsed() < Duration::from_secs(5), "never woken");
                thread::sleep(Duration::from_millis(1));
            }
            match wait.poll_ready(&mut cx, socket, events::READABLE) {
                Poll::Ready(Ok(found)) => assert_ne!(found & events::RECEIVE, 0),
                other => panic!("the wait gave {other:?}"),
            }
        });
        thread::sleep(Duration::from_millis(20));
        client.write_all(b"x").unwrap();
        while !waiter.is_finished() {
            engine.park(Some(Duration::from_millis(10))).unwrap();
        }
        waiter.join().unwrap();
        drop(server);
    }

    #[test]
    fn a_dropped_engine_cancels_its_polls_which_start_again_elsewhere() {
        let (_listener, mut client, server) = pair();
        let (woken, waker) = task();
        let mut cx = Context::from_waker(&waker);
        let mut wait = ReadyWait::new();
        {
            let first = Builder::new().build().unwrap();
            let _entered = first.enter();
            let polled = wait.poll_ready(&mut cx, base(&server), events::READABLE);
            assert!(polled.is_pending());
        }
        assert_eq!(woken.0.load(Ordering::SeqCst), 1);
        assert!(!wait.is_waiting());
        let second = Builder::new().build().unwrap();
        let _entered = second.enter();
        assert!(
            wait.poll_ready(&mut cx, base(&server), events::READABLE)
                .is_pending()
        );
        client.write_all(b"x").unwrap();
        let found = ready(&second, &mut wait, base(&server), events::READABLE);
        assert_ne!(found & events::RECEIVE, 0, "found {found:#x}");
    }

    #[test]
    fn a_wait_with_no_engine_current_is_unsupported() {
        let (_listener, _client, server) = pair();
        let mut wait = ReadyWait::new();
        let polled = pin!(poll_fn(|cx| wait.poll_ready(
            cx,
            base(&server),
            events::READABLE
        )));
        let (_, waker) = task();
        match polled.poll(&mut Context::from_waker(&waker)) {
            Poll::Ready(Err(err)) => assert_eq!(err.kind(), io::ErrorKind::Unsupported),
            other => panic!("the wait gave {other:?}"),
        }
    }
}
