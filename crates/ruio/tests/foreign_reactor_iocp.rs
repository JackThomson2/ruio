//! The portable sockets polled on the threads of a foreign reactor, a
//! stand-in for another executor's: their waits go to the reactor, which
//! keeps one waker each way, as a poll-based reactor does. Where no runtime
//! runs and the reactor takes no wait, a wait has no port to run on, and
//! fails as unsupported.

#![cfg(ruio_iocp)]

use std::cell::Cell;
use std::future::{Future, poll_fn};
use std::io::{self, Write as _};
use std::net::Shutdown;
use std::os::windows::io::{AsRawSocket, BorrowedSocket, RawSocket};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::Duration;

use ruio::io::portable::Interest;
use ruio::io::{Reactor, Registration};
use ruio::net::portable::{TcpListener, TcpStream};

/// How long a test waits for anything before it is considered hung.
const PATIENCE: Duration = Duration::from_secs(10);

/// `WSAPOLLFD`.
#[repr(C)]
struct WsaPollFd {
    fd: usize,
    events: i16,
    revents: i16,
}

const POLLRDNORM: i16 = 0x0100;
const POLLWRNORM: i16 = 0x0010;
const POLLNVAL: i16 = 0x0004;

#[link(name = "ws2_32")]
unsafe extern "system" {
    fn WSAPoll(sockets: *mut WsaPollFd, count: u32, timeout: i32) -> i32;
}

/// Polls `socket` for `events` for up to `timeout` milliseconds: the events
/// it found, or `None` if the poll failed, as for no socket.
fn poll(socket: RawSocket, events: i16, timeout: i32) -> Option<i16> {
    let mut poll = WsaPollFd {
        fd: socket as usize,
        events,
        revents: 0,
    };
    // SAFETY: the call reads and writes the one `WSAPOLLFD` it is given.
    let polled = unsafe { WSAPoll(&raw mut poll, 1, timeout) };
    (polled >= 0).then_some(poll.revents)
}

thread_local! {
    /// Whether the thread is one of the stand-in reactor's.
    static REACTOR_THREAD: Cell<bool> = const { Cell::new(false) };
}

/// A reactor that watches each wait with a thread of its own, which polls
/// the socket with `WSAPoll` and wakes the latest waker kept that way.
struct Watching {
    /// The sockets it has registered.
    registered: Mutex<Vec<RawSocket>>,
    /// How many registrations were dropped after their socket closed.
    late: AtomicUsize,
}

static REACTOR: Watching = Watching {
    registered: Mutex::new(Vec::new()),
    late: AtomicUsize::new(0),
};

impl Reactor for Watching {
    fn register(&self, socket: BorrowedSocket<'_>) -> Option<io::Result<Box<dyn Registration>>> {
        if !REACTOR_THREAD.with(Cell::get) {
            return None;
        }
        let socket = socket.as_raw_socket();
        self.registered.lock().unwrap().push(socket);
        Some(Ok(Box::new(Watched::new(socket))))
    }
}

/// A socket registered with [`Watching`].
struct Watched {
    socket: RawSocket,
    /// The latest waker kept each way: reading, writing.
    wakers: Arc<Mutex<[Option<Waker>; 2]>>,
    /// Whether the registration lives, for its watching threads to stop.
    alive: Arc<AtomicBool>,
}

impl Watched {
    fn new(socket: RawSocket) -> Self {
        Self {
            socket,
            wakers: Arc::new(Mutex::new([None, None])),
            alive: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Keeps `waker` as the one to wake when the socket is ready for
    /// `interest`, and watches it from a thread of its own.
    fn watch(&self, waker: &Waker, interest: Interest) {
        let way = usize::from(interest.is_writable());
        self.wakers.lock().unwrap()[way] = Some(waker.clone());
        let events = if interest.is_writable() {
            POLLWRNORM
        } else {
            POLLRDNORM
        };
        let socket = self.socket;
        let (wakers, alive) = (Arc::clone(&self.wakers), Arc::clone(&self.alive));
        thread::spawn(move || {
            while alive.load(Ordering::SeqCst) {
                if poll(socket, events, 10) != Some(0) {
                    break;
                }
            }
            if let Some(waker) = wakers.lock().unwrap()[way].take() {
                waker.wake();
            }
        });
    }
}

impl Registration for Watched {
    fn is_current(&self) -> bool {
        REACTOR_THREAD.with(Cell::get)
    }

    fn poll_io(
        &self,
        cx: &mut Context<'_>,
        interest: Interest,
        _len: usize,
        call: &mut dyn FnMut() -> io::Result<usize>,
    ) -> Poll<io::Result<usize>> {
        match call() {
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                self.watch(cx.waker(), interest);
                Poll::Pending
            }
            done => Poll::Ready(done),
        }
    }
}

impl Drop for Watched {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::SeqCst);
        let found = poll(self.socket, POLLRDNORM, 0);
        if found.is_none_or(|found| found & POLLNVAL != 0) {
            REACTOR.late.fetch_add(1, Ordering::SeqCst);
        }
    }
}

fn set() {
    ruio::io::set_foreign_reactor(&REACTOR);
}

/// Runs the future `make` makes with `futures::executor::block_on` on a
/// thread of the reactor's.
fn on_reactor<F, Fut, T>(make: F) -> T
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = io::Result<T>>,
    T: Send + 'static,
{
    set();
    let (done, finished) = mpsc::channel();
    let thread = thread::spawn(move || {
        REACTOR_THREAD.with(|reactor| reactor.set(true));
        let output = futures_executor::block_on(make());
        let _ = done.send(());
        output
    });
    match finished.recv_timeout(PATIENCE) {
        Ok(()) => thread.join().unwrap().unwrap(),
        Err(mpsc::RecvTimeoutError::Disconnected) => match thread.join() {
            Err(panic) => std::panic::resume_unwind(panic),
            Ok(_) => unreachable!("the thread ended without saying so"),
        },
        Err(mpsc::RecvTimeoutError::Timeout) => panic!("still pending after {PATIENCE:?}"),
    }
}

/// Runs two futures at once, to both their ends.
async fn join<A: Future, B: Future>(a: A, b: B) -> (A::Output, B::Output) {
    let (mut a, mut b) = (pin!(a), pin!(b));
    let (mut a_out, mut b_out) = (None, None);
    poll_fn(|cx| {
        if a_out.is_none()
            && let Poll::Ready(out) = a.as_mut().poll(cx)
        {
            a_out = Some(out);
        }
        if b_out.is_none()
            && let Poll::Ready(out) = b.as_mut().poll(cx)
        {
            b_out = Some(out);
        }
        if a_out.is_some() && b_out.is_some() {
            Poll::Ready((a_out.take().unwrap(), b_out.take().unwrap()))
        } else {
            Poll::Pending
        }
    })
    .await
}

/// `len` bytes that repeat only every 251.
fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|index| (index % 251) as u8).collect()
}

/// A connected pair: the standard library's end, and a portable stream.
fn pair() -> (std::net::TcpStream, TcpStream) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let peer = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (ours, _) = listener.accept().unwrap();
    (peer, TcpStream::from_std(ours).unwrap())
}

/// A listener accepts on the reactor, and echoes what it reads until the
/// end, while a client connects, writes more than the socket takes at once,
/// shuts its writing half down and reads the echo back; each read finds
/// nothing at first and waits on the reactor.
#[test]
fn a_portable_tcp_echo_waits_on_the_reactor() {
    on_reactor(|| async {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let data = pattern(4 << 20);
        let echo = async {
            let (stream, _) = listener.accept().await?;
            let mut buf = Vec::with_capacity(64 << 10);
            loop {
                let (read, back) = stream.read(buf).await;
                if read? == 0 {
                    break;
                }
                let (written, back) = stream.write_all(back).await;
                written?;
                buf = back;
                buf.clear();
            }
            stream.shutdown(Shutdown::Write)
        };
        let client = async {
            let stream = TcpStream::connect(addr).await?;
            let sending = async {
                stream.write_all(data.clone()).await.0?;
                stream.shutdown(Shutdown::Write)
            };
            let receiving = async {
                let (read, back) = stream.read_exact(Vec::with_capacity(data.len())).await;
                read.map(|()| back)
            };
            let (sent, back) = join(sending, receiving).await;
            sent?;
            back
        };
        let (echoed, back) = join(echo, client).await;
        echoed?;
        assert!(back? == data);
        Ok(())
    });
    assert_eq!(REACTOR.late.load(Ordering::SeqCst), 0);
}

/// Two tasks on two threads reading one stream at once both get what
/// arrives for them, though the reactor keeps only the latest waker each
/// way: the socket's own waker, which it keeps, wakes both.
#[test]
fn every_task_waiting_on_a_socket_is_woken() {
    set();
    let (mut peer, ours) = pair();
    let stream = Arc::new(ours);
    let (done, finished) = mpsc::channel();
    let readers: Vec<_> = (0..2)
        .map(|_| {
            let (stream, done) = (Arc::clone(&stream), done.clone());
            thread::spawn(move || {
                REACTOR_THREAD.with(|reactor| reactor.set(true));
                let (read, byte) = futures_executor::block_on(stream.read(Vec::with_capacity(1)));
                assert_eq!(read.unwrap(), 1);
                done.send(byte[0]).unwrap();
            })
        })
        .collect();
    // Both readers wait before anything arrives.
    thread::sleep(Duration::from_millis(100));
    peer.write_all(&[1]).unwrap();
    thread::sleep(Duration::from_millis(50));
    peer.write_all(&[2]).unwrap();
    let mut read = [0; 2];
    for byte in &mut read {
        *byte = finished
            .recv_timeout(PATIENCE)
            .expect("both readers are woken");
    }
    read.sort_unstable();
    assert_eq!(read, [1, 2]);
    for reader in readers {
        reader.join().unwrap();
    }
}

/// A stream readable and writable waits on the reactor too.
#[test]
fn readiness_waits_on_the_reactor() {
    let (mut peer, ours) = pair();
    let writer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        peer.write_all(b"x").unwrap();
        peer
    });
    on_reactor(move || async move {
        ours.writable().await?;
        ours.readable().await?;
        let mut byte = [0];
        assert_eq!(ours.try_read(&mut byte)?, 1);
        Ok(())
    });
    drop(writer.join().unwrap());
}

/// A thread where a runtime of ruio's runs waits on its port, whatever the
/// reactor would say: it registers nothing.
#[test]
fn a_runtimes_thread_waits_on_its_port() {
    set();
    let (mut peer, ours) = pair();
    let socket = ours.as_raw_socket();
    thread::spawn(move || {
        REACTOR_THREAD.with(|reactor| reactor.set(true));
        ruio::Runtime::new().unwrap().block_on(async move {
            let reading = ours.read(Vec::with_capacity(1));
            let writing = async {
                ruio::time::sleep(Duration::from_millis(20)).await;
                peer.write_all(b"x").unwrap();
            };
            let ((read, _), ()) = join(reading, writing).await;
            assert_eq!(read.unwrap(), 1);
        });
    })
    .join()
    .unwrap();
    assert!(!REACTOR.registered.lock().unwrap().contains(&socket));
}

/// A thread that is none of the reactor's, where no runtime runs, has no
/// port for a wait: a read that finds nothing fails as unsupported, and
/// one that finds data reads it.
#[test]
fn a_thread_of_neither_cannot_wait() {
    set();
    let (mut peer, ours) = pair();
    let (read, _) = futures_executor::block_on(ours.read(Vec::with_capacity(1)));
    assert_eq!(read.unwrap_err().kind(), io::ErrorKind::Unsupported);
    peer.write_all(b"x").unwrap();
    let mut byte = [0];
    let read = loop {
        match ours.try_read(&mut byte) {
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(1))
            }
            read => break read,
        }
    };
    assert_eq!(read.unwrap(), 1);
}
