//! The portable sockets and a child's pipes polled on the threads of a
//! foreign reactor, a stand-in for another executor's: their waits go to
//! the reactor, which keeps one waker each way, as a poll-based reactor
//! does. No foreign ring is set in this test binary, so a wait that went to
//! one would fail with `ENOSYS` instead.

use std::cell::Cell;
use std::future::{Future, poll_fn};
use std::io::{self, Write as _};
use std::mem;
use std::os::fd::{AsRawFd, BorrowedFd, RawFd};
use std::pin::pin;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::Duration;

use ruio::io::portable::Interest;
use ruio::io::{AsyncWrite, Reactor, Registration};
use ruio::net::portable::{TcpListener, TcpStream};
use ruio::process::Command;

/// How long a test waits for anything before it is considered hung.
const PATIENCE: Duration = Duration::from_secs(10);

thread_local! {
    /// Whether the thread is one of the stand-in reactor's.
    static REACTOR_THREAD: Cell<bool> = const { Cell::new(false) };
}

/// A reactor that watches each wait with a thread of its own, which polls
/// the descriptor with `poll(2)` and wakes the latest waker kept that way.
struct Watching {
    /// The files of the descriptors it has registered.
    registered: Mutex<Vec<(u64, u64)>>,
    /// How many registrations were dropped after their descriptor closed.
    late: AtomicUsize,
}

static REACTOR: Watching = Watching {
    registered: Mutex::new(Vec::new()),
    late: AtomicUsize::new(0),
};

impl Reactor for Watching {
    fn register(&self, fd: BorrowedFd<'_>) -> Option<io::Result<Box<dyn Registration>>> {
        if !REACTOR_THREAD.with(Cell::get) {
            return None;
        }
        let watched = Watched::new(fd.as_raw_fd());
        self.registered.lock().unwrap().extend(watched.file);
        Some(Ok(Box::new(watched)))
    }
}

/// The identity of the file a descriptor names.
fn identity(fd: RawFd) -> Option<(u64, u64)> {
    let mut status = mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: fstat(2) writes at most one `stat` into `status`.
    if unsafe { libc::fstat(fd, status.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: the call succeeded, so it wrote the structure.
    let status = unsafe { status.assume_init() };
    Some((status.st_dev, status.st_ino))
}

/// A descriptor registered with [`Watching`].
struct Watched {
    fd: RawFd,
    file: Option<(u64, u64)>,
    /// The latest waker kept each way: reading, writing.
    wakers: Arc<Mutex<[Option<Waker>; 2]>>,
    /// Whether the registration lives, for its watching threads to stop.
    alive: Arc<AtomicBool>,
}

impl Watched {
    fn new(fd: RawFd) -> Self {
        Self {
            fd,
            file: identity(fd),
            wakers: Arc::new(Mutex::new([None, None])),
            alive: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Keeps `waker` as the one to wake when the descriptor is ready for
    /// `interest`, and watches it from a thread of its own.
    fn watch(&self, waker: &Waker, interest: Interest) {
        let way = usize::from(interest.is_writable());
        self.wakers.lock().unwrap()[way] = Some(waker.clone());
        let events = if interest.is_writable() {
            libc::POLLOUT
        } else {
            libc::POLLIN
        };
        let (fd, wakers, alive) = (self.fd, Arc::clone(&self.wakers), Arc::clone(&self.alive));
        thread::spawn(move || {
            while alive.load(Ordering::SeqCst) {
                let mut poll = libc::pollfd {
                    fd,
                    events,
                    revents: 0,
                };
                // SAFETY: poll(2) reads and writes the one `pollfd`.
                if unsafe { libc::poll(&raw mut poll, 1, 10) } != 0 {
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
        if identity(self.fd) != self.file {
            REACTOR.late.fetch_add(1, Ordering::SeqCst);
        }
    }
}

/// Sets the stand-in reactor, and checks that no foreign ring is.
fn set() {
    ruio::io::set_foreign_reactor(&REACTOR);
    assert!(ruio::uring::foreign_ring().is_none());
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
            stream.shutdown(std::net::Shutdown::Write)
        };
        let client = async {
            let stream = TcpStream::connect(addr).await?;
            let sending = async {
                stream.write_all(data.clone()).await.0?;
                stream.shutdown(std::net::Shutdown::Write)
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
/// way: the descriptor's own waker, which it keeps, wakes both.
#[test]
fn every_task_waiting_on_a_descriptor_is_woken() {
    set();
    let (mut peer, ours) = std::os::unix::net::UnixStream::pair().unwrap();
    let stream = Arc::new(ruio::net::portable::UnixStream::from_std(ours).unwrap());
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
    peer.write_all(&[1, 2]).unwrap();
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

/// A child's pipes wait on the reactor: input written to `cat` comes back
/// on its output, and the end once its input closes.
#[test]
fn a_childs_pipes_wait_on_the_reactor() {
    on_reactor(|| async {
        let mut child = Command::new("cat")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let data = pattern(1 << 20);
        let writing = async {
            stdin.write_all(data.clone()).await.0?;
            drop(stdin);
            io::Result::Ok(())
        };
        let reading = async {
            let mut back = Vec::new();
            let mut buf = Vec::with_capacity(16 << 10);
            loop {
                let (read, more) = stdout.read(buf).await;
                if read? == 0 {
                    break;
                }
                back.extend_from_slice(&more);
                buf = more;
                buf.clear();
            }
            io::Result::Ok(back)
        };
        let (written, back) = join(writing, reading).await;
        written?;
        assert!(back? == data);
        // Its wait would go to the foreign ring, which this binary has none
        // of: the reaper waits for it once it is dropped.
        drop(child);
        Ok(())
    });
}

/// A stream readable and writable waits on the reactor too, and one that
/// connects waits there for the connection.
#[test]
fn readiness_waits_on_the_reactor() {
    let (mut peer, ours) = std::os::unix::net::UnixStream::pair().unwrap();
    let writer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        peer.write_all(b"x").unwrap();
        peer
    });
    on_reactor(move || async move {
        let stream = ruio::net::portable::UnixStream::from_std(ours)?;
        stream.writable().await?;
        stream.readable().await?;
        let mut byte = [0];
        assert_eq!(stream.try_read(&mut byte)?, 1);
        Ok(())
    });
    drop(writer.join().unwrap());
}

/// A thread where a runtime of ruio's runs waits on its ring, whatever the
/// reactor would say: it registers nothing.
#[test]
fn a_runtimes_thread_waits_on_its_ring() {
    set();
    let (mut peer, ours) = std::os::unix::net::UnixStream::pair().unwrap();
    let file = identity(ours.as_raw_fd()).unwrap();
    thread::spawn(move || {
        REACTOR_THREAD.with(|reactor| reactor.set(true));
        ruio::Runtime::new().unwrap().block_on(async move {
            let stream = ruio::net::portable::UnixStream::from_std(ours).unwrap();
            let reading = stream.read(Vec::with_capacity(1));
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
    assert!(!REACTOR.registered.lock().unwrap().contains(&file));
}
