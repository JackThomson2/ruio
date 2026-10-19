//! Portable I/O polled where no runtime runs, by
//! `futures::executor::block_on` on a plain thread and by the workers of
//! tokio's multi-threaded runtime, while a ruio runtime on a thread of its
//! own is the foreign ring their waits go to.

use std::future::{Future, poll_fn};
use std::io::{self, Write as _};
use std::os::unix::net::UnixStream as StdUnixStream;
use std::pin::pin;
use std::process::Stdio;
use std::rc::Rc;
use std::sync::{OnceLock, mpsc};
use std::task::Poll;
use std::thread;
use std::time::{Duration, Instant};

use ruio::io::AsyncRead;
use ruio::net::portable::{TcpListener, TcpStream};
use ruio::process::Command;
use ruio::uring::PortableFd;

/// How long a test waits for anything before it is considered hung.
const PATIENCE: Duration = Duration::from_secs(10);

/// What a wait for readable data waits for.
const READABLE: u32 = libc::POLLIN as u32;

/// What a wait for room to write waits for.
const WRITABLE: u32 = libc::POLLOUT as u32;

/// Starts, once, a ruio runtime on a thread of its own, which runs for as
/// long as the tests do, and makes its ring the foreign one.
fn foreign() {
    static STARTED: OnceLock<()> = OnceLock::new();
    STARTED.get_or_init(|| {
        let (send, receive) = mpsc::channel();
        thread::Builder::new()
            .name("ruio".to_owned())
            .spawn(move || {
                ruio::Runtime::new().unwrap().block_on(async move {
                    send.send(ruio::handle().ring().clone()).unwrap();
                    std::future::pending::<()>().await;
                });
            })
            .unwrap();
        ruio::uring::set_foreign_ring(&receive.recv().unwrap());
    });
}

/// Runs the future `make` makes with `futures::executor::block_on` on a
/// plain thread of its own, with no runtime, which makes it there, so that
/// it need not be `Send`.
fn on_plain_thread<F, Fut>(make: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = io::Result<()>>,
{
    foreign();
    let (done, finished) = mpsc::channel();
    let thread = thread::spawn(move || {
        let result = futures_executor::block_on(make());
        let _ = done.send(());
        result
    });
    match finished.recv_timeout(PATIENCE) {
        Ok(()) => thread.join().unwrap().unwrap(),
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            std::panic::resume_unwind(thread.join().unwrap_err())
        }
        Err(mpsc::RecvTimeoutError::Timeout) => panic!("still pending after {PATIENCE:?}"),
    }
}

/// Runs `future` as a task on the workers of a tokio multi-threaded
/// runtime of its own.
fn on_tokio<Fut>(future: Fut)
where
    Fut: Future<Output = io::Result<()>> + Send + 'static,
{
    foreign();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_time()
        .build()
        .unwrap();
    runtime
        .block_on(async {
            tokio::time::timeout(PATIENCE, tokio::spawn(future))
                .await
                .expect("finished in time")
        })
        .unwrap()
        .unwrap();
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

/// A portable TCP echo: a listener accepts, and echoes what it reads until
/// the end, while a client connects, writes, shuts its writing half down and
/// reads the echo back; each read finds nothing at first and waits.
async fn tcp_echo() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let data = pattern(64 << 10);
    let expected = data.clone();
    let server = async move {
        let (stream, _) = listener.accept().await?;
        let mut buf = Vec::with_capacity(16 << 10);
        loop {
            let (read, filled) = stream.read(buf).await;
            if read? == 0 {
                return io::Result::Ok(());
            }
            let (written, mut emptied) = stream.write_all(filled).await;
            written?;
            emptied.clear();
            buf = emptied;
        }
    };
    let client = async move {
        let stream = TcpStream::connect(addr).await?;
        stream.write_all(data).await.0?;
        stream.shutdown(std::net::Shutdown::Write)?;
        let (read, echoed) = stream.read_to_end(Vec::new()).await;
        read?;
        io::Result::Ok(echoed)
    };
    let (served, echoed) = join(server, client).await;
    served?;
    assert!(echoed? == expected, "the echo differs");
    Ok(())
}

/// A child's piped standard output, read to its end after a pause that
/// leaves the pipe empty at first, and the child waited for.
async fn child_stdout() -> io::Result<()> {
    let mut child = Command::new("sh")
        .args(["-c", "sleep 0.05; echo out"])
        .stdout(Stdio::piped())
        .spawn()?;
    let stdout = child.stdout.take().unwrap();
    let (read, out) = stdout.read_to_end(Vec::new()).await;
    read?;
    assert_eq!(out, b"out\n");
    assert!(child.wait().await?.success());
    Ok(())
}

/// Children waited for as they exit, with their statuses.
async fn child_wait() -> io::Result<()> {
    let mut child = Command::new("sleep").arg("0.05").spawn()?;
    assert!(child.wait().await?.success());
    let status = Command::new("sh").args(["-c", "exit 3"]).status().await?;
    assert_eq!(status.code(), Some(3));
    Ok(())
}

/// A file read whole by path, and one written, through the foreign ring.
async fn fs_read() -> io::Result<()> {
    let dir = std::env::temp_dir().join(format!(
        "ruio-foreign-{}-{:?}",
        std::process::id(),
        thread::current().id()
    ));
    std::fs::create_dir_all(&dir)?;
    let path = dir.join("data");
    let data = pattern(100_000);
    std::fs::write(&path, &data)?;
    assert!(ruio::fs::read(&path).await? == data);
    let written = dir.join("written");
    ruio::fs::write(&written, b"written".to_vec()).await?;
    assert_eq!(ruio::fs::metadata(&written).await?.len(), 7);
    assert_eq!(ruio::fs::read_to_string(&written).await?, "written");
    std::fs::remove_dir_all(&dir)?;
    Ok(())
}

/// A portable descriptor waited on: writable at once, and readable once its
/// peer writes, which a thread does after a pause.
async fn portable_fd_ready() -> io::Result<()> {
    let (ours, theirs) = StdUnixStream::pair()?;
    let ours = PortableFd::new(ours.into());
    assert_ne!(ours.ready(WRITABLE).await? & WRITABLE, 0);
    let started = Instant::now();
    let writer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(30));
        (&theirs).write_all(b"x").unwrap();
        theirs
    });
    assert_ne!(ours.ready(READABLE).await? & READABLE, 0);
    assert!(started.elapsed() >= Duration::from_millis(25), "it waited");
    drop(writer.join().unwrap());
    Ok(())
}

#[test]
fn a_portable_tcp_echo_on_a_plain_thread() {
    on_plain_thread(tcp_echo);
}

#[test]
fn a_portable_tcp_echo_on_tokio() {
    on_tokio(tcp_echo());
}

#[test]
fn a_childs_stdout_on_a_plain_thread() {
    on_plain_thread(child_stdout);
}

#[test]
fn a_childs_stdout_on_tokio() {
    on_tokio(child_stdout());
}

#[test]
fn child_waits_on_a_plain_thread() {
    on_plain_thread(child_wait);
}

#[test]
fn child_waits_on_tokio() {
    on_tokio(child_wait());
}

#[test]
fn fs_read_on_a_plain_thread() {
    on_plain_thread(fs_read);
}

#[test]
fn fs_read_on_tokio() {
    on_tokio(fs_read());
}

#[test]
fn portable_fd_ready_on_a_plain_thread() {
    on_plain_thread(portable_fd_ready);
}

#[test]
fn portable_fd_ready_on_tokio() {
    on_tokio(portable_fd_ready());
}

/// A future that is not `Send`, made and polled on a plain thread, does its
/// portable I/O through the foreign ring too: the operations stay with it.
#[test]
fn a_future_that_is_not_send_does_its_io_on_a_plain_thread() {
    on_plain_thread(|| async {
        let here = Rc::new(());
        let (ours, theirs) = StdUnixStream::pair()?;
        let ours = ruio::net::portable::UnixStream::from_std(ours)?;
        let writer = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            (&theirs).write_all(b"local").unwrap();
            theirs
        });
        let (read, buf) = ours.read(Vec::with_capacity(16)).await;
        assert_eq!(&buf[..read?], b"local");
        drop(here);
        drop(writer.join().unwrap());
        Ok(())
    });
}

/// A stream connected on a ruio runtime's thread is read and written on a
/// tokio worker, and the other way round: each wait runs wherever it is
/// polled.
#[test]
fn streams_made_under_a_runtime_move_to_threads_with_none() {
    foreign();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let (stream, peer) = ruio::Runtime::new()
        .unwrap()
        .block_on(async move {
            let accepted = ruio::spawn(async move { listener.accept().await });
            let stream = TcpStream::connect(addr).await?;
            let (peer, _) = accepted.await.unwrap()?;
            io::Result::Ok((stream, peer))
        })
        .unwrap();
    let answering = thread::spawn(move || {
        futures_executor::block_on(async {
            let (read, asked) = peer.read_exact(Vec::with_capacity(7)).await;
            read?;
            assert_eq!(asked, b"to ruio");
            peer.write_all("to tokio").await.0
        })
    });
    on_tokio(async move {
        let (written, _) = stream.write_all("to ruio").await;
        written?;
        let (read, reply) = stream.read_exact(Vec::with_capacity(8)).await;
        read?;
        assert_eq!(reply, b"to tokio");
        Ok(())
    });
    answering.join().unwrap().unwrap();
}
