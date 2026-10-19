//! ruio's portable streams, listeners and a child's pipes polled by tokio's
//! runtimes with `TokioReactor` as the foreign reactor: their waits go to
//! tokio's reactor. No foreign ring is set in this test binary, so a wait
//! that went to one would fail with `Unsupported` instead.

#![cfg(feature = "tokio-reactor")]

use std::future::Future;
use std::io;
use std::pin::pin;
use std::process::Stdio;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use ruio::net::portable::{TcpListener, TcpStream};
use ruio::process::Command;
use ruio_compat::{PortableCompat, TokioReactor};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// How long a test waits for anything before it is considered hung.
const PATIENCE: Duration = Duration::from_secs(10);

/// `len` bytes that repeat only every 251.
fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|index| (index % 251) as u8).collect()
}

/// Sets tokio's reactor as the foreign one, and checks that no foreign ring
/// is set.
fn set() {
    ruio::io::set_foreign_reactor(&TokioReactor);
    assert!(ruio::uring::foreign_ring().is_none());
}

/// Runs `future` on a current-thread runtime of tokio's.
fn on_current_thread<Fut: Future<Output = io::Result<()>>>(future: Fut) {
    set();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime
        .block_on(async { tokio::time::timeout(PATIENCE, future).await })
        .expect("finished in time")
        .unwrap();
}

/// Runs `future` as a task on the workers of a multi-threaded runtime of
/// tokio's.
fn on_workers<Fut>(future: Fut)
where
    Fut: Future<Output = io::Result<()>> + Send + 'static,
{
    set();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    runtime
        .block_on(async { tokio::time::timeout(PATIENCE, tokio::spawn(future)).await })
        .expect("finished in time")
        .unwrap()
        .unwrap();
}

/// Echoes what `stream` reads until the end, through tokio's traits.
async fn echo(stream: TcpStream) -> io::Result<()> {
    let mut stream = PortableCompat::new(stream);
    let mut buf = vec![0; 64 << 10];
    loop {
        let read = stream.read(&mut buf).await?;
        if read == 0 {
            return stream.shutdown().await;
        }
        stream.write_all(&buf[..read]).await?;
    }
}

/// Sends `data` over a new connection to `addr` and reads the echo back,
/// through tokio's traits, writing and reading at once.
async fn exchange(addr: std::net::SocketAddr, data: Arc<Vec<u8>>) -> io::Result<()> {
    let stream = PortableCompat::new(TcpStream::connect(addr).await?);
    let (mut reader, mut writer) = tokio::io::split(stream);
    let sent = Arc::clone(&data);
    let sending = tokio::spawn(async move {
        writer.write_all(&sent).await?;
        writer.shutdown().await
    });
    let mut back = Vec::with_capacity(data.len());
    reader.read_to_end(&mut back).await?;
    sending.await.unwrap()?;
    assert!(back == *data);
    Ok(())
}

/// A listener accepts on tokio's reactor, and echoes a connection that sends
/// more than the socket takes at once; both ends on one thread.
#[test]
fn a_portable_echo_waits_on_a_current_thread_runtime() {
    on_current_thread(async {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            echo(stream).await
        });
        exchange(addr, Arc::new(pattern(4 << 20))).await?;
        server.await.unwrap()
    });
}

/// Tasks on four workers accept from one listener at once, and echo the
/// connections of as many clients again, all of whose waits go to tokio's
/// reactor, which keeps one waker each way.
#[test]
fn many_tasks_wait_on_one_listener_across_workers() {
    on_workers(async {
        let listener = Arc::new(TcpListener::bind("127.0.0.1:0")?);
        let addr = listener.local_addr()?;
        let servers: Vec<_> = (0..4)
            .map(|_| {
                let listener = Arc::clone(&listener);
                tokio::spawn(async move {
                    for _ in 0..4 {
                        let (stream, _) = listener.accept().await?;
                        tokio::spawn(echo(stream));
                    }
                    io::Result::Ok(())
                })
            })
            .collect();
        let data = Arc::new(pattern(256 << 10));
        let clients: Vec<_> = (0..16)
            .map(|_| tokio::spawn(exchange(addr, Arc::clone(&data))))
            .collect();
        for client in clients {
            client.await.unwrap()?;
        }
        for server in servers {
            server.await.unwrap()?;
        }
        Ok(())
    });
}

/// A child's pipes wait on tokio's reactor: what is written to `cat` comes
/// back, through tokio's traits.
#[test]
fn a_childs_pipes_wait_on_tokios_reactor() {
    on_current_thread(async {
        let mut child = Command::new("cat")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        let mut stdin = PortableCompat::new(child.stdin.take().unwrap());
        let mut stdout = PortableCompat::new(child.stdout.take().unwrap());
        let data = pattern(1 << 20);
        let sent = data.clone();
        let writing = tokio::spawn(async move {
            stdin.write_all(&sent).await?;
            stdin.shutdown().await
        });
        let mut back = Vec::new();
        stdout.read_to_end(&mut back).await?;
        writing.await.unwrap()?;
        assert!(back == data);
        Ok(())
    });
}

/// A stream handed out after waiting on tokio's reactor has left it: tokio
/// registers it again as its own, which it could not were it still there.
#[test]
fn a_stream_handed_out_has_left_the_reactor() {
    on_current_thread(async {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = tokio::spawn(async move { listener.accept().await });
        let stream = TcpStream::connect(addr).await?;
        let (peer, _) = server.await.unwrap()?;
        let read = tokio::spawn(async move {
            let (read, _) = stream.read(Vec::with_capacity(1)).await;
            read.map(|_| stream)
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        peer.try_write(b"x")?;
        let stream = read.await.unwrap()?;
        // Blocking again once handed out, as tokio refuses to register.
        let stream = stream.into_std();
        stream.set_nonblocking(true)?;
        drop(tokio::net::TcpStream::from_std(stream)?);
        Ok(())
    });
}

/// A thread of no runtime waits on the foreign ring, which is not set: the
/// reactor takes no wait from it.
#[test]
fn a_thread_of_no_runtime_does_not_wait_on_tokio() {
    set();
    let (peer, ours) = std::os::unix::net::UnixStream::pair().unwrap();
    let stream = ruio::net::portable::UnixStream::from_std(ours).unwrap();
    let mut read = pin!(stream.read(Vec::with_capacity(1)));
    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready((read, _)) = read.as_mut().poll(&mut cx) else {
        panic!("a wait with nowhere to go fails at once");
    };
    assert_eq!(read.unwrap_err().kind(), io::ErrorKind::Unsupported);
    drop(peer);
}
