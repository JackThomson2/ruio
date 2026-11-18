//! Operations through the port that time out: cancelled once their time is
//! up, they fail with `ERROR_TIMEOUT` and hand back what they hold, and what
//! they were on carries on as before.
#![cfg(windows)]

use std::io::{self, Write as _};
use std::net::{self, Ipv4Addr, SocketAddr};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use ruio_executor::Runtime;
use ruio_iocp::net::{TcpListener, TcpStream, UdpSocket};
use ruio_iocp::pipe::NamedPipe;

/// `ERROR_TIMEOUT`.
const ERROR_TIMEOUT: i32 = 1460;

/// Long enough that an operation that is to complete does.
const AMPLE: Duration = Duration::from_secs(10);

fn name(test: &str) -> String {
    format!(r"\\.\pipe\ruio-iocp-timeout-{}-{test}", std::process::id())
}

/// The loopback address, and a port for the system to choose.
fn loopback() -> SocketAddr {
    (Ipv4Addr::LOCALHOST, 0).into()
}

/// Asserts that `err` is the error of an operation whose time ran out.
fn assert_timed_out(err: &io::Error) {
    assert_eq!(err.kind(), io::ErrorKind::TimedOut, "{err}");
    assert_eq!(err.raw_os_error(), Some(ERROR_TIMEOUT), "{err}");
}

#[test]
fn a_read_that_times_out_hands_back_its_buffer() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let server = NamedPipe::create(name("read"))?;
        let client = NamedPipe::connect(name("read"))?;
        let timeout = Duration::from_millis(30);
        let started = Instant::now();
        let (read, buf) = server.read(Vec::with_capacity(16)).timeout(timeout).await;
        assert_timed_out(&read.unwrap_err());
        assert!(started.elapsed() >= timeout);
        assert!(buf.is_empty());
        assert_eq!(buf.capacity(), 16);
        // The pipe reads on, into the buffer handed back, and a read that
        // completes in time is not cut short.
        client.write(b"ping".to_vec()).await.0?;
        let (read, buf) = server.read(buf).timeout(AMPLE).await;
        assert_eq!(read?, 4);
        assert_eq!(buf, b"ping");
        Ok(())
    })
}

#[test]
fn a_write_the_other_end_does_not_read_times_out() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let server = NamedPipe::create(name("write"))?;
        let client = NamedPipe::connect(name("write"))?;
        // Far more than the pipe buffers, and the server reads none of it.
        let write = client.write(vec![7; 4 << 20]);
        let (written, buf) = write.timeout(Duration::from_millis(30)).await;
        assert_timed_out(&written.unwrap_err());
        assert_eq!(buf.len(), 4 << 20);
        // The other way, the pipe carries on.
        server.write(b"pong".to_vec()).timeout(AMPLE).await.0?;
        let (read, buf) = client.read(Vec::with_capacity(16)).await;
        assert_eq!(read?, 4);
        assert_eq!(buf, b"pong");
        Ok(())
    })
}

#[test]
fn the_reads_of_a_read_exact_share_its_time() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let listener = TcpListener::bind(loopback())?;
        let addr = listener.local_addr()?;
        // The peer sends part of the buffer, more of it 150 ms later, and
        // then nothing until the read is done.
        let (done, wait) = mpsc::channel::<()>();
        let peer = thread::spawn(move || -> io::Result<()> {
            let mut stream = net::TcpStream::connect(addr)?;
            stream.write_all(&[1; 4])?;
            thread::sleep(Duration::from_millis(150));
            stream.write_all(&[2; 2])?;
            let _ = wait.recv();
            Ok(())
        });
        let (stream, _) = listener.accept().await?;
        let timeout = Duration::from_millis(300);
        let started = Instant::now();
        let (read, buf) = stream
            .read_exact(Vec::with_capacity(8))
            .timeout(timeout)
            .await;
        let elapsed = started.elapsed();
        let _ = done.send(());
        peer.join().expect("the peer panicked")?;
        assert_timed_out(&read.unwrap_err());
        // What was read is kept.
        assert_eq!(buf, [1, 1, 1, 1, 2, 2]);
        // The last read had what was left of the time, not all of it, which
        // would have taken it to 450 ms.
        assert!(elapsed >= timeout, "{elapsed:?}");
        assert!(elapsed < Duration::from_millis(440), "{elapsed:?}");
        Ok(())
    })
}

#[test]
fn an_accept_that_times_out_leaves_the_listener_listening() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let listener = TcpListener::bind(loopback())?;
        let accept = listener.accept().timeout(Duration::from_millis(30));
        assert_timed_out(&accept.await.unwrap_err());
        let client = TcpStream::connect(listener.local_addr()?).await?;
        let (_server, peer) = listener.accept().timeout(AMPLE).await?;
        assert_eq!(peer, client.local_addr()?);
        Ok(())
    })
}

#[test]
fn a_receive_that_times_out_hands_back_its_buffer() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let socket = UdpSocket::bind(loopback())?;
        let receive = socket.recv_from(Vec::with_capacity(8));
        let (received, buf) = receive.timeout(Duration::from_millis(30)).await;
        assert_timed_out(&received.unwrap_err());
        assert_eq!(buf.capacity(), 8);
        let sender = UdpSocket::bind(loopback())?;
        let send = sender.send_to(b"hi".to_vec(), socket.local_addr()?);
        send.timeout(AMPLE).await.0?;
        let (received, buf) = socket.recv_from(buf).timeout(AMPLE).await;
        assert_eq!(received?, (2, sender.local_addr()?));
        assert_eq!(buf, b"hi");
        Ok(())
    })
}
