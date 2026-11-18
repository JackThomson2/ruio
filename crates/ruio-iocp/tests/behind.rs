//! Writes left behind a stream: sent without being waited for, in order
//! with the writes after them, and reported by those when they fail.
#![cfg(windows)]

use std::io::{self, Read as _};
use std::net::{Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

use ruio_buf::IoBuf;
use ruio_executor::{Runtime, time};
use ruio_iocp::net::{TcpListener, TcpStream};

/// The loopback address, and a port for the system to choose.
fn loopback() -> SocketAddr {
    (Ipv4Addr::LOCALHOST, 0).into()
}

/// A listener, a stream connected to it, and the stream it accepted.
async fn pair() -> io::Result<(TcpListener, TcpStream, TcpStream)> {
    let listener = TcpListener::bind(loopback())?;
    let client = TcpStream::connect(listener.local_addr()?).await?;
    let (server, _) = listener.accept().await?;
    Ok((listener, client, server))
}

/// `len` bytes that no part of which repeats the part before it.
fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|at| (at % 251) as u8).collect()
}

/// The system's code of the error `result` is.
fn code<T: std::fmt::Debug>(result: io::Result<T>) -> Option<i32> {
    result
        .expect_err("a write after one that failed")
        .raw_os_error()
}

#[test]
fn a_write_behind_is_sent_without_being_waited_for() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let (_listener, client, server) = pair().await?;
        // Nothing has been written behind the stream.
        assert!(matches!(client.try_flush(), Some(Ok(()))));
        client.flush().await?;

        let bufs = vec![b"hello, ".to_vec(), Vec::new(), b"world".to_vec()];
        client.write_behind(bufs).await?;
        // Buffers that hold nothing send nothing.
        client.write_behind(Vec::<Vec<u8>>::new()).await?;
        client.write_behind([Vec::<u8>::new()]).await?;
        let (read, buf) = server.read_exact(Vec::with_capacity(12).slice(..12)).await;
        read?;
        assert_eq!(buf.into_inner(), b"hello, world");
        client.flush().await?;
        assert!(matches!(client.try_flush(), Some(Ok(()))));
        Ok(())
    })
}

#[test]
fn the_writes_after_a_write_behind_wait_for_it_and_go_in_order() -> io::Result<()> {
    const LEN: usize = 8 << 20;
    const TAIL: &[u8] = b"one.two.three.four";
    Runtime::new()?.block_on(async {
        let (_listener, client, server) = pair().await?;
        let sent = pattern(LEN);
        // More than the system takes while the peer reads nothing, so the
        // send is in flight when the writes after it are made.
        client.write_behind(vec![sent.clone()]).await?;
        let reading = ruio_executor::spawn(async move {
            let total = LEN + TAIL.len();
            let buf = Vec::with_capacity(total).slice(..total);
            let (read, buf) = server.read_exact(buf).await;
            read.map(|()| buf.into_inner())
        });
        let (written, _) = client.write(b"one.".to_vec()).await;
        assert_eq!(written?, 4);
        let (written, _) = client.write_all(b"two.".to_vec()).await;
        written?;
        let (written, _) = client
            .write_all_vectored([b"th".to_vec(), b"ree.".to_vec()])
            .await;
        written?;
        client.write_behind(vec![b"four".to_vec()]).await?;
        client.flush().await?;
        assert!(matches!(client.try_flush(), Some(Ok(()))));
        let received = reading.await.unwrap()?;
        assert!(received[..LEN] == sent[..], "the write behind came first");
        assert_eq!(&received[LEN..], TAIL);
        Ok(())
    })
}

#[test]
fn a_stream_dropped_with_a_write_behind_in_flight_closes_once_it_is_sent() -> io::Result<()> {
    const LEN: usize = 8 << 20;
    Runtime::new()?.block_on(async {
        let (_listener, client, server) = pair().await?;
        let sent = pattern(LEN);
        client.write_behind(vec![sent.clone()]).await?;
        drop(client);
        // All of it arrives, and then the end of the stream.
        let mut received = Vec::with_capacity(LEN);
        loop {
            let (read, buf) = server.read(Vec::with_capacity(256 * 1024)).await;
            if read? == 0 {
                break;
            }
            received.extend_from_slice(&buf);
        }
        assert!(received == sent, "what arrived is what was written behind");
        Ok(())
    })
}

#[test]
fn a_write_behind_that_fails_fails_the_writes_after_it() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let (_listener, client, server) = pair().await?;
        // The peer goes with data unread, so what is sent to it after that
        // is refused.
        let (written, _) = client.write_all(b"unread".to_vec()).await;
        written?;
        time::sleep(Duration::from_millis(20)).await;
        drop(server);
        let mut failed = None;
        for _ in 0..200 {
            let sent = client.write_behind(vec![vec![7_u8; 64 * 1024]]).await;
            let flushed = match sent {
                Ok(()) => client.flush().await,
                Err(err) => Err(err),
            };
            if let Err(err) = flushed {
                failed = Some(err);
                break;
            }
            time::sleep(Duration::from_millis(10)).await;
        }
        let failed = failed.expect("a write to a peer that has gone fails");
        let first = failed.raw_os_error();
        assert!(first.is_some(), "{failed}");

        // Every write after it fails with its error, and hands its buffers
        // back unwritten.
        let known = client.try_flush().expect("no write behind is in flight");
        assert_eq!(code(known), first);
        assert_eq!(code(client.flush().await), first);
        let (written, buf) = client.write(b"late".to_vec()).await;
        assert_eq!(code(written), first);
        assert_eq!(buf, b"late");
        let (written, buf) = client.write_all(b"later".to_vec()).await;
        assert_eq!(code(written), first);
        assert_eq!(buf, b"later");
        let bufs = vec![b"la".to_vec(), b"test".to_vec()];
        let (written, bufs) = client.write_all_vectored(bufs).await;
        assert_eq!(code(written), first);
        assert_eq!(bufs, [&b"la"[..], b"test"]);
        let behind = client.write_behind(vec![b"last".to_vec()]).await;
        assert_eq!(code(behind), first);
        Ok(())
    })
}

#[test]
fn a_runtime_dropped_cancels_a_write_behind_in_flight_and_closes_its_socket() -> io::Result<()> {
    const LEN: usize = 32 << 20;
    let listener = std::net::TcpListener::bind(loopback())?;
    let addr = listener.local_addr()?;
    // Long enough that only the send's cancellation, not the timeout, ends
    // the wait in time.
    let runtime = Runtime::builder()
        .shutdown_timeout(Duration::from_secs(60))
        .build()?;
    runtime.block_on(async {
        let client = TcpStream::connect(addr).await?;
        // The peer reads nothing, so the send is in flight when the stream
        // goes, and the runtime after it.
        client.write_behind(vec![vec![1_u8; LEN]]).await?;
        io::Result::Ok(())
    })?;
    let (mut peer, _) = listener.accept()?;
    let started = Instant::now();
    drop(runtime);
    assert!(started.elapsed() < Duration::from_secs(30));

    // The socket has closed: the peer reads what was sent by then and finds
    // the stream ended, or reset, rather than waiting for more.
    peer.set_read_timeout(Some(Duration::from_secs(30)))?;
    match peer.read_to_end(&mut Vec::new()) {
        Ok(read) => assert!(read <= LEN),
        Err(err) => assert!(
            !matches!(
                err.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
            ),
            "the peer waited for more: {err}"
        ),
    }
    Ok(())
}
