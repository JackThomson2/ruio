//! The adapters, through the traits of futures-io.

#![cfg(feature = "futures-io")]

mod support;

use std::future::poll_fn;
use std::io::{self, ErrorKind};
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use futures_util::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use ruio::buf::BufPool;
use ruio::net::UnixStream;
use ruio_compat::Compat;

use support::{Reads, pair, pattern, within};

/// Polls a read of `buf` from `stream` once.
async fn poll_read_once<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut [u8],
) -> Poll<io::Result<usize>> {
    poll_fn(|cx| Poll::Ready(Pin::new(&mut *stream).poll_read(cx, buf))).await
}

/// More than a read buffer holds and a write takes goes one way, and a reply
/// the other, each side closing after it.
async fn cross_both_ways(reads: Reads) -> io::Result<()> {
    let (client, server) = pair(false).await?;
    let (mut client, mut server) = (Compat::new(client), reads.adapt(server));
    let message = pattern(1 << 20, 3);
    let expected = message.clone();
    let reader = ruio::spawn(async move {
        // Not `read_to_end`, which zeroes all its spare room again each time
        // a read is pending; and less at a time than the adapter receives.
        let mut received = Vec::new();
        let mut buf = vec![0; 300];
        loop {
            let read = within(server.read(&mut buf)).await?;
            if read == 0 {
                break;
            }
            received.extend_from_slice(&buf[..read]);
        }
        within(server.write_all(b"received")).await?;
        within(server.close()).await?;
        io::Result::Ok(received)
    });
    within(client.write_all(&message)).await?;
    within(client.close()).await?;
    let mut reply = Vec::new();
    within(client.read_to_end(&mut reply)).await?;
    assert!(reader.await.unwrap()? == expected);
    assert_eq!(reply, b"received");
    Ok(())
}

#[ruio::test]
async fn data_crosses_the_adapters_both_ways() -> io::Result<()> {
    cross_both_ways(Reads::Buffer(1000)).await
}

/// The pool is too small for what arrives, so its receive waits for the
/// reader to give buffers back, again and again.
#[ruio::test]
async fn data_crosses_the_adapters_through_a_pool() -> io::Result<()> {
    cross_both_ways(Reads::Pool(4, 1000)).await
}

/// A direct descriptor's operations reach it through its ring's table.
#[ruio::test]
async fn direct_streams_are_adapted() -> io::Result<()> {
    let (client, server) = pair(true).await?;
    let (mut client, mut server) = (Compat::new(client), Compat::new(server));
    within(server.write_all(b"from a direct descriptor")).await?;
    within(server.close()).await?;
    let mut received = String::new();
    within(client.read_to_string(&mut received)).await?;
    assert_eq!(received, "from a direct descriptor");
    Ok(())
}

/// Lines come out of the adapter's buffers, however they fall across
/// receives and buffers.
#[ruio::test]
async fn lines_are_read_from_the_buffers() -> io::Result<()> {
    for reads in [Reads::Buffer(5), Reads::Pool(2, 5)] {
        read_lines(reads).await?;
    }
    Ok(())
}

async fn read_lines(reads: Reads) -> io::Result<()> {
    let (writer, reader) = UnixStream::pair()?;
    let (mut writer, mut reader) = (Compat::new(writer), reads.adapt(reader));
    within(writer.write_all(b"one\ntwo\nthree, the last\n")).await?;
    within(writer.close()).await?;
    let mut lines = Vec::new();
    loop {
        let mut line = String::new();
        if within(reader.read_line(&mut line)).await? == 0 {
            break;
        }
        lines.push(line);
    }
    assert_eq!(lines, ["one\n", "two\n", "three, the last\n"], "{reads:?}");
    Ok(())
}

/// A read takes all that the pool's receive has brought in, however many
/// buffers it fills, as far as the read's buffer takes it.
#[ruio::test]
async fn a_read_takes_what_several_buffers_hold() -> io::Result<()> {
    let (client, server) = pair(false).await?;
    let pool = BufPool::new(&ruio::driver(), 8, 100)?;
    let (mut client, mut server) = (Compat::new(client), Compat::with_pool(&pool, server));
    let message = pattern(500, 4);
    within(client.write_all(&message[..100])).await?;
    let mut buf = vec![0; 1000];
    within(server.read_exact(&mut buf[..1])).await?;
    // Sent one after another, the rest arrives in receives of its own, which
    // the loopback interface completes long before the read.
    for part in message[100..].chunks(100) {
        within(client.write_all(part)).await?;
        ruio::yield_now().await;
    }
    ruio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(within(server.read(&mut buf[1..])).await?, 499);
    assert!(buf[..500] == message);
    Ok(())
}

/// A dropped adapter gives back the buffers it held, and those its receive
/// took after.
#[ruio::test]
async fn a_dropped_adapter_gives_its_buffers_back() -> io::Result<()> {
    let (client, server) = pair(false).await?;
    let pool = BufPool::new(&ruio::driver(), 8, 64)?;
    let (mut client, mut server) = (Compat::new(client), Compat::with_pool(&pool, server));
    within(client.write_all(&pattern(4096, 5))).await?;
    let mut first = [0; 1];
    within(server.read_exact(&mut first)).await?;
    assert!(pool.available() < pool.count(), "the adapter holds buffers");
    drop(server);
    let driver = ruio::driver();
    within(async {
        while driver.in_flight() > 0 || pool.available() < pool.count() {
            ruio::yield_now().await;
        }
    })
    .await;
    Ok(())
}

/// A receive in flight, with nothing to read yet, holds up neither writes
/// nor the data that then arrives, which the next read takes.
#[ruio::test]
async fn a_read_in_flight_leaves_writes_free() -> io::Result<()> {
    let (client, server) = pair(false).await?;
    let (mut client, mut server) = (Compat::new(client), Compat::new(server));
    let mut buf = [0; 4];
    assert!(poll_read_once(&mut client, &mut buf).await.is_pending());
    within(client.write_all(b"ping")).await?;
    within(client.flush()).await?;
    within(server.read_exact(&mut buf)).await?;
    assert_eq!(&buf, b"ping");
    within(server.write_all(b"pong")).await?;
    within(client.read_exact(&mut buf)).await?;
    assert_eq!(&buf, b"pong");
    Ok(())
}

/// Written behind, the data goes out though the adapter is dropped before
/// the send reaches the kernel, and the stream closes after it.
#[ruio::test]
async fn what_was_written_arrives_though_the_adapter_is_dropped() -> io::Result<()> {
    let (client, server) = pair(false).await?;
    let (mut client, mut server) = (Compat::new(client), Compat::new(server));
    within(client.write_all(b"last words")).await?;
    drop(client);
    let mut received = Vec::new();
    within(server.read_to_end(&mut received)).await?;
    assert_eq!(received, b"last words");
    Ok(())
}

/// Closing waits for the write in flight before it shuts the connection
/// down, which would cut the write short. A Unix socket holds less than
/// the write, which so waits for the reader.
#[ruio::test]
async fn closing_waits_for_the_write_in_flight() -> io::Result<()> {
    let (writer, reader) = UnixStream::pair()?;
    let (mut writer, mut reader) = (Compat::new(writer), Compat::new(reader));
    let message = pattern(256 << 10, 5);
    let expected = message.clone();
    let reader = ruio::spawn(async move {
        let mut received = Vec::new();
        let mut buf = vec![0; 64 << 10];
        loop {
            let read = within(reader.read(&mut buf)).await?;
            if read == 0 {
                break;
            }
            received.extend_from_slice(&buf[..read]);
        }
        io::Result::Ok(received)
    });
    within(writer.write_all(&message)).await?;
    within(writer.close()).await?;
    assert!(reader.await.unwrap()? == expected);
    Ok(())
}

/// A flush has nothing to wait for, not even a send that waits for a
/// reader.
#[ruio::test]
async fn flushing_does_not_wait_for_the_send_in_flight() -> io::Result<()> {
    let (writer, reader) = UnixStream::pair()?;
    let (mut writer, mut reader) = (Compat::new(writer), Compat::new(reader));
    let message = pattern(256 << 10, 6);
    within(writer.write_all(&message)).await?;
    let flushed = poll_fn(|cx| Poll::Ready(Pin::new(&mut writer).poll_flush(cx))).await;
    assert!(matches!(flushed, Poll::Ready(Ok(()))), "{flushed:?}");
    let mut received = vec![0; message.len()];
    within(reader.read_exact(&mut received)).await?;
    assert!(received == message);
    Ok(())
}

/// Dropping an adapter cancels the receive in flight, and closes the
/// stream.
#[ruio::test]
async fn dropping_an_adapter_cancels_its_read() -> io::Result<()> {
    let (client, server) = pair(false).await?;
    let (mut client, mut server) = (Compat::new(client), Compat::new(server));
    let mut buf = [0; 4];
    assert!(poll_read_once(&mut client, &mut buf).await.is_pending());
    drop(client);
    assert_eq!(within(server.read(&mut buf)).await?, 0, "the stream ends");
    let driver = ruio::driver();
    within(async {
        while driver.in_flight() > 0 {
            ruio::yield_now().await;
        }
    })
    .await;
    Ok(())
}

/// A write that fails, as the peer resets the connection, fails the writes
/// after it, and the flush.
#[ruio::test]
async fn a_failed_write_fails_what_comes_after() -> io::Result<()> {
    let (client, server) = pair(false).await?;
    let mut client = Compat::new(client);
    let mut server = Compat::new(server);
    within(client.write_all(b"unread")).await?;
    within(client.flush()).await?;
    let mut buf = [0; 1];
    within(server.read_exact(&mut buf)).await?;
    // Closed with data unread, the socket resets the connection.
    drop(server);
    let chunk = vec![7; 64 << 10];
    let mut failed = None;
    for _ in 0..1000 {
        if let Err(err) = within(client.write_all(&chunk)).await {
            failed = Some(err);
            break;
        }
    }
    let err = failed.expect("writes to a reset connection fail");
    assert!(
        matches!(
            err.kind(),
            ErrorKind::BrokenPipe | ErrorKind::ConnectionReset
        ),
        "{err}"
    );
    assert_eq!(within(client.flush()).await.unwrap_err().kind(), err.kind());
    assert_eq!(
        within(client.write(b"more")).await.unwrap_err().kind(),
        err.kind()
    );
    Ok(())
}

/// Once closed, the writing half takes nothing more; closing again does
/// nothing. Empty reads and writes move nothing.
#[ruio::test]
async fn writes_after_close_fail() -> io::Result<()> {
    let (client, server) = pair(false).await?;
    let (mut client, mut server) = (Compat::new(client), Compat::new(server));
    assert_eq!(within(client.read(&mut [])).await?, 0);
    assert_eq!(within(client.write(&[])).await?, 0);
    within(client.close()).await?;
    within(client.close()).await?;
    let err = within(client.write(b"late")).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::BrokenPipe);
    let mut received = Vec::new();
    within(server.read_to_end(&mut received)).await?;
    assert!(received.is_empty());
    Ok(())
}
