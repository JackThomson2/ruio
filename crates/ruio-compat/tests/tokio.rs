//! The adapters, through the traits of `tokio::io`.

#![cfg(feature = "tokio")]

mod support;

use std::io::{self, IoSlice};

use ruio::net::{TcpListener, TcpStream, UnixStream};
use ruio_compat::Compat;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use support::{Reads, pair, pattern, within};

/// More than a read buffer holds and a write takes goes one way, and a reply
/// the other, each side shutting down after it.
async fn cross_both_ways(reads: Reads) -> io::Result<()> {
    let (client, server) = pair(false).await?;
    let (mut client, mut server) = (Compat::new(client), reads.adapt(server));
    let message = pattern(1 << 20, 7);
    let expected = message.clone();
    let reader = ruio::spawn(async move {
        let mut received = Vec::new();
        within(server.read_to_end(&mut received)).await?;
        within(server.write_all(b"received")).await?;
        within(server.shutdown()).await?;
        io::Result::Ok(received)
    });
    within(client.write_all(&message)).await?;
    within(client.shutdown()).await?;
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

#[ruio::test]
async fn data_crosses_the_adapters_through_a_pool() -> io::Result<()> {
    cross_both_ways(Reads::Pool(4, 1000)).await
}

/// A vectored write sends its buffers together, in order, as many bytes of
/// them as one write takes.
#[ruio::test]
async fn vectored_writes_gather_their_buffers() -> io::Result<()> {
    let (writer, reader) = UnixStream::pair()?;
    let (mut writer, mut reader) = (Compat::new(writer), Compat::new(reader));
    assert!(writer.is_write_vectored());
    let small = [
        IoSlice::new(b"head"),
        IoSlice::new(b""),
        IoSlice::new(b"er, "),
        IoSlice::new(b"body"),
    ];
    assert_eq!(within(writer.write_vectored(&small)).await?, 12);
    let (first, second) = (pattern(200 << 10, 1), pattern(200 << 10, 2));
    let large = [IoSlice::new(&first), IoSlice::new(&second)];
    let expected = [b"header, body", &first[..], &second[..]].concat();
    let receiver = ruio::spawn(async move {
        let mut received = Vec::new();
        within(reader.read_to_end(&mut received)).await?;
        io::Result::Ok(received)
    });
    let written = within(writer.write_vectored(&large)).await?;
    assert_eq!(written, 256 << 10, "a write takes 256 KiB at most");
    within(writer.write_all(&expected[12 + written..])).await?;
    within(writer.shutdown()).await?;
    assert!(receiver.await.unwrap()? == expected);
    Ok(())
}

/// Lines come out of the adapter's buffers, however they fall across
/// receives and buffers.
#[ruio::test]
async fn lines_are_read_from_the_buffers() -> io::Result<()> {
    for reads in [Reads::Buffer(5), Reads::Pool(2, 5)] {
        let (writer, reader) = UnixStream::pair()?;
        let mut writer = Compat::new(writer);
        within(writer.write_all(b"one\ntwo\nthree, the last\n")).await?;
        within(writer.shutdown()).await?;
        let mut lines = reads.adapt(reader).lines();
        let mut read = Vec::new();
        while let Some(line) = within(lines.next_line()).await? {
            read.push(line);
        }
        assert_eq!(read, ["one", "two", "three, the last"], "{reads:?}");
    }
    Ok(())
}

/// tokio's `copy_bidirectional` proxies a connection through the adapters,
/// to a server that echoes it, and shuts each way down as it ends.
#[ruio::test]
async fn a_proxy_copies_both_ways() -> io::Result<()> {
    let backend = TcpListener::bind("127.0.0.1:0")?;
    let backend_addr = backend.local_addr()?;
    let _backend = ruio::spawn(async move {
        let (stream, _) = backend.accept().await?;
        let mut stream = Compat::new(stream);
        let mut buf = vec![0; 4096];
        loop {
            let read = stream.read(&mut buf).await?;
            if read == 0 {
                break;
            }
            stream.write_all(&buf[..read]).await?;
        }
        stream.shutdown().await
    });
    let proxy = TcpListener::bind("127.0.0.1:0")?;
    let proxy_addr = proxy.local_addr()?;
    let proxied = ruio::spawn(async move {
        let (client, _) = proxy.accept().await?;
        let mut client = Compat::new(client);
        let mut server = Compat::new(TcpStream::connect(backend_addr).await?);
        tokio::io::copy_bidirectional(&mut client, &mut server).await
    });

    let client = Compat::new(within(TcpStream::connect(proxy_addr)).await?);
    let (mut reader, mut writer) = tokio::io::split(client);
    let message = pattern(300 << 10, 9);
    let sent = message.clone();
    let sender = ruio::spawn(async move {
        within(writer.write_all(&sent)).await?;
        within(writer.shutdown()).await
    });
    let mut echo = Vec::new();
    within(reader.read_to_end(&mut echo)).await?;
    sender.await.unwrap()?;
    assert!(echo == message);
    let len = message.len() as u64;
    assert_eq!(within(proxied).await.unwrap()?, (len, len));
    Ok(())
}
