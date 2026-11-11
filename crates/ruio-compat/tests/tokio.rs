//! The adapters, through the traits of `tokio::io`.

#![cfg(all(feature = "tokio", ruio_uring))]

mod support;

use std::io::{self, IoSlice};

use ruio::net::UnixStream;
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

#[cfg(ruio_compat_ring)]
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
    let ways = [
        Reads::Buffer(5),
        #[cfg(ruio_compat_ring)]
        Reads::Pool(2, 5),
    ];
    for reads in ways {
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
