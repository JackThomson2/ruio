//! The sockets every engine with I/O offers, through the API the engines
//! share: the tests name nothing that only one engine has, and run on
//! io_uring and on the I/O completion port alike.

#![cfg(any(ruio_uring, ruio_iocp))]

mod support;

use std::io::{self, ErrorKind};
use std::net::Shutdown;
use std::rc::Rc;
use std::time::Duration;

use ruio::buf::{BufPool, IoBuf};
use ruio::net::{TcpListener, TcpStream, UdpSocket};

use support::within;

/// A listener on the loopback interface, a stream connected to it, and the
/// stream it accepted.
async fn pair() -> io::Result<(TcpListener, TcpStream, TcpStream)> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let client = within(TcpStream::connect(listener.local_addr()?)).await?;
    let (server, peer) = within(listener.accept()).await?;
    assert_eq!(peer, client.local_addr().await?);
    Ok((listener, client, server))
}

/// `len` bytes that repeat only every 251.
fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|index| (index % 251) as u8).collect()
}

#[ruio::test]
async fn a_stream_reads_what_its_peer_writes() -> io::Result<()> {
    let (listener, client, server) = pair().await?;
    assert_eq!(client.peer_addr().await?, listener.local_addr()?);
    assert_eq!(server.local_addr().await?, listener.local_addr()?);
    assert_eq!(server.peer_addr().await?, client.local_addr().await?);

    let (written, _) = within(client.write(b"ping".to_vec())).await;
    assert_eq!(written?, 4);
    let (read, buf) = within(server.read(Vec::with_capacity(16))).await;
    assert_eq!(read?, 4);
    assert_eq!(buf, b"ping");

    let (written, _) = within(server.write("pong")).await;
    assert_eq!(written?, 4);
    let (read, buf) = within(client.read(Vec::with_capacity(16))).await;
    assert_eq!(read?, 4);
    assert_eq!(buf, b"pong");
    Ok(())
}

#[ruio::test]
async fn whole_buffers_are_written_and_filled() -> io::Result<()> {
    const LEN: usize = 300 * 1024;
    let (_listener, client, server) = pair().await?;
    let data: Vec<u8> = (0..LEN).map(|i| (i % 253) as u8).collect();
    // The data is read while it is written, as neither end's buffers hold
    // all of it.
    let reading = ruio::spawn(async move {
        let (read, buf) = server
            .read_exact(Vec::with_capacity(LEN).slice(..LEN))
            .await;
        read.map(|()| buf.into_inner())
    });
    let (written, data) = within(client.write_all(data)).await;
    written?;
    let read = within(reading).await.unwrap()?;
    assert!(read == data, "what is read is what was written");
    Ok(())
}

#[ruio::test]
async fn a_stream_that_ends_short_fails_a_read_of_a_whole_buffer() -> io::Result<()> {
    let (_listener, client, server) = pair().await?;
    within(client.write_all(b"abc".to_vec())).await.0?;
    drop(client);
    let buf = Vec::with_capacity(8).slice(..8);
    let (read, buf) = within(server.read_exact(buf)).await;
    assert_eq!(read.unwrap_err().kind(), ErrorKind::UnexpectedEof);
    assert_eq!(buf.into_inner(), b"abc");
    Ok(())
}

#[ruio::test]
async fn a_stream_shut_down_for_writing_still_reads() -> io::Result<()> {
    let (_listener, client, server) = pair().await?;
    within(client.shutdown(Shutdown::Write)).await?;
    let (read, _) = within(server.read(Vec::with_capacity(8))).await;
    assert_eq!(read?, 0);
    within(server.write_all(b"late".to_vec())).await.0?;
    let (read, buf) = within(client.read(Vec::with_capacity(8))).await;
    assert_eq!(read?, 4);
    assert_eq!(buf, b"late");
    Ok(())
}

#[ruio::test]
async fn incoming_yields_the_connections_as_they_arrive() -> io::Result<()> {
    const CLIENTS: u8 = 8;
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let serving = ruio::spawn(async move {
        let mut incoming = listener.incoming();
        let mut sum = 0;
        for _ in 0..CLIENTS {
            let stream = incoming.next().await.expect("connections never end")?;
            let (read, buf) = stream.read(Vec::with_capacity(4)).await;
            assert_eq!(read?, 1);
            sum += u32::from(buf[0]);
            stream.write_all(buf).await.0?;
        }
        io::Result::Ok(sum)
    });
    for client in 0..CLIENTS {
        let stream = within(TcpStream::connect(addr)).await?;
        within(stream.write_all(vec![client])).await.0?;
        let (read, buf) = within(stream.read(Vec::with_capacity(4))).await;
        assert_eq!(read?, 1);
        assert_eq!(buf, [client]);
    }
    let sum = within(serving).await.unwrap()?;
    assert_eq!(sum, (0..u32::from(CLIENTS)).sum::<u32>());
    Ok(())
}

#[ruio::test]
async fn a_streams_options_read_back() -> io::Result<()> {
    let (_listener, client, _server) = pair().await?;
    client.set_nodelay(true).await?;
    assert!(client.nodelay().await?);
    client.set_nodelay(false).await?;
    assert!(!client.nodelay().await?);
    client.set_ttl(42).await?;
    assert_eq!(client.ttl().await?, 42);
    assert_eq!(client.linger().await?, None);
    client.set_linger(Some(Duration::from_secs(3))).await?;
    assert_eq!(client.linger().await?, Some(Duration::from_secs(3)));
    client.set_linger(None).await?;
    assert_eq!(client.linger().await?, None);
    assert!(client.take_error().await?.is_none());
    Ok(())
}

#[ruio::test]
async fn a_connect_that_nothing_listens_for_is_refused() -> io::Result<()> {
    // Bound, and let go: nothing listens there.
    let closed = TcpListener::bind("127.0.0.1:0")?.local_addr()?;
    let err = within(TcpStream::connect(closed)).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ConnectionRefused, "{err}");
    Ok(())
}

#[ruio::test]
async fn datagrams_go_to_and_come_from_addresses() -> io::Result<()> {
    let a = UdpSocket::bind("127.0.0.1:0")?;
    let b = UdpSocket::bind("127.0.0.1:0")?;
    let to = b.local_addr()?;
    assert_eq!(within(a.send_to("hello", to)).await.0?, 5);
    let (received, buf) = within(b.recv_from(Vec::with_capacity(16))).await;
    assert_eq!(received?, (5, a.local_addr()?));
    assert_eq!(buf, b"hello");

    // Connected, a socket needs no address to send, and receives from its
    // peer alone.
    a.connect(to)?;
    b.connect(a.local_addr()?)?;
    assert_eq!(b.peer_addr()?, a.local_addr()?);
    assert_eq!(within(b.send("back")).await.0?, 4);
    let (received, buf) = within(a.recv(Vec::with_capacity(16))).await;
    assert_eq!(received?, 4);
    assert_eq!(buf, b"back");
    Ok(())
}

/// The buffer holds the datagram's start, and the rest is lost, with no
/// error, whatever the system makes of it.
#[ruio::test]
async fn a_datagram_longer_than_its_buffer_is_cut() -> io::Result<()> {
    let a = UdpSocket::bind("127.0.0.1:0")?;
    let b = UdpSocket::bind("127.0.0.1:0")?;
    assert_eq!(within(a.send_to("12345678", b.local_addr()?)).await.0?, 8);
    let buf = Vec::with_capacity(4).slice(..4);
    let (received, buf) = within(b.recv_from(buf)).await;
    assert_eq!(received?, (4, a.local_addr()?));
    assert_eq!(buf.into_inner(), b"1234");
    Ok(())
}

#[ruio::test]
async fn a_datagram_sockets_options_read_back() -> io::Result<()> {
    let socket = UdpSocket::bind("127.0.0.1:0")?;
    assert!(!socket.broadcast()?);
    socket.set_broadcast(true)?;
    assert!(socket.broadcast()?);
    Ok(())
}

#[ruio::test]
async fn a_stream_is_received_into_a_pool() -> io::Result<()> {
    const LEN: usize = 100_000;
    let (_listener, client, server) = pair().await?;
    let pool = BufPool::new(4, 4096)?;
    let sent = pattern(LEN);
    let writing = ruio::spawn({
        let sent = sent.clone();
        async move {
            client.write_all(sent).await.0?;
            client.shutdown(Shutdown::Write).await?;
            // Kept open until the other end has read it all.
            io::Result::Ok(client)
        }
    });
    let mut arriving = server.recv_stream(&pool);
    assert!(!arriving.is_ended());
    let mut received = Vec::with_capacity(LEN);
    while let Some(data) = within(arriving.next()).await {
        let data = data?;
        assert!(!data.is_empty());
        assert!(data.buffers() <= pool.count());
        received.extend_from_slice(&data.to_vec());
    }
    assert!(arriving.is_ended());
    assert!(within(arriving.next()).await.is_none());
    assert!(arriving.next_ready().is_none());
    assert!(received == sent, "what arrived is what was written");
    drop(arriving);
    assert_eq!(pool.available(), 4);
    let _client = within(writing).await.unwrap()?;
    Ok(())
}

#[ruio::test]
async fn a_stream_waits_for_a_buffer_of_a_pool_that_is_all_leased() -> io::Result<()> {
    let (_listener, client, server) = pair().await?;
    let pool = BufPool::new(1, 64)?;
    let mut arriving = server.recv_stream(&pool);
    within(client.write_all(b"one".to_vec())).await.0?;
    let one = within(arriving.next()).await.expect("the first data")?;
    assert_eq!(one.to_vec(), b"one");
    assert_eq!(pool.available(), 0);

    // What arrives next waits for the one buffer, which the lease holds.
    within(client.write_all(b"two".to_vec())).await.0?;
    let waited = ruio::time::timeout(Duration::from_millis(50), arriving.next()).await;
    assert!(waited.is_err());
    drop(one);
    let two = within(arriving.next()).await.expect("the second data")?;
    assert_eq!(two.to_vec(), b"two");
    Ok(())
}

#[ruio::test]
async fn what_has_arrived_is_taken_from_a_stream_without_waiting() -> io::Result<()> {
    let (_listener, client, server) = pair().await?;
    let pool = BufPool::new(8, 16)?;
    let mut arriving = server.recv_stream(&pool);
    // Nothing has arrived.
    assert!(arriving.next_ready().is_none());
    let sent = pattern(40);
    within(client.write_all(sent.clone())).await.0?;
    let mut data = within(arriving.next()).await.expect("the data")?;
    // What is there already is taken at once, and the rest, if it came
    // apart on the way, as it arrives.
    while data.len() < sent.len() {
        let more = match arriving.next_ready() {
            Some(more) => more?,
            None => within(arriving.next()).await.expect("the rest")?,
        };
        data.append(more);
    }
    assert_eq!(data.to_vec(), sent);
    assert!(data.buffers() >= 3);
    let lens: usize = data.iter().map(|lease| lease.len()).sum();
    assert_eq!(lens, data.len());
    assert_eq!(pool.available(), 8 - data.buffers());
    assert!(arriving.next_ready().is_none());
    assert!(!arriving.is_ended());
    Ok(())
}

#[ruio::test]
async fn several_buffers_are_written_as_one() -> io::Result<()> {
    let (_listener, client, server) = pair().await?;
    let bufs = vec![
        b"hello, ".to_vec(),
        Vec::new(),
        b"wor".to_vec(),
        b"ld".to_vec(),
    ];
    let (written, bufs) = within(client.write_all_vectored(bufs)).await;
    written?;
    assert_eq!(bufs, [&b"hello, "[..], b"", b"wor", b"ld"]);
    let buf = Vec::with_capacity(12).slice(..12);
    let (read, buf) = within(server.read_exact(buf)).await;
    read?;
    assert_eq!(buf.into_inner(), b"hello, world");

    // An array of buffers is written the same, and no buffers write
    // nothing.
    let bufs = [b"ab".to_vec(), b"cd".to_vec()];
    within(server.write_all_vectored(bufs)).await.0?;
    within(server.write_all_vectored(Vec::<Vec<u8>>::new()))
        .await
        .0?;
    let buf = Vec::with_capacity(4).slice(..4);
    let (read, buf) = within(client.read_exact(buf)).await;
    read?;
    assert_eq!(buf.into_inner(), b"abcd");
    Ok(())
}

#[ruio::test]
async fn what_is_received_into_a_pool_is_written_back_as_it_is() -> io::Result<()> {
    const LEN: usize = 200_000;
    let (_listener, client, server) = pair().await?;
    let pool = BufPool::new(16, 4096)?;
    let echo = ruio::spawn(async move {
        let mut arriving = server.recv_stream(&pool);
        let mut echoed = 0;
        while let Some(data) = arriving.next().await {
            let mut data = data?;
            // What has arrived by now goes out with it, up to half of the
            // pool.
            while data.buffers() < 8 {
                match arriving.next_ready() {
                    Some(more) => data.append(more?),
                    None => break,
                }
            }
            echoed += data.len();
            server.write_all_vectored(data).await.0?;
        }
        drop(arriving);
        server.shutdown(Shutdown::Write).await?;
        io::Result::Ok((echoed, server))
    });
    let client = Rc::new(client);
    // The echo is read while the data is written, as neither end's buffers
    // hold all of it.
    let reading = ruio::spawn({
        let client = Rc::clone(&client);
        async move {
            let mut back = Vec::with_capacity(LEN);
            loop {
                let (read, buf) = client.read(Vec::with_capacity(64 * 1024)).await;
                if read? == 0 {
                    return io::Result::Ok(back);
                }
                back.extend_from_slice(&buf);
            }
        }
    });
    let (written, sent) = within(client.write_all(pattern(LEN))).await;
    written?;
    within(client.shutdown(Shutdown::Write)).await?;
    let back = within(reading).await.unwrap()?;
    assert!(back == sent, "the echo is the data");
    let (echoed, _server) = within(echo).await.unwrap()?;
    assert_eq!(echoed, LEN);
    Ok(())
}

#[ruio::test]
async fn a_write_behind_is_sent_without_being_waited_for() -> io::Result<()> {
    let (_listener, client, server) = pair().await?;
    // Nothing has been written behind the stream.
    assert!(matches!(client.try_flush(), Some(Ok(()))));
    within(client.flush()).await?;

    let bufs = vec![b"hello, ".to_vec(), Vec::new(), b"world".to_vec()];
    within(client.write_behind(bufs)).await?;
    // No buffers send nothing.
    within(client.write_behind(Vec::<Vec<u8>>::new())).await?;
    let buf = Vec::with_capacity(12).slice(..12);
    let (read, buf) = within(server.read_exact(buf)).await;
    read?;
    assert_eq!(buf.into_inner(), b"hello, world");
    within(client.flush()).await?;
    assert!(matches!(client.try_flush(), Some(Ok(()))));
    Ok(())
}

#[ruio::test]
async fn the_writes_after_a_write_behind_wait_for_it_and_go_in_order() -> io::Result<()> {
    const LEN: usize = 8 << 20;
    const TAIL: &[u8] = b"one.two.three.four";
    let (_listener, client, server) = pair().await?;
    let sent = pattern(LEN);
    // More than the system takes while the peer reads nothing, so the send
    // is in flight when the writes after it are made.
    within(client.write_behind(vec![sent.clone()])).await?;
    let reading = ruio::spawn(async move {
        let mut received = Vec::with_capacity(LEN + TAIL.len());
        loop {
            let (read, buf) = server.read(Vec::with_capacity(256 * 1024)).await;
            if read? == 0 {
                return io::Result::Ok(received);
            }
            received.extend_from_slice(&buf);
        }
    });
    let (written, _) = within(client.write(b"one.".to_vec())).await;
    assert_eq!(written?, 4);
    within(client.write_all(b"two.".to_vec())).await.0?;
    let bufs = [b"th".to_vec(), b"ree.".to_vec()];
    within(client.write_all_vectored(bufs)).await.0?;
    within(client.write_behind(vec![b"four".to_vec()])).await?;
    // The shutdown waits for the write behind it, too.
    within(client.shutdown(Shutdown::Write)).await?;
    assert!(matches!(client.try_flush(), Some(Ok(()))));
    let received = within(reading).await.unwrap()?;
    assert_eq!(received.len(), LEN + TAIL.len());
    assert!(received[..LEN] == sent[..], "the write behind came first");
    assert_eq!(&received[LEN..], TAIL);
    Ok(())
}

#[ruio::test]
async fn a_write_behind_that_fails_fails_the_writes_after_it() -> io::Result<()> {
    /// The system's code of the error `result` is.
    fn code<T: std::fmt::Debug>(result: io::Result<T>) -> Option<i32> {
        result
            .expect_err("a write after one that failed")
            .raw_os_error()
    }

    let (_listener, client, server) = pair().await?;
    // The peer goes with data unread, so what is sent to it after that is
    // refused.
    within(client.write_all(b"unread".to_vec())).await.0?;
    ruio::time::sleep(Duration::from_millis(20)).await;
    drop(server);
    let mut failed = None;
    for _ in 0..200 {
        let sent = within(client.write_behind(vec![vec![7_u8; 64 * 1024]])).await;
        let flushed = match sent {
            Ok(()) => within(client.flush()).await,
            Err(err) => Err(err),
        };
        if let Err(err) = flushed {
            failed = Some(err);
            break;
        }
        ruio::time::sleep(Duration::from_millis(10)).await;
    }
    let failed = failed.expect("a write to a peer that has gone fails");
    let first = failed.raw_os_error();
    assert!(first.is_some(), "{failed}");

    // Every write after it fails with its error, and hands its buffers
    // back unwritten.
    let known = client.try_flush().expect("no write behind is in flight");
    assert_eq!(code(known), first);
    assert_eq!(code(within(client.flush()).await), first);
    let (written, buf) = within(client.write(b"late".to_vec())).await;
    assert_eq!(code(written), first);
    assert_eq!(buf, b"late");
    let (written, buf) = within(client.write_all(b"later".to_vec())).await;
    assert_eq!(code(written), first);
    assert_eq!(buf, b"later");
    let bufs = vec![b"la".to_vec(), b"test".to_vec()];
    let (written, bufs) = within(client.write_all_vectored(bufs)).await;
    assert_eq!(code(written), first);
    assert_eq!(bufs, [&b"la"[..], b"test"]);
    let behind = within(client.write_behind(vec![b"last".to_vec()])).await;
    assert_eq!(code(behind), first);
    assert_eq!(code(within(client.shutdown(Shutdown::Write)).await), first);
    Ok(())
}

/// A listener's connections are a `Stream` of futures-core.
#[cfg(feature = "futures")]
#[ruio::test]
async fn incoming_is_a_stream_of_connections() -> io::Result<()> {
    use futures_util::StreamExt;
    use futures_util::stream::FusedStream;

    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let mut incoming = listener.incoming();
    assert!(!incoming.is_terminated());
    let connecting = ruio::spawn(async move { TcpStream::connect(addr).await });
    let server = within(StreamExt::next(&mut incoming))
        .await
        .expect("connections never end")?;
    let client = within(connecting).await.unwrap()?;
    assert_eq!(server.peer_addr().await?, client.local_addr().await?);
    Ok(())
}

/// What arrives on a connection is a `Stream` of futures-core.
#[cfg(feature = "futures")]
#[ruio::test]
async fn what_arrives_is_a_stream_of_bundles() -> io::Result<()> {
    use futures_util::StreamExt;
    use futures_util::stream::FusedStream;

    let (_listener, client, server) = pair().await?;
    let pool = BufPool::new(4, 1024)?;
    let sent = pattern(10_000);
    let writing = ruio::spawn({
        let sent = sent.clone();
        async move {
            client.write_all(sent).await.0?;
            client.shutdown(Shutdown::Write).await?;
            io::Result::Ok(client)
        }
    });
    let mut arriving = server.recv_stream(&pool);
    assert!(!arriving.is_terminated());
    let mut received = Vec::new();
    while let Some(data) = within(StreamExt::next(&mut arriving)).await {
        received.extend_from_slice(&data?.to_vec());
    }
    assert!(received == sent, "what arrived is what was written");
    assert!(arriving.is_terminated());
    let _client = within(writing).await.unwrap()?;
    Ok(())
}
