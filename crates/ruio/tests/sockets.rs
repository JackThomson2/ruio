//! The sockets every engine with I/O offers, through the API the engines
//! share: the tests name nothing that only one engine has, and run on
//! io_uring and on the I/O completion port alike.

#![cfg(any(ruio_uring, ruio_iocp))]

mod support;

use std::io::{self, ErrorKind};
use std::net::Shutdown;

use ruio::buf::IoBuf;
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
