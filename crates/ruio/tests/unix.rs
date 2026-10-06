//! Unix sockets inside the runtime.

mod support;

use std::fs;
use std::io::{self, ErrorKind};
use std::path::PathBuf;

use ruio::buf::BufPool;
use ruio::net::{UnixDatagram, UnixListener, UnixStream};

use support::within;

/// A path for a socket in the temporary directory, removed when dropped.
///
/// Not in the target directory, whose path can be too long for a socket
/// address, which holds 107 bytes; nor in a temporary directory that long.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let file = format!("ruio-{name}-{}.sock", std::process::id());
        let mut path = std::env::temp_dir().join(&file);
        if path.as_os_str().len() >= 100 {
            path = PathBuf::from("/tmp").join(file);
        }
        let _ = fs::remove_file(&path);
        Self(path)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

#[ruio::test]
async fn streams_write_behind_in_order() -> io::Result<()> {
    let (writer, reader) = UnixStream::pair()?;
    for round in 0..10u8 {
        within(writer.write_behind(vec![vec![round; 1000]])).await?;
    }
    within(writer.flush()).await?;
    let (read, received) = within(reader.read_exact(Vec::with_capacity(10_000))).await;
    read?;
    for (round, chunk) in received.chunks(1000).enumerate() {
        assert!(chunk.iter().all(|&byte| usize::from(byte) == round));
    }
    Ok(())
}

/// The other writes wait for a write behind in flight.
#[ruio::test]
async fn stream_writes_wait_for_the_write_behind_in_flight() -> io::Result<()> {
    let (writer, reader) = UnixStream::pair()?;
    let large: Vec<u8> = (0..8 << 20).map(|index| (index % 251) as u8).collect();
    let expected = [&large[..], b"tail"].concat();
    let len = expected.len();
    let read = ruio::spawn(async move {
        let (read, received) = within(reader.read_exact(Vec::with_capacity(len))).await;
        read.map(|()| received)
    });
    within(writer.write_behind(vec![large])).await?;
    within(writer.write_all(b"tail".to_vec())).await.0?;
    assert!(read.await.unwrap()? == expected);
    Ok(())
}

#[ruio::test]
async fn streams_connect_to_a_listener() -> io::Result<()> {
    let path = Scratch::new("stream");
    let listener = UnixListener::bind(&path.0)?;
    let client = ruio::spawn(UnixStream::connect(path.0.clone()));
    let (server, peer) = within(listener.accept()).await?;
    let client = client.await.unwrap()?;
    assert!(peer.is_unnamed());
    assert_eq!(
        client.peer_addr().await?.as_pathname(),
        Some(path.0.as_path())
    );

    within(client.write_all("over a unix socket")).await.0?;
    let (read, buf) = within(server.read_exact(Vec::with_capacity(18))).await;
    read?;
    assert_eq!(buf, b"over a unix socket");

    // A direct stream asks its addresses through a descriptor installed for
    // the call, and is given one for good by `into_plain`.
    listener.set_direct(true);
    let mut incoming = listener.incoming();
    let _client = ruio::spawn(UnixStream::connect(path.0.clone()));
    let direct = within(incoming.next()).await.unwrap()?;
    assert!(direct.is_direct());
    assert_eq!(
        within(direct.local_addr()).await?.as_pathname(),
        Some(path.0.as_path())
    );
    assert!(within(direct.peer_addr()).await?.is_unnamed());
    assert!(within(direct.take_error()).await?.is_none());
    let plain = within(direct.into_plain()).await?;
    assert!(!plain.is_direct());
    assert_eq!(
        within(plain.local_addr()).await?.as_pathname(),
        Some(path.0.as_path())
    );
    Ok(())
}

#[ruio::test]
async fn connecting_to_nothing_fails() {
    let path = Scratch::new("nothing");
    let err = within(UnixStream::connect(&path.0)).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    let long = "x".repeat(200);
    let err = within(UnixStream::connect(long)).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
}

#[ruio::test]
async fn a_pair_streams_into_a_pool() -> io::Result<()> {
    let (a, b) = UnixStream::pair()?;
    let pool = BufPool::new(&ruio::driver(), 2, 4)?;
    within(a.write_all("0123456789")).await.0?;
    drop(a);
    let mut received = Vec::new();
    let mut stream = b.recv_stream(&pool);
    while let Some(bundle) = within(stream.next()).await {
        received.extend(bundle?.to_vec());
    }
    assert_eq!(received, b"0123456789");
    Ok(())
}

#[ruio::test]
async fn datagrams_come_with_their_senders_address() -> io::Result<()> {
    let receiver_path = Scratch::new("receiver");
    let sender_path = Scratch::new("sender");
    let receiver = UnixDatagram::bind(&receiver_path.0)?;
    let named = UnixDatagram::bind(&sender_path.0)?;
    let unbound = UnixDatagram::unbound()?;

    within(named.send_to("from named", &receiver_path.0))
        .await
        .0?;
    let (received, buf) = within(receiver.recv_from(Vec::with_capacity(32))).await;
    let (len, from) = received?;
    assert_eq!(&buf[..len], b"from named");
    assert_eq!(from.as_pathname(), Some(sender_path.0.as_path()));
    within(receiver.send_to_addr("reply", &from)).await.0?;
    let (received, buf) = within(named.recv(Vec::with_capacity(8))).await;
    assert_eq!(received?, 5);
    assert_eq!(buf, b"reply");

    within(unbound.send_to("from unbound", &receiver_path.0))
        .await
        .0?;
    let (received, _) = within(receiver.recv_from(Vec::with_capacity(32))).await;
    assert!(received?.1.is_unnamed());

    // Paths that make no address fail the send.
    for path in [String::new(), "x".repeat(200)] {
        let (sent, _) = within(unbound.send_to("x", path)).await;
        assert_eq!(sent.unwrap_err().kind(), ErrorKind::InvalidInput);
    }
    Ok(())
}

#[ruio::test]
async fn connected_datagram_pairs_need_no_address() -> io::Result<()> {
    let (a, b) = UnixDatagram::pair()?;
    within(a.send("one")).await.0?;
    within(a.send("two")).await.0?;
    let (received, buf) = within(b.recv(Vec::with_capacity(8))).await;
    assert_eq!((received?, &buf[..]), (3, &b"one"[..]));
    let (received, buf) = within(b.recv(Vec::with_capacity(8))).await;
    assert_eq!((received?, &buf[..]), (3, &b"two"[..]));
    Ok(())
}
