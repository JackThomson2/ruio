//! UDP sockets inside the runtime.

mod support;

use std::io::{self, ErrorKind};
use std::net::SocketAddr;
use std::time::Duration;

use ruio::buf::BufPool;
use ruio::net::UdpSocket;

use support::within;

#[ruio::test]
async fn datagrams_go_to_and_come_from_addresses() -> io::Result<()> {
    let a = UdpSocket::bind("127.0.0.1:0")?;
    let b = UdpSocket::bind("127.0.0.1:0")?;
    let to: SocketAddr = b.local_addr()?;
    assert_eq!(within(a.send_to("hello", to)).await.0?, 5);
    let (received, buf) = within(b.recv_from(Vec::with_capacity(16))).await;
    assert_eq!(received?, (5, a.local_addr()?));
    assert_eq!(buf, b"hello");

    // Connected, a socket needs no address to send.
    b.connect(a.local_addr()?)?;
    assert_eq!(b.peer_addr()?, a.local_addr()?);
    within(b.send("back")).await.0?;
    let (received, buf) = within(a.recv(Vec::with_capacity(16))).await;
    assert_eq!(received?, 4);
    assert_eq!(buf, b"back");

    assert_eq!(within(a.send_zc_to(vec![1; 100], to)).await.0?, 100);
    let (received, _) = within(b.recv(Vec::with_capacity(128))).await;
    assert_eq!(received?, 100);

    let recv = a
        .recv(Vec::with_capacity(8))
        .timeout(Duration::from_millis(10));
    assert_eq!(recv.await.0.unwrap_err().kind(), ErrorKind::TimedOut);
    Ok(())
}

#[ruio::test]
async fn a_recv_stream_takes_datagram_after_datagram() -> io::Result<()> {
    let receiver = UdpSocket::bind("127.0.0.1:0")?;
    let sender = UdpSocket::bind("127.0.0.1:0")?;
    // Fewer buffers than datagrams: the pool runs dry and refills.
    let pool = BufPool::new(&ruio::driver(), 4, 64)?;
    for size in 1..=10 {
        let datagram = vec![size as u8; size];
        within(sender.send_to(datagram, receiver.local_addr()?))
            .await
            .0?;
    }
    let mut datagrams = receiver.recv_stream(&pool);
    for size in 1..=10 {
        let datagram = within(datagrams.next()).await.unwrap()?;
        assert_eq!(datagram.to_vec(), vec![size as u8; size]);
    }
    Ok(())
}
