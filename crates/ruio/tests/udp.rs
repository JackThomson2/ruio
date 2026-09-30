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

/// Sockets bound with `SO_REUSEPORT` share an address, which a socket
/// without it cannot bind, and the kernel spreads the datagrams among them
/// by sender.
#[ruio::test]
async fn sockets_bound_with_reuseport_share_an_address() -> io::Result<()> {
    let first = UdpSocket::bind_reuseport("127.0.0.1:0")?;
    let addr = first.local_addr()?;
    let second = UdpSocket::bind_reuseport(addr)?;
    assert_eq!(second.local_addr()?, addr);
    let err = UdpSocket::bind(addr).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::AddrInUse, "{err}");

    let senders = (0..32)
        .map(|_| std::net::UdpSocket::bind("127.0.0.1:0"))
        .collect::<io::Result<Vec<_>>>()?;
    for sender in &senders {
        sender.send_to(b"hi", addr)?;
    }
    let mut received = [0; 2];
    for (count, socket) in received.iter_mut().zip([&first, &second]) {
        // Past the last datagram waiting, a receive waits.
        let wait = Duration::from_millis(100);
        while let (Ok((len, _)), _) = socket.recv_from(Vec::with_capacity(8)).timeout(wait).await {
            assert_eq!(len, 2);
            *count += 1;
        }
    }
    assert_eq!(received.iter().sum::<usize>(), senders.len());
    assert!(received.iter().all(|&count| count > 0), "{received:?}");
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
