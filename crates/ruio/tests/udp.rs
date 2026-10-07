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

/// A socket that is not connected answers its senders from the stream of
/// its datagrams, taking those that have arrived together: each sender gets
/// its own datagrams back, in order, and only those.
#[ruio::test]
async fn a_recv_from_stream_answers_each_sender() -> io::Result<()> {
    let server = UdpSocket::bind("127.0.0.1:0")?;
    let addr = server.local_addr()?;
    let clients = [
        UdpSocket::bind("127.0.0.1:0")?,
        UdpSocket::bind("127.0.0.1:0")?,
    ];
    for round in 0..3 {
        for (index, client) in clients.iter().enumerate() {
            within(client.send_to(vec![index as u8, round], addr))
                .await
                .0?;
        }
    }
    let pool = BufPool::new(&ruio::driver(), 8, 2048)?;
    let mut datagrams = server.recv_from_stream(&pool)?;
    let mut answered = 0;
    while answered < 6 {
        let mut batch = vec![within(datagrams.next()).await.unwrap()?];
        while let Some(datagram) = datagrams.next_ready() {
            batch.push(datagram?);
        }
        for datagram in batch {
            assert!(!datagram.is_truncated());
            let (data, from) = datagram.into_parts();
            within(server.send_to(data, from)).await.0?;
            answered += 1;
        }
    }
    for (index, client) in clients.iter().enumerate() {
        for round in 0..3 {
            let (received, buf) = within(client.recv_from(Vec::with_capacity(8))).await;
            assert_eq!(received?, (2, addr));
            assert_eq!(buf, [index as u8, round]);
        }
    }
    assert_eq!(pool.available(), 8);
    assert!(!datagrams.is_ended());
    Ok(())
}

/// A datagram longer than its buffer has room for after the header and the
/// address is cut short, and says so; the next one comes whole.
#[ruio::test]
async fn a_recv_from_stream_cuts_short_what_its_buffers_cannot_hold() -> io::Result<()> {
    let receiver = UdpSocket::bind("127.0.0.1:0")?;
    let sender = UdpSocket::bind("127.0.0.1:0")?;
    // Of each buffer's 64 bytes, the header and an IPv4 address take 32.
    let pool = BufPool::new(&ruio::driver(), 4, 64)?;
    within(sender.send_to(vec![7; 33], receiver.local_addr()?))
        .await
        .0?;
    within(sender.send_to(vec![8; 32], receiver.local_addr()?))
        .await
        .0?;
    let mut datagrams = receiver.recv_from_stream(&pool)?;
    let cut = within(datagrams.next()).await.unwrap()?;
    assert_eq!((&*cut, cut.is_truncated()), (&[7; 32][..], true));
    let whole = within(datagrams.next()).await.unwrap()?;
    assert_eq!((&*whole, whole.is_truncated()), (&[8; 32][..], false));
    assert_eq!(whole.from(), sender.local_addr()?);
    assert_eq!(whole.into_data().capacity(), 32);
    Ok(())
}

/// The header and an IPv4 address take 32 bytes of each buffer, and an
/// IPv6 one 48, which a pool's buffers must have more than.
#[ruio::test]
async fn a_recv_from_stream_needs_room_for_a_payload() -> io::Result<()> {
    let v4 = UdpSocket::bind("127.0.0.1:0")?;
    let small = BufPool::new(&ruio::driver(), 2, 32)?;
    let err = v4.recv_from_stream(&small).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
    let pool = BufPool::new(&ruio::driver(), 2, 48)?;
    assert!(v4.recv_from_stream(&pool).is_ok());

    let Ok(v6) = UdpSocket::bind("[::1]:0") else {
        eprintln!("skipped: no IPv6 loopback address");
        return Ok(());
    };
    let err = v6.recv_from_stream(&pool).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
    let sender = UdpSocket::bind("[::1]:0")?;
    within(sender.send_to("six", v6.local_addr()?)).await.0?;
    let pool = BufPool::new(&ruio::driver(), 2, 64)?;
    let mut datagrams = v6.recv_from_stream(&pool)?;
    let datagram = within(datagrams.next()).await.unwrap()?;
    assert_eq!(
        (&*datagram, datagram.from()),
        (&b"six"[..], sender.local_addr()?)
    );
    Ok(())
}
