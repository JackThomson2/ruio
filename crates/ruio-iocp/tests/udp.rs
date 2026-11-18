//! UDP sockets through the port: datagrams to and from the standard
//! library's sockets and each other, ones longer than their buffers,
//! receives cancelled while the system has them, and streams of datagrams
//! received into pools of buffers.
#![cfg(windows)]

use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::rc::Rc;
use std::time::Duration;

use ruio_executor::{Runtime, time};
use ruio_iocp::buf::BufPool;
use ruio_iocp::net::{Datagram, RecvFromStream, UdpSocket};

/// How long a test waits for what should come at once, rather than hang.
const PATIENCE: Duration = Duration::from_secs(10);

/// The loopback address, and a port for the system to choose.
fn loopback() -> SocketAddr {
    (Ipv4Addr::LOCALHOST, 0).into()
}

#[test]
fn datagrams_go_between_two_sockets_with_their_senders_addresses() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let one = UdpSocket::bind(loopback())?;
        let other = UdpSocket::bind(loopback())?;
        let (sent, _) = one.send_to(b"ping".to_vec(), other.local_addr()?).await;
        assert_eq!(sent?, 4);
        let (received, buf) = other.recv_from(Vec::with_capacity(16)).await;
        let (len, from) = received?;
        assert_eq!((len, from), (4, one.local_addr()?));
        assert_eq!(buf, b"ping");
        let (sent, _) = other.send_to(b"pong!".to_vec(), from).await;
        assert_eq!(sent?, 5);
        let (received, buf) = one.recv_from(Vec::with_capacity(16)).await;
        assert_eq!(received?, (5, other.local_addr()?));
        assert_eq!(buf, b"pong!");
        Ok(())
    })
}

#[test]
fn a_connected_socket_sends_to_and_receives_from_its_peer() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let one = UdpSocket::bind(loopback())?;
        let other = UdpSocket::bind(loopback())?;
        one.connect(other.local_addr()?)?;
        other.connect(one.local_addr()?)?;
        assert_eq!(one.peer_addr()?, other.local_addr()?);
        let (sent, _) = one.send(b"hello".to_vec()).await;
        assert_eq!(sent?, 5);
        let (received, buf) = other.recv(Vec::with_capacity(16)).await;
        assert_eq!(received?, 5);
        assert_eq!(buf, b"hello");
        Ok(())
    })
}

#[test]
fn the_standard_librarys_socket_is_heard_and_answered() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let ours = UdpSocket::bind(loopback())?;
        let theirs = std::net::UdpSocket::bind(loopback())?;
        theirs.set_read_timeout(Some(PATIENCE))?;
        theirs.send_to(b"hi", ours.local_addr()?)?;
        let (received, buf) = ours.recv_from(Vec::with_capacity(16)).await;
        let (len, from) = received?;
        assert_eq!((len, from), (2, theirs.local_addr()?));
        assert_eq!(buf, b"hi");
        let (sent, _) = ours.send_to(b"yo".to_vec(), from).await;
        assert_eq!(sent?, 2);
        let mut reply = [0; 16];
        let (len, from) = theirs.recv_from(&mut reply)?;
        assert_eq!((&reply[..len], from), (&b"yo"[..], ours.local_addr()?));
        Ok(())
    })
}

#[test]
fn a_datagram_longer_than_a_waiting_buffer_is_cut() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let receiver = Rc::new(UdpSocket::bind(loopback())?);
        let sender = UdpSocket::bind(loopback())?;
        let waiting = ruio_executor::spawn({
            let receiver = Rc::clone(&receiver);
            async move { receiver.recv_from(Vec::with_capacity(4)).await }
        });
        // The task starts its receive, which waits for the datagram.
        ruio_executor::yield_now().await;
        let (sent, _) = sender
            .send_to(b"12345678".to_vec(), receiver.local_addr()?)
            .await;
        assert_eq!(sent?, 8);
        let (received, buf) = time::timeout(PATIENCE, waiting)
            .await
            .expect("the receive completes")
            .unwrap();
        assert_eq!(received?, (4, sender.local_addr()?));
        assert_eq!(buf, b"1234");
        Ok(())
    })
}

#[test]
fn a_datagram_longer_than_the_buffer_that_comes_for_it_is_cut() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let receiver = UdpSocket::bind(loopback())?;
        let sender = UdpSocket::bind(loopback())?;
        let to = receiver.local_addr()?;
        let (sent, _) = sender.send_to(b"12345678".to_vec(), to).await;
        assert_eq!(sent?, 8);
        let (sent, _) = sender.send_to(b"abcdefgh".to_vec(), to).await;
        assert_eq!(sent?, 8);
        // The datagrams are in the socket by now, so each receive completes
        // as it starts, where Windows reports the cut as a failure of the
        // call and queues the completion all the same.
        time::sleep(Duration::from_millis(50)).await;
        let (received, buf) = time::timeout(PATIENCE, receiver.recv_from(Vec::with_capacity(4)))
            .await
            .expect("the receive completes");
        assert_eq!(received?, (4, sender.local_addr()?));
        assert_eq!(buf, b"1234");
        // A connected socket's receive is cut the same way.
        receiver.connect(sender.local_addr()?)?;
        let (received, buf) = time::timeout(PATIENCE, receiver.recv(Vec::with_capacity(3)))
            .await
            .expect("the receive completes");
        assert_eq!(received?, 3);
        assert_eq!(buf, b"abc");
        // The socket receives whole datagrams after.
        let (sent, _) = sender.send_to(b"whole".to_vec(), to).await;
        assert_eq!(sent?, 5);
        let (received, buf) = receiver.recv(Vec::with_capacity(16)).await;
        assert_eq!(received?, 5);
        assert_eq!(buf, b"whole");
        Ok(())
    })
}

#[test]
fn a_datagram_to_a_port_nobody_has_fails_no_later_receive() -> io::Result<()> {
    // A port that was free a moment ago.
    let nobody = std::net::UdpSocket::bind(loopback())?.local_addr()?;
    Runtime::new()?.block_on(async {
        let socket = UdpSocket::bind(loopback())?;
        let (sent, _) = socket.send_to(b"anyone?".to_vec(), nobody).await;
        assert_eq!(sent?, 7);
        // Windows would fail this receive with a reset by default.
        let waited = time::timeout(
            Duration::from_millis(200),
            socket.recv_from(Vec::with_capacity(16)),
        )
        .await;
        assert!(waited.is_err(), "the receive waits for a datagram");
        // The dropped receive was cancelled, and the socket receives after.
        let other = UdpSocket::bind(loopback())?;
        let (sent, _) = other.send_to(b"here".to_vec(), socket.local_addr()?).await;
        assert_eq!(sent?, 4);
        let (received, buf) = socket.recv_from(Vec::with_capacity(16)).await;
        assert_eq!(received?, (4, other.local_addr()?));
        assert_eq!(buf, b"here");
        Ok(())
    })
}

#[test]
fn datagrams_go_over_ipv6() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let one = match UdpSocket::bind((Ipv6Addr::LOCALHOST, 0)) {
            Ok(socket) => socket,
            Err(err) => {
                eprintln!("skipped: no IPv6 loopback: {err}");
                return Ok(());
            }
        };
        let other = UdpSocket::bind((Ipv6Addr::LOCALHOST, 0))?;
        assert!(other.local_addr()?.is_ipv6());
        let (sent, _) = one.send_to(b"six".to_vec(), other.local_addr()?).await;
        assert_eq!(sent?, 3);
        let (received, buf) = other.recv_from(Vec::with_capacity(8)).await;
        assert_eq!(received?, (3, one.local_addr()?));
        assert_eq!(buf, b"six");
        Ok(())
    })
}

#[test]
fn an_ipv6_socket_hears_and_answers_ipv4_addresses() -> io::Result<()> {
    if let Err(err) = std::net::UdpSocket::bind((Ipv6Addr::LOCALHOST, 0)) {
        eprintln!("skipped: no IPv6 loopback: {err}");
        return Ok(());
    }
    Runtime::new()?.block_on(async {
        // The IPv6 form of the IPv4 loopback address, which only a socket
        // that reaches IPv4 addresses binds to.
        let mapped = Ipv4Addr::LOCALHOST.to_ipv6_mapped();
        let six = UdpSocket::bind((mapped, 0))?;
        let four = UdpSocket::bind(loopback())?;
        let port = six.local_addr()?.port();
        let (sent, _) = four
            .send_to(b"four".to_vec(), (Ipv4Addr::LOCALHOST, port).into())
            .await;
        assert_eq!(sent?, 4);
        let (received, buf) = six.recv_from(Vec::with_capacity(8)).await;
        let (len, from) = received?;
        // The sender, in the IPv6 form of its address, which is answered.
        assert_eq!(from, SocketAddr::from((mapped, four.local_addr()?.port())));
        assert_eq!((len, &buf[..]), (4, &b"four"[..]));
        let (sent, _) = six.send_to(b"six".to_vec(), from).await;
        assert_eq!(sent?, 3);
        let (received, buf) = four.recv_from(Vec::with_capacity(8)).await;
        assert_eq!(received?, (3, (Ipv4Addr::LOCALHOST, port).into()));
        assert_eq!(buf, b"six");
        Ok(())
    })
}

#[test]
fn a_socket_keeps_the_option_to_broadcast() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let socket = UdpSocket::bind(loopback())?;
        assert!(!socket.broadcast()?);
        socket.set_broadcast(true)?;
        assert!(socket.broadcast()?);
        Ok(())
    })
}

/// Sends each of `datagrams` from `sender` to `to`.
async fn send_all(
    sender: &UdpSocket,
    to: SocketAddr,
    datagrams: impl IntoIterator<Item = Vec<u8>>,
) -> io::Result<()> {
    for datagram in datagrams {
        let len = datagram.len();
        let (sent, _) = sender.send_to(datagram, to).await;
        assert_eq!(sent?, len);
    }
    Ok(())
}

/// The next datagram of `datagrams`, which should come at once.
async fn next_datagram(datagrams: &mut RecvFromStream<'_>) -> io::Result<Datagram> {
    time::timeout(PATIENCE, datagrams.next())
        .await
        .expect("a datagram comes")
        .expect("the stream goes on")
}

#[test]
fn a_recv_from_stream_answers_each_sender() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let server = UdpSocket::bind(loopback())?;
        let addr = server.local_addr()?;
        let clients = [UdpSocket::bind(loopback())?, UdpSocket::bind(loopback())?];
        for round in 0..3 {
            for (index, client) in clients.iter().enumerate() {
                send_all(client, addr, [vec![index as u8, round]]).await?;
            }
        }
        let pool = BufPool::new(8, 2048)?;
        let mut datagrams = server.recv_from_stream(&pool)?;
        let mut answered = 0;
        while answered < 6 {
            let mut batch = vec![next_datagram(&mut datagrams).await?];
            while let Some(datagram) = datagrams.next_ready() {
                batch.push(datagram?);
            }
            for datagram in batch {
                assert!(!datagram.is_truncated());
                let (data, from) = datagram.into_parts();
                // The payload's room leaves out what io_uring's header and
                // an IPv4 address take.
                assert_eq!((data.len(), data.capacity()), (2, 2048 - 32));
                let (sent, _) = server.send_to(data, from).await;
                assert_eq!(sent?, 2);
                answered += 1;
            }
        }
        for (index, client) in clients.iter().enumerate() {
            for round in 0..3 {
                let (received, buf) =
                    time::timeout(PATIENCE, client.recv_from(Vec::with_capacity(8)))
                        .await
                        .expect("the answer comes");
                assert_eq!(received?, (2, addr));
                assert_eq!(buf, [index as u8, round]);
            }
        }
        assert_eq!(pool.available(), 8);
        assert!(datagrams.next_ready().is_none());
        assert!(!datagrams.is_ended());
        Ok(())
    })
}

#[test]
fn datagrams_that_have_arrived_are_taken_without_waiting() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let receiver = UdpSocket::bind(loopback())?;
        let sender = UdpSocket::bind(loopback())?;
        let pool = BufPool::new(4, 64)?;
        let mut datagrams = receiver.recv_from_stream(&pool)?;
        // Nothing has arrived, and nothing is waited for.
        assert!(datagrams.next_ready().is_none());
        let to = receiver.local_addr()?;
        send_all(&sender, to, (0..10).map(|at| vec![at; 3])).await?;
        time::sleep(Duration::from_millis(50)).await;
        let first = next_datagram(&mut datagrams).await?;
        let mut taken = vec![first[0]];
        drop(first);
        while let Some(datagram) = datagrams.next_ready() {
            let datagram = datagram?;
            assert_eq!((datagram.len(), datagram.from()), (3, sender.local_addr()?));
            taken.push(datagram[0]);
        }
        assert_eq!(taken, (0..10).collect::<Vec<u8>>());
        Ok(())
    })
}

#[test]
fn a_datagram_longer_than_the_room_of_its_buffer_is_cut_short() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let receiver = UdpSocket::bind(loopback())?;
        let sender = UdpSocket::bind(loopback())?;
        let to = receiver.local_addr()?;
        // Of each buffer's 64 bytes, the stream leaves out 32.
        let pool = BufPool::new(4, 64)?;
        send_all(&sender, to, [vec![7; 33], vec![8; 32]]).await?;
        let mut datagrams = receiver.recv_from_stream(&pool)?;
        let cut = next_datagram(&mut datagrams).await?;
        assert_eq!((&*cut, cut.is_truncated()), (&[7; 32][..], true));
        assert_eq!(cut.from(), sender.local_addr()?);
        let whole = next_datagram(&mut datagrams).await?;
        assert_eq!((&*whole, whole.is_truncated()), (&[8; 32][..], false));
        assert_eq!(whole.from(), sender.local_addr()?);
        let debug = format!("{whole:?}");
        assert!(debug.contains("len: 32, truncated: false"), "{debug}");
        assert_eq!(whole.into_data().capacity(), 32);

        // One that arrives while the stream waits is cut the same way.
        let waited = time::timeout(Duration::from_millis(20), datagrams.next()).await;
        assert!(waited.is_err(), "the stream waits for a datagram");
        send_all(&sender, to, [vec![9; 100]]).await?;
        let mut cut = next_datagram(&mut datagrams).await?;
        assert_eq!((&*cut, cut.is_truncated()), (&[9; 32][..], true));
        cut[0] = 0;
        let (data, from) = cut.into_parts();
        assert_eq!(
            (data[..2].to_vec(), from),
            (vec![0, 9], sender.local_addr()?)
        );
        Ok(())
    })
}

#[test]
fn a_recv_from_stream_needs_room_for_a_payload() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let v4 = UdpSocket::bind(loopback())?;
        let small = BufPool::new(2, 32)?;
        let err = v4.recv_from_stream(&small).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        let pool = BufPool::new(2, 48)?;
        assert!(v4.recv_from_stream(&pool).is_ok());

        let v6 = match UdpSocket::bind((Ipv6Addr::LOCALHOST, 0)) {
            Ok(socket) => socket,
            Err(err) => {
                eprintln!("skipped: no IPv6 loopback: {err}");
                return Ok(());
            }
        };
        let err = v6.recv_from_stream(&pool).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        let sender = UdpSocket::bind((Ipv6Addr::LOCALHOST, 0))?;
        send_all(&sender, v6.local_addr()?, [b"six".to_vec()]).await?;
        let pool = BufPool::new(2, 64)?;
        let mut datagrams = v6.recv_from_stream(&pool)?;
        let datagram = next_datagram(&mut datagrams).await?;
        assert_eq!(
            (&*datagram, datagram.from()),
            (&b"six"[..], sender.local_addr()?)
        );
        assert_eq!(datagram.into_data().capacity(), 16);
        Ok(())
    })
}

#[test]
fn datagrams_wait_in_the_socket_while_the_pool_is_dry() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let receiver = UdpSocket::bind(loopback())?;
        let sender = UdpSocket::bind(loopback())?;
        let pool = BufPool::new(1, 64)?;
        let mut datagrams = receiver.recv_from_stream(&pool)?;
        let to = receiver.local_addr()?;
        send_all(&sender, to, [b"one".to_vec(), b"two".to_vec()]).await?;
        let one = next_datagram(&mut datagrams).await?;
        assert_eq!(&*one, b"one");
        assert_eq!(pool.available(), 0);
        // The second waits for the one buffer.
        assert!(datagrams.next_ready().is_none());
        let waited = time::timeout(Duration::from_millis(50), datagrams.next()).await;
        assert!(waited.is_err(), "the stream waits for a buffer");
        drop(one);
        let two = next_datagram(&mut datagrams).await?;
        assert_eq!((&*two, two.from()), (&b"two"[..], sender.local_addr()?));
        drop(two);
        assert_eq!(pool.available(), 1);
        Ok(())
    })
}

#[test]
fn a_datagram_of_no_bytes_is_one_all_the_same() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let receiver = UdpSocket::bind(loopback())?;
        let sender = UdpSocket::bind(loopback())?;
        let to = receiver.local_addr()?;
        let pool = BufPool::new(2, 64)?;
        let mut datagrams = receiver.recv_from_stream(&pool)?;
        // One that arrives while the stream waits for it.
        let waited = time::timeout(Duration::from_millis(20), datagrams.next()).await;
        assert!(waited.is_err(), "the stream waits for a datagram");
        send_all(&sender, to, [Vec::new()]).await?;
        let empty = next_datagram(&mut datagrams).await?;
        assert!(empty.is_empty() && !empty.is_truncated());
        assert_eq!(empty.from(), sender.local_addr()?);
        drop(empty);
        // And one that has arrived before, with another behind it.
        send_all(&sender, to, [Vec::new(), b"after".to_vec()]).await?;
        let empty = next_datagram(&mut datagrams).await?;
        assert!(empty.is_empty());
        drop(empty);
        let after = next_datagram(&mut datagrams).await?;
        assert_eq!(&*after, b"after");
        Ok(())
    })
}

#[test]
fn a_stream_dropped_while_it_waits_leaves_datagrams_to_other_receives() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let receiver = UdpSocket::bind(loopback())?;
        let sender = UdpSocket::bind(loopback())?;
        let to = receiver.local_addr()?;
        let pool = BufPool::new(1, 64)?;
        let mut datagrams = receiver.recv_from_stream(&pool)?;
        let waited = time::timeout(Duration::from_millis(20), datagrams.next()).await;
        assert!(waited.is_err(), "the stream waits for a datagram");
        drop(datagrams);

        // The socket, which the stream made non-blocking, receives through
        // the port as before.
        send_all(&sender, to, [b"after".to_vec()]).await?;
        let (received, buf) = time::timeout(PATIENCE, receiver.recv_from(Vec::with_capacity(16)))
            .await
            .expect("the receive completes");
        assert_eq!(received?, (5, sender.local_addr()?));
        assert_eq!(buf, b"after");
        let waiting = time::timeout(
            Duration::from_millis(20),
            receiver.recv_from(Vec::with_capacity(16)),
        )
        .await;
        assert!(waiting.is_err(), "the receive waits for a datagram");

        // Another stream receives what comes next.
        send_all(&sender, to, [b"again".to_vec()]).await?;
        let mut datagrams = receiver.recv_from_stream(&pool)?;
        let again = next_datagram(&mut datagrams).await?;
        assert_eq!(&*again, b"again");
        Ok(())
    })
}

#[test]
fn a_recv_stream_takes_datagram_after_datagram() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let receiver = UdpSocket::bind(loopback())?;
        let sender = UdpSocket::bind(loopback())?;
        receiver.connect(sender.local_addr()?)?;
        let to = receiver.local_addr()?;
        // Fewer buffers than datagrams, each of which takes one.
        let pool = BufPool::new(4, 16)?;
        send_all(&sender, to, (1..=10).map(|size| vec![size as u8; size])).await?;
        // One longer than a buffer, of which the buffer holds the start.
        send_all(&sender, to, [vec![20; 20]]).await?;
        let mut datagrams = receiver.recv_stream(&pool);
        let mut held = Vec::new();
        for size in 1..=10 {
            let data = time::timeout(PATIENCE, datagrams.next())
                .await
                .expect("a datagram comes")
                .expect("the stream goes on")?;
            assert_eq!((data.buffers(), data.to_vec()), (1, vec![size as u8; size]));
            // The pool runs dry and refills.
            held.push(data);
            if held.len() == 4 {
                held.clear();
            }
        }
        drop(held);
        let cut = time::timeout(PATIENCE, datagrams.next())
            .await
            .expect("a datagram comes")
            .expect("the stream goes on")?;
        assert_eq!(cut.to_vec(), [20; 16]);
        drop(cut);
        assert!(datagrams.next_ready().is_none());
        assert!(!datagrams.is_ended());
        assert_eq!(pool.available(), 4);
        Ok(())
    })
}

#[test]
fn a_socket_binds_only_where_a_runtime_runs() {
    let err = UdpSocket::bind(loopback()).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Other);
}
