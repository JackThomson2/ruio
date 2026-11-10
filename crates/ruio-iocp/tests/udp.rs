//! UDP sockets through the port: datagrams to and from the standard
//! library's sockets and each other, ones longer than their buffers, and
//! receives cancelled while the system has them.
#![cfg(windows)]

use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::rc::Rc;
use std::time::Duration;

use ruio_executor::{Runtime, time};
use ruio_iocp::net::UdpSocket;

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
fn a_socket_keeps_the_option_to_broadcast() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let socket = UdpSocket::bind(loopback())?;
        assert!(!socket.broadcast()?);
        socket.set_broadcast(true)?;
        assert!(socket.broadcast()?);
        Ok(())
    })
}

#[test]
fn a_socket_binds_only_where_a_runtime_runs() {
    let err = UdpSocket::bind(loopback()).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Other);
}
