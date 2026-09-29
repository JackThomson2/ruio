//! Multishot accepts, and the stream that keeps them armed.

mod support;

use std::io::{ErrorKind, Read as _};
use std::net::{TcpListener, TcpStream, UdpSocket};
use std::os::fd::AsRawFd;
use std::task::{Context, Poll, Waker};

use ruio_driver::ops::{AcceptMulti, AcceptStream, Send};
use ruio_driver::{Descriptor, Driver};

use support::{PATIENCE, block_on, settle};

/// A client of `listener`, whose blocking reads give up after [`PATIENCE`].
fn connect(listener: &TcpListener) -> TcpStream {
    let stream = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    stream.set_read_timeout(Some(PATIENCE)).unwrap();
    stream
}

/// Whether the server closed `client`'s connection.
fn closed(client: &mut TcpStream) -> bool {
    match client.read(&mut [0]) {
        Ok(0) => true,
        Err(err) => err.kind() == ErrorKind::ConnectionReset,
        Ok(_) => false,
    }
}

fn poll_once(stream: &mut AcceptStream) -> Poll<std::io::Result<Descriptor>> {
    stream.poll_next(&mut Context::from_waker(Waker::noop()))
}

#[test]
fn one_request_accepts_connection_after_connection() {
    let driver = Driver::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut stream = AcceptStream::new(&driver, AcceptMulti::new(listener.as_raw_fd()));
    for round in 0..5 {
        let mut client = connect(&listener);
        let server = block_on(&driver, stream.next()).unwrap();
        assert!(!server.is_direct());
        let (sent, _) = block_on(&driver, driver.op(Send::new(server.fd(), vec![round])));
        assert_eq!(sent.unwrap(), 1);
        let mut byte = [0];
        client.read_exact(&mut byte).unwrap();
        assert_eq!(byte, [round]);
    }
    // One accept request and five sends.
    assert_eq!(driver.stats().operations, 6);
}

/// With room for eight completions, a burst of connections can end the
/// request on the way, and the stream re-arms it until all are accepted.
#[test]
fn a_burst_beyond_the_completion_queue_is_accepted() {
    let driver = Driver::builder().entries(2).build().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut clients: Vec<TcpStream> = (0..40).map(|_| connect(&listener)).collect();
    let mut stream = AcceptStream::new(&driver, AcceptMulti::new(listener.as_raw_fd()));
    let accepted: Vec<Descriptor> = (0..40)
        .map(|_| block_on(&driver, stream.next()).unwrap())
        .collect();
    // The kernel ends a multishot accept when it has no room for another
    // completion, so the burst needed more than one request.
    let requests = driver.stats().operations;
    assert!(
        requests > 1,
        "40 connections took {requests} accept requests"
    );
    drop(accepted);
    assert!(clients.iter_mut().all(closed));
}

#[test]
fn direct_connections_fill_the_table_then_fail() {
    let driver = Driver::builder().files(2).build().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let request = AcceptMulti::new(listener.as_raw_fd()).direct(&driver);
    let mut stream = AcceptStream::new(&driver, request);
    let _clients = [connect(&listener), connect(&listener)];
    let mut accepted = vec![
        block_on(&driver, stream.next()).unwrap(),
        block_on(&driver, stream.next()).unwrap(),
    ];
    assert!(accepted.iter().all(Descriptor::is_direct));

    // The kernel accepts a third connection, finds no slot for it and closes
    // it.
    let mut third = connect(&listener);
    let err = block_on(&driver, stream.next()).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENFILE));
    assert!(closed(&mut third));

    // A freed slot takes the next connection, once the stream re-arms.
    drop(accepted.pop());
    let _fourth = connect(&listener);
    assert!(block_on(&driver, stream.next()).unwrap().is_direct());
}

/// An error ends the request; the stream yields it, and re-arms the request
/// when polled again.
#[test]
fn the_stream_carries_on_after_an_error() {
    let driver = Driver::new().unwrap();
    // Nothing can be accepted on a datagram socket.
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let mut stream = AcceptStream::new(&driver, AcceptMulti::new(socket.as_raw_fd()));
    for round in 1..=3 {
        let err = block_on(&driver, stream.next()).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EOPNOTSUPP));
        assert_eq!(driver.stats().operations, round);
    }
}

/// Connections the kernel accepted for a stream that is dropped before
/// taking them are closed.
#[test]
fn a_dropped_stream_closes_what_it_had_not_taken() {
    let driver = Driver::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut clients: Vec<TcpStream> = (0..3).map(|_| connect(&listener)).collect();
    let mut stream = AcceptStream::new(&driver, AcceptMulti::new(listener.as_raw_fd()));
    assert!(poll_once(&mut stream).is_pending());
    // The kernel accepts all three at once, and they wait in the stream.
    driver.submit().unwrap();

    drop(stream);
    settle(&driver);
    // A connection left in the backlog would stay open with the listener.
    for client in &mut clients {
        assert!(closed(client));
    }
}

#[test]
fn streams_dropped_after_every_number_of_polls() {
    let driver = Driver::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    for polls in 0..6 {
        let direct = polls % 2 == 1;
        let _client = (polls % 3 != 0).then(|| connect(&listener));
        let mut request = AcceptMulti::new(listener.as_raw_fd());
        if direct {
            request = request.direct(&driver);
        }
        let mut stream = AcceptStream::new(&driver, request);
        for _ in 0..polls {
            if let Poll::Ready(accepted) = poll_once(&mut stream) {
                assert_eq!(accepted.unwrap().is_direct(), direct);
            }
            driver.submit().unwrap();
        }
        drop(stream);
        settle(&driver);
    }
    assert_eq!(driver.in_flight(), 0);
}
