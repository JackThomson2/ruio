//! The streams of multishot requests through the `Stream` and `FusedStream`
//! traits of futures-core, and the combinators of futures-util over them.

#![cfg(feature = "futures")]

mod support;

use std::io::{self, Read as _, Write as _};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::UnixStream;

use futures_util::StreamExt;
use futures_util::stream::FusedStream;
use ruio_driver::Driver;
use ruio_driver::buf::BufPool;
use ruio_driver::ops::{AcceptMulti, AcceptStream, PollMulti, PollStream, ReadStream, RecvStream};

use support::{block_on, settle};

/// `len` bytes that repeat only every 251.
fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|index| (index % 251) as u8).collect()
}

/// A poll stream is a stream of readiness that never ends; each item comes
/// through the trait as it does through the method.
#[test]
fn a_poll_stream_is_a_stream_that_never_ends() {
    let driver = Driver::new().unwrap();
    let (mut reader, mut writer) = io::pipe().unwrap();
    let mut events = PollStream::new(
        &driver,
        PollMulti::new(reader.as_raw_fd(), libc::POLLIN as u32),
    );
    assert!(!events.is_terminated());
    for round in 0..5u8 {
        writer.write_all(&[round]).unwrap();
        let ready = block_on(&driver, StreamExt::next(&mut events))
            .expect("the stream never ends")
            .unwrap();
        assert_ne!(ready & libc::POLLIN as u32, 0);
        let mut byte = [0];
        reader.read_exact(&mut byte).unwrap();
        assert_eq!(byte, [round]);
    }
    assert!(!events.is_terminated());
    drop(events);
    settle(&driver);
}

/// A receive stream collects everything the peer sends, and once the peer
/// has shut down it is terminated, and stays so.
#[test]
fn a_recv_stream_ends_when_the_peer_shuts_down_and_says_so() {
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 8, 4096).unwrap();
    let (socket, mut peer) = UnixStream::pair().unwrap();
    let mut received = RecvStream::new(&driver, socket.as_raw_fd(), &pool);
    assert!(!received.is_terminated());
    let sent = pattern(20_000);
    peer.write_all(&sent).unwrap();
    peer.shutdown(Shutdown::Write).unwrap();

    let bundles: Vec<_> = block_on(&driver, StreamExt::collect(&mut received));
    let mut gathered = Vec::new();
    for bundle in bundles {
        for lease in bundle.unwrap() {
            gathered.extend_from_slice(&lease);
        }
    }
    assert_eq!(gathered, sent);
    assert!(received.is_terminated());
    assert!(block_on(&driver, StreamExt::next(&mut received)).is_none());
    assert_eq!(pool.available(), 8, "every buffer is back");
}

/// A read stream reads a pipe to its end, in order, through combinators that
/// map and collect, and is terminated once the writer is gone.
#[test]
fn a_read_stream_ends_at_the_end_of_the_file_and_says_so() {
    let driver = Driver::new().unwrap();
    let pool = BufPool::new(&driver, 4, 4096).unwrap();
    let (reader, mut writer) = io::pipe().unwrap();
    let mut data = ReadStream::new(&driver, reader.as_raw_fd(), &pool);
    let sent = pattern(10_000);
    writer.write_all(&sent).unwrap();
    drop(writer);

    let chunks: Vec<Vec<u8>> = block_on(
        &driver,
        StreamExt::collect(StreamExt::map(&mut data, |lease| lease.unwrap().to_vec())),
    );
    assert!(chunks.iter().all(|chunk| !chunk.is_empty()));
    assert_eq!(chunks.concat(), sent);
    assert!(data.is_terminated());
    assert!(block_on(&driver, StreamExt::next(&mut data)).is_none());
    assert_eq!(pool.available(), 4);
}

/// An accept stream yields the connections through `take`, and goes on
/// after: it never ends.
#[test]
fn an_accept_stream_yields_connections_through_the_trait() {
    let driver = Driver::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut accepted = AcceptStream::new(&driver, AcceptMulti::new(listener.as_raw_fd()));
    let clients: Vec<TcpStream> = (0..3)
        .map(|_| TcpStream::connect(listener.local_addr().unwrap()).unwrap())
        .collect();
    let servers: Vec<_> = block_on(
        &driver,
        StreamExt::collect(StreamExt::take(&mut accepted, 3)),
    );
    assert_eq!(servers.len(), 3);
    for server in &servers {
        assert!(!server.as_ref().unwrap().is_direct());
    }
    assert!(!accepted.is_terminated());
    let late = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let fourth = block_on(&driver, StreamExt::next(&mut accepted)).unwrap();
    fourth.unwrap();
    drop((clients, late, servers, accepted));
    settle(&driver);
}

/// A multishot request the kernel refuses yields its error and ends; the
/// stream is terminated until it is re-armed, which lets it be polled again.
#[test]
fn a_multishot_request_that_ends_is_terminated_until_rearmed() {
    let driver = Driver::new().unwrap();
    let bad: RawFd = -1;
    let mut stream = driver.stream(PollMulti::new(bad, libc::POLLIN as u32));
    assert!(!stream.is_terminated());
    let first = block_on(&driver, StreamExt::next(&mut stream)).expect("an item comes");
    assert_eq!(first.unwrap_err().raw_os_error(), Some(libc::EBADF));
    assert!(stream.is_terminated());
    assert!(block_on(&driver, StreamExt::next(&mut stream)).is_none());
    assert!(stream.is_terminated());

    stream.rearm();
    assert!(!stream.is_terminated());
    let again = block_on(&driver, StreamExt::next(&mut stream)).expect("an item comes");
    assert_eq!(again.unwrap_err().raw_os_error(), Some(libc::EBADF));
    assert!(stream.is_terminated());
}

/// Two streams merge through `select`, each item from the pipe it came
/// from, which the readiness streams of two pipes show.
#[test]
fn streams_merge_through_select() {
    let driver = Driver::new().unwrap();
    let (mut first_reader, mut first_writer) = io::pipe().unwrap();
    let (mut second_reader, mut second_writer) = io::pipe().unwrap();
    let first = PollStream::new(
        &driver,
        PollMulti::new(first_reader.as_raw_fd(), libc::POLLIN as u32),
    );
    let second = PollStream::new(
        &driver,
        PollMulti::new(second_reader.as_raw_fd(), libc::POLLIN as u32),
    );
    let mut merged = futures_util::stream::select(
        StreamExt::map(first, |events| (1, events)),
        StreamExt::map(second, |events| (2, events)),
    );
    first_writer.write_all(b"a").unwrap();
    let (from, events) = block_on(&driver, StreamExt::next(&mut merged)).unwrap();
    assert_eq!(from, 1);
    assert_ne!(events.unwrap() & libc::POLLIN as u32, 0);
    first_reader.read_exact(&mut [0]).unwrap();

    second_writer.write_all(b"b").unwrap();
    let (from, events) = block_on(&driver, StreamExt::next(&mut merged)).unwrap();
    assert_eq!(from, 2);
    assert_ne!(events.unwrap() & libc::POLLIN as u32, 0);
    second_reader.read_exact(&mut [0]).unwrap();
    assert!(!merged.is_terminated());
    drop(merged);
    settle(&driver);
}
