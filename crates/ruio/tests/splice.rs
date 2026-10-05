//! Moving bytes between descriptors through a pipe, without a buffer in
//! user space.

mod support;

use std::io::{self, ErrorKind};
use std::net::Shutdown;
use std::time::Duration;

use ruio::fs::{File, OpenOptions};
use ruio::io::{AsyncRead, splice};
use ruio::net::{TcpListener, TcpStream, UnixStream};

use support::within;

/// A connected pair of TCP streams on the loopback interface.
async fn tcp_pair() -> io::Result<(TcpStream, TcpStream)> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let client = ruio::spawn(TcpStream::connect(addr));
    let (server, _) = within(listener.accept()).await?;
    Ok((client.await.unwrap()?, server))
}

/// A directory of the test's own, removed when dropped.
struct Scratch(std::path::PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("ruio-splice-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    fn path(&self, name: &str) -> std::path::PathBuf {
        self.0.join(name)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Bytes that repeat with a long period, so a splice that drops or reorders
/// a chunk shows.
fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// Writes `data` to `stream` in pieces, then shuts its writing half.
fn feed(stream: TcpStream, data: Vec<u8>, piece: usize) -> ruio::JoinHandle<()> {
    ruio::spawn(async move {
        for chunk in data.chunks(piece) {
            stream.write_all(chunk.to_vec()).await.0.unwrap();
        }
        stream.shutdown(Shutdown::Write).await.unwrap();
    })
}

#[test]
fn a_splice_relays_one_connection_to_another() {
    ruio::Runtime::new().unwrap().block_on(async {
        let (source, relay_in) = tcp_pair().await.unwrap();
        let (relay_out, sink) = tcp_pair().await.unwrap();
        let data = pattern(8_000_000);
        let feeder = feed(source, data.clone(), 100_000);
        let relay = ruio::spawn(async move {
            let moved = splice(&relay_in, &relay_out).await.unwrap();
            relay_out.shutdown(Shutdown::Write).await.unwrap();
            moved
        });
        let (read, buf) = within(AsyncRead::read_to_end(&sink, Vec::new())).await;
        assert_eq!(read.unwrap(), data.len());
        assert_eq!(buf, data);
        assert_eq!(within(relay).await.unwrap(), data.len() as u64);
        feeder.await.unwrap();
    });
}

#[test]
fn a_splice_relays_small_pieces_that_fill_the_pipes_slots() {
    ruio::Runtime::new().unwrap().block_on(async {
        let (source, relay_in) = tcp_pair().await.unwrap();
        let (relay_out, sink) = tcp_pair().await.unwrap();
        source.set_nodelay(true).unwrap();
        let data = pattern(600_000);
        let feeder = feed(source, data.clone(), 100);
        let relay = ruio::spawn(async move {
            let moved = splice(&relay_in, &relay_out).await.unwrap();
            relay_out.shutdown(Shutdown::Write).await.unwrap();
            moved
        });
        let (read, buf) = within(AsyncRead::read_to_end(&sink, Vec::new())).await;
        assert_eq!(read.unwrap(), data.len());
        assert_eq!(buf, data);
        assert_eq!(within(relay).await.unwrap(), data.len() as u64);
        feeder.await.unwrap();
    });
}

#[test]
fn a_file_splices_into_a_socket_from_its_position_or_an_offset() {
    let scratch = Scratch::new("sendfile");
    let data = pattern(3_000_000);
    std::fs::write(scratch.path("source"), &data).unwrap();
    ruio::Runtime::new().unwrap().block_on(async {
        let file = File::open(scratch.path("source")).await.unwrap();
        let (client, server) = tcp_pair().await.unwrap();
        let sender = ruio::spawn(async move {
            let moved = splice(&file, &client).await.unwrap();
            client.shutdown(Shutdown::Write).await.unwrap();
            moved
        });
        let (read, buf) = within(AsyncRead::read_to_end(&server, Vec::new())).await;
        assert_eq!(read.unwrap(), data.len());
        assert_eq!(buf, data);
        assert_eq!(within(sender).await.unwrap(), data.len() as u64);

        // A range of the file, with the position left alone.
        let file = File::open(scratch.path("source")).await.unwrap();
        let (client, server) = tcp_pair().await.unwrap();
        let sender = ruio::spawn(async move {
            let moved = splice(&file, &client)
                .offset_in(1_000_000)
                .limit(500_000)
                .await
                .unwrap();
            client.shutdown(Shutdown::Write).await.unwrap();
            let (read, head) = file.read(Vec::with_capacity(4)).await;
            read.unwrap();
            (moved, head)
        });
        let (read, buf) = within(AsyncRead::read_to_end(&server, Vec::new())).await;
        assert_eq!(read.unwrap(), 500_000);
        assert_eq!(buf, &data[1_000_000..1_500_000]);
        let (moved, head) = within(sender).await.unwrap();
        assert_eq!(moved, 500_000);
        assert_eq!(head, &data[..4], "the file's position did not move");
    });
}

#[test]
fn a_socket_splices_into_a_file_at_its_position_or_an_offset() {
    let scratch = Scratch::new("receive-file");
    let data = pattern(2_000_000);
    ruio::Runtime::new().unwrap().block_on(async {
        let target = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(scratch.path("target"))
            .await
            .unwrap();
        let (source, receiver) = tcp_pair().await.unwrap();
        let feeder = feed(source, data.clone(), 65_536);
        let moved = within(splice(&receiver, &target)).await.unwrap();
        assert_eq!(moved, data.len() as u64);
        feeder.await.unwrap();

        let (source, receiver) = tcp_pair().await.unwrap();
        let feeder = feed(source, b"tail".to_vec(), 4);
        let moved = within(splice(&receiver, &target).offset_out(data.len() as u64 + 100))
            .await
            .unwrap();
        assert_eq!(moved, 4);
        feeder.await.unwrap();
        target.sync_all().await.unwrap();
    });
    let written = std::fs::read(scratch.path("target")).unwrap();
    assert_eq!(&written[..data.len()], &data[..]);
    assert_eq!(&written[data.len()..data.len() + 100], &[0; 100]);
    assert_eq!(&written[data.len() + 100..], b"tail");
}

#[test]
fn a_limit_leaves_the_rest_in_the_source() {
    ruio::Runtime::new().unwrap().block_on(async {
        let (source, relay_in) = tcp_pair().await.unwrap();
        let (relay_out, sink) = tcp_pair().await.unwrap();
        let data = pattern(200_000);
        let feeder = feed(source, data.clone(), 50_000);
        let moved = within(splice(&relay_in, &relay_out).limit(120_000))
            .await
            .unwrap();
        assert_eq!(moved, 120_000);
        relay_out.shutdown(Shutdown::Write).await.unwrap();
        let (read, buf) = within(AsyncRead::read_to_end(&sink, Vec::new())).await;
        assert_eq!(read.unwrap(), 120_000);
        assert_eq!(buf, &data[..120_000]);
        let (read, rest) = within(AsyncRead::read_to_end(&relay_in, Vec::new())).await;
        assert_eq!(read.unwrap(), 80_000);
        assert_eq!(rest, &data[120_000..]);
        feeder.await.unwrap();

        assert_eq!(
            within(splice(&relay_in, &relay_out).limit(0))
                .await
                .unwrap(),
            0
        );
    });
}

#[test]
fn a_source_that_ends_at_once_moves_nothing() {
    ruio::Runtime::new().unwrap().block_on(async {
        let (source, relay_in) = tcp_pair().await.unwrap();
        let (relay_out, _sink) = tcp_pair().await.unwrap();
        source.shutdown(Shutdown::Write).await.unwrap();
        assert_eq!(within(splice(&relay_in, &relay_out)).await.unwrap(), 0);
    });
    let scratch = Scratch::new("empty");
    std::fs::write(scratch.path("empty"), b"").unwrap();
    ruio::Runtime::new().unwrap().block_on(async {
        let file = File::open(scratch.path("empty")).await.unwrap();
        let (client, _server) = tcp_pair().await.unwrap();
        assert_eq!(within(splice(&file, &client)).await.unwrap(), 0);
    });
}

#[test]
fn two_splices_relay_both_ways_at_once() {
    ruio::Runtime::new().unwrap().block_on(async {
        let (a, relay_a) = tcp_pair().await.unwrap();
        let (relay_b, b) = tcp_pair().await.unwrap();
        let forward = pattern(3_000_000);
        let back: Vec<u8> = forward.iter().rev().copied().collect();
        let (a, b) = (std::rc::Rc::new(a), std::rc::Rc::new(b));
        // Each end writes its data and reads the other's in tasks of their
        // own, so neither waits on the other.
        let write_a = ruio::spawn({
            let (a, forward) = (a.clone(), forward.clone());
            async move {
                a.write_all(forward).await.0.unwrap();
                a.shutdown(Shutdown::Write).await.unwrap();
            }
        });
        let write_b = ruio::spawn({
            let (b, back) = (b.clone(), back.clone());
            async move {
                b.write_all(back).await.0.unwrap();
                b.shutdown(Shutdown::Write).await.unwrap();
            }
        });
        let read_a = ruio::spawn({
            let a = a.clone();
            async move { AsyncRead::read_to_end(&a, Vec::new()).await }
        });
        let read_b = ruio::spawn({
            let b = b.clone();
            async move { AsyncRead::read_to_end(&b, Vec::new()).await }
        });
        let relay_a = std::rc::Rc::new(relay_a);
        let relay_b = std::rc::Rc::new(relay_b);
        let there = ruio::spawn({
            let (relay_a, relay_b) = (relay_a.clone(), relay_b.clone());
            async move {
                let moved = splice(&relay_a, &relay_b).await.unwrap();
                relay_b.shutdown(Shutdown::Write).await.unwrap();
                moved
            }
        });
        let and_back = ruio::spawn({
            let (relay_a, relay_b) = (relay_a.clone(), relay_b.clone());
            async move {
                let moved = splice(&relay_b, &relay_a).await.unwrap();
                relay_a.shutdown(Shutdown::Write).await.unwrap();
                moved
            }
        });
        assert_eq!(within(there).await.unwrap(), forward.len() as u64);
        assert_eq!(within(and_back).await.unwrap(), back.len() as u64);
        let (read, at_b) = within(read_b).await.unwrap();
        assert_eq!(read.unwrap(), forward.len());
        assert_eq!(at_b, forward);
        let (read, at_a) = within(read_a).await.unwrap();
        assert_eq!(read.unwrap(), back.len());
        assert_eq!(at_a, back);
        write_a.await.unwrap();
        write_b.await.unwrap();
    });
}

/// A splice dropped while it waits for a source leaves the runtime, the
/// streams and the thread's pipes usable.
#[test]
fn a_splice_dropped_while_waiting_leaves_everything_usable() {
    ruio::Runtime::new().unwrap().block_on(async {
        let (source, relay_in) = tcp_pair().await.unwrap();
        let (relay_out, sink) = tcp_pair().await.unwrap();
        let waiting = ruio::time::timeout(Duration::from_millis(20), splice(&relay_in, &relay_out));
        assert!(waiting.await.is_err(), "nothing to move yet");
        let data = pattern(100_000);
        let feeder = feed(source, data.clone(), 10_000);
        let moved = within(splice(&relay_in, &relay_out)).await.unwrap();
        assert_eq!(moved, data.len() as u64);
        relay_out.shutdown(Shutdown::Write).await.unwrap();
        let (read, buf) = within(AsyncRead::read_to_end(&sink, Vec::new())).await;
        assert_eq!(read.unwrap(), data.len());
        assert_eq!(buf, data);
        feeder.await.unwrap();
    });
}

/// Splices one after another reuse the thread's pipes, and each moves its
/// own bytes.
#[test]
fn splices_one_after_another_reuse_the_pipe() {
    let scratch = Scratch::new("reuse");
    let data = pattern(10_000);
    std::fs::write(scratch.path("source"), &data).unwrap();
    ruio::Runtime::new().unwrap().block_on(async {
        let file = File::open(scratch.path("source")).await.unwrap();
        let (client, server) = tcp_pair().await.unwrap();
        for round in 0..200u64 {
            let offset = (round * 37) % 9_000;
            let moved = within(splice(&file, &client).offset_in(offset).limit(1000))
                .await
                .unwrap();
            assert_eq!(moved, 1000);
            let (read, buf) = within(server.read_exact(Vec::with_capacity(1000))).await;
            read.unwrap();
            assert_eq!(buf, &data[offset as usize..offset as usize + 1000]);
        }
    });
}

#[test]
fn unix_streams_splice_too() {
    ruio::Runtime::new().unwrap().block_on(async {
        let (a, relay_in) = UnixStream::pair().unwrap();
        let (relay_out, b) = UnixStream::pair().unwrap();
        let data = pattern(1_000_000);
        let feeder = ruio::spawn({
            let data = data.clone();
            async move {
                a.write_all(data).await.0.unwrap();
                a.shutdown(Shutdown::Write).await.unwrap();
            }
        });
        let relay = ruio::spawn(async move {
            let moved = splice(&relay_in, &relay_out).await.unwrap();
            relay_out.shutdown(Shutdown::Write).await.unwrap();
            moved
        });
        let (read, buf) = within(AsyncRead::read_to_end(&b, Vec::new())).await;
        assert_eq!(read.unwrap(), data.len());
        assert_eq!(buf, data);
        assert_eq!(within(relay).await.unwrap(), data.len() as u64);
        feeder.await.unwrap();
    });
}

#[test]
fn an_offset_on_a_socket_is_refused() {
    ruio::Runtime::new().unwrap().block_on(async {
        let (source, relay_in) = tcp_pair().await.unwrap();
        let (relay_out, _sink) = tcp_pair().await.unwrap();
        let feeder = feed(source, b"data".to_vec(), 4);
        let err = within(splice(&relay_in, &relay_out).offset_in(0))
            .await
            .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EINVAL));
        assert_ne!(err.kind(), ErrorKind::WouldBlock);
        feeder.await.unwrap();
    });
}
