//! The streams of connections, data, directory entries, readiness and
//! signals through the `Stream` and `FusedStream` traits of futures-core, and
//! futures-util's combinators over them.

#![cfg(feature = "futures")]

mod support;

use std::io::{self, Write as _};
use std::net::Shutdown;
use std::os::fd::OwnedFd;
use std::time::Duration;

use futures_util::StreamExt;
use futures_util::stream::FusedStream;
use ruio::buf::BufPool;
use ruio::fs::File;
use ruio::io::AsyncFd;
use ruio::net::{TcpListener, TcpStream};
use ruio::signal::{SignalKind, signal};

use support::within;

/// A directory of the test's own, removed when dropped.
struct Scratch(std::path::PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("ruio-stream-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// `len` bytes that repeat only every 251.
fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|index| (index % 251) as u8).collect()
}

/// Connections come through `take` and `filter_map`, and the stream goes
/// on after: it never ends.
#[ruio::test]
async fn incoming_yields_connections_through_combinators() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let mut incoming = listener.incoming();
    assert!(!incoming.is_terminated());
    let clients = ruio::spawn(async move {
        let mut clients = Vec::new();
        for _ in 0..4 {
            clients.push(TcpStream::connect(addr).await.unwrap());
        }
        clients
    });
    let accepted: Vec<TcpStream> = within(StreamExt::collect(StreamExt::take(
        StreamExt::filter_map(&mut incoming, |stream| async { stream.ok() }),
        3,
    )))
    .await;
    assert_eq!(accepted.len(), 3);
    let fourth = within(StreamExt::next(&mut incoming)).await.unwrap()?;
    assert!(fourth.peer_addr().is_ok());
    assert!(!incoming.is_terminated());
    drop(clients.await.unwrap());
    Ok(())
}

/// A connection's data streams through `map` and `fold` into a count, and
/// the stream is terminated once the peer shuts down.
#[ruio::test]
async fn recv_stream_folds_a_connections_data() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let sent = pattern(300_000);
    let sender = ruio::spawn({
        let sent = sent.clone();
        async move {
            let client = TcpStream::connect(addr).await.unwrap();
            client.write_all(sent).await.0.unwrap();
            client.shutdown(Shutdown::Write).await.unwrap();
            client
        }
    });
    let (server, _) = within(listener.accept()).await?;
    let pool = BufPool::new(&ruio::driver(), 16, 16 << 10)?;
    let mut received = server.recv_stream(&pool);
    assert!(!received.is_terminated());
    let (bytes, checksum) = within(StreamExt::fold(
        &mut received,
        (0usize, 0u64),
        |(bytes, checksum), bundle| async move {
            let bundle = bundle.unwrap();
            let mut checksum = checksum;
            for lease in &bundle {
                for &byte in lease.iter() {
                    checksum = checksum.wrapping_mul(31).wrapping_add(u64::from(byte));
                }
            }
            (bytes + bundle.len(), checksum)
        },
    ))
    .await;
    assert_eq!(bytes, sent.len());
    let expected = sent.iter().fold(0u64, |sum, &byte| {
        sum.wrapping_mul(31).wrapping_add(u64::from(byte))
    });
    assert_eq!(checksum, expected);
    assert!(received.is_terminated());
    drop(sender.await.unwrap());
    Ok(())
}

/// A pipe's data streams from a file to its end.
#[ruio::test]
async fn read_stream_collects_a_pipe_to_its_end() -> io::Result<()> {
    let (reader, mut writer) = io::pipe()?;
    let reader = File::from_std(OwnedFd::from(reader).into());
    let pool = BufPool::new(&ruio::driver(), 8, 4096)?;
    let mut data = reader.read_stream(&pool);
    let sent = pattern(50_000);
    let feeder = std::thread::spawn({
        let sent = sent.clone();
        move || {
            writer.write_all(&sent).unwrap();
            drop(writer);
        }
    });
    let chunks: Vec<Vec<u8>> = within(StreamExt::collect(StreamExt::map(&mut data, |lease| {
        lease.unwrap().to_vec()
    })))
    .await;
    assert_eq!(chunks.concat(), sent);
    assert!(data.is_terminated());
    feeder.join().unwrap();
    Ok(())
}

/// A directory's entries stream, sorted by name after; the stream is
/// terminated once they are all taken, and yields between batches as the
/// reader does.
#[ruio::test]
async fn read_dir_streams_its_entries() -> io::Result<()> {
    const COUNT: usize = 2500;
    let scratch = Scratch::new("read-dir");
    for index in 0..COUNT {
        std::fs::File::create(scratch.0.join(format!("entry-{index:05}")))?;
    }
    let mut entries = within(ruio::fs::read_dir(&scratch.0)).await?;
    assert!(!entries.is_terminated(), "the first batch is being read");
    let bystander = ruio::spawn(async { 1 });
    let mut names: Vec<String> =
        within(StreamExt::collect(StreamExt::map(&mut entries, |entry| {
            entry.unwrap().file_name().to_string_lossy().into_owned()
        })))
        .await;
    assert!(entries.is_terminated());
    assert!(within(StreamExt::next(&mut entries)).await.is_none());
    names.sort();
    assert_eq!(names.len(), COUNT);
    assert_eq!(names[0], "entry-00000");
    assert_eq!(names[COUNT - 1], format!("entry-{:05}", COUNT - 1));
    assert_eq!(bystander.await.unwrap(), 1, "the other task ran meanwhile");
    Ok(())
}

/// A descriptor's readiness streams; each item an event, through `take`.
#[ruio::test]
async fn readiness_streams_each_wakeup() -> io::Result<()> {
    let (reader, mut writer) = io::pipe()?;
    let reader = AsyncFd::new(OwnedFd::from(reader));
    let mut readiness = reader.readiness(libc::POLLIN as u32);
    assert!(!readiness.is_terminated());
    let writes = std::thread::spawn(move || {
        for byte in 0..3u8 {
            writer.write_all(&[byte]).unwrap();
            std::thread::sleep(Duration::from_millis(5));
        }
        writer
    });
    let mut seen = 0;
    let mut events = StreamExt::take(&mut readiness, 3);
    while let Some(event) = within(StreamExt::next(&mut events)).await {
        assert_ne!(event? & libc::POLLIN as u32, 0);
        seen += 1;
        // Taken out, so the next wakeup is for the next byte.
        let (read, _) = ruio::io::AsyncRead::read(&reader, Vec::with_capacity(16)).await;
        read?;
    }
    assert_eq!(seen, 3);
    assert!(!readiness.is_terminated());
    drop(writes.join().unwrap());
    Ok(())
}

/// A server's connections merge with its shutdown signal through `select`:
/// the connections are served until the signal comes, which ends the loop.
#[ruio::test]
async fn incoming_merges_with_a_signal_through_select() -> io::Result<()> {
    enum Event {
        Connection(io::Result<TcpStream>),
        Shutdown,
    }

    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let incoming = StreamExt::map(listener.incoming(), Event::Connection);
    let shutdown = StreamExt::map(signal(SignalKind::user_defined1())?, |()| Event::Shutdown);
    let mut events = futures_util::stream::select(incoming, shutdown);
    assert!(!events.is_terminated());

    let clients = ruio::spawn(async move {
        let mut clients = Vec::new();
        for _ in 0..3 {
            clients.push(TcpStream::connect(addr).await.unwrap());
        }
        clients
    });
    let mut served = 0;
    while let Event::Connection(stream) = within(StreamExt::next(&mut events)).await.unwrap() {
        stream?;
        served += 1;
        if served == 3 {
            // SAFETY: `getpid` touches nothing.
            let pid = unsafe { libc::getpid() };
            // SAFETY: `kill` of the process itself, with a signal the test
            // has a handler for.
            assert_eq!(unsafe { libc::kill(pid, libc::SIGUSR1) }, 0);
        }
    }
    assert_eq!(served, 3);
    drop(clients.await.unwrap());
    Ok(())
}
