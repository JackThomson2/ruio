//! What the I/O completion port engine does in its own way, which the
//! tests that the engines share cannot name: a file's stream into a pool
//! reads a file on a disk, from the file's position on, where io_uring's is
//! for the files that data arrives on; and listeners bound to one address
//! with `bind_reuseport` share one socket of the process's.

#![cfg(ruio_iocp)]

mod support;

use std::io;
use std::path::PathBuf;
use std::time::Duration;

use ruio::buf::BufPool;
use ruio::fs::{self, File};
use ruio::net::TcpListener;

use support::within;

/// A directory of the test's own in the temporary directory, removed with
/// what is in it when dropped.
struct Scratch(PathBuf);

impl Scratch {
    fn new(test: &str) -> Self {
        let name = format!("ruio-iocp-{test}-{}", std::process::id());
        let dir = std::env::temp_dir().join(name);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
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

#[ruio::test]
async fn a_files_stream_starts_at_the_position_and_moves_it_on() -> io::Result<()> {
    let scratch = Scratch::new("position");
    let path = scratch.path("file");
    let data = pattern(10_000);
    within(fs::write(&path, data.clone())).await?;
    let file = within(File::open(&path)).await?;
    let pool = BufPool::new(2, 4096)?;

    let (read, head) = within(file.read(Vec::with_capacity(100))).await;
    assert_eq!(read?, 100);
    let mut stream = file.read_stream(&pool);
    let first = within(stream.next()).await.expect("the file's data")?;
    assert_eq!(first.len(), 4096);
    assert!(
        *first == data[100..4196],
        "the stream starts at the position"
    );
    drop(first);
    drop(stream);

    // A read at the position carries on where the stream left off, and a
    // stream where the read did.
    let (read, middle) = within(file.read(Vec::with_capacity(4))).await;
    assert_eq!(read?, 4);
    assert_eq!(middle, data[4196..4200]);
    let mut stream = file.read_stream(&pool);
    assert!(!stream.is_ended());
    let mut rest = Vec::new();
    while let Some(chunk) = within(stream.next()).await {
        rest.extend_from_slice(&chunk?);
    }
    assert!(stream.is_ended());
    assert!(stream.next_ready().is_none());
    assert!(
        rest == data[4200..],
        "the stream reads to the end of the file"
    );
    drop(stream);
    assert_eq!(head, data[..100]);

    // The position is at the end of the file.
    let (read, _) = within(file.read(Vec::with_capacity(8))).await;
    assert_eq!(read?, 0);
    assert_eq!(pool.available(), 2);
    Ok(())
}

#[ruio::test]
async fn a_files_stream_waits_for_a_buffer_of_a_pool_that_is_all_leased() -> io::Result<()> {
    let scratch = Scratch::new("leased");
    let path = scratch.path("file");
    within(fs::write(&path, b"0123456789".to_vec())).await?;
    let file = within(File::open(&path)).await?;
    let pool = BufPool::new(1, 4)?;
    let mut stream = file.read_stream(&pool);
    let first = within(stream.next()).await.expect("the file's start")?;
    assert_eq!(&*first, b"0123");
    // The one buffer is leased, so the next read does not start.
    let waited = ruio::time::timeout(Duration::from_millis(20), stream.next()).await;
    assert!(waited.is_err());
    assert!(stream.next_ready().is_none());
    drop(first);
    let second = within(stream.next()).await.expect("the file's middle")?;
    assert_eq!(&*second, b"4567");
    drop(second);
    let last = within(stream.next()).await.expect("the file's end")?;
    assert_eq!(&*last, b"89");
    drop(last);
    assert!(within(stream.next()).await.is_none());
    Ok(())
}

/// A file's data is a `Stream` of futures-core.
#[cfg(feature = "futures")]
#[ruio::test]
async fn a_files_data_is_a_stream_of_leases() -> io::Result<()> {
    use futures_util::StreamExt;
    use futures_util::stream::FusedStream;

    let scratch = Scratch::new("stream");
    let path = scratch.path("file");
    let data = pattern(50_000);
    within(fs::write(&path, data.clone())).await?;
    let file = within(File::open(&path)).await?;
    let pool = BufPool::new(8, 4096)?;
    let mut stream = file.read_stream(&pool);
    assert!(!stream.is_terminated());
    let chunks: Vec<Vec<u8>> = within(StreamExt::collect(StreamExt::map(&mut stream, |lease| {
        lease.unwrap().to_vec()
    })))
    .await;
    assert!(chunks.concat() == data, "what was read is the file");
    assert!(stream.is_terminated());
    Ok(())
}

/// Listeners bound to one address with `bind_reuseport` share one socket,
/// which closes once the last of them is dropped; a port for the system to
/// choose is a new one each time, and a listener bound with `bind` shares
/// its address with none.
#[ruio::test]
async fn listeners_bound_with_reuseport_share_a_socket_until_the_last_goes() -> io::Result<()> {
    let first = TcpListener::bind_reuseport("127.0.0.1:0")?;
    let addr = first.local_addr()?;
    let other = TcpListener::bind_reuseport("127.0.0.1:0")?;
    assert_ne!(other.local_addr()?, addr);
    let second = TcpListener::bind_reuseport(addr)?;
    let err = TcpListener::bind(addr).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::AddrInUse, "{err}");
    // The second holds the socket, which a third takes too.
    drop(first);
    let err = TcpListener::bind(addr).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::AddrInUse, "{err}");
    let third = TcpListener::bind_reuseport(addr)?;
    assert!(format!("{third:?}").starts_with("SharedListener"));
    drop((second, third));
    // The socket closed with the last of its listeners.
    let plain = TcpListener::bind(addr)?;
    let err = TcpListener::bind_reuseport(addr).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::AddrInUse, "{err}");
    drop((plain, other));
    Ok(())
}
