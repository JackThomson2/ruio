//! The owned-buffer read and write traits, and what is built on them.

mod support;

use std::cell::{Cell, RefCell};
use std::future::{Future, ready};
use std::io::{self, ErrorKind};
use std::pin::Pin;
use std::rc::Rc;
use std::time::Duration;

use ruio::buf::{BufResult, IoBuf, IoBufMut};
use ruio::fs::{File, OpenOptions};
use ruio::io::{AsyncRead, AsyncWrite, copy};
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
        let dir = std::env::temp_dir().join(format!("ruio-io-{name}-{}", std::process::id()));
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

/// Bytes that repeat with a long period, so a copy that drops or reorders
/// a chunk shows.
fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// A reader of a vector, in pieces of at most `chunk` bytes, with a boxed
/// future: a reader outside the crate, and short reads on demand.
struct Pieces {
    data: Vec<u8>,
    pos: Cell<usize>,
    chunk: usize,
    reads: Cell<usize>,
}

impl Pieces {
    fn new(data: Vec<u8>, chunk: usize) -> Self {
        Self {
            data,
            pos: Cell::new(0),
            chunk,
            reads: Cell::new(0),
        }
    }
}

impl AsyncRead for Pieces {
    type Read<'a, B: IoBufMut> = Pin<Box<dyn Future<Output = BufResult<usize, B>> + 'a>>;

    fn read<B: IoBufMut>(&self, mut buf: B) -> Self::Read<'_, B> {
        self.reads.set(self.reads.get() + 1);
        let pos = self.pos.get();
        let len = (self.data.len() - pos)
            .min(self.chunk)
            .min(buf.bytes_total());
        // SAFETY: the buffer has room for `bytes_total` bytes, of which `len`
        // are written here from the data, which has that many from `pos`.
        unsafe {
            buf.stable_mut_ptr()
                .copy_from_nonoverlapping(self.data[pos..].as_ptr(), len);
        }
        // SAFETY: the first `len` bytes were just written.
        unsafe { buf.set_init(len) };
        self.pos.set(pos + len);
        Box::pin(ready((Ok(len), buf)))
    }
}

/// A writer that keeps what it is given, taking at most `chunk` bytes a
/// write.
struct Sink {
    data: RefCell<Vec<u8>>,
    chunk: usize,
    writes: Cell<usize>,
}

impl Sink {
    fn new(chunk: usize) -> Self {
        Self {
            data: RefCell::new(Vec::new()),
            chunk,
            writes: Cell::new(0),
        }
    }
}

impl AsyncWrite for Sink {
    type Write<'a, B: IoBuf> = std::future::Ready<BufResult<usize, B>>;

    fn write<B: IoBuf>(&self, buf: B) -> Self::Write<'_, B> {
        self.writes.set(self.writes.get() + 1);
        let len = buf.bytes_init().min(self.chunk);
        // SAFETY: the buffer's first `bytes_init` bytes are initialized.
        let bytes = unsafe { std::slice::from_raw_parts(buf.stable_ptr(), len) };
        self.data.borrow_mut().extend_from_slice(bytes);
        ready((Ok(len), buf))
    }
}

/// Fills `buf` through the trait, however many reads it takes.
async fn fill<R: AsyncRead>(reader: &R, buf: Vec<u8>) -> BufResult<(), Vec<u8>> {
    reader.read_exact(buf).await
}

/// Writes all of `buf` through the trait, however many writes it takes.
async fn drain<W: AsyncWrite, B: IoBuf>(writer: &W, buf: B) -> BufResult<(), B> {
    writer.write_all(buf).await
}

#[test]
fn read_exact_takes_as_many_reads_as_it_needs() {
    let data = pattern(1000);
    let reader = Pieces::new(data.clone(), 64);
    ruio::Runtime::new().unwrap().block_on(async {
        let (read, buf) = fill(&reader, Vec::with_capacity(700)).await;
        read.unwrap();
        assert_eq!(buf, &data[..700]);
        assert_eq!(reader.reads.get(), 11, "700 bytes in pieces of 64");

        let (read, buf) = fill(&reader, Vec::new()).await;
        read.unwrap();
        assert!(buf.is_empty(), "a buffer with no room is full at once");
        assert_eq!(reader.reads.get(), 11);

        let (read, buf) = fill(&reader, Vec::with_capacity(400)).await;
        assert_eq!(read.unwrap_err().kind(), ErrorKind::UnexpectedEof);
        assert_eq!(buf, &data[700..], "what arrived is kept");
    });
}

#[test]
fn read_to_end_grows_the_vector_after_what_it_holds() {
    let data = pattern(100_000);
    let reader = Pieces::new(data.clone(), 7000);
    ruio::Runtime::new().unwrap().block_on(async {
        let (read, buf) = reader.read_to_end(b"head:".to_vec()).await;
        assert_eq!(read.unwrap(), data.len());
        assert_eq!(&buf[..5], b"head:");
        assert_eq!(&buf[5..], &data[..]);
        assert!(buf.capacity() >= buf.len());
    });
}

#[test]
fn write_all_takes_as_many_writes_as_it_needs() {
    let data = pattern(1000);
    let sink = Sink::new(300);
    ruio::Runtime::new().unwrap().block_on(async {
        let (written, buf) = drain(&sink, data.clone()).await;
        written.unwrap();
        assert_eq!(buf, data);
        assert_eq!(*sink.data.borrow(), data);
        assert_eq!(sink.writes.get(), 4, "1000 bytes in writes of 300");

        let (written, _) = drain(&sink, Vec::<u8>::new()).await;
        written.unwrap();
        assert_eq!(sink.writes.get(), 4, "nothing to write takes no write");

        let (written, _) = drain(&sink, b"tail".to_vec()).await;
        written.unwrap();
        assert_eq!(sink.data.borrow().len(), 1004);
    });
}

#[test]
fn write_all_reports_a_writer_that_takes_nothing() {
    let sink = Sink::new(0);
    ruio::Runtime::new().unwrap().block_on(async {
        let (written, buf) = drain(&sink, b"data".to_vec()).await;
        assert_eq!(written.unwrap_err().kind(), ErrorKind::WriteZero);
        assert_eq!(buf, b"data");
    });
}

#[test]
fn copy_moves_everything_in_order_through_short_reads_and_writes() {
    let data = pattern(1_000_000);
    let reader = Pieces::new(data.clone(), 50_000);
    let sink = Sink::new(30_000);
    ruio::Runtime::new().unwrap().block_on(async {
        let copied = copy(&reader, &sink).await.unwrap();
        assert_eq!(copied, data.len() as u64);
        assert_eq!(*sink.data.borrow(), data);
    });
}

#[test]
fn copy_of_nothing_is_nothing() {
    let reader = Pieces::new(Vec::new(), 100);
    let sink = Sink::new(100);
    ruio::Runtime::new().unwrap().block_on(async {
        assert_eq!(copy(&reader, &sink).await.unwrap(), 0);
        assert!(sink.data.borrow().is_empty());
        assert_eq!(sink.writes.get(), 0);
    });
}

#[test]
fn the_traits_reach_the_streams_own_operations() {
    ruio::Runtime::new().unwrap().block_on(async {
        let (client, server) = tcp_pair().await.unwrap();
        let data = pattern(200_000);
        let writer = ruio::spawn({
            let data = data.clone();
            async move {
                drain(&client, data).await.0.unwrap();
                client.shutdown(std::net::Shutdown::Write).await.unwrap();
                client
            }
        });
        let (read, buf) = within(fill(&server, Vec::with_capacity(200_000))).await;
        read.unwrap();
        assert_eq!(buf, data);
        let (read, buf) = within(fill(&server, Vec::with_capacity(1))).await;
        assert_eq!(read.unwrap_err().kind(), ErrorKind::UnexpectedEof);
        assert!(buf.is_empty());
        writer.await.unwrap();
    });
}

#[test]
fn read_to_end_reads_a_stream_until_the_peer_shuts_down() {
    ruio::Runtime::new().unwrap().block_on(async {
        let (client, server) = tcp_pair().await.unwrap();
        let data = pattern(300_000);
        let writer = ruio::spawn({
            let data = data.clone();
            async move {
                for chunk in data.chunks(70_000) {
                    client.write_all(chunk.to_vec()).await.0.unwrap();
                }
                client.shutdown(std::net::Shutdown::Write).await.unwrap();
                client
            }
        });
        let (read, buf) = within(AsyncRead::read_to_end(&server, Vec::new())).await;
        assert_eq!(read.unwrap(), data.len());
        assert_eq!(buf, data);
        writer.await.unwrap();
    });
}

#[test]
fn copy_relays_one_connection_to_another() {
    ruio::Runtime::new().unwrap().block_on(async {
        let (source, relay_in) = tcp_pair().await.unwrap();
        let (relay_out, sink) = tcp_pair().await.unwrap();
        let data = pattern(4_000_000);
        let feeder = ruio::spawn({
            let data = data.clone();
            async move {
                for chunk in data.chunks(100_000) {
                    source.write_all(chunk.to_vec()).await.0.unwrap();
                }
                source.shutdown(std::net::Shutdown::Write).await.unwrap();
                source
            }
        });
        let relay = ruio::spawn(async move {
            let copied = copy(&relay_in, &relay_out).await.unwrap();
            relay_out.shutdown(std::net::Shutdown::Write).await.unwrap();
            copied
        });
        let (read, buf) = within(AsyncRead::read_to_end(&sink, Vec::new())).await;
        assert_eq!(read.unwrap(), data.len());
        assert_eq!(buf, data);
        assert_eq!(within(relay).await.unwrap(), data.len() as u64);
        feeder.await.unwrap();
    });
}

#[test]
fn copy_reads_a_file_into_a_socket_and_a_socket_into_a_file() {
    let scratch = Scratch::new("copy");
    let data = pattern(1_500_000);
    std::fs::write(scratch.path("source"), &data).unwrap();
    ruio::Runtime::new().unwrap().block_on(async {
        let source = File::open(scratch.path("source")).await.unwrap();
        let target = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(scratch.path("target"))
            .await
            .unwrap();
        let (client, server) = tcp_pair().await.unwrap();
        let sender = ruio::spawn(async move {
            let copied = copy(&source, &client).await.unwrap();
            client.shutdown(std::net::Shutdown::Write).await.unwrap();
            copied
        });
        let copied = within(copy(&server, &target)).await.unwrap();
        assert_eq!(copied, data.len() as u64);
        assert_eq!(within(sender).await.unwrap(), data.len() as u64);
        target.sync_all().await.unwrap();
    });
    assert_eq!(std::fs::read(scratch.path("target")).unwrap(), data);
}

#[test]
fn copy_between_files_goes_at_their_positions() {
    let scratch = Scratch::new("copy-files");
    let data = pattern(300_000);
    std::fs::write(scratch.path("a"), &data).unwrap();
    ruio::Runtime::new().unwrap().block_on(async {
        let a = File::open(scratch.path("a")).await.unwrap();
        let b = File::create(scratch.path("b")).await.unwrap();
        // The first 1000 bytes read by hand move the position on.
        let (read, _) = a.read_exact(Vec::with_capacity(1000)).await;
        read.unwrap();
        assert_eq!(copy(&a, &b).await.unwrap(), data.len() as u64 - 1000);
        b.sync_all().await.unwrap();
    });
    assert_eq!(std::fs::read(scratch.path("b")).unwrap(), &data[1000..]);
}

#[test]
fn references_and_pointers_to_a_reader_or_writer_are_one() {
    ruio::Runtime::new().unwrap().block_on(async {
        let (client, server) = UnixStream::pair().unwrap();
        let client = Rc::new(client);
        let server = Box::new(server);
        let by_rc = Rc::clone(&client);
        let writer = ruio::spawn(async move {
            drain(&by_rc, b"through an Rc".to_vec()).await.0.unwrap();
            drain(&&*by_rc, b" and a reference".to_vec())
                .await
                .0
                .unwrap();
        });
        let (read, buf) = within(fill(&server, Vec::with_capacity(29))).await;
        read.unwrap();
        assert_eq!(buf, b"through an Rc and a reference");
        writer.await.unwrap();
        drop(client);
    });
}

/// A read exact on a stream dropped before the peer writes leaves nothing
/// behind, and the stream stays usable.
#[test]
fn a_dropped_read_exact_leaves_the_stream_usable() {
    ruio::Runtime::new().unwrap().block_on(async {
        let (client, server) = tcp_pair().await.unwrap();
        let read = fill(&server, Vec::with_capacity(16));
        let timed_out = ruio::time::timeout(Duration::from_millis(20), read).await;
        assert!(timed_out.is_err());
        drain(&client, b"after".to_vec()).await.0.unwrap();
        let (read, buf) = within(fill(&server, Vec::with_capacity(5))).await;
        read.unwrap();
        assert_eq!(buf, b"after");
    });
}
