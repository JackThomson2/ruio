//! The adapter for the portable file: tokio's and futures-io's traits for
//! reading, writing and seeking over `ruio::fs::portable::File`, as code
//! written for tokio's file uses them: a file made with `File::create`, or
//! with `OpenOptions` and `create_new`, and written with `write_all`; under a
//! ruio runtime, and from the workers of tokio's runtime, whose waits go to
//! a ruio runtime's ring set as the foreign one.

#![cfg(all(feature = "tokio", feature = "futures-io"))]

mod support;

use std::io::{self, ErrorKind, SeekFrom};
use std::path::PathBuf;
use std::sync::{OnceLock, mpsc};
use std::time::Duration;

use ruio::fs::OpenOptions;
use ruio::fs::portable::File;
use ruio_compat::PortableCompat;

use support::{pattern, within};

/// A directory of the test's own, removed when dropped.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "ruio-compat-portable-file-{name}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
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

#[test]
fn the_adapter_has_the_traits_of_a_file() {
    fn assert_traits<T>()
    where
        T: Send
            + Sync
            + Unpin
            + tokio::io::AsyncRead
            + tokio::io::AsyncWrite
            + tokio::io::AsyncSeek
            + futures_io::AsyncRead
            + futures_io::AsyncWrite
            + futures_io::AsyncSeek,
    {
    }
    assert_traits::<PortableCompat<File>>();
}

/// A file made with `File::create`, and one with `create_new`, written
/// through tokio's traits; `create_new` refuses a file that is there.
#[ruio::test]
async fn files_are_made_and_written_through_tokios_traits() -> io::Result<()> {
    use tokio::io::AsyncWriteExt;

    let scratch = Scratch::new("tokio-write");
    let path = scratch.path("created");
    let data = pattern(3 << 20, 1);
    let mut file = PortableCompat::new(File::create(&path).await?);
    within(file.write_all(&data)).await?;
    within(file.write_all(b"tail")).await?;
    within(file.flush()).await?;
    let mut expected = data.clone();
    expected.extend_from_slice(b"tail");
    assert!(std::fs::read(&path)? == expected);
    within(file.shutdown()).await?;

    let path = scratch.path("new");
    let opened = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open_portable(&path)
        .await?;
    let mut file = PortableCompat::new(opened);
    within(file.write_all(b"only once")).await?;
    within(file.shutdown()).await?;
    assert_eq!(std::fs::read(&path)?, b"only once");
    let err = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open_portable(&path)
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::AlreadyExists);
    Ok(())
}

/// tokio's traits read a file to its end, seek in it, and read lines of it
/// through a buffered reader.
#[ruio::test]
async fn tokios_traits_read_and_seek_a_file() -> io::Result<()> {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader};

    let scratch = Scratch::new("tokio-read");
    let path = scratch.path("file");
    let data = pattern(100_000, 2);
    std::fs::write(&path, &data)?;
    let mut file = PortableCompat::new(File::open(&path).await?);
    let mut read = Vec::new();
    within(file.read_to_end(&mut read)).await?;
    assert!(read == data);
    assert_eq!(within(file.seek(SeekFrom::Start(5))).await?, 5);
    let mut four = [0; 4];
    within(file.read_exact(&mut four)).await?;
    assert_eq!(four, data[5..9]);
    assert_eq!(within(file.seek(SeekFrom::Current(-2))).await?, 7);
    assert_eq!(within(file.seek(SeekFrom::End(-10))).await?, 99_990);
    let mut end = Vec::new();
    within(file.read_to_end(&mut end)).await?;
    assert_eq!(end, data[99_990..]);

    let path = scratch.path("lines");
    std::fs::write(&path, b"one\ntwo\nthree")?;
    let mut lines = BufReader::new(PortableCompat::new(File::open(&path).await?));
    let mut line = Vec::new();
    within(lines.read_until(b'\n', &mut line)).await?;
    assert_eq!(line, b"one\n");
    line.clear();
    within(lines.read_until(b'\n', &mut line)).await?;
    assert_eq!(line, b"two\n");
    line.clear();
    within(lines.read_until(b'\n', &mut line)).await?;
    assert_eq!(line, b"three");
    Ok(())
}

/// futures-io's traits write a file, read it back, and seek in it.
#[ruio::test]
async fn futures_io_traits_write_read_and_seek_a_file() -> io::Result<()> {
    use futures_util::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

    let scratch = Scratch::new("futures");
    let path = scratch.path("file");
    let opened = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open_portable(&path)
        .await?;
    let mut file = PortableCompat::new(opened);
    within(file.write_all(b"through futures-io")).await?;
    within(file.flush()).await?;
    assert_eq!(within(file.seek(SeekFrom::Start(8))).await?, 8);
    let mut rest = Vec::new();
    within(file.read_to_end(&mut rest)).await?;
    assert_eq!(rest, b"futures-io");
    within(file.close()).await?;
    assert_eq!(std::fs::read(&path)?, b"through futures-io");
    Ok(())
}

/// Starts, once, a ruio runtime on a thread of its own, which runs for as
/// long as the tests do, and makes its ring the foreign one, which the
/// portable operations of tokio's workers go to.
fn foreign() {
    static STARTED: OnceLock<()> = OnceLock::new();
    STARTED.get_or_init(|| {
        let (send, receive) = mpsc::channel();
        std::thread::spawn(move || {
            ruio::Runtime::new().unwrap().block_on(async move {
                send.send(ruio::handle().ring().clone()).unwrap();
                std::future::pending::<()>().await;
            });
        });
        ruio::uring::set_foreign_ring(&receive.recv().unwrap());
    });
}

/// Code written for tokio's file runs on a tokio worker with ruio's: made
/// with `File::create` and with `create_new`, written, synced, and read back,
/// each wait on the foreign ring.
#[test]
fn a_file_is_written_from_a_tokio_worker() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    foreign();
    let scratch = Scratch::new("tokio-worker");
    let created = scratch.path("created");
    let new = scratch.path("new");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_time()
        .build()
        .unwrap();
    let task = runtime.spawn(async move {
        let data = pattern(1 << 20, 3);
        let mut file = PortableCompat::new(File::create(&created).await?);
        file.write_all(&data).await?;
        file.shutdown().await?;
        let opened = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open_portable(&new)
            .await?;
        let mut file = PortableCompat::new(opened);
        file.write_all(b"from a tokio worker").await?;
        file.flush().await?;
        file.get_ref().sync_all().await?;
        let mut back = Vec::new();
        PortableCompat::new(File::open(&created).await?)
            .read_to_end(&mut back)
            .await?;
        assert!(back == data);
        io::Result::Ok(())
    });
    runtime
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(10), task)
                .await
                .expect("finished in time")
        })
        .unwrap()
        .unwrap();
    assert_eq!(
        std::fs::read(scratch.path("new")).unwrap(),
        b"from a tokio worker"
    );
}
