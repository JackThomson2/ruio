//! The adapters over files and other descriptors, through the traits of
//! `tokio::io`.

#![cfg(feature = "tokio")]

mod support;

use std::io::{self, Read as _, Write as _};
use std::os::fd::{AsRawFd, OwnedFd};
use std::thread;

use ruio::fs::{File, OpenOptions};
use ruio::io::AsyncFd;
use ruio_compat::Compat;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

use support::{pattern, within};

/// A directory of the test's own, removed when dropped.
struct Scratch(std::path::PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("ruio-compat-{name}-{}", std::process::id()));
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

/// A file is read from its position through the adapter, in reads of the
/// adapter's buffer size, to its end.
#[ruio::test]
async fn a_file_is_read_to_its_end() -> io::Result<()> {
    let scratch = Scratch::new("read");
    let data = pattern(100_000, 3);
    std::fs::write(scratch.path("data"), &data)?;
    let file = File::open(scratch.path("data")).await?;
    let mut file = Compat::with_capacity(4096, file);
    let mut read = Vec::new();
    within(file.read_to_end(&mut read)).await?;
    assert!(read == data);
    assert_eq!(within(file.read(&mut [0; 16])).await?, 0, "it stays ended");
    Ok(())
}

/// Lines are read through `AsyncBufRead`, which lends the adapter's buffer
/// as it is.
#[ruio::test]
async fn a_file_is_read_by_lines() -> io::Result<()> {
    let scratch = Scratch::new("lines");
    std::fs::write(scratch.path("lines"), b"first\nsecond\n\nfourth")?;
    let file = Compat::with_capacity(8, File::open(scratch.path("lines")).await?);
    let mut lines = file.lines();
    let mut read = Vec::new();
    while let Some(line) = within(lines.next_line()).await? {
        read.push(line);
    }
    assert_eq!(read, ["first", "second", "", "fourth"]);
    Ok(())
}

/// Writes go to the file at its position, each behind the last, and a
/// shutdown waits for the last; the file is then whole.
#[ruio::test]
async fn a_file_is_written_at_its_position() -> io::Result<()> {
    let scratch = Scratch::new("write");
    let data = pattern(1 << 20, 11);
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(scratch.path("out"))
        .await?;
    let mut file = Compat::new(file);
    for chunk in data.chunks(70_000) {
        within(file.write_all(chunk)).await?;
    }
    within(file.flush()).await?;
    within(file.shutdown()).await?;
    // A file has no half to shut: it takes more after a shutdown.
    within(file.write_all(b"tail")).await?;
    within(file.shutdown()).await?;
    let file = file.into_inner();
    file.sync_all().await?;
    drop(file);
    let written = std::fs::read(scratch.path("out"))?;
    assert!(written[..data.len()] == data[..]);
    assert_eq!(&written[data.len()..], b"tail");
    Ok(())
}

/// A file read through the adapter after reads of its own goes on from
/// where they left it.
#[ruio::test]
async fn an_adapter_takes_the_file_from_its_position() -> io::Result<()> {
    let scratch = Scratch::new("position");
    let data = pattern(10_000, 5);
    std::fs::write(scratch.path("data"), &data)?;
    let file = File::open(scratch.path("data")).await?;
    let (read, _) = ruio::io::AsyncRead::read_exact(&file, Vec::with_capacity(1000)).await;
    read?;
    let mut file = Compat::new(file);
    let mut rest = Vec::new();
    within(file.read_to_end(&mut rest)).await?;
    assert!(rest == data[1000..]);
    Ok(())
}

/// A pipe's ends in `AsyncFd`s are read and written through the adapters,
/// a megabyte crossing from a writer that writes in one piece to a reader
/// on the same runtime.
#[ruio::test]
async fn a_pipe_crosses_the_adapters() -> io::Result<()> {
    let (reader, writer) = io::pipe()?;
    let (reader, writer) = (OwnedFd::from(reader), OwnedFd::from(writer));
    let mut reader = Compat::with_capacity(4096, AsyncFd::new(reader));
    let mut writer = Compat::new(AsyncFd::new(writer));
    let data = pattern(1 << 20, 17);
    let expected = data.clone();
    let writing = ruio::spawn(async move {
        within(writer.write_all(&data)).await?;
        within(writer.shutdown()).await?;
        // Dropped: the reader then sees the end.
        io::Result::Ok(())
    });
    let mut read = Vec::new();
    within(reader.read_to_end(&mut read)).await?;
    writing.await.unwrap()?;
    assert!(read == expected);
    Ok(())
}

/// A write the kernel cuts short, as it does a write to a pipe with less
/// room than the write, is carried on with the rest: the bytes the caller
/// was told were taken all arrive, in order.
#[ruio::test]
async fn a_short_write_is_carried_on() -> io::Result<()> {
    let (mut reader, writer) = io::pipe()?;
    let fd = writer.as_raw_fd();
    // SAFETY: `fcntl` with these commands touches no memory.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    // SAFETY: as above.
    let set = unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
    assert_eq!(set, 0);
    let mut writer = Compat::new(AsyncFd::new(OwnedFd::from(writer)));
    // Far more than the pipe holds: the first write stops short of it.
    let data = pattern(200_000, 23);
    let expected = data.clone();
    let draining = thread::spawn(move || {
        let mut read = Vec::new();
        reader.read_to_end(&mut read).unwrap();
        read
    });
    let mut written = 0;
    while written < data.len() {
        written += within(writer.write(&data[written..])).await?;
    }
    within(writer.shutdown()).await?;
    drop(writer);
    assert!(draining.join().unwrap() == expected);
    Ok(())
}

/// The adapter gives its descriptor's owner back, and reaches it meanwhile.
#[ruio::test]
async fn the_owner_is_reached_and_given_back() -> io::Result<()> {
    let (reader, mut writer) = io::pipe()?;
    let mut reader = Compat::new(AsyncFd::new(OwnedFd::from(reader)));
    writer.write_all(b"through")?;
    assert!(reader.get_ref().get_ref().as_raw_fd() >= 0);
    reader.get_mut();
    let mut word = [0; 7];
    within(reader.read_exact(&mut word)).await?;
    assert_eq!(&word, b"through");
    let owner: AsyncFd<OwnedFd> = reader.into_inner();
    let inner: OwnedFd = owner.into_inner();
    writer.write_all(b"!")?;
    let mut byte = [0; 1];
    std::fs::File::from(inner).read_exact(&mut byte)?;
    assert_eq!(&byte, b"!");
    Ok(())
}
