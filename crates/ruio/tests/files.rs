//! Files as every engine with I/O offers them, through the API the engines
//! share: the tests name nothing that only one engine has, and run on
//! io_uring and on the I/O completion port alike.

#![cfg(any(ruio_uring, ruio_iocp))]

mod support;

use std::io::{self, ErrorKind};
use std::path::PathBuf;

use ruio::buf::IoBuf;
use ruio::fs::{self, File, OpenOptions};

use support::within;

/// A directory of the test's own in the temporary directory, removed with
/// what is in it when dropped.
struct Scratch(PathBuf);

impl Scratch {
    fn new(test: &str) -> Self {
        let name = format!("ruio-files-{test}-{}", std::process::id());
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

#[ruio::test]
async fn what_is_written_at_offsets_reads_back() -> io::Result<()> {
    let scratch = Scratch::new("offsets");
    let path = scratch.path("file");
    let file = within(File::create(&path)).await?;
    let (written, _) = within(file.write_at(b"hello, file".to_vec(), 0)).await;
    assert_eq!(written?, 11);
    within(file.write_all_at("!", 11)).await.0?;
    within(file.close()).await?;

    let file = within(File::open(&path)).await?;
    let (read, buf) = within(file.read_at(Vec::with_capacity(64), 0)).await;
    assert_eq!(read?, 12);
    assert_eq!(buf, b"hello, file!");

    // A whole buffer is filled after what it holds.
    let mut buf = Vec::with_capacity(8);
    buf.extend_from_slice(b"got ");
    let (read, buf) = within(file.read_exact_at(buf.slice(..8), 7)).await;
    read?;
    assert_eq!(buf.into_inner(), b"got file");

    // The file ends before this buffer is full.
    let buf = Vec::with_capacity(8).slice(..8);
    let (read, buf) = within(file.read_exact_at(buf, 7)).await;
    assert_eq!(read.unwrap_err().kind(), ErrorKind::UnexpectedEof);
    assert_eq!(buf.into_inner(), b"file!");

    let (read, _) = within(file.read_at(Vec::with_capacity(8), 100)).await;
    assert_eq!(read?, 0);
    Ok(())
}

#[ruio::test]
async fn reads_and_writes_at_the_position_move_it_on() -> io::Result<()> {
    let scratch = Scratch::new("position");
    let path = scratch.path("file");
    let file = within(File::create(&path)).await?;
    assert_eq!(within(file.write("hello, ")).await.0?, 7);
    assert_eq!(within(file.write("world")).await.0?, 5);
    within(file.close()).await?;

    let file = within(File::open(&path)).await?;
    let (read, buf) = within(file.read(Vec::with_capacity(7))).await;
    assert_eq!(read?, 7);
    assert_eq!(buf, b"hello, ");
    // A read at an offset leaves the position where it is.
    let (read, buf) = within(file.read_at(Vec::with_capacity(2), 0)).await;
    assert_eq!(read?, 2);
    assert_eq!(buf, b"he");
    let (read, buf) = within(file.read(Vec::with_capacity(16))).await;
    assert_eq!(read?, 5);
    assert_eq!(buf, b"world");
    let (read, _) = within(file.read(Vec::with_capacity(16))).await;
    assert_eq!(read?, 0);
    Ok(())
}

#[ruio::test]
async fn files_open_as_their_options_say() -> io::Result<()> {
    let scratch = Scratch::new("options");
    let path = scratch.path("file");
    let missing = within(File::open(&path)).await.expect_err("no file");
    assert_eq!(missing.kind(), ErrorKind::NotFound);

    let mut new = OpenOptions::new();
    new.write(true).create_new(true);
    let file = within(new.open(&path)).await?;
    within(file.write_all_at("first", 0)).await.0?;
    within(file.close()).await?;
    let exists = within(new.open(&path)).await.expect_err("a file");
    assert_eq!(exists.kind(), ErrorKind::AlreadyExists);

    // A file opened to append writes at its end.
    let file = within(File::options().append(true).open(&path)).await?;
    assert_eq!(within(file.write(", second")).await.0?, 8);
    within(file.close()).await?;
    assert_eq!(within(fs::read(&path)).await?, b"first, second");

    let file = within(File::options().write(true).truncate(true).open(&path)).await?;
    within(file.close()).await?;
    assert!(within(fs::read(&path)).await?.is_empty());
    Ok(())
}

#[ruio::test]
async fn a_file_is_synced_resized_and_measured() -> io::Result<()> {
    let scratch = Scratch::new("resize");
    let path = scratch.path("file");
    let mut options = File::options();
    options.read(true).write(true).create(true);
    let file = within(options.open(&path)).await?;
    within(file.write_all_at("0123456789", 0)).await.0?;
    within(file.sync_all()).await?;
    within(file.sync_data()).await?;
    let status = within(file.metadata()).await?;
    assert!(status.is_file());
    assert_eq!(status.len(), 10);

    within(file.set_len(4)).await?;
    assert_eq!(within(file.metadata()).await?.len(), 4);
    let (read, buf) = within(file.read_at(Vec::with_capacity(16), 0)).await;
    assert_eq!(read?, 4);
    assert_eq!(buf, b"0123");

    // A file made longer is filled out with zeros.
    within(file.set_len(6)).await?;
    let (read, buf) = within(file.read_at(Vec::with_capacity(16), 0)).await;
    assert_eq!(read?, 6);
    assert_eq!(buf, b"0123\0\0");
    Ok(())
}

#[ruio::test]
async fn names_are_made_changed_and_removed() -> io::Result<()> {
    let scratch = Scratch::new("names");
    let file = scratch.path("file");
    within(fs::write(&file, "contents")).await?;
    assert_eq!(within(fs::read(&file)).await?, b"contents");
    assert_eq!(within(fs::read_to_string(&file)).await?, "contents");
    let status = within(fs::metadata(&file)).await?;
    assert!(status.is_file() && !status.is_dir() && !status.is_symlink());
    assert_eq!(status.len(), 8);
    assert!(status.modified().is_ok());
    let own = within(fs::symlink_metadata(&file)).await?;
    assert!(own.file_type().is_file());
    assert!(within(fs::try_exists(&file)).await?);
    assert!(within(fs::canonicalize(&file)).await?.is_absolute());

    let renamed = scratch.path("renamed");
    within(fs::rename(&file, &renamed)).await?;
    assert!(!within(fs::try_exists(&file)).await?);
    let copy = scratch.path("copy");
    assert_eq!(within(fs::copy(&renamed, &copy)).await?, 8);
    let link = scratch.path("link");
    within(fs::hard_link(&copy, &link)).await?;
    assert_eq!(within(fs::read(&link)).await?, b"contents");

    // Read-only, and as it was again.
    let writable = within(fs::metadata(&copy)).await?.permissions();
    assert!(!writable.readonly());
    let mut read_only = writable.clone();
    read_only.set_readonly(true);
    within(fs::set_permissions(&copy, read_only)).await?;
    assert!(within(fs::metadata(&copy)).await?.permissions().readonly());
    within(fs::set_permissions(&copy, writable)).await?;
    assert!(!within(fs::metadata(&copy)).await?.permissions().readonly());

    for path in [&renamed, &copy, &link] {
        within(fs::remove_file(path)).await?;
    }
    let removed = within(fs::metadata(&renamed)).await.expect_err("no file");
    assert_eq!(removed.kind(), ErrorKind::NotFound);

    let above = scratch.path("above");
    let nested = above.join("nested");
    within(fs::create_dir_all(&nested)).await?;
    assert!(within(fs::metadata(&nested)).await?.is_dir());
    within(fs::remove_dir(&nested)).await?;
    within(fs::create_dir(&nested)).await?;
    within(fs::write(nested.join("file"), "x")).await?;
    within(fs::remove_dir_all(&above)).await?;
    assert!(!within(fs::try_exists(&above)).await?);
    Ok(())
}

#[ruio::test]
async fn a_directorys_entries_are_read() -> io::Result<()> {
    // More than one batch of them, on either engine.
    const FILES: usize = 150;
    let scratch = Scratch::new("entries");
    for file in 0..FILES {
        std::fs::write(scratch.path(&format!("file-{file:03}")), vec![b'x'; file])?;
    }
    std::fs::create_dir(scratch.path("dir"))?;

    let mut entries = within(fs::read_dir(&scratch.0)).await?;
    let mut names = Vec::new();
    while let Some(entry) = within(entries.next_entry()).await? {
        let name = entry.file_name().into_string().expect("an ASCII name");
        assert_eq!(entry.path(), scratch.path(&name));
        if name == "dir" {
            assert!(entry.file_type().await?.is_dir());
            assert!(entry.metadata().await?.is_dir());
        } else {
            assert!(entry.file_type().await?.is_file());
            let len: u64 = name["file-".len()..].parse().unwrap();
            assert_eq!(entry.metadata().await?.len(), len);
        }
        names.push(name);
    }
    names.sort();
    let mut expected = vec!["dir".to_owned()];
    expected.extend((0..FILES).map(|file| format!("file-{file:03}")));
    assert_eq!(names, expected);
    // Past the end is the end still.
    assert!(within(entries.next_entry()).await?.is_none());

    let missing = within(fs::read_dir(scratch.path("missing"))).await;
    assert_eq!(
        missing.expect_err("no directory").kind(),
        ErrorKind::NotFound
    );
    Ok(())
}

/// A directory's entries are a `Stream` of futures-core.
#[cfg(feature = "futures")]
#[ruio::test]
async fn a_directorys_entries_are_a_stream() -> io::Result<()> {
    use futures_util::StreamExt;
    use futures_util::stream::FusedStream;

    let scratch = Scratch::new("stream");
    std::fs::write(scratch.path("one"), b"1")?;
    std::fs::write(scratch.path("two"), b"2")?;
    let mut entries = within(fs::read_dir(&scratch.0)).await?;
    let mut names = Vec::new();
    while let Some(entry) = within(StreamExt::next(&mut entries)).await {
        names.push(entry?.file_name());
    }
    names.sort();
    assert_eq!(names, ["one", "two"]);
    assert!(entries.is_terminated());
    Ok(())
}
