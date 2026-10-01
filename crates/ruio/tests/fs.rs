//! Files inside the runtime.

mod support;

use std::io::{self, ErrorKind};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::path::PathBuf;

use ruio::buf::{AlignedBuf, FixedBuffers};
use ruio::fs::{self, File, OpenOptions};

use support::within;

/// A directory of the test's own, removed when dropped.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("ruio-fs-{name}-{}", std::process::id()));
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

fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|index| (index % 251) as u8).collect()
}

#[ruio::test]
async fn files_are_written_and_read_back() -> io::Result<()> {
    let scratch = Scratch::new("rw");
    let path = scratch.path("file");
    let file = File::create(&path).await?;
    let data = pattern(100_000);
    within(file.write_all_at(data.clone(), 0)).await.0?;
    within(file.write_all_at(b"tail".to_vec(), 100_000))
        .await
        .0?;
    within(file.sync_all()).await?;
    within(file.sync_data()).await?;
    assert_eq!(within(file.metadata()).await?.len(), 100_004);
    within(file.close()).await?;

    let file = File::open(&path).await?;
    let (read, middle) = within(file.read_exact_at(Vec::with_capacity(1000), 50_000)).await;
    read?;
    assert_eq!(middle, &data[50_000..51_000]);
    let (read, end) = within(file.read_at(Vec::with_capacity(16), 100_000)).await;
    assert_eq!((read?, &end[..]), (4, &b"tail"[..]));
    let (read, _) = within(file.read_at(Vec::with_capacity(16), 200_000)).await;
    assert_eq!(read?, 0, "nothing past the end");
    let (read, partial) = within(file.read_exact_at(Vec::with_capacity(16), 99_998)).await;
    assert_eq!(read.unwrap_err().kind(), ErrorKind::UnexpectedEof);
    assert_eq!(
        partial,
        [data[99_998], data[99_999], b't', b'a', b'i', b'l']
    );
    Ok(())
}

#[ruio::test]
async fn files_grow_and_shrink() -> io::Result<()> {
    let scratch = Scratch::new("len");
    let path = scratch.path("file");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .await?;
    within(file.write_all_at(vec![1; 100], 0)).await.0?;
    within(file.set_len(40)).await?;
    within(file.set_len(50)).await?;
    let mut expected = vec![1; 40];
    expected.resize(50, 0);
    assert_eq!(fs::read(&path).await?, expected);
    Ok(())
}

/// Reads and writes at the file's position advance it, and appending writes
/// go to the end whatever the offset.
#[ruio::test]
async fn the_file_position_advances() -> io::Result<()> {
    let scratch = Scratch::new("position");
    let path = scratch.path("file");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .await?;
    assert_eq!(within(file.write(b"abc".to_vec())).await.0?, 3);
    assert_eq!(within(file.write(b"def".to_vec())).await.0?, 3);
    assert_eq!(std::fs::read(&path)?, b"abcdef");

    let file = File::open(&path).await?;
    let (_, first) = within(file.read(Vec::with_capacity(2))).await;
    let (_, second) = within(file.read(Vec::with_capacity(8))).await;
    assert_eq!((&first[..], &second[..]), (&b"ab"[..], &b"cdef"[..]));

    let appending = OpenOptions::new().append(true).open(&path).await?;
    within(appending.write_all_at(b"!".to_vec(), 0)).await.0?;
    assert_eq!(std::fs::read(&path)?, b"abcdef!");
    Ok(())
}

/// The options mean what the standard library's do.
#[ruio::test]
async fn options_follow_the_standard_library() -> io::Result<()> {
    let scratch = Scratch::new("options");
    let path = scratch.path("file");
    let created = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)
        .await?;
    drop(created);
    let mode = std::fs::metadata(&path)?.permissions().mode() & 0o777;
    assert_eq!(mode & !0o600, 0, "{mode:o}");

    let again = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .await;
    assert_eq!(again.unwrap_err().kind(), ErrorKind::AlreadyExists);
    let missing = File::open(scratch.path("missing")).await;
    assert_eq!(missing.unwrap_err().kind(), ErrorKind::NotFound);
    for options in [
        OpenOptions::new(),
        OpenOptions::new().read(true).truncate(true).clone(),
        OpenOptions::new().read(true).create(true).clone(),
        OpenOptions::new().append(true).truncate(true).clone(),
    ] {
        let err = options.open(&path).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput, "{options:?}");
    }
    Ok(())
}

#[ruio::test]
async fn metadata_reports_on_files() -> io::Result<()> {
    let scratch = Scratch::new("metadata");
    let path = scratch.path("file");
    let link = scratch.path("link");
    std::fs::write(&path, pattern(5000))?;
    symlink(&path, &link)?;
    let expected = std::fs::metadata(&path)?;

    let found = fs::metadata(&path).await?;
    assert!(found.is_file() && !found.is_dir() && !found.is_symlink());
    assert_eq!(
        (found.len(), found.ino(), found.dev()),
        (5000, expected.ino(), expected.dev())
    );
    assert_eq!(found.mode(), expected.mode());
    assert_eq!(found.modified()?, expected.modified()?);
    assert_eq!(found.nlink(), 1);
    assert!(fs::metadata(&link).await?.is_file());
    assert!(fs::symlink_metadata(&link).await?.is_symlink());
    assert!(fs::metadata(&scratch.0).await?.is_dir());
    let opened = File::open(&path).await?.metadata().await?;
    assert_eq!((opened.len(), opened.ino()), (5000, expected.ino()));
    Ok(())
}

/// A direct descriptor reads and writes as a plain one does, and has no
/// metadata to ask for.
#[ruio::test]
async fn files_open_as_direct_descriptors() -> io::Result<()> {
    let scratch = Scratch::new("direct");
    let path = scratch.path("file");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .direct_descriptor(true)
        .open(&path)
        .await?;
    assert!(file.is_direct());
    within(file.write_all_at(b"direct".to_vec(), 0)).await.0?;
    let (read, data) = within(file.read_exact_at(Vec::with_capacity(6), 0)).await;
    read?;
    assert_eq!(data, b"direct");
    assert_eq!(
        file.metadata().await.unwrap_err().kind(),
        ErrorKind::Unsupported
    );
    assert!(file.as_std().is_err());
    within(file.close()).await?;
    assert_eq!(std::fs::read(&path)?, b"direct");
    Ok(())
}

/// Direct I/O, past the page cache, with aligned buffers and with
/// registered ones.
#[ruio::test]
async fn direct_io_reads_and_writes_aligned_buffers() -> io::Result<()> {
    let scratch = Scratch::new("odirect");
    let path = scratch.path("file");
    let opened = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .custom_flags(libc::O_DIRECT)
        .open(&path)
        .await;
    let file = match opened {
        Ok(file) => file,
        Err(err) if err.kind() == ErrorKind::InvalidInput => {
            eprintln!("skipped: no direct I/O in {}", scratch.0.display());
            return Ok(());
        }
        Err(err) => return Err(err),
    };
    // SAFETY: F_GETFL reads the file's status flags and changes nothing.
    let flags = unsafe { libc::fcntl(file.as_std()?.as_raw_fd(), libc::F_GETFL) };
    assert_ne!(flags & libc::O_DIRECT, 0, "opened for direct I/O");
    let alignment = file
        .metadata()
        .await?
        .dio_alignment()
        .expect("a file system that opens files for direct I/O says how to align it");
    let block = alignment.offset.max(4096);
    let data = pattern(4 * block);
    let mut aligned = AlignedBuf::with_capacity(4 * block, alignment.memory);
    aligned.extend_from_slice(&data);
    within(file.write_all_at(aligned, 0)).await.0?;
    let buf = AlignedBuf::with_capacity(2 * block, alignment.memory);
    let (read, buf) = within(file.read_exact_at(buf, block as u64)).await;
    read?;
    assert_eq!(&*buf, &data[block..3 * block]);

    let registered = FixedBuffers::new(&ruio::driver(), 2, block)?;
    let buf = registered.check_out(1).unwrap();
    let (read, buf) = within(file.read_fixed_at(buf, 3 * block as u64)).await;
    assert_eq!(read?, block);
    assert_eq!(&*buf, &data[3 * block..]);
    Ok(())
}

#[ruio::test]
async fn names_are_made_changed_and_removed() -> io::Result<()> {
    let scratch = Scratch::new("names");
    let nested = scratch.path("a/b/c");
    fs::create_dir_all(&nested).await?;
    fs::create_dir_all(&nested).await?;
    assert!(nested.is_dir());
    let err = fs::create_dir(&nested).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::AlreadyExists);

    let file = nested.join("file");
    fs::write(&file, "contents").await?;
    assert_eq!(fs::read_to_string(&file).await?, "contents");
    let renamed = scratch.path("a/renamed");
    fs::rename(&file, &renamed).await?;
    assert!(!file.exists());
    assert_eq!(fs::read(&renamed).await?, b"contents");
    fs::remove_file(&renamed).await?;
    assert_eq!(
        fs::remove_file(&renamed).await.unwrap_err().kind(),
        ErrorKind::NotFound
    );
    for dir in ["a/b/c", "a/b", "a"] {
        fs::remove_dir(scratch.path(dir)).await?;
    }
    assert!(!scratch.path("a").exists());

    let big = pattern(300_000);
    fs::write(scratch.path("big"), big.clone()).await?;
    assert!(fs::read(scratch.path("big")).await? == big);
    std::fs::write(scratch.path("binary"), [0xff, 0xfe])?;
    let err = fs::read_to_string(scratch.path("binary"))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidData);
    Ok(())
}

/// A pipe's data arrives through one multishot read until its writing end
/// closes.
#[ruio::test]
async fn a_read_stream_takes_what_arrives_on_a_pipe() -> io::Result<()> {
    let (reader, writer) = io::pipe()?;
    let reader = File::from_std(std::os::fd::OwnedFd::from(reader).into());
    let pool = ruio::buf::BufPool::new(&ruio::driver(), 4, 1024)?;
    let sent = pattern(100_000);
    let writer = std::thread::spawn({
        let sent = sent.clone();
        move || {
            let mut writer = writer;
            for chunk in sent.chunks(3000) {
                io::Write::write_all(&mut writer, chunk)?;
            }
            io::Result::Ok(())
        }
    });
    let mut stream = reader.read_stream(&pool);
    let mut received = Vec::new();
    while let Some(chunk) = within(stream.next()).await {
        received.extend_from_slice(&chunk?);
    }
    writer.join().unwrap()?;
    assert_eq!(received, sent);
    Ok(())
}

/// A FIFO opened by its name, without blocking as the kernel needs to see
/// its end, streams what a writer sends until the writer closes; a regular
/// file, which cannot be polled, is refused.
#[ruio::test]
async fn a_read_stream_takes_what_arrives_on_a_fifo() -> io::Result<()> {
    let scratch = Scratch::new("fifo");
    let path = scratch.path("fifo");
    let name = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
    // SAFETY: `name` is a nul-terminated path that outlives the call.
    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
    let fifo = within(
        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(&path),
    )
    .await?;
    // With the reader there, the writer opens at once.
    let mut writer = std::fs::OpenOptions::new().write(true).open(&path)?;
    let pool = ruio::buf::BufPool::new(&ruio::driver(), 2, 64)?;
    let mut stream = fifo.read_stream(&pool);
    io::Write::write_all(&mut writer, b"through ")?;
    let first = within(stream.next()).await.unwrap()?;
    assert_eq!(&*first, b"through ");
    io::Write::write_all(&mut writer, b"the fifo")?;
    drop(writer);
    let mut received = first.to_vec();
    while let Some(chunk) = within(stream.next()).await {
        received.extend_from_slice(&chunk?);
    }
    assert_eq!(received, b"through the fifo");

    let regular = scratch.path("regular");
    fs::write(&regular, "data").await?;
    let file = File::open(&regular).await?;
    let mut stream = file.read_stream(&pool);
    let err = within(stream.next()).await.unwrap().unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EBADFD));
    Ok(())
}

/// Every entry, of every type, but `.` and `..`, with its name, path,
/// inode number and type; the status of a symbolic link is the link's.
#[ruio::test]
async fn read_dir_lists_the_entries_of_a_directory() -> io::Result<()> {
    let scratch = Scratch::new("read-dir");
    std::fs::write(scratch.path("file"), b"data")?;
    std::fs::create_dir(scratch.path("dir"))?;
    symlink("file", scratch.path("link"))?;
    let fifo =
        std::ffi::CString::new(scratch.path("fifo").into_os_string().into_encoded_bytes()).unwrap();
    // SAFETY: `fifo` is a nul-terminated path that outlives the call.
    assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);

    let mut entries = within(fs::read_dir(&scratch.0)).await?;
    let mut found = Vec::new();
    while let Some(entry) = within(entries.next_entry()).await? {
        let name = entry.file_name().into_string().unwrap();
        assert_eq!(entry.path(), scratch.path(&name));
        let std_status = std::fs::symlink_metadata(entry.path())?;
        assert_eq!(entry.ino(), std_status.ino());
        let kind = entry.file_type().await?;
        let status = entry.metadata().await?;
        assert_eq!(status.file_type(), kind);
        assert_eq!(status.ino(), std_status.ino());
        found.push((name, kind));
    }
    assert!(
        within(entries.next_entry()).await?.is_none(),
        "it stays ended"
    );
    found.sort_by(|a, b| a.0.cmp(&b.0));
    let names: Vec<_> = found.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(names, ["dir", "fifo", "file", "link"]);
    assert!(found[0].1.is_dir());
    assert!(found[1].1.is_fifo());
    assert!(found[2].1.is_file());
    assert!(found[3].1.is_symlink());
    Ok(())
}

/// A directory of more entries than one read takes is read in batches, all
/// of it, once, and the other tasks run between batches.
#[ruio::test]
async fn read_dir_reads_a_large_directory_in_batches() -> io::Result<()> {
    const COUNT: usize = 3000;
    let scratch = Scratch::new("read-dir-large");
    for index in 0..COUNT {
        std::fs::File::create(scratch.path(&format!("entry-{index:05}")))?;
    }
    let mut entries = within(fs::read_dir(&scratch.0)).await?;
    let listed = std::rc::Rc::new(std::cell::Cell::new(0));
    let bystander = ruio::spawn({
        let listed = listed.clone();
        async move { listed.get() }
    });
    let mut names = Vec::new();
    while let Some(entry) = within(entries.next_entry()).await? {
        names.push(entry.file_name());
        listed.set(names.len());
    }
    let seen = bystander.await.unwrap();
    assert!(0 < seen && seen < COUNT, "the bystander saw {seen} entries");
    names.sort();
    let expected: Vec<_> = (0..COUNT)
        .map(|index| std::ffi::OsString::from(format!("entry-{index:05}")))
        .collect();
    assert_eq!(names, expected);
    Ok(())
}

#[ruio::test]
async fn read_dir_refuses_what_is_not_a_directory() -> io::Result<()> {
    let scratch = Scratch::new("read-dir-errors");
    let missing = within(fs::read_dir(scratch.path("missing"))).await;
    assert_eq!(missing.unwrap_err().kind(), ErrorKind::NotFound);
    std::fs::write(scratch.path("file"), b"")?;
    let file = within(fs::read_dir(scratch.path("file"))).await;
    assert_eq!(file.unwrap_err().kind(), ErrorKind::NotADirectory);
    Ok(())
}

/// An entry keeps its directory open: its status is the entry's in the
/// directory read, after the reader is gone and the directory renamed.
#[ruio::test]
async fn an_entry_outlives_its_reader() -> io::Result<()> {
    let scratch = Scratch::new("read-dir-entry");
    std::fs::create_dir(scratch.path("before"))?;
    std::fs::write(scratch.path("before/inside"), b"12345")?;
    let mut entries = within(fs::read_dir(scratch.path("before"))).await?;
    let entry = within(entries.next_entry()).await?.unwrap();
    drop(entries);
    std::fs::rename(scratch.path("before"), scratch.path("after"))?;
    assert_eq!(entry.metadata().await?.len(), 5);
    assert_eq!(entry.path(), scratch.path("before/inside"));
    Ok(())
}

/// A write and a flush linked into a chain run in order, and the flush
/// waits for the write.
#[ruio::test]
async fn a_write_linked_to_a_flush_reaches_the_file() -> io::Result<()> {
    let scratch = Scratch::new("chain");
    let path = scratch.path("journal");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .await?;
    let (((first, _), (second, _)), synced) = within(
        file.write_at("first ", 0)
            .link(file.write_at("second", 6))
            .link(file.sync_data()),
    )
    .await;
    assert_eq!(first?, 6);
    assert_eq!(second?, 6);
    synced?;
    let (((written, _), synced), (read, data)) = within(
        file.write_at("!", 12)
            .link(file.sync_all())
            .link(file.read_at(Vec::with_capacity(13), 0)),
    )
    .await;
    assert_eq!(written?, 1);
    synced?;
    assert_eq!(read?, 13);
    assert_eq!(data, b"first second!");
    assert_eq!(std::fs::read(&path)?, b"first second!");
    Ok(())
}

/// A failing operation cancels the rest of its chain.
#[ruio::test]
async fn a_failed_write_cancels_its_flush() -> io::Result<()> {
    let scratch = Scratch::new("chain-failure");
    let path = scratch.path("read-only");
    std::fs::write(&path, b"")?;
    let file = File::open(&path).await?;
    let ((written, _), synced) = within(file.write_at("never", 0).link(file.sync_all())).await;
    assert_eq!(written.unwrap_err().raw_os_error(), Some(libc::EBADF));
    assert_eq!(synced.unwrap_err().raw_os_error(), Some(libc::ECANCELED));
    Ok(())
}

/// A pipe's data arrives whole through a stream whose own pool the kernel
/// consumes incrementally.
#[ruio::test]
async fn an_incremental_read_stream_takes_what_arrives() -> io::Result<()> {
    if !ruio::uring::Probe::current()?.supports_incremental_buffers() {
        eprintln!("skipped: the kernel does not consume buffers incrementally");
        return Ok(());
    }
    let (reader, writer) = io::pipe()?;
    let reader = File::from_std(std::os::fd::OwnedFd::from(reader).into());
    let sent = pattern(30_000);
    let writer = std::thread::spawn({
        let sent = sent.clone();
        move || {
            let mut writer = writer;
            for chunk in sent.chunks(250) {
                io::Write::write_all(&mut writer, chunk)?;
            }
            io::Result::Ok(())
        }
    });
    let mut stream = reader.read_stream_incremental(2, 8192)?;
    let mut received = Vec::new();
    while let Some(chunk) = within(stream.next()).await {
        received.extend_from_slice(&chunk?);
    }
    writer.join().unwrap()?;
    assert_eq!(received, sent);
    Ok(())
}
