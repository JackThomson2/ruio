//! Files inside the runtime.

mod support;

use std::fs::Permissions;
use std::io::{self, ErrorKind};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use ruio::buf::{AlignedBuf, FixedBuffers};
use ruio::fs::{self, Advice, File, OpenOptions};

use support::within;

/// The null block device's path, block size and the alignment direct I/O
/// on it needs, where the polled tests run.
const NULLB: &str = "/dev/nullb0";
const BLOCK: usize = 4096;

/// Opens the null block device for polled I/O, or says why the test has
/// nothing to run against: the kernel's io_uring has no polled rings, or
/// the device is missing or has no poll queues.
async fn polled_nullb() -> Option<File> {
    if !ruio::uring::Probe::current()
        .unwrap()
        .supports_setup_flags(ruio::uring::sys::IORING_SETUP_IOPOLL)
    {
        eprintln!("skipped: the kernel's io_uring has no polled rings");
        return None;
    }
    let polls = std::fs::read_to_string("/sys/block/nullb0/queue/io_poll")
        .map(|polls| polls.trim() == "1")
        .unwrap_or(false);
    if !polls {
        eprintln!("skipped: no {NULLB} with poll queues");
        return None;
    }
    let opened = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .polled(true)
        .open(NULLB)
        .await;
    match opened {
        Ok(file) => Some(file),
        Err(err) => {
            eprintln!("skipped: {NULLB} cannot be opened for polled I/O: {err}");
            None
        }
    }
}

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

/// A direct descriptor reads and writes as a plain one does; what wants a
/// file descriptor, such as statx and fchmod, installs one for the call;
/// and the file becomes a plain one, or the standard library's, with its
/// slot freed.
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
    let found = within(file.metadata()).await?;
    assert_eq!(
        (found.len(), found.ino()),
        (6, std::fs::metadata(&path)?.ino())
    );
    assert_eq!(file.as_std().unwrap_err().kind(), ErrorKind::Unsupported);
    within(file.allocate(0, 4096)).await?;
    within(file.sync_range(0, 0)).await?;
    within(file.advise(0, 0, Advice::Sequential)).await?;
    within(file.set_permissions(Permissions::from_mode(0o600))).await?;
    assert_eq!(std::fs::metadata(&path)?.mode() & 0o777, 0o600);

    let plain = within(file.into_plain()).await?;
    assert!(!plain.is_direct());
    assert_eq!(plain.as_std()?.metadata()?.len(), 4096);
    let std_file = within(plain.into_std()).await?;
    assert_eq!(std_file.metadata()?.len(), 4096);
    drop(std_file);

    let file = OpenOptions::new()
        .read(true)
        .direct_descriptor(true)
        .open(&path)
        .await?;
    within(file.close()).await?;
    assert_eq!(std::fs::read(&path)?[..6], b"direct"[..]);
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

/// A polled file's reads, plain, exact and into registered buffers, go
/// through the driver's polled ring.
#[ruio::test]
async fn polled_reads_go_through_the_polled_ring() -> io::Result<()> {
    let Some(file) = polled_nullb().await else {
        return Ok(());
    };
    assert!(file.is_polled() && !file.is_direct());
    let before = ruio::driver().stats().polled;
    let buf = AlignedBuf::with_capacity(BLOCK, BLOCK);
    let (read, buf) = within(file.read_at(buf, 0)).await;
    assert_eq!(read?, BLOCK);
    assert_eq!(buf.len(), BLOCK);

    let buf = AlignedBuf::with_capacity(2 * BLOCK, BLOCK);
    let (read, buf) = within(file.read_exact_at(buf, 4 * BLOCK as u64)).await;
    read?;
    assert_eq!(buf.len(), 2 * BLOCK);

    let registered = FixedBuffers::new(&ruio::driver(), 1, BLOCK)?;
    let buf = registered.check_out(0).unwrap();
    let (read, buf) = within(file.read_fixed_at(buf, BLOCK as u64)).await;
    assert_eq!(read?, BLOCK);
    assert_eq!(buf.len(), BLOCK);
    assert_eq!(ruio::driver().stats().polled - before, 3);

    let err = within(
        file.read_at(AlignedBuf::with_capacity(BLOCK, BLOCK), 0)
            .timeout(Duration::from_secs(1)),
    )
    .await
    .0
    .unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EOPNOTSUPP));
    assert!(within(file.metadata()).await?.file_type().is_block_device());
    within(file.close()).await
}

/// Polled reads keep completing while a sleep is in flight, and the sleep
/// ends on time: the runtime polls the device and watches its timer.
#[ruio::test]
async fn polled_reads_run_beside_a_sleep() -> io::Result<()> {
    let Some(file) = polled_nullb().await else {
        return Ok(());
    };
    let file = std::rc::Rc::new(file);
    let reads = std::rc::Rc::new(std::cell::Cell::new(0_u64));
    let stop = std::rc::Rc::new(std::cell::Cell::new(false));
    let reader = ruio::spawn({
        let (file, reads, stop) = (file.clone(), reads.clone(), stop.clone());
        async move {
            let mut buf = AlignedBuf::with_capacity(BLOCK, BLOCK);
            while !stop.get() {
                let offset = (reads.get() % 1024) * BLOCK as u64;
                let (read, back) = file.read_at(buf, offset).await;
                buf = back;
                read?;
                reads.set(reads.get() + 1);
            }
            io::Result::Ok(())
        }
    });
    let start = Instant::now();
    within(ruio::time::sleep(Duration::from_millis(50))).await;
    let slept = start.elapsed();
    stop.set(true);
    within(reader).await.unwrap()?;
    assert!(slept >= Duration::from_millis(50), "{slept:?}");
    assert!(slept < Duration::from_secs(2), "{slept:?}");
    assert!(reads.get() > 100, "{} reads during the sleep", reads.get());
    Ok(())
}

/// Two polled files read at once, each from a task of its own.
#[ruio::test]
async fn two_polled_files_read_at_once() -> io::Result<()> {
    let Some(first) = polled_nullb().await else {
        return Ok(());
    };
    let second = polled_nullb().await.unwrap();
    let read_blocks = |file: File, from: u64| {
        ruio::spawn(async move {
            let mut buf = AlignedBuf::with_capacity(BLOCK, BLOCK);
            for block in 0..200 {
                let (read, back) = file.read_at(buf, (from + block) * BLOCK as u64).await;
                buf = back;
                assert_eq!(read?, BLOCK);
            }
            io::Result::Ok(())
        })
    };
    let before = ruio::driver().stats().polled;
    let (first, second) = (read_blocks(first, 0), read_blocks(second, 100_000));
    within(first).await.unwrap()?;
    within(second).await.unwrap()?;
    assert_eq!(ruio::driver().stats().polled - before, 400);
    assert_eq!(ruio::driver().in_flight(), 0);
    Ok(())
}

/// A file for polled I/O must be opened for direct I/O, and as a plain
/// descriptor.
#[ruio::test]
async fn polled_files_take_direct_io_and_plain_descriptors_only() -> io::Result<()> {
    let buffered = OpenOptions::new().read(true).polled(true).open(NULLB).await;
    assert_eq!(buffered.unwrap_err().kind(), ErrorKind::InvalidInput);
    let direct = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .direct_descriptor(true)
        .polled(true)
        .open(NULLB)
        .await;
    assert_eq!(direct.unwrap_err().kind(), ErrorKind::InvalidInput);
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

/// Links are made through the ring and read back on the pool.
#[ruio::test]
async fn names_are_linked() -> io::Result<()> {
    let scratch = Scratch::new("links");
    let file = scratch.path("file");
    fs::write(&file, "linked").await?;
    let original = std::fs::metadata(&file)?;

    let hard = scratch.path("hard");
    fs::hard_link(&file, &hard).await?;
    let linked = fs::metadata(&hard).await?;
    assert_eq!((linked.ino(), linked.nlink()), (original.ino(), 2));
    assert_eq!(
        fs::hard_link(&file, &hard).await.unwrap_err().kind(),
        ErrorKind::AlreadyExists
    );

    let soft = scratch.path("soft");
    fs::symlink("file", &soft).await?;
    assert_eq!(within(fs::read_link(&soft)).await?, PathBuf::from("file"));
    assert_eq!(fs::read(&soft).await?, b"linked");
    assert!(fs::symlink_metadata(&soft).await?.is_symlink());
    assert_eq!(
        within(fs::canonicalize(&soft)).await?,
        std::fs::canonicalize(&file)?
    );
    let to_link = scratch.path("to-link");
    fs::hard_link(&soft, &to_link).await?;
    assert!(
        fs::symlink_metadata(&to_link).await?.is_symlink(),
        "the link itself, as the standard library links it"
    );

    assert!(fs::try_exists(&file).await?);
    assert!(!fs::try_exists(scratch.path("missing")).await?);
    let dangling = scratch.path("dangling");
    fs::symlink(scratch.path("missing"), &dangling).await?;
    assert!(fs::symlink_metadata(&dangling).await?.is_symlink());
    assert!(
        !fs::try_exists(&dangling).await?,
        "a dangling link is no file"
    );
    assert_eq!(
        within(fs::read_link(&file)).await.unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        fs::symlink("x", scratch.path("nul\0byte"))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    Ok(())
}

/// Space is allocated and freed, ranges are written back, and advice is
/// taken, on plain descriptors here and on a direct one in
/// `files_open_as_direct_descriptors`.
#[ruio::test]
async fn files_are_allocated_synced_and_advised_on() -> io::Result<()> {
    let scratch = Scratch::new("allocate");
    let path = scratch.path("file");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .await?;
    within(file.allocate(0, 1 << 16)).await?;
    let found = within(file.metadata()).await?;
    assert_eq!(found.len(), 1 << 16);
    assert!(found.blocks() * 512 >= 1 << 16, "{} blocks", found.blocks());
    within(file.fallocate(1 << 16, 1 << 16, libc::FALLOC_FL_KEEP_SIZE)).await?;
    assert_eq!(within(file.metadata()).await?.len(), 1 << 16);

    let data = pattern(1 << 16);
    within(file.write_all_at(data.clone(), 0)).await.0?;
    let hole = libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE;
    within(file.fallocate(0, 4096, hole)).await?;
    let (read, head) = within(file.read_exact_at(Vec::with_capacity(8192), 0)).await;
    read?;
    assert_eq!(&head[..4096], &[0; 4096]);
    assert_eq!(&head[4096..], &data[4096..8192]);

    within(file.start_sync_range(0, 1 << 15)).await?;
    within(file.sync_range(0, 0)).await?;
    for advice in [
        Advice::Sequential,
        Advice::Random,
        Advice::Normal,
        Advice::WillNeed,
        Advice::DontNeed,
        Advice::NoReuse,
    ] {
        within(file.advise(0, 0, advice)).await?;
    }
    // The sync of a chunk follows its write, in one submission.
    let write = file.write_at(data.clone(), 1 << 16);
    let (written, synced) = within(write.link(file.sync_range(1 << 16, 1 << 16))).await;
    assert_eq!(written.0?, 1 << 16);
    synced?;
    assert_eq!(std::fs::read(&path)?[1 << 16..], data[..]);
    Ok(())
}

#[ruio::test]
async fn permissions_are_set() -> io::Result<()> {
    let scratch = Scratch::new("permissions");
    let path = scratch.path("file");
    fs::write(&path, "x").await?;
    within(fs::set_permissions(&path, Permissions::from_mode(0o600))).await?;
    assert_eq!(fs::metadata(&path).await?.mode() & 0o777, 0o600);
    let file = File::open(&path).await?;
    within(file.set_permissions(Permissions::from_mode(0o640))).await?;
    assert_eq!(within(file.metadata()).await?.mode() & 0o777, 0o640);
    let missing = fs::set_permissions(scratch.path("missing"), Permissions::from_mode(0o600));
    assert_eq!(
        within(missing).await.unwrap_err().kind(),
        ErrorKind::NotFound
    );
    Ok(())
}

/// A copy brings the permissions along; a removal takes the whole tree and
/// follows no link out of it.
#[ruio::test]
async fn files_are_copied_and_trees_removed() -> io::Result<()> {
    let scratch = Scratch::new("tree");
    let big = pattern(300_000);
    fs::write(scratch.path("big"), big.clone()).await?;
    within(fs::set_permissions(
        scratch.path("big"),
        Permissions::from_mode(0o640),
    ))
    .await?;
    let copied = within(fs::copy(scratch.path("big"), scratch.path("copy"))).await?;
    assert_eq!(copied, 300_000);
    assert!(fs::read(scratch.path("copy")).await? == big);
    assert_eq!(
        fs::metadata(scratch.path("copy")).await?.mode() & 0o777,
        0o640
    );
    let missing = fs::copy(scratch.path("missing"), scratch.path("x"));
    assert_eq!(
        within(missing).await.unwrap_err().kind(),
        ErrorKind::NotFound
    );

    let tree = scratch.path("tree");
    fs::create_dir_all(tree.join("a/b/c")).await?;
    for name in ["a/1", "a/b/2", "a/b/c/3"] {
        fs::write(tree.join(name), name).await?;
    }
    fs::symlink(&scratch.0, tree.join("a/out")).await?;
    within(fs::remove_dir_all(&tree)).await?;
    assert!(!tree.exists());
    assert!(scratch.path("big").exists(), "nothing past the link went");
    assert_eq!(
        within(fs::remove_dir_all(&tree)).await.unwrap_err().kind(),
        ErrorKind::NotFound
    );
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
    let mut names = vec![within(entries.next_entry()).await?.unwrap().file_name()];
    listed.set(names.len());
    // Spawned with the first batch in: it runs when the reader yields
    // between batches, before the reader is done.
    let bystander = ruio::spawn({
        let listed = listed.clone();
        async move { listed.get() }
    });
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

/// The thread is not held up by a read: a task spawned beside the reader
/// runs while the pool reads the first batch of a directory that is not
/// cached, or at the latest when the reader yields between batches.
#[ruio::test]
async fn read_dir_does_not_hold_up_the_thread() -> io::Result<()> {
    const COUNT: usize = 3000;
    let scratch = Scratch::new("read-dir-thread");
    for index in 0..COUNT {
        std::fs::File::create(scratch.path(&format!("entry-{index:05}")))?;
    }
    let mut entries = within(fs::read_dir(&scratch.0)).await?;
    let listed = std::rc::Rc::new(std::cell::Cell::new(0));
    let bystander = ruio::spawn({
        let listed = listed.clone();
        async move { listed.get() }
    });
    let mut count = 0;
    while within(entries.next_entry()).await?.is_some() {
        count += 1;
        listed.set(count);
    }
    assert_eq!(count, COUNT);
    let seen = bystander.await.unwrap();
    assert!(seen < COUNT, "the bystander saw {seen} entries");
    Ok(())
}

#[ruio::test]
async fn read_dir_of_an_empty_directory_ends_at_once() -> io::Result<()> {
    let scratch = Scratch::new("read-dir-empty");
    std::fs::create_dir(scratch.path("empty"))?;
    let mut entries = within(fs::read_dir(scratch.path("empty"))).await?;
    assert!(within(entries.next_entry()).await?.is_none());
    assert!(
        within(entries.next_entry()).await?.is_none(),
        "it stays ended"
    );
    Ok(())
}

/// A reader dropped with a batch being read on the pool leaves the read to
/// finish on its own; the directory stays open for the entries taken.
#[ruio::test]
async fn a_reader_dropped_mid_way_leaves_its_read_to_finish() -> io::Result<()> {
    const COUNT: usize = 3000;
    let scratch = Scratch::new("read-dir-dropped");
    for index in 0..COUNT {
        std::fs::write(scratch.path(&format!("entry-{index:05}")), b"xy")?;
    }
    let mut entries = within(fs::read_dir(&scratch.0)).await?;
    let entry = within(entries.next_entry()).await?.unwrap();
    drop(entries);
    assert_eq!(entry.metadata().await?.len(), 2);
    // Readers made and dropped with reads in flight, again and again.
    for _ in 0..50 {
        let mut entries = within(fs::read_dir(&scratch.0)).await?;
        within(entries.next_entry()).await?.unwrap();
    }
    let mut entries = within(fs::read_dir(&scratch.0)).await?;
    let mut count = 0;
    while within(entries.next_entry()).await?.is_some() {
        count += 1;
    }
    assert_eq!(count, COUNT);
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

/// What `async_trait` makes of a method: a boxed future that must be `Send`.
type SendFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// The functions that take a path give `Send` futures, which a boxed future
/// that must be one can hold across its awaits.
#[ruio::test]
async fn the_path_functions_run_in_futures_that_must_be_send() -> io::Result<()> {
    let scratch = Scratch::new("send");
    let dir = scratch.path("nested/deeper");
    let work: SendFuture<'_, io::Result<()>> = Box::pin(async {
        fs::create_dir_all(&dir).await?;
        let file = dir.join("file");
        fs::write(&file, pattern(20_000)).await?;
        assert_eq!(fs::read(&file).await?, pattern(20_000));
        assert_eq!(fs::metadata(&file).await?.len(), 20_000);
        assert!(fs::try_exists(&file).await?);
        fs::symlink(&file, dir.join("soft")).await?;
        assert!(fs::symlink_metadata(dir.join("soft")).await?.is_symlink());
        assert_eq!(fs::read_link(dir.join("soft")).await?, file);
        assert_eq!(
            fs::canonicalize(dir.join("soft")).await?,
            std::fs::canonicalize(&file)?
        );
        fs::hard_link(&file, dir.join("hard")).await?;
        fs::rename(dir.join("hard"), dir.join("moved")).await?;
        fs::copy(dir.join("moved"), dir.join("copied")).await?;
        fs::write(dir.join("text"), "text").await?;
        assert_eq!(fs::read_to_string(dir.join("text")).await?, "text");
        fs::set_permissions(&file, Permissions::from_mode(0o600)).await?;
        fs::remove_file(dir.join("soft")).await?;
        fs::remove_dir(scratch.path("empty")).await.unwrap_err();
        fs::create_dir(scratch.path("empty")).await?;
        fs::remove_dir(scratch.path("empty")).await?;
        fs::remove_dir_all(scratch.path("nested")).await?;
        assert!(!fs::try_exists(scratch.path("nested")).await?);
        Ok(())
    });
    within(work).await
}

/// A future reading and writing by path is spawned onto another core, which
/// runs its operations on its own ring.
#[test]
fn the_path_functions_run_on_any_core() {
    let all = ruio::Cores::all().unwrap();
    let cores = ruio::Cores::cpus(all.cpu_list().iter().copied().cycle().take(2));
    let scratch = Scratch::new("cores");
    let path = scratch.path("file");
    let outputs = cores
        .run(|core| {
            let path = path.clone();
            async move {
                if core.index() != 0 {
                    return None;
                }
                let task = ruio::spawn_on(1, async move {
                    fs::write(&path, b"from core one".to_vec()).await?;
                    let read = fs::read(&path).await?;
                    io::Result::Ok((read, ruio::core().unwrap().index()))
                });
                Some(task.await.unwrap())
            }
        })
        .unwrap();
    let (read, core) = outputs.into_iter().next().flatten().unwrap().unwrap();
    assert_eq!((&read[..], core), (&b"from core one"[..], 1));
}

/// A file whose size says nothing of what it holds, as a procfs file's
/// does, is read to its end, as the reads find it.
#[ruio::test]
async fn read_takes_a_file_to_its_end_whatever_its_size() -> io::Result<()> {
    assert_eq!(std::fs::metadata("/proc/self/status")?.len(), 0);
    let status = within(fs::read_to_string("/proc/self/status")).await?;
    assert!(status.contains("Pid:"), "{status}");
    let scratch = Scratch::new("ends");
    std::fs::write(scratch.path("empty"), b"")?;
    assert!(within(fs::read(scratch.path("empty"))).await?.is_empty());
    let err = within(fs::write(scratch.path("missing/file"), "x"))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    let err = within(fs::read(scratch.path("missing"))).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    Ok(())
}
