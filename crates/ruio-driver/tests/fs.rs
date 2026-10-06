//! File and file-system operations, against files in a scratch directory.

mod support;

use std::fs;
use std::io::Read as _;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::path::PathBuf;

use ruio_driver::buf::{AlignedBuf, IoBuf};
use ruio_driver::ops::{
    Close, Fadvise, Fallocate, Fsync, Ftruncate, LinkAt, MkdirAt, OpenAt, Read, RenameAt, Statx,
    SymlinkAt, SyncFileRange, UnlinkAt, Write,
};
use ruio_driver::{Descriptor, Driver, Fd};

use support::{block_on, poll_once, settle};

/// A directory of the test's own, removed when dropped.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("ruio-driver-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn plain(descriptor: Descriptor) -> OwnedFd {
    match descriptor {
        Descriptor::Plain(fd) => fd,
        Descriptor::Direct(_) => panic!("a direct descriptor where a plain one was asked for"),
    }
}

/// Whether `fd` is open, as `fcntl(F_GETFD)` tells.
fn is_open(fd: RawFd) -> bool {
    // SAFETY: F_GETFD reads the descriptor's flags and changes nothing.
    unsafe { libc::fcntl(fd, libc::F_GETFD) != -1 }
}

#[test]
fn files_open_write_and_read_back() {
    let driver = Driver::new().unwrap();
    let scratch = Scratch::new("open");
    let path = scratch.path("file");
    let flags = libc::O_RDWR | libc::O_CREAT | libc::O_EXCL;
    let open = OpenAt::new(&path).unwrap().flags(flags).mode(0o640);
    let file = plain(block_on(&driver, driver.op(open)).unwrap());
    // SAFETY: F_GETFD reads the descriptor's flags and changes nothing.
    let fd_flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFD) };
    assert_eq!(fd_flags & libc::FD_CLOEXEC, libc::FD_CLOEXEC);
    let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode & !0o640, 0, "{mode:o} is 0o640 less the umask");

    let write = Write::at(file.as_raw_fd(), b"hello, world".to_vec(), 0);
    assert_eq!(block_on(&driver, driver.op(write)).0.unwrap(), 12);
    let read = Read::at(file.as_raw_fd(), Vec::with_capacity(5), 7);
    let (read, buf) = block_on(&driver, driver.op(read));
    assert_eq!(read.unwrap(), 5);
    assert_eq!(buf, b"world");

    let again = OpenAt::new(&path).unwrap().flags(flags);
    let err = block_on(&driver, driver.op(again)).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EEXIST));
    // An open that creates nothing ignores the mode, which `openat2(2)`
    // would refuse.
    let reopen = OpenAt::new(&path).unwrap().mode(0o600);
    plain(block_on(&driver, driver.op(reopen)).unwrap());
    let missing = OpenAt::new(scratch.path("missing")).unwrap();
    let err = block_on(&driver, driver.op(missing)).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOENT));
    assert_eq!(
        OpenAt::new("nul\0byte").unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
}

/// A direct descriptor of a file reads as a plain one does, and closes with
/// a result; close-on-exec, which the kernel refuses for one, is dropped.
#[test]
fn files_open_as_direct_descriptors() {
    let driver = Driver::builder().files(8).build().unwrap();
    let scratch = Scratch::new("direct");
    let path = scratch.path("file");
    fs::write(&path, b"direct").unwrap();
    let open = OpenAt::new(&path)
        .unwrap()
        .flags(libc::O_RDONLY | libc::O_CLOEXEC)
        .direct(&driver);
    let Descriptor::Direct(file) = block_on(&driver, driver.op(open)).unwrap() else {
        panic!("a plain descriptor where a direct one was asked for");
    };
    let (read, buf) = block_on(
        &driver,
        driver.op(Read::at(file.fd(), Vec::with_capacity(8), 0)),
    );
    assert_eq!(read.unwrap(), 6);
    assert_eq!(buf, b"direct");

    let slot = file.slot();
    block_on(&driver, file.close()).unwrap();
    let read = Read::at(Fd::Direct(slot), Vec::with_capacity(8), 0);
    let err = block_on(&driver, driver.op(read)).0.unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EBADF), "the slot is free");
}

#[test]
fn statx_reports_on_files_by_path_and_by_descriptor() {
    let driver = Driver::new().unwrap();
    let scratch = Scratch::new("statx");
    let path = scratch.path("file");
    let link = scratch.path("link");
    fs::write(&path, vec![7; 5000]).unwrap();
    symlink(&path, &link).unwrap();
    let expected = fs::metadata(&path).unwrap();

    let stat = block_on(&driver, driver.op(Statx::new(&path).unwrap())).unwrap();
    assert_ne!(stat.stx_mask & libc::STATX_SIZE, 0);
    assert_eq!(stat.stx_size, 5000);
    assert_eq!(stat.stx_ino, expected.ino());
    assert_eq!(u32::from(stat.stx_mode) & libc::S_IFMT, libc::S_IFREG);

    let through = block_on(&driver, driver.op(Statx::new(&link).unwrap())).unwrap();
    assert_eq!(through.stx_ino, expected.ino());
    let nofollow = Statx::new(&link).unwrap().flags(libc::AT_SYMLINK_NOFOLLOW);
    let itself = block_on(&driver, driver.op(nofollow)).unwrap();
    assert_eq!(u32::from(itself.stx_mode) & libc::S_IFMT, libc::S_IFLNK);

    let file = fs::File::open(&path).unwrap();
    let stat = block_on(&driver, driver.op(Statx::fd(file.as_raw_fd()))).unwrap();
    assert_eq!((stat.stx_size, stat.stx_ino), (5000, expected.ino()));
    let dio = Statx::fd(file.as_raw_fd()).mask(libc::STATX_DIOALIGN);
    let stat = block_on(&driver, driver.op(dio)).unwrap();
    if stat.stx_mask & libc::STATX_DIOALIGN != 0 {
        assert!(stat.stx_dio_mem_align.is_power_of_two() || stat.stx_dio_mem_align == 0);
    }

    let missing = Statx::new(scratch.path("missing")).unwrap();
    let err = block_on(&driver, driver.op(missing)).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOENT));
}

#[test]
fn files_sync_grow_and_shrink() {
    let driver = Driver::new().unwrap();
    let scratch = Scratch::new("sync");
    let path = scratch.path("file");
    let file = fs::File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    let fd = file.as_raw_fd();
    let len = || fs::metadata(&path).unwrap().len();

    let write = Write::at(fd, vec![1; 100], 0);
    assert_eq!(block_on(&driver, driver.op(write)).0.unwrap(), 100);
    block_on(&driver, driver.op(Fsync::new(fd))).unwrap();
    block_on(&driver, driver.op(Fsync::new(fd).data_only())).unwrap();

    block_on(&driver, driver.op(Fallocate::new(fd, 0, 8192))).unwrap();
    assert_eq!(len(), 8192);
    let keep = Fallocate::new(fd, 8192, 4096).mode(libc::FALLOC_FL_KEEP_SIZE);
    block_on(&driver, driver.op(keep)).unwrap();
    assert_eq!(len(), 8192);

    block_on(&driver, driver.op(Ftruncate::new(fd, 50))).unwrap();
    block_on(&driver, driver.op(Ftruncate::new(fd, 60))).unwrap();
    let mut expected = vec![1; 50];
    expected.resize(60, 0);
    assert_eq!(fs::read(&path).unwrap(), expected);

    let read_only = fs::File::open(&path).unwrap();
    let err = block_on(&driver, driver.op(Ftruncate::new(read_only.as_raw_fd(), 0))).unwrap_err();
    assert!(
        matches!(err.raw_os_error(), Some(libc::EINVAL | libc::EBADF)),
        "{err}"
    );
}

#[test]
fn names_are_made_moved_and_removed() {
    let driver = Driver::new().unwrap();
    let scratch = Scratch::new("names");
    let dir = scratch.path("dir");
    block_on(&driver, driver.op(MkdirAt::new(&dir).unwrap().mode(0o700))).unwrap();
    assert!(fs::metadata(&dir).unwrap().is_dir());

    fs::write(dir.join("a"), "a").unwrap();
    let rename = RenameAt::new(dir.join("a"), dir.join("b")).unwrap();
    block_on(&driver, driver.op(rename)).unwrap();
    assert!(!dir.join("a").exists());
    assert_eq!(fs::read(dir.join("b")).unwrap(), b"a");

    fs::write(dir.join("c"), "c").unwrap();
    let noreplace = RenameAt::new(dir.join("c"), dir.join("b"))
        .unwrap()
        .flags(libc::RENAME_NOREPLACE);
    let err = block_on(&driver, driver.op(noreplace)).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EEXIST));

    let mode = fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode & !0o700, 0, "{mode:o} is 0o700 less the umask");

    // Relative to directories' descriptors.
    let handle = fs::File::open(&dir).unwrap();
    let sub = MkdirAt::at(handle.as_raw_fd(), "sub").unwrap();
    block_on(&driver, driver.op(sub)).unwrap();
    assert!(dir.join("sub").is_dir());
    let other = fs::File::open(dir.join("sub")).unwrap();
    let across = RenameAt::at(handle.as_raw_fd(), "c", other.as_raw_fd(), "moved").unwrap();
    block_on(&driver, driver.op(across)).unwrap();
    let back = RenameAt::at(other.as_raw_fd(), "moved", handle.as_raw_fd(), "c").unwrap();
    block_on(&driver, driver.op(back)).unwrap();
    assert_eq!(fs::read(dir.join("c")).unwrap(), b"c");
    let sub = UnlinkAt::at(handle.as_raw_fd(), "sub").unwrap().directory();
    block_on(&driver, driver.op(sub)).unwrap();

    for name in ["b", "c"] {
        block_on(&driver, driver.op(UnlinkAt::new(dir.join(name)).unwrap())).unwrap();
    }
    let err = block_on(&driver, driver.op(UnlinkAt::new(&dir).unwrap())).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EISDIR));
    block_on(&driver, driver.op(UnlinkAt::new(&dir).unwrap().directory())).unwrap();
    assert!(!dir.exists());
}

/// A hard link is another name for the same inode; a symbolic link holds
/// its target as given. Either is made relative to the working directory or
/// to a directory's descriptor.
#[test]
fn names_are_linked() {
    let driver = Driver::new().unwrap();
    let scratch = Scratch::new("links");
    let file = scratch.path("file");
    fs::write(&file, "linked").unwrap();
    let original = fs::metadata(&file).unwrap();

    let hard = scratch.path("hard");
    block_on(&driver, driver.op(LinkAt::new(&file, &hard).unwrap())).unwrap();
    let linked = fs::metadata(&hard).unwrap();
    assert_eq!((linked.ino(), linked.nlink()), (original.ino(), 2));
    assert_eq!(fs::read(&hard).unwrap(), b"linked");
    let err = block_on(&driver, driver.op(LinkAt::new(&file, &hard).unwrap())).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EEXIST));

    let soft = scratch.path("soft");
    block_on(&driver, driver.op(SymlinkAt::new("file", &soft).unwrap())).unwrap();
    assert_eq!(fs::read_link(&soft).unwrap(), PathBuf::from("file"));
    assert_eq!(
        fs::read(&soft).unwrap(),
        b"linked",
        "relative to its directory"
    );
    let dangling = scratch.path("dangling");
    let to_nowhere = SymlinkAt::new(scratch.path("nowhere"), &dangling).unwrap();
    block_on(&driver, driver.op(to_nowhere)).unwrap();
    assert!(fs::symlink_metadata(&dangling).unwrap().is_symlink());
    assert_eq!(
        fs::metadata(&dangling).unwrap_err().kind(),
        std::io::ErrorKind::NotFound
    );

    // A hard link to a symbolic link links the link itself, unless asked to
    // follow it.
    let to_link = scratch.path("to-link");
    block_on(&driver, driver.op(LinkAt::new(&soft, &to_link).unwrap())).unwrap();
    assert!(fs::symlink_metadata(&to_link).unwrap().is_symlink());
    let through = scratch.path("through");
    let follow = LinkAt::new(&soft, &through)
        .unwrap()
        .flags(libc::AT_SYMLINK_FOLLOW);
    block_on(&driver, driver.op(follow)).unwrap();
    let through = fs::symlink_metadata(&through).unwrap();
    assert!(through.is_file());
    assert_eq!((through.ino(), through.nlink()), (original.ino(), 3));

    // Relative to directories' descriptors.
    let dir = scratch.path("dir");
    fs::create_dir(&dir).unwrap();
    let top = fs::File::open(&scratch.0).unwrap();
    let inside = fs::File::open(&dir).unwrap();
    let across = LinkAt::at(top.as_raw_fd(), "file", inside.as_raw_fd(), "hard").unwrap();
    block_on(&driver, driver.op(across)).unwrap();
    assert_eq!(
        fs::metadata(dir.join("hard")).unwrap().ino(),
        original.ino()
    );
    let soft = SymlinkAt::at("../file", inside.as_raw_fd(), "soft").unwrap();
    block_on(&driver, driver.op(soft)).unwrap();
    assert_eq!(fs::read(dir.join("soft")).unwrap(), b"linked");

    for (path, kind) in [
        (
            LinkAt::new("nul\0byte", "x").err(),
            "a hard link's original",
        ),
        (LinkAt::new("x", "nul\0byte").err(), "a hard link's name"),
        (
            SymlinkAt::new("nul\0byte", "x").err(),
            "a symbolic link's target",
        ),
        (
            SymlinkAt::new("x", "nul\0byte").err(),
            "a symbolic link's name",
        ),
    ] {
        assert_eq!(
            path.map(|err| err.kind()),
            Some(std::io::ErrorKind::InvalidInput),
            "{kind}"
        );
    }
}

/// What a range sync writes back and what advice does to the page cache are
/// invisible to a test; the kernel accepts them, and refuses what is not a
/// file or not advice.
#[test]
fn ranges_are_synced_and_advised_on() {
    let driver = Driver::new().unwrap();
    let scratch = Scratch::new("advise");
    let path = scratch.path("file");
    let file = fs::File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    let fd = file.as_raw_fd();
    let write = Write::at(fd, vec![2; 16384], 0);
    assert_eq!(block_on(&driver, driver.op(write)).0.unwrap(), 16384);

    block_on(&driver, driver.op(SyncFileRange::new(fd, 0, 8192))).unwrap();
    let rest = SyncFileRange::new(fd, 8192, 0).flags(libc::SYNC_FILE_RANGE_WRITE);
    block_on(&driver, driver.op(rest)).unwrap();
    let wait = SyncFileRange::new(fd, 0, 0).flags(libc::SYNC_FILE_RANGE_WAIT_AFTER);
    block_on(&driver, driver.op(wait)).unwrap();
    assert_eq!(fs::read(&path).unwrap(), vec![2; 16384]);

    for advice in [
        libc::POSIX_FADV_SEQUENTIAL,
        libc::POSIX_FADV_RANDOM,
        libc::POSIX_FADV_NORMAL,
        libc::POSIX_FADV_WILLNEED,
        libc::POSIX_FADV_DONTNEED,
        libc::POSIX_FADV_NOREUSE,
    ] {
        let advise = Fadvise::new(fd, 0, 8192, advice);
        block_on(&driver, driver.op(advise)).unwrap_or_else(|err| panic!("{advice}: {err}"));
    }
    let whole = Fadvise::new(fd, 0, 0, libc::POSIX_FADV_DONTNEED);
    block_on(&driver, driver.op(whole)).unwrap();
    let err = block_on(&driver, driver.op(Fadvise::new(fd, 0, 0, 1 << 20))).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EINVAL), "not advice");

    let (reader, _writer) = std::io::pipe().unwrap();
    let sync = SyncFileRange::new(reader.as_raw_fd(), 0, 0);
    let err = block_on(&driver, driver.op(sync)).unwrap_err();
    assert_eq!(
        err.raw_os_error(),
        Some(libc::ESPIPE),
        "a pipe has no ranges"
    );
    let advise = Fadvise::new(reader.as_raw_fd(), 0, 0, libc::POSIX_FADV_NORMAL);
    let err = block_on(&driver, driver.op(advise)).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ESPIPE));
}

/// Direct I/O moves data between the device and memory past the page cache,
/// so the memory, and each transfer's offset and length, must be aligned as
/// the file system says (`STATX_DIOALIGN`).
#[test]
fn direct_io_moves_aligned_buffers() {
    let driver = Driver::new().unwrap();
    let scratch = Scratch::new("odirect");
    let path = scratch.path("file");
    let flags = libc::O_RDWR | libc::O_CREAT | libc::O_DIRECT;
    let open = OpenAt::new(&path).unwrap().flags(flags).mode(0o600);
    let file = match block_on(&driver, driver.op(open)) {
        Ok(file) => plain(file),
        // tmpfs, for one, has no direct I/O.
        Err(err) if err.raw_os_error() == Some(libc::EINVAL) => {
            eprintln!("skipped: no direct I/O in {}", scratch.0.display());
            return;
        }
        Err(err) => panic!("{err}"),
    };
    let fd = file.as_raw_fd();
    let dio = Statx::fd(fd).mask(libc::STATX_DIOALIGN);
    let dio = block_on(&driver, driver.op(dio)).unwrap();
    assert_ne!(dio.stx_mask & libc::STATX_DIOALIGN, 0);
    let memory = dio.stx_dio_mem_align as usize;
    let offset = dio.stx_dio_offset_align as usize;
    assert!(memory > 0 && offset > 1, "{memory} {offset}");

    let block = offset.max(4096);
    let pattern: Vec<u8> = (0..2 * block).map(|index| (index % 251) as u8).collect();
    let mut data = AlignedBuf::with_capacity(2 * block, memory);
    data.extend_from_slice(&pattern);
    let (written, _) = block_on(&driver, driver.op(Write::at(fd, data, 0)));
    assert_eq!(written.unwrap(), 2 * block);
    let read = Read::at(fd, AlignedBuf::with_capacity(block, memory), block as u64);
    let (read, buf) = block_on(&driver, driver.op(read));
    assert_eq!(read.unwrap(), block);
    assert_eq!(&*buf, &pattern[block..]);

    let off_offset = Read::at(
        fd,
        AlignedBuf::with_capacity(block, memory),
        offset as u64 / 2,
    );
    let err = block_on(&driver, driver.op(off_offset)).0.unwrap_err();
    assert_eq!(
        err.raw_os_error(),
        Some(libc::EINVAL),
        "a misaligned offset"
    );
    if memory > 1 {
        let off_memory = AlignedBuf::with_capacity(block + memory, memory).slice(1..=block);
        let err = block_on(&driver, driver.op(Read::at(fd, off_memory, 0)))
            .0
            .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EINVAL), "misaligned memory");
    }
}

/// A close closes its descriptor once: through the kernel once submitted,
/// by the operation's drop before that, and never twice, which could close
/// a file that reused the number.
///
/// Other tests open files meanwhile, which take the lowest free numbers, so
/// the files here sit at numbers of their own, just below the limit.
#[test]
fn a_close_closes_its_descriptor_once() {
    let driver = Driver::new().unwrap();
    let scratch = Scratch::new("close");
    let path = scratch.path("file");
    fs::write(&path, b"x").unwrap();
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: getrlimit writes the one `rlimit` it is given.
    let got = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) };
    assert_eq!(got, 0, "{}", std::io::Error::last_os_error());
    let high = i32::try_from(limit.rlim_cur.min(1 << 20)).unwrap() - 8;
    let open = |at: RawFd| -> OwnedFd {
        let file = fs::File::open(&path).unwrap();
        // SAFETY: F_DUPFD_CLOEXEC makes a new descriptor, the lowest free one
        // from `at` on, which the `OwnedFd` then owns.
        let fd = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_DUPFD_CLOEXEC, at) };
        assert!(fd >= at, "{}", std::io::Error::last_os_error());
        // SAFETY: the descriptor is new, and nothing else owns it.
        unsafe { std::os::fd::FromRawFd::from_raw_fd(fd) }
    };

    let file = open(high);
    let raw = file.as_raw_fd();
    block_on(&driver, driver.op(Close::new(file))).unwrap();
    assert!(!is_open(raw));

    let file = open(high + 1);
    let raw = file.as_raw_fd();
    drop(driver.op(Close::new(file)));
    assert!(!is_open(raw), "dropped before it was queued, it closes");

    let file = open(high + 2);
    let raw = file.as_raw_fd();
    let mut close = driver.op(Close::new(file));
    assert!(poll_once(&mut close).is_pending());
    driver.submit().unwrap();
    assert!(!is_open(raw));
    let reused = open(raw);
    assert_eq!(reused.as_raw_fd(), raw, "the number is reused");
    drop(close);
    settle(&driver);
    assert!(is_open(reused.as_raw_fd()), "the reused number stays open");
}

/// A descriptor closed in order stays open for the operations queued before
/// the close, and is closed once the kernel has taken them.
#[test]
fn a_descriptor_closed_in_order_outlasts_what_was_queued_before() {
    let driver = Driver::new().unwrap();
    let (mut reader, writer) = std::io::pipe().unwrap();
    let writer = OwnedFd::from(writer);
    let raw = writer.as_raw_fd();
    let mut write = driver.op(Write::new(raw, b"queued".to_vec()));
    assert!(poll_once(&mut write).is_pending());
    driver.close_in_order(writer);
    assert!(is_open(raw), "the close waits for the next submission");

    let (written, _) = block_on(&driver, write);
    assert_eq!(written.unwrap(), 6);
    // Without a writer left, the pipe ends after the data; with one, the
    // read fails with `EAGAIN` rather than block.
    // SAFETY: F_SETFL changes the flags of the reader, which the test owns.
    let set = unsafe { libc::fcntl(reader.as_raw_fd(), libc::F_SETFL, libc::O_NONBLOCK) };
    assert_eq!(set, 0, "{}", std::io::Error::last_os_error());
    let mut data = Vec::new();
    reader.read_to_end(&mut data).unwrap();
    assert_eq!(data, b"queued");
}
