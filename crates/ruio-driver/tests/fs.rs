//! File and file-system operations, against files in a scratch directory.

mod support;

use std::fs;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::path::PathBuf;

use ruio_driver::ops::{
    Close, Fallocate, Fsync, Ftruncate, MkdirAt, OpenAt, Read, RenameAt, Statx, UnlinkAt, Write,
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
