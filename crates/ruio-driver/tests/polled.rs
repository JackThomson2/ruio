//! Polled operations, on the ring that polls its devices for completions,
//! against the kernel's null block device when it has poll queues.

mod support;

use std::fs::{self, File};
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::task::Poll;
use std::time::{Duration, Instant};

use ruio_driver::buf::{AlignedBuf, FixedBuffers};
use ruio_driver::ops::{Read, ReadFixed, Timeout};
use ruio_driver::{Driver, Fd, Probe, monotonic_now};

use support::{block_on, poll_once, settle};

/// The block size the device is loaded with, and the alignment direct I/O
/// on it needs.
const BLOCK: usize = 4096;

/// The null block device, if it is there and its queue can be polled, open
/// for direct I/O unless `direct` is false; `None`, with a message, if the
/// test has nothing to run against.
fn nullb(direct: bool) -> Option<File> {
    if !Probe::current()
        .unwrap()
        .supports_setup_flags(ruio_sys::IORING_SETUP_IOPOLL)
    {
        eprintln!("skipped: the kernel's io_uring has no polled rings");
        return None;
    }
    let polls = fs::read_to_string("/sys/block/nullb0/queue/io_poll")
        .map(|polls| polls.trim() == "1")
        .unwrap_or(false);
    if !polls {
        eprintln!("skipped: no /dev/nullb0 with poll queues");
        return None;
    }
    let flags = if direct { libc::O_DIRECT } else { 0 };
    match fs::OpenOptions::new()
        .read(true)
        .custom_flags(flags)
        .open("/dev/nullb0")
    {
        Ok(file) => Some(file),
        Err(err) => {
            eprintln!("skipped: /dev/nullb0 cannot be opened: {err}");
            None
        }
    }
}

fn block() -> AlignedBuf {
    AlignedBuf::with_capacity(BLOCK, BLOCK)
}

/// A polled read completes through the polled ring, created as it is
/// queued, and a park with nothing else in flight waits for it inside the
/// kernel.
#[test]
fn polled_reads_complete_on_the_polled_ring() {
    let Some(file) = nullb(true) else {
        return;
    };
    let driver = Driver::new().unwrap();
    let mut read = driver.op(Read::at(file.as_raw_fd(), block(), 0).polled(true));
    assert!(poll_once(&mut read).is_pending());
    assert_eq!(driver.in_flight(), 1);
    assert_eq!(driver.stats().polled, 1);
    // Only the polled read is in flight, so the park polls in the kernel.
    driver.park(None).unwrap();
    let Poll::Ready((read, buf)) = poll_once(&mut read) else {
        panic!("the read has not completed");
    };
    assert_eq!(read.unwrap(), BLOCK);
    assert_eq!(buf.len(), BLOCK);
    assert_eq!(driver.in_flight(), 0);
    let stats = driver.stats();
    assert_eq!((stats.operations, stats.polled), (1, 1));
    assert!(stats.enters >= 1);

    // And many at once, through a park with a timeout, which polls a pass
    // at a time.
    let reads: Vec<_> = (0..64)
        .map(|index| {
            let offset = index * BLOCK as u64;
            driver.op(Read::at(file.as_raw_fd(), block(), offset).polled(true))
        })
        .collect();
    for read in reads {
        let (read, _) = block_on(&driver, read);
        assert_eq!(read.unwrap(), BLOCK);
    }
    assert_eq!(driver.in_flight(), 0);
    assert_eq!(driver.stats().polled, 65);
}

/// The kernel takes only direct I/O on the polled ring.
#[test]
fn a_polled_read_needs_direct_io() {
    let Some(file) = nullb(false) else {
        return;
    };
    let driver = Driver::new().unwrap();
    let read = Read::at(file.as_raw_fd(), block(), 0).polled(true);
    let (read, _) = block_on(&driver, driver.op(read));
    assert_eq!(read.unwrap_err().raw_os_error(), Some(libc::EOPNOTSUPP));
    assert_eq!(driver.in_flight(), 0);
}

/// A polled operation takes no linked timeout and joins no chain; the
/// driver refuses either before the kernel sees it.
#[test]
fn polled_operations_take_no_timeout_and_no_link() {
    if !Probe::current()
        .unwrap()
        .supports_setup_flags(ruio_sys::IORING_SETUP_IOPOLL)
    {
        eprintln!("skipped: the kernel's io_uring has no polled rings");
        return;
    }
    let driver = Driver::new().unwrap();
    let timed = driver
        .op(Read::at(-1, block(), 0).polled(true))
        .timeout(Duration::from_secs(1));
    let (read, _) = block_on(&driver, timed);
    let err = read.unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EOPNOTSUPP));
    assert_eq!(err.kind(), std::io::ErrorKind::Unsupported);

    let chain = driver
        .op(Read::at(-1, block(), 0).polled(true))
        .link(driver.op(Read::at(-1, block(), 0)));
    let ((first, _), (second, _)) = block_on(&driver, chain);
    assert_eq!(first.unwrap_err().raw_os_error(), Some(libc::EOPNOTSUPP));
    assert_eq!(second.unwrap_err().raw_os_error(), Some(libc::EOPNOTSUPP));
    assert_eq!(driver.in_flight(), 0);
    assert_eq!(driver.stats().operations, 0);
}

/// Polled reads keep flowing while a timeout of the main ring is in
/// flight, and the timeout completes on time: the park polls a pass at a
/// time and looks at the main ring between passes.
#[test]
fn a_timeout_completes_among_polled_reads() {
    let Some(file) = nullb(true) else {
        return;
    };
    let driver = Driver::new().unwrap();
    let mut timeout = driver.op(Timeout::new(Duration::from_millis(30)));
    assert!(poll_once(&mut timeout).is_pending());
    let start = Instant::now();
    let mut reads = 0;
    while poll_once(&mut timeout).is_pending() {
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "the timeout is late"
        );
        let mut read = driver.op(Read::at(file.as_raw_fd(), block(), 0).polled(true));
        assert!(poll_once(&mut read).is_pending());
        driver.park(None).unwrap();
        let Poll::Ready((read, _)) = poll_once(&mut read) else {
            panic!("a polled read did not complete in a park");
        };
        assert_eq!(read.unwrap(), BLOCK);
        reads += 1;
    }
    assert!(start.elapsed() >= Duration::from_millis(30));
    assert!(reads > 1, "reads completed while the timeout was in flight");
    assert_eq!(driver.in_flight(), 0);
}

/// The wake timer fires while polled reads are in flight, and a park with
/// a timeout returns by it.
#[test]
fn the_wake_timer_fires_among_polled_reads() {
    let Some(file) = nullb(true) else {
        return;
    };
    let driver = Driver::new().unwrap();
    driver
        .wake_at(monotonic_now() + Duration::from_millis(20))
        .unwrap();
    let start = Instant::now();
    while !driver.timer_fired() {
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "the timer is late"
        );
        let read = driver.op(Read::at(file.as_raw_fd(), block(), 0).polled(true));
        let (read, _) = block_on(&driver, read);
        assert_eq!(read.unwrap(), BLOCK);
    }
    assert!(start.elapsed() >= Duration::from_millis(20));

    let mut read = driver.op(Read::at(file.as_raw_fd(), block(), BLOCK as u64).polled(true));
    assert!(poll_once(&mut read).is_pending());
    let start = Instant::now();
    driver.park(Some(Duration::from_millis(50))).unwrap();
    assert!(start.elapsed() < Duration::from_secs(1));
    settle(&driver);
    assert!(poll_once(&mut read).is_ready());
}

/// Registered buffers reach the polled ring whether they are registered
/// before it exists or after.
#[test]
fn polled_reads_fill_registered_buffers() {
    let Some(file) = nullb(true) else {
        return;
    };
    // Registered first, shared as the polled ring is created.
    let driver = Driver::new().unwrap();
    let buffers = FixedBuffers::new(&driver, 2, BLOCK).unwrap();
    let buf = buffers.take().unwrap();
    let read = ReadFixed::at(file.as_raw_fd(), buf, 0).polled(true);
    let (read, buf) = block_on(&driver, driver.op(read));
    assert_eq!(read.unwrap(), BLOCK);
    assert_eq!(buf.len(), BLOCK);
    drop(buf);
    drop(buffers);

    // Registered again, with the polled ring there to share them.
    let buffers = FixedBuffers::new(&driver, 2, BLOCK).unwrap();
    let buf = buffers.take().unwrap();
    let read = ReadFixed::at(file.as_raw_fd(), buf, BLOCK as u64).polled(true);
    let (read, _) = block_on(&driver, driver.op(read));
    assert_eq!(read.unwrap(), BLOCK);

    // And on a driver whose polled ring comes first.
    let driver = Driver::builder().polled(true).build().unwrap();
    let buffers = FixedBuffers::new(&driver, 1, BLOCK).unwrap();
    let buf = buffers.take().unwrap();
    let read = ReadFixed::at(file.as_raw_fd(), buf, 0).polled(true);
    let (read, _) = block_on(&driver, driver.op(read));
    assert_eq!(read.unwrap(), BLOCK);
    assert_eq!(driver.stats().polled, 1);
}

/// A polled read whose future is dropped completes unseen, with no
/// cancellation, and the driver waits for it before it closes.
#[test]
fn dropped_polled_reads_complete_unseen() {
    let Some(file) = nullb(true) else {
        return;
    };
    let driver = Driver::new().unwrap();
    for _ in 0..8 {
        let mut read = driver.op(Read::at(file.as_raw_fd(), block(), 0).polled(true));
        assert!(poll_once(&mut read).is_pending());
        drop(read);
    }
    assert_eq!(driver.in_flight(), 8);
    settle(&driver);
    assert_eq!(driver.in_flight(), 0);
    assert_eq!(driver.stats().polled, 8);

    let mut read = driver.op(Read::at(file.as_raw_fd(), block(), 0).polled(true));
    assert!(poll_once(&mut read).is_pending());
    drop(read);
    // Dropped in flight: the driver's drop waits for the completion.
    let start = Instant::now();
    drop(driver);
    assert!(start.elapsed() < Duration::from_millis(500));
}

/// A file installed into the polled ring's table reads as a direct
/// descriptor there, beside its plain descriptor; dropping the descriptor
/// frees the slot after the reads queued before.
#[test]
fn polled_files_install_as_direct_descriptors() {
    let Some(file) = nullb(true) else {
        return;
    };
    let driver = Driver::builder().files(4).build().unwrap();
    let polled = block_on(&driver, driver.install_polled(file.as_raw_fd())).unwrap();
    assert!(polled.belongs_to(&driver));
    assert_eq!(driver.stats().polled, 1);
    let read = Read::at(polled.fd(), block(), 0).polled(true);
    let (read, buf) = block_on(&driver, driver.op(read));
    assert_eq!(read.unwrap(), BLOCK);
    assert_eq!(buf.len(), BLOCK);
    // The plain descriptor reads on, on either ring.
    let read = Read::at(file.as_raw_fd(), block(), 0).polled(true);
    assert_eq!(block_on(&driver, driver.op(read)).0.unwrap(), BLOCK);
    let read = Read::at(file.as_raw_fd(), block(), 0);
    assert_eq!(block_on(&driver, driver.op(read)).0.unwrap(), BLOCK);

    // A read queued before the drop still finds the file in the slot.
    let slot = polled.slot();
    let mut queued = driver.op(Read::at(polled.fd(), block(), 0).polled(true));
    assert!(poll_once(&mut queued).is_pending());
    drop(polled);
    let (read, _) = block_on(&driver, queued);
    assert_eq!(read.unwrap(), BLOCK);
    settle(&driver);
    let read = Read::at(Fd::Direct(slot), block(), 0).polled(true);
    let err = block_on(&driver, driver.op(read)).0.unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EBADF), "the slot is free");

    // The table has four slots, which installing fills and freeing empties.
    let installed: Vec<_> = (0..4)
        .map(|_| block_on(&driver, driver.install_polled(file.as_raw_fd())).unwrap())
        .collect();
    let err = block_on(&driver, driver.install_polled(file.as_raw_fd())).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENFILE));
    drop(installed);
    block_on(&driver, driver.install_polled(file.as_raw_fd())).unwrap();

    // Without a table there is nowhere to install.
    let driver = Driver::builder().files(0).build().unwrap();
    let err = block_on(&driver, driver.install_polled(file.as_raw_fd())).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENXIO));
}

/// The polled ring can be asked for ahead of any operation, and is sized
/// as the main ring.
#[test]
fn the_polled_ring_can_be_made_ahead() {
    if !Probe::current()
        .unwrap()
        .supports_setup_flags(ruio_sys::IORING_SETUP_IOPOLL)
    {
        eprintln!("skipped: the kernel's io_uring has no polled rings");
        return;
    }
    let driver = Driver::builder().entries(4).build().unwrap();
    driver.enable_polling().unwrap();
    driver.enable_polling().unwrap();
    assert_eq!(driver.stats().enters, 0);
    assert_eq!(driver.sq_entries(), 4);
}
