//! Readiness and I/O on descriptors of any kind, through `AsyncFd`.

mod support;

use std::io::{self, ErrorKind, Read as _, Write as _};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::thread;
use std::time::{Duration, Instant};

use ruio::io::{AsyncFd, AsyncRead, AsyncWrite};

use support::within;

/// An eventfd, whose count a write adds to and a read takes.
fn eventfd() -> OwnedFd {
    // SAFETY: `eventfd` makes a new descriptor, which nothing else owns.
    let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    assert!(fd >= 0, "{}", io::Error::last_os_error());
    // SAFETY: the descriptor is new and owned by nobody else.
    unsafe { OwnedFd::from_raw_fd(fd) }
}

/// A timerfd that expires every `period`, from `period` on.
fn timerfd(period: Duration) -> OwnedFd {
    // SAFETY: `timerfd_create` makes a new descriptor, which nothing else
    // owns.
    let fd = unsafe { libc::timerfd_create(libc::CLOCK_MONOTONIC, libc::TFD_CLOEXEC) };
    assert!(fd >= 0, "{}", io::Error::last_os_error());
    let spec = libc::timespec {
        tv_sec: period.as_secs() as libc::time_t,
        tv_nsec: libc::c_long::from(period.subsec_nanos()),
    };
    let value = libc::itimerspec {
        it_interval: spec,
        it_value: spec,
    };
    // SAFETY: `value` is a valid itimerspec, and the old value is not asked
    // for.
    let armed = unsafe { libc::timerfd_settime(fd, 0, &value, std::ptr::null_mut()) };
    assert_eq!(armed, 0, "{}", io::Error::last_os_error());
    // SAFETY: as above.
    unsafe { OwnedFd::from_raw_fd(fd) }
}

#[test]
fn readable_waits_until_the_descriptor_has_something() {
    ruio::Runtime::new().unwrap().block_on(async {
        let (reader, mut writer) = io::pipe().unwrap();
        let reader = AsyncFd::new(reader);
        let waited = ruio::time::timeout(Duration::from_millis(20), reader.readable()).await;
        assert!(waited.is_err(), "an empty pipe is not readable");
        writer.write_all(b"now").unwrap();
        let events = within(reader.readable()).await.unwrap();
        assert_ne!(events & libc::POLLIN as u32, 0);
        let mut buf = [0; 3];
        reader.get_ref().read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"now");
    });
}

#[test]
fn writable_is_ready_at_once_on_a_fresh_pipe_and_waits_on_a_full_one() {
    ruio::Runtime::new().unwrap().block_on(async {
        let (mut reader, writer) = io::pipe().unwrap();
        let writer = AsyncFd::new(writer);
        let events = within(writer.writable()).await.unwrap();
        assert_ne!(events & libc::POLLOUT as u32, 0);

        // Filled to the brim: the pipe takes no more.
        let fd = writer.as_raw_fd();
        // SAFETY: `fcntl` with these commands touches no memory.
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        // SAFETY: as above.
        let set = unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
        assert_eq!(set, 0);
        let chunk = vec![1u8; 64 << 10];
        let mut filled = 0;
        loop {
            match writer.get_ref().write(&chunk) {
                Ok(written) => filled += written,
                Err(err) if err.kind() == ErrorKind::WouldBlock => break,
                Err(err) => panic!("{err}"),
            }
        }
        let waited = ruio::time::timeout(Duration::from_millis(20), writer.writable()).await;
        assert!(waited.is_err(), "a full pipe is not writable");
        let drained = thread::spawn(move || {
            let mut buf = vec![0; filled];
            reader.read_exact(&mut buf).unwrap();
            reader
        });
        let events = within(writer.writable()).await.unwrap();
        assert_ne!(events & libc::POLLOUT as u32, 0);
        drained.join().unwrap();
    });
}

#[test]
fn ready_reports_the_events_the_descriptor_has() {
    ruio::Runtime::new().unwrap().block_on(async {
        let (reader, writer) = io::pipe().unwrap();
        let reader = AsyncFd::new(reader);
        drop(writer);
        let events = within(reader.ready((libc::POLLIN | libc::POLLHUP) as u32))
            .await
            .unwrap();
        assert_ne!(events & libc::POLLHUP as u32, 0, "the writer is gone");
    });
}

#[test]
fn an_eventfd_signalled_from_another_thread_wakes_the_task() {
    ruio::Runtime::new().unwrap().block_on(async {
        let event = AsyncFd::new(eventfd());
        let fd = event.get_ref().as_raw_fd();
        let signaller = thread::spawn(move || {
            thread::sleep(Duration::from_millis(30));
            // SAFETY: a borrowed descriptor the test keeps open until the
            // thread is joined.
            let fd = unsafe { std::os::fd::BorrowedFd::borrow_raw(fd) };
            let bytes = 5u64.to_ne_bytes();
            // SAFETY: eight bytes, which an eventfd write takes.
            let written = unsafe { libc::write(fd.as_raw_fd(), bytes.as_ptr().cast(), 8) };
            assert_eq!(written, 8);
        });
        let start = Instant::now();
        within(event.readable()).await.unwrap();
        assert!(start.elapsed() >= Duration::from_millis(20));
        let (read, buf) = within(AsyncRead::read(&event, Vec::with_capacity(8))).await;
        assert_eq!(read.unwrap(), 8);
        assert_eq!(u64::from_ne_bytes(buf[..8].try_into().unwrap()), 5);
        signaller.join().unwrap();
    });
}

#[test]
fn a_timerfd_ticks_through_readiness() {
    ruio::Runtime::new().unwrap().block_on(async {
        let timer = AsyncFd::new(timerfd(Duration::from_millis(5)));
        let start = Instant::now();
        let mut expiries = 0;
        for _ in 0..5 {
            within(timer.readable()).await.unwrap();
            let (read, buf) = within(AsyncRead::read(&timer, Vec::with_capacity(8))).await;
            assert_eq!(read.unwrap(), 8);
            expiries += u64::from_ne_bytes(buf[..8].try_into().unwrap());
        }
        assert!(expiries >= 5);
        assert!(
            start.elapsed() >= Duration::from_millis(20),
            "{:?}",
            start.elapsed()
        );
    });
}

/// One request reports every expiry of a timer; the stream takes the count
/// after each.
#[test]
fn readiness_reports_each_wakeup_on_one_request() {
    ruio::Runtime::new().unwrap().block_on(async {
        let timer = AsyncFd::new(timerfd(Duration::from_millis(2)));
        let mut readiness = timer.readiness(libc::POLLIN as u32);
        let before = ruio::driver().stats().operations;
        let mut expiries = 0;
        let mut items = 0;
        while expiries < 20 {
            let events = within(readiness.next()).await.unwrap();
            assert_ne!(events & libc::POLLIN as u32, 0);
            items += 1;
            let (read, buf) = within(AsyncRead::read(&timer, Vec::with_capacity(8))).await;
            assert_eq!(read.unwrap(), 8);
            expiries += u64::from_ne_bytes(buf[..8].try_into().unwrap());
        }
        assert!(items >= 10, "{items} items for {expiries} expiries");
        // One read per item went through the ring; the readiness itself took
        // one request, or two if the kernel ended it once meanwhile.
        let requests = ruio::driver().stats().operations - before - items;
        assert!(requests <= 2, "{requests} requests for {items} items");
        assert!(readiness.next_ready().is_none());
    });
}

#[test]
fn readiness_dropped_and_made_again_misses_nothing_that_is_there() {
    ruio::Runtime::new().unwrap().block_on(async {
        let (reader, mut writer) = io::pipe().unwrap();
        let reader = AsyncFd::new(reader);
        {
            let mut readiness = reader.readiness(libc::POLLIN as u32);
            writer.write_all(b"a").unwrap();
            within(readiness.next()).await.unwrap();
        }
        // The byte is still there: a new stream reports it at once.
        let mut readiness = reader.readiness(libc::POLLIN as u32);
        within(readiness.next()).await.unwrap();
        let mut buf = [0; 1];
        reader.get_ref().read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"a");
    });
}

#[test]
fn the_traits_read_and_write_the_descriptor_through_the_ring() {
    ruio::Runtime::new().unwrap().block_on(async {
        let (reader, writer) = io::pipe().unwrap();
        let (reader, writer) = (AsyncFd::new(reader), AsyncFd::new(writer));
        let data: Vec<u8> = (0..200_000u32).map(|i| (i % 253) as u8).collect();
        let feeder = ruio::spawn({
            let data = data.clone();
            async move {
                writer.write_all(data).await.0.unwrap();
                writer.into_inner()
            }
        });
        let (read, buf) = within(reader.read_exact(Vec::with_capacity(data.len()))).await;
        read.unwrap();
        assert_eq!(buf, data);
        drop(feeder.await.unwrap());
        let (read, buf) = within(AsyncRead::read(&reader, Vec::with_capacity(16))).await;
        assert_eq!(read.unwrap(), 0, "the writer is gone");
        assert!(buf.is_empty());
    });
}

#[test]
fn copy_runs_between_async_fds() {
    ruio::Runtime::new().unwrap().block_on(async {
        let (source_read, mut source_write) = io::pipe().unwrap();
        let (mut sink_read, sink_write) = io::pipe().unwrap();
        let data: Vec<u8> = (0..300_000u32).map(|i| (i % 241) as u8).collect();
        let feeder = thread::spawn({
            let data = data.clone();
            move || {
                source_write.write_all(&data).unwrap();
                drop(source_write);
            }
        });
        let drainer = thread::spawn(move || {
            let mut out = Vec::new();
            sink_read.read_to_end(&mut out).unwrap();
            out
        });
        let (from, to) = (AsyncFd::new(source_read), AsyncFd::new(sink_write));
        let copied = within(ruio::io::copy(&from, &to)).await.unwrap();
        assert_eq!(copied, data.len() as u64);
        drop(to);
        feeder.join().unwrap();
        assert_eq!(drainer.join().unwrap(), data);
    });
}

#[test]
fn a_wait_with_a_timeout_ends_on_time() {
    ruio::Runtime::new().unwrap().block_on(async {
        let (reader, _writer) = io::pipe().unwrap();
        let reader = AsyncFd::new(reader);
        let start = Instant::now();
        let err = within(reader.readable().timeout(Duration::from_millis(30)))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::TimedOut);
        assert!(start.elapsed() >= Duration::from_millis(25));
    });
}

#[test]
fn the_wrapper_can_be_made_before_the_runtime_runs() {
    let (reader, mut writer) = io::pipe().unwrap();
    let reader = AsyncFd::new(reader);
    writer.write_all(b"early").unwrap();
    ruio::Runtime::new().unwrap().block_on(async {
        within(reader.readable()).await.unwrap();
        let (read, buf) = within(AsyncRead::read(&reader, Vec::with_capacity(5))).await;
        assert_eq!(read.unwrap(), 5);
        assert_eq!(buf, b"early");
    });
    let mut inner = reader.into_inner();
    writer.write_all(b"x").unwrap();
    let mut byte = [0; 1];
    inner.read_exact(&mut byte).unwrap();
    assert_eq!(&byte, b"x");
}
