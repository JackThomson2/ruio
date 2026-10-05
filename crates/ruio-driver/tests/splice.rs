//! Splice, tee and poll, against pipes and files.

mod support;

use std::env;
use std::fs::{self, File};
use std::future::poll_fn;
use std::io::{self, Read as _, Write as _};
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::time::Duration;

use ruio_driver::ops::{OpenAt, PollAdd, PollMulti, PollStream, Splice, Tee};
use ruio_driver::{Descriptor, Driver};

use support::{block_on, poll_once, settle};

/// A file in the temporary directory, removed when dropped.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str, contents: &[u8]) -> (Self, File) {
        let path = env::temp_dir().join(format!("ruio-splice-{}-{name}", std::process::id()));
        let mut file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        file.write_all(contents).unwrap();
        (Self(path), file)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

/// Reads what the pipe holds, up to 64 bytes.
fn drain(pipe: &mut io::PipeReader) -> Vec<u8> {
    let mut buf = [0; 64];
    let read = pipe.read(&mut buf).unwrap();
    buf[..read].to_vec()
}

/// Lets the kernel run for a while, completing what it can.
fn idle(driver: &Driver) {
    driver.park(Some(Duration::from_millis(20))).unwrap();
}

#[test]
fn bytes_splice_from_one_pipe_into_another() {
    let driver = Driver::new().unwrap();
    let (input, mut feed) = io::pipe().unwrap();
    let (mut output, sink) = io::pipe().unwrap();
    feed.write_all(b"hello world").unwrap();

    let splice = Splice::new(input.as_raw_fd(), sink.as_raw_fd(), 5);
    assert_eq!(block_on(&driver, driver.op(splice)).unwrap(), 5);
    assert_eq!(drain(&mut output), b"hello");

    // Asking for more than the pipe holds moves what it holds.
    let splice = Splice::new(input.as_raw_fd(), sink.as_raw_fd(), 64);
    assert_eq!(block_on(&driver, driver.op(splice)).unwrap(), 6);
    assert_eq!(drain(&mut output), b" world");

    // An empty pipe whose writer is gone is at its end.
    drop(feed);
    let splice = Splice::new(input.as_raw_fd(), sink.as_raw_fd(), 64);
    assert_eq!(block_on(&driver, driver.op(splice)).unwrap(), 0);
}

#[test]
fn a_file_splices_into_a_pipe_from_an_offset() {
    let driver = Driver::new().unwrap();
    let (_scratch, file) = Scratch::new("from-offset", b"0123456789abcdef");
    let (mut output, sink) = io::pipe().unwrap();

    let splice = Splice::new(file.as_raw_fd(), sink.as_raw_fd(), 6).offset_in(4);
    assert_eq!(block_on(&driver, driver.op(splice)).unwrap(), 6);
    assert_eq!(drain(&mut output), b"456789");
}

#[test]
fn a_pipe_splices_into_a_file_at_an_offset() {
    let driver = Driver::new().unwrap();
    let (scratch, file) = Scratch::new("at-offset", b"0123456789");
    let (input, mut feed) = io::pipe().unwrap();
    feed.write_all(b"XYZ").unwrap();

    let splice = Splice::new(input.as_raw_fd(), file.as_raw_fd(), 3).offset_out(2);
    assert_eq!(block_on(&driver, driver.op(splice)).unwrap(), 3);
    assert_eq!(fs::read(&scratch.0).unwrap(), b"01XYZ56789");
}

#[test]
fn a_pipe_has_no_offset() {
    let driver = Driver::new().unwrap();
    let (input, mut feed) = io::pipe().unwrap();
    let (_output, sink) = io::pipe().unwrap();
    feed.write_all(b"data").unwrap();

    let splice = Splice::new(input.as_raw_fd(), sink.as_raw_fd(), 4).offset_in(0);
    let err = block_on(&driver, driver.op(splice)).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ESPIPE));
}

#[test]
fn a_tee_duplicates_a_pipe_without_draining_it() {
    let driver = Driver::new().unwrap();
    let (mut input, mut feed) = io::pipe().unwrap();
    let (mut output, sink) = io::pipe().unwrap();
    feed.write_all(b"copy me").unwrap();

    let tee = Tee::new(input.as_raw_fd(), sink.as_raw_fd(), 64);
    assert_eq!(block_on(&driver, driver.op(tee)).unwrap(), 7);
    assert_eq!(drain(&mut output), b"copy me");
    assert_eq!(drain(&mut input), b"copy me");
}

#[test]
fn a_poll_completes_once_the_pipe_is_readable() {
    let driver = Driver::new().unwrap();
    let (reader, mut writer) = io::pipe().unwrap();

    let mut poll = driver.op(PollAdd::new(reader.as_raw_fd(), libc::POLLIN as u32));
    assert!(poll_once(&mut poll).is_pending());
    driver.submit().unwrap();
    idle(&driver);
    assert!(
        poll_once(&mut poll).is_pending(),
        "an empty pipe is not readable"
    );

    writer.write_all(b"x").unwrap();
    let events = block_on(&driver, poll).unwrap();
    assert_ne!(events & libc::POLLIN as u32, 0);
}

#[test]
fn a_poll_on_a_writable_pipe_completes_at_once() {
    let driver = Driver::new().unwrap();
    let (_reader, writer) = io::pipe().unwrap();

    let poll = PollAdd::new(writer.as_raw_fd(), libc::POLLOUT as u32);
    let events = block_on(&driver, driver.op(poll)).unwrap();
    assert_ne!(events & libc::POLLOUT as u32, 0);
}

/// One request reports each write into the pipe, as long as the reader
/// drains it; the events come with the request still armed.
#[test]
fn a_multishot_poll_reports_each_wakeup_on_one_request() {
    let driver = Driver::new().unwrap();
    let (mut reader, mut writer) = io::pipe().unwrap();

    let mut events = PollStream::new(
        &driver,
        PollMulti::new(reader.as_raw_fd(), libc::POLLIN as u32),
    );
    assert!(poll_once(&mut poll_fn(|cx| events.poll_next(cx))).is_pending());
    driver.submit().unwrap();
    let before = driver.stats().operations;
    for round in 0..100u8 {
        writer.write_all(&[round]).unwrap();
        let ready = block_on(&driver, poll_fn(|cx| events.poll_next(cx))).unwrap();
        assert_ne!(ready & libc::POLLIN as u32, 0);
        let mut byte = [0];
        reader.read_exact(&mut byte).unwrap();
        assert_eq!(byte, [round]);
    }
    assert_eq!(
        driver.stats().operations,
        before,
        "the hundred events took no further request"
    );
    assert!(events.next_ready().is_none());
    drop(events);
    settle(&driver);
}

/// A poll armed on a file that is ready completes at once, so a stream
/// armed late, or again, misses nothing that is there.
#[test]
fn a_multishot_poll_armed_on_a_ready_file_completes_at_once() {
    let driver = Driver::new().unwrap();
    let (_reader, mut writer) = io::pipe().unwrap();
    writer.write_all(b"already").unwrap();
    let mut events = PollStream::new(
        &driver,
        PollMulti::new(_reader.as_raw_fd(), libc::POLLIN as u32),
    );
    let ready = block_on(&driver, poll_fn(|cx| events.poll_next(cx))).unwrap();
    assert_ne!(ready & libc::POLLIN as u32, 0);
    drop(events);
    settle(&driver);
}

/// The splice waits in the kernel, on the poll, until there is something to
/// move.
#[test]
fn a_poll_linked_before_a_splice_waits_for_the_data() {
    let driver = Driver::new().unwrap();
    let (input, mut feed) = io::pipe().unwrap();
    let (mut output, sink) = io::pipe().unwrap();

    let poll = PollAdd::new(input.as_raw_fd(), libc::POLLIN as u32);
    let splice = Splice::new(input.as_raw_fd(), sink.as_raw_fd(), 64);
    let mut chain = driver.op(poll).link(driver.op(splice));
    assert!(poll_once(&mut chain).is_pending());
    driver.submit().unwrap();
    idle(&driver);
    assert!(poll_once(&mut chain).is_pending());

    feed.write_all(b"linked").unwrap();
    let (events, moved) = block_on(&driver, chain);
    assert_ne!(events.unwrap() & libc::POLLIN as u32, 0);
    assert_eq!(moved.unwrap(), 6);
    assert_eq!(drain(&mut output), b"linked");
}

#[test]
fn a_direct_descriptor_splices_either_way() {
    let driver = Driver::new().unwrap();
    let (scratch, _file) = Scratch::new("direct", b"direct file");
    let open = OpenAt::new(&scratch.0).unwrap().flags(libc::O_RDWR);
    let file = block_on(&driver, driver.op(open.direct(&driver))).unwrap();
    assert!(matches!(file, Descriptor::Direct(_)));

    let (mut output, sink) = io::pipe().unwrap();
    let splice = Splice::new(file.fd(), sink.as_raw_fd(), 6).offset_in(0);
    assert_eq!(block_on(&driver, driver.op(splice)).unwrap(), 6);
    assert_eq!(drain(&mut output), b"direct");

    let (input, mut feed) = io::pipe().unwrap();
    feed.write_all(b" more").unwrap();
    let splice = Splice::new(input.as_raw_fd(), file.fd(), 5).offset_out(11);
    assert_eq!(block_on(&driver, driver.op(splice)).unwrap(), 5);
    assert_eq!(fs::read(&scratch.0).unwrap(), b"direct file more");

    drop(file);
    settle(&driver);
}
