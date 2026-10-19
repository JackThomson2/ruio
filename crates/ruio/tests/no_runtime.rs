//! Portable I/O polled where no runtime runs and no foreign ring is set, as
//! none is in this test binary: what would wait on a ring fails with
//! `ENOSYS`, whose kind is `Unsupported`, rather than panicking, so that the
//! caller can do its I/O another way.

use std::io::{self, ErrorKind};
use std::os::unix::net::UnixStream as StdUnixStream;
use std::process::Stdio;

use ruio::net::portable::UnixStream;
use ruio::process::Command;
use ruio::signal::{SignalKind, ctrl_c, signal};
use ruio::uring::PortableFd;

fn unsupported<T>(result: io::Result<T>) -> bool {
    matches!(result, Err(err) if err.kind() == ErrorKind::Unsupported && err.raw_os_error() == Some(libc::ENOSYS))
}

#[test]
fn portable_io_with_no_runtime_and_no_foreign_ring_is_unsupported() {
    assert!(ruio::uring::foreign_ring().is_none());
    futures_executor::block_on(async {
        assert!(unsupported(ruio::fs::read("/").await));
        assert!(unsupported(ruio::fs::metadata("/").await));

        // What has arrived is read with no ring; only a wait needs one.
        let (ours, theirs) = StdUnixStream::pair().unwrap();
        let ours = UnixStream::from_std(ours).unwrap();
        std::io::Write::write_all(&mut &theirs, b"x").unwrap();
        let (read, buf) = ours.read(Vec::with_capacity(4)).await;
        assert_eq!(&buf[..read.unwrap()], b"x");
        let (read, _) = ours.read(Vec::with_capacity(4)).await;
        assert!(unsupported(read));

        let fd = PortableFd::new(theirs.into());
        assert!(unsupported(fd.ready(libc::POLLOUT as u32).await));

        let mut child = Command::new("sleep")
            .arg("0.01")
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        assert!(unsupported(child.wait().await));
        // Reaped, once it exits, as a child dropped unwaited for is.
        drop(child);
    });
}

/// A listener needs a ring to wait on: where none could, none is made, and
/// no handler is installed, so the interrupt keeps its default action.
#[test]
fn a_signal_listener_with_no_runtime_and_no_foreign_ring_is_unsupported() {
    let err = signal(SignalKind::user_defined1()).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    let err = futures_executor::block_on(ctrl_c()).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    // SAFETY: a zeroed `sigaction` is a valid place for the current one.
    let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
    // SAFETY: `action` is a valid place for the current action; none is
    // given.
    let asked = unsafe { libc::sigaction(libc::SIGINT, std::ptr::null(), &raw mut action) };
    assert_eq!(asked, 0);
    assert_eq!(action.sa_sigaction, libc::SIG_DFL, "no handler installed");
}
