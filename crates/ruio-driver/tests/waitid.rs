//! Waiting for child processes through the ring.
//!
//! Tests run at once in one process, so each waits for its own children
//! only, by process ID, pidfd or process group: a wait for any child could
//! take another test's.

#![allow(
    clippy::zombie_processes,
    reason = "the tests reap their children through the ring, which the lint cannot see"
)]

mod support;

use std::io::{self, ErrorKind};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::process::{Child, Command, ExitStatus};

use ruio_driver::Driver;
use ruio_driver::ops::Waitid;

use support::{block_on, poll_once, settle};

/// Runs `script` in a shell, as a child of the process.
fn shell(script: &str) -> Child {
    Command::new("sh").args(["-c", script]).spawn().unwrap()
}

/// A pidfd for the child with process ID `pid`.
fn pidfd(pid: u32) -> OwnedFd {
    // SAFETY: pidfd_open takes a process ID and flags, and touches no memory.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    assert!(fd >= 0, "pidfd_open: {}", io::Error::last_os_error());
    // SAFETY: the descriptor is new, and nothing else owns it.
    unsafe { OwnedFd::from_raw_fd(i32::try_from(fd).unwrap()) }
}

/// Whether the child with process ID `pid` is still waitable: not reaped.
fn waitable(pid: u32) -> bool {
    let mut info = std::mem::MaybeUninit::<libc::siginfo_t>::zeroed();
    // SAFETY: waitid writes the one `siginfo_t` it is given.
    let waited = unsafe {
        libc::waitid(
            libc::P_PID,
            pid,
            info.as_mut_ptr(),
            libc::WEXITED | libc::WSTOPPED | libc::WCONTINUED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    waited == 0
}

#[test]
fn a_wait_reports_the_exit_and_reaps_the_child() {
    let driver = Driver::new().unwrap();
    let child = shell("exit 3");
    let pid = child.id();
    let state = block_on(&driver, driver.op(Waitid::pid(pid)))
        .unwrap()
        .unwrap();
    assert_eq!(
        (state.pid(), state.exit_code(), state.signal()),
        (pid, Some(3), None)
    );
    // SAFETY: getuid reads the caller's user ID.
    assert_eq!(state.uid(), unsafe { libc::getuid() });
    assert!(ExitStatus::from_raw(state.wait_status()).code() == Some(3));
    assert!(!waitable(pid), "reaped");
    let err = block_on(&driver, driver.op(Waitid::pid(pid))).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ECHILD));
}

/// A wait in flight completes as the child it waits for is killed, the
/// ring parked meanwhile.
#[test]
fn a_wait_in_flight_completes_as_the_child_is_killed() {
    let driver = Driver::new().unwrap();
    let mut child = shell("sleep 30");
    let pid = child.id();
    let mut wait = driver.op(Waitid::pid(pid));
    assert!(poll_once(&mut wait).is_pending());
    driver.submit().unwrap();
    assert_eq!(driver.in_flight(), 1, "waiting for the child");
    child.kill().unwrap();
    let state = block_on(&driver, wait).unwrap().unwrap();
    assert_eq!(state.code(), libc::CLD_KILLED);
    assert_eq!(state.signal(), Some(libc::SIGKILL));
    let status = ExitStatus::from_raw(state.wait_status());
    assert_eq!(status.signal(), Some(libc::SIGKILL));
}

/// With `WNOWAIT` the child stays waitable, and the standard library reaps
/// it with the same status.
#[test]
fn a_wait_with_wnowait_leaves_the_child_to_reap() {
    let driver = Driver::new().unwrap();
    let mut child = shell("exit 5");
    let pid = child.id();
    let peek = Waitid::pid(pid).options(libc::WEXITED | libc::WNOWAIT);
    let state = block_on(&driver, driver.op(peek)).unwrap().unwrap();
    assert_eq!(state.exit_code(), Some(5));
    assert!(waitable(pid));
    assert_eq!(child.wait().unwrap().code(), Some(5));
    assert!(!waitable(pid));
}

/// A pidfd names the child, as a process ID does.
#[test]
fn a_wait_by_pidfd_reports_its_child() {
    let driver = Driver::new().unwrap();
    let child = shell("exit 9");
    let pid = child.id();
    let fd = pidfd(pid);
    let state = block_on(&driver, driver.op(Waitid::pidfd(fd.as_raw_fd())))
        .unwrap()
        .unwrap();
    assert_eq!((state.pid(), state.exit_code()), (pid, Some(9)));
}

/// A process group names the children in it.
#[test]
fn a_wait_by_process_group_reports_a_child_in_it() {
    let driver = Driver::new().unwrap();
    let child = Command::new("sh")
        .args(["-c", "exit 7"])
        .process_group(0)
        .spawn()
        .unwrap();
    let pid = child.id();
    // The child leads a group of its own, whose ID is its process ID.
    let state = block_on(&driver, driver.op(Waitid::group(pid)))
        .unwrap()
        .unwrap();
    assert_eq!((state.pid(), state.exit_code()), (pid, Some(7)));
}

/// With `WNOHANG`, a child that has not changed is reported as none.
#[test]
fn a_wait_with_wnohang_reports_no_change_as_none() {
    let driver = Driver::new().unwrap();
    let mut child = shell("sleep 30");
    let pid = child.id();
    let now = Waitid::pid(pid).options(libc::WEXITED | libc::WNOHANG);
    assert!(block_on(&driver, driver.op(now)).unwrap().is_none());
    child.kill().unwrap();
    assert_eq!(child.wait().unwrap().signal(), Some(libc::SIGKILL));
}

/// A wait dropped in flight is cancelled, and leaves the child to reap.
#[test]
fn a_wait_dropped_in_flight_is_cancelled() {
    let driver = Driver::new().unwrap();
    let mut child = shell("sleep 30");
    let pid = child.id();
    let mut wait = driver.op(Waitid::pid(pid));
    assert!(poll_once(&mut wait).is_pending());
    driver.submit().unwrap();
    drop(wait);
    settle(&driver);
    assert!(waitable(pid));
    child.kill().unwrap();
    assert_eq!(child.wait().unwrap().signal(), Some(libc::SIGKILL));
}

/// Options that make no sense are refused.
#[test]
fn a_wait_for_nothing_is_refused() {
    let driver = Driver::new().unwrap();
    let child = shell("exit 0");
    let none = Waitid::pid(child.id()).options(libc::WNOHANG);
    let err = block_on(&driver, driver.op(none)).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
    let state = block_on(&driver, driver.op(Waitid::pid(child.id())))
        .unwrap()
        .unwrap();
    assert_eq!(state.exit_code(), Some(0));
}
