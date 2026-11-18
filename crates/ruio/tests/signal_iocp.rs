//! The console's control events delivered to tasks, on Windows.
//!
//! An event goes to every process attached to the console, cargo among
//! them, so each test sends its events in a child of its own: this test
//! binary again, spawned with a console of its own, which has no window,
//! to run a test of its own that does nothing unless the environment says
//! it is that child.

#![cfg(ruio_iocp)]

mod support;

use std::io;
use std::process::Output;
use std::time::Duration;

use ruio::process::Command;
use ruio::signal::{SignalKind, ctrl_c, signal};
use ruio::time::timeout;

use support::within;

/// `CREATE_NO_WINDOW`: the child gets a console of its own, with no window.
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

/// `CREATE_NEW_PROCESS_GROUP`: the child leads a process group of its own.
const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;

/// `STATUS_CONTROL_C_EXIT`: how a process the system ended for a control
/// event exits.
const STATUS_CONTROL_C_EXIT: u32 = 0xC000_013A;

/// The variable that names the test a child is to run.
const CHILD: &str = "RUIO_SIGNAL_CHILD";

/// How long an event sent may take to reach a listener.
const SOON: Duration = Duration::from_millis(500);

#[link(name = "kernel32")]
unsafe extern "system" {
    fn GenerateConsoleCtrlEvent(event: u32, group: u32) -> i32;
    fn SetConsoleCtrlHandler(
        handler: Option<unsafe extern "system" fn(u32) -> i32>,
        add: i32,
    ) -> i32;
}

/// Sends `kind` to every process attached to the console: in a child, the
/// child alone.
fn send(kind: SignalKind) {
    // SAFETY: the call takes two numbers, and touches no memory of ours.
    let sent = unsafe { GenerateConsoleCtrlEvent(kind.as_raw_value(), 0) };
    assert_ne!(sent, 0, "{}", io::Error::last_os_error());
}

/// Whether this process is the child that runs the test `name`.
fn is_child(name: &str) -> bool {
    std::env::var(CHILD).is_ok_and(|child| child == name)
}

/// Runs the test `name` of this binary in a child with a console of its
/// own, spawned with `flags` besides: how it exited, and what it wrote.
async fn run_child(name: &str, flags: u32) -> Output {
    let exe = std::env::current_exe().unwrap();
    let output = Command::new(exe)
        .args(["--exact", name, "--nocapture", "--test-threads=1"])
        .env(CHILD, name)
        .creation_flags(CREATE_NO_WINDOW | flags)
        .output();
    within(output).await.unwrap()
}

/// Asserts that the child exited as `code` says, showing what it wrote
/// if it did not.
fn assert_exited(output: &Output, code: i32) {
    assert_eq!(
        output.status.code(),
        Some(code),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[ruio::test]
async fn ctrl_c_and_ctrl_break_reach_their_listeners() {
    let output = run_child("child_receives_ctrl_c_and_ctrl_break", 0).await;
    assert_exited(&output, 0);
}

#[test]
fn child_receives_ctrl_c_and_ctrl_break() {
    if !is_child("child_receives_ctrl_c_and_ctrl_break") {
        return;
    }
    // Ctrl+C is ignored where the parent ignored it, as a harness that
    // runs the tests in a process group of its own does; not here.
    // SAFETY: no handler is given, which turns ignoring Ctrl+C off.
    assert_ne!(unsafe { SetConsoleCtrlHandler(None, 0) }, 0);
    ruio::Runtime::new().unwrap().block_on(async {
        // Listening from its first poll, before the event is sent.
        let waiting = ruio::spawn(ctrl_c());
        for _ in 0..3 {
            ruio::yield_now().await;
        }
        send(SignalKind::interrupt());
        within(timeout(SOON, waiting))
            .await
            .expect("Ctrl+C is delivered")
            .unwrap()
            .unwrap();
        let mut breaks = signal(SignalKind::ctrl_break()).unwrap();
        for _ in 0..2 {
            send(SignalKind::ctrl_break());
            within(timeout(SOON, breaks.recv()))
                .await
                .expect("Ctrl+Break is delivered")
                .unwrap();
        }
    });
}

/// With no listener for it, an event goes on to the system, which ends the
/// process as it would with no handler of ruio's.
#[ruio::test]
async fn an_event_with_no_listener_ends_the_process() {
    let output = run_child("child_is_ended_by_ctrl_break", 0).await;
    assert_exited(&output, STATUS_CONTROL_C_EXIT.cast_signed());
}

#[test]
fn child_is_ended_by_ctrl_break() {
    if !is_child("child_is_ended_by_ctrl_break") {
        return;
    }
    // The handler is installed, and has no listener left.
    drop(signal(SignalKind::ctrl_break()).unwrap());
    send(SignalKind::ctrl_break());
    std::thread::sleep(Duration::from_secs(5));
    panic!("the process outlived the event");
}

/// A child that leads a process group of its own ignores Ctrl+C, and
/// receives Ctrl+Break.
#[ruio::test]
async fn a_group_of_its_own_receives_ctrl_break_alone() {
    let output = run_child("child_ignores_ctrl_c", CREATE_NEW_PROCESS_GROUP).await;
    assert_exited(&output, 0);
}

#[test]
fn child_ignores_ctrl_c() {
    if !is_child("child_ignores_ctrl_c") {
        return;
    }
    ruio::Runtime::new().unwrap().block_on(async {
        let mut interrupts = signal(SignalKind::interrupt()).unwrap();
        let mut breaks = signal(SignalKind::ctrl_break()).unwrap();
        send(SignalKind::interrupt());
        send(SignalKind::ctrl_break());
        within(timeout(SOON, breaks.recv()))
            .await
            .expect("Ctrl+Break is delivered")
            .unwrap();
        assert!(
            timeout(SOON, interrupts.recv()).await.is_err(),
            "Ctrl+C is ignored"
        );
    });
}
