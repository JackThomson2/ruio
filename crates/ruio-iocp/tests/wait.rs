//! Waits for handles to be signaled, polled by a runtime's tasks.
#![cfg(windows)]

use std::future::poll_fn;
use std::os::windows::io::AsHandle;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use ruio_executor::{Runtime, spawn};
use ruio_iocp::portable::HandleWait;

/// A thread that exits once told to, whose handle is signaled then.
fn thread_to_end() -> (thread::JoinHandle<()>, mpsc::Sender<()>) {
    let (end, ending) = mpsc::channel::<()>();
    let thread = thread::spawn(move || {
        let _ = ending.recv();
    });
    (thread, end)
}

#[test]
fn a_task_waits_for_a_handle_while_the_others_run() {
    let runtime = Runtime::new().unwrap();
    let (thread, end) = thread_to_end();
    let started = Instant::now();
    runtime.block_on(async {
        let ender = spawn(async move {
            ruio_executor::time::sleep(Duration::from_millis(20)).await;
            end.send(()).unwrap();
        });
        let mut wait = HandleWait::new();
        poll_fn(|cx| wait.poll_wait(cx, thread.as_handle()))
            .await
            .unwrap();
        ender.await.unwrap();
    });
    assert!(started.elapsed() >= Duration::from_millis(20));
    thread.join().unwrap();
}

#[test]
fn a_wait_moves_with_its_task_to_another_runtime() {
    let (thread, end) = thread_to_end();
    let handle = thread.as_handle().try_clone_to_owned().unwrap();
    let (sent, received) = mpsc::channel::<HandleWait>();
    // The wait starts on one runtime, which then ends.
    Runtime::new().unwrap().block_on(async {
        let mut wait = HandleWait::new();
        let polled =
            poll_fn(|cx| std::task::Poll::Ready(wait.poll_wait(cx, handle.as_handle()))).await;
        assert!(polled.is_pending());
        sent.send(wait).unwrap();
    });
    let mut wait = received.recv().unwrap();
    assert!(!wait.is_waiting());
    // Another goes on with it.
    let other = thread::spawn(move || {
        Runtime::new().unwrap().block_on(async move {
            poll_fn(|cx| wait.poll_wait(cx, handle.as_handle()))
                .await
                .unwrap();
        });
    });
    thread::sleep(Duration::from_millis(20));
    end.send(()).unwrap();
    other.join().unwrap();
    thread.join().unwrap();
}
