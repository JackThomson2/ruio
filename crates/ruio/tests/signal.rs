//! Signals delivered to tasks.
//!
//! Handlers are process-wide and the tests run in parallel, so each test
//! that counts arrivals listens for a kind of its own, and the kinds the
//! tests share are only ever asked "has it arrived at all".

mod support;

use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use libc::c_int;
use ruio::signal::{Signal, SignalKind, ctrl_c, signal};
use ruio::time::timeout;

use support::within;

/// Sends `kind` to the process, as `kill(1)` would.
fn send(kind: SignalKind) {
    // SAFETY: `getpid` touches nothing.
    let pid = unsafe { libc::getpid() };
    // SAFETY: `kill` of the process itself, with a signal the test has a
    // handler for.
    let sent = unsafe { libc::kill(pid, kind.as_raw_value()) };
    assert_eq!(sent, 0);
}

/// Lets the dispatcher run and hand on what has arrived: a receive that
/// completes within this is one that was delivered.
const SOON: Duration = Duration::from_millis(500);

/// Waits for `signal` to receive, failing if it does not soon.
async fn receives(signal: &mut Signal) {
    within(timeout(SOON, signal.recv()))
        .await
        .expect("the signal is delivered")
        .expect("the listener is open");
}

/// Whether `signal` receives within `wait`.
async fn receives_within(signal: &mut Signal, wait: Duration) -> bool {
    timeout(wait, signal.recv()).await.is_ok()
}

#[test]
fn a_signal_sent_to_the_process_is_received() {
    ruio::Runtime::new().unwrap().block_on(async {
        let mut usr1 = signal(SignalKind::user_defined1()).unwrap();
        assert_eq!(usr1.kind(), SignalKind::user_defined1());
        send(SignalKind::user_defined1());
        receives(&mut usr1).await;
    });
}

#[test]
fn signals_that_arrive_together_coalesce_into_one_receive() {
    ruio::Runtime::new().unwrap().block_on(async {
        let mut winch = signal(SignalKind::window_change()).unwrap();
        for _ in 0..5 {
            send(SignalKind::window_change());
        }
        receives(&mut winch).await;
        // Everything that arrived is in the one receive; what the
        // dispatcher may still be handing on arrives within the wait.
        thread::sleep(Duration::from_millis(50));
        let _ = receives_within(&mut winch, Duration::from_millis(50)).await;
        assert!(
            !receives_within(&mut winch, Duration::from_millis(50)).await,
            "nothing more arrives"
        );
        send(SignalKind::window_change());
        receives(&mut winch).await;
    });
}

#[test]
fn listeners_on_several_runtimes_all_receive() {
    const THREADS: usize = 4;
    let ready = Arc::new(Barrier::new(THREADS + 1));
    let received = Arc::new(AtomicUsize::new(0));
    let listeners: Vec<_> = (0..THREADS)
        .map(|_| {
            let (ready, received) = (ready.clone(), received.clone());
            thread::spawn(move || {
                ruio::Runtime::new().unwrap().block_on(async {
                    let mut usr2 = signal(SignalKind::user_defined2()).unwrap();
                    ready.wait();
                    receives(&mut usr2).await;
                    received.fetch_add(1, Ordering::Relaxed);
                });
            })
        })
        .collect();
    ready.wait();
    send(SignalKind::user_defined2());
    for listener in listeners {
        listener.join().unwrap();
    }
    assert_eq!(received.load(Ordering::Relaxed), THREADS);
}

#[test]
fn several_listeners_for_one_kind_on_one_runtime_all_receive() {
    ruio::Runtime::new().unwrap().block_on(async {
        let mut listeners: Vec<_> = (0..3).map(|_| signal(SignalKind::io()).unwrap()).collect();
        send(SignalKind::io());
        for listener in &mut listeners {
            receives(listener).await;
        }
    });
}

#[test]
fn ctrl_c_waits_for_sigint() {
    ruio::Runtime::new().unwrap().block_on(async {
        // Installed by the first poll, before the signal is sent.
        let waiting = ruio::spawn(async { ctrl_c().await });
        for _ in 0..3 {
            ruio::yield_now().await;
        }
        send(SignalKind::interrupt());
        within(timeout(SOON, waiting))
            .await
            .expect("the interrupt is delivered")
            .unwrap()
            .unwrap();
    });
}

#[test]
fn the_handler_a_library_installed_is_still_called() {
    static CALLED: AtomicBool = AtomicBool::new(false);
    extern "C" fn library_handler(_: c_int) {
        CALLED.store(true, Ordering::Relaxed);
    }
    // SAFETY: a zeroed `sigaction` is a valid one, filled in below.
    let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
    action.sa_sigaction = library_handler as extern "C" fn(c_int) as usize;
    action.sa_flags = libc::SA_RESTART;
    // SAFETY: `action` is a valid action, and the old one is not asked for.
    let installed = unsafe { libc::sigaction(libc::SIGHUP, &action, std::ptr::null_mut()) };
    assert_eq!(installed, 0);
    ruio::Runtime::new().unwrap().block_on(async {
        let mut hup = signal(SignalKind::hangup()).unwrap();
        send(SignalKind::hangup());
        receives(&mut hup).await;
    });
    assert!(
        CALLED.load(Ordering::Relaxed),
        "the library's handler ran too"
    );
}

#[test]
fn kinds_that_cannot_be_listened_for_are_refused() {
    ruio::Runtime::new().unwrap().block_on(async {
        for signum in [
            libc::SIGKILL,
            libc::SIGSTOP,
            libc::SIGSEGV,
            libc::SIGBUS,
            0,
            65,
            -1,
        ] {
            let err = signal(SignalKind::from_raw(signum)).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::InvalidInput, "signal {signum}");
        }
    });
}

#[test]
fn a_listener_dropped_and_made_again_receives() {
    ruio::Runtime::new().unwrap().block_on(async {
        let alarm = signal(SignalKind::alarm()).unwrap();
        drop(alarm);
        let mut alarm = signal(SignalKind::alarm()).unwrap();
        send(SignalKind::alarm());
        receives(&mut alarm).await;
    });
}

#[test]
fn a_runtime_made_after_another_on_the_thread_dispatches_too() {
    for round in 0..3 {
        ruio::Runtime::new().unwrap().block_on(async {
            let mut chld = signal(SignalKind::child()).unwrap();
            send(SignalKind::child());
            receives(&mut chld).await;
        });
        let _ = round;
    }
}

#[test]
fn a_listener_is_send_and_the_kind_converts() {
    fn is_send<T: Send>() {}
    is_send::<Signal>();
    assert_eq!(c_int::from(SignalKind::terminate()), libc::SIGTERM);
    assert_eq!(SignalKind::from(libc::SIGQUIT), SignalKind::quit());
    assert_eq!(SignalKind::pipe().as_raw_value(), libc::SIGPIPE);
    assert!(format!("{:?}", SignalKind::interrupt()).contains("SignalKind"));
}
