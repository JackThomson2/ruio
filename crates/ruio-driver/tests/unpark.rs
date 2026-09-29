//! Waking a parked driver from other threads.

use std::thread;
use std::time::{Duration, Instant};

use ruio_driver::{Driver, Unparker};

const LONG: Duration = Duration::from_secs(5);

#[test]
fn unparkers_can_move_between_threads() {
    fn assert_send_sync<T: Send + Sync + Clone>() {}
    assert_send_sync::<Unparker>();
}

#[test]
fn an_unpark_before_parking_makes_the_next_park_return() {
    let driver = Driver::new().unwrap();
    driver.unparker().unpark();
    let start = Instant::now();
    driver.park(Some(LONG)).unwrap();
    assert!(start.elapsed() < LONG);

    // The unpark is used up, so the next park waits for its timeout.
    let start = Instant::now();
    driver.park(Some(Duration::from_millis(20))).unwrap();
    assert!(start.elapsed() >= Duration::from_millis(20));
}

#[test]
fn unparks_do_not_add_up() {
    let driver = Driver::new().unwrap();
    let unparker = driver.unparker();
    for _ in 0..100 {
        unparker.unpark();
    }
    driver.park(Some(LONG)).unwrap();
    let start = Instant::now();
    driver.park(Some(Duration::from_millis(20))).unwrap();
    assert!(start.elapsed() >= Duration::from_millis(20));
}

/// Parks while another thread unparks after `delay`, and returns whether the
/// wake came through the kernel, as a completion of the eventfd poll, rather
/// than as an unpark left before parking.
fn park_until_unparked(driver: &Driver, delay: Duration) -> bool {
    let unparker = driver.unparker();
    let waker = thread::spawn(move || {
        thread::sleep(delay);
        unparker.unpark();
    });
    let completions = driver.stats().completions;
    let start = Instant::now();
    driver.park(Some(LONG)).unwrap();
    assert!(start.elapsed() < LONG, "the unpark did not wake the driver");
    waker.join().unwrap();
    driver.stats().completions > completions
}

#[test]
fn another_thread_wakes_a_parked_driver() {
    let driver = Driver::new().unwrap();
    assert!(park_until_unparked(&driver, Duration::from_millis(50)));
    // Repeated parks exercise the poll staying armed between wakes.
    let through_the_kernel = (0..20)
        .filter(|_| park_until_unparked(&driver, Duration::from_millis(5)))
        .count();
    assert!(through_the_kernel > 0);
}

#[test]
fn unparking_a_dropped_driver_does_nothing() {
    let unparker = Driver::new().unwrap().unparker();
    unparker.unpark();
    unparker.unpark();
}

#[test]
fn the_wake_poll_is_not_an_operation_in_flight() {
    let driver = Driver::new().unwrap();
    driver.park(Some(Duration::from_millis(1))).unwrap();
    assert_eq!(driver.in_flight(), 0);
    let start = Instant::now();
    drop(driver);
    assert!(start.elapsed() < Duration::from_millis(500));
}
