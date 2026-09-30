//! `#[ruio::test]`, and through it the expansion `#[ruio::main]` shares.

use std::io;
use std::time::Duration;

use ruio::uring::ops::Nop;

#[ruio::test]
async fn a_test_runs_on_a_runtime() {
    let task = ruio::spawn(async {
        ruio::time::sleep(Duration::from_millis(1)).await;
        3
    });
    assert_eq!(task.await.unwrap(), 3);
}

#[ruio::test]
async fn a_test_can_return_a_result() -> io::Result<()> {
    ruio::driver().op(Nop).await?;
    Ok(())
}

#[ruio::test]
#[should_panic(expected = "from the test body")]
async fn other_attributes_still_apply() {
    ruio::yield_now().await;
    panic!("from the test body");
}

/// `#[ruio::main]` works on any function, and keeps its other qualifiers,
/// its arguments and its return type.
///
/// # Safety
///
/// None needed; `unsafe` is here to check that the qualifier survives.
#[ruio::main]
async unsafe fn doubled(n: u32) -> u32 {
    ruio::yield_now().await;
    n * 2
}

#[test]
fn main_keeps_qualifiers_arguments_and_the_output() {
    // SAFETY: `doubled` has no safety requirements.
    assert_eq!(unsafe { doubled(21) }, 42);
}

/// With `cores`, every core runs the body.
#[ruio::test(cores = "all")]
async fn a_test_runs_on_every_core() {
    let core = ruio::core().expect("the test runs on a core");
    assert_eq!(ruio::core_count(), ruio::Cores::all().unwrap().count());
    let next = (core.index() + 1) % ruio::core_count();
    let there = ruio::spawn_on(next, async { ruio::core().unwrap().index() });
    assert_eq!(there.await.unwrap(), next);
}

#[ruio::test(cores = 1)]
async fn a_test_on_cores_can_return_a_result() -> io::Result<()> {
    assert_eq!(ruio::core_count(), 1);
    ruio::driver().op(Nop).await?;
    Ok(())
}

/// Every core runs the body, which copies the argument, and the output is
/// the first core's error, in core order.
#[ruio::main(cores = "all")]
async fn fails_from(first: usize) -> Result<(), usize> {
    let index = ruio::core().unwrap().index();
    if index >= first { Err(index) } else { Ok(()) }
}

#[test]
fn main_on_cores_returns_the_first_error() {
    let count = ruio::Cores::all().unwrap().count();
    assert_eq!(fails_from(0), Err(0));
    assert_eq!(fails_from(count - 1), Err(count - 1));
    assert_eq!(fails_from(count), Ok(()));
}

#[ruio::test]
async fn early_returns_leave_the_body() {
    if ruio::driver().op(Nop).await.is_ok() {
        return;
    }
    unreachable!();
}
