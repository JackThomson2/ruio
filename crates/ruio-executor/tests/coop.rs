//! The cooperative budget, as a primitive of another crate's takes part in
//! it through `coop::poll_proceed`.

use std::cell::Cell;
use std::future::poll_fn;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use ruio_executor::time::timeout;
use ruio_executor::{Runtime, coop, spawn};

/// Long enough for any of these to finish; a lost wake runs it out.
const PATIENCE: Duration = Duration::from_secs(20);

/// Runs `future` on a runtime of its own, failing if it takes longer than
/// [`PATIENCE`], as it does if a wake is lost.
fn run<F: Future>(future: F) -> F::Output {
    Runtime::new()
        .unwrap()
        .block_on(timeout(PATIENCE, future))
        .expect("finished in time, with no wake lost")
}

/// A primitive that is always ready, as a nonblocking read that finds data
/// waiting is, and spends a step of the budget each time.
async fn always_ready() {
    poll_fn(|cx| {
        let proceed = std::task::ready!(coop::poll_proceed(cx));
        proceed.made_progress();
        Poll::Ready(())
    })
    .await;
}

/// A task that always finds the primitive ready still lets the others run:
/// once its budget is spent, the primitive makes it yield.
#[test]
fn a_primitive_that_is_always_ready_does_not_starve_the_other_tasks() {
    const MANY: usize = 10_000;
    run(async {
        let done = Rc::new(Cell::new(0));
        let looping = spawn({
            let done = done.clone();
            async move {
                for _ in 0..MANY {
                    always_ready().await;
                    done.set(done.get() + 1);
                }
            }
        });
        let seen = spawn(async move { done.get() }).await.unwrap();
        assert!(0 < seen && seen < MANY / 10, "{seen} steps");
        looping.await.unwrap();
    });
}

/// One poll takes 128 steps; a step dropped unspent, as by a primitive that
/// was not ready, costs nothing.
#[test]
fn a_poll_takes_its_budget_of_steps_and_unspent_steps_cost_nothing() {
    run(async {
        let steps = poll_fn(|cx| {
            for _ in 0..1000 {
                // Asked and not spent.
                assert!(coop::poll_proceed(cx).is_ready());
            }
            let mut steps = 0;
            while let Poll::Ready(proceed) = coop::poll_proceed(cx) {
                proceed.made_progress();
                steps += 1;
            }
            Poll::Ready(steps)
        })
        .await;
        assert_eq!(steps, 128);
        // Spent, for the rest of this poll.
        assert!(poll_fn(|cx| Poll::Ready(coop::poll_proceed(cx).is_pending())).await);
        // The next poll, after the task yields, has a budget of its own.
        ruio_executor::yield_now().await;
        assert!(poll_fn(|cx| Poll::Ready(coop::poll_proceed(cx).is_ready())).await);
    });
}

/// Outside a runtime there is no budget.
#[test]
fn outside_a_runtime_a_primitive_always_proceeds() {
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..1000 {
        match coop::poll_proceed(&mut cx) {
            Poll::Ready(proceed) => proceed.made_progress(),
            Poll::Pending => panic!("a budget outside a runtime"),
        }
    }
}
