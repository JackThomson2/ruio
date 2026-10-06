//! The channels' receivers, watch streams and intervals through the `Stream`
//! and `FusedStream` traits of futures-core, and futures-util's combinators.

#![cfg(feature = "futures")]

use std::future::Future;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use futures_util::stream::FusedStream;
use ruio_executor::sync::watch::WatchStream;
use ruio_executor::sync::{broadcast, local, mpsc, watch};
use ruio_executor::time::{interval, timeout};
use ruio_executor::{Runtime, spawn};

/// Long enough for any of these to finish; a lost wake runs it out.
const PATIENCE: Duration = Duration::from_secs(20);

/// Runs `future` on a runtime of its own, failing if it takes longer than
/// [`PATIENCE`].
fn run<F: Future>(future: F) -> F::Output {
    Runtime::new()
        .unwrap()
        .block_on(timeout(PATIENCE, future))
        .expect("finished in time, with no wake lost")
}

#[test]
fn a_bounded_receiver_streams_until_the_senders_are_gone() {
    run(async {
        let (tx, mut rx) = mpsc::channel::<u32>(4);
        let producer = spawn(async move {
            for n in 0..100 {
                tx.send(n).await.unwrap();
            }
        });
        assert!(!rx.is_terminated());
        let doubled: Vec<u32> = StreamExt::collect(StreamExt::map(&mut rx, |n| n * 2)).await;
        assert_eq!(doubled, (0..100).map(|n| n * 2).collect::<Vec<_>>());
        assert!(rx.is_terminated(), "{rx:?}");
        assert!(StreamExt::next(&mut rx).await.is_none());
        producer.await.unwrap();
    });
}

#[test]
fn an_unbounded_receiver_closed_by_the_receiver_drains_then_ends() {
    run(async {
        let (tx, mut rx) = mpsc::unbounded_channel::<u64>();
        for n in 1..=10 {
            tx.send(n).unwrap();
        }
        rx.close();
        assert!(tx.send(11).is_err(), "closed to the senders");
        assert!(!rx.is_terminated(), "values are left to take");
        let sum = StreamExt::fold(&mut rx, 0, |sum, n| async move { sum + n }).await;
        assert_eq!(sum, 55);
        assert!(rx.is_terminated());
    });
}

#[test]
fn the_local_receivers_stream_too() {
    run(async {
        let (tx, mut rx) = local::mpsc::channel::<u8>(2);
        let producer = spawn(async move {
            for n in 0..5 {
                tx.send(n).await.unwrap();
            }
        });
        let taken: Vec<u8> = StreamExt::collect(StreamExt::take(&mut rx, 3)).await;
        assert_eq!(taken, [0, 1, 2]);
        assert!(!rx.is_terminated());
        let rest: Vec<u8> = StreamExt::collect(&mut rx).await;
        assert_eq!(rest, [3, 4]);
        assert!(rx.is_terminated());
        producer.await.unwrap();

        let (tx, mut rx) = local::mpsc::unbounded_channel::<&str>();
        tx.send("a").unwrap();
        tx.send("b").unwrap();
        drop(tx);
        assert!(!rx.is_terminated());
        let words: Vec<&str> = StreamExt::collect(&mut rx).await;
        assert_eq!(words, ["a", "b"]);
        assert!(rx.is_terminated());
    });
}

/// A broadcast receiver yields every value, or that it missed some, and ends
/// rather than yielding `Closed`.
#[test]
fn a_broadcast_receiver_streams_values_and_reports_lag() {
    run(async {
        let (tx, mut prompt) = broadcast::channel::<u32>(8);
        let mut late = tx.subscribe();
        for n in 0..5 {
            tx.send(n).unwrap();
        }
        let early: Vec<u32> = StreamExt::collect(StreamExt::map(
            StreamExt::take(&mut prompt, 5),
            Result::unwrap,
        ))
        .await;
        assert_eq!(early, [0, 1, 2, 3, 4]);
        assert!(!prompt.is_terminated(), "the sender is still there");
        drop(tx);
        assert!(prompt.is_terminated(), "nothing left, and no sender");
        assert!(StreamExt::next(&mut prompt).await.is_none());

        let items: Vec<Result<u32, broadcast::RecvError>> = StreamExt::collect(&mut late).await;
        assert_eq!(items, [Ok(0), Ok(1), Ok(2), Ok(3), Ok(4)]);
        assert!(late.is_terminated());

        // A ring of two keeps two of five: the first item says three were
        // missed, and the rest follow.
        let (tx, mut rx) = broadcast::channel::<u32>(2);
        for n in 0..5 {
            tx.send(n).unwrap();
        }
        drop(tx);
        let items: Vec<Result<u32, broadcast::RecvError>> = StreamExt::collect(&mut rx).await;
        assert_eq!(items, [Err(broadcast::RecvError::Lagged(3)), Ok(3), Ok(4)]);
        assert!(rx.is_terminated());
    });
}

#[test]
fn a_watch_stream_yields_the_latest_value_on_each_change() {
    run(async {
        let (tx, rx) = watch::channel(0u32);
        let mut current = WatchStream::new(rx.clone());
        let mut changes = WatchStream::from_changes(rx);
        assert_eq!(
            StreamExt::next(&mut current).await,
            Some(0),
            "the value as it is comes first"
        );
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        assert_eq!(
            StreamExt::next(&mut current).await,
            Some(2),
            "the latest, however many were sent"
        );
        assert_eq!(StreamExt::next(&mut changes).await, Some(2));
        assert!(!current.is_terminated());
        drop(tx);
        assert!(current.is_terminated(), "{current:?}");
        assert_eq!(StreamExt::next(&mut current).await, None);
        let rest: Vec<u32> = StreamExt::collect(changes).await;
        assert!(rest.is_empty());
        assert_eq!(*current.into_inner().borrow(), 2, "the last value stays");
    });
}

#[test]
fn an_interval_streams_its_ticks() {
    run(async {
        let period = Duration::from_millis(10);
        let mut ticks = interval(period);
        assert!(!ticks.is_terminated());
        let start = Instant::now();
        let deadlines: Vec<Instant> = StreamExt::collect(StreamExt::take(&mut ticks, 3)).await;
        assert_eq!(deadlines.len(), 3);
        assert_eq!(deadlines[1] - deadlines[0], period);
        assert_eq!(deadlines[2] - deadlines[1], period);
        assert!(start.elapsed() >= 2 * period, "{:?}", start.elapsed());
        assert!(!ticks.is_terminated());
    });
}

/// A receiver and an interval merge through `select`: the values come as
/// they are sent, between the ticks.
#[test]
fn a_receiver_and_an_interval_merge_through_select() {
    #[derive(Debug, PartialEq)]
    enum Event {
        Value(u8),
        Tick,
    }

    run(async {
        let (tx, rx) = mpsc::unbounded_channel::<u8>();
        let ticks = StreamExt::map(interval(Duration::from_millis(2)), |_| Event::Tick);
        let values = StreamExt::map(rx, Event::Value);
        let mut merged = futures_util::stream::select(values, ticks);
        let mut ticks_seen = 0;
        let mut values_seen = Vec::new();
        tx.send(7).unwrap();
        while ticks_seen < 3 || values_seen.len() < 2 {
            match StreamExt::next(&mut merged).await.unwrap() {
                Event::Value(value) => {
                    values_seen.push(value);
                    if value == 7 {
                        tx.send(8).unwrap();
                    }
                }
                Event::Tick => ticks_seen += 1,
            }
        }
        assert_eq!(values_seen, [7, 8]);
        assert!(!merged.is_terminated(), "the ticks never end");
    });
}
