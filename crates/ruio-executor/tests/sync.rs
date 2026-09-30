//! Channels, on one runtime and across threads.

use std::future::poll_fn;
use std::pin::pin;
use std::task::Poll;
use std::thread;
use std::time::Duration;

use ruio_executor::sync::{mpsc, oneshot};
use ruio_executor::time::sleep;
use ruio_executor::{Runtime, spawn, yield_now};

#[test]
fn a_oneshot_carries_its_value() {
    Runtime::new().unwrap().block_on(async {
        let (tx, rx) = oneshot::channel();
        let task = spawn(rx);
        yield_now().await;
        tx.send(7).unwrap();
        assert_eq!(task.await.unwrap(), Ok(7));
    });
}

#[test]
fn a_oneshot_reports_its_other_half_gone() {
    Runtime::new().unwrap().block_on(async {
        let (tx, rx) = oneshot::channel::<u32>();
        drop(tx);
        assert!(rx.await.is_err());

        let (tx, rx) = oneshot::channel();
        drop(rx);
        assert!(tx.is_closed());
        assert_eq!(tx.send(7), Err(7));

        // A receiver already waiting learns of the drop too.
        let (tx, rx) = oneshot::channel::<u32>();
        let waiting = spawn(rx);
        yield_now().await;
        drop(tx);
        assert!(waiting.await.unwrap().is_err());

        let (tx, mut rx) = oneshot::channel();
        assert_eq!(rx.try_recv(), Err(oneshot::TryRecvError::Empty));
        tx.send(1).unwrap();
        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.try_recv(), Err(oneshot::TryRecvError::Closed));
    });
}

/// A value sent from another thread wakes the task waiting for it.
#[test]
fn a_oneshot_wakes_a_task_from_another_thread() {
    Runtime::new().unwrap().block_on(async {
        let (tx, rx) = oneshot::channel();
        let sender = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            tx.send(String::from("across")).unwrap();
        });
        assert_eq!(rx.await.unwrap(), "across");
        sender.join().unwrap();
    });
}

#[test]
fn a_channel_keeps_its_values_in_order_and_ends() {
    Runtime::new().unwrap().block_on(async {
        let (tx, mut rx) = mpsc::channel(4);
        let other = tx.clone();
        let _sender = spawn(async move {
            for value in 0..100 {
                tx.send(value).await.unwrap();
            }
        });
        drop(other);
        let mut received = Vec::new();
        while let Some(value) = rx.recv().await {
            received.push(value);
        }
        assert_eq!(received, (0..100).collect::<Vec<_>>());

        // A receiver already waiting learns that the last sender went.
        let (tx, mut rx) = mpsc::channel::<u32>(1);
        let waiting = spawn(async move { rx.recv().await });
        yield_now().await;
        drop(tx);
        assert_eq!(waiting.await.unwrap(), None);
    });
}

/// A full channel makes a send wait until the receiver takes a value.
#[test]
fn a_full_channel_holds_back_its_senders() {
    Runtime::new().unwrap().block_on(async {
        let (tx, mut rx) = mpsc::channel(2);
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        assert!(matches!(tx.try_send(3), Err(mpsc::TrySendError::Full(3))));
        let mut send = pin!(tx.send(3));
        poll_fn(|cx| {
            assert!(send.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert_eq!(rx.recv().await, Some(1));
        send.await.unwrap();
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.try_recv(), Ok(3));
        assert_eq!(rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        drop(tx);
        assert_eq!(rx.try_recv(), Err(mpsc::TryRecvError::Closed));
    });
}

#[test]
fn a_channel_hands_values_back_once_its_receiver_is_gone() {
    Runtime::new().unwrap().block_on(async {
        let (tx, rx) = mpsc::channel(1);
        tx.try_send(1).unwrap();
        let mut waiting = pin!(tx.send(2));
        poll_fn(|cx| {
            assert!(waiting.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        // A sender waiting in another task is woken to learn it too.
        let blocked = {
            let tx = tx.clone();
            spawn(async move { tx.send(4).await.map_err(|err| err.0) })
        };
        yield_now().await;
        drop(rx);
        assert!(tx.is_closed());
        assert_eq!(waiting.await.unwrap_err().0, 2);
        assert_eq!(blocked.await.unwrap(), Err(4));
        assert!(matches!(tx.try_send(3), Err(mpsc::TrySendError::Closed(3))));
    });
}

/// Room made while two senders wait reaches the one still there, though
/// the other gave up after it was woken.
#[test]
fn a_sender_that_gives_up_does_not_hold_back_the_others() {
    Runtime::new().unwrap().block_on(async {
        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send(0).unwrap();
        let quitter = {
            let tx = tx.clone();
            spawn(async move {
                let mut send = pin!(tx.send(1));
                poll_fn(|cx| {
                    let _ = send.as_mut().poll(cx);
                    Poll::Ready(())
                })
                .await;
                // Gives up, dropping the send, as soon as it is woken.
                yield_now().await;
            })
        };
        let stayer = {
            let tx = tx.clone();
            spawn(async move { tx.send(2).await })
        };
        yield_now().await;
        assert_eq!(rx.recv().await, Some(0));
        quitter.await.unwrap();
        stayer.await.unwrap().unwrap();
        assert_eq!(rx.recv().await, Some(2));
    });
}

/// Values from many threads all arrive, and the channel ends when the last
/// sender goes.
#[test]
fn many_threads_send_into_one_task() {
    Runtime::new().unwrap().block_on(async {
        let (tx, mut rx) = mpsc::channel(16);
        let threads: Vec<_> = (0..8)
            .map(|thread| {
                let tx = tx.clone();
                thread::spawn(move || {
                    Runtime::new().unwrap().block_on(async move {
                        for value in 0..1000 {
                            tx.send((thread, value)).await.unwrap();
                        }
                    });
                })
            })
            .collect();
        drop(tx);
        let mut last = [None; 8];
        let mut count = 0;
        while let Some((thread, value)) = rx.recv().await {
            assert!(
                last[thread].is_none_or(|last| last < value),
                "in order per sender"
            );
            last[thread] = Some(value);
            count += 1;
            if count % 97 == 0 {
                sleep(Duration::from_millis(1)).await;
            }
        }
        assert_eq!(count, 8000);
        for thread in threads {
            thread.join().unwrap();
        }
    });
}
