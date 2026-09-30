//! Channels, which carry values between tasks, on one core or across cores.
//!
//! Both halves of a channel are `Send` when their values are, and wake a
//! task wherever it runs: a task on another core is woken through its
//! runtime's queue of remote wakes. [`oneshot`] carries one value;
//! [`mpsc`] carries many, from any number of senders, with a bound.

use std::sync::{Mutex, MutexGuard, PoisonError};
use std::task::{Context, Waker};

/// Locks `mutex`, whose data no panic can leave half-changed: every critical
/// section here only moves values in and out.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Keeps `cx`'s waker in `slot`, cloning it only if it is another task's.
fn register(slot: &mut Option<Waker>, cx: &Context<'_>) {
    match slot {
        Some(waker) if waker.will_wake(cx.waker()) => {}
        _ => *slot = Some(cx.waker().clone()),
    }
}

pub mod oneshot {
    //! A channel for one value.

    use std::error::Error;
    use std::fmt;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Waker};

    use super::{lock, register};

    /// A channel for one value: the sender sends it, the receiver awaits it.
    pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
        let shared = Arc::new(Mutex::new(State {
            value: None,
            waker: None,
            sender: true,
            receiver: true,
        }));
        (
            Sender {
                shared: Arc::clone(&shared),
            },
            Receiver { shared },
        )
    }

    struct State<T> {
        value: Option<T>,
        /// The task awaiting the value.
        waker: Option<Waker>,
        /// Whether the sender is still there, and the receiver.
        sender: bool,
        receiver: bool,
    }

    /// Sends the channel's value. Dropping it without sending closes the
    /// channel.
    pub struct Sender<T> {
        shared: Arc<Mutex<State<T>>>,
    }

    impl<T> Sender<T> {
        /// Sends `value`, or hands it back if the receiver is gone.
        pub fn send(self, value: T) -> Result<(), T> {
            let waker = {
                let mut state = lock(&self.shared);
                if !state.receiver {
                    return Err(value);
                }
                state.value = Some(value);
                state.waker.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
            Ok(())
        }

        /// Whether the receiver is gone, so that sending is in vain.
        pub fn is_closed(&self) -> bool {
            !lock(&self.shared).receiver
        }
    }

    impl<T> Drop for Sender<T> {
        fn drop(&mut self) {
            let waker = {
                let mut state = lock(&self.shared);
                state.sender = false;
                state.waker.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }

    impl<T> fmt::Debug for Sender<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("Sender").finish_non_exhaustive()
        }
    }

    /// Awaits the channel's value. The output is the value, or
    /// [`RecvError`] if the sender was dropped without sending.
    pub struct Receiver<T> {
        shared: Arc<Mutex<State<T>>>,
    }

    impl<T> Receiver<T> {
        /// The value if it has been sent, without waiting.
        ///
        /// # Errors
        ///
        /// [`TryRecvError::Empty`] if it has not been sent yet, and
        /// [`TryRecvError::Closed`] if it never will be.
        pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
            let mut state = lock(&self.shared);
            match state.value.take() {
                Some(value) => Ok(value),
                None if state.sender => Err(TryRecvError::Empty),
                None => Err(TryRecvError::Closed),
            }
        }
    }

    impl<T> Future for Receiver<T> {
        type Output = Result<T, RecvError>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let mut state = lock(&self.shared);
            if let Some(value) = state.value.take() {
                return Poll::Ready(Ok(value));
            }
            if !state.sender {
                return Poll::Ready(Err(RecvError(())));
            }
            register(&mut state.waker, cx);
            Poll::Pending
        }
    }

    impl<T> Drop for Receiver<T> {
        fn drop(&mut self) {
            let value = {
                let mut state = lock(&self.shared);
                state.receiver = false;
                state.value.take()
            };
            // Dropped outside the lock: a value's drop can run any code.
            drop(value);
        }
    }

    impl<T> fmt::Debug for Receiver<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("Receiver").finish_non_exhaustive()
        }
    }

    /// The sender was dropped without sending.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct RecvError(());

    impl fmt::Display for RecvError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("the sender was dropped without sending")
        }
    }

    impl Error for RecvError {}

    /// Why [`Receiver::try_recv`] has no value.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum TryRecvError {
        /// The value has not been sent yet.
        Empty,
        /// The sender was dropped without sending.
        Closed,
    }

    impl fmt::Display for TryRecvError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(match self {
                TryRecvError::Empty => "the value has not been sent yet",
                TryRecvError::Closed => "the sender was dropped without sending",
            })
        }
    }

    impl Error for TryRecvError {}
}

pub mod mpsc {
    //! A channel for many values, from any number of senders to one
    //! receiver, which holds a bounded number of them.

    use std::collections::VecDeque;
    use std::error::Error;
    use std::fmt;
    use std::future::{Future, poll_fn};
    use std::mem;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Waker};

    use super::{lock, register};

    /// A channel that holds up to `capacity` values: sending waits while it
    /// is full.
    ///
    /// # Panics
    ///
    /// If `capacity` is zero.
    pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
        assert!(capacity > 0, "a channel holds at least one value");
        let shared = Arc::new(Mutex::new(State {
            queue: VecDeque::new(),
            capacity,
            receiver_waker: None,
            sender_wakers: Vec::new(),
            senders: 1,
            receiver: true,
        }));
        (
            Sender {
                shared: Arc::clone(&shared),
            },
            Receiver { shared },
        )
    }

    struct State<T> {
        queue: VecDeque<T>,
        capacity: usize,
        /// The receiver's task, waiting for a value.
        receiver_waker: Option<Waker>,
        /// Senders' tasks, waiting for room.
        sender_wakers: Vec<Waker>,
        /// How many senders there are.
        senders: usize,
        /// Whether the receiver is still there.
        receiver: bool,
    }

    /// Sends values into the channel. Clones send into the same channel;
    /// once every one is dropped, the receiver sees the channel end.
    pub struct Sender<T> {
        shared: Arc<Mutex<State<T>>>,
    }

    impl<T> Sender<T> {
        /// Sends `value`, waiting while the channel is full; see [`Sending`].
        pub fn send(&self, value: T) -> Sending<'_, T> {
            Sending {
                sender: self,
                value: Some(value),
            }
        }

        /// Sends `value` if the channel has room, or hands it back.
        ///
        /// # Errors
        ///
        /// [`TrySendError::Full`] if the channel is full, and
        /// [`TrySendError::Closed`] if the receiver is gone.
        pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
            let waker = {
                let mut state = lock(&self.shared);
                if !state.receiver {
                    return Err(TrySendError::Closed(value));
                }
                if state.queue.len() >= state.capacity {
                    return Err(TrySendError::Full(value));
                }
                state.queue.push_back(value);
                state.receiver_waker.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
            Ok(())
        }

        /// Whether the receiver is gone, so that sending is in vain.
        pub fn is_closed(&self) -> bool {
            !lock(&self.shared).receiver
        }
    }

    impl<T> Clone for Sender<T> {
        fn clone(&self) -> Self {
            lock(&self.shared).senders += 1;
            Self {
                shared: Arc::clone(&self.shared),
            }
        }
    }

    impl<T> Drop for Sender<T> {
        fn drop(&mut self) {
            let waker = {
                let mut state = lock(&self.shared);
                state.senders -= 1;
                if state.senders == 0 {
                    state.receiver_waker.take()
                } else {
                    None
                }
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }

    impl<T> fmt::Debug for Sender<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("Sender").finish_non_exhaustive()
        }
    }

    /// Sends a value into a channel, waiting while it is full. Made by
    /// [`Sender::send`]; the output hands the value back if the receiver is
    /// gone.
    #[must_use = "a send does nothing until polled"]
    pub struct Sending<'a, T> {
        sender: &'a Sender<T>,
        /// Taken once sent or handed back.
        value: Option<T>,
    }

    impl<T> Future for Sending<'_, T> {
        type Output = Result<(), SendError<T>>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.get_mut();
            let value = this
                .value
                .take()
                .expect("`Sending` polled after it completed");
            let waker = {
                let mut state = lock(&this.sender.shared);
                if !state.receiver {
                    return Poll::Ready(Err(SendError(value)));
                }
                if state.queue.len() >= state.capacity {
                    if !state
                        .sender_wakers
                        .iter()
                        .any(|waker| waker.will_wake(cx.waker()))
                    {
                        state.sender_wakers.push(cx.waker().clone());
                    }
                    this.value = Some(value);
                    return Poll::Pending;
                }
                state.queue.push_back(value);
                state.receiver_waker.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
            Poll::Ready(Ok(()))
        }
    }

    // Nothing is pinned: the value is only ever moved.
    impl<T> Unpin for Sending<'_, T> {}

    impl<T> fmt::Debug for Sending<'_, T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("Sending").finish_non_exhaustive()
        }
    }

    /// Receives the channel's values, in the order they were sent.
    pub struct Receiver<T> {
        shared: Arc<Mutex<State<T>>>,
    }

    impl<T> Receiver<T> {
        /// The next value, waiting for one, or `None` once every sender is
        /// gone and the channel is empty.
        pub async fn recv(&mut self) -> Option<T> {
            poll_fn(|cx| self.poll_recv(cx)).await
        }

        /// The next value, or `None` once every sender is gone and the
        /// channel is empty; arranges for the task to be woken otherwise.
        pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
            match self.take(Some(cx)) {
                Ok(value) => Poll::Ready(Some(value)),
                Err(TryRecvError::Closed) => Poll::Ready(None),
                Err(TryRecvError::Empty) => Poll::Pending,
            }
        }

        /// The next value if there is one, without waiting.
        ///
        /// # Errors
        ///
        /// [`TryRecvError::Empty`] if the channel is empty, and
        /// [`TryRecvError::Closed`] if every sender is gone as well.
        pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
            self.take(None)
        }

        /// Takes the next value; if there is none, and a context is given,
        /// keeps its waker, under the same lock, so that no send is missed.
        fn take(&mut self, cx: Option<&Context<'_>>) -> Result<T, TryRecvError> {
            let (value, senders) = {
                let mut state = lock(&self.shared);
                match state.queue.pop_front() {
                    // Room for every waiting sender to try again: one woken
                    // alone could have been dropped, and waste the wake.
                    Some(value) => (value, mem::take(&mut state.sender_wakers)),
                    None if state.senders == 0 => return Err(TryRecvError::Closed),
                    None => {
                        if let Some(cx) = cx {
                            register(&mut state.receiver_waker, cx);
                        }
                        return Err(TryRecvError::Empty);
                    }
                }
            };
            for waker in senders {
                waker.wake();
            }
            Ok(value)
        }
    }

    impl<T> Drop for Receiver<T> {
        fn drop(&mut self) {
            let (queue, senders) = {
                let mut state = lock(&self.shared);
                state.receiver = false;
                (
                    mem::take(&mut state.queue),
                    mem::take(&mut state.sender_wakers),
                )
            };
            // Dropped outside the lock: a value's drop can run any code.
            drop(queue);
            for waker in senders {
                waker.wake();
            }
        }
    }

    impl<T> fmt::Debug for Receiver<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("Receiver").finish_non_exhaustive()
        }
    }

    /// The receiver is gone; the value comes back.
    #[derive(Clone, Copy, PartialEq, Eq)]
    pub struct SendError<T>(pub T);

    impl<T> fmt::Debug for SendError<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("SendError").finish_non_exhaustive()
        }
    }

    impl<T> fmt::Display for SendError<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("the receiver is gone")
        }
    }

    impl<T> Error for SendError<T> {}

    /// Why [`Sender::try_send`] could not send; the value comes back.
    #[derive(Clone, Copy, PartialEq, Eq)]
    pub enum TrySendError<T> {
        /// The channel is full.
        Full(T),
        /// The receiver is gone.
        Closed(T),
    }

    impl<T> fmt::Debug for TrySendError<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(match self {
                TrySendError::Full(_) => "Full(..)",
                TrySendError::Closed(_) => "Closed(..)",
            })
        }
    }

    impl<T> fmt::Display for TrySendError<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(match self {
                TrySendError::Full(_) => "the channel is full",
                TrySendError::Closed(_) => "the receiver is gone",
            })
        }
    }

    impl<T> Error for TrySendError<T> {}

    /// Why [`Receiver::try_recv`] has no value.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum TryRecvError {
        /// The channel is empty.
        Empty,
        /// The channel is empty, and every sender is gone.
        Closed,
    }

    impl fmt::Display for TryRecvError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(match self {
                TryRecvError::Empty => "the channel is empty",
                TryRecvError::Closed => "the channel is empty, and every sender is gone",
            })
        }
    }

    impl Error for TryRecvError {}
}
