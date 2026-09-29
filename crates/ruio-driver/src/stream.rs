//! Multishot requests, which post a stream of completions.

use std::fmt;
use std::future::poll_fn;
use std::mem;
use std::task::{Context, Poll};

use ruio_sys as sys;

use crate::op::Orphan;
use crate::{Completion, Driver};

/// An io_uring request that posts a stream of completions, such as a
/// multishot receive.
///
/// The driver calls [`prepare`](Multishot::prepare) when the [`MultiOp`]
/// holding the request is first polled, or polled again after
/// [`MultiOp::rearm`], and [`item`](Multishot::item) for each completion.
///
/// # Safety
///
/// As for [`Operation`](crate::Operation), except that the entry must be a
/// multishot request: every completion but the last carries
/// `IORING_CQE_F_MORE`. The driver keeps the value until the last one, even
/// if the stream is dropped first.
pub unsafe trait Multishot: 'static {
    /// What each completion produces.
    type Item;

    /// Fills in the submission entry, which arrives zeroed. The driver sets
    /// `user_data` afterwards.
    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe);

    /// Turns a completion into an item.
    ///
    /// This runs for every completion, also those arriving after the stream
    /// is dropped, whose items are dropped at once, so an item that owns a
    /// resource, such as a leased buffer, releases it.
    fn item(&mut self, completion: Completion) -> Self::Item;
}

/// The value of a multishot request whose stream was dropped.
pub(crate) struct Stranded<T>(pub(crate) T);

impl<T: Multishot> Orphan for Stranded<T> {
    fn deliver(&mut self, completion: Completion) {
        drop(self.0.item(completion));
    }
}

/// The items of a multishot request, which is submitted when the stream is
/// first polled. Made by [`Driver::stream`].
///
/// When the kernel ends the request, the stream returns `None` until
/// [`rearm`](MultiOp::rearm) submits it again. Dropping the stream while the
/// request is armed cancels it; the driver keeps the value, and hands it the
/// rest of the completions, until the kernel is done.
#[must_use = "a stream does nothing until polled"]
pub struct MultiOp<T: Multishot> {
    driver: Driver,
    state: State<T>,
}

enum State<T> {
    /// To be submitted at the next poll.
    Ready(T),
    Armed {
        op: T,
        user_data: u64,
    },
    /// The last completion has been taken.
    Ended(T),
    /// Only while a transition moves the value from one state to the next;
    /// nothing that can panic runs meanwhile.
    Taken,
}

impl<T: Multishot> MultiOp<T> {
    pub(crate) fn new(driver: Driver, op: T) -> Self {
        Self {
            driver,
            state: State::Ready(op),
        }
    }

    /// The next item, submitting the request first if it is not armed, or
    /// `None` if the request has ended.
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<T::Item>> {
        match &mut self.state {
            State::Ready(op) => match self.driver.queue_stream(op, cx.waker()) {
                Ok(user_data) => {
                    self.state = match mem::replace(&mut self.state, State::Taken) {
                        State::Ready(op) => State::Armed { op, user_data },
                        _ => unreachable!(),
                    };
                    Poll::Pending
                }
                Err(err) => {
                    let item = op.item(Completion::from_error(&err));
                    self.end();
                    Poll::Ready(Some(item))
                }
            },
            State::Armed { op, user_data } => {
                let Some(completion) = self.driver.poll_stream(*user_data, cx.waker()) else {
                    return Poll::Pending;
                };
                let last = completion.flags() & sys::IORING_CQE_F_MORE == 0;
                // The value stays in place while it makes the item, so that a
                // panic there leaves it for `drop` to hand to the driver.
                let item = op.item(completion);
                if last {
                    self.end();
                }
                Poll::Ready(Some(item))
            }
            State::Ended(_) => Poll::Ready(None),
            State::Taken => unreachable!("a stream's value is always in place"),
        }
    }

    /// The next item, or `None` if the request has ended; see
    /// [`poll_next`](MultiOp::poll_next).
    pub async fn next(&mut self) -> Option<T::Item> {
        poll_fn(|cx| self.poll_next(cx)).await
    }

    /// Whether the request is submitted and has not ended.
    pub fn is_armed(&self) -> bool {
        matches!(self.state, State::Armed { .. })
    }

    /// Submits the request again at the next poll, once it has ended. Does
    /// nothing otherwise.
    pub fn rearm(&mut self) {
        if let State::Ended(_) = self.state {
            self.state = match mem::replace(&mut self.state, State::Taken) {
                State::Ended(op) => State::Ready(op),
                _ => unreachable!(),
            };
        }
    }

    fn end(&mut self) {
        self.state = match mem::replace(&mut self.state, State::Taken) {
            State::Ready(op) | State::Armed { op, .. } | State::Ended(op) => State::Ended(op),
            State::Taken => unreachable!(),
        };
    }
}

impl<T: Multishot> Drop for MultiOp<T> {
    fn drop(&mut self) {
        if let State::Armed { op, user_data } = mem::replace(&mut self.state, State::Taken) {
            self.driver.abandon(user_data, op);
        }
    }
}

// A stream never pins its value: by the contract of `Multishot`, the memory
// the kernel uses is not inside it.
impl<T: Multishot> Unpin for MultiOp<T> {}

impl<T: Multishot> fmt::Debug for MultiOp<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = match self.state {
            State::Ready(_) => "ready",
            State::Armed { .. } => "armed",
            State::Ended(_) => "ended",
            State::Taken => "taken",
        };
        f.debug_struct("MultiOp")
            .field("state", &state)
            .finish_non_exhaustive()
    }
}
