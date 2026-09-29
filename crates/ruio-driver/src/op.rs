//! Operations, their results, and the futures that run them.

use std::fmt;
use std::future::Future;
use std::io;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use ruio_sys as sys;

use crate::Driver;

/// The result the kernel posted for an operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Completion {
    res: i32,
    flags: u32,
}

impl Completion {
    /// A completion with the given result and `IORING_CQE_F_*` flags.
    pub const fn new(res: i32, flags: u32) -> Self {
        Self { res, flags }
    }

    /// A completion reporting `err`, for an operation that could not be
    /// submitted.
    pub(crate) fn from_error(err: &io::Error) -> Self {
        Self::new(-err.raw_os_error().unwrap_or(libc::EIO), 0)
    }

    /// The raw result: what the equivalent system call returns, or a negated
    /// errno.
    pub fn res(&self) -> i32 {
        self.res
    }

    /// The `IORING_CQE_F_*` flags.
    pub fn flags(&self) -> u32 {
        self.flags
    }

    /// The result as an [`io::Result`]: the value if it is not negative,
    /// otherwise the error its negation names.
    pub fn result(&self) -> io::Result<u32> {
        u32::try_from(self.res).map_err(|_| io::Error::from_raw_os_error(-self.res))
    }
}

/// An io_uring request: how to fill in its submission entry, and what to make
/// of its completion.
///
/// The driver calls [`prepare`](Operation::prepare) when the [`Op`] holding
/// the operation is first polled. From then on it keeps the operation alive
/// until the kernel posts the completion, even if the `Op` is dropped first,
/// and then passes the completion to [`complete`](Operation::complete).
///
/// # Safety
///
/// Every address `prepare` writes into the entry must stay valid for what the
/// kernel does with it until `complete` is called, or for as long as the
/// operation exists if it never is. The driver moves the operation around
/// (into a heap allocation, if its `Op` is dropped early), so the entry must
/// not point into the operation itself: point it at heap memory the
/// operation owns, such as the contents of a `Box` or `Vec`, or at `'static`
/// memory.
///
/// The entry must produce exactly one completion: no multishot flags,
/// `IOSQE_CQE_SKIP_SUCCESS` or links. `prepare` must not use the driver.
pub unsafe trait Operation: 'static {
    /// What the operation produces.
    type Output;

    /// Fills in the submission entry, which arrives zeroed. The driver sets
    /// `user_data` afterwards.
    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe);

    /// Turns the completion into the output.
    ///
    /// If the operation's [`Op`] was dropped, this still runs when the
    /// completion arrives and the output is dropped, so an output that owns
    /// a resource, such as a new file descriptor, releases it.
    fn complete(self, completion: Completion) -> Self::Output;
}

/// The payload of a request whose future was dropped before the kernel
/// finished with it.
pub(crate) trait Orphan {
    /// Hands the payload one of its completions; after the last, the driver
    /// drops it.
    fn deliver(&mut self, completion: Completion);
}

/// A single-shot operation, until its completion arrives.
impl<T: Operation> Orphan for Option<T> {
    fn deliver(&mut self, completion: Completion) {
        if let Some(operation) = self.take() {
            drop(operation.complete(completion));
        }
    }
}

/// A future that submits an operation when first polled and resolves to its
/// output.
///
/// Dropping an `Op` whose operation is in flight cancels the operation. The
/// driver keeps the operation, and the memory the kernel may still be using,
/// until the kernel reports it done.
///
/// [`timeout`](Op::timeout) bounds how long the kernel may take over the
/// operation, at no cost until then: nothing is submitted before the first
/// poll.
#[must_use = "an operation does nothing until polled"]
pub struct Op<T: Operation> {
    driver: Driver,
    state: State<T>,
    timeout: Option<Duration>,
}

enum State<T> {
    Unsubmitted(T),
    Submitted {
        operation: T,
        user_data: u64,
        /// Whether a linked timeout went to the kernel with it.
        timed: bool,
    },
    Done,
}

impl<T: Operation> Op<T> {
    pub(crate) fn new(driver: Driver, operation: T) -> Self {
        Self {
            driver,
            state: State::Unsubmitted(operation),
            timeout: None,
        }
    }

    /// Cancels the operation if it has not completed `duration` after it is
    /// submitted, with a linked timeout (`IORING_OP_LINK_TIMEOUT`) that goes
    /// to the kernel with it.
    ///
    /// An operation cancelled this way fails with `ETIMEDOUT`
    /// ([`io::ErrorKind::TimedOut`]), and hands back what it holds, such as
    /// its buffer, as it does with any other error. Dropping the `Op`
    /// instead, as a timer in user space would, gives the operation to the
    /// driver, and its buffer with it. An operation that the kernel cannot
    /// interrupt completes as usual, however long it takes.
    ///
    /// This has no effect once the operation has been submitted.
    pub fn timeout(mut self, duration: Duration) -> Self {
        self.timeout = Some(duration);
        self
    }
}

/// The completion as the operation sees it. For an operation with a linked
/// timeout, cancelled means timed out: nothing else cancels an operation
/// whose `Op` still exists.
fn seen(completion: Completion, timed: bool) -> Completion {
    if timed && completion.res() == -libc::ECANCELED {
        Completion::new(-libc::ETIMEDOUT, completion.flags())
    } else {
        completion
    }
}

impl<T: Operation> Future for Op<T> {
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T::Output> {
        let this = self.get_mut();
        match mem::replace(&mut this.state, State::Done) {
            State::Unsubmitted(mut operation) => {
                match this.driver.queue(&mut operation, this.timeout, cx.waker()) {
                    Ok(user_data) => {
                        this.state = State::Submitted {
                            operation,
                            user_data,
                            timed: this.timeout.is_some(),
                        };
                        Poll::Pending
                    }
                    Err(err) => Poll::Ready(operation.complete(Completion::from_error(&err))),
                }
            }
            State::Submitted {
                operation,
                user_data,
                timed,
            } => match this.driver.poll_completion(user_data, cx.waker()) {
                Some(completion) => Poll::Ready(operation.complete(seen(completion, timed))),
                None => {
                    this.state = State::Submitted {
                        operation,
                        user_data,
                        timed,
                    };
                    Poll::Pending
                }
            },
            State::Done => panic!("`Op` polled after it completed"),
        }
    }
}

impl<T: Operation> Drop for Op<T> {
    fn drop(&mut self) {
        if let State::Submitted {
            operation,
            user_data,
            ..
        } = mem::replace(&mut self.state, State::Done)
        {
            self.driver.orphan(user_data, operation);
        }
    }
}

// An `Op` never pins its operation: the memory the kernel uses is not inside
// it, by the contract of `Operation`.
impl<T: Operation> Unpin for Op<T> {}

impl<T: Operation> fmt::Debug for Op<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = match self.state {
            State::Unsubmitted(_) => "unsubmitted",
            State::Submitted { .. } => "submitted",
            State::Done => "done",
        };
        f.debug_struct("Op")
            .field("state", &state)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_decodes_errors() {
        assert_eq!(Completion::new(7, 0).result().unwrap(), 7);
        let err = Completion::new(-2, 0).result().unwrap_err();
        assert_eq!(err.raw_os_error(), Some(2));
    }

    #[test]
    fn only_timed_operations_see_cancellation_as_a_timeout() {
        let cancelled = Completion::new(-libc::ECANCELED, 0);
        assert_eq!(seen(cancelled, true).res(), -libc::ETIMEDOUT);
        assert_eq!(seen(cancelled, false), cancelled);
        let done = Completion::new(3, 0);
        assert_eq!(seen(done, true), done);
    }

    #[test]
    fn submission_errors_become_completions() {
        let err = io::Error::from_raw_os_error(libc::EBUSY);
        assert_eq!(Completion::from_error(&err).res(), -libc::EBUSY);
        let err = io::Error::other("not an OS error");
        assert_eq!(Completion::from_error(&err).res(), -libc::EIO);
    }
}
