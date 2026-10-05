//! Chains of linked operations, which the kernel runs one after another.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::driver::Waiter;
use crate::{Driver, Op, Operation};

pub(crate) mod private {
    //! The workings of a chain, which only this crate implements.

    use std::io;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use ruio_sys as sys;

    use crate::Driver;
    use crate::orphan::Payload;

    /// What [`Chain`](super::Chain) asks of a part of a chain: an `Op`, or
    /// a `Link` of parts.
    pub trait Linkable {
        /// What the part produces.
        type Output;

        /// The ring the part's operations are on.
        fn driver(&self) -> &Driver;

        /// Whether none of the part's operations has been submitted, so that
        /// it can still join a chain.
        fn unsubmitted(&self) -> bool;

        /// Adds the part's operations to `members`, in order, and records
        /// that a chain holds them, so that the part does not queue them
        /// itself.
        fn members<'a>(&'a mut self, members: &mut Vec<&'a mut dyn Member>);

        /// Polls the part's operations, once they are queued.
        fn poll_chain(&mut self, cx: &mut Context<'_>) -> Poll<Self::Output>;
    }

    /// An operation of a chain, as the driver queues it.
    pub trait Member {
        /// The operation's linked timeout, if it has one.
        fn timeout(&self) -> Option<Duration>;

        /// Whether the operation asks for the polled ring, which takes no
        /// chain; see `Operation::polled`.
        fn polled(&self) -> bool;

        /// Fills in the operation's entry; see `Operation::prepare`.
        fn prepare(&mut self, sqe: &mut sys::io_uring_sqe);

        /// Records that the operation was queued as `user_data`, the first
        /// of its chain or not.
        fn queued(&mut self, user_data: u64, first: bool);

        /// Takes the operation, queued with nobody waiting for it, as the
        /// payload of its slot, the first of its chain or not.
        fn detached(&mut self, first: bool) -> Payload;

        /// Records that the chain could not be queued, for `error`.
        fn refused(&mut self, error: &io::Error);
    }
}

use private::Linkable;

/// What can be part of a chain of linked operations: an [`Op`] that has not
/// been submitted, or a [`Link`] of them. Only this crate implements it.
pub trait Chain: Linkable {}

impl<T: Operation> Chain for Op<T> {}

impl<A: Chain, B: Chain> Chain for Link<A, B> {}

/// Operations linked into a chain (`IOSQE_IO_LINK`), which the kernel runs
/// one after another, each once the one before has succeeded; made by
/// [`Op::link`] and [`Link::link`]. The output is every operation's, in a
/// pair of the first part's and the second's.
///
/// The chain is queued as a whole when it is first polled, and reaches the
/// kernel in one submission, as the kernel only links entries submitted
/// together: it fails if the submission queue cannot hold all of it. An
/// operation that fails, or is cancelled, cancels every one after it, which
/// completes with `ECANCELED` and hands back what it holds, such as its
/// buffer. The kernel counts a read or a write that moves fewer bytes than
/// it asked for as failing, and so a send or a receive with `MSG_WAITALL`.
///
/// Each operation's [`timeout`](Op::timeout) bounds it alone, from when it
/// starts, and cancels the rest of the chain if it fires. The first
/// operation reports its timeout as `ETIMEDOUT`; a later one cannot tell
/// its own timeout from a failure before it, and reports either as
/// `ECANCELED`. Dropping the chain cancels what of it is in flight.
///
/// ```no_run
/// use ruio_driver::Driver;
/// use ruio_driver::ops::{Fsync, Write};
///
/// # fn run(driver: &Driver, fd: std::os::fd::RawFd) {
/// // Written, then flushed; the flush is cancelled if the write fails.
/// let chain = driver
///     .op(Write::at(fd, b"durable".to_vec(), 0))
///     .link(driver.op(Fsync::new(fd)));
/// # let _ = chain;
/// # }
/// ```
#[must_use = "a chain does nothing until polled"]
pub struct Link<A: Chain, B: Chain> {
    first: A,
    second: B,
    first_output: Option<A::Output>,
    second_output: Option<B::Output>,
    /// Whether the chain's operations are queued, by this link or by a
    /// chain holding it.
    queued: bool,
}

impl<A: Chain, B: Chain> Link<A, B> {
    /// Links `second` after `first`, as [`Op::link`] and [`Link::link`] do.
    ///
    /// # Panics
    ///
    /// If an operation of either has been submitted, or they are on
    /// different rings.
    pub fn new(first: A, second: B) -> Self {
        assert!(
            first.unsubmitted() && second.unsubmitted(),
            "an operation can be linked only before it is submitted"
        );
        assert!(
            first.driver().same_ring(second.driver()),
            "linked operations must be on the same ring"
        );
        Self {
            first,
            second,
            first_output: None,
            second_output: None,
            queued: false,
        }
    }

    /// Links `next` after the chain's last operation, which it runs once
    /// every one before has succeeded.
    ///
    /// # Panics
    ///
    /// If an operation of `next` has been submitted, or it is on another
    /// ring.
    pub fn link<C: Chain>(self, next: C) -> Link<Self, C> {
        Link::new(self, next)
    }
}

impl<A: Chain, B: Chain> Linkable for Link<A, B> {
    type Output = (A::Output, B::Output);

    fn driver(&self) -> &Driver {
        self.first.driver()
    }

    fn unsubmitted(&self) -> bool {
        !self.queued
    }

    fn members<'a>(&'a mut self, members: &mut Vec<&'a mut dyn private::Member>) {
        self.queued = true;
        self.first.members(members);
        self.second.members(members);
    }

    fn poll_chain(&mut self, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.first_output.is_none()
            && let Poll::Ready(output) = self.first.poll_chain(cx)
        {
            self.first_output = Some(output);
        }
        if self.second_output.is_none()
            && let Poll::Ready(output) = self.second.poll_chain(cx)
        {
            self.second_output = Some(output);
        }
        if self.first_output.is_some() && self.second_output.is_some() {
            let first = self.first_output.take().expect("checked above");
            let second = self.second_output.take().expect("checked above");
            Poll::Ready((first, second))
        } else {
            Poll::Pending
        }
    }
}

impl<A: Chain, B: Chain> Future for Link<A, B> {
    type Output = (A::Output, B::Output);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if !this.queued {
            let driver = this.driver().clone();
            let mut members = Vec::new();
            this.members(&mut members);
            driver.queue_chain(&mut members, Waiter::Task(cx.waker()));
        }
        this.poll_chain(cx)
    }
}

// A chain never pins its parts: an `Op` is `Unpin`, and so is every `Link`
// of them; the outputs are only moved.
impl<A: Chain, B: Chain> Unpin for Link<A, B> {}

impl<A: Chain + fmt::Debug, B: Chain + fmt::Debug> fmt::Debug for Link<A, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Link")
            .field("first", &self.first)
            .field("second", &self.second)
            .finish_non_exhaustive()
    }
}
