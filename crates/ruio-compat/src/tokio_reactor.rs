//! tokio's reactor as ruio's foreign reactor: [`TokioReactor`].

use std::io;
use std::os::fd::{AsRawFd, BorrowedFd, RawFd};
use std::task::{Context, Poll, ready};

use ruio::io::portable::Interest;
use ruio::io::{Reactor, Registration};
use tokio::io::unix::AsyncFd;

/// tokio's reactor, which ruio's portable sockets and listeners and a
/// child's pipes wait on from the threads of a tokio runtime once it is the
/// foreign reactor:
///
/// ```no_run
/// ruio::io::set_foreign_reactor(&ruio_compat::TokioReactor);
/// ```
///
/// A descriptor of ruio's that finds nothing to read, or no room, on a
/// thread of a tokio runtime is then registered with that runtime's reactor,
/// as a tokio socket is as it is made, and waits there from then on: the
/// reactor wakes the task once epoll says the descriptor is ready, and the
/// system call is made again under that readiness, which a call that would
/// block, or that moves less than it could, clears, as tokio's own sockets
/// do. So a wait costs what it costs a tokio socket, where on the foreign
/// ring it cost two wakes across threads.
///
/// The registration is with the runtime the descriptor first waited on, and
/// lasts until the descriptor closes: polled on the threads of another
/// runtime, it waits on the first one's reactor, which works for as long as
/// that runtime runs, as for a tokio socket moved between runtimes. Threads
/// of no runtime of tokio's wait on the foreign ring, and threads where a
/// runtime of ruio's runs on their own ring.
///
/// # Panics
///
/// A descriptor first waiting on a thread of a runtime without I/O enabled
/// (`enable_io`) panics, as a tokio socket made there does.
#[derive(Clone, Copy, Debug, Default)]
pub struct TokioReactor;

impl Reactor for TokioReactor {
    fn register(&self, fd: BorrowedFd<'_>) -> Option<io::Result<Box<dyn Registration>>> {
        tokio::runtime::Handle::try_current().ok()?;
        let interest = tokio::io::Interest::READABLE | tokio::io::Interest::WRITABLE;
        let registered = AsyncFd::with_interest(Number(fd.as_raw_fd()), interest);
        Some(registered.map(|fd| Box::new(Registered(fd)) as Box<dyn Registration>))
    }
}

/// A descriptor of ruio's, by its number, which tokio's reactor polls and
/// ruio keeps open for as long as the registration lives.
struct Number(RawFd);

impl AsRawFd for Number {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

/// A descriptor registered with a tokio runtime's reactor, which leaves it
/// as this is dropped.
struct Registered(AsyncFd<Number>);

impl Registration for Registered {
    #[inline]
    fn is_current(&self) -> bool {
        // A task's ID is a thread-local away; the future of `block_on` has
        // none, and the runtime's handle tells there.
        tokio::task::try_id().is_some() || tokio::runtime::Handle::try_current().is_ok()
    }

    fn poll_io(
        &self,
        cx: &mut Context<'_>,
        interest: Interest,
        len: usize,
        call: &mut dyn FnMut() -> io::Result<usize>,
    ) -> Poll<io::Result<usize>> {
        loop {
            let mut ready = ready!(if interest.is_writable() {
                self.0.poll_write_ready(cx)
            } else {
                self.0.poll_read_ready(cx)
            })?;
            match ready.try_io(|_| call()) {
                Ok(Ok(done)) => {
                    // Drained, or filled: the next call would only find so.
                    if 0 < done && done < len {
                        ready.clear_ready();
                    }
                    return Poll::Ready(Ok(done));
                }
                Ok(Err(err)) => return Poll::Ready(Err(err)),
                // The readiness is cleared, and the reactor waits for more.
                Err(_would_block) => {}
            }
        }
    }
}
