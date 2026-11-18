//! tokio's reactor as ruio's foreign reactor: [`TokioReactor`].

use std::io;
#[cfg(ruio_uring)]
use std::os::fd::{AsRawFd, BorrowedFd, RawFd};
#[cfg(ruio_iocp)]
use std::os::windows::io::BorrowedSocket;
use std::task::{Context, Poll, ready};

use ruio::io::portable::Interest;
use ruio::io::{Reactor, Registration};
#[cfg(ruio_uring)]
use tokio::io::unix::AsyncFd;

#[cfg_attr(
    ruio_uring,
    doc = r#"tokio's reactor, which ruio's portable sockets and listeners and a
child's pipes wait on from the threads of a tokio runtime once it is the
foreign reactor:"#
)]
#[cfg_attr(
    ruio_iocp,
    doc = r#"tokio's reactor, which ruio's portable sockets and listeners wait on
from the threads of a tokio runtime once it is the foreign reactor:"#
)]
///
/// ```no_run
/// ruio::io::set_foreign_reactor(&ruio_compat::TokioReactor);
/// ```
#[cfg_attr(
    ruio_uring,
    doc = r#"
A descriptor of ruio's that finds nothing to read, or no room, on a thread
of a tokio runtime is then registered with that runtime's reactor, as a
tokio socket is as it is made, and waits there from then on: the reactor
wakes the task once epoll says the descriptor is ready, and the system
call is made again under that readiness, which a call that would block,
or that moves less than it could, clears, as tokio's own sockets do. So a
wait costs what it costs a tokio socket, where on the foreign ring it cost
two wakes across threads.

The registration is with the runtime the descriptor first waited on, and
lasts until the descriptor closes: polled on the threads of another
runtime, it waits on the first one's reactor, which works for as long as
that runtime runs, as for a tokio socket moved between runtimes. Threads
of no runtime of tokio's wait on the foreign ring, and threads where a
runtime of ruio's runs on their own ring.

# Panics

A descriptor first waiting on a thread of a runtime without I/O enabled
(`enable_io`) panics, as a tokio socket made there does."#
)]
#[cfg_attr(
    ruio_iocp,
    doc = r#"
A socket of ruio's that finds nothing to read, or no room, on a thread of
a tokio runtime is then registered with that runtime's reactor, as a
tokio socket is as it is made, and waits there from then on: the reactor
wakes the task once the AFD driver says the socket is ready, and the call
is made again under that readiness, which a call that would block, or
that moves less than it could, clears, as tokio's own sockets do. So a
wait costs what it costs a tokio socket, where it would otherwise have no
port to wait on.

tokio registers no socket but its own on Windows, having no `AsyncFd`
there, so the registration is a tokio `TcpStream` of a duplicate of the
socket's handle, made with `WSADuplicateSocketW`, as
[`try_clone_to_owned`](std::os::windows::io::BorrowedSocket::try_clone_to_owned)
makes it: the duplicate names the same socket, which tokio's reactor
polls with an AFD poll of its own, as it polls its own sockets, and
closes the duplicate as the registration is dropped, which leaves the
socket open on ruio's handle. Neither handle has a port, so tokio's polls
and those of ruio's runtimes, on their ports, go side by side. A
listener registers so too, and is readable once a connection waits to
be accepted. tokio's reactor on Windows watches a socket again only once
a call made through it finds the socket not ready, so the readiness that
a call moving less than it could leaves is forgotten with a call that
says it would block, made through tokio with no system call of its own.

The registration is with the runtime the socket first waited on, and
lasts until the socket closes: polled on the threads of another runtime,
it waits on the first one's reactor, which works for as long as that
runtime runs, as for a tokio socket moved between runtimes. Threads of no
runtime of tokio's have no wait, which fails as unsupported, and threads
where a runtime of ruio's runs wait on their own port.

A child's pipes give no sign that they have data, or room, short of an
operation that completes, and the foreign reactor takes sockets alone:
on a thread of a tokio runtime, a pipe's read that finds nothing, or
write that finds no room, fails as unsupported, as [`ruio::process`]
says.

# Errors

A socket whose duplicate cannot be made, or that tokio's reactor does
not take, is refused, and its waits fail as unsupported.

# Panics

A socket first waiting on a thread of a runtime without I/O enabled
(`enable_io`) panics, as a tokio socket made there does."#
)]
#[derive(Clone, Copy, Debug, Default)]
pub struct TokioReactor;

#[cfg(ruio_uring)]
impl Reactor for TokioReactor {
    fn register(&self, fd: BorrowedFd<'_>) -> Option<io::Result<Box<dyn Registration>>> {
        tokio::runtime::Handle::try_current().ok()?;
        let interest = tokio::io::Interest::READABLE | tokio::io::Interest::WRITABLE;
        let registered = AsyncFd::with_interest(Number(fd.as_raw_fd()), interest);
        Some(registered.map(|fd| Box::new(Registered(fd)) as Box<dyn Registration>))
    }
}

#[cfg(ruio_iocp)]
impl Reactor for TokioReactor {
    fn register(&self, socket: BorrowedSocket<'_>) -> Option<io::Result<Box<dyn Registration>>> {
        tokio::runtime::Handle::try_current().ok()?;
        // Readable and writable, as tokio registers its own sockets.
        let registered = socket
            .try_clone_to_owned()
            .and_then(|duplicate| tokio::net::TcpStream::from_std(duplicate.into()));
        Some(registered.map(|socket| Box::new(Registered(socket)) as Box<dyn Registration>))
    }
}

/// A descriptor of ruio's, by its number, which tokio's reactor polls and
/// ruio keeps open for as long as the registration lives.
#[cfg(ruio_uring)]
struct Number(RawFd);

#[cfg(ruio_uring)]
impl AsRawFd for Number {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

/// A descriptor registered with a tokio runtime's reactor, which leaves it
/// as this is dropped.
#[cfg(ruio_uring)]
struct Registered(AsyncFd<Number>);

/// A socket of ruio's registered with a tokio runtime's reactor, through a
/// duplicate of its handle that tokio takes for a stream of its own, and
/// closes as this is dropped, as it leaves the reactor; the socket stays
/// open on ruio's handle. Only the readiness and `try_io` of the stream are
/// used: ruio makes its calls on its own handle.
#[cfg(ruio_iocp)]
struct Registered(tokio::net::TcpStream);

#[cfg(ruio_uring)]
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

#[cfg(ruio_iocp)]
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
        let way = if interest.is_writable() {
            tokio::io::Interest::WRITABLE
        } else {
            tokio::io::Interest::READABLE
        };
        loop {
            ready!(if interest.is_writable() {
                self.0.poll_write_ready(cx)
            } else {
                self.0.poll_read_ready(cx)
            })?;
            // Made under the readiness, and through mio, which watches the
            // socket again once a call would block.
            match self.0.try_io(way, &mut *call) {
                Ok(done) => {
                    // Drained, or filled: the next call would only find so.
                    // tokio forgets the readiness for a call that would
                    // block, and mio then watches for more, which a call
                    // that says so without one asks of them both.
                    if 0 < done && done < len {
                        let forgotten = self.0.try_io(way, || {
                            io::Result::<()>::Err(io::ErrorKind::WouldBlock.into())
                        });
                        debug_assert!(forgotten.is_err());
                    }
                    return Poll::Ready(Ok(done));
                }
                // The readiness is cleared, and the reactor waits for more.
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
                Err(err) => return Poll::Ready(Err(err)),
            }
        }
    }
}
