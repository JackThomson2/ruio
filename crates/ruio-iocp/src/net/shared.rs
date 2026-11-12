//! A listener that the engines of a process share: its socket is on no
//! port, and each engine's thread accepts from it, waiting on its own port.

use std::fmt;
use std::future::Future;
use std::io;
use std::mem::ManuallyDrop;
use std::net::{self, SocketAddr, ToSocketAddrs};
use std::os::windows::io::{AsRawSocket, FromRawSocket};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, ready};

use crate::engine::Iocp;
use crate::handle::IoHandle;
use crate::portable::{self, ReadyWait, events};

use super::{Socket, TcpStream};

/// A TCP listener that the engines of a process share, which is `Send` and
/// `Sync`: its socket, made nonblocking, is on no port, and an accept on
/// any engine's thread takes a connection with a call made at once, and
/// puts the stream on that engine's port.
///
/// An accept that finds no connection waiting waits for the listener to be
/// ready on the port of its own thread's engine, with a poll of the AFD
/// driver's, as a [`ReadyWait`] does, and then tries again. So engines that
/// accept from one listener each take a share of its connections, as many
/// as each is free to take: when a connection arrives, the polls of all the
/// engines that wait complete, and the first to accept takes it, while the
/// others find none and wait again. An engine busy with other work takes
/// fewer, and one that stops accepting holds up none of the others.
///
/// This is what ruio's listeners bound with `SO_REUSEPORT` do on Linux,
/// where the kernel spreads the connections among several sockets. Windows
/// has no such option: of several sockets bound to one address with
/// `SO_REUSEADDR`, one takes every connection, and of several `AcceptEx`
/// waiting on one listener, the one that started last takes the next
/// connection, so that the engine that accepted last would accept again.
///
/// An accept costs no turn of the port when a connection is waiting, and
/// is counted with the operations that finish as they start: once as many
/// have finished so since the port was last turned as may between two, it
/// wakes its task and waits for the next turn, so that a flood of
/// connections keeps no other task from running.
pub struct SharedListener {
    socket: portable::Socket,
}

impl SharedListener {
    /// Binds a listener to `addr`, or to the first address it resolves to
    /// that it can be bound to, as [`TcpListener::bind`](super::TcpListener::bind)
    /// does, with the backlog of the standard library's listeners: a host
    /// name is resolved on the calling thread, and one on an IPv6 address
    /// accepts connections from IPv4 addresses too. No engine need be
    /// current.
    ///
    /// # Errors
    ///
    /// What binding and listening report, as when the address is in use,
    /// or `InvalidInput` if `addr` resolves to no address.
    pub fn bind(addr: impl ToSocketAddrs) -> io::Result<Self> {
        Ok(Self {
            socket: portable::Socket::listen(addr)?,
        })
    }

    /// The address the listener is bound to.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        // SAFETY: the socket is the listener's own, open for as long as the
        // listener is, which the view does not outlive; the view is never
        // dropped, so it closes nothing.
        let view = ManuallyDrop::new(unsafe {
            net::TcpListener::from_raw_socket(self.socket.as_raw_socket())
        });
        view.local_addr()
    }

    /// Accepts a connection on the thread that polls the future, for the
    /// port of that thread's engine; see [`SharedAccept`].
    pub fn accept(&self) -> SharedAccept<'_> {
        SharedAccept {
            listener: self,
            wait: ReadyWait::new(),
        }
    }
}

impl fmt::Debug for SharedListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedListener")
            .field("socket", &self.socket)
            .finish()
    }
}

/// The future of [`SharedListener::accept`]: the stream of a connection,
/// on the port of the engine of the thread that polled the future last,
/// and the address of its peer.
///
/// It takes a connection that waits at once, and otherwise waits for one
/// on the port of the thread's engine, as the listener says. It has no
/// timeout of its own, as an engine keeps deadlines only for the operations
/// that go through its port; a timer of the runtime's bounds it. Dropping
/// it cancels its wait, and takes no connection.
#[must_use = "futures do nothing unless polled"]
pub struct SharedAccept<'a> {
    listener: &'a SharedListener,
    wait: ReadyWait,
}

impl Future for SharedAccept<'_> {
    type Output = io::Result<(TcpStream, SocketAddr)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let Some(engine) = Iocp::current() else {
            return Poll::Ready(Err(io::Error::other(
                "the thread has no ruio runtime for the accept to run on",
            )));
        };
        let socket = &this.listener.socket;
        loop {
            // A wait in flight goes on; one that has completed is done with,
            // whatever it found, as the accept after it tells.
            if this.wait.is_waiting()
                && let Err(err) = ready!(socket.poll_ready(cx, events::READABLE, &mut this.wait))
            {
                return Poll::Ready(Err(err));
            }
            // The accept costs no turn of the port, so it waits for one once
            // as many operations have finished as they started as may
            // between two.
            if engine.at_once_spent() {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            match socket.try_accept() {
                Ok((accepted, peer)) => {
                    engine.finished_at_once();
                    return Poll::Ready(adopt(accepted, &engine).map(|stream| (stream, peer)));
                }
                // Another engine took the connection first, or none came.
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
                Err(err) => return Poll::Ready(Err(err)),
            }
            match socket.poll_ready(cx, events::READABLE, &mut this.wait) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Ready(Ok(_)) => {}
            }
        }
    }
}

impl fmt::Debug for SharedAccept<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedAccept")
            .field("listener", self.listener)
            .field("wait", &self.wait)
            .finish()
    }
}

/// The stream of `accepted`, a connection's socket that the listener made
/// nonblocking, as it made itself, blocking again and on the port of
/// `engine`. It was made by an accept on a socket opened for overlapped I/O,
/// as it is, and so is one.
fn adopt(accepted: portable::Socket, engine: &Iocp) -> io::Result<TcpStream> {
    let stream = net::TcpStream::from(accepted.into_socket());
    let stream = IoHandle::on(Rc::new(Socket::new(stream)), engine.clone())?;
    Ok(TcpStream::new(stream))
}
