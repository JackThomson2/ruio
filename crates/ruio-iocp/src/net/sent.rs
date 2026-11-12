//! Streams sent from the port of one engine to that of another, as ruio
//! moves a stream from one core to another.

use std::fmt;
use std::future::poll_fn;
use std::io;
use std::rc::Rc;
use std::task::Poll;
use std::time::{Duration, Instant};

use crate::handle::IoHandle;

use super::{Socket, TcpStream};

/// How long [`TcpStream::send_off`] waits for the port to deliver what is
/// left of the operations on the stream's socket before it gives up: a
/// moment's work for the cancellations of those whose futures were dropped.
const PATIENCE: Duration = Duration::from_secs(1);

impl TcpStream {
    /// Takes the stream off the port of its engine, once the writes left
    /// behind it have finished, for the thread of another engine to take it
    /// with [`SentTcpStream::receive`]: as ruio moves a stream from one core
    /// to another.
    ///
    /// The system moves a socket off its port only once the port has
    /// delivered the completions of all of its operations. One cancelled as
    /// its future was dropped completes a moment after, and is delivered at
    /// a turn of the port: until then, the future waits for the next turn,
    /// and tries again.
    ///
    /// # Errors
    ///
    /// Hands the stream back with what a write behind failed with, or what
    /// moving the socket reports: `ResourceBusy` if an operation on it is
    /// still in flight a second on, as one on a socket duplicated from it
    /// may be, and `Unsupported` before Windows 8.1, which moves no socket
    /// off its port.
    pub async fn send_off(self) -> Result<SentTcpStream, (io::Error, Self)> {
        if let Err(err) = self.flush().await {
            return Err((err, self));
        }
        let mut stream = self;
        let mut deadline = None;
        loop {
            // The sends left behind the stream let go of its socket as they
            // finish, which they have.
            if Rc::strong_count(stream.stream.io()) > 1 {
                let err = io::Error::new(
                    io::ErrorKind::ResourceBusy,
                    "a write behind the stream holds its socket",
                );
                return Err((err, stream));
            }
            let TcpStream {
                stream: handle,
                behind,
            } = stream;
            let (err, handle) = match handle.release() {
                Ok(socket) => {
                    let socket = Rc::into_inner(socket).expect("the stream holds its socket alone");
                    return Ok(SentTcpStream { socket });
                }
                Err(refused) => refused,
            };
            stream = TcpStream {
                stream: handle,
                behind,
            };
            let deadline = *deadline.get_or_insert_with(|| Instant::now() + PATIENCE);
            if err.kind() != io::ErrorKind::ResourceBusy || Instant::now() >= deadline {
                return Err((err, stream));
            }
            next_turn().await;
        }
    }
}

/// Waits for the runtime to turn the port, which it does before it polls a
/// task again that woke itself.
async fn next_turn() {
    let mut yielded = false;
    poll_fn(|cx| {
        if yielded {
            return Poll::Ready(());
        }
        yielded = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .await;
}

/// A TCP stream taken off the port of its engine, on its way to the thread
/// of another: `Send`, unlike a [`TcpStream`]. Made by
/// [`TcpStream::send_off`]; [`receive`](SentTcpStream::receive) makes it a
/// stream again, on the port of the thread's current engine. Dropping it
/// closes the connection.
pub struct SentTcpStream {
    socket: Socket,
}

impl SentTcpStream {
    /// The stream, on the port of the thread's current engine, with no
    /// write behind it. It may be the engine the stream was sent off from.
    ///
    /// # Errors
    ///
    /// An error of kind `Other` if the thread has no current engine, as
    /// outside a runtime, and what associating the socket with the port
    /// reports. The connection is closed then.
    pub fn receive(self) -> io::Result<TcpStream> {
        Ok(TcpStream::new(IoHandle::new(Rc::new(self.socket))?))
    }
}

impl fmt::Debug for SentTcpStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SentTcpStream")
            .field("socket", &self.socket.socket)
            .finish()
    }
}
