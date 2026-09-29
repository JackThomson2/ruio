//! Networking: sockets whose I/O runs on the ring of the thread's runtime.
//!
//! Sockets move data with owned buffers, as the ring's operations do: a read
//! takes a buffer and hands it back filled, a write takes one and hands it
//! back once sent, each with its result. Any [`IoBuf`](crate::buf::IoBuf)
//! will do, a slice of one or a lease from a [`BufPool`](crate::buf::BufPool)
//! included. A `recv_stream` receives into a pool instead: the kernel picks a
//! buffer only once data arrives, so idle connections pin no memory, and one
//! multishot request keeps receiving. A listener's `incoming` accepts
//! connection after connection with one multishot request too.
//!
//! An operation borrows its socket, which so stays open until the kernel has
//! the operation:
//!
//! ```compile_fail
//! # async fn read(stream: ruio::net::TcpStream) {
//! let read = stream.read(Vec::with_capacity(8));
//! drop(stream);
//! let _ = read.await;
//! # }
//! ```
//!
//! [`SocketOp::timeout`], [`WriteAll::timeout`] and
//! [`ReadExact::timeout`] bound how long the kernel may take with a linked
//! timeout: an operation cut short fails with
//! [`TimedOut`](std::io::ErrorKind::TimedOut) and still hands back its
//! buffer.
//!
//! An echo server:
//!
//! ```no_run
//! use ruio::buf::BufPool;
//! use ruio::net::TcpListener;
//!
//! #[ruio::main]
//! async fn main() -> std::io::Result<()> {
//!     let listener = TcpListener::bind("127.0.0.1:7878")?;
//!     let pool = BufPool::new(&ruio::driver(), 1024, 4096)?;
//!     let mut incoming = listener.incoming();
//!     while let Some(stream) = incoming.next().await {
//!         let stream = stream?;
//!         let pool = pool.clone();
//!         ruio::spawn(async move {
//!             let mut received = stream.recv_stream(&pool);
//!             'echo: while let Some(Ok(data)) = received.next().await {
//!                 for lease in data {
//!                     if stream.write_all(lease).await.0.is_err() {
//!                         break 'echo;
//!                     }
//!                 }
//!             }
//!         });
//!     }
//!     Ok(())
//! }
//! ```
//!
//! Sockets have plain descriptors, unless a listener is asked for direct
//! ones ([`TcpListener::set_direct`]), which save the kernel looking up the
//! socket for every operation but cannot be used outside the ring.
//!
//! Like the ring their I/O runs on, sockets belong to their thread: they are
//! neither `Send` nor `Sync`.
//!
//! ```compile_fail
//! fn assert_send<T: Send>() {}
//! assert_send::<ruio::net::TcpStream>();
//! ```

mod socket;
mod tcp;
mod udp;
mod unix;

pub use socket::{ReadExact, RecvStream, SendZc, SocketOp, WriteAll};
pub use tcp::{Incoming, TcpListener, TcpStream};
pub use udp::UdpSocket;
pub use unix::{UnixDatagram, UnixListener, UnixStream};
