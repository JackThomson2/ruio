//! Networking: sockets whose I/O runs on the ring of the thread's runtime.
//!
//! Sockets move data with owned buffers, as the ring's operations do: a read
//! takes a buffer and hands it back filled, a write takes one and hands it
//! back once sent, each with its result. Any [`IoBuf`](crate::buf::IoBuf)
//! will do, a slice of one or a lease from a [`BufPool`](crate::buf::BufPool)
//! included. A `recv_stream` receives into a pool instead: the kernel picks a
//! buffer only once data arrives, so idle connections pin no memory, and one
//! multishot request keeps receiving. A UDP socket's
//! [`recv_from_stream`](UdpSocket::recv_from_stream) does so too, with each
//! datagram's sender, for a socket that is not connected to answer. A
//! listener's `incoming` accepts connection after connection with one
//! multishot request too.
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
//!             while let Some(Ok(mut data)) = received.next().await {
//!                 // Whatever else has arrived goes with it.
//!                 while let Some(Ok(more)) = received.next_ready() {
//!                     data.append(more);
//!                 }
//!                 // Nothing needs the buffers back: write without waiting.
//!                 if stream.write_behind(data).await.is_err() {
//!                     break;
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
//! A server on several cores has a listener on each, all bound to one
//! address with [`TcpListener::bind_reuseport`], and the kernel spreads the
//! connections among them; [`UdpSocket::bind_reuseport`] does the same for
//! datagrams. A connection can also move from one core to another, with
//! [`TcpStream::send_to`], for a task spawned there with
//! [`Handle::spawn_with`](crate::Handle::spawn_with) to receive.
//!
//! [`TcpStream::connect`] takes this module's [`ToSocketAddrs`]: an
//! address, or a string that parses as one, resolves at once, with no
//! allocation and no runtime, and a host name resolves with
//! `getaddrinfo(3)` on the blocking pool, so that the thread's tasks run
//! meanwhile; [`lookup_host`] resolves one so for anything else. The binds
//! take the standard library's trait, and resolve a host name on the
//! thread, as a program binds its listeners as it starts.
//!
//! Like the ring their I/O runs on, sockets belong to their thread: they are
//! neither `Send` nor `Sync`.
//!
//! ```compile_fail
//! fn assert_send<T: Send>() {}
//! assert_send::<ruio::net::TcpStream>();
//! ```
//!
//! Code whose futures must be `Send`, such as `async_trait`'s, or that moves
//! a connection between cores, has the sockets of [`portable`], which are
//! `Send` and `Sync`: their reads and writes are system calls made at once
//! on whichever thread polls them, which wait on that thread's ring only
//! when there is nothing to read or no room to write.

mod addr;
mod behind;
pub mod portable;
mod socket;
mod tcp;
mod udp;
mod unix;

pub use addr::{ToSocketAddrs, lookup_host};
pub use behind::{Flush, WriteBehind};
pub(crate) use socket::{Desc, Plain, Sock, ThreadBound};
pub use socket::{ReadExact, RecvStream, SendZc, SocketOp, WriteAll, WriteAllVectored};
pub use tcp::{Incoming, SentTcpStream, TcpListener, TcpStream};
pub use udp::{Datagram, RecvFromStream, UdpSocket};
pub use unix::{UnixDatagram, UnixListener, UnixStream};
