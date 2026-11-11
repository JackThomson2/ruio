//! Adapters from ruio's streams and files to the poll-based I/O traits that
//! the ecosystem builds on: `AsyncRead`, `AsyncBufRead` and `AsyncWrite` of
//! [`tokio::io`](https://docs.rs/tokio/latest/tokio/io/), with the `tokio`
//! feature, and of [`futures-io`](https://docs.rs/futures-io), with the
//! `futures-io` feature. Both are on by default. tokio's traits need none of
//! its runtime, which ruio's takes the place of.
//!
//! ruio's I/O is completion-based: an operation takes a buffer of its own,
//! the kernel fills or sends it, and the operation hands it back. The
//! poll-based traits lend the caller's buffer for one call instead, which
//! the kernel cannot use once the call returns.
#![cfg_attr(
    ruio_compat_ring,
    doc = r#"
[`Compat`] bridges the two with buffers of its own, for a
[`TcpStream`](ruio::net::TcpStream) or a
[`UnixStream`](ruio::net::UnixStream), a [`File`](ruio::fs::File) at its
position, or any other descriptor in an [`AsyncFd`](ruio::io::AsyncFd):

- A read receives into the adapter's buffer, and copies out as much as
  the caller's takes; the rest waits for the next read, and
  `AsyncBufRead` lends it out as it is. A receive stays in flight from
  one poll to the next, however long the caller takes to poll again, so
  nothing it receives is lost; dropping the adapter cancels it. An
  adapter made [`with_pool`](Compat::with_pool) receives into the
  buffers of a pool instead, with one multishot receive that stays
  armed, as [`TcpStream::recv_stream`](ruio::net::TcpStream::recv_stream)
  does, so that its reads cost the ring no request each.
- A write copies the caller's bytes and sends them, as
  [`TcpStream::write_behind`](ruio::net::TcpStream::write_behind) does:
  it is done, as far as the caller can tell, once the send is on its way,
  and the next write waits for it to finish. How the send went comes out
  later: a failure fails the writes, flushes and closes after it.
  Dropping the adapter leaves the send to finish, and the stream closes
  after it.
- A flush has nothing to wait for, since the adapter holds nothing back:
  it reports a failure if one is known, as a socket's flush would. Were
  it to wait for the send to finish, each flush would cost its task
  another wake-up, which a server flushing every response pays dearly for
  with many connections.
- Closing waits for the send in flight, then shuts down the writing half
  of a connection; a file or a descriptor has no half to shut.

Like the streams, an adapter belongs to its thread, whose runtime's ring
runs its I/O."#
)]
#![cfg_attr(
    ruio_compat_generic,
    doc = r#"
[`Compat`] bridges the two with buffers of its own, for a
[`TcpStream`](ruio::net::TcpStream) or a [`File`](ruio::fs::File) at its
position, with the stream's own reads and writes, which the traits of
[`ruio::io`] give it, and a stream socket's own `write_all`:

- A read goes into the adapter's buffer, and copies out as much as the
  caller's takes; the rest waits for the next read, and `AsyncBufRead`
  lends it out as it is. A read stays in flight from one poll to the
  next, however long the caller takes to poll again, so nothing it reads
  is lost; dropping the adapter cancels it.
- A write copies the caller's bytes and starts the stream's write of
  them, which for a socket sees them all out by itself: it is done, as
  far as the caller can tell, once the write is on its way, and the next
  write waits for it to finish. How the write went comes out later: a
  failure fails the writes, flushes and closes after it. Dropping the
  adapter leaves the write to finish, in a task of its own, and the
  stream closes after it; dropped outside a runtime, where no task would
  run, the adapter cancels the write instead.
- A flush has nothing to wait for, since the adapter holds nothing back:
  it reports a failure if one is known, as a socket's flush would. Were
  it to wait for the write to finish, each flush would cost its task
  another wake-up.
- Closing waits for the write in flight, then shuts down the writing half
  of a connection; a file has no half to shut.

The stream's reads and writes borrow the stream, so the adapter keeps it
where they can, shared, for as long as it lives: it lends the stream out
with [`get_ref`](Compat::get_ref), and has no way to hand it back. Like
the streams, an adapter belongs to its thread, whose runtime runs its
I/O.

This is the adapter of every engine but io_uring, which has one built on
the ring's own operations, with nothing shared and a pool to receive
into. A build for io_uring has this one instead when it asks for it with
`--cfg ruio_compat="generic"`, as the tests of this adapter do, and then
it adapts a `UnixStream` too."#
)]
#![cfg_attr(
    ruio_uring,
    doc = r#"
[`PortableCompat`] adapts ruio's portable streams, of
[`ruio::net::portable`], and a child's pipes, of [`ruio::process`], to
the same traits with no buffers of its own: they read and write with
system calls made at once on whichever thread polls them, so a read goes
into the caller's buffer and a write comes from the caller's, and the
adapter, as the stream, is `Send` and `Sync`, for what needs a connection
that is, as hyper-util's client does. A child's standard output and
error are read through it, and its standard input written.
With the `client` feature, [`client`] runs hyper-util's client on the
portable streams: a connector of portable TCP streams, and the client's
builder with ruio's executor and timer.

With the `tokio-reactor` feature, [`TokioReactor`] is tokio's reactor as
ruio's foreign one ([`ruio::io::set_foreign_reactor`]): set so, the
portable streams and listeners and a child's pipes polled on the threads
of a tokio runtime wait on that runtime's reactor, as its own sockets do,
rather than on the foreign ring, where each wait cost two wakes across
threads."#
)]
#![cfg_attr(
    ruio_iocp,
    doc = r#"
`PortableCompat`, the `client` module and `TokioReactor` are made of
io_uring's portable streams and of descriptors, and come with that engine
alone: the `client` and `tokio-reactor` features add nothing here."#
)]
#![cfg_attr(
    not(any(ruio_uring, ruio_iocp)),
    doc = r#"
Built for an engine ruio offers no I/O on, as the park engine is, the
crate has no stream to adapt: it has what asks nothing of an engine,
[`Executor`] and [`Timer`], and the features that name an adapter add
nothing."#
)]
//!
//! With the `hyper` feature, on by default, an adapter has hyper's own I/O
//! traits, `hyper::rt::Read` and `Write`, so hyper serves it, or connects
//! through it, as it is, with nothing between them; [`Executor`] spawns
//! the tasks hyper's HTTP/2 needs on the thread's runtime; and [`Timer`]
//! gives hyper ruio's timers, which its timeouts need: that for reading a
//! request's head, say.
#![cfg_attr(
    any(ruio_compat_ring, ruio_compat_generic),
    doc = r#"
```no_run
use ruio::net::TcpStream;
use ruio_compat::Compat;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[ruio::main]
async fn main() -> std::io::Result<()> {
    let mut stream = Compat::new(TcpStream::connect("127.0.0.1:7878").await?);
    stream.write_all(b"hello").await?;
    stream.shutdown().await?;
    let mut reply = Vec::new();
    stream.read_to_end(&mut reply).await?;
    Ok(())
}
```

With both families implemented, a method such as `write_all` is
ambiguous where the extension traits of both are in scope: import one
family's, or call through the trait, as in
`futures_util::AsyncWriteExt::write_all(&mut stream, data)`."#
)]

// io_uring's own: the client connects through the portable streams, which
// the portable adapter adapts and tokio's reactor serves.
#[cfg(all(feature = "client", ruio_uring))]
pub mod client;
#[cfg(feature = "hyper")]
mod executor;
#[cfg(all(feature = "futures-io", any(ruio_compat_ring, ruio_compat_generic)))]
mod futures_impls;
#[cfg(ruio_compat_generic)]
mod generic;
#[cfg(all(feature = "hyper", any(ruio_compat_ring, ruio_compat_generic)))]
mod hyper_impls;
#[cfg(ruio_uring)]
mod portable;
#[cfg(ruio_compat_ring)]
mod ring;
#[cfg(feature = "hyper")]
mod timer;
#[cfg(all(feature = "tokio", any(ruio_compat_ring, ruio_compat_generic)))]
mod tokio_impls;
#[cfg(all(feature = "tokio-reactor", ruio_uring))]
mod tokio_reactor;

#[cfg(feature = "hyper")]
pub use executor::Executor;
#[cfg(ruio_compat_generic)]
pub use generic::{Adaptable, Compat};
#[cfg(ruio_uring)]
pub use portable::{PortableCompat, PortableReader, PortableStream, PortableWriter};
#[cfg(ruio_compat_ring)]
pub use ring::{Adaptable, Compat, StreamSocket};
#[cfg(feature = "hyper")]
pub use timer::Timer;
#[cfg(all(feature = "tokio-reactor", ruio_uring))]
pub use tokio_reactor::TokioReactor;
