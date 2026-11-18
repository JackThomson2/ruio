//! Helpers shared by the integration tests.

#![allow(dead_code)]

use std::future::Future;
use std::io;
use std::time::Duration;

#[cfg(ruio_compat_ring)]
use ruio::buf::BufPool;
use ruio::net::{TcpListener, TcpStream};
#[cfg(ruio_compat_generic)]
use ruio_compat::Adaptable;
use ruio_compat::Compat;
#[cfg(ruio_compat_ring)]
use ruio_compat::StreamSocket;

/// How long a test waits for anything before it is considered hung.
pub(crate) const PATIENCE: Duration = Duration::from_secs(10);

/// Waits for `future`, failing the test if it takes longer than
/// [`PATIENCE`].
pub(crate) async fn within<F: Future>(future: F) -> F::Output {
    ruio::time::timeout(PATIENCE, future)
        .await
        .expect("still waiting after PATIENCE")
}

/// A connected pair of streams on the loopback interface, the client's and
/// the server's, made with what every engine's `net` has.
pub(crate) async fn connected() -> io::Result<(TcpStream, TcpStream)> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let client = ruio::spawn(TcpStream::connect(addr));
    let (server, _) = within(listener.accept()).await?;
    Ok((client.await.unwrap()?, server))
}

/// A connected pair of streams on the loopback interface: the client's and
/// the server's, the server's direct if `direct` is set.
#[cfg(ruio_uring)]
pub(crate) async fn pair(direct: bool) -> io::Result<(TcpStream, TcpStream)> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    listener.set_direct(direct);
    let addr = listener.local_addr()?;
    let client = ruio::spawn(TcpStream::connect(addr));
    let (server, _) = within(listener.accept()).await?;
    assert_eq!(server.is_direct(), direct);
    Ok((client.await.unwrap()?, server))
}

/// How an adapter under test reads.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Reads {
    /// Into a buffer of its own, this many bytes at a time.
    Buffer(usize),
    /// Into a pool of its own, of this many buffers of this many bytes,
    /// which the ring's adapter alone can.
    #[cfg(ruio_compat_ring)]
    Pool(u16, u32),
}

impl Reads {
    /// Adapts `stream` to read so.
    #[cfg(ruio_compat_ring)]
    pub(crate) fn adapt<S: StreamSocket>(self, stream: S) -> Compat<S> {
        match self {
            Reads::Buffer(capacity) => Compat::with_capacity(capacity, stream),
            Reads::Pool(count, size) => {
                let pool = BufPool::new(count, size).unwrap();
                Compat::with_pool(&pool, stream)
            }
        }
    }

    /// Adapts `stream` to read so.
    #[cfg(ruio_compat_generic)]
    pub(crate) fn adapt<S: Adaptable>(self, stream: S) -> Compat<S> {
        let Reads::Buffer(capacity) = self;
        Compat::with_capacity(capacity, stream)
    }
}

/// `len` bytes that differ with `seed`, and repeat only every 251.
pub(crate) fn pattern(len: usize, seed: usize) -> Vec<u8> {
    (0..len).map(|index| ((index + seed) % 251) as u8).collect()
}
