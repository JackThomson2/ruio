//! Addresses to connect to: literals, which resolve at once, and host
//! names, which resolve on the blocking pool.

use std::future::{Future, Ready, ready};
use std::io;
use std::iter::Copied;
use std::net::{self, IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::option;
use std::pin::Pin;
use std::slice;
use std::task::{Context, Poll, ready};
use std::vec;

use ruio_executor::blocking::BlockingHandle;

use self::sealed::{Internal, Sealed};

/// An address, or a host name and port, that resolves to socket addresses:
/// what [`TcpStream::connect`](super::TcpStream::connect) and
/// [`lookup_host`] take.
///
/// It is implemented for what the standard library's
/// [`ToSocketAddrs`](net::ToSocketAddrs) is implemented for: a
/// [`SocketAddr`], a [`SocketAddrV4`] or [`SocketAddrV6`], an IP address
/// with a port, a slice of addresses, a string such as `"127.0.0.1:80"`,
/// `"[::1]:80"` or `"example.com:80"`, a host with a port such as
/// `("example.com", 80)`, and references to any of these.
///
/// An address resolves to itself at once, with no allocation, no thread and
/// no runtime, and so does a string that parses as one. A host name goes to
/// the blocking pool, where `getaddrinfo(3)` resolves it on a thread of its
/// own ([`spawn_blocking`](crate::spawn_blocking)), so that the thread's
/// tasks run meanwhile; the lookup costs the string's copy, the job's
/// allocation and the addresses it comes back with.
///
/// The trait is sealed: only this crate's types implement it, and only this
/// crate resolves through it.
pub trait ToSocketAddrs: Sealed {}

/// Resolves `addr`: ready at once for a literal, or on the blocking pool
/// for a host name.
#[inline]
pub(crate) fn resolve<T: ToSocketAddrs + ?Sized>(addr: &T) -> T::Future {
    addr.to_socket_addrs(Internal)
}

/// Resolves `host` to its socket addresses, as
/// [`TcpStream::connect`](super::TcpStream::connect) resolves what it is
/// given: an address, or a string that parses as one, resolves at once, and
/// a host name resolves with `getaddrinfo(3)` on the blocking pool, so that
/// the thread's tasks run meanwhile. A bind that must not block resolves
/// its name here and binds the addresses, where
/// [`TcpListener::bind`](super::TcpListener::bind) would resolve on the
/// thread:
///
/// ```
/// use ruio::net::{TcpListener, lookup_host};
///
/// # ruio::Runtime::new()?.block_on(async {
/// let addrs: Vec<_> = lookup_host("localhost:0").await?.collect();
/// let listener = TcpListener::bind(&addrs[..])?;
/// assert!(listener.local_addr()?.ip().is_loopback());
/// # std::io::Result::Ok(())
/// # })?;
/// # Ok::<(), std::io::Error>(())
/// ```
///
/// The future can be awaited from any thread, inside a runtime or not.
///
/// # Errors
///
/// What `getaddrinfo(3)` reports, or `InvalidInput` if a string has no
/// port, or a port that is not a number.
pub async fn lookup_host(host: impl ToSocketAddrs) -> io::Result<impl Iterator<Item = SocketAddr>> {
    resolve(&host).await
}

/// The addresses a string resolves to: the one it parsed as, or those a
/// lookup found.
#[derive(Debug)]
pub enum OneOrMore {
    /// The address the string parsed as.
    One(option::IntoIter<SocketAddr>),
    /// The addresses `getaddrinfo(3)` found for the host name.
    More(vec::IntoIter<SocketAddr>),
}

impl Iterator for OneOrMore {
    type Item = SocketAddr;

    #[inline]
    fn next(&mut self) -> Option<SocketAddr> {
        match self {
            OneOrMore::One(one) => one.next(),
            OneOrMore::More(more) => more.next(),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            OneOrMore::One(one) => one.size_hint(),
            OneOrMore::More(more) => more.size_hint(),
        }
    }
}

/// Resolves a string: ready at once with the address it parsed as, or
/// awaiting the lookup of the host name on the blocking pool.
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
pub enum MaybeReady {
    /// The address the string parsed as, until it is taken.
    Ready(Option<SocketAddr>),
    /// The lookup, on a thread of the blocking pool.
    Lookup(BlockingHandle<io::Result<vec::IntoIter<SocketAddr>>>),
}

impl MaybeReady {
    /// Resolves `host` on the blocking pool, as the standard library does on
    /// the thread.
    fn lookup<T>(host: T) -> Self
    where
        T: net::ToSocketAddrs<Iter = vec::IntoIter<SocketAddr>> + Send + 'static,
    {
        MaybeReady::Lookup(ruio_executor::spawn_blocking(move || {
            net::ToSocketAddrs::to_socket_addrs(&host)
        }))
    }
}

impl Future for MaybeReady {
    type Output = io::Result<OneOrMore>;

    /// # Panics
    ///
    /// If polled again after it completed.
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            MaybeReady::Ready(addr) => {
                let addr = addr.take().expect("`MaybeReady` polled after it completed");
                Poll::Ready(Ok(OneOrMore::One(Some(addr).into_iter())))
            }
            MaybeReady::Lookup(handle) => {
                // A `JoinError` is a panic inside the lookup: the job cannot
                // be cancelled, as nothing but this holds its handle.
                let found = ready!(Pin::new(handle).poll(cx)).map_err(io::Error::other)?;
                Poll::Ready(found.map(OneOrMore::More))
            }
        }
    }
}

/// Ready at once with `addr`.
#[inline]
fn one(addr: SocketAddr) -> Ready<io::Result<option::IntoIter<SocketAddr>>> {
    ready(Ok(Some(addr).into_iter()))
}

/// The address types, each of which resolves to itself at once.
macro_rules! literal {
    ($($ty:ty),* $(,)?) => {$(
        impl ToSocketAddrs for $ty {}

        impl Sealed for $ty {
            type Iter = option::IntoIter<SocketAddr>;
            type Future = Ready<io::Result<Self::Iter>>;

            #[inline]
            fn to_socket_addrs(&self, _: Internal) -> Self::Future {
                one(SocketAddr::from(*self))
            }
        }
    )*};
}

literal!(
    SocketAddr,
    SocketAddrV4,
    SocketAddrV6,
    (IpAddr, u16),
    (Ipv4Addr, u16),
    (Ipv6Addr, u16),
);

impl ToSocketAddrs for &[SocketAddr] {}

impl<'a> Sealed for &'a [SocketAddr] {
    type Iter = Copied<slice::Iter<'a, SocketAddr>>;
    type Future = Ready<io::Result<Self::Iter>>;

    #[inline]
    fn to_socket_addrs(&self, _: Internal) -> Self::Future {
        ready(Ok(self.iter().copied()))
    }
}

impl ToSocketAddrs for str {}

/// A string such as `"127.0.0.1:80"`, `"[::1]:80"` or `"example.com:80"`.
impl Sealed for str {
    type Iter = OneOrMore;
    type Future = MaybeReady;

    #[inline]
    fn to_socket_addrs(&self, _: Internal) -> Self::Future {
        match self.parse::<SocketAddr>() {
            Ok(addr) => MaybeReady::Ready(Some(addr)),
            Err(_) => MaybeReady::lookup(self.to_owned()),
        }
    }
}

impl ToSocketAddrs for String {}

impl Sealed for String {
    type Iter = OneOrMore;
    type Future = MaybeReady;

    #[inline]
    fn to_socket_addrs(&self, _: Internal) -> Self::Future {
        Sealed::to_socket_addrs(self.as_str(), Internal)
    }
}

impl ToSocketAddrs for (&str, u16) {}

/// A host with a port, such as `("127.0.0.1", 80)` or `("example.com", 80)`.
impl Sealed for (&str, u16) {
    type Iter = OneOrMore;
    type Future = MaybeReady;

    #[inline]
    fn to_socket_addrs(&self, _: Internal) -> Self::Future {
        let (host, port) = *self;
        match host.parse::<IpAddr>() {
            Ok(ip) => MaybeReady::Ready(Some(SocketAddr::new(ip, port))),
            Err(_) => MaybeReady::lookup((host.to_owned(), port)),
        }
    }
}

impl ToSocketAddrs for (String, u16) {}

impl Sealed for (String, u16) {
    type Iter = OneOrMore;
    type Future = MaybeReady;

    #[inline]
    fn to_socket_addrs(&self, _: Internal) -> Self::Future {
        Sealed::to_socket_addrs(&(self.0.as_str(), self.1), Internal)
    }
}

impl<T: ToSocketAddrs + ?Sized> ToSocketAddrs for &T {}

impl<T: ToSocketAddrs + ?Sized> Sealed for &T {
    type Iter = T::Iter;
    type Future = T::Future;

    #[inline]
    fn to_socket_addrs(&self, _: Internal) -> Self::Future {
        resolve(*self)
    }
}

mod sealed {
    use std::future::Future;
    use std::io;
    use std::net::SocketAddr;

    /// How a [`ToSocketAddrs`](super::ToSocketAddrs) resolves: the addresses
    /// it resolves to, and the future that resolves them.
    pub trait Sealed {
        /// The addresses resolved to.
        type Iter: Iterator<Item = SocketAddr>;
        /// Resolves them: ready at once for a literal, or awaiting the
        /// blocking pool for a host name.
        type Future: Future<Output = io::Result<Self::Iter>>;
        /// Starts resolving. Only this crate can name [`Internal`], so only
        /// this crate can call it.
        fn to_socket_addrs(&self, _: Internal) -> Self::Future;
    }

    /// The token [`Sealed::to_socket_addrs`] takes, which nothing outside
    /// the crate can name.
    #[derive(Debug)]
    pub struct Internal;
}
