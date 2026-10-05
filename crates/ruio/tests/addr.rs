//! Resolving addresses: literals at once, host names on the blocking pool.

mod support;

use std::future::Future;
use std::io::{self, ErrorKind};
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread;

use ruio::net::{TcpListener, TcpStream, ToSocketAddrs, lookup_host};

use support::within;

/// Polls `future` once, with a waker that wakes nothing.
fn poll_once<F: Future>(future: F) -> Poll<F::Output> {
    let mut future = pin!(future);
    future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
}

/// The addresses `addr` resolves to in one poll, outside any runtime: what
/// a literal takes, with no pool and no thread.
fn resolved_at_once(addr: impl ToSocketAddrs) -> Vec<SocketAddr> {
    match poll_once(lookup_host(addr)) {
        Poll::Ready(addrs) => addrs.unwrap().collect(),
        Poll::Pending => panic!("a literal resolves in one poll"),
    }
}

/// Polls `future` on this thread, parked between polls, with no runtime.
fn block_on_this_thread<F: Future>(future: F) -> F::Output {
    struct Unpark(thread::Thread);

    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = Waker::from(Arc::new(Unpark(thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        thread::park();
    }
}

/// Connects to `listener` by `addr`, and sees the server accept the
/// connection.
async fn connects(listener: &TcpListener, addr: impl ToSocketAddrs) -> io::Result<()> {
    let client = within(TcpStream::connect(addr)).await?;
    let (server, peer) = within(listener.accept()).await?;
    assert_eq!(peer, client.local_addr()?);
    assert_eq!(server.local_addr()?, listener.local_addr()?);
    Ok(())
}

/// The addresses of `localhost` with port `port`: at least one of them on
/// the loopback interface, and every one with the port.
fn is_localhost(addrs: &[SocketAddr], port: u16) -> bool {
    !addrs.is_empty()
        && addrs.iter().any(|addr| addr.ip().is_loopback())
        && addrs.iter().all(|addr| addr.port() == port)
}

#[test]
fn a_literal_resolves_at_once_with_no_runtime() {
    let v4: SocketAddr = "127.0.0.1:80".parse().unwrap();
    let v6: SocketAddr = "[::1]:80".parse().unwrap();
    assert_eq!(resolved_at_once("127.0.0.1:80"), [v4]);
    assert_eq!(resolved_at_once("[::1]:80"), [v6]);
    assert_eq!(resolved_at_once(String::from("127.0.0.1:80")), [v4]);
    assert_eq!(resolved_at_once(("127.0.0.1", 80)), [v4]);
    assert_eq!(resolved_at_once(("::1", 80)), [v6]);
    assert_eq!(resolved_at_once((String::from("::1"), 80)), [v6]);
    assert_eq!(resolved_at_once(v4), [v4]);
    assert_eq!(
        resolved_at_once((IpAddr::V6(Ipv6Addr::LOCALHOST), 80)),
        [v6]
    );
    assert_eq!(resolved_at_once(&[v4, v6][..]), [v4, v6]);
}

#[ruio::test]
async fn a_host_name_resolves_on_the_blocking_pool() -> io::Result<()> {
    let addrs: Vec<_> = within(lookup_host("localhost:80")).await?.collect();
    assert!(is_localhost(&addrs, 80), "{addrs:?}");
    let addrs: Vec<_> = within(lookup_host(("localhost", 80))).await?.collect();
    assert!(is_localhost(&addrs, 80), "{addrs:?}");
    Ok(())
}

/// A string without a port is no address and no host name to look up.
#[ruio::test]
async fn a_string_without_a_port_is_invalid() {
    let err = within(lookup_host("nonsense"))
        .await
        .err()
        .expect("no address");
    assert_eq!(err.kind(), ErrorKind::InvalidInput, "{err}");
    let err = within(TcpStream::connect("nonsense")).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput, "{err}");
}

/// `localhost` may resolve to `::1` before `127.0.0.1`, which the listener
/// refuses, so a connect by name falls back through the addresses, as one
/// given a slice whose first address is a closed port does.
#[ruio::test]
async fn connect_resolves_a_host_name_and_falls_back_through_the_addresses() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let port = addr.port();
    // Bound, and let go: nothing listens there.
    let closed = TcpListener::bind("127.0.0.1:0")?.local_addr()?;
    connects(&listener, format!("localhost:{port}")).await?;
    connects(&listener, ("localhost", port)).await?;
    connects(&listener, ("127.0.0.1", port)).await?;
    connects(&listener, &[closed, addr][..]).await?;
    Ok(())
}

#[ruio::test]
async fn connect_to_no_address_is_invalid() {
    let none: &[SocketAddr] = &[];
    let err = within(TcpStream::connect(none)).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput, "{err}");
}

/// The lookup's handle needs no runtime to be awaited.
#[test]
fn a_host_name_resolves_with_no_runtime() {
    let addrs: Vec<_> = block_on_this_thread(lookup_host("localhost:80"))
        .unwrap()
        .collect();
    assert!(is_localhost(&addrs, 80), "{addrs:?}");
}
