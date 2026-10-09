//! hyper-util's client on ruio: [`Connector`] makes its connections, ruio's
//! portable TCP streams adapted with [`PortableCompat`], and [`builder`]
//! gives it ruio's executor and timer.
//!
//! hyper-util's [`Client`](hyper_util::client::legacy::Client) asks for
//! connections that are `Send`, as its pool hands a connection to whichever
//! task asks for one next. A portable stream is `Send`, and is read and
//! written with system calls made at once on whichever thread polls it, into
//! and from hyper's own buffers, so a connection works from any core. A
//! connection's task, which reads and writes its stream, runs on the core
//! that made the connection, where [`Executor`] spawns it; a request from
//! another core goes to it through hyper's channels. The client is `Send`,
//! `Sync` and `Clone`, so one client, and one pool, can serve every core, and
//! a future that must be `Send`, such as `async_trait`'s, can make requests.
//!
//! The connector speaks plain HTTP, and sets `TCP_NODELAY` on its
//! connections unless told otherwise, so that a request whose head and body
//! go in two writes is not held back for the peer's acknowledgement. For
//! HTTPS, a TLS connector that wraps another, such as hyper-rustls's, wraps
//! this one, with [`enforce_http`](Connector::enforce_http) off so that it
//! connects to `https` URIs too.
//!
//! ```no_run
//! use http_body_util::{BodyExt, Empty};
//! use hyper::body::Bytes;
//! use ruio_compat::client::{self, Connector};
//!
//! # ruio::Runtime::new().unwrap().block_on(async {
//! let client = client::builder().build::<_, Empty<Bytes>>(Connector::new());
//! let response = client.get("http://example.com/".parse()?).await?;
//! let body = response.into_body().collect().await?.to_bytes();
//! # Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
//! # }).unwrap();
//! ```

use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use hyper::Uri;
use hyper_util::client::legacy::Builder;
use hyper_util::client::legacy::connect::{Connected, Connection};
use ruio::net::portable::{TcpStream, UnixStream};

use crate::{Executor, PortableCompat, Timer};

/// A builder of hyper-util's client with ruio's [`Executor`], which runs each
/// connection as a task of the core that made it, and ruio's [`Timer`], for
/// the pool's idle timeouts and HTTP/2's keep-alive.
///
/// The client is then built with a connector:
/// `builder().build::<_, B>(Connector::new())`, for request bodies of type
/// `B`.
pub fn builder() -> Builder {
    let mut builder = Builder::new(Executor::new());
    builder.timer(Timer::new()).pool_timer(Timer::new());
    builder
}

/// A connector of hyper-util's client: a tower `Service<Uri>` that resolves
/// a URI's host and connects a portable [`TcpStream`] to it, which it gives
/// the client adapted with [`PortableCompat`]; see the [module's
/// documentation](self).
///
/// A host name is resolved on ruio's blocking pool, and an address in the
/// URI costs no lookup; the addresses a name resolves to are tried in turn.
/// The port is the URI's, or 80 for `http` and 443 for `https`.
#[derive(Clone, Debug)]
pub struct Connector {
    nodelay: bool,
    enforce_http: bool,
    connect_timeout: Option<Duration>,
}

impl Connector {
    /// A connector of plain HTTP, which sets `TCP_NODELAY` and gives
    /// connecting no time limit but the kernel's.
    pub fn new() -> Self {
        Self {
            nodelay: true,
            enforce_http: true,
            connect_timeout: None,
        }
    }

    /// Sets whether the connections have `TCP_NODELAY`, which they do unless
    /// this turns it off.
    pub fn set_nodelay(&mut self, nodelay: bool) {
        self.nodelay = nodelay;
    }

    /// Sets whether only `http` URIs are connected to, as they are unless
    /// this turns it off, for a connector that wraps this one in TLS.
    pub fn enforce_http(&mut self, is_enforced: bool) {
        self.enforce_http = is_enforced;
    }

    /// Sets how long connecting may take, the lookup and every address tried
    /// included, after which it fails with `TimedOut`; `None`, as it is
    /// unless this sets one, waits for as long as the kernel does.
    pub fn set_connect_timeout(&mut self, timeout: Option<Duration>) {
        self.connect_timeout = timeout;
    }
}

impl Default for Connector {
    fn default() -> Self {
        Self::new()
    }
}

impl tower_service::Service<Uri> for Connector {
    type Response = PortableCompat<TcpStream>;
    type Error = io::Error;
    type Future = Connecting;

    /// Always ready: each connection is made on its own.
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Connecting {
        Connecting(Box::pin(connect(self.clone(), uri)))
    }
}

/// A connection being made by a [`Connector`], which is `Send` and `Unpin`,
/// as hyper-util asks of a connector's futures.
#[must_use = "futures do nothing unless polled"]
pub struct Connecting(Pin<Box<dyn Future<Output = io::Result<PortableCompat<TcpStream>>> + Send>>);

impl Future for Connecting {
    type Output = io::Result<PortableCompat<TcpStream>>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}

impl fmt::Debug for Connecting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connecting").finish_non_exhaustive()
    }
}

/// Connects to where `uri` names, as `connector` says.
async fn connect(connector: Connector, uri: Uri) -> io::Result<PortableCompat<TcpStream>> {
    let (host, port) = destination(&uri, connector.enforce_http)?;
    let connecting = TcpStream::connect((host, port));
    let stream = match connector.connect_timeout {
        Some(limit) => ruio::time::timeout(limit, connecting)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connecting timed out"))??,
        None => connecting.await?,
    };
    if connector.nodelay {
        stream.set_nodelay(true)?;
    }
    Ok(PortableCompat::new(stream))
}

/// The host and port `uri` names: its port, or the scheme's.
fn destination(uri: &Uri, enforce_http: bool) -> io::Result<(String, u16)> {
    let invalid = |message: &'static str| io::Error::new(io::ErrorKind::InvalidInput, message);
    let scheme = uri
        .scheme_str()
        .ok_or_else(|| invalid("invalid URI: the scheme is missing"))?;
    if enforce_http && scheme != "http" {
        return Err(invalid("invalid URI: the scheme is not http"));
    }
    let host = uri
        .host()
        .ok_or_else(|| invalid("invalid URI: the host is missing"))?;
    // An IPv6 address, which a URI puts in brackets.
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    let port = uri
        .port_u16()
        .unwrap_or(if scheme == "https" { 443 } else { 80 });
    Ok((host.to_owned(), port))
}

/// A plain TCP connection, which tells hyper-util nothing more.
impl Connection for PortableCompat<TcpStream> {
    fn connected(&self) -> Connected {
        Connected::new()
    }
}

/// A plain Unix connection, for a connector of one's own, which tells
/// hyper-util nothing more.
impl Connection for PortableCompat<UnixStream> {
    fn connected(&self) -> Connected {
        Connected::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn named(uri: &str, enforce_http: bool) -> io::Result<(String, u16)> {
        destination(&uri.parse().unwrap(), enforce_http)
    }

    #[test]
    fn a_uri_names_its_host_and_its_port_or_the_schemes() {
        assert_eq!(
            named("http://example.com/", true).unwrap(),
            ("example.com".into(), 80)
        );
        assert_eq!(
            named("http://example.com:8080/x", true).unwrap(),
            ("example.com".into(), 8080)
        );
        assert_eq!(
            named("https://example.com/", false).unwrap(),
            ("example.com".into(), 443)
        );
        assert_eq!(
            named("http://127.0.0.1:1/", true).unwrap(),
            ("127.0.0.1".into(), 1)
        );
        assert_eq!(
            named("http://[::1]:8080/", true).unwrap(),
            ("::1".into(), 8080)
        );
    }

    #[test]
    fn a_uri_without_http_a_scheme_or_a_host_is_refused() {
        let kind = |result: io::Result<(String, u16)>| result.unwrap_err().kind();
        assert_eq!(
            kind(named("https://example.com/", true)),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(kind(named("/path", true)), io::ErrorKind::InvalidInput);
        assert_eq!(kind(named("/path", false)), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn the_connector_and_its_future_are_what_hyper_util_asks() {
        fn connector<T: Clone + Send + Sync + 'static>() {}
        fn future<T: Send + Unpin>() {}
        connector::<Connector>();
        future::<Connecting>();
    }
}
