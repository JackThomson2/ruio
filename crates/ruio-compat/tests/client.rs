//! hyper-util's client on ruio: its connections are portable TCP streams,
//! its tasks and timers ruio's, and one client serves tasks on every core.

#![cfg(all(feature = "client", feature = "tokio"))]

mod support;

use std::convert::Infallible;
use std::error::Error;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use http_body_util::{BodyExt, Either, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::{http1, http2};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::{Connected, Connection};
use hyper_util::rt::TokioIo;
use ruio::net::portable::{TcpListener, TcpStream};
use ruio_compat::client::{self, Connector};
use ruio_compat::{Executor, PortableCompat};
use rustls::pki_types::{PrivatePkcs8KeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tower_service::Service;

use support::{pattern, within};

type Body = Either<Full<Bytes>, Incoming>;

/// Says hello at `/`, and echoes what is posted to `/echo`.
async fn service(request: Request<Incoming>) -> Result<Response<Body>, Infallible> {
    Ok(match (request.method(), request.uri().path()) {
        (&Method::POST, "/echo") => Response::new(Either::Right(request.into_body())),
        _ => Response::new(Either::Left(Full::from("hello"))),
    })
}

/// What a test server speaks.
enum Protocol {
    Http1,
    Http2,
    Https(Arc<ServerConfig>),
}

/// A server on a thread and a runtime of its own, which counts the
/// connections it accepts.
struct Server {
    addr: SocketAddr,
    connections: Arc<AtomicUsize>,
}

impl Server {
    fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }

    fn uri(&self, path: &str) -> Uri {
        format!("http://{}{path}", self.addr).parse().unwrap()
    }
}

fn serve(protocol: Protocol) -> Server {
    serve_on("127.0.0.1:0", protocol).unwrap()
}

fn serve_on(addr: &str, protocol: Protocol) -> io::Result<Server> {
    let listener = TcpListener::bind(addr)?;
    let addr = listener.local_addr()?;
    let connections = Arc::new(AtomicUsize::new(0));
    let counted = connections.clone();
    std::thread::spawn(move || {
        ruio::Runtime::new().unwrap().block_on(async move {
            while let Ok((stream, _)) = listener.accept().await {
                counted.fetch_add(1, Ordering::SeqCst);
                let io = PortableCompat::new(stream);
                match &protocol {
                    Protocol::Http1 => drop(ruio::spawn(
                        http1::Builder::new().serve_connection(io, service_fn(service)),
                    )),
                    Protocol::Http2 => drop(ruio::spawn(
                        http2::Builder::new(Executor::new())
                            .serve_connection(io, service_fn(service)),
                    )),
                    Protocol::Https(config) => {
                        let acceptor = TlsAcceptor::from(config.clone());
                        drop(ruio::spawn(async move {
                            let Ok(tls) = acceptor.accept(io).await else {
                                return;
                            };
                            let _ = http1::Builder::new()
                                .serve_connection(TokioIo::new(tls), service_fn(service))
                                .await;
                        }));
                    }
                }
            }
        });
    });
    Ok(Server { addr, connections })
}

/// The client most tests use.
fn http_client() -> Client<Connector, Full<Bytes>> {
    client::builder().build(Connector::new())
}

/// A response's body, whole.
async fn collect(response: Response<Incoming>) -> io::Result<Vec<u8>> {
    let body = response
        .into_body()
        .collect()
        .await
        .map_err(io::Error::other)?;
    Ok(body.to_bytes().to_vec())
}

/// The body of a `GET` of `uri`.
async fn get<C>(client: &Client<C, Full<Bytes>>, uri: Uri) -> io::Result<Vec<u8>>
where
    C: hyper_util::client::legacy::connect::Connect + Clone + Send + Sync + 'static,
{
    let response = client.get(uri).await.map_err(io::Error::other)?;
    collect(response).await
}

/// Whether `err`, or an error it came from, is an I/O error of `kind`.
fn has_kind(err: &(dyn Error + 'static), kind: io::ErrorKind) -> bool {
    let mut source = Some(err);
    while let Some(err) = source {
        if err
            .downcast_ref::<io::Error>()
            .is_some_and(|io| io.kind() == kind)
        {
            return true;
        }
        source = err.source();
    }
    false
}

/// Two cores, on two CPUs if the machine has them.
fn two() -> ruio::Cores {
    let all = ruio::Cores::all().unwrap();
    ruio::Cores::cpus(all.cpu_list().iter().copied().cycle().take(2))
}

#[test]
fn the_client_is_send_sync_and_clone_and_so_are_its_requests() {
    fn assert_client<T: Send + Sync + Clone>(_: &T) {}
    fn assert_send<T: Send>(_: &T) {}
    let client = http_client();
    assert_client(&client);
    assert_send(&client.get(Uri::from_static("http://127.0.0.1:1/")));
}

/// Requests one after another reuse the connection the pool keeps, bodies
/// large enough to take many reads each way among them.
#[ruio::test]
async fn requests_go_through_one_pooled_connection() -> io::Result<()> {
    let server = serve(Protocol::Http1);
    let client = http_client();
    for _ in 0..10 {
        assert_eq!(within(get(&client, server.uri("/"))).await?, b"hello");
    }
    let body = pattern(300_000, 5);
    let post = Request::post(server.uri("/echo"))
        .body(Full::from(body.clone()))
        .unwrap();
    let response = within(client.request(post))
        .await
        .map_err(io::Error::other)?;
    assert!(within(collect(response)).await? == body);
    assert_eq!(server.connections(), 1);
    Ok(())
}

/// A host name is resolved, its addresses tried in turn, and an IPv6
/// address in brackets is connected to, where the host has IPv6.
#[ruio::test]
async fn names_and_ipv6_addresses_are_connected_to() -> io::Result<()> {
    let server = serve(Protocol::Http1);
    let client = http_client();
    let named = format!("http://localhost:{}/", server.addr.port());
    assert_eq!(
        within(get(&client, named.parse().unwrap())).await?,
        b"hello"
    );
    if let Ok(six) = serve_on("[::1]:0", Protocol::Http1) {
        let uri = format!("http://[::1]:{}/", six.addr.port());
        assert_eq!(within(get(&client, uri.parse().unwrap())).await?, b"hello");
    }
    Ok(())
}

/// An `https` URI is refused, unless the connector is told to connect to
/// it anyway, as a TLS connector wrapping this one does.
#[ruio::test]
async fn https_uris_are_refused_unless_http_is_not_enforced() -> io::Result<()> {
    let server = serve(Protocol::Http1);
    let https: Uri = format!("https://{}/", server.addr).parse().unwrap();
    let err = within(http_client().get(https.clone())).await.unwrap_err();
    assert!(err.is_connect(), "{err:?}");
    assert!(has_kind(&err, io::ErrorKind::InvalidInput), "{err:?}");
    let mut connector = Connector::new();
    connector.enforce_http(false);
    let plain = client::builder().build::<_, Full<Bytes>>(connector);
    // Plain HTTP to the port the URI names: the connector does no TLS.
    assert_eq!(within(get(&plain, https)).await?, b"hello");
    Ok(())
}

/// A connection refused fails to connect; so does one that takes longer
/// than the connector's timeout, to a listener whose full backlog drops new
/// connections.
#[ruio::test]
async fn refused_and_timed_out_connections_fail_to_connect() -> io::Result<()> {
    let gone = std::net::TcpListener::bind("127.0.0.1:0")?;
    let addr = gone.local_addr()?;
    drop(gone);
    let err = within(http_client().get(format!("http://{addr}/").parse().unwrap()))
        .await
        .unwrap_err();
    assert!(err.is_connect(), "{err:?}");
    assert!(has_kind(&err, io::ErrorKind::ConnectionRefused), "{err:?}");

    let full = std::net::TcpListener::bind("127.0.0.1:0")?;
    // A backlog of none: one connection waits in it, after which the kernel
    // drops new connections' SYNs, and connecting waits for its retries.
    // SAFETY: listen(2) touches no memory.
    assert_eq!(unsafe { libc::listen(full.as_raw_fd(), 0) }, 0);
    let addr = full.local_addr()?;
    let waiting: Vec<_> = (0..2)
        .filter_map(|_| {
            std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200)).ok()
        })
        .collect();
    assert!(!waiting.is_empty());
    let mut connector = Connector::new();
    connector.set_connect_timeout(Some(Duration::from_millis(100)));
    let impatient = client::builder().build::<_, Full<Bytes>>(connector);
    let start = Instant::now();
    let err = within(impatient.get(format!("http://{addr}/").parse().unwrap()))
        .await
        .unwrap_err();
    assert!(err.is_connect(), "{err:?}");
    assert!(has_kind(&err, io::ErrorKind::TimedOut), "{err:?}");
    assert!(start.elapsed() >= Duration::from_millis(100));
    Ok(())
}

/// The pool keeps an idle connection until its idle timeout, and then lets
/// it go, so the next request makes another.
#[ruio::test]
async fn idle_connections_go_after_the_pools_idle_timeout() -> io::Result<()> {
    let server = serve(Protocol::Http1);
    let client = http_client();
    within(get(&client, server.uri("/"))).await?;
    ruio::time::sleep(Duration::from_millis(300)).await;
    within(get(&client, server.uri("/"))).await?;
    assert_eq!(server.connections(), 1, "kept for the default 90 s");

    let server = serve(Protocol::Http1);
    let mut builder = client::builder();
    builder.pool_idle_timeout(Duration::from_millis(50));
    let client = builder.build::<_, Full<Bytes>>(Connector::new());
    within(get(&client, server.uri("/"))).await?;
    ruio::time::sleep(Duration::from_millis(300)).await;
    within(get(&client, server.uri("/"))).await?;
    assert_eq!(server.connections(), 2);
    Ok(())
}

/// With HTTP/2, requests at once share one connection, whose streams the
/// executor runs as tasks.
#[ruio::test]
async fn http2_requests_share_one_connection() -> io::Result<()> {
    let server = serve(Protocol::Http2);
    let mut builder = client::builder();
    builder.http2_only(true);
    let client = builder.build::<_, Full<Bytes>>(Connector::new());
    within(get(&client, server.uri("/"))).await?;
    let requests: Vec<_> = (0..16)
        .map(|index| {
            let client = client.clone();
            let uri = server.uri("/echo");
            ruio::spawn(async move {
                let body = pattern(20_000, index);
                let post = Request::post(uri).body(Full::from(body.clone())).unwrap();
                let response = client.request(post).await.map_err(io::Error::other)?;
                io::Result::Ok(collect(response).await? == body)
            })
        })
        .collect();
    for request in requests {
        assert!(within(request).await.unwrap()?);
    }
    assert_eq!(server.connections(), 1);
    Ok(())
}

/// Tasks on two cores, and futures that must be `Send` sent from each core
/// to the other, make their requests through one client and its pool.
#[test]
fn requests_from_several_cores_share_one_client_and_its_pool() {
    const TASKS: usize = 4;
    const REQUESTS: usize = 10;
    const SENT: usize = 5;
    let server = serve(Protocol::Http1);
    let client = http_client();
    let uri = server.uri("/");
    let outputs = two()
        .run(|core| {
            let (client, uri) = (client.clone(), uri.clone());
            async move {
                let tasks: Vec<_> = (0..TASKS)
                    .map(|_| {
                        let (client, uri) = (client.clone(), uri.clone());
                        ruio::spawn(async move {
                            let mut hellos = 0;
                            for _ in 0..REQUESTS {
                                hellos += usize::from(
                                    get(&client, uri.clone()).await.unwrap() == b"hello",
                                );
                            }
                            hellos
                        })
                    })
                    .collect();
                let other = 1 - core.index();
                let sent = ruio::spawn_on(other, async move {
                    let mut hellos = 0;
                    for _ in 0..SENT {
                        hellos += usize::from(get(&client, uri.clone()).await.unwrap() == b"hello");
                    }
                    (hellos, ruio::core().unwrap().index())
                });
                let mut hellos = 0;
                for task in tasks {
                    hellos += within(task).await.unwrap();
                }
                let (sent_hellos, ran_on) = within(sent).await.unwrap();
                (hellos + sent_hellos, ran_on == other)
            }
        })
        .unwrap();
    let requests = 2 * (TASKS * REQUESTS + SENT);
    let hellos: usize = outputs.iter().map(|(hellos, _)| hellos).sum();
    assert_eq!(hellos, requests);
    assert!(outputs.iter().all(|(_, elsewhere)| *elsewhere));
    // While a request waits for an idle connection, the pool makes a new one
    // too, and keeps whichever loses, so warming up makes more connections
    // than there are tasks; after that, each request finds one idle.
    let connections = server.connections();
    assert!(
        0 < connections && connections <= requests / 3,
        "{connections} connections for {requests} requests"
    );
}

/// A server's configuration and a client's: the server's certificate is
/// for `localhost`, signed by its own key, and the client trusts it.
fn configs() -> (Arc<ServerConfig>, Arc<ClientConfig>) {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let certificate = certified.cert.der().clone();
    let key = PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der());
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let server = ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![certificate.clone()], key.into())
        .unwrap();
    let mut roots = RootCertStore::empty();
    roots.add(certificate).unwrap();
    let client = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    (Arc::new(server), Arc::new(client))
}

/// A TLS connector wrapping the plain one, as hyper-rustls's does: what an
/// HTTPS client of hyper-util's on ruio is.
#[derive(Clone)]
struct Https {
    http: Connector,
    tls: TlsConnector,
}

impl Https {
    fn new(config: Arc<ClientConfig>) -> Self {
        let mut http = Connector::new();
        http.enforce_http(false);
        Self {
            http,
            tls: TlsConnector::from(config),
        }
    }
}

/// A TLS connection over a portable stream, as hyper-util's client takes it.
struct TlsConnection(TokioIo<tokio_rustls::client::TlsStream<PortableCompat<TcpStream>>>);

impl hyper::rt::Read for TlsConnection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_read(cx, buf)
    }
}

impl hyper::rt::Write for TlsConnection {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().0).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_shutdown(cx)
    }
}

impl Connection for TlsConnection {
    fn connected(&self) -> Connected {
        Connected::new()
    }
}

impl Service<Uri> for Https {
    type Response = TlsConnection;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = io::Result<TlsConnection>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let mut http = self.http.clone();
        let tls = self.tls.clone();
        Box::pin(async move {
            let host = uri.host().unwrap_or_default().to_owned();
            let name = ServerName::try_from(host).map_err(io::Error::other)?;
            let stream = http.call(uri).await?;
            Ok(TlsConnection(TokioIo::new(
                tls.connect(name, stream).await?,
            )))
        })
    }
}

/// HTTPS through a TLS connector that wraps this one, with a body that
/// takes many records, over one pooled connection.
#[ruio::test]
async fn https_runs_through_a_tls_connector_wrapping_this_one() -> io::Result<()> {
    let (server_config, client_config) = configs();
    let server = serve(Protocol::Https(server_config));
    let client = client::builder().build::<_, Full<Bytes>>(Https::new(client_config));
    let base = format!("https://localhost:{}", server.addr.port());
    let hello = within(get(&client, format!("{base}/").parse().unwrap())).await?;
    assert_eq!(hello, b"hello");
    let body = pattern(150_000, 9);
    let post = Request::post(format!("{base}/echo"))
        .body(Full::from(body.clone()))
        .unwrap();
    let response = within(client.request(post))
        .await
        .map_err(io::Error::other)?;
    assert!(within(collect(response)).await? == body);
    assert_eq!(server.connections(), 1);
    Ok(())
}
