//! The adapter for the portable streams: tokio's, futures-io's and hyper's
//! traits over streams that are `Send`, reading into the caller's buffers and
//! writing from them; hyper over HTTP/1.1 and HTTP/2 and over TLS through it,
//! and a connection that moves between cores.

#![cfg(all(feature = "hyper", feature = "tokio", feature = "futures-io"))]

mod support;

use std::convert::Infallible;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use http_body_util::{BodyExt, Either, Full};
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::TokioIo;
use ruio::net::portable::{TcpListener, TcpStream, UnixStream};
use ruio_compat::{Executor, PortableCompat};
use rustls::pki_types::{PrivatePkcs8KeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use support::{pattern, within};

type Body = Either<Full<Bytes>, Incoming>;

/// Says hello at `/`, and echoes what is posted to `/echo`.
async fn service(request: Request<Incoming>) -> Result<Response<Body>, Infallible> {
    Ok(match (request.method(), request.uri().path()) {
        (&Method::POST, "/echo") => Response::new(Either::Right(request.into_body())),
        _ => Response::new(Either::Left(Full::from("hello"))),
    })
}

/// A connected pair of portable streams on the loopback interface: the
/// client's and the server's.
async fn pair() -> io::Result<(TcpStream, TcpStream)> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let client = ruio::spawn(TcpStream::connect(addr));
    let (server, _) = within(listener.accept()).await?;
    Ok((client.await.unwrap()?, server))
}

/// Serves HTTP/1.1 over the adapter on every connection to arrive, each in
/// a task of its own.
fn serve_http1() -> io::Result<SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    drop(ruio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let connection = hyper::server::conn::http1::Builder::new()
                .serve_connection(PortableCompat::new(stream), service_fn(service));
            drop(ruio::spawn(connection));
        }
    }));
    Ok(addr)
}

/// A request's response body, whole.
async fn body_of(response: Response<Incoming>) -> Vec<u8> {
    response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec()
}

#[test]
fn the_adapter_is_send_and_sync_and_has_the_traits() {
    fn assert_traits<T>()
    where
        T: Send
            + Sync
            + Unpin
            + tokio::io::AsyncRead
            + tokio::io::AsyncWrite
            + futures_io::AsyncRead
            + futures_io::AsyncWrite
            + hyper::rt::Read
            + hyper::rt::Write,
    {
    }
    assert_traits::<PortableCompat<TcpStream>>();
    assert_traits::<PortableCompat<UnixStream>>();
}

/// tokio's traits read into the caller's buffer and write from it: a
/// megabyte one way while the other end reads it, and the end once the
/// writer shuts down.
#[ruio::test]
async fn tokio_traits_carry_data_both_ways() -> io::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (client, server) = pair().await?;
    let (mut client, mut server) = (PortableCompat::new(client), PortableCompat::new(server));
    let data = pattern(1 << 20, 3);
    let expected = data.clone();
    let reader = ruio::spawn(async move {
        let mut received = Vec::new();
        server.read_to_end(&mut received).await?;
        server.write_all(b"done").await?;
        server.shutdown().await?;
        io::Result::Ok(received == expected)
    });
    within(client.write_all(&data)).await?;
    client.shutdown().await?;
    assert!(within(reader).await.unwrap()?);
    let mut reply = Vec::new();
    within(client.read_to_end(&mut reply)).await?;
    assert_eq!(reply, b"done");
    Ok(())
}

/// futures-io's traits, likewise.
#[ruio::test]
async fn futures_io_traits_carry_data_both_ways() -> io::Result<()> {
    use futures_util::{AsyncReadExt, AsyncWriteExt};

    let (one, other) = UnixStream::pair()?;
    let (mut one, mut other) = (PortableCompat::new(one), PortableCompat::new(other));
    within(one.write_all(b"through futures-io")).await?;
    one.close().await?;
    let mut received = Vec::new();
    within(other.read_to_end(&mut received)).await?;
    assert_eq!(received, b"through futures-io");
    Ok(())
}

/// hyper serves HTTP/1.1 through the adapter, to its own client through it,
/// with a body large enough to take many reads each way.
#[ruio::test]
async fn hyper_serves_and_connects_through_the_adapter() -> io::Result<()> {
    let addr = serve_http1()?;
    let stream = within(TcpStream::connect(addr)).await?;
    stream.set_nodelay(true)?;
    let (mut sender, connection) =
        hyper::client::conn::http1::handshake::<_, Full<Bytes>>(PortableCompat::new(stream))
            .await
            .map_err(io::Error::other)?;
    let connection = ruio::spawn(connection);
    let hello = within(sender.send_request(Request::new(Full::default())))
        .await
        .map_err(io::Error::other)?;
    assert_eq!(body_of(hello).await, b"hello");
    let body = pattern(256 << 10, 7);
    let post = Request::post("/echo")
        .body(Full::from(body.clone()))
        .unwrap();
    let echoed = within(sender.send_request(post))
        .await
        .map_err(io::Error::other)?;
    assert!(within(body_of(echoed)).await == body);
    drop(sender);
    within(connection)
        .await
        .unwrap()
        .map_err(io::Error::other)?;
    Ok(())
}

/// hyper's HTTP/2, whose streams the executor runs as tasks, over the
/// adapter: requests at once on one connection.
#[ruio::test]
async fn hyper_runs_http2_over_the_adapter() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let server = ruio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        hyper::server::conn::http2::Builder::new(Executor::new())
            .serve_connection(PortableCompat::new(stream), service_fn(service))
            .await
            .map_err(io::Error::other)
    });
    let stream = within(TcpStream::connect(addr)).await?;
    let (sender, connection) = hyper::client::conn::http2::handshake::<_, _, Full<Bytes>>(
        Executor::new(),
        PortableCompat::new(stream),
    )
    .await
    .map_err(io::Error::other)?;
    let connection = ruio::spawn(connection);
    let mut requests = Vec::new();
    for index in 0..8 {
        let mut sender = sender.clone();
        requests.push(ruio::spawn(async move {
            let body = pattern(10_000, index);
            let post = Request::post("http://localhost/echo")
                .body(Full::from(body.clone()))
                .unwrap();
            let response = sender.send_request(post).await.map_err(io::Error::other)?;
            io::Result::Ok(body_of(response).await == body)
        }));
    }
    for request in requests {
        assert!(within(request).await.unwrap()?);
    }
    drop(sender);
    within(connection)
        .await
        .unwrap()
        .map_err(io::Error::other)?;
    within(server).await.unwrap()?;
    Ok(())
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

/// HTTPS: TLS over the adapter both ways, hyper over TLS, and a body that
/// takes many records.
#[ruio::test]
async fn hyper_runs_https_over_tls_over_the_adapter() -> io::Result<()> {
    let (server_config, client_config) = configs();
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let server = ruio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let tls = TlsAcceptor::from(server_config)
            .accept(PortableCompat::new(stream))
            .await?;
        hyper::server::conn::http1::Builder::new()
            .serve_connection(TokioIo::new(tls), service_fn(service))
            .await
            .map_err(io::Error::other)
    });
    let stream = within(TcpStream::connect(addr)).await?;
    let name = ServerName::try_from("localhost").unwrap();
    let tls = within(TlsConnector::from(client_config).connect(name, PortableCompat::new(stream)))
        .await?;
    let (mut sender, connection) =
        hyper::client::conn::http1::handshake::<_, Full<Bytes>>(TokioIo::new(tls))
            .await
            .map_err(io::Error::other)?;
    let connection = ruio::spawn(connection);
    let body = pattern(100_000, 1);
    let post = Request::post("/echo")
        .body(Full::from(body.clone()))
        .unwrap();
    let echoed = within(sender.send_request(post))
        .await
        .map_err(io::Error::other)?;
    assert!(within(body_of(echoed)).await == body);
    drop(sender);
    within(connection)
        .await
        .unwrap()
        .map_err(io::Error::other)?;
    within(server).await.unwrap()?;
    Ok(())
}

/// A client's connection made on one core runs on another, its requests
/// still sent from the first; and a client made on the second core
/// altogether.
#[test]
fn a_connection_runs_on_another_core_from_the_one_that_made_it() {
    let all = ruio::Cores::all().unwrap();
    let cores = ruio::Cores::cpus(all.cpu_list().iter().copied().cycle().take(2));
    let outputs = cores
        .run(|core| async move {
            if core.index() != 0 {
                return None;
            }
            let addr = serve_http1().unwrap();
            let stream = TcpStream::connect(addr).await.unwrap();
            let (mut sender, connection) = hyper::client::conn::http1::handshake::<_, Full<Bytes>>(
                PortableCompat::new(stream),
            )
            .await
            .unwrap();
            // The connection's task, and so its stream, on the second core.
            let far = ruio::spawn_on(1, connection);
            let mut bodies = Vec::new();
            for _ in 0..3 {
                let response = within(sender.send_request(Request::new(Full::default())))
                    .await
                    .unwrap();
                bodies.push(body_of(response).await);
            }
            drop(sender);
            within(far).await.unwrap().unwrap();
            let elsewhere = ruio::spawn_on(1, async move {
                let stream = TcpStream::connect(addr).await.unwrap();
                let (mut sender, connection) = hyper::client::conn::http1::handshake::<
                    _,
                    Full<Bytes>,
                >(PortableCompat::new(stream))
                .await
                .unwrap();
                // Detached, and so no handle of the core's held across an
                // await: the connection ends once the sender goes.
                drop(ruio::spawn(connection));
                let response = sender.send_request(Request::new(Full::default())).await;
                let body = body_of(response.unwrap()).await;
                (body, ruio::core().unwrap().index())
            });
            Some((bodies, within(elsewhere).await.unwrap()))
        })
        .unwrap();
    let (bodies, (body, ran_on)) = outputs.into_iter().next().flatten().unwrap();
    assert!(bodies.iter().all(|body| body == b"hello"));
    assert_eq!((&body[..], ran_on), (&b"hello"[..], 1));
}
