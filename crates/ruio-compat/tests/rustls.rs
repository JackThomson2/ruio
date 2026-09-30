//! TLS through the adapters: hyper serves HTTPS over tokio-rustls on ruio,
//! to hyper's client over tokio-rustls on ruio too, and to a blocking
//! client of rustls's own.

#![cfg(feature = "tokio")]

mod support;

use std::io::{self, Read as _, Write as _};
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;

use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use ruio::net::{TcpListener, TcpStream};
use ruio_compat::Compat;
use rustls::pki_types::{PrivatePkcs8KeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use support::{PATIENCE, Reads, pattern, within};

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

/// Sends back what is posted, whatever the path.
async fn echo(request: Request<Incoming>) -> Result<Response<Incoming>, hyper::Error> {
    Ok(Response::new(request.into_body()))
}

/// Serves HTTPS on the next connection to arrive: TLS over the adapter,
/// which reads so, and hyper over TLS.
fn serve_one(
    config: Arc<ServerConfig>,
    reads: Reads,
) -> io::Result<(SocketAddr, ruio::JoinHandle<io::Result<()>>)> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let server = ruio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let tls = TlsAcceptor::from(config)
            .accept(reads.adapt(stream))
            .await?;
        hyper::server::conn::http1::Builder::new()
            .serve_connection(TokioIo::new(tls), service_fn(echo))
            .await
            .map_err(io::Error::other)
    });
    Ok((addr, server))
}

/// Bodies large and small cross a TLS connection kept alive both ways, the
/// server receiving into a pool that holds less than a body, and the
/// server's connection ends as the client's closes.
#[ruio::test]
async fn hyper_serves_https_through_the_adapters() -> io::Result<()> {
    let (server_config, client_config) = configs();
    let (addr, server) = serve_one(server_config, Reads::Pool(8, 4096))?;
    let stream = within(TcpStream::connect(addr)).await?;
    let name = ServerName::try_from("localhost").unwrap();
    let connector = TlsConnector::from(client_config);
    let tls = within(connector.connect(name, Compat::new(stream))).await?;
    let (mut sender, connection) = within(hyper::client::conn::http1::handshake(TokioIo::new(tls)))
        .await
        .map_err(io::Error::other)?;
    let connection = ruio::spawn(connection);
    for (round, len) in [100, 1 << 20, 100].into_iter().enumerate() {
        let message = Bytes::from(pattern(len, round));
        let request = Request::post("/")
            .header("host", "localhost")
            .body(Full::new(message.clone()))
            .unwrap();
        let response = within(sender.send_request(request))
            .await
            .map_err(io::Error::other)?;
        let body = within(response.into_body().collect())
            .await
            .map_err(io::Error::other)?;
        assert!(body.to_bytes() == message, "round {round}");
    }
    drop(sender);
    within(connection)
        .await
        .unwrap()
        .map_err(io::Error::other)?;
    within(server).await.unwrap()
}

/// A connection the client asks to close ends after the response with
/// TLS's `close_notify`, then the end of the stream: rustls reports a
/// stream that just ends as an error.
#[ruio::test]
async fn a_closed_https_connection_ends_with_close_notify() -> io::Result<()> {
    let (server_config, client_config) = configs();
    let (addr, server) = serve_one(server_config, Reads::Buffer(16 << 10))?;
    let client = thread::spawn(move || {
        let name = ServerName::try_from("localhost").unwrap();
        let connection =
            rustls::ClientConnection::new(client_config, name).map_err(io::Error::other)?;
        let socket = std::net::TcpStream::connect(addr)?;
        socket.set_read_timeout(Some(PATIENCE))?;
        let mut stream = rustls::StreamOwned::new(connection, socket);
        stream.write_all(
            b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n\
              Connection: close\r\n\r\nhello",
        )?;
        let mut response = String::new();
        stream.read_to_string(&mut response)?;
        io::Result::Ok(response)
    });
    within(server).await.unwrap()?;
    let response = client.join().unwrap()?;
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
    assert!(response.ends_with("\r\n\r\nhello"), "{response}");
    Ok(())
}
