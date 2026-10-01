//! An HTTPS server: hyper over rustls, over ruio's streams through the
//! adapters, on every core the process may run on. It says hello at `/`,
//! and sends back what is posted to `/echo`.
//!
//! ```text
//! cargo run --release -p ruio-compat --example rustls [ADDRESS [CERTIFICATE KEY]]
//! curl --insecure https://localhost:3443/
//! ```
//!
//! The certificate and its key are PEM files; without them the server
//! makes a certificate of its own for `localhost`, signed by itself, which
//! clients have to be told to accept, as `curl --insecure` does.
//!
//! tokio-rustls runs TLS over anything with tokio's traits, as `Compat`
//! gives them to ruio's streams, and has them itself, so hyper-util's
//! `TokioIo` makes a TLS stream something hyper serves. The TLS
//! configuration is made once and shared by the cores; each serves its
//! connections on a thread of its own, receiving into a pool of its own, as
//! the hyper example does.
//!
//! A client has 30 seconds for its handshake, and as long again for each
//! request's head, which hyper times through `ruio_compat::Timer`.

use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use http_body_util::{Either, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use ruio::Cores;
use ruio::buf::BufPool;
use ruio::net::TcpListener;
use ruio_compat::{Compat, Timer};
use rustls::ServerConfig;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio_rustls::TlsAcceptor;

type Body = Either<Full<Bytes>, Incoming>;

/// How long a client has for its handshake.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

async fn serve(request: Request<Incoming>) -> Result<Response<Body>, hyper::Error> {
    let response = match (request.method(), request.uri().path()) {
        (&Method::GET, "/") => {
            Response::new(Either::Left(Full::from("Hello from ruio, over TLS!\n")))
        }
        (&Method::POST, "/echo") => Response::new(Either::Right(request.into_body())),
        _ => {
            let mut response = Response::new(Either::Left(Full::from("Not found\n")));
            *response.status_mut() = StatusCode::NOT_FOUND;
            response
        }
    };
    Ok(response)
}

/// The certificate chain and key in the PEM files at `certificate` and
/// `key`.
fn load(
    certificate: &Path,
    key: &Path,
) -> io::Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let chain = CertificateDer::pem_file_iter(certificate)
        .and_then(Iterator::collect)
        .map_err(io::Error::other)?;
    let key = PrivateKeyDer::from_pem_file(key).map_err(io::Error::other)?;
    Ok((chain, key))
}

/// A certificate for `localhost`, signed by its own key.
fn self_signed() -> io::Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let names = vec!["localhost".to_owned(), "127.0.0.1".to_owned()];
    let certified = rcgen::generate_simple_self_signed(names).map_err(io::Error::other)?;
    let key = PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der());
    Ok((vec![certified.cert.der().clone()], key.into()))
}

fn main() -> io::Result<()> {
    let mut args = std::env::args().skip(1);
    let address = args.next().unwrap_or_else(|| "127.0.0.1:3443".to_owned());
    let (chain, key) = match (args.next(), args.next()) {
        (Some(certificate), Some(key)) => load(certificate.as_ref(), key.as_ref())?,
        _ => self_signed()?,
    };
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .and_then(|config| config.with_no_client_auth().with_single_cert(chain, key))
        .map_err(io::Error::other)?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let acceptor = TlsAcceptor::from(Arc::new(config));

    let cores = Cores::all()?;
    println!(
        "listening on https://{address} with {} cores",
        cores.count()
    );
    let outputs = cores.run(|_core| {
        let acceptor = acceptor.clone();
        let address = address.clone();
        async move {
            let listener = TcpListener::bind_reuseport(address)?;
            let pool = BufPool::new(&ruio::driver(), 256, 4096)?;
            let mut http = http1::Builder::new();
            http.timer(Timer::new());
            let mut incoming = listener.incoming();
            while let Some(stream) = incoming.next().await {
                let stream = match stream {
                    Ok(stream) => stream,
                    Err(err) => {
                        eprintln!("accept failed: {err}");
                        continue;
                    }
                };
                stream.set_nodelay(true)?;
                let acceptor = acceptor.clone();
                let http = http.clone();
                let stream = Compat::with_pool(&pool, stream);
                let _detached = ruio::spawn(async move {
                    let handshake = acceptor.accept(stream);
                    let tls = match ruio::time::timeout(HANDSHAKE_TIMEOUT, handshake).await {
                        Ok(Ok(tls)) => tls,
                        Ok(Err(err)) => return eprintln!("handshake failed: {err}"),
                        Err(_) => return eprintln!("handshake timed out"),
                    };
                    let connection = http.serve_connection(TokioIo::new(tls), service_fn(serve));
                    if let Err(err) = connection.await {
                        eprintln!("connection failed: {err}");
                    }
                });
            }
            io::Result::Ok(())
        }
    })?;
    outputs.into_iter().collect()
}
