//! The HTTP benchmarks' servers: hyper answering every request with a short
//! plaintext response, over HTTP/1.1 or HTTP/2, on one thread, on ruio
//! through its adapters, which have hyper's own traits, with ruio's executor
//! for HTTP/2's streams, and on tokio's current-thread runtime through
//! hyper-util's `TokioIo` and `TokioExecutor`; and what the HTTP/1.1 load
//! generator sends and checks.

use std::convert::Infallible;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::os::fd::AsRawFd;
use std::str::FromStr;

use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::header::{CONTENT_TYPE, HeaderValue};
use hyper::server::conn::{http1, http2};
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use ruio::buf::BufPool;
use ruio_compat::{Compat, Executor};

use crate::sys;

/// What every response says.
pub(crate) const BODY: &[u8] = b"Hello, World!";

/// What the load generator asks, again and again, on each connection.
pub(crate) const REQUEST: &[u8] = b"GET /plaintext HTTP/1.1\r\nHost: ruio-bench\r\n\r\n";

/// The size of the buffers of ruio's pool, which hold a request each.
const BUFFER_SIZE: u32 = 4096;

/// Which HTTP a server speaks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Protocol {
    /// HTTP/1.1: a request at a time on a connection.
    Http1,
    /// HTTP/2, from the start of the connection: many streams at once, each
    /// a task of the executor.
    Http2,
}

/// An HTTP server implementation: hyper, on one thread, on a runtime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HttpServer {
    /// ruio, hyper's I/O going through `ruio_compat::Compat`, which has
    /// hyper's traits, each connection receiving into a buffer of its own.
    Ruio,
    /// The same, with the connections receiving into a shared pool.
    RuioPool,
    /// tokio's current-thread runtime, with `TokioIo`.
    Tokio,
}

impl HttpServer {
    pub(crate) const ALL: [HttpServer; 3] =
        [HttpServer::Tokio, HttpServer::Ruio, HttpServer::RuioPool];
}

impl FromStr for HttpServer {
    type Err = String;

    fn from_str(name: &str) -> Result<Self, String> {
        match name {
            "ruio" => Ok(HttpServer::Ruio),
            "ruio-pool" => Ok(HttpServer::RuioPool),
            "tokio" => Ok(HttpServer::Tokio),
            _ => Err(format!(
                "unknown server {name:?}; try ruio, ruio-pool or tokio"
            )),
        }
    }
}

impl fmt::Display for HttpServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            HttpServer::Ruio => "ruio",
            HttpServer::RuioPool => "ruio-pool",
            HttpServer::Tokio => "tokio",
        })
    }
}

async fn hello(_: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    let mut response = Response::new(Full::new(Bytes::from_static(BODY)));
    let plain = HeaderValue::from_static("text/plain");
    response.headers_mut().insert(CONTENT_TYPE, plain);
    Ok(response)
}

/// Runs `server` on `addr`, speaking `protocol`, until the process is
/// killed. `buffers` sizes ruio's pool.
pub(crate) fn run(
    server: HttpServer,
    protocol: Protocol,
    addr: SocketAddr,
    buffers: u16,
) -> io::Result<()> {
    match server {
        HttpServer::Ruio => ruio(protocol, addr, None),
        HttpServer::RuioPool => ruio(protocol, addr, Some(buffers)),
        HttpServer::Tokio => tokio(protocol, addr),
    }
}

fn ruio(protocol: Protocol, addr: SocketAddr, buffers: Option<u16>) -> io::Result<()> {
    ruio::Runtime::builder()
        .entries(4096)
        .build()?
        .block_on(async move {
            let listener = ruio::net::TcpListener::bind(addr)?;
            sys::set_nodelay(listener.as_raw_fd())?;
            let pool = buffers
                .map(|count| BufPool::new(&ruio::driver(), count, BUFFER_SIZE))
                .transpose()?;
            let mut incoming = listener.incoming();
            while let Some(stream) = incoming.next().await {
                let stream = match stream {
                    Ok(stream) => stream,
                    Err(err) => {
                        eprintln!("ruio HTTP server: accept failed: {err}");
                        continue;
                    }
                };
                let io = match &pool {
                    Some(pool) => Compat::with_pool(pool, stream),
                    None => Compat::new(stream),
                };
                match protocol {
                    Protocol::Http1 => {
                        let connection =
                            http1::Builder::new().serve_connection(io, service_fn(hello));
                        drop(ruio::spawn(async move {
                            let _ = connection.await;
                        }));
                    }
                    Protocol::Http2 => {
                        let connection = http2::Builder::new(Executor::new())
                            .serve_connection(io, service_fn(hello));
                        drop(ruio::spawn(async move {
                            let _ = connection.await;
                        }));
                    }
                }
            }
            Ok(())
        })
}

fn tokio(protocol: Protocol, addr: SocketAddr) -> io::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()?;
    runtime.block_on(async move {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        sys::set_nodelay(listener.as_raw_fd())?;
        loop {
            let stream = match listener.accept().await {
                Ok((stream, _)) => stream,
                Err(err) => {
                    eprintln!("tokio HTTP server: accept failed: {err}");
                    continue;
                }
            };
            let io = TokioIo::new(stream);
            match protocol {
                Protocol::Http1 => {
                    let connection = http1::Builder::new().serve_connection(io, service_fn(hello));
                    tokio::spawn(async move {
                        let _ = connection.await;
                    });
                }
                Protocol::Http2 => {
                    let connection = http2::Builder::new(TokioExecutor::new())
                        .serve_connection(io, service_fn(hello));
                    tokio::spawn(async move {
                        let _ = connection.await;
                    });
                }
            }
        }
    })
}

/// How long the response at the start of `buf` is, head and body, once its
/// head has arrived.
///
/// # Errors
///
/// `InvalidData` unless the head is of a `200 OK` response with a
/// `content-length`.
pub(crate) fn response_len(buf: &[u8]) -> io::Result<Option<usize>> {
    let Some(end) = buf.windows(4).position(|window| window == b"\r\n\r\n") else {
        return Ok(None);
    };
    let invalid = |what: &str| io::Error::new(io::ErrorKind::InvalidData, what.to_owned());
    let head = std::str::from_utf8(&buf[..end]).map_err(|_| invalid("a head not in UTF-8"))?;
    if !head.starts_with("HTTP/1.1 200 ") {
        return Err(invalid("a response other than 200 OK"));
    }
    let length = head
        .split("\r\n")
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        .ok_or_else(|| invalid("a response without a content-length"))?;
    Ok(Some(end + 4 + length))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn responses_are_measured_once_their_heads_are_in() {
        let response = b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\ndate: x\r\n\r\nHello, World!";
        assert_eq!(response_len(&response[..20]).unwrap(), None);
        assert_eq!(response_len(response).unwrap(), Some(response.len()));
        assert!(response_len(b"HTTP/1.1 404 Not Found\r\n\r\n").is_err());
        assert!(response_len(b"HTTP/1.1 200 OK\r\n\r\n").is_err());
    }
}
