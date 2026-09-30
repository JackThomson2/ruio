//! hyper serves HTTP/1.1 through the adapters, to hyper's own client through
//! them too, and to a blocking client of the standard library's.

#![cfg(feature = "tokio")]

mod support;

use std::io::{self, Read as _, Write as _};
use std::net::SocketAddr;
use std::thread;

use http_body_util::{BodyExt, Either, Full};
use hyper::body::{Bytes, Incoming};
use hyper::client::conn::http1 as client;
use hyper::server::conn::http1 as server;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use ruio::buf::BufPool;
use ruio::net::{TcpListener, TcpStream};
use ruio_compat::Compat;

use support::{PATIENCE, pattern, within};

type Body = Either<Full<Bytes>, Incoming>;

/// Says hello at `/`, and echoes what is posted to `/echo`.
async fn service(request: Request<Incoming>) -> Result<Response<Body>, hyper::Error> {
    let response = match (request.method(), request.uri().path()) {
        (&Method::GET, "/") => Response::new(Either::Left(Full::from("hello"))),
        (&Method::POST, "/echo") => Response::new(Either::Right(request.into_body())),
        _ => {
            let mut response = Response::new(Either::Left(Full::default()));
            *response.status_mut() = StatusCode::NOT_FOUND;
            response
        }
    };
    Ok(response)
}

/// Serves the next `connections` connections to arrive on a listener, each
/// in a task of its own, and reports how they went. With a pool, they all
/// receive into it.
fn serve(
    connections: usize,
    pool: Option<BufPool>,
) -> io::Result<(SocketAddr, ruio::JoinHandle<Result<(), hyper::Error>>)> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let server = ruio::spawn(async move {
        let mut incoming = listener.incoming();
        let mut served = Vec::new();
        for _ in 0..connections {
            let stream = incoming.next().await.unwrap().unwrap();
            let io = TokioIo::new(match &pool {
                Some(pool) => Compat::with_pool(pool, stream),
                None => Compat::new(stream),
            });
            served.push(ruio::spawn(
                server::Builder::new().serve_connection(io, service_fn(service)),
            ));
        }
        for connection in served {
            connection.await.unwrap()?;
        }
        Ok(())
    });
    Ok((addr, server))
}

/// A connection of hyper's client, through the adapters, with its
/// connection's task.
async fn connect(
    addr: SocketAddr,
) -> io::Result<(
    client::SendRequest<Full<Bytes>>,
    ruio::JoinHandle<Result<(), hyper::Error>>,
)> {
    let stream = within(TcpStream::connect(addr)).await?;
    let io = TokioIo::new(Compat::new(stream));
    let (sender, connection) = within(client::handshake(io))
        .await
        .map_err(io::Error::other)?;
    Ok((sender, ruio::spawn(connection)))
}

/// Sends `request`, and reads the response's status and body.
async fn send(
    sender: &mut client::SendRequest<Full<Bytes>>,
    request: Request<Full<Bytes>>,
) -> io::Result<(StatusCode, Bytes)> {
    let response = within(sender.send_request(request))
        .await
        .map_err(io::Error::other)?;
    let status = response.status();
    let body = within(response.into_body().collect())
        .await
        .map_err(io::Error::other)?;
    Ok((status, body.to_bytes()))
}

fn get(path: &str) -> Request<Full<Bytes>> {
    Request::get(path)
        .header("host", "ruio")
        .body(Full::default())
        .unwrap()
}

/// Requests follow each other on a connection kept alive, a body larger
/// than a read buffer and a write goes both ways, and closing the client's
/// connection ends the server's.
#[ruio::test]
async fn hyper_serves_hyper_through_the_adapters() -> io::Result<()> {
    let (addr, server) = serve(1, None)?;
    let (mut sender, connection) = connect(addr).await?;
    for _ in 0..3 {
        assert_eq!(
            send(&mut sender, get("/")).await?,
            (StatusCode::OK, Bytes::from("hello"))
        );
    }
    assert_eq!(
        send(&mut sender, get("/nowhere")).await?.0,
        StatusCode::NOT_FOUND
    );

    let message = Bytes::from(pattern(1 << 20, 11));
    let echo = Request::post("/echo")
        .header("host", "ruio")
        .body(Full::new(message.clone()))
        .unwrap();
    assert_eq!(send(&mut sender, echo).await?, (StatusCode::OK, message));

    drop(sender);
    within(connection)
        .await
        .unwrap()
        .map_err(io::Error::other)?;
    within(server).await.unwrap().map_err(io::Error::other)?;
    Ok(())
}

/// Several connections are served at once, each request on its own, all
/// receiving into one pool, which holds less than they send.
#[ruio::test]
async fn connections_are_served_side_by_side() -> io::Result<()> {
    let pool = BufPool::new(&ruio::driver(), 16, 4096)?;
    let (addr, server) = serve(8, Some(pool))?;
    let mut clients = Vec::new();
    for client in 0..8 {
        clients.push(ruio::spawn(async move {
            let (mut sender, connection) = connect(addr).await?;
            let message = Bytes::from(pattern(64 << 10, client));
            let echo = Request::post("/echo")
                .header("host", "ruio")
                .body(Full::new(message.clone()))
                .unwrap();
            assert_eq!(send(&mut sender, echo).await?, (StatusCode::OK, message));
            drop(sender);
            within(connection).await.unwrap().map_err(io::Error::other)
        }));
    }
    for client in clients {
        client.await.unwrap()?;
    }
    within(server).await.unwrap().map_err(io::Error::other)?;
    Ok(())
}

/// A client that asks for the connection to close reads the response to
/// the end of the stream, which the server shuts down after writing it.
#[ruio::test]
async fn a_closed_connection_ends_after_the_response() -> io::Result<()> {
    let (addr, server) = serve(1, None)?;
    let client = thread::spawn(move || {
        let mut stream = std::net::TcpStream::connect(addr)?;
        stream.set_read_timeout(Some(PATIENCE))?;
        stream.write_all(b"GET / HTTP/1.1\r\nHost: ruio\r\nConnection: close\r\n\r\n")?;
        let mut response = String::new();
        stream.read_to_string(&mut response)?;
        io::Result::Ok(response)
    });
    within(server).await.unwrap().map_err(io::Error::other)?;
    let response = client.join().unwrap()?;
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
    assert!(response.ends_with("\r\n\r\nhello"), "{response}");
    Ok(())
}
