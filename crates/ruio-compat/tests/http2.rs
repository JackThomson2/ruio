//! hyper serves HTTP/2 through the adapters, with ruio's executor running
//! the streams, to hyper's own client through them too.

#![cfg(feature = "hyper")]

mod support;

use std::cell::Cell;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use http_body_util::{BodyExt, Either, Full};
use hyper::body::{Bytes, Incoming};
use hyper::client::conn::http2 as client;
use hyper::server::conn::http2 as server;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use ruio::buf::BufPool;
use ruio::net::{TcpListener, TcpStream};
use ruio_compat::{Compat, Executor};

use support::{pattern, within};

type Body = Either<Full<Bytes>, Incoming>;

/// How many requests the service has in hand at once, at most, so far.
#[derive(Clone, Default)]
struct InFlight {
    now: Rc<Cell<usize>>,
    most: Rc<Cell<usize>>,
}

impl InFlight {
    fn enter(&self) {
        let now = self.now.get() + 1;
        self.now.set(now);
        self.most.set(self.most.get().max(now));
    }

    fn leave(&self) {
        self.now.set(self.now.get() - 1);
    }
}

/// Says hello at `/`, echoes what is posted to `/echo`, and at `/slow`
/// takes a while before saying so, counting the requests in hand.
async fn service(
    request: Request<Incoming>,
    in_flight: InFlight,
) -> Result<Response<Body>, hyper::Error> {
    in_flight.enter();
    let response = match (request.method(), request.uri().path()) {
        (&Method::GET, "/") => Response::new(Either::Left(Full::from("hello"))),
        (&Method::POST, "/echo") => Response::new(Either::Right(request.into_body())),
        (&Method::GET, "/slow") => {
            ruio::time::sleep(Duration::from_millis(20)).await;
            Response::new(Either::Left(Full::from("slowly")))
        }
        _ => {
            let mut response = Response::new(Either::Left(Full::default()));
            *response.status_mut() = StatusCode::NOT_FOUND;
            response
        }
    };
    in_flight.leave();
    Ok(response)
}

/// Serves the next `connections` HTTP/2 connections to arrive on a
/// listener, each in a task of its own, its streams in tasks of ruio's
/// executor, and reports how they went.
fn serve(
    connections: usize,
    pool: Option<BufPool>,
    in_flight: InFlight,
) -> io::Result<(SocketAddr, ruio::JoinHandle<Result<(), hyper::Error>>)> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let server = ruio::spawn(async move {
        let mut incoming = listener.incoming();
        let mut served = Vec::new();
        for _ in 0..connections {
            let stream = incoming.next().await.unwrap().unwrap();
            let io = match &pool {
                Some(pool) => Compat::with_pool(pool, stream),
                None => Compat::new(stream),
            };
            let in_flight = in_flight.clone();
            let connection = server::Builder::new(Executor::new()).serve_connection(
                io,
                service_fn(move |request| service(request, in_flight.clone())),
            );
            served.push(ruio::spawn(connection));
        }
        for connection in served {
            connection.await.unwrap()?;
        }
        Ok(())
    });
    Ok((addr, server))
}

/// An HTTP/2 connection of hyper's client, through the adapters, with its
/// connection's task.
async fn connect(
    addr: SocketAddr,
) -> io::Result<(
    client::SendRequest<Full<Bytes>>,
    ruio::JoinHandle<Result<(), hyper::Error>>,
)> {
    let stream = within(TcpStream::connect(addr)).await?;
    let io = Compat::new(stream);
    let (sender, connection) = within(client::handshake(Executor::new(), io))
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
    Request::builder()
        .method(Method::GET)
        .uri(path)
        .body(Full::default())
        .unwrap()
}

fn post(path: &str, body: Vec<u8>) -> Request<Full<Bytes>> {
    Request::builder()
        .method(Method::POST)
        .uri(path)
        .body(Full::from(body))
        .unwrap()
}

#[ruio::test]
async fn hyper_serves_http2_to_hyper_through_the_adapters() -> io::Result<()> {
    let (addr, server) = serve(1, None, InFlight::default())?;
    let (mut sender, connection) = connect(addr).await?;
    let (status, body) = send(&mut sender, get("/")).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "hello");
    let (status, body) = send(&mut sender, post("/echo", b"back to me".to_vec())).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "back to me");
    let (status, _) = send(&mut sender, get("/missing")).await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    drop(sender);
    within(connection)
        .await
        .unwrap()
        .map_err(io::Error::other)?;
    within(server).await.unwrap().map_err(io::Error::other)?;
    Ok(())
}

/// Many requests on one connection are served side by side, each stream a
/// task of the executor: the service has many in hand at once.
#[ruio::test]
async fn streams_of_one_connection_are_served_side_by_side() -> io::Result<()> {
    const STREAMS: usize = 64;
    let in_flight = InFlight::default();
    let (addr, server) = serve(1, None, in_flight.clone())?;
    let (sender, connection) = connect(addr).await?;
    let requests: Vec<_> = (0..STREAMS)
        .map(|n| {
            let mut sender = sender.clone();
            ruio::spawn(async move {
                let (status, body) = send(&mut sender, get("/slow")).await?;
                assert_eq!(status, StatusCode::OK);
                assert_eq!(body, "slowly");
                let data = pattern(1000 + n, n);
                let (status, body) = send(&mut sender, post("/echo", data.clone())).await?;
                assert_eq!(status, StatusCode::OK);
                assert!(body == data);
                io::Result::Ok(())
            })
        })
        .collect();
    for request in requests {
        within(request).await.unwrap()?;
    }
    assert!(
        in_flight.most.get() > STREAMS / 2,
        "{} in hand at most",
        in_flight.most.get()
    );
    drop(sender);
    within(connection)
        .await
        .unwrap()
        .map_err(io::Error::other)?;
    within(server).await.unwrap().map_err(io::Error::other)?;
    Ok(())
}

/// A body far larger than a frame, and than the connection's window,
/// streams through both ways, in many frames with flow control between.
#[ruio::test]
async fn large_bodies_stream_through() -> io::Result<()> {
    let pool = BufPool::new(&ruio::driver(), 32, 16 << 10)?;
    let (addr, server) = serve(1, Some(pool), InFlight::default())?;
    let (mut sender, connection) = connect(addr).await?;
    let data = pattern(4 << 20, 9);
    let (status, body) = send(&mut sender, post("/echo", data.clone())).await?;
    assert_eq!(status, StatusCode::OK);
    assert!(body == data);
    drop(sender);
    within(connection)
        .await
        .unwrap()
        .map_err(io::Error::other)?;
    within(server).await.unwrap().map_err(io::Error::other)?;
    Ok(())
}

/// Several connections are served at once, each with its streams.
#[ruio::test]
async fn connections_are_served_side_by_side() -> io::Result<()> {
    const CONNECTIONS: usize = 8;
    let (addr, server) = serve(CONNECTIONS, None, InFlight::default())?;
    let clients: Vec<_> = (0..CONNECTIONS)
        .map(|n| {
            ruio::spawn(async move {
                let (mut sender, connection) = connect(addr).await?;
                for round in 0..4 {
                    let data = pattern(500 * (round + 1), n);
                    let (status, body) = send(&mut sender, post("/echo", data.clone())).await?;
                    assert_eq!(status, StatusCode::OK);
                    assert!(body == data);
                }
                drop(sender);
                within(connection).await.unwrap().map_err(io::Error::other)
            })
        })
        .collect();
    for client in clients {
        within(client).await.unwrap()?;
    }
    within(server).await.unwrap().map_err(io::Error::other)?;
    Ok(())
}

/// The executor spawns on the thread's runtime, futures that need not be
/// `Send`, and runs them to their end on their own.
#[ruio::test]
async fn the_executor_spawns_detached_tasks() -> io::Result<()> {
    use hyper::rt::Executor as _;

    let ran = Rc::new(Cell::new(0));
    let executor = Executor::new();
    for _ in 0..10 {
        let ran = ran.clone();
        executor.execute(async move {
            ruio::yield_now().await;
            ran.set(ran.get() + 1);
        });
    }
    while ran.get() < 10 {
        ruio::yield_now().await;
    }
    let copy = executor;
    assert!(format!("{copy:?}").contains("Executor"));
    Ok(())
}
