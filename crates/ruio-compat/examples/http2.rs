//! An HTTP/2 server with hyper, on every core the process may run on, each
//! with a listener of its own on one address: it says hello at `/`, and
//! sends back what is posted to `/echo`. The connections are HTTP/2 from
//! the start, with no upgrade from HTTP/1.1, as a client that knows the
//! server speaks it makes them.
//!
//! ```text
//! cargo run --release -p ruio-compat --example http2 [ADDRESS]
//! curl --http2-prior-knowledge http://127.0.0.1:3000/
//! curl --http2-prior-knowledge --data-binary @Cargo.toml http://127.0.0.1:3000/echo
//! ```
//!
//! HTTP/2 runs each stream of a connection as a task, which hyper spawns
//! through an executor: `ruio_compat::Executor` spawns them on the core's
//! runtime, where they need not be `Send`. The connection's I/O goes through
//! `Compat`, which has hyper's own traits, receiving into a pool of the
//! core's with a multishot receive.

use std::io;

use http_body_util::{Either, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http2;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use ruio::buf::BufPool;
use ruio::net::TcpListener;
use ruio_compat::{Compat, Executor, Timer};

/// A response's body: all of it at once, or the request's, streamed back as
/// it arrives.
type Body = Either<Full<Bytes>, Incoming>;

async fn serve(request: Request<Incoming>) -> Result<Response<Body>, hyper::Error> {
    let response = match (request.method(), request.uri().path()) {
        (&Method::GET, "/") => Response::new(Either::Left(Full::from("Hello from ruio!\n"))),
        (&Method::POST, "/echo") => Response::new(Either::Right(request.into_body())),
        _ => {
            let mut response = Response::new(Either::Left(Full::from("Not found\n")));
            *response.status_mut() = StatusCode::NOT_FOUND;
            response
        }
    };
    Ok(response)
}

#[ruio::main(cores = "all")]
async fn main() -> io::Result<()> {
    let address = std::env::args().nth(1);
    let address = address.as_deref().unwrap_or("127.0.0.1:3000");
    let listener = TcpListener::bind_reuseport(address)?;
    if ruio::core().is_some_and(|core| core.index() == 0) {
        println!(
            "listening for HTTP/2 on http://{} with {} cores",
            listener.local_addr()?,
            ruio::core_count()
        );
    }
    let pool = BufPool::new(&ruio::driver(), 256, 16 << 10)?;
    let mut http = http2::Builder::new(Executor::new());
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
        stream.set_nodelay(true).await?;
        let connection = http.serve_connection(Compat::with_pool(&pool, stream), service_fn(serve));
        let _detached = ruio::spawn(async move {
            if let Err(err) = connection.await {
                eprintln!("connection failed: {err}");
            }
        });
    }
    Ok(())
}
