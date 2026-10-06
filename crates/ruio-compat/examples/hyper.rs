//! An HTTP/1.1 server with hyper, on every core the process may run on,
//! each with a listener of its own on one address: it says hello at `/`,
//! and sends back what is posted to `/echo`.
//!
//! ```text
//! cargo run --release -p ruio-compat --example hyper [ADDRESS]
//! curl http://127.0.0.1:3000/
//! curl --data-binary @Cargo.toml http://127.0.0.1:3000/echo
//! ```
//!
//! hyper takes its I/O through traits of its own, which `Compat` gives to
//! ruio's streams, so hyper serves an adapter as it is. Each connection is a
//! task of its core, which need not be `Send`, and receives into a pool of
//! the core's with a multishot receive, which costs the ring no request for
//! each read. hyper reads what arrives at once, into buffers of its own, so
//! a connection holds a buffer of the pool only while a request waits for
//! hyper, except while hyper has stopped reading a request's body to send
//! an echo that the client is slow to read.
//!
//! hyper's timeouts take ruio's timers through `ruio_compat::Timer`: a
//! client that takes longer than 30 seconds to send a request's head, the
//! first or one on a connection kept alive, is cut off.

use std::io;

use http_body_util::{Either, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use ruio::buf::BufPool;
use ruio::net::TcpListener;
use ruio_compat::{Compat, Timer};

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
            "listening on http://{} with {} cores",
            listener.local_addr()?,
            ruio::core_count()
        );
    }
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
        // Responses go out as soon as they are written; the option is set
        // through the ring, in the submission of the connection's first read.
        stream.set_nodelay(true).await?;
        let io = Compat::with_pool(&pool, stream);
        let connection = http.serve_connection(io, service_fn(serve));
        let _detached = ruio::spawn(async move {
            if let Err(err) = connection.await {
                eprintln!("connection failed: {err}");
            }
        });
    }
    Ok(())
}
