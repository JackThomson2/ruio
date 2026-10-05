//! hyper's executor, on ruio's tasks.

use std::future::Future;

use hyper::rt;

/// A [`hyper::rt::Executor`] that spawns on the thread's runtime: what
/// hyper's HTTP/2 needs, which runs each stream of a connection, and the
/// connection's own work, as a task of its own.
///
/// The tasks are ruio's, on the core that serves the connection, and need
/// not be `Send`: hyper's HTTP/2 server and client both run on one core, as
/// its HTTP/1.1 does, with nothing shared between cores.
///
/// ```no_run
/// use hyper::server::conn::http2;
/// use hyper::service::service_fn;
/// use ruio_compat::{Compat, Executor};
/// # use std::convert::Infallible;
/// # use http_body_util::Full;
/// # use hyper::body::{Bytes, Incoming};
/// # use hyper::{Request, Response};
/// # async fn hello(_: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
/// #     Ok(Response::new(Full::from("hello")))
/// # }
///
/// # ruio::Runtime::new().unwrap().block_on(async {
/// let listener = ruio::net::TcpListener::bind("127.0.0.1:3000")?;
/// let (stream, _) = listener.accept().await?;
/// let connection = http2::Builder::new(Executor::new())
///     .serve_connection(Compat::new(stream), service_fn(hello));
/// ruio::spawn(connection);
/// # Ok::<(), std::io::Error>(())
/// # }).unwrap();
/// ```
///
/// # Panics
///
/// Spawning panics if the thread is not inside a runtime's `block_on`,
/// where hyper's connections run.
#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub struct Executor;

impl Executor {
    /// An executor.
    pub fn new() -> Self {
        Self
    }
}

impl<F> rt::Executor<F> for Executor
where
    F: Future + 'static,
    F::Output: 'static,
{
    /// Spawns `future` as a task of the thread's runtime, detached: it runs
    /// to its end on its own.
    fn execute(&self, future: F) {
        drop(ruio::spawn(future));
    }
}
