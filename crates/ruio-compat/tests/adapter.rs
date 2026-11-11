//! The adapter as every engine with I/O has it, through the traits of
//! `tokio::io`, of futures-io and of hyper: the tests name nothing that only
//! one adapter or one engine has, and run on io_uring, over the ring's
//! adapter and over the one built on the traits of `ruio::io`, and on the
//! I/O completion port alike.

#![cfg(all(
    feature = "tokio",
    feature = "futures-io",
    feature = "hyper",
    any(ruio_compat_ring, ruio_compat_generic)
))]

mod support;

use std::io;
use std::path::PathBuf;
use std::time::Duration;

use ruio::net::{TcpListener, TcpStream};
use ruio_compat::Compat;

use support::{connected, pattern, within};

/// A directory of the test's own in the temporary directory, removed with
/// what is in it when dropped.
struct Scratch(PathBuf);

impl Scratch {
    fn new(test: &str) -> Self {
        let name = format!("ruio-compat-{test}-{}", std::process::id());
        let dir = std::env::temp_dir().join(name);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Through the traits of `tokio::io`.
mod tokio_io {
    use std::io::IoSlice;

    use ruio::fs::File;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};

    use super::*;

    /// More than a read buffer holds and a write takes goes one way, and a
    /// reply the other, each side shutting down after it.
    #[ruio::test]
    async fn data_crosses_the_adapters_both_ways() -> io::Result<()> {
        let (client, server) = connected().await?;
        let mut client = Compat::new(client);
        let mut server = Compat::with_capacity(1000, server);
        let message = pattern(1 << 20, 7);
        let expected = message.clone();
        let reader = ruio::spawn(async move {
            let mut received = Vec::new();
            within(server.read_to_end(&mut received)).await?;
            within(server.write_all(b"received")).await?;
            within(server.shutdown()).await?;
            io::Result::Ok(received)
        });
        within(client.write_all(&message)).await?;
        within(client.shutdown()).await?;
        let mut reply = Vec::new();
        within(client.read_to_end(&mut reply)).await?;
        assert!(reader.await.unwrap()? == expected);
        assert_eq!(reply, b"received");
        Ok(())
    }

    /// A vectored write sends its buffers together, in order, as many bytes
    /// of them as one write takes.
    #[ruio::test]
    async fn vectored_writes_gather_their_buffers() -> io::Result<()> {
        let (writer, reader) = connected().await?;
        let (mut writer, mut reader) = (Compat::new(writer), Compat::new(reader));
        assert!(writer.is_write_vectored());
        let small = [
            IoSlice::new(b"head"),
            IoSlice::new(b""),
            IoSlice::new(b"er, "),
            IoSlice::new(b"body"),
        ];
        assert_eq!(within(writer.write_vectored(&small)).await?, 12);
        let (first, second) = (pattern(200 << 10, 1), pattern(200 << 10, 2));
        let large = [IoSlice::new(&first), IoSlice::new(&second)];
        let expected = [b"header, body", &first[..], &second[..]].concat();
        let receiver = ruio::spawn(async move {
            let mut received = Vec::new();
            within(reader.read_to_end(&mut received)).await?;
            io::Result::Ok(received)
        });
        let written = within(writer.write_vectored(&large)).await?;
        assert_eq!(written, 256 << 10, "a write takes 256 KiB at most");
        within(writer.write_all(&expected[12 + written..])).await?;
        within(writer.shutdown()).await?;
        assert!(receiver.await.unwrap()? == expected);
        Ok(())
    }

    /// Lines come out of the adapter's buffer, however they fall across
    /// reads.
    #[ruio::test]
    async fn lines_are_read_from_the_buffer() -> io::Result<()> {
        let (writer, reader) = connected().await?;
        let mut writer = Compat::new(writer);
        within(writer.write_all(b"one\ntwo\nthree, the last\n")).await?;
        within(writer.shutdown()).await?;
        let mut lines = Compat::with_capacity(5, reader).lines();
        let mut read = Vec::new();
        while let Some(line) = within(lines.next_line()).await? {
            read.push(line);
        }
        assert_eq!(read, ["one", "two", "three, the last"]);
        Ok(())
    }

    /// What a read brought in beyond what was asked for waits in the
    /// adapter, which lends it out, and the stream, as they are.
    #[ruio::test]
    async fn what_was_read_and_not_taken_waits_in_the_buffer() -> io::Result<()> {
        let (client, server) = connected().await?;
        let (mut client, mut server) = (Compat::new(client), Compat::new(server));
        assert!(server.buffer().is_empty());
        within(client.write_all(b"abcdef")).await?;
        let mut first = [0; 1];
        within(server.read_exact(&mut first)).await?;
        assert_eq!(&first, b"a");
        assert_eq!(server.buffer(), b"bcdef");
        let mut rest = [0; 5];
        within(server.read_exact(&mut rest)).await?;
        assert_eq!(&rest, b"bcdef");
        assert!(server.buffer().is_empty());
        assert_eq!(
            server.get_ref().peer_addr().await?,
            client.get_ref().local_addr().await?
        );
        Ok(())
    }

    /// tokio's `copy_bidirectional` proxies a connection through the
    /// adapters, to a server that echoes it, and shuts each way down as it
    /// ends.
    #[ruio::test]
    async fn a_proxy_copies_both_ways() -> io::Result<()> {
        let backend = TcpListener::bind("127.0.0.1:0")?;
        let backend_addr = backend.local_addr()?;
        let _backend = ruio::spawn(async move {
            let (stream, _) = backend.accept().await?;
            let mut stream = Compat::new(stream);
            let mut buf = vec![0; 4096];
            loop {
                let read = stream.read(&mut buf).await?;
                if read == 0 {
                    break;
                }
                stream.write_all(&buf[..read]).await?;
            }
            stream.shutdown().await
        });
        let proxy = TcpListener::bind("127.0.0.1:0")?;
        let proxy_addr = proxy.local_addr()?;
        let proxied = ruio::spawn(async move {
            let (client, _) = proxy.accept().await?;
            let mut client = Compat::new(client);
            let mut server = Compat::new(TcpStream::connect(backend_addr).await?);
            tokio::io::copy_bidirectional(&mut client, &mut server).await
        });

        let client = Compat::new(within(TcpStream::connect(proxy_addr)).await?);
        let (mut reader, mut writer) = tokio::io::split(client);
        let message = pattern(300 << 10, 9);
        let sent = message.clone();
        let sender = ruio::spawn(async move {
            within(writer.write_all(&sent)).await?;
            within(writer.shutdown()).await
        });
        let mut echo = Vec::new();
        within(reader.read_to_end(&mut echo)).await?;
        sender.await.unwrap()?;
        assert!(echo == message);
        let len = message.len() as u64;
        assert_eq!(within(proxied).await.unwrap()?, (len, len));
        Ok(())
    }

    /// A file is written through the adapter at its position, one write
    /// after another, and read back through one to its end. It has no half
    /// to shut: shutting down waits for the write in flight, and leaves the
    /// file open for more.
    #[ruio::test]
    async fn a_file_is_written_and_read_back() -> io::Result<()> {
        let scratch = Scratch::new("file");
        let path = scratch.path("data");
        let data = pattern(600 << 10, 4);
        let mut file = Compat::new(within(File::create(&path)).await?);
        within(file.write_all(&data)).await?;
        within(file.shutdown()).await?;
        within(file.write_all(b"and more")).await?;
        within(file.shutdown()).await?;
        drop(file);
        let expected = [&data[..], b"and more"].concat();
        assert!(std::fs::read(&path)? == expected);

        let mut file = Compat::with_capacity(1000, within(File::open(&path)).await?);
        let mut read = Vec::new();
        within(file.read_to_end(&mut read)).await?;
        assert!(read == expected);
        Ok(())
    }

    /// A write to a file still in flight as the adapter is dropped is left
    /// to finish.
    #[ruio::test]
    async fn a_file_keeps_what_was_written_though_the_adapter_is_dropped() -> io::Result<()> {
        let scratch = Scratch::new("dropped");
        let path = scratch.path("data");
        let data = pattern(200 << 10, 6);
        let mut file = Compat::new(within(File::create(&path)).await?);
        within(file.write_all(&data)).await?;
        drop(file);
        within(async {
            while std::fs::metadata(&path)?.len() < data.len() as u64 {
                ruio::time::sleep(Duration::from_millis(1)).await;
            }
            io::Result::Ok(())
        })
        .await?;
        assert!(std::fs::read(&path)? == data);
        Ok(())
    }
}

/// Through the traits of futures-io.
mod futures_io {
    use std::future::poll_fn;
    use std::io::ErrorKind;
    use std::pin::Pin;
    use std::task::Poll;

    use futures_util::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

    use super::*;

    /// Polls a read of `buf` from `stream` once.
    async fn poll_read_once<S: AsyncRead + Unpin>(
        stream: &mut S,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        poll_fn(|cx| Poll::Ready(Pin::new(&mut *stream).poll_read(cx, buf))).await
    }

    /// Reads `stream` to its end, a buffer of `chunk` bytes at a time.
    async fn read_all<S: AsyncRead + Unpin>(stream: &mut S, chunk: usize) -> io::Result<Vec<u8>> {
        let mut received = Vec::new();
        let mut buf = vec![0; chunk];
        loop {
            let read = within(stream.read(&mut buf)).await?;
            if read == 0 {
                return Ok(received);
            }
            received.extend_from_slice(&buf[..read]);
        }
    }

    /// More than a read buffer holds and a write takes goes one way, read
    /// less at a time than the adapter reads, and a reply the other, each
    /// side closing after it.
    #[ruio::test]
    async fn data_crosses_the_adapters_both_ways() -> io::Result<()> {
        let (client, server) = connected().await?;
        let mut client = Compat::new(client);
        let mut server = Compat::with_capacity(1000, server);
        let message = pattern(1 << 20, 3);
        let expected = message.clone();
        let reader = ruio::spawn(async move {
            let received = read_all(&mut server, 300).await?;
            within(server.write_all(b"received")).await?;
            within(server.close()).await?;
            io::Result::Ok(received)
        });
        within(client.write_all(&message)).await?;
        within(client.close()).await?;
        let reply = read_all(&mut client, 300).await?;
        assert!(reader.await.unwrap()? == expected);
        assert_eq!(reply, b"received");
        Ok(())
    }

    /// A read in flight, with nothing to read yet, holds up neither writes
    /// nor the data that then arrives, which the next read takes.
    #[ruio::test]
    async fn a_read_in_flight_leaves_writes_free() -> io::Result<()> {
        let (client, server) = connected().await?;
        let (mut client, mut server) = (Compat::new(client), Compat::new(server));
        let mut buf = [0; 4];
        assert!(poll_read_once(&mut client, &mut buf).await.is_pending());
        within(client.write_all(b"ping")).await?;
        within(client.flush()).await?;
        within(server.read_exact(&mut buf)).await?;
        assert_eq!(&buf, b"ping");
        within(server.write_all(b"pong")).await?;
        within(client.read_exact(&mut buf)).await?;
        assert_eq!(&buf, b"pong");
        Ok(())
    }

    /// What was written goes out though the adapter is dropped before the
    /// write has finished, and the stream closes after it.
    #[ruio::test]
    async fn what_was_written_arrives_though_the_adapter_is_dropped() -> io::Result<()> {
        let (client, server) = connected().await?;
        let (mut client, mut server) = (Compat::new(client), Compat::new(server));
        within(client.write_all(b"last words")).await?;
        drop(client);
        assert_eq!(read_all(&mut server, 64).await?, b"last words");
        Ok(())
    }

    /// The same of a write that waits for the reader, the last of more
    /// than the connection holds: all of it arrives, in order, and then
    /// the end of the stream.
    #[ruio::test]
    async fn a_write_that_waits_arrives_though_the_adapter_is_dropped() -> io::Result<()> {
        let (client, server) = connected().await?;
        let (mut client, mut server) = (Compat::new(client), Compat::new(server));
        let message = pattern(1 << 20, 8);
        let expected = message.clone();
        let reader = ruio::spawn(async move { read_all(&mut server, 64 << 10).await });
        within(client.write_all(&message)).await?;
        drop(client);
        assert!(within(reader).await.unwrap()? == expected);
        Ok(())
    }

    /// Dropping an adapter cancels the read in flight, and closes the
    /// stream.
    #[ruio::test]
    async fn dropping_an_adapter_ends_the_stream() -> io::Result<()> {
        let (client, server) = connected().await?;
        let (mut client, mut server) = (Compat::new(client), Compat::new(server));
        let mut buf = [0; 4];
        assert!(poll_read_once(&mut client, &mut buf).await.is_pending());
        drop(client);
        assert_eq!(within(server.read(&mut buf)).await?, 0, "the stream ends");
        Ok(())
    }

    /// A write that fails, as the peer has closed the connection, fails the
    /// writes after it, and the flush.
    #[ruio::test]
    async fn a_failed_write_fails_what_comes_after() -> io::Result<()> {
        let (client, server) = connected().await?;
        let (mut client, mut server) = (Compat::new(client), Compat::new(server));
        within(client.write_all(b"unread")).await?;
        within(client.flush()).await?;
        let mut buf = [0; 1];
        within(server.read_exact(&mut buf)).await?;
        drop(server);
        let chunk = vec![7; 64 << 10];
        let mut failed = None;
        for _ in 0..1000 {
            if let Err(err) = within(client.write_all(&chunk)).await {
                failed = Some(err);
                break;
            }
        }
        let err = failed.expect("writes to a closed connection fail");
        assert!(
            matches!(
                err.kind(),
                ErrorKind::BrokenPipe | ErrorKind::ConnectionReset | ErrorKind::ConnectionAborted
            ),
            "{err}"
        );
        assert_eq!(within(client.flush()).await.unwrap_err().kind(), err.kind());
        assert_eq!(
            within(client.write(b"more")).await.unwrap_err().kind(),
            err.kind()
        );
        Ok(())
    }

    /// Once closed, the writing half takes nothing more; closing again does
    /// nothing. Empty reads and writes move nothing.
    #[ruio::test]
    async fn writes_after_close_fail() -> io::Result<()> {
        let (client, server) = connected().await?;
        let (mut client, mut server) = (Compat::new(client), Compat::new(server));
        assert_eq!(within(client.read(&mut [])).await?, 0);
        assert_eq!(within(client.write(&[])).await?, 0);
        within(client.close()).await?;
        within(client.close()).await?;
        let err = within(client.write(b"late")).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::BrokenPipe);
        assert!(read_all(&mut server, 64).await?.is_empty());
        Ok(())
    }
}

/// hyper, through its own I/O traits, with ruio's executor and timer.
mod http {
    use std::future::Future;
    use std::time::Instant;

    use http_body_util::{BodyExt, Either, Full};
    use hyper::body::{Bytes, Incoming};
    use hyper::service::service_fn;
    use hyper::{Method, Request, Response, StatusCode};
    use ruio_compat::{Executor, Timer};

    use super::*;

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

    /// A request for `path`, with `body` if there is one to post.
    fn request(path: &str, body: Option<Bytes>) -> Request<Full<Bytes>> {
        let method = match body {
            Some(_) => Method::POST,
            None => Method::GET,
        };
        Request::builder()
            .method(method)
            .uri(path)
            .header("host", "ruio")
            .body(Full::new(body.unwrap_or_default()))
            .unwrap()
    }

    /// The status and the body of the response a client's request gets.
    async fn answer(
        response: impl Future<Output = hyper::Result<Response<Incoming>>>,
    ) -> io::Result<(StatusCode, Bytes)> {
        let response = within(response).await.map_err(io::Error::other)?;
        let status = response.status();
        let body = within(response.into_body().collect())
            .await
            .map_err(io::Error::other)?;
        Ok((status, body.to_bytes()))
    }

    /// Requests follow each other on an HTTP/1.1 connection kept alive, a
    /// body larger than a read buffer and a write goes both ways, and
    /// closing the client's connection ends the server's.
    #[ruio::test]
    async fn hyper_serves_http1_through_the_adapters() -> io::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = ruio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut http = hyper::server::conn::http1::Builder::new();
            http.timer(Timer::new());
            http.serve_connection(Compat::new(stream), service_fn(service))
                .await
                .map_err(io::Error::other)
        });
        let stream = within(TcpStream::connect(addr)).await?;
        let (mut sender, connection) =
            within(hyper::client::conn::http1::handshake(Compat::new(stream)))
                .await
                .map_err(io::Error::other)?;
        let connection = ruio::spawn(connection);
        for _ in 0..3 {
            assert_eq!(
                answer(sender.send_request(request("/", None))).await?,
                (StatusCode::OK, Bytes::from("hello"))
            );
        }
        let (status, _) = answer(sender.send_request(request("/nowhere", None))).await?;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let message = Bytes::from(pattern(1 << 20, 11));
        let echo = request("/echo", Some(message.clone()));
        assert_eq!(
            answer(sender.send_request(echo)).await?,
            (StatusCode::OK, message)
        );
        drop(sender);
        within(connection)
            .await
            .unwrap()
            .map_err(io::Error::other)?;
        within(server).await.unwrap()
    }

    /// The same over HTTP/2, whose streams and connection run as tasks of
    /// ruio's executor, the requests side by side.
    #[ruio::test]
    async fn hyper_serves_http2_through_the_adapters() -> io::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = ruio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            hyper::server::conn::http2::Builder::new(Executor::new())
                .timer(Timer::new())
                .serve_connection(Compat::new(stream), service_fn(service))
                .await
                .map_err(io::Error::other)
        });
        let stream = within(TcpStream::connect(addr)).await?;
        let (mut sender, connection) = within(hyper::client::conn::http2::handshake(
            Executor::new(),
            Compat::new(stream),
        ))
        .await
        .map_err(io::Error::other)?;
        let connection = ruio::spawn(connection);
        let mut echoes = Vec::new();
        for round in 0..8 {
            let message = Bytes::from(pattern(100 << 10, round));
            let echo = sender.send_request(request("/echo", Some(message.clone())));
            echoes.push((message, ruio::spawn(answer(echo))));
        }
        assert_eq!(
            answer(sender.send_request(request("/", None))).await?,
            (StatusCode::OK, Bytes::from("hello"))
        );
        for (message, echo) in echoes {
            assert_eq!(echo.await.unwrap()?, (StatusCode::OK, message));
        }
        drop(sender);
        within(connection)
            .await
            .unwrap()
            .map_err(io::Error::other)?;
        within(server).await.unwrap()
    }

    /// The timer's sleeps are the runtime's, and the executor's tasks the
    /// thread's, on every engine.
    #[ruio::test]
    async fn the_timer_sleeps_and_the_executor_spawns() {
        let start = Instant::now();
        within(hyper::rt::Timer::sleep(
            &Timer::new(),
            Duration::from_millis(20),
        ))
        .await;
        assert!(start.elapsed() >= Duration::from_millis(20));

        let (sent, received) = ruio::sync::oneshot::channel();
        hyper::rt::Executor::execute(&Executor::new(), async move {
            let _ = sent.send(7);
        });
        assert_eq!(within(received).await.unwrap(), 7);
    }
}
