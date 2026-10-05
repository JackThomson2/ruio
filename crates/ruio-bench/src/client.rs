//! The load generator: connections that each send a message and wait for
//! its echo, or a request and wait for its response, again and again; and,
//! for HTTP/2, connections that each keep several streams going, each a
//! request and its response again and again.
//!
//! It is the same for every server, and runs on tokio's multi-threaded
//! runtime, on CPUs of its own, with enough threads that the server, on
//! one CPU, is what limits the rate.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper::client::conn::http2;
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;

use crate::http;

/// What each connection sends and waits for, again and again.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Exchange {
    /// A message of this many bytes, and its echo.
    Echo(usize),
    /// An HTTP/1.1 request, and a `200 OK` response saying [`http::BODY`].
    Http,
}

/// What to put on the server.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Load {
    pub(crate) connections: usize,
    pub(crate) exchange: Exchange,
    /// How long to run before measuring.
    pub(crate) warmup: Duration,
    /// How long to measure.
    pub(crate) measure: Duration,
    /// The client's threads.
    pub(crate) threads: usize,
}

/// What a run measured.
#[derive(Debug)]
pub(crate) struct Measurement {
    round_trips: u64,
    seconds: f64,
    /// The round-trip times in nanoseconds, sorted.
    latencies: Vec<u32>,
}

impl Measurement {
    /// Round trips per second.
    pub(crate) fn rate(&self) -> f64 {
        self.round_trips as f64 / self.seconds
    }

    /// The round-trip time that `fraction` of them take at most, in
    /// microseconds.
    pub(crate) fn percentile(&self, fraction: f64) -> f64 {
        let Some(last) = self.latencies.len().checked_sub(1) else {
            return f64::NAN;
        };
        let index = (last as f64 * fraction).round() as usize;
        f64::from(self.latencies[index]) / 1000.0
    }
}

/// Connects `load.connections` clients to `addr` and measures their round
/// trips.
pub(crate) fn run(addr: SocketAddr, load: Load) -> io::Result<Measurement> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(load.threads)
        .enable_io()
        .build()?;
    runtime.block_on(async move {
        // A hundred connections at a time, so that the server's backlog
        // never overflows, which would stall a connection for a second.
        let permits = Arc::new(Semaphore::new(100));
        let connects: Vec<_> = (0..load.connections)
            .map(|_| {
                let permits = Arc::clone(&permits);
                tokio::spawn(async move {
                    let _permit = permits.acquire_owned().await.map_err(io::Error::other)?;
                    let stream = TcpStream::connect(addr).await?;
                    stream.set_nodelay(true)?;
                    io::Result::Ok(stream)
                })
            })
            .collect();
        let mut streams = Vec::with_capacity(load.connections);
        for connect in connects {
            streams.push(connect.await.map_err(io::Error::other)??);
        }

        let from = Instant::now() + load.warmup;
        let until = from + load.measure;
        let tasks: Vec<_> = streams
            .into_iter()
            .map(|stream| tokio::spawn(round_trips(stream, load.exchange, from, until)))
            .collect();
        let mut total = 0;
        let mut latencies = Vec::new();
        for task in tasks {
            let (count, mut times) = task.await.map_err(io::Error::other)??;
            total += count;
            latencies.append(&mut times);
        }
        latencies.sort_unstable();
        Ok(Measurement {
            round_trips: total,
            seconds: load.measure.as_secs_f64(),
            latencies,
        })
    })
}

/// Connects `load.connections` HTTP/2 connections of hyper's client to
/// `addr`, each with `streams` streams going at once, and measures their
/// round trips: a request each, and its response.
pub(crate) fn run_http2(addr: SocketAddr, load: Load, streams: usize) -> io::Result<Measurement> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(load.threads)
        .enable_io()
        .build()?;
    runtime.block_on(async move {
        let permits = Arc::new(Semaphore::new(100));
        let connects: Vec<_> = (0..load.connections)
            .map(|_| {
                let permits = Arc::clone(&permits);
                tokio::spawn(async move {
                    let _permit = permits.acquire_owned().await.map_err(io::Error::other)?;
                    let stream = TcpStream::connect(addr).await?;
                    stream.set_nodelay(true)?;
                    let (sender, connection) = http2::handshake::<_, _, Empty<Bytes>>(
                        TokioExecutor::new(),
                        TokioIo::new(stream),
                    )
                    .await
                    .map_err(io::Error::other)?;
                    tokio::spawn(connection);
                    io::Result::Ok(sender)
                })
            })
            .collect();
        let mut senders = Vec::with_capacity(load.connections);
        for connect in connects {
            senders.push(connect.await.map_err(io::Error::other)??);
        }

        let from = Instant::now() + load.warmup;
        let until = from + load.measure;
        let tasks: Vec<_> = senders
            .iter()
            .flat_map(|sender| {
                (0..streams).map(|_| tokio::spawn(http2_round_trips(sender.clone(), from, until)))
            })
            .collect();
        let mut total = 0;
        let mut latencies = Vec::new();
        for task in tasks {
            let (count, mut times) = task.await.map_err(io::Error::other)??;
            total += count;
            latencies.append(&mut times);
        }
        latencies.sort_unstable();
        Ok(Measurement {
            round_trips: total,
            seconds: load.measure.as_secs_f64(),
            latencies,
        })
    })
}

/// Makes HTTP/2 requests on one stream after another of `sender`'s
/// connection until `until`, counting and timing those that start from
/// `from`, and checking every response.
async fn http2_round_trips(
    mut sender: http2::SendRequest<Empty<Bytes>>,
    from: Instant,
    until: Instant,
) -> io::Result<(u64, Vec<u32>)> {
    let mut count = 0;
    let mut latencies = Vec::new();
    loop {
        let sent = Instant::now();
        if sent >= until {
            break;
        }
        let request = Request::builder()
            .method(Method::GET)
            .uri("/plaintext")
            .body(Empty::new())
            .map_err(io::Error::other)?;
        let response = sender
            .send_request(request)
            .await
            .map_err(io::Error::other)?;
        if response.status() != StatusCode::OK {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("a response with status {}", response.status()),
            ));
        }
        let body = response
            .into_body()
            .collect()
            .await
            .map_err(io::Error::other)?
            .to_bytes();
        if body != http::BODY {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "a response with the wrong body",
            ));
        }
        let took = sent.elapsed();
        if sent >= from {
            count += 1;
            latencies.push(u32::try_from(took.as_nanos()).unwrap_or(u32::MAX));
        }
    }
    Ok((count, latencies))
}

/// Makes exchanges until `until`, counting and timing those that start from
/// `from`. Each message starts with its number, so an echo of the wrong
/// one shows.
async fn round_trips(
    mut stream: TcpStream,
    exchange: Exchange,
    from: Instant,
    until: Instant,
) -> io::Result<(u64, Vec<u32>)> {
    let size = match exchange {
        Exchange::Echo(size) => size,
        Exchange::Http => 0,
    };
    let mut message = vec![0x5a; size];
    let mut reply = vec![0; size];
    let mut response = Vec::with_capacity(4096);
    let mut count = 0;
    let mut latencies = Vec::new();
    for number in 0u64.. {
        let sent = Instant::now();
        if sent >= until {
            break;
        }
        match exchange {
            Exchange::Echo(_) => echo(&mut stream, &mut message, &mut reply, number).await?,
            Exchange::Http => request(&mut stream, &mut response).await?,
        }
        let took = sent.elapsed();
        if sent >= from {
            count += 1;
            latencies.push(u32::try_from(took.as_nanos()).unwrap_or(u32::MAX));
        }
    }
    Ok((count, latencies))
}

/// Sends `message`, tagged with `number`, and reads its echo into `echo`.
async fn echo(
    stream: &mut TcpStream,
    message: &mut [u8],
    echo: &mut [u8],
    number: u64,
) -> io::Result<()> {
    let tag = number.to_le_bytes();
    let tagged = tag.len().min(message.len());
    message[..tagged].copy_from_slice(&tag[..tagged]);
    stream.write_all(message).await?;
    stream.read_exact(echo).await?;
    if echo != message {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "the echo differs from the message",
        ));
    }
    Ok(())
}

/// Sends [`http::REQUEST`] and reads the response into `response`, checking
/// that it says [`http::BODY`].
async fn request(stream: &mut TcpStream, response: &mut Vec<u8>) -> io::Result<()> {
    stream.write_all(http::REQUEST).await?;
    response.clear();
    loop {
        if let Some(len) = http::response_len(response)?
            && response.len() >= len
        {
            // One request at a time: nothing follows the response.
            if response.len() > len || !response.ends_with(http::BODY) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "a response with the wrong body",
                ));
            }
            return Ok(());
        }
        if stream.read_buf(response).await? == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
    }
}
