//! The load generator: connections that each send a message and wait for
//! its echo, again and again.
//!
//! It is the same for every server, and runs on tokio's multi-threaded
//! runtime, on CPUs of its own, with enough threads that the server, on
//! one CPU, is what limits the rate.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;

/// What to put on the server.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Load {
    pub(crate) connections: usize,
    /// The size of each message.
    pub(crate) size: usize,
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
            .map(|stream| tokio::spawn(round_trips(stream, load.size, from, until)))
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

/// Sends messages of `size` bytes and reads their echoes until `until`,
/// counting and timing the round trips that start from `from`. Each
/// message starts with its number, so an echo of the wrong one shows.
async fn round_trips(
    mut stream: TcpStream,
    size: usize,
    from: Instant,
    until: Instant,
) -> io::Result<(u64, Vec<u32>)> {
    let mut message = vec![0x5a; size];
    let mut echo = vec![0; size];
    let mut count = 0;
    let mut latencies = Vec::new();
    for number in 0u64.. {
        let sent = Instant::now();
        if sent >= until {
            break;
        }
        let tag = number.to_le_bytes();
        let tagged = tag.len().min(size);
        message[..tagged].copy_from_slice(&tag[..tagged]);
        stream.write_all(&message).await?;
        stream.read_exact(&mut echo).await?;
        let took = sent.elapsed();
        if echo != message {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "the echo differs from the message",
            ));
        }
        if sent >= from {
            count += 1;
            latencies.push(u32::try_from(took.as_nanos()).unwrap_or(u32::MAX));
        }
    }
    Ok((count, latencies))
}
