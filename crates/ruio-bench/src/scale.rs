//! The scaling benchmark: every core loads itself, with an echo server and
//! connections of its own, so that nothing crosses from core to core, and
//! the rate of the cores together shows how the runtime scales.
//!
//! Each core runs the echo benchmark's server (see [`crate::server`]) on a
//! port of its own, and connections to it that each send a message and wait
//! for its echo, again and again, until the run ends; round trips that start
//! after the warm-up count. ruio runs on [`ruio::Cores`]; tokio, as a
//! reference, on a current-thread runtime per core, on a thread pinned to
//! it.

use std::io;
use std::net::SocketAddr;
use std::os::fd::AsRawFd;
use std::sync::Barrier;
use std::thread;
use std::time::{Duration, Instant};

use ruio::buf::BufPool;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::server::{self, BUFFER_SIZE};
use crate::sys;

/// What each core runs, and for how long.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Load {
    /// How many cores: the first CPUs the process may run on.
    pub(crate) cores: usize,
    /// The connections of each core.
    pub(crate) connections: usize,
    /// The size of each message.
    pub(crate) size: usize,
    /// How long to run before counting.
    pub(crate) warmup: Duration,
    /// How long to count.
    pub(crate) measure: Duration,
}

impl Load {
    /// The rate of `counts`, the round trips of each core.
    fn rate(&self, counts: impl IntoIterator<Item = io::Result<u64>>) -> io::Result<f64> {
        let mut total = 0;
        for count in counts {
            total += count?;
        }
        Ok(total as f64 / self.measure.as_secs_f64())
    }
}

/// Round trips per second of the cores together, on ruio.
pub(crate) fn ruio(load: Load) -> io::Result<f64> {
    let cores = ruio::Cores::first(load.cores)?.builder(ruio::Runtime::builder().entries(1024));
    // Every core's runtime is ready before any core starts.
    let counts = cores.run(|_| ruio_core(load))?;
    load.rate(counts)
}

async fn ruio_core(load: Load) -> io::Result<u64> {
    let listener = ruio::net::TcpListener::bind("127.0.0.1:0")?;
    sys::set_nodelay(listener.as_raw_fd())?;
    let addr = listener.local_addr()?;
    // A buffer per connection, as the echo benchmark gives its server.
    let buffers = load.connections.clamp(64, 1 << 15).next_power_of_two() as u16;
    let pool = BufPool::new(&ruio::driver(), buffers, BUFFER_SIZE)?;
    let _server = ruio::spawn(server::serve_ruio(listener, pool));
    let from = Instant::now() + load.warmup;
    let until = from + load.measure;
    let clients: Vec<_> = (0..load.connections)
        .map(|_| ruio::spawn(ruio_round_trips(addr, load.size, from, until)))
        .collect();
    let mut total = 0;
    for client in clients {
        total += client.await.map_err(io::Error::other)??;
    }
    Ok(total)
}

/// Sends messages of `size` bytes to `addr` and reads their echoes until
/// `until`, counting the round trips that start from `from`. Each message
/// starts with its number, so an echo of the wrong one shows.
async fn ruio_round_trips(
    addr: SocketAddr,
    size: usize,
    from: Instant,
    until: Instant,
) -> io::Result<u64> {
    let stream = ruio::net::TcpStream::connect(addr).await?;
    stream.set_nodelay(true).await?;
    let mut message = vec![0x5a; size];
    let mut echo = Vec::with_capacity(size);
    let mut count = 0;
    for number in 0u64.. {
        let sent = Instant::now();
        if sent >= until {
            break;
        }
        tag(&mut message, number);
        let (written, sent_message) = stream.write_all(message).await;
        written?;
        message = sent_message;
        echo.clear();
        let (read, received) = stream.read_exact(echo).await;
        read?;
        echo = received;
        check(&echo, &message)?;
        if sent >= from {
            count += 1;
        }
    }
    Ok(count)
}

/// Round trips per second of the cores together, on tokio.
pub(crate) fn tokio(load: Load) -> io::Result<f64> {
    let cpus = ruio::Cores::first(load.cores)?.cpu_list().to_vec();
    // Every core's runtime is ready before any core starts, as with ruio.
    let ready = Barrier::new(cpus.len());
    let counts = thread::scope(|scope| {
        let threads: Vec<_> = cpus
            .iter()
            .map(|&cpu| {
                let ready = &ready;
                scope.spawn(move || {
                    sys::pin(&[cpu])?;
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_io()
                        .build()?;
                    ready.wait();
                    runtime.block_on(tokio_core(load))
                })
            })
            .collect();
        threads
            .into_iter()
            .map(|thread| {
                thread
                    .join()
                    .unwrap_or_else(|_| Err(io::Error::other("a core panicked")))
            })
            .collect::<Vec<_>>()
    });
    load.rate(counts)
}

async fn tokio_core(load: Load) -> io::Result<u64> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    sys::set_nodelay(listener.as_raw_fd())?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(server::serve_tokio(listener));
    let from = Instant::now() + load.warmup;
    let until = from + load.measure;
    let clients: Vec<_> = (0..load.connections)
        .map(|_| tokio::spawn(tokio_round_trips(addr, load.size, from, until)))
        .collect();
    let mut total = 0;
    for client in clients {
        total += client.await.map_err(io::Error::other)??;
    }
    server.abort();
    Ok(total)
}

/// As [`ruio_round_trips`], on tokio.
async fn tokio_round_trips(
    addr: SocketAddr,
    size: usize,
    from: Instant,
    until: Instant,
) -> io::Result<u64> {
    let mut stream = tokio::net::TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    let mut message = vec![0x5a; size];
    let mut echo = vec![0; size];
    let mut count = 0;
    for number in 0u64.. {
        let sent = Instant::now();
        if sent >= until {
            break;
        }
        tag(&mut message, number);
        stream.write_all(&message).await?;
        stream.read_exact(&mut echo).await?;
        check(&echo, &message)?;
        if sent >= from {
            count += 1;
        }
    }
    Ok(count)
}

/// Starts `message` with `number`, as far as it has room.
fn tag(message: &mut [u8], number: u64) {
    let tag = number.to_le_bytes();
    let tagged = tag.len().min(message.len());
    message[..tagged].copy_from_slice(&tag[..tagged]);
}

fn check(echo: &[u8], message: &[u8]) -> io::Result<()> {
    if echo == message {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "the echo differs from the message",
        ))
    }
}
