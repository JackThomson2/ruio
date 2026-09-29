//! The echo servers: each sends back whatever its connections send, on one
//! thread.

use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::os::fd::AsRawFd;
use std::str::FromStr;

use ruio::buf::BufPool;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::sys;

/// The size of the buffers every server receives into.
pub(crate) const BUFFER_SIZE: u32 = 16 << 10;

/// An echo server implementation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Server {
    /// ruio: a multishot accept, and per connection a multishot receive
    /// into a shared pool; whatever has arrived goes back out with one
    /// vectored send.
    Ruio,
    /// The same, with the connections as direct descriptors.
    RuioDirect,
    /// tokio's current-thread runtime, the single-threaded equal of a ruio
    /// runtime: an accept loop, and per connection a task that reads into
    /// a buffer of its own and writes back what it read.
    Tokio,
}

impl Server {
    pub(crate) const ALL: [Server; 3] = [Server::Tokio, Server::Ruio, Server::RuioDirect];
}

impl FromStr for Server {
    type Err = String;

    fn from_str(name: &str) -> Result<Self, String> {
        match name {
            "ruio" => Ok(Server::Ruio),
            "ruio-direct" => Ok(Server::RuioDirect),
            "tokio" => Ok(Server::Tokio),
            _ => Err(format!(
                "unknown server {name:?}; try ruio, ruio-direct or tokio"
            )),
        }
    }
}

impl fmt::Display for Server {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Server::Ruio => "ruio",
            Server::RuioDirect => "ruio-direct",
            Server::Tokio => "tokio",
        })
    }
}

/// Runs `server` on `addr` until the process is killed. `buffers` sizes
/// ruio's pool; tokio's connections have a buffer each.
pub(crate) fn run(server: Server, addr: SocketAddr, buffers: u16) -> io::Result<()> {
    match server {
        Server::Ruio => ruio(addr, buffers, false),
        Server::RuioDirect => ruio(addr, buffers, true),
        Server::Tokio => tokio(addr),
    }
}

fn ruio(addr: SocketAddr, buffers: u16, direct: bool) -> io::Result<()> {
    let mut builder = ruio::Runtime::builder().entries(4096);
    if direct {
        builder = builder.files(1 << 16);
    }
    builder.build()?.block_on(async move {
        let listener = ruio::net::TcpListener::bind(addr)?;
        sys::set_nodelay(listener.as_raw_fd())?;
        listener.set_direct(direct);
        let pool = BufPool::new(&ruio::driver(), buffers, BUFFER_SIZE)?;
        let mut incoming = listener.incoming();
        while let Some(stream) = incoming.next().await {
            let stream = match stream {
                Ok(stream) => stream,
                Err(err) => {
                    eprintln!("ruio echo server: accept failed: {err}");
                    continue;
                }
            };
            let pool = pool.clone();
            let _detached = ruio::spawn(async move {
                let mut received = stream.recv_stream(&pool);
                while let Some(Ok(mut data)) = received.next().await {
                    while let Some(Ok(more)) = received.next_ready() {
                        data.append(more);
                    }
                    if stream.write_all_vectored(data).await.0.is_err() {
                        break;
                    }
                }
            });
        }
        Ok(())
    })
}

fn tokio(addr: SocketAddr) -> io::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()?;
    runtime.block_on(async move {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        sys::set_nodelay(listener.as_raw_fd())?;
        loop {
            let mut stream = match listener.accept().await {
                Ok((stream, _)) => stream,
                Err(err) => {
                    eprintln!("tokio echo server: accept failed: {err}");
                    continue;
                }
            };
            tokio::spawn(async move {
                let mut buf = vec![0; BUFFER_SIZE as usize];
                loop {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(read) => {
                            if stream.write_all(&buf[..read]).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            });
        }
    })
}
