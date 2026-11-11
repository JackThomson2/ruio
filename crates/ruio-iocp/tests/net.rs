//! TCP sockets through the port: accepting, connecting, reading and writing,
//! with the standard library's sockets at the other end and with each other,
//! and operations cancelled while the system has them.
#![cfg(windows)]

use std::cell::Cell;
use std::io::{self, Read as _, Write as _};
use std::net::{Ipv4Addr, Ipv6Addr, Shutdown, SocketAddr};
use std::rc::Rc;
use std::thread;
use std::time::{Duration, Instant};

use ruio_buf::{IoBuf, IoBufMut};
use ruio_executor::{Runtime, time};
use ruio_iocp::net::{TcpListener, TcpStream};

/// The loopback address, and a port for the system to choose.
fn loopback() -> SocketAddr {
    (Ipv4Addr::LOCALHOST, 0).into()
}

/// Writes all of `buf` to `stream`, each write taking what the last left.
async fn write_all(stream: &TcpStream, mut buf: Vec<u8>) -> io::Result<()> {
    let mut written = 0;
    while written < buf.len() {
        let (result, rest) = stream.write(buf.slice(written..)).await;
        buf = rest.into_inner();
        match result? {
            0 => return Err(io::ErrorKind::WriteZero.into()),
            more => written += more,
        }
    }
    Ok(())
}

/// A listener, a stream connected to it, and the stream it accepted.
async fn pair() -> io::Result<(TcpListener, TcpStream, TcpStream)> {
    let listener = TcpListener::bind(loopback())?;
    let client = TcpStream::connect(listener.local_addr()?).await?;
    let (server, peer) = listener.accept().await?;
    assert_eq!(peer, client.local_addr()?);
    Ok((listener, client, server))
}

/// A buffer that says when it is dropped.
struct Watched {
    bytes: Vec<u8>,
    dropped: Rc<Cell<bool>>,
}

impl Drop for Watched {
    fn drop(&mut self) {
        self.dropped.set(true);
    }
}

// SAFETY: the bytes live on the heap, which moving the buffer leaves in
// place, as for `Vec<u8>`.
unsafe impl IoBuf for Watched {
    fn stable_ptr(&self) -> *const u8 {
        self.bytes.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.bytes.len()
    }

    fn bytes_total(&self) -> usize {
        self.bytes.capacity()
    }
}

// SAFETY: as for `IoBuf`; `set_init` extends the length only as far as the
// caller vouches for, as `Vec<u8>`'s does.
unsafe impl IoBufMut for Watched {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.bytes.as_mut_ptr()
    }

    unsafe fn set_init(&mut self, pos: usize) {
        if pos > self.bytes.len() {
            // SAFETY: the caller vouches for the first `pos` bytes, within
            // the capacity.
            unsafe { self.bytes.set_len(pos) };
        }
    }
}

#[test]
fn a_listener_accepts_a_connection_of_the_standard_librarys() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let listener = TcpListener::bind(loopback())?;
        let addr = listener.local_addr()?;
        let client = thread::spawn(move || -> io::Result<(SocketAddr, [u8; 4])> {
            let mut stream = std::net::TcpStream::connect(addr)?;
            stream.write_all(b"ping")?;
            let mut reply = [0; 4];
            stream.read_exact(&mut reply)?;
            Ok((stream.local_addr()?, reply))
        });
        let (stream, peer) = listener.accept().await?;
        assert_eq!(stream.peer_addr()?, peer);
        assert_eq!(stream.local_addr()?, addr);
        let (read, buf) = stream.read(Vec::with_capacity(16)).await;
        assert_eq!(read?, 4);
        assert_eq!(buf, b"ping");
        let (written, _) = stream.write(b"pong".to_vec()).await;
        assert_eq!(written?, 4);
        let (client_addr, reply) = client.join().unwrap()?;
        assert_eq!(client_addr, peer);
        assert_eq!(&reply, b"pong");
        Ok(())
    })
}

#[test]
fn a_stream_connects_to_a_listener_of_the_standard_librarys() -> io::Result<()> {
    let listener = std::net::TcpListener::bind(loopback())?;
    let addr = listener.local_addr()?;
    let server = thread::spawn(move || -> io::Result<()> {
        let (mut stream, _) = listener.accept()?;
        let mut request = [0; 4];
        stream.read_exact(&mut request)?;
        stream.write_all(&request)
    });
    Runtime::new()?.block_on(async {
        let stream = TcpStream::connect(addr).await?;
        assert_eq!(stream.peer_addr()?, addr);
        stream.set_nodelay(true)?;
        assert!(stream.nodelay()?);
        let (written, _) = stream.write(b"echo".to_vec()).await;
        assert_eq!(written?, 4);
        let (read, buf) = stream.read(Vec::with_capacity(16)).await;
        assert_eq!(read?, 4);
        assert_eq!(buf, b"echo");
        // The server's thread ends, which closes its end.
        server.join().unwrap()?;
        let (read, _) = stream.read(Vec::with_capacity(16)).await;
        assert_eq!(read?, 0);
        Ok(())
    })
}

#[test]
fn streams_of_one_runtime_echo_a_megabyte() -> io::Result<()> {
    const LEN: usize = 1 << 20;
    Runtime::new()?.block_on(async {
        let listener = TcpListener::bind(loopback())?;
        let addr = listener.local_addr()?;
        let server = ruio_executor::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut echoed = 0;
            loop {
                let (read, buf) = stream.read(Vec::with_capacity(64 * 1024)).await;
                if read? == 0 {
                    return io::Result::Ok(echoed);
                }
                echoed += buf.len();
                write_all(&stream, buf).await?;
            }
        });
        let client = Rc::new(TcpStream::connect(addr).await?);
        let data: Vec<u8> = (0..LEN).map(|i| (i % 251) as u8).collect();
        // The echo is read while the data is written, as neither end's
        // buffers hold a megabyte.
        let reader = ruio_executor::spawn({
            let client = Rc::clone(&client);
            async move {
                let mut back = Vec::with_capacity(LEN);
                while back.len() < LEN {
                    let (read, buf) = client.read(Vec::with_capacity(64 * 1024)).await;
                    if read? == 0 {
                        break;
                    }
                    back.extend_from_slice(&buf);
                }
                io::Result::Ok(back)
            }
        });
        write_all(&client, data.clone()).await?;
        client.shutdown(Shutdown::Write)?;
        assert!(reader.await.unwrap()? == data, "the echo is the data");
        assert_eq!(server.await.unwrap()?, LEN);
        Ok(())
    })
}

#[test]
fn several_connections_are_served_at_once() -> io::Result<()> {
    const CLIENTS: u8 = 16;
    Runtime::new()?.block_on(async {
        let listener = TcpListener::bind(loopback())?;
        let addr = listener.local_addr()?;
        let server = ruio_executor::spawn(async move {
            let mut serving = Vec::new();
            for _ in 0..CLIENTS {
                let (stream, _) = listener.accept().await?;
                serving.push(ruio_executor::spawn(async move {
                    let (read, mut buf) = stream.read(Vec::with_capacity(8)).await;
                    assert_eq!(read?, 1);
                    buf[0] += 100;
                    let (written, _) = stream.write(buf).await;
                    written.map(drop)
                }));
            }
            for served in serving {
                served.await.unwrap()?;
            }
            io::Result::Ok(())
        });
        let clients: Vec<_> = (0..CLIENTS)
            .map(|client| {
                ruio_executor::spawn(async move {
                    let stream = TcpStream::connect(addr).await?;
                    let (written, _) = stream.write(vec![client]).await;
                    assert_eq!(written?, 1);
                    let (read, buf) = stream.read(Vec::with_capacity(8)).await;
                    assert_eq!(read?, 1);
                    io::Result::Ok(buf[0])
                })
            })
            .collect();
        for (client, reply) in (0..CLIENTS).zip(clients) {
            assert_eq!(reply.await.unwrap()?, client + 100);
        }
        server.await.unwrap()
    })
}

#[test]
fn sockets_speak_ipv6() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let listener = match TcpListener::bind((Ipv6Addr::LOCALHOST, 0)) {
            Ok(listener) => listener,
            Err(err) => {
                eprintln!("skipped: no IPv6 loopback: {err}");
                return Ok(());
            }
        };
        let addr = listener.local_addr()?;
        assert!(addr.is_ipv6());
        let client = TcpStream::connect(addr).await?;
        let (server, peer) = listener.accept().await?;
        assert_eq!(peer, client.local_addr()?);
        assert_eq!(client.peer_addr()?, addr);
        let (written, _) = client.write(b"six".to_vec()).await;
        assert_eq!(written?, 3);
        let (read, buf) = server.read(Vec::with_capacity(8)).await;
        assert_eq!(read?, 3);
        assert_eq!(buf, b"six");
        Ok(())
    })
}

#[test]
fn a_stream_shut_down_for_writing_still_reads() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let (_listener, client, server) = pair().await?;
        client.shutdown(Shutdown::Write)?;
        let (read, _) = server.read(Vec::with_capacity(8)).await;
        assert_eq!(read?, 0);
        let (written, _) = server.write(b"late".to_vec()).await;
        assert_eq!(written?, 4);
        let (read, buf) = client.read(Vec::with_capacity(8)).await;
        assert_eq!(read?, 4);
        assert_eq!(buf, b"late");
        Ok(())
    })
}

#[test]
fn a_connect_to_a_port_nothing_listens_on_is_refused() -> io::Result<()> {
    // A port that was free a moment ago.
    let addr = std::net::TcpListener::bind(loopback())?.local_addr()?;
    Runtime::new()?.block_on(async {
        let err = TcpStream::connect(addr).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::ConnectionRefused, "{err}");
        Ok(())
    })
}

#[test]
fn a_dropped_accept_is_cancelled_and_the_listener_accepts_after() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let listener = TcpListener::bind(loopback())?;
        // Nothing connects, so the accept waits until the timeout drops it.
        let waited = time::timeout(Duration::from_millis(20), listener.accept()).await;
        assert!(waited.is_err());
        let client = TcpStream::connect(listener.local_addr()?).await?;
        let (server, peer) = listener.accept().await?;
        assert_eq!(peer, client.local_addr()?);
        let (written, _) = client.write(b"after".to_vec()).await;
        assert_eq!(written?, 5);
        let (read, buf) = server.read(Vec::with_capacity(16)).await;
        assert_eq!(read?, 5);
        assert_eq!(buf, b"after");
        Ok(())
    })
}

#[test]
fn a_dropped_read_is_cancelled_and_frees_its_buffer_once_the_system_is_done() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let (_listener, client, server) = pair().await?;
        let dropped = Rc::new(Cell::new(false));
        let buf = Watched {
            bytes: Vec::with_capacity(16),
            dropped: Rc::clone(&dropped),
        };
        // Nothing is written, so the read waits until the timeout drops it.
        let waited = time::timeout(Duration::from_millis(20), server.read(buf)).await;
        assert!(waited.is_err());
        // The cancellation completes through the port, and the engine frees
        // the buffer then.
        for _ in 0..200 {
            if dropped.get() {
                break;
            }
            time::sleep(Duration::from_millis(5)).await;
        }
        assert!(dropped.get(), "the cancelled read's buffer is freed");
        // The stream works after.
        let (written, _) = client.write(b"after".to_vec()).await;
        assert_eq!(written?, 5);
        let (read, buf) = server.read(Vec::with_capacity(16)).await;
        assert_eq!(read?, 5);
        assert_eq!(buf, b"after");
        Ok(())
    })
}

#[test]
fn a_runtime_dropped_with_an_accept_in_flight_waits_for_its_cancellation() -> io::Result<()> {
    // Long enough that only the accept's cancellation completing, not the
    // timeout, ends the wait in time.
    let runtime = Runtime::builder()
        .shutdown_timeout(Duration::from_secs(60))
        .build()?;
    runtime.block_on(async {
        let listener = TcpListener::bind(loopback())?;
        // Detached, so that the runtime holds the task, and its accept, when
        // `block_on` returns.
        drop(ruio_executor::spawn(async move {
            let _ = listener.accept().await;
        }));
        ruio_executor::yield_now().await;
        io::Result::Ok(())
    })?;
    let started = Instant::now();
    drop(runtime);
    assert!(started.elapsed() < Duration::from_secs(30));
    Ok(())
}

#[test]
fn a_stream_fills_a_buffer_whole_and_writes_all_of_one() -> io::Result<()> {
    const LEN: usize = 300 * 1024;
    Runtime::new()?.block_on(async {
        let (_listener, client, server) = pair().await?;
        let data: Vec<u8> = (0..LEN).map(|i| (i % 253) as u8).collect();
        let reading = ruio_executor::spawn(async move {
            // A stream's read fills the buffer from its start, over what it
            // held.
            let mut buf = Vec::with_capacity(LEN);
            buf.extend_from_slice(b"old");
            let (read, buf) = server.read_exact(buf.slice(..LEN)).await;
            read.map(|()| buf.into_inner())
        });
        let (written, data) = client.write_all(data).await;
        written?;
        assert!(reading.await.unwrap()? == data, "what is read was written");
        Ok(())
    })
}

#[test]
fn a_read_that_cannot_fill_its_buffer_says_the_stream_ended() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let (_listener, client, server) = pair().await?;
        let (written, _) = client.write_all(b"abc".to_vec()).await;
        written?;
        drop(client);
        let (read, buf) = server.read_exact(Vec::with_capacity(8)).await;
        assert_eq!(read.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(buf, b"abc");
        Ok(())
    })
}

#[test]
fn several_buffers_are_written_as_one() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let (_listener, client, server) = pair().await?;
        let bufs = vec![
            b"hello, ".to_vec(),
            Vec::new(),
            b"wor".to_vec(),
            b"ld".to_vec(),
        ];
        let (written, bufs) = client.write_all_vectored(bufs).await;
        written?;
        assert_eq!(bufs, [&b"hello, "[..], b"", b"wor", b"ld"]);
        let (read, buf) = server.read_exact(Vec::with_capacity(12).slice(..12)).await;
        read?;
        assert_eq!(buf.into_inner(), b"hello, world");

        // An array of buffers is written the same, and buffers that hold
        // nothing write nothing.
        let (written, _) = server
            .write_all_vectored([b"ab".to_vec(), b"cd".to_vec()])
            .await;
        written?;
        let (written, _) = server.write_all_vectored(Vec::<Vec<u8>>::new()).await;
        written?;
        let (written, _) = server
            .write_all_vectored([Vec::<u8>::new(), Vec::new()])
            .await;
        written?;
        let (read, buf) = client.read_exact(Vec::with_capacity(4).slice(..4)).await;
        read?;
        assert_eq!(buf.into_inner(), b"abcd");
        Ok(())
    })
}

#[test]
fn many_buffers_are_written_as_one_while_the_peer_reads() -> io::Result<()> {
    const BUFFERS: usize = 64;
    const LEN: usize = 16 * 1024;
    Runtime::new()?.block_on(async {
        let (_listener, client, server) = pair().await?;
        let bufs: Vec<Vec<u8>> = (0..BUFFERS)
            .map(|buffer| (0..LEN).map(|at| (buffer * 7 + at % 251) as u8).collect())
            .collect();
        let sent = bufs.concat();
        let reading = ruio_executor::spawn(async move {
            let buf = Vec::with_capacity(BUFFERS * LEN).slice(..BUFFERS * LEN);
            let (read, buf) = server.read_exact(buf).await;
            read.map(|()| buf.into_inner())
        });
        let (written, bufs) = client.write_all_vectored(bufs).await;
        written?;
        assert_eq!(bufs.len(), BUFFERS);
        assert!(reading.await.unwrap()? == sent, "what is read was written");
        Ok(())
    })
}

#[test]
fn a_stream_lingers_for_as_long_as_it_is_told() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let (_listener, client, server) = pair().await?;
        assert_eq!(client.linger()?, None);
        client.set_linger(Some(Duration::from_secs(3)))?;
        assert_eq!(client.linger()?, Some(Duration::from_secs(3)));
        client.set_linger(None)?;
        assert_eq!(client.linger()?, None);

        // A stream that lingers for no time resets its connection as it
        // closes.
        client.set_linger(Some(Duration::ZERO))?;
        assert_eq!(client.linger()?, Some(Duration::ZERO));
        drop(client);
        let (read, _) = server.read(Vec::with_capacity(8)).await;
        let err = read.expect_err("a read of a connection that was reset");
        assert_eq!(err.kind(), io::ErrorKind::ConnectionReset, "{err}");
        Ok(())
    })
}

#[test]
fn a_listener_binds_only_where_a_runtime_runs() {
    let err = TcpListener::bind(loopback()).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Other);
}
