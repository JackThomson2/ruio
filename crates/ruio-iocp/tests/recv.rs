//! Streams received into pools of buffers: what a peer writes arrives in
//! order, in leases that return their buffers, and a stream that waits for
//! data holds no buffer meanwhile.
#![cfg(windows)]

use std::io::{self, Read as _, Write as _};
use std::net::{Ipv4Addr, Shutdown, SocketAddr};
use std::thread;
use std::time::Duration;

use ruio_executor::{Runtime, time};
use ruio_iocp::buf::BufPool;
use ruio_iocp::net::{TcpListener, TcpStream};

/// The loopback address, and a port for the system to choose.
fn loopback() -> SocketAddr {
    (Ipv4Addr::LOCALHOST, 0).into()
}

/// A listener, a stream connected to it, and the stream it accepted.
async fn pair() -> io::Result<(TcpListener, TcpStream, TcpStream)> {
    let listener = TcpListener::bind(loopback())?;
    let client = TcpStream::connect(listener.local_addr()?).await?;
    let (server, _) = listener.accept().await?;
    Ok((listener, client, server))
}

/// `len` bytes that no part of which repeats the part before it.
fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|at| (at % 251) as u8).collect()
}

#[test]
fn what_a_peer_writes_is_received_into_a_pool_in_order() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let (_listener, client, server) = pair().await?;
        let pool = BufPool::new(4, 1024)?;
        let sent = pattern(64 * 1024 + 123);
        let writer = ruio_executor::spawn({
            let sent = sent.clone();
            async move {
                let (written, _) = client.write_all(sent).await;
                written?;
                client.shutdown(Shutdown::Write)?;
                // Kept open until the other end has read it all.
                io::Result::Ok(client)
            }
        });
        let mut arriving = server.recv_stream(&pool);
        let mut received = Vec::new();
        while let Some(data) = arriving.next().await {
            let data = data?;
            assert!(!data.is_empty());
            assert!(data.len() <= pool.buffer_size());
            assert_eq!(data.buffers(), 1);
            received.extend_from_slice(&data.to_vec());
        }
        assert!(arriving.is_ended());
        assert!(arriving.next().await.is_none());
        assert!(arriving.next_ready().is_none());
        assert!(received == sent, "what arrived is what was written");
        assert_eq!(pool.available(), 4);
        let _client = writer.await.unwrap()?;
        Ok(())
    })
}

#[test]
fn a_stream_ends_when_a_peer_of_the_standard_librarys_shuts_down() -> io::Result<()> {
    let listener = std::net::TcpListener::bind(loopback())?;
    let addr = listener.local_addr()?;
    let sent = pattern(100_000);
    let peer = thread::spawn({
        let sent = sent.clone();
        move || -> io::Result<usize> {
            let (mut stream, _) = listener.accept()?;
            stream.write_all(&sent)?;
            stream.shutdown(Shutdown::Write)?;
            // Until the other end closes, having received it all.
            stream.read_to_end(&mut Vec::new())
        }
    });
    Runtime::new()?.block_on(async {
        let stream = TcpStream::connect(addr).await?;
        let pool = BufPool::new(2, 4096)?;
        let mut arriving = stream.recv_stream(&pool);
        let mut received = Vec::new();
        while let Some(data) = arriving.next().await {
            received.extend_from_slice(&data?.to_vec());
        }
        assert!(received == sent, "what arrived is what was written");
        io::Result::Ok(())
    })?;
    assert_eq!(peer.join().unwrap()?, 0);
    Ok(())
}

#[test]
fn streams_that_wait_for_data_hold_no_buffer() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let (_first_listener, first_client, first_server) = pair().await?;
        let (_second_listener, second_client, second_server) = pair().await?;
        let pool = BufPool::new(1, 64)?;
        let mut first = first_server.recv_stream(&pool);
        let mut second = second_server.recv_stream(&pool);
        // Nothing is written, so each waits until its timeout.
        let waited = time::timeout(Duration::from_millis(20), first.next()).await;
        assert!(waited.is_err());
        let waited = time::timeout(Duration::from_millis(20), second.next()).await;
        assert!(waited.is_err());
        assert_eq!(pool.available(), 1);

        let (written, _) = first_client.write(b"one".to_vec()).await;
        assert_eq!(written?, 3);
        let (written, _) = second_client.write(b"two".to_vec()).await;
        assert_eq!(written?, 3);
        let one = first.next().await.expect("the first stream's data")?;
        assert_eq!(one.to_vec(), b"one");
        assert_eq!(pool.available(), 0);
        // The second has data, and waits for the one buffer.
        let waited = time::timeout(Duration::from_millis(50), second.next()).await;
        assert!(waited.is_err());
        drop(one);
        let two = second.next().await.expect("the second stream's data")?;
        assert_eq!(two.to_vec(), b"two");
        Ok(())
    })
}

#[test]
fn readers_share_a_pool_of_fewer_buffers_than_there_are_of_them() -> io::Result<()> {
    const READERS: u8 = 4;
    const LEN: usize = 4000;
    Runtime::new()?.block_on(async {
        let pool = BufPool::new(1, 256)?;
        let mut readers = Vec::new();
        for reader in 0..READERS {
            let (listener, client, server) = pair().await?;
            let pool = pool.clone();
            readers.push(ruio_executor::spawn(async move {
                let writer = ruio_executor::spawn(async move {
                    let (written, _) = client.write_all(vec![reader; LEN]).await;
                    written?;
                    client.shutdown(Shutdown::Write)?;
                    io::Result::Ok(client)
                });
                let mut arriving = server.recv_stream(&pool);
                let mut received = Vec::new();
                while let Some(data) = arriving.next().await {
                    let data = data?;
                    // Held through a turn of the runtime, in which the
                    // others find the pool empty.
                    ruio_executor::yield_now().await;
                    received.extend_from_slice(&data.to_vec());
                }
                drop(arriving);
                drop(writer.await.unwrap()?);
                drop(listener);
                io::Result::Ok(received)
            }));
        }
        for (reader, received) in (0..READERS).zip(readers) {
            let received = received.await.unwrap()?;
            assert!(received == vec![reader; LEN], "reader {reader}'s data");
        }
        assert_eq!(pool.available(), 1);
        Ok(())
    })
}

#[test]
fn data_that_has_arrived_is_taken_without_waiting() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let (_listener, client, server) = pair().await?;
        let pool = BufPool::new(8, 16)?;
        let mut arriving = server.recv_stream(&pool);
        // Nothing has arrived, and nothing is waited for.
        assert!(arriving.next_ready().is_none());
        let sent = pattern(40);
        let (written, _) = client.write_all(sent.clone()).await;
        written?;
        let mut data = arriving.next().await.expect("the data")?;
        // What a receive that filled its buffer left behind is taken at
        // once; the rest, if it came apart on the way, as it arrives.
        while data.len() < sent.len() {
            let more = match arriving.next_ready() {
                Some(more) => more?,
                None => arriving.next().await.expect("the rest of the data")?,
            };
            data.append(more);
        }
        assert_eq!(data.to_vec(), sent);
        assert!(data.buffers() >= 3);
        assert_eq!(pool.available(), 8 - data.buffers());
        assert!(arriving.next_ready().is_none());
        assert!(!arriving.is_ended());
        Ok(())
    })
}

#[test]
fn a_stream_dropped_while_it_waits_leaves_the_socket_to_other_reads() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let (_listener, client, server) = pair().await?;
        let pool = BufPool::new(1, 64)?;
        let mut arriving = server.recv_stream(&pool);
        // Nothing is written, so the stream waits until the timeout.
        let waited = time::timeout(Duration::from_millis(20), arriving.next()).await;
        assert!(waited.is_err());
        drop(arriving);

        let (written, _) = client.write(b"after".to_vec()).await;
        assert_eq!(written?, 5);
        let (read, buf) = server.read(Vec::with_capacity(16)).await;
        assert_eq!(read?, 5);
        assert_eq!(buf, b"after");

        // Another stream of the socket receives what comes next.
        let (written, _) = client.write(b"again".to_vec()).await;
        assert_eq!(written?, 5);
        let mut arriving = server.recv_stream(&pool);
        let data = arriving.next().await.expect("the data")?;
        assert_eq!(data.to_vec(), b"again");
        Ok(())
    })
}

#[test]
fn what_is_received_into_a_pool_is_written_back_as_it_is() -> io::Result<()> {
    const LEN: usize = 200_000;
    Runtime::new()?.block_on(async {
        let (_listener, client, server) = pair().await?;
        let pool = BufPool::new(16, 4096)?;
        let echo = ruio_executor::spawn(async move {
            let mut arriving = server.recv_stream(&pool);
            let mut echoed = 0;
            while let Some(data) = arriving.next().await {
                let mut data = data?;
                // What has arrived by now goes out with it, up to half of
                // the pool.
                while data.buffers() < 8 {
                    match arriving.next_ready() {
                        Some(more) => data.append(more?),
                        None => break,
                    }
                }
                echoed += data.len();
                let (written, data) = server.write_all_vectored(data).await;
                written?;
                drop(data);
            }
            drop(arriving);
            server.shutdown(Shutdown::Write)?;
            io::Result::Ok((echoed, pool.available(), server))
        });
        let client = std::rc::Rc::new(client);
        let sent = pattern(LEN);
        // The echo is read while the data is written, as neither end's
        // buffers hold all of it.
        let reading = ruio_executor::spawn({
            let client = std::rc::Rc::clone(&client);
            async move {
                let mut back = Vec::with_capacity(LEN);
                loop {
                    let (read, buf) = client.read(Vec::with_capacity(64 * 1024)).await;
                    if read? == 0 {
                        return io::Result::Ok(back);
                    }
                    back.extend_from_slice(&buf);
                }
            }
        });
        let (written, sent) = client.write_all(sent).await;
        written?;
        client.shutdown(Shutdown::Write)?;
        assert!(reading.await.unwrap()? == sent, "the echo is the data");
        let (echoed, available, _server) = echo.await.unwrap()?;
        assert_eq!(echoed, LEN);
        assert_eq!(available, 16);
        Ok(())
    })
}

#[test]
fn a_stream_shut_down_for_reading_ends() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let (_listener, _client, server) = pair().await?;
        let pool = BufPool::new(1, 64)?;
        server.shutdown(Shutdown::Read)?;
        let mut arriving = server.recv_stream(&pool);
        assert!(arriving.next().await.is_none());
        assert!(arriving.is_ended());
        assert_eq!(pool.available(), 1);
        Ok(())
    })
}
