//! TCP sockets inside the runtime.

mod support;

use std::io::{self, ErrorKind, Write as _};
use std::net::Shutdown;
use std::sync::{Barrier, Mutex, OnceLock, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use ruio::buf::BufPool;
use ruio::net::{TcpListener, TcpStream};
use ruio::sync::oneshot;
use ruio::uring::ops::Nop;

use support::within;

/// A connected pair of streams on the loopback interface: the client's and
/// the server's.
async fn pair() -> io::Result<(TcpStream, TcpStream)> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let client = ruio::spawn(TcpStream::connect(addr));
    let (server, _) = within(listener.accept()).await?;
    Ok((client.await.unwrap()?, server))
}

/// Echoes every connection `listener` accepts, through a pool, writing
/// behind.
fn serve_echo(listener: TcpListener) -> ruio::JoinHandle<()> {
    ruio::spawn(async move {
        let pool = BufPool::new(&ruio::driver(), 16, 1024).unwrap();
        let mut incoming = listener.incoming();
        while let Some(stream) = incoming.next().await {
            let stream = stream.unwrap();
            let pool = pool.clone();
            let _detached = ruio::spawn(async move {
                let mut received = stream.recv_stream(&pool);
                while let Some(data) = received.next().await {
                    let mut data = data.unwrap();
                    while let Some(more) = received.next_ready() {
                        data.append(more.unwrap());
                    }
                    stream.write_behind(data).await.unwrap();
                }
                stream.flush().await.unwrap();
            });
        }
    })
}

fn pattern(len: usize, seed: usize) -> Vec<u8> {
    (0..len).map(|index| ((index + seed) % 251) as u8).collect()
}

#[ruio::test]
async fn a_server_echoes_what_its_clients_send() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let _server = serve_echo(listener);
    let clients: Vec<_> = (0..4)
        .map(|client| {
            ruio::spawn(async move {
                let stream = within(TcpStream::connect(addr)).await?;
                assert_eq!(stream.peer_addr().await?, addr);
                let message = pattern(5000, client);
                within(stream.write_all(message.clone())).await.0?;
                let echo = Vec::with_capacity(message.len());
                let (read, echo) = within(stream.read_exact(echo)).await;
                read?;
                assert!(echo == message);
                io::Result::Ok(())
            })
        })
        .collect();
    for client in clients {
        client.await.unwrap()?;
    }
    Ok(())
}

/// Messages larger than a buffer arrive in bundles, which go back out whole.
#[ruio::test]
async fn large_messages_echo_through_bundles() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let _server = serve_echo(listener);
    let stream = within(TcpStream::connect(addr)).await?;
    for round in 0..20 {
        let message = pattern(64 << 10, round);
        within(stream.write_all(message.clone())).await.0?;
        let (read, echo) = within(stream.read_exact(Vec::with_capacity(64 << 10))).await;
        read?;
        assert!(echo == message, "round {round}");
    }
    Ok(())
}

#[ruio::test]
async fn write_all_vectored_sends_more_than_the_socket_holds() -> io::Result<()> {
    let (client, server) = pair().await?;
    let bufs: Vec<Vec<u8>> = (0..3).map(|seed| pattern(8 << 20, seed)).collect();
    let expected = bufs.concat();
    let len = expected.len();
    let reader = ruio::spawn(async move {
        let (read, buf) = within(server.read_exact(Vec::with_capacity(len))).await;
        read.map(|()| buf)
    });
    let (written, bufs) = within(client.write_all_vectored(bufs)).await;
    written?;
    assert_eq!(bufs.len(), 3);
    assert!(reader.await.unwrap()? == expected);
    Ok(())
}

/// Writes behind go out in order, one at a time, whatever their sizes: a
/// large one is still in flight when the next is written, which waits.
#[ruio::test]
async fn writes_behind_arrive_in_order() -> io::Result<()> {
    let (client, server) = pair().await?;
    let messages: Vec<Vec<u8>> = (0..40)
        .map(|index| {
            pattern(
                if index % 8 == 0 {
                    4 << 20
                } else {
                    1 + index * 97
                },
                index,
            )
        })
        .collect();
    let expected = messages.concat();
    let len = expected.len();
    let reader = ruio::spawn(async move {
        let (read, received) = within(server.read_exact(Vec::with_capacity(len))).await;
        read.map(|()| received)
    });
    for message in messages {
        within(client.write_behind(vec![message])).await?;
    }
    within(client.flush()).await?;
    assert!(reader.await.unwrap()? == expected);
    Ok(())
}

/// While the thread waits on one peer at a time, a large write behind goes
/// as a chain of sends, which arrive in order, and the other writes wait
/// for the last of them.
#[ruio::test]
async fn a_large_write_behind_goes_as_a_chain_while_idle() -> io::Result<()> {
    let (client, server) = pair().await?;
    let bufs: Vec<Vec<u8>> = (0..10).map(|seed| pattern(20 << 10, seed)).collect();
    let expected = [bufs.concat(), b"tail".to_vec()].concat();
    let len = expected.len();
    let reader = ruio::spawn(async move {
        let (read, received) = within(server.read_exact(Vec::with_capacity(len))).await;
        read.map(|()| received)
    });
    // One completion in the last turn of the ring: the thread is idle.
    let driver = ruio::driver();
    driver.op(Nop).await?;
    assert_eq!(driver.completions_last_turn(), 1);
    let before = driver.stats().operations;
    within(client.write_behind(bufs)).await?;
    assert_eq!(driver.stats().operations - before, 4, "four sends");
    within(client.write_all(b"tail".to_vec())).await.0?;
    assert!(reader.await.unwrap()? == expected);
    Ok(())
}

/// A stream whose messages span several buffers learns how many, and,
/// while the thread waits on one peer at a time, asks the kernel for all
/// but the last buffer of a message, so that it hands over the first part
/// before the rest. The change of the ask submits the receive again, and
/// loses nothing.
#[ruio::test]
async fn a_receive_asks_for_the_buffers_its_messages_take() -> io::Result<()> {
    const ROUNDS: usize = 16;
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    // The peer is another thread, as a server's peers are other processes:
    // on this ring a message's completion then comes in a turn of its own,
    // as the thread waits for it, where a peer on the same ring would have
    // its send complete in the same turn.
    let (answered, answers) = mpsc::channel::<()>();
    let peer = thread::spawn(move || -> io::Result<()> {
        let mut stream = std::net::TcpStream::connect(addr)?;
        for round in 0..ROUNDS {
            stream.write_all(&pattern(4096, round))?;
            if answers.recv().is_err() {
                break;
            }
        }
        Ok(())
    });
    let (server, _) = within(listener.accept()).await?;
    let driver = ruio::driver();
    let pool = BufPool::new(&driver, 16, 1024)?;
    let mut received = server.recv_stream(&pool);
    let before = driver.stats().operations;
    let mut shapes = Vec::new();
    for round in 0..ROUNDS {
        let message = pattern(4096, round);
        let mut echo = Vec::new();
        let mut shape = Vec::new();
        while echo.len() < message.len() {
            // Awaited bare: a timeout's timer would be a completion too.
            let data = received.next().await.unwrap()?;
            shape.push(data.buffers());
            echo.extend(data.to_vec());
        }
        assert!(echo == message, "round {round}");
        shapes.push(shape);
        answered.send(()).unwrap();
    }
    peer.join().unwrap()?;
    // The receive, and the receive submitted again, with its cancellation,
    // once the turns delivering one completion each have shown the thread
    // idle.
    assert_eq!(driver.stats().operations - before, 1 + 2, "{shapes:?}");
    assert_eq!(shapes[0], [4], "the first message comes whole");
    // The kernel caps a receive at its ask (`IORING_RECV_MSHOT_CAP`), so
    // messages then come as three buffers and one.
    let (_, split) = shapes.split_at(8);
    assert!(split.iter().all(|shape| shape == &[3, 1]), "{shapes:?}");
    Ok(())
}

/// The other writes wait for a write behind in flight.
#[ruio::test]
async fn writes_wait_for_the_write_behind_in_flight() -> io::Result<()> {
    let (client, server) = pair().await?;
    let large = pattern(8 << 20, 1);
    let expected = [&large[..], b"tail"].concat();
    let len = expected.len();
    let reader = ruio::spawn(async move {
        let (read, received) = within(server.read_exact(Vec::with_capacity(len))).await;
        read.map(|()| received)
    });
    within(client.write_behind(vec![large])).await?;
    within(client.write_all(b"tail".to_vec())).await.0?;
    assert!(reader.await.unwrap()? == expected);
    Ok(())
}

/// A write behind that fails, here by timing out against a peer that does
/// not read, fails every write after it, handing buffers back unsent, and
/// the stream's shutdown.
#[ruio::test]
async fn a_failed_write_behind_fails_what_comes_after() -> io::Result<()> {
    let (client, _server) = pair().await?;
    let bufs = vec![vec![7; 64 << 20]];
    within(client.write_behind(bufs).timeout(Duration::from_millis(50))).await?;
    let err = within(client.flush()).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::TimedOut);

    let (written, buf) = within(client.write_all(b"after".to_vec())).await;
    assert_eq!(written.unwrap_err().kind(), ErrorKind::TimedOut);
    assert_eq!(buf, b"after");
    let (written, buf) = within(client.write(b"after".to_vec())).await;
    assert_eq!(written.unwrap_err().kind(), ErrorKind::TimedOut);
    assert_eq!(buf, b"after");
    let (sent, buf) = within(client.send_zc(b"after".to_vec())).await;
    assert_eq!(sent.unwrap_err().kind(), ErrorKind::TimedOut);
    assert_eq!(buf, b"after");
    let err = within(client.write_behind(vec![b"after".to_vec()]))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::TimedOut);
    let err = within(client.shutdown(Shutdown::Write)).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::TimedOut);
    Ok(())
}

/// Asked without waiting, a stream says nothing while a write behind is in
/// flight, and how the writes went once none is.
#[ruio::test]
async fn try_flush_reports_only_what_is_known() -> io::Result<()> {
    let (client, server) = pair().await?;
    assert!(matches!(client.try_flush(), Some(Ok(()))));
    within(client.write_behind(vec![vec![7; 64 << 20]])).await?;
    assert!(client.try_flush().is_none(), "the write is in flight");
    let (read, _) = within(server.read_exact(Vec::with_capacity(64 << 20))).await;
    read?;
    within(client.flush()).await?;
    assert!(matches!(client.try_flush(), Some(Ok(()))));

    let (client, _server) = pair().await?;
    within(
        client
            .write_behind(vec![vec![7; 64 << 20]])
            .timeout(Duration::from_millis(50)),
    )
    .await?;
    within(client.flush()).await.unwrap_err();
    let err = client
        .try_flush()
        .expect("nothing is in flight")
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::TimedOut);
    Ok(())
}

/// A peer that resets the connection part of the way through a write behind
/// cuts it short, which fails the stream's writes from then on.
#[ruio::test]
async fn a_write_behind_cut_short_by_the_peer_fails() -> io::Result<()> {
    let (client, server) = pair().await?;
    within(client.write_behind(vec![vec![7; 64 << 20]])).await?;
    let (read, _) = within(server.read_exact(Vec::with_capacity(1 << 20))).await;
    read?;
    // Closed with data unread, the socket resets the connection.
    drop(server);
    let err = within(client.flush()).await.unwrap_err();
    assert!(
        matches!(
            err.kind(),
            ErrorKind::BrokenPipe | ErrorKind::ConnectionReset
        ),
        "{err}"
    );
    let (written, _) = within(client.write_all(b"after".to_vec())).await;
    assert_eq!(written.unwrap_err().kind(), err.kind());
    Ok(())
}

/// A stream dropped just after a write behind, before the send has reached
/// the kernel, closes only after it: the data arrives, then the end of the
/// stream.
#[ruio::test]
async fn a_write_behind_arrives_though_the_stream_is_dropped() -> io::Result<()> {
    let (client, server) = pair().await?;
    within(client.write_behind(vec![b"last words".to_vec()])).await?;
    drop(client);
    let (read, received) = within(server.read_exact(Vec::with_capacity(10))).await;
    read?;
    assert_eq!(received, b"last words");
    let (read, _) = within(server.read(Vec::with_capacity(1))).await;
    assert_eq!(read?, 0, "the stream ends");
    Ok(())
}

/// More buffers than one `sendmsg` takes go out in several.
#[ruio::test]
async fn write_all_vectored_sends_more_buffers_than_one_send_takes() -> io::Result<()> {
    let (client, server) = pair().await?;
    let bufs: Vec<Vec<u8>> = (0..1500).map(|index| vec![(index % 251) as u8]).collect();
    let expected = bufs.concat();
    within(client.write_all_vectored(bufs)).await.0?;
    let (read, received) = within(server.read_exact(Vec::with_capacity(1500))).await;
    read?;
    assert_eq!(received, expected);
    Ok(())
}

/// Listeners bound with `SO_REUSEPORT` share an address, which a listener
/// without it cannot bind, and the kernel spreads the connections among
/// them.
#[ruio::test]
async fn listeners_bound_with_reuseport_share_an_address() -> io::Result<()> {
    let first = TcpListener::bind_reuseport("127.0.0.1:0")?;
    let addr = first.local_addr()?;
    let second = TcpListener::bind_reuseport(addr)?;
    assert_eq!(second.local_addr()?, addr);
    let err = TcpListener::bind(addr).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::AddrInUse, "{err}");

    // The kernel completes the handshakes before any accept.
    let clients = (0..32)
        .map(|_| std::net::TcpStream::connect(addr))
        .collect::<io::Result<Vec<_>>>()?;
    let mut accepted = [0; 2];
    for (count, listener) in accepted.iter_mut().zip([&first, &second]) {
        // Past the last connection waiting, an accept waits.
        let wait = Duration::from_millis(100);
        while let Ok((stream, _)) = listener.accept().timeout(wait).await {
            assert_eq!(stream.local_addr().await?, addr);
            *count += 1;
        }
    }
    assert_eq!(accepted.iter().sum::<usize>(), clients.len());
    assert!(accepted.iter().all(|&count| count > 0), "{accepted:?}");
    Ok(())
}

/// A server on several cores, with a listener on each bound to one address,
/// shares the connections out among its cores.
#[test]
fn a_server_on_several_cores_shares_out_the_connections() {
    const CLIENTS: usize = 32;
    let all = ruio::Cores::all().unwrap();
    // Four cores, some sharing a CPU on a machine with fewer.
    let cores = ruio::Cores::cpus(all.cpu_list().iter().copied().cycle().take(4));
    let count = cores.count();
    let addr = OnceLock::new();
    let (published, bound) = (Barrier::new(count), Barrier::new(count));
    // Core 0 runs the clients, and then lets the others go.
    let (dones, waits): (Vec<_>, Vec<_>) = (1..count).map(|_| oneshot::channel::<()>()).unzip();
    let dones = Mutex::new(Some(dones));
    let waits: Vec<_> = waits
        .into_iter()
        .map(|wait| Mutex::new(Some(wait)))
        .collect();
    let served = cores
        .run(|core| {
            let (addr, published, bound) = (&addr, &published, &bound);
            let (dones, waits) = (&dones, &waits);
            async move {
                let index = core.index();
                let listener = if index == 0 {
                    let listener = TcpListener::bind_reuseport("127.0.0.1:0").unwrap();
                    addr.set(listener.local_addr().unwrap()).unwrap();
                    published.wait();
                    listener
                } else {
                    published.wait();
                    TcpListener::bind_reuseport(addr.get().unwrap()).unwrap()
                };
                bound.wait();
                // Each connection is told which core accepted it.
                let _server = ruio::spawn(async move {
                    let mut incoming = listener.incoming();
                    while let Some(stream) = incoming.next().await {
                        let stream = stream.unwrap();
                        let _reply = ruio::spawn(async move {
                            stream.write_all(vec![index as u8]).await.0.unwrap();
                        });
                    }
                });
                if index != 0 {
                    let wait = waits[index - 1].lock().unwrap().take().unwrap();
                    wait.await.unwrap();
                    return Vec::new();
                }
                let mut served = Vec::new();
                for _ in 0..CLIENTS {
                    let stream = within(TcpStream::connect(addr.get().unwrap()))
                        .await
                        .unwrap();
                    let (read, reply) = within(stream.read_exact(Vec::with_capacity(1))).await;
                    read.unwrap();
                    served.push(usize::from(reply[0]));
                }
                for done in dones.lock().unwrap().take().unwrap() {
                    done.send(()).unwrap();
                }
                served
            }
        })
        .unwrap();
    let served = &served[0];
    assert_eq!(served.len(), CLIENTS);
    let mut serving = served.clone();
    serving.sort_unstable();
    serving.dedup();
    assert!(serving.len() > 1, "{served:?}");
    assert!(serving.iter().all(|&index| index < count));
}

/// A connection accepted on one core moves to another, which serves it: as
/// a plain descriptor, and as a direct one, from ring to ring.
#[test]
fn a_stream_moves_to_another_core() {
    let all = ruio::Cores::all().unwrap();
    // Two cores, sharing a CPU on a machine with one.
    let cores = ruio::Cores::cpus(all.cpu_list().iter().copied().cycle().take(2));
    let outputs = cores
        .run(|core| async move {
            // Core 1 serves what core 0 sends it, until the run ends.
            if core.index() != 0 {
                return Ok(());
            }
            for direct in [false, true] {
                let listener = TcpListener::bind("127.0.0.1:0")?;
                listener.set_direct(direct);
                let addr = listener.local_addr()?;
                let head = pattern(8 << 20, usize::from(direct));
                let expected = [&head[..], b"tail"].concat();
                let client = ruio::spawn(async move {
                    let client = within(TcpStream::connect(addr)).await?;
                    let received = Vec::with_capacity(expected.len());
                    let (read, received) = within(client.read_exact(received)).await;
                    read?;
                    assert!(received == expected, "out of order");
                    within(client.write_all("ping")).await.0?;
                    let (read, echo) = within(client.read_exact(Vec::with_capacity(4))).await;
                    read?;
                    assert_eq!(echo, b"ping");
                    io::Result::Ok(())
                });
                let (server, _) = within(listener.accept()).await?;
                // Larger than the socket holds, so still in flight as the
                // stream is sent: it goes out before what the other core
                // writes.
                within(server.write_behind(vec![head])).await?;
                let sent = within(server.send_to(1)).await.map_err(|(err, _)| err)?;
                let served = ruio::handle_of(1).spawn_with(move || async move {
                    let stream = sent.receive();
                    assert_eq!(stream.is_direct(), direct);
                    within(stream.write_all("tail")).await.0?;
                    let (read, buf) = within(stream.read_exact(Vec::with_capacity(4))).await;
                    read?;
                    within(stream.write_all(buf)).await.0?;
                    io::Result::Ok(ruio::core().unwrap().index())
                });
                client.await.unwrap()?;
                assert_eq!(within(served).await.unwrap()?, 1);
            }
            io::Result::Ok(())
        })
        .unwrap();
    for output in outputs {
        output.unwrap();
    }
}

#[ruio::test]
async fn accept_reports_the_peer() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let client = ruio::spawn(TcpStream::connect(addr));
    let (server, peer) = within(listener.accept()).await?;
    let client = client.await.unwrap()?;
    assert_eq!(peer, client.local_addr().await?);
    assert_eq!(server.peer_addr().await?, peer);
    assert_eq!(server.local_addr().await?, addr);
    assert!(!server.is_direct());
    assert!(!within(server.nodelay()).await?);
    within(server.set_nodelay(true)).await?;
    assert!(within(server.nodelay()).await?);
    assert_eq!(within(server.ttl()).await?, 64);
    within(server.set_ttl(100)).await?;
    assert_eq!(within(server.ttl()).await?, 100);
    assert_eq!(within(server.linger()).await?, None);
    within(server.set_linger(Some(Duration::from_secs(3)))).await?;
    assert_eq!(within(server.linger()).await?, Some(Duration::from_secs(3)));
    within(server.set_linger(None)).await?;
    assert_eq!(within(server.linger()).await?, None);
    assert!(within(server.take_error()).await?.is_none());
    Ok(())
}

#[ruio::test]
async fn connecting_to_a_closed_port_is_refused() {
    let addr = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let err = within(TcpStream::connect(addr)).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ConnectionRefused);
}

#[ruio::test]
async fn a_read_that_times_out_hands_back_its_buffer() -> io::Result<()> {
    let (client, server) = pair().await?;
    let start = Instant::now();
    let read = server.read(Vec::with_capacity(16));
    let (read, buf) = read.timeout(Duration::from_millis(20)).await;
    assert_eq!(read.unwrap_err().kind(), ErrorKind::TimedOut);
    assert!(start.elapsed() >= Duration::from_millis(20));
    assert_eq!(buf.capacity(), 16);

    // The stream carries on, and the buffer with it.
    within(client.write_all("later")).await.0?;
    let (read, buf) = within(server.read(buf)).await;
    assert_eq!(read?, 5);
    assert_eq!(buf, b"later");
    Ok(())
}

#[ruio::test]
async fn write_all_sends_more_than_the_socket_holds() -> io::Result<()> {
    let (client, server) = pair().await?;
    let data = pattern(16 << 20, 0);
    let len = data.len();
    let reader = ruio::spawn(async move {
        let (read, buf) = within(server.read_exact(Vec::with_capacity(len))).await;
        read.map(|()| buf)
    });
    let (written, data) = within(client.write_all(data)).await;
    written?;
    assert!(reader.await.unwrap()? == data);
    Ok(())
}

#[ruio::test]
async fn read_exact_fails_if_the_peer_shuts_down_first() -> io::Result<()> {
    let (client, server) = pair().await?;
    within(client.write_all("abc")).await.0?;
    within(client.shutdown(Shutdown::Write)).await?;
    let (read, buf) = within(server.read_exact(Vec::with_capacity(8))).await;
    assert_eq!(read.unwrap_err().kind(), ErrorKind::UnexpectedEof);
    assert_eq!(buf, b"abc");
    Ok(())
}

/// The first send stops part of the way when the time is up, and the next
/// one has no time left, which is what reports the timeout: the timeout
/// bounds the whole write, not each send.
#[ruio::test]
async fn a_write_to_a_peer_that_stops_reading_times_out() -> io::Result<()> {
    let (client, _server) = pair().await?;
    let timeout = Duration::from_millis(300);
    let start = Instant::now();
    let write = client.write_all(vec![7; 64 << 20]);
    let (written, data) = within(write.timeout(timeout)).await;
    assert_eq!(written.unwrap_err().kind(), ErrorKind::TimedOut);
    let elapsed = start.elapsed();
    assert!(elapsed >= timeout);
    assert!(
        elapsed < timeout + Duration::from_millis(250),
        "{elapsed:?}"
    );
    assert_eq!(data.len(), 64 << 20);
    Ok(())
}

/// A direct stream whose ring is gone reaches nothing, not even the file in
/// the same slot of the next ring.
#[test]
fn a_direct_stream_outliving_its_ring_reaches_nothing() {
    let direct_pair = || async {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_direct(true);
        let client = ruio::spawn(TcpStream::connect(listener.local_addr()?));
        let (server, _) = within(listener.accept()).await?;
        io::Result::Ok((server, client.await.unwrap()?))
    };
    let first = ruio::Runtime::new().unwrap();
    let (stale, _client) = first.block_on(direct_pair()).unwrap();
    assert_eq!(stale.fd(), ruio::uring::Fd::Direct(0));
    drop(first);

    let second = ruio::Runtime::new().unwrap();
    second
        .block_on(async {
            let (server, client) = direct_pair().await?;
            assert_eq!(server.fd(), stale.fd());
            within(client.write_all("not for the stale stream"))
                .await
                .0?;
            let (read, _) = within(stale.read(Vec::with_capacity(32))).await;
            assert_eq!(read.unwrap_err().raw_os_error(), Some(libc::EBADF));
            io::Result::Ok(())
        })
        .unwrap();
}

/// More than the socket buffers hold, so that the send has to carry on as
/// the peer reads.
#[ruio::test]
async fn a_stream_sends_zero_copy() -> io::Result<()> {
    let (client, server) = pair().await?;
    let len = 16 << 20;
    let reader = ruio::spawn(async move {
        let (read, buf) = within(server.read_exact(Vec::with_capacity(len))).await;
        read.map(|()| buf)
    });
    let (sent, data) = within(client.send_zc(pattern(len, 3))).await;
    assert_eq!(sent?, len);
    assert!(reader.await.unwrap()? == data);
    Ok(())
}

/// A direct stream's options are set and read as a plain one's are, through
/// the ring; its addresses, which only a file descriptor can ask for, come
/// through one installed for the call.
#[ruio::test]
async fn a_listener_accepts_direct_streams_on_request() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    listener.set_direct(true);
    let addr = listener.local_addr()?;
    let client = ruio::spawn(TcpStream::connect(addr));
    let (server, peer) = within(listener.accept()).await?;
    let client = client.await.unwrap()?;
    assert!(server.is_direct() && !client.is_direct());
    assert_eq!(server.peer_addr().await?, peer);
    assert_eq!(within(server.local_addr()).await?, addr);

    assert!(!within(server.nodelay()).await?);
    within(server.set_nodelay(true)).await?;
    assert!(within(server.nodelay()).await?);
    within(server.set_ttl(77)).await?;
    assert_eq!(within(server.ttl()).await?, 77);
    within(server.set_linger(Some(Duration::from_secs(2)))).await?;
    assert_eq!(within(server.linger()).await?, Some(Duration::from_secs(2)));
    assert!(within(server.take_error()).await?.is_none());

    within(client.write_all("ping")).await.0?;
    let (read, buf) = within(server.read_exact(Vec::with_capacity(4))).await;
    read?;
    within(server.write_all(buf)).await.0?;
    let (read, buf) = within(client.read_exact(Vec::with_capacity(4))).await;
    read?;
    assert_eq!(buf, b"ping");

    // A multishot accept learns no peer's address; the stream asks for it.
    let mut incoming = listener.incoming();
    let client = ruio::spawn(TcpStream::connect(addr));
    let server = within(incoming.next()).await.unwrap()?;
    let client = client.await.unwrap()?;
    assert!(server.is_direct());
    assert_eq!(
        within(server.peer_addr()).await?,
        client.local_addr().await?
    );
    Ok(())
}

/// A direct stream becomes a plain one through `into_plain`: the same
/// connection, with a file descriptor of the process, and the writes behind
/// it carried over.
#[ruio::test]
async fn a_direct_stream_becomes_a_plain_one() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    listener.set_direct(true);
    let addr = listener.local_addr()?;
    let client = ruio::spawn(TcpStream::connect(addr));
    let (server, peer) = within(listener.accept()).await?;
    let client = client.await.unwrap()?;
    assert!(server.is_direct());
    within(server.set_nodelay(true)).await?;
    within(server.write_behind(vec![pattern(100_000, 7)])).await?;

    let server = within(server.into_plain()).await?;
    assert!(!server.is_direct());
    assert_eq!(server.peer_addr().await?, peer);
    assert_eq!(server.local_addr().await?, addr);
    assert!(within(server.nodelay()).await?, "the option stays set");
    within(server.write_all("tail")).await.0?;
    let (read, buf) = within(client.read_exact(Vec::with_capacity(100_004))).await;
    read?;
    assert_eq!(&buf[..100_000], &pattern(100_000, 7)[..]);
    assert_eq!(&buf[100_000..], b"tail");

    // A plain stream is given back as it is.
    let client = within(client.into_plain()).await?;
    assert!(!client.is_direct());
    within(client.write_all("again")).await.0?;
    let (read, buf) = within(server.read_exact(Vec::with_capacity(5))).await;
    read?;
    assert_eq!(buf, b"again");
    Ok(())
}

#[ruio::test]
async fn dropping_a_stream_closes_the_connection() -> io::Result<()> {
    for direct in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_direct(direct);
        let client = ruio::spawn(TcpStream::connect(listener.local_addr()?));
        let (server, _) = within(listener.accept()).await?;
        let client = client.await.unwrap()?;
        drop(server);
        let (read, _) = within(client.read(Vec::with_capacity(4))).await;
        assert_eq!(read?, 0, "a direct stream: {direct}");
    }
    Ok(())
}

/// Once the file table is full, the kernel closes the connection it had no
/// slot for, and the listener carries on with plain descriptors.
#[test]
fn incoming_carries_on_with_plain_streams_once_the_table_is_full() {
    let runtime = ruio::Runtime::builder().files(1).build().unwrap();
    runtime
        .block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0")?;
            listener.set_direct(true);
            let addr = listener.local_addr()?;
            let mut incoming = listener.incoming();
            let _first_client = ruio::spawn(TcpStream::connect(addr));
            let first = within(incoming.next()).await.unwrap()?;
            assert!(first.is_direct());

            let second = ruio::spawn(async move {
                let stream = TcpStream::connect(addr).await?;
                let (read, _) = within(stream.read(Vec::with_capacity(4))).await;
                read
            });
            let full = within(incoming.next()).await.unwrap().unwrap_err();
            assert_eq!(full.raw_os_error(), Some(libc::ENFILE));
            assert_eq!(second.await.unwrap()?, 0, "the connection was closed");

            let _third_client = ruio::spawn(TcpStream::connect(addr));
            let third = within(incoming.next()).await.unwrap()?;
            assert!(!third.is_direct());
            io::Result::Ok(())
        })
        .unwrap();
}

/// A connection's data arrives whole through a stream whose own pool the
/// kernel consumes incrementally, many small writes to a buffer.
#[ruio::test]
async fn an_incremental_stream_receives_everything() -> io::Result<()> {
    if !ruio::uring::Probe::current()?.supports_incremental_buffers() {
        eprintln!("skipped: the kernel does not consume buffers incrementally");
        return Ok(());
    }
    let (client, server) = within(pair()).await?;
    let sent: Vec<u8> = (0..50_000).map(|index| (index % 251) as u8).collect();
    let writer = ruio::spawn({
        let sent = sent.clone();
        async move {
            for chunk in sent.chunks(333) {
                client.write_all(chunk.to_vec()).await.0?;
            }
            client.shutdown(Shutdown::Write).await
        }
    });
    let mut received = Vec::new();
    let mut stream = server.recv_stream_incremental(4, 16 << 10)?;
    while let Some(data) = within(stream.next()).await {
        received.extend(data?.to_vec());
    }
    within(writer).await.unwrap()?;
    assert_eq!(received, sent);
    Ok(())
}
