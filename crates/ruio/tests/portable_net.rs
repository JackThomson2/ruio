//! The portable sockets: `Send` and `Sync` streams and listeners, read and
//! written with system calls made at once on whichever thread polls them.

mod support;

use std::future::{Future, poll_fn};
use std::io::{self, ErrorKind, IoSlice, Read as _, Write as _};
use std::mem::MaybeUninit;
use std::net::{Shutdown, SocketAddr};
use std::os::fd::AsRawFd;
use std::os::linux::net::SocketAddrExt;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use ruio::net::portable::{TcpListener, TcpStream, UnixListener, UnixStream};

use support::within;

fn assert_send_sync<T: Send + Sync>(_: &T) {}

/// Two cores, on two CPUs if the machine has them.
fn two() -> ruio::Cores {
    let all = ruio::Cores::all().unwrap();
    ruio::Cores::cpus(all.cpu_list().iter().copied().cycle().take(2))
}

/// A connected pair: the client's end, and the end the listener accepted.
async fn tcp_pair() -> io::Result<(TcpStream, TcpStream)> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let (client, accepted) = within(async {
        let connect = TcpStream::connect(addr);
        let accept = listener.accept();
        assert_send_sync(&connect);
        assert_send_sync(&accept);
        let client = connect.await?;
        let (accepted, peer) = accept.await?;
        assert_eq!(peer, client.local_addr()?);
        io::Result::Ok((client, accepted))
    })
    .await?;
    Ok((client, accepted))
}

/// Polls `future` once, where it lies, and says whether it is pending.
async fn pending_once<F: Future + Unpin>(future: &mut F) -> bool {
    poll_fn(|cx| Poll::Ready(Pin::new(&mut *future).poll(cx).is_pending())).await
}

#[test]
fn the_sockets_are_send_and_sync() {
    fn assert_types<T: Send + Sync + Unpin>() {}
    assert_types::<TcpStream>();
    assert_types::<TcpListener>();
    assert_types::<UnixStream>();
    assert_types::<UnixListener>();
}

#[ruio::test]
async fn a_connection_carries_data_both_ways_and_its_addresses() -> io::Result<()> {
    let (client, server) = tcp_pair().await?;
    assert_eq!(client.peer_addr()?, server.local_addr()?);
    assert_eq!(server.peer_addr()?, client.local_addr()?);
    let write = client.write_all("ping");
    assert_send_sync(&write);
    write.await.0?;
    let read = server.read_exact(Vec::with_capacity(4));
    assert_send_sync(&read);
    let (read, buf) = within(read).await;
    read?;
    assert_eq!(buf, b"ping");
    let (written, _) = server.write(b"pong".to_vec()).await;
    assert_eq!(written?, 4);
    let (read, buf) = within(client.read(Vec::with_capacity(16))).await;
    assert_eq!((read?, &buf[..]), (4, &b"pong"[..]));
    assert!(client.take_error()?.is_none());
    Ok(())
}

#[ruio::test]
async fn socket_options_are_set_and_read_without_the_ring() -> io::Result<()> {
    let (client, _server) = tcp_pair().await?;
    client.set_nodelay(true)?;
    assert!(client.nodelay()?);
    client.set_nodelay(false)?;
    assert!(!client.nodelay()?);
    client.set_ttl(42)?;
    assert_eq!(client.ttl()?, 42);
    assert_eq!(client.linger()?, None);
    client.set_linger(Some(Duration::from_secs(3)))?;
    assert_eq!(client.linger()?, Some(Duration::from_secs(3)));
    Ok(())
}

/// What a future that must be `Send`, as `async_trait` makes one, holds
/// across its awaits: the stream, and writes of a `&'static str`.
#[ruio::test]
async fn a_future_that_must_be_send_holds_a_stream() -> io::Result<()> {
    let (client, server) = tcp_pair().await?;
    let exchange: Pin<Box<dyn Future<Output = io::Result<Vec<u8>>> + Send + '_>> =
        Box::pin(async {
            client.write_all("hello").await.0?;
            client.readable().await?;
            let (read, reply) = client.read_exact(Vec::with_capacity(5)).await;
            read.map(|()| reply)
        });
    let echo = async {
        let (read, buf) = server.read_exact(Vec::with_capacity(5)).await;
        read?;
        server.write_all(buf).await.0
    };
    let (reply, echoed) = within(async { futures_join(exchange, echo).await }).await;
    echoed?;
    assert_eq!(reply?, b"hello");
    Ok(())
}

/// Runs two futures at once, to both their ends.
async fn futures_join<A: Future, B: Future>(a: A, b: B) -> (A::Output, B::Output) {
    let (mut a, mut b) = (Box::pin(a), Box::pin(b));
    let (mut a_out, mut b_out) = (None, None);
    poll_fn(|cx| {
        if a_out.is_none()
            && let Poll::Ready(out) = a.as_mut().poll(cx)
        {
            a_out = Some(out);
        }
        if b_out.is_none()
            && let Poll::Ready(out) = b.as_mut().poll(cx)
        {
            b_out = Some(out);
        }
        if a_out.is_some() && b_out.is_some() {
            Poll::Ready((a_out.take().unwrap(), b_out.take().unwrap()))
        } else {
            Poll::Pending
        }
    })
    .await
}

/// The poll methods read into and write from the caller's own buffers.
#[ruio::test]
async fn the_poll_methods_read_into_and_write_from_borrowed_buffers() -> io::Result<()> {
    let (mut client, mut server) = tcp_pair().await?;
    let written = poll_fn(|cx| client.poll_write(cx, b"borrowed ")).await?;
    assert_eq!(written, 9);
    let bufs = [IoSlice::new(b"and "), IoSlice::new(b"vectored")];
    let written = poll_fn(|cx| client.poll_write_vectored(cx, &bufs)).await?;
    assert_eq!(written, 12);
    let mut received = Vec::new();
    while received.len() < 21 {
        let mut buf = [MaybeUninit::<u8>::uninit(); 64];
        let read = within(poll_fn(|cx| server.poll_read(cx, &mut buf))).await?;
        assert!(read > 0);
        // SAFETY: the read initialized the first `read` bytes.
        received.extend(buf[..read].iter().map(|byte| unsafe { byte.assume_init() }));
    }
    assert_eq!(received, b"borrowed and vectored");
    Ok(())
}

/// A read with nothing to take waits, and its wait is kept from one poll to
/// the next; `try_read` finds nothing at once, and `readable` waits.
#[ruio::test]
async fn reads_wait_for_data_and_try_read_does_not() -> io::Result<()> {
    let (client, mut server) = tcp_pair().await?;
    let mut buf = [0u8; 8];
    assert_eq!(
        server.try_read(&mut buf).unwrap_err().kind(),
        ErrorKind::WouldBlock
    );
    let mut uninit = [MaybeUninit::<u8>::uninit(); 8];
    let pending = poll_fn(|cx| Poll::Ready(server.poll_read(cx, &mut uninit).is_pending())).await;
    assert!(pending);
    client.write_all("x").await.0?;
    let read = within(poll_fn(|cx| server.poll_read(cx, &mut uninit))).await?;
    assert_eq!(read, 1);
    client.write_all("y").await.0?;
    within(server.readable()).await?;
    assert_eq!(server.try_read(&mut buf)?, 1);
    within(client.writable()).await?;
    assert_eq!(client.try_write(b"z")?, 1);
    Ok(())
}

/// A write that finds no room waits for the peer to read, and a megabyte
/// goes through while another task reads it; owned vectors go whole.
#[ruio::test]
async fn writes_wait_for_room_while_the_peer_reads() -> io::Result<()> {
    let (client, server) = tcp_pair().await?;
    let data: Vec<u8> = (0..1 << 20).map(|index: u32| (index % 251) as u8).collect();
    let expected = data.clone();
    let reader = ruio::spawn(async move {
        let (read, buf) = server.read_exact(Vec::with_capacity(expected.len())).await;
        read.map(|()| buf == expected)
    });
    let (written, _) = within(client.write_all(data)).await;
    written?;
    assert!(within(reader).await.unwrap()?);
    let (client, server) = tcp_pair().await?;
    let bufs: Vec<Vec<u8>> = (0..100u8).map(|index| vec![index; 1000]).collect();
    let expected = bufs.concat();
    let write = client.write_all_vectored(bufs);
    assert_send_sync(&write);
    within(write).await.0?;
    let (read, buf) = within(server.read_exact(Vec::with_capacity(expected.len()))).await;
    read?;
    assert!(buf == expected);
    Ok(())
}

/// Shutting the writing half down shows the peer the end, and writing to a
/// peer that has gone fails rather than raising `SIGPIPE`.
#[ruio::test]
async fn shutdown_shows_the_end_and_a_peer_gone_fails_writes() -> io::Result<()> {
    let (client, server) = tcp_pair().await?;
    client.shutdown(Shutdown::Write)?;
    let (read, _) = within(server.read(Vec::with_capacity(8))).await;
    assert_eq!(read?, 0);
    drop(server);
    // The first write may go before the reset comes back.
    let mut failed = None;
    for _ in 0..100 {
        let (written, _) = within(client.write("after")).await;
        if let Err(err) = written {
            failed = Some(err);
            break;
        }
        ruio::time::sleep(Duration::from_millis(1)).await;
    }
    let kind = failed.expect("a write to a peer gone fails").kind();
    assert!(
        matches!(kind, ErrorKind::BrokenPipe | ErrorKind::ConnectionReset),
        "{kind:?}"
    );
    Ok(())
}

#[ruio::test]
async fn connecting_resolves_names_and_reports_refusals() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    let client = within(TcpStream::connect(("localhost", port))).await?;
    let (_, peer) = within(listener.accept()).await?;
    assert_eq!(peer, client.local_addr()?);
    drop(listener);
    let refused = within(TcpStream::connect(SocketAddr::from(([127, 0, 0, 1], port)))).await;
    assert_eq!(refused.unwrap_err().kind(), ErrorKind::ConnectionRefused);
    Ok(())
}

/// The standard library's sockets come in made nonblocking, and go out
/// blocking again.
#[ruio::test]
async fn standard_sockets_come_in_and_go_out() -> io::Result<()> {
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let addr = std_listener.local_addr()?;
    let listener = TcpListener::from_std(std_listener)?;
    let client = TcpStream::from_std(std::net::TcpStream::connect(addr)?)?;
    let (server, _) = within(listener.accept()).await?;
    let mut server = server.into_std();
    server.write_all(b"blocking")?;
    let (read, buf) = within(client.read_exact(Vec::with_capacity(8))).await;
    read?;
    assert_eq!(buf, b"blocking");
    client.write_all("back").await.0?;
    let mut back = [0; 4];
    server.read_exact(&mut back)?;
    assert_eq!(&back, b"back");
    let listener = listener.into_std();
    assert_eq!(listener.local_addr()?, addr);
    Ok(())
}

#[ruio::test]
async fn listeners_poll_for_connections() -> io::Result<()> {
    let mut listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let pending = poll_fn(|cx| Poll::Ready(listener.poll_accept(cx).is_pending())).await;
    assert!(pending, "no connection yet");
    let clients: Vec<_> = (0..8)
        .map(|_| std::net::TcpStream::connect(addr))
        .collect::<io::Result<_>>()?;
    for _ in &clients {
        let (_, peer) = within(poll_fn(|cx| listener.poll_accept(cx))).await?;
        assert!(
            clients
                .iter()
                .any(|client| client.local_addr().ok() == Some(peer))
        );
    }
    Ok(())
}

/// Unix streams: a pair, a listener at a path and one in the abstract
/// namespace, with their addresses.
#[ruio::test]
async fn unix_streams_connect_by_path_and_by_abstract_name() -> io::Result<()> {
    let (one, other) = UnixStream::pair()?;
    one.write_all("pair").await.0?;
    let (read, buf) = within(other.read_exact(Vec::with_capacity(4))).await;
    read?;
    assert_eq!(buf, b"pair");

    let dir = std::env::temp_dir().join(format!("ruio-portable-unix-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir(&dir)?;
    let path = dir.join("socket");
    let listener = UnixListener::bind(&path)?;
    assert_eq!(listener.local_addr()?.as_pathname(), Some(path.as_path()));
    let client = within(UnixStream::connect(&path)).await?;
    let (server, peer) = within(listener.accept()).await?;
    assert!(peer.is_unnamed());
    assert_eq!(client.peer_addr()?.as_pathname(), Some(path.as_path()));
    server.write_all("by path").await.0?;
    let (read, buf) = within(client.read_exact(Vec::with_capacity(7))).await;
    read?;
    assert_eq!(buf, b"by path");
    std::fs::remove_dir_all(&dir)?;

    let name = format!("ruio-portable-{}", std::process::id());
    let addr = std::os::unix::net::SocketAddr::from_abstract_name(name.as_bytes())?;
    let listener = UnixListener::bind_addr(&addr)?;
    let client = within(UnixStream::connect_addr(&addr)).await?;
    let (server, _) = within(listener.accept()).await?;
    client.write_all("abstract").await.0?;
    let (read, buf) = within(server.read_exact(Vec::with_capacity(8))).await;
    read?;
    assert_eq!(buf, b"abstract");
    let missing = UnixStream::connect(dir.join("missing")).await.unwrap_err();
    assert_eq!(missing.kind(), ErrorKind::NotFound);
    Ok(())
}

/// A stream moves to another core, whose ring its waits then run on, and
/// comes back.
#[test]
fn a_stream_is_read_and_written_on_another_core() {
    let outputs = two()
        .run(|core| async move {
            if core.index() != 0 {
                return None;
            }
            let (client, server) = tcp_pair().await.unwrap();
            let far = ruio::spawn_on(1, async move {
                let (read, buf) = server.read_exact(Vec::with_capacity(5)).await;
                read.unwrap();
                server.write_all(buf).await.0.unwrap();
                (server, ruio::core().unwrap().index())
            });
            client.write_all("there").await.0.unwrap();
            let (read, reply) = within(client.read_exact(Vec::with_capacity(5))).await;
            read.unwrap();
            let (server, ran_on) = within(far).await.unwrap();
            // Back on this core, and still working.
            server.write_all("again").await.0.unwrap();
            let (read, again) = within(client.read_exact(Vec::with_capacity(5))).await;
            read.unwrap();
            Some((reply, again, ran_on))
        })
        .unwrap();
    let (reply, again, ran_on) = outputs.into_iter().next().flatten().unwrap();
    assert_eq!(
        (&reply[..], &again[..], ran_on),
        (&b"there"[..], &b"again"[..], 1)
    );
}

/// A read waiting on one core moves to another while it waits, and
/// completes there once data arrives.
#[test]
fn a_read_moved_to_another_core_mid_wait_completes_there() {
    let outputs = two()
        .run(|core| async move {
            if core.index() != 0 {
                return None;
            }
            let (client, server) = tcp_pair().await.unwrap();
            let server = std::sync::Arc::new(server);
            let reading = {
                let server = server.clone();
                Box::pin(async move {
                    let (read, buf) = server.read(Vec::with_capacity(16)).await;
                    (read.unwrap(), buf, ruio::core().unwrap().index())
                })
            };
            let mut reading = reading;
            assert!(pending_once(&mut reading).await, "waiting here");
            let far = ruio::spawn_on(1, reading);
            ruio::time::sleep(Duration::from_millis(5)).await;
            client.write_all("moved").await.0.unwrap();
            Some(within(far).await.unwrap())
        })
        .unwrap();
    let (read, buf, ran_on) = outputs.into_iter().next().flatten().unwrap();
    assert_eq!((read, &buf[..], ran_on), (5, &b"moved"[..], 1));
}

/// A stream dropped on a thread with no runtime closes, and its peer reads
/// the end.
#[ruio::test]
async fn a_stream_dropped_on_another_thread_closes() -> io::Result<()> {
    let (client, server) = tcp_pair().await?;
    let fd = client.as_raw_fd();
    assert!(fd >= 0);
    std::thread::spawn(move || drop(client)).join().unwrap();
    let (read, _) = within(server.read(Vec::with_capacity(8))).await;
    assert_eq!(read?, 0);
    Ok(())
}
