//! A listener that the engines of a process share: engines on several
//! threads accept from it, each taking a share of its connections onto its
//! own port, and an accept waits for a connection on its thread's port.
#![cfg(windows)]

use std::future::Future;
use std::io::{self, Read as _};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::Duration;

use ruio_executor::{Runtime, time};
use ruio_iocp::buf::BufPool;
use ruio_iocp::net::{SharedListener, TcpListener, TcpStream};

/// How long a test waits for what should come at once, rather than hang.
const PATIENCE: Duration = Duration::from_secs(10);

/// The loopback address, and a port for the system to choose.
fn loopback() -> SocketAddr {
    (Ipv4Addr::LOCALHOST, 0).into()
}

#[test]
fn engines_on_several_threads_share_out_the_connections() -> io::Result<()> {
    const ENGINES: usize = 3;
    const CLIENTS: usize = 60;
    let listener = Arc::new(SharedListener::bind(loopback())?);
    let addr = listener.local_addr()?;
    let done = Arc::new(AtomicBool::new(false));
    let started = Arc::new(Barrier::new(ENGINES + 1));
    let servers: Vec<_> = (0..ENGINES)
        .map(|index| {
            let listener = Arc::clone(&listener);
            let (done, started) = (Arc::clone(&done), Arc::clone(&started));
            thread::spawn(move || -> io::Result<usize> {
                let runtime = Runtime::new()?;
                started.wait();
                runtime.block_on(async move {
                    let mut accepted = 0;
                    while !done.load(Ordering::Acquire) {
                        // An accept dropped at its timeout cancels its wait.
                        let wait = Duration::from_millis(20);
                        let Ok(taken) = time::timeout(wait, listener.accept()).await else {
                            continue;
                        };
                        let (stream, peer) = taken?;
                        assert_eq!(stream.peer_addr()?, peer);
                        // Each connection is told which engine took it.
                        let (written, _) = stream.write_all(vec![index as u8]).await;
                        written?;
                        accepted += 1;
                    }
                    Ok(accepted)
                })
            })
        })
        .collect();
    started.wait();
    let mut served = [0; ENGINES];
    for _ in 0..CLIENTS {
        let mut client = std::net::TcpStream::connect(addr)?;
        client.set_read_timeout(Some(PATIENCE))?;
        let mut index = [0];
        client.read_exact(&mut index)?;
        served[usize::from(index[0])] += 1;
    }
    done.store(true, Ordering::Release);
    let accepted = servers
        .into_iter()
        .map(|server| server.join().unwrap())
        .collect::<io::Result<Vec<usize>>>()?;
    // Each connection was taken once, and more than one engine took some.
    assert_eq!(accepted, served);
    assert_eq!(served.iter().sum::<usize>(), CLIENTS);
    assert!(
        served.iter().filter(|&&count| count > 0).count() > 1,
        "{served:?}"
    );
    Ok(())
}

#[test]
fn an_accept_waits_for_a_connection_whose_stream_goes_through_the_port() -> io::Result<()> {
    let listener = SharedListener::bind(loopback())?;
    let addr = listener.local_addr()?;
    Runtime::new()?.block_on(async {
        // Nothing connects, so the accept waits until its timeout.
        let waited = time::timeout(Duration::from_millis(20), listener.accept()).await;
        assert!(waited.is_err(), "the accept waits for a connection");
        let client = ruio_executor::spawn(TcpStream::connect(addr));
        let (server, peer) = time::timeout(PATIENCE, listener.accept())
            .await
            .expect("the connection comes")?;
        let client = client.await.unwrap()?;
        assert_eq!(peer, client.local_addr()?);
        assert_eq!(server.local_addr()?, addr);

        let (written, _) = client.write_all(b"ping".to_vec()).await;
        written?;
        let pool = BufPool::new(2, 64)?;
        let mut arriving = server.recv_stream(&pool);
        let data = arriving.next().await.expect("the data")?;
        assert_eq!(data.to_vec(), b"ping");
        drop((data, arriving));
        let (written, _) = server.write_all(b"pong".to_vec()).await;
        written?;
        let (read, buf) = client.read_exact(Vec::with_capacity(4)).await;
        read?;
        assert_eq!(buf, b"pong");
        Ok(())
    })
}

#[test]
fn a_listener_binds_anywhere_and_accepts_where_a_runtime_runs() -> io::Result<()> {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SharedListener>();
    let listener = SharedListener::bind(loopback())?;
    let addr = listener.local_addr()?;
    assert!(format!("{listener:?}").starts_with("SharedListener"));
    let _client = std::net::TcpStream::connect(addr)?;
    let poll = pin!(listener.accept()).poll(&mut Context::from_waker(Waker::noop()));
    let Poll::Ready(Err(err)) = poll else {
        panic!("an accept ran where no runtime runs");
    };
    assert_eq!(err.kind(), io::ErrorKind::Other);
    Runtime::new()?.block_on(async {
        // The connection waited for an accept that could take it.
        let (_server, _) = time::timeout(PATIENCE, listener.accept())
            .await
            .expect("the connection waits")?;
        // A listener of the port's cannot bind the address.
        let err = TcpListener::bind(addr).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AddrInUse, "{err}");
        Ok(())
    })
}

#[test]
fn an_ipv6_listener_takes_connections_from_ipv4_addresses() -> io::Result<()> {
    if let Err(err) = std::net::TcpListener::bind((Ipv6Addr::LOCALHOST, 0)) {
        eprintln!("skipped: no IPv6 loopback: {err}");
        return Ok(());
    }
    let mapped = Ipv4Addr::LOCALHOST.to_ipv6_mapped();
    let listener = SharedListener::bind((mapped, 0))?;
    let port = listener.local_addr()?.port();
    Runtime::new()?.block_on(async {
        let client = ruio_executor::spawn(TcpStream::connect((Ipv4Addr::LOCALHOST, port).into()));
        let (_server, peer) = time::timeout(PATIENCE, listener.accept())
            .await
            .expect("the connection comes")?;
        let client = client.await.unwrap()?;
        assert_eq!(
            peer,
            SocketAddr::from((mapped, client.local_addr()?.port()))
        );
        Ok(())
    })
}
