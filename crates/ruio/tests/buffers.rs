//! Buffers and pooled receives inside the runtime.

use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;

use ruio::buf::{BufPool, IoBuf};
use ruio::uring::ops::{RecvStream, Write};

/// A task receives through a pool while another sends in pieces; the pool
/// is small enough to run dry along the way.
#[ruio::test]
async fn a_task_receives_what_another_sends() -> io::Result<()> {
    let driver = ruio::driver();
    let pool = BufPool::new(&driver, 2, 16)?;
    let (socket, peer) = UnixStream::pair()?;

    let receiver = ruio::spawn({
        let pool = pool.clone();
        async move {
            let mut stream = RecvStream::new(&ruio::driver(), socket.as_raw_fd(), &pool);
            let mut received = Vec::new();
            while let Some(bundle) = stream.next().await {
                received.extend(bundle?.to_vec());
            }
            io::Result::Ok(received)
        }
    });

    let message: Vec<u8> = (0..=255).collect();
    for piece in message.chunks(40) {
        let piece = piece.to_vec();
        let length = piece.len();
        let (written, _) = driver.op(Write::new(peer.as_raw_fd(), piece)).await;
        assert_eq!(written?, length);
    }
    // A slice writes just its part.
    let (written, _) = driver
        .op(Write::new(peer.as_raw_fd(), b"--end".slice(2..)))
        .await;
    assert_eq!(written?, 3);
    drop(peer);

    let received = receiver.await.unwrap()?;
    assert_eq!(&received[..256], &message[..]);
    assert_eq!(&received[256..], b"end");
    assert_eq!(pool.available(), 2);
    Ok(())
}
