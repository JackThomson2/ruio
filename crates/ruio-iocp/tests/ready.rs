//! Waits for sockets to be ready, polled by a runtime's tasks.
#![cfg(windows)]

use std::future::poll_fn;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::windows::io::AsSocket;
use std::time::Duration;

use ruio_executor::{Runtime, spawn, time};
use ruio_iocp::portable::{ReadyWait, events};

#[test]
fn a_task_reads_a_nonblocking_socket_once_it_is_ready() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let mut client = TcpStream::connect(listener.local_addr()?)?;
        let (server, _) = listener.accept()?;
        server.set_nonblocking(true)?;
        let writer = spawn(async move {
            time::sleep(Duration::from_millis(20)).await;
            client.write_all(b"ready").unwrap();
            client
        });
        let mut wait = ReadyWait::new();
        let mut buf = [0; 16];
        let read = loop {
            match (&server).read(&mut buf) {
                Ok(read) => break read,
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    poll_fn(|cx| wait.poll_ready(cx, server.as_socket(), events::READABLE)).await?;
                }
                Err(err) => return Err(err),
            }
        };
        assert_eq!(&buf[..read], b"ready");
        drop(writer.await.unwrap());
        Ok(())
    })
}
