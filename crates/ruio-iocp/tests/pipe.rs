//! Named pipes read and written through the port, and reads cancelled while
//! the system has their buffers.
#![cfg(windows)]

use std::cell::Cell;
use std::io;
use std::rc::Rc;
use std::time::Duration;

use ruio_buf::{IoBuf, IoBufMut};
use ruio_executor::{Runtime, time};
use ruio_iocp::pipe::NamedPipe;

fn name(test: &str) -> String {
    format!(r"\\.\pipe\ruio-iocp-{}-{test}", std::process::id())
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
fn bytes_written_at_one_end_arrive_at_the_other() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let server = NamedPipe::create(name("ends"))?;
        let client = NamedPipe::connect(name("ends"))?;
        let (written, _) = client.write(b"ping".to_vec()).await;
        assert_eq!(written?, 4);
        let (read, buf) = server.read(Vec::with_capacity(16)).await;
        assert_eq!(read?, 4);
        assert_eq!(buf, b"ping");
        let (written, _) = server.write(b"pong".to_vec()).await;
        assert_eq!(written?, 4);
        let (read, buf) = client.read(Vec::with_capacity(16)).await;
        assert_eq!(read?, 4);
        assert_eq!(buf, b"pong");
        Ok(())
    })
}

#[test]
fn a_read_once_the_other_end_closes_reads_nothing() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let server = NamedPipe::create(name("closed"))?;
        let client = NamedPipe::connect(name("closed"))?;
        drop(client);
        let (read, _) = server.read(Vec::with_capacity(16)).await;
        assert_eq!(read?, 0);
        Ok(())
    })
}

/// `ERROR_PIPE_LISTENING`: the server's end of a pipe that no client has
/// opened yet.
const ERROR_PIPE_LISTENING: i32 = 536;

#[test]
fn the_servers_end_waits_for_a_client_to_open_the_pipe() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let server = Rc::new(NamedPipe::create(name("wait"))?);
        // With no client, the server's end has nobody to read from.
        let (read, _) = server.read(Vec::with_capacity(16)).await;
        assert_eq!(read.unwrap_err().raw_os_error(), Some(ERROR_PIPE_LISTENING));
        // A wait that no client ends waits until the timeout drops it,
        // which cancels it.
        let waited = time::timeout(Duration::from_millis(20), server.wait_for_client()).await;
        assert!(waited.is_err());

        let waiting = ruio_executor::spawn({
            let server = Rc::clone(&server);
            async move { server.wait_for_client().await }
        });
        ruio_executor::yield_now().await;
        let client = NamedPipe::connect(name("wait"))?;
        waiting.await.expect("the wait's task ran to its end")?;
        let (written, _) = client.write(b"hello".to_vec()).await;
        assert_eq!(written?, 5);
        let (read, buf) = server.read(Vec::with_capacity(16)).await;
        assert_eq!(read?, 5);
        assert_eq!(buf, b"hello");
        Ok(())
    })
}

#[test]
fn a_wait_for_a_client_that_has_opened_the_pipe_ends_at_once() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let server = NamedPipe::create(name("opened"))?;
        let client = NamedPipe::connect(name("opened"))?;
        server.wait_for_client().await?;
        let (written, _) = server.write(b"there".to_vec()).await;
        assert_eq!(written?, 5);
        let (read, buf) = client.read(Vec::with_capacity(16)).await;
        assert_eq!(read?, 5);
        assert_eq!(buf, b"there");
        Ok(())
    })
}

#[test]
fn a_dropped_read_is_cancelled_and_frees_its_buffer_once_the_system_is_done() -> io::Result<()> {
    Runtime::new()?.block_on(async {
        let server = NamedPipe::create(name("cancel"))?;
        let client = NamedPipe::connect(name("cancel"))?;
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
        // The pipe works after.
        let (written, _) = client.write(b"after".to_vec()).await;
        assert_eq!(written?, 5);
        let (read, buf) = server.read(Vec::with_capacity(16)).await;
        assert_eq!(read?, 5);
        assert_eq!(buf, b"after");
        Ok(())
    })
}

#[test]
fn a_runtime_dropped_with_a_read_in_flight_waits_for_its_cancellation() -> io::Result<()> {
    let dropped = Rc::new(Cell::new(false));
    let runtime = Runtime::new()?;
    let spawned = Rc::clone(&dropped);
    runtime.block_on(async move {
        let server = NamedPipe::create(name("shutdown"))?;
        let client = NamedPipe::connect(name("shutdown"))?;
        // Detached, so that the runtime holds the task, and its read, when
        // `block_on` returns.
        drop(ruio_executor::spawn(async move {
            let buf = Watched {
                bytes: Vec::with_capacity(16),
                dropped: spawned,
            };
            let _client = client;
            let _ = server.read(buf).await;
        }));
        ruio_executor::yield_now().await;
        io::Result::Ok(())
    })?;
    assert!(!dropped.get(), "the read is still in flight");
    drop(runtime);
    assert!(dropped.get(), "dropping the runtime freed the buffer");
    Ok(())
}
