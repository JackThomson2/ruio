//! Files read and written through the port, on ruio-executor's runtime.
#![cfg(windows)]

use std::future::poll_fn;
use std::io;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Poll;
use std::thread;
use std::time::Duration;

use ruio_buf::IoBuf;
use ruio_executor::{Runtime, time};
use ruio_iocp::buf::BufPool;
use ruio_iocp::fs::{File, OpenOptions};

/// A path for a test's file in the temporary directory, which it removes
/// when dropped.
struct Scratch(PathBuf);

impl Scratch {
    fn new(test: &str) -> Self {
        let name = format!("ruio-iocp-{}-{test}", std::process::id());
        Self(std::env::temp_dir().join(name))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[test]
fn what_is_written_through_the_port_reads_back() -> io::Result<()> {
    let scratch = Scratch::new("round-trip");
    Runtime::new()?.block_on(async {
        let file = File::create(&scratch.0)?;
        let (written, _) = file.write_at(b"hello, port".to_vec(), 0).await;
        assert_eq!(written?, 11);
        let (written, _) = file.write_at("!", 11).await;
        assert_eq!(written?, 1);
        drop(file);

        let file = File::open(&scratch.0)?;
        let (read, buf) = file.read_at(Vec::with_capacity(64), 0).await;
        assert_eq!(read?, 12);
        assert_eq!(buf, b"hello, port!");
        let (read, buf) = file.read_at(Vec::with_capacity(4), 7).await;
        assert_eq!(read?, 4);
        assert_eq!(buf, b"port");
        Ok(())
    })
}

#[test]
fn a_read_past_the_end_reads_nothing() -> io::Result<()> {
    let scratch = Scratch::new("past-the-end");
    std::fs::write(&scratch.0, b"short")?;
    Runtime::new()?.block_on(async {
        let file = File::open(&scratch.0)?;
        let (read, buf) = file.read_at(Vec::with_capacity(8), 1000).await;
        assert_eq!(read?, 0);
        assert!(buf.is_empty());
        Ok(())
    })
}

#[test]
fn reads_in_flight_at_once_each_get_their_part() -> io::Result<()> {
    let scratch = Scratch::new("at-once");
    let data: Vec<u8> = (0..=255).collect();
    std::fs::write(&scratch.0, &data)?;
    Runtime::new()?.block_on(async {
        let file = Rc::new(File::open(&scratch.0)?);
        let reads: Vec<_> = (0..16)
            .map(|part| {
                let file = Rc::clone(&file);
                ruio_executor::spawn(async move {
                    file.read_at(Vec::with_capacity(16), part * 16).await
                })
            })
            .collect();
        for (part, read) in reads.into_iter().enumerate() {
            let (read, buf) = read.await.unwrap();
            assert_eq!(read?, 16);
            assert_eq!(buf, data[part * 16..][..16]);
        }
        Ok(())
    })
}

#[test]
fn a_buffer_is_written_whole_and_filled_whole_at_an_offset() -> io::Result<()> {
    const LEN: usize = 200_000;
    let scratch = Scratch::new("whole");
    Runtime::new()?.block_on(async {
        let file = File::create(&scratch.0)?;
        let data: Vec<u8> = (0..LEN).map(|i| (i % 251) as u8).collect();
        let (written, data) = file.write_all_at(data, 10).await;
        written?;
        drop(file);

        let file = File::open(&scratch.0)?;
        // A file's read fills the buffer after what it holds.
        let mut buf = Vec::with_capacity(1004);
        buf.extend_from_slice(b"head");
        let (read, buf) = file.read_exact_at(buf.slice(..1004), 10 + 500).await;
        read?;
        let buf = buf.into_inner();
        assert_eq!(&buf[..4], b"head");
        assert_eq!(buf[4..], data[500..1500]);

        // The file ends before this buffer is full.
        let end = (10 + LEN - 5) as u64;
        let (read, buf) = file.read_exact_at(Vec::with_capacity(64), end).await;
        assert_eq!(read.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(buf, data[LEN - 5..]);
        Ok(())
    })
}

#[test]
fn options_open_as_the_standard_librarys_do() -> io::Result<()> {
    let scratch = Scratch::new("options");
    Runtime::new()?.block_on(async {
        let missing = OpenOptions::new().read(true).open(&scratch.0).unwrap_err();
        assert_eq!(missing.kind(), io::ErrorKind::NotFound);

        let mut new = OpenOptions::new();
        new.write(true).create_new(true);
        let file = new.open(&scratch.0)?;
        let (written, _) = file.write_all_at(b"first".to_vec(), 0).await;
        written?;
        drop(file);
        let exists = new.open(&scratch.0).unwrap_err();
        assert_eq!(exists.kind(), io::ErrorKind::AlreadyExists);

        // A file opened to append writes at its end, whatever the offset.
        let file = File::options().append(true).open(&scratch.0)?;
        let (written, _) = file.write_all_at(b", second".to_vec(), 0).await;
        written?;
        drop(file);
        assert_eq!(std::fs::read(&scratch.0)?, b"first, second");

        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&scratch.0)?;
        drop(file);
        assert!(std::fs::read(&scratch.0)?.is_empty());
        Ok(())
    })
}

#[test]
fn a_file_opened_on_another_thread_is_read_through_the_port() -> io::Result<()> {
    let scratch = Scratch::new("elsewhere");
    std::fs::write(&scratch.0, b"opened elsewhere")?;
    let path = scratch.0.clone();
    let opened = thread::spawn(move || OpenOptions::new().read(true).open_std(path))
        .join()
        .unwrap()?;
    Runtime::new()?.block_on(async {
        let file = File::from_std(opened)?;
        let (read, buf) = file.read_at(Vec::with_capacity(32), 0).await;
        assert_eq!(read?, 16);
        assert_eq!(buf, b"opened elsewhere");
        Ok(())
    })
}

#[test]
fn the_standard_librarys_file_does_what_the_port_has_no_call_for() -> io::Result<()> {
    let scratch = Scratch::new("as-std");
    Runtime::new()?.block_on(async {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&scratch.0)?;
        let (written, _) = file.write_all_at(b"0123456789".to_vec(), 0).await;
        written?;
        // On another thread, as a runtime's blocking pool runs such calls.
        let shared = Arc::clone(file.as_std());
        let len = thread::spawn(move || {
            shared.sync_all()?;
            shared.set_len(4)?;
            io::Result::Ok(shared.metadata()?.len())
        })
        .join()
        .unwrap()?;
        assert_eq!(len, 4);
        let (read, buf) = file.read_at(Vec::with_capacity(16), 0).await;
        assert_eq!(read?, 4);
        assert_eq!(buf, b"0123");
        Ok(())
    })
}

#[test]
fn a_file_is_read_into_a_pool_from_an_offset_to_its_end() -> io::Result<()> {
    let scratch = Scratch::new("stream");
    let data: Vec<u8> = (0..10_000).map(|at| (at % 251) as u8).collect();
    std::fs::write(&scratch.0, &data)?;
    Runtime::new()?.block_on(async {
        let file = File::open(&scratch.0)?;
        let pool = BufPool::new(2, 4096)?;
        let mut stream = file.read_stream(&pool, 100);
        // Nothing is read until the stream is polled.
        assert!(stream.next_ready().is_none());
        assert_eq!(pool.available(), 2);
        let mut read = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            assert!(!chunk.is_empty());
            assert!(chunk.len() <= pool.buffer_size());
            read.extend_from_slice(&chunk);
            assert_eq!(stream.offset(), 100 + read.len() as u64);
        }
        assert!(stream.is_ended());
        assert!(stream.next().await.is_none());
        assert!(stream.next_ready().is_none());
        assert!(
            read == data[100..],
            "what was read is the file from the offset on"
        );
        assert_eq!(pool.available(), 2);
        Ok(())
    })
}

#[test]
fn a_stream_of_a_file_waits_for_a_buffer_that_a_lease_holds() -> io::Result<()> {
    let scratch = Scratch::new("stream-waits");
    std::fs::write(&scratch.0, b"0123456789")?;
    Runtime::new()?.block_on(async {
        let file = File::open(&scratch.0)?;
        let pool = BufPool::new(1, 4)?;
        let mut stream = file.read_stream(&pool, 0);
        let first = stream.next().await.expect("the file's start")?;
        assert_eq!(&*first, b"0123");
        // The one buffer is leased, so the next read does not start.
        let waited = time::timeout(Duration::from_millis(20), stream.next()).await;
        assert!(waited.is_err());
        assert!(stream.next_ready().is_none());
        drop(first);
        let second = stream.next().await.expect("the file's middle")?;
        assert_eq!(&*second, b"4567");
        drop(second);

        // A read that was started and has completed is there to take
        // without waiting.
        let started = poll_fn(|cx| Poll::Ready(stream.poll_next(cx).is_pending())).await;
        assert!(started);
        let mut last = None;
        for _ in 0..200 {
            last = stream.next_ready();
            if last.is_some() {
                break;
            }
            time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(&*last.expect("the file's end")?, b"89");
        assert!(stream.next().await.is_none());
        Ok(())
    })
}

#[test]
fn a_stream_dropped_with_a_read_in_flight_returns_its_buffer() -> io::Result<()> {
    let scratch = Scratch::new("stream-dropped");
    std::fs::write(&scratch.0, b"0123456789")?;
    Runtime::new()?.block_on(async {
        let file = File::open(&scratch.0)?;
        let pool = BufPool::new(1, 16)?;
        let mut stream = file.read_stream(&pool, 0);
        let started = poll_fn(|cx| Poll::Ready(stream.poll_next(cx).is_pending())).await;
        assert!(started);
        assert_eq!(pool.available(), 0);
        drop(stream);
        // The read's completion comes through the port, cancelled or not,
        // and the engine frees the read's slot, and the lease in it, then.
        for _ in 0..200 {
            if pool.available() == 1 {
                break;
            }
            time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(pool.available(), 1);
        // The file reads as before.
        let mut stream = file.read_stream(&pool, 6);
        let rest = stream.next().await.expect("the file's end")?;
        assert_eq!(&*rest, b"6789");
        Ok(())
    })
}

#[test]
fn a_file_opens_only_where_a_runtime_runs() {
    let scratch = Scratch::new("outside");
    std::fs::write(&scratch.0, b"x").unwrap();
    let err = File::open(&scratch.0).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Other);
}

/// A file that the standard library opened for I/O that waits, which no
/// port can take, is read and written all the same, each operation
/// finishing as it starts. One opened for overlapped I/O goes through the
/// port, which another engine's cannot take it from.
#[test]
fn files_of_the_standard_library_are_taken_however_they_were_opened() -> io::Result<()> {
    let scratch = Scratch::new("from-std");
    std::fs::write(&scratch.0, b"hello world")?;
    let overlapped = Runtime::new()?.block_on(async {
        let waits = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&scratch.0)?;
        let waits = File::from_std(waits)?;
        assert!(!waits.appends());
        let (read, buf) = waits.read_at(Vec::with_capacity(16), 6).await;
        assert_eq!(read?, 5);
        assert_eq!(buf, b"world");
        let (read, buf) = waits.read_at(Vec::with_capacity(16), 100).await;
        assert_eq!(read?, 0);
        assert!(buf.is_empty());
        waits.write_all_at(b"HELLO".to_vec(), 0).await.0?;
        let pool = BufPool::new(1, 8)?;
        let mut stream = waits.read_stream(&pool, 4);
        let data = stream.next().await.expect("the file's data")?;
        assert_eq!(&*data, b"O world");
        drop(data);
        assert!(stream.next().await.is_none());

        let appending = std::fs::OpenOptions::new().append(true).open(&scratch.0)?;
        let appending = File::from_std(appending)?;
        assert!(appending.appends());
        appending.write_at(b"!".to_vec(), 0).await.0?;

        let overlapped = File::from_std(OpenOptions::new().read(true).open_std(&scratch.0)?)?;
        assert!(!overlapped.appends());
        let (read, buf) = overlapped.read_at(Vec::with_capacity(16), 0).await;
        assert_eq!(read?, 12);
        assert_eq!(buf, b"HELLO world!");
        let overlapped = Arc::into_inner(overlapped.into_std());
        io::Result::Ok(overlapped.expect("nothing else has the file"))
    })?;
    let taken = thread::spawn(move || {
        Runtime::new()?.block_on(async { File::from_std(overlapped).map(drop) })
    });
    // `ERROR_INVALID_PARAMETER`, as for any handle that a port has.
    let err = taken.join().unwrap().unwrap_err();
    assert_eq!(err.raw_os_error(), Some(87));
    Ok(())
}
