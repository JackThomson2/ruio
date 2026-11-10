//! Files read and written through the port, on ruio-executor's runtime.
#![cfg(windows)]

use std::io;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::thread;

use ruio_buf::IoBuf;
use ruio_executor::Runtime;
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
fn a_file_not_opened_for_overlapped_io_is_refused() -> io::Result<()> {
    let scratch = Scratch::new("not-overlapped");
    std::fs::write(&scratch.0, b"x")?;
    Runtime::new()?.block_on(async {
        let plain = std::fs::File::open(&scratch.0)?;
        assert!(
            File::from_std(plain).is_err(),
            "a port takes only files opened for overlapped I/O"
        );
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
fn a_file_opens_only_where_a_runtime_runs() {
    let scratch = Scratch::new("outside");
    std::fs::write(&scratch.0, b"x").unwrap();
    let err = File::open(&scratch.0).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Other);
}
