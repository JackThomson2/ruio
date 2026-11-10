//! Files read and written through the port, on ruio-executor's runtime.
#![cfg(windows)]

use std::io;
use std::path::PathBuf;
use std::rc::Rc;

use ruio_buf::IoBuf;
use ruio_executor::Runtime;
use ruio_iocp::fs::File;

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
fn a_file_opens_only_where_a_runtime_runs() {
    let scratch = Scratch::new("outside");
    std::fs::write(&scratch.0, b"x").unwrap();
    let err = File::open(&scratch.0).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Other);
}
