//! Reads and writes of handles that no port has, files and pipes, polled
//! by a runtime's tasks and by other threads.
#![cfg(windows)]

use std::future::poll_fn;
use std::io;
use std::mem::MaybeUninit;
use std::os::windows::io::{AsHandle, OwnedHandle};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use ruio_executor::{Runtime, spawn};
use ruio_iocp::fs::OpenOptions;
use ruio_iocp::portable::{HandleWait, Pipe, read_at, write_at};

struct TempFile(std::path::PathBuf);

impl TempFile {
    fn new(test: &str) -> Self {
        Self(std::env::temp_dir().join(format!("ruio-iocp-portable-{}-{test}", std::process::id())))
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Reads `pipe` to its end, waiting whenever it has nothing.
async fn read_to_end(pipe: &Pipe) -> io::Result<Vec<u8>> {
    let mut wait = HandleWait::new();
    let mut read = Vec::new();
    loop {
        let mut buf = [MaybeUninit::new(0); 512];
        match pipe.try_read(&mut buf) {
            Ok(0) => return Ok(read),
            // SAFETY: the read initialized as many bytes.
            Ok(taken) => read.extend(
                buf[..taken]
                    .iter()
                    .map(|byte| unsafe { byte.assume_init() }),
            ),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                poll_fn(|cx| pipe.poll_read_ready(cx, &mut wait)).await?;
            }
            Err(err) => return Err(err),
        }
    }
}

#[test]
fn what_is_written_at_offsets_reads_back() -> io::Result<()> {
    let path = TempFile::new("offsets");
    Runtime::new()?.block_on(async {
        let file: OwnedHandle = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open_std(&path.0)?
            .into();
        let (written, _) = write_at(file.as_handle(), b"world".to_vec(), 6).await;
        assert_eq!(written?, 5);
        let (written, _) = write_at(file.as_handle(), b"hello ".to_vec(), 0).await;
        assert_eq!(written?, 6);
        let (read, buf) = read_at(file.as_handle(), Vec::with_capacity(32), 0).await;
        assert_eq!(read?, 11);
        assert_eq!(buf, b"hello world");
        let (read, buf) = read_at(file.as_handle(), Vec::with_capacity(8), 100).await;
        assert_eq!(read?, 0);
        assert!(buf.is_empty());
        // At the end of the file, whatever it holds.
        let (written, _) = write_at(file.as_handle(), b"!".to_vec(), u64::MAX).await;
        assert_eq!(written?, 1);
        assert_eq!(std::fs::read(&path.0)?, b"hello world!");
        Ok(())
    })
}

#[test]
fn a_childs_output_is_read_through_its_pipe() -> io::Result<()> {
    let mut child = Command::new("cmd")
        .args(["/c", "echo hello"])
        .stdout(Stdio::piped())
        .spawn()?;
    let stdout = Pipe::new(child.stdout.take().unwrap().into());
    let output = Runtime::new()?.block_on(async { read_to_end(&stdout).await })?;
    assert_eq!(output, b"hello\r\n");
    assert!(child.wait()?.success());
    Ok(())
}

#[test]
fn a_pipe_is_read_by_a_task_that_moves_between_runtimes() -> io::Result<()> {
    let mut child = Command::new("cmd")
        .args(["/c", "ping -n 2 127.0.0.1 >nul & echo late"])
        .stdout(Stdio::piped())
        .spawn()?;
    let stdout = Pipe::new(child.stdout.take().unwrap().into());
    // A first runtime starts the wait, and ends with the read in flight.
    Runtime::new()?.block_on(async {
        let mut buf = [MaybeUninit::new(0); 8];
        let err = stdout.try_read(&mut buf).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
    });
    // Another, on another thread, reads it all.
    let output =
        std::thread::spawn(move || Runtime::new()?.block_on(async { read_to_end(&stdout).await }))
            .join()
            .unwrap()?;
    assert_eq!(output, b"late\r\n");
    assert!(child.wait()?.success());
    Ok(())
}

#[test]
fn a_read_dropped_in_flight_is_cancelled() -> io::Result<()> {
    let mut child = Command::new("cmd")
        .args(["/c", "ping -n 3 127.0.0.1 >nul"])
        .stdout(Stdio::piped())
        .spawn()?;
    let stdout: OwnedHandle = child.stdout.take().unwrap().into();
    Runtime::new()?.block_on(async {
        let started = Instant::now();
        let read = spawn(async move {
            let (read, _) = read_at(stdout.as_handle(), Vec::with_capacity(16), 0).await;
            read
        });
        ruio_executor::time::sleep(Duration::from_millis(20)).await;
        read.abort();
        assert!(read.await.is_err());
        assert!(started.elapsed() < Duration::from_secs(1));
    });
    child.kill()?;
    child.wait()?;
    Ok(())
}
