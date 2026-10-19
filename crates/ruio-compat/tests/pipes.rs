//! The adapter for a child's pipes: tokio's, futures-io's and hyper's
//! traits over the pipes of a `cat`, each pipe with its own direction.

#![cfg(all(feature = "tokio", feature = "futures-io", feature = "hyper"))]

mod support;

use std::io;
use std::process::Stdio;

use ruio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use ruio_compat::PortableCompat;

use support::{pattern, within};

/// A `cat`, its input and output piped.
fn cat() -> io::Result<Child> {
    Command::new("cat")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
}

/// The adapters of a child's input and output.
fn adapt(child: &mut Child) -> (PortableCompat<ChildStdin>, PortableCompat<ChildStdout>) {
    let stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    (PortableCompat::new(stdin), PortableCompat::new(stdout))
}

#[test]
fn the_adapters_have_the_traits_of_their_direction() {
    fn assert_reader<T>()
    where
        T: Send + Sync + Unpin + tokio::io::AsyncRead + futures_io::AsyncRead + hyper::rt::Read,
    {
    }
    fn assert_writer<T>()
    where
        T: Send + Sync + Unpin + tokio::io::AsyncWrite + futures_io::AsyncWrite + hyper::rt::Write,
    {
    }
    assert_reader::<PortableCompat<ChildStdout>>();
    assert_reader::<PortableCompat<ChildStderr>>();
    assert_writer::<PortableCompat<ChildStdin>>();
}

/// tokio's traits: a megabyte written by one task while another reads it,
/// so that both pipes fill and both sides wait; shutting the input down is
/// ready at once, and dropping it ends the child's input.
#[ruio::test]
async fn tokio_traits_carry_data_through_a_child() -> io::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut child = cat()?;
    let (mut stdin, mut stdout) = adapt(&mut child);
    let data = pattern(1 << 20, 5);
    let expected = data.clone();
    let writer = ruio::spawn(async move {
        stdin.write_all(&data).await?;
        stdin.write_all(b"a small line\n").await?;
        let bufs = [io::IoSlice::new(b"and "), io::IoSlice::new(b"another\n")];
        assert!(tokio::io::AsyncWrite::is_write_vectored(&stdin));
        let written = stdin.write_vectored(&bufs).await?;
        assert_eq!(written, 12);
        stdin.flush().await?;
        stdin.shutdown().await?;
        // Dropped here, which closes the pipe: cat's input ends.
        io::Result::Ok(())
    });
    let mut out = Vec::new();
    within(stdout.read_to_end(&mut out)).await?;
    within(writer).await.unwrap()?;
    let (body, tail) = out.split_at(expected.len().min(out.len()));
    assert!(body == expected);
    assert_eq!(tail, b"a small line\nand another\n");
    assert!(within(child.wait()).await?.success());
    Ok(())
}

/// futures-io's traits, likewise; closing the input is ready at once, and
/// the pipe closes as the adapter is dropped.
#[ruio::test]
async fn futures_io_traits_carry_data_through_a_child() -> io::Result<()> {
    use futures_util::{AsyncReadExt, AsyncWriteExt};

    let mut child = cat()?;
    let (mut stdin, mut stdout) = adapt(&mut child);
    let data = pattern(1 << 20, 9);
    let expected = data.clone();
    let writer = ruio::spawn(async move {
        stdin.write_all(&data).await?;
        stdin.write_all(b"through futures-io\n").await?;
        stdin.flush().await?;
        stdin.close().await?;
        drop(stdin);
        io::Result::Ok(())
    });
    let mut out = Vec::new();
    within(stdout.read_to_end(&mut out)).await?;
    within(writer).await.unwrap()?;
    let (body, tail) = out.split_at(expected.len().min(out.len()));
    assert!(body == expected);
    assert_eq!(tail, b"through futures-io\n");
    assert!(within(child.wait()).await?.success());
    Ok(())
}

/// tokio's `BufReader` over the output reads it line by line, as each line
/// is written.
#[ruio::test]
async fn a_buffered_reader_reads_a_child_line_by_line() -> io::Result<()> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let mut child = cat()?;
    let (mut stdin, stdout) = adapt(&mut child);
    let mut lines = BufReader::new(stdout);
    for line in ["first\n", "second line\n", "\n", "the last\n"] {
        within(stdin.write_all(line.as_bytes())).await?;
        let mut buf = Vec::new();
        within(lines.read_until(b'\n', &mut buf)).await?;
        assert_eq!(buf, line.as_bytes());
    }
    drop(stdin);
    let mut buf = Vec::new();
    assert_eq!(
        within(lines.read_until(b'\n', &mut buf)).await?,
        0,
        "the end"
    );
    assert!(within(child.wait()).await?.success());
    Ok(())
}

/// The standard error is read through its adapter, as the output is.
#[ruio::test]
async fn standard_error_is_read_through_the_adapter() -> io::Result<()> {
    use tokio::io::AsyncReadExt;

    let mut child = Command::new("sh")
        .args(["-c", "echo out; echo err >&2"])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut stderr = PortableCompat::new(child.stderr.take().unwrap());
    let mut out = String::new();
    within(stderr.read_to_string(&mut out)).await?;
    assert_eq!(out, "err\n");
    assert!(within(child.wait()).await?.success());
    Ok(())
}

/// hyper's traits, called through them: a write and a read through `cat`,
/// and a shutdown that leaves the pipe open until the adapter is dropped.
#[ruio::test]
async fn hyper_traits_carry_data_through_a_child() -> io::Result<()> {
    use std::future::poll_fn;
    use std::mem::MaybeUninit;
    use std::pin::Pin;

    use hyper::rt::{Read, ReadBuf, Write};

    let mut child = cat()?;
    let (mut stdin, mut stdout) = adapt(&mut child);
    assert!(stdin.is_write_vectored());
    let written = within(poll_fn(|cx| {
        Pin::new(&mut stdin).poll_write(cx, b"through hyper")
    }))
    .await?;
    assert_eq!(written, 13);
    within(poll_fn(|cx| Pin::new(&mut stdin).poll_flush(cx))).await?;
    within(poll_fn(|cx| Pin::new(&mut stdin).poll_shutdown(cx))).await?;
    // Still open: the child echoes what is written after the shutdown.
    within(poll_fn(|cx| Pin::new(&mut stdin).poll_write(cx, b"!"))).await?;
    drop(stdin);
    let mut out = Vec::new();
    loop {
        let mut room = [MaybeUninit::<u8>::uninit(); 64];
        let mut buf = ReadBuf::uninit(&mut room);
        within(poll_fn(|cx| {
            Pin::new(&mut stdout).poll_read(cx, buf.unfilled())
        }))
        .await?;
        if buf.filled().is_empty() {
            break;
        }
        out.extend_from_slice(buf.filled());
    }
    assert_eq!(out, b"through hyper!");
    assert!(within(child.wait()).await?.success());
    Ok(())
}
