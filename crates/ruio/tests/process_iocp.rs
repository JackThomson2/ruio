//! Child processes inside the runtime, on Windows.
//!
//! The children are programs every Windows has: `cmd`; `findstr ^`, which
//! copies its input to its output, as `cat` does, for lines that end in
//! CRLF; and `ping -n`, which takes about a second for each echo after the
//! first.

#![cfg(ruio_iocp)]

mod support;

use std::future::{Future, poll_fn};
use std::io::{self, ErrorKind, IoSlice};
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::process::Stdio;
use std::task::Poll;
use std::time::{Duration, Instant};

use ruio::io::{AsyncRead, AsyncWrite};
use ruio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};

use support::{PATIENCE, within};

/// `CREATE_NO_WINDOW`, a flag of `CreateProcessW`'s.
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

fn assert_send<T: Send>(_: &T) {}

/// `cmd` running `script`, as it is, with no AutoRun.
fn shell(script: &str) -> Command {
    let mut command = Command::new("cmd");
    command.args(["/d", "/c"]).raw_arg(script);
    command
}

/// A child that copies its input to its output, line by line.
fn cat() -> Command {
    let mut command = Command::new("findstr");
    command.arg("^");
    command
}

/// A child that runs for about `seconds` seconds, writing nothing.
fn sleep(seconds: u32) -> Command {
    let mut command = Command::new("ping");
    command
        .args(["-n", &(seconds + 1).to_string(), "127.0.0.1"])
        .stdout(Stdio::null());
    command
}

/// `count` lines of text, each ending in CRLF, which `cat` copies whole.
fn lines(count: usize) -> Vec<u8> {
    (0..count)
        .flat_map(|index| {
            format!("line {index:07} of what goes through the pipes\r\n").into_bytes()
        })
        .collect()
}

/// Two cores, on two CPUs if the machine has them.
fn two() -> ruio::Cores {
    let all = ruio::Cores::all().unwrap();
    ruio::Cores::cpus(all.cpu_list().iter().copied().cycle().take(2))
}

/// Polls `future` once, where it lies, and says whether it is pending.
async fn pending_once<F: Future + Unpin>(future: &mut F) -> bool {
    poll_fn(|cx| Poll::Ready(Pin::new(&mut *future).poll(cx).is_pending())).await
}

#[ruio::test]
async fn a_child_is_spawned_and_waited_for() -> io::Result<()> {
    let status = within(shell("exit 3").status()).await?;
    assert_eq!(status.code(), Some(3));
    let mut child = shell("exit 0").creation_flags(CREATE_NO_WINDOW).spawn()?;
    assert!(child.id().is_some());
    let wait = child.wait();
    assert_send(&wait);
    assert!(within(wait).await?.success());
    assert_eq!(child.id(), None, "its status is known");
    // The status is kept.
    assert!(within(child.wait()).await?.success());
    assert!(child.try_wait()?.unwrap().success());
    let err = Command::new(r"C:\nonexistent\program.exe")
        .spawn()
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    Ok(())
}

#[ruio::test]
async fn output_collects_both_streams_with_no_input() -> io::Result<()> {
    let output = shell(r#"findstr "^" & echo out& echo err>&2& exit 2"#).output();
    assert_send(&output);
    let output = within(output).await?;
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(
        output.stdout, b"out\r\n",
        "the input is null, so findstr ends at once"
    );
    assert_eq!(output.stderr, b"err\r\n");
    // Streams set explicitly stay as they are.
    let output = within(shell("echo out").stdout(Stdio::null()).output()).await?;
    assert!(output.stdout.is_empty());
    // As do the command's other settings.
    let output = within(
        shell("echo %RUIO_PROCESS_TEST%")
            .env("RUIO_PROCESS_TEST", "set")
            .output(),
    )
    .await?;
    assert_eq!(output.stdout, b"set\r\n");
    Ok(())
}

/// A megabyte through `cat`, written by one task while another reads, so
/// that neither pipe stalls the child.
#[ruio::test]
async fn data_goes_through_the_childs_pipes() -> io::Result<()> {
    let data = lines(20_000);
    let mut child = cat().stdin(Stdio::piped()).stdout(Stdio::piped()).spawn()?;
    let stdin = child.stdin.take().unwrap();
    let expected = data.clone();
    let writer = ruio::spawn(async move {
        let (written, _) = stdin.write_all(data).await;
        // Dropped here, which closes the pipe once the last write is done:
        // cat's input ends.
        written
    });
    let stdout = child.stdout.take().unwrap();
    let (read, out) = within(stdout.read_to_end(Vec::new())).await;
    assert_eq!(read?, expected.len());
    assert!(out == expected);
    within(writer).await.unwrap()?;
    assert!(within(child.wait()).await?.success());
    Ok(())
}

/// Everything `stdout` has, to its end, read with its poll method into a
/// buffer of the caller's.
async fn poll_read_to_end(stdout: &mut ChildStdout) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut buf = [MaybeUninit::<u8>::uninit(); 4096];
    loop {
        let read = poll_fn(|cx| stdout.poll_read(cx, &mut buf)).await?;
        if read == 0 {
            return Ok(out);
        }
        // SAFETY: the read initialized the first `read` bytes.
        out.extend_from_slice(unsafe { std::slice::from_raw_parts(buf.as_ptr().cast(), read) });
    }
}

/// A megabyte through `cat` with the poll methods, written by one task from
/// the caller's buffer while another reads into its own, so that the writes
/// wait for the last to finish and the reads for data.
#[ruio::test]
async fn data_goes_through_the_childs_pipes_with_poll_methods() -> io::Result<()> {
    let data = lines(20_000);
    let mut child = cat().stdin(Stdio::piped()).stdout(Stdio::piped()).spawn()?;
    let mut stdin = child.stdin.take().unwrap();
    let expected = data.clone();
    let writer = ruio::spawn(async move {
        let mut done = 0;
        while done < data.len() {
            let written = poll_fn(|cx| stdin.poll_write(cx, &data[done..])).await?;
            assert_ne!(written, 0);
            done += written;
        }
        // Dropped here, which closes the pipe once the last write is done:
        // cat's input ends.
        io::Result::Ok(())
    });
    let mut stdout = child.stdout.take().unwrap();
    let out = within(poll_read_to_end(&mut stdout)).await?;
    assert_eq!(out.len(), expected.len());
    assert!(out == expected);
    within(writer).await.unwrap()?;
    assert!(within(child.wait()).await?.success());
    Ok(())
}

/// A vectored write gives its buffers' bytes one after the other.
#[ruio::test]
async fn poll_write_vectored_writes_the_buffers_in_order() -> io::Result<()> {
    let mut child = cat().stdin(Stdio::piped()).stdout(Stdio::piped()).spawn()?;
    let mut stdin = child.stdin.take().unwrap();
    let parts: [&[u8]; 4] = [b"one ", b"", b"two ", b"three\r\n"];
    let bufs = parts.map(IoSlice::new);
    let written = within(poll_fn(|cx| stdin.poll_write_vectored(cx, &bufs))).await?;
    assert_eq!(written, 15, "the pipe's buffer takes them all");
    drop(stdin);
    let mut stdout = child.stdout.take().unwrap();
    let out = within(poll_read_to_end(&mut stdout)).await?;
    assert_eq!(out, b"one two three\r\n");
    assert!(within(child.wait()).await?.success());
    Ok(())
}

/// The calls made with no wait: a read before anything has arrived would
/// block, and one once the pipe is readable takes what arrived; a write
/// once the pipe is writable goes at once. `findstr` holds its output back
/// until its input ends, so the input is closed before the output is read.
#[ruio::test]
async fn try_read_and_try_write_wait_for_nothing() -> io::Result<()> {
    let mut child = cat().stdin(Stdio::piped()).stdout(Stdio::piped()).spawn()?;
    let stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut buf = [0; 64];
    let err = stdout.try_read(&mut buf).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::WouldBlock, "nothing has arrived");
    within(stdin.writable()).await?;
    assert_eq!(stdin.try_write(b"ping\r\n")?, 6);
    within(stdin.writable()).await?;
    let bufs = [IoSlice::new(b"pi"), IoSlice::new(b"ng\r\n")];
    assert_eq!(stdin.try_write_vectored(&bufs)?, 6);
    within(stdin.writable()).await?;
    drop(stdin);
    let mut out = Vec::new();
    loop {
        within(stdout.readable()).await?;
        // Readable, and read by nothing else, so there is something, or
        // the end.
        let read = stdout.try_read(&mut buf)?;
        if read == 0 {
            break;
        }
        out.extend_from_slice(&buf[..read]);
    }
    assert_eq!(out, b"ping\r\nping\r\n");
    assert!(within(child.wait()).await?.success());
    Ok(())
}

/// A poll read of a child's standard error, which waits for the child to
/// write, as standard output's does.
#[ruio::test]
async fn stderr_is_read_with_its_poll_method() -> io::Result<()> {
    let mut child = shell("ping -n 2 127.0.0.1 >nul & echo err>&2")
        .stderr(Stdio::piped())
        .spawn()?;
    let mut stderr = child.stderr.take().unwrap();
    let mut buf = [MaybeUninit::<u8>::uninit(); 16];
    let mut out = Vec::<u8>::new();
    loop {
        let read = within(poll_fn(|cx| stderr.poll_read(cx, &mut buf))).await?;
        if read == 0 {
            break;
        }
        // SAFETY: the read initialized the first `read` bytes.
        out.extend_from_slice(unsafe { std::slice::from_raw_parts(buf.as_ptr().cast(), read) });
    }
    assert_eq!(out, b"err\r\n");
    assert!(within(child.wait()).await?.success());
    Ok(())
}

#[test]
fn the_pipes_are_send_and_sync() {
    fn assert_send_sync<T: Send + Sync + Unpin>() {}
    assert_send_sync::<ChildStdin>();
    assert_send_sync::<ChildStdout>();
    assert_send_sync::<ChildStderr>();
    assert_send_sync::<Child>();
}

#[ruio::test]
async fn wait_with_output_reads_the_pipes_and_closes_the_input() -> io::Result<()> {
    let child = cat()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .as_ref()
        .unwrap()
        .write_all("piped\r\n")
        .await
        .0?;
    let output = within(child.wait_with_output()).await?;
    assert!(output.status.success());
    assert_eq!(
        (&output.stdout[..], &output.stderr[..]),
        (&b"piped\r\n"[..], &b""[..])
    );
    Ok(())
}

#[ruio::test]
async fn a_child_is_killed() -> io::Result<()> {
    let mut child = sleep(30).spawn()?;
    assert!(child.try_wait()?.is_none(), "still running");
    child.start_kill()?;
    let status = within(child.wait()).await?;
    assert_eq!(status.code(), Some(1), "the exit code of a process killed");
    // Killing what has exited does nothing.
    child.start_kill()?;
    let mut child = sleep(30).spawn()?;
    within(child.kill()).await?;
    assert_eq!(child.try_wait()?.unwrap().code(), Some(1));
    Ok(())
}

/// A child dropped with `kill_on_drop` is killed, and its output ends; one
/// dropped without it runs on to its end.
#[ruio::test]
async fn kill_on_drop_kills_a_child_dropped() -> io::Result<()> {
    let mut child = Command::new("ping")
        .args(["-n", "31", "127.0.0.1"])
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let stdout = child.stdout.take().unwrap();
    let start = Instant::now();
    drop(child);
    let (read, _) = within(stdout.read_to_end(Vec::new())).await;
    read?;
    assert!(
        start.elapsed() < Duration::from_secs(20),
        "killed, not run out"
    );

    let mut child = shell("ping -n 2 127.0.0.1 >nul & echo done")
        .stdout(Stdio::piped())
        .spawn()?;
    let stdout = child.stdout.take().unwrap();
    drop(child);
    let (read, out) = within(stdout.read_to_end(Vec::new())).await;
    read?;
    assert_eq!(out, b"done\r\n", "left to run");
    Ok(())
}

/// Writing to the input of a child that has exited fails. A child that
/// another test spawns at the same moment can hold a copy of the pipe's
/// reading end until it is done, so the write is tried until every copy is
/// gone.
#[ruio::test]
async fn writing_to_an_exited_child_fails() -> io::Result<()> {
    let mut child = shell("exit 0").stdin(Stdio::piped()).spawn()?;
    let stdin = child.stdin.take().unwrap();
    assert!(within(child.wait()).await?.success());
    let deadline = Instant::now() + PATIENCE;
    loop {
        let (written, _) = within(stdin.write_all("too late")).await;
        match written {
            Err(err) if err.kind() == ErrorKind::BrokenPipe => return Ok(()),
            Err(err) => return Err(err),
            Ok(()) => assert!(Instant::now() < deadline, "the pipe is still read"),
        }
        ruio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// A wait dropped before the child exits, under a timeout, leaves the
/// status to the next wait.
#[ruio::test]
async fn a_wait_cut_short_leaves_the_status_to_the_next() -> io::Result<()> {
    let mut child = shell("ping -n 2 127.0.0.1 >nul & exit 6").spawn()?;
    let waited = ruio::time::timeout(Duration::from_millis(20), child.wait()).await;
    assert!(waited.is_err(), "timed out");
    assert!(child.id().is_some());
    assert_eq!(within(child.wait()).await?.code(), Some(6));
    Ok(())
}

/// Where no runtime runs, a wait for a child still running has no port to
/// run on, and one for a child that has exited needs none.
#[test]
fn without_a_runtime_a_wait_needs_the_child_to_have_exited() {
    let mut child = sleep(30).kill_on_drop(true).spawn().unwrap();
    let err = futures_executor::block_on(child.wait()).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    child.start_kill().unwrap();
    let deadline = Instant::now() + PATIENCE;
    while child.try_wait().unwrap().is_none() {
        assert!(Instant::now() < deadline, "still running");
        std::thread::sleep(Duration::from_millis(5));
    }
    let status = futures_executor::block_on(child.wait()).unwrap();
    assert_eq!(status.code(), Some(1));
}

/// A child spawned on one core is waited for on another, and a wait that
/// began on one core moves to another while in flight.
#[test]
fn a_child_is_waited_for_on_any_core() {
    let outputs = two()
        .run(|core| async move {
            if core.index() != 0 {
                return None;
            }
            let mut child: Child = shell("exit 4").spawn().unwrap();
            let elsewhere = ruio::spawn_on(1, async move {
                let status = child.wait().await.unwrap();
                (status.code(), ruio::core().unwrap().index())
            });
            let waited = elsewhere.await.unwrap();

            let mut child = shell("ping -n 2 127.0.0.1 >nul & exit 8").spawn().unwrap();
            let mut wait = Box::pin(async move { child.wait().await });
            assert!(pending_once(&mut wait).await, "queued on this core");
            let moved = ruio::spawn_on(1, wait).await.unwrap().unwrap();

            let output = ruio::spawn_on(1, shell("echo far").output()).await.unwrap();
            Some((waited, moved.code(), output.unwrap().stdout))
        })
        .unwrap();
    let (waited, moved, stdout) = outputs.into_iter().next().flatten().unwrap();
    assert_eq!(waited, (Some(4), 1));
    assert_eq!(moved, Some(8));
    assert_eq!(stdout, b"far\r\n");
}
