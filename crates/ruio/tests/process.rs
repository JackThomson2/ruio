//! Child processes inside the runtime.
//!
//! Tests run at once in one process, so each signals and waits for its own
//! children only.

mod support;

use std::future::{Future, poll_fn};
use std::io::{self, ErrorKind};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::process::ExitStatusExt;
use std::pin::Pin;
use std::process::Stdio;
use std::task::Poll;
use std::time::{Duration, Instant};

use ruio::io::{AsyncRead, AsyncWrite};
use ruio::process::{Child, ChildStdout, Command};

use support::{PATIENCE, within};

fn assert_send<T: Send>(_: &T) {}

fn shell(script: &str) -> Command {
    let mut command = Command::new("sh");
    command.args(["-c", script]);
    command
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

/// A pidfd for the process with ID `pid`.
fn pidfd(pid: u32) -> OwnedFd {
    // SAFETY: pidfd_open takes a process ID and flags, and touches no memory.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    assert!(fd >= 0, "pidfd_open: {}", io::Error::last_os_error());
    // SAFETY: the descriptor is new, and nothing else owns it.
    unsafe { OwnedFd::from_raw_fd(i32::try_from(fd).unwrap()) }
}

/// Whether the process `pidfd` refers to exits within `PATIENCE`.
fn exits(pidfd: &OwnedFd) -> bool {
    let mut poll = libc::pollfd {
        fd: pidfd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let timeout = i32::try_from(PATIENCE.as_millis()).unwrap();
    // SAFETY: poll writes the one `pollfd` it is given.
    unsafe { libc::poll(&mut poll, 1, timeout) == 1 }
}

/// Whether the child with process ID `pid` is still to be reaped.
fn unreaped(pid: u32) -> bool {
    let mut info = std::mem::MaybeUninit::<libc::siginfo_t>::zeroed();
    // SAFETY: waitid writes the one `siginfo_t` it is given.
    let waited = unsafe {
        libc::waitid(
            libc::P_PID,
            pid,
            info.as_mut_ptr(),
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    waited == 0
}

/// The first line the child writes, a process ID of the script's.
async fn first_line_pid(stdout: &ChildStdout) -> u32 {
    let mut line = Vec::new();
    while !line.contains(&b'\n') {
        let (read, buf) = within(stdout.read(Vec::with_capacity(64))).await;
        assert_ne!(read.unwrap(), 0, "the output ended without a line");
        line.extend_from_slice(&buf);
    }
    let line = String::from_utf8(line).unwrap();
    line.lines().next().unwrap().trim().parse().unwrap()
}

#[ruio::test]
async fn a_child_is_spawned_and_waited_for() -> io::Result<()> {
    let status = within(shell("exit 3").status()).await?;
    assert_eq!(status.code(), Some(3));
    let mut child = shell("exit 0").spawn()?;
    let pid = child.id().unwrap();
    let wait = child.wait();
    assert_send(&wait);
    assert!(within(wait).await?.success());
    assert_eq!(child.id(), None, "reaped");
    assert!(!unreaped(pid));
    // The status is kept.
    assert!(within(child.wait()).await?.success());
    assert!(child.try_wait()?.unwrap().success());
    let err = Command::new("/nonexistent/program").spawn().unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    Ok(())
}

#[ruio::test]
async fn output_collects_both_streams_with_no_input() -> io::Result<()> {
    let output = shell("cat; echo out; echo err >&2; exit 2").output();
    assert_send(&output);
    let output = within(output).await?;
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(
        output.stdout, b"out\n",
        "the input is null, so cat ends at once"
    );
    assert_eq!(output.stderr, b"err\n");
    // Streams set explicitly stay as they are.
    let output = within(shell("echo out").stdout(Stdio::null()).output()).await?;
    assert!(output.stdout.is_empty());
    Ok(())
}

/// A megabyte through `cat`, written by one task while another reads, so
/// that neither pipe's buffer stalls the child.
#[ruio::test]
async fn data_goes_through_the_childs_pipes() -> io::Result<()> {
    let data: Vec<u8> = (0..1 << 20).map(|index| (index % 251) as u8).collect();
    let mut child = Command::new("cat")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    let stdin = child.stdin.take().unwrap();
    let expected = data.clone();
    let writer = ruio::spawn(async move {
        let (written, _) = stdin.write_all(data).await;
        // Dropped here, which closes the pipe: cat's input ends.
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

#[ruio::test]
async fn wait_with_output_reads_the_pipes_and_closes_the_input() -> io::Result<()> {
    let child = Command::new("cat")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    child.stdin.as_ref().unwrap().write_all("piped").await.0?;
    let output = within(child.wait_with_output()).await?;
    assert!(output.status.success());
    assert_eq!(
        (&output.stdout[..], &output.stderr[..]),
        (&b"piped"[..], &b""[..])
    );
    Ok(())
}

#[ruio::test]
async fn a_child_is_killed() -> io::Result<()> {
    let mut child = shell("sleep 30").spawn()?;
    assert!(child.try_wait()?.is_none(), "still running");
    child.start_kill()?;
    let status = within(child.wait()).await?;
    assert_eq!(status.signal(), Some(libc::SIGKILL));
    // Killing what has exited does nothing.
    child.start_kill()?;
    let mut child = shell("sleep 30").spawn()?;
    within(child.kill()).await?;
    assert_eq!(child.try_wait()?.unwrap().signal(), Some(libc::SIGKILL));
    let mut child = shell("sleep 30").spawn()?;
    child.signal(libc::SIGTERM)?;
    assert_eq!(within(child.wait()).await?.signal(), Some(libc::SIGTERM));
    let err = child.signal(libc::SIGTERM).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ESRCH), "reaped");
    Ok(())
}

/// Writing to the input of a child that has exited fails, rather than
/// raising `SIGPIPE`. A child that another test spawns at the same moment
/// can hold a copy of the pipe's reading end until it execs, so the write
/// is tried until every copy is gone.
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

/// A wait dropped before the child exits, under a timeout, leaves the child
/// and its status to the next wait.
#[ruio::test]
async fn a_wait_cut_short_leaves_the_status_to_the_next() -> io::Result<()> {
    let mut child = shell("sleep 0.2; exit 6").spawn()?;
    let waited = ruio::time::timeout(Duration::from_millis(20), child.wait()).await;
    assert!(waited.is_err(), "timed out");
    assert!(child.id().is_some());
    assert_eq!(within(child.wait()).await?.code(), Some(6));
    Ok(())
}

/// A child spawned in a group of its own leads it, and its group, its
/// background job with it, is signalled as one.
#[ruio::test]
async fn a_childs_group_is_signalled_through_its_pidfd() -> io::Result<()> {
    let mut child = shell("sleep 30 & echo $!; wait")
        .stdout(Stdio::piped())
        .process_group(0)
        .spawn()?;
    let pid = child.id().unwrap();
    // SAFETY: getpgid reads a process's group.
    let pgid = unsafe { libc::getpgid(pid.cast_signed()) };
    assert_eq!(pgid, pid.cast_signed(), "it leads a group of its own");
    let job = pidfd(first_line_pid(child.stdout.as_ref().unwrap()).await);
    child.signal_group(libc::SIGTERM)?;
    assert_eq!(within(child.wait()).await?.signal(), Some(libc::SIGTERM));
    assert!(exits(&job), "the background job went with its group");
    Ok(())
}

/// A child that leads no group has none to signal, and the parent's own is
/// not signalled in its place: the signal is one that would do nothing to
/// the tests if it were.
#[ruio::test]
async fn a_child_without_a_group_has_none_to_signal() -> io::Result<()> {
    let mut child = shell("sleep 30").spawn()?;
    let err = child.signal_group(libc::SIGWINCH).unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ESRCH));
    within(child.kill()).await?;
    Ok(())
}

/// A child dropped with `kill_on_drop` is killed with its group, the
/// background job it started among them, even once the child itself has
/// gone.
#[ruio::test]
async fn kill_on_drop_kills_the_group_a_child_leads() -> io::Result<()> {
    let child = shell("sleep 30 & echo $!; wait")
        .stdout(Stdio::piped())
        .process_group(0)
        .kill_on_drop(true)
        .spawn()?;
    let job = pidfd(first_line_pid(child.stdout.as_ref().unwrap()).await);
    let leader = pidfd(child.id().unwrap());
    drop(child);
    assert!(exits(&leader));
    assert!(exits(&job), "the background job went with its group");

    // A leader reaped first: its job is killed all the same.
    let mut child = shell("sleep 30 & echo $!")
        .stdout(Stdio::piped())
        .process_group(0)
        .kill_on_drop(true)
        .spawn()?;
    let job = pidfd(first_line_pid(child.stdout.as_ref().unwrap()).await);
    assert!(within(child.wait()).await?.success());
    drop(child);
    assert!(exits(&job));
    Ok(())
}

/// A child dropped while it runs is reaped as it exits, by the reaper.
#[ruio::test]
async fn a_dropped_child_is_reaped_as_it_exits() -> io::Result<()> {
    let child = shell("sleep 0.05").spawn()?;
    let pid = child.id().unwrap();
    drop(child);
    let deadline = Instant::now() + PATIENCE;
    while unreaped(pid) {
        assert!(Instant::now() < deadline, "still a zombie");
        ruio::time::sleep(Duration::from_millis(5)).await;
    }
    Ok(())
}

#[ruio::test]
async fn pre_exec_runs_in_the_child() -> io::Result<()> {
    let mut command = shell("exit 0");
    let new_session = || {
        // SAFETY: setsid makes a session, and touches no memory.
        if unsafe { libc::setsid() } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    };
    // SAFETY: the closure calls setsid alone, which is async-signal-safe.
    unsafe { command.pre_exec(new_session) };
    let mut child = command.spawn()?;
    let pid = child.id().unwrap();
    // SAFETY: getsid reads a process's session.
    let sid = unsafe { libc::getsid(pid.cast_signed()) };
    assert_eq!(sid, pid.cast_signed(), "it leads a session of its own");
    assert!(within(child.wait()).await?.success());

    let mut command = shell("exit 0");
    // SAFETY: the closure calls nothing.
    unsafe { command.pre_exec(|| Err(io::Error::from_raw_os_error(libc::EPERM))) };
    let err = command.spawn().unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EPERM));
    Ok(())
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

            let mut child = shell("sleep 0.05; exit 8").spawn().unwrap();
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
    assert_eq!(stdout, b"far\n");
}
