//! Child processes: spawning them, with pipes to their standard streams,
//! waiting for them on the ring, and signalling them.
//!
//! [`Command`] builds a process as the standard library's does, and spawns
//! it the same way: with `posix_spawn(3)`, a clone that shares the parent's
//! memory until the child execs, unless something only a forked child can
//! do is asked for, [`pre_exec`](Command::pre_exec), a user, a group or a
//! `PATH` of the child's own for a bare program name, which forks. A
//! [`process_group`](Command::process_group) keeps the fast way. The
//! [`Child`] has a pidfd for the process, which names it and no other for
//! as long as the `Child` lives, so signalling it never reaches another
//! process that took its number.
//!
//! [`Child::wait`] waits on the ring of the thread that polls it, with an
//! `IORING_OP_WAITID` that the kernel completes as the child exits, and no
//! thread blocks or handles `SIGCHLD` meanwhile. The wait watches the child
//! rather than reap it (`WNOWAIT`), and the child is reaped with one
//! `waitid(2)` as the future completes: a wait dropped as the child exits,
//! under a timeout say, leaves the child and its status for the next. A
//! child whose `Child` is dropped before it has exited is reaped as it does
//! by a thread that polls the pidfds of such children, started as the first
//! is dropped, so that none lingers as a zombie.
//!
//! [`Child::signal_group`] signals the process group a child leads, as one
//! spawned with `process_group(0)` does, which holds what the child started
//! too: a shell's background jobs, say. It goes through the pidfd, so it
//! reaches that group alone even after the child itself has been reaped,
//! and nothing if the child leads no group. A command spawned with
//! [`kill_on_drop`](Command::kill_on_drop) has its group, if it leads one,
//! and the child killed as the `Child` is dropped.
//!
//! The pipes, [`ChildStdin`], [`ChildStdout`] and [`ChildStderr`], implement
//! [`AsyncRead`] and [`AsyncWrite`]. The parent's ends are nonblocking, which
//! they are alone in being, the child's ends being files of their own: a
//! read or a write is a system call, made at once, which takes what has
//! arrived, or gives what there is room for, without a turn of the ring's
//! loop; only one that finds the pipe empty, or full, waits, for the pipe to
//! be ready, with a poll on the ring of the thread that polls it. A read of
//! an empty pipe through the ring would fail in the kernel first, which then
//! arms a poll of its own and tries again: a request, a failure and a retry
//! where a child that answers at once is read with two system calls. Each
//! read and write that moves data spends a step of the task's budget
//! ([`coop`](crate::coop)), so a pipe a child keeps full cannot hold up the
//! runtime's other tasks.
//!
//! The pipes read into and write from owned buffers, and also, through
//! their poll methods, [`ChildStdout::poll_read`] and
//! [`ChildStdin::poll_write`] among them, into and from the caller's own,
//! with no copy, as the portable sockets do: what an adapter to the
//! poll-based I/O traits reads and writes through. A poll method that waits
//! keeps its wait in the pipe until it is polled again.
//!
//! A `Child` and every future here are `Send`, so code whose futures must
//! be, such as `async_trait`'s, can hold them, and a task can move between
//! cores with them; the pipes are `Send` and `Sync`.
//!
//! ```no_run
//! # ruio::Runtime::new().unwrap().block_on(async {
//! use std::process::Stdio;
//!
//! use ruio::io::AsyncRead;
//! use ruio::process::Command;
//!
//! let mut child = Command::new("sh")
//!     .args(["-c", "echo hello; sleep 1 & wait"])
//!     .stdout(Stdio::piped())
//!     .process_group(0)
//!     .kill_on_drop(true)
//!     .spawn()?;
//! let stdout = child.stdout.take().unwrap();
//! let (read, out) = stdout.read_to_end(Vec::new()).await;
//! read?;
//! assert_eq!(out, b"hello\n");
//! assert!(child.wait().await?.success());
//! # Ok::<(), std::io::Error>(())
//! # }).unwrap();
//! ```
//!
//! ```
//! fn assert_send<T: Send>(_: &T) {}
//!
//! async fn waits(child: &mut ruio::process::Child) -> std::io::Result<()> {
//!     let wait = child.wait();
//!     assert_send(&wait);
//!     wait.await.map(drop)
//! }
//! ```

use std::ffi::OsStr;
use std::fmt;
use std::future::{Future, poll_fn};
use std::io::{self, IoSlice};
use std::mem::{ManuallyDrop, MaybeUninit};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::Path;
use std::pin::pin;
use std::process::{ExitStatus, Output, Stdio};
use std::ptr;
use std::task::{Context, Poll};

use ruio_driver::buf::{IoBuf, IoBufMut};
use ruio_driver::ops::Waitid;
use ruio_driver::{PortableFd, ReadyWait};

use crate::io::{AsyncRead, AsyncWrite, Kind, Nonblocking, PortableRead, PortableWrite};

mod reaper;

/// The flag of `pidfd_send_signal(2)` that sends the signal to the process
/// group the pidfd's process leads (Linux 6.9).
const PIDFD_SIGNAL_PROCESS_GROUP: libc::c_uint = 1 << 2;

/// The events a read of a pipe waits for.
const READABLE: u32 = libc::POLLIN as u32;

/// The events a write to a pipe waits for.
const WRITABLE: u32 = libc::POLLOUT as u32;

/// A process to spawn: the standard library's [`Command`](std::process::Command),
/// with its child waited for on the ring.
///
/// Every setting is the standard library's, as are the defaults: the child
/// inherits the parent's standard streams unless told otherwise, except for
/// [`output`](Command::output).
pub struct Command {
    std: std::process::Command,
    kill_on_drop: bool,
    /// Whether stdin, stdout and stderr have been set, for the defaults of
    /// `output`.
    stdio_set: [bool; 3],
}

impl Command {
    /// A command to run `program`, found on the `PATH` if it is a bare
    /// name, as the standard library finds it.
    pub fn new(program: impl AsRef<OsStr>) -> Self {
        std::process::Command::new(program).into()
    }

    /// Adds an argument.
    pub fn arg(&mut self, arg: impl AsRef<OsStr>) -> &mut Self {
        self.std.arg(arg);
        self
    }

    /// Adds arguments.
    pub fn args<I, S>(&mut self, args: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.std.args(args);
        self
    }

    /// Sets an environment variable of the child.
    pub fn env(&mut self, key: impl AsRef<OsStr>, value: impl AsRef<OsStr>) -> &mut Self {
        self.std.env(key, value);
        self
    }

    /// Sets environment variables of the child.
    pub fn envs<I, K, V>(&mut self, vars: I) -> &mut Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        self.std.envs(vars);
        self
    }

    /// Leaves an environment variable out of the child's environment.
    pub fn env_remove(&mut self, key: impl AsRef<OsStr>) -> &mut Self {
        self.std.env_remove(key);
        self
    }

    /// Gives the child no environment but what is set after this.
    pub fn env_clear(&mut self) -> &mut Self {
        self.std.env_clear();
        self
    }

    /// Sets the child's working directory.
    pub fn current_dir(&mut self, dir: impl AsRef<Path>) -> &mut Self {
        self.std.current_dir(dir);
        self
    }

    /// Sets the child's standard input: [`Stdio::piped`] gives the `Child`
    /// a [`ChildStdin`] to write it with.
    pub fn stdin(&mut self, cfg: impl Into<Stdio>) -> &mut Self {
        self.std.stdin(cfg);
        self.stdio_set[0] = true;
        self
    }

    /// Sets the child's standard output: [`Stdio::piped`] gives the `Child`
    /// a [`ChildStdout`] to read it with.
    pub fn stdout(&mut self, cfg: impl Into<Stdio>) -> &mut Self {
        self.std.stdout(cfg);
        self.stdio_set[1] = true;
        self
    }

    /// Sets the child's standard error: [`Stdio::piped`] gives the `Child`
    /// a [`ChildStderr`] to read it with.
    pub fn stderr(&mut self, cfg: impl Into<Stdio>) -> &mut Self {
        self.std.stderr(cfg);
        self.stdio_set[2] = true;
        self
    }

    /// Whether dropping the `Child` before it has been reaped kills it: the
    /// process group it leads, if it leads one, with what that holds, as
    /// [`Child::signal_group`] reaches it, and the child itself. Off unless
    /// set, as for the standard library's children, which are left to run.
    pub fn kill_on_drop(&mut self, kill_on_drop: bool) -> &mut Self {
        self.kill_on_drop = kill_on_drop;
        self
    }

    /// Puts the child in the process group `pgroup`, or, if it is 0, in a
    /// group of its own, which it leads: what it starts joins the group,
    /// and [`Child::signal_group`] reaches them all. The child is spawned
    /// the fast way all the same.
    pub fn process_group(&mut self, pgroup: i32) -> &mut Self {
        self.std.process_group(pgroup);
        self
    }

    /// Runs the child as the user `id`, which makes it a forked one.
    pub fn uid(&mut self, id: u32) -> &mut Self {
        self.std.uid(id);
        self
    }

    /// Runs the child as the group `id`, which makes it a forked one.
    pub fn gid(&mut self, id: u32) -> &mut Self {
        self.std.gid(id);
        self
    }

    /// Sets the name the child sees itself run as, its `argv[0]`.
    pub fn arg0(&mut self, arg: impl AsRef<OsStr>) -> &mut Self {
        self.std.arg0(arg);
        self
    }

    /// Runs `f` in the child after it is forked and before it execs the
    /// program, as the standard library's `pre_exec` does: to call
    /// `setsid(2)`, say. The child is then always forked, which costs more
    /// than the clone it is otherwise spawned with, the more so the more
    /// memory the parent has mapped; [`process_group`](Command::process_group)
    /// is the cheaper way to a group of the child's own.
    ///
    /// # Safety
    ///
    /// As for [`CommandExt::pre_exec`]: `f` runs in a copy of the parent
    /// with one thread, where only what is async-signal-safe may be called.
    pub unsafe fn pre_exec<F>(&mut self, f: F) -> &mut Self
    where
        F: FnMut() -> io::Result<()> + Send + Sync + 'static,
    {
        // SAFETY: the caller's contract, which is the standard library's.
        unsafe { self.std.pre_exec(f) };
        self
    }

    /// The program the command runs.
    pub fn get_program(&self) -> &OsStr {
        self.std.get_program()
    }

    /// The standard library's command, as configured.
    pub fn as_std(&self) -> &std::process::Command {
        &self.std
    }

    /// The standard library's command, to configure what this has no method
    /// for.
    pub fn as_std_mut(&mut self) -> &mut std::process::Command {
        &mut self.std
    }

    /// Spawns the child, as the standard library does, and opens its pidfd.
    ///
    /// # Errors
    ///
    /// What spawning reports, such as `NotFound` for a program that is not
    /// there, and what `pidfd_open(2)` does; a child whose pidfd cannot be
    /// opened is killed and reaped.
    pub fn spawn(&mut self) -> io::Result<Child> {
        let mut child = self.std.spawn()?;
        let pid = child.id();
        let pidfd = match pidfd_open(pid) {
            Ok(pidfd) => pidfd,
            Err(err) => {
                // Not to be waited for here, it is not left running either.
                let _ = child.kill();
                let _ = child.wait();
                return Err(err);
            }
        };
        let mut spawned = Child {
            pid,
            pidfd: ManuallyDrop::new(PortableFd::new(pidfd)),
            status: None,
            kill_on_drop: self.kill_on_drop,
            stdin: None,
            stdout: None,
            stderr: None,
        };
        // A pipe that cannot be made nonblocking leaves the child to its
        // `Drop`, which reaps it, or has it killed.
        spawned.stdin = child.stdin.take().map(ChildStdin::try_from).transpose()?;
        spawned.stdout = child.stdout.take().map(ChildStdout::try_from).transpose()?;
        spawned.stderr = child.stderr.take().map(ChildStderr::try_from).transpose()?;
        Ok(spawned)
    }

    /// Spawns the child and waits for it, as [`Child::wait`] does. The child
    /// is spawned as this is called, not as the future is first polled.
    pub fn status(&mut self) -> impl Future<Output = io::Result<ExitStatus>> + Send + use<> {
        let child = self.spawn();
        async move { child?.wait().await }
    }

    /// Spawns the child, collects what it writes to its standard output and
    /// error, and waits for it, as [`Child::wait_with_output`] does. As for
    /// the standard library's `output`, its standard input is null and its
    /// output and error piped unless they are set; unlike it, they stay so
    /// for the command's later spawns. The child is spawned as this is
    /// called.
    pub fn output(&mut self) -> impl Future<Output = io::Result<Output>> + Send + use<> {
        if !self.stdio_set[0] {
            self.stdin(Stdio::null());
        }
        if !self.stdio_set[1] {
            self.stdout(Stdio::piped());
        }
        if !self.stdio_set[2] {
            self.stderr(Stdio::piped());
        }
        let child = self.spawn();
        async move { child?.wait_with_output().await }
    }
}

impl From<std::process::Command> for Command {
    fn from(std: std::process::Command) -> Self {
        Self {
            std,
            kill_on_drop: false,
            stdio_set: [false; 3],
        }
    }
}

impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.std.fmt(f)
    }
}

/// A spawned child process, which [`Command::spawn`] makes.
///
/// [`wait`](Child::wait) waits for it on the ring, and reaps it, after which
/// it can be waited for again, to the same status, and signalled no more.
/// Dropping it before then leaves the child to run, unless
/// [`kill_on_drop`](Command::kill_on_drop) says otherwise; the child is
/// reaped as it exits all the same.
pub struct Child {
    pid: u32,
    /// The pidfd, which names the child and no other process for as long as
    /// it is open. Taken only as the child is dropped.
    pidfd: ManuallyDrop<PortableFd>,
    /// The status, once the child has been reaped.
    status: Option<ExitStatus>,
    kill_on_drop: bool,
    /// The writing end of the pipe to the child's standard input, if it was
    /// piped.
    pub stdin: Option<ChildStdin>,
    /// The reading end of the pipe from the child's standard output, if it
    /// was piped.
    pub stdout: Option<ChildStdout>,
    /// The reading end of the pipe from the child's standard error, if it
    /// was piped.
    pub stderr: Option<ChildStderr>,
}

impl Child {
    /// The child's process ID, until it is reaped, when it may name another
    /// process.
    pub fn id(&self) -> Option<u32> {
        self.status.is_none().then_some(self.pid)
    }

    /// The child's pidfd, which names it alone, reaped or not, for as long
    /// as the `Child` lives.
    pub fn pidfd(&self) -> BorrowedFd<'_> {
        self.pidfd.as_fd()
    }

    /// Waits for the child to exit, and reaps it; the status, once known,
    /// is kept, and waiting again returns it. The pipe to the child's
    /// standard input is closed first, as the standard library closes it,
    /// so that a child that reads its input to the end can get there.
    ///
    /// The wait runs on the ring of the thread that polls it, or, polled
    /// where no runtime runs, on the foreign ring the embedder set
    /// ([`set_foreign_ring`](crate::uring::set_foreign_ring)); a wait dropped
    /// before it completes, as the child exits even, leaves the child to
    /// reap and its status to the next.
    ///
    /// # Errors
    ///
    /// What `waitid(2)` reports: `ECHILD` if something else in the process
    /// reaped the child, with `waitpid(-1)` say; and `ENOSYS`, whose kind is
    /// [`io::ErrorKind::Unsupported`], polled where no runtime runs and no
    /// foreign ring is set.
    pub async fn wait(&mut self) -> io::Result<ExitStatus> {
        if let Some(status) = self.status {
            return Ok(status);
        }
        drop(self.stdin.take());
        let pidfd = self.pidfd.as_raw_fd();
        loop {
            // Watched, not reaped: whatever becomes of this future, the child
            // and its status stay until a call that hands the status on.
            let watch = Waitid::pidfd(pidfd).options(libc::WEXITED | libc::WNOWAIT);
            self.pidfd.op(|_| watch).await?;
            if let Some(status) = self.try_wait()? {
                return Ok(status);
            }
        }
    }

    /// The child's status if it has exited, reaping it, and `None` if it
    /// is still running, without waiting.
    ///
    /// # Errors
    ///
    /// As for [`wait`](Child::wait).
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        if let Some(status) = self.status {
            return Ok(Some(status));
        }
        self.status = reap(self.pidfd.as_raw_fd())?;
        Ok(self.status)
    }

    /// Sends the child `SIGKILL`, without waiting for it to exit; nothing,
    /// if it has exited already.
    ///
    /// # Errors
    ///
    /// What `pidfd_send_signal(2)` reports.
    pub fn start_kill(&mut self) -> io::Result<()> {
        if self.status.is_some() {
            return Ok(());
        }
        match self.signal(libc::SIGKILL) {
            // Exited, and not reaped yet.
            Err(err) if err.raw_os_error() == Some(libc::ESRCH) => Ok(()),
            sent => sent,
        }
    }

    /// Kills the child, as [`start_kill`](Child::start_kill) does, and waits
    /// for it, as [`wait`](Child::wait) does.
    ///
    /// # Errors
    ///
    /// As for the two.
    pub async fn kill(&mut self) -> io::Result<()> {
        self.start_kill()?;
        self.wait().await.map(drop)
    }

    /// Sends the child `signal`, through its pidfd, which reaches the child
    /// and no other process that took its number.
    ///
    /// # Errors
    ///
    /// What `pidfd_send_signal(2)` reports: `ESRCH` once the child has
    /// exited.
    pub fn signal(&self, signal: i32) -> io::Result<()> {
        send_signal(&self.pidfd, signal, 0)
    }

    /// Sends `signal` to the process group the child leads, as one spawned
    /// with [`process_group(0)`](Command::process_group) does: the child
    /// and what it started, which joins its group unless it makes its own.
    /// The signal goes through the pidfd, so it reaches that group alone,
    /// even once the child has been reaped and others have its number.
    ///
    /// # Errors
    ///
    /// What `pidfd_send_signal(2)` reports: `ESRCH` if the group has no
    /// process left, or the child leads none, when no process is signalled,
    /// the parent's own group least of all.
    pub fn signal_group(&self, signal: i32) -> io::Result<()> {
        send_signal(&self.pidfd, signal, PIDFD_SIGNAL_PROCESS_GROUP)
    }

    /// Collects what the child writes to its standard output and error, if
    /// they are piped, until they are closed, both at once so that a child
    /// that fills one pipe while the other is read cannot stall; and waits
    /// for it, as [`wait`](Child::wait) does, closing its standard input
    /// first.
    ///
    /// # Errors
    ///
    /// What reading the pipes and waiting report.
    pub async fn wait_with_output(mut self) -> io::Result<Output> {
        drop(self.stdin.take());
        let (stdout, stderr) = (self.stdout.take(), self.stderr.take());
        let (stdout, stderr) = join(read_all(stdout.as_ref()), read_all(stderr.as_ref())).await;
        let status = self.wait().await?;
        Ok(Output {
            status,
            stdout: stdout?,
            stderr: stderr?,
        })
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        if self.kill_on_drop {
            // The group first, which it may outlive.
            let _ = self.signal_group(libc::SIGKILL);
            let _ = self.start_kill();
        }
        // SAFETY: taken once, here, and not used again.
        let pidfd = unsafe { ManuallyDrop::take(&mut self.pidfd) };
        match self.status {
            Some(_) => drop(pidfd),
            None => match reap(pidfd.as_raw_fd()) {
                Ok(Some(_)) => drop(pidfd),
                // Reaped as it exits, by a thread that waits for the
                // children dropped before they had: kept open until then.
                Ok(None) => reaper::adopt(pidfd.into_owned_fd()),
                // Reaped by something else.
                Err(_) => drop(pidfd),
            },
        }
    }
}

impl fmt::Debug for Child {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Child")
            .field("pid", &self.pid)
            .field("status", &self.status)
            .field("stdin", &self.stdin)
            .field("stdout", &self.stdout)
            .field("stderr", &self.stderr)
            .finish_non_exhaustive()
    }
}

/// Opens a pidfd for the child with process ID `pid`, which, unreaped,
/// cannot have given its number to another process.
fn pidfd_open(pid: u32) -> io::Result<OwnedFd> {
    let pid =
        libc::pid_t::try_from(pid).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    // SAFETY: pidfd_open takes a process ID and flags, and touches no memory.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let fd = RawFd::try_from(fd).map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
    // SAFETY: the descriptor is new, and nothing else owns it.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Sends `signal` through `pidfd` with `pidfd_send_signal(2)`'s `flags`.
fn send_signal(pidfd: &PortableFd, signal: i32, flags: libc::c_uint) -> io::Result<()> {
    // SAFETY: pidfd_send_signal reads no memory when given no siginfo.
    let sent = unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            pidfd.as_raw_fd(),
            signal,
            ptr::null::<libc::siginfo_t>(),
            flags,
        )
    };
    if sent < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Reaps the child that `pidfd` refers to if it has exited, with its status,
/// or says it has not, without waiting.
fn reap(pidfd: RawFd) -> io::Result<Option<ExitStatus>> {
    let mut info = MaybeUninit::<libc::siginfo_t>::zeroed();
    // SAFETY: waitid writes the one `siginfo_t` it is given.
    let waited = unsafe {
        libc::waitid(
            libc::P_PIDFD,
            pidfd.cast_unsigned(),
            info.as_mut_ptr(),
            libc::WEXITED | libc::WNOHANG,
        )
    };
    if waited < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: zeroed, and written by waitid.
    let info = unsafe { info.assume_init() };
    // A child that has not exited leaves `si_signo` zero.
    if info.si_signo != libc::SIGCHLD {
        return Ok(None);
    }
    // SAFETY: the kernel filled in the fields of a `SIGCHLD`'s `siginfo_t`.
    let status = unsafe { info.si_status() };
    // The status word as `waitpid(2)` encodes it, of an exit, which is all
    // that is waited for.
    let raw = match info.si_code {
        libc::CLD_EXITED => (status & 0xff) << 8,
        libc::CLD_DUMPED => (status & 0x7f) | 0x80,
        _ => status & 0x7f,
    };
    Ok(Some(ExitStatus::from_raw(raw)))
}

/// Everything `reader` has, to its end, or nothing if there is none.
async fn read_all<R: AsyncRead>(reader: Option<&R>) -> io::Result<Vec<u8>> {
    let Some(reader) = reader else {
        return Ok(Vec::new());
    };
    let (read, data) = reader.read_to_end(Vec::new()).await;
    read.map(|_| data)
}

/// Runs two futures at once, to both their ends.
async fn join<A: Future, B: Future>(a: A, b: B) -> (A::Output, B::Output) {
    let (mut a, mut b) = (pin!(a), pin!(b));
    let (mut a_out, mut b_out) = (None, None);
    poll_fn(|cx| {
        if a_out.is_none()
            && let Poll::Ready(out) = a.as_mut().poll(cx)
        {
            a_out = Some(out);
        }
        if b_out.is_none()
            && let Poll::Ready(out) = b.as_mut().poll(cx)
        {
            b_out = Some(out);
        }
        match (a_out.take(), b_out.take()) {
            (Some(a), Some(b)) => Poll::Ready((a, b)),
            (a, b) => {
                (a_out, b_out) = (a, b);
                Poll::Pending
            }
        }
    })
    .await
}

/// Writes to a child's standard input, with system calls made at once that
/// wait on the ring of the thread polling them only if the pipe is full;
/// [`AsyncWrite`] writes it all, and the poll methods write from the
/// caller's buffers. Dropping it closes the pipe, after any wait queued for
/// it on a ring, which the child reads as the end of its input.
pub struct ChildStdin {
    pipe: Nonblocking,
    /// The wait of `poll_write` and `poll_write_vectored`.
    wait: ReadyWait,
}

impl ChildStdin {
    /// Writes `buf`'s initialized bytes, or as many as the pipe has room
    /// for, waiting for room if it has none.
    pub fn write<B: IoBuf>(&self, buf: B) -> PortableWrite<'_, B> {
        self.pipe.write(buf)
    }

    /// Writes as much of the caller's `buf` as the pipe has room for, as
    /// [`write`](Self::write) does, polled: a system call made at once, and
    /// if there is no room, a wait the pipe keeps until it is polled again,
    /// so that the next poll goes on with it.
    ///
    /// A write that gives less than `buf` has filled the pipe, so the next
    /// one waits for room before it writes. Each write spends a step of the
    /// task's budget ([`coop`](crate::coop)). A write that finds the pipe
    /// full waits as
    /// [`PortableFd::poll_ready`](crate::uring::PortableFd::poll_ready)
    /// does. The wait of the poll methods is the pipe's own, which they
    /// borrow it mutably to keep; the owned-buffer writes have their own.
    pub fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        self.pipe.poll_write(cx, buf, &mut self.wait)
    }

    /// Writes as much of the caller's `bufs`, one after the other, as the
    /// pipe has room for, with one `writev(2)` of up to 1024 of them; see
    /// [`poll_write`](Self::poll_write).
    pub fn poll_write_vectored(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.pipe.poll_write_vectored(cx, bufs, &mut self.wait)
    }

    /// Writes as much of `buf` as the pipe has room for, with one system
    /// call and no wait: `WouldBlock` if there is none.
    ///
    /// # Errors
    ///
    /// What `write(2)` reports: `BrokenPipe` once nothing holds the pipe's
    /// reading end.
    pub fn try_write(&self, buf: &[u8]) -> io::Result<usize> {
        self.pipe.try_write(buf)
    }

    /// Writes as much of `bufs`, one after the other, as the pipe has room
    /// for, with one system call and no wait: `WouldBlock` if there is none.
    ///
    /// # Errors
    ///
    /// As for [`try_write`](Self::try_write).
    pub fn try_write_vectored(&self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        self.pipe.try_write_vectored(bufs)
    }

    /// Waits for the pipe to be writable: for room, or for its reading end
    /// to have gone, which a write then finds, with a poll on the ring of
    /// the thread polling it. Another writer may take the room first, so a
    /// [`try_write`](Self::try_write) after it can still find none.
    ///
    /// # Errors
    ///
    /// What the kernel reports for the poll.
    pub async fn writable(&self) -> io::Result<()> {
        self.pipe.ready(WRITABLE).await.map(drop)
    }

    /// The pipe's descriptor, blocking again, which then closes as an
    /// [`OwnedFd`] does.
    pub fn into_owned_fd(self) -> OwnedFd {
        self.pipe.into_owned_fd()
    }
}

impl AsyncWrite for ChildStdin {
    type Write<'a, B: IoBuf> = PortableWrite<'a, B>;

    #[inline]
    fn write<B: IoBuf>(&self, buf: B) -> Self::Write<'_, B> {
        ChildStdin::write(self, buf)
    }
}

/// Made nonblocking, as the `Child` makes its pipes.
impl TryFrom<std::process::ChildStdin> for ChildStdin {
    type Error = io::Error;

    fn try_from(stdin: std::process::ChildStdin) -> io::Result<Self> {
        Ok(Self {
            pipe: Nonblocking::new(stdin.into(), Kind::Pipe)?,
            wait: ReadyWait::new(),
        })
    }
}

/// The poll methods of the pipes a child writes to, on their `pipe` and
/// `wait`.
macro_rules! pipe_read_methods {
    () => {
        /// Reads into the caller's `buf` what has arrived, as
        /// [`read`](Self::read) does, polled: a system call made at once, and
        /// if nothing has arrived, a wait the pipe keeps until it is polled
        /// again, so that the next poll goes on with it. The first bytes of
        /// `buf`, as many as were read, are then initialized; 0 bytes are
        /// read at the end.
        ///
        /// A read that takes less than `buf` has drained the pipe, so the
        /// next one waits for the pipe to be readable before it reads. Each
        /// read spends a step of the task's budget ([`coop`](crate::coop)).
        /// A read that finds the pipe empty waits as
        /// [`PortableFd::poll_ready`](crate::uring::PortableFd::poll_ready)
        /// does. The wait of the poll method is the pipe's own, which it
        /// borrows the pipe mutably to keep; the owned-buffer reads have
        /// their own.
        pub fn poll_read(
            &mut self,
            cx: &mut Context<'_>,
            buf: &mut [MaybeUninit<u8>],
        ) -> Poll<io::Result<usize>> {
            self.pipe.poll_read(cx, buf, &mut self.wait)
        }

        /// Reads into `buf` what has arrived, with one system call and no
        /// wait: `WouldBlock` if nothing has, 0 bytes at the end.
        ///
        /// # Errors
        ///
        /// What `read(2)` reports.
        pub fn try_read(&self, buf: &mut [u8]) -> io::Result<usize> {
            // SAFETY: the read writes none but initialized bytes, so the
            // bytes stay initialized.
            let buf = unsafe { &mut *(ptr::from_mut(buf) as *mut [MaybeUninit<u8>]) };
            self.pipe.try_read(buf)
        }

        /// Waits for the pipe to be readable: for data, or the end, to have
        /// arrived, which a read then takes, with a poll on the ring of the
        /// thread polling it. Another reader may take it first, so a
        /// [`try_read`](Self::try_read) after it can still find nothing.
        ///
        /// # Errors
        ///
        /// What the kernel reports for the poll.
        pub async fn readable(&self) -> io::Result<()> {
            self.pipe.ready(READABLE).await.map(drop)
        }
    };
}

/// Reads a child's standard output, with system calls made at once that
/// wait on the ring of the thread polling them only if nothing has arrived;
/// [`AsyncRead`] reads it to its end, and the poll methods into the caller's
/// buffers.
pub struct ChildStdout {
    pipe: Nonblocking,
    /// The wait of `poll_read`.
    wait: ReadyWait,
}

impl ChildStdout {
    /// Reads into `buf`, up to its capacity, what has arrived, waiting for
    /// something to if nothing has; 0 bytes at the end, once the child, and
    /// whatever else holds the pipe, has closed it.
    pub fn read<B: IoBufMut>(&self, buf: B) -> PortableRead<'_, B> {
        self.pipe.read(buf)
    }

    pipe_read_methods!();

    /// The pipe's descriptor, blocking again, which then closes as an
    /// [`OwnedFd`] does.
    pub fn into_owned_fd(self) -> OwnedFd {
        self.pipe.into_owned_fd()
    }
}

impl AsyncRead for ChildStdout {
    type Read<'a, B: IoBufMut> = PortableRead<'a, B>;

    #[inline]
    fn read<B: IoBufMut>(&self, buf: B) -> Self::Read<'_, B> {
        ChildStdout::read(self, buf)
    }
}

/// Made nonblocking, as the `Child` makes its pipes.
impl TryFrom<std::process::ChildStdout> for ChildStdout {
    type Error = io::Error;

    fn try_from(stdout: std::process::ChildStdout) -> io::Result<Self> {
        Ok(Self {
            pipe: Nonblocking::new(stdout.into(), Kind::Pipe)?,
            wait: ReadyWait::new(),
        })
    }
}

/// Reads a child's standard error, as [`ChildStdout`] reads its output.
pub struct ChildStderr {
    pipe: Nonblocking,
    /// The wait of `poll_read`.
    wait: ReadyWait,
}

impl ChildStderr {
    /// Reads into `buf`, up to its capacity; see [`ChildStdout::read`].
    pub fn read<B: IoBufMut>(&self, buf: B) -> PortableRead<'_, B> {
        self.pipe.read(buf)
    }

    pipe_read_methods!();

    /// The pipe's descriptor, blocking again, which then closes as an
    /// [`OwnedFd`] does.
    pub fn into_owned_fd(self) -> OwnedFd {
        self.pipe.into_owned_fd()
    }
}

impl AsyncRead for ChildStderr {
    type Read<'a, B: IoBufMut> = PortableRead<'a, B>;

    #[inline]
    fn read<B: IoBufMut>(&self, buf: B) -> Self::Read<'_, B> {
        ChildStderr::read(self, buf)
    }
}

/// Made nonblocking, as the `Child` makes its pipes.
impl TryFrom<std::process::ChildStderr> for ChildStderr {
    type Error = io::Error;

    fn try_from(stderr: std::process::ChildStderr) -> io::Result<Self> {
        Ok(Self {
            pipe: Nonblocking::new(stderr.into(), Kind::Pipe)?,
            wait: ReadyWait::new(),
        })
    }
}

macro_rules! pipe_fd_impls {
    ($($pipe:ident),*) => {$(
        impl AsFd for $pipe {
            fn as_fd(&self) -> BorrowedFd<'_> {
                self.pipe.fd().as_fd()
            }
        }

        impl AsRawFd for $pipe {
            fn as_raw_fd(&self) -> RawFd {
                self.pipe.fd().as_raw_fd()
            }
        }

        impl fmt::Debug for $pipe {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct(stringify!($pipe))
                    .field("fd", &self.pipe.fd().as_raw_fd())
                    .finish()
            }
        }
    )*};
}

pipe_fd_impls!(ChildStdin, ChildStdout, ChildStderr);
