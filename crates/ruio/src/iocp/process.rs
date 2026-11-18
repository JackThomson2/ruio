//! Child processes: spawning them, with pipes to their standard streams,
//! and waiting for them on the port.
//!
//! This is ruio's `process` on the I/O completion port engine, under the
//! names and with the signatures it has on io_uring, so that code written
//! to it builds for both, but for what Linux alone has: signals, pidfds and
//! process groups, users and groups, `arg0` and `pre_exec`. Windows has
//! [`creation_flags`](Command::creation_flags) and
//! [`raw_arg`](Command::raw_arg) instead.
//!
//! [`Command`] builds a process as the standard library's does, and spawns
//! it the same way, with `CreateProcessW`. The [`Child`] holds the
//! process's handle, which names it and no other for as long as the
//! `Child` lives, so killing it never reaches another process that took
//! its number; a process that has exited leaves nothing behind to reap
//! once its last handle closes.
//!
//! [`Child::wait`] waits on the port of the thread that polls it for the
//! process's handle to be signaled, as it is once the process has exited,
//! with a wait completion packet that the system posts to the port then,
//! and no thread blocks meanwhile. A wait dropped before it completes, under
//! a timeout say, takes its packet back, and leaves the status to the next.
//! Polled where no runtime runs, a wait for a child still running has no
//! port to run on and fails with [`io::ErrorKind::Unsupported`], as the
//! pipes' waits do: the foreign reactor takes sockets alone.
//!
//! The pipes, [`ChildStdin`], [`ChildStdout`] and [`ChildStderr`], implement
//! [`AsyncRead`] and [`AsyncWrite`]. The parent's ends are opened for
//! overlapped I/O, as the standard library opens them, and no port has
//! them. A pipe gives no sign that it has data, or room, short of an
//! operation that completes, so each keeps a read, or a write, of its own in
//! flight, with a buffer of 64 KiB, as mio's pipes do: a read takes what the
//! pipe's read brought, with no call to the system unless the next read is
//! to start; a write copies the caller's bytes into the pipe's buffer and
//! starts writing them, done as far as the caller can tell, and the next
//! write waits for it to finish, and fails with its error if it failed.
//! Only a read that finds nothing brought, or a write that finds the last
//! still in flight, waits, for that operation to complete, on the port of
//! the thread that polls it. Each read and write that moves data spends a
//! step of the task's budget ([`coop`](crate::coop)), so a pipe a child
//! keeps full cannot hold up the runtime's other tasks.
//!
//! The pipes read into and write from owned buffers, and also, through
//! their poll methods, [`ChildStdout::poll_read`] and
//! [`ChildStdin::poll_write`] among them, into and from the caller's own,
//! as the portable sockets do: what an adapter to the poll-based I/O traits
//! reads and writes through. A poll method that waits keeps its wait in the
//! pipe until it is polled again.
//!
//! A `Child` and every future here are `Send`, so code whose futures must
//! be, such as `async_trait`'s, can hold them, and a task can move between
//! cores with them; the pipes are `Send` and `Sync`.
//!
//! ```
//! # ruio::Runtime::new().unwrap().block_on(async {
//! use std::process::Stdio;
//!
//! use ruio::io::AsyncRead;
//! use ruio::process::Command;
//!
//! let mut child = Command::new("cmd")
//!     .args(["/d", "/c", "echo", "hello"])
//!     .stdout(Stdio::piped())
//!     .kill_on_drop(true)
//!     .spawn()?;
//! let stdout = child.stdout.take().unwrap();
//! let (read, out) = stdout.read_to_end(Vec::new()).await;
//! read?;
//! assert_eq!(out, b"hello\r\n");
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
use std::mem::MaybeUninit;
use std::os::windows::io::{AsHandle, AsRawHandle, BorrowedHandle, OwnedHandle, RawHandle};
use std::os::windows::process::CommandExt;
use std::path::Path;
use std::pin::pin;
use std::process::{ExitStatus, Output, Stdio};
use std::ptr;
use std::task::{Context, Poll};

use ruio_iocp::portable::{HandleWait, Pipe};

use crate::buf::{IoBuf, IoBufMut};
use crate::io::{
    AsyncRead, AsyncWrite, Nonblocking, PortableRead, PortableWrite, READABLE, WRITABLE, Wait,
};

/// A process to spawn: the standard library's [`Command`](std::process::Command),
/// with its child waited for on the port.
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
    /// A command to run `program`, found as the standard library finds it:
    /// on the `PATH`, with `.exe` added, if it is a bare name.
    pub fn new(program: impl AsRef<OsStr>) -> Self {
        std::process::Command::new(program).into()
    }

    /// Adds an argument, quoted on the command line as the standard
    /// library quotes it.
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

    /// Adds `arg` to the command line as it is, with no quoting, as the
    /// standard library's [`CommandExt::raw_arg`] does: for a program that
    /// reads its command line its own way, as `cmd.exe` does.
    pub fn raw_arg(&mut self, arg: impl AsRef<OsStr>) -> &mut Self {
        self.std.raw_arg(arg);
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

    /// Whether dropping the `Child` before its status is known kills it, as
    /// [`Child::start_kill`] does: the child alone, and not what it started.
    /// Off unless set, as for the standard library's children, which are
    /// left to run.
    pub fn kill_on_drop(&mut self, kill_on_drop: bool) -> &mut Self {
        self.kill_on_drop = kill_on_drop;
        self
    }

    /// Sets the flags `CreateProcessW` takes, as the standard library's
    /// [`CommandExt::creation_flags`] does: `CREATE_NO_WINDOW`,
    /// `0x0800_0000`, to run a console program with no console window, say,
    /// or `CREATE_NEW_PROCESS_GROUP`, `0x200`, to put the child in a group
    /// of its own, which the console's Ctrl+C does not reach.
    pub fn creation_flags(&mut self, flags: u32) -> &mut Self {
        self.std.creation_flags(flags);
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

    /// Spawns the child, as the standard library does.
    ///
    /// # Errors
    ///
    /// What spawning reports, such as `NotFound` for a program that is not
    /// there.
    pub fn spawn(&mut self) -> io::Result<Child> {
        let mut child = self.std.spawn()?;
        Ok(Child {
            stdin: child.stdin.take().map(ChildStdin::from_std),
            stdout: child.stdout.take().map(ChildStdout::from_std),
            stderr: child.stderr.take().map(ChildStderr::from_std),
            std: child,
            status: None,
            kill_on_drop: self.kill_on_drop,
        })
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
/// [`wait`](Child::wait) waits for it on the port, and keeps its status,
/// after which it can be waited for again, to the same status. Dropping it
/// before then leaves the child to run, unless
/// [`kill_on_drop`](Command::kill_on_drop) says otherwise.
pub struct Child {
    /// The standard library's child, whose handle names the process, and
    /// whose pipes have been taken.
    std: std::process::Child,
    /// The status, once the child has been found to have exited.
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
    /// The child's process ID, until its status is known, as on Linux,
    /// where a child reaped gives its number up. Here the number stays the
    /// child's for as long as the `Child` holds its handle.
    pub fn id(&self) -> Option<u32> {
        self.status.is_none().then(|| self.std.id())
    }

    /// Waits for the child to exit; the status, once known, is kept, and
    /// waiting again returns it. The pipe to the child's standard input is
    /// closed first, as the standard library closes it, so that a child
    /// that reads its input to the end can get there.
    ///
    /// The wait runs on the port of the thread that polls it, for the
    /// process's handle to be signaled; a wait dropped before it completes
    /// leaves the status to the next.
    ///
    /// # Errors
    ///
    /// What the system reports for the process, and, polled where no
    /// runtime runs while the child is still running, an error of kind
    /// [`io::ErrorKind::Unsupported`].
    pub async fn wait(&mut self) -> io::Result<ExitStatus> {
        if let Some(status) = self.status {
            return Ok(status);
        }
        drop(self.stdin.take());
        let mut wait = HandleWait::new();
        loop {
            if let Some(status) = self.try_wait()? {
                return Ok(status);
            }
            let process = self.std.as_handle();
            poll_fn(|cx| wait.poll_wait(cx, process)).await?;
        }
    }

    /// The child's status if it has exited, and `None` if it is still
    /// running, without waiting.
    ///
    /// # Errors
    ///
    /// What the system reports for the process.
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        if let Some(status) = self.status {
            return Ok(Some(status));
        }
        self.status = self.std.try_wait()?;
        Ok(self.status)
    }

    /// Terminates the child, with `TerminateProcess`, whose exit code it
    /// then has is 1, without waiting for it to exit; nothing, if it has
    /// exited already. What the child started runs on.
    ///
    /// # Errors
    ///
    /// What `TerminateProcess` reports.
    pub fn start_kill(&mut self) -> io::Result<()> {
        if self.status.is_some() {
            return Ok(());
        }
        self.std.kill()
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
        if self.kill_on_drop && self.status.is_none() {
            let _ = self.std.kill();
        }
    }
}

/// The process's handle, which names the child and no other process for as
/// long as the `Child` lives.
impl AsHandle for Child {
    fn as_handle(&self) -> BorrowedHandle<'_> {
        self.std.as_handle()
    }
}

impl AsRawHandle for Child {
    fn as_raw_handle(&self) -> RawHandle {
        self.std.as_raw_handle()
    }
}

impl fmt::Debug for Child {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Child")
            .field("pid", &self.std.id())
            .field("status", &self.status)
            .field("stdin", &self.stdin)
            .field("stdout", &self.stdout)
            .field("stderr", &self.stderr)
            .finish_non_exhaustive()
    }
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

/// Writes to a child's standard input, with writes started at once, each of
/// a copy of up to 64 KiB of the caller's bytes, that wait on the port of
/// the thread polling them only while the last is in flight; [`AsyncWrite`]
/// writes it all, and the poll methods write from the caller's buffers.
/// Dropping it closes the pipe once its last write has finished, which the
/// child reads as the end of its input.
pub struct ChildStdin {
    pipe: Nonblocking,
    /// The wait of `poll_write` and `poll_write_vectored`.
    wait: Wait,
}

impl ChildStdin {
    /// Takes over the parent's end of the pipe, which the standard library
    /// opens for overlapped I/O, and no port has.
    fn from_std(stdin: std::process::ChildStdin) -> Self {
        Self {
            pipe: Nonblocking::pipe(Pipe::new(OwnedHandle::from(stdin))),
            wait: Wait::new(),
        }
    }

    /// Writes `buf`'s initialized bytes, or as many as the pipe's buffer
    /// takes, waiting for the last write to finish if it has not.
    pub fn write<B: IoBuf>(&self, buf: B) -> PortableWrite<'_, B> {
        self.pipe.write(buf)
    }

    /// Writes as much of the caller's `buf` as the pipe's buffer takes, as
    /// [`write`](Self::write) does, polled: a write started at once, and if
    /// the last is still in flight, a wait the pipe keeps until it is polled
    /// again, so that the next poll goes on with it.
    ///
    /// A write that takes less than `buf` has filled the pipe's buffer, so
    /// the next one waits for it to be written before it writes. Each write
    /// spends a step of the task's budget ([`coop`](crate::coop)). A write
    /// waits on the port of the thread polling it. The wait of the poll
    /// methods is the pipe's own, which they borrow it mutably to keep; the
    /// owned-buffer writes have their own.
    ///
    /// # Errors
    ///
    /// As for [`try_write`](Self::try_write), and, for a wait where no
    /// runtime runs, an error of kind [`io::ErrorKind::Unsupported`].
    pub fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        self.pipe.poll_write(cx, buf, &mut self.wait)
    }

    /// Writes as much of the caller's `bufs`, one after the other, as the
    /// pipe's buffer takes; see [`poll_write`](Self::poll_write).
    ///
    /// # Errors
    ///
    /// As for [`poll_write`](Self::poll_write).
    pub fn poll_write_vectored(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.pipe.poll_write_vectored(cx, bufs, &mut self.wait)
    }

    /// Writes as much of `buf` as the pipe's buffer takes, up to 64 KiB, by
    /// copying it there and starting a write of it, with no wait:
    /// `WouldBlock` while the last write is in flight.
    ///
    /// # Errors
    ///
    /// `WouldBlock`; what starting the write reports, `BrokenPipe` once
    /// nothing holds the pipe's reading end; and what the last write failed
    /// with, once.
    pub fn try_write(&self, buf: &[u8]) -> io::Result<usize> {
        self.pipe.try_write(buf)
    }

    /// Writes as much of `bufs`, one after the other, as the pipe's buffer
    /// takes, with no wait, as [`try_write`](Self::try_write) does.
    ///
    /// # Errors
    ///
    /// As for [`try_write`](Self::try_write).
    pub fn try_write_vectored(&self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        self.pipe.try_write_vectored(bufs)
    }

    /// Waits for the pipe to be writable: for its last write, if one is in
    /// flight, to have finished, on the port of the thread polling it.
    /// Another writer may start the next first, so a
    /// [`try_write`](Self::try_write) after it can still find no room.
    ///
    /// # Errors
    ///
    /// What the system reports for the wait, and, where no runtime runs
    /// while a write is in flight, an error of kind
    /// [`io::ErrorKind::Unsupported`].
    pub async fn writable(&self) -> io::Result<()> {
        self.pipe.ready(WRITABLE).await.map(drop)
    }

    /// The pipe's handle, with its write in flight, if one is, cancelled,
    /// so that what it held may not reach the child: wait until the pipe is
    /// [`writable`](Self::writable) first to have it all written.
    pub fn into_owned_handle(self) -> OwnedHandle {
        self.pipe.into_owned_handle()
    }
}

impl AsyncWrite for ChildStdin {
    type Write<'a, B: IoBuf> = PortableWrite<'a, B>;

    #[inline]
    fn write<B: IoBuf>(&self, buf: B) -> Self::Write<'_, B> {
        ChildStdin::write(self, buf)
    }
}

/// Takes over the standard library's end of the pipe, as the `Child` takes
/// its pipes over: opened for overlapped I/O, and associated with no port,
/// as the standard library opens it. It never fails, as it may where the
/// pipe is to be made nonblocking.
impl TryFrom<std::process::ChildStdin> for ChildStdin {
    type Error = io::Error;

    fn try_from(stdin: std::process::ChildStdin) -> io::Result<Self> {
        Ok(Self::from_std(stdin))
    }
}

/// The poll methods of the pipes a child writes to, on their `pipe` and
/// `wait`.
macro_rules! pipe_read_methods {
    () => {
        /// Reads into the caller's `buf` what has arrived, as
        /// [`read`](Self::read) does, polled: a read of what the pipe's read
        /// in flight brought, and if it brought nothing yet, a wait the pipe
        /// keeps until it is polled again, so that the next poll goes on
        /// with it. The first bytes of `buf`, as many as were read, are then
        /// initialized; 0 bytes are read at the end.
        ///
        /// A read that takes less than `buf` has taken all the pipe's read
        /// brought, so the next one waits for the pipe's next read before it
        /// reads. Each read spends a step of the task's budget
        /// ([`coop`](crate::coop)). A read waits on the port of the thread
        /// polling it. The wait of the poll method is the pipe's own, which
        /// it borrows the pipe mutably to keep; the owned-buffer reads have
        /// their own.
        ///
        /// # Errors
        ///
        /// What the pipe's read failed with, and, for a wait where no
        /// runtime runs, an error of kind [`io::ErrorKind::Unsupported`].
        pub fn poll_read(
            &mut self,
            cx: &mut Context<'_>,
            buf: &mut [MaybeUninit<u8>],
        ) -> Poll<io::Result<usize>> {
            self.pipe.poll_read(cx, buf, &mut self.wait)
        }

        /// Reads into `buf` what the pipe's read in flight has brought, with
        /// no wait, and starts the next read once that is all taken:
        /// `WouldBlock` if nothing has arrived, 0 bytes at the end.
        ///
        /// # Errors
        ///
        /// `WouldBlock`, and what the pipe's read failed with, once.
        pub fn try_read(&self, buf: &mut [u8]) -> io::Result<usize> {
            // SAFETY: the read writes none but initialized bytes, so the
            // bytes stay initialized.
            let buf = unsafe { &mut *(ptr::from_mut(buf) as *mut [MaybeUninit<u8>]) };
            self.pipe.try_read(buf)
        }

        /// Waits for the pipe to be readable: for its read in flight, which
        /// this starts if none is, to have brought data, or the end, which a
        /// read then takes, on the port of the thread polling it. Another
        /// reader may take it first, so a [`try_read`](Self::try_read)
        /// after it can still find nothing.
        ///
        /// # Errors
        ///
        /// What the system reports for the wait, and, where no runtime runs
        /// while nothing has arrived, an error of kind
        /// [`io::ErrorKind::Unsupported`].
        pub async fn readable(&self) -> io::Result<()> {
            self.pipe.ready(READABLE).await.map(drop)
        }

        /// The pipe's handle, with its read in flight, if one is, cancelled,
        /// and what it brought, not read yet, dropped.
        pub fn into_owned_handle(self) -> OwnedHandle {
            self.pipe.into_owned_handle()
        }
    };
}

/// Reads a child's standard output, with reads of what the pipe's read in
/// flight brought, made at once, that wait on the port of the thread
/// polling them only if nothing has arrived; [`AsyncRead`] reads it to its
/// end, and the poll methods into the caller's buffers.
pub struct ChildStdout {
    pipe: Nonblocking,
    /// The wait of `poll_read`.
    wait: Wait,
}

impl ChildStdout {
    /// Takes over the parent's end of the pipe, which the standard library
    /// opens for overlapped I/O, and no port has.
    fn from_std(stdout: std::process::ChildStdout) -> Self {
        Self {
            pipe: Nonblocking::pipe(Pipe::new(OwnedHandle::from(stdout))),
            wait: Wait::new(),
        }
    }

    /// Reads into `buf`, up to its capacity, what has arrived, waiting for
    /// something to if nothing has; 0 bytes at the end, once the child, and
    /// whatever else holds the pipe, has closed it.
    pub fn read<B: IoBufMut>(&self, buf: B) -> PortableRead<'_, B> {
        self.pipe.read(buf)
    }

    pipe_read_methods!();
}

impl AsyncRead for ChildStdout {
    type Read<'a, B: IoBufMut> = PortableRead<'a, B>;

    #[inline]
    fn read<B: IoBufMut>(&self, buf: B) -> Self::Read<'_, B> {
        ChildStdout::read(self, buf)
    }
}

/// Takes over the standard library's end of the pipe, as
/// [`ChildStdin`]'s conversion does; it never fails.
impl TryFrom<std::process::ChildStdout> for ChildStdout {
    type Error = io::Error;

    fn try_from(stdout: std::process::ChildStdout) -> io::Result<Self> {
        Ok(Self::from_std(stdout))
    }
}

/// Reads a child's standard error, as [`ChildStdout`] reads its output.
pub struct ChildStderr {
    pipe: Nonblocking,
    /// The wait of `poll_read`.
    wait: Wait,
}

impl ChildStderr {
    /// Takes over the parent's end of the pipe, which the standard library
    /// opens for overlapped I/O, and no port has.
    fn from_std(stderr: std::process::ChildStderr) -> Self {
        Self {
            pipe: Nonblocking::pipe(Pipe::new(OwnedHandle::from(stderr))),
            wait: Wait::new(),
        }
    }

    /// Reads into `buf`, up to its capacity; see [`ChildStdout::read`].
    pub fn read<B: IoBufMut>(&self, buf: B) -> PortableRead<'_, B> {
        self.pipe.read(buf)
    }

    pipe_read_methods!();
}

impl AsyncRead for ChildStderr {
    type Read<'a, B: IoBufMut> = PortableRead<'a, B>;

    #[inline]
    fn read<B: IoBufMut>(&self, buf: B) -> Self::Read<'_, B> {
        ChildStderr::read(self, buf)
    }
}

/// Takes over the standard library's end of the pipe, as
/// [`ChildStdin`]'s conversion does; it never fails.
impl TryFrom<std::process::ChildStderr> for ChildStderr {
    type Error = io::Error;

    fn try_from(stderr: std::process::ChildStderr) -> io::Result<Self> {
        Ok(Self::from_std(stderr))
    }
}

macro_rules! pipe_handle_impls {
    ($($pipe:ident),*) => {$(
        impl AsHandle for $pipe {
            fn as_handle(&self) -> BorrowedHandle<'_> {
                self.pipe.as_pipe().as_handle()
            }
        }

        impl AsRawHandle for $pipe {
            fn as_raw_handle(&self) -> RawHandle {
                self.pipe.as_pipe().as_raw_handle()
            }
        }

        impl fmt::Debug for $pipe {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct(stringify!($pipe))
                    .field("handle", &self.pipe.as_pipe().as_raw_handle())
                    .finish()
            }
        }
    )*};
}

pipe_handle_impls!(ChildStdin, ChildStdout, ChildStderr);
