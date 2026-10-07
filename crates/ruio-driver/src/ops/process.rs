//! Waiting for child processes.

use std::fmt;
use std::io;
use std::mem;
use std::os::fd::RawFd;

use ruio_sys as sys;

use crate::{Completion, Operation};

/// Waits for a child process to change state, like `waitid(2)`, without
/// holding up the ring's thread: the kernel wakes the request as the child
/// changes, as it would wake the system call.
///
/// It waits for children to exit (`WEXITED`) unless [`options`](Waitid::options)
/// says otherwise, and reaps the child it reports, which can then be waited
/// for no more, unless the options include `WNOWAIT`: then the child stays
/// waitable, for a later wait to reap. A child is waited for from any thread
/// of the process, whichever spawned it.
///
/// The output is the change the kernel reported, or `None` for a wait with
/// `WNOHANG` that found none.
///
/// # Errors
///
/// What `waitid(2)` reports: `ECHILD` if no child matches, because there is
/// none or it was reaped already, and `EINVAL` for options that make no
/// sense.
pub struct Waitid {
    idtype: libc::idtype_t,
    id: libc::id_t,
    options: i32,
    // Boxed: the kernel writes it when it completes the operation.
    info: Box<libc::siginfo_t>,
}

impl Waitid {
    /// Waits for the child with process ID `pid` (`P_PID`).
    pub fn pid(pid: u32) -> Self {
        Self::new(libc::P_PID, pid)
    }

    /// Waits for the child that the pidfd `fd` refers to (`P_PIDFD`), which
    /// names it even once its process ID could be another's. A pidfd opened
    /// with `PIDFD_NONBLOCK` waits as `WNOHANG` does.
    pub fn pidfd(fd: RawFd) -> Self {
        Self::new(libc::P_PIDFD, fd.cast_unsigned())
    }

    /// Waits for any child of the process in the process group `pgid`
    /// (`P_PGID`).
    pub fn group(pgid: u32) -> Self {
        Self::new(libc::P_PGID, pgid)
    }

    /// Waits for any child of the process (`P_ALL`): one that a wait for a
    /// particular child elsewhere in the process might be waiting for too.
    pub fn any() -> Self {
        Self::new(libc::P_ALL, 0)
    }

    fn new(idtype: libc::idtype_t, id: libc::id_t) -> Self {
        Self {
            idtype,
            id,
            options: libc::WEXITED,
            // SAFETY: all zeroes is a valid `siginfo_t`.
            info: Box::new(unsafe { mem::zeroed() }),
        }
    }

    /// Sets the `waitid(2)` options, replacing `WEXITED`: which changes to
    /// wait for, `WEXITED`, `WSTOPPED` and `WCONTINUED`, and how, `WNOHANG`
    /// to report none rather than wait, and `WNOWAIT` to leave the child
    /// waitable.
    pub fn options(mut self, options: i32) -> Self {
        self.options = options;
        self
    }
}

// SAFETY: the entry points to the boxed `siginfo_t`, which the operation owns
// and which stays put when it moves; the kernel writes it before completing.
// A waitid posts one completion.
unsafe impl Operation for Waitid {
    type Output = io::Result<Option<ChildState>>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_WAITID;
        sqe.len = self.idtype;
        sqe.fd = self.id.cast_signed();
        sqe.file_index = self.options.cast_unsigned();
        // The kernel's `addr2`, where it writes the `siginfo_t`.
        sqe.off = (&raw mut *self.info).expose_provenance() as u64;
    }

    fn complete(self, completion: Completion) -> Self::Output {
        completion.result()?;
        // The kernel sets `si_signo` to `SIGCHLD` only as it reports a child;
        // without one, under `WNOHANG`, the rest is not to be trusted.
        if self.info.si_signo != libc::SIGCHLD {
            return Ok(None);
        }
        // SAFETY: the kernel filled in the fields of a `SIGCHLD`'s
        // `siginfo_t`, which these read.
        let pid = unsafe { self.info.si_pid() };
        // SAFETY: as above.
        let uid = unsafe { self.info.si_uid() };
        // SAFETY: as above.
        let status = unsafe { self.info.si_status() };
        Ok(Some(ChildState {
            pid: pid.cast_unsigned(),
            uid,
            code: self.info.si_code,
            status,
        }))
    }
}

impl fmt::Debug for Waitid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Waitid")
            .field("idtype", &self.idtype)
            .field("id", &self.id)
            .field("options", &format_args!("{:#x}", self.options))
            .finish_non_exhaustive()
    }
}

/// How a child process changed, as [`Waitid`] reports it: the fields of the
/// `siginfo_t` that `waitid(2)` fills in.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ChildState {
    pid: u32,
    uid: u32,
    code: i32,
    status: i32,
}

impl ChildState {
    /// The child's process ID.
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// The real user ID of the child.
    pub fn uid(&self) -> u32 {
        self.uid
    }

    /// What happened to the child, the `si_code`: `CLD_EXITED`,
    /// `CLD_KILLED`, `CLD_DUMPED`, `CLD_STOPPED`, `CLD_TRAPPED` or
    /// `CLD_CONTINUED`.
    pub fn code(&self) -> i32 {
        self.code
    }

    /// The child's exit status if it exited, and otherwise the signal that
    /// killed, stopped or continued it, the `si_status`.
    pub fn status(&self) -> i32 {
        self.status
    }

    /// The exit status, if the child exited.
    pub fn exit_code(&self) -> Option<i32> {
        (self.code == libc::CLD_EXITED).then_some(self.status)
    }

    /// The signal that killed the child, if one did, with a core dump or
    /// without.
    pub fn signal(&self) -> Option<i32> {
        matches!(self.code, libc::CLD_KILLED | libc::CLD_DUMPED).then_some(self.status)
    }

    /// The change as `waitpid(2)` encodes it in its status word, which
    /// `std::os::unix::process::ExitStatusExt::from_raw` takes.
    pub fn wait_status(&self) -> i32 {
        let status = self.status;
        match self.code {
            libc::CLD_EXITED => (status & 0xff) << 8,
            libc::CLD_KILLED => status & 0x7f,
            libc::CLD_DUMPED => (status & 0x7f) | 0x80,
            libc::CLD_STOPPED | libc::CLD_TRAPPED => ((status & 0xff) << 8) | 0x7f,
            libc::CLD_CONTINUED => 0xffff,
            _ => status,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::process::ExitStatusExt;
    use std::process::ExitStatus;

    use super::*;

    fn state(code: i32, status: i32) -> ChildState {
        ChildState {
            pid: 1,
            uid: 0,
            code,
            status,
        }
    }

    /// The status words agree with how the standard library reads them.
    #[test]
    fn wait_statuses_are_encoded_as_waitpid_encodes_them() {
        let exited = ExitStatus::from_raw(state(libc::CLD_EXITED, 3).wait_status());
        assert_eq!((exited.code(), exited.signal()), (Some(3), None));
        let killed = ExitStatus::from_raw(state(libc::CLD_KILLED, libc::SIGKILL).wait_status());
        assert_eq!(
            (killed.code(), killed.signal()),
            (None, Some(libc::SIGKILL))
        );
        assert!(!killed.core_dumped());
        let dumped = ExitStatus::from_raw(state(libc::CLD_DUMPED, libc::SIGSEGV).wait_status());
        assert_eq!(dumped.signal(), Some(libc::SIGSEGV));
        assert!(dumped.core_dumped());
        let stopped = ExitStatus::from_raw(state(libc::CLD_STOPPED, libc::SIGSTOP).wait_status());
        assert_eq!(stopped.stopped_signal(), Some(libc::SIGSTOP));
        let continued =
            ExitStatus::from_raw(state(libc::CLD_CONTINUED, libc::SIGCONT).wait_status());
        assert!(continued.continued());
    }

    #[test]
    fn exit_codes_and_signals_are_told_apart() {
        assert_eq!(state(libc::CLD_EXITED, 3).exit_code(), Some(3));
        assert_eq!(state(libc::CLD_EXITED, 3).signal(), None);
        assert_eq!(state(libc::CLD_KILLED, 9).signal(), Some(9));
        assert_eq!(state(libc::CLD_DUMPED, 11).signal(), Some(11));
        assert_eq!(state(libc::CLD_STOPPED, 19).signal(), None);
        assert_eq!(state(libc::CLD_KILLED, 9).exit_code(), None);
    }
}
