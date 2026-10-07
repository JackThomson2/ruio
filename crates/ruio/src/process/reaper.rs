//! Reaps the children whose [`Child`](super::Child) was dropped before they
//! exited, so that none lingers as a zombie once it has.
//!
//! A thread of its own, started as the first such child is dropped, polls
//! their pidfds, which become readable as their processes exit, and reaps
//! each as it does, with one `waitid(2)`. A thread, rather than the ring of
//! whichever runtime dropped the child, so that a child outlives runtimes
//! and their shutdowns: the ring of one that ends would leave it unreaped.

use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::{Mutex, Once, OnceLock, PoisonError};
use std::thread;

/// The reaper's thread needs little stack: a poll and a vector.
const STACK: usize = 64 << 10;

struct Reaper {
    /// The pidfds of the children handed over since the thread last took
    /// them.
    pending: Mutex<Vec<OwnedFd>>,
    /// An eventfd, written as a child is handed over, which wakes the
    /// thread's poll.
    wake: OwnedFd,
}

/// Hands the reaper the pidfd of a child that has not exited, to reap it as
/// it does. Without a thread to reap it, which cannot be started, the child
/// lingers as a zombie once it exits, as it would have before.
pub(super) fn adopt(pidfd: OwnedFd) {
    let Some(reaper) = reaper() else {
        return;
    };
    reaper
        .pending
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push(pidfd);
    let one = 1u64.to_ne_bytes();
    // SAFETY: write reads the eight bytes it is given. The eventfd is
    // nonblocking, and a counter that could not take another would wake the
    // thread anyway.
    unsafe { libc::write(reaper.wake.as_raw_fd(), one.as_ptr().cast(), one.len()) };
}

/// The reaper, started at the first call.
fn reaper() -> Option<&'static Reaper> {
    static REAPER: OnceLock<Option<Reaper>> = OnceLock::new();
    static STARTED: Once = Once::new();
    let reaper = REAPER.get_or_init(Reaper::new).as_ref()?;
    STARTED.call_once(|| {
        let spawned = thread::Builder::new()
            .name("ruio-reaper".into())
            .stack_size(STACK)
            .spawn(move || reaper.run());
        // A reaper with no thread keeps what it is handed, unreaped.
        drop(spawned);
    });
    Some(reaper)
}

impl Reaper {
    fn new() -> Option<Self> {
        // SAFETY: eventfd creates a descriptor and touches no memory.
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if fd < 0 {
            return None;
        }
        Some(Self {
            pending: Mutex::new(Vec::new()),
            // SAFETY: the descriptor is new, and nothing else owns it.
            wake: unsafe { OwnedFd::from_raw_fd(fd) },
        })
    }

    /// Polls the children's pidfds, and the eventfd, for ever, reaping each
    /// child as it exits and taking in those handed over.
    fn run(&self) {
        let mut children: Vec<OwnedFd> = Vec::new();
        let mut polls: Vec<libc::pollfd> = Vec::new();
        loop {
            polls.clear();
            polls.push(pollfd(&self.wake));
            polls.extend(children.iter().map(pollfd));
            // SAFETY: poll writes the `revents` of the descriptors it is
            // given, which the vector holds.
            let ready = unsafe { libc::poll(polls.as_mut_ptr(), polls.len() as libc::nfds_t, -1) };
            if ready < 0 {
                // Interrupted by a signal.
                continue;
            }
            // From the back, so that removing a child moves only one already
            // looked at into its place.
            for index in (0..children.len()).rev() {
                if polls[index + 1].revents != 0 && reaped(&children[index]) {
                    drop(children.swap_remove(index));
                }
            }
            if polls[0].revents != 0 {
                let mut count = [0u8; 8];
                // SAFETY: read writes the eight bytes it is given.
                unsafe {
                    libc::read(
                        self.wake.as_raw_fd(),
                        count.as_mut_ptr().cast(),
                        count.len(),
                    )
                };
                let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
                children.append(&mut pending);
            }
        }
    }
}

/// A poll for `fd` to be readable.
fn pollfd(fd: &OwnedFd) -> libc::pollfd {
    libc::pollfd {
        fd: fd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    }
}

/// Reaps the child `pidfd` refers to, if it has exited, and says whether it
/// is done with: reaped now, or gone, reaped by something else.
fn reaped(pidfd: &OwnedFd) -> bool {
    let mut info = MaybeUninit::<libc::siginfo_t>::zeroed();
    // SAFETY: waitid writes the one `siginfo_t` it is given.
    let waited = unsafe {
        libc::waitid(
            libc::P_PIDFD,
            pidfd.as_raw_fd().cast_unsigned(),
            info.as_mut_ptr(),
            libc::WEXITED | libc::WNOHANG,
        )
    };
    // SAFETY: zeroed, and written by waitid if it succeeded.
    waited < 0 || unsafe { info.assume_init_ref() }.si_signo == libc::SIGCHLD
}
