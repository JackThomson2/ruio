//! Handles associated with an engine's port.

use std::io;
use std::os::windows::io::{AsRawHandle, OwnedHandle};
use std::ptr;
use std::task::Context;

use crate::engine::Iocp;
use crate::sys;

/// What can be associated with a port: the owner of a handle opened for
/// overlapped I/O.
pub(crate) trait Raw {
    /// The handle, as the system's calls take it.
    fn raw(&self) -> sys::Handle;

    /// Whether the system may be asked to queue no completion for the
    /// handle's operations that succeed during the calls that start them:
    /// only where such a call's success says for certain that nothing
    /// follows it. Not unless the owner says so.
    fn may_skip_port(&self) -> bool {
        false
    }
}

impl Raw for OwnedHandle {
    fn raw(&self) -> sys::Handle {
        self.as_raw_handle()
    }
}

/// A handle opened for overlapped I/O, in what owns it, associated with the
/// port of the engine current where it was made, to which its operations
/// complete; or one opened for I/O that waits, which belongs to that engine
/// and whose operations finish as they start.
pub(crate) struct IoHandle<T> {
    // Closed before the engine is let go of, so that the operations the
    // closing cancels complete while the engine waits for them.
    io: T,
    engine: Iocp,
    /// Whether the system queues no completion to the port for the
    /// handle's operations that succeed during the calls that start them.
    skips: bool,
    /// Whether the handle was opened for I/O that waits, and so is
    /// associated with no port: each of its operations finishes, or fails,
    /// during the call that starts it.
    waits: bool,
}

impl<T: Raw> IoHandle<T> {
    /// Associates `io`'s handle, opened for overlapped I/O, with the port of
    /// the thread's current engine.
    pub(crate) fn new(io: T) -> io::Result<Self> {
        Self::on(io, current()?)
    }

    /// Associates `io`'s handle, opened for overlapped I/O, with the port of
    /// `engine`, on whose thread this is.
    pub(crate) fn on(io: T, engine: Iocp) -> io::Result<Self> {
        engine.associate(io.raw())?;
        let skips = io.may_skip_port() && skip_port(io.raw());
        Ok(Self {
            io,
            engine,
            skips,
            waits: false,
        })
    }

    /// Takes `io`'s handle for the thread's current engine, whichever way
    /// it was opened. One opened for overlapped I/O is associated with the
    /// engine's port, as [`IoHandle::new`] associates it. One opened for I/O
    /// that waits, as the standard library opens a file, cannot be, and is
    /// not: each of its operations finishes, or fails, during the call that
    /// starts it, which waits for the device, and nothing comes through the
    /// port for it.
    pub(crate) fn adopt(io: T) -> io::Result<Self> {
        if !opened_to_wait(io.raw()) {
            return Self::new(io);
        }
        Ok(Self {
            io,
            engine: current()?,
            skips: true,
            waits: true,
        })
    }

    /// Associates `io`'s handle, taken from the standard library, with the
    /// port of the thread's current engine, as [`IoHandle::new`] does, for a
    /// `from_std` that reports no error, as it reports none on io_uring.
    ///
    /// # Panics
    ///
    /// If the thread has no current engine, or the system refuses to
    /// associate the handle, as when another port has it already.
    pub(crate) fn taken(io: T) -> Self {
        Self::new(io).unwrap_or_else(|err| {
            panic!("cannot take the handle for the port of the thread's engine: {err}")
        })
    }

    /// What owns the handle.
    pub(crate) fn io(&self) -> &T {
        &self.io
    }

    /// What owns the handle, out of it. The handle stays associated with the
    /// port, with no operation on it: each borrows what the handle is in.
    pub(crate) fn into_io(self) -> T {
        self.io
    }

    /// What owns the handle, taken off the engine's port, for another
    /// engine to associate with its own, as [`IoHandle::new`] does on that
    /// engine's thread. A handle opened for I/O that waits is on no port,
    /// and is taken as it is.
    ///
    /// The system moves a handle off its port only once the port has
    /// delivered the completions of all of its operations, which a turn
    /// of the port does: one cancelled a moment ago is in flight until
    /// then. So nothing comes through either port for an operation started
    /// on this one.
    ///
    /// # Errors
    ///
    /// Hands this back, still on the port, with `ResourceBusy` while an
    /// operation on the handle is in flight, `Unsupported` before Windows
    /// 8.1, which moves no handle off its port, and what else the system
    /// reports.
    pub(crate) fn release(self) -> Result<T, (io::Error, Self)> {
        if self.waits {
            return Ok(self.io);
        }
        match leave_port(self.io.raw()) {
            Ok(()) => Ok(self.io),
            Err(err) => Err((err, self)),
        }
    }

    /// The engine whose port the handle is associated with.
    pub(crate) fn engine(&self) -> &Iocp {
        &self.engine
    }

    pub(crate) fn raw(&self) -> sys::Handle {
        self.io.raw()
    }

    /// The handle, borrowed for an operation on it.
    pub(crate) fn target(&self) -> Target<'_> {
        Target {
            handle: self.io.raw(),
            engine: &self.engine,
            skips: self.skips,
            waits: self.waits,
        }
    }

    /// Whether the handle's operations that succeed during the calls that
    /// start them come through no port.
    #[cfg(test)]
    pub(crate) fn skips(&self) -> bool {
        self.skips
    }
}

/// The thread's current engine.
fn current() -> io::Result<Iocp> {
    Iocp::current().ok_or_else(|| {
        io::Error::other("the thread has no ruio runtime for the handle's I/O to run on")
    })
}

/// Whether `handle` was opened for I/O that waits, which the system does in
/// full during the call that asks for it, so that it cannot be associated
/// with a port. Not if the system cannot say, as of a socket that a
/// provider of its own made.
fn opened_to_wait(handle: sys::Handle) -> bool {
    query_file(handle, sys::FILE_MODE_INFORMATION)
        .is_some_and(|mode| mode & sys::FILE_SYNCHRONOUS_IO != 0)
}

/// What the query `class` of `NtQueryInformationFile`, one whose answer is
/// a `u32`, finds of the file of `handle`, or `None` if the system cannot
/// say.
pub(crate) fn query_file(handle: sys::Handle, class: u32) -> Option<u32> {
    let mut block = sys::IoStatusBlock {
        status: 0,
        information: 0,
    };
    let mut info = 0_u32;
    // SAFETY: the handle is open, as its owner is borrowed; the call writes
    // at most the value's four bytes and the status block, before it
    // returns, as the system answers these queries from the handle alone.
    let status = unsafe {
        sys::NtQueryInformationFile(
            handle,
            &mut block,
            ptr::from_mut(&mut info).cast(),
            size_of::<u32>() as u32,
            class,
        )
    };
    (status >= 0).then_some(info)
}

/// Asks the system to queue no completion to the port for `handle`'s
/// operations that succeed during the calls that start them, and not to
/// signal the handle as they complete, which nothing waits for; and says
/// whether it will not.
fn skip_port(handle: sys::Handle) -> bool {
    let modes = sys::FILE_SKIP_COMPLETION_PORT_ON_SUCCESS | sys::FILE_SKIP_SET_EVENT_ON_HANDLE;
    // SAFETY: the handle is open, as its owner is borrowed.
    let set = unsafe { sys::SetFileCompletionNotificationModes(handle, modes) };
    set != 0
}

/// Takes `handle` off the port it is associated with, which leaves it on
/// none, for a port to take as one takes a handle that was never on any.
fn leave_port(handle: sys::Handle) -> io::Result<()> {
    let mut block = sys::IoStatusBlock {
        status: 0,
        information: 0,
    };
    let none = sys::FileCompletionInformation {
        port: ptr::null_mut(),
        key: 0,
    };
    // SAFETY: the handle is open, as its owner is borrowed; the call reads
    // the information and writes the status block before it returns.
    let status = unsafe {
        sys::NtSetInformationFile(
            handle,
            &mut block,
            ptr::from_ref(&none).cast(),
            size_of::<sys::FileCompletionInformation>() as u32,
            sys::FILE_REPLACE_COMPLETION_INFORMATION,
        )
    };
    match status {
        0.. => Ok(()),
        // What the system says while an operation on the handle has yet to
        // be delivered by the port.
        sys::STATUS_UNSUCCESSFUL => Err(io::Error::new(
            io::ErrorKind::ResourceBusy,
            "an operation on the handle is in flight, which keeps it on its port",
        )),
        sys::STATUS_INVALID_INFO_CLASS => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "moving a handle off its port takes Windows 8.1 or later",
        )),
        status => Err(io::Error::from_raw_os_error(
            sys::RtlNtStatusToDosError(status) as i32,
        )),
    }
}

/// A handle of a port, borrowed from what owns it for an operation on it,
/// and the engine the port is.
#[derive(Clone, Copy)]
pub(crate) struct Target<'a> {
    pub(crate) handle: sys::Handle,
    pub(crate) engine: &'a Iocp,
    /// Whether the handle's operations that succeed during the calls that
    /// start them come through no port, and are finished there and then.
    pub(crate) skips: bool,
    /// Whether the handle was opened for I/O that waits, and is associated
    /// with no port: its operations finish, or fail, during the calls that
    /// start them, and nothing comes through the port for any of them.
    pub(crate) waits: bool,
}

impl Target<'_> {
    /// Whether an operation on the handle is to wait for the port's next
    /// turn before it starts, in which case the task of `cx` is woken to
    /// try again: when the handle's operations may finish as they start,
    /// and as many have since the last turn as may between two.
    pub(crate) fn defers(&self, cx: &mut Context<'_>) -> bool {
        if self.skips && self.engine.at_once_spent() {
            cx.waker().wake_by_ref();
            return true;
        }
        false
    }
}
