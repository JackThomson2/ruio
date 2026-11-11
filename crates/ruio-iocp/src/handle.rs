//! Handles associated with an engine's port.

use std::io;
use std::os::windows::io::{AsRawHandle, OwnedHandle};
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
/// complete.
pub(crate) struct IoHandle<T> {
    // Closed before the engine is let go of, so that the operations the
    // closing cancels complete while the engine waits for them.
    io: T,
    engine: Iocp,
    /// Whether the system queues no completion to the port for the
    /// handle's operations that succeed during the calls that start them.
    skips: bool,
}

impl<T: Raw> IoHandle<T> {
    /// Associates `io`'s handle, opened for overlapped I/O, with the port of
    /// the thread's current engine.
    pub(crate) fn new(io: T) -> io::Result<Self> {
        let engine = Iocp::current().ok_or_else(|| {
            io::Error::other("the thread has no ruio runtime for the handle's I/O to run on")
        })?;
        Self::on(io, engine)
    }

    /// Associates `io`'s handle, opened for overlapped I/O, with the port of
    /// `engine`, on whose thread this is.
    pub(crate) fn on(io: T, engine: Iocp) -> io::Result<Self> {
        engine.associate(io.raw())?;
        let skips = io.may_skip_port() && skip_port(io.raw());
        Ok(Self { io, engine, skips })
    }

    /// What owns the handle.
    pub(crate) fn io(&self) -> &T {
        &self.io
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
        }
    }

    /// Whether the handle's operations that succeed during the calls that
    /// start them come through no port.
    #[cfg(test)]
    pub(crate) fn skips(&self) -> bool {
        self.skips
    }
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

/// A handle of a port, borrowed from what owns it for an operation on it,
/// and the engine the port is.
#[derive(Clone, Copy)]
pub(crate) struct Target<'a> {
    pub(crate) handle: sys::Handle,
    pub(crate) engine: &'a Iocp,
    /// Whether the handle's operations that succeed during the calls that
    /// start them come through no port, and are finished there and then.
    pub(crate) skips: bool,
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
