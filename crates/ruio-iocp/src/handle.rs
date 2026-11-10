//! Handles associated with an engine's port.

use std::io;
use std::os::windows::io::{AsRawHandle, OwnedHandle};

use crate::engine::Iocp;
use crate::sys;

/// What can be associated with a port: the owner of a handle opened for
/// overlapped I/O.
pub(crate) trait Raw {
    /// The handle, as the system's calls take it.
    fn raw(&self) -> sys::Handle;
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
}

impl<T: Raw> IoHandle<T> {
    /// Associates `io`'s handle, opened for overlapped I/O, with the port of
    /// the thread's current engine.
    pub(crate) fn new(io: T) -> io::Result<Self> {
        let engine = Iocp::current().ok_or_else(|| {
            io::Error::other("the thread has no ruio runtime for the handle's I/O to run on")
        })?;
        engine.associate(io.raw())?;
        Ok(Self { io, engine })
    }

    pub(crate) fn raw(&self) -> sys::Handle {
        self.io.raw()
    }

    /// The handle, borrowed for an operation on it.
    pub(crate) fn target(&self) -> Target<'_> {
        Target {
            handle: self.io.raw(),
            engine: &self.engine,
        }
    }
}

/// A handle of a port, borrowed from what owns it for an operation on it,
/// and the engine the port is.
#[derive(Clone, Copy)]
pub(crate) struct Target<'a> {
    pub(crate) handle: sys::Handle,
    pub(crate) engine: &'a Iocp,
}
