//! Handles associated with an engine's port.

use std::io;
use std::os::windows::io::{AsRawHandle, OwnedHandle};

use crate::engine::Iocp;
use crate::sys;

/// A handle opened for overlapped I/O and associated with the port of the
/// engine current where it was made, to which its operations complete.
pub(crate) struct IoHandle {
    // Closed before the engine is let go of, so that the operations the
    // closing cancels complete while the engine waits for them.
    handle: OwnedHandle,
    engine: Iocp,
}

impl IoHandle {
    /// Associates `handle`, opened for overlapped I/O, with the port of the
    /// thread's current engine.
    pub(crate) fn new(handle: OwnedHandle) -> io::Result<Self> {
        let engine = Iocp::current().ok_or_else(|| {
            io::Error::other("the thread has no ruio runtime for the handle's I/O to run on")
        })?;
        engine.associate(handle.as_raw_handle())?;
        Ok(Self { handle, engine })
    }

    pub(crate) fn raw(&self) -> sys::Handle {
        self.handle.as_raw_handle()
    }

    pub(crate) fn engine(&self) -> &Iocp {
        &self.engine
    }
}
