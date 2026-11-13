//! The I/O completion port engine of `ruio-iocp`, which the executor runs
//! on on Windows unless it is built with `--cfg ruio_engine="park"`.

use ruio_engine::{Engine, Unpark};
use ruio_iocp::{Iocp, Unparker};

/// The engine.
pub(crate) type Native = Iocp;

/// What a runtime's shared part keeps of its engine for other threads: the
/// unparker, which posts to the port.
pub(crate) struct Remote {
    unparker: Unparker,
}

impl Remote {
    pub(crate) fn new(iocp: &Iocp) -> Self {
        Self {
            unparker: Engine::unparker(iocp),
        }
    }

    /// Wakes the engine, or makes its next park return at once.
    #[inline]
    pub(crate) fn unpark(&self) {
        Unpark::unpark(&self.unparker);
    }
}
