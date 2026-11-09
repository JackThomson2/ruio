//! The park engine of `ruio-engine`: tasks, timers and channels run, and
//! nothing does I/O. The executor runs on it where ruio has no I/O engine
//! yet, and wherever it is built with `--cfg ruio_engine="park"`.

use ruio_engine::park::{Parker, Unparker};
use ruio_engine::{Engine, Unpark};

/// The engine.
pub(crate) type Native = Parker;

/// What a runtime's shared part keeps of its engine for other threads: the
/// unparker.
pub(crate) struct Remote {
    unparker: Unparker,
}

impl Remote {
    pub(crate) fn new(parker: &Parker) -> Self {
        Self {
            unparker: Engine::unparker(parker),
        }
    }

    /// Wakes the parked thread, or makes its next park return at once.
    #[inline]
    pub(crate) fn unpark(&self) {
        Unpark::unpark(&self.unparker);
    }
}
