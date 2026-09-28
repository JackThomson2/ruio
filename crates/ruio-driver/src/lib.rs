//! The io_uring driver of ruio.
//!
//! [`Probe`] finds out what the running kernel's io_uring supports and checks
//! it against ruio's baseline, the capabilities of Linux 6.1.

mod op;
mod probe;
// Used by the driver, which is added separately.
#[allow(dead_code)]
mod ring;
// Used by the driver, which is added separately.
#[allow(dead_code)]
mod table;

pub use op::Completion;
pub use probe::{BaselineError, Probe};
