//! The operations the driver provides.

use std::io;

use ruio_sys as sys;

use crate::{Completion, Operation};

/// Does nothing and completes with `Ok(())`, which makes it a measure of the
/// driver's own overhead.
#[derive(Clone, Copy, Debug, Default)]
pub struct Nop;

// SAFETY: a no-op points to no memory and posts one completion.
unsafe impl Operation for Nop {
    type Output = io::Result<()>;

    fn prepare(&mut self, sqe: &mut sys::io_uring_sqe) {
        sqe.opcode = sys::IORING_OP_NOP;
    }

    fn complete(self, completion: Completion) -> io::Result<()> {
        completion.result().map(drop)
    }
}
