//! The io_uring system calls.

use std::ffi::{c_int, c_long, c_void};
use std::io;
use std::ptr;

use crate::io_uring_params;

/// Turns a raw system call return value into a result, reading `errno` on
/// failure.
#[inline]
fn check(ret: c_long) -> io::Result<c_long> {
    if ret < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(ret)
    }
}

/// Creates a ring with room for at least `entries` submissions; see
/// `io_uring_setup(2)`.
///
/// The kernel fills in the rest of `params` and returns the ring's file
/// descriptor, which the caller owns. With
/// [`IORING_SETUP_REGISTERED_FD_ONLY`](crate::IORING_SETUP_REGISTERED_FD_ONLY)
/// it returns a registered ring index instead.
///
/// # Safety
///
/// With [`IORING_SETUP_NO_MMAP`](crate::IORING_SETUP_NO_MMAP), the memory
/// that `params.sq_off.user_addr` and `params.cq_off.user_addr` point to must
/// meet the requirements of `io_uring_setup(2)` and stay valid for as long as
/// the ring exists.
pub unsafe fn io_uring_setup(entries: u32, params: &mut io_uring_params) -> io::Result<c_int> {
    // SAFETY: `params` is valid for reads and writes, and the caller vouches
    // for any memory it points to.
    let ret = unsafe { libc::syscall(libc::SYS_io_uring_setup, entries, ptr::from_mut(params)) };
    check(ret).map(|fd| fd as c_int)
}

/// Submits queued entries and optionally waits for completions; see
/// `io_uring_enter(2)`.
///
/// Returns the number of entries submitted.
///
/// # Safety
///
/// Every entry submitted must point only to memory that stays valid, as its
/// opcode requires, until the kernel is done with it; for most opcodes that
/// is when the request's final completion is posted. `arg` must be null or
/// point to `argsz` readable bytes of what `flags` selects: a signal mask, or
/// an [`io_uring_getevents_arg`](crate::io_uring_getevents_arg) with
/// [`IORING_ENTER_EXT_ARG`](crate::IORING_ENTER_EXT_ARG).
#[inline]
pub unsafe fn io_uring_enter(
    fd: c_int,
    to_submit: u32,
    min_complete: u32,
    flags: u32,
    arg: *const c_void,
    argsz: usize,
) -> io::Result<u32> {
    // SAFETY: the caller vouches for the submitted entries and for `arg`.
    let ret = unsafe {
        libc::syscall(
            libc::SYS_io_uring_enter,
            fd,
            to_submit,
            min_complete,
            flags,
            arg,
            argsz,
        )
    };
    check(ret).map(|submitted| submitted as u32)
}

/// Registers resources with a ring, or queries or configures it; see
/// `io_uring_register(2)`.
///
/// Returns the opcode's result, which is never negative.
///
/// # Safety
///
/// `arg` must be valid for the reads and writes that `opcode` performs for
/// `nr_args` elements. Memory the ring keeps using after the call, such as
/// registered buffers or provided-buffer rings, must stay valid until it is
/// unregistered or the ring is destroyed.
pub unsafe fn io_uring_register(
    fd: c_int,
    opcode: u32,
    arg: *mut c_void,
    nr_args: u32,
) -> io::Result<u32> {
    // SAFETY: the caller vouches for `arg` and for any memory it registers.
    let ret = unsafe { libc::syscall(libc::SYS_io_uring_register, fd, opcode, arg, nr_args) };
    check(ret).map(|result| result as u32)
}

#[cfg(test)]
mod tests {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

    use super::*;
    use crate::*;

    fn ring(entries: u32) -> (OwnedFd, io_uring_params) {
        let mut params = io_uring_params::default();
        // SAFETY: no flags are set, so the kernel uses no memory from `params`.
        let fd = unsafe { io_uring_setup(entries, &mut params) }.unwrap();
        // SAFETY: `fd` is a new descriptor that nothing else owns.
        let ring = unsafe { OwnedFd::from_raw_fd(fd) };
        (ring, params)
    }

    #[test]
    fn setup_creates_a_ring() {
        let (_ring, params) = ring(4);
        assert_eq!(params.sq_entries, 4);
        assert_eq!(params.cq_entries, 8);
        assert_ne!(params.features & IORING_FEAT_NODROP, 0);
    }

    #[test]
    fn setup_reports_invalid_flags() {
        let mut params = io_uring_params {
            flags: 1 << 31,
            ..Default::default()
        };
        // SAFETY: the kernel rejects the unknown flag before using `params`.
        let err = unsafe { io_uring_setup(4, &mut params) }.unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EINVAL));
    }

    #[test]
    fn enter_with_nothing_to_do_returns_zero() {
        let (ring, _) = ring(4);
        // SAFETY: nothing is submitted and there is no argument.
        let submitted = unsafe { io_uring_enter(ring.as_raw_fd(), 0, 0, 0, ptr::null(), 0) };
        assert_eq!(submitted.unwrap(), 0);
    }

    #[test]
    fn enter_reports_bad_descriptors() {
        // SAFETY: nothing is submitted and there is no argument.
        let err = unsafe { io_uring_enter(-1, 0, 0, 0, ptr::null(), 0) }.unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EBADF));
    }

    #[test]
    fn register_probe_reports_nop() {
        #[repr(C)]
        struct Probe {
            header: io_uring_probe,
            ops: [io_uring_probe_op; 8],
        }

        let (ring, _) = ring(4);
        let mut probe = Probe {
            header: io_uring_probe::default(),
            ops: [io_uring_probe_op::default(); 8],
        };
        let arg = ptr::from_mut(&mut probe).cast();
        // SAFETY: `probe` is zeroed, as the kernel requires, with room for the
        // header and the eight entries asked for.
        unsafe { io_uring_register(ring.as_raw_fd(), IORING_REGISTER_PROBE, arg, 8) }.unwrap();

        assert_eq!(probe.header.ops_len, 8);
        assert!(probe.header.last_op >= IORING_OP_SENDMSG_ZC);
        assert_eq!(probe.ops[IORING_OP_NOP as usize].op, IORING_OP_NOP);
        assert_ne!(
            probe.ops[IORING_OP_NOP as usize].flags & IO_URING_OP_SUPPORTED,
            0
        );
    }
}
