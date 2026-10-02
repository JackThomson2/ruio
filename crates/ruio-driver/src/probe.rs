//! Detection of what the running kernel's io_uring supports.

use std::error::Error;
use std::fmt;
use std::fs;
use std::io;
use std::mem::offset_of;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::ptr;
use std::sync::OnceLock;

use ruio_sys as sys;

use crate::ring::Ring;

/// Every `IORING_FEAT_*` bit that Linux 6.10 reports.
const BASELINE_FEATURES: u32 = sys::IORING_FEAT_SINGLE_MMAP
    | sys::IORING_FEAT_NODROP
    | sys::IORING_FEAT_SUBMIT_STABLE
    | sys::IORING_FEAT_RW_CUR_POS
    | sys::IORING_FEAT_CUR_PERSONALITY
    | sys::IORING_FEAT_FAST_POLL
    | sys::IORING_FEAT_POLL_32BITS
    | sys::IORING_FEAT_SQPOLL_NONFIXED
    | sys::IORING_FEAT_EXT_ARG
    | sys::IORING_FEAT_NATIVE_WORKERS
    | sys::IORING_FEAT_RSRC_TAGS
    | sys::IORING_FEAT_CQE_SKIP
    | sys::IORING_FEAT_LINKED_FILE
    | sys::IORING_FEAT_REG_REG_RING
    | sys::IORING_FEAT_RECVSEND_BUNDLE;

/// The setup flags of ruio's default ring configuration.
const BASELINE_SETUP_FLAGS: u32 = sys::IORING_SETUP_CQSIZE
    | sys::IORING_SETUP_CLAMP
    | sys::IORING_SETUP_SUBMIT_ALL
    | sys::IORING_SETUP_TASKRUN_FLAG
    | sys::IORING_SETUP_SINGLE_ISSUER
    | sys::IORING_SETUP_DEFER_TASKRUN
    | sys::IORING_SETUP_NO_SQARRAY;

/// The opcodes ruio is built on, all of them present in Linux 6.10.
const BASELINE_OPCODES: &[u8] = &[
    sys::IORING_OP_NOP,
    sys::IORING_OP_READV,
    sys::IORING_OP_WRITEV,
    sys::IORING_OP_FSYNC,
    sys::IORING_OP_READ_FIXED,
    sys::IORING_OP_WRITE_FIXED,
    sys::IORING_OP_POLL_ADD,
    sys::IORING_OP_POLL_REMOVE,
    sys::IORING_OP_SENDMSG,
    sys::IORING_OP_RECVMSG,
    sys::IORING_OP_TIMEOUT,
    sys::IORING_OP_TIMEOUT_REMOVE,
    sys::IORING_OP_ACCEPT,
    sys::IORING_OP_ASYNC_CANCEL,
    sys::IORING_OP_LINK_TIMEOUT,
    sys::IORING_OP_CONNECT,
    sys::IORING_OP_FALLOCATE,
    sys::IORING_OP_OPENAT,
    sys::IORING_OP_OPENAT2,
    sys::IORING_OP_CLOSE,
    sys::IORING_OP_FILES_UPDATE,
    sys::IORING_OP_STATX,
    sys::IORING_OP_READ,
    sys::IORING_OP_WRITE,
    sys::IORING_OP_SEND,
    sys::IORING_OP_RECV,
    sys::IORING_OP_SPLICE,
    sys::IORING_OP_SHUTDOWN,
    sys::IORING_OP_RENAMEAT,
    sys::IORING_OP_UNLINKAT,
    sys::IORING_OP_MKDIRAT,
    sys::IORING_OP_MSG_RING,
    sys::IORING_OP_SOCKET,
    sys::IORING_OP_SEND_ZC,
    sys::IORING_OP_SENDMSG_ZC,
    sys::IORING_OP_READ_MULTISHOT,
    sys::IORING_OP_FTRUNCATE,
];

/// The setup flags [`Probe::detect`] tries, each with the flags it requires.
///
/// Flags that need resources the probe does not create are left out:
/// `SQ_AFF` (a CPU), `ATTACH_WQ` (another ring), and `NO_MMAP` and
/// `REGISTERED_FD_ONLY` (ring memory).
const PROBED_SETUP_FLAGS: &[(u32, u32)] = &[
    (sys::IORING_SETUP_IOPOLL, 0),
    (sys::IORING_SETUP_SQPOLL, 0),
    (sys::IORING_SETUP_CQSIZE, 0),
    (sys::IORING_SETUP_CLAMP, 0),
    (sys::IORING_SETUP_R_DISABLED, 0),
    (sys::IORING_SETUP_SUBMIT_ALL, 0),
    (sys::IORING_SETUP_COOP_TASKRUN, 0),
    (
        sys::IORING_SETUP_TASKRUN_FLAG,
        sys::IORING_SETUP_COOP_TASKRUN,
    ),
    (sys::IORING_SETUP_SQE128, 0),
    (sys::IORING_SETUP_CQE32, 0),
    (sys::IORING_SETUP_SINGLE_ISSUER, 0),
    (
        sys::IORING_SETUP_DEFER_TASKRUN,
        sys::IORING_SETUP_SINGLE_ISSUER,
    ),
    (sys::IORING_SETUP_NO_SQARRAY, 0),
    (sys::IORING_SETUP_HYBRID_IOPOLL, sys::IORING_SETUP_IOPOLL),
    (sys::IORING_SETUP_CQE_MIXED, 0),
    (sys::IORING_SETUP_SQE_MIXED, 0),
    (sys::IORING_SETUP_SQ_REWIND, sys::IORING_SETUP_NO_SQARRAY),
];

/// What the running kernel's io_uring supports.
///
/// [`Probe::detect`] creates a few short-lived rings to find out. The result
/// is plain data; `Display` renders it as a readable report.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Probe {
    kernel_release: String,
    features: u32,
    setup_flags: u32,
    opcodes: [u64; 4],
    last_opcode: u8,
    incremental_buffers: bool,
    vectorized_send: bool,
}

impl Probe {
    /// Probes the running kernel.
    ///
    /// # Errors
    ///
    /// Fails if no ring can be created at all, typically with `ENOSYS` when
    /// the kernel lacks io_uring or `EPERM` when it is disabled by the
    /// `kernel.io_uring_disabled` sysctl or a seccomp filter. Setup flags the
    /// kernel rejects are recorded rather than reported as errors.
    pub fn detect() -> io::Result<Self> {
        let (ring, params) = setup(0)?;
        let (opcodes, last_opcode) = probe_opcodes(&ring)?;
        let setup_flags = PROBED_SETUP_FLAGS
            .iter()
            .filter(|&&(flag, required)| setup(flag | required).is_ok())
            .fold(0, |accepted, &(flag, _)| accepted | flag);
        Ok(Self {
            kernel_release: kernel_release(),
            features: params.features,
            setup_flags,
            opcodes,
            last_opcode,
            incremental_buffers: probe_incremental_buffers(&ring),
            vectorized_send: probe_vectorized_send(),
        })
    }

    /// The probe of the running kernel, detected on first use and cached for
    /// the life of the process.
    ///
    /// # Errors
    ///
    /// As for [`Probe::detect`]. A failed detection is not cached.
    #[inline]
    pub fn current() -> io::Result<&'static Probe> {
        static CURRENT: OnceLock<Probe> = OnceLock::new();
        if let Some(probe) = CURRENT.get() {
            return Ok(probe);
        }
        let probe = Self::detect()?;
        Ok(CURRENT.get_or_init(|| probe))
    }

    /// The kernel release, as in `uname -r`, or `unknown` without `/proc`.
    pub fn kernel_release(&self) -> &str {
        &self.kernel_release
    }

    /// The `IORING_FEAT_*` bits the kernel reported.
    pub fn features(&self) -> u32 {
        self.features
    }

    /// Whether the kernel reported every `IORING_FEAT_*` bit in `mask`.
    pub fn has_features(&self, mask: u32) -> bool {
        self.features & mask == mask
    }

    /// The `IORING_SETUP_*` flags the kernel accepted.
    ///
    /// Each flag is tried on its own, together with any flag it requires, so
    /// a combination of accepted flags may still be rejected. `SQ_AFF`,
    /// `ATTACH_WQ`, `NO_MMAP` and `REGISTERED_FD_ONLY` are not tried and never
    /// reported.
    pub fn setup_flags(&self) -> u32 {
        self.setup_flags
    }

    /// Whether the kernel accepted every `IORING_SETUP_*` flag in `mask`; see
    /// [`Probe::setup_flags`].
    pub fn supports_setup_flags(&self, mask: u32) -> bool {
        self.setup_flags & mask == mask
    }

    /// Whether the kernel supports `opcode`, an `IORING_OP_*` constant.
    pub fn supports_opcode(&self, opcode: u8) -> bool {
        self.opcodes[usize::from(opcode / 64)] & (1 << (opcode % 64)) != 0
    }

    /// The highest opcode the kernel knows, supported or not.
    pub fn last_opcode(&self) -> u8 {
        self.last_opcode
    }

    /// Whether the kernel consumes provided buffers incrementally
    /// (`IOU_PBUF_RING_INC`, Linux 6.12): a buffer ring registered for it
    /// lends each buffer to as many completions as fit in it.
    #[inline]
    pub fn supports_incremental_buffers(&self) -> bool {
        self.incremental_buffers
    }

    /// Whether a send takes a vector of buffers (`IORING_SEND_VECTORIZED`,
    /// Linux 6.17): an `IORING_OP_SEND` whose address and length name an
    /// array of `iovec`s.
    #[inline]
    pub fn supports_vectorized_send(&self) -> bool {
        self.vectorized_send
    }

    /// Checks the kernel against ruio's baseline, the capabilities of Linux
    /// 6.10.
    ///
    /// # Errors
    ///
    /// Returns every missing feature bit, setup flag and opcode.
    pub fn check_baseline(&self) -> Result<(), BaselineError> {
        let mut missing: Vec<&'static str> =
            missing_bits(sys::FEATURES, BASELINE_FEATURES, self.features)
                .chain(missing_bits(
                    sys::SETUP_FLAGS,
                    BASELINE_SETUP_FLAGS,
                    self.setup_flags,
                ))
                .collect();
        missing.extend(
            sys::OPCODES
                .iter()
                .filter(|&&(_, op)| BASELINE_OPCODES.contains(&op) && !self.supports_opcode(op))
                .map(|&(name, _)| name),
        );
        if missing.is_empty() {
            Ok(())
        } else {
            Err(BaselineError {
                kernel_release: self.kernel_release.clone(),
                missing,
            })
        }
    }
}

impl fmt::Display for Probe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "kernel    {}", self.kernel_release)?;
        match self.check_baseline() {
            Ok(()) => writeln!(f, "baseline  met: Linux 6.10 capabilities present")?,
            Err(err) => writeln!(f, "baseline  NOT met: missing {}", err.missing.join(", "))?,
        }

        let features: Vec<_> = sys::FEATURES
            .iter()
            .map(|&(name, bit)| (name, self.has_features(bit)))
            .collect();
        write_line(
            f,
            "features",
            "IORING_FEAT_",
            "reported",
            "missing",
            &features,
        )?;
        let known = sys::FEATURES.iter().fold(0, |all, &(_, bit)| all | bit);
        if self.features & !known != 0 {
            writeln!(
                f,
                "          and unknown bits {:#x}",
                self.features & !known
            )?;
        }

        let probed = PROBED_SETUP_FLAGS
            .iter()
            .fold(0, |all, &(flag, _)| all | flag);
        let setup: Vec<_> = sys::SETUP_FLAGS
            .iter()
            .filter(|&&(_, flag)| probed & flag != 0)
            .map(|&(name, flag)| (name, self.supports_setup_flags(flag)))
            .collect();
        write_line(f, "setup", "IORING_SETUP_", "accepted", "rejected", &setup)?;

        let opcodes: Vec<_> = sys::OPCODES
            .iter()
            .map(|&(name, op)| (name, self.supports_opcode(op)))
            .collect();
        write_line(
            f,
            "opcodes",
            "IORING_OP_",
            "supported",
            "unsupported",
            &opcodes,
        )?;
        let last_known = sys::OPCODES.iter().map(|&(_, op)| op).max().unwrap_or(0);
        if self.last_opcode > last_known {
            writeln!(
                f,
                "          and {} opcodes ruio-sys does not know",
                self.last_opcode - last_known
            )?;
        }
        let incremental = if self.incremental_buffers {
            "supported"
        } else {
            "unsupported"
        };
        writeln!(f, "buffers   incremental consumption {incremental}")?;
        let vectorized = if self.vectorized_send {
            "supported"
        } else {
            "unsupported"
        };
        writeln!(f, "sends     vectorized {vectorized}")
    }
}

/// The kernel lacks io_uring capabilities that ruio requires.
///
/// ruio's baseline is Linux 6.10. The check looks at capabilities rather than
/// the version number, so kernels with backports pass and kernels built
/// without parts of io_uring fail.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BaselineError {
    kernel_release: String,
    missing: Vec<&'static str>,
}

impl BaselineError {
    /// The missing capabilities, as `ruio-sys` constant names.
    pub fn missing(&self) -> &[&'static str] {
        &self.missing
    }
}

impl fmt::Display for BaselineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "io_uring on kernel {} lacks {}; ruio needs the capabilities of Linux 6.10 or newer",
            self.kernel_release,
            self.missing.join(", ")
        )
    }
}

impl Error for BaselineError {}

/// Names the bits of `wanted` that `have` lacks.
fn missing_bits(
    table: &'static [(&'static str, u32)],
    wanted: u32,
    have: u32,
) -> impl Iterator<Item = &'static str> {
    table
        .iter()
        .filter(move |&&(_, bit)| wanted & bit != 0 && have & bit == 0)
        .map(|&(name, _)| name)
}

/// Writes one report line: how many of `items` are present, then the names,
/// without `prefix`, of those that are not.
fn write_line(
    f: &mut fmt::Formatter<'_>,
    label: &str,
    prefix: &str,
    present: &str,
    absent: &str,
    items: &[(&str, bool)],
) -> fmt::Result {
    let missing: Vec<&str> = items
        .iter()
        .filter(|&&(_, is_present)| !is_present)
        .map(|&(name, _)| name.strip_prefix(prefix).unwrap_or(name))
        .collect();
    write!(
        f,
        "{label:<10}{}/{} {prefix}* {present}",
        items.len() - missing.len(),
        items.len()
    )?;
    if !missing.is_empty() {
        write!(f, "; {absent}: {}", missing.join(" "))?;
    }
    writeln!(f)
}

/// Entries in each probe ring: the fewest every probed flag accepts, since
/// `SQE_MIXED` needs two submission and `CQE_MIXED` two completion entries.
const PROBE_ENTRIES: u32 = 2;

/// Creates a small ring with `flags`.
fn setup(flags: u32) -> io::Result<(OwnedFd, sys::io_uring_params)> {
    let mut params = sys::io_uring_params {
        flags,
        // Only read with IORING_SETUP_CQSIZE.
        cq_entries: PROBE_ENTRIES,
        ..Default::default()
    };
    // SAFETY: the probe never passes IORING_SETUP_NO_MMAP, so the kernel uses
    // no memory that `params` points to.
    let fd = unsafe { sys::io_uring_setup(PROBE_ENTRIES, &mut params) }?;
    // SAFETY: `fd` is a new descriptor that nothing else owns. The probe never
    // passes IORING_SETUP_REGISTERED_FD_ONLY, which would return an index.
    let ring = unsafe { OwnedFd::from_raw_fd(fd) };
    Ok((ring, params))
}

/// A probe buffer with room for every opcode an SQE can name.
#[repr(C)]
struct OpcodeProbe {
    header: sys::io_uring_probe,
    ops: [sys::io_uring_probe_op; 256],
}

const _: () = assert!(offset_of!(OpcodeProbe, ops) == offset_of!(sys::io_uring_probe, ops));

/// Reads the ring's opcode table as a 256-bit set, with the last opcode the
/// kernel knows.
fn probe_opcodes(ring: &OwnedFd) -> io::Result<([u64; 4], u8)> {
    let mut probe = OpcodeProbe {
        header: sys::io_uring_probe::default(),
        ops: [sys::io_uring_probe_op::default(); 256],
    };
    let arg = ptr::from_mut(&mut probe).cast();
    // SAFETY: `probe` is zeroed, as the kernel requires, with room for the
    // header and the 256 entries asked for; the kernel writes no more.
    unsafe { sys::io_uring_register(ring.as_raw_fd(), sys::IORING_REGISTER_PROBE, arg, 256) }?;

    let mut opcodes = [0; 4];
    for op in &probe.ops[..usize::from(probe.header.ops_len)] {
        if op.flags & sys::IO_URING_OP_SUPPORTED != 0 {
            opcodes[usize::from(op.op / 64)] |= 1 << (op.op % 64);
        }
    }
    Ok((opcodes, probe.header.last_op))
}

/// Whether the kernel accepts a buffer ring registered for incremental
/// consumption. The ring is the kernel's own memory, so the probe maps none,
/// and goes with the probe ring.
fn probe_incremental_buffers(ring: &OwnedFd) -> bool {
    let mut reg = sys::io_uring_buf_reg {
        ring_entries: 1,
        flags: sys::IOU_PBUF_RING_MMAP | sys::IOU_PBUF_RING_INC,
        ..Default::default()
    };
    let arg = ptr::from_mut(&mut reg).cast();
    // SAFETY: `reg` is one valid registration, of a ring the kernel
    // allocates: the kernel reads `reg` and no other memory of the process.
    let registered =
        unsafe { sys::io_uring_register(ring.as_raw_fd(), sys::IORING_REGISTER_PBUF_RING, arg, 1) };
    registered.is_ok()
}

/// Whether the kernel takes a vectorized send: one is issued on a ring of
/// its own, on no descriptor, and the kernel either rejects the flag as it
/// takes the entry, with `EINVAL`, or finds no file, with `EBADF`, once it
/// has read the vector.
fn probe_vectorized_send() -> bool {
    let Ok(mut ring) = Ring::new(PROBE_ENTRIES, PROBE_ENTRIES, 0) else {
        return false;
    };
    let mut byte = 0u8;
    let iovec = libc::iovec {
        iov_base: ptr::from_mut(&mut byte).cast(),
        iov_len: 1,
    };
    let Some(sqe) = ring.next_sqe() else {
        return false;
    };
    sqe.opcode = sys::IORING_OP_SEND;
    sqe.fd = -1;
    sqe.ioprio = sys::IORING_SEND_VECTORIZED;
    sqe.addr = ptr::from_ref(&iovec).expose_provenance() as u64;
    sqe.len = 1;
    // SAFETY: the entry points to `iovec`, which points to `byte`; both
    // outlive the call, which waits for the completion, and the kernel
    // reads the vector as it takes the entry and never gets to the byte,
    // having no file to send on.
    let entered = unsafe { ring.enter(1, None) };
    let completion = ring.completions().next();
    entered.is_ok() && completion.is_some_and(|cqe| cqe.res == -libc::EBADF)
}

fn kernel_release() -> String {
    fs::read_to_string("/proc/sys/kernel/osrelease").map_or_else(
        |_| "unknown".to_owned(),
        |release| release.trim().to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn probe(features: u32, setup_flags: u32, opcodes: &[u8]) -> Probe {
        let mut bits = [0; 4];
        for &op in opcodes {
            bits[usize::from(op / 64)] |= 1 << (op % 64);
        }
        Probe {
            kernel_release: "test".to_owned(),
            features,
            setup_flags,
            opcodes: bits,
            last_opcode: opcodes.iter().copied().max().unwrap_or(0),
            incremental_buffers: false,
            vectorized_send: false,
        }
    }

    #[test]
    fn running_kernel_meets_the_baseline() {
        let probe = Probe::detect().unwrap();
        if let Err(err) = probe.check_baseline() {
            panic!("{err}\n{probe}");
        }
        assert!(probe.supports_opcode(sys::IORING_OP_NOP));
        assert!(probe.has_features(sys::IORING_FEAT_NODROP | sys::IORING_FEAT_EXT_ARG));
        assert!(probe.supports_setup_flags(
            sys::IORING_SETUP_SINGLE_ISSUER | sys::IORING_SETUP_DEFER_TASKRUN
        ));
        assert!(probe.last_opcode() >= sys::IORING_OP_SENDMSG_ZC);
    }

    #[test]
    fn baseline_capabilities_meet_the_baseline() {
        let probe = probe(BASELINE_FEATURES, BASELINE_SETUP_FLAGS, BASELINE_OPCODES);
        assert_eq!(probe.check_baseline(), Ok(()));
    }

    /// Everything before 6.10 is not enough: the network path needs bundles.
    #[test]
    fn a_kernel_without_bundles_is_below_the_baseline() {
        let probe = probe(
            BASELINE_FEATURES & !sys::IORING_FEAT_RECVSEND_BUNDLE,
            BASELINE_SETUP_FLAGS,
            BASELINE_OPCODES,
        );
        let err = probe.check_baseline().unwrap_err();
        assert_eq!(err.missing(), ["IORING_FEAT_RECVSEND_BUNDLE"]);
    }

    #[test]
    fn baseline_error_names_what_is_missing() {
        let opcodes: Vec<u8> = BASELINE_OPCODES
            .iter()
            .copied()
            .filter(|&op| op != sys::IORING_OP_SEND_ZC)
            .collect();
        let probe = probe(
            BASELINE_FEATURES & !sys::IORING_FEAT_NODROP,
            BASELINE_SETUP_FLAGS & !sys::IORING_SETUP_DEFER_TASKRUN,
            &opcodes,
        );
        let err = probe.check_baseline().unwrap_err();
        assert_eq!(
            err.missing(),
            [
                "IORING_FEAT_NODROP",
                "IORING_SETUP_DEFER_TASKRUN",
                "IORING_OP_SEND_ZC"
            ]
        );
        assert!(err.to_string().contains("Linux 6.10"), "{err}");
    }

    /// A baseline capability without a name could not be reported missing,
    /// and a baseline setup flag that is never probed would always be.
    #[test]
    fn every_baseline_capability_is_named_and_probed() {
        let all = |table: &[(&str, u32)]| table.iter().fold(0, |all, &(_, bit)| all | bit);
        assert_eq!(BASELINE_FEATURES & !all(sys::FEATURES), 0);
        assert_eq!(BASELINE_SETUP_FLAGS & !all(sys::SETUP_FLAGS), 0);
        for op in BASELINE_OPCODES {
            assert!(sys::OPCODES.iter().any(|(_, known)| known == op), "{op}");
        }
        let probed = PROBED_SETUP_FLAGS
            .iter()
            .fold(0, |all, &(flag, _)| all | flag);
        assert_eq!(BASELINE_SETUP_FLAGS & !probed, 0);
    }

    #[test]
    fn only_probed_opcodes_are_supported() {
        let probe = probe(0, 0, &[sys::IORING_OP_NOP, 200]);
        assert!(probe.supports_opcode(sys::IORING_OP_NOP));
        assert!(probe.supports_opcode(200));
        assert!(!probe.supports_opcode(sys::IORING_OP_READ));
        assert!(!probe.supports_opcode(255));
    }

    #[test]
    fn report_names_what_is_missing() {
        let report = probe(sys::IORING_FEAT_NODROP, 0, &[sys::IORING_OP_NOP]).to_string();
        assert!(report.contains("baseline  NOT met"), "{report}");
        assert!(report.contains("missing: SINGLE_MMAP"), "{report}");
        assert!(report.contains("rejected: IOPOLL"), "{report}");
        assert!(report.contains("unsupported: READV"), "{report}");
    }
}
