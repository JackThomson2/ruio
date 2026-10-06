//! Checks the ABI definitions against the kernel's own header.
//!
//! The test writes a C program that prints the size, alignment and field
//! offsets of each structure and the value of each constant, compiles it
//! against `linux/io_uring.h`, and compares the output with the Rust
//! definitions. Names the header does not contain (it may predate them) are
//! skipped and listed.
//!
//! The header is `$RUIO_UAPI_HEADER` if set, otherwise the running kernel's
//! from its headers package, otherwise the copy in `/usr/include`. The C
//! compiler is `$CC`, or `cc`. Without a header or a compiler the test is
//! skipped, unless `RUIO_REQUIRE_ABI_CHECK` is set, which makes it fail.

use std::collections::HashMap;
use std::env;
use std::fmt::Write as _;
use std::fs;
use std::io::ErrorKind;
use std::mem::offset_of;
use std::path::{Path, PathBuf};
use std::process::Command;

use ruio_sys::*;

/// The layout of one structure as Rust sees it.
struct Layout {
    name: &'static str,
    size: usize,
    align: usize,
    /// Kernel field names, each with the offset of the Rust field holding it.
    fields: &'static [(&'static str, usize)],
    /// Whether the structure is declared in `io_uring.h` itself. Only names
    /// in that header are looked for in its text before being checked.
    in_header: bool,
}

macro_rules! layout {
    ($ty:ident, in_header: $in_header:expr, {
        $($field:ident => $rust:ident $(+ $extra:literal)?),* $(,)?
    }) => {
        Layout {
            name: stringify!($ty),
            size: size_of::<$ty>(),
            align: align_of::<$ty>(),
            fields: &[$((stringify!($field), offset_of!($ty, $rust) $(+ $extra)?)),*],
            in_header: $in_header,
        }
    };
}

const LAYOUTS: &[Layout] = &[
    layout!(io_uring_sqe, in_header: true, {
        opcode => opcode,
        flags => flags,
        ioprio => ioprio,
        fd => fd,
        off => off,
        addr2 => off,
        cmd_op => off,
        __pad1 => off + 4,
        addr => addr,
        splice_off_in => addr,
        level => addr,
        optname => addr + 4,
        len => len,
        rw_flags => op_flags,
        fsync_flags => op_flags,
        poll_events => op_flags,
        poll32_events => op_flags,
        sync_range_flags => op_flags,
        msg_flags => op_flags,
        timeout_flags => op_flags,
        accept_flags => op_flags,
        cancel_flags => op_flags,
        open_flags => op_flags,
        statx_flags => op_flags,
        fadvise_advice => op_flags,
        splice_flags => op_flags,
        rename_flags => op_flags,
        unlink_flags => op_flags,
        hardlink_flags => op_flags,
        xattr_flags => op_flags,
        msg_ring_flags => op_flags,
        uring_cmd_flags => op_flags,
        waitid_flags => op_flags,
        futex_flags => op_flags,
        install_fd_flags => op_flags,
        nop_flags => op_flags,
        pipe_flags => op_flags,
        user_data => user_data,
        buf_index => buf_index,
        buf_group => buf_index,
        personality => personality,
        splice_fd_in => file_index,
        file_index => file_index,
        zcrx_ifq_idx => file_index,
        optlen => file_index,
        addr_len => file_index,
        write_stream => file_index,
        addr3 => addr3,
        __pad2 => __pad2,
        attr_ptr => addr3,
        attr_type_mask => __pad2,
        optval => addr3,
        cmd => addr3,
    }),
    layout!(io_uring_cqe, in_header: true, {
        user_data => user_data,
        res => res,
        flags => flags,
        big_cqe => big_cqe,
    }),
    layout!(io_sqring_offsets, in_header: true, {
        head => head,
        tail => tail,
        ring_mask => ring_mask,
        ring_entries => ring_entries,
        flags => flags,
        dropped => dropped,
        array => array,
        resv1 => resv1,
        user_addr => user_addr,
    }),
    layout!(io_cqring_offsets, in_header: true, {
        head => head,
        tail => tail,
        ring_mask => ring_mask,
        ring_entries => ring_entries,
        overflow => overflow,
        cqes => cqes,
        flags => flags,
        resv1 => resv1,
        user_addr => user_addr,
    }),
    layout!(io_uring_params, in_header: true, {
        sq_entries => sq_entries,
        cq_entries => cq_entries,
        flags => flags,
        sq_thread_cpu => sq_thread_cpu,
        sq_thread_idle => sq_thread_idle,
        features => features,
        wq_fd => wq_fd,
        resv => resv,
        sq_off => sq_off,
        cq_off => cq_off,
    }),
    layout!(io_uring_probe_op, in_header: true, {
        op => op,
        resv => resv,
        flags => flags,
        resv2 => resv2,
    }),
    layout!(io_uring_probe, in_header: true, {
        last_op => last_op,
        ops_len => ops_len,
        resv => resv,
        resv2 => resv2,
        ops => ops,
    }),
    layout!(io_uring_getevents_arg, in_header: true, {
        sigmask => sigmask,
        sigmask_sz => sigmask_sz,
        min_wait_usec => min_wait_usec,
        ts => ts,
    }),
    layout!(io_uring_rsrc_register, in_header: true, {
        nr => nr,
        flags => flags,
        resv2 => resv2,
        data => data,
        tags => tags,
    }),
    layout!(io_uring_rsrc_update, in_header: true, {
        offset => offset,
        resv => resv,
        data => data,
    }),
    layout!(io_uring_buf, in_header: true, {
        addr => addr,
        len => len,
        bid => bid,
        resv => resv,
    }),
    layout!(io_uring_buf_ring, in_header: true, {
        resv1 => resv1,
        resv2 => resv2,
        resv3 => resv3,
        tail => tail,
        bufs => resv1,
    }),
    // `resv` is left to the size check: before 7.0 the header declares it as
    // three `u64`s at offset 16, where 7.0 has `min_left` and five `u32`s.
    layout!(io_uring_buf_reg, in_header: true, {
        ring_addr => ring_addr,
        ring_entries => ring_entries,
        bgid => bgid,
        flags => flags,
        min_left => min_left,
    }),
    layout!(io_uring_clone_buffers, in_header: true, {
        src_fd => src_fd,
        flags => flags,
        src_off => src_off,
        dst_off => dst_off,
        nr => nr,
        pad => pad,
    }),
    layout!(__kernel_timespec, in_header: false, {
        tv_sec => tv_sec,
        tv_nsec => tv_nsec,
    }),
];

/// Lists constant tables by name, with their values widened to `u64`.
macro_rules! tables {
    ($($table:ident),* $(,)?) => {
        vec![$((stringify!($table), widen($table))),*]
    };
}

fn widen<T: Copy + Into<u64>>(table: &[(&'static str, T)]) -> Vec<(&'static str, u64)> {
    table
        .iter()
        .map(|&(name, value)| (name, value.into()))
        .collect()
}

/// Every constant table, in the order `src/abi.rs` defines them.
fn tables() -> Vec<(&'static str, Vec<(&'static str, u64)>)> {
    tables![
        SETUP_FLAGS,
        FEATURES,
        OPCODES,
        SQE_FLAGS,
        CQE_FLAGS,
        ENTER_FLAGS,
        SQ_RING_FLAGS,
        CQ_RING_FLAGS,
        MMAP_OFFSETS,
        REGISTER_OPS,
        RSRC_REGISTER_FLAGS,
        CLONE_BUFFERS_FLAGS,
        FSYNC_FLAGS,
        TIMEOUT_FLAGS,
        POLL_FLAGS,
        ASYNC_CANCEL_FLAGS,
        RECVSEND_FLAGS,
        ACCEPT_FLAGS,
        PBUF_RING_FLAGS,
        MSG_RING_COMMANDS,
        MSG_RING_FLAGS,
        NOP_FLAGS,
        URING_CMD_FLAGS,
        SOCKET_URING_OPS,
        PROBE_OP_FLAGS,
        MISC_CONSTANTS,
    ]
}

/// A C program printing one `key value` line per check, with the value Rust
/// expects for each key.
struct Program<'a> {
    header_text: &'a str,
    source: String,
    expected: Vec<(String, u64)>,
    skipped: Vec<String>,
}

impl<'a> Program<'a> {
    fn new(header: &Path, header_text: &'a str) -> Self {
        let mut source = String::from("#include <stddef.h>\n#include <stdio.h>\n");
        writeln!(source, "#include \"{}\"", header.display()).unwrap();
        source.push_str("int main(void) {\n");
        Self {
            header_text,
            source,
            expected: Vec::new(),
            skipped: Vec::new(),
        }
    }

    /// Whether the header declares `name`, or the check applies regardless.
    fn present(&mut self, name: &str, always: bool) -> bool {
        let found = always || contains_word(self.header_text, name);
        if !found {
            self.skipped.push(name.to_owned());
        }
        found
    }

    fn check(&mut self, key: String, expression: &str, rust: u64) {
        writeln!(
            self.source,
            "    printf(\"{key} %llu\\n\", (unsigned long long)({expression}));"
        )
        .unwrap();
        self.expected.push((key, rust));
    }

    fn finish(mut self) -> (String, Vec<(String, u64)>, Vec<String>) {
        self.source.push_str("    return 0;\n}\n");
        (self.source, self.expected, self.skipped)
    }
}

fn contains_word(text: &str, word: &str) -> bool {
    let is_ident = |c: char| c.is_ascii_alphanumeric() || c == '_';
    text.match_indices(word).any(|(at, _)| {
        !text[..at].chars().next_back().is_some_and(is_ident)
            && !text[at + word.len()..].chars().next().is_some_and(is_ident)
    })
}

fn find_header() -> Option<PathBuf> {
    if let Some(path) = env::var_os("RUIO_UAPI_HEADER") {
        return Some(path.into());
    }
    let release = fs::read_to_string("/proc/sys/kernel/osrelease").unwrap_or_default();
    [
        format!(
            "/usr/src/linux-headers-{}/include/uapi/linux/io_uring.h",
            release.trim()
        ),
        "/usr/include/linux/io_uring.h".to_owned(),
    ]
    .into_iter()
    .map(PathBuf::from)
    .find(|path| path.is_file())
}

fn skip(reason: &str) {
    assert!(
        env::var_os("RUIO_REQUIRE_ABI_CHECK").is_none(),
        "RUIO_REQUIRE_ABI_CHECK is set, but the ABI check cannot run: {reason}"
    );
    eprintln!("skipping the ABI check: {reason}");
}

#[test]
fn abi_matches_kernel_header() {
    let Some(header) = find_header() else {
        return skip("no linux/io_uring.h found; set RUIO_UAPI_HEADER");
    };
    let header_text = fs::read_to_string(&header)
        .unwrap_or_else(|err| panic!("reading {}: {err}", header.display()));

    let mut program = Program::new(&header, &header_text);
    for layout in LAYOUTS {
        if !program.present(layout.name, !layout.in_header) {
            continue;
        }
        let name = layout.name;
        let ty = format!("struct {name}");
        program.check(
            format!("size {name}"),
            &format!("sizeof({ty})"),
            layout.size as u64,
        );
        program.check(
            format!("align {name}"),
            &format!("_Alignof({ty})"),
            layout.align as u64,
        );
        for &(field, offset) in layout.fields {
            if program.present(field, !layout.in_header) {
                let key = format!("offset {name}.{field}");
                program.check(key, &format!("offsetof({ty}, {field})"), offset as u64);
            }
        }
    }
    for (_, table) in tables() {
        for (name, value) in table {
            if program.present(name, false) {
                program.check(format!("value {name}"), name, value);
            }
        }
    }
    let (source, expected, skipped) = program.finish();

    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"));
    let source_path = dir.join("abi.c");
    let binary = dir.join("abi");
    fs::write(&source_path, source).unwrap();
    let cc = env::var("CC").unwrap_or_else(|_| "cc".to_owned());
    let compiled = match Command::new(&cc)
        .arg("-o")
        .arg(&binary)
        .arg(&source_path)
        .output()
    {
        Ok(output) => output,
        Err(err) if err.kind() == ErrorKind::NotFound => {
            return skip(&format!("no C compiler found ({cc}); set CC"));
        }
        Err(err) => panic!("running {cc}: {err}"),
    };
    assert!(
        compiled.status.success(),
        "compiling {} against {} failed:\n{}",
        source_path.display(),
        header.display(),
        String::from_utf8_lossy(&compiled.stderr)
    );
    let run = Command::new(&binary).output().unwrap();
    assert!(run.status.success(), "{} failed", binary.display());

    let stdout = String::from_utf8(run.stdout).unwrap();
    let actual: HashMap<&str, u64> = stdout
        .lines()
        .map(|line| {
            let (key, value) = line.rsplit_once(' ').unwrap();
            (key, value.parse().unwrap())
        })
        .collect();
    let mismatches: Vec<String> = expected
        .iter()
        .filter_map(|(key, rust)| {
            let c = actual[key.as_str()];
            (c != *rust).then(|| format!("{key}: Rust has {rust}, the header has {c}"))
        })
        .collect();
    assert!(
        mismatches.is_empty(),
        "ABI mismatches against {}:\n{}",
        header.display(),
        mismatches.join("\n")
    );

    eprintln!(
        "checked {} definitions against {}; not in that header: {}",
        expected.len(),
        header.display(),
        if skipped.is_empty() {
            "none".to_owned()
        } else {
            skipped.join(", ")
        }
    );
}

/// Every structure and constant table in `src/abi.rs` must be covered above,
/// so that a new definition cannot escape the header check.
#[test]
fn every_definition_is_checked() {
    let source = include_str!("../src/abi.rs");
    let declared = |prefix: &str, suffix: &str| -> Vec<String> {
        source
            .lines()
            .filter_map(|line| line.trim().strip_prefix(prefix)?.split_once(suffix))
            .map(|(name, _)| name)
            // Skips macro patterns such as `$table:ident`.
            .filter(|name| name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
            .map(str::to_owned)
            .collect()
    };

    let structs: Vec<&str> = LAYOUTS.iter().map(|layout| layout.name).collect();
    assert_eq!(declared("pub struct ", " {"), structs);

    let tables: Vec<&str> = tables().into_iter().map(|(name, _)| name).collect();
    assert_eq!(declared("pub const ", ": ["), tables);
}
