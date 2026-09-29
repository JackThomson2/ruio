//! The process setup a fair benchmark needs: CPU pinning, enough file
//! descriptors, and `TCP_NODELAY`.

use std::io;
use std::mem;
use std::os::fd::RawFd;
use std::ptr;

/// Pins the calling thread, and every thread it starts from then on, to
/// `cpus`.
pub(crate) fn pin(cpus: &[usize]) -> io::Result<()> {
    // SAFETY: all zeroes is an empty CPU set.
    let mut set: libc::cpu_set_t = unsafe { mem::zeroed() };
    for &cpu in cpus {
        // SAFETY: `CPU_SET` writes inside the set, and ignores CPUs beyond
        // its size.
        unsafe { libc::CPU_SET(cpu, &mut set) };
    }
    // SAFETY: `sched_setaffinity` reads one CPU set of the size given.
    let pinned = unsafe { libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &set) };
    check(pinned)
}

/// Raises the soft limit on open files to the hard limit, for benchmarks
/// with thousands of connections.
pub(crate) fn raise_file_limit() -> io::Result<()> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `getrlimit` writes one `rlimit`.
    check(unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) })?;
    limit.rlim_cur = limit.rlim_max;
    // SAFETY: `setrlimit` reads one `rlimit`.
    check(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) })
}

/// Sets `TCP_NODELAY` on a listening socket; the connections it accepts
/// inherit it. Without it, Nagle's algorithm would hold back the last
/// segment of a multi-segment echo until the previous one is acknowledged,
/// which delayed acknowledgement puts off for tens of milliseconds.
pub(crate) fn set_nodelay(fd: RawFd) -> io::Result<()> {
    let on: libc::c_int = 1;
    // SAFETY: `setsockopt` reads the one `c_int` it is given.
    check(unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            ptr::from_ref(&on).cast(),
            size_of::<libc::c_int>() as libc::socklen_t,
        )
    })
}

fn check(ret: libc::c_int) -> io::Result<()> {
    if ret == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Parses a CPU list such as `2-9,12`.
pub(crate) fn parse_cpus(list: &str) -> Result<Vec<usize>, String> {
    let mut cpus = Vec::new();
    for part in list.split(',') {
        let (first, last) = part.split_once('-').unwrap_or((part, part));
        let parse = |cpu: &str| {
            cpu.trim()
                .parse::<usize>()
                .map_err(|err| format!("bad CPU list {list:?}: {err}"))
        };
        let (first, last) = (parse(first)?, parse(last)?);
        if first > last {
            return Err(format!("bad CPU list {list:?}: {first} comes after {last}"));
        }
        cpus.extend(first..=last);
    }
    Ok(cpus)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_lists_parse() {
        assert_eq!(parse_cpus("2-4,7").unwrap(), [2, 3, 4, 7]);
        assert_eq!(parse_cpus("0").unwrap(), [0]);
        assert!(parse_cpus("4-2").is_err());
        assert!(parse_cpus("x").is_err());
    }
}
