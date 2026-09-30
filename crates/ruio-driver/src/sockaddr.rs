//! Socket addresses in the form the kernel reads and writes them.

use std::ffi::OsStr;
use std::fmt;
use std::io;
use std::mem::{self, offset_of};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::os::linux::net::SocketAddrExt;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net;
use std::path::Path;
use std::ptr;

/// Where the path starts in a `sockaddr_un`.
const SUN_PATH_OFFSET: usize = offset_of!(libc::sockaddr_un, sun_path);

/// The room a `sockaddr_un` has for a path or an abstract name, which a path
/// must leave a byte of for its terminating nul.
const SUN_PATH_LEN: usize = size_of::<libc::sockaddr_un>() - SUN_PATH_OFFSET;

/// A socket address as the kernel reads and writes it: an IPv4, IPv6 or Unix
/// socket address, or one of any other family a socket reports.
///
/// Operations that take an address, such as [`Connect`](crate::ops::Connect),
/// and those that report one, such as [`Accept`](crate::ops::Accept), keep it
/// in memory of their own until the kernel is done with it. The conversions
/// to and from the standard library's address types are exact.
#[derive(Clone, Copy)]
pub struct SockAddr {
    storage: libc::sockaddr_storage,
    len: libc::socklen_t,
}

impl SockAddr {
    /// An address with room for any family and no content, for the kernel
    /// to fill in.
    pub(crate) fn empty() -> Self {
        Self {
            // SAFETY: all zeroes is a valid `sockaddr_storage`.
            storage: unsafe { mem::zeroed() },
            len: size_of::<libc::sockaddr_storage>() as libc::socklen_t,
        }
    }

    /// A Unix socket address for the path `path`.
    ///
    /// # Errors
    ///
    /// `InvalidInput` if the path contains a nul byte or does not fit in a
    /// `sockaddr_un`, which has room for 107 bytes.
    pub fn unix(path: impl AsRef<Path>) -> io::Result<Self> {
        let bytes = path.as_ref().as_os_str().as_bytes();
        if bytes.contains(&0) {
            return Err(invalid("a Unix socket path must not contain nul bytes"));
        }
        if bytes.len() >= SUN_PATH_LEN {
            return Err(invalid("the Unix socket path is too long"));
        }
        // An empty path is the unnamed address, which has no terminator.
        let terminator = usize::from(!bytes.is_empty());
        Ok(Self::unix_from(bytes, bytes.len() + terminator))
    }

    /// A Unix socket address in the abstract namespace, which has no file:
    /// `name` is the name without the leading nul byte.
    ///
    /// # Errors
    ///
    /// `InvalidInput` if the name does not fit in a `sockaddr_un`, which has
    /// room for 107 bytes.
    pub fn unix_abstract(name: &[u8]) -> io::Result<Self> {
        if name.len() >= SUN_PATH_LEN {
            return Err(invalid("the abstract Unix socket name is too long"));
        }
        let mut path = [0; SUN_PATH_LEN];
        path[1..=name.len()].copy_from_slice(name);
        Ok(Self::unix_from(&path[..=name.len()], name.len() + 1))
    }

    /// The Unix socket address that is `sun_path` up to `len` bytes, with
    /// `path` at its start and zeroes after.
    fn unix_from(path: &[u8], len: usize) -> Self {
        let mut addr = Self::empty();
        let family = libc::AF_UNIX as libc::sa_family_t;
        let bytes = addr.bytes_mut();
        bytes[..size_of::<libc::sa_family_t>()].copy_from_slice(&family.to_ne_bytes());
        bytes[SUN_PATH_OFFSET..SUN_PATH_OFFSET + path.len()].copy_from_slice(path);
        addr.len = (SUN_PATH_OFFSET + len) as libc::socklen_t;
        addr
    }

    /// The address family, such as `AF_INET`.
    pub fn family(&self) -> libc::sa_family_t {
        self.storage.ss_family
    }

    /// The address's length, as the kernel counts it.
    pub fn len(&self) -> usize {
        self.len as usize
    }

    /// Whether the address is empty, as that of a socket with no family.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The IPv4 or IPv6 address, if it is one.
    pub fn as_inet(&self) -> Option<SocketAddr> {
        match i32::from(self.family()) {
            libc::AF_INET if self.len() >= size_of::<libc::sockaddr_in>() => {
                // SAFETY: the storage is large and aligned enough for any
                // address, holds an IPv4 one, and any bytes are a valid
                // `sockaddr_in`.
                let addr = unsafe {
                    ptr::from_ref(&self.storage)
                        .cast::<libc::sockaddr_in>()
                        .read()
                };
                let ip = Ipv4Addr::from(addr.sin_addr.s_addr.to_ne_bytes());
                Some(SocketAddrV4::new(ip, u16::from_be(addr.sin_port)).into())
            }
            libc::AF_INET6 if self.len() >= size_of::<libc::sockaddr_in6>() => {
                // SAFETY: as above, for an IPv6 address.
                let addr = unsafe {
                    ptr::from_ref(&self.storage)
                        .cast::<libc::sockaddr_in6>()
                        .read()
                };
                let ip = Ipv6Addr::from(addr.sin6_addr.s6_addr);
                let port = u16::from_be(addr.sin6_port);
                Some(SocketAddrV6::new(ip, port, addr.sin6_flowinfo, addr.sin6_scope_id).into())
            }
            _ => None,
        }
    }

    /// The Unix socket address, if it is one.
    ///
    /// An empty address counts as the unnamed one: it is what the kernel
    /// reports for a datagram from a socket without an address.
    pub fn as_unix(&self) -> Option<net::SocketAddr> {
        if self.is_empty() {
            return net::SocketAddr::from_pathname("").ok();
        }
        if i32::from(self.family()) != libc::AF_UNIX {
            return None;
        }
        let path = self.bytes().get(SUN_PATH_OFFSET..self.len())?;
        match path.split_first() {
            // An empty path makes the unnamed address.
            None => net::SocketAddr::from_pathname("").ok(),
            Some((0, name)) => net::SocketAddr::from_abstract_name(name).ok(),
            Some(_) => {
                let end = path
                    .iter()
                    .position(|&byte| byte == 0)
                    .unwrap_or(path.len());
                let path = Path::new(OsStr::from_bytes(&path[..end]));
                net::SocketAddr::from_pathname(path).ok()
            }
        }
    }

    /// A pointer to the address, for the kernel to read [`len`](Self::len)
    /// bytes of: in an entry, or a system call such as `bind(2)`.
    pub fn as_ptr(&self) -> *const libc::sockaddr {
        ptr::from_ref(&self.storage).cast()
    }

    /// Pointers to the address and its length, for the kernel to write
    /// through.
    pub(crate) fn as_mut_ptrs(&mut self) -> (*mut libc::sockaddr, *mut libc::socklen_t) {
        (ptr::from_mut(&mut self.storage).cast(), &raw mut self.len)
    }

    /// Records the length the kernel wrote the address with.
    pub(crate) fn set_len(&mut self, len: libc::socklen_t) {
        self.len = len.min(size_of::<libc::sockaddr_storage>() as libc::socklen_t);
    }

    fn bytes(&self) -> &[u8] {
        // SAFETY: the storage is plain bytes, every one initialized.
        unsafe {
            std::slice::from_raw_parts(
                ptr::from_ref(&self.storage).cast(),
                size_of::<libc::sockaddr_storage>(),
            )
        }
    }

    fn bytes_mut(&mut self) -> &mut [u8] {
        // SAFETY: as in `bytes`, and any bytes are a valid storage.
        unsafe {
            std::slice::from_raw_parts_mut(
                ptr::from_mut(&mut self.storage).cast(),
                size_of::<libc::sockaddr_storage>(),
            )
        }
    }
}

impl From<SocketAddr> for SockAddr {
    fn from(addr: SocketAddr) -> Self {
        let mut sockaddr = Self::empty();
        let storage = ptr::from_mut(&mut sockaddr.storage);
        match addr {
            SocketAddr::V4(addr) => {
                let addr = libc::sockaddr_in {
                    sin_family: libc::AF_INET as libc::sa_family_t,
                    sin_port: addr.port().to_be(),
                    sin_addr: libc::in_addr {
                        s_addr: u32::from_ne_bytes(addr.ip().octets()),
                    },
                    sin_zero: [0; 8],
                };
                // SAFETY: the storage is large and aligned enough for any
                // address.
                unsafe { storage.cast::<libc::sockaddr_in>().write(addr) };
                sockaddr.len = size_of::<libc::sockaddr_in>() as libc::socklen_t;
            }
            SocketAddr::V6(addr) => {
                let addr = libc::sockaddr_in6 {
                    sin6_family: libc::AF_INET6 as libc::sa_family_t,
                    sin6_port: addr.port().to_be(),
                    sin6_flowinfo: addr.flowinfo(),
                    sin6_addr: libc::in6_addr {
                        s6_addr: addr.ip().octets(),
                    },
                    sin6_scope_id: addr.scope_id(),
                };
                // SAFETY: as above.
                unsafe { storage.cast::<libc::sockaddr_in6>().write(addr) };
                sockaddr.len = size_of::<libc::sockaddr_in6>() as libc::socklen_t;
            }
        }
        sockaddr
    }
}

impl From<SocketAddrV4> for SockAddr {
    fn from(addr: SocketAddrV4) -> Self {
        SocketAddr::V4(addr).into()
    }
}

impl From<SocketAddrV6> for SockAddr {
    fn from(addr: SocketAddrV6) -> Self {
        SocketAddr::V6(addr).into()
    }
}

impl TryFrom<&net::SocketAddr> for SockAddr {
    type Error = io::Error;

    fn try_from(addr: &net::SocketAddr) -> io::Result<Self> {
        if let Some(path) = addr.as_pathname() {
            Self::unix(path)
        } else if let Some(name) = addr.as_abstract_name() {
            Self::unix_abstract(name)
        } else {
            Self::unix("")
        }
    }
}

impl PartialEq for SockAddr {
    fn eq(&self, other: &Self) -> bool {
        self.bytes()[..self.len()] == other.bytes()[..other.len()]
    }
}

impl Eq for SockAddr {}

impl fmt::Debug for SockAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(addr) = self.as_inet() {
            fmt::Debug::fmt(&addr, f)
        } else if let Some(addr) = self.as_unix() {
            fmt::Debug::fmt(&addr, f)
        } else {
            f.debug_struct("SockAddr")
                .field("family", &self.family())
                .field("len", &self.len)
                .finish_non_exhaustive()
        }
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internet_addresses_convert_both_ways() {
        let addrs: [SocketAddr; 3] = [
            "127.0.0.1:8080".parse().unwrap(),
            "[::1]:443".parse().unwrap(),
            SocketAddrV6::new("fe80::1".parse().unwrap(), 9, 7, 3).into(),
        ];
        for addr in addrs {
            let sockaddr = SockAddr::from(addr);
            assert_eq!(sockaddr.as_inet(), Some(addr));
            assert!(sockaddr.as_unix().is_none());
        }
        let v4 = SockAddr::from(addrs[0]);
        assert_eq!(i32::from(v4.family()), libc::AF_INET);
        assert_eq!(v4.len(), 16);
        // The port and address are in network byte order.
        assert_eq!(&v4.bytes()[2..8], [0x1f, 0x90, 127, 0, 0, 1]);
    }

    #[test]
    fn unix_addresses_convert_both_ways() {
        let path = SockAddr::unix("/tmp/ruio.sock").unwrap();
        assert_eq!(path.len(), SUN_PATH_OFFSET + 15);
        let unix = path.as_unix().unwrap();
        assert_eq!(unix.as_pathname(), Some(Path::new("/tmp/ruio.sock")));
        assert_eq!(SockAddr::try_from(&unix).unwrap(), path);

        let name = SockAddr::unix_abstract(b"ruio").unwrap();
        assert_eq!(name.len(), SUN_PATH_OFFSET + 5);
        let unix = name.as_unix().unwrap();
        assert_eq!(unix.as_abstract_name(), Some(&b"ruio"[..]));
        assert_eq!(SockAddr::try_from(&unix).unwrap(), name);

        let unnamed = SockAddr::unix("").unwrap();
        assert_eq!(unnamed.len(), SUN_PATH_OFFSET);
        let unix = unnamed.as_unix().unwrap();
        assert!(unix.is_unnamed());
        assert_eq!(SockAddr::try_from(&unix).unwrap(), unnamed);
        assert!(unnamed.as_inet().is_none());

        let mut empty = SockAddr::empty();
        empty.set_len(0);
        assert!(empty.as_unix().unwrap().is_unnamed());
        assert!(empty.as_inet().is_none());
    }

    #[test]
    fn unix_addresses_must_fit() {
        let longest = "x".repeat(SUN_PATH_LEN - 1);
        assert!(SockAddr::unix(&longest).is_ok());
        let err = SockAddr::unix(format!("{longest}x")).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(SockAddr::unix("nul\0byte").is_err());
        assert!(SockAddr::unix_abstract(&[1; SUN_PATH_LEN - 1]).is_ok());
        assert!(SockAddr::unix_abstract(&[1; SUN_PATH_LEN]).is_err());
    }

    #[test]
    fn a_kernel_address_is_read_up_to_its_length() {
        let mut addr = SockAddr::unix("/tmp/abc").unwrap();
        // The kernel may report the path without its terminator.
        addr.set_len((SUN_PATH_OFFSET + 8) as libc::socklen_t);
        assert_eq!(
            addr.as_unix().unwrap().as_pathname(),
            Some(Path::new("/tmp/abc"))
        );
        addr.set_len(u32::MAX);
        assert_eq!(addr.len(), size_of::<libc::sockaddr_storage>());
    }
}
