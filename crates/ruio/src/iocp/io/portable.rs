//! The readiness that a foreign reactor's [`Registration`](super::Registration)
//! waits for: [`Interest`], as it has on io_uring.
//!
//! Not here yet, of what this module has on io_uring: `AsyncFd`, which
//! wraps a descriptor of any kind so that a task that must be `Send` can
//! wait for it to be ready, its guards, and the `Ready` they speak in.

use std::fmt;
use std::ops;

/// The kinds of readiness a wait asks for: readable, writable, priority or
/// error, or several of them joined with [`add`](Interest::add) or `|`.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Interest(u8);

impl Interest {
    /// Readable: data to receive, a connection to accept, or the end.
    pub const READABLE: Interest = Interest(1);
    /// Writable: room to send, or a connection made or failed.
    pub const WRITABLE: Interest = Interest(1 << 1);
    /// Priority: urgent data, such as a socket's out-of-band data.
    pub const PRIORITY: Interest = Interest(1 << 2);
    /// Error: an error pending on the socket.
    pub const ERROR: Interest = Interest(1 << 3);

    /// Whether it asks for readable.
    pub const fn is_readable(self) -> bool {
        self.0 & Self::READABLE.0 != 0
    }

    /// Whether it asks for writable.
    pub const fn is_writable(self) -> bool {
        self.0 & Self::WRITABLE.0 != 0
    }

    /// Whether it asks for priority.
    pub const fn is_priority(self) -> bool {
        self.0 & Self::PRIORITY.0 != 0
    }

    /// Whether it asks for error.
    pub const fn is_error(self) -> bool {
        self.0 & Self::ERROR.0 != 0
    }

    /// What it asks for and what `other` does.
    ///
    /// ```
    /// use ruio::io::portable::Interest;
    ///
    /// const BOTH: Interest = Interest::READABLE.add(Interest::WRITABLE);
    /// assert!(BOTH.is_readable() && BOTH.is_writable());
    /// ```
    #[must_use = "this returns the joined interest, without changing either"]
    pub const fn add(self, other: Interest) -> Interest {
        Interest(self.0 | other.0)
    }

    /// What it asks for and `other` does not, or `None` if that is nothing.
    #[must_use = "this returns the remaining interest, without changing either"]
    pub fn remove(self, other: Interest) -> Option<Interest> {
        let rest = self.0 & !other.0;
        (rest != 0).then_some(Interest(rest))
    }
}

impl ops::BitOr for Interest {
    type Output = Interest;

    #[inline]
    fn bitor(self, other: Interest) -> Interest {
        self.add(other)
    }
}

impl ops::BitOrAssign for Interest {
    #[inline]
    fn bitor_assign(&mut self, other: Interest) {
        *self = self.add(other);
    }
}

impl fmt::Debug for Interest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names = [
            (Self::READABLE, "READABLE"),
            (Self::WRITABLE, "WRITABLE"),
            (Self::PRIORITY, "PRIORITY"),
            (Self::ERROR, "ERROR"),
        ];
        let mut first = true;
        for (interest, name) in names {
            if self.0 & interest.0 != 0 {
                if !first {
                    f.write_str(" | ")?;
                }
                f.write_str(name)?;
                first = false;
            }
        }
        if first {
            f.write_str("(empty)")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Interest;

    #[test]
    fn interests_join_split_and_name_themselves() {
        let both = Interest::READABLE | Interest::WRITABLE;
        assert!(both.is_readable() && both.is_writable() && !both.is_priority());
        assert_eq!(both.remove(Interest::READABLE), Some(Interest::WRITABLE));
        assert_eq!(Interest::ERROR.remove(Interest::ERROR), None);
        let mut all = both;
        all |= Interest::PRIORITY.add(Interest::ERROR);
        assert!(all.is_priority() && all.is_error());
        assert_eq!(format!("{both:?}"), "READABLE | WRITABLE");
    }
}
