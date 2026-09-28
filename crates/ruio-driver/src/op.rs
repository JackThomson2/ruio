//! Operations and their results.

use std::io;

/// The result the kernel posted for an operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Completion {
    res: i32,
    flags: u32,
}

impl Completion {
    /// A completion with the given result and `IORING_CQE_F_*` flags.
    pub const fn new(res: i32, flags: u32) -> Self {
        Self { res, flags }
    }

    /// The raw result: what the equivalent system call returns, or a negated
    /// errno.
    pub fn res(&self) -> i32 {
        self.res
    }

    /// The `IORING_CQE_F_*` flags.
    pub fn flags(&self) -> u32 {
        self.flags
    }

    /// The result as an [`io::Result`]: the value if it is not negative,
    /// otherwise the error its negation names.
    pub fn result(&self) -> io::Result<u32> {
        u32::try_from(self.res).map_err(|_| io::Error::from_raw_os_error(-self.res))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_decodes_errors() {
        assert_eq!(Completion::new(7, 0).result().unwrap(), 7);
        let err = Completion::new(-2, 0).result().unwrap_err();
        assert_eq!(err.raw_os_error(), Some(2));
    }
}
