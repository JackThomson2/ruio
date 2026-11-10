//! Named pipes, read and written through the port.

use std::ffi::OsStr;
use std::fmt;
use std::fs;
use std::io;
use std::iter;
use std::os::windows::ffi::OsStrExt;
use std::os::windows::fs::OpenOptionsExt;
use std::os::windows::io::{FromRawHandle, OwnedHandle};
use std::ptr;

use ruio_buf::{IoBuf, IoBufMut};

use crate::handle::IoHandle;
use crate::op::{Read, Write};
use crate::sys;

/// How many bytes the system buffers each way.
const BUFFER: u32 = 64 * 1024;

/// One end of a named pipe, opened for overlapped I/O, whose reads and
/// writes complete to the port of the engine current where it was opened,
/// and which belongs to that engine's thread.
pub struct NamedPipe {
    handle: IoHandle<OwnedHandle>,
}

impl NamedPipe {
    /// Creates the pipe `name`, of the form `\\.\pipe\` and a name, as its
    /// first and only instance, for one client of this machine to
    /// [`connect`](NamedPipe::connect) to, and returns its server's end.
    ///
    /// # Errors
    ///
    /// What creating the pipe reports, as when it exists already, and an
    /// error of kind `Other` if the thread has no current engine.
    pub fn create(name: impl AsRef<OsStr>) -> io::Result<Self> {
        let name: Vec<u16> = name.as_ref().encode_wide().chain(iter::once(0)).collect();
        // SAFETY: the name is UTF-16 ended by a zero, and the pipe gets the
        // default security, given no attributes.
        let handle = unsafe {
            sys::CreateNamedPipeW(
                name.as_ptr(),
                sys::PIPE_ACCESS_DUPLEX
                    | sys::FILE_FLAG_OVERLAPPED
                    | sys::FILE_FLAG_FIRST_PIPE_INSTANCE,
                sys::PIPE_REJECT_REMOTE_CLIENTS,
                1,
                BUFFER,
                BUFFER,
                0,
                ptr::null(),
            )
        };
        if handle == sys::INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the handle was just opened, and nothing else owns it.
        let handle = unsafe { OwnedHandle::from_raw_handle(handle) };
        Ok(Self {
            handle: IoHandle::new(handle)?,
        })
    }

    /// Opens the client's end of the pipe `name`, which a server created.
    ///
    /// # Errors
    ///
    /// What opening the pipe reports, as when no server has created it,
    /// and an error of kind `Other` if the thread has no current engine.
    pub fn connect(name: impl AsRef<OsStr>) -> io::Result<Self> {
        let pipe = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(sys::FILE_FLAG_OVERLAPPED)
            .open(name.as_ref())?;
        Ok(Self {
            handle: IoHandle::new(OwnedHandle::from(pipe))?,
        })
    }

    /// Reads into `buf`, up to its `bytes_total`, what the other end has
    /// written; see [`Read`].
    pub fn read<B: IoBufMut>(&self, buf: B) -> Read<'_, B> {
        Read::new(self.handle.target(), buf, 0)
    }

    /// Writes `buf`'s bytes for the other end to read; see [`Write`].
    pub fn write<B: IoBuf>(&self, buf: B) -> Write<'_, B> {
        Write::new(self.handle.target(), buf, 0)
    }
}

impl fmt::Debug for NamedPipe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NamedPipe")
            .field("handle", &self.handle.raw())
            .finish()
    }
}
