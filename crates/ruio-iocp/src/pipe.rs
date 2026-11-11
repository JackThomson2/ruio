//! Named pipes, read and written through the port.

use std::ffi::OsStr;
use std::fmt;
use std::fs;
use std::future::poll_fn;
use std::io;
use std::iter;
use std::os::windows::ffi::OsStrExt;
use std::os::windows::fs::OpenOptionsExt;
use std::os::windows::io::{FromRawHandle, OwnedHandle};
use std::ptr;

use ruio_buf::{IoBuf, IoBufMut};

use crate::handle::IoHandle;
use crate::op::{Op, Read, Started, Write, status_outcome, taken};
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
    /// A client may open the pipe as soon as it exists, and
    /// [`wait_for_client`](NamedPipe::wait_for_client) waits for one to.
    /// Until one has, this end's reads and writes fail, with
    /// `ERROR_PIPE_LISTENING`.
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

    /// Waits, on the server's end, for a client to open the pipe, which
    /// one may have done already. Dropping the future before then cancels
    /// the wait.
    ///
    /// # Errors
    ///
    /// What the system reports, as on a client's end, or where the client
    /// has opened the pipe and closed it again by then (`ERROR_NO_DATA`).
    pub async fn wait_for_client(&self) -> io::Result<()> {
        let mut op = Op::new(self.handle.target(), (), 0);
        let (result, ()) = poll_fn(|cx| op.poll_with(cx, start_wait, status_outcome)).await;
        match result {
            Ok(_) => Ok(()),
            // A client that opened the pipe before the wait has connected.
            Err(err) if err.raw_os_error() == Some(sys::ERROR_PIPE_CONNECTED as i32) => Ok(()),
            Err(err) => Err(err),
        }
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

/// Starts waiting on `pipe`, a server's end, for a client to open the pipe,
/// as `op`'s `Start` says. A client that has opened it already fails the
/// call with `ERROR_PIPE_CONNECTED`, which queues nothing to the port.
///
/// # Safety
///
/// As for `Start`.
unsafe fn start_wait(
    pipe: sys::Handle,
    _: &mut (),
    overlapped: *mut sys::Overlapped,
) -> io::Result<Started> {
    // SAFETY: the caller's promise for the pipe's end and the `OVERLAPPED`.
    let ok = unsafe { sys::ConnectNamedPipe(pipe, overlapped) };
    taken(ok, 0)
}
