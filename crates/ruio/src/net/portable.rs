//! Sockets that are `Send` and `Sync`: TCP and Unix streams, and the
//! listeners that accept them, which a future that must be `Send`, one that
//! `async_trait` boxes or one spawned onto another core, can hold across an
//! `.await`, and any thread can read and write.
//!
//! A portable socket is a plain descriptor, made nonblocking. Its reads and
//! writes are system calls made at once, on whichever thread polls them,
//! which take what has arrived or give what there is room for, with no turn
//! of a ring's loop; only one that finds nothing to read, or no room, waits,
//! with a poll on the ring of the thread that polls it
//! ([`PortableFd::poll_ready`](crate::uring::PortableFd::poll_ready)), and
//! is made again once the socket is ready. A future that moves to another
//! thread mid-wait takes the wait along, which its ring finishes, as for any
//! portable operation. Each read and write that moves data spends a step of
//! the task's budget ([`coop`]), so a peer that keeps a socket
//! full cannot hold up the thread. Nothing is read but what a read returns,
//! so a read dropped while it waits loses nothing.
//!
//! A thread where no runtime runs, one of another executor's say, reads and
//! writes a portable socket the same way, and its waits go to the foreign
//! ring the embedder set ([`set_foreign_ring`](crate::uring::set_foreign_ring)),
//! whose thread sends the readiness back; with none set, a wait fails with
//! `ENOSYS`, whose kind is [`io::ErrorKind::Unsupported`], so the caller can
//! do its I/O another way.
//!
//! The streams read into and write from owned buffers, as the thread's
//! sockets do, and also, through their poll methods, into and from the
//! caller's own, with no copy: what `ruio-compat`'s adapter gives hyper and
//! the poll-based I/O traits. Connecting is a `connect(2)` made at once,
//! which may connect at once, as over loopback, and otherwise waits for the
//! socket to be writable, as it is once the connection is made or has
//! failed; accepting is an `accept4(2)`, made at once, waiting while there
//! is no connection to take. Socket options and addresses are plain system
//! calls, made with no ring and no `.await`. A socket closes after the polls
//! queued for it, on the ring that queued the last, as a
//! [`PortableFd`](crate::uring::PortableFd) does.
//!
//! The sockets of [`net`](super), with direct descriptors, multishot
//! receives into pools of buffers and writes behind, are what a server on
//! one core is fastest with: they belong to their thread's ring. These are
//! for code that has to be `Send`.
//!
//! ```no_run
//! use ruio::net::portable::{TcpListener, TcpStream};
//!
//! fn assert_send<T: Send + Sync>(_: &T) {}
//!
//! # ruio::Runtime::new().unwrap().block_on(async {
//! let listener = TcpListener::bind("127.0.0.1:0")?;
//! let addr = listener.local_addr()?;
//! let echo = ruio::spawn(async move {
//!     let (stream, _) = listener.accept().await?;
//!     let (read, buf) = stream.read(Vec::with_capacity(64)).await;
//!     read?;
//!     stream.write_all(buf).await.0
//! });
//! let stream = TcpStream::connect(addr).await?;
//! assert_send(&stream);
//! // A future that must be `Send`, holding the stream across its awaits.
//! let exchange: std::pin::Pin<Box<dyn Future<Output = std::io::Result<Vec<u8>>> + Send + '_>> =
//!     Box::pin(async {
//!         stream.write_all("hello").await.0?;
//!         let (read, reply) = stream.read_exact(Vec::with_capacity(5)).await;
//!         read.map(|()| reply)
//!     });
//! assert_eq!(exchange.await?, b"hello");
//! echo.await.unwrap()?;
//! # Ok::<(), std::io::Error>(())
//! # }).unwrap();
//! ```

use std::io;
use std::mem;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::slice;
use std::task::{Context, Poll, ready};

use ruio_driver::{ReadyWait, SockAddr};

use crate::coop;
use crate::io::{Kind, Nonblocking};
use crate::net::socket::getsockopt;

/// The events a read, and an accept, wait for.
const READABLE: u32 = libc::POLLIN as u32;

/// The events a write, and a connect, wait for.
const WRITABLE: u32 = libc::POLLOUT as u32;

/// The methods every portable stream has, on its `stream`, a [`Stream`].
macro_rules! stream_methods {
    () => {
        /// Reads into `buf`, up to its capacity, what has arrived: a system
        /// call made at once, which waits on the ring of the thread polling it
        /// only if nothing has. Reads 0 bytes once the peer has shut its
        /// writing half down.
        #[inline]
        pub fn read<B: IoBufMut>(&self, buf: B) -> PortableRead<'_, B> {
            self.stream.io.read(buf)
        }

        /// Reads until `buf` is full, with as many reads as it takes, each a
        /// system call made at once, waiting whenever nothing has arrived;
        /// `UnexpectedEof` if the stream ends first. See
        /// [`PortableReadExact`].
        #[inline]
        pub fn read_exact<B: IoBufMut>(&self, buf: B) -> PortableReadExact<'_, B> {
            self.stream.io.read_exact(buf)
        }

        /// Writes `buf`'s initialized bytes, or as many as there is room for:
        /// a system call made at once, which waits on the ring of the thread
        /// polling it only if there is no room.
        #[inline]
        pub fn write<B: IoBuf>(&self, buf: B) -> PortableWrite<'_, B> {
            self.stream.io.write(buf)
        }

        /// Writes all of `buf`'s initialized bytes, with as many writes as it
        /// takes, each a system call made at once, waiting whenever there is
        /// no room. See [`PortableWriteAll`].
        #[inline]
        pub fn write_all<B: IoBuf>(&self, buf: B) -> PortableWriteAll<'_, B> {
            self.stream.io.write_all(buf)
        }

        /// Writes every initialized byte of `bufs`, one after the other, with
        /// as many vectored writes as it takes, each of up to 64 buffers.
        #[inline]
        pub fn write_all_vectored<V: IoBufs>(&self, bufs: V) -> PortableWriteAllVectored<'_, V> {
            self.stream.io.write_all_vectored(bufs)
        }

        /// Reads into the caller's `buf` what has arrived, as
        /// [`read`](Self::read) does, polled: a system call made at once, and
        /// if nothing has arrived, a wait the stream keeps until it is
        /// polled again, so that the next poll goes on with it. The first
        /// bytes of `buf`, as many as were read, are then initialized.
        ///
        /// The waits of the poll methods are the stream's own, which they
        /// borrow it mutably to keep; the owned-buffer methods have their
        /// own. A wait runs where a portable operation does: on the ring of
        /// the thread polling it, or on the foreign ring where no runtime
        /// runs; see the [module's documentation](super).
        pub fn poll_read(
            &mut self,
            cx: &mut Context<'_>,
            buf: &mut [MaybeUninit<u8>],
        ) -> Poll<io::Result<usize>> {
            let stream = &mut self.stream;
            stream.io.poll_read(cx, buf, &mut stream.read)
        }

        /// Writes as much of the caller's `buf` as there is room for, as
        /// [`write`](Self::write) does, polled; see
        /// [`poll_read`](Self::poll_read).
        pub fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
            let stream = &mut self.stream;
            stream.io.poll_write(cx, buf, &mut stream.write)
        }

        /// Writes as much of the caller's `bufs`, one after the other, as
        /// there is room for, with one `sendmsg(2)` of up to 1024 of them;
        /// see [`poll_write`](Self::poll_write).
        pub fn poll_write_vectored(
            &mut self,
            cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            let stream = &mut self.stream;
            stream.io.poll_write_vectored(cx, bufs, &mut stream.write)
        }

        /// Reads into `buf` what has arrived, with one system call and no
        /// wait: `WouldBlock` if nothing has.
        pub fn try_read(&self, buf: &mut [u8]) -> io::Result<usize> {
            // SAFETY: the read writes none but initialized bytes, so the
            // bytes stay initialized.
            let buf = unsafe { &mut *(std::ptr::from_mut(buf) as *mut [MaybeUninit<u8>]) };
            self.stream.io.try_read(buf)
        }

        /// Writes as much of `buf` as there is room for, with one system call
        /// and no wait: `WouldBlock` if there is none.
        pub fn try_write(&self, buf: &[u8]) -> io::Result<usize> {
            self.stream.io.try_write(buf)
        }

        /// Writes as much of `bufs` as there is room for, with one system
        /// call and no wait: `WouldBlock` if there is none.
        pub fn try_write_vectored(&self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
            self.stream.io.try_write_vectored(bufs)
        }

        /// Waits for the stream to be readable: for data, the end, or an
        /// error to have arrived, which a read then takes, with a poll on the
        /// ring of the thread polling it. Another reader may take it first,
        /// so a [`try_read`](Self::try_read) after it can still find nothing.
        ///
        /// # Errors
        ///
        /// What the kernel reports for the poll.
        pub async fn readable(&self) -> io::Result<()> {
            self.stream.io.fd().ready(super::READABLE).await.map(drop)
        }

        /// Waits for the stream to be writable, as
        /// [`readable`](Self::readable) waits for it to be readable.
        ///
        /// # Errors
        ///
        /// What the kernel reports for the poll.
        pub async fn writable(&self) -> io::Result<()> {
            self.stream.io.fd().ready(super::WRITABLE).await.map(drop)
        }

        /// Shuts down the reading half, the writing half, or both, with one
        /// `shutdown(2)`: the peer reads the end once it has read what was
        /// written before.
        ///
        /// # Errors
        ///
        /// What `shutdown(2)` reports: `NotConnected` if the stream is not.
        pub fn shutdown(&self, how: Shutdown) -> io::Result<()> {
            super::shutdown(&self.stream.io, how)
        }

        /// The error pending on the socket, which reading `SO_ERROR` clears.
        ///
        /// # Errors
        ///
        /// What `getsockopt(2)` reports.
        pub fn take_error(&self) -> io::Result<Option<io::Error>> {
            super::take_error(&self.stream.io)
        }
    };
}

/// The trait implementations every portable stream has.
macro_rules! stream_impls {
    ($stream:ident) => {
        impl AsRawFd for $stream {
            #[inline]
            fn as_raw_fd(&self) -> RawFd {
                self.stream.io.fd().as_raw_fd()
            }
        }

        impl AsFd for $stream {
            #[inline]
            fn as_fd(&self) -> BorrowedFd<'_> {
                self.stream.io.fd().as_fd()
            }
        }

        impl AsyncRead for $stream {
            type Read<'a, B: IoBufMut> = PortableRead<'a, B>;

            #[inline]
            fn read<B: IoBufMut>(&self, buf: B) -> Self::Read<'_, B> {
                $stream::read(self, buf)
            }
        }

        impl AsyncWrite for $stream {
            type Write<'a, B: IoBuf> = PortableWrite<'a, B>;

            #[inline]
            fn write<B: IoBuf>(&self, buf: B) -> Self::Write<'_, B> {
                $stream::write(self, buf)
            }
        }
    };
}

mod tcp;
mod unix;

pub use tcp::{TcpListener, TcpStream};
pub use unix::{UnixListener, UnixStream};

/// What a portable stream holds: its descriptor, and the waits of its poll
/// methods, which take the stream mutably and so have them to themselves.
struct Stream {
    io: Nonblocking,
    /// The wait of `poll_read`.
    read: ReadyWait,
    /// The wait of `poll_write` and `poll_write_vectored`.
    write: ReadyWait,
}

impl Stream {
    /// Takes over the socket `fd`, made nonblocking.
    fn new(fd: OwnedFd) -> io::Result<Self> {
        Ok(Self::with(Nonblocking::new(fd, Kind::Socket)?))
    }

    /// Takes over the socket `fd`, nonblocking already.
    fn from_nonblocking(fd: OwnedFd) -> Self {
        Self::with(Nonblocking::from_nonblocking(fd, Kind::Socket))
    }

    fn with(io: Nonblocking) -> Self {
        Self {
            io,
            read: ReadyWait::new(),
            write: ReadyWait::new(),
        }
    }
}

/// A listener's descriptor, and the wait of its `poll_accept`.
struct Listener {
    io: Nonblocking,
    wait: ReadyWait,
}

impl Listener {
    /// Takes over the listening socket `fd`, made nonblocking.
    fn new(fd: OwnedFd) -> io::Result<Self> {
        Ok(Self {
            io: Nonblocking::new(fd, Kind::Socket)?,
            wait: ReadyWait::new(),
        })
    }
}

/// A socket of `domain` and `kind`, such as `SOCK_STREAM`, nonblocking and
/// closed on exec, made with one `socket(2)`.
fn socket(domain: libc::c_int, kind: libc::c_int) -> io::Result<OwnedFd> {
    let kind = kind | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC;
    // SAFETY: socket(2) makes a descriptor and touches no memory.
    let fd = unsafe { libc::socket(domain, kind, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the descriptor is new, and nothing else owns it.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Connects the nonblocking socket of `io` to `addr`: a `connect(2)` made at
/// once, which connects at once or starts connecting, and then a wait for
/// the socket to be writable, as it is once the connection is made or has
/// failed, which `SO_ERROR` then says.
async fn connect(io: &Nonblocking, addr: &SockAddr) -> io::Result<()> {
    let fd = io.fd().as_raw_fd();
    // A `sockaddr_storage` long at most.
    let len = addr.len() as libc::socklen_t;
    // SAFETY: connect(2) reads the `len` bytes of the address, which `addr`
    // holds.
    if unsafe { libc::connect(fd, addr.as_ptr(), len) } == 0 {
        return Ok(());
    }
    let err = io::Error::last_os_error();
    match err.raw_os_error() {
        // Connecting, which a nonblocking connect interrupted goes on with.
        Some(libc::EINPROGRESS | libc::EINTR) => {}
        _ => return Err(err),
    }
    io.fd().ready(WRITABLE).await?;
    match take_error(io)? {
        None => Ok(()),
        Some(err) => Err(err),
    }
}

/// Takes a connection from the listening socket of `io` with one
/// `accept4(2)`, made at once: the connection's descriptor, nonblocking and
/// closed on exec, and the peer's address as the kernel wrote it; or
/// `WouldBlock` if there is none to take.
fn try_accept(io: &Nonblocking) -> io::Result<(OwnedFd, SockAddr)> {
    // SAFETY: all zeroes is a valid `sockaddr_storage`.
    let mut storage: libc::sockaddr_storage = unsafe { mem::zeroed() };
    let room = mem::size_of::<libc::sockaddr_storage>();
    loop {
        let mut len = room as libc::socklen_t;
        let flags = libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC;
        let listener = io.fd().as_raw_fd();
        // SAFETY: accept4(2) writes at most `len` bytes of the peer's address
        // into `storage`, which has room for them, and how many into `len`.
        let fd = unsafe { libc::accept4(listener, (&raw mut storage).cast(), &raw mut len, flags) };
        if fd >= 0 {
            // SAFETY: the descriptor is new, and nothing else owns it.
            let fd = unsafe { OwnedFd::from_raw_fd(fd) };
            // SAFETY: the storage is `room` initialized bytes, zeroed and then
            // written by the kernel, with no padding between its fields.
            let bytes = unsafe { slice::from_raw_parts((&raw const storage).cast::<u8>(), room) };
            let written = (len as usize).min(room);
            return Ok((fd, SockAddr::from_kernel(&bytes[..written])));
        }
        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            // Interrupted, or a connection given up before it was taken: the
            // next one.
            Some(libc::EINTR | libc::ECONNABORTED) => continue,
            _ => return Err(err),
        }
    }
}

/// Takes a connection from the listening socket of `io`, as [`try_accept`]
/// does, waiting with `wait` while there is none; it spends a step of the
/// task's budget for each it takes.
fn poll_accept(
    io: &Nonblocking,
    cx: &mut Context<'_>,
    wait: &mut ReadyWait,
) -> Poll<io::Result<(OwnedFd, SockAddr)>> {
    let proceed = ready!(coop::poll_proceed(cx));
    loop {
        if wait.is_waiting() {
            ready!(io.poll_ready(cx, READABLE, wait))?;
        }
        match try_accept(io) {
            Ok(accepted) => {
                proceed.made_progress();
                return Poll::Ready(Ok(accepted));
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                ready!(io.poll_ready(cx, READABLE, wait))?;
            }
            Err(err) => return Poll::Ready(Err(err)),
        }
    }
}

/// Shuts down a half, or both, of the socket of `io`.
fn shutdown(io: &Nonblocking, how: std::net::Shutdown) -> io::Result<()> {
    let how = match how {
        std::net::Shutdown::Read => libc::SHUT_RD,
        std::net::Shutdown::Write => libc::SHUT_WR,
        std::net::Shutdown::Both => libc::SHUT_RDWR,
    };
    // SAFETY: shutdown(2) touches no memory.
    if unsafe { libc::shutdown(io.fd().as_raw_fd(), how) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// The error pending on the socket of `io`, which reading `SO_ERROR` clears.
fn take_error(io: &Nonblocking) -> io::Result<Option<io::Error>> {
    let errno: i32 = getsockopt(io.fd().as_raw_fd(), libc::SOL_SOCKET, libc::SO_ERROR)?;
    Ok((errno != 0).then(|| io::Error::from_raw_os_error(errno)))
}
