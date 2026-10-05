//! Files, on the ring of the thread's runtime: opening, reading, writing and
//! syncing them, their status, and the names in the file system.
//!
//! Reads and writes take owned buffers and hand them back with the result,
//! as the sockets' do. They go at an offset, which lets any number run at
//! once; [`File::read`] and [`File::write`] use the file's position instead,
//! as `read(2)` and `write(2)` do. [`File::read_stream`] reads what arrives
//! on a pipe, a FIFO or a character device into a pool of buffers, with
//! one multishot read.
//!
//! ```no_run
//! # ruio::Runtime::new().unwrap().block_on(async {
//! use ruio::fs::File;
//!
//! let file = File::create("greeting").await?;
//! file.write_all_at("hello", 0).await.0?;
//! file.sync_all().await?;
//! let (read, greeting) = file.read_exact_at(Vec::with_capacity(5), 0).await;
//! read?;
//! assert_eq!(greeting, b"hello");
//! # Ok::<(), std::io::Error>(())
//! # }).unwrap();
//! ```
//!
//! Direct I/O (`O_DIRECT`, through [`OpenOptions::custom_flags`]) reads and
//! writes past the page cache, with memory, offsets and lengths aligned as
//! [`Metadata::dio_alignment`] says: an
//! [`AlignedBuf`](crate::buf::AlignedBuf), or a registered buffer for
//! [`File::read_fixed_at`], which also skips mapping the memory on each read.
//! A file on a block device with poll queues can be opened for polled I/O
//! as well ([`OpenOptions::polled`]): the kernel then polls the device for
//! each read's and write's completion instead of waiting for an interrupt,
//! on a second ring of the thread's driver.
//!
//! [`read_dir`] reads a directory's entries, which io_uring has no
//! operation for: with `getdents64(2)`, a batch at a time, on the thread.
//!
//! The kernel does most of what is not a read or a write in an io-wq worker,
//! as the calls it stands for can block: opening a file whose name is not
//! cached, statx, fsync, truncating, and changing names. Buffered reads and
//! writes that miss the page cache go there too, while direct ones go to the
//! device from the ring.

use std::fmt;
use std::future::{Future, poll_fn};
use std::io;
use std::marker::PhantomData;
use std::mem;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};

use ruio_driver::buf::{BufLease, BufPool, BufResult, FixedIoBuf, IoBuf, IoBufMut, Slice};
use ruio_driver::ops::{
    self, Fsync, Ftruncate, MkdirAt, OpenAt, Read, ReadFixed, RenameAt, Statx, UnlinkAt, Write,
    WriteFixed,
};
use ruio_driver::{Chain, Driver, Fd, Link, Op, Operation, PolledFd};

use crate::net::{Desc, Sock, ThreadBound};

mod dir;

pub use dir::{DirEntry, ReadDir, read_dir};

/// The status fields [`Metadata`] asks for.
const METADATA: u32 = libc::STATX_BASIC_STATS | libc::STATX_BTIME | libc::STATX_DIOALIGN;

/// An open file.
///
/// It is a plain descriptor, unless opened with
/// [`OpenOptions::direct_descriptor`]; either closes when dropped, or with a
/// result from [`close`](File::close). Every operation borrows the file, so
/// it cannot close before the kernel has the operation:
///
/// ```compile_fail
/// # async fn run(file: ruio::fs::File) {
/// let read = file.read_at(Vec::with_capacity(8), 0);
/// drop(file);
/// read.await;
/// # }
/// ```
pub struct File {
    sock: Sock<std::fs::File>,
    /// The file's slot in the polled ring's table, if its reads and writes
    /// are polled; see [`OpenOptions::polled`].
    polled: Option<PolledFd>,
    _thread: ThreadBound,
}

impl File {
    /// Opens the file at `path` for reading.
    ///
    /// # Errors
    ///
    /// What `openat2(2)` reports, and `InvalidInput` if the path contains a
    /// nul byte.
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new().read(true).open(path).await
    }

    /// Opens the file at `path` for writing, creating it or emptying it.
    ///
    /// # Errors
    ///
    /// As for [`File::open`].
    pub async fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .await
    }

    /// Options for opening a file; see [`OpenOptions`].
    pub fn options() -> OpenOptions {
        OpenOptions::new()
    }

    /// Wraps a file from the standard library.
    pub fn from_std(file: std::fs::File) -> Self {
        Self {
            sock: Sock::from_std(file),
            polled: None,
            _thread: PhantomData,
        }
    }

    /// Whether the file is a direct descriptor.
    pub fn is_direct(&self) -> bool {
        self.sock.is_direct()
    }

    /// Whether the file's reads and writes are polled; see
    /// [`OpenOptions::polled`].
    pub fn is_polled(&self) -> bool {
        self.polled.is_some()
    }

    /// The descriptor, for operations on the thread's ring.
    pub fn fd(&self) -> Fd {
        self.sock.fd()
    }

    /// The standard library's file, which a direct descriptor has not.
    ///
    /// # Errors
    ///
    /// `Unsupported` for a direct descriptor.
    pub fn as_std(&self) -> io::Result<&std::fs::File> {
        self.sock.plain()
    }

    fn op<T: Operation>(&self, make: impl FnOnce(Fd) -> T) -> FileOp<'_, T> {
        let (driver, fd) = self.sock.target();
        FileOp {
            op: driver.op(make(fd)),
            _file: PhantomData,
        }
    }

    /// The ring a read or a write runs on, the descriptor to give it, and
    /// whether it is polled: the polled ring's slot for a polled file, and
    /// what [`Sock::target`] says otherwise.
    fn io_target(&self) -> (Driver, Fd, bool) {
        let (driver, fd) = self.sock.target();
        match &self.polled {
            Some(polled) => (driver, polled.fd(), true),
            None => (driver, fd, false),
        }
    }

    /// Runs the read or write `make` makes for the file's descriptor, polled
    /// if the file is.
    fn io_op<T: Operation>(&self, make: impl FnOnce(Fd, bool) -> T) -> FileOp<'_, T> {
        let (driver, fd, polled) = self.io_target();
        FileOp {
            op: driver.op(make(fd, polled)),
            _file: PhantomData,
        }
    }

    /// Reads into `buf`, up to its capacity, from `offset` on. Fewer bytes
    /// come back only at the end of the file, where none do.
    pub fn read_at<B: IoBufMut>(&self, buf: B, offset: u64) -> FileOp<'_, Read<B>> {
        self.io_op(|fd, polled| Read::at(fd, buf, offset).polled(polled))
    }

    /// Writes `buf`'s initialized bytes, from `offset` on, or as many as the
    /// file takes.
    pub fn write_at<B: IoBuf>(&self, buf: B, offset: u64) -> FileOp<'_, Write<B>> {
        self.io_op(|fd, polled| Write::at(fd, buf, offset).polled(polled))
    }

    /// Fills `buf` to its capacity from `offset` on; see [`ReadExactAt`].
    pub fn read_exact_at<B: IoBufMut>(&self, buf: B, offset: u64) -> ReadExactAt<'_, B> {
        let (driver, fd, polled) = self.io_target();
        ReadExactAt {
            transfer: Transfer::new(driver, fd, offset, buf, polled),
            _file: PhantomData,
        }
    }

    /// Writes all of `buf`'s initialized bytes from `offset` on; see
    /// [`WriteAllAt`].
    pub fn write_all_at<B: IoBuf>(&self, buf: B, offset: u64) -> WriteAllAt<'_, B> {
        let (driver, fd, polled) = self.io_target();
        WriteAllAt {
            transfer: Transfer::new(driver, fd, offset, buf, polled),
            _file: PhantomData,
        }
    }

    /// Reads into a registered buffer from `offset` on, as
    /// [`read_at`](File::read_at) does, but without mapping its memory for
    /// the read.
    pub fn read_fixed_at<B: IoBufMut + FixedIoBuf>(
        &self,
        buf: B,
        offset: u64,
    ) -> FileOp<'_, ReadFixed<B>> {
        self.io_op(|fd, polled| ReadFixed::at(fd, buf, offset).polled(polled))
    }

    /// Writes a registered buffer's initialized bytes from `offset` on; see
    /// [`read_fixed_at`](File::read_fixed_at).
    pub fn write_fixed_at<B: IoBuf + FixedIoBuf>(
        &self,
        buf: B,
        offset: u64,
    ) -> FileOp<'_, WriteFixed<B>> {
        self.io_op(|fd, polled| WriteFixed::at(fd, buf, offset).polled(polled))
    }

    /// Reads into `buf` at the file's position, which the read advances.
    ///
    /// Nothing orders reads and writes at the position that run at once, so
    /// they must not overlap; ones at offsets may.
    pub fn read<B: IoBufMut>(&self, buf: B) -> FileOp<'_, Read<B>> {
        self.io_op(|fd, polled| Read::new(fd, buf).polled(polled))
    }

    /// Writes `buf`'s initialized bytes at the file's position, which the
    /// write advances, or at its end if it was opened to append; see
    /// [`read`](File::read).
    pub fn write<B: IoBuf>(&self, buf: B) -> FileOp<'_, Write<B>> {
        self.io_op(|fd, polled| Write::new(fd, buf).polled(polled))
    }

    /// The data arriving on the file, read into `pool`, which must belong
    /// to the thread's ring, as it arrives; see [`ReadStream`].
    ///
    /// It suits files that the kernel can poll, which data arrives on:
    /// pipes, FIFOs and character devices. A regular file is always
    /// readable, and the stream yields `EBADFD` for one.
    ///
    /// A FIFO, and most character devices, do not tell the kernel that they
    /// can be read without blocking, so it reads them only while they poll
    /// as readable; a FIFO whose writers have gone polls only as hung up,
    /// and the stream would wait for ever for its end. Open such a file with
    /// `O_NONBLOCK`, through [`OpenOptions::custom_flags`], which makes it
    /// one that can be. A FIFO reads as ended while it has no writer, as
    /// `read(2)` finds it, so open it once one is there, or open it to read
    /// and write, so that it always has one.
    pub fn read_stream(&self, pool: &BufPool) -> ReadStream<'_> {
        let (driver, fd) = self.sock.target();
        ReadStream {
            stream: ops::ReadStream::new(&driver, fd, pool),
            _file: PhantomData,
        }
    }

    /// The data arriving on the file, read into a pool of the stream's own,
    /// of `count` buffers of `size` bytes, which the kernel consumes
    /// incrementally (Linux 6.12), so that large buffers hold many small
    /// reads; see [`read_stream`](File::read_stream) and
    /// [`ops::ReadStream::incremental`].
    ///
    /// # Errors
    ///
    /// `Unsupported` before Linux 6.12, and as for [`BufPool::new`].
    pub fn read_stream_incremental(&self, count: u16, size: u32) -> io::Result<ReadStream<'_>> {
        let (driver, fd) = self.sock.target();
        Ok(ReadStream {
            stream: ops::ReadStream::incremental(&driver, fd, count, size)?,
            _file: PhantomData,
        })
    }

    /// Flushes the file's data and metadata to its device, as `fsync(2)`
    /// does.
    pub fn sync_all(&self) -> FileOp<'_, Fsync> {
        self.op(Fsync::new)
    }

    /// Flushes the file's data, and only the metadata needed to read it
    /// back, as `fdatasync(2)` does.
    pub fn sync_data(&self) -> FileOp<'_, Fsync> {
        self.op(|fd| Fsync::new(fd).data_only())
    }

    /// Makes the file `len` bytes long, cutting it short or padding it with
    /// zeroes, as `ftruncate(2)` does.
    pub fn set_len(&self, len: u64) -> FileOp<'_, Ftruncate> {
        self.op(|fd| Ftruncate::new(fd, len))
    }

    /// The file's status, as `statx(2)` reports it.
    ///
    /// # Errors
    ///
    /// What `statx(2)` reports, and `Unsupported` for a direct descriptor,
    /// which the kernel does not take for statx.
    pub async fn metadata(&self) -> io::Result<Metadata> {
        use std::os::fd::AsRawFd as _;
        let fd = self.sock.plain()?.as_raw_fd();
        let stat = Statx::fd(fd).mask(METADATA);
        let stat = crate::driver().op(stat).await?;
        Ok(Metadata { stat: *stat })
    }

    /// Closes the file, reporting what closing it found, such as an error
    /// writing back data, where dropping it closes it without saying.
    ///
    /// # Errors
    ///
    /// What `close(2)` reports.
    pub async fn close(self) -> io::Result<()> {
        match self.sock.into_desc() {
            Desc::Plain(file) => {
                let file = file.into_inner();
                crate::driver().op(ops::Close::new(file.into())).await
            }
            Desc::Direct(fd) => fd.close().await,
        }
    }
}

impl fmt::Debug for File {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("File")
            .field("fd", &self.fd())
            .finish_non_exhaustive()
    }
}

/// The data arriving on a file that can be polled, such as a pipe, read
/// into a [`BufPool`] by a multishot read that the stream keeps armed.
/// Made by [`File::read_stream`]; see [`ops::ReadStream`].
///
/// Each item is one read's data, in one buffer of the pool, so a request
/// costs nothing per read. The stream ends at the end of the file, as when
/// a pipe's writing end closes, or after yielding an error.
///
/// ```no_run
/// # ruio::Runtime::new().unwrap().block_on(async {
/// use ruio::buf::BufPool;
/// use ruio::fs::File;
///
/// let (reader, _writer) = std::io::pipe()?;
/// let reader = File::from_std(std::os::fd::OwnedFd::from(reader).into());
/// let pool = BufPool::new(&ruio::driver(), 16, 4096)?;
/// let mut data = reader.read_stream(&pool);
/// while let Some(chunk) = data.next().await {
///     println!("{} bytes", chunk?.len());
/// }
/// # Ok::<(), std::io::Error>(())
/// # }).unwrap();
/// ```
#[derive(Debug)]
pub struct ReadStream<'a> {
    stream: ops::ReadStream,
    _file: PhantomData<&'a File>,
}

impl ReadStream<'_> {
    /// The next data, or `None` at the end of the file or once an error has
    /// been yielded.
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<BufLease>>> {
        self.stream.poll_next(cx)
    }

    /// The next data; see [`poll_next`](ReadStream::poll_next).
    pub async fn next(&mut self) -> Option<io::Result<BufLease>> {
        poll_fn(|cx| self.poll_next(cx)).await
    }

    /// The next data if it has been read already, or `None`, without
    /// waiting; see [`ops::ReadStream::next_ready`].
    pub fn next_ready(&mut self) -> Option<io::Result<BufLease>> {
        self.stream.next_ready()
    }
}

/// An operation on a file. Made by [`File`]'s methods; its output is the
/// operation's, such as the bytes read and the buffer.
#[must_use = "an operation does nothing until polled"]
pub struct FileOp<'a, T: Operation> {
    op: Op<T>,
    _file: PhantomData<&'a File>,
}

impl<'a, T: Operation> FileOp<'a, T> {
    /// Cancels the operation if the kernel has not completed it `duration`
    /// after it is submitted, with a linked timeout; see [`Op::timeout`].
    /// The kernel cannot stop every operation part of the way, such as a
    /// buffered read an io-wq worker has begun. A read or a write of a
    /// polled file takes no timeout, and fails with `EOPNOTSUPP` given one.
    pub fn timeout(self, duration: Duration) -> Self {
        Self {
            op: self.op.timeout(duration),
            ..self
        }
    }

    /// Links `next` after this operation into a chain, which the kernel
    /// runs in order, each operation once the one before has succeeded;
    /// see [`FileChain`]. A chain with a read or a write of a polled file in
    /// it fails with `EOPNOTSUPP`.
    ///
    /// # Panics
    ///
    /// If `next` was made on another ring, under another runtime.
    pub fn link<U: Operation>(self, next: FileOp<'a, U>) -> FileChain<'a, Link<Op<T>, Op<U>>> {
        FileChain {
            chain: self.op.link(next.op),
            _files: PhantomData,
        }
    }
}

/// Operations on files linked into a chain (`IOSQE_IO_LINK`), which the
/// kernel runs one after another, each once the one before has succeeded;
/// made by [`FileOp::link`] and [`FileChain::link`]. The output is every
/// operation's, in pairs, as for the driver's [`Link`], which says how a
/// failure, a timeout or a drop goes. It borrows every file, which so
/// stays open until the kernel has the chain.
///
/// The kernel counts a read or a write that moves fewer bytes than asked
/// for as failing, and cancels the rest of the chain with `ECANCELED`.
///
/// ```no_run
/// # ruio::Runtime::new().unwrap().block_on(async {
/// use ruio::fs::File;
///
/// let file = File::create("journal").await?;
/// // The flush runs once the write has succeeded, with no turn of the
/// // loop in between, and not at all if it fails.
/// let ((written, _), synced) = file
///     .write_at("entry\n", 0)
///     .link(file.sync_data())
///     .await;
/// written?;
/// synced?;
/// # Ok::<(), std::io::Error>(())
/// # }).unwrap();
/// ```
#[must_use = "a chain does nothing until polled"]
pub struct FileChain<'a, C: Chain> {
    chain: C,
    _files: PhantomData<&'a File>,
}

impl<'a, C: Chain> FileChain<'a, C> {
    /// Links `next` after the chain's last operation, which it runs once
    /// every one before has succeeded.
    ///
    /// # Panics
    ///
    /// If `next` was made on another ring, under another runtime.
    pub fn link<U: Operation>(self, next: FileOp<'a, U>) -> FileChain<'a, Link<C, Op<U>>> {
        FileChain {
            chain: Link::new(self.chain, next.op),
            _files: PhantomData,
        }
    }
}

impl<A: Chain, B: Chain> Future for FileChain<'_, Link<A, B>> {
    type Output = <Link<A, B> as Future>::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().chain).poll(cx)
    }
}

impl<C: Chain + fmt::Debug> fmt::Debug for FileChain<'_, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileChain")
            .field("chain", &self.chain)
            .finish_non_exhaustive()
    }
}

impl<T: Operation> Future for FileOp<'_, T> {
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T::Output> {
        Pin::new(&mut self.get_mut().op).poll(cx)
    }
}

impl<T: Operation> fmt::Debug for FileOp<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileOp")
            .field("op", &self.op)
            .finish_non_exhaustive()
    }
}

/// A transfer that takes as many operations as it needs.
struct Transfer<T: Operation, B> {
    driver: Driver,
    fd: Fd,
    offset: u64,
    /// Whether the operations go on the polled ring.
    polled: bool,
    state: TransferState<T, B>,
}

enum TransferState<T: Operation, B> {
    Start(B),
    /// `done` bytes have moved already.
    Moving {
        op: Op<T>,
        done: usize,
    },
    Finished,
}

impl<T: Operation, B> Transfer<T, B> {
    fn new(driver: Driver, fd: Fd, offset: u64, buf: B, polled: bool) -> Self {
        Self {
            driver,
            fd,
            offset,
            polled,
            state: TransferState::Start(buf),
        }
    }

    fn start(&mut self, buf: B, done: usize, make: impl FnOnce(Fd, B, u64) -> T) {
        let op = self
            .driver
            .op(make(self.fd, buf, self.offset + done as u64));
        self.state = TransferState::Moving { op, done };
    }
}

/// Fills a whole buffer from a file, from an offset on, with as many reads
/// as it takes. Made by [`File::read_exact_at`].
///
/// The output is the buffer, and `Ok(())` once it is full, `UnexpectedEof`
/// if the file ends first, or the error that stopped the reads; either way
/// the buffer holds what was read.
#[must_use = "a read does nothing until polled"]
pub struct ReadExactAt<'a, B: IoBufMut> {
    transfer: Transfer<Read<Slice<B>>, B>,
    _file: PhantomData<&'a File>,
}

impl<B: IoBufMut> Future for ReadExactAt<'_, B> {
    type Output = BufResult<(), B>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let transfer = &mut self.get_mut().transfer;
        let polled = transfer.polled;
        let read = |fd, buf: B, offset| {
            let done = buf.bytes_init();
            Read::at(fd, buf.slice(done..), offset).polled(polled)
        };
        loop {
            match mem::replace(&mut transfer.state, TransferState::Finished) {
                TransferState::Start(buf) if buf.bytes_init() >= buf.bytes_total() => {
                    return Poll::Ready((Ok(()), buf));
                }
                TransferState::Start(buf) => transfer.start(buf, 0, read),
                TransferState::Moving { mut op, done } => {
                    let Poll::Ready((read_now, slice)) = Pin::new(&mut op).poll(cx) else {
                        transfer.state = TransferState::Moving { op, done };
                        return Poll::Pending;
                    };
                    let buf = slice.into_inner();
                    let done = match read_now {
                        Ok(0) => {
                            return Poll::Ready((Err(io::ErrorKind::UnexpectedEof.into()), buf));
                        }
                        Ok(read) => done + read,
                        Err(err) => return Poll::Ready((Err(err), buf)),
                    };
                    if buf.bytes_init() >= buf.bytes_total() {
                        return Poll::Ready((Ok(()), buf));
                    }
                    transfer.start(buf, done, read);
                }
                TransferState::Finished => panic!("`ReadExactAt` polled after it completed"),
            }
        }
    }
}

// Nothing is pinned: the buffer is only ever moved.
impl<B: IoBufMut> Unpin for ReadExactAt<'_, B> {}

impl<B: IoBufMut> fmt::Debug for ReadExactAt<'_, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadExactAt").finish_non_exhaustive()
    }
}

/// Writes all of a buffer's initialized bytes to a file, from an offset on,
/// with as many writes as it takes. Made by [`File::write_all_at`].
///
/// The output is the buffer, and `Ok(())` once every byte is written, or the
/// error that stopped the writes, `WriteZero` if the file took none.
#[must_use = "a write does nothing until polled"]
pub struct WriteAllAt<'a, B: IoBuf> {
    transfer: Transfer<Write<Slice<B>>, B>,
    _file: PhantomData<&'a File>,
}

impl<B: IoBuf> Future for WriteAllAt<'_, B> {
    type Output = BufResult<(), B>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let transfer = &mut self.get_mut().transfer;
        let polled = transfer.polled;
        loop {
            match mem::replace(&mut transfer.state, TransferState::Finished) {
                TransferState::Start(buf) if buf.bytes_init() == 0 => {
                    return Poll::Ready((Ok(()), buf));
                }
                TransferState::Start(buf) => {
                    transfer.start(buf, 0, |fd, buf, offset| {
                        Write::at(fd, buf.slice(..), offset).polled(polled)
                    });
                }
                TransferState::Moving { mut op, done } => {
                    let Poll::Ready((written, slice)) = Pin::new(&mut op).poll(cx) else {
                        transfer.state = TransferState::Moving { op, done };
                        return Poll::Pending;
                    };
                    let buf = slice.into_inner();
                    let done = match written {
                        Ok(0) => return Poll::Ready((Err(io::ErrorKind::WriteZero.into()), buf)),
                        Ok(written) => done + written,
                        Err(err) => return Poll::Ready((Err(err), buf)),
                    };
                    if done >= buf.bytes_init() {
                        return Poll::Ready((Ok(()), buf));
                    }
                    transfer.start(buf, done, |fd, buf, offset| {
                        Write::at(fd, buf.slice(done..), offset).polled(polled)
                    });
                }
                TransferState::Finished => panic!("`WriteAllAt` polled after it completed"),
            }
        }
    }
}

// Nothing is pinned: the buffer is only ever moved.
impl<B: IoBuf> Unpin for WriteAllAt<'_, B> {}

impl<B: IoBuf> fmt::Debug for WriteAllAt<'_, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteAllAt").finish_non_exhaustive()
    }
}

/// Options for opening a file, as the standard library's
/// [`OpenOptions`](std::fs::OpenOptions) has them, and whether to open it as
/// a direct descriptor, or for polled I/O.
#[derive(Clone, Debug)]
pub struct OpenOptions {
    read: bool,
    write: bool,
    append: bool,
    truncate: bool,
    create: bool,
    create_new: bool,
    mode: u32,
    custom_flags: i32,
    direct_descriptor: bool,
    polled: bool,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenOptions {
    /// Options to open nothing yet: at least one of read, write and append
    /// has to be set.
    pub fn new() -> Self {
        Self {
            read: false,
            write: false,
            append: false,
            truncate: false,
            create: false,
            create_new: false,
            mode: 0o666,
            custom_flags: 0,
            direct_descriptor: false,
            polled: false,
        }
    }

    /// Opens the file for reading.
    pub fn read(&mut self, read: bool) -> &mut Self {
        self.read = read;
        self
    }

    /// Opens the file for writing.
    pub fn write(&mut self, write: bool) -> &mut Self {
        self.write = write;
        self
    }

    /// Opens the file for writing at its end, whatever the offset asked.
    pub fn append(&mut self, append: bool) -> &mut Self {
        self.append = append;
        self
    }

    /// Empties the file, which must be opened for writing.
    pub fn truncate(&mut self, truncate: bool) -> &mut Self {
        self.truncate = truncate;
        self
    }

    /// Creates the file if it does not exist; it must be opened for writing.
    pub fn create(&mut self, create: bool) -> &mut Self {
        self.create = create;
        self
    }

    /// Creates the file, failing with `AlreadyExists` if it exists.
    pub fn create_new(&mut self, create_new: bool) -> &mut Self {
        self.create_new = create_new;
        self
    }

    /// Sets the permissions of a file the open creates, before the umask:
    /// `0o666` unless set.
    pub fn mode(&mut self, mode: u32) -> &mut Self {
        self.mode = mode;
        self
    }

    /// Adds `open(2)` flags, such as `O_DIRECT` for direct I/O, `O_NOATIME`
    /// or `O_SYNC`. The access mode flags are ignored; the options above
    /// set them.
    pub fn custom_flags(&mut self, flags: i32) -> &mut Self {
        self.custom_flags = flags;
        self
    }

    /// Opens the file as a direct descriptor, in a slot of the ring's
    /// registered file table, which saves the kernel looking the file up on
    /// every operation; see [`Builder::files`](crate::Builder::files). The
    /// file then has no metadata to ask for, and belongs to the ring: nothing
    /// outside it can use the file.
    pub fn direct_descriptor(&mut self, direct: bool) -> &mut Self {
        self.direct_descriptor = direct;
        self
    }

    /// Opens the file for polled I/O: its reads and writes go on a second
    /// ring of the thread's driver, which polls the device for their
    /// completions rather than wait for its interrupts
    /// (`IORING_SETUP_IOPOLL`); see
    /// [`Driver::enable_polling`](crate::uring::Driver::enable_polling).
    /// The thread polls while a read or a write is outstanding and nothing
    /// else is ready, instead of sleeping, so a read completes as soon as
    /// the device has it, at the cost of the CPU meanwhile.
    ///
    /// Polled I/O needs direct I/O: [`custom_flags`](OpenOptions::custom_flags)
    /// must include `O_DIRECT`, with buffers, offsets and lengths aligned as
    /// for any direct I/O, and the file must be on a block device with poll
    /// queues, which `/sys/block/DEV/queue/io_poll` reports as 1; a read or
    /// a write of a device without them fails with `EOPNOTSUPP`. The file
    /// is a plain descriptor, which the polled ring also has in its own
    /// file table, so that its reads and writes skip the kernel's lookup
    /// of the file as a direct descriptor's do; it cannot be a
    /// [`direct_descriptor`](OpenOptions::direct_descriptor) of the main
    /// ring. Its reads and writes take no [`timeout`](FileOp::timeout) and
    /// join no chain, and one whose future is dropped completes unseen
    /// rather than being cancelled. Everything else on the file, syncing
    /// it, its metadata and closing it, works as for any file.
    pub fn polled(&mut self, polled: bool) -> &mut Self {
        self.polled = polled;
        self
    }

    /// The access mode flags, as the standard library works them out.
    fn access_mode(&self) -> io::Result<i32> {
        match (self.read, self.write, self.append) {
            (true, false, false) => Ok(libc::O_RDONLY),
            (false, true, false) => Ok(libc::O_WRONLY),
            (true, true, false) => Ok(libc::O_RDWR),
            (false, _, true) => Ok(libc::O_WRONLY | libc::O_APPEND),
            (true, _, true) => Ok(libc::O_RDWR | libc::O_APPEND),
            (false, false, false) => Err(invalid("a file must be opened to read, write or append")),
        }
    }

    /// The creation flags, as the standard library works them out.
    fn creation_mode(&self) -> io::Result<i32> {
        match (self.write, self.append) {
            (true, false) => {}
            (false, false) if self.truncate || self.create || self.create_new => {
                return Err(invalid(
                    "a file must be opened to write to be created or emptied",
                ));
            }
            (_, true) if self.truncate && !self.create_new => {
                return Err(invalid("a file opened to append cannot be emptied"));
            }
            _ => {}
        }
        Ok(match (self.create, self.truncate, self.create_new) {
            (false, false, false) => 0,
            (true, false, false) => libc::O_CREAT,
            (false, true, false) => libc::O_TRUNC,
            (true, true, false) => libc::O_CREAT | libc::O_TRUNC,
            (_, _, true) => libc::O_CREAT | libc::O_EXCL,
        })
    }

    /// Opens the file at `path` with these options.
    ///
    /// # Errors
    ///
    /// What `openat2(2)` reports, and `InvalidInput` if the options make no
    /// sense, as for the standard library, or the path contains a nul byte:
    /// a file for [polled](OpenOptions::polled) I/O that is not opened for
    /// direct I/O, or is to be a direct descriptor, makes no sense. Opening
    /// one fails with `Unsupported` if the kernel's io_uring has no polled
    /// rings, and with `ENFILE` if the polled ring's file table is full.
    pub async fn open(&self, path: impl AsRef<Path>) -> io::Result<File> {
        let flags =
            self.access_mode()? | self.creation_mode()? | (self.custom_flags & !libc::O_ACCMODE);
        let driver = crate::driver();
        if self.polled {
            if flags & libc::O_DIRECT == 0 {
                return Err(invalid(
                    "a file for polled I/O must be opened for direct I/O",
                ));
            }
            if self.direct_descriptor {
                return Err(invalid("a file for polled I/O is a plain descriptor"));
            }
            driver.enable_polling()?;
        }
        let mut open = OpenAt::new(path)?.flags(flags).mode(self.mode);
        if self.direct_descriptor {
            open = open.direct(&driver);
        }
        let descriptor = driver.op(open).await?;
        let polled = match (&descriptor, self.polled) {
            (ruio_driver::Descriptor::Plain(fd), true) => {
                use std::os::fd::AsRawFd as _;
                Some(driver.install_polled(fd.as_raw_fd()).await?)
            }
            _ => None,
        };
        Ok(File {
            sock: Sock::new(descriptor),
            polled,
            _thread: PhantomData,
        })
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

/// A file's status, as `statx(2)` reports it.
#[derive(Clone)]
pub struct Metadata {
    stat: libc::statx,
}

impl Metadata {
    /// Whether the kernel filled in the fields of `mask`.
    fn has(&self, mask: u32) -> bool {
        self.stat.stx_mask & mask == mask
    }

    /// The file's type.
    pub fn file_type(&self) -> FileType {
        FileType(u32::from(self.stat.stx_mode) & libc::S_IFMT)
    }

    /// Whether the file is a regular file.
    pub fn is_file(&self) -> bool {
        self.file_type().is_file()
    }

    /// Whether the file is a directory.
    pub fn is_dir(&self) -> bool {
        self.file_type().is_dir()
    }

    /// Whether the file is a symbolic link, which a status reports only
    /// when asked not to follow it, as [`symlink_metadata`] does.
    pub fn is_symlink(&self) -> bool {
        self.file_type().is_symlink()
    }

    /// The file's size in bytes.
    #[expect(
        clippy::len_without_is_empty,
        reason = "as the standard library's `Metadata`, whose size is a length"
    )]
    pub fn len(&self) -> u64 {
        self.stat.stx_size
    }

    /// The file's permissions.
    pub fn permissions(&self) -> std::fs::Permissions {
        std::fs::Permissions::from_mode(u32::from(self.stat.stx_mode) & 0o7777)
    }

    /// The file's mode: its type and permission bits, as in `st_mode`.
    pub fn mode(&self) -> u32 {
        u32::from(self.stat.stx_mode)
    }

    /// The file's inode number.
    pub fn ino(&self) -> u64 {
        self.stat.stx_ino
    }

    /// The device the file is on, as `st_dev` has it.
    pub fn dev(&self) -> u64 {
        libc::makedev(self.stat.stx_dev_major, self.stat.stx_dev_minor)
    }

    /// How many hard links the file has.
    pub fn nlink(&self) -> u64 {
        u64::from(self.stat.stx_nlink)
    }

    /// The user that owns the file.
    pub fn uid(&self) -> u32 {
        self.stat.stx_uid
    }

    /// The group that owns the file.
    pub fn gid(&self) -> u32 {
        self.stat.stx_gid
    }

    /// The size of block the file system prefers for I/O.
    pub fn blksize(&self) -> u64 {
        u64::from(self.stat.stx_blksize)
    }

    /// How many 512-byte blocks the file takes up.
    pub fn blocks(&self) -> u64 {
        self.stat.stx_blocks
    }

    /// When the file was last modified.
    ///
    /// # Errors
    ///
    /// `Unsupported` if the file system did not say.
    pub fn modified(&self) -> io::Result<SystemTime> {
        self.time(libc::STATX_MTIME, self.stat.stx_mtime)
    }

    /// When the file was last read.
    ///
    /// # Errors
    ///
    /// As for [`modified`](Metadata::modified).
    pub fn accessed(&self) -> io::Result<SystemTime> {
        self.time(libc::STATX_ATIME, self.stat.stx_atime)
    }

    /// When the file was made, which not every file system records.
    ///
    /// # Errors
    ///
    /// As for [`modified`](Metadata::modified).
    pub fn created(&self) -> io::Result<SystemTime> {
        self.time(libc::STATX_BTIME, self.stat.stx_btime)
    }

    fn time(&self, mask: u32, time: libc::statx_timestamp) -> io::Result<SystemTime> {
        if !self.has(mask) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "the file system does not record this time",
            ));
        }
        let nanos = Duration::from_nanos(u64::from(time.tv_nsec));
        let time = match u64::try_from(time.tv_sec) {
            Ok(secs) => SystemTime::UNIX_EPOCH + Duration::from_secs(secs) + nanos,
            Err(_) => {
                SystemTime::UNIX_EPOCH - Duration::from_secs(time.tv_sec.unsigned_abs()) + nanos
            }
        };
        Ok(time)
    }

    /// How direct I/O (`O_DIRECT`) on the file must be aligned, if the file
    /// system does it: `None` if it does not, or did not say.
    pub fn dio_alignment(&self) -> Option<DioAlignment> {
        if !self.has(libc::STATX_DIOALIGN) || self.stat.stx_dio_mem_align == 0 {
            return None;
        }
        Some(DioAlignment {
            memory: self.stat.stx_dio_mem_align as usize,
            offset: self.stat.stx_dio_offset_align as usize,
        })
    }
}

impl fmt::Debug for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Metadata")
            .field("file_type", &self.file_type())
            .field("len", &self.len())
            .field("mode", &format_args!("{:o}", self.mode()))
            .finish_non_exhaustive()
    }
}

/// How direct I/O on a file must be aligned. Made by
/// [`Metadata::dio_alignment`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DioAlignment {
    /// What a buffer's memory must start at a multiple of.
    pub memory: usize,
    /// What each transfer's offset in the file, and length, must be a
    /// multiple of.
    pub offset: usize,
}

/// A file's type. Made by [`Metadata::file_type`].
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct FileType(u32);

impl FileType {
    /// Whether it is a regular file.
    pub fn is_file(self) -> bool {
        self.0 == libc::S_IFREG
    }

    /// Whether it is a directory.
    pub fn is_dir(self) -> bool {
        self.0 == libc::S_IFDIR
    }

    /// Whether it is a symbolic link.
    pub fn is_symlink(self) -> bool {
        self.0 == libc::S_IFLNK
    }

    /// Whether it is a block device.
    pub fn is_block_device(self) -> bool {
        self.0 == libc::S_IFBLK
    }

    /// Whether it is a character device.
    pub fn is_char_device(self) -> bool {
        self.0 == libc::S_IFCHR
    }

    /// Whether it is a named pipe.
    pub fn is_fifo(self) -> bool {
        self.0 == libc::S_IFIFO
    }

    /// Whether it is a Unix socket.
    pub fn is_socket(self) -> bool {
        self.0 == libc::S_IFSOCK
    }
}

impl fmt::Debug for FileType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match () {
            () if self.is_file() => "file",
            () if self.is_dir() => "directory",
            () if self.is_symlink() => "symbolic link",
            () if self.is_block_device() => "block device",
            () if self.is_char_device() => "character device",
            () if self.is_fifo() => "named pipe",
            () if self.is_socket() => "socket",
            () => "unknown",
        };
        f.write_str(name)
    }
}

/// The status of the file at `path`, following a final symbolic link.
///
/// # Errors
///
/// What `statx(2)` reports, and `InvalidInput` if the path contains a nul
/// byte.
pub async fn metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
    let stat = Statx::new(path)?.mask(METADATA);
    let stat = crate::driver().op(stat).await?;
    Ok(Metadata { stat: *stat })
}

/// The status of the file at `path`, or of the symbolic link there itself.
///
/// # Errors
///
/// As for [`metadata`].
pub async fn symlink_metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
    let stat = Statx::new(path)?
        .flags(libc::AT_SYMLINK_NOFOLLOW)
        .mask(METADATA);
    let stat = crate::driver().op(stat).await?;
    Ok(Metadata { stat: *stat })
}

/// Reads the whole file at `path`.
///
/// # Errors
///
/// What opening and reading the file report.
pub async fn read(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
    let file = File::open(path).await?;
    let mut data = Vec::with_capacity(8 << 10);
    loop {
        if data.len() == data.capacity() {
            data.reserve(data.capacity());
        }
        let len = data.len();
        let (read, rest) = file.read_at(data.slice(len..), len as u64).await;
        data = rest.into_inner();
        if read? == 0 {
            return Ok(data);
        }
    }
}

/// Reads the whole file at `path`, which must hold UTF-8.
///
/// # Errors
///
/// As for [`read`], and `InvalidData` if the file is not UTF-8.
pub async fn read_to_string(path: impl AsRef<Path>) -> io::Result<String> {
    String::from_utf8(read(path).await?)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
}

/// Writes `contents` to the file at `path`, creating it or emptying it
/// first, and closes it, reporting what closing it found.
///
/// # Errors
///
/// What opening, writing and closing the file report.
pub async fn write<B: IoBuf>(path: impl AsRef<Path>, contents: B) -> io::Result<()> {
    let file = File::create(path).await?;
    file.write_all_at(contents, 0).await.0?;
    file.close().await
}

/// Removes the file at `path`.
///
/// # Errors
///
/// What `unlinkat(2)` reports, and `InvalidInput` if the path contains a nul
/// byte.
pub async fn remove_file(path: impl AsRef<Path>) -> io::Result<()> {
    crate::driver().op(UnlinkAt::new(path)?).await
}

/// Removes the empty directory at `path`.
///
/// # Errors
///
/// As for [`remove_file`].
pub async fn remove_dir(path: impl AsRef<Path>) -> io::Result<()> {
    crate::driver().op(UnlinkAt::new(path)?.directory()).await
}

/// Makes a directory at `path`.
///
/// # Errors
///
/// What `mkdirat(2)` reports, and `InvalidInput` if the path contains a nul
/// byte.
pub async fn create_dir(path: impl AsRef<Path>) -> io::Result<()> {
    crate::driver().op(MkdirAt::new(path)?).await
}

/// Makes a directory at `path`, and any of its parents that do not exist.
///
/// # Errors
///
/// As for [`create_dir`], except that a directory that exists already is
/// not an error.
pub async fn create_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref();
    // The ancestors to make, from the deepest that is missing up.
    let mut missing = Vec::new();
    for dir in path.ancestors().filter(|dir| !dir.as_os_str().is_empty()) {
        match metadata(dir).await {
            Ok(found) if found.is_dir() => break,
            Ok(_) => return Err(io::Error::from(io::ErrorKind::AlreadyExists)),
            Err(err) if err.kind() == io::ErrorKind::NotFound => missing.push(dir),
            Err(err) => return Err(err),
        }
    }
    for dir in missing.into_iter().rev() {
        match create_dir(dir).await {
            Ok(()) => {}
            // Made meanwhile by someone else.
            Err(err)
                if err.kind() == io::ErrorKind::AlreadyExists && metadata(dir).await?.is_dir() => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

/// Renames `from` to `to`, replacing what `to` names.
///
/// # Errors
///
/// What `renameat2(2)` reports, and `InvalidInput` if a path contains a nul
/// byte.
pub async fn rename(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
    crate::driver().op(RenameAt::new(from, to)?).await
}
