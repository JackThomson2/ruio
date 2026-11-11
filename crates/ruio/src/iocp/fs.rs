//! Files: reading and writing them through the I/O completion port of the
//! thread's runtime, and opening, syncing and resizing them, their status,
//! and the names in the file system, on the blocking pool.
//!
//! This is the part of ruio's `fs` that the completion port engine has so
//! far, a proof of concept, under the names and with the signatures it has
//! on io_uring, so that code written to it builds for both.
//!
//! Reads and writes take owned buffers and hand them back with the result,
//! as the sockets' do. They go at an offset, which lets any number run at
//! once; [`File::read`] and [`File::write`] use the file's position
//! instead, which the file keeps itself here, as Windows keeps none for a
//! file opened for overlapped I/O.
//!
//! [`File::read_stream`] reads a file from its position on into the buffers
//! of a pool, a [`BufPool`], one buffer a read, and
//! hands each back as a lease.
//!
//! ```
//! # ruio::Runtime::new()?.block_on(async {
//! use ruio::fs::{self, File};
//!
//! let name = format!("ruio-greeting-{}", std::process::id());
//! let path = std::env::temp_dir().join(name);
//! let file = File::create(&path).await?;
//! file.write_all_at("hello", 0).await.0?;
//! file.sync_all().await?;
//! file.close().await?;
//! assert_eq!(fs::read(&path).await?, b"hello");
//! fs::remove_file(&path).await?;
//! # std::io::Result::Ok(())
//! # })?;
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! Windows has an overlapped form of reading and writing a file, and of
//! little else. What waits for the disk wherever it is called runs on the
//! blocking pool ([`spawn_blocking`](crate::spawn_blocking)), as the
//! standard library's call in a job of its own, so that it holds a thread
//! of the pool and not the runtime's: opening a file, [`File::sync_all`],
//! [`File::sync_data`], [`File::set_len`], [`File::allocate`],
//! [`File::set_permissions`] and [`File::metadata`], every function that
//! takes a path, and [`read_dir`], which reads a directory's entries a
//! batch at a time.
//!
//! Each method's future has a name of its own here, after the method:
//! [`Read`] and [`Write`] for the reads and writes at an offset and at the
//! position alike, [`SyncAll`], [`SyncData`], [`SetLen`] and [`Allocate`].
//! So code that names a future names no operation of the engine's.
//!
//! A [`File`] belongs to its thread's port, and is neither `Send` nor
//! `Sync`. The functions that take a path hold no file of the port, so
//! their futures are `Send` when what they are given is, and they can be
//! awaited on any thread, in a runtime or out of one; a [`ReadDir`] and its
//! [`DirEntry`]s are `Send` and `Sync`. A [`portable::File`], opened with
//! [`OpenOptions::open_portable`], is `Send` and `Sync`, and waits for each
//! of its reads and writes on the port of the runtime on the thread that
//! polls it.
//!
//! ```
//! fn assert_send<T: Send>(_: T) {}
//!
//! assert_send(ruio::fs::read("config.toml"));
//! assert_send(ruio::fs::write("config.toml", b"key = 1".to_vec()));
//! assert_send(ruio::fs::metadata("config.toml"));
//! assert_send(ruio::fs::read_dir("."));
//! ```
//!
//! Not here yet, of what `fs` has on io_uring: the thread-bound files
//! taken from and given to the standard library (`from_std`, `into_std`),
//! which the portable ones are, and the futures' timeouts. A [`Metadata`]
//! reports what the standard library's does on every system; an owner, a
//! mode and an inode are Unix's.

use std::cell::Cell;
use std::collections::VecDeque;
use std::ffi::OsString;
use std::fmt;
use std::future::{Future, poll_fn};
use std::io;
use std::marker::PhantomData;
use std::mem;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::slice;
use std::sync::Arc;
use std::task::{Context, Poll, ready};
use std::time::SystemTime;

use ruio_executor::blocking::BlockingHandle;
use ruio_iocp::fs as iocp;

use crate::buf::{BufLease, BufPool, BufResult, IoBuf, IoBufMut};

pub use ruio_iocp::{ReadExact as ReadExactAt, WriteAll as WriteAllAt};

#[path = "fs/portable.rs"]
pub mod portable;

/// How many entries of a directory one job of the blocking pool reads.
const BATCH: usize = 64;

/// Runs `call` on the blocking pool, and waits for what it returns.
async fn pooled<T, F>(call: F) -> io::Result<T>
where
    F: FnOnce() -> io::Result<T> + Send + 'static,
    T: Send + 'static,
{
    // A `JoinError` is a panic inside the call: the job cannot be
    // cancelled, as nothing but this holds its handle.
    ruio_executor::spawn_blocking(call)
        .await
        .map_err(io::Error::other)?
}

/// A file, whose reads and writes complete to the port of the runtime it
/// was opened in, and which belongs to that runtime's thread.
///
/// Reads and writes take owned buffers and hand them back with the result.
/// Those at an offset may run at once, any number of them. Those at the
/// position go one at a time: each starts where the file's position is as
/// the method is called, and moves it on by what it transferred once it
/// completes. A file opened to append writes at its end whatever the
/// offset or the position, and a write at the position moves the position
/// to the end it made, as on Linux: the next read at the position asks the
/// system how long the file is then.
pub struct File {
    file: iocp::File,
    /// Where the next read or write at the position goes. Windows keeps no
    /// position for a file opened for overlapped I/O, so the file does.
    pos: Cell<u64>,
    /// Whether the file was opened to append.
    append: bool,
    /// Whether a write at the position of a file opened to append has left
    /// the position at the end of the file, wherever that is, which a read
    /// at the position asks for before it starts.
    at_end: Cell<bool>,
}

impl File {
    /// Opens the file at `path` for reading.
    ///
    /// # Errors
    ///
    /// As for [`OpenOptions::open`].
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new().read(true).open(path).await
    }

    /// Opens the file at `path` for writing, creating it if it does not
    /// exist and emptying it if it does.
    ///
    /// # Errors
    ///
    /// As for [`OpenOptions::open`].
    pub async fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .await
    }

    /// The options a file is opened with, none of them set; see
    /// [`OpenOptions`].
    pub fn options() -> OpenOptions {
        OpenOptions::new()
    }

    /// Reads into `buf`, up to its `bytes_total`, from `offset`: 0 bytes at
    /// or past the end of the file; see [`Read`].
    pub fn read_at<B: IoBufMut>(&self, buf: B, offset: u64) -> Read<'_, B> {
        Read {
            read: self.file.read_at(buf, offset),
            pos: None,
        }
    }

    /// Writes `buf`'s initialized bytes at `offset`, or as many as the
    /// system takes; see [`Write`].
    pub fn write_at<B: IoBuf>(&self, buf: B, offset: u64) -> Write<'_, B> {
        Write {
            write: self.file.write_at(buf, offset),
            pos: None,
            at_end: None,
        }
    }

    /// Fills `buf` after its contents, up to its `bytes_total`, from
    /// `offset` on, with as many reads as that takes; see [`ReadExactAt`].
    pub fn read_exact_at<B: IoBufMut>(&self, buf: B, offset: u64) -> ReadExactAt<'_, B> {
        self.file.read_exact_at(buf, offset)
    }

    /// Writes all of `buf`'s initialized bytes from `offset` on, with as
    /// many writes as that takes; see [`WriteAllAt`].
    pub fn write_all_at<B: IoBuf>(&self, buf: B, offset: u64) -> WriteAllAt<'_, B> {
        self.file.write_all_at(buf, offset)
    }

    /// Reads into `buf`, up to its `bytes_total`, at the file's position,
    /// which the read moves on. Reads and writes at the position go one at
    /// a time: they must not overlap; ones at offsets may.
    pub fn read<B: IoBufMut>(&self, buf: B) -> Read<'_, B> {
        Read {
            read: self.file.read_at(buf, self.position()),
            pos: Some(&self.pos),
        }
    }

    /// Writes `buf`'s initialized bytes at the file's position, which the
    /// write moves on, or at its end if it was opened to append, where it
    /// leaves the position; see [`read`](File::read).
    pub fn write<B: IoBuf>(&self, buf: B) -> Write<'_, B> {
        Write {
            write: self.file.write_at(buf, self.pos.get()),
            pos: Some(&self.pos),
            at_end: self.append.then_some(&self.at_end),
        }
    }

    /// Where a read at the position starts: the file's position, or the
    /// end of the file, where a write at the position left it if the file
    /// was opened to append, which the system is asked for here, on the
    /// runtime's thread, as the length of an open file is what the file
    /// system holds of it, with no disk to wait for. Where the system
    /// cannot say, the position is where the writes moved it on, and it is
    /// asked again the next time.
    fn position(&self) -> u64 {
        if self.at_end.get()
            && let Ok(status) = self.file.as_std().metadata()
        {
            self.pos.set(status.len());
            self.at_end.set(false);
        }
        self.pos.get()
    }

    /// The file's data from its position on, read into buffers of `pool`,
    /// one buffer a read; see [`ReadStream`].
    ///
    /// The stream starts where the file's position is as the method is
    /// called, and moves it on past each item it yields, so it must not
    /// overlap the file's other reads and writes at the position.
    ///
    /// Here the stream reads a file on a disk to its end. io_uring's is for
    /// the files that data arrives on, pipes and devices, and yields an
    /// error for a regular file.
    pub fn read_stream(&self, pool: &BufPool) -> ReadStream<'_> {
        ReadStream {
            stream: self.file.read_stream(pool, self.position()),
            pos: &self.pos,
        }
    }

    /// Flushes the file's data and its status to the disk, on the blocking
    /// pool.
    pub fn sync_all(&self) -> SyncAll<'_> {
        self.call(Call::SyncAll)
    }

    /// Flushes the file's data to the disk, on the blocking pool. Windows
    /// has the one flush, so this is [`sync_all`](File::sync_all).
    pub fn sync_data(&self) -> SyncData<'_> {
        self.call(Call::SyncData)
    }

    /// Cuts the file to `len` bytes, or extends it to them with zeros, on
    /// the blocking pool. The file must be open for writing.
    pub fn set_len(&self, len: u64) -> SetLen<'_> {
        self.call(Call::SetLen(len))
    }

    /// Makes room for `len` bytes of the file from `offset`, growing the
    /// file if they reach past its end, on the blocking pool: a write there
    /// then finds its space, and cannot run out of it. The file must be
    /// open for writing.
    ///
    /// Windows allocates what a file is extended by, and a file that is
    /// not sparse has no part without space, so this extends a file that
    /// ends before the range does, with zeros, and leaves a longer one as
    /// it is. It must not run while a write makes the file longer, which it
    /// could then cut short.
    ///
    /// The call fails with `InvalidInput` for no bytes, as `fallocate(2)`
    /// does, and with `FileTooLarge` for a range that ends past what an
    /// offset counts.
    pub fn allocate(&self, offset: u64, len: u64) -> Allocate<'_> {
        self.call(Call::Allocate(offset, len))
    }

    fn call(&self, call: Call) -> FileCall<'_> {
        FileCall::new(Arc::clone(self.file.as_std()), call)
    }

    /// The file's status, asked for on the blocking pool.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub async fn metadata(&self) -> io::Result<Metadata> {
        let file = Arc::clone(self.file.as_std());
        pooled(move || file.metadata()).await.map(Metadata)
    }

    /// Sets the file's permissions, on the blocking pool: here, whether it
    /// is read-only, which is what Windows keeps of them. The file must be
    /// open for writing.
    ///
    /// # Errors
    ///
    /// What the system reports.
    pub async fn set_permissions(&self, perm: std::fs::Permissions) -> io::Result<()> {
        let file = Arc::clone(self.file.as_std());
        pooled(move || file.set_permissions(perm)).await
    }

    /// The standard library's file, for what ruio has no call for.
    ///
    /// Read and write the file through ruio and not through this: it is
    /// open for overlapped I/O, and the standard library ends the process
    /// if a read or a write of its own on such a file does not complete at
    /// once.
    ///
    /// # Errors
    ///
    /// None here. On io_uring, a file that the ring alone holds has none.
    pub fn as_std(&self) -> io::Result<&std::fs::File> {
        Ok(self.file.as_std())
    }

    /// Closes the file. Windows reports nothing as a file closes that a
    /// write or a sync has not reported already, so this is dropping it,
    /// under the name it has on io_uring.
    ///
    /// # Errors
    ///
    /// None here.
    #[expect(
        clippy::unused_async,
        reason = "the signature is io_uring's, where closing waits for the ring"
    )]
    pub async fn close(self) -> io::Result<()> {
        drop(self);
        Ok(())
    }
}

impl fmt::Debug for File {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("File")
            .field("file", &self.file)
            .field("pos", &self.pos.get())
            .finish()
    }
}

/// The future of a file's read: [`File::read_at`] and [`File::read`]. It
/// hands the buffer back with how many bytes it read: 0 at or past the end
/// of the file.
///
/// It starts the read when first polled. Dropping it before the read
/// completes cancels the read, and the buffer is dropped once the
/// cancellation has completed.
#[must_use = "futures do nothing unless polled"]
pub struct Read<'a, B: IoBufMut> {
    read: ruio_iocp::Read<'a, B>,
    /// The file's position, which a read at it moves on.
    pos: Option<&'a Cell<u64>>,
}

impl<B: IoBufMut> Future for Read<'_, B> {
    type Output = BufResult<usize, B>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (result, buf) = ready!(Pin::new(&mut self.read).poll(cx));
        if let (Some(pos), Ok(read)) = (self.pos, &result) {
            pos.set(pos.get() + *read as u64);
        }
        Poll::Ready((result, buf))
    }
}

impl<B: IoBufMut> fmt::Debug for Read<'_, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.read, f)
    }
}

/// The future of a file's write: [`File::write_at`] and [`File::write`]. It
/// hands the buffer back with how many bytes it wrote.
///
/// It starts the write when first polled. Dropping it before the write
/// completes cancels the write, which may have written some of the buffer
/// by then, and the buffer is dropped once the cancellation has completed.
#[must_use = "futures do nothing unless polled"]
pub struct Write<'a, B: IoBuf> {
    write: ruio_iocp::Write<'a, B>,
    /// The file's position, which a write at it moves on.
    pos: Option<&'a Cell<u64>>,
    /// Whether the file's position is at its end, which a write at it to a
    /// file opened to append sets.
    at_end: Option<&'a Cell<bool>>,
}

impl<B: IoBuf> Future for Write<'_, B> {
    type Output = BufResult<usize, B>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (result, buf) = ready!(Pin::new(&mut self.write).poll(cx));
        if let (Some(pos), Ok(written)) = (self.pos, &result) {
            pos.set(pos.get() + *written as u64);
            if let Some(at_end) = self.at_end {
                at_end.set(true);
            }
        }
        Poll::Ready((result, buf))
    }
}

impl<B: IoBuf> fmt::Debug for Write<'_, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.write, f)
    }
}

/// A file's data from its position on, read into a [`BufPool`]. Made by
/// [`File::read_stream`].
///
/// Each item is one read's data, in one buffer of the pool, and moves the
/// file's position on past it. The stream ends at the end of the file, or
/// after yielding an error. One read is in flight at a time, started as the
/// stream is polled, once the pool has a buffer for it; dropping the stream
/// cancels it, and its buffer returns to the pool once the cancellation has
/// completed.
///
/// ```
/// # ruio::Runtime::new()?.block_on(async {
/// use ruio::buf::BufPool;
/// use ruio::fs::{self, File};
///
/// let name = format!("ruio-stream-{}", std::process::id());
/// let path = std::env::temp_dir().join(name);
/// fs::write(&path, vec![7_u8; 10_000]).await?;
///
/// let file = File::open(&path).await?;
/// let pool = BufPool::new(4, 4096)?;
/// let mut read = 0;
/// {
///     let mut data = file.read_stream(&pool);
///     while let Some(chunk) = data.next().await {
///         read += chunk?.len();
///     }
/// }
/// assert_eq!(read, 10_000);
/// file.close().await?;
/// fs::remove_file(&path).await?;
/// # std::io::Result::Ok(())
/// # })?;
/// # Ok::<(), std::io::Error>(())
/// ```
#[derive(Debug)]
pub struct ReadStream<'a> {
    stream: iocp::ReadStream<'a>,
    /// The file's position, which the stream moves on.
    pos: &'a Cell<u64>,
}

impl ReadStream<'_> {
    /// The next data, or `None` at the end of the file or once an error has
    /// been yielded.
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<BufLease>>> {
        let next = ready!(self.stream.poll_next(cx));
        self.pos.set(self.stream.offset());
        Poll::Ready(next)
    }

    /// The next data; see [`poll_next`](ReadStream::poll_next).
    pub async fn next(&mut self) -> Option<io::Result<BufLease>> {
        poll_fn(|cx| self.poll_next(cx)).await
    }

    /// The next data if it has been read already, or `None`, without
    /// waiting, and without starting a read.
    pub fn next_ready(&mut self) -> Option<io::Result<BufLease>> {
        let next = self.stream.next_ready();
        self.pos.set(self.stream.offset());
        next
    }

    /// Whether the stream has ended: the file has ended, or an error has
    /// been yielded.
    pub fn is_ended(&self) -> bool {
        self.stream.is_ended()
    }
}

#[cfg(feature = "futures")]
impl futures_core::Stream for ReadStream<'_> {
    type Item = io::Result<BufLease>;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        ReadStream::poll_next(self.get_mut(), cx)
    }
}

/// Terminated once the file has ended or an error has been yielded.
#[cfg(feature = "futures")]
impl futures_core::stream::FusedStream for ReadStream<'_> {
    #[inline]
    fn is_terminated(&self) -> bool {
        self.is_ended()
    }
}

/// What a file asks of the blocking pool.
#[derive(Clone, Copy, Debug)]
enum Call {
    SyncAll,
    SyncData,
    SetLen(u64),
    /// A range to make room for: its offset, and its length.
    Allocate(u64, u64),
}

/// The future of a call on a file that Windows has no overlapped form of,
/// which runs on the blocking pool: [`File::sync_all`], [`File::sync_data`],
/// [`File::set_len`] and [`File::allocate`], under the names [`SyncAll`],
/// [`SyncData`], [`SetLen`] and [`Allocate`], and the same of a
/// [`portable::File`], whose future, which borrows a file that is `Sync`,
/// is `Send`.
///
/// It starts the call when first polled. Dropping it leaves a call that
/// has started to finish on the pool.
#[must_use = "futures do nothing unless polled"]
pub struct FileCall<'a, F = File> {
    state: CallState,
    /// The file the call is made on, which the future borrows: one that
    /// belongs to its thread, as what it starts then does, or a portable
    /// one.
    _file: PhantomData<&'a F>,
}

impl<F> FileCall<'_, F> {
    /// The call `call` on `file`, to start when first polled.
    fn new(file: Arc<std::fs::File>, call: Call) -> Self {
        Self {
            state: CallState::Unstarted(file, call),
            _file: PhantomData,
        }
    }
}

enum CallState {
    /// Not started yet: the file, and the call to make on it.
    Unstarted(Arc<std::fs::File>, Call),
    /// On the blocking pool.
    Running(BlockingHandle<io::Result<()>>),
    /// Finished.
    Finished,
}

impl<F> Future for FileCall<'_, F> {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            match mem::replace(&mut this.state, CallState::Finished) {
                CallState::Unstarted(file, call) => {
                    let job = ruio_executor::spawn_blocking(move || match call {
                        Call::SyncAll => file.sync_all(),
                        Call::SyncData => file.sync_data(),
                        Call::SetLen(len) => file.set_len(len),
                        Call::Allocate(offset, len) => allocate(&file, offset, len),
                    });
                    this.state = CallState::Running(job);
                }
                CallState::Running(mut job) => {
                    let Poll::Ready(done) = Pin::new(&mut job).poll(cx) else {
                        this.state = CallState::Running(job);
                        return Poll::Pending;
                    };
                    // A `JoinError` is a panic inside the call: the job
                    // cannot be cancelled, as nothing but this holds its
                    // handle.
                    return Poll::Ready(done.map_err(io::Error::other).and_then(|done| done));
                }
                CallState::Finished => panic!("a file's call polled after it completed"),
            }
        }
    }
}

impl<F> fmt::Debug for FileCall<'_, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = match &self.state {
            CallState::Unstarted(_, call) => format!("{call:?}, not started"),
            CallState::Running(_) => "running".to_owned(),
            CallState::Finished => "finished".to_owned(),
        };
        f.debug_struct("FileCall").field("state", &state).finish()
    }
}

/// The future of [`File::sync_all`].
pub type SyncAll<'a> = FileCall<'a>;

/// The future of [`File::sync_data`].
pub type SyncData<'a> = FileCall<'a>;

/// The future of [`File::set_len`].
pub type SetLen<'a> = FileCall<'a>;

/// The future of [`File::allocate`].
pub type Allocate<'a> = FileCall<'a>;

/// Makes room for `len` bytes of `file` from `offset`, as [`File::allocate`]
/// says: extends the file to the end of the range if it ends before it.
fn allocate(file: &std::fs::File, offset: u64, len: u64) -> io::Result<()> {
    if len == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "a range to allocate holds at least one byte",
        ));
    }
    let end = offset.checked_add(len).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::FileTooLarge,
            "the range to allocate ends past what an offset counts",
        )
    })?;
    if end > file.metadata()?.len() {
        file.set_len(end)?;
    }
    Ok(())
}

/// How a file is opened: the options of the standard library's
/// `OpenOptions`, with the same meanings.
#[derive(Clone, Debug)]
pub struct OpenOptions {
    options: iocp::OpenOptions,
    /// Whether the file is opened to append, as its position then follows.
    append: bool,
}

impl OpenOptions {
    /// Options with none set: a file opened with them as they are can be
    /// neither read nor written.
    pub fn new() -> Self {
        Self {
            options: iocp::OpenOptions::new(),
            append: false,
        }
    }

    /// Opens the file for reading.
    pub fn read(&mut self, read: bool) -> &mut Self {
        self.options.read(read);
        self
    }

    /// Opens the file for writing.
    pub fn write(&mut self, write: bool) -> &mut Self {
        self.options.write(write);
        self
    }

    /// Opens the file for writing at its end, whatever offset or position
    /// a write names; a write at the position moves the position there.
    pub fn append(&mut self, append: bool) -> &mut Self {
        self.options.append(append);
        self.append = append;
        self
    }

    /// Empties the file as it is opened, which must be for writing.
    pub fn truncate(&mut self, truncate: bool) -> &mut Self {
        self.options.truncate(truncate);
        self
    }

    /// Creates the file if it does not exist, which needs it opened for
    /// writing or appending.
    pub fn create(&mut self, create: bool) -> &mut Self {
        self.options.create(create);
        self
    }

    /// Creates the file, and fails with `AlreadyExists` if it exists.
    pub fn create_new(&mut self, create_new: bool) -> &mut Self {
        self.options.create_new(create_new);
        self
    }

    /// Sets flags of `CreateFileW`'s `dwFlagsAndAttributes` beside
    /// `FILE_FLAG_OVERLAPPED`, which is always set, in place of any set
    /// before, as the standard library's `custom_flags` does on Windows.
    pub fn custom_flags(&mut self, flags: u32) -> &mut Self {
        self.options.custom_flags(flags);
        self
    }

    /// Opens the file at `path`, on the blocking pool, so that the thread's
    /// other tasks run meanwhile.
    ///
    /// # Errors
    ///
    /// What opening the file reports, and an error of kind `Other` where no
    /// runtime runs, before anything is opened.
    pub async fn open(&self, path: impl AsRef<Path>) -> io::Result<File> {
        // Asked first, so that a thread with no runtime for the file
        // creates and empties nothing.
        if ruio_iocp::Iocp::current().is_none() {
            return Err(io::Error::other(
                "the thread has no ruio runtime for the file's I/O to run on",
            ));
        }
        let options = self.options.clone();
        let path = path.as_ref().to_owned();
        let file = pooled(move || options.open_std(path)).await?;
        Ok(File {
            file: iocp::File::from_std(file)?,
            pos: Cell::new(0),
            append: self.append,
            at_end: Cell::new(false),
        })
    }

    /// Opens the file at `path` with these options as a [`portable::File`],
    /// which is `Send` and `Sync` and belongs to no port, on the blocking
    /// pool, as [`open`](OpenOptions::open) opens one. It needs no runtime to
    /// open it: each of the file's reads and writes waits on the port of the
    /// runtime on the thread that polls it.
    ///
    /// # Errors
    ///
    /// What opening the file reports: `InvalidInput`, say, for options that
    /// open it neither to read nor to write.
    pub async fn open_portable(&self, path: impl AsRef<Path>) -> io::Result<portable::File> {
        let options = self.options.clone();
        let path = path.as_ref().to_owned();
        let file = pooled(move || options.open_std(path)).await?;
        Ok(portable::File::new(file, 0, self.append))
    }
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// A file's status, as the standard library reports it.
#[derive(Clone, Debug)]
pub struct Metadata(std::fs::Metadata);

impl Metadata {
    /// The file's type.
    pub fn file_type(&self) -> FileType {
        FileType(self.0.file_type())
    }

    /// Whether the file is a regular file.
    pub fn is_file(&self) -> bool {
        self.0.is_file()
    }

    /// Whether the file is a directory.
    pub fn is_dir(&self) -> bool {
        self.0.is_dir()
    }

    /// Whether the file is a symbolic link, which a status reports only
    /// when asked not to follow it, as [`symlink_metadata`] does.
    pub fn is_symlink(&self) -> bool {
        self.0.is_symlink()
    }

    /// The file's size in bytes.
    #[expect(
        clippy::len_without_is_empty,
        reason = "as the standard library's `Metadata`, whose size is a length"
    )]
    pub fn len(&self) -> u64 {
        self.0.len()
    }

    /// The file's permissions.
    pub fn permissions(&self) -> std::fs::Permissions {
        self.0.permissions()
    }

    /// When the file was last modified.
    ///
    /// # Errors
    ///
    /// What the standard library reports if the system did not say.
    pub fn modified(&self) -> io::Result<SystemTime> {
        self.0.modified()
    }

    /// When the file was last read.
    ///
    /// # Errors
    ///
    /// As for [`modified`](Metadata::modified).
    pub fn accessed(&self) -> io::Result<SystemTime> {
        self.0.accessed()
    }

    /// When the file was made.
    ///
    /// # Errors
    ///
    /// As for [`modified`](Metadata::modified).
    pub fn created(&self) -> io::Result<SystemTime> {
        self.0.created()
    }
}

/// A file's type. Made by [`Metadata::file_type`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileType(std::fs::FileType);

impl FileType {
    /// Whether it is a regular file.
    pub fn is_file(self) -> bool {
        self.0.is_file()
    }

    /// Whether it is a directory.
    pub fn is_dir(self) -> bool {
        self.0.is_dir()
    }

    /// Whether it is a symbolic link.
    pub fn is_symlink(self) -> bool {
        self.0.is_symlink()
    }
}

/// The status of the file at `path`, which follows symbolic links.
///
/// # Errors
///
/// What the system reports, such as `NotFound`.
pub async fn metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
    let path = path.as_ref().to_owned();
    pooled(move || std::fs::metadata(path)).await.map(Metadata)
}

/// The status of the file at `path`, or of the symbolic link itself if it
/// is one.
///
/// # Errors
///
/// As for [`metadata`].
pub async fn symlink_metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
    let path = path.as_ref().to_owned();
    pooled(move || std::fs::symlink_metadata(path))
        .await
        .map(Metadata)
}

/// Whether anything is at `path`: `Ok(false)` if nothing is, and an error
/// if that cannot be told.
///
/// # Errors
///
/// What the system reports, other than that nothing is there.
pub async fn try_exists(path: impl AsRef<Path>) -> io::Result<bool> {
    let path = path.as_ref().to_owned();
    pooled(move || path.try_exists()).await
}

/// What the symbolic link at `path` points to.
///
/// # Errors
///
/// What the system reports, as for a path that is no symbolic link.
pub async fn read_link(path: impl AsRef<Path>) -> io::Result<PathBuf> {
    let path = path.as_ref().to_owned();
    pooled(move || std::fs::read_link(path)).await
}

/// The absolute path of `path`, with every symbolic link in it followed,
/// as the standard library gives it: on Windows, in the `\\?\` form.
///
/// # Errors
///
/// What the system reports, such as `NotFound`.
pub async fn canonicalize(path: impl AsRef<Path>) -> io::Result<PathBuf> {
    let path = path.as_ref().to_owned();
    pooled(move || std::fs::canonicalize(path)).await
}

/// Sets the permissions of the file at `path`.
///
/// # Errors
///
/// What the system reports.
pub async fn set_permissions(path: impl AsRef<Path>, perm: std::fs::Permissions) -> io::Result<()> {
    let path = path.as_ref().to_owned();
    pooled(move || std::fs::set_permissions(path, perm)).await
}

/// Reads the whole file at `path`, as the standard library's `fs::read`
/// does, on the blocking pool.
///
/// # Errors
///
/// What opening and reading the file report.
pub async fn read(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
    let path = path.as_ref().to_owned();
    pooled(move || std::fs::read(path)).await
}

/// Reads the whole file at `path` as a string.
///
/// # Errors
///
/// As for [`read`], and `InvalidData` if the file is not UTF-8.
pub async fn read_to_string(path: impl AsRef<Path>) -> io::Result<String> {
    let path = path.as_ref().to_owned();
    pooled(move || std::fs::read_to_string(path)).await
}

/// Writes `contents` to the file at `path`, creating it or emptying it
/// first, on the blocking pool, whose thread gets a copy of the bytes: the
/// buffer itself may belong to this one.
///
/// # Errors
///
/// What opening and writing the file report.
pub async fn write<B: IoBuf>(path: impl AsRef<Path>, contents: B) -> io::Result<()> {
    let path = path.as_ref().to_owned();
    // SAFETY: an `IoBuf`'s first `bytes_init` bytes from `stable_ptr` are
    // initialized, and the buffer is not touched while they are copied.
    let bytes = unsafe { slice::from_raw_parts(contents.stable_ptr(), contents.bytes_init()) };
    let bytes = bytes.to_vec();
    drop(contents);
    pooled(move || std::fs::write(path, bytes)).await
}

/// Removes the file at `path`.
///
/// # Errors
///
/// What the system reports, such as `NotFound`.
pub async fn remove_file(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref().to_owned();
    pooled(move || std::fs::remove_file(path)).await
}

/// Removes the directory at `path`, which must be empty.
///
/// # Errors
///
/// What the system reports.
pub async fn remove_dir(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref().to_owned();
    pooled(move || std::fs::remove_dir(path)).await
}

/// Creates a directory at `path`, in a directory that exists.
///
/// # Errors
///
/// What the system reports, such as `AlreadyExists`.
pub async fn create_dir(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref().to_owned();
    pooled(move || std::fs::create_dir(path)).await
}

/// Creates a directory at `path`, and every directory above it that does
/// not exist. It is no error for the directory to exist already.
///
/// # Errors
///
/// What the system reports.
pub async fn create_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref().to_owned();
    pooled(move || std::fs::create_dir_all(path)).await
}

/// Renames `from` to `to`, replacing a file there.
///
/// # Errors
///
/// What the system reports.
pub async fn rename(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
    let (from, to) = (from.as_ref().to_owned(), to.as_ref().to_owned());
    pooled(move || std::fs::rename(from, to)).await
}

/// Makes `link` another name of the file `original`.
///
/// # Errors
///
/// What the system reports.
pub async fn hard_link(original: impl AsRef<Path>, link: impl AsRef<Path>) -> io::Result<()> {
    let (original, link) = (original.as_ref().to_owned(), link.as_ref().to_owned());
    pooled(move || std::fs::hard_link(original, link)).await
}

/// Makes `link` a symbolic link to `original`, which it holds as given and
/// which need not exist.
///
/// Windows has links of two kinds, to files and to directories, and is
/// told which as a link is made: this makes a link to a directory if
/// `original` names one then, and a link to a file if it does not. A
/// relative `original` is looked for where the link resolves it, from the
/// link's own directory.
///
/// # Errors
///
/// What the system reports. It lets an account make links only with a
/// privilege, which an administrator has, and anyone in developer mode;
/// without it the error's code is 1314.
pub async fn symlink(original: impl AsRef<Path>, link: impl AsRef<Path>) -> io::Result<()> {
    use std::os::windows::fs::{symlink_dir, symlink_file};

    let (original, link) = (original.as_ref().to_owned(), link.as_ref().to_owned());
    pooled(move || {
        let named = match link.parent() {
            Some(dir) if original.is_relative() => dir.join(&original),
            _ => original.clone(),
        };
        if std::fs::metadata(named).is_ok_and(|named| named.is_dir()) {
            symlink_dir(original, link)
        } else {
            symlink_file(original, link)
        }
    })
    .await
}

/// Copies the file `from` to `to`, replacing a file there, and reports how
/// many bytes it copied.
///
/// # Errors
///
/// What the system reports.
pub async fn copy(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<u64> {
    let (from, to) = (from.as_ref().to_owned(), to.as_ref().to_owned());
    pooled(move || std::fs::copy(from, to)).await
}

/// Removes the directory at `path` and everything in it, as the standard
/// library's `remove_dir_all` does.
///
/// # Errors
///
/// What the system reports.
pub async fn remove_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref().to_owned();
    pooled(move || std::fs::remove_dir_all(path)).await
}

/// The entries of the directory at `path`, which [`ReadDir`] reads.
///
/// # Errors
///
/// What the system reports, such as `NotFound`, or `NotADirectory` for a
/// path that names something else.
pub async fn read_dir(path: impl AsRef<Path>) -> io::Result<ReadDir> {
    let path = path.as_ref().to_owned();
    let dir = pooled(move || std::fs::read_dir(path)).await?;
    Ok(ReadDir {
        entries: VecDeque::new(),
        state: DirState::Idle(Box::new(dir)),
    })
}

/// The entries of a directory, in the directory's order, without `.` and
/// `..`. Made by [`read_dir`].
///
/// The blocking pool reads the entries a batch at a time, as the standard
/// library's reader hands them out, each batch once the one before it has
/// been taken. An error ends the reading.
///
/// ```
/// fn assert_send<T: Send + Sync>() {}
///
/// assert_send::<ruio::fs::ReadDir>();
/// assert_send::<ruio::fs::DirEntry>();
/// ```
pub struct ReadDir {
    /// The entries read and not yet taken.
    entries: VecDeque<io::Result<std::fs::DirEntry>>,
    state: DirState,
}

enum DirState {
    /// The directory, between batches; boxed, as Windows' reader is some
    /// hundreds of bytes.
    Idle(Box<std::fs::ReadDir>),
    /// The next batch, being read on the blocking pool.
    Reading(BlockingHandle<Batch>),
    /// The directory has ended, or reading it has failed.
    Ended,
}

/// A batch of a directory's entries, and the directory, unless it has
/// ended or reading it has failed.
type Batch = (
    Option<Box<std::fs::ReadDir>>,
    VecDeque<io::Result<std::fs::DirEntry>>,
);

/// Reads up to [`BATCH`] of `dir`'s entries, stopping at its end or at an
/// error, which is the batch's last entry.
fn read_batch(mut dir: Box<std::fs::ReadDir>) -> Batch {
    let mut entries = VecDeque::with_capacity(BATCH);
    while entries.len() < BATCH {
        let Some(entry) = dir.next() else {
            return (None, entries);
        };
        let failed = entry.is_err();
        entries.push_back(entry);
        if failed {
            return (None, entries);
        }
    }
    (Some(dir), entries)
}

impl ReadDir {
    /// The next entry, or `None` once there are no more.
    ///
    /// # Errors
    ///
    /// What reading the directory reports.
    pub async fn next_entry(&mut self) -> io::Result<Option<DirEntry>> {
        poll_fn(|cx| self.poll_next_entry(cx)).await
    }

    /// The next entry, or `None` once there are no more; a batch the pool
    /// has not read yet is waited for.
    ///
    /// # Errors
    ///
    /// As for [`next_entry`](ReadDir::next_entry).
    pub fn poll_next_entry(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Option<DirEntry>>> {
        loop {
            if let Some(entry) = self.entries.pop_front() {
                return Poll::Ready(entry.map(|entry| Some(DirEntry(entry))));
            }
            match mem::replace(&mut self.state, DirState::Ended) {
                DirState::Idle(dir) => {
                    let job = ruio_executor::spawn_blocking(move || read_batch(dir));
                    self.state = DirState::Reading(job);
                }
                DirState::Reading(mut job) => {
                    let Poll::Ready(batch) = Pin::new(&mut job).poll(cx) else {
                        self.state = DirState::Reading(job);
                        return Poll::Pending;
                    };
                    // A `JoinError` is a panic inside the read: the job
                    // cannot be cancelled, as nothing but this holds its
                    // handle.
                    let (dir, entries) = batch.map_err(io::Error::other)?;
                    self.entries = entries;
                    if let Some(dir) = dir {
                        self.state = DirState::Idle(dir);
                    }
                }
                DirState::Ended => return Poll::Ready(Ok(None)),
            }
        }
    }

    /// Whether every entry has been taken.
    #[cfg(feature = "futures")]
    fn is_ended(&self) -> bool {
        self.entries.is_empty() && matches!(self.state, DirState::Ended)
    }
}

impl fmt::Debug for ReadDir {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadDir")
            .field("read", &self.entries.len())
            .finish_non_exhaustive()
    }
}

/// Each item is an entry, or the error that stopped the reading; the stream
/// ends at the end of the directory, and after an error.
#[cfg(feature = "futures")]
impl futures_core::Stream for ReadDir {
    type Item = io::Result<DirEntry>;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().poll_next_entry(cx).map(Result::transpose)
    }
}

/// Terminated once every entry has been taken.
#[cfg(feature = "futures")]
impl futures_core::stream::FusedStream for ReadDir {
    #[inline]
    fn is_terminated(&self) -> bool {
        self.is_ended()
    }
}

/// An entry of a directory, which [`ReadDir`] reads.
#[derive(Debug)]
pub struct DirEntry(std::fs::DirEntry);

// The type and the status come with the entry on Windows, and cost no call
// to the system; they are `async` to have the signatures they have on
// io_uring.
#[expect(
    clippy::unused_async,
    reason = "the signatures are io_uring's, where these may wait for the ring"
)]
impl DirEntry {
    /// The entry's path: the directory's, as it was given to [`read_dir`],
    /// joined with the entry's name.
    pub fn path(&self) -> PathBuf {
        self.0.path()
    }

    /// The entry's name in the directory.
    pub fn file_name(&self) -> OsString {
        self.0.file_name()
    }

    /// The entry's type, which the directory says; a symbolic link is a
    /// symbolic link.
    ///
    /// # Errors
    ///
    /// What the standard library reports.
    pub async fn file_type(&self) -> io::Result<FileType> {
        self.0.file_type().map(FileType)
    }

    /// The entry's status as the directory had it, of a symbolic link
    /// itself rather than what it points to.
    ///
    /// # Errors
    ///
    /// What the standard library reports.
    pub async fn metadata(&self) -> io::Result<Metadata> {
        self.0.metadata().map(Metadata)
    }
}
