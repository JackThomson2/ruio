//! Files that are `Send`, whose operations run on the ring of whichever
//! thread polls them.

mod support;

use std::future::Future;
use std::io::{self, ErrorKind};
use std::os::fd::AsFd;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::{Duration, Instant};

use ruio::fs::OpenOptions;
use ruio::fs::portable::File;
use ruio::io::{AsyncRead, AsyncWrite};

use support::{PATIENCE, within};

/// A directory of the test's own, removed when dropped.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("ruio-fs-portable-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|index| (index % 251) as u8).collect()
}

fn assert_send<T: Send>(_: &T) {}

fn assert_send_sync<T: Send + Sync>() {}

/// Writes through a borrowed file in a boxed future that must be `Send`, as
/// `async_trait` makes one: a `&'static str` and a `Vec<u8>`.
fn write_boxed(file: &File) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
    Box::pin(async move {
        file.write_all_at("static ", 0).await.0?;
        file.write_all_at(b"vector".to_vec(), 7).await.0?;
        Ok(())
    })
}

/// Holds the file itself across the awaits of a boxed future that must be
/// `Send`.
fn hold_boxed(file: File) -> Pin<Box<dyn Future<Output = io::Result<Vec<u8>>> + Send>> {
    Box::pin(async move {
        file.write_all_at("held", 13).await.0?;
        file.sync_data().await?;
        let (read, data) = file.read_exact_at(Vec::with_capacity(17), 0).await;
        read?;
        file.close().await?;
        Ok(data)
    })
}

#[test]
fn the_file_and_its_futures_are_send() {
    assert_send_sync::<File>();
    fn futures(file: &File) {
        assert_send(&file.read_at(Vec::with_capacity(8), 0));
        assert_send(&file.write_at("data", 0));
        assert_send(&file.read_exact_at(Vec::with_capacity(8), 0));
        assert_send(&file.write_all_at("data", 0));
        assert_send(&file.write_all_at(b"data".to_vec(), 0));
        assert_send(&file.read(Vec::with_capacity(8)));
        assert_send(&file.write("data"));
        assert_send(&file.sync_all());
        assert_send(&file.set_len(0));
        assert_send(&file.metadata());
    }
    let _ = futures;
    assert_send(&File::open("anywhere"));
    assert_send(&OpenOptions::new().read(true).open_portable("anywhere"));
}

#[ruio::test]
async fn boxed_send_futures_write_and_read_the_file() -> io::Result<()> {
    let scratch = Scratch::new("boxed");
    let path = scratch.path("file");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open_portable(&path)
        .await?;
    within(write_boxed(&file)).await?;
    let data = within(hold_boxed(file)).await?;
    assert_eq!(data, b"static vectorheld");
    Ok(())
}

#[ruio::test]
async fn files_are_written_and_read_back_at_offsets() -> io::Result<()> {
    let scratch = Scratch::new("rw");
    let path = scratch.path("file");
    let file = File::create(&path).await?;
    let data = pattern(100_000);
    within(file.write_all_at(data.clone(), 0)).await.0?;
    within(file.write_all_at(b"tail".to_vec(), 100_000))
        .await
        .0?;
    let (written, _) = within(file.write_at("!", 100_004)).await;
    assert_eq!(written?, 1);
    within(file.close()).await?;

    let file = File::open(&path).await?;
    let (read, back) = within(file.read_exact_at(Vec::with_capacity(100_005), 0)).await;
    read?;
    assert_eq!(&back[..100_000], &data[..]);
    assert_eq!(&back[100_000..], b"tail!");
    let (read, part) = within(file.read_at(Vec::with_capacity(4), 100_000)).await;
    assert_eq!(read?, 4);
    assert_eq!(part, b"tail");
    assert_eq!(std::fs::read(&path)?.len(), 100_005);
    Ok(())
}

#[ruio::test]
async fn reads_and_writes_at_the_position_advance_it() -> io::Result<()> {
    let scratch = Scratch::new("position");
    let path = scratch.path("file");
    let file = File::create(&path).await?;
    let (written, _) = within(file.write("one ")).await;
    assert_eq!(written?, 4);
    within(AsyncWrite::write_all(&file, b"two".to_vec()))
        .await
        .0?;
    within(file.close()).await?;
    assert_eq!(std::fs::read(&path)?, b"one two");

    let file = File::open(&path).await?;
    let (read, first) = within(file.read(Vec::with_capacity(4))).await;
    assert_eq!(read?, 4);
    assert_eq!(first, b"one ");
    let (read, rest) = within(AsyncRead::read_to_end(&file, Vec::new())).await;
    assert_eq!(read?, 3);
    assert_eq!(rest, b"two");
    let (read, _) = within(file.read(Vec::with_capacity(4))).await;
    assert_eq!(read?, 0);
    Ok(())
}

#[ruio::test]
async fn read_exact_at_past_the_end_is_unexpected_eof() -> io::Result<()> {
    let scratch = Scratch::new("eof");
    let path = scratch.path("file");
    std::fs::write(&path, b"short")?;
    let file = File::open(&path).await?;
    let (read, buf) = within(file.read_exact_at(Vec::with_capacity(8), 2)).await;
    assert_eq!(read.unwrap_err().kind(), ErrorKind::UnexpectedEof);
    assert_eq!(buf, b"ort");
    let (read, buf) = within(file.read_exact_at(Vec::with_capacity(8), 100)).await;
    assert_eq!(read.unwrap_err().kind(), ErrorKind::UnexpectedEof);
    assert!(buf.is_empty());
    Ok(())
}

#[ruio::test]
async fn set_len_shows_in_the_metadata_and_syncs_succeed() -> io::Result<()> {
    let scratch = Scratch::new("len");
    let path = scratch.path("file");
    let file = File::create(&path).await?;
    within(file.write_all_at(pattern(1000), 0)).await.0?;
    within(file.sync_all()).await?;
    within(file.sync_data()).await?;
    let metadata = within(file.metadata()).await?;
    assert!(metadata.is_file());
    assert_eq!(metadata.len(), 1000);
    within(file.set_len(10)).await?;
    assert_eq!(within(file.metadata()).await?.len(), 10);
    within(file.set_len(4096)).await?;
    assert_eq!(within(file.metadata()).await?.len(), 4096);
    within(file.close()).await?;
    assert_eq!(std::fs::metadata(&path)?.len(), 4096);
    Ok(())
}

#[ruio::test]
async fn files_convert_to_and_from_the_standard_library() -> io::Result<()> {
    let scratch = Scratch::new("std");
    let path = scratch.path("file");
    std::fs::write(&path, b"from std")?;
    let file = File::from_std(std::fs::File::open(&path)?);
    let (read, data) = within(file.read_exact_at(Vec::with_capacity(8), 0)).await;
    read?;
    assert_eq!(data, b"from std");
    assert!(format!("{file:?}").starts_with("File"));
    let std = file.into_std();
    assert_eq!(std.metadata()?.len(), 8);
    Ok(())
}

#[ruio::test]
async fn open_portable_refuses_what_a_plain_descriptor_cannot_honour() -> io::Result<()> {
    let scratch = Scratch::new("refused");
    let path = scratch.path("file");
    std::fs::write(&path, b"")?;
    let err = OpenOptions::new()
        .read(true)
        .direct_descriptor(true)
        .open_portable(&path)
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
    let err = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .polled(true)
        .open_portable(&path)
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
    let err = OpenOptions::new().open_portable(&path).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
    let err = File::open(scratch.path("missing")).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    Ok(())
}

#[test]
fn a_file_written_on_one_core_is_read_on_another() {
    let all = ruio::Cores::all().unwrap();
    let cores = ruio::Cores::cpus(all.cpu_list().iter().copied().cycle().take(2));
    let scratch = Scratch::new("cores");
    let path = scratch.path("file");
    let outputs = cores
        .run(|core| {
            let path = path.clone();
            async move {
                if core.index() != 0 {
                    return None;
                }
                let run = async move {
                    let file = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open_portable(&path)
                        .await?;
                    within(file.write_all_at(b"from core zero".to_vec(), 0))
                        .await
                        .0?;
                    let task = ruio::spawn_on(1, async move {
                        let (read, data) =
                            within(file.read_exact_at(Vec::with_capacity(14), 0)).await;
                        read?;
                        within(file.write_all_at(", read on one", 14)).await.0?;
                        within(file.close()).await?;
                        io::Result::Ok((data, ruio::core().unwrap().index()))
                    });
                    let (data, core) = within(task).await.map_err(io::Error::other)??;
                    io::Result::Ok((data, core, std::fs::read(&path)?))
                };
                Some(run.await)
            }
        })
        .unwrap();
    let (data, core, whole) = outputs.into_iter().next().flatten().unwrap().unwrap();
    assert_eq!((&data[..], core), (&b"from core zero"[..], 1));
    assert_eq!(whole, b"from core zero, read on one");
}

/// Locks the file the descriptor of `file` is open on, with `flock(2)`,
/// which holds the lock until every descriptor of that open file is closed.
fn lock(file: &File) -> io::Result<()> {
    let dup = std::fs::File::from(file.as_fd().try_clone_to_owned()?);
    dup.try_lock().map_err(io::Error::from)
}

/// Opens the file at `path` and writes `data` to it on a runtime, locked
/// with `flock(2)`, which another open of the file then cannot take.
fn written_and_locked(runtime: &ruio::Runtime, path: &PathBuf, data: &'static str) -> File {
    let file = runtime.block_on(async {
        let file = File::create(path).await?;
        within(file.write_all_at(data, 0)).await.0?;
        within(file.sync_data()).await?;
        io::Result::Ok(file)
    });
    let file = file.unwrap();
    lock(&file).unwrap();
    let other = std::fs::File::open(path).unwrap();
    assert!(other.try_lock().is_err(), "the file holds its lock");
    file
}

#[test]
fn a_file_dropped_off_its_runtime_closes_at_the_rings_next_turn() {
    let scratch = Scratch::new("dropped");
    let path = scratch.path("file");
    let runtime = ruio::Runtime::new().unwrap();
    let file = written_and_locked(&runtime, &path, "complete");
    std::thread::spawn(move || drop(file)).join().unwrap();
    // The close goes to the ring the file's operations ran on, which takes
    // it at its next turn: until then the file is open.
    let other = std::fs::File::open(&path).unwrap();
    assert!(
        other.try_lock().is_err(),
        "the file closed before its ring's turn"
    );
    runtime.block_on(async {
        let deadline = Instant::now() + PATIENCE;
        while other.try_lock().is_err() {
            assert!(Instant::now() < deadline, "the file never closed");
            ruio::time::sleep(Duration::from_millis(1)).await;
        }
    });
    assert_eq!(std::fs::read(&path).unwrap(), b"complete");
}

#[test]
fn a_file_dropped_after_its_runtime_closes_at_once() {
    let scratch = Scratch::new("orphaned");
    let path = scratch.path("file");
    let runtime = ruio::Runtime::new().unwrap();
    let file = written_and_locked(&runtime, &path, "complete too");
    drop(runtime);
    std::thread::spawn(move || drop(file)).join().unwrap();
    let other = std::fs::File::open(&path).unwrap();
    other.try_lock().unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"complete too");
}
