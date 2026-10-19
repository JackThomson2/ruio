//! Cold-cache reads of a directory tree: how many reads the block device is
//! asked for when every file of the tree is read whole, with
//! `std::fs::read` and with `ruio::fs::read`, from a page cache that holds
//! none of the tree.
//!
//! Each run first drops the tree's pages from the page cache, file by file,
//! with `posix_fadvise(POSIX_FADV_DONTNEED)`, which needs no privilege and
//! leaves the rest of the host's cache alone; dirty pages, such as a fresh
//! copy leaves, are written back first, since the kernel drops only clean
//! ones. Then it reads every file in turn on one thread pinned to a CPU, and
//! counts how far the device's statistics, `/sys/dev/block/MAJ:MIN/stat`,
//! moved meanwhile: reads completed, reads merged, and sectors read. Fewer,
//! larger reads for the same data mean the kernel's readahead saw more of
//! each request.
//!
//! The counts are the device's, not the process's: any other I/O on the
//! same device while a reader runs adds to them, so the benchmark wants a
//! quiet host, and several runs, which the readers take in turns, the one
//! that goes first alternating from run to run. A page the kernel cannot
//! drop, because another process maps it, is read from the cache rather
//! than the device, and the share of the data the device delivered shows
//! it: well under 100% means the eviction failed, and that run's counts are
//! low.

use std::fmt;
use std::fs::{self, File};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::{median, sys};

/// What to read, and how.
#[derive(Debug)]
pub(crate) struct Load {
    /// The directory whose tree is read.
    pub(crate) dir: PathBuf,
    /// How many times each reader reads the tree.
    pub(crate) runs: usize,
    /// The CPU the reading thread runs on.
    pub(crate) cpu: usize,
}

/// The size of a sector in the device's statistics, whatever the device's
/// own sector size.
const SECTOR: f64 = 512.0;

/// A mebibyte, in bytes.
const MIB: f64 = (1 << 20) as f64;

/// Reads every file under the load's directory cold, with each reader in
/// turns, and reports what the device was asked for, run by run, and the
/// medians.
pub(crate) fn run(load: &Load) -> Result<(), String> {
    sys::pin(&[load.cpu]).map_err(|err| format!("pinning to CPU {}: {err}", load.cpu))?;
    let tree = Tree::walk(&load.dir)?;
    if tree.files.is_empty() {
        return Err(format!("{} holds no regular file", load.dir.display()));
    }
    let device = Device::of(&load.dir)?;
    let readahead = device
        .read_ahead_kb()
        .map_or_else(String::new, |kb| format!(", readahead {kb} KiB"));
    println!(
        "Cold reads of the {} files under {}, {:.1} MiB, from {} ({}){readahead}, on CPU {}: \
         the device's reads while each reader ran, for each run and the medians of {} runs, \
         which the readers take in turns. Delivered is the share of the data the device \
         read; well under 100% means some pages stayed in the cache.",
        tree.files.len(),
        load.dir.display(),
        tree.bytes as f64 / MIB,
        device.name,
        device.number,
        load.cpu,
        load.runs
    );
    println!();
    println!(
        "| run | reader | files | MiB read | reads | merged | MiB from device | KiB per read \
         | delivered | ms |"
    );
    println!("|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|");
    let mut samples: [Vec<Figures>; 2] = [Vec::new(), Vec::new()];
    for round in 1..=load.runs {
        let order = if round % 2 == 1 {
            [Reader::Std, Reader::Ruio]
        } else {
            [Reader::Ruio, Reader::Std]
        };
        for reader in order {
            eprintln!("{reader}, run {round}");
            let figures = measure(reader, &tree, &device)?;
            figures.print(&round.to_string(), reader, tree.files.len());
            samples[reader.index()].push(figures);
        }
    }
    let [std, ruio] = samples.map(|runs| Figures::median(&runs));
    std.print("median", Reader::Std, tree.files.len());
    ruio.print("median", Reader::Ruio, tree.files.len());
    println!();
    println!(
        "ruio made {:.2}x the device reads std did, of {:.1} KiB each against {:.1} KiB.",
        ruio.reads / std.reads,
        ruio.kib_per_read(),
        std.kib_per_read()
    );
    Ok(())
}

/// Drops the tree's pages, then reads it whole with `reader`, and returns
/// what the device did meanwhile.
fn measure(reader: Reader, tree: &Tree, device: &Device) -> Result<Figures, String> {
    tree.evict()?;
    // The runtime starts before the device is sampled, so that the
    // measurement holds only the reads.
    let runtime = match reader {
        Reader::Std => None,
        Reader::Ruio => {
            Some(ruio::Runtime::new().map_err(|err| format!("starting the runtime: {err}"))?)
        }
    };
    let before = device.counters()?;
    let start = Instant::now();
    let bytes = match &runtime {
        None => read_std(tree)?,
        Some(runtime) => read_ruio(runtime, tree)?,
    };
    let elapsed = start.elapsed();
    let after = device.counters()?;
    if bytes != tree.bytes {
        return Err(format!(
            "{reader} read {bytes} B where the tree held {} B: did it change?",
            tree.bytes
        ));
    }
    Ok(Figures {
        bytes: bytes as f64,
        reads: after.reads.wrapping_sub(before.reads) as f64,
        merged: after.merged.wrapping_sub(before.merged) as f64,
        sectors: after.sectors.wrapping_sub(before.sectors) as f64,
        ms: elapsed.as_secs_f64() * 1e3,
    })
}

/// Reads every file of the tree with `std::fs::read`, one after another,
/// and returns how many bytes they held.
fn read_std(tree: &Tree) -> Result<u64, String> {
    let mut bytes = 0;
    for (path, _) in &tree.files {
        let data =
            fs::read(path).map_err(|err| format!("std reading {}: {err}", path.display()))?;
        bytes += data.len() as u64;
    }
    Ok(bytes)
}

/// Reads every file of the tree with `ruio::fs::read`, one after another,
/// on `runtime`, and returns how many bytes they held.
fn read_ruio(runtime: &ruio::Runtime, tree: &Tree) -> Result<u64, String> {
    runtime.block_on(async {
        let mut bytes = 0;
        for (path, _) in &tree.files {
            let data = ruio::fs::read(path)
                .await
                .map_err(|err| format!("ruio reading {}: {err}", path.display()))?;
            bytes += data.len() as u64;
        }
        Ok(bytes)
    })
}

/// The two ways of reading a file whole.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Reader {
    /// `std::fs::read`, on the calling thread.
    Std,
    /// `ruio::fs::read`, on a runtime of the calling thread.
    Ruio,
}

impl Reader {
    /// Where the reader's samples go.
    fn index(self) -> usize {
        match self {
            Self::Std => 0,
            Self::Ruio => 1,
        }
    }
}

impl fmt::Display for Reader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Std => "std",
            Self::Ruio => "ruio",
        })
    }
}

/// One reader's figures for one run, or their medians over the runs.
#[derive(Clone, Copy, Debug)]
struct Figures {
    /// The bytes the files held.
    bytes: f64,
    /// The reads the device completed.
    reads: f64,
    /// The reads the kernel merged into others before the device saw them.
    merged: f64,
    /// The sectors the device read, of [`SECTOR`] bytes.
    sectors: f64,
    /// How long the reads took, in milliseconds.
    ms: f64,
}

impl Figures {
    /// The median of each figure over the runs.
    fn median(runs: &[Self]) -> Self {
        let of = |figure: fn(&Self) -> f64| median(runs.iter().map(figure));
        Self {
            bytes: of(|figures| figures.bytes),
            reads: of(|figures| figures.reads),
            merged: of(|figures| figures.merged),
            sectors: of(|figures| figures.sectors),
            ms: of(|figures| figures.ms),
        }
    }

    /// The bytes the device read.
    fn device_bytes(&self) -> f64 {
        self.sectors * SECTOR
    }

    /// The average size of a read the device completed, in KiB.
    fn kib_per_read(&self) -> f64 {
        self.device_bytes() / 1024.0 / self.reads
    }

    /// Prints the figures as a row of the report's table.
    fn print(&self, run: &str, reader: Reader, files: usize) {
        let per_read = if self.reads > 0.0 {
            format!("{:.1}", self.kib_per_read())
        } else {
            "-".to_owned()
        };
        println!(
            "| {run} | {reader} | {files} | {:.1} | {:.0} | {:.0} | {:.1} | {per_read} | {:.0}% \
             | {:.0} |",
            self.bytes / MIB,
            self.reads,
            self.merged,
            self.device_bytes() / MIB,
            100.0 * self.device_bytes() / self.bytes,
            self.ms
        );
    }
}

/// The regular files of a tree, in a stable order, with their sizes.
#[derive(Debug)]
struct Tree {
    /// Each file's path, and its size in bytes, sorted by path.
    files: Vec<(PathBuf, u64)>,
    /// The sum of the sizes.
    bytes: u64,
}

impl Tree {
    /// Walks the tree under `dir`, without following symbolic links, and
    /// keeps its regular files.
    fn walk(dir: &Path) -> Result<Self, String> {
        let mut files = Vec::new();
        let mut dirs = vec![dir.to_owned()];
        while let Some(dir) = dirs.pop() {
            let listing = |err: io::Error| format!("listing {}: {err}", dir.display());
            for entry in fs::read_dir(&dir).map_err(listing)? {
                let entry = entry.map_err(listing)?;
                let path = entry.path();
                // The entry's own type, and its own metadata: neither
                // follows a symbolic link.
                let kind = entry.file_type().map_err(listing)?;
                if kind.is_dir() {
                    dirs.push(path);
                } else if kind.is_file() {
                    let size = entry
                        .metadata()
                        .map_err(|err| format!("{}: {err}", path.display()))?
                        .len();
                    files.push((path, size));
                }
            }
        }
        files.sort();
        let bytes = files.iter().map(|(_, size)| size).sum();
        Ok(Self { files, bytes })
    }

    /// Drops the tree's pages from the page cache: writes each file's
    /// dirty pages back, then advises the kernel that none of the file is
    /// needed, which drops the clean pages no process maps.
    fn evict(&self) -> Result<(), String> {
        for (path, _) in &self.files {
            let evicting = |err: io::Error| format!("evicting {}: {err}", path.display());
            let file = File::open(path).map_err(evicting)?;
            file.sync_data().map_err(evicting)?;
            // SAFETY: `posix_fadvise` takes a descriptor, which `file`
            // keeps open, and numbers; it touches no memory of ours.
            let err =
                unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
            if err != 0 {
                return Err(evicting(io::Error::from_raw_os_error(err)));
            }
        }
        Ok(())
    }
}

/// The block device a tree is on: a disk, or a partition of one.
#[derive(Debug)]
struct Device {
    /// The kernel's name for it, such as `nvme0n1p1`.
    name: String,
    /// Its major and minor numbers, as `MAJ:MIN`.
    number: String,
    /// Its directory in sysfs, `/sys/dev/block/MAJ:MIN`.
    sys: PathBuf,
}

impl Device {
    /// The block device the file system holding `dir` is on.
    fn of(dir: &Path) -> Result<Self, String> {
        let dev = fs::metadata(dir)
            .map_err(|err| format!("{}: {err}", dir.display()))?
            .dev();
        let (major, minor) = (libc::major(dev), libc::minor(dev));
        let number = format!("{major}:{minor}");
        if major == 0 {
            return Err(format!(
                "{} is on device {number}, which no block device is behind, as for tmpfs; \
                 its reads cannot be counted",
                dir.display()
            ));
        }
        let sys = PathBuf::from(format!("/sys/dev/block/{number}"));
        if !sys.join("stat").is_file() {
            return Err(format!(
                "{} is on device {number}, which has no {}/stat to count its reads",
                dir.display(),
                sys.display()
            ));
        }
        let name = fs::canonicalize(&sys)
            .ok()
            .and_then(|real| Some(real.file_name()?.to_string_lossy().into_owned()))
            .unwrap_or_else(|| number.clone());
        Ok(Self { name, number, sys })
    }

    /// How far the kernel reads ahead on the device, in KiB: a disk's own
    /// setting, or for a partition, its disk's.
    fn read_ahead_kb(&self) -> Option<u64> {
        ["queue/read_ahead_kb", "../queue/read_ahead_kb"]
            .iter()
            .find_map(|setting| {
                fs::read_to_string(self.sys.join(setting))
                    .ok()?
                    .trim()
                    .parse()
                    .ok()
            })
    }

    /// The device's read counters now.
    fn counters(&self) -> Result<Counters, String> {
        let path = self.sys.join("stat");
        let stat = fs::read_to_string(&path).map_err(|err| format!("{}: {err}", path.display()))?;
        Counters::parse(&stat).ok_or_else(|| format!("{}: unexpected {stat:?}", path.display()))
    }
}

/// A block device's read counters at one moment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Counters {
    /// The reads it has completed.
    reads: u64,
    /// The reads the kernel merged into others before they reached it.
    merged: u64,
    /// The sectors it has read, of [`SECTOR`] bytes.
    sectors: u64,
}

impl Counters {
    /// The counters in a device's `stat` file, whose first three fields
    /// are these (the kernel's Documentation/block/stat.rst).
    fn parse(stat: &str) -> Option<Self> {
        let mut fields = stat.split_whitespace();
        let mut next = || fields.next()?.parse().ok();
        Some(Self {
            reads: next()?,
            merged: next()?,
            sectors: next()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stat_files_give_their_read_counters() {
        let stat = " 13684356  2655652 825863203 52173301 23454709 31763971 2970950696 \
                    92156986        0 10376973 144330287        0        0        0        0\n";
        assert_eq!(
            Counters::parse(stat),
            Some(Counters {
                reads: 13_684_356,
                merged: 2_655_652,
                sectors: 825_863_203,
            })
        );
        assert_eq!(Counters::parse("1 2"), None);
        assert_eq!(Counters::parse("1 x 3"), None);
    }

    #[test]
    fn medians_are_taken_figure_by_figure() {
        let figures = |reads: f64, ms: f64| Figures {
            bytes: 1024.0,
            reads,
            merged: 0.0,
            sectors: 2.0 * reads,
            ms,
        };
        let median = Figures::median(&[figures(3.0, 10.0), figures(1.0, 30.0), figures(2.0, 20.0)]);
        assert_eq!(median.reads, 2.0);
        assert_eq!(median.ms, 20.0);
        assert_eq!(median.kib_per_read(), 1.0);
    }
}
