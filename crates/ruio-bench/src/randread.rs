//! Random reads with direct I/O: ruio's reader, and fio's for comparison.

use std::cell::Cell;
use std::fs;
use std::io::{self, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::rc::Rc;
use std::time::{Duration, Instant};

use ruio::buf::{AlignedBuf, FixedBuffers};
use ruio::fs::{File, OpenOptions};
use ruio::time::sleep;

use crate::sys;

/// What to read, and how.
#[derive(Clone, Debug)]
pub(crate) struct Load {
    /// A file or block device, opened for direct I/O.
    pub(crate) path: PathBuf,
    /// How many reads are outstanding at once.
    pub(crate) depth: usize,
    /// The size of each read, and what its offset is a multiple of.
    pub(crate) block: usize,
    pub(crate) warmup: Duration,
    pub(crate) measure: Duration,
    /// The CPU to run on.
    pub(crate) cpu: usize,
}

/// The size of the file or device at `path`.
fn size(path: &Path) -> io::Result<u64> {
    // A block device's status gives no size; the end of it does.
    fs::File::open(path)?.seek(SeekFrom::End(0))
}

/// A small, fast pseudo-random number generator (xorshift64*).
struct Rng(u64);

impl Rng {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d) % bound
    }
}

/// Reads with ruio: a task per outstanding read, each reading a block at a
/// random offset into a registered buffer, again and again, from a file
/// opened for direct I/O as a direct descriptor. Returns reads per second.
pub(crate) fn ruio(load: &Load) -> io::Result<f64> {
    sys::pin(&[load.cpu])?;
    let blocks = size(&load.path)? / load.block as u64;
    if blocks == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the file is smaller than a block",
        ));
    }
    let entries = u32::try_from(load.depth.next_power_of_two().max(8)).unwrap_or(1 << 15);
    let runtime = ruio::Runtime::builder().entries(entries).build()?;
    let load = load.clone();
    runtime.block_on(async move {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .direct_descriptor(true)
            .open(&load.path)
            .await?;
        let file = Rc::new(file);
        let depth = u16::try_from(load.depth)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "at most 65535 reads"))?;
        let buffers = FixedBuffers::new(&ruio::driver(), depth, load.block)?;
        let done = Rc::new(Cell::new(0_u64));
        let stop = Rc::new(Cell::new(false));
        let failed: Rc<Cell<Option<io::ErrorKind>>> = Rc::new(Cell::new(None));
        for index in 0..depth {
            let mut buf = buffers.check_out(index).expect("one buffer per reader");
            let (file, done, stop, failed) =
                (file.clone(), done.clone(), stop.clone(), failed.clone());
            let block = load.block as u64;
            let mut rng =
                Rng(0x9e37_79b9_7f4a_7c15
                    ^ (u64::from(index) + 1).wrapping_mul(0xbf58_476d_1ce4_e5b9));
            let _detached = ruio::spawn(async move {
                while !stop.get() {
                    let offset = rng.below(blocks) * block;
                    let (read, back) = file.read_fixed_at(buf, offset).await;
                    buf = back;
                    match read {
                        Ok(_) => done.set(done.get() + 1),
                        Err(err) => {
                            failed.set(Some(err.kind()));
                            return;
                        }
                    }
                }
            });
        }
        sleep(load.warmup).await;
        let (from, start) = (done.get(), Instant::now());
        sleep(load.measure).await;
        let (count, elapsed) = (done.get() - from, start.elapsed());
        stop.set(true);
        if let Some(kind) = failed.get() {
            return Err(io::Error::new(kind, "a read failed"));
        }
        Ok(count as f64 / elapsed.as_secs_f64())
    })
}

/// A plain buffer for direct I/O, to check that `path` takes it.
pub(crate) fn check_direct_io(path: &Path, block: usize) -> io::Result<()> {
    let runtime = ruio::Runtime::new()?;
    runtime.block_on(async {
        let file = File::options()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)
            .await?;
        let (read, _) = file
            .read_exact_at(AlignedBuf::with_capacity(block, 4096), 0)
            .await;
        read
    })
}

/// Reads with fio's io_uring engine, at its best: registered buffers and
/// files, as ruio's reader has; no latency statistics (`gtod_reduce`),
/// which ruio's does not gather either; and reads submitted in batches of up
/// to 32, where by default fio enters the kernel for every one. Returns
/// reads per second.
pub(crate) fn fio(fio: &str, load: &Load) -> Result<f64, String> {
    let secs = |duration: Duration| format!("{}ms", duration.as_millis());
    let output = Command::new(fio)
        .arg("--name=randread")
        .arg(format!("--filename={}", load.path.display()))
        .args(["--ioengine=io_uring", "--direct=1", "--rw=randread"])
        .args(["--fixedbufs=1", "--registerfiles=1", "--gtod_reduce=1"])
        .args(["--norandommap", "--randrepeat=0", "--numjobs=1"])
        // Nothing to drop from a cache for direct reads.
        .arg("--invalidate=0")
        .arg(format!("--bs={}", load.block))
        .arg(format!("--iodepth={}", load.depth))
        .arg(format!("--iodepth_batch_submit={}", load.depth.min(32)))
        .arg("--iodepth_batch_complete_min=1")
        .arg(format!("--iodepth_batch_complete_max={}", load.depth))
        .args(["--time_based=1", "--output-format=json"])
        .arg(format!("--ramp_time={}", secs(load.warmup)))
        .arg(format!("--runtime={}", secs(load.measure)))
        .arg(format!("--cpus_allowed={}", load.cpu))
        .output()
        .map_err(|err| format!("could not run {fio}: {err}"))?;
    if !output.status.success() {
        return Err(format!(
            "{fio} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let json = String::from_utf8_lossy(&output.stdout);
    iops(&json).ok_or_else(|| format!("no read IOPS in fio's report: {json}"))
}

/// The read IOPS in fio's JSON report, without a JSON parser: the first
/// `"iops"` of the job's `"read"` object.
fn iops(json: &str) -> Option<f64> {
    let read = json.find("\"read\"")?;
    let rest = &json[read..];
    let field = rest.find("\"iops\"")?;
    let value = rest[field + "\"iops\"".len()..]
        .trim_start()
        .strip_prefix(':')?;
    let end = value.find([',', '\n', '}']).unwrap_or(value.len());
    value[..end].trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fio_reports_iops_in_its_read_object() {
        let json = r#"{"jobs": [{"jobname": "randread", "read": {"io_bytes": 1, "iops" : 1234.5,
            "runtime": 1}, "write": {"iops": 0.0}}]}"#;
        assert_eq!(iops(json), Some(1234.5));
        assert_eq!(iops("{}"), None);
    }

    #[test]
    fn random_offsets_stay_below_the_bound() {
        let mut rng = Rng(42);
        let mut seen = [false; 16];
        for _ in 0..10_000 {
            seen[rng.below(16) as usize] = true;
        }
        assert!(seen.iter().all(|&seen| seen));
    }
}
