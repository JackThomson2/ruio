//! Benchmarks of ruio against other runtimes, as DESIGN.md describes them.
//!
//! `ruio-bench echo` runs the TCP echo benchmark over the loopback interface.
//! For each number of connections and message size, and for each server, it
//! starts the server in a process of its own, pinned to one CPU, and the load
//! generator in another, pinned to CPUs of their own. Each connection sends a
//! message and waits for its echo, again and again. The report gives round
//! trips per second, how that compares with tokio, and the 99th percentile
//! round-trip time, each the median of a few runs, which the servers take
//! in turns.
//!
//! The servers each run on one thread: ruio, with plain and with direct
//! descriptors, and tokio's current-thread runtime. Runtimes across cores
//! are milestone M6.
//!
//! `ruio-bench randread` compares random reads with direct I/O against fio's,
//! on a file or block device: for each queue depth, ruio's reader and fio's
//! io_uring engine each run in a process of their own on the same CPU, with
//! registered buffers and files, in turns; fio submits in batches, at its
//! best. The report gives reads per second
//! and how ruio's compare.
//!
//! The server and the load generator also run on their own:
//!
//! ```text
//! ruio-bench echo [--quick] [--runs N] [--seconds S] [--warmup S] [--server-cpu N]
//!                 [--client-cpus LIST]
//! ruio-bench echo-server ruio|ruio-direct|tokio ADDR [--cpu N] [--buffers N]
//! ruio-bench echo-client ADDR [--connections N] [--size BYTES] [--seconds S] [--warmup S]
//!                             [--cpus LIST]
//! ```

mod client;
mod randread;
mod server;
mod sys;

use std::env;
use std::fmt::Display;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::process::{self, Child, Command, Stdio};
use std::str::FromStr;
use std::thread;
use std::time::{Duration, Instant};

use client::Load;
use server::Server;

const USAGE: &str = "usage: ruio-bench echo [--quick] [--runs N] [--seconds S] [--warmup S] \
                     [--server-cpu N] [--client-cpus LIST]
       ruio-bench echo-server ruio|ruio-direct|tokio ADDR [--cpu N] [--buffers N]
       ruio-bench echo-client ADDR [--connections N] [--size BYTES] [--seconds S] \
                     [--warmup S] [--cpus LIST]
       ruio-bench randread PATH [--depths LIST] [--block BYTES] [--runs N] [--seconds S] \
                     [--warmup S] [--cpu N] [--fio FIO]";

fn main() {
    let mut args = env::args().skip(1);
    let command = args.next();
    let result = Options::parse(args).and_then(|options| match command.as_deref() {
        Some("echo") => echo(&options),
        Some("echo-server") => echo_server(&options),
        Some("echo-client") => echo_client(&options),
        Some("randread") => randread(&options),
        Some("randread-ruio") => randread_ruio(&options),
        _ => Err(USAGE.to_owned()),
    });
    if let Err(err) = result {
        eprintln!("ruio-bench: {err}");
        process::exit(2);
    }
}

/// Positional arguments, then `--name value` options and `--flag`s.
struct Options {
    positional: Vec<String>,
    named: Vec<(String, String)>,
    flags: Vec<String>,
}

impl Options {
    /// Options that take no value.
    const FLAGS: &[&str] = &["quick"];

    fn parse(args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut options = Self {
            positional: Vec::new(),
            named: Vec::new(),
            flags: Vec::new(),
        };
        let mut args = args.peekable();
        while let Some(arg) = args.next() {
            let Some(name) = arg.strip_prefix("--") else {
                options.positional.push(arg);
                continue;
            };
            if Self::FLAGS.contains(&name) {
                options.flags.push(name.to_owned());
            } else {
                let value = args
                    .next()
                    .ok_or_else(|| format!("--{name} needs a value"))?;
                options.named.push((name.to_owned(), value));
            }
        }
        Ok(options)
    }

    fn get<T: FromStr>(&self, name: &str, default: T) -> Result<T, String>
    where
        T::Err: Display,
    {
        match self.named.iter().rev().find(|(named, _)| named == name) {
            Some((_, value)) => value
                .parse()
                .map_err(|err| format!("--{name} {value}: {err}")),
            None => Ok(default),
        }
    }

    fn duration(&self, name: &str, default: f64) -> Result<Duration, String> {
        let seconds = self.get(name, default)?;
        Duration::try_from_secs_f64(seconds).map_err(|err| format!("--{name}: {err}"))
    }

    fn flag(&self, name: &str) -> bool {
        self.flags.iter().any(|flag| flag == name)
    }

    fn positional<T: FromStr>(&self, index: usize, what: &str) -> Result<T, String>
    where
        T::Err: Display,
    {
        let value = self
            .positional
            .get(index)
            .ok_or_else(|| format!("missing {what}\n{USAGE}"))?;
        value
            .parse()
            .map_err(|err| format!("{what} {value}: {err}"))
    }
}

/// The load of a random-read run, from the options.
fn read_load(options: &Options, depth: usize) -> Result<randread::Load, String> {
    Ok(randread::Load {
        path: options.positional::<String>(0, "path")?.into(),
        depth,
        block: options.get("block", 4096)?,
        warmup: options.duration("warmup", 1.0)?,
        measure: options.duration("seconds", 5.0)?,
        cpu: options.get("cpu", 0)?,
    })
}

/// Compares ruio's random reads with fio's, each in a process of its own on
/// the same CPU, run by run in turns, for each queue depth.
fn randread(options: &Options) -> Result<(), String> {
    let exe = env::current_exe().map_err(|err| err.to_string())?;
    let fio: String = options.get("fio", "fio".to_owned())?;
    let runs: usize = options.get("runs", 3)?;
    let depths: String = options.get("depths", "1,4,16,32,64,128".to_owned())?;
    let depths = sys::parse_cpus(&depths)?;
    let load = read_load(options, 1)?;
    randread::check_direct_io(&load.path, load.block)
        .map_err(|err| format!("{} does not take direct I/O: {err}", load.path.display()))?;
    println!(
        "{} B random reads of {}, direct, on CPU {}: reads per second, medians of {runs} runs.",
        load.block,
        load.path.display(),
        load.cpu
    );
    println!();
    println!("| depth | fio | ruio |");
    println!("|---:|---:|---:|");
    for depth in depths {
        let load = read_load(options, depth)?;
        let (mut fio_runs, mut ruio_runs) = (Vec::new(), Vec::new());
        for round in 1..=runs {
            eprintln!("depth {depth}, run {round}");
            fio_runs.push(randread::fio(&fio, &load)?);
            ruio_runs.push(read_child(&exe, &load)?);
        }
        let (fio, ruio) = (median(fio_runs.into_iter()), median(ruio_runs.into_iter()));
        println!(
            "| {depth} | {} | {} ({:.2}x) |",
            thousands(fio),
            thousands(ruio),
            ruio / fio
        );
    }
    Ok(())
}

/// Runs ruio's reader in a process of its own, and returns its rate.
fn read_child(exe: &Path, load: &randread::Load) -> Result<f64, String> {
    let output = Command::new(exe)
        .arg("randread-ruio")
        .arg(&load.path)
        .args(["--depth", &load.depth.to_string()])
        .args(["--block", &load.block.to_string()])
        .args(["--warmup", &load.warmup.as_secs_f64().to_string()])
        .args(["--seconds", &load.measure.as_secs_f64().to_string()])
        .args(["--cpu", &load.cpu.to_string()])
        .output()
        .map_err(|err| format!("could not run the reader: {err}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .trim()
        .strip_prefix("rate=")
        .and_then(|rate| rate.parse().ok())
        .ok_or_else(|| {
            format!(
                "the reader failed: {}{}",
                stdout,
                String::from_utf8_lossy(&output.stderr)
            )
        })
}

fn randread_ruio(options: &Options) -> Result<(), String> {
    let load = read_load(options, options.get("depth", 32)?)?;
    let rate = randread::ruio(&load).map_err(|err| err.to_string())?;
    println!("rate={rate:.0}");
    Ok(())
}

fn echo_server(options: &Options) -> Result<(), String> {
    let server: Server = options.positional(0, "server")?;
    let addr: SocketAddr = options.positional(1, "address")?;
    let cpu = options.get("cpu", 0)?;
    let buffers = options.get("buffers", 1024)?;
    sys::pin(&[cpu]).map_err(|err| format!("pinning to CPU {cpu}: {err}"))?;
    sys::raise_file_limit().map_err(|err| format!("raising the file limit: {err}"))?;
    server::run(server, addr, buffers).map_err(|err| format!("{server} echo server: {err}"))
}

fn echo_client(options: &Options) -> Result<(), String> {
    let addr: SocketAddr = options.positional(0, "address")?;
    let connections: usize = options.get("connections", 1)?;
    let cpus = sys::parse_cpus(&options.get("cpus", "2-25".to_owned())?)?;
    let load = Load {
        connections,
        size: options.get("size", 64)?,
        warmup: options.duration("warmup", 1.0)?,
        measure: options.duration("seconds", 3.0)?,
        threads: cpus.len().min(connections).max(1),
    };
    sys::pin(&cpus).map_err(|err| format!("pinning to CPUs {cpus:?}: {err}"))?;
    sys::raise_file_limit().map_err(|err| format!("raising the file limit: {err}"))?;
    let measured = client::run(addr, load).map_err(|err| format!("echo client: {err}"))?;
    println!(
        "rate={:.0} p50={:.1} p99={:.1}",
        measured.rate(),
        measured.percentile(0.5),
        measured.percentile(0.99)
    );
    Ok(())
}

/// The connection counts and message sizes of DESIGN.md.
const MATRIX: &[(usize, usize)] = &[
    (1, 64),
    (1, 1024),
    (1, 16 << 10),
    (1, 64 << 10),
    (16, 64),
    (16, 1024),
    (16, 16 << 10),
    (16, 64 << 10),
    (256, 64),
    (256, 1024),
    (256, 16 << 10),
    (256, 64 << 10),
    (1024, 64),
    (1024, 1024),
    (1024, 16 << 10),
    (1024, 64 << 10),
    (10_000, 64),
    (10_000, 1024),
    (10_000, 16 << 10),
    (10_000, 64 << 10),
];

const QUICK: &[(usize, usize)] = &[(1, 64), (256, 1024), (1024, 16 << 10)];

/// One server's result for one load.
struct Row {
    rate: f64,
    p99: f64,
}

fn echo(options: &Options) -> Result<(), String> {
    let exe = env::current_exe().map_err(|err| err.to_string())?;
    let matrix = if options.flag("quick") { QUICK } else { MATRIX };
    let server_cpu: usize = options.get("server-cpu", 0)?;
    let client_cpus: String = options.get("client-cpus", "2-25".to_owned())?;
    let seconds = options.duration("seconds", 3.0)?;
    let warmup = options.duration("warmup", 1.0)?;
    let runs: usize = options.get("runs", 3)?;
    if runs == 0 {
        return Err("--runs must be at least 1".to_owned());
    }

    println!(
        "Round trips per second, and 99th percentile round-trip time, medians of {runs} \
         runs; servers on CPU {server_cpu}, the client on {client_cpus}."
    );
    println!();
    print!("| connections | message |");
    for server in Server::ALL {
        print!(" {server} |");
    }
    println!();
    println!("|---:|---:|{}", "---:|".repeat(Server::ALL.len()));
    for &(connections, size) in matrix {
        // The servers take turns, run after run, so that whatever else the
        // host does falls on all of them alike.
        let mut samples: Vec<Vec<Row>> = Server::ALL.iter().map(|_| Vec::new()).collect();
        for round in 1..=runs {
            for (server, samples) in Server::ALL.into_iter().zip(&mut samples) {
                eprintln!("{server}: {connections} connections, {size} B messages, run {round}");
                let run = Run {
                    exe: &exe,
                    server,
                    server_cpu,
                    client_cpus: &client_cpus,
                    connections,
                    size,
                    seconds,
                    warmup,
                };
                samples.push(run.measure()?);
            }
        }
        let rows: Vec<Row> = samples
            .iter()
            .map(|samples| Row {
                rate: median(samples.iter().map(|row| row.rate)),
                p99: median(samples.iter().map(|row| row.p99)),
            })
            .collect();
        let tokio = rows[0].rate;
        print!("| {connections} | {} |", bytes(size));
        for (server, row) in Server::ALL.iter().zip(&rows) {
            let versus = if *server == Server::Tokio {
                String::new()
            } else {
                format!(", {:.2}x", row.rate / tokio)
            };
            print!(" {}{versus}, {:.0} µs |", thousands(row.rate), row.p99);
        }
        println!();
    }
    Ok(())
}

/// One measurement: a server in a process of its own, and the client in
/// another.
struct Run<'a> {
    exe: &'a Path,
    server: Server,
    server_cpu: usize,
    client_cpus: &'a str,
    connections: usize,
    size: usize,
    seconds: Duration,
    warmup: Duration,
}

impl Run<'_> {
    fn measure(&self) -> Result<Row, String> {
        let addr = free_addr().map_err(|err| format!("finding a free port: {err}"))?;
        let mut server = Command::new(self.exe)
            .arg("echo-server")
            .arg(self.server.to_string())
            .arg(addr.to_string())
            .args(["--cpu", &self.server_cpu.to_string()])
            .args(["--buffers", &self.pool_buffers().to_string()])
            .stdin(Stdio::null())
            .spawn()
            .map_err(|err| format!("starting the {} server: {err}", self.server))?;
        let measured = self.client(addr, &mut server);
        let _ = server.kill();
        let _ = server.wait();
        measured
    }

    fn client(&self, addr: SocketAddr, server: &mut Child) -> Result<Row, String> {
        wait_until_listening(addr, server)?;
        let output = Command::new(self.exe)
            .arg("echo-client")
            .arg(addr.to_string())
            .args(["--connections", &self.connections.to_string()])
            .args(["--size", &self.size.to_string()])
            .args(["--seconds", &self.seconds.as_secs_f64().to_string()])
            .args(["--warmup", &self.warmup.as_secs_f64().to_string()])
            .args(["--cpus", self.client_cpus])
            .stdin(Stdio::null())
            .output()
            .map_err(|err| format!("running the client: {err}"))?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        if !output.status.success() {
            return Err(format!(
                "the client failed against {}: {}",
                self.server,
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        let field = |name: &str| -> Result<f64, String> {
            stdout
                .split_whitespace()
                .find_map(|field| field.strip_prefix(name)?.strip_prefix('='))
                .and_then(|value| value.parse().ok())
                .ok_or_else(|| format!("no {name} in the client's report {stdout:?}"))
        };
        Ok(Row {
            rate: field("rate")?,
            p99: field("p99")?,
        })
    }

    /// A buffer per connection, as tokio's server has, within what a pool
    /// can have.
    ///
    /// That is enough whatever the size of the messages: the server writes
    /// its echoes behind, so a buffer goes back once the kernel has sent its
    /// data, and data a pool has no room for yet waits in the socket. More
    /// costs throughput: the kernel takes buffers from a pool's ring in the
    /// order they went back, so every buffer of the pool is in use in turn,
    /// and a pool larger than its load spreads the copies over more memory
    /// than the cache holds. With 1024 connections and 64 KiB messages, 8192
    /// buffers where 1024 do cost a fifth of the throughput.
    fn pool_buffers(&self) -> u16 {
        self.connections
            .clamp(64, 1 << 15)
            .next_power_of_two()
            .min(1 << 15) as u16
    }
}

/// The median of some measurements.
fn median(values: impl Iterator<Item = f64>) -> f64 {
    let mut values: Vec<f64> = values.collect();
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    if values.len() % 2 == 1 {
        values[middle]
    } else {
        (values[middle - 1] + values[middle]) / 2.0
    }
}

/// A port on the loopback interface that was free a moment ago.
fn free_addr() -> std::io::Result<SocketAddr> {
    TcpListener::bind("127.0.0.1:0")?.local_addr()
}

/// Waits until the server accepts connections, or has died.
fn wait_until_listening(addr: SocketAddr, server: &mut Child) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if TcpStream::connect(addr).is_ok() {
            return Ok(());
        }
        if let Ok(Some(status)) = server.try_wait() {
            return Err(format!("the server exited with {status}"));
        }
        if Instant::now() > deadline {
            return Err("the server did not start listening".to_owned());
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn bytes(size: usize) -> String {
    if size >= 1024 && size.is_multiple_of(1024) {
        format!("{} KiB", size / 1024)
    } else {
        format!("{size} B")
    }
}

#[cfg(test)]
#[test]
fn medians_take_the_middle() {
    assert_eq!(median([3.0, 1.0, 2.0].into_iter()), 2.0);
    assert_eq!(median([4.0, 1.0, 3.0, 2.0].into_iter()), 2.5);
}

fn thousands(rate: f64) -> String {
    if rate >= 1e6 {
        format!("{:.2}M", rate / 1e6)
    } else {
        format!("{:.0}k", rate / 1e3)
    }
}
