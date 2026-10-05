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
//! descriptors, and tokio's current-thread runtime.
//!
//! `ruio-bench http` runs the HTTP/1.1 benchmark the same way: hyper
//! serving a short plaintext response on one thread, on ruio through its
//! adapters, each connection receiving into a buffer of its own or into a
//! shared pool, and on tokio's current-thread runtime; and connections that
//! each send a request and wait for the response, again and again.
//!
//! `ruio-bench scale` measures how a runtime scales across cores: for each
//! number of cores, every core loads itself, with an echo server and
//! connections of its own, and the report gives the round trips per second
//! of the cores together, and each core's share of them against the first
//! row's, for ruio on its cores and for tokio with a current-thread runtime
//! per core, each run in a process of its own, in turns.
//!
//! `ruio-bench micro` measures the runtime itself, apart from the kernel: what
//! a turn of the loop, a task, a wake, a timer or a channel costs, as steps
//! per second of a loop of one small step each, on ruio and, where tokio has
//! the same thing, on its current-thread runtime (see [`micro`](mod@micro)).
//!
//! `ruio-bench randread` compares random reads with direct I/O against fio's,
//! on a file or block device: for each queue depth, ruio's reader and fio's
//! io_uring engine each run in a process of their own on the same CPU, with
//! registered buffers and files, in turns; fio submits in batches, at its
//! best. With `--polled` both poll the device for completions rather than
//! wait for its interrupts: ruio's reader on its polled ring, fio with
//! `hipri`. The report gives reads per second and how ruio's compare.
//!
//! The server and the load generator also run on their own:
//!
//! ```text
//! ruio-bench echo [--quick] [--connections LIST] [--runs N] [--seconds S] [--warmup S]
//!                 [--server-cpu N] [--client-cpus LIST] [--spin US]
//! ruio-bench echo-server ruio|ruio-direct|tokio ADDR [--cpu N] [--buffers N] [--spin US]
//! ruio-bench echo-client ADDR [--connections N] [--size BYTES] [--seconds S] [--warmup S]
//!                             [--cpus LIST]
//! ruio-bench http-server ruio|ruio-pool|tokio ADDR [--cpu N] [--buffers N]
//! ruio-bench http-client ADDR [--connections N] [--seconds S] [--warmup S] [--cpus LIST]
//! ruio-bench scale-run ruio|tokio [--cores N] [--connections N] [--size BYTES] [--seconds S]
//!                                 [--warmup S]
//! ruio-bench micro [--only LIST] [--runs N] [--seconds S] [--cpu N]
//! ```

mod client;
mod http;
mod micro;
mod randread;
mod scale;
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

use client::{Exchange, Load};
use http::HttpServer;
use server::Server;

const USAGE: &str =
    "usage: ruio-bench echo [--quick] [--connections LIST] [--runs N] [--seconds S] \
                     [--warmup S] [--server-cpu N] [--client-cpus LIST] [--spin US]
       ruio-bench echo-server ruio|ruio-direct|tokio ADDR [--cpu N] [--buffers N] [--spin US]
       ruio-bench echo-client ADDR [--connections N] [--size BYTES] [--seconds S] \
                     [--warmup S] [--cpus LIST]
       ruio-bench http [--quick] [--connections LIST] [--runs N] [--seconds S] [--warmup S] \
                     [--server-cpu N] [--client-cpus LIST]
       ruio-bench http-server ruio|ruio-pool|tokio ADDR [--cpu N] [--buffers N]
       ruio-bench http-client ADDR [--connections N] [--seconds S] [--warmup S] [--cpus LIST]
       ruio-bench randread PATH [--polled] [--depths LIST] [--block BYTES] [--runs N] \
                     [--seconds S] [--warmup S] [--cpu N] [--fio FIO]
       ruio-bench scale [--cores LIST] [--connections N] [--size BYTES] [--runs N] \
                     [--seconds S] [--warmup S]
       ruio-bench scale-run ruio|tokio [--cores N] [--connections N] [--size BYTES] \
                     [--seconds S] [--warmup S]
       ruio-bench micro [--only LIST] [--runs N] [--seconds S] [--cpu N]";

fn main() {
    let mut args = env::args().skip(1);
    let command = args.next();
    let result = Options::parse(args).and_then(|options| match command.as_deref() {
        Some("echo") => echo(&options),
        Some("echo-server") => echo_server(&options),
        Some("echo-client") => echo_client(&options),
        Some("http") => http(&options),
        Some("http-server") => http_server(&options),
        Some("http-client") => http_client(&options),
        Some("randread") => randread(&options),
        Some("randread-ruio") => randread_ruio(&options),
        Some("scale") => scale(&options),
        Some("scale-run") => scale_run(&options),
        Some("micro") => micro(&options),
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
    const FLAGS: &[&str] = &["quick", "polled"];

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
        polled: options.flag("polled"),
    })
}

/// Compares ruio's random reads with fio's, each in a process of its own on
/// the same CPU, run by run in turns, for each queue depth; polled on both
/// sides with `--polled`.
fn randread(options: &Options) -> Result<(), String> {
    let exe = env::current_exe().map_err(|err| err.to_string())?;
    let fio: String = options.get("fio", "fio".to_owned())?;
    let runs: usize = options.get("runs", 3)?;
    let depths: String = options.get("depths", "1,4,16,32,64,128".to_owned())?;
    let depths = sys::parse_cpus(&depths)?;
    let load = read_load(options, 1)?;
    randread::check_direct_io(&load.path, load.block)
        .map_err(|err| format!("{} does not take direct I/O: {err}", load.path.display()))?;
    let (how, fio_column, ruio_column) = if load.polled {
        ("direct and polled", "fio --hipri", "ruio polled")
    } else {
        ("direct", "fio", "ruio")
    };
    println!(
        "{} B random reads of {}, {how}, on CPU {}: reads per second, medians of {runs} runs.",
        load.block,
        load.path.display(),
        load.cpu
    );
    println!();
    println!("| depth | {fio_column} | {ruio_column} |");
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
    let mut reader = Command::new(exe);
    reader
        .arg("randread-ruio")
        .arg(&load.path)
        .args(["--depth", &load.depth.to_string()])
        .args(["--block", &load.block.to_string()])
        .args(["--warmup", &load.warmup.as_secs_f64().to_string()])
        .args(["--seconds", &load.measure.as_secs_f64().to_string()])
        .args(["--cpu", &load.cpu.to_string()]);
    if load.polled {
        reader.arg("--polled");
    }
    let output = reader
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

/// The load of a scaling run on `cores` cores, from the options.
fn scale_load(options: &Options, cores: usize) -> Result<scale::Load, String> {
    Ok(scale::Load {
        cores,
        connections: options.get("connections", 32)?,
        size: options.get("size", 64)?,
        warmup: options.duration("warmup", 1.0)?,
        measure: options.duration("seconds", 3.0)?,
    })
}

/// Measures ruio and tokio on more and more cores, each run in a process of
/// its own, in turns.
fn scale(options: &Options) -> Result<(), String> {
    let exe = env::current_exe().map_err(|err| err.to_string())?;
    let counts: String = options.get("cores", "1,2,4,8,16,24,32,48,64,96".to_owned())?;
    let counts = sys::parse_cpus(&counts)?;
    let runs: usize = options.get("runs", 3)?;
    if runs == 0 {
        return Err("--runs must be at least 1".to_owned());
    }
    let available = ruio::Cores::all()
        .map_err(|err| format!("finding the CPUs: {err}"))?
        .count();
    let load = scale_load(options, 1)?;
    println!(
        "Round trips per second over loopback, every core with an echo server and {} \
         connections of its own, {} B messages; medians of {runs} runs. Scaling is each \
         row's rate per core, against the first row's.",
        load.connections, load.size
    );
    println!();
    println!("| cores | tokio | scaling | ruio | scaling |");
    println!("|---:|---:|---:|---:|---:|");
    let mut first: Option<(f64, f64)> = None;
    for cores in counts {
        if cores == 0 || cores > available {
            eprintln!("skipping {cores} cores: the process may run on {available} CPUs");
            continue;
        }
        let load = scale_load(options, cores)?;
        let (mut tokio_runs, mut ruio_runs) = (Vec::new(), Vec::new());
        for round in 1..=runs {
            eprintln!("{cores} cores, run {round}");
            tokio_runs.push(scale_child(&exe, "tokio", &load)?);
            ruio_runs.push(scale_child(&exe, "ruio", &load)?);
        }
        let (tokio, ruio) = (
            median(tokio_runs.into_iter()),
            median(ruio_runs.into_iter()),
        );
        let per_core = |rate: f64| rate / cores as f64;
        let (tokio_one, ruio_one) = *first.get_or_insert((per_core(tokio), per_core(ruio)));
        println!(
            "| {cores} | {} | {:.0}% | {} ({:.2}x) | {:.0}% |",
            thousands(tokio),
            100.0 * per_core(tokio) / tokio_one,
            thousands(ruio),
            ruio / tokio,
            100.0 * per_core(ruio) / ruio_one,
        );
    }
    Ok(())
}

/// Runs one scaling run in a process of its own, and returns its rate.
fn scale_child(exe: &Path, runtime: &str, load: &scale::Load) -> Result<f64, String> {
    let output = Command::new(exe)
        .args(["scale-run", runtime])
        .args(["--cores", &load.cores.to_string()])
        .args(["--connections", &load.connections.to_string()])
        .args(["--size", &load.size.to_string()])
        .args(["--warmup", &load.warmup.as_secs_f64().to_string()])
        .args(["--seconds", &load.measure.as_secs_f64().to_string()])
        .stdin(Stdio::null())
        .output()
        .map_err(|err| format!("could not run {runtime} on {} cores: {err}", load.cores))?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .trim()
        .strip_prefix("rate=")
        .and_then(|rate| rate.parse().ok())
        .ok_or_else(|| {
            format!(
                "{runtime} on {} cores failed: {}{}",
                load.cores,
                stdout,
                String::from_utf8_lossy(&output.stderr)
            )
        })
}

fn scale_run(options: &Options) -> Result<(), String> {
    let runtime: String = options.positional(0, "runtime")?;
    let load = scale_load(options, options.get("cores", 1)?)?;
    sys::raise_file_limit().map_err(|err| format!("raising the file limit: {err}"))?;
    let rate = match runtime.as_str() {
        "ruio" => scale::ruio(load),
        "tokio" => scale::tokio(load),
        _ => return Err(format!("unknown runtime {runtime:?}; try ruio or tokio")),
    };
    let rate = rate.map_err(|err| format!("{runtime} on {} cores: {err}", load.cores))?;
    println!("rate={rate:.0}");
    Ok(())
}

/// Measures the runtime's own overheads, each the median of a few runs, which
/// the runtimes take in turns; see [`micro`](mod@micro).
fn micro(options: &Options) -> Result<(), String> {
    let runs: usize = options.get("runs", 3)?;
    if runs == 0 {
        return Err("--runs must be at least 1".to_owned());
    }
    let only: String = options.get("only", String::new())?;
    let wanted: Vec<&str> = only.split(',').filter(|name| !name.is_empty()).collect();
    let load = micro::Load {
        measure: options.duration("seconds", 1.0)?,
        cpu: options.get("cpu", 0)?,
    };
    sys::pin(&[load.cpu]).map_err(|err| format!("pinning to CPU {}: {err}", load.cpu))?;
    let benchmarks = micro::ALL
        .iter()
        .filter(|benchmark| wanted.is_empty() || wanted.contains(&benchmark.name));

    println!(
        "Steps per second of the runtime's own loops, on CPU {}, medians of {runs} runs of \
         {:.1} s; see the module documentation of ruio-bench's micro.rs for what each step is.",
        load.cpu,
        load.measure.as_secs_f64()
    );
    println!();
    print!("| benchmark |");
    for runtime in micro::Runtime::ALL {
        print!(" {runtime} |");
    }
    println!();
    println!("|---|{}", "---:|".repeat(micro::Runtime::ALL.len()));
    for benchmark in benchmarks {
        let mut samples: Vec<Vec<f64>> = micro::Runtime::ALL.iter().map(|_| Vec::new()).collect();
        for round in 1..=runs {
            for (runtime, samples) in micro::Runtime::ALL.into_iter().zip(&mut samples) {
                let Some(measured) = benchmark.run(runtime, &load) else {
                    continue;
                };
                eprintln!("{}: {runtime}, run {round}", benchmark.name);
                samples.push(
                    measured.map_err(|err| format!("{} on {runtime}: {err}", benchmark.name))?,
                );
            }
        }
        let rates: Vec<Option<f64>> = samples
            .iter()
            .map(|samples| (!samples.is_empty()).then(|| median(samples.iter().copied())))
            .collect();
        print!("| {} |", benchmark.name);
        for (runtime, rate) in micro::Runtime::ALL.iter().zip(&rates) {
            match rate {
                None => print!(" - |"),
                Some(rate) if *runtime == micro::Runtime::Ruio && rates[0].is_some() => {
                    print!(
                        " {} ({:.2}x) |",
                        thousands(*rate),
                        rate / rates[0].unwrap_or(1.0)
                    );
                }
                Some(rate) => print!(" {} |", thousands(*rate)),
            }
        }
        println!();
    }
    Ok(())
}

fn echo_server(options: &Options) -> Result<(), String> {
    let server: Server = options.positional(0, "server")?;
    let addr: SocketAddr = options.positional(1, "address")?;
    let cpu = options.get("cpu", 0)?;
    let buffers = options.get("buffers", 1024)?;
    let spin = Duration::from_micros(options.get("spin", 0)?);
    sys::pin(&[cpu]).map_err(|err| format!("pinning to CPU {cpu}: {err}"))?;
    sys::raise_file_limit().map_err(|err| format!("raising the file limit: {err}"))?;
    server::run(server, addr, buffers, spin).map_err(|err| format!("{server} echo server: {err}"))
}

fn echo_client(options: &Options) -> Result<(), String> {
    let size = options.get("size", 64)?;
    run_client(options, Exchange::Echo(size))
}

fn http_server(options: &Options) -> Result<(), String> {
    let server: HttpServer = options.positional(0, "server")?;
    let addr: SocketAddr = options.positional(1, "address")?;
    let cpu = options.get("cpu", 0)?;
    let buffers = options.get("buffers", 1024)?;
    sys::pin(&[cpu]).map_err(|err| format!("pinning to CPU {cpu}: {err}"))?;
    sys::raise_file_limit().map_err(|err| format!("raising the file limit: {err}"))?;
    http::run(server, addr, buffers).map_err(|err| format!("{server} HTTP server: {err}"))
}

fn http_client(options: &Options) -> Result<(), String> {
    run_client(options, Exchange::Http)
}

/// Runs the load generator, as the options say, making `exchange` on each
/// connection, and reports what it measured.
fn run_client(options: &Options, exchange: Exchange) -> Result<(), String> {
    let addr: SocketAddr = options.positional(0, "address")?;
    let connections: usize = options.get("connections", 1)?;
    let cpus = sys::parse_cpus(&options.get("cpus", "2-25".to_owned())?)?;
    let load = Load {
        connections,
        exchange,
        warmup: options.duration("warmup", 1.0)?,
        measure: options.duration("seconds", 3.0)?,
        threads: cpus.len().min(connections).max(1),
    };
    sys::pin(&cpus).map_err(|err| format!("pinning to CPUs {cpus:?}: {err}"))?;
    sys::raise_file_limit().map_err(|err| format!("raising the file limit: {err}"))?;
    let measured = client::run(addr, load).map_err(|err| format!("client: {err}"))?;
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

/// The connection counts `--connections` picks out of a benchmark's matrix,
/// for running part of it again; every one if it is not given.
fn connections_wanted(options: &Options) -> Result<Option<Vec<usize>>, String> {
    let list: String = options.get("connections", String::new())?;
    if list.is_empty() {
        Ok(None)
    } else {
        sys::parse_cpus(&list).map(Some)
    }
}

fn echo(options: &Options) -> Result<(), String> {
    let exe = env::current_exe().map_err(|err| err.to_string())?;
    let wanted = connections_wanted(options)?;
    let matrix: Vec<(usize, usize)> = if options.flag("quick") { QUICK } else { MATRIX }
        .iter()
        .copied()
        .filter(|(connections, _)| {
            wanted
                .as_ref()
                .is_none_or(|wanted| wanted.contains(connections))
        })
        .collect();
    let server_cpu: usize = options.get("server-cpu", 0)?;
    let client_cpus: String = options.get("client-cpus", "2-25".to_owned())?;
    let seconds = options.duration("seconds", 3.0)?;
    let warmup = options.duration("warmup", 1.0)?;
    let runs: usize = options.get("runs", 3)?;
    if runs == 0 {
        return Err("--runs must be at least 1".to_owned());
    }
    let spin: u64 = options.get("spin", 0)?;

    println!(
        "Round trips per second, and 99th percentile round-trip time, medians of {runs} \
         runs; servers on CPU {server_cpu}, the client on {client_cpus}."
    );
    if spin != 0 {
        println!("The ruio servers spin for up to {spin} µs before they park.");
    }
    println!();
    print!("| connections | message |");
    for server in Server::ALL {
        print!(" {server} |");
    }
    println!();
    println!("|---:|---:|{}", "---:|".repeat(Server::ALL.len()));
    for (connections, size) in matrix {
        // The servers take turns, run after run, so that whatever else the
        // host does falls on all of them alike.
        let mut samples: Vec<Vec<Row>> = Server::ALL.iter().map(|_| Vec::new()).collect();
        for round in 1..=runs {
            for (server, samples) in Server::ALL.into_iter().zip(&mut samples) {
                eprintln!("{server}: {connections} connections, {size} B messages, run {round}");
                let run = Run {
                    exe: &exe,
                    benchmark: "echo",
                    server: server.to_string(),
                    server_cpu,
                    client_cpus: &client_cpus,
                    connections,
                    size,
                    seconds,
                    warmup,
                    spin,
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

/// The connection counts of the HTTP benchmark.
const HTTP_CONNECTIONS: &[usize] = &[1, 16, 256, 1024, 10_000];

const HTTP_QUICK: &[usize] = &[1, 256];

fn http(options: &Options) -> Result<(), String> {
    let exe = env::current_exe().map_err(|err| err.to_string())?;
    let wanted = connections_wanted(options)?;
    let counts: Vec<usize> = if options.flag("quick") {
        HTTP_QUICK
    } else {
        HTTP_CONNECTIONS
    }
    .iter()
    .copied()
    .filter(|connections| {
        wanted
            .as_ref()
            .is_none_or(|wanted| wanted.contains(connections))
    })
    .collect();
    let server_cpu: usize = options.get("server-cpu", 0)?;
    let client_cpus: String = options.get("client-cpus", "2-25".to_owned())?;
    let seconds = options.duration("seconds", 3.0)?;
    let warmup = options.duration("warmup", 1.0)?;
    let runs: usize = options.get("runs", 3)?;
    if runs == 0 {
        return Err("--runs must be at least 1".to_owned());
    }

    println!(
        "Requests per second, and 99th percentile response time, medians of {runs} runs; \
         hyper servers on CPU {server_cpu}, the client on {client_cpus}."
    );
    println!();
    print!("| connections |");
    for server in HttpServer::ALL {
        print!(" {server} |");
    }
    println!();
    println!("|---:|{}", "---:|".repeat(HttpServer::ALL.len()));
    for connections in counts {
        let mut samples: Vec<Vec<Row>> = HttpServer::ALL.iter().map(|_| Vec::new()).collect();
        for round in 1..=runs {
            for (server, samples) in HttpServer::ALL.into_iter().zip(&mut samples) {
                eprintln!("{server}: {connections} connections, run {round}");
                let run = Run {
                    exe: &exe,
                    benchmark: "http",
                    server: server.to_string(),
                    server_cpu,
                    client_cpus: &client_cpus,
                    connections,
                    size: 0,
                    seconds,
                    warmup,
                    spin: 0,
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
        print!("| {connections} |");
        for (server, row) in HttpServer::ALL.iter().zip(&rows) {
            let versus = if *server == HttpServer::Tokio {
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
    /// `echo` or `http`: what the server and client subcommands start with.
    benchmark: &'static str,
    server: String,
    server_cpu: usize,
    client_cpus: &'a str,
    connections: usize,
    size: usize,
    seconds: Duration,
    warmup: Duration,
    /// How long a ruio echo server spins before it parks, in microseconds;
    /// 0 parks at once, as tokio's does.
    spin: u64,
}

impl Run<'_> {
    fn measure(&self) -> Result<Row, String> {
        let addr = free_addr().map_err(|err| format!("finding a free port: {err}"))?;
        let mut server = Command::new(self.exe);
        server
            .arg(format!("{}-server", self.benchmark))
            .arg(&self.server)
            .arg(addr.to_string())
            .args(["--cpu", &self.server_cpu.to_string()])
            .args(["--buffers", &self.pool_buffers().to_string()]);
        if self.benchmark == "echo" && self.spin != 0 {
            server.args(["--spin", &self.spin.to_string()]);
        }
        let mut server = server
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
            .arg(format!("{}-client", self.benchmark))
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

    /// A buffer per connection, as tokio's server has, up to 4096.
    ///
    /// A pool holds only the data in flight: the server writes its echoes
    /// behind, so a buffer goes back once the kernel has sent its data, and
    /// data a pool has no room for yet waits in the socket. So a buffer per
    /// connection is more than enough, and more than the load needs costs
    /// throughput: the kernel takes buffers from a pool's ring in the order
    /// they went back, so every buffer of the pool is in use in turn, and a
    /// pool larger than its load spreads the copies over more memory than
    /// the cache holds. With 1024 connections and 64 KiB messages, 8192
    /// buffers where 1024 do cost a fifth of the throughput; with 10,000
    /// connections, 4096 buffers (64 MiB) beat 16,384 by a tenth with 64 KiB
    /// messages, where 2048 starve: the sends to that many clients hold
    /// more than that while the clients' acknowledgements are on their way.
    fn pool_buffers(&self) -> u16 {
        self.connections.clamp(64, 4096).next_power_of_two() as u16
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
