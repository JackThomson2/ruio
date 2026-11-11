//! Times what an engine does for its timers and its I/O, through the API
//! that every engine with I/O shares: how long a sleep of a millisecond
//! takes, round trips and bytes a second over a loopback connection, a file
//! written and read back, and tasks yielding to each other. Each figure is
//! the best of a few runs, all on one thread.
//!
//! Run it with `cargo run --release -p ruio --example speed`.

#[cfg(any(ruio_uring, ruio_iocp))]
mod timed {
    use std::future::Future;
    use std::io;
    use std::net::Shutdown;
    use std::time::{Duration, Instant};

    use ruio::buf::{BufPool, IoBuf};
    use ruio::fs::{self, File};
    use ruio::net::{TcpListener, TcpStream};

    /// How many times each measurement runs, of which the best counts.
    const RUNS: usize = 3;

    const KIB: usize = 1024;
    const MIB: usize = 1024 * KIB;

    /// How much a stream's reads and writes, and a file's, move at a time.
    const CHUNK: usize = 64 * KIB;

    /// A stream connected to a listener on the loopback interface, and the
    /// stream the listener accepted.
    async fn pair() -> io::Result<(TcpStream, TcpStream)> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let client = TcpStream::connect(listener.local_addr()?).await?;
        let (server, _) = listener.accept().await?;
        client.set_nodelay(true).await?;
        server.set_nodelay(true).await?;
        Ok((client, server))
    }

    /// How many MiB a second `bytes` in `elapsed` are.
    fn mib_a_second(bytes: usize, elapsed: Duration) -> f64 {
        bytes as f64 / MIB as f64 / elapsed.as_secs_f64()
    }

    /// The highest of what `run` measures in [`RUNS`] runs.
    async fn best<F, Fut>(run: F) -> io::Result<f64>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = io::Result<f64>>,
    {
        let mut best = 0.0_f64;
        for _ in 0..RUNS {
            best = best.max(run().await?);
        }
        Ok(best)
    }

    /// How long sleeps of a millisecond take: their mean, and the longest.
    async fn sleeps() -> (Duration, Duration) {
        const SLEEPS: u32 = 50;
        let mut worst = Duration::ZERO;
        let started = Instant::now();
        for _ in 0..SLEEPS {
            let before = Instant::now();
            ruio::time::sleep(Duration::from_millis(1)).await;
            worst = worst.max(before.elapsed());
        }
        (started.elapsed() / SLEEPS, worst)
    }

    /// How many times a second a message of 64 bytes goes to a task that
    /// echoes it and comes back.
    async fn round_trips() -> io::Result<f64> {
        const TRIPS: usize = 20_000;
        const LEN: usize = 64;
        let (client, server) = pair().await?;
        let echo = ruio::spawn(async move {
            let mut buf = Vec::with_capacity(LEN);
            // Until the client has gone.
            loop {
                let (read, filled) = server.read_exact(buf.slice(..LEN)).await;
                buf = filled.into_inner();
                if read.is_err() {
                    break;
                }
                let (written, sent) = server.write_all(buf).await;
                buf = sent;
                if written.is_err() {
                    break;
                }
            }
        });
        let mut buf = vec![7_u8; LEN];
        let started = Instant::now();
        for _ in 0..TRIPS {
            let (written, sent) = client.write_all(buf).await;
            written?;
            let (read, back) = client.read_exact(sent.slice(..LEN)).await;
            read?;
            buf = back.into_inner();
        }
        let elapsed = started.elapsed();
        drop(client);
        echo.await.map_err(io::Error::other)?;
        Ok(TRIPS as f64 / elapsed.as_secs_f64())
    }

    /// How many MiB a second go through a connection that one task writes
    /// and another reads, into a buffer of its own or into those of a pool.
    async fn stream(pooled: bool) -> io::Result<f64> {
        const TOTAL: usize = 256 * MIB;
        let (client, server) = pair().await?;
        let writing = ruio::spawn(async move {
            let mut buf = vec![7_u8; CHUNK];
            for _ in 0..TOTAL / CHUNK {
                let (written, sent) = client.write_all(buf).await;
                written?;
                buf = sent;
            }
            client.shutdown(Shutdown::Write).await?;
            // Kept open until the other end has read it all.
            io::Result::Ok(client)
        });
        let started = Instant::now();
        let mut received = 0;
        if pooled {
            let pool = BufPool::new(16, CHUNK as u32)?;
            let mut arriving = server.recv_stream(&pool);
            while let Some(data) = arriving.next().await {
                received += data?.len();
            }
        } else {
            let mut buf = Vec::with_capacity(CHUNK);
            loop {
                buf.clear();
                let (read, filled) = server.read(buf).await;
                buf = filled;
                match read? {
                    0 => break,
                    read => received += read,
                }
            }
        }
        let elapsed = started.elapsed();
        writing.await.map_err(io::Error::other)??;
        if received != TOTAL {
            return Err(io::Error::other("the stream ended short"));
        }
        Ok(mib_a_second(received, elapsed))
    }

    /// How many MiB a second are written to a file, and read back from it.
    async fn file() -> io::Result<(f64, f64)> {
        const TOTAL: usize = 64 * MIB;
        let name = format!("ruio-speed-{}", std::process::id());
        let path = std::env::temp_dir().join(name);
        let file = File::create(&path).await?;
        let mut buf = vec![7_u8; CHUNK];
        let started = Instant::now();
        for chunk in 0..TOTAL / CHUNK {
            let offset = (chunk * CHUNK) as u64;
            let (written, sent) = file.write_all_at(buf, offset).await;
            written?;
            buf = sent;
        }
        let written = mib_a_second(TOTAL, started.elapsed());
        file.close().await?;

        let file = File::open(&path).await?;
        let started = Instant::now();
        let mut total = 0;
        loop {
            buf.clear();
            let (read, filled) = file.read_at(buf, total as u64).await;
            buf = filled;
            match read? {
                0 => break,
                read => total += read,
            }
        }
        let read = mib_a_second(total, started.elapsed());
        file.close().await?;
        fs::remove_file(&path).await?;
        if total != TOTAL {
            return Err(io::Error::other("the file read back short"));
        }
        Ok((written, read))
    }

    /// How many million times a second two tasks yield to each other.
    async fn yields() -> io::Result<f64> {
        const YIELDS: usize = 1_000_000;
        let other = ruio::spawn(async {
            for _ in 0..YIELDS {
                ruio::yield_now().await;
            }
        });
        let started = Instant::now();
        for _ in 0..YIELDS {
            ruio::yield_now().await;
        }
        other.await.map_err(io::Error::other)?;
        Ok((2 * YIELDS) as f64 / started.elapsed().as_secs_f64() / 1e6)
    }

    /// Measures each, and prints what it found.
    pub(super) async fn run() -> io::Result<()> {
        let mut slept = (Duration::MAX, Duration::MAX);
        for _ in 0..RUNS {
            slept = slept.min(sleeps().await);
        }
        let millis = |time: Duration| time.as_secs_f64() * 1e3;
        println!(
            "sleep of 1 ms: {:.2} ms mean, {:.2} ms longest",
            millis(slept.0),
            millis(slept.1)
        );
        let trips = best(round_trips).await?;
        println!("round trips of 64 bytes: {trips:.0} a second");
        let plain = best(|| stream(false)).await?;
        println!("stream: {plain:.0} MiB/s");
        let pooled = best(|| stream(true)).await?;
        println!("stream into a pool: {pooled:.0} MiB/s");
        let mut filed = (0.0_f64, 0.0_f64);
        for _ in 0..RUNS {
            let (written, read) = file().await?;
            filed = (filed.0.max(written), filed.1.max(read));
        }
        println!("file written: {:.0} MiB/s", filed.0);
        println!("file read: {:.0} MiB/s", filed.1);
        let yielded = best(yields).await?;
        println!("yields between two tasks: {yielded:.1} million a second");
        Ok(())
    }
}

#[cfg(any(ruio_uring, ruio_iocp))]
#[ruio::main]
async fn main() -> std::io::Result<()> {
    timed::run().await
}

#[cfg(not(any(ruio_uring, ruio_iocp)))]
fn main() {
    println!("ruio has no I/O to time on this engine");
}
