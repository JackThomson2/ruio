//! Spawns a few tasks that sleep for different times, and prints them as they
//! finish.

use std::time::{Duration, Instant};

#[ruio::main]
async fn main() {
    let start = Instant::now();
    let tasks: Vec<_> = [30, 10, 20]
        .into_iter()
        .map(|millis| {
            ruio::spawn(async move {
                ruio::time::sleep(Duration::from_millis(millis)).await;
                println!("{millis} ms task done after {:?}", start.elapsed());
            })
        })
        .collect();
    for task in tasks {
        task.await.expect("the task panicked");
    }
}
