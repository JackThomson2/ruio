//! Prints what the running kernel's io_uring supports, and exits with an
//! error if it is below ruio's baseline.

use std::process::ExitCode;

use ruio_driver::Probe;

fn main() -> ExitCode {
    match Probe::detect() {
        Ok(probe) => {
            print!("{probe}");
            if probe.check_baseline().is_ok() {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Err(err) => {
            eprintln!("cannot create an io_uring instance: {err}");
            ExitCode::FAILURE
        }
    }
}
