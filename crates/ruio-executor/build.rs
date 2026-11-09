//! Chooses the engine the executor runs on, and tells the code as
//! `cfg(ruio_uring)`: io_uring on Linux, unless the build asks for the park
//! engine with `--cfg ruio_engine="park"`, and the park engine everywhere
//! else, until ruio has an engine there. Cargo.toml chooses the
//! dependencies by the same rule.

use std::env;

fn main() {
    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rustc-check-cfg=cfg(ruio_uring)");
    let linux = env::var("CARGO_CFG_TARGET_OS").is_ok_and(|os| os == "linux");
    let park = env::var("CARGO_CFG_RUIO_ENGINE")
        .is_ok_and(|engines| engines.split(',').any(|engine| engine == "park"));
    if linux && !park {
        println!("cargo::rustc-cfg=ruio_uring");
    }
}
