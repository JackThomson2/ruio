//! Chooses the engine ruio's I/O runs on, by the rule of ruio-executor's
//! build.rs, and tells the code: io_uring on Linux, as `cfg(ruio_uring)`,
//! and an I/O completion port on Windows, as `cfg(ruio_iocp)`, unless the
//! build asks for the park engine with `--cfg ruio_engine="park"`; and the
//! park engine, which runs no I/O, everywhere else, until ruio has an
//! engine there. Cargo.toml chooses the dependencies by the same rule.

use std::env;

fn main() {
    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rustc-check-cfg=cfg(ruio_uring, ruio_iocp)");
    let os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let park = env::var("CARGO_CFG_RUIO_ENGINE")
        .is_ok_and(|engines| engines.split(',').any(|engine| engine == "park"));
    if park {
        return;
    }
    match os.as_str() {
        "linux" => println!("cargo::rustc-cfg=ruio_uring"),
        "windows" => println!("cargo::rustc-cfg=ruio_iocp"),
        _ => {}
    }
}
