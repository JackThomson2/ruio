//! Tells the code the engine ruio's I/O runs on, by the rule of ruio's
//! build.rs, and with it which adapter the crate builds for the engine's
//! streams and files. The engine is io_uring on Linux, as `cfg(ruio_uring)`,
//! and an I/O completion port on Windows, as `cfg(ruio_iocp)`, unless the
//! build asks for the park engine with `--cfg ruio_engine="park"`, which
//! runs no I/O, as the engine everywhere else does. The adapter is the
//! ring's, as `cfg(ruio_compat_ring)`, where the engine is io_uring.

use std::env;

fn main() {
    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rustc-check-cfg=cfg(ruio_uring, ruio_iocp, ruio_compat_ring)");
    let os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let park = env::var("CARGO_CFG_RUIO_ENGINE")
        .is_ok_and(|engines| engines.split(',').any(|engine| engine == "park"));
    if park {
        return;
    }
    match os.as_str() {
        "linux" => {
            println!("cargo::rustc-cfg=ruio_uring");
            println!("cargo::rustc-cfg=ruio_compat_ring");
        }
        "windows" => println!("cargo::rustc-cfg=ruio_iocp"),
        _ => {}
    }
}
