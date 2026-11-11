#!/usr/bin/env bash
# Runs the checks CI runs: formatting, lints, tests and documentation, on the
# io_uring engine and on the park engine, the adapters of ruio-compat that
# Windows has on io_uring, and builds for macOS and Windows.
set -euo pipefail

cd "$(dirname "$0")/.."

cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked

# The executor and ruio on the park engine, the one engine besides io_uring,
# which shows that the executor needs nothing of an engine but the contract,
# and that ruio offers its runtime without io_uring. The cfg changes every
# unit's flags, so it builds in a target directory of its own.
park=(env RUSTFLAGS='--cfg ruio_engine="park"' CARGO_TARGET_DIR=target/park)
"${park[@]}" cargo clippy -p ruio-executor -p ruio --all-targets --all-features --locked -- -D warnings
"${park[@]}" cargo test -p ruio-executor -p ruio --all-features --locked
"${park[@]}" RUSTDOCFLAGS="-D warnings" cargo doc -p ruio-executor -p ruio --all-features --no-deps --locked

# ruio-compat's adapter for the engines whose streams lend themselves to
# their own operations, which is the adapter on Windows, built for io_uring,
# where it can be run: its library, its tests and its documentation. The
# examples receive into pools, which only the ring's adapter has. The cfg
# changes every unit's flags, so it builds in a target directory of its own.
generic=(env RUSTFLAGS='--cfg ruio_compat="generic"' CARGO_TARGET_DIR=target/generic)
"${generic[@]}" cargo clippy -p ruio-compat --lib --tests --all-features --locked -- -D warnings
"${generic[@]}" cargo test -p ruio-compat --lib --tests --all-features --locked
"${generic[@]}" cargo test -p ruio-compat --doc --all-features --locked
"${generic[@]}" RUSTDOCFLAGS="-D warnings" cargo doc -p ruio-compat --all-features --no-deps --locked

# The executor, ruio and ruio-compat off Linux: the park engine on macOS,
# which has no I/O engine yet, and the I/O completion port engine of
# ruio-iocp on Windows, built, not run, for want of the systems. Of
# ruio-compat it is the library that is built: its tests need a C compiler
# for the target, for the cryptography TLS is tested with. A target that is
# not installed is skipped, or fails with RUIO_REQUIRE_CROSS_CHECK=1, as CI
# sets; `rustup target add` installs one.
installed="$(rustup target list --installed 2>/dev/null || true)"
for target in aarch64-apple-darwin x86_64-pc-windows-msvc; do
    if ! grep -qx "$target" <<<"$installed"; then
        if [[ "${RUIO_REQUIRE_CROSS_CHECK:-}" == 1 ]]; then
            echo "error: the $target target is not installed" >&2
            exit 1
        fi
        echo "skipped: the $target target is not installed" >&2
        continue
    fi
    CARGO_TARGET_DIR=target/cross cargo clippy -p ruio-executor -p ruio -p ruio-iocp --all-targets \
        --all-features --locked --target "$target" -- -D warnings
    CARGO_TARGET_DIR=target/cross cargo clippy -p ruio-compat --lib --all-features --locked \
        --target "$target" -- -D warnings
done
