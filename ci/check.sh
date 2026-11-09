#!/usr/bin/env bash
# Runs the checks CI runs: formatting, lints, tests and documentation, on the
# io_uring engine and on the park engine, and builds for macOS and Windows.
set -euo pipefail

cd "$(dirname "$0")/.."

cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked

# The executor on the park engine, the one engine besides io_uring, which
# shows that it needs nothing of an engine but the contract. The cfg changes
# every unit's flags, so it builds in a target directory of its own.
park=(env RUSTFLAGS='--cfg ruio_engine="park"' CARGO_TARGET_DIR=target/park)
"${park[@]}" cargo clippy -p ruio-executor --all-targets --all-features --locked -- -D warnings
"${park[@]}" cargo test -p ruio-executor --all-features --locked
"${park[@]}" RUSTDOCFLAGS="-D warnings" cargo doc -p ruio-executor --all-features --no-deps --locked

# The executor on systems with no I/O engine yet, which get the park engine:
# built, not run, for want of the systems. A target that is not installed
# is skipped, or fails with RUIO_REQUIRE_CROSS_CHECK=1, as CI sets;
# `rustup target add` installs one.
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
    CARGO_TARGET_DIR=target/cross cargo clippy -p ruio-executor --all-targets \
        --all-features --locked --target "$target" -- -D warnings
done
