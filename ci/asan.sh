#!/usr/bin/env bash
# Runs the tests under AddressSanitizer, with leak detection.
#
# Needs a nightly toolchain with the rust-src component, which rebuilds the
# standard library with the sanitizer:
#
#     rustup toolchain install nightly --component rust-src
#
# RUIO_NIGHTLY picks another nightly toolchain. Extra arguments go to
# `cargo test`.
set -euo pipefail

cd "$(dirname "$0")/.."

toolchain="${RUIO_NIGHTLY:-nightly}"
target="$(rustc +"$toolchain" -vV | sed -n 's/^host: //p')"

export RUSTFLAGS="-Zsanitizer=address"
export RUSTDOCFLAGS="-Zsanitizer=address"
export ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=1}"

# Doc tests are left out: rustdoc does not build them with the sanitizer.
cargo +"$toolchain" test --workspace --lib --tests --locked \
  --ignore-rust-version -Zbuild-std --target "$target" "$@"
