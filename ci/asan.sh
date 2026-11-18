#!/usr/bin/env bash
# Runs the tests under AddressSanitizer, with leak detection: the
# workspace's, and ruio-compat's again with the adapter Windows has.
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

# ruio-compat's adapter for the engines whose streams lend themselves to
# their own operations, which Windows has and a build for io_uring has on
# request: it keeps a stream where its operations borrow it, the one place
# the adapters go beyond what the compiler checks. The cfg changes every
# unit's flags, so it builds in a target directory of its own.
RUSTFLAGS="$RUSTFLAGS --cfg ruio_compat=\"generic\"" CARGO_TARGET_DIR=target/asan-generic \
  cargo +"$toolchain" test -p ruio-compat --lib --tests --all-features --locked \
  --ignore-rust-version -Zbuild-std --target "$target" "$@"
