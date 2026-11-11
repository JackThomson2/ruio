#!/usr/bin/env bash
# Times an engine as it was at a revision and as it is now, on one machine.
# Two runs of CI land on machines whose speeds differ by half, so what one
# run measured says little beside what another did.
#
#   ci/compare.sh REVISION
#
# Builds this tree's timing example against the crates of REVISION and
# against this tree's, in release, runs the two in turn, twice over, and
# prints each figure as it was before and as it is after. If REVISION cannot
# be had, or does not build the example, it says so and fails nothing.
set -euo pipefail

if [ $# -ne 1 ]; then
    echo "usage: $0 REVISION" >&2
    exit 2
fi
revision=$1
root=$(git rev-parse --show-toplevel)
cd "$root"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# What a program is called once built.
exe=
case $(uname -s) in
MINGW* | MSYS* | CYGWIN*) exe=.exe ;;
esac

# A checkout of CI's holds the one commit it tests.
if ! git cat-file -e "$revision^{commit}" 2>/dev/null &&
    ! git fetch --quiet --depth=1 origin "$revision" 2>/dev/null; then
    echo "$revision could not be fetched, so there is nothing to compare with"
    exit 0
fi
mkdir "$work/before"
git archive "$revision" | tar -x -C "$work/before"
# Both run this tree's example, so that they measure the same.
mkdir -p "$work/before/crates/ruio/examples"
cp crates/ruio/examples/speed.rs "$work/before/crates/ruio/examples/speed.rs"

# One target directory for both, so that what they depend on is built once.
target=${CARGO_TARGET_DIR:-$root/target}

# Builds the example of the tree at $1, and keeps it as $2.
build() {
    (cd "$1" && CARGO_TARGET_DIR=$target cargo build --release --locked -p ruio --example speed) >&2 ||
        return 1
    cp "$target/release/examples/speed$exe" "$2"
}

if ! build "$work/before" "$work/speed-before$exe"; then
    echo "$revision does not build the timing example, so there is nothing to compare with"
    exit 0
fi
build "$root" "$work/speed-after$exe"

# In turn, so that what else the machine does meanwhile slows both alike.
for round in 1 2; do
    for which in before after; do
        "$work/speed-$which$exe" >"$work/$which-$round" ||
            echo "the example failed as it is $which" >&2
    done
done

echo "before is $(git rev-parse --short "$revision"), after is $(git rev-parse --short HEAD); each ran twice, in turn"
paste -d '\t' "$work/before-1" "$work/before-2" "$work/after-1" "$work/after-2" |
    awk -F '\t' '{
        name = $1
        sub(/: .*/, "", name)
        for (run = 1; run <= 4; run++) {
            sub(/^[^:]*: /, "", $run)
        }
        printf "%s: %s | %s before, %s | %s after\n", name, $1, $2, $3, $4
    }'
