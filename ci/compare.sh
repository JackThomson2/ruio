#!/usr/bin/env bash
# Times an engine as it was at earlier revisions and as it is now, on one
# machine. Two runs of CI land on machines whose speeds differ by half, so
# what one run measured says little beside what another did.
#
#   ci/compare.sh REVISION...
#
# Builds this tree's timing example against the crates of each REVISION and
# against this tree's, in release, runs them in turn, twice over, and prints
# each figure as it was at each revision and as it is now. A REVISION that
# cannot be had, or does not build the example, is said to be so, and fails
# nothing.
set -euo pipefail

if [ $# -eq 0 ]; then
    echo "usage: $0 REVISION..." >&2
    exit 2
fi
root=$(git rev-parse --show-toplevel)
cd "$root"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# What a program is called once built.
exe=
case $(uname -s) in
MINGW* | MSYS* | CYGWIN*) exe=.exe ;;
esac

# Builds the example of the tree at $1 in the target directory $2, and keeps
# it as $3.
build() {
    (cd "$1" && CARGO_TARGET_DIR=$2 cargo build --release --locked -p ruio --example speed) >&2 ||
        return 1
    cp "$2/release/examples/speed$exe" "$3"
}

# The programs to run, and what each is called in what is printed.
programs=()
names=()
for revision in "$@"; do
    # A checkout of CI's holds the one commit it tests.
    if ! git cat-file -e "$revision^{commit}" 2>/dev/null &&
        ! git fetch --quiet --depth=1 origin "$revision" 2>/dev/null; then
        echo "$revision could not be fetched, and is left out"
        continue
    fi
    name=$(git rev-parse --short "$revision^{commit}")
    # Named twice, it is built and run once.
    case " ${names[*]-} " in
    *" $name "*) continue ;;
    esac
    tree=$work/$name
    mkdir "$tree"
    git archive "$revision" | tar -x -C "$tree"
    # Every program is this tree's example, so that all measure the same.
    mkdir -p "$tree/crates/ruio/examples"
    cp crates/ruio/examples/speed.rs "$tree/crates/ruio/examples/speed.rs"
    # In a target directory of its own. Cargo names what it builds by a
    # crate's place in its workspace, wherever the workspace is, and takes
    # what is built for up to date while the sources are no newer: in this
    # tree's directory it would take this tree's build for the revision's,
    # whose files are as old as its commit.
    if build "$tree" "$work/target-$name" "$work/speed-$name$exe"; then
        programs+=("$work/speed-$name$exe")
        names+=("$name")
    else
        echo "$revision does not build the timing example, and is left out"
    fi
    # Only the program is kept.
    rm -rf "$tree" "$work/target-$name"
done
if [ ${#programs[@]} -eq 0 ]; then
    exit 0
fi
build "$root" "${CARGO_TARGET_DIR:-$root/target}" "$work/speed-now$exe"
programs+=("$work/speed-now$exe")
names+=(now)

# In turn, so that what else the machine does meanwhile slows all alike.
for round in 1 2; do
    for index in "${!programs[@]}"; do
        "${programs[$index]}" >"$work/${names[$index]}-$round" ||
            echo "the example failed as built for ${names[$index]}" >&2
    done
done

runs=()
for name in "${names[@]}"; do
    runs+=("$work/$name-1" "$work/$name-2")
done
echo "two runs of each, made in turn; a commit's figures are of the engine as it was there"
paste -d '\t' "${runs[@]}" |
    awk -F '\t' -v names="${names[*]}" '
        BEGIN { count = split(names, label, " ") }
        {
            name = $1
            sub(/: .*/, "", name)
            line = name ":"
            for (which = 1; which <= count; which++) {
                first = $(2 * which - 1)
                second = $(2 * which)
                sub(/^[^:]*: /, "", first)
                sub(/^[^:]*: /, "", second)
                where = label[which] == "now" ? "now" : "at " label[which]
                line = line (which > 1 ? ";" : "") " " first " | " second " " where
            }
            print line
        }'
