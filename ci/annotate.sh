#!/usr/bin/env bash
# Says something on the page of a GitHub Actions run, as an annotation,
# which the API gives to anyone who asks, where a run's logs take an
# account to read.
#
#   ci/annotate.sh notice TITLE FILE     the file, as a notice
#   ci/annotate.sh failures TITLE LOG    what failed in a log, as an error
#
# What failed is the lines of a log that say so, and the few after each:
# the tests that failed or hung, where each panicked and with what, and the
# errors of the compiler and of cargo, without the backtraces; or the log's
# last lines if it has none of those. A title has no comma and no colon.
set -euo pipefail

if [ $# -ne 3 ]; then
    echo "usage: $0 notice|failures TITLE FILE" >&2
    exit 2
fi
kind=$1
title=$2
file=$3

# An annotation is one line: its percent signs and its line breaks are
# escaped, the percent signs first, and GitHub keeps 64 KiB of it.
annotate() {
    local level=$1 text=$2 body
    body=$(printf '%s\n' "$text" | sed -e 's/%/%25/g' -e 's/\r$//' | awk '{ printf "%s%%0A", $0 }')
    echo "::$level title=$title::${body:0:60000}"
}

if [ ! -f "$file" ]; then
    annotate warning "$file was not written"
    exit 0
fi

case $kind in
notice)
    annotate notice "$(cat "$file")"
    ;;
failures)
    # Without the colours that cargo gives its own lines.
    plain=$(sed -e 's/\x1b\[[0-9;]*m//g' "$file")
    found=$(printf '%s\n' "$plain" |
        grep -E -A8 'panicked at|^error|\.\.\. FAILED$|has been running for over' |
        grep -E -v '^ +[0-9]+: |^ +at |^--$|^stack backtrace:|RUST_BACKTRACE' || true)
    if [ -z "$found" ]; then
        found=$(printf '%s\n' "$plain" | tail -n 30)
    fi
    annotate error "$found"
    ;;
*)
    echo "usage: $0 notice|failures TITLE FILE" >&2
    exit 2
    ;;
esac
