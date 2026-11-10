#!/usr/bin/env bash
# A/B the benchmarks of this working tree against a base commit on this
# machine: the test binaries of both trees are built once, then run in
# alternation, one sample per round, and benchstat writes the comparison
# to stdout. A base missing locally is fetched from origin; one that cannot
# be resolved is an error. Benchmarks absent from the base show only the
# current value. The exit status reflects build and run errors only, never
# a regression.
#
# usage: hack/ab-bench.sh [-n count] [-t benchtime] [-b regex] <base>
#   -n  samples per benchmark per side (default 8)
#   -t  -benchtime of each sample (default 100ms)
#   -b  -bench regex (default .)

set -euo pipefail

benchstat=golang.org/x/perf/cmd/benchstat@v0.0.0-20261009192801-be2c69fb417e

usage() {
	echo "usage: ${0##*/} [-n count] [-t benchtime] [-b regex] <base>" >&2
	exit 2
}

count=8 benchtime=100ms bench=.
while getopts 'n:t:b:h' opt; do
	case $opt in
	n) count=$OPTARG ;;
	t) benchtime=$OPTARG ;;
	b) bench=$OPTARG ;;
	*) usage ;;
	esac
done
shift $((OPTIND - 1))
[ $# -eq 1 ] || usage

log() { echo "ab-bench: $*" >&2; }

root=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
base=$(git -C "$root" rev-parse --verify --quiet "$1^{commit}") ||
	{ git -C "$root" fetch origin "$1" && base=$(git -C "$root" rev-parse FETCH_HEAD); } ||
	{ log "base $1 not found locally or at origin"; exit 1; }

tmp=$(mktemp -d)
trap 'git -C "$root" worktree remove --force "$tmp/base" 2>/dev/null || true; rm -rf "$tmp"' EXIT
git -C "$root" worktree add --detach --quiet "$tmp/base" "$base"

goexe=$(go env GOEXE)

# pkgs <tree> lists the package directories of <tree> that define
# benchmarks, relative to <tree>, "." for the root package.
pkgs() {
	local mod pkg rel
	mod=$(go -C "$1" list -m)
	for pkg in $(go -C "$1" list -f '{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}{{end}}' ./...); do
		rel=${pkg#"$mod"}
		rel=${rel#/}
		rel=${rel:-.}
		if grep -qs '^func Benchmark' "$1/$rel"/*_test.go; then
			echo "$rel"
		fi
	done
}

# build <tree> <side> compiles each package of <tree> listed in
# $tmp/<side>.pkgs to $tmp/<side>/<package>/bench.test.
build() {
	local rel
	while read -r rel; do
		mkdir -p "$tmp/$2/$rel"
		go -C "$1" test -c -vet=off -o "$tmp/$2/$rel/bench.test$goexe" "./$rel"
	done <"$tmp/$2.pkgs"
}

# run <tree> <side> takes one sample of every benchmark of <side>, each
# binary run from its package directory as go test would, appending to
# $tmp/<side>.txt.
run() {
	local rel
	while read -r rel; do
		(cd "$1/$rel" && "$tmp/$2/$rel/bench.test$goexe" -test.run '^$' -test.bench "$bench" \
			-test.benchtime "$benchtime" -test.count 1 -test.timeout 10m) </dev/null >>"$tmp/$2.txt"
	done <"$tmp/$2.pkgs"
}

label_base="base $(git -C "$root" rev-parse --short "$base")"
label_current="current $(git -C "$root" rev-parse --short HEAD)"
if [ -n "$(git -C "$root" status --porcelain --untracked-files=no)" ]; then
	label_current+=" (dirty)"
fi
log "$label_base: $(git -C "$root" --no-pager log -1 --format=%s "$base")"
log "$label_current: $(git -C "$root" --no-pager log -1 --format=%s HEAD)"

pkgs "$tmp/base" >"$tmp/base.pkgs"
pkgs "$root" >"$tmp/current.pkgs"
log "building base: $(tr '\n' ' ' <"$tmp/base.pkgs")"
build "$tmp/base" base
log "building current: $(tr '\n' ' ' <"$tmp/current.pkgs")"
build "$root" current

# A tree key ahead of the samples labels the columns through -col tree.
echo "tree: $label_base" >"$tmp/base.txt"
echo "tree: $label_current" >"$tmp/current.txt"
for ((i = 1; i <= count; i++)); do
	log "round $i/$count"
	run "$tmp/base" base
	run "$root" current
done

go run "$benchstat" -col tree "$tmp/base.txt" "$tmp/current.txt"
log "done in ${SECONDS}s"
