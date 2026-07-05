#!/usr/bin/env bash
# Push the structural-dictionary strings fix to yoavbendor/nanolance and open a PR.
# Run from a machine with push access to nanolance (the cloud agent token cannot).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PATCH="${ROOT}/patches/nanolance-structural-dict.patch"
BASE="08e1bd4ee8060f4a9aea473b1b80e8e3246a342b"
BRANCH="cursor/structural-dictionary-strings-c2c9"
WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

if [[ ! -f "$PATCH" ]]; then
  echo "missing patch: $PATCH" >&2
  exit 1
fi

git clone https://github.com/yoavbendor/nanolance.git "$WORKDIR/nanolance"
cd "$WORKDIR/nanolance"
git checkout -b "$BRANCH" "$BASE"
git apply "$PATCH"
git add -A
git commit -m "feat: structural dictionary encoding for scattered low-card strings

Add Lance-compatible structural dictionary path (flat bitpacked u32 indices +
uncompressed dictionary buffer) for low-cardinality string columns that do not
form long runs. Mirrors stock Lance heuristics (dict-divisor=2, size-ratio=0.8).

Includes round-trip test, pylance interop smoke, and 8-byte miniblock padding."
git push -u origin "$BRANCH"

if command -v gh >/dev/null 2>&1; then
  gh pr create \
    --repo yoavbendor/nanolance \
    --base main \
    --head "$BRANCH" \
    --title "Structural dictionary encoding for scattered low-cardinality strings" \
    --body "Lance-compatible structural dictionary for scattered low-card strings (flat bitpacked u32 indices + uncompressed dictionary buffer).

Mirrors stock Lance heuristics (dict-divisor=2, size-ratio=0.8). Includes round-trip test and pylance interop smoke.

Companion PR: https://github.com/yoavbendor/nanoarrow2parquet/pull/10"
else
  echo "Branch pushed. Open PR at:"
  echo "https://github.com/yoavbendor/nanolance/compare/main...${BRANCH}?expand=1"
fi
