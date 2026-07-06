#!/usr/bin/env bash
# Apply nanolance Python bindings + structural-dict branch from a bundle and open a PR.
set -euo pipefail

REPO="${NANOLANCE_REPO:-$HOME/nanolance}"
BUNDLE="$(cd "$(dirname "$0")/.." && pwd)/patches/nanolance-python-bindings.bundle"
BRANCH="cursor/structural-dictionary-strings-c2c9"

if [[ ! -f "$BUNDLE" ]]; then
  echo "Bundle not found: $BUNDLE" >&2
  exit 1
fi

if [[ ! -d "$REPO/.git" ]]; then
  echo "Clone nanolance first: git clone https://github.com/yoavbendor/nanolance.git $REPO" >&2
  exit 1
fi

cd "$REPO"
git fetch origin main
git fetch "$BUNDLE" "refs/heads/$BRANCH:$BRANCH"
git checkout "$BRANCH"
git push -u origin "$BRANCH"

echo "Branch pushed. Open PR:"
echo "  https://github.com/yoavbendor/nanolance/compare/main...$BRANCH"
