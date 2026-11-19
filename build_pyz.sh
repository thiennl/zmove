#!/bin/sh
# Build a single-file executable (zipapp) runnable on Rocky 8+ with
# python3.8+ (e.g. `dnf install -y python38`), no install needed.
# Output: dist/zmove.pyz (or $1). Pure stdlib, no network.
set -eu
SRC=$(cd "$(dirname "$(readlink -f "$0")" )" && pwd)
OUT=${1:-"$SRC/dist/zmove.pyz"}
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT INT TERM
mkdir -p "$(dirname "$OUT")"
mkdir -p "$TMP/zmove"
cd "$SRC"
# copy code only: skip caches, Windows metadata, build output, git data
find . -path ./dist -prune -o -path ./__pycache__ -prune \
  -o -name '*.Zone.Identifier' -prune -o -path ./.git -prune \
  -o -type f -print | while IFS= read -r f; do
  d="$TMP/zmove/$(dirname "$f")"
  mkdir -p "$d"
  cp "$f" "$TMP/zmove/$f"
done
cat > "$TMP/__main__.py" <<'EOF'
from zmove.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
EOF
python3 -m zipapp "$TMP" -o "$OUT" -p "/usr/bin/env python3" -c
chmod +x "$OUT"
ls -l "$OUT"
"$OUT" --help >/dev/null && echo "smoke: --help OK"
