#!/usr/bin/env bash
# The product's SQL changes between two commits (tools/equality-measure/README.md):
#   tools/equality-measure/sql-changes.sh <commit A> <commit B> <out dir>
#   tools/equality-measure/sql-changes.sh --dirs <out dir A> <out dir B> <out dir>   (two dumps already taken)
# For each commit: checks it out (detached, in this tree, which must be clean), lays this harness over it, runs
# bazel run //core:sql_dump on its own lean-SQL sets, and keeps <out dir>/<commit>/sql.tsv; then writes
# <out dir>/sql-changes-<A>-<B>.md. The tree is returned to where it was.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
if [ "$1" = "--dirs" ]; then
  out="$(mkdir -p "$4" && cd "$4" && pwd)"
  python3 "$here/sql_diff.py" "$2" "$3" "$out/sql-changes-$(basename "$2")-$(basename "$3").md" "$(basename "$2")" "$(basename "$3")"
  exit 0
fi
a="$1"; b="$2"
out="$(mkdir -p "$3" && cd "$3" && pwd)"
cd "$repo"
[ -z "$(git status --porcelain --untracked-files=no)" ] || { echo "the tree must be clean" >&2; exit 1; }
harness="$(git rev-parse HEAD)"
base="$(cat "$here/BASE")"
paths=(tools/equality-measure core/src/test/java/com/legend/equalitymeasure core/BUILD.bazel)
back="$(git symbolic-ref -q --short HEAD || git rev-parse HEAD)"
restore() { git reset -q --hard; git checkout -q "$back"; }
trap restore EXIT
for c in "$a" "$b"; do
  short="$(git rev-parse --short=8 "$c")"
  git checkout -q --detach "$c"
  git diff "$base" "$harness" -- "${paths[@]}" | git apply --3way
  bazel run //core:sql_dump -- "$repo/core/src/test/resources/lean-sql" "$out/$short"
  git reset -q --hard
done
sa="$(git rev-parse --short=8 "$a")"; sb="$(git rev-parse --short=8 "$b")"
python3 "$here/sql_diff.py" "$out/$sa" "$out/$sb" "$out/sql-changes-$sa-$sb.md" "$sa" "$sb"
