#!/bin/sh
# The gs-quant oracle (README.md): snap.py on the checkout's .venv (or GSQUANT_ORACLE_PYTHON), with every pin of
# pins.env installed from requirements.txt when the interpreter holds another release -- never into a system Python:
# with no .venv and no GSQUANT_ORACLE_PYTHON this refuses, naming the fix. Build the samples first
# (bazel build //datacube:ts_replay_samples).
#   tools/gs-quant-oracle/snap.sh --write [--only GLOB]          snap the answers into answers.jsonl.gz and ledger.tsv
#   tools/gs-quant-oracle/snap.sh --check [--only GLOB]          report DRIFT, UNSNAPPED, STALE, GONE, VERSION, TAG,
#                                                                READER, WORDS (exit 1 on any)
#   tools/gs-quant-oracle/snap.sh --write --fixture DIR          an adversarial fixture's two price files, separately,
#                                                                and traps.tsv
#   tools/gs-quant-oracle/snap.sh --release-diff OTHER_PYTHON    release-diff.tsv: the mapped definitions at another
#                                                                gs-quant against the pin's
# or `bazel run //tools/gs-quant-oracle:snap -- --check`.
set -e
if [ -n "${BUILD_WORKSPACE_DIRECTORY:-}" ]; then
  root=$BUILD_WORKSPACE_DIRECTORY
else
  root=$(cd "$(dirname "$0")/../.." && pwd)
fi
here=$root/tools/gs-quant-oracle
python=${GSQUANT_ORACLE_PYTHON:-}
if [ -z "$python" ]; then
  if [ -x "$root/.venv/bin/python" ]; then
    python=$root/.venv/bin/python
  else
    echo "snap.sh: no interpreter: make the checkout's .venv (uv venv --python 3.12 .venv) or set GSQUANT_ORACLE_PYTHON;" \
         "this tool never installs into a system Python" >&2
    exit 2
  fi
fi
# every pin exported, so requirements.txt's ${KEY}s read them (pip expands them; uv does not, so they are substituted)
eval "$(sed -n 's/^\([A-Z_]*\)=\(.*\)$/\1=\2; export \1/p' "$here/pins.env")"
wanted=$(mktemp)
trap 'rm -f "$wanted"' EXIT
sed -n 's/^\([a-z-]*\)==\${\([A-Z_]*\)}$/\1 \2/p' "$here/requirements.txt" | while read -r dist key; do
  eval "echo \"$dist==\${$key}\""
done > "$wanted"
missing=$("$python" -I -c '
import sys, importlib.metadata as m
for line in open(sys.argv[1]):
    dist, version = line.strip().split("==")
    try:
        have = m.version(dist)
    except m.PackageNotFoundError:
        have = None
    if have != version:
        print(dist + " " + str(have) + " -> " + version)
' "$wanted")
if [ -n "$missing" ]; then
  echo "snap.sh: off the pins in $python: $missing; installing from requirements.txt" >&2
  if command -v uv >/dev/null 2>&1; then
    uv pip install --python "$python" -r "$wanted"
  else
    "$python" -m pip install -r "$wanted"
  fi
fi
exec "$python" -I "$here/snap.py" "$@"
