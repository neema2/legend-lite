#!/bin/sh
# The gs-quant oracle (README.md): snap.py on a Python 3 with gs-quant at the pin (pins.env), installed from
# requirements.txt into the interpreter when it holds another release, over the gallery's samples (bazel build
# //datacube:ts_replay_samples first).
#   tools/gs-quant-oracle/snap.sh --write [--only GLOB]          snap the answers into answers.jsonl.gz and ledger.tsv
#   tools/gs-quant-oracle/snap.sh --check [--only GLOB]          report DRIFT, UNSNAPPED, STALE, GONE, VERSION, TAG,
#                                                                READER (exit 1 on any)
#   tools/gs-quant-oracle/snap.sh --write --fixture DIR          an adversarial fixture's two price files, separately
# or `bazel run //tools/gs-quant-oracle:snap -- --check`. GSQUANT_ORACLE_PYTHON (default: the checkout's .venv/bin/python,
# else python3) overrides the interpreter.
set -e
if [ -n "${BUILD_WORKSPACE_DIRECTORY:-}" ]; then
  root=$BUILD_WORKSPACE_DIRECTORY
else
  root=$(cd "$(dirname "$0")/../.." && pwd)
fi
here=$root/tools/gs-quant-oracle
python=${GSQUANT_ORACLE_PYTHON:-}
if [ -z "$python" ]; then
  if [ -x "$root/.venv/bin/python" ]; then python=$root/.venv/bin/python; else python=python3; fi
fi
GS_QUANT_VERSION=$(sed -n 's/^GS_QUANT_VERSION=//p' "$here/pins.env")
export GS_QUANT_VERSION
installed=$("$python" -I -c 'import gs_quant; print(gs_quant.__version__)' 2>/dev/null || true)
if [ "$installed" != "$GS_QUANT_VERSION" ]; then
  echo "snap.sh: gs-quant ${installed:-absent} in $python, the pin is $GS_QUANT_VERSION: installing from requirements.txt" >&2
  # requirements.txt with the pin substituted (pip expands ${GS_QUANT_VERSION} itself; uv does not)
  wanted=$(mktemp)
  sed "s/\${GS_QUANT_VERSION}/$GS_QUANT_VERSION/" "$here/requirements.txt" > "$wanted"
  if command -v uv >/dev/null 2>&1; then
    uv pip install --python "$python" -r "$wanted"
  else
    "$python" -m pip install -r "$wanted"
  fi
  rm -f "$wanted"
fi
exec "$python" -I "$here/snap.py" "$@"
