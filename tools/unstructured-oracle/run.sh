#!/bin/sh
# A whole run of the judge: the embedded Postgres in a throwaway directory, the corpora loaded, the vectors computed, the
# keyword index built, then judge.py with this script's argument. load.py replaces the DuckDB file, so the order is fixed.
#   UNSTRUCTURED_ORACLE_PYTHON=<venv>/bin/python UNSTRUCTURED_MODEL_DIR=<model> tools/unstructured-oracle/run.sh --write
#   ... tools/unstructured-oracle/run.sh --check
set -e
root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$root"
python=${UNSTRUCTURED_ORACLE_PYTHON:?name the venv pinned by tools/unstructured-corpus/pins.env}
: "${UNSTRUCTURED_MODEL_DIR:?name the directory embed.py --fetch filled}"
bazel build @embedded_postgres//:postgres
marker=$(bazel cquery --output=files @embedded_postgres//:pg/PG_ROOT 2>/dev/null)
pg_root=$(dirname "$(bazel info output_base)/$marker")
exec "$python" tools/unstructured-oracle/run_postgres.py "$pg_root" sh -c '
  "$0" tools/unstructured-corpus/load.py &&
  "$0" tools/unstructured-corpus/embed.py &&
  "$0" tools/unstructured-oracle/judge.py --index &&
  "$0" tools/unstructured-oracle/judge.py "$1"' "$python" "$1"
