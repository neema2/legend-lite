#!/bin/bash
# Re-run one corpus host pass outside Bazel, from a given execroot, writing its outputs to OUT.
# usage: ab_run.sh <execroot> <params template> <judge name> <out dir>
set -eu
ER=$1; TEMPLATE=$2; J=$3; OUT=$4
rm -rf "$OUT"; mkdir -p "$OUT/tmp"
P=bazel-out/darwin_arm64-fastbuild/bin/spec
sed -e "s#^-Djava.io.tmpdir=$P/${J}_tmp\$#-Djava.io.tmpdir=$OUT/tmp#" \
    -e "s#$P/${J}/#$OUT/#g" \
    -e "s#=$P/${J}\$#=$OUT#" \
    "$TEMPLATE" > "$OUT/params"
if grep -q "$P/${J}" "$OUT/params"; then echo "unreplaced output path in params" >&2; exit 2; fi
( cd "$ER" && external/rules_java++toolchains+remotejdk25_macos_aarch64/bin/java "@$OUT/params" ) > "$OUT/stdout.txt" 2>&1 || true
grep -o "Test run finished after [0-9]* ms" "$OUT/"*.log | head -1
grep -o "sql-chars=[0-9]*" "$OUT/"*.log | head -1
