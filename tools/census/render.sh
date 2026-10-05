#!/bin/bash
# The render census, one side: build this checkout's core test deploy jar, then lower every case
# once and render it with DuckDb, H2, EngineStyleH2 and Postgres (RenderCensus.java) into
# runs/census/render/<label>.tsv. Run at two commits over the SAME cases, then diff the two files.
# RenderCensus compiles against the jar of the commit being measured, so it runs at any commit,
# including ones older than this tool. See README.md.
set -eu
label=${1:?usage: tools/census/render.sh <label> <cases.tsv>...}; shift
bin=$(bazel info output_base 2>/dev/null)/external/rules_java++toolchains+remotejdk25_macos_aarch64/bin
out=runs/census/render
mkdir -p "$out/$label"
# the core tests' deploy jar: core_tests_root's since core_tests became a suite (Bazel workplan P3-05; every
# core_tests_* target runs on the same classpath), core_tests' at an older commit
jar=core_tests_root
bazel query //core:core_tests_root >/dev/null 2>&1 || jar=core_tests
bazel build //core:${jar}_deploy.jar >/dev/null 2>&1 || { echo "render.sh: cannot build //core:${jar}_deploy.jar" >&2; exit 1; }
cat bazel-bin/core/${jar}_deploy.jar > "$out/$label.jar"
"$bin/javac" -cp "$out/$label.jar" -d "$out/$label" tools/census/RenderCensus.java
"$bin/java" -Xss16m -cp "$out/$label.jar:$out/$label" RenderCensus "$out/$label.tsv" "$@" 2>&1 | grep -v "WARN\|SLF4J"
