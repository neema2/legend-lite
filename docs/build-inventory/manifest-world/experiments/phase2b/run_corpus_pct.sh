#!/bin/bash
# Phase 2b: the corpus's six passes and PCT, with the core world (S1D + upstream core's extra overloads) first.
set -u
B=$PWD; H=$B/runs/homework/world; D=$B/runs/homework/phase2b; O=$D/override_2b_core
echo "== baseline: the six corpus passes at this commit"
bazel build //spec:judge_host_duckdb //spec:judge_database_duckdb //spec:judge_host_h2 //spec:judge_database_h2 //spec:judge_host_warehouse //spec:judge_database_warehouse > $D/baseline_build.log 2>&1; echo "baseline build exit $?"
echo "== the corpus passes with the core world"
LABEL=2b_core python3 $H/e6_lanes.py $B $H $H/probe $O > $D/corpus_2b_core.txt 2>&1; echo "corpus exit $?"; tail -12 $D/corpus_2b_core.txt
echo "== PCT with the core world first (resources are parent-first on the boot class path)"
bazel test //pct:pct_duckdb //pct:pct_channel_b //pct:pct_postgres //pct:pct_h2 --test_env=JAVA_TOOL_OPTIONS=-Xbootclasspath/a:$O > $D/pct_2b_core.log 2>&1; echo "pct exit $?"; grep -E "PASSED|FAILED|Executed|NO STATUS" $D/pct_2b_core.log | tail -30
