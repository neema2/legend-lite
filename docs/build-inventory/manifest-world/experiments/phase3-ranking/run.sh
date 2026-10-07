#!/bin/bash
# Phase 3 experiment: legend-pure's left-to-right overload ranking (-Dlegend.overload.lexicographic=true).
set -u
B=$PWD; H=$B/runs/homework/world; X=$B/runs/homework/phase3x; W2B=$B/runs/homework/phase2b/override_2b_core
LEX=-Dlegend.overload.lexicographic=true
restore() { bazel build //spec:judge_host_duckdb //spec:judge_database_duckdb //spec:judge_host_h2 //spec:judge_database_h2 //spec:judge_host_warehouse //spec:judge_database_warehouse > /dev/null 2>&1; }
echo "== 1. the 2b world (S1D + upstream core's 45 extras) with the new ranking"
restore; LABEL=x_2b_lex EXTRA_JVM=$LEX ONLY=judge_host_duckdb,judge_host_h2 python3 $H/e6_lanes.py $B $H $H/probe $W2B 2>&1 | tail -2
bazel test //pct:pct_duckdb_essential //pct:pct_postgres_essential //pct:pct_channel_b_essential //pct:pct_channel_b_unclassified "--test_env=JAVA_TOOL_OPTIONS=-Xbootclasspath/a:$W2B $LEX" > $X/pct_2b_lex.log 2>&1; grep -E "PASSED|FAILED" $X/pct_2b_lex.log
echo "== 2a. today's world, the new ranking: the local gate (core's suite and the guards)"
bazel test --lockfile_mode=error //gates:local "--test_env=JAVA_TOOL_OPTIONS=$LEX" > $X/gate_lex.log 2>&1; grep -E "Executed|FAILED" $X/gate_lex.log | tail -12
echo "== 2b. today's world, the new ranking: the six corpus passes"
restore; LABEL=x_today_lex EXTRA_JVM=$LEX python3 $H/e6_lanes.py $B $H $H/probe $X/override_none 2>&1 | tail -6
echo "== 2c. today's world, the new ranking: PCT"
bazel test //pct:pct_duckdb //pct:pct_h2 //pct:pct_postgres //pct:pct_channel_b "--test_env=JAVA_TOOL_OPTIONS=$LEX" > $X/pct_lex.log 2>&1; grep -E "Executed|FAILED" $X/pct_lex.log | tail -12
echo "== 2d. today's world, the new ranking: the reference lane"
bazel test //spec:reference_lane "--test_env=JAVA_TOOL_OPTIONS=$LEX" > $X/reflane_lex.log 2>&1; grep -E "PASSED|FAILED" $X/reflane_lex.log | tail -2
grep -E "^OVERLOAD\s|OVERLOAD\t[0-9]" bazel-testlogs/spec/reference_lane/test.log | head -3
