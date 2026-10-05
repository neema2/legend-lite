#!/bin/bash
# The execution census, one side: run every lane with the statement dump and the debug-only PCT case recorder
# (-Dlegend.diagnostics=dump-sql,pct-cases; LEGEND_LITE_DUMP_SQL too, which lowering's dump still reads until
# P7-14), and keep each lane's log and recorded cases under
# runs/census/<label>/. Run at two commits, then compare with lanes_diff.py. See README.md.
set -u
label=${1:?usage: tools/census/lanes.sh <label>}
# the PCT lanes are suites of one target per suite or class (Bazel workplan P3-09): a suite writes no test.log, so
# each is expanded to its tests here
# (the corpus lanes too: their passes are actions, and the lane is a suite of checks)
expanded=$(bazel query 'tests(//pct:pct_duckdb + //pct:pct_channel_b + //spec:corpus_duckdb + //spec:corpus_h2)')
[ -n "$expanded" ] || { echo "lanes.sh: the suites expanded to nothing" >&2; exit 1; }
lanes=(//core:core_tests //core:stress_suites //core:stress_suites_h2 $expanded //pct:pct_h2)
mkdir -p runs/census/$label
bazel test "${lanes[@]}" --jvmopt=-Dlegend.diagnostics=dump-sql,pct-cases \
  --test_env=LEGEND_LITE_DUMP_SQL=1 --cache_test_results=no \
  > runs/census/$label/bazel.out 2>&1
grep -E "^//" runs/census/$label/bazel.out
T=$(bazel info bazel-testlogs 2>/dev/null)
for l in "${lanes[@]}"; do
  p=${l#//}; p=${p/://}
  cat "$T/$p/test.log" > "runs/census/$label/${p//\//_}.log"
  if [ -f "$T/$p/test.outputs/pct-cases.tsv" ]; then
    cat "$T/$p/test.outputs/pct-cases.tsv" > "runs/census/$label/${p//\//_}.cases.tsv"
  fi
done
