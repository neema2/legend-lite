# The SQL census: what a change does to the SQL we send

A before/after proof for any change that may alter generated SQL (first used for store types,
`docs/STORE_TYPES_HOMEWORK_2026_10_02.md` step 5). Two parts, each run at a BASE and a HEAD commit
and compared. Everything is written under the git-ignored `runs/census/`.

## 1. The execution census: every statement the lanes send

```
git switch --detach <base>; tools/census/lanes.sh base
git switch <branch>;        tools/census/lanes.sh head
bazel run //tools/census:lanes_diff -- runs/census/base runs/census/head
```

`lanes.sh` runs core, the stress suites, the three spec corpora and the three PCT lanes with
`-Dlegend.diagnostics=dump-sql` (every statement sent, to the log; com.legend.diagnostics.Diagnostics) and keeps each lane's log. `lanes_diff.py`
compares the logs as multisets of lines after removing run-to-run noise (ports, sandbox paths, timings,
UUIDs, per-run verdict ids, the row order of an unordered result) and lists what remains per lane.
This covers the dialects the lanes EXECUTE: DuckDB and H2.

## 2. The render census: every PCT case on every dialect

```
git switch --detach <base>; tools/census/render.sh base runs/census/head/pct_pct_duckdb.cases.tsv runs/census/head/pct_pct_h2.cases.tsv
git switch <branch>;        tools/census/render.sh head runs/census/head/pct_pct_duckdb.cases.tsv runs/census/head/pct_pct_h2.cases.tsv
diff runs/census/render/base.tsv runs/census/render/head.tsv
```

At a commit that has it, the program is also a target: `bazel run //tools/census:render_census -- <out.tsv>
<cases.tsv>...` renders with that checkout's core (Bazel workplan P3-17); `render.sh` compiles it against a commit's
deploy jar, so it also measures commits older than the target.

The cases are the (model, expression) pairs the PCT lanes ran, recorded by the DEBUG-ONLY
`PctCaseRecorder` under `-Dlegend.diagnostics=pct-cases` (`lanes.sh` sets both). `RenderCensus.java` lowers each
case once, as `Compiler.execute` does, and renders it with DuckDb, H2, EngineStyleH2 and Postgres:
one line per case and dialect, the SQL or the failure. It compiles against the measured commit's own
`//core:core_tests_deploy.jar`, so it runs at commits older than itself. This covers the dialects no
lane executes: EngineStyleH2 and Postgres.

Give it a few cases of your own too (Base64 model TAB Base64 expression, one a line): a change that
shows no difference on them proves nothing about them.
