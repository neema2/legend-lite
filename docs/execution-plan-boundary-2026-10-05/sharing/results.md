# Sharing an in-memory database: measured (2026-10-08)

The measurements behind decision 1 of step 2 (`docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md` §9): when two runs may
share one in-memory test database. Run on commit `33db7a4d9` (the setup move; no behaviour change from main), on a
machine shared with three other sessions' Bazel jobs (load averages 8 to 51), so times are rough; counts are exact.

## The stress corpus (`//core:stress_tool`, 4,736 service tests, DuckDB)

Its tests take their data from connection test data, the kind the server runs.

| How sessions are given out | execution | wall | pass / fail / skipped |
|---|---|---|---|
| shared: one loaded session per distinct test data, a test with effects gets a private one (today's lane default) | 25.5 s | 42.7 s | 4,705 / 15 / 16 |
| a fresh, freshly loaded session per test | 3,473 s (58 min) | 3,484 s | 4,705 / 15 / 16 |

```
bazel run //core:stress_tool -- --backend duckdb --sessions shared --out <dir>
bazel run //core:stress_tool -- --backend duckdb --sessions fresh  --out <dir>
```

The same answers both ways; a fresh database per test is two orders of magnitude slower, as measured on 2026-09-16
(`StressSuites.java`: 27 min against about 30 s) and still after bulk loading (2026-09-23). The slowest tests load for
up to 20 s each.

## How often a run changes a shared database (`count-probe.patch`, applied, measured, reverted)

The probe prints one line when a session's test data is loaded (`FIRST`) or loaded again after a statement that
writes (`RELOAD_AFTER_WRITE`), with the number of setup statements that load runs, and one line when the stress
runner gives a test with effects its own session.

| Run | first loads (with any statement) | reloads after a write (with any statement) | tests with effects |
|---|---|---|---|
| stress corpus, shared, DuckDB (the probe's first version: statements not counted) | 16 | 0 | 0 |
| relational corpus, `bazel run //spec:corpus_one -- duckdb host` | 338 (8; 21 statements) | 124,148 (0) | — |
| relational corpus, `bazel run //spec:corpus_one -- h2 host` | 337 (8; 21 statements) | 124,147 (0) | — |

The stress corpus never writes. The relational corpus writes constantly, but its tests build their data with their
own setup functions (`executeInDb`) on one database per test package that the corpus harness opens and owns (the
engine's grouping, `MinimalCorpus`); the connection test-data reload after those writes never has a statement to run.

## What follows (decision A)

- Two runs share a database the runner opens when their connection and setup statements are the same; setup runs
  once, when it opens. The stress corpus already works this way: identical work, 25.5 s.
- A database the runner opened for sharing that a run changed is thrown away; the next run opens a fresh one. The
  stress corpus has no such run; the relational corpus's databases are the harness's, not the runner's, and are
  untouched.
- Today's in-place reload rebuilds the setup tables but keeps a table the run created itself; throwing the database
  away does not. The reloads it replaces ran no statement in either corpus.
