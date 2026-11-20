# engine-runner

Runs .pure sources through **legend-engine** — parse, compile, then the Testable framework
— and reports each testSuite as PASS/FAIL.

It exists because the asserted corpus in `core/src/test/resources/stress/` is portable
Legend grammar, and "portable" is only a claim until a second engine executes it. legend-lite
parses and plans those files; this runs them for real and checks the answers.

## Build and run

A Bazel target beside legend-lite's `core`, with its own jar pool (`@maven_runner` in
MODULE.bazel: the engine's execution stack at the pinned release, and the driver versions
it runs with). Paths are relative to where you type the command.

```
bazel run //tools/engine-runner:testable -- <file.pure>... [--testable=<fqn>]... [--dump=<dir>]
bazel run //tools/engine-runner:parse -- <file.pure>...
bazel run //tools/engine-runner:lite_parse -- <file.pure>...
bazel run //tools/engine-runner:token_dump
```

- `--testable=` selects which testable elements to run; omit to parse and compile only.
  They run one at a time, in the order given.
- `--dump=` writes the full expected/actual JSON per failing assertion, and a test error's
  whole text (`<testable>.<suite>___<test>.error.txt`). Without it the report truncates at
  300 characters, which for a 60-column TDS is identical on both sides and tells you nothing,
  and for an error is stack before the engine's own message.
- `--report=<file>` writes one line per test, `status`, `testable`, `suite`, `test`, `millis`
  (the testable's, all its tests together) and `detail` (tab-separated), as each testable
  finishes. The status is PASS, FAIL, ERROR, ROWS (`--rows`) or HUNG.
- `--limit=<seconds>`: a testable still running after this long is reported HUNG, and the JVM
  halts with exit status 3.
- `@<file>` stands for that file's lines, one argument each: a whole corpus and a batch of
  testables are longer than Windows' 32,767-character command line.

The whole stress corpus, or a named set of its services, runs through
`bazel run //scripts/corpus:run` (see `scripts/corpus/run.py`). It passes the linked projects
before the stress files, batches the testables one JVM each, and judges the results against
`scripts/corpus/quarantine.py`.

## Dependencies worth knowing about

`duckdb-execution` + `duckdb_jdbc` are present because the stress runtime declares
`type: DuckDB`. They are needed for the model to *compile*; the testSuites themselves always
execute against H2 regardless, because `TestRuntimeBuilder` swaps every connection for a
seeded local H2 (see docs/UPSTREAM_FINDINGS.md).

A test that reads from no store runs as Java: legend-engine compiles its plan's Java with
javac over `new ClassGraph().getClasspath()`. ClassGraph 4.8.25, the engine BOM's, drops
every class-path entry that starts with `../`, and under `bazel run` Bazel's launcher writes
every external jar that way, so `TestableMain` makes `java.class.path` absolute first
(`AbsoluteClassPath`; the S383 probe, 2026-11-19). Without it javac saw 47 of 666 jars, and
every storeless test errored with `package org.finos.legend.engine.plan.dependencies.store.shared
does not exist`. The corpus met the same cause first, with graph fetches (F94): `scripts/corpus/run.py`
launches the runner with `--classpath_limit=0`, whose manifest jar holds absolute URLs.
