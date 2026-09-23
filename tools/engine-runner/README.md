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
- `--dump=` writes the full expected/actual JSON per failing assertion. Without it the
  report truncates at 300 characters, which for a 60-column TDS is identical on both sides
  and tells you nothing.

The whole stress corpus:

```
S=core/src/test/resources/stress
bazel run //tools/engine-runner:testable -- $S/*.pure \
  $(grep -h '^Service ' $S/92-services.pure | awk '{print "--testable="$2}')
```

(Out of date on the corpus side, measured 2026-09-22: the stress corpus now names
classes from the linked projects under projects/, so this fails to compile with
`Can't find class 'core_ratings::RatingVersion'` until the command passes those files
too. The Maven-built runner failed identically.)

Note zsh does not word-split unquoted parameter expansions; use an array or `${=VAR}`.

## Dependencies worth knowing about

`duckdb-execution` + `duckdb_jdbc` are present because the stress runtime declares
`type: DuckDB`. They are needed for the model to *compile*; the testSuites themselves always
execute against H2 regardless, because `TestRuntimeBuilder` swaps every connection for a
seeded local H2 (see docs/UPSTREAM_FINDINGS.md).
