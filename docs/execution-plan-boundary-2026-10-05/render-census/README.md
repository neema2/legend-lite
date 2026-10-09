# The render census (E-0)

Every SQL statement the JVM suites render — queries, DDL and DML, from every dialect — recorded so that two commits can
be compared byte for byte. It is the judge for E, the dialects' move to one writer
(`docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md` §10): each stage must render exactly what its parent renders.

- `probe.py apply|remove` — the probe, never committed to the product, applied by matching method SIGNATURES so it fits
  every stage of E: it adds `RenderCensus` (`RenderCensus.java.txt`) and wraps each outermost render entry
  (`AnsiSqlRenderer.render` for queries, DDL and DML and `renderStatement` for statements with bound parameters, kind
  `statement`; `EngineStyleH2.render` for queries (Postgres's own DDL `render` went with PARK-16: it renders through the base); a dialect's call up to its super is counted
  once). `apply` checks every signature before it writes and refuses a tree it has already applied to, and removes it exactly. Each render writes one line — dialect, kind,
  the text's SHA-256 — and each distinct text once, under the test's undeclared-outputs folder, `LEGEND_RENDER_CENSUS`,
  or (a corpus judge action) beside the pass's ledger. (E-0 shipped it as a patch; E-1 reshaped `render`, so the patch's
  context lines no longer matched — the signature probe replaced it, recording exactly the same.)
- `run.sh <outdir>` — applies the probe (uncommitted work is measured as it stands), runs the lanes below with `--cache_test_results=no` (a run id stamped into the
  probe makes every rendering test and corpus judge action run again), collects the records, removes the probe.
  Lanes: `//gates:core`, `//gates:stress`, `//pct:pct_duckdb`, `//pct:pct_h2`, `//pct:pct_postgres`,
  `//pct:pct_channel_b`, `//spec:corpus_duckdb` and `//spec:corpus_h2` (unsandboxed, for the judge actions). About ten
  minutes on a quiet machine. Not run: the browser lanes (the probe is JVM-only; `//pure-protocol:twins_test`, which
  needs the WebAssembly planner, fails to build under the probe by design).
- `compare.py <dir>` summarises a run; `compare.py <before> <after>` lists every (lane, dialect, kind, text) whose count
  differs and exits non-zero if any does.

**Measured determinism (2026-10-08, three runs on unchanged code).** Byte-identical across runs except three things
the comparison normalises, and nothing else: an absolute temporary path quoted in SQL (sandbox folders, random temp file
names); a random UUID (any UUID is masked; the one measured is the activity comment's `executionTraceID`, legend-engine's
per-execution trace id); and, until 2026-10-08, a lambda's
scope id `fn:<hex>:<length>` (`resolver/FunctionBodyRows.scopeId` hashes the typed lambda's print, and the typed tree held
collections whose iteration order a JVM salts: fixed in the product by the compiler line, W1.5, and checked by the census
since). With those normalised, two full runs agree on all 52,085 entries.

**The baseline** (`baseline-summary.txt`, main `daa78d0eb`): 5,706,332 renders, 47,904 distinct texts.

**Stage results.** `e1-result.txt` — E-1: 0 of 52,085 entries differ. `e2-result.txt` — E-2, against main: of 52,094
entries, the 3 that differ are E-2's new test statements; every statement main renders, E-2 renders identically.
`e3-result.txt` — E-3, against E-2: of 52,095 entries, the 1 that differs is E-3's new test statement.
