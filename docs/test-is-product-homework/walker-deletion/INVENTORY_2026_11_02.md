# The walker-deletion inventory (2026-11-02)

The user's ask (2026-11-02): every test path on the product path, the walk (`BodyDriver`, `BodyCompiler`,
`BodyRouter`, `BodyClass`) deleted, and no way for it to come back. This file is the measured starting line: every
body that still reaches the walk, on every lane, with the kinds that keep it there. The data files beside it are the
rows; nothing here is an estimate.

## How it was measured

- **Core and channel B: the walk forbidden.** `-Dlegend.diagnostics=forbid-walk` (`Diagnostics.NAMES`) makes the body
  router throw `IllegalStateException("forbid-walk: <reason> [<BodyKinds>]")` where it would walk a body: no class
  admits it, or the judge's planner refused an assert. Every such test fails naming why.
  - core: `bazel test //core:core_tests //core:core_tests_integration --jvmopt=-Dlegend.diagnostics=forbid-walk`
  - channel B under one judge: `bazel test //pct:pct_channel_b --jvmopt=-Dlegend.diagnostics=forbid-walk
    --jvmopt=-Dpct.channelb.judge=HOST` (then `=DATABASE`); `pct.channelb.judge` defaults to `BOTH`.
- **The corpus: its own census.** Each pass writes `corpus2-body-stayed.tsv` (the walked bodies, status, kinds), so
  `bazel test //spec:corpus_duckdb //spec:corpus_h2` gives the list without forcing.

## The starting line (before the agreement judge moved)

| Lane | Bodies still walked | Data |
|---|---|---|
| core (28 test targets) | 104 tests fail when forced | `core_forbid_walk_2026_11_02.tsv` |
| channel B, HOST judge | 23 of 1,249 | `channel_b_host_forbid_walk_2026_11_02.tsv` |
| channel B, DATABASE judge | 32 of 1,249 (the 23 + 9 asserts its planner refuses) | `channel_b_database_forbid_walk_2026_11_02.tsv` |
| channel B, BOTH judges (its default) | every assert body: BOTH had no program form | -- |
| corpus DuckDB host / database | 120 / 194 (moved 3,067 / 2,992) | `corpus_stayed_*_duckdb_*.tsv` |
| corpus H2 host / database | 120 / 307 (moved 3,067 / 2,879) | `corpus_stayed_*_h2_*.tsv` |

Under one judge, every channel B body that does not walk gives the same outcome as under both judges (1,226 of
1,226 compared, per test).

### Core's 104, by why

| Count | Why | Kinds |
|---|---|---|
| 28 | no class admits the body | last-query, system-read (a query program reading the system store: system routing is the assert class's alone) |
| 27 | no class admits the body | last-query (raw grids, objects, legacy text, tagged lists: the query class's exclusions) |
| 10 | a test expecting a specific exception | (the forbid's own exception replaced it) |
| 9 | a judge's planner refused an assert | assert |
| 8+2 | no class admits the body | last-query, let-execute (a frame then a query, no assert) |
| 4+ | no class admits the body | assert-error |
| 3+ | no class admits the body | declares-test-data (seeded runtimes) |
| 4 | no class admits the body | store-write, effect-let, let-staged |

### Channel B's 23 (HOST)

21 `assertError` bodies (`[assert,assert-error]`), one `[assert,check]` (variant flatten of a scalar), one
`[assert,assert-error,last-let]`. The DATABASE judge adds 9 asserts its planner refuses.

### The corpus

The host passes' 120 are the mixed and effect shapes (`system-read` with effects, the test-data generator, the
computed effect maps, `assertError`, the census carrier); the database passes add the asserts the database planner
does not plan (`database-unplanned-assert`: 70 DuckDB bodies, 185 H2).

## First move: the agreement judge on the program road (2026-11-02)

`JudgeMode.BOTH` had no program form: `BodyClass.of(..., oneJudge=false)` refused every assert and frame body, so
channel B walked almost all of it. Now (`AgreementAsserts`) each statement-root assert's verdict step carries what both
judges need: the host judge's base sides and the database judge's verdict statement over them (`DatabaseAsserts
.planEach`: the assert's own root, planned alone; its wrapper, and its fused statement where it has several members).
The host's judgment rides beside the plan with its judge. The runner reads both at the assert's turn
(`PlanRunner.Judging.BOTH`), and the router's sink applies the walk's agreement rule (`AgreementArm.agree`).

Measured:
- channel B: the 23 shapes above are all that is left (`forbid-walk` under BOTH); per test, every outcome equals the
  walked baseline; the five lanes green unforced.
- corpus DuckDB under both judges (`corpus_one duckdb both`): moved 613 -> 3,067; outcomes unchanged (2,471 pass,
  108 fail); `disagree=0`. The database judge DECLINES 44 tests' asserts (`corpus_both_database_declined_*.txt`): they
  are exactly the database pass's walked bodies with a `database-unplanned-assert`. That residue is pinned once, by
  `PLATFORM_TEXT_DECIDED` "database" 44, and shrinks to zero with the database pass's burn-down.
