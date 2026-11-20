# Stage A of the compatibility program, as one PR (DRAFT)

> Draft by the compat owner, 2026-11-20 ~00:50 UTC. Numbers marked **[final]** come from the judges' full measurement
> on the batch's final head; A3 piece 2's delta is added when it joins. The per-step sections are written by each
> step's builder (A2's is `A2-section.md` beside this file).

## In one paragraph

Lite's core now means one thing, Pure's definitions, and legend-engine's own spellings move out of the product.

- **legend-engine's SQL text printers are deleted.** Lite shows its own SQL.
- **`substring` and `indexOf` compute Pure's answer in the database.**
- **A detector at legend-engine's door refuses, by default, any request whose answer would differ from legend-engine's
  because of that change.** An operator can choose `warn` instead, or `engine`, which gives legend-engine's answer on H2
  and DuckDB.
- **Lite's two judges agree on meaning,** and legend-pure-style checking leaves the database judge.

Every place lite's answer now differs from legend-engine's is a register row the user ruled on.

It also carries three fixes that rode along to save CI runs:
- the parser test's speed-up;
- legend-engine's test runner running tests that read no database;
- a product bug that returned wrong rows on SQLite and Postgres when a window value was read inside a list operation.

It replaces #127 (A5's PR, kept open as review and CI evidence), which is closed with a pointer to this one.

## What is in it

| Step | What it does | Branch, head | Reviewed |
|---|---|---|---|
| **A2** | legend-engine's SQL text printers deleted (about 3,000 lines); lite's own SQL text in their place; then the plan-text family's dead code (`LiteSqlText`, the splice and temp-table tables, an unreachable FreeMarker branch) | `claude/compat-a2-printers` 6551c2036, `claude/compat-a2-plan-text` 059cc71df | approved |
| **A5** | the product's judge-parity fixes, and a PCT lane under both of lite's judges | `claude/compat-judges-parity` 6f21bed29 (#127) | approved |
| **A4** | legend-pure-style checking leaves lite's database judge, with a guard (shown to catch a plant) that no legend-pure or legend-engine audience plans a database-judge verdict | the batch branch | approved |
| **A3 piece 1** | lite's SQL-family frontends keep SQL's own `substring`/`position`, as `sqlSubstring`/`sqlPosition` | `claude/compat-a3-frontends` a4c1f2b1 | approved |
| **A3 piece 2** | the core's `substring` and `indexOf` mean Pure's (counting from 0; out of range raises, S383) | `claude/compat-a3-switch` **[pending the user's word on its lean-SQL re-pin]** | **[pending]** |
| **A3 detector** | refuses by default, at legend-engine's door, a request whose answer the core change would alter; walks store-bound code, user functions, qualified properties, graphFetch trees, constraints; lets only when their value is a lambda | `claude/compat-a3-detector` afcb77770 | approved |
| **A3 `engine` opt-in** | under `LEGEND_LITE_PURE_MEANING=engine`, the flagged calls are rewritten at the door to the database's own call (`substringAsGiven`, `sqlPosition`); H2 and DuckDB, the rest refused by name | `claude/compat-a3-raw-substring` b622e54f1, `claude/compat-a3-engine` **[final]** | approved |
| A3 docs | the register rows, the differences page, the rulebook amendment | `claude/compat-a3-docs` **[final]** | — |
| Parser test speed-up | the parser-equivalence referee parses each corpus text once (284.7 s → about 164 s) | `claude/compat-parser-parity-memo` 6c4509e0c | approved |
| Engine runner fix (Q12) | legend-engine's test runner runs storeless tests (ClassGraph dropped `../` class-path entries under `bazel run`) | `claude/compat-engine-runner-storeless` b648c377b | approved |
| Window-in-a-list fix (Q5) | `SelectMerge` keeps a window, aggregate or UNNEST at its own level when a dialect writes the reading operation as a query of its own (SQLite, Postgres) | `claude/window-list-lowering` bf2e9c4c3 | approved |

## The numbers [final]

- **PCT:**
  - under lite's own rules, by suite and database;
  - the 63 `indexOf` results that now pass, with the 18 predicted for BigQuery, Snowflake and Databricks measured by
    this CI run.
- **The corpus fail rosters:**
  - expected: H2 276 → 428 (+152), DuckDB 108 → 264 (+156);
  - A2: +145 and +149;
  - A3 piece 2: +7 per database (four answer tests and three text tests, none failing without A3).
- **The accepted divergences:** H2 41 → 44, DuckDB 46 → 49, each matched by its witness.
- **Lean SQL:**
  - A2 grows nothing;
  - A3 piece 2 grows 59 pins:
    - `indexOf`'s `- 1`;
    - `substring`'s out-of-range check, about 60 characters on a column. The core tests show 199 → 539 because their
      literal is 43 characters long and written twice.
  - The window fix grows nothing.
- **The `engine` batteries:** H2 46 agree / 2 differ / 1 both-fail; DuckDB 47 / 2. The differing rows are the
  3-argument `indexOf`, which legend-engine refuses and lite answers (E-R2).
- **own_corpus.matched:** 3310 → 3325.

## Every difference from legend-engine, ruled

S130, S134 (rewritten in place); S380 (CP-4: SQL text is never promised); S381 (DuckDB's `SET threads TO 1` for test
plans); S382 (OS maths digits); S383 (an out-of-range `substring` raises, like Pure); S384 (`toOne()` of empty); S385
(lite answers where legend-engine refuses); S386 (the `joinStrings` separator); S387 (firstDayOfWeek); S388 (a
serialize tree's qualified property the fetch tree lacks: lite computes it, ruled 2026-11-19 ~23:20). S128 and S316 are
amended. **[Each row's status checked against the user's rulings before this PR opens; any still PROPOSED is listed
below.]**

## For the user's word at this PR

- the PROPOSED register rows not yet ruled **[list]**;
- the lean-SQL baseline re-pins: A5's +0.121% plan JSON for `SET threads TO 1`, and A3 piece 2's 59 if not already
  ruled;
- the re-recorded edge-net ledgers and the rosters;
- A4's four narrowed ruled hooks;
- TestPlanJson 32 → 33;
- the `RootLoops.rejected` allowlist entry;
- own_corpus.matched's +5 from `InCollectionSpliceRefusalTest`, and +2 from `39956d2b7` (EngineSubstitutesTest's
  sectioned literals; the reviewer's M-b3);
- the ENGINE lane re-recorded by `22d53782f` (DuckDB lite.PASS 317 → 324, H2 252 → 257, every row FAIL → PASS from
  piece 2; M-b1);
- the WARN battery pins moved by `087ee888e` with piece 2 (H2 AGREE 46 → 11 / DIFFER 2 → 37; DuckDB AGREE 31 → 8 /
  DIFFER 18 → 41; and the engine files' i04 lite-side rows; M-b2);
- A2 PR 1's ordered-keys registers (`e66846d4d`: DuckDB +5, H2 +4, an UP move; M-b4) and A2's fail-roster growth
  itself (H2 +145, DuckDB +149; the six accepted rows were ruled ~20:35, the growth was not);
- the LiteSqlText entry A2 added to the edge's closed file list;
- piece 2's review round's moves (corpus +2/−1 per database, edge net, lean SQL +172/−2, census +17 and the 11 print
  casts −1) were ruled in the translator's session, 2026-11-20 ~07:20.

## Proof

- **Each piece:** the fast loop and its touched lanes, then //gates:local, on its own branch.
- **The batch:** the full measurement on the final head **[final]**, then this PR's CI on every OS.
- **Known in this container, not this batch's:**
  - channel B on MariaDB and MySQL is red on `main` already (Q3, Q3b);
  - the Snowflake cloud lanes need an account;
  - the 12 embedded-Postgres tests pass as the unprivileged user.

## Windows

**[CI on Linux, macOS and Windows, every job.]**

## Independent review

The compat program's independent reviewer (session_0195rQEnCA5MRQEk6iGfgDnb) reviewed every piece above, each
approved with its records on `claude/compat-review`; the combination review is on the full measurement **[final]**.

## Not in it

- **Stage B**, ratified after this lands.
- **The product-track items found on the way:**
  - lite's `sqlSubstring` following the SQL standard;
  - the test-data scan serving no function over a property in a filter;
  - the pure/v1 door skipping `ValidateDesugar`;
  - a packaged ModelChainConnection not reaching the typed context;
  - H2's `weekOfYear` following the JVM locale;
  - a draws-afresh value under a multiply-written operand;
  - the nine dead `AnsiSqlRenderer` hooks.
