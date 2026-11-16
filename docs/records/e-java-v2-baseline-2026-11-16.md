# E-Java item 6: the v2 judge baseline (2026-11-16)

**Measured at:** `cf7810c261` (`claude/total-wall-e-v3`, which is PR1's `973b3753ca` plus E-Java's audit fixes).

**Lanes:** PCT channel B on DuckDB, the reference lane, under BOTH. Each suite ran alone, with
`-Dlegend.diagnostics=sql-sizes`.

**Time split:** taken with an uncommitted timing patch in `ChannelB`. It times `TestExecution.planBody`, planning only,
beside `runProgram`. Planning sends nothing, so planning twice is safe.

## Per suite

| Suite | Tests | Planning (s) | Run, excluding planning (s) | Statements | Per test | Of them: session / judge / system | Characters |
|---|---|---|---|---|---|---|---|
| essential | 345 | 2.4 | 2.9 | 892 | 2.59 | 343 / 549 / 0 | 2,279,267 |
| grammar | 137 | 1.7 | 1.3 | 542 | 3.96 | 134 / 291 / 117 | 1,503,027 |
| relation | 469 | 4.1 | 4.4 | 1,114 | 2.38 | 469 / 644 / 0 (+1 statement) | 2,329,245 |
| standard | 205 | 2.3 | 2.7 | 473 | 2.31 | 205 / 267 / 0 (+1) | 2,468,798 |
| unclassified | 93 | 0.5 | 0.6 | 193 | 2.08 | 93 / 100 / 0 | 253,403 |

"Run, excluding planning" is `runProgram`'s time minus a separate `planBody`'s, since `runProgram` plans again.

D2's ratio (judge statements per test, the ratchet's `judge.statements` / `discovered`) is as pinned:

| Suite | Judge statements per test |
|---|---|
| essential | 1.59 |
| grammar | 2.12 |
| relation | 1.37 |
| standard | 1.30 |
| unclassified | 1.08 |

## What it says

1. **Every test sends one `session` statement** before its own (`SET TimeZone` and the like). That is one statement
   per test on every suite: about 40% of all statements, with few characters.
2. **Grammar's outliers are three bodies:**

   | Test | Statements | Characters |
   |---|---|---|
   | `collection::tests::getAll::testBasic` | 119 (117 `system`: the metamodel's seed) | 392,595 |
   | `equality::equal::testEqualNonPrimitive` | 48 | 316,624 |
   | `equality::eq::testEqNonPrimitive` | 24 | 170,385 |

   - Together they're 191 of grammar's 542 statements and 880K of its 1.5M characters.
   - Without them, grammar is about 2.6 statements per test, in line with the others.
3. **The largest single statements by characters** are dates and floats:
   - testMonthNumber 51.6K, testFormatFloatWithRounding 48.2K, testAdjustByMonths 47.1K;
   - testSinH/CosH/TanH_EvalFuncSig, about 48.2K each.

   Those are print forms and canons, which move behind the edge hook in W10 (D's files, now E-Java's). Item 11 and W10
   measure them again.
4. **Planning is about 45% of a test's time** on every suite (essential 2.4 s of 5.3 s). The judges' speed work has a
   planning half, not only SQL.

## The v2 lean targets, in order

| # | Target | Measure | Expected |
|---|---|---|---|
| L1 | Grammar's three outliers (getAll's metamodel seed; NonPrimitive equality's per-element statements) | statements and characters on those three tests | grammar to ≤ 2.6 statements per test; D2's grammar ratio from 2.12 to ≤ 1.6 |
| L2 | The judge's float and date print forms | the top-20 characters | lands with W10 (the edge hook) |
| L3 | Planning time: a profile of `planBody` on essential (by phase: typing, lowering, rendering, the judge planner) | planning seconds per suite | first find where it goes; a target follows the profile |
| — | The session statement per test | — | not a judge item. It's the runner's session setup, and belongs to exec. Parked unless the orchestrator assigns it |
