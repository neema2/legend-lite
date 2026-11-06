# M8b audit brief — the database verdict row self-contained (2026-11-01)

The independent audit of the test-is-the-product plan's M8b (R15/R22). Read-only; findings as blockers / should-fix /
nits with file:line and a concrete Pure body each; fold nothing. The user's ruling in force (the plan's §6 "B3 as
settled", design 1, 2026-11-01): an assert under the database judge is one lowered statement answering holds /
message / unjudged; the plan carries that statement and NOTHING about the assert. M8b must change no outcome, no
census count and no failure MESSAGE TEXT (the accepted-divergence registers match witness substrings of messages):
the DuckDB corpus lanes read identical to M8a's. The verdict statements' TEXT changes by design (two more columns).

## What M8b built (the diff since M8a, commit 52092449)

1. **`lowering/VerdictSql.selfContained(row, name, wantEqual)`**: every verdict row (the five columns `__verdict,
   __expected, __actual, __unjudged, __lenient`, emitted by `predicateOver`, `predicate` and `statementOf`) gains
   `__holds` -- the verdict under the assert's polarity (`__verdict` itself, or `NOT __verdict`) -- and `__message` --
   the failure text: `CONCAT(name || '\nexpected: ', COALESCE(__expected, 'null'), '\nactual:   ', COALESCE(__actual,
   'null'))` for the positive families, the constant `'assertNotEquals: both sides are equal'` for the negated ones --
   added to the row's body select (a `SqlWith`'s body or a bare `SqlSelect`; the new projections reference the three
   existing projections' expressions). `batchOutputs()` and `schema()` carry the two columns (so the fused statement's
   `indexed` union and the runner's TABULAR decode see seven).
2. **`judge/VerdictBatch`**: a `SelfContained` function beside the `Fusion` (both the lowering's, handed by
   `BodyDriver.newVerdictBatch`); `defer` wraps the row before it is pending (so the fused members, the keyed members
   and the split rung's lazy `executeOne` all run the self-contained form); `Judge.judge(name, row)` (no polarity);
   `Pending` drops `wantEqual`; `executeOne(name, vq, on, runner)`.
3. **`driver/DatabaseJudge.verdictOf(name, row)`**: unjudged as before (`row[3]`, the Java-formatted detail with the
   excerpts); then `__holds` (`row[5]`, a Boolean or a loud wall), `sqlJudgedInDatabase`, the leniency census off
   `row[4]`, `ok()` where it holds, else `fail(String.valueOf(row[6]))` -- the statement's own message, no Java text.
   `runVerdict`'s non-batched path wraps the row itself before `executeOne`.
4. **`executionplan/ExecutionPlan.Verdict(name, arm, base, wrapper)`**: `wantEqual` deleted from the plan record (MV2:
   nothing about the assert in the plan but the listener's name and the census arm); `PlanJson` VERSION 24 -> 25
   (dated); every constructor site (HostSides, HostAsserts, VerdictBatch ×3, PlanJson, the tests) adjusted. The host
   judgment records carry their own polarity since M8a (`Equals.wantEqual`, `GridPair.wantEqual`, `Rendered.wantEqual`,
   `Condition.wantTrue`, ...).
5. **Guards, dated:** `JavaEvalLedgerTest` DatabaseJudge 596 -> 595, BodyDriver 945 -> 946; `PlanJsonTest`'s two
   version pins 24 -> 25.

## Questions

- **A. Message text, byte for byte.** For each verdict family's row (equality, grid rows, grid cells, size, size of a
  graph, empty, contains, condition, allOf, tolerance, subset, rendered text, grid tolerance, JSON text, JSON root
  multiset, textEquals), compare the Java text `name + "\nexpected: " + row.get(1) + "\nactual:   " + row.get(2)` the
  deleted `verdictOf` built with the SQL text `selfContained` builds: (i) are `__expected`/`__actual` always VARCHAR
  (`String.valueOf` of the decoded cell is the cell's own text) -- any family whose expected/actual projection is a
  number, a boolean or a date, which Java would have spelled by `toString` and SQL by its own cast (DuckDB's `CAST(1.0
  AS VARCHAR)` is `1.0`; H2's; a boolean `true` vs `TRUE`)? (ii) a NULL canon: Java spelled `null`; SQL
  `COALESCE(x, 'null')` -- same; (iii) `CONCAT` semantics per dialect (MySQL/MariaDB `CONCAT` returns NULL on a NULL
  argument -- every argument is COALESCEd or a literal; Postgres `||`; Snowflake; ClickHouse; BigQuery's string literal
  escapes `\n` as `\\n` -- the dialect's `stringLit` must write a newline the engine reads back as one; SQLite);
  (iv) the newline inside a SQL string literal on every dialect the corpora and channel B run (DuckDB, H2, Postgres,
  SQLite, MariaDB/MySQL, Databricks, Snowflake, BigQuery, Trino, ClickHouse) -- which `stringLit` overrides escape it
  and which pass it raw, and does each engine read a raw newline inside quotes as a newline? (v) the message where the
  verdict HOLDS: computed but unread (CONCAT evaluated per row: a cost, no effect) -- confirm no dialect errors on
  evaluating it (a CAST inside `__expected` that raises only when evaluated?).
- **B. `__holds` under `__unjudged`.** When `__unjudged` is non-null the judge raises before reading `__holds`; the
  SQL still computes `NOT COALESCE(__verdict, false)` -- harmless? For the negated families (`assertNotEquals`,
  `assertNotEmpty`, `assertFalse`): which rows reach `selfContained` with `wantEqual` false (the `runVerdict` callers:
  `databaseVerdict`, `databaseSize`, `databaseCondition`, `databaseTdsEquivalent`, `quantified`, `cellPool`, the
  rendered road, `SqlTextVerdicts`' three `textEquals` with `true`)? Is the polarity ALWAYS applied through
  `wantEqual` (never already folded into the row's `__verdict` by a builder such as `empty(sideRows, wantEmpty)` or
  `condition(condScalar, wantTrue)`)? A family whose builder already negates AND whose caller passes `wantEqual=false`
  would now double-negate: list every builder with a polarity argument and its caller's `wantEqual`.
- **C. The row's shape.** `selfContained` requires the body to be a `SqlSelect` with the three labelled projections:
  is every verdict row one (a `SqlUnion` body? `jsonRootMultiset`? `gridTolerance`? `textEquals`)? The keyed members
  (`keyedGroup`: a verdict over a raw grid's report) and the split rung's `executeOne` -- all wrapped once at `defer`;
  confirm no path wraps twice (the non-batched `runVerdict` wraps, then `executeOne` runs it unwrapped again? -- no:
  `executeOne` no longer wraps) and none runs an unwrapped row (`verdictOf` on a five-column row would read `row[5]`
  out of bounds: which callers reach `verdictOf`?).
- **D. The fused statement.** `batch()`'s `indexed(i, body, outs)` matches the body's projections to `batchOutputs`
  by label when `projections().size() == outs.size() - 1` (now 7): the wrapped bodies have exactly 7 -- and the
  "any other body shape rides a derived table" arm: does it project the two new columns too? `RenameCtes.rewriteRoot`
  over the new projections (column references into renamed CTEs: `__e_3`) -- the message's `COALESCE(__e.__t, ...)`
  references rename with the rest?
- **E. The plan record.** `ExecutionPlan.Verdict` without `wantEqual`: every reader gone (`PlanRunner`? the sinks?
  `PlanJson` round trip; `PlanRoundTrip`; the WebAssembly/native planners' plan readers, if any read verdict groups).
  PlanJson 25: any stored plan fixture (none found under `core/src/test/resources`; `fixtures/`? `datacube/`?
  `wasm/`?) and the `//:generated` checks.
- **F. Census and listener.** `sqlJudgedInDatabase`, `sqlUlpPolicy`, `sqlUnjudged`, the appeal path (`textEquals`
  failing by row -> `AssertFailed(message)` -> the appeal), the listener's verdict text (`listener.verdict(name, false,
  e.getMessage())`: the message is now the statement's -- same bytes, A). `[verdict]` diagnostics print.
- **G. Guards.** The two ledger re-pins; `VerdictSql` (lowering) and `VerdictBatch` (judge) unpinned -- should
  `selfContained` be named in a register (it composes message TEXT in SQL: a render-time spelling the lowering owns,
  not Java; is that the right layer by invariant 2/3 -- the message's words are Pure's assert vocabulary, not SQL
  syntax; the dialect renders `CONCAT`)? `ErrorShapeGuardrailTest`, `CodeShapeGuardrailTest` (method lengths),
  `HarnessDisciplineTest` (any new sort/distinct spelling: none expected), `ArchitectureTest` (judge depends on no
  lowering class: `SelfContained` is a functional interface the driver fills).
- **H. Parity and the corpora.** `BodyRouterParityTest`'s DATABASE-mode runs compare statement text between the walk
  and the router (both wrapped: same text); the DuckDB corpus must read exactly M8a's (host 2471 / 108, database
  2473 / 108, `other=6219`); H2 (`fallback=18`) and the other lanes at the chain. Name any body whose message could
  differ (a non-VARCHAR canon, a NULL canon, a dialect's CONCAT).
- **I. Records.** The plan's §9 M8b entry should name: the self-contained row (what the two columns are, where the
  text is spelled), MV2's plan-record change (PlanJson 25), the fused statement's two more columns on every lane (the
  core ids dumps change for the database judge's statements at the chain), the residue (the unjudged DETAIL still
  Java-formatted with excerpts; the leniency flag still read in Java for the census; `name` and `arm` still in the plan
  for the listener and the census; the message computed where the verdict holds).

## Files

`core/src/main/java/com/legend/lowering/VerdictSql.java` (`selfContained`, `withHoldsAndMessage`, `batchOutputs`,
`schema`, `batch`/`indexed`), `core/src/main/java/com/legend/judge/VerdictBatch.java`,
`core/src/main/java/com/legend/driver/{DatabaseJudge,BodyDriver,SqlTextVerdicts,HostSides,HostAsserts}.java`,
`core/src/main/java/com/legend/executionplan/{ExecutionPlan,PlanJson}.java`, the dialects' `stringLit` and `CONCAT`
arms under `core/src/main/java/com/legend/sql/dialect/`, `core/src/test/java/com/legend/{exec/ProgramRunnerTest,
executionplan/PlanJsonTest,testcases/PlanRoundTrip,driver/BodyRouterParityTest}.java`. Context: the plan's §6 "B3 as
settled", §2.D/2.E (MV2, MV3), §7 M8, §9 "M8a"; `docs/test-is-product-homework/M8A_AUDIT_BRIEF.md`. The diff:
`git diff` in the working tree against HEAD (52092449).
