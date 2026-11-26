# Step 8: the state when the work was paused (2026-11-26, about 22:30 UTC)

*Written by the step 8 session on the orchestrator's word ("PAUSE. The user has decided to put all attention on
step 6 until it is done"). Branch `claude/edition-step8`. Nothing here merges while paused. A session that resumes
this needs only this note, the records beside it and the two audits on `claude/edit-ahead`
(`docs/records/edition-homework/audits/step8-slice2-repair-recheck-2026-11-26.md` and
`step8-slices3-4-independent-2026-11-26.md`).*

## The commits

| Commit | What | State |
|---|---|---|
| up to `6a6f5e06d` | Slices 3 and 4 (the Float text deletion, the DateTime text), their records, S428 and S429 | Measured and reported; audited from outside since (2 blockers: below) |
| `90d2759b5` .. `5e9a8879b` | The repair of slice 2 after its first audit | Measured: all five recordings re-run, no recorded answer moved; 108 of 109 targets, the one a generated file, regenerated; local gate 557 pass, the 14 root-only failures |
| `095bebfd1`, `a8f8f7931`, `428e638ad`, `13c94cf52`, and the commit that holds this note | SCRATCH: the re-check of the repair (R1 to R3, S7, the notes), my reviewer's fixes, and the outside audit of slices 3 and 4 | The fast set alone is green. NOT measured, NOT reviewed in its last part, NOT for merge |

## Done and verified by the fast set

The fast set is `//core:guardrails //core:guardrails_edge //core:census //core:core_edge_tests
//pct:pct_route_print_test`; it passed on the code of `428e638ad` (the fourth fast run). After it only test data, a
generated file and records changed.

**The re-check of the repair (slice 2):**

- **R1.** `UpstreamPrints.unrepeatable`: a value's print is left to the database where its operand, anywhere in it
  (a lambda, the compiled body of a user function or derived property it calls), calls `now`, `today`, `random` or
  `generateGuid`, or reads rows by their order where the program does not define it: a `limit`, `drop` or `slice`
  node, or any native call not known to be order-free, over unordered rows. A callee's body is scanned without the
  call's arguments; a body the compiler walls is not in sight. Tests both ways in `UpstreamPrintsTest`
  (`aValueFromAnUnrepeatableSourceIsLeftToTheDatabase`, `aValueFromARepeatableSourceIsLifted`): the audit's two
  examples, a deterministic function and derived property lifted, a draw behind a function not.
- **R2.** The stale notes (`PrintedRelations`, `JavaEvalLedgerTest`, `UpstreamPrints.PRINTER`).
- **R3.** `ArchitectureTest.noWriterDependsOnAPrintsTail` (ArchUnit) in place of the source scan; `Replace` moved
  into `PrintTail`; the record says the carriage guard's scan was loosened and why.
- **S7.** One recorded answer per case per database: `EdgeCasesGoldenTest` now runs on every in-process database
  (SQLite joined) and lifts as each style's door does; cases `tail.*` (the parting inputs, the empty text, the
  pattern-special needles of N3) and `float.*` (a Float the database computes). The any-of test in
  `PrintTailParityTest` is deleted. Recorded with `bazel run //core:update_edge_cases`: 306 lines added, none of the
  48 existing lines moved.
- **N1.** Measured: lite's run does not compute a let that nothing reads (`let y = 'x'->parseInteger();
  (1.5 * 1.0)->toString()` answers `1.5`), so a literal's print written at the door skips nothing a run would have
  run. `PrintedRelations.lift` now writes a literal's print only where every earlier statement is a let. Test:
  `PctPlanRoutePrintTest.aBodysEarlierStatementAnswersAlike_whetherItsLastPrintIsWrittenOrRun`.
- **N2, R4.** In `slice2-repair-2026-11-26.md`: the product RAISES on H2 for a start past an int; MariaDB's and
  MySQL's padded `startsWith`/`endsWith` "measured by the lane, pending".

**The outside audit of slices 3 and 4** (what each item became is in `slices3-4-audit-response-2026-11-26.md`):

- **B1** fixed (`UpstreamPrints.anyPrinted` reads through the body's lets), with the audit's body as the test.
- **B2 (a)** fixed (a Float's last-step `toRepresentation` lifted as its `toString`).
- **The literal the lift could not see:** probed, the audit's suspicion is TRUE, fixed
  (`PrintedRelations.mayBeASpelledDate`).
- **S3** read: legend-pure writes a year unpadded (`DateFormat.java:240`); lite matches; test added.
- **Nits 1 and 3** fixed (stale comments; the DateTime digit notes).

## Not done, not verified

1. **The measurement of everything after `5e9a8879b`.** The five recordings and the pre-push set were stopped when
   the pause came. Only `//core:update_edge_cases` was re-run (above, and 24 lines of the four `format` cases
   re-recorded after their Pure was fixed). NOT run on these changes: `//pct:update_edge_contract`,
   `//spec:update_edge_corpus`, `//core:update_edge_pure_v1`, `//pct:update_engine_lane`; the PCT lanes, both
   corpus lanes, the size pins, `//gates:pct_h2`, `//docs:all` (three new records), `//core:update_generated_tests`
   (the ledger was regenerated by its tool), `//parser-equivalence:update_ratchets_test` (the new tests' model
   literals may move its count: a parser bump, "always okay"), the local gate.
   - The narrowed R1 rule lifts MORE than the measured repair did, and the DateTime rule lifts LESS: a recorded
     answer may move. Every moved line must be named.
2. **The second independent review** (of the audit's fixes: `mayBeASpelledDate`, B1, B2 (a), the written-literal
   rule, the R1 rewrite) was running when the pause came. Its findings are not applied. If its report arrived it is
   the last section of this note.
3. **The lane probe (S2)** is written (`probe-engine-date-time-cast-on-a-lane.md`, `.java.txt`) and NOT verified.
   The first local try never selected the probe class (its name did not end in `Test`; renamed since). Whether
   `--test_arg=--select-class=...` reaches the runner on a lane's target is not known. Do not send it to a lane
   before it prints its `PROBE CASE` lines on DuckDB, H2 and SQLite here.
4. **`slices3-4-audit-response-2026-11-26.md`**: its table for the user and its "Measured" section are not written.
5. **The register amendment drafts** (S428 for B2 (b) and S1, S429 for S4) are not written. Their material is
   recorded (below).
6. **`slice2-repair-2026-11-26.md`** lacks a section for what the re-check's fixes measured.
7. **Nit 2**, `docs/dialect-capability/reach.tsv` (11 `FLOOR_RAW` lines): not re-measured. Recipe: the five PCT
   suites on DuckDB with `-Dlegend.diagnostics=pct-cases` (each writes `pct-cases.tsv` to its undeclared outputs),
   `bazel run //core:capability_reach -- docs/dialect-capability/reach.tsv <the five files>`, then
   `bazel run //docs:update_dialect_capability`. It re-measures the whole lowering, so its diff will hold other
   work's lines too: read each.
8. **Nit 5**, a core test that runs the Float ROW against a database: not added.
9. **B2 (b)**, a last-step `format` lift: not built, by instruction. Its cost is in the response record.

## The material for the drafts (recorded in `edge_cases_answers.tsv`, 2026-11-26)

"Base" is the tree before step 8's deletions, READ from the deleted code by the outside audit, not run. "New" is
recorded on DuckDB, H2 and SQLite alike unless said.

| # | Where | The expression | legend-pure | legend-engine | Base | New | Row |
|---|---|---|---|---|---|---|---|
| 1 | legend-pure's door, last step | `format('%r', [(1.0 / 3.0)])` | `0.3333333333333333` | | `0.3333333333333333` | `0.333333` | S428 to amend, or lift (B2 (b), the user's) |
| 2 | same | `format('%s', [(1.5e-8 * 1.0)])` | `0.000000015` | | `1.5e-08` (DuckDB's cast) | `0.0` | same |
| 3 | same | `format('%s', [(1.0e-5 * 1.0)])` | `0.00001` | | `1e-05` (DuckDB's cast) | `0.00001` | same |
| 4 | same | `format('%f', [(1.0 / 3.0)])` | its `toString` text through `FormatTools.appendFloatString` (that method not read) | | not read | `0.333333` | same |
| 5 | same | `toRepresentation(0.1 + 0.2)` | `0.30000000000000004` | | `0.30000000000000004` | `0.30000000000000004` again (B2 (a) fixed; `0.3` between) | none needed |
| 6 | legend-pure's door, used further | `#TDS x:Float 0.00001#->toString() + ';'` | `0.00001` | | `1e-05` (DuckDB), `1.0E-5` (H2) | `0.00001` | S428 to amend (S1) |
| 7 | both doors | `format('%r', [$r.x->toOne()])`, `x` a Float cell written `1000000` | `1000000.0` | | `1000000` (the audit's reading for a Float held in an INTEGER column; this cell's base not run) | `1000000.0` | S428 to amend (S1) |
| 8 | legend-engine's door, in a store query | `format('%s', [$r.at->toOne()])`, a computed DateTime | | no `format` in a store query | `2014-01-01T10:20:30.25+0000` | `2014-01-01T10:20:30.250000000Z` | S429 to amend (S4) |
| 9 | same | `$r.at->toOne()->toString()` | | the database's cast | the `+0000` text | `2014-01-01 10:20:30.25` on DuckDB and H2; lite's `Z` text on SQLite | S429 (carried) |

The product's behaviours the tail's tests found, with no row (legend-engine's column is its SQL template, read in
the pinned 4.145.0 sources; its values were not asked of the engine runner):

| # | The expression | legend-pure | legend-engine | DuckDB | H2 | SQLite |
|---|---|---|---|---|---|---|
| 1 | `'\ta\n'->trim()->length()` | 1 (`String.trim`) | `trim(%s)`: spaces alone | 3 | 3 | 3 |
| 2a | `'a😀b'->length()` | 4 (UTF-16 units) | `length` / `char_length` | 3 | 4 | 3 |
| 2b | `'a😀b'->indexOf('b')` | 3 | `instr` / `LOCATE` (1-based: S134) | 2 | 3 | 2 |
| 3 | `'abc'->replace('', 'x')` | `xaxbxcx` | `replace(%s, %s, %s)` | `abc` | `abc` | `abc` |
| 4 | `'abcb'->indexOf('b', 2147483648)` | 1 (`intValue()` wraps) | its template takes two operands | -1 | RAISES ("Numeric value out of range") | -1 |
| 5 | a Float a store holds as the integer 1000000, `->toString()` at a last step at legend-pure's door | `1000000.0` | `1000000` in its corpus results | `1000000.0` | the same | the same |

Pending, to be measured by the MariaDB and MySQL lane (R4): `'a'->startsWith('a ')` and `'a'->endsWith('a ')` answer
TRUE in lite's SQL there (a padded comparison); legend-pure and the three embedded databases answer false.

## Found on the way, not step 8's to fix (for the orchestrator)

- Lite's run does not compute a let that nothing reads; legend-pure evaluates each statement (not read in the pinned
  sources today). A raise in such a let is not raised. No row.
- On SQLite a Float's NaN arrives as NULL: a lifted print of it is refused by name ("a value's toString prints one
  value; the answer is empty"), recorded as `float.zeroByZero`. H2 raises its own division by zero for the same
  expression.
- Lite's own text of a Float differs by database at the extremes: `1.7976931348623157E308` is three different digit
  strings on DuckDB, H2 and SQLite, and DuckDB's text of NaN is `-NaN` (the `pure_v1` lines of `float.largest` and
  `float.zeroByZero`).
- The pre-read of a printed side raises where Pure short-circuits (`a || b` with the print under `b`); the same
  print is Java's when written inline and the database's when written in a helper function.

## What remains, in order

1. Apply the second review's findings (the last section, if it arrived; else run a review of
   `git diff 5e9a8879b HEAD` first).
2. Verify the lane probe here on DuckDB, H2 and SQLite, then hand it to the orchestrator.
3. The full measurement (below), every moved recorded line named.
4. `reach.tsv` (nit 2), the Float row's lane test (nit 5).
5. The records: the response record's table and "Measured"; the amendment drafts for S428 and S429; the repair
   record's re-check section; then ONE table for the user (the two tables above joined).
6. One commit that is not scratch, the local gate, the push, and the head to the orchestrator with: the repair's
   R1 to R3, S7 and notes; slices 3 and 4's B1, B2, S1 to S4 and nits; the table.
7. Step 8's later slices, none started: `TDSNull`'s row; `+` in the tail; a last-step `format` lift, if the user
   rules it (B2 (b)).

## The exact next action

In a tree at this branch's head, with `export PATH=/root/.local/bin:$PATH` and one Bazel at a time:

```
for t in //core:update_edge_cases //pct:update_edge_contract //spec:update_edge_corpus //core:update_edge_pure_v1 //pct:update_engine_lane; do bazel run $t --jobs=3; git status --short; done
bazel test //core:guardrails //core:guardrails_edge //core:census //docs:all //core:core_edge_tests \
  //core:core_tests_integration //core:core_tests_lanes //core:lean_sql_baseline_test //spec:spec_tests \
  //spec:corpus_duckdb //spec:corpus_h2 //spec:lean_sql_corpus_duckdb //spec:lean_sql_corpus_h2 \
  //pct:engine_lane_duckdb_test //pct:engine_lane_h2_test //pct:update_edge_contract_tests \
  //core:update_edge_pure_v1_tests //core:update_generated_tests //core:update_edge_cases_test \
  //pct:pct_duckdb //pct:pct_h2 //pct:pct_sqlite //pct:pct_channel_b //pct:pct_channel_b_h2 \
  //pct:pct_route_print_test //gates:pct_h2 //parser-equivalence:update_ratchets_test \
  --keep_going --jobs=3 --local_test_jobs=2 --test_output=errors
```

A recorded file that moves is read line by line against what this note predicts (more lifts from R1, fewer from the
DateTime rule) before it is kept. Then `bazel test --lockfile_mode=error //gates:local --keep_going --jobs=3
--local_test_jobs=2` (14 Postgres-backed and root-only targets fail in a container that runs as root).

The session's second worktree and its scratchpad are gone with its container; everything they held is in this
branch or in this note.
