# C5-J's batch: status at the pause (2026-11-16)

The user paused the batch on 2026-11-16 ("focus everything on D and E until both are done"). This is its exact
state, so it resumes cleanly after D and E land. Branch: `claude/c5-zone-stage`, cut from main `0c9032f56f`. No PR,
no CI. The batch lands only after D's first version (the user's word). Register rows are numbered provisionally
(`S-batch-n`) and assigned at assembly.

## Done (on `claude/c5-zone-stage`, in order)

| SHA | What |
|---|---|
| `dfd040b22e` | Group 1: `round(x, scale)` HALF_UP, `divide` exact HALF_UP, `toDecimal(Float)` by its text |
| `37119854fe` | Group 2: a fold over an emulated FULL OUTER JOIN folds the join once |
| `1f7083f447` | Group 4(a): a virtual thread per request, an Error answered 500, the LSP's documents guarded, a kept database one request at a time behind its lock, the catalog's turn released before the sink is written |
| `137e7e6aa3` | Group 1 rulings: S-batch-1..5 (SQLite's Decimal a double; a run-time divide scale refused; NaN/Inf round propagates, pure's JDK 17 words under the compatibility rules, `PlanOption.COMPATIBILITY_RAISES`, `SqlFn.IS_NAN`, `H2.finiteOnly`; toDecimal's scale 18; the FULL-join subquery limit), MongoDB made exact |
| `22936d758d` | 4(b) P1: a ceiling of 1024 concurrent requests, beyond it queued. P3: a 30 s wait for a kept database's session or the catalog's turn, then refused by name (S-batch-7) |
| `a13153480c` | 4(b) P2: a connection's declared `queryTimeOutInSeconds` carried by the plan's connection, set by exec on every statement for its target (`setQueryTimeout`); no default |
| `d460e22ba8` | 4(b) P2: plan version 54 (52 and 53 are D's), every reader moved together |
| `8517894ccd` | P2: `PlanJsonTest` reads version 54 (missed in `d460e22ba8`) |
| `1ed16d6f94` | 3a: pure's assert family carried as pure composes it; a leading always-raising statement is its raise; any other discarded leading statement kept (`TypedDiscarded`) and refused by name at planning (S-batch-6, ruled) |

P1, P3 and P2 were built and pushed after the pause was sent and before this session read it. They are tested
(each red on revert) and can stay or be reverted at the orchestrator's word.

## Half-done: owed before the batch lands

1. **P3's words.** The user ruled (2026-11-16) that the 30 s refusal uses lite's own words. Today it leads with
   legend-engine's pool's ("Connection is not available, request timed out after 30000ms: another request holds this
   database's session"). Owed: lite's own words in `Sessions.busy`, `SessionWaitTest`'s two expected texts, and
   S-batch-7 moved from pending to ruled.
2. **P2's readers on the other hosts.** The orchestrator's note: a declared `queryTimeOutInSeconds: 0` is written
   as 0, and every reader takes 0 as "no limit". Java already does this, since `setQueryTimeout(0)` is none and no
   change is needed. The TypeScript reader is `7ba5a4d43e` (`claude/total-wall-b3-batch`); the Python one is
   `fe0c2bfb` plus its 0 fix (`claude/c5-python-kind-stage`). At assembly, reconcile the plan version: this branch
   bumps to 54 on Java, plan-runner.ts and Python's `_runs`.
3. **RunScope.Refused at the outermost route** (the orchestrator's note): answered 500 and logged as an internal
   error naming the wall crossing, never as an ordinary answer, while everything below the route rethrows. Today
   `PureV1Api.answer` rethrows it, and `LegendHttpServer.route`'s `catch (Throwable)` answers 500. Owed: check
   the log line names the crossing, and add a test.
4. **Rows ruled by the user on 2026-11-16, not yet written or built:**
   - a time-zone `[` later in a date pattern: record as a row only;
   - an unknown time-zone name: BUILD it. Refuse at planning, checked against Java's zone list, in pure's words
     "Unknown time zone: X" (read in pure 5.99.0's `TimeZones.parse`). Test with a misspelt zone, red on revert;
   - rounding extreme Floats with a scale (≥2^52 with some scales; scale >22 on |x| < 1e-5): the closest answer,
     recorded as a row.
5. **The LSP's deep-nesting StackOverflowError** (no depth guard in the text parser) is pre-existing: record it as
   a known limit. Since 4(a) it is answered 500 (`ServerConcurrencyTest.anErrorPastAHandlerIsAnswered`).

## Not started

- **3c, TDSNull under lite's rules** (approved, (i) and (ii)). MEASURE first: both corpora under COMPAT with the
  string rule off. If outcomes move, list them before changing anything. The design:
  - (i) keep the instance `^TDSNull()` distinct through planning on both judges. Today both collapse it into the
    string: the database judge's planner rewrites it (`VerdictQueries.tdsNullSentinel`, DatabaseJudge:127/451, with
    the peer rule `VerdictSql.peerCells` and `CanonicalRenderSql`'s JSON-null arms), and the host spells a null
    cell under a TDSNull root as the string (`SideShapes.valuesShape`), which `Equality.same` equates with null.
  - (ii) the string `'TDSNull'` never equals a raw cell under lite's rules, on both judges.
  - Measuring the string rule alone needs (i) first, since the instance travels as the string. Re-pin
    `AssertVerdictSpliceTest.flatCellsTdsNullSentinel` with a dated note.
  - Earlier measurement: 18 corpus firings in the two host passes, all under COMPAT, 8 typed TDSNull and 10
    untyped. The 10 untyped are the candidates to move.
- **3b** (waiting on its probe): unchanged.
- **P5, the execute answer streamed** as upstream does:
  - a failure after the first row must leave the answer detectably broken, never a well-formed partial;
  - lite's readers (TS HttpEngine, Python, any Java client) must fail by name on a cut stream, with a test that
    cuts one mid-row;
  - Arrow stays buffered.

## Parking list (later batches)

- H2's `format('%s', [true])` writes `TRUE` where pure writes `true` (found by 3a's tests, which use a
  non-Boolean argument).
- Pure's `assertNotSize`, `assertNotContains`, `assertNotEq` and `assertIsNot` are not registered in lite. They
  are refused at typing, so not a false green: a coverage gap.

## How each piece was verified

- Group 1 rulings: core's 79 non-Postgres targets, PCT A/B on DuckDB and H2 (1573 testcases) and both corpora's four
  judge passes. No outcome moved; the TSVs are byte-identical. The 12 Postgres targets ran unprivileged; the
  MongoDB target passed.
- 3a, in its earlier at-typing form: the same wide run, with no move except the spec census (two prelude bodies).
  The final at-planning form: the targeted suites, with the census at 0 unwalled and 26 walled.
- P1, P3, P2: the targeted suites (architecture, root, server, exec, planner, model, protocol, compiler, census),
  each change red on revert; the version bump also ran the 26 engine-client and Python targets.
