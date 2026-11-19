# H13: the error-records spike (2026-11-19)

**What this is.** A scratch prototype of stage B, step B1 (slice 1.2 of `compat-program-plan-2026-11-19.md`): a raise
site carries its kind, its place and its literal operands as typed data; each audience's formatter words it in Java
after the run; upstream words never enter a plan. It is a measuring instrument. It is never proposed for merge.

- **Branch:** `claude/compat-error-records-spike`, from `main` `b75d6c73a`.
- **Session:** `session_0195rQEoAgK9WB7AM5Cowp2f`. **Orchestrator:** `session_0195rQEoSpv1FxC3WndEk5ZH`.
- **Rules kept:** no PR, no force push, `main` untouched. No golden, ceiling, ledger or allowlist is moved by this
  session (section 9 says exactly which guard files the branch differs in, and why).
- **Order:** findings are appended as they are found. The newest is last.

## 1. The base: B1's S1 applies to `main` almost cleanly

- `git cherry-pick 70592a5bb` (B1's S1, from `claude/presentation-b1`) onto `main` `b75d6c73a`: 79 files, +663 / -853.
- **3 conflicts, all trivial:**
  - `edge/EdgeServer.java` and `pct/.../PctPlanRoute.java` named S1m's `Modes.ENGINE.computation()` and
    `Modes.PURE.computation()`. S1m is not carried, so the spike keeps `main`'s `PureText.PURE_V1` and
    `PureText.UPSTREAM`.
  - `ModeBranchCensusTest.java` is S0's (not on `main`). It is not carried.
- No other file of S1 refers to S1m. So **S1's deletions can be re-done on `main` without S0 or S1m** (commit
  `5c34feb2`).

## 2. The record, as prototyped

`com.legend.executionplan.Operand`, a sealed interface, product data:

| Operand | Built from | JSON (one key per operand) |
|---|---|---|
| `Text(String)` | `SqlExpr.StringLit` | `{"string":"..."}` |
| `Int(long)` | `SqlExpr.IntLit` | `{"integer":5}` |
| `Flt(double)` | `SqlExpr.FloatLit` | `{"float":"-1.0"}` (Java's shortest round-trip text, never a JSON number) |
| `Dec(BigDecimal)` | `SqlExpr.DecimalLit` | `{"decimal":"1.500"}` (digits and scale as written) |
| `Bool(boolean)` | `SqlExpr.BoolLit` | `{"boolean":true}` |
| `Day(String iso)` | `SqlExpr.DateLit` | `{"date":"2014-02-29"}` |
| `Instant(String iso)` | `SqlExpr.TimestampLit` | `{"dateTime":"2014-01-01 10:10:10.123"}` |
| `Computed()` | any other expression | `{"computed":true}`: the position is kept, the value is never carried (A3) |

- `ExecutionPlan.Site` is `(n, kind, where, message, List<Operand> operands)`. S1's `List<Extension> extensions` is
  gone.
- `PlanMaker.sites` builds the operands with `Operand.of(SqlExpr)` from `RaiseSite.operands()`. **It asks no style.**
  The record is the same under every style.
- `PlanJson` writes `operands` natively (no codec, no ServiceLoader) and omits the key when a site has none. Its
  reader accepts the key and refuses an operand of any other shape by name.
- Deleted on top of S1: `planner.SiteFacts` (S1's two-form hook), `edge.SiteWords`, the `siteWords` tag of
  `EdgeCodec`, `PureMessages.wordable`, `words` and `wordsOrNull` (the planning-time worder), and the edge dialects'
  choice between a site and a sentence in SQL.

## 3. Wording after the run, as prototyped (run-time raises)

- **Every raise is a site, under every style.** The 12 edge dialects of `PureText` no longer choose between a site
  and legend-pure's sentence written in SQL (`PureMessages.wordable ? site : sentence`). They write the product's
  token. So an edge plan's statements are the product's statements, raise for raise.
- **One function words a raised site:** `PureMessages.worded(kind, List<Operand>)` returns the sentence and two
  counts: `withheld` (printed operands that were computed, A3) and `javaPrinted` (printed literals the SQL sentence
  had the database print: a Float, a Decimal, a DateTime, a Boolean).
- **Both routes already funnel through it.** After S1, `RaisedWords.wordsOf(Raised)` serves pure/v1's shipped runner
  and PCT channel A (`RaisedWords.run`), and a test body's `assertError` (`EdgeHostWords.caught`). The spike changed
  that one function's body: it words from `r.site().kind()` and `r.site().operands()`. No recorder, no
  `PlanStyle.siteWords`, no state, no lookup.
- **The database judge still works.** Under the database judge the host catches the raise and binds its message to
  the verdict statement (`VerdictSql.Raised.bound`). The bound message is the formatter's, worded after the run.
- **A placeholder stands where a computed operand's value was:** `<computed>`. It is the spike's own spelling, not a
  ruling. The register row slice 1.2 names would rule the spelling.

**What stays in SQL, found by the spike (2 things):**

1. **A checked graph's defect cell** (`RaiseText.asData`, kind `CONSTRAINT_UNEVALUABLE`). It is not a raise. It is
   an ANSWER cell whose text the edge dialects write as legend-engine's sentence
   (`Unable to evaluate constraint [id]: data not available - check your mappings`). The spike keeps it
   (`r.asData() != null ? PureMessages.sentence(r) : site(r)`). An error record does not cover it: it needs a typed
   defect cell that the ENGINE formatter words (slice 1.3 or 1.4), or a ruling that the cell says lite's words.
2. **`EngineStyleH2.raiseMessage`** always writes `PureMessages.sentence` (legend-engine's SQL text, for
   `toSQLString`, `planToString` and an activity's SQL). It is the SQL half's one remaining caller. It goes with
   step A2 (the engine-spelling printers), not with this slice. Until A2 lands, `PureMessages.sentence` (57 lines)
   cannot be deleted.

## 4. The hosts (the tab, Python)

**Checked by running each host's own reader over the shared fixture** (`engine-client/test/fixtures/raise-envelopes.json`,
11 cases), with typed operands added to its four sites. The scripts are in section 10.

| Host | Reader | Result |
|---|---|---|
| Python | `legend_lite/_raised.py` (`declare_sites`, `raised_words`, `raised_kind`) | 11 of 11 cases read as before |
| The tab | `engine-client/src/declared-raises.ts`, `raised.ts` (`declareSites`, `failure`) | 11 of 11 cases read as before |
| Java | `exec.RaisedErrors` | unchanged code; `Raised#site` now carries the operands |

- **Why nothing breaks:** neither host checks a site's key set. Python checks `n`, `kind` and `message`
  (`_sites_of`). The tab casts. Both read `message` for the words, so a LITE raise says exactly what it said.
- **One rule that must be kept:** each host refuses a statement whose sites are declared two different ways
  (`JSON.stringify` / `json.dumps` equality). An ENGINE answer declares a statement's sites twice: from its `sites`
  view and from the plan beside it. **Checked:** a view written without the operands is refused by name by both
  hosts. So `PlanJson.sitesOf` must write a site exactly as the plan does. It does, with no codec: the operands are
  product data, so S1's two explicit-codec overloads (`write(plan, codec)`, `sitesOf(plan, statement, codec)`) are
  not needed.
- **What changes for the hosts in this slice:** only `PLAN_VERSION` (58 to 59), in `plan-runner.ts` and `_runs.py`.
- **A gap this slice does not close, found here:** on the hosts' ENGINE routes (`wasm-edge.ts`,
  `edge/_pure_v1.py`) a raise is worded from `site.message`, which is LITE's sentence. That is already so on `main`
  for every raise the edge words at planning (its words sit in a `siteWords` table beside the plan that no host
  reads). On `main` only the computed-operand raises say legend-pure's words on a host, because their sentence is
  in the SQL. After this slice every raise on a host's ENGINE route says lite's sentence, until slice 1.6 moves the
  ENGINE error half into Java. Java's own pure/v1 route is not affected.

**Would lite's own sentence leaving the plan later (slice 1.6, plan v60) break anything?** Yes, in a small, known
set of places, all of which that slice must change together:

| Where | What breaks if `message` and the plan-level `raises` simply go |
|---|---|
| `_raised.py` `_sites_of` | refuses every site: it requires `message` to be a string |
| `_raised.py` `raised_words`, `raised.ts` `failure` | read `site['message']` / `site.message` for the words |
| `_raised.py` `declare_sites`, `declared-raises.ts` `declareSites` | refuse an answer that carries no `raises` map |
| `PlanJson.readSites` | checks the site's `message` against its kind's sentence |
| `exec.RaisedErrors.dataErrorOf` and `dataError` | build `DataError` from `site.message()` |
| `raise-envelopes.json` | each site states its `message`; Java's test holds it to `ErrorKind` |
| `edge.HostAnswers` | writes `raises` into both ENGINE answers |
| tests that build sites by hand | `plan-runner.test.ts` (2 places), `test_planned.py` (2 plans with `raises`) |

- The rule the generated table must carry is one line: `ErrorKind.sentence + (where == null ? "" : ": " + where)`,
  over 27 raise kinds (`ExecutionPlan.Site.sentence`). Every reader of a planner's answer on both hosts goes through
  two functions (10 call sites on the tab and in DataCube, 9 in Python), so the change is contained.
- **Nothing in the operands design blocks it.** The operands do not depend on `message`.

## 5. The first run of the edge's core tests (`//core:core_edge_tests`)

1,011 tests: **991 pass, 20 fail.** Every failure is the design working as ruled, or a test of the deleted
mechanism:

| Count | Tests | Why |
|---|---|---|
| 6 | `AssertErrorNativeTest` | `[1,2]->at(3)`: legend-pure's sentence prints the collection's size, which the lowering computes even for a literal list. After: `... at offset 3 where the collection is of size <computed>` |
| 12 | `MultiplicityStrictnessTest` (2), `RelationToOneGuardTest` (1), `OptionalCollectionNullDropTest` (2), `ToOneLaneTest` (7) | `toOne` over a collection of another size. The size is computed. These tests also call the door without the formatter (`RaisedWords.run`), which worked only while the sentence was in the SQL |
| 1 | `InterimEdgeSitesTest` | the spike's own first draft of the measure (fixed) |
| 1 | `PureV1ApiTest.m2_theHostsPureV1HalvesRaiseInPuresWords` | read S1's `extensions` on a site (adapted) |

## 6. THE CHECK, first run: PCT on DuckDB, channels A and B, all five suites

Run: the net's own recordings (`//pct:edge_norm_duckdb_*`, `//pct:edge_norm_channel_b_duckdb_*`), 3 min 22 s, diffed
against the goldens under `pct/src/test/resources/edge/`. Raise sites only (the planning-failure half was not in
this run).

| Golden | Lines | Differing lines |
|---|---|---|
| channel A answers, Essential | 847 | **6** |
| channel A answers, Grammar | 554 | 0 |
| channel A answers, Relation | 479 | **2** (SQL text only) |
| channel A answers, Standard | 708 | 0 |
| channel A answers, Unclassified | 122 | 0 |
| channel A outcomes, all five suites | 1,165 | **6** (the same 6 tests) |
| channel B outcomes, Essential | 345 | **6** (the same 6 tests) |
| channel B outcomes, the other four suites | 904 | 0 |

**The 6 tests whose message changes (A3: a computed operand's value is withheld).** All are in Essential. Each
passes on `main` and fails after, on both channels:

| Test | Before (the pinned line) | After | Why the operand is "computed" |
|---|---|---|---|
| `testAtError` | `The system is trying to get an element at offset 2 where the collection is of size 2` | `... of size <computed>` | the size is `LIST_LENGTH(...)` of the list, a call, even over a literal list |
| `testDayOfMonthError` | `Cannot get day of month for 2017` | `Cannot get day of month for <computed>` | the date is `Cast(StringLit '2017', TEMPORAL_TEXT)`: a literal inside a cast |
| `testHourError` | `Cannot get hour for 2017` | `Cannot get hour for <computed>` | the same |
| `testMinuteError` | `Cannot get minute for 2017` | `Cannot get minute for <computed>` | the same |
| `testSecondError` | `Cannot get second for 2017` | `Cannot get second for <computed>` | the same |
| `testSquareRootError` | `Unable to compute sqrt of -1.0` | `Unable to compute sqrt of <computed>` | the operand is `Cast(FloatLit -1.0, DOUBLE)`: a literal inside a cast |

- **In every one of the 6, the query wrote a literal.** The value is lost only because the lowering hands the raise
  a wrapped expression, not the literal it was given. None is a true run-time value.
- **So the true A3 cost on DuckDB's PCT is 0 tests, if the lowering states what it knows.** That is a change in
  lite's own lowering (three raise sites: `sqrt`, the date-part reads, `at`), with no hook. Section 7 measures it.

**The 2 Relation lines** (`testFlatten_LateralJoin_Nested_Extend`, `testVariantColumn_slice`) differ only in the SQL
text the answer line quotes: `error(... concat('The system is trying to get an element at offset ', CAST(0 AS
VARCHAR), ...))` became `error(... '\u001cLG#1' ...)`. Their rows are the same. This is the design's intent (no
upstream sentence in a statement). B1's S0 takes the SQL out of the answer line; on `main` it is still in.

**Every other pinned message is byte-identical**, including the raises worded after the run from literal operands.
The formatter worded 40 raises in this run:

| Kind | Raises | Exact | Example |
|---|---|---|---|
| `DATE_PART_ABSENT` | 12 | 0 | the 4 tests above, 3 runs each |
| `DATE_COMPONENT_INVALID` | 7 | 7 | `Invalid day: 2016-12-32`, `Invalid month: 13`, `Invalid hour: 24` |
| `SHIFT_OUT_OF_RANGE` | 6 | 6 | `Unsupported number of bits to shift - max bits allowed is 62` |
| `SQRT_OF_NEGATIVE` | 3 | 0 | above |
| `SLICE_BOUNDS_INVERTED` | 3 | 3 | `The low bound (3) can't be higher than the high bound (2) in a slice operation` |
| `RANGE_STEP_ZERO` | 3 | 3 | `range step must not be 0` |
| `INDEX_OUT_OF_BOUNDS` | 3 | 0 | above |
| `DIVIDE_BY_ZERO` | 3 | 3 | `Cannot divide 5 by zero` |

**The site census of the same run** (every site planned, fired or not; 438 sites, all in the edge's style):

| Kind | Sites | Operand shapes |
|---|---|---|
| `RANGE_STEP_ZERO` | 157 | none |
| `LIST_LENGTHS_DIFFER` | 56 | `StringLit` (placed) |
| `DIVIDE_BY_ZERO` | 68 | none (14), `IntLit` (33), `FloatLit` (14), `DecimalLit` (7) |
| `INDEX_OUT_OF_BOUNDS` | 33 | `IntLit, Column` (21), `IntLit, Call` (12) |
| `SQRT_OF_NEGATIVE` | 30 | `Cast(FloatLit)` (16), `Cast(IntLit)` (14) |
| `SIZE_NOT_MULTIPLICITY` | 25 | `StringLit` (placed) |
| `DATE_PART_ABSENT` | 20 | `StringLit, Cast(StringLit)` |
| `SLICE_BOUNDS_INVERTED` | 19 | `IntLit, IntLit` (12), `Call, Call` (7) |
| `DATE_COMPONENT_INVALID` | 13 | `StringLit, IntLit` (9), `StringLit, IntLit, IntLit, IntLit` (4) |
| `SHIFT_OUT_OF_RANGE` | 10 | none |
| `INSERT_OUT_OF_BOUNDS` | 7 | none |

- 91 of the 438 planned sites (21%) have a computed operand in a position legend-pure's sentence prints: 33
  `INDEX_OUT_OF_BOUNDS`, 30 `SQRT_OF_NEGATIVE`, 20 `DATE_PART_ABSENT`, 7 `SLICE_BOUNDS_INVERTED` and (2-operand form)
  0 `SIZE_NOT_MULTIPLICITY`. Only 18 raises of those sites fire in a test that reads the message (the 6 tests).
