# H13: the error-records spike (2026-11-19)

**What this is.** A scratch prototype of stage B, step B1 (slice 1.2 of `compat-program-plan-2026-11-19.md`): a raise
site carries its kind, its place and its literal operands as typed data; each audience's formatter words it in Java
after the run; upstream words never enter a plan. It is a measuring instrument. It is never proposed for merge.

- **Branch:** `claude/compat-error-records-spike`, from `main` `b75d6c73a`.
- **Session:** `session_0195rQEoAgK9WB7AM5Cowp2f`. **Orchestrator:** `session_0195rQEoSpv1FxC3WndEk5ZH`.
- **Rules kept:** no PR, no force push, `main` untouched. No golden, ceiling, ledger or allowlist is moved by this
  session (section 19 says exactly which guard files the real slice would move, and why).
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
11 cases), with typed operands added to its four sites. The two scripts are filed as `H13/hosts-check.py.txt` and
`H13/hosts-check.mjs.txt`.

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
  ENGINE error half into Java. Java's own pure/v1 route is not affected. **(Corrected in section 17: it is, for every
  site whose operands were literals, already on `main`.)**

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

- 90 of the 438 planned sites (21%) have a computed operand in a position legend-pure's sentence prints: 33
  `INDEX_OUT_OF_BOUNDS`, 30 `SQRT_OF_NEGATIVE`, 20 `DATE_PART_ABSENT`, 7 `SLICE_BOUNDS_INVERTED` and (2-operand form)
  0 `SIZE_NOT_MULTIPLICITY`. Only 18 raises of those sites fire in a test that reads the message (the 6 tests).

## 7. The literal kinds `RaiseSite.operands()` produces today, and whether `Operand` carries each

Two sources, which agree: a static read of every construction site (the whole table, site by site, with the code
quoted, is `H13/raise-operand-census.md`, written by a subagent of this session and spot-checked here at 4 sites),
and the run-time census of section 6.

**Static counts** (at `main` `b75d6c73a`; no file under `lowering/`, `sql/` or `sql/dialect/` is changed by the spike):

| | Construction sites | Operand positions | Always a String literal | Always an Integer literal | Always a Float or Decimal literal | Always computed | Either (a lowered argument) | Sites with no operand |
|---|---|---|---|---|---|---|---|---|
| The lowering | 26 | 37 | 18 | 4 | 1 | 6 | 8 | 4 |
| A dialect, at render | 25 | 23 | 12 | 0 | 0 | 2 | 9 | 3 |
| **Total** | **51** | **60** | **30** | **4** | **1** | **8** | **17** | **7** |

- 15 sites are `RaiseText.placed` (their one operand is the where). 1 site is `asData` (the defect cell of section 3).
- No package other than `lowering/` and `sql/dialect/` builds a raise.

**Each literal kind against the record:**

| SQL literal | Reaches an operand today? | `Operand` |
|---|---|---|
| `StringLit` | yes: labels, type names, bounds, the where (30 fixed positions, and any "either" position) | `Text` |
| `IntLit` | yes: `rem`'s dividend, `at`'s offset, `slice`'s bounds, a date's components | `Int` |
| `FloatLit` | yes: `rem`'s dividend, a date's fractional second | `Flt` |
| `DecimalLit` | yes: a Float literal the lowering made exact (`rem`'s dividend), an Integer past 64 bits | `Dec` |
| `DateLit` | only through `DATE_PART_ABSENT`'s date | `Day` |
| `TimestampLit` | only through `DATE_PART_ABSENT`'s date | `Instant` |
| `BoolLit` | **no position is Boolean today** | `Bool` (carried, unused) |
| `NullLit`, `TimeLit`, `ArrayLit`, `StructLit`, `DatePartLit`, `FormatLit`, `TypeNameLit`, `JsonKindLit` | none is built as an operand at any site | `Computed` (the default arm) |
| anything else (a column, a call, a cast, a parameter) | yes | `Computed` |

**The record carries every literal kind that occurs.** `Bool` is the only variant with no producer.

**Which kinds can lose a printed value.** 27 raise kinds. 15 cannot (no operand, or planner-known text only). 12
have a position legend-pure's sentence prints that can be computed:

| Kind | Position at risk | Note |
|---|---|---|
| `DIVIDE_BY_ZERO` | the dividend (either) | `rem` and `quotient` only; `Integer / Integer` has no operand |
| `DATE_PART_ABSENT` | the date (either) | the raise is written when planning knows the date's precision: a date literal (wrapped in a cast), a `date(...)` constructor call, or a `StrictDate` column asked for a time part. The last two are truly computed |
| `INDEX_OUT_OF_BOUNDS` | the offset (either), the size (always computed) | the size is `LIST_LENGTH(...)` |
| `SQRT_OF_NEGATIVE` | the value (always computed) | always `Cast(argument, DOUBLE)` |
| `SIZE_NOT_MULTIPLICITY` | the size (always computed), in the 2-operand form | `toOne` over a relation or a list; a row count |
| `SLICE_BOUNDS_INVERTED` | both bounds (either) | each clamped at 0 |
| `DATE_COMPONENT_INVALID` | the value, at 1 of 4 sites | the other three are all literal |
| `TEXT_NOT_INTEGER`, `TEXT_NOT_FLOAT`, `TEXT_NOT_DATE` | the text (either) | a dialect's own cast errors; always computed on SQLite for `parseFloat` |
| `FRACTION_NOT_KEPT` | the value, at 1 site | lite's own sentence, no upstream text |
| `USER_FAIL` | the message | never a site: the statement raises the user's own message, in every style |

**Four things the static read found that the real slice must decide:**

1. **A literal's value is not always what the query wrote.** `slice`'s bounds are clamped at 0 (`slice(3, -1)`
   holds `IntLit(0)`). A `parseDate` literal has its `T` replaced by a space. An hour- or minute-precision date
   literal is a padded `TimestampLit`, spelled in the database's time zone when one is set. A Float literal can be a
   `DecimalLit`. The operand records what the STATEMENT holds, which is what upstream's SQL sentence printed too.
2. **An operand can contain another raise** (`FRACTION_NOT_KEPT`'s value is the guarded `CASE` that holds
   `DATE_COMPONENT_INVALID`). `Computed` covers it.
3. **`LIST_LENGTHS_DIFFER`'s text is a Java `List.toString()`** of function ids, not a native's name
   (`Scalars.java:2146`). It is lite's own sentence, but it is a planner-internal spelling in a message.
4. **Six raises are not a `RaiseText` at all** (`VerdictSql.java:1915`, `CanonicalRenderSql.java:668`, four in
   `MongoDb.java`). They make no site and carry no operands; `RaiseTextGuardTest` exempts them.

## 8. Planning failures, as prototyped

**Today (`main`).** `PlanStyle.errorWords(e)` has ONE product call site: `testplan/AssertErrorNative.caught`. It asks
the plan's style to word an error that `assertError`'s function raised while it was PLANNED, and stores the words in
the test plan (`Failure.message`). The edge's answer is `PureMessages.compile(e)`, which finds the kinded error in
the exception's cause chain and splices upstream's sentence into lite's message by string position
(`message.lastIndexOf(own)`). pure/v1's routes call `PureMessages.compile` directly, on the live exception, at the
edge's own door. No plan is involved there.

**The prototype.**

- `Failure` (the test plan's record of a planned error) gains `Compile compile`:
  `(prefix, kind, operands, position, suffix)`. `kind` is lite's `CompileKind`. `prefix` and `suffix` are LITE'S
  WRAPPER around the kinded sentence, as data: `"in function 'f': "` before (the body compiler,
  `SpecCompiler.java:109`) and `" [inlined via a -> b]"` after (the inliner, `UserCallInliner.java:604`).
- `Failure.message` is always lite's own message. The record checks that
  `prefix + [position + " "] + kind.sentence(operands) + suffix` spells it exactly, so the data cannot drift from
  the words.
- The product builds the record (`Failures.compileOf(Throwable)`). The edge words it:
  `PureMessages.planned(record) = prefix + upstream(kind, operands, position) + suffix`.
- **No new hook.** At the verdict's turn the run rebuilds the planned exception as it always did
  (`Failures.raised`), now with the record riding it as its cause, and words it through the EXISTING
  `HostWords.caught(RuntimeException)`. Lite's words say the message. The edge's words read the record.
- `PureMessages.compile(Throwable)` (pure/v1's routes) now reads the same record from the live exception, so the
  splice by position exists in one place, in the product, and the edge only words data.
- **Deleted:** `PlanStyle.errorWords` and its three implementations.
- The test plan's JSON writes `compile` only where a failure has one (test plan v34 in the real slice).

**What the spike did NOT do, and the real slice should:** the wrapper is still FOUND by position, now in the
product (`Failures.compileOf`). The two wrapping sites should state their own texts (a wrapping exception that
holds its prefix or suffix), so nothing is searched. That is 2 throw sites and one small exception field.

**A dependency found here: the compatibility DATABASE judge.** Under it, a planned failure's message is planted as a
literal in the verdict statement at PLANNING (`testplan/DatabaseAsserts.java:387`,
`VerdictSql.Raised.raised(caught.message())`), and the database compares it with the expected message. With lite's
sentence in `Failure.message`, that literal is lite's sentence, and an `assertError` expecting legend-pure's words
fails under the compatibility database judge wherever the two differ. So **deleting `PlanStyle.errorWords` needs
slice 2.2 (C4: the compatibility database judge deleted) first, or the host judge only.** Run-time raises are not
affected: the host catches the raise, words it after the run and binds the words.

**The test plan's other planned words (not prototyped; they are in slice 1.2's list, not in this spike's):**

| Field | Holds | To become data |
|---|---|---|
| `SideShape.sizeWords` | under the edge, legend-pure's `Cannot cast a collection of size 0 to multiplicity [1]`; raised as it is by `testexec/SideShapes.java:262` | the bound is already in the record (`SideShape.multiplicity`). The run would raise a typed refusal and the run's words would word it |
| `TestPlan.answerSizeWords` | the same sentence for the body's answer | the same |
| `TestPlan.words` | a NAME (`lite` or `legend-pure`): which words the plan's failures were planned in | goes when no words are planned |

- These are planned through the facet member `EngineTestText.sizeWords` and `EngineTextGates.sizeWords`, both of
  which would be deleted.
- **Wording them after the run needs one new member on the existing `HostWords` interface** (the run's word hook,
  census J29), for example `noRows(multiplicity)`. No existing member says this sentence. That is a widening of an
  existing hook's surface, not a new hook, and it deletes a facet member in exchange. **It is a question for the
  orchestrator before it is built.**

## 9. THE CHECK, second run: the whole PCT net and pure/v1, with the planning-failure half in

Run: every PCT lane of the net (DuckDB 5 suites, SQLite 5 suites, H2 Relation, channel B on DuckDB 5 suites) and
`//core:edge_pure_v1`, 5 min 9 s, diffed against `main`'s goldens.

| Golden | Lines | Differing lines | What differs |
|---|---|---|---|
| DuckDB channel A answers (5 suites) | 2,710 | 8 | the 6 Essential tests of section 6; 2 Relation lines' quoted SQL |
| DuckDB channel A outcomes (5 suites) | 1,165 | 6 | the same 6 tests |
| SQLite channel A answers (5 suites) | 2,702 | 8 | **the same 6 tests**, the same 2 SQL lines |
| SQLite channel A outcomes (5 suites) | 1,165 | 6 | the same 6 tests |
| H2 channel A answers and outcomes (Relation) | 880 | **0** | |
| DuckDB channel B outcomes (5 suites) | 1,249 | 10 | the same 6 tests; **4 Relation tests, a new cause (below)** |
| pure/v1's answers | 28 | **0** | every body byte-identical, the error bodies included |

**pure/v1's error bodies.** The golden holds 9 error answers (status 500) with 5 distinct bodies:

| Body | Answers | What it is |
|---|---|---|
| `execute: a SCALAR result's serialization is unprobed` | 1 | a compile refusal with no kind |
| `LIST_FILTER reached a dialect without a list encoding` | 4 | a compile refusal with no kind |
| `LIST_GET reached a dialect without a list encoding` | 2 | a compile refusal with no kind |
| `JdbcSQLDataException: Division by zero: ...` | 1 | H2's own error: legend-engine writes no guard there, so no site |
| `JdbcSQLDataException: Data conversion error converting "abc" ...` | 1 | H2's own error, the same |

- All 9 are byte-identical.
- **The golden exercises neither half of this slice.** None of the 5 bodies is a site raise, and none holds a
  kinded compile error. It proves only that the routes' plain messages still pass through unchanged.
- The kinded and wrapped compile sentences are checked by the core edge tests instead (`PlanStyleFacetsTest`,
  `ReferenceIntegrityTest`, `DecimalKindsTest`, `PureV1ApiTest`, `TestDataPlannerTest`); section 10 has their result.
- **A gap in the net, for the orchestrator:** pure/v1's golden has no request that raises a site with a literal
  operand and none that raises a kinded compile error. One of each would make it a check of this slice.

**The 4 new channel B differences: the compatibility DATABASE judge.**

| Test | Before | After |
|---|---|---|
| `relation::tests::over::testRange_InvalidWindowFrameBoundary` | PASS | `JudgesDisagree`: host `assertError` held; database `assertError` failed, the actual message `window frame starts after it ends` |
| `relation::tests::over::testRows_InvalidWindowFrameBoundary` | PASS | the same |
| `relation::tests::reduce::testRange_InvalidWindowFrameBoundary` | PASS | the same |
| `relation::tests::reduce::testRows_InvalidWindowFrameBoundary` | PASS | the same |

- These are section 8's dependency, measured. The frame's refusal is raised at PLANNING (`WINDOW_FRAME_INVERTED`, a
  kinded compile error, no wrapper). The host judge words it after the run from the record and holds, byte for
  byte. The compatibility database judge planted the planned failure's message in its verdict statement at
  planning, and that message is now lite's sentence.
- Channel B runs under both judges, so the two disagree. Channel A's outcomes for the same 4 tests are unchanged.
- **It is the only planning failure in PCT whose words differ between lite and legend-pure.** The census over PCT
  found 8 planned failures an `assertError` catches: these 4 (kinded, upstream's words differ) and 4 un-kinded
  `ModelException`s whose message is already legend-pure's sentence inside the product
  (`Unsupported duration unit for StrictDate. ...` 3 tests, `The type ... is not supported yet!` 1 test). The second
  group is the census's "upstream sentence inside the product" family. This slice does not touch it.
- **No wrapped planning failure occurs in PCT.** The wrapper (`in function 'f': ...`, `[inlined via ...]`) is
  exercised by the core edge tests and by pure/v1's golden, both of which pass.

**The raises worded after the run, whole net (62 raises):** `DATE_PART_ABSENT` 16 (all withheld),
`VALUE_NOT_HELD` 10, `SHIFT_OUT_OF_RANGE` 8, `DATE_COMPONENT_INVALID` 8, `SQRT_OF_NEGATIVE` 4 (withheld),
`SLICE_BOUNDS_INVERTED` 4, `RANGE_STEP_ZERO` 4, `INDEX_OUT_OF_BOUNDS` 4 (withheld), `DIVIDE_BY_ZERO` 4. So 38 are
exact and 24 are withheld, and the 24 are the 6 tests on two databases and two channels. **No raise in the net
prints a literal that Java prints where the database printed it before** (`javaPrinted` is 0 on all 62): a Float, a
Decimal or a DateTime literal in a printed position does not occur in PCT.

**The count the brief asked for.** Raises whose upstream sentence prints a computed operand, by test:

| Test | Kind | Databases | Channels | A true run-time value? |
|---|---|---|---|---|
| `testAtError` | `INDEX_OUT_OF_BOUNDS` | DuckDB, SQLite | A, B | no: `[1, 2]->at(2)` (at.pure:72) |
| `testDayOfMonthError` | `DATE_PART_ABSENT` | DuckDB, SQLite | A, B | no: `%2017` |
| `testHourError` | `DATE_PART_ABSENT` | DuckDB, SQLite | A, B | no: `%2017->hour()` (hour.pure:50) |
| `testMinuteError` | `DATE_PART_ABSENT` | DuckDB, SQLite | A, B | no: `%2017` |
| `testSecondError` | `DATE_PART_ABSENT` | DuckDB, SQLite | A, B | no: `%2017` |
| `testSquareRootError` | `SQRT_OF_NEGATIVE` | DuckDB, SQLite | A, B | no: `sqrt(-1.0)` (sqrt.pure:54) |

**6 tests. 0 of them raise over a value known only at run time.** H2's Relation lane has none.

## 10. Lite's own plan: what the operands add (the orchestrator's item 1)

**The plan's check said "the product plan's JSON byte-identical but its version". It is not**, and the orchestrator
has amended the done-when (2026-11-19 ~13:10 UTC): a LITE plan carrying the operands is the design, since both
audiences word from the record. `RaiseSite.operands()` is the same under every dialect, so a product plan's sites
carry the same operands an edge plan's do.

**Measured with the product's own gauge**, `//core:lean_sql_baseline_test` (`core/src/test/resources/lean-sql/baseline.tsv`:
3,777 keys, each a set, a database and a family of queries, planned in LITE's style):

| Measure | Baseline | After | Change |
|---|---|---|---|
| SQL characters, all keys | 14,200,461 | 14,200,461 | **0** (no key moved: the product already raises the token) |
| Plan JSON characters, all keys | 147,464,602 | 147,555,033 | **+90,431 (+0.061%)** |
| Keys whose plan JSON moved | | | 281 of 3,777 (7.4%), in 45 families |

By set:

| Set | Keys | Plan JSON before | Growth |
|---|---|---|---|
| `channel_a` (PCT's queries, 11 databases) | 3,421 | 112,935,654 | +84,186 |
| `core` | 356 | 34,528,948 | +6,245 |

By database (the keys that moved):

| Database | Keys moved | Growth | Of those keys' plan JSON |
|---|---|---|---|
| SQLite | 39 | +24,931 | +0.39% |
| ClickHouse | 25 | +15,567 | +0.49% |
| MariaDB | 27 | +11,429 | +0.34% |
| MySQL | 27 | +11,243 | +0.33% |
| DuckDB | 42 | +8,182 | +0.05% |
| Databricks | 22 | +3,573 | +0.49% |
| BigQuery | 22 | +3,571 | +0.48% |
| Postgres | 22 | +3,571 | +0.46% |
| Snowflake | 21 | +3,442 | +0.45% |
| Trino | 21 | +3,442 | +0.48% |
| H2 | 13 | +1,480 | +0.09% |

The largest growth, by family (summed over its databases) and by single key:

| Family | Growth | Databases | Note |
|---|---|---|---|
| `date::adjust(Date, Integer, DurationUnit)` | +26,719 | 4 | the largest single key: ClickHouse, 210,588 to 220,213 (+4.6%) |
| `TypedExtendWindow` | +10,634 | 4 | SQLite alone: 1,846,860 to 1,856,956 (+0.5%) |
| `math::covarSample` | +7,240 | 10 | largest case `[1, 3.0, 6.0]->covarSample([10, -20, 30])` |
| `math::covarPopulation` | +5,859 | 11 | |
| `math::corr` | +5,511 | 11 | |
| `date::dateDiff` | +4,858 | 4 | |
| `string::parseFloat` | +2,966 | 12 | |
| `math::rem` | +2,454 | 11 | largest case `5.5->rem(-1.5)` |
| `collection::at` | +1,845 | 11 | |
| `math::sqrt` | +1,395 | 12 | largest case `sqrt(-1.0)` |

- The largest relative growth of one key is +5.7% (SQLite `math::acos` and `math::asin`, 6,839 to 7,231): small
  plans whose guards are placed raises (`VALUE_NOT_HELD`).
- **The gauge is a guard.** `LeanSqlBaselineTest` is shrink-only (G3) and goes red on any growth. The real slice
  re-records `baseline.tsv`'s plan column, with the orchestrator's amended done-when as its dated reason.

## 11. The six tests: the lowering hands the raise the literal it was given (session 2, task 1)

**Session 2** (`session_0195rQEjNZoybxirC5uGba42`, continuing `session_0195rQEoAgK9WB7AM5Cowp2f`'s spike, 2026-11-19 from ~13:20 UTC, from the branch's head
`dad98589`). The orchestrator's calls of 13:15 UTC are its tasks.

**The three sites, as changed** (`lowering/Scalars.java`, `lowering/TaggedLists.java`; 33 changed lines in the
lowering, none in `sql/` or a dialect):

| Site | Before | After | What the lowering reads | Invariant 2 |
|---|---|---|---|---|
| `sqrt` (`Scalars`, `AT_MATH_SQRT`) | the operand is `x`, the rule's own `Cast(args[0], DOUBLE)` | the operand is `args[0]`, the argument as it was lowered (a literal stays one) | nothing: the rule passes the node it was handed instead of the one it built | no dispatch at all |
| the date parts (`Scalars`, `DATE_PART_ABSENT`, 6 natives) | the date is `args[0]`, its carrier (`Cast(StringLit '2017', TEMPORAL_TEXT)` for a partial date, a padded `TimestampLit` for an hour or minute date) | a date LITERAL is its text as written (`dateLiteralText(n.args()[0])`, a `StringLit` of `PureDateLiteral.canonicalText`); any other date is `args[0]` | the TYPED SPEC: the same `TypedCDate` node the rule already reads to decide that it raises (`datePrecision`), through the helper the `toString` rule already uses (`Scalars.java:2374`) | structural (is the argument a literal?), not type dispatch: the type is a date either way, and no cast is inspected |
| `at` (`Scalars`, `AT_COLLECTION_AT`; `TaggedLists.at`, a mixed list) | the size is `LIST_LENGTH(list)` | the size is `statedSize(n.args()[0], LIST_LENGTH(list))`: `IntLit(k)` where the typed multiplicity is exactly `[k]` (a literal list), else the count | the TYPED SPEC's multiplicity annotation | "MAY: read type annotations"; the same rule already reads it (`isToOne`) |

- **The guard keeps its count.** Only the raise's OPERAND changes: the out-of-bounds test is still `LIST_LENGTH` in SQL,
  and `sqrt`'s test still reads its cast. No SQL statement changes (the product writes the site's token; the operands
  are never rendered).
- **No cast is inspected anywhere, and no value is read at run.** Condition (a) holds. So the stop condition (a site
  needing the lowering to look inside a cast for its literal) was not met: none of the three needs it.
- **Is the typed spec the cleaner place? Yes, for two of the three.** For the date and the size, the fact is the
  typed spec's (the literal node; the multiplicity the frontend computed, invariant 1), and the lowered expression holds
  only its carrier. For `sqrt`, the lowered argument already is the literal: the wrap was the rule's own.
- **The formatter, one arm** (`edge/PureMessages.java`): `DATE_PART_ABSENT`'s date is printed as legend-pure's
  `DateFormat.append` prints a date (read at the pin, legend-pure `7fbc7d6e`:
  `legend-pure-m4/.../date/DateFormat.java:237-267`: the year, then `-MM`, `-DD`, `THH`, `:mm`, `:ss`, `.subsecond`,
  and `+0000` written inside the minute branch), through the edge's existing printer `EdgePureWords.pureText`, from
  the literal's text parsed back (`PureDateLiteral.parse`, whose round trip is the type's stated identity). The SQL
  half (`PureMessages.sentence`, `EngineStyleH2`'s last caller) re-applies `sqrt`'s cast itself, so its SQL text is
  unchanged.
- **The record's typing, a question for the real slice.** The date reaches the record as `Operand.Text`; that it is a
  date is the kind's contract (`DATE_PART_ABSENT`'s operands are `[the part's label, the date]`), not the operand's
  type. A typed `Operand.Date` would need a MIR literal for a Pure date at its precision (a new `SqlExpr` variant and a
  render arm per dialect) or typed operands on `RaiseText` beside its SQL ones. The spike does neither.

**Upstream's sentences, read at the pins** (legend-pure `7fbc7d6e`, `legend-pure-runtime-java-engine-compiled`):

| Kind | legend-pure (compiled) | The operand it prints |
|---|---|---|
| `SQRT_OF_NEGATIVE` | `CoreHelper.java:520`: `"Unable to compute sqrt of " + input`, `input` a Java `double` (`:510-513` converts any `Number`) | `sqrt(-1)` prints `-1.0`; `sqrt(-1.0)` `-1.0`. The interpreted runtime prints `input.toString()` (`Sqrt.java:53`: `-1` for an Integer): the two runtimes differ, and PCT's one test (`sqrt.pure:54`, `-1.0`) cannot tell them apart |
| `DATE_PART_ABSENT` | `CoreHelper.java:266` and `:280` (`dayOfMonth`, `hour`; `minute`, `second` alike): `"Cannot get day of month for " + date` | `PureDate.toString` = `DateFormat.append` (above) |
| `INDEX_OUT_OF_BOUNDS` | `CompiledSupport.java:225-273`: `"... at offset " + offset + " where the collection is of size " + size` | the collection's size |

**THE CHECK, third run** (the whole net as in section 9: PCT DuckDB 5 suites, SQLite 5, H2 Relation, channel B on
DuckDB 5, and `//core:edge_pure_v1`; this session's two code changes of sections 11 and 12 together; diffed against
the goldens in the tree, which the spike does not move):

| Golden | Lines | Differing lines, section 9 | Differing lines, now |
|---|---|---|---|
| DuckDB channel A answers (5 suites) | 2,710 | 8 | **2** (the 2 Relation lines' quoted SQL) |
| DuckDB channel A outcomes (5 suites) | 1,165 | 6 | **0** |
| SQLite channel A answers (5 suites) | 2,702 | 8 | **2** (the same 2 lines) |
| SQLite channel A outcomes (5 suites) | 1,165 | 6 | **0** |
| H2 channel A answers and outcomes (Relation) | 880 | 0 | **0** |
| DuckDB channel B outcomes (5 suites) | 1,249 | 10 | **0** |
| pure/v1's answers | 28 | 0 | **0** |

- **The 6 tests pass on both databases and both channels**, byte for byte against their pinned lines. Condition (b)
  holds: the net decided it.
- **The 2 Relation lines per database are the only difference left** (`testFlatten_LateralJoin_Nested_Extend`,
  `testVariantColumn_slice`): the SQL the answer line quotes holds the site's token (`'\u001cLG#1'`) where it held
  `concat('The system is trying to get an element at offset ', ...)` and `concat('The low bound (', ...)`. Their rows
  are the same. As section 6 says, this is the design's intent; B1's S0 takes the SQL out of the answer line.
- **The raises worded after the run** (channel B's log, 26 raises: the channel A lanes came from the action cache,
  section 13 has their log): every one exact, **0 withheld, 0 printed by Java where the database printed before**.
  `DATE_PART_ABSENT` 8 (`Text 2017` each), `DATE_COMPONENT_INVALID` 6, `SHIFT_OUT_OF_RANGE` 4, `SQRT_OF_NEGATIVE` 2
  (`Flt -1.0`), `INDEX_OUT_OF_BOUNDS` 2 (`Int 2, Int 2`), `SLICE_BOUNDS_INVERTED` 2, `RANGE_STEP_ZERO` 2,
  `DIVIDE_BY_ZERO` 2.
- **So A3's true cost on PCT is 0 tests**, as section 6 predicted: no PCT test raises over a value known only at run.

**What changes beyond PCT** (not in any golden; read, not run):

| Case | main (the SQL sentence) | Section 9's spike | Now | legend-pure |
|---|---|---|---|---|
| `%2017-01-01T10->minute()` | `Cannot get minute for 2017-01-01 10:00:00` (the padded timestamp, the database's text) | the same, Java's text of the `Instant` | `Cannot get minute for 2017-01-01T10` | `2017-01-01T10` |
| `%2017-01-01T10:30->second()` | `... for 2017-01-01 10:30:00` | the same | `... for 2017-01-01T10:30+0000` | `2017-01-01T10:30+0000` |
| `%2017-01-01->hour()` | `... for 2017-01-01` | the same | the same | the same |
| `date(2017)->hour()` (the constructor) | the database's text of the computed date | `<computed>` | `<computed>` | `2017` |
| `$strictDateColumn->hour()` | the database's text | `<computed>` | `<computed>` | the value |
| `[1, 2]->filter(...)->at(5)` | the database's count | `<computed>` | `<computed>` | the count |

- The first two move TOWARD legend-pure (main printed a timestamp's database text). The last three stay withheld:
  true run-time values (A3).
- **`SIZE_NOT_MULTIPLICITY` is not one of the three, and stays withheld:** its size is built by the DIALECT at render
  (`AnsiSqlRenderer.checkedOne`: `COALESCE(LIST_LENGTH(list), 0)`) from a `SqlExpr.CheckedOne`, which carries no
  typed spec. Stating it would need a MIR field (`CheckedOne`'s stated size, set by the lowering from the
  multiplicity). Not built. It is section 5's 12 core edge tests (`toOne` over a list of another size); no PCT test.

## 12. The database judge's 4 tests: the planned failure's words bound at the verdict's turn (task 2)

**The minimal change, prototyped (no hook, no new member; +15 / -4 lines in two product files):**

1. `testplan/DatabaseAsserts.java` (the `ae.caught()` arm of `errorVerdict`): the verdict statement no longer plants
   the planned failure's message as a literal (`VerdictSql.Raised.raised(caught.message(), null, null)`). It BINDS it,
   as it already does for a run-time raise: `VerdictSql.Raised.bound(RAISED_MESSAGE, RAISED_LINE, RAISED_COLUMN)`,
   with no base and `raises = true`.
2. `testexec/TestRun.java` (`raisingVerdict`): a verdict whose host judgment is an `assertError` with a planned
   failure (`HostJudgment.AssertError.caught()`) raises that failure at its turn (`Failures.raised`, as the host judge
   already does in `AssertErrorRuns.judge`) instead of running a base, and binds `judges.raisedMessage(each, error)`:
   the run's words of it, which is the existing K10b path (`TestRunner.raisedMessage` -> `BodyRun.caughtMessage` ->
   the hearing's words). Under the edge, those words read the record (section 8); under lite, they are lite's message.

**Measured (section 11's run):** channel B Relation, 469 lines, **0 differ**. The 4 `*_InvalidWindowFrameBoundary`
tests pass under both judges again. The planned-failure log has 8 wordings, all `WINDOW_FRAME_INVERTED`, bare: the 4
tests under the host judge and under the database judge, each worded from the record. No other channel B line moved.

**What it changes for lite's own plans:** a test plan whose `assertError` catches a planned failure now holds a
raising verdict statement with a bound message (3 parameters) where it held a literal. The verdict is the same (the
bound message is lite's own under lite's words). Section 13 has the core suites' result over it.

**Does it wait for stage B's judge move?** It need not. Two orders work:

| Order | What B1 needs for these 4 tests |
|---|---|
| B1 before the compatibility database judge is deleted (2.2a) | this change (+15 / -4 lines, product code, no hook), or the 4 tests stay `JudgesDisagree` on channel B until 2.2a |
| 2.2a (PCT's compatibility runs host-only) before B1 | nothing: the host judge already words from the record |

- The change is also right on its own terms: it makes a planned failure and a run-time raise reach the database judge
  the same way (bound at the verdict's turn), so A4's "lite's own rules in lite's database judge" holds for both.
- **Checked against the judges' H10** (`origin/claude/compat-judges-spike` at `beacf4c9`, `testexec/JudgeRun.java`):
  H10's `JudgeRun.Hearing.caughtMessage(RuntimeException error)` has the shape this spike's words go through (an
  exception in, the audience's words out): the run-time raise (`RaisedErrors.Raised`, its `Site` with operands) and the
  planned failure (rebuilt by `Failures.raised`, its `Failures.Planned` record as the cause) both pass through it
  unchanged. H10's `TestRun.raisingVerdict` binding is the same call (`judges.raisedMessage`), so the change above
  ports to H10 as written. **One difference, for the judges session:** H6 §7.6 names the method
  `errorMatches(expected, raised)` (the match inside the hearing), H10 builds `caughtMessage(RuntimeException)` (the
  words, the match in the product). This spike needs only the second. Sent to the judges session.

## 13. Lite's own suites and the guards over the spike (measured, session 2)

Run: 65 test targets of `//core` (every `core_tests_*` shard, both judges' shards, every `core_edge_tests*`,
`:guardrails`, `:guardrails_edge`, `:lean_sql_baseline_test`, `:raise_envelope_conformance_test`,
`:product_raise_probe`), the spike's head plus sections 11 and 12. **56 pass, 9 fail.**

**Every one of lite's own suites passes**: all 51 `core_tests_*` targets, under the host judge, `judge_both` and
`judge_database`. So the three lowering changes and the database judge's binding change no lite answer, verdict or
message that a core test pins. `raise_envelope_conformance_test` (the hosts' shared fixture, read by Java) passes.

**The 9 failing targets, every cause named:**

| Target | Failing | Cause |
|---|---|---|
| `core_edge_tests` (1,011) | 12 | section 5's `toOne` size tests (`MultiplicityStrictnessTest` 2, `RelationToOneGuardTest` 1, `OptionalCollectionNullDropTest` 2, `ToOneLaneTest` 7): `SIZE_NOT_MULTIPLICITY`'s size is built in the dialect (section 11). **Section 5's other 8 now pass**: `AssertErrorNativeTest`'s 6 (`at`'s size) and the 2 the first session fixed |
| `core_edge_tests_judge_both`, `_judge_database` (878 each) | 12 each | the same 12 |
| `core_edge_tests_integration_h2`, `_sqlite` (371 each) | 1 each | `RelationToOneGuardTest.onTheLane`, the same cause |
| `guardrails` (76) | 3 | `PlatformNamesGuardrailTest.bareNameSwitchArmsOutsideTheParserOnlyShrink` (195 > 188: `PlanJson`'s operand reader switches on the 8 JSON keys); `CodeShapeGuardrailTest.mutableInstanceStateIsExplicit` (`Operand.COMPUTED`, `Operand.LOOSE`: the spike's interface constants, `LOOSE` its measuring switch); `CompatSpellingGuardTest` (**this session's scratch log** in `testexec/HostJudgments`, section 14: not the slice) |
| `guardrails_edge` (218) | 6 | `ArchitectureTest.thePlannerWritesNoFile` (the spike's site census in `PlanMaker`: scratch); `EdgeOnlyDataTest` (below); `EdgeOnlyMembersTest` (below); `PlanStyleSurfaceTest` 3 (`errorWords` deleted from `PlanStyle`) |
| `lean_sql_baseline_test` | 1 | lite's plan JSON grows by the operands (section 10; re-measured in section 15) |
| `product_raise_probe` | 1 | environmental: it needs `LEGENDLITE_PROBE_DATABASE` (a cloud database); not run here, not the slice |

**`EdgeOnlyDataTest` (the plans' data, each piece with a product use):** 8 new pieces "by the edge":
`ExecutionPlan.Site#operands` and the 7 operand values (`Operand.Text#value`, `Int#value`, `Flt#value`, `Dec#value`,
`Bool#value`, `Day#iso`, `Instant#iso`). Lite's own sentence reads only the kind and the where, so the operands are
read by the edge's formatter alone. This is CP-7's one ruled `EdgeOnlyDataTest` row (the user, 2026-11-19 ~12:10:
"error words leave the plan, with one EdgeOnlyDataTest row"); the real slice writes it as one row if the guard can rule
a type whole, or 8 rows citing the one ruling.

**`EdgeOnlyMembersTest` (a public product member only the edge uses):**
- new: `PlanJson#write(ExecutionPlan, ExtensionCodec)` and `PlanJson#sitesOf(ExecutionPlan, String, ExtensionCodec)`
  (S1's explicit-codec overloads, which section 4 found unneeded: the real slice DELETES them) and
  `Failures$Planned#compile()` (the planning-failure record, read by the edge's formatter: pinned with its reason, or
  the product's `assertError` judge reads it too);
- gone from the edge-only set: `RaisedErrors$Raised#statement()` and `PlanJson#sitesOf(ExecutionPlan, String)` (their
  pins are deleted: "one pinned that the product uses again, or that went, is deleted").

## 14. The whole net and the corpus again, every log on (session 2)

Run: the whole net as in section 11, then `//spec:corpus_duckdb` and `//spec:corpus_h2`, with this session's scratch
logs (a defect cell the edge writes; a JSON assert that reads a defect; a planned size refusal raised at run; the
latter two in product files, reverted after the run, never committed).

- **The net:** the same 8 differing lines as section 11 (the 2 Relation lines on DuckDB and SQLite), nothing else.
- **The raises worded after the run, the whole net: 62. All 62 exact: 0 withheld, 0 printed by Java where the
  database printed before.** Section 9's same run had 38 exact and 24 withheld. By kind: `DATE_PART_ABSENT` 16,
  `VALUE_NOT_HELD` 10, `SHIFT_OUT_OF_RANGE` 8, `DATE_COMPONENT_INVALID` 8, `SQRT_OF_NEGATIVE` 4,
  `SLICE_BOUNDS_INVERTED` 4, `RANGE_STEP_ZERO` 4, `INDEX_OUT_OF_BOUNDS` 4, `DIVIDE_BY_ZERO` 4.
- **The corpus:** both lanes' rosters unchanged (`//spec:corpus_duckdb`, `//spec:corpus_h2`: 10 of 10 targets pass,
  the verdicts and the four roster shards each).
- **Planned size refusals raised at run (task 4): 0** in the whole net and both corpus lanes.

## 15. The checked graph's defect cell (task 3; counted and sized, not built)

**What it is.** A `graphFetchChecked` envelope holds one defect per class constraint that does not hold. Where a
constraint's predicate is NULL (a property absent on the wire), the defect's message is a cell the planner writes:
lite's words in the product (`ErrorKind.CONSTRAINT_UNEVALUABLE`'s sentence and the constraint's id,
`lowering/CheckedEnvelope.java:79-89`), legend-engine's sentence under the edge's dialects
(`Unable to evaluate constraint [<id>]: data not available - check your mappings`, `PureText`'s `asData` arm ->
`PureMessages.sentence`). It is an ANSWER cell, not a raise: no site, no error.

**Upstream's sentence, read at the pin** (legend-engine `230c1591`):
`legend-engine-xt-javaPlatformBinding-pure/.../legendJavaPlatformBinding/shared/constraints.pure:171` and `:177`, the
generated Java checker's catch branch: `'Unable to evaluate constraint ['+$info.id+']: data not available - check your
mappings'`. So the words are legend-engine's (its Java platform binding), not legend-pure's.

**Who reaches it (measured, section 14's run; the expected texts read at the pin):**

| Where | Cells the edge plans | Answers that hold one | Tests whose EXPECTED answer holds the sentence |
|---|---|---|---|
| PCT (DuckDB, SQLite, H2, channels A and B) | 0 (no PCT function serializes a checked graph) | 0 | 0 |
| the corpus, DuckDB | 12 (constraint ids `firstNameTest` 4, `titleNotEmpty` 2, `0` 2, `1` 2, `duplicateEmployee` 1, `typeNotEmpty` 1), in the 7 checked-graph tests of the host ledger | **0** | **1**: `graphFetch::tests::simple::testCheckedWithCircularConstraints` (`testSimpleRelationalGraphFetch.pure:1319`), which FAILS on both lanes today for another reason: legend-engine's expected answer is itself its own defect (its `//toFix: after fixing isDistinct related bug` comment; DuckDB's accepted roster `engine-golden-defect:isDistinct-in-checked-constraint`, H2's fail roster) |
| the corpus, H2 | 0 | 0 | the same 1 test |
| pure/v1's golden | 0 (no checked-graph request) | 0 | 0 |
| core's tests | `GraphFetchCheckedIntegrationTest` (5 tests, lite's words), `RaiseTextTest` (the edge's SQL sentence), `CompatSpellingGuardTest` (the phrase, allowed at the edge) | | |

- The other `Unable to evaluate constraint` texts at the pin are legend-engine's M2M tests (`core/store/m2m/tests/
  legend/dataQuality.pure`, `constraints.pure`: `... No mapping for property 'i'`, another sentence), which lite does
  not run.
- **So 0 passing tests depend on the cell's words today**, and 0 answers in the whole net, the corpus and pure/v1
  carry it.

**Which presenter words it:** legend-engine's, the ENGINE audience's result presenter (B2's, the one that writes a
graph fetch's JSON in legend-engine's envelope). Not legend-pure's: no PCT test reaches it.

**The typed cell, sized** (the orchestrator's call: a typed defect cell worded by the presenter):
- the lowering writes the defect's `message` for an unevaluable constraint as data the presenter recognises, not as
  words: the kind and the constraint's id (`CheckedEnvelope.wrap`, about 10 lines). The cleanest form is the raise
  site's own shape (kind `CONSTRAINT_UNEVALUABLE`, operand `Text(id)`), declared on the statement like a site, the cell
  holding its token; the presenter of each audience replaces the token with its sentence;
- lite's presenter of a graph fetch's JSON words it lite's way, legend-engine's presenter legend-engine's way (about 40
  lines each with their tests); the edge's `asData` arm in `PureText` (12 dialects, one line each) and
  `PureMessages.sentence`'s arm go; `RaiseText.asData` goes from the MIR (`SqlExpr.java:1230`, its two rewriters);
- **one constraint to check first:** a writer decides nothing (`WriterCarriageGuardTest`); replacing a token inside a
  JSON cell at the writer is wording, which is the presenter's, so it must sit in the presenter, not in a writer;
- **size: about 120 lines changed, 1 to 1.5 days, as a row of B2.** No test is expected to move (0 answers carry it).
- **Adjacent, found here, not this slice's:** the checked graph's DEFAULT violation message is legend-engine's
  sentence too, written by the PRODUCT (`resolver/GraphEmission.java:653`, `"Constraint :[" + id + "] violated in the
  Class " + simple`), asserted by `GraphFetchCheckedIntegrationTest.falseAndTrueArmsKeepTheirSpellings`. It is the
  census's "upstream sentence inside the product" family.

## 16. The size words (task 4; no `HostWords` member, recorded for the judges' `Hearing`)

**What the words are.** A test body's side (or its answer) whose shape requires at least one row and reads none is
refused. The words today: legend-pure's `Cannot cast a collection of size 0 to multiplicity [1]` under the edge
(`EdgeEngineTestText.sizeWords`, through `PureMessages.worded(SIZE_NOT_MULTIPLICITY, [Text(bound)])`); lite's
`ErrorKind.SIZE_NOT_MULTIPLICITY.sentence() + ": " + bound` otherwise (`EngineTextGates.sizeWords:92-95`).

**Where they are decided and raised:**

| Step | Where |
|---|---|
| worded, at PLANNING | a VALUES side with `lower >= 1`: `testplan/JudgePlans.java:308` (via `JudgePlans.sizeWords:325` -> `EngineTextGates.sizeWords:92` -> the facet `EngineTestText.sizeWords`); the body's answer: `testplan/TestDoor.java:162-179` (`answerSizeWords`) |
| carried, in the TEST PLAN | `SideShape.sizeWords` (`testexecutionplan/SideShape.java:37`, JSON `HostJson.java:207/223`); `TestPlan.answerSizeWords` (`TestPlan.java:48`, JSON `TestPlanJson.java:114/277`); `TestPlan.words`, the name of the words planned |
| raised, at RUN | `testexec/SideShapes.java:262` (a scalar side of no rows), `:291` (a collection side of no rows), `:162` (the answer's sink, `holding(...).end()`, from `TestRunner.java:401`), each as `IllegalStateException(words)` |

**What data the wording needs: the kind and one operand, the bound.** The size is 0 by construction (no rows). The
bound is already in the plan twice (`SideShape.multiplicity`; the answer column's `ExecutionPlan.Multiplicity`). It is
exactly the one-operand form of the placed raise site (`RaiseText.placed(SIZE_NOT_MULTIPLICITY, bound)`: kind,
`where = bound`, operands `[Text(bound)]`).

**For the `Hearing` design (an input, not built):**
- if the run raises this refusal as a typed error carrying that record (a `DataError` whose cause holds kind
  `SIZE_NOT_MULTIPLICITY` and `[Text(bound)]`, the shape a raised site has), then the existing word path words it:
  H10's `Hearing.caughtMessage(RuntimeException)`, this spike's `RaisedWords.wordsOf`. **No new member at all**, and
  the facet members `EngineTestText.sizeWords`, `EngineTextGates.sizeWords`, `JudgePlans.sizeWords` and the plan's
  three fields (`SideShape.sizeWords`, `TestPlan.answerSizeWords`, `TestPlan.words`) go;
- **one behaviour to rule:** today the refusal is an `IllegalStateException`, which `assertError` does not catch (its
  arm takes `DataError` and `ModelException` only, `AssertErrorRuns.judge`). legend-pure raises a
  `PureExecutionException` there, which `assertError` catches. As a `DataError` it would be caught: closer to
  upstream, but a change, to list. **Reach today: 0** (section 14: no planned size refusal is raised in PCT or the
  corpus; no pinned line holds the sentence).

## 17. pure/v1's golden: the two requests to add, legend-engine's answers, and a defect they find (task 5)

**How legend-engine's answers were got: ASKED of the engine runner** (`docs/result-boundary-homework/README.md` §7's
recipe): `//tools/engine-runner:testable`'s classpath (legend-engine 4.145.0), a scratch probe
(`H13/ErrorBodyProbe.java.txt`: parse the model, build the lambda, generate and execute the plan as pure/v1's execute
does, and on a failure print the 500 entity its `ExceptionTool` writes, `new ExceptionError(-1, exception)`, trace
left out as lite's net leaves it out), over the golden's own model (`upstream-api/trades-h2.pure`, H2). The envelope
read at the pin: `shared/core/operational/errorManagement/ExceptionTool.java:35-37` (500, the `ExceptionError` entity)
and `ExceptionError.java` (the message an `EngineException`'s, else `ExceptionUtils.getRootCauseMessage`, i.e.
`<simple class name>: <message>`; `errorType` and `sourceInformation` an `EngineException`'s alone).

**Lite's answers were recorded by the net's own recorder** (`PureV1EdgeGoldenTest`'s requests, the shipped edge
route), the requests added to the test locally and removed after: no golden moved. `main`'s answers were recorded the
same way in a worktree at `b75d6c73a`.

**The two requests I would add:**

| Name | The lambda (the golden's `TRADES` and `RT`) | What it checks |
|---|---|---|
| `raise.datePartAbsent` | `TRADES->limit(1)->extend(~h: r\|%2017->hour())->select(~[h])` | a raise SITE with a LITERAL operand (`DATE_PART_ABSENT`, `Text 2017`), not in legend-engine's unchecked set, worded after the run |
| `compile.windowFrameInverted` | `TRADES->extend(over(~[region], ~year->ascending(), rows(4, 2)), ~s:{p,w,r\|$r.qty}:y\|$y->plus())->select(~[s])` | a KINDED compile error (`WINDOW_FRAME_INVERTED`, raised at planning), worded from its record (section 8) |

**Their answers:**

| Request | legend-engine 4.145.0 (asked) | lite at `main` | lite, the spike before this section | lite, the spike now |
|---|---|---|---|---|
| `raise.datePartAbsent` | 500 `{"code":-1,"message":"PureAssertFailException: Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:26 column:5), \"Ensure the target system understands Year or Year-month semantic.\"","status":"error"}` | 500 `{"code":-1,"message":"Raised: Cannot get hour for 2017","status":"error"}` | 500 `{... "message":"Raised: DATE_PART_ABSENT" ...}` | 500 `{... "message":"Raised: Cannot get hour for 2017" ...}` |
| `compile.windowFrameInverted` | 500 `{"code":-1,"message":"PureAssertFailException: Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:26 column:5), \"Invalid window frame boundary - lower bound of window frame cannot be greater than the upper bound!\"","status":"error"}` | 500 `{"code":-1,"errorType":"COMPILATION","message":"Invalid window frame boundary - lower bound of window frame cannot be greater than the upper bound!","status":"error"}` | the same | the same |

- Neither matches legend-engine's body, and neither need be for the golden, which pins LITE's answers (a regression
  net). The differences, for the register: legend-engine refuses `%2017->hour()` at planning with its own assert's
  words where lite raises legend-pure's; for the frame it reports a `PureAssertFailException` with the assert's
  location and no `errorType`, where lite reports a `COMPILATION` error with legend-pure's sentence alone.

**THE DEFECT THESE REQUESTS FIND (fixed in the spike, edge only).** pure/v1's envelope (`EngineEnvelope.answer`)
prints a `DataError`'s CAUSE (`typeName(n) + ": " + words(n)`), so the after-run words that `RaisedWords.worded` puts in
the `DataError`'s message never reach it; `PureMessages.compile`, its `words`, turns a raised site's marked text into
its kind's bare name. Measured:

| Request | `main` | the spike before the fix | the spike after |
|---|---|---|---|
| `%2017->hour()` | `Raised: Cannot get hour for 2017` (its sentence was in the SQL) | `Raised: DATE_PART_ABSENT` | `Raised: Cannot get hour for 2017` |
| `%2017-01-01T10:30->second()` | `Raised: Cannot get second for 2017-01-01 10:30:00` (the database's text) | `Raised: DATE_PART_ABSENT` | `Raised: Cannot get second for 2017-01-01T10:30+0000` (legend-pure's) |
| `date(2017, 13, 1)` | **`Raised: DATE_COMPONENT_INVALID`** | the same | `Raised: Invalid month: 13` |
| `date(2017, 1, 1, 24)` | **`Raised: DATE_COMPONENT_INVALID`** | the same | `Raised: Invalid hour: 24` |

- **So the defect is on `main` already** for every site whose operands were literals (its words sat beside the plan, in
  the `siteWords` table, which this route never read). Section 4's line "Java's own pure/v1 route is not affected" is
  wrong: it is, for those sites. The spike widened it to every site, because no site's sentence is in the SQL any more.
- **The fix** (`edge/PureMessages.compile`, 6 lines): a `RaisedErrors.Raised` with a site is worded by
  `RaisedWords.wordsOf`, the one function every other route words a site with. No core change, no hook.
- **Checked:** pure/v1's 28 golden lines byte-identical after the fix.
- The net did not see it because pure/v1's golden has no site raise (section 9's gap): this is the first request's
  value.

**Adjacent, found here (for the register or the product):** a relation accessor over a database the model lacks
(`#>{nope::DB.TRADES_SCHEMA.TRADES}#`) is refused by lite as `Can't find database 'nope::DB'` (`MISSING`, "in
legend-engine's words (`HelperRelationalBuilder.getDatabase`)", `TableReferenceChecker.java:112-115`), but legend-engine
4.145.0 answers `The store 'nope::DB' can't be found.` (`COMPILATION`, with a `sourceInformation`): the edge's own
`MISSING_STORE` sentence. The kind chosen at that site does not match what legend-engine says there.

## 18. Lite's own plan and SQL, re-measured with this session's changes (section 10's gauge)

`//core:update_lean_sql_baseline` run locally over the spike's head (the regenerated `baseline.tsv` compared with
`main`'s and restored, never committed):

| Measure | `main` | Section 10 | Now | Change from `main` |
|---|---|---|---|---|
| SQL characters, 3,777 keys | 14,200,461 | 14,200,461 | 14,198,781 | **-1,680** |
| plan JSON characters | 147,464,602 | 147,555,033 | 147,512,654 | **+48,052 (+0.033%)** |
| keys whose plan JSON moved | | 281 | 281: 214 grew, 67 shrank | |

- **The plan JSON grows less than in section 10** (+48,052 against +90,431): the families whose operand became a literal
  shrink (`collection::at` -24,142, `math::sqrt` -6,637, `date::hour` -1,507, `minute` and `second` -1,485 each,
  `dayOfMonth` -1,419), since `{"computed":true}` is replaced by a literal and, for `at`, a subselect goes (next point).
- **Lite's SQL SHRINKS, in one family only:** `collection::at`, 11 keys (10 databases' channel A and core's DuckDB),
  -144 to -168 characters and one subselect fewer per case (3 fewer per key), the 3 cases being PCT's
  `testAtOtherScenario` (`$collection->map(x | $x + 'z')->at(i)`). One of them rendered on `main` and on the spike (a
  scratch probe, removed):

  ```
  main:  SELECT CASE WHEN 1 >= __lets.__l0 OR 1 < 0 THEN error(...'LG#1'...) ELSE list_extract(..., 2) END AS value
         FROM (SELECT len(list_filter(list_transform(['a', 'b', 'c'], x -> concat(x, 'z')), x -> x IS NOT NULL)) AS __l0) AS __lets
  spike: SELECT CASE WHEN 1 >= len(list_filter(list_transform(['a', 'b', 'c'], ...), x -> x IS NOT NULL)) OR 1 < 0
         THEN error(...'LG#1'...) ELSE list_extract(..., 2) END AS value
  ```

  On `main` the count was read twice, by the guard and by the raise's operand, so the let binding (`LetBinding`, E3)
  computed it once in a `__lets` subselect. **The second read was the message's operand, a value lite's product never
  prints.** With the size stated from the multiplicity, the count is read once and stays inline.
- **So the amended done-when holds, and better:** lite's plan changes only by the sites' operands; its SQL does not grow
  anywhere and shrinks where a raise's unprinted operand had shaped it.

## 19. The guarded files the real slice would touch (task 6)

**Measured** (section 13's run and the ratchets, `//pct:ratchets`, run here) **or read**, beyond what S1
(`70592a5bb`) already carries with its own reasons (`ArchitectureTest`, `HookLambdasTest`, `HookRulingTest`,
`JavaEvalLedgerTest`, `OpenClassMembersTest`, `PlanStyleSurfaceTest`, `PlanTypesAreDataTest`, `ServiceLoaderSitesTest`,
`PlanJson.VERSION` 58 to 59 and the hosts' `PLAN_VERSION`, `TestPlanJson.VERSION` 32 to 33):

| File | Moves how | The dated reason it would carry |
|---|---|---|
| `core/src/test/java/com/legend/PlanStyleSurfaceTest.java` | `errorWords` leaves the pinned surface (3 tests) | "`PlanStyle.errorWords` deleted (B1, slice 1.2): a planned failure is worded after the run from its record (H13 §8), the user's CP-7 of 2026-11-19" |
| `core/src/test/java/com/legend/EdgeOnlyDataTest.java` | 8 pieces ruled "by the edge": `ExecutionPlan.Site#operands` and the 7 operand values | "CP-7's one ruled row (the user, 2026-11-19 ~12:10): a site's literal operands are the formatters' data; lite's own sentence reads the kind and the where" |
| `core/src/test/java/com/legend/EdgeOnlyMembersTest.java` | pin `Failures$Planned#compile()`; delete the pins of `RaisedErrors$Raised#statement()` and `PlanJson#sitesOf(ExecutionPlan, String)`; S1's codec overloads `PlanJson#write(plan, codec)` and `sitesOf(plan, statement, codec)` are DELETED, not pinned | "B1: the planning-failure record is the presenter's input (H13 §8); the codec overloads have no use once the operands are product data (H13 §4)" |
| `core/src/test/java/com/legend/PlatformNamesGuardrailTest.java` | the bare-name switch ceiling (188): **no move if** `PlanJson`'s operand reader is a table keyed by `Operand`'s JSON key rather than an 8-arm switch; else +8 | if moved: "B1: the operand's one-key JSON (H13 §2), 8 kinds" |
| `core/src/test/java/com/legend/CodeShapeGuardrailTest.java` | **no move**: `Operand.COMPUTED` becomes the `Computed` record's own singleton (not an interface field), and `Operand.LOOSE` (the spike's measuring switch) goes | |
| `core/src/test/resources/lean-sql/baseline.tsv` (`LeanSqlBaselineTest`, shrink-only, G3) | re-recorded: 214 keys' plan JSON grows (+84,727 chars over those keys), 67 shrink, SQL -1,680 (section 18) | "the orchestrator's amended done-when of 2026-11-19 ~13:10 UTC: the product plan's JSON changes only by the sites' operands; SQL shrinks" |
| `pct/src/test/resources/org/finos/legend/lite/pct/channelb/ratchets.tsv` (exact pins, gate 9) | 6 values in 3 suites (measured): essential `judge.chars` 1,717,207 to 1,709,612, `judge.plan` 28,684,638 to 28,675,508; relation 2,006,804 to 2,004,587 and 34,502,872 to 34,503,501; standard 1,647,537 to 1,647,153 and 24,542,270 to 24,543,986. `judge.statements` and `discovered` unchanged; grammar and unclassified unchanged | "B1: the judges' SQL no longer writes upstream's sentence (a site's token instead), and their plans carry the operands and the planning-failure record" |
| `TestPlanJson.VERSION` 33 to 34 (and `TestPlanJsonRoundTripTest`) | `Failure` gains `compile` | "B1: the planning-failure record (H13 §8)" |
| `PlanJson.VERSION` | stays 59 if S1 and the operands land together; 60 if S1 lands first (and CP-7's later step, lite's sentence leaving the plan, the next number) | the same |
| `engine-client/test/fixtures/raise-envelopes.json` (the hosts' shared fixture, `raise_envelope_conformance_test`) | its sites gain `operands`, so both hosts' readers and Java's are held to the record's shape | "B1: the site's typed operands (H13 §2, §4)" |
| `docs/SEMANTICS_REGISTER.md` | rows the user rules first: the withheld operand's spelling (`<computed>` is the spike's placeholder, never ruled; 0 tests reach it in PCT, the corpus or pure/v1 today); pure/v1's site raises now in legend-pure's words where `main` said the kind's name (section 17) | per row |
| `core/src/test/resources/edge/pure_v1_answers.tsv` (pure/v1's golden) | **no move** today (28 of 28 identical); the two requests of section 17, if the user adds them, are new lines | "the net's gap (H13 §9): a site raise and a kinded compile error on pure/v1" |

- **No move needed** (checked by section 13's run): `RaiseTextGuardTest`, `HookRulingTest`, `HookRulingsAgreeTest`,
  `GuardNamesExistTest`, `OpenClassMembersTest`, `ArchitectureTest` (its one failure, `thePlannerWritesNoFile`, is the
  spike's census writer, which the real slice does not have), `CompatSpellingGuardTest` (its one failure was this
  session's scratch log, reverted).
- **Tests of the mechanism to adapt** (not guards): `SiteWordsTest`, `RaiseTextTest`, `InterimEdgeSitesTest`,
  `PureV1ApiTest`, `TestDataPlannerTest`, `PlanStyleFacetsTest`, `PlanJsonTest`; and the 12 `toOne` tests of section
  13, which pass unchanged if `CheckedOne` carries the stated size (section 11), else change their expected words.

**Does lite's own sentence leaving the plan later (slice 1.6, to a table generated from Java) break a host?** Section
4's answer stands: yes, in a known, contained set (`_raised.py`'s `_sites_of`, `raised_words` and `declare_sites`;
`raised.ts`'s `failure`; `declared-raises.ts`'s `declareSites`; `PlanJson.readSites`; `RaisedErrors.dataErrorOf` and
`dataError`; the shared fixture; `HostAnswers`; 4 hand-built test plans), all through two functions per host. One
correction from this session: section 4 said Java's own pure/v1 route was not affected by the hosts' gap; it was
(section 17), and after the fix it words every site from the record, so it does not read `message` at all and is not
broken by 1.6. Nothing in the operands design blocks 1.6.

## 20. The real slice's size (task 6, this session's estimate)

| Piece | Lines (main code) | Days |
|---|---|---|
| S1 re-done on `main` without S0 or S1m (section 1: 3 trivial conflicts) | +374 / -591 (as measured) | 0.5 |
| the record: `Operand`, `Site.operands`, `PlanMaker.sites`, `PlanJson` (a keyed reader, no switch) | about +200 | 1 |
| the formatter: `PureMessages.worded` and the date printer; the 12 dialects' one choice deleted; `PureMessages.compile`'s site arm (section 17) | about +250 / -200 | 1 |
| the lowering: the 3 sites (section 11, 33 lines) and `CheckedOne`'s stated size (a MIR field, its lowering, one dialect arm) | about +70 | 0.5 |
| the planning-failure record: `Failure.Compile`, its words, the 2 wrapping sites stating their texts (section 8) | about +150 / -80 | 1 |
| the database judge's binding (section 12), unless 2.2a lands first | +15 / -4 | 0.25 |
| tests: the record's round trip, each kind's words, the date print cases, the planning failure, the 2 pure/v1 requests if added; the tests of the mechanism adapted | about +400 | 1.5 |
| the guard files of section 19, the baseline and ratchets re-recorded, the hosts' fixture | | 0.5 |
| the checks: the net (5 min here), the corpus (2 lanes), the core suites, the hosts' fixture, CI on every OS | | 0.5 to 1 |
| **Total** | **about +1,450 / -880 including S1** | **about 6.5 to 7 days, one session** |

**Not in it** (each elsewhere, sized above): the defect cell (B2, 1 to 1.5 days, section 15); the size words (with the
judges' `Hearing`, section 16); lite's sentence leaving the plan (slice 1.6); `EngineStyleH2.raiseMessage`, the SQL
sentence's last caller (A2, section 3).

## 21. B1's H9 found the same six independently (credit, and how the two fixes compare)

B1's homework (`claude/compat-b1-homework`, `docs/records/compat-homework/H9-phase1-measures.md` §0.4 (g) and "The
Operand's coverage"; raw data `H9-data/raise_census.tsv`, `g7_moved_lines.tsv`) found the same 6 PCT essential tests
by its own probe (a `<<G7>>` marker on every computed-operand sentence the edge wrote in SQL, the whole net
re-recorded), and censused all 5,017 raise renders of the net. Its counts agree with this record's sections 6 and 7:
literal kinds String 4,135, Integer 465, Float 36, Decimal 21; no Date, Timestamp, Boolean, null, Time, Array or Struct
literal; a literal under a cast in 100 renders (`Cast(IntLit -> DOUBLE)` 36, `Cast(StringLit -> TEMPORAL_TEXT)` 32,
`Cast(FloatLit -> DOUBLE)` 32); `INDEX_OUT_OF_BOUNDS`'s size never a literal. This session did not re-measure what H9
measured.

**H9's proposal, and what this session did instead:**

| H9 | This session (section 11) | Why |
|---|---|---|
| "a `Cast` over a literal is that literal, with the literal's Pure kind", in `Operand.of` | no cast is read anywhere: `sqrt` hands the raise the argument it was given; a date part hands it the typed literal's text | the orchestrator's caution (invariant 2) is met without a structural rule on casts at all. And it covers a case the cast rule does not: an hour or minute date literal reaches the raise as a padded `TimestampLit` (no cast), whose text was the database's (`2017-01-01 10:30:00`); now it is legend-pure's (`2017-01-01T10:30+0000`) |
| "a literal list's size folded at planning" | the size READ from the typed multiplicity (`statedSize`: `IntLit(k)` where the multiplicity is exactly `[k]`) | the orchestrator's acceptable form: a type fact the typer already holds (`[1, 2]` is `[2]`), nothing counted at lowering. It also covers lists that are not literals but have an exact multiplicity (`$collection->map(...)`, section 18) |

- Both reach the same result on the net: all 6 tests exact.
- H9's census rows map to section 11's changes: the 68 `Cast(... -> DOUBLE)` renders are `sqrt`'s (now its argument),
  the 32 `Cast(StringLit -> TEMPORAL_TEXT)` are the date parts' (now the literal's text), and the sizes are now a
  literal wherever the multiplicity is exact.

## 22. The orchestrator's three notes on tasks 1 and 2 (13:35 UTC), and what the other sessions agreed

**Note 1: the side effect, a move toward legend-pure.**

| Expression | Whose message | `main` | Now | legend-pure (read at the pin, section 11) |
|---|---|---|---|---|
| `%2017-01-01T10:30->second()` | legend-pure's audience: PCT's routes (channels A and B, an `assertError`'s words) and pure/v1's route (which says legend-pure's words today, section 17) | `Cannot get second for 2017-01-01 10:30:00` (the database's text of a padded timestamp) | `Cannot get second for 2017-01-01T10:30+0000` | `2017-01-01T10:30+0000` |
| `%2017-01-01T10->minute()` | the same | `... for 2017-01-01 10:00:00` | `... for 2017-01-01T10` | `2017-01-01T10` |

- **Lite's own route's message does not change, by construction:** lite's sentence is `ErrorKind.sentence + (where ==
  null ? "" : ": " + where)` (`ExecutionPlan.Site.sentence`), and the three lowering changes touch only the OPERANDS
  (the kind and the where are the same records). Measured as far as the suites reach: all 51 `core_tests_*` targets
  pass (section 13), under the host judge, `judge_both` and `judge_database`.
- No pinned line in PCT, the corpus or pure/v1 holds an hour or minute date in such a message today (sections 11, 14).

**Note 2: `SIZE_NOT_MULTIPLICITY`, the MIR field the real slice would need (not built).**
- **The field:** `SqlExpr.CheckedOne(list, scalarCarrier, atLeastOnly, type)` gains `@Nullable Integer statedSize`,
  set by the lowering from the operand's typed multiplicity where it is one number (the rule of `statedSize`, section
  11). The renderer (`AnsiSqlRenderer.checkedOne`, `sql/dialect/AnsiSqlRenderer.java:805-830`) puts `IntLit(statedSize)`
  in the raise's size operand where it is set, else `COALESCE(LIST_LENGTH(list), 0)` as today; the guard keeps its
  count.
- **Its size:** the record and its 2 convenience constructors (`SqlExpr.java:1177-1197`), its `mapChildren` arm
  (`:281`), `SqlRewriter.java:275`, `FoldToListReduce.java:131`, `MirJson`'s writer and reader (`:1033`), the 3
  lowering sites (`Coercions.java:75/83/169`) and `Lowerer.java:1094`, the renderer's 2 lines: **about 10 sites, 30
  lines, 0.5 day with its tests.** `MirJson` writes the MIR into plans, so the plan's JSON moves with it (the same
  version bump).
- **What it covers, read from the 12 tests (not run):** the field states a size only where the multiplicity is exact.
  Of the 12, three would then pass unchanged (`MultiplicityStrictnessTest.literalCollectionToOneRaisesUserError`,
  `ToOneLaneTest.ifRaises`, `sortDescRaises`: a literal list, `if` of two `[2]`s, `sort`/`reverse` keep `[m]`). **The
  other nine are TRUE run-time sizes** (a `filter`, `slice`, `range`, `take`, `limit` or `zip` result, a relation's row
  count, a run-time empty list: `RelationToOneGuardTest.onTheLane`, `OptionalCollectionNullDropTest` 2,
  `ToOneLaneTest` `rangeRaises`, `zipRaises`, `sliceRaises`, `takeLimitCarryTheLane`, `literalAndCallShapes`' filter
  case, `MultiplicityStrictnessTest.runtimeEmptyListToOneRaises`): A3 withholds them by design, so their expected
  words change under the register row that rules the withheld spelling.
- **It joins `raise.toOneOfEmpty` on pure/v1** (B1-R's finding today, relayed by the orchestrator, not measured here):
  legend-engine answers 200 with a row where lite answers 500. Both belong to the same `SIZE_NOT_MULTIPLICITY` row.

**Note 3: the typed `Operand.Date`, an open item for the real slice, with a recommendation.**
- **The options:**
  1. a MIR literal for a Pure date at its precision (a new `SqlExpr` variant: a render arm in every dialect, a throwing
     arm being allowed, and `Operand.of` maps it to `Operand.Date(canonical)`);
  2. typed operands on `RaiseText` beside its SQL ones: **not open**, because `sql/` depends on nothing
     (`sqlLayerIsStandalone`), so `RaiseText` cannot hold an `executionplan.Operand`, and a second `sql/`-side operand
     type would duplicate `Operand`;
  3. keep `Operand.Text` and let the kind's contract type it (as the spike does): `DATE_PART_ABSENT`'s operands are
     `[the part's label, the date]`, the formatter parses the date's canonical text back (`PureDateLiteral.parse`,
     whose round trip is the type's stated identity).
- **Recommendation: option 3 for B1**, with a unit test that holds the contract (each kind's operand positions and
  their types, one table), and option 1 only if a second kind needs a date operand. Today exactly one kind prints a
  date (`DATE_PART_ABSENT`); `DATE_COMPONENT_INVALID` prints the components as Integers, not a date. Option 1 would
  add a MIR variant to the product for a value the product never prints.

**What the other sessions agreed (13:35 to 14:10 UTC):**
- **The judges session (H10):** its `Hearing.caughtMessage(RuntimeException)` is this spike's word path; the order
  difference (H10's edge hearing still reads `siteWords`) is about 30 lines for whichever lands second. It suggests a
  typed parameter (a sealed `Site` / `Planned` / `Message` record instead of the exception's cause chain), for both
  builds. It ADOPTED section 16's size-words shape (a `DataError` carrying the one-operand site record, worded by
  `caughtMessage`, no new member, no new kind; H10 §9.4 rewritten, `claude/compat-judges-homework` `6c0a5889`), and
  listed the `assertError` behaviour change for the user's word. It reads section 12's database-judge 4 tests as 2.2a's
  deletion; section 12's binding is the alternative if B1 lands first.
- **The host-parity worker:** `lite_wire.Refusal` lands with `raise` as the kind's wire name and `where` its own, no
  `operands` key yet; when the record lands, `operands` joins as a sibling key (order kind, message, where, raise,
  operands), present only on a data refusal whose raise has operands, written by `PlanJson.operandJson` made shared.

## 23. Against the rules of record (the user, 2026-11-19 ~15:20 UTC; `compat-rules-of-record-2026-11-19.md` at `e4aafee0`)

The orchestrator's three questions (14:48 UTC). Nothing built for this section.

**Rule 1 ("Database calculates everything ... including format and tostring"): the product's raise message stays the
database's, values included.**
- What the spike leaves in the product: the raise writes the site's token, and lite's own message is the site's
  planned sentence (`ErrorKind.sentence + (where == null ? "" : ": " + where)`): a constant the planner knows, holding
  NO run-time value. No Java computes a value for it, and none is folded.
- If lite's own message is to print a value (the size, the dividend, the date), rule 1 places that printing in the
  database: the SQL writes the value's text into the raise (`error(... || CAST(<value> AS VARCHAR) || ...)`), as
  `main`'s edge did for its computed operands. The presenter (rule 2) does not compute or print it.
- **One question for the orchestrator, raised by rule 1's letter** ("the only computation before the database is the
  compiler's folding of constant String and Boolean expressions"). Section 11's three operands are not folds of an
  expression: `sqrt`'s is the literal as written, a date part's is the literal's written text, and `at`'s size is the
  typed multiplicity (a type fact, the orchestrator's accepted form). But `at`'s size is an Integer the message prints,
  produced before the database. If that reads as a fold beyond rule 1, the alternative below (the database returns the
  size with the raise) replaces `statedSize` and `CheckedOne.statedSize` both.

**Rule 3 (compatibility words upstream's sentence in Java "only from output of typed values from lite"): what lite
would have to return where a computed operand's value is withheld today.**

| Kind | The withheld value | The typed value lite would return | The cases today |
|---|---|---|---|
| `INDEX_OUT_OF_BOUNDS` | the offset (when computed), the size | Integer, Integer | `at` over a computed list, a variable offset |
| `SIZE_NOT_MULTIPLICITY` | the size | Integer | section 22's 9 `toOne` tests (a `filter`, `slice`, `range`, `take`, `limit`, `zip` result, a relation's rows, a run-time empty list) |
| `SQRT_OF_NEGATIVE` | the value | Float | `sqrt` of a column or an expression |
| `DIVIDE_BY_ZERO` | the dividend (`rem`, `quotient`) | Integer, Float or Decimal, as declared | `rem` of a column |
| `DATE_PART_ABSENT` | the date | a date at its precision (the precision is the planner's: the constructor's arity, or `StrictDate` for a column) | `date(2017)->hour()`, a `StrictDate` column's `hour()` |
| `SLICE_BOUNDS_INVERTED` | the bounds (clamped at 0) | Integer, Integer | `slice` with computed bounds |
| `DATE_COMPONENT_INVALID` | the component's value, at 1 of 4 sites | Integer or Float | a computed component |
| `TEXT_NOT_INTEGER`, `TEXT_NOT_FLOAT`, `TEXT_NOT_DATE` | the text | String | a parse of a column |

**Can the database return them with the raise? Yes, as text in the raise's own envelope, which exec reads back as
typed values.** Every dialect builds its raise from a text EXPRESSION, not a constant (DuckDB `error(...)`, SQLite's
`json_extract('{}', ...)`, ClickHouse's `toInt64(concat(...))`, Snowflake's and MariaDB's failing casts, MariaDB and
MySQL's `@legend_raise` read-back, H2's `errorArm`, BigQuery `ERROR`, Databricks `raise_error`, Trino's): `main`'s edge
already wrote computed values into it on all of them. The envelope already carries a second field, the position
(`chr(31) || 'line:col' || chr(30) || <message> || chr(31)`). So:
- the planner declares each withheld operand's slot on the site: its position and its Pure type (planning knows both;
  no value);
- the SQL writes each slot's value into the envelope as the DATABASE prints it, in a lossless spelling the dialect owns
  (Integer and Decimal digits; a Float's shortest round-trip text, the existing `FLOAT_SHORTEST_TEXT` per dialect; a
  date's text at its precision; a String escaped so it cannot hold the envelope's separators `chr(29)`, `chr(30)`,
  `chr(31)`);
- exec reads the slots back by their declared types into typed values, as it decodes a typed cell's text, and hands
  them to the run's words with the site; compatibility words upstream's sentence from them in Java.
- **To measure before building:** each dialect's error-text limits (the length a raise carries, how Snowflake's and
  ClickHouse's error texts quote it) and the Float spelling's round trip on SQLite (its text of a double is 15
  significant digits unless the dialect writes it otherwise). The product path would not change: lite's own message
  still says no value unless the product chooses to (rule 1, above).
- **A3 is the ruling this touches**, for the orchestrator to put to the user: A3 says no run-time value in a message.
  Under rules 1 and 3 the database computes the value and lite returns it typed with the raise; the PLAN still holds
  no run-time value (the site holds only the slot's type). If allowed, the `<computed>` placeholder, the register row
  for it, `statedSize` and `CheckedOne.statedSize` are all unneeded: every operand legend-pure prints would be
  printed, and the 9 `toOne` tests keep legend-pure's words.

**Rule 4 (the host judge is the only judge in compatibility): the database judge's 4 tests are moot in compatibility.
Is section 12's +15 / -4 change still needed for the product? No.**
- In the product, both judges run, and a planned failure's message is lite's own on both sides. The planted literal and
  the host judge's words agree by construction. Measured: every `core_tests_*` target passes under `judge_both` and
  `judge_database` on `main` (with the literal planted) and on the spike (with it bound).
- The planted literal is planner-known text (a constant), and the database still computes the verdict (it compares
  the expected text with it), so it holds rule 1.
- **So the real slice drops section 12's change** (-0.25 day from section 20's estimate), unless the product wants a
  planned failure and a run-time raise to reach its database judge the same way. That is uniformity, not a need.
  A4 removes the compatibility database judge before B1 (stage A), so the 4 channel B tests go with it.
