# Step 8 research: presentation at the edge (read-only, 2026-11-26)

Read at `/home/user/ll-edit`, branch `claude/edit-ahead`, head `20a315c1`. Nothing was built or run. Line counts come
from `wc -l` or from reading the files. Anything I inferred without reading is marked **(inferred)**.
Paths: `L` = `core/src/main/java/com/legend/`, `E` = `L/edge/`, `T` = `core/src/test/java/com/legend/`.

## 0. The short answer

- Almost all of the SQL that writes upstream's text comes from three nodes the product's lowering already writes:
  `FLOAT_TO_TEXT`, `DATETIME_TO_TEXT` and `CELL_TEXT`. The edge rewrites each one in its dialects' first pass
  (`E/PurePrintForms.java`, 175 lines), and its twelve dialect subclasses override `floatToText`. Around that core
  sit five smaller spellings: the DateTime-literal two-form, the list text, the MongoDB date format, the graph-fetch
  key text and the JSON forms.
- **One finding changes the framing.** At the **legend-engine door** (pure/v1, PCT's engine lane, and the corpus's
  query inside `execute()`), a Float's or DateTime's text inside a store query is not a spelling. legend-engine itself
  sends `toString` to the database as `CAST(x AS VARCHAR)`, so the database's text is legend-engine's **answer**.
  Upstream's own extension files say so (cited in `E/PurePrintForms.java:107-114`). By the plan's §2 that is a
  kind-2 difference (an edition-table row), not kind 4. Only **legend-pure's** print (`DecimalFormat`, Java in
  legend-pure) is a true kind-4 presentation.
- **What decides each case is its position.**
  - At the **last step**, a presenter can write the text from the typed value. The text is a query's final cell
    (PCT channel A, pure/v1), or the root of an assert's side, which the host judge alone compares (channel B, the
    corpus).
  - **Mid-chain** (the text is joined, measured, sorted, or put in a message), tenet 1 says lite's own text is
    written. So a deletion **moves answers** there: the corpus's `TDSNull` (112 DuckDB and 107 H2 tests, measured in
    S6) and the validation showcase (2 per lane).
- **Realistic size:** about 450-650 lines leave main if the Float and DateTime passes go. About 900-1,100 go if
  `TDSNull`, lists, the graph-key text and the `FLOAT_SHORTEST_TEXT` machinery go too. The plan's "about 2,000" is
  not reachable from text alone (§F).

## A. The inventory

Doors: **U** = `PureText.UPSTREAM` (PCT channel A for legend-pure, PCT channel B, the relational corpus). **V** =
`PureText.PURE_V1` (pure/v1, PCT's engine lane, wasm). V takes U's dialects (`E/PureText.java:189-191`), so every
group-1 item reaches both.

### (1) Dialect-subclass overrides and the edge's SQL passes

| # | Where | Doors | Upstream text, against lite's product text | Register | Lines |
|---|---|---|---|---|---|
| 1a | `E/PurePrintForms.java:161-174` (`expr`, the `FLOAT_TO_TEXT` arm) + `floatOf` :111-119 + `FloatLiteral` :78-105 | U, V | `1.3421e-8->toString()`: `CAST(0.000000013421 AS VARCHAR)` gives `0.000000013421`. Computed `(1.3421e-8*1.0)->toString()` on DuckDB: `1.3421e-08`. Product: `0.0` (six decimals). On H2, `PRINT_AS_FLOAT` gives `1.3421E-8` | S311 (in `docs/records/d-register-rows-2026-11-16.md:14-16`); S20 for the literal | about 60 |
| 1b | the `floatToText` overrides, 12 dialects (`E/PureText.java:302-307, 326-330, 356-361, 373-378, 389-394, 405-410, 429-434, 445-450, 461-466, 478-483, 494-499, 516-521`) | U, V | the same as 1a, for a `FLOAT_TO_TEXT` no pass reached | S311 | about 84 (7 × 12) |
| 1c | the `spellingPasses` overrides, 12 dialects (`E/PureText.java:296-300` and the same in each class) | U, V | turn on 1a, 1d, 1e and 1f | — | about 72 (6 × 12) |
| 1d | `PurePrintForms` `DATETIME_TO_TEXT` arm (:169-172) and `E/PureDateTimePrint.java` (43) | U, V | `2014-01-02T01:54:27.352+0000` against the product's `2014-01-02T01:54:27.352000000Z` | S312 (`d-register-rows…:17-20`) | 4 + 43 |
| 1e | `TdsNullCells` (:66-71), `joined`, `isJoined`, `tdsNullOf` (:126-159) | U, V | an empty TDS cell's text `TDSNull`. A row's cells joined: `a,TDSNull,b` against the product's `a,b` (the product skips an empty element) | S313 (`d-register-rows…:21`) | about 45 |
| 1f | `UnguardedParses` (:47-61) | U, V | **not a spelling.** F31: a plain cast, so the database's error replaces lite's `TEXT_NOT_INTEGER`. This is meaning (an edition row), not step 8 | F31 (an orchestrator approval, not a register row) | 15 |
| 1g | `MongoDb.dateTimeFormat` (`E/PureText.java:504-508`) | U, V on MongoDB | `%Y-%m-%dT%H:%M:%S.%L+0000` against the product's `.%L000Z` (`L/sql/dialect/MongoDb.java:834`). Used in `$dateToString` for `toString`, in `format`'s `%s` (:1603) and in the JSON text (:715, :919) | S312 | 5 |
| 1h | H2's constructor (`DatabaseSettingsForm`, `E/PureText.java:313-316`), `SQLite.refusesDecimalArithmetic` (:418-421), `raw` (:283-286, :341-348), `scanOrderPasses` (:291-294) | U, V | **not spellings:** H2's session settings (hard case 4), REAL arithmetic (meaning), the corpus harness (step 9) | — | out of step 8 |

### (2) Lowering forms and two-form hooks that only change text

| # | Where | Doors | Upstream text, against the product | Register | Lines |
|---|---|---|---|---|---|
| 2a | `DateTimeTexts`, two-form: `E/EdgeDateTimeTexts.java` (28); interface `L/lowering/DateTimeTexts.java` (20); asked at `L/lowering/Scalars.java:3702`, `E/EdgePureOnlyTexts.java:79,131`; `Styled.LITE_DATE_TIMES` (`L/lowering/Styled.java:35-36`); `PlanEnv.styled` (`L/planner/PlanEnv.java:181`) | U, V | a DateTime **literal** folded at lowering to its written text + `+0000`: `%2014-01-01T10:20:30.5->toString()` gives `2014-01-01T10:20:30.5+0000`; the product gives `…30.500000000Z` | S312 | 48 + plumbing about 10 |
| 2b | `ListTexts`, two-form: `E/PureListTexts.java` (49); `L/lowering/ListTexts.java` (21); asked at `Scalars.java:2959`, `TaggedLists.java:566` | U, V | `[1, 2]`, against the product's JSON array `[1,2]` | S316 (`d-register-rows…:24-27`) | 70 |
| 2c | the `PureOnlyTexts` facet: `E/EdgePureOnlyTexts.java` (150); `L/lowering/PureOnlyTexts.java` (34); `TextRefusals` (48); asked at `Scalars.java:1805, 3030, 3583, 3602` | U, V | `format('%05d', -3)` gives `-00003`; an instance's `@_<id>`; `%r` / `toRepresentation` gives `'abc'`, `%2014-01-01`, `1.5D`. **The product refuses all of these by name**, so there is no product text to re-spell | S314 (`d-register-rows…:22`) | 232 |
| 2d | `LoweringForms.objectReferenceConnection`: `E/EdgeLoweringForms.java:29-44`; used at `L/lowering/Lowerer.java:1604` into `SnapshotEnvelope.asorWrap` (`L/lowering/SnapshotEnvelope.java:72-120`) | U, V | legend-engine's H2 connection JSON inside an object reference, against the product's empty segment. The SQL builds the whole length-prefixed reference text | (audit J12; no S row found) | about 20 edge + 10 product |
| 2e | `LoweringForms.executeValues` (JsonBuilder's one object bare): `E/EdgeLoweringForms.java:46-72` (`oneBare`); used at `Lowerer.java:818, 4434` | U, V | exactly one graph object is written bare `{…}`, against the product's array `[{…}]`. SQL: `CASE WHEN COUNT(*)=1 THEN MIN(v) ELSE array END` | (audit J3; no S row found) | about 30 + 10 |
| 2f | `PlanStyle.graphKeyDate` / `graphKeyString`: `E/PureText.java:149-159`, 208-215; `L/database/PlanStyle.java:52,56,123-130`; `L/resolver/GraphKeyText.java` (23); `L/planner/BodyPlanner.java:76-77`; `StoreResolver.java:104-108`; `GraphEmission.java:61-64, 2905` | U, V | a graph fetch's qualified-property key: `synonyms(2023-10-15T00:00:00+0000)` and `'abc'`, against the product's `…00.000000000Z` and `abc`. Written into the SQL as a literal key | S312 (cited in GraphKeyText's javadoc) | about 60 |
| 2g | `parsedDecimal`, `parsedJson`, `isNumeric`, `existsJoin`, `whereZones` (`E/EdgeLoweringForms.java:74-116`) | U, V | **meaning or SQL shape, not text.** Out of step 8 | S367, S370 | — |
| 2h | `traceCell` (`E/PureText.java:161-166`; `PlanStyle.java:31,112`; `PlanEnv.java:169-174`; `Lowerer.java:244-251, 2867`), `ExecutedValues`, `ActivityTexts`, `RoutedQueries`, `ResultEnvelopes` | U, V | text inside upstream's own plan, activity and envelope values (`-- "executionTraceID" : "<id>"`). The census calls these **T (test harness): step 9**, not step 8 | — | — |
| 2i | `TestDataPlanner.Printing.timestampText` (`E/TestDataRoutes.java:52-67`; `E/EnginePlans.java:227-229`) | pure/v1 test-data routes | a timestamp cell in the generated CSV, cut to seconds. The CSV is built in SQL by the product as well, so moving it needs a different product plan | (L9; S306 for the rest) | out of scope (note) |

### (3) Typed-tree rewrites at a door that only change text

I found none that build text in SQL. The door rewrites that touch text are already presentation in Java (part C):
`PrintedRelations.lift` (PCT channel A) and `UpstreamPrints.read` (channel B, corpus). The other typed-tree rewrites
at the doors change meaning: `PureComparisons`, `EngineComparisons`, `UpstreamTestBodies`. One thing comes close:
`TdsDesugars` mints an explicit `cellText` call for a TDS cell's `toString` on every door (`L/lowering/Coercions.java:165-166`).
That is a product node, and only the edge's 1e gives it `TDSNull`.

### (4) Product code that exists only to serve an upstream spelling

| # | Where | Serves | Lines |
|---|---|---|---|
| 4a | `AnsiSqlRenderer.spellingPasses` hook and its call (`L/sql/dialect/AnsiSqlRenderer.java:261-263, 312-318`) | 1a-1f. It has no product body; its javadoc says "the product's dialects run none" | about 10 |
| 4b | `MongoDb.dateTimeFormat` as a protected `@SpellingArm` hook (`MongoDb.java:830-836`). The product needs the format, but not the hook | 1g | 7 |
| 4c | **`SqlFn.FLOAT_SHORTEST_TEXT` and everything built for it.** `L/sql/dialect/FloatShortestText.java` (184); `spellsFloatShortestText` and `readsLongTextOnce` (`AnsiSqlRenderer.java:277-301`; `Snowflake.java:198-203`; `BigQuery.java:205-212`); Snowflake's own `floatShortestText` (`Snowflake.java:1899-about 1935`); the `FLOAT_SHORTEST_TEXT` arm in 12 dialects; the SqlFn entry (`SqlFn.java:157-167`). **Its only minter in main is the edge** (`E/EdgePureOnlyTexts.java:97`, the `%r` / `toRepresentation` of a Float). I grepped all main sources for `FLOAT_SHORTEST_TEXT`: no product lowering writes it | 2c's Float `%r` | about 265 |
| 4d | the `Styled` fields `pureOnly`, `dateTimes` and `lists` (`L/lowering/Styled.java:28-42`) and the three interfaces (`DateTimeTexts`, `ListTexts`, `PureOnlyTexts`); `PlanEnv.styled` (:178-184) | 2a, 2b, 2c | about 95 |
| 4e | the `PlanStyle.graphKeyDate` / `graphKeyString` methods, `GraphKeyText` and the resolver plumbing | 2f | about 45 (counted in 2f) |
| 4f | `LoweringForms.executeValues` and `objectReferenceConnection` (`L/lowering/LoweringForms.java:43,50`) and their product forms (`ProductLoweringForms.java:26-35`) | 2d, 2e | about 20 |

**Not upstream-only, and they stay:**
- `CellTexts` and `DateTimeText` (`L/sql/dialect`): the product's own forms.
- `FLOAT_FULL_TEXT`: the product's exact `toCSV`, S311 amended.
- `ProductRelationText`.
- The product's `floatToText` bodies: lite's six decimals.

## B. Last step, mid-chain, or both

| # | Classification | Why |
|---|---|---|
| 1a/1b Float text | **(iii) both** | Channel A: the case's expression is a query, and its cell goes back to legend-pure, which compares it in Java, so it is the last step. Channel B and the corpus: an assert side whose root is `toString` is a last step for the host judge. Anything else is mid-chain: `->length()`, joined, or in a constraint message (the corpus's validation showcase, 2 per lane, `1000000` against `1000000.0`). **At the engine door this is kind 2 (§0), not a presentation** |
| 1d/2a DateTime text | **(iii) both** | The same split. PCT's 5 DateTime cases (E3 §4) are `toString`, `format('%s')` and `format('%r')`. The `toString` ones are last-step or assert-side. The two `format` cases are a `format` call, so a `toString` lift does not reach them |
| 1e `TDSNull` | **(ii) mid-chain, nearly always** | S6 measured it on DuckDB: of 112 corpus tests, 76 are a row's cells joined, 10 a cell's `toString`, 8 both, 16 other. The texts are built per row inside a `map` lambda, then joined and compared. Upstream does this in Java after `execute()`, so it is really a "which side runs it" (kind 3) question |
| 1g MongoDB format | (iii) both | `toString` (either), `format`'s `%s` (mid-chain), the JSON text of a wire value (mid-chain) |
| 2b list `[a, b]` | (iii) both, mostly mid-chain | a `toString` of a list in a message or compared. At the last step, `Presenter.LEGEND_PURE.structure` already prints legend-pure's list text in Java (`E/Presenter.java:177-199`) |
| 2c `%0wd`, `@_`, `%r` | **(ii), and not a re-spelling** | The product refuses each one, so there is no typed product output to re-spell. These are mostly message texts (`format('%r')` in assert messages). They stay an edition row unless the user rules that a last-step `format` may be lifted into Java (Q6) |
| 2d, 2e, 2f JSON forms | (iii) both | At pure/v1's last step, `PureV1Writer` already writes "the array or the one object bare" (`E/PureV1Writer.java` header). A writer could rewrite the key text or the connection segment there. In the corpus, `serialize` output is compared inside the test (`assertJsonStringsEqual`), so it is mid-chain (census hard case 7) |

**How the code can tell the two apart today:**
- **Queries (PCT channel A, pure/v1):** `TypedQuery` holds the last statement. `E/PrintedRelations.java:92` (`lift`)
  already reads it and removes a final print. B2b's S6a extended it with `Form.VALUE` for a last-step Float `toString`.
  The patch is `docs/records/edition-homework/s6a-full-stash-2026-11-25.patch.txt` (hunks at its lines 17-66 and
  385-450). It applies to base `413960af`, not to the head as it is.
- **The result writers cannot tell.** They get `ExecutionPlan.Column` (`L/executionplan/ExecutionPlan.java:655`:
  the Pure type, slot, store type, precision, decimal digits). A `toString` cell's type is `String`, and nothing says
  it came from a Float. So the lift must happen on the typed tree at the door, before planning, as `PrintedRelations`
  does.
- **Test bodies (channel B, corpus):** each host-judged assert's sides are planned as their own statements
  (`L/testplan/HostAsserts.java`, `HostSides.plan`). The host judge compares typed values, `String` against `String`
  (`L/testexec/Equality`). The database judge is refused in compatibility (`TestPlanner.refuseTheDatabaseJudgeInCompatibility`).
  `PrintedRelations.side(TypedSpec)` (:67) is "the one rule for a printed expression". `UpstreamPrints.read`
  (`E/UpstreamPrints.java:71`) already pre-reads a relation side and puts a String literal in its place. A **scalar
  side lift** is the same thing for a value side. S6a's patch planned it, gated off at `UpstreamPrints` (patch line 706).
  This is the third review's decision 8 (`third-review-v3-2026-11-25.md:256`).
- **Mid-chain needs no detection.** Anything a lift does not take keeps the SQL form. After step 8 that form is lite's.

**The two-printer trap** (it is why B2b's design audit removed S6a): at one door, `x->toString()` gives legend-pure's
text in Java, but `x->toString()->length()` reads the SQL text. Today's example: `(1.0E-5*1.0)->toString()` printed
`0.00001`, while `…->toString()->length()` read the cast's `1e-05`. After the passes go, the mid-chain text is lite's
six decimals. So `1.3421e-8->toString()` prints `0.000000013421`, but `1.3421e-8->toString()->length()` gives 3
(`0.0`). That is tenet 1 as amended, but it is a visible inconsistency (Q1).

## C. What the edge already presents in Java (reuse these)

| Class | Lines | What it writes | Called by |
|---|---|---|---|
| `E/PureValueText.java` | 130 | legend-pure's cell text: `pureFloat(double)` (legend-pure 5.99.0's `DecimalFormat` rule, :85-99), `tdsDate` (:111), `cell` (:52) | `PureRelationText`; S6a's `VALUE` form |
| `E/PureRelationText.java` | 223 | `#TDS` text (`tds`), `toCSV` with and without `TDSNull`, a TDS's `csv`, literal `replace` | `PureRelationTextWriter` |
| `E/PureRelationTextWriter.java` | 103 | a `ResultSink` receiver; `Form` = TO_STRING, TO_CSV, TO_CSV_TDSNULL, CSV_PROPERTY | `PrintedRelations.Lifted.writer`; `UpstreamPrints.PRINTER` |
| `E/PrintedRelations.java` | 175 | lifts a final print off a query (`lift`), or reads one side (`side`) | `pct/.../PctPlanRoute.java:161-180`; `UpstreamPrints` |
| `E/UpstreamPrints.java` | 337 | pre-reads an assert side's relation, prints it, substitutes a String literal | `spec/.../MinimalCorpus.java:633`; `pct/.../channelb/ChannelB.java:419` |
| `E/Presenter.java` | 231 | legend-engine's JSON tokens (`engine`, `writeEngine`: Float via `Double.toString`, dates), `presentedName`, `LEGEND_PURE` `CellText.Forms` (legend-pure's date text, List text), `TDS_NULL` | `PureV1Writer`, `TdsObjectsWriter`, pure/v1's Arrow route |
| `E/PureV1Writer.java`, `EngineAnswers.java`, `HostAnswers.java`, `TdsObjectsWriter.java` | 307 / 134 / 464 / — | pure/v1's TDS and graph JSON (one object bare at the last step), Arrow, the host halves | `PureV1Routes`, `EdgeServer`, wasm |
| `E/PureMessages.java` | 180 | legend-pure's error sentences, worded after the run from a site's record (B1). **Errors are already done** | the test runner's edge hook |
| `L/result/CellText.java` | 369 | the product's text (`carriedFloat`, `productTime`) and the `Forms` extension point (:52-58) the edge plugs into | every writer |

## D. The tests that judge step 8

| What | Target | Narrow run |
|---|---|---|
| PCT channel A answers per case (the edge golden net) | `//pct:edge_net` (it holds `:update_edge_contract_tests`, `//core:update_edge_pure_v1_tests`, `//core:update_edge_cases_tests`); per suite `//pct:edge_<duckdb\|sqlite\|h2>_<suite>` | `bazel build //pct:edge_duckdb_essential`, then the diff test; re-record with `bazel run //pct:update_edge_contract` (only for a ruled move) |
| PCT itself (legend-pure's audience) | `//pct:pct_duckdb_<suite>`, `//pct:pct_sqlite_<suite>`, `//pct:pct_h2`; channel B `//pct:pct_channel_b_<suite>` | `bazel test //pct:pct_duckdb_essential --test_filter=<class>#<case>` (inferred: the junit `--test_filter` form, not tried) |
| PCT's engine lane (V) | `//pct:engine_lane_<duckdb\|h2>_<suite>`, pinned by `//pct:engine_lane_duckdb_test` and `_h2_test` | a build of one suite's lane |
| PCT's print route | `//pct:pct_route_print_test` (`PctPlanRoutePrintTest`) | itself |
| pure/v1 answers | `//core:edge_pure_v1` (golden `core/src/test/resources/edge/pure_v1_answers.tsv`); update with `//core:update_edge_pure_v1` | itself |
| The edge's missing cases (EDGE-1 to EDGE-3) | `//core:edge_cases` (`EdgeCasesGoldenTest`); update with `//core:update_edge_cases` | itself |
| The relational corpus | `//spec:corpus_duckdb`, `//spec:corpus_h2` | `bazel run //spec:corpus_one -- duckdb host <test fqn>` (about 2 minutes) |
| The corpus's SQL size (plans under **U**, `LeanSqlCorpusTest.java:132`) | `//spec:lean_sql_corpus_duckdb`, `_h2`; re-pin with `//spec:update_lean_sql_corpus_<lane>` | itself (planning only) |
| Core's edge unit tests | `//core:core_edge_tests`, which holds `PrintedRelationsTest`, `PureRelationTextTest`, `UpstreamPrintsTest`, `PresenterTest`, `FloatTextTest`, `PureRelationTextParityTest`, `EdgeSpellingHookTest`, `TypeInferenceIntegrationTest` (inferred for the last), and the hook guards | `bazel test //core:core_edge_tests --test_filter=com.legend.edge.PrintedRelationsTest` |
| Product guards in the root package | `//core:core_tests_root` | `--test_filter=com.legend.CompatSpellingGuardTest` (also `JavaEvalLedgerTest`, `WriterCarriageGuardTest`, `DialectLeakRatchetTest`) |

## E. The guards a change will trip

| Guard | What trips it | Direction |
|---|---|---|
| `T/integration/EdgeSpellingHookTest.java:160-205, 260` (`OVERRIDES`) | each edge dialect's overridden arms, pinned exactly. Removing `spellingPasses`, `floatToText` or `dateTimeFormat` fails until the map is re-pinned | **shrink, welcome** ("shrink-only toward the floor") |
| `T/HookRulingTest.java:572-573` `SPELLING_HOOKS` (`raw`, `spellingPasses`, `raiseMessage`, `scanOrderPasses`, `dateTimeFormat`); `onlyTheEdgeOverridesASpellingHook`; the two-form list in order (:344-366); `PRODUCT.twoForm` identity checks (:385-423); the implementers (:549) | deleting a hook or a two-form kind (`DateTimeTexts`, `ListTexts`) or the `PureOnlyTexts` facet | shrink; edit the lists with a dated reason |
| `T/HookLambdasTest.java:47, 57-61, 110, 117, 185` | `Styled` makes one `DateTimeTexts` lambda; `PlanEnv#styled()` "(twoForm reaches 3)"; the product classes that implement hook types | the counts fall when a kind goes; re-pin |
| `T/PlanStyleSurfaceTest.java` | `PlanStyle`'s declared methods (`graphKeyDate`, `graphKeyString`, `traceCell`) and the `KINDS` and `TWO_FORMS` lists, pinned exactly | deleting is welcome but fails until re-pinned. **Adding a method or kind fails** |
| `T/JavaEvalLedgerTest.java` `theFunnelPackagesAreClosedRegisters` (the edge's file list, :1869-2140) and `javaEvaluationSurfaceOnlyShrinks` | **a new edge class** (for example a new presenter) needs a dated entry; a deleted file must be removed from the list | deletion welcome; growth needs a justification |
| `T/WriterCarriageGuardTest.java:128-179, 325-331` | **new Java formatting vocabulary** (`compareTo`, arithmetic, `contains`, `DecimalFormat`) in `Presenter`, `PureRelationText` or `PureValueText`. A new presenter file must be added to the scan list, or it is not scanned | **growth fails**; register with a dated reason |
| `T/CompatSpellingGuardTest.java` (`LITERALS`, `SYMBOLS`, `REGISTER`, shrink-only) | an upstream spelling (`+0000`, `TDSNull`, `@_`, Pure's Float text) appearing in a product source. If step 8 moves a hook's default into the product, keep the literal in the edge | a new hit fails; a falling count must be re-pinned |
| `T/EdgeOnlyMembersTest.java` (`OPEN`, `READ_API`, exact) | deleting edge code can make a product member used by nothing (not counted) or used only by the edge (counted, fails). A product public member added for a new presenter and used only by the edge fails | either way; check `ReprParts.pureToString`, `CellText.carriedFloat(BigDecimal)`, `Styled` accessors |
| `T/EdgeOnlyNodesCensusTest.java`, `T/SqlFnSortedTest.java` | removing `FLOAT_SHORTEST_TEXT`, or moving where `CELL_TEXT` and `DATETIME_TO_TEXT` are written | re-pin |
| `T/DialectLeakRatchetTest.java` (a ban) and `T/DatabaseOwners.java` | **a presenter that branches by database** (for example "H2 prints `1.3421E-8`") names a `DatabaseType` outside the owners and fails unless the file is listed as an owner | growth fails |
| `//spec:lean_sql_corpus_*` (`LeanSqlCorpusTest`) | **a mid-chain Float text that changes from the edge's cast to lite's six decimals makes the corpus's SQL longer.** E3 §6 measured `1.3421e-8->toString()` at 47 characters today and 183 as lite's. Growth fails; a shrink past 1% fails until re-pinned. A lifted last step shrinks it | **growth fails** (Q5) |
| `ArchitectureTest.noProductClassReachesTheEdge`, `//tools/deps:edge_separation_closure_test` | a product class reaching an edge presenter | ban |

## F. A proposed slicing (smallest and safest first)

Each slice gets its narrow tests while it is built, and the edge net and the corpus at the step's end. "Lines" means
main sources only, measured from part A.

| # | What moves | Lines out of main | Judged by | Answers expected to move |
|---|---|---|---|---|
| 8.1 | **Move `FLOAT_SHORTEST_TEXT` out of the product.** The edge writes the Float `%r` from the edge's own rewrite pass, built from product nodes, or keeps the node edge-private. Then delete `FloatShortestText`'s product hooks (`spellsFloatShortestText`, `readsLongTextOnce`, 12 arms, Snowflake's spelling) | about 265 out of the product (about 230 net, if the expansion moves to the edge) | `EdgeOnlyNodesCensusTest`, `HookRulingTest`, `//core:edge_cases` (EDGE-3), PCT `format::testFormatRepr` | none (the SQL is the same text) |
| 8.2 | **A last-step value lift at legend-pure's door, PCT channel A.** Revive S6a's `PrintedRelations` `Form.VALUE`: `x->toString()` with `x` a Float or a DateTime, plus literal `replace` calls; printed by `PureValueText.pureFloat` and the date print. No deletion yet | +about 80 | `PrintedRelationsTest`, `//pct:pct_route_print_test`, `//pct:edge_net` | Computed Floats get closer to Pure. `(1.3421e-8*1.0)->toString()` goes from `1.3421e-08` to `0.000000013421`. S6a measured no PCT move; re-measure |
| 8.3 | **A scalar side lift for test bodies (channel B, corpus).** `UpstreamPrints` reads a value side the way it reads a relation side. **Needs decision 8 first** | +about 60 | `UpstreamPrintsTest`, `//pct:pct_channel_b_*`, `corpus_one` | none expected for PCT's 6 text cases. Corpus: list them |
| 8.4 | **The DateTime text out of the SQL** at legend-pure's door: delete the `DATETIME_TO_TEXT` arm, `PureDateTimePrint`, `EdgeDateTimeTexts`, the `DateTimeTexts` two-form kind, and MongoDB's `dateTimeFormat` override and hook | about 150 | `//pct:edge_net` (DuckDB, SQLite), `HookRulingTest`, `PlanStyleSurfaceTest`, the MongoDB lane | **Moves:** `format::testSimpleFormatDate` and `format::testFormatRepr` on DuckDB and SQLite, both channels (E3 §4; a `format` is not lifted). Example: `%2014-01-01T00:00:00.000+0000` becomes `%2014-01-01T00:00:00.000000000Z`. Any mid-chain date text at the engine door too (unmeasured: Q4) |
| 8.5 | **The engine door's Float text becomes an edition row (kind 2).** `toString(Float)` under `engine` lowers to the database's own cast (`FLOAT_FULL_TEXT`, plus legend-engine's literal processor) | +about 30 (rows), −0 | `//pct:engine_lane_*`, `//core:edge_pure_v1`, the corpus | none, if the row matches today's cast; H2's `PRINT_AS_FLOAT` needs its own row |
| 8.6 | **The Float pass out at legend-pure's door:** delete the `FLOAT_TO_TEXT` arm, `FloatLiteral`, the 12 `floatToText` overrides and, once 8.7 and the F31 row are done, the `spellingPasses` hook | about 230 | `//pct:edge_net`, `FloatTextTest`, `EdgeSpellingHookTest`, `lean_sql_corpus_*` | **Moves:** mid-chain Floats at legend-pure's door, for example `1.3421e-8->toString()->length()`. The corpus's validation showcase (2 per lane: `1000000` becomes `1000000.0`) moves if the corpus test code is under `pure`. Lean SQL grows (Q5) |
| 8.7 | **`TDSNull`** (1e): an edition row (the corpus's test-code side, kind 2 or 3) or a deletion | about 45 | `corpus_duckdb`, `corpus_h2`, the edge net | **as a deletion: 112 DuckDB and 107 H2 corpus tests** (S6). As a row: none. **The user must rule first** |
| 8.8 | **List text and the JSON forms at the writer:** `PureListTexts` for a last step (`Presenter.LEGEND_PURE` already prints a list); graph keys, the object-reference connection and one-bare in `PureV1Writer`; delete `graphKeyDate`/`String`, `GraphKeyText`, `executeValues`, `objectReferenceConnection` | about 200 | `//core:edge_pure_v1`, corpus graph-fetch tests, the edge net | the corpus's `serialize`-then-compare tests are mid-chain. **Measure first**; likely edition rows, not deletions |

**Totals:** slices 8.1, 8.4 and 8.6 alone remove about 600-650 lines (8.2 and 8.3 add about 140). With 8.7 and 8.8
it is about 900-1,100. Reaching "about 2,000" would need the `PureOnlyTexts` facet (232) and step-9 items
(`traceCell`, `ExecutedValues`, `ResultEnvelopes`, the test-data printing); I would not count those here.

**Before 8.6 and 8.7:** F31's `UnguardedParses` has to become an edition row, because it is the last thing in
`spellingPasses` that is not a spelling.

## G. Open questions for the user

1. **Two printers at one door.** After step 8, a last-step `toString` gives legend-pure's text (Java) and a mid-chain
   one gives lite's (SQL). Is that accepted under tenet 1? Example: `1.3421e-8->toString()` is `0.000000013421`, but
   `…->toString()->length()` is 3 (`0.0`). B2b's audit pulled S6a for exactly this.
2. **The scalar side lift (decision 8).** Is it ruled, and how far does it reach: `toString` only, or also `format`
   and `makeString` at a side's root? Lifting `format` means Java writes `format`'s text from typed operands.
3. **`TDSNull`:** an edition row (no moves), or presentation with 112/107 corpus moves?
4. **The engine door:** is the Float or DateTime text inside a store query legend-engine's answer (an edition row,
   §0), or a presentation? Nobody has read or asked legend-engine's DateTime text inside a store query (E3 §5); ask the
   engine runner.
5. **Lean SQL in the corpus:** may `//spec:lean_sql_corpus_*` grow where a mid-chain cast becomes lite's six-decimal
   text? It is "compatibility" SQL, but the pin plans under **U**.
6. **`PureOnlyTexts`** (`%r`, `%0wd`, `@_`): the product refuses these, so they cannot be re-spelled. Do they stay
   edition rows?
7. **MongoDB's date format** (third review P-13): which edition or presenter owns it?
8. **The step's target size:** "about 2,000 lines" looks like an overestimate for text alone (§F). Restate it, or
   count step-9 items?
