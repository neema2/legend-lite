# One plan, two presentations: the design (2026-11-18, revised after review)

**Status.**
- **The ruling.** The user ruled the seven decisions on 2026-11-18 ("go with your recommendations on all seven
  decisions", §0).
- **The review.** An independent xhigh review then found facts that ruling was given without:
  `docs/records/presentation-design-review-2026-11-18.md` on `claude/refactor-code-quality-review-9fqhug`. Its verdict
  was READY WITH CORRECTIONS, with 16 corrections.
- **This revision** applies the corrections and answers each one in §10. Three decisions are re-opened and two are new,
  each marked **PENDING THE USER**. The rest stay **RULED**.
- **Nobody builds before PR #118 merges.**

**Revisions:**
- `75e3fbcd1`: the draft.
- `079c343d3`: the ruling recorded.
- This commit: the review applied.

The design is of the approved program `docs/records/edge-portability-program-2026-11-18.md` (on
`claude/total-wall-orch`; "the program" below). That program starts once PR #118 (`claude/total-wall-d-stage` →
`main`, head `68f20eb4d`) has merged and its fix round (F1-F8, `claude/pr118-fixes`) has landed.

**Inputs read:**
- AGENTS.md at the PR head.
- The program.
- The plan of record `docs/records/total-wall-edge-program-2026-11-16.md`: §1-§11, and the §12 log from 2026-11-16
  15:00 to 2026-11-18 14:47, including the error design adopted at 2026-11-16 21:20 and the judge decision of
  2026-11-17 02:30.
- The PR #118 audit (`docs/records/pr118-audit-2026-11-18/`).
- The review of this design.
- The code at `68f20eb4d`.
- Upstream, read through blob-less clones at the pinned tags (§8):
  - legend-engine 4.145.0 (`230c1591`): `pureToSQLQuery.pure`, `h2Extension2_1_214.pure`, `duckdbExtension.pure`, the
    protocol's plan nodes, the DuckDB and H2 PCT manifests;
  - legend-pure 5.99.0 (`7fbc7d6e`), through the review's reads.

**Measurements:** every number in §2 was measured at `68f20eb4d`, by this design or by the review where it says so. The
commands are in §9.

---

## 0. Rulings, and what is pending

**RULED by the user, 2026-11-18 (about 16:30 UTC), relayed by the orchestrator (`session_0195rQEoRb1AGCHxtZChjWsg`),
verbatim:** "go with your recommendations on all seven decisions".

The orchestrator then asked, on the review's evidence, that the hidden re-rulings become explicit decisions, because
the ruling "was given without them in view". The status of each:

| # | Decision | Status | Where it stands |
|---|---|---|---|
| (a) | Raise operands | **PENDING THE USER** (re-opened; review H2) | A1 was ruled without three facts. It re-opens item (5) of the error design adopted with the user on 2026-11-16 21:20 ("Dropped: the operand carriage … per-type read-back"). Its error transport has never been measured on any database: it is not the probe's value carrier. And it does nothing for pure/v1. **My recommendation changes to A3:** a computed-operand site at the edge is worded with lite's sentence and no value, as a register row. That is the choice the plan of record promised at Phase 2's end (2026-11-16 21:30). A1 is built only when a first real case appears. |
| (b) | The kind-B rows | **RULED**, except two items **PENDING THE USER**: R1 (its cost now measured) and H6 (a new register row) | Two corrections within the ruled rule: it is scoped by answer (H7), and it gets a sub-rule for databases legend-engine has no extension for (H5). Rows were corrected per database: B6, B10, B11, R3. |
| (c) | The test plan's words; the database judge | **PENDING THE USER** (re-opened; review H3) | Option C4 is added: the plan of record's DECIDED entry of 2026-11-17 02:30 (port the 36 assert-and-shape combos into core, PCT HOST-only, delete the compatibility database judge). **Recommendation: C4.** |
| (d) | The safety net | **RULED** (D1) | Its premise is corrected: channel B and channel A already pin messages; the corpus, and channel B on H2 and SQLite, do not. Re-sized to 2.5 days. |
| (e) | Import | **RULED** (E1) | The node table is completed. S9 starts with a census of the node types of legend-engine's own plans. |
| (f) | `EngineResultEnvelopes` | **RULED** (retire) | One clarification: the `ENGINE_*` forms are written by `PureV1Writer` inside a LEGEND_PURE plan too. |
| (g) | Plan format | **RULED**, with one fact corrected | "Two bumps" was wrong, because later slices add fields. Corrected: once S1 generates `PLAN_VERSION` for the hosts, each slice that changes the format bumps it. The ruled intent (generated host constants, exact-version reading) is unchanged. |
| (h) | Lite's own sentence in the product plan | **PENDING THE USER** (new; review H1) | The draft's done-when 2 and S1 removed lite's sentence and `raises` from every product plan. That re-opens item (2) of the 21:20 design ("the plan carries a site table: kind, where, and both messages RENDERED AT PLANNING … exec only looks it up … the hosts read the same table"). The program does not need it. **Recommendation: K1, keep as built.** |

**Design corrections adopted without a ruling.** They change no ruled decision and are listed for the user's note:
- **H4:** the product's records carry no upstream names. `presentation` is a name (`"lite"` in the product), and edge
  facts ride an opaque `Extension` seam (§3.3).
- **H5:** on a database with no legend-engine extension, the edge writes the product's form unless a row rules
  otherwise (§3.2).
- **H7:** rule 1 is scoped by answer, not by SQL (§3.2).
- **H8:** no generated sentence tables. This is moot under K1; under K2, a Java error half serves both families.
- **H9:** one version bump per format-changing slice.
- **Review item 12:** families are passed in explicitly by the composition roots. No `ServiceLoader` discovery is
  used, so nothing needs proving in TeaVM or GraalVM native-image.

**Pending the user, one screen:**

| Item | Choice | Recommendation | Why |
|---|---|---|---|
| (a) | A1 carriers / **A3 lite's sentence, no value** / A4 keep the capped in-SQL sentence | **A3** | 0 upstream-facing cases (census, 2026-11-16 21:05); keeps 21:20's "exec only looks it up"; 0.5 day |
| (c) | C1 interim / **C4 PCT HOST-only (02:30)** | **C4** | already DECIDED in the plan of record; PCT showed 100% agreement, so the database judge brought 0 signal; deletes the machinery C1 would maintain |
| (h) | **K1 keep lite's sentence in the plan (21:20 + K10)** / K2 remove it | **K1** | the program needs no change to lite's sites; the product plan's bytes do not move |
| R1 | a DateTime/Boolean `toString` in a query: R1-a keep legend-pure's print in SQL / **R1-b legend-engine's cast** / R1-c present at roots | **R1-b, after S5a's count** | the user's own rule for Floats ("for toString inside an expression use engine cast( as varchar)"). Cost: 3 DuckDB tests (13 cases) and H2's `testBooleanToString` go from pass to fail in channels A and B, exactly as legend-engine's manifests expect |
| H6 | kind-2 roots that legend-engine refuses (12 `format` tests on DuckDB): **H6-a present legend-pure's answer (PCT, corpus), refuse on pure/v1** / H6-b refuse everywhere | **H6-a** | where legend-engine refuses, legend-pure's answer is the only upstream answer; no PCT move; a register row |

---

## 1. The goal and "done when", by phase

**The goal, in the user's words** (2026-11-18 05:00 and 05:15, verbatim in the program's §1): "mostly everything that
could be would really be at the edge edge mostly flexing the product and then trying to fix up later in java instead
of parallel tracks for product and compatibility". And: "our edge should really be plans that don't do a lot of the
stuff in SQL", so that lite can "import upstream plans to run on our platform and export plans from us to to be able
to run in upstream".

### Phase 1: presentation (S0-S7, about 18.5 working days)

Done when:

1. **One plan.** For every case of the following, every statement the edge sends is the product's statement for the
   same expression, except statements a ruled kind-B row changes:
   - PCT channel A (DuckDB, H2, SQLite, Postgres, and each lane database by a lane round);
   - PCT channel B;
   - both relational corpora;
   - the pure/v1 golden requests.

   `//core:one_plan_measure` (a heavy lane, §6 G3) attributes every difference to exactly one row.
2. **No upstream word in a product record.**
   - No component of `ExecutionPlan`, `TestPlan` or their sub-records holds legend-pure's or legend-engine's text.
   - Upstream presentation lives only in edge `Extension` data, or in edge writers.
   - Lite's own sentence stays where the 21:20 design put it under (h) K1. Under K2 it leaves the plan too.
   - Checked by §6 G2 over every plan those suites make, in both styles.
3. **No edge class builds upstream text as SQL** except ledgered rows (§6 G1). These are deleted:
   - `RelationTextLowering`;
   - `PurePrintForms`' print passes (`TdsNullCells`, `joined`, the `DATETIME_TO_TEXT` arm);
   - `EdgePureOnlyTexts`, `PureListTexts`, `PureDateTimePrint`, `EdgeDateTimeTexts`;
   - `PureMessages.sentence`, `EngineResultEnvelopes`, `EngineExecutedValues`, `EdgeLoweringForms.oneBare`;
   - `SiteWordsRecorder` and the hosts' unread `siteWords`;
   - under (c) C4, `UpstreamVerdictWords` and the compatibility database judge.
4. **No outcome moved except ruled moves.** PCT channel A matches its expected-failure lists, channel B its ledgers,
   both corpora their rosters, and the net (§4.d). Each exception is a ruled move with upstream's answer read (§8),
   for example R1's if it is ruled R1-b.
5. **The product is untouched** under K1 and A3. The product plan's JSON is byte-identical except its version number,
   so the `lean_sql_*` pins (which count plan-JSON characters, `LeanSqlCorpusTest.measure`) do not move. Under K2 or
   A1 they move, each with a dated note.
6. **Lean.**
   - The edge's SQL characters shrink.
   - The judge-plan pins (PCT channel B `*.judge.plan`, the corpus lean pins) only shrink, each with a dated note.
   - Under C4 the compatibility database judge's plans are gone.

### Phase 2: portability (runner tooling R, S8, S9, about 9.5 working days)

Done when:

7. **Export.** A lite plan exported as legend-engine's `SingleExecutionPlan` and executed by legend-engine 4.145.0's
   `PlanExecutor` (the engine runner, after slice R) gives answer bytes identical to lite's edge answer for the same
   plan.
   - The corpus: the pure/v1 golden requests plus a fixed set of parameterized corpus service queries, so that
     FreeMarker values are exercised.
   - Activities are compared by §3.9's rule, and the trace id is masked.
   - A plan that cannot be exported is refused by name; every refusal is listed and counted.
8. **Import.** legend-engine's own plans for the same queries, generated by its `PlanGenerator`, are listed by node
   type first (S9's census).
   - Every plan whose nodes map imports, and lite runs and presents it with answer bytes identical to legend-engine
     executing it.
   - The refused share is stated with its node types, so the measure cannot pass vacuously.

---

## 2. Where presentation happens today (measured at `68f20eb4d`)

### 2.1 Every SQL text builder

The columns are:
- *lines*: the whole file;
- *code*: non-blank, non-comment lines;
- *SQL code*: code lines inside methods whose declared return type is a MIR or rewriter type (`SqlExpr`, `SqlSelect`,
  `SqlQuery`, `SqlSource`, `SqlWriter`, `SqlRewriter`, `TypedSpec`);
- *Java words*: code lines in methods returning `String` or another non-MIR type.

The review's counter, which also counts signature lines, gives about 4% more (909 for the nine, ≈ 1,135 in all); the
code columns agree exactly. Every row is in `core/src/main/java/com/legend/edge/` unless marked.

| File | Lines | Code | SQL code | Java words | What it builds in SQL | Reached by | Class (§3.2) |
|---|---|---|---|---|---|---|---|
| `RelationTextLowering` | 781 | 556 | **459** | 63 | A relation's `toString` (`#TDS` text), `toCSV` and a TDS's `csv`, composed over the rows with `STRING_AGG`/`CONCAT` | channel B, the corpus, pure/v1 (`Lowerer.java:4317`, the `RelationTexts` facet) | kind 2: **MOVE**. 0 of 2,710 DuckDB channel-A answer lines (0 on H2 and SQLite too) is such a text computed in the query |
| `PureMessages` | 481 | 325 | 93 | **228** | `sentence` (57 lines): legend-pure's raise sentence by `CONCAT` for a computed operand | 12 edge `raiseMessage` overrides | SQL half: **decision (a)**. Java half: kept |
| `UpstreamVerdictWords` | 198 | 124 | 79 | 14 | The compatibility database judge's legend-pure failure words (24 `VerdictWords` methods) | the database judge under the edge's style | **RETIRE** (C1), or deleted with that judge (C4) |
| `PurePrintForms` (+ `TdsNullCells`, `UnguardedParses`) | 184 | 106 | 85 | 6 | `FLOAT_TO_TEXT` → a cast (row 8); `DATETIME_TO_TEXT` → legend-pure's print; `CELL_TEXT` → `TDSNull` and the `joined` interleave; a parse → a plain cast | 12 `spellingPasses` | `floatOf`, `FloatLiteral` and `UnguardedParses` (≈ 30 lines): **KEEP** (B1, B2, B6). The rest: MOVE, or R1 |
| `EdgePureOnlyTexts` | 136 | 87 | 72 | 2 | `format`'s `%0Nd` and `%r`, `toRepresentation`, an instance's `@_` id | the `PureOnlyTexts` facet | kind 2: **MOVE** |
| `EngineResultEnvelopes` | 92 | 60 | 52 | 2 | legend-engine's `tdsBuilder`/`classBuilder` envelopes by the database's JSON constructors (EDGE-8) | `FramePlanner:126-141` → `EngineExecutedValues` (the corpus's `executeLegendQuery`) | **RETIRE** (f) |
| `PureListTexts` | 49 | 26 | 19 | 2 | A list's `[a, b]` | the `ListTexts` two-form kind | kind 2: **MOVE** |
| `PureDateTimePrint` | 43 | 19 | 15 | 2 | legend-pure's DateTime print by `STRFTIME`/`CONCAT` | `PurePrintForms`, `RelationTextLowering` | **MOVE**, or R1 |
| `EdgeDateTimeTexts` | 28 | 10 | 4 | 2 | A DateTime literal's written text | the `DateTimeTexts` two-form kind | **MOVE** |
| *the program's nine* | *1,992* | *1,313* | ***878*** | *321* | | | |
| `EngineExecutedValues` | 44 | 19 | 13 | 0 | legend-engine's json-builder `values` envelope | the `ExecutedValues` two-form kind | **RETIRE** (f) |
| `EdgeLoweringForms.oneBare` | (in 262) | — | 15 | — | The JsonBuilder cardinality fold (`CASE COUNT(*)=1 …`) | `LoweringForms.executeValues` | **MOVE** (a count of written rows) |
| `TestDataRoutes.seedJson` and two others | (in 519) | — | 41 | 234 | legend-engine's seed answer JSON and key texts | the `Printing` two-form kind | **MOVE** (S306) |
| `PureText` (12 dialect subclasses) | 731 | 469 | 139 | 214 | Dispatch wiring: 46 overrides (12 × `spellingPasses`, 12 × `floatToText`, 12 × `raiseMessage`, 5 × `floatLiteralKind`, 2 × `raw`, and one each of `scanOrderPasses`, `refusesDecimalArithmetic`, `dateTimeFormat`) | every edge plan | shrinks to the KEEP rows' overrides |
| `planner/PlanEnv.traceText` (product) | — | — | 5 | — | legend-engine's trace comment around the bound execution id | `PlanStyle.traceCell` | **MOVE** |
| **Total** | | | **≈ 1,090** | | | | |

**What leaves:** about **920** SQL code lines are deleted. That is the ≈ 1,090 less PureText's wiring for the KEEP rows
and PurePrintForms' ≈ 30 KEEP lines. PureMessages' 228-line Java half stays and becomes the LEGEND_PURE family's
words.

**Not presentation**, so it stays and this program does not count it:
- `StableScanOrder` (kind B, B4);
- `RawSqlBoundary` (input);
- `CompiledPureVerdictNumbers` (kind D);
- `TestDataFetches` (kind C);
- **legend-engine's plan-text family**: `EnginePlans`, `PlanText`, `EngineStyleH2/DB2/Composite`, `PlanEnumForm`,
  `InProtocol`, `PlanVariables`. That is **5,541 lines, measured**; the program's "~9,400" was not a measurement. It is
  kind C: the answers of `toSQLString`/`planToString` and of the engine-text asserts. **It is not export's base**,
  because export writes lite's own SQL in legend-engine's plan format (§3.9). This departs from the program's §5
  table, which put export on these renderers.

### 2.2 The hook kinds and facets that carry presentation into the plan

`PlanStyle` (`database/PlanStyle.java`) has 10 methods. `testplan.PlanStyleFacets` lists 5 `KINDS` and 16 `TWO_FORMS`.
`LoweringForms` has 13 members.

| Hook | Shape | Means | Target |
|---|---|---|---|
| `RelationTexts`, `PureOnlyTexts`, `ResultEnvelopes` (facets) | `SqlExpr` | presentation | deleted |
| `UpstreamPlans` (facet) | plan text, Java | kind C | stays |
| `EngineTestText` (facet) | judge arm | kind D | stays |
| `DateTimeTexts`, `ListTexts`, `ExecutedValues`, `TestDataPlanner.Printing` (two-form) | `SqlExpr` | presentation | deleted (writers); `DateTimeTexts` per R1 |
| `VerdictWords` (two-form) | `SqlExpr` | the judge's words | the edge's form deleted (C1, or C4 with its judge) |
| `DateLiteralTexts`, `RoutedQueries`, `ActivityTexts` (two-form) | Java text at planning | kind C (upstream's plan text) | stay |
| `GoldenNullWord`, `RenderedTextRule`, `JsonGrammar`, `RelationCounting`, `CrossKindNumbers`, `VerdictNumbers` (two-form) | rules | kind D | stay. Under C4, those only the compatibility database judge reads go with it |
| `DatabaseSettingsForm` (two-form) | settings | kind B (B5) | stays |
| `LoweringForms` (two-form) | 13 members | **9 computation (kind B), 1 presentation** (`executeValues`/`oneBare`), **1 protocol constant** (`objectReferenceConnection`, kind C, row C1), 2 plan text only (`unionFrame`, `EngineShape`'s own) | the presentation member leaves |
| `PlanStyle.siteWords` | a stateful recorder | presentation | replaced by a pure function that returns edge data (§3.5) |
| `PlanStyle.errorWords` | Java words at planning | presentation (a planned `Failure`'s message) | replaced by kinds (§3.7) |
| `PlanStyle.traceCell` | constants into SQL | presentation | the writer |
| `PlanStyle.graphKeyDate`/`graphKeyString` | a literal at planning, inside the database-built graph JSON | kind C (row C2) | stays, listed |

### 2.3 The plan fields that carry presentation

| Format | Field | Today | Target |
|---|---|---|---|
| plan v58 (`PlanJson.java:171`) | top-level `raises` (kind → lite's sentence, `:241`, `:255-264`) | lite's sentence | **(h) K1: kept.** K2: kinds only |
| plan v58 | `sites[].message` (`:1405-1442`; `ExecutionPlan.Site:843-870`) | lite's sentence, checked on read | **K1: kept.** K2: dropped, `operands` added |
| plan v58 | `Column.precision` | a fact | kept: both writers read it |
| host plan halves (`HostAnswers.arrowPlan:179`, `jsonPlan:234`) | `siteWords` (`:212`, `:279`) | legend-pure's words, **read by no host** (ARCH-1) | deleted. The words move into the plan's edge extension, and the hosts' edge routes use the Java error half (§3.8) |
| test plan v32 (`TestPlanJson.java:60`) | `TestPlan.words` | a name (`"lite"` or the edge's) | kept, as the same name space as the product plan's `presentation` |
| test plan v32 | `answerSizeWords`, `SideShape.sizeWords` | a sentence worded at planning | dropped. The hearing words it from `SideShape.multiplicity`/`lower` |
| test plan v32 | a planned `Failure.message` (`AssertErrorNative:76-82` via `PlanStyle.errorWords`) | upstream's sentence at planning | `Failure(kind, operands, message)`, where `message` is lite's; the hearing words upstream's |
| test plan v32 | `SiteWords` extension | edge data, produced by the stateful recorder | kept as edge data, produced by a pure function (§3.5) |
| test plan v32 | the database verdict row's `MESSAGE_COLUMN` (`RootLoop.java:43`) | words built in SQL | product: lite's words stay (01:30 ruling). Edge: deleted with the judge (C4), or lite's words (C1) |

### 2.4 The golden net: what it pins today

**Messages that are pinned** (corrected; the draft said none were):
- PCT channel B on DuckDB: the outcome files hold the full message of every non-PASS row (9 in essential: 5
  `AssertFailed`, 4 `DataError`).
- The channel-B ledger `pct/src/test/resources/org/finos/legend/lite/pct/channelb/ledger.tsv` (420 lines) pins a
  message substring for each expected failure (295 across 11 lanes, by the review's count).
- Channel A's `ERROR` answer lines pin their messages: DuckDB 27, H2 26, SQLite 34.
- pure/v1's 5 error bodies are pinned.

**Messages that are not pinned:**
- The corpus `*_judge-host.tsv` FAIL rows carry no message: DuckDB 62, H2 78.
- Channel B is in the net on DuckDB only, not on H2 or SQLite.
- The words `UpstreamVerdictWords` builds reach no reader under BOTH, so no golden can pin them.

**Lite's own SQL is pinned as if it were an answer:**
- `PctCaseRecorder.java:108-114` records each relation answer's `activities`. That is 1,486 lines: DuckDB relation 460
  and standard 64; H2 439; SQLite 459 and 64. The PCT adapter never reads them.
- pure/v1's 8 activity answer lines carry lite's own SQL (`"t0"` aliases). That one is client-visible.

**Values and order are hidden:**
- The mask `[0-9a-fA-F]{8}-…|\b[0-9a-fA-F]{16,}\b` (`PctCaseRecorder.java:64-65`) erases 171 values in VALUE lines (88
  DuckDB, 83 SQLite).
- `EdgeNormalize` sorts every relation's rows.

**Not in CI:** `//pct:edge_contract_test` is in `//gates:heavy` only (`gates/BUILD.bazel:489-491`).

### 2.5 The hosts and the other plan readers

| Reader | Reads | Words a raise from |
|---|---|---|
| Java `exec/RaisedErrors` | the site by token | `ErrorKind`; plus `edge.RaisedWords` on the server and PCT routes |
| TypeScript `engine-client/src`: `raised.ts:58-67`, `declared-raises.ts`, `plan-runner.ts:301` (`PLAN_VERSION = 58`) | `sites[].message`, `raises` | the plan's `message` |
| Python `python/legend_lite`: `_raised.py:73-95`, `_runs.py:27` (`PLAN_VERSION = 58`) | `sites[n-1]['message']` | the plan's `message` |
| `datacube/src` (`lite-planner.ts`, `table-output.ts`, `remote.ts`, `upload.ts`), `query/src/app/run.ts` | plans through engine-client | engine-client |
| the shared fixture `engine-client/test/fixtures/raise-envelopes.json` (`LEGEND_LITE_RAISE_FIXTURE`, `python/tests/test_planned.py:1132`, the tab's tests) | raise envelopes | — |

- **Neither host reads `siteWords`.**
- **The answer half is Java.** Both hosts present legend-engine's answers by calling the Java edge's answer half: wasm
  `EngineExports:26-46` (a separate TeaVM module, `//wasm:engine_planner`) and native `EngineEntries`
  (`//native:engine`). Neither has an error half.

### 2.6 Where this reading differs from the audit (and where the review corrected it)

1. **The SQL-code count agrees** with the audit's EDGE-4 (878 against ~900; the review's counter gives 909). EDGE-4's
   missing list is right; `TestDataRoutes.seedJson` and `PlanEnv.traceText` belong to it too.
2. **`LoweringForms` holds one presentation member** (`executeValues`/`oneBare`) and **one kind-C constant**
   (`objectReferenceConnection`); the rest are computation. ARCH-2 counted it "mostly computation".
3. **Relation text needs no kind-B row.**
   - The evidence: 0 channel-A cases on DuckDB, H2 and SQLite compute a relation's text inside the query.
   - legend-engine maps none of `toCSV`, `format`, `makeString` or `toRepresentation` among its 451 function pairs.
   - legend-pure's tests call `toString` outside `$f->eval(…)`.
   - The text-producing pairs it *does* push are `toString_Any_1` (`pureToSQLQuery.pure:10663`), `formatDate` ×2
     (`:10634-5`), `joinStrings` ×2 (`:10647-8`), `toJson` and `parseJson`. These are kind 1 (§3.2).
4. **Raise operands have no measured golden:** D's census of 2026-11-16 21:05. The review spot-checked channel A's
   ERROR messages and found only literal operands.
5. **ARCH-1's premise is refuted by V-HOSTS:** the server answers `Raised: <KIND>`. F5 in the fix round sets the
   baseline.
6. **Corrected by the review:** the net *does* pin failure messages for channel B on DuckDB, the channel-B ledger and
   channel A (§2.4). Only the corpus, and channel B on H2 and SQLite, lack them.
7. **pure/v1's activity SQL is lite's.** That holds today and stays so under one plan (register row C3).

---

## 3. The target design

### 3.1 The principle

- **The plan says what runs and whose text a reader gets.**
  - The statements compute typed values.
  - `presentation` is a name: the product writes `"lite"`, and the edge its own names.
  - The facts are typed.
  - Upstream presentation facts are **edge `Extension` data**, opaque to the product.
- **The format is the caller's** (`Output`: JSON rows, CSV, Arrow, pure/v1 JSON, a collector), chosen at run.
- **The runner is unchanged.** It sends the plan's statements and streams typed rows (G1, G4). Exec still only looks up
  a raise's site (the 21:20 design).
- **Writers present.** They decide nothing (`WriterCarriageGuardTest`).
  - The product's: `CellText`, `JsonRowsWriter`, `CsvWriter`, `ArrowWriter`.
  - The edge's: the families of §3.4, passed in explicitly by the edge's composition roots.
- **Kind B is the exception, and it is listed.** It is answer-driven (§3.2) and ruled row by row.

### 3.2 The rule: computation or presentation (restated by answer)

For each text-producing function:

1. **Kind 1: legend-engine pushes it to SQL.** It has a `PureFunctionToRelationalFunctionPair` in
   `pureToSQLQuery.pure` and a `dynaFnToSql` in the database's extension.
   - **The rule (H7):** inside a query, the edge writes **the product's SQL, unless a measured answer differs from the
     answer of legend-engine's SQL**. Then it writes legend-engine's SQL, as a ruled kind-B row.
   - **The kind-1 text functions at 4.145.0**, each compared by answer in S5a:

     | Function | H2 | DuckDB |
     |---|---|---|
     | `toString_Any_1` | `cast(%s as varchar)` (`h2Extension2_1_214.pure:266`) | `:284` |
     | `formatDate` | — | `:302`, a transform |
     | `joinStrings` | `group_concat(%s separator %s)` (`:235`) | `string_agg(%s,%s)` (`:240`) |
     | `toJson` (Variant) | — | — |
     | `parseJson` | `legend_h2_extension_json_parse(%s)` (`:250`) | `cast(%s as JSON)` (`:259`) |

   - **The sub-rule (H5):** on a database legend-engine 4.145.0 has no extension for (SQLite, MariaDB, MySQL, MongoDB),
     there is no legend-engine SQL to follow. The edge writes the product's form unless a row rules otherwise. B3 (SQLite
     REAL) and `FloatLiteral.PRINT_BARE` ("the closest form where legend-engine has no extension") are such rows today.
2. **Kind 2: legend-engine has no SQL for it.** This covers relation `toString`, `toCSV`, a TDS's `csv`, `format`,
   `makeString`, `toRepresentation`, an instance's id and the result envelopes.
   - It is **presentation** where its value is the answer's root or an assert's side. The plan returns the operand's
     typed rows, and a writer prints them in Java with legend-pure's print.
   - **At a channel-A eval root, legend-engine refuses these at planning.** Its DuckDB essential manifest lists 12
     `format` tests, each "No SQL translation exists for the PURE function 'format_String_1__Any_MANY__String_1_'".
     Lite passes them today and will keep passing by presenting in Java. That is better than legend-engine and equal
     to legend-pure, so it is **H6, PENDING THE USER** (a register row).
   - On pure/v1 (LEGEND_ENGINE) the edge refuses them in legend-engine's words, under the 2026-11-17 ruling "the
     pure/v1 route matches legend-engine".
   - **Bounded evaluation (G1 one evaluator; G6 small Java formatting).** A presentable root is one of a **closed
     list**. Each item is one Java port of one legend-pure native, over SQL-computed typed operands:
     - relation `toString`, `toCSV`, a TDS's `csv`;
     - `format` with a *constant* format string (`%s %d %f %t{…} %r`, and padding);
     - `toRepresentation`;
     - `makeString` over a typed list of primitives;
     - an instance's `@_` id;
     - the three result envelopes.
   - **Nesting.** A kind-2 call nested in another is presented only where the outer native's own rule takes the inner
     value's text (`format`'s `%s` over a `toString`). Any other nesting, and any computed format string, is refused by
     name.
3. **A kind-2 call whose text feeds a computation inside the query** has no upstream SQL form. It is refused by name,
   in legend-engine's words where legend-engine refuses.

**(b0) The query boundary.** No channel-B boundary marker for now. R1-b interacts with it: legend-pure prints a DateTime
`toString` written *outside* an eval in a channel-B body, and R1-b would give it legend-engine's cast. So S5a counts
those sites in the PCT sources before R1 is built, and a non-zero count brings the marker back to the user.

### 3.3 The records (plan v59 in S1; later slices bump per §4.g)

`Extension` and `ExtensionCodec` move down from `testexecutionplan` to `executionplan`, so both plan types share the
one seam the edge already uses (H4).

**The codec is passed in explicitly.** Today the test plan finds its codec by `ServiceLoader` (`TestPlanJson.java:283-295`,
JVM only). A product plan's extensions are also read in the TeaVM and native edge modules, by the error half (§3.8).
So `PlanJson.read(text, ExtensionCodec codec)` takes the codec from its caller, as §3.4 takes the families: the
product's callers pass `ExtensionCodec.NONE`, which refuses any extension by name, and the edge's roots pass
`EdgeCodec`. The hosts never decode an extension themselves; they call the Java edge.

```java
// com.legend.executionplan -- the product's records
public interface Extension { }                       // the compatibility edge's data: opaque to the product

// ExecutionPlan gains two components; both are OMITTED from the JSON at their product value, so a product plan's
// bytes are unchanged but for its version (done-when 5):
//   String presentation                         -- "lite" (ExecutionPlan.LITE) in the product; the edge writes its own
//                                                  names ("legend-pure", "legend-engine"); the product compares it with
//                                                  the family a run is offered and reads nothing else from it
//   Map<String, List<Extension>> extensions     -- by key: "root", or "statement:<i>"; empty in the product

// Site is unchanged under (h) K1: Site(int n, RaiseKind kind, @Nullable String where, String message)
```

The edge's records:

```java
// com.legend.edge
/** Upstream's words of site n of statement i, rendered at planning (K1; the 21:20 design's "both messages rendered
 *  at planning"); replaces SiteWordsRecorder and HostAnswers.siteWords. */
record SiteWords(int statement, int n, String words) implements Extension { }

/** A root's or a side's value is the PRESENTATION of its typed rows (§3.2 kind 2). */
record Presented(Form form, boolean sorted) implements Extension {
    enum Form { PURE_TDS_TEXT, PURE_CSV, PURE_TDS_CSV, PURE_FORMAT, PURE_REPRESENTATION, PURE_MAKE_STRING,
                PURE_INSTANCE_ID, ENGINE_TDS_JSON, ENGINE_CLASS_JSON, ENGINE_VALUES_JSON }
}
```

**Test plan v33:**
- `words` stays the name, in `presentation`'s name space.
- `answerSizeWords` and `SideShape.sizeWords` are dropped.
- `Failure` becomes `(kind?, operands, message)`, where `message` is lite's.
- Presented sides carry `Presented` in `extensionsByVerdict`.

**The separation lens** (`EdgeOnlyDataTest`) holds `ExecutionPlan.presentation` and `ExecutionPlan.extensions` as
product facts with no upstream meaning, one pinned row each: a name the product only compares, and an opaque map the
product never reads. No upstream name enters a product enum.

### 3.4 The presenter family, passed in explicitly

**There is no ServiceLoader lookup** (review item 12). Each composition root that serves an upstream presentation hands
its family to the run beside the caller's `Output`. A plan whose `presentation` differs from the offered family's is
refused before any statement runs.

The roots are:
- `EdgeServer`, `PureV1Routes`;
- `PctPlanRoute`, `PctExecuteNative`;
- the test run hook (`EdgeRun`);
- wasm `EngineExports` and native `EngineEntries`.

```java
// com.legend.result (product)
public record PresenterFamily(String presentation, ErrorWords errors, ValueTexts values, AnswerWriters answers) {
    public static final PresenterFamily LITE = ...;   // CellText, JsonRowsWriter, CsvWriter, ArrowWriter, ErrorKind
}
public interface ErrorWords  { String words(com.legend.error.ErrorKind kind, @Nullable String where, List<Object> operands,
                                            @Nullable Extension planned); }
public interface ValueTexts  { String print(@Nullable Object value, String pureType, ExecutionPlan.@Nullable Precision p);
                               String presented(Extension form, List<ExecutionPlan.Column> columns, List<List<Object>> rows); }
public interface AnswerWriters { ResultSink writer(Output output, ExecutionPlan plan, java.util.@Nullable UUID execution); }
```

| Family | `errors` | `values` | `answers` |
|---|---|---|---|
| `LITE` (product) | `ErrorKind.sentence()` + where | `CellText`; refuses `presented` (S316b) | the product's writers |
| `edge.Families.LEGEND_PURE` | the planned `SiteWords`, or lite's sentence for a computed-operand site (A3); `PureMessages.compile` for compile kinds | `Presenter.legendPure`, `edge.RelationText` (Java: `#TDS` text, `toCSV`, `csv`, `TDSNull` at an empty cell, `escapeCsv`), the `format`/`%r`/`toRepresentation` ports. **The `ENGINE_*` forms are written by `PureV1Writer` here too**: the corpus is LEGEND_PURE, but its `executeLegendQuery` value is legend-engine's JSON (review (f)) | the PCT route's engine JSON |
| `edge.Families.LEGEND_ENGINE` | pure/v1's words (census classes C, D, E; F43/F44 read first) | `Presenter.engine`; refuses kind-2 roots by name in legend-engine's words (§3.2 rule 2) | `EngineAnswers.engine`/`.arrow`, `PureV1Writer` with the JsonBuilder cardinality rule |

### 3.5 Raises (the recommended K1 + A3; the alternatives in §4)

1. **Planning.**
   - The product writes the token and the site `(n, kind, where, message)`, as today.
   - Under an edge style, the planner asks the style
     `List<Extension> siteExtensions(int statement, String text, List<RaiseSite> sites)`. This is a **pure function**
     (data out, no state), and the planner places its `SiteWords` in the plan's `extensions`.
   - A literal-operand site gets legend-pure's or legend-engine's words (`PureMessages.wordsOrNull`).
   - Under A3, a computed-operand site gets lite's sentence (a register row).
   - `PureMessages.sentence`, `SiteWordsRecorder`, `PureText.recording` and `PlanStyle.siteWords` are deleted.
2. **Run.** Unchanged. Exec reads the token and looks the site up, with no parsing and no formatting (the 21:20
   design).
3. **The boundary.**
   - The edge door's family words a raised site from the plan's `SiteWords` for `(statement, n)`.
   - A site with none under an edge plan is refused by name: that is a planning bug.
   - `RaisedWords` becomes this one stateless lookup.

### 3.6 Presented answers and presented sides

- **A root.** A query whose root is a presentable kind-2 call plans its operand's typed result with a `Presented`
  extension on `"root"`. The writer presents it as a VALUE.
- **A side.** A test body's side whose value is a presentable kind-2 call over a query result is a TABULAR side with
  `Presented`. The **host judge** reads the rows, presents them, and compares the strings by the `RenderedTexts` rule.
  S131 and S132's leniency are re-measured in S4 and are expected to shrink.
- **The database judge.**
  - Under (c) C4 there is no compatibility database judge, so there is nothing to decline.
  - Under C1 it declines a presented side by name, and `IsolatedRun.difference` skips declines under the
    compatibility rules. That works, but it stops the database judge on every relation-text assert (all 469 channel-B
    relation tests).
- **EDGE-1, EDGE-2, EDGE-3 and EDGE-8 go by construction:**
  - a Float cell prints by `DecimalFormat("0.0")`;
  - a CSV DateTime prints by `SimpleDateTimeFormat`'s rule, with one owner (`EnginePlans.testDataTimestampText`);
  - `%r` has one Java owner;
  - envelope rows are written in result order.
- **S4 also reads the PCT harness's own spelling:** `TestTDS.java:209`, the `.SSS` DateTime cells of the relation suite
  (homework README §7).

### 3.7 The judges

- **The host judge** words every failure through `testexec.HostWords` (`LiteHostWords` or `EdgeHostWords`, chosen by
  the plan's `words` name). It works from kinds and bounds:
  - a size refusal from `SideShape.multiplicity`/`lower`;
  - a planned failure from `Failure.kind` and `operands`.
- **The database judge in the product** keeps lite's words in SQL (`LiteVerdictWords`), per the 2026-11-17 01:30
  ruling.
- **Under the edge:**
  - **C4** (recommended) follows the plan of record's 02:30 DECIDED order. Port the 36 combos into core (the database
    judge's only signal), run PCT HOST-only, and delete the compatibility database machinery: `UpstreamVerdictWords`,
    the edge's `VerdictWords` and its edge-only rules, and the edge judge-plan pins.
  - **C1:** the edge's database judge plans lite's words (unread, ARCH-3), and `UpstreamVerdictWords` alone is deleted.

### 3.8 Hosts

- **Lite's routes (the product).**
  - Under K1: unchanged. They read the plan's `sites[].message`, so there is no generator for sentences.
  - S1 generates `PLAN_VERSION` for TypeScript and Python from `PlanJson.VERSION` (`TypeFacts.java`, with a diff
    test).
- **The edge routes.**
  - A new Java error half, `HostAnswers.errorAnswer(String plan, String statement, String databaseMessage)`, is
    exported as wasm `executeErrorAnswerOrError` and native `lite_execute_error_answer`. It reads the token from the
    database's message and the `SiteWords` from the plan, and answers pure/v1's error JSON in the plan's family.
  - `edge-engine.ts:58-63` and `_pure_v1.py:200-238` call it on a send's failure. This is ARCH-1's second option, and
    `siteWords` is deleted.
  - Under K2 (if ruled), the same error half serves the LITE family too (the review's simpler H8). That replaces the
    draft's generated sentence tables.
- **The readers that change with each bump:**
  - Java: `PlanJson`, `TestPlanJson`;
  - TypeScript: `plan-runner.ts`, `declared-raises.ts`, `raised.ts`, and their consumers in `datacube/src` and
    `query/src/app/run.ts`;
  - Python: `_runs.py`, `_raised.py`, `_planned.py`;
  - the shared fixture `raise-envelopes.json`.

### 3.9 Export (phase 2): a lite plan as legend-engine's plan

- **The entry point:** `edge.PlanExport.singleExecutionPlan(ExecutionPlan plan, Map<String,Object> connection) →
  Json.Obj`, translated per node and refused by name. It generalizes `PureV1Routes.generatePlan`/`executionPlan`
  (`:256-290`, `:363-422`). It plans once (ARCH-11).
- **What runs is lite's SQL.** The exported SQL is the product's own SQL, which legend-engine sends verbatim, not
  legend-engine's text. That is why the 5,541-line plan-text family is not export (§2.1).

| lite | legend-engine | Notes |
|---|---|---|
| `TdsResult` RELATION | `RelationalTdsInstantiationExecutionNode` over `SQLExecutionNode` | as today |
| `TdsResult` VALUE | `RelationalDataTypeInstantiationExecutionNode` over `SQLExecutionNode` | the exact JSON is read from legend-engine's own value plan in S8 |
| `TdsResult` OBJECTS, `Keyed`, `Update`, `Check`, `Effect` | refused by name | legend-engine instantiates classes with generated Java; it has no keyed regions; the rest are a test's or a write program's |
| `Constant` | `ConstantExecutionNode` | exists |
| `Sequence` | `SequenceExecutionNode` | |
| `Load` | `CreateAndPopulateTempTableExecutionNode` for a temp-table IN, else refused | |
| bound parameters | FreeMarker `${…}` with legend-engine's placeholder functions | re-rendered from the plan's `tree` (`PlanJson.java:766`) through a `SqlWriter`, never by editing text (invariant 3). **A statement whose `tree` is null is refused by name** |
| a raise site | refused by name, unless its kind is census class A under `PURE_V1` (already plain) | legend-engine raises nothing in SQL |

**FreeMarker typing.** legend-engine renders values into the SQL text through its literal processors; lite binds typed
parameters (D1). So a Float, a Decimal or a date can differ in typing (the S20 class). Done-when 7 includes
parameterized queries for that reason; the pure/v1 golden set has none.

**Comparing activities.** legend-engine's activity records the SQL *with values substituted*. Done-when 7 compares the
answer's builder and result bytes exactly. The activity's `sql` is compared with the exported template rendered by
legend-engine's own literal processors (the runner renders it), never byte against lite's bound text.

### 3.10 Import (phase 2): legend-engine's plan as a lite plan

- **The entry point:** `edge.PlanImport.fromEngine(Json.Obj) → ExecutionPlan`, with presentation `"legend-engine"`.
  It is planning-time input translation: upstream's template text goes through a `SqlWriter`, as `RawSqlBoundary`
  translates input today.
- **S9 starts with a census** of the node types in legend-engine's plans for the measured queries.

| legend-engine node | lite | Rule |
|---|---|---|
| `SQLExecutionNode` | `Sql` + `TdsResult` | `${var}` and each placeholder function on a closed list read from 4.145.0 become a bound `Slot`; anything else is refused by name. `resultColumns` and the result type give the typed `Column`s |
| `RelationalTdsInstantiationExecutionNode`, `RelationalRelationDataInstantiationExecutionNode` | `TdsResult` RELATION | the TDS columns, or the relation API's columns, → `Column.type` |
| `RelationalDataTypeInstantiationExecutionNode` | `TdsResult` VALUE | |
| `ConstantExecutionNode` | `Constant` | |
| `SequenceExecutionNode`, `MultiResultSequenceExecutionNode`, `RelationalBlockExecutionNode` | `Sequence` | a block's children on one connection, where each maps |
| `AllocationExecutionNode` | a bound value of a later statement, where the allocation is a scalar computed in SQL; else refused | |
| `CreateAndPopulateTempTableExecutionNode` | a `Load` | |
| `FunctionParametersValidationNode` | the plan's `Parameter` declarations (type, multiplicity, enum values), checked as lite checks its own; a validation lite cannot express is refused | |
| `FreeMarkerConditionalExecutionNode` | **refused** | needs a product planned branch, which is not on A3's ratified list. **This departs from the program's §6** ("a conditional node becomes a planned branch") |
| `LimitExecutionNode`, `PlatformUnionExecutionNode`, `PlatformMergeExecutionNode`, `AggregationAwareExecutionNode` | **refused** | they run in Java |
| any node with an `implementation` (`JavaPlatformImplementation`), `PureExpressionPlatformExecutionNode`, the graph-fetch nodes, M2M, external-format, REST and error nodes | **refused** (E1) | G4, G1. **Re-planning from the Pure source, which the program called "the likely answer", is not done** (E2, not ruled) |

### 3.11 What is deleted and what stays

- **Deleted:**
  - the MOVE and RETIRE rows of §2.1 (≈ 920 SQL code lines);
  - `SiteWordsRecorder`, `PureText.recording`, `PlanStyle.siteWords`, `PlanStyle.errorWords`, `HostAnswers.siteWordsOf`;
  - the `RelationTexts`, `PureOnlyTexts` and `ResultEnvelopes` facets and their 3 `@OpenLine` product interfaces;
  - the `ExecutedValues`, `ListTexts` and `Printing` two-form kinds, and `DateTimeTexts` per R1;
  - under C4, the compatibility database judge.
- **Stays:**
  - the KEEP rows;
  - kind C (the plan-text family, the C1 and C2 constants);
  - kind D;
  - `Column.precision`;
  - lite's sites (K1).
- **Then the hook surface collapses (S7, ARCH-4).** `PlanStyle` keeps `dialect`. Each layer gets a typed record of its
  computation forms. `PURE_V1` becomes `UPSTREAM` with one component replaced.

---

## 4. The decisions

### (a) Raise operands: **PENDING THE USER** (re-opened)

*Ruled A1 on 2026-11-18 without the facts below; re-opened at the orchestrator's request.*

**The facts:**
- 0 upstream-facing goldens need a computed operand (D's census, 2026-11-16 21:05; the review's spot check).
- A1 re-opens the 21:20 design's item (5) ("Dropped: the operand carriage, framing, the length prefix, the NULL token,
  per-type read-back"), and with it parsing in `exec.RaisedErrors`.
- A1's transport is **unmeasured on every database**. The product probe measures value carriers in query results, not
  text inside a raise. The transport differs per database: the envelopes, SQLite's `QUOTE_MARK`, H2 echoing the
  statement, DuckDB-wasm against JDBC.
- A1 does nothing for LEGEND_ENGINE, where the database raises (class A).

| Option | What | Cost | Fidelity |
|---|---|---|---|
| A1 | The edge's raise returns computed operands as carriers; exec decodes them | 2 days plus a lane round; re-opens 21:20 (5) | exact, if the transport holds on every database |
| **A3 (recommended)** | A computed-operand site at the edge is worded with lite's sentence and no value (a register row). `PureMessages.sentence` is deleted. A1 is built when a first real case appears; a guard counts computed-operand edge sites | 0.5 day | not exact, on 0 measured cases |
| A4 | Keep the capped in-SQL sentence (today's TRANSITIONAL path, 21:30's other choice) | 0 | exact, but SQL builds upstream text: a ledgered exception to the plan of record's §4 item 1 |

### (b) The kind-B rows: **RULED**, except R1 and H6, which are **PENDING THE USER**

*RULED (the user, 2026-11-18, relayed by the orchestrator: "go with your recommendations on all seven decisions"):
rule (b0) and the classification below. Corrected after the review: the rule is restated by answer (H7), with the
sub-rule H5, and B6, B10, B11 and R3 are corrected per database.*

| Row | What (where) | Upstream, read | Verdict |
|---|---|---|---|
| B1 (§6 r8) | Float `toString` in a query → `cast(x as varchar)` | H2 `:266`, DuckDB `:284` | **KEEP** (answers differ: lite's six decimals); only in a query, since relation cells become presentation |
| B2 (r21) | Float literal kind DECIMAL | ruled R2 | **KEEP** |
| B3 (r22) | SQLite Decimal arithmetic as REAL | ruled (c); SQLite has no extension (H5) | **KEEP** |
| B4 (r12) | DuckDB engine scan order | the corpus's replay | **KEEP**, as a style fact instead of a JVM property (ARCH-9) |
| B5 (J22) | H2 `MODE=LEGACY`, `DEFAULT_NULL_ORDERING=HIGH` | ruled J22 | **KEEP** |
| B6 (F31) | a parse as a plain cast | H2 `cast(%s as integer)`/`as float` (`:248-249`); **DuckDB `parseFloat` `cast(%s as float)` (`:257`), which is a 32-bit REAL in DuckDB**, and `parseInteger` `cast(%s as BIGINT)` (`:258`) | **KEEP** as plain casts, **per database**: F38 (H2 INTEGER), and DuckDB's REAL compared by answer in S5a. Lite's edge casts to DOUBLE today |
| B7 (B7) | pure/v1's unchecked raises plain | census class A | **KEEP** |
| B8 (J20) | `isNumeric` as `lower(s)=upper(s)` | H2 `:232`, DuckDB `:238` (the same) | **KEEP** |
| B9 (J19, S367) | EXISTS as joins | legend-engine's second exists emission | **KEEP** (row multiplicity) |
| B10 (J1) | `parseDecimal` as `decimal(5,2)` | H2 `transformParseDecimalH2` (`:247`); DuckDB `transformParseDecimal` (`:256`), whose `cast(… as decimal)` is DECIMAL(18,3) in DuckDB (its manifest's `testParseDecimal`, line 398: `Could not convert string "3.14159d" to DECIMAL(18,3)`) | **RE-RULE in S5a:** per database by answer; today the H2 form is written on every database |
| B11 (J2) | `parseJSON`'s `}{` tolerance | `parseJson` is pushed (kind 1): DuckDB a strict `cast(%s as JSON)`, H2 `legend_h2_extension_json_parse(%s)` (`:250`) | **RE-RULE in S5a:** the tolerance is not legend-engine's SQL; by answer, it is the product's form or legend-engine's strict cast |
| B12 (L10) | WHERE conjunct zones | legend-engine's order | **RE-RULE:** kept in executed SQL only if an answer changes; else plan text only |
| B13 | `RawSqlBoundary.translated` | lite's adaptation | **KEEP** as an INPUT row |
| B14 (§10 i2, F5) | a pruned unused derived column that can raise | not yet read | **RE-RULE** once the engine runner answers |
| T1-T3 | `formatDate`, `joinStrings`, `toJson`/`parseJson` in a query | kind 1 (§3.2) | **compared by answer in S5a;** a row only where an answer differs |
| **R1** | a DateTime's (and, by the same cast, a Boolean's) `toString` in a query: today legend-pure's print composed in SQL (`PureDateTimePrint`) | `toString_Any_1` → `cast(%s as varchar)` for every type. **legend-engine's own manifests:** on DuckDB `testDateTimeToString`, `testDateTimeWithTimezoneToString`, `testSimpleDateToString` fail ("expected: '2014-01-01T00:00:00.000+0000' / actual: '2014-01-01 00:00:00'"); on H2 these plus `testBooleanToString` ("actual: 'TRUE'") | **PENDING THE USER** (below) |
| R2 | a list's `[a, b]` | no pair | **MOVE** at a root or side; refused where it feeds SQL |
| R3 | `TDSNull` in a join | `joinStrings` → `group_concat`/`string_agg`, which **skip NULLs: the product's form** | **MOVE** at a root or side; **in a query, a DELETION** (the product's form), not a row |
| R4 | `format`'s `%r`/`%0Nd`, `toRepresentation`, `@_` | no pair | **MOVE**; refused where it feeds SQL |
| M1-M5 | relation text, envelopes, seed JSON, the trace cell | after the SQL | **MOVE** |
| C1 | `objectReferenceConnection` | legend-engine's constant | **KEEP**, kind C |
| C2 | graph-key texts | legend-engine's key print | **KEEP**, kind C |
| C3 | pure/v1 activity SQL is lite's | legend-engine reports its own | a **new register row** (S310's principle: report what ran) |
| **H6** | kind-2 roots legend-engine refuses at planning (12 `format` tests in its DuckDB essential manifest) | legend-engine: "No SQL translation exists…"; legend-pure: the tests' expected values | **PENDING THE USER** (below) |

**R1, PENDING THE USER.** The measured cost is from legend-engine's own manifests and lite's net at `68f20eb4d`:
- DuckDB: 3 tests (13 cases) that pass in lite today, in channel A and channel B, go to fail.
- H2: those, plus `testBooleanToString`.
- These are exactly the failures legend-engine's manifests expect.

| Option | What |
|---|---|
| R1-a | Keep legend-pure's print in SQL: a ledgered exception to the plan of record's §4 item 1 |
| **R1-b (recommended)** | legend-engine's cast. It follows the user's own rule of 2026-11-16 for Floats ("for toString inside an expression use engine cast( as varchar)"). Built only after S5a counts the channel-B DateTime `toString`s written outside an eval: if any, the boundary marker comes to the user first |
| R1-c | Present a DateTime `toString` at a root or side in Java (no move), and the cast only where it feeds SQL: a better-than-upstream divergence, like H6 |

**H6, PENDING THE USER.** A kind-2 root that legend-engine refuses, such as `format` at an eval root.

| Option | What |
|---|---|
| **H6-a (recommended)** | PCT and the corpus present legend-pure's answer (no PCT move; a register row). pure/v1 refuses in legend-engine's words (the 2026-11-17 ruling) |
| H6-b | Refuse everywhere at the edge. The 12 DuckDB `format` tests, and their siblings per database, go to fail, matching legend-engine |

Where legend-engine refuses, legend-pure's answer is the only upstream answer there is. This is consistent with R1-b,
which follows legend-engine where legend-engine *answers*.

### (c) The test plan's words and the database judge: **PENDING THE USER** (re-opened)

*Ruled C1 on 2026-11-18. The review found the plan of record's DECIDED entry of 2026-11-17 02:30, which the draft did
not cite. Re-opened at the orchestrator's request.*

| Option | What | Cost |
|---|---|---|
| C1 | The test plan carries kinds and bounds, and the hearing words them. The edge's database judge plans lite's words (unread). `UpstreamVerdictWords` is deleted | S3: 1 day. Keeps a judge the plan of record scheduled for deletion, and makes it decline every presented side |
| **C4 (recommended)** | The 02:30 order: port the 36 assert-and-shape combos into core; PCT runs HOST-only; delete the compatibility database machinery (`UpstreamVerdictWords`, the edge `VerdictWords` and its edge-only rules, the edge judge-plan pins). Then S3 carries kinds and bounds for the host judge alone | S2b: 3 days, then S3: 0.5 day. The 02:30 measurement found 100% verdict agreement on PCT, so the database judge brought 0 signal |
| C2 | C1, plus lite's words leave the product database judge's SQL | re-rules 01:30 ("lite's own wording built in SQL, zero Java"); not proposed |

### (d) The safety net: **RULED** (D1)

*RULED (as (b)): D1, as slice S0, first.*

- **The premise, corrected (§2.4).** Messages are already pinned in channel B (DuckDB), its ledger and channel A. Add:
  - corpus failure messages (DuckDB 62, H2 78 FAIL rows);
  - channel B on H2 and SQLite.
- **Activities.** PCT's go to an informational file no diff test reads. pure/v1's go to their own diff file, re-recorded
  only by a slice that names the move.
- **Masking by field.** Mask the `executionTraceID` comment's id and UUIDs, never a value.
- **Order.** The recorder writes a `sorted` flag (from the plan's root ORDER BY), because `EdgeNormalize` sees only TSV.
  Rows keep their order only across distinct sort keys; within ties, and everywhere else, they are sorted.
- **Missing cases.** Add EDGE-1's five values (DuckDB, H2), EDGE-2's DateTime CSV, EDGE-3's `%r` newline, and a checked
  pure/v1 raise.
- **Host parity** (the pure/v1 set through the wasm and native answer halves) runs in a heavy lane. It needs TeaVM and
  native-image builds, so it stays out of the per-push lane.
- **The per-push `edge_net` lane** runs in-process on DuckDB, H2 and SQLite. Recording PCT on SQLite in CI is marked as
  the user's own call in `gates/BUILD.bazel`, so S0 brings it back to the user with the lane's measured cost.
- **The proof.** Planted changes: an SQL-only alias rename passes; a 17th-digit value change, a row-order change across
  distinct keys, and a message change each fail.

### (e) Import: **RULED** (E1)

*RULED (as (b)): E1.*

- E2 (re-plan from the Pure source) is not ruled.
- The conditional node stays refused until the user rules on a product planned branch.
- Both depart from the program's §6, and §3.10 says so.
- The node table is completed (§3.10).

### (f) `EngineResultEnvelopes`: **RULED** (retire)

*RULED (as (b)).*

- The string value of `executeLegendQuery` becomes a presented side (`ENGINE_TDS_JSON`, `ENGINE_CLASS_JSON`,
  `ENGINE_VALUES_JSON`), written by `PureV1Writer` over the typed rows in result order.
- It is written so in the LEGEND_PURE family too (§3.4).
- The check is the corpus rosters.

### (g) The plan format: **RULED**, with one fact corrected

*RULED (as (b)): generated host constants, exact-version reading kept.*

- **Corrected:** "two bumps" contradicted the slices, because S4 and S6 add `Presented` uses after S1. S1 generates
  `PLAN_VERSION` for TypeScript and Python from Java (`TypeFacts.java`, with a diff test). From then on **every slice
  that changes either format bumps it**, and a version's meaning is never amended.
- **The readers** are listed in §3.8.

### (h) Lite's own sentence in the product plan: **PENDING THE USER** (new)

**The 2026-11-16 21:20 error design, adopted with the user**, item (2): "the plan carries a site table: kind, where,
and both messages RENDERED AT PLANNING … At run time exec only looks it up … the hosts read the same table". K10
(2026-11-17, the orchestrator's ruling (A)) later moved upstream's words out of the product's site, into edge records.

| Option | What | Cost |
|---|---|---|
| **K1 (recommended)** | Keep as built: lite's `message` and `raises` stay. Upstream words move from the stateful recorder into the plan's edge extension, rendered at planning. The hosts read the site table as today | S1: 2 days. Product plan bytes unchanged. Done-when 2 reads "no *upstream* word in a product record" |
| K2 | Remove lite's sentence and `raises` from every product plan (the draft). Every presentation words a kind at its writer. The hosts use the Java error half for LITE too | S1: 3 days. Product plan pins move (dated). Re-rules 21:20 (2). Rewording an `ErrorKind` sentence stops being a format change (PRODUCT-4's point) |

The program needs neither: export refuses plans with sites, and import carries none of lite's sites. K1 keeps the
adopted design and still deletes every upstream word and the recorder's state.

---

## 5. The phased plan

**The order:**
1. The net, plus the guards in report-only mode.
2. Raise words as data.
3. PCT HOST-only (if C4).
4. The SQL builders become Java writers.
5. The hook surface.
6. Phase 2: runner tooling, then export, then import.

**Every slice** sends a shape note to the orchestrator before building (handoff §4.5).

**The local loop:** the touched packages' tests, `//core:guardrails`, `//core:census`, the net (`edge_net`, from S0)
and the report-only G1/G3 counts (from S0), so that each slice shows its own shrink.

**Product pins.** Under K1 and A3, the product's SQL and plan-JSON pins (`lean_sql_baseline`, `query_capture`,
`lean_sql_corpus` on DuckDB and H2) and the product probe stay identical in every slice. Under K2 or A1 they move in
S1 or S2, with a dated note.

### Phase 1: presentation

| Slice | Shape | The check that proves it | Lanes identical | Size |
|---|---|---|---|---|
| **S0 The net** (d) | `PctCaseRecorder`, `EdgeNormalize`, `PureV1EdgeGoldenTest`; corpus messages; channel B on H2 and SQLite; the `edge_net` lane; G1 and G3 report-only; regeneration at the base | the planted changes; green at the base | everything (no product or edge code) | 2.5 days |
| **S1 Raise words as edge data** (K1; v59) | `Extension` moved to `executionplan`; `presentation`/`extensions` (omitted at product values); `siteExtensions` as a pure function; `SiteWords` in the plan; the stateless `RaisedWords`; the Java error half (wasm, native); the recorder and `siteWords` deleted; `PLAN_VERSION` generated | the net's messages unchanged; a literal-operand raise on the tab and Python (ARCH-1's input); the product plan bytes identical but the version | PCT A and B, corpora, pure/v1, hosts | 2 days (K2: 3) |
| **S2 Computed operands** (a) | A3: lite's sentence for computed-operand edge sites (a register row); `PureMessages.sentence` deleted; a counting guard | the net unchanged (0 cases); the guard reads 0 | as S1 | 0.5 day (A1: 2 + a lane round) |
| **S2b PCT HOST-only** (c, C4) | port the 36 combos into core; PCT HOST-only; the compatibility database machinery deleted | core under isolated BOTH green on the ported shapes; channel B under HOST unchanged verdict for verdict; edge judge-plan pins gone | channel B (all lanes by round), core | 3 days (C1: none) |
| **S3 Test plan words** (v33) | kinds and bounds; `answerSizeWords`, `sizeWords`, `errorWords` gone | channel B messages unchanged in the net and the ledger | channel B, corpora | 0.5 day (C1: 1) |
| **S4 Relation text in Java** (M1) | `Presented`, `edge.RelationText`, presented roots and sides, `RelationTextLowering` deleted; S131/S132 re-measured; `TestTDS.java:209` read | V-EDGE's EDGE-1/2/3 tests; the net unchanged except listed moves, each with upstream's source | channel B (all lanes), corpora, pure/v1 | 3.5 days |
| **S5a Measure, then the user rules** | per row, read from upstream's 77 relational manifests (the oracle for which moves legend-engine shares) and measured on PCT A and B: R1, the outside-eval count, R2-R4, the kind-1 answers (formatDate, joinStrings, toJson/parseJson), B6, B10, B11 | one table to the user: row, upstream evidence, moves | none (measurement) | 1 day + the ruling |
| **S5b The other prints** | the presenters for R2-R4 and M2-M5; the ruled in-query forms | the net's moves equal the ruled list exactly | PCT A and B (lane round), corpora | 2 days |
| **S6 Envelopes** (f) | `EngineResultEnvelopes`, `EngineExecutedValues`, `oneBare` → `PureV1Writer` | corpus rosters unchanged; EDGE-8's ordered 200,000-row probe on DuckDB | corpora | 1 day |
| **S7 The hook surface** (ARCH-4) | typed per-layer records over the 43 product files that name `PlanStyle`; `KINDS`/`TWO_FORMS` shrink; §6 at its end state, blocking | javac exhaustiveness; G1-G5 green and blocking | everything byte-identical | 2.5 days |

**Phase 1:** about **18.5 working days** with K1, A3 and C4. With K2, A1 and C1 it is also about 18.5: S1 +1, S2 +1.5,
S2b −3, S3 +0.5. Rulings and lane latency come on top.

### Phase 2: portability

| Slice | Shape | The check that proves it | Size |
|---|---|---|---|
| **R Runner tooling** | `tools/engine-runner` gains two modes: execute a `SingleExecutionPlan` JSON (`PlanExecutor`), and run `PlanGenerator` for a lambda and a model. Its `TestRuntimeBuilder` swap to a local H2 is replaced by a shared seeded database. It needs a Bazel session (§8) | the pure/v1 golden set planned and executed by legend-engine, with bytes recorded | 3 days |
| **S8 Export** | `edge.PlanExport`; FreeMarker from the `SqlWriter`; refusals; `generatePlan` on it; the activities rule | done-when 7 | 2.5 days |
| **S9 Import** | the node-type census first; `edge.PlanImport`; FreeMarker resolution; refusals | done-when 8 | 4 days |

**Phase 2:** about **9.5 working days**.

**In all:** about **28 working days** (the review measured about 27). On a three-week budget, phase 1 fits and phase 2
does not.

**Lane rounds** for the databases CI does not run follow S2b, S4, S5b and S6.

---

## 6. The guard for the end state (re-specified after the review)

**What it is:** `core/src/test/java/com/legend/OnePlanGuardTest.java` (G1, G2, G5) and `//core:one_plan_measure` (G3,
heavy lane). Both run report-only from S0 and block from S7.

**Rules common to every guard:**
- Every rule has a **planted violator**, a test-only class or fixture the rule must fail on.
- **No rule depends on a clock** (G10).

| Rule | What it checks | How its vocabulary is derived | Both ways |
|---|---|---|---|
| **G1 No upstream text built as SQL** | Each method in `com.legend.edge..`, **and each product method it reaches transitively** (EDGE-1's own path: `RelationTextLowering` → `LiteralSpelling.floatPrint` → `FLOAT_TO_TEXT`), that constructs a text-producing MIR node is a row of exactly one ledger: `KIND_B`, `KIND_C` (e.g. `PlanEnumForm.apply`, `InProtocol.apply`, `EngineStyleH2.planned`, the C1/C2 constants, `TestDataFetches`), `KIND_D` (`CompiledPureVerdictNumbers`) or `INPUT` (`RawSqlBoundary.translated`, `PlanImport`). Each row is `Class#method → register id or ruling, date`. Measured by a call-graph scan (ArchUnit's classes, as `ArchitectureTest` already imports) | text-producing MIR from `sql.SqlTyping.callType` (`:333`) over each `SqlFn`, plus `StringLit`, the JSON constructors and `Cast` to VARCHAR or JSON | every site in exactly one row; a row that matches no site fails |
| **G2 No upstream word in a product record** | Every `String`-typed component of every record in `executionplan..` and `testexecutionplan..`, found by reflection (a new component is scanned automatically), in every plan the suites make, both styles. `extensions` are excluded (edge data by construction). **A value that appears verbatim as a literal of the body's source is exempt**: `assertError`'s expected messages, the SQL's literals | the edge families' sentences as **whole templates** (literal runs of two words or more, with operand slots as wildcards), produced by calling the families with sentinel operands; never fragments like " by zero" | every scanned component must be populated by at least one plan in the suites (a component never reached is reported). This replaces the draft's tautology |
| **G3 One plan** | Each case is planned under `PRODUCT` and under "edge minus kind B"; every statement must be byte-equal. The real edge plan's differences are then attributed by enabling each `KIND_B` row alone | the suites' own cases | an unattributed difference fails; **a `KIND_B` row that attributes no difference over the measured suites fails** (deterministic; this replaces the draft's 30-day rule). About 2 + N_B plannings per case, so a heavy lane |
| **G4 Hosts present nothing upstream** | `engine-client/src`, `python/legend_lite`, `datacube/src` and `query/src` contain no G2 template outside generated files. A **behavioural** check, not a lexical one: each host's plan readers run over a planted plan whose site and extensions carry an upstream sentence, and the host's output must not contain it except through the Java error half | G2's templates, exported by `TypeFacts.java` as a fixture | the fixture's entries must be produced by a Java family (diff test) |
| **G5 Writers evaluate nothing** | `ArchitectureTest.presentersReachNoPlanning`, **transitive**, mirroring `execReachesNoPlanning`: the presenter classes (`result..`, `edge.Families`, `edge.RelationText`, `edge.Presenter`, `edge.PureV1Writer`, `edge.EdgeHostWords`) may reach base, json, the records (model, execution_plan, sql, values, result) and the switchboards, and no other `com.legend` package | ArchUnit packages | the planted violator |
| **`CompatSpellingGuardTest` (kept)** | product Java holds no upstream text | **derived** from the edge's constant pools minus a short generic stoplist (GUARDS-3's fix), replacing its 54 hand-listed literals | an entry no edge class holds fails |

**Shrink-only.** Each ledger loses rows freely, and gains one only with a dated line naming the user's ruling
(`tools/guards/shrink_only.py`).

---

## 7. Risks and how each is caught

| Risk | Caught by |
|---|---|
| A Java print differs from the SQL print where S132's leniency hid it, so a verdict flips | the net's messages and outcomes (S0); each flip is listed with upstream's source before landing |
| A second evaluator grows in the writers | G5 (transitive); the closed list of presentable roots (§3.2); nesting refused |
| **A misclassified print** | **channel A is the referee:** a channel-A move that is not on S5a's ruled list is a misclassification and is fixed before landing. R1-b's moves, if ruled, are on the list. *(This corrects the draft's "any channel-A move".)* |
| The hosts drift | G4 (behavioural); generated `PLAN_VERSION`; the Java error and answer halves |
| ServiceLoader in TeaVM or native-image | not used for product plans: the families and the plan's extension codec are passed in explicitly (§3.3, §3.4) |
| Format churn across readers | a bump per format-changing slice; the generated constant; one commit per bump |
| A1's raise transport fails on a database (if A1 is ruled) | a computed-operand raise test per database, lane-round measured; that is why A3 is recommended |
| C4 loses a real-bug signal | the 02:30 measurement found 100% agreement on PCT (0 signal); core keeps isolated BOTH, with the 36 combos ported |
| Databases CI does not run | lane rounds after S2b, S4, S5b and S6 |
| Upstream's answer guessed | §8; no ruling on a recalled answer |
| Export and import scope | closed node tables; S9's census first; refusals counted |
| Performance | low: Java presentation is linear over streamed rows. G3's cost goes to a heavy lane; host parity to a heavy lane |
| The net's blind spots | S0's planted changes, rerun whenever the net changes |
| ARCH-4's refactor touches 43 files | last in phase 1 (S7), byte-identical by construction |

---

## 8. How fidelity is verified: reading upstream, never recalling it

**This container (checked 2026-11-18):**
- There is no legend-engine or legend-pure checkout and no Bazel binary, so neither `@legend_engine_src` nor the engine
  runner is available.
- `git ls-remote` reaches the pinned tags:
  - legend-engine-4.145.0 = `230c159196d6512486fd382556c5e9e4fb128ebb`;
  - legend-pure-5.99.0 = `7fbc7d6e8d52e2488bdae67280e5f5dfed448d68`.
- raw.githubusercontent.com answers.
- Blob-less clones at the tags work. This revision read `pureToSQLQuery.pure`, `h2Extension2_1_214.pure`,
  `duckdbExtension.pure`, the protocol's node classes, and the DuckDB and H2 essential PCT manifests that way. The
  review cloned both tags.

| Slice | Upstream's answer | Read how |
|---|---|---|
| S0 | none | n/a |
| S1, S2 | legend-pure's sentences (`CompiledSupport.java:1483-1485, 1588`, `CoreHelper.java:520, 544`, `Rem.java:64`, the census's `*.pure` tests); pure/v1's for classes C, D, E (F43/F44) | pinned sources from a blob-less clone; pure/v1 by **the engine runner** in a Bazel session |
| S2b | none new (lite's own judges) | n/a |
| S3 | legend-pure's assert words (`asserts/*.pure`); JUDGES-5's relabelled sentences checked against them | pinned sources |
| S4 | `relation/functions/toString.pure`, `s.pure`, `helperFunctions.pure:203-243`, `dateExtension.pure:384-387`, `CompiledSupport.java:132-137, 1271-1273`, **and the harness `TestTDS.java:209`** | pinned sources; channel A as referee |
| S5a | **the 77 relational PCT manifests** (which moves legend-engine shares); each function's pair and its `dynaFnToSql` **per database** (B1, B6, B8, B10 read for H2 *and* DuckDB, the others per lane); no source exists for SQLite, MariaDB, MySQL or MongoDB (H5) | pinned sources, before any build |
| S6 | `RelationalResultToJsonDefaultSerializer`, the JsonBuilder rule | pinned sources, then the runner |
| R, S8, S9 | legend-engine's executor and serializer | **the engine runner**, after slice R, in a Bazel session (a lane session, or a fresh one with `tools/cloud-distdir.sh` and `tools/cloud-maven-distdir.sh`, `docs/CLOUD_SESSIONS.md`) |

**Recorded answers.** Every answer the runner gives is recorded under `core/src/test/resources/upstream-api/` with its
model and provenance. Every divergence kept is posted to the user and becomes a register row.

---

## 9. Measurement commands (at `68f20eb4d`)

- **SQL code per file:** a brace-matching method counter over comment- and string-stripped Java, in the designer's
  scratchpad (`methods.py`). The review's counter is `scratchpad/design-review/tools/methods.py`.
- **The net:**
  - `grep -c 'activities=' pct/src/test/resources/edge/*_answers.tsv`;
  - `grep $'\tVALUE\t' <file> | grep -o '<id>' | wc -l`;
  - `awk -F'\t' '$2!="PASS"' pct/src/test/resources/edge/channel_b_duckdb_*_outcomes.tsv`;
  - `grep -c $'\tERROR\t'` per answers file;
  - `grep -c $'\tFAIL' spec/src/test/resources/edge/*judge-host.tsv`.
- **Upstream:**
  - `git clone --filter=blob:none --no-checkout --depth 1 --branch legend-engine-4.145.0 https://github.com/finos/legend-engine`;
  - `git show HEAD:<path>/h2Extension2_1_214.pure | grep dynaFnToSql`, and the same for `duckdbExtension.pure`;
  - `git show HEAD:<path>/pureToSQLQuery.pure | grep -c 'PureFunctionToRelationalFunctionPair(first'` (451);
  - `git show HEAD:<…>/relational-duckdb/EssentialFunctions_manifest.json | grep -n 'ToString\|format_String'`, and
    the same for H2.
- **The plan-text family:** `wc -l core/src/main/java/com/legend/edge/{EnginePlans,PlanText,EngineStyleH2,EngineStyleDB2,EngineStyleComposite,PlanEnumForm,InProtocol,PlanVariables}.java`
  (5,541).

---

## 10. Review responses

The review is `docs/records/presentation-design-review-2026-11-18.md`; its corrections are in priority order. All 16
are accepted. Two are accepted in a modified form, and the table says how.

| # | The review's correction | Response | Where |
|---|---|---|---|
| 1 | Surface the 21:20 re-rulings (H1: lite's sentence out of the product plan; H2: operand carriage), each with a "keep as built" option | **Accepted.** (h) is new, with K1 keep-as-built recommended. (a) is re-opened with A3 recommended (and A4 as keep-as-built). Both are PENDING THE USER | §0, §4 (a), (h) |
| 2 | Align (c) with the 02:30 decision; add C4 before S3 | **Accepted.** C4 is recommended and becomes slice S2b | §4 (c), §5 |
| 3 | Fix the net's premise | **Accepted.** I was wrong: channel B on DuckDB, its ledger and channel A pin messages. S0's item is re-scoped to the corpus and to channel B on H2 and SQLite | §2.4, §2.6.6, §4 (d) |
| 4 | Quantify R1 from the manifests; state its interaction with (b0); fix §7 | **Accepted, verified independently:** DuckDB's manifest lists the three tests with legend-engine's expected failures, and lite's net has them SUCCESSFUL and PASS (13 answer lines). H2 also has `testBooleanToString`. R1 is PENDING THE USER, with S5a's outside-eval count first. §7 is corrected | §4 (b), §7 |
| 5 | Rule 1 by answer; a sub-rule for databases without an extension; list formatDate and joinStrings | **Accepted** (H7, H5, rows T1-T3) | §3.2, §4 (b) |
| 6 | B6 per database; record B11's evidence; R3's in-query half as a deletion | **Accepted, verified:** DuckDB `parseFloat` is `cast(%s as float)` (`duckdbExtension.pure:257`), `parseInteger` `as BIGINT` (`:258`), `parseJson` `cast(%s as JSON)` (`:259`) | §4 (b) |
| 7 | Decide the product records' upstream names | **Accepted in a modified form:** `presentation` is a name (the precedent of `TestPlan.words`, J29), and edge facts ride `Extension`, moved down to `executionplan` so both plan types share the seam. No product enum names upstream | §3.3 |
| 8 | The product plan changes in S1; its pins move | **Accepted in a modified form:** under K1 and A3, both new components are omitted at their product values, so a product plan's JSON is byte-identical except its version and the pins do not move. Under K2 or A1 they move, with a dated note. Done-when 5 and §5 say so | §1, §3.3, §5 |
| 9 | Drop "two bumps"; bump per slice; add the fixture and the `datacube`/`query` consumers | **Accepted** | §4 (g), §3.8 |
| 10 | Rework the guard | **Accepted, all seven points:** G1 transitive with four ledgers; G2 on planner metadata with source-literal exemption and whole templates; G3 deterministic; G4 behavioural, covering `datacube` and `query`; G5 transitive like THE WALL; planted violators; `CompatSpellingGuardTest` kept with a derived vocabulary | §6 |
| 11 | Bound the writers' evaluation; register channel-A kind-2 roots | **Accepted:** the closed list and the nesting rule; H6 is PENDING THE USER | §3.2, §4 (b) |
| 12 | Prove ServiceLoader in TeaVM and native-image, or pass families in explicitly | **Accepted:** the families and the product plan's extension codec are passed in explicitly, so there is nothing to prove | §3.3, §3.4 |
| 13 | Correct the numbers | **Accepted:** 5,541 measured, and not export's base (a stated departure from the program's §5); `objectReferenceConnection` is kind C throughout; ≈ 920 deleted | §2.1, §2.2, §3.9 |
| 14 | Re-size; split S5; add runner tooling; two phases; G1 and G3 report-only in S0 | **Accepted:** about 28 days, in phase 1 (18.5) and phase 2 (9.5) | §1, §5 |
| 15 | Import and export scope | **Accepted:** the nodes added (mapped or refused); S9's census first; done-when widened; the activities rule | §1, §3.9, §3.10 |
| 16 | For (a): unmeasured transport, nothing for LEGEND_ENGINE, A3 the promised end choice | **Accepted.** My recommendation changes from A1 to A3 | §4 (a) |

**Also taken from the review's body:**
- the database-judge decline behaviour under C1 (§3.6);
- the FreeMarker typing difference (§3.9);
- a statement whose `tree` is null is refused (§3.9);
- G3's cost goes to a heavy lane (§6);
- S4 reads the PCT harness (§3.6, §8).
