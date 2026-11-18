# One plan, two presentations: the design (2026-11-18)

**Status: RULED. The user ruled all seven decisions of §4 on 2026-11-18 (§0); an independent review is running, and
its corrections will be recorded here. Nobody builds from it before PR #118 merges.** It designs the approved
program `docs/records/edge-portability-program-2026-11-18.md` (on `claude/total-wall-orch`; "the program" below).
That program starts once PR #118 (`claude/total-wall-d-stage` → `main`, head `68f20eb4d`) has merged and its fix round
(F1-F8, `claude/pr118-fixes`) has landed.

**Inputs read for this design:**
- AGENTS.md at the PR head.
- The program (all of it) and the plan of record `docs/records/total-wall-edge-program-2026-11-16.md` (§1-§11, and the
  §12 log from 2026-11-16 15:00 to 2026-11-18 14:47).
- The PR #118 audit (`claude/refactor-code-quality-review-9fqhug`, `docs/records/pr118-audit-2026-11-18/`): README,
  every finding the brief named, and V-EDGE and V-HOSTS.
- The code at `68f20eb4d`: the edge package, the plan style and its hooks, the raise path, the writers, the plan
  formats, and the TypeScript and Python hosts' raise and plan readers.
- Upstream, where §2 and §4 rest on it: legend-engine 4.145.0 (`230c1591`) through a blob-less clone (§8).

**Measurements:** every number in §2 was measured by this design at `68f20eb4d`. The commands are in §9. Where my
numbers differ from the audit's, §2.6 says so.

---

## 0. The decisions on one page

**RULED by the user, 2026-11-18 (about 16:30 UTC), relayed by the orchestrator (`session_0195rQEoRb1AGCHxtZChjWsg`),
verbatim:** "go with your recommendations on all seven decisions".

So: (a) A1; (b) the rule of §3.2 and §4.b's 26-item classification; (c) C1; (d) D1, with S0 first; (e) E1; (f) retire
`EngineResultEnvelopes` into a presented side written by `PureV1Writer`; (g) the two format bumps, with generated host
constants and exact-version reading kept. The rulings within (b) that §4.b leaves to evidence (RE-RULE) and to listed
PCT moves (CHANGE R1; S5's moves) still come back to the user with that evidence before they are built.

The design moves every piece of presentation out of SQL and out of the plan's words. The plan says *whose* text a
reader gets (one field) and carries typed facts. Java writers present at the boundary, through one presenter family.
For most queries the edge's statements become the product's statements. The rest are a short list of ruled
in-query computations (kind B).

| # | Decision | Recommendation (RULED 2026-11-18) |
|---|---|---|
| (a) | Raise operands. No raise returns its operand values today, so legend-pure's "Cannot divide 5 by zero" can be worded after the run only for literal operands. | **A1:** the edge's raise returns its computed operands as typed carriers (one listed kind-B row). The product's SQL stays token-only (S308 as built). |
| (b) | The kind-B list, re-ruled under "one plan". | Adopt the rule in §3.2: compute in SQL only where legend-engine pushes the function to SQL, using legend-engine's own SQL; present in Java where it does not. Of the 26 items in §4.b: **KEEP 10, CHANGE 1** (a DateTime's in-query `toString` becomes legend-engine's cast), **RE-RULE 4** with evidence, **MOVE 8** to writers, **2** kind-C constants, **1** new register row. |
| (c) | The test plan's words, and the database judge's words built in SQL. | **C1:** the test plan carries kinds and bounds, and the hearing words them. `UpstreamVerdictWords` is retired. Under the edge, the database judge plans lite's words, which nobody reads (ARCH-3). This is consistent with the 2026-11-17 01:30 judge-model ruling. |
| (d) | The safety net. | **D1:** compare outcomes, answers and messages. Drop PCT `activities`, mask by field, keep order where the query sorts, pin failure messages, and add a per-push CI lane. Slice S0, before anything moves. |
| (e) | Import of upstream plan nodes that run Java or Pure. | **E1:** refuse by name in the first import. Re-planning from the Pure source (E2) is a separate route, only if the user asks for it. FreeMarker is resolved at import. A conditional node is refused until the user rules on a product planned branch. |
| (f) | `EngineResultEnvelopes` (SQL) beside `PureV1Writer` (Java). | **Retire the SQL one.** Its only caller is the corpus's `executeLegendQuery` string entry (`FramePlanner` → `EngineExecutedValues`). It becomes a presented side written by `PureV1Writer`. |
| (g) | Plan format and the three hosts' readers. | Two bumps in total: **v59** (presentation, sites without words, operand slots) and **test plan v33**. All readers change in the same commit. TypeScript and Python generate their version constant and lite's sentence table from Java, with diff tests. Exact-version reading stays. |

---

## 1. The goal and "done when"

**The goal, in the user's words** (2026-11-18 05:00 and 05:15, verbatim in the program's §1): "mostly everything that
could be would really be at the edge edge mostly flexing the product and then trying to fix up later in java instead
of parallel tracks for product and compatibility". And: "our edge should really be plans that don't do a lot of the
stuff in SQL", so that lite can "import upstream plans to run on our platform and export plans from us to to be able
to run in upstream".

**Done when.** Each item is a measured check; §6 and §5 name the target that measures it.

1. **One plan.** For every case of PCT channel A (DuckDB, H2, SQLite, Postgres, and each lane database by a lane
   round), PCT channel B, both relational corpora and the pure/v1 golden requests, every statement the edge sends is
   the statement the product plans for the same expression. The only exceptions are statements a listed kind-B row
   changes, and the measure attributes each one to its row. `//core:one_plan_measure` reads 0 unattributed
   differences. Each kind-B row has a dated ruling from the user.
2. **No presentation in any plan.** No product plan, test plan or host plan half carries a sentence, lite's or
   upstream's. The plan carries one `presentation` value and typed facts. This is checked over every plan the suites
   above make, both styles, by the vocabulary scan of §6 (rule G2).
3. **Presentation is Java at the boundary.** No edge class builds upstream text as SQL, except the listed kind-B rows
   (§6 rule G1, reading only ruled rows). The following are deleted:
   - `RelationTextLowering`, `PurePrintForms`' print passes (`TdsNullCells` and `joined`), `EdgePureOnlyTexts`,
     `PureListTexts`, `PureDateTimePrint` and `EdgeDateTimeTexts`;
   - `PureMessages.sentence`, `UpstreamVerdictWords`, `EngineResultEnvelopes`, `EngineExecutedValues` and
     `EdgeLoweringForms.oneBare`;
   - `SiteWordsRecorder`, `SiteWords`, `PlanStyle.siteWords` and the hosts' `siteWords`.
4. **Export.** A lite plan exported as legend-engine's `SingleExecutionPlan` and executed by legend-engine 4.145.0's
   `PlanExecutor` (the engine runner) gives answer bytes identical to lite's edge answer for the same plan, with the
   trace id masked. This is measured over the pure/v1 golden requests and a fixed corpus subset (§5 S8). A plan with
   a node that cannot be exported is refused by name, and every refusal is listed.
5. **Import.** legend-engine's own plans for the same queries, generated by its `PlanGenerator` in the engine runner,
   import into lite. Run by lite's runner and presented by the edge's writers, they give answer bytes identical to
   legend-engine executing them (§5 S9). Refusals are by name and listed.
6. **No outcome moved without a ruling.** PCT channel A matches its expected-failure lists, channel B matches its
   ledgers, and both corpora's rosters and the edge net (§4.d) are unchanged. Each exception is a listed move, with
   upstream's answer read (§8) and ruled by the user. The product's SQL pins do not move, because the product is not
   touched (except under (a) A2).
7. **Lean.** The edge's SQL characters shrink, since the edge now sends product statements. The judge-plan pins (PCT
   channel B `*.judge.plan`, the corpus lean pins) only shrink, each with a dated note.

---

## 2. Where presentation happens today (measured at `68f20eb4d`)

### 2.1 Every SQL text builder

The columns are: *lines*, the whole file; *code*, non-blank, non-comment lines; *SQL code*, code lines inside methods
whose declared return type is a MIR or rewriter type (`SqlExpr`, `SqlSelect`, `SqlQuery`, `SqlSource`, `SqlWriter`,
`SqlRewriter`, `TypedSpec`); *Java words*, code lines in methods returning `String` or other non-MIR types. Every
row is in `core/src/main/java/com/legend/edge/`.

| File | Lines | Code | SQL code | Java words | What it builds in SQL | Reached by | Class (§3.2) |
|---|---|---|---|---|---|---|---|
| `RelationTextLowering` | 781 | 556 | **459** | 63 | A relation's `toString` (`#TDS` text), `toCSV`, a TDS's `csv`, composed over the rows with `STRING_AGG`/`CONCAT` (`tdsString` 46, `csv` 45+37, `lowerToCsv` 52, `lowerToString` 41, `tdsCell`, `cell`, `escapeCsv`, `hoistOrder`) | channel B, the corpus, pure/v1 (`Lowerer.java:4317`, through the `RelationTexts` facet) | **A, MOVE** (legend-engine never pushes these to SQL; 0 of the 2,710 DuckDB channel-A answer lines is a `#TDS` text computed in the query) |
| `PureMessages` | 481 | 325 | 93 | **228** | `sentence` (57 lines): legend-pure's raise sentence by `CONCAT` for a computed operand; `dateComponent`, `parts`. The Java half is `wordsOrNull`, `compile`, `typeName` and the test-data sentences | every edge dialect's `raiseMessage` (12 overrides) | SQL half: **raise, decision (a)**. Java half: kept, moves into the presenter family |
| `UpstreamVerdictWords` | 198 | 124 | 79 | 14 | The database judge's legend-pure failure words (24 `VerdictWords` methods): "No error was thrown", " is not empty", " should not equal " and the rest | the database judge under the edge's style | **D, RETIRE** (ARCH-3: nobody reads them) |
| `PurePrintForms` (+ `TdsNullCells`, `UnguardedParses`) | 184 | 106 | 85 | 6 | Three MIR rewrite passes: `FLOAT_TO_TEXT` → `CAST(x AS VARCHAR)` (row 8); `DATETIME_TO_TEXT` → legend-pure's print (`PureDateTimePrint`); a join's `CELL_TEXT` → `COALESCE(x,'TDSNull')` plus the CONCAT interleave `joined`; and a parse → a plain cast | every edge dialect's `spellingPasses` (12) | Float cast and parses: **B, KEEP**. DateTime print and TDSNull: **RE-RULE / MOVE** (§4.b) |
| `EdgePureOnlyTexts` | 136 | 87 | 72 | 2 | `format`'s `%0Nd` padding (10), `%r` (`formatR` 16), `toRepresentation` (`representationOf` 33), an instance's `@_` id, a date's representation | the `PureOnlyTexts` facet | **A, MOVE** (no legend-engine SQL for `format`/`toRepresentation`) |
| `EngineResultEnvelopes` | 92 | 60 | 52 | 2 | legend-engine's `tdsBuilder` and `classBuilder` result envelopes, from the database's JSON constructors with `STRING_AGG` (no sort keys: EDGE-8) | `ResultEnvelopes` facet ← `EngineExecutedValues` ← `FramePlanner` (corpus `executeLegendQuery`) | **A, RETIRE** (decision f) |
| `PureListTexts` | 49 | 26 | 19 | 2 | A list's `[a, b]` | `ListTexts` two-form kind | **A, MOVE**, or RE-RULE where it feeds SQL |
| `PureDateTimePrint` | 43 | 19 | 15 | 2 | legend-pure's DateTime print, composed with `STRFTIME` and `CONCAT` | `PurePrintForms`, `RelationTextLowering` | **A, MOVE** |
| `EdgeDateTimeTexts` | 28 | 10 | 4 | 2 | A DateTime literal's written text | `DateTimeTexts` two-form kind | **A, MOVE** |
| *the nine of the program's table* | *1,992* | *1,313* | ***878*** | *321* | | | |
| `EngineExecutedValues` | 44 | 19 | 13 | 0 | legend-engine's `{"builder":{"_type":"json"},"values":…}` envelope as a typed concatenation (`values` 7) | `ExecutedValues` two-form kind | **A, RETIRE** with (f) |
| `EdgeLoweringForms.oneBare` | (in 262) | — | 15 | — | The JsonBuilder cardinality fold: `CASE WHEN COUNT(*)=1 THEN MIN(v) ELSE JSON_ARRAYAGG END` | `LoweringForms.executeValues` | **A, MOVE** to the writer (a count of the rows) |
| `TestDataRoutes.seedJson` / `upstreamKeyText` / `timestampText` | (in 519) | — | 41 | 234 | legend-engine's seed answer JSON and key texts in SQL | the test-data routes (`Printing` two-form kind) | **A, MOVE** (S306) |
| `PureText` (12 dialect subclasses, `:346-731`) | 731 | 469 | 139 | 214 | Wiring: 12 × `spellingPasses`, 12 × `floatToText`, 12 × `raiseMessage`, 5 × `floatLiteralKind`, 2 × `raw`, 1 × `scanOrderPasses`, 1 × `refusesDecimalArithmetic`, 1 × `dateTimeFormat` (46 overrides) | every edge plan | Shrinks to the KEEP rows' overrides (≈ 2 per dialect) |
| `PlanEnv.traceText` (product package) | — | — | 5 | — | legend-engine's trace comment around the bound execution id, `CONCAT` in SQL (`PlanEnv.java:173`) | `PlanStyle.traceCell` | **A, MOVE** |
| **Total SQL-building code in these rows** | | | **≈ 1,090** | | | | |

Of the ≈ 1,090, PureText's 139 is dispatch wiring, part of which serves the kind-B rows that stay. The presentation
proper is ≈ 950.

**Not presentation**, so it stays and this program does not count it:
- `StableScanOrder` (319 SQL code lines, row 12, kind B).
- `RawSqlBoundary` (input translation).
- `CompiledPureVerdictNumbers` (typed comparisons).
- `TestDataFetches` (the generator's fetch tree, kind C).
- The plan-text family: `EnginePlans`, `PlanText`, `EngineStyleH2/DB2/Composite`, `PlanEnumForm`, `InProtocol` and
  `PlanVariables`, about 9,400 lines of legend-engine's plan and SQL *text*, which is export's base (§3.9).

### 2.2 The hook kinds and facets that carry presentation into the plan

`PlanStyle` (`database/PlanStyle.java`) has 10 methods. `testplan.PlanStyleFacets` lists 5 `KINDS` and 16 `TWO_FORMS`.
`LoweringForms` has 13 members. Classified by what each one *means*:

| Hook | Shape | Means | Target |
|---|---|---|---|
| `RelationTexts` (facet) | `SqlExpr` | presentation | deleted (presented side) |
| `PureOnlyTexts` (facet) | `SqlExpr` | presentation | deleted |
| `ResultEnvelopes` (facet) | `SqlExpr` | presentation | deleted (f) |
| `UpstreamPlans` (facet) | plan text, Java | protocol text (kind C) | stays, becomes export's |
| `EngineTestText` (facet) | judge arm | kind D (an engine-text assert) | stays |
| `DateTimeTexts`, `ListTexts` (two-form) | `SqlExpr` | presentation | deleted (writer), or legend-engine's SQL per (b) |
| `VerdictWords` (two-form) | `SqlExpr` | presentation (the judge's words) | the edge's form retired (c) |
| `ExecutedValues` (two-form) | `SqlExpr` | presentation | deleted (f) |
| `TestDataPlanner.Printing` (two-form) | `SqlExpr` | presentation | writer |
| `DateLiteralTexts`, `RoutedQueries`, `ActivityTexts` (two-form) | Java text at planning | protocol and plan text (kind C) | stay as plan data. Words at planning are a fact of upstream's *plan text*, not of an answer |
| `GoldenNullWord`, `RenderedTextRule`, `JsonGrammar`, `RelationCounting`, `CrossKindNumbers`, `VerdictNumbers` (two-form) | rules | judging (kind D) | stay |
| `DatabaseSettingsForm` (two-form) | settings | computation (B5) | stays |
| `LoweringForms` (two-form) | 13 members | 9 computation (B), **2 presentation** (`executeValues`/`oneBare`, `objectReferenceConnection`), 2 plan text only (`unionFrame`, and `EngineShape`'s own) | presentation members leave |
| `PlanStyle.siteWords` | recorder | presentation (words of sites) | deleted |
| `PlanStyle.errorWords` | Java words at planning | presentation (a planned `Failure`'s message) | deleted (c) |
| `PlanStyle.traceCell` | constants into SQL | presentation | writer |
| `PlanStyle.graphKeyDate` / `graphKeyString` | text literal at planning | protocol text inside the database-built graph JSON (kind C) | stays, listed (§4.b row C2) |

### 2.3 The plan fields that carry presentation

| Format | Field | Today | Target |
|---|---|---|---|
| plan v58 (`PlanJson.java:171`) | top-level `raises` | kind name → lite's sentence (`ErrorKind.sentence()`), derived from the sites (`:241`, `:255-264`), checked again on read (`:1057-1060`) | `raises`: a sorted list of kind names, no sentence |
| plan v58 | a statement's `sites[].message` | lite's sentence, validated against `kind`+`where` on read (`:1405-1442`, `ExecutionPlan.Site:843-870`) | dropped. A site is `n`, `kind`, `where` and `operands` (§3.3) |
| plan v58 | `Column.precision` | a fact (`DAY`, `WRITTEN`, `INSTANT`, `MINIMAL`) | **kept** as a fact both writers read (COMPLETENESS-9a) |
| host plan halves (`HostAnswers.arrowPlan:179`, `jsonPlan:234`) | `siteWords` | legend-pure's words per site, written at `:212` and `:279`, **read by no host** (ARCH-1) | deleted. The answer half gains an error entry (§3.8) |
| test plan v32 (`TestPlanJson.java:60`) | `TestPlan.words` | a word-family name (`"lite"` or the edge's) | replaced by the plan's `presentation` |
| test plan v32 | `TestPlan.answerSizeWords`, `SideShape.sizeWords` | a sentence worded at planning by the style | dropped. The hearing words SIZE_NOT_MULTIPLICITY from `SideShape.multiplicity`/`lower` |
| test plan v32 | `Failure.message` of a planned failure (`AssertErrorNative:76-82`, via `PlanStyle.errorWords`) | upstream's sentence at planning | `Failure(kind, operands, liteMessage)`; the hearing words it |
| test plan v32 | `SiteWords` extension (`edge/SiteWords.java`) | legend-pure's words keyed `(step, at, n)` | deleted |
| test plan v32 | the database verdict row's `MESSAGE_COLUMN` (`RootLoop.java:43`) | the message built in SQL (`VerdictWords`) | product: lite's words stay (ruling of 2026-11-17 01:30). Edge: lite's words (c) |
| test plan v32 | `SideShape.oneCarrier`, `tdsNullRoot` | facts that name upstream shapes | kept (facts, not words) |

### 2.4 Where the golden net pins SQL, and what its mask hides

- **SQL pinned as answer bytes.** `PctCaseRecorder.java:108-114` writes each relation answer's `activities` (the
  executed SQL) into the answers golden. I counted the lines carrying `activities=`:
  - DuckDB: relation 460, standard 64;
  - H2: relation 439;
  - SQLite: relation 459, standard 64.

  That is 1,486 lines. The PCT adapter never reads them: `ValueBridge.engineTDS` reads `builder.columns` and
  `result.rows`. In `pure_v1_answers.tsv`, all 8 answer lines with activities carry **lite's own SQL** (`"t0"`
  aliases), not legend-engine's. That is client-visible.
- **The mask erases values.** `PctCaseRecorder.java:64-65` masks UUIDs and any run of 16 or more hex digits. In VALUE
  lines (no activity, so no trace id) it hides 171 values: DuckDB essential 17, grammar 9, standard 59, unclassified 3;
  SQLite 17, 4, 59 and 3. These are 64-bit integers and 17-digit doubles. `sqlite_relation_answers.tsv` has 831 masks
  against 459 trace ids. `PureV1EdgeGoldenTest.java:66` masks UUIDs only, which is correct.
- **Order is erased.** `tools/edgenet/EdgeNormalize.java` sorts every relation's rows, including a sorted query's.
- **Messages are not pinned where this program moves them.** The channel-B outcome files and the corpus
  `*_judge-host.tsv` and `*_outcomes.tsv` hold verdicts only (`PASS`, `SUCCESSFUL`). A failing assert's message, which
  is the text this program moves to Java, is in no golden. Only channel A's `ERROR` answer lines hold a message (16 in
  DuckDB essential).
- **Not in CI.** `//pct:edge_contract_test` (36 diff tests) is in `//gates:heavy` only (`gates/BUILD.bazel:489-491`).

### 2.5 The hosts

| Host | Reads | Words a raise from | Reads `siteWords` |
|---|---|---|---|
| Java (`exec/RaisedErrors.java:83-94`, `Raised:105-140`) | the site by token | `ErrorKind` (lite's), plus `edge.RaisedWords` on the server and PCT routes | n/a |
| TypeScript (`engine-client/src/raised.ts:58-67`, `declared-raises.ts`, `plan-runner.ts:301` `PLAN_VERSION = 58`) | `sites[].message`, `raises` (its presence is checked) | the plan's `message` | no |
| Python (`python/legend_lite/_raised.py:73-95`, `_runs.py:27` `PLAN_VERSION = 58`) | `sites[n-1]['message']` | the plan's `message` | no |

Both hosts present legend-engine's answers by calling the Java edge's answer half: wasm `EngineExports`
(`wasm/src/edge/.../EngineExports.java:26-46`) and native `EngineEntries` (`lite_execute_json_answer` and
`lite_execute_arrow_answer`). **Neither host has an error half.** Lite's raise constants already reach TypeScript as
generated code (`engine-client/src/generated/lite-raise.ts`, from `engine-client/tools/typefacts/TypeFacts.java`, with
a diff test). That is the pattern §3.8 reuses.

### 2.6 Where this reading differs from the audit

1. **The SQL-code count agrees.** For the nine files I count 878 code lines in SQL-building methods; the audit has
   ~900 (EDGE-4). Across every presentation builder (§2.1) it is ≈ 1,090. EDGE-4's "missing" list is right, and
   `TestDataRoutes.seedJson` (ARCH-2) and `PlanEnv.traceText` belong to it too.
2. **`LoweringForms` is not "mostly computation" (ARCH-2).** Two of its 13 members are presentation:
   - `executeValues`, which delegates to `oneBare`;
   - `objectReferenceConnection`, a protocol constant.
3. **Relation text needs no kind-B row.** Neither the program nor the audit says this. In channel A, where the
   adapter's `eval` boundary is physical, no case computes a relation's text inside the query: 0 of 2,710 DuckDB
   answer lines is a `String:#TDS…` value. legend-engine 4.145.0 maps none of `toCSV`, `format`, `makeString` or
   `toRepresentation` among the 451 function pairs of `pureToSQLQuery.pure`, and its PCT adapter reads a relation back
   and legend-pure prints it (`docs/result-boundary-homework/README.md` §7). So `RelationTextLowering` is pure
   presentation, and the largest builder (459 of ≈ 1,090 lines) leaves whole.
4. **Raise operands have no measured golden.** ARCH-6 and PRODUCT-5 are right that no raise returns its operands. But
   D's operand census (the plan of record's log, 2026-11-16 21:05) found **zero computed operands in any
   upstream-facing golden**. Decision (a) therefore guards untested inputs and moves no measured case. The audit does
   not cite this, and it lowers the stakes.
5. **ARCH-1's premise is refuted.** V-HOSTS shows the server answers `Raised: <KIND>` too (V-HOSTS-A), so no pure/v1
   route words a literal site the legend-pure way today. F5 in the fix round settles the baseline. This design starts
   from whatever F5 lands, and S1 replaces the mechanism either way.
6. **The net pins verdicts, not messages (§2.4).** The audit (GUARDS-5, ARCH-5) treats the net as an answer-and-message
   net. For the text this program moves, it pins nothing yet. S0 must add messages first.
7. **pure/v1's activity SQL is lite's, not legend-engine's.** This is true today, so pure/v1's relation answers are not
   upstream's bytes wherever SQL appears. With "one plan" it stays lite's SQL, which is truthful, as S310 rules for the
   product. Only an *imported* plan reproduces legend-engine's activity bytes. This is a register row to write
   (§4.b, row C3).

---

## 3. The target design

### 3.1 The principle

- **The plan says what runs and whose text a reader gets.** The planner writes statements that compute typed values,
  plus one `presentation` value (`LITE`, `LEGEND_PURE`, `LEGEND_ENGINE`) and typed facts: column types, precision,
  decimal digits, a raise site's kind, where and operand slots, and a side's bound. It writes no sentence and no
  printed value.
- **The format is the caller's.** JSON rows, CSV, Arrow, pure/v1 JSON or a collector are chosen at run (`Output`), as
  today (plan of record §10 item 11).
- **The runner is unchanged.** One road; it sends the plan's statements and streams typed rows (G1, G4).
- **Writers present.** A writer formats typed values in the plan's presentation and decides nothing
  (`WriterCarriageGuardTest`). The edge's writers are the presenter family's upstream members (§3.4). The product's
  are `result.CellText`/`JsonRowsWriter`/`CsvWriter`/`ArrowWriter`.
- **Kind B is the exception, and it is listed.** Where legend-engine computes a function inside its SQL, the edge's
  statement uses legend-engine's SQL for it. Each such row is a user-ruled ledger entry the guard reads (§6).

### 3.2 The rule: computation or presentation

For each text-producing function (`toString`, `toCSV`, a TDS's `csv`, `format`, `makeString`/`joinStrings`,
`toRepresentation`, `executeLegendQuery`'s string, a cell's empty word):

1. **legend-engine pushes it to SQL** (it has a `PureFunctionToRelationalFunctionPair` and a `dynaFnToSql` in the
   database's extension). Then, *inside a query*, the edge computes it in SQL **with legend-engine's own SQL** (kind
   B). Example: `toString_Any_1__String_1_` → `processDynaFunction` → H2 `cast(%s as varchar)`
   (`h2Extension2_1_214.pure:266`), which is row 8 for a Float. **The same cast applies to a DateTime, a StrictDate and
   a Boolean.** Today the edge writes legend-pure's DateTime print in SQL there (§4.b row R1).
2. **legend-engine does not push it to SQL.** It runs after the SQL, in legend-pure's runtime (PCT, the corpus's test
   bodies) or in a platform node (pure/v1). Then it is **presentation**: the plan returns the operand's typed rows,
   and a writer prints them in Java with legend-pure's print. This covers relation text, `toCSV`, `format`'s `%r` and
   padding, `toRepresentation`, an instance's id and the result envelopes.
3. **A function of kind 2 whose text feeds a further computation inside the query** (a filter over `toCSV`, say) has
   no upstream SQL form. The edge refuses it by name, in legend-engine's words where legend-engine refuses
   ("No SQL translation exists for the PURE function '…'"), never by inventing SQL. The product's own forms are
   unaffected. The rows' counts are measured in S5 before the user rules (§4.b).

*Where the query ends* is physical in channel A and in pure/v1. In channel B, `ChannelB.eliminateAdapter`
(`ChannelB.java:483-…`) erases the `$f->eval(|expr)` boundary. The rule above does not need the boundary for kinds 2
and 3. It needs it only for a kind-1 print written *outside* the eval in a channel-B body, where legend-pure would
print in Java and the rule computes legend-engine's cast. Today's edge ignores the boundary the same way, so nothing
regresses. Decision (b0) in §4.b decides whether to add a boundary marker.

### 3.3 The plan's records (format v59, test plan v33)

```java
// com.legend.executionplan -- plan data, the product's
/** Whose text a reader of this plan's answer, errors and verdicts gets: a fact the planner states (G4), never a
 *  writer's choice. LITE: lite's forms (S311, S312, ErrorKind's sentences). LEGEND_PURE: legend-pure's print and
 *  sentences (PCT, the corpus). LEGEND_ENGINE: legend-engine's pure/v1 answers. */
public enum Presentation { LITE, LEGEND_PURE, LEGEND_ENGINE }

// ExecutionPlan gains one component, written by the planner from the door's style:
//   Presentation presentation
// the product's door writes LITE; PureText.UPSTREAM writes LEGEND_PURE; PureText.PURE_V1 writes LEGEND_ENGINE.

/** A raise site (v59): what the token LG#<n> means. No words. */
public record Site(int n, RaiseKind kind, @Nullable String where, List<Operand> operands) { ... }

/** An operand of a site's sentence, typed: the writer prints it in the plan's presentation. */
public sealed interface Operand permits Operand.Fixed, Operand.Returned {
    /** A literal the planner knew: its Pure type and its written text (an Integer's digits, a date as written). */
    record Fixed(String type, String text) implements Operand { }
    /** A value the database returns with the raise (decision (a), A1): slot i of the raise's payload, read back by
     *  its declared Pure type as a carrier is (Phase 8's exact-carrier rule). Only an edge plan writes one. */
    record Returned(String type, int slot) implements Operand { }
}
```

- `PlanJson.VERSION` becomes 59:
  - `presentation` is added;
  - `raises` becomes `["DIVIDE_BY_ZERO", …]`;
  - `sites[]` becomes `{n, kind, where?, operands: [{type, text} | {type, slot}]}`, with no `message`;
  - a v58 plan is refused by its version.
- `TestPlanJson.VERSION` becomes 33:
  - `words` becomes the plan's `presentation`;
  - `answerSizeWords` and `SideShape.sizeWords` are gone;
  - `Failure` becomes `{kind?, operands, message}`;
  - the `SiteWords` extension is gone;
  - a side may carry `presented` (below).

```java
// com.legend.testexecutionplan
/** A side whose value is the PRESENTATION of a relation or a serialized root (§3.2 kind 2): its statement returns the
 *  typed rows (a TABULAR side), and the hearing presents them in the plan's presentation before it compares. */
public record Presented(Form form, boolean sorted) {
    public enum Form { PURE_TDS_TEXT, PURE_CSV, PURE_TDS_CSV, ENGINE_TDS_JSON, ENGINE_CLASS_JSON, ENGINE_VALUES_JSON }
}
// SideShape gains: @Nullable Presented presented
```

### 3.4 The presenter family

**A product contract in `com.legend.result`, implemented at the edge, found through the existing `ServiceLoader`
seam.** That is the same seam as `ExtensionCodec` and `MountProvider`, so G5 holds: the edge depends on the product.
Each family is a record of typed components, so javac checks every form (ARCH-4: no class-token lookup).

```java
// com.legend.result (product)
public record PresenterFamily(Presentation presentation, ErrorWords errors, ValueTexts values, AnswerWriters answers) { }

public interface ErrorWords {
    /** An error's words in this presentation, from its kind, its where and its typed operands. */
    String words(com.legend.error.ErrorKind kind, @Nullable String where, List<Object> operands);
}
public interface ValueTexts {
    /** A value's text: refused by name outside its types (invariant 4). */
    String print(@Nullable Object value, String pureType, ExecutionPlan.@Nullable Precision precision);
    /** A relation's presented text; refused by name in a family that has none (LITE: S316b). */
    String relation(Presented.Form form, List<ExecutionPlan.Column> columns, List<List<Object>> rows);
}
public interface AnswerWriters {
    /** The writer of plan's answer in the caller's format: the family's, or refused by name. */
    ResultSink writer(Output output, ExecutionPlan plan, java.util.@Nullable UUID execution);
}
public interface PresenterFamilyProvider { PresenterFamily family(Presentation p); }   // META-INF/services

public final class Presenters {
    /** LITE is built in (CellText, JsonRowsWriter, CsvWriter, ArrowWriter, ErrorKind); any other comes from the
     *  classpath's provider, refused by name where none serves it ("a LEGEND_ENGINE plan is presented by the
     *  compatibility edge, com.legend.edge, which is not on this classpath"). */
    public static PresenterFamily of(Presentation p) { ... }
}
```

| Family | `errors` | `values` | `answers` |
|---|---|---|---|
| `LITE` (product) | `ErrorKind.sentence()` + `": " + where`, as `DataError` spells it today | `CellText` | `JsonRowsWriter`, `CsvWriter`, `ArrowWriter` |
| `LEGEND_PURE` (`edge.PureFamily`) | `PureMessages.wordsOrNull` over **typed values**, the `SqlExpr`-literal parsing gone | `edge.Presenter.legendPure` plus `edge.RelationText` (new, Java): `#TDS` text, `toCSV` and `csv` over rows, the logic of `RelationTextLowering`'s `tdsString`/`csv`/`escapeCsv` in Java, `TDSNull` at an empty cell, `%r`/`toRepresentation` from `representationOf`'s rules | the PCT route's engine JSON (`EngineAnswers.engine`) |
| `LEGEND_ENGINE` (`edge.EngineFamily`) | pure/v1's answers per kind (census classes C, D and E; F43/F44 read first) | `Presenter.engine` (JSON tokens), `Presenter.LEGEND_PURE` dates for Arrow | `EngineAnswers.engine`/`.arrow`, `PureV1Writer` including the `objects`/`json` builders and the JsonBuilder cardinality rule (`oneBare` as a count of the written rows) |

`Output.writerFor(plan, execution)` asks `Presenters.of(plan.presentation()).answers()`. A caller's receiver that
does not serve the plan's presentation is refused before any statement runs (as §10 item 11's receiver refusal
already does). The door chooses the *format*; the plan states the *presentation*. That is ARCH-8 resolved.

### 3.5 Raises: from a token to words

1. **Planning.** The dialect's `raiseMessage` (`AnsiSqlRenderer.java:1229`) writes the site's token. A `RaiseSite`
   (`sql/dialect/RaiseSite.java`) holds `operands` as MIR. The planner turns each literal operand into
   `Operand.Fixed(type, text)`, the logic of `PureMessages.wordsOrNull`'s literal test moved to where the site becomes
   plan data. Under (a) A1, the edge's `raiseMessage` override appends each computed operand as a carrier after the
   token (`MARK LG#n MARK <carrier 1> …`) and records `Operand.Returned(type, i)`. `PureMessages.sentence` is deleted.
2. **Run.** `exec.RaisedErrors.siteOf` reads the token and the carriers. `Raised` gains `List<Object> operands()`:
   fixed operands are decoded from the site's text, returned ones by their declared type. `DataError` gains
   `operands()` and `where()`. Its `getMessage()` stays lite's sentence: `ErrorKind` is the error layer's own form.
3. **The boundary.** A door that presents in another family words the error with
   `Presenters.of(plan.presentation()).errors().words(e.kind(), e.where(), e.operands())`:
   - pure/v1 (`EdgeServer`, `PureV1Routes`);
   - the PCT route (`PctPlanRoute`, `PctExecuteNative`);
   - the hearing (`EdgeHearing`, `Appeals#raisedWords`);
   - a host's error half (§3.8).

   `RaisedWords` becomes this one stateless call. `SiteWordsRecorder`, `PureText.recording`, `PlanStyle.siteWords`,
   `SiteWords` and `HostAnswers.siteWordsOf` are deleted. The state the audit flagged (a recorder minted per call,
   sealed after one read) goes with them.

### 3.6 Presented answers and presented sides

- **A query whose root is a kind-2 print** (a pure/v1 `|#TDS…#->toString()`, or a corpus `executeLegendQuery`) plans
  the operand's typed result. The writer presents the root in the family's `values().relation(form, …)`, so the answer
  is a VALUE holding that text. The plan's root node is the operand's `TdsResult` plus a `Presented` fact:
  `TdsResult` gains `@Nullable Presented presented`.
- **A test body's side** whose expression is a kind-2 print over a query result (channel B's
  `assertEquals('#TDS…#', $res->toString())`, the corpus's `toCSV` asserts) is a TABULAR side with `presented` set.
  The host judge reads the rows, presents them, and compares the strings by the `RenderedTexts` rule. S131 and S132's
  leniency are re-measured in S4: with Java print the text should match exactly, and the leniency is expected to
  shrink. The database judge declines a presented side by name under the edge ("a presented side is the host's: its
  text is Java's"). HOST is the compatibility judge (the user's ruling of 2026-11-17 01:30), and the product refuses
  relation text anyway (S316b), so G2's product interchangeability check is untouched.
- **EDGE-1 and EDGE-2 go by construction.** A relation cell's Float is printed by `Presenter.legendPure`
  (`DecimalFormat("0.0")`, never an exponent). A CSV DateTime is printed by `SimpleDateTimeFormat`'s rule, the one
  `EnginePlans.testDataTimestampText` already holds, so there is one owner. **EDGE-3** goes because `%r` has one owner
  in Java. **EDGE-8** goes because the envelope's rows are written in the result's order by `PureV1Writer`.

### 3.7 The judges

- **The host judge** words every failure through `testexec.HostWords` (`LiteHostWords` or `EdgeHostWords`, chosen by
  the plan's `presentation`, which replaces the `words` name). A planned failure's and a size refusal's words come
  from kinds and bounds at the hearing (decision (c)).
- **The database judge.** In the product it keeps lite's words in SQL (`LiteVerdictWords`), per the 2026-11-17 01:30
  ruling ("lite's own wording built in SQL, zero Java"). Under the edge it is planned with the same lite words, and
  `UpstreamVerdictWords` is deleted (its 79 SQL lines and ARCH-3's four-implementation problem). Its words are read
  only by the `JudgesDisagree` diagnostic, which compares verdicts (`IsolatedRun.java:112`, `messages=false` under the
  edge).

### 3.8 How a host presents (TypeScript, Python)

- **The plan half.** `HostAnswers.jsonPlan` and `arrowPlan` keep their shape minus `siteWords`. They carry the v59
  plan.
- **The answer half** is unchanged: `jsonAnswer(plan, rows)` and `arrowAnswer(plan, rows)`, in Java (wasm/native),
  reading `plan.presentation`.
- **A new error half:** `HostAnswers.errorAnswer(String plan, String statement, String databaseMessage)`, exported as
  wasm `executeErrorAnswerOrError` and native `lite_execute_error_answer`. The Java edge reads the site and its
  operand carriers from the database's message and answers pure/v1's error JSON in the plan's family. The hosts call
  it on a send's failure on an edge route (`edge-engine.ts:58-63`, `_pure_v1.py:200-238`) instead of building a
  `PlanRaise` from the plan. This is ARCH-1's second option. It deletes the hosts' last reading of words.
- **Lite's own routes** (the product) word a site with lite's sentence from a **generated table**:
  - TypeScript: `engine-client/src/generated/lite-errors.ts`;
  - Python: `python/legend_lite/_generated_errors.py`.

  Both are emitted by `TypeFacts.java` from `ErrorKind`, like `lite-raise.ts`, with a diff test. `raised.ts:67` and
  `_raised.py:95` read the table by `kind`, plus `where`. `PLAN_VERSION` in `plan-runner.ts:301` and `_runs.py:27` is
  generated the same way, so a bump cannot leave a host behind.

### 3.9 Export: a lite plan as legend-engine's plan

**What exists.** `PureV1Routes.generatePlan` (`:256-290`) and `executionPlan` (`:363-422`) write one
`relationalTdsInstantiation` over one `sql` node, with `sqlQuery = plan.sql()` and legend-engine's template
functions.

**The target:** `edge.PlanExport.singleExecutionPlan(ExecutionPlan plan, Map<String,Object> connection) →
Json.Obj`, a translation per node that refuses by name:

| lite node | legend-engine node | Notes |
|---|---|---|
| `TdsResult` RELATION | `RelationalTdsInstantiationExecutionNode` over `SQLExecutionNode` | as today |
| `TdsResult` VALUE | `RelationalDataTypeInstantiationExecutionNode` over `SQLExecutionNode` | new; the node's exact JSON is read from legend-engine's own plan for a value query in S8 |
| `TdsResult` OBJECTS | refused by name | legend-engine instantiates classes with generated Java |
| `Constant` | `ConstantExecutionNode` | exists (`constantPlan`) |
| `Sequence` | `SequenceExecutionNode` | the last step's result type |
| `Keyed` (dynamic pivot) | refused by name | legend-engine's runner has no keyed regions (lite's A3(a)) |
| `Load` | `CreateAndPopulateTempTableExecutionNode` where it is a temp-table IN, else refused | |
| `Update`, `Check`, `Effect` | refused by name | a test's or a write program's |
| a statement's bound parameters | FreeMarker `${…}` with legend-engine's placeholder functions | re-rendered from the plan's `tree` with a `SqlWriter` that writes each parameter as legend-engine's placeholder, never by editing the text (invariant 3). The exact placeholder spelling is read in S8's first step from `dbExtension.pure` 4.145.0 |
| a raise site | refused by name, unless its kind is census class A under `PURE_V1` (already a plain operation) | legend-engine has no site table, and its own plans raise nothing in SQL (classes A and B) |

The exported plan's SQL is lite's product SQL. legend-engine sends it verbatim and serializes the typed results with
its own serializer, which is done-when 4. `generatePlan`'s double planning (ARCH-11) goes. The ~9,400 lines of
legend-engine plan *text* (`EnginePlans`, `EngineStyleH2` family) stay as the `toSQLString`/`planToString` answers
(kind C). They are not export.

### 3.10 Import: legend-engine's plan as a lite plan

**The target:** `edge.PlanImport.fromEngine(Json.Obj singleExecutionPlan) → ExecutionPlan` with
`presentation = LEGEND_ENGINE`. It is the edge's planner at import time. It runs nothing and edits none of lite's SQL.
Its input is upstream's template text, translated through a `SqlWriter`, as `RawSqlBoundary` translates input today.

| legend-engine node | lite | Rule |
|---|---|---|
| `SQLExecutionNode` | `Sql` + `TdsResult` | `sqlQuery`'s FreeMarker resolved: each `${var}` and each known placeholder function (a closed list read from 4.145.0) becomes a bound `Slot`; an unknown construct is refused by name. `resultColumns` and the result type give the plan's typed `Column`s |
| `RelationalTdsInstantiationExecutionNode` | `TdsResult` RELATION | `tdsColumns` → `Column.type` |
| `RelationalDataTypeInstantiationExecutionNode` | `TdsResult` VALUE | |
| `ConstantExecutionNode` | `Constant` | |
| `SequenceExecutionNode`, `MultiResultSequenceExecutionNode` | `Sequence` | |
| `AllocationExecutionNode` | a bound value of a later statement where the allocation is a scalar the plan computes in SQL; else refused | |
| `CreateAndPopulateTempTableExecutionNode` | a `Load` (data in the plan: `docs/DATA_IN_THE_PLAN_2026_11_09.md`) | |
| `FreeMarkerConditionalExecutionNode` | **refused by name** | needs a product planned branch, which is not on A3's ratified list (decision e) |
| any node with `implementation` (`JavaPlatformImplementation`), `PureExpressionPlatformExecutionNode`, graph-fetch nodes (`graphFetch/*`), M2M, `ErrorExecutionNode`, `RestServiceExecutionNode`, external-format nodes | **refused by name** (decision e) | G4 (no code in a plan), G1 (one evaluator) |

The node list was read from legend-engine 4.145.0's protocol package
(`legend-engine-protocol-pure/.../executionPlan/nodes/`, 40 classes, and `graphFetch/` with 15 more). `ExecutionNode`
carries `implementation`, so the refusal is by field as well as by type.

### 3.11 What is deleted and what stays

- **Deleted:**
  - every file in §2.1's MOVE and RETIRE rows (≈ 950 SQL code lines);
  - `SiteWordsRecorder`, `SiteWords`, `RaisedWords`' recorder, `PlanStyle.siteWords` and `PlanStyle.errorWords`;
  - the `RelationTexts`, `PureOnlyTexts` and `ResultEnvelopes` facets;
  - the `ExecutedValues`, `ListTexts`, `DateTimeTexts`, `VerdictWords`-edge and `Printing` two-form kinds;
  - 3 of 7 `@OpenLine` product facets (`lowering/RelationTexts`, `PureOnlyTexts`, `ResultEnvelopes`);
  - the hosts' `siteWords`.
- **Stays:**
  - the kind-B rows the user keeps;
  - the plan-text family (kind C);
  - the judging rules (kind D);
  - `Column.precision`.
- **Then the hook surface collapses (ARCH-4, slice S7).** `PlanStyle` keeps `dialect`. Each layer gets a typed record
  of its *computation* forms: `lowering.Styled` (exists), a judge record, a planner record. `PURE_V1` becomes
  `UPSTREAM` with one component replaced. `KINDS`/`TWO_FORMS` and their guards shrink to what remains.

---

## 4. The hard problems: decisions for the user

### (a) Raise operands

**RULED (the user, 2026-11-18, relayed by the orchestrator: "go with your recommendations on all seven decisions"): A1.**

Today the database returns only `LG#<n>` (`AnsiSqlRenderer.site`, `:1241-1262`). The edge words a literal-operand
site at planning (`PureMessages.wordsOrNull`), and writes legend-pure's sentence in SQL for a computed one
(`PureMessages.sentence`, 57 SQL lines).

| Option | What | Cost | Fidelity |
|---|---|---|---|
| **A1 (recommended)** | The edge's raise appends each computed operand as a typed carrier after its token (`Operand.Returned`). Literal operands become `Operand.Fixed`. The product writes the token alone, as now. | One kind-B row (R-OPERANDS), in edge SQL only, on computed-operand raises only. That is 0 measured upstream-facing goldens (§2.6 item 4), so no golden move is expected. Exact carriers per database already exist and are measured by the product probe (Phase 8). About 1 day. | exact |
| A2 | The same carriers in the **product's** SQL too (truly one plan, no row). Lite's words ignore them. | The user amends S308's built form ("the value never in the product's *message*; it may travel as data"). Product SQL grows in error branches only, which cost nothing at run. Lean pins move, with a dated reason. About 1.5 days. | exact; the product could later word richer errors |
| A3 | No operands. A computed-operand site at the edge answers the kind's sentence without the value, as a register row. | Cheapest (0.5 day). A ledgered divergence that no golden measures today. | not exact |

**Recommendation: A1.** It keeps two of the user's rulings as built (S308 and the lean product SQL), stays exact at the
edge, and costs nothing measurable. Choose A2 if "one plan" should hold with no raise row at all.

### (b) The kind-B rows, re-ruled under "one plan"

**RULED (the user, 2026-11-18, relayed by the orchestrator: "go with your recommendations on all seven decisions"): the rule (b0), no boundary marker for now, and the 26-item classification below. The RE-RULE rows and the CHANGE row come back to the user with their evidence and PCT moves before they are built.**

**(b0) The rule.** Adopt §3.2: compute in SQL only what legend-engine pushes to SQL, and with its SQL; present the
rest in Java; refuse a kind-2 print that feeds a computation.

- *Boundary marker:* **recommended no for now.** Keep channel B's erased `eval`. Add an internal marker native (F32's
  refusal to user code applies) only if S4/S5 measure a channel-B move caused by the erased boundary.

Each row below: what it is, upstream's evidence, the "one plan" verdict, and the recommendation.

| Row | What (where) | Upstream | Verdict | Recommendation |
|---|---|---|---|---|
| B1 (§6 r8) | Float `toString` in a query → `cast(x as varchar)` (`PurePrintForms.floatOf`, 12 `floatToText`) | `h2Extension2_1_214.pure:266`; duckdbExtension | computation | **KEEP.** Narrowed to in-query use, since relation cells become presentation (EDGE-1 fixed by construction) |
| B2 (r21) | Float literal kind DECIMAL (5 `floatLiteralKind`) | ruled R2, 2026-11-17 | computation | **KEEP** |
| B3 (r22) | SQLite Decimal arithmetic as REAL | ruled (c), 2026-11-17 | computation | **KEEP** |
| B4 (r12) | DuckDB engine scan order (`StableScanOrder`) | the corpus's replay order | computation | **KEEP**, as a style fact instead of `Boolean.getBoolean("legend.exec.engineScanOrder")` (ARCH-9, G10) |
| B5 (J22) | H2 `MODE=LEGACY`, `DEFAULT_NULL_ORDERING=HIGH` | ruled J22 | computation | **KEEP** |
| B6 (F31) | a parse as a plain cast (`UnguardedParses`) | H2 `cast(%s as integer)`/`as float` (`:248-249`) | computation | **KEEP**, and fix F38 in the same slice (H2 INTEGER, not BIGINT: confirmed at `:249`) |
| B7 (B7) | pure/v1's unchecked raises plain (`ENGINE_UNCHECKED`) | census class A | computation | **KEEP** |
| B8 (J20) | `isNumeric` as `lower(s)=upper(s)` | `h2Extension2_1_214.pure:232` | computation | **KEEP**; per database from its own extension (S5 reads DuckDB's) |
| B9 (J19, S367) | EXISTS as joins | legend-engine's second exists emission | computation (row multiplicity) | **KEEP** |
| B10 (J1) | `parseDecimal` as `decimal(5,2)` | `transformParseDecimalH2` (`:247`) | computation | **RE-RULE:** today it applies on *every* edge database; legend-engine's form is H2's. Per database from each extension |
| B11 (J2) | `parseJSON`'s `}{` → `},{` tolerance | "Json.parseOne lenient", lite's reading | unclear: query or judge? | **RE-RULE with evidence:** if legend-engine parses the golden in Java at the assert, it belongs to the judge's `JsonGrammar` (S144) and leaves the SQL |
| B12 (L10) | WHERE conjunct zones (`whereZones`) | legend-engine's order | plan shape unless it changes an answer | **RE-RULE:** keep in the executed SQL only if a case shows an answer change (an error or short-circuit). Else it becomes plan text only (`EngineShape`) |
| B13 | `RawSqlBoundary.translated` (legend-engine's H2 text for DuckDB and Postgres) | none: lite's adaptation | input translation | **KEEP** as a listed input row (I1) |
| B14 (§10 i2, F5) | an unused derived column that can raise: pruned or kept | not yet read | computation | **RE-RULE** once the engine runner answers (F5 of the plan of record) |
| R1 | DateTime `toString` in a query (`PureDateTimePrint` in SQL) | `toString_Any_1` → `cast(%s as varchar)` for every type | today *not* upstream's | **CHANGE** to legend-engine's cast. PCT moves are listed first, and a move legend-engine shares is accepted, as §10 item 6 did for Floats |
| R2 | a list's `[a, b]` in a query (`PureListTexts`) | no `makeString` pair | kind 2 | **MOVE** where presented; **refuse** where it feeds SQL (moves listed) |
| R3 | `TDSNull` in a join in a query (`TdsNullCells`, `joined`) | `joinStrings` → `group_concat` (`:235`); the empty word is legend-pure's | kind 2 at a side, kind 1 in a query | **MOVE** at a side or root. In a query: legend-engine's `group_concat` form, with moves listed |
| R4 | `format`'s `%r`, `%0Nd`, `toRepresentation`, an instance's `@_` (`EdgePureOnlyTexts`) | no pair for `format`/`toRepresentation` | kind 2 | **MOVE**; refuse where it feeds SQL |
| M1-M5 | relation text, `toCSV`, `csv` (`RelationTextLowering`); the envelopes (`EngineResultEnvelopes`, `EngineExecutedValues`, `oneBare`); the seed JSON (`TestDataRoutes.seedJson`); the trace cell (`PlanEnv.traceText`) | after the SQL | presentation | **MOVE** (no ruling needed beyond (f): it follows the program as approved) |
| C1 | `objectReferenceConnection` (H2's connection JSON in an object reference) | legend-engine's constant | protocol text | **KEEP** as a planned constant (kind C), listed |
| C2 | graph-key texts (`graphKeyDate`/`graphKeyString`) | legend-engine's key print | protocol text inside the database-built JSON | **KEEP** as a planned constant (kind C), listed |
| C3 | pure/v1 activity SQL is lite's | legend-engine's own SQL | truthful (S310's principle) | **NEW REGISTER ROW** for the user to rule: the edge reports the SQL that ran; only an imported plan reproduces legend-engine's bytes |

### (c) The test plan's words and the database judge's words

**RULED (the user, 2026-11-18, relayed by the orchestrator: "go with your recommendations on all seven decisions"): C1.** C2 is not ruled; it stays a separate question.

| Option | What | Cost |
|---|---|---|
| **C1 (recommended)** | The test plan carries kinds and bounds (§3.3), and the hearing words them through `HostWords`. `UpstreamVerdictWords` is deleted. Under the edge, the database judge plans `LiteVerdictWords` (unread, ARCH-3). The product's database judge is unchanged (01:30 ruling). | About 1 day. The judge-plan pins shrink (the edge's CONCATs leave `channel_b.essential.judge.plan`, 28.68M characters). |
| C2 | C1, plus lite's words leave the database judge's SQL: the verdict row returns typed operands (expected, actual, index, bound), and `HostWords` words both judges. | **Re-rules 01:30** ("lite's own wording built in SQL, zero Java"). About 2 days. One wording owner; smaller product judge plans. |
| C3 | Keep the edge's database words. | Leaves ARCH-3's four implementations. Not recommended. |

**Recommendation: C1 now.** C2 is a separate question for the user, outside this program's need.

### (d) The safety net

**RULED (the user, 2026-11-18, relayed by the orchestrator: "go with your recommendations on all seven decisions"): D1, as slice S0, first.** One point stays open: `gates/BUILD.bazel` marks recording PCT on SQLite in CI as the user's own call. S0 brings it back to the user with the lane's measured cost.

| Option | What | Cost |
|---|---|---|
| **D1 (recommended), slice S0** | 1. `PctCaseRecorder` moves `activities` to an informational file no diff test reads; pure/v1's activities go to their own diff file, re-recorded only by a slice that names the move. 2. Mask by field: the `executionTraceID` comment's id and UUIDs only, never a value (GUARDS-6). 3. `EdgeNormalize` keeps a relation's order where the query sorts (the plan's root has an ORDER BY), and sorts otherwise. 4. Pin messages: channel-B ledgered and failing rows' messages, corpus failure messages, pure/v1 error bodies. 5. Add the uncovered cases: EDGE-1's five values (DuckDB, H2), EDGE-2's DateTime CSV, EDGE-3's `%r` newline, a checked pure/v1 raise, and host parity (the pure/v1 golden set through the wasm and native answer halves). 6. A per-push CI lane `edge_net` (in-process DuckDB, H2 and SQLite; the PCT SQLite recording in CI is the user's call per `gates/BUILD.bazel`). | About 1 day; one regeneration at the base commit. Proven by a planted SQL-only change (an alias rename) that the net passes, and a planted value change (a 17th digit) that it fails. |
| D2 | Keep the byte-identical SQL net. | Blocks the program (handoff §4.3). |
| D3 | No net, lanes only. | Too slow and too weak (lanes run behind). |

### (e) Import of nodes that run Java or Pure

**RULED (the user, 2026-11-18, relayed by the orchestrator: "go with your recommendations on all seven decisions"): E1.** E2 is not ruled. The FreeMarker conditional stays refused until the user rules on a product planned branch.

| Option | What | Cost |
|---|---|---|
| **E1 (recommended)** | Refuse by name every node with an `implementation`, every platform, graph-fetch, M2M, external-format, REST and error node, and the FreeMarker conditional. The refusal names the node's `_type` and the field. | Smallest. Done-when 5 measured over relational TDS and value plans. |
| E2 | Additionally, a route `importOrReplan(plan, function, model)` that plans the query again from its Pure source when the import is refused. The answer says "re-planned by lite". | Not an import: lite's SQL, not upstream's. Values identical, activity bytes differ. About +1 day. |
| E3 | Run generated Java. | Violates G1 and G4. Not offered. |

**FreeMarker at import:**
- `${var}` and each placeholder function on a closed list read from 4.145.0 become bound parameters;
- the temp-table IN protocol becomes a load;
- anything else is refused by name;
- a conditional node is refused until the user rules whether the product's runner gains a planned branch (an
  addition to A3's ratified list).

### (f) `EngineResultEnvelopes` beside `PureV1Writer`

**RULED (the user, 2026-11-18, relayed by the orchestrator: "go with your recommendations on all seven decisions"): retire it into a presented side written by `PureV1Writer`.**

The only caller is `FramePlanner.java:126-141` → `EngineExecutedValues.relation` → `JsonEmission.result` →
`EngineResultEnvelopes.tds` (the corpus's `executeLegendQuery` string entry; V-EDGE EDGE-8). **Recommendation: retire
it.** The string value becomes a presented side (`ENGINE_TDS_JSON`/`ENGINE_CLASS_JSON`/`ENGINE_VALUES_JSON`), written
by `PureV1Writer` over the typed rows in result order. That fixes EDGE-8's ordering and spacing too. The corpus rosters
are the check; a move is listed.

### (g) The plan format and the three hosts' readers

**RULED (the user, 2026-11-18, relayed by the orchestrator: "go with your recommendations on all seven decisions"): the two bumps, generated host constants, exact-version kept.**

- **Two bumps.**
  - Plan v59 lands with S1, and S2 adds `Operand.Returned` as part of the same version, before any host ships it.
    That can be done because the format is exact-version and no plan is stored.
  - Test plan v33 lands with S3.
- **The readers change in the same commit:**
  - Java: `PlanJson`, `TestPlanJson`;
  - TypeScript: `plan-runner.ts`, `declared-raises.ts`, `raised.ts`;
  - Python: `_runs.py`, `_raised.py`.
- **Generated constants.** `PLAN_VERSION` and lite's sentence table are generated for TypeScript and Python
  (`TypeFacts.java`, diff test).
- **Exact-version stays.** Exported and imported plans are legend-engine's own format (its `serializer.version`), so
  portability does not need lite's format to be versioned for outsiders.

---

## 5. The phased plan

**The order:** the safety net first; then presentation out of the plan; then the SQL builders become Java writers;
then export; then import.

**Every slice:** a shape note to the orchestrator before building (handoff §4.5). The local loop is the touched
packages' tests, `//core:guardrails`, `//core:census` and the net (S0 onward).

**The lanes that must stay identical, every slice:** the product's SQL pins (`lean_sql_baseline`, `query_capture`,
`lean_sql_corpus` on DuckDB and H2) and the product probe (`//core:product_raise_probe`). The exception is (a) A2.

| Slice | Shape | The check that proves it | Lanes identical | Size |
|---|---|---|---|---|
| **S0 The net** (d) | `PctCaseRecorder`, `EdgeNormalize`, `PureV1EdgeGoldenTest`, a new `edge_net` CI lane, regeneration at the base | The planted SQL-only change passes; the planted value change fails; the net is green at the base | everything (no product or edge code changes) | 1 day |
| **S1 Errors as data** (v59: presentation, sites without words, `Operand.Fixed`) | `Presentation`, `Site`/`Operand`, `PresenterFamily.errors`, `DataError.operands`, stateless `RaisedWords`, `HostAnswers.errorAnswer` + wasm/native exports, the generated sentence tables; recorder, `siteWords` and `SiteWords` deleted | The net's messages unchanged (pure/v1 error bodies, channel A `ERROR` lines, channel-B ledger messages); `RaisedErrorsTest`; host tests with a literal-operand raise (ARCH-1's input) on the tab and Python | PCT A and B, corpora, pure/v1, hosts | 1.5 days |
| **S2 Computed operands** (a) | the edge's `raiseMessage` writes carriers (A1); `Operand.Returned`; `PureMessages.sentence` deleted | The product probe's carrier cases per database for the operand types; a computed-operand raise test per edge database (DuckDB, H2, SQLite local; lanes by round) | as S1 + a lane round | 1 day |
| **S3 Test plan words** (c; test plan v33) | `TestPlan.presentation`, sizes and failures as kinds, `UpstreamVerdictWords` deleted, the edge's database judge on lite's words | channel B under BOTH and HOST unchanged verdict for verdict; messages unchanged in the net; judge-plan pins shrink | channel B (all lanes by round), corpora | 1 day |
| **S4 Relation text in Java** (b: M1; the biggest) | `Presented`, `edge.RelationText`, presented sides and roots, `RelationTextLowering` deleted; S131/S132 re-measured | EDGE-1, EDGE-2 and EDGE-3 tests (V-EDGE's) pass; the net's outcomes unchanged, except listed moves where the Java print is upstream's and the SQL print was not (each with upstream's source) | channel B (all lanes), corpora, pure/v1 | 2.5 days |
| **S5 The other prints** (b: R1-R4, M5) | `EdgePureOnlyTexts`, `PureListTexts`, `PureDateTimePrint`, `EdgeDateTimeTexts`, `TdsNullCells`, `joined`, `traceText` and `seedJson` → presenters; in-query rows to legend-engine's SQL or refusals | **First** the PCT moves per row (DuckDB, H2, SQLite A and B) to the user; then the build; the net's moves equal the ruled list exactly | PCT A and B (lane round), corpora | 1.5 days + the ruling |
| **S6 Envelopes** (f) | `EngineResultEnvelopes`, `EngineExecutedValues`, `oneBare` → `PureV1Writer` | corpus rosters unchanged; an ordered `executeLegendQuery` over 200,000 rows on DuckDB (EDGE-8's probe) | corpora | 1 day |
| **S7 The hook surface** (ARCH-4) | typed per-layer records; `PlanStyle` keeps `dialect`; `KINDS`/`TWO_FORMS` and their guards shrink; the §6 guard at its end state | javac exhaustiveness replaces the closed-list tests; `//core:one_plan_measure` reads only kind-B rows | everything byte-identical (a refactor) | 1.5 days |
| **S8 Export** | `edge.PlanExport`; FreeMarker parameters from the `SqlWriter`; refusals; `generatePlan` on it | **The engine runner** executes each exported plan of the pure/v1 golden set and a fixed corpus subset; its serializer's bytes equal lite's edge bytes (done-when 4) | pure/v1 | 2 days + runner setup |
| **S9 Import** | `edge.PlanImport`; node translation; FreeMarker resolution; refusals | **The engine runner** generates legend-engine's plans for the same queries and executes them; lite imports, runs and presents; bytes equal (done-when 5) | everything (import is new code only) | 3 days |

**In total:** about 16 working days of building, plus the rulings and two runner setups.

**Lane rounds** (the databases CI does not run) follow S2, S4, S5 and S6. A finding from a round comes back as one
batch. Only a regression stops the line (the plan of record's §5.1).

---

## 6. The guard for the end state

**The guard:** `core/src/test/java/com/legend/OnePlanGuardTest.java`, plus the `//core:one_plan_measure` target. It is
designed against the census's known failures:

- GUARDS-3: hand-listed vocabularies saw 6%;
- EDGE-4: whole-file counts;
- GUARDS-3: items closed by reading;
- COMPLETENESS-10: hosts unscanned;
- rules that match nothing.

| Rule | What it checks | How the vocabulary is **derived** | Both ways |
|---|---|---|---|
| **G1 No upstream text built as SQL** | Every method in `com.legend.edge..` that constructs a text-producing MIR node (bytecode scan through the ArchUnit classes `ArchitectureTest` already imports) is a row of `KIND_B` (`Class#method` → register id, ruling date). Anything else fails, naming the method. | Text-producing MIR is derived: every `SqlFn` whose `SqlTyping.callType` (`SqlTyping.java:333`) is VARCHAR or JSON for a typed probe argument list, plus `StringLit`, `JsonObject`, `JsonArray`, `JsonArrayAgg`, and `Cast` to VARCHAR/JSON. A new text function joins without an edit. | Every `KIND_B` row must match at least one method ("a row that sees nothing is deleted"); every match must have a row |
| **G2 No words in a plan** | Every plan the suites make (PCT A and B, corpora, pure/v1; both styles) is serialized, and no JSON string in it contains a sentence | Derived by calling every presenter family with sentinel operands: `ErrorKind.sentence()`, `PureMessages`' words per kind, the engine family's, both `HostWords` families. The vocabulary is the presenters' own output, never a list. | Every vocabulary entry must be produced by a presenter on the current classpath (no stale entries) |
| **G3 One plan** | Each case is planned under `PlanStyle.PRODUCT` and under the edge with every `KIND_B` form switched to the product's form ("edge minus B"); every statement text must be equal. On the differences of the real edge plan, each `KIND_B` form is enabled alone, and each difference must be attributed to exactly one row. | The cases are the suites' own | Unattributed = fail; a row attributed nowhere for 30 days is a candidate for deletion, reported |
| **G4 Hosts present nothing** | `engine-client/src/**` and `python/legend_lite/**` contain no entry of G2's vocabulary except the generated files, and no reading of a site's words (`site.message`, `['message']`) | The vocabulary is G2's, exported by `TypeFacts.java` as a test fixture | The fixture is regenerated by the diff test; a fixture entry no Java presenter writes fails |
| **G5 Writers evaluate nothing** | `PresenterFamily` implementations and `com.legend.result..` depend on no `compiler.spec..`, no `TypedSpec`, no `lowering..` (G1: no second evaluator) | ArchUnit packages | n/a |

**Shrink-only:**
- `KIND_B` loses rows freely;
- it gains one only with a dated line naming the user's ruling (`tools/guards/shrink_only.py` reads it as today);
- the old `CompatSpellingGuardTest` lists are deleted when G2 and G4 land (S7), since their derived vocabulary
  supersedes the 54 hand-listed literals.

---

## 7. Risks and how each is caught

| Risk | Caught by |
|---|---|
| A Java print differs from the SQL print where S132's leniency hid it, so a verdict flips | S0's messages and outcomes; every flip is listed with upstream's source before landing (G7: no silent leniency to absorb it) |
| A second evaluator creeps into the writers (formatting grows into computing) | G5; presenter signatures take typed values only; a `format` with a computed format string is refused (§3.2 rule 3) |
| The rule of §3.2 misclassifies a print inside a query | channel A is the referee (its boundary is physical): any channel-A move in S4 or S5 is a misclassification, fixed before landing |
| The hosts drift from Java | G4; the generated tables and version constant with diff tests; host parity in the net (S0) |
| Format churn across the three readers | two bumps only; the generated `PLAN_VERSION`; one commit per bump |
| A carrier is not exact on a database (A1) | the product probe's carrier case per database, extended to each operand type a site declares (S2) |
| Databases CI does not run | lane rounds after S2, S4, S5 and S6, compared case by case with the previous round |
| Upstream's answer guessed | §8: every slice names its source in the commit message and the register row; no ruling on a recalled answer |
| Export and import scope creep | the node tables of §3.9 and §3.10 are closed; anything else is refused by name and counted |
| Performance: large relations printed in Java | measured: corpus and channel-B wall time per lane before and after S4; Java writes linearly and drops the `STRING_AGG` text from the database |
| The net's own blind spots | S0's planted changes, rerun on every change to the net |
| ARCH-4's refactor touches many files | it comes last (S7), after most kinds are deleted, and is byte-identical by construction |
| Long sessions and default effort (handoff §4.1, §4.7) | one worker per slice, a written handoff per slice, effort set explicitly at creation |

---

## 8. How fidelity is verified: reading upstream, never recalling it

**What this container has (checked 2026-11-18):**
- no legend-engine or legend-pure checkout;
- no `-Dlegend.engine.root` outside Bazel (`spec/BUILD.bazel:263-309` passes `@legend_engine_src` and
  `@legend_pure_src` from `release.MODULE.bazel:262-271`);
- **no Bazel binary**, so neither those repositories nor the engine runner (`tools/engine-runner`, about 600 MB of
  jars, `docs/result-boundary-homework/README.md` §7's recipe) is available as is.

**What it can do:**
- `git ls-remote` reaches the public tags (legend-engine-4.145.0 = `230c159196d6512486fd382556c5e9e4fb128ebb`,
  legend-pure-5.99.0 = `7fbc7d6e8d52e2488bdae67280e5f5dfed448d68`, the census's pins);
- raw.githubusercontent.com answers 200;
- a blob-less clone at the tag works. This design read `h2Extension2_1_214.pure`, `pureToSQLQuery.pure` and the
  protocol's node classes that way.

**Per slice:**

| Slice | Upstream's answer | Read how |
|---|---|---|
| S0 | none (the net records lite at the base) | n/a |
| S1, S2 | legend-pure's sentences per kind (`CompiledSupport.java:1483-1485, 1588`, `CoreHelper.java:520, 544`, `Rem.java:64`, the `*.pure` tests the census cites); pure/v1's answers for classes C, D and E (F43/F44) | the pinned sources by `git show <tag>:<path>` from a blob-less clone (any session); pure/v1 by **the engine runner** (a session with Bazel: a lane session, or a fresh one using `tools/cloud-distdir.sh` and `tools/cloud-maven-distdir.sh` per `docs/CLOUD_SESSIONS.md`) |
| S3 | legend-pure's assert words (`asserts/*.pure` at 5.99.0); JUDGES-5's relabelled sentences checked against them | pinned sources |
| S4 | `relation/functions/toString.pure`, `s.pure`, `helperFunctions.pure:203-243`, `dateExtension.pure:384-387`, `CompiledSupport.java:132-137, 1271-1273` | pinned sources (the audit's reads, re-read at the tag); channel A as referee |
| S5 | per function: its `PureFunctionToRelationalFunctionPair` in `pureToSQLQuery.pure` and its `dynaFnToSql` per database extension; the 77 PCT manifests for legend-engine's own expected failures | pinned sources; the moves measured on PCT before the ruling |
| S6 | `RelationalResultToJsonDefaultSerializer`, the JsonBuilder rule | pinned sources, then **the engine runner** for the bytes |
| S8, S9 | legend-engine's executor and serializer on the exported or upstream plan | **the engine runner**, mandatory: no recorded or recalled bytes |

**Recorded answers.** Every answer read from the engine runner is recorded under `core/src/test/resources/upstream-api/`
with its model and provenance, as §7 of the homework README does. Every divergence kept is posted to the user before it
lands and becomes a register row (the upstream-fidelity tenet).

---

## 9. Measurement commands (at `68f20eb4d`)

- **SQL code per file:** a brace-matching counter over comment- and string-stripped Java. It counts methods by
  declared return type and is kept in the designer's scratchpad. Rerun:
  `python3 methods.py core/src/main/java/com/legend/edge/{RelationTextLowering,PureMessages,…}.java`.
- **Construction sites:** `grep -c 'StringLit(\|SqlFn\.CONCAT\|STRING_AGG\|SqlFn\.' <file>` per edge file.
- **Golden activities and masks:**
  - `grep -c 'activities=' pct/src/test/resources/edge/*_answers.tsv`;
  - `grep $'\tVALUE\t' <file> | grep -o '<id>' | wc -l`;
  - `grep -c 'String:#TDS' pct/src/test/resources/edge/duckdb_*_answers.tsv` (0 in every file).
- **Answer kinds:** `grep -c $'\tRELATION\t'`, `$'\tVALUE\t'` and `$'\tERROR\t'` per answers file.
- **Hook surface:**
  - `PlanStyleFacets.java:24-77`;
  - `PureText.java` overrides by name (`grep -nE 'protected|public' … | awk`);
  - `LoweringForms.java:45-104`.
- **Upstream:**
  - `git clone --filter=blob:none --no-checkout --depth 1 --branch legend-engine-4.145.0 https://github.com/finos/legend-engine`;
  - `git ls-tree -r --name-only HEAD | grep executionPlan/nodes/`;
  - `git show HEAD:<path>/h2Extension2_1_214.pure | grep dynaFnToSql`;
  - `git show HEAD:<path>/pureToSQLQuery.pure | grep -c 'PureFunctionToRelationalFunctionPair(first'` (451).
