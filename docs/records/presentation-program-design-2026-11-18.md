# Three modes, one plan each: the presentation program's design (2026-11-18, reframed)

**Status.**
- **Reframed as three modes, LITE, ENGINE and PURE**, by the user's ruling of 2026-11-18, about 19:45 UTC (§0).
- **Some decisions are pending.** They are marked **PENDING THE USER**; the rest stay **RULED** (§0's table). §11 says
  what the reframe changed from the previous revision.
- **Nobody builds before PR #118 merges.**

**Revisions:**
- `75e3fbcd1`: the draft.
- `079c343d3`: the 16:30 ruling recorded.
- `000008e11`: the xhigh review applied.
- This commit: reframed as three modes.

The design is of the approved program `docs/records/edge-portability-program-2026-11-18.md` (on
`claude/total-wall-orch`; "the program" below). That program starts once PR #118 (`claude/total-wall-d-stage` →
`main`, head `68f20eb4d`) has merged and its fix round (F1-F8, `claude/pr118-fixes`) has landed.

**Inputs read:**
- AGENTS.md at the PR head.
- The program.
- The plan of record `docs/records/total-wall-edge-program-2026-11-16.md`: §1-§11 and the §12 log, including the
  error design of 2026-11-16 21:20 and the judge decision of 2026-11-17 02:30.
- `docs/TENET_CHARTER.md`, Clause 7's A3.
- `docs/GRAMMAR_LEVELS.md` and `core/src/main/java/com/legend/parser/GrammarLevel.java`.
- The PR #118 audit, and the xhigh review of this design (`docs/records/presentation-design-review-2026-11-18.md` on
  `claude/refactor-code-quality-review-9fqhug`).
- The code at `68f20eb4d`.
- Upstream, read through blob-less clones at the pinned tags (§8):
  - legend-engine 4.145.0 (`230c1591`): `pureToSQLQuery.pure`, `h2Extension2_1_214.pure`, `duckdbExtension.pure`, the
    protocol's plan nodes, and the PCT manifests for DuckDB and H2. It has manifests for 11 databases.
  - legend-pure 5.99.0 (`7fbc7d6e`), through the review's reads.

**Measurements:** every number in §2 was measured at `68f20eb4d`, by this design or by the review where it says so. The
commands are in §9.

---

## 0. Rulings, and what is pending

**RULED by the user, 2026-11-18, about 19:45 UTC, relayed by the orchestrator (`session_0195rQEoRb1AGCHxtZChjWsg`),
verbatim:**
- "Yes reframe as 3 modes"
- "Only runtime/text not constructs for now"
- "Planning mode/direct entry for now"

**The context, as relayed by the orchestrator:**
- Some use cases and users use legend-pure directly.
- The parser already has three grammar levels (`GrammarLevel`: `LEGEND_PLATFORM`, `LEGEND_LITE`, `LEGEND_ENGINE`).
- The three modes the user said yes to are the orchestrator's proposal. They are defined in §3.1.

**RULED by the user earlier, 2026-11-18, about 16:30 UTC, relayed by the orchestrator, verbatim:** "go with your
recommendations on all seven decisions". The reframe re-cuts several of those decisions per mode, as the table below
shows.

| # | Decision | Status | Where it stands |
|---|---|---|---|
| — | **Three modes**: one axis, chosen once at the door, carried through parse, plan and presentation, written into the plan as data | **RULED** (19:45) | §3.1-§3.3 |
| (a) | Raise operands | **PENDING THE USER**, reframed | LITE needs none (the 2026-11-10 ruling). ENGINE needs none, because the database raises where legend-engine plants no check (B7). **PURE needs computed operands.** The design is a **planned diagnostic read-back** (§3.13): on a raise, exec sends a statement the plan carries for that site, which selects the guard's operands, typed, as a row. **It extends the charter's A3 list**, which needs the user's ruling. **Recommendation:** yes, in **phase 2**. In phase 1, PURE words a computed-operand site with legend-pure's sentence without the value, as a register row; no measured case needs more (D's census). |
| (b) | The kind-B rows | **RULED**, re-cut per mode | Each row now has **one form per mode** (§4.b's matrix). **R1 and H6 need no either/or ruling any more:** PURE gives legend-pure's print and answers `format`; ENGINE gives legend-engine's cast and refuses `format`. Rows whose PURE or ENGINE form is still open are measured in S5a against that mode's oracle: PCT's expected values, or legend-engine's manifests. |
| (c) | The test plan's words; the database judge | **PENDING THE USER** (unchanged) | **Recommendation: C4**, the 2026-11-17 02:30 decision: port the 36 combos into core, PCT HOST-only, delete the compatibility database judge |
| (d) | The safety net | **RULED** (D1) | One addition is **PENDING THE USER**, as (d′) below: what runs per push in CI |
| (e) | Import | **RULED** (E1) | Import and export are ENGINE mode |
| (e′) | ENGINE's scope for legend-engine's refusals and defects | **PENDING THE USER** (new) | **Recommendation: E-R1.** ENGINE refuses at planning exactly the functions legend-engine has no SQL translation for, in its words, using a table derived from its manifests. Where legend-engine fails by its own defect, ENGINE answers correctly, and each such case is a register row. |
| (f) | `EngineResultEnvelopes` | **RULED** (retire) | ENGINE's writer |
| (g) | Plan format | **RULED**, one fact corrected | A bump for each format-changing slice, after S1 generates `PLAN_VERSION` for the hosts |
| (h) | Lite's sentence in the product plan | **PENDING THE USER**, recommendation **flips to K2** | One error kind now has three wordings, and PURE's computed-operand words can only be written after the run. One mechanism serves all three: **the plan carries facts (kind, where, operands), and each mode's writer words them.** K1 (words rendered at planning) would need a second mechanism for PURE. |

**Design determinations, listed for the user's note** (they touch no prior ruling):
- **PURE parses at `LEGEND_LITE`.** legend-pure code uses function types and generics, which `LEGEND_ENGINE` refuses
  and `LEGEND_LITE` declares (the function-types-generics family). `LEGEND_PLATFORM` stays closed to users. The PCT
  channel-B harness keeps its whitelisted `LEGEND_PLATFORM` parse of the PCT sources, as a test-harness exception
  (`ChannelB.java:140-142`).
- **ENGINE parses at `LEGEND_ENGINE`; LITE at `LEGEND_LITE`.**
- **Which mode each door runs:**

  | Door | Mode |
  |---|---|
  | lite's routes | LITE |
  | pure/v1, the hosts' edge routes, the corpus, plan export and import | ENGINE |
  | PCT channel A | PURE and ENGINE, two lanes |
  | PCT channel B | PURE only |

  ENGINE needs the query boundary, which channel B erases, so running ENGINE only where the boundary is physical
  removes the boundary-marker question altogether.
- **PURE's direct entry** is one Java door, `com.legend.edge.PureEntry` (§3.12). There is no server route and no
  Python or TypeScript entry.
- The review's corrections stand (§10), re-cut per mode where needed (§11).

**Pending the user, one screen:**

| Item | The choice | Recommendation | Why |
|---|---|---|---|
| (a) | PURE's computed operands: **RB, a planned read-back (extends charter A3)** / no value, permanently (a register row) / carriers in the raise text (rejected by the review: unmeasured transport) | **RB, in phase 2**; the interim register row in phase 1 | typed rows, no text parsing, exec sends only planned statements; 0 measured cases, so not urgent |
| (h) | **K2: facts in the plan, words in each mode's writer** / K1: words rendered at planning | **K2** | three wordings per kind, and PURE's need the value; one mechanism. The product plan JSON changes once, in S1 (dated) |
| (c) | **C4: PCT HOST-only (02:30)** / C1 interim | **C4** | already DECIDED in the plan of record; 0 signal on PCT |
| (d′) | per-push CI: **add ENGINE channel A (DuckDB's 5 suites and H2's relation suite) and the SQLite PCT recording** / keep them in rounds | **add both** | about +2 to +4 minutes per push (§8); the modes' fidelity is checked on every push |
| (e′) | ENGINE: **E-R1, refuse what legend-engine refuses (derived from its manifests); answer where it fails by defect (register rows)** / E-R2, answer everything lite can | **E-R1** | ENGINE's lane then compares exactly against legend-engine's own manifests |

---

## 1. The goal and "done when", by phase

**The goal, in the user's words** (2026-11-18 05:00 and 05:15, verbatim in the program's §1): "mostly everything that
could be would really be at the edge edge mostly flexing the product and then trying to fix up later in java instead
of parallel tracks for product and compatibility". And: "our edge should really be plans that don't do a lot of the
stuff in SQL", so that lite can "import upstream plans to run on our platform and export plans from us to to be able
to run in upstream". Then, at 19:45: "Yes reframe as 3 modes".

**So the program is one compiler, one planner and one runner, with three modes.** A mode decides only:
- which grammar the door parses;
- the short list of in-query computations that differ from lite's own (kind B);
- which writers present the answer, the errors and the verdicts.

### Phase 1: modes and presentation (S0-S7, about 25 working days)

Done when:

1. **Modes are data.**
   - Three `Mode` records exist (§3.1): LITE, ENGINE, PURE.
   - Every door chooses exactly one, and its name is in the plan.
   - The parse level, the kind-B forms and the presenter family come from the record.
   - §6 G6 is green: no code branches on a mode outside the records.
   - Today's route-keyed styles are gone: `PlanStyle.PRODUCT`, `PureText.UPSTREAM` and `PureText.PURE_V1`.
2. **One plan per mode.** A LITE statement is the product's statement. An ENGINE or PURE statement is LITE's statement
   for the same expression, except where one of that mode's ruled kind-B rows changes it. `//core:one_plan_measure`
   (heavy lane, §6 G3) attributes every difference to exactly one row of that mode.
3. **No upstream word in a product record.**
   - Under K2, no sentence is in any plan, lite's included.
   - Each mode's writer words the facts.
   - Checked by §6 G2 over every plan the suites make, in every mode.
4. **No edge class builds upstream text as SQL** except each mode's ledgered rows (§6 G1).
   - PURE's in-query print forms (legend-pure's print computed in SQL where the text feeds a further computation) are
     PURE rows.
   - Presentation at a root or a side is always Java.
5. **Fidelity per mode** (§8):
   - **PURE:** PCT channels A and B match PCT's expected values, except PURE's ruled rows.
   - **ENGINE:** PCT channel A matches legend-engine's own manifests on the 7 databases both cover (ClickHouse,
     Databricks, DuckDB, H2, Postgres, Snowflake, Trino); the corpus matches its rosters; pure/v1 matches the net.
   - **LITE:** the product's own suites are unchanged.
6. **PURE's direct entry works.** `PureEntry` plans and runs a lambda over a model in PURE mode, and is tested on
   DuckDB, H2 and SQLite (§3.12).
7. **The product is unchanged except as stated.**
   - The product's SQL is byte-identical.
   - Under K2, the product plan's JSON changes once, in S1: lite's sentences and the `raises` sentences leave, with a
     dated note on the `lean_sql_*` plan pins.
8. **Lean.**
   - Edge SQL characters shrink.
   - The judge-plan pins only shrink.
   - Under C4, the compatibility database judge's plans are gone.

### Phase 2: PURE's read-back and portability (RB, R, S8, S9, about 13 working days)

Done when:

9. **PURE's read-back (if ruled).**
   - Every PURE site whose legend-pure sentence prints a computed operand has a planned read-back (§3.13).
   - A raise of such a site answers legend-pure's sentence with the operand's value, measured on DuckDB, H2 and SQLite
     and by a lane round.
   - The phase-1 interim register row is deleted.
10. **Export (ENGINE).** A lite ENGINE plan, exported as legend-engine's `SingleExecutionPlan` and executed by
    legend-engine 4.145.0's `PlanExecutor` (the engine runner, after slice R), gives answer bytes identical to lite's
    ENGINE answer.
    - The set measured: the pure/v1 golden requests plus a fixed set of parameterized corpus service queries.
    - Activities are compared by §3.9's rule, with the trace id masked.
    - Refusals are by name, listed and counted.
11. **Import (ENGINE).**
    - legend-engine's own plans for the same queries are listed by node type first (S9's census).
    - Every plan whose nodes map imports, and lite runs and presents it with answer bytes identical to legend-engine
      executing it.
    - The refused share is stated, with its node types.

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

The *Class* column predates the three modes; §4.b's matrix re-cuts every row per mode. For example, the Float cast is
ENGINE's form, while PURE keeps legend-pure's print in SQL where the text feeds a computation.

**What leaves:** about **920** SQL code lines are deleted. That is the ≈ 1,090 less PureText's wiring for the KEEP rows
and PurePrintForms' ≈ 30 KEEP lines. PureMessages' 228-line Java half stays and becomes the PURE family's
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
| `PlanStyle.siteWords` | a stateful recorder | presentation | deleted: under (h) K2 each mode's writer words the site's facts (§3.5) |
| `PlanStyle.errorWords` | Java words at planning | presentation (a planned `Failure`'s message) | replaced by kinds (§3.7) |
| `PlanStyle.traceCell` | constants into SQL | presentation | the writer |
| `PlanStyle.graphKeyDate`/`graphKeyString` | a literal at planning, inside the database-built graph JSON | kind C (row C2) | stays, listed |

### 2.3 The plan fields that carry presentation

| Format | Field | Today | Target |
|---|---|---|---|
| plan v58 (`PlanJson.java:171`) | top-level `raises` (kind → lite's sentence, `:241`, `:255-264`) | lite's sentence | **(h) K2 (recommended): kinds only.** K1: kept |
| plan v58 | `sites[].message` (`:1405-1442`; `ExecutionPlan.Site:843-870`) | lite's sentence, checked on read | **K2: dropped**; `operands` where the mode carries them (PURE), `readBack` in phase 2. K1: kept |
| plan v58 | `Column.precision` | a fact | kept: both writers read it |
| host plan halves (`HostAnswers.arrowPlan:179`, `jsonPlan:234`) | `siteWords` (`:212`, `:279`) | legend-pure's words, **read by no host** (ARCH-1) | deleted. The words move into the plan's edge extension, and the hosts' edge routes use the Java error half (§3.8) |
| test plan v32 (`TestPlanJson.java:60`) | `TestPlan.words` | a name (`"lite"` or the edge's) | becomes `mode`, the same name space as the product plan's `mode` |
| test plan v32 | `answerSizeWords`, `SideShape.sizeWords` | a sentence worded at planning | dropped. The hearing words it from `SideShape.multiplicity`/`lower` |
| test plan v32 | a planned `Failure.message` (`AssertErrorNative:76-82` via `PlanStyle.errorWords`) | upstream's sentence at planning | `Failure(kind, operands, message)`, where `message` is lite's; the hearing words upstream's |
| test plan v32 | `SiteWords` extension | edge data, produced by the stateful recorder | deleted under K2 (the writers word the facts) |
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

### 2.7 Today's route-keyed styles and grammar levels

| Today | Used by | Grammar level | Becomes |
|---|---|---|---|
| `PlanStyle.PRODUCT` | lite's doors | `LEGEND_LITE` (`Compiler.java:39, 46, 87, 111, 176, 302`; `door/Parse.java:56, 79`) | **LITE** |
| `PureText.UPSTREAM` | PCT channel A (`PctPlanRoute.java:50`, `PctExecuteNative`), channel B, the corpus | channel B's PCT sources at `LEGEND_PLATFORM` (`ChannelB.java:140-142`, the harness exception); channel A's model at `LEGEND_LITE` | **split:** PURE (PCT) and ENGINE (the corpus; PCT channel A's ENGINE lane) |
| `PureText.PURE_V1` | pure/v1 (`PureV1Routes`, `EdgeServer.runner`), the hosts' edge routes (`HostAnswers`) | `LEGEND_ENGINE` (`PmcdParser.java:47`; `SpecParser.java:319, 338`) | **ENGINE** |

**Today's `UPSTREAM` mixes the two upstream meanings:**
- Row 8 (legend-engine's Float cast) and row 21 (the DECIMAL literal) are legend-engine's computations. PCT uses them,
  yet PCT's expected values are legend-pure's.
- The DateTime print composed in SQL (`PureDateTimePrint`) is legend-pure's.

The split puts each form in its mode (§4.b).

**Code that branches on a "mode" today.** G6 (§6) catches all four, and each goes:
- `PureText.isEdgeStyle(style)`: `TestRunner` refuses a mixed run by it.
- `PlanStyle.judgesEngineText()`: an "is this the edge" flag (JUDGES-6).
- `PureText.recording(template)`'s template check.
- `Boolean.getBoolean("legend.exec.engineScanOrder")` (`PureText.java:368`): a JVM-wide switch (ARCH-9).

---

## 3. The target design

### 3.1 The three modes

| | **LITE** | **ENGINE** | **PURE** |
|---|---|---|---|
| For | lite's users | legend-engine's clients: Studio, services, pure/v1, plan import and export | users of legend-pure directly, and PCT's reference |
| Meaning | lite's: its SQL, its typed forms | exactly what legend-engine does end to end: its SQL where it pushes down (the database raises where legend-engine plants no check); legend-pure's runtime forms for what it does after the query | legend-pure's runtime meaning and text, **even inside the query**: its print forms in an in-query `toString`, its checks and their sentences, `format` answered |
| Grammar | `LEGEND_LITE` | `LEGEND_ENGINE` | `LEGEND_LITE` (§0) |
| Computed by | the database | the database | **the database**: no interpreter, no second evaluator (G1, G6); runtime and text only, no new constructs (the ruling) |
| Oracle | lite's register | legend-engine 4.145.0's sources, its 77 relational manifests, the engine runner | legend-pure 5.99.0's sources and PCT's expected values |
| Doors | lite's routes | pure/v1, the hosts' edge routes, the corpus, PCT channel A's ENGINE lane, export, import | PCT channels A and B, `PureEntry` (§3.12) |

**A mode is a record, data only** (§6 G6 keeps it so):

```java
// com.legend.mode (product): the record and LITE's instance; the edge holds ENGINE's and PURE's (G5: the edge depends
// on the product). Every difference between modes is a typed component, so javac checks each mode has every form.
public record Mode(
        String name,                       // "lite", "legend-engine", "legend-pure"; written into the plan
        com.legend.parser.GrammarLevel grammar,
        Computation computation,           // the dialects and the kind-B forms: what PlanStyle and its hooks hold today
        com.legend.result.PresenterFamily presentation,
        boolean siteOperands) {            // whether a site carries its operands (PURE: yes; LITE: never, the
                                           // 2026-11-10 ruling; ENGINE: no, the database raises)
    public static final Mode LITE = new Mode("lite", GrammarLevel.LEGEND_LITE, Computation.PRODUCT,
            PresenterFamily.LITE, false);
}

/** The computation forms a mode chooses, each a typed component (ARCH-4: the hook surface as records, not class
 *  tokens). The members today's PlanStyle, LoweringForms and the two-form kinds carry. */
public record Computation(java.util.function.Function<DatabaseType, SqlDialect> dialects, LoweringForms lowering,
        DatabaseSettingsForm settings, ScanOrder scanOrder, JudgeRules judging /* kind D */) { ... }
```

```java
// com.legend.edge (the compatibility edge)
public final class Modes {
    public static final Mode ENGINE = new Mode("legend-engine", GrammarLevel.LEGEND_ENGINE, EngineComputation.FORMS,
            Families.ENGINE, false);
    public static final Mode PURE = new Mode("legend-pure", GrammarLevel.LEGEND_LITE, PureComputation.FORMS,
            Families.PURE, true);
}
```

**What the mode is carried through:**
- **Parse:** the door's parse uses `mode.grammar()`. `Compiler` gains `compileModel(String, Mode)` and
  `query(ctx, lambda, Mode)` beside today's LITE overloads.
- **Plan:** the planner takes `mode.computation()`, where today it takes a `PlanStyle`.
- **Presentation:** the run takes `mode.presentation()` beside the caller's `Output`. A plan whose `mode` name differs
  from the offered family's is refused before any statement.

### 3.2 The rule per mode: computation or presentation

**LITE.** The product, unchanged. Text functions are the product's: S311, S312, S313-S316. Relation text is refused
(S316b).

**ENGINE.** For each text-producing function:
1. **legend-engine pushes it to SQL** (kind 1: a `PureFunctionToRelationalFunctionPair` in `pureToSQLQuery.pure` and a
   `dynaFnToSql` in the database's extension). Inside the query, ENGINE writes **LITE's SQL unless a measured answer
   differs from the answer of legend-engine's SQL; then legend-engine's SQL**, as an ENGINE kind-B row (H7).
   - The kind-1 text functions: `toString_Any_1`, `formatDate`, `joinStrings`, `toJson`, `parseJson` (§4.b).
   - On a database legend-engine has no extension for, ENGINE writes LITE's form (H5) and has no PCT lane (§8).
2. **legend-engine does not push it to SQL** (kind 2: relation `toString`/`toCSV`/`csv`, `format`, `makeString`,
   `toRepresentation`, an instance's id).
   - **At the query's root:** refused at planning in legend-engine's words ("No SQL translation exists for the PURE
     function '…'"), under (e′) E-R1.
   - **After the query** (a corpus test body around `execute`, pure/v1's answer envelope): presentation. A writer
     prints the typed rows in Java with legend-pure's print, as legend-engine's test runner and serializers do.
3. **Checks:** none where legend-engine plants none (census class A: the database raises). Class B is refused at
   planning in legend-engine's words (F43). Classes C and D raise their constant sentences.

**PURE.** legend-pure's meaning, computed by the database:
1. **A print or check inside the query** (its text feeds a further computation) is legend-pure's form, computed in
   SQL:
   - legend-pure's Float print (`FLOAT_SHORTEST_TEXT`);
   - its DateTime print;
   - `[a, b]`;
   - `format` with `%r` and padding;
   - every check, with legend-pure's sentence (§3.5).

   These are **PURE kind-B rows**, ledgered (§6 G1), each citing legend-pure 5.99.0 and its PCT cases.
2. **A print at the answer's root or an assert's side** is presentation: a writer prints the typed rows in Java with
   legend-pure's print. The SQL returns the operand, which is LITE's statement.
3. **No interpreter (G1, G6).** A presentable root or side is one of a **closed list**, each one Java port of one
   legend-pure native over SQL-computed typed operands:
   - relation `toString`, `toCSV`, `csv`;
   - `format` with a constant format string;
   - `toRepresentation`;
   - `makeString` over a typed list of primitives;
   - an instance's `@_` id.

   **Nesting.** A nested print is presented only where the outer native's own rule takes the inner text. Any other
   nesting, a computed format string, and a relation's text feeding a computation are refused by name.

**The query boundary.** Only ENGINE needs it, because its in-query and after-query forms differ, and ENGINE runs only
where the boundary is physical: PCT channel A's eval, the corpus's `execute`, pure/v1's request. PURE's in-query and
after-query forms are both legend-pure's, so channel B's erased eval changes nothing. **No marker is needed.**

### 3.3 The records (plan v59 in S1; later slices bump per §4.g)

`Extension` and `ExtensionCodec` move down from `testexecutionplan` to `executionplan` (H4).

**The codec is passed in explicitly**, because a product plan's extensions are also read in the TeaVM and native edge
modules. `PlanJson.read(text, ExtensionCodec codec)`:
- product callers pass `ExtensionCodec.NONE`, which refuses any extension by name;
- the edge's roots pass `EdgeCodec`.

```java
// com.legend.executionplan (product)
// ExecutionPlan gains:
//   String mode                              -- the mode's name; omitted in JSON when "lite"
//   Map<String, List<Extension>> extensions  -- edge data ("root", "statement:<i>"); omitted when empty
// The top-level `raises` becomes a sorted list of kind names (K2: no sentence).
public record Site(int n, RaiseKind kind, @Nullable String where, List<Operand> operands,
        @Nullable ReadBack readBack) { }        // operands only where the plan's mode carries them (Mode.siteOperands);
                                                // readBack: phase 2, PURE only (§3.13)
public sealed interface Operand permits Operand.Fixed, Operand.Read {
    record Fixed(String type, String text) implements Operand { }   // a literal known at planning
    record Read(String type, int column) implements Operand { }     // column i of the site's read-back row (§3.13)
}
```

The edge's own record is `edge.Presented(Form form, boolean sorted) implements Extension`, on a root or a side. Its
forms: `PURE_TDS_TEXT`, `PURE_CSV`, `PURE_TDS_CSV`, `PURE_FORMAT`, `PURE_REPRESENTATION`, `PURE_MAKE_STRING`,
`PURE_INSTANCE_ID`, `ENGINE_TDS_JSON`, `ENGINE_CLASS_JSON`, `ENGINE_VALUES_JSON`.

**Test plan v33:**
- `words` becomes `mode` (the same name space);
- `answerSizeWords` and `SideShape.sizeWords` are dropped;
- `Failure` becomes `(kind?, operands, message)`, where `message` is LITE's;
- the `SiteWords` extension is deleted (K2).

**The separation lens** (`EdgeOnlyDataTest`) holds `mode` and `extensions` as product facts with no upstream meaning:
- the product compares `mode` with the offered family's name, and never branches on it (G6);
- it never reads `extensions`.

### 3.4 The presenter families, passed in explicitly

There is no `ServiceLoader` for the families (review item 12). A door hands its mode, and so its family, to the run.

```java
// com.legend.result (product)
public record PresenterFamily(String mode, ErrorWords errors, ValueTexts values, AnswerWriters answers) {
    public static final PresenterFamily LITE = ...;    // CellText, the product's writers, ErrorKind's sentences
}
public interface ErrorWords    { String words(ErrorKind kind, @Nullable String where, List<Object> operands); }
public interface ValueTexts    { String print(@Nullable Object v, String pureType, ExecutionPlan.@Nullable Precision p);
                                 String presented(Extension form, List<ExecutionPlan.Column> cols, List<List<Object>> rows); }
public interface AnswerWriters { ResultSink writer(Output output, ExecutionPlan plan, java.util.@Nullable UUID execution); }
```

| Family | `errors` | `values` | `answers` |
|---|---|---|---|
| `LITE` | `ErrorKind.sentence()` + where | `CellText`; refuses `presented` (S316b) | the product's writers |
| `edge.Families.ENGINE` | legend-engine's pure/v1 words (census classes C, D, E; F43/F44); class A is the database's text | `Presenter.engine` for JSON tokens; legend-pure's print (`Presenter.legendPure`, `edge.RelationText`) for after-query text | `EngineAnswers.engine`/`.arrow`, `PureV1Writer` with the JsonBuilder rule, the `ENGINE_*` presented forms |
| `edge.Families.PURE` | legend-pure's sentences (`PureMessages.wordsOrNull` over typed operands: fixed now, read back in phase 2); `PureMessages.compile` for compile kinds | `Presenter.legendPure`, `edge.RelationText`, the `format`/`%r`/`toRepresentation` ports | typed rows to the caller's `Output`; PCT's harness transport (`EngineAnswers.engine`: the PCT adapter reads legend-engine's JSON) |

### 3.5 Raises per mode (K2)

1. **Planning.**
   - Every mode writes the token `LG#<n>` and a site `(n, kind, where, operands?, readBack?)`. There are no sentences
     (K2).
   - `PURE` writes `Operand.Fixed` for literal operands; under the phase-1 interim, a computed operand leaves its
     sentence to the interim row.
   - `ENGINE` writes no site where legend-engine plants no check: a plain operation, B7.
   - `PureMessages.sentence`, `SiteWordsRecorder`, `PureText.recording` and `PlanStyle.siteWords` are deleted.
2. **Run.**
   - Exec reads the token and looks up the site. It does no parsing and no formatting; the 21:20 design holds.
   - In phase 2, a PURE site with a read-back gets its planned read-back sent (§3.13).
3. **The boundary.** The door's family words the site from `kind`, `where` and the typed operands:
   - LITE: lite's sentence;
   - ENGINE: legend-engine's words;
   - PURE: legend-pure's sentence.

   `RaisedWords` becomes this one stateless call.
4. **The interim (phase 1).** A PURE site whose legend-pure sentence needs a computed operand is worded with
   legend-pure's sentence without the value. That is a register row (0 measured cases), deleted by RB.

### 3.6 Presented answers and presented sides

- **A root.** A query whose root is a presentable print plans its operand's typed result with `Presented` on
  `"root"`, and the writer presents it. PURE presents every presentable root; ENGINE refuses a kind-2 root (§3.2).
- **A side.** A test body's side whose value is a presentable print over a query result is a TABULAR side with
  `Presented`.
  - The **host judge** reads the rows, presents them, and compares the strings by the `RenderedTexts` rule. S131 and
    S132 are re-measured in S4.
  - Under C4 no compatibility database judge exists to decline it.
- **EDGE-1, EDGE-2, EDGE-3 and EDGE-8 go by construction**, in both upstream modes:
  - a Float cell prints by `DecimalFormat("0.0")`;
  - a CSV DateTime prints by `SimpleDateTimeFormat`'s rule;
  - `%r` has one Java owner;
  - envelope rows are written in result order.
- **S4 reads the PCT harness's spelling** (`TestTDS.java:209`).

### 3.7 The judges

- **The host judge** words failures through `HostWords`, from kinds and bounds, in the plan's mode: `LiteHostWords`
  or `EdgeHostWords` (legend-pure's assert words, for PURE and for ENGINE's after-query asserts).
- **The product's database judge** keeps lite's words in SQL (01:30 ruling).
- **Under C4,** PCT runs HOST-only after the 36 combos are ported into core, and the compatibility database machinery
  is deleted. Under C1, the edge's database judge plans lite's words.

### 3.8 Hosts

- **LITE routes.**
  - Under K2, a host words a site from a **generated sentence table**: TypeScript
    `engine-client/src/generated/lite-errors.ts` and Python `python/legend_lite/_lite_errors.py`.
  - Both are emitted by `engine-client/tools/typefacts/TypeFacts.java`, the generator that already writes
    `lite-raise.ts` and `_type_facts.py`, with the existing diff tests (`//engine-client:update_generated`,
    `//python:update_generated`).
  - `PLAN_VERSION` is generated the same way.
- **ENGINE routes** (the hosts' edge routes) call the Java error half: `HostAnswers.errorAnswer(plan, statement,
  databaseMessage)`, exported as wasm `executeErrorAnswerOrError` and native `lite_execute_error_answer`. `siteWords`
  is deleted.
- **PURE** has no host surface now.
- **The readers that change with each bump:**
  - Java: `PlanJson`, `TestPlanJson`;
  - TypeScript: `plan-runner.ts`, `declared-raises.ts`, `raised.ts`, and their consumers in `datacube/src` and
    `query/src/app/run.ts`;
  - Python: `_runs.py`, `_raised.py`, `_planned.py`;
  - the shared fixture `raise-envelopes.json`.

### 3.9 Export (phase 2, ENGINE)

As in the previous revision. `edge.PlanExport.singleExecutionPlan(plan, connection)` translates per node and refuses
by name:
- **What it maps:** RELATION and VALUE `TdsResult`s, `Constant`, `Sequence`, temp-table IN `Load`s.
- **What it refuses:** OBJECTS, `Keyed`, `Update`, `Check`, `Effect`, raise sites outside class A, and a statement
  whose `tree` is null.
- **Parameters:** FreeMarker parameters are re-rendered from the `tree` through a `SqlWriter`, never by editing text.
- **Activities:** builder and result bytes are compared exactly; the activity `sql` is compared with the template
  rendered by legend-engine's own literal processors.
- **What runs** is lite's ENGINE SQL, not the 5,541-line plan-text family.

**Only ENGINE plans export.** A LITE or PURE plan is refused by name, because legend-engine would not give its answers.

### 3.10 Import (phase 2, ENGINE)

As in the previous revision: `edge.PlanImport.fromEngine(json)` gives an ENGINE plan. Its node table:
- **Mapped:** SQL, TDS, relation-data and data-type instantiation nodes, constants, sequences, blocks, allocations,
  temp tables, and `FunctionParametersValidationNode`.
- **Refused:** the FreeMarker conditional, Limit, PlatformUnion and PlatformMerge, AggregationAware, and every node
  with an `implementation`, platform, graph-fetch, M2M, external-format, REST or error node.

S9 starts with a census of node types. The departures from the program's §6 are stated: the conditional node is
refused, and nothing is re-planned.

### 3.11 What is deleted and what stays

- **Deleted:**
  - the MOVE and RETIRE rows of §2.1 (≈ 920 SQL code lines, less PURE's in-query print forms kept as PURE rows: about
    90 lines of `PureDateTimePrint`, `PureListTexts` and `EdgePureOnlyTexts`' format arm, measured in S5a);
  - `SiteWordsRecorder`, `PureText.recording`, `PlanStyle.siteWords` and `PlanStyle.errorWords`;
  - the route-keyed `PlanStyle` constants and the mode branches of §2.7;
  - under C4, the compatibility database judge.
- **Stays:**
  - each mode's KEEP rows;
  - kind C (the plan-text family, the C1 and C2 constants);
  - kind D;
  - `Column.precision`.

### 3.12 PURE's direct entry

**The door:** `com.legend.edge.PureEntry`, in the edge library (`//core:edge`), the only door for PURE beside PCT:

```java
public final class PureEntry {
    /** {@code lambda} over {@code model}, planned in PURE mode (parsed at LEGEND_LITE) and run on {@code runtime}'s
     *  sessions; the answer to {@code output}, errors in legend-pure's words. One planning call, one run call (the
     *  doors rule, ArchitectureTest.theDoorsOnlyCompose). */
    public static ExecutionPlan run(String model, String lambda, String runtime, PlanRunner.Sources sources,
            Output output) { ... }
    /** The plan alone, for a caller that runs it itself or inspects it. */
    public static ExecutionPlan plan(String model, String lambda, String runtime) { ... }
}
```

**How it is reached:**
- A Java caller depends on `//core:edge`, an opt-in, like K2's edge mount.
- Tests reach it through the existing test helper (`Lean.runInMode(model, query, runtime, backend, Modes.PURE)`,
  which replaces `Lean.runInStyle(…, PureText.UPSTREAM)`).
- It has no server route, no `--engine` mount and no Python or TypeScript entry ("Planning mode/direct entry for
  now").

### 3.13 PURE's planned diagnostic read-back (phase 2, if ruled)

**What it is.** A PURE site whose legend-pure sentence prints a computed operand carries a `ReadBack`: one planned
statement, written by the dialect at planning, that selects the guard's operand values from the same source where the
guard fires.

**How the planner writes it:**
- It takes the statement's own FROM, JOIN, WHERE and GROUP context.
- It replaces the projection with the operands' expressions, typed, as `Operand.Read` columns.
- It adds the guard's firing condition to the filter.
- It orders by a deterministic key and takes `LIMIT 1`. The key is the query's own order where it sorts, otherwise
  the operands' and the source key's columns ascending (G10). That gives legend-pure's "first failing element" where
  the order is defined.

**When exec sends it.** Only after the statement raised that site's token, on the same session, before any later
statement of the plan.
- On a database that aborts the transaction on an error (Postgres), it is sent after the rollback.
- Its row is decoded by its declared types, like any result. **There is no text parsing.**
- The row goes to the error, never to the answer.
- If no row comes back (the data changed, or a volatile operand), the error is reported by name ("the operands of
  site n could not be read back") with legend-pure's sentence without the value. This is not a guessed value (G7).

**What is not read back**, decided at planning, with the site keeping the interim words:
- a guard inside an aggregate, a window or a correlated subquery whose context the read-back cannot reproduce;
- a plan with writes before the statement;
- a volatile operand.

**What it is not.** It is not A1's carriage: nothing travels inside the raise's text, so the per-database transport
problems the review found do not arise.

**Its correctness per dialect** is measured: a computed-operand raise test per database, DuckDB, H2 and SQLite per
push, the others by a lane round.

**It extends charter A3.** Today A3(c) allows "statements the plan's target declares, sent as written when the run
needs them" (MariaDB's and MySQL's raise read-back, H2's settings). A site's read-back is a statement the plan's
**site** declares. That widens (c) from a target's statements to a site's. **The user ratified A3 as one list
(2026-11-12), so this needs the user's ruling: (a) above.**
- `SentStatementsAreThePlansTest` and the `RunScope` proof count read-backs as the plan's statements, which they are.

**Cost:** 3.5 days plus a lane round.

**Recommended phase: 2.** PURE is a planning mode with one direct entry for now, and no measured case needs it.

---

## 4. The decisions

### (a) Raise operands: **PENDING THE USER** (reframed per mode)

| Mode | Needs operands? | Mechanism |
|---|---|---|
| LITE | no (the 2026-11-10 ruling: kind and where, never the value) | — |
| ENGINE | no: where legend-engine plants no check the database raises (class A); class B is refused at planning; classes C and D are constant sentences | — |
| PURE | yes: legend-pure's sentence prints the value ("Cannot divide 5 by zero") | literal operands as `Operand.Fixed` (phase 1); computed ones by the planned read-back (§3.13, phase 2) |

| Option for PURE's computed operands | What | Cost |
|---|---|---|
| **RB (recommended)** | the planned diagnostic read-back; **extends charter A3(c) from a target's statements to a site's** | 3.5 days + a round, phase 2; phase 1 keeps the interim register row |
| A3 permanent | legend-pure's sentence without the value, as a register row forever | 0.5 day |
| A1 | carriers inside the raise's text | rejected by the review: the transport is unmeasured on every database, and it re-opens 21:20 item (5) |

### (b) The kind-B rows: **RULED**, re-cut per mode

*RULED 16:30 (rule (b0) and the classification); re-cut 19:45 per mode. Every cell is one mode's one form; there is
no either/or left between modes. A cell marked "S5a" is read from that mode's oracle before it is built.*

| Row | LITE (product) | PURE (legend-pure 5.99.0; PCT's expected values decide) | ENGINE (legend-engine 4.145.0; its manifests decide) |
|---|---|---|---|
| Float `toString` in a query (r8) | S311 six decimals | legend-pure's print (`DecimalFormat("0.0")`), in SQL (`FLOAT_SHORTEST_TEXT`) | `cast(%s as varchar)` (H2 `:266`, DuckDB `:284`) |
| **R1**: DateTime or Boolean `toString` in a query | S312 ISO `Z`; `true` | legend-pure's print (`2014-01-01T00:00:00.000+0000`; `true`) | the cast: `2014-01-01 00:00:00`, H2 `TRUE`. legend-engine's manifests expect it: DuckDB `testDateTimeToString`, `testDateTimeWithTimezoneToString`, `testSimpleDateToString`; H2 those plus `testBooleanToString` |
| Float literal (r21) | DOUBLE (S20) | S5a against PCT (it moved 0 PCT tests when adopted); by default LITE's DOUBLE | DECIMAL (legend-engine's literal processor) |
| **H6 / R4**: `format` | `%s %d %f %t{}`; `%r` and padding refused (S314) | answered, including `%r` and padding: in SQL where it feeds a computation, in Java at a root | refused at planning: "No SQL translation exists for the PURE function 'format_String_1__Any_MANY__String_1_'" (12 `format` tests in its DuckDB essential manifest) |
| relation `toString`/`toCSV`/`csv` (M1) | refused (S316b) | presented (Java); refused where it feeds a computation | after the query: presented (Java, legend-pure's print); at the query's root: refused (no SQL) |
| a list's text (R2) | JSON array (S316c) | `[a, b]` (SQL in a query, Java at a root) | after the query `[a, b]` (Java); in the query: refused |
| an empty cell in a join (R3) | skipped (S313) | S5a against PCT (`TDSNull` or skipped) | `group_concat`/`string_agg` skip NULLs: LITE's form, a deletion |
| raises (B7, classes A-E) | every check; kind and where | every check; legend-pure's sentence | class A: no check; B: refused at planning in its words; C, D: constant sentences; E: F44 (the runner) |
| parses (B6) | the grammar guard, `TEXT_NOT_*` | legend-pure's parse semantics and sentences (`parseInteger.pure`, `parseFloat.pure`; S5a) | plain casts per database: H2 `as integer` (F38) and `as float`; DuckDB `as BIGINT` and `as float` (a 32-bit REAL) |
| `parseDecimal` (B10) | the product's | legend-pure's exact parse (S5a) | H2 `decimal(5,2)`; DuckDB `decimal` = DECIMAL(18,3) (manifest, line 398) |
| `parseJson` (B11) | the product's | legend-pure's parse (S5a) | the strict cast (DuckDB `:259`, H2 `:250`). The `}{` tolerance leaves the SQL for the corpus judge's grammar (S144) |
| `isNumeric` (B8) | the product's | legend-pure's (S5a) | `lower(s)=upper(s)` (H2 `:232`, DuckDB `:238`) |
| EXISTS as a join (B9) | lean | LITE's, unless an answer differs | legend-engine's second emission (S367) |
| scan order (B4) | none | none | `StableScanOrder`, a `Computation` component, not a JVM property |
| H2 settings (B5) | H2's defaults | S5a (PCT's H2 lane) | `MODE=LEGACY`, `DEFAULT_NULL_ORDERING=HIGH` |
| SQLite Decimal arithmetic (B3) | refused by name | REAL (ruled (c); PCT unmoved) | n/a: no extension, no lane (H5) |
| raw SQL translation (B13) | as written | n/a | translated (an INPUT row) |
| an unused derived column that can raise (B14) | pruned (ruled) | legend-pure evaluates `extend` eagerly (S5a) | the engine runner's answer (F5) |
| WHERE zones (B12) | none | none | only if an answer changes (S5a) |
| result envelopes (f) | refused | n/a (PCT's transport only) | `PureV1Writer` |
| activity SQL (C3) | lite's SQL (S310) | n/a | lite's SQL: a register row |
| object reference connection, graph keys (C1, C2) | none | S5a | legend-engine's constants (kind C) |

### (c) The test plan's words and the database judge: **PENDING THE USER** (unchanged)

The options, costs and recommendation are as in the previous revision.
- **C4 (recommended):** the 2026-11-17 02:30 order. Port the 36 combos into core, run PCT HOST-only, and delete the
  compatibility database machinery. It is slice S2b, 3 days.
- **C1:** an interim in which the edge's database judge plans lite's words.

### (d) The safety net: **RULED** (D1); (d′) the per-push lanes: **PENDING THE USER**

D1 stands as revised: the corpus messages, channel B on H2 and SQLite, masking by field, the ordered rows, activities
out, and the planted changes.

**(d′) What runs per push:**
- **PCT channel A in ENGINE mode** against legend-engine's manifests: DuckDB's five suites and H2's relation suite.
- **The SQLite PCT recording** (PURE).

Their cost is in §8. Recommendation: add both.

### (e) Import: **RULED** (E1). (e′) ENGINE's scope: **PENDING THE USER**

legend-engine fails some PCT cases. Its manifests' `expectedError` texts fall into two classes:
- **planning refusals**: "No SQL translation exists for the PURE function '…'", and class B's own words;
- **defects**: wrong answers from its SQL or its planner, such as "Cannot cast a collection of size 0 to
  multiplicity [1]" for `testListToString` and "Match failure: ClassInstanceHolderObject instanceOf
  ClassInstanceHolder".

| Option | What | Cost |
|---|---|---|
| **E-R1 (recommended)** | ENGINE refuses exactly what legend-engine refuses at planning, in its words: a per-database table of refused functions **derived from the manifests' "No SQL translation" entries**, never hand-listed. Where legend-engine fails by its own defect, ENGINE answers correctly, and each such case is a register row counted from the manifests | +1 day in S5b. ENGINE's PCT lane then compares exactly against legend-engine's manifests (pass where it passes, its words where it refuses), with the defect rows ledgered |
| E-R2 | ENGINE answers everything lite can; it differs from PURE only in forms | ENGINE's lane needs a large "better than legend-engine" ledger; Studio clients would get answers legend-engine never gives |

### (f) `EngineResultEnvelopes`: **RULED** (retire)

Written by ENGINE's `PureV1Writer` over typed rows (the corpus is ENGINE).

### (g) The plan format: **RULED**, with one fact corrected

There is one bump for each format-changing slice after S1 generates `PLAN_VERSION`. S1 itself adds `mode`,
`extensions`, sites without words and `Operand.Fixed`. RB, in phase 2, adds `ReadBack` and `Operand.Read`.

### (h) Lite's own sentence in the product plan: **PENDING THE USER**; the recommendation flips to K2

| Option | What | Cost |
|---|---|---|
| **K2 (recommended)** | The plan carries facts: kind, where, operands where the mode carries them, a read-back. Each mode's writer words them. The hosts' LITE routes word from the generated sentence table; their ENGINE routes call the Java error half | S1: 3 days. The product plan JSON changes once: sentences out, so it shrinks, with a dated note on the `lean_sql_*` plan pins. Re-rules 21:20 item (2) ("both messages RENDERED AT PLANNING"). Exec still only looks up (21:20's intent) |
| K1 | Each mode's words are rendered at planning into the plan (lite's in the site, the upstream modes' in an edge extension) | S1: 2 days. **PURE's computed-operand words cannot be rendered at planning**, so PURE needs a second, writer-side mechanism: two mechanisms |

**Why K2 now:**
- One kind has three wordings.
- PURE's sentences need values that exist only after the run.
- K2 is one mechanism for all three modes, and it makes rewording an `ErrorKind` sentence no longer a format change
  (PRODUCT-4).

---

## 5. The phased plan

**The order:**
1. The net, plus the guards in report-only mode, plus ENGINE's PCT lane.
2. Modes as data (a scaffold that changes no behaviour).
3. Raise words in the writers.
4. PCT HOST-only.
5. The SQL builders become Java writers, and each form moves to its mode.
6. PURE's door.
7. The hook surface folded into the mode records.
8. Phase 2: RB, runner tooling, export, import.

**Every slice** sends a shape note to the orchestrator before building. **The local loop** is the touched packages'
tests, `//core:guardrails`, `//core:census`, `edge_net`, and the report-only G1, G3 and G6 counts.

### Phase 1

| Slice | Shape | The check that proves it | Lanes identical | Size |
|---|---|---|---|---|
| **S0 The net** (d) | as in the previous revision; G1, G3 and G6 report-only | the planted changes; green at the base | everything | 2.5 days |
| **S0e ENGINE's PCT lane** | channel A's expected failures in ENGINE mode read from legend-engine's manifests (`@legend_engine_src`, spec input only), on DuckDB's 5 suites and H2's relation suite | the lane runs; its differences from the manifests are listed (the baseline for S5) | PURE lanes unchanged | 1.5 days |
| **S1m Modes as data** (scaffold) | `Mode`, `Computation`, `Modes.ENGINE`/`PURE`; the doors choose a mode; the grammar from the mode; the plan's `mode`. **PURE := today's `UPSTREAM`, ENGINE := `PURE_V1` plus the corpus's forms: no behaviour change** | every suite byte-identical; G6 counts the remaining branches | everything | 1.5 days |
| **S1 Raise words in the writers** (K2; v59) | sites as facts; `Operand.Fixed` (PURE); the families' `ErrorWords`; the stateless `RaisedWords`; the Java error half; the generated sentence tables and `PLAN_VERSION`; the recorder and `siteWords` deleted | the net's messages unchanged; literal-operand raises on the tab and Python; product plan pins moved once, dated | PCT A and B, corpora, pure/v1, hosts | 3 days |
| **S2 Raises per mode** | ENGINE: class A plain, B refused (F43), C/D constant; PURE: every check, legend-pure's sentence, the computed-operand interim row; `PureMessages.sentence` deleted | the net unchanged (0 computed cases); ENGINE's PCT lane moves toward the manifests, listed | as S1 | 0.5 day |
| **S2b PCT HOST-only** (C4) | port the 36 combos into core; PCT HOST-only; the compatibility database machinery deleted | core isolated BOTH green on the ported shapes; channel B under HOST unchanged | channel B, core | 3 days |
| **S3 Test plan words** (v33) | kinds and bounds; `words` → `mode` | channel-B messages unchanged | channel B, corpora | 0.5 day |
| **S4 Relation text in Java** (M1) | `Presented`, `edge.RelationText`, presented roots and sides; `RelationTextLowering` deleted | V-EDGE's EDGE-1/2/3 tests in PURE and ENGINE; the net unchanged except listed moves | channel B, corpora, pure/v1 | 3.5 days |
| **S5a Measure per mode** | each open cell of §4.b read from its mode's oracle: PCT's expected values for PURE, the manifests for ENGINE; the E-R1 refusal table derived | one table to the user: row, mode, upstream evidence, moves | none (measurement) | 1.5 days + rulings |
| **S5b Each form to its mode** | PURE's in-query print forms kept and ledgered; ENGINE's casts and refusals (E-R1); R2-R4 and M2-M5 presenters | PURE's lanes move only toward PCT's expectations; ENGINE's only toward the manifests; each move listed | PCT A and B per mode (lane round), corpora | 3.5 days |
| **S5c PURE's door** | `PureEntry`; `Lean.runInMode` | PURE tests on DuckDB, H2 and SQLite | — | 0.5 day |
| **S6 Envelopes** (f) | `EngineResultEnvelopes`, `EngineExecutedValues`, `oneBare` → `PureV1Writer` | corpus rosters; EDGE-8's ordered probe | corpora | 1 day |
| **S7 The hook surface into the mode records** (ARCH-4) | `PlanStyle`'s service locator, `twoForm`/`facet`, `KINDS`/`TWO_FORMS` replaced by `Computation`'s components across the 43 product files; §6 blocking | javac exhaustiveness; G1-G6 green and blocking | everything byte-identical | 2.5 days |

**Phase 1:** about **25 working days**, plus rulings and lane latency. That is 6.5 more than the previous revision's
18.5:
- S0e: +1.5;
- S1m: +1.5;
- K2's S1: +1;
- S5a per mode: +0.5;
- S5b per mode, with E-R1: +1.5;
- `PureEntry`: +0.5.

### Phase 2

| Slice | Shape | The check | Size |
|---|---|---|---|
| **RB PURE's read-back** (a), if ruled | `ReadBack` per computed-operand PURE site; exec sends it after the site raised (charter A3, extended) | done-when 9: a computed-operand raise test per database (DuckDB, H2, SQLite per push; a lane round) | 3.5 days |
| **R Runner tooling** | the engine runner executes a `SingleExecutionPlan` and runs `PlanGenerator`; a shared seeded database | the pure/v1 golden set executed by legend-engine | 3 days |
| **S8 Export** (ENGINE) | `edge.PlanExport` | done-when 10 | 2.5 days |
| **S9 Import** (ENGINE) | the node census, then `edge.PlanImport` | done-when 11 | 4 days |

**Phase 2:** about **13 working days**: 9.5 as before, plus RB's 3.5.

**In all: about 38 working days** (the previous revision's 28, plus the modes' 6.5 and RB's 3.5). On a three-week
budget, phase 1 does not fit whole: S0 to S5c, about 22 days, do.

---

## 6. The guard for the end state

The previous revision's G1-G5 stand, each now **per mode**:
- **G1's ledgers** are keyed by mode: `KIND_B[PURE]`, `KIND_B[ENGINE]`, plus `KIND_C`, `KIND_D` and `INPUT`.
- **G2** scans every plan in every mode.
- **G3** plans each case under LITE and under each upstream mode minus its kind-B rows, and attributes the rest per
  mode.

They run report-only from S0 and block from S7. Every rule has a planted violator, and no rule depends on a clock.

**New: G6, no code branches on the mode outside the mode records.**

| Check | How | Both ways |
|---|---|---|
| `Mode` and `Computation` are records with data components only | reflection: no method beyond the components, `equals`, `hashCode` and `toString`; no component typed `boolean` except `siteOperands`, which `PlanJson` alone reads; no `isEngine`-style accessor | a planted record with a branch method fails |
| No comparison of modes outside the records | bytecode scan (`ArchitectureTest`'s classes): no `if_acmp*` or `equals` with a `Mode` operand, no `switch` over a mode's name, no `Mode#name()` read outside `PlanJson`/`TestPlanJson` (serialization) and the run's one family check (`Output.writerFor`, listed) | a planted `if (mode == Modes.PURE)` fails; each listed reader must exist |
| A door may choose a mode, never test one | the doors (`ArchitectureTest.PLANNER_ENTRIES_ONLY`'s callers, `PureEntry`, `EdgeServer`, `PureV1Routes`, `PctPlanRoute`, `ChannelB`, the corpus) may read `Modes.ENGINE`/`PURE`/`Mode.LITE`; no other class may | a planted non-door read fails |
| Today's branches are gone | `PureText.isEdgeStyle`, `PlanStyle.judgesEngineText`, the recorder's template check and the scan-order system property are deleted; the rule's first run counts them | n/a |

The rest of §6 is unchanged:
- `CompatSpellingGuardTest` is kept, with a derived vocabulary;
- G4 is behavioural, over `engine-client/src`, `python/legend_lite`, `datacube/src` and `query/src`;
- G5 is transitive, like THE WALL;
- the ledgers are shrink-only, each new row with a dated ruling.

---

## 7. Risks and how each is caught

The previous revision's risks stand. The reframe adds these:

| Risk | Caught by |
|---|---|
| A mode branch creeps back (an `if PURE` in a planner or writer) | G6 (§6), blocking from S7 |
| PURE drifts into an interpreter (Java evaluating more than one native's print) | the closed presentable list (§3.2); G5; nesting refused |
| A form lands in the wrong mode during the split (S1m to S5b) | S1m is byte-identical by construction; then each mode's lane may move **only toward its oracle**. A PURE lane moving away from PCT's expectations, or an ENGINE lane moving away from the manifests, is a misclassification |
| ENGINE's manifest oracle is read wrongly (a defect taken for a refusal) | the E-R1 table is derived by message class ("No SQL translation exists…" alone), and every other manifest entry is a defect row the user sees in S5a |
| The read-back reports another failing row than the one that raised | its deterministic order (§3.13); where no order is defined, the reported operand is the first by that order, and the register row says so |
| The read-back on a database that aborts transactions | sent after the rollback; measured per database (RB's check) |
| Lane cost grows per push | measured in S0 and S0e; if over budget, ENGINE's lane moves to every merged slice instead of every push (that is (d′)) |

---

## 8. Fidelity per mode, and the lanes

**The oracle of each mode:**
- **PURE:** legend-pure 5.99.0's pinned sources and PCT's expected values, the tests' own asserts. Where compiled and
  interpreted legend-pure differ, PCT's expected value decides.
- **ENGINE:** legend-engine 4.145.0's pinned sources (`pureToSQLQuery.pure`, each database's extension), its 77
  relational PCT manifests, and the engine runner for answer bytes.
- **LITE:** lite's own register.

Every kind-B row cites its mode's source: the file and line, or the manifest entry, or the PCT case.

**Reading them in this container.** There is no checkout and no Bazel here. `git ls-remote` reaches both pinned tags,
and blob-less clones at the tags work; this revision read the extensions and manifests that way. The engine runner
needs a Bazel session, after slice R.

**The lanes:**

| Lane | Mode | Oracle | Where it runs |
|---|---|---|---|
| PCT channel A, DuckDB (5 suites), H2 (relation), SQLite (5 suites) | **PURE** | PCT's expected values | per push, in-process (SQLite per (d′)) |
| PCT channel A, DuckDB (5), H2 (relation) | **ENGINE** | legend-engine's DuckDB and H2 manifests | per push, in-process ((d′)) |
| PCT channel A, Postgres | PURE and ENGINE | as above, the Postgres manifest | CI |
| PCT channel A, ClickHouse, Databricks, Snowflake, Trino | PURE and ENGINE | as above, their manifests | rotating lane rounds |
| PCT channel A, MariaDB, MySQL, MongoDB, BigQuery | PURE only | PCT's expected values (legend-engine has no manifest; H5) | rotating lane rounds |
| PCT channel B, every lane | PURE only | the channel-B ledger | as today |
| the corpus, DuckDB and H2 | ENGINE | its rosters | as today |
| the pure/v1 net | ENGINE | its goldens | per push (`edge_net`) |
| the product's suites | LITE | the product's own tests | as today |

**The cost.**
- **Per push:** ENGINE adds one more channel-A pass of DuckDB's five suites and H2's relation suite. The last
  recorded figure is old: `pct_duckdb` 86 s and `pct_h2` 35 s (`docs/GATES.md:6642`, an earlier batch). The current
  times are measured in S0. On that figure the addition is about 2 minutes per push; the SQLite recording adds about
  the same again. So the per-push total is about +2 to +4 minutes.
- **External rounds:** ENGINE adds 5 databases (ClickHouse, Databricks, Postgres, Snowflake, Trino) to each round. It
  adds nothing on the 5 databases legend-engine does not cover.

---

## 9. Measurement commands (at `68f20eb4d`)

As in the previous revision, plus:
- **The manifests' databases:** `git ls-tree -r --name-only HEAD | grep -oE 'pct-manifests/relational-[a-zA-Z0-9]+/' | sort -u`
  (clickhouse databricks duckdb h2 memsql oracle postgres snowflake spanner sqlserver trino).
- **The R1 tests in lite's net:**
  `grep -E 'testDateTimeToString|testDateTimeWithTimezoneToString|testSimpleDateToString|testBooleanToString' pct/src/test/resources/edge/duckdb_essential_outcomes.tsv pct/src/test/resources/edge/channel_b_duckdb_essential_outcomes.tsv`
  (all SUCCESSFUL and PASS).
- **The grammar levels at the doors:** `grep -rn 'GrammarLevel\.' core/src/main/java/com/legend/Compiler.java core/src/main/java/com/legend/door/Parse.java core/src/main/java/com/legend/parser/PmcdParser.java`;
  `ChannelB.java:140-142`.

---

## 10. Review responses

These answered the review of the previous revision (`000008e11`). They stand, re-cut per mode where §11 says so.

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

---

## 11. What the reframe changed (19:45), against the previous revision

| Item | Before (`000008e11`) | Now |
|---|---|---|
| The axis | presentations `lite`, `legend-pure`, `legend-engine`, chosen by route-keyed styles; `UPSTREAM` mixed both upstream meanings | **one mode axis: LITE, ENGINE, PURE**, records of data, chosen at the door, carried through parse, plan and presentation (§3.1); `UPSTREAM` split (§2.7) |
| R1 (DateTime/Boolean `toString` in a query) | PENDING: R1-a, b or c | **resolved by modes:** PURE gives legend-pure's print, ENGINE the cast. PCT's PURE lane keeps passing those tests; ENGINE's lane fails them exactly as legend-engine's manifests expect |
| H6 (kind-2 roots legend-engine refuses) | PENDING: H6-a or b | **resolved by modes:** PURE answers (`format` included), ENGINE refuses in legend-engine's words |
| (a) | A3 (no value) recommended | **per mode:** LITE and ENGINE need none; PURE needs the planned read-back (a charter A3 extension, PENDING), recommended in phase 2 with an interim row |
| (h) | K1 recommended | **K2 recommended:** three wordings, and PURE's need run-time values |
| The query boundary | a marker if S5a counted outside-eval cases | **not needed:** ENGINE runs only where the boundary is physical; PURE's two sides are both legend-pure's |
| (b0) H5 | LITE's form on databases without an extension | the same for ENGINE; such databases get no ENGINE lane |
| New | — | (e′) ENGINE's scope (E-R1); (d′) the per-push lanes; G6 (no mode branching); `PureEntry`; the read-back design |
| Size | about 28 days (18.5 + 9.5) | **about 38 days (25 + 13)**, itemized in §5 |
