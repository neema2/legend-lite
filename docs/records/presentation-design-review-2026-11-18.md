# Review: "One plan, two presentations: the design (2026-11-18)"

Reviewed: `origin/claude/presentation-design:docs/records/presentation-program-design-2026-11-18.md` (741 lines),
against the program (`edge-portability-program-2026-11-18.md`), the plan of record
(`total-wall-edge-program-2026-11-16.md`, log to 2026-11-18 14:47), AGENTS.md, the PR #118 audit (README, findings,
verification), the code at `68f20eb4d` (read-only checkout), and upstream read directly: blob-less clones of
legend-engine 4.145.0 (`230c159`) and legend-pure 5.99.0 (`7fbc7d6`). I did not run Bazel. My own method counter is in
`scratchpad/design-review/tools/methods.py`.

## Verdict: READY WITH CORRECTIONS

The architecture is right. Computation stays in SQL for both products, presentation moves to Java writers over typed
rows, the plan names its presentation as data, the runner is unchanged, and refusals are by name. That matches
G1/G4/G6/G7, the plan of record's done-criteria 2 and 3, and the program. Most measurements check out, and the
biggest claim holds: relation text needs no kind-B row.

The owner should not rule on §4 as it stands, though. Five things are wrong or missing:

- **Two hidden re-rulings.** The design undoes parts of the error design the user adopted on 2026-11-16 at 21:20, and
  it ignores the plan of record's DECIDED entry of 2026-11-17 02:30 (PCT goes HOST-only, and the compatibility
  database judge is deleted).
- **A wrong premise about the net.** The net does pin failure messages.
- **R1's cost is knowable now and unstated.** Lite would fail PCT tests it passes today.
- **The guard would not work as specified.**
- **The size is about 1.7x optimistic.**

All of it can be fixed in the document in a few hours. The "Corrections" list at the end is in priority order.

---

## 1. The measured claims

| Claim | Status | Evidence |
|---|---|---|
| ≈1,090 SQL-building code lines; the nine files 878; split per file | **CONFIRMED** (within ±5%) | My counter gives 909 for the nine files (audit ~900). RelationTextLowering is 481 to the design's 459, the gap being signature lines. The code counts agree exactly: 556/325/124/106/87/60/26/19/10. The total with EngineExecutedValues 13, oneBare 15, TestDataRoutes 42, PureText 151 and traceText 5 is ≈1,135 to the design's 1,091. **Caveat:** "presentation proper ≈950", and "≈950 deleted" in §3.11, are slightly high. PurePrintForms' KEEP parts (`floatOf`, `FloatLiteral`, `UnguardedParses`, about 30 lines) stay, and so does PureMessages' 228-line Java half. |
| 1,486 `activities=` lines (DuckDB 460+64, H2 439, SQLite 459+64) | **CONFIRMED** | `grep -c 'activities='`. All are lite's executed SQL (lite's quoting, SQLite's `LG#` guards), not legend-engine plan text. |
| 171 masked VALUE values; SQLite relation 831 masks vs 459 trace ids | **CONFIRMED** | 88 DuckDB plus 83 SQLite. The other 372 SQLite masks are inside the activities' SQL, not in rows. |
| pure/v1: 8 answer lines with activities, all lite's SQL | **CONFIRMED** | All 8 carry `\"t0\"` aliases. |
| "0 of the 2,710 DuckDB channel-A answer lines computes a #TDS text inside the query" | **CONFIRMED** | 847+554+479+708+122 = 2,710. `String:#TDS` and multi-line CSV values: 0 on DuckDB, and also 0 on H2 and SQLite. Upstream agrees: legend-engine's DuckDB RelationFunctions manifest has only 2 exclusions. legend-pure's tests call `toString` outside `$f->eval(...)`. |
| legend-engine 4.145.0: 451 pairs; none for `toCSV`, `format`, `makeString`, `toRepresentation` | **CONFIRMED** | `grep -c 'PureFunctionToRelationalFunctionPair(first'` gives 451. The h2Extension2_1_214 lines check out: `:232` isNumeric, `:235` joinStrings, `:247` parseDecimal, `:248-249` parseFloat/parseInteger, `:266` toString. |
| (the brief's paraphrase) "pushes only toString (and formatDate) of the print functions" | **CORRECTED** | The text-producing pairs are `toString_Any_1` (:10663), `formatDate` ×2 (:10634-5), `joinStrings` ×2 (:10647-8) and `toJson_Variant`; `parseJson` is pushed through a dynaFn. DuckDB's forms: toString `cast(%s as varchar)` (:284), joinStrings `string_agg(%s,%s)` (:240), parseJson `cast(%s as JSON)` (:259), formatDate (:302). §3.2 lumps "`makeString`/`joinStrings`" together, but joinStrings is kind 1 and makeString is not. `formatDate` (kind 1) is missing from the rule's list and from §4.b. |
| "The edge net pins verdicts only for channel B and the corpus, not failure messages" | **CORRECTED** | **Channel B does pin messages.** `channel_b_duckdb_*_outcomes.tsv` hold the full message of every non-PASS row: 9 in essential, 5 `AssertFailed` (for example "No error was thrown" and "expected: 0 / actual: 1") and 4 `DataError`. Channel B's `ledger.tsv` pins a message substring for 295 expected failures across 11 lanes. Channel A ERROR lines pin messages too (DuckDB 27, H2 26, SQLite 34); the channel-A outcome files are all SUCCESSFUL. The **corpus** claim is right: `*_judge-host.tsv` has 62 FAIL rows (DuckDB) and 78 (H2) with no message. pure/v1's 5 error bodies are pinned. The real gaps are corpus messages and channel B on H2 and SQLite (the net has DuckDB only). The words this program removes from the judge (`UpstreamVerdictWords`) reach no reader under BOTH, so no golden could pin them. |
| Zero computed raise operands in upstream-facing goldens | **CONFIRMED** (as the census's finding) | Plan-of-record log 21:05. My spot check of the channel-A ERROR messages found only literal operands: "Cannot divide 5 by zero", "sqrt of -1.0", "offset 2 … size 2", "Cannot get hour for 2017", "Invalid month: 13", the slice bounds. Proving none at the head would need a run. |
| "~9,400 lines of legend-engine plan text … export's base" (§2.1) | **CORRECTED** | The named family (EnginePlans, PlanText, EngineStyleH2/DB2/Composite, PlanEnumForm, InProtocol, PlanVariables) is **5,541** lines at `68f20eb4d`; the audit says ~4,500 lines of renderers. 9,400 is the program doc's number, not a measurement, although §0 says every §2 number was measured. §2.1 also calls it "export's base" while §3.9 says "They are not export". The design is right that it is not export, but it should say that this departs from the program's §5 table. |
| §2.2: `objectReferenceConnection` is "presentation", and the presentation members "leave" | **CORRECTED** (internal) | §2.6.2 calls it a protocol constant and §4.b row C1 KEEPs it as kind C. Pick one. |
| 77 PCT manifests; 40 node classes | CONFIRMED / minor | 77 relational manifests (11 databases × 7; 99 in all). The 40 node files span three protocol modules, and some are not nodes. `graphFetch` has 19 files, not 15. |

## 2. The target design (§3) against G1-G11 and AGENTS.md

**Right:**
- one runner, unchanged (G1);
- exec sends only the plan's statements: A1's carriers come back in the error, with no new statement;
- the plan holds statements and facts (G4);
- every family refuses by name (G7);
- the dialect still owns SQL, and export re-renders from the plan's `tree` (`PlanJson.java:766`) with a `SqlWriter`, never by editing text (invariant 3);
- the edge depends on the product through ServiceLoader (G5).

**Problems:**

1. **It reverses parts of the user-adopted error design (2026-11-16 21:20, "rethought with the user from a clean
   sheet and adopted") without saying so.**
   - Item (2) of that design: "the plan carries a site table: kind, where, and both messages RENDERED AT PLANNING …
     exec only looks it up, with no parsing or formatting; the hosts read the same table". Done-when 2 and S1 remove
     *lite's own* sentence from every product plan (`sites[].message`, `raises`) and move host wording to generated
     tables. The program does not need this: export and import never carry lite's sites, and "one plan" is about SQL.
     ARCH-7 rates it low.
   - Item (5) of that design: "Dropped: the operand carriage … per-type read-back". A1 brings that carriage back, and
     with it parsing in `exec.RaisedErrors`.

   Both may be good changes, but they are re-rulings, and §4 must present them as such.

2. **Upstream names in the product's records.**
   - `Presentation {LITE, LEGEND_PURE, LEGEND_ENGINE}` in `com.legend.executionplan`, and
     `Presented.Form {PURE_TDS_TEXT, …, ENGINE_TDS_JSON, …}` in `com.legend.testexecutionplan`, put upstream concepts
     into product plan records. The separation census reached 0 partly by deleting exactly such components
     (`UPSTREAM_WORDED`).
   - The test plan already has the seam for edge-only facts: `Extension`/`ExtensionCodec`, which JUDGES praises
     ("the edge's facts are opaque to the product").
   - **Alternative:** the product knows only `LITE` plus an opaque presentation id that the edge's provider resolves.
     `Presented` becomes an edge `Extension`.
   - Either way, say how `EdgeSeparationTest`'s meaning lens treats it.

3. **The product plan is touched, so its pins move in S1.**
   - Every product site gains `operands` (`Fixed` is not limited to edge plans), and every plan gains `presentation`.
     Messages and `raises` sentences leave.
   - `lean_sql_baseline` and `lean_sql_corpus` pin a `plan` column of plan-JSON characters
     (`LeanSqlCorpusTest.measure`, `lean-sql-baseline.tsv`). It will move, probably down.
   - That contradicts §5 ("must stay identical, every slice") and done-when 6 ("the product is not touched").
   - Either write `Fixed` operands only in edge-style plans, or state the move with a dated note.

4. **The database judge: nothing breaks, but the design builds machinery the plan of record has already decided to
   delete.**
   - In the product, nothing changes: S316b refuses relation text, and lite's database judge keeps its SQL words
     (ruling 01:30).
   - Under the edge, every presented side gets a DECLINED database verdict. `IsolatedRun.difference` skips declines
     under the compatibility rules, so it works mechanically, but the database judge stops judging every
     relation-text assert (all 469 channel-B relation tests).
   - The plan of record **DECIDED** on 2026-11-17 at 02:30: "PCT runs HOST only after the 36 combos are ported into
     core … (4) PCT host-only and delete the leftover compat database machinery". The design cites 01:30 but not
     02:30.
   - C1, "the edge's database judge on lite's words", is an interim for machinery that is scheduled to go.

5. **PCT: R1 and rule 3 move channel-A outcomes on purpose.**
   - §7 says "any channel-A move in S4 or S5 is a misclassification", which contradicts R1.
   - §3.2 rule 2's justification ("it runs after the SQL, in legend-pure's runtime") is false at a channel-A eval
     root. There legend-engine **refuses** kind-2 functions: its DuckDB essential manifest lists 12 `format` tests
     with "No SQL translation exists for the PURE function 'format_String_1__Any_MANY__String_1_'".
   - Lite passes those today and will keep passing them by presenting in Java. That is a deliberate
     better-than-upstream divergence; state it and register it.

6. **The second-evaluator boundary is not drawn.**
   - Presenting a kind-2 root means the writer evaluates Pure natives in Java: `format` with `%.2f` rounding and
     `%t{…}` patterns, `toRepresentation`, `makeString` over nested lists, pairs, an instance's `@_` id.
   - G6 allows this at the edge, but bound it: a closed list of presentable root functions, each one Java port of
     legend-pure's native, over SQL-computed typed operands, with nested kind-2 calls refused or listed.
   - G5 as written ("no `compiler.spec`, `TypedSpec`, `lowering`") is narrower than THE WALL. Mirror
     `execReachesNoPlanning`: transitive, with the allowed set base, json, the records and the switchboards.

7. **Hosts.** Java answer and error halves are the right shape (ARCH-1's option 2). Three gaps:
   - **ServiceLoader in wasm and native is unproven.** `Presenters.of` is a static ServiceLoader lookup, but the edge
     ships as a *separate* TeaVM module (`//wasm:engine_planner` beside `//wasm:planner`) and a GraalVM native-image
     (`//native:engine`). Prove discovery in both, or have the composition roots (`EngineExports`, `EngineEntries`,
     `EdgeServer`, `PctPlanRoute`) pass their family in explicitly.
   - **Two mechanisms for host wording.** The edge uses the Java error half; LITE uses generated sentence tables,
     which need a new Python generator. A Java error half for both families is simpler and keeps "hosts present
     nothing" literally true. This is the owner's choice.
   - **G4 would flag legitimate code.** Its lexical `['message']` rule matches error-body reads in
     `python/legend_lite/engine.py:435`, `compiler.py:91` and `warehouse.py:125`.

8. **Databases with no legend-engine extension.** legend-engine 4.145.0 has no SQLite, MariaDB, MySQL or MongoDB
   extension, so rule 1 ("legend-engine's own SQL") is undefined there. Rows already depend on this: B3 (SQLite REAL)
   and `PRINT_BARE`, "the closest form where legend-engine has no extension". A sub-rule is needed, for example "the
   product's form, unless ruled".

9. **Rule 1 is scoped by SQL, not by answer.**
   - As worded ("compute it in SQL with legend-engine's own SQL"), it covers every pushed text function (formatDate,
     joinStrings, toJson), and in the limit all 451 pairs.
   - The B list is in fact answer-driven: row 8 exists because the answers differ.
   - Say so: "the product's SQL, unless a measured answer differs from legend-engine's SQL's; then legend-engine's SQL,
     as a ruled row."

10. **Performance: low risk.**
    - Presenting in Java is linear over rows that are streamed anyway; the database stops building `STRING_AGG`
      texts; the full text is materialized either way.
    - The real costs are elsewhere. G3's measure plans each case 2 + N_B times (about 16× over PCT A, PCT B and both
      corpora), so it belongs in a heavy lane. And with host parity inside it, the per-push `edge_net` lane would
      need TeaVM and native-image builds (see §3 (d)).

11. **Export details.**
    - `tree` is nullable: refuse those statements by name.
    - legend-engine inlines FreeMarker values as literals, where lite binds typed parameters (D1). Float, Decimal and
      date typing can differ (the S20 class). The pure/v1 golden set has no parameters, so done-when 4 never
      exercises this.
    - legend-engine's activities record the SQL with values substituted, and lite's do not. Say how done-when 4 and 5
      compare activities.

## 3. The decisions (a)-(g)

**(a) Raise operands.**
- The options are nearly complete, and the recommendation is technically sound. But:
  - A1 re-opens 21:20 item (5).
  - "Exact carriers per database already exist and are measured by the product probe" conflates *value* carriers
    (query results) with *error-message transport*. The only path that sends computed text inside a raise today is
    the capped TRANSITIONAL `PureMessages.sentence`, and it has 0 upstream-facing cases, so it has never been measured
    against a golden.
  - The transport differs per database: the envelopes, SQLite's `QUOTE_MARK`, H2 echoing the statement text that
    holds the marker, DuckDB-wasm against JDBC. One day for 12 databases is low.
  - A1 does nothing for LEGEND_ENGINE: pure/v1 matches legend-engine, where SQL computes and the database raises
    (B7, class A).
- With 0 measured cases, A3 costs 0.5 day. The plan of record already offered A3 as Phase 2's end choice (21:30): "word
  such a site with lite's sentence and no value (a register row)". So does "A1 deferred until a first case". Present
  A1 against A3 with these facts. I lean A3 now, but the owner may prefer exactness.

**(b) The kind-B rows.** Rule (b0) points the right way, but needs items 2.8 and 2.9 above. Row by row:

- **R1 CHANGE: the cost is knowable now, from upstream's own manifests.**
  - DuckDB: `testDateTimeToString`, `testDateTimeWithTimezoneToString` and `testSimpleDateToString` (13 cases) flip
    PASS to FAIL. legend-engine expects "actual: '2014-01-01 00:00:00'".
  - H2: the same three, plus `testBooleanToString` ('TRUE'), because §3.2 extends the cast to Boolean.
  - Both channels move, because channel B erases the eval.
  - **R1 also breaks (b0)'s "no boundary marker".** A DateTime `toString` *outside* an eval in a channel-B body would
    become legend-engine's cast, a move legend-engine does *not* share (its outside part runs in legend-pure). Today
    the edge's SQL print is legend-pure's, so it is correct on both sides of the boundary. Measure this before ruling
    R1.
- **B6 KEEP rests on H2's evidence alone.** DuckDB's legend-engine form is `cast(%s as float)` (:257), and in DuckDB
  `FLOAT` is a 32-bit REAL; lite's edge casts to DOUBLE. Read it per database, as B8 and B10 are.
- **B10** is confirmed per database: DuckDB writes `cast(… as decimal)`, which is DECIMAL(18,3) (its manifest's
  `testParseDecimal`); H2 writes `decimal(5,2)`.
- **B11's upstream half is readable now:** `parseJson` is pushed (DuckDB strict `cast(%s as JSON)`, H2
  `legend_h2_extension_json_parse`), so in a query it is kind 1.
- **R3:** in a query, legend-engine's `group_concat`/`string_agg` skips NULLs, which is the product's form. So the "in a
  query" half is a deletion, not a row.
- **Missing:** an answer comparison of formatDate and joinStrings against the product.

**(c) The test plan's words.**
- Missing option **C4: PCT HOST-only.** Port the 36 assert-and-shape combos into core, then delete the edge's database
  judge, as decided at 02:30. That removes `UpstreamVerdictWords`, the edge `VerdictWords` form, the "declines a
  presented side" rule and the edge judge-plan pins together. C1 builds an interim state that is already scheduled
  for deletion.

**(d) The safety net.** D1 is right in direction (GUARDS-5, GUARDS-6, ARCH-5). Corrections:
- Fix the premise (§1).
- The order rule. ORDER BY with ties is nondeterministic, so keep the order only across distinct sort keys (or check
  sortedness plus the multiset). `EdgeNormalize` sees only TSV lines, so the recorder must write the "sorted" flag.
- Add H2 and SQLite channel B to the net.
- Keep host parity (TeaVM and native-image) out of the per-push lane.
- Add planted order and message changes.
- 1 day is really 2-3.

**(e) Import.**
- E1 is sound, and the design says that it departs from the approved program twice (program §6: "a conditional node
  becomes a planned branch"; re-planning was "the likely answer").
- The node table omits nodes that legend-engine plans commonly carry:
  - `FunctionParametersValidationNode` (any parameterized plan);
  - `RelationalRelationDataInstantiationExecutionNode` (relation-API results);
  - `LimitExecutionNode`, `PlatformUnionExecutionNode`/`PlatformMergeExecutionNode`, `RelationalBlockExecutionNode`
    and `AggregationAwareExecutionNode`.
- Refusing by default is safe, but done-when 5 could be vacuous. S9 should start by generating legend-engine's plans
  for the measured queries and listing their node types.

**(f) The envelopes.** Sound; the only-caller path is confirmed. One gap: the corpus is LEGEND_PURE but needs the
`ENGINE_*` forms, so the family table must say which family's writer presents those forms inside a LEGEND_PURE plan.

**(g) The plan format.**
- "Two bumps" contradicts the slices. `TdsResult.presented` (S4, S6) changes v59 after S1, and `Presented` changes v33
  after S3.
- Once S1 generates `PLAN_VERSION`, bump per slice. Never amend what a version means.
- The readers list misses the shared raise fixture (`LEGEND_LITE_RAISE_FIXTURE`, read by `python/tests/test_planned.py`
  and the tab) and `plan-runner.ts`'s consumers (`datacube`, `query`).

**Hidden decisions to add to §4:**
- H1: lite's sentence out of the product plan (21:20 item (2)).
- H2: operand carriage (21:20 item (5)).
- H3: PCT HOST-only (02:30).
- H4: a closed product enum against an opaque id or `Extension`.
- H5: rows on databases legend-engine does not support.
- H6: presenting kind-2 roots where legend-engine refuses them.
- H7: rule 1 scoped by answer.
- H8: host wording mechanism for LITE.
- H9: the version-bump policy.

## 4. The phased plan (§5)

The order is right: the net first, export and import last. Changes:

- **S0 should also install G3 (one plan) and G1 (the census) in report-only mode**, so each slice proves its own shrink
  instead of waiting for S7.
- **PCT HOST-only (C4) goes before S3.** That shrinks S3 and simplifies S4.
- **Split S5** into S5a (measure the moves, then the user rules) and S5b (build). A ruling wait should not sit inside a
  slice.
- **Add a runner-tooling slice before S8.** `tools/engine-runner` today is a Testable runner: parse, compile, Testable.
  It has no mode to execute a `SingleExecutionPlan` JSON or to run `PlanGenerator`. Its `TestRuntimeBuilder` swaps every
  connection for a local H2. And it needs a Bazel session, which this container lacks. Consider S8 and S9 a second
  phase.
- **Widen done-when 4 and 5's evidence.** The pure/v1 set is 14 requests with no parameters, 5 of which refuse. Add
  parameterized service queries from the corpus.

**Sizes.**
- The design: 16 days.
- My estimates, per slice:

  | Slice | Estimate | Why |
  |---|---|---|
  | S0 | 2.5 days | corpus messages need the corpus lanes |
  | S1 | 3 days | three readers, wasm and native exports, a Python generator |
  | S2 | 2 days, plus a round | error transport on 12 databases |
  | S3 | 1 day (0.5 with C4) | |
  | S4 | 3.5 days | |
  | S5 | 2.5 days, plus the ruling | |
  | S6 | 1 day | |
  | S7 | 2.5 days | 43 product files reference `PlanStyle`, plus the guard |
  | Runner tooling | 3 days | |
  | S8 | 2.5 days | |
  | S9 | 4 days | |

- In all: **about 27 working days**, plus the rulings and the lane latency.
- If the owner budgets 3 weeks, S0-S7 fit (about 18-19 days); export and import do not.

## 5. The guard (§6) against the census's known failures

- **Vocabularies derived.**
  - G1 derives text-producing MIR from `SqlTyping.callType`, which is good. But a scan of `com.legend.edge..` misses
    *indirect* construction through product helpers, which is EDGE-1's own path: `RelationTextLowering` calls
    `LiteralSpelling.floatPrint`, which writes `FLOAT_TO_TEXT` in the product. Follow calls into product builders.
  - G2's vocabulary from sentinel-operand calls yields short fragments (" by zero", "expected: "). It will match
    legitimate data. Channel-B test plans carry legend-pure's sentences as `assertError` expected values, and the SQL
    text carries the body's literals. Scan only planner-written metadata fields (sites, failures, words), or exempt
    strings that appear verbatim as source literals.
- **Every rule matches at least one site.**
  - Only G1 checks this.
  - G2, G4 and G5 need planted violators.
  - G3's "a row attributed nowhere for 30 days" depends on wall-clock time (G10). Replace it with "a row that
    attributes no difference over the measured suites fails".
- **Both ways.**
  - G1 and G3 check both ways.
  - G2's check is tautological: its vocabulary is generated from the presenters, so "every entry is produced by a
    presenter" always holds.
- **Kind C, D and input sites.** G1 says "anything else fails", yet sites that stay build text MIR:
  - the plan-text reshapes (`PlanEnumForm.apply`, `InProtocol.apply`, `EngineStyleH2.planned`);
  - `RawSqlBoundary`, `TestDataFetches`, `CompiledPureVerdictNumbers`;
  - the C1/C2 constants;
  - the future `PlanImport`.

  Either they go into a second ledger (KIND_C, INPUT, JUDGE) with the same rules, or G1 is red at its end state, or
  they get mislabelled as kind B.
- **The product source is left unguarded.** Deleting `CompatSpellingGuardTest` in S7 removes the only scan of product
  Java for upstream text: G2 scans plans and G4 scans hosts. Keep it, with GUARDS-3's derived vocabulary.
- **Hosts.**
  - G4 covers `engine-client/src` and `python/legend_lite` only. `datacube` and `query` read plans and errors (for
    example `datacube/src/page-document.ts:1044`). Include them or justify leaving them out.
  - Make the `['message']` check schema-based, not lexical.

## 6. Fidelity (§8)

**What is concrete:**
- most slices cite pinned files and lines;
- the blob-less-clone method works: I used it for both tags;
- the rule "no ruling on a recalled answer" is right.

**Gaps:**
- **R1's and rule 3's PCT consequences are already in upstream's 77 relational manifests.** Read them now and use them
  per row as the oracle for which moves legend-engine shares, rather than discovering the moves in S5.
- **Per-database readings.** B1, B6 and B8 cite H2 only; DuckDB's forms differ for B6. And there is no source at all
  for the databases without an extension (2.8).
- **S4 must read the harness, not only legend-pure's print.** `TestTDS.java:209` (`.SSS` DateTime cells in TDS text,
  homework README §7) is the PCT relation suite's spelling.
- **The engine-runner dependency.** S1's pure/v1 classes (F43/F44), S6 and S8/S9 depend on the runner, which needs
  modes that do not exist and a Bazel session. Schedule it as work, not as a reading step.

---

## Corrections the designer should make before the owner rules

1. **Surface the 21:20 re-rulings as decisions:**
   - (H1) removing lite's own sentence and `raises` from the product plan, and the hosts no longer reading the site
     table;
   - (H2) A1 re-introducing operand carriage and parsing in exec.

   Give each a "keep as built" option.
2. **Align (c) with the plan of record's 02:30 decision.** Add C4 (PCT HOST-only after porting the 36 combos; delete
   the edge's database judge) and order it before S3.
3. **Fix §2.4 and §2.6.6.** Channel-B outcomes already pin every failing row's message (9 on DuckDB), the ledger pins
   295 substrings, and channel A pins its ERROR messages. Only the corpus (62 + 78 FAIL rows) and channel B on H2 and
   SQLite lack them. Re-scope S0's item 4.
4. **Quantify R1 from upstream's manifests now:**
   - DuckDB: 3 tests (13 cases) flip PASS to FAIL in both channels;
   - H2: those plus `testBooleanToString`.

   Then state R1's interaction with (b0); the outside-eval channel-B moves are not legend-engine's. Fix §7's
   contradictory "any channel-A move is a misclassification".
5. **Restate rule 1 (§3.2) by answer, not by SQL**, add the sub-rule for databases without a legend-engine extension,
   and list formatDate and joinStrings (kind 1) with an answer comparison.
6. **Re-read B6 per database** (DuckDB `cast as float` = REAL, lite DOUBLE). Record B11's evidence (parseJson pushed).
   Recast R3's in-query half as a deletion.
7. **Decide the product records' upstream names** (`Presentation`, `Presented.Form`): an opaque id or an `Extension`,
   or rule the enum explicitly against `EdgeSeparationTest`.
8. **The product plan changes in S1**, so its pins move: state that the `lean_sql_*` `plan` column moves, or write
   `Fixed` operands only in edge plans. Correct §5 and done-when 6.
9. **The format:** drop "two bumps"; generate `PLAN_VERSION` in S1 and bump per slice. Add the raise fixture and the
   `datacube`/`query` consumers to the readers list.
10. **Rework the guard:**
    - G1 follows calls into product builders, and gets a second ledger for kind-C, kind-D and input sites;
    - G2 scans planner metadata only, or exempts source literals;
    - G3's 30-day rule becomes deterministic;
    - G4 is schema-based and covers `datacube` and `query`;
    - G5 mirrors THE WALL transitively;
    - every rule gets a planted violator;
    - `CompatSpellingGuardTest` is kept, with a derived vocabulary.
11. **Bound the writers' evaluation:** a closed list of presentable kind-2 roots, a rule for nesting, and a note that
    channel-A kind-2 roots are a better-than-upstream divergence to register.
12. **Prove ServiceLoader discovery** in the TeaVM engine module and the GraalVM native-image, or have the composition
    roots pass their family in explicitly.
13. **Correct the numbers:**
    - "~9,400" becomes 5,541 measured, and §2.1's "export's base" must agree with §3.9;
    - `objectReferenceConnection` is kind C throughout;
    - "≈950 deleted" excludes PurePrintForms' KEEP passes.
14. **Re-size to about 27 days.**
    - Split S5 into a measure-and-ruling half and a build half.
    - Add a runner-tooling slice: plan-JSON execute, `PlanGenerator`, a seeded shared database.
    - Present S8 and S9 as a second phase if the budget is 3 weeks.
    - Put G1 and G3 in report-only mode in S0.
15. **Import and export scope:**
    - Add `FunctionParametersValidationNode` and `RelationalRelationDataInstantiationExecutionNode` to §3.10, mapped
      or explicitly refused.
    - Begin S9 by listing the node types of legend-engine's plans for the measured queries.
    - Widen done-when 4 and 5 beyond the 14 parameterless pure/v1 requests.
    - Say how activities are compared, given that legend-engine substitutes values.
16. **For (a):** state that A1's error transport is unmeasured on every database (it is not the probe's value
    carrier), that it does nothing for LEGEND_ENGINE, and that A3 was the plan of record's promised end choice. Then
    let the owner choose.
