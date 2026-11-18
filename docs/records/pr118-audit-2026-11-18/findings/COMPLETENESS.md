# COMPLETENESS: verdict INCOMPLETE (no blockers)

## Summary (at most 12 lines; answer your brief's questions first)

1. **Own inventory (edge off):** core's Java is nearly clean of meaning the census does not list or rule. The gaps are where no guard reads: the TS and Python product (findings 1, 4, 10), the prelude's Pure bodies (2), and DataCube's product queries shaped for legend-engine on every planner (3).
2. **Against theirs:** findings 1-4, 7 and 8 are in no census row, audit item, follow-up or register row. J20 (substring, indexOf: 5) and J21 (the engine grammar level: 6) are counted as closed but are really "ruled to stay", on reasons that run against the program's own S365/S370 rule. Nothing theirs counts is unreal (J30 withdrawn correctly; K1 and J43 really closed).
3. **Follow-ups:** none is a blocker under the narrow definition. F32 and F33/D6 were promised "before landing" and are not done; neither adds a wrong answer or weakens a guard of record.
4. **Plan format:** no portable-plan field holds an upstream spelling, envelope or edge-only record (K10, J31). Presentation-shaped but lite's own and cheap to drop: `Column.precision`'s three time values (edge-read only), `Site.message`, the test plan's `*SizeWords` and `words`. Not blockers.

## Blockers (if any; each one also appears in Findings)

None.

## Strengths

- **Plan records are product-built.** No `ExecutionPlan` or `TestPlan` record is built only by the edge (script count below: every record type is constructed in the product; the edge builds `Column`, `Site`, `Array`, `Key`, `Setting`, `Statement` and `Held` alongside). No `SqlExpr` or `SqlSource` variant is built by the edge alone. `SqlDdl.DropSchema` is the only MIR record without a product constructor, and it is ruled READ_API (`EdgeOnlyMembersTest.java` READ_API "MIR DDL").
- **The plan carries no upstream words.** `ExecutionPlan.Site` holds a kind, a where and lite's sentence (`core/src/main/java/com/legend/executionplan/ExecutionPlan.java:843-866`). `PlanJson.VERSION = 58` refuses a v57 plan whose sites carried upstream words (`PlanJson.java:168-171`). `SqlExpr.PlanParam` has no FreeMarker or template field (`sql/SqlExpr.java:772-792`, J31).
- **No product SqlFn node is edge-only.** Every SqlFn constant the edge emits is also emitted by non-dialect product code (script below; `FLOAT_SHORTEST_TEXT` is emitted by `lowering/LiteText.java:56`, so K1 is closed for real).
- **No flag or system property in product main selects upstream behaviour.** The 36 `getProperty`/`getenv` sites are debug traces, the port and the bind address (grep below).
- **The product's two-form answers are lite's.** `lowering/ProductLoweringForms.java:26-92` and `database/PlanStyle.java` `PRODUCT` answer lite's form or refuse by name. The facets throw `NotImplementedException` naming the edge (PlanStyle.java:121-125).
- **Most host items closed in code.**
  - Python's product holds only a refusing seam (`python/legend_lite/_edge_seam.py:47-60`, `NoEdgeRoutes` answers 404).
  - `LegendError` carries lite's kinds (`compiler.py:37-45`).
  - H3: `engine-client/src/legend/browser-engine.ts` is lite's API v1.
  - H4: Python's errors are lite's kinds.
  - H6: saved queries are on lite's route.
  - H9: `_raised.py:7`.
- **Rulings are written down.** S361-S370 each record legend-engine's answer, lite's answer and the user's dated words (`docs/SEMANTICS_REGISTER.md`).

## Findings

### COMPLETENESS-1 [WRONG-DESIGN] [FIX-AFTER] TypeScript product records read legend-engine's shapes, and only the edge calls them
- Where:
  - `engine-client/src/relation-type.ts:66-80`: `relationColumns` reads legend-engine's `RelationType` JSON (`columns[].genericType.rawType.fullPath`, the answer of `lambdaRelationType`).
  - `engine-client/src/relation-type.ts:89-104`: `tdsColumns` reads generatePlan's `rootExecutionNode.resultType.tdsColumns`.
  - `engine-client/src/values.ts:108-118`: `timestampFromText` reads legend-engine's `2024-01-02T03:04:05.123456000+0000`.
- What: these files are in `_RECORDS`, the product layer (`engine-client/BUILD.bazel:39-60`). Their only product-side callers are edge files:
  - `engine-client/src/engine-remote.ts:78` and `:178`;
  - `datacube/src/edge/upstream-planner.ts:135` and `:187`.
  The rest is tests.
- Why it matters: in Java, `EdgeOnlyMembersTest` counts exactly this ("a public product member only the edge uses"). TypeScript has no such check, so these readers stand in the product layer uncounted. H10 moved only the TinyInt half of this file.
- Instead: move the three functions into the `_EDGE` files (`engine-wire.ts` or `engine-remote.ts`). Give `TsLayeringTest` an export-level rule: an export of a product module whose only importers are `_EDGE` modules is counted.
- Confidence: verified (callers enumerated with `grep -rn 'relationColumns\|tdsColumns\|timestampFromText'` over engine-client, datacube, query and studio src).

### COMPLETENESS-2 [WRONG-DESIGN] [FIX-AFTER] The prelude still ships legend-pure's print bodies (J27's twins), and the product writer prints a Pair as `<first, second>`
- Where:
  - `core/src/main/resources/com/legend/builtin/prelude.pure:16-24`: Pair `toString` `['<', first, ', ', second, '>']`.
  - `prelude.pure:31-38`: List `toString` `joinStrings('[', ', ', ']')`.
  - `prelude.pure:3542-3550`: SQLNull `toString(){'SQLNull'}`.
  - `prelude.pure:4950-4975`: TDSColumn `toString` `'meta::pure::tds::TDSColumn(name=...'`.
  - `core/src/main/java/com/legend/result/CellText.java:89-94`: the product writer's Pair arm, "the prelude's Pair.toString()".
  - Pinned as product output by `core/src/test/java/com/legend/ResultWritersTest.java:177` (`"<1, a>"`) and `testplan/LiteralsParityTest.java:75` (`"<a, b>"`).
- What:
  - J27 deleted only TDSNull's `toString(){'TDSNull'}` (PR diff of prelude.pure). Its reasoning applies to the other bodies unchanged: an upstream print form shipped as a Pure body, against the world-map rule "declarations, no other Pure bodies".
  - These bodies run on the product path: `Scalars.java:2867-2870` ("the prelude module carries the spec bodies and the typer's derived route runs them"), and `UserCallInliner.java:676` ("the module's Pair.toString body running").
  - `MongoDbDifferentialTest.java:267` plans `pair('a','b')->toString()` on the product.
- Why it matters:
  - The user's product print forms (`docs/TOTAL_WALL_START_HERE_2026_11_11.md` §5.4: "list: a JSON array"; "Objects and relations are refused") and S363 ("legend-pure's forms stay at the edge") say otherwise.
  - So the product writes legend-pure's form for a Pair, for a `List` instance (suspected, not run) and for `sqlNull()`.
  - No guard sees it. `CompatResourceTest` scans the prelude, but only for the closed `LITERALS` list, which holds `TDSNull` and none of `'SQLNull'`, `'<'` or `', '`.
- Instead:
  - Delete the four bodies (declarations only, as J27 did).
  - The product answers lite's form: a List as its JSON (S363), a Pair or another instance refused by name (S314) or written as JSON.
  - legend-pure's bodies become the edge's (PureOnlyTexts or Presenter).
  - Then re-pin `ResultWritersTest:177`.
- Confidence: verified for the bodies, the CellText arm and the pinned tests. Suspected for the exact product answer of `^List(...)->toString()` (not run).

### COMPLETENESS-3 [WRONG-DESIGN] [FIX-AFTER] DataCube's product queries and offers work around legend-engine's defects on every planner
- Where:
  - `datacube/src/offers.ts:37-45`: S23, "so these four are not offered on a boolean, on any planner". `lessThan`, `lessThanEqual`, `greaterThan` and `greaterThanEqual` against a Boolean value are withheld because legend-engine types BIT as TinyInt.
  - `datacube/src/query.ts:319-329` and `datacube/src/grid/columns.ts:63-66`: S24, "ENGINE GAP". A window with no partition gets a constant `__all__` partition column, because legend-engine registers no unpartitioned `over`. lite accepts `over([], ...)`.
  - `datacube/src/grid/columns.ts:50-60`: `ROOT_COLUMN`, a constant group key, because `groupBy(~[], ...)` "crashes the real engine".
- What: the product planner (`lite-planner.ts`) gets queries shaped for legend-engine and loses four operators. The user ruled this on 2026-10-01 (S24: "engine gaps compensated in DataCube and recorded"), before the edge program. H10 moved only the TinyInt reading (`relation-type.ts:56`); `offers.ts:37` still says "delete with relation-type.ts's compensation".
- Why it matters: under K2 ("Product root, edge opt-in") and the meaning rule, these are upstream meaning in the product: the product's query text and its menu change because of legend-engine. The census, the audit list (H1-H11) and the follow-ups do not list them.
- Instead: put S23 and S24 back to the user under the edge rule. Then:
  - gate the operator withholding and the `__all__` and `__root__` shapes on `datacube/src/edge/upstream-planner.ts`, the legend-engine planner;
  - the lite planner writes `over([], ...)`, `groupBy(~[], ...)` and offers all operators.
- Confidence: verified (code and register rows read). Behaviour on lite's planner is not run.

### COMPLETENESS-4 [WRONG-DESIGN] [FIX-AFTER] Python's product plan record reads legend-engine's `relationalType`
- Where: `python/legend_lite/_plans.py:25-30` (`Column.sql_type`, "a server's `tdsColumns` carry `relationalType`").
- What: `relationalType` is written only by the edge (`core/src/main/java/com/legend/edge/PureV1Routes.java:374`, `PureV1Writer.java:109`, `HostAnswers.java:416`). Nothing in `python/`, `datacube/test` or `engine-client` reads `sql_type`.
- Why it matters: it is dead product code whose only meaning is an upstream field. It is small, but no Python meaning guard exists to catch the next one (finding 10).
- Instead: delete `sql_type`.
- Confidence: verified (`grep -rn 'sql_type' python datacube/test engine-client` gives only the definition).

### COMPLETENESS-5 [WRONG-DESIGN] [NOTE] The product's default substring and indexOf are legend-engine's relational semantics, not Pure's (S130, S134, before the program)
- Where:
  - `core/src/main/java/com/legend/lowering/Scalars.java:1489-1497`: `substring` passes `(start, end)` verbatim as SQL `substring(start, length)`; the corrected rule applies only under `Feature.CORRECT_SQL_SUBSTRING_INDEXING` (`platform/Feature.java:36-44`).
  - `Scalars.java:1557-1566`: indexOf is a 1-based `strpos`, 0 on a miss.
  - `Execution.java:43-57`: the default door passes no feature flags.
- What, with concrete inputs:
  - `'abcdef'->substring(1, 3)`: the product sends `substring('abcdef', 1, 3)` and answers `abc`; Pure answers `bc`.
  - `'abc'->indexOf('b')`: the product answers `2`, Pure `1`; on a miss the product answers `0`, Pure `-1`.
  - Both are pre-existing on main (`git show main:.../Scalars.java` line 1437).
- Why it matters:
  - This is exactly upstream meaning in the product (edge off).
  - J20 closed it as "substring and indexOf the rows ruled before the program" (`EdgeAuditOpenTest.java:121-123`).
  - The program's own later rule (S365, S366, S370: "the product follows Pure's own definition ... legend-engine's quirk stays at the edge") points the other way.
  - It is user-ruled and ledgered, so it is not a defect of this PR.
- Instead: ask the user to re-rule S130 and S134 under the edge rule, and if confirmed:
  - make both `LoweringForms` members, as isNumeric is;
  - the product answers Pure's 0-based form;
  - the edge answers the engine's form; the corpus and the engine-runner routes keep their answers.
- Confidence: verified in the lowering and the door; the default feature set read from `Execution.run`.

### COMPLETENESS-6 [WRONG-DESIGN] [NOTE] legend-engine's grammar level is in the product parser, and the level-less public entries default to it
- Where:
  - `core/src/main/java/com/legend/parser/GrammarLevel.java:32-46` (`LEGEND_ENGINE`, `refusesLiteExtensions`): 37 branch sites over 9 parser files.
  - `parser/SpecParser.java:318-320,337-339`: `parseLambda(String)` and `lambdaJson(String, boolean)` default to `LEGEND_ENGINE`.
  - `parser/PmcdParser.java:47`.
  - `door/Parse.java:45-49,59-64,84-89` (`engineRecords`, `engineDocumentJson`, `engineLambdaJson`).
  - Ruled at `core/src/test/java/com/legend/EdgeOnlyMembersTest.java:336-347` (`RULED_J21`).
- What: the product parser computes legend-engine's refusals of lite's extensions, and only the edge asks for them (`ArchitectureTest.legendEnginesGrammarIsTheEdgesAlone`). The ruling's reason quotes the user's "parse legend-pure and legend-engine projects as-is". That requirement is met by `LEGEND_LITE`, which accepts upstream's grammar (J42, S364). The engine level exists to reproduce upstream's refusals for pure/v1 clients.
- Why it matters: under the meaning rule this is a product method computing upstream's alternative for the edge. The ruling cites a requirement it does not serve. The level-less default is also inverted: the product's own level should be the one that needs no argument.
- Instead: keep one parser (G1), but make the refusal of lite's extensions an edge-side check over the parsed records. Or at least make the level-less entries `LEGEND_LITE`, and re-word RULED_J21's reason as "compat refusals, ruled to stay" for the user to rule.
- Confidence: verified.

### COMPLETENESS-7 [WRONG-DESIGN] [NOTE] Parser and protocol emitter reproduce legend-engine's span and emit quirks unconditionally
- Where:
  - `core/src/main/java/com/legend/parser/DatabaseProtocolParser.java:450-454`: milestoning spans "+1 on BOTH span columns", legend-engine's double-added anchor.
  - `DatabaseProtocolParser.java:766-769`: the left operand's span swallows the operator and the right operand.
  - `protocol/ProtocolEmitter.java:2841-2847`: `package=::` is written as legend-engine's harness writes it.
- What: these are "ENGINE QUIRK" reproductions at every grammar level. They reach the product's protocol JSON when source information is asked (`door/Parse.java:52-57`, `documentJson(source, true)`). S369 and J35 covered only the no-spans case.
- Why it matters: the product's model JSON carries an upstream defect's offsets. It is in no census row or audit item.
- Instead: compute lite's own spans in the product, and apply the quirks in the edge's `EngineSpans` when the edge asks.
- Confidence: verified that the code exists; suspected that it reaches a product answer (not traced end to end).

### COMPLETENESS-8 [WRONG-DESIGN] [NOTE] Two product orders and names follow legend-engine with no row
- Where:
  - `core/src/main/java/com/legend/test/PureTests.java:107,111-129`: test discovery sorted by `engineSuiteOrder`, legend-engine's `PureTestBuilder.buildSuite` order.
  - `core/src/main/java/com/legend/lineage/ScanRelations.java:1450-1452,1536-1538` and `compiler/spec/LineageTreeLines.java:149`: scanRelations' labels use legend-engine's internal SQL aliases (`unionAlias`, `"joinleft_"`, `root`, `SQLNull`).
- What and why: the lineage labels are the value of an upstream function (follow-up C8 covers "lineage upstream's way"). The test order is the product's own runner borrowing upstream's order. Neither is in the audit list.
- Instead: record both as STAYS with a reason, or give the product its own order and labels.
- Confidence: verified.

### COMPLETENESS-9 [WRONG-DESIGN] [NOTE] Plan format: presentation-shaped fields (the brief's item 4)
- Where and what:
  - (a) `ExecutionPlan.Column.precision` (`executionplan/ExecutionPlan.java:697-737`; JSON `precision`, `PlanJson.java:711`):
    - the product writers read only `DAY` (`result/CellText.java:288`) and publish the name (`result/ArrowWriter.java:113-114`);
    - the three time values `WRITTEN`, `INSTANT` and `MINIMAL` are told apart only by `edge/Presenter.java:239-246`, for legend-pure's and legend-engine's print forms.
  - (b) `ExecutionPlan.Site.message` (`ExecutionPlan.java:843-859`): lite's sentence rendered at planning. It is derivable from `kind` and `where` (`Site.sentence`). It is presentation text in the plan, though lite's own.
  - (c) The test plan's `TestPlan.answerSizeWords`, `SideShape.sizeWords` and `TestPlan.words` (`testexecutionplan/TestPlan.java:44-47`, `SideShape.java:31-33`). Under the edge style they carry legend-pure's sentence and the edge's word-family name. `SideShape.oneCarrier` ("one carrier in the engine's Result.values") and `tdsNullRoot` name upstream shapes.
- Why it matters: the next program is "one plan, two presentations". (a) is a fact, not a spelling, and fits that design: the edge writer reads it. (b) and (c) are words in the plan. The portability program should word them at the writer.
- Not blockers: no field is an upstream spelling or envelope, and all are cheap to remove. A plan of another version is refused whole (`PlanJson.java:168-171`), so a removal is a version bump, PlanJson's own practice (it is at version 58).
- Instead: in the portability program, drop `Site.message` (word at the writer from kind and where). Keep `precision` as a fact, documented as both writers' input. Leave the test plan's words to its own decision (test plans are not the export or import target).
- Confidence: verified (readers enumerated with `grep -rn 'Precision\.\(WRITTEN\|INSTANT\|MINIMAL\|DAY\)'`).

### COMPLETENESS-10 [BROKEN-RULE] [FIX-AFTER] No meaning guard reads the TypeScript or Python product, and core's scans match a closed vocabulary
- Where:
  - `core/src/test/java/com/legend/CompatSpellingGuardTest.java:489-505` (`scan`: `.java` under `core/src/main` only).
  - `CompatResourceTest.java` and `CompatConstantPoolTest.java` (`:product_jars` = core targets, `core/BUILD.bazel:2675-2682`).
  - `EdgeOnlyMembersTest` (Java classes only).
  - `tools/deps/ts-layers.txt`: TS deps only, no meaning rule.
- What: the audit's C.8 asked for "Scans of resources, the hosts' Java, Python and TypeScript". Resources are scanned, but only for listed spellings. The hosts are unscanned: 390 files and 137,513 lines (measurement below). `LITERALS` is a closed list (`CompatSpellingGuardTest.java:54-101`), which its own javadoc admits ("a fixed list catches only what is on it").
- Why it matters: findings 1-4 are exactly what this gap hides. G9 ("a guard for every goal") holds for core's Java only. This is a guard gap, not a guard weakened by this PR, so it is not a blocker.
- Instead:
  - a TS and a Python twin of `EdgeOnlyMembersTest` (an export or function of a product module imported only by edge modules is counted);
  - a host scan with the same vocabulary;
  - for the prelude, a rule that a platform class's `toString` body is refused, as J27 did for TDSNull.
- Confidence: verified (guard sources read).

### COMPLETENESS-11 [BROKEN-RULE] [NOTE] Four terminal STAYS rows rest on the orchestrator's ruling, not the user's
- Where: `core/src/test/java/com/legend/CompatSpellingGuardTest.java`:
  - the REGISTER rows `compiler/spec/LambdaBodies.java | Assert failed`, `compiler/spec/VerdictQueries.java | Assert failed` ("the orchestrator's, not the user's");
  - the rows `testdatagen/TestDataPlanner.java | createTableRowIdentifiers(` and `| createRowIdentifier([` ("the orchestrator's ruling of 2026-11-17 (not the user's; put to the user)").
- Why it matters: AGENTS.md's upstream-fidelity tenet: "a row the user did not rule is a defect too". These are upstream texts kept in the product on the orchestrator's word alone. They are frozen by `TerminalRowsFrozenTest`, so a later session will read them as settled.
- Instead: put them to the user, in one line each, in the merge decision.
- Confidence: verified (comments in the register).

### COMPLETENESS-12 [COSMETIC] [NOTE] Product text that names legend-engine, and stale comments that say the product serves pure/v1
- Pattern: product sentences and identifiers name legend-engine where lite's own words would do, and comments describe routes that moved.
- Product sentences and identifiers, 4 sites:
  - `datacube/src/cube.ts:552`, `datacube/src/runner.ts:237`, `datacube/src/ui/timeseries-view.ts:1765`: "this grid's planner is legend-engine's, which has no #TS";
  - DataCube's planner method name `lambdaRelationType`, legend-engine's endpoint name (`datacube/src/runner.ts:300`, `planner-types.ts:25`).
- Stale comments, 5 sites:
  - `python/legend_lite/_home.py:133,140` ("Python's engine serves them at /api/pure/v1/query");
  - `datacube/src/services/python-host.ts:29,656`;
  - `query-store/src/wire.ts:1`.
- Instead: "this grid's planner has no #TS"; refresh the comments.
- Confidence: verified.

### Follow-ups (`docs/records/edge-follow-ups-2026-11-18.md` on `origin/claude/total-wall-orch`): blocker check
None is a blocker under the rubric's narrow definition.
- **F32** (internal natives resolve by full name):
  - `cellText` and `typeRaise` are new in this PR (absent from main's Pure.java); `trustOne` is pre-existing.
  - `INTERNAL_DESUGAR` still refuses bare names only (`builtin/Pure.java:632-646`, `platform/CoreFn.java:464`).
  - It was planned "before landing" and is not done. `trustOne` skipping toOne's guard predates the PR, and the two new natives answer lite's forms, so no new wrong answer was found. FIX-AFTER, first in the queue.
- **F33/D6** (the edge golden net is not in CI): it was planned "before landing". D's heavy round ran it on the final commit, so the merge itself is covered. After the merge, main's CI cannot see an edge regression. FIX-AFTER, urgent.
- **F47, F48, F11, F12, F30** are pre-existing value or edge-fidelity differences, not introduced by this PR (the follow-up text says so; F47 was not re-verified). FIX-AFTER.
- **F49** is fixed (`sql/dialect/SQLite.java:442`, `FloatAndDecimalTest.java:33`).
- **D2** (`decimal` digits with no reader) is lite's data, not an upstream form.
- **A4** (MIR `tree` in run plans) is a version-bumpable format change. Neither D2 nor A4 conflicts with the portability program enough to block.

## Cross-area observations

- **For HOSTS/GUARDS:** the TS layering guard checks dependencies, not meaning, so a product-layer export used only by the edge is invisible (finding 1). The same holds for Python (finding 4).
- **For the census auditor:** "closed" in `EdgeAuditOpenTest` covers three different outcomes, and the count reads 0 either way:
  - moved, e.g. J1;
  - ruled to stay with a reason that does not match the code's meaning (J21 in finding 6);
  - left as a pre-program row (J20's substring and indexOf in finding 5).
- **For EDGE / D1:** the prelude's legend-pure bodies (finding 2) are the platform-side half of D1. The edge's presenter should own legend-pure's `toString` forms, not the platform's Pure.
- **DataCube's S23/S24 workarounds** (finding 3) are a product query-shape change. The lean-SQL and answer lanes would not flag them, because the answers are equal.

## Measurements (numbers with the command that produced them)

- **Product inventory:** 1,524 files, 538,043 lines. Built with `find core/src/main/java -name '*.java' -not -path '*/com/legend/edge/*'`, plus core resources, base, json, `wasm/src/main`, `native/src/main`, `python/legend_lite` minus `edge/`, `datacube/src` minus `src/edge`, and `engine-client/src` minus `_EDGE` (list at `/tmp/claude-0/-home-user-legend-lite/e46cefdc-2e0e-59c4-80ed-3422bd6cbcd6/scratchpad/audit/verify/compl/product_files.txt`); measured with `xargs cat | wc -l`.
- **String literals extracted from the product:** 117,386, by `/tmp/claude-0/-home-user-legend-lite/e46cefdc-2e0e-59c4-80ed-3422bd6cbcd6/scratchpad/audit/verify/compl/scripts/lits.py` (comments stripped).
- **Product literals holding these spellings:** `TDSNull` 11 (all the Pure type's name, as the register says); `executionTraceID`, `Assert failure`, `Cannot divide`, `Invalid provided`, `Missing external` and `tdsBuilder` 0 each; `unionAlias` 3, all in lineage (finding 8).
- **Spelling-guard coverage:**
  - core Java scanned: 1,130 files;
  - core resources scanned against the closed list: 4 files, 9,818 lines;
  - hosts not scanned for spellings: 390 files, 137,513 lines;
  - command: `grep -c '^core/src/main/java' product_files.txt`, then `grep -v '^core/'` piped to `xargs cat | wc -l`.
- **Prelude `toString` bodies left:** 5 (lines 20, 34, 3546, 4729, 4950; the one at 4729 delegates to a function field), with `grep -n '^\s*toString()\s*\({\)\?\s*$\|^\s*toString(){' prelude.pure`. 4 of them are print forms.
- **Edge-only callers of the TS records readers:** `relationColumns` 2, `tdsColumns` 1, `timestampFromText` 1, all `_EDGE` files (`grep -rn` over engine-client, datacube, query and studio src).
- **SqlFn nodes emitted only by the edge (outside dialect renderers):** 0 of 189 (Python script over `sql/SqlFn.java` and every main `.java`).
- **MIR records built only by the edge:** `SqlDdl.DropSchema` (READ_API ruled). The other hit, `SqlExpr.Frame` in `EngineSpans.java`, is a name clash. Same script.
- **ExecutionPlan and TestPlan records built only by the edge:** 0 of 58.
- **`getProperty`/`getenv` in product main that select upstream behaviour:** 0 of 36 sites (`grep -rn 'System.getProperty\|Boolean.getBoolean\|System.getenv\|Integer.getInteger'`).
- **Engine-level grammar branch sites in the product parser:** 37 (`grep -rn 'refusesLiteExtensions\|== GrammarLevel.LEGEND_ENGINE\|LEGEND_ENGINE)' core/src/main/java | grep -v /edge/ | wc -l`), over 9 files.
