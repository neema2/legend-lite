# PRODUCT: verdict INCOMPLETE

## Summary (at most 12 lines; answer your brief's questions first)

1. **Meaning rule.** No: the product with the edge off still holds upstream meaning that the guards cannot see. It sits in string literals and status codes in three peripheral features. The query store keeps legend-engine's ApplicationQueryException statuses and Java class names, and lite's own `/api/lite/v1/queries` route serves them as `internal`/500 (PRODUCT-1). The test-data generator and CSV loader keep legend-engine's exact sentences (PRODUCT-2). legend-pure's "Assert failed" is the product's default message on two paths (PRODUCT-3). There are also 110 edge-only product members exempt by ruling, a few on reasons that do not hold (PRODUCT-7), and J21's legend-engine grammar level is ruled product (PRODUCT-8). The core planner, exec and result path is clean: the engine writers, trace comments, `+0000` and TDS tokens are gone.
2. **Hooks.** Criteria (a)-(c) hold for every kind I read. The design is not minimal, though: there are three overlapping mechanisms (direct PlanStyle methods, `twoForm`, two `facet` overloads) and several hook families outside the registry. The count tracks the D1 debt (SQL-level divergences), not the goal (PRODUCT-6).
3. **K10 and typed forms.** K10 is consistent in Java and in the hosts: a site holds `n, kind, where, message` and nothing else, and a read refuses any other key. S311 and S312 hold on the paths I read. Exceptions: a DateTime cell writes 9 digits where the value is finer than a microsecond, while `toString` writes 6 (PRODUCT-10). Errors-as-kinds is uneven: about 30 kinded compile sites against 481 free-text ones, `DataError` drops `where`, and database or connection failures come back as `data` (PRODUCT-9).
4. **Plan format.** v57 is type data and fine (it has no reader yet). v54 and v56 put lite's English sentence into the plan twice and validate it on read (PRODUCT-4). Separately, token-only raises cannot carry operand values back, so step 1 of the next program needs a raise-format change (PRODUCT-5). Neither is a blocker: undoing them is cheap before plan export ships.
5. **Quality.** The new lite API, the K2 mount and the result-library cut are good designs. Around them sits census-driven machinery: a fake `pageContent` key, a dead facet method, an edge-only Runner ruled READ_API, an edge-only flag carried through four layers, and stale upstream comments (PRODUCT-7, 11, 12).

## Blockers (if any; each one also appears in Findings)

None. The wrong answers I found are error classifications on malformed input and default assert messages (PRODUCT-1, 3, 9), not wrong values. The plan-format items (PRODUCT-4, 5) are cheap to undo while no plan export exists.

## Strengths

- **legend-engine's writers left the product result library.** `Output` is `JsonRows | Csv | Rows` (core/src/main/java/com/legend/result/Output.java:20-56). The engine and Arrow answers are edge receivers behind `Output.Rows`, told the plan through `ResultSink.planned` (result/ResultSink.java:31-43).
- **`CellText` keeps only product forms.** `engine`, `writeEngine`, `presentedName` and the Map's `{k=v}` text are deleted. The `Forms` hook has no default (result/CellText.java:44-74). Pivot names are bare in `CsvWriter` and `JsonRowsWriter`. `PureDateLiteral.toString` no longer prints `+0000` (values/PureDateLiteral.java:440-480).
- **The trace comment left the plan.** `ExecutionPlan.traceComment` is deleted. The runner binds the execution id alone (exec/PlanRunner.java:843-848).
- **Upstream-only MIR nodes left the plan format.** `DeferredTdsString` (with `renderTdsNull`), `TempTableInSplice`, `VarSetPlaceholder` and `PlanParam.enumMapFn` are gone (executionplan/MirJson.java diff).
- **K10 is consistent.** `ExecutionPlan.Site` has no upstream field (executionplan/ExecutionPlan.java:843). `PlanJson.readSites` allows only `n, kind, where, message` and checks the message against `ErrorKind` (executionplan/PlanJson.java:1424-1442). exec words a site by its `message` alone (exec/RaisedErrors.java:253-274). Python's `_raised.py` and TypeScript's `raised.ts` read no `upstream` field.
- **K2's server half is minimal and sound.** Lite's main mounts nothing by default. `--engine NAME` finds one `MountProvider` by ServiceLoader, and an unknown name is refused at boot (server/LegendHttpServer.java:583-627; server/MountProvider.java; core/src/main/edge_server/META-INF/services/com.legend.server.MountProvider).
- **The lite API is a clean design.** One error shape, one closed request reader (`LiteV1Api.read`) and one classifier (`LiteV1Api.refusal`). Kinds carry their statuses, and there is no PARSER/COMPILATION envelope (server/LiteV1Api.java:289-415). Query parameters are read by their declared Pure types, with typed refusals (server/LiteV1Planning.java:173-271).
- **G7 fixes in exec.** A Number's kind is no longer sniffed from text, a String cell is no longer parsed as JSON, and zoned date-times are refused, not shifted (exec/JdbcDecode.java:323-334, 615-645; exec/WireTypes.java:84-115).
- **Compile errors where upstream's words differ are kinds with operands** (`error/CompileKind.java`, `compiler/element/ModelIntegrity.java`), so the edge words them.
- **J30 was corrected from the pinned sources** (Pure's association rule, not an engine bug: parser/EngineQuirks.java:22-58).

## Findings

### PRODUCT-1 [WRONG-ANSWER] [FIX-AFTER] Lite's saved-query route answers client errors as `internal`/500: the product store still holds legend-engine's statuses and Java class names
- **Where:**
  - core/src/main/java/com/legend/queries/SavedQueries.java:39 (javadoc "An ApplicationQueryException: its status and the engine's {"message"} body"), :244 (MALFORMED → 500), :325 (QUERIES_NOT_FOUND → 500), :497 (NO_SEARCH_TERM → 500), :613-660 (shape messages naming `QuerySearchSpecification`, `QuerySearchTermSpecification`, `QueryProjectCoordinates`, `TaggedValue`, `TagPtr`, `StereotypePtr`; "another kind is the 500 for a body the engine cannot read").
  - queries/SavedCubes.java:246 (`DataCubeQuery.`), :273-284, :276 (500).
  - server/LegendHttpServer.java:249-298 (the lite route remaps `MISSING` because "a batch's is upstream's 500", then `a.status() >= 500 ? INTERNAL : REQUEST`).
  - Pinned on every host by datacube/test/saved-pages/conformance.ts:123-140.
- **What:**
  - `POST /api/lite/v1/queries/dataCube/search` with `{"limit":"ten"}` answers HTTP 500 `{"kind":"internal","message":"the request could not be served: QuerySearchSpecification.limit is not a number"}`.
  - `{"searchTermSpecification":{}}` answers 500 `internal`.
  - An unknown path answers 404 with kind `request`.
  - Lite's API spec (docs/records/lite-api-v1-2026-11-17.md §3) says malformed input is `request` (400), `internal` is "a defect, said as one", and 404 is `notFound`.
- **Why it matters:**
  - The store's statuses are legend-engine's ApplicationQueryException statuses, and edge/StoreWords.java:90-106 passes them through unchanged as upstream's. By the meaning rule they are upstream meaning in the product.
  - Lite's own route patches one case and misclassifies the rest.
  - legend-engine's Java type names reach a product answer.
  - The census cannot see a status code or a string literal.
- **Instead:**
  - The store refuses with lite's kinds: `request`/400 for shape and validation, `notFound`/404, `internal`/500 only for the disk failing. Its messages name the request's own fields.
  - `StoreWords` (or a status map beside it) maps each kind to legend-engine's status and words: 500 for batch-missing, no-search-term and malformed, plus the class-named messages.
  - Re-pin the conformance cases.
- **Confidence:** verified (read; the conformance test pins the 500/`internal` answers).

### PRODUCT-2 [WRONG-DESIGN] [FIX-AFTER] legend-engine's exact sentences in the product's test-data generator and CSV loader
- **Where:**
  - core/src/main/java/com/legend/testdatagen/TestDataPlanner.java:762-786 ("Seed data generation currently supports queries with a single root table/view (unions are not supported)", "...with a root table (unions/views are not supported, got: RelationTree)").
  - TestDataPlanner.java:1135-1158 ("There should be exactly 1 schema with name ... found in the provided runtime database(s) [...] at the given connection.", "Table \"...\" not found in Schema \"...\" in Database(s) ...", "Column \"...\" not found in Table ...").
  - core/src/main/java/com/legend/setup/CsvSeed.java:289.
  - 11 literals in all.
- **What:** product refusals (`AssertFailed`, a test plan's failure) carry legend-engine's words, byte for byte. The comments say so ("upstream's words").
- **Why it matters:**
  - Done criterion 1 of the plan of record: "No legend-pure or legend-engine spelling is computed for a product answer".
  - These were ruled under S195/S302 (2026-11-11/12), before "everything compatibility-related to the edge" (2026-11-15/16), and never re-ruled. The census misses them because they are literals.
  - The PR did the analogous job for `SavedQueries` (J9) and `NoRowIdentifiers`, but not here. It touched TestDataPlanner (+198/-195).
- **Instead:**
  - Kinds with facts (schema, table, column, branch count, root kind) and lite's sentences.
  - legend-engine's sentences in the edge, which already words `NoRowIdentifiers`.
  - Take S195's and S302's message clauses back to the user.
- **Confidence:** verified.

### PRODUCT-3 [WRONG-ANSWER] [FIX-AFTER] legend-pure's "Assert failed" is the product's default assert message on two paths; lite's is "expected true, got false"
- **Where:**
  - core/src/main/java/com/legend/compiler/spec/VerdictQueries.java:150-158: `assertMessage` returns "Assert failed", "upstream's default for the base overload", whatever the style.
  - It is used by testplan/AssertVerdicts.java:1488-1489 (the vector-contract path). The sibling path at :1503-1505 uses `defaultConditionMessage` → lowering/LiteVerdictWords.java:172 → error/AssertSentence `TRUE` "expected true, got false".
  - compiler/spec/LambdaBodies.java:128-131: `assert(cond); rest` in a function body lowers to `fail("Assert failed")` on the product query road.
  - compiler/spec/AssertMessages.java:97.
- **What (inputs, not executed):**
  - A product test body `#TDS\nx\n1\n-1#->forAll(r|assert($r.x > 0))` (a row source and a row-local predicate: `VerdictQueries.vectorContract`) fails with "Assert failed".
  - A model function `f(x:Integer[1]):Integer[1]{ assert($x > 0); $x; }` with the query `|f(-1)` raises "Assert failed" as a product `DataError`.
  - The same `assert(false)` on the simple-predicate path says "expected true, got false".
- **Why it matters:** two product forms for one failure, one of them legend-pure's words. Judge parity in the product's wording is broken by path.
- **Instead:**
  - `assertMessage` takes the style's words (`VerdictWords.conditionFailed`), as the simple path does.
  - LambdaBodies raises a kind (an assert failure) or lite's sentence; legend-pure's words stay at the edge.
- **Confidence:** verified by reading both paths; the outputs were not executed.

### PRODUCT-4 [WRONG-DESIGN] [FIX-AFTER] Lite's English sentences are baked into the portable plan (v54 `raises`, v56 site `message`) and validated on read
- **Where:**
  - core/src/main/java/com/legend/executionplan/PlanJson.java:241 (`out.put("raises", raises(plan))`) and :255-264 (each kind to `ErrorKind.sentence()`).
  - PlanJson.java:1404-1416 (`sitesJson` writes `message`) and :1424-1442 (`readSites` refuses a message that is not the kind's sentence; `read` refuses a `raises` map that is not the statements').
  - executionplan/ExecutionPlan.java:843-870 (`Site.message`).
  - exec/PlanRunner.java:100-112 (`declared` checks membership by comparing sentence texts).
- **What:**
  - `message` is a pure function of `kind` and `where`, and the plan-level `raises` is a function of the sites. Both are redundant presentation text in the plan.
  - Because they are validated on read, rewording any `ErrorKind` sentence is a format change with no version bump.
- **Why it matters:**
  - The approved next program is "one plan, two presentations": presentation lives in writers, and the plan names a form but holds no text.
  - An imported upstream plan would have to make up lite's sentences to be read.
  - The undo cost today is a v59 bump and three readers (Java `PlanJson`, python/legend_lite/_raised.py, engine-client raised.ts). It grows once plan export ships.
  - Not a blocker: no external consumer stores v58 plans, and old versions are already refused by name.
- **Instead:**
  - Sites carry `n, kind, where`, and the plan's `raises` lists kinds.
  - Each presentation (exec's `DataError`, the hosts, the edge) words a kind through its writer, from one generated sentence table or fixture (as before #104).
  - Do it first in the portability program, before export.
- **Confidence:** verified.

### PRODUCT-5 [WRONG-DESIGN] [NOTE] The raise-site format cannot carry operand values back, so step 1 of the next program rests on a false premise
- **Where:**
  - core/src/main/java/com/legend/sql/dialect/AnsiSqlRenderer.java (`site(RaiseText)`): the statement raises only the token `LG#<n>`.
  - core/src/main/java/com/legend/sql/dialect/RaiseSite.java:22: operands kept as plan-time `SqlExpr`.
  - exec/RaisedErrors.java:83-94: a raise read back is a site number.
  - executionplan/MirJson.java `raiseText` (operands in the plan's tree).
- **What:**
  - In the product, a run-time raise returns no operand value (for example the dividend).
  - edge-portability-program-2026-11-18.md §7 item 1 says "The product's raise already carries its kind and typed operands ... The edge writes legend-pure's sentence in Java from them, after the error comes back".
  - With token-only raises, value-bearing sentences ("Cannot divide 5 by zero") cannot be worded after the run. That is why `PureMessages` builds them in SQL today (the D1 debt).
- **Why it matters:** the next program needs an edge raise form that returns typed operand values as data. That is a raise and plan format change the program does not name.
- **Instead:** plan that format change explicitly at the start of the portability program, with the user's ruling (S308 says the product raise never carries the value; an edge-only variant may).
- **Confidence:** verified (code). The conclusion about the next program is my inference.

### PRODUCT-6 [WRONG-DESIGN] [FIX-AFTER] Three overlapping hook mechanisms, with families outside the registry
- **Where:**
  - core/src/main/java/com/legend/database/PlanStyle.java:20-153: six direct two-form methods (`traceCell`, `graphKeyDate`, `graphKeyString`, `errorWords`, `siteWords`, `judgesEngineText`), plus `twoForm` and two `facet` overloads.
  - testplan/PlanStyleFacets.java:24-77: 5 facets and 16 two-form kinds.
  - Two-form kinds whose product form refuses, which are facets in all but name: planner/RefusedExecutedValues.java and compiler/spec/RelationCountAsserts.java.
  - Families outside `PlanStyleFacets`: result/CellText.java:50 `Forms`; the `Function<Refusal,String> words` of SavedQueries, SavedCubes and `LegendHttpServer.storeRoutes`; `Compiler.compileErrors(model, words)` (Compiler.java:1053-1072); `LiteV1Api.Runner`; `lowering.Styled`.
  - The edge's `SiteWordsRecorder` is a stateful recorder passed as a "style" (edge/PureText.java:286-296).
- **What:**
  - Criteria (a)-(c) hold for every kind I read: interfaces of signatures only, a product form or a named refusal, and the non-product forms in `com.legend.edge`.
  - Criterion (d) holds for the registry. The off-registry families are pinned unevenly (HookLambdasTest covers `CellText$1`; the words functions are lambdas).
  - Hooks cost 45 `PlanStyle style` parameters in 17 product files, 16 `twoForm` sites and 21 `facet` sites (Measurements).
- **Why it matters:**
  - The size is proportionate to the D1 debt: most kinds are SQL-level divergences that "one plan, two presentations" removes. It is not proportionate to the goal.
  - Three ways to express the same fork make the next program's job harder and the rulings easy to drift.
- **Instead:**
  - One mechanism: `twoForm(kind, productForm)`, with a refusal as a product form.
  - Move the direct PlanStyle methods and the words functions into `TWO_FORMS`.
  - Stop the recorder riding as a style: let the door that wants legend-pure's words read the sites from the plan.
  - Add no new kinds in the next program except for kind-B rows.
- **Confidence:** verified.

### PRODUCT-7 [WRONG-DESIGN] [FIX-AFTER] Edge-only product surface exempted on reasons that do not hold
- **Where and what:**
  - **`LiteV1Api.Runner`** (server/LiteV1Api.java:84-88), **`ConnectionResolver.RUNNER`** and **`runner(...)`** (server/ConnectionResolver.java:33-41): used only by the edge (edge/EdgeServer.java, edge/PureV1Routes.java, edge/TestDataRoutes.java, wasm EdgeBoundary) and by tests. Lite's own `query/run` uses `LiteV1Planning.Runner`. Ruled READ_API as "lite v1's own answer and runner seams" (core/src/test/java/com/legend/EdgeOnlyMembersTest.java:275).
  - **`UpstreamPlans.testDataTimestampText()`** (planner/UpstreamPlans.java:58): no caller anywhere. The edge calls `EnginePlans.testDataTimestampText` directly (edge/TestDataRoutes.java:67, 117).
  - **`SavedCubes.java:240`**: `FIELD_MISSING` passes the operand `"pageContent"`. That is not a field; it exists only so edge/StoreWords.java:68 can pick legend-engine's "Query content is missing" over "...or empty". It is upstream data in a product literal.
- **Why it matters:**
  - 110 product members are called only by the edge and are exempt by ruling (95 READ_API, 5 RULED_J21, 10 HOOK_ENTRIES).
  - Most are fair "facts about the program", but these show the ruling was sometimes a label, not a change of meaning. "Renaming does not close a finding."
- **Instead:**
  - Move `LiteV1Api.Runner` and `ConnectionResolver.runner`/`RUNNER` to the edge, or fold them into `LiteV1Planning.Runner` with no values.
  - Delete `testDataTimestampText` from the facet.
  - Pass the real field (`content`) and let `StoreWords` key on store and field.
- **Confidence:** verified.

### PRODUCT-8 [WRONG-DESIGN] [NOTE] J21: the product parser keeps legend-engine's grammar level, reached by the edge alone
- **Where:**
  - core/src/main/java/com/legend/door/Parse.java (`engineRecords`, `engineDocumentJson`, `engineLambdaJson`).
  - parser/GrammarLevel.java:20-23 (`LEGEND_ENGINE`: "Refuses BOTH the platform grammar and the lite extensions, with the engine's own messages").
  - EdgeOnlyMembersTest `RULED_J21` (5 members).
- **What:** by the meaning rule this is upstream meaning in the product (legend-engine's refusals and shapes), exempt by a ruling citing G1 (one parser). The ruling is reasoned and the access is guarded (`ArchitectureTest.legendEnginesGrammarIsTheEdgesAlone`). "Census 0" does not mean "none".
- **Instead:** keep it as ruled, but list it in the handoff as a known product-side compatibility residue.
- **Confidence:** verified.

### PRODUCT-9 [WRONG-ANSWER] [FIX-AFTER] Lite's error shape: non-raise failures answered as `data`, `where` dropped for data refusals, position duplicated in compile messages
- **Where and what:**
  - **core/src/main/java/com/legend/server/LiteV1Api.java:404-407:** every `DataError` becomes `data` (422) with `raise: lowerCamel(kind)`. So a database's own failure answers `raise: "databaseError"`, a lost connection `"connectionError"` and an oversized file `"fileTooLarge"`, although `ErrorKind.isRaise()` is false for all three (error/ErrorKind.java:56-66). Spec §3 says `raise` is a raise-site kind and `data` is "a run's raise".
  - **error/DataError.java:57-62:** the planned-site constructor accepts `where` and drops it (no field, no accessor). The refusal therefore never carries `where`, which spec §3 asks for "where there is one". It also silently ignores an argument (invariant 4's spirit).
  - **error/LegendCompileException.java:40-50 and LiteV1Api.refusal:** the message carries the position and `where` repeats it, `{"message":"[1:1] Unknown type: ...","where":"[1:1]"}`. Pinned in engine-client/test/fixtures/lite-api-cases.json:431-436.
- **Why it matters:** errors-as-kinds is the program's product form, and these are the places it is not applied consistently.
- **Instead:**
  - Database and connection failures answer `internal` (or a `database` kind, if the user wants one), and `raise` only for `isRaise()` kinds.
  - `DataError` keeps `where` as data and the refusal writes it.
  - The message drops the position when `where` carries it.
- **Confidence:** verified.

### PRODUCT-10 [WRONG-DESIGN] [NOTE] Two product texts of one DateTime, and the product's carried Float layout is legend-pure's
- **Where:**
  - core/src/main/java/com/legend/values/PureDateLiteral.java:94-99 (`toCarriedDigits`: six digits, or nine where finer than a microsecond) and result/CellText.java:302-307 (`productTime`).
  - S312's register row (docs/SEMANTICS_REGISTER.md:15) and `CellText.productDate`'s javadoc (:282, "the one form the SQL's DATETIME_TO_TEXT writes too") both say six.
  - result/CellText.java:255-270 (`carriedFloat`, "Pure's layout ... legend-pure's own text"), shared with edge.Presenter.
- **What:**
  - A cell carries nine digits while `toString` in SQL writes six. This is defensible under §10 items 3 and 5 (carriage is exact), but the register row and the javadoc disagree with the code.
  - The product's CSV and JSON Float text is legend-pure's print layout (a point always, never an exponent). That is acceptable as carriage, but the product and the edge share one Float printer, so the product's form cannot move alone.
- **Instead:** amend S312's row with the carriage clause, fix the javadoc, and name the shared printer as a product form the edge reuses.
- **Confidence:** verified.

### PRODUCT-11 [WRONG-DESIGN] [NOTE] `readThroughExecuteValues`: an edge-only flag renamed into a "fact" and carried through four product layers
- **Where:**
  - core/src/main/java/com/legend/compiler/spec/typed/TypedSerialize.java:30 (was `engineExecuteCardinality`).
  - resolver/StoreResolver.java:1676-1699, lowering/Lowerer.java:824-831.
  - lowering/ProductLoweringForms.java:41-43 (the product form returns its input unchanged).
- **What:**
  - Only the edge's form reads the flag. The product's two branches build the same SQL, `conformJsonEgress(serializeGraph(g, false))`.
  - The Lowerer's comment still calls it "the value of an engine execute(...) (the splice's mark)".
  - It passes the meaning rule (a fact about the program), but it is the rename pattern the rule warns about.
- **Instead:** let the edge's form detect the read-through itself from the chain, or keep the fact and fix the comment; no product branch on it.
- **Confidence:** verified.

### PRODUCT-12 [COSMETIC] [FIX-AFTER] Stale upstream claims in product comments, orphaned javadocs, and a mutable hook setter
- **Stale comments that state upstream behaviour (about 8):**
  - exec/RaisedErrors.java:13-20 ("The message inside is OURS -- composed by the compiler, pure's exact text").
  - error/DataError.java:54-56 ("or legend-pure's in a plan the compatibility edge wrote", after K10).
  - executionplan/PlanJson.java:250 ("a site's own words, upstream's in an edge plan").
  - Further examples: server/ConnectionResolver.java:26; queries/SavedQueries.java:39; Execution.java (the values overload names the old `/api/lite/v1/execution/execute`); plan/EnumMappings.java:10 ("the planner ... read[s] the mapping": it does not).
- **Orphaned stacked javadocs introduced by the PR:**
  - values/PureDateLiteral.java:84-90 (the `toNineDigits` doc above `toCarriedDigits`).
  - planner/PlanEnv.java:80-82 and :89-95.
- **Quality:**
  - resolver/GraphEmission.java:61-67: a new non-final `keyText` field set by a mutating `withKeyText(..)` that returns `this`, threaded by hand through the construction sites at :240, :1006 and :1422. A missed site fails at run time.
  - database/PlanStyle.java:122-123: a product refusal names `com.legend.edge`, one of 18 product literals that name legend-engine, legend-pure or the edge.
- **Instead:** fix the comments to say what the code means now; one javadoc per member; pass `GraphKeyText` through the constructor.
- **Confidence:** verified.

### PRODUCT-13 [BROKEN-RULE] [NOTE] Plan-format changes without a version bump on the last day
- **Where:** core/src/main/java/com/legend/executionplan/MirJson.java:487-492.
- **What:**
  - `PlanParam`'s key `"kind"` now holds a `ValueType`, and `enumMapFn` was dropped: "the wire word kept, no version bump".
  - Further no-bump changes: HAVING, QUALIFY and OFFSET are omitted when null (:84-96); v57's `decimal` is omitted when null ("a codec form, not a format change", PlanJson.java:166).
  - An old `RAW` kind is still refused by name, so nothing is misread silently.
- **Why it matters:** G11 (the plan as portable data) relies on the version naming the format. Four silent codec changes in one day weaken that just before plans become an export format.
- **Instead:** bump to v59 when these land on main, or record the codec-compatibility rule in `PlanJson.VERSION`'s javadoc.
- **Confidence:** verified.

## Cross-area observations

- **Lowering and sql:** `SqlExpr.RaiseText.operands` (sql/SqlExpr.java:1218-1256) exist only for the edge's in-SQL sentences. `ExistsSites.keysWhere`, `EnvelopeParts`, `RelationParts` and `ReprParts` (`pureToString`, `dateLiteralText`, `heldAsInteger`, `cat`) are product facades read only by the edge's in-SQL compositions, exempt as HOOK_ENTRIES or READ_API. They are the product-side cost of D1 and go away with it.
- **Judges:** AssertVerdicts' vector and simple paths give different default messages (PRODUCT-3). The fix belongs in testplan, fed by VerdictQueries.
- **Hosts:** DataCube's saved-pages conformance suite pins 500/`internal` for malformed requests on every host (PRODUCT-1). The Python and TypeScript hosts read a site's `message` from the plan (PRODUCT-4), so a fix there is a three-language change.
- **Edge:** `StoreWords.engineShape` passes the product store's statuses through as legend-engine's (PRODUCT-1). If the product statuses become lite's, the edge needs its own status map. I did not re-measure the D1 count (the edge's area).

## Measurements (numbers with the command that produced them)

| Number | What it counts | Command |
|---|---|---|
| 170 files, +6,064 -5,224 | Area size | `awk -F'\t' 'NR>1 && $1 ~ /^[0-9]+$/ {a+=$1; d+=$2; n++} END {print n, a, d}' audit/units/PRODUCT.txt` |
| 45 params in 17 files | `PlanStyle style` parameters in the product | `grep -rn --include=*.java -E 'PlanStyle style[,)]' core/src/main/java/com/legend \| grep -v /edge/` (`wc -l`, `-l \| wc -l`) |
| 16 | `twoForm(` call sites in the product | `grep -rn 'twoForm(' ... \| grep -v /edge/ \| grep -v PlanStyle.java` |
| 21 | `.facet(` call sites in the product | same grep for `\.facet(` |
| 13 | `words` function parameters | same grep for `Function<SavedQueries.Refusal, String>\|Function<Refusal, String>\|Function<Throwable, String> words` |
| 5 facets, 16 two-form kinds | The hook registry | read of core/src/main/java/com/legend/testplan/PlanStyleFacets.java:24-77 |
| 6 | Two-form methods directly on PlanStyle | read of core/src/main/java/com/legend/database/PlanStyle.java |
| 95 READ_API, 5 RULED_J21 | Edge-only product members exempt by ruling | `awk '/static final Map<String, String> /{name=$5} /Map.entry\(/{c[name]++} END{for(k in c) print k, c[k]}' core/src/test/java/com/legend/EdgeOnlyMembersTest.java` |
| 10 HOOK_ENTRIES | Hook entries exempt by ruling | `sed -n '/HOOK_ENTRIES = /,/static List/p' core/src/test/java/com/legend/HookRulingTest.java \| grep -c 'Map.entry('` |
| 30 kinded, 481 free-text | Compile-error throw sites outside the edge and `error/` | `grep -rn 'CompileKind\.[A-Z_]*'` against `grep -rnE 'new (ModelException\|TypeInferenceException\|MappingResolutionException\|NameResolutionException)\(' \| grep -v CompileKind` |
| 24 | Store literals naming legend-engine's classes | `grep -nE '"[^"]*(QuerySearchSpecification\|QuerySearchTermSpecification\|QueryProjectCoordinates\|TaggedValue\|TagPtr\|StereotypePtr\|DataCubeQuery\|"Query")' queries/*.java \| wc -l` |
| 6 | Status-500 refusals in the store | `grep -nE ', 500\)' queries/*.java` |
| 11 | legend-engine sentences in testdatagen and setup | `grep -nE '"(Seed data generation currently\|There should be exactly 1 schema\|Table \\"\|Column \\")' testdatagen/*.java setup/*.java \| wc -l` |
| 3 | "Assert failed" code literals in the product | `grep -rn '"Assert failed' core/src/main/java/com/legend \| grep -v /edge/` (AssertMessages:97, LambdaBodies:130, VerdictQueries:157; plus 1 comment) |
| 18 | Product string literals naming legend-engine, legend-pure or the edge | `grep -rnE '"[^"]*(legend-engine\|legend-pure\|compatibility edge\|com\.legend\.edge\|backwards-compatibility)[^"]*"' ... \| grep -v /edge/` (comment lines excluded) |
| 0 | Readers of v57's `decimal` digits, beyond pass-through copies and `PlanJson`'s own codec | `grep -rn '\.decimal()' core/src/main/java`; the host greps (python/legend_lite, engine-client/src, datacube/src) find only parameter-kind words, no column `decimal` reader |
| 0 | Callers of `UpstreamPlans.testDataTimestampText()` | `grep -rn 'testDataTimestampText' core/src/main/java`: the edge calls `EnginePlans.testDataTimestampText(x)` directly |

The census figures (0) are the handoff's claims; I did not re-run them (no Bazel, per the rubric).
