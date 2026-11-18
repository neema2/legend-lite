# EDGE: verdict SOUND-WITH-DEBT

## Summary (at most 12 lines; answer your brief's questions first)

1. **Java over typed results, or upstream text in SQL?** Both. Every answer that goes back to a client is written in Java (Presenter, PureV1Writer, TdsObjectsWriter, ServiceTestRunner, EdgeHostWords). Text that test runs compare is still composed in SQL. **The 1,978 number is confirmed and has not materially grown.** The nine named files are 1,992 lines at the head, +14. That count is whole files, though: only **~900 of those lines are code in methods that build SQL**, and 212 lines of PureMessages are already Java words. The list also misses **EngineExecutedValues** (44 lines, the engine's JSON envelope built through a SQL CONCAT), `EdgeLoweringForms.oneBare`, the `EnginePlans.SECONDS` print and the **40 print/raise overrides in PureText's 12 dialect subclasses** (~239 code lines). Net growth is about +50 lines, all absorbed from the product.
2. **Moves: copies, not redesigns.** In the 29 core files moved by rename, 91% of code lines are unchanged (7,884 code lines, 701 added); 7 files are 99-100% identical. Ten files extracted from product classes are 85-98% line-identical (RelationTextLowering is 85% `lowering/Render`). The real redesign was on the product side: upstream-only MIR variants left `sql/` (VarSetPlaceholder, TempTableInSplice, DeferredTdsString, FLOAT_PRINT, TDS_NULL_CELL, in 6/9/10/19/2 files at main, now 0). Legend-pure's words for literal raise sites are now produced in Java.
3. **Correctness.** One regression against main: in the edge, a relation's `toString` and `toCSV` print a Float cell with the database's cast, not legend-pure's print. On DuckDB, 0.00001 comes out as `1e-05` where upstream writes `0.00001` (EDGE-1). Three wrong answers predate the PR and were moved unchanged:
   - toCSV prints a DateTime as ISO text with `+0000`, where upstream's `SimpleDateTimeFormat` writes `yyyy-MM-dd HH:mm:ss` (EDGE-2);
   - `format('%r')` does not escape a newline (EDGE-3);
   - the SQL-built tdsBuilder envelope has unordered rows and database-specific JSON spacing (EDGE-8).
4. **Quality.**
   - Duplication: 12 near-identical dialect subclasses; two forms of each of 26 raise sentences (SQL and Java) with no parity test; ~14 assert templates in both SQL and Java.
   - The MIR rewrite passes match the product lowering's *encodings*, not typed facts. One node, FLOAT_TO_TEXT, carries two upstream meanings, which is the root cause of EDGE-1.
   - "Four passes" undercounts the reshapes (EDGE-10).
5. **Next program.** No BLOCKER. Two things make "one plan, two presentations" harder: the edge as its own SQL render target (PureText), and lowering facets that compose text in SQL. Both are internal Java and can be removed; neither is a plan-format field or a shipped root. The edge already has an export base: pure/v1 `generatePlan` writes legend-engine's SingleExecutionPlan JSON, plus ~4,500 lines of plan-text renderers. The wasm and native edge modules already split a plan from its answer, so rows come back typed and Java writes the answer.

## Blockers (if any; each one also appears in Findings)

None. EDGE-1 is a wrong answer introduced by this PR, but it is latent:
- it appears only at extreme magnitudes on DuckDB and Postgres, and above 1e7 on H2, where the S132 leniency hides it from the judge;
- no PCT or corpus test reaches it (the program measured "PCT moves 0");
- the fix is one line.

It is graded FIX-AFTER, recommended as the first follow-up, or folded into the PR if another commit lands anyway.

## Strengths

- **The product's MIR lost the upstream-only variants.**
  - Measured: in `core/src/main/java/com/legend/sql`, `VarSetPlaceholder` (6 files at main), `TempTableInSplice` (9), `DeferredTdsString` (10) and `TDS_NULL_CELL` (2) are gone. `FLOAT_PRINT` is down from 19 files to one comment.
  - The edge recognises its own constructs through side tables: `PlanVariables.java:19-39` (a CTE no WITH defines), and `EngineParams` (`isSplice`, `isTempTable`, `enumMapFn`) as used by `EngineStyleH2`.
  - This is a real redesign: the product statement no longer carries plan-text-only nodes.
- **Answers to clients are Java over typed values.**
  - `Presenter.java:44-73` (legend-pure's print) and `:82-123` (legend-engine's JSON token). Both refuse any type they do not know by name, never by a default (invariant 4).
  - `EdgeHostWords.java` words the host judge's failures in Java over typed sides.
  - `PureMessages.wordsOrNull` (`PureMessages.java:132-188`) words a literal-operand raise at planning, in Java.
- **State discipline in the K10 recorder.**
  - `SiteWordsRecorder.java:23-75` is minted per planning call, sealed after one read, and refuses two different words for one key by name.
  - Apart from StableScanOrder's pinned firing counter (`StableScanOrder.java:32`), the edge has no static mutable state. Checked by grep for static collections and atomics.
- **The shipped-artifact opt-in is clean.** `EngineMount.java:13-26` is a `ServiceLoader` `MountProvider` keyed `legend-engine`. `EdgeServer.java:23-28` is the composition root over the product server.
  - The wasm and native edge modules (`wasm/src/edge/.../EngineExports.java:24-46`, `native/src/edge/.../EngineEntries.java:26-97`) export separate plan and answer halves: the host's database runs the plan's SQL, and Java writes the answer.
- **Export foundation exists.**
  - `PureV1Routes.generatePlan` (`PureV1Routes.java:256-290`, `:400-422`) writes legend-engine's `SingleExecutionPlan` JSON (`_type: simple`, `rootExecutionNode`, `relationalTdsInstantiation`, template functions).
  - `EnginePlans`, `PlanText`, `EngineStyleH2/DB2/Composite`, `InProtocol` and `PlanEnumForm` already render legend-engine's plan text.
- **Upstream is cited.** Most sentences and print rules cite upstream file:line at 4.145.0 and 5.99.0, and I could check them. `relation/functions/toString.pure` and `s.pure` match `RelationTextLowering.tdsString` and `tdsCell` exactly on header quoting, the `{`/`[` String quoting rule, `null`/`'null'` and the empty-relation frame. `helperFunctions.pure`'s frame and `escapeCSVString` match `RelationTextLowering.csv` and `escapeCsv`.
- **Complexity is moderate.** Lizard: 1,147 functions, average CCN 4.0, 50 above CCN 25 or 150 lines. Most of those are the engine-text renderer (`EngineStyleH2::call`, CCN 147).

## Findings

### EDGE-1 [WRONG-ANSWER] [FIX-AFTER] A Float cell in a relation's text is the database's cast, not legend-pure's print (regression from main)
- **Where:**
  - `core/src/main/java/com/legend/edge/RelationTextLowering.java:566-567`: tdsCell's Float arm, `pureToString(styled, FLOAT, CAST(c AS DOUBLE))`.
  - `RelationTextLowering.java:694`: the toCSV cell, `escapeCsv(pureToString(styled, t, c))`. Both reach `LiteralSpelling.floatPrint`, which writes `SqlFn.FLOAT_TO_TEXT`.
  - `PurePrintForms.java:175-176` rewrites every `FLOAT_TO_TEXT` to `floatOf`, which at `:120-122` is `CAST(x AS VARCHAR)`.
- **What:**
  - Phase 5 row 8 (`237bd8e5f`, "WIP (unmeasured, not for hand-off)") switched the edge's Float text from `FLOAT_PRINT` to the database's cast. Row 8 covers "a Float's `toString` inside an expression", which legend-engine pushes to SQL as `cast(%s as varchar)`. Because one pass rewrites every `FLOAT_TO_TEXT`, the switch also reached the cells of a relation's `#TDS` text and of `toCSV`.
  - Upstream writes those cells in Pure, after the query: `s.pure` `o : Any[1] | $o->toString()`, and `helperFunctions.pure:234` `$a->toString()->escapeCSVString()`. Legend-pure prints a double with `DecimalFormat("0.0")` and 340 fraction digits, so never with an exponent (`CompiledSupport.java:132-137, 1271-1273`, legend-pure 5.99.0).
  - Main printed these cells through `FLOAT_PRINT`, i.e. `sql/dialect/FloatPrintText.print`, plain digits.
- **Concrete input:** under the edge on DuckDB, `#TDS\n x\n 0.00001\n#->toString()`.
  - Lite writes the cell `1e-05`. Measured: `CAST(CAST(0.00001 AS DOUBLE) AS VARCHAR)` gives `1e-05` (DuckDB 2.0.0; lite's own javadoc at `PurePrintForms.java:86-88` documents the same exponent form on its pinned DuckDB). Upstream writes `0.00001`.
  - `toCSV` of a Float column holding 1e20: lite `1e+20`, upstream `100000000000000000000.0`.
  - On H2 (Java's `Double.toString`) any cell of 1e7 or more prints as an exponent (`1.2345678E7`). The judge's S132 leniency hides that case, because both texts contain a `.` (`UpstreamRenderedTexts.java:156-170`). It does not hide `1e-05`, which has no point.
  - The product's own javadoc says the opposite of the code: `sql/SqlFn.java:148-152`, "the compatibility edge writes FLOAT_SHORTEST_TEXT for it, legend-pure's print form".
- **Why it matters:** these are upstream-facing bytes that main got right. A test that compares such a text gets the wrong verdict.
- **Instead:**
  - Now: in `RelationTextLowering`, print a Float cell with `SqlFn.FLOAT_SHORTEST_TEXT`, which `PureMessages` and `EdgePureOnlyTexts.representationOf` already use as "legend-pure's print". Or give relation-text cells their own node, so the row-8 pass cannot reach them. Add the two inputs above to `FloatTextTest`.
  - Long term: the next program's relation text in Java.
- **Confidence:** verified by reading lite's path, main's path and upstream's source; DuckDB's output measured. Not run end to end through lite.

### EDGE-2 [WRONG-ANSWER] [FIX-AFTER] toCSV writes a DateTime cell as Pure's ISO text, not upstream's `SimpleDateTimeFormat` (from before the PR, never ledgered)
- **Where:** `RelationTextLowering.java:688-692`. `DATE_TIME`/`LATEST_DATE` go to `dateTimeText(c)`, i.e. `EdgeDateTimeTexts` and `PureDateTimePrint`: `T`, the fraction, `+0000`. `:683-687` does the same for an abstract `Date` with a time.
- **What:**
  - Upstream: `helperFunctions.pure:238-243`, `formatDateTime` is `format($dateTimeFormat, $date)->escapeCSVString()` when the value has an hour. With `SimpleDateTimeFormat()` = `'%t{yyyy-MM-dd HH:mm:ss}'` (`core/pure/corefunctions/dateExtension.pure:384-387`, legend-engine 4.145.0), `%2024-01-02T10:30:00.123+0000` prints `2024-01-02 10:30:00`. Lite prints `2024-01-02T10:30:00.123+0000`.
  - The edge itself writes upstream's form elsewhere. `EnginePlans.java:222-231` (`SECONDS`, register S306) prints the same upstream function (`toCSVString`) as `yyyy-MM-dd HH:mm:ss` for test data. That makes two edge implementations of one upstream rule that disagree.
  - The code's comment at `:690-691` knows ("the engine's own CSV wrote 'yyyy-MM-dd HH:mm:ss', the fraction dropped"). `docs/SEMANTICS_REGISTER.md` has no row for it.
  - Unchanged from main's `lowering/Render.java:778-782`.
- **Why it matters:** this is a wrong CSV text for a corpus-style `toCSV` over a timestamp column. A divergence that is not a register row is a defect (the upstream-fidelity tenet).
- **Instead:** use `EnginePlans.testDataTimestampText`, the one upstream `SimpleDateTimeFormat` print, in `RelationTextLowering.cell`. Or post the divergence to the user and ledger it.
- **Confidence:** verified (lite code and upstream source read).

### EDGE-3 [WRONG-ANSWER] [FIX-AFTER] `format('%r', s)` does not escape a newline in a String (from before the PR)
- **Where:** `EdgePureOnlyTexts.java:106-123`. `formatR`'s String path escapes `\` and `'` only (`:120-122`). `representationOf` at `:60-70` escapes `\n` as well.
- **What:**
  - legend-pure's `%r` is `toRepresentation`. Interpreted: `Format.java:95-101` calls `toRepresentation_Any_1__String_1_`. Compiled: `PureStringFormat.java:59-62`.
  - `toRepresentation.pure`'s String arm is `replace('\\','\\\\')->replace('\'','\\\'')->replace('\n','\\n')`.
  - **Input:** `format('%r', 'a' + '\n' + 'b')` under the edge returns `'a<LF>b'`; upstream returns `'a\nb'`, a backslash and an n.
  - UpstreamVerdictWords patches this only inside assert messages (`reprNewlines`, `:193-197`). Same as main's `lowering/Repr.of(TypedSpec, SqlExpr)`.
- **Why it matters:** wrong text for a user's `%r`, and two edge spellings of one upstream function that disagree.
- **Instead:** `formatR`'s String arm should call `representationOf(styled, STRING, e)`. That gives one owner and removes the duplicate escape.
- **Confidence:** verified (lite code and upstream source read).

### EDGE-4 [WRONG-DESIGN] [FIX-AFTER] Presentation built in SQL: the count, method by method (follow-up D1, confirmed and corrected)
- **Where:** these nine files (all `core/src/main/java/com/legend/edge/`):

  | File | Lines at `4e8702016` | Lines at head | Code lines | Code lines in methods that return MIR |
  |---|---|---|---|---|
  | `RelationTextLowering` | 780 | 781 | 570 | 483 |
  | `PureMessages` | 479 | 481 | 327 | 93 (plus 212 in Java String methods) |
  | `UpstreamVerdictWords` | 191 | 198 | 134 | 83 |
  | `PurePrintForms` (with `TdsNullCells`, `UnguardedParses`) | 184 | 184 | 109 | 76 |
  | `EdgePureOnlyTexts` | 136 | 136 | 90 | 75 |
  | `EngineResultEnvelopes` | 88 | 92 | 65 | 54 |
  | `PureListTexts` | 49 | 49 | 31 | 19 |
  | `PureDateTimePrint` | 43 | 43 | 23 | 15 |
  | `EdgeDateTimeTexts` | 28 | 28 | 12 | 4 |
  | **Total** | **1,978** | **1,992** | **1,361** | **~900** |

- **Missing from the list (built in SQL, presentation):**
  - `EngineExecutedValues.java:37-43`: legend-engine's `{"builder":{"_type":"json"},"values":...}` envelope as a typed concatenation lowered to SQL CONCAT. New since `4e8702016`, 44 lines; it replaced `EngineResultEnvelopes.values` (−7).
  - `EdgeLoweringForms.java:79-94`: `oneBare`, the JsonBuilder cardinality fold as `CASE COUNT(*)=1 THEN MIN(..) ELSE JSON_ARRAYAGG END`, 15 code lines.
  - `EnginePlans.java:222-231`: the `SECONDS` strftime print.
  - `PureText.java:346-731`: 40 print and raise overrides across 12 dialect subclasses, 239 code lines of wiring that make the edge an SQL render target.
- **What:**
  - The program's 1,978 counts whole files, including about 630 comment and blank lines, and the 212 lines of PureMessages that are already the target design (Java words). It also misses the classes above.
  - Growth since `4e8702016` is +14 lines in the listed nine and about +50 overall. All of it came in from the product, which is the right direction (for example `VerdictSql.reprNewlines` moved into `UpstreamVerdictWords:193-197`). None of it is new capability.
- **Why it matters:** this contradicts the plan of record's §2.2 ("the edge is Java over typed results") and §4 item 1. It is already recorded and accepted as D1.
- **Instead:** the portability program's §7 items 1-3. Its guard (§7 item 5) should count code lines in methods that build text, by the method above, rather than whole files. It should also include EngineExecutedValues, `oneBare` and PureText's overrides.
- **Confidence:** verified (measured; commands below).

### EDGE-5 [WRONG-DESIGN] [FIX-AFTER] The moves were relocations, not redesigns (handoff §4 item 2, confirmed with numbers)
- **Where:** every renamed and extracted file. Examples:
  - `StableScanOrder.java` (99% unchanged), `RawSqlBoundary.java` (100% of the code it kept), `RelationalMapperRenames.java` (100%), `EngineStyleH2.java` (96%), `PlanText.java` (94%), `SqlTextVerdicts.java` (86%);
  - `RelationTextLowering.java` (85% of its code lines appear in main's `lowering/Render.java`), `EngineSeedText.java` (96% from `Ddl.java`), `JsonSimpleGrammar.java` (93% from `JudgeJson.java`), `PureV1Routes.java` (91% from `PureV1Api.java`).
- **What:**
  - The diffs I read are mechanical adaptations, not changes to how the code works:
    - EngineStyleH2: package and import changes, `VarSetPlaceholder` becomes a `Cte` plus `PlanVariables`, RAW `PlanParam` becomes `params.isSplice`;
    - SqlTextVerdicts: `Collector` becomes `testplan.TextCollector`, statics move to `SqlTextReferee`;
    - EnginePlans: renderer and `EngineParams` threading.
  - Byte identity was the safety net's demand (D5), and the comments say so: "so the SQL is byte-identical" (`EdgePureOnlyTexts.java:13`, `PureListTexts.java:14`, `PurePrintForms.java:17`).
- **Why it matters:** the rubric's example of WRONG-DESIGN ("a move that copied code without redesigning it where redesign was the point"). It is recorded, and it is the next program's work. Merging it entrenches nothing, because the code is only internal.
- **Instead:** the next program, with the golden net comparing outcomes, not edge SQL bytes (D5).
- **Confidence:** verified (measured; 12 diffs read in full).

### EDGE-6 [WRONG-DESIGN] [FIX-AFTER] The MIR rewrite passes match the product lowering's encodings; one node carries two upstream meanings
- **Where:**
  - `PurePrintForms.java:171-183`: every `FLOAT_TO_TEXT` and `DATETIME_TO_TEXT` is rewritten alike.
  - `:135-160` (`joined`): matches `COALESCE(STRING_AGG(ArrayLit<CELL_TEXT..>, sep), '')` and `ReduceCollection(STRING_AGG, LIST_TRANSFORM(x, λ CELL_TEXT))`.
  - `:53-68` (`UnguardedParses`): treats any `Cast(VARCHAR→DOUBLE, Reason.USER)` as parseFloat.
- **What:**
  - These passes key on shapes the product lowering happens to write, not on typed facts:
    - `FLOAT_TO_TEXT` means both "a user's toString in a query" (upstream: legend-engine's cast) and "a relation-text cell" (upstream: legend-pure's print after the run). That conflation is EDGE-1.
    - parseFloat is recognised by a cast's `Reason` and source type. `lowering/CastPolicy.java:90-172` also writes `Reason.USER` casts.
    - The `joined` CONCAT interleave exists only to reproduce the pre-2026-11-15 bytes (`:128-134`). `TdsNullCells` alone gives the same values.
  - If the product changes one of these shapes, the compiler says nothing; only the golden net on DuckDB, H2 and SQLite catches it, and that net is not in CI (D6).
- **Why it matters:** the edge's correctness depends silently on how the product happens to write each construct, and is checked on 3 of 12 databases. Rewriting MIR at render entry is structurally sound: it is typed MIR to MIR, uses the dialect's own pass hook, and edits no text. The fragility is in what the passes match on.
- **Instead:** typed nodes or facts for the two meanings (for example a `RELATION_CELL_TEXT` node, or a `Reason` on the call), so a pass dispatches on meaning, not on shape. Delete `joined` once outcome goldens replace byte goldens. Better still, the next program removes these passes, except the kind-B ones (`UnguardedParses`, `StableScanOrder`).
- **Confidence:** verified (code read). The parseFloat collision is suspected: I found no non-parse producer of a VARCHAR→DOUBLE USER cast, but did not prove there is none.

### EDGE-7 [COSMETIC] [FIX-AFTER] Duplication inside the edge, and between edge and product
- **Pattern:** one rule written two or more times by hand.
  - Inside the edge (jscpd): 287 duplicated lines in 13 clones, 263 of them in PureText.
  - Between the edge and the product (jscpd): 205 lines.
- **Examples:**
  1. `PureText.java:346-731`: 12 dialect subclasses each repeat `spellingPasses`, `floatToText` and `raiseMessage`, often with `floatLiteralKind`; 40 overrides, almost all identical. A shared policy object that the product dialects consult, or one static helper, would make it one place.
  2. `PureMessages.java:22-109` (`sentence`, SQL) against `:132-188` (`wordsOrNull`, Java): two forms of each of the 26 `RaiseKind` sentences, kept "byte for byte" by hand. No test pins that the two agree (only `InterimEdgeSitesTest` counts sites).
  3. `UpstreamVerdictWords.java` (SQL) against `EdgeHostWords.java` (Java): the same ~14 legend-pure assert templates ("No error was thrown", "Execution error message mismatch...", "expected size: ", " is not empty", " should not equal ", ...). Judge parity (G2) on wording rests on keeping the two equal by hand.
  4. Smaller twins:
     - `RelationTextLowering.withSortOutputs`/`hoistOrder` (`:583-655`) against the product's `Render.liftOrder` (24 lines);
     - `UpstreamRelationType` against `plan/RelationTypeJson` (47);
     - `EngineAllocations` against `planner/PlanAllocations` (45).
- **Why it matters:** each pair is a drift risk on upstream bytes or on G2. Items 2 and 3 go away with the next program.
- **Instead:** one owner per rule, and a parity test for 2 and 3 until the SQL forms are deleted.
- **Confidence:** verified (jscpd plus reading).

### EDGE-8 [WRONG-ANSWER] [NOTE] The SQL-built tdsBuilder envelope: unordered rows and each database's JSON spacing (from before the PR)
- **Where:** `EngineResultEnvelopes.java:55-82`. The rows are `STRING_AGG(rowText, ',')` with no sort keys (`:72-75`), and each piece is `CAST(json AS VARCHAR)` of the database's JSON constructors (`:29-31`).
- **What:**
  - `RelationTextLowering` carries a sorted relation's order into its aggregate (`hoistOrder`, "never a silently unordered render"). This envelope does not, so a sorted chain's rows rely on the aggregate's input order.
  - The text is each database's JSON spelling. Postgres's `json_build_object` (`sql/dialect/Postgres.java:2539-2541`) writes `{"a" : 1}`, where DuckDB and H2 write it compact. Legend-engine writes it one way in Java, as the edge's own `PureV1Writer` does.
  - Moved unchanged from main's `lowering/JsonEmission.java:178-187`.
- **Why it matters:** two edge writers of one upstream envelope can disagree, by database and by row order. I could not construct a client-reachable input (the path is the corpus's `execute` value, on DuckDB and H2).
- **Instead:** the portability doc's open decision 3: retire the SQL envelope and use PureV1Writer's Java over the typed rows. At least pass `hoistOrder`'s keys.
- **Confidence:** suspected (code read; not executed).

### EDGE-9 [COSMETIC] [NOTE] Hook dispatch by chains of class-identity tests, and hand-written delegation
- **Where:**
  - `PureText.java:75-102` (`facet`, 5 kinds) and `:110-180` (`twoForm`, 14 kinds): `if (kind == X.class) return kind.cast(...)`.
  - `PureText.PURE_V1` (`:218-283`) and `SiteWordsRecorder` (`:77-124`) forward the `PlanStyle` members one by one.
- **What:** adding one `PlanStyle` member or kind touches `PlanStyle.PRODUCT`, `UPSTREAM`, `PURE_V1`, `SiteWordsRecorder` and `PlanStyleFacets`' closed lists. `HookRulingTest` pins the set, so drift is caught, but only at test time.
- **Instead:** one immutable `Map<Class<?>, Object>` of the edge's forms, built once and checked against the closed lists at class initialisation. `PURE_V1` as UPSTREAM's map with one entry overridden.
- **Confidence:** verified.

### EDGE-10 [WRONG-DESIGN] [NOTE] "Four MIR rewrite passes" and "the edge never edits SQL text" are both narrower than they read
- **Where:**
  - Plan-text-only reshapes: `PlanEnumForm.apply` (`PlanEnumForm.java:34-71`), `InProtocol.apply` (`InProtocol.java:76-110`), `PlanVariables.of`, and `EngineStyleH2.planned`/`wrapTdsJoinTop` (`EngineStyleH2.java:607, 660`).
  - Lowering-time reshapes through hooks: `EdgeLoweringForms.oneBare`, `existsJoin`, `computedPruning` and `view`.
  - Regex rewriting of raw SQL text: `RawSqlBoundary.translated` (`RawSqlBoundary.java:109-218`), called by the edge dialects' `raw()` (`PureText.java:359-362, 447-455`).
  - A JVM-wide switch: StableScanOrder is enabled by a system property read at render time (`PureText.java:367-369`) and counts firings in a static `AtomicLong` (`StableScanOrder.java:32`).
- **What:** the handoff's four passes (§2.1) are only the render-time passes over executed SQL. Raw-SQL translation is input translation and was moved unchanged, but it does edit statement text at planning.
- **Why it matters:** the next program's inventory of kind-B rows should start from the full list, not from four.
- **Instead:** list every reshape in the kind-B ledger the portability program creates. Replace the system property with a plan-style fact.
- **Confidence:** verified.

### EDGE-11 [COSMETIC] [NOTE] Style patterns
- **Pattern:**
  - inline fully qualified names instead of imports (1,446 `com.legend.<pkg>.<Type>` references in code lines);
  - long lines (274 over 120 characters, 5 over 200);
  - dated ticket history in javadoc (426 comment lines carry a date).
- **Examples:**
  - `SqlTextVerdicts.java` `Deferring.defer`: a whole method on one ~300-character line;
  - `EnginePlans.java:24` (315 characters);
  - `RelationTextLowering.java:53-55`: `relationText(text, what)` returns `text` and ignores `what`, a leftover of the deleted RELATION_TEXT marker.
- **Confidence:** verified.

## Cross-area observations

- **Product SqlFn javadoc.** It says the edge writes legend-pure's print for `FLOAT_TO_TEXT` (`sql/SqlFn.java:148-152`); the code writes the database's cast (EDGE-1). It belongs to the product area but is the clearest evidence of the regression.
- **The census's 0 means "ruled", not "gone".** Seven product-package facets still carry `@OpenLine`: `lowering/RelationTexts`, `PureOnlyTexts`, `ResultEnvelopes`, `planner/UpstreamPlans`, `testplan/EngineTestText`, `EngineTextPlanning`, `PlanStyleFacets`. The census reads 0 because each is in `EdgeSeparationTest.HOOKS_RULED`. These are the hooks the edge's SQL text plugs into, and they are what the next program deletes.
- **The product's `toJSON` has the same no-order aggregate.** `JsonArrayAgg` over the relation without sort keys (`lowering/JsonEmission.java:120-130`) has the same latent ordering issue as EDGE-8.
- **The golden net is the obstacle to redesign.** `//pct:edge_contract_test` compares edge SQL bytes (D5) and is not in CI (D6). Until it compares outcomes, every fix to EDGE-1 through EDGE-6 shows as a regression, which is why the code was copied.
- **Lost passes on other databases are not visible between the lane commits.** EDGE-1's mechanism (row 8) predates both lane commits (`815de5810` and the head), so the base-versus-head lane comparison cannot show it. Only a comparison with `main` can.

## Measurements (numbers with the command that produced them)

- **Edge size:**
  - At `4e8702016`: 78 files, 19,811 lines.
  - At head: 93 files, 21,961 lines. `wc -l core/src/main/java/com/legend/edge/*.java` in the worktree, and `git show 4e8702016:<file> | wc -l` per file.
- **The nine SQL-text classes:** 1,978 lines at `4e8702016`, 1,992 at head (diffs: `git diff 4e8702016 68f20eb4d -- core/src/main/java/com/legend/edge/{RelationTextLowering,PureMessages,UpstreamVerdictWords,EngineResultEnvelopes}.java`).
  - 1,361 code lines (no comments, blanks or imports).
  - ~900 code lines in methods returning `SqlExpr`, `SqlSelect` or `SqlQuery`: `scratchpad/audit/verify/methods.py`, a brace-matching method counter, run on the nine files.
- **Renames into the edge:** 31 with `git diff -M50% --name-status main...68f20eb4d | grep com/legend/edge/`.
  - Over the 29 core files: 7,884 code lines, 701 added, 91% unchanged. For each pair, `git diff -M50% -U0` counting added non-comment lines.
- **Extracted files' origin:** the share of their code lines (≥ 30 characters) found verbatim in main's core sources: `scratchpad/audit/verify/origin.py` over a `git archive main core/src/main/java` copy. Results are in `scratchpad/audit/verify/origins.txt`.
- **Duplication** (`jscpd --min-lines 8 --min-tokens 70`):
  - inside the edge: 13 clones, 287 duplicated lines (1.83%), 263 of them in PureText;
  - between edge and product: 205 lines.
- **Complexity** (`python3 -m lizard -l java -C 25 -L 150 core/src/main/java/com/legend/edge`): 1,147 functions, average CCN 4.0, 50 warnings.
- **DuckDB double text** (DuckDB 2.0.0 installed under `scratchpad/audit/verify/pylib`): `CAST(CAST(0.00001 AS DOUBLE) AS VARCHAR)` is `1e-05`, `1e20` is `1e+20`, `1e16` is `1e+16`, `12345678.0` is `12345678.0`, `3.0` is `3.0`.
- **Upstream sources read** (fetched to `scratchpad/audit/verify/upstream/` from raw.githubusercontent.com at the pinned tags):
  - legend-engine 4.145.0: `helperFunctions.pure`, `dateExtension.pure`, `relation/functions/toString.pure`, `s.pure`;
  - legend-pure 5.99.0: `toRepresentation.pure`, `Format.java` (interpreted), `PureStringFormat.java`, `CompiledSupport.java` (compiled).
- **Product MIR clean-up:** `grep -rlw <name> core/src/main/java/com/legend/sql` at head, against `git grep -lw <name> main -- core/src/main/java/com/legend/sql`:

  | Name | Files at main | Files at head |
  |---|---|---|
  | `VarSetPlaceholder` | 6 | 0 |
  | `TempTableInSplice` | 9 | 0 |
  | `DeferredTdsString` | 10 | 0 |
  | `FLOAT_PRINT` | 19 | 1 (a comment) |
  | `TDS_NULL_CELL` | 2 | 0 |
