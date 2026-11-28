# Edition-table inventory: what the table must gather, and what it cannot (trace 1, 2026-11-25)

Read-only. L = core/src/main/java/com/legend. Inventory ids from docs/records/edition-homework/r2/sql-inventory.md (T/P/S) and docs/records/edition-census-2026-11-25.md.

## Facts the table depends on

- **The lowering's choke point.** Every native call reaches `Scalars.lower` (L/lowering/Scalars.java:2818-2852) from six Lowerer sites. Each site wraps the result in `NullSemantics.verbatim(...)` (L/lowering/Lowerer.java:2277, 2301, 3521, 4344-4345, 4352, 4500). `FeatureRules.select` (L/lowering/FeatureRules.java:102-114) wins over `RULES.get`, then `rule.apply(call, args, styled)`.
- **Where Styled comes from.** It is built only in `PlanEnv.styled()` (L/planner/PlanEnv.java:178-183) from `style.twoForm(LoweringForms/DateTimeTexts/ListTexts)` and `style.facet(PureOnlyTexts, r)`. It is installed by `PlanEnv.configure` (:190-195), which `BodyPlanner.java:150` and `ProductStatements.java:381` call, and by `EnginePlans.java:171`.
- **Sites fixed to `Styled.PRODUCT` whatever the door.** These are PlanMaker.java:956 (the write count), Compiler.java:965 (`lowerResolved`), SeedableLets.java:43 (a probe whose tree is discarded), and ProductRelationText.java:366-371 (a relation's toString/toCSV cells).
- **Where the passes run.** `spellingPasses` runs at RENDER, never at lowering. Order: `AnsiSqlRenderer.render` → `passed()` (L/sql/dialect/AnsiSqlRenderer.java:128, 236-242) → `renderPasses()` (:259-289). That runs spellingPasses FIRST, then DateTimeText.Expansion, DateTimeCanonText.Expansion, CellTexts.Expansion/Rest, FloatShortestText.Expansion, the dialect's `passes()` (DuckDB: CarrierStrategies … SelectMerge, SelectLayers, scanOrderPasses, VariantEgress; L/sql/dialect/DuckDb.java:766-797), and StoredReads last. So the edge passes see the whole lowered tree after every lowering-time form and before every product pass.
- **The plan keeps the PRE-pass tree.** `PlanMaker.sql` stores `DataSources.byDigest(tree, …)` beside the rendered text (L/planner/PlanMaker.java:1041-1045). Moving a pass's rewrite into lowering keeps the SQL text identical but changes the carried tree in the plan data (see Not verified).
- **No PureTextOverridesTest.** PureText.java:18 cites `PureTextOverridesTest`, but no such file exists in the repo.
- **`floatLiteralKind` is already gone from PureText.** It survives at PlanEnv.java:85-89, Compiler.java:966-967, EnginePlans.java:171, ProductStatements.java:382 and BodyPlanner.java:155 (ignored, as asked).
- **Doors versus styles.** PCT's legend-pure route and the corpus both hand `UPSTREAM`, yet their doors differ:
  - PCT's legend-pure route: `PureComparisons.asPures` (pct/…/PctPlanRoute.java:145; ChannelB.java:411).
  - The corpus: `EngineComparisons.asTheDatabases` (MinimalCorpus.java:626 → UpstreamTestBodies.admitted, UpstreamTestBodies.java:~57).

  PCT's legend-engine route hands `PURE_V1` but does NOT run the A3 rewrite (PctPlanRoute.java:146, comparisons only). Only pure/v1 (PureV1Routes.java:273, 314, 357, 331-335), HostAnswers.java:423 and TestDataRoutes.java:420 run it. **So the table cannot be keyed by today's PlanStyle alone. It must be keyed by door.**

## A. The table: one row per decision point

Kind: MEANING (table), FORMAT (edge presenter), HARNESS (test tooling), DEAD (identical in every form). "Same" means same as product.

**Test-resource key.**

| Key | Resource | Test | What it pins |
|---|---|---|---|
| R1 | core/src/test/resources/edge/pure_v1_sql_text.tsv, pure_v1_plan_text.tsv, pure_v1_answers.tsv | PureV1SqlTextTest | PURE_V1, H2. Real SQL TEXT, but only about 30 requests |
| R2 | pure_v1_requests.tsv | PureV1EdgeGoldenTest | PURE_V1 answers |
| R3 | edge_cases_answers.tsv | EdgeCasesGoldenTest | answers, not text |
| R4 | core/src/test/resources/lean-sql/baseline.tsv | LeanSqlBaselineTest | PRODUCT. SIZE only (statements, chars, subselects); inputs channel_a.jsonl (2451) and core.jsonl (4399) |
| R5 | spec/src/test/resources/rcorpus/lean-sql-baseline.tsv | LeanSqlCorpusTest | UPSTREAM corpus. SIZE only |
| R6 | spec rcorpus *-roster.txt, *-register.txt | //spec:corpus_duckdb, //spec:corpus_h2 | verdicts |
| R7 | core/src/test/resources/lean-sql/cast-case-census.tsv | CastCaseCensus | node counts |
| R8 | integration/FloatTextTest, JoinedTextTest, EdgeSpellingHookTest, GraphKeyTextTest, PureOnlyTextTest; planner/IsNumericProbeTest; lowering/ReadThroughExecuteValuesTest; DatabaseSettingsH2Test, H2SettingsReadBackTest, ExecutionTraceTest | named tests | mostly ANSWERS run under a style |
| R9 | HookRulingTest, PlanStyleFacetsTest, ProductFormsUseTheirArgumentsTest, EdgeAuditOpenTest, CompatSpellingGuardTest, ArchitectureTest, JavaEvalLedgerTest | guard tests | structure, not SQL |

**Nothing pins whole-lane SQL text byte for byte.** Only R1 holds real text. Step one needs a new capture: for example, plan R4's two inputs and the corpus under all three styles and diff the text.

**The rows.**

| id (inv.) | Decided at (call sites; implementations) | PRODUCT | UPSTREAM | PURE_V1 | Kind | Granularity; per-call at `Scalars.lower`? | Detected by |
|---|---|---|---|---|---|---|---|
| raise (S-1, S-22) | `forms().raise` at Scalars.java:437 (shift), Scalars.java:3149 (styled `guarded`, reached from Scalars 477 rem, 1627 index, 1977 sqrt, 2599 quotient, 3141 substring; ListRules.java:27 range; DecimalKindRules.java:57 divide; Coercions.java:102 toOne; DateCtorRule.java:90,173; MixedEncoding.java:1041; TaggedLists.java:200), ListEncodings.java:265 (slice), CastPolicy.java:183 (impossible cast). Impls: ProductLoweringForms.java:26; EdgeLoweringForms.java:37 with `PureText.ENGINE_UNCHECKED` (PureText.java:169-174) | every check | every check (`PCT`, empty set) | plain op for DIVIDE_BY_ZERO, SQRT_OF_NEGATIVE, SHIFT_OUT_OF_RANGE, SIZE_NOT_MULTIPLICITY, SLICE_BOUNDS_INVERTED, RANGE_STEP_ZERO, CAST_IMPOSSIBLE | MEANING | Per raise kind, **but only at these forms.raise sites**. The same kinds are written UNSTYLED, and stay checked under PURE_V1, at ToOneGuard.java:68-70, Coercions.java:63-67 and :176-178, AnsiSqlRenderer.java:827/837 (render), Substitution.java:2323 (resolver). A blanket kind→plain rule would change SQL there. TEXT_NOT_INTEGER, TEXT_NOT_FLOAT and TEXT_NOT_DATE are in the set but no forms.raise site asks for them, so the membership is DEAD. The parses' plain form comes from the U-parse row; TEXT_NOT_DATE stays checked (MariaDb.java:2796, SQLite.java:2538) | R1, R2, R8 (PCT engine lane) |
| isNumeric (S-6) | Scalars.java:2614; Product :90, Edge :142 | `IS_NUMERIC` | `lower(s)=upper(s)` | same as UPSTREAM | MEANING | Per native call (Relational isNumeric id). Trivial | IsNumericProbeTest, R1 if present |
| parseDecimal (S-7) | Scalars.java:2507, in the 1-argument non-literal branch only; Product :70, Edge :118 | `CAST(.. AS DECIMAL(p,s))` of Decimal's type | `CAST(.. AS DECIMAL(5,2))` on every database | same as UPSTREAM | MEANING | Per call, plus an argument-shape condition (the literal and 3-argument branches, Scalars.java:2487-2503, are shared). Expressible as a whole replacement rule for the parseDecimal ids | R6 (corpus goldens), EdgeAuditOpenTest |
| parseJSON (S-8) | JsonLane.java:34; Product :77, Edge :126 | `CAST(x AS JSON)` | `CAST(REGEXP_REPLACE(x,'\}\s*\{','},{','g') AS JSON)` | same as UPSTREAM | MEANING | Per native call. Trivial | R6 |
| executeValues (P-6) | Lowerer.java:819, :4475; Product :41, Edge :69 → `oneBare` :77-91 | array as lowered | CASE COUNT=1 → MIN(value) else array | same as UPSTREAM | MEANING inside a program, FORMAT at the last step | Statement level: wraps a serialize's SqlSelect at a root or scalar serialize, not a native call | ReadThroughExecuteValuesTest, R6 |
| objectRefConn (P-8) | Lowerer.java:1611; Product :47, Edge :61 | `""` | legend-engine's H2 connection JSON | same as UPSTREAM | FORMAT | A named constant (closed choice) | R6 (graph goldens) |
| existsJoin (S367, §1.4) | Lowerer.java:2235; Product :84, Edge :135 | EXISTS as lowered | `ExistsJoins.rewrite` (join to distinct keys) | same as UPSTREAM | HARNESS (SQL shape only, CP-4) | Statement level: needs the filtered SqlSelect plus ExistsSites (alias, keysWhere from whereZones) | R5 (sizes), R6 |
| whereZones (S367) | Lowerer.java:2319; Product :113-130, Edge :158 (`EngineWhereZones`) | conjuncts appended | conjuncts ordered by zone, splits kept | same as UPSTREAM | HARNESS (shape) | Statement level: a stateful merger for each fresh statement; existsJoin reads it | R5, R6 |
| view, wireCast, relations, keyedCounts, computedPruning, unionFrame (§1.4; S-13 for keyedCounts) | Lowerer.java:1225, CastPolicy.java:55, Lowerer.java:522, 534-535, 605, 2578/2612/2962/2967/3000/3016; Product :31, 36, 52, 58, 64, 105; Edge :42, 47, 96, 103, 110, 150 | — | identical | identical | DEAD | None. keyedCounts folds on every door (S-13's "engine counts the data" is NOT built) | — |
| DateTime text, literal (P-2, S-16) | Scalars.java:3693 (`styled.dateTimes()`), EdgePureOnlyTexts.java:79,131; `Styled.LITE_DATE_TIMES` Styled.java:33; EdgeDateTimeTexts.java:24-27 | `DATETIME_TO_TEXT(x)` | written literal → `'<written>+0000'` StringLit; otherwise `DATETIME_TO_TEXT(x)` | same as UPSTREAM | MEANING when used further (S-16), FORMAT at the last step | Per call; needs the TypedCDate's written text, which the call holds. The non-literal half is the PP-dt pass row | R8 FloatTextTest/JoinedTextTest (answers), R1 |
| list text (P-3) | Scalars.java:2952 (`anyList`), TaggedLists.java:566 (`mixedList`); ProductListTexts.java:23,31; PureListTexts.java:26,33 | JSON array | `[a, b]` | same as UPSTREAM | MEANING when used further, else FORMAT | Per call. Trivial | R8, PCT |
| pure-only texts (T-5) | Scalars.java:1804 (toRepresentation), :3023 (instance @id), :3574 (`%r`), :3593 (`%0wd`); PlanEnv.java:179; EdgePureOnlyTexts.java:26-106 | refused by name | legend-pure's text | same as UPSTREAM | MEANING (refusal vs answer) | Per call / per format directive. Expressible | PureOnlyTextTest |
| trace cell (P-9) | Lowerer.java:2875 via PlanEnv.java:169-174; PlanStyle.java:112; PureText.java:159-161, 196-198 | the id alone | `-- "executionTraceID" : "<id>"` | same as UPSTREAM | HARNESS / FORMAT | A named constant pair | R1 (pure_v1_plan_text `activities.tds`), ExecutionTraceTest |
| result envelope (T-3) | JsonEmission.java:95 via PlanEnv.java:193; EngineResultEnvelopes.java:46 | refused | legend-engine's JSON envelope | same as UPSTREAM | HARNESS | Per call (executeLegendQuery) | R6 |
| U-parse (S-2, F31) | pass `UnguardedParses` PurePrintForms.java:47-61 (render). Product guard: AnsiSqlRenderer.java:905-911 (`guardedParseFloat`), :951-957 (`guardedParseInt`), dialect arms (MariaDb 1731/2285, SQLite 1421/2024, DuckDb 1364, MongoDb 650/1069) | guarded CASE raising TEXT_NOT_INTEGER / TEXT_NOT_FLOAT | plain `CAST(.. AS BIGINT)` / `CAST(.. AS DOUBLE)` (Reason.DIALECT) | same as UPSTREAM | MEANING | Node-shaped pass; see C. PARSE_INT is made only at Scalars.java:2470-2473, so it is per call. A USER Cast VARCHAR→DOUBLE is made by castFamily (Scalars.java:2757-2760, for parseFloat AND toFloat) and possibly by CastPolicy.java:90-197 and Lowerer.java:3553/4785, so it is not provably per call | R1/R2 (PureV1EdgeGoldenTest mentions F31), R6 |
| PP-float (P-1, S-15) | pass `PurePrintForms` on FLOAT_TO_TEXT, PurePrintForms.java:165-167 → `floatOf` :110-113, literal processor `FloatLiteral` :77-104. H2 = PRINT_AS_FLOAT (PureText.java:303-305); every other edge dialect PRINT_BARE. Created at LiteralSpelling.java:516 via Scalars.floatRepr :3525 (callers Scalars 2444, 2937, 3563; ProductRelationText 369; the edge's ReprParts) | the dialect's `floatToText` (six decimals) | `CAST(x AS VARCHAR)`; a literal x printed by legend-pure (DecimalLit), and on H2 `CAST(<lit> AS DOUBLE)` | same as UPSTREAM | MEANING used further (S-15), else FORMAT | Per node plus the target database (H2 vs the rest). Movable to `floatPrint` only if the lowering knows the database (PlanEnv.floatLiteralKind already derives it from the runtime, null when none is named) AND ProductRelationText's `Styled.PRODUCT` cells take it too (today the pass re-spells them: ProductRelationText.java:33-36 says so; FloatTextTest pins it) | FloatTextTest, R1, R6, R4/R5 sizes |
| PP-float render fallback (P-1) | each edge dialect's `floatToText` override (PureText.java 286-289, 309-312, …, 500-503). Reached by a FLOAT_TO_TEXT made AT RENDER: ClickHouse.java:2350 (printf `%s`) | the product's `floatToText` | `CAST(x AS VARCHAR)` | same | as PP-float | Dialect render arm. **Cannot be gathered at lowering** unless ClickHouse's format arm stops making the node | none specific (ClickHouse lanes) |
| PP-dt (P-2, S-16) | pass PurePrintForms.java:168-171 → `PureDateTimePrint.of` (PureDateTimePrint.java:28). Product: DateTimeText.Expansion (AnsiSqlRenderer.java:266). DATETIME_TO_TEXT made at Styled.java:34, EdgeDateTimeTexts.java:26, ProductRelationText.java:366, LitePrinting.java:38 | STRFTIME ISO + Z (DateTimeText) | legend-pure print composition (RTRIM/STRFTIME/CASE) | same | MEANING used further, else FORMAT | Per node, no context. Movable into `EdgeDateTimeTexts` except for ProductRelationText's `Styled.PRODUCT` cells (same window as PP-float) | FloatTextTest, R1 |
| TDSNull (P-4, S-14) | passes PurePrintForms.joined :125-149 (at a join) and TdsNullCells :66-71 (anything else). CELL_TEXT made at Scalars.java:1025 (list join), Lowerer.java:4280 + ValueCollections.rowCellsJoin :398-410 (row join), Coercions.java:166 (Lite.CELL_TEXT native from the TdsDesugars.java:162-167 desugar). Product: CellTexts.Expansion/Rest (CellTexts.java) | an empty cell skipped (CONCAT_WS / STRING_AGG skip; a lone cell its operand) | `COALESCE(x,'TDSNull')`; a row join becomes a CONCAT interleave with each separator | same | MEANING used further (S-14), else FORMAT | Each creation site knows its context. Coercions:166 is per call (Lite.CELL_TEXT id). Scalars:1025 and rowCellsJoin are inside the makeString/joinStrings rules, so per call with the join's shape. Needs no tree context if decided at creation | JoinedTextTest (answers), R6 (112 DuckDB / 107 H2 corpus tests per the inventory) |
| scan order (§1.4) | `scanOrderPasses` DuckDb.java:762, 792; edge PureText.java:273-276 (`legend.exec.engineScanOrder` system property → StableScanOrder) | none | ORDER BY added (only when the property is set: the corpus lane) | same as UPSTREAM | HARNESS | A pass over the whole statement. Not lowering | R6 DuckDB |
| raw() (§1.4) | edge DuckDb/Postgres `raw` PureText.java:264-268, 322-330; base AnsiSqlRenderer.java:310 | the statement as written | ENGINE_TEXT statements translated (keywords, types, case) | same | HARNESS | Dialect arm over a raw executeInDb text. Not lowering | ExecuteInDbDuckDbEdgeTest, R6 |
| SQLite decimal (T-10) | SQLite.java:1655-1680; edge PureText.java:400-403 | refused by name (DialectCapability) | REAL arithmetic, same text with no refusal | same | MEANING (a refusal vs an answer; no SQL difference) | Render arm on SQLite. It could be a lowering refusal given the target database and the operands' TypeFacts | EdgeSpellingHookTest |
| Mongo date format (P-13) | MongoDb.java:715, 826, 834, 919, 1600; edge PureText.java:488-490 | product format | `%Y-%m-%dT%H:%M:%S.%L+0000` | same | FORMAT / MEANING when used further | Dialect render arm (pipeline text). Not lowering | EdgeSpellingHookTest, CompatSpellingGuardTest |
| H2 settings (S-9) | edge `H2()` ctor PureText.java:295-298 → `UPSTREAM.twoForm(DatabaseSettingsForm)`; H2.java:104-106, 239; read by exec (JdbcStatements.java:232, PlanSessions.java:307-352) | H2 defaults | MODE=LEGACY, DEFAULT_NULL_ORDERING=HIGH | same | MEANING, session-wide | Plan TARGET data, not SQL. **Cannot be gathered at lowering** | DatabaseSettingsH2Test, H2SettingsReadBackTest, R6 H2 |
| graph keys (P-7) | RESOLVER: BodyPlanner.java:72-73 → StoreResolver.java:107 → GraphEmission.java:1835, 2927, 2931; PlanStyle.java:122-130; PureText.java:146-154 | the ISO-Z date / the String | `<written>+0000` / `'<s>'` | same | FORMAT (written into SQL as key text) | Phase H, not lowering. Needs a resolver entry or a presenter re-key | GraphKeyTextTest, R6 |
| null-unsafe = (S-12) | Lowerer.java:229-233 sets it; applied at the six `NullSemantics.verbatim` wrappers above (NullSemantics.java:101-105) | from the program's flag, any door | same | same | MEANING, but **a program flag, not an audience choice** (hard case 5) | Post-processes `Scalars.lower`'s output (NULL_SAFE_EQUAL → EQUAL). Expressible inside Scalars.lower | none found (no test names the flag) |
| corrected substring flag (S-3, T-4) | FeatureRules.java:91-95 → PURE_SUBSTRING = the unflagged rule (Scalars.java:1489-1494) | identical with or without the flag | identical | identical | DEAD in the core (read by the edge door: PureMeaningCheck.java:425, 462) | — | SubstrIndexingTest, SubstringRangeTest |
| PUSH_DOWN_ENUM_TRANSFORM | FeatureRules.java:47-48 (a no-op); edge EnginePlans.java:187, 398, 1004 | — | — | — | HARNESS (plan text only) | — | — |
| cross-type compare (T-1, T-2) | the doors before planning: EngineComparisons.java:28-44; PureComparisons.java:33-75 | the marker refused at planning | corpus: the database's comparison; PCT pure: false / false / -1 / database ordering / refused | the database's comparison | MEANING | Typed-tree rewrite BEFORE G½/H. A lowering rule keyed on the `TypedCrossTypeComparison` kind could emit the same leaf, but the resolver's and inliner's simplifications run on the door's literal today (a TypedCBoolean false), so SQL could differ. **Per door, not per style** | PureComparisonsTest, R6, PCT lanes |
| A3 substring / indexOf / substr / toOne (S-3, S-4, S-5) | PureV1Routes.java:331-335 → PureMeaningRewrite + PureMeaningCheck + EngineSubstitutes.java (substringAsGiven, sqlPosition, substrAsGiven, trustOne) | Pure's meaning (guarded) | Pure's meaning | pure/v1 door only: the database's own function. PCT's engine route NOT rewritten | MEANING | Statement context: "store-bound" roots found after a partial inlining, refused when a call is reached through a mapping, and the target database must be H2 or DuckDB (PureText.java:39-45). A function-id key alone is not enough | PureMeaningRewriteTest/EngineTest/CheckTest/PostgresTest, R1, R2 |
| test-body door (T-6, T-7, S144) | UpstreamTestBodies.java (admitted, admittedForPure) | refused / as written | rewritten | n/a | HARNESS | Typed body | R6, ChannelB |
| UpstreamPrints (census hard case 8) | UpstreamPrints.read (ChannelB.java:419, MinimalCorpus.java:633) | — | relation text read on a session and substituted | n/a | HARNESS | Needs a run. **Cannot be gathered at lowering** | UpstreamPrintsTest |
| twoForm Printing (census M/T) | TestPlanner.java:57-59 | LitePrinting | TestDataRoutes.FUNCTION | same | HARNESS / MEANING | Test-data planning | test-data tests |
| UpstreamPlans / ActivityTexts / RoutedQueries / ExecutedValues / EngineTestText (T-3) | PlanAllocations.java:113, 187, 257; PlanTextRoutines.java:27; BodyPlanner.java:311, 328; FramePlanner.java:136; Seeds.java:161; SeedSqlForms.java:39, 51; BodyDriver.java:81, 861; EngineTextGates.java:26 | refused / lite's own | legend-engine's text | same (`judgesEngineText` throws) | HARNESS | Not lowering | R1 plan text, R6 |

## B. Branches on the style or the flags outside L/edge (the sites in A not repeated)

These sites branch on the style or the flags. Sites that only pass the style along are carriers: PlanMaker, ProductStatements, ProgramState, SystemCatalog/SystemRoutes (`style.dialect`), GeneratedTestData, PlanningOptions, TestDoor, JudgePlans.shapeOf, and the dead `style` argument of PlanMaker.sql (L/planner/PlanMaker.java:1020-1046 never reads it; OutputPlan.java:103 passes PRODUCT).

- **The dialect choice itself.** `style.dialect(type)` at PlanMaker.java:541, SystemCatalog.java:38, SystemRoutes.java:32, TestPlanner.java:50, PlanEnv.java:87. This is the whole "edge subclass" mechanism: every A-row marked "pass" or "render arm" hangs on it.
- **TestPlanner.java:74** refuses a DATABASE/BOTH judge when `style != PRODUCT`. HARNESS.
- **EngineTextGates.java:26** branches on `judgesEngineText()`. HARNESS.
- **The forced-product sites.** PlanMaker.java:956, Compiler.java:965, SeedableLets.java:43 and ProductRelationText.java:366-371 force `Styled.PRODUCT`; they branch on nothing, which is itself a decision. ProductRelationText's cells are rendered by the edge dialect and so re-spelled by PP-float and PP-dt. The other three hold no text node (count, probe) or are product-only roads.
- **Lowerer.java:2277…4500** `legacyNullUnsafeEquals` (row "null-unsafe =").
- **ExecutionContext.java:176, 187-188; ContextReading.java:409-419.** The typer reads FeatureFlagOption from the program. These carry the flags, decide nothing per audience.
- **H2.java:239** takes DatabaseSettingsForm (row "H2 settings").
- **testexec, exec, result: no style branch** (grep: none). PureTestRunner and TestObserver carry the `Audience` (L/test/Audience.java), which is harness.

## C. The three passes in `PurePrintForms.passes` (L/edge/PurePrintForms.java:36-38)

`SqlRewriter` is bottom-up: a hook fires after the node's children were rewritten (L/sql/SqlRewriter.java:10-11, 50+). The three run in order, each over the whole statement, as `renderPasses` positions 1-3 (AnsiSqlRenderer.java:263).

1. **`PurePrintForms` (:160-173).**
   - Context: none beyond the node itself and its direct operands.
   - Fires on three nodes:
     - (a) `ReduceCollection(STRING_AGG, LIST_TRANSFORM(c, λ CELL_TEXT(t)))` → the lambda's body becomes `COALESCE(t,'TDSNull')`.
     - (b) `COALESCE(STRING_AGG(ArrayLit[CELL_TEXT…], sep), '')` → `CONCAT(COALESCE(c1,'TDSNull'), sep, …)`, with `sep` omitted when it is `''`, and a lone part unwrapped.
     - (c) `Call FLOAT_TO_TEXT(x)` → `Cast(x' , VARCHAR, USER)`, where `x'` for a FloatLit is the literal processor's form (H2: `Cast(DecimalLit, DOUBLE, DECLARED)`, others: `DecimalLit`); `Call DATETIME_TO_TEXT(x)` → `PureDateTimePrint.of(x)`.
   - At lowering instead? Yes for (a)/(b) at their creators: Scalars.java:1017-1030 builds exactly (a); ValueCollections.rowCellsJoin builds exactly (b), knowing `sep`. Yes for (c) at `LiteralSpelling.floatPrint`/`Styled.dateTimes`, given the target database for the Float-literal processor.
2. **`TdsNullCells` (:66-71).**
   - Context: none. Fires on every `CELL_TEXT` the first pass did not consume, meaning one not under a join: in practice the Lite.CELL_TEXT native (Coercions.java:165-167), i.e. `TDSRow.get(..)->toString()` from TdsDesugars.java:162-167. Becomes `COALESCE(x,'TDSNull')`. Product: CellTexts.Rest unwraps it to `x`.
   - At lowering: yes, a per-call rule for the `AT_LEGEND_LITE_CELL_TEXT` ids. "isJoined" is just `fn()==CELL_TEXT` (:151-153); the join context matters only to pass 1.
3. **`UnguardedParses` (:47-61).**
   - Context: the operand's type fact (`TypeFact.Typed VARCHAR`) and the Cast's `Reason`. Fires on `Call PARSE_INT(s)` → `Cast(s, BIGINT, DIALECT)`, and on any `Cast(v, DOUBLE, USER)` whose operand is typed VARCHAR → `Cast(v, DOUBLE, DIALECT)`. The product's guard is written by the RENDERER for those two shapes, so DIALECT-reason casts escape it.
   - At lowering: PARSE_INT yes (one creator, Scalars.java:2470-2473). The Cast half only if every source of a USER VARCHAR→DOUBLE cast is enumerated. castFamily serves both parseFloat and toFloat (Scalars.java:2474-2475, 2757-2760), and CastPolicy/Lowerer also make USER casts (list in A). A per-call rule for the parseFloat ids alone is byte-identical only if no other site yields that shape on the edge lanes (not verified).

**Interactions that would change the SQL text if the decision moved into lowering.**
- **The `Styled.PRODUCT` sites rendered by an edge dialect** (ProductRelationText.java:366-371, a relation's toString/toCSV cells) are re-spelled by the passes today. A lowering-time table consulted through Styled would leave them product-spelled. To stay byte-identical, those cells must read the table too, or the change must be ruled (that is "B2b's S6", ProductRelationText.java:33-36; FloatTextTest pins the window).
- **Nodes made after lowering.**
  - ClickHouse's render-time FLOAT_TO_TEXT (ClickHouse.java:2350) reaches the edge `floatToText` override. A lowering rule never sees it.
  - Whether any dialect `passes()` (after the spelling passes) makes CELL_TEXT, FLOAT_TO_TEXT, DATETIME_TO_TEXT or PARSE_INT: grep found none in L/sql besides ClickHouse:2350.
- **Lowering-time transforms between creation and render.** These are SubselectPrune/computedPruning, KeyedCounts, existsJoin and the lowering's folds. grep finds no lowering code that pattern-matches FLOAT_TO_TEXT, DATETIME_TO_TEXT, CELL_TEXT, PARSE_INT or a STRING_AGG over them. Only dialect passes do (CarrierStrategies.java:1390/1445/1508, Postgres.java:1220), and those run after the spelling passes either way. So moving the rewrite earlier should not change text, provided the rewritten nodes type the same in SqlTyping (COALESCE, CONCAT, Cast are typed structurally).
- **The plan's carried tree** (PlanMaker.java:1041) would hold the edge forms instead of the product nodes. The SQL text stays the same; the plan data changes.
- **Separate DateTime split.** The literal half of P-2 is already at lowering (EdgeDateTimeTexts); the computed half is in the pass. Moving both into `EdgeDateTimeTexts` is consistent.

## D. Verdict

- **Trivially table rows (a function id to a rule, or a closed constant):**
  - isNumeric, parsedJson, parsedDecimal (as a full replacement of the parseDecimal rule);
  - the pure-only texts (four sites), list texts (two sites), the DateTime literal text;
  - `TdsNullCells` (the Lite.CELL_TEXT id);
  - `UnguardedParses` for PARSE_INT;
  - objectReferenceConnection and the trace cell (named constants);
  - the raise kinds, as a kind→plain map consulted ONLY at the existing `forms().raise` sites (Scalars 437/3149, ListEncodings 265, CastPolicy 183).
- **Table rows with a condition:**
  - **PP-float** needs the target database (H2 vs the rest) at lowering, which the runtime gives (PlanEnv.java:85-89), plus ProductRelationText's forced-product cells.
  - **PP-dt** has the same ProductRelationText caveat.
  - **The join forms of TDSNull** are per call at the two makeString/joinStrings creators, needing their own shape: the separator, and a list vs a row.
  - **The parseFloat half of UnguardedParses** is safe only once every USER VARCHAR→DOUBLE cast source is accounted for.
- **Need a statement-level entry:** executeValues (a serialize's SqlSelect), existsJoin and whereZones (shape only; deletable under CP-4), and the A3 rewrite (store-bound roots after inlining, mapping refusal, database check).
- **Cannot be gathered at lowering:**
  - the H2 settings (plan target data, session-wide);
  - graph key texts (decided in the resolver, phase H);
  - the cross-type comparisons (typed-tree door rewrites before G½/H, and **per door**: the corpus and PCT-pure share UPSTREAM but answer differently);
  - the ClickHouse render-time Float text;
  - Mongo's date format, `raw()`, scanOrderPasses, SQLite's decimal refusal (dialect render arms or harness);
  - UpstreamPrints (needs a run);
  - the harness facets (UpstreamPlans, ActivityTexts, RoutedQueries, ExecutedValues, EngineTestText, ResultEnvelopes, Printing).
- **DEAD:** six LoweringForms methods, TEXT_NOT_* in ENGINE_UNCHECKED, CORRECT_SQL_SUBSTRING_INDEXING in the core, the `style` argument of PlanMaker.sql and JudgePlans.shapeOf.
- **The key rule:** the table must be keyed by **door** (pure/v1, PCT-engine, PCT-pure, corpus, product), not by today's three PlanStyles. PCT-engine and pure/v1 share PURE_V1 but differ on A3; PCT-pure and the corpus share UPSTREAM but differ on comparisons.

## Not verified

- **Whether any edge-lane query yields a `Cast(VARCHAR→DOUBLE, USER)` from CastPolicy.java:90-197 or Lowerer.java:3553/4785** rather than parseFloat/toFloat. This decides whether UnguardedParses' Cast half is per call.
- **Whether a Lite.CELL_TEXT made by Coercions.java:166 can sit inside a makeString join pattern,** which would make pass 1 consume it instead of TdsNullCells.
- **Whether a runtime-less plan can reach an H2 Float print,** where PlanEnv.floatLiteralKind returns null.
- **Whether the judges' SQL (VerdictSql) holds FLOAT_TO_TEXT, DATETIME_TO_TEXT or CELL_TEXT** that the passes re-spell under the edge styles.
- **Who reads the plan's carried tree** (PlanMaker.java:1041), and whether any digest, cache key or pin covers it.
- **The R8/R9 tests' exact assertions:** found by grep, not read in full. FloatTextTest and JoinedTextTest were seen to assert answers under UPSTREAM, not SQL text.
- **Every caller of Scalars.guarded's 4-argument form:** listed from grep. DateCtorRule, MixedEncoding and TaggedLists use kinds outside ENGINE_UNCHECKED, so they cannot differ either way.
