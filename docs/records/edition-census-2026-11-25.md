# Census of audience switches in legend-lite (read-only, at commit 7c2ac303, 2026-11-25)

A research agent read the code and classified every place lite does something different for an audience. Each place got a kind and a fate:

- **M (meaning):** changes what the program computes. Fate: an edition rule.
- **F (format):** changes how the answer is written out. Fate: the door's presenter.
- **T (test harness):** exists to judge upstream's tests. Fate: test tooling.
- **P (product option).** Fate: stays.
- **X (dead).** Fate: delete.

Path roots: `L` = `core/src/main/java/com/legend/`, `E` = `L/edge/`.

## The three styles today

- **`PlanStyle.PRODUCT`** (`L/database/PlanStyle.java:86`).
- **`PureText.UPSTREAM`** (`E/PureText.java:54`). Passed in by:
  - PCT's legend-pure route (`PctPlanRoute.java:96`);
  - PCT channel B (`ChannelB.java:448`);
  - the relational corpus (`MinimalCorpus.java:526,614`).
- **`PureText.PURE_V1`** (`E/PureText.java:180`). Passed in by:
  - pure/v1 (`E/EdgeServer.java:43`, `E/PureV1Routes.java:277,282`);
  - PCT's legend-engine route (`PctPlanRoute.java:97`);
  - wasm.

The closed lists of facets and two-form kinds are in `L/testplan/PlanStyleFacets.java` (4 facets at :24-26, 8 two-form kinds at :37-62).

## 1. PlanStyle's parts

| Part | What differs | Kind | Fate |
|---|---|---|---|
| `dialect(type)` | Which SQL dialect writes each statement. The edge's subclasses (`E/PureText.java:238-534`) override: the DECIMAL Float literal on DuckDB, H2, Postgres, Databricks and Trino (S20); `spellingPasses`/`floatToText` through `PurePrintForms` (S311-S316, F31); MongoDB's date format; SQLite's refusal of decimal arithmetic; the H2 settings; `raw()` (translating H2 text, DuckDB and Postgres); DuckDB's scan order | M and T mixed | Split: the meaning overrides become edition rules (typed natives the product dialects render); `raw` and scan order go to the harness; then the product uses `Databases.dialect` alone |
| `traceCell()` | legend-engine's `executionTraceID` comment around an execution id | T | test tooling |
| twoForm `DateTimeTexts` | a DateTime's text inside a message (S312) | M | edition rule |
| twoForm `ListTexts` | a list's text: JSON array vs `[a, b]` (S316) | M | edition rule |
| twoForm `TestDataPlanner.Printing` | the test-data generator's timestamp text | M or T | edition rule or harness |
| twoForm `ExecutedValues`, `RoutedQueries`, `ActivityTexts` | `executeLegendQuery`'s envelope, the routing activity, an activity's SQL | T | test tooling |
| twoForm `DatabaseSettingsForm` | H2's `MODE=LEGACY` and `DEFAULT_NULL_ORDERING=HIGH` | M, session-wide | hard case 4 |
| LoweringForms `raise` | under PURE_V1, legend-engine's plain operations instead of lite's checked ones | M | edition rule (`legend-engine`) |
| LoweringForms `view`, `wireCast`, `relations`, `keyedCounts`, `computedPruning`, `unionFrame` | identical in both forms | X | delete |
| LoweringForms `executeValues` | legend-engine's JsonBuilder writes one object bare | M inside a program, F at the last step | hard case 7 |
| LoweringForms `isNumeric`, `parsedDecimal`, `parsedJson` | legend-engine's `isNumeric` check, `DECIMAL(5,2)`, lenient `parseJSON` | M | edition rules |
| LoweringForms `objectReferenceConnection` | legend-engine's H2 connection JSON | F | presenter |
| LoweringForms `existsJoin`, `whereZones` | legend-engine's join shape; the answer is the same (S367) | T (SQL shape only) | harness, or delete (CP-4) |
| `graphKeyDate` / `graphKeyString` | a graph fetch key's text (`+0000`, a quoted String) | F, but written into the SQL | the writer re-keys it at the door (hard case 7) |
| facet `EngineTestText`, `judgesEngineText()` | the SQL-text assert arm | T | step 6 |
| facet `UpstreamPlans` | planToString, toSQLString, toDDL, the `$plan` handle, seed SQL text, test-data plan text | T | test tooling |
| facet `PureOnlyTexts` | `%0wd`, `%r`, an instance's `@id`, `toRepresentation` (S314) | M | edition rule |
| facet `ResultEnvelopes` | `executeLegendQuery`'s JSON envelope | T | test tooling |
| `PRODUCT` | the product's own forms | P | stays, folded into `Databases` |

## 2. Core classes outside the edge that read a switch

44 files found by the search, plus about 5 it missed:

| Kind | Classes |
|---|---|
| **M** | `PlatformTypes`, `ContextReading` and `ExecutionContext` (the typer reads feature flags written in a program); `DateTimeTexts`, `ListTexts`, `Lowerer`, `LoweringForms`, `Scalars` (the texts, forms and the null-unsafe equality flag); `PlanEnv` (the hub that turns facets into the lowering's configuration) |
| **T** | `ActivityTexts`, `BodyPlanner` (toDDL), `PlanAllocations`, `RoutedQueries`, `UpstreamPlans`, `PureTestRunner`, `TestObserver`, `BodyDriver`, `EngineTestText`, `EngineTextGates`, `EngineTextPlanning`, `HostAsserts`, `JudgePlans`, `Seeds`, `TestPlanner` (refuses the database judge for non-product styles), plus `FramePlanner`, `PlanTextRoutines`, `SeedSqlForms`, `Audience`, and the run-time edge channel (`testexec.EdgeRun`, `EdgeHearing`, `HostForms`, `NoEdgeRun`, `LiteHostForms`, `testexecutionplan.Edge`/`NoEdge`: 19 files) |
| **Carriers** (pass the style or the flags along, branch on nothing) | `ExecuteOptions`, `Execution`, `TypedQuery`, `PlanMaker`, `ProductStatements`, `ProgramState`, `SystemCatalog`, `SystemRoutes`, `GeneratedTestData`, `PlanningOptions`, `TestDoor`, `TestKinds` |
| **X** | `FeatureRules`, `RegistryKeys` and the feature registry (`Implementation`, `Registrations`, `ImplementationTable`, `PlatformRegistrations`); `OutputPlan`'s and `JudgePlans`' dead `style` arguments to `PlanMaker.sql`, which never reads it; `JsonGrammar` (one form); `AssertErrorNative` (a comment); `DatabaseAsserts`' dead `appealOn` |
| **P** | `Compiler` (uses the product style as a constant) |

The five feature flags:
- **`LEGACY_SQL_NULL_UNSAFE_EQUALS`:** live in the lowering. M.
- **`CORRECT_SQL_SUBSTRING_INDEXING`:** a no-op in the core, but the engine door reads it (`E/PureMeaningCheck:425,462`).
- **`PUSH_DOWN_ENUM_TRANSFORM`:** read only by the edge's plan text. T.
- **The other two:** refused when set. X.

## 3. The 85 edge classes, grouped

| Group | Classes | Kind | Fate |
|---|---|---|---|
| Styles and dialects | `PureText` | mixed | dissolves along the split above |
| legend-pure's texts inside a program | `PurePrintForms`, `PureDateTimePrint`, `EdgeDateTimeTexts`, `PureListTexts`, `EdgePureOnlyTexts` | M | edition rules; the passes deleted |
| Meanings | `EdgeLoweringForms`, `EngineComparisons`, `PureComparisons`, `PureMeaningCheck`, `PureMeaningRewrite`, `EngineSubstitutes`, `EngineDatabaseSettings` | M | edition rules (settings: hard case 4) |
| The test-body door | `UpstreamTestBodies` (S137, S144, S131's order-free rewrites, the cross-type rewrites) | M | edition rules |
| | `UpstreamPrints` | T | step 6 |
| Last-step print in Java | `PrintedRelations`, `PureRelationText`, `PureRelationTextWriter`, `PureValueText` | F | stay at the door |
| Upstream API, protocol, envelopes | `EdgeServer`, `EngineMount`, `PureV1Routes`, `PureV1Writer`, `EngineAnswers`, `HostAnswers`, `EngineEnvelope`, `PureMessages`, `StoreWords`, `Presenter`, `EngineGrammar`, `EngineSpans`, `ModelInput`, `ExecuteInputs`, `UpstreamRelationType`, `TestDataRoutes` (partly) | F | stay; `PureV1Routes`' meaning rewrites become edition rules |
| Plan-text natives | `EnginePlans`, `EngineAllocations`, `EnginePlanRows`, `PlanNode`, `PlanConn`, `PlanEnvelope`, `PlanEnumForm`, `PlanSupportFunctions`, `PlanText`, `PlanVariables`, `PreciseTypes`, `InProtocol`, `PurePrint`, `SetImplementations`, `RuntimeContexts`, `EngineParams`, `EngineText`, `RelationalMapperRenames`, `EngineSeedText`, `TestDataFetches`, `TestDataPlanText`, `SqlTextInputs` | T | test tooling |
| legend-engine envelopes inside a program | `EngineResultEnvelopes`, `EngineExecutedValues`, `AggAwareActivities` | T | test tooling |
| SQL-text arm, appeals, referee | `SqlTextVerdicts`, `SqlTextRuns`, `SqlTextReferee`, `EdgeEngineTestText`, `EngineTextJudgment`, `NotAConstant`, `Appeal`, `AppealJson`, `AppealRun`, `Appeals`, `PlannedAppeals`, `ReplayGates`, `SqlReplayReferee`, `CompatEdge`, `EffectTexts`, `RefereeLoad`, `RawLedger`, `EdgeCodec`, `CompiledPureHostForms` | T | step 6 |
| Corpus SQL compatibility | `RawSqlBoundary`, `StableScanOrder` | T | harness |
| Unclear | `ServiceTestRunner`, `EqualToJson`, `TdsObjectsWriter` | X? | hard case 12 |

## 4. Totals

| | M | F | T | P | X | carriers or mixed |
|---|---|---|---|---|---|---|
| PlanStyle's 30 parts | 10 | 2 | 10 | 1 | 6 | 1 mixed |
| 44 core files | 9 | 0 | 15 | 1 | 6 | 11 carriers, 2 interfaces |
| 85 edge files | 13 | 20 | 48 | 0 | 3 | 1 mixed |

**The largest piece is test harness:** 48 of the 85 edge files, 15 core files and 19 run-time files exist only to judge upstream's tests. Moving them into test tooling (step 6 and its follow-ons) is the biggest part of the cleanup.

## 5. Hard cases

1. **One style serves two audiences.** `UPSTREAM` serves both legend-pure's PCT and legend-engine's corpus. So legend-engine's meanings reach legend-pure's tests: F31's unguarded parses, `TDSNull`, the DECIMAL Float literal, the H2 settings, `isNumeric`, `parseDecimal(5,2)` and lenient `parseJSON`. And legend-pure's prints reach the corpus. Two editions means assigning every rule to one of them, which the code has never done.
2. **Some meanings depend on the database.** The DECIMAL Float literal applies on 5 dialects only; SQLite gets REAL arithmetic; MongoDB has its own date format; the engine door's rewrite is checked on H2 and DuckDB only. The database is decided at planning, not at typing.
3. **Store-boundness is decided after typing.** The `substring` rewrite flags only calls in code that reads a store, which needs inlining first, and it refuses calls reached through mappings.
4. **Session settings aren't expressions.** H2's `MODE=LEGACY` and `DEFAULT_NULL_ORDERING=HIGH` affect every statement, and there is no typed call to rewrite.
5. **Some flags are written in the program,** not set by the audience. `LEGACY_SQL_NULL_UNSAFE_EQUALS` is one; the typer already reads it. Whether the `lite` edition refuses it is unruled.
6. **Upstream's text as a value inside a program:** planToString, toSQLString, toDDL, `executionPlan` handle rows, `executeLegendQuery` envelopes, activity SQL. These aren't formats, and CP-4 never promises upstream's SQL text, so no edition rule can produce them.
7. **Formats the database builds into JSON:** the graph fetch key text, `objectReferenceConnection`, JsonBuilder's bare single object, the trace cell. At the last step they're formats; inside a program they're meaning.
8. **The harness computes a value in Java:** `UpstreamPrints` prints a relation in Java and substitutes the text into the body.
9. **Error words read inside a program:** PCT's `assertError` compares legend-pure's message inside the body.
10. **Deletable now:** the six identical LoweringForms forms; the dead `style` parameters of `PlanMaker.sql` and `JudgePlans.shapeOf` and their callers; `JsonGrammar`; `DatabaseAsserts`' dead branch; `FeatureRules` and the feature registry.
11. **Answer-neutral SQL shape:** `existsJoin` and `EngineWhereZones` change only SQL text, which CP-4 no longer promises.
12. **`ServiceTestRunner`, `EqualToJson`, `TdsObjectsWriter`:** a product-like feature (running a user's service test suites) built on legend-engine's comparator, with no caller in main code. It fits none of the kinds.
13. **The register:** S311 to S316 live in `docs/records/d-register-rows-2026-11-16.md`, not in `SEMANTICS_REGISTER.md`; S403 isn't committed yet; S404 is a kept draft.

**Already in the edition's shape** (typed query in, typed query out, after typing and before planning): `EngineComparisons.asTheDatabases`, `PureComparisons.asPures`, `UpstreamTestBodies.admitted`, `PureMeaningRewrite.rewrite`, `PrintedRelations.lift`. What changes for them is who calls them: today the door, in the design the typer under an edition.
