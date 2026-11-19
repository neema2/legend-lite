# H8: the phase-0 measurements, slice 0.4 (census B2, 2026-11-19)

The compatibility program's slice 0.4 (plan r5, `docs/records/compat-program-plan-2026-11-19.md` on
`claude/compat-program`, §4 and §3). Each item is committed as it is measured. Every number is reproducible from a
probe in `H8-probes/`. The measurements are at `main` `b75d6c73a`, or at `claude/census-b2`, whose product equals it
(the branch differs from main only in `tools/meaning`, `tools/guards`, `gates`, docs and visibility lines).

Items: (a) the strand map (with (a.3), the `Feature` flags, plan r6), (c) legend-engine's unchecked operations, (d) the
strands' doors, (e) CP-4's remaining cost, (h) H2 without `MODE=LEGACY`, (j) CP-5's cost, (k) E-R2 against the corpus
(plan r6). Item (b) is F1 (`docs/records/census-b2-sqltext-probe-2026-11-19.md`).
B1 takes (f), (g) and (i).

No pin, ceiling, ledger or allowlist moves in this slice.

## (a) The strand map, confirmed

### (a.1) The ratchet's start: `PlanStyle` and `LoweringForms` in the product

The probe is `H8-probes/H8a_StrandMap.java.txt`, and its full output is `H8-probes/H8a-out.txt`.
- **What it reads:** the bytecode of every product class, meaning `//core:product_jars` plus the hosts' Java
  (`//tools/guards:meaning_host_jars`), outside `com/legend/edge/`.
- **What it counts,** for each hook type:
  - its declared methods;
  - parameters of its type on product methods and constructors (a record's canonical constructor included; synthetic
    methods and bridges left out);
  - fields of its type;
  - calls OF its methods (invoke instructions on the type, and method references to it);
  - calls CARRYING it (invoke instructions whose target takes a parameter of the type);
  - the product classes naming it in their constant pool.

| Count at `b75d6c73a` | `PlanStyle` | `LoweringForms` |
|---|---:|---:|
| Declared methods | 10 | 15 |
| Product classes naming it (bytecode) | 36 | 10 |
| Product source files naming it, comments included (`grep -rln`, outside `edge/`, itself excluded) | 42 | — |
| **Parameters of its type** | **49**, in 19 classes | 6, in 3 classes |
| Fields of its type | 4 | 1 |
| **Calls of its methods** | **50** | 25 |
| **Calls carrying it** | **112**, in 22 classes | 11, in 4 classes |

`PlanStyle`'s 50 calls, by method:
- `facet`: 16 (both overloads);
- `twoForm`: 16;
- `dialect`: 8;
- `judgesEngineText`: 4;
- `siteWords`: 2 (one a method reference);
- `errorWords`, `traceCell`: 1 each;
- `graphKeyDate`, `graphKeyString`: 1 each, by method reference (`BodyPlanner:72`).

The hosts' Java adds no use: the 12 extra classes name neither type, so every use is in core.

**Against the handoff's "45 parameters and 37 call sites"** (no method recorded): the bytecode count is 49
parameters and 50 calls of the style's methods. The parameters are spread as follows:
- `PlanMaker`: 14;
- `ProductStatements`: 5;
- `TypedQuery`: 4;
- `EngineTextGates`, `GeneratedTestData`: 3 each;
- the other 14 classes: 1 or 2 each.

The shrink-only ratchet's start is therefore **49 parameters, 4 fields, 50 calls of its methods, 112 calls carrying
it**, each reproducible from the probe. It ends at 0 when `PlanStyle` dies (slice 4.1).

**The census's (a) count at `main`** (the shrink-only pin's start, not added; adding it is the user's word): **142**,
plus 2 in (p), of 1,244 hits. That is `//tools/guards:meaning_census_test` at `claude/census-b2` `bd9ba3476`, whose
product equals `b75d6c73a`, with the user's rulings applied (record:
`docs/records/census-b2-inventory-2026-11-19.md`).

### (a.2) §3's rows against the code

**Confirmed as written:**
- **The facet kinds,** 5 (`testplan/PlanStyleFacets.KINDS`): `EngineTestText`, `UpstreamPlans`, `RelationTexts`,
  `PureOnlyTexts`, `ResultEnvelopes`.
- **The two-forms,** 16 (`PlanStyleFacets.TWO_FORMS`): `DateTimeTexts`, `ListTexts`, `JsonGrammar`,
  `RelationCounting`, `TestDataPlanner.Printing`, `CrossKindNumbers`, `VerdictNumbers`, `ExecutedValues`,
  `RenderedTextRule`, `DateLiteralTexts`, `GoldenNullWord`, `RoutedQueries`, `ActivityTexts`, `LoweringForms`,
  `VerdictWords`, `DatabaseSettingsForm`. All 16 are §3 rows; `LoweringForms` is split by member.
- **`LoweringForms`'s 15 members,** each compared body to body (`edge/EdgeLoweringForms.java` against
  `lowering/ProductLoweringForms.java`):
  - `view`, `wireCast`, `relations`, `keyedCounts` and `computedPruning`: identical in the edge and the product (edge
    `:44`, `:49`, `:104`, `:111`, `:118`; product `:31`, `:36`, `:58`, `:64`, `:70`).
  - `unionFrame`: null in both (edge `:168`, product `:111`).
  - Only `EngineShape` (`:186`) differs, at `:189`, `:195`, `:203`, `:223` and `:234`. So these rows are S, as written.
  - `existsJoin` and `whereZones` differ in EXECUTED SQL: the edge has `ExistsJoins.rewrite` and `EngineWhereZones`,
    the product `filtered` and `APPENDED`.
  - `isNumeric` differs: the edge has `lower(s) = upper(s)`, the product `IS_NUMERIC`.
  - `parsedDecimal` differs: the edge has `CAST(text AS DECIMAL(5,2))`, the product the platform's Decimal.
  - `parsedJson` differs: the edge has a `regexp_replace` of `}\s*{` and a cast, the product a JSON cast.
  - `raise` differs: the edge has the plain set, the product always `checked`.
  - `executeValues` differs: the edge has `oneBare`, the product `own`.
  - `objectReferenceConnection` differs: the edge has the H2 test connection's JSON, the product `""`.
  - `assertFailed` differs: the edge has "Assert failed", the product `AssertSentence.TRUE`.
- **`PureText`'s dialect passes** (`edge/PureText.java`): 12 subclasses.
  - All 12 override `spellingPasses` and `floatToText`.
  - `floatLiteralKind`: H2 (`:406`), DuckDB, Postgres (`:438`), Databricks (`:484`), Trino (`:649`).
  - `raw`: DuckDB and Postgres.
  - `refusesDecimalArithmetic`: SQLite.
  - `dateTimeFormat`: MongoDB (`:705`).
  - `scanOrderPasses`: DuckDB alone (`:367`).
- **The cited call sites:**
  - `planner/PlanEnv.java:173-177` (`traceCell`'s concatenation);
  - `resolver/GraphEmission.java:1846` and `:2938`, `planner/BodyPlanner.java:72` (the graph keys);
  - `testexecutionplan/TestPlan.java:45-48` (`answerSizeWords`, `words`) and `SideShape.java:37` (`sizeWords`);
  - `testplan/AssertErrorNative.java:81` (`style.errorWords(e)`);
  - `resolver/RelationalRootForm.java:51` (`apply`), called at `edge/EnginePlans.java:164`;
  - `RelationalRootForm.primaryKeyColumns` on the six product paths cited (`ObjectReferenceDecode:133`,
    `ClassSources:797`, `CorrelatedSubselects:321/486/1094`, `GraphEmission:533`);
  - `edge/EnginePlans.java:174` (`engineSql`), `:754` and `:767` (`setUpDataSqlsText`), `:958-995` (`ddlText`).
- **The runner's hooks exist as named:** `testexec` `EdgeRun`, `EdgeHearing`, `HostWords`, `HostForms`;
  `testexecutionplan` `Edge`, `NoEdge`; `ExecuteOptions.recording` (`:82`).
- **The census word groups** C05, C08, C10, C11, C14-C17 and C19: 24 hits in H1's `H1-census-hits.tsv`, as written.
- **Sizes:** `PlanStyle` 173 lines; the edge 93 files, 21,988 lines; `RelationTextLowering` 791; `StableScanOrder`
  598; `EngineStyleH2` 2,507, `EngineStyleDB2` 365, `EngineStyleComposite` 79; and in the appeal stack
  `SqlTextVerdicts` 1,675, `AppealJson` 273, `SqlTextRuns` 291, `SqlTextReferee` 231, `Appeal` 191,
  `SqlReplayReferee` 173, `PlannedAppeals` 56, `AppealRun` 40, `RefereeLoad` 34, `EffectTexts` 28,
  `EngineTextJudgment` 18.

**Corrected:**

| §3's text | The code |
|---|---|
| `floatLiteralKind` "DuckDB `:318`" | DuckDB's override is at `edge/PureText.java:350` |
| `raw` "`edge/PureText.java:326-330`" | `:326-330` is `PureText.dialect`'s switch; `raw` is at `:359` (DuckDB) and `:447` (Postgres) |
| `refusesDecimalArithmetic` "SQLite `:535`" | `:567` |
| The appeal stack: `Appeals` 354, total "about 3,360 lines" | `Appeals.java` is 401 lines, so the twelve files total 3,411 |
| §11: "`LoweringForms`: its 17 members" | 15, as §3 says |
| `PlanStyle`'s methods: no row for `siteWords` | **A missing strand.** `PlanStyle.siteWords(text, sites)` records legend-pure's words for a statement's raise sites (`PureText.recording`'s `SiteWordsRecorder`, minted per planning call). It is called twice in the product: once by an invoke, once by a method reference. It belongs beside `errorWords` (P, slice 1.2) and should be a row. |

Every other row's hook, its file and the behaviour §3 describes are confirmed. The classes and slices are the plan's
own; the code supports each where it states a fact (S where the executed form already equals the product's; R0, P or
J where it differs).

### (a.3) A strand the map missed: upstream's `Feature` flags inside the core

The independent review's addition (plan r6, `af6d5a9a`, §3's new row; class "a mode by another name", slice 3.2: the
translator's input, or not supported, under CP-12). Probe: `H8a2_Features.java.txt` over `//core:product_jars` and
`//core:edge`'s `libedge.jar` (output `H8a2-out.txt`); upstream's carriers by `H8a2_flag_tests.py.txt`
(`H8a2-flag-tests.tsv`).

**The strand.** `platform/Feature` mirrors legend-engine's `meta::pure::executionPlan::features::Feature` member for
member (5 members; spec's `FeatureFlagParityTest` holds them equal). The core consumes it at lowering:
- `Lowerer.withFeatures` (`lowering/Lowerer.java:230-236`) checks each flag has a consumer
  (`FeatureRules.requireConsumed`) and keeps the set.
- **`LEGACY_SQL_NULL_UNSAFE_EQUALS` switches the core's equality.** It sets `legacyNullUnsafeEquals`, which picks
  `NullSemantics.verbatim` (plain `=` for `IS NOT DISTINCT FROM`) at the 6 scalar sites: `aroundReducer:2288`,
  `overReducers:2312`, `windowScalar:3532`, `scalarValueTailArms:4339` and `:4347`, `formatted:4495`.
- **`CORRECT_SQL_SUBSTRING_INDEXING` switches `substring`.** `FeatureRules.select` swaps in `CORRECTED_SUBSTRING`
  (Pure's 0-based, end-exclusive indexes to SQL's 1-based start and length) for the `substring` overloads
  (`FeatureRules.java:77`), through `Scalars.lower`'s `features` argument at the same 6 sites.
- `PUSH_DOWN_ENUM_TRANSFORM` is a documented no-op in the product; its one consumer is the edge's plan text
  (`EnginePlans.engineSql:190`, `planToString:402`, `pushDownEnums:1008`).
- `VARIANT_TYPE_AS_INPUT` and `USE_DB_NATIVE_IMPLICIT_NULL_ORDERING` have no consumer. Either one, when set, is refused
  by name ("feature flag ... is not implemented by the platform").

**Its carriers, all in the product:**
- an execute call's `ExecutionOptionContext` (`ExecutionContext.withOptions`, `ContextReading.contextFeatures`);
- a `withFeatureFlags` call in the query body (`ExecutionContext.treeFeatures`);
- a runner's defaults (`ExecuteOptions.withFeatures`, `PlanningOptions.withFeatures`);
- the product's own doors: `TypedQuery.executionPlan(runtime, features)` (`TypedQuery.java:223`) and `Execution.run(...,
  features, ...)` (`Execution.java:65`, `:77`).

So unlike the `PlanStyle` strands, a product query reaches it with no style at all.

**The ratchet's start** (bytecode, 2026-11-19, at the product of `b75d6c73a`):

| Measure | Product (outside `com/legend/edge/`) | Edge |
|---|---:|---:|
| Classes naming `Feature` | 20 | 1 (`EnginePlans`) |
| Loads of a member (`getstatic`) | 4: `LEGACY_SQL_NULL_UNSAFE_EQUALS` 2 (`FeatureRules.<clinit>:45`, `Lowerer.withFeatures:233`), `CORRECT_SQL_SUBSTRING_INDEXING` 1 (`FeatureRules.<clinit>:77`), `PUSH_DOWN_ENUM_TRANSFORM` 1 (`FeatureRules.<clinit>:45`) | 3, all `PUSH_DOWN_ENUM_TRANSFORM` |
| Calls of the setters, readers and `FeatureRules` | 28: `ProgramState.features` 11, `PlanningOptions.features` 3, `BodyPlanner.featuresOf` 2, `Lowerer.withFeatures` 2, `PlanEnv.features` 2, and 1 each of `ContextReading.contextFeatures`, `ExecutionContext.features`, `ExecutionContext.withFeatures`, `FeatureRules.requireConsumed`, `FeatureRules.select`, `FeatureRules.under`, `PlanEnv.withFeatures`, `PlanningOptions.withFeatures` | 8 |
| Reads of the Lowerer's two flag fields | 12: `legacyNullUnsafeEquals` 6, `features` 6 | 0 |
| Generic signatures carrying `Feature` (`Set<Feature>` parameters, fields, returns; distinct strings per class) | 62 in 17 classes (`ProductStatements` 7; `ExecutionContext`, `PlanEnv`, `PlanMaker`, `TestDoor` 5 each; ...) | 1 |

The probe's raw call total is 30; 2 of those are `RelationalConnectionComposer.features`, a same-named helper
unrelated to flags, and are left out of the 28.

**Who turns a flag on** (the strand's doors, beside item (d)):
- **PCT A and PCT B, always:** `CORRECT_SQL_SUBSTRING_INDEXING` on every query (`pct/.../PctPlanRoute.java:46`,
  `channelb/ChannelB.java:392`), as legend-engine's testable runner does.
- **The corpus, per test.** The test's own carriers decide. Upstream's tree has 8 tests that set one; the 7 in the
  corpus's root are:

  | Test | Flag | H2 | DuckDB |
  |---|---|---|---|
  | `legacyNullUnsafeEquals::testLegacyFlagProjectionEmitsPlainEquals` | `LEGACY_SQL_NULL_UNSAFE_EQUALS` | PASS | PASS |
  | `legacyNullUnsafeEquals::testLegacyFlagRestoresOptionalParamFreeMarkerSelector` | `LEGACY_SQL_NULL_UNSAFE_EQUALS` | FAIL | FAIL |
  | `executionPlan::tests::testSubstringIndexingCorrectedByFeatureFlag` | `CORRECT_SQL_SUBSTRING_INDEXING` | PASS | PASS |
  | `enumPushDown::testPushDownProject` | `PUSH_DOWN_ENUM_TRANSFORM` | PASS | PASS |
  | `enumPushDown::testPushDownProjectWithParameter` | `PUSH_DOWN_ENUM_TRANSFORM` | FAIL | PASS |
  | `sqlQueryToString::tests::testUseDbNativeImplicitNullOrderingResolutionExeCtxFlag` and `...OrPrecedence` | `USE_DB_NATIVE_IMPLICIT_NULL_ORDERING` | FAIL (refused by name) | FAIL |

  The eighth, Databricks' `testUseDbNativeImplicitNullOrderingWithFeatureFlagsAtQuerySurface`, is outside the
  corpus's root. The verdicts are from the committed host ledgers (`spec/src/test/resources/edge/corpus_*_host_judge-host.tsv`).
- **pure/v1 and the product's `Execution.run`:** whatever flags the query's own context carries.
- **Inside legend-engine itself:** its SQL protocol's `queryToPure::getPlan` sets `LEGACY_SQL_NULL_UNSAFE_EQUALS`, and
  its mapping-less `executionPlan` sets `PUSH_DOWN_ENUM_TRANSFORM` (`H8a2-flag-tests.tsv`'s `fn` rows). Lite's
  `protocol/sql` sets no flag.

## (c) legend-engine's unchecked operations (its raise class A), per database

**The question:** which of lite's checked operations does legend-engine leave unchecked, on which database? Slice 3.6
needs the answer to count the register rows CP-5 creates.

**The evidence** is legend-engine 4.145.0's PCT manifests, all 77 of them (7 scopes, 11 relational databases), spec
input only. Each manifest entry is legend-engine's recorded result for a test, judged on its ACTUAL message (an
entry's "expected message was ..." half is ignored):
- **pass:** no entry. legend-engine raised legend-pure's words.
- **chk:** legend-engine raised legend-pure's words, but the test failed on the wrapping.
- **VAL:** "No error was thrown": the database returned a value.
- **db:** the database raised its own error (legend-engine's wrapper "Unexpected error executing function", or the
  database's text, for example SQL Server's "An invalid floating point operation occurred").
- **ref:** refused at planning ("No SQL translation", "supported only after direct access", "Ensure the target system
  understands Year...", unsupported-api).

**The probes:**
- `H8c_pct_error_tests.py.txt` lists the PCT tests that assert an error (`assertError` in a `<<PCT.test>>` function,
  across both pinned trees): 35 asserts in 26 tests, in `H8c-pct-error-tests.tsv`.
- `H8c_raise_class_a.py.txt` reads every database's entry for each, into `H8c-engine-per-database.tsv`.
- `H8c_PlainSet.java.txt` reads the product's bytecode for the raise kinds `LoweringForms.raise` can reach
  (`H8c-plainset-out.txt`).
- `H8c-lite-raise-sites.tsv` counts lite's raise sites per kind.

### (c.1) legend-engine per test and database

| Kind | PCT test | clickhouse | databricks | duckdb | h2 | memsql | oracle | postgres | snowflake | spanner | sqlserver | trino |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| INDEX_OUT_OF_BOUNDS | `testAtError` | ref | ref | ref | ref | ref | ref | ref | ref | ref | ref | ref |
| RANGE_STEP_ZERO | `testRangeStepError` | db | chk | **VAL** | ref | ref | ref | ref | db | ref | ref | ref |
| SLICE_BOUNDS_INVERTED | `testSliceError` | ref | chk | **VAL** | ref | ref | ref | ref | **VAL** | ref | ref | ref |
| DATE_PART_ABSENT | `testDayOfMonthError` | ref | ref | ref | ref | ref | ref | ref | ref | ref | ref | ref |
| DATE_PART_ABSENT | `testHourError` | ref | ref | ref | ref | ref | ref | ref | ref | ref | ref | ref |
| DATE_PART_ABSENT | `testMinuteError` | ref | ref | ref | ref | ref | ref | ref | ref | ref | ref | ref |
| DATE_COMPONENT_INVALID | `testNewDateError` | ref | ref | ref | ref | ref | ref | ref | ref | ref | ref | ref |
| DATE_PART_ABSENT | `testSecondError` | ref | ref | ref | ref | ref | ref | ref | ref | ref | ref | ref |
| (none: a planning refusal) | `testTimeBucketHours` | pass | pass | pass | pass | pass | pass | pass | pass | pass | pass | pass |
| (none: a planning refusal) | `testTimeBucketMinutes` | pass | pass | pass | pass | pass | pass | pass | pass | pass | pass | pass |
| (none: a planning refusal) | `testTimeBucketSeconds` | pass | pass | pass | pass | pass | pass | pass | pass | pass | pass | pass |
| SHIFT_OUT_OF_RANGE | `testBitShiftLeft_MoreThan62Bits` | ref | chk | db | **VAL** | ref | ref | ref | db | ref | ref | ref |
| SHIFT_OUT_OF_RANGE | `testBitShiftRight_MoreThan62Bits` | ref | chk | **VAL** | **VAL** | ref | ref | ref | **VAL** | ref | ref | ref |
| DIVIDE_BY_ZERO | `testRemError` | db | db | **VAL** | db | **VAL** | **VAL** | db | db | db | db | db |
| SQRT_OF_NEGATIVE | `testSquareRootError` | **VAL** | **VAL** | db | **VAL** | **VAL** | **VAL** | db | db | db | db | **VAL** |
| (none: lite has no kind) | `testArcCosineError` | **VAL** | **VAL** | db | **VAL** | **VAL** | **VAL** | db | db | db | db | **VAL** |
| (none: lite has no kind) | `testArcSineError` | **VAL** | **VAL** | db | **VAL** | **VAL** | **VAL** | db | db | db | db | **VAL** |
| WINDOW_FRAME_INVERTED | `testRange_InvalidWindowFrameBoundary` | pass | pass | pass | pass | pass | pass | pass | pass | pass | pass | pass |
| WINDOW_FRAME_INVERTED | `testRows_InvalidWindowFrameBoundary` | pass | pass | pass | pass | pass | pass | pass | pass | pass | pass | pass |

The variant-conversion tests (9) are left out: lite's relational lowering plants no raise kind for them.

### (c.2) Class A per database: the kinds lite checks and legend-engine does not

| Database | Unchecked: a VALUE returned | Unchecked: the database's own error | Refused at planning | Checked |
|---|---|---|---|---|
| clickhouse | SQRT_OF_NEGATIVE | DIVIDE_BY_ZERO, RANGE_STEP_ZERO | DATE_COMPONENT_INVALID, DATE_PART_ABSENT, INDEX_OUT_OF_BOUNDS, SHIFT_OUT_OF_RANGE, SLICE_BOUNDS_INVERTED | WINDOW_FRAME_INVERTED |
| databricks | SQRT_OF_NEGATIVE | DIVIDE_BY_ZERO | DATE_COMPONENT_INVALID, DATE_PART_ABSENT, INDEX_OUT_OF_BOUNDS | RANGE_STEP_ZERO, SHIFT_OUT_OF_RANGE, SLICE_BOUNDS_INVERTED, WINDOW_FRAME_INVERTED |
| duckdb | DIVIDE_BY_ZERO, RANGE_STEP_ZERO, SHIFT_OUT_OF_RANGE, SLICE_BOUNDS_INVERTED | SHIFT_OUT_OF_RANGE, SQRT_OF_NEGATIVE | DATE_COMPONENT_INVALID, DATE_PART_ABSENT, INDEX_OUT_OF_BOUNDS | WINDOW_FRAME_INVERTED |
| h2 | SHIFT_OUT_OF_RANGE, SQRT_OF_NEGATIVE | DIVIDE_BY_ZERO | DATE_COMPONENT_INVALID, DATE_PART_ABSENT, INDEX_OUT_OF_BOUNDS, RANGE_STEP_ZERO, SLICE_BOUNDS_INVERTED | WINDOW_FRAME_INVERTED |
| memsql | DIVIDE_BY_ZERO, SQRT_OF_NEGATIVE | — | DATE_COMPONENT_INVALID, DATE_PART_ABSENT, INDEX_OUT_OF_BOUNDS, RANGE_STEP_ZERO, SHIFT_OUT_OF_RANGE, SLICE_BOUNDS_INVERTED | WINDOW_FRAME_INVERTED |
| oracle | DIVIDE_BY_ZERO, SQRT_OF_NEGATIVE | — | DATE_COMPONENT_INVALID, DATE_PART_ABSENT, INDEX_OUT_OF_BOUNDS, RANGE_STEP_ZERO, SHIFT_OUT_OF_RANGE, SLICE_BOUNDS_INVERTED | WINDOW_FRAME_INVERTED |
| postgres | — | DIVIDE_BY_ZERO, SQRT_OF_NEGATIVE | DATE_COMPONENT_INVALID, DATE_PART_ABSENT, INDEX_OUT_OF_BOUNDS, RANGE_STEP_ZERO, SHIFT_OUT_OF_RANGE, SLICE_BOUNDS_INVERTED | WINDOW_FRAME_INVERTED |
| snowflake | SHIFT_OUT_OF_RANGE, SLICE_BOUNDS_INVERTED | DIVIDE_BY_ZERO, RANGE_STEP_ZERO, SHIFT_OUT_OF_RANGE, SQRT_OF_NEGATIVE | DATE_COMPONENT_INVALID, DATE_PART_ABSENT, INDEX_OUT_OF_BOUNDS | WINDOW_FRAME_INVERTED |
| spanner | — | DIVIDE_BY_ZERO, SQRT_OF_NEGATIVE | DATE_COMPONENT_INVALID, DATE_PART_ABSENT, INDEX_OUT_OF_BOUNDS, RANGE_STEP_ZERO, SHIFT_OUT_OF_RANGE, SLICE_BOUNDS_INVERTED | WINDOW_FRAME_INVERTED |
| sqlserver | — | DIVIDE_BY_ZERO, SQRT_OF_NEGATIVE | DATE_COMPONENT_INVALID, DATE_PART_ABSENT, INDEX_OUT_OF_BOUNDS, RANGE_STEP_ZERO, SHIFT_OUT_OF_RANGE, SLICE_BOUNDS_INVERTED | WINDOW_FRAME_INVERTED |
| trino | SQRT_OF_NEGATIVE | DIVIDE_BY_ZERO | DATE_COMPONENT_INVALID, DATE_PART_ABSENT, INDEX_OUT_OF_BOUNDS, RANGE_STEP_ZERO, SHIFT_OUT_OF_RANGE, SLICE_BOUNDS_INVERTED | WINDOW_FRAME_INVERTED |

**The cells CP-5 turns into register rows** are the "a VALUE returned" ones. There lite raises Pure's error and
legend-engine answers the database's value: **16 test cells on 8 databases**.

| Database | VALUE cells | Kinds |
|---|---:|---|
| DuckDB | 4 | rem by zero, `range` step 0, `slice` inverted, `bitShiftRight` > 62 |
| H2 | 3 | `sqrt` of a negative, `bitShiftLeft` and `bitShiftRight` > 62 |
| MemSQL | 2 | rem by zero, `sqrt` |
| Oracle | 2 | rem by zero, `sqrt` |
| Snowflake | 2 | `slice`, `bitShiftRight` |
| ClickHouse | 1 | `sqrt` |
| Databricks | 1 | `sqrt` |
| Trino | 1 | `sqrt` |

Postgres, Spanner and SQL Server have none: their databases raise their own errors.

The "db" cells keep the answer's STATUS (an error on both sides; only the words differ, which are the presenter's).
The "ref" cells are E-R2's (lite answers where legend-engine refuses).

### (c.3) What the plain set can reach, and lite's checks no PCT test exercises

- **The plain set reaches 7 kinds, not 10.** `LoweringForms.raise` is asked at 4 call sites:
  - `Scalars:435` (shifts);
  - `ListEncodings:265` (`slice`);
  - `CastPolicy:183` (casts);
  - `Scalars.guarded`'s styled overload (`Scalars:3024`), whose callers bring `DIVIDE_BY_ZERO` (3 sites: `Scalars:475`,
    `:2531`, `DecimalKindRules:57`), `SQRT_OF_NEGATIVE` (`Scalars:1924`), `RANGE_STEP_ZERO` (`ListRules:27`) and
    `SIZE_NOT_MULTIPLICITY` (`Coercions:100`).

  `PureText.ENGINE_UNCHECKED` declares 10 kinds. Its `TEXT_NOT_INTEGER`, `TEXT_NOT_FLOAT` and `TEXT_NOT_DATE` are raised
  in the dialects (`AnsiSqlRenderer`, `DuckDb`, `SQLite`, `MariaDb`, `MongoDb`), never through `LoweringForms.raise`.
  So those three entries change nothing: the pure/v1 route still writes lite's parse checks. A finding for 3.6: they
  are dead entries, and deleting the plain set moves no parse answer.
- **Lite plants 27 raise kinds at 72 sites** (`H8c-lite-raise-sites.tsv`). Only 9 kinds have a PCT error test:
  `INDEX_OUT_OF_BOUNDS`, `RANGE_STEP_ZERO`, `SLICE_BOUNDS_INVERTED`, `DATE_PART_ABSENT`, `DATE_COMPONENT_INVALID`,
  `SHIFT_OUT_OF_RANGE`, `DIVIDE_BY_ZERO`, `SQRT_OF_NEGATIVE` and `WINDOW_FRAME_INVERTED`.
- **The other 18 kinds are unmeasured by PCT.** `CAST_IMPOSSIBLE`, `SIZE_NOT_MULTIPLICITY`, the `TEXT_NOT_*` parses,
  `MATCH_FAILURE`, the map and list kinds, `VALUE_NOT_HELD`, `FRACTION_NOT_KEPT`, `CSV_TOO_WIDE`,
  `WINDOW_FRAME_OFFSET_INVALID`, `USER_FAIL` and the rest have no PCT error test. Where legend-engine's SQL generation
  is generic (`toOne` → `processNoOp`, `processCast`, the plain parse casts: the 2026-11-17 census), it writes no guard
  on any database, and the outcome is each database's. Two of them, `CAST_IMPOSSIBLE` and `SIZE_NOT_MULTIPLICITY`, are
  in the plain set's reach. Their per-database answers need the oracle, which item (j) asks for.
- **`acos` and `asin` of 2.0** (`testArcCosineError`, `testArcSineError`) answer a value on 6 databases under
  legend-engine. Lite has no raise kind for them, so lite does not check them either: they are not class A. Under the
  PURE audience, legend-pure raises (S138, being corrected in 0.5), so lite's PURE answer is the gap there, not
  ENGINE's.

## (d) The strands' doors: which door reaches each strand

### (d.1) The method

**The recorder.** `H8d_HookProbe.java.txt` counts every answer the edge's styles give, by hook. A static counter is
written at JVM exit, one file per run, headed by the run's door. `H8d_instrument_hooks.py.txt` plants it in a probe
worktree off `b75d6c73a`, never for main, at:
- every `PlanStyle` method of `PureText.UPSTREAM` and `PureText.PURE_V1`: a facet or two-form by kind, a dialect by
  database;
- every `EdgeLoweringForms` member: the executed form, and `EngineShape`'s text form apart;
- every `LoweringForms.raise` by kind, plain or checked;
- `PureText`'s dialect passes and the engine-text printer `engineText`;
- the appeal stack (`Appeals`, `AppealRun`);
- the runner's edge (`CompatEdge`'s constructor).

A count is answers given, not distinct sites. `PURE_V1` answers by delegating to `UPSTREAM`, so a delegated answer is
counted once (`H8d_hooks_matrix.py.txt`).

**The runs** (2026-11-19, 11:09-11:23 UTC, every suite passing): 21 JVMs, one file each in `H8d-hooks/`.
- **The corpus:** `//spec:judge_host_h2` and `//spec:judge_host_duckdb`.
- **PCT B:** the 12 `//pct:pct_channel_b_*` suites.
- **PCT A:** the 5 `//pct:pct_duckdb_*` suites and `//pct:pct_h2`.
- **pure/v1:** `//core:edge_pure_v1`, the route's golden net.
- **The test-data generator** is not a door of its own: its tests are corpus tests (`testDataGeneration`). Its strands
  are the corpus's `Printing` two-form (`TestDataPlanner.Printing`) and its `setUpDataSQLs` text. Item (e) measures
  that text's executions.

**Two more doors ran and reached no edge hook.** The recorder loads on its first hit, so a run with no hit leaves no
file. Each was forced past the caches:
- `//core:core_tests_server`, the server's own tests, under the product's style;
- `//parser-equivalence:tab_round_trip`, the JVM half of the tab's round trip, which only parses and composes.

**Not run: the hosts' edge routes.** The WebAssembly engine module's `EdgeBoundary` has no JVM test, and building the
module was not worth its cost here. By the code, `EdgeBoundary` → `HostAnswers` → `PureV1Routes`, handed
`PureText.PURE_V1`. That is pure/v1's style and code, so the hosts reach pure/v1's strands. Their counts would differ,
because their inputs are the module's pins.

**Two hooks the instrumenter missed:** its text patterns did not match `RelationalRootForm.apply` (`EnginePlans.java:164`)
or `PureText.enginePlanText`. By the code:
- `RelationalRootForm.apply` sits inside `EnginePlans.engineSql`, which runs only with an engine-text renderer.
- `enginePlanText` serves the `planToString` and plan paths, the `UpstreamPlans` facet's.

Both renderers' measured counts (the printer and `UpstreamPlans`) are corpus-only, so both hooks are the corpus's.

### (d.2) The matrix

Hook answers per door. The full table is `H8d-matrix.md`: 58 hook rows.

| Strand | corpus H2 | corpus DuckDB | PCT B | PCT A DuckDB | PCT A H2 | pure/v1 |
|---|---:|---:|---:|---:|---:|---:|
| **Facets** | | | | | | |
| `EngineTestText` (= `judgesEngineText`) | 28,342 | 28,651 | 79,503 | · | · | · |
| `RelationTexts` | 878 | 878 | 5,056 | · | · | · |
| `PureOnlyTexts` | · | · | 12,792 | 10 | · | · |
| `UpstreamPlans` | 676 | 676 | · | · | · | · |
| `ResultEnvelopes` | 66 | 66 | · | · | · | · |
| **Two-forms** | | | | | | |
| `DateTimeTexts`, `ListTexts` (each) | 13,653 | 13,744 | 33,520 | 2,489 | 479 | 39 |
| `LoweringForms` (asked) | 13,653 | 13,744 | 33,520 | 2,489 | 479 | (no hook: `PURE_V1` answers it itself) |
| `DatabaseSettingsForm` | 10,158 | · | 1,876 | · | 946 | 54 |
| `VerdictWords` | 3,167 | 3,170 | 27,552 | · | · | · |
| `Printing` (the test-data generator), `RelationCounting` | 3,186 / 3,184 | 3,186 / 3,184 | 6,714 / 6,714 | · | · | · |
| `ActivityTexts` | 1,643 | 1,643 | · | · | · | · |
| `ExecutedValues` | 68 | 68 | · | · | · | · |
| `RoutedQueries` | 18 | 18 | · | · | · | · |
| `VerdictNumbers` | · | · | 11,022 | · | · | · |
| `DateLiteralTexts` | · | · | 7,348 | · | · | · |
| `CrossKindNumbers` | · | · | 3,378 | · | · | · |
| **`LoweringForms` members, executed** | | | | | | |
| `existsJoin` | 12,251 | 12,314 | 3,103 | 143 | 142 | · |
| `computedPruning` | 11,565 | 11,656 | 26,077 | 2,480 | 474 | 39 |
| `unionFrame` | 10,481 | 10,533 | 3,008 | 132 | 132 | 36 |
| `whereZones` | 6,303 | 6,334 | 1,444 | 123 | 122 | · |
| `wireCast` | 366 | 368 | · | · | · | · |
| `executeValues` | 179 | 179 | · | · | · | · |
| `parsedJson`, `view`, `isNumeric`, `parsedDecimal`, `objectReferenceConnection` | 39, 18, 18, 14, 6 | the same | · | · | · | · |
| `raise`, checked | 33 | 33 | 453 | 69 | 4 | · |
| `raise`, **plain** | · | · | · | · | · | **6** (`DIVIDE_BY_ZERO` 3, `SQRT_OF_NEGATIVE` 3) |
| **`EngineShape`'s text forms** (`engineSql`) | `computedPruning` 2,067, `unionFrame` 1,624, `existsJoin` 1,362, `whereZones` 1,148, `wireCast` 172, `raise` 5, `parsedDecimal` 3, `view` 2 | the same | · | · | · | · |
| **`PlanStyle` methods** | | | | | | |
| `dialect` (the `PureText` passes) | H2 10,158 | DuckDB 9,841 | DuckDB 21,243, H2 1,876 | DuckDB 5,360 | H2 946 | H2 54 |
| `errorWords` | · | · | 45 | · | · | · |
| `siteWords` | · | · | · | · | · | 22 |
| `traceCell`, `graphKeyDate`, `graphKeyString` | 36, 35, 2 | the same | · | · | · | · |
| **The dialect passes** | | | | | | |
| DuckDB `raw` | · | 120,811 | · | · | · | · |
| DuckDB `scanOrderPasses` | · | 10,326 | 28,958 | 2,490 | · | · |
| `floatLiteralKind` (S20's bare DECIMAL) | H2 609 | DuckDB 612 | DuckDB 4,858, H2 179 | DuckDB 400 | H2 15 | H2 15 |
| **The engine-text printers** | H2 1,806, DB2 36, Composite 9 | the same | · | · | · | · |
| **The appeal stack** (open / verdict / declined) | 3,165 / 5,542 / 60 | 3,168 / 5,795 / 60 | 4,216 / 5,430 / · | · | · | · |
| **The runner's edge** (`CompatEdge`) | 2,575 | 2,575 | 12 | · | · | · |

The hook rows each door reaches: corpus H2 47, corpus DuckDB 48, PCT B 28, PCT A DuckDB 12, PCT A H2 11, pure/v1 9.

### (d.3) What the matrix says

- **pure/v1 alone reaches the plain set and `siteWords`.** Every other door writes `LoweringForms.raise` checked. So
  3.6's deletion of the plain set changes pure/v1, and through `HostAnswers` the hosts' routes, and nothing else.
  Under the golden net it moves 6 answers: 3 divides by zero and 3 square roots of a negative.
- **pure/v1 reaches 9 strands.** They are the H2 dialect pass, `DatabaseSettingsForm`, `DateTimeTexts`, `ListTexts`,
  `computedPruning`, `unionFrame`, the plain raise, `siteWords` and S20's `floatLiteralKind` (15).
- **PCT A reaches 11-12 strands:**
  - the four executed `LoweringForms` members (`existsJoin`, `computedPruning`, `unionFrame`, `whereZones`);
  - checked raises;
  - the dialect passes, with S20's `floatLiteralKind` on both databases;
  - the two-forms `DateTimeTexts`, `ListTexts` and `LoweringForms`, plus `DatabaseSettingsForm` on H2;
  - DuckDB's `scanOrderPasses`;
  - `PureOnlyTexts`, 10 answers on DuckDB.

  It reaches no text judging, no appeal and no printer, since PCT A has no SQL-text assert.
- **PCT B is the only door of 5 strands:** `errorWords`, `VerdictNumbers`, `DateLiteralTexts`, `CrossKindNumbers`, and
  nearly all of `PureOnlyTexts` (PCT A DuckDB 10). It also uses the appeal stack (4,216 appeals opened) and judges
  legend-engine's text (`EngineTestText`, 79,503 answers).
- **The corpus is the only door of the rest:**
  - every `EngineShape` text form and every engine-text printer;
  - `UpstreamPlans`, `ResultEnvelopes`, `ActivityTexts`, `ExecutedValues` and `RoutedQueries`;
  - `traceCell` and the graph keys;
  - the `LoweringForms` members `wireCast`, `executeValues`, `parsedJson`, `view`, `isNumeric`, `parsedDecimal` and
    `objectReferenceConnection`;
  - DuckDB's `raw` pass (120,811 answers on the DuckDB lane alone);
  - by the code, `RelationalRootForm.apply` and `enginePlanText`.
- **Instrumented, reached by no door:**
  - the two-forms `JsonGrammar`, `RenderedTextRule` and `GoldenNullWord`;
  - `LoweringForms.assertFailed`, both forms;
  - `EngineShape`'s `isNumeric`, `executeValues`, `objectReferenceConnection` and `parsedJson` text forms (their
    executed forms are reached);
  - `Appeals.rowAppealed`;
  - the passes of databases no door runs in the edge's style: Postgres, Databricks and Trino `floatLiteralKind`,
    Postgres `raw`, SQLite `refusesDecimalArithmetic`, MongoDB `dateTimeFormat`. PCT A runs DuckDB and H2 only.
  - `PURE_V1.judgesEngineText` is refused by name, so 0 is its right answer.

  A strand no door reaches has no measured user. Its deletion moves no measured answer, but no suite here would
  catch a break either.
- **The `Feature` flags (a.3) are not a `PlanStyle` strand.** The product's doors reach them through the query's own
  context. PCT A and PCT B set `CORRECT_SQL_SUBSTRING_INDEXING` always, and 7 corpus tests set a flag.

## (k) E-R2 against the corpus: the tests that expect legend-engine to refuse

The independent review's item (plan r6): count the corpus tests that expect legend-engine's refusal (an asserted
error, or an expected failure) and that lite would now answer, then class each one. Does lite's answer agree with an
expected value, or is it a lite answer with nothing to check it against?

### (k.1) Asserted errors: none

**No runnable corpus test asserts an error.** The corpus's own body-shape census at `main` reports `assertError=0`
over its 2,613 runnable tests, on both lanes. Its line in the passes of item (d)'s run:
`[corpus2] body-shapes pure=2471 effectful=142 interleaved(effect-after-assert)=34 assertError=0`.

In legend-engine's relational tree, `assertError` occurs 4 times, all in `mft/relationalmft.pure`:
- 3 live calls, all inside MFT wrapper functions (`relationalExecuteWrapperExecuteSimple`), none of them a
  `<<test.Test>>`;
- 1 commented out.

`H8k_assert_error_tests.py.txt` finds 0 test functions calling it.

Pure has no catch, so a test that does not assert an error fails when legend-engine refuses. **So E-R2 moves no
runnable corpus test's verdict**, and no corpus test passes today because lite copies a refusal.

### (k.2) Expected failures: legend-engine's own marks

148 of the corpus's 2,761 declared tests carry `ToFix` or `ExcludeAlloy`. The corpus excludes them as legend-engine
does (`PureTests.discover`; MinimalCorpusTest's census pins `excluded=148`). What the marks mean, from the pinned
sources:
- **`ExcludeAlloy`** (96 tests): legend-engine's runner skips them (`PureTestHelperFramework.satisfiesConditions`,
  `:171-177`), and legend-pure runs them. Their expected values are Pure's, held by legend-pure's runner.
- **`ToFix`** (52, 11 of them also `AlloyOnly`): legend-pure keeps them apart (`TestCollection`'s `toFixFunctions`,
  `:93`), and legend-engine skips them too. Neither upstream runs them, so their expected values are their authors'
  intent alone.

**The probe** (`H8k_probe.patch.txt`, off `b75d6c73a`, never for main):
- `PureTests.Discovery.runnable` returns the excluded tests and nothing else.
- The census pin prints instead of failing.

Both lanes' host passes ran in 47 s of wall time on 2026-11-19 (H2 21 s, DuckDB 32 s of test time). Each test is
classed by `H8k_classify.py.txt`, one row per test in `H8k-expected-failures.tsv`.

| Class | H2 `ExcludeAlloy` | H2 `ToFix` | DuckDB `ExcludeAlloy` | DuckDB `ToFix` |
|---|---:|---:|---:|---:|
| Pass, with a value assert | 44 | 13 | 50 | 13 |
| Pass, on SQL text alone (`assertSameSQL`, `assertEqualsH2Compatible`) | 4 | 0 | 4 | 0 |
| Fail: lite answers, and the test's value or the golden's rows contradict it | 9 | 16 | 5 | 16 |
| Fail: the SQL text failed and the rows appeal declined | 0 | 3 | 0 | 3 |
| Fail: lite refuses | 39 | 17 | 37 | 17 |
| Skipped by lite | 0 | 3 | 0 | 3 |
| **Total** | 96 | 52 | 96 | 52 |

**The classes asked for:**
- **Lite answers, and agrees with Pure's expected value: 44 on H2, 50 on DuckDB.** These are `ExcludeAlloy` tests
  that pass on a value assert. Their expected values are the ones legend-pure's runner holds. Whether legend-pure
  passes each one today was not run here.
- **Lite answers, and agrees with its authors' value only: 13 on each lane.** These are `ToFix` tests that pass.
  Neither upstream runs them, so the value is checked against nothing else.
- **Lite's answer is not checked: 4 on each lane.** They pass on legend-engine's SQL text alone, which CP-4 does not
  promise:
  - `groupBy::datePeriods::testGroupByWithRelativeDateFunctions`;
  - its `WithPathFilter` and `_noDatePath` variants;
  - `tds::groupBy::testTDSGroupByWithEnumArgumentFunctionCall`.
- **Lite answers, and the test contradicts it: 25 on H2, 21 on DuckDB.**
  - 4 are H2 only, all `groupBy::datePeriods` (`testGroupBy`, `...WithFilterFunction`, `testInFlowGroupBy_noDatePath`,
    `testPlanGroupBy_noDatePath`). H2 writes `100000` where the test expects `100000.0`: a Float's print, the class
    S20 is about. DuckDB passes them.
  - The rest disagree on both lanes: wrong row counts, `TDSNull` rows, and appeal divergences from the golden's rows.
- **Lite refuses: 56 on H2, 54 on DuckDB.** The refusals are `NotImplementedException` 17, `IllegalStateException`
  10, the typer's `type:` 9, `TypeInferenceException` 8, `DialectCapability` 6, `MappingResolutionException` 4,
  `IsolationWall` 1 and `DataError` 1. Two `DialectCapability` refusals (UNNEST) are H2 only; DuckDB passes both.

**What this does not settle:** whether legend-engine REFUSES these tests or answers them wrongly. A mark records that
it is excluded, not why. That answer is the engine runner's to give (AGENTS.md's upstream-fidelity tenet). B1-R's
ENGINE lane can ask it test by test, if 3.5 needs the split.

## (h) H2 without `MODE=LEGACY`: the corpus's H2 lane under H2's regular mode

CP-8's measure. **The probe** (`H8h_probe.patch.txt`, off `b75d6c73a`, never for main): `PureText.H2`'s constructor
takes the product's settings form (`ProductDatabaseSettings`: `MODE=REGULAR`, `DEFAULT_NULL_ORDERING=LOW`, H2 2.5.252's
own defaults) in place of the style's `DatabaseSettingsForm` two-form (`EngineDatabaseSettings`: `MODE=LEGACY`,
`DEFAULT_NULL_ORDERING=HIGH`). An H2 database opens with the settings the plan's target declares
(`H2Connector.url`), so every database the lane opens ran in regular mode. `//spec:judge_host_h2` ran alone on
2026-11-19, 12:17-12:20 UTC; output in `H8h-out.txt`.

**What moves: 1 test, from pass to fail.** The fail roster goes from 276 to 277. The skipped roster (7), the unordered
register (899) and the engine-order register (0) are unchanged, line for line. The strength census moves by the same
one test (LITERAL 1276 → 1275).
- **The test:** `meta::relational::graphFetch::tests::isolation::testIsolationOfPropertyTargetFilter`.
- **Why it fails:** the test's own database declares `active BIT`, and its mapping's own filter is
  `Filter activePersonFilter(personTable.active = 1)`. In regular mode H2 reads `BIT` as `BOOLEAN` and refuses
  `BOOLEAN = INTEGER` ("Values of types "BOOLEAN" and "INTEGER" are not comparable"). Legacy mode compares them. This is
  the test's own model text, not lite's lowering: a database's semantics that the program relies on.
- **Null ordering moves nothing.** Lite's H2 writes its null placement on every sort key (`ASC NULLS LAST`, as in the
  failing statement), so the database's default placement never decides an order lite executes. Legend-engine's
  printer leaves the placement bare (`EngineDatabaseSettings`'s javadoc), but that text is judged, never executed.

**The referee keeps legend-engine's settings.** It replays goldens on legend-engine's own H2, 2.1.214, opened
`MODE=LEGACY;DEFAULT_NULL_ORDERING=HIGH` with H2's `NON_KEYWORDS` (`testbackend/EngineH2Referee.openReferee`,
`core/src/test/java/com/legend/testbackend/EngineH2Referee.java:67-70`). The probe does not touch it: the golden is
replayed as legend-engine runs it, and lite's rows come from the regular-mode database.

## (e) CP-4's remaining cost: what the appeal decides, the generated text executed, the time

B1's review splits CP-4's cost three ways, and asks for the corpus lanes' time with every SQL-text assert sent to the
appeal. A later question from the orchestrator is answered in (e.2): the tests that pass today THROUGH a successful
appeal, where the text failed and the rows appeal held.

### (e.1) Asserted legend-engine text the appeal cannot decide: 143

This part is F1's count (`docs/records/census-b2-sqltext-probe-2026-11-19.md` §2, at `32ef19118`). **143 tests pass
only by legend-engine's text.** By sub-class:
- the text failed and the rows appeal declined: 64;
- a printed string compared with no appeal: 22;
- lite's printer has no form for the construct: 53;
- SQL text read from legend-engine's plan: 4.

Beside them, **3** tests are DIVERGED appeals: the text verdict had hidden an answer difference. **6** more are judged
on legend-engine's plan structure, not on SQL text.

### (e.2) The tests the appeal decides

**The method.** The corpus's strength census classes every pass by what judged it (`MinimalCorpus.run`):
- **DIFFERENTIAL:** a referee reported MATCH. It is reported only from a heard appeal (`SqlTextRuns.judge` → `hold`),
  and an appeal is heard only when a SQL-text assert's text failed. So DIFFERENTIAL is exactly "a text failed and its
  rows appeal held". `+literal` means a value assert held too; `-only` means the appeal alone carried the pass.
- **LITERAL:** a value assert, with no referee.
- **CARDINALITY:** only size, emptiness or boolean asserts.
- **SPELLING:** every verdict was decided by text (a declined appeal).

**The runs.** Each lane ran alone, one after another, on 2026-11-19 from 11:36 to 12:05 UTC (`H8e-timing.txt`):
- **main:** `claude/census-b2`, whose product equals `b75d6c73a`;
- **the lite-text probe:** F1's, `claude/census-b2-sqltext` at `0968d1220`, where every engine-text printer writes
  lite's own SQL. So every SQL-text assert's text fails and goes to the appeal. One line was added
  (`H8e_strength_print.patch.txt`) to print the census before the moved rosters' pins stop the run.

| Passes by what judged them | H2 main | H2 probe | DuckDB main | DuckDB probe |
|---|---:|---:|---:|---:|
| **DIFFERENTIAL: a failed text whose appeal held** | **964** | **1,433** | **990** | **1,475** |
| of which `+literal` | 795 | 1,185 | 821 | 1,228 |
| of which `-only` (the appeal alone) | 169 | 248 | 169 | 247 |
| LITERAL | 1,276 | 689 | 1,413 | 805 |
| CARDINALITY | 27 | 20 | 27 | 20 |
| SPELLING | 22 | 0 | 22 | 0 |
| **Passes** | 2,289 | 2,142 | 2,452 | 2,300 |

- **Today, 964 tests on H2 and 990 on DuckDB pass through a successful appeal.** For 169 on each lane, the appeal
  alone carries the pass.
- **With every SQL-text assert sent to the appeal, the appeal decides 1,433 on H2 and 1,475 on DuckDB.** That is 469
  and 485 more than today, and they stay passing, by answer.
- The 147 and 152 tests that fail under the probe are F1's.
- SPELLING goes to 0: no pass is decided by text alone once text is never equal.

### (e.3) The programs that EXECUTE generated text

**The probe** (never for main): `H8e_GenProbe.java.txt`, planted by `H8e_gen_hooks.patch.txt`.
- **Generation:** the edge's generators register each statement they write. These are `EnginePlans.ddlText` (`toDDL`'s
  statements) and `EngineSeedText`'s two `setUpDataSqlsText` forms, each keyed to the test that generated it.
- **Execution:** the test planner's `executeInDb` arm (`EffectPlanner`) counts each statement it plans that is one of
  that test's own generated statements.

Both lanes' host passes ran on 2026-11-19, 12:21-12:32 UTC. Output: `H8e-gen-judge_host_{h2,duckdb}.tsv`.

**An earlier run was discarded.** It matched generated text across tests and counted 8,800 `setUpDataSQLs` executions
in 391 tests. Every one was a hand-written statement whose text equals the generator's (the corpus's setups write
`Drop table if exists PersonTable` as `setUpDataSQLs` does). Keyed to the generating test, none is left.

| Generated text | Executed by `executeInDb` (each lane) | The tests |
|---|---|---|
| `toDDL` (`createSchemaStatement`, `createTableStatement`, ...) | **3 tests, 57 statements** (19 each), written for H2 | `aggregationAware::testRewrite::objectGroupBy::testRewriteCanAggregateGroupByOnLiteralWithMultipleAgg`, `testRewriteAsNOP::nonAggregationAware::testRewriteFilter`, `testRewriteAsNOP::nonGroupBy::testRewriteFilter`: their setup (`aggregationAware/test/store.pure:166-188`) maps `executeInDb` over the DDL |
| `setUpDataSQLs` | **0** | none: lite sends none, by S192 |

- **The 3 `toDDL` tests pass on both lanes at main, and on H2 under the lite-text probe.** There, `ddlText`'s printer
  (`PureText.engineText`) writes lite's own DDL. So lite's own DDL text runs these programs as well. On the DuckDB
  lane, the H2 text goes through DuckDB's `raw` pass.
- **`setUpDataSQLs` passed to `executeInDb` is never sent.** Register row **S192** (ruled by the user 2026-11-11,
  "Read only reload is fine") covers upstream's 30 `loadAndTestExecution` tests
  (`testDataGeneration/tests/testDataGeneration.pure:1784-1792`):
  - the reload sends no statement;
  - the query after it is planned again with each reloaded table read from the generated slice by a typed table swap
    (`TestDataPlanner.slice`, `TableSwaps`).
  
  So no program in the corpus depends on `setUpDataSQLs`' text executing. Its text is judged only where a test
  compares it, which is F1's a2, and S195's row counts.

### (e.4) The corpus lanes' time with every SQL-text assert sent to the appeal

Each lane ran alone, as in (e.2):

| Lane | Test loop, main | Test loop, probe | JUnit total, main / probe | Wall, main / probe |
|---|---:|---:|---:|---:|
| H2 | 187 s | 179 s | 193.6 / 184.9 s | 241.5 / 210.6 s |
| DuckDB | 628 s | 630 s | 634.5 / 636.2 s | 636.5 / 638.7 s |

**No measurable cost.** The appeal sends more statements under the probe:

| Statements | H2 main → probe | DuckDB main → probe |
|---|---|---|
| Referee, ours | 1,136 → 1,726 (+590) | 1,156 → 1,756 (+600) |
| Referee, the golden's replays | 1,073 → 1,628 (+555) | 1,093 → 1,659 (+566) |
| Mirror seeds | 68,220 → 87,618 (+19,398) | 69,609 → 89,447 (+19,838) |

Those are offset within a few seconds by two things: the probe's failing tests (147 and 152) stop early, and lite's
printer is cheaper than the engine-text printer it replaces. The H2 lane even runs 8 s faster.

So keeping the appeal as CP-4's judge costs no corpus time. Deleting the engine-text printers also removes the
printing work itself.

## (j) CP-5's honest cost: S20's bare DECIMAL and the plain set, switched off

**The question.** How many answers that match legend-engine today would stop matching, by case, if both were
switched off? For each one, does an ordinary-function rewrite (3.4's guards) reproduce legend-engine's answer? The
two strands:
- S20's bare-DECIMAL literal;
- `LoweringForms.raise`'s plain set.

**The probe** (`H8j_probe.patch.txt`, never for main):
- the five `PureText` `floatLiteralKind` overrides (DuckDB `:350`, H2 `:406`, Postgres `:438`, Databricks `:484`,
  Trino `:649`) return the product's `super.floatLiteralKind()`;
- `PureText.ENGINE_UNCHECKED` is empty, so every raise is written checked;
- the engine-text printer's own literal (`EngineStyleH2.floatLiteralKind`) is left alone: it is text, CP-4's.

**The harness is B1-R's ENGINE lane, not a second one.** It is `claude/compat-engine-lane` at `815690a57`
(`docs/records/compat-homework/H7-engine-lane.md` there). PCT channel A runs as legend-engine's audience under
`PureText.PURE_V1`, and each test is classed against legend-engine's manifests (SAME, LITE_ANSWERS, LITE_FIXES,
BOTH_FAIL, LITE_BEHIND).
- The probe ran in a local worktree of that commit. Its product equals `b75d6c73a`.
- The regenerated tables were diffed against B1-R's pins (`H8j_engine_lane_diff.py.txt`).
- Beside it ran:
  - PCT channel A's default audience: the edge golden net, every case's ANSWER recorded, at main and under the probe
    (`H8j_pct_a_answers.py.txt`);
  - pure/v1's golden net;
  - both corpus lanes.

All ran on 2026-11-19, 12:31-12:58 UTC.

### (j.1) The results, suite by suite

**PCT channel A as ENGINE** (B1-R's six lanes: DuckDB's 5 suites, 1,249 tests, and H2's relation suite, 469). **6
tests move, all from BOTH_FAIL to LITE_FIXES. No SAME test moves.** All 6 are DuckDB error tests
(`H8j-engine-lane-moved.tsv`). Before the probe, lite let the database answer as legend-engine does, so both failed
PCT; under the probe lite raises PCT's expected error.

| Test (DuckDB) | The expression | legend-engine (manifest) | DuckDB's answer to the unchecked SQL | Lite under the probe |
|---|---|---|---|---|
| `slice::testSliceError` | `[1,2,3,4,5]->slice(3, 2)` | no error: a value | `[]` | PCT's error |
| `rem::testRemError` | `5->rem(0)` | no error: a value | `null` | PCT's error |
| `range::testRangeStepError` | `range(0, 5, 0)` | no error: a value | `[]` | PCT's error |
| `bitShiftRight::testBitShiftRight_MoreThan62Bits` | `1->bitShiftRight(63)` | no error: a value | `0` | PCT's error |
| `testSquareRootError` | `sqrt(-1.0)` | an error (the database's) | Out of Range Error | PCT's error: still an error |
| `bitShiftLeft::testBitShiftLeft_MoreThan62Bits` | `1->bitShiftLeft(63)` | an error (the database's) | Out of Range Error | PCT's error: still an error |

DuckDB's answers come from DuckDB 1.5.6 itself (`H8j_DuckDbPlain.java.txt`, output `H8j-duckdb-plain.txt`). The SQL
there is written by hand to the plain form, not lite's own text.

S20 moves no verdict in this lane. It does move two answers the verdict cannot see; see the next suite.

**PCT channel A, its default audience** (the edge golden net, `//pct:edge_duckdb_*` and `//pct:edge_h2_relation`,
under `UPSTREAM`, whose raises are already checked). **No outcome moves.** Comparing each case's recorded answer
(`H8j-pct-a-answers-moved.tsv`):
- **2 values move, both S20's.** In `plus::testFloatPlus` case 1, `1.7` (S20's exact decimal: legend-engine's bare
  literal, which DuckDB computes as a DECIMAL) becomes `1.7000000000000002`, Pure's double. In `times::testFloatTimes`
  case 2, `2.1` becomes the double, likewise. PCT passes both ways, and B1-R's lane reads both SAME before and after.
  Under the ENGINE audience the same two answers move, because `PURE_V1` dials the same passes, but its verdict cannot
  see it either.
- **9 cases move their SQL text alone, with equal rows:**
  - DuckDB: `testProjectNumbersPlusTimesMinus`, `testSimpleExtendFloat` and its `_MultipleExpressions` variant,
    `testSimpleRelationProjectLiterals`, `testPercentile_Relation_Aggregate`, `testPercentile_Relation_Window`;
  - H2: the first three again.

  A Float literal's cast is written differently. This is CP-4's (text not promised).
- 192 relation cases differ only in the order their rows arrived. The recorder writes rows as they come, and the golden
  net's normalizer compares them as a set, so these are not answers.

**pure/v1's golden net** (`//core:edge_pure_v1`, 28 answers, H2; `H8j-pure-v1-diff.txt`). Four requests move. Each one's
plan twin moves its SQL text with it.

| Request | legend-engine 4.145.0 (B1-R's oracle, `engine_h2_answers.tsv`) | Lite today | Lite under the probe |
|---|---|---|---|
| `raise.sqrtOfNegative` | 200, `[null]` | 200, `[null]` | **500**, "Raised: Unable to compute sqrt of -2.0" |
| `raise.divideByZero` | 500, H2's "Division by zero" | 500, H2's words | 500, "Raised: DIVIDE_BY_ZERO" (the same status; the words are the presenter's) |
| `cell.float` | 200, `0.33333333` | 200, `0.3333333333333333` | the same value; the SQL's cast text moves |
| `cell.floatWhole` | 200, `6.0` | 200, `6.0` | the same value; the SQL text moves |

**The relational corpus** (S20 is its strand: the plain set never reaches it, since `UPSTREAM` is checked;
`H8j-corpus.txt`):
- **No test moves from pass to fail, on either lane.**
- **On H2, 2 tests move from fail to pass:**
  - `graphFetch::tests::qualifier::testSubAggregationInQualifier`;
  - `mapping::relation::aggregation::testSubAggregationWithIfOnRelationMapping`.

  With S20's DECIMAL literal, H2 answers `52` and `1` where the tests expect the Floats `52.0` and `1.0`. The fail
  roster goes from 276 to 274.
- **DuckDB's roster is unchanged.**
- **4 tests on each lane move from DIFFERENTIAL+literal to LITERAL:** `embedded::advanced::testGroupBy`,
  `otherwiseTestGroupBy`, `inline::nested::testGroupBy`, `inline::targetid::testGroupBy`. With the product's literal,
  the engine text is byte-equal to the golden, so no appeal is needed. Their 4 unordered-chain register lines go with
  them.

### (j.2) The answers that stop matching legend-engine, by case

| Case | Where | Answers | An ordinary-function rewrite |
|---|---|---:|---|
| legend-engine answers a value where Pure raises (the plain set) | PCT ENGINE, DuckDB: `slice`, `rem`, `range`, `bitShiftRight` | 4 | Yes, each a guard: `if($low > $high, \|[], \|$x->slice($low, $high))` → `[]`; `if($d == 0, \|[], \|$x->rem($d))` → `null`; `if($step == 0, \|[], \|range(...))` → `[]`; `if($n > 62, \|0, \|$x->bitShiftRight($n))` → `0`. A negative operand of `>>` shifts in ones, so a negative `$x` needs `-1` there |
| legend-engine answers null where Pure raises (the plain set) | pure/v1, H2: `sqrt(-2.0)` | 1 | Yes: `if($x < 0, \|[], \|sqrt($x))` (3.4's own example) |
| legend-engine's exact-decimal Float arithmetic (S20) | PCT A (both audiences), DuckDB: `testFloatPlus`, `testFloatTimes` | 2 | Only through Decimal: `toDecimal()` on each literal, then `toFloat()`. Whether the translator may write that is 3.6's to rule. PCT's verdict cannot see these two |
| **Total, by value or status** | | **7** | 5 by a guard, 2 through Decimal |

**The status is kept and only the words move (3 cases):** PCT DuckDB `sqrt` and `bitShiftLeft`, where both sides raise
the database's error, and pure/v1's `raise.divideByZero`, which stays 500. Words are the presenter's.

**Answers that START matching the tests' expected values: 2.** These are the H2 corpus tests above. Under S20 today,
lite answers an Integer-printed value where the test, and legend-engine's own corpus run, expect a Float.

**Not measured:**
- the plain set's kinds on databases the ENGINE lane does not run;
- H2's essential and standard suites, which hold (c)'s H2 value cells for `sqrt` and both shifts;
- `CAST_IMPOSSIBLE` and `SIZE_NOT_MULTIPLICITY`, which no PCT test reaches ((c.3)).

pure/v1's golden net covers H2's `sqrt` and divide. The ENGINE lane can grow those suites (B1-R's) if 3.6 needs them.

### (j.3) pure/v1's plain-set answers, per database

The orchestrator's addition. pure/v1 is the only door that reaches the plain set ((d)). Its 6 plain answers there
(`DIVIDE_BY_ZERO` 3, `SQRT_OF_NEGATIVE` 3) are the lowerings of two golden-net requests, through their execute and
plan routes:
- `raise.divideByZero`: `$r.year->toOne() / 0`;
- `raise.sqrtOfNegative`: `sqrt(-2.0)`.

legend-engine 4.145.0's answers are B1-R's oracle's (`tools/engine-runner/src/test/resources/pure_v1/engine_{h2,duckdb}_answers.tsv`
at `815690a57`). DuckDB's own answers come from DuckDB 1.5.6, lite's version (`H8j_DuckDbDivide.java.txt`, appended to
`H8j-duckdb-plain.txt`).

| Request | Database | legend-engine | Lite, plain (today) | Lite, checked (the probe) | Ordinary-function rewrite |
|---|---|---|---|---|---|
| `raise.divideByZero` | H2 | 500, H2's "Division by zero" | 500, the same error | 500, lite's raise | **none needed**: the status is kept, the words are the presenter's |
| `raise.divideByZero` | DuckDB | 200, `null` | not run here; DuckDB 1.5.6 answers the unchecked `/ 0` with `Infinity` (IEEE division), so the plain form would NOT reproduce legend-engine's `null` | 500, lite's raise | **yes:** `if($d == 0, \|[], \|$x / $d)` answers `null` |
| `raise.sqrtOfNegative` | H2 | 200, `null` | 200, `null` (measured) | **500** (measured) | **yes:** `if($x < 0, \|[], \|sqrt($x))` answers `null` |
| `raise.sqrtOfNegative` | DuckDB | 500, DuckDB's "Out of Range Error" | 500, the same error (DuckDB 1.5.6: Out of Range Error) | 500, lite's raise | **none needed**: the status is kept |

**Each rewrite is needed on one database and wrong on the other.** On H2 the divide raises as legend-engine does, and
the square root needs the guard. On DuckDB it is the reverse. A guard written on both databases would turn
legend-engine's error into a null on the other one. So the rewrites must be per target, as the translator's
"target-aware" rule says.

**The plain set does not reproduce legend-engine on DuckDB's divide either.** Its answer depends on the database's
version: lite's DuckDB answers `Infinity`, where legend-engine's answers null. So the guard is the only way to match
there.

The translator session measured the same strand on its own branch (`claude/compat-translator-spike` `d5ba073e`, `h5/purev1`):
`plain` moves pure/v1's `raise.sqrtOfNegative` from 200 null to a raise, and changes only `divideByZero`'s words. That
agrees with (j.1).

### (j.4) H2's other PCT suites, closing (j)'s "not measured" for H2

The orchestrator's follow-up. **The method:**
- B1-R's ENGINE lane (`815690a57`), widened in two local worktrees to H2's Essential, Grammar, Standard and
  Unclassified suites beside Relation (`H8j4_h2_lane_widening.patch.txt`: `_ENGINE_LANES` takes H2 on every suite);
- each case's answer recorded too (`-Dlegend.edge.answers`);
- one worktree as the BASELINE, one with (j)'s probe.

Each H2 table is joined with legend-engine's own H2 manifest for its suite (`relational-h2/<Suite>Functions_manifest.json`,
read from the pinned sources). The baseline's Relation table is byte-identical to B1-R's pin. Tables:
`H8j4-h2-lane-tables/{baseline,probe}/`. Runs: 2026-11-19, about 13:05-13:15 UTC.

**The baseline**, H2 as legend-engine's audience, all 1,249 tests:

| Suite | Tests | SAME | LITE_ANSWERS | LITE_FIXES | BOTH_FAIL | LITE_BEHIND |
|---|---:|---:|---:|---:|---:|---:|
| Essential | 345 | 125 | 26 | 101 | 87 | 6 |
| Grammar | 137 | 77 | 14 | 28 | 18 | 0 |
| Relation | 469 | 411 | 0 | 37 | 21 | 0 |
| Standard | 205 | 107 | 0 | 6 | 65 | 27 |
| Unclassified | 93 | 72 | 0 | 1 | 10 | 10 |

**Verdict moves under the probe: 4, all BOTH_FAIL → LITE_FIXES. No SAME test moves** (`H8j4-h2-lane-moved.tsv`).
H2's own answers to the unchecked SQL are from both H2 2.5.252, lite's, and 2.1.214, legend-engine's, which agree
(`H8j4_H2Plain.java.txt`, `H8j4-h2-plain.txt`).

| Test (H2) | legend-engine (manifest) | H2 on the unchecked SQL | Lite today (plain) | Lite, probe |
|---|---|---|---|---|
| `rem::testRemError` (`5->rem(0)`) | an error ("Unexpected error executing function") | `MOD(5, 0)`: Division by zero | H2's Division by zero | PCT's error: **the status is kept**, only the words move |
| `testSquareRootError` (`sqrt(-1.0)`) | no error: a value | `SQRT(-1.0)` = **NaN** | lite's refusal: "the database answered NaN for a Float, which has no NaN in Pure" | PCT's error |
| `bitShiftLeft::testBitShiftLeft_MoreThan62Bits` (`1->bitShiftLeft(63)`) | no error: a value | `LSHIFT(1, 63)` = **-9223372036854775808** | lite's refusal: "BIT_SHIFT_LEFT reached a dialect without bit-op support" | PCT's error |
| `bitShiftRight::testBitShiftRight_MoreThan62Bits` (`1->bitShiftRight(63)`) | no error: a value | `RSHIFT(1, 63)` = **0** | lite's refusal: "BIT_SHIFT_RIGHT reached a dialect without bit-op support" | PCT's error |

**Answers (`H8j4-h2-answers-moved.tsv`):**
- **2 values move, both S20's**, as on DuckDB: `testFloatPlus` 1.7 → 1.7000000000000002, and `testFloatTimes` 2.1 →
  its double. Both are SAME before and after, since PCT's verdict cannot see them.
- 8 cases move their SQL text alone, with equal rows.
- 3 errors change their words, and one value turns into PCT's error: the four tests above.

**The answers that stop matching legend-engine on H2: 2**, both S20's Float case. They are reproducible only through
Decimal, as (j.2) says. The plain set stops no H2 answer from matching: `rem` keeps its error status, and the other
three never matched. (j.2)'s total becomes **9 answers: 5 by a guard, 4 through Decimal only.**

**What D2 should know: 3 legend-engine answers on H2 that no ordinary guard reproduces.** Lite does not match them
today, with or without the plain set:
- `sqrt(-1.0)` → NaN. Pure has no NaN, so no Pure program, and so no rewrite, produces it.
- `1->bitShiftLeft(63)` → -9223372036854775808. LSHIFT wraps.
- `1->bitShiftRight(63)` → 0. A guard `if($n > 62, |0, |...)` answers 0, but its other branch still needs a shift.

Lite's H2 dialect writes no shift at all (the `DialectCapability` refusal above), so no rewrite helps until it does. That
is a capability gap of lite's H2, not a CP-5 cost.

