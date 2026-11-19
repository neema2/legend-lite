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
