# H8: the phase-0 measurements, slice 0.4 (census B2, 2026-11-19)

The compatibility program's slice 0.4 (plan r5, `docs/records/compat-program-plan-2026-11-19.md` on
`claude/compat-program`, §4 and §3). Each item is committed as it is measured. Every number is reproducible from a
probe in `H8-probes/`. The measurements are at `main` `b75d6c73a`, or at `claude/census-b2`, whose product equals it
(the branch differs from main only in `tools/meaning`, `tools/guards`, `gates`, docs and visibility lines).

Items: (a) the strand map, (c) legend-engine's unchecked operations, (d) the strands' doors, (e) CP-4's remaining cost,
(h) H2 without `MODE=LEGACY`, (j) CP-5's cost. Item (b) is F1 (`docs/records/census-b2-sqltext-probe-2026-11-19.md`).
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
