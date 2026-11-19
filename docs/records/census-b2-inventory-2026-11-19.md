# Census B2: a census that can see (2026-11-19)

Track B2 of PR #118's follow-ups (FOLLOW_UPS.md §8, B2: "a census that can see: GUARDS-1, -3, -4, COMPLETENESS-10,
HOSTS-8"). Branch `claude/census-b2`, stacked on `claude/pr118-fixes` (`cf763c815`). This record is the REAL INVENTORY
of compatibility meaning still in the product: legend-engine's and legend-pure's spellings, envelopes, messages, print
forms, SQL shapes and routes, and the product code that computes or branches on them. It covers Java core and the
hosts' Java, core's resources, the TypeScript product (engine-client, query-store, datacube, query, studio and the
libraries they bundle) and the Python product (`python/legend_lite` outside its edge).

**Everything here is report-only.** No hit fails the build. The census fails only when a lens goes blind: a planted
violator it does not find, or an input under its floor. Its switch, `MeaningCensusTest.BLOCKING`, is `false` until B3
has closed the items (a later decision).

## 0. The counts first

| Lens | (a) | (p) | (b) | (c) | all |
|---|---|---|---|---|---|
| M1 | 55 | 0 | 322 | 235 | 612 |
| M2 | 12 | 0 | 25 | 17 | 54 |
| M3 | 57 | 0 | 31 | 131 | 219 |
| M4 | 0 | 0 | 196 | 1 | 197 |
| M5 | 7 | 2 | 42 | 83 | 134 |
| M6 | 9 | 0 | 0 | 0 | 9 |
| M7 | 2 | 0 | 5 | 12 | 19 |
| **all** | **142** | **2** | **621** | **479** | **1244** |

Every hit is classified by the committed `tools/meaning/inventory.tsv`:
- **(a)** upstream meaning in the product. A finding still in the product, including items the user has ruled to move,
  each with its ruling named.
- **(b)** covered by a user ruling that keeps it: a `docs/SEMANTICS_REGISTER.md` row, R1-R11 of FOLLOW_UPS.md §6/§7, the
  lite API v1 spec's rulings, or a tenet the user ratified. The ruling is named.
- **(c)** no upstream meaning: a false positive, with the reason.
- **(p)** upstream meaning whose ruling is pending, in a class of its own until the user rules it. There is one item:
  COMPLETENESS-5's substring and indexOf (R4).

The inventory has **no catch-all row**. A (c) row names its terms, or a package whose own vocabulary the terms are (a
dialect's SQL, the #SQL island's words). A new upstream sentence or name therefore arrives unclassified and is never
hidden. The comparison with main (§8) found four catch-alls hiding main's hits. Each is now an explicit list, and the
head's classes did not change.

None is unclassified. The full list, one row per hit with its class and reason, is
`docs/records/census-b2-inventory-2026-11-19.tsv`. The test writes the same rows on every run (`meaning-census.tsv` in
its outputs).

## 1. The census

| Piece | What |
|---|---|
| `//tools/guards:meaning_census_test` | The test, in `//gates:checks` (per push). `theProductIsReadAndItsMeaningReported` runs every lens over the real product and writes the report. `eachLensBitesItsPlant` runs every lens over its planted violators. It lives in tools/guards because that package may use the JUnit pool; tools/meaning may not, and adding a pool user is an allowlist move. |
| `//tools/meaning:census` | The lenses: `ClassFiles` (a JDK-only class-file reader with Code attributes), `Flow` (one method's data flow over an abstract stack), `FormsLens` (M3) and `MeaningCensus` (M1, M2, M4-M7). |
| `//tools/meaning:host_sources` | The reusable TypeScript/Python reader (§5). |
| `//tools/meaning:vocabulary` | Upstream's vocabulary: a `java_run` over `@legend_engine_src` and `@legend_pure_src` (§4), a build output. |
| `//tools/meaning:host_edge_srcs` | The hosts' edge sources, as the build declares them: a genquery over the srcs of `//engine-client:edge`, `//datacube:edge_entry`, `//python:edge` and `//python:engine_package`. |
| `//tools/meaning:shipped_reach`, `:shipped_reach_planted` | K2 for every host (§3, M7), report-only genqueries. |
| `tools/meaning/inventory.tsv` | The classification, and the known items with why any is unseen. |
| `tools/meaning/planted/` | The plants: Java (a product side and an edge side, by package), TypeScript, Python, Pure. |

Run it: `bazel test //tools/guards:meaning_census_test`. The report is in
`bazel-testlogs/tools/guards/meaning_census_test/test.outputs/`; the summary lists the counts, the known items and any
unclassified hit.

## 2. The blind spots, measured (before)

| Lens | Its blind spot at `cf763c815` |
|---|---|
| L-A, second half (`UpstreamBranchesTest`, GUARDS-1) | Five names, 3 of about 40 packages, no floor. Its words survive in code only as `numbers.floatTolerance()` (VerdictSql, 8 sites), which it exempts as "the hook's answer". It can fire only if a deleted name comes back. |
| TEXT (`CompatSpellingGuardTest` and the pool and resource twins, GUARDS-3) | A fixed list of 54 literals and 22 symbols. Upstream's main sources hold 23,127 sentence fragments (3+ words, 12+ characters), and product Java literals hold 114 of them in 122 files. The edge's 1,279 distinctive literals overlap the product's at 242. |
| L-A, first half (`ProductFormsUseTheirArgumentsTest`, GUARDS-4) | Function arguments only. A plain value a product form ignores, computed eagerly by its callers, is unseen. |
| TS and Python (COMPLETENESS-10) | No meaning guard: 408 TypeScript and 33 Python product files unscanned (the closure and layering guards check dependencies, not meaning). |
| K2 (HOSTS-8) | `_EDGE_TARGETS` and `_EDGE_TARGET_LIST` name core, native and wasm. `//engine-client:edge`, `//datacube:edge_entry` and `//python:edge` are absent. There is no source-level "the product entry reaches no edge" check for TypeScript or Python. |

## 3. The lenses (after)

Each lens is a function over its inputs, so the same code reads the real product and the plants.

| Lens | Reads | Finds | Its plant (in `eachLensBitesItsPlant`) | Its stated limits |
|---|---|---|---|---|
| **M1 TEXT** (GUARDS-3, COMPLETENESS-10) | Product class files' string constants (a concatenation's recipe split at its arguments); product resources (a Pure file's literals, other text files' lines); TS/Python literals and names in code | A product string holding an upstream name (TYPE, EXCEPTION, FIELD, SUBTYPE, IDENT), route or sentence, or one of the edge's own strings that upstream also holds. A name in TS/Python code is matched against upstream's execution protocol shapes (FIELD, SUBTYPE, 10+ characters). | `PlantedLowering.envelope()` holds `"RelationalTdsInstantiationExecutionNode"` and `"Query project group ID is invalid"`. `relation-reader.ts` reads `plan.rootExecutionNode.resultType.tdsColumns`. `_planted_reader.py` reads `raw.get('relationalType')`. | A string built at run time from parts that hold no term; a sentence under 4 words that is not capitalised (it is English); a short name in code (DOM's `className`) |
| **M2 NAMES** | Product classes' own names and declared members; TS/Python names in code | A product name naming upstream: upstream, legend-engine, legend-pure, pure/v1, and in Java "engine" inside a longer name. In the hosts "engine" is lite's own word (`QueryEngine`, Python's `Engine`), so a host name counts only by the explicit tokens. | `legendEngineShape` (TS), `LEGEND_ENGINE_ROUTES` (Python) | A hand token list: lite's own naming convention, not upstream's vocabulary |
| **M3 FORMS** (GUARDS-1, -2, -4) | The bytecode of product and edge classes | For every product declaration with a product and an edge implementation (a hook, a two-form kind, an overridden product method; a lambda implements one too): **UNUSED**, a parameter the product's form never reads and the edge's does (UNUSED when the form reads its others, UNUSED_NOOP when it reads none, UNUSED_LAZY for a function value); **ANSWER**, the product answers a constant where the edge does not; **BRANCH**, every product method whose conditional tests a scalar or enum answer, directly, through a local, a parameter it is handed or a method returning it (taint to a fixpoint); **EDGE_LIVE**, a product method whose product callers all pass null, false or 0 at a parameter an edge caller passes live; **REFUSED**, a product call of a declaration the product's form refuses. | `PlantedHook`, `PlantedProductForm` (answers null, ignores `upstreamWords`), `PlantedEdgeForm`, and `PlantedLowering`, which branches on `frame()`, hands it to `wrap`, computes `"Assert failed"` for `raise`, and calls `prune(q, null)` where the edge passes a value | It follows parameters, locals and returns, never a record field's or an object field's provenance (PRODUCT-11's `readThroughExecuteValues`, C2's `existsJoinable`). A ternary's merged value loses its taint. A strategy object (a form, a collection) is reported as an ANSWER but seeds no taint. |
| **M4 PRINT BODIES and BODY** (COMPLETENESS-2) | Product Pure resources | Every `toString` body (a print form), and every Pure body shipped: a function with a body, a class's qualified property. Under the world-map tenet the platform ships declarations and no other Pure bodies. | `planted.pure`: a `toString`, a qualified property, a function body | A body in a resource that is not a `.pure` file |
| **M5 PROVENANCE** | Product Java sources' comments; TS/Python comments and docstrings | A comment declaring a reproduction of upstream ("ENGINE QUIRK", "ENGINE GAP", "(engine model)", "as legend-engine writes", "reproduces the engine"), or that the code follows legend-engine's recorded answers or its translation ("engine golden(s)", "the engine translates": COMPLETENESS-5's two comments, and 40 goldens cited as the witness of a semantic the product shares) | `// ENGINE QUIRK` (TS, Java), `# ENGINE GAP` (Python) | A hand marker list: lite's commenting convention (the code cites upstream where it copies it). A copy that does not say so is unseen. |
| **M6 EDGE-ONLY** (COMPLETENESS-1's design) | TS imports and exports; Python top-level names | A product export whose only non-test users are edge modules (TS: imported by name; Python: spelled) | `tdsColumnsOf` (TS), `engine_columns` (Python), each used only by a planted edge module | A Python name spelled by a test or by the product itself is no edge-only use; a TS namespace import counts as using every export |
| **M7 K2** (HOSTS-8) | The build graph; each product module's imports; the product classes' references | **BUILD**: a shipped artifact's target depending directly on any host's edge target (the edge targets derived: each closure target whose srcs sit under an `edge/` directory, plus engine-client's edge and the engine wheel's package), declared opt-ins left out. **STATIC**: a product TS or Python module importing an edge module at load. **OPT_IN**: a dynamic or guarded import, or a ServiceLoader lookup (reported, not a violation). **DOOR**: an edge class a JVM can start by name. Java reach: the class-reference closure from each Java host's product entry. | `planted_shipped_reach` (a shipped-like target on `//python:edge`); `relation-reader.ts` importing `../edge/engine-routes.ts`; `_planted_reader.py` importing `.edge`; `PlantedEntries.Server`, `.Tab`, `.Native` and `.QueriesPage`, each reaching `planted.edge`; `PlantedEdgeDoor.main` | A reach by name at run time (reflection; a module fetched by a computed URL) is seen only as OPT_IN |

The floors (`FLOORS`): product classes 2,800, edge classes 150, vocabulary names 4,000, sentences 10,000, routes 100,
TypeScript product files 300, Python product files 25, Java product sources 900. Measured: 3,468, 218, 6,615, 15,749,
173, 467, 34 and 1,141.

## 4. The vocabulary, derived

`VocabularyGenerator` reads every `src/main` `.java` and `.pure` file of the two pinned trees (legend-engine 4.145.0,
legend-pure 5.99.0) and writes `term, category, surface, occurrences, one upstream file`. Nothing is recalled and nothing
is listed by hand. It derives:

| Category | From |
|---|---|
| TYPE, EXCEPTION | each class, interface, enum and record a main source declares (camel case, two or more words) |
| FIELD | each field of a class under a `protocol` package |
| SUBTYPE | each protocol subtype's JSON name (`@JsonSubTypes.Type(name=)`, `@JsonTypeName`, `Tuples.pair(X.class, "name")`, `withSubtype`) |
| ROUTE | each JAX-RS resource's route: the class's `@Path`, then the method's (`server/v1/currentUser`) |
| IDENT | each identifier-shaped literal of 6+ characters: two camel-case words, or an underscore beside a lower-case letter. Never all capitals (SQL's keywords and types) and never lower snake case (SQL's functions). |
| SENTENCE | each literal's runs between its placeholders: 4+ words, or 2-3 words starting with a capital; 12-160 characters; in at most 5 upstream literals, or a capitalised whole literal in at most 25 ("Assert failed") |
| PURE_NAME | each name a Pure file declares (functions, classes, enumerations, profiles, properties) |
| TEXT | every upstream literal, whitespace folded: the edge's strings are verified against it |

Each term carries its **surface**, read from where its file sits:
- **LANGUAGE**: the grammar, the compiler, the protocol of model elements and value specifications, legend-pure's M3 and
  platform, legend-engine's Pure function library. This is the language lite implements and parses as-is (S364).
- **EXECUTION**: everything else. Execution plans and their results, the result serializers, the server's routes, the
  query store, test data, the relational Pure that writes legend-engine's SQL, legend-pure's runtime.

At read time the census leaves out, mechanically:
- the LANGUAGE surface's names (5,312);
- the names lite's own Pure spells: the prelude and the builtin tables (406), protocol fields and subtypes excepted,
  since they are wire shapes;
- the JDK's API names, read from the running JDK's `java.base` (405);
- a TYPE or EXCEPTION naming a class lite's product declares;
- the edge's strings upstream does not hold (449 of 573 distinctive ones).

Kept: 6,615 names, 15,749 sentences, 173 routes and 124 edge strings.

## 5. The reusable TypeScript/Python scan (for B1's G4)

`//tools/meaning:host_sources`, `tools/meaning/HostSources.java` (public, testonly):

```java
HostSources.Source s = HostSources.read("engine-client/src/relation-type.ts", text);
s.tokens();   // LITERAL, DOCSTRING (a Python string standing alone), NAME, PROPERTY (after . or ?.), COMMENT, PUNCT; each with its line
s.imports();  // module, names taken ("default", "*"), kind STATIC | TYPE_ONLY | DYNAMIC | GUARDED, line
s.exports();  // top-level exports (TS) or definitions (Python)
```

The census's helpers are in `MeaningCensusTest`, for G4 to call or copy:
- the scope: `HostRule.read(tools/guards/host_sql.txt)` gives its [typescript] and [python] folders, and its [harnesses]
  as the tests;
- the product/edge partition: `edgeSources(//tools/meaning:host_edge_srcs)`;
- the lenses: `MeaningCensus.hostStrings`, `hostNames`, `hostComments`, `edgeOnly`, `hostReach`.

G4's "the hosts' and DataCube's readers" of a Pair's `<a, b>` text is a LITERAL or PROPERTY query over the same tokens.

## 6. The inventory

### 6.1 By lens and category

| Lens | Category | (a) | (p) | (b) | (c) |
|---|---|---|---|---|---|
| M1 | DOC | 0 | 0 | 54 | 0 |
| M1 | EDGE | 2 | 0 | 3 | 47 |
| M1 | NAME | 33 | 0 | 136 | 137 |
| M1 | ROUTE | 0 | 0 | 1 | 3 |
| M1 | SENTENCE | 20 | 0 | 128 | 48 |
| M2 | NAME | 12 | 0 | 25 | 17 |
| M3 | ANSWER | 2 | 0 | 2 | 32 |
| M3 | BRANCH | 25 | 0 | 6 | 2 |
| M3 | EDGE_LIVE | 2 | 0 | 2 | 1 |
| M3 | REFUSED | 6 | 0 | 6 | 16 |
| M3 | UNUSED | 22 | 0 | 10 | 16 |
| M3 | UNUSED_LAZY | 0 | 0 | 0 | 6 |
| M3 | UNUSED_NOOP | 0 | 0 | 5 | 58 |
| M4 | BODY | 0 | 0 | 192 | 0 |
| M4 | TOSTRING | 0 | 0 | 4 | 1 |
| M5 | COMMENT | 7 | 2 | 42 | 83 |
| M6 | EXPORT | 9 | 0 | 0 | 0 |
| M7 | BUILD | 0 | 0 | 5 | 0 |
| M7 | DOOR | 1 | 0 | 0 | 1 |
| M7 | OPT_IN | 1 | 0 | 0 | 9 |
| M7 | STATIC | 0 | 0 | 0 | 2 |

### 6.2 (a): upstream meaning in the product, by item

The items the audit knew (tagged by their ids) and the ones this census found, which no audit row lists:
- **N-JUDGE**: the product's judges compute and branch for compiled legend-pure's comparison: VerdictSql's tolerance
  and canon grids, Equality, HostJudgments, DatabaseJudge, AssertVerdicts, and the words families' eager operands.
- **N-ROOTFORM**: legend-engine's toSQLString root form in the product resolver.

The user ruled on 2026-11-19, "we're not going to change prelude or parser". So two of this census's own groups and one
audit item are (b), in §6.4:
- **N-PRELUDE**: the prelude's upstream Pure bodies and the messages inside them. This goes beyond C5's four print
  forms: legend-engine's DbExtension asserts, its plan text warning, its External Format and TDS bodies, the PCT
  runner's prints.
- **N-PARSER**: legend-engine's grammar refusals in its words, in the product parser, its section grammars, its model
  builder and its protocol upgrade.
- **COMPLETENESS-7**: the parser's and the protocol emitter's span and emit quirks.

C5, C6, HOSTS-5, R2 and R7 are ruled, so their rows are in (b) (§6.4).

Untagged rows are single sites, each with its reason. The first 60 sites of each group are shown; the TSV holds all.


#### N-JUDGE (38)

Reason: the product judge computes the operands of legend-pure's failure sentence for the edge's words alone (GUARDS-4's eager value) [N-JUDGE]

| Lens | Where | Term |
|---|---|---|
| M3 UNUSED | com/legend/lowering/LiteVerdictWords.java:89 | `VerdictWords#elementText(Type, SqlExpr, Styled)` |
| M3 UNUSED | com/legend/lowering/LiteVerdictWords.java:111 | `VerdictWords#notEquivalent(String, SqlExpr, SqlExpr, SqlExpr, SqlExpr, SqlExpr, ` |
| M3 UNUSED | com/legend/lowering/LiteVerdictWords.java:111 | `VerdictWords#notEquivalent(String, SqlExpr, SqlExpr, SqlExpr, SqlExpr, SqlExpr, ` |
| M3 UNUSED | com/legend/lowering/LiteVerdictWords.java:111 | `VerdictWords#notEquivalent(String, SqlExpr, SqlExpr, SqlExpr, SqlExpr, SqlExpr, ` |
| M3 UNUSED | com/legend/lowering/LiteVerdictWords.java:111 | `VerdictWords#notEquivalent(String, SqlExpr, SqlExpr, SqlExpr, SqlExpr, SqlExpr, ` |
| M3 UNUSED | com/legend/lowering/LiteVerdictWords.java:111 | `VerdictWords#notEquivalent(String, SqlExpr, SqlExpr, SqlExpr, SqlExpr, SqlExpr, ` |
| M3 UNUSED | com/legend/lowering/LiteVerdictWords.java:111 | `VerdictWords#notEquivalent(String, SqlExpr, SqlExpr, SqlExpr, SqlExpr, SqlExpr, ` |
| M3 UNUSED | com/legend/lowering/LiteVerdictWords.java:111 | `VerdictWords#notEquivalent(String, SqlExpr, SqlExpr, SqlExpr, SqlExpr, SqlExpr, ` |
| M3 UNUSED | com/legend/lowering/LiteVerdictWords.java:82 | `VerdictWords#notInstanceOf(SqlExpr, boolean, String, String)` |
| M3 UNUSED | com/legend/testexec/LiteHostWords.java:222 | `HostWords#caught(RuntimeException, EdgeHearing)` |
| M3 UNUSED | com/legend/testexec/LiteHostWords.java:58 | `HostWords#cellMultisetDiffers(String, List, List, HostRules)` |
| M3 UNUSED | com/legend/testexec/LiteHostWords.java:58 | `HostWords#cellMultisetDiffers(String, List, List, HostRules)` |
| M3 UNUSED | com/legend/testexec/LiteHostWords.java:187 | `HostWords#jsonDiffers(String, String, boolean, String)` |
| M3 UNUSED | com/legend/testexec/LiteHostWords.java:64 | `HostWords#notEquivalent(Tabular, Tabular, double, String)` |
| M3 UNUSED | com/legend/testexec/LiteHostWords.java:151 | `HostWords#notInstanceOf(Object, String, String)` |
| M3 UNUSED | com/legend/testexec/LiteHostWords.java:39 | `HostWords#rowValuesDiffer(List, List, List, List, boolean)` |
| M3 UNUSED | com/legend/testexec/LiteHostWords.java:39 | `HostWords#rowValuesDiffer(List, List, List, List, boolean)` |
| M3 BRANCH | com/legend/compiler/spec/VerdictQueries.java:795 | `com.legend.compiler.spec.VerdictQueries#parseRendered` |
| M3 BRANCH | com/legend/compiler/spec/VerdictQueries.java:977 | `com.legend.compiler.spec.VerdictQueries#tdsCellText` |
| M3 BRANCH | com/legend/lowering/VerdictSql.java:234 | `com.legend.lowering.VerdictSql#equality` |
| M3 BRANCH | com/legend/lowering/VerdictSql.java:343 | `com.legend.lowering.VerdictSql#gridRows` |
| M3 BRANCH | com/legend/lowering/VerdictSql.java:375 | `com.legend.lowering.VerdictSql#gridCells` |
| M3 BRANCH | com/legend/lowering/VerdictSql.java:574 | `com.legend.lowering.VerdictSql#contains` |
| M3 BRANCH | com/legend/lowering/VerdictSql.java:698 | `com.legend.lowering.VerdictSql#perElement` |
| M3 BRANCH | com/legend/lowering/VerdictSql.java:895 | `com.legend.lowering.VerdictSql#gridTolerance` |
| M3 BRANCH | com/legend/lowering/VerdictSql.java:1286 | `com.legend.lowering.VerdictSql#gridPair` |
| M3 BRANCH | com/legend/testexec/Equality.java:206 | `com.legend.testexec.Equality#same` |
| M3 BRANCH | com/legend/testexec/Equality.java:234 | `com.legend.testexec.Equality#ordered` |
| M3 BRANCH | com/legend/testexec/Equality.java:251 | `com.legend.testexec.Equality#contains` |
| M3 BRANCH | com/legend/testexec/HostJudgments.java:571 | `com.legend.testexec.HostJudgments#eqOf` |
| M3 BRANCH | com/legend/testexec/HostJudgments.java:890 | `com.legend.testexec.HostJudgments#renderedOf` |
| M3 BRANCH | com/legend/testplan/AssertVerdicts.java:1872 | `com.legend.testplan.AssertVerdicts#textFirst` |
| M3 BRANCH | com/legend/testplan/DatabaseJudge.java:337 | `com.legend.testplan.DatabaseJudge#lite` |
| M3 BRANCH | com/legend/testplan/DatabaseJudge.java:367 | `com.legend.testplan.DatabaseJudge#crossNumberKinds` |
| M3 REFUSED | com/legend/lowering/VerdictSql.java:701 | `VerdictNumbers#floatsWithin(SqlExpr, SqlExpr)` |
| M3 REFUSED | com/legend/lowering/VerdictSql.java:3067 | `VerdictNumbers#floatsWithin(SqlExpr, SqlExpr)` |
| M5 COMMENT | com/legend/testplan/DatabaseJudge.java:230 | `as upstream writes` |
| M5 COMMENT | com/legend/testplan/TestPlanner.java:224 | `as upstream does` |

#### untagged: legend-engine's calendar schema (LegendCalendarSchema.<type>_Calendar and its fiscal columns) in product lowering: the calendar functions' own definition or a legend-engine convention; no register row (for the user) (23)

| Lens | Where | Term |
|---|---|---|
| M1 NAME | com/legend/lowering/CalendarAgg.java | `fiscalDay` |
| M1 NAME | com/legend/lowering/CalendarAgg.java | `numberOfFiscalDaysInYear` |
| M1 NAME | com/legend/lowering/CalendarAgg.java | `currentYear` |
| M1 NAME | com/legend/lowering/CalendarAgg.java | `numberOfFiscalDaysInMonth` |
| M1 NAME | com/legend/lowering/CalendarAgg.java | `currentMonthNum` |
| M1 NAME | com/legend/lowering/CalendarAgg.java | `prior4WeekDate` |
| M1 NAME | com/legend/lowering/CalendarAgg.java | `prior12WeekDate` |
| M1 NAME | com/legend/lowering/CalendarAgg.java | `prior52WeekDate` |
| M1 NAME | com/legend/lowering/CalendarAgg.java | `previousFiscalYear` |
| M1 NAME | com/legend/lowering/CalendarAgg.java | `previousFiscalMonth` |
| M1 NAME | com/legend/lowering/CalendarAgg.java | `currentWeek` |
| M1 NAME | com/legend/lowering/CalendarAgg.java | `fiscalDayOfWeek` |
| M1 NAME | com/legend/lowering/CalendarAgg.java | `adjustedDate` |
| M1 NAME | com/legend/lowering/CalendarAgg.java | `previousBusinessDay` |
| M1 NAME | com/legend/lowering/CalendarAgg.java | `fiscalWeekOffset` |
| M1 NAME | com/legend/lowering/CalendarAgg.java | `shortNameWeekDay` |
| M1 NAME | com/legend/lowering/CalendarAgg.java | `dayOfCalendarYear` |
| M1 NAME | com/legend/lowering/CalendarAgg.java | `currentQuarterNum` |
| M1 NAME | com/legend/lowering/CalendarAgg.java | `fiscalDayOfMonth` |
| M1 NAME | com/legend/lowering/CalendarAgg.java | `previousFiscalQuarter` |
| M1 NAME | com/legend/lowering/CalendarAgg.java | `fiscalDayOfQuarter` |
| M1 NAME | com/legend/lowering/CalendarAgg.java | `LegendCalendarSchema` |
| M1 NAME | com/legend/lowering/CalendarAgg.java | `_Calendar` |

#### JUDGES-6 (13)

Reason: the "is this the edge" flag [JUDGES-6]

| Lens | Where | Term |
|---|---|---|
| M2 NAME | com/legend/database/PlanStyle.java | `judgesEngineText` |
| M2 NAME | com/legend/database/PlanStyle.java | `judgesEngineText` |
| M2 NAME | com/legend/testplan/EngineTextGates.java | `EngineTextGates` |
| M2 NAME | com/legend/testplan/EngineTextGates.java | `reachesUpstreamText` |
| M3 ANSWER | com/legend/database/PlanStyle.java:117 | `PlanStyle#judgesEngineText()` |
| M3 BRANCH | com/legend/testplan/EngineTextGates.java:26 | `com.legend.testplan.EngineTextGates#facet` |
| M3 BRANCH | com/legend/testplan/EngineTextGates.java:94 | `com.legend.testplan.EngineTextGates#planningCall` |
| M3 BRANCH | com/legend/testplan/EngineTextGates.java:101 | `com.legend.testplan.EngineTextGates#siteWords` |
| M3 BRANCH | com/legend/testplan/EngineTextGates.java:107 | `com.legend.testplan.EngineTextGates#sizeWords` |
| M3 REFUSED | com/legend/testplan/EngineTextGates.java:26 | `PlanStyle#facet(Class)` |
| M3 REFUSED | com/legend/testplan/EngineTextGates.java:94 | `PlanStyle#facet(Class)` |
| M3 REFUSED | com/legend/testplan/EngineTextGates.java:101 | `PlanStyle#facet(Class)` |
| M3 REFUSED | com/legend/testplan/EngineTextGates.java:107 | `PlanStyle#facet(Class)` |

#### C3-TS (10)

Reason: a product record reads legend-engine's generatePlan and lambdaRelationType shapes, its only callers the edge's (COMPLETENESS-1) [C3-TS]

| Lens | Where | Term |
|---|---|---|
| M1 NAME | engine-client/src/relation-type.ts:90 | `tdsColumns` |
| M1 NAME | engine-client/src/relation-type.ts:91 | `rootExecutionNode` |
| M1 NAME | engine-client/src/relation-type.ts:91 | `resultType` |
| M1 NAME | engine-client/src/relation-type.ts:91 | `tdsColumns` |
| M1 NAME | engine-client/src/relation-type.ts:92 | `rootExecutionNode` |
| M1 NAME | engine-client/src/relation-type.ts:93 | `resultType` |
| M1 NAME | engine-client/src/relation-type.ts:93 | `tdsColumns` |
| M1 NAME | engine-client/src/relation-type.ts:95 | `tdsColumns` |
| M6 EXPORT | engine-client/src/planned-sql.ts:28 | `fromPlan` |
| M6 EXPORT | engine-client/src/values.ts:112 | `timestampFromText` |

#### untagged: legend-engine's test-data setup messages, in its words, in the product's seeding (R2 rules the generator's spellings, not these) (5)

| Lens | Where | Term |
|---|---|---|
| M1 SENTENCE | com/legend/setup/CsvSeed.java | `There should be exactly 1 schema with name` |
| M1 SENTENCE | com/legend/setup/CsvSeed.java | `found in the provided runtime database(s) [` |
| M1 SENTENCE | com/legend/setup/CsvSeed.java | `] at the given connection.` |
| M1 SENTENCE | com/legend/setup/CsvSeed.java | `not found in Schema` |
| M1 SENTENCE | com/legend/setup/CsvSeed.java | `not found in Table` |

#### C1 (5)

Reason: legend-engine's union frame: the product answers null, the decisions on it stay in product lowering [C1]

| Lens | Where | Term |
|---|---|---|
| M3 ANSWER | com/legend/lowering/ProductLoweringForms.java:112 | `LoweringForms#unionFrame()` |
| M3 BRANCH | com/legend/lowering/Fold.java:1661 | `com.legend.lowering.Fold#tryProjectAll` |
| M3 BRANCH | com/legend/lowering/Lowerer.java:3028 | `com.legend.lowering.Lowerer#unionFrame` |
| M3 EDGE_LIVE | com.legend.lowering.SubselectPrune:75 | `SubselectPrune#prune(SqlQuery, boolean, String)` |
| M5 COMMENT | com/legend/lowering/Lowerer.java:3004 | `(engine model)` |

#### untagged: a product helper only the Python edge uses (HOSTS-15: the edge reaches into product privates) (5)

| Lens | Where | Term |
|---|---|---|
| M6 EXPORT | python/legend_lite/_home.py:100 | `locked` |
| M6 EXPORT | python/legend_lite/_reads.py:292 | `run_arrow` |
| M6 EXPORT | python/legend_lite/_reads.py:299 | `run_rows_json` |
| M6 EXPORT | python/legend_lite/engine.py:393 | `_under` |
| M6 EXPORT | python/legend_lite/engine.py:403 | `_stored` |

#### R11a (4)

Reason: the store's legend-engine error type in a product answer (PH-2); R11(a) (the user, 2026-11-18): to the presentation program [R11a]

| Lens | Where | Term |
|---|---|---|
| M1 EDGE | com/legend/queries/SavedQueries.java | `IllegalArgumentException` |
| M1 SENTENCE | com/legend/queries/SavedQueries.java | `Unknown sort-by value` |
| M5 COMMENT | com/legend/queries/SavedQueries.java:31 | `as the engine does` |
| M5 COMMENT | com/legend/queries/SavedQueries.java:230 | `as legend-engine answers` |

#### LOWERSQL-7 (4)

Reason: legend-engine's H2 lexicon and its NON_KEYWORDS session setting in the product dialect (ruled to stay by the P2 checkpoint audit's S5, not the user; B3's item) [LOWERSQL-7]

| Lens | Where | Term |
|---|---|---|
| M2 NAME | com/legend/sql/dialect/H2.java | `ENGINE_LEXICON` |
| M2 NAME | com/legend/sql/dialect/H2.java | `FREED_BY_THE_ENGINE` |
| M2 NAME | com/legend/sql/dialect/H2.java | `freedByTheEngine` |
| M5 COMMENT | com/legend/sql/dialect/H2.java:35 | `replayed here reads as it did in the engine` |

#### untagged: legend-engine's aggregation-aware groupBy assertions, in its words, in the product typer (3)

| Lens | Where | Term |
|---|---|---|
| M1 SENTENCE | com/legend/compiler/spec/GroupByChecker.java | `SubAggIds and Ids should not have an intersection` |
| M1 SENTENCE | com/legend/compiler/spec/GroupByChecker.java | `Ids and Ids should not have an intersection` |
| M1 SENTENCE | com/legend/compiler/spec/GroupByChecker.java | `SubAggIds must be a subset of ids` |

#### untagged: legend-pure's date-format parse messages, in its words, in product lowering (K1's family; CompatSpellingGuardTest's list does not hold them) (3)

| Lens | Where | Term |
|---|---|---|
| M1 SENTENCE | com/legend/lowering/DateFormats.java | `Missing closing quote in format string` |
| M1 SENTENCE | com/legend/lowering/DateFormats.java | `Could not find` |
| M1 SENTENCE | com/legend/lowering/DateFormats.java | `Could not find end of date format starting at index` |

#### PRODUCT-7 (3)

Reason: the edge's facet in the product planner, its testDataTimestampText with no caller [PRODUCT-7]

| Lens | Where | Term |
|---|---|---|
| M2 NAME | com/legend/planner/UpstreamPlans.java | `UpstreamPlans` |
| M2 NAME | com/legend/planner/UpstreamPlans.java | `engineSeedSql` |
| M2 NAME | com/legend/planner/UpstreamPlans.java | `engineSeedSqlFromRecords` |

#### HOSTS-4 (3)

Reason: the engine library's loader in the product module (HOSTS-4) [HOSTS-4]

| Lens | Where | Term |
|---|---|---|
| M6 EXPORT | python/legend_lite/_loader.py:209 | `EngineMissing` |
| M6 EXPORT | python/legend_lite/_loader.py:231 | `engine_loaded` |
| M7 OPT_IN | python/legend_lite/_loader.py:220 | `legend_lite_engine` |

#### R10 (2)

Reason: legend-pure's "Assert failed" as the six other in-body guards' default; R10 (the user, 2026-11-18) [R10]

| Lens | Where | Term |
|---|---|---|
| M1 SENTENCE | com/legend/compiler/spec/LambdaBodies.java | `Assert failed` |
| M1 EDGE | com/legend/compiler/spec/LambdaBodies.java | `Assert failed` |

#### untagged: legend-pure's compile message for a missing required property, in its words, in the product typer (J10's rule) (2)

| Lens | Where | Term |
|---|---|---|
| M1 SENTENCE | com/legend/compiler/spec/NewChecker.java | `Missing value(s) for required property` |
| M1 SENTENCE | com/legend/compiler/spec/NewChecker.java | `which has a multiplicity of` |

#### untagged: legend-pure's duration-unit message, in its words, in product lowering (K1's family) (2)

| Lens | Where | Term |
|---|---|---|
| M1 SENTENCE | com/legend/lowering/Scalars.java | `Unsupported duration unit for StrictDate. Units can only be: [YEARS, DAYS, MONTH` |
| M1 SENTENCE | com/legend/lowering/Scalars.java | `Unsupported duration unit for StrictDate. Units can only be` |

#### untagged: legend-pure's constraint-violation message, in its words, raised by the product (2)

| Lens | Where | Term |
|---|---|---|
| M1 SENTENCE | com/legend/resolver/GraphEmission.java | `Constraint :[` |
| M1 SENTENCE | com/legend/resolver/GraphEmission.java | `] violated in the Class` |

#### COMPLETENESS-8 (2)

Reason: the product's test discovery in legend-engine's PureTestBuilder order, and its exclusions [COMPLETENESS-8]

| Lens | Where | Term |
|---|---|---|
| M2 NAME | com/legend/test/PureTests.java | `excludedByEngine` |
| M2 NAME | com/legend/test/PureTests.java | `engineSuiteOrder` |

#### untagged: legend-engine's mapping message, in its words, in the product normalizer (J10's rule) (1)

| Lens | Where | Term |
|---|---|---|
| M1 SENTENCE | com/legend/normalizer/MappingNormalizer.java | `Inconsistent database definitions for the mapping` |

#### untagged: legend-engine's CTE names (subquery_cte_<level>_<index>) for a model's declared post-processor, in product lowering; no register row (for the user) (1)

| Lens | Where | Term |
|---|---|---|
| M1 NAME | com/legend/lowering/SqlPostProcessors.java | `subquery_cte_` |

#### C3-Py (1)

Reason: Python's product plan record reads legend-engine's tdsColumns' relationalType (COMPLETENESS-4) [C3-Py]

| Lens | Where | Term |
|---|---|---|
| M1 NAME | python/legend_lite/_plans.py:29 | `relationalType` |

#### untagged: a DateTime literal's written text, handed to the product's text form only the edge's legend-pure print reads (D1.2's DATETIME_PRINT) (1)

| Lens | Where | Term |
|---|---|---|
| M3 UNUSED | com/legend/lowering/Styled.java:34 | `DateTimeTexts#dateTimeText(SqlExpr, String)` |

#### C2 (1)

Reason: the exists-join seam: sites the edge's form alone reads; the chooser flag beside it (existsJoinable, a Lowerer field the planner sets, LOWERSQL-4) is a field, which M3 does not follow [C2]

| Lens | Where | Term |
|---|---|---|
| M3 UNUSED | com/legend/lowering/ProductLoweringForms.java:91 | `LoweringForms#existsJoin(SqlSelect, ExistsSites)` |

#### untagged: view's name argument, computed for the edge's frame alone (the product writes the subselect unnamed); unruled (ProductFormsUseTheirArgumentsTest rules only its alias supplier) (1)

| Lens | Where | Term |
|---|---|---|
| M3 UNUSED | com/legend/lowering/ProductLoweringForms.java:32 | `LoweringForms#view(SqlSelect, Supplier, String)` |

#### untagged: the WHERE zone stamp the product computes for legend-engine's zone order (L10's order is the edge's; the stamp is still the product's work) (1)

| Lens | Where | Term |
|---|---|---|
| M3 UNUSED | com/legend/lowering/ProductLoweringForms.java:128 | `WhereZones#merge(SqlExpr, SqlExpr, Stamp)` |

#### untagged: a structure cell's type name, handed to the product writer only the edge's presenter reads (D1: legend-pure's print forms) (1)

| Lens | Where | Term |
|---|---|---|
| M3 UNUSED | com/legend/result/CellText.java:70 | `CellText$Forms#structure(Object, String, Column)` |

#### untagged: SQLite's Decimal arithmetic SQL in the product dialect, reached only under the edge's text style (the product refuses it) (1)

| Lens | Where | Term |
|---|---|---|
| M3 BRANCH | com/legend/sql/dialect/SQLite.java:1597 | `com.legend.sql.dialect.SQLite#call` |

#### JUDGES-3 (1)

Reason: BOTH compares the judges' messages unless the edge's run says their words differ: where JUDGES-3's disagreement surfaces [JUDGES-3]

| Lens | Where | Term |
|---|---|---|
| M3 BRANCH | com/legend/testexec/IsolatedRun.java:154 | `com.legend.testexec.IsolatedRun#difference` |

#### N-ROOTFORM (1)

Reason: legend-engine's toSQLString root form, its javadoc "consumed ONLY by the toSQLString golden-text surface", sits in the product resolver; its mapping argument only the edge passes [N-ROOTFORM]

| Lens | Where | Term |
|---|---|---|
| M3 EDGE_LIVE | com.legend.resolver.RelationalRootForm:45 | `RelationalRootForm#apply(List, ModelContext, String)` |

#### untagged: a generated column's presented name, as upstream prints it, in the product MIR (a presentation in the plan's data; for the presentation program) (1)

| Lens | Where | Term |
|---|---|---|
| M5 COMMENT | com/legend/sql/SqlSource.java:44 | `as upstream prints` |

#### HOSTS-7 (1)

Reason: an undeclared door: java -cp server_deploy.jar com.legend.edge.EdgeServer serves legend-engine's routes; R11(b) (the user, 2026-11-18): the first small follow-up PR [HOSTS-7]

| Lens | Where | Term |
|---|---|---|
| M7 DOOR | com/legend/edge/EdgeServer.java:93 | `com.legend.edge.EdgeServer#main` |


### 6.3 (p): a ruling pending

| Lens | Reason | Hits | e.g. |
|---|---|---|---|
| M5 COMMENT | the product's default substring passes legend-engine's relational (start, length) verbatim, against Pure's 0-based end-exclusive indexes, its corrected rule only under CORRECT_SQL_SUBSTRING_INDEXING (S130; R4, its ruling pending) [COMPLETENESS-5] | 1 | com/legend/lowering/Scalars.java:1490 `engine goldens` |
| M5 COMMENT | the product's default indexOf is legend-engine's 1-based strpos, 0 on a miss, against Pure's 0-based, -1 on a miss (S134; R4, its ruling pending) [COMPLETENESS-5] | 1 | com/legend/lowering/Scalars.java:1555 `the engine translates` |


### 6.4 (b): covered by a user ruling

| Lens | Reason | Hits | e.g. |
|---|---|---|---|
| M4 BODY | an upstream Pure body the prelude ships (the world-map tenet: declarations, views over the platform's own tables, no other Pure bodies; residue named, never ported) [N-PRELUDE] -- (b) by the user's ruling of 2026-11-19, "we're not going to change prelude or parser" | 192 | prelude:prelude.pure:438 `meta::pure::functions::math::binFloor` |
| M1 SENTENCE | legend-engine's grammar refusal, in its words, raised by the product parser (a product raise is lite's kind; upstream's words the edge's: J10, D1.6) [N-PARSER] -- (b) by the user's ruling of 2026-11-19, "we're not going to change prelude or parser" | 65 | com/legend/parser/DatabaseProtocolParser.java `Missing table or alias` |
| M1 DOC | a platform declaration's documentation, a tagged value verbatim with its signature (the reference-checkout tenet: signatures verified verbatim against the real .pure sources) | 54 | prelude:prelude.pure:68 `Identity (i.e., pointer equality)` |
| M1 NAME | the model's own element fields (S364: the one grammar) | 43 | com/legend/model/AssociationDefinition.java `qualifiedName` |
| M1 SENTENCE | an upstream message inside a Pure body the prelude ships (legend-engine's relational asserts, its External Format and TDS bodies, the PCT runner's prints) [N-PRELUDE] -- (b) by the user's ruling of 2026-11-19, "we're not going to change prelude or parser" | 41 | prelude:prelude.pure:1412 `Surveyor Suite completed` |
| M1 NAME | the language's names (the ###Relational dynaFunctions, the registry's natives): the one grammar (S364, the user, 2026-11-17) and the reference-checkout tenet (signatures verified verbatim) | 34 | com/legend/builtin/DynaFn.java `ASCII` |
| M5 COMMENT | the protocol reader and emitter reproduce legend-engine's span and emit quirks [COMPLETENESS-7] -- (b) by the user's ruling of 2026-11-19, "we're not going to change prelude or parser" | 19 | com/legend/protocol/Protocol.java:484 `quirk` |
| M1 NAME | the saved queries' and pages' records keep legend-engine's Query shape: lite API v1 §1, "the same store and the same records" (the user, 2026-11-17: every choice ruled) | 16 | com/legend/queries/SavedCubes.java `createdAt` |
| M5 COMMENT | the parser reproduces legend-engine's span and walker quirks, which reach the product's protocol JSON with source information [COMPLETENESS-7] -- (b) by the user's ruling of 2026-11-19, "we're not going to change prelude or parser" | 15 | com/legend/parser/DatabaseProtocolParser.java:450 `QUIRK` |
| M1 NAME | the saved queries' records (lite API v1 §1, the user, 2026-11-17) | 11 | datacube/src/saved-queries.ts:139 `defaultParameterValues` |
| M2 NAME | the model protocol's date and value JSON (S369, RULED by the user, 2026-11-17) | 11 | com/legend/protocol/DomainReader.java `engineBound` |
| M1 SENTENCE | the test-data generator's legend-engine words stay for now (R2, the user, 2026-11-18) [R2] | 10 | com/legend/testdatagen/TestDataGenerator.java `not passed in TemporalMilestoningDates` |
| M1 NAME | the model protocol (S364, S369) | 8 | com/legend/protocol/ActivatorReader.java `bigQueryFunctionConfig` |
| M1 SENTENCE | legend-engine's model-building refusal, in its words, in the product (J10's rule: a kind, upstream's words at the edge) [N-PARSER] -- (b) by the user's ruling of 2026-11-19, "we're not going to change prelude or parser" | 7 | com/legend/model/AssociationDefinition.java `Property name cannot be null` |
| M3 UNUSED | the test plan's edge extension, no-op in the product (docs/PLAN_AS_DATA_2026_11_07.md, the user's rulings of 2026-11-08: the edge's typed extension point) | 6 | com/legend/testexec/NoEdgeRun.java:100 `EdgeHearing#afterFailure(RuntimeExceptio` |
| M1 NAME | the model protocol lite reads and writes as upstream defines it (S364, S369: RULED by the user, 2026-11-17) | 5 | com/legend/protocol/Protocol.java `qualifiedName` |
| M1 NAME | a saved query's execution context, its record's field (lite API v1 §1, the user, 2026-11-17) | 5 | datacube/src/examples/markets-classes.ts:25 `explicitExecutionContext` |
| M3 BRANCH | the test runner asks the edge's extension whether it records (PLAN_AS_DATA, the user's rulings of 2026-11-08: the edge's typed extension point) | 5 | com/legend/testexec/TestRunner.java:188 `com.legend.testexec.TestRunner$Moved#<in` |
| M1 SENTENCE | the model's own grammar text, printed (S364: the one grammar) | 4 | com/legend/protocol/DeephavenComposer.java `ownership: Deployment { identifier` |
| M1 NAME | DataCube's saved pages' records (lite API v1 §1, the user, 2026-11-17) | 4 | datacube/src/cube-store.ts:175 `createdAt` |
| M7 BUILD | a product bundle lists the edge entry as an input, so the build graph cannot prove it edge-free (HOSTS-8; HOSTS-5, ruled) [HOSTS-5] | 4 | //datacube:bundle_bundle `//datacube:bundle_bundle` |
| M1 NAME | lineage rebuilt upstream's way for everything (C8, the user, 2026-11-14): scanRelations' labels are its value [COMPLETENESS-8] | 3 | com/legend/lineage/ScanRelations.java `tdsJoin` |
| M1 NAME | the test-data generator's legend-engine form stays for now (R2, the user, 2026-11-18) [R2] | 3 | com/legend/testdatagen/TestDataGenerationNatives.java `rowIdentifiers` |
| M2 NAME | the doors' legend-engine parse entries (S364, J21); R7 (the user, 2026-11-18) takes the level-less defaults to LITE [R7] | 3 | com/legend/door/Parse.java `engineRecords` |
| M3 UNUSED_NOOP | as its ANSWER row: S310 | 3 | com/legend/planner/PlanAllocations.java:264 `RoutedQueries#routingActivity(TypedSpec,` |
| M3 REFUSED | the test runner hands the edge's extension its rows (PLAN_AS_DATA, the user's rulings of 2026-11-08) | 3 | com/legend/testexec/BodyRun.java:60 `EdgeHearing#judged(EdgeJudged, List, boo` |
| M3 REFUSED | the test door hands the edge's extension its data (PLAN_AS_DATA, the user's rulings of 2026-11-08) | 3 | com/legend/testplan/TestDoor.java:189 `Edge#siteWords(int, Map)` |
| M1 NAME | lineage rebuilt upstream's way for everything (C8, the user, 2026-11-14) [COMPLETENESS-8] | 2 | com/legend/compiler/spec/LineageTreeLines.java `unionBase` |
| M1 EDGE | legend-engine's plan-text warning in a prelude body [N-PRELUDE] -- (b) by the user's ruling of 2026-11-19, "we're not going to change prelude or parser" | 2 | prelude:prelude.pure:6757 `Warning: Results only shown for first re` |
| M2 NAME | the engine-text hook, refused by the product (HOOK-RULED; R3's residue) | 2 | com/legend/testplan/EngineTestText.java `EngineTestText` |
| M3 UNUSED_NOOP | the test-data generator's printing seam: R2 (the user, 2026-11-18) [R2] | 2 | com/legend/testdatagen/LitePrinting.java:64 `TestDataPlanner$Printing#checkTree(Model` |
| M3 UNUSED | the test-data generator's printing seam: R2 (the user, 2026-11-18) [R2] | 2 | com/legend/testdatagen/LitePrinting.java:58 `TestDataPlanner$Printing#headerName(Tabl` |
| M3 EDGE_LIVE | the test-data generator's legend-engine row-identifier form stays for now (R2, the user, 2026-11-18): its seed argument only the edge's route passes [R2] | 2 | com.legend.testdatagen.TestDataPlanner:142 `TestDataPlanner#plan(ModelContext, Lambd` |
| M5 COMMENT | the model JSON without source information, as legend-engine writes it (S369, RULED by the user, 2026-11-17) | 2 | com/legend/protocol/SourceInformation.java:13 `as the engine answers` |
| M5 COMMENT | the test-data generator writes as legend-engine does (R2, the user, 2026-11-18) [R2] | 2 | com/legend/testdatagen/TestDataPlanner.java:647 `as upstream writes` |
| M1 SENTENCE | legend-engine's protocol refusal, in its words, in the product [N-PARSER] -- (b) by the user's ruling of 2026-11-19, "we're not going to change prelude or parser" | 1 | com/legend/protocol/ProtocolUpgrade.java `Unexpected number of parameters values f` |
| M1 EDGE | lineage rebuilt upstream's way for everything (C8, the user, 2026-11-14): scanRelations' labels are its value [COMPLETENESS-8] | 1 | com/legend/lineage/ScanRelations.java `unionAlias` |
| M1 NAME | lite's route for DataCube's saved pages (lite API v1 §1, the user, 2026-11-17) | 1 | com/legend/server/LegendHttpServer.java `dataCube` |
| M1 NAME | a saved query's record field (lite API v1 §1, the user, 2026-11-17) | 1 | datacube/src/app/sources.ts:285 `defaultParameterValues` |
| M1 ROUTE | the engine plane's health check, in the picker that loads the declared opt-in by URL (H11); R6 (the user, 2026-11-18) keeps legend-engine's work toward legend-engine | 1 | datacube/demo/planners.ts:88 `server/v1/info` |
| M2 NAME | a date literal's protocol JSON (S369) | 1 | com/legend/values/PureDateLiteral.java `engineJson` |
| M2 NAME | upstream's function ids, the language's own names (S364) | 1 | com/legend/model/SignatureMangle.java `upstreamTail` |
| M2 NAME | legend-engine's parse quirks in the product parser [COMPLETENESS-7] -- (b) by the user's ruling of 2026-11-19, "we're not going to change prelude or parser" | 1 | com/legend/parser/EngineQuirks.java `EngineQuirks` |
| M2 NAME | legend-engine's grammar level: the one grammar (S364, the user, 2026-11-17; J21); R7 (the user, 2026-11-18): the level-less entries default to LITE in the three-mode program [R7] | 1 | com/legend/parser/GrammarLevel.java `LEGEND_ENGINE` |
| M2 NAME | legend-engine's bare-name function surface, generated from its Handlers registry: the language's names (S364; the reference-checkout tenet) | 1 | com/legend/builtin/EngineHandlers.java `EngineHandlers` |
| M2 NAME | the engine-text hook in the DDL spelling (D1.7: engine text the edge's, its printer handed in) | 1 | com/legend/sql/dialect/DdlSpelling.java `engineText` |
| M2 NAME | the test-data generator's legend-engine form stays for now (R2, the user, 2026-11-18) [R2] | 1 | com/legend/testdatagen/TestDataPlanner.java `upstreamCell` |
| M2 NAME | the engine-text residue in the body driver, its pin ratified (R3, the user, 2026-11-18: BodyDriver 545) | 1 | com/legend/testplan/BodyDriver.java `engineText` |
| M2 NAME | the engine-text residue, its pin ratified (R3, the user, 2026-11-18: EngineTextPlanning 151) | 1 | com/legend/testplan/EngineTextPlanning.java `EngineTextPlanning` |
| M3 ANSWER | S362 (RULED by the user, 2026-11-17): the product's object reference names no connection; legend-engine's bytes stay at the edge | 1 | com/legend/lowering/ProductLoweringForms.java:48 `LoweringForms#objectReferenceConnection(` |
| M3 UNUSED | the activity SQL is the style's pick (S310, RULED by the user, 2026-11-14, DE2 (a)): the edge's flag beside the product's own SQL | 1 | com/legend/planner/PlanAllocations.java:193 `ActivityTexts#activitySql(List, String, ` |
| M3 ANSWER | an aggregation-aware query's routing activity is the style's (S310; S361's ruling 7 (a)): the product answers none | 1 | com/legend/planner/PlanAllocations.java:264 `RoutedQueries#routingActivity(TypedSpec,` |
| M3 UNUSED | an assert listener's no-op: the test plan's edge extension (PLAN_AS_DATA, the user's rulings of 2026-11-08) | 1 | com/legend/test/PureTestRunner.java:373 `AssertListener#unjudged(String, String)` |
| M3 BRANCH | the test door asks the edge's extension (PLAN_AS_DATA, the user's rulings of 2026-11-08) | 1 | com/legend/testplan/TestDoor.java:142 `com.legend.testplan.TestDoor#planned` |
| M4 TOSTRING | legend-pure's <a, b> print of a Pair, the product writer's form today; R5 (the user, 2026-11-18): LITE's is its JSON object, <a, b> to PURE and ENGINE [C5] | 1 | prelude:prelude.pure:20 `meta::pure::functions::collection::Pair` |
| M4 TOSTRING | legend-pure's [a, b] print of a List; S363 (RULED): the product's text of a list is its JSON; R5 takes the body to PURE and ENGINE [C5] | 1 | prelude:prelude.pure:34 `meta::pure::functions::collection::List` |
| M4 TOSTRING | legend-pure's 'SQLNull' print; R5 takes the prelude's legend-pure print bodies to PURE and ENGINE [C5] | 1 | prelude:prelude.pure:3546 `meta::relational::metamodel::SQLNull` |
| M4 TOSTRING | legend-pure's TDSColumn print; R5 takes the prelude's legend-pure print bodies to PURE and ENGINE [C5] | 1 | prelude:prelude.pure:4950 `meta::pure::tds::TDSColumn` |
| M5 COMMENT | the format read as upstream does (the user's ruling of 2026-11-10, cited at the site) | 1 | com/legend/executionplan/PlanJson.java:102 `as upstream does` |
| M5 COMMENT | a number's plain-digit text, as upstream prints it (the user's ruling, cited at the site) | 1 | com/legend/sql/dialect/H2.java:616 `as upstream prints` |
| M5 COMMENT | a store's name folded as legend-engine does (decision D8, the user's, 2026-10-13) | 1 | com/legend/sql/dialect/Postgres.java:1377 `as legend-engine does` |
| M5 COMMENT | DataCube's product query works around legend-engine's unpartitioned over (S24); R6 (the user, 2026-11-18): only toward legend-engine [C6] | 1 | datacube/src/query.ts:319 `ENGINE GAP` |
| M7 BUILD | the product wheel ships legend_lite.edge (HOSTS-5, ruled: the orchestrator's corrected list, 2026-11-19) [HOSTS-5] | 1 | //python:wheel_package `//python:wheel_package` |


### 6.5 (c): no upstream meaning

| Lens | Reason | Hits | e.g. |
|---|---|---|---|
| M1 NAME | a generic coding word (rowCount, notEmpty, distinctCount, isError, ContentType, openFiles, yyyyMMdd) that upstream also spells (listed 2026-11-19: no catch-all, so a new hit is unclassified, never hidden) | 57 | com/legend/protocol/spec/GraphFetchLiteral.java `className` |
| M3 UNUSED_NOOP | a product form that reads none of its parameters (a no-op, a constant or a refusal): the designed seam; any meaning on it is in its BRANCH, UNUSED or REFUSED rows | 50 | com/legend/compiler/spec/LiteGoldenNullWord.java:15 `GoldenNullWord#isNullWord(String)` |
| M5 COMMENT | legend-engine's recorded answer (a golden) cited as the witness of a semantic the product shares -- what a navigation, a membership, a milestone or an aggregate means -- not a spelling (the upstream-fidelity tenet); sampled 2026-11-19 (TemporalFrame, Substitution, SyntheticHeads, ModelJoinNesting, EnumSourceValues), the rest not read one by one | 37 | com/legend/compiler/element/type/PlatformTypes.java:257 `Engine golden` |
| M1 EDGE | a phrase lite's own words and the edge's share ("Unexpected token", "in mapping '", "is not empty", "does not contain", "cannot be cast to"): lite's sentence, which upstream happens to hold too (listed 2026-11-19: no catch-all, so a new hit is unclassified, never hidden) | 33 | com/legend/error/CompileKind.java `' cannot extend '` |
| M3 ANSWER | a two-form kind's, a dialect's or a hook's product answer: the seam itself (hook criteria, HookRulingTest); any meaning on it is in its BRANCH rows | 30 | com/legend/compiler/spec/LiteGoldenNullWord.java:15 `GoldenNullWord#isNullWord(String)` |
| M5 COMMENT | the model's text as upstream's grammar composer prints it: the language's own form (the upstream-fidelity tenet; S364) | 27 | com/legend/protocol/ConnectionComposer.java:14 `as upstream prints` |
| M1 SENTENCE | a generic phrase lite's own message and upstream's share (each listed, 2026-11-19: no catch-all, so a new upstream sentence is unclassified, never hidden) | 26 | com/legend/json/Json.java `Cannot serialize` |
| M1 NAME | lite's own DataCube app's settings and routes | 21 | datacube/src/app.ts:726 `dataCube` |
| M1 NAME | the database's own function and option names | 18 | com/legend/sql/dialect/BigQuery.java `ASCII` |
| M3 REFUSED | the plan style's facet lookup, refused by the product's style by name: the hook itself (PlanStyleFacetsTest, HookRulingTest) | 12 | com/legend/planner/BodyPlanner.java:311 `PlanStyle#facet(Class)` |
| M1 SENTENCE | a SQL statement's text (the database's language) | 11 | com/legend/sql/dialect/AnsiSqlRenderer.java `Create Table` |
| M3 UNUSED | a dialect hook the edge's engine-text dialects override (D1.7: engine text the edge's); the product dialect needs not every argument | 11 | com/legend/sql/dialect/Databricks.java:863 `AnsiSqlRenderer#compactList(SqlWriter, C` |
| M5 COMMENT | the product does exactly what upstream does: a semantic, not a spelling (AGENTS.md's upstream-fidelity tenet, the user, 2026-10-29) (listed 2026-11-19: no catch-all, so a new hit is unclassified, never hidden) | 11 | com/legend/compiler/element/TypeClassifier.java:211 `as the engine prints` |
| M1 SENTENCE | the #SQL and #IQ islands' own words (Postgres's type and refusal vocabulary) | 9 | com/legend/protocol/sql/PostgresCasts.java `timestamp without time zone` |
| M7 OPT_IN | a ServiceLoader lookup: the declared opt-in mechanism (K2, the user's ruling 2026-11-18), or a product registry (connectors, grammars, probes) | 9 | com/legend/builtin/DecisionProbe.java:82 `ServiceLoader.load` |
| M1 NAME | a demo or verification script driving a real legend-engine for comparison (a test in all but its folder) | 8 | datacube/demo/chaos.mjs:46 `grammarToJson` |
| M1 EDGE | lite's own identity layout field (SqlExpr.SYNTHETIC_TYPE, ClassLayouts.SYNTHETIC_TYPE) | 7 | com/legend/sql/SqlExpr.java `__type` |
| M1 NAME | lite's own names (a catalog grid's pattern) | 6 | com/legend/compiler/element/TypedClass.java `qualifiedName` |
| M1 NAME | an internal alias or column name of lite's own lowering | 6 | com/legend/lowering/AsorReaders.java `pathToMapping` |
| M1 NAME | lite's own plan JSON field names | 6 | com/legend/executionplan/Connection.java `qualifiedName` |
| M2 NAME | the platform's ledger of upstream's bodies and machinery, named (the world-map tenet) | 6 | com/legend/platform/Implementation.java `ENGINE_MACHINERY` |
| M3 UNUSED_LAZY | a lazy input (a function value) the product's form need not call: ProductFormsUseTheirArgumentsTest's domain, ruled there | 6 | com/legend/lowering/ProductLoweringForms.java:60 `LoweringForms#relations(Supplier, Suppli` |
| M3 UNUSED_NOOP | the host forms' inputs are the judge's own values; the meaning is in the BRANCH rows [N-JUDGE] | 6 | com/legend/testexec/LiteHostForms.java:16 `HostForms#equalAcrossNumberKinds(Kind, K` |
| M1 EDGE | a verification or chaos script driving a real legend-engine (a test in all but its folder) | 5 | datacube/demo/chaos.mjs:46 `/api/pure/v1/` |
| M1 NAME | lite's own identity layout field (SqlExpr.SYNTHETIC_TYPE, ClassLayouts.SYNTHETIC_TYPE) | 4 | com/legend/compiler/element/ClassLayouts.java `__type` |
| M1 ROUTE | a verification or chaos script driving a real legend-engine (a test in all but its folder) | 3 | datacube/demo/chaos.mjs:46 `pure/v1/grammar/grammarToJson/lambda` |
| M2 NAME | a verification script driving a real legend-engine (a test in all but its folder) | 3 | datacube/demo/verify-calc-vocabulary.mjs:25 `LegendEngineExecutor` |
| M3 REFUSED | an executed value's envelope: the product refuses legend-engine's envelopes by name (RULED in ProductFormsUseTheirArgumentsTest, 2026-11-18) | 3 | com/legend/planner/FramePlanner.java:139 `ExecutedValues#values(TypedSpec, ModelCo` |
| M5 COMMENT | legend-studio's DataCube's behaviour, the app DataCube follows (not legend-engine's or legend-pure's) | 3 | datacube/src/app.ts:2770 `as upstream does` |
| M1 NAME | the JSON library's own words | 2 | com/legend/json/Json.java `decimalValue` |
| M1 NAME | lite's own MIR fields (__type: lite's identity layout; tablePattern: lite's catalog grid) | 2 | com/legend/sql/SqlExpr.java `__type` |
| M1 NAME | SQL's own vocabulary (the #SQL and #IQ islands) | 2 | com/legend/protocol/sql/SqlAst.java `withinGroup` |
| M1 NAME | the value of a platform function (toJSON's metaType), as upstream defines it (the upstream-fidelity tenet) | 2 | com/legend/lowering/JsonEmission.java `InvalidType` |
| M2 NAME | the platform's ledger of upstream's own machinery, named (the world-map tenet: residue named, never ported) | 2 | com/legend/builtin/Pure.java `ENGINE_VOCAB_SHIMS` |
| M2 NAME | lite's own system database engine | 2 | com/legend/planner/PlanMaker.java `systemEngine` |
| M2 NAME | the picker that loads the declared opt-in engine plane by URL (H11) | 2 | datacube/demo/planners.ts:36 `legendEnginePlanner` |
| M3 UNUSED | raise's kind and plain value: the product's own values, which the checked form wraps (GUARDS-4: plain is the bare value checked wraps, Scalars.java:3024) | 2 | com/legend/lowering/ProductLoweringForms.java:27 `LoweringForms#raise(RaiseKind, Supplier,` |
| M3 UNUSED_NOOP | the verdict numbers' inputs are the judge's own expressions; the meaning is in the BRANCH rows [N-JUDGE] | 2 | com/legend/lowering/LiteVerdictNumbers.java:24 `VerdictNumbers#floatsWithin(SqlExpr, Sql` |
| M5 COMMENT | legend-studio's query builder's behaviour, the app Query follows | 2 | query/src/builder/build.ts:7 `as upstream does` |
| M7 STATIC | a verification script (a test in all but its folder) that drives a real legend-engine for comparison | 2 | datacube/demo/verify-calc-vocabulary.mjs:25 `../../engine-client/src/engine-remote.ts` |
| M1 SENTENCE | the #Cypher island's own refusal | 1 | com/legend/protocol/cypher/CypherToPure.java `Not supported` |
| M1 SENTENCE | lite's own sentence for an assert whose message could not be evaluated (AssertMessages.unevaluated, 2026-11-04): it begins with legend-pure's two words, and is neither assert's default (PRODUCT-3, fixed) nor the six guards' (R10) | 1 | com/legend/compiler/spec/AssertMessages.java `Assert failed` |
| M1 EDGE | lite's own sentence for an assert whose message could not be evaluated (AssertMessages.unevaluated, 2026-11-04): it begins with legend-pure's two words, and is neither assert's default (PRODUCT-3, fixed) nor the six guards' (R10) | 1 | com/legend/compiler/spec/AssertMessages.java `Assert failed` |
| M1 NAME | a database's format name (BigQuery's) | 1 | com/legend/lowering/Scalars.java `ISO8601_NanoSecondPrecision` |
| M1 EDGE | the product server naming legend-engine's API to refuse it by name and point at --engine (K2's declared opt-in) | 1 | com/legend/server/LegendHttpServer.java `/api/pure/v1` |
| M1 NAME | the language server protocol's own field | 1 | com/legend/server/PureLspServer.java `serverInfo` |
| M1 NAME | legend-sdlc's API, which lite's SDLC server serves (its own program; outside legend-engine and legend-pure) | 1 | sdlc-client/src/client.ts:87 `currentUser` |
| M2 NAME | the declared opt-in's switch (--engine legend-engine; K2, the user's ruling 2026-11-18) | 1 | com/legend/server/LegendHttpServer.java `engineOf` |
| M2 NAME | the page setting naming the declared opt-in engine plane (H11) | 1 | datacube/demo/page-config.ts:15 `legendEngine` |
| M3 UNUSED | the plan style's facet lookup: refused by the product's style by name (the hook, HookRulingTest) | 1 | com/legend/database/PlanStyle.java:128 `PlanStyle#facet(Class, Supplier)` |
| M3 ANSWER | the trace cell is the product's bound execution id; legend-engine's comment is the edge's (D1.4) | 1 | com/legend/database/PlanStyle.java:135 `PlanStyle#traceCell()` |
| M3 UNUSED | wireCast's value: the product writes its own cast from the written supplier, the value is the edge's read; both the product's own expressions | 1 | com/legend/lowering/ProductLoweringForms.java:37 `LoweringForms#wireCast(SqlExpr, Supplier` |
| M3 ANSWER | a two-form kind's product answer: the seam itself; its branches are the BRANCH rows [N-JUDGE] | 1 | com/legend/lowering/LiteVerdictNumbers.java:19 `VerdictNumbers#floatTolerance()` |
| M3 UNUSED | the server's route seam (K2's opt-in mount, the user's ruling 2026-11-18): the product's route ignores what the edge's reads | 1 | com/legend/server/LegendHttpServer.java:431 `LegendHttpServer$Routes#answer(String, S` |
| M3 BRANCH | the MongoDB dialect's own date format handed to its JSON writer; the edge overrides the format (D1.2) | 1 | com/legend/sql/dialect/MongoJson.java:30 `com.legend.sql.dialect.MongoJson#string` |
| M3 BRANCH | the server's route seam (K2's opt-in mount, the user's ruling 2026-11-18) | 1 | com/legend/server/LegendHttpServer.java:371 `com.legend.server.LegendHttpServer$Modul` |
| M3 REFUSED | a dialect hook the edge's engine-text dialects implement (D1.7) | 1 | com/legend/sql/dialect/AnsiSqlRenderer.java:1054 `AnsiSqlRenderer#tagged(SqlWriter, Tagged` |
| M3 EDGE_LIVE | a door's output parameter: the product's test door, which the edge's service-test runner calls with its own sink | 1 | com.legend.TestExecution:56 `TestExecution#runProgram(TypedQuery, Str` |
| M4 TOSTRING | a function field's own toString, delegated: no print form | 1 | prelude:prelude.pure:4729 `meta::pure::router::metamodel::RoutingSt` |
| M5 COMMENT | describes the edge's words (edge.PureMessages.compile) | 1 | com/legend/Compiler.java:1054 `as legend-engine does` |
| M5 COMMENT | describes the edge's form: legend-engine's zone order is the edge's (L10) | 1 | com/legend/lowering/WhereZones.java:12 `legend-engine's order` |
| M5 COMMENT | legend-sdlc's API, which lite's SDLC server serves | 1 | sdlc-client/src/client.ts:272 `as upstream answers` |
| M7 DOOR | the declared opt-in module's entry (//wasm:engine_planner, TeaVM's main; _EDGE_OPT_IN_MODULES) | 1 | com/legend/edge/wasm/EngineExports.java:53 `com.legend.edge.wasm.EngineExports#main` |


## 7. The known items, cross-checked

The 16 open items, as the orchestrator corrected the list (2026-11-19):
1. C1
2. C2
3. LOWERSQL-7
4. COMPLETENESS-7 (ruled (b) by the user, 2026-11-19: the parser stays)
5. COMPLETENESS-8
6. C3-TS
7. C3-Py
8. HOSTS-4
9. HOSTS-7
10. R11a (PRODUCT-1, HOSTS-9, PH-2)
11. PRODUCT-10
12. J5
13. PRODUCT-11
14. PRODUCT-7
15. JUDGES-3 and JUDGES-6
16. R10

Then the items the census must also find, and class (b) as ruled: C5 (R5), C6 (R6), R2, R7 (COMPLETENESS-6 and
PRODUCT-8), and HOSTS-5. Then COMPLETENESS-5 (R4, pending), in its own class (p).

Then the negative checks, which the census must NOT find:
- HOSTS-3: Python's currentUser, now the edge's.
- PRODUCT-3's `assert` default: lite's sentence in the product, legend-pure's at `EdgeLoweringForms:71`.

The product's two "Assert failed" hits are accounted for:
- `LambdaBodies:142` is the six other guards' default: R10, (a).
- `AssertMessages.unevaluated` is lite's own sentence: (c).

Then this census's new items (N-*). N-PRELUDE and N-PARSER are ruled (b) with COMPLETENESS-7.

Every open item is found except three, and each says why: PRODUCT-11, PRODUCT-10 and J5, all lens limits (§9). Both
negatives hold.


| Item | What | Hits | Lenses | Sites (first 4) |
|---|---|---|---|---|
| C1 | legend-engine's union framing shape runs in product lowering (LOWERSQL-1, GUARDS-2) | 5 | M3 ANSWER, M3 BRANCH, M3 EDGE_LIVE, M5 COMMENT | com.legend.lowering.SubselectPrune:75 `SubselectPrune#prune(SqlQuery, boolean, `; com/legend/lowering/Fold.java:1661 `com.legend.lowering.Fold#tryProjectAll`; com/legend/lowering/Lowerer.java:3004 `(engine model)`; com/legend/lowering/Lowerer.java:3028 `com.legend.lowering.Lowerer#unionFrame` |
| C2 | the product planner computes legend-engine's exists-join rule (LOWERSQL-4) | 1 | M3 UNUSED | com/legend/lowering/ProductLoweringForms.java:91 `LoweringForms#existsJoin(SqlSelect, Exis` |
| C3-TS | TypeScript product records read legend-engine's shapes (COMPLETENESS-1) | 10 | M1 NAME, M6 EXPORT | engine-client/src/planned-sql.ts:28 `fromPlan`; engine-client/src/relation-type.ts:90 `tdsColumns`; engine-client/src/relation-type.ts:91 `resultType`; engine-client/src/relation-type.ts:91 `rootExecutionNode` |
| C3-Py | Python's product plan record reads relationalType (COMPLETENESS-4) | 1 | M1 NAME | python/legend_lite/_plans.py:29 `relationalType` |
| HOSTS-3 | Python's product engine serves /api/server/v1/currentUser (C4) | not found | closed on this branch at 8fe8c4cf8 (PR #118's N4): Python's currentUser is the edge's; M1's route lens bites a planted /api/server/v1/currentUser |  |
| HOSTS-4 | Python's product loader holds the engine library's entry table and imports legend_lite_engine (C4) | 3 | M6 EXPORT, M7 OPT_IN | python/legend_lite/_loader.py:209 `EngineMissing`; python/legend_lite/_loader.py:220 `legend_lite_engine`; python/legend_lite/_loader.py:231 `engine_loaded` |
| HOSTS-7 | two undeclared doors to legend-engine's routes: EdgeServer.main, Python's direct EngineRoutes() (R11(b)) | 1 | M7 DOOR | com/legend/edge/EdgeServer.java:93 `com.legend.edge.EdgeServer#main` |
| C5 | the prelude ships legend-pure's print bodies (Pair, List, SQLNull, TDSColumn) (COMPLETENESS-2, R5) | 4 | M4 TOSTRING | prelude:prelude.pure:20 `meta::pure::functions::collection::Pair`; prelude:prelude.pure:34 `meta::pure::functions::collection::List`; prelude:prelude.pure:3546 `meta::relational::metamodel::SQLNull`; prelude:prelude.pure:4950 `meta::pure::tds::TDSColumn` |
| R11a | the product store's legend-engine statuses and IllegalArgumentException text (PH-2, PRODUCT-1, HOSTS-9) | 4 | M1 EDGE, M1 SENTENCE, M5 COMMENT | com/legend/queries/SavedQueries.java `IllegalArgumentException`; com/legend/queries/SavedQueries.java `Unknown sort-by value`; com/legend/queries/SavedQueries.java:230 `as legend-engine answers`; com/legend/queries/SavedQueries.java:31 `as the engine does` |
| LOWERSQL-7 | H2's product dialect sets legend-engine's NON_KEYWORDS on every session | 4 | M2 NAME, M5 COMMENT | com/legend/sql/dialect/H2.java `ENGINE_LEXICON`; com/legend/sql/dialect/H2.java `FREED_BY_THE_ENGINE`; com/legend/sql/dialect/H2.java `freedByTheEngine`; com/legend/sql/dialect/H2.java:35 `replayed here reads as it did in the eng` |
| COMPLETENESS-7 | parser and protocol emitter reproduce legend-engine's span and emit quirks (ruled (b) by the user, 2026-11-19) | 35 | M2 NAME, M5 COMMENT | com/legend/parser/DatabaseProtocolParser.java:450 `QUIRK`; com/legend/parser/DatabaseProtocolParser.java:490 `quirk`; com/legend/parser/DatabaseProtocolParser.java:766 `QUIRK`; com/legend/parser/ElementParser.java:1425 `quirk` |
| COMPLETENESS-8 | product test order and lineage labels follow legend-engine (engineSuiteOrder, unionAlias, SQLNull) | 8 | M1 EDGE, M1 NAME, M2 NAME | com/legend/compiler/spec/LineageTreeLines.java `unionAlias`; com/legend/compiler/spec/LineageTreeLines.java `unionBase`; com/legend/lineage/ScanRelations.java `tdsJoin`; com/legend/lineage/ScanRelations.java `unionAlias` |
| PRODUCT-7 | edge-only product surface exempted on reasons that do not hold (LiteV1Api.Runner, testDataTimestampText, "pageContent") | 3 | M2 NAME | com/legend/planner/UpstreamPlans.java `UpstreamPlans`; com/legend/planner/UpstreamPlans.java `engineSeedSqlFromRecords`; com/legend/planner/UpstreamPlans.java `engineSeedSql` |
| PRODUCT-11 | readThroughExecuteValues: an edge-only flag carried through four product layers | not found | a record component branched on at Lowerer.java:829: M3 follows parameters and hook answers, never a record field's provenance (C2's existsJoinable has the same limit) |  |
| JUDGES-3 | host failure texts bypass HostWords, so the judges disagree on the message | 1 | M3 BRANCH | com/legend/testexec/IsolatedRun.java:154 `com.legend.testexec.IsolatedRun#differen` |
| JUDGES-6 | judgesEngineText() serves as an "is this the edge" flag | 13 | M2 NAME, M3 ANSWER, M3 BRANCH, M3 REFUSED | com/legend/database/PlanStyle.java `judgesEngineText`; com/legend/database/PlanStyle.java:117 `PlanStyle#judgesEngineText()`; com/legend/testplan/EngineTextGates.java `EngineTextGates`; com/legend/testplan/EngineTextGates.java `reachesUpstreamText` |
| C6 | DataCube's product queries work around legend-engine's defects on every planner (COMPLETENESS-3, R6) | 1 | M5 COMMENT | datacube/src/query.ts:319 `ENGINE GAP` |
| R10 | the six other in-body guards' default is legend-pure's "Assert failed" (LambdaBodies; PRODUCT-3's other half) | 2 | M1 EDGE, M1 SENTENCE | com/legend/compiler/spec/LambdaBodies.java `Assert failed` |
| PRODUCT-10 | the product's carried Float layout is legend-pure's; two product texts of one DateTime | not found | a layout and a digit count carry no upstream literal, and CellText's comment cites legend-pure as the spec the text matches, which M5 cannot tell from a copy (lite cites legend-pure everywhere as spec) |  |
| J5 | the store-read carrier spells a Decimal 1D, legend-pure's %r form (LiteralSpelling.decimalPrintD, the DECIMAL_TEXT carrier) | not found | the spelling is a one-letter SQL literal, "D", concatenated onto the value's text: below every text lens's length, with no upstream name and no reproduction comment |  |
| R2 | the test-data generator's legend-engine forms and words stay for now (ruled) | 22 | M1 NAME, M1 SENTENCE, M2 NAME, M3 EDGE_LIVE, M3 UNUSED, M3 UNUSED_NOOP, M5 COMMENT | com.legend.testdatagen.TestDataPlanner:142 `TestDataPlanner#plan(ModelContext, Lambd`; com.legend.testplan.GeneratedTestData:102 `TestDataPlanner#seed(ModelContext, Lambd`; com/legend/testdatagen/LitePrinting.java:53 `TestDataPlanner$Printing#headerOrder(Tab`; com/legend/testdatagen/LitePrinting.java:58 `TestDataPlanner$Printing#headerName(Tabl` |
| R7 | the grammar level and the level-less entries (COMPLETENESS-6, PRODUCT-8; ruled) | 4 | M2 NAME | com/legend/door/Parse.java `engineDocumentJson`; com/legend/door/Parse.java `engineLambdaJson`; com/legend/door/Parse.java `engineRecords`; com/legend/parser/GrammarLevel.java `LEGEND_ENGINE` |
| COMPLETENESS-5 | the product's default substring and indexOf are legend-engine's relational semantics (R4, its ruling pending) | 2 | M5 COMMENT | com/legend/lowering/Scalars.java:1490 `engine goldens`; com/legend/lowering/Scalars.java:1555 `the engine translates` |
| PRODUCT-3 | NEGATIVE: assert's own default is no longer legend-pure's in the product (fixed: Pure.Lite.ASSERT_FAILED_TEXT; the edge's EdgeLoweringForms) | not found | expected unseen: the product's "Assert failed" hits are the six other guards' (R10) and AssertMessages.unevaluated, lite's own sentence (c) |  |
| N-PRELUDE | the prelude ships upstream Pure bodies beyond C5's print forms (legend-engine's relational asserts, its plan warning, the PCT runner's prints) (ruled (b) by the user, 2026-11-19) | 235 | M1 EDGE, M1 SENTENCE, M4 BODY | prelude:prelude.pure:1017 `meta::pure::functions::meta::_subTypeOf`; prelude:prelude.pure:1053 `meta::pure::functions::meta::class`; prelude:prelude.pure:1121 `meta::pure::functions::meta::properties`; prelude:prelude.pure:1190 `meta::pure::functions::meta::functionTyp` |
| N-PARSER | the product parser and its section grammars hold legend-engine's grammar refusals in upstream's words (ruled (b) by the user, 2026-11-19) | 73 | M1 SENTENCE | com/legend/model/AssociationDefinition.java `Property name cannot be null`; com/legend/model/ClassDefinition.java `Property name cannot be null`; com/legend/model/ClassMapping.java `Property name cannot be null`; com/legend/model/FromProtocol.java `Expected 2 properties for an association` |
| N-JUDGE | the product judges compute and branch for compiled legend-pure's comparison (tolerances, canons, rendered texts, message operands) | 47 | M3 ANSWER, M3 BRANCH, M3 REFUSED, M3 UNUSED, M3 UNUSED_NOOP, M5 COMMENT | com/legend/compiler/spec/VerdictQueries.java:795 `com.legend.compiler.spec.VerdictQueries#`; com/legend/compiler/spec/VerdictQueries.java:977 `com.legend.compiler.spec.VerdictQueries#`; com/legend/lowering/LiteVerdictNumbers.java:19 `VerdictNumbers#floatTolerance()`; com/legend/lowering/LiteVerdictNumbers.java:24 `VerdictNumbers#floatsWithin(SqlExpr, Sql` |
| N-ROOTFORM | RelationalRootForm: legend-engine's toSQLString root form in the product resolver, its mapping argument only the edge passes | 1 | M3 EDGE_LIVE | com.legend.resolver.RelationalRootForm:45 `RelationalRootForm#apply(List, ModelCont` |
| HOSTS-5 | the product wheel ships the edge's Python; the DataCube product bundles list the edge entry as an input | 5 | M7 BUILD | //datacube:bundle_bundle `//datacube:bundle_bundle`; //datacube:bundle_bundle_engine `//datacube:bundle_bundle_engine`; //datacube:bundle_remote_bundle `//datacube:bundle_remote_bundle`; //datacube:bundle_stress `//datacube:bundle_stress` |


Partial finds, stated:
- **HOSTS-7**: `EdgeServer.main` (M7 DOOR). Python's direct `EngineRoutes()` composition is not seen (§9).
- **PRODUCT-7**: the `UpstreamPlans` facet (M2). `LiteV1Api.Runner` and the `"pageContent"` operand are not seen: lite
  names, no upstream spelling.
- **JUDGES-3**: one of its four sites (the rendered-text branch, M3).
- **C6**: the ENGINE GAP comment (M5). `offers.ts`'s operator withholding is not seen.
- **C2**: its seam (M3 UNUSED). The `existsJoinable` field is a field (§9).
- **COMPLETENESS-5**: by its two comments (M5), not by its semantics. A default that differs from Pure's, with no
  upstream spelling, is unseen by every lens (§9).


## 8. Main against #118's head

The orchestrator asked for the census at main (`4dfbb5b76`) and at #118's head (`cf763c815`), both detached and measured
only, and for the (a) hits per group: new in #118, removed by #118, and on both.

**How.** The same census code, vocabulary and inventory ran at both. The head's run is this branch, whose product equals
`cf763c815`. Main's run used a worktree with the census copied in. Only the input wiring differs: main has no
`//core:edge_jars`, no host edge targets and no opt-ins, so its run reads `//core:edge` (6 classes). Its floor for
"edge classes" was relaxed to 1 in the worktree.

Hits are compared by (lens, file, term), the line left out. M1's category is left out too. Which matcher fires (NAME,
SENTENCE, EDGE) depends on the edge's size, so one string is counted once per side.

**Main's classes.** The inventory is written for the head's files, so main needed two steps:
- **316 main hits had no row**, in files #118 moved or rewrote. They were classed by the head's own rules (`MOVED: ...`
  below): a route, a plan name, a message or template, a self-named member, a print body, a reproduction comment, each
  (a). The one exception is `Dialect.LEGEND_ENGINE`, the R7 grammar level under its old name, which is (b).
- **48 main hits were classed (c) by head-written package rows, and their keys are gone at the head.** They were
  re-read, and 28 became (a):
  - 22 in files that left the product (mostly `EngineStyleH2` and `EngineStyleDB2`, which sat in the product's dialect
    package on main, where the dialect row calls their text "the database's own");
  - 6 read one by one: legend-pure's cast message in `AnsiSqlRenderer`, legend-engine's window-frame message in
    `FrameGuard`, and legend-engine's `unionAlias` in three lowering files [COMPLETENESS-8].
  - The other 20 stay (c).

**The catch-alls.** The first comparison found four catch-all rows hiding main's hits:
- M1 SENTENCE `* | *` hid 84 upstream sentences, PRODUCT-3's four "Assert failed" judge sites among them;
- M1 NAME `* | *` hid 88 of legend-engine's plan names;
- M1 EDGE `* | *`;
- the M5 fidelity row.

Each is now an explicit list (§0), and the head's classes did not change.

**A structural limit.** M3 (forms) and M6 (edge-only exports) need an edge implementation or an edge importer. Main's
edge has neither, so main has 0 M3 and 0 M6 hits by construction. A head M3 or M6 hit reads as "new in #118", but it is
mostly meaning main held inline, which #118's seams made visible.

**The user's ruling of 2026-11-19** ("we're not going to change prelude or parser") moved N-PRELUDE, N-PARSER and
COMPLETENESS-7 to (b) after the first comparison was sent. The table is under the ruling. Before it, the totals were:
(a) at main 800, at the head 484, new 79, removed 395, on both 405.

### 8.1 (a) per group

| group | (a) at main | (a) at head | new in #118 | removed by #118 | on both |
|---|---:|---:|---:|---:|---:|
| MOVED: an upstream message or plan template, in its words, in a product file | 86 | 0 | 0 | 86 | 0 |
| MOVED: a product member named for upstream (the edge's facet) | 84 | 0 | 0 | 84 | 0 |
| MOVED: legend-engine's protocol or plan name in a product file | 84 | 0 | 0 | 84 | 0 |
| MOVED: legend-engine's API route in a product file | 48 | 0 | 0 | 48 | 0 |
| legend-engine's calendar schema (LegendCalendarSchema.<type>_Calendar and its fiscal colum | 23 | 23 | 0 | 0 | 23 |
| N-JUDGE | 2 | 38 | 36 | 0 | 2 |
| R11a | 24 | 4 | 2 | 22 | 2 |
| MOVED: a (c) package row's hit in a file that left the product | 22 | 0 | 0 | 22 | 0 |
| C3-TS | 8 | 10 | 2 | 0 | 8 |
| JUDGES-6 | 0 | 13 | 13 | 0 | 0 |
| MOVED: a product comment declaring an upstream reproduction | 12 | 0 | 0 | 12 | 0 |
| legend-engine's test-data setup messages, in its words, in the product's seeding (R2 rules | 5 | 5 | 0 | 0 | 5 |
| LOWERSQL-7 | 5 | 4 | 0 | 1 | 4 |
| legend-pure's duration-unit message, in its words, in product lowering (K1's family) | 7 | 2 | 0 | 5 | 2 |
| COMPLETENESS-8 | 5 | 2 | 0 | 3 | 2 |
| legend-engine's mapping message, in its words, in the product normalizer (J10's rule) | 6 | 1 | 0 | 5 | 1 |
| C1 | 1 | 5 | 4 | 0 | 1 |
| legend-engine's aggregation-aware groupBy assertions, in its words, in the product typer | 3 | 3 | 0 | 0 | 3 |
| legend-pure's date-format parse messages, in its words, in product lowering (K1's family;  | 3 | 3 | 0 | 0 | 3 |
| a product helper only the Python edge uses (HOSTS-15: the edge reaches into product privat | 0 | 5 | 5 | 0 | 0 |
| legend-pure's compile message for a missing required property, in its words, in the produc | 2 | 2 | 0 | 0 | 2 |
| legend-pure's constraint-violation message, in its words, raised by the product | 2 | 2 | 0 | 0 | 2 |
| HOSTS-4 | 0 | 3 | 3 | 0 | 0 |
| PRODUCT-7 | 0 | 3 | 3 | 0 | 0 |
| C3-Py | 1 | 1 | 0 | 0 | 1 |
| R10 | 1 | 1 | 0 | 0 | 1 |
| RE-READ: legend-pure's cast message, in its words, in the product renderer | 2 | 0 | 0 | 2 | 0 |
| a generated column's presented name, as upstream prints it, in the product MIR (a presenta | 1 | 1 | 0 | 0 | 1 |
| legend-engine's CTE names (subquery_cte_<level>_<index>) for a model's declared post-proce | 1 | 1 | 0 | 0 | 1 |
| C2 | 0 | 1 | 1 | 0 | 0 |
| HOSTS-7 | 0 | 1 | 1 | 0 | 0 |
| JUDGES-3 | 0 | 1 | 1 | 0 | 0 |
| N-ROOTFORM | 0 | 1 | 1 | 0 | 0 |
| MOVED: a legend-pure print body the prelude ships (C5's family) | 1 | 0 | 0 | 1 | 0 |
| RE-READ: legend-engine's window-frame message, in its words, in the product dialect | 1 | 0 | 0 | 1 | 0 |
| SQLite's Decimal arithmetic SQL in the product dialect, reached only under the edge's text | 0 | 1 | 1 | 0 | 0 |
| a DateTime literal's written text, handed to the product's text form only the edge's legen | 0 | 1 | 1 | 0 | 0 |
| a structure cell's type name, handed to the product writer only the edge's presenter reads | 0 | 1 | 1 | 0 | 0 |
| the WHERE zone stamp the product computes for legend-engine's zone order (L10's order is t | 0 | 1 | 1 | 0 | 0 |
| view's name argument, computed for the edge's frame alone (the product writes the subselec | 0 | 1 | 1 | 0 | 0 |
| **all (a)** | **440** | **141** | **77** | **376** | **64** |

COMPLETENESS-5 (p) is on both, 2 and 2. The head's (a) is 141 here and 142 in the census, because one string (R10's
`LambdaBodies`) fires two matchers.

### 8.2 The 77 (a) hits new in #118

By why:
- 40 sit in files new in #118: the product halves of its seams and facets (`ProductLoweringForms`,
  `LiteVerdictWords`, `LiteHostWords`, `PlanStyle`, `EngineTextGates`, `UpstreamPlans`, `IsolatedRun`, `Styled`,
  `EdgeServer`).
- 35 sit in files on main, where only the seam or edge the lens reads is #118's (M3, M6, M7).
- 1 is text on main too, seen now by M1 EDGE through the edge's strings: `SavedQueries`'
  `IllegalArgumentException`.
- 1 is new text: `SavedQueries.java:230`, the comment "as legend-engine answers" [R11a].

- **N-JUDGE** (36)
  - M3 BRANCH `com/legend/compiler/spec/VerdictQueries.java:795`: `com.legend.compiler.spec.VerdictQueries#parseRendered`
  - M3 BRANCH `com/legend/compiler/spec/VerdictQueries.java:977`: `com.legend.compiler.spec.VerdictQueries#tdsCellText`
  - M3 UNUSED `com/legend/lowering/LiteVerdictWords.java:111` x7: `VerdictWords#notEquivalent(String, SqlExpr, SqlExpr, SqlExpr, SqlExpr, SqlExpr, SqlExpr, S`
  - M3 UNUSED `com/legend/lowering/LiteVerdictWords.java:82`: `VerdictWords#notInstanceOf(SqlExpr, boolean, String, String)`
  - M3 UNUSED `com/legend/lowering/LiteVerdictWords.java:89`: `VerdictWords#elementText(Type, SqlExpr, Styled)`
  - M3 BRANCH `com/legend/lowering/VerdictSql.java:1286`: `com.legend.lowering.VerdictSql#gridPair`
  - M3 BRANCH `com/legend/lowering/VerdictSql.java:234`: `com.legend.lowering.VerdictSql#equality`
  - M3 REFUSED `com/legend/lowering/VerdictSql.java:3067`: `VerdictNumbers#floatsWithin(SqlExpr, SqlExpr)`
  - M3 BRANCH `com/legend/lowering/VerdictSql.java:343`: `com.legend.lowering.VerdictSql#gridRows`
  - M3 BRANCH `com/legend/lowering/VerdictSql.java:375`: `com.legend.lowering.VerdictSql#gridCells`
  - M3 BRANCH `com/legend/lowering/VerdictSql.java:574`: `com.legend.lowering.VerdictSql#contains`
  - M3 BRANCH `com/legend/lowering/VerdictSql.java:698`: `com.legend.lowering.VerdictSql#perElement`
  - M3 REFUSED `com/legend/lowering/VerdictSql.java:701`: `VerdictNumbers#floatsWithin(SqlExpr, SqlExpr)`
  - M3 BRANCH `com/legend/lowering/VerdictSql.java:895`: `com.legend.lowering.VerdictSql#gridTolerance`
  - M3 BRANCH `com/legend/testexec/Equality.java:206`: `com.legend.testexec.Equality#same`
  - M3 BRANCH `com/legend/testexec/Equality.java:234`: `com.legend.testexec.Equality#ordered`
  - M3 BRANCH `com/legend/testexec/Equality.java:251`: `com.legend.testexec.Equality#contains`
  - M3 BRANCH `com/legend/testexec/HostJudgments.java:571`: `com.legend.testexec.HostJudgments#eqOf`
  - M3 BRANCH `com/legend/testexec/HostJudgments.java:890`: `com.legend.testexec.HostJudgments#renderedOf`
  - M3 UNUSED `com/legend/testexec/LiteHostWords.java:151`: `HostWords#notInstanceOf(Object, String, String)`
  - M3 UNUSED `com/legend/testexec/LiteHostWords.java:187`: `HostWords#jsonDiffers(String, String, boolean, String)`
  - M3 UNUSED `com/legend/testexec/LiteHostWords.java:222`: `HostWords#caught(RuntimeException, EdgeHearing)`
  - M3 UNUSED `com/legend/testexec/LiteHostWords.java:39` x2: `HostWords#rowValuesDiffer(List, List, List, List, boolean)`
  - M3 UNUSED `com/legend/testexec/LiteHostWords.java:58` x2: `HostWords#cellMultisetDiffers(String, List, List, HostRules)`
  - M3 UNUSED `com/legend/testexec/LiteHostWords.java:64`: `HostWords#notEquivalent(Tabular, Tabular, double, String)`
  - M3 BRANCH `com/legend/testplan/AssertVerdicts.java:1872`: `com.legend.testplan.AssertVerdicts#textFirst`
  - M3 BRANCH `com/legend/testplan/DatabaseJudge.java:337`: `com.legend.testplan.DatabaseJudge#lite`
  - M3 BRANCH `com/legend/testplan/DatabaseJudge.java:367`: `com.legend.testplan.DatabaseJudge#crossNumberKinds`
- **R11a** (2)
  - M1 EDGE `com/legend/queries/SavedQueries.java`: `IllegalArgumentException`
  - M5 COMMENT `core/src/main/java/com/legend/queries/SavedQueries.java:230`: `as legend-engine answers`
- **C3-TS** (2)
  - M6 EXPORT `engine-client/src/planned-sql.ts:28`: `fromPlan`
  - M6 EXPORT `engine-client/src/values.ts:112`: `timestampFromText`
- **JUDGES-6** (13)
  - M2 NAME `com/legend/database/PlanStyle.java` x2: `judgesEngineText`
  - M3 ANSWER `com/legend/database/PlanStyle.java:117`: `PlanStyle#judgesEngineText()`
  - M2 NAME `com/legend/testplan/EngineTextGates.java`: `EngineTextGates`
  - M2 NAME `com/legend/testplan/EngineTextGates.java`: `reachesUpstreamText`
  - M3 BRANCH `com/legend/testplan/EngineTextGates.java:101`: `com.legend.testplan.EngineTextGates#siteWords`
  - M3 REFUSED `com/legend/testplan/EngineTextGates.java:101`: `PlanStyle#facet(Class)`
  - M3 BRANCH `com/legend/testplan/EngineTextGates.java:107`: `com.legend.testplan.EngineTextGates#sizeWords`
  - M3 REFUSED `com/legend/testplan/EngineTextGates.java:107`: `PlanStyle#facet(Class)`
  - M3 BRANCH `com/legend/testplan/EngineTextGates.java:26`: `com.legend.testplan.EngineTextGates#facet`
  - M3 REFUSED `com/legend/testplan/EngineTextGates.java:26`: `PlanStyle#facet(Class)`
  - M3 BRANCH `com/legend/testplan/EngineTextGates.java:94`: `com.legend.testplan.EngineTextGates#planningCall`
  - M3 REFUSED `com/legend/testplan/EngineTextGates.java:94`: `PlanStyle#facet(Class)`
- **C1** (4)
  - M3 EDGE_LIVE `com.legend.lowering.SubselectPrune:75`: `SubselectPrune#prune(SqlQuery, boolean, String)`
  - M3 BRANCH `com/legend/lowering/Fold.java:1661`: `com.legend.lowering.Fold#tryProjectAll`
  - M3 BRANCH `com/legend/lowering/Lowerer.java:3028`: `com.legend.lowering.Lowerer#unionFrame`
  - M3 ANSWER `com/legend/lowering/ProductLoweringForms.java:112`: `LoweringForms#unionFrame()`
- **a product helper only the Python edge uses (HOSTS-15: the edge reaches** (5)
  - M6 EXPORT `python/legend_lite/_home.py:100`: `locked`
  - M6 EXPORT `python/legend_lite/_reads.py:292`: `run_arrow`
  - M6 EXPORT `python/legend_lite/_reads.py:299`: `run_rows_json`
  - M6 EXPORT `python/legend_lite/engine.py:393`: `_under`
  - M6 EXPORT `python/legend_lite/engine.py:403`: `_stored`
- **HOSTS-4** (3)
  - M6 EXPORT `python/legend_lite/_loader.py:209`: `EngineMissing`
  - M7 OPT_IN `python/legend_lite/_loader.py:220`: `legend_lite_engine`
  - M6 EXPORT `python/legend_lite/_loader.py:231`: `engine_loaded`
- **PRODUCT-7** (3)
  - M2 NAME `com/legend/planner/UpstreamPlans.java`: `UpstreamPlans`
  - M2 NAME `com/legend/planner/UpstreamPlans.java`: `engineSeedSql`
  - M2 NAME `com/legend/planner/UpstreamPlans.java`: `engineSeedSqlFromRecords`
- **C2** (1)
  - M3 UNUSED `com/legend/lowering/ProductLoweringForms.java:91`: `LoweringForms#existsJoin(SqlSelect, ExistsSites)`
- **HOSTS-7** (1)
  - M7 DOOR `com/legend/edge/EdgeServer.java:93`: `com.legend.edge.EdgeServer#main`
- **JUDGES-3** (1)
  - M3 BRANCH `com/legend/testexec/IsolatedRun.java:154`: `com.legend.testexec.IsolatedRun#difference`
- **N-ROOTFORM** (1)
  - M3 EDGE_LIVE `com.legend.resolver.RelationalRootForm:45`: `RelationalRootForm#apply(List, ModelContext, String)`
- **SQLite's Decimal arithmetic SQL in the product dialect, reached only u** (1)
  - M3 BRANCH `com/legend/sql/dialect/SQLite.java:1597`: `com.legend.sql.dialect.SQLite#call`
- **a DateTime literal's written text, handed to the product's text form o** (1)
  - M3 UNUSED `com/legend/lowering/Styled.java:34`: `DateTimeTexts#dateTimeText(SqlExpr, String)`
- **a structure cell's type name, handed to the product writer only the ed** (1)
  - M3 UNUSED `com/legend/result/CellText.java:70`: `CellText$Forms#structure(Object, String, Column)`
- **the WHERE zone stamp the product computes for legend-engine's zone ord** (1)
  - M3 UNUSED `com/legend/lowering/ProductLoweringForms.java:128`: `WhereZones#merge(SqlExpr, SqlExpr, Stamp)`
- **view's name argument, computed for the edge's frame alone (the product** (1)
  - M3 UNUSED `com/legend/lowering/ProductLoweringForms.java:32`: `LoweringForms#view(SqlSelect, Supplier, String)`


## 9. What the census cannot see (its limits, stated)

- **A field's provenance.** M3 follows parameters, locals and returns. A record component or an object field carrying an
  upstream decision is unseen: PRODUCT-11's `readThroughExecuteValues`, and C2's `existsJoinable`, which is found only
  through its seam's UNUSED row. A lens that would see it: a product branch whose two arms build the same MIR, or a
  record component only edge readers branch on.
- **Lite's own words where they should be a word family's.** JUDGES-3's sites are lite's own sentences, not upstream's,
  so no text lens can see them; the census finds one of the four (the rendered-text branch). A structural lens would see
  them: a judge's failure text built outside its words hook.
- **A copy that names nothing.** M2 and M5 rest on lite's convention of citing upstream where it copies it, and M1 on
  the derived vocabulary. A copied algorithm with neither (a SQL shape lowered with no upstream literal and no comment)
  is seen only by M3, and only when it sits behind a seam.
- **Python's direct `EngineRoutes()` composition** (HOSTS-7's Python half) and **DataCube's operator withholding**
  (`offers.ts`, C6's other half): both are behaviour in product code with no upstream spelling.
- **A layout, a digit count or a one-letter suffix.** PRODUCT-10 is unseen: the product's carried Float layout and its
  two texts of one DateTime have no upstream literal. `CellText`'s comment cites legend-pure as the spec the text
  matches, and lite cites legend-pure everywhere as spec, so M5 cannot tell it from a copy. J5 is unseen: the
  store-read carrier spells a Decimal `1D` (`LiteralSpelling.decimalPrintD`, the DECIMAL_TEXT carrier) by a one-letter
  SQL literal, `"D"`, below every text lens's length. A lens that would see both: a product `Reason.PRINT` expression
  whose constants spell a form.
- **A semantic default.** COMPLETENESS-5 is found by its two comments, not by what the code computes. Pure's
  `substring` and `indexOf` against the product's relational ones have no upstream spelling, and the flag that corrects
  `substring` is upstream's language enum (`Feature.CORRECT_SQL_SUBSTRING_INDEXING`), which the census leaves out as
  language. A default that differs from Pure's and says nothing is unseen.
- **The inventory is the head's judgement.** A package row ("the database's own function names") is true of the code it
  was written for. Another tree's code under the same package can be mis-classed: on main, the EngineStyle dialects'
  text. The comparison (§8) re-read those 48 hits. A row that would hide a new hit is a catch-all, and there is none.
- **The language/execution line is mechanical.** The surface comes from where a file sits in upstream's tree. Where the
  rule misreads a module, a name is dropped as language or kept as execution wrongly. The (c) rows say why each kept
  one is noise.
- **Cost.** About 12 s in the checks lane, beside the vocabulary action (about 5 s over the pinned trees, cached).

## 10. Making it blocking (later, after B3)

Three things, in order:
1. B3 closes or rules the (a) items.
2. Each (a) and (p) row of `inventory.tsv` that remains becomes a ruled (b) row, citing the user's ruling.
3. `MeaningCensusTest.BLOCKING` becomes `true`. A new (a) hit, or an unclassified one, then fails the build.

A classification row is then a guard entry: AGENTS.md's rule applies, and a row moves only with a dated note. Until
then the rows are this census's judgement, report-only.

## 11. Files

- New: `tools/meaning/{BUILD.bazel, HostSources.java, ClassFiles.java, Flow.java, FormsLens.java, MeaningCensus.java,
  VocabularyGenerator.java, inventory.tsv}`, `tools/meaning/planted/**`, `tools/guards/MeaningCensusTest.java`, this
  record and its TSV (the head's run). The main comparison (§8) was a measurement in a scratch worktree, and nothing
  of it is committed beyond this record.
- Changed: `tools/guards/BUILD.bazel` (the test, its jar list, its source list); `gates/BUILD.bazel` (the test in
  `checks`); visibility only, on `//core:product_jars`, `//core:edge_jars`, `//native:entry`, `//native:edge_entry`,
  `//wasm:boundary`, `//wasm:tab`, `//wasm:edge_boundary`, `//wasm:edge_tab`, `//query-store:page_entry` and
  `//python:edge`.
- **No pin, ceiling or allowlist moved.** The existing census, guardrails, ShippedArtifactRootsTest and their lists are
  untouched; K2's new queries are this census's own.
