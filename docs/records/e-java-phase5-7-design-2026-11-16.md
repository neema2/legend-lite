# E-Java: Phase 5 (the kind-B items as legend-engine's SQL) and Phase 7's build half -- design note (2026-11-16)

The edge program (`docs/records/total-wall-edge-program-2026-11-16.md`, branch `claude/total-wall-orch`), rebalanced to
E-Java at 19:10. **Not built yet: this note is for the orchestrator's overlap check first.** Each phase is built on
its own branch, off `claude/total-wall-d-stage`, for D to merge.

## Phase 5: the kind-B rows

### 1. The rows, legend-engine's SQL, and the moves

Upstream was read from legend-engine 4.145.0's pinned Pure sources: `extensionDefaults.pure`, `duckdbExtension.pure`,
`h2Extension2_1_214.pure`.

| § 6 row | Item | legend-engine's SQL (read) | Lite today (edge) | PCT moves measured | Owner |
|---|---|---|---|---|---|
| 8 | a Float's `toString` inside a query | DuckDB and H2: `dynaFnToSql('toString', ..., format='cast(%s as varchar)')`. A Float **literal** is written by the default literal processor, `pair(Float, ^LiteralProcessor(format = '%s', transform = toString_Any_1__String_1_->literalTransform()))`: legend-pure's own print, bare, so `0.000000013421` reaches the database as an exact numeric literal. DuckDB reads it as a DECIMAL, and its cast writes `'0.000000013421'`. A Float **column** is a DOUBLE, cast as the database casts it. | `PurePrintForms` rewrites the product's `FLOAT_TO_TEXT` into `FLOAT_PRINT`, legend-pure's print spelled in SQL. D's WIP `05775e1eb` made it a plain `CAST(x AS VARCHAR)`, but the literal still reaches the database as a DOUBLE. | Round 67, on D's WIP: ONE move, `testFloatToStringWithNegativeExponent` (`'1.3421e-08'` where `'0.000000013421'` is expected), on DuckDB, SQLite, MariaDB, MySQL, Trino, Databricks and BigQuery. legend-engine passes it on every relational database. **Target: 0 moves.** | E-Java |
| 9 | a raise sentence's operand | upstream's sentence | -- | -- | D, Phase 2 (the marker plus Java words). Not mine. |
| 10 | ClickHouse's product `format('%s', Float)` | -- | -- | -- | Phase 3, a product fix (D). Not mine. |
| 11 | H2's quoting of reserved words | -- | restored `9c28a1a7e` | -- | done |
| 12 | DuckDB's engine scan order | -- | `StableScanOrder` | -- | done |

Follow-ups the plan files under "D, during Phase 5", which I take with Phase 5:
- **F5:** legend-engine's answer for an unused derived column that can raise. I'll ask the engine runner; the edge follows it.
- **F11:** "Missing external parameter(s)". The edge joins with `", "`, upstream with `","`. A one-character Java fix in the edge's words.
- **F12:** the main-table sentence, made upstream's exact text (`RelationalCompilerExtension:350`, to be read).

### Row 8's build

Under the edge's style, a Float literal under a `toString` is written as legend-engine writes it: legend-pure's print
of the value (`CellText.pureFloat`), as a bare exact-numeric literal, `SqlExpr.DecimalLit` of that text. Every other
Float under `toString` is `CAST(x AS VARCHAR)`, which is D's WIP.

`FLOAT_PRINT` then has no edge user in a query. Phase 3's presenter owns a Float *value's* text; my Phase 4 already
removed the judges' uses.

**Scope question, decided by measurement:** should EVERY Float literal under the edge be written bare (legend-engine
writes all of them that way), or only one under a `toString`?
- Writing all of them changes arithmetic typing on DuckDB, a DECIMAL where lite had a DOUBLE, and may move other tests.
- So I measure both on DuckDB, H2 and SQLite, PCT A and B.
- If the narrow form gives 0 moves and the wide one moves anything, the narrow form lands, and the wide form's moves go
  to you as ONE batch.

### 2. Files (Phase 5)
- `core/src/main/java/com/legend/edge/PurePrintForms.java`: the `FLOAT_TO_TEXT` arm.
- Possibly `core/src/main/java/com/legend/edge/PureText.java` / `EngineStyleH2.java`: an edge render of a Float literal, only if
  the wide form wins.
- `core/src/main/java/com/legend/edge/PureMessages.java`: F11 and F12, words only. **Overlap with D's Phase 2**, which moves
  `PureMessages.sentence` into Java. I'll touch only the two sentences and rebase onto Phase 2 if it lands first.
- Tests: `core/src/test/.../integration/FloatTextTest.java`, plus one new edge test pinning
  `0.000000013421->toString()` on DuckDB, H2 and SQLite.
- PCT rosters/ledgers only if a ruled move needs it, with the ruling cited.

### 3. Contracts
- **From D's Phase 2:** nothing for row 8. F11 and F12 share `PureMessages.java`, so whoever lands second rebases.
- **From D's Phase 3 (the presenter):** it owns `FLOAT_PRINT`'s deletion. After row 8, `FLOAT_PRINT` remains only where the presenter
  replaces it. I won't delete `SqlFn.FLOAT_PRINT` myself.
- **E-SQL Phase 8 (the carriers):** row 20's shortest-text emulations (`floatText` on Snowflake, BigQuery, SQLite, MariaDB, plus
  ClickHouse's regexp). Row 8's `CAST(x AS VARCHAR)` reaches them on those databases, so their moves today are against
  lite's emulation, not legend-engine's raw cast. **I need Phase 8 to keep a Float's `CAST AS VARCHAR` under the edge as
  the database's raw cast**, or I measure against whatever it leaves.
- **E-SQL Phase 6 (plan text):** none. Row 8 changes the edge's SQL text, so the engine-text goldens that quote it may
  move. The golden net catches that.
- **Others need from me:** that the edge's `FLOAT_TO_TEXT` arm is settled before Phase 3 deletes `FLOAT_PRINT`.

### 4. Risks, and what I measure before building
- **The narrow form against the wide one:** the PCT A/B moves on DuckDB, H2 and SQLite for each, plus the golden net.
- **SQLite:** the pinned release's relational extensions may not include SQLite (to check). If legend-engine has no
  SQLite, "legend-engine's SQL" there means the closest, and that row comes to you.
- **Pure's print of an extreme value** (1e300, 1.5e-30): `toString_Any_1` prints an exponent there, so the bare literal is an
  exponent literal, which DuckDB reads as a DOUBLE. That's legend-engine's behaviour too, so no move is expected; I'll check one.

## Phase 7's build half: product targets and tests without the edge

### Today (`claude/total-wall-d-stage`, read)
- No product LIBRARY reaches `:edge`. Only `edge_server` and `edge_service_tests` do (`tools/deps/core-layers.txt`).
- The product's TEST library `core_tests_lib` depends on `:edge` and `:edge_server` (core/BUILD.bazel ~line 725), so
  every `core_tests_<package>` links the edge.
- 65 test files under `core/src/test` use an edge class:
  - the guards (`ArchitectureTest`, `CompatSpellingGuardTest`, `DialectBoundaryTest`, `PlanStyleFacetsTest`, ...);
  - 18 in `integration/` (`FloatTextTest`, `EdgeSpellingHookTest`, `PureOnlyTextTest`, ...);
  - 10 in `server/`, mostly `PureV1*`;
  - 15 in `sql/dialect/`, mostly the engine-style dialects;
  - `testing/Program.upstream()`, the opt-in to `PureText.UPSTREAM`, which about 6 sites call.
- The harness's default is the product's style.

### The targets
- `core_tests_lib` loses `:edge` and `:edge_server`.
- A new `core_edge_tests_lib` holds the edge-using test files, by an explicit list in core/BUILD.bazel, `EDGE_TEST_SRCS`. It depends on
  `core_tests_lib`, `:edge` and `:edge_server`, and runs as `core_edge_tests`, in `//gates:local` like the others.
- `testing/Program.upstream()` moves to an edge test helper, `testing/EdgeProgram`, in the edge test library.
- The guards that NAME edge classes as strings stay put. Those that LOAD edge classes for ArchUnit import the class files
  (`ArchitectureTest` imports by package) and move only if Bazel requires it. ArchUnit reads the classpath, so the
  guards' target gets `:edge` as a runtime dep, which is a guard, not a product test. That is the one exception, named.

### The rule
`ArchitectureTest.productTestsReachNoEdge`:
- No class in `com.legend..` test sources outside `EDGE_TEST_SRCS`' classes, and outside the guards, depends on
  `com.legend.edge..`.
- The list is read from one resource the BUILD file also reads, so the two cannot drift.
- A planted violator, `PlantedProductTestEdgeReach`, must be caught. It follows the existing planted-violator pattern
  (`PlantedEdgeReach`).

Bazel enforces it as well: a product test file that imports the edge fails to compile.

### Files (Phase 7)
- `core/BUILD.bazel`: the split.
- `tools/deps/core-layers.txt`: only if the test library's line is held there.
- `core/src/test/java/com/legend/testing/Program.java` and a new `EdgeProgram.java`.
- `ArchitectureTest.java`: the rule, plus the planted violator.
- No main sources. **Overlap:** D's Phase 7 guard review (`CompatSpellingGuardTest`) is a different file. `ArchitectureTest` is
  shared with every phase; I add one rule and one planted class only.

### Risks
- A product test that today passes only under `upstream()`, through a transitive helper, would fail to compile when the edge
  goes. Each one either moves to the edge library (it is an edge test) or stops needing the edge. I list them before
  moving anything.
- CI targets that name `core_tests_lib` (`//gates:local`, `.github` workflows) must gain `core_edge_tests`, so nothing stops running.
  I'll count the tests run before and after: the sum must match.
