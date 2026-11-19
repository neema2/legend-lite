# The plan oracle: legend-engine plans and runs the pure/v1 golden requests (slice R, 2026-11-18)

Slice R of the presentation program (phase 2's runner tooling; the design is
`docs/records/presentation-program-design-2026-11-18.md` on `claude/presentation-design`, §1 items 9 and 10, §3.9,
§3.10, §5 and §8). It extends the engine runner so that legend-engine 4.145.0 can be asked, as test tooling, for its
plan of a pure/v1 request and for its answer to a given plan. This record says what was built, how to run it, and how
each pure/v1 golden differs from legend-engine's answer, with the reason.

Every legend-engine answer here was asked of legend-engine itself, through the runner's jars at the pin (legend-engine
`230c1591`), never recalled. Every reason was measured, and the measurement is cited. legend-engine is test input
only. Nothing here is a product component: `tools/engine-runner` is testonly, and its pool is usable from that package
alone (`tools/deps/pools.bzl:26`).

## 1. What was built

| Target | What it does |
|---|---|
| `//tools/engine-runner:plan` | `bazel run //tools/engine-runner:plan -- <request.json> <out.json>`. A pure/v1 request body (an `ExecuteInput`) in, legend-engine's `generatePlan` answer out: its `SingleExecutionPlan` JSON, or its error. This is the input S9's import census reads. |
| `//tools/engine-runner:execute_plan` | `bazel run //tools/engine-runner:execute_plan -- <plan.json> <out> [<parameterValues.json>]`. A given plan's JSON (legend-engine's own, or one lite exported) run by legend-engine's `PlanExecutor`. The answer's bytes are written as pure/v1's execute route sends them. This is the oracle S8's export is compared against. |
| `//tools/engine-runner:trades_duckdb` | The shared DuckDB (§3): `pure_v1/trades.duckdb` and `pure_v1/trades-duckdb.pure`. |
| `//tools/engine-runner:pure_v1_oracle_test` | The check (§4). The net's 14 requests are planned and run by legend-engine, on H2 and on DuckDB. Three files are pinned: legend-engine's answers on each database, and how each golden line differs from legend-engine's. It is in `//gates:heavy`. Update with `bazel run //tools/engine-runner:update_pure_v1_oracle`. |

The code is in `tools/engine-runner/src/main/java/planoracle/`:
- `EngineRoutes` holds legend-engine's two routes.
- `GoldenRunMain` runs the requests.
- `SeedDuckDbMain` makes the shared DuckDB.
- `compare/GoldenCompareMain` writes the answers in the net's line form, using lite's own JSON library, and lists the
  differences.

The legend-engine library (`:planoracle`) does not depend on core, so a core change never reruns it.

### How legend-engine is asked (read at the pin)

The server registers `new Execute(modelManager, planExecutor, routerExtensions, transformers)` with no plan
execution authorizer (`legend-engine-server-http-server` `Server.java:395-397`). So `pure/v1/execution/execute` runs
`Execute.execLegacy` (`legend-engine-core-query-pure-http-api` `Execute.java:309-342`), and `generatePlan` runs
`Execute.buildPlan` (`:268-279`).

Neither `Execute` nor `ResultManager` is in the runner's pool, so `EngineRoutes` composes the calls they make, and
each call is cited in the code:

- **Planning:**
  - `ModelManager(DeploymentMode.TEST).loadModel` (`ModelManager.java:91`, a text model at `:179`).
  - `HelperValueSpecificationBuilder.buildLambda`, `HelperRuntimeBuilder.buildPureRuntime` and
    `processExecutionContext`.
  - `PlanGenerator.generateExecutionPlan(..., PlanPlatform.JAVA, null, extensions, transformers)`
    (`PlanGenerator.java:64`), with the server's own extensions and transformers (`Server.java:395-396`).
- **The plan's JSON:** written with the server's mapper configuration: the Pure protocol's extensions, then
  `ObjectMapperFactory.withStandardConfigurations` (properties sorted, nulls left out; `Server.java:216-219`,
  `ObjectMapperFactory.java:32`).
- **Execution:**
  - The plan JSON is read by `PlanExecutor.readExecutionPlan` (`PlanExecutor.java:401`).
  - Its parameters are made constants as `execLegacy` makes them.
  - It runs through `execute(plan, constants, user, identity, null, requestContext)` (`:235`).
  - The answer is written as `ResultManager.manageResultWithCustomErrorCodeImpl` writes it (`ResultManager.java:59-80`):
    - an `ErrorResult` is a 500 with code 20;
    - a `StreamingResult` is a 200 and the DEFAULT serializer;
    - a `ConstantResult` is a 200 and the standard mapper.
  - An exception is a 500 and `new ExceptionError(-1, ex)` (`ExceptionTool.java:35,60`; `ExceptionError.java:50`).
- **The execute route's own plan is the same plan.** `execLegacy` plans through its own overload,
  `generateExecutionPlanWithTrace` (`Execute.java:404-420`), and `generatePlan` through `generateExecutionPlan`.
  Both call `generateExecutionPlanAsPure` with no plan id, then `serializeToJSON` and `stringToPlan`; the trace
  variant adds only a tracing span and a log line (`PlanGenerator.java:64-85`).
- **The execute answer of a request legend-engine cannot plan** is that planning error: `execLegacy` builds the
  plan first (`Execute.java:316-323`).

A plan always goes through its JSON between planning and execution, so S8 can feed `execute_plan` a plan lite exported.

**What is not reproduced, by construction:**
- the HTTP layer (Dropwizard's own mapper modules, Jersey's response buffering);
- the server's configured store executors (`Server.java:316-325`); here it is `withAvailableStoreExecutors()`, with
  each store's default configuration;
- an error's stack-trace frames, which are the in-process call path. The compare keeps the trace's first line and
  writes `<frames>` for the rest.

### The run time

Measured in this container (4 cores):
- **Run, jars fetched:** the four actions take about **16 s** of wall time, the H2 run and the DuckDB run in parallel
  (11 s and 9 s each, one legend-engine Pure load each). The diff tests take 0.1 s. A forced rerun gave byte-identical
  outputs.
- **Cold fetch:** the runner pool is 611 jars for Linux, **980 MB**, by its lock (`maven_runner_install.json`). All of
  them, from Maven Central through this cloud's proxy with an empty cache, took **11.3 s** (curl, 8 in parallel, every
  answer a 200). That is the download alone; Bazel's own first fetch adds its repository setup, and in a cloud
  session it may need `tools/cloud-maven-distdir.sh` (docs/CLOUD_SESSIONS.md). The pool was already fetched for
  `//tools/engine-runner:smoke_test` before this slice, so the check adds no download to a lane that has it.

**The lane: `//gates:heavy`, beside `//pct:edge_contract_test`,** whose goldens it reads (the orchestrator,
2026-11-18). Run per push in the `core` lane instead, it would cost about 16 s of actions and no download; that is
the per-push question, (d′) of the design, and the user's.

## 2. Facts found on the way

- **The runner's H2 is legend-engine's own patched 2.1.214, not the pool's 2.2.224.**
  - `legend-engine-xt-relationalStore-h2-execution-2.1.214` carries H2's classes, un-relocated, with legend-engine's
    patches (S141).
  - It is ahead of `com.h2database:h2:2.2.224` on the class path, so `org.h2.engine.Constants.VERSION` answers
    2.1.214, and the errors carry `[...-214]`.
  - The oracle writes the version it loaded into the first line of each pinned answers file
    (`# legend-engine 4.145.0; H2 2.1.214; DuckDB v1.3.0`). A class-path change that flips it fails the diff test.
- **The pool's H2 2.2.224 would move 3 of the 28 lines** (the orchestrator's measurement, 2026-11-18). On a scratch
  class path with `h2-2.2.224.jar` first (the pool's pin left as it is), the 14 requests answered as on 2.1.214 except:
  - `raise.divideByZero` and `raise.parseInteger`: the error's build suffix, `[22012-224]` and `[22018-224]` against
    `-214`;
  - `cell.float`: `0.3333333333333333` against 2.1.214's `0.33333333`.

  Plain JDBC on 2.2.224 types legend-engine's SQL for `cell.float` as a DECFLOAT `0.333333333333333333`; why
  legend-engine's path then answers 16 digits is not measured. Every other line, the plans included, was
  byte-identical. So legend-engine's own answer (its pom's 2.1.214, `pom.xml:160`) is what the runner gives today,
  by class-path order alone, and the pin decides it only if that order changes.
- **The runner's existing runtime set lacks DuckDB's grammar.** Only `legend-engine-test-runner-service` brings
  `legend-engine-xt-relationalStore-duckdb-grammar` and `-duckdb-pure`, and without them a DuckDB connection does not
  parse ("Unsupported Data Source Specification type 'DuckDB'"). The oracle takes the runner's whole jar set, with core
  left out.
- **Lite's plans are close to legend-engine's.** Both plan 11 of the 14 requests. In 10 of those 11 plans, the only
  differences are the SQL text and the column types listed in §4; the 11th is `value`, whose root node differs. Both
  refuse the other 3.

## 3. The shared seeded database

**H2.** The goldens' model is `core/src/test/resources/upstream-api/trades-h2.pure`:
- Its seed is the connection's `LocalH2 { testDataSetupSqls }`, and it travels in the plan: the plan's connection is
  `h2Local` with the 19 statements.
- Each executor seeds a fresh in-memory H2 from the same statements, so the data is identical.
- The versions differ: lite runs 2.5.252 (S141, the user's ruling of 2026-11-05) and legend-engine runs its patched
  2.1.214.
- Being literally one database would take an H2 server of one version, which changes the goldens' model. That is not
  proposed.

**DuckDB.** `//tools/engine-runner:trades_duckdb` builds one shared file:
- **The seed:** the same 19 statements, read from the H2 model by legend-engine's own parser (the connection's
  protocol JSON), so the seed has one source.
- **The writer:** legend-engine's DuckDB, 1.3.0.0 (`pom.xml:165`). It is the oldest reader: a newer DuckDB reads an
  older file, and the reverse does not hold.
- **The model:** `trades-duckdb.pure` is the H2 model with its connection replaced by
  `src/test/resources/pure_v1/duckdb-connection.pure`, composed by legend-engine's grammar composer.
  `${DUCKDB_PATH}` names the reader's copy, because DuckDB opens a database file to write and a build input is
  read-only.
- **Read by lite's DuckDB, checked by hand:** lite's own DuckDB (the product's `duckdb_jdbc-1.5.6.0`) opened a copy
  and read 128 rows, `sum(qty)` 509, as on H2.
- **No test of that read yet.** It would need `tools/engine-runner` added to `duckdb_jdbc`'s users in
  `tools/deps/jars_table.bzl`. That is an allowlist entry, so it waits on the user's word, or S8 hosts the test in
  core, where lite's runs read the file.

## 4. The check: the pure/v1 goldens against legend-engine (H2)

**The result.**
- **No golden line is byte-identical** to legend-engine's answer: 0 of 28.
- **The statuses agree on 27 of the 28 lines.** The exception is `value`.
- **Legend-engine cannot plan 3 of the 14 requests on H2:** `raise.atOutOfBounds`, `raise.toOneOverMany` and
  `cell.empty`. Lite refuses the same three (§5).

The pinned listing is `tools/engine-runner/src/test/resources/pure_v1/engine_h2_differences.tsv`, and legend-engine's
answers are in `engine_h2_answers.tsv`.

**Reasons that recur:**

| Reason | Where it shows |
|---|---|
| **R-SQL**: lite's SQL text is lite's own. It shows in `activities[0].sql` and the plan's `sqlQuery`; for example lite has `"t0"` aliases, upper-case keywords and `LIMIT`, where legend-engine has `"trades_0"`, lower case and `top`. | expected: what runs is lite's ENGINE SQL, not legend-engine's text (§3.9) |
| **R-H2**: the H2 build in an error's text: lite's `[22012-252]` against legend-engine's `[22012-214]`, and the two versions word the failing value differently. | every H2 error |
| **R-TRACE**: legend-engine's `ExceptionError` carries `trace` (the stack trace); lite sends none. Lite adds `errorType` where legend-engine has none. | every planning refusal |
| **R-WORDS**: the refusal's sentence is each engine's own. | §5 |

**Each line, the execute answer (E) and the plan (P):**

| Golden | Differs in | Reason |
|---|---|---|
| `tds` E/P | `builder.columns[*].type`; sql | **Type:** legend-engine types a store column by its precise primitive: `meta::pure::precisePrimitives::Int` and `::Varchar`, where lite writes `Integer` and `String`. That holds in the builder and in the plan's `tdsColumns`. The rows are identical, plus R-SQL. |
| `value` E | status 500/200, the whole body | **legend-engine answers; lite refuses.** Lite answers 500, "a SCALAR result's serialization is unprobed". legend-engine answers 200, `{"builder":{"_type":"dataTypeBuilder","type":"Integer"},"activities":[...],"result":{"columns":["COUNT(*)"],"rows":[{"values":[128]}]}}`. This is the probe the edge's message asks for. |
| `value` P | the root node | lite plans a `relationalTdsInstantiation` of one column, labelled `"value"`, over `SELECT (SELECT COUNT(*) ...) AS "value"`. legend-engine plans a `relationalDataTypeInstantiation`, `resultType` `dataType` `Integer`, `resultSizeRange` [1, 1], over `select count(*) ...`, its result column labelled `count(*)`. |
| `raise.divideByZero` E | message; +trace | Both answer 500 with H2's division-by-zero error, and both plans are 200. The message differs by R-SQL, R-H2 and R-TRACE. legend-engine's SQL in the message also starts with its trace-id comment. |
| `raise.atOutOfBounds` E/P | errorType; message; trace | Both refuse at planning. legend-engine says "->at(...) function is supported only after direct access of 1->MANY properties. Current expression: [1, 2] -> at(5)" (`pureToSQLQuery.pure:3307`, a `PureAssertFailException`). Lite says "LIST_FILTER reached a dialect without a list encoding". R-WORDS, R-TRACE. |
| `raise.toOneOverMany` E/P | errorType; message; trace | Both refuse at planning. legend-engine says "Cannot cast a collection of size 2 to multiplicity [1]" (`pureToSQLQuery.pure:3136`, a `PureExecutionException`). Lite says "LIST_FILTER reached a dialect without a list encoding". R-WORDS, R-TRACE. |
| `raise.sqrtOfNegative` E/P | sql | The same answer: both 200 with `null`. R-SQL only. |
| `raise.parseInteger` E | message; +trace | Both answer 500 with H2's conversion error. lite casts to `BIGINT`, legend-engine to `integer`. Plus R-H2 and R-TRACE. |
| `raise.parseInteger` P | `tdsColumns[0].relationalType` | **Crossed:** lite's plan says `INTEGER` while its SQL casts to `BIGINT`. legend-engine's plan says `BIGINT` while its SQL casts to `integer`. Plus R-SQL. |
| `cell.dateTimeCoarse`, `cell.dateTimeSubMicro`, `cell.floatWhole`, `cell.strictDate` E/P | sql | The same cells, builders and statuses: `2024-01-02T10:30:00.000000000+0000`, `...123456789+0000`, `6.0`, `2024-01-02`. R-SQL only. |
| `cell.float` E | the cell; sql | **legend-engine answers `0.33333333`; lite answers `0.3333333333333333`.** Measured (`H2Attr`, plain JDBC, on both H2 builds): legend-engine's SQL `(1.0 * cast(1.0 as float)) / cast(3.0 as float)` is a `DECFLOAT` on H2 2.x, because NUMERIC times DOUBLE gives DECFLOAT. Its H2 2.1.214 divides that to 8 digits; H2 2.5.252 would give `0.333333333333333333`. Lite's `CAST(1.0 AS DOUBLE) / CAST(3.0 AS DOUBLE)` is a DOUBLE on both. **It is legend-engine's SQL typing, a Float answered at 8 digits: a defect candidate for (e′).** |
| `cell.float` P | sql | R-SQL |
| `cell.empty` E/P | errorType; message; trace | Both refuse at planning. legend-engine says "Couldn't find DynaFunction to Postgres model translation for toVariantList()" (`toPostgresModel.pure:277`). Lite says "LIST_GET reached a dialect without a list encoding". R-WORDS, R-TRACE. |
| `cell.emptyIf` E/P | `relationalType`; sql | **Type:** legend-engine types the computed String column `VARCHAR(8192)` in the builder and the plan; lite types it `VARCHAR(1024)`. The cell is `null` on both. Plus R-SQL. |

**What the table says, for the slices that follow:**
- **Lite already matches legend-engine** on:
  - the shape of 10 of the 11 plans both make;
  - every cell value lite answers but one;
  - every status but one;
  - the step of every refusal: both refuse the same 3 requests at planning.
- **The answer-level differences that are not SQL text, wording or trace** are four, each a cell of §4.b's kind for
  the user to rule, not a register row today:
  - the precise-primitive column type (`tds`);
  - `VARCHAR(8192)` against `VARCHAR(1024)` for a computed String (`cell.emptyIf`);
  - the scalar answer that lite refuses (`value`);
  - the crossed `INTEGER`/`BIGINT` (`raise.parseInteger`'s plan).
- **One is a legend-engine defect candidate:** `cell.float`'s 8 digits.

### Main against legend-engine: what PR #118 changed (the orchestrator's question, 2026-11-18)

**How main was measured.**
- **The checkout:** main at `4dfbb5b76`, detached, measured in place; nothing pushed there.
- **The recorder:** a scratch recorder, never committed. It sends the same 14 bodies the net sends: lite's
  `grammarToJsonLambda(lambda, false)`, the model text, `vX_X_X` and a `BaseExecutionContext`.
- **The routes:** `PureV1Api.execute` and `generatePlan`, through main's shipped server runner,
  `ConnectionResolver.runner("none")` (`LegendHttpServer`'s default sources).
- **The lines:** written as the net writes them, then compared with legend-engine's answers by the same comparison.

**Main answers the five answer-level differences exactly as PR #118 does,** so none of them is new in PR #118:

| Difference | main | PR #118 | legend-engine |
|---|---|---|---|
| `tds`: column types | `Integer`, `String` | the same | `meta::pure::precisePrimitives::Int`, `::Varchar` |
| `cell.emptyIf`: computed String | `VARCHAR(1024)` | the same | `VARCHAR(8192)` |
| `value`: the scalar answer | 500 "a SCALAR result's serialization is unprobed" | the same | 200, `dataTypeBuilder`, 128 |
| `plan.raise.parseInteger`: `relationalType` | `INTEGER` | the same | `BIGINT` |
| `cell.float` | `0.3333333333333333` | the same | `0.33333333` |

**Main and PR #118 differ on 4 of the 28 lines, and on each PR #118 moved toward legend-engine.** main plants a
raise check with lite's words; PR #118 plants none on pure/v1, so the database raises (or not) as under
legend-engine:

| Line | main | PR #118 | legend-engine |
|---|---|---|---|
| `raise.divideByZero` | 500 "Raised: Division by zero" | 500, H2's own division-by-zero error | 500, H2's own division-by-zero error |
| `plan.raise.divideByZero` | `CASE WHEN 0 = 0 THEN SIGNAL(...)` in the SQL | the division alone | the division alone |
| `raise.sqrtOfNegative` | 500 "Raised: Unable to compute sqrt of -2.0" | 200, `null` | 200, `null` |
| `plan.raise.sqrtOfNegative` | a `SIGNAL` guard in the SQL | `sqrt(-2.0)` alone | `sqrt(-cast(2.0 as float))` alone |

The other 24 lines are byte-identical on main and PR #118.

## 5. Requests legend-engine cannot plan

| Request | H2 | DuckDB | legend-engine's words |
|---|---|---|---|
| `raise.atOutOfBounds` | refused | refused | "->at(...) function is supported only after direct access of 1->MANY properties. Current expression: [1, 2] -> at(5)" |
| `raise.toOneOverMany` | refused | refused | "Cannot cast a collection of size 2 to multiplicity [1]" |
| `cell.empty` | refused | **planned** | on H2: "Couldn't find DynaFunction to Postgres model translation for toVariantList()". DuckDB plans `cast(list_value() as VARCHAR(1024)[])[1]` and answers `null` |

Every request legend-engine's grammar reads: no `PARSE` line.

## 6. legend-engine on the shared DuckDB (no lite goldens yet; for S8)

`engine_duckdb_answers.tsv` holds legend-engine's answers on DuckDB:
- **Plans:** 12 of 14 requests planned.
- **Answers that differ from its H2 answers:**
  - `raise.divideByZero` answers 200 with `null`: DuckDB's division by zero.
  - `raise.sqrtOfNegative` answers 500 "Out of Range Error: cannot take square root of a negative number"; H2 gave
    `null`.
  - `raise.parseInteger` fails converting to `INT64`.
  - `cell.empty` answers `null`.
  - `cell.dateTimeSubMicro` is cut to microseconds: `2024-01-02T10:30:00.123456000+0000`.
  - `cell.float` is `0.3333333333333333`.

S8 compares lite's exported plans against these.

**A sample beyond the net, through the two tools by hand** (`plan`, then `execute_plan` on its output):
- the request: `->filter(r|$r.year == 2024)->groupBy(~region, ~total: r|$r.notional : x|$x->sum())->sort(...)` on H2;
- legend-engine's answer: `["AMER",296]`, `["APAC",1576]`, `["EMEA",1404]`, `["LATAM",9.8E+2]`.

The sum of the DOUBLE column comes back as a decimal, and the JSON writes that decimal's own text (`9.8E+2`). Whether
H2 2.1.214 or legend-engine's patch types it so is not measured here. The oracle answers what legend-engine answers,
quirks included; S8's comparison is where each such case is classified.

## 7. What this slice changed outside the runner, for review

- **The net's request list is one file.** B1's lead moved `PureV1EdgeGoldenTest.REQUESTS` into
  `core/src/test/resources/edge/pure_v1_requests.tsv` in a commit of its own (`b7ec3649c`, the orchestrator's
  arrangement of 2026-11-18). This branch merged that commit alone, and the oracle reads the same file, so the two
  cannot drift. Its 14 rows are the oracle's earlier copy byte for byte, so no oracle line moved.
- **Core exports three test files** to `//tools/engine-runner`: the requests, the model and the golden.
- **`release.MODULE.bazel` declares ten artifacts** the oracle names directly: Jackson core and databind, Eclipse
  Collections' API, and legend-engine's execution, identity, compiled core, core extension, store platform, mapping
  and m3. Every one was already in the runner pool's closure.
  - The repin (`REPIN=1 bazel run @maven_runner//:pin`) changed only `__INPUT_ARTIFACTS_HASH`: ten entries added, no
    resolved artifact, version or dependency moved.
  - The resolver warned of checksum mismatches on some POMs through the proxy: the empty file's SHA-1, `da39a3ee`.
    The lock is still checked by sha256.
- **`//json`'s visibility** adds `//tools/engine-runner`, for the compare. This is Bazel visibility, not a guard
  allowlist; flagged for review.
- **Not changed:** no pin, ceiling, ledger or guard allowlist. The lite-side DuckDB test waits on one (§3).
