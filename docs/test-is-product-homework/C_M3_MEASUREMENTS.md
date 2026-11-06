# C3. M3a: the measurements (the test-is-the-product plan's step M3a, 2026-10-31)

The plan's M3 (`docs/TEST_IS_THE_PRODUCT_PLAN_2026_10_31.md` §6 B12) starts with a measuring step, M3a. It counts what
M3's later sub-steps will move, and it changes nothing in the product. Every count below comes from one research patch
that was never committed. It is saved here as text:

- `probes/M3Census.java.txt`: the collector, plus three appendices: the F11 JDBC probe, the aggregator and the run
  scripts.
- `probes/m3-hooks.diff.txt`: the hooks, in seven files: `BodyCompiler`, `BodyDriver`, `AssertVerdicts`,
  `BodyPlanner`, `PlanAllocations`, `PlanMaker` and `PureTestRunner`.

The patch ran on e170939a and was removed afterwards.

The runs:

| Run | What | Outcomes with the probe on |
|---|---|---|
| corpora | `//spec:corpus_duckdb` and `//spec:corpus_h2` (host and database passes) | 6/6 on both lanes: the verdict test and all five register shards green, so every test's outcome matches the committed rosters. Unlike M0 and M2, the two DuckDB engine-order registers did **not** move: this probe renders nothing. |
| channel B | the five `//pct:pct_channel_b_*` suites | 5/5 PASSED |
| core | every `//core:core_tests_<p>` package of `m1/ids.sh`, plus `core_tests_root` | 27/27 pass |
| F11 | a plain-JDK JDBC program against the jars Bazel holds | -- |

Columns: `chB` is the five channel B suites summed. Channel B runs the judge mode `BOTH`: one walk per test, no verdict
batch, no frames, no effects. Each corpus column is one pass. Where DuckDB and H2 agree, one number is given; where they
differ, the H2 figure follows in parentheses.

## 0. The answers in plain words

1. **The `from()` renames (M3h) move nothing in the corpora.**
   - 11 statements per host pass (7 tests) and 7 per database pass (6 tests) have an outermost `from()` whose context
     carries a `tableReplace`.
   - In every one of them the environment already carries the same renames: those of an execute frame, from
     `FramePlanner.chain`.
   - Channel B has none.
   - So reading the `from()`'s renames in `BodyPlanner.prepare` changes no corpus or channel B SQL.
2. **No seeds are sent inside an effect run.**
   - While an effect segment is pending, `establishContexts` passes the memo about 124,143 times per pass. Each time the
     session is marked written, so the memo always misses.
   - Every one of those passes sends nothing, because no test data is named.
   - Only 8 establishments per corpus pass send seeds (100 statements). None of them happens during an effect run.
   - Channel B sends no seeds at all.
3. **A frame is read once per resolved SQL in 97% of cases.** MV9's two candidate rules move very different amounts.

   | Measure | Read once | Read twice | Read 3+ times |
   |---|---|---|---|
   | each resolved body (one SQL), host | 3,650 (3,544) | 75 | 40 |
   | each resolved body (one SQL), database | 3,917 (3,901) | 75 | 40 |
   | per statement, host | 3,644 (3,538) | 78 | 40 |
   | per statement, database | 3,040 (3,026) | 443 (442) | 87 |

   - The database pass's per-statement "twice" is mostly the judge planning one side two or three times (415
     statements). It is not two reads in one SQL.
   - The hook-level count the brief named (splice rewrites per statement) is inflated: its raw rewrites are more than
     twice the frame occurrences that reach resolution, from let-prefix re-splices and chained arms. §3 gives both
     measures.
4. **Inline eager executes: 1 per corpus pass, 0 in channel B.**
   - The one case is `testWithParameterToClassNestedSelect`, `assertSize(0, execute(...)->size())`.
   - The host pass runs it while the assert is planned.
   - The database pass never runs it: under a verdict batch, `buildFrame(eager=true)` runs only a result that is needed.
5. **Staged grids (T12): 1 per corpus pass, 0 in channel B.** The case is `meta::relational::tests::ddl::dropAndCreateTempTable`.
6. **Activity comments (D49).**
   - Registrations per pass:
     - host: 1,934 (1,917) real run stamps and 354 (353) empty ones, each from a frame that never ran (an inline lazy read);
     - database: 2,290 reserved stamps.
   - 18 statements per pass read the activities.
   - Exactly **one statement reads a comment**: `meta::relational::tests::query::simple::testSQLComments`. It sees the
     real stamp on the host pass and the reserved stamp on the database pass.
7. **Frames reading the system store: 0 in every pass.**
   - 3,344 to 3,784 frames per pass were resolved.
   - 3 to 6 dry resolutions refused, all of them user cross-store or model-join chains.
8. **The query door in core's tests: 63 of 3,568 product-door queries would be refused. Every one plans today, and every
   one is a product use, not a test body.**
   - 47 are `com.legend.server.PureV1ApiTest` queries whose `from()` names a seeded runtime. In each the runtime is the
     plan's own runtime, whose declared seeds the plan's target already carries.
   - 16 are Cypher write programs: `CypherDomainWritesTest`, `CypherNativeEndsTest` and `CypherWritePlansTest`. They
     "hold a verdict" only because their write guard calls `meta::pure::functions::asserts::fail`, which
     `StatementEffects.callsVerdict` counts as a verdict.
   - The other kinds never reach the door: an effect, inline seeds, a let-bound `execute`, `assertError`.
   - **So the door rule as B12 words it would break 63 product queries.** It needs two narrowings: §8.
9. **The databases.**
   - **DuckDB 1.5.6:**
     - an error raised while a statement runs aborts the transaction: a conversion, a constraint or `error()`;
     - a parse, catalog or binder error does not;
     - it has **no savepoints at all**: the syntax is a parser error, and the JDBC call is unsupported;
     - a JDBC `commit()` of an aborted transaction returns normally but silently rolls everything back.
   - **H2 2.1.214 and 2.4.240:**
     - no error aborts the transaction;
     - `SAVEPOINT s`, `ROLLBACK TO SAVEPOINT s` and `RELEASE SAVEPOINT s` all work as text;
     - DDL commits the transaction implicitly;
     - an effect script's own `BEGIN ... COMMIT` sent inside a JDBC transaction commits the caller's transaction.

**One surprise beyond the nine items: F11's case is not empty.** M0 counted fused verdict groups inside a *body*
transaction, and found 0. This run also counted groups inside the corpus harness's per-test *attempt* transaction: the
harness turns autocommit off around an effect body.

| Pass | Groups inside the attempt | Fused statements | Tests | Fused statement rejected there |
|---|---|---|---|---|
| DuckDB database | 133 | 187 | 131 | 0 |
| H2 database | 132 | 182 | 131 | 4 |

H2 does not abort, so its 4 rejections are harmless today. But DuckDB aborts on run-time errors and has no savepoint. So
F11's rung cannot be a savepoint there, and C5's second question is live: "a dialect that aborts on error with no
savepoint: no fused statement in a transaction, or refuse". §9 has the details.

## 1. The `from()` renames (M3h, R17 C9)

**How.** The hook sits in `BodyPlanner.prepare`, where the outermost `from()` is chosen. It reads
`froms.get(0).context().postProcessors().tableReplace()`, which no code reads today. When that is non-empty, it compares
the map with `env.tableReplace()`, the renames the environment already carries:

- an execute frame's, from `FramePlanner.chain`;
- the reader's widening, from `BodyDriver.frameReplaceEnv`.

It counts prepares, distinct statements (a statement id per walked statement) and distinct tests.

| | chB | duckdb/host | duckdb/database | h2/host | h2/database |
|---|---|---|---|---|---|
| prepares with an outermost `from()` | 0 | 5,574 | 5,336 | 5,477 | 5,320 |
| ... whose `from()` carries renames: prepares = statements | 0 | 11 | 7 | 11 | 7 |
| ... distinct tests | 0 | 7 | 6 | 7 | 6 |
| ... the environment already carries the same renames | 0 | 11 | 7 | 11 | 7 |
| ... the environment carries none (would move at M3h) | 0 | **0** | **0** | **0** | **0** |
| ... a conflicting rename | 0 | 0 | 0 | 0 | 0 |

**The tests.**

- `meta::relational::graphFetch::tests::simple::testGraphFetchWithTableMapperPostProcessor`
- the `meta::relational::tests::postProcessor` tests:
  - `testReplaceTablePostProcessor`
  - `testReplaceTablePostProcessorWithExists`
  - `testReplaceTablePostProcessorWithView`
  - `testReplaceTablesPostProcessor`
  - `testToSqlStringReplaceTablesPostProcessor`
  - `testReplaceTableMultiplePostProcessor` (host only)

The host pass has 4 more statements than the database pass. They are the frames' own eager runs at their lets.

**Example.** `testReplaceTablePostProcessor`: the `from()` has `{personTable=differentPersonTable}`, and the environment
has `{personTable=differentPersonTable}`.

**Reading.** M3h's merge, loud on a conflict, lands with no SQL movement on the corpora or channel B. Its movement is
confined to a `from()` whose runtime carries renames with no execute frame around it. The test road has no such
statement. M2's `toSQLString` case (`EnginePlans.toSqlString`) is a separate path, fixed at M2.

## 2. Seed sends inside an effect run

**How.**

- The body loop (`BodyCompiler`, before `establishContexts`) records whether the segment's effect sink is non-empty, and
  whether the statement is itself an effect.
- `establishContexts` counts four things:
  - a pass that the memo skips;
  - an establishment whose `CsvSeed.planned` steps are empty: it sends nothing;
  - one that sends;
  - one that sends while effects are pending.

| | chB | duckdb/host | duckdb/database | h2/host | h2/database |
|---|---|---|---|---|---|
| establishments skipped by the memo | 3,074 | 16,439 | 16,462 | 16,275 | 16,463 |
| establishments that send nothing, effects pending | 0 | 124,143 | 124,143 | 124,143 | 124,142 |
| establishments that send nothing, no effects pending | 1,249 | 335 | 335 | 334 | 334 |
| establishments that **send** (seed statements sent) | 0 | 8 (100) | 8 (100) | 8 (100) | 8 (100) |
| ... of which **inside an effect run** | **0** | **0** | **0** | **0** | **0** |

**Why the memo misses inside an effect run.** `collectEffect` marks the session written before each effect, so the memo
misses at every later statement. But those statements name only their runtime, whose connections declare no test data:
the setups seed with `executeInDb`. So nothing is sent.

**Example.** `setup meta::relational::tests::createTablesAndFillDb`: an `executeInDb` statement inside the run of
statements 2-18.

**The 8 tests that seed.** All are outside any effect run:

- `meta::pure::graphFetch::tests::XStore::milestoning::CrossStoreGraphFetchWithRelationalMilestoned`
- `...CrossStoreGraphFetchWithRelationalMilestonedAllversions`
- three `m2m2r::milestoning` tests
- `meta::relational::graphFetch::tests::union::rootLevel::testSpecialUnion_m2m2r`
- `meta::relational::tests::projection::filter::testParametrizedEnumFilter`
- `meta::relational::tests::query::paginate::testPaginated`

**Reading.** The brief's risk is empty in the corpora: "Setup steps must precede the script step". The order rule stays
as designed.

## 3. Frames read per statement (MV9)

**How: two measures.**

**(a) The hook-level count the brief named.**

- `BodyDriver.spliceHook` was wrapped:
  - every hook application that rewrote its node, after `Frames.frame(name)` answered, counts one read of that frame;
  - reads are deduplicated by the rewritten node's identity, per (statement, frame);
  - the frame's form is recorded: a CTE, a class-extent CTE, or pasted.
- This over-counts in three ways:
  - A `VALUE` let reading a frame (`let results = $result.values`) rides the let prefix. It is re-spliced for every
    later statement, counted as "an earlier statement's node rewritten again": 541 (505) host and 554 (552) database
    rewrites.
  - One logical read can pass through several splice arms, for example `sqlRemoveFormatting($r)`.
  - Reads the inliner splices can be dropped before resolution.

**(b) The tree-level count.**

- At `BodyPlanner.resolvedToExecute`, every body about to be resolved and executed was scanned for the frames it holds:
  - a pasted chain, found by the identity of its root (registered when `frame()` answered);
  - a `TypedFrameRef` (a relation CTE);
  - a `TypedFrom` with an `extentFrame` (a class-extent CTE).
- A resolved body is one SQL plan: a value statement, a host side, or a database side.
- "Per statement" sums the statement's resolved bodies. "Per program" sums the body's.

**(b) The tree-level count: what is resolved and lowered**

| reads of one frame | duckdb/host | duckdb/database | h2/host | h2/database |
|---|---|---|---|---|
| **per resolved body (one SQL)**: 1 / 2 / 3+ | 3,650 / 75 / 40 | 3,917 / 75 / 40 | 3,544 / 75 / 40 | 3,901 / 75 / 40 |
| ... the database's CTE form: 1 / 2 / 3+ | -- | 2,679 / 58 / 32 | -- | 2,667 / 58 / 32 |
| ... the class-extent CTE: 1 / 2 | -- | 142 / 6 | -- | 142 / 6 |
| ... pasted: 1 / 2 / 3+ | 3,650 / 75 / 40 | 1,096 / 11 / 8 | 3,544 / 75 / 40 | 1,092 / 11 / 8 |
| **per statement** (summed over its resolved bodies): 1 / 2 / 3+ | 3,644 / 78 / 40 | 3,040 / 443 / 87 | 3,538 / 78 / 40 | 3,026 / 442 / 87 |
| ... the CTE form: 1 / 2 / 3+ | -- | 1,878 / 388 / 79 | -- | 1,868 / 387 / 79 |
| ... the class-extent CTE: 1 / 2 | -- | 130 / 12 | -- | 130 / 12 |
| ... pasted: 1 / 2 / 3+ | 3,644 / 78 / 40 | 1,032 / 43 / 8 | 3,538 / 78 / 40 | 1,028 / 43 / 8 |
| statements whose frame is in 2 / 3+ resolved bodies | 3 / 0 | 368 / 47 | 3 / 0 | 367 / 47 |
| **per program**: 1 / 2 / 3+ | 794 / 770 / 434 | 701 / 736 / 568 | 806 / 755 / 410 | 706 / 739 / 559 |

No statement read one frame in two forms.

**The examples.**

- **One SQL reading a frame twice:**
  - `meta::pure::tds::tests::extensions::columnValueDifferenceTest` (frame `rawPrevTradeDate`, x2);
  - three times: `meta::relational::testDataGeneration::tests::TestDatGenForNestedViews` (frames `_s15_result` and
    `_s19_result`, x3).
- **The database's multi-body statements:**
  - Callers:
    - `planValue` -> `planBare` twice: 210 statements;
    - three times: 7;
    - `planValue` plus a host side through `JudgePlans.evalSide`: 157;
    - a mix of the two: 40;
    - `evalSide` twice: 1.
  - Example: `meta::relational::tests::tds::sort::testSimpleSortAscWithPreval`, frame `result`, read in two bodies, once
    each.
  - These are the database judge planning one side more than once, not two reads in the fused SQL.
- **The host's 3 multi-body statements:** `meta::relational::tests::getter::testGetterTwice` and
  `...projection::enumeration::testEnumValueReturnedInIfExp`.

**(a) The hook-level count (the brief's measure, inflated as described)**

| splice reads of one frame per statement: 1 / 2 / 3+ | duckdb/host | duckdb/database | h2/host | h2/database |
|---|---|---|---|---|
| all forms | 2,701 / 555 / 1,094 | 2,720 / 556 / 1,094 | 2,613 / 542 / 1,077 | 2,720 / 571 / 1,079 |
| ... the database's CTE form | -- | 1,790 / 418 / 745 | -- | 1,790 / 430 / 733 |
| ... the class-extent CTE | -- | 115 / 13 / 34 | -- | 115 / 13 / 34 |
| ... pasted | 2,701 / 555 / 1,094 | 815 / 125 / 315 | 2,613 / 542 / 1,077 | 815 / 128 / 312 |
| hook instances per (statement, frame) | always 1 | always 1 | always 1 | always 1 |
| raw hook rewrites | 8,679 | 8,986 | 8,471 | 8,952 |
| of which re-splice a node an earlier statement rewrote | 541 | 554 | 505 | 552 |

Examples of 3+ reads: `meta::relational::tests::query::filter::exists::testExistsInAbstractProperty` (frame `result`,
4 reads) and `...query::function::testFilterUsingStartsWithFunction` (`result_escape`, 3 reads). Both reach their
resolved bodies once per SQL.

**MV9's movement under C3's two forms** (DuckDB; H2 within 10).

- **"A CTE only where a statement reads the frame more than once"** (B10's words, per statement):
  - database single reads that are CTEs today become pasted: **2,008** statement-frames (1,878 + 130);
  - host statements reading a frame twice or more become CTEs where `FramePlanner.cte` plans one: at most **118**
    (78 + 40).
- **"A CTE wherever `FramePlanner.cte` plans one"** (both modes):
  - the database text is unchanged;
  - the host text moves for every statement reading such a frame: about **2,487** of the host's 3,762 statement-frames
    (the database's CTE-form count: 2,345 relation + 142 class extent).
- **Per program instead of per statement** (M7's fusion): more than half of the programs read a frame twice or more:
  1,304 (1,298) of 2,005 (2,004) on the database pass; 1,204 (1,165) of 1,998 (1,971) on the host pass.

Channel B reads no frame.

## 4. Inline eager executes

**How.** `Frames.inlineExecute(ec, eager)` in `BodyDriver.spliceHook` counts by `eager` and by whether a verdict batch is
present. With a batch, `buildFrame` runs a frame only when its result is needed.

| | chB | duckdb/host | duckdb/database | h2/host | h2/database |
|---|---|---|---|---|---|
| `inlineExecute(eager=true)` | 0 | 1 (runs now) | 1 (**not run**: under a verdict batch) | 1 (runs now) | 1 (not run) |
| `inlineExecute(eager=false)` (lazy inline reads) | 0 | 1,405 | 1,708 | 1,399 | 1,845 |

**The one case.** `meta::relational::tests::query::qualifier::testWithParameterToClassNestedSelect`, statement 4:
`assertSize(0, execute(|Product.all()->filter(...), simpleRelationalMapping, testRuntime(), ...)->size())`. The splice
folds `size(execute(...))` to 1 and asks for the eager run, "Pure is strict".

The host pass runs the query while the assert's side is planned. The database pass never sends it. This is a small
instance of decision 1 (MV7) for an inline execute: on the program road it is a `Check` step before the statement.

## 5. Staged grids (T12) and the let kinds

**How.** `LetPlanner.kind` was counted in the body loop.

| let kind | chB | duckdb/host | duckdb/database | h2/host | h2/database |
|---|---|---|---|---|---|
| FRAME | 0 | 1,974 | 1,975 | 1,959 | 1,975 |
| ALIAS | 0 | 198 | 198 | 198 | 198 |
| STAGED | 0 | 0 | 0 | 0 | 0 |
| **STAGED_GRID** | **0** | **1** | **1** | **1** | **1** |
| VALUE | 1,581 | 4,740 | 4,742 | 4,705 | 4,742 |

**The one staged grid.** `meta::relational::tests::ddl::dropAndCreateTempTable`: `let res = executeInDb('select * from
tt', $c)`, statement 3. It is read by `$res.columnNames` after `dropTempTable('tt', $c)`. T12's refusal on the program
road touches one corpus test per lane.

## 6. Activity comments (D49)

**How.**

- In `buildFrame`, the comment kind was recorded before `PlanAllocations.registerActivityRows`:
  - a real run stamp: the frame ran, `trace.lastComment()`;
  - a reserved stamp: under a verdict batch, `trace.reserve()`;
  - `''`: neither.
- `registerActivityRows` counts each registration, after its "already registered" return.
- The rows' lazy supplier counts each materialization, which happens when a statement reads the activities.
- A reach-through scan (callees included) finds a statement that reads `.comment` of an `...Activity` class. The
  materializations during such a statement were counted too.

| | chB | duckdb/host | duckdb/database | h2/host | h2/database |
|---|---|---|---|---|---|
| registered: a real run stamp | 0 | 1,934 (1,771 tests) | -- | 1,917 | -- |
| registered: `''` (a frame that never ran: an inline lazy read) | 0 | 354 (298 tests) | -- | 353 | -- |
| registered: a reserved stamp | 0 | -- | 2,290 (2,063 tests) | -- | 2,290 |
| skipped (the scope already registered) | 0 | 1,091 | 1,394 | 1,078 | 1,531 |
| materialized (a statement read the activities) | 0 | 18 | 18 | 18 | 18 |
| **statements reading an activity's `.comment`** | **0** | **1** | **1** | **1** | **1** |
| ... the comment it saw | -- | a real run stamp | a reserved stamp | a real run stamp | a reserved stamp |

**The comment reader.** `meta::relational::tests::query::simple::testSQLComments`, statement 2:
`assert($result.activities->at(0)->cast(@RelationalActivity).comment->toOne()->matches('-- "executionTraceID" : "<uuid>"'))`.

**The other 17 activity readers** are the `aggregationAware` rewrite tests, which read `rewrittenQuery` (for example
`...aggregationAware::testRewrite::objectGroupBy::testRewriteEmployeeToSales`).

**Reading.** C4's slot, bound at run time, has exactly one corpus reader, and it only regex-matches the stamp's shape.
The host's 354 `''` registrations and the database's reserved stamps on frames that never run are the two rules C4 asked
to unify. No statement reads either kind's comment.

## 7. Frames reading the system store

**How.** In `buildFrame`, each frame's run chain was dry-resolved:

- by its own `StoreResolver`, with the query lets read and no plan rows;
- nothing of the resolution was kept;
- the stores were collected, and the metamodel store (`SystemMetamodel.STORE_FQN`) looked for.

| | chB | duckdb/host | duckdb/database | h2/host | h2/database |
|---|---|---|---|---|---|
| frames resolved | 0 | 3,344 | 3,647 | 3,324 | 3,784 |
| string-entry frames (`executeLegendQuery`), not resolved | 0 | 33 | 33 | 22 | 33 |
| **frames whose chain reads the system store** | **0** | **0** | **0** | **0** | **0** |
| dry resolution refused | 0 | 3 | 6 | 3 | 6 |

**The refusals** are user stores the plain resolver cannot route alone:

- `XStore::inMemoryAndRelational::testCrossMappingJsonToDBWithExplosion` (in-memory and relational);
- `modelJoin::advanced::testNestedModelJoinCompoundInnerCondition`;
- `graphFetch::tests::simple::testObjectReferenceInUsingResultReferences`.

M4's refusal by name on the test door costs no corpus frame.

## 8. The query door's would-be refusals in core's tests

**How.** `PlanMaker.plan`, the product door that `Execution.run` and the server reach through
`TypedQuery.executionPlan`, became a wrapper. It scans `query.body()` once and then records what the door does today: it
plans, or it throws. A statement's kinds, in B12's order:

- an effect: `StatementEffects.containsEffect`;
- a `from()`'s inline seeds;
- a `from()`'s named runtime with seeds: its connections declare `testData`;
- a let-bound `execute(...)` / `executeLegendQuery(...)`: `LetPlanner.rhs`, split into non-last and last;
- a verdict: `StatementEffects.callsVerdict`;
- an `assertError`: a reach-through scan for a `NativeFn.ContextOwner`.

The test class was read off the stack. The corpora and channel B never reach this door: 0 queries.

| core tests (27 packages) | queries | statements | test classes |
|---|---|---|---|
| product-door queries | 3,568 | 4,179 | 115 |
| ... planned today, holding none of the kinds | 3,451 | | |
| ... threw today (product refusals unrelated to the kinds) | 54 | | |
| ... **would be refused** (they all **plan today**) | **63** | | **4** |

| kind | statements | queries refused by it | test classes |
|---|---|---|---|
| an effect | 0 | 0 | -- |
| a `from()`'s inline seeds | 0 | 0 | -- |
| **a `from()`'s named runtime with seeds** | 47 | 47 | `com.legend.server.PureV1ApiTest` |
| a let-bound `execute(...)` (non-last / last) | 0 / 0 | 0 | -- |
| **a verdict (`callsVerdict`)** | 32 | 16 | `com.legend.integration.CypherDomainWritesTest`, `CypherNativeEndsTest`, `CypherWritePlansTest` |
| an `assertError` | 0 | 0 | -- |

**Why each case is a product use.**

- **The seeded runtimes.** In every `PureV1ApiTest` case the `from()`'s runtime **is the plan's own runtime**:
  `demo::trading::H2Runtime`, `trades::h2::RT`, `w::RT`. Its declared test data is already the plan target's setup:
  `PlanMaker.target`, "the setup that establishes it, written at plan time".
  - Example: a `TypedFrom` over `trades::h2::DB`'s `TRADES_SCHEMA.TRADES` (a group-by then a sort), and a graph fetch of
    `demo::trading::Trade`.
- **The verdicts.** The only verdict function reached is `meta::pure::functions::asserts::fail`. It is the Cypher write
  guard that `PlanMaker.program` plans as a check step ("a Cypher write's guard raises").
  `PlatformTypes.isVerdictFunction` matches the whole `meta::pure::functions::asserts::` package, so `fail` reads as a
  verdict.

**Reading.** If M3c's door refuses by the owners as B12 words it, these 63 product queries break. Two narrowings keep
them:

- the seeds rule applies only to a `from()` naming a runtime **other than** the plan's runtime (or to inline seeds);
- the verdict rule excludes `fail` (a raise, not a verdict), or uses a verdict test narrower than `callsVerdict`.

With both narrowings, no core test hits the door's refusals today.

## 9. The databases (F11)

**How.** A plain-JDK program (`probes/M3Census.java.txt`, appendix A) ran against the jars Bazel holds:

- `duckdb_jdbc-1.5.6.0` (the platform's DuckDB);
- `h2-2.1.214` (the corpus lane, `@h2`), with `H2Connector.SETTINGS`;
- `h2-2.4.240` (the modern lane, `@h2_modern`).

Each scenario ran on a fresh connection seeded with `t = {1}`, autocommit off, each step on a fresh `Statement`. The
DuckDB driver closes a `Statement` after its error, so reusing one gives "Statement was closed" whatever the transaction's
state.

| Scenario (autocommit off) | DuckDB v1.5.6 | H2 2.1.214 | H2 2.4.240 |
|---|---|---|---|
| a parse error, then a valid statement | valid statement runs (no abort) | runs | runs |
| a catalog error (missing table) or binder error (missing column), then a valid one | runs (no abort) | runs | runs |
| a conversion error (`CAST('abc' AS INTEGER)`), then a valid one | **aborted**: "Current transaction is aborted (please ROLLBACK)" | runs | runs |
| a constraint error (duplicate key), then a valid one | **aborted** | runs | runs |
| `error('boom')` (DuckDB) / a numeric overflow (H2), then a valid one | **aborted** | runs | runs |
| JDBC `commit()` after an aborted transaction | returns normally, **rolls everything back** (the earlier insert lost) | -- | -- |
| `ROLLBACK` text after an abort | clears it (the transaction ends; a JDBC `commit()` then errors "no transaction is active") | -- | -- |
| `SAVEPOINT s` | **parser error** (no savepoints) | OK | OK |
| `ROLLBACK TO SAVEPOINT s` after a failing statement | parser error | OK: back to the savepoint, the transaction usable | OK |
| `RELEASE SAVEPOINT s` | parser error | OK | OK |
| `ROLLBACK TO s` / `RELEASE s` (no keyword) | parser error | `ROLLBACK TO s` a syntax error; `RELEASE s` OK | same |
| JDBC `setSavepoint()` / `rollback(sp)` / `releaseSavepoint` | `SQLFeatureNotSupportedException` | all work | all work |
| `BEGIN TRANSACTION` text inside a JDBC transaction | error "cannot start a transaction within a transaction", **and it aborts the transaction** | accepted; the `COMMIT` text then **commits the caller's transaction** (its earlier insert survives a JDBC rollback) | same |
| DDL (`CREATE TABLE`) inside a JDBC transaction, then a JDBC rollback | transactional: the table and the earlier insert both rolled back | **commits implicitly**: the earlier insert survives the rollback | same |
| `supportsSavepoints()` | false | true | true |

**Where F11's case occurs in the corpora.** The F11 hook sat in `AssertVerdicts.flush`. Before each flush it read whether
the session owns its transaction, and across the flush it read the census deltas of fused statements and fallbacks.

| database pass | DuckDB | H2 |
|---|---|---|
| verdict groups flushed in autocommit | 2,388 (2,390 fused statements) | 2,388 (2,266 fused statements) |
| ... the fused statement rejected (a fallback) | 0 | 123 |
| verdict groups flushed **inside the harness's attempt transaction** | **133** (187 fused statements, 131 tests) | **132** (182 fused statements, 131 tests) |
| ... the fused statement rejected there | **0** | **4** |

M0's totals agree: 2,521 groups on DuckDB, 2,520 on H2.

**The four H2 tests rejected inside the attempt.** All are `meta::relational::graphFetch::tests::resultSourcing`:

- `testSourcingJsonResultToQuery`
- `...WithFiltersInMapping`
- `...WithParametersInUrl`
- `...WithPureDateAsParam`

H2 does not abort, so their wrappers ran normally. An example group inside the attempt:
`meta::pure::executionPlan::m2m2r::tests::executeProjectWithNestedDerivedProperty`, 1 row, 1 fused statement.

**Reading for M3b's rung.**

- **H2** needs no savepoint for correctness. A savepoint is available as text, so C5's "texts sent as statements" works
  there.
- **DuckDB** aborts on exactly the errors a fused verdict statement raises at run time, and cannot set a savepoint. So
  `SqlDialect` states "aborts on error, no savepoint". For such a dialect, B12 item 6 must choose:
  - no fused statement inside a transaction: 133 groups per DuckDB pass would move to one statement per verdict, a
    statement-count movement;
  - or keep fusing and accept today's latent behaviour: on a rejection the wrappers fail with "transaction aborted".
    This has 0 corpus cases today.

## 10. The transaction scopes (MV10, R17 C1)

**How.**

- In `BodyDriver.execute`: whether each body runs in one body transaction (it writes a store or stages rows), and whether
  it holds an effect.
- In `BodyDriver.sendScript`: whether each effect script was bracketed (the session owns its transaction) or sent bare.

| | chB | duckdb/host | duckdb/database | h2/host | h2/database |
|---|---|---|---|---|---|
| bodies run in one body transaction | 0 | 0 | 0 | 0 | 0 |
| bodies run with no body transaction | 1,249 | 2,433 | 2,433 | 2,433 | 2,433 |
| ... holding an effect | 0 | 754 | 754 | 754 | 754 |
| effect scripts bracketed (the session owns its transaction) | 0 | 3,579 (108 tests, chiefly setups) | 3,579 | 3,578 | 3,578 |
| effect scripts sent bare (inside the harness's attempt transaction) | 0 | 393 (140 tests) | 393 | 394 | 393 |

- **Under C1** (R17: only an `Update` opens the program's transaction), the corpora and channel B move 0 bodies:
  **MV10 = 0**. No test body writes a store or stages rows.
- **Under the rejected option B** (every change brackets), 754 effect-holding bodies per corpus pass would have run in
  one transaction.
- Example of a bracketed script: `setup meta::relational::tests::createTablesAndFillDb`, statements 2-18 (17
  statements).
- Example of a bare one: `meta::relational::graphFetch::tests::isolation::testIsolationOfPropertyTargetFilter`,
  statements 3-12 (13 statements).
