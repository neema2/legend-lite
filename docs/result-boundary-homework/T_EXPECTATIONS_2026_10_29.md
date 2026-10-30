# Track T — the expectations that moved, one by one (started 2026-10-29)

The user's ruling (2026-10-28): the overlapping tests are not deleted; the same queries and logic are judged on lite's
own LEAN output (lite's JSON rows, what the query service serves), the platform twin of PCT and the corpus. The rule
of this file (the start page's track T, build item 2): every expectation that moves is decided one by one and listed
here -- the file, the test, the old expectation, the new one, the reason (the old decoder's Java carrier; a Decimal's
scale; a date's precision; a bug in the new path FIXED -- never a loosened assertion).

## The reader (`core/src/test/java/com/legend/testing/Lean.java`; the decisions log D42)

`Lean.run(model, query, runtime, connection)` runs the product's own route (`Execution.run`: plan, the plan's runner,
`JsonRowsWriter`) and reads the JSON back as a typed client does. Three rules decided while the first batch ran:

1. **The columns are the ones the runner handed the writer**, captured by the sink -- a keyed plan's (a dynamic pivot's
   per-key columns) exist only once its keys have run; `ExecutionPlan.answerColumns()` is null for it by design.
   (Seen as 26 pivot tests refusing "the plan answers no rows".)
2. **A row's key is the column's PRESENTED name** (`CellText.presentedName`: a generated pivot column in its quotes, as
   the engine labels it), the writer's own spelling, looked up the same way. (Seen as null cells in 5 pivot and
   quoted-name tests.)
3. **Each cell is decoded by the product's own row decoder** (`result/RowDecode.cell`, by the column's three type
   facts: an Integer a long, a Float a double, a Decimal an exact BigDecimal, a date its `PureDateLiteral`, a String its
   text); a TAGGED column (a mixed selection), which reaches the writer as the one cell the session resolved, is read
   by its declared Pure type as any slotless cell is (a Number: a whole number a long, any other an exact decimal; a
   Date: its text parsed). (Seen as 10 refusals "a tagged cell is read with its row" and, before rule 3 chose the
   decoder, 11 date carriers arriving as text.)

## Batch T-a (2026-10-29): `AbstractDatabaseTest`, `CheckerResults`, the 28 subclasses -- 2,232 tests

| File | Test | Old expectation | New expectation | Reason |
|---|---|---|---|---|
| (none) | -- | -- | -- | every expectation held: 2,232 of 2,232 pass on the LEAN reader with the three rules above; the mechanical rewrites changed the FORM of the reads, never a value (the T-a audit compared the 23 rewritten files statement by statement: zero residue): `asCollection().values()` -> `values()`; `returnType().typeName()` -> `columns().get(0).type()` (six sites, every one a scalar answer whose column 0 IS the value's type); `asGraph().json()` -> `json()`; `assertInstanceOf(ExecutionResult.Graph.class, r)` -> `assertEquals(Shape.OBJECTS, r.shape())`; `col.pureType().typeName()` -> `col.type()`; `Row` -> `List<Object>` and a row's `.values()` dropped; `ExecutionResult` (simple and qualified) -> `Lean.Result`; the service's materialized `execute` and `Execution.execute` -> `Lean.run`; `((Scalar) r).value()` -> `r.one()` (which also enforces one row of one column); two unused `QueryService` fields deleted |
| DuckDBIntegrationTest, TypeInferenceIntegrationTest | the tagged-column tests (`testMode_MixedNumericList_returnsDouble`, `testMinNumbers_preservesOriginalType`, `testGreatest/Least_MixedDateTypes_*`, `testMinNumbers`) | a mixed selection's cell carried the SLOT's carrier (a Float slot a `Double`; a DateTime slot at its presented precision) | the JSON token read by the column's declared type (a Number: a whole number a `Long`, any other an exact `BigDecimal`; a Date: its text parsed) | a CARRIER fact that changed with no assertion depending on it (the T-a audit's finding 4): the tests assert `instanceof Number`, `.doubleValue()` within a tolerance, `instanceof Integer || Long` and a date's `toString()`, all of which hold; the `returnsDouble` name was already vacuous (`instanceof Number`), before this batch. Named here, not folded into rule 3 |

`ExecutionResultIntegrationTest` (8 tests) asserted the old door's result VARIANTS (`Scalar`, `Tabular`, the typed
accessors' errors, `returnType` non-null): rewritten to the LEAN answer's SHAPES (`VALUE`, `RELATION`), the plan's
column names and types (`firstName`, `Integer`, `String`) and the one column a VALUE answer is written with
(`[{"value": ...}]`) -- the same queries, the facts the product's output carries. Dropped: `Type.relationSchema(returnType)`
non-null (the relation's shape is asserted as `RELATION`); the typed accessors' error (an old-door fact).

**Counting rule for "left on the old door"** (revised with T-b2 after the T-b audit, D45): the census
`docs/result-boundary-homework/probes/old_door_census.py.txt` (run from the repository root with `python3 -I`): every
line under `core/src/test/java` that names the old door or its result -- `Execution.execute(`,
`Execution.executeResolved(`, `ExecutionResult`, or the query service's MATERIALIZED execute by any receiver
(`qs.execute(`, `queryService.execute(`, `service.execute(`, `new QueryService().execute(`); a line of the service's
writer or stream doors (an `OutputFormat` or an `out` argument) is not the old door; a file counts when it has such a
line. The rule the T-b records first stated (two greps, one of them without the service's receivers) missed 25 call
sites in six integration files whose results were `var`s (the T-b audit's finding 1), and its figures for `39a2a657`
("114 files, 142 + 89 = 231 lines") were not what it yields: by that rule `39a2a657` has 115 files and 174 lines, the
T-a records' own numbers (finding 2). By the census: at `9ca7482f` (T-b) 101 files, 461 lines.

## Batch T-b (2026-10-29): the integration package's remaining 31 files; `testing/Program.java` -- 2,232 tests

Rule 4 (the reader): **a VARIANT column's cell is the variant's JSON text.** Lite's JSON rows write a Variant as a
STRING holding its JSON (legend-engine's measured form, `CellText`), so the token in the row is a JSON string; the
reader hands its value (`Json.Str.value()`), the text the old door's cell held. The writer is unchanged.

Rule 5 (the view, `Lean.of`): **a body only the program entry runs is read through the same reader.** Six files run
bodies the plan path (`Execution.run`) does not run today -- a read of the system metamodel store (`MetamodelStoreTest`,
`MetamodelMappingStoreTest`, `MetamodelStoreSubstitutionTest`, `MetamodelQueryFunctionsTest`: measured on `Lean.run`,
`DataError: Catalog Error: Table with name classes does not exist!` -- the plan's metamodel setup is not applied on the
caller's session where the body driver's route establishes the graph's system database, `SystemDatabase.Route`; an
open finding for track D, when the body driver's route goes, D44), the DDL natives (`DeclaredQuotedTableH2Test`), a
staged let (`StagedRowsTest`). They enter through `testing/Program.java` (`Execution.runProgram`, the one entry for a
program: the body typed once, the verdicts kept in order, the last statement's side) and read the side through
`Lean.of`: a scalar or a collection a VALUE answer of one column `value`, a relation a RELATION answer, a graph fetch
an OBJECTS answer. The body driver's `Scalar(null)` is Pure's `[]`: ZERO rows, as the plan path writes an empty value
(a statement answering no row writes `[]`; the judges' value lane makes `Scalar(null)` of no row, `JudgePlans`).

| File | Test | Old expectation | New expectation | Reason |
|---|---|---|---|---|
| AssociationIntegrationTest | `CombinedTraverse.testExtendTraverseThenFilterOnBoth` (the one query whose root keeps a navigate's slot column `n1`; the others select it away) | the old door's judge flattened the row-struct column to the prefixed columns the join emitted (`n1_ID`, `n1_NAME`, ...; `JudgePlans`' private `flattenStructColumns`) and answered 7 columns | the plan path declared `n1` as ONE column with no slot (`ColumnFacts`' tolerance for a name prefixing some outputs) and the runner refused: "the statement answers 7 columns, and the plan declares 5 [name, firm, deptId, n1, deptName]" | **a bug in the new path, fixed in main** (nothing in the test moved): `TypedRows.schema` flattens a row-struct column (`TypedRows.flat`, the lowering's own `outputsOf` rule, stated once), the judge's private copy deleted, `PlanColumns`' second derivation of the columns (`of(ExprType, SqlQuery)` over `UpstreamRelationType.columns`, which `TypedQuery.plan` took) deleted for the one (`TypedRows.schema`), `ColumnFacts`' tolerance deleted (AGENTS.md invariant 4: it hid the defect; a column none of the outputs covers is refused by name). Both routes now answer the 7 columns |
| CypherNativeEndsTest, CypherIslandIntegrationTest | `theNodesAtAWrittenRelationshipsEndsAreItsStartAndItsEnd`; one test of the island | a Variant cell was the variant's JSON text | read as a JSON string token the decoder handed WITH its quotes | rule 4 (the reader): the string token's value; no value moved |
| MetamodelMappingStoreTest, MetamodelQueryFunctionsTest | `classMappingByIdIsAQuery`, `extendsChainAsRows`, `classMappingsByClass` | the helper mapped the old door's `Scalar(null)` to `[]` (`v == null ? List.of() : List.of(v)`) | `Lean.of` first gave one row of a null cell, `[null]` | rule 5 (the view): `Scalar(null)` is zero rows, as the plan path writes it; the helpers now read `values()`; no value moved |

The mechanical rewrite (`docs/result-boundary-homework/probes/t-b-code.py.txt`, the forms of T-a plus: `((Scalar)
r).value()` -> `r.one()`; `.rows().get(i).values()` -> `.rows().get(i)`; `col.pureType().typeName()` -> `col.type()`;
and, unlisted until the T-b audit (its finding 13, every one value-neutral): `assertInstanceOf(ExecutionResult.Tabular
.class, r)` -> `assertEquals(Shape.RELATION, r.shape())`; the three-argument `Rows.assertSameRows(expected, r.rows(),
Row::values)` -> the two-argument one (the same multiset compare); `result.asTabular()` -> `result`;
`com.legend.exec.Column::name` -> `Lean.Column::name`; two javadocs) put 24 files on `Lean.run`; the six files above
went to `Program.last` by hand (the same model, body, runtime and connection; the two three-argument sites,
`Execution.execute(model, query, c)`, as `Program.last(model, query, null, c)`); the 31st, `ExecutionResultIntegrationTest`,
was already on `Lean.run` (T-a) and changed only by hand. `ExecutionResultIntegrationTest`'s
`typedAccessorErrorMessage` (which judged the reader's refusal, not the product: the T-a audit) is replaced by
`valueAnswerHasOneColumnNamedValue` (the one column `value` a VALUE answer is written with, `[{"value": ...}]`).

**Left on the old door after T-b** (the counting rule above): 85 files, 149 lines. By package: lowering 16, exec 16,
resolver 12, integration 6 (the parity-lane files `CypherParityTest`, `CypherParityNativeTest`, `SqlParityTest`,
`StreamingIntegrationTest`, `CypherWritePlansTest`: T-c with the helpers; and `ExecutionResultIntegrationTest`, a false
hit: its own class name matches `ExecutionResult`), compiler/spec 5, testcases 4, sql/dialect 3, server 3, normalizer
3, compiler 2, the root package 13, and one each in testing (`Lean`, whose view `Lean.of` takes the old result until D)
and compiler/element.

## Batch T-b2 and T-c (2026-10-29): the other packages, the six files the rule missed, the parity harnesses and lanes

Reader rules added (the reader stays a reader of the product's output; the writer unchanged):

- Rule 6: **`value()` is a VALUE answer as a client reads it** -- a to-one column's one cell (null when no row was
  written, Pure's `[]`), a to-many column's cells in order; refused for any other shape. The old door's helpers
  `r instanceof Scalar s ? s.value() : r instanceof Collection c ? c.values() : r` are this.
- Rule 7: **`Lean.service(model, query, runtime)`** runs the query service's own route with no caller's connection
  (`QueryService.execute(model, query, runtime, sinkOf)`, new in main: the route its `execute(..., out, format)` now
  rides -- the runtime's connection leased from the server's resolver, the plan sessions shared); a test of the server's
  sessions judges it. **`Lean.established(model, query, runtime, c)`** runs on the caller's connection with the
  runtime's DECLARED test data established first, as the engine-shaped API establishes a connection on every
  acquisition (`QueryService.executeUpstream`, `Execution.Establish.DECLARED_SETUP`); a test of the declared data's
  loading judges it.
- Rule 5 completed: `Lean.of` gives a scalar side [0..1], a collection side [0..*], a relation side the typer's
  multiplicity where the judge had one, and a graph side one row per object (the T-b audit's finding 14).
- `Program` types a body as the old door and the corpus runner type it (`Compiler.resolveQuery`: the names, the
  statement splice, the desugars; then `queryResolved`), never by `Compiler.query` alone (the audit's finding 7);
  `Program.last(..., options)` for a run under execute options (a judge mode, the substring correction).

| File | Test | Old expectation | New expectation | Reason |
|---|---|---|---|---|
| (the root, exec, lowering, resolver, compiler, normalizer, sql/dialect, server packages: 68 files; the six integration files the first rule missed; the parity harnesses and their eleven lane tests) | -- | -- | -- | every expectation held on `Lean.run`, `Lean.service`, `Lean.established` or `Program.last` with the rules above and the two product fixes below; the mechanical forms of T-b plus: `((Scalar) X).value()` -> `X.one()` across lines; `((Collection) X).values()` -> `X.values()`; `((Tabular) X)` -> `X`; `assertInstanceOf(ExecutionResult.{Scalar,Collection,Tabular,Graph}.class, r)` -> `assertEquals(Shape.{VALUE,VALUE,RELATION,OBJECTS}, r.shape())`; `Type.Primitive.X, s.returnType()` -> `"X", s.columns().get(0).type()`; `col.pureType()` -> `col.type()`; `r instanceof Graph g ? g.json() : String.valueOf(r)` -> `r.json()`; `String.valueOf(r).contains("rows=[Row[values=[2]]]")` (an old-door toString) -> the row's value; the IQ, SQL and Cypher lanes' `service.execute(model, island, RUNTIME, session)` -> `Lean.run(...)`; `SqlParity.grid(Lean.Result)` (a to-one VALUE answer the count grid, as a Scalar was), `CypherValues.ofCell/ofRow(Lean.Column)` (a Variant by the column's type name) |
| CypherParityTest, CypherParityNativeTest, CypherParityPostgresTest | the negative-zero cases (`tck/expressions/literals/Literals5/[9]`, `[10]`, `lite/basics/review_float_arithmetic_is_double`, `lite/exprtail/float_remainders_exact`) | the driver's `-0.0` | `0.0`: lite's JSON rows spell `-0.0`, and `Json.parse` computed a float token's double from its exact `BigDecimal`, which has no negative zero | **a bug in the product's JSON library fixed in main** (`json/.../Json.java`: the double from the token, `Double.parseDouble`); the writer unchanged; the lanes' PASS kept |
| StructValueTest | `instanceRootIsOrderedMap` | the instance's declared properties in declaration order | the declared properties, then `__id`, `__type` as null properties: the writer drops an object's identity fields (`CellText.declared`), and `RowDecode`'s STRUCT branch read every field of the plan's slot back, the dropped ones as null | **a bug in the product's row decoder fixed in main** (`RowDecode`: an identity field the JSON does not carry is not materialized; `CellText.synthetic`); the writer unchanged |
| MultiplicityStrictnessTest | `collectionEgressLowerBoundRaisesOnZeroRows` | Pure's error "Cannot cast a collection of size 0 to multiplicity [1..*]" from the judges' value lane on `->toOneMany()` over no row | the plan path writes `[]`: no egress bound is enforced on it | the test keeps the judges' answer (`Program.last`); the plan path's silence is a DIVERGENCE from legend-pure, which raises at `toOneMany` (D46, queued to track F: the egress lower bound enforced on the plan path as Pure enforces it, or ruled) |
| LegacyTextTest (6), ResolveDeepEmptinessProbeTest (`classRootedFrameRead`), RowLoadDuckDbTest (`systemSeedIsBulk`), the assert-arm, verdict, instance-identity and `executeInDb` files (AssertErrorNativeTest, AssertVerdictSpliceTest, AssertVerdictsTest, InstanceIdentityTest, LiteralChannelTest, AuditTier1PipelineTest, ExecuteFrameTest, ExecuteInDbDuckDbTest, ExecuteInDbProbeCountTest, Phase1AuditTest, RawGridSchemaDuckDbTest, PctFunctionSuppressionTest, JudgesOnPlansTest) | -- | the old door | `Program.last` / `Execution.runProgram`: bodies only the program entry runs (the SQL-text natives `toSQLString`, which the plan path refuses as a read of a store the storeless runtime binds no connection to; the router's `execute` frame in a let; a system metamodel store read, D44; the assert arms and the effect natives, which ARE the body driver's judging) | the user's rule: a test of a verdict reads what the platform judged; no value moved |
| RowLoadH2Test | `declaredCsvTestDataLoadsOnItsDatabase` | the old door established the runtime's declared test data on the connection | `Lean.run` (the query service's route on a given connection, `Establish.AS_GIVEN`) found no table | rule 7: `Lean.established`, the engine-shaped API's route, which establishes; no value moved |
| ResolveGraphUnionProbeTest, StructValueTest, AuditTier1PipelineTest, ExecuteInDbDuckDbTest, Phase1AuditTest | `namedSetRoute`; `mapOverInstanceList`; `nullCellToStringIsEmpty`; `multiStatementBlobWithKeywordColumns`; `columnNamesIsASchemaFact` | the old door's forms | the reader's list forms the script's `Row` regex had stripped (`r` for `r.values()`), an old-door `toString` containment (`rows=[Row[values=[2]]]`), and `one()` where the answer is no row | repaired to `r.values()`, the row's value (one row, its cell 2), `r.value()` (null for no row): no value moved. With the rows above these are the 16 that failed the first T-b2 run |
| ConstantPlanParityTest, SubstrIndexingTest, ServerSessionsTest | the `scalar` helpers; the shape assertion | `assertTrue(r instanceof Scalar, "expected a scalar frame, got <class>")` | `assertEquals(Shape.VALUE, r.shape(), "expected a scalar frame")` then `one()` (one row of one column) | a helper's form and message (the audit's finding 10): value-equivalent |

Not on the twin by design, left to track D (the census: 7 files, 21 lines): the judges' own vocabulary
(`AgreementArmTest` builds the arm's sides; `ConstantPlanParityTest` pins `LiteralFold`'s own result type;
`JudgesOnPlansTest` is off the census, its verdicts read through `Execution.runProgram`), the guards' registers
(`ArchitectureTest`, `JavaEvalLedgerTest`), two javadocs (`ExecutionResultIntegrationTest`, `StreamingIntegrationTest`),
and `Lean.of`, the view of the body driver's side until D renames it.

**By the census** (`probes/old_door_census.py.txt`): at `9ca7482f` 101 files, 461 lines; after T-b2 and T-c 7 files,
21 lines, every one named above (the T-b2/T-c audit: the records first said 8 and 23, counting `JudgesOnPlansTest`,
which this batch had already moved). The 94 files that left the old door between the two commits tile as: 76 in the
ten directories of the eight named packages (root 9, exec 16, lowering 16, resolver 12, server 9 -- the five Postgres lanes' files, the two
connection-lease tests, the project rows, the server sessions --, compiler/spec 5, normalizer 3, sql/dialect 3,
compiler 2, compiler/element 1), 13 in integration (the six the first rule missed; the SQL, Cypher and native Cypher
parity lanes' files; the IQ parity, IQ corpus and regress corpus lanes' files; `CypherWritePlansTest`), 5 harness
files in testcases (`SqlParity`, `CypherParity`, `CypherValues`, `IqParity`, `ReservedNames`, with
`SqlStressOnPostgres` the sixth changed there). The eleven per-lane targets whose files moved: `sql_parity_test`,
`sql_parity_postgres_test`, `iq_parity_test`, `iq_parity_postgres_test`, `iq_corpus_test`, `pg_regress_corpus_test`,
`cypher_parity_test`, `cypher_parity_native_test`, `cypher_parity_postgres_test`, `sql_island_postgres_test`,
`postgres_arm_test`.

## Batch T-d (2026-10-29): the twin's lanes -- core's integration tests on SQLite and H2, each held to its floor

The twin runs per lane like PCT (the start page's track T, item 3): `-Dlegend.test.lane=<backend name>` makes
`AbstractDatabaseTest.backend()` the lane's (`testing/LaneFloor.backend()`) -- at T-d the base class's 26 subclasses
(two named for a database keep theirs), about 1,280 tests; at T-d2 every integration test (below); the Bazel targets
`//core:core_tests_integration_sqlite` and `//core:core_tests_integration_h2` (the suite `//core:core_tests_lanes`) set
it. Every test is held to the lane's RECORDED floor, `core/src/test/resources/lanes/<lane>.txt` (`Class#method`, a tab,
the reason): a listed test that fails passes the run and is printed; a listed test that PASSES fails the run, asking
for its line to go; an unlisted failure fails the run. A listed test whose set-up fails is not run (its invocation skipped, said in the log), not run against a
broken set-up. The floors are measurements (each line's reason the failure's own message, measured 2026-10-29), not
judgments: a line moves only with its reason in the commit. A listed test whose set-up fails is not run (JUnit reports
it successful; the run's log says it was not run).

| Lane | Tests | On the floor | The floor's weight, by cause (measured 2026-10-29, at T-d: the base class's subclasses) |
|---|---|---|---|
| the platform (DuckDB) | 2,232 | 0 | -- |
| sqlite | 2,232 | 15 | the dialect's refusals (a windowed `ARG_MAX`/`ARG_MIN`, a `PIVOT`, `CURRENT_USER`, `year()`), an `ASOF JOIN` answer, three tests that assert the platform's SQL text (`LEFT JOIN LATERAL`, `UNNEST`, `PIVOT`) |
| h2 | 2,232 | 169 | the dialect's refusals (fold and unnest encodings, `PIVOT`, `ARG_MAX`/`ARG_MIN`, `JARO_WINKLER_SIMILARITY`, bit operations, a signed 64-bit `hashCode`, `LIST_GET`), the same three SQL-text tests, two expected-error texts |

Harness assumptions corrected so the floors measure the dialects, not the harness (no value moved): the UTC session
statement (`SET timezone='UTC'`, DuckDB's syntax) runs on the platform's session only (`TypeInferenceIntegrationTest`,
`ExecutionResultIntegrationTest`: 309 + 8 set-up failures on both lanes before); the connection text a test builds
itself takes the lane's type and specification (`DynaFunctionIntegrationTest`, `RenderCsvIntegrationTest`: "the query
executes on DuckDB but the session is <lane>", 35 before); a storeless runtime typed for the platform takes the lane's
(`FoldCheckerTest.testMapReduceFoldThroughLetInlining`); a seed's `DATE '2001-02-03'` is the plain literal every
database reads into a DATE column (`RenderCsvIntegrationTest`).

**No MongoDB lane yet:** the integration base opens a JDBC session (`AbstractDatabaseTest.connection`), and the
MongoDB backend has none -- measured 2026-10-29: 1,102 of the tests then on the lane (the base class's subclasses)
refused by that name, the rest ran on the platform's database (the T-d audit's finding 1; T-d2 puts every
integration test on the lane). Its lane waits on the base class moving to the session seam (`Sessions.Session`).

The start page's item 4 (the ~850 compile-only tests on the old door) is closed by T-b2 and T-c: by the census no
test reaches the old door to compile or plan (7 files, 21 lines remain, every one track D's); a test of a refusal
asserts on `Lean.run`'s planning, a test of SQL text on the plan's statement. The T-b2/T-c audit's nits landed with
T-d: a display name the `Row` regex rewrote restored (`Phase1AuditTest`), two unused imports, the vestigial
parentheses of the cast rewrite, a vacuous `assertNotNull` on the lease route replaced by the answer's shape and
column (`ConnectionLeaseTest`), `SqlParity.grid`'s count arm reading `value()` (a to-one answer of no row is a null
cell, as a `Scalar(null)` was), the JDBC decoder's own struct loop on the same identity-field rule.

## Batch T-d2 (2026-10-29): every integration test on the lanes; the floors re-measured; the H2 pin

At T-d the lane reached only `AbstractDatabaseTest`'s subclasses (about 1,280 tests; the T-d audit's finding 1).
T-d2 puts the integration package's other 37 classes on it: each under `@ExtendWith(LaneFloor.class)`, its
`TestBackends.platform()` now `LaneFloor.backend()` and its `Databases.PLATFORM` the lane's type. Off the lane, by
name: the parity and corpus lane files (their own lanes), `Stress*`, `DeclaredQuotedTableH2Test` (H2's by
name), `PostgresRegressCorpusTest` (Postgres's), `SqlIslandIntegrationTest` (its parameterized cases seed a
DuckDB-spelled island; measured failing on SQLite as a seed, not a case); and, each choosing its own database or
none, the one-test delegators (`ClassWrites{DuckDb,H2}Test`, `CypherDomainWritesH2Test`,
`SqlWrites{DuckDb,H2,Postgres}Test`, `StoreWrites{DuckDb,H2,Postgres}Test`, `WritePlans{DuckDb,H2,Postgres}Test`,
`StressServiceSuitesH2Test`), `DuckDBStructSyntaxTest`'s raw probe, `BazelSmokeTest` and `ProfileBuildCost` (the T-d2
audit's finding 7). 2,232 tests run on each lane.

**The re-spelling rule (`LaneFloor.model`).** A test's own connection text -- `type: DuckDB; specification: DuckDB
{ }; auth: Test;`, matched whitespace-insensitively -- is re-spelled to the SESSION's backend: the backend of the
connection the test opened (`TestBackends.of(connection)`, so `DuckDBIntegrationTest`'s own DuckDB session keeps
DuckDB's text on every lane), or the lane's where no connection is in hand (`QueryService`'s string door); on the
platform the text is untouched. Applied by the reader (`Lean.run`, `established`, `service`), by `Program`, and by
`StreamingIntegrationTest`'s service calls. Measured on the way: the first T-d2 run put 729 of 866 floor lines on
"the query executes on DuckDB but the session is <lane>", the second
283 on the whitespace variants and 59 on the reverse ("executes on SQLite but the session is DuckDB":
`DuckDBIntegrationTest`), the third 16 (`StreamingIntegrationTest`'s service calls): zero now.

**A floor key for a parameterized or repeated test** is `Class#method[#N]`, the invocation's ordinal (JUnit's
`[test-template-invocation:#N]`); the floors are LIVE (`testing/LaneFloorTest`: every key names a class and a
method that exist, so a line cannot outlive its test by a rename or a deletion).

**The not-run pin (new floor-line form).** A line whose reason opens `not run <date>:` pins its test as NOT RUN
on the lane (its invocation skipped, said in the log): for a failure that would END the run, which no measured line
can hold. One today: `UserFunctionIntegrationTest$RelationGeneric#aChainOfCallsIsTypedOnceEach` on H2 -- twenty
chained calls of a window-extending generic function lower to twenty nested derived tables; H2's planner re-plans
every level per candidate plan (`QueryExpressionIndex.getQuery`) and exhausts the 512 MB heap in
`Select.getPlanSQL`, and JUnit rethrows the `OutOfMemoryError` as unrecoverable (the first T-d2 H2 run died at its
759th test with one `<testcase>` reported). It passes on DuckDB and SQLite. A pin carries its date and cause
(`LaneFloorTest` checks the form) and moves only with one (AGENTS.md's guard-file rule); the start page's track F
carries the question (a chain of extends over one window could flatten into one select).

**Harness assumptions corrected so the floors measure the dialects (no value moved):** the UTC session statement
by the backend (`TestBackend.sessionSetUp`: `ComputedProjectIntegrationTest`'s six set-up sites here,
`TypeInferenceIntegrationTest`'s and `ExecutionResultIntegrationTest`'s at T-d; the warehouse backend's own UTC
statement, which it lacked, with the T-d2 audit's fixes); `ExtendCheckerTest.testNow` reads the year of the
database's clock by the backend's own probe (`TestBackend.yearOfNowSql`; SQLite's `strftime`) and the UTC year from
the launcher's clock, where DuckDB's `year(current_timestamp AT TIME ZONE 'UTC')` ran on every lane;
`ConcatenateFlattenCheckerTest`'s three error-text assertions compare case-insensitively (H2's `Column count does
not match` failed on its capital alone -- and the finding stands that the DATABASE refuses, not the typer: track
F); `DynaFunctionIntegrationTest` and `RenderCsvIntegrationTest` keep their connection text as WHOLE Pure text again
(T-d had templated it, `type: %s; %s;`, from the backend: the own-corpus parity harvest reads the tests' inline models
as Pure, and a template is a source the reference parser rejects -- PR #80's `parser_equivalence` lane measured the
matched elements 3004 -> 2999 at `132a4ff2`, the two files' five elements; the lane re-spells the text instead); `FoldCheckerTest`'s storeless runtime typed for the lane; `InstanceIdentityTest`'s residual
parentheses; `Phase1AuditTest`'s display name.

| Lane | Tests | On the floor | The floor's weight, by cause (measured 2026-10-29, at T-d2: every integration test) |
|---|---|---|---|
| the platform (DuckDB) | 2,232 | 0 | -- |
| sqlite | 2,232 | 89 | `sourceUrl` (an external JSON source: no encoding on SQLite) 56; no corpus workspace on SQLite (the class-writes and staged-rows tests) 6; `PIVOT` (unemulated; one SQL-text assertion) 6; an INSERT of a query's rows (no spelling measured on SQLite) 4; the metamodel store's reads (`MetamodelMappingStoreTest`, `MetamodelQueryFunctionsTest`: the mapping store's answer differs on SQLite -- a finding to read) 4; checked graph fetch (list lambdas; two defect texts) 3; a windowed `ARG_MAX`/`ARG_MIN` 2; two SQL-text assertions of the platform's `UNNEST` and `LEFT JOIN LATERAL`; an integer past 64 bits 2; the `asOfJoin` emulation's wrong answer 1; `CURRENT_USER` 1; `SCHEMA` syntax 1; a union mapping's join cast (`UnionJoinMappedPropertyTest`: `ux::A is not a ux::B` -- a finding to read) 1 |
| h2 | 2,232 | 270 + 1 pin | variant navigation (no JSON support in the H2 dialect) 84; fold 39; the list encodings (`LIST_GET`, `LIST_MAX`, `LIST_FILTER`, `LIST_TRANSFORM`, `LIST_POSITION`, `LIST_MIN`, membership) 26; `UNNEST` placement 24; a struct literal or extraction 15; the list reductions (`STRING_AGG`, `QUANTILE_CONT`, `VAR_POP`, `VAR_SAMP`) 11; bit operations 11; DuckDB's function spellings reaching H2 unrefused (`ARG_MAX`/`ARG_MIN` 6, `cbrt` 4, `jaro_winkler_similarity` 3, `levenshtein` 2, `reverse` 2, `timezone` 2, `make_date` 1: the H2 dialect has no arm and no refusal for them) 20; `time_bucket` 6; `PIVOT` (one an SQL-text assertion) 6; base64 5; the list predicates (`forAll`, `exists`) 4; a signed 64-bit `hashCode` 4; three SQL-text assertions of the platform's spelling (`UNNEST`, `LEFT JOIN LATERAL`, a write plan's text); the minimal-fraction date format 2; `make_timestamp` 1; `printf` 1; checked defects 1; the `asOfJoin` emulation's wrong answer 1; a correlated scalar subquery's outer reference (`CorrelatedRelationCheckerTest`: H2 finds no `"_tds0.id"`) 1; a staged write's temp table already existing in the session (`CypherIslandIntegrationTest`) 1; a Cypher node's JSON 1; an average's digits (`CypherIslandIntegrationTest`: H2's `AVG` over integers is a DECIMAL, `31.66666666667`, where Pure's `average` is a Float and DuckDB's double gives `31.666666666666668` -- upstream's answer to be READ before any fix) 1; `ascii('')` null 1; a Variant compared to an Integer (`JSON to BIGINT`) 1 |

Between T-d and T-d2 the sqlite floor went from 15 to 89 and the h2 floor from 169 to 270 + 1 pin: every added line
is a test the lane did not reach at T-d, re-measured here; no line is a test that passed at T-d and fails now (the
confirm run holds every listed line as failing and every other test as passing). The six lines the T-d table
counted under "two expected-error texts" (`ConcatenateFlattenCheckerTest`'s two) and the `year()` refusal are gone
by the harness corrections above.

The floors' reasons stay the measured messages, not root causes (D47); the lines named "a finding to read" above
and in the start page's track F are the ones whose message is a wrong ANSWER rather than a refusal.

**What a floor line holds, and what it cannot (the T-d2 audit's finding 9, 2026-10-29).** A measured line now
holds its test to its REASON: the failure's message, normalized as the generator normalized it, must open with the
line's first 60 characters (`LaneFloor.REASON_PREFIX`), or the run fails naming both -- a listed test that starts
failing for another reason is no longer green. What stays by design: a listed test whose set-up newly fails is not
run (said in the log, reported successful); a not-run pin never asks for its own removal (it is re-measured by hand
when its cause moves); a floor line names a class under the extension (`LaneFloorTest` refuses one that is not);
an empty reason is refused; `LaneFloor.model(String)` without a session re-spells to the lane's backend for the
service door (`Lean.service`), which no laned test uses today.
