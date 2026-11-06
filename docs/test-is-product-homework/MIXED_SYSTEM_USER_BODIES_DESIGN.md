# The mixed bodies: a system-metamodel read beside a user query -- the bucket measured, the decision (2026-11-02)

The largest class of bodies still walked after M10c items 1a-5: the ones the class rule tags `assert,system-read`
without routing them to the system database (137 on the DuckDB host pass, 139 on the database pass), and the effect and
frame mixes carrying a system read (about 140: `activities,assert,check,effect,let-execute,system-read` 38,
`assert,check,effect,last-query,let-execute,system-read,tdg-generator` 30, `assert,effect,system-read` 25, and smaller
lines). The plan's words for them so far: "a body mixing the two stores has two targets and stays on the walk, by name"
(M10a). This note reads what the bodies are, what each road does with them, what the PRODUCT does for a metamodel read in
a user query, and proposes the decision. Written at the user's ask ("look at some examples ... make a written down
decision/design"), from the code and the corpus; §2's measurement landed the same day, (A) was built as M10c item 7, and §6 (added 2026-11-02, at the user's second ask: "real examples of where there is genuine read of both ... 1 by 1") reads EVERY stayed body that still carries a system read by what it actually sends at run time.

## 1. What the bucket is, measured

The 145 `assert,system-read` bodies of the host pass after M10b (the stayed roster the brief's inventory was taken from),
classified by the Pure text of each body (a script over the engine checkout; every body found):

| Bodies | Shape | Example |
|---|---|---|
| 127 | a `graphFetch` test: `let query = {|X.all()->graphFetch($tree)->serialize($tree)}; let mapping = <a mapping>; let runtime = meta::external::store::relational::tests::testRuntime(); let result = execute($query, $mapping, $runtime, ext).values; assertJsonStringsEqual(<golden>, $result)` | `meta::relational::graphFetch::tests::qualifier::testEnumParameter` |
| 4 | the same with the runtime spelled inline: `let runtime = ^meta::core::runtime::Runtime(connectionStores = ^ConnectionStore(element = meta::relational::tests::db, connection = ...))` | `testRelationalChainExecutionFlat` |
| 3 | the same, the runtime an inline literal, no helper | `testGraphFetchWithTableMapperPostProcessor` |
| 1 | `testRuntime()` and an `executionPlan(...)` handle | `testMilestonedProperty` |
| 1 | `testRuntime(<a store>)` | `testCheckedWithCircularConstraints` |
| 9 | `toPostgresModel::tests::testConvert*`: `getTable('firmTable')` / `getColumn(...)` (a navigation of the `db` element's schemas and tables) and `assertConversion` (item 4's helper) | `testConvertAlias` |

So 136 of 145 are graphFetch tests whose ONLY system read is an ELEMENT REFERENCE inside a `^Runtime(...)` construction:
`meta::external::store::relational::tests::testRuntime()` is `testRuntime(db)`, which builds
`^Runtime(connectionStores = ^ConnectionStore(element = $db, connection = ^TestDatabaseConnection(...)))`; the `db`
there is a `TypedPackageableRef` to a tracked class (a Database), which `BodyClass.systemElementReference` counts as a
read of the system store. The class rule reaches it through the user call in the VALUE let (`readsStore` follows user
calls); M10b's let rule (`bindsASystemElement` + `bindingConsumed`) exempts a let that binds an element DIRECTLY
(`let m = x::M`) and is consumed by a planning-time native -- not an element reached through a helper's body or an
instance literal. The body is not system-routed because the `execute` frame is a user read; so the kind keeps it on the
walk. The 9 `testConvert*` bodies read the metamodel for real (the `db` element's tables) and have no user read: they
are system-routed candidates whose helper root item 4 now plans (to re-measure).

## 2. What each road does with a graphFetch body today

- **The walk.** The let `runtime` is a VALUE let: it is dissolved into its readers (R11: a let lives in the database or
  nowhere). Its one reader is `execute($query, $mapping, $runtime, ext)`, whose runtime argument is the call's
  ORCHESTRATION position (`NativeFn.Handle.orchestrationArgument`): read in SOURCE form at planning -- the connection's
  declared database, its test data -- never evaluated as a value. So the walk should send nothing for it; the
  statement-origin census of one such body decides (`bazel run //spec:corpus_one -- duckdb host|database
  meta::relational::graphFetch::tests::qualifier::testEnumParameter`, the `system=` count): MEASURED 2026-11-02 05:08Z,
  `system=0` on both lanes (host: body=14, session=15, system=0; database: body=15, session=15, system=0). The walk
  sends nothing to the system session for the body: the read is spurious, and (A) is the fix.
- **The program road.** `BodyClass.assertProgram` refuses the body: the let's statement reaches a `SYSTEM_READ` kind
  (`sideKind`), and the body is not system-routed. Nothing else about it is new: the frame, the JSON assert and the
  value lets are M9's shapes, moved by the thousand.

## 3. What the PRODUCT does for a metamodel read inside a user query

`PlanMaker.metamodel` (the plan path program's step 9; R21 keeps it): the system metamodel tables a query reads
(`Class.all()`, a mapping's class mappings, a database's tables) are carried BY THE PLAN as setup steps on the plan's own
session -- on an in-process engine the target's tables, created and seeded at plan time with the graph's facts
(`MetamodelSeeds.rows`, `Ddl.metamodelSteps`); on a server engine the session's temporary tables, made by the session's
statements and gone with it; on a server without temporary tables the query's own inline rows. ONE session, no second
database: a product query mixing a user store read and a metamodel read is one plan on one target. The test road's
system reads took another road (M10a, the narrow M4): the graph's SYSTEM DATABASE, a second session seeded once per
graph, which is the walk's route and the reason a body reading both stores has "two targets". The user's R21: "Why
would test diverge from product? ... get test to match product if product correct"; R23: "skip m4 for now".

## 4. The decision, proposed

**(A) The spurious reads are a class-rule fix, not a design (an M10c item).** A value let whose system reach is
element REFERENCES alone -- no `all()` of a system class, no system table, no system function, no navigation of an
element's properties -- and whose binding is consumed (M10b's `bindingConsumed`: read by no later statement outside a
lambda or a planning-time native's arguments) is no read of the system store; the same for a `^Runtime(...)` literal
bound by a let and consumed by an execute's orchestration argument. The rule is the walk's own behaviour made a static
predicate (R16); §2's measurement read `system=0` for such a body on both lanes. Expected movement: about 130
bodies per lane; the witness a frame-model graphFetch body with a `testRuntime()`-shaped helper under both judges.

**(B) The real mixed bodies take the PRODUCT's road (M4, as R21 asked; scheduled at M10d's start, where the walk's
deletion forces their fate).** A test body reading a metamodel table beside a user store is planned as the product
plans such a query: the referenced system tables ride the body's plan as setup steps on the body's session
(`PlanMaker.metamodel`), and the separate system database -- M10a's narrow road, the `Database.System` target, the
per-graph opener -- is retired: every body has ONE target again, the two-session question dissolves, and the test road
and the product share one code path for the metamodel (B7). Measured before built: (i) how many bodies read a
metamodel TABLE for real (the 9 `testConvert*` bodies, the system-routed bodies M10a moved -- about 390 -- and the
effect/frame mixes); (ii) the referenced tables' row counts for the corpus graph (`MetamodelSeeds.rows` per table) and
the per-body seeding time on DuckDB and H2 against today's once-per-graph seeding; (iii) whether the product's steps are
idempotent on a session that already holds the tables. The cost the user named in R23 ("per-session seeding") is real
and is answered by a per-session seed cache -- the referenced tables seeded once per session per graph, which the
product gains too (a long-lived product session re-seeding the metamodel on every query is the same waste) -- or, if the
measurement says the rows are small, by nothing.

**(C) Two targets in one program -- kept as the fallback, not the choice.** The batch already groups verdict rows by
session (`VerdictBatch.plan`), and a program's steps already name a target each; a mixed body could run its system
groups on the system session and its user groups on the body's. It keeps two code paths and two databases, which R21
rejects; it is the choice only if (B)'s cost is prohibitive after measurement.

## 5. What is asked of the user

(A) is a test-road rule the measurement validates; it proceeded under R23 (M10c item 7, built 2026-11-02). (B) is M4:
the user asked to see the real mixed bodies one by one before any infrastructure moves -- §6 is that reading. Its
conclusion: ONE body in the corpus genuinely reads both stores for its verdict; the 56 test-data-generation bodies read
one metamodel row each as a table LOOKUP the generator could take in source form; everything else is spurious. The
decision asked is §6.4's: no M4 for these; the one body and the lookup family handled as spelled there.

## 6. (B) read body by body: what the stayed bodies actually send (measured 2026-11-02)

### 6.1 The method

After item 7, 254 bodies stay on the DuckDB host pass; 204 of them carry a `system-read` tag somewhere in their kinds
(the stayed table `corpus2-body-stayed.tsv` of the item 7 lane). The question is not what the TAG says but what the body
DOES: the 204 were run again on the DuckDB host pass alone (`bazel run //spec:corpus_one -- duckdb host` with
`-Drcorpus.tests=<the 204 names>` and `-Dlegend.diagnostics=corpus-trace,dump-sql`: every statement the executor sends,
on every session, printed under the test's name; 184 pass, 13 fail, 7 skipped -- the lane's own outcomes), and each
body's statements were counted by whether they name a table of the system store (`"metamodel"."<table>"`: the seed's
DDL and every system query spell it so; the system session's statements are dumped like any other -- the seed's 68 and
one body's `SELECT ... FROM "metamodel"."mappings" ...` prove it). The census is
`MIXED_BODIES_RUNTIME_CENSUS_2026_11_02.tsv` (fqn, status, tags, statements, over the system store, over anything else,
by origin, the system tables named); the script `mixed_census.py` is in the session's scratchpad (a reader of the log,
deciding nothing).

### 6.2 The count

| Bodies | What they send | Reading |
|---|---|---|
| 146 | NO statement over any system table (139 send user-store statements; 7 are SKIPPED and send nothing) | the `system-read` tag is spurious at run time: the walk reads the element in SOURCE form at planning |
| 56 | 1 to 10 statements over `metamodel.relational_elements` (a TABLE's row looked up by name), the rest over the user store | the test-data generator's `createTableRowIdentifiers($db, 'default', 'Person', ...)` = `getTable($db, $schema, $table)`: a lookup of a planning input, then the generated data loaded and the query run on the user store |
| 1 | a metamodel QUERY (`"metamodel"."mappings"` joined to `mapping_includes_closure` and `class_mappings`) judged by an assert, then a user-store query judged by an engine-text assert | **the one genuine mixed body**: `meta::relational::tests::mapping::subType::testSubTypeMappingValidWhenMappedExplicitly` |
| 1 | the system database's SEED (34 `Drop` + 34 `Create`, every metamodel table twice, no `SELECT`) | `testRelationalDatabaseMapper`: the first body of the run to open the system database pays the graph's seed; it reads nothing from it |

So of 2,613 corpus bodies, 204 tagged, 58 touch the system store at run time, 2 for a reason other than the generator's
table lookup, 1 with a verdict that depends on both stores.

### 6.3 The families, one by one, with the Pure text

**F1. The runtime copied or extended (23 bodies; 0 system statements).** `let dbRuntime =
meta::external::store::relational::tests::testRuntime(); let runtime = ^$dbRuntime(connectionStores =
$dbRuntime.connectionStores->concatenate(^ConnectionStore(element = ^ModelStore(), connection = $jsonConnection)))`
(the XStore and XStoreUnion graphFetch tests: ordered 8, inMemoryAndRelational 3 + 4 union), `^Runtime(connectionStores
= testRuntime().connectionStores->concatenate(^ConnectionStore(element = ^ModelStore(), connection =
$modelChainConnection)))` (the 4 `testRelationalChainExecution*`), `let connectionStore = $runtime.connectionStores->at(0);
let conn = $connectionStore.connection->cast(@TestDatabaseConnection); let runtimeWithPostProcessor =
^$runtime(connectionStores = ^$connectionStore(connection = ^$conn(sqlQueryPostProcessors = [{query | ...}])))`
(`testExistsWithEmbeddedWithPostProcessor`, `testRestrictWithPostProcessor`), `testCrossStoreWithCSVDataSource`. The
"system read" is the `db` element carried inside a Runtime VALUE and navigated as a Runtime's or a ConnectionStore's
property -- not a system class -- on its way into `execute`'s orchestration argument. Item 7's rule stops at the first
let because `$dbRuntime` is read as a value by the second let: a TRANSITIVE consumption (a let read only by a let that is
itself consumed). A rule extension, not a design.

**F2. `setupTestData([$csv...], $db, $runtime)` (48 bodies: modelJoin advanced 25, simple 10, milestoning 9, union 4;
0 system statements).** `let db = meta::relational::tests::mapping::modelJoin::store::Db; let runtime = testRuntime($db);
meta::relational::tests::mapping::modelJoin::setupTestData([$firmCsv, $personCsv], $db, $runtime); let result =
execute(|Firm.all()->project(~[...]), SimpleEqualityMapping, $runtime, ext); assertEquals($expectedSql,
$result->sql()); assertEquals(#TDS ... #->toString(), $result.values->at(0)->sort(...)->toString())`. The helper's body:
`$runtime.connectionByElement($db)` (an Effect native), `createDbConfig($connection.type)`,
`$csvs->joinStrings('\n-\n')->setUpDataSQLsV2($db, $dbConfig)` (an Effect native: the CSV spelled as DDL and inserts
from the MODEL's declared tables -- build-time knowledge, typed constants, never the system store's rows),
`$sqls->map(sql | executeInDb($sql, $connection))`. `$db` is read as an ARGUMENT of two helper calls whose bodies hand it
to a construction and to two Effect natives that consume it at planning; `readsStore` excludes only Handle and
JavaRoutine natives' arguments, so the let is "not consumed". Measured: nothing reaches the system store. A rule
extension (consumption through a helper's parameter, and through the Effect natives that read their element argument
at planning).

**F3. The test-data generator (56 + 19 bodies).** `let db = meta::relational::tests::mapping::inheritance::myDB; let
runtime = testRuntime($db); createTablesAndFillDb(); let tableRowIdentifiers = createTableRowIdentifiers($db,
'default', 'Person', createRowIdentifier(['name'], ['Atul Anand'])); let testData = generateTestData($query, $mapping,
$runtime, $tableRowIdentifiers, ext)->toOne(); assertSize($testData.sqls, 1); assertEquals('select top 20 ...',
$testData.sqls->at(0)); assertTestData('default\nPerson\nID,NAME\n2,Atul Anand\n-----\n', $testData.dataCsvString,
$db); loadAndTestExecution($query, [], $mapping, $runtime, $testData.dataCsvString, $db)`. The 56 bodies with system
statements send 1 to 10 `SELECT`s over `relational_elements` -- `createTableRowIdentifiers` is
`createTableRowIdentifiers(getTable($database, $schema, $table), $identifiers)`: a Table looked up by three literals,
handed to the generator as the identity of the table whose rows it should fetch. That is a planning input spelled by
name (as `from(mapping)`'s mapping is), not a question about the metamodel; the generator could take the table in
source form. The 19 others of the family send none (the `_Alloy` variants: `mayExecuteAlloyTest({...})` wraps the
generator in a lambda the walk declines; `getRelationalCSVDataFromQuery` folds the census in Java). The whole family
stays for the GENERATOR kind first (T8: no program-road step yet), so its system lookup is second-order: when T8 plans
the generator as a Keyed step, the table it names is a spelled constant and the lookup goes with the walk.

**F4. A table navigated into a post-processor (6 bodies; 0 system statements).** `let oldTable =
db->schema('default')->toOne()->table('personTable')->toOne(); let newTable = db->schema('default')->toOne()->
table('differentPersonTable')->toOne(); let result = execute(|Person.all(), simpleRelationalMapping,
runtimeWithTableReplace(pair($oldTable, $newTable)), ext); assertSameSQL('select ... from differentPersonTable as
"root"', $result)` (`testReplaceTable*PostProcessor` 5, `testToSqlStringReplaceTablesPostProcessor`). A real navigation
of the Database element (schema, table) in the text -- and NO statement over the system store at run time: the frame
planner reads the `replaceTables` shape off the runtime helper at planning (`FramePlanner`, the rename map threaded to the
lowering), the two tables by name. Source form again; item 7's audit made such a navigation a read
(`elementNavigation`), rightly for a navigation that IS evaluated -- here it is consumed by the rename reader. A rule
refinement (a navigation consumed by the replaceTables reader), or these 6 stay: small either way.

**F5. The plan-text and plan-object bodies (22 bodies; 0 system statements).** `let plan = executionPlan($query,
$mapping, $runtime, ext); assertEquals($expected, $plan->planToString(ext))` (`testViewToTDS`, m2m2r
`planProjectWith*` 4 with `getM2M2RRuntime()`), `$plan.rootExecutionNode->allNodes(ext)->filter(node |
$node->instanceOf(FunctionParametersValidationNode))...supportsStream` (`testSupportStreamFlag*` 6,
`testSQLCommentsInPlan`), `executionPlanForQueryWithDateTimeConstantForDatabaseConnectionTimeZone(%2019-4-8T18:00:00,
'EST').second` with FreeMarker template texts under `assertSameElements` (datetime 5), `^Service(...)->
executeServiceTests(2)` (2, FAIL). They stay for the ASSERT shape (`host-unplanned-assert`: a plan object navigated,
a service harness), not for the store; the element references are `executionPlan`'s own arguments.

**F6. `->from($mapping, $runtime)` inside the query (13 bodies; 0 system statements).** `let result =
execute({|Person.all()->filter(...)->project(...)->from($mapping1, $runtime)->concatenate(Person.all()->project(...)
->from($mapping2, $runtime))}, ^Mapping(name = ''), ^Runtime(), ext)` (fromMapping 6), `executeLegendQuery({|
RestrictedPerson.all()->graphFetch($tree)->serialize($tree)->from($mapping, $runtime)}, [], ext)` (m2m2r
milestoning 5, `testSpecialUnion_m2m2r`, `testCrossStoreWithCSVDataSource`). The mapping and the runtime are `from`'s
arguments, read at planning (`FramePlanner.firstMappingFqn`); the tag is `frame-system-read` (the frame's query reaches
the element), the class rule's `frameProgram` refuses the frame for it. Source form; a rule refinement (`from`'s
arguments are consumed).

**F7. Pure-level metamodel manipulation with NO user read (11 bodies; 1 system statement in one of them).**
`let alias1 = ^Alias(relationalElement = getTable('firmTable'), name = 'root'); let expected1 = ^AliasedRelation(...);
assertConversion($expected1, $alias1)` (`testConvertAlias`: the one that sends a `SELECT` over `databases`, `schemas`,
`relational_elements` -- a real lookup of the table's row, and the body's whole store traffic),
`createTempTableStatement()->eval('tt', ^Column(...), DatabaseType.H2)` (FAIL), `setUpDataSQLs($parsedRecords,
dbInc, createDbConfig(DatabaseType.H2))` with `assertSameElements` over 30 DDL strings (3), `buildJoinTreeNode(...)` /
`reAliasMergedJoinOperations(...)` (FAIL), `translateCoreTypeToDbSpecificType(^Varchar(size = 100),
^TranslationContext(coreTypeToDbSpecificSqlTranslator = {type | $type->match([...])}))` (FAIL),
`applyMilestoningFilters(^SemiStructuredPropertyAccess(...), {t|[]}, {t|$t}, false, $ext)` (FAIL, no assert), the 10
`relationalMapper` tests (`relationalMapperSqlQuery({|Product.all()}, productMappingWithFilter, [], [], tableMappers())`:
a private helper rendering SQL under schema and table mappers, `assertEquals(text, $resultSQL)`; 2 FAIL). None reads the
USER store: they are M10a's system-routed candidates, kept off the program road by their SHAPES (constructions compared
as instances, `eval`, `match`, a helper that renders text) -- M10d's class-rule residue, not a mixed-store question.

**F8. The one genuine mixed body.**

```
function <<test.Test>> meta::relational::tests::mapping::subType::testSubTypeMappingValidWhenMappedExplicitly():Boolean[1]
{
   let cm = meta::relational::tests::mapping::subType::SubTypeMappingValidWhenMappedExplicitly
              ->_classMappingByClass(meta::relational::tests::mapping::subType::MyProduct)->filter(s|$s.root == true);
   assertSize($cm, 1);
   let result = execute(|CreditRating.all()->project([r| $r.product.rating.description], ['description']),
                        SubTypeMappingValidWhenMappedExplicitly, meta::external::store::relational::tests::testRuntime(), ext);
   assertSameSQL('select "product_rating_1".RATING as "description" from PRODUCT_RATING as "root" left outer join ...', $result);
}
```

The first assert's side is a query of the system store -- on the walk one `SELECT` over `"metamodel"."mappings"`
(filtered to the mapping's fqn), `"metamodel"."mapping_includes_closure"` and `"metamodel"."class_mappings"`, the root
class mappings of `MyProduct` as a JSON array, judged by the host (`assertSize`); the second is a user query under the
same mapping whose SQL text is the verdict. Two stores, two sessions, one body: the shape (B) was written for. It is
ONE body of 2,613 (PASS on both lanes today, on the walk).

### 6.4 The decision this reading supports

No infrastructure moves for (B). The user's test -- "real use cases 1 by 1 to see if real and worth it" -- is answered:
one body is real; the 56 generator lookups are a spelled constant waiting on T8; the 146 are spurious and name five rule
extensions, each the walk's own behaviour as a static predicate (R16), each measured at `system=0`:

1. transitive consumption -- a let read only by a let that is itself consumed (F1, 23 bodies);
2. consumption through a helper's parameter -- a variable handed to a user function whose body passes that parameter only
   to constructions, element positions and planning-time natives (F2, 48 bodies; needs the Effect natives
   `connectionByElement` and `setUpDataSQLsV2` named as consuming their element argument, as Handle and JavaRoutine are);
3. `from`'s mapping and runtime arguments consumed (F6, 13 bodies -- the frame program's rule);
4. a navigation consumed by the replaceTables reader (F4, 6 bodies) -- or left;
5. nothing for F5 and F7: they stay for their shapes (M10d's residue), and the tag is noise the census may drop.

For the one genuine body: it stays on the walk until M10d, when its fate is the walk's deletion -- the honest options
then are (C) for it alone (two targets in one program: the batch already groups by session) or a `docs/SEMANTICS_REGISTER.md`
row if the user prefers to leave the metamodel-plus-user shape to the product's `PlanMaker.metamodel` road when M4 comes
for the product's own reasons. Building M4 on the test road for one body is not worth it; this note withdraws §4 (B)'s
"at M10d's start" and keeps it as what the product does (§3), to be matched when the product needs it (R21's direction,
the user's R23 "skip m4 for now").
