# The mixed bodies: a system-metamodel read beside a user query -- the bucket measured, the decision (2026-11-02)

The largest class of bodies still walked after M10c items 1a-5: the ones the class rule tags `assert,system-read`
without routing them to the system database (137 on the DuckDB host pass, 139 on the database pass), and the effect and
frame mixes carrying a system read (about 140: `activities,assert,check,effect,let-execute,system-read` 38,
`assert,check,effect,last-query,let-execute,system-read,tdg-generator` 30, `assert,effect,system-read` 25, and smaller
lines). The plan's words for them so far: "a body mixing the two stores has two targets and stays on the walk, by name"
(M10a). This note reads what the bodies are, what each road does with them, what the PRODUCT does for a metamodel read in
a user query, and proposes the decision. Written at the user's ask ("look at some examples ... make a written down
decision/design"), from the code and the corpus; the one measurement still running is named in §2.

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

(A) is a test-road rule the measurement validates; it proceeds under R23 unless the user objects. (B) is M4: the user
ruled R21 (match the product) and R23 (skip M4 for now, finish the rest); this note proposes M4 at M10d's start, measured
first, with (C) the fallback. A word on (B)'s timing is the one decision needed.
