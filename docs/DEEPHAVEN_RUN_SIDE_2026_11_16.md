# Deephaven on the run side: the design (2026-11-16)

**2026-11-17: the plan now is docs/DATACUBE_ENGINE_AND_DEEPHAVEN_PLAN_2026_11_17.md (PROPOSED, for the user's ruling):
it supersedes §10.3's answers and §11's order below; §0-§9 stand.**

**Status: RULED by the user on 2026-11-16 ("Let's go"); being built on `claude/deephaven-3-browser`. Corrected
2026-11-17 (§4.1, §5.2, §7.1, §10.2-§10.3, §11): the build drifted from §2, and this design had not been reconciled
with DataCube's engine contract (docs/DATACUBE_ENGINE_CONTRACT_2026_11_12.md, ratified 2026-11-13, on
`claude/datacube-engine`); the homework in §10.3 comes before more code.** Written once,
whole, then reviewed independently (the reviewer saw only this document, the rules and the code) and revised once with
its findings and the user's rulings (§13). It changes the run side of docs/DEEPHAVEN_PLAN_2026_10_22.md (DH11, DH22,
DH23, B1) so the Deephaven road meets the plan/exec split C5 enforces on `main` (G4), in its spirit as well as its
letter. The program around it -- deploying, ingest, materialized views, data products, the lake -- is
docs/DEPLOYMENT_DESIGN_2026_11_14.md §15.7.

## 0. In one paragraph

Every request a runtime sends is one the plan declares; the runtime chooses only when -- a table's lifetime, and how
the caller takes the rows. A Deephaven plan is a short chain of steps, each a table the server makes, each written by
the planner as the exact bytes the server reads with a few four-byte slots for the numbers only a session knows, each
declaring how to create it if it does not exist and how to drop it. A grid's base is not a table anyone declares but a
way of running the grid's queries, so it is the planner's: DataCube writes it as a Pure `let`; a SQL planner splices it
as today; the Deephaven planner makes it a step the session keeps, named by its definition's hash, and the query a step
that reads it. One plan format, a small runtime per environment -- the browser's for v0, Java's right after it (for
PCT), Python's later -- as DuckDB already runs.

## 1. What is wrong today (read from the code)

The browser reaches the planner through one door, `Boundary.deephaven(op, args)` (`wasm/src/main/java/planner/
Boundary.java`), 13 operations and no `only(...)` closure:

| Operation | Asked by | What it is |
|---|---|---|
| `topics`, `plan`, `validate` | the feed, deploying | planning |
| `holds` | connecting (`deephaven-grids.ts`) | planning |
| `nullable` | a base being made (`server-base.ts #make`) | planning, for a declaration |
| `stored` | a base being made (`deephaven.ts stored`) | a planning rewrite (view to select) at run |
| `keys` | every query and base (`keepAlive`) | hashing the plan at run |
| `send` | every query | cutting the plan by what the session holds |
| `made` | every query | decoding and checking the answer (DH16) |
| `releaseExport`, `doGet`, `publish` | the session | encoding a request |
| `batch` | nothing in the browser | encoding |

Five things are wrong:

1. **The run asks the planner** three times a query (`keys`, `send`, `made`) and for each release, read and publish.
   It passes in the browser only because the run frame cannot see there (`engine-client/src/run-frame.ts`: "a browser
   tab ... the witness sees nothing there: a gap, named"); under Node, `main`'s guard refuses each one.
2. **Sends outside any run.** Besides a base's making (`ServerBaseRunner.#hold`, before the query's `inRun`): the check
   on connect (`deephaven-grids.ts:161` to `keepAlive`), live following (`#startWhole`'s `keepAlive` and
   `publishKept`), warming (`server-base.ts:300`), the releases of the session's keep and of close
   (`deephaven.ts:283-297`) and the token refresh (`deephaven.ts:144`). `main`'s `sending()` refuses a send outside a
   run on Node.
3. **The plan is edited at run.** `DeephavenWire.send` drops the operations the session holds and rewrites the
   references to them. `docs/TENET_CHARTER.md` A3 allows exec three things beyond sending a plan as written; this is
   none of them.
4. **A base is typed by the server's answer, and the model carries session state.** `server-base.ts #make` declares
   each base as a table of a model fragment (`legend::Bases`) from the column types the server reports
   (`columnType(c, held)`); queries are planned against it, and the session swaps the table's name for its own ticket
   as it sends (`DeephavenEngine.#supplied`, `DeephavenWire.suppliedTicket`). Each new base changes a model -- hence
   the bases' second planner and model (`ServerBaseOptions.connectionOf`: the page's 2.3 MB model held again for each
   base took 4.4 s, 2026-10-25) -- and nullability had to be asked of the planner only to write the declaration.
5. **A fallback.** A base whose columns cannot be declared is read "as written", inlined into each query
   (`server-base.ts:189-194`, B1's `asWritten`), and a page filter that rewrites inside the base makes `rebase` miss it,
   so its queries silently read as written too (`cube.ts:432`, `runner.ts:162-170`). AGENTS.md invariant 4.

## 2. The rule

**Every request a runtime sends is one the plan declares; the runtime chooses only when** -- when a table's lifetime
ends (drop), and how the caller takes the rows (fetch once, or follow live). Its other duties are bookkeeping that
decides no answer: its session's handles, which tables it holds, waiting for a table another send is making, keeping a
table it is reading, and the session's keep (a cache bound: letting a table go changes when the server works, never
what an answer is). It is given no planner, encodes no request, and reads no type to plan with. This sentence is added
to `docs/TENET_CHARTER.md` A3 and AGENTS.md invariant 3, with the code that relies on it.

The rest follows from it:

- **One plan format, a runner per environment** -- as DuckDB runs today: Java's `PlanRunner` over DuckDB's JDBC
  driver, TypeScript's `plan-runner.ts` over DuckDB-WASM, Python's `_runs.py` (on `main`) over `duckdb`. A Deephaven
  plan is that one execution plan (`PlanJson`): its steps are the plan's own steps, never steps packed inside one
  statement's text, and they are run by the environment's one runner, Deephaven supplying only a session that sends one
  step and reads its answer (as DuckDB-WASM does for DuckDB). (2026-11-17: the build drifted from this -- §10.2.)
- **The planner writes the exact bytes a server reads**, as a SQL dialect writes the exact text a database reads.
- **Types are the planner's;** a server's answer is checked against them (DH16), never read to plan.
- **Session state never enters a model;** a way of running a query is the planner's and lives in the plan.

## 3. Three concepts

| | A deployed table (landed data, a materialized view) | A session table (a grid's base, a kept result) | Delivery |
|---|---|---|---|
| Declared by | the model's author, in a deployment | the planner, from a query | the plan: how |
| Named | its declared name | the hash of its definition | -- |
| Shared | by everyone | by one session | -- |
| Created | `install`: make what is missing, change what changed, leave what matches (built) | create if it does not exist, at first use | -- |
| Dropped | `uninstall` | drop, when nothing uses it | -- |
| When, by whom | release time, by who deploys | first use, a grid's close, the keep, the session's end: the runtime | fetch or follow: the caller |

A session table is named by its definition's hash so that "create if it does not exist" is always safe: a changed
definition is a new name, so a session table is never changed in place -- the reason a deployed table, whose name stays
while its definition changes, needs `install`'s "change what changed". On Deephaven a session table stays current as its
source ticks; on a SQL database it would be a snapshot, so no SQL target keeps one until it has a freshness rule (not in
v0).

## 4. The plan's Deephaven contract

### 4.1 Steps and their verbs

A Deephaven plan is a chain of one or more steps OF THE ONE EXECUTION PLAN (§2: the plan's own steps, run by the
environment's runner; never a document of steps inside one statement's text), each one table the server makes, sent one
request per step and pipelined (each step's request goes as soon as the one before it is written: a read of a table the
server is still making waits for it there, as DoGet-on-sent does today). A step carries:

- **`request`**: its `BatchTableRequest` as the bytes the server reads. The dialect writes the operations (as
  `DeephavenBatch` does); the planner's encoder (`ProtoWire` over the message table) writes them in the protocol's
  binary form. Within a step, an operation reads an earlier one by `batchOffset`, and a server's own table by its scope
  ticket (`s/NAME`, a deployment's tables): fixed bytes. Only its last operation is exported.
- **`slots`**: the byte positions where the runtime writes its session's export ids -- the step's own result, and
  each earlier step it reads. An export ticket is `e` and a four-byte number (`DeephavenWire.exportTicket`); the
  encoder writes each length as a varint of the inner size (`ProtoWire`), so filling a slot changes no length. The
  planner writes a placeholder id of its own (the encoder refuses id 0 today). Filling a slot is binding a value, as a
  SQL plan's `?` is bound. A run-time parameter (none is sent yet: DH5) is uploaded as a one-row table and read by a
  five-byte ticket: one more slot.
- **`identity`**: SHA-256 of the step's bytes with its slots zeroed, each step it reads named by that step's identity;
  made by the planner. It is the session table's name: the argument of "create if it does not exist".
- **Its verbs, the planner's, chosen by ONE fact the database type declares -- does a table it makes from a query keep
  itself current as its sources change?** Deephaven's does (the server updates it incrementally); a SQL database's does
  not (`CREATE TABLE AS` is a frozen copy; "live DuckDB" in the tab stays current only by re-running whole queries,
  so its tables do not either). Where the answer is yes: **create if it does not exist** (the step's request, sent
  unless the session holds the identity) and **drop** (a release of its export), declared, its timing the runtime's
  (§2). Where it is no, a table-making step is today's staged `let`: made fresh, dropped when the plan answers. The
  runner performs the verbs it reads; it never asks which database it is on.
- (2026-11-17: the `kept` field this list had is gone -- whether a table outlives the plan is what its verbs say.)
- **A step reading an earlier step's table** does so by a place of its own where the runtime writes the number the
  session gave that table: a binding of the plan, as a `?` is (SQL reads a staged table by a name written into its text;
  Deephaven names a table by a number its session picks).
- **`columns`**: the plan's columns as every plan has them -- the Pure type and multiplicity, the store type, the
  database type (`slot`) -- with one fact added for Deephaven: the type as Deephaven spells it (`long`,
  `java.lang.String`), written by the dialect (`DeephavenBatch.javaTypeOf`), which the runtime's check reads. No runtime
  maps a type.
- **`declared`**: the store tables the step reads, as their store declares them (DH16, unchanged).
- **`readable`**: the step's operations as readable JSON (today's text), for the query pane and error messages; made
  by the planner from the same operations as the bytes, a test holding the two equal. No runtime reads it.

### 4.2 The session's requests, declared once a plan

The plan declares, once, the requests a session sends around its steps, each as a byte template with slots: release
an export (drop), publish an export under a shared name (follow; the name a fixed-length random value the runtime
fills), and the Flight ticket a row read names (fetch). So no runtime writes a request in Deephaven's message format;
a runtime only fills slots and decodes answers (each export's success, error and schema, read with the Arrow library).

### 4.3 What leaves the plan's runtime

`keys` (the identity is in the plan), `stored` (the dialect writes a base stored), `send`'s cut (one step, one request,
sent as written), `nullable` (the planner types the base itself, §5), and every encoder.

## 5. A grid's base

### 5.1 What it is

A grid's base (B1) is its source and its leaf-level calculated columns (`query.ts sourceWithDerived`), the part every
query of the grid begins with. It is not data anyone declared; it is a way of running the grid's queries. On DuckDB
lite has none: each query is planned whole (`server-base.ts`: "the planner folds a calculated column into each query
that reads it, as every SQL database wants"). On Deephaven a followed result recomputes its joins and calculated
columns on every tick, so the base is computed once on the server and every query reads it.

### 5.2 DataCube writes it as a `let`

DataCube writes each grid query as `{| let base = <sourceWithDerived>; <the query, its base read as $base> }` -- the
substitution `runner.ts rebase` already does, onto a variable instead of a declared table. It is ordinary Pure:

- **The existing typer** types it once; nothing is declared, and nothing is asked of the planner or the server.
- **Upstream routes take it unchanged** -- legend-engine's `pure/v1` `lambdaRelationType` (lite-planner.ts:631),
  RemoteRun, Python's `plan-and-run.json` -- since no request gains a field. (Measured, M5.)
- **A page filter** rewrites the whole query, the `let` included, so it reaches the base (§1, point 5, fixed).
- **On a SQL database** a `let` read once is spliced, as every query `let` is (`Lowerer.lower`: "the lean output has
  no trace of the lets"), so DataCube's SQL on DuckDB does not change; a query that reads its base twice
  (`joinedGrain`) writes it once, as a WITH (`SharedRelations`): leaner, and a change the SQL lanes will show.
- **On Deephaven** a query's relation `let` is planned as a statement of its own -- the staged `let`'s machinery
  (`PlanMaker.stagedLet`, `StagedRows.boundTo`), lowered alone so its bytes do not depend on what reads it -- and made a
  step with the verbs of §4.1 (create if it does not exist, under its identity; drop, the runtime's timing), the query
  a step reading it through its place. The planner decides it from the database's fact (§4.1), never from the `let`:
  the same DataCube query is spliced on DuckDB and stepped on Deephaven. A `let` a person writes in their own
  Deephaven query is planned the same way (bounded by the keep). (2026-11-17: this replaces "a WITH expression never
  spliced", which the resolver cannot keep: a `let`'s node is not one object after store resolution for a class's
  source.)

### 5.3 What the runtime and DataCube do

- The runtime creates the base step unless its session holds its identity, keeps it, and makes it again from the same
  plan after a reconnect. Grids and charts with the same base share it: the same base is the same identity.
- DataCube holds a base by its identity while a grid reads it and drops it when none does, as `ServerBases` counts its
  holders today.
- **Removed:** the bases' model fragment, their second planner and model (`connectionOf`), `ServerBaseRunner`'s
  substitution by a declared table (Snap's own copy, `rebased` for a snapped grid, is unchanged), `keepBase`, `stored`,
  the `nullable` question, the name swap, and the "as written" fallback. The cube lane's checks on `based`, `made`,
  `asWritten` and `base.table` (`cube.mjs:146-153`) are replaced, before the class goes, by the same facts read from the
  runtime: the base created once, every query reading it through its slot, none inlining it.

### 5.4 Why not a named table, real or pretend

- **Pretend** (today): session state in a model, the base typed by the server, a name swapped at run (§1, point 4).
- **Real** (published under a server-wide name and declared as an ordinary table): still session state in a model (a
  model change per base, the second planner kept), still typed from outside the planner, names every session can see,
  and no lifetime of its own -- a shared name dies with the export it was published from (DeephavenWire.java:333-335).
- **A named table is right for something else:** a dataset the model's author declares, shared and lasting -- a
  materialized view, deployed (docs/DEPLOYMENT_DESIGN_2026_11_14.md §5).
- **No hint at all** (the planner keeping what it guesses is shared): a kept table costs ticks for as long as it is
  held, and which part is shared is knowledge across queries only the caller has.

## 6. Reuse: every table kept is a step of the plan

The user's ruling (2026-11-16): "If we want to be able to cache/build intermediate tables off base tables that's fine,
but then should be part of the plan".

- **v0 keeps the base and each query's result.** A result kept means closing a group sends nothing
  (`datacube/test/deephaven/cube.mjs`). A table between them -- the grid's filtered base, which every level of its tree
  reads -- is kept when M1 shows a gesture needs it: a change to what the plan keeps, never a second mechanism.
- **Why not every operation, as today:** each table a session keeps alive keeps ticking on the server
  (`deephaven.ts`: "an idle table still ticks on the server, so they are not free"); the plan keeps what is shared and
  worth that cost. (Deephaven itself memoizes some operations on a shared parent, DH23, which softens the loss.)
- **How long:** a base while a grid holds it; a result while read and then idle within the session's keep (128 today),
  the least lately read dropped first.

## 7. One runtime per environment

### 7.1 The browser, TypeScript (v0)

- **Which runner, and who asks it (2026-11-17; answered in §10.3, H1 and H3):** DataCube's engine contract, ratified 2026-11-13,
  puts the planner, the runner and the database together in one engine and has DataCube send only Pure (its R1:
  "everything multi-step happens inside the engine"). Under it, the Deephaven engine in the tab is the WebAssembly
  planner, the tab's one runner and the Deephaven session; DataCube sends a grid's query, its base a `let`, and never
  sees the plan. Today no grid engine runs a full plan: every one, DuckDB's too, runs the text plan's one statement
  (`DuckDbEngine.execute` runs `plan.sql`), and `runPlan` runs full plans for writes and snaps only, answering JSON
  rows. The contract has no call for following a result live.
- **Where:** a run layer of `tools/deps/ts-layers.txt` (C5-T), reaching the records and npm packages only
  (`TsLayeringTest`). Constructed with no planner, so it has none to ask: proven by its type, as Java's session
  statements take a `PlannedSql` and no `String`.
- **What it keeps from today:** `engine-client/src/deephaven.ts`'s transport (gRPC-web over the page's `fetch`,
  sign-in, token refresh, the keep), rows by DoGet read with apache-arrow, the live reader (`deephaven-live.ts`).
- **What it does:** fills slots, sends steps and the declared session requests, decodes answers, checks them against
  the plan's columns (DH16). It encodes nothing.
- **Every send in a run:** a query's steps, a base's, the check on connect's, a follow's publish, warming, a release
  (the keep's, a grid's close, the page's close) and the token refresh, each inside `inRun` naming it.

### 7.2 Java, in the PR right after v0's (for PCT)

`com.legend.deephaven` (`//core:deephaven_wire`) splits: its encoder (`ProtoWire`, `DeephavenMessages`) goes to the
planner's side, where a step's bytes and the session's templates are made; its decoding (the answer's reader,
`ArrowSchema`) moves into exec as Java's Deephaven runtime, registered with the acts it performs as the JDBC census
registers its classes. Its planning parts (`keys`, `stored`, `send`'s cut) go. Transport: gRPC-web over the JDK's
`HttpClient`, no gRPC library (M4). Then the PCT lane on a Deephaven runtime.

### 7.3 Python, later

The same contract: slots, the declared requests, the answer's reader.

### 7.4 Kept identical

- **One message table, the planner's alone:** `DeephavenMessages` (the fields, numbers and types of the subset lite
  writes, checked against the pinned server's `.proto` by `DeephavenMessagesTest`).
- **Shared fixtures:** recorded cases -- a plan, a session's handles, the server's answers -- with the bytes each
  runtime must send and the result it must read; recorded from the pinned server by a lane and run by every runtime.

## 8. The doors

`Boundary.deephaven` keeps its planning operations -- `topics`, `plan`, `validate`, `holds` -- each closed by
`only(...)` over the keys it reads and registered in `PlannerSurfaceTest` with a dated note. The other nine go:
`nullable`, `stored`, `keys`, `send` into the plan; `made`, `batch`, `releaseExport`, `doGet`, `publish` into the
runtime and the plan's declared requests. No request gains a field. `ingestPlans` and the lite route
`/api/lite/v1/grammar/grammarToJson/model` are registered the same way.

## 9. The witnesses

- **What is sent is the plan's:** a test captures every request a runtime sends and finds each one among the plan's
  declared requests (a step's or the session's), byte for byte, only the listed slots differing -- Deephaven's twin of
  `MongoDbPlanTest.theCommandSentIsThePlans_itsParametersBoundByTheLet`.
- **No planner in a run, no send outside one:** the runtime takes no planner; the door's run-side operations are
  deleted; a Node test runs a cube's gestures, the check on connect, a follow and a release through the runtime under
  the run frame (`frameKept` true), counting no planner entry inside a run and no send outside one.
- **The runtime is a run layer:** `ts-layers.txt`; in Java, `execReachesNoPlanning`.
- **Types are the plan's:** no code path reads a server's types to plan; a server holding a column otherwise than the
  plan says is refused by name.
- **No session state in a model:** a grid's queries change no model (the planner's model handle is the same before
  and after a new base).
- **One base, one table:** every query of a grid writes its base as the same bytes (the same identity), whatever its
  view reads of it.

## 10. Measured before it is built on

- **M5, the base as a `let`** (first): every DataCube query shape (the tree's levels, a pivot's two queries, the
  filter's values, the row count, a window, a chart, a page filter) written with its base as a `let` -- on Deephaven
  the base a step of the same identity in each; on DuckDB the SQL unchanged where the base is read once; through
  legend-engine's `lambdaRelationType` route unchanged. And lean SQL E's answer (§12, answer 3).
- **M1, reuse:** v0's kept steps against today's per-operation reuse, on the cube lane's gestures (the regroup and the
  filter within 250 ms, closing a group within 150 ms with nothing sent) and the desk lane's opening and warm times
  (`market.mjs`). A missed budget is a defect to find, never a raised budget.
- **M2, types:** for every step the lanes make, the server's Java types equal the plan's (computed columns included:
  the dialect predicts each formula's type; a miss is a dialect fix).
- **M4, Java's transport:** the JDK's `HttpClient` speaking gRPC-web to the pinned server, before the Java runtime is
  promised.
- **M6, size:** the browser runtime's bytes, under the bundle budgets (`bundle-budget.test.ts`).

### 10.1 Measured (2026-11-17, after the merge of `main`, this container)

- **M5, part 1 (the `let` as written today):** five DataCube query shapes (the tree's root, a filtered level, a
  regroup, the filter's values, a row count) planned whole and with their base as a top-level `let`, by the built
  planner, on DuckDB and on Deephaven: the same SQL and the same batch in all ten. Today's lowering splices the `let`
  and optimizes across it -- a filter goes below the calculated column, a calculated column a view does not read is
  dropped -- so the base's operations agree across the five only on the two table reads and the join (three
  operations). Keeping the base whole on Deephaven (§5.2) is therefore new planner work, and the identity witness (§9)
  is needed. Still to measure: legend-engine's `lambdaRelationType` route on a `let`, and lean SQL E's answer.
- **M1 (reuse):** a temporary change (not committed) reusing only the base and whole results, the cube lane three
  times each way. Regroup: today 160, 132, 193 ms; base and results only 129, 147, 115 ms. Filter: today 92, 126,
  106 ms; base and results only 186, 219, 153 ms. Closing a group: today 34, 27, 19 ms; base and results only 52, 26,
  44 ms, nothing sent. Every gesture within its budget (250, 250, 150 ms); the filter about 70 ms slower, the one to
  watch -- it sends the two operations below its filter again (8 sent of 8 planned, where today sends 6). The desk
  lane passed with it (opened in 7.4 s, reopened from Open... in 13.6 s; today 10.3 s and 13.0 s in the run before).
  The cube lane's check "no table made twice" fails with it, by design: it tests the per-operation reuse this design
  deletes, and is replaced (§5.3).
- **M2 (types)** is measured as it is built: the plan carries each column's Deephaven type and the runtime refuses a
  server that holds otherwise (DH16), so every Deephaven lane run is the measurement, each refusal a dialect fix.

### 10.2 As built (2026-11-17, step 4 in progress; the drift named)

What the code does where it differs from the text above, each for a reason, and what is interim:

- **DRIFT, being corrected:** the build reads the text plan's one statement (the shortcut every grid engine uses) and
  packed the plan's steps inside it -- a steps document in the statement's text, run by a loop of the Deephaven session's
  own (`deephaven.ts keepAlive`). That is a second plan format inside the first and a second runner, against §2. The
  work that deepened it (a kept `let` as steps inside the document) is parked, uncompiled, on
  `claude/deephaven-kept-let-wip`, and is not built on until §10.3 is answered.
- **The plan's statement** on a Deephaven target is the steps document (`com.legend.deephaven.DeephavenSteps`,
  format `legend-deephaven-steps`): v0 writes one kept step a plan, the query's whole batch, and the session's three
  requests. A step exports each operation that reads a store's table (checked against its declaration, DH16, then let
  go by the declared release) and its last operation, its result.
- **The server answers once for every operation of a batch**, an exported one naming its export, any other naming its
  place (measured on 42.7): the runtime reads every answer for its success and matches the exported ones by id
  (`deephaven-steps.ts made`).
- **No Deephaven type on the plan's result columns** (§4.1 `columns`): nothing reads it -- the runtime checks the
  store tables a step reads (`declared`), which carry their Java types already. It is added when something reads it,
  the rule the user ruled for nullability (§13).
- **A kept result's lifetime** is the session's keep: held while read, then idle among the 128 most lately read, the
  least lately read let go first. No hold counting.
- **INTERIM, the base read inline.** Until the planner keeps a query's `let` as a step (§5.2, held for lean SQL E's
  answer, §12 answer 3), DataCube's queries read their base as written: no base on the server
  (`datacube/src/server-base.ts #make`, marked INTERIM). The checks that count bases fail by design until it lands:
  the cube lane's "the grid's base" and its per-gesture base counts, and the board lane's "the grids read N bases".
- **Found by the inline base:** a chart's bin, grouped by, lent a helper to the view after the group of what was inside
  the group's key, a helper reading columns the group had not kept; the server refused the options desk's smile chart.
  The dialect's shared subtrees now leave alone what a view reads by a name (`SharedSubtrees.read`;
  `DeephavenDialectTest.aGroupKeyReadByItsNameLendsNoHelperToTheViewAfterTheGroup`).
- **The lanes (2026-11-17, after DataCube's doors):** corpus, desk, door, feed, flat, market, `deephaven_test` and the
  live desk in the tab pass; cube and board fail on the base checks alone (above), and the cube's filter at 251 ms
  against its 250 ms budget, its 15 operations sent each time with no base on the server. One timing finding to watch:
  run beside another lane, the board's six seconds of ticks fetched two results (and, after the restart, sent 18
  operations) -- a following not yet live when the ticks began; run alone it sent and fetched nothing, both times.

### 10.3 Homework before more code (2026-11-17; H1-H3 answered the same day, from the code)

What is settled, each with a precedent in the code: the base as a `let`; the planner choosing splice or step from the
database's one fact; the verbs (create if it does not exist, under the identity; drop, the runtime's timing); a step
reading an earlier one by a binding; the planner writing the bytes; the session's requests declared by the target (as
MariaDB's raised-error read-back and H2's settings are, the charter's A3(c)). What is not, each answered before the
build resumes:

- **H1, the engine contract.** Read DataCube's engine contract whole and place Deephaven in it: the Deephaven engine
  as planner, runner and session behind `Engine`; DataCube sending only Pure. Ask its owner (the `claude/datacube-engine`
  session) and the total-wall orchestrator, and read the landing order from the plans themselves, not from a message (the
  orchestrator's "item F26" names an unrelated item on every branch read).
- **H2, following live.** The contract has no call for a result followed live; Deephaven's grids and the tab's live
  DuckDB both need one. Propose it to the contract's owner, one call for both.
- **H3, the runner.** Whether the tab's one runner (`plan-runner.ts`, today JSON rows for writes and snaps) grows the
  engine's query answer (typed rows, a window, the verbs, pipelined steps), or the engine's runner is another shape.
  Read, then measure on the parked work: two steps sent back to back cost no round trip.
- **H4, the base's identity.** On the parked work, plan the cube lane's query shapes (the tree's levels, a pivot's two
  queries, the filter's values, the row count, a window, a chart, a page filter) with the base as a `let` on
  Deephaven, and assert the base step's bytes the same in every one: the guard test, run before anything is built on it.
- **H5, the cost of a base of its own.** M1 again with the base a step: the filter measured 70 ms slower (within its
  250 ms budget) when the base was its own table; confirmed or found to be worse, on the same gestures.

**The answers (read 2026-11-17 on `claude/datacube-engine` @417a80b62, the contract and its built tab engine):**

- **H1.** The contract's tab engine answers `query` by planning with the plane's planner (`p.plan(ask.query)`) and
  running the plan on a DATABASE ADAPTER, `QueryEngine.execute(plan)` (`datacube/src/engines/tab-engine.ts` 527-545),
  a project's database chosen by its connection (`engineFor`, C5). So Deephaven is a database adapter -- a
  `QueryEngine` chosen by a Deephaven connection, as `DeephavenEngine` already is -- not an engine of its own, and the
  engine knows of it only its planner's dialect and the adapter's session. DataCube sends Pure: the base as a `let` is
  what the contract asks of it. Nothing on the contract's surfaces changes; the contract's owner is told that
  `engineFor` offers one more adapter.
- **H3.** A plan that needs more than its answer's statement already travels with it: the text plan carries the steps
  it depends on (B3.2: a target's loads, its bound values), and the host runs them before the text. A kept base is one
  more such step: the planner writes it as a SETUP step of the answer (create if it does not exist, under its identity),
  the answer statement reading it through a binding; the Deephaven adapter runs the setup through a function of the
  shared runner (`plan-runner.ts`, the verbs performed through its session) and the answer itself -- rows, a window, a
  follow -- as DuckDB's adapter runs `plan.sql`. One runner's code, no Deephaven loop, the same call on `main` today and
  behind the contract.
- **H2.** The contract has no call for a result followed live; Deephaven's following stays where it works, in the
  adapter (`QueryEngine`'s follow), until the contract adds one. The kept base does not need it. Raised with the
  contract's owner, with the tab's live DuckDB beside it.
- **H4, H5** are the build's first measurements: the identity guard before anything is built on the planner's step, the
  gestures' times once the lanes run; either failing stops the build and comes back here.

## 11. The order of work

(2026-11-17: steps 1-3 done, and step 4's runtime, doors and witnesses built. The rest, in order, after §10.3's answers:
the planner's kept `let` as a setup step with the identity guard (H4); the shared runner's setup function and the
Deephaven adapter running through it, the steps packed inside a statement deleted; DataCube's base as a `let`,
`server-base.ts`'s interim and fragment deleted; every lane, the cube's and the board's base checks on their merits
(H5); then step 5.)


1. Merge `main` (`4dfbb5b7`: C5-P, C5-J, C5-T; 85 commits, about 18 conflicts), the Deephaven TypeScript placed in
   C5-T's layers. Checkpoint with the user.
2. M5, then M1 and M2. Checkpoint with the user.
3. The planner: steps as bytes with slots, identity and verbs; the session's declared requests; the Deephaven type
   on columns; a query's top-level relation `let` kept as a step on Deephaven; the encoder and the planning parts of
   `DeephavenWire` moved to the planner's side; the rule's sentence in A3 and invariant 3; Java tests.
4. The browser runtime in a run layer; every send in a run; the door narrowed and closed; DataCube writing the base as a
   `let`, its fragment, second planner, name swap and fallback deleted; the witnesses; every Deephaven lane green.
5. A second independent review, of the built code. The surfaces and layers registered, every lane run, and the
   landing (§12, answer 6), on the user's word.
6. Then the Java runtime, its own PR.

## 12. The total-wall orchestrator's answers this follows (2026-11-16, pasted by the user)

1. Encode on the run side, from plan data only, type-checked against the plan's types, with a once-per-session
   establishment; never keep the planner's door open to the run. **This design goes further:** the run side encodes
   nothing (§4.2) -- told to the orchestrator.
2. Deephaven lands before DataCube's single API (`claude/datacube-engine`).
3. Lean SQL changes shared types: `DeephavenBatch` gives arms to the new `SqlExpr` variants, `SqlSource.RowValues` and
   `readsRowLets` when E lands; the WITH reuse is not extended. **Asked 2026-11-16:** whether a Deephaven target keeping
   a query's top-level relation `let` as its own step fits E's let rule, or should ride on E's representation.
4. lite's protocol extensions are product, under lite's names and `_type` classifiers.
5. Each new door registered with a dated note; the wire codec is not a door; plan data (`PLAN_DATA`) is fine; a "what
   is sent is the plan's" witness.
6. The plan version is assigned at landing; landing is a staging branch, an independent review, a SQL lane round, one
   PR and one CI run.

## 13. Rulings (the user, 2026-11-16)

- The Java runtime in the PR right after v0's ("yes right after"); Python later ("yes python later").
- The planner writes the exact bytes and no runtime encodes; the plan carries Deephaven's own type names ("Agree with
  decision 2 and decisions 3"). Nullability comes from the planner's own types: the separate question and the server's
  answer go, and no copy is added to the plan's result columns until something reads it.
- Every kept table is a planned step ("should be part of the plan not a fallback").
- A grid's base written as a `let`, the planner's, kept as a step on Deephaven, in place of a request field or a named
  table.
- Declared verbs: create if it does not exist and drop, a session table named by its definition's hash; the session's
  requests declared once a plan; the one-sentence rule (§2) in place of a list of special acts.
- The readable form beside the bytes.
- The lake and history last, as a phase of their own (docs/DEPLOYMENT_DESIGN_2026_11_14.md §15.7).
- "Let's go."
