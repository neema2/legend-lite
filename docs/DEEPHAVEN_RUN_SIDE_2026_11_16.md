# Deephaven on the run side: the design (2026-11-16)

**Status: RULED by the user on 2026-11-16 ("Let's go"); being built on `claude/deephaven-3-browser`.** Written once,
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
  driver, TypeScript's `plan-runner.ts` over DuckDB-WASM, Python's `_runs.py` (on `main`) over `duckdb`.
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

A Deephaven plan is a chain of one or more steps, each one table the server makes, sent one request per step and
pipelined (each step's request goes as soon as the one before it is written: a read of a table the server is still
making waits for it there, as DoGet-on-sent does today). A step carries:

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
  made by the planner. It is the session table's name.
- **Its verbs:** create if it does not exist (the step's request, sent unless the session holds the identity), and drop
  (a release of its export). A kept step is dropped when its lifetime ends; a step that is not kept is dropped once
  the plan's result no longer needs it.
- **`kept`**: whether its table outlives the plan (§6).
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
- **On Deephaven** a query's top-level `let` of a relation is kept whole: lowered as a WITH expression that is never
  spliced, pruned or folded into the body, which `DeephavenBatch` (already writing a WITH expression's operation once,
  `DeephavenBatch.with`) writes as a step of its own: stored (`select`, `update` where it ends in `view`,
  `updateView`; a base ending in `updateBy`, a window column, is stored already), kept, read through a slot. The
  target's capability, as the dialects' others are; a `let` a person writes in their own Deephaven query is kept the
  same way (bounded by the keep).

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

## 11. The order of work

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
