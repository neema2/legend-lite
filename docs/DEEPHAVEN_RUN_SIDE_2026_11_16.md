# Deephaven on the run side: the design (2026-11-16)

**Status: PROPOSED, for an independent review and then the user's word; nothing is built.** Rewritten once, whole, on
the user's request of 2026-11-16 after a day of answers that changed it piece by piece; the earlier versions are in git
history. Its rulings are §12. It changes the run side of docs/DEEPHAVEN_PLAN_2026_10_22.md (DH11, DH22, DH23, B1) so the
Deephaven road meets the plan/exec split C5 enforces on `main` (G4), in its spirit as well as its letter.

## 0. In one paragraph

A Deephaven plan is a short chain of steps, each a table the server makes, each written by the planner as the exact
bytes the server reads, with a few four-byte slots for the numbers only a session knows. A runtime fills the slots,
skips a step its session already holds, sends, checks the answer against the plan's types and reads the rows -- and
does nothing else: it is given no planner, and it encodes no query. A grid's base is not a table anyone declares but a
way of running the grid's queries, so it is the planner's: DataCube sends each query as its base and a view over it, on
every backend; on DuckDB the planner joins the two into one statement, as today; on Deephaven it makes the base a step
the session keeps and the view a step that reads it. One plan format, a small runtime per environment -- the browser's
for v0, Java's right after it (for PCT), Python's later -- as DuckDB already runs.

## 1. What is wrong today (read from the code)

The browser reaches the planner through one door, `Boundary.deephaven(op, args)` (`wasm/src/main/java/planner/
Boundary.java`), 13 operations and no `only(...)` closure:

| Operation | Asked by | When | What it is |
|---|---|---|---|
| `topics`, `plan`, `validate` | the feed, deploying | before any run | planning |
| `holds` | connecting (`deephaven-grids.ts`) | before the check's run | planning |
| `nullable` | a base being made (`server-base.ts #make`) | before the base is sent | planning |
| `stored` | a base being made (`deephaven.ts stored`) | while it is made | a planning rewrite (view to select) at run |
| `keys` | every query and base (`keepAlive`) | inside the run | hashing the plan at run |
| `send` | every query | inside the run | cutting the plan by what the session holds |
| `made` | every query | inside the run | decoding and checking the answer (DH16) |
| `releaseExport`, `doGet`, `publish` | the session | inside the run | encoding a request |
| `batch` | nothing in the browser | -- | encoding |

Four things are wrong:

1. **The run asks the planner** three times a query (`keys`, `send`, `made`) and for each release, read and publish.
   It passes in the browser only because the run frame cannot see there (`engine-client/src/run-frame.ts`: "where no
   async context exists -- a browser tab -- ... the witness sees nothing there: a gap, named"); under Node, `main`'s
   guard (`plan-guard.ts planning`) refuses each one. A base's making is not even marked as a run
   (`ServerBaseRunner.#hold` sends it before the query's `inRun` opens).
2. **The plan is edited at run.** `DeephavenWire.send` drops the operations the session holds and rewrites the
   references to them. `docs/TENET_CHARTER.md` A3 allows exec three things beyond sending a plan as written (keyed
   regions, planned branches, declared read-backs); this is none of them.
3. **A base is typed by the server's answer.** `server-base.ts #make` declares the base's table from the column types
   the server reports (`columnType(c, held)`) and every later query is planned against that declaration. Types are the
   frontend's (AGENTS.md invariant 1); the server's answer is checked against them (DH16), never read to plan.
4. **A query finds its base by name at run, and the model carries session state.** DataCube declares each base as a
   table of a model fragment (`legend::Bases`), plans queries against it, and the session swaps the table's name for
   its own ticket as it sends (`DeephavenEngine.#supplied`, `DeephavenWire.suppliedTicket`). Each new base changes a
   model, which is why the bases have a second planner and model of their own (`ServerBaseOptions.connectionOf`: one
   2.3 MB model held again for each new base took 4.4 s, 2026-10-25), and why a base's nullability had to be asked of
   the planner at all: only to write its declaration.

## 2. The rules this design follows

- **R1, the plan says everything.** A runtime may do these and nothing else: (1) allocate its session's handles and
  write them into the slots the plan lists; (2) skip a step whose identity its session holds; (3) send a step as
  written, its slots filled; (4) decode the server's answer; (5) check it against the plan's types, refusing by name a
  table made otherwise (DH16); (6) read rows (Arrow Flight's DoGet) and, in the browser, follow them live (the server's
  own JS API, `deephaven-live.ts`); (7) release what it holds, and publish a held result under a fresh name for the
  live reader. No hashing, cutting or rewriting a plan, no choosing an operation, no reading a type to plan with.
- **R2, a way of running a query is the planner's, and lives in the plan; a model describes declared data.** Session
  state never enters a model. (The user's ruling of 2026-11-16 on kept tables: "should be part of the plan".)
- **R3, one plan format, a runner per environment** -- as DuckDB runs today: Java's `PlanRunner` over DuckDB's JDBC
  driver, TypeScript's `plan-runner.ts` over DuckDB-WASM, Python's `_runs.py` (on `main`) over `duckdb`.
- **R4, the planner writes the exact bytes a server reads**, as a SQL dialect writes the exact text a database reads.
- **R5, types are the frontend's;** a server's answer is checked against them.

## 3. The plan's Deephaven contract

### 3.1 Steps

A Deephaven plan is a chain of one or more steps, each one table the server makes. A step carries:

- **`request`**: the step's `BatchTableRequest` as the bytes the server reads. The dialect writes the operations as it
  does today (`DeephavenBatch`); the planner's encoder (`ProtoWire` over the message table, §6.4) writes them in the
  protocol's binary form. Within a step, an operation reads an earlier one by `batchOffset`, and a server's own table by
  its scope ticket (`s/NAME`, a deployment's tables): fixed bytes. Only the step's last operation is exported.
- **`slots`**: the byte positions where the runtime writes its session's export ids: the step's own result (its last
  operation's `resultId`), and each earlier step it reads. An export ticket is `e` and a four-byte number
  (`DeephavenWire.exportTicket`), so a slot is four bytes at a fixed place and filling it changes no length the encoder
  wrote. Filling a slot is binding a value, as a SQL plan's `?` is bound. A run-time parameter (none is sent yet: DH5)
  is uploaded as a one-row table and read by ticket: one more slot of the same kind.
- **`identity`**: SHA-256 of the step's bytes with its slots zeroed and each step it reads named by that step's
  identity, made by the planner. Two plans that build the same table name it alike, so one plan can find a table
  another plan's step made.
- **`kept`**: whether the session keeps the step's table for later plans (§5).
- **`columns`**: the step's columns, as every plan has them (§3.2).
- **`declared`**: the store tables the step reads, as their store declares them (DH16, unchanged).

The readable form of a step (today's JSON) stays the planner's: a test or an error message asks the planner for it,
never the run.

### 3.2 The columns: the three types, and two facts added

Every plan column already carries three types (`ExecutionPlan.Column`): its Pure type and multiplicity, its store's
declared type when it is a store column read through, and its database type (`slot`, lite's neutral name of the type
the database answers with). Two facts are added to the database type:

- **Whether the database can return a null there.** The multiplicity is the model's promise, as upstream types it: a
  `[1]` property read across a join that finds no row comes back null all the same. Measured on the branch during the
  merge of `main`: the plan's types gave the wrong answer for 227 of the 315 base columns the lanes make, the
  lowering's own flag (`OutputCol.nullable`) the right one for all 315. The lowering knows it; the plan now carries it.
- **The type as the database spells it.** Deephaven answers in Java types (`long`, `java.lang.String`); the mapping is
  the dialect's (`DeephavenBatch.javaTypeOf`), so the dialect writes it and no runtime maps a type.

The nullability fact is written for every plan, not only Deephaven's: a plan format change, its version assigned at
landing (§11, answer 6).

## 4. A grid's base

### 4.1 What it is

A grid's base (B1) is its source and its leaf-level calculated columns (`query.ts sourceWithDerived`), the part every
query of the grid begins with. It is not data anyone declared; it is a way of running the grid's queries. On DuckDB
lite has none: each query is planned whole and the database optimizes it (`server-base.ts`: "the planner folds a
calculated column into each query that reads it, as every SQL database wants"). On Deephaven a followed result
recomputes its joins and calculated columns on every tick, so the base is computed once on the server and every view
reads it.

### 4.2 What DataCube sends: the base and the view, on every backend

DataCube sends each grid query in two parts: the **base**, a lambda (`sourceWithDerived`), and the **view**, a lambda
of one parameter, `base`, over it -- what the query does after its base (filter, group, pivot, sort, limit). The
planner types `base` from the base. DataCube already holds the two apart (`ServerBaseOptions.baseOf`) and already
substitutes one node for another (`runner.ts rebased`): the view is the query with the base's node replaced by the
variable. Every planning question DataCube asks of a based query -- a plan, a window's, a stream's, its columns
(`relationType`), its compile -- carries both.

**A public change, named:** the query planning request (`planJson` and its siblings) gains an optional `base`; with
it, `lambda` is the view. Registered in `PlannerSurfaceTest` with a dated note.

### 4.3 What the planner does with them

- **On a SQL database** it composes the two -- the base's expression where the view reads `base` -- and plans one
  statement: the same SQL as today, so DataCube's queries on DuckDB do not change.
- **On Deephaven** it plans the base as a step (`kept`), and the view as the next step, reading the base through a
  slot. Its columns are typed from the base's own planning, nullability included, so nothing is declared and nothing
  is asked of the server.

**Inside the planner:** one new kind of source in the IR, the table an earlier step of the plan made, typed by that
step's outputs (`SqlSource`, sealed: a new variant, an arm in every dialect). The Deephaven dialect writes it as a
slot. A SQL dialect's arm refuses it by name: its planner composes the base and never hands it one. Lite has nothing
of the kind today -- a `let`'s value is spliced at every read (`SharedRelations`), a program's frames are pasted where
read (`ProgramFrames`) -- and legend-engine's plans have the pattern (a node reading another node's result as a plan
variable; lite keeps its print-only shape, `SqlSource.VarSetPlaceholder`).

### 4.4 What the runtime and DataCube do

- The runtime skips the base step while its session holds the base's identity, and makes it otherwise; after a
  reconnect it makes it again from the same plan. Nothing is planned again.
- DataCube holds a base by its identity while a grid reads it and releases it when none does, as `ServerBases` counts
  its holders today. Grids and charts with the same base share it, because the same base has the same identity.
- **Removed from DataCube:** the bases' model fragment (`legend::Bases`), their second planner and model
  (`connectionOf`), the base's substitution by a declared table (`ServerBaseRunner`'s use of `rebased`; Snap's own copy,
  `runner.ts rebased` for a snapped grid, is unchanged), `keepBase`, `stored`, the `nullable` question and the name
  swap. `ServerBases` keeps what is DataCube's: which base each grid reads, and when none does.

### 4.5 Why not a named table, real or pretend

- **A pretend table** (today) puts session state into a model, types the base by the server's answer and swaps a name
  at run (§1, points 3 and 4).
- **A real named table** (the base published under a server-wide name, read by name as any table) still puts session
  state into a model -- every new base a model change, the second planner kept -- still types the base from outside the
  planner (the declaration needs its types), and adds a connection setting for temporary tables and names every
  session on the server can see. It was considered on 2026-11-16 and is not recommended.
- **A named table is right for something else:** a dataset the model's author declares, shared and lasting -- a
  materialized view, deployed (docs/DEPLOYMENT_DESIGN_2026_11_14.md §5).

### 4.6 In familiar terms

A kept step is a temp table (`CREATE TEMP TABLE b AS SELECT ...`) that Deephaven keeps live: the server keeps it current
as its source ticks, so keeping it for the session is safe there. Finding a kept table by its plan's identity is
Spark's cache (`cache()` marks a result; any later plan holding the same fragment reads it). The skip rule is the seeds'
memo (`ExecutionPlan.Load.Cadence.BEFORE_STEP`: skipped where the session holds the identity). It is not a `Load`: its
rows come from tables the server holds, not from outside, so the data-in-the-plan rule stays whole.

## 5. Reuse: every table kept is a step of the plan

The user's ruling (2026-11-16): "If we want to be able to cache/build intermediate tables off base tables that's fine,
but then should be part of the plan".

- **Every step follows the same two rules:** skipped while the session holds its identity; else sent, its slots
  filled. A base, a table kept above it and a query's result differ only in whether the plan marks them `kept`.
- **Which tables are kept is decided before the run**, by the planner, with DataCube saying what its queries share (the
  base). The runtime never chooses, cuts or rewrites.
- **v0 keeps the base and each query's result.** A result kept means closing a group sends nothing
  (`datacube/test/deephaven/cube.mjs`). A table between them -- the grid's filtered base, which every level of its tree
  reads -- is kept when M1 shows a gesture needs it: a change to what the plan keeps, never a second mechanism.
- **Why not every operation, as today:** each table a session keeps alive keeps ticking on the server
  (`deephaven.ts`: "an idle table still ticks on the server, so they are not free"); the plan keeps what is shared and
  worth that cost.
- **How long:** a base while a grid holds it; a result while read and then idle within the session's keep (128 today),
  the least lately read let go first. A step let go is made again from the same plan when next needed: letting go
  changes when the server works, never what an answer is.

## 6. One runtime per environment

### 6.1 The browser, TypeScript (v0)

- **Where:** a run layer of `tools/deps/ts-layers.txt` (C5-T), reaching the records and npm packages only
  (`TsLayeringTest`). It is constructed with no planner, so it has none to ask: proven by its type, as Java's session
  statements take a `PlannedSql` and no `String`.
- **What it keeps from today:** `engine-client/src/deephaven.ts`'s transport (gRPC-web over the page's `fetch`,
  sign-in, token refresh, the session's keep), rows by DoGet read with apache-arrow, the live reader.
- **What it gains:** filling slots; the session's three fixed requests (release an export, publish one under a shared
  name, the ticket a row read names: a ticket each); the batch answer's reader (each export's success, error and
  schema) and the DH16 check against the plan's columns. It encodes no query.
- **The run frame:** every Deephaven send inside `inRun`, a base's included.

### 6.2 Java, in the PR right after v0's (for PCT)

`com.legend.deephaven` (`//core:deephaven_wire`) splits: its encoder (`ProtoWire`, `DeephavenMessages`) goes to the
planner's side, where a step's bytes are made; its session part (the requests, the answer's reader, `ArrowSchema`)
moves into exec as Java's Deephaven runtime, registered with the acts it performs as the JDBC census registers its
classes. Its planning parts (`keys`, `stored`, `send`'s cut) go. Transport: gRPC-web over the JDK's `HttpClient`, no
gRPC library (M4). Then the PCT lane on a Deephaven runtime.

### 6.3 Python, later

The same contract and no encoder of its own: the slots, the session's requests and the answer's reader.

### 6.4 Kept identical

- **One message table, the planner's alone:** `DeephavenMessages` (the fields, numbers and types of the subset lite
  writes, checked against the pinned server's `.proto` by `DeephavenMessagesTest`), read only by the planner's encoder.
- **Shared fixtures:** recorded cases -- a plan, a session's handles, the server's answers -- with the bytes each
  runtime must send and the result it must read; recorded from the pinned server by a lane and run by every runtime.

## 7. The doors

`Boundary.deephaven` keeps its planning operations -- `topics`, `plan`, `validate`, `holds` -- each closed by
`only(...)` over the keys it reads and registered in `PlannerSurfaceTest`. The other nine go: `nullable`, `stored`,
`keys`, `send` into the plan; `made`, `batch`, `releaseExport`, `doGet`, `publish` into the runtime. The query
planning request's `base` (§4.2), `ingestPlans` and the lite route `/api/lite/v1/grammar/grammarToJson/model` are
registered the same way.

## 8. The witnesses

- **What is sent is the plan's:** a test captures every request a runtime sends and compares it with the step's
  `request`, byte for byte, only the listed slots differing -- Deephaven's twin of
  `MongoDbPlanTest.theCommandSentIsThePlans_itsParametersBoundByTheLet`.
- **No planner in a run:** the runtime takes no planner; the door's run-side operations are deleted; a Node test runs
  a cube's gestures through the runtime under the run frame (`frameKept` true) and counts no planner entry inside a run.
- **The runtime is a run layer:** `ts-layers.txt`; in Java, `execReachesNoPlanning`.
- **Types are the plan's:** no code path reads a server's types to plan; a server holding a column otherwise than the
  plan says is refused by name.
- **No session state in a model:** DataCube's based queries change no model (the planner's model handle is the same
  before and after a new base).

## 9. Measured before it is built on

- **M1, reuse:** v0's kept steps (the base and each result) against today's per-operation reuse, on the cube lane's
  gestures (the regroup and the filter within 250 ms, closing a group within 150 ms with nothing sent) and the desk
  lane's opening and warm times (`market.mjs`). A missed budget is a defect to find, never a raised budget.
- **M2, types:** for every step the lanes make, the server's Java type and nullability equal the plan's.
- **M3, a step reading another made in the same request:** the pinned server accepting an operation that reads, by
  ticket, an export an earlier operation of the same request makes. If not, the runtime sends a plan's missing steps
  in order, one request each (more round trips on a first build; the plan unchanged).
- **M4, Java's transport:** the JDK's `HttpClient` speaking gRPC-web to the pinned server (a probe), before the Java
  runtime is promised.
- **M5, the view over its base:** a spike typing and lowering every DataCube query shape as a view over a base (the
  tree's levels, a pivot's two queries, the filter's values, the row count, a window, a chart) through the new source,
  on Deephaven and composed on DuckDB, with DuckDB's SQL unchanged. The design's one real implementation risk.
- **M6, size:** the browser runtime's bytes, under the bundle budgets (`bundle-budget.test.ts`).

## 10. The order of work

1. This document reviewed (independently, then by the user) and ruled.
2. Merge `main` (`4dfbb5b7`: C5-P, C5-J, C5-T; 85 commits, about 18 conflicts), the Deephaven TypeScript placed in
   C5-T's layers.
3. M5 (the spike), then M1, M2, M3.
4. The planner: steps as bytes with slots and identity; the columns' two facts; the view over its base and the new
   source; the encoder and the planning parts of `DeephavenWire` moved to the planner's side; Java tests.
5. The browser runtime in a run layer; the door narrowed and closed; DataCube's based queries sent as base and view, its
   fragment, second planner and name swap deleted; the witnesses; every Deephaven lane green.
6. The surfaces and layers registered, every lane run, and the landing (§11, answer 6), on the user's word.
7. Then the Java runtime, its own PR.

## 11. The total-wall orchestrator's answers this follows (2026-11-16, pasted by the user)

1. Encode on the run side, from plan data only, type-checked against the plan's types, with a once-per-session
   establishment; never keep the planner's door open to the run. **This design goes one step further:** the run side
   encodes no query at all (§3.1), only the session's three requests -- to be told to the orchestrator.
2. Deephaven lands before DataCube's single API (`claude/datacube-engine`).
3. Lean SQL changes shared types: `DeephavenBatch` gives arms to the new `SqlExpr` variants, `SqlSource.RowValues` and
   `readsRowLets` when E lands; the WITH reuse is not extended. The new source of §4.3 needs an arm in E's dialects.
4. lite's protocol extensions are product, under lite's names and `_type` classifiers.
5. Each new door registered with a dated note; the wire codec is not a door; plan data (`PLAN_DATA`) is fine; a "what
   is sent is the plan's" witness.
6. The plan version is assigned at landing; landing is a staging branch, an independent review, a SQL lane round, one
   PR and one CI run.

## 12. Rulings and questions

- **Ruled 2026-11-16:** the Java runtime in the PR right after v0's ("yes right after"); Python later ("yes python
  later"); the planner writes the exact bytes and no runtime encodes, and the database type gains its nullability and
  its own spelling ("Agree with decision 2 and decisions 3"); every kept table is a planned step ("should be part of
  the plan not a fallback").
- **For the user's word:** the base as the planner's (§4) -- DataCube sends base and view on every backend, the query
  planning request gains `base`, the planner gains the new source -- in place of a named table (§4.5).
