# Deephaven on the run side: the plan as data, one driver per runtime (2026-11-16)

**Status: PROPOSED, for review with the user before any code. Nothing here is built.** It changes how the browser
runs a Deephaven plan (docs/DEEPHAVEN_PLAN_2026_10_22.md DH11, DH22, DH23, B1), so that the Deephaven road meets the
plan/exec split C5 enforces on `main` (G4), in its spirit and not only its letter. It came out of the user's questions
of 2026-11-16 ("plan vs exec split does not mean we have to split languages ... a plan that is data and then multiple
runtimes like Java, TypeScript, Python that can run the plan without doing things the planner is supposed to do"; "V0
can be browser only for the exec side but we should make sure we have a design/path for all"; "we will want Java very
quickly to run PCT") and the total-wall orchestrator's answers of the same day (§8).

## 0. In one paragraph

The planner writes everything a Deephaven server is sent, as the exact bytes the server reads -- as a SQL dialect
writes the exact text a database reads: each query as one batch of operations, with a few listed slots for the
numbers only a session knows (export ids, four bytes each, so filling one moves nothing else); its columns with the
three types every plan carries, the database's now saying whether it may hold a null; and each grid's base as a step
of its own, made once per session and skipped while the session holds it (a temp table, live on Deephaven). Each
environment has a small Deephaven driver that fills the slots, sends, checks the answer against the plan's types and
reads the rows: TypeScript in the browser for v0, Java's exec right after (for PCT), Python later -- the way DuckDB
already runs: one plan format, a runner per environment. No driver asks the planner anything, and none can: it is
given no planner. No driver encodes a query either: only the planner knows Deephaven's message layouts.

## 1. Why: what the run asks the planner today, measured

The browser's run reaches the planner through one door, `Boundary.deephaven(op, args)`
(`wasm/src/main/java/planner/Boundary.java`), which multiplexes 13 operations with no `only(...)` closure:

| Operation | Asked by | When | What it is | After this design |
|---|---|---|---|---|
| `topics` | the world's feed | before any run | planning | stays a planning operation |
| `plan` | deploying | before any run | planning | stays |
| `validate` | deploying | before any run | planning | stays |
| `holds` | connecting (`deephaven-grids.ts`) | before the check's run | planning | stays |
| `nullable` | a base being made (`server-base.ts #make`) | before the base's send | planning | folded into the base's plan (§3.2) |
| `stored` | a base being made (`deephaven.ts stored`) | while the base is made | a planning rewrite (view to select) done at run | the dialect writes the base stored (§3.2) |
| `keys` | every query and every base (`keepAlive`) | inside the run | hashing the plan at run | the plan carries its identity (§3.1) |
| `send` | every query | inside the run | cutting the plan by what the session holds | gone: the batch is sent as written (§3.3) |
| `made` | every query | inside the run | decoding and checking the answer (DH16) | the driver's (§4) |
| `releaseExport`, `doGet`, `publish` | the session | inside the run | encoding a request | the driver's |
| `batch` | nothing in the browser | -- | encoding | deleted (its work is the driver's) |

Four things are wrong, and only the first is the door:

1. **The run asks the planner, three times a query** (`keys`, `send`, `made`) and again for each release, read and
   publish. In the browser this passes only because the run frame cannot see there (`engine-client/src/run-frame.ts`:
   "where no async context exists -- a browser tab -- the frame is not kept and the witness sees nothing there: a gap,
   named"). Under Node, `main`'s guard (`plan-guard.ts planning`) refuses every one. A base's making is not even marked
   as a run: `ServerBaseRunner.#hold` sends the base before the query's `inRun` opens.
2. **The plan is edited at run.** `DeephavenWire.send` drops the operations the session already holds and rewrites
   the references to them as tickets. `docs/TENET_CHARTER.md` A3 lets exec do three things beyond sending a plan as
   written (keyed regions, planned branches, declared read-backs: AGENTS.md invariant 3); cutting a plan by what a
   session holds is none of them. Porting `send` to TypeScript would move this, not fix it.
3. **A base is typed by the server's answer.** `server-base.ts #make` declares the base's table from the column types
   the server reports (`columnType(c, held)`), and every later query of the grid is planned against that declaration:
   the server's answer feeds the planner. Types are the frontend's (AGENTS.md invariant 1); the server's answer is
   checked against them (DH16, decided strict), never read to plan.
4. **A query finds its base by name, during the run.** The query reads the base as if it were a server's table,
   `s/legend_base_<key>` (declared in DataCube's model fragment `legend::Bases`), and the session swaps that name for
   its own ticket as it sends (`DeephavenEngine.#supplied`, `DeephavenWire.suppliedTicket`): a run-time rewrite keyed
   on a name. The planner does not know the table is the session's base, so it cannot mark the place as a slot.

## 2. The model: DuckDB's pattern

lite already runs DuckDB this way: one plan format (version 51: Java `PlanJson.VERSION`, TypeScript `PLAN_VERSION`,
Python), and a runner in each environment using DuckDB's own client there -- Java's `PlanRunner` over DuckDB's JDBC
driver, TypeScript's `plan-runner.ts` over DuckDB-WASM, Python's `_runs.py` (on `main` since C5-P) over the `duckdb` package. Deephaven is the
same: the plan says everything, and each environment's driver carries it.

**What a Deephaven driver may do -- the whole list:**

1. Allocate its session's handles (export ids) and write them into the slots the plan declares.
2. Skip a step whose identity its session already holds (the seeds' rule, below).
3. Send the plan's batch as written, its slots filled.
4. Decode the server's answer.
5. Check the answer against the plan's types: a table made or held otherwise than the plan says is refused by name
   (DH16).
6. Read rows (Arrow Flight's DoGet) and, in the browser, follow them live (the server's own JS API, loaded from the
   server at run time, `deephaven-live.ts`).
7. Release what it holds, and publish a held result under a fresh shared name for the live reader.

Nothing else: no hashing a plan, no rewriting one, no choosing an operation, no reading a type to plan with.

## 3. The plan's Deephaven contract (what the planner writes)

### 3.1 A query: one batch, the bytes the server reads

Today a Deephaven statement in a plan is a batch of operations in the protocol's JSON form (proto3 JSON of
`BatchTableRequest`, written by `DeephavenBatch`), which the planner's door turns into bytes during the run. The plan
carries the bytes instead, as a SQL plan carries the exact text a database reads:

- **`request`**: the `BatchTableRequest`'s bytes, rendered by the planner -- the dialect writes the operations as
  today, and the planner's encoder (`ProtoWire` over the message table, §4.4) writes them in the protocol's binary
  form. Inside the batch an operation reads an earlier one by `batchOffset`, and a server's table by its scope ticket
  (`s/NAME`, the deployment's tables): fixed bytes. Only the result is exported.
- **`slots`**: the byte positions where the driver writes its session's numbers -- the result's export id (the last
  operation's `resultId`) and each grid base's export id (§3.2). An export ticket is `e` and a four-byte number
  (`DeephavenWire.exportTicket`), so a slot is four bytes at a fixed place, and filling it changes no length the
  encoder wrote. That is binding a value, as a SQL plan's `?` is bound; the driver fills the listed slots and nothing
  else. A run-time parameter (none is sent yet: DH5) is uploaded as a one-row table and read by a ticket, so it is a
  slot like the others.
- **`columns`**: the plan's columns, as every plan has them, with their three types (§3.4).
- **`declared`**: the store tables the batch reads, as their store declares them (DH16, unchanged).
- **`identity`**: the statement's digest, SHA-256 of its bytes with its slots zeroed, made by the planner. It names the
  result for reuse (§3.3), where `keys` hashed the plan during the run.

The readable form, today's JSON, stays the planner's: a test or an error message asks the planner for it, never the
run.

### 3.2 A grid's base: a session table

A grid's base (B1: the grid's source and its leaf-level calculated columns, made once and read by every query the
grid sends and every result it follows) becomes a plan step of its own:

- **What the planner writes:** the base's identity; its batch already in stored form (`select` and `update` where the
  query is `view` and `updateView`, so the server computes each column once a change and keeps it -- the rewrite
  `DeephavenWire.stored` does at run today, done by the dialect), as bytes with its result's slot (§3.1); and its
  columns with their three types (§3.4), replacing the `nullable` question and the server-typed declaration of §1,
  point 3.
- **What the driver does:** if its session holds the identity, nothing is sent; else the batch is sent, its columns
  checked against the plan's, and the identity's export remembered. It is kept until released (no grid reads it) or
  the session ends; after a reconnect the same step is run again -- nothing is planned again.
- **What it is, in familiar terms:** a temp table (`CREATE TEMP TABLE b AS SELECT ...`), live on Deephaven: the server
  keeps it current as its source ticks, so keeping it for the session is safe there (on a SQL database the same reuse
  would go stale, so a base there would be a view or remade on change; no SQL target uses bases in v0). The skip rule is
  the seeds' memo (`ExecutionPlan.Load.Cadence.BEFORE_STEP`: skipped where the session holds the identity). It is not
  a `Load`: its rows come from a query over tables the server already holds, not from outside, so the data-in-the-plan
  rule stays whole. It is not a materialized view either: a materialized view is the model's (declared, shared,
  deployed: docs/DEPLOYMENT_DESIGN_2026_11_14.md §5); a base is one grid's, private and temporary. A base many people
  open is a sign it should be declared as a materialized view -- the author's call.
- **A new plan node.** Its name and place in the plan format are agreed with the plan format's owner (the total-wall
  orchestrator); the plan version number is assigned at landing (§8, answer 6).
- **How a query reads its base: planned with it.** The base and a query over it are planned together, the query as a
  function of its base: the base its typed input, a lambda, and the query a lambda over it. The plan then holds the
  base step and the query's step, and the query reads the base through a plan variable -- legend-engine's own pattern
  for one plan node reading another's result (lite keeps its shape in the IR, `SqlSource.VarSetPlaceholder`, plan text
  only today) -- which the Deephaven dialect writes as a ticket slot naming the base step. Nothing is found by name
  during a run (§1, point 4).
- **What goes from DataCube:** the model fragment it writes for its bases (`legend::Bases`, a `Table` per base,
  declared from the server's answer: §1, point 3), the second planner and model it holds so a new base does not
  re-hold the page's model (`ServerBaseOptions.connectionOf`: the trader screens' 4.4 s, 2026-10-25), and the name swap.
  `ServerBases` keeps what it decides -- which base each grid reads, and when no grid reads one -- and DataCube's
  `rebased` seam writes the query over the base's parameter instead of over a declared table.
- **A public interface change, named:** one planning entry takes the base as well as the query (the query's planning
  entry gains the base lambda; its name and shape settled with the C5 surface's rows, `PlannerSurfaceTest`).

### 3.3 Reuse: every table kept is a step of the plan

The user's rule (2026-11-16): "If we want to be able to cache/build intermediate tables off base tables that's fine,
but then should be part of the plan". So a plan is a short chain of steps, each a table on the server:

- **Every step has the same shape:** its bytes, its slots (its own result's export id, and the export id of each step
  it reads) and its identity -- the digest of its bytes with each step it reads named by that step's identity, so two
  plans that build the same table name it alike. Within a step, operations read each other by `batchOffset`, fixed;
  between steps, by ticket, a slot.
- **Every step follows the same two rules:** skipped while the session holds its identity; else sent, its handles
  filled. The steps of one plan the session lacks go in one request. Nothing else differs between a base, a table kept
  above it and a query's result.
- **Which tables become steps is decided before the run**, by the planner, with DataCube saying what its grid's
  queries share -- as it names the base today. The runtime never chooses, cuts or rewrites; it only skips what it holds.
- **v0's steps:** the grid's base, and each query's result (which keeps the cube lane's check that closing a group
  sends nothing, `datacube/test/deephaven/cube.mjs`). A table between them -- the grid's filtered base, which every
  level of its tree reads -- becomes a step when M1 shows a gesture needs it: a change to what the plan cuts, measured,
  never a second mechanism.
- **Why not keep every operation, as today:** each table a session keeps alive keeps ticking on the server
  (`deephaven.ts`: "an idle table still ticks on the server, so they are not free"), so a kept table costs the server
  for as long as it is held. The plan keeps what is shared and worth that cost; and today's per-operation reuse is
  decided at run, by cutting the plan (§1, point 2), which is what this replaces.
- **How long a step is kept:** a base, while a grid reads it; a result or a table above the base, while read and then
  idle within the session's keep (128 today), the least lately read let go first. A step let go is made again from the
  same plan when next needed: letting go changes no answer, only when the server works.

### 3.4 The three types, and the two facts the database's lacks

Every plan column already carries three types (`ExecutionPlan.Column`): its Pure type and multiplicity; its store's
declared type, when it is a store column read through; and its database type (`slot`, the type the database answers
with). Deephaven needs nothing new in kind. The database type lacks two facts:

- **Whether the database can return a null there.** The multiplicity is the model's promise, as upstream types it: a
  `[1]` property read across a join that finds no row comes back null all the same. Measured on the branch during the
  merge of `main`: the plan's types gave the wrong answer for 227 of the 315 base columns the lanes make, and the
  lowering's own flag (`OutputCol.nullable`) the right one for all 315. The lowering knows it; the plan does not carry
  it, which is why the run asks the planner `nullable` today.
- **The database type as the database spells it.** `slot` is lite's neutral name (`BIGINT`); Deephaven answers in Java
  types (`long`), and the mapping is the dialect's (`DeephavenBatch.javaTypeOf`), so the dialect writes it into the
  plan and no driver maps a type.

## 4. One driver per runtime

Every driver does §2's list and nothing else, from the same plan.

### 4.1 The browser, TypeScript (v0)

- **Where:** a run layer of `tools/deps/ts-layers.txt` (C5-T), reaching the records and npm packages only
  (`TsLayeringTest`). It is constructed with no planner, so it has none to ask: the separation is proven by its type,
  as Java's session statements take a `PlannedSql` and no `String`.
- **What it keeps:** today's transport in `engine-client/src/deephaven.ts` (gRPC-web over the page's `fetch`, sign-in,
  token refresh, the session's keep), rows by DoGet read with apache-arrow, the live reader (`deephaven-live.ts`).
- **What it gains, and what it does not:** it fills the slots and frames the request for gRPC-web; it writes the
  session's own few requests -- release an export, publish one under a shared name, the ticket a row read names: fixed
  shapes, a ticket each; and it reads the batch's answer -- each export's success, error and schema -- and checks the
  schema against the plan's columns (DH16), the Arrow schema read with apache-arrow. It encodes no query: the message
  table stays the planner's (§4.4).
- **The run frame:** every Deephaven send inside `inRun`, a base's included.

### 4.2 Java (next, for PCT)

- `com.legend.deephaven` (`//core:deephaven_wire`, planner-free today, reached only by the WASM planner's door) splits:
  the encoder (`ProtoWire`, `DeephavenMessages`) goes where the plan's bytes are made, the planner's side; the session
  part (its requests, the answer's reader, `ArrowSchema`) moves into exec as Java's Deephaven driver. The planning
  parts (`keys`, `stored`, `send`'s cut) go as §3 says.
- Transport: gRPC-web, as the browser speaks it, over the JDK's `HttpClient` -- no gRPC library (to verify: M4).
- Then a Deephaven session in exec beside the JDBC ones, registered with the acts it performs as the JDBC census
  registers its classes (opens, sends, decodes, closes), and the PCT lane on a Deephaven runtime.

### 4.3 Python (later: the path only)

The same contract, and no encoder of its own either: the slots, the session's few requests and the answer's reader.
Its transport (gRPC-web, or Deephaven's own Python client) is chosen when it is built.

### 4.4 Keeping the drivers identical

- **One message table, the planner's alone:** `DeephavenMessages` -- each message's fields, their numbers and types,
  only the subset lite writes, checked against the pinned server's `.proto` by `DeephavenMessagesTest` -- is read by
  the planner's encoder and by nothing else, so there is one encoder and nothing to drift. A driver writes only the
  session's fixed requests.
- **Shared conformance fixtures:** recorded cases of a plan, a session's handles and the server's answers, with the
  bytes each driver must send and the result it must read. Recorded from the pinned server by a lane, rerecorded
  when it moves; run by every driver's tests -- as Java's and TypeScript's DuckDB runners are held to one plan.

## 5. The door after

`Boundary.deephaven` keeps planning operations only -- `topics`, `plan`, `validate`, `holds` -- and the query's planning
entry takes a base (§3.2); each is closed by `only(...)` over the keys it reads and each registered in
`PlannerSurfaceTest` with a dated note (the orchestrator's answer 5). The other nine go: `nullable`, `stored`, `keys`,
`send` into the plan; `made`, `batch`, `releaseExport`, `doGet`, `publish` into the driver. `ingestPlans` and the lite
route `/api/lite/v1/grammar/grammarToJson/model` are registered the same way.

## 6. The witnesses

- **What is sent is the plan's:** a test captures every request a driver sends and compares it with the plan's
  `request`, byte for byte, only the listed slots differing -- Deephaven's twin of `MongoDbPlanTest.theCommandSentIsThePlans_itsParametersBoundByTheLet`.
  In TypeScript against a capturing fake server, and in Java when its driver lands.
- **No planner in a run:** the driver takes no planner (§4.1); the door's run-side operations are deleted, so a call
  cannot compile; and a Node test runs a cube's gestures through the driver under the run frame (`frameKept` true),
  counting zero planner entries inside runs.
- **The driver is a run layer:** `ts-layers.txt` and `TsLayeringTest`; in Java, `execReachesNoPlanning`.
- **Types are the plan's:** a base's declared columns come from its step; a server holding a column otherwise is
  refused by name (DH16), and no code path reads the server's types to plan.

## 7. To measure before building on it

- **M1, reuse:** v0's steps (the base and the result), against today's per-operation reuse, on the cube lane's
  gestures (the regroup and the filter within 250 ms, closing a group within 150 ms with nothing sent) and the desk
  lane's opening
  and warm times (`market.mjs`). A missed budget is a defect to find (§3.3), never a raised budget.
- **M5, a step reading another made in the same request:** the pinned server accepting an operation that reads, by
  ticket, an export an earlier operation of the same batch makes (so every reference between steps is a ticket slot,
  whether that step is held or sent alongside). If it does not, the runtime sends a plan's missing steps in order, one
  request each: more round trips on a first build, the plan unchanged.
- **M2, the base's types:** for every base and result the lanes make, the server's held Java type equals the type the
  plan writes. A difference is a dialect fix (DH16 strict), found before the plan carries the type.
- **M3, size:** the TypeScript driver is small now (no encoder); measured under the bundle budgets anyway
  (`bundle-budget.test.ts`) and the Deephaven chunk.
- **M4, Java's transport:** the JDK's `HttpClient` speaking gRPC-web to the pinned server (a probe) before the Java
  driver is promised.

## 8. The orchestrator's answers this follows (2026-11-16, pasted by the user)

1. Encode on the run side, from plan data only, type-checked against the plan's types, with a once-per-session
   establishment; never keep the planner's door open to the run. This design goes one step further: the run side
   encodes no query at all (§3.1), only the session's few requests -- to be told to the orchestrator.
2. Deephaven lands before DataCube's single API (`claude/datacube-engine`).
3. Lean SQL changes shared types: `DeephavenBatch` gives arms to the new `SqlExpr` variants, `SqlSource.RowValues` and
   `readsRowLets` when E lands; the WITH reuse is not extended.
4. lite's protocol extensions are product, under lite's names and `_type` classifiers.
5. Each new door registered with a dated note; the wire codec is not a door; plan data (`PLAN_DATA`) is fine; a "what
   is sent is the plan's" witness.
6. The plan version is assigned at landing; landing is a staging branch, an independent review, a SQL lane round, one
   PR and one CI run.

## 9. The order of work

1. **This document reviewed** with the user, and §10's questions ruled.
2. **Merge `main`** (`4dfbb5b7`: C5-P, C5-J, C5-T; 85 commits, about 18 conflicts), placing the Deephaven TypeScript
   in C5-T's layers.
3. **M1, M2 and M5** measured on the branch.
4. **The planner:** the statement as bytes, its slots and identity; the columns' two facts (§3.4); a query planned
   over its base, the base a step and the query reading it through a plan variable; the encoder and the planning
   parts of `DeephavenWire` (`keys`, `stored`) moved to the planner's side; Java tests.
5. **The browser's driver** in a run layer; the door narrowed and closed; DataCube's bases planned with their queries
   (its model fragment, second planner and name swap deleted); the witnesses; every Deephaven lane green.
6. **The Java driver** -- the session part re-homed into exec, with the shared fixtures -- in the PR right after v0's
   (Q1, ruled).
7. **The surfaces and layers registered, every lane run,** and the landing as §8, answer 6 says (the user's word for
   the PR).

## 10. Rulings and questions

- **Q1, ruled 2026-11-16:** the Java driver in the PR right after v0's ("yes right after").
- **Q2, the user asked what the message table is, and the clean-sheet design for it:** answered by §3.1 and §4.4 --
  the planner writes the exact bytes, so the table is the planner's alone and no runtime encodes. To confirm.
- **Q3, the user asked whether every plan does not already carry the three types (Pure, store, database):** it does
  (§3.4); the database type lacks two facts, its nullability and its own spelling. To confirm.
- **Q4, the user asked why a fallback, then ruled (2026-11-16):** intermediate tables built off base tables are fine
  if they are part of the plan. §3.3: every kept table is a planned step under the same two rules; v0 cuts the base
  and the result, and a table between them only when M1 shows a gesture needs it.
- **Q5, ruled 2026-11-16:** Python later ("yes python later").
- **Q6, a public interface change (§3.2):** the query's planning entry takes the grid's base too, so the plan knows
  the base is a step and the query reads it through a slot. Found 2026-11-16 by tracing how a query reaches its base
  (§1, point 4). For the user's word.
