# Deephaven on the run side: the plan as data, one driver per runtime (2026-11-16)

**Status: PROPOSED, for review with the user before any code. Nothing here is built.** It changes how the browser
runs a Deephaven plan (docs/DEEPHAVEN_PLAN_2026_10_22.md DH11, DH22, DH23, B1), so that the Deephaven road meets the
plan/exec split C5 enforces on `main` (G4), in its spirit and not only its letter. It came out of the user's questions
of 2026-11-16 ("plan vs exec split does not mean we have to split languages ... a plan that is data and then multiple
runtimes like Java, TypeScript, Python that can run the plan without doing things the planner is supposed to do"; "V0
can be browser only for the exec side but we should make sure we have a design/path for all"; "we will want Java very
quickly to run PCT") and the total-wall orchestrator's answers of the same day (§8).

## 0. In one paragraph

The planner writes everything a Deephaven server is sent: each query as one batch of operations, sent exactly as
written, with a few marked slots for the numbers only a session knows (the result's export id, a base's ticket); the
result's columns with their types; and each grid's base as a step of its own, made once per session and skipped while
the session holds it (a temp table, live on Deephaven). Each environment has a small Deephaven driver that fills those
slots, sends, checks the answer against the plan's types and reads the rows: TypeScript in the browser for v0, Java's
exec next (for PCT), Python later -- the way DuckDB already runs: one plan format, a runner per environment. No driver
asks the planner anything, and none can: it is given no planner.

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

Three things are wrong, and only the first is the door:

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

### 3.1 A query: one batch, sent as written

A Deephaven statement in a plan is today a batch of operations in Deephaven's protocol's JSON form (proto3 JSON of
`BatchTableRequest`, written by `DeephavenBatch`) plus `declared`. It gains three fields and a rule:

- **`ops`**, as today. Inside the batch an operation reads an earlier one by `batchOffset`, and a server's table by
  its scope ticket (`s/NAME`, the deployment's tables): fixed text.
- **`slots`** (new): every place the driver writes a number of its session's, listed: the result (the last
  operation's `resultId`, the export id the driver allocates -- exactly what `DeephavenWire.batch` adds today), and
  each reference to a grid's base (§3.2), which holds a marker ticket in the text, `legend/base/<identity>`, so the
  text stays valid. Precedent: the dialect's parameters marker, `legend/parameters` (`DeephavenJson.PARAMETERS_TICKET`),
  which a sender replaces. The driver fills listed slots only; it never scans text for markers.
- **`result`** (new): each result column's name, the Java type the server must hold it as (`deephaven:type`) and
  whether it may hold a null. The dialect already writes this for a store's columns (`DeephavenBatch.javaTypeOf`);
  computed columns are new (measured first: M2).
- **`identity`** (new): the statement's digest, SHA-256 of its text with its slots as markers, made by the planner.
  It names the result for reuse (§3.3) where `keys` hashed the plan at run.

Example, schematic (field names shortened; a ticket is base64 bytes in proto3 JSON, shown as text):

```json
{ "identity": "9c1e...",
  "ops": [
    { "aggregate": { "sourceId": { "ticket": "legend/base/3f2a..." }, "groupBy": ["region", "ccy"], "...": "..." } },
    { "sort":      { "sourceId": { "batchOffset": 0 }, "...": "..." } } ],
  "slots": [ { "op": 0, "reference": "sourceId", "base": "3f2a..." }, { "op": 1, "result": true } ],
  "declared": [],
  "result": [ { "name": "region", "type": "java.lang.String", "nullable": true },
              { "name": "mv", "type": "double", "nullable": true } ] }
```

The driver sends these two operations, the base's export ticket in the first slot and the result's id in the second,
and nothing else differs from the text.

### 3.2 A grid's base: a session table

A grid's base (B1: the grid's source and its leaf-level calculated columns, made once and read by every query the
grid sends and every result it follows) becomes a plan step of its own:

- **What the planner writes:** the base's identity; its batch already in stored form (`select` and `update` where the
  query is `view` and `updateView`, so the server computes each column once a change and keeps it -- the rewrite
  `DeephavenWire.stored` does at run today, done by the dialect); and its columns, each with its Java type and whether
  it may be null (replacing the `nullable` question and the server-typed declaration of §1, point 3).
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
- **DataCube's side:** `ServerBases` still declares each base's table in its model fragment (`legend::Bases`), but from
  the base step's columns. So a grid's queries can be planned before its base is made, and are never planned from a
  server's answer.

### 3.3 Reuse: bases and whole results, nothing finer

- **A base** is reused as §3.2 says.
- **A whole result** whose identity the session holds (in use, or idle within the session's keep of 128) is not sent
  again; its export is read. This keeps the cube lane's check that closing a group sends nothing
  (`datacube/test/deephaven/cube.mjs`).
- **Gone: reuse of single operations above the base** (today a regroup sends only the operations where it parts from
  an earlier query). It saved little once bases came -- above a base a query is a few cheap operations -- and it is
  the part that rewrites the plan (§1, point 2). M1 measures what it costs to drop.
- **If M1 shows it is needed,** the form that stays within the rules: each operation its own planned step with its own
  identity, skipped when held, its references bound -- the same two rules (skip, bind) applied per operation, at the
  price of larger plans. Not built unless M1 says so.

## 4. One driver per runtime

Every driver does §2's list and nothing else, from the same plan.

### 4.1 The browser, TypeScript (v0)

- **Where:** a run layer of `tools/deps/ts-layers.txt` (C5-T), reaching the records and npm packages only
  (`TsLayeringTest`). It is constructed with no planner, so it has none to ask: the separation is proven by its type,
  as Java's session statements take a `PlannedSql` and no `String`.
- **What it keeps:** today's transport in `engine-client/src/deephaven.ts` (gRPC-web over the page's `fetch`, sign-in,
  token refresh, the session's keep), rows by DoGet read with apache-arrow, the live reader (`deephaven-live.ts`).
- **What it gains:** the encoding and decoding the WASM planner does for it today -- the protocol writer
  (`ProtoWire`), the answer's reader and the DH16 check (`DeephavenWire.made`), the schema reader (`ArrowSchema`) --
  written in TypeScript. That is about 860 lines of Java (`ProtoWire` 353, `DeephavenWire` 483 less its planning parts,
  `ArrowSchema` 130), over the message table `DeephavenMessages` (369 lines, a table of fields; §4.4).
- **The run frame:** every Deephaven send inside `inRun`, a base's included.

### 4.2 Java (next, for PCT)

- `com.legend.deephaven` (`//core:deephaven_wire`, planner-free today, reached only by the WASM planner's door) moves
  into exec as Java's Deephaven driver; its planning parts (`keys`, `stored`) move into the planner as §3 says.
- Transport: gRPC-web, as the browser speaks it, over the JDK's `HttpClient` -- no gRPC library (to verify: M4).
- Then a Deephaven session in exec beside the JDBC ones, registered with the acts it performs as the JDBC census
  registers its classes (opens, sends, decodes, closes), and the PCT lane on a Deephaven runtime.

### 4.3 Python (later: the path only)

The same contract. Python's protobuf library reads proto3 JSON directly, so a Python driver can send the plan's
batch through Deephaven's Python client's generated messages without an encoder of its own (to verify by reading the
pinned client before it is promised).

### 4.4 Keeping the drivers identical

- **One message table:** `DeephavenMessages` (each message's fields, numbers and types, checked against the pinned
  server's `.proto` by `DeephavenMessagesTest`) becomes one data file both encoders read, so they cannot drift on a
  field.
- **Shared conformance fixtures:** recorded cases of a plan, a session's handles and the server's answers, with the
  request bytes and the decoded result each driver must produce. Recorded from the pinned server by a lane, rerecorded
  when it moves; run by every driver's tests -- as Java's and TypeScript's DuckDB runners are held to one plan.

## 5. The door after

`Boundary.deephaven` keeps planning operations only -- `topics`, `plan`, `validate`, `holds`, and one new `base` (a
grid's base as its session-table step: §3.2), each closed by `only(...)` over the keys it reads and each registered in
`PlannerSurfaceTest` with a dated note (the orchestrator's answer 5). The other nine go: `nullable`, `stored`, `keys`,
`send` into the plan; `made`, `batch`, `releaseExport`, `doGet`, `publish` into the driver. `ingestPlans` and the lite
route `/api/lite/v1/grammar/grammarToJson/model` are registered the same way.

## 6. The witnesses

- **What is sent is the plan's:** a test captures every request a driver sends and compares it with the plan's
  batch, only the listed slots differing -- Deephaven's twin of `MongoDbPlanTest.theCommandSentIsThePlans_itsParametersBoundByTheLet`.
  In TypeScript against a capturing fake server, and in Java when its driver lands.
- **No planner in a run:** the driver takes no planner (§4.1); the door's run-side operations are deleted, so a call
  cannot compile; and a Node test runs a cube's gestures through the driver under the run frame (`frameKept` true),
  counting zero planner entries inside runs.
- **The driver is a run layer:** `ts-layers.txt` and `TsLayeringTest`; in Java, `execReachesNoPlanning`.
- **Types are the plan's:** a base's declared columns come from its step; a server holding a column otherwise is
  refused by name (DH16), and no code path reads the server's types to plan.

## 7. To measure before building on it

- **M1, reuse:** bases and whole results only, against today's per-operation reuse, on the cube lane's gestures (the
  regroup and the filter within 250 ms, closing a group within 150 ms with nothing sent) and the desk lane's opening
  and warm times (`market.mjs`). A budget missed means §3.3's per-operation steps, never a raised budget.
- **M2, the base's types:** for every base and result the lanes make, the server's held Java type equals the type the
  plan writes. A difference is a dialect fix (DH16 strict), found before the plan carries the type.
- **M3, size:** the TypeScript driver's bytes against what the WASM planner sheds, under the bundle budgets
  (`bundle-budget.test.ts`) and the Deephaven chunk.
- **M4, Java's transport:** the JDK's `HttpClient` speaking gRPC-web to the pinned server (a probe) before the Java
  driver is promised.

## 8. The orchestrator's answers this follows (2026-11-16, pasted by the user)

1. Encode on the run side, from plan data only, type-checked against the plan's types, with a once-per-session
   establishment; never keep the planner's door open to the run.
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
3. **M1 and M2** measured on the branch.
4. **The planner:** the statement's slots, result types and identity; the base step; the planning parts of
   `DeephavenWire` (`keys`, `stored`) moved in; Java tests.
5. **The browser's driver** in a run layer; the door narrowed and closed; DataCube's bases declared from the plan; the
   witnesses; every Deephaven lane green.
6. **The Java driver**, re-homed into exec, with the shared fixtures (in v0's PR or the next: question Q1).
7. **The surfaces and layers registered, every lane run,** and the landing as §8, answer 6 says (the user's word for
   the PR).

## 10. Questions for the user

- **Q1.** The Java driver: in v0's PR, or the PR right after it? Recommended: right after. v0 stays browser-only as
  ruled, and the Java code moves into exec in v0 as the reference the shared fixtures are checked against, so it does
  not sit unused.
- **Q2.** One message table as a data file both encoders read (recommended), or a table in each language held equal
  by the fixtures alone?
- **Q3.** A base's column types written by the planner and checked against the server (recommended, §3.2), confirmed.
- **Q4.** If M1 misses a budget: per-operation steps (§3.3, recommended), never a raised budget -- confirmed.
- **Q5.** Python: the path written down (§4.3), no code now -- confirmed.
