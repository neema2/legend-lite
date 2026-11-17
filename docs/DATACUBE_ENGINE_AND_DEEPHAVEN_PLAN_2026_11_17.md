# DataCube's engine contract finished, and Deephaven built on it: the plan (2026-11-17)

**Status: PROPOSED for the user's ruling, 2026-11-17. Nothing in it is built until the user rules.** This session takes
over DataCube's engine contract stream (the user, 2026-11-17: "Nobody working on it, you can take over fully") and
builds Deephaven on it. It supersedes docs/DEEPHAVEN_RUN_SIDE_2026_11_16.md §10.3's answers and §11's order (that
document's §0-§9 stand: the rule, the verbs, the bytes, the witnesses) and adds three amendments to the ratified
contract, docs/DATACUBE_ENGINE_CONTRACT_2026_11_12.md (on `claude/datacube-engine`), for the user to rule one by one.

## 1. What it rests on (read 2026-11-17, from the branches)

- **The contract's branch** (`claude/datacube-engine` @417a80b62): steps 1, 2, 3, 3b and 4a built, 4b in part (a
  table output as one write, its facts through runners); 4b's rest, 4c, 4d, 5, 6, 7 and 8 not. Idle since 2026-11-14;
  19 commits not on `main`, its base three days behind it. Merged into `claude/deephaven-3-browser` today it conflicts in
  27 files (DataCube's app, headless and services; Python's host; `plan-runner.ts`; one guard), most from `main`'s C5-T
  layering.
- **How the contract's tab engine answers `query`:** it plans with the plane's planner (`p.plan(ask.query)`) and runs
  the plan on a DATABASE ADAPTER, `QueryEngine.execute(plan)` (`datacube/src/engines/tab-engine.ts` 527-545); a
  project's database is chosen by its connection (`engineFor`, C5).
- **No grid runs a full plan, on any database.** Every adapter runs the text plan's one statement (`DuckDbEngine.execute`
  runs `plan.sql`); the text plan carries the full execution plan only where its text alone will not run (B3.2), and
  DataCube reads from it only its bound values (`lite-planner.ts textPlan`). `runPlan` (`plan-runner.ts`) runs full plans
  for writes and snaps alone, answering JSON rows.
- **Live data exists only on `claude/deephaven-3-browser`:** `engine-client/src/live.ts` (`LiveReader.follow`,
  `onLive`), `QueryEngine.executeWindow` (Deephaven's viewport: the board's book, 100,000 rows read 62 at a time),
  `duckdb-live.ts` (the tab's live DuckDB: `follow` and `onLive`, no window), `GridRows.live` / `LiveGrid`. `main` has
  none of them, and the contract was designed without them (its §9: "No new windowing").
- **D and E** (`claude/total-wall-d`, `-d-stage` updated today, `-e`, `-e-sql-v3`): neither adds a node kind to the plan;
  both edit `plan-runner.ts`'s read path and `PlanJson`, the format numbered 52 to 56 in flight and "renumbered at
  landing"; the user's O9 gives the number to D first, then E.

## 2. The target

DataCube sends Pure to an `Engine` and nothing else. The engine plans; a database adapter -- DuckDB-WASM's, the tab's
live DuckDB's, Deephaven's, a project's -- is handed the plan whole and runs it, its steps through the one runner
(`plan-runner.ts`) and its answer itself: rows, a window of them, or followed live. A grid's base is a Pure `let`: spliced
where the database's made tables are frozen copies (every SQL database), a `CreateIfNotExists` step where they keep
themselves current (Deephaven). The runtime chooses only when (docs/TENET_CHARTER.md A3): a table's drop, its rows
fetched or followed. No shortcut plan, no Deephaven-only runner, nothing of DataCube's reaching a planner or a database.

## 3. Decisions, for the user to rule

**P1. Two pull requests, per O9 (one per stream).** First the DataCube stream (`claude/datacube-engine`): the contract
through its step 4, with amendment A. Then the Deephaven stream (`claude/deephaven-3-browser`) on top of it: amendments
B and C, live data and Deephaven behind the contract. *Rejected:* one PR for both (O9); Deephaven first on DataCube's
old road (built twice).

**P2. Amendment A -- a database adapter takes the full plan.** The tab engine hands an adapter the plan the planner
wrote, whole (the execution plan), never its answer's text alone. A plan that is one statement the adapter runs as today;
a plan with steps before its answer -- a dynamic pivot's keys (`Keyed`), a kept table (P4) -- it runs through the shared
runner (`plan-runner.ts`: the steps through the adapter's session), then reads its answer's rows itself. One runner's code
for every adapter's steps; the text plan's `sql` is the answer's statement, read from the plan. *Rejected:* adding the
kept steps to the text plan's fields (a second description of the plan); Deephaven alone on the full plan (two roads).

**P3. Amendment B -- live results and windows.** An engine whose database keeps results current (Deephaven, the tab's
live DuckDB) says so in `capabilities()` -- `live`, and `window` where it reads part of a result. Then:
- `query(ask, { window })` answers the window's rows, where they start, and the result's total (Deephaven's viewport);
- `follow(view)` keeps every result the view read current in the engine, and lets the older ones go;
- `onChange(listener)` says when a followed result changed, the live connection ended, or a result could not be
  followed; DataCube then asks again, answered from the engine's live copy with nothing sent.

An engine without them refuses them by name (R6). It carries the live grids built on `claude/deephaven-3-browser`
(`LiveReader`, `executeWindow`, `GridRows.live`) into the contract with their behaviour unchanged. It lands with the
Deephaven stream, the stream that brings live data. *Rejected:* `query` re-run on a timer (polls and fetches what
the server pushes); a window left out (the big grids would not fit the contract).

**P4. Amendment C -- a table kept by identity: `CreateIfNotExists(identity, statement)`.** A new node of the execution
plan. Where the plan's database keeps the tables it makes current as their sources change -- one fact its dialect
declares: Deephaven yes; every SQL database, the tab's live DuckDB too, no (their made tables are frozen copies; live
DuckDB stays current by running whole queries again) -- the planner plans a query's relation `let` as this step:
- made unless the session holds a table under `identity`, the SHA-256 of its statement: the same base, whatever query
  reads it, is one table;
- read by the statements after as that step's result (`SqlSource.VarSetPlaceholder`, legend-engine's own "another
  node's result set", named by the identity);
- dropped by the target's declared release, when the runtime lets it go.

Elsewhere the `let` is spliced as today, its SQL unchanged. Every plan reader handles it or refuses it by name: the tab's
runner runs it through the adapter's session; Java's JDBC runner and Python's runner refuse it (Java's Deephaven runtime,
the PR after, runs it). A Deephaven statement is one step's bytes and its places (its exports, and each kept table it
reads by identity): the steps packed inside one statement (built, never landed) are deleted. *Rejected:* reusing `Load`
(data entering a database; its skip-if-held cadence is the test door's); steps inside a statement (the drift); a
`->keep()` DataCube writes (a database's fact in the query).

**P5. The plan format's number.** Per O9 it goes to D, then E: P4's node takes the next one after theirs, so the
Deephaven PR lands after D and E. *Alternative, the user's to rule:* the orchestrator gives P4 a number ahead of them.

**P6. Deephaven as a database adapter.** Chosen by its connection (`engineFor`), as a project's database is; DataCube's
own Deephaven code (`deephaven-grids.ts`, `server-base.ts`) and its live-in-tab glue (`live-in-tab.ts`) are deleted,
every grid going through `Engine`.

**P7. The contract's steps 5-8** (Python's page, the warehouse and the Java server, the deletion of DataCube's direct
paths, a real legend-engine) follow the Deephaven PR, in the DataCube stream. Step 7 needs 5 and 6 first.

## 4. The order of work (each step green on its own and reported with its commit)

**Stream 1, DataCube (`claude/datacube-engine`):**
1. Merge `main` in (the 27 conflicts' DataCube and Python share, C5-T's layers kept): the contract's conformance suite
   (R7) green on Python headless and the tab; every DataCube lane.
2. Amendment A: the adapter handed the full plan; the shared runner's step function; DuckDB-WASM's adapter on it; a
   conformance case of a plan with steps run in the engine.
3. Step 4's rest: 4b's remaining flows, 4c (the browser lane's C17), 4d (native Arrow; `heldDataHost` and `LocalEngine`
   off DataCube's surface, a guard biting a planted violator).
4. An independent review; the local gate; the PR. It lands when A, B and C allow.

**Stream 2, Deephaven (`claude/deephaven-3-browser`):**
5. Merge stream 1 in.
6. Amendment C in the planner and the plan format, the identity guard FIRST (§5); the Java and Python runners' refusals.
7. Amendment B: the contract's live calls; the tab's live DuckDB and Deephaven adapters behind them.
8. Deephaven as an adapter (P6): its session sends one statement; kept tables through the shared runner; DataCube's base
   as a `let` through `rebased`; `deephaven-grids.ts`, `server-base.ts`, `live-in-tab.ts` and the packed steps deleted.
9. Every Deephaven lane and the live desk's; an independent review; the local gate; the PR, after D and E (P5).

**Then:** Java's Deephaven runtime (its own PR: PCT on Deephaven), then the contract's steps 5-8.

## 5. The tests and witnesses

- **The identity guard, before anything is built on P4:** the cube's query shapes -- the tree's levels, a pivot's two
  queries, the filter's values, the row count, a window, a chart, a page filter -- each with its base as a `let`, plan
  one identical `CreateIfNotExists` step on Deephaven; on DuckDB, the same queries' SQL unchanged.
- **The plan format:** `CreateIfNotExists` written and read back whole; Java's and Python's runners refuse it by name.
- **The contract's conformance suite** grows: a plan with steps run in the engine (A); `follow`, `onChange` and a window
  answered by the live engines and refused by name by the others (B).
- **What is sent is the plan's** (deephaven-session.test.ts, extended): every request byte for byte the plan's, only its
  places filled; a second query over the same base sends its answer's step alone; the same query again sends nothing.
- **The lanes:** every DataCube lane after stream 1; every Deephaven lane and the live desk's after stream 2 -- the cube's
  and the board's base checks passing on their merits (one base made, every query reading it), the cube's filter within
  its 250 ms over five runs.
- **The local gate** before each PR, its root-refusing tests under `tools/postgres/as_unprivileged.sh`.

## 6. Where the work stops and comes back to the user

The merge forces a choice between two sides' behaviour; the conformance suite stays red after the merge with no clear
cause; the identity guard fails; a budget is missed over five runs; a lane regresses; D's or E's landing changes the
runner or the format so that A or C no longer fits; an owner objects.

## 7. Who is told, before stream 1's merge

- **The total-wall orchestrator:** this session owns the DataCube stream now; amendments A, B and C to a ratified
  contract; P4's new node and P5's number.
- **D's session** (`claude/total-wall-d-stage`, active today): stream 1 and P4 edit `plan-runner.ts`'s read path and
  `PlanJson`, as D does.

The notes are drafted for the user to send.
