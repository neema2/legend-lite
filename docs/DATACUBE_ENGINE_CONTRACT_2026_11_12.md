# DataCube's engine contract: one intent API, every engine behind it (design, for the user's ratification)

**Status:** PROPOSED, 2026-11-12. Not built. Written at the user's request ("Step back clean sheet how should datacube
work in expert system?", then "Let's write the design doc"), after the total wall's C moved headless Python's DataCube
onto intent (`claude/total-wall-bridge`) and the host census of 2026-11-12 (§6) mapped every host DataCube runs against.

**Scope:** how DataCube (the UI, its query builder and its data flows) talks to whatever plans and runs its work. It does
not change Pure, the planner, lowering or SQL; it changes who asks whom for what, and what crosses each boundary.

---

## 1. The principle

**DataCube is a pure client of intent.** It owns the screen and one thing of value: what the person means -- the query
as Pure, the view (pivots, sorts, formats), the choices (which file, which table, which write mode). It never holds SQL,
a plan, a file path, a connection or a secret.

**An engine is a planner, a runner and a database, owned together.** It turns intent into answers. DataCube never knows
which engine it talks to:

```
DataCube (UI) ──── one intent contract ────► Engine
                                              ├─ the tab: a Web Worker (WASM planner + DuckDB-WASM)
                                              ├─ Python: native planner + DuckDB (the V8 bridge, or HTTP for its page)
                                              ├─ the Java server: planner + its JDBC databases
                                              └─ the warehouse: planner + the warehouse's database
```

Only planners write statements; only runners send them; only databases receive them. Plans may travel between a planner
and a runner as data (G11), but always inside one engine; they never come back from DataCube to be run.

## 2. The contract

| Call | In | Out | Notes |
|---|---|---|---|
| `capabilities()` | -- | the calls and options this engine supports, the contract version | DataCube adapts its menus; anything not listed is refused by name if asked |
| `session.open()` / `close()` | -- | a session | handles live in a session; `close` drops what it made |
| `addSource(source, how)` | bytes (an upload), a URL, or a remote reference (a warehouse table, an S3 object); `how` is **live** (a mount, read in place) or **snap** (a planned copy into the engine's database), DataCube's two kinds today | an opaque **handle**, its name, digest and size | the engine issues and resolves handles; DataCube never sends a path back |
| `describe(handle)` | a handle | its columns and types | the engine's catalog, by the engine's planner |
| `drop(handle)` | a handle | done | |
| `typeOf(query)` | Pure, a runtime | the result type (columns, types) without running | DataCube's builder and typeahead |
| `query(query, cap)` | Pure, a runtime, parameters; DataCube's row cap (the person's `maxRows`, General panel; asked as cap + 1 to see a cut) | typed rows streamed as Arrow, their schema, the execution id; the flat total when the cap cut it (asked only then, as today) | a dynamic pivot's two phases, retries and the error read-back are the engine's |
| `write(box, target, mode)` | DataCube's table output: a box's query, a target table of the host's database, its mode -- **create**, **overwrite**, **append**, **merge** by keys (table-output.ts, today's four) | rows written, the execution id | ONE call: the engine runs create, catalog, write and commit as one transaction |
| `cancel(request)` | a request id | -- | best effort; an answer already streamed stays valid |

**Errors are typed:** `compile` (the query does not type), `run` (the database refused it: its own words, secrets
redacted), `refused` (this engine does not do that, by name), `transport`. Every answer and every error carries the
door-minted execution id.

**Wire formats:** intents and small answers as JSON; rows as Arrow IPC. The contract is versioned; `capabilities()`
states the version, and a client refuses an engine it cannot speak, by name.

## 3. The rules

- **R1 -- The planner and runner sit with the database.** Everything multi-step happens inside the engine. DataCube asks
  for an answer; it never orchestrates steps across a boundary.
- **R2 -- One call per user action.** In particular, table output is one `write`, not a create / catalog / write /
  commit dialogue across a boundary (a transaction held open across a boundary is fragile and leaks on a dropped link).
- **R3 -- Handles, not paths or names.** A file, an upload, a mount and a snapshot are handles the engine issued and
  resolves itself; an unknown handle is refused by name.
- **R4 -- No SQL and no plan crosses into an engine.** An "explain" view may show a plan as text, read-only.
- **R5 -- Auth belongs to the engine's connection.** Secrets are bound by name inside the engine (B3's presigned-link
  design) and never reach DataCube.
- **R6 -- Refusal by name, never a fallback.** An engine that cannot do a call says so; DataCube shows it. No call is
  silently routed another way (G7).
- **R7 -- One conformance suite.** Every engine is held to the same asks with the same answers or the same named
  refusals, in CI, as the plan runners are held by their shared fixtures today.
- **R8 -- The tab is not special.** It is an engine in a Worker, behind the same contract, reached by the same client
  library; only the transport differs (postMessage, the V8 bridge, HTTP).

## 4. The engines

| Engine | Implements | Refuses by name |
|---|---|---|
| **Tab** (Worker: WASM planner + DuckDB-WASM) | all | -- |
| **Python, headless** (the V8 bridge) | all | -- |
| **Python, its page** (HTTP) | all (today's coarse routes become the contract's) | -- |
| **Java server** (`pure/v1`, lite's own routes) | `capabilities`, `typeOf`, `query`, `cancel` | `addSource`, `drop`, `write` (no page database lives there), until a project database is offered |
| **Warehouse** | `capabilities`, `describe` (its catalog), `typeOf`, `query` | `addSource`, `write` until the warehouse offers them |

A planner on a server with the rows run in the tab (today's "server plane") is an engine-internal arrangement: from
DataCube's side it is one engine. With legend-engine as that planner, it is compatibility at the edge (G5).

## 5. One client library

`engine-client` provides one `Engine` interface (§2) and three transports: `worker()` (the tab), `bridge()` (headless
Python), `http(url)` (Python's page, the Java server, the warehouse). DataCube's code is written once against `Engine`.
Today's special cases (`heldDataHost` handing the UI a held DuckDB connection, `pythonServices`' per-route calls,
`WarehouseEngine`'s SQL API) become transports or engine internals.

## 6. Where we are (the census of 2026-11-12, `claude/total-wall-bridge`)

| Host | Today | Gap to the contract |
|---|---|---|
| Tab, local plane | plans and runs in the tab; `heldDataHost` | move behind a Worker boundary; `write` as one call |
| Tab, server plane | the server plans rows, the tab runs them; data intents planned in the tab | engine-internal; no DataCube change |
| Python's page | Python plans and runs; coarse routes (`file-part.json`, `file.json`, `remote.json`, `forget.json`, `join.json`, `plan-and-run.json`); no `write` on the page | the contract's routes; `write` offered |
| Python, headless | intent only (`DataHost`: load, operations, catalog, forget; the write as an intent dialogue) | handles for files (in progress); `write` as one call; `capabilities` |
| Warehouse | catalog by `listObjects`; rows: the tab or Python plans, SQL goes to the warehouse's SQL API | a `query` intent the warehouse plans and runs (or stays engine-internal: the tab's planner + the warehouse as its database); refusals |
| Java server | `query` through PLANANDRUN (`pure/v1` execute) | `capabilities`, refusals |

## 7. The path (each step green on its own; the conformance suite grows with it)

1. **The contract and the suite.** `engine-client`'s `Engine` interface, the typed errors, `capabilities`, and the
   conformance suite, run first against the headless Python engine (closest to the contract today).
2. **`write` as one call** on Python headless and the tab (the engine runs the transaction).
3. **Handles everywhere** (files on Python headless are in progress; uploads, mounts and snapshots follow).
4. **The tab behind a Worker:** the tab's engine moves off the UI thread behind `worker()`; `heldDataHost` goes.
5. **Python's page on the contract:** its routes become the contract's; `write` offered on the page.
6. **The warehouse and the Java server:** `capabilities`, `describe`, `typeOf`, `query`, named refusals.
7. **Delete** every DataCube path that reaches a database, a plan or a statement directly; a guard pins it (DataCube's
   sources import `Engine` and nothing below it).

## 8. What it deletes

DataCube's direct database handles (`heldDataHost`, the held connection in the UI), the per-host service objects
(`browserServices`, `pythonServices`' route calls) as DataCube-facing APIs, the table-output dialogue across a boundary,
paths handed back and forth, and the SQL a client sends to the warehouse's SQL API from DataCube's side.

## 9. Settled by what DataCube does today (2026-11-12, the user's review)

The contract carries DataCube's existing behaviour, unchanged; nothing here is new product behaviour.

- **Rows:** the person's row cap (`maxRows`, user-configurable, `DEFAULT_MAX_ROWS`), asked as cap + 1, the flat total
  asked only when the cap cut it -- as `query.ts` does today. No new windowing.
- **Remote sources:** **live** (a remote mount, read in place) and **snap** (a planned copy, B3's planned snapshot) --
  both exist; `addSource` names which.
- **Writes:** DataCube's table output and its four modes (create, overwrite, append, merge by keys).
- **Timing:** its own workstream, in parallel with D and E (the user, 2026-11-12); mostly DataCube and engine-client,
  not core.
- **Consolidated with the intent bridge** (`claude/total-wall-bridge`): the bridge's Python implementation is step 1.

## 10. Not in scope

The planner, lowering and SQL; Pure semantics; the warehouse's own server; legend-engine's API (served at the edge as
today); the UI's look.
