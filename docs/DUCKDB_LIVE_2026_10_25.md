# A live DuckDB in the tab: streaming's concepts, no server (2026-10-25)

> The user, 2026-10-25: "can we build the concepts into a demo version of duckdb that works with no server" -- after the
> Proton spike (not to be used) and the Deephaven line (live, but under a licence question). A dev, demo and simulation
> tool: the trading desk live in a browser tab, on a static site, over the one trading model -- licence-free.

## 0. What it is, and what it is not

DataCube asks little of a live engine (learned on the Deephaven line): a reader that runs a plan, **follows** the results
a grid's view read, and **says when they changed**; the grid then runs again (`services.ts` LiveGrid, LiveRows: the
app's hooks, engine-agnostic). DuckDB has no live results. This line gives the tab's DuckDB-WASM a thin layer that does
-- a streaming engine's concepts (Proton's, ClickHouse's) at a demo's scale:

| concept (Proton's word) | here |
|---|---|
| an append-only **stream** | a table written only by appends, its history **bounded** (the last N rows, or the last T seconds by a time column): a tape of a day's millions of prints does not fit a tab |
| a **versioned_kv** stream: the latest row a key | a table with a primary key, written by upserts (`INSERT OR REPLACE`): a name's quote |
| `table(stream)`: a snapshot query | every query DataCube sends: a plan's SQL, read in one snapshot (a transaction) per view |
| a streaming query, **EMIT PERIODIC** | each followed query run again at the emit interval when a table it reads changed, its result compared with the last, the grid told only when it differs |
| a query of `now()` | run again at each emit, changed tables or none: a live time window ("the last five minutes": DH30) for free |
| an **incremental materialized view** | later, measured first (§3): a hot aggregate kept in a table of its own, updated from each appended batch |

It is **not** a streaming database: one person, one tab, bounded history, every followed query run again whole (until
§3), no deltas on a wire. Deephaven stays the live server target where its licence allows.

## 1. The pieces

- **L1, the live engine** (`engine-client/src/duckdb-live.ts`): over a DuckDB connection (the tab's DuckDB-WASM, or
  DuckDB in Node for its tests). Streams and keyed tables declared with their retention; writes (append, upsert) that
  mark what changed; readers (`QueryEngine` + `follow`, `onLive`, `close`, as the Deephaven reader's), each on a
  connection of its own so a view's queries read one snapshot; the emit loop. A followed query's new result is kept, so
  the grid's run at its tick is answered from it -- nothing run twice.
- **L2, the market in the tab** (`datacube/demo/market-sim/`): the market world's simulation as TypeScript -- the names,
  the price process, quotes and prints, the morning, the newsroom, the bank's day (desks, positions, limits, orders worked
  into fills, FX, RFQs, the option chain) -- writing the one trading model's tables (`30-store.pure`, every column) into
  the live engine; crypto from Coinbase's public feed where the browser reaches it, simulated where not (a browser's
  WebSocket needs no server). The same world as `market.groovy`, whose simulation it replaces, its Deephaven publisher
  apart (the licence question's).
- **L3, the desk on it** (`market/host.ts`'s DuckDB twin): the market's project over a DuckDB connection, every grid on
  its runtime run by a live reader; the trading desk an example of DataCube's own page with no config at all -- on the
  static site.
- **L4, measured**: the desk's lane on it (no server: a test for CI's datacube lane, not a manual one), each tab's ticks.
- **L5, incremental views** (§3), where L4 says a tab's re-runs miss its budget.

### L1, landed (2026-10-26)

`engine-client/src/duckdb-live.ts`, its test `//engine-client:duckdb_live_test` (eight cases on a real DuckDB-WASM in
Node, every statement each connection is sent counted):

- **It spells no SQL** (D5, `tools/guards/host_sql.txt`): a write's rows go in as Arrow (`arrow-ipc.ts`, which gained
  instants -- TIMESTAMP_MS, either side of the epoch -- and truths for the market's tables) to a staging table, and every
  statement it runs is the planner's, for an operation it names, asked once a shape: the existing `insert` (from the
  staging relation), `drop` and `transaction`, and two new ones in core's `HostOperation` -- **`Upsert`** (the latest row
  a key: `INSERT OR REPLACE`, the batch's last row of a key by its row order) and **`Bound`** (a stream's rows past its
  latest N, or past its latest value less a span, deleted) -- DuckDB's in `DuckDbHostOperations`, every other database
  refusing them by name; `HostOperationsTest.liveTablesUpsertAndBound` runs both on DuckDB over JDBC.
- **A reader follows as the Deephaven reader does**: what a view read, and what is read after it (a chart drawn after
  its grid); a result knows the declared tables its SQL names and their versions when it ran (read before it runs: a
  write landing meanwhile runs it again, never goes unseen).
- **The emit**: a stream's bound first (once it has grown a tenth past it), then each reader's stale followed results
  run again in one transaction -- proven one snapshot: a write on another connection the moment the first run is sent is
  seen by neither run, and by both at the next emit -- the reader told only when a result's values differ; a result of
  the clock runs every emit. The grid's run at the tick is answered from the kept results: nothing sent (counted).

## 2. Measure, not assume

DuckDB-WASM is single-threaded (threads need cross-origin isolation, which a static host may not give) and capped near
4 GB. The world the desk's lane runs: 100 names, 2,000 quotes and 400 prints a second, half an hour of morning; the
shown tab's followed queries re-run each emit (the app runs no hidden tab's: lazy views, held ticks). Each landing says
what it measured: an emit's time per tab, a tick on screen after its rows, the tape's bound, memory.

## 3. Incremental views, when measured

ClickHouse's lesson (its insert-triggered materialized views, aggregate states that merge): an append-only table's
decomposable aggregates (count, sum, min, max, an average as sum and count, VWAP as two sums) and the latest row a key
are kept from each batch, never recomputed from the whole history. Only where L4 measures a re-run too slow, and only for
a shape the planner's SQL proves (a group of such aggregates over one stream).

## 4. Where it lands

None of it is Deephaven's: it goes to `main` with the platform's work (the first PR's part A), the trading desk live
there with it.
