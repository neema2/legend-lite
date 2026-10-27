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

### Stage A, the method (the user, 2026-10-26: "Let's measure and do it expertly")

The question: can the tab's DuckDB run a desk tab's live queries again every 250 ms while the world writes into it,
with room left for a person's click, its memory bounded? `bazel test //datacube:market_measure` (manual; settings in
`MEASURE_*`) answers it, before any of the app is wired:

- **The real stack.** The pinned headless Chromium; the site's own DuckDB build (eh, one thread, in a Web Worker: what a
  static site gets); the planner on its worker; the live engine and the world as committed.
- **The real workload.** The saved trading desk (`market/screens.ts` DESK) as the app opens it: each cube's saved query
  by the app's rule (`savedQueryOpened`, `openCube`), each tile's first view's query by the app's builders
  (`levelWithValues` for a grid, `chartQuery` for a chart), planned by the planner; the tables by the planner's own DDL
  (the model's `MARKET_TAB`: the same runtime's name, its connections the tab's DuckDB). One reader a cube.
- **The run.** One tab at a time; 5 s warm, 20 s measured; the world's beat every 50 ms, its writes every 250, the
  engine's emit every 250; the tape held at 25k, 100k and 250k prints by its bound.
- **Instruments.** Each operation's wall time; a click probe (a planned query every 100 ms, its wait the queue's);
  every request to DuckDB's worker logged (`postTask`: its kind, posted, answered) -- the union of their intervals the
  share of time the worker had work, their count an operation's round trips; and, the clock stopped after each tab, a
  write alone and a refresh alone, each one's time and round trips with nothing else in flight.

The budgets, fixed before any number came in: a refresh p95 ≤ 100 ms (40% of the cycle); a write p95 ≤ 30 ms; a click's
wait p95 ≤ 150 ms; a tick (write landed to grid told) p95 ≤ 500 ms; the world's step on the page's thread p95 ≤ 4 ms;
memory flat once the tape is at its bound; the market's own start (its tables and its world) ≤ 5 s.

Not measured in stage A (stage B's, the app wired): a group expanded, a total row, a drill -- so its queries a tab are a
floor -- and drawing (the same on any engine: the Deephaven desk measured 6-54 ms p50 a tick). The figures are this
container's (4 cores of a 2.8 GHz Xeon, 16 GB), said with each run.

### Stage A, the baseline (2026-10-26, the engine as committed in 3580c468)

Each tab's measured 20 s, ms (p95 unless said); "alone" the clock stopped, nothing else in flight. Budgets: refresh 100,
write 30, click 150, tick 500, beat 4.

| tape | tab | refresh p50/p95 | write | click p50/p95 | tick | DuckDB in use | alone: write ms/trips | alone: refresh ms/trips |
|---|---|---|---|---|---|---|---|---|
| 25k | Markets | 111/150 | 155 | 5/19 | 348 | 45% | 83/49 | 70/40 |
| 25k | Stock | 686/1230 | 1221 | 22/363 | 2104 | 92% | 87/49 | 683/45 |
| 25k | News | 197/511 | 928 | 12/318 | 740 | 77% | 79/49 | 289/12 |
| 25k | Order flow | 409/645 | 358 | 13/77 | 1042 | 82% | 81/49 | 313/54 |
| 25k | Crypto | 289/368 | 246 | 11/38 | 781 | 68% | 79/49 | 148/52 |
| 25k | Blotter | 55/157 | 187 | 5/18 | 361 | 46% | 70/49 | 81/36 |
| 25k | TCA | 944/1098 | 1191 | 44/163 | 1783 | 90% | 90/49 | 599/34 |
| 25k | P&L and risk | 144/187 | 212 | 12/27 | 401 | 64% | 83/49 | 84/32 |
| 25k | Options | 450/590 | 613 | 24/69 | 1077 | 83% | 96/49 | 438/32 |
| 25k | FX | 115/174 | 162 | 7/19 | 363 | 51% | 79/49 | 65/43 |
| 100k | Markets | 127/169 | 163 | 8/24 | 359 | 53% | 79/49 | 67/40 |
| 100k | Stock | 14961 | 14894 | 111/5173 | 25181 | 100% | 102/49 | 8827/51 |
| 100k | News | 4318/8867 | 13305 | 24/4151 | 9023 | 97% | 121/56 | 4848/68 |
| 100k | Order flow | 1914/2353 | 1433 | 38/111 | 4349 | 100% | 113/49 | 1144/93 |
| 100k | Crypto | 735/961 | 672 | 33/156 | 1776 | 89% | 89/49 | 447/55 |
| 100k | Blotter | 69/180 | 210 | 8/24 | 374 | 53% | 76/49 | 76/36 |
| 100k | TCA | 559/2332 | 2701 | 145/328 | 2696 | 95% | 97/49 | 1405/40 |
| 100k | P&L and risk | 136/180 | 191 | 9/24 | 383 | 60% | 83/49 | 84/32 |
| 100k | Options | 470/622 | 659 | 22/70 | 1122 | 82% | 102/49 | 406/32 |
| 100k | FX | 115/173 | 175 | 6/20 | 378 | 51% | 94/49 | 62/43 |
| 250k | Stock | out of memory: DuckDB at 3.1 of 3.1 GiB on a Stock tile's first query | | | | | | |

What it says:

- **Two kinds of tab.** Markets, Blotter, P&L and risk and FX barely move from 25k to 100k: a refresh alone ~60-85 ms,
  in its round trips (40 of them), the worker in use half the time, a click's wait ~20 ms, a tick ~0.35-0.4 s. Stock,
  News, Order flow, TCA, Crypto and Options are DuckDB's own work, and grow with the tape: Stock's refresh alone 683 ms
  at 25k, 8.8 s at 100k -- four times the rows, thirteen times the time -- so some query shape there costs more than
  its rows (a join or window over the tape), named by the next run's per-query timings.
- **Round trips.** A write alone is 49 of them whatever the tab (each table's batch: a staging table dropped, loaded,
  inserted from, dropped; each statement streamed in two or three), ~70-100 ms; a streamed statement's round trips are
  each ~1.3-4.8 ms (more under load).
- **Work done twice.** A grid's run at its tick was answered from the kept results less often than it should be (Order
  flow: 116 served, 216 run again; Crypto 215, 266): the write's timer and the refresh's are apart, so a write lands
  between a refresh and the grid's run, and the grid's queries run again.
- **Start.** The market's own start is 7-9 s at 25-100k, of which the planner's writing the tables' DDL is 6.5-7.6 s
  (the whole trading model compiled for it): over its 5 s budget, and a planner cost, not DuckDB's.

### Stage A, with the round-trip fixes (2026-10-26)

The fixes the baseline called for: a connection that reads each result whole (`DuckDbEngine` `whole`: one round trip a
statement); a stream's batch of its declared columns appended straight in (Arrow, by position, its columns checked);
the staging table dropped beforehand only after a failure; one cycle (written, then refreshed). Same matrix, 25k and
100k (p95, ms):

| tape | tab | refresh p50/p95 | write | click | tick | in use | alone: write ms/trips | alone: refresh ms/trips |
|---|---|---|---|---|---|---|---|---|
| 25k | Markets | 60/89 | 77 | 9 | 123 | 41% | 63/15 | 51/16 |
| 25k | Blotter | 68/89 | 71 | 10 | 126 | 40% | 54/15 | 65/14 |
| 25k | P&L and risk | 75/102 | 73 | 10 | 148 | 51% | 51/15 | 76/12 |
| 25k | FX | 51/68 | 71 | 8 | 105 | 36% | 66/15 | 61/17 |
| 25k | Crypto | 139/180 | 74 | 22 | 204 | 70% | 53/15 | 151/20 |
| 25k | Options | 356/421 | 78 | 55 | 452 | 82% | 57/15 | 349/12 |
| 25k | Stock | 620/731 | 83 | 297 | 772 | 85% | 65/15 | 605/16 |
| 25k | News | 285/420 | 77 | 255 | 456 | 70% | 63/15 | 286/4 |
| 25k | Order flow | 371/506 | 84 | 246 | 475 | 52% | 74/15 | 397/16 |
| 25k | TCA | 568/634 | 92 | 94 | 640 | 83% | 65/15 | 555/13 |
| 100k | Markets | 56/76 | 81 | 9 | 119 | 42% | 52/15 | 56/16 |
| 100k | Blotter | 71/91 | 77 | 11 | 133 | 44% | 63/15 | 82/14 |
| 100k | P&L and risk | 79/109 | 80 | 11 | 157 | 53% | 67/15 | 80/12 |
| 100k | FX | 53/76 | 77 | 9 | 119 | 38% | 67/15 | 56/17 |
| 100k | Crypto | 339/497 | 82 | 111 | 493 | 76% | 66/15 | 351/20 |
| 100k | Options | 377/449 | 90 | 56 | 490 | 83% | 63/15 | 378/12 |
| 100k | Stock | 11637 | 196 | 6365 | 11724 | 97% | 80/15 | 9064/16 |
| 100k | News | 4331/4535 | 134 | 4169 | 4595 | 97% | 72/15 | 4708/4 |
| 100k | Order flow | 1687/1986 | 96 | 1512 | 1926 | 87% | 64/15 | 1485/16 |
| 100k | TCA | 1502/1620 | 101 | 249 | 1670 | 93% | 68/15 | 1368/13 |

Every grid's run at its tick answered from the kept results now (none run twice). The light tabs hold at 100k: a tick
~120-160 ms, a click ~10 ms. Writes are 15 round trips (from 49), ~50-80 ms alone -- still over their 30 ms budget.

Each tab's queries alone, the slowest (100k): **Stock**'s prints grid 4.6 s (16k rows) and its price chart 4.4 s (29
rows); **News**' prints grid 4.0 s; **Order flow**'s time and sales 1.0 s (the whole tape, 104k rows, to the grid);
**TCA**'s every tile ~0.2 s (its markouts); Crypto's price chart 0.15 s; every other tile under 0.1 s.

**The cause of Stock and News: the DuckDB dialect's EMA is quadratic.** `DuckDb.windowCall` writes lite's `ema` as
`list_reduce(list(price) OVER (... ROWS UNBOUNDED PRECEDING ...), ...)`: each print's whole history rebuilt as a list
and folded -- quadratic in time and in memory (Stock's chart 273 ms at 25k, 4.4 s at 100k; 250k's out-of-memory). Not
the live layer's: a DuckDB grid of the STOCK source over a long tape pays it once, the live one every refresh.
Deephaven computes the same `ema` incrementally (updateBy), which is why the desk on Deephaven was fine.

**The EMA made linear where its values are never null (2026-10-27).** A value `k` rows back weighs `(1 - alpha)^k` of
it, so the fold need read only the rows whose weight a double holds: `ceil(64 ln 2 / -ln(1 - alpha))` of them (421 for
an alpha of 0.1), a count the database computes in the frame (`ROWS BETWEEN CAST(ceil(...) AS BIGINT) PRECEDING`,
DuckDB 1.4.4 takes it). Measured on JDBC over 100,000 rows in three partitions: the whole-history fold 24.2 s, the cut
0.31 s, every row the same to the last place but 16 of them, those within one unit (alpha 0.02: 21.9 s against 1.6 s).
It holds only where no value is null -- a null skipped makes those rows fewer values than that -- so `DuckDb.windowCall`
cuts only a value the SQL tree proves never null (`TypeFact`), with an alpha spelled in (0, 1); `EmaTest` pins the cut
against the whole history over 3,000 rows. **The desk does not get it yet**: the trading model declares no column `NOT
NULL` (`MARKET_TRADE.PRICE DOUBLE`, though `MarketTrade.price` is `Float[1]`), so its prices are "may be null" to the
SQL tree and the Stock and News tabs keep the whole-history fold. The model's tables declaring `NOT NULL` where their
property is `[1]` is a change to the stress corpus (`scripts/corpus/trading_model.py`), the user's call.

### L3 and L4, landed (2026-10-27)

**L3, the desk in the tab** (`datacube/demo/market-sim/local-host.ts`): where the page names no market server -- DataCube's
static site, no config -- Open…'s Examples list *Trading desk: ten screens, live in this tab*, first. Nothing starts with
the page: opening it fetches the trading model and the desk's own (the site's `trading-model.pure` and `market.pure`),
holds them in a module of the tab's planner, makes the market's tables by the planner's statements in the tab's own
DuckDB, and starts the world. The same saved page, model and screens as the Deephaven desk; only the connections differ
(`model.ts` MARKET_TAB). Each grid is a live reader of its own (L1), as each is on Deephaven.

**L4, its lane** (`//datacube:verify_live_desk_test`, in the datacube CI lane): the site served on the loopback, the desk
opened as Open… opens it with a small world (40 names, a 5,000-print tape, the crypto feed off): every tab's grids and
charts drawn and the tab ticking -- `demo/desk-tabs.mjs`, the Deephaven desk's own checks, shared -- nothing asked of the
network. About 95 s.

What the lane found, fixed the same day:

- **Hidden tabs ran at every emit.** A reader followed its grid's last view whether or not its sheet was shown, so every
  tab visited ran at every emit on the tab's one DuckDB: an emit 7.1 s by the last tab, its ticks on screen 7.2 s after
  their rows. A grid not shown now rests (`DuckDbLiveReader.rest`, when the app says its tick has no sheet to land on)
  and follows again at its next view: the emit 19 ms, re-runs 7,144 to 2,235, every tab's ticks on screen 56-176 ms.
- **The options chain's SQL was 364,053 characters**: Black and Scholes written as a textbook writes it, each function's
  body written wherever it is called -- 9 s to draw, 6.3 s a tick. DuckDB now computes a shared subtree of 50 nodes or
  more once, as a helper column of a select of its own beneath the one that reads it (`DuckDb#select`,
  `SharedSubtrees`, the Deephaven target's rule): 5,243 characters, its ticks on screen in 197 ms. Not DuckDB's lateral
  aliases: DuckDB writes an aliased expression in where its name is read, so helpers read by helpers cost what the
  whole tree did, doubling a level (fourteen levels 2.5 s over 1,000 rows); stacked selects stay linear (thirty levels
  10 ms over 100,000).

Still slow: the options tab's first draw, 6.5 s (on Deephaven 4-7 s): its planning, not its SQL. The bundle: the grid
page's startup script +410 bytes (one startup file split by esbuild, `arrow-ipc.ts`), signed off.

## 3. Incremental views, when measured

ClickHouse's lesson (its insert-triggered materialized views, aggregate states that merge): an append-only table's
decomposable aggregates (count, sum, min, max, an average as sum and count, VWAP as two sums) and the latest row a key
are kept from each batch, never recomputed from the whole history. Only where L4 measures a re-run too slow, and only for
a shape the planner's SQL proves (a group of such aggregates over one stream).

## 4. Where it lands

None of it is Deephaven's: it goes to `main` with the platform's work (the first PR's part A), the trading desk live
there with it.
